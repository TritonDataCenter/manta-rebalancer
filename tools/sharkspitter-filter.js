/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2020, Joyent, Inc.
 *
 *
 * This filter expects a PostgreSQL-style COPY output on stdin. Most easily
 * this can be done with a command like:
 *
 *   pg_dump -Fp -a -U postgres -t manta moray | $0 <storId> ...
 *
 * The arguments should all be storageIds either in just the number (e.g. 123)
 * or with a .stor suffix (e.g. 123.stor).
 *
 * It must be run in a zone with an alias that matches the pattern:
 *
 *   /^(\d+)\..*\.([a-z\-]+\.scloud.host)-/
 *
 * For example, the zone could have the alias:
 *
 *   46.test-postgres.ap-southeast.scloud.host-5a8ef15e
 *
 * We then take the first component (46 here) as the moray shard number, and
 * the [a-z\-]+\.scloud.host part as the region name for storageIds (in this
 * case ap-southeast.scloud.host)..
 *
 * So in this case if we were run as:
 *
 *   $0 123.stor 456.stor
 *
 * we'd be looking through the input for "sharks" arrays that contain:
 *
 *   123.stor.ap-southeast.scloud.host
 *
 *   or:
 *
 *   456.stor.ap-southeast.scloud.host
 *
 * any lines that include those sharks would be processed.
 *
 * Directories and objects that either have no sharks, or have sharks that were
 * not specified are ignored.
 *
 * For those lines that match one of the specified storageId's, we write out an
 * entry for each line which includes:
 *
 *   <storageId>\t<moray shard>\t<_etag>\t<_value>
 *
 * where <storageId> is the expanded storageId that we matched, and <moray
 * shard> is the moray shard we determined from this zone's alias, and the
 * <_etag> and <_value> match the column values with the same names from the
 * manta table.
 *
 * At the end of normal processing a 'Completed Processing.' log message is
 * written which includes a number of counters to describe what was found in
 * processing.  For the most part these are informational, but a couple should
 * be checked:
 *
 *   numBadObjects   -- If this is non-zero there should be an out/*.badObjects
 *                      output file which includes lines we were unable to
 *                      parse, and this should be investigated before using any
 *                      of the results.
 *
 *   numIgnoredLines -- If this is non-zero, the output should be searched for
 *                      "IGNORING:" to determine why there were lines we did
 *                      not expect.
 *
 *   numManyCopies   -- Currently we expect all objects to have copies=2, if
 *                      this counter is non-zero it indicates the number of
 *                      objects that had copies > 2. The list of these
 *                      objects will be in an out/*.manyCopies file.
 *
 *   numSameDC       -- If this is non-zero, it indicates that objects were
 *                      found where more than one copy is in the *same*
 *                      datacenter. The list of these objects will be in an
 *                      out/*.sharedDC file.
 *
 *   reachedEOF      -- This indicates that we read all of stdin and got to
 *                      EOF. If this is not true, the problem should be
 *                      investigated and the output should not be used as it
 *                      may be incomplete.
 *
 */

var assert = require('assert-plus');
var child_process = require('child_process');
var fs = require('fs');
var os = require('os');

var bunyan = require('bunyan');
var lineReader = require('line-reader');


// Config
var reportFreq = 10000; // progress message every X lines

// Globals
var filePrefix = 'out/' + process.pid;
var logger = bunyan.createLogger({name: 'sharkspitter-filter'});
var myShard;
var targets = [];

// Counters
var numAllMigrating = 0;
var numBadObjects = 0;
var numDirs = 0;
var numFixedBadObjects = 0;
var numIgnoredLines = 0;
var numLines = 0;
var numManyCopies = 0;
var numMpuPartsIgnored = 0;
var numObjects = 0;
var numSameDC = 0;


function setupOutputFilenames(opts)  {
    var idx;
    var storId;

    assert.object(opts, 'opts');
    assert.string(opts.myRegion, 'opts.myRegion');
    assert.string(opts.myShard, 'opts.myShard');

    // Figure out which storageIds we're looking for and build the filenames for output files
    for (idx = 2; idx < process.argv.length; idx++) {
        storId = process.argv[idx];

        //
        // Args should be either '123' or '123.stor'
        //
        if (storId.match(/^[0-9]+$/)) {
            storId = storId + '.stor.' + opts.myRegion;
        } else if (storId.match(/^[0-9]+\.stor$/)) {
            storId = storId + '.' + opts.myRegion;
        } else {
            console.error('Unexpected argument: [' + storId + ']');
            process.exit(1);
        }

        targets[storId] = filePrefix + '.' + opts.myShard + '.moray.sharkObjects.' + storId;

        // empty write to ensure the file is created even if there are no objects found
        fs.appendFileSync(targets[storId] + '.objects', '');
    }
}

function processObject(fields, line, callback) {
    var brokenContentTypeRe = /"contentType":"\\\\"([^"]+)\\\\""/;
    var datacenters = [];
    var found;
    var matches;
    var _value;

    //
    // MPU is not used, so we're going to ignore all MPU upload parts.
    //
    // fields[2] is the key (Manta path)
    //
    if (fields[2].match(/^\/[0-9a-f\-]+\/uploads\//)) {
        numMpuPartsIgnored++;
        callback();
        return;
    }

    try {
        _value = JSON.parse(fields[3]);
    } catch (e) {
        //
        // We have some bad data where we end up with JSON lines like:
        //
        // "contentType":"\\"image/jpeg\\""
        //
        // when they come out of Postgres. If we hit this case, we'll just fix
        // the contentType and try again.
        //

        matches = fields[3].match(brokenContentTypeRe);

        if (matches) {
            fixed =  fields[3].replace(brokenContentTypeRe, '"contentType":"' + matches[1] + '"');
            logger.warn({
                broken: fields[3],
                fixed: fixed
            }, 'Attempted to fix broken JSON');

            try {
               _value = JSON.parse(fixed);
               // If the JSON.parse() didn't blow up, the fixed version is
               // parsable so pretend that's what we read.
               fields[3] = fixed;
               numFixedBadObjects++;
            } catch (e) {
                numBadObjects++;
                fs.appendFileSync(filePrefix + '.badObjects', line + '\n');
                callback();
                return;
            }
        } else {
            numBadObjects++;
            fs.appendFileSync(filePrefix + '.badObjects', line + '\n');
            callback();
            return;
        }
    }

    assert.string(_value.key, 'Key must exist');
    assert.equal(_value.key, fields[2], 'Keys must match');
    assert.arrayOfObject(_value.sharks, 'Must have sharks array');

    found = 0;
    for (sharkObj of _value.sharks) {
        assert.string(sharkObj.datacenter, 'sharkObj.datacenter');
        assert.string(sharkObj.manta_storage_id, 'sharkObj.manta_storage_id');

        sharkFile = targets[sharkObj.manta_storage_id];

        if (sharkFile === undefined) {
            continue;
        }

        found++;

        if (_value.sharks.length > 2) {
            // We expect at most copies=2
            numManyCopies++;
            fs.appendFileSync(sharkFile + '.manyCopies', fields[4] + '\t' + fields[3] + '\n');
        }

        if (datacenters.indexOf(sharkObj.datacenter) !== -1) {
            // There was already a copy in this DC
            numSameDC++;
            fs.appendFileSync(sharkFile + '.sharedDC', fields[4] + '\t' + fields[3] + '\n');
        }
        datacenters.push(sharkObj.datacenter);

        // We don't catch the error here, so if this fails it will throw and we'll crash
        fs.appendFileSync(sharkFile + '.objects',
            sharkObj.manta_storage_id + '\t' +
            myShard + '\t' +
            fields[4] + '\t' + // _etag
            fields[3] + '\n'); // _value
    }

    if ((_value.sharks.length > 0) && (found === _value.sharks.length)) {
        // All copies are being migrated with this batch
        numAllMigrating++;
        fs.appendFileSync(filePrefix + '.allMigrating', fields[4] + '\t' + fields[3] + '\n');
    }

    callback();
}

function main() {
    var hostname;
    var fieldCount;
    var inCopy = false;
    var matches;
    var myRegion;
    var reachedEOF = false;

    // os.hostname() gives the zonename rather than hostname on SmartOS. LAME.
    hostname = child_process.execSync('/usr/sbin/mdata-get sdc:alias').toString().trim();

    // myShard = hostname.split('.')[0];
    matches = hostname.match(/^(\d+)\..*\.([a-z\-]+\.scloud.host)-/);

    assert.ok(matches, 'Unexpected zone alias, should look like: 46.rebalancer-postgres.ap-southeast.scloud.host-5a8ef15e');
    assert.equal(matches.length, 3);

    myShard = matches[1];
    myRegion = matches[2];

    logger.info('Running on shard ' + myShard + '.moray.' + myRegion + ' based on hostname "' + hostname + '"');

    setupOutputFilenames({
        myRegion: myRegion,
        myShard: myShard
    });

    assert.ok(Object.keys(targets).length > 0, 'Should have list of targets');
    logger.info({targets: Object.keys(targets)}, 'Built list of targets');

    lineReader.eachLine(process.stdin, function _lineHandler(rawLine, lastLine, cb) {
        var fields;
        var line = rawLine.trim();
        var sharkObj;

        numLines++;

        if ((numLines > 0) && ((numLines % reportFreq) == 0)) {
            logger.info('Processed %d lines', numLines);
        }

        if (!inCopy && line.match(/^COPY manta \(_id, _txn_snap, _key, _value, _etag, _mtime, _vnode, dirname, name, owner, objectid, type, _idx\) FROM stdin;$/)) {
            fieldCount = 13;
            inCopy = true;
            cb();
            return;
        } else if (!inCopy && line.match(/^COPY manta \(_id, _txn_snap, _key, _value, _etag, _mtime, _vnode, dirname, name, owner, objectid, type\) FROM stdin;$/)) {
            fieldCount = 12;
            inCopy = true;
            cb();
            return;
        }

        if (lastLine) {
            // This lets us know if we stopped because we reached end of input or if we hit an error
            reachedEOF = true;
        }

        if (inCopy) {
            if (line.match(/^\\.$/)) {
                inCopy = false;
                cb();
            } else if (line.match(/"type":"object"/)) {
                numObjects++;
                fields = line.split('\t');

                assert.equal(fields.length, fieldCount, 'Lines must have ' + fieldCount + ' fields');

                processObject(fields, line, cb);
            } else if (line.match(/"type":"directory"/)) {
                // Keep track of dirs just for sanity checking that things look reasonable
                numDirs++;
                cb();
            } else {
                numIgnoredLines++;
                logger.warn('IGNORING: ' + line);
                cb();
            }
        } else {
            cb();
        }
    }, function _done(err) {
        assert.ifError(err, 'Should have processed all lines');

        logger.info({
            numAllMigrating: numAllMigrating,
            numBadObjects: numBadObjects,
            numDirs: numDirs,
            numFixedBadObjects: numFixedBadObjects,
            numIgnoredLines: numIgnoredLines,
            numLines: numLines,
            numManyCopies: numManyCopies,
            numMpuPartsIgnored: numMpuPartsIgnored,
            numObjects: numObjects,
            numSameDC: numSameDC,
            reachedEOF: reachedEOF
        }, 'Completed Processing.');

        if (!reachedEOF) {
            // Sad.
            process.exit(1);
        }
    });
}

// Call main to get the party started!
main();
