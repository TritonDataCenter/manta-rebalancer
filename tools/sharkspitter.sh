#!/bin/bash
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2020, Joyent, Inc.
#
#
# This script expects to run in a rebalancer-postgres zone. It takes a list of
# storageIds and searches through the local database which is assumed to be a
# snapshot of the shard.
#
# It will find all objects that belong to the specified storageIds and write
# them to files in out/<pid>.output and will write other files to out/ for
# different conditions encountered.
#
# At the end of the run a final log message is output which includes counts for
# the various conditions encountered.
#

[[ -n ${TRACE} ]] && set -o xtrace
set -o errexit

JOB=$1

if [[ -z $1 ]]; then
    echo "Usage: $0 <storageId> [<storageId> ...]" >&2
    exit 2
fi

mkdir -p out

dataDir=/zones/$(zonename)/data
pgVersion=$(json current < ${dataDir}/manatee-config.json)
/usr/bin/nohup /usr/bin/time \
    /opt/postgresql/${pgVersion}/bin/pg_dump -Fp -a -U postgres -t manta moray \
    | ./node/bin/node sharkspitter-filter.js "$@" \
    > out/$$.stdout 2> out/$$.stderr &

# Wait for nohup to output it's message
sleep 1

echo "# For status, run: tail -f out/$$.std* | bunyan"
