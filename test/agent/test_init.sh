#!/bin/bash
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright 2019, Joyent, Inc.
#

#
# This is a utility intended to automate an appreciable amount of the laborious
# work required to set up a test enviornment to test the rebalancer agent.  It
# requires two arguments to get started:
#
# 1. The destination storage node on which object will reside in the form of
#    n.stor.<domain>.  An example would look like: 2.stor.us-west.joyent.us.
#
# 2. A path which contains one or more files that the rebalaner agent will
#    attempt to download during a test.
#
# With the provided storage node information and a file path, this utility will
# create an assignment based on this information and store it in a database
# which the rebalancer agent will be able to load from disk and process like an
# assignment that it would receive from a rebalancer zone.  In addition to
# creating the assignment, it will also upload the objects to the destination
# storage node (i.e. #1 above), where they will be stored in /manta/rebalancer.
# The upload is performed via the http PUT interface supplied to us by mako.
#

uuid=$(uuid)
files_dir="files"
account="rebalancer"
storage_id="${1}"
object_dir="/var/tmp/rebalancer/src"

if [[ -n "$TRACE" ]]; then
    export PS4='[\D{%FT%TZ}] ${BASH_SOURCE}:${LINENO}: ${FUNCNAME[0]:+${FUNCNAME[0]}(): }'
    set -o xtrace
fi
set -o errexit
set -o pipefail

function fatal
{
	local LNOW=`date`
	echo "$LNOW: $(basename $0): fatal error: $*" >&2
	exit 1
}

function string_to_hex()
{
    if [[ -z "$1" ]]; then
        fatal "No string was provided to string_to_hex()."
    fi

    echo -n "$1" | od -A n -t x1 |sed 's/ *//g' | tr -d '\n'
}

# Wrapper for the sqlite3 client.  Any failure of the sqlite3
# client along the way should bring this process to an immedaite
# halt, so rather than having the scaffolding of checking the
# return status of the operation, we wrap it up with the call
# to sqlite3 itself.
function sqlite_exec()
{
    db="$1"
    query="$2"

    if [[ -z "$db" || -z "$query" ]]; then
        fatal "insert_task() requires 2 arguments."
    fi

    sqlite3 "$db" "$query"

    if [[ $? -ne 0 ]]; then
        fatal "Failed to execute $query on $db"
    fi
}

# Given the the name of a database and the location of a file, genereate the
# associated task information for it and insert it in to the local database.
# The tasks table will eventually be recalled by the rebalancer test automation
# code and processed.
function insert_task()
{
    db="$1"
    path="$2"
    object="$(basename $2)"
    md5sum="$(cat $path | openssl dgst -md5 -binary | base64)"
    status=$(string_to_hex '"Pending"')

    if [[ -z "$db" || -z "$path" ]]; then
        fatal "insert_task() requires 2 arguments."
    fi

    query=$(echo "INSERT INTO tasks " \
        "(object_id, owner, md5sum, datacenter, manta_storage_id, status) " \
        "VALUES " \
        "('$object', '$account', '$md5sum', 'dc', '$storage_id', X'$status');")

    sqlite_exec "$db" "$query"
}

# Create a database which will be used by the rebalancer test automation.  The
# database will represent a single assignment.  The database representation of
# an assignment in the rebalancer is comprised of two tables: A tasks table,
# which contains some number of tasks (objects to download and their associated
# location information within manta and a stats table.  The stats table contains
# one single serialized entry which the agent loads in to memory and updates
# as it processes tasks.
function create_db()
{
    db="$1"

    if [[ -z "$db" ]]; then
        fatal "create_db() requires a name."
    fi

    # Create a table for the tasks
    query="CREATE TABLE tasks (
        object_id text primary key not null unique,
        owner text not null,
        md5sum text not null,
        datacenter text not null,
        manta_storage_id text not null,
        status text not null);"
    sqlite_exec "$db" "$query"

    # Create a table for stats
    query="CREATE TABLE stats (stats text not null);"
    sqlite_exec "$db" "$query"

    # Initialize the single stats structure that goes in to
    # the stats table.
    total=$(ls $files_dir | wc -l)
    tsobj=$(echo "{}" | json -e "\
        this.state = '"Scheduled"';
        this.failed = 0;
        this.complete = 0;
        this.total = ${total};" | json -o json-0)

    # Convert the json object to hex so that serde_json is able
    # to make sense of it upon reading it in.
    tsobjx=$(string_to_hex $tsobj)

    # Finally, insert the stats structure in to the stats table.
    query=$(echo "INSERT INTO stats VALUES(X'${tsobjx}');")
    sqlite_exec "$db" "$query"
}

echo $uuid
create_db "$uuid"

for i in "$files_dir"/*
do
    insert_task "$uuid" "$i"

    if [[ "$storage_id" -eq "localhost" ]]; then
        mkdir -p "$object_dir"
        cp "$i" "$object_dir"
    else
        curl -X PUT -T $i "http://$storage_id/$account/$(basename $i)"
    fi

    if [[ $? -ne 0 ]]; then
        fatal "Failed to upload object $i to storage node $storage_id"
    fi
done
