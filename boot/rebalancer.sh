#!/bin/bash
# -*- mode: shell-script; fill-column: 80; -*-
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright 2020 Joyent, Inc.
#

#
# Common steps to enable a rebalancer service in a zone.

REBAL_ROOT=/rebalancer
SUDO=/opt/local/bin/sudo
INITDB=/opt/postgresql/12.0/bin/initdb

# Sets up delegated dataset at /$REBAL_ROOT/rebalancer and configures directory
# for use of postgresql.
function rebalancer_delegated_dataset
{
    local ZONE_UUID=$(zonename)
    local ZONE_DATASET=zones/$ZONE_UUID/data
    local mountpoint=

    mountpoint=$(zfs get -H -o value mountpoint $ZONE_DATASET)
    if [[ ${mountpoint} != ${REBAL_ROOT} ]]; then
        zfs set mountpoint=${REBAL_ROOT} ${ZONE_DATASET} || \
            fatal "failed to set mountpoint"
        chmod 777 ${REBAL_ROOT}
    fi

    if [ ! -d "${REBAL_ROOT}/pg/data" ]; then
        mkdir -p ${REBAL_ROOT}/pg/data
        chown postgres:postgres ${REBAL_ROOT}/pg/data
        $SUDO -u postgres $INITDB -D ${REBAL_ROOT}/pg/data
    fi
}
