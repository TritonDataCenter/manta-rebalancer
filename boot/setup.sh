#!/bin/bash
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright 2020 Joyent, Inc.
#

export PS4='[\D{%FT%TZ}] ${BASH_SOURCE}:${LINENO}: ${FUNCNAME[0]:+${FUNCNAME[0]}(): }'
set -o xtrace
# Dev Note: Attempt to get away with using 'errexit'. I'm not sure how ready
# the manta-scripts are for this.
set -o errexit

PGFMRI=svc:/manta/postgresql
PG_LOG_DIR=/var/pg

source /opt/smartdc/boot/scripts/util.sh
source /opt/smartdc/boot/scripts/services.sh

function create_postgres_user {
    # create postgres group
    echo "creating postgres group (gid=907)"
    groupadd -g 907 postgres

    # create postgres user
    echo "creating postgres user (uid=907)"
    useradd -u 907 -g postgres -m postgres

    # grant postgres user chmod chown privileges with sudo
    echo "postgres    ALL=(ALL) NOPASSWD: /usr/bin/chown, /usr/bin/chmod, /opt/local/bin/chown, /opt/local/bin/chmod" >> /opt/local/etc/sudoers

    # XXX: is this needed?
    # give postgres user zfs permmissions.
    #echo "grant postgres user zfs perms"
    #zfs allow -ld postgres create,destroy,diff,hold,release,rename,setuid,rollback,share,snapshot,mount,promote,send,receive,clone,mountpoint,canmount $PARENT_DATASET

    # add pg log dir
    mkdir -p $PG_LOG_DIR
    chown -R postgres $PG_LOG_DIR
    chmod 700 $PG_LOG_DIR
}

manta_common_presetup
manta_add_manifest_dir "/opt/smartdc/rebalancer"
manta_common2_setup "rebalancer"

# Set path for rebalancer-adm
echo "export PATH=\$PATH:/opt/smartdc/rebalancer/bin" >> /root/.bashrc

create_postgres_user

echo "Setting up rebalancer delegated dataset and PGDATA directory"
source /opt/smartdc/boot/rebalancer.sh
rebalancer_delegated_dataset

echo "Setting up rebalancer manager"
cp /opt/smartdc/rebalancer/smf/methods/postgresql /opt/local/lib/svc/method/postgresql
chmod +x /opt/local/lib/svc/method/postgresql
cp /opt/smartdc/rebalancer/smf/manifests/postgresql.xml /opt/local/lib/svc/manifest/postgresql.xml
/usr/sbin/svccfg import /opt/local/lib/svc/manifest/postgresql.xml
cp /opt/smartdc/rebalancer/etc/postgresql.conf /rebalancer/pg/data/
/usr/sbin/svccfg -s $PGFMRI setprop config/data = /rebalancer/pg/data
/usr/sbin/svcadm refresh $PGFMRI
/usr/sbin/svcadm enable $PGFMRI
/usr/sbin/svccfg import /opt/smartdc/rebalancer/smf/manifests/rebalancer.xml

manta_common2_setup_log_rotation "rebalancer"
manta_common2_setup_end

# metricPorts are scraped by cmon-agent for prometheus metrics.
#
# 8878 is the metrics port for manta-rebalancer (i.e. the rebalaner manager).  This
# port is currently not configurable, so it is specified directly here.
#
mdata-put metricPorts "8878"

exit 0
