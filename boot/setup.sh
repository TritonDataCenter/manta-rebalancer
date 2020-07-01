#!/bin/bash
# 
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright 2020 Joyent, Inc.
#

export PS4='[\D{%FT%TZ}] ${BASH_SOURCE}:${LINENO}:
${FUNCNAME[0]:+${FUNCNAME[0]}(): }'
set -o xtrace
# Dev Note: Attempt to get away with using 'errexit'. I'm not sure how ready
# the manta-scripts are for this.
set -o errexit

PGFMRI=svc:/pkgsrc/postgresql

source /opt/smartdc/boot/scripts/util.sh
source /opt/smartdc/boot/scripts/services.sh

manta_common_presetup
manta_add_manifest_dir "/opt/smartdc/rebalancer"
manta_common2_setup "rebalancer"

# Set path for rebalancer-adm
echo "export PATH=\$PATH:/opt/smartdc/rebalancer/bin" >> /root/.bashrc

echo "Setting up rebalancer delegated dataset and PGDATA directory"
source /opt/smartdc/boot/rebalancer.sh
rebalancer_delegated_dataset

echo "Setting up rebalancer manager"
/usr/sbin/svccfg import /opt/local/lib/svc/manifest/postgresql.xml
cp /opt/smartdc/rebalancer/etc/postgresql.conf /rebalancer/pg/data/
/usr/sbin/svccfg -s $PGFMRI setprop config/data = /rebalancer/pg/data
/usr/sbin/svcadm refresh $PGFMRI
/usr/sbin/svcadm enable $PGFMRI
/usr/sbin/svccfg import /opt/smartdc/rebalancer/smf/manifests/rebalancer.xml


manta_common2_setup_log_rotation "rebalancer"
manta_common2_setup_end

exit 0
