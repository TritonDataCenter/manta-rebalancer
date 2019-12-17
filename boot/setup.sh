#!/bin/bash
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright 2019 Joyent, Inc.
#

export PS4='[\D{%FT%TZ}] ${BASH_SOURCE}:${LINENO}: ${FUNCNAME[0]:+${FUNCNAME[0]}(): }'
set -o xtrace
# Dev Note: Attempt to get away with using 'errexit'. I'm not sure how ready
# the manta-scripts are for this.
set -o errexit

source /opt/smartdc/boot/scripts/util.sh
source /opt/smartdc/boot/scripts/services.sh


manta_common_presetup
manta_add_manifest_dir "/opt/smartdc/rebalancer"
manta_common2_setup "rebalancer-manager"

echo "Setting up rebalancer-manager"
/usr/sbin/svccfg import /opt/smartdc/rebalancer/smf/manifests/rebalancer-manager.xml

manta_common2_setup_log_rotation "rebalancer-manager"
manta_common2_setup_end

exit 0
