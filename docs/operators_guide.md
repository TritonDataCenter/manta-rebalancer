# Manta Rebalancer Operators Guide

The manta-rebalancer consists of a manager and a set of agents.  The manager is
a single service that is deployed to its own container/zone.  A rebalancer agent
is deployed to each of the mako zones on a storage node.

## Rebalancer Manager

### Build and Deployment 

The rebalancer manager is part of the default manta v2 deployment, and is built
using Jenkins.

The rebalancer manager can be deployed/upgraded in the same way as other manta
components using `manta-adm update -f <update_file>` where the `<update_file>`
specifies the image uuid of the rebalancer image to update to.

The rebalancer manager places its local postgres database in a delegated dataset
so that it will be maintained across reprovisions.

### Configuration
The rebalancer manager runs as an SMF service and makes use of config-agent.
It's sapi_manifest template can be found in:
`/opt/smartdc/rebalancer/sapi_manifests/rebalancer/template`

Most of the configuration parameters and tunables are controlled by SAPI.  Only
changes to the `log_level` require an SMF restart. 

Refer to the [manager documentation](https://github.com/joyent/manta-rebalancer/blob/master/docs/manager.md#manager-configuration-parameters) for more details on available configuration parameters and tunables.

### Metadata throttle


### Troubleshooting
The first place to begin
investigations is in the in the SMF logs, which currently default to `debug`
level.  To change this specify a higher log level in `/opt/smartdc/rebalancer`:
```
"log_level": "debug"
```
Log level changes require a service restart.


### Metrics

### Marking evacuate target readonly
When an evacaute job is run the target storage node needs to be marked readonly
and remain readonly for the duration of the job.

1. Login to the target storage node and disable the minnow service: 
```
svcadm disable svc:/manta/application/minnow:default
```

1. If [storinfo](https://github.com/joyent/manta-storinfo) is deployed issue
   each storinfo endpoint the flush command:
```
for ip in `dig +short storinfo.<domain>`; do curl $ip/flush -X POST; done
```

1. Restart Muskie on all storage nodes:
```
manta-oneach -s webapi 'for s in `svcs -o FMRI muskie | grep muskie-`; do svcadm restart $s; done'
```


## Rebalancer Agent
### Configuration
### Deployment


