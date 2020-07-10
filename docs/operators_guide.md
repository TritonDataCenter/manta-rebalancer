# Manta Rebalancer Operators Guide

The manta-rebalancer consists of a manager and a set of agents.  The manager is
a single service that is deployed to its own container/zone.  A rebalancer agent
is deployed to each of the mako zones on a storage node.

## Performance
The performance of the rebalancer is primarily dependent on the allowable impact
on the metadata tier.  With the tunables discussed below it is possible to
increase the performance of both the metadata tier and the object download
concurrency to a level that would result in degradation of the user experience.

During testing we did notice some delays in overall job time if storage nodes
were not available.  This applies to both the destination storage nodes where
the rebalancer agent runs as well as the source storage nodes where objects are
copied from.  If we start to see a significant increase in `skip` level errors
it is worth investigating the manager and agent logs.  Some relevant Jira issues
are:

* [MANTA-5326](https://jira.joyent.us/browse/MANTA-5326)
* [MANTA-5330](https://jira.joyent.us/browse/MANTA-5330)
* [MANTA-5231](https://jira.joyent.us/browse/MANTA-5231) 
* [MANTA-5119](https://jira.joyent.us/browse/MANTA-5119)
* [MANTA-5159](https://jira.joyent.us/browse/MANTA-5159)
* See also `rebalancer-performance` Jira label

## Rebalancer Manager

### Build and Deployment 

The rebalancer manager is part of the default manta v2 deployment, and is built
using Jenkins.

The rebalancer manager can be deployed/upgraded in the same way as other manta
components using `manta-adm update -f <update_file>` where the `<update_file>`
specifies the image uuid of the rebalancer image to update to.

The rebalancer manager places its local postgres database in a delegated dataset
so that it will be maintained across reprovisions.  The memory requirements are
defined in the [sdc-manta repository](https://github.com/joyent/sdc-manta).


### Configuration and Troubleshooting
The rebalancer manager runs as an [SMF service](https://github.com/joyent/manta-rebalancer/blob/master/docs/manager.md#service-parameters) on its own zone:
```
svc:/manta/application/rebalancer:default
```

Logs are located in the SMF log directory and rotated hourly:
```
$ svcs -L svc:/manta/application/rebalancer:default
/var/svc/log/manta-application-rebalancer:default.log
```

The log level defaults to `debug`.  To change this specify a higher log level in
SAPI like so (this process is the same for all other [tunables](https://github.com/joyent/manta-rebalancer/blob/master/docs/manager.md#job-options)):
```
MANTA_APP=$(sdc-sapi /applications?name=manta | json -Ha uuid)
echo '{ "metadata": {"REBALANCER_LOG_LEVEL": "trace" } }' | sapiadm update $MANTA_APP
```

This should be propagated to the configuration file found in
`/opt/smartdc/rebalancer/config.json`
```
"log_level": "trace"
```

A Log level change is the only tunable that require a full service restart
(`svcadm restart rebalancer`), other's should be refreshed by config-agent's
invocation `svcadm refresh` and applied to the next Job that is run.
```
svcadm restart svc:/manta/application/rebalancer:default
```

Logs are located in the SMF log directory and rotated hourly:
```
$ svcs -L svc:/manta/application/rebalancer-agent:default
/var/svc/log/manta-application-rebalancer-agent:default.log
```

### Metadata throttle

The metadata throttle is an undocumented feature that exposes the ability to dynamically (while
a job is running) update the [REBALANCER_MAX_METADATA_UPDATE_THREADS](https://github.com/joyent/manta-rebalancer/blob/master/docs/manager.md#job-options) tunable.  This can be done with curl like so:
```
curl localhost/jobs/<job_uuid> -X PUT -d '{
    "action": "set_metadata_threads",
    "params": 30
}'
```

It is not currently possible to increase the number of metadata update threads beyond 100.  This maximum value is hard coded to minimize the impact to the
metadata tier by an accidental update. At the time of writing even the maximum
of 100 is not advised.



### Metrics
_TODO_

### Marking evacuate target read-only
When an evacuate job is run the target storage node needs to be marked read-only
and remain read-only for the duration of the job.

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


### Local Database Backup
Until [MANTA-5105](https://jira.joyent.us/browse/MANTA-5105) is implemented the local database corresponding to each job
should be backed up once that job is complete. 

The backup can be accomplished as follows
```
pg_dump -U postgres <job_uuid> > <job_uuid>.backup
```

The `<job_uuid>.backup` file should be saved outside the rebalancer zone as a
backup.

The database can then be restored like so:
```
createdb -U postgres <job_uuid>
psql -U postgres <job_uuid> < <job_uuid>.backup
```


## Rebalancer Agent

### Deployment
_TODO_:
    _insert rebalancer agent mako upgrade instructions_


### Configuration and Troubleshooting
The rebalancer agent runs as an SMF service on each mako zone:
```
svc:/manta/application/rebalancer-agent:default
```

Logs are located in the SMF log directory and rotated hourly:
```
$ svcs -L svc:/manta/application/rebalancer-agent:default
/var/svc/log/manta-application-rebalancer-agent:default.log
```

See [agent documentation](https://github.com/joyent/manta-rebalancer/blob/master/docs/agent.md) for additional details.

### Metrics
_TODO_
