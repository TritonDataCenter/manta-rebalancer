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

Rebalancer manager metrics can be accessed on port `8878` and the following
metrics are exposed:

* Request count, categorized by request type.
* Total number of bytes processed.
* Object count, indicating the total number of objects which have been processed.
* Error count, categorized by type of error observed.
* Skipped object, count categorized by reason that an object was skipped.
* Assignment processing times (in the form of a histogram).

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

### Cleaning up old assignments
Currently the rebalancer manager does not clean up assignments created when the
job ends [MANTA-5288](https://jira.joyent.us/browse/MANTA-5288).  On each
rebalancer-agent the completed assignemnts are stored in
`/var/tmp/rebalancer/completed/`.

The number of assignments can be determined via:
```
manta-oneach -s storage 'ls  /var/tmp/rebalancer/completed/ | wc -l'
```

Those assignemnts can be removed *ONLY* when there are no rebalancer jobs
running via:
```
manta-oneach -s storage 'rm /var/tmp/rebalancer/completed/*'
```


## Rebalancer Agent

### Deployment

As part of [MANTA-5293](https://jira.joyent.us/browse/MANTA-5293) a new
`manta-hotpatch-rebalancer-agent` tool was added to the headnode global zone.

To install it requires a recent `sdcadm` and an update of your manta-deployment
zone (a.k.a. the "manta0" zone):

    sdcadm self-update --latest
    sdcadm up manta

Using the hotpatch tool should hopefully be obvious from its help output:

    [root@headnode (nightly-2) ~]# manta-hotpatch-rebalancer-agent help
    Hotpatch rebalancer-agent in deployed "storage" instances.
    
    Usage:
        manta-hotpatch-rebalancer-agent [OPTIONS] COMMAND [ARGS...]
        manta-hotpatch-rebalancer-agent help COMMAND
    
    Options:
        -h, --help      Print this help and exit.
        --version       Print version and exit.
        -v, --verbose   Verbose trace logging.
    
    Commands:
        help (?)        Help on a specific sub-command.
    
        list            List running rebalancer-agent versions.
        avail           List newer available images for hotpatching rebalancer-agent.
        deploy          Deploy the given rebalancer-agent image hotpatch to storage instances.
        undeploy        Undo the rebalancer-agent hotpatch on storage instances.
    
    Use this tool to hotpatch the "rebalancer-agent" service that runs in each Manta
    "storage" service instance. While hotpatching is discouraged, this tool exists
    during active Rebalancer development because reprovisioning all "storage"
    instances in a large datacenter solely for a rebalancer-agent fix can be
    painful.
    
    Typical usage is:
    
    1. List the current version of all rebalancer-agents:
            manta-hotpatch-rebalancer-agent list
    
    2. List available rebalancer-agent builds (in the "dev" channel of
       updates.joyent.com) to import and use for hotpatching. This only lists
       builds newer than the current oldest rebalancer-agent.
            manta-hotpatch-rebalancer-agent avail
       Alternatively a rebalancer-agent build can be manually imported
       into the local IMGAPI.
    
    3. Hotpatch a rebalancer-agent image in all storage instances in this DC:
            manta-hotpatch-rebalancer-agent deploy -a IMAGE-UUID
    
    4. If needed, revert any hotpatches and restore the storage image's original
       rebalancer-agent.
            manta-hotpatch-rebalancer-agent undeploy -a
    
Note that this tool only operates on instances in the current datacenter. As
with most Manta tooling, to perform upgrades across an entire region requires
running the tooling in each DC separately.


### Example usage

The rest of this document is an example of running this tool in nightly-2
(a small test Triton datacenter). Each subcommand has other options that are not
all shown here, e.g. controlling concurrency, selecting particular storage
instances to hotpatch, etc.

The "list" command will show the current rebalancer-agent version and whether
it is hotpatched in every storage node:

    [root@headnode (nightly-2) ~]# manta-hotpatch-rebalancer-agent list
    STORAGE NODE                          VERSION                                   HOTPATCHED
    64052e9d-c379-44ae-9036-2293b88baa7c  0.1.0 (master-20200616T185217Z-g82b8008)  false
    a83343ec-1d91-467b-b938-a0af7f86e92c  0.1.0 (master-20200616T185217Z-g82b8008)  false
    a8aaa7c4-2699-40ed-83e5-aabec7d55b3d  0.1.0 (master-20200616T185217Z-g82b8008)  false

The "avail" command lists any available rebalancer-agent builds at or newer
than what is currently deployed:

    [root@headnode (nightly-2) ~]# manta-hotpatch-rebalancer-agent avail
    UUID                                  NAME                      VERSION                           PUBLISHED_AT
    7a5529e2-3d8b-4c9c-84af-46a1f6e0bb95  mantav2-rebalancer-agent  master-20200617T234037Z-g6dc482c  2020-06-18T00:09:27.901Z
    
The "deploy" command does the hotpatching:

    [root@headnode (nightly-2) ~]# manta-hotpatch-rebalancer-agent deploy 7a5529e2-3d8b-4c9c-84af-46a1f6e0bb95 -a
    This will do the following:
    - Import rebalancer-agent image 7a5529e2-3d8b-4c9c-84af-46a1f6e0bb95
      (master-20200617T234037Z-g6dc482c) from updates.joyent.com.
    - Hotpatch rebalancer-agent image 7a5529e2-3d8b-4c9c-84af-46a1f6e0bb95
      (master-20200617T234037Z-g6dc482c) on all 3 storage instances in this DC
    
    Would you like to hotpatch? [y/N] y
    Trace logging to "/var/tmp/manta-hotpatch-rebalancer-agent.20200619T180117Z.deploy.log"
    Importing image 7a5529e2-3d8b-4c9c-84af-46a1f6e0bb95 from updates.joyent.com
    Imported image
    Hotpatched storage instance a8aaa7c4-2699-40ed-83e5-aabec7d55b3d
    Hotpatched storage instance a83343ec-1d91-467b-b938-a0af7f86e92c
    Hotpatching 3 storage insts       [================================================================>] 100%        3
    Hotpatched storage instance 64052e9d-c379-44ae-9036-2293b88baa7c
    Successfully hotpatched.
    
    [root@headnode (nightly-2) ~]# manta-hotpatch-rebalancer-agent list
    STORAGE NODE                          VERSION                                   HOTPATCHED
    64052e9d-c379-44ae-9036-2293b88baa7c  0.1.0 (master-20200617T234037Z-g6dc482c)  true
    a83343ec-1d91-467b-b938-a0af7f86e92c  0.1.0 (master-20200617T234037Z-g6dc482c)  true
    a8aaa7c4-2699-40ed-83e5-aabec7d55b3d  0.1.0 (master-20200617T234037Z-g6dc482c)  true

The "undeploy" command can be used to revert back to the original
rebalancer-agent in a storage instance (i.e. to undo any hotpatching):

    [root@headnode (nightly-2) ~]# manta-hotpatch-rebalancer-agent undeploy -a
    This will revert any rebalancer-agent hotpatches on all 3 storage instances in this DC
    
    Would you like to continue? [y/N] y
    Trace logging to "/var/tmp/manta-hotpatch-rebalancer-agent.20200619T180148Z.undeploy.log"
    Unhotpatched storage instance 64052e9d-c379-44ae-9036-2293b88baa7c
    Unhotpatched storage instance a83343ec-1d91-467b-b938-a0af7f86e92c
    Unhotpatching 3 storage insts     [================================================================>] 100%        3
    Unhotpatched storage instance a8aaa7c4-2699-40ed-83e5-aabec7d55b3d
    Successfully reverted hotpatches.
    
    [root@headnode (nightly-2) ~]# manta-hotpatch-rebalancer-agent list
    STORAGE NODE                          VERSION                                   HOTPATCHED
    64052e9d-c379-44ae-9036-2293b88baa7c  0.1.0 (master-20200616T185217Z-g82b8008)  false
    a83343ec-1d91-467b-b938-a0af7f86e92c  0.1.0 (master-20200616T185217Z-g82b8008)  false
    a8aaa7c4-2699-40ed-83e5-aabec7d55b3d  0.1.0 (master-20200616T185217Z-g82b8008)  false


### Configuration and Troubleshooting
* The rebalancer agent runs as an SMF service on each mako zone:
```
svc:/manta/application/rebalancer-agent:default
```

* Logs are located in the SMF log directory and rotated hourly:
```
$ svcs -L svc:/manta/application/rebalancer-agent:default
/var/svc/log/manta-application-rebalancer-agent:default.log
```

* Finding `error` or `skipped` reasons:
    1.  Enter the local job database
    ```
    psql -U postgres <job uuid>
    ```
    2.  Query the `evacuateobjects` table:
    ```
    SELECT skipped_reason,count(skipped_reason) FROM evacauteobjects WHERE status = 'skipped' GROUP BY skipped_reason;
    ```
    or 
    ```
    SELECT error,count(error) FROM evacauteobjects WHERE status = 'error' GROUP BY error;
    ```

See [agent documentation](https://github.com/joyent/manta-rebalancer/blob/master/docs/agent.md) for additional details.

### Metrics
Rebalancer agent metrics can be accessed on port `8878` and the following
metrics are exposed:

* Request count, categorized by request type.
* Object count, indicating the total number of objects which have been processed.
* Total bytes processed.
* Error count, categorized by type of error observed.
* Assignment processing times (in the form of a histogram).
