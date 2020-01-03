<!--
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
-->

<!--
    Copyright 2019, Joyent, Inc.
-->

# Manta Rebalancer Manager
The [Manta](https://github.com/joyent/manta) object rebalancer manager is a
major subsystem within the rebalancer project that oversees and orchestrates
operations referred to as jobs which currently evacuate objects from storage
nodes in a Manta deployment.  A job is comprised of one or more assignments
which are then delegated to agents for processing.  Each assignment consists of one or more objects to download.  The manager is responsible for ensuring that
each assignment runs to completion as well as reporting detailed information
about any failures that occur during the execution of a job, whether that be
propagating failure information from an agent or reporting errors that occur
during other stages of the job (e.g. assignment creation, metadata tier book
keeping at the completion of an assignment, etc).

## Rebalancer Manager Overview
The diagram below shows the key components that comprise the rebalancer manager
during the execution of a job.  A few important notes:
* Each job can (and likely will) be comprised of multiple assignments.  This is
because when a shark is evacuated, its objects are redistributed throughout the
region rather than just to a single destination.
* Destinations for objects are decided based on free capacity and location (two
copies of the same object should not reside in the same data center and
definitely not on the same storage node).
* Assignments are compiled by the per-shark threads from the objects passed to
them by the assignment manager.  They are eventually flushed out under one of
two conditions: Either they have reached their maximum allowed size, or the
sharkspotter has finished reporting all objects on the storage node to be
evacuated.
* The rebalancer manager only performs one job at a time.

```
                          +-------------------------+
                          |    Assignment Manager   |
        Manager asks      |                         |  Request a list
        shark spotter     | * Obtain latest list of |  of storage nodes
        for all objects   |   qualified storage     |  as potential
        on a given        |   nodes from  picker.   |  evacuation
        storage node.     | * Select destination    |  destinations.    +------------------------+
        +-----------------+   storage node(s) from  +------------------>+        Picker          |
        |                 |   list based on space   |                   |                        |
+-------v-------+         |   and fault domain      | Picker responds   | Maintains a cache of   |
| Shark Spotter |         |   requirements.         | with most recently| eligible storage nodes |
+-------+-------+         | * Direct objects to     | obtained list.    | which is periodically  |
        |                 |   sharks based on       +<------------------+ refreshed.             |
        +---------------->+   selection criteria.   |                   +------------------------+
        Shark spotter     +-----------+-------------+
        answers with a                |                                 +------------+
        list of objects               |                                 |            |
        to evacuate.                  |                                 | +----------+-+ +---------->
                                      |                                 | |            |
                                      +-------------------------------->+-+ +----------+-+ +---------->
                                                 Send object(s) to        | | Per shark  |
                                                 per-shark threads        +-+ assignment |   +---------->
                                                 for processing as          | thread     | Each thread
                                                 an assignment.             +------------+ contacts the
                                                                                           agent running
                                                                                           on their
                                                                                           respective
                                                                                           shark to post
                                                                                           the newly created
                                                                                           assignment.
+-------------------------+           +-------------------------+
|         Checker         |           | Metadata Update Broker  |       +------------+
|                         |           |                         |       |            |
| * Periodically queries  |           | * Receives completed    |       | +----------+-+ +---------->
|   agents for status on  |           |   completed assignments |       | |            |
|   assignment progress.  +---------->+   from checker.         +------>+-+ +----------+-+ +---------->
| * Notifies Metadata     |           | * Spawns worker threads |         | | Metadata   |
|   Update Broker of all  |           |   that contact metadata |         +-+ Upate      |   +---------->
|   completed assignments |           |   tier to update object |           | Worker     | Each thread
|                         |           |   metadata.             |           +------------+ uses a moray
+-------------------------+           +-------------------------+                          client to update
                                                                             +             object metadata
                                                                             |  +          on whatever shard
                                                                             |  |  +       the object info
                                                                             |  |  |       resides.
                                                                             v  |  |
                                                                                v  |
                                                                                   v

                                                                        At the completion
                                                                        of the metadata update
                                                                        each worker updates
                                                                        locally stored records
                                                                        to reflect completion
                                                                        of the processing of
                                                                        each object.


```


## Build
To build the rebalancer-manager only:
```
cargo build --bin rebalancer-manager --features "postgres"
```
Note: When building the manager, it comes with two binary deliverables:
1. rebalancer-manager: This is the actual rebalancer manager service.
2. rebalancer-adm: Utility for interacting with the manager.
3. For release bits, include the `--release` flag when building.

## Usage

### rebalancer-adm
```

USAGE:
    rebalancer-adm [FLAGS] [SUBCOMMAND]

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

SUBCOMMANDS:
    help    Prints this message or the help of the given subcommand(s)
    job     Job Management

```

```
Job Management

USAGE:
   rebalancer-adm job [SUBCOMMAND]

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

SUBCOMMANDS:
    help      Prints this message or the help of the given subcommand(s)
    list      Get list of rebalancer jobs
    status    Get the status of a rebalancer job
```

```
Get the status of a rebalancer job

USAGE:
    remora job status <JOB_ID>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

ARGS:
    <JOB_ID>    UUID of job
```

## Manager Configuration Parameters
The rebalancer manager requires certain  service configuration parameters in
`etc/config.json`.  This file is populated by the config-agent using
`sapi_manifests/rebalancer/template` as a template.

### Service Parameters

| Param                | Type   | Description                        |
| -------------------- | ------ | ---------------------------------- |
| domain_name          | String | The domain name of the manta deployment.  From SAPI application metadata (`DOMAIN_NAME`). |
| shards               | Array  | The array of directory-api shards.  From SAPI application metadata `INDEX_MORAY_SHARDS`. |
 
## Development
Currently the rebalancer manager and rebalancer-adm rely on a postgres database
for maintaining records of known jobs and their status.  For this reason, it is
necessary to install postgres prior to any development and/or testing of the
rebalancer manager.

### Install Postgres
Installing postgres is not necessary if you are deploying a rebalancer image
as postgres will be setup as part of the image-build process.  If you would
like to run the rebalancer manager in a development zone with existing projects
though, installing postgres will be necessary.

```
pkgin install postgresql10
```

* Authentication configuration:
Optionally add the following line to your `/var/pgsql/data/pg_hba.conf`
```
# TYPE  DATABASE        USER            ADDRESS                 METHOD
local   all             postgres                                trust
```
or leave the default password of `postgres` in place.

* Start Postgres
```
svcadm enable -s postgresql
```

## Posting an evacuate job (POST /jobs)

Post a job (currently of action type "evacuate") to the manager.  In future
releases there will be other action types, however currently, the rebalancer
project evacuates objects from a given storage node.  The object supplied in the
request has different parts.  The section immediately following shows a JSON
representation of the structure as well as a table which details its consituent
parts.

### Inputs

```
{
  "action": {
    "Evacuate": {
      "from_shark": {
        "datacenter": "ruidc0",
        "manta_storage_id": "1.stor"
      },
      "domain_name": "fake.joyent.us",
      "max_objects": null
    }
  }
}
```

| Param      | Type                    | Description                                              |
| ---------- | ----------------------- | -------------------------------------------------------- |
| Evacuate(EvacuateJobPayload)  | [JobActionPayload](https://github.com/joyent/manta-rebalancer/blob/c61fdf438306a183b09337c738f1fb530b929749/src/manager.rs#L177-L180) | Contains all information required to perform an evacuation action. |
| from_shark | [MantaObjectShark](https://github.com/joyent/rust-libmanta/blob/864eadcb0ae2e7442ab61abc6243e64aeb754cbe/src/moray.rs#L119-L123) | Part of the evacuate job payload containing information about the shark to be evacuated. |
| domain_name | String (optional) | Domain name. |
| max_objects | u32 (optional) | Maximum number of objects to be processed (i.e. evacuated). |

### Responses
| Code | Description                                             |
| ---- | ------------------------------------------------------- |
| 200  | Action posted successfully + uuid of newly created job. |
| 400  | Bad request (mal-formed payload).                       |
| 500  | Internal server error.                                  |


## List Jobs (GET /jobs)

### Responses
| Code | Description                                                       |
| ---- | ----------------------------------------------------------------- |
| 200  | Successful request + list of all job uuid's                       |
| 500  | Internal server error: Error encoutered while obtaining job list. |

## Get Job (GET /jobs/uuid)
Given the path to a uuid, supply the job information (including status)
associated with it.

### Responses
| Code | Description                                                       |
| ---- | ----------------------------------------------------------------- |
| 200  | Successful request + job status (details below).                  |
| 400  | Bad request (invalid uuid).                                       |
| 500  | Internal server error.

### Job status
This is an aggregation of information across several structures maintained by
the rebalancer manager:

| Param       | Type   | Description                                    |
| ----------- | ------ | ---------------------------------------------- |
| Total       | usize  | Total number of objects to process in the job. |
| Unprocessed | usize  | Number of objects yet to be processed.         |
| Skipped     | usize  | Number of object skipped.                      |
| Error       | usize  | Number of errors encountered while processing the job. |
| Post Processing | usize | Number of objects currently undergoing post-processing (i.e. metadata tier update) |
| Complete | usize | Number of objects which have been successully processed completely. |


## Testing

### Testing certain modules
* To test only a certain module (including all of its submodules):
```
cargo test --features "postgres" <module name>
```

* To test only the REST API server:
```
cargo test manager --features "postgres"
```
