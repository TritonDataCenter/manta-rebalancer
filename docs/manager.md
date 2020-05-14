<!--
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
-->

<!--
    Copyright 2020 Joyent, Inc.
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
        on a given        |   nodes from storinfo.  |  evacuation
        storage node.     | * Select destination    |  destinations.    +------------------------+
        +-----------------+   storage node(s) from  +------------------>+        Storinfo        |
        |                 |   list based on space   |                   |                        |
+-------v-------+         |   and fault domain      | Storinfo responds | Maintains a cache of   |
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


## rebalancer-adm
A command line utility has been created to make managing the rebalacner more
convenient.  Currently, it can get a single job, list the uuids of all known
jobs as well as create new ones.  As the functionality of the rebalancer
expands, rebalancer-adm will too in order to meet the needs of the operator.

### Usage
```
rebalancer-adm 0.1.0
Rebalancer client utility

USAGE:
    rebalancer-adm [SUBCOMMAND]

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

SUBCOMMANDS:
    help    Prints this message or the help of the given subcommand(s)
    job     Job operations

```

### Job Operations
```
Job operations

USAGE:
    rebalancer-adm job [SUBCOMMAND]

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

SUBCOMMANDS:
    create    Create a rebalancer job
    get       Get information on a specific job
    help      Prints this message or the help of the given subcommand(s)
    list      List all known rebalancer jobs

```

### Get the status of a rebalancer job
```
rebalancer-adm job get --uuid <uuid>
```

### List all known jobs
```
rebalancer-adm job list
```

All output is displayed in JSON format.  The output below is the result of a
`job list` request:
```
[
  {
    "action": "Evacuate",
    "id": "9d5e4b18-cdec-440c-88fa-64f6c49ea814",
    "state": "Setup"
  },
  {
    "action": "Evacuate",
    "id": "bbd4088d-fec9-4875-9aa4-d1ca43a21c93",
    "state": "Setup"
  },
  {
    "action": "Evacuate",
    "id": "1090b9de-d03c-4082-8a61-8637193ff829",
    "state": "Setup"
  }
]

```

Note: One current shortcoming of the tool is that it can not display results
in tabular format, however, it is still possible to format the output to your
liking via `jq` (a command line utility which processes JSON data):

```
[root@a422f03b-f17e-62fc-d3d5-eacc548e8a8a]#  rebalancer-adm job list | jq -r '.[] | "\(.action)\t\(.id)\t\(.state)"'
Evacuate        0c89c985-3e79-4011-b7e3-191c09b074af    Complete
Evacuate        5a2ddd94-c52c-491d-9ca8-8f80840712dc    Failed
Evacuate        e13e6181-ca7c-486c-8ece-3b129052a482    Setup
Evacuate        1e80ca29-816e-4dd1-a6c8-418e050e5c22    Failed
Evacuate        964e4031-8e90-4244-b4e6-826e98a6236f    Setup
Evacuate        9d25d63f-b659-4633-b692-8b4402e57c8e    Setup
Evacuate        611ce930-8f14-470d-b271-3044c6cf782a    Setup
Evacuate        c17370f4-33b3-4e5b-b3d7-04a0ec9bbd9a    Complete
```

### Create a new job
To see a list of all currently supported jobs:
```
rebalancer-adm help job create
```

Create an evacuate job:
```
rebalancer-adm job create evacuate --shark=<storage server name> [--max_objects=<maximum number of ojects]
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
| listen_port | u16 | Optionally specify a port to listen on.  Default 80.|
 
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
    "action": "evacuate",
    "params": {
        "from_shark": "1.stor"
    }
}
```

| Param      | Type                    | Description                                              |
| ---------- | ----------------------- | -------------------------------------------------------- |
| action | [JobAction](https://github.com/joyent/manta-rebalancer/blob/b5fe811bd53813e4051aef282cf80853fb5af434/manager/src/jobs/mod.rs#L208-L211) | The action that this job will take. |
| params | Object | Parameters unique to each job action |


#### Evacuate Job Parameters
| Param      | Type                    | Description                                              |
| ---------- | ----------------------- | -------------------------------------------------------- |
| from_shark | String | The hostname of the shark to evacuate objects from. |


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

## Internals

### Database Schema
The rebalancer manager saves information about the state of all rebalance
 jobs in a local database.

#### `jobs` Table
| Column  | Type | Description  |
|---|---|---|
| id | TEXT | Job UUID |
| action | TEXT(enum) | JobAction |
| state | TEXT(enum) | JobState |


### `evacuateobjects` Table
| Column  | Type | Description  |
|---|---|---|
| id  | TEXT  | UUID of object  |
| assignment_id | TEXT  | UUID of assignment |
| object  | JSONB | JSON blob of object metadata  |
| shard | INTEGER| shard number  |
| dest_shark | TEXT | shark(mako) hostname|
| etag | TEXT | object etag for putting metadata back to moray |
| status | TEXT(enum)  | EvacuateObjectStatus |
| skipped_reason | TEXT(enum)(nullable)  | ObjectSkippedReason  |
| error | TEXT(enum)(nullable)  | EvacuateObjectError |
