<!--
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
-->

<!--
    Copyright 2019, Joyent, Inc.
-->

# Agent
The [Manta](https://github.com/joyent/manta) object rebalancer agent is a
subsystem within the
[Rebalancer project](https://github.com/joyent/manta-rebalancer) which is
orchestrated by the rebalancer manager.

# Overview
The rebalancer agent is a service which runs on every storage node in a Manta
deployment and its actions are orchestrated by a per-region manager referred to
as the rebalancer "manager".  Its primary role is to handle requests containing
an assignment posted by the manager.

## Build
```
make
```

## Usage
```
rebalancer-agent

USAGE:
    rebalancer-agent [FLAGS]

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

```

## Configuration Parameters
Currently, the rebalancer agent does not require any configuration parameters.

## Development

Before integration run `fmt`, `check`, `test`, and
[clippy](https://github.com/rust-lang/rust-clippy):
```
cargo fmt
cargo check
cargo clippy
cargo test agent -- --nocapture | bunyan
```

## Testing

### Execution
As discussed in the development section above, anytime that a change is made to
the agent code (or any subsystem that it directly consumes), at a minimum, a
clean test run of all agent tests is necessary.

Current sanity checks include:
* Download an object (without any problems).
* Replace an existing object.
* Attempt to download an object that is not present on the source.
* Downloaded object fails checksum verification.
* Attempt to send the agent an assignment which it has already received.

Note: It is worth mentioning that when the agent unit test suite is started,
all tests actually run in parallel.  All existing tests use the same instance
of the rebalancer agent which is declared (lazy static, guarded by a mutex),
accessible to all threads running a test.  It is possible for individual threads
to create additional instances of an agent, accessible only to that thread, if
the agent that is globally accessible does not meet the needs of the test.  The
primary reason for having additional agents would be if the developer wanted an
agent that processed tasks in a different way than the way it would in
production.  For example, an agent that blindly always fails tasks might be
necessary if we are testing how the agent (or a client of the agent handles
failure scenarios).

### Development
When developing test automation for the agent (or for the rebalancer manager,
where an instance of an agent is required), there is an easy way to create of an
agent, running within the same process as the test code itself:

```rust
use crate::util::test::{get_progress, send_assignment_impl};
use gotham::test::TestServer;

// Function that treats all tasks as being successful.
fn always_pass(task: &mut Task) {
    task.set_status(TaskStatus::Complete);
    return;
}

// test_server is a variable of type TestServer, returned by router()
let test_server = router(always_pass);

// Now to send an assignment to the newly created agent.
let uuid = Uuid::new_v4().to_hyphenated().to_string();
let mut tasks = Vec::new();

// Populate the vector (tasks) with as many tasks as you'd like.
[..]

// Send the assignment to the agent
send_assignment_impl(&tasks, &uuid, &test_server, StatusCode::OK);

[..]
//wait some period of time
[..]

// Ask the agent for a status update.  This is basically the same thing as issuing
// a GET to http://<shark>/assignments/<uuid>.
let assignment = get_progress(uuid, &test_server);
```

Note: The above example will create a version of an agent that will declare
all tasks in an assignment that it receives as having passed.  To create an
instance of an agent where it processes tasks the it would in production, it
is a simple matter of supplying a different function for `process_task()`:

```rust
let test_server = router(crate::agent::process_task);
```

For more examples of how agent tests are currently implemented, see the unit
test section in `src/agent.rs`.

## Send assignment (POST /assignments)

Sends an assignment to the agent.  Assignments are processed sequentially, in
the order in which they are received.  An assignment posted to the agent is
comprised of the following:

### Inputs
| Param           | Type   | Description                     |
| --------------- | ------ | ------------------------------- |
| Assignment uuid | String | Unique identifier of assignment |
| Task list       | Array  | Array of [Tasks](https://github.com/joyent/manta-rebalancer/blob/77a5d01f182261f9842cb00134bd55ef1e280afc/src/jobs/mod.rs#L139-L148) |

### Responses
| Code | Description                                            |
| ---- | ------------------------------------------------------ |
| 200  | Assignment posted successfully                         |
| 400  | Bad request (mal-formed assignment)                    |
| 409  | Conflict (assignment by specified uuid already exists) |


### Example
Below is a sample of the payload supplied in a request by the manager to post an
assignment to the agent:


```
POST /assignments -d '[
  "463ec933-1d31-41f9-8e76-0db3191f6346",
  [
    {
      "object_id": "7f3ee78a-2e64-4f3d-829f-a31c7c2c2b03",
      "owner": "d50c4fc4-f408-492f-b8bc-a0dd7c73683f",
      "md5sum": "QXBlX0QFcscVIwptkUaI8g==",
      "source": {
        "datacenter": "robert-dc",
        "manta_storage_id": "3.stor.us-west.joyent.us"
      },
      "status": "Pending"
    }
  ]
]'
```

Note: The `status` property of each task is optional when posting and will
default to `"Pending"`.

The assignment above has an id of `463ec933-1d31-41f9-8e76-0db3191f6346` and a
list containing only one task representing a single object that the agent should
download and store locally under the directory
`/manta/d50c4fc4-f408-492f-b8bc-a0dd7c73683f`.  According to the task
information, the object to download is located on `3.stor.us-west.joyent.us`.
Also notice that there is checksum information for each object as well so that
at the completion of the download, data integrity can be verified.

The above request is sent to the following endpoint called `/assignments` on the
agent.  This can also be done via command line as follows:

```
curl --header "Content-Type: application/json" --request POST \
--data '[
  "463ec933-1d31-41f9-8e76-0db3191f6346",
  [
    {
      "object_id": "7f3ee78a-2e64-4f3d-829f-a31c7c2c2b03",
      "owner": "d50c4fc4-f408-492f-b8bc-a0dd7c73683f",
      "md5sum": "QXBlX0QFcscVIwptkUaI8g==",
      "source": {
        "datacenter": "dc",
        "manta_storage_id": "3.stor.us-west.joyent.us"
      },
      "status": "Pending"
    }
  ]
]' http://localhost:7878/assignments
```

Note: The above should only be used for debugging purposes as relocating an
object to a new storage node also necessitates an update to the metadata tier
which is not done by the agent, but by the rebalancer manager.

## Get Assignment (GET /assignments/uuid)
Returns JSON object representing an assignment as seen by the agent.

### Responses
| Code | Description                                               |
| ---- | --------------------------------------------------------- |
| 200  | A valid assignment with the uuid supplied has been found  |
| 400  | Invalid request:  Correct end point, but mal-formed uuid  |
| 404  | Assignment not found at the requested location            |

### Examples
GET /assignments/77ed8169-a59f-4d0b-a9e8-1af8a3a3c4cf

Where `77ed8169-a59f-4d0b-a9e8-1af8a3a3c4cf` sustained no failures, would look
something like this:

```
{
  "uuid": "77ed8169-a59f-4d0b-a9e8-1af8a3a3c4ce",
  "stats": {
    "state": {
      "Complete": null
    },
    "failed": 0,
    "complete": 1,
    "total": 1
  }
}
```

In the above example, we can see that the assignment contained a total of 1
task and that in the course of processing the assignment, 0 failures were
sustained.  If (for some reason) a particular task that is part of an assignment
fails, the object and reason associated with the failure will be described in
the information returned in the response to the GET request:

```
{
  "uuid": "fd45e70b-5435-457e-a371-93fcb8215e0d",
  "stats": {
    "state": {
      "Complete": [
        {
          "object_id": "7f3ee78a-2e64-4f3d-829f-a31c7c2c2b03",
          "owner": "d50c4fc4-f408-492f-b8bc-a0dd7c73683f",
          "md5sum": "QXBlX0QFcscVIwptkUaI8g==",
          "source": {
            "datacenter": "dc",
            "manta_storage_id": "3.stor.us-west.joyent.us"
          },
          "status": {
            "Failed": {
              "Failed": "MD5Mismatch"
            }
          }
        }
      ]
    },
    "failed": 1,
    "complete": 1,
    "total": 1
  }
}
```

As you can see in the above response, the assignment
`f10460aa-b142-444f-8a97-54bcbdc73bc6` contains a single task which happened to
fail.  The failure suggests that downloading account/object
`d50c4fc4-f408-492f-b8bc-a0dd7c73683f/7f3ee78a-2e64-4f3d-829f-a31c7c2c2b03`
failed and the reason supplied in the status block is `MD5Mismatch`, that is
the object downloaded failed checksum verification.  This is not to be confused
with the overall status code of the `GET` request which was 200 since the
assignment by the supplied uuid was indeed located.


## Task Status
The agent processes tasks within a given assignment sequentially.  There are
several different states that a task can be in during the course of processing
an overall assignment.  Below is a table detailing each:

| Value    | Description                                               |
| -------- | --------------------------------------------------------- |
| Pending  | The task has not been processed yet                       |
| Running  | The task is currently being processed                     |
| Complete | The agent finished processing the task *successfully*     |
| Failed   | The agent finished processing the task but it *failed*    |

In the case of tasks that have failed, the agent will report all failures in
the body of the response to a `GET` request, detailing the owner, object,
storage node, checksum information and finally, the specific reason for the
failure.  Depending on the reason for the failure, the rebalancer manager may
elect to retry the task as part of another assignment, or require operator
intervention in a situation where retrying is not programmatically possible
right now.
