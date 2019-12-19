<!--
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
-->

<!--
    Copyright 2019, Joyent, Inc.
-->

# Manta Rebalancer 

This repository is part of the Joyent Manta project.  For contribution
guidelines, issues, and general documentation, visit the main
[Manta](http://github.com/joyent/manta) project page.

This repository contains sources for the Manta object rebalancer manager and
[agent](https://github.com/joyent/manta-rebalancer/blob/docs/docs/agent.md).

## Build
```
make all release publish buildimage
```

or

```
make agent pkg_agent
```

## Usage
```
remora

USAGE:
    remora [FLAGS] [SUBCOMMAND]

FLAGS:
    -h, --help       Prints help information
    -s, --server     Run in server mode
    -V, --version    Prints version information

SUBCOMMANDS:
    help    Prints this message or the help of the given subcommand(s)
    job     Job Management

```

```
Job Management

USAGE:
    remora job [SUBCOMMAND]

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

SUBCOMMANDS:
    create    Create a rebalancer Job
    help      Prints this message or the help of the given subcommand(s)

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

## Configuration Parameters
The remora zone leverages configuration parameters in `src/config.json`.  This
file is populated by the config-agent using `src/config.json.in` as a template.

*Parameters:*
* domain_name<String>: The domain name of the manta deployment.  From SAPI application
metadata (`DOMAIN_NAME`).
number of records.
* shards<Array>: The first and last shard.  From SAPI application metadata `INDEX_MORAY_SHARDS`.


### Pre-integration
Before integration run `fmt`, `check`, `test`, and
[clippy](https://github.com/rust-lang/rust-clippy):
```
cargo fmt
cargo check
cargo clippy
cargo test
```


## Testing

There is a certain flavor of the rebalancer agent that allow for more convenient
testing of the zone -- namely one that receives a (properly formed) assignment
and blindly marks all tasks within it as "Complete" instead of actually doing
the leg work of processing each task.  This is intended for scenarios where
functional verification of the happy path in the rebalancer zone.  As this
project evolves, other modes will likely be introduced.

To build the version of the agent described above, a special flag must be
passed to the compiled to enable the feature:

```
cargo build --features "always_pass"
```

Note: By default, this feature will never be enabled.

### Testing certain modules
* To test only a certain module (including all of its submodules:
```
cargo test <module name>
```

* To test only the REST API server:
```
cargo test --bin server
```

