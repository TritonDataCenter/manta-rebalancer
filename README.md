# remora 
The [Manta](https://github.com/joyent/manta) object rebalancer manager and agent.


## Build
```
make
```

## Usage
```
remora

USAGE:
    remora [FLAGS] [SUBCOMMAND]

FLAGS:
    -a, --agent      Run in agent mode
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

## Configuration Parameters
The remora zone leverages configuration parameters in `src/config.json`.  This
file is populated by the config-agent using `src/config.json.in` as a template.

*Parameters:*
* sapi_url<String>: The url of the SAPI zone.  Populated by the manta deployment zone.
* domain_name<String>: The domain name of the manta deployment.  From SAPI application
metadata (`DOMAIN_NAME`).
* database_url<String>: Location and name of the zone's local database.
* database_buffer_size<uint>:  Writes to the database are buffered by up to this
number of records.
* shards<Array>: The first and last shard.  From SAPI application metadata `INDEX_MORAY_SHARDS`.


## Development

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