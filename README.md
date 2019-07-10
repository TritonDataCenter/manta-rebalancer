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

## Development

Before integration run `fmt`, `check`, `test`, and
[clippy](https://github.com/rust-lang/rust-clippy):
```
cargo fmt
cargo check
cargo clippy
cargo test
```
