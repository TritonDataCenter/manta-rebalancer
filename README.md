<!--
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
-->

<!--
    Copyright 2019, Joyent, Inc.
-->

# Manta Rebalancer 
This repository is part of the Joyent Manta Project.  For contribution
guidelines, issues and general documentation, visit the
[Manta](https://github.com/joyent/manta) project pages.

## Overview
The Manta Object Rebalancer is comprised of two main parts: a manager and an
agent.  Currently, the main function of the rebalancer is to evacuate objects
from an operator specified storage server (i.e. mako).  A rebalancer agent runs
on every mako in manta, while the rebalancer manager delegates work to various
agents in the form of something called "assignments".  An assignment is a list
containing information about objects for a given agent to download.  Through a
series of agent selections and assignments an entire storage node can be fully
evacuated.

For information in each piece of the project, please see:
* [Rebalancer Manager Guide](docs/manager.md)
* [Rebalancer Agent Guide](docs/agent.md)

## Basic Rebalancer Topology
```
                       Manager receives a
                       request to evacuate
                       all objects from a
                       given storage node.
                               +
                               |
                               v
+-----------+            +-----+------+                        +------------+
| Metadata  |            |            |   Assignment           |Storage Node|
|   Tier    |            |            |   {o1, o2, ..., oN}    |  +------+  |
| +-------+ |     +------+  Manager   +----------------------->+  |Agent |  |
| | Moray | |     |      |            |                        |  +------+  |
| |Shard 0| +<----+      |            |                        |            |
| +-------+ |     |      +------------+                        +-+---+---+--+
|           |     |                                              ^   ^   ^
| +-------+ |     |                                              |   |   |
| | Moray | |     |                                +-------------+   |   +-------------+
| |Shard 1| +<----+                                |o1               |o2               |oN
| +-------+ |     |                                |                 |                 |
|     .     |     |      +------------+      +-----+------+    +-----+------+    +-----+------+
|     .     |     |      |Storage Node|      |Storage Node|    |Storage Node|    |Storage Node|
| +-------+ |     |      |  +------+  |      |  +------+  |    |  +------+  |    |  +------+  |
| | Moray | |     |      |  |Agent |  |      |  |Agent |  |    |  |Agent |  |    |  |Agent |  |
| |Shard M| +<----+      |  +------+  |      |  +------+  |    |  +------+  |    |  +------+  |
| +-------+ |            |{o1, o2, oN}|      |            |    |            |    |            |
+-----------+            +-----+------+      +------------+    +------------+    +------------+
      ^                        ^
      |                        |             +                                                +
      +                        |             |                                                |
When all objects in            |             +-----------------------+------------------------+
an assignent have              |                                     |
been processed, the            +                                     v
manager updates the    Storage node to                     Objects in the assignment
metadata tier to       evacuate contains                   are retrieved from storage
reflect the new        objects:                            nodes other than the one
object locations.      {o1, o2, ..., oN}                   being evacuated.

```

## Build

### Binaries
Build release versions of `rebalancer-manager`, `rebalancer-agent`, and
`rebalancer-adm`:
```
make all
```

Build debug versions of `rebalancer-manager`, `rebalancer-agent`, and
`rebalancer-adm`:
```
make debug
```

For specific instructions on building individual parts of the project, please
review the instructions in their respective pages (listed above).

### Images
Information on how to building Triton/Manta components to be deployed within
an image please see the [Developer Guide for Building Triton and Manta](https://github.com/joyent/triton/blob/master/docs/developer-guide/building.md#building-a-component)


### Pre-integration
Before integration of a change to any part of the rebalancer, the following
procedures must run cleanly:run `fmt`, `check`, `test`, and
[clippy](https://github.com/rust-lang/rust-clippy):
```
cargo fmt -- --check
make check
make test
```

Note: On the `cargo fmt -- --check`, this will display any lines that *would*
be impacted by an actual run of `cargo fmt`.  It is recommended to first
evaluate the scope of the change that format *would* make.  If it's the case
that the tool catches long standing format errors, it might be desirable to
address those in a separate change, otherwise a reviewer may have trouble
determing what is related to a current change and what is cosmetic, historical
clean up.

