#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright 2019, Joyent, Inc.
#

all: check doc
	cargo build --bin rebalancer-agent --release
	cargo build --bin rebalancer-manager --features "postgres" --release
	cargo build --bin rebalancer-adm --features "postgres" --release
	cp src/config.json target/release/

debug:
	cargo build --bin rebalancer-agent
	cargo build --bin rebalancer-manager --features "postgres"
	cargo build --bin rebalancer-adm --features "postgres"
	cp src/config.json target/debug/

doc:
	cargo doc --features "postgres"

clean:
	cargo clean 

check: 
	cargo clippy --features "postgres"
	cargo check --features "postgres"

jobtests:
	RUST_LOG=remora=trace cargo test job --features "postgres" -- --test-threads=1

agenttests:
	RUST_LOG=remora=trace cargo test agent --features "postgres"

test: agenttests jobtests
