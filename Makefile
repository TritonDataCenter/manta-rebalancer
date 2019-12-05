all: check doc test
	cargo build --bin rebalancer-agent
	cargo build --bin rebalancer-manager --features "postgres"
	cp src/config.json target/debug/

agent:
	cargo build --bin rebalancer-agent

manager:
	cargo build --bin rebalancer-manager --features "postgres"
       
doc:
	cargo doc --features

clean:
	cargo clean 

check: 
	cargo clippy --features "postgres"
	cargo check --features "postgres"

jobtests:
	cargo test job -- --test-threads=1

test:
	RUST_LOG=remora=trace cargo test -- --nocapture
