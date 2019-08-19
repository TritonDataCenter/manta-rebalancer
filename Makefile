all: check doc test
	cargo build
	cp src/config.json target/debug/

doc:
	cargo doc

clean:
	cargo clean 

check: 
	cargo clippy
	cargo check

test:
	RUST_LOG=remora=trace cargo test -- --nocapture
