.PHONY: build test bench bench-baseline bench-compare fmt clippy clean

build:
	cargo build --workspace

test:
	cargo test --workspace 2>&1

bench:
	cargo bench --workspace -- --output-format bencher | tee target/bench-current.txt

bench-baseline:
	cargo bench --workspace -- --save-baseline main

bench-compare:
	cargo bench --workspace -- --baseline main \
		| awk '/regressed/{found=1} END{exit found}'

fmt:
	cargo fmt --all

clippy:
	cargo clippy --workspace -- -D warnings

clean:
	cargo clean
