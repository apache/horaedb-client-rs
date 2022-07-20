SHELL = /bin/bash

DIR=$(shell pwd)

fmt:
	cd $(DIR); cargo fmt -- --check

clippy:
	cd $(DIR); cargo clippy --all-targets --all-features -- -D warnings

test:
	cd $(DIR); cargo test --workspace -- --test-threads=4
