SHELL = /bin/bash

DIR=$(shell pwd)

fmt:
	cd $(DIR); cargo fmt --all --check

clippy:
	cd $(DIR); cargo clippy --tests --all-features --all-targets --workspace -- -D warnings

test:
	cd $(DIR); cargo test --workspace

check-toml:
	cd $(DIR); cargo sort --workspace --check

dry-run:
	cd $(DIR); cargo publish --dry-run --registry crates-io
