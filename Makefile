SHELL = /bin/bash

fmt:
	cargo fmt -- --check

clippy:
	cargo clippy --all-targets --all-features -- -D warnings
