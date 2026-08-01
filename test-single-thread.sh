#!/bin/bash
# Script to run cargo tests sequentially (single thread) to avoid username/database conflicts
export EMULATOR_MODE=true
export RUST_LOG=info
cargo test -- --test-threads=1 "$@"
