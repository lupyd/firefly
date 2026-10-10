#!/usr/bin/env bash
set -e
wasm-pack build crates/client-node --target web --out-dir=pkg
