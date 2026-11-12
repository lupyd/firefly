fn main() {
    std::process::exit(firefly_mcp::cli::run(firefly_mcp::cli::Defaults { name: "firefly-mcp", ..Default::default() }));
}
