fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=EMULATOR_MODE");
    let em_mode = std::env::var("EMULATOR_MODE").unwrap_or("false".to_string());
    println!("cargo::rustc-check-cfg=cfg(emulator_mode, values(\"false\", \"true\"))",);
    println!("cargo:rustc-cfg=emulator_mode=\"{}\"", em_mode);
}
