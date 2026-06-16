fn main() {
    let out_dir = String::from("src/protos");
    let protobufs_directory = String::from("protobufs");
    let proto_filenames = ["message.proto"];

    let input_files = proto_filenames.map(|x| protobufs_directory.clone() + "/" + x);

    let config =
        pb_rs::ConfigBuilder::new(&input_files, None, Some(&out_dir), &[protobufs_directory])
            .unwrap()
            .custom_struct_derive(vec![]);
    pb_rs::types::FileDescriptor::run(&config.build()).unwrap();

    println!("cargo:rerun-if-changed=build.rs");
    for input_file in input_files {
        println!("cargo:rerun-if-changed={}", input_file);
    }
}
