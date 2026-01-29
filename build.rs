use protobuf_codegen::Codegen;

fn main() {
    Codegen::new()
        .pure()
        .includes(&["proto"])
        .input("proto/gtfs-realtime.proto")
        .cargo_out_dir("protos")
        .run_from_script();
}
