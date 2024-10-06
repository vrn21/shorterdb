fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_client(true) // Generate client code
        .build_server(true) // Generate server code
        .compile(&["proto/commands.proto"], &["proto"])?; // Path to your .proto file

    Ok(())
}
