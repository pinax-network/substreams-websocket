fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .build_server(false)
        .compile_protos(
            &[
                "proto/sf/substreams/rpc/v3/service.proto",
                "proto/sf/substreams/rpc/v2/service.proto",
                "proto/sf/substreams/v1/package.proto",
                "proto/sf/substreams/v1/modules.proto",
                "proto/sf/substreams/v1/clock.proto",
                "proto/sf/substreams/v1/deltas.proto",
                "proto/dex/swaps/v1/dex-swaps.proto",
                "proto/solana/spl/token/v1/spl-token.proto",
                "proto/sf/substreams/sink/database/v1/database.proto",
            ],
            &["proto"],
        )?;

    Ok(())
}
