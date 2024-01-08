use std::error::Error;
use std::ffi::c_void;

use blosc2_src::{
    blosc2_create_dctx, blosc2_decompress_ctx, blosc2_free_ctx, BLOSC2_DPARAMS_DEFAULTS,
};
// use futures::stream::StreamExt;
// use itertools::iproduct;

const ZARR_URL: &str = "https://zarr.world/hrrr-analysis-TMPonly-2023-06-chunks360x240x240.zarr";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let original_client = reqwest::Client::new();

    let compressed_chunk_bytes = get_chunk_bytes(original_client.clone(), 1, 1, 1).await?;

    // TODO use blosc1_cbuffer_sizes to get # of decompressed bytes to allocate https://www.blosc.org/c-blosc2/reference/blosc1.html#blosc2_8h_1a892258a6289fffd86744dbb70491517f
    let mut decompressed_chunk_bytes = vec![0_u8; 100000000];

    unsafe {
        let dparams = BLOSC2_DPARAMS_DEFAULTS;
        let decompress_context = blosc2_create_dctx(dparams);
        dbg!(dparams);
        dbg!(decompress_context);
        dbg!(compressed_chunk_bytes.len());
        dbg!(decompressed_chunk_bytes.len());

        let num_bytes_decompressed = blosc2_decompress_ctx(
            decompress_context,
            compressed_chunk_bytes.as_ptr() as *const c_void,
            compressed_chunk_bytes.len().try_into()?,
            decompressed_chunk_bytes.as_ptr() as *mut c_void,
            decompressed_chunk_bytes.len().try_into()?,
        );

        // Don't need this once we allocate exactly as much as we need
        decompressed_chunk_bytes.truncate(num_bytes_decompressed.try_into()?);

        // TODO now re-interpret these bytes as float16s

        dbg!(num_bytes_decompressed);

        blosc2_free_ctx(decompress_context);
    }

    return Ok(());

    // let futures = iproduct!(0..2, 0..5, 0..8).map(|(x, y, z)| {
    //     let client = original_client.clone(); // clone a copy which can be `move`d into future
    //     async move { get_chunk_bytes(client, x, y, z) }
    // });

    // let results = futures::stream::iter(futures)
    //     .buffer_unordered(1000) // max n concurrent requests
    //     .collect::<Vec<_>>() // collect results into a vector
    //     .await;

    // dbg!(results.len());

    // Ok(())
}

async fn get_chunk_bytes(
    client: reqwest::Client,
    x: i64,
    y: i64,
    z: i64,
) -> Result<bytes::Bytes, reqwest::Error> {
    let chunk_str = format!("{}.{}.{}", x, y, z);
    println!("starting {}...", chunk_str);

    let chunk_bytes = client
        .get(format!("{}/TMP/{}", ZARR_URL, chunk_str))
        .send()
        .await?
        .bytes()
        .await?;

    println!("    done {}", chunk_str);
    Ok(chunk_bytes)
}
