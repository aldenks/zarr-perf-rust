use std::error::Error;
use std::ffi::c_void;

use blosc2_src::{
    blosc1_cbuffer_sizes, blosc2_create_dctx, blosc2_decompress_ctx, blosc2_free_ctx,
    BLOSC2_DPARAMS_DEFAULTS,
};
use half::f16;
// use futures::stream::StreamExt;
// use itertools::iproduct;

const ZARR_URL: &str = "https://zarr.world/hrrr-analysis-TMPonly-2023-06-chunks360x240x240.zarr";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let original_client = reqwest::Client::new();

    let compressed_bytes = get_chunk_bytes(original_client.clone(), 1, 1, 1).await?;

    let chunk_values = decompress_chunk(&compressed_bytes)?;

    let chunk_sum = chunk_values
        .iter()
        .cloned()
        .fold(0 as f64, |sum, x| sum + f64::from(x));
    let chunk_mean = chunk_sum / chunk_values.len() as f64;
    dbg!(chunk_mean);

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

fn decompress_chunk(compressed_bytes: &bytes::Bytes) -> Result<Vec<f16>, Box<dyn Error>> {
    let (decompressed_len, compressed_len, _) = blosc_buffer_sizes(compressed_bytes);

    // Allocate buffer to decompress into
    let value_size = std::mem::size_of::<f16>();
    let values = vec![f16::NAN; decompressed_len / value_size];

    let actual_decompressed_len_or_err_code = unsafe {
        let decompress_context = blosc2_create_dctx(BLOSC2_DPARAMS_DEFAULTS);
        let actual_decompressed_len_or_err_code = blosc2_decompress_ctx(
            decompress_context,
            compressed_bytes.as_ptr() as *const c_void,
            compressed_bytes.len().try_into()?,
            values.as_ptr() as *mut c_void,
            (values.len() * value_size).try_into()?,
        );
        blosc2_free_ctx(decompress_context);
        actual_decompressed_len_or_err_code
    };

    if actual_decompressed_len_or_err_code != i32::try_from(decompressed_len)? {
        return Err(format!(
            "Decompression error code {}",
            actual_decompressed_len_or_err_code
        )
        .into());
    } else {
        let decompressed_mb = decompressed_len as f64 / (1000 * 1000) as f64;
        let compressed_mb = compressed_len as f64 / (1000 * 1000) as f64;
        println!(
            "Decompressed to {:.1}mb from {:.1}mb ({:.1}x compression)",
            decompressed_mb,
            compressed_mb,
            decompressed_mb / compressed_mb
        )
    }

    Ok(values)
}

fn blosc_buffer_sizes(compressed_bytes: &bytes::Bytes) -> (usize, usize, usize) {
    let (mut decompressed_len, mut compressed_len, mut blosc_block_len) = (0, 0, 0);
    unsafe {
        blosc1_cbuffer_sizes(
            compressed_bytes.as_ptr() as *const c_void,
            // These 3 values are "returned" by this function
            &mut decompressed_len,
            &mut compressed_len,
            &mut blosc_block_len,
        );
    }
    (decompressed_len, compressed_len, blosc_block_len)
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
