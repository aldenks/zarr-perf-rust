use std::ffi::c_void;
use std::{error::Error, num::TryFromIntError};

use blosc2_src::{
    blosc1_cbuffer_sizes, blosc2_create_dctx, blosc2_decompress_ctx, blosc2_free_ctx,
    BLOSC2_DPARAMS_DEFAULTS,
};
use half::f16;
use itertools::{iproduct, Itertools};

const HRRR_2023_06_TMP_URL: &str =
    "https://zarr.world/hrrr-analysis-TMPonly-2023-06-chunks360x240x240.zarr";
type ElementType = f16;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut set = tokio::task::JoinSet::new();
    let original_client = reqwest::Client::new();

    iproduct!(0..2, 0..5, 0..8)
        .map(|(x, y, z)| {
            let client = original_client.clone(); // clone a copy which can be `move`d into future
            set.spawn(get_chunk_partial_mean_unwrapped(client, x, y, z));
        })
        .collect_vec();

    let mut sum: f64 = 0.;
    let mut count: u64 = 0;
    while let Some(Ok(partial_mean)) = set.join_next().await {
        sum += partial_mean.sum;
        count += partial_mean.count;
        // dbg!(partial_mean);
    }
    dbg!(sum, count, sum / count as f64);

    Ok(())
}

#[derive(Debug)]
struct PartialMean {
    sum: f64,
    count: u64,
}

async fn get_chunk_partial_mean_unwrapped(
    client: reqwest::Client,
    x: u64,
    y: u64,
    z: u64,
) -> PartialMean {
    let compressed_bytes = get_chunk_bytes(client, HRRR_2023_06_TMP_URL, x, y, z)
        .await
        .expect("get bytes error");

    let res = tokio::task::spawn_blocking(move || {
        let values = decompress_chunk(&compressed_bytes)?;
        let sum = values
            .iter()
            .cloned()
            .fold(0 as f64, |sum, x| sum + f64::from(x));

        let count = values.len().try_into()?;

        Ok::<PartialMean, TryFromIntError>(PartialMean { sum, count })
    })
    .await;

    res.expect("join error").expect("more")
}

fn decompress_chunk(compressed_bytes: &bytes::Bytes) -> Result<Vec<ElementType>, TryFromIntError> {
    let (decompressed_len, _compressed_len, _) = blosc_buffer_sizes(compressed_bytes);

    // Allocate buffer to decompress into
    let value_size = std::mem::size_of::<ElementType>();
    assert_eq!(decompressed_len % value_size, 0);
    let values = vec![ElementType::NAN; decompressed_len / value_size];

    let actual_decompressed_len_or_err_code = unsafe {
        let decompress_context = blosc2_create_dctx(BLOSC2_DPARAMS_DEFAULTS);
        let actual_decompressed_len_or_err_code = blosc2_decompress_ctx(
            decompress_context,
            compressed_bytes.as_ptr() as *const c_void,
            compressed_bytes.len().try_into()?,
            values.as_ptr() as *mut c_void,
            // The max number of bytes to write to `values` -- especially dangerous if this is incorrect.
            // This expression is equal to `decompressed_bytes` but calc it from the values vec explicitly
            // to reduce the chance a later change in vec allocating above could introduce a bug.
            (values.len() * std::mem::size_of_val(&values[0])).try_into()?,
        );
        blosc2_free_ctx(decompress_context);
        actual_decompressed_len_or_err_code
    };

    assert!(
        actual_decompressed_len_or_err_code
            == i32::try_from(decompressed_len).expect("decompressed len too long for i32")
    );
    // if actual_decompressed_len_or_err_code != i32::try_from(decompressed_len)? {
    //     return Err(format!(
    //         "Decompression error code {}",
    //         actual_decompressed_len_or_err_code
    //     )
    //     .into());
    // } else {
    //     let decompressed_mb = decompressed_len as f64 / (1000 * 1000) as f64;
    //     let compressed_mb = _compressed_len as f64 / (1000 * 1000) as f64;
    //     println!(
    //         "Decompressed to {:.1}mb from {:.1}mb ({:.1}x compression)",
    //         decompressed_mb,
    //         compressed_mb,
    //         decompressed_mb / compressed_mb
    //     )
    // }

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
    url: &str,
    x: u64,
    y: u64,
    z: u64,
) -> Result<bytes::Bytes, reqwest::Error> {
    let chunk_str = format!("{}.{}.{}", x, y, z);
    println!("starting {}...", chunk_str);

    let chunk_bytes = client
        .get(format!("{}/TMP/{}", url, chunk_str))
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;

    println!("    done {}", chunk_str);
    Ok(chunk_bytes)
}
