use futures::stream::StreamExt;
use itertools::iproduct;

const ZARR_URL: &str = "https://zarr.world/hrrr-analysis-TMPonly-2023-06-chunks360x240x240.zarr";

#[tokio::main]
async fn main() -> Result<(), reqwest::Error> {
    let original_client = reqwest::Client::new();

    let futures = iproduct!(0..2, 0..4, 0..5).map(|(x, y, z)| {
        let client = original_client.clone(); // clone a copy which can be `move`d into future

        async move {
            let chunk_str = format!("{}.{}.{}", x, y, z);
            println!("starting {}...", chunk_str);

            let chunk_bytes = client
                .get(format!("{}/TMP/{}", ZARR_URL, chunk_str))
                .send()
                .await?
                .bytes()
                .await?;

            println!("    done {}", chunk_str);
            Ok::<bytes::Bytes, reqwest::Error>(chunk_bytes)
        }
    });

    let results = futures::stream::iter(futures)
        .buffer_unordered(10) // max 10 concurrent requests
        .collect::<Vec<_>>() // collect results into a vector
        .await;

    dbg!(results.len());

    Ok(())
}
