use std::sync::Arc;

use aws_config;
use aws_sdk_s3::config::BehaviorVersion;
use aws_sdk_s3::Client;
use memmap2::MmapMut;
use s3::{fetch_to_mem, list_objects};
use tokio::time::Instant;

mod s3;

const ITERATIONS: usize = 1;
const N_OBJECTS: usize = 50;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    tracing_subscriber::fmt::init();
    let config = aws_config::defaults(BehaviorVersion::latest()).load().await;

    let objects = list_objects(
        Arc::new(Client::new(&config)),
        "s3-bench-test-bucket-ps".to_string(),
        "test_file",
    )
    .await?[..N_OBJECTS]
        .to_vec();

    // allocate memory for the objects
    const MEM_BUFFER_SIZE: usize = N_OBJECTS * (1 << 30);
    let mut buffer = MmapMut::map_anon(MEM_BUFFER_SIZE).unwrap();

    tracing::info!(
        "[+] Allocated {}GB of memory",
        MEM_BUFFER_SIZE as f64 / (1 << 30) as f64
    );

    // Write zeroes to the memory to avoid page faults
    unsafe {
        std::ptr::write_bytes(buffer.as_mut_ptr(), 0, MEM_BUFFER_SIZE);
    }

    tracing::info!("[+] Memory prepared. Starting benchmark...");

    let now = Instant::now();
    for _ in 0..ITERATIONS {
        fetch_to_mem(
            Arc::new(Client::new(&config)),
            objects.clone(),
            "s3-bench-test-bucket-ps".to_string(),
            buffer.as_mut_ptr(),
        )
        .await?;
    }
    let elapsed = now.elapsed();

    println!("elapsed: {:?}", elapsed);
    let gbps =
        (objects.len() * 1024 * 1024 * 1024 * 8) as f64 / elapsed.as_secs_f64() / 1_000_000_000.0
            * ITERATIONS as f64;
    println!("[*] Throughput: {:.2} Gbps", gbps);

    Ok(())
}
