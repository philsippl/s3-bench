use std::sync::Arc;

use aws_sdk_s3::Client;
use tokio::{io::AsyncReadExt, task};

const CHUNK_SIZE: usize = 1 << 30;
const PART_SIZE: usize = 50 << 20;
const MAX_CONCURRENT_REQUESTS: usize = 256;

pub async fn list_objects(
    client: Arc<Client>,
    bucket: String,
    prefix: &str,
) -> eyre::Result<Vec<String>> {
    let mut objects = Vec::new();
    let mut continuation_token = None;

    loop {
        let mut request = client.list_objects_v2().bucket(&bucket).prefix(prefix);

        if let Some(token) = continuation_token {
            request = request.continuation_token(token);
        }

        let response = request.send().await?;

        objects.extend(
            response
                .contents()
                .iter()
                .filter_map(|obj| obj.key().map(String::from)),
        );

        match response.next_continuation_token() {
            Some(token) => continuation_token = Some(token.to_string()),
            None => break,
        }
    }

    Ok(objects)
}
pub async fn fetch_to_mem(
    client: Arc<Client>,
    chunks: Vec<String>,
    bucket: String,
    mem_ptr: *mut u8,
) -> eyre::Result<()> {
    let mut handles: Vec<task::JoinHandle<Result<(), eyre::Error>>> = Vec::new();
    let mut active_handles = 0;

    for (chunk_idx, chunk) in chunks.into_iter().enumerate() {
        for part_idx in 0..(CHUNK_SIZE / PART_SIZE) {
            let client = client.clone();
            let bucket = bucket.clone();
            let chunk = chunk.clone();
            let slice = unsafe {
                std::slice::from_raw_parts_mut(
                    mem_ptr.add(chunk_idx * CHUNK_SIZE + part_idx * PART_SIZE),
                    PART_SIZE,
                )
            };

            // Wait if we've hit the concurrency limit
            if active_handles >= MAX_CONCURRENT_REQUESTS {
                let handle = handles.remove(0);
                handle.await??;
                active_handles -= 1;
            }

            handles.push(task::spawn(async move {
                let mut result = client
                    .get_object()
                    .bucket(&bucket)
                    .key(&chunk)
                    .range(format!(
                        "bytes={}-{}",
                        part_idx * PART_SIZE,
                        (part_idx + 1) * PART_SIZE - 1
                    ))
                    .send()
                    .await?
                    .body
                    .into_async_read();

                let mut bytes_read = 0;
                while bytes_read < CHUNK_SIZE {
                    let n = result.read(&mut slice[bytes_read..]).await?;
                    if n == 0 {
                        break;
                    }
                    bytes_read += n;
                }

                Ok(())
            }));
            active_handles += 1;
        }
    }

    // Wait for remaining handles
    for handle in handles {
        handle.await??;
    }

    Ok(())
}
