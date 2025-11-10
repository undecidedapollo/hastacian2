use crate::utils::ephemeral_distacian_cluster;
use distacean::{ClusterDistaceanConfig, Distacean};
use tracing_subscriber::EnvFilter;
mod utils;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_target(true)
        .with_thread_ids(false)
        .with_level(true)
        .with_ansi(false)
        .with_env_filter(EnvFilter::from_default_env())
        .fmt_fields(tracing_subscriber::fmt::format::DefaultFields::new())
        .init();

    let distacean = ephemeral_distacian_cluster().await?;
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    let queues = distacean.fifo_queues();
    let queue_name = "my_queue";

    for j in 0..10 {
        queues
            .enqueue(queue_name, vec![format!("item_{}", j)])
            .await
            .expect("Failed to enqueue");
        println!("Enqueued item_{} to {}", j, queue_name);
    }

    loop {
        let items: Vec<String> = queues
            .dequeue(queue_name, 3)
            .await
            .expect("Failed to dequeue");
        if items.is_empty() {
            println!("Queue is empty, stopping dequeue");
            break;
        }
        for item in items {
            println!("Dequeued {} from {}", item, queue_name);
        }
    }

    // Sleep for a second to warm up
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    Ok(())

    // let kv = distacean.kv_store();
}
