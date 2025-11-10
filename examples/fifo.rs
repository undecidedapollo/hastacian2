use crate::utils::ephemeral_distacian_cluster;
use clap::{Parser, Subcommand, ValueEnum};
use distacean::{ClusterDistaceanConfig, Distacean, NodeId};
use tracing_subscriber::EnvFilter;
mod utils;

#[derive(Parser, Debug)]
#[command(name = "distacean", subcommand = "Ephemeral")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(ValueEnum, Debug, Clone, Copy)]
enum Type {
    Producer,
    Consumer,
    Both,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Start the server
    Cluster {
        #[arg(long)]
        tcp_port: u16,
        #[arg(long)]
        node_id: NodeId,

        #[arg(long)]
        #[clap(default_value = "Type::producer")]
        role: Type,
    },

    Ephemeral,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .with_target(true)
        .with_thread_ids(false)
        .with_level(true)
        .with_ansi(false)
        .with_env_filter(EnvFilter::from_default_env())
        .fmt_fields(tracing_subscriber::fmt::format::DefaultFields::new())
        .init();

    let (distacean, role) = match cli.command {
        Commands::Cluster {
            tcp_port,
            node_id,
            role,
        } => (
            Distacean::init(ClusterDistaceanConfig {
                node_id,
                tcp_port,
                nodes: vec![
                    (1, "127.0.0.1:22001".to_string()),
                    (2, "127.0.0.1:22002".to_string()),
                    (3, "127.0.0.1:22003".to_string()),
                ],
            })
            .await
            .map_err(|e| {
                eprintln!("Failed to initialize Distacean: {}", e);
                std::io::Error::new(std::io::ErrorKind::Other, "Distacean initialization failed")
            })?,
            role,
        ),
        Commands::Ephemeral => (ephemeral_distacian_cluster().await?, Type::Both),
    };
    distacean
        .wait_until_ready()
        .await
        .expect("Failed to wait until ready");

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let queue_name = "my_queue";

    if matches!(role, Type::Producer | Type::Both) {
        let dist_clone = distacean.clone();
        tokio::spawn(async move {
            let queues = dist_clone.fifo_queues();
            let mut i = 0;
            loop {
                i += 1;
                queues
                    .enqueue(queue_name, vec![format!("item_{}", i)])
                    .await
                    .expect("Failed to enqueue");
                println!("Enqueued item_{} to {}", i, queue_name);
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
        });
    }

    if matches!(role, Type::Consumer | Type::Both) {
        let dist_clone = distacean.clone();
        tokio::spawn(async move {
            let queues = dist_clone.fifo_queues();
            loop {
                let items: Vec<String> = queues
                    .dequeue(queue_name, 2)
                    .await
                    .expect("Failed to dequeue");
                if items.is_empty() {
                    println!("Queue is empty, waiting to dequeue");
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    continue;
                }
                for item in items {
                    println!("Dequeued {} from {}", item, queue_name);
                }
            }
        });
    }
    tokio::time::sleep(std::time::Duration::from_secs(60)).await;
    Ok(())
}
