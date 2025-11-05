use clap::Parser;
use distacean::{Distacean, DistaceanConfig};
use tracing_subscriber::EnvFilter;

#[derive(Parser, Clone, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Opt {
    #[clap(long)]
    pub id: u64,

    #[clap(long)]
    pub tcp_port: u16,

    pub value: u16,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse the parameters passed by arguments.
    let options = Opt::parse();

    // Setup the logger with node_id and port in the format
    tracing_subscriber::fmt()
        .with_target(true)
        .with_thread_ids(false)
        .with_level(true)
        .with_ansi(false)
        .with_env_filter(EnvFilter::from_default_env())
        .fmt_fields(tracing_subscriber::fmt::format::DefaultFields::new())
        .init();

    tracing::info!("Starting node {} on {}", options.id, options.tcp_port);

    let distacean = Distacean::init(DistaceanConfig {
        node_id: options.id,
        tcp_port: options.tcp_port,
        nodes: vec![
            (1, "127.0.0.1:22001".to_owned()),
            (2, "127.0.0.1:22002".to_owned()),
            (3, "127.0.0.1:22003".to_owned()),
        ],
    })
    .await?;

    // Sleep for 3 seconds
    tokio::time::sleep(std::time::Duration::from_secs(8)).await;
    let kv = distacean.distkv();
    let key = "foo".to_string();
    let value = options.value.to_string();

    if options.id == 3 {
        kv.set(key.clone(), value.clone()).await?;

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        let read_value = kv.eventual_read(key.clone()).await?;

        println!("Node 3 read value: {}", read_value);
    } else if options.id == 2 {
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;

        let read_value = kv.eventual_read(key.clone()).await?;

        println!("Node 2 read value: {}", read_value);
    } else if options.id == 1 {
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;

        let read_value = kv.eventual_read(key.clone()).await?;

        println!("Node 1 read value: {}", read_value);
    }

    futures::future::pending::<()>().await;
    Ok(())
}
