use clap::Parser;
use hastacian2::start_example_raft_node;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Clone, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Opt {
    #[clap(long)]
    pub id: u64,

    #[clap(long)]
    pub http_addr: String,

    #[clap(long)]
    pub tcp_port: u16,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
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

    tracing::info!("Starting node {} on {}", options.id, options.http_addr);

    start_example_raft_node(options.id, options.http_addr, options.tcp_port).await
}
