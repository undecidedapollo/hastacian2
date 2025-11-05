pub mod app;
pub mod client_http;
pub mod log_store;
pub mod network;
pub mod network_http;
pub mod network_tcp;
pub mod peernet;
pub mod store;

use std::sync::Arc;

use actix_web::HttpServer;
use actix_web::middleware;
use actix_web::middleware::Logger;
use actix_web::web::Data;
use openraft::Config;
use tokio::net::TcpListener;
use tokio::net::TcpStream;

use crate::app::App;
use crate::network::api;
use crate::network::management;
use crate::network::raft;
use crate::network_tcp::RaftPeerManager;
use crate::network_tcp::watch_peer_request;
use crate::peernet::PeerManager;
use crate::peernet::StartableStream;
use crate::peernet::TcpStreamStarter;
use crate::store::Request;
use crate::store::Response;

pub type NodeId = u64;

openraft::declare_raft_types!(
    /// Declare the type configuration for example K/V store.
    pub TypeConfig:
        D = Request,
        R = Response,
);

pub type LogStore = store::rocks::log_store::RocksLogStore<TypeConfig>;
pub type StateMachineStore = store::rocks::RocksStateMachine;
pub type Raft = openraft::Raft<TypeConfig>;

pub async fn start_example_raft_node(
    node_id: NodeId,
    http_addr: String,
    tcp_port: u16,
) -> std::io::Result<()> {
    // Create a configuration for the raft instance.
    let config = Config {
        heartbeat_interval: 500,
        election_timeout_min: 1500,
        election_timeout_max: 3000,
        // TODO: Memstore + persistent client ID requires this because a node w/ no data has no way to get that data back / we should allow re-initialization with that id.
        // allow_log_reversion: Some(true),
        ..Default::default()
    };

    let config = Arc::new(config.validate().unwrap());

    // Create a instance of where the Raft logs will be stored.
    // let log_store = LogStore::default();
    // // Create a instance of where the Raft data will be stored.
    // let state_machine_store = Arc::new(StateMachineStore::default());

    let dir = format!("./rocks/node-{}", node_id);
    let (log_store, state_machine_store) = crate::store::rocks::new(&dir).await.unwrap();

    // Create the network layer that will connect and communicate the raft instances and
    // will be used in conjunction with the store created above.
    let _network = network_http::NetworkFactory {};
    let peer_manager = PeerManager::new(tcp_port, TcpStreamStarter {});
    let mut rc = peer_manager.clone().get_recv();
    let network = RaftPeerManager::new(peer_manager.clone());
    let peer_manager_clone = peer_manager.clone();
    tokio::spawn(async move {
        run_listener(peer_manager_clone).await;
    });

    // Create a local raft instance.
    let raft = openraft::Raft::new(
        node_id,
        config.clone(),
        network,
        log_store.clone(),
        state_machine_store.clone(),
    )
    .await
    .unwrap();

    let rclone = raft.clone();
    tokio::spawn(async move {
        loop {
            match rc.recv().await {
                Ok(msg) => {
                    watch_peer_request(msg, rclone.clone());
                }
                Err(e) => {
                    eprintln!("[{}] Error receiving message: {}", tcp_port, e);
                }
            }
        }
    });

    // raft.initialize(hashmap! {
    //     1_u64 => BasicNode { addr: "127.0.0.1:21001".to_owned() },
    //     2_u64 => BasicNode { addr: "127.0.0.1:21002".to_owned() },
    //     3_u64 => BasicNode { addr: "127.0.0.1:21003".to_owned() },
    // })
    // .await
    // .unwrap();

    // Create an application that will store all the instances created above, this will
    // later be used on the actix-web services.
    let app_data = Data::new(App {
        id: node_id,
        addr: http_addr.clone(),
        raft,
        state_machine_store,
    });

    // Start the actix-web server.
    let log_format = Arc::new(format!(
        "[Node {}:{}] %a \"%r\" %s %b \"%{{User-Agent}}i\" %T",
        node_id, tcp_port
    ));

    let server = HttpServer::new(move || {
        let fmt = log_format.clone();
        actix_web::App::new()
            .wrap(Logger::new(fmt.as_str()))
            .wrap(middleware::Compress::default())
            .app_data(app_data.clone())
            // raft internal RPC
            .service(raft::append)
            .service(raft::snapshot)
            .service(raft::vote)
            // admin API
            .service(management::init)
            .service(management::add_learner)
            .service(management::change_membership)
            .service(management::metrics)
            .service(management::get_linearizer)
            // application API
            .service(api::write)
            .service(api::read)
            .service(api::linearizable_read)
            .service(api::follower_read)
    });

    let x = server.bind(http_addr)?;

    x.run().await
}

pub struct AutoAbort<T>(Option<tokio::task::JoinHandle<T>>);

impl<T> Drop for AutoAbort<T> {
    fn drop(&mut self) {
        if let Some(handle) = self.0.take() {
            handle.abort();
        }
    }
}

impl<T> AutoAbort<T> {
    pub fn new(handle: tokio::task::JoinHandle<T>) -> Self {
        Self(Some(handle))
    }

    pub async fn join(mut self) -> Result<T, tokio::task::JoinError> {
        self.0.take().unwrap().await
    }
}

async fn run_listener(
    peer_manager: Arc<
        PeerManager<TcpStream, impl StartableStream<TcpStream> + Send + Sync + 'static>,
    >,
) {
    let addr = format!("127.0.0.1:{}", peer_manager.local_port);
    let listener = match TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!(
                "[{}] Failed to bind listener: {}",
                peer_manager.local_port, e
            );
            return;
        }
    };

    println!("[{}] Listening on {}", peer_manager.local_port, addr);

    loop {
        match listener.accept().await {
            Ok((stream, _peer_addr)) => {
                let peer_manager = peer_manager.clone();
                tokio::spawn(async move {
                    peer_manager.handle_incoming_connection(stream).await;
                });
            }
            Err(e) => {
                eprintln!(
                    "[{}] Failed to accept connection: {}",
                    peer_manager.local_port, e
                );
            }
        }
    }
}
