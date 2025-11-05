pub mod log_store;
pub mod network;
pub mod network_tcp;
pub mod peernet;
mod protocol;
pub mod store;
mod util;

use std::sync::Arc;

use openraft::BasicNode;
use openraft::Config;
use openraft::raft::ClientWriteResponse;
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::net::TcpStream;

use crate::network_tcp::RaftPeerManager;
use crate::network_tcp::watch_peer_request;
use crate::peernet::PeerConnection;
use crate::peernet::PeerManager;
use crate::peernet::StartableStream;
use crate::peernet::TcpStreamStarter;
use crate::protocol::RequestType;
use crate::store::Request;
use crate::store::Response;
use crate::store::rocks::RocksStateMachine;

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

pub struct DistaceanCore {
    node_id: NodeId,
    raft: Raft,
    peer_manager: Arc<PeerManager<TcpStream, TcpStreamStarter>>,
    state_machine_store: RocksStateMachine,
}

pub struct DistaceanConfig {
    pub node_id: NodeId,
    pub tcp_port: u16,
    pub nodes: Vec<(NodeId, String)>,
}

#[derive(Error, Debug)]
pub enum DistaceanSetupError {
    #[error("unknown setup error")]
    Unknown,
}

#[derive(Clone)]
pub struct Distacean {
    core: Arc<DistaceanCore>,
}

impl Distacean {
    pub async fn init(opts: DistaceanConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let config = Config {
            heartbeat_interval: 500,
            election_timeout_min: 1500,
            election_timeout_max: 3000,
            ..Default::default()
        };

        let config = Arc::new(config.validate().unwrap());

        let dir = format!("./rocks/node-{}", opts.node_id);
        let (log_store, state_machine_store) = crate::store::rocks::new(&dir).await.unwrap();
        let is_initialized = {
            let (_, membership) = state_machine_store
                .get_meta()
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            !membership.membership().nodes().into_iter().next().is_none()
        };

        // Create the network layer that will connect and communicate the raft instances and
        // will be used in conjunction with the store created above.
        let peer_manager: Arc<PeerManager<TcpStream, TcpStreamStarter>> =
            PeerManager::new(opts.tcp_port, TcpStreamStarter {});
        let mut rc = peer_manager.clone().get_recv();
        let network = RaftPeerManager::new(peer_manager.clone());
        let peer_manager_clone = peer_manager.clone();
        tokio::spawn(async move {
            run_listener(peer_manager_clone).await;
        });

        // Create a local raft instance.
        let raft = openraft::Raft::new(
            opts.node_id,
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
                        eprintln!("[{}] Error receiving message: {}", opts.tcp_port, e);
                    }
                }
            }
        });

        if !is_initialized {
            let mut nodes = std::collections::BTreeMap::new();
            for (id, addr) in opts.nodes.into_iter() {
                nodes.insert(id, BasicNode { addr });
            }
            // User provided nodes for initialization
            raft.initialize(nodes).await.unwrap();
        } else {
            // Already initialized, skip
            tracing::info!("Cluster already initialized, skipping init");
        }

        Ok(Self {
            core: Arc::new(DistaceanCore {
                node_id: opts.node_id,
                raft,
                peer_manager,
                state_machine_store,
            }),
        })
    }

    pub fn distkv(self: &Self) -> DistKV {
        DistKV {
            core: Arc::new(DistKVCore {
                distacean: self.core.clone(),
            }),
        }
    }
}

impl DistaceanCore {
    pub(crate) fn raft(&self) -> Raft {
        self.raft.clone()
    }

    async fn get_leader_peer(&self) -> Option<Arc<PeerConnection<TcpStream, TcpStreamStarter>>> {
        // 1. Get current leader
        let leader_id = match self.raft.current_leader().await {
            Some(id) => id,
            None => {
                return None;
            }
        };

        // 2. Get leader's address from membership config
        let metrics = self.raft.metrics().borrow().clone();
        let leader_node = match metrics.membership_config.membership().get_node(&leader_id) {
            Some(node) => node,
            None => {
                return None;
            }
        };
        let leader_addr = &leader_node.addr;
        let port = leader_addr
            .split(':')
            .last()
            .and_then(|s| s.parse().ok())
            .unwrap();
        // 3. Get or create peer connection to leader
        let peer = self.peer_manager.get_or_create_connection(port).await;
        Some(peer)
    }

    async fn write_or_forward_to_leader(
        &self,
        req: Request,
    ) -> Result<Response, Box<dyn std::error::Error>> {
        // Check if current node is leader
        let is_leader = self.raft.current_leader().await == Some(self.node_id);
        if is_leader {
            // If leader, perform the write
            let res = self.raft.client_write(req).await?;
            Ok(res.response().clone())
        } else {
            // If not leader, forward to leader
            if let Some(leader_peer) = self.get_leader_peer().await {
                let req_bytes = bincode::serialize(&RequestType::AppRequest(req))?;
                let res_bytes = leader_peer.req_response(req_bytes).await.unwrap();
                let res: ClientWriteResponse<TypeConfig> = bincode::deserialize(&res_bytes)?;
                Ok(res.response().clone())
            } else {
                Err("No leader available".into())
            }
        }
    }
}

struct DistKVCore {
    distacean: Arc<DistaceanCore>,
}

#[derive(Clone)]
pub struct DistKV {
    core: Arc<DistKVCore>,
}

impl DistKVCore {
    pub fn set(self: Arc<DistKVCore>, key: String, value: String) {
        let raft = self.distacean.raft();
        tokio::spawn(async move {
            let req = Request::Set { key, value };
            let _res = raft.client_write(req).await;
        });
    }
}

impl DistKV {
    pub async fn set(
        self: &DistKV,
        key: String,
        value: String,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.core
            .distacean
            .write_or_forward_to_leader(Request::Set {
                key: key.clone(),
                value: value.clone(),
            })
            .await?;
        Ok(())
    }

    pub async fn delete(self: &DistKV, key: String) -> Result<(), Box<dyn std::error::Error>> {
        self.core
            .distacean
            .write_or_forward_to_leader(Request::Del { key: key.clone() })
            .await?;
        Ok(())
    }

    pub async fn eventual_read(
        self: &DistKV,
        key: String,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let value = self.core.distacean.state_machine_store.get(&key).await?;
        Ok(value.unwrap_or_default())
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
