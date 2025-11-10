use std::sync::Arc;

use maplit::hashmap;
use openraft::BasicNode;
use openraft::Config;
use openraft::error::decompose::DecomposeResult;
use openraft::raft::linearizable_read::LinearizeState;
use openraft::raft::linearizable_read::Linearizer;
// use thiserror::Error;
use tokio::net::TcpListener;
use tokio::net::TcpStream;

use crate::distkv::DistKV;
use crate::distkv::DistKVCore;
use crate::network_tcp::RaftPeerManager;
use crate::network_tcp::TcpStreamStarter;
use crate::peernet::PeerConnection;
use crate::peernet::PeerManager;
use crate::peernet::StartableStream;
use crate::protocol::LinearizerData;
use crate::protocol::ReadPolicy;
use crate::protocol::RequestType;
use crate::raft::RequestOperation;
use crate::raft::StateMachineStore;
use crate::raft::TypeConfig;
use crate::raft::{NodeId, Raft, Request, Response};
use crate::router::route_peer_connection_messages;

// #[derive(Error, Debug)]
// pub enum DistaceanSetupError {
//     #[error("unknown setup error")]
//     Unknown,
// }

pub struct SingleNodeDistaceanConfig {
    pub node_id: NodeId,
}

pub struct ClusterDistaceanConfig {
    pub node_id: NodeId,
    pub tcp_port: u16,
    pub nodes: Vec<(NodeId, String)>,
}

// pub enum DistaceanSetupConfig {
//     SingleNode(SingleNodeDistaceanConfig),
//     Cluster(ClusterDistaceanConfig),
// }

// pub struct DistaceanConfig {
//     pub node_id: NodeId,
//     pub tcp_port: u16,
//     pub nodes: Vec<(NodeId, String)>,
// }

pub struct DistaceanCore {
    node_id: NodeId,
    pub(crate) raft: Raft,
    peer_manager: Arc<PeerManager<TcpStream, TcpStreamStarter>>,
    pub(crate) state_machine_store: StateMachineStore,
    request_seq_id: std::sync::atomic::AtomicU64,
}

#[derive(Clone)]
pub struct Distacean {
    core: Arc<DistaceanCore>,
}

impl Distacean {
    pub async fn init(
        opts: ClusterDistaceanConfig,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let config = Config {
            heartbeat_interval: 1000,
            election_timeout_min: 1500,
            election_timeout_max: 3000,
            ..Default::default()
        };

        let config = Arc::new(config.validate().unwrap());

        let dir = format!("./rocks/node-{}", opts.node_id);
        let (log_store, state_machine_store) =
            crate::raft::store::create_rocks_stores(&dir).await.unwrap();
        let is_initialized = {
            let (_, membership) = state_machine_store
                .get_meta()
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            !membership.membership().nodes().into_iter().next().is_none()
        };

        let peer_manager: Arc<PeerManager<TcpStream, TcpStreamStarter>> =
            PeerManager::new(opts.tcp_port, TcpStreamStarter {});
        let mut on_new_peer_receiver = peer_manager.clone().get_recv();

        let network = RaftPeerManager::new(peer_manager.clone());

        // Spin up listener for incoming connections. It routes new connections to the peer manager to handle.
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
                match on_new_peer_receiver.recv().await {
                    Ok(msg) => {
                        route_peer_connection_messages(msg, rclone.clone());
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
                request_seq_id: std::sync::atomic::AtomicU64::new(1),
            }),
        })
    }

    pub async fn init_single_node_cluster(
        opts: SingleNodeDistaceanConfig,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let config = Config {
            heartbeat_interval: 500,
            election_timeout_min: 1500,
            election_timeout_max: 3000,
            ..Default::default()
        };

        let config = Arc::new(config.validate().unwrap());
        let dir = format!("./rocks/node-{}", opts.node_id);
        let (log_store, state_machine_store) =
            crate::raft::store::create_rocks_stores(&dir).await.unwrap();
        let is_initialized = {
            let (_, membership) = state_machine_store
                .get_meta()
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            !membership.membership().nodes().into_iter().next().is_none()
        };

        let peer_manager: Arc<PeerManager<TcpStream, TcpStreamStarter>> =
            PeerManager::new(1, TcpStreamStarter {});

        let network = RaftPeerManager::new(peer_manager.clone());

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

        if !is_initialized {
            let nodes = hashmap! {
                opts.node_id => BasicNode {
                    addr: format!("local:{}", opts.node_id),
                }
            };
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
                request_seq_id: std::sync::atomic::AtomicU64::new(1),
            }),
        })
    }

    pub fn kv_store(self: &Self) -> DistKV {
        DistKV {
            core: Arc::new(DistKVCore {
                distacean: self.core.clone(),
            }),
        }
    }

    pub fn fifo_queues(self: &Self) -> crate::fifo::DistFIFO {
        crate::fifo::DistFIFO {
            distacean: self.core.clone(),
        }
    }
}

pub enum LeaderResponse {
    NodeIsLeader,
    NodeIsFollower(Arc<PeerConnection<TcpStream, TcpStreamStarter>>),
    NoLeader,
}

#[derive(Clone, Copy)]
pub enum ReadSource {
    Local,
    Leader,
}

impl DistaceanCore {
    #[allow(dead_code)]
    pub(crate) fn raft(&self) -> Raft {
        self.raft.clone()
    }

    pub(crate) async fn get_leader_peer(
        &self,
    ) -> Result<LeaderResponse, Box<dyn std::error::Error + Send + Sync>> {
        // 1. Get current leader
        let leader_id = match self.raft.current_leader().await {
            Some(id) => id,
            None => {
                return Ok(LeaderResponse::NoLeader);
            }
        };

        if self.node_id == leader_id {
            return Ok(LeaderResponse::NodeIsLeader);
        }

        // 2. Get leader's address from membership config
        let metrics = self.raft.metrics().borrow().clone();
        let leader_node = match metrics.membership_config.membership().get_node(&leader_id) {
            Some(node) => node,
            None => {
                return Err("Leader node not found in membership".into());
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
        Ok(LeaderResponse::NodeIsFollower(peer))
    }

    pub(crate) async fn write_or_forward_to_leader(
        &self,
        req: RequestOperation,
    ) -> Result<Response, Box<dyn std::error::Error + Send + Sync>> {
        let node_id = self.node_id;
        let seq_id = self
            .request_seq_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        match self.get_leader_peer().await? {
            LeaderResponse::NodeIsLeader => {
                let res = self
                    .raft
                    .client_write(Request {
                        client_id: node_id,
                        seq_id: Some(seq_id),
                        op: req,
                    })
                    .await
                    .decompose()
                    .unwrap()?;
                Ok(res.response().clone())
            }
            LeaderResponse::NodeIsFollower(leader_peer) => {
                // If not leader, forward to leader
                let req_bytes = rmp_serde::to_vec(&RequestType::AppRequest(Request {
                    client_id: node_id,
                    seq_id: Some(seq_id),
                    op: req,
                }))?;
                let res_bytes = leader_peer.req_res(req_bytes).await.unwrap();
                let res: Result<
                    openraft::raft::ClientWriteResponse<TypeConfig>,
                    openraft::error::ClientWriteError<TypeConfig>,
                > = rmp_serde::from_slice(&res_bytes)?;

                match res {
                    Ok(r) => Ok(r.response().clone()),
                    Err(e) => Err(Box::new(e)),
                }
            }
            LeaderResponse::NoLeader => Err("No leader available".into()),
        }
    }

    pub(crate) async fn get_linearizer(
        &self,
        read_source: ReadSource,
        read_policy: ReadPolicy,
    ) -> Result<LinearizeState<TypeConfig>, Box<dyn std::error::Error + Send + Sync>> {
        let linearizer = match read_source {
            ReadSource::Local => {
                let linearizer = self
                    .raft
                    .get_read_linearizer(read_policy.into())
                    .await
                    .decompose()
                    .unwrap()?;
                linearizer
            }
            ReadSource::Leader => {
                let leader = self.get_leader_peer().await?;

                match leader {
                    LeaderResponse::NodeIsLeader => {
                        // Get linearizer from local raft
                        let linearizer = self
                            .raft
                            .get_read_linearizer(read_policy.into())
                            .await
                            .decompose()
                            .unwrap()?;
                        linearizer
                    }
                    LeaderResponse::NodeIsFollower(leader_peer) => {
                        let data = rmp_serde::to_vec(&RequestType::Linearizer { read_policy })?;
                        let res = leader_peer.req_res(data).await?;
                        let linearizer_data: Result<LinearizerData, String> =
                            rmp_serde::from_slice(&res)?;
                        let linearizer_data = linearizer_data?;

                        Linearizer::new(
                            linearizer_data.node_id,
                            linearizer_data.read_log_id,
                            linearizer_data.applied,
                        )
                    }
                    LeaderResponse::NoLeader => {
                        return Err("No leader available".to_string().into());
                    }
                }
            }
        };

        let data = linearizer.await_ready(&self.raft).await?;
        Ok(data)
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
