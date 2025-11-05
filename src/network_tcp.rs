use std::sync::Arc;

use openraft::{
    BasicNode, RaftNetwork, RaftNetworkFactory, RaftTypeConfig,
    error::{InstallSnapshotError, RPCError, RaftError, Unreachable},
    network::RPCOption,
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncRead, AsyncSeek, AsyncWrite},
    net::TcpStream,
};

use crate::{
    Raft, TypeConfig,
    peernet::{PeerConnection, PeerManager, RecvMessage, TcpStreamStarter},
};

pub struct RaftPeerManager {
    inner: Arc<PeerManager<TcpStream, TcpStreamStarter>>,
}

impl RaftPeerManager {
    pub fn new(mgr: Arc<PeerManager<TcpStream, TcpStreamStarter>>) -> Self {
        RaftPeerManager { inner: mgr.clone() }
    }
}

impl<C> RaftNetworkFactory<C> for RaftPeerManager
where
    C: RaftTypeConfig<Node = BasicNode>,
    // RaftNetworkV2 is implemented automatically for RaftNetwork, but requires the following trait bounds.
    // In V2 network, the snapshot has no constraints, but RaftNetwork assumes a Snapshot is a file-like
    // object that can be seeked, read from, and written to.
    <C as RaftTypeConfig>::SnapshotData: AsyncRead + AsyncWrite + AsyncSeek + Unpin,
{
    type Network = RaftPeerNetwork<C>;

    async fn new_client(&mut self, target: C::NodeId, node: &BasicNode) -> Self::Network {
        // unimplemented!("test");
        let addr = node.addr.clone();
        let port = addr.split(':').last().and_then(|s| s.parse().ok()).unwrap();
        let x: Arc<PeerConnection<TcpStream, TcpStreamStarter>> =
            self.inner.get_or_create_connection(port).await;

        RaftPeerNetwork {
            port,
            target,
            inner: x,
        }
    }
}

pub struct RaftPeerNetwork<C>
where
    C: RaftTypeConfig,
{
    port: u16,
    target: C::NodeId,
    inner: Arc<PeerConnection<TcpStream, TcpStreamStarter>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum RequestType {
    AppendEntriesRequest(Vec<u8>),
    InstallSnapshotRequest(Vec<u8>),
    VoteRequest(Vec<u8>),
    AppRequest(crate::store::Request),
}

#[allow(clippy::blocks_in_conditions)]
impl<C> RaftNetwork<C> for RaftPeerNetwork<C>
where
    C: RaftTypeConfig,
{
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<C>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<C>, RPCError<C, RaftError<C>>> {
        let bytes = bincode::serialize(&req).unwrap();
        let req_bytes = bincode::serialize(&RequestType::AppendEntriesRequest(bytes)).unwrap();
        let res = self
            .inner
            .clone()
            .req_response(req_bytes)
            .await
            .map_err(|e| {
                let io_err = std::io::Error::new(std::io::ErrorKind::Other, e.to_string());
                RPCError::Unreachable(Unreachable::new(&io_err))
            })?;
        let resp: Result<AppendEntriesResponse<C>, RaftError<C>> =
            bincode::deserialize(&res).unwrap();
        match resp {
            Ok(x) => Ok(x),
            Err(x) => Err(RPCError::RemoteError(openraft::error::RemoteError::new(
                self.target.clone(),
                x,
            ))),
        }
    }

    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<C>,
        _option: RPCOption,
    ) -> Result<InstallSnapshotResponse<C>, RPCError<C, RaftError<C, InstallSnapshotError>>> {
        let bytes = bincode::serialize(&req).unwrap();
        let req_bytes = bincode::serialize(&RequestType::InstallSnapshotRequest(bytes)).unwrap();
        let res = self
            .inner
            .clone()
            .req_response(req_bytes)
            .await
            .map_err(|e| {
                let io_err = std::io::Error::new(std::io::ErrorKind::Other, e.to_string());
                RPCError::Unreachable(Unreachable::new(&io_err))
            })?;
        let resp: Result<InstallSnapshotResponse<C>, RaftError<C, InstallSnapshotError>> =
            bincode::deserialize(&res).unwrap();
        match resp {
            Ok(x) => Ok(x),
            Err(x) => Err(RPCError::RemoteError(openraft::error::RemoteError::new(
                self.target.clone(),
                x,
            ))),
        }
    }

    async fn vote(
        &mut self,
        req: VoteRequest<C>,
        _option: RPCOption,
    ) -> Result<VoteResponse<C>, RPCError<C, RaftError<C>>> {
        let bytes = bincode::serialize(&req).unwrap();
        let req_bytes = bincode::serialize(&RequestType::VoteRequest(bytes)).unwrap();
        let res = self
            .inner
            .clone()
            .req_response(req_bytes)
            .await
            .map_err(|e| {
                let io_err = std::io::Error::new(std::io::ErrorKind::Other, e.to_string());
                RPCError::Unreachable(Unreachable::new(&io_err))
            })?;
        let resp: Result<VoteResponse<C>, RaftError<C>> = bincode::deserialize(&res).unwrap();
        match resp {
            Ok(x) => Ok(x),
            Err(x) => Err(RPCError::RemoteError(openraft::error::RemoteError::new(
                self.target.clone(),
                x,
            ))),
        }
    }
}

pub fn watch_peer_request(peer_con: Arc<PeerConnection<TcpStream, TcpStreamStarter>>, raft: Raft) {
    let peer_clone = peer_con.clone();
    let mut read_channel = peer_con.get_read_channel();
    tokio::spawn(async move {
        loop {
            let msg = read_channel.recv().await.unwrap();
            match msg {
                RecvMessage::Req { req_id, payload } => {
                    let request: RequestType = bincode::deserialize(&payload).unwrap();
                    match request {
                        RequestType::AppendEntriesRequest(bytes) => {
                            let req: AppendEntriesRequest<TypeConfig> =
                                bincode::deserialize(&bytes).unwrap();
                            let res: Result<
                                openraft::raft::AppendEntriesResponse<TypeConfig>,
                                openraft::error::RaftError<TypeConfig>,
                            > = raft.append_entries(req).await;
                            let res_bytes = bincode::serialize(&res).unwrap();
                            peer_clone
                                .clone()
                                .send_response(req_id, res_bytes)
                                .await
                                .unwrap();
                        }
                        RequestType::InstallSnapshotRequest(bytes) => {
                            let req: InstallSnapshotRequest<TypeConfig> =
                                bincode::deserialize(&bytes).unwrap();
                            let res: Result<
                                openraft::raft::InstallSnapshotResponse<TypeConfig>,
                                openraft::error::RaftError<TypeConfig, InstallSnapshotError>,
                            > = raft.install_snapshot(req).await;
                            let res_bytes = bincode::serialize(&res).unwrap();
                            peer_clone
                                .clone()
                                .send_response(req_id, res_bytes)
                                .await
                                .unwrap();
                        }
                        RequestType::VoteRequest(bytes) => {
                            let req: VoteRequest<TypeConfig> =
                                bincode::deserialize(&bytes).unwrap();
                            let res: Result<
                                openraft::raft::VoteResponse<TypeConfig>,
                                openraft::error::RaftError<TypeConfig>,
                            > = raft.vote(req).await;
                            let res_bytes = bincode::serialize(&res).unwrap();
                            peer_clone
                                .clone()
                                .send_response(req_id, res_bytes)
                                .await
                                .unwrap();
                        }
                        RequestType::AppRequest(app_req) => {
                            let res = raft.client_write(app_req).await.unwrap();
                            let res_bytes = bincode::serialize(&res).unwrap();
                            peer_clone
                                .clone()
                                .send_response(req_id, res_bytes)
                                .await
                                .unwrap();
                        }
                    }
                }
                RecvMessage::Res { .. } => {}
                RecvMessage::Data { .. } => {
                    unimplemented!("handle data message");
                }
            }
        }
    });
}
