use std::sync::Arc;

use openraft::{
    error::{InstallSnapshotError, decompose::DecomposeResult},
    raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest},
};
use tokio::net::TcpStream;

use crate::{
    network_tcp::TcpStreamStarter,
    peernet::{PeerConnection, RecvMessage},
    protocol::{LinearizerData, RequestType},
    raft::{Raft, TypeConfig},
};

pub fn route_peer_connection_messages(
    peer_con: Arc<PeerConnection<TcpStream, TcpStreamStarter>>,
    raft: Raft,
) {
    let peer_clone = peer_con.clone();
    let mut read_channel = peer_con.get_read_channel();
    tokio::spawn(async move {
        loop {
            let msg = read_channel.recv().await.unwrap();
            match msg {
                RecvMessage::Req { req_id, payload } => {
                    let request: RequestType = rmp_serde::from_slice(&payload).unwrap();
                    match request {
                        RequestType::AppendEntriesRequest(bytes) => {
                            let req: AppendEntriesRequest<TypeConfig> =
                                rmp_serde::from_slice(&bytes).unwrap();
                            let res: Result<
                                openraft::raft::AppendEntriesResponse<TypeConfig>,
                                openraft::error::RaftError<TypeConfig>,
                            > = raft.append_entries(req).await;
                            let res_bytes = rmp_serde::to_vec(&res).unwrap();
                            peer_clone
                                .clone()
                                .send_response(req_id, res_bytes)
                                .await
                                .unwrap();
                        }
                        RequestType::InstallSnapshotRequest(bytes) => {
                            let req: InstallSnapshotRequest<TypeConfig> =
                                rmp_serde::from_slice(&bytes).unwrap();
                            let res: Result<
                                openraft::raft::InstallSnapshotResponse<TypeConfig>,
                                openraft::error::RaftError<TypeConfig, InstallSnapshotError>,
                            > = raft.install_snapshot(req).await;
                            let res_bytes = rmp_serde::to_vec(&res).unwrap();
                            peer_clone
                                .clone()
                                .send_response(req_id, res_bytes)
                                .await
                                .unwrap();
                        }
                        RequestType::VoteRequest(bytes) => {
                            let req: VoteRequest<TypeConfig> =
                                rmp_serde::from_slice(&bytes).unwrap();
                            let res: Result<
                                openraft::raft::VoteResponse<TypeConfig>,
                                openraft::error::RaftError<TypeConfig>,
                            > = raft.vote(req).await;
                            let res_bytes = rmp_serde::to_vec(&res).unwrap();
                            peer_clone
                                .clone()
                                .send_response(req_id, res_bytes)
                                .await
                                .unwrap();
                        }
                        RequestType::AppRequest(app_req) => {
                            let res: Result<
                                openraft::raft::ClientWriteResponse<TypeConfig>,
                                openraft::error::ClientWriteError<TypeConfig>,
                            > = raft.client_write(app_req).await.decompose().unwrap();
                            let res_bytes = rmp_serde::to_vec(&res).unwrap();
                            peer_clone
                                .clone()
                                .send_response(req_id, res_bytes)
                                .await
                                .unwrap();
                        }
                        RequestType::Linearizer { read_policy } => {
                            let linearizer = raft
                                .get_read_linearizer(read_policy.into())
                                .await
                                .decompose()
                                .unwrap();
                            let to_send = match linearizer {
                                Ok(lin) => {
                                    let data = LinearizerData {
                                        node_id: *lin.node_id(),
                                        read_log_id: *lin.read_log_id(),
                                        applied: lin.applied().cloned(),
                                    };
                                    Ok(data)
                                }
                                Err(e) => Err(format!("Failed to get linearizer: {:?}", e)),
                            };

                            let res_bytes = rmp_serde::to_vec(&to_send).unwrap();
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
