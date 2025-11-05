use crate::AutoAbort;
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use tokio::io::{AsyncReadExt, AsyncWriteExt, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::{Duration, sleep};

#[derive(Parser, Debug)]
#[command(name = "hastacian")]
#[command(about = "Bidirectional TCP server with ping/pong")]
struct Args {
    #[arg(short, long)]
    port: u16,

    #[arg(short = 'P', long, value_delimiter = ',')]
    peers: Vec<u16>,
}

#[derive(Debug, Serialize, Deserialize)]
enum Message {
    HandshakeInit { client_id: u16 },
    Data { payload: Vec<u8> },
    Req { req_id: u64, payload: Vec<u8> },
    Res { res_id: u64, payload: Vec<u8> },
}

#[derive(Debug, Clone)]
pub enum RecvMessage {
    Data { payload: Vec<u8> },
    Req { req_id: u64, payload: Vec<u8> },
    Res { res_id: u64, payload: Vec<u8> },
}

async fn write_message<T: AsyncWriteExt + Unpin>(
    writer: &mut T,
    msg: &Message,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let bytes = bincode::serialize(msg)?;
    let len = bytes.len() as u32;
    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(&bytes).await?;
    Ok(())
}

async fn read_message<T: AsyncReadExt + Unpin>(
    reader: &mut T,
) -> Result<Message, Box<dyn std::error::Error + Send + Sync>> {
    let mut len_bytes = [0u8; 4];
    reader.read_exact(&mut len_bytes).await?;
    let len = u32::from_be_bytes(len_bytes) as usize;
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf).await?;
    let msg = bincode::deserialize(&buf)?;
    Ok(msg)
}

pub trait HStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static {}
impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static> HStream for T {}

pub trait HStartable<T: HStream>: StartableStream<T> + Send + Sync + 'static {}
impl<T, TS> HStartable<T> for TS
where
    T: HStream,
    TS: StartableStream<T> + Send + Sync + 'static,
{
}

// #[tokio::main]
// async fn main() {
//     let args = Args::parse();
//     let local_port = args.port;
//     let peers = args.peers;

//     println!("[{}] Starting server", local_port);

//     // Spawn TCP listener
//     let peer_manager_clone = peer_manager.clone();
//     let listener_handle = tokio::spawn(async move {
//         run_listener(peer_manager_clone).await;
//     });

//     // Spawn connectors for each peer
//     // The tie-breaker in handle_connection will handle any race conditions
//     for peer_port in peers {
//         peer_manager.get_or_create_connection(peer_port).await;
//     }

//     // Wait for Ctrl+C
//     println!("[{}] Press Ctrl+C to shutdown", local_port);
//     tokio::select! {
//         _ = tokio::signal::ctrl_c() => {
//             println!("[{}] Shutting down gracefully...", local_port);
//         }
//         _ = listener_handle => {}
//     }
// }

pub struct PeerManager<T: HStream, TS: HStartable<T>> {
    pub local_port: u16,
    connections: Mutex<HashMap<u16, PeerConnectionWrap<T, TS>>>,
    ts: Arc<TS>,
    clients: tokio::sync::broadcast::Sender<Arc<PeerConnection<T, TS>>>,
}

struct PeerConnectionWrap<T: HStream, TS: HStartable<T>> {
    connection: Arc<PeerConnection<T, TS>>,
    _handle: AutoAbort<()>,
}

impl<T, TS> PeerManager<T, TS>
where
    T: HStream,
    TS: HStartable<T>,
{
    pub fn new(local_port: u16, ts: TS) -> Arc<Self> {
        Arc::new(Self {
            local_port,
            connections: Mutex::new(HashMap::new()),
            ts: Arc::new(ts),
            clients: tokio::sync::broadcast::channel(16).0,
        })
    }

    pub async fn get_or_create_connection(
        self: &Arc<Self>,
        peer_port: u16,
    ) -> Arc<PeerConnection<T, TS>> {
        let mut conns = self.connections.lock().await;
        match conns.entry(peer_port) {
            Entry::Occupied(entry) => entry.get().connection.clone(),
            Entry::Vacant(entry) => {
                let (new_con, handle) =
                    PeerConnection::init(self.local_port, peer_port, self.ts.clone());
                entry.insert(PeerConnectionWrap {
                    connection: new_con.clone(),
                    _handle: AutoAbort::new(handle),
                });
                let _ = self.clients.send(new_con.clone());
                new_con
            }
        }
    }

    pub fn get_recv(
        self: Arc<Self>,
    ) -> tokio::sync::broadcast::Receiver<Arc<PeerConnection<T, TS>>> {
        self.clients.subscribe()
    }

    pub async fn handle_incoming_connection(self: Arc<Self>, mut stream: T) {
        let peer = match read_message(&mut stream).await {
            Ok(Message::HandshakeInit { client_id: port }) => port,
            Ok(_) => {
                eprintln!("[{}] Expected Hello message", self.local_port);
                return;
            }
            Err(e) => {
                eprintln!("[{}] Failed to read Hello: {}", self.local_port, e);
                return;
            }
        };

        let peer_con: Arc<PeerConnection<T, TS>> = self.get_or_create_connection(peer).await;

        peer_con
            .signal_tx
            .send(PeerConnectionSignal::ConRequest(PeerConnectionRequest {
                direction: ConnectionDirection::Incoming,
                peer_port: peer,
                stream,
            }))
            .await
            .unwrap();
    }
}

#[derive(Debug)]
struct PeerConnectionRequest<T: HStream> {
    direction: ConnectionDirection,
    peer_port: u16,
    stream: T,
}

#[derive(Debug)]
enum PeerConnectionSignal<T: HStream> {
    ConRequest(PeerConnectionRequest<T>),
}

pub struct PeerConnection<T: HStream, TS: HStartable<T>> {
    local_port: u16,
    peer_port: u16,
    signal_tx: tokio::sync::mpsc::Sender<PeerConnectionSignal<T>>,
    starter: Arc<TS>,
    req_id: AtomicU64,
    write_stream: Mutex<Option<WriteHalf<T>>>,
    read_channel: tokio::sync::broadcast::Sender<RecvMessage>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectionDirection {
    Incoming,
    Outgoing,
}

pub trait StartableStream<T: HStream> {
    fn connect(&self, addr: String) -> impl std::future::Future<Output = T> + Send;
}

pub struct TcpStreamStarter {}

impl StartableStream<TcpStream> for TcpStreamStarter {
    fn connect(&self, addr: String) -> impl std::future::Future<Output = TcpStream> + Send {
        async move {
            loop {
                match TcpStream::connect(&addr).await {
                    Ok(stream) => {
                        if let Err(e) = stream.set_nodelay(true) {
                            eprintln!("nodelay: {e}");
                        }
                        return stream;
                    }
                    Err(_) => sleep(Duration::from_secs(5)).await,
                }
            }
        }
    }
}

impl<T, TS> PeerConnection<T, TS>
where
    T: HStream,
    TS: HStartable<T>,
{
    fn init(
        local_port: u16,
        peer_port: u16,
        starter: Arc<TS>,
    ) -> (Arc<Self>, tokio::task::JoinHandle<()>) {
        let (signal_tx, signal_rx) = tokio::sync::mpsc::channel::<PeerConnectionSignal<T>>(16);

        let con = Arc::new(Self {
            local_port,
            peer_port,
            signal_tx,
            starter,
            req_id: AtomicU64::new(0),
            write_stream: Mutex::new(None),
            read_channel: tokio::sync::broadcast::channel(16).0,
        });

        let con_clone = con.clone();
        let handle = tokio::spawn(async move {
            con_clone.run_core(signal_rx).await;
        });
        (con, handle)
    }

    pub async fn send_data(
        self: Arc<Self>,
        data: Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut stream_lock = self.write_stream.lock().await;
        if let Some(stream) = stream_lock.as_mut() {
            let msg = Message::Data { payload: data };
            if let Err(e) = write_message(stream, &msg).await {
                stream_lock.take();
                return Err(e);
            }
            Ok(())
        } else {
            Err("No active connection".into())
        }
    }

    pub async fn send_request(
        self: Arc<Self>,
        data: Vec<u8>,
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let req_id = self
            .req_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let msg = Message::Req {
            req_id,
            payload: data,
        };

        let mut stream_lock = self.write_stream.lock().await;
        if let Some(stream) = stream_lock.as_mut() {
            if let Err(e) = write_message(stream, &msg).await {
                stream_lock.take();
                return Err(e);
            }
        } else {
            return Err("No active connection".into());
        }
        Ok(req_id)
    }

    pub async fn send_response(
        self: Arc<Self>,
        res_id: u64,
        data: Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let msg = Message::Res {
            res_id,
            payload: data,
        };

        let mut stream_lock = self.write_stream.lock().await;
        if let Some(stream) = stream_lock.as_mut() {
            if let Err(e) = write_message(stream, &msg).await {
                stream_lock.take();
                return Err(e);
            }
            Ok(())
        } else {
            Err("No active connection".into())
        }
    }

    pub async fn req_response(
        self: Arc<Self>,
        data: Vec<u8>,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        let mut rec = self.read_channel.subscribe();
        let req_id = self.send_request(data).await?;
        loop {
            let msg = rec.recv().await?;
            match msg {
                RecvMessage::Res { res_id, payload } if res_id == req_id => {
                    return Ok(payload);
                }
                _ => {
                    // Ignore other messages
                }
            }
        }
    }

    pub fn get_read_channel(self: Arc<Self>) -> tokio::sync::broadcast::Receiver<RecvMessage> {
        self.read_channel.subscribe()
    }

    async fn run_core(
        self: Arc<Self>,
        mut signal_rx: tokio::sync::mpsc::Receiver<PeerConnectionSignal<T>>,
    ) {
        loop {
            let addr: String = format!("127.0.0.1:{}", self.peer_port);
            let con: PeerConnectionRequest<T> = match signal_rx.try_recv() {
                Ok(PeerConnectionSignal::ConRequest(req)) => {
                    println!(
                        "[{}] SPECIAL CASE request from {}",
                        self.local_port, req.peer_port
                    );
                    req
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                    let selfclone = self.clone();
                    tokio::select! {
                        sig = signal_rx.recv() => {
                            match sig {
                                Some(PeerConnectionSignal::ConRequest(req)) => req,
                                None => {
                                    eprintln!("[{}] Signal channel closed", self.local_port);
                                    return;
                                }
                            }
                        },
                        mut stream = selfclone.starter.connect(addr) => {
                            let hello = Message::HandshakeInit {
                                client_id: self.local_port,
                            };
                            if let Err(e) = write_message(&mut stream, &hello).await {
                                eprintln!("[{}] Failed to send Hello: {}", self.local_port, e);
                                continue;
                            }
                            PeerConnectionRequest {
                                direction: ConnectionDirection::Outgoing,
                                peer_port: self.peer_port,
                                stream,
                            }
                        },
                    }
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                    eprintln!("[{}] Signal channel closed", self.local_port);
                    return;
                }
            };

            let peer_port = con.peer_port;
            let direction = con.direction;

            let mut signal_checker = async || {
                while let Some(sig) = signal_rx.recv().await {
                    match sig {
                        PeerConnectionSignal::ConRequest(req) => {
                            let is_connection_from_higher_port = (self.local_port >= peer_port
                                && direction == ConnectionDirection::Outgoing)
                                || (self.local_port < peer_port
                                    && direction == ConnectionDirection::Incoming);

                            if is_connection_from_higher_port {
                                println!(
                                    "[{}] [X {}] Closing duplicate (tie-breaker: {} > {})",
                                    self.local_port, peer_port, self.local_port, peer_port
                                );
                                return Some(req);
                            }
                        }
                    }
                }
                None
            };

            let (read_half, write_half) = tokio::io::split(con.stream);
            self.write_stream.lock().await.replace(write_half);

            let mut read_handle = {
                let read_channel = self.read_channel.clone();
                let local_port = self.local_port;
                let peer_port = peer_port;
                tokio::spawn(async move {
                    let mut reader = read_half;
                    loop {
                        match read_message(&mut reader).await {
                            Ok(Message::Data { payload }) => {
                                println!(
                                    "[{}] [< {}] Received data: {:?}",
                                    local_port, peer_port, payload
                                );
                                let _ = read_channel.send(RecvMessage::Data { payload });
                            }
                            Ok(Message::Req { req_id, payload }) => {
                                println!(
                                    "[{}] [< {}] Received request {}: {:?}",
                                    local_port, peer_port, req_id, payload
                                );
                                let _ = read_channel.send(RecvMessage::Req { req_id, payload });
                            }
                            Ok(Message::Res { res_id, payload }) => {
                                println!(
                                    "[{}] [< {}] Received response {}: {:?}",
                                    local_port, peer_port, res_id, payload
                                );
                                let _ = read_channel.send(RecvMessage::Res { res_id, payload });
                            }
                            Ok(_) => {
                                eprintln!(
                                    "[{}] [X {}] Unexpected message type",
                                    local_port, peer_port
                                );
                            }
                            Err(e) => {
                                eprintln!("[{}] [X {}] Read error: {}", local_port, peer_port, e);
                                break;
                            }
                        }
                    }
                })
            };

            tokio::select! {
                new_req = signal_checker() => {
                    match new_req {
                        Some(req) => {
                            // New connection request received, close current connection
                            println!(
                                "[{}] [X {}] Closing current connection due to new request",
                                self.local_port, peer_port
                            );
                            read_handle.abort();
                            self.write_stream.lock().await.take();
                            self.signal_tx
                                .send(PeerConnectionSignal::ConRequest(req))
                                .await
                                .unwrap();
                            continue;
                        }
                        None => {
                            // Signal channel closed
                            eprintln!("[{}] Signal channel closed", self.local_port);
                            read_handle.abort();
                            return;
                        }
                    }
                }
                _ = &mut read_handle => {
                    // Read task finished (likely due to error), clear write stream and reconnect
                    println!(
                        "[{}] [X {}] Connection closed, reconnecting...",
                        self.local_port, peer_port
                    );
                    self.write_stream.lock().await.take();
                    continue;
                }
            }
        }
    }
}
