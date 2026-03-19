//! libp2p networking for EarthGrid — P2P peer discovery, NAT traversal, chunk transfer.
//!
//! Runs alongside the HTTP server. Provides:
//! - Kademlia DHT for decentralized peer discovery
//! - mDNS for LAN peer discovery
//! - Relay + DCUtR for NAT hole-punching
//! - Request/Response for chunk transfer and catalog queries

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::time::Duration;

use futures::StreamExt;
use libp2p::{
    Multiaddr, PeerId, StreamProtocol, Swarm, SwarmBuilder,
    identify, kad, mdns, ping, relay, dcutr,
    request_response::{self, ProtocolSupport},
    swarm::NetworkBehaviour,
};
use tokio::sync::mpsc;
use tracing::{info, warn, debug};

use crate::transport::{EarthGridRpc, EarthGridRequest, EarthGridResponse};

// ---------------------------------------------------------------------------
// Behaviour — combines all libp2p protocols
// ---------------------------------------------------------------------------

#[derive(NetworkBehaviour)]
pub struct EarthGridBehaviour {
    pub kademlia: kad::Behaviour<kad::store::MemoryStore>,
    pub identify: identify::Behaviour,
    pub mdns: mdns::tokio::Behaviour,
    pub ping: ping::Behaviour,
    pub relay_client: relay::client::Behaviour,
    pub dcutr: dcutr::Behaviour,
    pub rpc: EarthGridRpc,
}

// ---------------------------------------------------------------------------
// Events for the application layer
// ---------------------------------------------------------------------------

/// Events sent from the network layer to the application.
#[derive(Debug)]
pub enum NetworkEvent {
    PeerDiscovered {
        peer_id: PeerId,
        addresses: Vec<Multiaddr>,
    },
    PeerLost(PeerId),
    InboundRequest {
        peer: PeerId,
        request: EarthGridRequest,
        channel: request_response::ResponseChannel<EarthGridResponse>,
    },
}

/// Commands sent from the application to the network layer.
pub enum NetworkCommand {
    /// Send a request to a specific peer.
    SendRequest {
        peer: PeerId,
        request: EarthGridRequest,
        response_tx: tokio::sync::oneshot::Sender<Result<EarthGridResponse, String>>,
    },
    /// Send a response back on a request-response channel.
    SendResponse {
        channel: request_response::ResponseChannel<EarthGridResponse>,
        response: EarthGridResponse,
    },
    /// Announce this node as a provider for a key (collection name).
    Provide { key: String },
    /// Find providers for a key (collection name).
    FindProviders { key: String },
    /// Bootstrap Kademlia from known peers.
    Bootstrap,
}

// NetworkCommand can't derive Debug because ResponseChannel doesn't impl Debug
impl std::fmt::Debug for NetworkCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SendRequest { peer, .. } => write!(f, "SendRequest({})", peer),
            Self::SendResponse { .. } => write!(f, "SendResponse"),
            Self::Provide { key } => write!(f, "Provide({})", key),
            Self::FindProviders { key } => write!(f, "FindProviders({})", key),
            Self::Bootstrap => write!(f, "Bootstrap"),
        }
    }
}

// ---------------------------------------------------------------------------
// Keypair management
// ---------------------------------------------------------------------------

/// Load or generate an Ed25519 keypair.
/// Stored at `data_dir/node.key` (raw 64-byte secret + public).
fn load_or_generate_keypair(data_dir: &Path) -> libp2p::identity::Keypair {
    let key_path = data_dir.join("node.key");

    if key_path.exists() {
        if let Ok(bytes) = std::fs::read(&key_path) {
            // ed25519_from_bytes expects exactly 32-byte secret key
            let secret = if bytes.len() == 64 { bytes[..32].to_vec() } else { bytes };
            if let Ok(kp) = libp2p::identity::Keypair::ed25519_from_bytes(secret) {
                info!("Loaded keypair from {}", key_path.display());
                return kp;
            }
            warn!("Failed to parse keypair from {}, generating new", key_path.display());
        }
    }

    let kp = libp2p::identity::Keypair::generate_ed25519();
    // Save the 64-byte keypair (secret + public) for reload
    if let Ok(ed_kp) = kp.clone().try_into_ed25519() {
        let _ = std::fs::create_dir_all(data_dir);
        // Store full 64 bytes; on reload we extract the 32-byte secret
        if std::fs::write(&key_path, ed_kp.to_bytes()).is_ok() {
            info!("Generated new keypair, saved to {}", key_path.display());
        }
    }
    kp
}

// ---------------------------------------------------------------------------
// Network startup
// ---------------------------------------------------------------------------

/// Configuration for the libp2p network.
pub struct NetworkConfig {
    pub data_dir: PathBuf,
    pub listen_port: u16,
    pub bootstrap_peers: Vec<String>,
    pub node_name: String,
}

/// Start the libp2p network.
///
/// Returns channels for communication with the swarm event loop:
/// - `event_rx`: receive network events (peer discovered, inbound requests)
/// - `cmd_tx`: send commands to the network (send request, provide, bootstrap)
/// - `PeerId`: this node's peer ID
pub async fn start(
    config: NetworkConfig,
) -> anyhow::Result<(
    mpsc::Receiver<NetworkEvent>,
    mpsc::Sender<NetworkCommand>,
    PeerId,
)> {
    let keypair = load_or_generate_keypair(&config.data_dir);
    let local_peer_id = PeerId::from(keypair.public());
    info!(%local_peer_id, "Starting libp2p network");

    // Build swarm
    let mut swarm = SwarmBuilder::with_existing_identity(keypair)
        .with_tokio()
        .with_tcp(
            libp2p::tcp::Config::default(),
            libp2p::noise::Config::new,
            libp2p::yamux::Config::default,
        )?
        .with_relay_client(
            libp2p::noise::Config::new,
            libp2p::yamux::Config::default,
        )?
        .with_behaviour(|keypair, relay_client| {
            // Kademlia
            let mut kad_config = kad::Config::new(
                StreamProtocol::new("/earthgrid/kad/1.0.0")
            );
            kad_config.set_query_timeout(Duration::from_secs(30));

            let store = kad::store::MemoryStore::new(keypair.public().to_peer_id());
            let kademlia = kad::Behaviour::with_config(
                keypair.public().to_peer_id(),
                store,
                kad_config,
            );

            // Identify
            let identify = identify::Behaviour::new(identify::Config::new(
                "/earthgrid/id/1.0.0".to_string(),
                keypair.public(),
            ));

            // mDNS (LAN discovery)
            let mdns = mdns::tokio::Behaviour::new(
                mdns::Config::default(),
                keypair.public().to_peer_id(),
            )?;

            // Request-Response (chunk transfer, catalog queries, job delegation)
            // cbor::Behaviour is a type alias for request_response::Behaviour<cbor::codec::Codec<Req, Resp>>
            let rpc = EarthGridRpc::new(
                [(StreamProtocol::new("/earthgrid/rpc/1.0.0"), ProtocolSupport::Full)],
                request_response::Config::default()
                    .with_request_timeout(Duration::from_secs(300)),
            );

            Ok(EarthGridBehaviour {
                kademlia,
                identify,
                mdns,
                ping: ping::Behaviour::default(),
                relay_client,
                dcutr: dcutr::Behaviour::new(keypair.public().to_peer_id()),
                rpc,
            })
        })?
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(300)))
        .build();

    // Listen on TCP
    let listen_addr: Multiaddr = format!("/ip4/0.0.0.0/tcp/{}", config.listen_port).parse()?;
    swarm.listen_on(listen_addr)?;

    // Add bootstrap peers to Kademlia
    for addr_str in &config.bootstrap_peers {
        if let Ok(addr) = addr_str.parse::<Multiaddr>() {
            if let Some(peer_id) = extract_peer_id(&addr) {
                swarm.behaviour_mut().kademlia.add_address(&peer_id, addr.clone());
                info!(%peer_id, %addr, "Added bootstrap peer");
            }
        }
    }

    // Channels
    let (event_tx, event_rx) = mpsc::channel(256);
    let (cmd_tx, cmd_rx) = mpsc::channel(256);

    // Spawn the swarm event loop
    tokio::spawn(swarm_loop(swarm, event_tx, cmd_rx));

    Ok((event_rx, cmd_tx, local_peer_id))
}

// ---------------------------------------------------------------------------
// Extract PeerId from a multiaddr (if it ends with /p2p/<peer_id>)
// ---------------------------------------------------------------------------

fn extract_peer_id(addr: &Multiaddr) -> Option<PeerId> {
    addr.iter().find_map(|proto| {
        if let libp2p::multiaddr::Protocol::P2p(peer_id) = proto {
            Some(peer_id)
        } else {
            None
        }
    })
}

// ---------------------------------------------------------------------------
// Swarm event loop
// ---------------------------------------------------------------------------

async fn swarm_loop(
    mut swarm: Swarm<EarthGridBehaviour>,
    event_tx: mpsc::Sender<NetworkEvent>,
    mut cmd_rx: mpsc::Receiver<NetworkCommand>,
) {
    use libp2p::swarm::SwarmEvent;

    // Track pending request-response round-trips
    let mut pending_responses: std::collections::HashMap<
        request_response::OutboundRequestId,
        tokio::sync::oneshot::Sender<Result<EarthGridResponse, String>>,
    > = Default::default();

    loop {
        tokio::select! {
            // Process commands from the application
            Some(cmd) = cmd_rx.recv() => {
                match cmd {
                    NetworkCommand::SendRequest { peer, request, response_tx } => {
                        let req_id = swarm.behaviour_mut().rpc.send_request(&peer, request);
                        pending_responses.insert(req_id, response_tx);
                    }
                    NetworkCommand::SendResponse { channel, response } => {
                        let _ = swarm.behaviour_mut().rpc.send_response(channel, response);
                    }
                    NetworkCommand::Provide { key } => {
                        let record_key = kad::RecordKey::new(&key_hash(&key));
                        let _ = swarm.behaviour_mut().kademlia.start_providing(record_key);
                    }
                    NetworkCommand::FindProviders { key } => {
                        let record_key = kad::RecordKey::new(&key_hash(&key));
                        swarm.behaviour_mut().kademlia.get_providers(record_key);
                    }
                    NetworkCommand::Bootstrap => {
                        let _ = swarm.behaviour_mut().kademlia.bootstrap();
                    }
                }
            }

            // Process swarm events
            event = swarm.select_next_some() => {
                match event {
                    // --- mDNS ---
                    SwarmEvent::Behaviour(EarthGridBehaviourEvent::Mdns(
                        mdns::Event::Discovered(peers)
                    )) => {
                        for (peer_id, addr) in &peers {
                            info!(%peer_id, %addr, "mDNS: discovered peer");
                            swarm.behaviour_mut().kademlia.add_address(peer_id, addr.clone());
                        }
                        for (peer_id, addr) in peers {
                            let _ = event_tx.send(NetworkEvent::PeerDiscovered {
                                peer_id,
                                addresses: vec![addr],
                            }).await;
                        }
                    }
                    SwarmEvent::Behaviour(EarthGridBehaviourEvent::Mdns(
                        mdns::Event::Expired(peers)
                    )) => {
                        for (peer_id, _addr) in peers {
                            debug!(%peer_id, "mDNS: peer expired");
                            let _ = event_tx.send(NetworkEvent::PeerLost(peer_id)).await;
                        }
                    }

                    // --- Identify ---
                    SwarmEvent::Behaviour(EarthGridBehaviourEvent::Identify(
                        identify::Event::Received { peer_id, info, .. }
                    )) => {
                        info!(
                            %peer_id,
                            agent = %info.agent_version,
                            "Identified peer"
                        );
                        // Add all addresses to Kademlia
                        for addr in &info.listen_addrs {
                            swarm.behaviour_mut().kademlia.add_address(&peer_id, addr.clone());
                        }
                    }

                    // --- Kademlia ---
                    SwarmEvent::Behaviour(EarthGridBehaviourEvent::Kademlia(
                        kad::Event::RoutingUpdated { peer, .. }
                    )) => {
                        debug!(%peer, "Kademlia routing updated");
                    }

                    // --- Request-Response ---
                    SwarmEvent::Behaviour(EarthGridBehaviourEvent::Rpc(
                        request_response::Event::Message { peer, message }
                    )) => {
                        match message {
                            request_response::Message::Request { request, channel, .. } => {
                                let _ = event_tx.send(NetworkEvent::InboundRequest {
                                    peer,
                                    request,
                                    channel,
                                }).await;
                            }
                            request_response::Message::Response { request_id, response } => {
                                if let Some(tx) = pending_responses.remove(&request_id) {
                                    let _ = tx.send(Ok(response));
                                }
                            }
                        }
                    }
                    SwarmEvent::Behaviour(EarthGridBehaviourEvent::Rpc(
                        request_response::Event::OutboundFailure { request_id, error, .. }
                    )) => {
                        if let Some(tx) = pending_responses.remove(&request_id) {
                            let _ = tx.send(Err(format!("Request failed: {:?}", error)));
                        }
                    }

                    // --- Connection events ---
                    SwarmEvent::NewListenAddr { address, .. } => {
                        info!(%address, "Listening on");
                    }
                    SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                        debug!(%peer_id, "Connection established");
                    }
                    SwarmEvent::ConnectionClosed { peer_id, .. } => {
                        debug!(%peer_id, "Connection closed");
                    }

                    _ => {}
                }
            }
        }
    }
}

/// Hash a string key for Kademlia record keys.
fn key_hash(key: &str) -> Vec<u8> {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish().to_be_bytes().to_vec()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_keypair_generation() {
        let dir = tempfile::tempdir().unwrap();
        let kp1 = load_or_generate_keypair(dir.path());
        let id1 = PeerId::from(kp1.public());

        // Loading again should give same keypair
        let kp2 = load_or_generate_keypair(dir.path());
        let id2 = PeerId::from(kp2.public());
        assert_eq!(id1, id2);
    }

    #[test]
    fn test_key_hash_deterministic() {
        let h1 = key_hash("sentinel-2-l2a");
        let h2 = key_hash("sentinel-2-l2a");
        assert_eq!(h1, h2);

        let h3 = key_hash("sentinel-1-rtc");
        assert_ne!(h1, h3);
    }

    #[test]
    fn test_extract_peer_id() {
        // With peer ID
        let addr: Multiaddr = "/ip4/127.0.0.1/tcp/8400/p2p/12D3KooWDpJ7As7BWAwRMfu1VU2WCqNjvq387JEYKDBj4kx6nXTN".parse().unwrap();
        assert!(extract_peer_id(&addr).is_some());

        // Without peer ID
        let addr2: Multiaddr = "/ip4/127.0.0.1/tcp/8400".parse().unwrap();
        assert!(extract_peer_id(&addr2).is_none());
    }
}
