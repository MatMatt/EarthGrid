//! Outbound URL policy — the one gate for every URL this node fetches on
//! someone else's behalf (peer URLs, replication sources, registered node URLs).
//!
//! - scheme must be `http` or `https`, the host a name or an IP literal,
//!   the whole URL at most [`MAX_URL_LEN`] characters;
//! - loopback, link-local, unspecified, multicast and cloud-metadata addresses
//!   are rejected outright;
//! - private ranges (RFC1918, RFC4193 unique-local, RFC6598 shared) are allowed
//!   only when the operator configured the host as a peer (see
//!   [`operator_hosts`]), so a LAN peer keeps working but neither a caller nor
//!   a self-registered node can point the node at an internal address.
//!
//! Callers must pair this with `reqwest::redirect::Policy::none()`, otherwise a
//! permitted URL could redirect the node into a blocked address.

use std::collections::HashSet;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, ToSocketAddrs};

use crate::error::{EarthGridError, Result};

/// Longest URL accepted.
pub const MAX_URL_LEN: usize = 2048;

/// Cloud metadata endpoints. Most sit in link-local space and are blocked
/// anyway; listed so they stay blocked even if a known peer claims the host.
const METADATA_V4: [Ipv4Addr; 4] = [
    Ipv4Addr::new(169, 254, 169, 254), // AWS, GCP, Azure, DigitalOcean, OpenStack
    Ipv4Addr::new(169, 254, 170, 2),   // AWS ECS task metadata
    Ipv4Addr::new(100, 100, 100, 200), // Alibaba Cloud
    Ipv4Addr::new(192, 0, 0, 192),     // Oracle Cloud
];
const METADATA_V6: Ipv6Addr = Ipv6Addr::new(0xfd00, 0x0ec2, 0, 0, 0, 0, 0, 0x0254); // AWS IMDS

#[derive(Debug, PartialEq)]
enum AddrClass {
    Public,
    Private,
    Blocked,
}

fn classify_v4(ip: Ipv4Addr) -> AddrClass {
    let o = ip.octets();
    if ip.is_loopback()
        || ip.is_link_local()
        || ip.is_unspecified()
        || o[0] == 0
        || ip.is_multicast()
        || ip.is_broadcast()
        || METADATA_V4.contains(&ip)
    {
        AddrClass::Blocked
    } else if ip.is_private() || (o[0] == 100 && (o[1] & 0xc0) == 64) {
        AddrClass::Private
    } else {
        AddrClass::Public
    }
}

fn classify_v6(ip: Ipv6Addr) -> AddrClass {
    // IPv4-mapped (::ffff:a.b.c.d) and NAT64 (64:ff9b::/96) embed an IPv4
    // address: judge them by it, or they would walk around the v4 rules.
    if let Some(v4) = ip.to_ipv4_mapped() {
        return classify_v4(v4);
    }
    let s = ip.segments();
    if s[0] == 0x0064 && s[1] == 0xff9b && s[2..6] == [0, 0, 0, 0] {
        let v4 = Ipv4Addr::new((s[6] >> 8) as u8, s[6] as u8, (s[7] >> 8) as u8, s[7] as u8);
        return classify_v4(v4);
    }
    if ip.is_loopback()
        || ip.is_unspecified()
        || ip.is_multicast()
        || (s[0] & 0xffc0) == 0xfe80 // link-local fe80::/10
        || ip == METADATA_V6
    {
        AddrClass::Blocked
    } else if (s[0] & 0xfe00) == 0xfc00 {
        // unique-local fc00::/7
        AddrClass::Private
    } else {
        AddrClass::Public
    }
}

fn classify(ip: IpAddr) -> AddrClass {
    match ip {
        IpAddr::V4(v4) => classify_v4(v4),
        IpAddr::V6(v6) => classify_v6(v6),
    }
}

fn rejected(why: &str) -> EarthGridError {
    EarthGridError::Other(format!("URL not allowed: {}", why))
}

/// Lower-cased host of `url` (IPv6 without brackets), as used for the
/// known-peer check. `None` if the URL does not parse or has no host.
pub fn host_of(url: &str) -> Option<String> {
    let parsed = reqwest::Url::parse(url).ok()?;
    let host = parsed.host_str()?;
    let host = host.trim_start_matches('[').trim_end_matches(']').to_ascii_lowercase();
    if host.is_empty() { None } else { Some(host) }
}

/// Hosts of the peers this node already knows, from their URLs.
pub fn known_hosts<'a>(urls: impl IntoIterator<Item = &'a str>) -> HashSet<String> {
    urls.into_iter().filter_map(host_of).collect()
}

/// Host of a libp2p multiaddr, lower-cased. `None` unless the **whole**
/// multiaddr is well-formed:
///
/// `/ip4/<dotted quad>` | `/ip6/<literal>` | `/dns|dns4|dns6/<name>`, then a
/// transport `/tcp/<port>` or `/udp/<port>`, then only recognised components
/// (`quic`, `quic-v1`, `ws`, `wss`, `tls`, `noise`, `webtransport`,
/// `p2p-circuit`, `p2p/<id>`, `ipfs/<id>`). The address family must match the
/// protocol. A malformed entry grants no private-address exception.
fn multiaddr_host(addr: &str) -> Option<String> {
    let mut parts = addr.split('/');
    if !parts.next()?.is_empty() {
        return None; // a multiaddr starts with '/'
    }
    let proto = parts.next()?;
    let host = parts.next()?;
    let host = match proto {
        "ip4" => host.parse::<Ipv4Addr>().ok()?.to_string(),
        "ip6" => host.parse::<Ipv6Addr>().ok()?.to_string(),
        "dns" | "dns4" | "dns6"
            if !host.is_empty()
                && host.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '.' || c == '_') =>
        {
            host.to_ascii_lowercase()
        }
        _ => return None,
    };

    // A recognised transport with a port must follow the address.
    let transport = parts.next()?;
    let port = parts.next()?;
    if !matches!(transport, "tcp" | "udp")
        || !port.bytes().all(|b| b.is_ascii_digit())
        || port.parse::<u16>().is_err()
    {
        return None;
    }

    // Whatever follows must be a recognised component, to the end.
    while let Some(component) = parts.next() {
        match component {
            "quic" | "quic-v1" | "ws" | "wss" | "tls" | "noise" | "webtransport" | "p2p-circuit" => {}
            "p2p" | "ipfs" => {
                let id = parts.next()?;
                if id.is_empty() || !id.chars().all(|c| c.is_ascii_alphanumeric()) {
                    return None;
                }
            }
            _ => return None,
        }
    }
    Some(host)
}

/// Host of an operator-written `http(s)` URL. `None` for any other scheme.
fn operator_url_host(entry: &str) -> Option<String> {
    let parsed = reqwest::Url::parse(entry).ok()?;
    if parsed.scheme() != "http" && parsed.scheme() != "https" {
        return None;
    }
    host_of(entry)
}

/// Hosts named by an operator-written peer list: comma-separated libp2p
/// multiaddrs (the `EARTHGRID_PEERS` / `EARTHGRID_BOOTSTRAP_PEERS` format) or
/// plain `http(s)` URLs. An entry that is not a well-formed one of those —
/// including a malformed multiaddr — is ignored and grants nothing.
pub fn hosts_from_peer_list(list: &str) -> HashSet<String> {
    list.split(',')
        .map(|e| e.trim_matches(|c: char| c.is_whitespace() || matches!(c, '[' | ']' | '"' | '\'')))
        .filter(|e| !e.is_empty())
        .filter_map(|e| if e.starts_with('/') { multiaddr_host(e) } else { operator_url_host(e) })
        .collect()
}

/// The hosts allowed to live in a private range — the `known_peer_hosts` every
/// caller passes to [`validate_outbound_url`].
///
/// Operator input only: `EARTHGRID_PEERS`, `EARTHGRID_BOOTSTRAP_PEERS` and the
/// `peers` list of `~/.earthgrid/config.json`. Never the peer registry, gossip
/// or the beacon registry — anything a remote party can write to must not be
/// able to grant itself the private-address exception. Read once per process.
pub fn operator_hosts() -> &'static HashSet<String> {
    static HOSTS: std::sync::OnceLock<HashSet<String>> = std::sync::OnceLock::new();
    HOSTS.get_or_init(|| {
        let mut hosts = HashSet::new();
        for var in ["EARTHGRID_PEERS", "EARTHGRID_BOOTSTRAP_PEERS"] {
            if let Ok(list) = std::env::var(var) {
                hosts.extend(hosts_from_peer_list(&list));
            }
        }
        let cfg_path = crate::config::Settings::config_dir().join("config.json");
        let configured = std::fs::read_to_string(&cfg_path)
            .ok()
            .and_then(|c| serde_json::from_str::<serde_json::Value>(&c).ok())
            .and_then(|v| v["peers"].as_array().cloned())
            .unwrap_or_default();
        for peer in configured.iter().filter_map(|p| p.as_str()) {
            hosts.extend(hosts_from_peer_list(peer));
        }
        hosts
    })
}

/// Validate a URL before the node makes any request to it.
///
/// `known_peer_hosts` is the set of hosts that may live in a private range.
/// Pass [`operator_hosts`]: it must never be derived from a registry that a
/// remote party can write to.
///
/// Resolves the host with `std::net`, which blocks — from async code use
/// [`validate_outbound_url_async`].
pub fn validate_outbound_url(url: &str, known_peer_hosts: &HashSet<String>) -> Result<()> {
    if url.len() > MAX_URL_LEN {
        return Err(rejected("longer than 2048 characters"));
    }
    let parsed = reqwest::Url::parse(url).map_err(|_| rejected("not a valid URL"))?;
    if parsed.scheme() != "http" && parsed.scheme() != "https" {
        return Err(rejected("scheme must be http or https"));
    }
    let host = host_of(url).ok_or_else(|| rejected("no host"))?;
    let port = parsed.port_or_known_default().unwrap_or(80);

    let addrs: Vec<IpAddr> = if let Ok(ip) = host.parse::<IpAddr>() {
        vec![ip]
    } else {
        if !host.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '.' || c == '_') {
            return Err(rejected("host is not a name or IP literal"));
        }
        (host.as_str(), port)
            .to_socket_addrs()
            .map_err(|_| rejected("host does not resolve"))?
            .map(|a| a.ip())
            .collect()
    };
    if addrs.is_empty() {
        return Err(rejected("host does not resolve"));
    }

    // Every address the name resolves to must pass, not just the first.
    for ip in addrs {
        match classify(ip) {
            AddrClass::Blocked => {
                return Err(rejected(
                    "loopback, link-local, unspecified, multicast and metadata addresses are blocked",
                ));
            }
            AddrClass::Private if !known_peer_hosts.contains(&host) => {
                return Err(rejected("private address that is not a known peer"));
            }
            _ => {}
        }
    }
    Ok(())
}

/// [`validate_outbound_url`] for async callers: runs the blocking DNS
/// resolution off the async workers.
pub async fn validate_outbound_url_async(url: &str, known_peer_hosts: &HashSet<String>) -> Result<()> {
    let url = url.to_string();
    let known = known_peer_hosts.clone();
    tokio::task::spawn_blocking(move || validate_outbound_url(&url, &known))
        .await
        .unwrap_or_else(|e| Err(EarthGridError::Other(format!("URL validation failed: {}", e))))
}

// ---------------------------------------------------------------------------
// Tests (IP literals only — no DNS, no network)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn none() -> HashSet<String> {
        HashSet::new()
    }

    #[test]
    fn rejects_bad_scheme_host_and_length() {
        for url in ["ftp://93.184.216.34/", "file:///etc/passwd", "gopher://93.184.216.34", "not a url", "http://", ""] {
            assert!(validate_outbound_url(url, &none()).is_err(), "{url} must be rejected");
        }
        let long = format!("http://93.184.216.34/{}", "a".repeat(MAX_URL_LEN));
        assert!(validate_outbound_url(&long, &none()).is_err());
        assert!(validate_outbound_url("http://93.184.216.34:8400", &none()).is_ok());
        assert!(validate_outbound_url("https://93.184.216.34/earthgrid", &none()).is_ok());
    }

    #[test]
    fn blocks_loopback_linklocal_metadata_outright() {
        let urls = [
            "http://127.0.0.1:8400",
            "http://2130706433/",           // 127.0.0.1 as a decimal
            "http://[::1]:8400",
            "http://[::ffff:127.0.0.1]/",   // IPv4-mapped loopback
            "http://0.0.0.0/",
            "http://[::]/",
            "http://169.254.169.254/latest/meta-data/",
            "http://169.254.10.10/",
            "http://[fe80::1]/",
            "http://224.0.0.1/",
            "http://[ff02::1]/",
            "http://100.100.100.200/",
            "http://[fd00:ec2::254]/",
        ];
        for url in urls {
            assert!(validate_outbound_url(url, &none()).is_err(), "{url} must be blocked");
            // Being a "known peer" never unblocks these.
            let known = known_hosts([url]);
            assert!(validate_outbound_url(url, &known).is_err(), "{url} must stay blocked when known");
        }
    }

    #[test]
    fn private_ranges_only_for_known_peers() {
        let lan = [
            "http://192.168.188.219:8400",
            "http://10.0.0.5:8400",
            "http://172.16.3.4:8400",
            "http://100.64.0.7:8400",
            "http://[fd12:3456::1]:8400",
        ];
        for url in lan {
            assert!(validate_outbound_url(url, &none()).is_err(), "{url} must need a known peer");
            let known = known_hosts([url]);
            assert!(validate_outbound_url(url, &known).is_ok(), "{url} must work for a known peer");
        }
        // A different private host is not covered by someone else's entry.
        let known = known_hosts(["http://192.168.188.219:8400"]);
        assert!(validate_outbound_url("http://192.168.188.1/", &known).is_err());
        // Known by host: a different port or path on the same host is fine.
        assert!(validate_outbound_url("http://192.168.188.219:9000/api", &known).is_ok());
    }

    #[test]
    fn private_exception_comes_from_operator_multiaddrs() {
        let hosts = hosts_from_peer_list(
            "/ip4/192.168.188.219/tcp/4001/p2p/12D3KooWExample, /ip6/fd12:3456::1/tcp/4001,\
             /dns4/Peer.Example/tcp/4001,http://10.0.0.5:8400,/p2p/12D3KooWExample,garbage,",
        );
        let expected: HashSet<String> = ["192.168.188.219", "fd12:3456::1", "peer.example", "10.0.0.5"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert_eq!(hosts, expected);

        // The bootstrap peer keeps working over HTTP; its LAN neighbours do not.
        assert!(validate_outbound_url("http://192.168.188.219:8400", &hosts).is_ok());
        assert!(validate_outbound_url("http://192.168.188.1:8400", &hosts).is_err());
        // An operator entry never unblocks loopback or metadata addresses.
        let bad = hosts_from_peer_list("/ip4/127.0.0.1/tcp/4001,/ip4/169.254.169.254/tcp/80");
        assert!(validate_outbound_url("http://127.0.0.1:8400", &bad).is_err());
        assert!(validate_outbound_url("http://169.254.169.254/", &bad).is_err());
        // JSON-list spelling of the env var (`EARTHGRID_PEERS=[]`) is tolerated.
        assert!(hosts_from_peer_list("[]").is_empty());
        assert!(hosts_from_peer_list("[\"/ip4/10.1.2.3/tcp/4001\"]").contains("10.1.2.3"));
    }

    #[test]
    fn malformed_operator_entries_grant_nothing() {
        let malformed = [
            "/ip4/10.0.0.7/not-a-protocol",       // no recognised transport
            "/ip4/10.0.0.7",                      // no transport at all
            "/ip4/10.0.0.7/tcp",                  // transport without a port
            "/ip4/10.0.0.7/tcp/notaport",
            "/ip4/10.0.0.7/tcp/+4001",
            "/ip4/10.0.0.7/tcp/70000",
            "/ip4/10.0.0.7/tcp/4001/",            // empty trailing component
            "/ip4/10.0.0.7/tcp/4001/bogus",       // unknown component after the transport
            "/ip4/10.0.0.7/tcp/4001/p2p",         // p2p without an id
            "/ip4/fd12:3456::1/tcp/4001",         // wrong family for the protocol
            "/ip6/10.0.0.7/tcp/4001",
            "/ip4/10.0.7/tcp/4001",               // not a dotted quad
            "/ip4/010.0.0.7/tcp/4001",
            "/dns4//tcp/4001",
            "/dns4/bad host/tcp/4001",
            "10.0.0.7",                           // neither a multiaddr nor a URL
            "10.0.0.7:8400",
            "ftp://10.0.0.7/",                    // a URL, but not http(s)
        ];
        for entry in malformed {
            assert!(hosts_from_peer_list(entry).is_empty(), "{entry} must grant nothing");
        }
        // Well-formed multiaddrs with further recognised components still work
        for entry in ["/ip4/10.0.0.7/udp/4001/quic-v1", "/ip4/10.0.0.7/tcp/4001/ws/p2p/12D3KooWExample/p2p-circuit"] {
            assert!(hosts_from_peer_list(entry).contains("10.0.0.7"), "{entry} must be accepted");
        }
    }
}
