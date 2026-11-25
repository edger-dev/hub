#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Peer hubs: a consumer on one hub sees and reaches another hub's nodes one
//! hop away, as `<peer>::<node>`, and follows its events.
//!
//! implements: a297d880b240fa86f9872753ebc3009c0d3af0dd572ec9eaa2a17a3f815f8d45@55ffeceb39795324eb405923fdba976d6b9a2e959425e4dee7511dc681794e87

use std::sync::Arc;
use std::time::Duration;

use hub::{Dialer, Peers, ServedHub, serve_connection};
use hub_protocol::service::ConsumerApiClient;
use hub_protocol::{HubEvent, LinkLabel, LinkSelector, NodeId, RouteError, RoutedCall};
use node::files::{FileContents, PathRequest};
use node::{Connectors, FilesConnector};

fn attach(hub: &ServedHub) -> vox::MemoryLink {
    let (peer, hub_link) = vox::memory_link_pair(64);
    let hub = hub.clone();
    tokio::spawn(async move {
        let _ = serve_connection(hub, hub_link).await;
    });
    peer
}

/// A dialer reaching `hub` in memory.
fn dialer(hub: &ServedHub) -> Dialer {
    let hub = hub.clone();
    Arc::new(move || {
        let link = attach(&hub);
        Box::pin(async move {
            vox::initiator_on(link)
                .establish_connection()
                .await
                .map_err(|e| format!("{e:?}"))
        })
    })
}

async fn consumer(hub: &ServedHub) -> ConsumerApiClient {
    vox::initiator_on(attach(hub))
        .establish_connection()
        .await
        .expect("establish")
        .open_lane()
        .await
        .expect("lane")
}

/// A node named `name` on `hub`, serving a files connector over `dir`.
async fn node_on(hub: &ServedHub, name: &str, dir: &std::path::Path) -> vox::ConnectionHandle {
    let mut connectors = Connectors::new();
    connectors.register(Box::new(FilesConnector::new("files-0", dir)));
    let config = node::NodeConfig::new(
        NodeId(name.to_string()),
        LinkLabel::stable(),
        Some(node::HubEndpoint("memory://peers".to_string())),
        Vec::new(),
        "1.0.0".to_string(),
    )
    .expect("config");
    tokio::time::timeout(
        Duration::from_secs(10),
        node::serve_and_register_with(attach(hub), &config, Arc::new(connectors)),
    )
    .await
    .expect("no timeout")
    .expect("node registers")
}

fn read(path: &str) -> RoutedCall {
    RoutedCall {
        connector: "files-0".to_string(),
        method: "read".to_string(),
        payload: facet_json::to_string(&PathRequest {
            path: path.to_string(),
        })
        .unwrap()
        .into_bytes(),
    }
}

fn names(snapshot: &hub_protocol::TopologySnapshot) -> Vec<String> {
    let mut n: Vec<String> = snapshot.nodes.iter().map(|n| n.node.0.clone()).collect();
    n.sort();
    n
}

/// Until `check` holds, for up to five seconds.
async fn eventually<F, Fut>(mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    for _ in 0..100 {
        if check().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("never held");
}

/// Two hubs, `pc` and `edger`, each peered with the other.
fn two_hubs() -> (ServedHub, ServedHub) {
    let pc = ServedHub::new().with_peers(Peers::new("pc"));
    let edger = ServedHub::new().with_peers(Peers::new("edger"));
    pc.peers().add("edger", dialer(&edger)).unwrap();
    edger.peers().add("pc", dialer(&pc)).unwrap();
    (pc, edger)
}

#[test]
fn a_qualified_id_names_the_peer_and_the_node() {
    let q = NodeId::qualified("edger", &NodeId("yolo".into()));
    assert_eq!(q.0, "edger::yolo");
    assert_eq!(q.split_peer(), Some(("edger", NodeId("yolo".into()))));
    assert_eq!(NodeId("yolo".into()).split_peer(), None);
}

#[test]
fn hub_peers_parses_name_equals_addr() {
    assert_eq!(
        hub::parse_peers(" edger=10.0.0.2:15400 , b=h:1").unwrap(),
        vec![
            ("edger".to_string(), "10.0.0.2:15400".to_string()),
            ("b".to_string(), "h:1".to_string())
        ]
    );
    assert_eq!(hub::parse_peers("").unwrap(), vec![]);
    assert!(hub::parse_peers("no-equals").is_err());
    assert!(
        hub::parse_peers("a::b=h:1").is_err(),
        "a peer name may not hold ::"
    );
    assert!(
        hub::parse_peers("a=h:1,a=h:2").is_err(),
        "a peer named twice"
    );
}

#[tokio::test]
async fn a_node_name_holding_the_separator_is_refused() {
    let hub = ServedHub::new();
    let dir = tempdir::TempDir::new("peers-sep").unwrap();
    let mut connectors = Connectors::new();
    connectors.register(Box::new(FilesConnector::new("files-0", dir.path())));
    let config = node::NodeConfig::new(
        NodeId("edger::yolo".to_string()),
        LinkLabel::stable(),
        Some(node::HubEndpoint("memory://peers".to_string())),
        Vec::new(),
        "1.0.0".to_string(),
    )
    .expect("config");
    let refused = tokio::time::timeout(
        Duration::from_secs(10),
        node::serve_and_register_with(attach(&hub), &config, Arc::new(connectors)),
    )
    .await
    .expect("no timeout");
    assert!(
        matches!(refused, Err(node::DialError::Rejected(_))),
        "{:?}",
        refused.err()
    );
    assert!(hub.snapshot().await.nodes.is_empty());
}

#[tokio::test]
async fn a_consumer_sees_and_reaches_a_peers_node() {
    let (pc, edger) = two_hubs();
    let dir = tempdir::TempDir::new("peers-route").unwrap();
    std::fs::write(dir.path().join("motd.txt"), "hello from edger").unwrap();
    let _yolo = node_on(&edger, "yolo", dir.path()).await;
    let _local = node_on(&pc, "local", dir.path()).await;

    let on_pc = consumer(&pc).await;
    eventually(|| {
        let on_pc = on_pc.clone();
        async move { names(&on_pc.topology().await.unwrap()) == ["edger::yolo", "local"] }
    })
    .await;
    // What a peer reads: only the hub's own nodes, so two hubs never recurse.
    assert_eq!(names(&on_pc.local_topology().await.unwrap()), ["local"]);

    let reply = on_pc
        .route(
            LinkSelector::node(NodeId("edger::yolo".into())),
            read("motd.txt"),
        )
        .await
        .expect("routed one hop");
    let contents: FileContents =
        facet_json::from_str(std::str::from_utf8(&reply.payload).unwrap()).unwrap();
    assert_eq!(contents.contents, "hello from edger");

    // The hub's own name qualifies its own nodes too.
    on_pc
        .route(
            LinkSelector::node(NodeId("pc::local".into())),
            read("motd.txt"),
        )
        .await
        .expect("own name is local");

    // A peer's node refusing is still the node's error.
    let err = on_pc
        .route(
            LinkSelector::node(NodeId("edger::yolo".into())),
            read("../escape"),
        )
        .await
        .expect_err("refused");
    assert!(
        matches!(&err, vox::VoxError::User(e) if matches!(**e, RouteError::Node(_))),
        "{err:?}"
    );
}

#[tokio::test]
async fn one_outbound_peer_connection_carries_both_directions() {
    let public = ServedHub::new().with_peers(Peers::new("hub"));
    let edger = ServedHub::new().with_peers(Peers::new("edger"));
    edger.peers().add("hub", dialer(&public)).unwrap();

    let edger_dir = tempdir::TempDir::new("reverse-peer-edger").unwrap();
    std::fs::write(edger_dir.path().join("motd.txt"), "hello from edger").unwrap();
    let public_dir = tempdir::TempDir::new("reverse-peer-public").unwrap();
    std::fs::write(public_dir.path().join("motd.txt"), "hello from public").unwrap();
    let _yolo = node_on(&edger, "yolo", edger_dir.path()).await;
    let _public_node = node_on(&public, "public-node", public_dir.path()).await;
    let on_edger = consumer(&edger).await;
    let on_public = consumer(&public).await;

    eventually(|| {
        let (on_edger, on_public) = (on_edger.clone(), on_public.clone());
        async move {
            names(&on_edger.topology().await.unwrap()) == ["hub::public-node"]
                && names(&on_public.topology().await.unwrap()) == ["edger::yolo"]
        }
    })
    .await;

    for (client, node, expected) in [
        (&on_edger, "hub::public-node", "hello from public"),
        (&on_public, "edger::yolo", "hello from edger"),
    ] {
        let reply = client
            .route(LinkSelector::node(NodeId(node.into())), read("motd.txt"))
            .await
            .expect("routed over the reverse-registered connection");
        let contents: FileContents =
            facet_json::from_str(std::str::from_utf8(&reply.payload).unwrap()).unwrap();
        assert_eq!(contents.contents, expected);
    }
}

#[tokio::test]
async fn an_unreachable_peer_hides_its_nodes_and_its_calls_fail_fast() {
    let pc = ServedHub::new().with_peers(Peers::new("pc"));
    pc.peers()
        .add(
            "edger",
            Arc::new(|| Box::pin(async { Err("down".to_string()) })),
        )
        .unwrap();
    let dir = tempdir::TempDir::new("peers-down").unwrap();
    let _local = node_on(&pc, "local", dir.path()).await;
    let on_pc = consumer(&pc).await;
    let started = std::time::Instant::now();
    assert_eq!(names(&on_pc.topology().await.unwrap()), ["local"]);
    let err = on_pc
        .route(LinkSelector::node(NodeId("edger::yolo".into())), read("x"))
        .await
        .expect_err("no peer");
    assert!(
        matches!(&err, vox::VoxError::User(e) if matches!(**e, RouteError::Hub(hub_protocol::HubError::UnknownNode))),
        "{err:?}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "fast: {:?}",
        started.elapsed()
    );
}

fn owned(got: vox::SelfRef<HubEvent>) -> HubEvent {
    let mut out = None;
    let _ = got.map(|e| out = Some(e));
    out.expect("an event")
}

/// The topics received from `rx` until every one of `wanted` has come (5 s at
/// most).
async fn until_all(rx: &mut vox::Rx<HubEvent>, wanted: &[&str]) -> Vec<String> {
    let mut seen: Vec<String> = Vec::new();
    let done = |seen: &[String]| wanted.iter().all(|w| seen.iter().any(|t| t == w));
    let _ = tokio::time::timeout(Duration::from_secs(5), async {
        while !done(&seen) {
            match rx.recv().await {
                Ok(Some(event)) => seen.push(owned(event).topic),
                _ => break,
            }
        }
    })
    .await;
    assert!(done(&seen), "wanted {wanted:?}, saw {seen:?}");
    seen
}

async fn publish(c: &ConsumerApiClient, topic: &str) {
    c.publish(HubEvent {
        topic: topic.to_string(),
        payload: Vec::new(),
    })
    .await
    .unwrap();
}

async fn subscribe(c: &ConsumerApiClient) -> vox::Rx<HubEvent> {
    let (tx, rx) = vox::channel::<HubEvent>();
    let c = c.clone();
    tokio::spawn(async move {
        let _ = c.subscribe_events("kudo/".into(), tx).await;
    });
    rx
}

#[tokio::test]
async fn an_event_crosses_one_hop_and_never_echoes() {
    let (pc, edger) = two_hubs();
    let on_pc = consumer(&pc).await;
    let on_edger = consumer(&edger).await;
    let mut pc_rx = subscribe(&on_pc).await;
    let mut edger_rx = subscribe(&on_edger).await;

    // Until each side follows the other: probe until a probe crosses.
    for (from, rx) in [(&on_edger, &mut pc_rx), (&on_pc, &mut edger_rx)] {
        let mut crossed = false;
        for n in 0..100 {
            let topic = format!("kudo/probe/{n}");
            publish(from, &topic).await;
            let got = tokio::time::timeout(Duration::from_millis(50), async {
                loop {
                    match rx.recv().await {
                        Ok(Some(e)) => {
                            if owned(e).topic.starts_with("kudo/probe/") {
                                return;
                            }
                        }
                        _ => std::future::pending::<()>().await,
                    }
                }
            })
            .await;
            if got.is_ok() {
                crossed = true;
                break;
            }
        }
        assert!(crossed, "a peer's subscription never came up");
    }

    // One event from each side reaches each subscriber exactly once.
    publish(&on_edger, "kudo/from-edger").await;
    publish(&on_pc, "kudo/from-pc").await;
    publish(&on_edger, "kudo/end-edger").await;
    publish(&on_pc, "kudo/end-pc").await;
    for rx in [&mut pc_rx, &mut edger_rx] {
        let mut seen = until_all(rx, &["kudo/end-edger", "kudo/end-pc"]).await;
        // Anything echoed would come after its original: give it a moment.
        if let Ok(Ok(Some(e))) = tokio::time::timeout(Duration::from_millis(200), rx.recv()).await {
            seen.push(owned(e).topic);
        }
        for topic in ["kudo/from-edger", "kudo/from-pc"] {
            assert_eq!(
                seen.iter().filter(|t| *t == topic).count(),
                1,
                "{topic} once: {seen:?}"
            );
        }
    }
}

/// The same over TCP, with the daemon's own dialer: two hubs on their own
/// ports, peered, a node on one seen from the other.
#[tokio::test]
async fn peers_reach_each_other_over_tcp() {
    let edger = ServedHub::new().with_peers(Peers::new("edger"));
    let edger_listener = hub::HubListener::bind("127.0.0.1:0").await.unwrap();
    let edger_addr = edger_listener.local_addr().to_string();
    tokio::spawn(edger_listener.serve(edger.clone()));

    let pc = ServedHub::new().with_peers(Peers::new("pc"));
    pc.peers()
        .add("edger", hub::tcp_dialer(edger_addr))
        .unwrap();
    let dir = tempdir::TempDir::new("peers-tcp").unwrap();
    let _yolo = node_on(&edger, "yolo", dir.path()).await;

    let on_pc = consumer(&pc).await;
    eventually(|| {
        let on_pc = on_pc.clone();
        async move { names(&on_pc.topology().await.unwrap()) == ["edger::yolo"] }
    })
    .await;
}

/// A peer never forwards a call onward: a name still qualified after one hop
/// is refused, not routed again.
#[tokio::test]
async fn a_call_never_goes_two_hops() {
    let (pc, edger) = two_hubs();
    let dir = tempdir::TempDir::new("peers-hops").unwrap();
    let _local = node_on(&pc, "local", dir.path()).await;
    let _yolo = node_on(&edger, "yolo", dir.path()).await;
    let on_pc = consumer(&pc).await;
    eventually(|| {
        let on_pc = on_pc.clone();
        async move { names(&on_pc.topology().await.unwrap()).len() == 2 }
    })
    .await;
    let err = on_pc
        .route(
            LinkSelector::node(NodeId("edger::pc::local".into())),
            read("x"),
        )
        .await
        .expect_err("two hops refused");
    assert!(
        matches!(&err, vox::VoxError::User(e) if matches!(**e, RouteError::Hub(hub_protocol::HubError::UnknownNode))),
        "{err:?}"
    );
}

/// A hub cannot peer with itself or with one peer twice.
#[test]
fn a_hub_refuses_itself_and_a_second_peer_of_one_name() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let pc = ServedHub::new().with_peers(Peers::new("pc"));
        let down: Dialer = Arc::new(|| Box::pin(async { Err("down".to_string()) }));
        assert!(pc.peers().add("pc", down.clone()).is_err(), "itself");
        pc.peers().add("edger", down.clone()).unwrap();
        assert!(pc.peers().add("edger", down).is_err(), "twice");
        assert_eq!(pc.peers().names(), ["edger"]);
    });
}

/// Many subscribers on one hub share one subscription per peer: every one of
/// them still gets the peer's events, and calls to the peer still go through.
#[tokio::test]
async fn many_subscribers_neither_starve_calls_nor_miss_events() {
    let (pc, edger) = two_hubs();
    let dir = tempdir::TempDir::new("peers-many").unwrap();
    std::fs::write(dir.path().join("motd.txt"), "still answering").unwrap();
    let _yolo = node_on(&edger, "yolo", dir.path()).await;
    let mut rxs = Vec::new();
    for _ in 0..4 {
        let c = consumer(&pc).await;
        for _ in 0..25 {
            rxs.push(subscribe(&c).await);
        }
    }
    let on_edger = consumer(&edger).await;
    // Until the first subscriber follows edger.
    let mut crossed = false;
    for n in 0..100 {
        publish(&on_edger, &format!("kudo/probe/{n}")).await;
        let got = tokio::time::timeout(Duration::from_millis(50), async {
            loop {
                if let Ok(Some(e)) = rxs[0].recv().await
                    && owned(e).topic.starts_with("kudo/probe/")
                {
                    return;
                }
            }
        })
        .await;
        if got.is_ok() {
            crossed = true;
            break;
        }
    }
    assert!(crossed);
    publish(&on_edger, "kudo/everyone").await;
    for rx in &mut rxs {
        until_all(rx, &["kudo/everyone"]).await;
    }
    let on_pc = consumer(&pc).await;
    let reply = tokio::time::timeout(
        Duration::from_secs(5),
        on_pc.route(
            LinkSelector::node(NodeId("edger::yolo".into())),
            read("motd.txt"),
        ),
    )
    .await
    .expect("not starved")
    .expect("routed");
    assert!(!reply.payload.is_empty());
}

/// When nobody here follows events any more, the peer lets go of this hub's
/// subscription too (a quiet peer included), so reloads never pile up calls.
#[tokio::test]
async fn a_peer_lets_go_once_nobody_follows() {
    let (pc, edger) = two_hubs();
    let c = consumer(&pc).await;
    let sub = {
        let c = c.clone();
        let (tx, rx) = vox::channel::<HubEvent>();
        (
            tokio::spawn(async move {
                let _ = c.subscribe_events("kudo/".into(), tx).await;
            }),
            rx,
        )
    };
    eventually(|| {
        let edger = edger.clone();
        async move { edger.event_subscriber_count().await == 1 }
    })
    .await;
    sub.0.abort();
    drop(sub.1);
    // pc learns its own subscriber is gone on its next send (vox cancels
    // nothing when a caller drops): one event here, then nobody follows.
    publish(&c, "kudo/after").await;
    drop(c);
    for _ in 0..200 {
        if edger.event_subscriber_count().await == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("the peer still holds this hub's subscription");
}
