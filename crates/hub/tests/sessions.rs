#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use hub::{Dialer, Peers, ServedHub, serve_connection};
use hub_protocol::service::{ConsumerApiClient, SessionIngressClient, SessionServiceDispatcher};
use hub_protocol::{HubError, SessionInfo, SessionMessage, SessionService};

#[derive(Clone, Default)]
struct Mailbox {
    messages: Arc<Mutex<Vec<SessionMessage>>>,
}

impl SessionService for Mailbox {
    async fn deliver(&self, message: SessionMessage) -> Result<(), HubError> {
        self.messages.lock().unwrap().push(message);
        Ok(())
    }
}

fn attach(hub: &ServedHub) -> vox::MemoryLink {
    let (client, hub_link) = vox::memory_link_pair(64);
    let hub = hub.clone();
    tokio::spawn(async move {
        let _ = serve_connection(hub, hub_link).await;
    });
    client
}

fn peer_dialer(remote: &ServedHub, local: &ServedHub) -> Dialer {
    let (remote, local) = (remote.clone(), local.clone());
    Arc::new(move || {
        let link = attach(&remote);
        let acceptor = hub::serve::peer_consumer_acceptor(&local);
        Box::pin(async move {
            vox::initiator_on(link)
                .on_lane(acceptor)
                .establish_connection()
                .await
                .map_err(|error| format!("{error:?}"))
        })
    })
}

fn info(id: &str, node: &str) -> SessionInfo {
    SessionInfo {
        id: id.into(),
        node: node.into(),
        harness: "test".into(),
        workspace: format!("/work/{id}"),
        context: None,
    }
}

async fn session(
    hub: &ServedHub,
    info: SessionInfo,
) -> (vox::ConnectionHandle, ConsumerApiClient, Mailbox) {
    let mailbox = Mailbox::default();
    let connection = vox::initiator_on(attach(hub))
        .on_lane(SessionServiceDispatcher::new(mailbox.clone()))
        .establish_connection()
        .await
        .unwrap();
    let ingress: SessionIngressClient = connection.open_lane().await.unwrap();
    let consumer: ConsumerApiClient = connection.open_lane().await.unwrap();
    if let Some(token) = hub.auth_token() {
        ingress.authenticate(token.to_string()).await.unwrap();
        consumer.authenticate(token.to_string()).await.unwrap();
    }
    ingress.register(info).await.unwrap();
    (connection, consumer, mailbox)
}

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

#[tokio::test(flavor = "multi_thread")]
async fn sessions_list_message_and_reply_across_one_outbound_peer() {
    let public = ServedHub::new()
        .with_auth_token("secret")
        .with_peers(Peers::new("hub").with_auth_token("secret"));
    let edger = ServedHub::new()
        .with_auth_token("secret")
        .with_peers(Peers::new("edger").with_auth_token("secret"));
    edger
        .peers()
        .add("hub", peer_dialer(&public, &edger))
        .unwrap();

    let (_a_connection, on_a, a_mailbox) = session(&edger, info("a", "yolo")).await;
    let (_b_connection, on_b, b_mailbox) = session(&public, info("b", "public-node")).await;
    eventually(|| {
        let (on_a, on_b) = (on_a.clone(), on_b.clone());
        async move {
            let a = on_a.sessions().await.unwrap();
            let b = on_b.sessions().await.unwrap();
            a.iter().any(|s| s.id == "hub::b") && b.iter().any(|s| s.id == "edger::a")
        }
    })
    .await;

    on_a.send_session("hub::b".into(), "m1".into(), "hello from a".into())
        .await
        .unwrap();
    let mailbox = b_mailbox.clone();
    eventually(move || {
        let mailbox = mailbox.clone();
        async move { !mailbox.messages.lock().unwrap().is_empty() }
    })
    .await;
    let message = b_mailbox.messages.lock().unwrap()[0].clone();
    assert_eq!(message.body, "hello from a");
    assert_eq!(message.from.id, "edger::a");
    assert_eq!(message.from.node, "edger::yolo");

    on_b.send_session("edger::a".into(), "m2".into(), "reply from b".into())
        .await
        .unwrap();
    let mailbox = a_mailbox.clone();
    eventually(move || {
        let mailbox = mailbox.clone();
        async move { !mailbox.messages.lock().unwrap().is_empty() }
    })
    .await;
    let reply = a_mailbox.messages.lock().unwrap()[0].clone();
    assert_eq!(reply.body, "reply from b");
    assert_eq!(reply.from.id, "hub::b");
    assert_eq!(reply.from.node, "hub::public-node");

    assert!(
        on_a.send_session("hub::missing".into(), "m3".into(), "nobody".into())
            .await
            .is_err()
    );
}
