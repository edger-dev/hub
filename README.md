# Hub

Lightweight coordination layer for multi-agent development environments.

Hub connects autonomous project agents running in sandboxed containers, routes messages between them, and provides a unified gateway for external access.

## Design Principles

- **Each project owns its own data** — the hub never stores project state, only routes and relays
- **Lightweight by default** — hub doesn't change how other tools work; it's plumbing, not a framework
- **Coordinators are the brains** — each project runs its own coordinator process that decides how to handle messages, manage sessions, and persist data
- **Hub is the gatekeeper** — project agents have limited permissions in their Incus containers; the hub mediates all external access

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                      External                        │
│              (users, webhooks, triggers)              │
└──────────────────────┬──────────────────────────────┘
                       │ REST + WebSocket (ingress)
┌──────────────────────▼──────────────────────────────┐
│                       Hub                            │
│                                                      │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────┐  │
│  │  Ingress    │  │   Message    │  │  Project   │  │
│  │  Gateway    │  │   Router     │  │  Registry  │  │
│  └─────────────┘  └──────────────┘  └────────────┘  │
└───┬──────────────────┬───────────────────┬──────────┘
    │ vox              │ vox               │ vox
    │ (long-lived)     │ (long-lived)      │ (long-lived)
┌───▼────┐        ┌────▼───┐         ┌────▼───┐
│Coord A │        │Coord B │         │Coord C │
│        │        │        │         │        │
│Kinora  │        │Kinora  │         │Kinora  │
│  Git   │        │  Git   │         │  Git   │
└────────┘        └────────┘         └────────┘
 Incus container   Incus container    Incus container
```

Each project coordinator maintains a persistent [vox](https://github.com/bearcove/vox) connection to the hub. Coordinators push state updates and receive routed messages over this bidirectional link. The hub aggregates project state and exposes it to external consumers.

## Core Concepts

### Project Coordinator

Each project runs a coordinator process built on top of Kinora. The coordinator is the project's agent — it manages local Claude Code sessions (interactive or automatic), handles incoming messages, and decides what to persist as kinos in its own git-managed kinograph.

### Hub

The hub is a long-running Rust process that serves two roles:

**Inward-facing**: maintains vox connections to all project coordinators and routes messages between them. The hub doesn't inspect message payloads — it just delivers them to the right recipient.

**Outward-facing**: acts as the gateway between sandboxed project agents and the outside world. External users query project state through the hub's REST and WebSocket APIs. Outside triggers (webhooks, scheduled events) land at the hub and are forwarded to the appropriate coordinator.

### Messages

All structured data exchanged between coordinators and the hub uses [facet](https://github.com/facet-rs/facet)-derived Rust types. Facet provides reflection and serialization — Rust types are the schema, with no separate IDL needed.

## Tech Stack

- **Language**: Rust
- **RPC / Transport**: [vox](https://github.com/bearcove/vox) — Rust-native RPC with bidirectional channels, virtual connections, and middleware support
- **Data types**: [facet](https://github.com/facet-rs/facet) — Rust reflection and serialization
- **Content management**: Kinora — all project content (tasks, docs, communications) stored as kinos in git
- **Sandboxing**: Incus containers — each project agent runs in its own isolated environment

## MVP Scope

### Milestone 1: Project Registry & Ingress

The hub accepts vox connections from project coordinators and exposes project state to external consumers.

**Hub side:**

- Accept incoming vox connections from coordinators
- Maintain a project registry (connected projects, metadata, current state)
- Expose REST API for querying project state
  - `GET /projects` — list registered projects
  - `GET /projects/{id}` — project details and current status
  - `GET /projects/{id}/sessions` — active Claude Code sessions
- Expose WebSocket endpoint for live state updates
  - Subscribe to all projects or a specific project
  - Receive real-time status changes, session events

**Coordinator side:**

- Connect to hub on startup via vox
- Send `RegisterProject` with project metadata
- Push `StatusUpdate` messages when state changes (sessions started/stopped, tasks completed)

**Vox service definitions:**

```rust
#[vox::service]
pub trait HubService {
    /// Register a project coordinator with the hub
    async fn register(&self, reg: ProjectRegistration) -> Result<(), HubError>;

    /// Stream status updates from coordinator to hub
    async fn status_stream(&self, updates: Tx<StatusUpdate>);
}

#[vox::service]
pub trait CoordinatorService {
    /// Health check from hub to coordinator
    async fn ping(&self) -> PongResponse;
}
```

**Key data types:**

```rust
#[derive(Facet)]
pub struct ProjectRegistration {
    pub project_id: String,
    pub name: String,
    pub description: String,
    pub capabilities: Vec<String>,
}

#[derive(Facet)]
pub struct StatusUpdate {
    pub project_id: String,
    pub state: ProjectState,
    pub active_sessions: Vec<SessionInfo>,
    pub timestamp: u64,
}

#[derive(Facet)]
pub enum ProjectState {
    Starting,
    Idle,
    Active,
    Error { message: String },
}

#[derive(Facet)]
pub struct SessionInfo {
    pub session_id: String,
    pub session_type: SessionType,
    pub started_at: u64,
}

#[derive(Facet)]
pub enum SessionType {
    Interactive,
    Automatic { trigger: String },
}
```

### Milestone 2: Message Routing

Coordinators can send messages to other coordinators through the hub.

**Additions to hub:**

- Route messages based on recipient project ID
- Return errors for undeliverable messages (target offline)
- No message persistence — the hub is a router, not a queue

**Additions to vox services:**

```rust
// Added to HubService
async fn send_message(&self, msg: ProjectMessage) -> Result<(), HubError>;

// Added to CoordinatorService
async fn receive_message(&self, msg: ProjectMessage);
```

**Message type:**

```rust
#[derive(Facet)]
pub struct ProjectMessage {
    pub from: String,
    pub to: String,
    pub message_type: String,
    pub payload: Vec<u8>,
}
```

Each coordinator decides how to handle received messages — persist as kinos, trigger a Claude Code session, ignore, or anything else. The hub doesn't care.

### Future: Network Monitoring

Hub tracks connection health, message throughput, and error rates across all connected coordinators. Leverages vox's built-in OpenTelemetry middleware. Details TBD.

### Future: Token Injection & Credential Management

Hub acts as a credential broker for sandboxed agents. Coordinators request tokens by capability name; hub resolves and injects them without exposing raw secrets to the container. Details TBD.


## Connection authentication

Set `HUB_AUTH_TOKEN` on the hub, nodes, peer hubs, and consumers to require that
exact token before any registration, topology read, subscription, publication,
or routed call. Missing or incorrect credentials are refused; a correct token
authenticates only that connection. An empty token is a startup error, and an
unset token preserves existing behavior.

The token is read from the environment and is never logged or accepted as a
command-line argument. It authenticates but does not encrypt traffic, so remote
connections must remain on a private or separately encrypted network.

## WebSocket listener

Set `HUB_WS_ADDR` to serve plain WebSocket beside the existing TCP listeners:

```sh
HUB_WS_ADDR=127.0.0.1:15401
```

This uses the same routing and connection authentication. For public access,
let Cloudflare terminate HTTPS/WSS and configure the published application to
forward to `http://127.0.0.1:15401`; the hub listener itself stays local. Keep
`HUB_AUTH_TOKEN` configured. Native TLS termination in the hub is not part of
this listener.

`GET /healthz` returns `200 OK` with no topology, session, or credential data;
use it for Caddy and uptime probes. Other ordinary non-WebSocket HTTP requests
return `426 Upgrade Required`, rather than a misleading `502 Bad Gateway`. The
probe is shallow: use an authenticated MCP `session_list` to verify peer
discovery and a routed call to verify end-to-end delivery.

### Node WebSocket dialing

Nodes accept `HUB_ADDR=ws://host:port` or `HUB_ADDR=wss://host:port` as well as
bare/TCP addresses. Registration, callbacks, token authentication, and reconnect
backoff are shared with TCP. Use WSS for direct public VPS endpoints; plain WS
is for local/private links. Cloudflare Access service-token headers are not yet
part of the node handshake.

### Peer WebSocket dialing

`HUB_PEERS` accepts WebSocket addresses alongside bare/TCP addresses:

```sh
HUB_NAME=edger
HUB_PEERS=hub=wss://hub.example.com
HUB_AUTH_TOKEN=<shared-hub-token>
```

The peer connection carries authenticated `ConsumerApi` lanes in both
directions, with keepalive, reconnect backoff, topology, events, and one-hop
routing shared with TCP. Edger registers its `HUB_NAME` through `PeerIngress`;
the receiving hub opens a reverse consumer lane over the same connection. The
public hub therefore sees `edger::<node>` without a return address or its own
`HUB_PEERS` entry, while edger sees `hub::<node>`. Registration and both
consumer directions authenticate before use. Multi-hop routing remains refused.

A reconnect supersedes the earlier reverse registration without letting the old
connection's cleanup remove it. Existing mutually configured peers and older
peers without `PeerIngress` retain their forward-only behavior.

For a Cloudflare Access-protected endpoint, set both service-token values on the
dialing hub. They are sent only as WebSocket handshake headers and are never
logged:

```sh
CF_ACCESS_CLIENT_ID=<service-token-id>
CF_ACCESS_CLIENT_SECRET=<service-token-secret>
```

Both must be nonempty and set together. A direct WSS endpoint without
Cloudflare Access needs neither. Prefer WSS for public endpoints; plain
`ws://` and bare/TCP addresses remain available for local or private networks.

## Live session messaging

Agent shims authenticate through `SessionIngress`, register one live session,
and serve `SessionService` on the same connection. Consumers use `sessions` for
local plus one-hop peer sessions, or `local_sessions` for the peer-safe view.
A peer's session and node names are both qualified as `<peer>::<id>`.

Several MCP clients belonging to one agent may register the same logical session
id. Listing shows one entry; the newest registration is current, and disconnecting
it falls back to the newest surviving registration. Receivers must use a shared
mailbox when they share an id, so delivery through any registration remains
retrievable by the agent.

`send_session` derives its sender from the connection's authenticated session
registration and acknowledges only after the target accepts the message into
its local mailbox. Hubs forward to peers with `forward_session`, which accepts
only the origin prefix belonging to the authenticated peer. Unknown or offline
sessions fail visibly. Messages are live-only: the hub retains no offline queue
or history, and multi-hop targets are refused.
