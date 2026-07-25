//! The hub daemon: bind, accept, serve — until killed.
//!
//! Address from the first argument or `HUB_ADDR`, defaulting to 127.0.0.1:15400.

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let addr = std::env::args()
        .nth(1)
        .or_else(|| std::env::var("HUB_ADDR").ok())
        .unwrap_or_else(|| "127.0.0.1:15400".to_string());

    let listener = hub::HubListener::bind(addr.as_str()).await?;
    println!("hub listening on {}", listener.local_addr());
    listener.serve(hub::ServedHub::new()).await
}
