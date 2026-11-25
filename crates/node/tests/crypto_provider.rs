#[test]
fn websocket_nodes_have_a_crypto_provider() {
    node::install_crypto_provider();
    assert!(rustls::crypto::CryptoProvider::get_default().is_some());
}
