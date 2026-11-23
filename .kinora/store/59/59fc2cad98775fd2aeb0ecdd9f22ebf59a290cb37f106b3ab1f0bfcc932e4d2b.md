# Spec — hub connections authenticate before use when a token is configured

**Contract.** When a hub has `HUB_AUTH_TOKEN` set, every `NodeIngress` and
`ConsumerApi` connection must first authenticate with that exact token. Missing
or incorrect credentials are refused before registration, topology reads,
subscriptions, publication, or routing. Successful authentication is scoped to
that connection. Nodes, peer hubs, and consumers carry the token explicitly and
never log it. With no token configured, existing behavior is unchanged.

**Transport boundary.** The token authenticates a home-lab connection; it does
not encrypt it. Remote deployment must stay on a private or separately encrypted
network. The token is read from the environment, never a command-line argument.

**Why.** The remote inbox needs authenticated delivery without a new public
service. Guarding the whole connection, including reads, avoids leaving topology
or event data unauthenticated while only writes are protected.

**Rejected.** Connector-only credentials for this contract, authentication on
routed writes alone, mandatory tokens for existing local deployments, and
claiming the token makes a public plaintext listener safe.

**Escape hatch.** A future credential/TLS mechanism can replace the shared token
without changing the requirement that every operation is authorized.
