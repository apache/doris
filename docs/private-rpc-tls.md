<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# OSS private RPC mutual TLS

The OSS providers support mutual TLS for FE gRPC to BE BRPC and for blocking
Thrift RPC between nodes. Both ends require a certificate issued by the configured
CA. Configuring TLS never falls back to plaintext after an authentication or
connection failure. `enable_tls=false` preserves the existing plaintext behavior.

MaxCompute writers also call the local FE from the BE's embedded JVM to allocate
block IDs, including after block rotation and FE master redirection. These
callbacks use the BE's internal Thrift TLS policy and node identity. MaxCompute
catalog properties cannot override that local policy.

External RPC functions retain plaintext for both FE validation and BE execution.
Broker heartbeats and filesystem clients on FE and BE also retain their existing
plaintext policy. Remote Doris catalogs also retain their existing plaintext
Thrift metadata/transaction calls, gRPC fragment execution/result fetch/cancellation,
native BRPC writes (including HTTP batches and streaming writes) and partition
callbacks. File-key descriptors are rejected before serialization to those remote
plaintext channels, even when local private TLS is enabled. Remote endpoint
pools and cached channels are isolated from internal connections, including when
the addresses coincide. These external endpoints do not inherit the local
cluster's private RPC TLS configuration; this provider does not configure TLS
for an independent remote cluster.

With the OSS private BRPC or Thrift transport enabled, remote Doris reads must
use `use_arrow_flight=true` (the catalog's default). Arrow Flight retrieves
external results into local workers, so query exchanges and execution-status
reports stay inside the local TLS cluster. Direct OLAP mode
(`use_arrow_flight=false`) assigns fragments to remote BEs, which independently
initiate exchanges and Thrift reports; an outbound FE channel policy cannot
supply those BEs with a local TLS identity. Such execution graphs are rejected
before tasks are created, including remote-only graphs with reverse status
callbacks. This does not change metadata access or remote tablet writes that
execute in local workers. Custom providers retain their existing policy unless
they explicitly opt out of remote workers.

The default P0/nonConcurrent `rpc_p0/test_remote_doris_private_rpc_tls` regression
provisions two independent loopback clusters: private TLS enabled locally and
plaintext RPC remotely. It uses Linux FE/BE packages and the pipeline's MySQL CLI
on the regression host.
Unsupported operating systems skip this Linux fixture before starting helper
tests or engines; the standard Linux NonConcurrent run keeps it enabled by default.
`privateRpcTlsPackageHome` defaults to `DORIS_HOME`; both a source checkout's
`output/` and an installed package with `fe/` and `be/` are supported. Certificates
and unused ports are generated automatically, without a test-specific enable flag
or remote endpoint settings. The suite verifies mutual authentication before
testing broadcast/shuffle joins in both directions, local/remote reads, and
rejection of direct remote execution. The fixture stops its own processes and
removes test keys on completion, failure, or runner pipe closure; diagnostic logs
remain under `regression-test/log/`.

## Configuration and deployment

Configure each participating FE and BE with its own PEM certificate and private
key, and the cluster CA certificate:

```properties
enable_tls=true
tls_verify_mode=verify_fail_if_no_peer_cert
tls_certificate_path=/path/to/node-certificate.pem
tls_private_key_path=/path/to/node-private-key.pem
tls_ca_certificate_path=/path/to/cluster-ca.pem
tls_cert_refresh_interval_seconds=0
```

The initial provider supports static identities. Restart nodes after replacing
identity or trust files. A nonzero refresh interval, optional peer verification,
or a nonempty `tls_peer_cert_required_san_dns` is rejected. BE private keys must
be unencrypted PEM; a nonempty `tls_private_key_password` is rejected on BE.
FE supports its existing private-key password setting.

Public protocol TLS remains provider-specific. Explicitly exclude unsupported
protocols for a private-RPC-only deployment:

| Process | `tls_excluded_protocols` | Additional requirement |
| --- | --- | --- |
| FE | `http,mysql,arrowflight,bdbje` | Blocking Thrift: `THREAD_POOL` or `SIMPLE` |
| BE | `http,arrowflight` | Blocking backend and heartbeat Thrift listeners |

The FE Thrift handshake uses a positive `thrift_rpc_connect_timeout_ms`. BE uses a
positive `thrift_connect_timeout_seconds` representable in milliseconds. These
bound TLS handshake IO independently of unlimited RPC read timeouts. A smaller
caller timeout is preserved. Accepted sockets authenticate on request workers,
and authenticated sockets regain their normal RPC read and write timeouts.
Client reconnection creates another authenticated TLS connection.

Coordinate activation across all participating nodes. A plaintext peer cannot
communicate with a listener configured to require TLS. This provider does not
establish a rolling transition between plaintext and TLS, or cover every cloud
control-plane transport. Encrypted Iceberg SQL admission additionally requires
its own backend capability and end-to-end acceptance checks.

## Trust and key ownership

Use a dedicated cluster CA. FE gRPC and Thrift clients verify the original target
hostname or IP against the server identity, including after DNS resolution.
The pinned native BRPC client authenticates the CA chain; it does not implement a
per-node hostname or role allowlist. Server listeners authorize members of the
configured CA trust domain. A certificate issued by that CA is therefore a
cluster RPC credential, and the CA must not be shared with unrelated services.

Each node should receive only its own private key and the public CA certificate.
Keep the CA private key outside Doris nodes. Restrict private-key files to their
service account, and provision certificates for both client and server usage
with SANs matching the addresses peers actually use.

TLS protects bytes in transit. Plaintext file keys still exist in the processes
that use them; it does not protect against a privileged host administrator or
process memory capture. Protect crash artifacts and avoid passing unrelated
credentials to test or service processes. File-key diagnostic redaction and
statement-scoped key ownership are separate requirements.

## Test fixtures

`docker/thirdparties/docker-compose/iceberg/encryption/generate-tls-fixture.sh`
generates ephemeral test CA and peer identities with OpenSSL or a JDK keytool fallback. Certificates expire
after two days, private files are created under `umask 077`, and tests use temporary
directories. The fixture includes an untrusted CA and a certificate with a wrong
hostname for rejection tests. A DNS-only peer identity covers resolved native
client-cache connections and reconnects. These credentials are generated during the test;
private key material is not distributed with the fixture. The generator is for
tests, including deliberately retaining its test CA key, and is not a production
certificate provisioning workflow.
