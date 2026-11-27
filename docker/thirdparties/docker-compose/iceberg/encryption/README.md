# Iceberg encryption reference fixtures

The isolated service uses Iceberg 1.12.0 and Parquet Java from the checksum-pinned
reference jars prepared by `prepare-jars.sh`. `start.sh` compiles the fixture
classes into the container's temporary directory. Production Doris plugins do
not include these classes.

The existing `/reset` and `/verify` endpoints exercise HMS metadata encryption
with the test KMS. The native delete endpoints use the same HMS, S3 storage and
fixture service:

- `POST /native-deletes/reset` replaces only the dedicated
  `iceberg_encryption_delete_db` tables and their fixture object prefixes.
- `GET /native-deletes/verify` reads the physical files with the Java SDK and
  checks delete application, descriptor keys and actual encrypted footer magic.

`NativeDeleteFixture` creates all eight combinations of plaintext/encrypted data,
position-delete and equality-delete files. Mode bits 1, 2 and 4 identify those
three file kinds. Their public synthetic AES keys and AAD prefixes are distinct.
Each table starts with values 1 through 8; position deletes remove offsets 1 and
6, and the equality delete removes value 5. The remaining values are
`1, 3, 4, 6, 8`.

V2 fixtures keep plaintext metadata with public synthetic StandardKeyMetadata V1
file descriptors. Their V3 counterparts retain the legacy Parquet position deletes
and register a V3 snapshot with the configured test KMS, then rewrite data and
delete manifests through the SDK. The current V3 manifest lists and manifests are
encrypted, and verification checks their table key chain before reading the files.
This fixture arrangement is not a production table-encryption migration workflow.

`test_iceberg_encryption_delete_reader` runs with the deployment read switch off.
It checks ordinary V2 results and rejects encrypted V3 metadata or undeclared
per-file keys before publishing native descriptors. Plain V2 position-delete
metadata remains readable independently of encrypted data/equality files.

`test_iceberg_encryption_native_read_integration` additionally requires
`enableIcebergEncryptionNativeRead=true`, the plugin's deployment-level
`enable_encryption_read=true`, and authenticated private RPC on capable FE/BE
nodes. It runs both HMS and REST catalogs over the same physical fixtures and
compares SQL against SDK/Spark history and the eight delete-key modes,
with cache, COUNT pushdown, batch planning and pruning toggles. Encrypted JNI
metadata tables remain explicitly unsupported; native position-deletes use the
protected reader path.

For warm-cache authorization checks, `/kms/runtime` proxies requests only to the
disposable Moto service configured at startup. `POST /kms/access/deny` revokes
mock decrypt permission without changing the Doris catalog or endpoint;
`POST /kms/access/allow` restores it. `GET /kms/access` reports the state and the
number of decrypt requests. Moto uses signing scopes for routing but does not
validate AWS signatures: this is test fault injection, not AWS IAM acceptance or
a production KMS gateway. Private RPC identities are generated separately at test
time by `generate-tls-fixture.sh`.

`POST /benchmark/reset` creates a matched pair of plaintext/encrypted V3 tables,
each with 1,048,576 integer rows in four files, Zstandard compression, 64 KiB row
groups and Bloom filters. `GET /benchmark/verify` checks their row count, sum,
physical file count and independent encrypted file keys with the SDK. These
larger samples exercise row-group boundaries and provide identical writer
settings for cost measurements; timing comparisons still require matching
release builds, explicit cache conditions and a declared regression threshold.

V3 planning currently remains synchronous, including with the external batch
setting enabled. The integration matrix checks both setting values without
claiming encrypted V3 lazy execution; protocol and transport tests cover lazy
caller rejection and binary payload round trips separately.

The read-only `/rest` fixture exposes its dedicated REST metadata namespace and
the existing metadata/delete namespaces. Native REST tests therefore consume
the exact same encrypted data, manifests, position deletes and equality deletes
as HMS, including independent file keys. They also exercise physical COUNT,
encrypted-write refusal, enabled SQL-cache settings and warm KMS denial/recovery.
The fixture does not implement REST mutations or a production authentication service.

Load-table responses include content-based ETags and return HTTP 304 for matching
`If-None-Match` requests. `GET /rest/access` reports total and encrypted
conditional-response counts. The native suite requires encrypted HTTP 304
responses during KMS denial, followed by successful reads after access is restored.
