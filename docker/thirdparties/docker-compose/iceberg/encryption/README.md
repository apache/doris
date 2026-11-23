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

Fixtures exist as V2 tables and V2 snapshots upgraded to V3, which retains their
older Parquet position deletes. Table metadata stays plaintext and carries
synthetic StandardKeyMetadata V1 descriptors directly. This isolates native-file
planning and the last dispatch guard from the separate KMS/metadata-encryption
fixture. It is a controlled test arrangement using public keys, not a production
catalog setup.

`test_iceberg_encryption_delete_reader` compares plaintext SQL delete results and
position-delete metadata output, and asserts that encrypted descriptors still
stop at the SQL dispatch guard. Metadata-only COUNT rejection is covered separately
by FE and BE unit tests. The SQL suite covers eager/lazy planning,
COUNT pushdown on/off, all file combinations and both format versions. The
physical reader UTs exercise successful decryption, key/AAD failures, repeated
splits, converted equality keys and unsupported encrypted ORC/Puffin descriptors.
Encrypted SQL admission remains closed until transport and cache integration are
complete.
