# Encrypted Parquet interoperability fixture

`iceberg_java.parquet` was produced with Iceberg 1.12.0's `GenericParquetWriter`
(Parquet Java), using `GenerateEncryptedParquet.java`. It contains eight rows of
integer, nullable string, nullable list and nullable struct values. The file uses
AES_GCM_V1, an encrypted footer, footer-key column encryption and Snappy.

The key `synthetic-key123` and AAD prefix `0123456789abcdef` are public test
values. The generator replaces only those SDK-generated fields; the format's
per-file identifier and encryption nonces remain random. Regeneration preserves
rows and parameters, not identical ciphertext bytes. No KMS or catalog is used.

Set `ICEBERG_REFERENCE_CLASSPATH` to the pinned Iceberg 1.12.0 Spark runtime and
its dependencies. The existing Iceberg encryption reference container supplies
that runtime together with Spark's Hadoop and SLF4J jars. From the repository root:

```sh
mkdir -p build/encrypted-parquet-fixture
javac -cp "$ICEBERG_REFERENCE_CLASSPATH" -d build/encrypted-parquet-fixture \
  be/test/format_v2/parquet/test_data/encryption/GenerateEncryptedParquet.java
java -cp "build/encrypted-parquet-fixture:$ICEBERG_REFERENCE_CLASSPATH" \
  GenerateEncryptedParquet \
  be/test/format_v2/parquet/test_data/encryption/iceberg_java.parquet \
  be/test/format_v2/parquet/test_data/encryption/iceberg_java_plain.parquet
cp be/test/format_v2/parquet/test_data/encryption/iceberg_java*.parquet \
  regression-test/data/external_table_p0/iceberg/encryption/
```

The native-reader regression compares every value, including null/empty nested
shapes, against the parquet-cpp reference reader. It also covers authenticated
zero padding emitted by Parquet Java's metadata serializer. Additional C++ writer
samples exercise Page V1/V2, dictionary/plain encoding, 128/192/256-bit keys,
multiple data pages and row groups, and invalid key/AAD/tampering/truncation.
The optional second generator output is a plaintext twin with the same schema,
rows and compression. The SQL regression has byte-identical copies because its
runner distributes `regression-test/data` independently from BE unit fixtures.

| Coverage | Expected behavior |
| --- | --- |
| Native scalar/nested reads, both page versions, dictionary/plain encoding, three key sizes | Exact reference values, including null and empty nested shapes |
| Missing/invalid/wrong keys and AAD; stored/external AAD disagreement | Explicit rejection without sensitive values in errors |
| Unsupported cipher, plaintext-footer encryption, separate column keys, ordinal limits | Explicit classification before unsupported decoding |
| Corrupted footer/header/dictionary/data, oversized metadata and module lengths | Reject before decoding or an unbounded allocation/read |
| Shared metadata cache and independent column decryptors | Another key cannot authorize a hit; failure cannot contaminate another reader |
| SQL schema inference, mixed plaintext/encrypted files, repeated scans and COUNT pushdown | Missing-key error for every encrypted file; plaintext reads remain correct |
| SQL truncated file and subsequent plaintext reads | Corruption error, followed by unchanged plaintext results |

The SQL coverage runs against the standard Iceberg MinIO environment without a
KMS or encryption-specific test switch:

```sh
bash run-regression-test.sh --run -d external_table_p0/iceberg \
  -s test_iceberg_encryption_native_reader
```

It creates a unique temporary bucket, uploads the fixtures, exercises page-cache
and COUNT-pushdown settings, restores session variables, and removes its objects
and bucket. An encrypted first file exercises schema inference; a plaintext first
file followed by an encrypted file exercises actual scan/COUNT failure. These SQL
checks do not supply a decryption key or enable encrypted Iceberg SQL admission.

Run through `run-be-ut.sh --run --filter='ParquetEncryptionTest.*'` with a Doris
third-party build using `PARQUET_REQUIRE_ENCRYPTION=ON`. The internal PME headers
installed by that build are required for decryption. Older prebuilt bundles run
the explicit dependency-unavailable contract test instead. Encrypted SQL admission,
shared decrypted caches, encrypted indexes and encrypted deletes remain disabled
at this stage; the physical reader's optional parameters support controlled tests
and subsequent scan integration.
