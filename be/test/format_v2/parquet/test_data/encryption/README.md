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
  be/test/format_v2/parquet/test_data/encryption/iceberg_java.parquet
```

The native-reader regression compares every value, including null/empty nested
shapes, against the parquet-cpp reference reader. It also covers authenticated
zero padding emitted by Parquet Java's metadata serializer. Additional C++ writer
samples exercise Page V1/V2, dictionary/plain encoding, 128/192/256-bit keys,
multiple data pages and row groups, and invalid key/AAD/tampering/truncation.

Run through `run-be-ut.sh --run --filter='ParquetEncryptionTest.*'` with a Doris
third-party build using `PARQUET_REQUIRE_ENCRYPTION=ON`. The internal PME headers
installed by that build are required for decryption. Older prebuilt bundles run
the explicit dependency-unavailable contract test instead. Encrypted SQL admission,
shared decrypted caches, encrypted indexes and encrypted deletes remain disabled
at this stage; the physical reader's optional parameters support controlled tests
and subsequent scan integration.
