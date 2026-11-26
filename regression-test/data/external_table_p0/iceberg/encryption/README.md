# Native encrypted Parquet SQL fixtures

These files are byte-identical copies of the BE interoperability fixtures in
`be/test/format_v2/parquet/test_data/encryption/`. See that directory's README and
`GenerateEncryptedParquet.java` for the pinned Iceberg generator and public test
key/AAD. `iceberg_java_plain.parquet` contains the same eight rows as the encrypted
file, including null strings, null/empty lists and nullable structs.

`test_iceberg_encryption_native_reader` uploads them to an isolated temporary
MinIO bucket. It verifies plaintext values, encrypted-footer rejection without a
key, mixed-file scan/COUNT rejection, corruption rejection and reader recovery.
The suite derives its truncated input at runtime; it does not require a KMS or
enable encrypted SQL admission.
