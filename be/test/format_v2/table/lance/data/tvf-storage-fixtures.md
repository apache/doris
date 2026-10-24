The metadata-only storage-policy fixtures were generated with lance-c 4fafcd64525b274b113208390bbcca86bbbc093b and its pinned Lance SDK 68c12dfd7efe02f90ad2d7f3a239a7eb64884e57. Their fixed relative base references the existing all_types.lance fixture. No data files were copied and no manifest bytes were hand-written. Both snapshots reopen and scan row_id 1, 2, 3, 4.

tvf_storage_clone.lance uses a dataset-root base. tvf_storage_direct_base.lance uses a data-directory base. The tests vary user_files_secure_path to admit or reject the same valid references. Run BE tests from the repository root, as required by run-be-ut.sh.

To regenerate, copy ../generate_storage_policy_fixtures.rs into the pinned lance-c examples directory as generate_tvf_storage_clone.rs. From that SDK checkout run:

```sh
cargo run --release --locked --example generate_tvf_storage_clone -- /absolute/doris/repository /absolute/new-output-directory
```

The output directory must not exist. The generator uses official shallow-clone/commit APIs, reopens both manifests, and verifies four actual rows before reporting success. Copy the resulting two dataset directories into this directory.

The optional remote-storage test uses an explicitly selected isolated S3 fixture. Upload all_types.lance as source.lance and the metadata-only clone as clone.lance. Set DORIS_LANCE_TEST_REMOTE_SOURCE_URI, DORIS_LANCE_TEST_REMOTE_CLONE_URI, DORIS_LANCE_TEST_S3_ENDPOINT, DORIS_LANCE_TEST_S3_ACCESS_KEY and DORIS_LANCE_TEST_S3_SECRET_KEY only for that test run. The clone's relative base deliberately resolves to the local repository fixture, while the source control reads remote objects.
