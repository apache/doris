# PK Bloom filter / S3 multipart failure reproducer

This branch is based on Apache Doris master at `4d7a207363b`. The original
failure analysis describes older behavior that master no longer has. This
reproducer uses the real PK Bloom filter writer, IndexedColumnWriter, PageIO,
S3FileWriter and upload buffers, with the existing in-memory object-storage mock.
No S3 credentials or running Doris cluster are needed.

## Run

From the repository root, with the normal Doris BE build dependencies and a
working JDK 17:

```sh
bash run-be-ut.sh --run --filter='S3Multipart/PrimaryKeyBloomFilterS3FailureTest.*' -j 4
```

The two parameterized cases are:

- `MasterPropagatesError`: inject a CreateMultipartUpload failure and verify that
  current master propagates it, without publishing the invalid BF root or
  uploading any part.
- `LegacyIgnoredError`: additionally use two test callbacks to emulate the
  affected version: ignore the actual error returned by `bf_writer.add()`, and
  calculate available S3 buffer space from its physical size. Then recover the
  mock service and write two real PageIO pages.

The callbacks are `PrimaryKeyBloomFilterIndexWriterImpl::finish_after_add` and
`S3FileWriter::appendv_data_size`. They do not change normal behavior without
registered callbacks and compile out when neither BE_TEST nor
ENABLE_INJECTION_POINT is enabled. No counters or page pointers are manually
set to manufacture the result.

## Assertions

1. The 5 MiB pending buffer starts at 5,203,052 bytes.
2. A BF larger than the current 1 MiB IndexedColumn page limit forces `add()` to
   flush. The actual CreateMultipartUpload failure occurs after 39,828 BF bytes
   have been copied, before the logical byte count is updated.
3. The IndexedColumnWriter has one value, zero written pages and a `(0,0)` page
   pointer. In the legacy case, finish succeeds and publishes those invalid BF
   index fields.
4. The next append consumes zero bytes while retrying multipart creation, which
   succeeds. Part 1 contains the full 5 MiB, including the failed BF prefix.
5. The two subsequent pages have physical offsets exactly 39,828 bytes beyond
   their PageIO pointers. The completed mock object is exactly 39,828 bytes
   larger than `bytes_appended()`.

The suffix is deliberately small; this test does not construct the original
274,749,789-byte object, a complete SegmentWriter segment, or rowset metadata.
It isolates the page/upload corruption chain and checks the byte count used by
segment and rowset size accounting.

## Validation on this machine

- clang-format 16 check: passed for the three changed C++ files.
- BE build-hygiene checks and `git diff --check`: passed.
- The command above was attempted but stopped during environment setup, before
  compilation. The configured JDK 17 fails with `Failed setting boot class path`.
  Further compilation and test execution were skipped at the user's request.
  The C++ tests have therefore **not been compiled or executed** here.
