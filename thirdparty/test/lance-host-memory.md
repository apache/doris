# Lance host memory regression coverage

The patched Lance-C library uses allocation-owner headers so Rust heap bytes
remain charged to their original host account until the last release. Host file
buffers and expanded nullable fixed-size-list buffers use fallible admission.
Ordinary Rust containers record allocations and report memory pressure at
fallible scan/decoder boundaries; their allocator must never return null merely
because a query exceeded its limit.

## Tests

| Layer | Regression coverage |
| --- | --- |
| BE `LanceMemoryTest` | Query/WG/process admission, query/process rollback and reserved/live conversion, failed-reservation retry, sticky ordinary-allocation overage, concurrent accounts, owner lifetime, expired WG, refresh and zero/overflow cases. |
| Native `lance-host-runtime-test.cpp` | C++ memory callback ABI, live/reserved balance, retained context release and query zero balance after real Arrow/async close on C++ host threads. |
| SDK `utils::memory::tests` | Out-of-order scopes, unwind/TLS-exit restoration, callback reentry, alignment, cross-thread free, original-owner realloc, failed realloc preserving contents and balance, ordinary allocation above limit, denied large buffer, empty and overflowing requests. |
| SDK wide-value decoding | Nullable fixed-size lists with 128/256/1024-byte elements preserve contents and the admitted allocation owner, use the required alignment without a second payload copy, and reject an insufficient aligned-layout budget. |
| SDK cache tests | Loader/backend ownership across pending polls, cross-query hits, weak/namespace handles, disabled caches, unsized payloads and retained lazy initialization. |
| Lance-C `host_memory_test` | Isolated processes exercise real host-file scans, filters/take, delayed Arrow release after reader/executor close, concurrent and nested queries, query/WG/process errors, denied reads and fresh-query recovery, shared metadata/index caches, and repeated early close. |

Run the BE tests through the normal BE unit-test runner with
`--gtest_filter=LanceMemoryTest.*`. For patched SDK sources, run:

```sh
cargo test --manifest-path "$LANCE_C_SOURCE/Cargo.toml" --test host_memory_test
cargo test --manifest-path "$LANCE_RUST_SOURCE/Cargo.toml" -p lance-core utils::memory::tests
cargo test --manifest-path "$LANCE_RUST_SOURCE/Cargo.toml" -p lance-core cache::tests
cargo test --manifest-path "$LANCE_RUST_SOURCE/Cargo.toml" -p lance-encoding test_fullzip_wide_values_keep_the_fallibly_admitted_buffer
```

The integration test generates synthetic fixtures and uses local files behind
host storage callbacks; it needs no cloud credentials or running Doris cluster.
Each scenario is a child process because host runtime and memory services are
registered once per process. Assertions cover exact rows, owner balances and
retained-context release, as well as process survival after a rejected query.

## Accounting boundaries

Allocation headers charge the raw Rust allocation layout, including header and
alignment padding. Cache eviction weights remain eviction estimates and are not
charged a second time. FileReader/File Cache allocations retain their existing
Doris accounting. Native-library malloc, mappings and allocator-retained RSS are
not measured by these Rust heap headers; process-pressure checks still apply.
WG interval growth is a conservative estimate until the next WG refresh; a
failed reservation never subtracts growth reported after a concurrent refresh.
The tests do not establish a zero-overshoot cap for every infallible container,
production throughput, or a complete distributed Doris regression result.
