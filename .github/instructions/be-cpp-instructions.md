---
applyTo: "be/src/**/*"
---

# Apache Doris BE (C++) Instructions

This document helps code assistants understand the backend C++ code structure, conventions, and review points.

## Directory Highlights

- `be/src/vec/`: Vectorized execution engine. Contains `columns/`, `data_types/`, `functions/`, `aggregate_functions/`, `exec/`, etc.
- `be/src/pipeline/`: Pipeline operators and scheduling; execution lifecycle and data movement.
- `be/src/olap/`: Storage engine (Tablet/Rowset/Segment/Compaction, etc.).
- `be/src/runtime/`: Runtime contexts during query execution (`RuntimeState`, `DescriptorTbl`, etc.) and resource management.
- `be/src/io/`: File and object storage abstraction (Local, S3, HDFS, etc.) and Reader/Writer implementations.
- `be/src/http/`: HTTP-related Actions/Handlers.
- `be/src/service/`: RPC services (bRPC), internal/external interfaces.
- `be/src/common/`: Infrastructure such as `status.h`, error codes, exception wrappers, etc.
- `be/src/util/`: Utilities and common components.

## Third-party and Base Libraries

- Logging: glog (`LOG(INFO/WARNING/ERROR)`).
- RPC: bRPC (possibly with bthread).
- Serialization: Thrift/Protobuf (headers under `gen_cpp/`).
- Formatting: fmt.

## Error Handling and Return Conventions

- Use unified `Status`/`Result<T>` (see `be/src/common/status.h`).
- Macros:
  - `RETURN_IF_ERROR(stmt)`: return `Status` on failure.
  - `RETURN_IF_ERROR_RESULT(stmt)`: return `unexpected(Status)` on failure.
  - `DORIS_TRY(expr)`: unwrap `Result<T>` and return `Status` on failure.
  - `WARN_IF_ERROR(stmt, prefix)` / `RETURN_NOT_OK_STATUS_WITH_WARN(...)`: propagate errors with logging.
- Review points:
  - Sufficient boundary/parameter checks; thorough resource cleanup on error paths.
  - Avoid hot-path log noise; record key info (tablet_id/txn/path, etc.).

## Concurrency & Resources

- Prefer RAII (`std::unique_ptr`/`std::shared_ptr`); avoid raw pointers and manual `new/delete`.
- Define lock granularity and hold scope clearly; avoid I/O or long work while holding locks.
- Mind thread interruption/cancellation, timeouts, and retry strategies (especially I/O and object storage).

## Performance & Style

- Avoid unnecessary copies; pass by `const&`/`&&`; `reserve`/reuse buffers on hot paths.
- Ensure hot code is efficient and vectorizable; follow columnar batch semantics.
- Favor readability and naming; avoid `using namespace`; use `auto` judiciously.

## Quick Reference Patterns

- Functions: add under `be/src/vec/functions/`, implement interfaces from `function.h`, lifecycle/registration via factory (`FunctionFactory`).
- Pipeline operators: follow operator lifecycle (prepare/open/close, etc.) and pull/push model; ensure backpressure and resource isolation.
- Filesystems: use unified abstractions (`io::FileSystem`, etc.); create Local/S3/HDFS via factories.

## Testing & Observability

- Unit tests: `be/test/` with gtest/gmock; cover error paths and extreme inputs.
- Metrics & logs: add metrics/counters as needed; set log levels/frequency carefully.
