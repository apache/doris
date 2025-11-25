---
applyTo:
  - "fe/fe-core/src/main/java/**/*.java"
  - "be/src/**/*.h"
  - "be/src/**/*.hpp"
  - "be/src/**/*.cc"
  - "be/src/**/*.cpp"
  - "cloud/src/**/*.h"
  - "cloud/src/**/*.hpp"
  - "cloud/src/**/*.cc"
  - "cloud/src/**/*.cpp"
---

# Apache Doris Architecture Instructions

This repository is Apache Doris, an MPP OLAP database. It includes two major modules: FE (Frontend) and BE (Backend). FE handles parsing/optimization/planning/scheduling, and BE handles execution/storage. They communicate via RPC.

## Architecture Overview

- FE (Frontend)
  - Parses/optimizes/plans/schedules (Nereids/Planner, `Coordinator`).
  - Dispatches plan fragments to BEs via bRPC.
  - Key classes: `org.apache.doris.qe.Coordinator`, `org.apache.doris.qe.runtime.MultiFragmentsPipelineTask`, `org.apache.doris.rpc.BackendServiceClient`, `BackendServiceProxy`.
- BE (Backend)
  - Executes fragments (Pipeline/Vec), performs storage reads/writes (Tablet/Rowset/Segment), and handles data exchange (Exchange).
  - Unified entry: `PInternalService{,Impl}` in `be/src/service/internal_service.{h,cpp}`.
  - Fragment management: `FragmentMgr` in `be/src/runtime/fragment_mgr.{h,cpp}`.
  - Data exchange: `be/src/vec/sink/vdata_stream_sender.{h,cpp}`, `be/src/vec/runtime/vdata_stream_recvr.{h,cpp}`; Pipeline-level `exchange_*` operators in `be/src/pipeline/exec/`.
- Protocols
  - Execution path: bRPC + Proto. See `org.apache.doris.proto.InternalService.*` ↔ `be/src/service/internal_service.cpp`.
  - Plan parameters use Thrift (`TExecPlanFragmentParams{List}`, `TPipelineFragmentParams`), serialized by FE and embedded in Proto requests.
- ExecEnv and Contexts
  - `ExecEnv` manages global execution resources; `FragmentMgr` handles fragment lifecycle scheduling.
  - Query/Fragment contexts: `QueryContext`, `RuntimeState` (e.g., `be/src/runtime/query_context.cpp`, `runtime_state.cpp`).

## Core FE↔BE Interactions (Primary RPCs)

- Plan dispatch
  - FE builds and serializes `TExecPlanFragmentParams{List}` or `TPipelineFragmentParams{List}`, then sends via `InternalService.PExecPlanFragmentRequest`.
    - References: `fe/.../planner/GroupCommitPlanner.java` (serialize with `TSerializer`, then `setRequest(...)`).
    - Async API: `BackendServiceProxy.execPlanFragmentsAsync(...)` (see `fe/.../qe/Coordinator.java`, `fe/.../qe/runtime/MultiFragmentsPipelineTask.java`, `fe/.../qe/InsertStreamTxnExecutor.java`).
  - BE entry: `be/src/service/internal_service.cpp`
    - `PInternalService::exec_plan_fragment_prepare(...)`
    - `PInternalService::exec_plan_fragment_start(...)`
- Cancel/Status: `PInternalService::cancel_plan_fragment(...)`; on BE side, `FragmentMgr::cancel_query(...)`.
- Result fetch: `PInternalService::fetch_data(...)`, `fetch_arrow_data(...)`.
- Load/Write: RPCs like `open_load_stream`, `tablet_writer_*` (Stream Load/INSERT).

## Core Flows

- 1) End-to-end query execution (SQL → Result)
  1. FE parses and optimizes (Nereids/Planner) to produce a distributed physical plan.
  2. `Coordinator` splits the plan into fragments, maps them to backends, serializes to Thrift, and embeds into Proto requests.
  3. FE invokes BE `exec_plan_fragment_prepare/start` via bRPC for two-phase startup.
  4. BE deserializes in `PInternalService::_exec_plan_fragment_impl(...)` and hands over to `FragmentMgr::exec_plan_fragment(...)`.
  5. `FragmentMgr` creates Pipeline tasks, initializes `RuntimeState/QueryContext`, then operators execute.
  6. Fragments exchange data via Exchange (local/cross-node); results are emitted by result writers.
  7. FE fetches results via `fetch_data/fetch_arrow_data`, or returns via MySQL/HTTP writers.

- 2) Two-phase fragment startup (prepare/start)
  - FE: `MultiFragmentsPipelineTask.sendPhaseOneRpc/sendPhaseTwoRpc/execPlanFragmentStartAsync(...)`.
  - BE: `PInternalService::exec_plan_fragment_prepare/start(...)`, `FragmentMgr::start_query_execution(...)`.

- 3) Inter-node data exchange (Exchange)
  - Pipeline layer: `exchange_sink_operator.*` and `exchange_source_operator.*` decide the distribution strategy: `ExchangeType::PASSTHROUGH/HASH_SHUFFLE/BUCKET_HASH_SHUFFLE/...` (see `be/src/pipeline/exec/`).
  - Vec layer network transfer: `VDataStreamSender`/`VDataStreamRecvr` (`be/src/vec/sink/runtime/*`).
  - Backpressure & buffering: components like `ExchangeSinkBuffer` (`be/src/pipeline/exec/exchange_sink_operator.h`).

- 4) Result return
  - RPC fetch: `fetch_data`/`fetch_arrow_data`.
  - Or via MySQL/HTTP Writers (e.g., `be/src/runtime/message_body_sink.cpp`, `be/src/vec/sink/vmysql_result_writer.cpp`).

- 5) Stream Load / INSERT INTO
  - FE creates load context → BE `open_load_stream` → client/FE writes blocks via `tablet_writer_*`.
  - BE: `tablets_channel`/`load_channel` manage concurrent routing and retries; persistence via `DeltaWriter{,V2}`/`RowsetWriter`/`SegmentCreator`.
  - Key files: `be/src/runtime/tablets_channel.cpp`, `be/src/olap/delta_writer*.{h,cpp}`, `be/src/olap/rowset/segment_creator.{h,cpp}`.

- 6) Cancellation and failures
  - FE calls `cancel_plan_fragment`; BE `FragmentMgr::cancel_query(...)` broadcasts.
  - Unified error propagation via `Status/Result<T>` (see `be/src/common/status.h`).

- 7) Runtime filters
  - BE provides `merge_filter`/`send_filter_size`/`apply_filterv2` (see `be/src/service/internal_service.cpp`).
  - Used to push down filters across nodes to reduce scan/network overhead.

## Key Code Locations (clickable paths)

- FE dispatch/scheduling:
  - `fe/fe-core/src/main/java/org/apache/doris/qe/Coordinator.java`
  - `fe/fe-core/src/main/java/org/apache/doris/qe/runtime/MultiFragmentsPipelineTask.java`
  - `fe/fe-core/src/main/java/org/apache/doris/rpc/BackendServiceClient.java`
  - `fe/fe-core/src/main/java/org/apache/doris/planner/GroupCommitPlanner.java`
- BE services and fragment management:
  - `be/src/service/internal_service.{h,cpp}` (`exec_plan_fragment_*`, `fetch_*`, `tablet_writer_*`)
  - `be/src/runtime/fragment_mgr.{h,cpp}` (`exec_plan_fragment`, `start_query_execution`, `cancel_query`)
- Data exchange and Pipeline:
  - `be/src/pipeline/exec/exchange_sink_operator.h`, `exchange_source_operator.*`
  - `be/src/vec/sink/vdata_stream_sender.{h,cpp}`, `be/src/vec/runtime/vdata_stream_recvr.{h,cpp}`
- Load and Storage:
  - `be/src/runtime/tablets_channel.cpp`, `be/src/runtime/load_channel*.{h,cpp}`
  - `be/src/olap/delta_writer*.{h,cpp}`, `be/src/olap/rowset/segment_creator.{h,cpp}`

## Reviewer Checklist (Condensed)

- FE→BE dispatch: two-phase startup correct; timeouts/retries/idempotency; serialized payload size controlled (shard/batch if necessary).
- BE execution: clear `FragmentMgr` lifecycle; resource cleanup on cancel/failure; Exchange distribution aligned with stats/bucketing; proper backpressure/buffering.
- Errors & observability: consistent `Status/Result<T>` propagation; logs carry `query_id/fragment_id/tablet_id`; low-cardinality metrics; avoid hot-path logging.

## Conventions & Notes

- Do not modify third-party/generated code directly: `be/src/gen_cpp/**`, `cloud/**/gen-cpp/**`, `be/src/clucene/**`, etc.
- Unified error handling: prefer `RETURN_IF_ERROR`, `DORIS_TRY`, `WARN_IF_ERROR` macros (see `be/src/common/status.h`).
- Exchange selection: ensure it matches upstream distribution/statistics to avoid data skew; add metrics if necessary.
