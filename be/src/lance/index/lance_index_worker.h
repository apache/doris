// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and
// limitations under the License.

#pragma once

#include <gen_cpp/LanceIndexWorker_types.h>
#include <lance/lance.h>

#include <cstdint>
#include <string>

namespace doris::lance {

// Fixed protocol magic of the worker handshake frame: the five ASCII bytes of
// "LANCE" (0x4C 41 4E 43 45) followed by 0x00 and the layout version byte 0x01,
// read as one big-endian int64. Never an RPC surface; the supervisor compares it
// against its own constant before decoding anything else on the pipe.
inline constexpr int64_t HANDSHAKE_PROTOCOL_MAGIC = 0x4C414E43450001LL;
inline constexpr int32_t HANDSHAKE_PROTOCOL_VERSION = 1;

// Name of the single controlled-environment entry through which the supervisor
// hands the worker its own pid (built by the supervisor's controlled
// environment, never inherited from the operator environment). The worker
// re-arms PR_SET_PDEATHSIG at its library entry (an execve into a
// file-capabilities binary clears the setting) and rechecks getppid() against
// this value to close the arm-after-death race.
inline constexpr const char* WORKER_EXPECTED_PPID_ENV = "DORIS_LANCE_WORKER_PPID";

// Controlled-environment variable through which the supervisor hands the
// worker its armed fault-injection debug points (comma-separated names):
// snapshotted per launch by the supervisor (commit 4) and re-registered by
// run_index_worker below through this one shared spelling, so the two sides
// cannot drift. The exec'd image starts with a default-off debug-point gate
// and an empty registry, so without this handoff the worker-side points could
// never fire outside unit tests. The fake worker's own literal
// (be/test/lance/fake_worker/lance_fake_worker.c) is deliberately separate:
// pure C, it cannot include this header.
inline constexpr const char* WORKER_DEBUG_POINTS_ENV = "DORIS_LANCE_WORKER_DEBUG_POINTS";

// The two worker-fault debug points, names fixed by the allowlist the env
// handoff honors. "hang" sits after the dispatch is fully read and validated,
// before the first lance FFI call (the supervisor's wall-clock deadline then
// converges the invocation). "skip_result" sits after the native invocation,
// before the result frame is written: the worker exits complete-but-silent,
// which the handler must classify as indeterminate. Renamed from the closed
// series' "skip_report" with the synchronous result channel in mind.
inline constexpr const char* WORKER_DEBUG_POINT_HANG = "LanceIndexWorker.hang";
inline constexpr const char* WORKER_DEBUG_POINT_SKIP_RESULT = "LanceIndexWorker.skip_result";

// Pure library entry of the isolated one-shot index worker: reads one
// length-prefixed thrift-compact TLanceIndexWorkerDispatch frame from
// dispatch_fd, writes one handshake frame followed by at most one
// TLanceIndexWorkerResult frame to result_fd, and emits bounded static
// diagnostic lines to diag_fd. Returns the process exit code: 0 when a
// complete result frame (a native outcome, an authoritative pre-invocation
// IF no-op, or a trusted pre-invocation rejection) was written, nonzero when
// no trusted result exists (no result frame was written; the supervisor
// converges via termination proof and the handler synthesizes the
// indeterminate phase).
//
// The function touches no BE global state (no config, logging, metrics, or
// ExecEnv) except the debug-point registry re-registration from
// WORKER_DEBUG_POINTS_ENV at its very top: a bare main can exec it directly,
// and unit tests can drive it over pipe pairs with tuned bounds. Its first
// actions, before reading the dispatch, are prctl(PR_SET_DUMPABLE, 0)
// (credentials in memory can never reach a core file) and the exec-side
// PR_SET_PDEATHSIG re-arm + getppid recheck against WORKER_EXPECTED_PPID_ENV
// (the parent-death backstop for deployment shapes whose execve clears the
// setting).
struct IndexWorkerParams {
    int dispatch_fd = 0;
    int result_fd = 1;
    int diag_fd = 2;
    // Protocol constants are injectable so unit tests can exercise the
    // length-cap paths with small values.
    uint32_t max_dispatch_bytes = 512 * 1024;
    uint32_t max_result_bytes = 8 * 1024;
};

int run_index_worker(const IndexWorkerParams& params);

// ── Unit-test / library seams (production reaches them only through
// run_index_worker; tests drive them directly) ──

// Outcome of resolving the normalized logical index name against one pinned
// snapshot's physical index list.
enum class PhysicalIndexResolution {
    // Exactly one stored spelling matches the normalized name
    // case-insensitively (ASCII fold): `stored_spelling` holds it.
    UNIQUE,
    // No stored spelling matches.
    ABSENT,
    // More than one stored spelling matches: the snapshot is ambiguous and no
    // single drop/replace by name can be trusted.
    AMBIGUOUS,
    // The provider list breached a bounded-processing rule (entry count or
    // per-name byte bound): typed pre-invocation rejection territory.
    UNBOUNDED,
    // The payload is not a decodable index list: provider contract violation,
    // no trusted typed fact exists.
    MALFORMED
};

// The worker-defended bounds of the physical index list, mirroring the FE's
// discovery bounds (LanceDatasetIndexDiscovery): the raw entry count cap and
// the external-string byte cap per stored name.
inline constexpr size_t MAX_PHYSICAL_INDEX_ENTRIES = 16 * 1024;
inline constexpr size_t MAX_INDEX_NAME_BYTES = 1024;

// Reserved system index entry names the resolution skips, mirroring the FE's
// discovery filter (lance-c's user-index listing does not emit them today;
// the filter is the defensive mirror so a provider change cannot surface
// system entries as droppable user indexes).
bool is_system_index_name(const std::string& name);

// Resolves `normalized_name` (the FE's LanceIndexNameNormalizer output:
// Java toLowerCase(Locale.ROOT), already applied) against the physical index
// list JSON of one pinned snapshot (lance_dataset_index_list_json shape: a
// JSON array of {name, uuid, columns, type, dataset_version} objects). The
// match is case-insensitive under the byte-wise ASCII fold (A-Z to a-z) the
// contract library also uses; non-ASCII bytes never fold, so a stored
// spelling containing them matches only by exact byte equality. System entry
// names are skipped; exact-duplicate spellings collapse to one candidate.
PhysicalIndexResolution resolve_physical_index(const std::string& index_list_json,
                                               const std::string& normalized_name,
                                               std::string* stored_spelling);

// The validated IVF_PQ arguments mirrored from the admitted properties JSON
// (the five-key whitelist replay against the frame's bounds snapshot). Built
// once by run_index_worker's revalidation rail and handed to the mutation
// phase; the native call site is the only place the values cross the C
// boundary.
struct VectorIndexArguments {
    uint32_t num_partitions = 0;
    uint32_t num_sub_vectors = 0;
    LanceMetricType metric = LANCE_METRIC_L2;
};

// The complete outcome of the worker's post-open phase. Every field is the
// raw content of the single result frame run_index_worker writes (or the
// instruction not to write one).
struct WorkerMutationOutcome {
    // False: no trusted result exists; run_index_worker exits nonzero without
    // a result frame (the handler synthesizes INDETERMINATE_AFTER_LAUNCH).
    bool trusted = false;
    TLanceIndexWorkerResultPhase::type phase = TLanceIndexWorkerResultPhase::NATIVE_RETURN;
    // Negative only with PRE_INVOCATION_REJECTED (one of the
    // LANCE_WORKER_REJECTED_* constants); with NATIVE_RETURN it is the raw
    // native Lance error code of the single mutating call (0 = LANCE_OK).
    int32_t lance_error_code = 0;
    // True only for the authoritative pre-invocation IF no-ops (phase
    // NATIVE_RETURN, code 0): the message slot then carries the static no-op
    // category instead of nothing.
    bool no_op = false;
    // Diagnostic: set only when the read-only latest observation saw the
    // dataset's latest version above the admitted one.
    int64_t observed_latest_version = 0;
};

// Everything the worker does after the pinned open succeeds: the read-only
// latest-version observation (diagnostics only, never a gate), contract v1
// revalidation for CREATE/REPLACE, the physical-snapshot resolution with the
// pre-invocation IF no-ops, and the single native mutating call whose typed
// error passes through verbatim. `build_arguments` carries the already
// revalidated properties of a CREATE/REPLACE dispatch (unused for DROP).
// Exposed so unit tests drive the real semantics against real
// (fixture-copied) local datasets; the D11 local-URI gate and the pinned open
// sit before it in run_index_worker and are covered by the params tests.
WorkerMutationOutcome execute_worker_mutation(LanceDataset* pinned_dataset,
                                              uint64_t admitted_dataset_version,
                                              const TLanceIndexWorkerDispatch& dispatch,
                                              const VectorIndexArguments& build_arguments);

} // namespace doris::lance
