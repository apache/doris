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
// specific language governing permissions and limitations
// under the License.

#include "lance/index_worker.h"

#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/MasterService_types.h>
#include <lance/lance.h>
#include <rapidjson/document.h>
#include <thrift/TConfiguration.h>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TBufferTransports.h>

#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#ifdef __linux__
#include <signal.h>
#include <sys/prctl.h>
#endif
#include <sys/resource.h>
#include <unistd.h>

#include "common/logging.h"
#include "lance/index_worker_contract.h"
#include "util/debug_points.h"

namespace doris::lance {
namespace {

// Protocol bounds (the dispatch bound itself is injectable through
// IndexWorkerParams). The thrift decoder recursion cap stops deep-nesting frames.
constexpr uint32_t MAX_HANDSHAKE_BYTES = 4 * 1024;
constexpr int THRIFT_DECODE_DEPTH_LIMIT = 16;
constexpr size_t MAX_STORAGE_OPTIONS = 64;
constexpr size_t MAX_STORAGE_OPTION_KEY_BYTES = 256;
constexpr size_t MAX_STORAGE_OPTION_VALUE_BYTES = 4096;

// Static diagnostic categories are the only text this process ever emits: no
// storage-option keys or values, no provider message text, no dataset locator.
constexpr const char* DIAG_MALFORMED_FRAME = "lance worker: malformed dispatch frame\n";
constexpr const char* DIAG_UNDECODABLE_FRAME = "lance worker: undecodable dispatch frame\n";
constexpr const char* DIAG_HANDSHAKE_WRITE_FAILED = "lance worker: handshake write failed\n";
constexpr const char* DIAG_VERSION_ASSERTION = "lance worker: pinned version assertion failed\n";
constexpr const char* DIAG_UNTYPED_OPEN_FAILURE = "lance worker: untyped open failure\n";
constexpr const char* DIAG_UNTYPED_RECOMPUTE_FAILURE = "lance worker: untyped recompute failure\n";
constexpr const char* DIAG_UNKNOWN_NATIVE_CODE = "lance worker: unmapped native error code\n";
constexpr const char* DIAG_RESULT_WRITE_FAILED = "lance worker: result write failed\n";
constexpr const char* DIAG_PARENT_GUARD = "lance worker: parent-death guard tripped\n";

// Bounded best-effort diagnostic write. Never carries dynamic content.
void diag(int fd, const char* message) {
    if (fd < 0 || message == nullptr) {
        return;
    }
    size_t size = std::strlen(message);
    ssize_t written = ::write(fd, message, size);
    (void)written;
}

size_t read_up_to(int fd, uint8_t* buffer, size_t size) {
    size_t total = 0;
    while (total < size) {
        ssize_t count = ::read(fd, buffer + total, size - total);
        if (count == 0) {
            break; // EOF before the frame completed
        }
        if (count < 0) {
            if (errno == EINTR) {
                continue;
            }
            break;
        }
        total += static_cast<size_t>(count);
    }
    return total;
}

bool write_full(int fd, const uint8_t* data, size_t size) {
    size_t total = 0;
    while (total < size) {
        ssize_t count = ::write(fd, data + total, size - total);
        if (count < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        total += static_cast<size_t>(count);
    }
    return true;
}

// Reads one length-prefixed frame: uint32 big-endian length, capped BEFORE any
// allocation, then exactly that many payload bytes. A partial or oversized frame
// is a malformed protocol and never reaches the decoder.
bool read_frame(int fd, uint32_t cap, std::vector<uint8_t>* out) {
    uint8_t header[4];
    if (read_up_to(fd, header, sizeof(header)) != sizeof(header)) {
        return false;
    }
    uint32_t length = (static_cast<uint32_t>(header[0]) << 24) |
                      (static_cast<uint32_t>(header[1]) << 16) |
                      (static_cast<uint32_t>(header[2]) << 8) | static_cast<uint32_t>(header[3]);
    if (length == 0 || length > cap) {
        return false;
    }
    out->resize(length);
    return read_up_to(fd, out->data(), length) == length;
}

bool write_frame(int fd, const std::vector<uint8_t>& payload) {
    uint32_t length = static_cast<uint32_t>(payload.size());
    uint8_t header[4] = {static_cast<uint8_t>(length >> 24), static_cast<uint8_t>(length >> 16),
                         static_cast<uint8_t>(length >> 8), static_cast<uint8_t>(length)};
    return write_full(fd, header, sizeof(header)) &&
           write_full(fd, payload.data(), payload.size());
}

bool decode_dispatch(const std::vector<uint8_t>& frame, TLanceIndexJobDispatch* dispatch) {
    try {
        auto config = std::make_shared<apache::thrift::TConfiguration>();
        config->setMaxMessageSize(static_cast<int>(frame.size()));
        config->setRecursionLimit(THRIFT_DECODE_DEPTH_LIMIT);
        auto transport = std::make_shared<apache::thrift::transport::TMemoryBuffer>(
                const_cast<uint8_t*>(frame.data()), static_cast<uint32_t>(frame.size()),
                apache::thrift::transport::TMemoryBuffer::OBSERVE, config);
        apache::thrift::protocol::TCompactProtocol protocol(
                transport, static_cast<int32_t>(frame.size()), /*container_limit=*/1024);
        dispatch->read(&protocol);
        // Exactly one struct per frame: trailing bytes are a protocol violation.
        return transport->available_read() == 0;
    } catch (...) {
        return false;
    }
}

template <typename T>
bool encode_compact(const T& value, std::vector<uint8_t>* out) {
    try {
        auto transport = std::make_shared<apache::thrift::transport::TMemoryBuffer>();
        apache::thrift::protocol::TCompactProtocol protocol(transport);
        value.write(&protocol);
        uint8_t* buffer = nullptr;
        uint32_t size = 0;
        transport->getBuffer(&buffer, &size);
        out->assign(buffer, buffer + size);
    } catch (...) {
        return false;
    }
    return true;
}

// Byte-wise ASCII fold (A-Z to a-z); returns false on any non-ASCII byte.
bool ascii_fold(const std::string& input, std::string* out) {
    bool pure_ascii = true;
    std::string folded;
    folded.reserve(input.size());
    for (char c : input) {
        unsigned char byte = static_cast<unsigned char>(c);
        if (byte >= 0x80) {
            pure_ascii = false;
        }
        folded.push_back(byte >= 'A' && byte <= 'Z' ? static_cast<char>(byte - 'A' + 'a') : c);
    }
    *out = std::move(folded);
    return pure_ascii;
}

// Strictly parses a positive uint32: digits only, no sign, no whitespace.
bool parse_positive_uint32(const std::string& text, uint32_t* out) {
    if (text.empty()) {
        return false;
    }
    uint64_t value = 0;
    for (char c : text) {
        if (c < '0' || c > '9') {
            return false;
        }
        value = value * 10 + static_cast<uint64_t>(c - '0');
        if (value > UINT32_MAX) {
            return false;
        }
    }
    if (value == 0) {
        return false;
    }
    *out = static_cast<uint32_t>(value);
    return true;
}

// Re-validates the storage options exactly as the FE bounded them before the
// first byte of network I/O: the BE is the last line of defense for values that
// cross a C-string boundary into lance-c.
bool storage_options_valid(const TLanceIndexJobDispatch& dispatch) {
    if (!dispatch.__isset.storage_options) {
        return true;
    }
    const auto& options = dispatch.storage_options;
    if (options.size() > MAX_STORAGE_OPTIONS) {
        return false;
    }
    for (const auto& option : options) {
        if (option.first.size() > MAX_STORAGE_OPTION_KEY_BYTES ||
            option.second.size() > MAX_STORAGE_OPTION_VALUE_BYTES) {
            return false;
        }
        if (option.first.find('\0') != std::string::npos ||
            option.second.find('\0') != std::string::npos) {
            return false;
        }
    }
    return true;
}

// The validated IVF_PQ arguments mirrored from the persisted properties JSON.
struct VectorIndexArguments {
    uint32_t num_partitions = 0;
    uint32_t num_sub_vectors = 0;
    LanceMetricType metric = LANCE_METRIC_L2;
};

// Mirrors the FE five-key whitelist validation (case-insensitive keys, no
// duplicates, IVF_PQ only, metric vocabulary, positive bounded integers,
// num_bits pinned to 8) and replays the admitted bounds snapshot rather than any
// live config. A metric the request omitted defaults to lance's L2, exactly as
// the FE admission treats it.
bool validate_and_map_properties(const std::string& json, int32_t max_num_partitions,
                                 int32_t max_num_sub_vectors, VectorIndexArguments* out) {
    rapidjson::Document doc;
    // Iterative parse: the JSON arrives on the dispatch frame and is therefore
    // FE-controlled; rapidjson's default recursive descent would blow the
    // worker's stack on a deeply nested payload (~500KB of '[' fits the frame).
    doc.Parse<rapidjson::kParseIterativeFlag>(json.data(), json.size());
    if (doc.HasParseError() || !doc.IsObject()) {
        return false;
    }
    std::optional<std::string> index_type;
    std::optional<std::string> metric;
    std::optional<std::string> num_partitions;
    std::optional<std::string> num_sub_vectors;
    std::optional<std::string> num_bits;
    for (auto member = doc.MemberBegin(); member != doc.MemberEnd(); ++member) {
        if (!member->name.IsString() || !member->value.IsString()) {
            return false;
        }
        std::string key(member->name.GetString(), member->name.GetStringLength());
        std::string folded_key;
        if (!ascii_fold(key, &folded_key)) {
            return false;
        }
        std::string value(member->value.GetString(), member->value.GetStringLength());
        std::optional<std::string>* slot = nullptr;
        if (folded_key == "index_type") {
            slot = &index_type;
        } else if (folded_key == "metric") {
            slot = &metric;
        } else if (folded_key == "num_partitions") {
            slot = &num_partitions;
        } else if (folded_key == "num_sub_vectors") {
            slot = &num_sub_vectors;
        } else if (folded_key == "num_bits") {
            slot = &num_bits;
        } else {
            return false; // outside the five-key whitelist
        }
        if (slot->has_value()) {
            return false; // duplicate key under the case-insensitive fold
        }
        *slot = std::move(value);
    }
    std::string folded_type;
    if (!index_type.has_value() || !ascii_fold(*index_type, &folded_type) ||
        folded_type != "ivf_pq") {
        return false;
    }
    if (metric.has_value()) {
        std::string folded_metric;
        if (!ascii_fold(*metric, &folded_metric)) {
            return false;
        }
        if (folded_metric == "l2") {
            out->metric = LANCE_METRIC_L2;
        } else if (folded_metric == "cosine") {
            out->metric = LANCE_METRIC_COSINE;
        } else if (folded_metric == "dot") {
            out->metric = LANCE_METRIC_DOT;
        } else {
            return false;
        }
    }
    if (!num_partitions.has_value() ||
        !parse_positive_uint32(*num_partitions, &out->num_partitions)) {
        return false;
    }
    if (!num_sub_vectors.has_value() ||
        !parse_positive_uint32(*num_sub_vectors, &out->num_sub_vectors)) {
        return false;
    }
    if (static_cast<int64_t>(out->num_partitions) > max_num_partitions ||
        static_cast<int64_t>(out->num_sub_vectors) > max_num_sub_vectors) {
        return false;
    }
    if (num_bits.has_value()) {
        uint32_t bits = 0;
        if (!parse_positive_uint32(*num_bits, &bits) || bits != 8) {
            return false;
        }
    }
    return true;
}

// D11: datasets on the local filesystem are rejected before any native call,
// even though the FE normally never dispatches them. A scheme-less locator is an
// absolute local path (the FE normalizer admits nothing else without a scheme);
// a file:// URI is local by definition. Mirrors the FE isLocalFileDataset check.
bool is_local_dataset_uri(const std::string& uri) {
    size_t separator = uri.find("://");
    if (separator == std::string::npos) {
        return true;
    }
    std::string scheme;
    ascii_fold(uri.substr(0, separator), &scheme);
    return scheme == "file";
}

// Best-effort self-observation for the handshake. The supervisor never trusts
// these values: it compares them against its own cgroup write/read-back and
// kills the worker on any mismatch. -1 / "" means "could not observe".
#ifdef __linux__
std::string read_self_cgroup_path() {
    FILE* file = std::fopen("/proc/self/cgroup", "r");
    if (file == nullptr) {
        return "";
    }
    char line[1024];
    std::string path;
    std::string fallback;
    while (std::fgets(line, sizeof(line), file) != nullptr) {
        std::string text(line);
        while (!text.empty() && (text.back() == '\n' || text.back() == '\r')) {
            text.pop_back();
        }
        size_t colon = text.rfind(':');
        if (colon == std::string::npos) {
            continue;
        }
        std::string entry_path = text.substr(colon + 1);
        if (fallback.empty()) {
            fallback = entry_path;
        }
        // The v2 unified hierarchy line is "0::<path>".
        if (text.compare(0, 3, "0::") == 0) {
            path = entry_path;
            break;
        }
    }
    std::fclose(file);
    if (path.empty()) {
        path = fallback;
    }
    if (path.size() > 512) {
        path.resize(512);
    }
    return path;
}

int64_t read_cgroup_limit(const std::string& cgroup_path, const char* file_name) {
    if (cgroup_path.empty() || cgroup_path.size() > 512) {
        return -1;
    }
    std::string full_path = "/sys/fs/cgroup" + cgroup_path + "/" + file_name;
    FILE* file = std::fopen(full_path.c_str(), "r");
    if (file == nullptr) {
        return -1;
    }
    char buffer[64] = {0};
    size_t read_count = std::fread(buffer, 1, sizeof(buffer) - 1, file);
    std::fclose(file);
    if (read_count == 0) {
        return -1;
    }
    std::string text(buffer, read_count);
    while (!text.empty() &&
           (text.back() == '\n' || text.back() == '\r' || text.back() == ' ')) {
        text.pop_back();
    }
    if (text == "max") {
        return INT64_MAX;
    }
    uint64_t value = 0;
    if (text.empty()) {
        return -1;
    }
    for (char c : text) {
        if (c < '0' || c > '9') {
            return -1;
        }
        value = value * 10 + static_cast<uint64_t>(c - '0');
        if (value > static_cast<uint64_t>(INT64_MAX)) {
            return -1;
        }
    }
    return static_cast<int64_t>(value);
}
#else
std::string read_self_cgroup_path() {
    return "";
}

int64_t read_cgroup_limit(const std::string& /*cgroup_path*/, const char* /*file_name*/) {
    return -1;
}
#endif

int64_t rlimit_current(int resource) {
    struct rlimit limit;
    if (getrlimit(resource, &limit) != 0) {
        return -1;
    }
    if (limit.rlim_cur == RLIM_INFINITY) {
        return INT64_MAX;
    }
    return static_cast<int64_t>(limit.rlim_cur);
}

bool write_handshake(const IndexWorkerParams& params) {
    TLanceIndexWorkerHandshake handshake;
    handshake.protocol_magic = HANDSHAKE_PROTOCOL_MAGIC;
    handshake.protocol_version = HANDSHAKE_PROTOCOL_VERSION;
    handshake.cgroup_path = read_self_cgroup_path();
    handshake.memory_max_bytes = read_cgroup_limit(handshake.cgroup_path, "memory.max");
    handshake.pids_max = read_cgroup_limit(handshake.cgroup_path, "pids.max");
#ifdef RLIMIT_AS
    handshake.rlimit_as_bytes = rlimit_current(RLIMIT_AS);
#else
    handshake.rlimit_as_bytes = -1;
#endif
    handshake.rlimit_cpu_seconds = rlimit_current(RLIMIT_CPU);
    handshake.rlimit_nofile = rlimit_current(RLIMIT_NOFILE);
    handshake.rlimit_core = rlimit_current(RLIMIT_CORE);
    std::vector<uint8_t> payload;
    if (!encode_compact(handshake, &payload) || payload.size() > MAX_HANDSHAKE_BYTES) {
        return false;
    }
    return write_frame(params.result_fd, payload);
}

// Composes and writes the single result frame. Only a trusted typed result code
// ever reaches this point; the message slot carries at most a static category,
// never provider text. Returns false when no complete frame reached the pipe.
bool write_result_frame(const IndexWorkerParams& params, const TLanceIndexJobDispatch& dispatch,
                        TLanceIndexJobResultCode::type result_code, bool if_condition_noop,
                        bool external_metadata_advanced, const char* static_category) {
    TLanceIndexJobReport report;
    report.job_id = dispatch.job_id;
    report.dispatch_revision = dispatch.dispatch_revision;
    report.invocation_id = dispatch.invocation_id;
    report.be_process_epoch = dispatch.be_process_epoch;
    report.result_code = result_code;
    if (if_condition_noop) {
        report.__set_completion_reason(TLanceIndexCompletionReason::IF_CONDITION_NOOP);
    }
    if (static_category != nullptr) {
        report.__set_sanitized_message(static_category);
    }
    report.__set_external_metadata_advanced(external_metadata_advanced);
    std::vector<uint8_t> payload;
    if (!encode_compact(report, &payload) || payload.size() > params.max_result_bytes) {
        return false;
    }
    return write_frame(params.result_fd, payload);
}

// Trusted pre-invocation rejections: a complete frame with a typed code proves
// NOT_COMMITTED, so a successfully written frame is a clean zero exit.
int reject_pre_invocation(const IndexWorkerParams& params, const TLanceIndexJobDispatch& dispatch,
                          TLanceIndexJobResultCode::type result_code,
                          bool external_metadata_advanced, const char* static_category) {
    if (!write_result_frame(params, dispatch, result_code, /*if_condition_noop=*/false,
                            external_metadata_advanced, static_category)) {
        diag(params.diag_fd, DIAG_RESULT_WRITE_FAILED);
        return 1;
    }
    return 0;
}

struct LanceDatasetDeleter {
    void operator()(LanceDataset* dataset) const {
        if (dataset != nullptr) {
            lance_dataset_close(dataset);
        }
    }
};

} // namespace

int run_index_worker(const IndexWorkerParams& params) {
    // Debug-point handoff across the exec boundary: the supervisor snapshots
    // its active worker-fault points into the controlled environment (see
    // build_child_env) because this process starts with a default-off gate and
    // an empty registry (the --lance-worker main branch runs before any BE
    // global state). Only the two known worker-fault names are honored —
    // anything else is ignored, and no byte of the value is ever logged.
    if (const char* handoff = std::getenv(WORKER_DEBUG_POINTS_ENV)) {
        const std::string list(handoff, std::min(std::strlen(handoff), size_t {256}));
        bool registered = false;
        size_t pos = 0;
        while (pos <= list.size()) {
            const size_t comma = list.find(',', pos);
            const std::string token =
                    list.substr(pos, comma == std::string::npos ? comma : comma - pos);
            if (token == "LanceIndexWorker.hang" || token == "LanceIndexWorker.skip_report") {
                DebugPoints::instance()->add(token);
                registered = true;
            }
            if (comma == std::string::npos) {
                break;
            }
            pos = comma + 1;
        }
        if (registered) {
            config::enable_debug_points = true;
        }
    }

    // Step 1: before anything else, including the dispatch read — credentials
    // entering this process must never reach a core file.
#ifdef __linux__
    prctl(PR_SET_DUMPABLE, 0, 0, 0, 0);
    // Re-arm the parent-death signal at the exec-side entry (the contract's
    // second guarantee, cpp_interface_contract §3: the supervisor arms it
    // post-fork/pre-exec, the worker main re-arms it here). A plain self-exec
    // preserves the setting across execve, but an execve into a binary with
    // file capabilities clears it — the same deployment shape the DUMPABLE
    // re-set above exists for — so without this re-arm the BE-loss backstop
    // would silently lapse. The getppid recheck closes the arm-after-death
    // race: a worker whose supervisor died before the re-arm has already been
    // reparented, and exits instead of running free. The expected supervisor
    // pid rides the supervisor's controlled env whitelist (never inherited
    // from the operator environment); an absent value (the in-process unit
    // tests) skips the recheck.
    prctl(PR_SET_PDEATHSIG, SIGKILL, 0, 0, 0);
    if (const char* expected_ppid = std::getenv(WORKER_EXPECTED_PPID_ENV)) {
        int64_t expected = 0;
        bool well_formed = *expected_ppid != '\0';
        for (const char* p = expected_ppid; well_formed && *p != '\0'; ++p) {
            if (*p < '0' || *p > '9' || expected > (INT64_MAX - 9) / 10) {
                well_formed = false;
            } else {
                expected = expected * 10 + (*p - '0');
            }
        }
        if (!well_formed || expected <= 0 || expected > static_cast<int64_t>(INT32_MAX) ||
            ::getppid() != static_cast<pid_t>(expected)) {
            diag(params.diag_fd, DIAG_PARENT_GUARD);
            return 1;
        }
    }
#endif

    // Step 2: the bounded dispatch frame. Without a decoded dispatch there is no
    // identity to frame a trusted result with, so a malformed frame is a bare
    // nonzero exit.
    std::vector<uint8_t> frame;
    if (!read_frame(params.dispatch_fd, params.max_dispatch_bytes, &frame)) {
        diag(params.diag_fd, DIAG_MALFORMED_FRAME);
        return 1;
    }
    TLanceIndexJobDispatch dispatch;
    if (!decode_dispatch(frame, &dispatch)) {
        diag(params.diag_fd, DIAG_UNDECODABLE_FRAME);
        return 1;
    }

    // Step 3: the handshake frame. It precedes EVERY result frame, including the
    // pre-invocation rejections below: the supervisor decodes the first stdout
    // frame as the handshake, so a result emitted before it would be a protocol
    // violation and the typed rejection would never reach the FE. (A malformed
    // dispatch above still exits bare — without an identity there is no result
    // path at all.)
    if (!write_handshake(params)) {
        diag(params.diag_fd, DIAG_HANDSHAKE_WRITE_FAILED);
        return 1;
    }

    // Step 4: dispatch revalidation. Every rejection from here on carries the
    // dispatch identity in a complete pre-invocation result frame.
    if (!storage_options_valid(dispatch)) {
        return reject_pre_invocation(params, dispatch,
                                     TLanceIndexJobResultCode::PRE_INVOCATION_RESOURCE_REJECTED,
                                     false, "resource rejected");
    }
    const bool is_create = dispatch.mutation_type == TLanceIndexMutationType::CREATE;
    const bool is_replace = dispatch.mutation_type == TLanceIndexMutationType::REPLACE;
    const bool is_drop = dispatch.mutation_type == TLanceIndexMutationType::DROP;
    if (!is_create && !is_replace && !is_drop) {
        return reject_pre_invocation(params, dispatch,
                                     TLanceIndexJobResultCode::PRE_INVOCATION_RESOURCE_REJECTED,
                                     false, "resource rejected");
    }
    const bool is_build = is_create || is_replace;
    // Scalar sanity: the values that cross C boundaries or drive the pinned open.
    // admitted_dataset_version must be positive because version 0 is the latest
    // sentinel, which the pinned open never uses.
    if (dispatch.dataset_uri.empty() || dispatch.index_name.empty() ||
        dispatch.admitted_dataset_version <= 0 ||
        dispatch.dataset_uri.find('\0') != std::string::npos ||
        dispatch.index_name.find('\0') != std::string::npos ||
        dispatch.column_name.find('\0') != std::string::npos) {
        return reject_pre_invocation(params, dispatch,
                                     TLanceIndexJobResultCode::PRE_INVOCATION_RESOURCE_REJECTED,
                                     false, "resource rejected");
    }
    // The admitted bounds snapshot must be present and positive (a dispatch
    // replayed from a pre-snapshot record is rejected safely rather than judged
    // against a hard-coded bound).
    if (!dispatch.__isset.max_num_partitions || dispatch.max_num_partitions <= 0 ||
        !dispatch.__isset.max_num_sub_vectors || dispatch.max_num_sub_vectors <= 0) {
        return reject_pre_invocation(
                params, dispatch, TLanceIndexJobResultCode::PRE_INVOCATION_UNSUPPORTED_SCHEMA_CONTRACT,
                false, "unsupported schema contract");
    }
    VectorIndexArguments arguments;
    if (is_build) {
        std::string folded_index_type;
        if (dispatch.column_name.empty() || !dispatch.__isset.properties_json ||
            dispatch.properties_json.empty() ||
            !ascii_fold(dispatch.index_type, &folded_index_type) ||
            folded_index_type != "ivf_pq" ||
            !validate_and_map_properties(dispatch.properties_json, dispatch.max_num_partitions,
                                         dispatch.max_num_sub_vectors, &arguments)) {
            return reject_pre_invocation(
                    params, dispatch,
                    TLanceIndexJobResultCode::PRE_INVOCATION_UNSUPPORTED_SCHEMA_CONTRACT, false,
                    "unsupported schema contract");
        }
    }

    // Step 5: D11 — a local-filesystem dataset is rejected before any native
    // call, regardless of any FE-side assertion.
    if (is_local_dataset_uri(dispatch.dataset_uri)) {
        return reject_pre_invocation(params, dispatch,
                                     TLanceIndexJobResultCode::PRE_INVOCATION_RESOURCE_REJECTED,
                                     false, "resource rejected");
    }

    // Fault-injection point: hang after the dispatch frame is fully read and
    // validated, before the first lance FFI call (the handshake has already
    // passed supervisor-side, so the supervisor's wall-clock deadline expires,
    // TERM->KILL escalates, and the FE converges UNKNOWN). The block releases
    // when the point is removed (the in-process unit tests rely on that).
    DBUG_EXECUTE_IF("LanceIndexWorker.hang", DBUG_BLOCK);

    // Step 6: the pinned open, never version 0. The storage options live in one
    // owned buffer until process exit; only the pointer view reaches lance-c, and
    // no key or value is ever logged.
    std::vector<std::string> storage_strings;
    if (dispatch.__isset.storage_options) {
        storage_strings.reserve(dispatch.storage_options.size() * 2);
        for (const auto& option : dispatch.storage_options) {
            storage_strings.push_back(option.first);
            storage_strings.push_back(option.second);
        }
    }
    std::vector<const char*> storage_ptrs;
    storage_ptrs.reserve(storage_strings.size() + 1);
    for (const auto& entry : storage_strings) {
        storage_ptrs.push_back(entry.c_str());
    }
    storage_ptrs.push_back(nullptr);
    const char* const* storage_options = storage_strings.empty() ? nullptr : storage_ptrs.data();
    const uint64_t admitted_version = static_cast<uint64_t>(dispatch.admitted_dataset_version);

    std::unique_ptr<LanceDataset, LanceDatasetDeleter> dataset(lance_dataset_open(
            dispatch.dataset_uri.c_str(), storage_options, admitted_version));
    if (dataset == nullptr) {
        // Save the typed error FIRST: the observation open below must never
        // overwrite the pinned-open evidence.
        SavedLanceError open_error = save_lance_error();
        if (open_error.code == LANCE_ERR_NOT_FOUND) {
            // A bare NOT_FOUND cannot attribute "admitted version unavailable" (a
            // missing dataset URI reads the same), so observe latest read-only:
            // the mutation never moves to this handle, and it closes immediately.
            std::unique_ptr<LanceDataset, LanceDatasetDeleter> observation(lance_dataset_open(
                    dispatch.dataset_uri.c_str(), storage_options, /*latest=*/0));
            if (observation != nullptr) {
                uint64_t observed_version = lance_dataset_version(observation.get());
                observation.reset();
                if (observed_version > admitted_version) {
                    // The admitted manifest was garbage-collected: attributable.
                    return reject_pre_invocation(
                            params, dispatch, TLanceIndexJobResultCode::PRE_INVOCATION_STALE_ADMISSION,
                            true, "stale admission");
                }
                // A same-or-older latest contradicts the NOT_FOUND (race or URI
                // replacement): unattributable, keep the saved pinned-open error.
                return reject_pre_invocation(
                        params, dispatch, TLanceIndexJobResultCode::PRE_INVOCATION_RESOURCE_REJECTED,
                        false, "resource rejected");
            }
            // The observation itself failed: dataset/URI/credential-layer problem,
            // unattributable, keep the saved pinned-open error.
            return reject_pre_invocation(params, dispatch,
                                         TLanceIndexJobResultCode::PRE_INVOCATION_RESOURCE_REJECTED,
                                         false, "resource rejected");
        }
        if (open_error.code != LANCE_OK) {
            // lance-c 0.1.9 produces no typed credential-expiry evidence, so every
            // other typed open failure is a resource rejection.
            return reject_pre_invocation(params, dispatch,
                                         TLanceIndexJobResultCode::PRE_INVOCATION_RESOURCE_REJECTED,
                                         false, "resource rejected");
        }
        diag(params.diag_fd, DIAG_UNTYPED_OPEN_FAILURE);
        return 1;
    }

    // Step 7: the version assertion. A mismatch means the open did not honor the
    // pin; there is no trusted result, so exit without a result frame.
    if (lance_dataset_version(dataset.get()) != admitted_version) {
        diag(params.diag_fd, DIAG_VERSION_ASSERTION);
        return 1;
    }

    // Step 8: external-metadata advancement on the pinned handle (a read-only
    // observation; the call re-reads the manifest listing). A failed observation
    // is false, never an inference.
    bool external_metadata_advanced = false;
    {
        uint64_t latest_version = lance_dataset_latest_version(dataset.get());
        if (latest_version != 0) {
            external_metadata_advanced = latest_version > admitted_version;
        }
    }

    // Step 9: contract revalidation, one path for CREATE, REPLACE and DROP. An
    // empty contract payload (a record from before DROP contract persistence) is
    // rejected safely, never skipped.
    SchemaContract admitted_contract;
    if (dispatch.schema_contract_json.empty() ||
        parse_schema_contract(dispatch.schema_contract_json, &admitted_contract) !=
                ContractStatus::OK) {
        return reject_pre_invocation(
                params, dispatch, TLanceIndexJobResultCode::PRE_INVOCATION_UNSUPPORTED_SCHEMA_CONTRACT,
                external_metadata_advanced, "unsupported schema contract");
    }
    SchemaContract recomputed_contract;
    ContractStatus recompute_status =
            recompute_contract(dataset.get(), dispatch.column_name, &recomputed_contract);
    if (recompute_status == ContractStatus::UNSUPPORTED) {
        return reject_pre_invocation(
                params, dispatch, TLanceIndexJobResultCode::PRE_INVOCATION_UNSUPPORTED_SCHEMA_CONTRACT,
                external_metadata_advanced, "unsupported schema contract");
    }
    if (recompute_status == ContractStatus::RESOURCE_REJECTED) {
        return reject_pre_invocation(params, dispatch,
                                     TLanceIndexJobResultCode::PRE_INVOCATION_RESOURCE_REJECTED,
                                     external_metadata_advanced, "resource rejected");
    }
    if (recompute_status != ContractStatus::OK) {
        diag(params.diag_fd, DIAG_UNTYPED_RECOMPUTE_FAILURE);
        return 1;
    }
    if (!(admitted_contract == recomputed_contract)) {
        return reject_pre_invocation(params, dispatch,
                                     TLanceIndexJobResultCode::PRE_INVOCATION_STALE_ADMISSION,
                                     external_metadata_advanced, "stale admission");
    }

    // Step 10: the mirror validation of the admitted build request against the
    // agreed contract (the contract holds exactly one field here: the comparison
    // above pins equality with the single-field recompute).
    if (is_build) {
        // Defensive: recompute_contract's OK path always produces exactly one
        // field, so front() is safe after the equality gate; refuse the
        // invariant instead of relying on it if that ever changes.
        if (recomputed_contract.flds.empty()) {
            diag(params.diag_fd, DIAG_UNTYPED_RECOMPUTE_FAILURE);
            return 1;
        }
        if (!vector_index_shape_supported(recomputed_contract.flds.front(),
                                          arguments.num_sub_vectors)) {
            return reject_pre_invocation(
                    params, dispatch,
                    TLanceIndexJobResultCode::PRE_INVOCATION_UNSUPPORTED_SCHEMA_CONTRACT,
                    external_metadata_advanced, "unsupported schema contract");
        }
    }

    // Step 11: exactly one native invocation. Its typed error is saved code-first,
    // message copied and freed, and only the mapped codes become a result frame.
    int32_t native_result;
    if (is_drop) {
        native_result = lance_dataset_drop_index(dataset.get(), dispatch.index_name.c_str());
    } else {
        LanceVectorIndexParams native_params = {};
        native_params.index_type = LANCE_INDEX_IVF_PQ;
        native_params.metric = arguments.metric;
        native_params.num_partitions = arguments.num_partitions;
        native_params.num_sub_vectors = arguments.num_sub_vectors;
        native_params.num_bits = 8;
        native_result = lance_dataset_create_vector_index(
                dataset.get(), dispatch.column_name.c_str(), dispatch.index_name.c_str(),
                &native_params, /*replace=*/is_replace);
    }
    LanceErrorCode native_code = LANCE_OK;
    if (native_result != 0) {
        SavedLanceError saved = save_lance_error();
        native_code = saved.code;
    }
    std::optional<TLanceIndexJobResultCode::type> mapped =
            wire_result_code_for_native_error(native_code);
    if (!mapped.has_value()) {
        // DATASET_ALREADY_EXISTS, PANIC, or any unknown code: never inferred, no
        // result frame, nonzero exit.
        diag(params.diag_fd, DIAG_UNKNOWN_NATIVE_CODE);
        return 1;
    }
    const TLanceIndexJobResultCode::type result_code = *mapped;

    // Defensive completion of the envelope: the FE recomputes the completion
    // reason from ifExists and the typed code, the worker only mirrors it.
    bool if_condition_noop = is_drop && dispatch.__isset.if_exists && dispatch.if_exists &&
                             native_code == LANCE_ERR_NOT_FOUND;
    const char* static_category = native_code == LANCE_OK ? nullptr : "lance native error";
    // Fault-injection point: exit 0 having completed the native invocation but
    // without ever writing the result frame. The supervisor sees a
    // complete-exec-but-silent child and converges via the termination-proof
    // path (FE UNKNOWN).
    DBUG_EXECUTE_IF("LanceIndexWorker.skip_report", { return 0; });
    if (!write_result_frame(params, dispatch, result_code, if_condition_noop,
                            external_metadata_advanced, static_category)) {
        diag(params.diag_fd, DIAG_RESULT_WRITE_FAILED);
        return 1;
    }
    return 0;
}

std::optional<TLanceIndexJobResultCode::type> wire_result_code_for_native_error(
        LanceErrorCode code) {
    switch (code) {
    case LANCE_OK:
        return TLanceIndexJobResultCode::NATIVE_OK;
    case LANCE_ERR_COMMIT_CONFLICT:
        return TLanceIndexJobResultCode::NATIVE_COMMIT_CONFLICT;
    case LANCE_ERR_NOT_FOUND:
        return TLanceIndexJobResultCode::NATIVE_NOT_FOUND;
    case LANCE_ERR_INVALID_ARGUMENT:
        return TLanceIndexJobResultCode::NATIVE_INVALID_ARGUMENT;
    case LANCE_ERR_NOT_SUPPORTED:
        return TLanceIndexJobResultCode::NATIVE_NOT_SUPPORTED;
    case LANCE_ERR_INDEX:
        return TLanceIndexJobResultCode::NATIVE_INDEX;
    case LANCE_ERR_IO:
        return TLanceIndexJobResultCode::NATIVE_IO;
    case LANCE_ERR_INTERNAL:
        return TLanceIndexJobResultCode::NATIVE_INTERNAL;
    default:
        return std::nullopt;
    }
}

} // namespace doris::lance
