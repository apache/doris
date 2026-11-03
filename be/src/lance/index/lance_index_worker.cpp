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

#include "lance/index/lance_index_worker.h"

#include <lance/lance.h>
#include <rapidjson/document.h>

#include <algorithm>
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

#include "common/config.h"
#include "gen_cpp/LanceIndexWorker_constants.h"
#include "lance/index/lance_index_contract.h"
#include "lance/index/lance_index_worker_frame.h"
#include "util/debug_points.h"

namespace doris::lance {
namespace {

// Protocol bound of the handshake frame (the dispatch and result bounds are
// injectable through IndexWorkerParams; the handshake has a fixed layout).
constexpr uint32_t MAX_HANDSHAKE_BYTES = 4 * 1024;
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
constexpr const char* DIAG_UNTRUSTED_OUTCOME = "lance worker: no trusted mutation outcome\n";
constexpr const char* DIAG_RESULT_WRITE_FAILED = "lance worker: result write failed\n";
constexpr const char* DIAG_PARENT_GUARD = "lance worker: parent-death guard tripped\n";

// The static message categories of the result frame (never provider text).
constexpr const char* CATEGORY_STALE_ADMISSION = "stale admission";
constexpr const char* CATEGORY_UNSUPPORTED_CONTRACT = "unsupported schema contract";
constexpr const char* CATEGORY_RESOURCE_REJECTED = "resource rejected";
constexpr const char* CATEGORY_INDEX_UNRESOLVED = "index unresolved";
constexpr const char* CATEGORY_NATIVE_ERROR = "lance native error";
constexpr const char* CATEGORY_CREATE_NO_OP = "index exists; create if not exists no-op";
constexpr const char* CATEGORY_DROP_NO_OP = "index absent; drop if exists no-op";

// Bounded best-effort diagnostic write. Never carries dynamic content.
void diag(int fd, const char* message) {
    if (fd < 0 || message == nullptr) {
        return;
    }
    size_t size = std::strlen(message);
    ssize_t written = ::write(fd, message, size);
    (void)written;
}

// Byte-wise ASCII fold (A-Z to a-z); returns false on any non-ASCII byte. The
// same discipline the contract library applies: the FE normalizer is Java
// toLowerCase(Locale.ROOT), which is byte-reproducible only over ASCII, so
// non-ASCII spellings never case-fold here.
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
// first byte of network I/O: the worker is the last line of defense for values
// that cross a C-string boundary into lance-c. The options live only in this
// process's memory (they arrived inside the dispatch frame); they are never
// logged, echoed, or written anywhere.
bool storage_options_valid(const TLanceIndexWorkerDispatch& dispatch) {
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

// Mirrors the FE five-key whitelist validation (case-insensitive keys, no
// duplicates, IVF_PQ only, metric vocabulary, positive bounded integers,
// num_bits pinned to 8) and replays the admitted bounds snapshot (frame
// fields 16/17) rather than any live config - the worker subprocess runs
// before any BE config exists, which is exactly why the bounds ride the
// frame. A metric the request omitted defaults to lance's L2, exactly as the
// FE admission treats it.
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
    while (!text.empty() && (text.back() == '\n' || text.back() == '\r' || text.back() == ' ')) {
        text.pop_back();
    }
    if (text == "max") {
        return INT64_MAX;
    }
    if (text.empty()) {
        return -1;
    }
    uint64_t value = 0;
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
    if (!lance_worker_encode_compact(handshake, &payload) || payload.size() > MAX_HANDSHAKE_BYTES) {
        return false;
    }
    return lance_worker_write_frame(params.result_fd, payload);
}

// Composes and writes the single result frame. Only a trusted typed outcome
// ever reaches this point; the message slot carries at most a static category,
// never provider text. Returns false when no complete frame reached the pipe.
bool write_result_frame(const IndexWorkerParams& params,
                        TLanceIndexWorkerResultPhase::type phase, int32_t lance_error_code,
                        const char* static_category, int64_t observed_latest_version) {
    TLanceIndexWorkerResult result;
    result.phase = phase;
    result.lance_error_code = lance_error_code;
    if (static_category != nullptr) {
        result.__set_message(static_category);
    }
    if (observed_latest_version > 0) {
        result.__set_observed_latest_version(observed_latest_version);
    }
    std::vector<uint8_t> payload;
    if (!lance_worker_encode_compact(result, &payload) || payload.size() > params.max_result_bytes) {
        return false;
    }
    return lance_worker_write_frame(params.result_fd, payload);
}

// Trusted pre-invocation rejections: a complete frame with a typed negative
// code proves the invocation made no change, so a successfully written frame
// is a clean zero exit.
int reject_pre_invocation(const IndexWorkerParams& params, int32_t worker_rejected_code,
                          int64_t observed_latest_version, const char* static_category) {
    if (!write_result_frame(params, TLanceIndexWorkerResultPhase::PRE_INVOCATION_REJECTED,
                            worker_rejected_code, static_category, observed_latest_version)) {
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

// The static category of one negative rejection code. Every code the worker
// defines has exactly one category; an out-of-band value (a protocol
// violation in itself) maps to the resource category, never to provider text.
const char* rejection_category(int32_t worker_rejected_code) {
    if (worker_rejected_code == g_LanceIndexWorker_constants.LANCE_WORKER_REJECTED_STALE_ADMISSION) {
        return CATEGORY_STALE_ADMISSION;
    }
    if (worker_rejected_code ==
        g_LanceIndexWorker_constants.LANCE_WORKER_REJECTED_UNSUPPORTED_SCHEMA_CONTRACT) {
        return CATEGORY_UNSUPPORTED_CONTRACT;
    }
    if (worker_rejected_code ==
        g_LanceIndexWorker_constants.LANCE_WORKER_REJECTED_INDEX_UNRESOLVED) {
        return CATEGORY_INDEX_UNRESOLVED;
    }
    return CATEGORY_RESOURCE_REJECTED;
}

// The create spelling of one build: the display name when the dispatch
// carries one (preserving the statement's case), else the normalized name
// (the older-request form). Used by CREATE and by REPLACE's
// missing-name-new-create branch.
std::string create_spelling(const TLanceIndexWorkerDispatch& dispatch) {
    if (dispatch.__isset.index_display_name && !dispatch.index_display_name.empty()) {
        return dispatch.index_display_name;
    }
    return dispatch.index_name;
}

} // namespace

bool is_system_index_name(const std::string& name) {
    return name == "__lance_frag_reuse" || name == "__lance_mem_wal";
}

PhysicalIndexResolution resolve_physical_index(const std::string& index_list_json,
                                               const std::string& normalized_name,
                                               std::string* stored_spelling) {
    rapidjson::Document doc;
    // Iterative parse: the provider payload is external data; a hostile or
    // corrupt listing must be rejected by bound, never by stack overflow.
    doc.Parse<rapidjson::kParseIterativeFlag>(index_list_json.data(), index_list_json.size());
    if (doc.HasParseError() || !doc.IsArray()) {
        return PhysicalIndexResolution::MALFORMED;
    }
    if (doc.Size() > MAX_PHYSICAL_INDEX_ENTRIES) {
        return PhysicalIndexResolution::UNBOUNDED;
    }
    std::string folded_needle;
    const bool needle_foldable = ascii_fold(normalized_name, &folded_needle);
    std::optional<std::string> match;
    bool ambiguous = false;
    for (rapidjson::SizeType i = 0; i < doc.Size(); ++i) {
        if (!doc[i].IsObject() || !doc[i].HasMember("name") || !doc[i]["name"].IsString()) {
            return PhysicalIndexResolution::MALFORMED;
        }
        std::string entry(doc[i]["name"].GetString(), doc[i]["name"].GetStringLength());
        if (entry.empty() || entry.size() > MAX_INDEX_NAME_BYTES ||
            entry.find('\0') != std::string::npos) {
            return PhysicalIndexResolution::UNBOUNDED;
        }
        if (is_system_index_name(entry)) {
            continue;
        }
        std::string folded_entry;
        const bool foldable = ascii_fold(entry, &folded_entry);
        // Case-insensitive match over the ASCII fold; non-ASCII spellings
        // never fold (the FE normalizer is byte-reproducible only over ASCII)
        // and therefore match only by exact byte equality.
        const bool hit = foldable && needle_foldable ? folded_entry == folded_needle
                                                     : entry == normalized_name;
        if (!hit) {
            continue;
        }
        // Exact-duplicate spellings collapse to one candidate (the provider
        // list holds physical entries; one logical name can repeat).
        if (!match.has_value()) {
            match = std::move(entry);
        } else if (*match != entry) {
            ambiguous = true;
        }
    }
    if (ambiguous) {
        return PhysicalIndexResolution::AMBIGUOUS;
    }
    if (!match.has_value()) {
        return PhysicalIndexResolution::ABSENT;
    }
    if (stored_spelling != nullptr) {
        *stored_spelling = std::move(*match);
    }
    return PhysicalIndexResolution::UNIQUE;
}

WorkerMutationOutcome execute_worker_mutation(LanceDataset* pinned_dataset,
                                              uint64_t admitted_dataset_version,
                                              const TLanceIndexWorkerDispatch& dispatch,
                                              const VectorIndexArguments& build_arguments) {
    WorkerMutationOutcome outcome;
    const bool is_create = dispatch.mutation_type == TLanceIndexWorkerMutationType::CREATE;
    const bool is_replace = dispatch.mutation_type == TLanceIndexWorkerMutationType::REPLACE;
    const bool is_drop = dispatch.mutation_type == TLanceIndexWorkerMutationType::DROP;
    const bool is_build = is_create || is_replace;

    // Read-only latest observation, diagnostics only - it never gates the
    // invocation (no worker-start latest-equality gate; a concurrent append is
    // arbitrated by the provider's commit machinery). A failed observation is
    // 0, never an inference. The value rides ONLY pre-invocation rejections:
    // every other outcome leaves the field unset, exactly as the result frame
    // contract reserves it.
    int64_t observed_latest = 0;
    {
        uint64_t latest_version = lance_dataset_latest_version(pinned_dataset);
        if (latest_version != 0 && latest_version > admitted_dataset_version) {
            observed_latest = static_cast<int64_t>(latest_version);
        }
    }
    auto reject = [&](int32_t code) {
        outcome.trusted = true;
        outcome.phase = TLanceIndexWorkerResultPhase::PRE_INVOCATION_REJECTED;
        outcome.lance_error_code = code;
        outcome.observed_latest_version = observed_latest;
        return outcome;
    };

    // Contract revalidation, CREATE/REPLACE only. DROP admits no field-schema
    // contract: CREATE's single-column and algorithm restrictions never limit
    // what can be dropped, and a contract carried by a DROP dispatch is simply
    // not consumed. An absent or unparsable contract on a build is a safe
    // rejection, never a skip.
    SchemaContract admitted_contract;
    if (is_build) {
        if (!dispatch.__isset.schema_contract_json || dispatch.schema_contract_json.empty() ||
            parse_schema_contract(dispatch.schema_contract_json, &admitted_contract) !=
                    ContractStatus::OK) {
            return reject(
                    g_LanceIndexWorker_constants.LANCE_WORKER_REJECTED_UNSUPPORTED_SCHEMA_CONTRACT);
        }
        SchemaContract recomputed_contract;
        ContractStatus recompute_status =
                recompute_contract(pinned_dataset, dispatch.column_name, &recomputed_contract);
        if (recompute_status == ContractStatus::UNSUPPORTED) {
            return reject(
                    g_LanceIndexWorker_constants.LANCE_WORKER_REJECTED_UNSUPPORTED_SCHEMA_CONTRACT);
        }
        if (recompute_status == ContractStatus::RESOURCE_REJECTED) {
            return reject(g_LanceIndexWorker_constants.LANCE_WORKER_REJECTED_RESOURCE_REJECTED);
        }
        if (recompute_status != ContractStatus::OK) {
            return outcome; // no trusted typed fact, no result frame
        }
        if (!(admitted_contract == recomputed_contract)) {
            return reject(g_LanceIndexWorker_constants.LANCE_WORKER_REJECTED_STALE_ADMISSION);
        }
    }

    // The pinned snapshot's physical index list: the bounded basis of the
    // DROP unique resolution, the REPLACE replace-vs-new-create split, and
    // the pre-invocation IF no-ops. The typed error code is read FIRST; a
    // null listing with no typed failure is a provider contract violation.
    const char* raw_list = lance_dataset_index_list_json(pinned_dataset);
    if (raw_list == nullptr) {
        SavedLanceError list_error = save_lance_error();
        if (list_error.code == LANCE_OK) {
            return outcome; // untyped provider violation, no result frame
        }
        return reject(g_LanceIndexWorker_constants.LANCE_WORKER_REJECTED_RESOURCE_REJECTED);
    }
    std::string index_list(raw_list);
    lance_free_string(raw_list);

    std::string stored_spelling;
    const PhysicalIndexResolution resolution =
            resolve_physical_index(index_list, dispatch.index_name, &stored_spelling);

    // Pre-invocation IF no-ops, honored against the pinned version only.
    if (is_create && dispatch.__isset.if_not_exists && dispatch.if_not_exists &&
        (resolution == PhysicalIndexResolution::UNIQUE ||
         resolution == PhysicalIndexResolution::AMBIGUOUS)) {
        // The name is occupied in the admitted snapshot (an ambiguous pair is
        // occupied twice over): an authoritative no-op success.
        outcome.trusted = true;
        outcome.phase = TLanceIndexWorkerResultPhase::NATIVE_RETURN;
        outcome.lance_error_code = static_cast<int32_t>(LANCE_OK);
        outcome.no_op = true;
        return outcome;
    }
    if (is_drop && resolution == PhysicalIndexResolution::ABSENT && dispatch.__isset.if_exists &&
        dispatch.if_exists) {
        outcome.trusted = true;
        outcome.phase = TLanceIndexWorkerResultPhase::NATIVE_RETURN;
        outcome.lance_error_code = static_cast<int32_t>(LANCE_OK);
        outcome.no_op = true;
        return outcome;
    }

    // Resolution outcomes that refuse the invocation before any mutating
    // call. DROP requires a unique target: absent without if_exists is the
    // -5 rejection, ambiguous always is (the index exists, but no single
    // stored spelling can be trusted - dropping either guess could drop the
    // wrong physical entry, and one drop by name is all the invocation has).
    // REPLACE tolerates absence (new-create) and, per the internal IDL's -5
    // band, ambiguity only when the display-name fallback is not set.
    if (resolution == PhysicalIndexResolution::UNBOUNDED) {
        return reject(g_LanceIndexWorker_constants.LANCE_WORKER_REJECTED_RESOURCE_REJECTED);
    }
    if (resolution == PhysicalIndexResolution::MALFORMED) {
        return outcome; // untrusted provider listing, no result frame
    }
    if (resolution == PhysicalIndexResolution::ABSENT && is_drop) {
        return reject(g_LanceIndexWorker_constants.LANCE_WORKER_REJECTED_INDEX_UNRESOLVED);
    }
    if (resolution == PhysicalIndexResolution::AMBIGUOUS &&
        (is_drop || !dispatch.__isset.index_display_name)) {
        return reject(g_LanceIndexWorker_constants.LANCE_WORKER_REJECTED_INDEX_UNRESOLVED);
    }

    // Exactly one native invocation, by the resolved spelling:
    //   DROP      -> one drop by the uniquely resolved stored spelling;
    //   CREATE    -> one create by the display spelling (replace=false);
    //   REPLACE   -> one create by the stored spelling with replace=true, or
    //                by the display/normalized spelling with replace=false
    //                when the name is absent (missing-name-new-create).
    int32_t native_result;
    if (is_drop) {
        native_result = lance_dataset_drop_index(pinned_dataset, stored_spelling.c_str());
    } else {
        bool replace = false;
        std::string name;
        if (is_replace && resolution == PhysicalIndexResolution::UNIQUE) {
            name = stored_spelling;
            replace = true;
        } else {
            name = create_spelling(dispatch);
        }
        LanceVectorIndexParams native_params = {};
        native_params.index_type = LANCE_INDEX_IVF_PQ;
        native_params.metric = build_arguments.metric;
        native_params.num_partitions = build_arguments.num_partitions;
        native_params.num_sub_vectors = build_arguments.num_sub_vectors;
        native_params.num_bits = 8;
        native_result = lance_dataset_create_vector_index(
                pinned_dataset, dispatch.column_name.c_str(), name.c_str(), &native_params,
                replace);
    }
    LanceErrorCode native_code = LANCE_OK;
    if (native_result != 0) {
        SavedLanceError saved = save_lance_error();
        native_code = saved.code;
    }
    // The typed code passes through verbatim. A code outside the pinned ABI's
    // value set (negative garbage, out-of-range) is no trusted fact: no
    // result frame, nonzero exit - the handler synthesizes indeterminate.
    if (native_code < LANCE_OK || native_code > LANCE_ERR_PANIC) {
        return outcome;
    }
    outcome.trusted = true;
    outcome.phase = TLanceIndexWorkerResultPhase::NATIVE_RETURN;
    outcome.lance_error_code = static_cast<int32_t>(native_code);
    return outcome;
}

int run_index_worker(const IndexWorkerParams& params) {
    // Step 0: debug-point handoff across the exec boundary. The supervisor
    // snapshots its armed worker-fault points into the controlled environment
    // (WORKER_DEBUG_POINTS_ENV) because this process starts with a
    // default-off gate and an empty registry (the --lance-worker main branch
    // runs before any BE global state). Only the two known worker-fault names
    // are honored - anything else is ignored, and no byte of the value is
    // ever logged.
    if (const char* handoff = std::getenv(WORKER_DEBUG_POINTS_ENV)) {
        const std::string list(handoff, std::min(std::strlen(handoff), size_t {256}));
        bool registered = false;
        size_t pos = 0;
        while (pos <= list.size()) {
            const size_t comma = list.find(',', pos);
            const std::string token =
                    list.substr(pos, comma == std::string::npos ? comma : comma - pos);
            if (token == WORKER_DEBUG_POINT_HANG || token == WORKER_DEBUG_POINT_SKIP_RESULT) {
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
    // Re-arm the parent-death signal at the exec-side entry: a plain self-exec
    // preserves the setting across execve, but an execve into a binary with
    // file capabilities clears it. The getppid recheck closes the
    // arm-after-death race: a worker whose supervisor died before the re-arm
    // has already been reparented, and exits instead of running free. The
    // expected supervisor pid rides the supervisor's controlled env whitelist
    // (never inherited from the operator environment); an absent value (the
    // in-process unit tests) skips the recheck.
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

    // Step 2: the bounded dispatch frame. Without a decoded dispatch there is
    // no identity to frame a trusted result with, so a malformed frame is a
    // bare nonzero exit.
    std::vector<uint8_t> frame;
    if (!lance_worker_read_frame(params.dispatch_fd, params.max_dispatch_bytes, &frame)) {
        diag(params.diag_fd, DIAG_MALFORMED_FRAME);
        return 1;
    }
    TLanceIndexWorkerDispatch dispatch;
    if (!lance_worker_decode_compact(frame, &dispatch)) {
        diag(params.diag_fd, DIAG_UNDECODABLE_FRAME);
        return 1;
    }

    // Step 3: the handshake frame. It precedes EVERY result frame, including
    // the pre-invocation rejections below: the supervisor decodes the first
    // stdout frame as the handshake, so a result emitted before it would be a
    // protocol violation and the typed rejection would never reach the wire.
    // (A malformed dispatch above still exits bare — without a frame-decoded
    // dispatch there is no result path at all.)
    if (!write_handshake(params)) {
        diag(params.diag_fd, DIAG_HANDSHAKE_WRITE_FAILED);
        return 1;
    }

    // Step 4: dispatch revalidation. Every rejection from here on carries the
    // dispatch identity implicitly — the reply is matched to the request
    // structurally — in a complete pre-invocation result frame.
    if (!storage_options_valid(dispatch)) {
        return reject_pre_invocation(
                params, g_LanceIndexWorker_constants.LANCE_WORKER_REJECTED_RESOURCE_REJECTED,
                /*observed_latest=*/0, CATEGORY_RESOURCE_REJECTED);
    }
    const bool is_create = dispatch.mutation_type == TLanceIndexWorkerMutationType::CREATE;
    const bool is_replace = dispatch.mutation_type == TLanceIndexWorkerMutationType::REPLACE;
    const bool is_drop = dispatch.mutation_type == TLanceIndexWorkerMutationType::DROP;
    if (!is_create && !is_replace && !is_drop) {
        return reject_pre_invocation(
                params, g_LanceIndexWorker_constants.LANCE_WORKER_REJECTED_RESOURCE_REJECTED,
                /*observed_latest=*/0, CATEGORY_RESOURCE_REJECTED);
    }
    // Scalar sanity: the values that cross C boundaries or drive the pinned open.
    // admitted_dataset_version must be positive because version 0 is the latest
    // sentinel, which the pinned open never uses.
    if (dispatch.dataset_uri.empty() || dispatch.index_name.empty() ||
        dispatch.admitted_dataset_version <= 0 ||
        dispatch.dataset_uri.find('\0') != std::string::npos ||
        dispatch.index_name.find('\0') != std::string::npos ||
        dispatch.column_name.find('\0') != std::string::npos) {
        return reject_pre_invocation(
                params, g_LanceIndexWorker_constants.LANCE_WORKER_REJECTED_RESOURCE_REJECTED,
                /*observed_latest=*/0, CATEGORY_RESOURCE_REJECTED);
    }
    // The admitted bounds snapshot (frame fields 16/17) must be present and
    // positive: the worker subprocess reads no BE config, so a dispatch
    // without bounds is rejected safely rather than judged against any
    // hard-coded fallback.
    if (!dispatch.__isset.max_num_partitions || dispatch.max_num_partitions <= 0 ||
        !dispatch.__isset.max_num_sub_vectors || dispatch.max_num_sub_vectors <= 0) {
        return reject_pre_invocation(
                params,
                g_LanceIndexWorker_constants.LANCE_WORKER_REJECTED_UNSUPPORTED_SCHEMA_CONTRACT,
                /*observed_latest=*/0, CATEGORY_UNSUPPORTED_CONTRACT);
    }
    if (is_create || is_replace) {
        std::string folded_index_type;
        if (dispatch.column_name.empty() || !dispatch.__isset.properties_json ||
            dispatch.properties_json.empty() ||
            !ascii_fold(dispatch.index_type, &folded_index_type) ||
            folded_index_type != "ivf_pq") {
            return reject_pre_invocation(
                    params,
                    g_LanceIndexWorker_constants.LANCE_WORKER_REJECTED_UNSUPPORTED_SCHEMA_CONTRACT,
                    /*observed_latest=*/0, CATEGORY_UNSUPPORTED_CONTRACT);
        }
        VectorIndexArguments arguments;
        if (!validate_and_map_properties(dispatch.properties_json, dispatch.max_num_partitions,
                                         dispatch.max_num_sub_vectors, &arguments)) {
            return reject_pre_invocation(
                    params,
                    g_LanceIndexWorker_constants.LANCE_WORKER_REJECTED_UNSUPPORTED_SCHEMA_CONTRACT,
                    /*observed_latest=*/0, CATEGORY_UNSUPPORTED_CONTRACT);
        }
    }

    // Step 5: D11 — a local-filesystem dataset is rejected before any native
    // call, regardless of any FE-side assertion.
    if (is_local_dataset_uri(dispatch.dataset_uri)) {
        return reject_pre_invocation(
                params, g_LanceIndexWorker_constants.LANCE_WORKER_REJECTED_RESOURCE_REJECTED,
                /*observed_latest=*/0, CATEGORY_RESOURCE_REJECTED);
    }

    // Fault-injection point: hang after the dispatch frame is fully read and
    // validated, before the first lance FFI call (the supervisor's wall-clock
    // deadline then TERM->KILLs this process and the invocation converges
    // indeterminate). The block releases when the point is removed (the
    // in-process unit tests rely on that).
    DBUG_EXECUTE_IF(WORKER_DEBUG_POINT_HANG, DBUG_BLOCK);

    // Step 6: the pinned open, never version 0. The storage options live in
    // one owned buffer until process exit; only the pointer view reaches
    // lance-c, and no key or value is ever logged.
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
            // A bare NOT_FOUND cannot attribute "admitted version unavailable"
            // (a missing dataset URI reads the same), so observe latest
            // read-only: the mutation never moves to this handle, and it
            // closes immediately.
            std::unique_ptr<LanceDataset, LanceDatasetDeleter> observation(lance_dataset_open(
                    dispatch.dataset_uri.c_str(), storage_options, /*latest=*/0));
            if (observation != nullptr) {
                uint64_t observed_version = lance_dataset_version(observation.get());
                observation.reset();
                if (observed_version > admitted_version) {
                    // The admitted manifest was garbage-collected: attributable,
                    // with the advancement as diagnostic evidence.
                    return reject_pre_invocation(
                            params,
                            g_LanceIndexWorker_constants.LANCE_WORKER_REJECTED_STALE_ADMISSION,
                            static_cast<int64_t>(observed_version), CATEGORY_STALE_ADMISSION);
                }
                // A same-or-older latest contradicts the NOT_FOUND (race or
                // URI replacement): unattributable, keep the saved
                // pinned-open error.
                return reject_pre_invocation(
                        params, g_LanceIndexWorker_constants.LANCE_WORKER_REJECTED_RESOURCE_REJECTED,
                        /*observed_latest=*/0, CATEGORY_RESOURCE_REJECTED);
            }
            // The observation itself failed: dataset/URI/credential-layer
            // problem, unattributable, keep the saved pinned-open error.
            return reject_pre_invocation(
                    params, g_LanceIndexWorker_constants.LANCE_WORKER_REJECTED_RESOURCE_REJECTED,
                    /*observed_latest=*/0, CATEGORY_RESOURCE_REJECTED);
        }
        if (open_error.code != LANCE_OK) {
            // lance-c 0.1.9 produces no typed credential-expiry evidence, so
            // every other typed open failure is a resource rejection.
            return reject_pre_invocation(
                    params, g_LanceIndexWorker_constants.LANCE_WORKER_REJECTED_RESOURCE_REJECTED,
                    /*observed_latest=*/0, CATEGORY_RESOURCE_REJECTED);
        }
        diag(params.diag_fd, DIAG_UNTYPED_OPEN_FAILURE);
        return 1;
    }

    // Step 7: the version assertion. A mismatch means the open did not honor
    // the pin; there is no trusted result, so exit without a result frame.
    if (lance_dataset_version(dataset.get()) != admitted_version) {
        diag(params.diag_fd, DIAG_VERSION_ASSERTION);
        return 1;
    }

    // Step 8: everything after the pinned open — latest observation (diagnostics
    // only), contract revalidation for builds, physical-snapshot resolution
    // with the pre-invocation IF no-ops, and the single native call. The build
    // arguments were revalidated in step 4 (DROP validated none); the mutation
    // phase consumes the same values.
    VectorIndexArguments build_arguments;
    if (is_create || is_replace) {
        validate_and_map_properties(dispatch.properties_json, dispatch.max_num_partitions,
                                    dispatch.max_num_sub_vectors, &build_arguments);
    }
    WorkerMutationOutcome outcome =
            execute_worker_mutation(dataset.get(), admitted_version, dispatch, build_arguments);
    if (!outcome.trusted) {
        // No trusted typed fact exists (unparsable provider listing, untyped
        // recompute failure, or an out-of-band native code): no result frame.
        // Diagnostics alone are never a result.
        diag(params.diag_fd, DIAG_UNTRUSTED_OUTCOME);
        return 1;
    }

    // The authoritative pre-invocation no-op: an authoritative success.
    if (outcome.no_op) {
        const char* category = is_drop ? CATEGORY_DROP_NO_OP : CATEGORY_CREATE_NO_OP;
        if (!write_result_frame(params, outcome.phase, outcome.lance_error_code, category,
                                /*observed_latest=*/0)) {
            diag(params.diag_fd, DIAG_RESULT_WRITE_FAILED);
            return 1;
        }
        return 0;
    }

    // Fault-injection point: exit 0 having completed the native invocation but
    // without ever writing the result frame (a complete-exec-but-silent
    // child). Honored only on the NATIVE_RETURN path so a pre-invocation
    // rejection frame is never suppressed - the point's phase is "after the
    // mutating call", exactly as in the closed series.
    if (outcome.phase == TLanceIndexWorkerResultPhase::NATIVE_RETURN) {
        DBUG_EXECUTE_IF(WORKER_DEBUG_POINT_SKIP_RESULT, { return 0; });
    }

    const char* category = nullptr;
    if (outcome.phase == TLanceIndexWorkerResultPhase::PRE_INVOCATION_REJECTED) {
        category = rejection_category(outcome.lance_error_code);
    } else if (outcome.lance_error_code != static_cast<int32_t>(LANCE_OK)) {
        category = CATEGORY_NATIVE_ERROR;
    }
    if (!write_result_frame(params, outcome.phase, outcome.lance_error_code, category,
                            outcome.observed_latest_version)) {
        diag(params.diag_fd, DIAG_RESULT_WRITE_FAILED);
        return 1;
    }
    return 0;
}

} // namespace doris::lance
