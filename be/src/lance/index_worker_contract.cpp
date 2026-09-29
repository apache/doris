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

#include "lance/index_worker_contract.h"

#include <rapidjson/document.h>

#include <chrono>
#include <cstring>
#include <memory>
#include <utility>

namespace doris::lance {
namespace {

// Arrow C Data Interface flag bits (the lance header intentionally does not name
// them): field-nullable and map-keys-sorted.
constexpr int64_t ARROW_FLAG_NULLABLE = 2;
constexpr int64_t ARROW_FLAG_MAP_KEYS_SORTED = 4;

// P1c fail-closed caps, evaluated on the cheap manifest-side fragment count before
// the expensive per-fragment statistics scan, plus a wall-clock sub-budget on that
// scan itself. The two fragment caps are mutable ONLY through
// force_recompute_fragment_caps_for_test so unit tests can reach the rejection
// with small values; production never touches them.
uint64_t g_max_stats_fragments = RECOMPUTE_MAX_STATS_FRAGMENTS;
uint64_t g_max_stats_fragment_field_product = RECOMPUTE_MAX_STATS_FRAGMENT_FIELD_PRODUCT;
constexpr int64_t MAX_RECOMPUTE_WALL_CLOCK_SECONDS = 60;

// Maps a failed lance FFI call to a contract status: the typed code makes it a
// resource-layer rejection, while a provider contract violation (failure without a
// typed code) yields NO_TRUSTED.
ContractStatus typed_failure_status() {
    SavedLanceError error = save_lance_error();
    return error.code == LANCE_OK ? ContractStatus::NO_TRUSTED
                                  : ContractStatus::RESOURCE_REJECTED;
}
// Strictly parses a decimal unsigned integer: digits only, at least one, no sign,
// no whitespace, value within int64.
bool parse_uint64(const char* data, size_t size, int64_t* value) {
    if (size == 0) {
        return false;
    }
    int64_t result = 0;
    for (size_t i = 0; i < size; ++i) {
        if (data[i] < '0' || data[i] > '9') {
            return false;
        }
        int digit = data[i] - '0';
        if (result > (INT64_MAX - digit) / 10) {
            return false;
        }
        result = result * 10 + digit;
    }
    *value = result;
    return true;
}

// Strictly parses a decimal signed integer component (optional single leading '-').
bool parse_int64_component(const char* data, size_t size, int64_t* value) {
    if (size == 0) {
        return false;
    }
    if (data[0] == '-') {
        int64_t magnitude = 0;
        if (!parse_uint64(data + 1, size - 1, &magnitude)) {
            return false;
        }
        *value = -magnitude;
        return true;
    }
    return parse_uint64(data, size, value);
}

// Splits `format` on the first ':' into (head, rest-after-colon). Returns false
// when no colon is present.
bool split_at_colon(const std::string& format, std::string* head, std::string* tail) {
    size_t colon = format.find(':');
    if (colon == std::string::npos) {
        return false;
    }
    *head = format.substr(0, colon);
    *tail = format.substr(colon + 1);
    return true;
}

bool is_safe_timezone(const std::string& tz) {
    // Mirrors LanceSchemaContractBuilder.SAFE_TIMEZONE ([A-Za-z0-9+_/-]+).
    if (tz.empty()) {
        return false;
    }
    for (char c : tz) {
        bool safe = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
                    c == '+' || c == '_' || c == '/' || c == '-';
        if (!safe) {
            return false;
        }
    }
    return true;
}

// The dimension of a "+w:N" fixed-size-list format, or nullopt when the format is
// not a well-formed positive-dimension fixed-size list.
std::optional<int64_t> fixed_size_list_dimension(const std::string& format) {
    std::string head;
    std::string tail;
    if (!split_at_colon(format, &head, &tail) || head != "+w") {
        return std::nullopt;
    }
    int64_t dimension = 0;
    if (!parse_uint64(tail.data(), tail.size(), &dimension) || dimension <= 0 ||
        dimension > INT32_MAX) {
        return std::nullopt;
    }
    return dimension;
}

bool is_fixed_size_list(const std::string& format) {
    return fixed_size_list_dimension(format).has_value();
}

// Byte-wise ASCII fold: A-Z becomes a-z, every other byte passes through. Returns
// false when the input contains a non-ASCII byte (the Java ROOT fold is not
// byte-reproducible there, so callers fail closed instead of risking a divergent
// fold).
bool ascii_fold(const char* data, size_t size, std::string* out) {
    bool pure_ascii = true;
    std::string folded;
    folded.reserve(size);
    for (size_t i = 0; i < size; ++i) {
        unsigned char c = static_cast<unsigned char>(data[i]);
        if (c >= 0x80) {
            pure_ascii = false;
        }
        folded.push_back(c >= 'A' && c <= 'Z' ? static_cast<char>(c - 'A' + 'a')
                                              : static_cast<char>(c));
    }
    *out = std::move(folded);
    return pure_ascii;
}

struct ArrowSchemaReleaser {
    void operator()(ArrowSchema* schema) const {
        if (schema != nullptr && schema->release != nullptr) {
            // The exporter's callback recursively releases the whole subtree and
            // then clears the struct; calling it exactly once is the entire duty.
            schema->release(schema);
        }
    }
};

struct LanceDataStatisticsDeleter {
    void operator()(LanceDataStatistics* stats) const {
        if (stats != nullptr) {
            lance_data_statistics_close(stats);
        }
    }
};

// One node of the manifest-DFS walk: DFS pre-order over the exported ArrowSchema
// tree, skipping the single item child of every fixed-size-list node (the item has
// no manifest field and no statistics entry — the proven binding model).
struct WalkNode {
    const ArrowSchema* schema = nullptr;
    // Index of the parent inside the walk vector, or -1 for a top-level field.
    int64_t parent_index = -1;
    bool top_level = false;
};

struct PendingNode {
    const ArrowSchema* schema = nullptr;
    int64_t parent_index = -1;
    bool top_level = false;
};

} // namespace

// Maps an Arrow C Data Interface format string to the canonical normalized-type
// vocabulary of the FE LanceSchemaContractBuilder, byte-exactly. Returns nullopt
// for any format outside the proven type set (scalar leaves, struct, list, large
// list, map, fixed-size list): the binding model is unproven there and the caller
// fails closed. The generic-fallback forms replicate the Java
// "<arrow class name lowercased>(param=value,...)" rendering for the shapes an
// Arrow format string can express. Defined outside the anonymous namespace as a
// unit-test seam (see the header); production reaches it via recompute_contract.
std::optional<std::string> canonical_type_for_format(const char* raw_format, int64_t flags) {
    if (raw_format == nullptr) {
        return std::nullopt;
    }
    const std::string format(raw_format);
    if (format == "b") return std::string("bool");
    if (format == "c") return std::string("int<8>");
    if (format == "s") return std::string("int<16>");
    if (format == "i") return std::string("int<32>");
    if (format == "l") return std::string("int<64>");
    if (format == "C") return std::string("uint<8>");
    if (format == "S") return std::string("uint<16>");
    if (format == "I") return std::string("uint<32>");
    if (format == "L") return std::string("uint<64>");
    if (format == "e") return std::string("float16");
    if (format == "f") return std::string("float32");
    if (format == "g") return std::string("float64");
    if (format == "u") return std::string("utf8");
    if (format == "U") return std::string("large_utf8");
    if (format == "z") return std::string("binary()");
    if (format == "Z") return std::string("largebinary()");
    if (format == "tdD") return std::string("date<day>");
    if (format == "tdm") return std::string("date<ms>");
    if (format == "+l") return std::string("list()");
    if (format == "+L") return std::string("largelist()");
    if (format == "+s") return std::string("struct()");
    if (format == "+m") {
        return std::string("map(keysSorted=") +
               ((flags & ARROW_FLAG_MAP_KEYS_SORTED) != 0 ? "true" : "false") + ")";
    }
    if (format == "n") return std::string("null()");
    if (format == "tts") return std::string("time(unit=SECOND,bitWidth=32)");
    if (format == "ttm") return std::string("time(unit=MILLISECOND,bitWidth=32)");
    if (format == "ttu") return std::string("time(unit=MICROSECOND,bitWidth=64)");
    if (format == "ttn") return std::string("time(unit=NANOSECOND,bitWidth=64)");
    if (format == "tDs") return std::string("duration(unit=SECOND)");
    if (format == "tDm") return std::string("duration(unit=MILLISECOND)");
    if (format == "tDu") return std::string("duration(unit=MICROSECOND)");
    if (format == "tDn") return std::string("duration(unit=NANOSECOND)");
    if (is_fixed_size_list(format)) {
        // The fixed-size list canonical form is the pinned literal; the dimension
        // and element facts live in their own contract slots.
        return std::string("fixed_size_list");
    }
    if (format.size() > 2 && format[0] == 'w' && format[1] == ':') {
        int64_t byte_width = 0;
        if (parse_uint64(format.data() + 2, format.size() - 2, &byte_width)) {
            return "fixedsizebinary(byteWidth=" + std::to_string(byte_width) + ")";
        }
        return std::nullopt;
    }
    if (format.size() > 2 && format[0] == 'd' && format[1] == ':') {
        // d:precision,scale[,bitWidth]; the bit width defaults to 128.
        std::string body = format.substr(2);
        size_t first = body.find(',');
        if (first == std::string::npos) {
            return std::nullopt;
        }
        size_t second = body.find(',', first + 1);
        int64_t precision = 0;
        int64_t scale = 0;
        int64_t bit_width = 128;
        if (!parse_int64_component(body.data(), first, &precision)) {
            return std::nullopt;
        }
        if (second == std::string::npos) {
            if (!parse_int64_component(body.data() + first + 1, body.size() - first - 1, &scale)) {
                return std::nullopt;
            }
        } else {
            if (!parse_int64_component(body.data() + first + 1, second - first - 1, &scale) ||
                !parse_int64_component(body.data() + second + 1, body.size() - second - 1,
                                       &bit_width)) {
                return std::nullopt;
            }
        }
        return "decimal<" + std::to_string(bit_width) + ">(" + std::to_string(precision) + "," +
               std::to_string(scale) + ")";
    }
    if (format.size() >= 4 && format[0] == 't' && format[1] == 's' && format[3] == ':') {
        const char unit = format[2];
        const char* canonical_unit = nullptr;
        const char* java_unit = nullptr;
        switch (unit) {
        case 's':
            canonical_unit = "sec";
            java_unit = "SECOND";
            break;
        case 'm':
            canonical_unit = "ms";
            java_unit = "MILLISECOND";
            break;
        case 'u':
            canonical_unit = "us";
            java_unit = "MICROSECOND";
            break;
        case 'n':
            canonical_unit = "ns";
            java_unit = "NANOSECOND";
            break;
        default:
            return std::nullopt;
        }
        std::string tz = format.substr(4);
        if (tz.empty()) {
            // The manifest records a timezone-less timestamp with the "-"
            // placeholder: arrow-rs normalizes it to the empty string on export,
            // while the FE builder copies the placeholder verbatim from the JNI
            // view (empirical, locked by the timestamp_sec_nozone golden fixture).
            // The canonical form must restore the placeholder or every no-tz
            // timestamp contract would read as a false STALE.
            tz = "-";
        }
        if (is_safe_timezone(tz)) {
            return std::string("timestamp<") + canonical_unit + ",tz=\"" + tz + "\">";
        }
        return std::string("timestamp(unit=") + java_unit + ",timezone=" + tz + ")";
    }
    return std::nullopt;
}

SavedLanceError save_lance_error() {
    SavedLanceError saved;
    // Code first: it is the typed fact. The message is copied and freed immediately;
    // it is kept only as evidence and never inspected or forwarded.
    saved.code = lance_last_error_code();
    const char* raw_message = lance_last_error_message();
    if (raw_message != nullptr) {
        saved.message.assign(raw_message);
        lance_free_string(raw_message);
    }
    return saved;
}

ContractStatus parse_schema_contract(const std::string& json, SchemaContract* out) {
    if (out == nullptr) {
        return ContractStatus::NO_TRUSTED;
    }
    if (json.empty()) {
        // A pre-contract-persistence record (or a corrupt dispatch) carries the
        // empty string; it is rejected safely rather than skipped.
        return ContractStatus::UNSUPPORTED;
    }
    rapidjson::Document doc;
    doc.Parse(json.data(), json.size());
    if (doc.HasParseError() || !doc.IsObject()) {
        return ContractStatus::UNSUPPORTED;
    }
    auto scv = doc.FindMember("scv");
    if (scv == doc.MemberEnd() || !scv->value.IsInt() ||
        scv->value.GetInt() != SchemaContract::SCHEMA_CONTRACT_VERSION_V1) {
        return ContractStatus::UNSUPPORTED;
    }
    auto flds = doc.FindMember("flds");
    if (flds == doc.MemberEnd() || !flds->value.IsArray() ||
        flds->value.Size() > SchemaContract::MAX_INDEXED_FIELDS) {
        return ContractStatus::UNSUPPORTED;
    }
    SchemaContract parsed;
    parsed.scv = SchemaContract::SCHEMA_CONTRACT_VERSION_V1;
    for (const auto& element : flds->value.GetArray()) {
        if (!element.IsObject()) {
            return ContractStatus::UNSUPPORTED;
        }
        ContractField field;
        auto fid = element.FindMember("fid");
        if (fid == element.MemberEnd() || !fid->value.IsInt64() || fid->value.GetInt64() < 0) {
            return ContractStatus::UNSUPPORTED;
        }
        field.fid = fid->value.GetInt64();
        auto nn = element.FindMember("nn");
        if (nn == element.MemberEnd() || !nn->value.IsString() ||
            nn->value.GetStringLength() == 0 ||
            nn->value.GetStringLength() > SchemaContract::MAX_FIELD_STRING_BYTES) {
            return ContractStatus::UNSUPPORTED;
        }
        field.nn.assign(nn->value.GetString(), nn->value.GetStringLength());
        auto nt = element.FindMember("nt");
        if (nt == element.MemberEnd() || !nt->value.IsString() ||
            nt->value.GetStringLength() == 0 ||
            nt->value.GetStringLength() > SchemaContract::MAX_FIELD_STRING_BYTES) {
            return ContractStatus::UNSUPPORTED;
        }
        field.nt.assign(nt->value.GetString(), nt->value.GetStringLength());
        auto nul = element.FindMember("nul");
        if (nul == element.MemberEnd() || !nul->value.IsBool()) {
            return ContractStatus::UNSUPPORTED;
        }
        field.nul = nul->value.GetBool();
        // The three vector slots are absent from the JSON when null (Gson does not
        // serialize nulls); tolerate the absence and reject a malformed presence.
        auto fsd = element.FindMember("fsd");
        if (fsd != element.MemberEnd()) {
            if (!fsd->value.IsInt() || fsd->value.GetInt() <= 0) {
                return ContractStatus::UNSUPPORTED;
            }
            field.fsd = fsd->value.GetInt();
        }
        auto vet = element.FindMember("vet");
        if (vet != element.MemberEnd()) {
            if (!vet->value.IsString() || vet->value.GetStringLength() == 0 ||
                vet->value.GetStringLength() > SchemaContract::MAX_FIELD_STRING_BYTES) {
                return ContractStatus::UNSUPPORTED;
            }
            field.vet = std::string(vet->value.GetString(), vet->value.GetStringLength());
        }
        auto ven = element.FindMember("ven");
        if (ven != element.MemberEnd()) {
            if (!ven->value.IsBool()) {
                return ContractStatus::UNSUPPORTED;
            }
            field.ven = ven->value.GetBool();
        }
        parsed.flds.push_back(std::move(field));
    }
    *out = std::move(parsed);
    return ContractStatus::OK;
}

ContractStatus recompute_contract(LanceDataset* dataset, const std::string& column_name,
                                  SchemaContract* out) {
    if (dataset == nullptr || out == nullptr) {
        return ContractStatus::NO_TRUSTED;
    }
    // 1. Export the schema of the pinned snapshot.
    ArrowSchema root = {};
    if (lance_dataset_schema(dataset, &root) != 0) {
        return typed_failure_status();
    }
    std::unique_ptr<ArrowSchema, ArrowSchemaReleaser> root_guard(&root);

    // 2. The exported root is the struct container whose children are the top-level
    // manifest fields; anything else is outside the proven shape.
    if (root.format == nullptr || std::strcmp(root.format, "+s") != 0 ||
        root.dictionary != nullptr || root.n_children < 0 ||
        (root.n_children > 0 && root.children == nullptr)) {
        return ContractStatus::UNSUPPORTED;
    }

    // 3. P1c fail-closed caps on the cheap manifest-side fragment count, before the
    // expensive per-fragment statistics scan. A zero return can mean an empty
    // dataset or a failed call; a failed count is caught by the statistics call
    // below failing typed on the same handle, so zero proceeds.
    uint64_t fragment_count = lance_dataset_fragment_count(dataset);
    uint64_t top_level_count = static_cast<uint64_t>(root.n_children);
    // fragments * top_level_fields > 32768, evaluated overflow-safely.
    if (fragment_count > g_max_stats_fragments ||
        top_level_count > g_max_stats_fragment_field_product ||
        (top_level_count != 0 &&
         fragment_count > g_max_stats_fragment_field_product / top_level_count)) {
        return ContractStatus::UNSUPPORTED;
    }

    // 4. The field-id source: statistics entries ordered by schema field id, one
    // per manifest field (never a fixed-size-list item). Wall-clock sub-budget: a
    // scan that overruns the budget is rejected fail-closed after the fact (the
    // supervisor wall-clock remains the outer bound).
    auto stats_begin = std::chrono::steady_clock::now();
    std::unique_ptr<LanceDataStatistics, LanceDataStatisticsDeleter> stats(
            lance_dataset_calculate_data_stats(dataset));
    auto stats_elapsed = std::chrono::steady_clock::now() - stats_begin;
    if (stats == nullptr) {
        return typed_failure_status();
    }
    if (std::chrono::duration_cast<std::chrono::seconds>(stats_elapsed).count() >
        MAX_RECOMPUTE_WALL_CLOCK_SECONDS) {
        return ContractStatus::UNSUPPORTED;
    }
    uint64_t stats_count = lance_data_statistics_count(stats.get());

    // 5. The manifest-DFS walk: DFS pre-order, skipping the single item child of
    // every fixed-size-list node. Per-node guardrails ride the same pass.
    std::vector<WalkNode> nodes;
    std::vector<PendingNode> pending;
    pending.reserve(static_cast<size_t>(root.n_children));
    for (int64_t i = root.n_children - 1; i >= 0; --i) {
        pending.push_back({root.children[i], -1, true});
    }
    while (!pending.empty()) {
        PendingNode entry = pending.back();
        pending.pop_back();
        const ArrowSchema* schema = entry.schema;
        if (schema == nullptr || schema->format == nullptr || schema->dictionary != nullptr ||
            schema->n_children < 0 || (schema->n_children > 0 && schema->children == nullptr)) {
            return ContractStatus::UNSUPPORTED;
        }
        if (!canonical_type_for_format(schema->format, schema->flags).has_value()) {
            return ContractStatus::UNSUPPORTED;
        }
        int64_t node_index = static_cast<int64_t>(nodes.size());
        nodes.push_back({schema, entry.parent_index, entry.top_level});
        if (is_fixed_size_list(schema->format)) {
            // The item child is folded into the manifest logical string: it owns no
            // field id and no statistics entry, so the walk skips it — but its shape
            // is still validated fail-closed (proven items are plain float leaves).
            if (schema->n_children != 1 || schema->children[0] == nullptr) {
                return ContractStatus::UNSUPPORTED;
            }
            const ArrowSchema* item = schema->children[0];
            if (item->format == nullptr || item->dictionary != nullptr ||
                item->n_children != 0 ||
                !canonical_type_for_format(item->format, item->flags).has_value()) {
                return ContractStatus::UNSUPPORTED;
            }
            continue;
        }
        for (int64_t i = schema->n_children - 1; i >= 0; --i) {
            pending.push_back({schema->children[i], node_index, false});
        }
    }

    // 6. The zip guardrails: one statistics entry per walked node, ids strictly
    // ascending, top-level ids strictly increasing, child id greater than parent id.
    if (stats_count != static_cast<uint64_t>(nodes.size())) {
        return ContractStatus::UNSUPPORTED;
    }
    std::vector<uint32_t> fids(nodes.size());
    bool have_top_level = false;
    uint32_t last_top_level_fid = 0;
    for (size_t i = 0; i < nodes.size(); ++i) {
        uint32_t fid = lance_data_statistics_field_id_at(stats.get(), i);
        if (i > 0 && fid <= fids[i - 1]) {
            return ContractStatus::UNSUPPORTED;
        }
        fids[i] = fid;
        const WalkNode& node = nodes[i];
        if (node.parent_index >= 0 && fid <= fids[static_cast<size_t>(node.parent_index)]) {
            return ContractStatus::UNSUPPORTED;
        }
        if (node.top_level) {
            if (have_top_level && fid <= last_top_level_fid) {
                return ContractStatus::UNSUPPORTED;
            }
            have_top_level = true;
            last_top_level_fid = fid;
        }
    }

    // 7. Column resolution: exactly one top-level field matches the stored column
    // name byte-exactly, and no second top-level field collides under the ASCII
    // fold (the FE admits case-duplicate datasets only through fail-closed paths,
    // so a fold collision means state outside the supported matrix).
    std::string folded_column;
    bool column_pure_ascii = ascii_fold(column_name.data(), column_name.size(), &folded_column);
    int64_t match_index = -1;
    size_t fold_matches = 0;
    for (size_t i = 0; i < nodes.size(); ++i) {
        if (!nodes[i].top_level) {
            continue;
        }
        const char* name = nodes[i].schema->name;
        if (name == nullptr) {
            return ContractStatus::UNSUPPORTED;
        }
        size_t name_size = std::strlen(name);
        if (name_size == column_name.size() &&
            std::memcmp(name, column_name.data(), name_size) == 0) {
            if (match_index >= 0) {
                return ContractStatus::UNSUPPORTED;
            }
            match_index = static_cast<int64_t>(i);
        }
        if (column_pure_ascii) {
            std::string folded_name;
            ascii_fold(name, name_size, &folded_name);
            if (folded_name == folded_column) {
                ++fold_matches;
            }
        }
    }
    if (match_index < 0 || fold_matches > 1) {
        return ContractStatus::UNSUPPORTED;
    }
    const WalkNode& indexed = nodes[static_cast<size_t>(match_index)];
    const ArrowSchema* field_schema = indexed.schema;

    // 8. The seven slots. fid rides the zip; nn is the ASCII fold (a non-ASCII name
    // is not byte-reproducible against the Java ROOT fold and fails closed); nt is
    // the canonical form validated in the walk; nul is the nullable flag bit.
    ContractField field;
    field.fid = fids[static_cast<size_t>(match_index)];
    if (!ascii_fold(field_schema->name, std::strlen(field_schema->name), &field.nn)) {
        return ContractStatus::UNSUPPORTED;
    }
    auto canonical = canonical_type_for_format(field_schema->format, field_schema->flags);
    if (!canonical.has_value()) {
        return ContractStatus::UNSUPPORTED;
    }
    field.nt = std::move(*canonical);
    field.nul = (field_schema->flags & ARROW_FLAG_NULLABLE) != 0;
    if (is_fixed_size_list(field_schema->format)) {
        auto dimension = fixed_size_list_dimension(field_schema->format);
        if (!dimension.has_value()) {
            return ContractStatus::UNSUPPORTED;
        }
        field.fsd = static_cast<int32_t>(*dimension);
        const ArrowSchema* item = field_schema->children[0];
        auto element = canonical_type_for_format(item->format, item->flags);
        if (!element.has_value()) {
            return ContractStatus::UNSUPPORTED;
        }
        // The worker product matrix only admits float16/float32 vector elements;
        // uint8/int8 are refused even though lance supports them natively.
        if (*element != "float16" && *element != "float32") {
            return ContractStatus::UNSUPPORTED;
        }
        field.vet = std::move(*element);
        // Copied as exported: lance-c always reports the element nullable, which is
        // exactly what the FE synthesizes from the same slot-less manifest (proven).
        field.ven = (item->flags & ARROW_FLAG_NULLABLE) != 0;
    }

    out->scv = SchemaContract::SCHEMA_CONTRACT_VERSION_V1;
    out->flds.clear();
    out->flds.push_back(std::move(field));
    return ContractStatus::OK;
}

bool vector_index_shape_supported(const ContractField& field, uint32_t num_sub_vectors) {
    // The worker product matrix only admits float16/float32 vector elements;
    // uint8/int8 are refused even though lance supports them natively.
    bool shape_supported = !field.nul && field.nt == "fixed_size_list" && field.fsd.has_value() &&
                           *field.fsd > 0 && field.vet.has_value() &&
                           (*field.vet == "float16" || *field.vet == "float32");
    return shape_supported && num_sub_vectors != 0 &&
           *field.fsd % static_cast<int32_t>(num_sub_vectors) == 0;
}

void force_recompute_fragment_caps_for_test(uint64_t max_stats_fragments,
                                            uint64_t max_stats_fragment_field_product) {
    g_max_stats_fragments = max_stats_fragments;
    g_max_stats_fragment_field_product = max_stats_fragment_field_product;
}

} // namespace doris::lance
