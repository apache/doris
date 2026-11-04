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

#include "format_v2/table/fluss_union_lake_reader.h"

#include <algorithm>
#include <array>
#include <charconv>
#include <memory>
#include <mutex>
#include <numeric>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/cast_set.h"
#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "exprs/vexpr_context.h"
#include "exprs/vslot_ref.h"
#include "format/format_common.h"
#include "format_v2/column_mapper.h"
#include "format_v2/expr/equality_delete_predicate.h"
#include "format_v2/jni/fluss_jni_reader.h"
#include "format_v2/table/fluss_range_type.h"
#include "format_v2/table/paimon_reader.h"
#include "runtime/descriptors.h"
#include "runtime/file_scan_profile.h"
#include "runtime/memory/lru_cache_policy.h"
#include "runtime/runtime_state.h"
#include "util/defer_op.h"
#include "util/url_coding.h"

namespace doris::format::fluss {
namespace {

// The scan-node properties this reader reads. The fluss connector states that `fluss.union.*` is the
// whole of what BE's C++ side knows about fluss; anything added here has to be added there too.
constexpr const char* PROP_PK_NAMES = "fluss.union.pk_names_base64";
constexpr const char* PROP_PK_NAMES_LEGACY = "fluss.union.pk_names";
constexpr const char* PROP_MAX_TAIL_ROWS = "fluss.union.max_tail_rows";

// The per-range payload of a wrapped lake split: its kind (PROP_RANGE_TYPE, one of the two lake
// kinds in fluss_range_type.h) and, for a suppressed one, the log tail that supersedes part of it.
constexpr const char* PROP_TAIL = "fluss.union.tail";

// The range this reader synthesizes to read the tail: a plain bounded log read (RANGE_TYPE_LOG),
// which is what the suppression set is -- every key the tail touched, whatever it ended up saying
// about it.
constexpr const char* PROP_PARTITION_ID = "fluss.partition_id";
constexpr const char* PROP_BUCKET_ID = "fluss.bucket_id";
constexpr const char* PROP_LOG_START_OFFSET = "fluss.log_start_offset";
constexpr const char* PROP_LOG_STOP_OFFSET = "fluss.log_stop_offset";

constexpr size_t TAIL_BATCH_ROWS = 4096;

// Marks the rows of one block of lake keys that the tail's sorted keys name, by walking both in
// ascending order: a comparison or two per row instead of a hash lookup. A primary-key table's lake files are
// sorted by key, so a block read from one ascends; one that does not is left unmarked and false is returned, so
// the caller falls back to its hash set. A lake that jumps ahead (a filter, a sparse file) is caught up by
// galloping rather than by stepping through every key in between.
template <typename T>
bool union_walk_sorted_keys(const std::vector<int64_t>& sorted, const T* data, size_t rows,
                            UInt8* keep, int64_t* dropped) {
    if (rows == 0) {
        return true;
    }
    if (!std::is_sorted(data, data + rows)) {
        return false;
    }
    const auto begin = sorted.begin();
    const size_t size = sorted.size();
    size_t at = std::lower_bound(begin, sorted.end(), static_cast<int64_t>(data[0])) - begin;
    for (size_t row = 0; row < rows; ++row) {
        const auto value = static_cast<int64_t>(data[row]);
        if (at < size && sorted[at] < value) {
            size_t step = 1;
            while (at + step < size && sorted[at + step] < value) {
                at += step;
                step <<= 1;
            }
            at = std::lower_bound(begin + at, begin + std::min(size, at + step + 1), value) - begin;
        }
        const bool replaced = at < size && sorted[at] == value;
        keep[row] = !replaced;
        *dropped += replaced;
    }
    return true;
}

// The class a key column packs as, or TYPE_NULL when it cannot be packed. A packed value is
// the bytes the value is stored as, so a type qualifies only when two of its values are equal exactly
// when those bytes are: not floating point (0.0 and -0.0 compare equal), not the legacy date and
// decimal types. The string types pack alike, as the generic predicate compares them as bytes.
PrimitiveType union_packing_type(PrimitiveType type) {
    switch (type) {
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
    case TYPE_LARGEINT:
    case TYPE_DATEV2:
    case TYPE_DATETIMEV2:
    case TYPE_TIMESTAMPTZ:
    case TYPE_DECIMAL32:
    case TYPE_DECIMAL64:
    case TYPE_DECIMAL128I:
    case TYPE_DECIMAL256:
        return type;
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_STRING:
    case TYPE_VARBINARY:
        return TYPE_STRING;
    default:
        return TYPE_NULL;
    }
}

// One key column as packing reads it.
struct UnionPackingColumn {
    // Keeps a constant column, expanded to one value per row, alive while the block is probed.
    ColumnPtr holder;
    const IColumn* data = nullptr;
    const UInt8* nulls = nullptr;
};

// The key columns at `positions`, with the class each packs as; false when one cannot be packed.
bool union_packing_columns(const Block& block, const std::vector<size_t>& positions,
                           std::vector<PrimitiveType>* types,
                           std::vector<UnionPackingColumn>* columns) {
    types->clear();
    columns->clear();
    for (const auto position : positions) {
        const auto& entry = block.get_by_position(position);
        const auto type = union_packing_type(remove_nullable(entry.type)->get_primitive_type());
        if (type == TYPE_NULL) {
            return false;
        }
        UnionPackingColumn column;
        column.holder = entry.column->convert_to_full_column_if_const();
        column.data = column.holder.get();
        if (const auto* nullable = check_and_get_column<ColumnNullable>(*column.data)) {
            column.nulls = nullable->get_null_map_data().data();
            column.data = &nullable->get_nested_column();
        }
        types->push_back(type);
        columns->push_back(std::move(column));
    }
    return true;
}

// One key appended as bytes. Per column a null byte, then the value's length and bytes; the
// length keeps ("ab", "c") and ("a", "bc") apart.
void union_pack_key(const std::vector<UnionPackingColumn>& columns, size_t row, std::string* out) {
    for (const auto& column : columns) {
        if (column.nulls != nullptr && column.nulls[row] != 0) {
            out->push_back('\1');
            continue;
        }
        out->push_back('\0');
        const auto value = column.data->get_data_at(row);
        const auto size = cast_set<uint32_t>(value.size);
        out->append(reinterpret_cast<const char*>(&size), sizeof(size));
        out->append(value.data, value.size);
    }
}

// Immutable, versioned tails may outlive a query. Use the normal byte-bounded BE cache so
// retained columns are charged to the cache tracker and memory GC can reclaim idle entries.
class ProcessTailCache final : public LRUCachePolicy {
public:
    using Keys = FlussUnionLakeReader::SuppressionKeys;

    static ProcessTailCache& instance() {
        static ProcessTailCache cache;
        return cache;
    }

    Status get(const std::string& key, const std::function<Status(Keys*)>& load,
               std::shared_ptr<const Keys>* result, bool* hit) {
        // A stripe serializes concurrent misses without holding the LRU lock across network I/O.
        std::lock_guard<std::mutex> guard(
                _loading[std::hash<std::string>()(key) % _loading.size()]);
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(mem_tracker());
        auto* handle = lookup(key);
        *hit = handle != nullptr;
        if (handle == nullptr) {
            auto value = std::make_unique<Value>();
            value->tracker = mem_tracker();
            value->keys = std::make_unique<Keys>();
            RETURN_IF_ERROR(load(value->keys.get()));
            const auto& keys = *value->keys;
            size_t extra_bytes = sizeof(Value) + sizeof(Keys) + keys.hash_index->memory_usage();
            if (keys.int_keys != nullptr) {
                extra_bytes += keys.int_keys->capacity() * (sizeof(int64_t) + 1);
                extra_bytes += keys.sorted_int_keys->capacity() * sizeof(int64_t);
            }
            if (keys.packed_keys != nullptr) {
                extra_bytes += keys.packed_keys->bytes.capacity();
                extra_bytes += keys.packed_keys->keys.capacity() * (sizeof(std::string_view) + 1);
                extra_bytes += keys.packed_keys->types.capacity() * sizeof(PrimitiveType);
            }
            // Column buffers use the Doris allocator; account for the STL/hash storage separately.
            const size_t charge = keys.keys.allocated_bytes() + extra_bytes + key.size();
            handle = insert(key, value.get(), charge, extra_bytes);
            value.release();
        }
        const auto* value = static_cast<const Value*>(this->value(handle));
        // The lease pins the entire entry, even if GC evicts it while a scan is using its key views.
        *result = std::shared_ptr<const Keys>(value->keys.get(), [this, handle](const Keys*) {
            SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(mem_tracker());
            release(handle);
        });
        return Status::OK();
    }

private:
    struct Value final : LRUCacheValueBase {
        ~Value() override {
            SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(tracker);
            keys.reset();
        }
        std::shared_ptr<MemTrackerLimiter> tracker;
        std::unique_ptr<Keys> keys;
    };

    ProcessTailCache()
            : LRUCachePolicy(CacheType::FLUSS_UNION_TAIL_CACHE, 64 * 1024 * 1024,
                             LRUCacheType::SIZE, 300, 1, 256, true, false) {}

    std::array<std::mutex, 32> _loading;
};

std::vector<std::string_view> split_on(std::string_view value, char separator) {
    std::vector<std::string_view> parts;
    size_t start = 0;
    while (true) {
        const auto end = value.find(separator, start);
        if (end == std::string_view::npos) {
            parts.push_back(value.substr(start));
            return parts;
        }
        parts.push_back(value.substr(start, end - start));
        start = end + 1;
    }
}

template <typename T>
bool parse_integer(std::string_view text, T* value) {
    if (text.empty()) {
        return false;
    }
    const auto result = std::from_chars(text.data(), text.data() + text.size(), *value);
    return result.ec == std::errc() && result.ptr == text.data() + text.size();
}

} // namespace

Status FlussUnionLakeReader::parse_tail(const std::string& spec, Tail* tail) {
    DORIS_CHECK(tail != nullptr);
    const auto parts = split_on(spec, ':');
    if (parts.size() != 4) {
        return Status::InternalError(
                "fluss union read: '{}' is not a log tail of the form "
                "partitionId:bucket:start:stop",
                spec);
    }
    // An unpartitioned table leaves the partition segment empty rather than writing a sentinel, so
    // that its bucket 0 and a partitioned table's bucket 0 cannot become the same cache entry.
    if (!parts[0].empty()) {
        int64_t partition_id = 0;
        if (!parse_integer(parts[0], &partition_id)) {
            return Status::InternalError(
                    "fluss union read: '{}' has a partition id that is not a "
                    "number in log tail '{}'",
                    parts[0], spec);
        }
    }
    Tail parsed;
    parsed.partition_id = std::string(parts[0]);
    if (!parse_integer(parts[1], &parsed.bucket_id) ||
        !parse_integer(parts[2], &parsed.start_offset) ||
        !parse_integer(parts[3], &parsed.stop_offset)) {
        return Status::InternalError(
                "fluss union read: log tail '{}' has a bucket or offset that is not a number",
                spec);
    }
    if (parsed.start_offset >= parsed.stop_offset) {
        // Planning never wraps a lake split whose bucket has nothing left in its log. One arriving
        // here means the two halves of this read were bounded by different offsets, and that is a
        // duplicated or a missing row either way.
        return Status::InternalError(
                "fluss union read: a suppressing log tail must contain something, but bucket {} "
                "was "
                "given [{}, {})",
                parsed.bucket_id, parsed.start_offset, parsed.stop_offset);
    }
    parsed.spec = spec;
    *tail = std::move(parsed);
    return Status::OK();
}

TFileRangeDesc FlussUnionLakeReader::tail_scan_range(const Tail& tail) {
    std::map<std::string, std::string> params {
            {PROP_RANGE_TYPE, RANGE_TYPE_LOG},
            {PROP_BUCKET_ID, std::to_string(tail.bucket_id)},
            {PROP_LOG_START_OFFSET, std::to_string(tail.start_offset)},
            {PROP_LOG_STOP_OFFSET, std::to_string(tail.stop_offset)},
    };
    if (!tail.partition_id.empty()) {
        // Absent rather than -1 on an unpartitioned table: that is how the scanner tells the two
        // apart, and fluss subscribes to a bucket of each by a different call.
        params.emplace(PROP_PARTITION_ID, tail.partition_id);
    }
    TTableFormatFileDesc table_format_params;
    table_format_params.__set_table_format_type("fluss");
    table_format_params.__set_fluss_params(std::move(params));
    TFileRangeDesc range;
    range.__set_table_format_params(std::move(table_format_params));
    range.__set_format_type(TFileFormatType::FORMAT_JNI);
    return range;
}

Status FlussUnionLakeReader::init(format::TableReadOptions&& options) {
    RETURN_IF_ERROR(format::TableReader::init(std::move(options)));
    RETURN_IF_ERROR(_resolve_union_properties());
    _init_union_profile();

    VExprContextSPtrs conjuncts;
    conjuncts.reserve(_conjuncts.size());
    for (const auto& conjunct : _conjuncts) {
        VExprSPtr root;
        RETURN_IF_ERROR(format::clone_table_expr_tree(conjunct->root(), &root));
        conjuncts.push_back(VExprContext::create_shared(std::move(root)));
    }
    _lake_reader = std::make_unique<format::paimon::PaimonHybridReader>();
    RETURN_IF_ERROR(_lake_reader->init({
            .projected_columns = _projected_columns,
            .conjuncts = std::move(conjuncts),
            .format = _format,
            .scan_params = _scan_params,
            .io_ctx = _io_ctx,
            .runtime_state = _runtime_state,
            .scanner_profile = _scanner_profile,
            .parquet_profile_cache = _parquet_profile_cache,
            .file_slot_descs = _file_slot_descs,
            // Aggregate pushdown is withheld from the lake half on purpose. A COUNT answered from
            // paimon's own file metadata would count the very rows this reader is about to suppress,
            // and it would do so without ever producing a block to suppress them from.
            .push_down_agg_type = TPushAggOp::type::NONE,
            .condition_cache_digest = _condition_cache_digest,
    }));
    if (_batch_size > 0) {
        _lake_reader->set_batch_size(_batch_size);
    }
    return Status::OK();
}

Status FlussUnionLakeReader::_resolve_union_properties() {
    if (_scan_params == nullptr || !_scan_params->__isset.fluss_properties) {
        return Status::InternalError(
                "missing fluss_properties for a fluss union read, possibly caused by FE/BE "
                "protocol "
                "mismatch");
    }
    const auto& properties = _scan_params->fluss_properties;
    const auto encoded_names_it = properties.find(PROP_PK_NAMES);
    const auto legacy_names_it = properties.find(PROP_PK_NAMES_LEGACY);
    if ((encoded_names_it == properties.end() || encoded_names_it->second.empty()) &&
        (legacy_names_it == properties.end() || legacy_names_it->second.empty())) {
        // FE states the union properties only when it plans a primary-key union, and this reader
        // also serves the plain LAKE splits of a log-table union, which have no keys and nothing to
        // suppress. Their absence is therefore not an error here; a LAKE_SUPPRESS split arriving
        // anyway fails loud in _prepare_suppression.
        return Status::OK();
    }
    const bool encoded = encoded_names_it != properties.end() && !encoded_names_it->second.empty();
    const auto& names = encoded ? encoded_names_it->second : legacy_names_it->second;
    for (const auto token : split_on(names, ',')) {
        std::string name;
        if (encoded) {
            if (token.empty() || token.front() != '$') {
                return Status::InternalError(
                        "fluss union read: '{}' contains an unencoded key name", PROP_PK_NAMES);
            }
            const std::string base64_name(token.substr(1));
            if (!base64_name.empty() && !base64_decode(base64_name, &name)) {
                return Status::InternalError("fluss union read: '{}' contains invalid Base64",
                                             PROP_PK_NAMES);
            }
        } else {
            // During a BE-first upgrade, the old FE still sends comma-separated plain names.
            name = token;
        }
        const auto column = std::ranges::find_if(
                _projected_columns,
                [&](const format::ColumnDefinition& candidate) { return candidate.name == name; });
        if (column == _projected_columns.end()) {
            // FE keeps the key columns in the scan's tuple whenever it plans a union read, so this
            // means its planning-time decision and its split-planning decision disagreed. Suppressing
            // nothing would return every superseded lake row a second time, silently.
            return Status::InternalError(
                    "fluss union read: key column '{}' is not among the columns this scan "
                    "projects. "
                    "The lake rows its log tail supersedes cannot be identified without it",
                    name);
        }
        _key_column_indexes.push_back(
                cast_set<size_t>(std::distance(_projected_columns.begin(), column)));
        _key_columns.push_back(*column);
    }

    const auto rows_it = properties.find(PROP_MAX_TAIL_ROWS);
    if (rows_it == properties.end() ||
        !parse_integer(std::string_view(rows_it->second), &_max_tail_rows) || _max_tail_rows <= 0) {
        return Status::InternalError(
                "fluss union read: '{}' must be a positive number of rows, but was '{}'",
                PROP_MAX_TAIL_ROWS,
                rows_it == properties.end() ? std::string("missing") : rows_it->second);
    }
    return Status::OK();
}

void FlussUnionLakeReader::_init_union_profile() {
    if (_scanner_profile == nullptr) {
        return;
    }
    static const char* table_profile = file_scan_profile::TABLE_READER;
    _suppressed_rows_counter = ADD_CHILD_COUNTER_WITH_LEVEL(
            _scanner_profile, "FlussUnionSuppressedRows", TUnit::UNIT, table_profile, 1);
    _tail_keys_read_counter = ADD_CHILD_COUNTER_WITH_LEVEL(
            _scanner_profile, "FlussUnionTailKeysRead", TUnit::UNIT, table_profile, 1);
    // Change-log records read against distinct keys retained: the second is what the scan holds for its
    // whole life, once per bucket per partition, so the two apart are what says whether it is bounded.
    _tail_keys_retained_counter = ADD_CHILD_COUNTER_WITH_LEVEL(
            _scanner_profile, "FlussUnionTailKeysRetained", TUnit::UNIT, table_profile, 1);
    _tail_cache_hit_counter = ADD_CHILD_COUNTER_WITH_LEVEL(
            _scanner_profile, "FlussUnionTailCacheHitCount", TUnit::UNIT, table_profile, 1);
    _tail_read_timer = ADD_CHILD_TIMER_WITH_LEVEL(_scanner_profile, "FlussUnionTailReadTime",
                                                  table_profile, 1);
    _suppress_timer = ADD_CHILD_TIMER_WITH_LEVEL(_scanner_profile, "FlussUnionSuppressTime",
                                                 table_profile, 1);
}

Status FlussUnionLakeReader::prepare_split(const format::SplitReadOptions& options) {
    DORIS_CHECK(_lake_reader != nullptr);
    // Reset before anything can fail or return early, so a split can never be filtered by what the
    // previous one prepared.
    _current_split_suppressed = false;
    RETURN_IF_ERROR(_lake_reader->prepare_split(options));
    if (_lake_reader->current_split_pruned()) {
        // A pruned split returns no rows, so there is nothing to suppress and no reason to spend a
        // read of the tail on it.
        return Status::OK();
    }
    return _prepare_suppression(options);
}

Status FlussUnionLakeReader::refresh_conjuncts(VExprContextSPtrs conjuncts) {
    RETURN_IF_ERROR(format::TableReader::refresh_conjuncts(std::move(conjuncts)));
    if (_lake_reader == nullptr) {
        return Status::OK();
    }
    VExprContextSPtrs child_conjuncts;
    RETURN_IF_ERROR(_clone_conjuncts(&child_conjuncts));
    return _lake_reader->refresh_conjuncts(std::move(child_conjuncts));
}

Status FlussUnionLakeReader::_prepare_suppression(const format::SplitReadOptions& options) {
    const auto& range = options.current_range;
    if (!range.__isset.table_format_params || !range.table_format_params.__isset.fluss_params) {
        return Status::InternalError(
                "missing fluss_params on a fluss union lake split, possibly caused by FE/BE "
                "protocol "
                "mismatch");
    }
    const auto& params = range.table_format_params.fluss_params;
    const auto type_it = params.find(PROP_RANGE_TYPE);
    if (type_it == params.end() ||
        (type_it->second != RANGE_TYPE_LAKE && type_it->second != RANGE_TYPE_LAKE_SUPPRESS)) {
        return Status::InternalError(
                "a fluss lake split must carry '{}={}' or '{}={}', but carries '{}'",
                PROP_RANGE_TYPE, RANGE_TYPE_LAKE, PROP_RANGE_TYPE, RANGE_TYPE_LAKE_SUPPRESS,
                type_it == params.end() ? std::string("nothing") : type_it->second);
    }
    if (type_it->second == RANGE_TYPE_LAKE) {
        // Nothing in this split's bucket supersedes it: read exactly as the sibling planned it.
        return Status::OK();
    }
    if (_key_columns.empty()) {
        // The scan never stated its key columns, so this split's superseded rows cannot be
        // identified. Reading it anyway would return every one of them a second time.
        return Status::InternalError(
                "missing '{}' for a fluss union read: a lake split carries the log tail '{}' but "
                "the scan does not say which key columns to suppress by",
                PROP_PK_NAMES, params.count(PROP_TAIL) ? params.at(PROP_TAIL) : std::string());
    }
    const auto tail_it = params.find(PROP_TAIL);
    if (tail_it == params.end()) {
        return Status::InternalError("missing '{}' on a fluss union lake split", PROP_TAIL);
    }
    if (_suppression != nullptr && _suppression_tail_spec == tail_it->second) {
        // Consecutive splits of one bucket are common; their suppression is the same one.
        _current_split_suppressed = true;
        return Status::OK();
    }
    Tail tail;
    RETURN_IF_ERROR(parse_tail(tail_it->second, &tail));
    RETURN_IF_ERROR(_load_suppression_keys(options, tail));
    _suppression_tail_spec = tail_it->second;
    _current_split_suppressed = true;
    return Status::OK();
}

std::string FlussUnionLakeReader::_tail_cache_key(const Tail& tail) const {
    std::string key;
    const auto append = [&](const std::string& part) {
        key += std::to_string(part.size()) + ":" + part;
    };
    // Include credentials, table/schema ids, limits and timezone. A recreated table or another
    // catalog must not reuse keys merely because its name and log offsets happen to match.
    append(std::to_string(_scan_params->fluss_properties.size()));
    for (const auto& [name, value] : _scan_params->fluss_properties) {
        append(name);
        append(value);
    }
    append(std::to_string(_key_columns.size()));
    for (const auto& column : _key_columns) {
        append(column.name);
        append(column.type->get_name());
    }
    append(_runtime_state == nullptr ? "UTC" : _runtime_state->timezone());
    append(tail.spec);
    return key;
}

std::shared_ptr<const phmap::flat_hash_set<int64_t>> FlussUnionLakeReader::build_int_keys(
        const Block& keys) {
    if (keys.columns() != 1) {
        return nullptr;
    }
    const IColumn* column = keys.get_by_position(0).column.get();
    const UInt8* nulls = nullptr;
    if (const auto* nullable = check_and_get_column<ColumnNullable>(*column)) {
        // The generic/packed path also matches NULL keys; an integer set has no NULL sentinel.
        if (nullable->has_null()) {
            return nullptr;
        }
        nulls = nullable->get_null_map_data().data();
        column = &nullable->get_nested_column();
    }
    auto set = std::make_shared<phmap::flat_hash_set<int64_t>>();
    const auto fill = [&](const auto& data) {
        set->reserve(data.size());
        for (size_t row = 0; row < data.size(); ++row) {
            if (nulls == nullptr || nulls[row] == 0) {
                set->insert(static_cast<int64_t>(data[row]));
            }
        }
    };
    if (const auto* longs = check_and_get_column<ColumnInt64>(*column)) {
        fill(longs->get_data());
        return set;
    }
    if (const auto* ints = check_and_get_column<ColumnInt32>(*column)) {
        fill(ints->get_data());
        return set;
    }
    return nullptr;
}

std::shared_ptr<const FlussUnionLakeReader::PackedKeys> FlussUnionLakeReader::build_packed_keys(
        const Block& keys) {
    std::vector<size_t> positions(keys.columns());
    std::iota(positions.begin(), positions.end(), 0);
    auto packed = std::make_shared<PackedKeys>();
    std::vector<UnionPackingColumn> columns;
    if (!union_packing_columns(keys, positions, &packed->types, &columns)) {
        return nullptr;
    }
    const auto rows = keys.rows();
    std::vector<size_t> ends;
    ends.reserve(rows);
    for (size_t row = 0; row < rows; ++row) {
        union_pack_key(columns, row, &packed->bytes);
        ends.push_back(packed->bytes.size());
    }
    // Views are taken only once the storage has stopped growing.
    packed->keys.reserve(rows);
    size_t begin = 0;
    for (const auto end : ends) {
        packed->keys.emplace(packed->bytes.data() + begin, end - begin);
        begin = end;
    }
    return packed;
}

Status FlussUnionLakeReader::_prepare_suppression_keys(const Tail& tail, SuppressionKeys* loaded) {
    RETURN_IF_ERROR(_read_tail_keys(tail, loaded));
    loaded->int_keys = build_int_keys(loaded->keys);
    if (loaded->int_keys != nullptr) {
        auto sorted = std::make_shared<std::vector<int64_t>>(loaded->int_keys->begin(),
                                                             loaded->int_keys->end());
        std::sort(sorted->begin(), sorted->end());
        loaded->sorted_int_keys = std::move(sorted);
    } else {
        loaded->packed_keys = build_packed_keys(loaded->keys);
    }
    update_counter(_tail_keys_read_counter, loaded->records);
    return Status::OK();
}

Status FlussUnionLakeReader::_load_suppression_keys(const format::SplitReadOptions& options,
                                                    const Tail& tail) {
    // An older FE does not send stable table identity. Its cache remains scan-scoped: a TTL
    // alone cannot distinguish a dropped/recreated table with the same name and offsets.
    const auto& props = _scan_params->fluss_properties;
    if (props.contains("fluss.table_id") && props.contains("fluss.schema_id")) {
        bool cache_hit = false;
        std::shared_ptr<const SuppressionKeys> keys;
        RETURN_IF_ERROR(ProcessTailCache::instance().get(
                _tail_cache_key(tail),
                [&](SuppressionKeys* loaded) { return _prepare_suppression_keys(tail, loaded); },
                &keys, &cache_hit));
        // Replace the predicate's column references before releasing the old cache lease.
        RETURN_IF_ERROR(_build_suppression_predicate(*keys));
        _current_keys = std::move(keys);
        _fast_keys = _current_keys->int_keys.get();
        _sorted_keys = _current_keys->sorted_int_keys.get();
        _packed_keys = _current_keys->packed_keys.get();
        if (cache_hit) {
            update_counter(_tail_cache_hit_counter, 1);
        } else {
            update_counter(_tail_keys_retained_counter,
                           cast_set<int64_t>(_current_keys->keys.rows()));
        }
        return Status::OK();
    }
    if (options.cache == nullptr) {
        return Status::InternalError(
                "fluss union read: no split cache to hold the keys of log tail '{}'", tail.spec);
    }
    const auto cache_key = _tail_cache_key(tail);
    Status read_status = Status::OK();
    bool cache_hit = false;
    auto* cached = options.cache->get<SuppressionKeys>(
            cache_key,
            [&]() -> SuppressionKeys* {
                auto loaded = std::make_unique<SuppressionKeys>();
                read_status = _prepare_suppression_keys(tail, loaded.get());
                if (!read_status.ok()) {
                    return nullptr;
                }
                return loaded.release();
            },
            &cache_hit);
    RETURN_IF_ERROR(read_status);
    DORIS_CHECK(cached != nullptr);
    if (!cache_hit) {
        update_counter(_tail_keys_retained_counter, cast_set<int64_t>(cached->keys.rows()));
    }
    if (cache_hit) {
        update_counter(_tail_cache_hit_counter, 1);
    }
    RETURN_IF_ERROR(_build_suppression_predicate(*cached));
    _fast_keys = cached->int_keys.get();
    _sorted_keys = cached->sorted_int_keys.get();
    _packed_keys = cached->packed_keys.get();
    return Status::OK();
}

Block FlussUnionLakeReader::_empty_key_block() const {
    Block block;
    for (const auto& column : _key_columns) {
        block.insert({column.type->create_column(), column.type, column.name});
    }
    return block;
}

Status FlussUnionLakeReader::_accumulate_tail_keys(const Tail& tail, const NextBatch& next_batch,
                                                   SuppressionKeys* keys) {
    auto builder = MutableBlock::build_mutable_block(_empty_key_block());
    Block batch = _empty_key_block();
    while (true) {
        bool eos = false;
        batch.clear_column_data(cast_set<int64_t>(_key_columns.size()));
        RETURN_IF_ERROR(next_batch(&batch, &eos));
        if (batch.rows() > 0) {
            RETURN_IF_ERROR(builder.merge(batch));
            if (cast_set<int64_t>(builder.rows()) > _max_tail_rows) {
                // The same yardstick and the same limit the Java side applies while replaying this
                // very range: change log records read, not distinct keys. Both halves of one table
                // therefore exceed it together, so which of them reports it is not a coin toss.
                return Status::InternalError(
                        "the log tail of bucket {} holds more than {} change log records, the "
                        "limit "
                        "set by 'fluss.union_read.max_tail_rows'. Read the table as pure fluss "
                        "with "
                        "'fluss.union_read.mode=disabled', wait for tiering to move the tail into "
                        "the lake, or raise the limit",
                        tail.bucket_id, _max_tail_rows);
            }
        }
        if (eos) {
            // Deduplicated only here, never during accumulation: the limit above counts change-log
            // records, which is the yardstick the Java side applies to this very range, and a limit
            // that counted distinct keys instead would let the two halves of one table disagree about
            // whether the table can be read at all.
            keys->records = cast_set<int64_t>(builder.rows());
            keys->keys = EqualityDeletePredicate::distinct_rows(builder.to_block());
            keys->hash_index = EqualityDeletePredicate::build_hash_index(keys->keys);
            return Status::OK();
        }
    }
}

Status FlussUnionLakeReader::_read_tail_keys(const Tail& tail, SuppressionKeys* keys) {
    DORIS_CHECK(keys != nullptr);
    // Only the reads reach here; a bucket whose keys the scan's cache already holds does not, which
    // is what makes this timer the answer to "how much did the tails themselves cost".
    SCOPED_TIMER(_tail_read_timer);
#ifdef BE_TEST
    if (_test_tail_reader) {
        // Stands in for the network read alone. Everything built on it - the accumulation, the
        // limit, the cache - is the same code the real read goes through.
        Block canned;
        RETURN_IF_ERROR(_test_tail_reader(tail, &canned));
        bool delivered = false;
        return _accumulate_tail_keys(
                tail,
                [&](Block* batch, bool* eos) {
                    if (!delivered) {
                        *batch = canned;
                        delivered = true;
                    }
                    *eos = true;
                    return Status::OK();
                },
                keys);
    }
#endif
    // The tail is read as an ordinary bounded log range, projected down to the key columns. Every
    // record counts, including the deletions: a lake row whose key the tail deleted must disappear
    // just as surely as one it updated, and what the tail ended up saying is contributed by its own
    // range, not by this read.
    FlussJniReader reader;
    // On the scan's own profile, deliberately: this read therefore also lands in the FlussJniScanner
    // node beside the log-side ranges, so the Java-side breakdown there - how much of it was fluss's
    // own scan against how much was moving the rows across JNI - covers the tail read as well. The
    // consequence to read the profile by: FlussJniScanner's total exceeds FlussLogReadTime by
    // roughly the tail reads, which FlussUnionTailReadTime states on its own.
    RETURN_IF_ERROR(reader.init({
            .projected_columns = _key_columns,
            .conjuncts = {},
            .format = format::FileFormat::JNI,
            .scan_params = _scan_params,
            .io_ctx = _io_ctx,
            .runtime_state = _runtime_state,
            .scanner_profile = _scanner_profile,
            .parquet_profile_cache = _parquet_profile_cache,
    }));
    // Not the adaptive size the lake half is being read at: this is a bounded pass over key columns
    // only, and its batches have nothing to do with how wide the lake's rows turned out to be.
    reader.set_batch_size(_runtime_state == nullptr ? TAIL_BATCH_ROWS
                                                    : _runtime_state->batch_size());

    format::SplitReadOptions tail_options;
    tail_options.conjuncts = VExprContextSPtrs {};
    tail_options.condition_cache_digest = 0;
    tail_options.current_range = tail_scan_range(tail);
    tail_options.current_split_format = format::FileFormat::JNI;

    const auto drain = [&]() -> Status {
        RETURN_IF_ERROR(reader.prepare_split(tail_options));
        return _accumulate_tail_keys(
                tail, [&](Block* batch, bool* eos) { return reader.get_block(batch, eos); }, keys);
    };
    Status status;
    Status close_status = Status::OK();
    {
        // Status-returning failures and C++ exceptions both pass through this scope. A leaked fluss
        // connection keeps its netty and metadata-updater threads alive for the life of the BE.
        Defer close_reader {[&]() { close_status = reader.close(); }};
        status = drain();
    }
    RETURN_IF_ERROR(status);
    return close_status;
}

Status FlussUnionLakeReader::_build_suppression_predicate(const SuppressionKeys& keys) {
    std::vector<int> key_positions;
    key_positions.reserve(_key_column_indexes.size());
    for (const auto index : _key_column_indexes) {
        key_positions.push_back(cast_set<int>(index));
    }
    DORIS_CHECK(keys.keys.columns() == key_positions.size());
    DORIS_CHECK(keys.hash_index != nullptr);
    auto predicate =
            std::make_shared<EqualityDeletePredicate>(keys.keys, key_positions, keys.hash_index);
    for (size_t i = 0; i < _key_column_indexes.size(); ++i) {
        const auto position = key_positions[i];
        // The block this runs against is the table-schema block the lake half returns, so a key
        // column is simply the projected column at its own position - already mapped, already of the
        // projected type, and therefore comparable with the tail's column of the same name.
        predicate->add_child(VSlotRef::create_shared(position, position, -1, _key_columns[i].type,
                                                     _key_columns[i].name));
    }
    auto suppression = VExprContext::create_shared(std::move(predicate));
    RowDescriptor row_desc;
    RETURN_IF_ERROR(suppression->prepare(_runtime_state, row_desc));
    RETURN_IF_ERROR(suppression->open(_runtime_state));
    // Publish only a fully opened predicate. If preparation ever gains a reachable failure mode, the
    // previous tail remains paired with its own predicate instead of being silently mislabeled.
    _suppression = std::move(suppression);
    return Status::OK();
}

Status FlussUnionLakeReader::_clone_conjuncts(VExprContextSPtrs* conjuncts) const {
    DORIS_CHECK(conjuncts != nullptr);
    conjuncts->clear();
    conjuncts->reserve(_conjuncts.size());
    for (const auto& conjunct : _conjuncts) {
        VExprSPtr root;
        RETURN_IF_ERROR(format::clone_table_expr_tree(conjunct->root(), &root));
        conjuncts->push_back(VExprContext::create_shared(std::move(root)));
    }
    return Status::OK();
}

Status FlussUnionLakeReader::get_block(Block* block, bool* eos) {
    DORIS_CHECK(_lake_reader != nullptr);
    RETURN_IF_ERROR(_lake_reader->get_block(block, eos));
    return _suppress(block);
}

Status FlussUnionLakeReader::_suppress(Block* block) {
    DORIS_CHECK(block != nullptr);
    if (!_current_split_suppressed) {
        // A plain LAKE split: nothing supersedes it, so nothing may filter it - least of all a
        // predicate a neighbouring bucket's suppressed split left cached.
        return Status::OK();
    }
    if (_suppression == nullptr) {
        // A suppressing lake split names the tail that supersedes part of it. Reading one without
        // having built that suppression would return the superseded rows a second time.
        return Status::InternalError(
                "fluss union read: a lake split was read with no log tail bound to it");
    }
    const auto rows = block->rows();
    if (rows == 0) {
        return Status::OK();
    }
    if (_fast_keys != nullptr) {
        bool handled = false;
        RETURN_IF_ERROR(_suppress_fast(block, &handled));
        if (handled) {
            return Status::OK();
        }
    }
    if (_packed_keys != nullptr) {
        bool handled = false;
        RETURN_IF_ERROR(_suppress_packed(block, &handled));
        if (handled) {
            return Status::OK();
        }
    }
    SCOPED_TIMER(_profile.finalize_timer);
    // Nested inside the established finalize timer rather than taken out of it: this is a detail of
    // what finalizing a block of this table costs, so FinalizeBlockTime keeps meaning what it did.
    SCOPED_TIMER(_suppress_timer);
    int result_column_id = -1;
    RETURN_IF_ERROR(_suppression->root()->execute(_suppression.get(), block, &result_column_id));
    DORIS_CHECK(result_column_id >= 0 && result_column_id < cast_set<int>(block->columns()));
    const auto& suppressed =
            assert_cast<const ColumnUInt8&>(*block->get_by_position(result_column_id).column)
                    .get_data();
    DORIS_CHECK(suppressed.size() == rows);
    IColumn::Filter keep(rows, 1);
    int64_t dropped = 0;
    for (size_t row = 0; row < rows; ++row) {
        keep[row] = suppressed[row] == 0;
        dropped += suppressed[row] != 0 ? 1 : 0;
    }
    block->erase(result_column_id);
    if (dropped == 0) {
        return Status::OK();
    }
    RETURN_IF_CATCH_EXCEPTION(Block::filter_block_internal(block, keep));
    update_counter(_suppressed_rows_counter, dropped);
    return Status::OK();
}

Status FlussUnionLakeReader::_suppress_fast(Block* block, bool* handled) {
    *handled = false;
    SCOPED_TIMER(_profile.finalize_timer);
    SCOPED_TIMER(_suppress_timer);
    const auto rows = block->rows();
    const IColumn* column = block->get_by_position(_key_column_indexes[0]).column.get();
    const UInt8* nulls = nullptr;
    if (const auto* nullable = check_and_get_column<ColumnNullable>(*column)) {
        // Lake key columns arrive nullable even though a key is never null; only a block that actually holds a
        // null needs the per-row check, and only one without can be walked.
        if (nullable->has_null()) {
            nulls = nullable->get_null_map_data().data();
        }
        column = &nullable->get_nested_column();
    }
    IColumn::Filter keep(rows, 1);
    int64_t dropped = 0;
    const auto probe = [&](const auto& data) {
        if (nulls == nullptr && _sorted_keys != nullptr &&
            union_walk_sorted_keys(*_sorted_keys, data.data(), rows, keep.data(), &dropped)) {
            return;
        }
        for (size_t row = 0; row < rows; ++row) {
            const bool replaced = (nulls == nullptr || nulls[row] == 0) &&
                                  _fast_keys->contains(static_cast<int64_t>(data[row]));
            keep[row] = !replaced;
            dropped += replaced;
        }
    };
    if (const auto* longs = check_and_get_column<ColumnInt64>(*column)) {
        probe(longs->get_data());
    } else if (const auto* ints = check_and_get_column<ColumnInt32>(*column)) {
        probe(ints->get_data());
    } else {
        return Status::OK();
    }
    *handled = true;
    if (dropped > 0) {
        RETURN_IF_CATCH_EXCEPTION(Block::filter_block_internal(block, keep));
        update_counter(_suppressed_rows_counter, dropped);
    }
    return Status::OK();
}

Status FlussUnionLakeReader::_suppress_packed(Block* block, bool* handled) {
    *handled = false;
    SCOPED_TIMER(_profile.finalize_timer);
    SCOPED_TIMER(_suppress_timer);
    std::vector<PrimitiveType> types;
    std::vector<UnionPackingColumn> columns;
    if (!union_packing_columns(*block, _key_column_indexes, &types, &columns) ||
        types != _packed_keys->types) {
        // Keys the tail was not packed as: the generic predicate compares them instead.
        return Status::OK();
    }
    const auto rows = block->rows();
    IColumn::Filter keep(rows, 1);
    int64_t dropped = 0;
    std::string key;
    for (size_t row = 0; row < rows; ++row) {
        key.clear();
        union_pack_key(columns, row, &key);
        const bool replaced = _packed_keys->keys.contains(std::string_view(key));
        keep[row] = !replaced;
        dropped += replaced;
    }
    *handled = true;
    if (dropped > 0) {
        // Release the key-column views before COW filtering; retaining them would clone each key column.
        columns.clear();
        RETURN_IF_CATCH_EXCEPTION(Block::filter_block_internal(block, keep));
        update_counter(_suppressed_rows_counter, dropped);
    }
    return Status::OK();
}

bool FlussUnionLakeReader::current_split_pruned() const {
    DORIS_CHECK(_lake_reader != nullptr);
    return _lake_reader->current_split_pruned();
}

bool FlussUnionLakeReader::current_split_uses_metadata_count() const {
    DORIS_CHECK(_lake_reader != nullptr);
    return _lake_reader->current_split_uses_metadata_count();
}

Status FlussUnionLakeReader::abort_split() {
    DORIS_CHECK(_lake_reader != nullptr);
    return _lake_reader->abort_split();
}

Status FlussUnionLakeReader::close() {
    _suppression.reset();
    _suppression_tail_spec.clear();
    _current_keys.reset();
    _fast_keys = nullptr;
    _sorted_keys = nullptr;
    _packed_keys = nullptr;
    if (_lake_reader == nullptr) {
        return Status::OK();
    }
    return _lake_reader->close();
}

void FlussUnionLakeReader::set_batch_size(size_t batch_size) {
    format::TableReader::set_batch_size(batch_size);
    if (_lake_reader != nullptr) {
        _lake_reader->set_batch_size(_batch_size);
    }
}

int64_t FlussUnionLakeReader::condition_cache_hit_count() const {
    return _lake_reader == nullptr ? 0 : _lake_reader->condition_cache_hit_count();
}

} // namespace doris::format::fluss
