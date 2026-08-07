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
#include <charconv>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/cast_set.h"
#include "core/assert_cast.h"
#include "core/column/column_vector.h"
#include "exprs/vexpr_context.h"
#include "exprs/vslot_ref.h"
#include "format_v2/column_mapper.h"
#include "format_v2/expr/equality_delete_predicate.h"
#include "format_v2/jni/fluss_jni_reader.h"
#include "format_v2/table/paimon_reader.h"
#include "runtime/descriptors.h"
#include "runtime/file_scan_profile.h"
#include "runtime/runtime_state.h"

namespace doris::format::fluss {
namespace {

// The scan-node properties this reader reads. The fluss connector states that `fluss.union.*` is the
// whole of what BE's C++ side knows about fluss; anything added here has to be added there too.
constexpr const char* PROP_PK_NAMES = "fluss.union.pk_names";
constexpr const char* PROP_MAX_TAIL_ROWS = "fluss.union.max_tail_rows";

// The per-range payload of a wrapped lake split: a plain one carries only its kind, a suppressed
// one also names the log tail that supersedes part of it.
constexpr const char* PROP_RANGE_TYPE = "fluss.range_type";
constexpr const char* PROP_TAIL = "fluss.union.tail";
constexpr const char* RANGE_TYPE_LAKE = "LAKE";
constexpr const char* RANGE_TYPE_LAKE_SUPPRESS = "LAKE_SUPPRESS";

// The range this reader synthesizes to read the tail. A plain bounded log read, which is what the
// suppression set is: every key the tail touched, whatever it ended up saying about it.
constexpr const char* RANGE_TYPE_LOG = "LOG";
constexpr const char* PROP_PARTITION_ID = "fluss.partition_id";
constexpr const char* PROP_BUCKET_ID = "fluss.bucket_id";
constexpr const char* PROP_LOG_START_OFFSET = "fluss.log_start_offset";
constexpr const char* PROP_LOG_STOP_OFFSET = "fluss.log_stop_offset";

constexpr size_t TAIL_BATCH_ROWS = 4096;

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

void update_counter(RuntimeProfile::Counter* counter, int64_t value) {
    if (counter != nullptr) {
        COUNTER_UPDATE(counter, value);
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
    const auto names_it = properties.find(PROP_PK_NAMES);
    if (names_it == properties.end() || names_it->second.empty()) {
        // FE states the union properties only when it plans a primary-key union, and this reader
        // also serves the plain LAKE splits of a log-table union, which have no keys and nothing to
        // suppress. Their absence is therefore not an error here; a LAKE_SUPPRESS split arriving
        // anyway fails loud in _prepare_suppression.
        return Status::OK();
    }
    for (const auto name : split_on(names_it->second, ',')) {
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

Status FlussUnionLakeReader::_load_suppression_keys(const format::SplitReadOptions& options,
                                                    const Tail& tail) {
    if (options.cache == nullptr) {
        return Status::InternalError(
                "fluss union read: no split cache to hold the keys of log tail '{}'", tail.spec);
    }
    // Length-prefixed so that no boundary between the fixed prefix and the tail can be reinterpreted
    // as part of the tail itself. One scan node reads one table, so the tail alone identifies it.
    const auto cache_key = fmt::format("fluss_union_tail:{}:{}", tail.spec.size(), tail.spec);
    Status read_status = Status::OK();
    bool cache_hit = false;
    auto* cached = options.cache->get<SuppressionKeys>(
            cache_key,
            [&]() -> SuppressionKeys* {
                auto keys = std::make_unique<SuppressionKeys>();
                read_status = _read_tail_keys(tail, keys.get());
                if (!read_status.ok()) {
                    return nullptr;
                }
                return keys.release();
            },
            &cache_hit);
    RETURN_IF_ERROR(read_status);
    DORIS_CHECK(cached != nullptr);
    if (cache_hit) {
        update_counter(_tail_cache_hit_counter, 1);
    } else {
        update_counter(_tail_keys_read_counter, cached->records);
        update_counter(_tail_keys_retained_counter, cast_set<int64_t>(cached->keys.rows()));
    }
    return _build_suppression_predicate(cached->keys);
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
            return Status::OK();
        }
    }
}

Status FlussUnionLakeReader::_read_tail_keys(const Tail& tail, SuppressionKeys* keys) {
    DORIS_CHECK(keys != nullptr);
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
    RETURN_IF_ERROR(reader.init({
            .projected_columns = _key_columns,
            .conjuncts = {},
            .format = format::FileFormat::JNI,
            .scan_params = _scan_params,
            .io_ctx = _io_ctx,
            .runtime_state = _runtime_state,
            .scanner_profile = _scanner_profile,
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
    const auto status = drain();
    // A fluss connection left open keeps its netty and metadata-updater threads alive for the life
    // of the BE process, so the reader is closed on the failing path too.
    const auto close_status = reader.close();
    RETURN_IF_ERROR(status);
    return close_status;
}

Status FlussUnionLakeReader::_build_suppression_predicate(const Block& keys) {
    std::vector<int> key_positions;
    key_positions.reserve(_key_column_indexes.size());
    for (const auto index : _key_column_indexes) {
        key_positions.push_back(cast_set<int>(index));
    }
    DORIS_CHECK(keys.columns() == key_positions.size());
    auto predicate = std::make_shared<EqualityDeletePredicate>(keys, key_positions);
    for (size_t i = 0; i < _key_column_indexes.size(); ++i) {
        const auto position = key_positions[i];
        // The block this runs against is the table-schema block the lake half returns, so a key
        // column is simply the projected column at its own position - already mapped, already of the
        // projected type, and therefore comparable with the tail's column of the same name.
        predicate->add_child(VSlotRef::create_shared(position, position, -1, _key_columns[i].type,
                                                     _key_columns[i].name));
    }
    _suppression = VExprContext::create_shared(std::move(predicate));
    RowDescriptor row_desc;
    RETURN_IF_ERROR(_suppression->prepare(_runtime_state, row_desc));
    return _suppression->open(_runtime_state);
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
    SCOPED_TIMER(_profile.finalize_timer);
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
