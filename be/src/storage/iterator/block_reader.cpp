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

#include "storage/iterator/block_reader.h"

#include <gen_cpp/olap_file.pb.h>
#include <glog/logging.h>
#include <stdint.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <iterator>
#include <memory>
#include <ostream>
#include <string>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "cloud/config.h"
#include "common/cast_set.h"
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/exception.h"
#include "common/status.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "runtime/runtime_state.h"
#include "storage/changelog.h"
#include "storage/iterator/agg_key_block_iterator.h"
#include "storage/iterator/changelog_block_reader_utils.h"
#include "storage/iterator/heap_merge_iterator.h"
#include "storage/iterator/vcollect_iterator.h"
#include "storage/olap_common.h"
#include "storage/olap_define.h"
#include "storage/row_ttl.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_reader_context.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/utils.h"

namespace doris {
class ColumnPredicate;
} // namespace doris

namespace doris {
using namespace ErrorCode;

static constexpr int32_t BLOCK_SIZE_CHECK_INTERVAL_ROWS = 64;

namespace {

// The rows of one rowset, read through its rowset reader: the child of the merge for a rowset
// whose reader reads its segments one after another.
class RowsetReaderIterator final : public RowwiseIterator {
public:
    RowsetReaderIterator(RowsetReaderSharedPtr rs_reader, ReadSchemaSPtr read_schema)
            : _rs_reader(std::move(rs_reader)), _read_schema(std::move(read_schema)) {}

    Status next_batch(Block* block) override { return _rs_reader->next_batch(block); }

    const ReadSchema& schema() const override { return *_read_schema; }

    void update_profile(RuntimeProfile* profile) override { _rs_reader->update_profile(profile); }

private:
    const RowsetReaderSharedPtr _rs_reader;
    const ReadSchemaSPtr _read_schema;
};

} // namespace

BlockReader::BlockReader() = default;

BlockReader::~BlockReader() = default;

void BlockReader::update_profile(RuntimeProfile* profile) {
    if (_agg_key_iter != nullptr) {
        _agg_key_iter->update_profile(profile);
        return;
    }
    _vcollect_iter.update_profile(profile);
}

Status BlockReader::next_block_with_aggregation(Block* block, bool* eof) {
    auto res = (this->*_next_block_func)(block, eof);
    if (res.ok() && (_filter_delete_sign || _filter_row_ttl) && block->rows() > 0) {
        RowVisibilityFilter filter;
        RETURN_IF_ERROR(build_row_visibility_filter(*block, *_tablet_schema, _filter_delete_sign,
                                                    _filter_row_ttl, _row_ttl_now_us, &filter));
        filter.record_filtered_rows(*block, _reader_context.rowid_conversion);
        RETURN_IF_CATCH_EXCEPTION(Block::filter_block_internal(block, filter.selection));
        _stats.rows_del_filtered += filter.rows_deleted;
    }
    if (!config::is_cloud_mode()) {
        if (!res.ok()) [[unlikely]] {
            static_cast<Tablet*>(_tablet.get())->report_error(res);
        }
    }
    return res;
}

ChangelogOp BlockReader::_read_changelog_op(const IColumn& col, size_t row) const {
    const IColumn* cur = &col;
    if (const auto* nullable = check_and_get_column<ColumnNullable>(*cur)) {
        if (nullable->is_null_at(row)) {
            return ChangelogOp::UNKNOWN;
        }
        cur = &nullable->get_nested_column();
    }

    if (const auto* int64_col = check_and_get_column<ColumnInt64>(*cur)) {
        return static_cast<ChangelogOp>(int64_col->get_element(row));
    }

    return ChangelogOp::UNKNOWN;
}

Status BlockReader::_write_changelog_op(IColumn& col, changelog::StreamChangeOp op) const {
    IColumn* cur = &col;
    ColumnNullable* nullable = nullptr;
    if (auto* n = typeid_cast<ColumnNullable*>(cur)) {
        nullable = n;
        cur = &nullable->get_nested_column();
    }

    if (auto* int64_col = typeid_cast<ColumnInt64*>(cur)) {
        int64_col->insert_value(static_cast<int64_t>(op));
    } else {
        return Status::InternalError("invalid column type");
    }

    if (nullable != nullptr) {
        nullable->get_null_map_data().push_back(0);
    }
    return Status::OK();
}

// Resolves which source-block column to read from for a given changelog row position.
// When use_before is true, return the ordinal of its before-image mirror.
// Changelog meta columns map to themselves.
uint32_t BlockReader::_resolve_source_column_ordinal(uint32_t ordinal, bool use_before) const {
    return use_before ? _read_schema->before_column_ordinal(ordinal) : ordinal;
}

bool BlockReader::_min_delta_values_equal(size_t last_row) {
    const auto& value_column_pairs = _read_schema->changelog_value_column_pairs();
    if (!_read_schema->changelog_value_pairs_complete() || value_column_pairs.empty() ||
        _min_delta_value_compare_unsupported) {
        return false;
    }
    bool before_image_available = false;
    for (const auto& [after_idx, before_idx] : value_column_pairs) {
        const auto* before_column = _stored_data_columns[before_idx].get();
        if (const auto* nullable = check_and_get_column<ColumnNullable>(*before_column)) {
            before_image_available |= !nullable->is_null_at(0);
        } else {
            before_image_available = true;
        }
        try {
            if (_stored_data_columns[before_idx]->compare_at(
                        0, last_row, *_stored_data_columns[after_idx], -1) != 0) {
                return false;
            }
        } catch (const Exception& e) {
            if (e.code() != ErrorCode::NOT_IMPLEMENTED_ERROR) {
                throw;
            }
            // Column types are stable for the lifetime of this reader. Once one value column
            // reports that compare_at is unsupported, no row can be proven to be a no-op. Cache
            // that capability result so BITMAP-like columns pay the exception cost at most once.
            _min_delta_value_compare_unsupported = true;
            return false;
        }
    }
    // Historical lookup and the compatibility writer both represent an unavailable BEFORE image
    // as an all-NULL row. Without a persisted validity bit, retaining the UPDATE is the only safe
    // choice; otherwise a missing-key DELETE followed by an all-NULL INSERT would disappear.
    return before_image_available;
}

void BlockReader::_init_pending_row_columns(const Block& block) {
    if (!_pending_row_columns.empty()) {
        return;
    }
    _pending_row_columns = block.clone_empty_columns();
}

// Drains the carry-over row produced on the previous batch boundary into the current
// output block. Returns true if a row was emitted, false if no pending row exists.
bool BlockReader::_emit_pending_row(MutableColumns& target_columns, size_t& output_row_count) {
    if (!_has_pending_row) {
        return false;
    }
    for (size_t i = 0; i < _pending_row_columns.size(); ++i) {
        target_columns[i]->insert_from(*_pending_row_columns[i], 0);
        _pending_row_columns[i]->clear();
    }
    _has_pending_row = false;
    output_row_count++;
    return true;
}

// Copies one source row into target_columns with the given output op code, picking BEFORE
// or AFTER values per column according to use_before. Used by _detail_change_next_block to
// materialize the BEFORE / AFTER halves of an UPDATE pair (and INSERT / DELETE singletons).
Status BlockReader::_append_change_row(MutableColumns& target_columns, const Block& src_block,
                                       size_t row_pos, changelog::StreamChangeOp output_op,
                                       bool use_before) {
    const int32_t op_ordinal = _read_schema->op_ordinal();
    for (size_t ordinal = 0; ordinal < target_columns.size(); ++ordinal) {
        if (static_cast<int32_t>(ordinal) == op_ordinal) {
            RETURN_IF_ERROR(_write_changelog_op(*target_columns[ordinal], output_op));
            continue;
        }
        auto source_ordinal =
                _resolve_source_column_ordinal(static_cast<uint32_t>(ordinal), use_before);
        target_columns[ordinal]->insert_from(*src_block.get_by_position(source_ordinal).column,
                                             row_pos);
    }
    return Status::OK();
}

// MIN_DELTA reader: groups consecutive rows sharing the same primary key in
// _stored_data_columns, then collapses the group into the minimum equivalent change
// (SKIP / INSERT / DELETE / UPDATE_BEFORE+AFTER) via AggregateFunctionMinDelta.
Status BlockReader::_min_delta_next_block(Block* block, bool* eof) {
    if (UNLIKELY(_eof && !_has_pending_row)) {
        *eof = true;
        return Status::OK();
    }

    if (_stored_data_columns.empty()) {
        _stored_data_columns = _next_row.block->clone_empty_columns();
    }

    auto target_columns_guard = block->mutate_columns_scoped();
    auto& target_columns = target_columns_guard.mutable_columns();
    size_t output_row_count = 0;
    _init_pending_row_columns(*block);
    const int32_t tso_ordinal = _read_schema->tso_ordinal();
    const int32_t lsn_ordinal = _read_schema->lsn_ordinal();
    const int32_t op_ordinal = _read_schema->op_ordinal();
    // A group is a run of consecutive rows sharing the same user key. Changelog reads are
    // globally key-ordered (ReaderParams::force_key_ordered_read), and the key columns are the
    // leading num_key_columns() columns of every read block, so a group boundary is exactly
    // where the user key changes. _stored_data_columns keeps the group's first row at index 0.
    const size_t num_key_columns = _tablet_schema->num_key_columns();
    while (output_row_count < batch_max_rows()) {
        if (_emit_pending_row(target_columns, output_row_count)) {
            continue;
        }
        if (_eof) {
            break;
        }
        bool need_pop = _stored_data_columns[0]->size() > 1;
        for (size_t i = 0; i < _stored_data_columns.size(); ++i) {
            if (need_pop) {
                _stored_data_columns[i]->pop_back(1);
            }
            _stored_data_columns[i]->insert_from(*_next_row.block->get_by_position(i).column,
                                                 _next_row.row_pos);
        }
        auto res = _vcollect_iter.next(&_next_row);
        if (UNLIKELY(res.is<END_OF_FILE>())) {
            _eof = true;
        } else if (UNLIKELY(!res.ok())) {
            return res;
        }

        // Extend the current group while the next row shares the same user key. is_same cannot
        // be used here: it marks cross-segment key matches for dedup, so consecutive same-key
        // rows that a compaction/quick-merge folded into one segment are left unmarked, which
        // would split one key's change chain into several groups. Compare the leading key
        // columns directly against the group's first row (index 0 of _stored_data_columns).
        if (!_eof) {
            if (_next_row.is_same) {
                continue;
            }
            bool same_key = true;
            for (size_t k = 0; k < num_key_columns; ++k) {
                if (_stored_data_columns[k]->compare_at(0, _next_row.row_pos,
                                                        *_next_row.block->get_by_position(k).column,
                                                        -1) != 0) {
                    same_key = false;
                    break;
                }
            }
            if (same_key) {
                continue;
            }
        }
        size_t group_size = _stored_data_columns[0]->size();
        auto first_op = _read_changelog_op(*_stored_data_columns[op_ordinal], 0);
        auto last_op = _read_changelog_op(*_stored_data_columns[op_ordinal], group_size - 1);
        auto result = changelog::AggregateFunctionMinDelta::calculate_result(first_op, last_op);
        switch (result) {
        case changelog::AggregateFunctionMinDelta::ResultType::SKIP:
            break;
        case changelog::AggregateFunctionMinDelta::ResultType::INSERT:
            for (size_t ordinal = 0; ordinal < target_columns.size(); ++ordinal) {
                if (static_cast<int32_t>(ordinal) == op_ordinal) {
                    RETURN_IF_ERROR(_write_changelog_op(*target_columns[ordinal],
                                                        changelog::StreamChangeOp::INSERT));
                } else {
                    // insert should use most recent value
                    target_columns[ordinal]->insert_from(*_stored_data_columns[ordinal],
                                                         group_size - 1);
                }
            }
            output_row_count++;
            break;
        case changelog::AggregateFunctionMinDelta::ResultType::DELETE:
            for (size_t ordinal = 0; ordinal < target_columns.size(); ++ordinal) {
                const auto read_ordinal = static_cast<int32_t>(ordinal);
                if (read_ordinal == op_ordinal) {
                    RETURN_IF_ERROR(_write_changelog_op(*target_columns[ordinal],
                                                        changelog::StreamChangeOp::DELETE));
                } else if (read_ordinal == lsn_ordinal || read_ordinal == tso_ordinal) {
                    target_columns[ordinal]->insert_from(*_stored_data_columns[ordinal],
                                                         group_size - 1);
                } else {
                    // delete should use first op value
                    auto source_ordinal =
                            _resolve_source_column_ordinal(static_cast<uint32_t>(ordinal), true);
                    target_columns[ordinal]->insert_from(*_stored_data_columns[source_ordinal], 0);
                }
            }
            output_row_count++;
            break;
        case changelog::AggregateFunctionMinDelta::ResultType::UPDATE_BEFORE_AFTER:
            if (changelog::is_valid_changelog_op(first_op) &&
                changelog::is_valid_changelog_op(last_op) &&
                _min_delta_values_equal(group_size - 1)) {
                break;
            }
            for (size_t ordinal = 0; ordinal < target_columns.size(); ++ordinal) {
                if (static_cast<int32_t>(ordinal) == op_ordinal) {
                    RETURN_IF_ERROR(_write_changelog_op(*target_columns[ordinal],
                                                        changelog::StreamChangeOp::UPDATE_BEFORE));
                } else {
                    auto source_ordinal =
                            _resolve_source_column_ordinal(static_cast<uint32_t>(ordinal), true);
                    target_columns[ordinal]->insert_from(*_stored_data_columns[source_ordinal], 0);
                }
            }
            output_row_count++;
            if (output_row_count >= batch_max_rows()) {
                for (size_t ordinal = 0; ordinal < target_columns.size(); ++ordinal) {
                    if (static_cast<int32_t>(ordinal) == op_ordinal) {
                        RETURN_IF_ERROR(
                                _write_changelog_op(*_pending_row_columns[ordinal],
                                                    changelog::StreamChangeOp::UPDATE_AFTER));
                    } else {
                        _pending_row_columns[ordinal]->insert_from(*_stored_data_columns[ordinal],
                                                                   group_size - 1);
                    }
                }
                _has_pending_row = true;
            } else {
                for (size_t ordinal = 0; ordinal < target_columns.size(); ++ordinal) {
                    if (static_cast<int32_t>(ordinal) == op_ordinal) {
                        RETURN_IF_ERROR(_write_changelog_op(
                                *target_columns[ordinal], changelog::StreamChangeOp::UPDATE_AFTER));
                    } else {
                        target_columns[ordinal]->insert_from(*_stored_data_columns[ordinal],
                                                             group_size - 1);
                    }
                }
                output_row_count++;
            }
            break;
        }

        for (auto& col : _stored_data_columns) {
            col->clear();
        }
    }
    *eof = _eof && !_has_pending_row;
    return Status::OK();
}

// DETAIL reader: emits every recorded changelog change verbatim. APPEND -> single INSERT row,
// DELETE -> single DELETE row, UPDATE -> a BEFORE+AFTER pair. When the AFTER row would
// overflow batch_max_rows(), it is parked in _pending_row_columns and flushed next call.
Status BlockReader::_detail_change_next_block(Block* block, bool* eof) {
    if (UNLIKELY(_eof && !_has_pending_row)) {
        *eof = true;
        return Status::OK();
    }
    auto target_columns_guard = block->mutate_columns_scoped();
    auto& target_columns = target_columns_guard.mutable_columns();
    size_t output_row_count = 0;
    _init_pending_row_columns(*block);
    const int32_t op_ordinal = _read_schema->op_ordinal();
    while (output_row_count < batch_max_rows()) {
        if (_emit_pending_row(target_columns, output_row_count)) {
            continue;
        }
        if (_eof) {
            break;
        }
        if (UNLIKELY(_next_row.block == nullptr)) {
            return Status::InternalError("invalid row reference in detail change reader");
        }
        const Block& source_block = *_next_row.block;
        const size_t row = _next_row.row_pos;
        const auto op = _read_changelog_op(*source_block.get_by_position(op_ordinal).column, row);
        if (op == ChangelogOp::UPDATE) {
            RETURN_IF_ERROR(_append_change_row(target_columns, source_block, row,
                                               changelog::StreamChangeOp::UPDATE_BEFORE, true));
            output_row_count++;
            if (output_row_count >= batch_max_rows()) {
                RETURN_IF_ERROR(_append_change_row(_pending_row_columns, source_block, row,
                                                   changelog::StreamChangeOp::UPDATE_AFTER, false));
                _has_pending_row = true;
            } else {
                RETURN_IF_ERROR(_append_change_row(target_columns, source_block, row,
                                                   changelog::StreamChangeOp::UPDATE_AFTER, false));
                output_row_count++;
            }
        } else if (op == ChangelogOp::APPEND) {
            RETURN_IF_ERROR(_append_change_row(target_columns, source_block, row,
                                               changelog::StreamChangeOp::INSERT, false));
            output_row_count++;
        } else if (op == ChangelogOp::DELETE) {
            RETURN_IF_ERROR(_append_change_row(target_columns, source_block, row,
                                               changelog::StreamChangeOp::DELETE, true));
            output_row_count++;
        }

        auto res = _vcollect_iter.next(&_next_row);
        if (UNLIKELY(res.is<END_OF_FILE>())) {
            _eof = true;
        } else if (UNLIKELY(!res.ok())) {
            return res;
        }
    }
    *eof = _eof && !_has_pending_row;
    return Status::OK();
}

bool BlockReader::_rowsets_not_mono_asc_disjoint(const ReaderParams& read_params) {
    std::string pre_rs_last_key;
    bool pre_rs_key_bounds_truncated {false};
    const std::vector<RowSetSplits>& rs_splits = read_params.rs_splits;
    for (const auto& rs_split : rs_splits) {
        if (rs_split.rs_reader->rowset()->num_rows() == 0) {
            continue;
        }
        if (rs_split.rs_reader->rowset()->is_segments_overlapping()) {
            return true;
        }
        std::string rs_first_key;
        bool has_first_key = rs_split.rs_reader->rowset()->first_key(&rs_first_key);
        if (!has_first_key) {
            return true;
        }
        bool cur_rs_key_bounds_truncated {
                rs_split.rs_reader->rowset()->is_segments_key_bounds_truncated()};
        if (!Slice::lhs_is_strictly_less_than_rhs(Slice {pre_rs_last_key},
                                                  pre_rs_key_bounds_truncated, Slice {rs_first_key},
                                                  cur_rs_key_bounds_truncated)) {
            return true;
        }
        bool has_last_key = rs_split.rs_reader->rowset()->last_key(&pre_rs_last_key);
        pre_rs_key_bounds_truncated = cur_rs_key_bounds_truncated;
        CHECK(has_last_key);
    }
    return false;
}

Status BlockReader::_init_collect_iter(const ReaderParams& read_params) {
    auto res = _capture_rs_readers(read_params);
    if (!res.ok()) {
        LOG(WARNING) << "fail to init reader when _capture_rs_readers. res:" << res
                     << ", tablet_id:" << read_params.tablet->tablet_id()
                     << ", schema_hash:" << read_params.tablet->schema_hash()
                     << ", reader_type:" << int(read_params.reader_type)
                     << ", version:" << read_params.version;
        return res;
    }
    // check if rowsets are noneoverlapping
    {
        SCOPED_RAW_TIMER(&_stats.block_reader_vcollect_iter_init_timer_ns);
        _is_rowsets_overlapping = _rowsets_not_mono_asc_disjoint(read_params);
        const bool is_min_delta_stream =
                read_params.changelog_scan_type == TChangelogScanType::MIN_DELTA;
        const bool force_merge = read_params.read_orderby_key || is_min_delta_stream;
        const bool is_reverse = !is_min_delta_stream && read_params.read_orderby_key_reverse;
        _vcollect_iter.init(this, _is_rowsets_overlapping, force_merge, is_reverse);
    }

    std::vector<RowsetReaderSharedPtr> valid_rs_readers;
    RuntimeState* runtime_state = read_params.runtime_state;

    {
        SCOPED_RAW_TIMER(&_stats.block_reader_rs_readers_init_timer_ns);
        for (int i = 0; i < read_params.rs_splits.size(); ++i) {
            if (runtime_state != nullptr && runtime_state->is_cancelled()) {
                return runtime_state->cancel_reason();
            }

            auto& rs_split = read_params.rs_splits[i];

            // _vcollect_iter.topn_next() will init rs_reader by itself
            if (!_vcollect_iter.use_topn_next()) {
                RETURN_IF_ERROR(rs_split.rs_reader->init(&_reader_context, rs_split));
            }

            Status res1 = _vcollect_iter.add_child(rs_split);
            if (!res1.ok() && !res1.is<END_OF_FILE>()) {
                LOG(WARNING) << "failed to add child to iterator, err=" << res1;
                return res1;
            }
            if (res1.ok()) {
                valid_rs_readers.push_back(rs_split.rs_reader);
            }
        }
    }
    {
        SCOPED_RAW_TIMER(&_stats.block_reader_build_heap_init_timer_ns);
        RETURN_IF_ERROR(_vcollect_iter.build_heap(valid_rs_readers));
        // _vcollect_iter.topn_next() can not use current_row
        if (!_vcollect_iter.use_topn_next()) {
            auto status = _vcollect_iter.current_row(&_next_row);
            _eof = status.is<END_OF_FILE>();
        }
    }

    return Status::OK();
}

bool BlockReader::_folds_agg_keys(const ReaderParams& read_params, bool map_changelog_columns) {
    if (map_changelog_columns || _direct_mode ||
        _tablet_schema->keys_type() != KeysType::AGG_KEYS) {
        return false;
    }
    // Rowsets with no segments that overlap on keys and keys that ascend from one rowset to the
    // next hold every key once: they have nothing to fold and are read as they are, like DUP_KEYS.
    SCOPED_RAW_TIMER(&_stats.block_reader_vcollect_iter_init_timer_ns);
    return _rowsets_not_mono_asc_disjoint(read_params);
}

Status BlockReader::_init_agg_key_iter(const ReaderParams& read_params) {
    auto res = _capture_rs_readers(read_params);
    if (!res.ok()) {
        LOG(WARNING) << "fail to init reader when _capture_rs_readers. res:" << res
                     << ", tablet_id:" << read_params.tablet->tablet_id()
                     << ", schema_hash:" << read_params.tablet->schema_hash()
                     << ", reader_type:" << int(read_params.reader_type)
                     << ", version:" << read_params.version;
        return res;
    }
    // Only duplicate and merge-on-write tables have a changelog, and the scanner pushes a LIMIT or
    // a key TopN down only into the reads that do not merge: the rows are merged by the whole key.
    DORIS_CHECK(!_reader_context.read_changelog);
    DORIS_CHECK(!read_params.read_orderby_key);
    DORIS_CHECK(_reader_context.general_read_limit <= 0);

    // The children of the merge, in (rowset version, segment) order.
    std::vector<RowwiseIteratorUPtr> children;
    RuntimeState* runtime_state = read_params.runtime_state;
    {
        SCOPED_RAW_TIMER(&_stats.block_reader_rs_readers_init_timer_ns);
        int64_t previous_start_version = -1;
        for (const auto& rs_split : read_params.rs_splits) {
            if (runtime_state != nullptr && runtime_state->is_cancelled()) {
                return runtime_state->cancel_reason();
            }
            const RowsetReaderSharedPtr& rs_reader = rs_split.rs_reader;
            RETURN_IF_ERROR(rs_reader->init(&_reader_context, rs_split));
            // The merge puts the rows of one key in the order of its children, the older first,
            // so the rowsets must come in version order.
            DORIS_CHECK_GE(rs_reader->rowset()->start_version(), previous_start_version);
            previous_start_version = rs_reader->rowset()->start_version();
            RETURN_IF_ERROR(_add_merge_children(rs_reader, &children));
        }
    }

    RowwiseIteratorUPtr input;
    RETURN_IF_ERROR(_init_agg_key_merge(std::move(children), runtime_state, &input));
    _agg_key_iter = std::make_unique<AggKeyBlockIterator>(
            std::move(input), batch_max_rows(), preferred_block_size_bytes(), _read_schema,
            read_params.get_be_exec_version(), &_merged_rows);
    Status st = _agg_key_iter->init();
    if (!st.ok()) {
        st.prepend(fmt::format(
                "Failed to init reader when init agg state: tablet_id: {}, schema_hash: {}, "
                "reader_type: {}, version: {}: ",
                read_params.tablet->tablet_id(), read_params.tablet->schema_hash(),
                int(read_params.reader_type), read_params.version.to_string()));
    }
    return st;
}

Status BlockReader::_add_merge_children(const RowsetReaderSharedPtr& rs_reader,
                                        std::vector<RowwiseIteratorUPtr>* children) {
    if (!rs_reader->is_merge_iterator()) {
        // The rowset reader reads the segments of this rowset one after another.
        children->push_back(std::make_unique<RowsetReaderIterator>(rs_reader, _read_schema));
        return Status::OK();
    }
    // The segments of this rowset overlap on keys: every one is a child of the merge, so one
    // merge orders the segments of a rowset and the rowsets together.
    std::vector<RowwiseIteratorUPtr> segment_iters;
    RETURN_IF_ERROR(rs_reader->get_segment_iterators(&_reader_context, &segment_iters));
    for (auto& segment_iter : segment_iters) {
        children->push_back(std::move(segment_iter));
    }
    _segment_merge_rs_readers.push_back(rs_reader);
    return Status::OK();
}

Status BlockReader::_init_agg_key_merge(std::vector<RowwiseIteratorUPtr> children,
                                        RuntimeState* runtime_state, RowwiseIteratorUPtr* input) {
    SCOPED_RAW_TIMER(&_stats.block_reader_build_heap_init_timer_ns);
    // By the whole key, the rows of a key in the order they were written: the fold copies the
    // REPLACE values of a key's last row, the newest one.
    MergeDecision decision;
    for (uint32_t ordinal = 0; ordinal < _read_schema->num_key_columns(); ++ordinal) {
        decision.keys.emplace_back(static_cast<int>(ordinal), /*direction=*/1,
                                   /*nulls_direction=*/-1);
    }
    *input = std::make_unique<VHeapMergeIterator>(std::move(children), _read_schema, decision);
    StorageReadOptions opts(_stats);
    opts.block_row_max = cast_set<uint32_t>(batch_max_rows());
    opts.preferred_block_size_bytes = preferred_block_size_bytes();
    opts.runtime_state = runtime_state;
    opts.tablet_id = _tablet->tablet_id();
    return (*input)->init(opts);
}

Status BlockReader::init(const ReaderParams& read_params) {
    SCOPED_RAW_TIMER(&_stats.tablet_reader_init_timer_ns);
    RETURN_IF_ERROR(TabletReader::init(read_params));
    _filter_row_ttl = should_gc_row_ttl(
            *_tablet_schema, _tablet->enable_unique_key_merge_on_write(),
            _tablet->is_changelog_tablet(), read_params.reader_type, read_params.version);
    if (_filter_row_ttl) {
        _row_ttl_now_us = UnixMicros();
    }

    // A changelog scan maps the before-image columns; every other read of this reader, which is
    // the one that merges rows across rowsets, builds the sequence mapping instead.
    const bool map_changelog_columns =
            read_params.changelog_scan_type == TChangelogScanType::MIN_DELTA ||
            read_params.changelog_scan_type == TChangelogScanType::DETAIL;
    const bool merge_by_sequence_mapping =
            !map_changelog_columns && _tablet_schema->has_seq_map() &&
            _tablet_schema->keys_type() == UNIQUE_KEYS && !_direct_mode &&
            !(read_params.reader_type == ReaderType::READER_QUERY &&
              _tablet->enable_unique_key_merge_on_write());
    RETURN_IF_ERROR(_read_schema->init_from_tablet_schema(
            *_tablet_schema, merge_by_sequence_mapping, map_changelog_columns));
    if (map_changelog_columns) {
        _min_delta_value_compare_unsupported = false;
    }

    // The reads of AGG_KEYS tables that fold the rows of a key (_agg_key_next_block below) merge
    // their rows with a VHeapMergeIterator; the other reads go through VCollectIterator.
    const bool fold_agg_keys = _folds_agg_keys(read_params, map_changelog_columns);
    auto status = fold_agg_keys ? _init_agg_key_iter(read_params) : _init_collect_iter(read_params);
    if (!status.ok()) [[unlikely]] {
        if (!config::is_cloud_mode()) {
            static_cast<Tablet*>(_tablet.get())->report_error(status);
        }
        return status;
    }

    // MIN_DELTA: collapse consecutive same-key changes to the minimum equivalent change set
    // (e.g. INSERT+DELETE -> SKIP, INSERT+UPDATE -> INSERT). Reduces downstream traffic.
    if (read_params.changelog_scan_type == TChangelogScanType::MIN_DELTA) {
        _next_block_func = &BlockReader::_min_delta_next_block;
        return Status::OK();
    }
    // DETAIL: emit every recorded change as-is, with BEFORE+AFTER rows for UPDATE.
    // Used when the consumer needs full change history rather than the net delta.
    if (read_params.changelog_scan_type == TChangelogScanType::DETAIL) {
        _next_block_func = &BlockReader::_detail_change_next_block;
        return Status::OK();
    }

    if (_direct_mode) {
        _next_block_func = &BlockReader::_direct_next_block;
        return Status::OK();
    }
    switch (_tablet_schema->keys_type()) {
    case KeysType::DUP_KEYS:
        _next_block_func = &BlockReader::_direct_next_block;
        break;
    case KeysType::UNIQUE_KEYS:
        if (read_params.reader_type == ReaderType::READER_QUERY &&
            _reader_context.enable_unique_key_merge_on_write) {
            _next_block_func = &BlockReader::_direct_next_block;
        } else if (merge_by_sequence_mapping) {
            _next_block_func = &BlockReader::_replace_key_next_block;
        } else {
            _next_block_func = &BlockReader::_unique_key_next_block;
        }
        break;
    case KeysType::AGG_KEYS:
        _next_block_func = fold_agg_keys ? &BlockReader::_agg_key_next_block
                                         : &BlockReader::_direct_next_block;
        break;
    default:
        DCHECK(false) << "No next row function for type:" << _tablet_schema->keys_type();
        break;
    }
    _filter_delete_sign =
            _next_block_func == &BlockReader::_unique_key_next_block && _delete_sign_available;

    return Status::OK();
}

Status BlockReader::_direct_next_block(Block* block, bool* eof) {
    auto res = _vcollect_iter.next(block);
    if (UNLIKELY(!res.ok() && !res.is<END_OF_FILE>())) {
        return res;
    }
    *eof = res.is<END_OF_FILE>();
    _eof = *eof;
    return Status::OK();
}

Status BlockReader::_replace_key_next_block(Block* block, bool* eof) {
    if (UNLIKELY(_eof)) {
        *eof = true;
        return Status::OK();
    }

    auto target_block_row = 0;
    auto target_columns_guard = block->mutate_columns_scoped();
    auto& target_columns = target_columns_guard.mutable_columns();
    auto merged_row = 0;
    while (target_block_row < batch_max_rows() && !_eof) {
        RETURN_IF_ERROR(_insert_data_normal(target_columns));
        target_block_row++;

        while (!_eof) {
            // the version is in reverse order, the first row is the highest version,
            // in UNIQUE_KEY highest version is the final result
            auto res = _vcollect_iter.next(&_next_row);
            if (UNLIKELY(res.is<END_OF_FILE>())) {
                _eof = true;
                *eof = true;
                break;
            }

            if (UNLIKELY(!res.ok())) {
                LOG(WARNING) << "next failed: " << res;
                return res;
            }

            if (_next_row.is_same) {
                merged_row++;
                _compare_sequence_map_and_replace(target_columns);
            } else {
                break;
            }
        }
        // Byte-budget check: after the inner loop _next_row is either EOF or the next different
        // key, so it is safe to stop accumulating here without repeating any row.
        if (target_block_row % BLOCK_SIZE_CHECK_INTERVAL_ROWS == 0 &&
            _reached_byte_budget(target_columns)) {
            break;
        }
    }
    _merged_rows += merged_row;
    return Status::OK();
}

bool BlockReader::_reached_byte_budget(const MutableColumns& columns) const {
    return config::enable_adaptive_batch_size && _reader_context.preferred_block_size_bytes > 0 &&
           Block::columns_byte_size(columns) >= _reader_context.preferred_block_size_bytes;
}

void BlockReader::_compare_sequence_map_and_replace(MutableColumns& columns) {
    auto src_block = _next_row.block.get();
    auto src_pos = _next_row.row_pos;

    for (const auto& [seq_ordinal, value_ordinals] : _read_schema->sequence_map()) {
        auto* dst_seq_column = columns[seq_ordinal].get();
        auto dst_pos = dst_seq_column->size() - 1;
        auto src_seq_column = src_block->get_by_position(seq_ordinal).column;
        // the rowset version of dst is higher .
        auto res = dst_seq_column->compare_at(dst_pos, src_pos, *src_seq_column, -1);
        if (res >= 0) {
            continue;
        }

        // update value and seq column
        for (auto value_ordinal : value_ordinals) {
            auto src_column = src_block->get_by_position(value_ordinal).column;
            auto* dst_column = columns[value_ordinal].get();
            dst_column->pop_back(1);
            dst_column->insert_from(*src_column, src_pos);
        }

        dst_seq_column->pop_back(1);
        dst_seq_column->insert_from(*src_seq_column, src_pos);
    }
}

Status BlockReader::_agg_key_next_block(Block* block, bool* eof) {
    return _agg_key_iter->next_block(block, eof);
}

Status BlockReader::_unique_key_next_block(Block* block, bool* eof) {
    if (UNLIKELY(_eof)) {
        *eof = true;
        return Status::OK();
    }

    auto target_block_row = 0;
    auto target_columns_guard = block->mutate_columns_scoped();
    auto& target_columns = target_columns_guard.mutable_columns();

    do {
        RETURN_IF_ERROR(_insert_data_normal(target_columns));
        target_block_row++;

        // the version is in reverse order, the first row is the highest version,
        // in UNIQUE_KEY highest version is the final result, there is no need to
        // merge the lower versions
        auto res = _vcollect_iter.next(&_next_row);
        if (UNLIKELY(res.is<END_OF_FILE>())) {
            _eof = true;
            *eof = true;
            break;
        }

        if (UNLIKELY(!res.ok())) {
            LOG(WARNING) << "next failed: " << res;
            return res;
        }
        // Byte-budget check: _next_row is already saved so stopping here is safe.
        if (target_block_row % BLOCK_SIZE_CHECK_INTERVAL_ROWS == 0 &&
            _reached_byte_budget(target_columns)) {
            break;
        }
    } while (target_block_row < batch_max_rows());

    return Status::OK();
}

Status BlockReader::_insert_data_normal(MutableColumns& columns) {
    auto block = _next_row.block.get();

    RETURN_IF_CATCH_EXCEPTION({
        for (size_t ordinal = 0; ordinal < columns.size(); ++ordinal) {
            columns[ordinal]->insert_from(*block->get_by_position(ordinal).column,
                                          _next_row.row_pos);
        }
    });
    return Status::OK();
}

} // namespace doris
