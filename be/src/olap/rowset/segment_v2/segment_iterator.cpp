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

#include "olap/rowset/segment_v2/segment_iterator.h"

#include <assert.h>
#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/olap_file.pb.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <iterator>
#include <memory>
#include <numeric>
#include <set>
#include <utility>
#include <vector>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/consts.h"
#include "common/exception.h"
#include "common/logging.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "io/fs/file_reader.h"
#include "io/io_common.h"
#include "olap/bloom_filter_predicate.h"
#include "olap/column_predicate.h"
#include "olap/field.h"
#include "olap/iterators.h"
#include "olap/like_column_predicate.h"
#include "olap/match_predicate.h"
#include "olap/olap_common.h"
#include "olap/primary_key_index.h"
#include "olap/rowset/segment_v2/bitmap_index_reader.h"
#include "olap/rowset/segment_v2/column_reader.h"
#include "olap/rowset/segment_v2/indexed_column_reader.h"
#include "olap/rowset/segment_v2/inverted_index_file_reader.h"
#include "olap/rowset/segment_v2/inverted_index_reader.h"
#include "olap/rowset/segment_v2/row_ranges.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/schema.h"
#include "olap/short_key_index.h"
#include "olap/tablet_schema.h"
#include "olap/types.h"
#include "olap/utils.h"
#include "runtime/define_primitive_type.h"
#include "runtime/query_context.h"
#include "runtime/runtime_predicate.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "util/defer_op.h"
#include "util/doris_metrics.h"
#include "util/key_util.h"
#include "util/simd/bits.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_object.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/schema_util.h"
#include "vec/common/string_ref.h"
#include "vec/common/typeid_cast.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_number.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vliteral.h"
#include "vec/exprs/vslot_ref.h"
#include "vec/json/path_in_data.h"

namespace doris {
using namespace ErrorCode;
namespace segment_v2 {

SegmentIterator::~SegmentIterator() = default;

// A fast range iterator for roaring bitmap. Output ranges use closed-open form, like [from, to).
// Example:
//   input bitmap:  [0 1 4 5 6 7 10 15 16 17 18 19]
//   output ranges: [0,2), [4,8), [10,11), [15,20) (when max_range_size=10)
//   output ranges: [0,2), [4,7), [7,8), [10,11), [15,18), [18,20) (when max_range_size=3)
class SegmentIterator::BitmapRangeIterator {
public:
    BitmapRangeIterator() = default;
    virtual ~BitmapRangeIterator() = default;

    explicit BitmapRangeIterator(const roaring::Roaring& bitmap) {
        roaring_init_iterator(&bitmap.roaring, &_iter);
    }

    bool has_more_range() const { return !_eof; }

    [[nodiscard]] static uint32_t get_batch_size() { return kBatchSize; }

    // read next range into [*from, *to) whose size <= max_range_size.
    // return false when there is no more range.
    virtual bool next_range(const uint32_t max_range_size, uint32_t* from, uint32_t* to) {
        if (_eof) {
            return false;
        }

        *from = _buf[_buf_pos];
        uint32_t range_size = 0;
        uint32_t expect_val = _buf[_buf_pos]; // this initial value just make first batch valid

        // if array is contiguous sequence then the following conditions need to be met :
        // a_0: x
        // a_1: x+1
        // a_2: x+2
        // ...
        // a_p: x+p
        // so we can just use (a_p-a_0)-p to check conditions
        // and should notice the previous batch needs to be continuous with the current batch
        while (!_eof && range_size + _buf_size - _buf_pos <= max_range_size &&
               expect_val == _buf[_buf_pos] &&
               _buf[_buf_size - 1] - _buf[_buf_pos] == _buf_size - 1 - _buf_pos) {
            range_size += _buf_size - _buf_pos;
            expect_val = _buf[_buf_size - 1] + 1;
            _read_next_batch();
        }

        // promise remain range not will reach next batch
        if (!_eof && range_size < max_range_size && expect_val == _buf[_buf_pos]) {
            do {
                _buf_pos++;
                range_size++;
            } while (range_size < max_range_size && _buf[_buf_pos] == _buf[_buf_pos - 1] + 1);
        }
        *to = *from + range_size;
        return true;
    }

    // read batch_size of rowids from roaring bitmap into buf array
    virtual uint32_t read_batch_rowids(rowid_t* buf, uint32_t batch_size) {
        return roaring::api::roaring_read_uint32_iterator(&_iter, buf, batch_size);
    }

private:
    void _read_next_batch() {
        _buf_pos = 0;
        _buf_size = roaring::api::roaring_read_uint32_iterator(&_iter, _buf, kBatchSize);
        _eof = (_buf_size == 0);
    }

    static const uint32_t kBatchSize = 256;
    roaring::api::roaring_uint32_iterator_t _iter;
    uint32_t _buf[kBatchSize];
    uint32_t _buf_pos = 0;
    uint32_t _buf_size = 0;
    bool _eof = false;
};

// A backward range iterator for roaring bitmap. Output ranges use closed-open form, like [from, to).
// Example:
//   input bitmap:  [0 1 4 5 6 7 10 15 16 17 18 19]
//   output ranges: , [15,20), [10,11), [4,8), [0,2) (when max_range_size=10)
//   output ranges: [17,20), [15,17), [10,11), [5,8), [4, 5), [0,2) (when max_range_size=3)
class SegmentIterator::BackwardBitmapRangeIterator : public SegmentIterator::BitmapRangeIterator {
public:
    explicit BackwardBitmapRangeIterator(const roaring::Roaring& bitmap) {
        roaring_init_iterator_last(&bitmap.roaring, &_riter);
        _rowid_count = roaring_bitmap_get_cardinality(&bitmap.roaring);
        _rowid_left = _rowid_count;
    }

    bool has_more_range() const { return !_riter.has_value; }

    // read next range into [*from, *to) whose size <= max_range_size.
    // return false when there is no more range.
    bool next_range(const uint32_t max_range_size, uint32_t* from, uint32_t* to) override {
        if (!_riter.has_value) {
            return false;
        }

        uint32_t range_size = 0;
        *to = _riter.current_value + 1;

        do {
            *from = _riter.current_value;
            range_size++;
            roaring_previous_uint32_iterator(&_riter);
        } while (range_size < max_range_size && _riter.has_value &&
                 _riter.current_value + 1 == *from);

        return true;
    }
    /**
     * Reads a batch of row IDs from a roaring bitmap, starting from the end and moving backwards.
     * This function retrieves the last `batch_size` row IDs from the bitmap and stores them in the provided buffer.
     * It updates the internal state to track how many row IDs are left to read in subsequent calls.
     *
     * The row IDs are read in reverse order, but stored in the buffer maintaining their original order in the bitmap.
     *
     * Example:
     *   input bitmap: [0 1 4 5 6 7 10 15 16 17 18 19]
     *   If the bitmap has 12 elements and batch_size is set to 5, the function will first read [15, 16, 17, 18, 19]
     *   into the buffer, leaving 7 elements left. In the next call with batch_size 5, it will read [4, 5, 6, 7, 10].
     *
     */
    uint32_t read_batch_rowids(rowid_t* buf, uint32_t batch_size) override {
        if (!_riter.has_value || _rowid_left == 0) {
            return 0;
        }

        if (_rowid_count <= batch_size) {
            roaring_bitmap_to_uint32_array(_riter.parent,
                                           buf); // Fill 'buf' with '_rowid_count' elements.
            uint32_t num_read = _rowid_left;     // Save the number of row IDs read.
            _rowid_left = 0;                     // No row IDs left after this operation.
            return num_read;                     // Return the number of row IDs read.
        }

        uint32_t read_size = std::min(batch_size, _rowid_left);
        uint32_t num_read = 0; // Counter for the number of row IDs read.

        // Read row IDs into the buffer in reverse order.
        while (num_read < read_size && _riter.has_value) {
            buf[read_size - num_read - 1] = _riter.current_value;
            num_read++;
            _rowid_left--; // Decrement the count of remaining row IDs.
            roaring_previous_uint32_iterator(&_riter);
        }

        // Return the actual number of row IDs read.
        return num_read;
    }

private:
    roaring::api::roaring_uint32_iterator_t _riter;
    uint32_t _rowid_count;
    uint32_t _rowid_left;
};

SegmentIterator::SegmentIterator(std::shared_ptr<Segment> segment, SchemaSPtr schema)
        : _segment(std::move(segment)),
          _schema(schema),
          _column_iterators(_schema->num_columns()),
          _bitmap_index_iterators(_schema->num_columns()),
          _inverted_index_iterators(_schema->num_columns()),
          _cur_rowid(0),
          _lazy_materialization_read(false),
          _lazy_inited(false),
          _inited(false),
          _pool(new ObjectPool) {}

Status SegmentIterator::init(const StorageReadOptions& opts) {
    auto status = _init_impl(opts);
    if (!status.ok()) {
        _segment->update_healthy_status(status);
    }
    return status;
}

Status SegmentIterator::_init_impl(const StorageReadOptions& opts) {
    // get file handle from file descriptor of segment
    if (_inited) {
        return Status::OK();
    }
    _inited = true;
    _file_reader = _segment->_file_reader;
    _opts = opts;
    _col_predicates.clear();

    for (const auto& predicate : opts.column_predicates) {
        if (!_segment->can_apply_predicate_safely(predicate->column_id(), predicate, *_schema,
                                                  _opts.io_ctx.reader_type)) {
            continue;
        }
        _col_predicates.emplace_back(predicate);
    }
    _tablet_id = opts.tablet_id;
    // Read options will not change, so that just resize here
    _block_rowids.resize(_opts.block_row_max);

    _remaining_conjunct_roots = opts.remaining_conjunct_roots;

    if (_schema->rowid_col_idx() > 0) {
        _record_rowids = true;
    }

    RETURN_IF_ERROR(init_iterators());

    if (opts.output_columns != nullptr) {
        _output_columns = *(opts.output_columns);
    }

    _storage_name_and_type.resize(_schema->columns().size());
    auto storage_format = _opts.tablet_schema->get_inverted_index_storage_format();
    for (int i = 0; i < _schema->columns().size(); ++i) {
        const Field* col = _schema->column(i);
        if (col) {
            auto storage_type = _segment->get_data_type_of(
                    Segment::ColumnIdentifier {
                            col->unique_id(),
                            col->parent_unique_id(),
                            col->path(),
                            col->is_nullable(),
                    },
                    _opts.io_ctx.reader_type != ReaderType::READER_QUERY);
            if (storage_type == nullptr) {
                storage_type = vectorized::DataTypeFactory::instance().create_data_type(*col);
            }
            // Currently, when writing a lucene index, the field of the document is column_name, and the column name is
            // bound to the index field. Since version 1.2, the data file storage has been changed from column_name to
            // column_unique_id, allowing the column name to be changed. Due to current limitations, previous inverted
            // index data cannot be used after Doris changes the column name. Column names also support Unicode
            // characters, which may cause other problems with indexing in non-ASCII characters.
            // After consideration, it was decided to change the field name from column_name to column_unique_id in
            // format V2, while format V1 continues to use column_name.
            std::string field_name;
            if (storage_format == InvertedIndexStorageFormatPB::V1) {
                field_name = col->name();
            } else {
                if (col->is_extracted_column()) {
                    // variant sub col
                    // field_name format: parent_unique_id.sub_col_name
                    field_name = std::to_string(col->parent_unique_id()) + "." + col->name();
                } else {
                    field_name = std::to_string(col->unique_id());
                }
            }
            _storage_name_and_type[i] = std::make_pair(field_name, storage_type);
        }
    }

    RETURN_IF_ERROR(_construct_compound_expr_context());
    _enable_common_expr_pushdown = !_common_expr_ctxs_push_down.empty();
    _initialize_predicate_results();
    return Status::OK();
}

void SegmentIterator::_initialize_predicate_results() {
    // Initialize from _col_predicates
    for (auto* pred : _col_predicates) {
        int cid = pred->column_id();
        _column_predicate_inverted_index_status[cid][pred] = false;
    }

    _calculate_expr_in_remaining_conjunct_root();
}

Status SegmentIterator::init_iterators() {
    RETURN_IF_ERROR(_init_return_column_iterators());
    RETURN_IF_ERROR(_init_bitmap_index_iterators());
    RETURN_IF_ERROR(_init_inverted_index_iterators());
    return Status::OK();
}

Status SegmentIterator::_lazy_init() {
    SCOPED_RAW_TIMER(&_opts.stats->block_init_ns);
    DorisMetrics::instance()->segment_read_total->increment(1);
    _row_bitmap.addRange(0, _segment->num_rows());
    // z-order can not use prefix index
    if (_segment->_tablet_schema->sort_type() != SortType::ZORDER &&
        _segment->_tablet_schema->cluster_key_idxes().empty()) {
        RETURN_IF_ERROR(_get_row_ranges_by_keys());
    }
    RETURN_IF_ERROR(_get_row_ranges_by_column_conditions());
    RETURN_IF_ERROR(_vec_init_lazy_materialization());
    // Remove rows that have been marked deleted
    if (_opts.delete_bitmap.count(segment_id()) > 0 &&
        _opts.delete_bitmap.at(segment_id()) != nullptr) {
        size_t pre_size = _row_bitmap.cardinality();
        _row_bitmap -= *(_opts.delete_bitmap.at(segment_id()));
        _opts.stats->rows_del_by_bitmap += (pre_size - _row_bitmap.cardinality());
        VLOG_DEBUG << "read on segment: " << segment_id() << ", delete bitmap cardinality: "
                   << _opts.delete_bitmap.at(segment_id())->cardinality() << ", "
                   << _opts.stats->rows_del_by_bitmap << " rows deleted by bitmap";
    }

    if (!_opts.row_ranges.is_empty()) {
        _row_bitmap &= RowRanges::ranges_to_roaring(_opts.row_ranges);
    }
    if (_opts.read_orderby_key_reverse) {
        _range_iter.reset(new BackwardBitmapRangeIterator(_row_bitmap));
    } else {
        _range_iter.reset(new BitmapRangeIterator(_row_bitmap));
    }
    return Status::OK();
}

Status SegmentIterator::_get_row_ranges_by_keys() {
    DorisMetrics::instance()->segment_row_total->increment(num_rows());

    // fast path for empty segment or empty key ranges
    if (_row_bitmap.isEmpty() || _opts.key_ranges.empty()) {
        return Status::OK();
    }

    // Read & seek key columns is a waste of time when no key column in _schema
    if (std::none_of(_schema->columns().begin(), _schema->columns().end(), [&](const Field* col) {
            return col && _opts.tablet_schema->column_by_uid(col->unique_id()).is_key();
        })) {
        return Status::OK();
    }

    RowRanges result_ranges;
    for (auto& key_range : _opts.key_ranges) {
        rowid_t lower_rowid = 0;
        rowid_t upper_rowid = num_rows();
        RETURN_IF_ERROR(_prepare_seek(key_range));
        if (key_range.upper_key != nullptr) {
            // If client want to read upper_bound, the include_upper is true. So we
            // should get the first ordinal at which key is larger than upper_bound.
            // So we call _lookup_ordinal with include_upper's negate
            RETURN_IF_ERROR(_lookup_ordinal(*key_range.upper_key, !key_range.include_upper,
                                            num_rows(), &upper_rowid));
        }
        if (upper_rowid > 0 && key_range.lower_key != nullptr) {
            RETURN_IF_ERROR(_lookup_ordinal(*key_range.lower_key, key_range.include_lower,
                                            upper_rowid, &lower_rowid));
        }
        auto row_range = RowRanges::create_single(lower_rowid, upper_rowid);
        RowRanges::ranges_union(result_ranges, row_range, &result_ranges);
    }
    // pre-condition: _row_ranges == [0, num_rows)
    size_t pre_size = _row_bitmap.cardinality();
    _row_bitmap = RowRanges::ranges_to_roaring(result_ranges);
    _opts.stats->rows_key_range_filtered += (pre_size - _row_bitmap.cardinality());

    return Status::OK();
}

// Set up environment for the following seek.
Status SegmentIterator::_prepare_seek(const StorageReadOptions::KeyRange& key_range) {
    std::vector<const Field*> key_fields;
    std::set<uint32_t> column_set;
    if (key_range.lower_key != nullptr) {
        for (auto cid : key_range.lower_key->schema()->column_ids()) {
            column_set.emplace(cid);
            key_fields.emplace_back(key_range.lower_key->column_schema(cid));
        }
    }
    if (key_range.upper_key != nullptr) {
        for (auto cid : key_range.upper_key->schema()->column_ids()) {
            if (column_set.count(cid) == 0) {
                key_fields.emplace_back(key_range.upper_key->column_schema(cid));
                column_set.emplace(cid);
            }
        }
    }
    if (!_seek_schema) {
        _seek_schema = std::make_unique<Schema>(key_fields, key_fields.size());
    }
    // todo(wb) need refactor here, when using pk to search, _seek_block is useless
    if (_seek_block.size() == 0) {
        _seek_block.resize(_seek_schema->num_column_ids());
        int i = 0;
        for (auto cid : _seek_schema->column_ids()) {
            auto column_desc = _seek_schema->column(cid);
            _seek_block[i] = Schema::get_column_by_field(*column_desc);
            i++;
        }
    }

    // create used column iterator
    for (auto cid : _seek_schema->column_ids()) {
        if (_column_iterators[cid] == nullptr) {
            RETURN_IF_ERROR(_segment->new_column_iterator(_opts.tablet_schema->column(cid),
                                                          &_column_iterators[cid], &_opts));
            ColumnIteratorOptions iter_opts {
                    .use_page_cache = _opts.use_page_cache,
                    .file_reader = _file_reader.get(),
                    .stats = _opts.stats,
                    .io_ctx = _opts.io_ctx,
            };
            RETURN_IF_ERROR(_column_iterators[cid]->init(iter_opts));
        }
    }

    return Status::OK();
}

Status SegmentIterator::_get_row_ranges_by_column_conditions() {
    SCOPED_RAW_TIMER(&_opts.stats->block_conditions_filtered_ns);
    if (_row_bitmap.isEmpty()) {
        return Status::OK();
    }

    RETURN_IF_ERROR(_apply_bitmap_index());
    {
        if (_opts.runtime_state &&
            _opts.runtime_state->query_options().enable_inverted_index_query &&
            has_inverted_index_in_iterators()) {
            SCOPED_RAW_TIMER(&_opts.stats->inverted_index_filter_timer);
            size_t input_rows = _row_bitmap.cardinality();
            RETURN_IF_ERROR(_apply_inverted_index());
            RETURN_IF_ERROR(_apply_index_expr());
            for (auto it = _common_expr_ctxs_push_down.begin();
                 it != _common_expr_ctxs_push_down.end();) {
                if ((*it)->all_expr_inverted_index_evaluated()) {
                    const auto* result =
                            (*it)->get_inverted_index_context()->get_inverted_index_result_for_expr(
                                    (*it)->root().get());
                    if (result != nullptr) {
                        _row_bitmap &= *result->get_data_bitmap();
                        auto root = (*it)->root();
                        auto iter_find = std::find(_remaining_conjunct_roots.begin(),
                                                   _remaining_conjunct_roots.end(), root);
                        if (iter_find != _remaining_conjunct_roots.end()) {
                            _remaining_conjunct_roots.erase(iter_find);
                        }
                        it = _common_expr_ctxs_push_down.erase(it);
                    }
                } else {
                    ++it;
                }
            }
            _opts.stats->rows_inverted_index_filtered += (input_rows - _row_bitmap.cardinality());
            for (auto cid : _schema->column_ids()) {
                bool result_true = _check_all_conditions_passed_inverted_index_for_column(cid);

                if (result_true) {
                    _need_read_data_indices[cid] = false;
                }
            }
        }
    }
    if (!_row_bitmap.isEmpty() &&
        (_opts.use_topn_opt || !_opts.col_id_to_predicates.empty() ||
         _opts.delete_condition_predicates->num_of_column_predicate() > 0)) {
        RowRanges condition_row_ranges = RowRanges::create_single(_segment->num_rows());
        RETURN_IF_ERROR(_get_row_ranges_from_conditions(&condition_row_ranges));
        size_t pre_size = _row_bitmap.cardinality();
        _row_bitmap &= RowRanges::ranges_to_roaring(condition_row_ranges);
        _opts.stats->rows_conditions_filtered += (pre_size - _row_bitmap.cardinality());
    }

    // TODO(hkp): calculate filter rate to decide whether to
    // use zone map/bloom filter/secondary index or not.
    return Status::OK();
}

Status SegmentIterator::_get_row_ranges_from_conditions(RowRanges* condition_row_ranges) {
    std::set<int32_t> cids;
    for (auto& entry : _opts.col_id_to_predicates) {
        cids.insert(entry.first);
    }

    size_t pre_size = 0;

    {
        SCOPED_RAW_TIMER(&_opts.stats->block_conditions_filtered_bf_ns);
        // first filter data by bloom filter index
        // bloom filter index only use CondColumn
        RowRanges bf_row_ranges = RowRanges::create_single(num_rows());
        for (auto& cid : cids) {
            DCHECK(_opts.col_id_to_predicates.count(cid) > 0);
            if (!_segment->can_apply_predicate_safely(cid, _opts.col_id_to_predicates.at(cid).get(),
                                                      *_schema, _opts.io_ctx.reader_type)) {
                continue;
            }
            // get row ranges by bf index of this column,
            RowRanges column_bf_row_ranges = RowRanges::create_single(num_rows());
            RETURN_IF_ERROR(_column_iterators[cid]->get_row_ranges_by_bloom_filter(
                    _opts.col_id_to_predicates.at(cid).get(), &column_bf_row_ranges));
            RowRanges::ranges_intersection(bf_row_ranges, column_bf_row_ranges, &bf_row_ranges);
        }

        pre_size = condition_row_ranges->count();
        RowRanges::ranges_intersection(*condition_row_ranges, bf_row_ranges, condition_row_ranges);
        _opts.stats->rows_bf_filtered += (pre_size - condition_row_ranges->count());

        DBUG_EXECUTE_IF("bloom_filter_must_filter_data", {
            if (pre_size - condition_row_ranges->count() == 0) {
                return Status::Error<ErrorCode::INTERNAL_ERROR>(
                        "Bloom filter did not filter the data.");
            }
        })
    }

    {
        SCOPED_RAW_TIMER(&_opts.stats->block_conditions_filtered_zonemap_ns);
        RowRanges zone_map_row_ranges = RowRanges::create_single(num_rows());
        // second filter data by zone map
        for (auto& cid : cids) {
            DCHECK(_opts.col_id_to_predicates.count(cid) > 0);
            if (!_segment->can_apply_predicate_safely(cid, _opts.col_id_to_predicates.at(cid).get(),
                                                      *_schema, _opts.io_ctx.reader_type)) {
                continue;
            }
            // do not check zonemap if predicate does not support zonemap
            if (!_opts.col_id_to_predicates.at(cid)->support_zonemap()) {
                VLOG_DEBUG << "skip zonemap for column " << cid;
                continue;
            }
            // get row ranges by zone map of this column,
            RowRanges column_row_ranges = RowRanges::create_single(num_rows());
            RETURN_IF_ERROR(_column_iterators[cid]->get_row_ranges_by_zone_map(
                    _opts.col_id_to_predicates.at(cid).get(),
                    _opts.del_predicates_for_zone_map.count(cid) > 0
                            ? &(_opts.del_predicates_for_zone_map.at(cid))
                            : nullptr,
                    &column_row_ranges));
            // intersect different columns's row ranges to get final row ranges by zone map
            RowRanges::ranges_intersection(zone_map_row_ranges, column_row_ranges,
                                           &zone_map_row_ranges);
        }

        pre_size = condition_row_ranges->count();
        RowRanges::ranges_intersection(*condition_row_ranges, zone_map_row_ranges,
                                       condition_row_ranges);

        if (_opts.use_topn_opt) {
            SCOPED_RAW_TIMER(&_opts.stats->block_conditions_filtered_zonemap_ns);
            auto* query_ctx = _opts.runtime_state->get_query_ctx();
            for (int id : _opts.topn_filter_source_node_ids) {
                if (!query_ctx->get_runtime_predicate(id).need_update()) {
                    continue;
                }

                std::shared_ptr<doris::ColumnPredicate> runtime_predicate =
                        query_ctx->get_runtime_predicate(id).get_predicate();
                if (_segment->can_apply_predicate_safely(runtime_predicate->column_id(),
                                                         runtime_predicate.get(), *_schema,
                                                         _opts.io_ctx.reader_type)) {
                    AndBlockColumnPredicate and_predicate;
                    and_predicate.add_column_predicate(
                            SingleColumnBlockPredicate::create_unique(runtime_predicate.get()));

                    RowRanges column_rp_row_ranges = RowRanges::create_single(num_rows());
                    RETURN_IF_ERROR(_column_iterators[runtime_predicate->column_id()]
                                            ->get_row_ranges_by_zone_map(&and_predicate, nullptr,
                                                                         &column_rp_row_ranges));

                    // intersect different columns's row ranges to get final row ranges by zone map
                    RowRanges::ranges_intersection(zone_map_row_ranges, column_rp_row_ranges,
                                                   &zone_map_row_ranges);
                }
            }
        }

        size_t pre_size2 = condition_row_ranges->count();
        RowRanges::ranges_intersection(*condition_row_ranges, zone_map_row_ranges,
                                       condition_row_ranges);
        _opts.stats->rows_stats_rp_filtered += (pre_size2 - condition_row_ranges->count());
        _opts.stats->rows_stats_filtered += (pre_size - condition_row_ranges->count());
    }

    {
        SCOPED_RAW_TIMER(&_opts.stats->block_conditions_filtered_dict_ns);
        /// Low cardinality optimization is currently not very stable, so to prevent data corruption,
        /// we are temporarily disabling its use in data compaction.
        if (_opts.io_ctx.reader_type == ReaderType::READER_QUERY) {
            RowRanges dict_row_ranges = RowRanges::create_single(num_rows());
            for (auto cid : cids) {
                if (!_segment->can_apply_predicate_safely(cid,
                                                          _opts.col_id_to_predicates.at(cid).get(),
                                                          *_schema, _opts.io_ctx.reader_type)) {
                    continue;
                }
                RowRanges tmp_row_ranges = RowRanges::create_single(num_rows());
                DCHECK(_opts.col_id_to_predicates.count(cid) > 0);
                RETURN_IF_ERROR(_column_iterators[cid]->get_row_ranges_by_dict(
                        _opts.col_id_to_predicates.at(cid).get(), &tmp_row_ranges));
                RowRanges::ranges_intersection(dict_row_ranges, tmp_row_ranges, &dict_row_ranges);
            }

            pre_size = condition_row_ranges->count();
            RowRanges::ranges_intersection(*condition_row_ranges, dict_row_ranges,
                                           condition_row_ranges);
            _opts.stats->rows_dict_filtered += (pre_size - condition_row_ranges->count());
        }
    }

    return Status::OK();
}

// filter rows by evaluating column predicates using bitmap indexes.
// upon return, predicates that've been evaluated by bitmap indexes are removed from _col_predicates.
Status SegmentIterator::_apply_bitmap_index() {
    SCOPED_RAW_TIMER(&_opts.stats->bitmap_index_filter_timer);
    size_t input_rows = _row_bitmap.cardinality();

    std::vector<ColumnPredicate*> remaining_predicates;
    auto is_like_predicate = [](ColumnPredicate* _pred) {
        if (dynamic_cast<LikeColumnPredicate<TYPE_CHAR>*>(_pred) != nullptr ||
            dynamic_cast<LikeColumnPredicate<TYPE_STRING>*>(_pred) != nullptr) {
            return true;
        }

        return false;
    };
    for (auto pred : _col_predicates) {
        auto cid = pred->column_id();
        if (_bitmap_index_iterators[cid] == nullptr || pred->type() == PredicateType::BF ||
            is_like_predicate(pred)) {
            // no bitmap index for this column
            remaining_predicates.push_back(pred);
        } else {
            RETURN_IF_ERROR(pred->evaluate(_bitmap_index_iterators[cid].get(), _segment->num_rows(),
                                           &_row_bitmap));
            if (_row_bitmap.isEmpty()) {
                break; // all rows have been pruned, no need to process further predicates
            }
        }
    }
    _col_predicates = std::move(remaining_predicates);
    _opts.stats->rows_bitmap_index_filtered += (input_rows - _row_bitmap.cardinality());
    return Status::OK();
}

bool SegmentIterator::_is_literal_node(const TExprNodeType::type& node_type) {
    switch (node_type) {
    case TExprNodeType::BOOL_LITERAL:
    case TExprNodeType::INT_LITERAL:
    case TExprNodeType::LARGE_INT_LITERAL:
    case TExprNodeType::FLOAT_LITERAL:
    case TExprNodeType::DECIMAL_LITERAL:
    case TExprNodeType::STRING_LITERAL:
    case TExprNodeType::DATE_LITERAL:
        return true;
    default:
        return false;
    }
}

Status SegmentIterator::_extract_common_expr_columns(const vectorized::VExprSPtr& expr) {
    auto& children = expr->children();
    for (int i = 0; i < children.size(); ++i) {
        RETURN_IF_ERROR(_extract_common_expr_columns(children[i]));
    }

    auto node_type = expr->node_type();
    if (node_type == TExprNodeType::SLOT_REF) {
        auto slot_expr = std::dynamic_pointer_cast<doris::vectorized::VSlotRef>(expr);
        _is_common_expr_column[_schema->column_id(slot_expr->column_id())] = true;
        _common_expr_columns.insert(_schema->column_id(slot_expr->column_id()));
    }

    return Status::OK();
}

bool SegmentIterator::_check_apply_by_inverted_index(ColumnPredicate* pred) {
    if (_opts.runtime_state && !_opts.runtime_state->query_options().enable_inverted_index_query) {
        return false;
    }
    auto pred_column_id = pred->column_id();
    if (_inverted_index_iterators[pred_column_id] == nullptr) {
        //this column without inverted index
        return false;
    }

    if (_inverted_index_not_support_pred_type(pred->type())) {
        return false;
    }

    if (pred->type() == PredicateType::IN_LIST || pred->type() == PredicateType::NOT_IN_LIST) {
        auto predicate_param = pred->predicate_params();
        // in_list or not_in_list predicate produced by runtime filter
        if (predicate_param->marked_by_runtime_filter) {
            return false;
        }
    }

    // UNTOKENIZED strings exceed ignore_above, they are written as null, causing range query errors
    if (PredicateTypeTraits::is_range(pred->type()) &&
        _inverted_index_iterators[pred_column_id] != nullptr &&
        _inverted_index_iterators[pred_column_id]->get_inverted_index_reader_type() ==
                InvertedIndexReaderType::STRING_TYPE) {
        return false;
    }

    // Function filter no apply inverted index
    if (dynamic_cast<LikeColumnPredicate<TYPE_CHAR>*>(pred) != nullptr ||
        dynamic_cast<LikeColumnPredicate<TYPE_STRING>*>(pred) != nullptr) {
        return false;
    }

    bool handle_by_fulltext = _column_has_fulltext_index(pred_column_id);
    if (handle_by_fulltext) {
        // when predicate is leafNode of andNode,
        // can apply 'match query' and 'equal query' and 'list query' for fulltext index.
        return pred->type() == PredicateType::MATCH || pred->type() == PredicateType::IS_NULL ||
               pred->type() == PredicateType::IS_NOT_NULL ||
               PredicateTypeTraits::is_equal_or_list(pred->type());
    }

    return true;
}

Status SegmentIterator::_apply_index_expr() {
    for (const auto& expr_ctx : _common_expr_ctxs_push_down) {
        if (Status st = expr_ctx->evaluate_inverted_index(num_rows()); !st.ok()) {
            if (_downgrade_without_index(st) || st.code() == ErrorCode::NOT_IMPLEMENTED_ERROR) {
                continue;
            } else {
                // other code is not to be handled, we should just break
                LOG(WARNING) << "failed to evaluate inverted index for expr_ctx: "
                             << expr_ctx->root()->debug_string()
                             << ", error msg: " << st.to_string();
                return st;
            }
        }
    }
    return Status::OK();
}

bool SegmentIterator::_downgrade_without_index(Status res, bool need_remaining) {
    bool is_fallback =
            _opts.runtime_state->query_options().enable_fallback_on_missing_inverted_index;
    if ((res.code() == ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND && is_fallback) ||
        res.code() == ErrorCode::INVERTED_INDEX_BYPASS ||
        res.code() == ErrorCode::INVERTED_INDEX_EVALUATE_SKIPPED ||
        (res.code() == ErrorCode::INVERTED_INDEX_NO_TERMS && need_remaining)) {
        // 1. INVERTED_INDEX_FILE_NOT_FOUND means index file has not been built,
        //    usually occurs when creating a new index, queries can be downgraded
        //    without index.
        // 2. INVERTED_INDEX_BYPASS means the hit of condition by index
        //    has reached the optimal limit, downgrade without index query can
        //    improve query performance.
        // 3. INVERTED_INDEX_EVALUATE_SKIPPED means the inverted index is not
        //    suitable for executing this predicate, skipped it and filter data
        //    by function later.
        // 4. INVERTED_INDEX_NO_TERMS means the column has fulltext index,
        //    but the column condition value no terms in specified parser,
        //    such as: where A = '' and B = ','
        //    the predicate of A and B need downgrade without index query.
        // above case can downgrade without index query
        LOG(INFO) << "will downgrade without index to evaluate predicate, because of res: " << res;
        return true;
    }
    return false;
}

bool SegmentIterator::_column_has_fulltext_index(int32_t cid) {
    bool has_fulltext_index = _inverted_index_iterators[cid] != nullptr &&
                              _inverted_index_iterators[cid]->get_inverted_index_reader_type() ==
                                      InvertedIndexReaderType::FULLTEXT;

    return has_fulltext_index;
}

inline bool SegmentIterator::_inverted_index_not_support_pred_type(const PredicateType& type) {
    return type == PredicateType::BF || type == PredicateType::BITMAP_FILTER;
}

Status SegmentIterator::_apply_inverted_index_on_column_predicate(
        ColumnPredicate* pred, std::vector<ColumnPredicate*>& remaining_predicates,
        bool* continue_apply) {
    if (!_check_apply_by_inverted_index(pred)) {
        remaining_predicates.emplace_back(pred);
    } else {
        bool need_remaining_after_evaluate = _column_has_fulltext_index(pred->column_id()) &&
                                             PredicateTypeTraits::is_equal_or_list(pred->type());
        Status res = pred->evaluate(_storage_name_and_type[pred->column_id()],
                                    _inverted_index_iterators[pred->column_id()].get(), num_rows(),
                                    &_row_bitmap);
        if (!res.ok()) {
            if (_downgrade_without_index(res, need_remaining_after_evaluate)) {
                remaining_predicates.emplace_back(pred);
                return Status::OK();
            }
            LOG(WARNING) << "failed to evaluate index"
                         << ", column predicate type: " << pred->pred_type_string(pred->type())
                         << ", error msg: " << res;
            return res;
        }

        if (_row_bitmap.isEmpty()) {
            // all rows have been pruned, no need to process further predicates
            *continue_apply = false;
        }

        if (need_remaining_after_evaluate) {
            remaining_predicates.emplace_back(pred);
            return Status::OK();
        }
        if (!pred->predicate_params()->marked_by_runtime_filter) {
            _column_predicate_inverted_index_status[pred->column_id()][pred] = true;
        }
    }
    return Status::OK();
}

bool SegmentIterator::_need_read_data(ColumnId cid) {
    if (_opts.runtime_state && !_opts.runtime_state->query_options().enable_no_need_read_data_opt) {
        return true;
    }
    // only support DUP_KEYS and UNIQUE_KEYS with MOW
    if (!((_opts.tablet_schema->keys_type() == KeysType::DUP_KEYS ||
           (_opts.tablet_schema->keys_type() == KeysType::UNIQUE_KEYS &&
            _opts.enable_unique_key_merge_on_write)))) {
        return true;
    }
    // if there is delete predicate, we always need to read data
    if (_has_delete_predicate(cid)) {
        return true;
    }
    if (_output_columns.count(-1)) {
        // if _output_columns contains -1, it means that the light
        // weight schema change may not be enabled or other reasons
        // caused the column unique_id not be set, to prevent errors
        // occurring, return true here that column data needs to be read
        return true;
    }
    // Check the following conditions:
    // 1. If the column represented by the unique ID is an inverted index column (indicated by '_need_read_data_indices.count(unique_id) > 0 && !_need_read_data_indices[unique_id]')
    //    and it's not marked for projection in '_output_columns'.
    // 2. Or, if the column is an inverted index column and it's marked for projection in '_output_columns',
    //    and the operation is a push down of the 'COUNT_ON_INDEX' aggregation function.
    // If any of the above conditions are met, log a debug message indicating that there's no need to read data for the indexed column.
    // Then, return false.
    int32_t unique_id = _opts.tablet_schema->column(cid).unique_id();
    if ((_need_read_data_indices.count(cid) > 0 && !_need_read_data_indices[cid] &&
         _output_columns.count(unique_id) < 1) ||
        (_need_read_data_indices.count(cid) > 0 && !_need_read_data_indices[cid] &&
         _output_columns.count(unique_id) == 1 &&
         _opts.push_down_agg_type_opt == TPushAggOp::COUNT_ON_INDEX)) {
        VLOG_DEBUG << "SegmentIterator no need read data for column: "
                   << _opts.tablet_schema->column_by_uid(unique_id).name();
        return false;
    }
    return true;
}

Status SegmentIterator::_apply_inverted_index() {
    std::vector<ColumnPredicate*> remaining_predicates;
    std::set<const ColumnPredicate*> no_need_to_pass_column_predicate_set;

    for (auto pred : _col_predicates) {
        if (no_need_to_pass_column_predicate_set.count(pred) > 0) {
            continue;
        } else {
            bool continue_apply = true;
            RETURN_IF_ERROR(_apply_inverted_index_on_column_predicate(pred, remaining_predicates,
                                                                      &continue_apply));
            if (!continue_apply) {
                break;
            }
        }
    }

    _col_predicates = std::move(remaining_predicates);
    return Status::OK();
}

/**
 * @brief Checks if all conditions related to a specific column have passed in both
 * `_column_predicate_inverted_index_status` and `_common_expr_inverted_index_status`.
 *
 * This function first checks the conditions in `_column_predicate_inverted_index_status`
 * for the given `ColumnId`. If all conditions pass, it sets `default_return` to `true`.
 * It then checks the conditions in `_common_expr_inverted_index_status` for the same column.
 *
 * The function returns `true` if all conditions in both maps pass. If any condition fails
 * in either map, the function immediately returns `false`. If the column does not exist
 * in one of the maps, the function returns `default_return`.
 *
 * @param cid The ColumnId of the column to check.
 * @param default_return The default value to return if the column is not found in the status maps.
 * @return true if all conditions in both status maps pass, or if the column is not found
 *         and `default_return` is true.
 * @return false if any condition in either status map fails, or if the column is not found
 *         and `default_return` is false.
 */
bool SegmentIterator::_check_all_conditions_passed_inverted_index_for_column(ColumnId cid,
                                                                             bool default_return) {
    // If common_expr_pushdown is disabled, we cannot guarantee that all conditions are processed by the inverted index.
    // Consider a scenario where there is a column predicate and an expression involving the same column in the SQL query,
    // such as 'a < 0' and 'abs(a) > 1'. This could potentially lead to errors.
    if (_opts.runtime_state && !_opts.runtime_state->query_options().enable_common_expr_pushdown) {
        return false;
    }
    auto pred_it = _column_predicate_inverted_index_status.find(cid);
    if (pred_it != _column_predicate_inverted_index_status.end()) {
        const auto& pred_map = pred_it->second;
        bool pred_passed = std::all_of(pred_map.begin(), pred_map.end(),
                                       [](const auto& pred_entry) { return pred_entry.second; });
        if (!pred_passed) {
            return false;
        } else {
            default_return = true;
        }
    }

    auto expr_it = _common_expr_inverted_index_status.find(cid);
    if (expr_it != _common_expr_inverted_index_status.end()) {
        const auto& expr_map = expr_it->second;
        return std::all_of(expr_map.begin(), expr_map.end(),
                           [](const auto& expr_entry) { return expr_entry.second; });
    }
    return default_return;
}

Status SegmentIterator::_init_return_column_iterators() {
    if (_cur_rowid >= num_rows()) {
        return Status::OK();
    }

    for (auto cid : _schema->column_ids()) {
        if (_schema->column(cid)->name() == BeConsts::ROWID_COL) {
            _column_iterators[cid].reset(
                    new RowIdColumnIterator(_opts.tablet_id, _opts.rowset_id, _segment->id()));
            continue;
        }
        std::set<ColumnId> del_cond_id_set;
        _opts.delete_condition_predicates->get_all_column_ids(del_cond_id_set);
        std::vector<bool> tmp_is_pred_column;
        tmp_is_pred_column.resize(_schema->columns().size(), false);
        for (auto predicate : _col_predicates) {
            auto p_cid = predicate->column_id();
            tmp_is_pred_column[p_cid] = true;
        }
        // handle delete_condition
        for (auto d_cid : del_cond_id_set) {
            tmp_is_pred_column[d_cid] = true;
        }

        if (_column_iterators[cid] == nullptr) {
            RETURN_IF_ERROR(_segment->new_column_iterator(_opts.tablet_schema->column(cid),
                                                          &_column_iterators[cid], &_opts));
            ColumnIteratorOptions iter_opts {
                    .use_page_cache = _opts.use_page_cache,
                    // If the col is predicate column, then should read the last page to check
                    // if the column is full dict encoding
                    .is_predicate_column = tmp_is_pred_column[cid],
                    .file_reader = _file_reader.get(),
                    .stats = _opts.stats,
                    .io_ctx = _opts.io_ctx,
            };
            RETURN_IF_ERROR(_column_iterators[cid]->init(iter_opts));
        }
    }
    return Status::OK();
}

Status SegmentIterator::_init_bitmap_index_iterators() {
    if (_cur_rowid >= num_rows()) {
        return Status::OK();
    }
    for (auto cid : _schema->column_ids()) {
        if (_bitmap_index_iterators[cid] == nullptr) {
            RETURN_IF_ERROR(_segment->new_bitmap_index_iterator(_opts.tablet_schema->column(cid),
                                                                &_bitmap_index_iterators[cid]));
        }
    }
    return Status::OK();
}

Status SegmentIterator::_init_inverted_index_iterators() {
    if (_cur_rowid >= num_rows()) {
        return Status::OK();
    }
    for (auto cid : _schema->column_ids()) {
        if (_inverted_index_iterators[cid] == nullptr) {
            // Not check type valid, since we need to get inverted index for related variant type when reading the segment.
            // If check type valid, we can not get inverted index for variant type, and result nullptr.The result for calling
            // get_inverted_index with variant suffix should return corresponding inverted index meta.
            bool check_inverted_index_by_type = false;
            // Use segment’s own index_meta, for compatibility with future indexing needs to default to lowercase.
            RETURN_IF_ERROR(_segment->new_inverted_index_iterator(
                    _opts.tablet_schema->column(cid),
                    _segment->_tablet_schema->get_inverted_index(_opts.tablet_schema->column(cid),
                                                                 check_inverted_index_by_type),
                    _opts, &_inverted_index_iterators[cid]));
        }
    }
    return Status::OK();
}

Status SegmentIterator::_lookup_ordinal(const RowCursor& key, bool is_include, rowid_t upper_bound,
                                        rowid_t* rowid) {
    if (_segment->_tablet_schema->keys_type() == UNIQUE_KEYS &&
        _segment->get_primary_key_index() != nullptr) {
        return _lookup_ordinal_from_pk_index(key, is_include, rowid);
    }
    return _lookup_ordinal_from_sk_index(key, is_include, upper_bound, rowid);
}

// look up one key to get its ordinal at which can get data by using short key index.
// 'upper_bound' is defined the max ordinal the function will search.
// We use upper_bound to reduce search times.
// If we find a valid ordinal, it will be set in rowid and with Status::OK()
// If we can not find a valid key in this segment, we will set rowid to upper_bound
// Otherwise return error.
// 1. get [start, end) ordinal through short key index
// 2. binary search to find exact ordinal that match the input condition
// Make is_include template to reduce branch
Status SegmentIterator::_lookup_ordinal_from_sk_index(const RowCursor& key, bool is_include,
                                                      rowid_t upper_bound, rowid_t* rowid) {
    const ShortKeyIndexDecoder* sk_index_decoder = _segment->get_short_key_index();
    DCHECK(sk_index_decoder != nullptr);

    std::string index_key;
    encode_key_with_padding(&index_key, key, _segment->_tablet_schema->num_short_key_columns(),
                            is_include);

    const auto& key_col_ids = key.schema()->column_ids();
    _convert_rowcursor_to_short_key(key, key_col_ids.size());

    uint32_t start_block_id = 0;
    auto start_iter = sk_index_decoder->lower_bound(index_key);
    if (start_iter.valid()) {
        // Because previous block may contain this key, so we should set rowid to
        // last block's first row.
        start_block_id = start_iter.ordinal();
        if (start_block_id > 0) {
            start_block_id--;
        }
    } else {
        // When we don't find a valid index item, which means all short key is
        // smaller than input key, this means that this key may exist in the last
        // row block. so we set the rowid to first row of last row block.
        start_block_id = sk_index_decoder->num_items() - 1;
    }
    rowid_t start = start_block_id * sk_index_decoder->num_rows_per_block();

    rowid_t end = upper_bound;
    auto end_iter = sk_index_decoder->upper_bound(index_key);
    if (end_iter.valid()) {
        end = end_iter.ordinal() * sk_index_decoder->num_rows_per_block();
    }

    // binary search to find the exact key
    while (start < end) {
        rowid_t mid = (start + end) / 2;
        RETURN_IF_ERROR(_seek_and_peek(mid));
        int cmp = _compare_short_key_with_seek_block(key_col_ids);
        if (cmp > 0) {
            start = mid + 1;
        } else if (cmp == 0) {
            if (is_include) {
                // lower bound
                end = mid;
            } else {
                // upper bound
                start = mid + 1;
            }
        } else {
            end = mid;
        }
    }

    *rowid = start;
    return Status::OK();
}

Status SegmentIterator::_lookup_ordinal_from_pk_index(const RowCursor& key, bool is_include,
                                                      rowid_t* rowid) {
    DCHECK(_segment->_tablet_schema->keys_type() == UNIQUE_KEYS);
    const PrimaryKeyIndexReader* pk_index_reader = _segment->get_primary_key_index();
    DCHECK(pk_index_reader != nullptr);

    std::string index_key;
    encode_key_with_padding<RowCursor, true>(
            &index_key, key, _segment->_tablet_schema->num_key_columns(), is_include);
    if (index_key < _segment->min_key()) {
        *rowid = 0;
        return Status::OK();
    } else if (index_key > _segment->max_key()) {
        *rowid = num_rows();
        return Status::OK();
    }
    bool exact_match = false;

    std::unique_ptr<segment_v2::IndexedColumnIterator> index_iterator;
    RETURN_IF_ERROR(pk_index_reader->new_iterator(&index_iterator));

    Status status = index_iterator->seek_at_or_after(&index_key, &exact_match);
    if (UNLIKELY(!status.ok())) {
        *rowid = num_rows();
        if (status.is<ENTRY_NOT_FOUND>()) {
            return Status::OK();
        }
        return status;
    }
    *rowid = index_iterator->get_current_ordinal();

    // The sequence column needs to be removed from primary key index when comparing key
    bool has_seq_col = _segment->_tablet_schema->has_sequence_col();
    // Used to get key range from primary key index,
    // for mow with cluster key table, we should get key range from short key index.
    DCHECK(_segment->_tablet_schema->cluster_key_idxes().empty());

    // if full key is exact_match, the primary key without sequence column should also the same
    if (has_seq_col && !exact_match) {
        size_t seq_col_length =
                _segment->_tablet_schema->column(_segment->_tablet_schema->sequence_col_idx())
                        .length() +
                1;
        auto index_type = vectorized::DataTypeFactory::instance().create_data_type(
                _segment->_pk_index_reader->type_info()->type(), 1, 0);
        auto index_column = index_type->create_column();
        size_t num_to_read = 1;
        size_t num_read = num_to_read;
        RETURN_IF_ERROR(index_iterator->next_batch(&num_read, index_column));
        DCHECK(num_to_read == num_read);

        Slice sought_key =
                Slice(index_column->get_data_at(0).data, index_column->get_data_at(0).size);
        Slice sought_key_without_seq =
                Slice(sought_key.get_data(), sought_key.get_size() - seq_col_length);

        // compare key
        if (Slice(index_key).compare(sought_key_without_seq) == 0) {
            exact_match = true;
        }
    }

    // find the key in primary key index, and the is_include is false, so move
    // to the next row.
    if (exact_match && !is_include) {
        *rowid += 1;
    }
    return Status::OK();
}

// seek to the row and load that row to _key_cursor
Status SegmentIterator::_seek_and_peek(rowid_t rowid) {
    {
        _opts.stats->block_init_seek_num += 1;
        SCOPED_RAW_TIMER(&_opts.stats->block_init_seek_ns);
        RETURN_IF_ERROR(_seek_columns(_seek_schema->column_ids(), rowid));
    }
    size_t num_rows = 1;

    //note(wb) reset _seek_block for memory reuse
    // it is easier to use row based memory layout for clear memory
    for (int i = 0; i < _seek_block.size(); i++) {
        _seek_block[i]->clear();
    }
    RETURN_IF_ERROR(_read_columns(_seek_schema->column_ids(), _seek_block, num_rows));
    return Status::OK();
}

Status SegmentIterator::_seek_columns(const std::vector<ColumnId>& column_ids, rowid_t pos) {
    for (auto cid : column_ids) {
        if (!_need_read_data(cid)) {
            continue;
        }
        RETURN_IF_ERROR(_column_iterators[cid]->seek_to_ordinal(pos));
    }
    return Status::OK();
}

/* ---------------------- for vectorization implementation  ---------------------- */

/**
 *  For storage layer data type, can be measured from two perspectives:
 *  1 Whether the type can be read in a fast way(batch read using SIMD)
 *    Such as integer type and float type, this type can be read in SIMD way.
 *    For the type string/bitmap/hll, they can not be read in batch way, so read this type data is slow.
 *   If a type can be read fast, we can try to eliminate Lazy Materialization, because we think for this type, seek cost > read cost.
 *   This is an estimate, if we want more precise cost, statistics collection is necessary(this is a todo).
 *   In short, when returned non-pred columns contains string/hll/bitmap, we using Lazy Materialization.
 *   Otherwise, we disable it.
 *    
 *   When Lazy Materialization enable, we need to read column at least two times.
 *   First time to read Pred col, second time to read non-pred.
 *   Here's an interesting question to research, whether read Pred col once is the best plan.
 *   (why not read Pred col twice or more?)
 *
 *   When Lazy Materialization disable, we just need to read once.
 *   
 * 
 *  2 Whether the predicate type can be evaluate in a fast way(using SIMD to eval pred)
 *    Such as integer type and float type, they can be eval fast.
 *    But for BloomFilter/string/date, they eval slow.
 *    If a type can be eval fast, we use vectorization to eval it.
 *    Otherwise, we use short-circuit to eval it.
 * 
 *  
 */

// todo(wb) need a UT here
Status SegmentIterator::_vec_init_lazy_materialization() {
    _is_pred_column.resize(_schema->columns().size(), false);

    // including short/vec/delete pred
    std::set<ColumnId> pred_column_ids;
    _lazy_materialization_read = false;

    std::set<ColumnId> del_cond_id_set;
    _opts.delete_condition_predicates->get_all_column_ids(del_cond_id_set);

    std::set<const ColumnPredicate*> delete_predicate_set {};
    _opts.delete_condition_predicates->get_all_column_predicate(delete_predicate_set);
    for (const auto predicate : delete_predicate_set) {
        if (PredicateTypeTraits::is_range(predicate->type())) {
            _delete_range_column_ids.push_back(predicate->column_id());
        } else if (PredicateTypeTraits::is_bloom_filter(predicate->type())) {
            _delete_bloom_filter_column_ids.push_back(predicate->column_id());
        }
    }

    // add runtime predicate to _col_predicates
    // should NOT add for order by key,
    //  since key is already sorted and topn_next only need first N rows from each segment,
    //  but runtime predicate will filter some rows and read more than N rows.
    // should add add for order by none-key column, since none-key column is not sorted and
    //  all rows should be read, so runtime predicate will reduce rows for topn node
    if (_opts.use_topn_opt &&
        (_opts.read_orderby_key_columns == nullptr || _opts.read_orderby_key_columns->empty())) {
        for (int id : _opts.topn_filter_source_node_ids) {
            if (!_opts.runtime_state->get_query_ctx()->get_runtime_predicate(id).need_update()) {
                continue;
            }

            auto& runtime_predicate =
                    _opts.runtime_state->get_query_ctx()->get_runtime_predicate(id);
            _col_predicates.push_back(runtime_predicate.get_predicate().get());
        }
    }

    // Step1: extract columns that can be lazy materialization
    if (!_col_predicates.empty() || !del_cond_id_set.empty()) {
        std::set<ColumnId> short_cir_pred_col_id_set; // using set for distinct cid
        std::set<ColumnId> vec_pred_col_id_set;

        for (auto* predicate : _col_predicates) {
            auto cid = predicate->column_id();
            _is_pred_column[cid] = true;
            pred_column_ids.insert(cid);

            // check pred using short eval or vec eval
            if (_can_evaluated_by_vectorized(predicate)) {
                vec_pred_col_id_set.insert(cid);
                _pre_eval_block_predicate.push_back(predicate);
            } else {
                short_cir_pred_col_id_set.insert(cid);
                _short_cir_eval_predicate.push_back(predicate);
            }
            if (predicate->is_filter()) {
                _filter_info_id.push_back(predicate);
            }
        }

        // handle delete_condition
        if (!del_cond_id_set.empty()) {
            short_cir_pred_col_id_set.insert(del_cond_id_set.begin(), del_cond_id_set.end());
            pred_column_ids.insert(del_cond_id_set.begin(), del_cond_id_set.end());

            for (auto cid : del_cond_id_set) {
                _is_pred_column[cid] = true;
            }
        }

        _vec_pred_column_ids.assign(vec_pred_col_id_set.cbegin(), vec_pred_col_id_set.cend());
        _short_cir_pred_column_ids.assign(short_cir_pred_col_id_set.cbegin(),
                                          short_cir_pred_col_id_set.cend());
    }

    if (!_vec_pred_column_ids.empty()) {
        _is_need_vec_eval = true;
    }
    if (!_short_cir_pred_column_ids.empty()) {
        _is_need_short_eval = true;
    }

    // make _schema_block_id_map
    _schema_block_id_map.resize(_schema->columns().size());
    for (int i = 0; i < _schema->num_column_ids(); i++) {
        auto cid = _schema->column_id(i);
        _schema_block_id_map[cid] = i;
    }

    // Step2: extract columns that can execute expr context
    _is_common_expr_column.resize(_schema->columns().size(), false);
    if (_enable_common_expr_pushdown && !_remaining_conjunct_roots.empty()) {
        for (auto expr : _remaining_conjunct_roots) {
            RETURN_IF_ERROR(_extract_common_expr_columns(expr));
        }
        if (!_common_expr_columns.empty()) {
            _is_need_expr_eval = true;
            for (auto cid : _schema->column_ids()) {
                // pred column also needs to be filtered by expr, exclude additional delete condition column.
                // if delete condition column not in the block, no filter is needed
                // and will be removed from _columns_to_filter in the first next_batch.
                if (_is_common_expr_column[cid] || _is_pred_column[cid]) {
                    auto loc = _schema_block_id_map[cid];
                    _columns_to_filter.push_back(loc);
                }
            }
        }
    }

    // Step 3: fill non predicate columns and second read column
    // if _schema columns size equal to pred_column_ids size, lazy_materialization_read is false,
    // all columns are lazy materialization columns without non predicte column.
    // If common expr pushdown exists, and expr column is not contained in lazy materialization columns,
    // add to second read column, which will be read after lazy materialization
    if (_schema->column_ids().size() > pred_column_ids.size()) {
        // pred_column_ids maybe empty, so that could not set _lazy_materialization_read = true here
        // has to check there is at least one predicate column
        for (auto cid : _schema->column_ids()) {
            if (!_is_pred_column[cid]) {
                if (_is_need_vec_eval || _is_need_short_eval) {
                    _lazy_materialization_read = true;
                }
                if (!_is_common_expr_column[cid]) {
                    _non_predicate_columns.push_back(cid);
                } else {
                    _second_read_column_ids.push_back(cid);
                }
            }
        }
    }

    // Step 4: fill first read columns
    if (_lazy_materialization_read) {
        // insert pred cid to first_read_columns
        for (auto cid : pred_column_ids) {
            _first_read_column_ids.push_back(cid);
        }
    } else if (!_is_need_vec_eval && !_is_need_short_eval &&
               !_is_need_expr_eval) { // no pred exists, just read and output column
        for (int i = 0; i < _schema->num_column_ids(); i++) {
            auto cid = _schema->column_id(i);
            _first_read_column_ids.push_back(cid);
        }
    } else {
        if (_is_need_vec_eval || _is_need_short_eval) {
            // TODO To refactor, because we suppose lazy materialization is better performance.
            // pred exits, but we can eliminate lazy materialization
            // insert pred/non-pred cid to first read columns
            std::set<ColumnId> pred_id_set;
            pred_id_set.insert(_short_cir_pred_column_ids.begin(),
                               _short_cir_pred_column_ids.end());
            pred_id_set.insert(_vec_pred_column_ids.begin(), _vec_pred_column_ids.end());
            std::set<ColumnId> non_pred_set(_non_predicate_columns.begin(),
                                            _non_predicate_columns.end());

            DCHECK(_second_read_column_ids.empty());
            // _second_read_column_ids must be empty. Otherwise _lazy_materialization_read must not false.
            for (int i = 0; i < _schema->num_column_ids(); i++) {
                auto cid = _schema->column_id(i);
                if (pred_id_set.find(cid) != pred_id_set.end()) {
                    _first_read_column_ids.push_back(cid);
                }
                // In the past, if schema columns > pred columns, the _lazy_materialization_read maybe == false, but
                // we make sure using _lazy_materialization_read= true now, so these logic may never happens. I comment
                // these lines and we could delete them in the future to make the code more clear.
                // else if (non_pred_set.find(cid) != non_pred_set.end()) {
                //    _first_read_column_ids.push_back(cid);
                //    // when _lazy_materialization_read = false, non-predicate column should also be filtered by sel idx, so we regard it as pred columns
                //    _is_pred_column[cid] = true;
                // }
            }
        } else if (_is_need_expr_eval) {
            DCHECK(!_is_need_vec_eval && !_is_need_short_eval);
            for (auto cid : _common_expr_columns) {
                _first_read_column_ids.push_back(cid);
            }
        }
    }
    return Status::OK();
}

bool SegmentIterator::_can_evaluated_by_vectorized(ColumnPredicate* predicate) {
    auto cid = predicate->column_id();
    FieldType field_type = _schema->column(cid)->type();
    if (field_type == FieldType::OLAP_FIELD_TYPE_VARIANT) {
        // Use variant cast dst type
        field_type = TabletColumn::get_field_type_by_type(
                _opts.target_cast_type_for_variants[_schema->column(cid)->name()].type);
    }
    switch (predicate->type()) {
    case PredicateType::EQ:
    case PredicateType::NE:
    case PredicateType::LE:
    case PredicateType::LT:
    case PredicateType::GE:
    case PredicateType::GT: {
        if (field_type == FieldType::OLAP_FIELD_TYPE_VARCHAR ||
            field_type == FieldType::OLAP_FIELD_TYPE_CHAR ||
            field_type == FieldType::OLAP_FIELD_TYPE_STRING) {
            return config::enable_low_cardinality_optimize &&
                   _opts.io_ctx.reader_type == ReaderType::READER_QUERY &&
                   _column_iterators[cid]->is_all_dict_encoding();
        } else if (field_type == FieldType::OLAP_FIELD_TYPE_DECIMAL) {
            return false;
        }
        return true;
    }
    default:
        return false;
    }
}

bool SegmentIterator::_has_char_type(const Field& column_desc) {
    switch (column_desc.type()) {
    case FieldType::OLAP_FIELD_TYPE_CHAR:
        return true;
    case FieldType::OLAP_FIELD_TYPE_ARRAY:
        return _has_char_type(*column_desc.get_sub_field(0));
    case FieldType::OLAP_FIELD_TYPE_MAP:
        return _has_char_type(*column_desc.get_sub_field(0)) ||
               _has_char_type(*column_desc.get_sub_field(1));
    case FieldType::OLAP_FIELD_TYPE_STRUCT:
        for (int idx = 0; idx < column_desc.get_sub_field_count(); ++idx) {
            if (_has_char_type(*column_desc.get_sub_field(idx))) {
                return true;
            }
        }
        return false;
    default:
        return false;
    }
};

void SegmentIterator::_vec_init_char_column_id(vectorized::Block* block) {
    for (size_t i = 0; i < _schema->num_column_ids(); i++) {
        auto cid = _schema->column_id(i);
        const Field* column_desc = _schema->column(cid);

        // The additional deleted filter condition will be in the materialized column at the end of the block.
        // After _output_column_by_sel_idx, it will be erased, so we do not need to shrink it.
        if (i < block->columns()) {
            if (_has_char_type(*column_desc)) {
                _char_type_idx.emplace_back(i);
                if (i != 0) {
                    _char_type_idx_no_0.emplace_back(i);
                }
            }
            if (column_desc->type() == FieldType::OLAP_FIELD_TYPE_CHAR) {
                _is_char_type[cid] = true;
            }
        }
    }
}

bool SegmentIterator::_prune_column(ColumnId cid, vectorized::MutableColumnPtr& column,
                                    bool fill_defaults, size_t num_of_defaults) {
    if (_need_read_data(cid)) {
        return false;
    }
    if (!fill_defaults) {
        return true;
    }
    if (column->is_nullable()) {
        auto nullable_col_ptr = reinterpret_cast<vectorized::ColumnNullable*>(column.get());
        nullable_col_ptr->get_null_map_column().insert_many_defaults(num_of_defaults);
        nullable_col_ptr->get_nested_column_ptr()->insert_many_defaults(num_of_defaults);
    } else {
        // assert(column->is_const());
        column->insert_many_defaults(num_of_defaults);
    }
    return true;
}

Status SegmentIterator::_read_columns(const std::vector<ColumnId>& column_ids,
                                      vectorized::MutableColumns& column_block, size_t nrows) {
    for (auto cid : column_ids) {
        auto& column = column_block[cid];
        size_t rows_read = nrows;
        if (_prune_column(cid, column, true, rows_read)) {
            continue;
        }
        RETURN_IF_ERROR(_column_iterators[cid]->next_batch(&rows_read, column));
        if (nrows != rows_read) {
            return Status::Error<ErrorCode::INTERNAL_ERROR>("nrows({}) != rows_read({})", nrows,
                                                            rows_read);
        }
    }
    return Status::OK();
}

Status SegmentIterator::_init_current_block(
        vectorized::Block* block, std::vector<vectorized::MutableColumnPtr>& current_columns,
        uint32_t nrows_read_limit) {
    block->clear_column_data(_schema->num_column_ids());

    for (size_t i = 0; i < _schema->num_column_ids(); i++) {
        auto cid = _schema->column_id(i);
        const auto* column_desc = _schema->column(cid);
        if (!_is_pred_column[cid] &&
            !_segment->same_with_storage_type(
                    cid, *_schema, _opts.io_ctx.reader_type != ReaderType::READER_QUERY)) {
            // The storage layer type is different from schema needed type, so we use storage
            // type to read columns instead of schema type for safety
            auto file_column_type = _storage_name_and_type[cid].second;
            VLOG_DEBUG << fmt::format(
                    "Recreate column with expected type {}, file column type {}, col_name {}, "
                    "col_path {}",
                    block->get_by_position(i).type->get_name(), file_column_type->get_name(),
                    column_desc->name(),
                    column_desc->path() == nullptr ? "" : column_desc->path()->get_path());
            // TODO reuse
            current_columns[cid] = file_column_type->create_column();
            current_columns[cid]->reserve(nrows_read_limit);
        } else {
            // the column in block must clear() here to insert new data
            if (_is_pred_column[cid] ||
                i >= block->columns()) { //todo(wb) maybe we can release it after output block
                if (current_columns[cid].get() == nullptr) {
                    return Status::InternalError(
                            "SegmentIterator meet invalid column, id={}, name={}", cid,
                            _schema->column(cid)->name());
                }
                current_columns[cid]->clear();
            } else { // non-predicate column
                current_columns[cid] = std::move(*block->get_by_position(i).column).mutate();

                if (column_desc->type() == FieldType::OLAP_FIELD_TYPE_DATE) {
                    current_columns[cid]->set_date_type();
                } else if (column_desc->type() == FieldType::OLAP_FIELD_TYPE_DATETIME) {
                    current_columns[cid]->set_datetime_type();
                }
                current_columns[cid]->reserve(nrows_read_limit);
            }
        }
    }
    return Status::OK();
}

void SegmentIterator::_output_non_pred_columns(vectorized::Block* block) {
    SCOPED_RAW_TIMER(&_opts.stats->output_col_ns);
    for (auto cid : _non_predicate_columns) {
        auto loc = _schema_block_id_map[cid];
        // if loc > block->columns() means the column is delete column and should
        // not output by block, so just skip the column.
        if (loc < block->columns()) {
            block->replace_by_position(loc, std::move(_current_return_columns[cid]));
        }
    }
}

/**
 * Reads columns by their index, handling both continuous and discontinuous rowid scenarios.
 *
 * This function is designed to read a specified number of rows (up to nrows_read_limit)
 * from the segment iterator, dealing with both continuous and discontinuous rowid arrays.
 * It operates as follows:
 *
 * 1. Reads a batch of rowids (up to the specified limit), and checks if they are continuous.
 *    Continuous here means that the rowids form an unbroken sequence (e.g., 1, 2, 3, 4...).
 *
 * 2. For each column that needs to be read (identified by _first_read_column_ids):
 *    - If the rowids are continuous, the function uses seek_to_ordinal and next_batch
 *      for efficient reading.
 *    - If the rowids are not continuous, the function processes them in smaller batches
 *      (each of size up to 256). Each batch is checked for internal continuity:
 *        a. If a batch is continuous, uses seek_to_ordinal and next_batch for that batch.
 *        b. If a batch is not continuous, uses read_by_rowids for individual rowids in the batch.
 *
 * This approach optimizes reading performance by leveraging batch processing for continuous
 * rowid sequences and handling discontinuities gracefully in smaller chunks.
 */
Status SegmentIterator::_read_columns_by_index(uint32_t nrows_read_limit, uint32_t& nrows_read,
                                               bool set_block_rowid) {
    SCOPED_RAW_TIMER(&_opts.stats->first_read_ns);

    nrows_read = _range_iter->read_batch_rowids(_block_rowids.data(), nrows_read_limit);
    bool is_continuous = (nrows_read > 1) &&
                         (_block_rowids[nrows_read - 1] - _block_rowids[0] == nrows_read - 1);

    for (auto cid : _first_read_column_ids) {
        auto& column = _current_return_columns[cid];
        if (_no_need_read_key_data(cid, column, nrows_read)) {
            continue;
        }
        if (_prune_column(cid, column, true, nrows_read)) {
            continue;
        }

        DBUG_EXECUTE_IF("segment_iterator._read_columns_by_index", {
            auto col_name = _opts.tablet_schema->column(cid).name();
            auto debug_col_name = DebugPoints::instance()->get_debug_param_or_default<std::string>(
                    "segment_iterator._read_columns_by_index", "column_name", "");
            if (debug_col_name.empty() && col_name != "__DORIS_DELETE_SIGN__") {
                return Status::Error<ErrorCode::INTERNAL_ERROR>("does not need to read data, {}",
                                                                col_name);
            }
            if (debug_col_name.find(col_name) != std::string::npos) {
                return Status::Error<ErrorCode::INTERNAL_ERROR>("does not need to read data, {}",
                                                                col_name);
            }
        })

        if (is_continuous) {
            size_t rows_read = nrows_read;
            _opts.stats->block_first_read_seek_num += 1;
            if (_opts.runtime_state && _opts.runtime_state->enable_profile()) {
                SCOPED_RAW_TIMER(&_opts.stats->block_first_read_seek_ns);
                RETURN_IF_ERROR(_column_iterators[cid]->seek_to_ordinal(_block_rowids[0]));
            } else {
                RETURN_IF_ERROR(_column_iterators[cid]->seek_to_ordinal(_block_rowids[0]));
            }
            RETURN_IF_ERROR(_column_iterators[cid]->next_batch(&rows_read, column));
            if (rows_read != nrows_read) {
                return Status::Error<ErrorCode::INTERNAL_ERROR>("nrows({}) != rows_read({})",
                                                                nrows_read, rows_read);
            }
        } else {
            const uint32_t batch_size = _range_iter->get_batch_size();
            uint32_t processed = 0;
            while (processed < nrows_read) {
                uint32_t current_batch_size = std::min(batch_size, nrows_read - processed);
                bool batch_continuous = (current_batch_size > 1) &&
                                        (_block_rowids[processed + current_batch_size - 1] -
                                                 _block_rowids[processed] ==
                                         current_batch_size - 1);

                if (batch_continuous) {
                    size_t rows_read = current_batch_size;
                    _opts.stats->block_first_read_seek_num += 1;
                    if (_opts.runtime_state && _opts.runtime_state->enable_profile()) {
                        SCOPED_RAW_TIMER(&_opts.stats->block_first_read_seek_ns);
                        RETURN_IF_ERROR(
                                _column_iterators[cid]->seek_to_ordinal(_block_rowids[processed]));
                    } else {
                        RETURN_IF_ERROR(
                                _column_iterators[cid]->seek_to_ordinal(_block_rowids[processed]));
                    }
                    RETURN_IF_ERROR(_column_iterators[cid]->next_batch(&rows_read, column));
                    if (rows_read != current_batch_size) {
                        return Status::Error<ErrorCode::INTERNAL_ERROR>(
                                "batch nrows({}) != rows_read({})", current_batch_size, rows_read);
                    }
                } else {
                    RETURN_IF_ERROR(_column_iterators[cid]->read_by_rowids(
                            &_block_rowids[processed], current_batch_size, column));
                }
                processed += current_batch_size;
            }
        }
    }

    return Status::OK();
}

void SegmentIterator::_replace_version_col(size_t num_rows) {
    // Only the rowset with single version need to replace the version column.
    // Doris can't determine the version before publish_version finished, so
    // we can't write data to __DORIS_VERSION_COL__ in segment writer, the value
    // is 0 by default.
    // So we need to replace the value to real version while reading.
    if (_opts.version.first != _opts.version.second) {
        return;
    }
    auto cids = _schema->column_ids();
    int32_t version_idx = _schema->version_col_idx();
    auto iter = std::find(cids.begin(), cids.end(), version_idx);
    if (iter == cids.end()) {
        return;
    }

    auto column_desc = _schema->column(version_idx);
    auto column = Schema::get_data_type_ptr(*column_desc)->create_column();
    DCHECK(_schema->column(version_idx)->type() == FieldType::OLAP_FIELD_TYPE_BIGINT);
    auto col_ptr = reinterpret_cast<vectorized::ColumnVector<vectorized::Int64>*>(column.get());
    for (size_t j = 0; j < num_rows; j++) {
        col_ptr->insert_value(_opts.version.second);
    }
    _current_return_columns[version_idx] = std::move(column);
    VLOG_DEBUG << "replaced version column in segment iterator, version_col_idx:" << version_idx;
}

uint16_t SegmentIterator::_evaluate_vectorization_predicate(uint16_t* sel_rowid_idx,
                                                            uint16_t selected_size) {
    SCOPED_RAW_TIMER(&_opts.stats->vec_cond_ns);
    bool all_pred_always_true = true;
    for (const auto& pred : _pre_eval_block_predicate) {
        if (!pred->always_true()) {
            all_pred_always_true = false;
            break;
        }
    }
    //If all predicates are always_true, then return directly.
    if (all_pred_always_true || !_is_need_vec_eval) {
        for (uint16_t i = 0; i < selected_size; ++i) {
            sel_rowid_idx[i] = i;
        }
        return selected_size;
    }

    uint16_t original_size = selected_size;
    bool ret_flags[original_size];
    DCHECK(!_pre_eval_block_predicate.empty());
    bool is_first = true;
    for (auto& pred : _pre_eval_block_predicate) {
        if (pred->always_true()) {
            continue;
        }
        auto column_id = pred->column_id();
        auto& column = _current_return_columns[column_id];
        if (is_first) {
            pred->evaluate_vec(*column, original_size, ret_flags);
            is_first = false;
        } else {
            pred->evaluate_and_vec(*column, original_size, ret_flags);
        }
    }

    uint16_t new_size = 0;

    uint32_t sel_pos = 0;
    const uint32_t sel_end = sel_pos + selected_size;
    static constexpr size_t SIMD_BYTES = simd::bits_mask_length();
    const uint32_t sel_end_simd = sel_pos + selected_size / SIMD_BYTES * SIMD_BYTES;

    while (sel_pos < sel_end_simd) {
        auto mask = simd::bytes_mask_to_bits_mask((const uint8_t*)ret_flags + sel_pos);
        if (0 == mask) {
            //pass
        } else if (simd::bits_mask_all() == mask) {
            for (uint32_t i = 0; i < SIMD_BYTES; i++) {
                sel_rowid_idx[new_size++] = sel_pos + i;
            }
        } else {
            simd::iterate_through_bits_mask(
                    [&](const size_t bit_pos) { sel_rowid_idx[new_size++] = sel_pos + bit_pos; },
                    mask);
        }
        sel_pos += SIMD_BYTES;
    }

    for (; sel_pos < sel_end; sel_pos++) {
        if (ret_flags[sel_pos]) {
            sel_rowid_idx[new_size++] = sel_pos;
        }
    }

    _opts.stats->vec_cond_input_rows += original_size;
    _opts.stats->rows_vec_cond_filtered += original_size - new_size;
    return new_size;
}

uint16_t SegmentIterator::_evaluate_short_circuit_predicate(uint16_t* vec_sel_rowid_idx,
                                                            uint16_t selected_size) {
    SCOPED_RAW_TIMER(&_opts.stats->short_cond_ns);
    if (!_is_need_short_eval) {
        return selected_size;
    }

    uint16_t original_size = selected_size;
    for (auto predicate : _short_cir_eval_predicate) {
        auto column_id = predicate->column_id();
        auto& short_cir_column = _current_return_columns[column_id];
        selected_size = predicate->evaluate(*short_cir_column, vec_sel_rowid_idx, selected_size);
    }

    // collect profile
    for (auto p : _filter_info_id) {
        _opts.stats->filter_info[p->get_filter_id()] = p->get_filtered_info();
    }
    _opts.stats->short_circuit_cond_input_rows += original_size;
    _opts.stats->rows_short_circuit_cond_filtered += original_size - selected_size;

    // evaluate delete condition
    original_size = selected_size;
    selected_size = _opts.delete_condition_predicates->evaluate(_current_return_columns,
                                                                vec_sel_rowid_idx, selected_size);
    _opts.stats->rows_vec_del_cond_filtered += original_size - selected_size;
    return selected_size;
}

Status SegmentIterator::_read_columns_by_rowids(std::vector<ColumnId>& read_column_ids,
                                                std::vector<rowid_t>& rowid_vector,
                                                uint16_t* sel_rowid_idx, size_t select_size,
                                                vectorized::MutableColumns* mutable_columns) {
    SCOPED_RAW_TIMER(&_opts.stats->lazy_read_ns);
    std::vector<rowid_t> rowids(select_size);
    for (size_t i = 0; i < select_size; ++i) {
        rowids[i] = rowid_vector[sel_rowid_idx[i]];
    }

    for (auto cid : read_column_ids) {
        auto& colunm = (*mutable_columns)[cid];
        if (_no_need_read_key_data(cid, colunm, select_size)) {
            continue;
        }
        if (_prune_column(cid, colunm, true, select_size)) {
            continue;
        }

        DBUG_EXECUTE_IF("segment_iterator._read_columns_by_index", {
            auto debug_col_name = DebugPoints::instance()->get_debug_param_or_default<std::string>(
                    "segment_iterator._read_columns_by_index", "column_name", "");
            if (debug_col_name.empty()) {
                return Status::Error<ErrorCode::INTERNAL_ERROR>("does not need to read data");
            }
            auto col_name = _opts.tablet_schema->column(cid).name();
            if (debug_col_name.find(col_name) != std::string::npos) {
                return Status::Error<ErrorCode::INTERNAL_ERROR>("does not need to read data, {}",
                                                                debug_col_name);
            }
        })

        RETURN_IF_ERROR(_column_iterators[cid]->read_by_rowids(rowids.data(), select_size,
                                                               _current_return_columns[cid]));
    }

    return Status::OK();
}

Status SegmentIterator::next_batch(vectorized::Block* block) {
    auto status = [&]() {
        RETURN_IF_CATCH_EXCEPTION({
            RETURN_IF_ERROR(_next_batch_internal(block));

            // reverse block row order if read_orderby_key_reverse is true for key topn
            // it should be processed for all success _next_batch_internal
            if (_opts.read_orderby_key_reverse) {
                size_t num_rows = block->rows();
                if (num_rows == 0) {
                    return Status::OK();
                }
                size_t num_columns = block->columns();
                vectorized::IColumn::Permutation permutation;
                for (size_t i = 0; i < num_rows; ++i) permutation.emplace_back(num_rows - 1 - i);

                for (size_t i = 0; i < num_columns; ++i)
                    block->get_by_position(i).column =
                            block->get_by_position(i).column->permute(permutation, num_rows);
            }

            return Status::OK();
        });
    }();

    // if rows read by batch is 0, will return end of file, we should not remove segment cache in this situation.
    if (!status.ok() && !status.is<END_OF_FILE>()) {
        _segment->update_healthy_status(status);
    }
    return status;
}

Status SegmentIterator::_convert_to_expected_type(const std::vector<ColumnId>& col_ids) {
    for (ColumnId i : col_ids) {
        if (_current_return_columns[i] == nullptr || _converted_column_ids[i] ||
            _is_pred_column[i]) {
            continue;
        }
        if (!_segment->same_with_storage_type(
                    i, *_schema, _opts.io_ctx.reader_type != ReaderType::READER_QUERY)) {
            const Field* field_type = _schema->column(i);
            vectorized::DataTypePtr expected_type = Schema::get_data_type_ptr(*field_type);
            vectorized::DataTypePtr file_column_type = _storage_name_and_type[i].second;
            vectorized::ColumnPtr expected;
            vectorized::ColumnPtr original =
                    _current_return_columns[i]->assume_mutable()->get_ptr();
            RETURN_IF_ERROR(vectorized::schema_util::cast_column({original, file_column_type, ""},
                                                                 expected_type, &expected));
            _current_return_columns[i] = expected->assume_mutable();
            _converted_column_ids[i] = 1;
            VLOG_DEBUG << fmt::format(
                    "Convert {} fom file column type {} to {}, num_rows {}",
                    field_type->path() == nullptr ? "" : field_type->path()->get_path(),
                    file_column_type->get_name(), expected_type->get_name(),
                    _current_return_columns[i]->size());
        }
    }
    return Status::OK();
}

Status SegmentIterator::copy_column_data_by_selector(vectorized::IColumn* input_col_ptr,
                                                     vectorized::MutableColumnPtr& output_col,
                                                     uint16_t* sel_rowid_idx, uint16_t select_size,
                                                     size_t batch_size) {
    output_col->reserve(batch_size);

    // adapt for outer join change column to nullable
    if (output_col->is_nullable() && !input_col_ptr->is_nullable()) {
        auto col_ptr_nullable = reinterpret_cast<vectorized::ColumnNullable*>(output_col.get());
        col_ptr_nullable->get_null_map_column().insert_many_defaults(select_size);
        output_col = col_ptr_nullable->get_nested_column_ptr();
    } else if (!output_col->is_nullable() && input_col_ptr->is_nullable()) {
        LOG(WARNING) << "nullable mismatch for output_column: " << output_col->dump_structure()
                     << " input_column: " << input_col_ptr->dump_structure()
                     << " select_size: " << select_size;
        return Status::RuntimeError("copy_column_data_by_selector nullable mismatch");
    }

    return input_col_ptr->filter_by_selector(sel_rowid_idx, select_size, output_col);
}

void SegmentIterator::_clear_iterators() {
    _column_iterators.clear();
    _bitmap_index_iterators.clear();
    _inverted_index_iterators.clear();
}

Status SegmentIterator::_next_batch_internal(vectorized::Block* block) {
    // TEMP column in block is not allowed here, need to erase.
    block->erase_tmp_columns();
    bool is_mem_reuse = block->mem_reuse();
    DCHECK(is_mem_reuse);

    SCOPED_RAW_TIMER(&_opts.stats->block_load_ns);
    if (UNLIKELY(!_lazy_inited)) {
        RETURN_IF_ERROR(_lazy_init());
        _lazy_inited = true;
        if (_lazy_materialization_read || _opts.record_rowids || _is_need_expr_eval) {
            _block_rowids.resize(_opts.block_row_max);
        }
        _current_return_columns.resize(_schema->columns().size());
        _converted_column_ids.resize(_schema->columns().size(), 0);
        if (_char_type_idx.empty() && _char_type_idx_no_0.empty()) {
            _is_char_type.resize(_schema->columns().size(), false);
            _vec_init_char_column_id(block);
        }
        for (size_t i = 0; i < _schema->num_column_ids(); i++) {
            auto cid = _schema->column_id(i);
            auto column_desc = _schema->column(cid);
            if (_is_pred_column[cid]) {
                auto storage_column_type = _storage_name_and_type[cid].second;
                // Char type is special , since char type's computational datatype is same with string,
                // both are DataTypeString, but DataTypeString only return FieldType::OLAP_FIELD_TYPE_STRING
                // in get_storage_field_type.
                RETURN_IF_CATCH_EXCEPTION(
                        _current_return_columns[cid] = Schema::get_predicate_column_ptr(
                                _is_char_type[cid] ? FieldType::OLAP_FIELD_TYPE_CHAR
                                                   : storage_column_type->get_storage_field_type(),
                                storage_column_type->is_nullable(), _opts.io_ctx.reader_type));
                _current_return_columns[cid]->set_rowset_segment_id(
                        {_segment->rowset_id(), _segment->id()});
                _current_return_columns[cid]->reserve(_opts.block_row_max);
            } else if (i >= block->columns()) {
                // if i >= block->columns means the column and not the pred_column means `column i` is
                // a delete condition column. but the column is not effective in the segment. so we just
                // create a column to hold the data.
                // a. origin data -> b. delete condition -> c. new load data
                // the segment of c do not effective delete condition, but it still need read the column
                // to match the schema.
                // TODO: skip read the not effective delete column to speed up segment read.
                _current_return_columns[cid] =
                        Schema::get_data_type_ptr(*column_desc)->create_column();
                _current_return_columns[cid]->reserve(_opts.block_row_max);
            }
        }

        // Additional deleted filter condition will be materialized column be at the end of the block,
        // after _output_column_by_sel_idx  will be erase, we not need to filter it,
        // so erase it from _columns_to_filter in the first next_batch.
        // Eg:
        //      `delete from table where a = 10;`
        //      `select b from table;`
        // a column only effective in segment iterator, the block from query engine only contain the b column,
        // so no need to filter a column by expr.
        for (auto it = _columns_to_filter.begin(); it != _columns_to_filter.end();) {
            if (*it >= block->columns()) {
                it = _columns_to_filter.erase(it);
            } else {
                ++it;
            }
        }
    }

    uint32_t nrows_read_limit = _opts.block_row_max;
    if (_can_opt_topn_reads()) {
        nrows_read_limit = std::min(static_cast<uint32_t>(_opts.topn_limit), nrows_read_limit);
    }

    DBUG_EXECUTE_IF("segment_iterator.topn_opt_1", {
        if (nrows_read_limit != 1) {
            return Status::Error<ErrorCode::INTERNAL_ERROR>("topn opt 1 execute failed: {}",
                                                            nrows_read_limit);
        }
    })

    RETURN_IF_ERROR(_init_current_block(block, _current_return_columns, nrows_read_limit));
    _converted_column_ids.assign(_schema->columns().size(), 0);

    _current_batch_rows_read = 0;
    RETURN_IF_ERROR(_read_columns_by_index(
            nrows_read_limit, _current_batch_rows_read,
            _lazy_materialization_read || _opts.record_rowids || _is_need_expr_eval));
    if (std::find(_first_read_column_ids.begin(), _first_read_column_ids.end(),
                  _schema->version_col_idx()) != _first_read_column_ids.end()) {
        _replace_version_col(_current_batch_rows_read);
    }

    _opts.stats->blocks_load += 1;
    _opts.stats->raw_rows_read += _current_batch_rows_read;

    if (_current_batch_rows_read == 0) {
        // Convert all columns in _current_return_columns to schema column
        RETURN_IF_ERROR(_convert_to_expected_type(_schema->column_ids()));
        for (int i = 0; i < block->columns(); i++) {
            auto cid = _schema->column_id(i);
            // todo(wb) abstract make column where
            if (!_is_pred_column[cid]) {
                block->replace_by_position(i, std::move(_current_return_columns[cid]));
            }
        }
        block->clear_column_data();
        // clear and release iterators memory footprint in advance
        _clear_iterators();
        return Status::EndOfFile("no more data in segment");
    }

    if (!_is_need_vec_eval && !_is_need_short_eval && !_is_need_expr_eval) {
        if (_non_predicate_columns.empty()) {
            return Status::InternalError("_non_predicate_columns is empty");
        }
        RETURN_IF_ERROR(_convert_to_expected_type(_first_read_column_ids));
        RETURN_IF_ERROR(_convert_to_expected_type(_non_predicate_columns));
        _output_non_pred_columns(block);
    } else {
        uint16_t selected_size = _current_batch_rows_read;
        uint16_t sel_rowid_idx[selected_size];

        if (_is_need_vec_eval || _is_need_short_eval) {
            _convert_dict_code_for_predicate_if_necessary();

            // step 1: evaluate vectorization predicate
            selected_size = _evaluate_vectorization_predicate(sel_rowid_idx, selected_size);

            // step 2: evaluate short circuit predicate
            // todo(wb) research whether need to read short predicate after vectorization evaluation
            //          to reduce cost of read short circuit columns.
            //          In SSB test, it make no difference; So need more scenarios to test
            selected_size = _evaluate_short_circuit_predicate(sel_rowid_idx, selected_size);

            if (selected_size > 0) {
                // step 3.1: output short circuit and predicate column
                // when lazy materialization enables, _first_read_column_ids = distinct(_short_cir_pred_column_ids + _vec_pred_column_ids)
                // see _vec_init_lazy_materialization
                // todo(wb) need to tell input columnids from output columnids
                RETURN_IF_ERROR(_output_column_by_sel_idx(block, _first_read_column_ids,
                                                          sel_rowid_idx, selected_size));

                // step 3.2: read remaining expr column and evaluate it.
                if (_is_need_expr_eval) {
                    // The predicate column contains the remaining expr column, no need second read.
                    if (!_second_read_column_ids.empty()) {
                        SCOPED_RAW_TIMER(&_opts.stats->second_read_ns);
                        RETURN_IF_ERROR(_read_columns_by_rowids(
                                _second_read_column_ids, _block_rowids, sel_rowid_idx,
                                selected_size, &_current_return_columns));
                        if (std::find(_second_read_column_ids.begin(),
                                      _second_read_column_ids.end(), _schema->version_col_idx()) !=
                            _second_read_column_ids.end()) {
                            _replace_version_col(selected_size);
                        }
                        RETURN_IF_ERROR(_convert_to_expected_type(_second_read_column_ids));
                        for (auto cid : _second_read_column_ids) {
                            auto loc = _schema_block_id_map[cid];
                            block->replace_by_position(loc,
                                                       std::move(_current_return_columns[cid]));
                        }
                    }

                    DCHECK(block->columns() > _schema_block_id_map[*_common_expr_columns.begin()]);
                    // block->rows() takes the size of the first column by default. If the first column is no predicate column,
                    // it has not been read yet. add a const column that has been read to calculate rows().
                    if (block->rows() == 0) {
                        vectorized::MutableColumnPtr col0 =
                                std::move(*block->get_by_position(0).column).mutate();
                        auto res_column = vectorized::ColumnString::create();
                        res_column->insert_data("", 0);
                        auto col_const = vectorized::ColumnConst::create(std::move(res_column),
                                                                         selected_size);
                        block->replace_by_position(0, std::move(col_const));
                        _output_index_result_column_for_expr(sel_rowid_idx, selected_size, block);
                        block->shrink_char_type_column_suffix_zero(_char_type_idx_no_0);
                        RETURN_IF_ERROR(_execute_common_expr(sel_rowid_idx, selected_size, block));
                        block->replace_by_position(0, std::move(col0));
                    } else {
                        _output_index_result_column_for_expr(sel_rowid_idx, selected_size, block);
                        block->shrink_char_type_column_suffix_zero(_char_type_idx);
                        RETURN_IF_ERROR(_execute_common_expr(sel_rowid_idx, selected_size, block));
                    }
                }
            } else if (_is_need_expr_eval) {
                RETURN_IF_ERROR(_convert_to_expected_type(_second_read_column_ids));
                for (auto cid : _second_read_column_ids) {
                    auto loc = _schema_block_id_map[cid];
                    block->replace_by_position(loc, std::move(_current_return_columns[cid]));
                }
            }
        } else if (_is_need_expr_eval) {
            DCHECK(!_first_read_column_ids.empty());
            RETURN_IF_ERROR(_convert_to_expected_type(_first_read_column_ids));
            // first read all rows are insert block, initialize sel_rowid_idx to all rows.
            for (auto cid : _first_read_column_ids) {
                auto loc = _schema_block_id_map[cid];
                block->replace_by_position(loc, std::move(_current_return_columns[cid]));
            }
            for (uint32_t i = 0; i < selected_size; ++i) {
                sel_rowid_idx[i] = i;
            }

            if (block->rows() == 0) {
                vectorized::MutableColumnPtr col0 =
                        std::move(*block->get_by_position(0).column).mutate();
                auto res_column = vectorized::ColumnString::create();
                res_column->insert_data("", 0);
                auto col_const =
                        vectorized::ColumnConst::create(std::move(res_column), selected_size);
                block->replace_by_position(0, std::move(col_const));
                _output_index_result_column_for_expr(sel_rowid_idx, selected_size, block);
                block->shrink_char_type_column_suffix_zero(_char_type_idx_no_0);
                RETURN_IF_ERROR(_execute_common_expr(sel_rowid_idx, selected_size, block));
                block->replace_by_position(0, std::move(col0));
            } else {
                _output_index_result_column_for_expr(sel_rowid_idx, selected_size, block);
                block->shrink_char_type_column_suffix_zero(_char_type_idx);
                RETURN_IF_ERROR(_execute_common_expr(sel_rowid_idx, selected_size, block));
            }
        }

        if (UNLIKELY(_opts.record_rowids)) {
            _sel_rowid_idx.resize(selected_size);
            _selected_size = selected_size;
            for (auto i = 0; i < _selected_size; i++) {
                _sel_rowid_idx[i] = sel_rowid_idx[i];
            }
        }

        if (_non_predicate_columns.empty()) {
            // shrink char_type suffix zero data
            block->shrink_char_type_column_suffix_zero(_char_type_idx);

            return Status::OK();
        }
        // step4: read non_predicate column
        if (selected_size > 0) {
            RETURN_IF_ERROR(_read_columns_by_rowids(_non_predicate_columns, _block_rowids,
                                                    sel_rowid_idx, selected_size,
                                                    &_current_return_columns));
            if (std::find(_non_predicate_columns.begin(), _non_predicate_columns.end(),
                          _schema->version_col_idx()) != _non_predicate_columns.end()) {
                _replace_version_col(selected_size);
            }
        }

        RETURN_IF_ERROR(_convert_to_expected_type(_non_predicate_columns));
        // step5: output columns
        _output_non_pred_columns(block);
    }

    // shrink char_type suffix zero data
    block->shrink_char_type_column_suffix_zero(_char_type_idx);

#ifndef NDEBUG
    size_t rows = block->rows();
    for (const auto& entry : *block) {
        if (entry.column->size() != rows) {
            throw doris::Exception(ErrorCode::INTERNAL_ERROR, "unmatched size {}, expected {}",
                                   entry.column->size(), rows);
        }
    }
#endif
    VLOG_DEBUG << "dump block " << block->dump_data(0, block->rows());

    return Status::OK();
}

Status SegmentIterator::_execute_common_expr(uint16_t* sel_rowid_idx, uint16_t& selected_size,
                                             vectorized::Block* block) {
    SCOPED_RAW_TIMER(&_opts.stats->expr_filter_ns);
    DCHECK(!_remaining_conjunct_roots.empty());
    DCHECK(block->rows() != 0);
    size_t prev_columns = block->columns();

    vectorized::IColumn::Filter filter;
    RETURN_IF_ERROR(vectorized::VExprContext::execute_conjuncts_and_filter_block(
            _common_expr_ctxs_push_down, block, _columns_to_filter, prev_columns, filter));

    selected_size = _evaluate_common_expr_filter(sel_rowid_idx, selected_size, filter);
    return Status::OK();
}

uint16_t SegmentIterator::_evaluate_common_expr_filter(uint16_t* sel_rowid_idx,
                                                       uint16_t selected_size,
                                                       const vectorized::IColumn::Filter& filter) {
    size_t count = filter.size() - simd::count_zero_num((int8_t*)filter.data(), filter.size());
    if (count == 0) {
        return 0;
    } else {
        const vectorized::UInt8* filt_pos = filter.data();

        uint16_t new_size = 0;
        uint32_t sel_pos = 0;
        const uint32_t sel_end = selected_size;
        static constexpr size_t SIMD_BYTES = simd::bits_mask_length();
        const uint32_t sel_end_simd = sel_pos + selected_size / SIMD_BYTES * SIMD_BYTES;

        while (sel_pos < sel_end_simd) {
            auto mask = simd::bytes_mask_to_bits_mask(filt_pos + sel_pos);
            if (0 == mask) {
                //pass
            } else if (simd::bits_mask_all() == mask) {
                for (uint32_t i = 0; i < SIMD_BYTES; i++) {
                    sel_rowid_idx[new_size++] = sel_rowid_idx[sel_pos + i];
                }
            } else {
                simd::iterate_through_bits_mask(
                        [&](const size_t bit_pos) {
                            sel_rowid_idx[new_size++] = sel_rowid_idx[sel_pos + bit_pos];
                        },
                        mask);
            }
            sel_pos += SIMD_BYTES;
        }

        for (; sel_pos < sel_end; sel_pos++) {
            if (filt_pos[sel_pos]) {
                sel_rowid_idx[new_size++] = sel_rowid_idx[sel_pos];
            }
        }
        return new_size;
    }
}

void SegmentIterator::_output_index_result_column_for_expr(uint16_t* sel_rowid_idx,
                                                           uint16_t select_size,
                                                           vectorized::Block* block) {
    SCOPED_RAW_TIMER(&_opts.stats->output_index_result_column_timer);
    if (block->rows() == 0) {
        return;
    }
    for (auto& expr_ctx : _common_expr_ctxs_push_down) {
        for (auto& inverted_index_result_bitmap_for_expr :
             expr_ctx->get_inverted_index_context()->get_inverted_index_result_bitmap()) {
            const auto* expr = inverted_index_result_bitmap_for_expr.first;
            const auto& index_result_bitmap =
                    inverted_index_result_bitmap_for_expr.second.get_data_bitmap();
            auto index_result_column = vectorized::ColumnUInt8::create();
            vectorized::ColumnUInt8::Container& vec_match_pred = index_result_column->get_data();
            vec_match_pred.resize(block->rows());
            size_t idx_in_selected = 0;
            roaring::BulkContext bulk_context;

            for (uint32_t i = 0; i < _current_batch_rows_read; i++) {
                auto rowid = _block_rowids[i];
                if (sel_rowid_idx == nullptr ||
                    (idx_in_selected < select_size && i == sel_rowid_idx[idx_in_selected])) {
                    if (index_result_bitmap->containsBulk(bulk_context, rowid)) {
                        vec_match_pred[idx_in_selected] = true;
                    } else {
                        vec_match_pred[idx_in_selected] = false;
                    }
                    idx_in_selected++;
                }
            }
            DCHECK(block->rows() == vec_match_pred.size());
            expr_ctx->get_inverted_index_context()->set_inverted_index_result_column_for_expr(
                    expr, std::move(index_result_column));
        }
    }
}

void SegmentIterator::_convert_dict_code_for_predicate_if_necessary() {
    for (auto predicate : _short_cir_eval_predicate) {
        _convert_dict_code_for_predicate_if_necessary_impl(predicate);
    }

    for (auto predicate : _pre_eval_block_predicate) {
        _convert_dict_code_for_predicate_if_necessary_impl(predicate);
    }

    for (auto column_id : _delete_range_column_ids) {
        _current_return_columns[column_id].get()->convert_dict_codes_if_necessary();
    }

    for (auto column_id : _delete_bloom_filter_column_ids) {
        _current_return_columns[column_id].get()->initialize_hash_values_for_runtime_filter();
    }
}

void SegmentIterator::_convert_dict_code_for_predicate_if_necessary_impl(
        ColumnPredicate* predicate) {
    auto& column = _current_return_columns[predicate->column_id()];
    auto* col_ptr = column.get();

    if (PredicateTypeTraits::is_range(predicate->type())) {
        col_ptr->convert_dict_codes_if_necessary();
    } else if (PredicateTypeTraits::is_bloom_filter(predicate->type())) {
        col_ptr->initialize_hash_values_for_runtime_filter();
    }
}

Status SegmentIterator::current_block_row_locations(std::vector<RowLocation>* block_row_locations) {
    DCHECK(_opts.record_rowids);
    DCHECK_GE(_block_rowids.size(), _current_batch_rows_read);
    uint32_t sid = segment_id();
    if (!_is_need_vec_eval && !_is_need_short_eval && !_is_need_expr_eval) {
        block_row_locations->resize(_current_batch_rows_read);
        for (auto i = 0; i < _current_batch_rows_read; i++) {
            (*block_row_locations)[i] = RowLocation(sid, _block_rowids[i]);
        }
    } else {
        block_row_locations->resize(_selected_size);
        for (auto i = 0; i < _selected_size; i++) {
            (*block_row_locations)[i] = RowLocation(sid, _block_rowids[_sel_rowid_idx[i]]);
        }
    }
    return Status::OK();
}

Status SegmentIterator::_construct_compound_expr_context() {
    auto inverted_index_context = std::make_shared<vectorized::InvertedIndexContext>(
            _schema->column_ids(), _inverted_index_iterators, _storage_name_and_type,
            _common_expr_inverted_index_status);
    for (const auto& expr_ctx : _opts.common_expr_ctxs_push_down) {
        vectorized::VExprContextSPtr context;
        RETURN_IF_ERROR(expr_ctx->clone(_opts.runtime_state, context));
        context->set_inverted_index_context(inverted_index_context);
        _common_expr_ctxs_push_down.emplace_back(context);
    }
    return Status::OK();
}

void SegmentIterator::_calculate_expr_in_remaining_conjunct_root() {
    for (const auto& root_expr_ctx : _common_expr_ctxs_push_down) {
        const auto& root_expr = root_expr_ctx->root();
        if (root_expr == nullptr) {
            continue;
        }

        std::stack<vectorized::VExprSPtr> stack;
        stack.emplace(root_expr);

        while (!stack.empty()) {
            const auto& expr = stack.top();
            stack.pop();

            for (const auto& child : expr->children()) {
                if (child->is_slot_ref()) {
                    auto* column_slot_ref = assert_cast<vectorized::VSlotRef*>(child.get());
                    _common_expr_inverted_index_status[_schema->column_id(
                            column_slot_ref->column_id())][expr.get()] = false;
                }
            }

            const auto& children = expr->children();
            for (int32_t i = children.size() - 1; i >= 0; --i) {
                if (!children[i]->children().empty()) {
                    stack.emplace(children[i]);
                }
            }
        }
    }
}

bool SegmentIterator::_no_need_read_key_data(ColumnId cid, vectorized::MutableColumnPtr& column,
                                             size_t nrows_read) {
    if (!((_opts.tablet_schema->keys_type() == KeysType::DUP_KEYS ||
           (_opts.tablet_schema->keys_type() == KeysType::UNIQUE_KEYS &&
            _opts.enable_unique_key_merge_on_write)))) {
        return false;
    }

    if (_opts.push_down_agg_type_opt != TPushAggOp::COUNT_ON_INDEX) {
        return false;
    }

    if (!_opts.tablet_schema->column(cid).is_key()) {
        return false;
    }

    if (_has_delete_predicate(cid)) {
        return false;
    }

    if (!_check_all_conditions_passed_inverted_index_for_column(cid)) {
        return false;
    }

    if (column->is_nullable()) {
        auto* nullable_col_ptr = reinterpret_cast<vectorized::ColumnNullable*>(column.get());
        nullable_col_ptr->get_null_map_column().insert_many_defaults(nrows_read);
        nullable_col_ptr->get_nested_column_ptr()->insert_many_defaults(nrows_read);
    } else {
        column->insert_many_defaults(nrows_read);
    }

    return true;
}

bool SegmentIterator::_has_delete_predicate(ColumnId cid) {
    std::set<uint32_t> delete_columns_set;
    _opts.delete_condition_predicates->get_all_column_ids(delete_columns_set);
    return delete_columns_set.contains(cid);
}

bool SegmentIterator::_can_opt_topn_reads() {
    if (_opts.topn_limit <= 0) {
        return false;
    }

    if (_opts.delete_condition_predicates->num_of_column_predicate() > 0) {
        return false;
    }

    bool all_true = std::ranges::all_of(_schema->column_ids(), [this](auto cid) {
        if (cid == _opts.tablet_schema->delete_sign_idx()) {
            return true;
        }
        if (_check_all_conditions_passed_inverted_index_for_column(cid, true)) {
            return true;
        }
        return false;
    });

    DBUG_EXECUTE_IF("segment_iterator.topn_opt_2", {
        if (all_true) {
            return Status::Error<ErrorCode::INTERNAL_ERROR>("topn opt 2 execute failed");
        }
    })

    return all_true;
}

} // namespace segment_v2
} // namespace doris
