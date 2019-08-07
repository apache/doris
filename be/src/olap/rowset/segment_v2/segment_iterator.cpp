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

#include <set>

#include "gutil/strings/substitute.h"
#include "util/doris_metrics.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/rowset/segment_v2/column_reader.h"
#include "olap/row_block2.h"
#include "olap/row_cursor.h"
#include "olap/short_key_index.h"

using strings::Substitute;

namespace doris {
namespace segment_v2 {

SegmentIterator::SegmentIterator(std::shared_ptr<Segment> segment,
                                 const Schema& schema)
        : _segment(std::move(segment)),
        _schema(schema),
        _column_iterators(_schema.num_columns(), nullptr) {
}

SegmentIterator::~SegmentIterator() {
    for (auto iter : _column_iterators) {
        delete iter;
    }
}

Status SegmentIterator::init(const StorageReadOptions& opts) {
    DorisMetrics::segment_read_total.increment(1);
    _opts = opts;
    RETURN_IF_ERROR(_init_short_key_range());
    RETURN_IF_ERROR(_init_column_iterators());
    RETURN_IF_ERROR(_init_row_ranges());
    return Status::OK();
}

// This function will use input key bounds to get a row range.
Status SegmentIterator::_init_short_key_range() {
    DorisMetrics::segment_row_total.increment(num_rows());
    _lower_rowid = 0;
    _upper_rowid = num_rows();

    // fast path for empty segment
    if (_upper_rowid == 0) {
        return Status::OK();
    }

    if (_opts.lower_bound == nullptr && _opts.upper_bound == nullptr) {
        return Status::OK();
    }

    RETURN_IF_ERROR(_prepare_seek());

    // init row range with short key range
    if (_opts.upper_bound != nullptr) {
        // If client want to read upper_bound, the include_upper_bound is true. So we
        // should get the first ordinal at which key is larger than upper_bound.
        // So we call _lookup_ordinal with include_upper_bound's negate
        RETURN_IF_ERROR(_lookup_ordinal(
                *_opts.upper_bound, !_opts.include_upper_bound, num_rows(), &_upper_rowid));
    }
    if (_upper_rowid > 0 && _opts.lower_bound != nullptr) {
        RETURN_IF_ERROR(_lookup_ordinal(
                *_opts.lower_bound, _opts.include_lower_bound, _upper_rowid, &_lower_rowid));
    }
    DorisMetrics::segment_rows_by_short_key.increment(_upper_rowid - _lower_rowid);

    return Status::OK();
}

// Set up environment for the following seek.
Status SegmentIterator::_prepare_seek() {
    std::vector<Field> key_fields;
    std::set<uint32_t> column_set;
    if (_opts.lower_bound != nullptr) {
        for (auto cid : _opts.lower_bound->schema()->column_ids()) {
            column_set.emplace(cid);
            key_fields.push_back(*_opts.lower_bound->schema()->column(cid));
        }
    }
    if (_opts.upper_bound != nullptr) {
        for (auto cid : _opts.upper_bound->schema()->column_ids()) {
            if (column_set.count(cid) == 0) {
                key_fields.push_back(*_opts.upper_bound->schema()->column(cid));
                column_set.emplace(cid);
            }
        }
    }
    _seek_schema.reset(new Schema(key_fields, key_fields.size()));
    _seek_block.reset(new RowBlockV2(*_seek_schema, 1, &_arena));

    // create used column iterator
    for (auto cid : _seek_schema->column_ids()) {
        if (_column_iterators[cid] == nullptr) {
            RETURN_IF_ERROR(_create_column_iterator(cid, &_column_iterators[cid]));
        }
    }

    return Status::OK();
}

Status SegmentIterator::_init_row_ranges() {
    if (_lower_rowid == _upper_rowid) {
        // no data return;
        _row_ranges = RowRanges();
        return Status::OK();
    }
    // short key range is [_lower_rowid, _upper_rowid)
    RowRanges short_key_row_ranges(std::move(RowRanges::create_single(_lower_rowid, _upper_rowid)));
    if (_opts.conditions != nullptr) {
        RETURN_IF_ERROR(_get_row_ranges_from_zone_map());
        // TODO(hkp): get row ranges from bloom filter and secondary index
        RowRanges::ranges_intersection(_row_ranges, short_key_row_ranges, &_row_ranges);
    } else {
        _row_ranges = std::move(short_key_row_ranges);
    }

    if (_row_ranges.has_next()) {
        _row_ranges.next();
        _cur_rowid = _row_ranges.current_range_from();
    }

    // TODO(hkp): calculate filter rate to decide whether to
    // use zone map/bloom filter/secondary index or not.
    return Status::OK();
}

Status SegmentIterator::_get_row_ranges_from_zone_map() {
    Conditions* conditions = _opts.conditions;
    for (auto& column_condition : conditions->columns()) {
        int32_t column_id = column_condition.first;
        // get row ranges from zone map
        const ColumnZoneMap* column_zone_map = _segment->_column_readers[column_id]->get_zone_map();
        if (column_zone_map == nullptr) {
            // there is no zone map for this column
            continue;
        }
        std::vector<uint32_t> page_indexes;
        const TypeInfo* type_info = _segment->_column_readers[column_id]->type_info();
        _get_filtered_pages(type_info->type(), column_zone_map, column_condition.second, &page_indexes);
        const OrdinalPageIndex* column_ordinal_index = _segment->_column_readers[column_id]->get_ordinal_index();
        _calculate_row_ranges(column_ordinal_index, page_indexes, &_row_ranges);
    }
    DorisMetrics::segment_rows_read_by_zone_map.increment(_row_ranges.count());
    return Status::OK();
}

Status SegmentIterator::_get_filtered_pages(FieldType type, const ColumnZoneMap* column_zone_map,
        CondColumn* cond_column, std::vector<uint32_t>* page_indexes) {
    DCHECK(page_indexes != nullptr);
    std::vector<ZoneMap> zone_maps(std::move(column_zone_map->get_column_zone_map()));
    int32_t page_size = column_zone_map->num_pages();
    std::unique_ptr<WrapperField> min_value(WrapperField::create_by_type(type));
    std::unique_ptr<WrapperField> max_value(WrapperField::create_by_type(type));
    for (int32_t i = 0; i < page_size; ++i) {
        min_value->deserialize(zone_maps[i].min_value());
        max_value->deserialize(zone_maps[i].max_value());
        if (cond_column->eval({min_value.get(), max_value.get()})) {
            page_indexes->push_back(i);
        }
    }
    return Status::OK();
}

Status SegmentIterator::_calculate_row_ranges(const OrdinalPageIndex* ordinal_index,
        const std::vector<uint32_t>& page_indexes, RowRanges* row_ranges) {
    for (auto i : page_indexes) {
        rowid_t page_first_id = ordinal_index->get_first_row_id(i);
        rowid_t page_last_id = ordinal_index->get_last_row_id(i, _segment->num_rows()) + 1;
        RowRanges page_row_ranges(std::move(RowRanges::create_single(page_first_id, page_last_id)));
        RowRanges::ranges_union(*row_ranges, page_row_ranges, row_ranges);
    }
    return Status::OK();
}

Status SegmentIterator::_init_column_iterators() {
    _cur_rowid = _lower_rowid;
    if (_cur_rowid >= num_rows()) {
        return Status::OK();
    }
    for (auto cid : _schema.column_ids()) {
        if (_column_iterators[cid] == nullptr) {
            RETURN_IF_ERROR(_create_column_iterator(cid, &_column_iterators[cid]));
        }

        _column_iterators[cid]->seek_to_ordinal(_cur_rowid);
    }
    return Status::OK();
}

Status SegmentIterator::_create_column_iterator(uint32_t cid, ColumnIterator** iter) {
    return _segment->new_column_iterator(cid, iter);
}

// Schema of lhs and rhs are different.
// callers should assure that rhs' schema has all columns in lhs schema
template<typename LhsRowType, typename RhsRowType>
int compare_row_with_lhs_columns(const LhsRowType& lhs, const RhsRowType& rhs) {
    for (auto cid : lhs.schema()->column_ids()) {
        auto res = lhs.schema()->column(cid)->compare_cell(lhs.cell(cid), rhs.cell(cid));
        if (res != 0) {
            return res;
        }
    }
    return 0;
}

// look up one key to get its ordinal at which can get data. 
// 'upper_bound' is defined the max ordinal the function will search.
// We use upper_bound to reduce search times.
// If we find a valid ordinal, it will be set in rowid and with Status::OK()
// If we can not find a valid key in this segment, we will set rowid to upper_bound
// Otherwise return error.
// 1. get [start, end) ordinal through short key index
// 2. binary search to find exact ordinal that match the input condition
// Make is_include template to reduce branch
Status SegmentIterator::_lookup_ordinal(const RowCursor& key, bool is_include,
                                        rowid_t upper_bound, rowid_t* rowid) {
    std::string index_key;
    encode_key_with_padding(&index_key, key, _segment->num_short_keys(), is_include);

    uint32_t start_block_id = 0;
    auto start_iter = _segment->lower_bound(index_key);
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
        start_block_id = _segment->last_block();
    }
    rowid_t start = start_block_id * _segment->num_rows_per_block();

    rowid_t end = upper_bound;
    auto end_iter = _segment->upper_bound(index_key);
    if (end_iter.valid()) {
        end = end_iter.ordinal() * _segment->num_rows_per_block();
    }

    // binary search to find the exact key
    while (start < end) {
        rowid_t mid = (start + end) / 2;
        RETURN_IF_ERROR(_seek_and_peek(mid));
        int cmp = compare_row_with_lhs_columns(key, _seek_block->row(0));
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

// seek to the row and load that row to _key_cursor
Status SegmentIterator::_seek_and_peek(rowid_t rowid) {
    for (auto cid : _seek_schema->column_ids()) {
        _column_iterators[cid]->seek_to_ordinal(rowid);
    }
    size_t num_rows = 1;
    _seek_block->resize(num_rows);
    RETURN_IF_ERROR(_next_batch(_seek_block.get(), &num_rows));
    return Status::OK();
}

// Try to read data as much to block->num_rows(). The number of read rows
// will be set in rows_read when return OK. rows_read will small than
// block->num_rows() when reach the end of this segment
Status SegmentIterator::_next_batch(RowBlockV2* block, size_t* rows_read) {
    bool has_read = false;
    size_t first_read = 0;
    for (int i = 0; i < block->schema()->column_ids().size(); ++i) {
        auto cid = block->schema()->column_ids()[i];
        size_t num_rows = has_read ? first_read : block->num_rows();
        auto column_block = block->column_block(i);
        RETURN_IF_ERROR(_column_iterators[cid]->next_batch(&num_rows, &column_block));
        if (!has_read) {
            has_read = true;
            first_read = num_rows;
        } else if (num_rows != first_read) {
            return Status::InternalError(
                Substitute("Read different rows in different columns"
                           ", column($0) read $1 vs column($2) read $3",
                           block->schema()->column_ids()[0], first_read, cid, num_rows));
        }
    }
    *rows_read = first_read;
    return Status::OK();
}

Status SegmentIterator::next_batch(RowBlockV2* block) {
    // _cur_rowid is 10
    // _row_ranges: [2-5], [8-15], [20-25]
    // then, cover_row_ranges will be [10, 25]
    // left_row_ranges will be [10-15], [20-25]
    if (_row_ranges.is_empty()) {
        return Status::OK();
    }
    RowRanges cover_row_ranges(std::move(RowRanges::create_single(_cur_rowid, _row_ranges.to())));
    RowRanges left_row_ranges;
    RowRanges::ranges_intersection(_row_ranges, cover_row_ranges, &left_row_ranges);
    size_t rows_to_read = std::min((size_t)block->capacity(), left_row_ranges.count());
    block->resize(rows_to_read);
    if (rows_to_read == 0) {
        return Status::OK();
    }
    size_t rows_left = rows_to_read;
    while (rows_left > 0) {
        if (_cur_rowid >= _row_ranges.current_range_to()) {
            // step to next range
            if (_row_ranges.has_next()) {
                _row_ranges.next();
                _cur_rowid = _row_ranges.current_range_from();
                for (auto cid : block->schema()->column_ids()) {
                    _column_iterators[cid]->seek_to_ordinal(_cur_rowid);
                }
            } else {
                return Status::RuntimeError(Substitute("invalid row ranges, _cur_rowid:$0, range_to:$1, rows_left:$2",
                        _cur_rowid, _row_ranges.current_range_to(), rows_left));
            }
        }
        size_t to_read_in_range = std::min(rows_left, _row_ranges.current_range_to() - _cur_rowid);
        RETURN_IF_ERROR(_next_batch(block, &to_read_in_range));
        _cur_rowid += to_read_in_range;
        rows_left -= to_read_in_range;
    }
    return Status::OK();
}

}
}
