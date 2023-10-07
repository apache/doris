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

#include "vec/olap/vcollect_iterator.h"

#include <gen_cpp/olap_file.pb.h>
#include <glog/logging.h>

#include <algorithm>
#include <iterator>
#include <ostream>
#include <set>
#include <utility>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "io/io_common.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/tablet.h"
#include "olap/tablet_schema.h"
#include "runtime/query_context.h"
#include "runtime/runtime_predicate.h"
#include "runtime/runtime_state.h"
#include "vec/columns/column.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/field.h"
#include "vec/data_types/data_type.h"
#include "vec/exprs/vexpr_context.h"

namespace doris {
using namespace ErrorCode;

namespace vectorized {

#define RETURN_IF_NOT_EOF_AND_OK(stmt)                                 \
    do {                                                               \
        const Status& _status_ = (stmt);                               \
        if (UNLIKELY(!_status_.ok() && !_status_.is<END_OF_FILE>())) { \
            return _status_;                                           \
        }                                                              \
    } while (false)

VCollectIterator::~VCollectIterator() = default;

void VCollectIterator::init(TabletReader* reader, bool ori_data_overlapping, bool force_merge,
                            bool is_reverse) {
    _reader = reader;

    // when aggregate is enabled or key_type is DUP_KEYS, we don't merge
    // multiple data to aggregate for better performance
    if (_reader->_reader_type == ReaderType::READER_QUERY &&
        (_reader->_direct_mode || _reader->_tablet->keys_type() == KeysType::DUP_KEYS ||
         (_reader->_tablet->keys_type() == KeysType::UNIQUE_KEYS &&
          _reader->_tablet->enable_unique_key_merge_on_write()))) {
        _merge = false;
    }

    // When data is none overlapping, no need to build heap to traverse data
    if (!ori_data_overlapping) {
        _merge = false;
    } else if (force_merge) {
        _merge = true;
    }
    _is_reverse = is_reverse;

    // use topn_next opt only for DUP_KEYS and UNIQUE_KEYS with MOW
    if (_reader->_reader_context.read_orderby_key_limit > 0 &&
        (_reader->_tablet->keys_type() == KeysType::DUP_KEYS ||
         (_reader->_tablet->keys_type() == KeysType::UNIQUE_KEYS &&
          _reader->_tablet->enable_unique_key_merge_on_write()))) {
        _topn_limit = _reader->_reader_context.read_orderby_key_limit;
    } else {
        _topn_limit = 0;
    }
}

Status VCollectIterator::add_child(const RowSetSplits& rs_splits) {
    if (use_topn_next()) {
        _rs_splits.push_back(rs_splits);
        return Status::OK();
    }

    _children.push_back(std::make_unique<Level0Iterator>(rs_splits.rs_reader, _reader));
    return Status::OK();
}

// Build a merge heap. If _merge is true, a rowset with the max rownum
// status will be used as the base rowset, and the other rowsets will be merged first and
// then merged with the base rowset.
Status VCollectIterator::build_heap(std::vector<RowsetReaderSharedPtr>& rs_readers) {
    if (use_topn_next()) {
        return Status::OK();
    }

    DCHECK(rs_readers.size() == _children.size());
    _skip_same = _reader->_tablet_schema->keys_type() == KeysType::UNIQUE_KEYS;
    if (_children.empty()) {
        _inner_iter.reset(nullptr);
        return Status::OK();
    } else if (_merge) {
        DCHECK(!rs_readers.empty());
        bool have_multiple_child = false;
        for (auto [c_iter, r_iter] = std::pair {_children.begin(), rs_readers.begin()};
             c_iter != _children.end();) {
            auto s = (*c_iter)->init(have_multiple_child);
            if (!s.ok()) {
                c_iter = _children.erase(c_iter);
                r_iter = rs_readers.erase(r_iter);
                if (!s.is<END_OF_FILE>()) {
                    return s;
                }
            } else {
                have_multiple_child = true;
                ++c_iter;
                ++r_iter;
            }
        }

        // build merge heap with two children, a base rowset as level0iterator and
        // other cumulative rowsets as a level1iterator
        if (_children.size() > 1) {
            // find 'base rowset', 'base rowset' is the rowset which contains the max row number
            int64_t max_row_num = 0;
            int base_reader_idx = 0;
            for (size_t i = 0; i < rs_readers.size(); ++i) {
                int64_t cur_row_num = rs_readers[i]->rowset()->rowset_meta()->num_rows();
                if (cur_row_num > max_row_num) {
                    max_row_num = cur_row_num;
                    base_reader_idx = i;
                }
            }
            auto base_reader_child = _children.begin();
            std::advance(base_reader_child, base_reader_idx);

            std::list<std::unique_ptr<LevelIterator>> cumu_children;
            for (auto iter = _children.begin(); iter != _children.end();) {
                if (iter != base_reader_child) {
                    cumu_children.push_back(std::move(*iter));
                    iter = _children.erase(iter);
                } else {
                    ++iter;
                }
            }
            bool is_merge = cumu_children.size() > 1;
            std::unique_ptr<LevelIterator> cumu_iter = std::make_unique<Level1Iterator>(
                    std::move(cumu_children), _reader, is_merge, _is_reverse, _skip_same);
            RETURN_IF_NOT_EOF_AND_OK(cumu_iter->init());
            std::list<std::unique_ptr<LevelIterator>> children;
            children.push_back(std::move(*base_reader_child));
            children.push_back(std::move(cumu_iter));
            _inner_iter.reset(new Level1Iterator(std::move(children), _reader, _merge, _is_reverse,
                                                 _skip_same));
            // need to clear _children here, or else if the following _inner_iter->init() return early
            // base_reader_child will be double deleted
            _children.clear();
        } else {
            // _children.size() == 1
            _inner_iter.reset(new Level1Iterator(std::move(_children), _reader, _merge, _is_reverse,
                                                 _skip_same));
        }
    } else {
        auto level1_iter = std::make_unique<Level1Iterator>(std::move(_children), _reader, _merge,
                                                            _is_reverse, _skip_same);
        _children.clear();
        level1_iter->init_level0_iterators_for_union();
        RETURN_IF_ERROR(level1_iter->ensure_first_row_ref());
        _inner_iter = std::move(level1_iter);
    }
    RETURN_IF_NOT_EOF_AND_OK(_inner_iter->init());
    // Clear _children earlier to release any related references
    _children.clear();
    return Status::OK();
}

bool VCollectIterator::LevelIteratorComparator::operator()(LevelIterator* lhs, LevelIterator* rhs) {
    const IteratorRowRef& lhs_ref = *lhs->current_row_ref();
    const IteratorRowRef& rhs_ref = *rhs->current_row_ref();

    int cmp_res = UNLIKELY(lhs->compare_columns())
                          ? lhs_ref.compare(rhs_ref, lhs->compare_columns())
                          : lhs_ref.compare(rhs_ref, lhs->tablet_schema().num_key_columns());
    if (cmp_res != 0) {
        return UNLIKELY(_is_reverse) ? cmp_res < 0 : cmp_res > 0;
    }

    if (_sequence != -1) {
        cmp_res = lhs_ref.block->get_by_position(_sequence).column->compare_at(
                lhs_ref.row_pos, rhs_ref.row_pos,
                *(rhs_ref.block->get_by_position(_sequence).column), -1);
    }

    // if row cursors equal, compare data version.
    // read data from higher version to lower version.
    // for UNIQUE_KEYS just read the highest version and no need agg_update.
    // for AGG_KEYS if a version is deleted, the lower version no need to agg_update
    bool lower = (cmp_res != 0) ? (cmp_res < 0) : (lhs->version() < rhs->version());
    lower ? lhs->set_same(true) : rhs->set_same(true);

    return lower;
}

Status VCollectIterator::current_row(IteratorRowRef* ref) const {
    if (LIKELY(_inner_iter)) {
        *ref = *_inner_iter->current_row_ref();
        if (ref->row_pos == -1) {
            return Status::Error<END_OF_FILE>("");
        } else {
            return Status::OK();
        }
    }
    return Status::Error<DATA_ROW_BLOCK_ERROR>("inner iter is nullptr");
}

Status VCollectIterator::next(IteratorRowRef* ref) {
    if (LIKELY(_inner_iter)) {
        return _inner_iter->next(ref);
    } else {
        return Status::Error<END_OF_FILE>("");
    }
}

Status VCollectIterator::next(Block* block) {
    if (use_topn_next()) {
        return _topn_next(block);
    }

    if (LIKELY(_inner_iter)) {
        return _inner_iter->next(block);
    } else {
        return Status::Error<END_OF_FILE>("");
    }
}

Status VCollectIterator::_topn_next(Block* block) {
    if (_topn_eof) {
        return Status::Error<END_OF_FILE>("");
    }

    auto clone_block = block->clone_empty();
    MutableBlock mutable_block = vectorized::MutableBlock::build_mutable_block(&clone_block);
    // clear TMPE columns to avoid column align problem in mutable_block.add_rows bellow
    auto all_column_names = mutable_block.get_names();
    for (auto& name : all_column_names) {
        if (name.rfind(BeConsts::BLOCK_TEMP_COLUMN_PREFIX, 0) == 0) {
            mutable_block.erase(name);
        }
    }

    if (!_reader->_reader_context.read_orderby_key_columns) {
        return Status::Error<ErrorCode::INTERNAL_ERROR>(
                "read_orderby_key_columns should not be nullptr");
    }

    size_t first_sort_column_idx = (*_reader->_reader_context.read_orderby_key_columns)[0];
    const std::vector<uint32_t>* sort_columns = _reader->_reader_context.read_orderby_key_columns;

    BlockRowPosComparator row_pos_comparator(&mutable_block, sort_columns,
                                             _reader->_reader_context.read_orderby_key_reverse);
    std::multiset<size_t, BlockRowPosComparator, std::allocator<size_t>> sorted_row_pos(
            row_pos_comparator);

    if (_is_reverse) {
        std::reverse(_rs_splits.begin(), _rs_splits.end());
    }

    for (size_t i = 0; i < _rs_splits.size(); i++) {
        const auto& rs_split = _rs_splits[i];
        // init will prune segment by _reader_context.conditions and _reader_context.runtime_conditions
        RETURN_IF_ERROR(rs_split.rs_reader->init(&_reader->_reader_context, rs_split));

        // read _topn_limit rows from this rs
        size_t read_rows = 0;
        bool eof = false;
        while (read_rows < _topn_limit && !eof) {
            block->clear_column_data();
            auto status = rs_split.rs_reader->next_block(block);
            if (!status.ok()) {
                if (status.is<END_OF_FILE>()) {
                    eof = true;
                    if (block->rows() == 0) {
                        break;
                    }
                } else {
                    return status;
                }
            }

            auto col_name = block->get_names()[first_sort_column_idx];

            // filter block
            RETURN_IF_ERROR(VExprContext::filter_block(
                    _reader->_reader_context.filter_block_conjuncts, block, block->columns()));
            // clear TMPE columns to avoid column align problem in mutable_block.add_rows bellow
            auto all_column_names = block->get_names();
            for (auto& name : all_column_names) {
                if (name.rfind(BeConsts::BLOCK_TEMP_COLUMN_PREFIX, 0) == 0) {
                    block->erase(name);
                }
            }

            // update read rows
            read_rows += block->rows();

            // insert block rows to mutable_block and adjust sorted_row_pos
            bool changed = false;

            size_t rows_to_copy = 0;
            if (sorted_row_pos.empty()) {
                rows_to_copy = std::min(block->rows(), _topn_limit);
            } else {
                // _is_reverse == true  last_row_pos is the pos of smallest row
                // _is_reverse == false last_row_pos is biggest row
                size_t last_row_pos = *sorted_row_pos.rbegin();

                // find the how many rows which is less than the last row in mutable_block
                for (size_t i = 0; i < block->rows(); i++) {
                    // if there is not enough rows in sorted_row_pos, just copy new rows
                    if (sorted_row_pos.size() + rows_to_copy < _topn_limit) {
                        rows_to_copy++;
                        continue;
                    }

                    DCHECK_GE(block->columns(), sort_columns->size());
                    DCHECK_GE(mutable_block.columns(), sort_columns->size());

                    int res = 0;
                    for (auto j : *sort_columns) {
                        DCHECK(block->get_by_position(j).type->equals(
                                *mutable_block.get_datatype_by_position(j)));
                        res = block->get_by_position(j).column->compare_at(
                                i, last_row_pos, *(mutable_block.get_column_by_position(j)), -1);
                        if (res) {
                            break;
                        }
                    }

                    // only copy needed rows
                    // _is_reverse == true  > smallest is ok
                    // _is_reverse == false < biggest is ok
                    if ((_is_reverse && res > 0) || (!_is_reverse && res < 0)) {
                        rows_to_copy++;
                    } else {
                        break;
                    }
                }
            }

            if (rows_to_copy > 0) {
                // create column that is not in mutable_block but in block
                for (size_t i = mutable_block.columns(); i < block->columns(); ++i) {
                    auto col = block->get_by_position(i).clone_empty();
                    mutable_block.mutable_columns().push_back(col.column->assume_mutable());
                    mutable_block.data_types().push_back(std::move(col.type));
                    mutable_block.get_names().push_back(std::move(col.name));
                }

                size_t base = mutable_block.rows();
                // append block to mutable_block
                mutable_block.add_rows(block, 0, rows_to_copy);
                // insert appended rows pos in mutable_block to sorted_row_pos and sort it
                for (size_t i = 0; i < rows_to_copy; i++) {
                    sorted_row_pos.insert(base + i);
                    changed = true;
                }
            }

            // delete to keep _topn_limit row pos
            if (sorted_row_pos.size() > _topn_limit) {
                auto first = sorted_row_pos.begin();
                for (size_t i = 0; i < _topn_limit; i++) {
                    first++;
                }
                sorted_row_pos.erase(first, sorted_row_pos.end());

                // shrink mutable_block to save memory when rows > _topn_limit * 2
                if (mutable_block.rows() > _topn_limit * 2) {
                    VLOG_DEBUG << "topn debug start  shrink mutable_block from "
                               << mutable_block.rows() << " rows";
                    Block tmp_block = mutable_block.to_block();
                    clone_block = tmp_block.clone_empty();
                    mutable_block = vectorized::MutableBlock::build_mutable_block(&clone_block);
                    for (auto it = sorted_row_pos.begin(); it != sorted_row_pos.end(); it++) {
                        mutable_block.add_row(&tmp_block, *it);
                    }

                    sorted_row_pos.clear();
                    for (size_t i = 0; i < _topn_limit; i++) {
                        sorted_row_pos.insert(i);
                    }
                    VLOG_DEBUG << "topn debug finish shrink mutable_block to "
                               << mutable_block.rows() << " rows";
                }
            }

            // update runtime_predicate
            if (_reader->_reader_context.use_topn_opt && changed &&
                sorted_row_pos.size() >= _topn_limit) {
                // get field value from column
                size_t last_sorted_row = *sorted_row_pos.rbegin();
                auto col_ptr = mutable_block.get_column_by_position(first_sort_column_idx).get();
                Field new_top;
                col_ptr->get(last_sorted_row, new_top);

                // update orderby_extrems in query global context
                auto query_ctx = _reader->_reader_context.runtime_state->get_query_ctx();
                RETURN_IF_ERROR(
                        query_ctx->get_runtime_predicate().update(new_top, col_name, _is_reverse));
            }
        } // end of while (read_rows < _topn_limit && !eof)
        VLOG_DEBUG << "topn debug rowset " << i << " read_rows=" << read_rows << " eof=" << eof
                   << " _topn_limit=" << _topn_limit
                   << " sorted_row_pos.size()=" << sorted_row_pos.size()
                   << " mutable_block.rows()=" << mutable_block.rows();
    } // end of for (auto rs_reader : _rs_readers)

    // copy result_block to block
    VLOG_DEBUG << "topn debug result _topn_limit=" << _topn_limit
               << " sorted_row_pos.size()=" << sorted_row_pos.size()
               << " mutable_block.rows()=" << mutable_block.rows();
    *block = mutable_block.to_block();
    // append a column to indicate scanner filter_block is already done
    auto filtered_datatype = std::make_shared<DataTypeUInt8>();
    auto filtered_column = filtered_datatype->create_column_const(block->rows(), (uint8_t)1);
    block->insert(
            {filtered_column, filtered_datatype, BeConsts::BLOCK_TEMP_COLUMN_SCANNER_FILTERED});

    _topn_eof = true;
    return block->rows() > 0 ? Status::OK() : Status::Error<END_OF_FILE>("");
}

bool VCollectIterator::BlockRowPosComparator::operator()(const size_t& lpos,
                                                         const size_t& rpos) const {
    int ret = _mutable_block->compare_at(lpos, rpos, _compare_columns, *_mutable_block, -1);
    return _is_reverse ? ret > 0 : ret < 0;
}

VCollectIterator::Level0Iterator::Level0Iterator(RowsetReaderSharedPtr rs_reader,
                                                 TabletReader* reader)
        : LevelIterator(reader), _rs_reader(rs_reader), _reader(reader) {
    DCHECK_EQ(RowsetTypePB::BETA_ROWSET, rs_reader->type());
}

Status VCollectIterator::Level0Iterator::init(bool get_data_by_ref) {
    _get_data_by_ref = get_data_by_ref && _rs_reader->support_return_data_by_ref();
    if (!_get_data_by_ref) {
        _block = std::make_shared<Block>(_schema.create_block(
                _reader->_return_columns, _reader->_tablet_columns_convert_to_null_set));
    }

    auto st = refresh_current_row();
    if (_get_data_by_ref && _block_view.size()) {
        _ref = _block_view[0];
    } else {
        _ref = {_block, 0, false};
    }
    return st;
}

// if is_first_child = true, return first row in block。Unique keys and agg keys will
// read a line first and then start loop :
// while (!eof) {
//     collect_iter->next(&_next_row);
// }
// so first child load first row and other child row_pos = -1
void VCollectIterator::Level0Iterator::init_for_union(bool get_data_by_ref) {
    _get_data_by_ref = get_data_by_ref && _rs_reader->support_return_data_by_ref();
}

Status VCollectIterator::Level0Iterator::ensure_first_row_ref() {
    DCHECK(!_get_data_by_ref);
    auto s = refresh_current_row();
    _ref = {_block, 0, false};

    return s;
}

int64_t VCollectIterator::Level0Iterator::version() const {
    return _rs_reader->version().second;
}

Status VCollectIterator::Level0Iterator::refresh_current_row() {
    do {
        if (_block == nullptr && !_get_data_by_ref) {
            _block = std::make_shared<Block>(_schema.create_block(
                    _reader->_return_columns, _reader->_tablet_columns_convert_to_null_set));
        }

        if (!_is_empty() && _current_valid()) {
            return Status::OK();
        } else {
            _reset();
            auto res = _refresh();
            if (!res.ok() && !res.is<END_OF_FILE>()) {
                return res;
            }
            if (res.is<END_OF_FILE>() && _is_empty()) {
                break;
            }

            if (UNLIKELY(_reader->_reader_context.record_rowids)) {
                RETURN_IF_ERROR(_rs_reader->current_block_row_locations(&_block_row_locations));
            }
        }
    } while (!_is_empty());
    _ref.row_pos = -1;
    _current = -1;
    _rs_reader = nullptr;
    return Status::Error<END_OF_FILE>("");
}

Status VCollectIterator::Level0Iterator::next(IteratorRowRef* ref) {
    if (_get_data_by_ref) {
        _current++;
    } else {
        _ref.row_pos++;
    }

    RETURN_IF_ERROR(refresh_current_row());

    if (_get_data_by_ref) {
        _ref = _block_view[_current];
    }

    *ref = _ref;
    return Status::OK();
}

Status VCollectIterator::Level0Iterator::next(Block* block) {
    CHECK(!_get_data_by_ref);
    if (_ref.row_pos <= 0 && _ref.block != nullptr && UNLIKELY(_ref.block->rows() > 0)) {
        block->swap(*_ref.block);
        _ref.reset();
        return Status::OK();
    } else {
        if (_rs_reader == nullptr) {
            return Status::Error<END_OF_FILE>("");
        }
        auto res = _rs_reader->next_block(block);
        if (!res.ok() && !res.is<END_OF_FILE>()) {
            return res;
        }
        if (res.is<END_OF_FILE>() && block->rows() == 0) {
            return Status::Error<END_OF_FILE>("");
        }
        if (UNLIKELY(_reader->_reader_context.record_rowids)) {
            RETURN_IF_ERROR(_rs_reader->current_block_row_locations(&_block_row_locations));
        }
        return Status::OK();
    }
}

RowLocation VCollectIterator::Level0Iterator::current_row_location() {
    RowLocation& segment_row_id = _block_row_locations[_get_data_by_ref ? _current : _ref.row_pos];
    return RowLocation(_rs_reader->rowset()->rowset_id(), segment_row_id.segment_id,
                       segment_row_id.row_id);
}

Status VCollectIterator::Level0Iterator::current_block_row_locations(
        std::vector<RowLocation>* block_row_locations) {
    block_row_locations->resize(_block_row_locations.size());
    for (auto i = 0; i < _block_row_locations.size(); i++) {
        RowLocation& row_location = _block_row_locations[i];
        (*block_row_locations)[i] = RowLocation(_rs_reader->rowset()->rowset_id(),
                                                row_location.segment_id, row_location.row_id);
    }
    return Status::OK();
}

VCollectIterator::Level1Iterator::Level1Iterator(
        std::list<std::unique_ptr<VCollectIterator::LevelIterator>> children, TabletReader* reader,
        bool merge, bool is_reverse, bool skip_same)
        : LevelIterator(reader),
          _children(std::move(children)),
          _reader(reader),
          _merge(merge),
          _is_reverse(is_reverse),
          _skip_same(skip_same) {
    _ref.reset();
    // !_merge means that data are in order, so we just reverse children to return data in reverse
    if (!_merge && _is_reverse) {
        _children.reverse();
    }
}

VCollectIterator::Level1Iterator::~Level1Iterator() {
    if (_heap) {
        while (!_heap->empty()) {
            delete _heap->top();
            _heap->pop();
        }
    }
}

// Read next row into *row.
// Returns
//      OK when read successfully.
//      Status::Error<END_OF_FILE>("") and set *row to nullptr when EOF is reached.
//      Others when error happens
Status VCollectIterator::Level1Iterator::next(IteratorRowRef* ref) {
    if (UNLIKELY(_cur_child == nullptr)) {
        _ref.reset();
        return Status::Error<END_OF_FILE>("");
    }
    if (_merge) {
        return _merge_next(ref);
    } else {
        return _normal_next(ref);
    }
}

// Read next block
// Returns
//      OK when read successfully.
//      Status::Error<END_OF_FILE>("") and set *row to nullptr when EOF is reached.
//      Others when error happens
Status VCollectIterator::Level1Iterator::next(Block* block) {
    if (UNLIKELY(_cur_child == nullptr)) {
        return Status::Error<END_OF_FILE>("");
    }
    if (_merge) {
        return _merge_next(block);
    } else {
        return _normal_next(block);
    }
}

int64_t VCollectIterator::Level1Iterator::version() const {
    if (_cur_child != nullptr) {
        return _cur_child->version();
    }
    return -1;
}

Status VCollectIterator::Level1Iterator::init(bool get_data_by_ref) {
    if (_children.empty()) {
        return Status::OK();
    }

    // Only when there are multiple children that need to be merged
    if (_merge && _children.size() > 1) {
        auto sequence_loc = -1;
        for (int loc = 0; loc < _reader->_return_columns.size(); loc++) {
            if (_reader->_return_columns[loc] == _reader->_sequence_col_idx) {
                sequence_loc = loc;
                break;
            }
        }
        _heap.reset(new MergeHeap {LevelIteratorComparator(sequence_loc, _is_reverse)});
        for (auto&& child : _children) {
            DCHECK(child != nullptr);
            //DCHECK(child->current_row().ok());
            _heap->push(child.release());
        }
        _cur_child.reset(_heap->top());
        _heap->pop();
        // Clear _children earlier to release any related references
        _children.clear();
    } else {
        _merge = false;
        _heap.reset(nullptr);
        _cur_child = std::move(*_children.begin());
        _children.pop_front();
    }
    _ref = *_cur_child->current_row_ref();
    return Status::OK();
}

Status VCollectIterator::Level1Iterator::ensure_first_row_ref() {
    for (auto iter = _children.begin(); iter != _children.end();) {
        auto s = (*iter)->ensure_first_row_ref();
        if (!s.ok()) {
            iter = _children.erase(iter);
            if (!s.is<END_OF_FILE>()) {
                return s;
            }
        } else {
            // we get a real row
            break;
        }
    }

    return Status::OK();
}

void VCollectIterator::Level1Iterator::init_level0_iterators_for_union() {
    bool have_multiple_child = false;
    for (auto iter = _children.begin(); iter != _children.end();) {
        (*iter)->init_for_union(have_multiple_child);
        have_multiple_child = true;
        ++iter;
    }
}

Status VCollectIterator::Level1Iterator::_merge_next(IteratorRowRef* ref) {
    auto res = _cur_child->next(ref);
    if (LIKELY(res.ok())) {
        _heap->push(_cur_child.release());
        _cur_child.reset(_heap->top());
        _heap->pop();
    } else if (res.is<END_OF_FILE>()) {
        // current child has been read, to read next
        if (!_heap->empty()) {
            _cur_child.reset(_heap->top());
            _heap->pop();
        } else {
            _ref.reset();
            _cur_child.reset();
            return Status::Error<END_OF_FILE>("");
        }
    } else {
        _ref.reset();
        _cur_child.reset();
        LOG(WARNING) << "failed to get next from child, res=" << res;
        return res;
    }

    if (_skip_same && _cur_child->is_same()) {
        _reader->_merged_rows++;
        _cur_child->set_same(false);
        return _merge_next(ref);
    }

    _ref = *_cur_child->current_row_ref();
    *ref = _ref;

    _cur_child->set_same(false);

    return Status::OK();
}

Status VCollectIterator::Level1Iterator::_normal_next(IteratorRowRef* ref) {
    auto res = _cur_child->next(ref);
    if (LIKELY(res.ok())) {
        _ref = *ref;
        return Status::OK();
    } else if (res.is<END_OF_FILE>()) {
        // current child has been read, to read next
        if (!_children.empty()) {
            _cur_child = std::move(*(_children.begin()));
            _children.pop_front();
            return _normal_next(ref);
        } else {
            _cur_child.reset();
            return Status::Error<END_OF_FILE>("");
        }
    } else {
        _cur_child.reset();
        LOG(WARNING) << "failed to get next from child, res=" << res;
        return res;
    }
}

Status VCollectIterator::Level1Iterator::_merge_next(Block* block) {
    int target_block_row = 0;
    auto target_columns = block->mutate_columns();
    size_t column_count = block->columns();
    IteratorRowRef cur_row = _ref;
    IteratorRowRef pre_row_ref = _ref;

    // append extra columns (eg. MATCH pred result column) from src_block to block
    for (size_t i = block->columns(); i < cur_row.block->columns(); ++i) {
        block->insert(cur_row.block->get_by_position(i).clone_empty());
    }

    auto batch_size = _reader->batch_size();
    if (UNLIKELY(_reader->_reader_context.record_rowids)) {
        _block_row_locations.resize(batch_size);
    }
    int continuous_row_in_block = 0;
    do {
        if (UNLIKELY(_reader->_reader_context.record_rowids)) {
            _block_row_locations[target_block_row] = _cur_child->current_row_location();
        }
        ++target_block_row;
        ++continuous_row_in_block;
        // cur block finished, copy before merge_next cause merge_next will
        // clear block column data
        if (pre_row_ref.row_pos + continuous_row_in_block == pre_row_ref.block->rows()) {
            const auto& src_block = pre_row_ref.block;
            RETURN_IF_CATCH_EXCEPTION({
                for (size_t i = 0; i < column_count; ++i) {
                    target_columns[i]->insert_range_from(*(src_block->get_by_position(i).column),
                                                         pre_row_ref.row_pos,
                                                         continuous_row_in_block);
                }
            });
            continuous_row_in_block = 0;
            pre_row_ref.reset();
        }
        auto res = _merge_next(&cur_row);
        if (UNLIKELY(res.is<END_OF_FILE>())) {
            if (UNLIKELY(_reader->_reader_context.record_rowids)) {
                _block_row_locations.resize(target_block_row);
            }
            return res;
        }

        if (UNLIKELY(!res.ok())) {
            LOG(WARNING) << "next failed: " << res;
            return res;
        }
        if (target_block_row >= batch_size) {
            if (continuous_row_in_block > 0) {
                const auto& src_block = pre_row_ref.block;
                for (size_t i = 0; i < column_count; ++i) {
                    target_columns[i]->insert_range_from(*(src_block->get_by_position(i).column),
                                                         pre_row_ref.row_pos,
                                                         continuous_row_in_block);
                }
            }
            return Status::OK();
        }
        if (continuous_row_in_block == 0) {
            pre_row_ref = _ref;
            continue;
        }
        // copy row if meet a new block
        if (cur_row.block != pre_row_ref.block) {
            const auto& src_block = pre_row_ref.block;
            for (size_t i = 0; i < column_count; ++i) {
                target_columns[i]->insert_range_from(*(src_block->get_by_position(i).column),
                                                     pre_row_ref.row_pos, continuous_row_in_block);
            }
            continuous_row_in_block = 0;
            pre_row_ref = cur_row;
        }
    } while (true);

    return Status::OK();
}

Status VCollectIterator::Level1Iterator::_normal_next(Block* block) {
    auto res = _cur_child->next(block);
    if (LIKELY(res.ok())) {
        return Status::OK();
    } else if (res.is<END_OF_FILE>()) {
        // current child has been read, to read next
        if (!_children.empty()) {
            _cur_child = std::move(*(_children.begin()));
            _children.pop_front();
            return _normal_next(block);
        } else {
            _cur_child.reset();
            return Status::Error<END_OF_FILE>("");
        }
    } else {
        _cur_child.reset();
        LOG(WARNING) << "failed to get next from child, res=" << res;
        return res;
    }
}

Status VCollectIterator::Level1Iterator::current_block_row_locations(
        std::vector<RowLocation>* block_row_locations) {
    if (!_merge) {
        if (UNLIKELY(_cur_child == nullptr)) {
            block_row_locations->clear();
            return Status::Error<END_OF_FILE>("");
        }
        return _cur_child->current_block_row_locations(block_row_locations);
    } else {
        DCHECK(_reader->_reader_context.record_rowids);
        *block_row_locations = _block_row_locations;
        return Status::OK();
    }
}

} // namespace vectorized
} // namespace doris
