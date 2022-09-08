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

#include "common/status.h"
#include "util/defer_op.h"

namespace doris {
namespace vectorized {

#define RETURN_IF_NOT_EOF_AND_OK(stmt)                                                  \
    do {                                                                                \
        const Status& _status_ = (stmt);                                                \
        if (UNLIKELY(!_status_.ok() && _status_.precise_code() != OLAP_ERR_DATA_EOF)) { \
            return _status_;                                                            \
        }                                                                               \
    } while (false)

void VCollectIterator::init(TabletReader* reader, bool force_merge, bool is_reverse) {
    _reader = reader;
    // when aggregate is enabled or key_type is DUP_KEYS, we don't merge
    // multiple data to aggregate for better performance
    if (_reader->_reader_type == READER_QUERY &&
        (_reader->_direct_mode || _reader->_tablet->keys_type() == KeysType::DUP_KEYS ||
         (_reader->_tablet->keys_type() == KeysType::UNIQUE_KEYS &&
          _reader->_tablet->enable_unique_key_merge_on_write()))) {
        _merge = false;
    }

    if (force_merge) {
        _merge = true;
    }
    _is_reverse = is_reverse;
}

Status VCollectIterator::add_child(RowsetReaderSharedPtr rs_reader) {
    std::unique_ptr<LevelIterator> child(new Level0Iterator(rs_reader, _reader));
    _children.push_back(child.release());
    return Status::OK();
}

// Build a merge heap. If _merge is true, a rowset with the max rownum
// status will be used as the base rowset, and the other rowsets will be merged first and
// then merged with the base rowset.
Status VCollectIterator::build_heap(std::vector<RowsetReaderSharedPtr>& rs_readers) {
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
                delete (*c_iter);
                c_iter = _children.erase(c_iter);
                r_iter = rs_readers.erase(r_iter);
                if (s.precise_code() != OLAP_ERR_DATA_EOF) {
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

            std::list<LevelIterator*> cumu_children;
            int i = 0;
            for (const auto& child : _children) {
                if (i != base_reader_idx) {
                    cumu_children.push_back(child);
                }
                ++i;
            }
            Level1Iterator* cumu_iter = new Level1Iterator(
                    cumu_children, _reader, cumu_children.size() > 1, _is_reverse, _skip_same);
            RETURN_IF_NOT_EOF_AND_OK(cumu_iter->init());
            std::list<LevelIterator*> children;
            children.push_back(*base_reader_child);
            children.push_back(cumu_iter);
            _inner_iter.reset(
                    new Level1Iterator(children, _reader, _merge, _is_reverse, _skip_same));
        } else {
            // _children.size() == 1
            _inner_iter.reset(
                    new Level1Iterator(_children, _reader, _merge, _is_reverse, _skip_same));
        }
    } else {
        _inner_iter.reset(new Level1Iterator(_children, _reader, _merge, _is_reverse, _skip_same));
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
            return Status::OLAPInternalError(OLAP_ERR_DATA_EOF);
        } else {
            return Status::OK();
        }
    }
    return Status::OLAPInternalError(OLAP_ERR_DATA_ROW_BLOCK_ERROR);
}

Status VCollectIterator::next(IteratorRowRef* ref) {
    if (LIKELY(_inner_iter)) {
        return _inner_iter->next(ref);
    } else {
        return Status::OLAPInternalError(OLAP_ERR_DATA_EOF);
    }
}

Status VCollectIterator::next(Block* block) {
    if (LIKELY(_inner_iter)) {
        return _inner_iter->next(block);
    } else {
        return Status::OLAPInternalError(OLAP_ERR_DATA_EOF);
    }
}

VCollectIterator::Level0Iterator::Level0Iterator(RowsetReaderSharedPtr rs_reader,
                                                 TabletReader* reader)
        : LevelIterator(reader), _rs_reader(rs_reader), _reader(reader) {
    DCHECK_EQ(RowsetTypePB::BETA_ROWSET, rs_reader->type());
}

Status VCollectIterator::Level0Iterator::init(bool get_data_by_ref) {
    _get_data_by_ref = get_data_by_ref && _rs_reader->support_return_data_by_ref() &&
                       config::enable_storage_vectorization;
    if (!_get_data_by_ref) {
        _block = std::make_shared<Block>(_schema.create_block(
                _reader->_return_columns, _reader->_tablet_columns_convert_to_null_set));
    }
    auto st = _refresh_current_row();
    if (_get_data_by_ref && _block_view.size()) {
        _ref = _block_view[0];
    } else {
        _ref = {_block, 0, false};
    }
    return st;
}

int64_t VCollectIterator::Level0Iterator::version() const {
    return _rs_reader->version().second;
}

Status VCollectIterator::Level0Iterator::_refresh_current_row() {
    do {
        if (!_is_empty() && _current_valid()) {
            return Status::OK();
        } else {
            _reset();
            auto res = _refresh();
            if (!res.ok() && res.precise_code() != OLAP_ERR_DATA_EOF) {
                return res;
            }
            if (res.precise_code() == OLAP_ERR_DATA_EOF && _is_empty()) {
                break;
            }

            if (UNLIKELY(_reader->_reader_context.record_rowids)) {
                RETURN_NOT_OK(_rs_reader->current_block_row_locations(&_block_row_locations));
            }
        }
    } while (!_is_empty());
    _ref.row_pos = -1;
    _current = -1;
    return Status::OLAPInternalError(OLAP_ERR_DATA_EOF);
}

Status VCollectIterator::Level0Iterator::next(IteratorRowRef* ref) {
    if (_get_data_by_ref) {
        _current++;
    } else {
        _ref.row_pos++;
    }

    RETURN_NOT_OK(_refresh_current_row());

    if (_get_data_by_ref) {
        _ref = _block_view[_current];
    }

    *ref = _ref;
    return Status::OK();
}

Status VCollectIterator::Level0Iterator::next(Block* block) {
    CHECK(!_get_data_by_ref);
    if (_ref.row_pos == 0 && _ref.block != nullptr && UNLIKELY(_ref.block->rows() > 0)) {
        block->swap(*_ref.block);
        _ref.reset();
        return Status::OK();
    } else {
        auto res = _rs_reader->next_block(block);
        if (!res.ok() && res.precise_code() != OLAP_ERR_DATA_EOF) {
            return res;
        }
        if (res.precise_code() == OLAP_ERR_DATA_EOF && block->rows() == 0) {
            return Status::OLAPInternalError(OLAP_ERR_DATA_EOF);
        }
        if (UNLIKELY(_reader->_reader_context.record_rowids)) {
            RETURN_NOT_OK(_rs_reader->current_block_row_locations(&_block_row_locations));
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
        const std::list<VCollectIterator::LevelIterator*>& children, TabletReader* reader,
        bool merge, bool is_reverse, bool skip_same)
        : LevelIterator(reader),
          _children(children),
          _reader(reader),
          _merge(merge),
          _is_reverse(is_reverse),
          _skip_same(skip_same) {
    _ref.reset();
    _batch_size = reader->_batch_size;
}

VCollectIterator::Level1Iterator::~Level1Iterator() {
    for (auto child : _children) {
        if (child != nullptr) {
            delete child;
            child = nullptr;
        }
    }

    if (_heap) {
        while (!_heap->empty()) {
            auto child = _heap->top();
            _heap->pop();
            if (child) {
                delete child;
            }
        }
    }
}

// Read next row into *row.
// Returns
//      OLAP_SUCCESS when read successfully.
//      Status::OLAPInternalError(OLAP_ERR_DATA_EOF) and set *row to nullptr when EOF is reached.
//      Others when error happens
Status VCollectIterator::Level1Iterator::next(IteratorRowRef* ref) {
    if (UNLIKELY(_cur_child == nullptr)) {
        _ref.reset();
        return Status::OLAPInternalError(OLAP_ERR_DATA_EOF);
    }
    if (_merge) {
        return _merge_next(ref);
    } else {
        return _normal_next(ref);
    }
}

// Read next block
// Returns
//      OLAP_SUCCESS when read successfully.
//      Status::OLAPInternalError(OLAP_ERR_DATA_EOF) and set *row to nullptr when EOF is reached.
//      Others when error happens
Status VCollectIterator::Level1Iterator::next(Block* block) {
    if (UNLIKELY(_cur_child == nullptr)) {
        return Status::OLAPInternalError(OLAP_ERR_DATA_EOF);
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
        for (auto child : _children) {
            DCHECK(child != nullptr);
            //DCHECK(child->current_row().ok());
            _heap->push(child);
        }
        _cur_child = _heap->top();
        // Clear _children earlier to release any related references
        _children.clear();
    } else {
        _merge = false;
        _heap.reset(nullptr);
        _cur_child = *_children.begin();
    }
    _ref = *_cur_child->current_row_ref();
    return Status::OK();
}

Status VCollectIterator::Level1Iterator::_merge_next(IteratorRowRef* ref) {
    _heap->pop();
    auto res = _cur_child->next(ref);
    if (LIKELY(res.ok())) {
        _heap->push(_cur_child);
        _cur_child = _heap->top();
    } else if (res.precise_code() == OLAP_ERR_DATA_EOF) {
        // current child has been read, to read next
        delete _cur_child;
        if (!_heap->empty()) {
            _cur_child = _heap->top();
        } else {
            _ref.reset();
            _cur_child = nullptr;
            return Status::OLAPInternalError(OLAP_ERR_DATA_EOF);
        }
    } else {
        _ref.reset();
        _cur_child = nullptr;
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
    } else if (res.precise_code() == OLAP_ERR_DATA_EOF) {
        // current child has been read, to read next
        delete _cur_child;
        _children.pop_front();
        if (!_children.empty()) {
            _cur_child = *(_children.begin());
            return _normal_next(ref);
        } else {
            _cur_child = nullptr;
            return Status::OLAPInternalError(OLAP_ERR_DATA_EOF);
        }
    } else {
        _cur_child = nullptr;
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

    if (UNLIKELY(_reader->_reader_context.record_rowids)) {
        _block_row_locations.resize(_batch_size);
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
            for (size_t i = 0; i < column_count; ++i) {
                target_columns[i]->insert_range_from(*(src_block->get_by_position(i).column),
                                                     pre_row_ref.row_pos, continuous_row_in_block);
            }
            continuous_row_in_block = 0;
            pre_row_ref.reset();
        }
        auto res = _merge_next(&cur_row);
        if (UNLIKELY(res.precise_code() == OLAP_ERR_DATA_EOF)) {
            if (UNLIKELY(_reader->_reader_context.record_rowids)) {
                _block_row_locations.resize(target_block_row);
            }
            return res;
        }

        if (UNLIKELY(!res.ok())) {
            LOG(WARNING) << "next failed: " << res;
            return res;
        }
        if (target_block_row >= _batch_size) {
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
    } else if (res.precise_code() == OLAP_ERR_DATA_EOF) {
        // current child has been read, to read next
        delete _cur_child;
        _children.pop_front();
        if (!_children.empty()) {
            _cur_child = *(_children.begin());
            return _normal_next(block);
        } else {
            _cur_child = nullptr;
            return Status::OLAPInternalError(OLAP_ERR_DATA_EOF);
        }
    } else {
        _cur_child = nullptr;
        LOG(WARNING) << "failed to get next from child, res=" << res;
        return res;
    }
}

Status VCollectIterator::Level1Iterator::current_block_row_locations(
        std::vector<RowLocation>* block_row_locations) {
    if (!_merge) {
        if (UNLIKELY(_cur_child == nullptr)) {
            block_row_locations->clear();
            return Status::OLAPInternalError(OLAP_ERR_DATA_EOF);
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
