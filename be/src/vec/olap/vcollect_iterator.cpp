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
#include <memory>

#include "olap/rowset/beta_rowset_reader.h"

namespace doris {
namespace vectorized {

VCollectIterator::~VCollectIterator() {}

#define RETURN_IF_NOT_EOF_AND_OK(stmt)                                                  \
    do {                                                                                \
        OLAPStatus _status_ = (stmt);                                                   \
        if (UNLIKELY(_status_ != OLAP_SUCCESS && _status_ != OLAP_ERR_DATA_EOF)) {      \
            return _status_;                                                            \
        }                                                                               \
    } while (false)

void VCollectIterator::init(TabletReader* reader) {
    _reader = reader;
    // when aggregate is enabled or key_type is DUP_KEYS, we don't merge
    // multiple data to aggregate for better performance
    if (_reader->_reader_type == READER_QUERY &&
        (_reader->_direct_mode || _reader->_tablet->keys_type() == KeysType::DUP_KEYS)) {
        _merge = false;
    }
}

OLAPStatus VCollectIterator::add_child(RowsetReaderSharedPtr rs_reader) {
    std::unique_ptr<LevelIterator> child(new Level0Iterator(rs_reader, _reader));
    _children.push_back(child.release());
    return OLAP_SUCCESS;
}

// Build a merge heap. If _merge is true, a rowset with the max rownum
// status will be used as the base rowset, and the other rowsets will be merged first and
// then merged with the base rowset.
OLAPStatus VCollectIterator::build_heap(std::vector<RowsetReaderSharedPtr>& rs_readers) {
    DCHECK(rs_readers.size() == _children.size());
    _skip_same = _reader->_tablet->tablet_schema().keys_type() == KeysType::UNIQUE_KEYS;
    if (_children.empty()) {
        _inner_iter.reset(nullptr);
        return OLAP_SUCCESS;
    } else if (_merge) {
        DCHECK(!rs_readers.empty());
        for (auto [c_iter, r_iter] = std::pair {_children.begin(), rs_readers.begin()};
             c_iter != _children.end();) {
            OLAPStatus s = (*c_iter)->init();
            if (s != OLAP_SUCCESS) {
                delete (*c_iter);
                c_iter = _children.erase(c_iter);
                r_iter = rs_readers.erase(r_iter);
                if (s != OLAP_ERR_DATA_EOF) {
                    return s;
                }
            } else {
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
            Level1Iterator* cumu_iter = new Level1Iterator(cumu_children, _reader,
                                                           cumu_children.size() > 1, _skip_same);
            RETURN_IF_NOT_EOF_AND_OK(cumu_iter->init());
            std::list<LevelIterator*> children;
            children.push_back(*base_reader_child);
            children.push_back(cumu_iter);
            _inner_iter.reset(new Level1Iterator(children, _reader, _merge, _skip_same));
        } else {
            // _children.size() == 1
            _inner_iter.reset(new Level1Iterator(_children, _reader, _merge, _skip_same));
        }
    } else {
        _inner_iter.reset(new Level1Iterator(_children, _reader, _merge, _skip_same));
    }
    RETURN_IF_NOT_EOF_AND_OK(_inner_iter->init());
    // Clear _children earlier to release any related references
    _children.clear();
    return OLAP_SUCCESS;
}

bool VCollectIterator::LevelIteratorComparator::operator()(LevelIterator* lhs, LevelIterator* rhs) {
    const IteratorRowRef& lhs_ref = *lhs->current_row_ref();
    const IteratorRowRef& rhs_ref = *rhs->current_row_ref();

    int cmp_res =
            lhs_ref.block->compare_at(lhs_ref.row_pos, rhs_ref.row_pos,
                                      lhs->tablet_schema().num_key_columns(), *rhs_ref.block, -1);
    if (cmp_res != 0) {
        return cmp_res > 0;
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

OLAPStatus VCollectIterator::current_row(IteratorRowRef* ref) const {
    if (LIKELY(_inner_iter)) {
        *ref = *_inner_iter->current_row_ref();
        if (ref->row_pos == -1) {
            return OLAP_ERR_DATA_EOF;
        } else {
            return OLAP_SUCCESS;
        }
    }
    return OLAP_ERR_DATA_ROW_BLOCK_ERROR;
}

OLAPStatus VCollectIterator::next(IteratorRowRef* ref) {
    if (LIKELY(_inner_iter)) {
        return _inner_iter->next(ref);
    } else {
        return OLAP_ERR_DATA_EOF;
    }
}

OLAPStatus VCollectIterator::next(Block* block) {
    if (LIKELY(_inner_iter)) {
        return _inner_iter->next(block);
    } else {
        return OLAP_ERR_DATA_EOF;
    }
}

VCollectIterator::Level0Iterator::Level0Iterator(RowsetReaderSharedPtr rs_reader, TabletReader* reader)
        : LevelIterator(reader), _rs_reader(rs_reader), _reader(reader) {
    DCHECK_EQ(RowsetTypePB::BETA_ROWSET, rs_reader->type());
    _block = std::make_shared<Block>(_schema.create_block(_reader->_return_columns, _reader->_tablet_columns_convert_to_null_set));
    _ref.block = _block;
    _ref.row_pos = 0;
    _ref.is_same = false;
}

OLAPStatus VCollectIterator::Level0Iterator::init() {
    return _refresh_current_row();
}

int64_t VCollectIterator::Level0Iterator::version() const {
    return _rs_reader->version().second;
}

OLAPStatus VCollectIterator::Level0Iterator::_refresh_current_row() {
    do {
        if (_block->rows() != 0 && _ref.row_pos < _block->rows()) {
            return OLAP_SUCCESS;
        } else {
            _ref.is_same = false;
            _ref.row_pos = 0;
            _block->clear_column_data();
            auto res = _rs_reader->next_block(_block.get());
            if (res != OLAP_SUCCESS && res != OLAP_ERR_DATA_EOF) {
                return res;
            }
            if (res == OLAP_ERR_DATA_EOF && _block->rows() == 0) {
                _ref.row_pos = -1;
                return OLAP_ERR_DATA_EOF;
            }
        }
    } while (_block->rows() != 0);
    _ref.row_pos = -1;
    return OLAP_ERR_DATA_EOF;
}

OLAPStatus VCollectIterator::Level0Iterator::next(IteratorRowRef* ref) {
    _ref.row_pos++;
    RETURN_NOT_OK(_refresh_current_row());
    *ref = _ref;
    return OLAP_SUCCESS;
}

OLAPStatus VCollectIterator::Level0Iterator::next(Block* block) {
    if (UNLIKELY(_ref.block->rows() > 0 && _ref.row_pos == 0)) {
        block->swap(*_ref.block);
        _ref.row_pos = -1;
        return OLAP_SUCCESS;
    } else {
        auto res = _rs_reader->next_block(block);
        if (res != OLAP_SUCCESS && res != OLAP_ERR_DATA_EOF) {
            return res;
        }
        if (res == OLAP_ERR_DATA_EOF && _block->rows() == 0) {
            return OLAP_ERR_DATA_EOF;
        }
        return OLAP_SUCCESS;
    }
}

VCollectIterator::Level1Iterator::Level1Iterator(
        const std::list<VCollectIterator::LevelIterator*>& children, TabletReader* reader, bool merge,
        bool skip_same)
        : LevelIterator(reader),
          _children(children),
          _reader(reader),
          _merge(merge),
          _skip_same(skip_same) {
    _ref.row_pos = -1; // represent eof
}

VCollectIterator::Level1Iterator::~Level1Iterator() {
    for (auto child : _children) {
        if (child != nullptr) {
            delete child;
            child = nullptr;
        }
    }
}

// Read next row into *row.
// Returns
//      OLAP_SUCCESS when read successfully.
//      OLAP_ERR_DATA_EOF and set *row to nullptr when EOF is reached.
//      Others when error happens
OLAPStatus VCollectIterator::Level1Iterator::next(IteratorRowRef* ref) {
    if (UNLIKELY(_cur_child == nullptr)) {
        _ref.row_pos = -1;
        return OLAP_ERR_DATA_EOF;
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
//      OLAP_ERR_DATA_EOF and set *row to nullptr when EOF is reached.
//      Others when error happens
OLAPStatus VCollectIterator::Level1Iterator::next(Block* block) {
    if (UNLIKELY(_cur_child == nullptr)) {
        return OLAP_ERR_DATA_EOF;
    }
    return _normal_next(block);
}

int64_t VCollectIterator::Level1Iterator::version() const {
    if (_cur_child != nullptr) {
        return _cur_child->version();
    }
    return -1;
}

OLAPStatus VCollectIterator::Level1Iterator::init() {
    if (_children.empty()) {
        return OLAP_SUCCESS;
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
        _heap.reset(new MergeHeap {LevelIteratorComparator(sequence_loc)});
        for (auto child : _children) {
            DCHECK(child != nullptr);
            //DCHECK(child->current_row() == OLAP_SUCCESS);
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
    return OLAP_SUCCESS;
}

OLAPStatus VCollectIterator::Level1Iterator::_merge_next(IteratorRowRef* ref) {
    _heap->pop();
    auto res = _cur_child->next(ref);
    if (LIKELY(res == OLAP_SUCCESS)) {
        _heap->push(_cur_child);
        _cur_child = _heap->top();
    } else if (res == OLAP_ERR_DATA_EOF) {
        // current child has been read, to read next
        delete _cur_child;
        if (!_heap->empty()) {
            _cur_child = _heap->top();
        } else {
            _cur_child = nullptr;
            _ref.row_pos = -1;
            return OLAP_ERR_DATA_EOF;
        }
    } else {
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

    return OLAP_SUCCESS;
}

OLAPStatus VCollectIterator::Level1Iterator::_normal_next(IteratorRowRef* ref) {
    auto res = _cur_child->next(ref);
    if (LIKELY(res == OLAP_SUCCESS)) {
        _ref = *ref;
        return OLAP_SUCCESS;
    } else if (res == OLAP_ERR_DATA_EOF) {
        // current child has been read, to read next
        delete _cur_child;
        _children.pop_front();
        if (!_children.empty()) {
            _cur_child = *(_children.begin());
            return _normal_next(ref);
        } else {
            _cur_child = nullptr;
            return OLAP_ERR_DATA_EOF;
        }
    } else {
        _cur_child = nullptr;
        LOG(WARNING) << "failed to get next from child, res=" << res;
        return res;
    }
}

OLAPStatus VCollectIterator::Level1Iterator::_normal_next(Block* block) {
    auto res = _cur_child->next(block);
    if (LIKELY(res == OLAP_SUCCESS)) {
        return OLAP_SUCCESS;
    } else if (res == OLAP_ERR_DATA_EOF) {
        // current child has been read, to read next
        delete _cur_child;
        _children.pop_front();
        if (!_children.empty()) {
            _cur_child = *(_children.begin());
            return _normal_next(block);
        } else {
            _cur_child = nullptr;
            return OLAP_ERR_DATA_EOF;
        }
    } else {
        _cur_child = nullptr;
        LOG(WARNING) << "failed to get next from child, res=" << res;
        return res;
    }
}

} // namespace vectorized
} // namespace doris
