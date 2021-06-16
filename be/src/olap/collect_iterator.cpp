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

#include "olap/collect_iterator.h"

#include "olap/reader.h"
#include "olap/row.h"
#include "olap/row_block.h"
#include "olap/row_cursor.h"
#include "olap/rowset/beta_rowset_reader.h"

namespace doris {

CollectIterator::~CollectIterator() {}

void CollectIterator::init(Reader* reader) {
    _reader = reader;
    // when aggregate is enabled or key_type is DUP_KEYS, we don't merge
    // multiple data to aggregate for performance in user fetch
    if (_reader->_reader_type == READER_QUERY &&
        (_reader->_aggregation || _reader->_tablet->keys_type() == KeysType::DUP_KEYS)) {
        _merge = false;
    }
}

OLAPStatus CollectIterator::add_child(RowsetReaderSharedPtr rs_reader) {
    std::unique_ptr<LevelIterator> child(new Level0Iterator(rs_reader, _reader));
    RETURN_NOT_OK(child->init());
    if (child->current_row() == nullptr) {
        return OLAP_ERR_DATA_EOF;
    }

    _children.push_back(child.release());
    return OLAP_SUCCESS;
}

// Build a merge heap. If _merge is true, a rowset with the max rownum
// status will be used as the base rowset, and the other rowsets will be merged first and
// then merged with the base rowset.
void CollectIterator::build_heap(const std::vector<RowsetReaderSharedPtr>& rs_readers) {
    DCHECK(rs_readers.size() == _children.size());
    _reverse = _reader->_tablet->tablet_schema().keys_type() == KeysType::UNIQUE_KEYS;
    if (_children.empty()) {
        _inner_iter.reset(nullptr);
        return;
    } else if (_merge) {
        DCHECK(!rs_readers.empty());
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
            Level1Iterator* cumu_iter =
                    new Level1Iterator(cumu_children, cumu_children.size() > 1, _reverse);
            cumu_iter->init();
            std::list<LevelIterator*> children;
            children.push_back(*base_reader_child);
            children.push_back(cumu_iter);
            _inner_iter.reset(new Level1Iterator(children, _merge, _reverse));
        } else {
            // _children.size() == 1
            _inner_iter.reset(new Level1Iterator(_children, _merge, _reverse));
        }
    } else {
        _inner_iter.reset(new Level1Iterator(_children, _merge, _reverse));
    }
    _inner_iter->init();
    // Clear _children earlier to release any related references
    _children.clear();
}

bool CollectIterator::LevelIteratorComparator::operator()(const LevelIterator* a,
                                                          const LevelIterator* b) {
    // First compare row cursor.
    const RowCursor* first = a->current_row();
    const RowCursor* second = b->current_row();
    int cmp_res = compare_row(*first, *second);
    if (cmp_res != 0) {
        return cmp_res > 0;
    }
    // if row cursors equal, compare data version.
    // read data from higher version to lower version.
    // for UNIQUE_KEYS just read the highest version and no need agg_update.
    // for AGG_KEYS if a version is deleted, the lower version no need to agg_update
    if (_reverse) {
        return a->version() < b->version();
    }
    return a->version() > b->version();
}

const RowCursor* CollectIterator::current_row(bool* delete_flag) const {
    if (LIKELY(_inner_iter)) {
        return _inner_iter->current_row(delete_flag);
    }
    return nullptr;
}

OLAPStatus CollectIterator::next(const RowCursor** row, bool* delete_flag) {
    if (LIKELY(_inner_iter)) {
        return _inner_iter->next(row, delete_flag);
    } else {
        return OLAP_ERR_DATA_EOF;
    }
}

CollectIterator::Level0Iterator::Level0Iterator(RowsetReaderSharedPtr rs_reader, Reader* reader)
        : _rs_reader(rs_reader), _is_delete(rs_reader->delete_flag()), _reader(reader) {
    auto* ans = dynamic_cast<BetaRowsetReader*>(rs_reader.get());
    if (LIKELY(ans != nullptr)) {
        _refresh_current_row = &Level0Iterator::_refresh_current_row_v2;
    } else {
        _refresh_current_row = &Level0Iterator::_refresh_current_row_v1;
    }
}

CollectIterator::Level0Iterator::~Level0Iterator() {}

OLAPStatus CollectIterator::Level0Iterator::init() {
    auto res = _row_cursor.init(_reader->_tablet->tablet_schema(), _reader->_seek_columns);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "failed to init row cursor, res=" << res;
        return res;
    }
    RETURN_NOT_OK((this->*_refresh_current_row)());
    return OLAP_SUCCESS;
}

const RowCursor* CollectIterator::Level0Iterator::current_row(bool* delete_flag) const {
    *delete_flag = _is_delete || _current_row->is_delete();
    return _current_row;
}

const RowCursor* CollectIterator::Level0Iterator::current_row() const {
    return _current_row;
}

int64_t CollectIterator::Level0Iterator::version() const {
    return _rs_reader->version().second;
}

OLAPStatus CollectIterator::Level0Iterator::_refresh_current_row_v1() {
    do {
        if (_row_block != nullptr && _row_block->has_remaining()) {
            size_t pos = _row_block->pos();
            _row_block->get_row(pos, &_row_cursor);
            if (_row_block->block_status() == DEL_PARTIAL_SATISFIED &&
                _reader->_delete_handler.is_filter_data(version(), _row_cursor)) {
                _reader->_stats.rows_del_filtered++;
                _row_block->pos_inc();
                continue;
            }
            _current_row = &_row_cursor;
            return OLAP_SUCCESS;
        } else {
            auto res = _rs_reader->next_block(&_row_block);
            if (res != OLAP_SUCCESS) {
                _current_row = nullptr;
                return res;
            }
        }
    } while (_row_block != nullptr);
    _current_row = nullptr;
    return OLAP_ERR_DATA_EOF;
}

OLAPStatus CollectIterator::Level0Iterator::_refresh_current_row_v2() {
    do {
        if (_row_block != nullptr && _row_block->has_remaining()) {
            size_t pos = _row_block->pos();
            _row_block->get_row(pos, &_row_cursor);
            _current_row = &_row_cursor;
            return OLAP_SUCCESS;
        } else {
            auto res = _rs_reader->next_block(&_row_block);
            if (res != OLAP_SUCCESS) {
                _current_row = nullptr;
                return res;
            }
        }
    } while (_row_block != nullptr);
    _current_row = nullptr;
    return OLAP_ERR_DATA_EOF;
}

OLAPStatus CollectIterator::Level0Iterator::next(const RowCursor** row, bool* delete_flag) {
    _row_block->pos_inc();
    auto res = (this->*_refresh_current_row)();
    *row = _current_row;
    *delete_flag = _is_delete;
    if (_current_row != nullptr) {
        *delete_flag = _is_delete || _current_row->is_delete();
    }
    return res;
}

CollectIterator::Level1Iterator::Level1Iterator(
        const std::list<CollectIterator::LevelIterator*>& children, bool merge, bool reverse)
        : _children(children), _merge(merge), _reverse(reverse) {}

CollectIterator::LevelIterator::~LevelIterator() {}

CollectIterator::Level1Iterator::~Level1Iterator() {
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
OLAPStatus CollectIterator::Level1Iterator::next(const RowCursor** row, bool* delete_flag) {
    if (UNLIKELY(_cur_child == nullptr)) {
        return OLAP_ERR_DATA_EOF;
    }
    if (_merge) {
        return _merge_next(row, delete_flag);
    } else {
        return _normal_next(row, delete_flag);
    }
}

// Get top row of the heap, nullptr if reach end.
const RowCursor* CollectIterator::Level1Iterator::current_row(bool* delete_flag) const {
    if (_cur_child != nullptr) {
        return _cur_child->current_row(delete_flag);
    }
    return nullptr;
}

// Get top row of the heap, nullptr if reach end.
const RowCursor* CollectIterator::Level1Iterator::current_row() const {
    if (_cur_child != nullptr) {
        return _cur_child->current_row();
    }
    return nullptr;
}

int64_t CollectIterator::Level1Iterator::version() const {
    if (_cur_child != nullptr) {
        return _cur_child->version();
    }
    return -1;
}

OLAPStatus CollectIterator::Level1Iterator::init() {
    if (_children.empty()) {
        return OLAP_SUCCESS;
    }

    // Only when there are multiple children that need to be merged
    if (_merge && _children.size() > 1) {
        _heap.reset(new MergeHeap(LevelIteratorComparator(_reverse)));
        for (auto child : _children) {
            DCHECK(child != nullptr);
            DCHECK(child->current_row() != nullptr);
            _heap->push(child);
        }
        _cur_child = _heap->top();
        // Clear _children earlier to release any related references
        _children.clear();
    } else {
        _merge = false;
        _heap.reset(nullptr);
        _cur_child = *(_children.begin());
    }
    return OLAP_SUCCESS;
}

inline OLAPStatus CollectIterator::Level1Iterator::_merge_next(const RowCursor** row,
                                                               bool* delete_flag) {
    _heap->pop();
    auto res = _cur_child->next(row, delete_flag);
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
            return OLAP_ERR_DATA_EOF;
        }
    } else {
        _cur_child = nullptr;
        LOG(WARNING) << "failed to get next from child, res=" << res;
        return res;
    }
    *row = _cur_child->current_row(delete_flag);
    return OLAP_SUCCESS;
}

inline OLAPStatus CollectIterator::Level1Iterator::_normal_next(const RowCursor** row,
                                                                bool* delete_flag) {
    auto res = _cur_child->next(row, delete_flag);
    if (LIKELY(res == OLAP_SUCCESS)) {
        return OLAP_SUCCESS;
    } else if (res == OLAP_ERR_DATA_EOF) {
        // current child has been read, to read next
        delete _cur_child;
        _children.pop_front();
        if (!_children.empty()) {
            _cur_child = *(_children.begin());
            *row = _cur_child->current_row(delete_flag);
            return OLAP_SUCCESS;
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

} // namespace doris
