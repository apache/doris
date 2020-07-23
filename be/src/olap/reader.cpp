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

#include "olap/reader.h"

#include "olap/rowset/column_data.h"
#include "olap/tablet.h"
#include "olap/row_block.h"
#include "olap/row_cursor.h"
#include "util/date_func.h"
#include "util/mem_util.hpp"
#include "runtime/mem_tracker.h"
#include "runtime/mem_pool.h"
#include <sstream>

#include "olap/comparison_predicate.h"
#include "olap/in_list_predicate.h"
#include "olap/null_predicate.h"
#include "olap/storage_engine.h"
#include "olap/row.h"

using std::nothrow;
using std::set;
using std::vector;

namespace doris {

class CollectIterator {
public:
    ~CollectIterator();

    // Hold reader point to get reader params
    void init(Reader* reader);

    OLAPStatus add_child(RowsetReaderSharedPtr rs_reader);

    // Get top row of the heap, nullptr if reach end.
    const RowCursor* current_row(bool* delete_flag) const {
        if (_cur_child != nullptr) {
            return _cur_child->current_row(delete_flag);
        }
        return nullptr;
    }

    // Read next row into *row.
    // Returns
    //      OLAP_SUCCESS when read successfully.
    //      OLAP_ERR_DATA_EOF and set *row to nullptr when EOF is reached.
    //      Others when error happens
    OLAPStatus next(const RowCursor** row, bool* delete_flag) {
        DCHECK(_cur_child != nullptr);
        if (_merge) {
            return _merge_next(row, delete_flag);
        } else {
            return _normal_next(row, delete_flag);
        }
    }

    // Clear the MergeSet element and reset state.
    void clear();

private:
    class ChildCtx {
    public:
        ChildCtx(RowsetReaderSharedPtr rs_reader, Reader* reader)
                : _rs_reader(rs_reader),
                  _is_delete(rs_reader->delete_flag()),
                  _reader(reader) { }

        OLAPStatus init() {
            auto res = _row_cursor.init(_reader->_tablet->tablet_schema(), _reader->_seek_columns);
            if (res != OLAP_SUCCESS) {
                LOG(WARNING) << "failed to init row cursor, res=" << res;
                return res;
            }
            RETURN_NOT_OK(_refresh_current_row());
            return OLAP_SUCCESS;
        }

        const RowCursor* current_row(bool* delete_flag) const {
            *delete_flag = _is_delete;
            return _current_row;
        }

        const RowCursor* current_row() const {
            return _current_row;
        }

        int32_t version() const {
            return _rs_reader->version().second;
        }

        OLAPStatus next(const RowCursor** row, bool* delete_flag) {
            _row_block->pos_inc();
            auto res = _refresh_current_row();
            *row = _current_row;
            *delete_flag = _is_delete;
            return res;
        }

    private:
        // refresh_current_row
        OLAPStatus _refresh_current_row() {
            do {
                if (_row_block != nullptr && _row_block->has_remaining()) {
                    size_t pos = _row_block->pos();
                    _row_block->get_row(pos, &_row_cursor);
                    if (_row_block->block_status() == DEL_PARTIAL_SATISFIED &&
                        _reader->_delete_handler.is_filter_data(_rs_reader->version().second, _row_cursor)) {
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

        RowsetReaderSharedPtr _rs_reader;
        const RowCursor* _current_row = nullptr;
        bool _is_delete = false;
        Reader* _reader = nullptr;

        RowCursor _row_cursor; // point to rows inside `_row_block`
        RowBlock* _row_block = nullptr;
    };

    // Compare row cursors between multiple merge elements,
    // if row cursors equal, compare data version.
    class ChildCtxComparator {
    public:
        ChildCtxComparator(const bool& revparam=false) {
            _reverse = revparam;
        }
        bool operator()(const ChildCtx* a, const ChildCtx* b);
    private:
        bool _reverse;
    };

    inline OLAPStatus _merge_next(const RowCursor** row, bool* delete_flag);
    inline OLAPStatus _normal_next(const RowCursor** row, bool* delete_flag);

    // each ChildCtx corresponds to a rowset reader
    std::vector<ChildCtx*> _children;
    // point to the ChildCtx containing the next output row.
    // null when CollectIterator hasn't been initialized or reaches EOF.
    ChildCtx* _cur_child = nullptr;

    // when `_merge == true`, rowset reader returns ordered rows and CollectIterator uses a priority queue to merge
    // sort them. The output of CollectIterator is also ordered.
    // When `_merge == false`, rowset reader returns *partial* ordered rows. CollectIterator simply returns all rows
    // from the first rowset, the second rowset, .., the last rowset. The output of CollectorIterator is also
    // *partially* ordered.
    bool _merge = true;
    // used when `_merge == true`
    typedef std::priority_queue<ChildCtx*, std::vector<ChildCtx*>, ChildCtxComparator> MergeHeap;
    std::unique_ptr<MergeHeap> _heap;
    // used when `_merge == false`
    int _child_idx = 0;

    // Hold reader point to access read params, such as fetch conditions.
    Reader* _reader = nullptr;
};


CollectIterator::~CollectIterator() {
    for (auto child : _children) {
        delete child;
    }
}

void CollectIterator::init(Reader* reader) {
    _reader = reader;
    // when aggregate is enabled or key_type is DUP_KEYS, we don't merge
    // multiple data to aggregate for performance in user fetch
    if (_reader->_reader_type == READER_QUERY &&
            (_reader->_aggregation ||
             _reader->_tablet->keys_type() == KeysType::DUP_KEYS)) {
        _merge = false;
        _heap.reset(nullptr);
    } else if (_reader->_tablet->keys_type() == KeysType::UNIQUE_KEYS) {
        _heap.reset(new MergeHeap(ChildCtxComparator(true)));
    } else {
        _heap.reset(new MergeHeap());
    }
}

OLAPStatus CollectIterator::add_child(RowsetReaderSharedPtr rs_reader) {
    std::unique_ptr<ChildCtx> child(new ChildCtx(rs_reader, _reader));
    RETURN_NOT_OK(child->init());
    if (child->current_row() == nullptr) {
        return OLAP_SUCCESS;
    }

    ChildCtx* child_ptr = child.release();
    _children.push_back(child_ptr);
    if (_merge) {
        _heap->push(child_ptr);
        _cur_child = _heap->top();
    } else {
        if (_cur_child == nullptr) {
            _cur_child = _children[_child_idx];
        }
    }
    return OLAP_SUCCESS;
}

inline OLAPStatus CollectIterator::_merge_next(const RowCursor** row, bool* delete_flag) {
    _heap->pop();
    auto res = _cur_child->next(row, delete_flag);
    if (res == OLAP_SUCCESS) {
        _heap->push(_cur_child);
        _cur_child = _heap->top();
    } else if (res == OLAP_ERR_DATA_EOF) {
        if (!_heap->empty()) {
            _cur_child = _heap->top();
        } else {
            _cur_child = nullptr;
            return OLAP_ERR_DATA_EOF;
        }
    } else {
        LOG(WARNING) << "failed to get next from child, res=" << res;
        return res;
    }
    *row = _cur_child->current_row(delete_flag);
    return OLAP_SUCCESS;
}

inline OLAPStatus CollectIterator::_normal_next(const RowCursor** row, bool* delete_flag) {
    auto res = _cur_child->next(row, delete_flag);
    if (LIKELY(res == OLAP_SUCCESS)) {
        return OLAP_SUCCESS;
    } else if (res == OLAP_ERR_DATA_EOF) {
        // this child has been read, to read next
        _child_idx++;
        if (_child_idx < _children.size()) {
            _cur_child = _children[_child_idx];
            *row = _cur_child->current_row(delete_flag);
            return OLAP_SUCCESS;
        } else {
            _cur_child = nullptr;
            return OLAP_ERR_DATA_EOF;
        }
    } else {
        LOG(WARNING) << "failed to get next from child, res=" << res;
        return res;
    }
}

bool CollectIterator::ChildCtxComparator::operator()(const ChildCtx* a, const ChildCtx* b) {
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

void CollectIterator::clear() {
    while (_heap != nullptr && !_heap->empty()) {
        _heap->pop();
    }
    for (auto child : _children) {
        delete child;
    }
    // _children.swap(std::vector<ChildCtx*>());
    _children.clear();
    _cur_child = nullptr;
    _child_idx = 0;
}

Reader::Reader() {
    _tracker.reset(new MemTracker(-1));
    _predicate_mem_pool.reset(new MemPool(_tracker.get()));
}

Reader::~Reader() {
    close();
}

OLAPStatus Reader::init(const ReaderParams& read_params) {
    OLAPStatus res = _init_params(read_params);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to init reader when init params. res:" << res
                     << ", tablet_id:" << read_params.tablet->tablet_id()
                     << ", schema_hash:" << read_params.tablet->schema_hash()
                     << ", reader type:" << read_params.reader_type
                     << ", version:" << read_params.version;
        return res;
    }

    res = _capture_rs_readers(read_params);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to init reader when _capture_rs_readers. res:" << res
                     << ", tablet_id:" << read_params.tablet->tablet_id()
                     << ", schema_hash:" << read_params.tablet->schema_hash()
                     << ", reader_type:" << read_params.reader_type
                     << ", version:" << read_params.version;
        return res;
    }

    switch (_tablet->keys_type()) {
    case KeysType::DUP_KEYS:
        _next_row_func = &Reader::_dup_key_next_row;
        break;
    case KeysType::UNIQUE_KEYS:
        _next_row_func = &Reader::_unique_key_next_row;
        break;
    case KeysType::AGG_KEYS:
        _next_row_func = &Reader::_agg_key_next_row;
        break;
    default:
        break;
    }
    DCHECK(_next_row_func != nullptr) << "No next row function for type:"
        << _tablet->keys_type();

    return OLAP_SUCCESS;
}

OLAPStatus Reader::_dup_key_next_row(RowCursor* row_cursor, MemPool* mem_pool, ObjectPool* agg_pool, bool* eof) {
    if (UNLIKELY(_next_key == nullptr)) {
        *eof = true;
        return OLAP_SUCCESS;
    }
    direct_copy_row(row_cursor, *_next_key);
    auto res = _collect_iter->next(&_next_key, &_next_delete_flag);
    if (res != OLAP_SUCCESS) {
        if (res != OLAP_ERR_DATA_EOF) {
            return res;
        }
    }
    return OLAP_SUCCESS;
}

OLAPStatus Reader::_agg_key_next_row(RowCursor* row_cursor, MemPool* mem_pool, ObjectPool* agg_pool, bool* eof) {
    if (UNLIKELY(_next_key == nullptr)) {
        *eof = true;
        return OLAP_SUCCESS;
    }
    init_row_with_others(row_cursor, *_next_key, mem_pool, agg_pool);
    int64_t merged_count = 0;
    do {
        auto res = _collect_iter->next(&_next_key, &_next_delete_flag);
        if (res != OLAP_SUCCESS) {
            if (res != OLAP_ERR_DATA_EOF) {
                LOG(WARNING) << "next failed:" << res;
                return res;
            }
            break;
        }

        if (_aggregation && merged_count > config::doris_scanner_row_num) {
            break;
        }

        // break while can NOT doing aggregation
        if (!equal_row(_key_cids, *row_cursor, *_next_key)) {
            break;
        }
        agg_update_row(_value_cids, row_cursor, *_next_key);
        ++merged_count;
    } while (true);
    _merged_rows += merged_count;
    // For agg query, we don't need finalize agg object and directly pass agg object to agg node
    if (_need_agg_finalize) {
        agg_finalize_row(_value_cids, row_cursor, mem_pool);
    }

    return OLAP_SUCCESS;
}

OLAPStatus Reader::_unique_key_next_row(RowCursor* row_cursor, MemPool* mem_pool, ObjectPool* agg_pool, bool* eof) {
    *eof = false;
    bool cur_delete_flag = false;
    int64_t merged_count = 0;
    do {
        if (UNLIKELY(_next_key == nullptr)) {
            *eof = true;
            return OLAP_SUCCESS;
        }

        cur_delete_flag = _next_delete_flag;
        // the verion is in reverse order, the first row is the highest version,
        // in UNIQUE_KEY highest version is the final result, there is no need to
        // merge the lower versions
        direct_copy_row(row_cursor, *_next_key);
        agg_finalize_row(_value_cids, row_cursor, mem_pool);
        // skip the lower version rows;
        while (nullptr != _next_key) {
            auto res = _collect_iter->next(&_next_key, &_next_delete_flag);
            if (res != OLAP_SUCCESS) {
                if (res != OLAP_ERR_DATA_EOF) {
                    return res;
                }
                break;
            }

            // break while can NOT doing aggregation
            if (!equal_row(_key_cids, *row_cursor, *_next_key)) {
                break;
            }
            ++merged_count;
        }
        if (!cur_delete_flag) {
            break;
        }
        _stats.rows_del_filtered++;
    } while (cur_delete_flag);
    _merged_rows += merged_count;
    return OLAP_SUCCESS;
}

void Reader::close() {
    VLOG(3) << "merged rows:" << _merged_rows;
    _conditions.finalize();
    _delete_handler.finalize();

    for (auto pred : _col_predicates) {
        delete pred;
    }

    delete _collect_iter;
}

OLAPStatus Reader::_capture_rs_readers(const ReaderParams& read_params) {
    const std::vector<RowsetReaderSharedPtr>* rs_readers = &read_params.rs_readers;
    if (rs_readers->empty()) {
        LOG(WARNING) << "fail to acquire data sources. tablet=" << _tablet->full_name();
        return OLAP_ERR_VERSION_NOT_EXIST;
    }

    bool eof = false;
    for (int i = 0; i < _keys_param.start_keys.size(); ++i) {
        RowCursor* start_key = _keys_param.start_keys[i];
        RowCursor* end_key = _keys_param.end_keys[i];
        bool is_lower_key_included = false;
        bool is_upper_key_included = false;
        if (_keys_param.end_range == "lt") {
            is_upper_key_included = false;
        } else if (_keys_param.end_range == "le") {
            is_upper_key_included = true;
        } else {
            LOG(WARNING) << "reader params end_range is error. "
                         << "range=" << _keys_param.to_string();
            return OLAP_ERR_READER_GET_ITERATOR_ERROR;
        }

        if (_keys_param.range == "gt") {
            if (end_key != nullptr && compare_row_key(*start_key, *end_key) >= 0) {
                VLOG(3) << "return EOF when range=" << _keys_param.range
                        << ", start_key=" << start_key->to_string()
                        << ", end_key=" << end_key->to_string();
                eof = true;
                break;
            }
            is_lower_key_included = false;
        } else if (_keys_param.range == "ge") {
            if (end_key != nullptr && compare_row_key(*start_key, *end_key) > 0) {
                VLOG(3) << "return EOF when range=" << _keys_param.range
                        << ", start_key=" << start_key->to_string()
                        << ", end_key=" << end_key->to_string();
                eof = true;
                break;
            }
            is_lower_key_included = true;
        } else if (_keys_param.range == "eq") {
            is_lower_key_included = true;
            is_upper_key_included = true;
        } else {
            LOG(WARNING) << "reader params range is error. "
                         << "range=" << _keys_param.to_string();
            return OLAP_ERR_READER_GET_ITERATOR_ERROR;
        }

        _is_lower_keys_included.push_back(is_lower_key_included);
        _is_upper_keys_included.push_back(is_upper_key_included);
    }

    if (eof) { return OLAP_SUCCESS; }

    bool need_ordered_result = true;
    if (read_params.reader_type == READER_QUERY) {
        if (_tablet->tablet_schema().keys_type() == DUP_KEYS) {
            // duplicated keys are allowed, no need to merge sort keys in rowset
            need_ordered_result = false;
        }
        if (_aggregation) {
            // compute engine will aggregate rows with the same key,
            // it's ok for rowset to return unordered result
            need_ordered_result = false;
        }
    }

    _reader_context.reader_type = read_params.reader_type;
    _reader_context.tablet_schema = &_tablet->tablet_schema();
    _reader_context.need_ordered_result = need_ordered_result;
    _reader_context.return_columns = &_return_columns;
    _reader_context.seek_columns = &_seek_columns;
    _reader_context.load_bf_columns = &_load_bf_columns;
    _reader_context.conditions = &_conditions;
    _reader_context.predicates = &_col_predicates;
    _reader_context.lower_bound_keys = &_keys_param.start_keys;
    _reader_context.is_lower_keys_included = &_is_lower_keys_included;
    _reader_context.upper_bound_keys = &_keys_param.end_keys;
    _reader_context.is_upper_keys_included = &_is_upper_keys_included;
    _reader_context.delete_handler = &_delete_handler;
    _reader_context.stats = &_stats;
    _reader_context.runtime_state = read_params.runtime_state;
    _reader_context.use_page_cache = read_params.use_page_cache;
    for (auto& rs_reader : *rs_readers) {
        RETURN_NOT_OK(rs_reader->init(&_reader_context));
        OLAPStatus res = _collect_iter->add_child(rs_reader);
        if (res != OLAP_SUCCESS && res != OLAP_ERR_DATA_EOF) {
            LOG(WARNING) << "failed to add child to iterator";
            return res;
        }
        _rs_readers.push_back(rs_reader);
    }

    _next_key = _collect_iter->current_row(&_next_delete_flag);
    return OLAP_SUCCESS;
}

OLAPStatus Reader::_init_params(const ReaderParams& read_params) {
    read_params.check_validation();

    _aggregation = read_params.aggregation;
    _need_agg_finalize = read_params.need_agg_finalize;
    _reader_type = read_params.reader_type;
    _tablet = read_params.tablet;

    _init_conditions_param(read_params);
    _init_load_bf_columns(read_params);

    OLAPStatus res = _init_delete_condition(read_params);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to init delete param. [res=%d]", res);
        return res;
    }

    res = _init_return_columns(read_params);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to init return columns. [res=%d]", res);
        return res;
    }

    res = _init_keys_param(read_params);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to init keys param. res=" << res;
        return res;
    }

    _init_seek_columns();

    _collect_iter = new CollectIterator();
    _collect_iter->init(this);

    return res;
}

OLAPStatus Reader::_init_return_columns(const ReaderParams& read_params) {
    if (read_params.reader_type == READER_QUERY) {
        _return_columns = read_params.return_columns;
        if (_delete_handler.conditions_num() != 0 && read_params.aggregation) {
            set<uint32_t> column_set(_return_columns.begin(), _return_columns.end());
            for (auto conds : _delete_handler.get_delete_conditions()) {
                for (auto cond_column : conds.del_cond->columns()) {
                    if (column_set.find(cond_column.first) == column_set.end()) {
                        column_set.insert(cond_column.first);
                        _return_columns.push_back(cond_column.first);
                    }
                }
            }
        }
        for (auto id : read_params.return_columns) {
            if (_tablet->tablet_schema().column(id).is_key()) {
                _key_cids.push_back(id);
            } else {
                _value_cids.push_back(id);
            }
        }
    } else if (read_params.return_columns.empty()) {
        for (size_t i = 0; i < _tablet->tablet_schema().num_columns(); ++i) {
            _return_columns.push_back(i);
            if (_tablet->tablet_schema().column(i).is_key()) {
                _key_cids.push_back(i);
            } else {
                _value_cids.push_back(i);
            }
        }
        VLOG(3) << "return column is empty, using full column as defaut.";
    } else if (read_params.reader_type == READER_CHECKSUM) {
        _return_columns = read_params.return_columns;
        for (auto id : read_params.return_columns) {
            if (_tablet->tablet_schema().column(id).is_key()) {
                _key_cids.push_back(id);
            } else {
                _value_cids.push_back(id);
            }
        }
    } else {
        OLAP_LOG_WARNING("fail to init return columns. [reader_type=%d return_columns_size=%u]",
                         read_params.reader_type, read_params.return_columns.size());
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    std::sort(_key_cids.begin(), _key_cids.end(), std::greater<uint32_t>());

    return OLAP_SUCCESS;
}


void Reader::_init_seek_columns() {
    std::unordered_set<uint32_t> column_set(_return_columns.begin(), _return_columns.end());
    for (auto& it : _conditions.columns()) {
        column_set.insert(it.first);
    }
    uint32_t max_key_column_count = 0;
    for (auto key : _keys_param.start_keys) {
        if (key->field_count() > max_key_column_count) {
            max_key_column_count = key->field_count();
        }
    }
    for (auto key : _keys_param.end_keys) {
        if (key->field_count() > max_key_column_count) {
            max_key_column_count = key->field_count();
        }
    }
    for (uint32_t i = 0; i < _tablet->tablet_schema().num_columns(); i++) {
        if (i < max_key_column_count || column_set.find(i) != column_set.end()) {
            _seek_columns.push_back(i);
        }
    }
}

OLAPStatus Reader::_init_keys_param(const ReaderParams& read_params) {
    if (read_params.start_key.empty()) {
        return OLAP_SUCCESS;
    }

    _keys_param.range = read_params.range;
    _keys_param.end_range = read_params.end_range;

    size_t start_key_size = read_params.start_key.size();
    _keys_param.start_keys.resize(start_key_size, nullptr);
    for (size_t i = 0; i < start_key_size; ++i) {
        if ((_keys_param.start_keys[i] = new(nothrow) RowCursor()) == nullptr) {
            OLAP_LOG_WARNING("fail to new RowCursor!");
            return OLAP_ERR_MALLOC_ERROR;
        }

        OLAPStatus res = _keys_param.start_keys[i]->init_scan_key(_tablet->tablet_schema(),
                                                       read_params.start_key[i].values());
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to init row cursor. [res=%d]", res);
            return res;
        }

        res = _keys_param.start_keys[i]->from_tuple(read_params.start_key[i]);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to init row cursor from Keys. [res=%d key_index=%ld]", res, i);
            return res;
        }
    }

    size_t end_key_size = read_params.end_key.size();
    _keys_param.end_keys.resize(end_key_size, NULL);
    for (size_t i = 0; i < end_key_size; ++i) {
        if ((_keys_param.end_keys[i] = new(nothrow) RowCursor()) == NULL) {
            OLAP_LOG_WARNING("fail to new RowCursor!");
            return OLAP_ERR_MALLOC_ERROR;
        }

        OLAPStatus res = _keys_param.end_keys[i]->init_scan_key(_tablet->tablet_schema(),
                                                     read_params.end_key[i].values());
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to init row cursor. [res=%d]", res);
            return res;
        }

        res = _keys_param.end_keys[i]->from_tuple(read_params.end_key[i]);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to init row cursor from Keys. [res=%d key_index=%ld]", res, i);
            return res;
        }
    }

    //TODO:check the valid of start_key and end_key.(eg. start_key <= end_key)

    return OLAP_SUCCESS;
}

void Reader::_init_conditions_param(const ReaderParams& read_params) {
    _conditions.set_tablet_schema(&_tablet->tablet_schema());
    for (const auto& condition : read_params.conditions) {
        _conditions.append_condition(condition);
        ColumnPredicate* predicate = _parse_to_predicate(condition);
        if (predicate != nullptr) {
            _col_predicates.push_back(predicate);
        }
    }
}

#define COMPARISON_PREDICATE_CONDITION_VALUE(NAME, PREDICATE) \
ColumnPredicate* Reader::_new_##NAME##_pred(const TabletColumn& column, int index, const std::string& cond) { \
    ColumnPredicate* predicate = nullptr; \
    switch (column.type()) { \
        case OLAP_FIELD_TYPE_TINYINT: { \
            std::stringstream ss(cond); \
            int32_t value = 0; \
            ss >> value; \
            predicate = new PREDICATE<int8_t>(index, value); \
            break; \
        } \
        case OLAP_FIELD_TYPE_SMALLINT: { \
            std::stringstream ss(cond); \
            int16_t value = 0; \
            ss >> value; \
            predicate = new PREDICATE<int16_t>(index, value); \
            break; \
        } \
        case OLAP_FIELD_TYPE_INT: { \
            std::stringstream ss(cond); \
            int32_t value = 0; \
            ss >> value; \
            predicate = new PREDICATE<int32_t>(index, value); \
            break; \
        } \
        case OLAP_FIELD_TYPE_BIGINT: { \
            std::stringstream ss(cond); \
            int64_t value = 0; \
            ss >> value; \
            predicate = new PREDICATE<int64_t>(index, value); \
            break; \
        } \
        case OLAP_FIELD_TYPE_LARGEINT: { \
            std::stringstream ss(cond); \
            int128_t value = 0; \
            ss >> value; \
            predicate = new PREDICATE<int128_t>(index, value); \
            break; \
        } \
        case OLAP_FIELD_TYPE_DECIMAL: { \
            decimal12_t value(0, 0); \
            value.from_string(cond); \
            predicate = new PREDICATE<decimal12_t>(index, value); \
            break; \
        } \
        case OLAP_FIELD_TYPE_CHAR: {\
            StringValue value; \
            size_t length = std::max(static_cast<size_t>(column.length()), cond.length());\
            char* buffer = reinterpret_cast<char*>(_predicate_mem_pool->allocate(length)); \
            memset(buffer, 0, length); \
            memory_copy(buffer, cond.c_str(), cond.length()); \
            value.len = length; \
            value.ptr = buffer; \
            predicate = new PREDICATE<StringValue>(index, value); \
            break; \
        } \
        case OLAP_FIELD_TYPE_VARCHAR: { \
            StringValue value; \
            int32_t length = cond.length(); \
            char* buffer = reinterpret_cast<char*>(_predicate_mem_pool->allocate(length)); \
            memory_copy(buffer, cond.c_str(), length); \
            value.len = length; \
            value.ptr = buffer; \
            predicate = new PREDICATE<StringValue>(index, value); \
            break; \
        } \
        case OLAP_FIELD_TYPE_DATE: { \
            uint24_t value = timestamp_from_date(cond); \
            predicate = new PREDICATE<uint24_t>(index, value); \
            break; \
        } \
        case OLAP_FIELD_TYPE_DATETIME: { \
            uint64_t value = timestamp_from_datetime(cond); \
            predicate = new PREDICATE<uint64_t>(index, value); \
            break; \
        } \
        case OLAP_FIELD_TYPE_BOOL: { \
            std::stringstream ss(cond); \
            bool value = false; \
            ss >> value; \
            predicate = new PREDICATE<bool>(index, value); \
            break; \
        } \
        default: break; \
    } \
 \
    return predicate; \
} \

COMPARISON_PREDICATE_CONDITION_VALUE(eq, EqualPredicate)
COMPARISON_PREDICATE_CONDITION_VALUE(ne, NotEqualPredicate)
COMPARISON_PREDICATE_CONDITION_VALUE(lt, LessPredicate)
COMPARISON_PREDICATE_CONDITION_VALUE(le, LessEqualPredicate)
COMPARISON_PREDICATE_CONDITION_VALUE(gt, GreaterPredicate)
COMPARISON_PREDICATE_CONDITION_VALUE(ge, GreaterEqualPredicate)

ColumnPredicate* Reader::_parse_to_predicate(const TCondition& condition) {
    // TODO: not equal and not in predicate is not pushed down
    int index = _tablet->field_index(condition.column_name);
    const TabletColumn& column = _tablet->tablet_schema().column(index);
    if (column.aggregation() != FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE) {
        return nullptr;
    }
    ColumnPredicate* predicate = nullptr;
    if (condition.condition_op == "*=" && condition.condition_values.size() == 1) {
        predicate = _new_eq_pred(column, index, condition.condition_values[0]);
    } else if (condition.condition_op == "<<") {
        predicate = _new_lt_pred(column, index, condition.condition_values[0]);
    } else if (condition.condition_op == "<=") {
        predicate = _new_le_pred(column, index, condition.condition_values[0]);
    } else if (condition.condition_op == ">>") {
        predicate = _new_gt_pred(column, index, condition.condition_values[0]);
    } else if (condition.condition_op == ">=") {
        predicate = _new_ge_pred(column, index, condition.condition_values[0]);
    } else if (condition.condition_op == "*=" && condition.condition_values.size() > 1) {
        switch (column.type()) {
            case OLAP_FIELD_TYPE_TINYINT: {
                std::set<int8_t> values;
                for (auto& cond_val : condition.condition_values) {
                    int32_t value = 0;
                    std::stringstream ss(cond_val);
                    ss >> value;
                    values.insert(value);
                }
                predicate = new InListPredicate<int8_t>(index, std::move(values));
                break;
            }
            case OLAP_FIELD_TYPE_SMALLINT: {
                std::set<int16_t> values;
                for (auto& cond_val : condition.condition_values) {
                    int16_t value = 0;
                    std::stringstream ss(cond_val);
                    ss >> value;
                    values.insert(value);
                }
                predicate = new InListPredicate<int16_t>(index, std::move(values));
                break;
            }
            case OLAP_FIELD_TYPE_INT: {
                std::set<int32_t> values;
                for (auto& cond_val : condition.condition_values) {
                    int32_t value = 0;
                    std::stringstream ss(cond_val);
                    ss >> value;
                    values.insert(value);
                }
                predicate = new InListPredicate<int32_t>(index, std::move(values));
                break;
            }
            case OLAP_FIELD_TYPE_BIGINT: {
                std::set<int64_t> values;
                for (auto& cond_val : condition.condition_values) {
                    int64_t value = 0;
                    std::stringstream ss(cond_val);
                    ss >> value;
                    values.insert(value);
                }
                predicate = new InListPredicate<int64_t>(index, std::move(values));
                break;
            }
            case OLAP_FIELD_TYPE_LARGEINT: {
                std::set<int128_t> values;
                for (auto& cond_val : condition.condition_values) {
                    int128_t value = 0;
                    std::stringstream ss(cond_val);
                    ss >> value;
                    values.insert(value);
                }
                predicate = new InListPredicate<int128_t>(index, std::move(values));
                break;
            }
            case OLAP_FIELD_TYPE_DECIMAL: {
                std::set<decimal12_t> values;
                for (auto& cond_val : condition.condition_values) {
                    decimal12_t value;
                    value.from_string(cond_val);
                    values.insert(value);
                }
                predicate = new InListPredicate<decimal12_t>(index, std::move(values));
                break;
            }
            case OLAP_FIELD_TYPE_CHAR: {
                std::set<StringValue> values;
                for (auto& cond_val : condition.condition_values) {
                    StringValue value;
                    size_t length = std::max(static_cast<size_t>(column.length()), cond_val.length());
                    char* buffer = reinterpret_cast<char*>(_predicate_mem_pool->allocate(length));
                    memset(buffer, 0, length);
                    memory_copy(buffer, cond_val.c_str(), cond_val.length());
                    value.len = length;
                    value.ptr = buffer;
                    values.insert(value);
                }
                predicate = new InListPredicate<StringValue>(index, std::move(values));
                break;
            }
            case OLAP_FIELD_TYPE_VARCHAR: {
                std::set<StringValue> values;
                for (auto& cond_val : condition.condition_values) {
                    StringValue value;
                    int32_t length = cond_val.length();
                    char* buffer = reinterpret_cast<char*>(_predicate_mem_pool->allocate(length));
                    memory_copy(buffer, cond_val.c_str(), length);
                    value.len = length;
                    value.ptr = buffer;
                    values.insert(value);
                }
                predicate = new InListPredicate<StringValue>(index, std::move(values));
                break;
            }
            case OLAP_FIELD_TYPE_DATE: {
                std::set<uint24_t> values;
                for (auto& cond_val : condition.condition_values) {
                    uint24_t value = timestamp_from_date(cond_val);
                    values.insert(value);
                }
                predicate = new InListPredicate<uint24_t>(index, std::move(values));
                break;
            }
            case OLAP_FIELD_TYPE_DATETIME: {
                std::set<uint64_t> values;
                for (auto& cond_val : condition.condition_values) {
                    uint64_t value = timestamp_from_datetime(cond_val);
                    values.insert(value);
                }
                predicate = new InListPredicate<uint64_t>(index, std::move(values));
                break;
            }
            // OLAP_FIELD_TYPE_BOOL is not valid in this case.
            default: break;
        }
    } else if (condition.condition_op == "is") {
        predicate = new NullPredicate(index, condition.condition_values[0] == "null");
    }
    return predicate;
}

void Reader::_init_load_bf_columns(const ReaderParams& read_params) {
    // add all columns with condition to _load_bf_columns
    for (const auto& cond_column : _conditions.columns()) {
        for (const Cond* cond : cond_column.second->conds()) {
            if (cond->op == OP_EQ
                    || (cond->op == OP_IN && cond->operand_set.size() < MAX_OP_IN_FIELD_NUM)) {
                _load_bf_columns.insert(cond_column.first);
            }
        }
    }

    // remove columns which have no bf stream
    for (int i = 0; i < _tablet->tablet_schema().num_columns(); ++i) {
        if (!_tablet->tablet_schema().column(i).is_bf_column()) {
            _load_bf_columns.erase(i);
        }
    }

    // remove columns which have same value between start_key and end_key
    int min_scan_key_len = _tablet->tablet_schema().num_columns();
    for (int i = 0; i < read_params.start_key.size(); ++i) {
        if (read_params.start_key[i].size() < min_scan_key_len) {
            min_scan_key_len = read_params.start_key[i].size();
        }
    }

    for (int i = 0; i < read_params.end_key.size(); ++i) {
        if (read_params.end_key[i].size() < min_scan_key_len) {
            min_scan_key_len = read_params.end_key[i].size();
        }
    }

    int max_equal_index = -1;
    for (int i = 0; i < read_params.start_key.size(); ++i) {
        int j = 0;
        for (; j < min_scan_key_len; ++j) {
            if (read_params.start_key[i].get_value(j) != read_params.end_key[i].get_value(j)) {
                break;
            }
        }

        if (max_equal_index < j - 1) {
            max_equal_index = j - 1;
        }
    }

    for (int i = 0; i < max_equal_index; ++i) {
        _load_bf_columns.erase(i);
    }

    // remove the max_equal_index column when it's not varchar
    // or longer than number of short key fields
    if (max_equal_index == -1) {
        return;
    }
    FieldType type = _tablet->tablet_schema().column(max_equal_index).type();
    if (type != OLAP_FIELD_TYPE_VARCHAR || max_equal_index + 1 > _tablet->num_short_key_columns()) {
        _load_bf_columns.erase(max_equal_index);
    }
}

OLAPStatus Reader::_init_delete_condition(const ReaderParams& read_params) {
    if (read_params.reader_type != READER_CUMULATIVE_COMPACTION) {
        _tablet->obtain_header_rdlock();
        OLAPStatus ret = _delete_handler.init(_tablet->tablet_schema(),
                                              _tablet->delete_predicates(),
                                              read_params.version.second);
        _tablet->release_header_lock();
        return ret;
    } else {
        return OLAP_SUCCESS;
    }
}

}  // namespace doris
