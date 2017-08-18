// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
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

#include "olap/olap_data.h"
#include "olap/olap_table.h"
#include "olap/row_block.h"
#include "olap/row_cursor.h"

using std::nothrow;
using std::set;
using std::vector;

namespace palo {

Reader::MergeSet::~MergeSet() {
    SAFE_DELETE(_heap);
}

OLAPStatus Reader::MergeSet::init(Reader* reader, bool reverse) {
    _reader = reader;

    _heap = new (nothrow) heap_t(RowCursorComparator(reverse));
    if (_heap == NULL) {
        OLAP_LOG_FATAL("failed to malloc. [size=%ld]", sizeof(heap_t));
        return OLAP_ERR_MALLOC_ERROR;
    }

    return OLAP_SUCCESS;
}

bool Reader::MergeSet::attach(const MergeElement& merge_element, const RowCursor* row) {
    // Use data file's end_version as data's version
    int32_t data_version = merge_element->version().second;
    do {
        if (row == NULL) {
            if (!merge_element->eof()) {
                // Return error if merge_element isn't reach end, but row equal NULL.
                OLAP_LOG_WARNING("internal error with IData.");
                return false;
            } else {
                _reader->_filted_rows += merge_element->get_filted_rows();
            }
        } else {
            _reader->_scan_rows++;
            if (merge_element->data_file_type() == OLAP_DATA_FILE) {
                if (_reader->_delete_handler.is_filter_data(data_version, *row)) {
                    _reader->_filted_rows++;
                    row = merge_element->get_next_row();
                    continue;
                }
            }
            _heap->push(merge_element);
        }

        break;
    } while (true);

    return true;
}

const RowCursor* Reader::MergeSet::curr(bool* delete_flag) {
    if (_heap->size() > 0) {
        *delete_flag = _heap->top()->delete_flag();
        return _heap->top()->get_current_row();
    } else {
        return NULL;
    }
}

bool Reader::MergeSet::next(const RowCursor** element, bool* delete_flag) {
    if (!_pop_from_heap()) {
        return false;
    }

    (element == NULL || (*element = curr(delete_flag)));
    return true;
}

bool Reader::MergeSet::_pop_from_heap() {
    MergeElement merge_element = _heap->top();
    const RowCursor* row = merge_element->get_next_row();

    // when Reader is used for fetch,
    // Reader will read deltas one by one without merge sort in DUP_KEYS keys type,
    // so we don't need to use pop and attach to adjust the _heap.
    if (_reader->_reader_type == READER_FETCH
            && _reader->_olap_table->keys_type() == KeysType::DUP_KEYS && row != NULL) {
        _reader->_scan_rows++;
        if (merge_element->data_file_type() == OLAP_DATA_FILE) {
            int32_t data_version = merge_element->version().second;
            if (_reader->_delete_handler.is_filter_data(data_version, *row)) {
                _reader->_filted_rows++;
                return _pop_from_heap();
            }
        }
        return true;
    }

    _heap->pop();
    return attach(merge_element, row);
}

bool Reader::MergeSet::clear() {
    if (_heap != NULL) {
        while (_heap->size() > 0) {
            _heap->pop();
        }
    }
    return true;
}

bool Reader::MergeSet::RowCursorComparator::operator()(
        const MergeElement &a,
        const MergeElement &b) {
    // First compare row cursor.
    const RowCursor* first = a->get_current_row();
    const RowCursor* second = b->get_current_row();
    int cmp_res = first->full_key_cmp(*second);
    if (cmp_res != 0) {
        if (_reverse) {
            return cmp_res < 0;
        } else {
            return cmp_res > 0;
        }
    }

    // if row cursors equal, compare data version.
    return a->version().second > b->version().second;
}

OLAPStatus Reader::init(const ReaderParams& read_params) {
    OLAPStatus res = OLAP_SUCCESS;

    res = _init_params(read_params);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to init reader when init params.[res=%d]", res);
        return res;
    }

    res = _acquire_data_sources(read_params);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to init reader when acquire data sources.[res=%d]", res);      
        return res;
    }

    bool eof = false;
    if (OLAP_SUCCESS != (res = _attach_data_to_merge_set(true, &eof))) {
        OLAP_LOG_WARNING("failed to attaching data to merge set. [res=%d]", res);
        return res;
    }

    for (auto i_data: _data_sources) {
        i_data->set_profile(read_params.profile);
    }

    _is_inited = true;
    return OLAP_SUCCESS;
}

OLAPStatus Reader::next_row_with_aggregation(
        RowCursor* row_cursor,
        int64_t* raw_rows_read,
        bool* eof) {
    OLAPStatus res = OLAP_SUCCESS;
    bool cur_delete_flag = false;
    *eof = false;

    do {
        if (NULL == _next_key) {
            ++_current_key_index;
            res = _attach_data_to_merge_set(false, eof);
            if (OLAP_SUCCESS != res) {
                OLAP_LOG_WARNING("failed to attach data to merge set.");
                return res;
            }
            if (*eof) {
                return OLAP_SUCCESS;
            }
        }
    
        cur_delete_flag = _next_delete_flag;
        res = row_cursor->copy(*_next_key);
    
        if (OLAP_SUCCESS != res) {
            OLAP_LOG_WARNING("failed to copy row_cursor. [row_cursor='%s']",
                    _next_key->to_string().c_str());
            return res;
        }
        ++(*raw_rows_read);
    
        int64_t merged_count = 0;
        while (NULL != _next_key) {
            if (!_merge_set.next(&_next_key, &_next_delete_flag)) {
                OLAP_LOG_WARNING("internal error with IData.");
                res = OLAP_ERR_READER_READING_ERROR;
                break;
            }
            
            if (NULL == _next_key) {
                row_cursor->finalize_one_merge();
                break;
            }
    
            // we will not do aggregation in two case:
            //   1. DUP_KEYS keys type has no semantic to aggregate,
            //   2. to make cost of  each scan round reasonable, we will control merged_count.
            if (_olap_table->keys_type() == KeysType::DUP_KEYS
                    || (_aggregation && merged_count > config::palo_scanner_row_num)) {
               row_cursor->finalize_one_merge(); 
               break;
            }
    
            // break while can NOT doing aggregation
            if (!row_cursor->equal(*_next_key)) {
               row_cursor->finalize_one_merge(); 
               break;
            }
    
            cur_delete_flag = _next_delete_flag;
            res = row_cursor->aggregate(*_next_key);
            if (OLAP_SUCCESS != res) {
                OLAP_LOG_WARNING("failed to aggregate row cursor.[base_row='%s', new_rows='%s']",
                        row_cursor->to_string().c_str(), _next_key->to_string().c_str());
                break;
            }
    
            ++merged_count;
        }
    
        if (res == OLAP_SUCCESS) {
            _merged_rows += merged_count;
            *raw_rows_read += merged_count;
        }
    
        if (res != OLAP_SUCCESS || !cur_delete_flag) {
            return res;
        }
    
        ++_filted_rows;
    } while (cur_delete_flag);

    return res;
}

void Reader::close() {
    OLAP_LOG_DEBUG("scan rows:%lu, filted rows:%lu, merged rows:%lu",
                   _scan_rows, _filted_rows, _merged_rows);
    _conditions.finalize();
    _delete_handler.finalize();
    if (!_is_set_data_sources) {
        _olap_table->release_data_sources(&_data_sources);
    }
}

OLAPStatus Reader::_acquire_data_sources(const ReaderParams& read_params) {
    _data_sources.clear();
    if (read_params.reader_type == READER_ALTER_TABLE
            || read_params.reader_type == READER_BASE_EXPANSION
            || read_params.reader_type == READER_CUMULATIVE_EXPANSION) {
        _data_sources = read_params.olap_data_arr;
        _is_set_data_sources = true;
    } else {
        _olap_table->obtain_header_rdlock();
        _olap_table->acquire_data_sources(_version, &_data_sources);
        _olap_table->release_header_lock();

        if (_data_sources.size() < 1) {
            OLAP_LOG_WARNING("fail to acquire data sources. [table_name='%s' version=%d-%d]",
                             _olap_table->full_name().c_str(),
                             _version.first,
                             _version.second);
            return OLAP_ERR_VERSION_NOT_EXIST;
        }
    }
    
    // do not use index stream cache when be/ce/alter/checksum,
    // to avoid bringing down lru cache hit ratio
    bool is_using_cache = true;
    if (read_params.reader_type != READER_FETCH) {
        is_using_cache = false;
    }

    for (auto i_data: _data_sources) {
        i_data->set_conjuncts(_query_conjunct_ctxs, NULL);
        i_data->set_delete_handler(_delete_handler);
        i_data->set_read_params(_return_columns,
                                _load_bf_columns,
                                _conditions,
                                _keys_param.start_keys,
                                _keys_param.end_keys,
                                is_using_cache,
                                read_params.runtime_state);
    }

    return OLAP_SUCCESS;
}

OLAPStatus Reader::_init_params(const ReaderParams& read_params) {
    OLAPStatus res = OLAP_SUCCESS;
    _aggregation = read_params.aggregation;
    _reader_type = read_params.reader_type;
    _olap_table = read_params.olap_table;
    _version = read_params.version;
    
    res = _init_conditions_param(read_params);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to init conditions param. [res=%d]", res);
        return res;
    }

    res = _init_load_bf_columns(read_params);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to init load bloom filter columns. [res=%d]", res);
        return res;
    }

    res = _init_delete_condition(read_params);
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
        OLAP_LOG_WARNING("fail to init keys param. [res=%d]", res);
        return res;
    }

    res = _merge_set.init(this, false);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to init merge set. [res=%d]", res);
        return res;
    }

    return res;
}

OLAPStatus Reader::_init_return_columns(const ReaderParams& read_params) {
    if (read_params.reader_type == READER_FETCH) {
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
    } else if (read_params.return_columns.size() == 0) {
        for (size_t i = 0; i < _olap_table->tablet_schema().size(); ++i) {
            _return_columns.push_back(i);
        }
        OLAP_LOG_DEBUG("return column is empty, using full column as defaut.");
    } else if (read_params.reader_type == READER_CHECKSUM) {
        // do nothing
    } else {
        OLAP_LOG_WARNING("fail to init return columns. [reader_type=%d return_columns_size=%u]",
                         read_params.reader_type, read_params.return_columns.size());
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    return OLAP_SUCCESS;
}

OLAPStatus Reader::_attach_data_to_merge_set(bool first, bool *eof) {
    OLAPStatus res = OLAP_SUCCESS;
    *eof = false;

    do {
        RowCursor *start_key = NULL;
        RowCursor *end_key = NULL;
        bool find_last_row = false;
        bool end_key_find_last_row = false;

        _merge_set.clear();

        if (_keys_param.start_keys.size() > 0) {
            if (_current_key_index >= _keys_param.start_keys.size()) {
                *eof = true;
                OLAP_LOG_DEBUG("can NOT attach while start_key has been used.");
                return res;
            }
            start_key = _keys_param.start_keys[_current_key_index];

            if (0 != _keys_param.end_keys.size()) {
                end_key = _keys_param.end_keys[_current_key_index];
                if (0 == _keys_param.end_range.compare("lt")) {
                    end_key_find_last_row = false;
                } else if (0 == _keys_param.end_range.compare("le")) {
                    end_key_find_last_row = true;
                } else {
                    OLAP_LOG_WARNING("reader params end_range is error. [range='%s']", 
                                     _keys_param.to_string().c_str());
                    res = OLAP_ERR_READER_GET_ITERATOR_ERROR;
                    return res;
                }
            }
            
            if (0 == _keys_param.range.compare("gt")) {
                if (NULL != end_key
                        && start_key->cmp(*end_key) >= 0) {
                    OLAP_LOG_TRACE("return EOF when range(%s) start_key(%s) end_key(%s).",
                                   _keys_param.range.c_str(),
                                   start_key->to_string().c_str(),
                                   end_key->to_string().c_str());
                    *eof = true;
                    return res;
                }
                
                find_last_row = true;
            } else if (0 == _keys_param.range.compare("ge")) {
                if (NULL != end_key
                        && start_key->cmp(*end_key) > 0) {
                    OLAP_LOG_TRACE("return EOF when range(%s) start_key(%s) end_key(%s).",
                                   _keys_param.range.c_str(),
                                   start_key->to_string().c_str(),
                                   end_key->to_string().c_str());
                    *eof = true;
                    return res;
                }
                
                find_last_row = false;
            } else if (0 == _keys_param.range.compare("eq")) {
                find_last_row = false;
                end_key = start_key;
                end_key_find_last_row = true;
            } else {
                OLAP_LOG_WARNING(
                        "reader params range is error. [range='%s']", 
                        _keys_param.to_string().c_str());
                res = OLAP_ERR_READER_GET_ITERATOR_ERROR;
                return res;
            }
        } else if (false == first) {
            *eof = true;
            return res;
        }

        for (std::vector<IData *>::iterator it = _data_sources.begin();
                it != _data_sources.end(); ++it) {
            const RowCursor *start_row_cursor = NULL;

            if (OLAP_LIKELY(start_key != NULL)) {
                if ((*it)->delta_pruning_filter()) {
                    OLAP_LOG_DEBUG("filter delta in query in condition: %d, %d",
                                   (*it)->version().first, (*it)->version().second);
                    _filted_rows += (*it)->num_rows();
                    continue;
                }

                int ret = (*it)->delete_pruning_filter();
                if (DEL_SATISFIED == ret) {
                    OLAP_LOG_DEBUG("filter delta in query: %d, %d",
                                   (*it)->version().first, (*it)->version().second);
                    _filted_rows += (*it)->num_rows();
                    continue;
                } else if (DEL_PARTIAL_SATISFIED == ret) {
                    OLAP_LOG_DEBUG("filter delta partially in query: %d, %d",
                                   (*it)->version().first, (*it)->version().second);
                    (*it)->set_delete_status(DEL_PARTIAL_SATISFIED);
                } else {
                    OLAP_LOG_DEBUG("not filter delta in query: %d, %d",
                                   (*it)->version().first, (*it)->version().second);
                    (*it)->set_delete_status(DEL_NOT_SATISFIED);
                }

                (*it)->set_end_key(end_key, end_key_find_last_row);
                start_row_cursor = (*it)->find_row(*start_key, find_last_row, false);
            } else {
                if ((*it)->empty()) {
                    continue;
                }

                //BE procedure will go into this branch, which key params is empty
                int ret = (*it)->delete_pruning_filter();
                if (DEL_SATISFIED == ret) {
                    OLAP_LOG_DEBUG("filter delta in query: %d, %d",
                                   (*it)->version().first, (*it)->version().second);
                    _filted_rows += (*it)->num_rows();
                    continue;
                } else if (DEL_PARTIAL_SATISFIED == ret) {
                    OLAP_LOG_DEBUG("filter delta partially in query: %d, %d",
                                   (*it)->version().first, (*it)->version().second);
                    (*it)->set_delete_status(DEL_PARTIAL_SATISFIED);
                } else {
                    OLAP_LOG_DEBUG("not filter delta in query: %d, %d",
                                   (*it)->version().first, (*it)->version().second);
                    (*it)->set_delete_status(DEL_NOT_SATISFIED);
                }

                start_row_cursor = (*it)->get_first_row();
            }

            if ((*it)->eof()) {
                OLAP_LOG_DEBUG("got EOF while setting start_row_cursor. "
                               "[version=%d-%d read_params='%s']",
                               (*it)->version().first, (*it)->version().second,
                               _keys_param.to_string().c_str());
                continue;
            }

            if (!start_row_cursor) {
                OLAP_LOG_WARNING("failed to set start_row_cursor. [read_params='%s']",
                        _keys_param.to_string().c_str());
                return OLAP_ERR_READER_GET_ITERATOR_ERROR;
            }

            _merge_set.attach(*it, start_row_cursor);
        }

        _next_key = _merge_set.curr(&_next_delete_flag);
        if (_next_key != NULL) {
            break;
        }

        ++_current_key_index;
        first = false;
    } while (NULL == _next_key);

    return res;
}

OLAPStatus Reader::_init_keys_param(const ReaderParams& read_params) {
    OLAPStatus res = OLAP_SUCCESS;

    _current_key_index = 0;

    if (read_params.start_key.size() == 0) {
        return OLAP_SUCCESS;
    }

    _keys_param.range = read_params.range;
    _keys_param.end_range = read_params.end_range;

    size_t start_key_size = read_params.start_key.size();
    _keys_param.start_keys.resize(start_key_size, NULL);
    for (size_t i = 0; i < start_key_size; ++i) {
        if ((_keys_param.start_keys[i] = new(nothrow) RowCursor()) == NULL) {
            OLAP_LOG_WARNING("fail to new RowCursor!");
            return OLAP_ERR_MALLOC_ERROR;
        }
        
        res = _keys_param.start_keys[i]->init_keys(_olap_table->tablet_schema(),
                                              read_params.start_key[i].key);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to init row cursor. [res=%d]", res);
            return res;
        }
        
        res = _keys_param.start_keys[i]->from_string(read_params.start_key[i].key);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to init row cursor from Keys. [res=%d key_index=%ld]", res, i);
            return res;
        }
        for (size_t j = 0; j < _keys_param.start_keys[i]->field_count(); ++j) {
            if (_olap_table->tablet_schema()[j].is_allow_null
                && _keys_param.start_keys[i]->is_min(j)) {
                _keys_param.start_keys[i]->set_null(j);
            }
        }
    }

    size_t end_key_size = read_params.end_key.size();
    _keys_param.end_keys.resize(end_key_size, NULL);
    for (size_t i = 0; i < end_key_size; ++i) {
        if ((_keys_param.end_keys[i] = new(nothrow) RowCursor()) == NULL) {
            OLAP_LOG_WARNING("fail to new RowCursor!");
            return OLAP_ERR_MALLOC_ERROR;
        }
        
        res = _keys_param.end_keys[i]->init_keys(_olap_table->tablet_schema(),
                                            read_params.end_key[i].key);
        /*
        for (size_t j = 0; j < read_params.end_key[i].key.size(); ++j) {
            _keys_param.end_keys[i]->set_null(j);
        }
        */
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to init row cursor. [res=%d]", res);
            return res;
        }
        
        res = _keys_param.end_keys[i]->from_string(read_params.end_key[i].key);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to init row cursor from Keys. [res=%d key_index=%ld]", res, i);
            return res;
        }
    }

    //TODO:check the valid of start_key and end_key.(eg. start_key <= end_key)

    return OLAP_SUCCESS;
}

OLAPStatus Reader::_init_conditions_param(const ReaderParams& read_params) {
    OLAPStatus res = OLAP_SUCCESS;

    _conditions.set_table(_olap_table);
    for (int i = 0; i < read_params.conditions.size(); ++i) {
        _conditions.append_condition(read_params.conditions[i]);
    }
    _query_conjunct_ctxs = read_params.conjunct_ctxs;

    return res;
}

OLAPStatus Reader::_init_load_bf_columns(const ReaderParams& read_params) {
    OLAPStatus res = OLAP_SUCCESS;

    // add all columns with condition to _load_bf_columns
    for (const auto& cond_column : _conditions.columns()) {
        for (const Cond& cond : cond_column.second.conds()) {
            if (cond.op == OP_EQ
                    || (cond.op == OP_IN && cond.operand_set.size() < MAX_OP_IN_FIELD_NUM)) {
                _load_bf_columns.insert(cond_column.first);
            }
        }
    }

    // remove columns which have no bf stream
    for (int i = 0; i < _olap_table->tablet_schema().size(); ++i) {
        if (!_olap_table->tablet_schema()[i].is_bf_column) {
            _load_bf_columns.erase(i);
        }
    }

    // remove columns which have same value between start_key and end_key
    int min_scan_key_len = _olap_table->tablet_schema().size();
    for (int i = 0; i < read_params.start_key.size(); ++i) {
        if (read_params.start_key[i].key.size() < min_scan_key_len) {
            min_scan_key_len = read_params.start_key[i].key.size();
        }
    }

    for (int i = 0; i < read_params.end_key.size(); ++i) {
        if (read_params.end_key[i].key.size() < min_scan_key_len) {
            min_scan_key_len = read_params.end_key[i].key.size();
        }
    }

    int max_equal_index = -1;
    for (int i = 0; i < read_params.start_key.size(); ++i) {
        int j = 0;
        for (; j < min_scan_key_len; ++j) {
            if (read_params.start_key[i].key[j] != read_params.end_key[i].key[j]) {
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
    FieldType type = _olap_table->get_field_type_by_index(max_equal_index);
    if ((type != OLAP_FIELD_TYPE_VARCHAR && type != OLAP_FIELD_TYPE_HLL)
            || max_equal_index + 1 > _olap_table->num_short_key_fields()) {
        _load_bf_columns.erase(max_equal_index);
    }

    return res;
}

OLAPStatus Reader::_init_delete_condition(const ReaderParams& read_params) {
    if (read_params.reader_type != READER_CUMULATIVE_EXPANSION) {
        _olap_table->obtain_header_rdlock();
        OLAPStatus ret = _delete_handler.init(_olap_table, read_params.version.second);
        _olap_table->release_header_lock();

        return ret;
    } else {
        return OLAP_SUCCESS;
    }
}

}  // namespace palo
