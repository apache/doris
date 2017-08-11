// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

#include "olap/row_cursor.h"

#include <algorithm>

using std::min;
using std::nothrow;
using std::string;
using std::vector;

namespace palo {
RowCursor::RowCursor() :
        _field_array(NULL),
        _field_array_size(0),
        _columns(NULL),
        _columns_size(0),
        _key_column_num(0),
        _length(0),
        _length_mysql(0),
        _field_length_array(NULL),
        _field_offset(NULL),
        _is_inited(false),
        _buf(NULL),
        _is_mysql_compatible(true) {}

RowCursor::~RowCursor() {
    // delete RowCursor's Fields
    for (size_t i = 0; i < _field_array_size; ++i) {
        SAFE_DELETE(_field_array[i]);
    }

    _is_inited = false;
    _key_column_num = 0;
    
    SAFE_DELETE_ARRAY(_field_array);
    SAFE_DELETE_ARRAY(_field_length_array);
    SAFE_DELETE_ARRAY(_field_offset);
    SAFE_DELETE_ARRAY(_columns);
    SAFE_DELETE_ARRAY(_buf);
}

OLAPStatus RowCursor::init(const vector<FieldInfo>& tablet_schema) {
    return init(tablet_schema, tablet_schema.size());
}

OLAPStatus RowCursor::init(const vector<FieldInfo>& tablet_schema, size_t column_count) {
    if (_is_inited) {
        OLAP_LOG_WARNING("Fail to init RowCursor; RowCursor has been inited.");
        
        return OLAP_ERR_INIT_FAILED;
    } else if (column_count > tablet_schema.size()) {
        OLAP_LOG_WARNING("input param are invalid. Column count is bigger than table schema size."
                         "[column_count=%lu tablet_schema.size=%lu]",
                         column_count,
                         tablet_schema.size());
        
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    // create an array of fields with size = column_count
    vector<uint32_t> columns(column_count);
    for (uint32_t i = 0; i < column_count; ++i) {
        columns[i] = i;
    }
    
    return _init(tablet_schema, columns, nullptr);
}

OLAPStatus RowCursor::init_keys(const std::vector<FieldInfo>& tablet_schema, 
                         const std::vector<std::string>& keys) {
    std::vector<size_t> lengths;
    for (int i = 0; i < keys.size(); i++) {
        lengths.push_back(keys[i].length());
    }
    return init_keys(tablet_schema, lengths);
}

OLAPStatus RowCursor::init_keys(const std::vector<FieldInfo>& tablet_schema, 
                    const std::vector<size_t>& lengths) {
    if (_is_inited) {
        OLAP_LOG_WARNING("Fail to init RowCursor; RowCursor has been inited.");

        return OLAP_ERR_INIT_FAILED;
    } else if (lengths.size() > tablet_schema.size()) {
        OLAP_LOG_WARNING("input param are invalid. Column count is bigger than table schema size."
                         "[column_count=%lu tablet_schema.size=%lu]",
                         lengths.size(),
                         tablet_schema.size());

        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    vector<uint32_t> columns(lengths.size());
    for (uint32_t i = 0; i < lengths.size(); ++i) {
        columns[i] = i;
    }

    return _init(tablet_schema, columns, &lengths);
}

OLAPStatus RowCursor::init(
        const vector<FieldInfo>& tablet_schema,
        const vector<uint32_t>& columns) {
    return _init(tablet_schema, columns, nullptr);
}
    
OLAPStatus RowCursor::_init(
        const vector<FieldInfo>& tablet_schema,
        const vector<uint32_t>& columns, const std::vector<size_t>* lengths) {
    // OLAP里面强制需要schema里面key在前，value在后
    _field_array_size = tablet_schema.size();
    _field_array = new (nothrow) Field*[tablet_schema.size()];
    _columns = new (nothrow) uint32_t[columns.size()];
    _field_length_array = new (nothrow) size_t[_field_array_size];
    _field_offset = new (nothrow) size_t[_field_array_size];
    if (_field_array == NULL
            || _columns == NULL
            || _field_offset == NULL
            || _field_length_array == NULL) {
        OLAP_LOG_WARNING("Fail to malloc internal structures."
                         "[tablet_schema_size=%lu; columns_size=%lu]",
                         tablet_schema.size(),
                         columns.size());
        
        return OLAP_ERR_MALLOC_ERROR;
    }

    // 因为区间key有可能大于schema的length，所以取大
    vector<size_t> field_buf_lens;
    for (size_t i = 0; i < _field_array_size; ++i) {
        if (lengths != nullptr && i < lengths->size()) {
            size_t buf_len = 0;
            switch (tablet_schema[i].type) {
                case OLAP_FIELD_TYPE_VARCHAR:
                    buf_len = (*lengths)[i] + sizeof(VarCharField::LengthValueType);
                    break;
                case OLAP_FIELD_TYPE_CHAR:
                    buf_len = (*lengths)[i];
                    break;
                default:
                    ;
            }
            field_buf_lens.push_back(std::max(buf_len, (size_t)tablet_schema[i].length));
        } else {
            field_buf_lens.push_back(tablet_schema[i].length);
        }
    }
    
    for (size_t i = 0; i < _field_array_size; ++i) {
        _field_array[i] = NULL;
        _field_length_array[i] = field_buf_lens[i] + sizeof(char);
        _field_offset[i] = field_buf_lens[i] + sizeof(char);
    }

    size_t len = 0;
    _columns_size = columns.size();
    for (size_t i = 0; i < _columns_size; ++i) {
        _field_array[columns[i]] = Field::create(tablet_schema[columns[i]]);
        if (_field_array[columns[i]] == NULL) {
            OLAP_LOG_WARNING("Fail to create field.");
            return OLAP_ERR_INIT_FAILED;
        }
        // 判断是否需要进行到MySQL的类型转换
        if (tablet_schema[columns[i]].type == OLAP_FIELD_TYPE_DISCRETE_DOUBLE) {
            _is_mysql_compatible = false;
        }

        if (tablet_schema[columns[i]].type == OLAP_FIELD_TYPE_CHAR
                  || tablet_schema[columns[i]].type == OLAP_FIELD_TYPE_VARCHAR) {
            _field_array[columns[i]]->set_buf_size(field_buf_lens[columns[i]]);
            _field_array[columns[i]]->set_string_length(field_buf_lens[columns[i]]);
        }   
        _columns[i] = columns[i];
        // 计算行长度
        len += field_buf_lens[columns[i]];
    }

    // 计算schema当中key的个数，顺便检查是否有value在key之前的错误
    _key_column_num = _field_array_size;
    bool is_last_column_key = true;
    bool is_current_column_key = false;
    for (size_t i = 0; i < _field_array_size; ++i) {
        is_current_column_key = tablet_schema[i].is_key;
        if (is_last_column_key && !is_current_column_key) {
            _key_column_num = i;
        }

        // 发现有value在key前面的情况，则报错
        if (!is_last_column_key && is_current_column_key) {
            OLAP_LOG_WARNING("invalid schema format; value column is before key column."
                             "[column_index=%lu]",
                             i);
            return OLAP_ERR_INVALID_SCHEMA;
        }

        is_last_column_key = is_current_column_key;
    }

    _length = len + _columns_size;
    _length_mysql = len;
    _buf = new (nothrow) char[_length];
    if (_buf == NULL) {
        OLAP_LOG_WARNING("Fail to malloc _buf.");
        return OLAP_ERR_MALLOC_ERROR;
    }
    memset(_buf, 0, _length);

    size_t offset = 0;
    for (size_t i = 0; i < _columns_size; ++i) {
        _field_array[_columns[i]]->attach_field(_buf + offset);
        offset += _field_offset[_columns[i]];
    }
    _is_inited = true;
    return OLAP_SUCCESS;
}

int RowCursor::full_key_cmp(const RowCursor& other) const {
    if (!_is_inited) {
        OLAP_LOG_FATAL("row curosr is not inited.");
        return -1;
    }

    // 只有key column才会参与比较
    int res = 0;
    for (size_t i = 0; i < _key_column_num; ++i) {
        if (0 != (res = _field_array[i]->cmp(other._field_array[i]))) {
            return res;
        }
    }

    return res;
}

int RowCursor::cmp(const RowCursor& other) const {
    if (!_is_inited) {
        OLAP_LOG_FATAL("row curosr is not inited.");
        return -1;
    }

    int res = 0;
    // 两个cursor有可能field个数不同，只比较共同部分
    size_t common_prefix_count = min(_key_column_num, other._key_column_num);
    // 只有key column才会参与比较
    for (size_t i = 0; i < common_prefix_count; ++i) {
        if (_field_array[i] == NULL || other._field_array[i] == NULL) {
            continue;
        }

        if (0 != (res = _field_array[i]->cmp(other._field_array[i]))) {
            return res;
        }
    }

    return res;
}

int RowCursor::index_cmp(const RowCursor& other) const {
    if (!_is_inited) {
        OLAP_LOG_FATAL("row curosr is not inited.");
        return -1;
    }

    int res = 0;
    // 两个cursor有可能field个数不同，只比较共同部分
    size_t common_prefix_count = min(_columns_size, other._key_column_num);
    // 只有key column才会参与比较
    for (size_t i = 0; i < common_prefix_count; ++i) {
        if (_field_array[i] == NULL || other._field_array[i] == NULL) {
            continue;
        }

        if (0 != (res = _field_array[i]->index_cmp(other._field_array[i]))) {
            return res;
        }
    }

    return res;
}

bool RowCursor::equal(const RowCursor& other) const {
    if (!_is_inited) {
        OLAP_LOG_FATAL("row curosr is not inited.");
        return false;
    }

    // 按field顺序从后往前比较，有利于尽快发现不同，提升比较性能
    size_t common_prefix_count = min(_key_column_num, other._key_column_num);
    for (int i = common_prefix_count - 1; i >= 0; --i) {
        if (_field_array[i] == NULL || other._field_array[i] == NULL) {
            continue;
        }

        if (false == _field_array[i]->equal(other._field_array[i])) {
            return false;
        }
    }
    return true;
}

void RowCursor::finalize_one_merge() {
    
    for (size_t i = _key_column_num; i < _field_array_size; ++i) {
        if (_field_array[i] == NULL) {
            continue;
        }
        if (_field_array[i]->get_aggregation_method() == OLAP_FIELD_AGGREGATION_HLL_UNION) {
            Field* field = _field_array[i];
            field->finalize_one_merge();
        }       
    }
}

OLAPStatus RowCursor::aggregate(const RowCursor& other) {
    CHECK_ROWCURSOR_INIT();
    if (_field_array_size != other._field_array_size) {
        OLAP_LOG_WARNING("Fail to do aggregate; the two rowcursors do not match."
                         "[_field_array_size=%lu; other._field_array_size=%lu; "
                         "_key_column_num=%lu; other._key_column_num=%lu]",
                         _field_array_size,
                         other._field_array_size,
                         _key_column_num,
                         other._key_column_num);
        
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    // 只有value column才会参与aggregate
    for (size_t i = _key_column_num; i < _field_array_size; ++i) {
        if (_field_array[i] == NULL || other._field_array[i] == NULL) {
            continue;
        }

        switch (_field_array[i]->get_aggregation_method()) {
        case OLAP_FIELD_AGGREGATION_MIN:
            if (true == is_null(i) && true == other.is_null(i)) {
                break;
            } else if (false == is_null(i) && true == other.is_null(i)) {
                set_null(i);
                _field_array[i]->copy(other._field_array[i]);
                _field_length_array[i] = _field_array[i]->field_size();
                break;
            } else if (true == is_null(i) && false == other.is_null(i)) {
                break;
            } else {
                _field_array[i]->aggregate(other._field_array[i]);
                _field_length_array[i] = _field_array[i]->field_size();
            }
            break;
        case OLAP_FIELD_AGGREGATION_MAX:
        case OLAP_FIELD_AGGREGATION_SUM:
        case OLAP_FIELD_AGGREGATION_HLL_UNION:
            if (true == is_null(i) && true == other.is_null(i)) {
                break;
            } else if (false == is_null(i) && true == other.is_null(i)) {
                break;
            } else if (true == is_null(i) && false == other.is_null(i)) {
                set_not_null(i);
                _field_array[i]->copy(other._field_array[i]);
                _field_length_array[i] = _field_array[i]->field_size();
            } else {
                _field_array[i]->aggregate(other._field_array[i]);
                _field_length_array[i] = _field_array[i]->field_size();
            }
            break;
        case OLAP_FIELD_AGGREGATION_REPLACE:
            if (true == is_null(i) && true == other.is_null(i)) {
                break;
            } else if (false == is_null(i) && true == other.is_null(i)) {
                set_null(i);
            } else if (true == is_null(i) && false == other.is_null(i)) {
                set_not_null(i);
            }
            _field_array[i]->aggregate(other._field_array[i]);
            _field_length_array[i] = _field_array[i]->field_size();
            break;
         case OLAP_FIELD_AGGREGATION_NONE:
         case OLAP_FIELD_AGGREGATION_UNKNOWN:
         default:
            break;
        }
    }

    return OLAP_SUCCESS;
}

OLAPStatus RowCursor::_write(char* buf, StorageFormatEnum format) const {
#ifndef PERFORMANCE
    CHECK_ROWCURSOR_INIT();

    if (buf == NULL) {
        OLAP_LOG_WARNING("input pointer is NULL. [buf=%p]", buf);
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }
#endif
    size_t offset = 0;
    for (size_t i = 0; i < _columns_size; ++i) {
        size_t column_id = _columns[i];
        size_t field_size = 0;

        if (LOCAL_STORAGE_FORMAT == format) {
            field_size = _field_array[column_id]->field_size();
            _field_array[column_id]->to_storage(buf + offset);
        } else {
            field_size = _field_array[column_id]->field_size();
            _field_array[column_id]->to_mysql(buf + offset);
        }
        offset += field_size;
    }

    return OLAP_SUCCESS;
}

OLAPStatus RowCursor::write(char* buf) const {
    return _write(buf, LOCAL_STORAGE_FORMAT);
}

OLAPStatus RowCursor::write_mysql(char* buf) const {
    return _write(buf, MYSQL_FORMAT);
}

OLAPStatus RowCursor::write_by_indices_mysql(const vector<uint32_t>& indices,
                                         char* buf,
                                         size_t buf_size,
                                         size_t* written_size) const {
    CHECK_ROWCURSOR_INIT();

    if (buf == NULL || buf_size == 0) {
        OLAP_LOG_WARNING("input params are invalid. [indices_size=%ld; buf=%p; buf_size=%lu]",
                         indices.size(),
                         buf,
                         buf_size);
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }
    for (size_t i = 0; i < indices.size(); ++i) {
        if (indices[i] > _field_array_size - 1) {
            OLAP_LOG_WARNING("input indices are invalid."
                             "[index=%lu; index_value=%d; column_size=%lu]",
                             i,
                             indices[i],
                             _field_array_size);
            return OLAP_ERR_INPUT_PARAMETER_ERROR;
        }
    }

    size_t total_size = 0;
    for (size_t i = 0; i < indices.size(); ++i) {
        total_size += _field_array[i]->size();
    }
    if (total_size > buf_size) {
        OLAP_LOG_WARNING("write buffer is not enough. [need_size=%ld; buf_size=%ld]",
                         total_size,
                         buf_size);
        return OLAP_ERR_BUFFER_OVERFLOW;
    }

    char *curr_buf = buf;
    for (size_t i = 0, size = indices.size(); i < size; ++i) {
        size_t current_index = indices[i];
        _field_array[current_index]->to_mysql(curr_buf);
        curr_buf += _field_array[current_index]->size();
    }

    if (written_size) {
        *written_size = curr_buf - buf;
    }
        
    return OLAP_SUCCESS;
}

OLAPStatus RowCursor::read(const char* buf, size_t max_buf_len) {
    CHECK_ROWCURSOR_INIT();

    if (buf == NULL) {
        OLAP_LOG_WARNING("input pointer is NULL. [buf=%p]", buf);
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    size_t offset = 0;
    size_t length = 0;
    
    for (size_t i = 0; i < _columns_size; ++i) {
        _field_array[_columns[i]]->from_storage(buf + offset);
        length = _field_array[_columns[i]]->field_size();
        offset += length;
        _field_length_array[_columns[i]] = length;

        if (offset > max_buf_len) {
            OLAP_LOG_WARNING("buffer overflow. [max_buf_len=%lu offset=%lu]", max_buf_len, offset);

            return OLAP_ERR_BUFFER_OVERFLOW;
        }
    }

    return OLAP_SUCCESS;
}


OLAPStatus RowCursor::read_field(const char* buf, size_t index, size_t field_size) {
    CHECK_ROWCURSOR_INIT();

    if (buf == NULL) {
        OLAP_LOG_WARNING("input pointer is NULL. [buf=%p]", buf);
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }
    _field_array[index]->from_storage(buf);
    _field_length_array[index] = _field_array[index]->field_size();
    return OLAP_SUCCESS;
}

OLAPStatus RowCursor::build_max_key() {
    CHECK_ROWCURSOR_INIT();

    for (uint32_t i = 0; i < _columns_size; ++i) {
        _field_array[_columns[i]]->set_to_max();
        _field_length_array[_columns[i]] = _field_array[_columns[i]]->field_size();
    }
    
    return OLAP_SUCCESS;
}

OLAPStatus RowCursor::build_min_key() {
    CHECK_ROWCURSOR_INIT();

    for (uint32_t i = 0; i < _columns_size; ++i) {
        _field_array[_columns[i]]->set_to_min();
        _field_length_array[_columns[i]] = _field_array[_columns[i]]->field_size();
    }

    return OLAP_SUCCESS;
}

OLAPStatus RowCursor::read_by_index(size_t index, const char* buf) {
    CHECK_ROWCURSOR_INIT();

    if (index >= _field_array_size) {
        OLAP_LOG_WARNING("index exceeds the max. [index=%lu; max_index=%lu]",
                         index,
                         _field_array_size);
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    _field_array[index]->from_storage(buf);
    _field_length_array[index] = _field_array[index]->field_size();
    
    return OLAP_SUCCESS;
}

OLAPStatus RowCursor::read_by_index(size_t index, const char* buf, int length) {
    CHECK_ROWCURSOR_INIT();

    if (index >= _field_array_size) {
        OLAP_LOG_WARNING("index exceeds the max. [index=%lu; max_index=%lu]",
                         index,
                         _field_array_size);
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    reinterpret_cast<VarCharField*>(_field_array[index])->from_storage_length(buf, length);
    _field_length_array[index] = _field_array[index]->field_size();
    
    return OLAP_SUCCESS;
}

OLAPStatus RowCursor::write_by_index(size_t index, char* buf) const {
    CHECK_ROWCURSOR_INIT();
    
    if (index >= _field_array_size) {
        OLAP_LOG_WARNING("index exceeds the max. [index=%lu; max_index=%lu]",
                         index,
                         _field_array_size);
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }
    
    if (_field_array[index]->is_null()) {
        buf[0] |= 1;
    } else {
        buf[0] &= ~1;
    }
    _field_array[index]->to_storage(buf + sizeof(char));

    return OLAP_SUCCESS;
}

OLAPStatus RowCursor::write_index_by_index(size_t index, char* index_buf) const {
    CHECK_ROWCURSOR_INIT();
    
    if (index >= _field_array_size) {
        OLAP_LOG_WARNING("index exceeds the max. [index=%lu; max_index=%lu]",
                         index,
                         _field_array_size);
        
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }
   
    _field_array[index]->to_index(index_buf);

    return OLAP_SUCCESS;
}

OLAPStatus RowCursor::from_string(const vector<string>& val_string_array) {
    CHECK_ROWCURSOR_INIT();
    
    if (val_string_array.size() != _columns_size) {
        OLAP_LOG_WARNING("column count does not match. [string_array_size=%lu; field_count=%lu]",
                         val_string_array.size(),
                         _field_array_size);
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    for (size_t i = 0; i < val_string_array.size(); ++i) {
        OLAPStatus res = _field_array[_columns[i]]->from_string(val_string_array[i].c_str());
        if (OLAP_SUCCESS != res) {
            OLAP_LOG_WARNING("Fail to convert field from string.[val_string=%s res=%d]", 
                    val_string_array[i].c_str(), res);
            return res;
        }
        _field_length_array[_columns[i]] = _field_array[_columns[i]]->field_size();
    }

    return OLAP_SUCCESS;
}

std::vector<std::string> RowCursor::to_string_vector() const {
    std::vector<std::string> result;

    for (size_t i = 0; i < _columns_size; ++i) {
        if (_field_array[_columns[i]] != NULL) {
            result.push_back(_field_array[_columns[i]]->to_string());
        } else {
            result.push_back("");
        }
    }

    return result;
}

string RowCursor::to_string() const {
    string result;
    for (size_t i = 0; i < _columns_size; ++i) {
        if (i > 0) {
            result.append("|");
        }

        result.append(std::to_string(_field_array[_columns[i]]->is_null()));
        result.append("&");
        if (_field_array[_columns[i]]->is_null()) {
            result.append("NULL");
        } else {
            result.append(_field_array[_columns[i]]->to_string());
        }
    }

    return result;
}

bool RowCursor::is_null(size_t index) const {
    return _field_array[index]->is_null();
}

bool RowCursor::is_null_converted(size_t index) const {
    size_t column_id = _columns[index];
    return _field_array[column_id]->is_null();
}

string RowCursor::to_string(string sep) const {
    string result;
    for (size_t i = 0; i < _columns_size; ++i) {
        if (i > 0) {
            result.append(sep);
        }
        
        if (_field_array[_columns[i]] != NULL) {
            result.append(_field_array[_columns[i]]->to_string());
        } else {
            result.append("NULL");
        }
    }

    return result;
}

OLAPStatus RowCursor::get_first_different_column_id(const RowCursor& other,
                                                size_t* first_diff_id) const {
    CHECK_ROWCURSOR_INIT();
    
    if (first_diff_id == NULL) {
        OLAP_LOG_WARNING("input parameter 'first_diff_id' is NULL.");
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    if (_columns_size != other.field_count()) {
        OLAP_LOG_WARNING("column number of two cursors do not match.");
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    size_t i = 0;
    for (; i < _field_array_size; ++i) {
        if (_field_array[i] == NULL || other._field_array[i] == NULL) {
            continue;
        }
            
        if (_field_array[i]->cmp(other._field_array[i]) != 0) {
            break;
        }
    }
    // Maybe a bug: if we don't find a different column, we shouldn't set first_diff_id.
    *first_diff_id = i;

    return OLAP_SUCCESS;
}

}  // namespace palo
