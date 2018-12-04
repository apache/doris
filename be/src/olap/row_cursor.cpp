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
#include <unordered_set>

using std::min;
using std::nothrow;
using std::string;
using std::vector;

namespace doris {
RowCursor::RowCursor() :
        _key_column_num(0),
        _fixed_len(0),
        _variable_len(0),
        _variable_buf_allocated_by_pool(false) {}

RowCursor::~RowCursor() {
    // delete RowCursor's Fields
    for (auto field : _field_array) {
        delete field;
    }

    _key_column_num = 0;

    delete [] _owned_fixed_buf;
    if (!_variable_buf_allocated_by_pool) {
        for (HllContext* context : hll_contexts) {
            delete context;
        }

        delete [] _variable_buf;
    }
}

OLAPStatus RowCursor::_init(const std::vector<TabletColumn>& schema,
                            const std::vector<uint32_t>& columns) {
    _field_array.resize(schema.size(), nullptr);
    _columns = columns;

    std::vector<size_t> field_buf_lens;
    for (size_t i = 0; i < schema.size(); ++i) {
        const TabletColumn& column = schema[i];
        FieldType type = column.type();
        if (type == OLAP_FIELD_TYPE_CHAR ||
            type == OLAP_FIELD_TYPE_VARCHAR ||
            type == OLAP_FIELD_TYPE_HLL) {
            field_buf_lens.push_back(sizeof(Slice));
        } else {
            field_buf_lens.push_back(column.length());
        }
    }

    _key_column_num = schema.size();
    for (size_t i = schema.size() - 1; i >= 0; --i) {
        const TabletColumn& column = schema[i];
        if (column.is_key()) {
            _key_column_num = i + 1;
            break;
        }
    }

    _fixed_len = 0;
    _variable_len = 0;
    for (auto cid : _columns) {
        const TabletColumn& column = schema[cid];
        _field_array[cid] = Field::create(column);
        if (_field_array[cid] == nullptr) {
            LOG(WARNING) << "Fail to create field.";
            return OLAP_ERR_INIT_FAILED;
        }
        _fixed_len += field_buf_lens[cid] + 1; //1 for null byte
        FieldType type = column.type();
        if (type == OLAP_FIELD_TYPE_VARCHAR) {
            _variable_len += column.length() - OLAP_STRING_MAX_BYTES;
        } else if (type == OLAP_FIELD_TYPE_CHAR) {
            _variable_len += column.length();
        } else if (type == OLAP_FIELD_TYPE_HLL) {
            _variable_len += HLL_COLUMN_DEFAULT_LEN + sizeof(HllContext*);
        }
        _string_columns.push_back(cid);
    }

    _fixed_buf = new (nothrow) char[_fixed_len];
    if (_fixed_buf == nullptr) {
        LOG(WARNING) <<  "Fail to malloc _fixed_buf.";
        return OLAP_ERR_MALLOC_ERROR;
    }
    _owned_fixed_buf = _fixed_buf;
    memset(_fixed_buf, 0, _fixed_len);

    // we must make sure that the offset is the same with RowBlock's
    std::unordered_set<uint32_t> column_set(_columns.begin(), _columns.end());
    _field_offsets.resize(schema.size(), -1);
    size_t offset = 0;
    for (int cid = 0; cid < schema.size(); ++cid) {
        if (column_set.find(cid) != std::end(column_set)) {
            _field_offsets[cid] = offset;
            _field_array[cid]->set_offset(offset);
            offset += field_buf_lens[cid] + 1;
        }
    }

    return OLAP_SUCCESS;
}

OLAPStatus RowCursor::init(const TabletSchema& schema) {
    return init(schema.columns(), schema.num_columns());
}

OLAPStatus RowCursor::init(const std::vector<TabletColumn>& schema) {
    return init(schema, schema.size());
}

OLAPStatus RowCursor::init(const TabletSchema& schema, size_t column_count) {
    if (column_count > schema.num_columns()) {
        LOG(WARNING) << "Input param are invalid. Column count is bigger than num_columns of schema. "
                     << "column_count=" << column_count
                     << ", schema.num_columns=" << schema.num_columns();
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    std::vector<uint32_t> columns;
    for (size_t i = 0; i < column_count; ++i) {
        columns.push_back(i);
    }
    RETURN_NOT_OK(_init(schema.columns(), columns));
    return OLAP_SUCCESS;
}

OLAPStatus RowCursor::init(const std::vector<TabletColumn>& schema, size_t column_count) {
    if (column_count > schema.size()) {
        LOG(WARNING) << "Input param are invalid. Column count is bigger than num_columns of schema. "
                     << "column_count=" << column_count
                     << ", schema.num_columns=" << schema.size();
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    std::vector<uint32_t> columns;
    for (size_t i = 0; i < column_count; ++i) {
        columns.push_back(i);
    }
    RETURN_NOT_OK(_init(schema, columns));
    return OLAP_SUCCESS;
}

OLAPStatus RowCursor::init(const TabletSchema& schema,
                           const vector<uint32_t>& columns) {
    RETURN_NOT_OK(_init(schema.columns(), columns));
    return OLAP_SUCCESS;
}

OLAPStatus RowCursor::init_scan_key(const TabletSchema& schema,
                                    const std::vector<std::string>& scan_keys) {
    size_t scan_key_size = scan_keys.size();
    if (scan_key_size > schema.num_columns()) {
        LOG(WARNING) << "Input param are invalid. Column count is bigger than num_columns of schema. "
                     << "column_count=" << scan_key_size
                     << ", schema.num_columns=" << schema.num_columns();
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    std::vector<uint32_t> columns;
    for (size_t i = 0; i < scan_key_size; ++i) {
        columns.push_back(i);
    }

    RETURN_NOT_OK(_init(schema.columns(), columns));

    // NOTE: cid equal with column index
    // Hyperloglog cannot be key, no need to handle it
    _variable_len = 0;
    for (auto cid : _columns) {
        const TabletColumn& column = schema.column(cid);
        FieldType type = column.type();
        if (type == OLAP_FIELD_TYPE_VARCHAR) {
            _variable_len += scan_keys[cid].length();
        } else if (type == OLAP_FIELD_TYPE_CHAR) {
            _variable_len += std::max(
                scan_keys[cid].length(), column.length());
        }
    }

    // variable_len for null bytes
    _variable_buf = new(nothrow) char[_variable_len];
    if (_variable_buf == nullptr) {
        OLAP_LOG_WARNING("Fail to malloc _variable_buf.");
        return OLAP_ERR_MALLOC_ERROR;
    }
    memset(_variable_buf, 0, _variable_len);
    char* fixed_ptr = _fixed_buf;
    char* variable_ptr = _variable_buf;
    for (auto cid : _columns) {
        const TabletColumn& column = schema.column(cid);
        fixed_ptr = _fixed_buf + _field_array[cid]->get_offset();
        FieldType type = column.type();
        if (type == OLAP_FIELD_TYPE_VARCHAR) {
            Slice* slice = reinterpret_cast<Slice*>(fixed_ptr + 1);
            slice->data = variable_ptr;
            slice->size = scan_keys[cid].length();
            variable_ptr += scan_keys[cid].length();
        } else if (type == OLAP_FIELD_TYPE_CHAR) {
            Slice* slice = reinterpret_cast<Slice*>(fixed_ptr + 1);
            slice->data = variable_ptr;
            slice->size = std::max(scan_keys[cid].length(), column.length());
            variable_ptr += slice->size;
        }
    }

    return OLAP_SUCCESS;
}

OLAPStatus RowCursor::allocate_memory_for_string_type(
        const TabletSchema& schema,
        MemPool* mem_pool) {
    // allocate memory for string type(char, varchar, hll)
    // The memory allocated in this function is used in aggregate and copy function
    if (_variable_len == 0) { return OLAP_SUCCESS; }
    if (mem_pool != nullptr) {
        /*
         * It is called by row_cursor of row_block in sc(schema change)
         * or be/ce if mem_pool is not null. RowCursor in rowblock do not
         * allocate memory for string type in initialization. So memory
         * for string and hllcontext are all administrated by mem_pool.
         */
        _variable_buf = reinterpret_cast<char*>(mem_pool->allocate(_variable_len));
        _variable_buf_allocated_by_pool = true;
    } else {
        DCHECK(_variable_buf == nullptr) << "allocate memory twice";
        _variable_buf = new (nothrow) char[_variable_len];
    }
    memset(_variable_buf, 0, _variable_len);

    // init slice of char, varchar, hll type
    char* fixed_ptr = _fixed_buf;
    char* variable_ptr = _variable_buf;
    for (auto cid : _columns) {
        const TabletColumn& column = schema.column(cid);
        fixed_ptr = _fixed_buf + _field_array[cid]->get_offset();
        FieldType type = column.type();
        if (type == OLAP_FIELD_TYPE_VARCHAR) {
            Slice* slice = reinterpret_cast<Slice*>(fixed_ptr + 1);
            slice->data = variable_ptr;
            slice->size = column.length() - OLAP_STRING_MAX_BYTES;
            variable_ptr += slice->size;
        } else if (type == OLAP_FIELD_TYPE_CHAR) {
            Slice* slice = reinterpret_cast<Slice*>(fixed_ptr + 1);
            slice->data = variable_ptr;
            slice->size = column.length();
            variable_ptr += slice->size;
        } else if (type == OLAP_FIELD_TYPE_HLL) {
            Slice* slice = reinterpret_cast<Slice*>(fixed_ptr + 1);
            HllContext* context = nullptr;
            if (mem_pool != nullptr) {
                char* mem = reinterpret_cast<char*>(mem_pool->allocate(sizeof(HllContext)));
                context = new (mem) HllContext;
            } else {
                // store context addr, which will be freed
                // in deconstructor if allocated by new function
                context = new HllContext();
                hll_contexts.push_back(context);
            }

            *(size_t*)(variable_ptr) = (size_t)(context);
            variable_ptr += sizeof(HllContext*);
            slice->data = variable_ptr;
            slice->size = HLL_COLUMN_DEFAULT_LEN;
            variable_ptr += slice->size;
        }
    }
    return OLAP_SUCCESS;
}

int RowCursor::full_key_cmp(const RowCursor& other) const {
    // 只有key column才会参与比较
    int res = 0;
    for (size_t i = 0; i < _key_column_num; ++i) {
        char* left = _field_array[i]->get_field_ptr(_fixed_buf);
        char* right = other._field_array[i]->get_field_ptr(other.get_buf());
        res = _field_array[i]->cmp(left, right);
        if (res != 0) {
            return res;
        }
    }

    return res;
}

int RowCursor::cmp(const RowCursor& other) const {
    int res = 0;
    // 两个cursor有可能field个数不同，只比较共同部分
    size_t common_prefix_count = min(_key_column_num, other._key_column_num);
    // 只有key column才会参与比较
    for (size_t i = 0; i < common_prefix_count; ++i) {
        if (_field_array[i] == nullptr || other._field_array[i] == nullptr) {
            continue;
        }

        char* left = _field_array[i]->get_field_ptr(_fixed_buf);
        char* right = other._field_array[i]->get_field_ptr(other.get_buf());
        res = _field_array[i]->cmp(left, right);
        if (res != 0) {
            return res;
        }
    }

    return res;
}

int RowCursor::index_cmp(const RowCursor& other) const {
    int res = 0;
    // 两个cursor有可能field个数不同，只比较共同部分
    size_t common_prefix_count = min(_columns.size(), other._key_column_num);
    // 只有key column才会参与比较
    for (size_t i = 0; i < common_prefix_count; ++i) {
        if (_field_array[i] == nullptr || other._field_array[i] == nullptr) {
            continue;
        }
        char* left = _field_array[i]->get_field_ptr(_fixed_buf);
        char* right = other._field_array[i]->get_field_ptr(other.get_buf());
        res = _field_array[i]->index_cmp(left, right);
        if (res != 0) {
            return res;
        }
    }

    return res;
}

bool RowCursor::equal(const RowCursor& other) const {
    // 按field顺序从后往前比较，有利于尽快发现不同，提升比较性能
    size_t common_prefix_count = min(_key_column_num, other._key_column_num);
    for (int i = common_prefix_count - 1; i >= 0; --i) {
        if (_field_array[i] == nullptr || other._field_array[i] == nullptr) {
            continue;
        }
        char* left = _field_array[i]->get_field_ptr(_fixed_buf);
        char* right = other._field_array[i]->get_field_ptr(other.get_buf());
        if (!_field_array[i]->equal(left, right)) {
            return false;
        }
    }
    return true;
}

void RowCursor::finalize_one_merge() {
    for (size_t i = _key_column_num; i < _field_array.size(); ++i) {
        if (_field_array[i] == nullptr) {
            continue;
        }
        char* dest = _field_array[i]->get_ptr(_fixed_buf);
        _field_array[i]->finalize(dest);
    }
}

void RowCursor::aggregate(const RowCursor& other) {
    // 只有value column才会参与aggregate
    for (size_t i = _key_column_num; i < _field_array.size(); ++i) {
        if (_field_array[i] == nullptr || other._field_array[i] == nullptr) {
            continue;
        }

        char* dest = _field_array[i]->get_field_ptr(_fixed_buf);
        char* src = other._field_array[i]->get_field_ptr(other.get_buf());
        _field_array[i]->aggregate(dest, src);
    }
}

OLAPStatus RowCursor::build_max_key() {
    for (auto cid : _columns) {
        Field* field = _field_array[cid];
        char* dest = field->get_ptr(_fixed_buf);
        field->set_to_max(dest);
        field->set_not_null(_fixed_buf);
    }
    return OLAP_SUCCESS;
}

OLAPStatus RowCursor::build_min_key() {
    for (auto cid : _columns) {
        Field* field = _field_array[cid];
        char* dest = field->get_ptr(_fixed_buf);
        field->set_to_min(dest);
        field->set_null(_fixed_buf);
    }

    return OLAP_SUCCESS;
}

OLAPStatus RowCursor::from_tuple(const OlapTuple& tuple) {
    if (tuple.size() != _columns.size()) {
        LOG(WARNING) << "column count does not match. tuple_size=" << tuple.size()
            << ", field_count=" << _field_array.size();
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    for (size_t i = 0; i < tuple.size(); ++i) {
        Field* field = _field_array[_columns[i]];
        if (tuple.is_null(i)) {
            field->set_null(_fixed_buf);
            continue;
        }
        field->set_not_null(_fixed_buf);
        char* buf = field->get_ptr(_fixed_buf);
        OLAPStatus res = field->from_string(buf, tuple.get_value(i));
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to convert field from string. string=" << tuple.get_value(i)
                << ", res=" << res;
            return res;
        }
    }

    return OLAP_SUCCESS;
}

OlapTuple RowCursor::to_tuple() const {
    OlapTuple tuple;

    for (auto cid : _columns) {
        if (_field_array[cid] != nullptr) {
            Field* field = _field_array[cid];
            char* src = field->get_ptr(_fixed_buf);
            if (field->is_null(_fixed_buf)) {
                tuple.add_null();
            } else {
                tuple.add_value(field->to_string(src));
            }
        } else {
            tuple.add_value("");
        }
    }

    return tuple;
}

string RowCursor::to_string() const {
    string result;
    size_t i = 0;
    for (auto cid : _columns) {
        if (i++ > 0) {
            result.append("|");
        }

        Field* field = _field_array[cid];
        result.append(std::to_string(field->is_null(_fixed_buf)));
        result.append("&");
        if (field->is_null(_fixed_buf)) {
            result.append("NULL");
        } else {
            char* src = field->get_ptr(_fixed_buf);
            result.append(field->to_string(src));
        }
    }

    return result;
}

string RowCursor::to_string(string sep) const {
    string result;
    size_t i = 0;
    for (auto cid : _columns) {
        if (i++ > 0) {
            result.append(sep);
        }

        Field* field = _field_array[cid];
        if (field != nullptr) {
            char* src = field->get_ptr(_fixed_buf);
            result.append(field->to_string(src));
        } else {
            result.append("NULL");
        }
    }

    return result;
}

OLAPStatus RowCursor::get_first_different_column_id(const RowCursor& other,
                                                size_t* first_diff_id) const {
    if (first_diff_id == nullptr) {
        OLAP_LOG_WARNING("input parameter 'first_diff_id' is NULL.");
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    if (_columns.size() != other.field_count()) {
        OLAP_LOG_WARNING("column number of two cursors do not match.");
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    size_t i = 0;
    for (; i < _field_array.size(); ++i) {
        if (_field_array[i] == nullptr || other._field_array[i] == nullptr) {
            continue;
        }

        char* left = _field_array[i]->get_field_ptr(_fixed_buf);
        char* right = other._field_array[i]->get_field_ptr(other.get_buf());
        if (0 != (_field_array[i]->cmp(left, right))) {
            break;
        }
    }
    // Maybe a bug: if we don't find a different column, we shouldn't set first_diff_id.
    *first_diff_id = i;

    return OLAP_SUCCESS;
}

}  // namespace doris
