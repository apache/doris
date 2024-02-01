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

#include "util/stack_util.h"

using std::min;
using std::nothrow;
using std::string;
using std::vector;

namespace doris {
using namespace ErrorCode;
RowCursor::RowCursor()
        : _fixed_len(0), _variable_len(0), _string_field_count(0), _long_text_buf(nullptr) {}

RowCursor::~RowCursor() {
    delete[] _owned_fixed_buf;
    delete[] _variable_buf;
    if (_string_field_count > 0 && _long_text_buf != nullptr) {
        for (int i = 0; i < _string_field_count; ++i) {
            free(_long_text_buf[i]);
        }
        free(_long_text_buf);
    }
}

Status RowCursor::_init(const std::vector<uint32_t>& columns) {
    _variable_len = 0;
    for (auto cid : columns) {
        if (_schema->column(cid) == nullptr) {
            LOG(WARNING) << "Fail to create field.";
            return Status::Error<INIT_FAILED>();
        }
        _variable_len += column_schema(cid)->get_variable_len();
        if (_schema->column(cid)->type() == OLAP_FIELD_TYPE_STRING ||
            _schema->column(cid)->type() == OLAP_FIELD_TYPE_JSONB) {
            ++_string_field_count;
        }
    }

    _fixed_len = _schema->schema_size();
    _fixed_buf = new (nothrow) char[_fixed_len]();
    if (_fixed_buf == nullptr) {
        LOG(WARNING) << "Fail to malloc _fixed_buf.";
        return Status::Error<MEM_ALLOC_FAILED>();
    }
    _owned_fixed_buf = _fixed_buf;

    return Status::OK();
}

Status RowCursor::_init(const std::shared_ptr<Schema>& shared_schema,
                        const std::vector<uint32_t>& columns) {
    _schema.reset(new Schema(*shared_schema.get()));
    return _init(columns);
}

Status RowCursor::_init(const std::vector<TabletColumn>& schema,
                        const std::vector<uint32_t>& columns) {
    _schema.reset(new Schema(schema, columns));
    return _init(columns);
}

Status RowCursor::_init_scan_key(TabletSchemaSPtr schema,
                                 const std::vector<std::string>& scan_keys) {
    // NOTE: cid equal with column index
    // Hyperloglog cannot be key, no need to handle it
    _variable_len = 0;
    for (auto cid : _schema->column_ids()) {
        const TabletColumn& column = schema->column(cid);
        FieldType type = column.type();
        if (type == OLAP_FIELD_TYPE_VARCHAR) {
            _variable_len += scan_keys[cid].length();
        } else if (type == OLAP_FIELD_TYPE_CHAR || type == OLAP_FIELD_TYPE_ARRAY) {
            _variable_len += std::max(scan_keys[cid].length(), column.length());
        } else if (type == OLAP_FIELD_TYPE_STRING || type == OLAP_FIELD_TYPE_JSONB) {
            ++_string_field_count;
        }
    }

    // variable_len for null bytes
    RETURN_NOT_OK(_alloc_buf());
    char* fixed_ptr = _fixed_buf;
    char* variable_ptr = _variable_buf;
    char** long_text_ptr = _long_text_buf;
    for (auto cid : _schema->column_ids()) {
        const TabletColumn& column = schema->column(cid);
        fixed_ptr = _fixed_buf + _schema->column_offset(cid);
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
        } else if (type == OLAP_FIELD_TYPE_STRING) {
            _schema->mutable_column(cid)->set_long_text_buf(long_text_ptr);
            Slice* slice = reinterpret_cast<Slice*>(fixed_ptr + 1);
            slice->data = *(long_text_ptr);
            slice->size = DEFAULT_TEXT_LENGTH;
            ++long_text_ptr;
        }
    }

    return Status::OK();
}

Status RowCursor::init(TabletSchemaSPtr schema) {
    return init(schema->columns(), schema->num_columns());
}

Status RowCursor::init(const std::vector<TabletColumn>& schema) {
    return init(schema, schema.size());
}

Status RowCursor::init(TabletSchemaSPtr schema, size_t column_count) {
    if (column_count > schema->num_columns()) {
        LOG(WARNING)
                << "Input param are invalid. Column count is bigger than num_columns of schema. "
                << "column_count=" << column_count
                << ", schema.num_columns=" << schema->num_columns();
        return Status::Error<INVALID_ARGUMENT>();
    }

    std::vector<uint32_t> columns;
    for (size_t i = 0; i < column_count; ++i) {
        columns.push_back(i);
    }
    RETURN_NOT_OK(_init(schema->columns(), columns));
    return Status::OK();
}

Status RowCursor::init(const std::vector<TabletColumn>& schema, size_t column_count) {
    if (column_count > schema.size()) {
        LOG(WARNING)
                << "Input param are invalid. Column count is bigger than num_columns of schema. "
                << "column_count=" << column_count << ", schema.num_columns=" << schema.size();
        return Status::Error<INVALID_ARGUMENT>();
    }

    std::vector<uint32_t> columns;
    for (size_t i = 0; i < column_count; ++i) {
        columns.push_back(i);
    }
    RETURN_NOT_OK(_init(schema, columns));
    return Status::OK();
}

Status RowCursor::init(TabletSchemaSPtr schema, const std::vector<uint32_t>& columns) {
    RETURN_NOT_OK(_init(schema->columns(), columns));
    return Status::OK();
}

Status RowCursor::init_scan_key(TabletSchemaSPtr schema,
                                const std::vector<std::string>& scan_keys) {
    size_t scan_key_size = scan_keys.size();
    if (scan_key_size > schema->num_columns()) {
        LOG(WARNING)
                << "Input param are invalid. Column count is bigger than num_columns of schema. "
                << "column_count=" << scan_key_size
                << ", schema.num_columns=" << schema->num_columns();
        return Status::Error<INVALID_ARGUMENT>();
    }

    std::vector<uint32_t> columns(scan_key_size);
    std::iota(columns.begin(), columns.end(), 0);

    RETURN_NOT_OK(_init(schema->columns(), columns));

    return _init_scan_key(schema, scan_keys);
}

Status RowCursor::init_scan_key(TabletSchemaSPtr schema, const std::vector<std::string>& scan_keys,
                                const std::shared_ptr<Schema>& shared_schema) {
    size_t scan_key_size = scan_keys.size();

    std::vector<uint32_t> columns;
    for (size_t i = 0; i < scan_key_size; ++i) {
        columns.push_back(i);
    }

    RETURN_NOT_OK(_init(shared_schema, columns));

    return _init_scan_key(schema, scan_keys);
}

// TODO(yingchun): parameter 'TabletSchemaSPtr  schema' is not used
Status RowCursor::allocate_memory_for_string_type(TabletSchemaSPtr schema) {
    // allocate memory for string type(char, varchar, hll, array)
    // The memory allocated in this function is used in aggregate and copy function
    if (_variable_len == 0 && _string_field_count == 0) {
        return Status::OK();
    }
    DCHECK(_variable_buf == nullptr) << "allocate memory twice";
    RETURN_NOT_OK(_alloc_buf());
    // init slice of char, varchar, hll type
    char* fixed_ptr = _fixed_buf;
    char* variable_ptr = _variable_buf;
    char** long_text_ptr = _long_text_buf;
    for (auto cid : _schema->column_ids()) {
        fixed_ptr = _fixed_buf + _schema->column_offset(cid);
        if (_schema->column(cid)->type() == OLAP_FIELD_TYPE_STRING ||
            _schema->column(cid)->type() == OLAP_FIELD_TYPE_JSONB) {
            Slice* slice = reinterpret_cast<Slice*>(fixed_ptr + 1);
            _schema->mutable_column(cid)->set_long_text_buf(long_text_ptr);
            slice->data = *(long_text_ptr);
            slice->size = DEFAULT_TEXT_LENGTH;
            ++long_text_ptr;
        } else if (_variable_len > 0) {
            variable_ptr = column_schema(cid)->allocate_memory(fixed_ptr + 1, variable_ptr);
        }
    }
    return Status::OK();
}

Status RowCursor::build_max_key() {
    for (auto cid : _schema->column_ids()) {
        const Field* field = column_schema(cid);
        char* dest = cell_ptr(cid);
        field->set_to_max(dest);
        set_not_null(cid);
    }
    return Status::OK();
}

Status RowCursor::build_min_key() {
    for (auto cid : _schema->column_ids()) {
        const Field* field = column_schema(cid);
        char* dest = cell_ptr(cid);
        field->set_to_min(dest);
        set_null(cid);
    }

    return Status::OK();
}

Status RowCursor::from_tuple(const OlapTuple& tuple) {
    if (tuple.size() != _schema->num_column_ids()) {
        LOG(WARNING) << "column count does not match. tuple_size=" << tuple.size()
                     << ", field_count=" << _schema->num_column_ids();
        return Status::Error<INVALID_ARGUMENT>();
    }

    for (size_t i = 0; i < tuple.size(); ++i) {
        auto cid = _schema->column_ids()[i];
        const Field* field = column_schema(cid);
        if (tuple.is_null(i)) {
            set_null(cid);
            continue;
        }
        set_not_null(cid);
        char* buf = cell_ptr(cid);
        Status res = field->from_string(buf, tuple.get_value(i), field->get_precision(),
                                        field->get_scale());
        if (!res.ok()) {
            LOG(WARNING) << "fail to convert field from string. string=" << tuple.get_value(i)
                         << ", res=" << res;
            return res;
        }
    }

    return Status::OK();
}

OlapTuple RowCursor::to_tuple() const {
    OlapTuple tuple;

    for (auto cid : _schema->column_ids()) {
        if (_schema->column(cid) != nullptr) {
            const Field* field = column_schema(cid);
            char* src = cell_ptr(cid);
            if (is_null(cid)) {
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

std::string RowCursor::to_string() const {
    std::string result;
    size_t i = 0;
    for (auto cid : _schema->column_ids()) {
        if (i++ > 0) {
            result.append("|");
        }

        const Field* field = column_schema(cid);
        result.append(std::to_string(is_null(cid)));
        result.append("&");
        if (is_null(cid)) {
            result.append("NULL");
        } else {
            char* src = cell_ptr(cid);
            result.append(field->to_string(src));
        }
    }

    return result;
}
Status RowCursor::_alloc_buf() {
    // variable_len for null bytes
    _variable_buf = new (nothrow) char[_variable_len]();
    if (_variable_buf == nullptr) {
        LOG(WARNING) << "Fail to malloc _variable_buf.";
        return Status::Error<MEM_ALLOC_FAILED>();
    }
    if (_string_field_count > 0) {
        _long_text_buf = (char**)malloc(_string_field_count * sizeof(char*));
        if (_long_text_buf == nullptr) {
            LOG(WARNING) << "Fail to malloc _long_text_buf.";
            return Status::Error<MEM_ALLOC_FAILED>();
        }
        for (int i = 0; i < _string_field_count; ++i) {
            _long_text_buf[i] = (char*)malloc(DEFAULT_TEXT_LENGTH * sizeof(char));
            if (_long_text_buf[i] == nullptr) {
                LOG(WARNING) << "Fail to malloc _long_text_buf.";
                return Status::Error<MEM_ALLOC_FAILED>();
            }
        }
    }
    return Status::OK();
}

} // namespace doris
