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

#include "olap/wrapper_field.h"

#include <glog/logging.h>

#include <algorithm>
#include <cstring>
#include <ostream>

#include "common/config.h"
#include "common/status.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/row_cursor.h"
#include "util/expected.hpp"

namespace doris {

const size_t DEFAULT_STRING_LENGTH = 50;

Result<WrapperField*> WrapperField::create(const TabletColumn& column, uint32_t len) {
    bool is_string_type = (column.type() == FieldType::OLAP_FIELD_TYPE_CHAR ||
                           column.type() == FieldType::OLAP_FIELD_TYPE_VARCHAR ||
                           column.type() == FieldType::OLAP_FIELD_TYPE_HLL ||
                           column.type() == FieldType::OLAP_FIELD_TYPE_OBJECT ||
                           column.type() == FieldType::OLAP_FIELD_TYPE_STRING);
    size_t max_length = column.type() == FieldType::OLAP_FIELD_TYPE_STRING
                                ? config::string_type_length_soft_limit_bytes
                                : OLAP_VARCHAR_MAX_LENGTH;
    if (is_string_type && len > max_length) {
        LOG(WARNING) << "length of string parameter is too long[len=" << len
                     << ", max_len=" << max_length << "].";
        return unexpected {Status::Error<ErrorCode::EXCEEDED_LIMIT>(
                "length of string parameter is too long[len={}, max_len={}].", len, max_length)};
    }

    Field* rep = FieldFactory::create(column);
    if (rep == nullptr) {
        return unexpected {Status::Uninitialized("Unsupport field creation of {}", column.name())};
    }

    size_t variable_len = 0;
    if (column.type() == FieldType::OLAP_FIELD_TYPE_CHAR) {
        variable_len = std::max(len, (uint32_t)(column.length()));
    } else if (column.type() == FieldType::OLAP_FIELD_TYPE_VARCHAR ||
               column.type() == FieldType::OLAP_FIELD_TYPE_HLL) {
        // column.length is the serialized varchar length
        // the first sizeof(VarcharLengthType) bytes is the length of varchar
        // variable_len is the real length of varchar
        variable_len =
                std::max(len, static_cast<uint32_t>(column.length() - sizeof(VarcharLengthType)));
    } else if (column.type() == FieldType::OLAP_FIELD_TYPE_STRING) {
        variable_len = len;
    } else {
        variable_len = column.length();
    }
    return new WrapperField(rep, variable_len, is_string_type);
}

WrapperField* WrapperField::create_by_type(const FieldType& type, int32_t var_length) {
    Field* rep = FieldFactory::create_by_type(type);
    if (rep == nullptr) {
        return nullptr;
    }
    bool is_string_type =
            (type == FieldType::OLAP_FIELD_TYPE_CHAR ||
             type == FieldType::OLAP_FIELD_TYPE_VARCHAR || type == FieldType::OLAP_FIELD_TYPE_HLL ||
             type == FieldType::OLAP_FIELD_TYPE_OBJECT ||
             type == FieldType::OLAP_FIELD_TYPE_STRING ||
             type == FieldType::OLAP_FIELD_TYPE_QUANTILE_STATE);
    return new WrapperField(rep, var_length, is_string_type);
}

WrapperField::WrapperField(Field* rep, size_t variable_len, bool is_string_type)
        : _rep(rep), _is_string_type(is_string_type), _var_length(0) {
    size_t fixed_len = _rep->size();
    _length = fixed_len + 1;
    _field_buf = new char[_length];
    memset(_field_buf, 0, _length);
    _owned_buf = _field_buf;
    char* buf = _field_buf + 1;

    if (_is_string_type) {
        _var_length = variable_len > DEFAULT_STRING_LENGTH ? DEFAULT_STRING_LENGTH : variable_len;
        auto* slice = reinterpret_cast<Slice*>(buf);
        slice->size = _var_length;
        _string_content.reset(new char[slice->size]);
        slice->data = _string_content.get();
    }
    if (_rep->type() == FieldType::OLAP_FIELD_TYPE_STRING) {
        _long_text_buf = (char*)malloc(RowCursor::DEFAULT_TEXT_LENGTH * sizeof(char));
        rep->set_long_text_buf(&_long_text_buf);
    }
}
} // namespace doris
