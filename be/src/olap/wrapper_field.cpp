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

namespace doris {

WrapperField* WrapperField::create(const FieldInfo& info, uint32_t len) {
    bool is_string_type =
        (info.type == OLAP_FIELD_TYPE_CHAR || info.type == OLAP_FIELD_TYPE_VARCHAR);
    if (is_string_type && len > OLAP_STRING_MAX_LENGTH) {
        OLAP_LOG_WARNING("length of string parameter is too long[len=%lu, max_len=%lu].",
                        len, OLAP_STRING_MAX_LENGTH);
        return nullptr;
    }

    Field* rep = Field::create(info);
    if (rep == nullptr) {
        return nullptr;
    }

    size_t variable_len = 0;
    if (info.type == OLAP_FIELD_TYPE_CHAR) {
        variable_len = std::max(len, info.length);
    } else if (info.type == OLAP_FIELD_TYPE_VARCHAR) {
        variable_len = std::max(len,
                static_cast<uint32_t>(info.length - sizeof(StringLengthType)));
    } else {
        variable_len = info.length;
    }

    WrapperField* wrapper = new WrapperField(rep, variable_len, is_string_type);
    return wrapper;
}

WrapperField* WrapperField::create_by_type(const FieldType& type) {
    Field* rep = Field::create_by_type(type);
    if (rep == nullptr) {
        return nullptr;
    }
    bool is_string_type = (type == OLAP_FIELD_TYPE_CHAR || type == OLAP_FIELD_TYPE_VARCHAR);
    WrapperField* wrapper = new WrapperField(rep, 0, is_string_type);
    return wrapper;
}

WrapperField::WrapperField(Field* rep, size_t variable_len, bool is_string_type)
        : _rep(rep), _is_string_type(is_string_type)  {
    size_t fixed_len = _rep->size();
    _length = fixed_len + variable_len + 1;
    _field_buf = new char[_length];
    memset(_field_buf, 0, _length);
    _owned_buf = _field_buf;
    _is_null = _field_buf;
    _buf = _field_buf + 1;

    if (_is_string_type) {
        Slice* slice = reinterpret_cast<Slice*>(_buf);
        slice->size = variable_len;
        slice->data = _buf + fixed_len;
    }
}

}
