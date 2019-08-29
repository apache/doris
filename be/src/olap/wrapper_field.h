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

#ifndef DORIS_BE_SRC_OLAP_WRAPPER_FIELD_H
#define DORIS_BE_SRC_OLAP_WRAPPER_FIELD_H

#include "olap/field.h"
#include "olap/olap_define.h"
#include "olap/tablet_schema.h"
#include "olap/row_cursor_cell.h"
#include "util/hash_util.hpp"

namespace doris {

class WrapperField {
public:
    static WrapperField* create(const TabletColumn& column, uint32_t len = 0);
    static WrapperField* create_by_type(const FieldType& type);

    WrapperField(Field* rep, size_t variable_len, bool is_string_type);

    virtual ~WrapperField() {
        delete _rep;
        delete [] _owned_buf;
    }

    // 将内部的value转成string输出
    // 没有考虑实现的性能，仅供DEBUG使用
    // do not include the null flag
    std::string to_string() const {
        return _rep->to_string(_field_buf + 1);
    }

    // 从传入的字符串反序列化field的值
    // 参数必须是一个\0结尾的字符串
    // do not include the null flag
    OLAPStatus from_string(const std::string& value_string) {
        if (_is_string_type) {
            if (value_string.size() > _var_length) {
                Slice* slice = reinterpret_cast<Slice*>(cell_ptr());
                slice->size = value_string.size();
                slice->data = _arena.Allocate(slice->size);
                memset(slice->data, 0, slice->size);
            }
        }
        return _rep->from_string(_field_buf + 1, value_string);
    }

    // attach到一段buf
    void attach_buf(char* buf) {
        _field_buf = _owned_buf;
        
        // set null byte
        *_field_buf = 0;
        memcpy(_field_buf + 1, buf, size());
    }

    void attach_field(char* field) {
        _field_buf = field;
    }

    bool is_string_type() const { return _is_string_type; }
    char* ptr() const { return _field_buf + 1; }
    size_t size() const { return _rep->size(); }
    size_t field_size() const { return _rep->field_size(); }
    bool is_null() const { return *reinterpret_cast<bool*>(_field_buf); }
    void set_is_null(bool is_null) { *reinterpret_cast<bool*>(_field_buf) = is_null; }
    void set_null() { *reinterpret_cast<bool*>(_field_buf) = true; }
    void set_not_null() { *reinterpret_cast<bool*>(_field_buf) = false; }
    char* nullable_cell_ptr() const { return _field_buf; }
    void set_to_max() { _rep->set_to_max(_field_buf + 1); }
    void set_to_min() { _rep->set_to_min(_field_buf + 1); }
    uint32_t hash_code() const { return _rep->hash_code(*this, 0); }
    void* cell_ptr() const { return _field_buf + 1; }
    void* mutable_cell_ptr() const { return _field_buf + 1; }
    const Field* field() const { return _rep; }

    int cmp(const WrapperField* field) const {
        return _rep->compare_cell(*this, *field);
    }

    void copy(const WrapperField* field) {
        _rep->direct_copy(this, *field);
    }

    void copy(const char* value) {
        set_is_null(false);
        _rep->deep_copy_content((char*)cell_ptr(), value, &_arena);
    }


private:
    Field* _rep = nullptr;
    bool _is_string_type;
    char* _field_buf = nullptr;
    char* _owned_buf = nullptr;

    //include fixed and variable length and null bytes
    size_t _length;
    size_t _var_length;
    Arena _arena;
};

}

#endif
