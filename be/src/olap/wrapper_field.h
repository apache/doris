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
    std::string to_string() const {
        return _rep->to_string(_buf);
    }

    // 从传入的字符串反序列化field的值
    // 参数必须是一个\0结尾的字符串
    OLAPStatus from_string(const std::string& value_string) {
        return _rep->from_string(_buf, value_string);
    }

    // attach到一段buf
    void attach_buf(char* buf) {
        _field_buf = _owned_buf;
        _buf = _field_buf + 1;
        _is_null = _owned_buf;
        *_is_null = 0;
        memcpy(_field_buf + 1, buf, size());
    }

    void attach_field(char* field) {
        _field_buf = field;
        _is_null = _field_buf;
        _buf = _field_buf + 1;
    }

    bool is_string_type() const { return _is_string_type; }

    char* ptr() const {
        return _buf;
    }

    char* field_ptr() const {
        return _field_buf;
    }

    size_t size() const {
        return _rep->size();
    }

    size_t field_size() const {
        return _rep->field_size();
    }

    bool is_null() const {
        return *reinterpret_cast<bool*>(_is_null);
    }

    void set_null() {
        *reinterpret_cast<bool*>(_is_null) = true;
    }

    void set_not_null() {
        *reinterpret_cast<bool*>(_is_null) = false;
    }

    char* get_null() const {
        return _is_null;
    }

    void set_to_max() {
        _rep->set_to_max(_buf);
    }

    void set_to_min() {
        _rep->set_to_min(_buf);
    }

    bool is_min() {
        return _rep->is_min(_buf);
    }

    int cmp(const WrapperField* field) const {
        return _rep->cmp(_field_buf, field->field_ptr());
    }

    int cmp(char* right) const {
        return _rep->cmp(_field_buf, right);
    }

    int cmp(bool r_null, char* right) const {
        return _rep->cmp(_field_buf, r_null, right);
    }

    void copy(const WrapperField* field) {
        _rep->copy_without_pool(_field_buf, field->field_ptr());
    }

    void copy(char* src) {
        _rep->copy_without_pool(_field_buf, src);
    }

    void copy(bool is_null, char* src) {
        _rep->copy_without_pool(_field_buf, is_null, src);
    }

    uint32_t hash_code() const {
        uint32_t hash_code = 0;
        return _rep->hash_code(_buf + _rep->get_offset(), hash_code);
    }

private:
    Field* _rep = nullptr;
    bool _is_string_type;
    char* _field_buf = nullptr;
    char* _owned_buf = nullptr;
    char* _buf = nullptr;
    char* _is_null = nullptr;

    //include fixed and variable length and null bytes
    size_t _length;
};

}

#endif
