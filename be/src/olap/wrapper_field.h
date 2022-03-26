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
#include "olap/row_cursor_cell.h"
#include "olap/tablet_schema.h"
#include "util/hash_util.hpp"

namespace doris {

class WrapperField {
public:
    static WrapperField* create(const TabletColumn& column, uint32_t len = 0);
    static WrapperField* create_by_type(const FieldType& type) { return create_by_type(type, 0); }
    static WrapperField* create_by_type(const FieldType& type, int32_t var_length);

    WrapperField(Field* rep, size_t variable_len, bool is_string_type);

    // Only used to wrapped content of row cursor cell to find element in wrapped field set
    // do not delete rep, should call release_field before deconstructed.
    WrapperField(Field* rep, const RowCursorCell& row_cursor_cell);

    virtual ~WrapperField() {
        delete _rep;
        delete[] _owned_buf;
        if (_long_text_buf) {
            free(_long_text_buf);
        }
    }

    // Convert the internal value to string output.
    //
    // NOTE: it only for DEBUG use. Do not include the null flag.
    std::string to_string() const { return _rep->to_string(_field_buf + 1); }

    // Deserialize field value from incoming string.
    //
    // NOTE: the parameter must be a '\0' terminated string. It do not include the null flag.
    OLAPStatus from_string(const std::string& value_string) {
        if (_is_string_type) {
            if (value_string.size() > _var_length) {
                Slice* slice = reinterpret_cast<Slice*>(cell_ptr());
                slice->size = value_string.size();
                _var_length = slice->size;
                _string_content.reset(new char[slice->size]);
                slice->data = _string_content.get();
            }
        }
        return _rep->from_string(_field_buf + 1, value_string);
    }

    // Attach to a buf.
    void attach_buf(char* buf) {
        _field_buf = _owned_buf;

        // Set null byte.
        *_field_buf = 0;
        memcpy(_field_buf + 1, buf, size());
    }

    void attach_field(char* field) { _field_buf = field; }

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

    // Should be only called by 'WrapperField' which constructed by 'RowCursorCell'.
    void release_field() { _rep = nullptr; }

    int cmp(const WrapperField* field) const { return _rep->compare_cell(*this, *field); }

    void copy(const WrapperField* field) { _rep->direct_copy(this, *field); }

private:
    Field* _rep = nullptr;
    bool _is_string_type;
    char* _field_buf = nullptr;
    char* _owned_buf = nullptr;
    char* _long_text_buf = nullptr;

    // Include fixed and variable length and null bytes.
    size_t _length;
    size_t _var_length;
    // Memory for string type field.
    std::unique_ptr<char[]> _string_content;
};

} // namespace doris

#endif
