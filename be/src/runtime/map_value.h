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

#pragma once

#include <type_traits>
#include "runtime/primitive_type.h"

namespace doris_udf {
class FunctionContext;
struct AnyVal;
} // namespace doris_udf

namespace doris {

using doris_udf::FunctionContext;
using doris_udf::AnyVal;

/**
 * MapValue is for map type in memory
 */
class MapValue {
public:
    MapValue() = default;

    explicit MapValue(int32_t length)
            : _key_data(nullptr), _value_data(nullptr), _length(length){}

    MapValue(void* k_data, void* v_data, int32_t length)
            : _key_data(k_data), _value_data(v_data), _length(length) {}

    MapValue(void* k_data, void* v_data, int32_t length, bool* _null_signs, bool is_key_null_signs)
            : _key_data(k_data), _value_data(v_data), _length(length) {
        if (is_key_null_signs) {
            _key_null_signs = _null_signs;
        } else {
            _val_null_signs = _null_signs;
        }
    }

    MapValue(void* k_data, void* v_data, int32_t length, bool* key_null_signs, bool* value_null_signs)
            : _key_data(k_data), _value_data(v_data), _length(length), _key_null_signs(key_null_signs), _val_null_signs(value_null_signs) {}


    void set_key_has_null(bool has_null) { _key_has_null = has_null; }
    void set_val_has_null(bool has_null) { _val_has_null = has_null; }
    bool is_key_null_at(int32_t index) const { return this->_key_has_null && this->_key_null_signs[index]; }
    bool is_val_null_at(int32_t index) const { return this->_val_has_null && this->_val_null_signs[index]; }

    void to_map_val(MapVal* val) const;

    int32_t size() const { return _length; }

    int32_t length() const { return _length; }

    void shallow_copy(const MapValue* other);

    void copy_null_signs(const MapValue* other);

    static MapValue from_map_val(const MapVal& val);

    const void* key_data() const { return _key_data; }
    void* mutable_key_data() const { return _key_data; }
    const void* value_data() const { return _value_data; }
    void* mutable_value_data() const { return _value_data; }
    const bool* key_null_signs() const { return _key_null_signs; }
    const bool* value_null_signs() const { return _val_null_signs; }
    void set_key_null_signs(bool* null_signs) { _key_null_signs = null_signs; }
    void set_value_null_signs(bool* null_signs) { _val_null_signs = null_signs; }
    void set_length(int32_t length) { _length = length; }
    void set_key(void* data) { _key_data = data; }
    void set_value(void* data) { _value_data = data; }

private:
    // child column data
    void* _key_data;
    void* _value_data;
    int32_t _length;
    bool _key_has_null;
    bool _val_has_null;
    bool* _key_null_signs;
    bool* _val_null_signs;

};//map-value
} // namespace doris
