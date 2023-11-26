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

#include <stdint.h>

namespace doris {

/**
 * MapValue is for map type in memory
 */
class MapValue {
public:
    MapValue() = default;

    explicit MapValue(int32_t length) : _key_data(nullptr), _value_data(nullptr), _length(length) {}

    MapValue(void* k_data, void* v_data, int32_t length)
            : _key_data(k_data), _value_data(v_data), _length(length) {}

    int32_t size() const { return _length; }

    int32_t length() const { return _length; }

    void shallow_copy(const MapValue* other);

    const void* key_data() const { return _key_data; }
    void* mutable_key_data() const { return _key_data; }
    const void* value_data() const { return _value_data; }
    void* mutable_value_data() const { return _value_data; }

    void set_length(int32_t length) { _length = length; }
    void set_key(void* data) { _key_data = data; }
    void set_value(void* data) { _value_data = data; }

private:
    // child column data pointer
    void* _key_data = nullptr;
    void* _value_data = nullptr;
    // length for map size
    int32_t _length;

}; //map-value
} // namespace doris
