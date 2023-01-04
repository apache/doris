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

#include "map_value.h"

namespace doris {

///====================== map-value funcs ======================///
void MapValue::to_map_val(MapVal* val) const {
    val->length = _length;
    val->key = _key_data;
    val->value = _value_data;
    val->key_null_signs = _key_null_signs;
    val->value_null_signs = _val_null_signs;
}

void MapValue::shallow_copy(const MapValue* value) {
    _length = value->_length;
    _key_null_signs = value->_key_null_signs;
    _val_null_signs = value->_val_null_signs;
    _key_data = value->_key_data;
    _value_data = value->_value_data;
}

void MapValue::copy_null_signs(const MapValue* other) {
    // todo(amory): here need to judge?
    memcpy(_key_null_signs, other->_key_null_signs, other->size());
    memcpy(_val_null_signs, other->_val_null_signs, other->size());
}

MapValue MapValue::from_map_val(const MapVal& val) {
    return MapValue(val.key, val.value, val.length, val.key_null_signs, val.value_null_signs);
}


} // namespace doris
