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

#include "runtime/collection_value.h"

#include <string.h>

namespace doris {

void CollectionValue::shallow_copy(const CollectionValue* value) {
    _length = value->_length;
    _null_signs = value->_null_signs;
    _data = value->_data;
    _has_null = value->_has_null;
}

void CollectionValue::copy_null_signs(const CollectionValue* other) {
    if (other->_has_null) {
        memcpy(_null_signs, other->_null_signs, other->size());
    } else {
        _null_signs = nullptr;
    }
}

} // namespace doris
