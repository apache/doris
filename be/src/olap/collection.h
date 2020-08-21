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

#include <iostream>

namespace doris {

// cpp type for ARRAY
struct CollectionValue {
    size_t length;
        // null bitmap
    bool* null_signs;
    // child column data
    void* data;

    CollectionValue(): length(0), null_signs(nullptr), data(nullptr) {}

    explicit CollectionValue(size_t length) : length(length), null_signs(nullptr), data(nullptr) {}

    CollectionValue(void* data, size_t length ) : length(length), null_signs(nullptr), data(data) {}

    CollectionValue(void* data, size_t length, bool* null_signs)
    : length(length), null_signs(null_signs), data(data) {}

    bool operator==(const CollectionValue& y) const;
    bool operator!=(const CollectionValue& value) const;
    bool operator<(const CollectionValue& value) const;
    bool operator<=(const CollectionValue& value) const;
    bool operator>(const CollectionValue& value) const;
    bool operator>=(const CollectionValue& value) const;
    int32_t cmp(const CollectionValue& other) const;
};

}  // namespace doris