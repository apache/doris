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
struct Collection {
    // child column data
    void* data;
    uint32_t length;
    // item has no null value if has_null is false.
    // item ```may``` has null value if has_null is true.
    bool has_null;
    // null bitmap
    bool* null_signs;

    Collection() : data(nullptr), length(0), has_null(false), null_signs(nullptr) {}

    explicit Collection(uint32_t length)
            : data(nullptr), length(length), has_null(false), null_signs(nullptr) {}

    Collection(void* data, size_t length)
            : data(data), length(length), has_null(false), null_signs(nullptr) {}

    Collection(void* data, size_t length, bool* null_signs)
            : data(data), length(length), has_null(true), null_signs(null_signs) {}

    Collection(void* data, size_t length, bool has_null, bool* null_signs)
            : data(data), length(length), has_null(has_null), null_signs(null_signs) {}

    bool is_null_at(uint32_t index) { return this->has_null && this->null_signs[index]; }

    bool operator==(const Collection& y) const;
    bool operator!=(const Collection& value) const;
    bool operator<(const Collection& value) const;
    bool operator<=(const Collection& value) const;
    bool operator>(const Collection& value) const;
    bool operator>=(const Collection& value) const;
    int32_t cmp(const Collection& other) const;
};

} // namespace doris
