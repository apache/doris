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

#include "common/status.h"
#include "geo/geo_tobinary.h"

namespace doris {
struct GeometryBinaryValue {
    static const int MAX_LENGTH = (1 << 30);

    // default nullprt and size 0 for invalid or NULL value
    const char* ptr = nullptr;
    size_t len = 0;
    std::stringstream result_stream;

    GeometryBinaryValue() : ptr(nullptr), len(0) {}
    GeometryBinaryValue(const std::string& s) { from_geometry_string(s.c_str(), s.length()); }
    GeometryBinaryValue(const char* ptr, size_t len) { from_geometry_string(ptr, len); }

    ~GeometryBinaryValue() { delete[] ptr; }

    const char* value() const { return ptr; }

    size_t size() const { return len; }

    void replace(const char* ptr_a, int len_a) {
        this->ptr = ptr_a;
        this->len = len_a;
    }

    Status from_geometry_string(const char* s, size_t len);

    std::string to_geometry_string() const;
};
} // namespace doris
