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

#ifndef DORIS_GEOMETRY_VALUE_H
#define DORIS_GEOMETRY_VALUE_H


#include "geo/geo_tobinary.h"
#include "common/status.h"


namespace doris {
struct GeometryBinaryValue {
    static const int MAX_LENGTH = (1 << 30);

    // default nullprt and size 0 for invalid or NULL value
    const char* ptr = nullptr;
    size_t len = 0;
    std::stringstream result_stream;

    GeometryBinaryValue() : ptr(nullptr), len(0) {}
    GeometryBinaryValue(const std::string& s) { from_geometry_string(s.c_str(), s.length()); }
    GeometryBinaryValue(const char* ptr, int len) { from_geometry_string(ptr, len); }

    ~GeometryBinaryValue() {
        delete[] ptr;
    }

    const char* value() { return ptr; }

    size_t size() { return len; }

    void replace(char* ptr, int len) {
        this->ptr = ptr;
        this->len = len;
    }

    Status from_geometry_string(const char* s, int len);

    std::string to_geometry_string() const;

};
}


#endif //DORIS_GEOMETRY_VALUE_H
