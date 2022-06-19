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

#ifndef DORIS_BE_RUNTIME_JSON_VALUE_H
#define DORIS_BE_RUNTIME_JSON_VALUE_H

#include "udf/udf.h"
#include "util/cpu_info.h"
#include "util/hash_util.hpp"
#include "util/jsonb_parser.h"
#include "util/jsonb_utils.h"
#include "util/jsonb_error.h"
#include "vec/common/string_ref.h"

#ifdef __SSE4_2__
#include "util/sse_util.hpp"
#endif

namespace doris {

struct JsonValue {
    static const int MAX_LENGTH = (1 << 30);

    const char* ptr;
    size_t len;
    JsonbParser parser;

    JsonValue() : ptr(nullptr), len(0) {}

    JsonValue(char* ptr, int len) {
        DCHECK_LE(len, MAX_LENGTH);
        DCHECK(parser.parse(const_cast<const char*>(ptr), len));
        this->ptr = parser.getWriter().getOutput()->getBuffer();
        this->len = (unsigned)parser.getWriter().getOutput()->getSize();
    }

    JsonValue(const char* ptr, int len) {
        DCHECK_LE(len, MAX_LENGTH);
        DCHECK(parser.parse(ptr, len));
        this->ptr = parser.getWriter().getOutput()->getBuffer();
        this->len = (unsigned)parser.getWriter().getOutput()->getSize();
    }

    /// Construct a JsonValue from 's'.
    // also check if 's' is valid json
    explicit JsonValue(const std::string& s) { from_json_str(s); }

    const char* value() { return ptr; }

    size_t size() { return len; }

    void replace(char* ptr, int len) {
        this->ptr = ptr;
        this->len = len;
    }

    bool operator==(const JsonValue& other) const {
        LOG(FATAL) << "comparing between JsonValue is not supported";
    }
    // !=
    bool ne(const JsonValue& other) const {
        LOG(FATAL) << "comparing between JsonValue is not supported";
    }
    // <=
    bool le(const JsonValue& other) const {
        LOG(FATAL) << "comparing between JsonValue is not supported";
    }
    // >=
    bool ge(const JsonValue& other) const {
        LOG(FATAL) << "comparing between JsonValue is not supported";
    }
    // <
    bool lt(const JsonValue& other) const {
        LOG(FATAL) << "comparing between JsonValue is not supported";
    }
    // >
    bool gt(const JsonValue& other) const {
        LOG(FATAL) << "comparing between JsonValue is not supported";
    }

    bool operator!=(const JsonValue& other) const {
        LOG(FATAL) << "comparing between JsonValue is not supported";
    }

    bool operator<=(const JsonValue& other) const {
        LOG(FATAL) << "comparing between JsonValue is not supported";
    }

    bool operator>=(const JsonValue& other) const {
        LOG(FATAL) << "comparing between JsonValue is not supported";
    }

    bool operator<(const JsonValue& other) const {
        LOG(FATAL) << "comparing between JsonValue is not supported";
    }

    bool operator>(const JsonValue& other) const {
        LOG(FATAL) << "comparing between JsonValue is not supported";
    }

    JsonbErrType from_json_str(std::string s);

    std::string to_string() const;

    struct HashOfJsonValue {
        size_t operator()(const JsonValue& v) const { return HashUtil::hash(v.ptr, v.len, 0); }
    };
};

// This function must be called 'hash_value' to be picked up by boost.
inline std::size_t hash_value(const JsonValue& v) {
    return HashUtil::hash(v.ptr, v.len, 0);
}

std::ostream& operator<<(std::ostream& os, const JsonValue& json_value);

std::size_t operator-(const JsonValue& v1, const JsonValue& v2);

} // namespace doris

#endif
