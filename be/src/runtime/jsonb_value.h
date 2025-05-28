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
#include <cstddef>
#include <string>

#include "common/status.h"
#include "util/jsonb_parser_simd.h"
#include "util/jsonb_utils.h"

namespace doris {
#include "common/compile_check_begin.h"

// JsonBinaryValue wraps a Doris jsonb object.
// The jsonb object is written using JsonbWriter.
// JsonBinaryValue is non-movable and non-copyable; it is only a simple wrapper.
// To parse a string to a jsonb object, use it like this:
//     JsonBinaryValue jsonb_value;
//     RETURN_IF_ERROR(jsonb_value.from_json_string(slice.data, slice.size));
//     insert_data(jsonb_value.value(), jsonb_value.size());
// insert_data should use copy semantics.
//
// from_json_string can be called multiple times.
// Example:
//     JsonBinaryValue jsonb_value;
//     for (;;) {
//         RETURN_IF_ERROR(jsonb_value.from_json_string(slice.data, slice.size));
//         insert_data(jsonb_value.value(), jsonb_value.size());
//     }

struct JsonBinaryValue final {
    static constexpr int MAX_LENGTH = (1 << 30);

    JsonBinaryValue() = default;

    JsonBinaryValue(const JsonBinaryValue&) = delete;
    JsonBinaryValue& operator=(const JsonBinaryValue&) = delete;
    JsonBinaryValue(JsonBinaryValue&&) = delete;
    JsonBinaryValue& operator=(JsonBinaryValue&&) = delete;

    /// TODO: The constructor here calls from_json_string and ignores its return value.
    // If an error occurs, ptr = nullptr and len = 0.
    // Previously, since the column was nullable, insert_data would handle ptr being null like this:
    //     /// Will insert null value if pos=nullptr
    //     void insert_data(const char* pos, size_t length) override;
    // However, this approach should not be used.
    JsonBinaryValue(char* ptr, size_t len) {
        static_cast<void>(from_json_string(const_cast<const char*>(ptr), len));
    }
    JsonBinaryValue(const std::string& s) {
        static_cast<void>(from_json_string(s.c_str(), s.length()));
    }
    JsonBinaryValue(const char* ptr, int len) { static_cast<void>(from_json_string(ptr, len)); }

    const char* value() const { return ptr; }

    size_t size() const { return len; }

    Status from_json_string(const char* s, size_t length) {
        // reset all fields
        ptr = nullptr;
        len = 0;
        writer.reset();
        RETURN_IF_ERROR(JsonbParser::parse(s, length, writer));
        ptr = writer.getOutput()->getBuffer();
        len = writer.getOutput()->getSize();
        if (len > MAX_LENGTH) {
            return Status::InternalError(
                    "Jsonb value length {} exceeds maximum allowed length of {} bytes", len,
                    MAX_LENGTH);
        }
        return Status::OK();
    }

    std::string to_json_string() const { return JsonbToJson::jsonb_to_json_string(ptr, len); }

private:
    // default nullprt and size 0 for invalid or NULL value
    const char* ptr = nullptr;
    size_t len = 0;
    JsonbWriter writer;
};

#include "common/compile_check_end.h"
} // namespace doris
