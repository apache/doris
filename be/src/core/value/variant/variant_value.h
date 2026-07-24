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

#include <array>
#include <cstddef>
#include <cstdint>

#include "core/string_ref.h"
#include "core/value/variant/variant_metadata.h"
#include "core/value/variant/variant_parquet_encoding.h"

namespace doris {

struct VariantRef {
    VariantMetadataRef metadata;
    StringRef value;

    VariantBasicType basic_type() const;
    VariantPrimitiveId primitive_id() const;
    size_t value_size() const;

    bool is_null() const;
    bool get_bool() const;
    int64_t get_int() const;
    float get_float() const;
    double get_double() const;
    VariantDecimal get_decimal() const;
    int32_t get_date() const;
    int64_t get_timestamp_micros() const;
    int64_t get_timestamp_ntz_micros() const;
    int64_t get_time_ntz_micros() const;
    int64_t get_timestamp_nanos() const;
    int64_t get_timestamp_ntz_nanos() const;
    StringRef get_binary() const;
    StringRef get_string() const;
    std::array<uint8_t, 16> get_uuid() const;

    uint32_t num_elements() const;
    bool object_find(StringRef key, VariantRef* out) const;
    bool object_find_by_id(uint32_t field_id, VariantRef* out) const;
    VariantRef object_value_at(uint32_t index, uint32_t* field_id_out) const;
    VariantRef array_at(uint32_t index) const;

private:
    struct ContainerLayout {
        uint32_t count;
        uint8_t offset_width;
        uint8_t id_width;
        size_t ids_offset;
        size_t offsets_offset;
        size_t values_offset;
        uint32_t values_size;
    };

    ContainerLayout _container_layout(VariantBasicType expected_type) const;
    uint32_t _object_field_id(const ContainerLayout& layout, uint32_t index) const;
    bool _object_find_by_id(const ContainerLayout& layout, uint32_t field_id,
                            VariantRef* out) const;
    VariantRef _container_value_at(const ContainerLayout& layout, uint32_t index,
                                   bool require_array_boundary) const;
};

} // namespace doris
