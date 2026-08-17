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

#include "core/custom_allocator.h"
#include "core/string_ref.h"
#include "core/value/variant/variant_metadata.h"
#include "core/value/variant/variant_parquet_encoding.h"

namespace doris {

class VariantContainerLookup;

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
    uint32_t _container_offset(const ContainerLayout& layout, uint32_t index) const;
    uint32_t _object_field_id(const ContainerLayout& layout, uint32_t index) const;
    bool _object_find_by_id(const ContainerLayout& layout, uint32_t field_id, VariantRef* out,
                            uint32_t* index_out = nullptr) const;
    VariantRef _container_value_at(const ContainerLayout& layout, uint32_t index,
                                   bool require_array_boundary) const;

    friend class VariantContainerLookup;
};

// A shallow, reusable lookup for a container whose recursive payload is still untrusted. Building
// it validates the exact container envelope, object key order, and all lookup-table offsets, but
// intentionally does not decode unrelated child payloads. Canonical objects use their monotonic
// offset table directly. Noncanonical objects initially scan that borrowed table for a selected
// boundary; a bounded caller may lazily retain a sorted offset index after this lookup is reused.
// The referenced Variant bytes and metadata must remain unchanged and outlive this object.
class VariantContainerLookup {
public:
    explicit VariantContainerLookup(VariantRef value);

    bool object_find_by_id(int64_t field_id, VariantRef* out, size_t maximum_offset_index_bytes = 0,
                           size_t* allocated_offset_index_bytes = nullptr);
    bool array_find(int64_t index, VariantRef* out) const;
    size_t allocated_bytes() const noexcept;

private:
    size_t _object_offset_index_bytes_required() const noexcept;
    size_t _promote_object_offset_index(size_t maximum_bytes);

    VariantRef _value;
    VariantBasicType _basic_type;
    uint32_t _container_count = 0;
    bool _object_offsets_in_field_order = false;
    DorisVector<uint32_t> _sorted_object_offsets;
};

} // namespace doris
