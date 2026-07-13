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
#include <memory>
#include <string>

#include "core/string_ref.h"
#include "util/variant/variant_metadata.h"
#include "util/variant/variant_value.h"

namespace doris {

class VariantBlockBuilder;
class VariantCollectionCore;
struct VariantEncodedBlockStorage;

// Owns the dictionary for one encoding unit. Multiple row builders may share this builder: all
// rows collect temporary key ids first, then seal() fixes the sorted dictionary and id remap.
// This object must outlive every row builder attached to it. seal() rejects incomplete live rows;
// completed rows destroyed or explicitly aborted before seal do not contribute dictionary keys.
class VariantMetadataBuilder {
public:
    VariantMetadataBuilder();
    ~VariantMetadataBuilder();

    VariantMetadataBuilder(const VariantMetadataBuilder&) = delete;
    VariantMetadataBuilder& operator=(const VariantMetadataBuilder&) = delete;
    VariantMetadataBuilder(VariantMetadataBuilder&&) = delete;
    VariantMetadataBuilder& operator=(VariantMetadataBuilder&&) = delete;

    // Interns a key and returns its temporary id. Interning alone does not retain the key:
    // seal() includes only keys referenced by completed, non-aborted row builders.
    uint32_t register_key(StringRef key);
    void seal();

    bool is_sealed() const noexcept;
    size_t num_keys() const noexcept;
    uint32_t final_id(uint32_t temporary_id) const;
    StringRef encoded_metadata() const;
    VariantMetadataRef metadata_ref() const;

private:
    friend class VariantBuilder;
    friend class VariantBlockBuilder;
    friend class VariantCollectionCore;

    void _begin_row();
    void _retain_key(uint32_t temporary_id) noexcept;
    void _complete_row() noexcept;
    void _abort_row(const uint32_t* temporary_ids, size_t count, bool was_collecting) noexcept;
    void _reserve_keys(size_t count);
    void _move_encoded_metadata(VariantEncodedBlockStorage& destination) noexcept;
    StringRef _temporary_key(uint32_t temporary_id) const noexcept;
    size_t _key_capacity() const noexcept;
    size_t _key_capacity_growths() const noexcept;

    struct Impl;
    std::unique_ptr<Impl> _impl;
};

// Legacy adapter that collects one row as a compact temporary tree against a caller-owned shared
// dictionary. New block producers should use VariantBlockBuilder instead.
class VariantBuilder {
public:
    class ObjectScope {
    public:
        ObjectScope(const ObjectScope&) = delete;
        ObjectScope& operator=(const ObjectScope&) = delete;
        ObjectScope(ObjectScope&& other) noexcept;
        ObjectScope& operator=(ObjectScope&& other) = delete;
        ~ObjectScope() = default;

        void add_key(StringRef key);
        void finish();

    private:
        friend class VariantBuilder;
        ObjectScope(VariantBuilder* builder, uint32_t token) noexcept;

        VariantBuilder* _builder;
        uint32_t _token;
    };

    class ArrayScope {
    public:
        ArrayScope(const ArrayScope&) = delete;
        ArrayScope& operator=(const ArrayScope&) = delete;
        ArrayScope(ArrayScope&& other) noexcept;
        ArrayScope& operator=(ArrayScope&& other) = delete;
        ~ArrayScope() = default;

        void finish();

    private:
        friend class VariantBuilder;
        ArrayScope(VariantBuilder* builder, uint32_t token) noexcept;

        VariantBuilder* _builder;
        uint32_t _token;
    };

    explicit VariantBuilder(VariantMetadataBuilder& metadata);
    ~VariantBuilder();

    VariantBuilder(const VariantBuilder&) = delete;
    VariantBuilder& operator=(const VariantBuilder&) = delete;
    VariantBuilder(VariantBuilder&&) = delete;
    VariantBuilder& operator=(VariantBuilder&&) = delete;

    void add_null();
    void add_bool(bool value);
    void add_int(int64_t value);
    void add_float(float value);
    void add_double(double value);
    void add_decimal(__int128 unscaled, uint8_t scale);
    // Encodes a decimal using the requested Parquet Variant physical width. Width must be 4, 8,
    // or 16 bytes and the unscaled value must fit that width's decimal precision.
    void add_decimal(__int128 unscaled, uint8_t scale, uint8_t width);
    void add_date(int32_t days_since_epoch);
    void add_timestamp_micros(int64_t value, bool utc_adjusted);
    void add_timestamp_nanos(int64_t value, bool utc_adjusted);
    void add_time_ntz_micros(int64_t value);
    void add_binary(StringRef value);
    void add_string(StringRef value);
    void add_uuid(const std::array<uint8_t, 16>& value);
    void add_largeint(__int128 value);
    // Canonicalizes one complete legal Variant value into this row. The input metadata/value may
    // use any legal non-canonical layout and only needs to remain alive for this call.
    void add_value(VariantValueRef value);

    ObjectScope start_object();
    ArrayScope start_array();

    void abort();

    // Appends the encoded row. The destination is unchanged when validation or encoding fails.
    void finish_row(std::string& destination);
    bool is_finished() const noexcept;

private:
    struct Impl;

    uint32_t _start_object();
    uint32_t _start_array();
    void _add_key(uint32_t token, StringRef key);
    void _finish_object(uint32_t token);
    void _finish_array(uint32_t token);
    void _complete_collection();
    void _abort_noexcept() noexcept;

    std::unique_ptr<Impl> _impl;
};

} // namespace doris
