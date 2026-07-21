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
#include <span>

#include "core/pod_array.h"
#include "core/string_ref.h"
#include "core/value/variant/variant_metadata.h"
#include "core/value/variant/variant_value.h"

namespace doris {

// A validated, stack-only plan for one physical Variant scalar. The plan either owns a small
// inline textual fallback or borrows a StringRef until write() returns; it never allocates.
// Callers can therefore preflight a complete batch, reserve its final buffer once, and write
// directly into that buffer without constructing a temporary value tree per row.
class VariantScalarEncodingPlan {
public:
    static VariantScalarEncodingPlan null_value() noexcept;
    static VariantScalarEncodingPlan boolean(bool value) noexcept;
    // width == 0 selects the narrowest signed integer encoding. Otherwise width must be one of
    // 1, 2, 4, or 8 and value must fit that signed width.
    static VariantScalarEncodingPlan integer(int64_t value, uint8_t width = 0);
    static VariantScalarEncodingPlan float32(float value) noexcept;
    static VariantScalarEncodingPlan float64(double value) noexcept;
    // width == 0 selects the narrowest decimal width. Otherwise width must be 4, 8, or 16.
    static VariantScalarEncodingPlan decimal(__int128 unscaled, uint8_t scale, uint8_t width = 0);
    static VariantScalarEncodingPlan date(int32_t days_since_epoch) noexcept;
    static VariantScalarEncodingPlan timestamp_micros(int64_t value, bool utc_adjusted) noexcept;
    static VariantScalarEncodingPlan timestamp_nanos(int64_t value, bool utc_adjusted) noexcept;
    static VariantScalarEncodingPlan time_ntz_micros(int64_t value) noexcept;
    static VariantScalarEncodingPlan binary(StringRef value);
    static VariantScalarEncodingPlan string(StringRef value);
    static VariantScalarEncodingPlan uuid(const std::array<uint8_t, 16>& value) noexcept;
    // Values outside decimal38 are encoded as their canonical decimal string representation.
    static VariantScalarEncodingPlan largeint(__int128 value) noexcept;

    size_t size() const noexcept { return _encoded_size; }
    bool used_string_fallback() const noexcept { return _used_string_fallback; }

    // Writes exactly size() bytes. Invalid destination/capacity fails before modifying memory.
    void write(char* destination, size_t capacity) const;

private:
    VariantScalarEncodingPlan() = default;

    enum class PayloadKind : uint8_t {
        HEADER_ONLY,
        UNSIGNED,
        SIGNED,
        BORROWED_BYTES,
        UUID,
        INLINE_STRING,
    };

    uint64_t _unsigned_value = 0;
    __int128 _signed_value = 0;
    StringRef _borrowed;
    std::array<uint8_t, 16> _uuid {};
    std::array<char, 40> _inline_string {};
    size_t _encoded_size = 0;
    uint8_t _header = 0;
    uint8_t _payload_width = 0;
    uint8_t _scale = 0;
    uint8_t _inline_size = 0;
    PayloadKind _kind = PayloadKind::HEADER_ONLY;
    bool _has_scale = false;
    bool _has_length = false;
    bool _used_string_fallback = false;
};

class VariantCollectionCore;

// Internal dictionary owner for one VariantBatchBuilder encoding unit. Rows collect temporary key
// ids first; seal() fixes the sorted dictionary and id remap for the completed batch.
class VariantMetadataBuilder {
public:
    VariantMetadataBuilder();
    ~VariantMetadataBuilder();

    VariantMetadataBuilder(const VariantMetadataBuilder&) = delete;
    VariantMetadataBuilder& operator=(const VariantMetadataBuilder&) = delete;
    VariantMetadataBuilder(VariantMetadataBuilder&&) = delete;
    VariantMetadataBuilder& operator=(VariantMetadataBuilder&&) = delete;

    uint32_t register_key(StringRef key);
    void seal();

    bool is_sealed() const noexcept;
    size_t num_keys() const noexcept;
    uint32_t final_id(uint32_t temporary_id) const;
    StringRef encoded_metadata() const;
    VariantMetadataRef metadata_ref() const;

private:
    friend class VariantBatchBuilder;
    friend class VariantCollectionCore;

    void _begin_row();
    void _retain_key(uint32_t temporary_id) noexcept;
    void _complete_row() noexcept;
    void _abort_row(const uint32_t* temporary_ids, size_t count, bool was_collecting) noexcept;
    void _reserve_keys(size_t count);
    PaddedPODArray<char> _take_encoded_metadata() noexcept;
    StringRef _temporary_key(uint32_t temporary_id) const noexcept;
    size_t _key_capacity() const noexcept;
    size_t _key_capacity_growths() const noexcept;

    struct Impl;
    std::unique_ptr<Impl> _impl;
};

// Collects a batch through one stack-only active Row at a time. The implementation owns one
// metadata dictionary and one set of batch-level scalar/node/container/child/row-root arenas.
// The shared scalar/node/container/child scratch for the complete encoding unit stays within its
// uint32 index/offset domain. Row and its scopes never own per-row heap containers and must not
// outlive this builder.
class VariantBatchBuilder {
public:
    struct ReserveHint {
        size_t rows = 0;
        size_t metadata_keys = 0;
        size_t scalar_bytes = 0;
        size_t nodes = 0;
        size_t containers = 0;
        size_t children = 0;
    };

#ifdef BE_TEST
    struct TestCounters {
        size_t metadata_capacity_growths = 0;
        size_t scalar_capacity_growths = 0;
        size_t node_capacity_growths = 0;
        size_t container_capacity_growths = 0;
        size_t child_capacity_growths = 0;
        size_t scope_stack_capacity_growths = 0;
        size_t object_id_scratch_capacity_growths = 0;
        size_t key_reference_capacity_growths = 0;
        size_t container_plan_capacity_growths = 0;
        size_t planned_object_child_capacity_growths = 0;
        size_t row_root_capacity_growths = 0;
        size_t object_token_capacity_growths = 0;
        size_t object_schema_hits = 0;
        size_t object_schema_fallbacks = 0;
        size_t object_plan_reuses = 0;
        size_t object_plan_fallbacks = 0;
        size_t metadata_unique_keys = 0;
        size_t metadata_key_capacity = 0;
        size_t scalar_byte_capacity = 0;
        size_t node_capacity = 0;
        size_t container_capacity = 0;
        size_t child_capacity = 0;
        size_t scope_stack_capacity = 0;
        size_t object_id_scratch_capacity = 0;
        size_t key_reference_capacity = 0;
        size_t container_plan_capacity = 0;
        size_t planned_object_child_capacity = 0;
        size_t row_root_capacity = 0;
        size_t previous_object_token_capacity = 0;
        size_t pending_object_token_capacity = 0;

        size_t total_capacity_growths() const noexcept {
            return metadata_capacity_growths + scalar_capacity_growths + node_capacity_growths +
                   container_capacity_growths + child_capacity_growths +
                   scope_stack_capacity_growths + object_id_scratch_capacity_growths +
                   key_reference_capacity_growths + container_plan_capacity_growths +
                   planned_object_child_capacity_growths + row_root_capacity_growths +
                   object_token_capacity_growths;
        }
    };
#endif

    class Row {
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
            friend class Row;
            ObjectScope(VariantBatchBuilder* builder, uint64_t generation, uint32_t token) noexcept;

            VariantBatchBuilder* _builder;
            uint64_t _generation;
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
            friend class Row;
            ArrayScope(VariantBatchBuilder* builder, uint64_t generation, uint32_t token) noexcept;

            VariantBatchBuilder* _builder;
            uint64_t _generation;
            uint32_t _token;
        };

        Row(const Row&) = delete;
        Row& operator=(const Row&) = delete;
        Row(Row&& other) noexcept;
        Row& operator=(Row&& other) = delete;
        ~Row();

        void add_null();
        void add_bool(bool value);
        void add_int(int64_t value);
        void add_float(float value);
        void add_double(double value);
        void add_decimal(__int128 unscaled, uint8_t scale);
        void add_decimal(__int128 unscaled, uint8_t scale, uint8_t width);
        void add_date(int32_t days_since_epoch);
        void add_timestamp_micros(int64_t value, bool utc_adjusted);
        void add_timestamp_nanos(int64_t value, bool utc_adjusted);
        void add_time_ntz_micros(int64_t value);
        void add_binary(StringRef value);
        void add_string(StringRef value);
        void add_uuid(const std::array<uint8_t, 16>& value);
        void add_largeint(__int128 value);
        void add_scalar(const VariantScalarEncodingPlan& plan);
        void add_value(VariantRef value);

        ObjectScope start_object();
        ArrayScope start_array();

        void finish();
        void abort();
        bool is_finished() const noexcept;

    private:
        friend class VariantBatchBuilder;
        Row(VariantBatchBuilder* builder, uint64_t generation) noexcept;

        void _add_scalar(const VariantScalarEncodingPlan& plan);
        uint32_t _start_object();
        uint32_t _start_array();

        VariantBatchBuilder* _builder;
        uint64_t _generation;
    };

    VariantBatchBuilder();
    explicit VariantBatchBuilder(ReserveHint hint);
    ~VariantBatchBuilder();

    VariantBatchBuilder(const VariantBatchBuilder&) = delete;
    VariantBatchBuilder& operator=(const VariantBatchBuilder&) = delete;
    VariantBatchBuilder(VariantBatchBuilder&&) noexcept;
    VariantBatchBuilder& operator=(VariantBatchBuilder&&) noexcept;

    Row begin_row();
    VariantBatchBuilder finish_batch();

    size_t num_rows() const noexcept;
    VariantMetadataRef metadata_ref() const noexcept {
        return {.data = _metadata.data(), .size = _metadata.size()};
    }
    VariantRef value_at(size_t row) const;
    StringRef value_bytes() const noexcept { return {_values.data(), _values.size()}; }
    std::span<const uint32_t> value_offsets() const noexcept {
        return {_offsets.data(), _offsets.size()};
    }

#ifdef BE_TEST
    const TestCounters& test_counters() const noexcept;
#endif

private:
    struct Impl;

    VariantBatchBuilder(PaddedPODArray<char> metadata, PaddedPODArray<char> values,
                        PaddedPODArray<uint32_t> offsets) noexcept;

    void _add_scalar(uint64_t generation, const VariantScalarEncodingPlan& plan);
    void _add_null(uint64_t generation);
    void _add_bool(uint64_t generation, bool value);
    void _add_int(uint64_t generation, int64_t value);
    void _add_value(uint64_t generation, VariantRef value);
    uint32_t _start_container(uint64_t generation, bool object);
    void _add_key(uint64_t generation, uint32_t token, StringRef key);
    void _finish_container(uint64_t generation, uint32_t token, bool object);
    void _finish_row(uint64_t generation);
    void _abort_row(uint64_t generation);
    void _abort_row_noexcept(uint64_t generation) noexcept;

    std::unique_ptr<Impl> _impl;
    PaddedPODArray<char> _metadata;
    PaddedPODArray<char> _values;
    PaddedPODArray<uint32_t> _offsets;
};

} // namespace doris
