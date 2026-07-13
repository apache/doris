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

#include <algorithm>
#include <limits>
#include <new>
#include <string_view>

#include "common/exception.h"
#include "core/column/column_const.h"
#include "core/data_type_serde/data_type_variant_v2_serde.h"
#include "core/data_type_serde/data_type_variant_v2_serde_binary_internal.h"
#include "util/variant/variant_field.h"

namespace doris::variant_v2_serde_binary_internal {

size_t checked_add(size_t left, size_t right, std::string_view description) {
    if (right > std::numeric_limits<size_t>::max() - left) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant V2 exchange {} exceeds size_t",
                        description);
    }
    return left + right;
}

size_t checked_multiply(size_t left, size_t right, std::string_view description) {
    if (left != 0 && right > std::numeric_limits<size_t>::max() / left) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant V2 exchange {} exceeds size_t",
                        description);
    }
    return left * right;
}

uint32_t checked_u32(size_t value, std::string_view description) {
    if (value > std::numeric_limits<uint32_t>::max()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant V2 exchange {} size {} exceeds uint32", description, value);
    }
    return static_cast<uint32_t>(value);
}

size_t checked_size(uint64_t value, std::string_view description) {
    if (value > std::numeric_limits<size_t>::max()) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Variant V2 exchange {} size {} exceeds addressable memory", description,
                        value);
    }
    return static_cast<size_t>(value);
}

namespace {

constexpr uint32_t UNMAPPED_METADATA = std::numeric_limits<uint32_t>::max();

struct EncodedPlan {
    uint32_t metadata_offsets_bytes = 0;
    uint32_t metadata_bytes = 0;
    uint32_t meta_ids_bytes = 0;
    uint32_t value_offsets_bytes = 0;
    uint32_t values_bytes = 0;
    size_t frame_bytes = 0;
    DorisVector<uint32_t> metadata_offsets;
    DorisVector<VariantMetadataRef> metadatas;
    DorisVector<uint32_t> meta_ids;
    DorisVector<uint32_t> value_offsets;
    DorisVector<VariantValueRef> values;
};

ResolvedColumn resolve_column(const IColumn& source) {
    const auto* constant = check_and_get_column<ColumnConst>(&source);
    const IColumn& physical = constant == nullptr ? source : constant->get_data_column();
    if (typeid(physical) != typeid(ColumnVariantV2)) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant V2 exchange requires an exact ColumnVariantV2 or ColumnConst "
                        "thereof");
    }
    const auto& variant = assert_cast<const ColumnVariantV2&>(physical);
    if (constant != nullptr && variant.size() != 1) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "constant Variant V2 source must contain exactly one physical row, found "
                        "{}",
                        variant.size());
    }
    if (constant == nullptr && variant.size() != source.size()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant V2 source logical size {} does not match physical size {}",
                        source.size(), variant.size());
    }
    return {.view = variant.read_view(),
            .logical_rows = source.size(),
            .is_constant = constant != nullptr};
}

EncodedPlan plan_encoded(const ResolvedColumn& source) {
    EncodedPlan plan;
    const size_t source_metadata_count = source.view.metadata_count();
    DorisVector<uint32_t> source_to_dense(source_metadata_count, UNMAPPED_METADATA);
    plan.metadata_offsets.push_back(0);
    plan.meta_ids.reserve(source.logical_rows);
    plan.value_offsets.push_back(0);
    plan.values.reserve(source.logical_rows);

    size_t metadata_bytes = 0;
    size_t values_bytes = 0;
    for (size_t row = 0; row < source.logical_rows; ++row) {
        const size_t physical_row = source.physical_row(row);
        const uint32_t source_id = source.view.metadata_id_at(physical_row);
        DORIS_CHECK_LT(source_id, source_to_dense.size());
        uint32_t dense_id = source_to_dense[source_id];
        if (dense_id == UNMAPPED_METADATA) {
            const VariantMetadataRef metadata = source.view.metadata_at(source_id);
            validate_variant_metadata(metadata);
            dense_id = checked_u32(plan.metadatas.size(), "metadata count");
            source_to_dense[source_id] = dense_id;
            plan.metadatas.push_back(metadata);
            metadata_bytes = checked_add(metadata_bytes, metadata.size, "metadata bytes");
            plan.metadata_offsets.push_back(checked_u32(metadata_bytes, "metadata bytes"));
        }
        plan.meta_ids.push_back(dense_id);

        const VariantValueRef value = source.view.value_at(physical_row);
        validate_variant_payload(value);
        plan.values.push_back(value);
        values_bytes = checked_add(values_bytes, value.size, "value bytes");
        plan.value_offsets.push_back(checked_u32(values_bytes, "value bytes"));
    }

    plan.metadata_offsets_bytes = checked_u32(
            checked_multiply(plan.metadata_offsets.size(), sizeof(uint32_t), "metadata offsets"),
            "metadata offsets");
    plan.metadata_bytes = checked_u32(metadata_bytes, "metadata bytes");
    plan.meta_ids_bytes =
            checked_u32(checked_multiply(plan.meta_ids.size(), sizeof(uint32_t), "metadata ids"),
                        "metadata ids");
    plan.value_offsets_bytes = checked_u32(
            checked_multiply(plan.value_offsets.size(), sizeof(uint32_t), "value offsets"),
            "value offsets");
    plan.values_bytes = checked_u32(values_bytes, "value bytes");

    size_t frame_bytes = HEADER_BYTES + ENCODED_DESCRIPTOR_BYTES;
    frame_bytes = checked_add(frame_bytes, plan.metadata_offsets_bytes, "encoded frame");
    frame_bytes = checked_add(frame_bytes, plan.metadata_bytes, "encoded frame");
    frame_bytes = checked_add(frame_bytes, plan.meta_ids_bytes, "encoded frame");
    frame_bytes = checked_add(frame_bytes, plan.value_offsets_bytes, "encoded frame");
    plan.frame_bytes = checked_add(frame_bytes, plan.values_bytes, "encoded frame");
    return plan;
}

void write_header(Writer& writer, State state, size_t frame_bytes, size_t row_count) {
    writer.raw(MAGIC);
    writer.u8(FORMAT_VERSION);
    writer.u8(static_cast<uint8_t>(state));
    writer.u16(0);
    writer.u64(frame_bytes);
    writer.u64(row_count);
}

void write_encoded(const ResolvedColumn& source, const EncodedPlan& plan, Writer& writer) {
    write_header(writer, State::ENCODED, plan.frame_bytes, source.logical_rows);
    writer.u32(plan.metadata_offsets_bytes);
    writer.u32(plan.metadata_bytes);
    writer.u32(plan.meta_ids_bytes);
    writer.u32(plan.value_offsets_bytes);
    writer.u32(plan.values_bytes);
    writer.u32(0);

    for (const uint32_t offset : plan.metadata_offsets) {
        writer.u32(offset);
    }
    for (const VariantMetadataRef metadata : plan.metadatas) {
        writer.raw({metadata.data, metadata.size});
    }
    for (const uint32_t id : plan.meta_ids) {
        writer.u32(id);
    }
    for (const uint32_t offset : plan.value_offsets) {
        writer.u32(offset);
    }
    for (const VariantValueRef value : plan.values) {
        writer.raw({value.data, value.size});
    }
}

DorisVector<uint32_t> read_offsets(std::span<const uint8_t> bytes, size_t expected_count,
                                   size_t payload_size, bool strictly_increasing,
                                   std::string_view description) {
    const size_t expected_bytes = checked_multiply(expected_count, sizeof(uint32_t), description);
    if (bytes.size() != expected_bytes) {
        throw Exception(ErrorCode::CORRUPTION, "Variant V2 exchange {} has {} bytes, expected {}",
                        description, bytes.size(), expected_bytes);
    }
    Reader reader(bytes);
    DorisVector<uint32_t> offsets;
    offsets.reserve(expected_count);
    for (size_t index = 0; index < expected_count; ++index) {
        offsets.push_back(reader.u32(description));
    }
    if (offsets.empty() || offsets.front() != 0) {
        throw Exception(ErrorCode::CORRUPTION, "Variant V2 exchange {} must start at zero",
                        description);
    }
    for (size_t index = 1; index < offsets.size(); ++index) {
        const bool invalid = strictly_increasing ? offsets[index] <= offsets[index - 1]
                                                 : offsets[index] < offsets[index - 1];
        if (invalid) {
            throw Exception(ErrorCode::CORRUPTION, "Variant V2 exchange {} are not {} at index {}",
                            description,
                            strictly_increasing ? "strictly increasing" : "nondecreasing", index);
        }
    }
    if (offsets.back() != payload_size) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Variant V2 exchange {} end {} does not match payload size {}", description,
                        offsets.back(), payload_size);
    }
    return offsets;
}

DorisVector<uint32_t> read_u32_values(std::span<const uint8_t> bytes,
                                      std::string_view description) {
    if (bytes.size() % sizeof(uint32_t) != 0) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Variant V2 exchange {} byte size {} is not a multiple of four",
                        description, bytes.size());
    }
    Reader reader(bytes);
    DorisVector<uint32_t> values;
    values.reserve(bytes.size() / sizeof(uint32_t));
    while (reader.remaining() != 0) {
        values.push_back(reader.u32(description));
    }
    return values;
}

ColumnVariantV2::MutablePtr decode_encoded(uint64_t row_count_u64,
                                           std::span<const uint8_t> metadata_offsets_bytes,
                                           std::span<const uint8_t> metadata_bytes,
                                           std::span<const uint8_t> meta_ids_bytes,
                                           std::span<const uint8_t> value_offsets_bytes,
                                           std::span<const uint8_t> value_bytes) {
    const size_t row_count = checked_size(row_count_u64, "row count");
    if (metadata_offsets_bytes.size() < sizeof(uint32_t) ||
        metadata_offsets_bytes.size() % sizeof(uint32_t) != 0) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Variant V2 exchange metadata offsets must contain at least [0]");
    }
    const size_t metadata_count = metadata_offsets_bytes.size() / sizeof(uint32_t) - 1;
    DorisVector<uint32_t> metadata_offsets =
            read_offsets(metadata_offsets_bytes, metadata_count + 1, metadata_bytes.size(), true,
                         "metadata offsets");
    const size_t expected_meta_ids = checked_multiply(row_count, sizeof(uint32_t), "metadata ids");
    if (meta_ids_bytes.size() != expected_meta_ids) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Variant V2 exchange metadata ids have {} bytes, expected {}",
                        meta_ids_bytes.size(), expected_meta_ids);
    }
    DorisVector<uint32_t> meta_ids = read_u32_values(meta_ids_bytes, "metadata ids");
    DorisVector<uint32_t> value_offsets =
            read_offsets(value_offsets_bytes, checked_add(row_count, 1, "value offset count"),
                         value_bytes.size(), true, "value offsets");

    DorisVector<VariantMetadataRef> metadatas;
    metadatas.reserve(metadata_count);
    for (size_t id = 0; id < metadata_count; ++id) {
        const uint32_t begin = metadata_offsets[id];
        const uint32_t end = metadata_offsets[id + 1];
        VariantMetadataRef metadata {
                .data = reinterpret_cast<const char*>(metadata_bytes.data() + begin),
                .size = end - begin};
        validate_variant_metadata(metadata);
        metadatas.push_back(metadata);
    }
    for (size_t row = 0; row < row_count; ++row) {
        if (meta_ids[row] >= metadata_count) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Variant V2 exchange metadata id {} at row {} exceeds count {}",
                            meta_ids[row], row, metadata_count);
        }
        const uint32_t begin = value_offsets[row];
        const uint32_t end = value_offsets[row + 1];
        validate_variant_payload({.metadata = metadatas[meta_ids[row]],
                                  .data = reinterpret_cast<const char*>(value_bytes.data() + begin),
                                  .size = end - begin});
    }

    auto result = ColumnVariantV2::create();
    result->insert_encoded_rows(
            {.metadata_bytes = {reinterpret_cast<const char*>(metadata_bytes.data()),
                                metadata_bytes.size()},
             .metadata_offsets = metadata_offsets,
             .meta_ids = meta_ids,
             .value_bytes = {reinterpret_cast<const char*>(value_bytes.data()), value_bytes.size()},
             .value_offsets = value_offsets});
    return result;
}

Status source_exception_status(const Exception& exception) {
    if (exception.code() == ErrorCode::MEM_ALLOC_FAILED) {
        return Status::MemoryLimitExceeded("Variant V2 exchange allocation failed: {}",
                                           exception.message());
    }
    return exception.to_status();
}

Status frame_exception_status(const Exception& exception) {
    if (exception.code() == ErrorCode::MEM_ALLOC_FAILED) {
        return Status::MemoryLimitExceeded("Variant V2 exchange allocation failed: {}",
                                           exception.message());
    }
    return Status::Corruption("Malformed Variant V2 exchange frame: {}", exception.message());
}

Status allocation_failure_status(const std::bad_alloc& exception) {
    return Status::MemoryLimitExceeded("Variant V2 exchange allocation failed: {}",
                                       exception.what());
}

struct ParsedHeader {
    State state;
    uint64_t row_count;
};

ParsedHeader read_header(Reader& reader, size_t actual_frame_bytes) {
    const std::span<const uint8_t> magic = reader.raw(MAGIC.size(), "magic");
    if (!std::ranges::equal(magic, MAGIC)) {
        throw Exception(ErrorCode::CORRUPTION, "Variant V2 exchange magic does not match DV2X");
    }
    const uint8_t version = reader.u8("version");
    if (version != FORMAT_VERSION) {
        throw Exception(ErrorCode::CORRUPTION, "Unsupported Variant V2 exchange format version {}",
                        version);
    }
    const uint8_t raw_state = reader.u8("state");
    if (raw_state > static_cast<uint8_t>(State::TYPED)) {
        throw Exception(ErrorCode::CORRUPTION, "Unknown Variant V2 exchange state {}", raw_state);
    }
    if (reader.u16("header flags") != 0) {
        throw Exception(ErrorCode::CORRUPTION, "Variant V2 exchange header flags must be zero");
    }
    const uint64_t declared_frame_bytes = reader.u64("frame size");
    if (declared_frame_bytes != actual_frame_bytes) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Variant V2 exchange frame size {} does not match exact input size {}",
                        declared_frame_bytes, actual_frame_bytes);
    }
    return {.state = static_cast<State>(raw_state), .row_count = reader.u64("row count")};
}

size_t serialized_size_impl(const ResolvedColumn& source) {
    return source.view.is_typed() ? plan_typed(source).frame_bytes
                                  : plan_encoded(source).frame_bytes;
}

void serialize_impl(const ResolvedColumn& source, std::span<uint8_t> exact_frame) {
    if (source.view.is_typed()) {
        const TypedPlan plan = plan_typed(source);
        if (exact_frame.size() != plan.frame_bytes) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Variant V2 exchange output has {} bytes, expected exactly {}",
                            exact_frame.size(), plan.frame_bytes);
        }
        Writer writer(exact_frame);
        write_header(writer, State::TYPED, plan.frame_bytes, source.logical_rows);
        write_typed(source, plan, writer);
        DCHECK_EQ(writer.position(), plan.frame_bytes);
        return;
    }
    const EncodedPlan plan = plan_encoded(source);
    if (exact_frame.size() != plan.frame_bytes) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant V2 exchange output has {} bytes, expected exactly {}",
                        exact_frame.size(), plan.frame_bytes);
    }
    Writer writer(exact_frame);
    write_encoded(source, plan, writer);
    DCHECK_EQ(writer.position(), plan.frame_bytes);
}

ColumnVariantV2::MutablePtr deserialize_impl(std::span<const uint8_t> exact_frame) {
    Reader reader(exact_frame);
    const ParsedHeader header = read_header(reader, exact_frame.size());
    if (header.state == State::ENCODED) {
        const uint32_t metadata_offsets_size = reader.u32("metadata offsets size");
        const uint32_t metadata_size = reader.u32("metadata size");
        const uint32_t meta_ids_size = reader.u32("metadata ids size");
        const uint32_t value_offsets_size = reader.u32("value offsets size");
        const uint32_t values_size = reader.u32("values size");
        if (reader.u32("encoded extension size") != 0) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Variant V2 encoded extension size must be zero");
        }
        const auto metadata_offsets = reader.raw(metadata_offsets_size, "metadata offsets");
        const auto metadata = reader.raw(metadata_size, "metadata");
        const auto meta_ids = reader.raw(meta_ids_size, "metadata ids");
        const auto value_offsets = reader.raw(value_offsets_size, "value offsets");
        const auto values = reader.raw(values_size, "values");
        if (reader.remaining() != 0) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Variant V2 encoded frame contains {} trailing bytes",
                            reader.remaining());
        }
        return decode_encoded(header.row_count, metadata_offsets, metadata, meta_ids, value_offsets,
                              values);
    }

    const uint32_t type_meta_size = reader.u32("type metadata size");
    const uint32_t extension_size = reader.u32("typed extension size");
    const uint32_t nullmap_size = reader.u32("null map size");
    const uint32_t value_offsets_size = reader.u32("value offsets size");
    const uint32_t values_size = reader.u32("values size");
    const auto type_meta = reader.raw(type_meta_size, "type metadata");
    const auto extension = reader.raw(extension_size, "typed extension");
    const auto nullmap = reader.raw(nullmap_size, "null map");
    const auto value_offsets = reader.raw(value_offsets_size, "value offsets");
    const auto values = reader.raw(values_size, "values");
    if (reader.remaining() != 0) {
        throw Exception(ErrorCode::CORRUPTION, "Variant V2 typed frame contains {} trailing bytes",
                        reader.remaining());
    }
    return decode_typed(header.row_count, type_meta, extension, nullmap, value_offsets, values);
}

} // namespace

} // namespace doris::variant_v2_serde_binary_internal

namespace doris {

Result<size_t> DataTypeVariantV2SerDe::serialized_size(const IColumn& source) {
    try {
        return variant_v2_serde_binary_internal::serialized_size_impl(
                variant_v2_serde_binary_internal::resolve_column(source));
    } catch (const Exception& exception) {
        return ResultError(variant_v2_serde_binary_internal::source_exception_status(exception));
    } catch (const std::bad_alloc& exception) {
        return ResultError(variant_v2_serde_binary_internal::allocation_failure_status(exception));
    }
}

Status DataTypeVariantV2SerDe::serialize_binary(const IColumn& source,
                                                std::span<uint8_t> exact_frame) {
    try {
        variant_v2_serde_binary_internal::serialize_impl(
                variant_v2_serde_binary_internal::resolve_column(source), exact_frame);
        return Status::OK();
    } catch (const Exception& exception) {
        return variant_v2_serde_binary_internal::source_exception_status(exception);
    } catch (const std::bad_alloc& exception) {
        return variant_v2_serde_binary_internal::allocation_failure_status(exception);
    }
}

// clang-tidy does not model mutable_ptr's overloaded assignment used to publish the result below.
Status DataTypeVariantV2SerDe::deserialize_binary(
        std::span<const uint8_t> exact_frame,
        MutableColumnPtr* destination) { // NOLINT(readability-non-const-parameter)
    if (destination == nullptr) {
        return Status::InvalidArgument("Variant V2 exchange destination must not be null");
    }
    try {
        ColumnVariantV2::MutablePtr decoded =
                variant_v2_serde_binary_internal::deserialize_impl(exact_frame);
        *destination = std::move(decoded);
        return Status::OK();
    } catch (const Exception& exception) {
        return variant_v2_serde_binary_internal::frame_exception_status(exception);
    } catch (const std::bad_alloc& exception) {
        return variant_v2_serde_binary_internal::allocation_failure_status(exception);
    }
}

} // namespace doris
