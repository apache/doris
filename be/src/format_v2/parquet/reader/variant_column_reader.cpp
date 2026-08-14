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

#include "format_v2/parquet/reader/variant_column_reader.h"

#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <mutex>
#include <optional>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/column/column_array.h"
#include "core/column/column_decimal.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/column/variant_v2/column_variant_v2_typed_column.h"
#include "core/custom_allocator.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_variant_v2.h"
#include "core/value/variant/variant_batch_builder.h"
#include "core/value/variant/variant_field.h"
#include "core/value/variant/variant_metadata.h"
#include "format_v2/parquet/parquet_column_schema.h"

namespace doris::format::parquet {
namespace {

struct Cell {
    const IColumn* column = nullptr;
    bool is_null = false;
};

constexpr std::array<char, 1> VARIANT_NULL_VALUE {static_cast<char>(
        static_cast<uint8_t>(VariantPrimitiveId::NULL_VALUE) << VARIANT_VALUE_HEADER_SHIFT)};

Cell cell_at(const IColumn& column, size_t row) {
    if (row >= column.size()) {
        throw Exception(ErrorCode::CORRUPTION, "Parquet Variant row {} exceeds column size {}", row,
                        column.size());
    }
    if (const auto* nullable = check_and_get_column<ColumnNullable>(column)) {
        return {.column = &nullable->get_nested_column(),
                .is_null = nullable->get_null_map_data()[row] != 0};
    }
    return {.column = &column, .is_null = false};
}

const ParquetColumnSchema* find_child(const ParquetColumnSchema& schema, std::string_view name,
                                      size_t* index) {
    for (size_t i = 0; i < schema.children.size(); ++i) {
        if (schema.children[i]->name == name) {
            if (index != nullptr) {
                *index = i;
            }
            return schema.children[i].get();
        }
    }
    return nullptr;
}

Cell struct_child_at(const ParquetColumnSchema& schema, const IColumn& physical, size_t row,
                     std::string_view name, const ParquetColumnSchema** child_schema) {
    const auto& structure = assert_cast<const ColumnStruct&>(physical);
    size_t index = 0;
    const auto* child = find_child(schema, name, &index);
    if (child == nullptr || index >= structure.tuple_size()) {
        throw Exception(ErrorCode::CORRUPTION, "Parquet Variant {} has no physical child {}",
                        schema.name, name);
    }
    if (child_schema != nullptr) {
        *child_schema = child;
    }
    return cell_at(structure.get_column(index), row);
}

uint8_t decimal_width(int precision) {
    if (precision <= 0 || precision > 38) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Parquet Variant decimal precision {} is outside [1, 38]", precision);
    }
    return precision <= 9 ? 4 : (precision <= 18 ? 8 : 16);
}

uint8_t integer_width(const ParquetColumnSchema& schema, PrimitiveType type) {
    if (schema.type_descriptor.is_unsigned_integer) {
        throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                        "Unsigned integers are not valid Parquet Variant typed values");
    }
    if (schema.type_descriptor.integer_bit_width > 0) {
        switch (schema.type_descriptor.integer_bit_width) {
        case 8:
            return 1;
        case 16:
            return 2;
        case 32:
            return 4;
        case 64:
            return 8;
        default:
            throw Exception(ErrorCode::CORRUPTION, "Invalid Parquet Variant integer width {}",
                            schema.type_descriptor.integer_bit_width);
        }
    }
    switch (type) {
    case TYPE_TINYINT:
        return 1;
    case TYPE_SMALLINT:
        return 2;
    case TYPE_INT:
        return 4;
    case TYPE_BIGINT:
        return 8;
    default:
        throw Exception(ErrorCode::CORRUPTION, "Invalid Parquet Variant integer type {}", type);
    }
}

void append_typed_scalar(const ParquetColumnSchema& schema, const IColumn& column, size_t row,
                         VariantBatchBuilder::Row& builder) {
    const PrimitiveType type = remove_nullable(schema.type)->get_primitive_type();
    switch (type) {
    case TYPE_BOOLEAN:
        builder.add_bool(assert_cast<const ColumnUInt8&>(column).get_data()[row] != 0);
        return;
    case TYPE_TINYINT:
        builder.add_scalar(
                VariantScalarRef::integer(assert_cast<const ColumnInt8&>(column).get_data()[row],
                                          integer_width(schema, type)));
        return;
    case TYPE_SMALLINT:
        builder.add_scalar(
                VariantScalarRef::integer(assert_cast<const ColumnInt16&>(column).get_data()[row],
                                          integer_width(schema, type)));
        return;
    case TYPE_INT:
        builder.add_scalar(
                VariantScalarRef::integer(assert_cast<const ColumnInt32&>(column).get_data()[row],
                                          integer_width(schema, type)));
        return;
    case TYPE_BIGINT:
        builder.add_scalar(
                VariantScalarRef::integer(assert_cast<const ColumnInt64&>(column).get_data()[row],
                                          integer_width(schema, type)));
        return;
    case TYPE_FLOAT:
        builder.add_float(assert_cast<const ColumnFloat32&>(column).get_data()[row]);
        return;
    case TYPE_DOUBLE:
        builder.add_double(assert_cast<const ColumnFloat64&>(column).get_data()[row]);
        return;
    case TYPE_DECIMAL128I: {
        const auto value = assert_cast<const ColumnDecimal128V3&>(column).get_data()[row].value;
        builder.add_decimal(value, static_cast<uint8_t>(schema.type_descriptor.decimal_scale),
                            decimal_width(schema.type_descriptor.decimal_precision));
        return;
    }
    case TYPE_TIMEV2: {
        const double seconds = assert_cast<const ColumnTimeV2&>(column).get_data()[row];
        if (!std::isfinite(seconds) ||
            std::abs(seconds) > static_cast<double>(std::numeric_limits<int64_t>::max()) / 1e6) {
            throw Exception(ErrorCode::CORRUPTION, "Invalid Parquet Variant TIME value");
        }
        builder.add_time_ntz_micros(static_cast<int64_t>(std::llround(seconds * 1e6)));
        return;
    }
    case TYPE_DATETIMEV2: {
        if (schema.type_descriptor.time_unit == ParquetTimeUnit::NANOS) {
            // Native DATETIMEV2 is microsecond based. Reject before returning a silently truncated
            // value; a raw INT64 nanos decoder can be added independently.
            throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                            "Parquet Variant TIMESTAMP(NANOS) is not supported");
        }
        const auto& value = assert_cast<const ColumnDateTimeV2&>(column).get_data()[row];
        builder.add_timestamp_micros(
                variant_timestamp_micros(value, row, "Parquet Variant TIMESTAMP"),
                schema.type_descriptor.timestamp_is_adjusted_to_utc);
        return;
    }
    case TYPE_TIMESTAMPTZ: {
        if (schema.type_descriptor.time_unit == ParquetTimeUnit::NANOS) {
            throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                            "Parquet Variant TIMESTAMP(NANOS) is not supported");
        }
        const auto& value = assert_cast<const ColumnTimeStampTz&>(column).get_data()[row];
        builder.add_timestamp_micros(
                variant_timestamp_micros(value, row, "Parquet Variant TIMESTAMP"), true);
        return;
    }
    case TYPE_VARBINARY: {
        const StringRef value = column.get_data_at(row);
        if (!schema.type_descriptor.is_uuid) {
            builder.add_binary(value);
            return;
        }
        if (value.size != 16) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Parquet Variant UUID has {} bytes instead of 16", value.size);
        }
        std::array<uint8_t, 16> uuid {};
        std::memcpy(uuid.data(), value.data, uuid.size());
        builder.add_uuid(uuid);
        return;
    }
    case TYPE_STRING: {
        const StringRef value = column.get_data_at(row);
        if (schema.type_descriptor.is_uuid) {
            if (value.size != 16) {
                throw Exception(ErrorCode::CORRUPTION,
                                "Parquet Variant UUID has {} bytes instead of 16", value.size);
            }
            std::array<uint8_t, 16> uuid {};
            std::memcpy(uuid.data(), value.data, uuid.size());
            builder.add_uuid(uuid);
        } else if (schema.type_descriptor.is_string_annotation) {
            builder.add_string(value);
        } else {
            builder.add_binary(value);
        }
        return;
    }
    default:
        if (!is_supported_variant_typed_identity(type)) {
            throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                            "Parquet Variant typed value {} is not supported",
                            remove_nullable(schema.type)->get_name());
        }
        dispatch_variant_typed_column(
                column, type, [&]<PrimitiveType Type>(const auto& typed_column) {
                    with_variant_typed_scalar<Type>(
                            typed_column, row,
                            static_cast<uint8_t>(remove_nullable(schema.type)->get_scale()),
                            [&](const VariantScalarRef& scalar) { builder.add_scalar(scalar); });
                });
    }
}

enum class WrapperContext { ROOT, ARRAY_ELEMENT, OBJECT_FIELD };

bool append_wrapper(const ParquetColumnSchema& schema, const IColumn& wrapper, size_t row,
                    VariantMetadataRef metadata, VariantBatchBuilder::Row& builder,
                    WrapperContext context);

void append_typed_value(const ParquetColumnSchema& schema, const IColumn& column, size_t row,
                        VariantMetadataRef metadata, const VariantRef* residual,
                        VariantBatchBuilder::Row& builder) {
    switch (schema.kind) {
    case ParquetColumnSchemaKind::PRIMITIVE:
        if (static_cast<bool>(residual)) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Parquet Variant scalar typed_value cannot have residual value bytes");
        }
        append_typed_scalar(schema, column, row, builder);
        return;
    case ParquetColumnSchemaKind::STRUCT: {
        if (static_cast<bool>(residual) && residual->basic_type() != VariantBasicType::OBJECT) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Parquet Variant object typed_value has non-object residual value");
        }
        const auto& structure = assert_cast<const ColumnStruct&>(column);
        if (structure.tuple_size() != schema.children.size()) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Parquet Variant object {} physical field count mismatch", schema.name);
        }
        auto object = builder.start_object();
        if (static_cast<bool>(residual)) {
            for (uint32_t i = 0; i < residual->num_elements(); ++i) {
                uint32_t field_id = 0;
                const VariantRef child = residual->object_value_at(i, &field_id);
                object.add_key(residual->metadata.key_at(field_id));
                builder.add_value(child);
            }
        }
        for (size_t i = 0; i < schema.children.size(); ++i) {
            const auto& child_schema = *schema.children[i];
            const Cell child = cell_at(structure.get_column(i), row);
            if (child.is_null) {
                // Shredded object fields are optional wrapper groups. A missing group means the
                // key is absent, which differs from a present wrapper encoding a Variant null.
                continue;
            }
            // A null/null wrapper means this object field is absent. Delay add_key until its
            // presence is known so absent shredded fields do not turn into Variant nulls.
            size_t value_index = 0;
            const auto* value_schema = find_child(child_schema, "value", &value_index);
            const auto& child_struct = assert_cast<const ColumnStruct&>(*child.column);
            const bool value_present = value_schema != nullptr &&
                                       !cell_at(child_struct.get_column(value_index), row).is_null;
            size_t typed_index = 0;
            const auto* typed_schema = find_child(child_schema, "typed_value", &typed_index);
            const bool typed_present = typed_schema != nullptr &&
                                       !cell_at(child_struct.get_column(typed_index), row).is_null;
            if (!value_present && !typed_present) {
                continue;
            }
            object.add_key(StringRef(child_schema.name));
            (void)append_wrapper(child_schema, *child.column, row, metadata, builder,
                                 WrapperContext::OBJECT_FIELD);
        }
        object.finish();
        return;
    }
    case ParquetColumnSchemaKind::LIST: {
        if (static_cast<bool>(residual)) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Parquet Variant array typed_value cannot have residual value bytes");
        }
        if (schema.children.size() != 1) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Parquet Variant array {} has invalid element schema", schema.name);
        }
        const auto& array = assert_cast<const ColumnArray&>(column);
        const size_t begin = array.offset_at(static_cast<ssize_t>(row));
        const size_t end = array.get_offsets()[row];
        auto scope = builder.start_array();
        for (size_t element = begin; element < end; ++element) {
            const Cell cell = cell_at(array.get_data(), element);
            if (cell.is_null) {
                throw Exception(ErrorCode::CORRUPTION,
                                "Parquet Variant shredded array element wrapper is null");
            }
            (void)append_wrapper(*schema.children[0], *cell.column, element, metadata, builder,
                                 WrapperContext::ARRAY_ELEMENT);
        }
        scope.finish();
        return;
    }
    case ParquetColumnSchemaKind::MAP:
    case ParquetColumnSchemaKind::VARIANT:
        throw Exception(ErrorCode::CORRUPTION, "Invalid Parquet Variant typed_value schema {}",
                        schema.name);
    }
}

bool append_wrapper(const ParquetColumnSchema& schema, const IColumn& wrapper, size_t row,
                    VariantMetadataRef metadata, VariantBatchBuilder::Row& builder,
                    WrapperContext context) {
    Cell value;
    if (find_child(schema, "value", nullptr) != nullptr) {
        value = struct_child_at(schema, wrapper, row, "value", nullptr);
    } else {
        value.is_null = true;
    }
    const ParquetColumnSchema* typed_schema = nullptr;
    Cell typed;
    if (find_child(schema, "typed_value", nullptr) != nullptr) {
        typed = struct_child_at(schema, wrapper, row, "typed_value", &typed_schema);
    } else {
        typed.is_null = true;
    }

    if (find_child(schema, "value", nullptr) == nullptr && typed_schema == nullptr) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Parquet Variant wrapper {} has neither value nor typed_value",
                        schema.name);
    }
    if (value.is_null && typed.is_null) {
        if (context == WrapperContext::OBJECT_FIELD) {
            return false;
        }
        if (context == WrapperContext::ARRAY_ELEMENT) {
            throw Exception(ErrorCode::CORRUPTION, "Parquet Variant array element is missing");
        }
        builder.add_null();
        return true;
    }

    VariantRef residual {.metadata = metadata, .value = {}};
    if (!value.is_null) {
        residual.value = value.column->get_data_at(row);
    }
    if (typed.is_null) {
        builder.add_value(residual);
        return true;
    }
    append_typed_value(*typed_schema, *typed.column, row, metadata,
                       value.is_null ? nullptr : &residual, builder);
    return true;
}

void encode_variant_range(const ParquetColumnSchema& schema, const IColumn& wrapper,
                          const ColumnNullable* outer_nullable, size_t begin, size_t end,
                          bool require_metadata, ColumnVariantV2& variants) {
    try {
        VariantBatchBuilder builder(VariantBatchBuilder::ReserveHint {.rows = end - begin});
        for (size_t row = begin; row < end; ++row) {
            auto output_row = builder.begin_row();
            if (outer_nullable != nullptr && outer_nullable->get_null_map_data()[row] != 0) {
                output_row.add_null();
                output_row.finish();
                continue;
            }
            VariantMetadataRef metadata;
            if (find_child(schema, "metadata", nullptr) != nullptr) {
                const Cell metadata_cell =
                        struct_child_at(schema, wrapper, row, "metadata", nullptr);
                if (metadata_cell.is_null) {
                    throw Exception(ErrorCode::CORRUPTION,
                                    "Parquet Variant {} has null metadata at row {}", schema.name,
                                    row);
                }
                const StringRef metadata_bytes = metadata_cell.column->get_data_at(row);
                metadata = {metadata_bytes.data, metadata_bytes.size};
                metadata.validate();
            } else if (require_metadata) {
                throw Exception(ErrorCode::CORRUPTION, "Parquet Variant {} has no root metadata",
                                schema.name);
            }
            (void)append_wrapper(schema, wrapper, row, metadata, output_row, WrapperContext::ROOT);
            output_row.finish();
        }
        VariantBatchBuilder batch = builder.finish_batch();
        variants.insert_encoded_batch(batch);
    } catch (...) {
        if (end - begin <= 1) {
            throw;
        }
        // A single builder has one metadata dictionary. If heterogeneous file rows cannot fit in
        // that dictionary, split without changing the destination column's already-valid batches.
        // Corrupt input still reaches a one-row range and propagates its original exception.
        const size_t middle = begin + (end - begin) / 2;
        encode_variant_range(schema, wrapper, outer_nullable, begin, middle, require_metadata,
                             variants);
        encode_variant_range(schema, wrapper, outer_nullable, middle, end, require_metadata,
                             variants);
    }
}

std::optional<std::pair<size_t, size_t>> unshredded_child_indices(
        const ParquetColumnSchema& schema) {
    if (schema.children.size() != 2 || find_child(schema, "typed_value", nullptr) != nullptr) {
        return std::nullopt;
    }
    size_t metadata_index = 0;
    size_t value_index = 0;
    if (find_child(schema, "metadata", &metadata_index) == nullptr ||
        find_child(schema, "value", &value_index) == nullptr) {
        return std::nullopt;
    }
    return std::pair {metadata_index, value_index};
}

void import_unshredded_variant_range(const ParquetColumnSchema& schema, const ColumnStruct& wrapper,
                                     const ColumnNullable* outer_nullable, size_t metadata_index,
                                     size_t value_index, size_t begin, size_t end,
                                     DorisVector<VariantRef>& encoded_rows,
                                     ColumnVariantV2::EncodedRowsAppender& appender,
                                     int64_t* imported_bytes) {
    encoded_rows.clear();
    if (encoded_rows.capacity() < end - begin) {
        encoded_rows.reserve(end - begin);
    }
    if (imported_bytes != nullptr) {
        *imported_bytes = 0;
    }
    auto count_imported_bytes = [&](size_t bytes) {
        if (imported_bytes == nullptr) {
            return;
        }
        DORIS_CHECK_LE(bytes,
                       static_cast<size_t>(std::numeric_limits<int64_t>::max() - *imported_bytes));
        *imported_bytes += static_cast<int64_t>(bytes);
    };
    for (size_t row = begin; row < end; ++row) {
        if (outer_nullable != nullptr && outer_nullable->get_null_map_data()[row] != 0) {
            encoded_rows.push_back(
                    {.metadata = {.data = VARIANT_EMPTY_METADATA.data(),
                                  .size = VARIANT_EMPTY_METADATA.size()},
                     .value = {VARIANT_NULL_VALUE.data(), VARIANT_NULL_VALUE.size()}});
            count_imported_bytes(VARIANT_EMPTY_METADATA.size());
            count_imported_bytes(VARIANT_NULL_VALUE.size());
            continue;
        }

        const Cell metadata = cell_at(wrapper.get_column(metadata_index), row);
        if (metadata.is_null) {
            throw Exception(ErrorCode::CORRUPTION, "Parquet Variant {} has null metadata at row {}",
                            schema.name, row);
        }
        const StringRef metadata_bytes = metadata.column->get_data_at(row);
        const Cell value = cell_at(wrapper.get_column(value_index), row);
        const StringRef value_bytes =
                value.is_null ? StringRef(VARIANT_NULL_VALUE.data(), VARIANT_NULL_VALUE.size())
                              : value.column->get_data_at(row);
        encoded_rows.push_back(
                {.metadata = {.data = metadata_bytes.data, .size = metadata_bytes.size},
                 .value = value_bytes});
        count_imported_bytes(metadata_bytes.size);
        count_imported_bytes(value_bytes.size);
    }
    appender.append(std::span<const VariantRef>(encoded_rows));
}

struct DirectResidualSeekResult {
    ColumnPtr column;
    int64_t rows = 0;
    int64_t selected_value_bytes = 0;
    int64_t container_index_builds = 0;
    int64_t container_index_hits = 0;
};

struct DirectSeekContainerKey {
    const char* metadata_data = nullptr;
    size_t metadata_size = 0;
    const char* value_data = nullptr;
    size_t value_size = 0;

    bool operator==(const DirectSeekContainerKey&) const = default;
};

struct DirectSeekContainerKeyHash {
    size_t operator()(const DirectSeekContainerKey& key) const noexcept {
        size_t hash = std::hash<const void*> {}(key.metadata_data);
        auto combine = [&](size_t value) {
            hash ^= value + 0x9e3779b97f4a7c15ULL + (hash << 6) + (hash >> 2);
        };
        combine(std::hash<size_t> {}(key.metadata_size));
        combine(std::hash<const void*> {}(key.value_data));
        combine(std::hash<size_t> {}(key.value_size));
        return hash;
    }
};

class DirectSeekContainerCache {
public:
    struct Lookup {
        VariantContainerLookup* value;
        bool cache_hit;
    };

    Lookup find_or_build(VariantRef value, size_t path_position, DirectResidualSeekResult& result,
                         std::optional<VariantContainerLookup>& uncached_lookup) {
        const DirectSeekContainerKey key {.metadata_data = value.metadata.data,
                                          .metadata_size = value.metadata.size,
                                          .value_data = value.value.data,
                                          .value_size = value.value.size};
        if (auto found = _entries.find(key); found != _entries.end()) {
            ++result.container_index_hits;
            return {.value = &found->second, .cache_hit = true};
        }
        ++result.container_index_builds;
        if (path_position >= MAX_RETAINED_PATH_DEPTH || _entries.size() >= MAX_RETAINED_ENTRIES) {
            // Validation is still mandatory after the high-water mark. Keep only this traversal's
            // lookup alive so an adversarial rows-times-depth path cannot grow persistent state.
            uncached_lookup.emplace(value);
            return {.value = &*uncached_lookup, .cache_hit = false};
        }
        auto [inserted, was_inserted] = _entries.try_emplace(key, value);
        DORIS_CHECK(was_inserted);
        return {.value = &inserted->second, .cache_hit = false};
    }

    bool object_find_by_id(Lookup lookup, int64_t field_id, VariantRef* out) {
        size_t promoted_bytes = 0;
        const size_t promotion_budget =
                lookup.cache_hit ? MAX_PROMOTED_OFFSET_BYTES - _promoted_offset_bytes : 0;
        const bool found =
                lookup.value->object_find_by_id(field_id, out, promotion_budget, &promoted_bytes);
        _promoted_offset_bytes += promoted_bytes;
        return found;
    }

    void reserve(size_t size) {
        if (_entries.empty()) {
            const size_t retained = size > MAX_RETAINED_ENTRIES / MAX_RETAINED_PATH_DEPTH
                                            ? MAX_RETAINED_ENTRIES
                                            : size * MAX_RETAINED_PATH_DEPTH;
            _entries.reserve(retained);
        }
    }

    void reset() {
        ContainerMap empty;
        _entries.swap(empty);
        _promoted_offset_bytes = 0;
    }

    size_t allocated_bytes() const {
        return _entries.bucket_count() * sizeof(void*) +
               _entries.size() * sizeof(ContainerMap::value_type) + _promoted_offset_bytes;
    }

private:
    // Four retained ancestors cover the common root/nested-object projections for a default
    // scanner batch. The independent entry and promoted-offset caps keep state bounded for deep
    // paths, appended columns, and noncanonical wide objects.
    static constexpr size_t MAX_RETAINED_PATH_DEPTH = 4;
    static constexpr size_t MAX_RETAINED_ENTRIES = 16 * 1024;
    static constexpr size_t MAX_PROMOTED_OFFSET_BYTES = 4 * 1024 * 1024;

    using ContainerMap = std::unordered_map<
            DirectSeekContainerKey, VariantContainerLookup, DirectSeekContainerKeyHash,
            std::equal_to<DirectSeekContainerKey>,
            CustomStdAllocator<std::pair<const DirectSeekContainerKey, VariantContainerLookup>>>;
    ContainerMap _entries;
    size_t _promoted_offset_bytes = 0;
};

struct UnshreddedMetadataCache {
    static constexpr uint32_t NO_METADATA = std::numeric_limits<uint32_t>::max();

    DorisVector<VariantMetadataRef> metadatas;
    DorisVector<uint32_t> row_metadata_ids;

    size_t allocated_bytes() const {
        return metadatas.capacity() * sizeof(VariantMetadataRef) +
               row_metadata_ids.capacity() * sizeof(uint32_t);
    }
};

std::shared_ptr<const UnshreddedMetadataCache> build_unshredded_metadata_cache(
        const ParquetColumnSchema& schema, const IColumn& physical, size_t metadata_index) {
    const auto* outer_nullable = check_and_get_column<ColumnNullable>(physical);
    const IColumn& wrapper =
            outer_nullable == nullptr ? physical : outer_nullable->get_nested_column();
    const auto& structure = assert_cast<const ColumnStruct&>(wrapper);
    if (structure.tuple_size() != schema.children.size()) {
        throw Exception(ErrorCode::CORRUPTION, "Parquet Variant {} physical field count mismatch",
                        schema.name);
    }

    using MetadataIndex =
            std::unordered_map<std::string_view, uint32_t, std::hash<std::string_view>,
                               std::equal_to<std::string_view>,
                               CustomStdAllocator<std::pair<const std::string_view, uint32_t>>>;
    auto cache = std::make_shared<UnshreddedMetadataCache>();
    cache->row_metadata_ids.resize(physical.size(), UnshreddedMetadataCache::NO_METADATA);
    MetadataIndex metadata_index_by_value;
    std::optional<uint32_t> previous_metadata_id;

    auto same_metadata = [&](uint32_t id, StringRef bytes) {
        const VariantMetadataRef cached = cache->metadatas[id];
        return StringRef(cached.data, cached.size) == bytes;
    };
    for (size_t row = 0; row < physical.size(); ++row) {
        if (outer_nullable != nullptr && outer_nullable->get_null_map_data()[row] != 0) {
            continue;
        }
        const Cell metadata_cell = cell_at(structure.get_column(metadata_index), row);
        if (metadata_cell.is_null) {
            throw Exception(ErrorCode::CORRUPTION, "Parquet Variant {} has null metadata at row {}",
                            schema.name, row);
        }
        const StringRef metadata_bytes = metadata_cell.column->get_data_at(row);
        if (previous_metadata_id.has_value() &&
            same_metadata(*previous_metadata_id, metadata_bytes)) {
            cache->row_metadata_ids[row] = *previous_metadata_id;
            continue;
        }

        const std::string_view key(metadata_bytes.data == nullptr ? "" : metadata_bytes.data,
                                   metadata_bytes.size);
        if (!metadata_index_by_value.empty()) {
            if (const auto found = metadata_index_by_value.find(key);
                found != metadata_index_by_value.end()) {
                cache->row_metadata_ids[row] = found->second;
                previous_metadata_id = found->second;
                continue;
            }
        }

        VariantMetadataRef metadata {.data = metadata_bytes.data, .size = metadata_bytes.size};
        validate_variant_metadata(metadata);
        if (cache->metadatas.size() == std::numeric_limits<uint32_t>::max()) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Parquet Variant metadata dictionary exceeds the uint32 id limit");
        }
        if (cache->metadatas.size() == 1 && metadata_index_by_value.empty()) {
            const VariantMetadataRef first = cache->metadatas.front();
            metadata_index_by_value.emplace(
                    std::string_view(first.data == nullptr ? "" : first.data, first.size), 0);
        }
        const auto id = static_cast<uint32_t>(cache->metadatas.size());
        cache->metadatas.push_back(metadata);
        if (!metadata_index_by_value.empty()) {
            metadata_index_by_value.emplace(key, id);
        }
        cache->row_metadata_ids[row] = id;
        previous_metadata_id = id;
    }
    return cache;
}

DirectResidualSeekResult seek_unshredded_variant_path(
        const ParquetColumnSchema& schema, const IColumn& physical, size_t value_index,
        const UnshreddedMetadataCache& metadata_cache, DirectSeekContainerCache& container_cache,
        std::span<const VariantShreddedPathSegment> path) {
    const auto* outer_nullable = check_and_get_column<ColumnNullable>(physical);
    const IColumn& wrapper =
            outer_nullable == nullptr ? physical : outer_nullable->get_nested_column();
    const auto& structure = assert_cast<const ColumnStruct&>(wrapper);
    if (structure.tuple_size() != schema.children.size()) {
        throw Exception(ErrorCode::CORRUPTION, "Parquet Variant {} physical field count mismatch",
                        schema.name);
    }

    DORIS_CHECK_EQ(metadata_cache.row_metadata_ids.size(), physical.size());
    if (!metadata_cache.metadatas.empty() &&
        path.size() > std::numeric_limits<size_t>::max() / metadata_cache.metadatas.size()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Parquet Variant direct-seek path cache size overflows size_t");
    }
    DorisVector<int64_t> object_ids(metadata_cache.metadatas.size() * path.size(), -1);
    for (size_t metadata_id = 0; metadata_id < metadata_cache.metadatas.size(); ++metadata_id) {
        for (size_t position = 0; position < path.size(); ++position) {
            if (path[position].kind == VariantShreddedPathSegment::Kind::OBJECT_KEY) {
                object_ids[metadata_id * path.size() + position] =
                        metadata_cache.metadatas[metadata_id].find_key(path[position].key);
            }
        }
    }
    VariantBatchBuilder builder(VariantBatchBuilder::ReserveHint {.rows = physical.size()});
    container_cache.reserve(physical.size());
    auto nulls = ColumnUInt8::create();
    nulls->reserve(physical.size());
    int64_t selected_value_bytes = 0;
    DirectResidualSeekResult result;
    DORIS_CHECK_LE(physical.size(), static_cast<size_t>(std::numeric_limits<int64_t>::max()));
    result.rows = static_cast<int64_t>(physical.size());

    auto add_selected_bytes = [&](size_t bytes) {
        DORIS_CHECK_LE(bytes, static_cast<size_t>(std::numeric_limits<int64_t>::max() -
                                                  selected_value_bytes));
        selected_value_bytes += static_cast<int64_t>(bytes);
    };
    auto append_missing = [&]() {
        auto output_row = builder.begin_row();
        output_row.add_null();
        output_row.finish();
        nulls->insert_value(1);
        add_selected_bytes(VARIANT_NULL_VALUE.size());
    };
    for (size_t row = 0; row < physical.size(); ++row) {
        if (outer_nullable != nullptr && outer_nullable->get_null_map_data()[row] != 0) {
            append_missing();
            continue;
        }

        const uint32_t metadata_id = metadata_cache.row_metadata_ids[row];
        DORIS_CHECK_NE(metadata_id, UnshreddedMetadataCache::NO_METADATA);
        DORIS_CHECK_LT(metadata_id, metadata_cache.metadatas.size());
        const VariantMetadataRef metadata = metadata_cache.metadatas[metadata_id];
        const Cell value_cell = cell_at(structure.get_column(value_index), row);
        if (value_cell.is_null) {
            append_missing();
            continue;
        }

        VariantRef current {.metadata = metadata, .value = value_cell.column->get_data_at(row)};
        bool found = true;
        uint32_t current_depth = 0;
        for (size_t position = 0; position < path.size(); ++position) {
            VariantRef selected;
            const VariantBasicType basic_type =
                    validate_variant_payload_shallow(current, current_depth);
            std::optional<DirectSeekContainerCache::Lookup> container;
            std::optional<VariantContainerLookup> uncached_container;
            if (basic_type == VariantBasicType::OBJECT || basic_type == VariantBasicType::ARRAY) {
                // Validate an accessed container even when the requested segment has the other
                // container kind. A malformed table must not be downgraded to a SQL NULL miss.
                container = container_cache.find_or_build(current, position, result,
                                                          uncached_container);
            }
            if (path[position].kind == VariantShreddedPathSegment::Kind::OBJECT_KEY) {
                const int64_t field_id = object_ids[metadata_id * path.size() + position];
                if (basic_type != VariantBasicType::OBJECT ||
                    !container_cache.object_find_by_id(*container, field_id, &selected)) {
                    found = false;
                    break;
                }
            } else {
                if (basic_type != VariantBasicType::ARRAY ||
                    !container->value->array_find(path[position].index, &selected)) {
                    found = false;
                    break;
                }
            }
            current = selected;
            ++current_depth;
        }
        if (!found) {
            append_missing();
            continue;
        }

        // Traversed containers perform bounded reads. Validate the selected subtree exactly, but
        // intentionally do not visit unrelated siblings in the unshredded root.
        validate_variant_payload(current, current_depth);
        // Re-encode only the selected subtree so scalar/missing results use empty metadata and
        // containers retain only reachable keys with remapped ids. Copying the root dictionary
        // would multiply wide metadata across independently projected paths and composite states.
        auto output_row = builder.begin_row();
        output_row.add_value(current);
        output_row.finish();
        nulls->insert_value(0);
        add_selected_bytes(current.value.size);
    }

    auto values = ColumnVariantV2::create();
    values->insert_encoded_batch(builder.finish_batch());
    result.column = ColumnNullable::create(std::move(values), std::move(nulls));
    result.selected_value_bytes = selected_value_bytes;
    return result;
}

ColumnVariantV2::MutablePtr encode_variant_column(
        const ParquetColumnSchema& schema, const IColumn& physical, bool require_metadata = true,
        const ParquetColumnReaderProfile* profile = nullptr) {
    if (schema.kind != ParquetColumnSchemaKind::VARIANT) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Parquet column {} is not Variant",
                        schema.name);
    }
    const auto* outer_nullable = check_and_get_column<ColumnNullable>(physical);
    const IColumn& wrapper =
            outer_nullable == nullptr ? physical : outer_nullable->get_nested_column();
    const auto& structure = assert_cast<const ColumnStruct&>(wrapper);
    if (structure.tuple_size() != schema.children.size()) {
        throw Exception(ErrorCode::CORRUPTION, "Parquet Variant {} physical field count mismatch",
                        schema.name);
    }

    auto variants = ColumnVariantV2::create();
    // A complete unshredded root already contains exact Variant Encoding V1 bytes. Keep the
    // existing lazy materialization boundary, but validate and copy those bytes without rebuilding
    // the value tree through VariantBatchBuilder.
    const auto unshredded_indices =
            require_metadata ? unshredded_child_indices(schema) : std::nullopt;
    constexpr size_t MAX_MATERIALIZATION_BATCH_ROWS = 4096;
    DorisVector<VariantRef> encoded_rows;
    std::optional<ColumnVariantV2::EncodedRowsAppender> encoded_rows_appender;
    if (unshredded_indices.has_value()) {
        encoded_rows.reserve(std::min(physical.size(), MAX_MATERIALIZATION_BATCH_ROWS));
        encoded_rows_appender.emplace(variants->create_encoded_rows_appender());
    }
    for (size_t begin = 0; begin < physical.size(); begin += MAX_MATERIALIZATION_BATCH_ROWS) {
        const size_t end = std::min(physical.size(), begin + MAX_MATERIALIZATION_BATCH_ROWS);
        if (unshredded_indices.has_value()) {
            DORIS_CHECK(encoded_rows_appender.has_value());
            int64_t imported_bytes = 0;
            RuntimeProfile::Counter* direct_import_time =
                    profile == nullptr ? nullptr
                                       : profile->variant_unshredded_direct_import_time.get();
            {
                // Update the shared timer on every exit, including validation failures. Rows and
                // bytes are published only after the complete chunk has been appended.
                SCOPED_TIMER(direct_import_time);
                import_unshredded_variant_range(
                        schema, structure, outer_nullable, unshredded_indices->first,
                        unshredded_indices->second, begin, end, encoded_rows,
                        *encoded_rows_appender, profile == nullptr ? nullptr : &imported_bytes);
            }
            if (profile != nullptr) {
                const auto imported_rows = static_cast<int64_t>(end - begin);
                if (profile->variant_unshredded_direct_import_rows != nullptr) {
                    COUNTER_UPDATE(profile->variant_unshredded_direct_import_rows.get(),
                                   imported_rows);
                }
                if (profile->variant_unshredded_direct_import_bytes != nullptr) {
                    COUNTER_UPDATE(profile->variant_unshredded_direct_import_bytes.get(),
                                   imported_bytes);
                }
            }
        } else {
            encode_variant_range(schema, wrapper, outer_nullable, begin, end, require_metadata,
                                 *variants);
        }
    }
    return variants;
}

std::unique_ptr<ParquetColumnSchema> clone_schema(
        const ParquetColumnSchema& source, const format::LocalColumnIndex* projection = nullptr) {
    auto result = std::make_unique<ParquetColumnSchema>();
    result->local_id = source.local_id;
    result->parquet_field_id = source.parquet_field_id;
    result->name = source.name;
    result->type = source.type;
    result->variant_physical_type = source.variant_physical_type;
    result->leaf_column_id = source.leaf_column_id;
    result->type_descriptor = source.type_descriptor;
    result->kind = source.kind;
    result->contains_variant = source.contains_variant;
    result->max_definition_level = source.max_definition_level;
    result->max_repetition_level = source.max_repetition_level;
    result->nullable_definition_level = source.nullable_definition_level;
    result->definition_level = source.definition_level;
    result->repetition_level = source.repetition_level;
    result->repeated_ancestor_definition_level = source.repeated_ancestor_definition_level;
    result->repeated_repetition_level = source.repeated_repetition_level;
    const bool partial = format::is_partial_projection(projection);
    result->children.reserve(partial ? projection->children.size() : source.children.size());
    if (partial) {
        // NativeColumnReader emits a partial STRUCT in projection order, so the retained schema
        // must use that same order or field names will address the wrong physical tuple element.
        for (const auto& child_projection : projection->children) {
            const auto child = std::ranges::find_if(source.children, [&](const auto& candidate) {
                return candidate->local_id == child_projection.local_id();
            });
            DORIS_CHECK(child != source.children.end());
            result->children.push_back(clone_schema(**child, &child_projection));
        }
    } else {
        for (const auto& child : source.children) {
            result->children.push_back(clone_schema(*child));
        }
    }
    return result;
}

ColumnPtr unwrap_nullable(ColumnPtr column) {
    if (const auto* nullable = check_and_get_column<ColumnNullable>(*column)) {
        return nullable->get_nested_column_ptr();
    }
    return column;
}

ColumnPtr struct_child(const ParquetColumnSchema& schema, ColumnPtr column, std::string_view name,
                       const ParquetColumnSchema** child_schema) {
    column = unwrap_nullable(std::move(column));
    const auto* structure = check_and_get_column<ColumnStruct>(*column);
    if (structure == nullptr) {
        return nullptr;
    }
    size_t index = 0;
    const auto* child = find_child(schema, name, &index);
    if (child == nullptr || index >= structure->tuple_size()) {
        return nullptr;
    }
    if (child_schema != nullptr) {
        *child_schema = child;
    }
    return structure->get_column_ptr(index);
}

bool has_present_value(const ColumnPtr& column) {
    if (const auto* nullable = check_and_get_column<ColumnNullable>(*column)) {
        return std::ranges::any_of(nullable->get_null_map_data(),
                                   [](uint8_t is_null) { return is_null == 0; });
    }
    return !column->empty();
}

bool supports_direct_typed_variant_state(const ParquetColumnSchema& schema) {
    if (schema.type == nullptr || schema.kind != ParquetColumnSchemaKind::PRIMITIVE) {
        return false;
    }
    // ColumnVariantV2 typed state carries only a Doris type. Binary/UUID annotations, temporal
    // units, and other Parquet-only identity must therefore reconstruct canonical Variant bytes.
    switch (remove_nullable(schema.type)->get_primitive_type()) {
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
    case TYPE_FLOAT:
    case TYPE_DOUBLE:
    case TYPE_DECIMAL128I:
    case TYPE_DATEV2:
        return true;
    default:
        return false;
    }
}

ColumnPtr normalize_projected_primitive_leaf(const ParquetColumnSchema& schema,
                                             const ColumnPtr& typed) {
    const auto& nullable = assert_cast<const ColumnNullable&>(*typed);
    VariantBatchBuilder builder(VariantBatchBuilder::ReserveHint {.rows = nullable.size()});
    for (size_t row = 0; row < nullable.size(); ++row) {
        auto output_row = builder.begin_row();
        if (nullable.get_null_map_data()[row] != 0) {
            output_row.add_null();
        } else {
            append_typed_scalar(schema, nullable.get_nested_column(), row, output_row);
        }
        output_row.finish();
    }
    auto values = ColumnVariantV2::create();
    values->insert_encoded_batch(builder.finish_batch());
    auto nulls = nullable.get_null_map_column().clone_resized(nullable.size());
    return ColumnNullable::create(std::move(values), std::move(nulls));
}

bool find_materialized_path(VariantRef current, std::span<const VariantShreddedPathSegment> path,
                            VariantRef* output) {
    DORIS_CHECK(output != nullptr);
    for (const auto& segment : path) {
        if (segment.kind == VariantShreddedPathSegment::Kind::OBJECT_KEY) {
            if (current.basic_type() != VariantBasicType::OBJECT ||
                !current.object_find(segment.key, &current)) {
                return false;
            }
            continue;
        }
        if (current.basic_type() != VariantBasicType::ARRAY) {
            return false;
        }
        const int64_t count = current.num_elements();
        const int64_t index = segment.index < 0 ? count + segment.index : segment.index;
        if (index < 0 || index >= count) {
            return false;
        }
        current = current.array_at(static_cast<uint32_t>(index));
    }
    *output = current;
    return true;
}

ColumnPtr normalize_materialized_path(const ColumnVariantV2& materialized,
                                      std::span<const VariantShreddedPathSegment> path) {
    VariantBatchBuilder builder(VariantBatchBuilder::ReserveHint {.rows = materialized.size()});
    auto nulls = ColumnUInt8::create();
    nulls->reserve(materialized.size());
    for (size_t row = 0; row < materialized.size(); ++row) {
        auto output_row = builder.begin_row();
        VariantRef value;
        if (find_materialized_path(materialized.get_value_ref(row), path, &value)) {
            output_row.add_value(value);
            nulls->insert_value(0);
        } else {
            output_row.add_null();
            nulls->insert_value(1);
        }
        output_row.finish();
    }
    auto values = ColumnVariantV2::create();
    values->insert_encoded_batch(builder.finish_batch());
    return ColumnNullable::create(std::move(values), std::move(nulls));
}

bool same_data_type(const DataTypePtr& left, const DataTypePtr& right) {
    return (!left && !right) || (left && right && left->equals(*right));
}

bool same_type_descriptor(const ParquetTypeDescriptor& left, const ParquetTypeDescriptor& right) {
    return same_data_type(left.doris_type, right.doris_type) &&
           same_data_type(left.physical_doris_type, right.physical_doris_type) &&
           left.extra_type_info == right.extra_type_info && left.time_unit == right.time_unit &&
           left.physical_type == right.physical_type &&
           left.integer_bit_width == right.integer_bit_width &&
           left.decimal_precision == right.decimal_precision &&
           left.decimal_scale == right.decimal_scale && left.fixed_length == right.fixed_length &&
           left.is_unsigned_integer == right.is_unsigned_integer &&
           left.is_decimal == right.is_decimal && left.is_timestamp == right.is_timestamp &&
           left.timestamp_is_adjusted_to_utc == right.timestamp_is_adjusted_to_utc &&
           left.is_string_like == right.is_string_like &&
           left.is_string_annotation == right.is_string_annotation &&
           left.is_uuid == right.is_uuid && left.unsupported_reason == right.unsupported_reason;
}

bool same_shredded_schema(const ParquetColumnSchema& left, const ParquetColumnSchema& right) {
    if (left.name != right.name || left.kind != right.kind ||
        !same_data_type(left.type, right.type) ||
        !same_type_descriptor(left.type_descriptor, right.type_descriptor) ||
        left.children.size() != right.children.size()) {
        return false;
    }
    for (size_t i = 0; i < left.children.size(); ++i) {
        if (!same_shredded_schema(*left.children[i], *right.children[i])) {
            return false;
        }
    }
    return true;
}

void append_compatible_column(IColumn& output, const IColumn& converted);
void validate_compatible_column(const IColumn& output, const IColumn& converted);

class ParquetVariantShreddedState final : public VariantShreddedState {
public:
    ParquetVariantShreddedState(std::shared_ptr<const ParquetColumnSchema> schema,
                                ColumnPtr physical, bool complete,
                                ParquetColumnReaderProfile profile = {})
            : _schema(std::move(schema)),
              _physical(std::move(physical)),
              _complete(complete),
              _profile(profile) {
        DORIS_CHECK(_schema != nullptr && static_cast<bool>(_physical));
        const ColumnPtr wrapper = unwrap_nullable(_physical);
        const auto* structure = check_and_get_column<ColumnStruct>(*wrapper);
        if (structure == nullptr || structure->tuple_size() != _schema->children.size()) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Parquet Variant {} physical field count mismatch", _schema->name);
        }
    }

    size_t size() const override { return _physical->size(); }
    size_t byte_size() const override {
        std::lock_guard lock(_materialization_lock);
        return _physical->byte_size() + (_materialized ? _materialized->byte_size() : 0) +
               (_serialized ? _serialized->byte_size() : 0);
    }
    size_t allocated_bytes() const override {
        std::lock_guard lock(_materialization_lock);
        return _physical->allocated_bytes() +
               (_materialized ? _materialized->allocated_bytes() : 0) +
               (_serialized ? _serialized->allocated_bytes() : 0) +
               (_unshredded_metadata_cache ? _unshredded_metadata_cache->allocated_bytes() : 0) +
               _direct_seek_container_cache.allocated_bytes();
    }
    void sanity_check() const override { _physical->sanity_check(); }

    void for_each_subcolumn(const IColumn::ImutableColumnCallback& callback) const override {
        callback(*_physical);
    }

    std::shared_ptr<VariantShreddedState> filter(const IColumn::Filter& filter,
                                                 ssize_t result_size_hint) const override {
        // Compact the decoded physical tree directly. In particular, a leaf-only projection has
        // no metadata/value columns from which a canonical Variant could be reconstructed.
        // The projection schema is immutable and reader-scoped, so derived selections share it
        // instead of cloning the whole shredded tree for every filter operation.
        return std::make_shared<ParquetVariantShreddedState>(
                _schema, _physical->filter(filter, result_size_hint), _complete, _profile);
    }

    std::shared_ptr<VariantShreddedState> select_range(size_t start, size_t length) const override {
        return std::make_shared<ParquetVariantShreddedState>(_schema, _physical->cut(start, length),
                                                             _complete, _profile);
    }

    std::shared_ptr<VariantShreddedState> select_indices(
            const uint32_t* indices_begin, const uint32_t* indices_end) const override {
        MutableColumnPtr selected = _physical->clone_empty();
        selected->insert_indices_from(*_physical, indices_begin, indices_end);
        return std::make_shared<ParquetVariantShreddedState>(_schema, std::move(selected),
                                                             _complete, _profile);
    }

    bool can_materialize() const override { return _complete; }

    bool try_append(const VariantShreddedState& source) override {
        const auto* parquet_source = dynamic_cast<const ParquetVariantShreddedState*>(&source);
        if (parquet_source == nullptr || _complete != parquet_source->_complete ||
            !same_shredded_schema(*_schema, *parquet_source->_schema)) {
            return false;
        }
        std::lock_guard lock(_materialization_lock);
        validate_compatible_column(*_physical, *parquet_source->_physical);
        auto mutable_physical = IColumn::mutate(std::move(_physical));
        append_compatible_column(*mutable_physical, *parquet_source->_physical);
        _physical = std::move(mutable_physical);
        _materialized.reset();
        _serialized.reset();
        _unshredded_metadata_cache.reset();
        _direct_seek_container_cache.reset();
        return true;
    }

    std::optional<VariantShreddedTypedValue> find_typed_value(
            std::span<const VariantShreddedPathSegment> path) const override {
        auto residual_seek_fallback = [&]() -> std::optional<VariantShreddedTypedValue> {
            // Complete mixed shredded states still need canonical reconstruction when neither a
            // typed leaf nor the pure unshredded direct-seek path can answer the request.
            if (_complete && !unshredded_child_indices(*_schema).has_value() &&
                find_child(*_schema, "value", nullptr) != nullptr) {
                update_counter(_profile.variant_direct_residual_seek_fallbacks, 1);
            }
            return std::nullopt;
        };
        auto path_miss = [&]() -> std::optional<VariantShreddedTypedValue> {
            update_counter(_profile.variant_direct_leaf_path_misses, 1);
            return residual_seek_fallback();
        };
        if (path.empty()) {
            return path_miss();
        }
        if (auto normalized = find_unshredded_normalized_value(path); normalized.has_value()) {
            return VariantShreddedTypedValue {
                    .column = nullptr, .type = nullptr, .normalized = std::move(*normalized)};
        }

        const ParquetColumnSchema* typed_schema = nullptr;
        ColumnPtr typed = struct_child(*_schema, _physical, "typed_value", &typed_schema);
        if (!typed || typed_schema->kind != ParquetColumnSchemaKind::STRUCT) {
            return path_miss();
        }

        for (size_t position = 0; position < path.size(); ++position) {
            if (path[position].kind != VariantShreddedPathSegment::Kind::OBJECT_KEY) {
                return path_miss();
            }

            const std::string_view key(path[position].key.data, path[position].key.size);
            const ParquetColumnSchema* wrapper_schema = nullptr;
            ColumnPtr wrapper = struct_child(*typed_schema, typed, key, &wrapper_schema);
            if (!wrapper) {
                return path_miss();
            }

            if (ColumnPtr residual = struct_child(*wrapper_schema, wrapper, "value", nullptr);
                static_cast<bool>(residual) && has_present_value(residual)) {
                // A residual value can contribute data to the same logical object. Reconstructing
                // is required in that case; returning only the typed leaf would drop information.
                update_counter(_profile.variant_direct_leaf_residual_fallbacks, 1);
                return residual_seek_fallback();
            }

            typed = struct_child(*wrapper_schema, wrapper, "typed_value", &typed_schema);
            if (!typed) {
                return path_miss();
            }
            if (position + 1 == path.size()) {
                if (typed_schema->kind != ParquetColumnSchemaKind::PRIMITIVE ||
                    check_and_get_column<ColumnNullable>(*typed) == nullptr) {
                    update_counter(_profile.variant_direct_leaf_unsupported_fallbacks, 1);
                    if (!_complete) {
                        // Binary element_at evaluates complex prefixes before the validated leaf.
                        // Serialize only retained descendants so projected-out fields stay hidden.
                        if (auto normalized = find_normalized_value(path); normalized.has_value()) {
                            return VariantShreddedTypedValue {
                                    .column = nullptr, .type = nullptr, .normalized = *normalized};
                        }
                    }
                    return residual_seek_fallback();
                }
                if (!supports_direct_typed_variant_state(*typed_schema)) {
                    if (_complete) {
                        update_counter(_profile.variant_direct_leaf_unsupported_fallbacks, 1);
                        return residual_seek_fallback();
                    }
                    // A partial projection cannot reconstruct its root. Normalize only the exact
                    // requested leaf so Parquet annotations survive heterogeneous file schemas.
                    update_counter(_profile.variant_direct_leaf_rows,
                                   static_cast<int64_t>(typed->size()));
                    return VariantShreddedTypedValue {
                            .column = nullptr,
                            .type = nullptr,
                            .normalized = normalize_projected_primitive_leaf(*typed_schema, typed)};
                }
                update_counter(_profile.variant_direct_leaf_rows,
                               static_cast<int64_t>(typed->size()));
                return VariantShreddedTypedValue {.column = std::move(typed),
                                                  .type = remove_nullable(typed_schema->type),
                                                  .normalized = nullptr};
            }
            if (typed_schema->kind != ParquetColumnSchemaKind::STRUCT) {
                return path_miss();
            }
        }
        return residual_seek_fallback();
    }

    std::optional<ColumnPtr> find_normalized_value(
            std::span<const VariantShreddedPathSegment> path) const override {
        if (path.empty()) {
            return std::nullopt;
        }
        if (auto normalized = find_unshredded_normalized_value(path); normalized.has_value()) {
            return normalized;
        }

        const ParquetColumnSchema* typed_schema = nullptr;
        ColumnPtr typed = struct_child(*_schema, _physical, "typed_value", &typed_schema);
        if (typed && typed_schema->kind == ParquetColumnSchemaKind::STRUCT) {
            bool direct = true;
            for (size_t position = 0; position < path.size(); ++position) {
                if (path[position].kind != VariantShreddedPathSegment::Kind::OBJECT_KEY) {
                    direct = false;
                    break;
                }
                const std::string_view key(path[position].key.data, path[position].key.size);
                const ParquetColumnSchema* wrapper_schema = nullptr;
                ColumnPtr wrapper = struct_child(*typed_schema, typed, key, &wrapper_schema);
                if (!wrapper) {
                    direct = false;
                    break;
                }
                ColumnPtr residual = struct_child(*wrapper_schema, wrapper, "value", nullptr);
                if (residual && has_present_value(residual)) {
                    direct = false;
                    break;
                }
                typed = struct_child(*wrapper_schema, wrapper, "typed_value", &typed_schema);
                if (!typed) {
                    direct = false;
                    break;
                }
                if (position + 1 == path.size()) {
                    direct = typed_schema->kind == ParquetColumnSchemaKind::PRIMITIVE &&
                             check_and_get_column<ColumnNullable>(*typed) != nullptr;
                } else if (typed_schema->kind != ParquetColumnSchemaKind::STRUCT) {
                    direct = false;
                    break;
                }
            }
            if (direct) {
                return normalize_projected_primitive_leaf(*typed_schema, typed);
            }
        }
        if (_complete) {
            return normalize_materialized_path(materialized_column(), path);
        }
        // Binary element_at chains request complex prefixes before their validated primitive leaf.
        // Reconstruct only retained descendants so an omitted field can never become observable.
        return normalize_materialized_path(serialized_column(), path);
    }

    const ColumnVariantV2& materialized_column() const override {
        std::lock_guard lock(_materialization_lock);
        if (!_complete) {
            throw Exception(
                    ErrorCode::INTERNAL_ERROR,
                    "A projected Parquet Variant can only serve its validated shredded leaves");
        }
        if (!_materialized) {
            SCOPED_TIMER(_profile.variant_reconstruction_time.get());
            _materialized = encode_variant_column(*_schema, *_physical, true, &_profile);
            update_counter(_profile.variant_reconstructed_rows,
                           static_cast<int64_t>(_physical->size()));
        }
        return *_materialized;
    }

    const ColumnVariantV2& serialized_column() const override {
        if (_complete) {
            return materialized_column();
        }
        std::lock_guard lock(_materialization_lock);
        if (!_serialized) {
            // Projected states intentionally omit root metadata, but an exchange buffer still
            // needs self-contained bytes. Rebuild only retained paths; access-path planning is the
            // invariant that prevents a downstream consumer from observing an omitted field.
            _serialized = encode_variant_column(*_schema, *_physical, false);
        }
        return *_serialized;
    }

private:
    std::optional<ColumnPtr> find_unshredded_normalized_value(
            std::span<const VariantShreddedPathSegment> path) const {
        if (!_complete) {
            return std::nullopt;
        }
        const auto indices = unshredded_child_indices(*_schema);
        if (!indices.has_value()) {
            return std::nullopt;
        }

        DirectResidualSeekResult result;
        {
            // Publish elapsed time even when an accessed node is corrupt. Row/byte counters only
            // describe complete direct-seek results.
            SCOPED_TIMER(_profile.variant_direct_residual_seek_time.get());
            std::lock_guard lock(_materialization_lock);
            if (!_unshredded_metadata_cache) {
                _unshredded_metadata_cache =
                        build_unshredded_metadata_cache(*_schema, *_physical, indices->first);
            }
            result = seek_unshredded_variant_path(*_schema, *_physical, indices->second,
                                                  *_unshredded_metadata_cache,
                                                  _direct_seek_container_cache, path);
        }
        update_counter(_profile.variant_direct_residual_seek_rows, result.rows);
        update_counter(_profile.variant_direct_residual_seek_bytes, result.selected_value_bytes);
        update_counter(_profile.variant_direct_residual_seek_container_index_builds,
                       result.container_index_builds);
        update_counter(_profile.variant_direct_residual_seek_container_index_hits,
                       result.container_index_hits);
        return std::move(result.column);
    }

    static void update_counter(const std::shared_ptr<RuntimeProfile::Counter>& counter,
                               int64_t value) {
        if (counter != nullptr) {
            COUNTER_UPDATE(counter.get(), value);
        }
    }

    std::shared_ptr<const ParquetColumnSchema> _schema;
    ColumnPtr _physical;
    bool _complete = true;
    ParquetColumnReaderProfile _profile;
    mutable std::mutex _materialization_lock;
    mutable ColumnVariantV2::MutablePtr _materialized;
    mutable ColumnVariantV2::MutablePtr _serialized;
    mutable std::shared_ptr<const UnshreddedMetadataCache> _unshredded_metadata_cache;
    mutable DirectSeekContainerCache _direct_seek_container_cache;
};

MutableColumnPtr build_variant_column(std::shared_ptr<const ParquetColumnSchema> schema,
                                      ColumnPtr physical, bool complete,
                                      const ParquetColumnReaderProfile& profile) {
    DORIS_CHECK(schema != nullptr);
    if (schema->kind != ParquetColumnSchemaKind::VARIANT) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Parquet column {} is not Variant",
                        schema->name);
    }

    const auto* outer_nullable = check_and_get_column<ColumnNullable>(*physical);
    MutableColumnPtr variants =
            ColumnVariantV2::create_shredded(std::make_shared<ParquetVariantShreddedState>(
                    std::move(schema), physical, complete, profile));
    if (outer_nullable == nullptr) {
        return variants;
    }
    auto nulls = outer_nullable->get_null_map_column().clone_resized(physical->size());
    return ColumnNullable::create(std::move(variants), std::move(nulls));
}

ColumnPtr transform_node(const VariantMaterializationNode& plan, ColumnPtr physical,
                         const ParquetColumnReaderProfile& profile);

ColumnPtr transform_non_nullable(const VariantMaterializationNode& plan, ColumnPtr physical,
                                 const ParquetColumnReaderProfile& profile) {
    const auto& schema = *plan.schema;
    switch (schema.kind) {
    case ParquetColumnSchemaKind::PRIMITIVE:
        return physical;
    case ParquetColumnSchemaKind::VARIANT:
        return build_variant_column(
                plan.variant_state_schema
                        ? plan.variant_state_schema
                        : create_variant_state_schema(schema, plan.variant_projection
                                                                      ? &*plan.variant_projection
                                                                      : nullptr),
                std::move(physical),
                !format::is_partial_projection(plan.variant_projection ? &*plan.variant_projection
                                                                       : nullptr),
                profile);
    case ParquetColumnSchemaKind::STRUCT: {
        const auto& structure = assert_cast<const ColumnStruct&>(*physical);
        if (structure.tuple_size() != plan.children.size()) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Projected Parquet STRUCT {} field count mismatch", schema.name);
        }
        Columns fields;
        fields.reserve(plan.children.size());
        for (size_t i = 0; i < plan.children.size(); ++i) {
            fields.push_back(
                    transform_node(*plan.children[i], structure.get_column_ptr(i), profile));
        }
        return ColumnStruct::create(std::move(fields));
    }
    case ParquetColumnSchemaKind::LIST: {
        const auto& array = assert_cast<const ColumnArray&>(*physical);
        if (plan.children.size() != 1) {
            throw Exception(ErrorCode::CORRUPTION, "Projected Parquet ARRAY plan is invalid");
        }
        auto values = transform_node(*plan.children[0], array.get_data_ptr(), profile);
        return ColumnArray::create(std::move(values), array.get_offsets_ptr());
    }
    case ParquetColumnSchemaKind::MAP: {
        const auto& map = assert_cast<const ColumnMap&>(*physical);
        if (plan.children.size() != 2) {
            throw Exception(ErrorCode::CORRUPTION, "Projected Parquet MAP plan is invalid");
        }
        auto keys = transform_node(*plan.children[0], map.get_keys_ptr(), profile);
        auto values = transform_node(*plan.children[1], map.get_values_ptr(), profile);
        return ColumnMap::create(std::move(keys), std::move(values), map.get_offsets_ptr());
    }
    }
    throw Exception(ErrorCode::INTERNAL_ERROR, "Unknown Parquet schema kind");
}

ColumnPtr transform_node(const VariantMaterializationNode& plan, ColumnPtr physical,
                         const ParquetColumnReaderProfile& profile) {
    if (plan.schema == nullptr) {
        throw Exception(ErrorCode::INTERNAL_ERROR, "Parquet Variant materialization plan is null");
    }
    if (plan.schema->kind == ParquetColumnSchemaKind::VARIANT) {
        return build_variant_column(
                plan.variant_state_schema
                        ? plan.variant_state_schema
                        : create_variant_state_schema(
                                  *plan.schema,
                                  plan.variant_projection ? &*plan.variant_projection : nullptr),
                std::move(physical),
                !format::is_partial_projection(plan.variant_projection ? &*plan.variant_projection
                                                                       : nullptr),
                profile);
    }
    if (const auto* nullable = check_and_get_column<ColumnNullable>(*physical)) {
        auto nested = transform_non_nullable(plan, nullable->get_nested_column_ptr(), profile);
        return ColumnNullable::create(std::move(nested), nullable->get_null_map_column_ptr());
    }
    return transform_non_nullable(plan, std::move(physical), profile);
}

void append_compatible_column(IColumn& output, const IColumn& converted) {
    if (auto* output_nullable = check_and_get_column<ColumnNullable>(output)) {
        auto& nested = output_nullable->get_nested_column();
        auto& null_map = output_nullable->get_null_map_column();
        const size_t nested_size = nested.size();
        const size_t null_map_size = null_map.size();
        try {
            if (const auto* converted_nullable = check_and_get_column<ColumnNullable>(converted)) {
                append_compatible_column(nested, converted_nullable->get_nested_column());
                null_map.insert_range_from(converted_nullable->get_null_map_column(), 0,
                                           converted.size());
            } else {
                append_compatible_column(nested, converted);
                // External slots and nested Iceberg fields may remain nullable even when one
                // file's physical node is required. Preserve that destination invariant with
                // non-null bits.
                output_nullable->push_false_to_nullmap(converted.size());
            }
        } catch (...) {
            if (nested.size() > nested_size) {
                nested.pop_back(nested.size() - nested_size);
            }
            if (null_map.size() > null_map_size) {
                null_map.pop_back(null_map.size() - null_map_size);
            }
            throw;
        }
        return;
    }

    if (const auto* converted_nullable = check_and_get_column<ColumnNullable>(converted)) {
        // Parquet writers may encode an Iceberg required field as optional. It can populate a
        // non-nullable destination only when this batch proves that every value is present.
        if (converted_nullable->has_null()) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Parquet Variant materialization produced null data for a "
                            "non-nullable destination");
        }
        append_compatible_column(output, converted_nullable->get_nested_column());
        return;
    }

    if (auto* output_struct = check_and_get_column<ColumnStruct>(output)) {
        const auto* converted_struct = check_and_get_column<ColumnStruct>(converted);
        if (converted_struct == nullptr ||
            output_struct->tuple_size() != converted_struct->tuple_size()) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Parquet Variant materialization produced an incompatible STRUCT");
        }
        std::vector<size_t> original_sizes(output_struct->tuple_size());
        for (size_t i = 0; i < output_struct->tuple_size(); ++i) {
            original_sizes[i] = output_struct->get_column(i).size();
        }
        try {
            for (size_t i = 0; i < output_struct->tuple_size(); ++i) {
                append_compatible_column(output_struct->get_column(i),
                                         converted_struct->get_column(i));
            }
        } catch (...) {
            // Variant corruption can surface only during lazy fallback after earlier siblings
            // were appended. Roll every child back to preserve the failed-append invariant.
            for (size_t i = 0; i < output_struct->tuple_size(); ++i) {
                auto& child = output_struct->get_column(i);
                if (child.size() > original_sizes[i]) {
                    child.pop_back(child.size() - original_sizes[i]);
                }
            }
            throw;
        }
        return;
    }

    if (auto* output_array = check_and_get_column<ColumnArray>(output)) {
        const auto* converted_array = check_and_get_column<ColumnArray>(converted);
        if (converted_array == nullptr) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Parquet Variant materialization produced an incompatible ARRAY");
        }
        auto& output_data = output_array->get_data();
        auto& output_offsets = output_array->get_offsets();
        const size_t element_base = output_data.size();
        const size_t offsets_size = output_offsets.size();
        try {
            append_compatible_column(output_data, converted_array->get_data());
            output_offsets.reserve(output_offsets.size() + converted_array->size());
            for (const auto offset : converted_array->get_offsets()) {
                output_offsets.push_back(element_base + offset);
            }
        } catch (...) {
            if (output_data.size() > element_base) {
                output_data.pop_back(output_data.size() - element_base);
            }
            output_offsets.resize(offsets_size);
            throw;
        }
        return;
    }

    if (auto* output_map = check_and_get_column<ColumnMap>(output)) {
        const auto* converted_map = check_and_get_column<ColumnMap>(converted);
        if (converted_map == nullptr) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Parquet Variant materialization produced an incompatible MAP");
        }
        auto& output_keys = output_map->get_keys();
        auto& output_values = output_map->get_values();
        auto& output_offsets = output_map->get_offsets();
        const size_t element_base = output_keys.size();
        const size_t values_size = output_values.size();
        const size_t offsets_size = output_offsets.size();
        try {
            append_compatible_column(output_keys, converted_map->get_keys());
            append_compatible_column(output_values, converted_map->get_values());
            output_offsets.reserve(output_offsets.size() + converted_map->size());
            for (const auto offset : converted_map->get_offsets()) {
                output_offsets.push_back(element_base + offset);
            }
        } catch (...) {
            if (output_keys.size() > element_base) {
                output_keys.pop_back(output_keys.size() - element_base);
            }
            if (output_values.size() > values_size) {
                output_values.pop_back(output_values.size() - values_size);
            }
            output_offsets.resize(offsets_size);
            throw;
        }
        return;
    }

    if (auto* output_variant = check_and_get_column<ColumnVariantV2>(output)) {
        const auto* converted_variant = check_and_get_column<ColumnVariantV2>(converted);
        if (converted_variant == nullptr) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Parquet Variant materialization produced an incompatible column");
        }
        output_variant->insert_range_from(*converted_variant, 0, converted_variant->size());
        return;
    }

    if (output.get_name() != converted.get_name()) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Parquet Variant materialization produced {} for {} destination",
                        converted.get_name(), output.get_name());
    }
    output.insert_range_from(converted, 0, converted.size());
}

void validate_compatible_column(const IColumn& output, const IColumn& converted) {
    if (const auto* output_nullable = check_and_get_column<ColumnNullable>(output)) {
        if (const auto* converted_nullable = check_and_get_column<ColumnNullable>(converted)) {
            validate_compatible_column(output_nullable->get_nested_column(),
                                       converted_nullable->get_nested_column());
        } else {
            validate_compatible_column(output_nullable->get_nested_column(), converted);
        }
        return;
    }
    if (const auto* converted_nullable = check_and_get_column<ColumnNullable>(converted)) {
        if (converted_nullable->has_null()) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Parquet Variant materialization produced null data for a "
                            "non-nullable destination");
        }
        validate_compatible_column(output, converted_nullable->get_nested_column());
        return;
    }
    if (const auto* output_struct = check_and_get_column<ColumnStruct>(output)) {
        const auto* converted_struct = check_and_get_column<ColumnStruct>(converted);
        if (converted_struct == nullptr ||
            output_struct->tuple_size() != converted_struct->tuple_size()) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Parquet Variant materialization produced an incompatible STRUCT");
        }
        for (size_t i = 0; i < output_struct->tuple_size(); ++i) {
            validate_compatible_column(output_struct->get_column(i),
                                       converted_struct->get_column(i));
        }
        return;
    }
    if (const auto* output_array = check_and_get_column<ColumnArray>(output)) {
        const auto* converted_array = check_and_get_column<ColumnArray>(converted);
        if (converted_array == nullptr) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Parquet Variant materialization produced an incompatible ARRAY");
        }
        validate_compatible_column(output_array->get_data(), converted_array->get_data());
        return;
    }
    if (const auto* output_map = check_and_get_column<ColumnMap>(output)) {
        const auto* converted_map = check_and_get_column<ColumnMap>(converted);
        if (converted_map == nullptr) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Parquet Variant materialization produced an incompatible MAP");
        }
        validate_compatible_column(output_map->get_keys(), converted_map->get_keys());
        validate_compatible_column(output_map->get_values(), converted_map->get_values());
        return;
    }
    if (check_and_get_column<ColumnVariantV2>(output) != nullptr) {
        if (check_and_get_column<ColumnVariantV2>(converted) == nullptr) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Parquet Variant materialization produced an incompatible column");
        }
        return;
    }
    if (output.get_name() != converted.get_name()) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Parquet Variant materialization produced {} for {} destination",
                        converted.get_name(), output.get_name());
    }
}

bool has_exact_column_shape(const IColumn& output, const IColumn& converted) {
    const auto* output_nullable = check_and_get_column<ColumnNullable>(output);
    const auto* converted_nullable = check_and_get_column<ColumnNullable>(converted);
    if (output_nullable != nullptr || converted_nullable != nullptr) {
        return output_nullable != nullptr && converted_nullable != nullptr &&
               has_exact_column_shape(output_nullable->get_nested_column(),
                                      converted_nullable->get_nested_column());
    }
    if (const auto* output_struct = check_and_get_column<ColumnStruct>(output)) {
        const auto* converted_struct = check_and_get_column<ColumnStruct>(converted);
        if (converted_struct == nullptr ||
            output_struct->tuple_size() != converted_struct->tuple_size()) {
            return false;
        }
        for (size_t i = 0; i < output_struct->tuple_size(); ++i) {
            if (!has_exact_column_shape(output_struct->get_column(i),
                                        converted_struct->get_column(i))) {
                return false;
            }
        }
        return true;
    }
    if (const auto* output_array = check_and_get_column<ColumnArray>(output)) {
        const auto* converted_array = check_and_get_column<ColumnArray>(converted);
        return converted_array != nullptr &&
               has_exact_column_shape(output_array->get_data(), converted_array->get_data());
    }
    if (const auto* output_map = check_and_get_column<ColumnMap>(output)) {
        const auto* converted_map = check_and_get_column<ColumnMap>(converted);
        return converted_map != nullptr &&
               has_exact_column_shape(output_map->get_keys(), converted_map->get_keys()) &&
               has_exact_column_shape(output_map->get_values(), converted_map->get_values());
    }
    if (check_and_get_column<ColumnVariantV2>(output) != nullptr) {
        return check_and_get_column<ColumnVariantV2>(converted) != nullptr;
    }
    return output.get_name() == converted.get_name();
}

void append_materialized_column(MutableColumnPtr& output, ColumnPtr converted) {
    // Validate the complete destination shape before mutation. This preserves atomic failures
    // without copying a full scratch batch, while an empty exact-shape output can adopt the tree.
    validate_compatible_column(*output, *converted);
    if (output->empty() && has_exact_column_shape(*output, *converted)) {
        if (converted->is_exclusive()) {
            // The transformed tree consumed the decoder tree and is recursively exclusive. Keep
            // primitive siblings and their buffers intact instead of recursively COW-cloning them.
            output = converted->assert_mutable();
            return;
        }
        output = IColumn::mutate(std::move(converted));
        return;
    }
    append_compatible_column(*output, *converted);
}

} // namespace

std::shared_ptr<const ParquetColumnSchema> create_variant_state_schema(
        const ParquetColumnSchema& schema, const format::LocalColumnIndex* projection) {
    return std::shared_ptr<const ParquetColumnSchema>(clone_schema(schema, projection));
}

Status materialize_variant_rows(const ParquetColumnSchema& schema, const IColumn& physical,
                                MutableColumnPtr& output,
                                const ParquetColumnReaderProfile& profile) {
    return materialize_variant_rows(schema, physical.get_ptr(), output, profile);
}

Status materialize_variant_rows(const ParquetColumnSchema& schema, ColumnPtr physical,
                                MutableColumnPtr& output,
                                const ParquetColumnReaderProfile& profile) {
    if (!output) {
        return Status::InvalidArgument("Parquet Variant output column is null");
    }
    RETURN_IF_CATCH_EXCEPTION({
        auto converted = build_variant_column(create_variant_state_schema(schema),
                                              std::move(physical), true, profile);
        append_materialized_column(output, std::move(converted));
    });
    return Status::OK();
}

Status materialize_variant_columns(const VariantMaterializationNode& plan, const IColumn& physical,
                                   MutableColumnPtr& output,
                                   const ParquetColumnReaderProfile& profile) {
    return materialize_variant_columns(plan, physical.get_ptr(), output, profile);
}

Status materialize_variant_columns(const VariantMaterializationNode& plan, ColumnPtr physical,
                                   MutableColumnPtr& output,
                                   const ParquetColumnReaderProfile& profile) {
    if (!output) {
        return Status::InvalidArgument("Parquet Variant output column is null");
    }
    RETURN_IF_CATCH_EXCEPTION({
        auto converted = transform_node(plan, std::move(physical), profile);
        append_materialized_column(output, std::move(converted));
    });
    return Status::OK();
}

} // namespace doris::format::parquet
