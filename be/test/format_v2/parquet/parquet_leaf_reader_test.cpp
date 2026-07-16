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

#include "format_v2/parquet/reader/parquet_leaf_reader.h"

#include <arrow/array/builder_binary.h>
#include <cctz/time_zone.h>
#include <gtest/gtest.h>
#include <parquet/api/schema.h>

#include <cmath>
#include <cstring>
#include <functional>
#include <optional>
#include <string>
#include <vector>

#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"

namespace doris::format::parquet {
namespace {

std::shared_ptr<arrow::Array> fixed_binary_array(const std::vector<std::string>& values,
                                                 int byte_width) {
    auto type = arrow::fixed_size_binary(byte_width);
    arrow::FixedSizeBinaryBuilder builder(type, arrow::default_memory_pool());
    for (const auto& value : values) {
        EXPECT_TRUE(builder.Append(reinterpret_cast<const uint8_t*>(value.data())).ok());
    }
    std::shared_ptr<arrow::Array> array;
    EXPECT_TRUE(builder.Finish(&array).ok());
    return array;
}

ParquetLeafReader make_leaf_reader(ParquetTypeDescriptor descriptor, DataTypePtr type) {
    return ParquetLeafReader(nullptr, descriptor, std::move(type), "leaf", nullptr);
}

struct CapturedDecodedView {
    DecodedValueKind value_kind = DecodedValueKind::INT32;
    DecodedTimeUnit time_unit = DecodedTimeUnit::UNKNOWN;
    int64_t row_count = 0;
    int decimal_precision = -1;
    int decimal_scale = -1;
    int fixed_length = -1;
    bool timestamp_is_adjusted_to_utc = false;
    bool enable_strict_mode = false;
    const cctz::time_zone* timezone = nullptr;
    bool null_map_is_null = true;
    std::vector<uint8_t> null_map;
    std::vector<uint8_t> fixed_values;
    std::vector<StringRef> binary_values;
    std::vector<std::string> owned_binary_values;
};

ParquetLeafReader make_spy_leaf_reader(ParquetTypeDescriptor descriptor, DataTypePtr type,
                                       CapturedDecodedView* captured,
                                       const cctz::time_zone* timezone = nullptr,
                                       bool enable_strict_mode = false) {
    auto appender = [captured](MutableColumnPtr&, const DecodedColumnView& view) {
        captured->value_kind = view.value_kind;
        captured->time_unit = view.time_unit;
        captured->row_count = view.row_count;
        captured->decimal_precision = view.decimal_precision;
        captured->decimal_scale = view.decimal_scale;
        captured->fixed_length = view.fixed_length;
        captured->timestamp_is_adjusted_to_utc = view.timestamp_is_adjusted_to_utc;
        captured->enable_strict_mode = view.enable_strict_mode;
        captured->timezone = view.timezone;
        captured->null_map_is_null = view.null_map == nullptr;
        captured->null_map.clear();
        if (view.null_map != nullptr) {
            captured->null_map.assign(view.null_map, view.null_map + view.row_count);
        }
        captured->fixed_values.clear();
        if (view.values != nullptr && view.value_kind == DecodedValueKind::INT64) {
            captured->fixed_values.assign(view.values, view.values + view.row_count * 8);
        } else if (view.values != nullptr && view.value_kind == DecodedValueKind::FLOAT) {
            captured->fixed_values.assign(view.values, view.values + view.row_count * 4);
        } else if (view.values != nullptr && view.value_kind == DecodedValueKind::INT32) {
            captured->fixed_values.assign(view.values, view.values + view.row_count * 4);
        }
        captured->binary_values.clear();
        captured->owned_binary_values.clear();
        if (view.binary_values != nullptr) {
            captured->owned_binary_values.reserve(view.binary_values->size());
            for (const auto& value : *view.binary_values) {
                captured->owned_binary_values.emplace_back(
                        value.data == nullptr ? std::string()
                                              : std::string(value.data, value.size));
            }
            captured->binary_values.reserve(captured->owned_binary_values.size());
            for (const auto& value : captured->owned_binary_values) {
                captured->binary_values.emplace_back(value.data(), value.size());
            }
        }
        return Status::OK();
    };
    return ParquetLeafReader(nullptr, descriptor, std::move(type), "leaf", nullptr, {}, timezone,
                             enable_strict_mode, std::move(appender));
}

} // namespace

struct ParquetLeafReaderTestAccess {
    static ParquetLeafBatch make_fixed_batch(const std::vector<int16_t>& def_levels,
                                             const std::vector<int16_t>& rep_levels,
                                             const std::vector<int32_t>& values,
                                             bool read_dense_for_nullable = false) {
        ParquetLeafBatch batch;
        batch._value_kind = DecodedValueKind::INT32;
        batch._consumed_level_count = static_cast<int64_t>(def_levels.size());
        batch._decoded_level_count = static_cast<int64_t>(def_levels.size());
        batch._values_written = static_cast<int64_t>(values.size());
        batch._def_levels = def_levels.data();
        batch._rep_levels = rep_levels.data();
        batch._fixed_values = reinterpret_cast<const uint8_t*>(values.data());
        batch._read_dense_for_nullable = read_dense_for_nullable;
        return batch;
    }

    static Status build_nested_batch(const ParquetLeafReader& reader,
                                     const ParquetLeafBatch& leaf_batch, int64_t records_read,
                                     int16_t value_slot_definition_level,
                                     int16_t value_slot_repetition_level,
                                     ParquetNestedScalarBatch* nested_batch) {
        return reader.build_nested_batch_from_leaf_batch(leaf_batch, records_read,
                                                         value_slot_definition_level, nested_batch,
                                                         value_slot_repetition_level);
    }

    static size_t binary_value_size(const ParquetLeafReader& reader) {
        return reader._binary_values.size();
    }

    static size_t binary_value_capacity(const ParquetLeafReader& reader) {
        return reader._binary_values.capacity();
    }
};

std::shared_ptr<::parquet::ColumnDescriptor> int32_column_descriptor(int16_t max_definition_level,
                                                                     int16_t max_repetition_level) {
    auto node = ::parquet::schema::PrimitiveNode::Make("leaf", ::parquet::Repetition::OPTIONAL,
                                                       ::parquet::Type::INT32);
    return std::make_shared<::parquet::ColumnDescriptor>(node, max_definition_level,
                                                         max_repetition_level);
}

ParquetLeafReader make_nested_leaf_reader(
        const std::shared_ptr<::parquet::ColumnDescriptor>& descriptor, DataTypePtr type) {
    ParquetTypeDescriptor type_descriptor;
    type_descriptor.physical_type = ::parquet::Type::INT32;
    type_descriptor.doris_type = type;
    return ParquetLeafReader(descriptor.get(), type_descriptor, std::move(type), "nested_leaf",
                             nullptr);
}

TEST(ParquetLeafReaderTest, DenseNullableFixedValuesAreSpacedBeforeSerde) {
    ParquetTypeDescriptor descriptor;
    descriptor.physical_type = ::parquet::Type::INT32;
    auto type = make_nullable(std::make_shared<DataTypeInt32>());
    auto reader = make_leaf_reader(descriptor, type);

    const std::vector<int32_t> compact_values = {10, 30, 50};
    ParquetLeafBatch batch;
    batch._value_kind = DecodedValueKind::INT32;
    batch._fixed_values = reinterpret_cast<const uint8_t*>(compact_values.data());
    batch._values_written = compact_values.size();
    batch._read_dense_for_nullable = true;

    const NullMap null_map = {0, 1, 0, 1, 0};
    auto column = type->create_column();
    auto status = reader.append_values(batch, 5, &null_map, column);
    ASSERT_TRUE(status.ok()) << status;

    const auto& nullable = assert_cast<const ColumnNullable&>(*column);
    ASSERT_EQ(nullable.size(), 5);
    EXPECT_FALSE(nullable.is_null_at(0));
    EXPECT_TRUE(nullable.is_null_at(1));
    EXPECT_FALSE(nullable.is_null_at(2));
    EXPECT_TRUE(nullable.is_null_at(3));
    EXPECT_FALSE(nullable.is_null_at(4));
    const auto& nested = assert_cast<const ColumnInt32&>(nullable.get_nested_column());
    EXPECT_EQ(nested.get_element(0), 10);
    EXPECT_EQ(nested.get_element(2), 30);
    EXPECT_EQ(nested.get_element(4), 50);
}

TEST(ParquetLeafReaderTest, DenseNullableFixedValuesRejectCountMismatch) {
    ParquetTypeDescriptor descriptor;
    descriptor.physical_type = ::parquet::Type::INT32;
    auto type = make_nullable(std::make_shared<DataTypeInt32>());
    auto reader = make_leaf_reader(descriptor, type);

    const std::vector<int32_t> compact_values = {10, 30};
    ParquetLeafBatch batch;
    batch._value_kind = DecodedValueKind::INT32;
    batch._fixed_values = reinterpret_cast<const uint8_t*>(compact_values.data());
    batch._values_written = compact_values.size();
    batch._read_dense_for_nullable = true;

    const NullMap null_map = {0, 1, 0, 1, 0};
    auto column = type->create_column();
    auto status = reader.append_values(batch, 5, &null_map, column);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("Invalid dense nullable parquet values"), std::string::npos);
}

TEST(ParquetLeafReaderTest, Float16BinaryValuesAreConvertedToFloat) {
    ParquetTypeDescriptor descriptor;
    descriptor.physical_type = ::parquet::Type::FIXED_LEN_BYTE_ARRAY;
    descriptor.extra_type_info = ParquetExtraTypeInfo::FLOAT16;
    descriptor.fixed_length = 2;
    auto type = std::make_shared<DataTypeFloat32>();
    auto reader = make_leaf_reader(descriptor, type);

    auto half = [](uint16_t value) {
        std::string bytes(sizeof(value), '\0');
        memcpy(bytes.data(), &value, sizeof(value));
        return bytes;
    };

    ParquetLeafBatch batch;
    batch._value_kind = DecodedValueKind::FIXED_BINARY;
    batch._binary_chunks = {fixed_binary_array(
            {half(0x0000), half(0x8000), half(0x3E00), half(0x0001), half(0x7E00)}, 2)};
    batch._values_written = 5;

    auto column = type->create_column();
    auto status = reader.append_values(batch, 5, nullptr, column);
    ASSERT_TRUE(status.ok()) << status;

    const auto& floats = assert_cast<const ColumnFloat32&>(*column);
    ASSERT_EQ(floats.size(), 5);
    EXPECT_FLOAT_EQ(floats.get_element(0), 0.0F);
    EXPECT_TRUE(std::signbit(floats.get_element(1)));
    EXPECT_FLOAT_EQ(floats.get_element(2), 1.5F);
    EXPECT_NEAR(floats.get_element(3), 5.9604645e-8F, 1e-12F);
    EXPECT_TRUE(std::isnan(floats.get_element(4)));
}

TEST(ParquetLeafReaderTest, BinaryDenseNullableValuesAreSpacedWithNullRefs) {
    ParquetTypeDescriptor descriptor;
    descriptor.physical_type = ::parquet::Type::BYTE_ARRAY;
    auto type = make_nullable(std::make_shared<DataTypeString>());
    auto reader = make_leaf_reader(descriptor, type);

    arrow::BinaryBuilder builder;
    ASSERT_TRUE(builder.Append("aa").ok());
    ASSERT_TRUE(builder.Append("cc").ok());
    ASSERT_TRUE(builder.Append("ee").ok());
    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(builder.Finish(&array).ok());

    ParquetLeafBatch batch;
    batch._value_kind = DecodedValueKind::BINARY;
    batch._binary_chunks = {array};
    batch._values_written = 3;
    batch._read_dense_for_nullable = true;

    const NullMap null_map = {0, 1, 0, 1, 0};
    auto column = type->create_column();
    auto status = reader.append_values(batch, 5, &null_map, column);
    ASSERT_TRUE(status.ok()) << status;

    EXPECT_EQ(ParquetLeafReaderTestAccess::binary_value_size(reader), 0);
    EXPECT_GE(ParquetLeafReaderTestAccess::binary_value_capacity(reader), 5);

    const auto& nullable = assert_cast<const ColumnNullable&>(*column);
    const auto& strings = assert_cast<const ColumnString&>(nullable.get_nested_column());
    ASSERT_EQ(nullable.size(), 5);
    EXPECT_EQ(strings.get_data_at(0).to_string(), "aa");
    EXPECT_TRUE(nullable.is_null_at(1));
    EXPECT_EQ(strings.get_data_at(2).to_string(), "cc");
    EXPECT_TRUE(nullable.is_null_at(3));
    EXPECT_EQ(strings.get_data_at(4).to_string(), "ee");
}

TEST(ParquetLeafReaderTest, ReleaseBinaryChunksDropsPayloadAndRetainsVectorCapacity) {
    ParquetLeafBatch batch;
    batch._binary_chunks.reserve(4);
    auto array = fixed_binary_array({"payload"}, 7);
    std::weak_ptr<arrow::Array> payload = array;
    batch._binary_chunks.push_back(array);
    array.reset();

    const auto capacity = batch.binary_chunks().capacity();
    ASSERT_FALSE(payload.expired());
    batch.release_binary_chunks();

    EXPECT_TRUE(payload.expired());
    EXPECT_TRUE(batch.binary_chunks().empty());
    EXPECT_EQ(batch.binary_chunks().capacity(), capacity);
}

TEST(ParquetLeafReaderTest, BinaryDenseNullableRejectsCountMismatch) {
    ParquetTypeDescriptor descriptor;
    descriptor.physical_type = ::parquet::Type::BYTE_ARRAY;
    auto type = make_nullable(std::make_shared<DataTypeString>());
    auto reader = make_leaf_reader(descriptor, type);

    arrow::BinaryBuilder builder;
    ASSERT_TRUE(builder.Append("only_one").ok());
    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(builder.Finish(&array).ok());

    ParquetLeafBatch batch;
    batch._value_kind = DecodedValueKind::BINARY;
    batch._binary_chunks = {array};
    batch._values_written = 1;
    batch._read_dense_for_nullable = true;

    const NullMap null_map = {0, 1, 0};
    auto column = type->create_column();
    auto status = reader.append_values(batch, 3, &null_map, column);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("Invalid dense nullable parquet binary values"),
              std::string::npos);
    EXPECT_EQ(ParquetLeafReaderTestAccess::binary_value_size(reader), 0);
}

TEST(ParquetLeafReaderTest, DecodedColumnViewCarriesDescriptorSessionAndNullMapFields) {
    ParquetTypeDescriptor descriptor;
    descriptor.physical_type = ::parquet::Type::INT64;
    descriptor.time_unit = ParquetTimeUnit::NANOS;
    descriptor.decimal_precision = 18;
    descriptor.decimal_scale = 4;
    descriptor.fixed_length = 12;
    descriptor.timestamp_is_adjusted_to_utc = true;
    auto type = make_nullable(std::make_shared<DataTypeInt64>());
    cctz::time_zone shanghai;
    ASSERT_TRUE(cctz::load_time_zone("Asia/Shanghai", &shanghai));

    CapturedDecodedView captured;
    auto reader = make_spy_leaf_reader(descriptor, type, &captured, &shanghai, true);
    const std::vector<int64_t> values = {100, 200, 300};
    ParquetLeafBatch batch;
    batch._value_kind = DecodedValueKind::INT64;
    batch._fixed_values = reinterpret_cast<const uint8_t*>(values.data());
    batch._values_written = values.size();

    const NullMap null_map = {0, 1, 0};
    auto column = type->create_column();
    ASSERT_TRUE(reader.append_values(batch, 3, &null_map, column).ok());
    EXPECT_EQ(captured.value_kind, DecodedValueKind::INT64);
    EXPECT_EQ(captured.time_unit, DecodedTimeUnit::NANOS);
    EXPECT_EQ(captured.row_count, 3);
    EXPECT_EQ(captured.decimal_precision, 18);
    EXPECT_EQ(captured.decimal_scale, 4);
    EXPECT_EQ(captured.fixed_length, 12);
    EXPECT_TRUE(captured.timestamp_is_adjusted_to_utc);
    EXPECT_TRUE(captured.enable_strict_mode);
    EXPECT_EQ(captured.timezone, &shanghai);
    EXPECT_FALSE(captured.null_map_is_null);
    EXPECT_EQ(captured.null_map, std::vector<uint8_t>({0, 1, 0}));

    auto required_column = type->create_column();
    ASSERT_TRUE(reader.append_values(batch, 3, nullptr, required_column).ok());
    EXPECT_TRUE(captured.null_map_is_null);

    const NullMap empty_null_map;
    ASSERT_TRUE(reader.append_values(batch, 3, &empty_null_map, required_column).ok());
    EXPECT_TRUE(captured.null_map_is_null);
}

TEST(ParquetLeafReaderTest, DecodedColumnViewCapturesBinaryFixedLengthAndFloat16Override) {
    ParquetTypeDescriptor binary_descriptor;
    binary_descriptor.physical_type = ::parquet::Type::FIXED_LEN_BYTE_ARRAY;
    binary_descriptor.fixed_length = 4;
    auto type = std::make_shared<DataTypeString>();

    CapturedDecodedView binary_view;
    auto binary_reader = make_spy_leaf_reader(binary_descriptor, type, &binary_view);
    ParquetLeafBatch binary_batch;
    binary_batch._value_kind = DecodedValueKind::FIXED_BINARY;
    binary_batch._binary_chunks = {fixed_binary_array({"abcd", "wxyz"}, 4)};
    binary_batch._values_written = 2;
    auto binary_column = type->create_column();
    ASSERT_TRUE(binary_reader.append_values(binary_batch, 2, nullptr, binary_column).ok());
    EXPECT_EQ(binary_view.value_kind, DecodedValueKind::FIXED_BINARY);
    EXPECT_EQ(binary_view.fixed_length, 4);
    ASSERT_EQ(binary_view.owned_binary_values.size(), 2);
    EXPECT_EQ(binary_view.owned_binary_values[0], "abcd");
    EXPECT_EQ(binary_view.owned_binary_values[1], "wxyz");

    ParquetTypeDescriptor float16_descriptor;
    float16_descriptor.physical_type = ::parquet::Type::FIXED_LEN_BYTE_ARRAY;
    float16_descriptor.extra_type_info = ParquetExtraTypeInfo::FLOAT16;
    float16_descriptor.fixed_length = 2;
    CapturedDecodedView float16_view;
    auto float16_reader = make_spy_leaf_reader(float16_descriptor,
                                               std::make_shared<DataTypeFloat32>(), &float16_view);
    auto half = [](uint16_t value) {
        std::string bytes(sizeof(value), '\0');
        memcpy(bytes.data(), &value, sizeof(value));
        return bytes;
    };
    ParquetLeafBatch float16_batch;
    float16_batch._value_kind = DecodedValueKind::FIXED_BINARY;
    float16_batch._binary_chunks = {fixed_binary_array({half(0x3E00), half(0x4000)}, 2)};
    float16_batch._values_written = 2;
    auto float16_column = std::make_shared<DataTypeFloat32>()->create_column();
    ASSERT_TRUE(float16_reader.append_values(float16_batch, 2, nullptr, float16_column).ok());
    EXPECT_EQ(float16_view.value_kind, DecodedValueKind::FLOAT);
    ASSERT_EQ(float16_view.fixed_values.size(), sizeof(float) * 2);
    const auto* floats = reinterpret_cast<const float*>(float16_view.fixed_values.data());
    EXPECT_FLOAT_EQ(floats[0], 1.5F);
    EXPECT_FLOAT_EQ(floats[1], 2.0F);
}

TEST(ParquetLeafReaderTest, NestedBatchValueLayoutLevels) {
    auto descriptor = int32_column_descriptor(2, 1);
    auto reader = make_nested_leaf_reader(descriptor, std::make_shared<DataTypeInt32>());
    const std::vector<int16_t> def_levels = {2, 2, 2};
    const std::vector<int16_t> rep_levels = {0, 1, 0};
    const std::vector<int32_t> values = {10, 20, 30};
    const auto leaf_batch =
            ParquetLeafReaderTestAccess::make_fixed_batch(def_levels, rep_levels, values);

    ParquetNestedScalarBatch nested_batch;
    auto status = ParquetLeafReaderTestAccess::build_nested_batch(reader, leaf_batch, 2, 2, 1,
                                                                  &nested_batch);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(nested_batch.records_read, 2);
    EXPECT_EQ(nested_batch.levels_written, 3);
    EXPECT_EQ(nested_batch.value_indices, std::vector<int64_t>({0, 1, 2}));
    const auto& nested_values = assert_cast<const ColumnInt32&>(*nested_batch.values_column);
    ASSERT_EQ(nested_values.size(), 3);
    EXPECT_EQ(nested_values.get_element(0), 10);
    EXPECT_EQ(nested_values.get_element(2), 30);
}

TEST(ParquetLeafReaderTest, NestedBatchValueLayoutValueSlots) {
    auto descriptor = int32_column_descriptor(2, 1);
    auto reader = make_nested_leaf_reader(descriptor, std::make_shared<DataTypeInt32>());
    const std::vector<int16_t> def_levels = {2, 1, 2, 0};
    const std::vector<int16_t> rep_levels = {0, 1, 0, 0};
    const std::vector<int32_t> values = {10, 777, 30};
    const auto leaf_batch =
            ParquetLeafReaderTestAccess::make_fixed_batch(def_levels, rep_levels, values);

    ParquetNestedScalarBatch nested_batch;
    auto status = ParquetLeafReaderTestAccess::build_nested_batch(reader, leaf_batch, 3, 1, 1,
                                                                  &nested_batch);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(nested_batch.value_indices, std::vector<int64_t>({0, -1, 2, -1}));
}

TEST(ParquetLeafReaderTest, NestedBatchValueLayoutLeafValues) {
    auto descriptor = int32_column_descriptor(2, 1);
    auto reader = make_nested_leaf_reader(descriptor, std::make_shared<DataTypeInt32>());
    const std::vector<int16_t> def_levels = {2, 1, 2, 0};
    const std::vector<int16_t> rep_levels = {0, 1, 0, 0};
    const std::vector<int32_t> values = {10, 30};
    const auto leaf_batch =
            ParquetLeafReaderTestAccess::make_fixed_batch(def_levels, rep_levels, values);

    ParquetNestedScalarBatch nested_batch;
    auto status = ParquetLeafReaderTestAccess::build_nested_batch(reader, leaf_batch, 3, 1, 1,
                                                                  &nested_batch);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(nested_batch.value_indices, std::vector<int64_t>({0, -1, 1, -1}));
}

TEST(ParquetLeafReaderTest, NestedBatchValueLayoutPayloadSlots) {
    auto descriptor = int32_column_descriptor(2, 1);
    auto reader = make_nested_leaf_reader(descriptor, std::make_shared<DataTypeInt32>());
    const std::vector<int16_t> def_levels = {1, 2, 0, 2};
    const std::vector<int16_t> rep_levels = {0, 0, 0, 0};
    const std::vector<int32_t> values = {777, 10, 30};
    const auto leaf_batch =
            ParquetLeafReaderTestAccess::make_fixed_batch(def_levels, rep_levels, values);

    ParquetNestedScalarBatch nested_batch;
    auto status = ParquetLeafReaderTestAccess::build_nested_batch(reader, leaf_batch, 4, 2, 1,
                                                                  &nested_batch);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(nested_batch.value_indices, std::vector<int64_t>({-1, 1, -1, 2}));
}

TEST(ParquetLeafReaderTest, NestedBatchRejectsMismatchedValueLayout) {
    auto descriptor = int32_column_descriptor(2, 1);
    auto reader = make_nested_leaf_reader(descriptor, std::make_shared<DataTypeInt32>());
    const std::vector<int16_t> def_levels = {2, 0, 2, 0};
    const std::vector<int16_t> rep_levels = {0, 0, 0, 0};
    const std::vector<int32_t> values = {10, 20, 30};
    const auto leaf_batch =
            ParquetLeafReaderTestAccess::make_fixed_batch(def_levels, rep_levels, values);

    ParquetNestedScalarBatch nested_batch;
    const auto status = ParquetLeafReaderTestAccess::build_nested_batch(reader, leaf_batch, 4, 2, 1,
                                                                        &nested_batch);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("inconsistent value count"), std::string::npos);
}

TEST(ParquetLeafReaderTest, NestedBatchRejectsDenseNullable) {
    auto descriptor = int32_column_descriptor(1, 0);
    auto reader =
            make_nested_leaf_reader(descriptor, make_nullable(std::make_shared<DataTypeInt32>()));
    const std::vector<int16_t> def_levels = {1};
    const std::vector<int16_t> rep_levels = {0};
    const std::vector<int32_t> values = {10};
    const auto leaf_batch =
            ParquetLeafReaderTestAccess::make_fixed_batch(def_levels, rep_levels, values, true);

    ParquetNestedScalarBatch nested_batch;
    const auto status = ParquetLeafReaderTestAccess::build_nested_batch(reader, leaf_batch, 1, 0, 0,
                                                                        &nested_batch);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("Dense nullable parquet nested reader is not supported"),
              std::string::npos);
}

} // namespace doris::format::parquet
