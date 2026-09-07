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

#include "core/data_type/data_type_uuid.h"

#include <arrow/array.h>
#include <arrow/builder.h>
#include <gen_cpp/data.pb.h>
#include <gen_cpp/types.pb.h>
#include <gtest/gtest.h>

#include <orc/Vector.hh>
#include <string>
#include <tuple>

#include "agent/be_exec_version_manager.h"
#include "core/block/block.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type_serde/data_type_uuid_serde.h"
#include "core/value/uuid_value.h"
#include "util/jsonb_document_cast.h"
#include "util/jsonb_writer.h"

namespace doris {

class DataTypeUUIDTest : public ::testing::Test {
protected:
    DataTypePtr type = std::make_shared<DataTypeUUID>();
    DataTypeSerDeSPtr serde = type->get_serde();

    static ColumnUUID::MutablePtr boundary_column() {
        auto column = ColumnUUID::create();
        for (const std::string& text :
             {"00000000-0000-0000-0000-000000000000", "00112233-4455-6677-8899-aabbccddeeff",
              "7fffffff-ffff-ffff-ffff-ffffffffffff", "80000000-0000-0000-0000-000000000000",
              "ffffffff-ffff-ffff-ffff-ffffffffffff"}) {
            UUIDValueType value;
            EXPECT_TRUE(UUIDValue::from_string(value, text));
            column->insert_value(value);
        }
        return column;
    }

    static ColumnString::MutablePtr mixed_strings() {
        auto strings = ColumnString::create();
        strings->insert_data("00112233445566778899AABBCCDDEEFF", 32);
        strings->insert_data("invalid", 7);
        strings->insert_data("ffffffff-ffff-ffff-ffff-ffffffffffff", 36);
        return strings;
    }
};

TEST_F(DataTypeUUIDTest, MetadataAndDefault) {
    EXPECT_EQ(type->get_primitive_type(), TYPE_UUID);
    EXPECT_EQ(type->get_storage_field_type(), FieldType::OLAP_FIELD_TYPE_UUID);
    EXPECT_EQ(type->get_family_name(), "UUID");
    EXPECT_EQ(type->get_size_of_value_in_memory(), sizeof(UUIDValueType));
    EXPECT_TRUE(type->equals(*std::make_shared<DataTypeUUID>()));

    const auto from_primitive = DataTypeFactory::instance().create_data_type(TYPE_UUID, false);
    const auto from_storage =
            DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_UUID, 0, 0);
    EXPECT_TRUE(type->equals(*from_primitive));
    EXPECT_TRUE(type->equals(*from_storage));

    auto column = type->create_column_const_with_default_value(1);
    EXPECT_EQ(type->to_string(*column, 0), "00000000-0000-0000-0000-000000000000");
}

TEST_F(DataTypeUUIDTest, LiteralAndTextSerde) {
    TExprNode node;
    node.node_type = TExprNodeType::UUID_LITERAL;
    node.uuid_literal.value = "550E8400E29B41D4A716446655440000";
    node.__isset.uuid_literal = true;
    const auto field = type->get_field(node);
    EXPECT_EQ(UUIDValue::to_string(field.get<TYPE_UUID>()), "550e8400-e29b-41d4-a716-446655440000");

    auto column = type->create_column();
    StringRef input {"550E8400E29B41D4A716446655440000"};
    EXPECT_TRUE(serde->from_string(input, *column, {}).ok());
    EXPECT_EQ(type->to_string(*column, 0), "550e8400-e29b-41d4-a716-446655440000");

    StringRef invalid {"550e8400-e29b-41d4-a716-44665544000g"};
    EXPECT_FALSE(serde->from_string(invalid, *column, {}).ok());
    EXPECT_EQ(column->size(), 1);
}

TEST_F(DataTypeUUIDTest, ProtobufRoundTripAndValidation) {
    auto source = type->create_column();
    auto& source_data = assert_cast<ColumnUUID&>(*source).get_data();
    UUIDValueType first;
    UUIDValueType second;
    ASSERT_TRUE(UUIDValue::from_string(first, "00000000-0000-0000-0000-000000000001"));
    ASSERT_TRUE(UUIDValue::from_string(second, "ffffffff-ffff-ffff-ffff-ffffffffffff"));
    source_data.push_back(first);
    source_data.push_back(second);

    PValues values;
    ASSERT_TRUE(serde->write_column_to_pb(*source, values, 0, source->size()).ok());
    EXPECT_EQ(values.type().id(), PGenericType::UUID);
    ASSERT_EQ(values.bytes_value_size(), 2);
    EXPECT_EQ(values.bytes_value(0).size(), sizeof(UUIDValueType));

    auto target = type->create_column();
    ASSERT_TRUE(serde->read_column_from_pb(*target, values).ok());
    EXPECT_EQ(assert_cast<ColumnUUID&>(*target).get_data(), source_data);

    PValues malformed;
    malformed.add_bytes_value("short");
    EXPECT_FALSE(serde->read_column_from_pb(*target, malformed).ok());
    EXPECT_EQ(target->size(), source->size());
}

TEST_F(DataTypeUUIDTest, JsonbScalarAndVector) {
    const std::string canonical = "00112233-4455-6677-8899-aabbccddeeff";
    for (bool strict : {false, true}) {
        CastParameters params;
        params.is_strict = strict;
        auto documents = ColumnString::create();
        for (const std::string& text :
             {canonical, std::string("00112233445566778899AABBCCDDEEFF")}) {
            JsonbWriter writer;
            writer.writeStartString();
            writer.writeString(text.data(), text.size());
            writer.writeEndString();
            auto scalar = type->create_column();
            ASSERT_TRUE(
                    serde->deserialize_column_from_jsonb(*scalar, writer.getValue(), params).ok());
            EXPECT_EQ(type->to_string(*scalar, 0), canonical);
            documents->insert_data(writer.getOutput()->getBuffer(), writer.getOutput()->getSize());
        }
        auto output = ColumnNullable::create(type->create_column(), ColumnUInt8::create());
        ASSERT_TRUE(serde->deserialize_column_from_jsonb_vector(*output, *documents, params).ok());
        ASSERT_EQ(output->size(), 2);
        for (size_t row = 0; row < 2; ++row) {
            EXPECT_FALSE(output->is_null_at(row));
            EXPECT_EQ(type->to_string(output->get_nested_column(), row), canonical);
        }
        JsonbWriter invalid;
        invalid.writeStartString();
        invalid.writeString("invalid", 7);
        invalid.writeEndString();
        auto scalar = type->create_column();
        EXPECT_FALSE(
                serde->deserialize_column_from_jsonb(*scalar, invalid.getValue(), params).ok());
        EXPECT_EQ(scalar->size(), 0);
        documents->insert_data(invalid.getOutput()->getBuffer(), invalid.getOutput()->getSize());
        output = ColumnNullable::create(type->create_column(), ColumnUInt8::create());
        auto status = serde->deserialize_column_from_jsonb_vector(*output, *documents, params);
        EXPECT_EQ(status.ok(), !strict);
        if (!strict) {
            EXPECT_TRUE(output->is_null_at(2));
            EXPECT_FALSE(output->is_null_at(0));
        }
    }
}

TEST_F(DataTypeUUIDTest, ColumnSelectionOrderingHashAndArena) {
    auto source = boundary_column();
    for (size_t i = 1; i < source->size(); ++i) {
        EXPECT_LT(source->compare_at(i - 1, i, *source, 1), 0);
    }
    auto selected = source->filter(IColumn::Filter {0, 1, 0, 1, 1}, -1);
    ASSERT_EQ(selected->size(), 3);
    EXPECT_EQ((*selected)[0], (*source)[1]);
    EXPECT_EQ((*selected)[1], (*source)[3]);
    auto reversed = source->permute(IColumn::Permutation {4, 3, 2, 1, 0}, 0);
    for (size_t i = 0; i < source->size(); ++i) {
        EXPECT_EQ((*reversed)[i], (*source)[4 - i]);
    }
    auto range = source->clone_empty();
    range->insert_range_from(*source, 1, 3);
    EXPECT_EQ((*range)[0], (*source)[1]);
    EXPECT_EQ((*range)[2], (*source)[3]);
}

TEST_F(DataTypeUUIDTest, HashArenaAndCopyOnWrite) {
    auto source = boundary_column();
    Arena arena;
    auto restored = source->clone_empty();
    for (size_t i = 0; i < source->size(); ++i) {
        const char* begin = nullptr;
        auto bytes = source->serialize_value_into_arena(i, arena, begin);
        EXPECT_EQ(bytes.size, 16);
        EXPECT_EQ(restored->deserialize_and_insert_from_arena(bytes.data), bytes.data + bytes.size);
        EXPECT_EQ((*restored)[i], (*source)[i]);
        SipHash before;
        SipHash after;
        source->update_hash_with_value(i, before);
        restored->update_hash_with_value(i, after);
        EXPECT_EQ(before.get64(), after.get64());
    }
    ColumnPtr shared = source->get_ptr();
    auto detached = IColumn::mutate(shared);
    detached->insert_default();
    EXPECT_EQ(source->size(), 5);
    EXPECT_EQ(detached->size(), 6);
}

class DataTypeUUIDBlockTest
        : public DataTypeUUIDTest,
          public ::testing::WithParamInterface<
                  std::tuple<size_t, bool, bool, int, segment_v2::CompressionTypePB>> {};

TEST_P(DataTypeUUIDBlockTest, SerializationRoundTrip) {
    const auto [rows, nullable, constant, version, compression] = GetParam();
    auto data = ColumnUUID::create();
    const UUIDValueType high = UUIDValueType {1} << 127;
    for (size_t row = 0; row < rows; ++row) {
        data->insert_value(high + row);
    }
    DataTypePtr data_type = nullable ? make_nullable(type) : type;
    ColumnPtr column;
    if (nullable) {
        auto nulls = ColumnUInt8::create(rows, 0);
        for (size_t row = 0; row < rows; ++row) {
            nulls->get_data()[row] = row % 3 == 0;
        }
        column = ColumnNullable::create(std::move(data), std::move(nulls));
    } else {
        column = std::move(data);
    }
    ColumnPtr input = constant ? data_type->create_column_const_with_default_value(rows) : column;
    Block original {{input, data_type, "uuid"}};
    PBlock wire;
    size_t uncompressed = 0, compressed = 0;
    int64_t elapsed = 0;
    ASSERT_TRUE(
            original.serialize(version, &wire, &uncompressed, &compressed, &elapsed, compression)
                    .ok());
    Block restored;
    ASSERT_TRUE(restored.deserialize(wire, &uncompressed, &elapsed).ok());
    ASSERT_EQ(restored.columns(), 1);
    ASSERT_EQ(restored.rows(), rows);
    EXPECT_TRUE(restored.get_by_position(0).type->equals(*data_type));
    for (size_t row = 0; row < rows; ++row) {
        EXPECT_EQ((*restored.get_by_position(0).column)[row], (*input)[row]);
    }
}

INSTANTIATE_TEST_SUITE_P(
        UUID, DataTypeUUIDBlockTest,
        ::testing::Combine(::testing::Values(0U, 1U, 4097U), ::testing::Bool(), ::testing::Bool(),
                           ::testing::Values(USE_NEW_FIXED_OBJECT_SERIALIZATION_VERSION - 1,
                                             BeExecVersionManager::get_newest_version()),
                           ::testing::Values(segment_v2::CompressionTypePB::NO_COMPRESSION,
                                             segment_v2::CompressionTypePB::ZSTD)));

TEST_F(DataTypeUUIDTest, BatchParsingNullMapAndColumnReuse) {
    auto strings = mixed_strings();
    auto output = ColumnNullable::create(type->create_column(), ColumnUInt8::create());
    for (int reuse = 0; reuse < 2; ++reuse) {
        ASSERT_TRUE(serde->from_string_batch(*strings, *output, {}).ok());
        ASSERT_EQ(output->size(), 3);
        EXPECT_FALSE(output->is_null_at(0));
        EXPECT_TRUE(output->is_null_at(1));
        EXPECT_FALSE(output->is_null_at(2));
        EXPECT_EQ(type->to_string(output->get_nested_column(), 0),
                  "00112233-4455-6677-8899-aabbccddeeff");
        EXPECT_EQ(type->to_string(output->get_nested_column(), 2),
                  "ffffffff-ffff-ffff-ffff-ffffffffffff");
    }
}

TEST_F(DataTypeUUIDTest, StrictBatchParsingSkipsOnlyNullInput) {
    auto strings = mixed_strings();
    auto strict = type->create_column();
    EXPECT_FALSE(serde->from_string_strict_mode_batch(*strings, *strict, {}, nullptr).ok());
    const NullMap nulls {0, 1, 0};
    ASSERT_TRUE(serde->from_string_strict_mode_batch(*strings, *strict, {}, nulls.data()).ok());
    EXPECT_EQ(type->to_string(*strict, 2), "ffffffff-ffff-ffff-ffff-ffffffffffff");
}

TEST_F(DataTypeUUIDTest, ArrowRangeNullsAndAppend) {
    auto source = boundary_column();
    NullMap nulls {0, 0, 1, 0, 0};
    cctz::time_zone utc;
    ASSERT_TRUE(cctz::load_time_zone("UTC", &utc));
    arrow::StringBuilder builder;
    ASSERT_TRUE(serde->write_column_to_arrow(*source, &nulls, &builder, 1, 4, utc).ok());
    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(builder.Finish(&array).ok());
    ASSERT_EQ(array->length(), 3);
    EXPECT_TRUE(array->IsNull(1));
    auto restored = type->create_column();
    ASSERT_TRUE(serde->read_column_from_arrow(*restored, array.get(), 0, 3, utc).ok());
    EXPECT_EQ((*restored)[0], (*source)[1]);
    EXPECT_EQ((*restored)[2], (*source)[3]);
    // Append a nonzero range into an existing destination without losing earlier rows.
    ASSERT_TRUE(serde->read_column_from_arrow(*restored, array.get(), 2, 3, utc).ok());
    EXPECT_EQ(restored->size(), 4);
    EXPECT_EQ((*restored)[3], (*source)[3]);
}

TEST_F(DataTypeUUIDTest, OrcRangeNullsAndArenaOwnership) {
    auto source = boundary_column();
    NullMap nulls {0, 0, 1, 0, 0};
    Arena arena;
    auto batch = std::make_unique<orc::StringVectorBatch>(5, *orc::getDefaultPool());
    batch->hasNulls = true;
    for (size_t row = 0; row < source->size(); ++row) {
        batch->notNull[row] = !nulls[row];
    }
    ASSERT_TRUE(
            serde->write_column_to_orc("UTC", *source, &nulls, batch.get(), 1, 5, arena, {}).ok());
    source->clear();
    EXPECT_EQ(batch->numElements, 4);
    EXPECT_EQ(std::string(batch->data[1], batch->length[1]),
              "00112233-4455-6677-8899-aabbccddeeff");
    EXPECT_EQ(std::string(batch->data[4], batch->length[4]),
              "ffffffff-ffff-ffff-ffff-ffffffffffff");
    EXPECT_EQ(batch->notNull[2], 0);
}

TEST_F(DataTypeUUIDTest, CompactBinaryKeepsUuidStorageTagAndBits) {
    auto column = boundary_column();
    ColumnString::Chars bytes;
    serde->write_one_cell_to_binary(*column, bytes, 4);
    ASSERT_EQ(bytes.size(), 1 + sizeof(UUIDValueType));
    EXPECT_EQ(bytes[0], static_cast<uint8_t>(FieldType::OLAP_FIELD_TYPE_UUID));
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(bytes.data() + 1), sizeof(UUIDValueType)),
              column->get_data_at(4).to_string());
}

TEST_F(DataTypeUUIDTest, HiveNestedTextUsesDelimitersWithoutJsonQuotes) {
    auto nested_type = std::make_shared<DataTypeArray>(
            make_nullable(std::make_shared<DataTypeArray>(make_nullable(type))));
    auto nested_serde = nested_type->get_serde();
    auto column = nested_type->create_column();
    std::string json =
            R"([["00112233-4455-6677-8899-aabbccddeeff", null], ["ffffffff-ffff-ffff-ffff-ffffffffffff"]])";
    Slice input(json);
    DataTypeSerDe::FormatOptions options;
    ASSERT_TRUE(nested_serde->deserialize_one_cell_from_json(*column, input, options).ok());
    options.collection_delim = '\002';
    options.map_key_delim = '\003';
    ColumnString text;
    VectorBufferWriter writer(text);
    ASSERT_TRUE(nested_serde->serialize_one_cell_to_hive_text(*column, 0, writer, options).ok());
    writer.commit();
    EXPECT_EQ(
            text.get_data_at(0).to_string(),
            "00112233-4455-6677-8899-aabbccddeeff\003\\N\002ffffffff-ffff-ffff-ffff-ffffffffffff");
    auto restored = nested_type->create_column();
    auto bytes = text.get_data_at(0);
    Slice hive(bytes.data, bytes.size);
    ASSERT_TRUE(nested_serde->deserialize_one_cell_from_hive_text(*restored, hive, options).ok());
    EXPECT_EQ((*restored)[0], (*column)[0]);
}

TEST_F(DataTypeUUIDTest, HiveBatchRejectsQuotedUuidWithoutAppendingBadRow) {
    DataTypeUUIDSerDe nested_serde(3);
    auto column = type->create_column();
    std::string valid = "00112233445566778899AABBCCDDEEFF";
    std::string quoted = "\"00112233-4455-6677-8899-aabbccddeeff\"";
    std::vector<Slice> slices {Slice(valid), Slice(quoted)};
    uint64_t rows = 0;
    EXPECT_FALSE(
            nested_serde.deserialize_column_from_hive_text_vector(*column, slices, &rows, {}).ok());
    EXPECT_EQ(rows, 1);
    EXPECT_EQ(column->size(), 1);
    EXPECT_EQ(type->to_string(*column, 0), "00112233-4455-6677-8899-aabbccddeeff");
}

} // namespace doris
