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

#include "core/data_type/data_type_spatial.h"

#include <gen_cpp/data.pb.h>
#include <gtest/gtest.h>

#include <array>
#include <cstring>
#include <string>

#include "agent/be_exec_version_manager.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_spatial.h"
#include "core/column/column_string.h"
#include "core/column/column_varbinary.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_varbinary.h"
#include "exec/common/sip_hash.h"
#include "exec/sort/sort_block.h"

namespace doris {

TEST(DataTypeSpatialTest, WireIdsDoNotReuseReleaseFileTypes) {
    // branch-4.1 FILE uses protobuf 45 and Thrift 47, independently of whether
    // mixed-version spatial execution is supported. Old FILE must remain unsupported.
    EXPECT_EQ(PGenericType::GEOMETRY, 46);
    EXPECT_EQ(PGenericType::GEOGRAPHY, 47);
    EXPECT_EQ(TPrimitiveType::GEOMETRY, 48);
    EXPECT_EQ(TPrimitiveType::GEOGRAPHY, 49);
}

TEST(DataTypeSpatialTest, ReleaseFileProtobufMetadataRemainsUnsupported) {
    PColumnMeta file_meta;
    // Parse release FILE's wire field (type, tag 2, value 45). Reserved proto2 enum
    // values remain unknown fields; the type defaults to UNKNOWN, never GEOMETRY.
    ASSERT_TRUE(file_meta.ParseFromString(std::string("\x10\x2d", 2)));
    file_meta.set_is_nullable(false);
    for (size_t child = 0; child < 6; ++child) {
        file_meta.add_children()->set_type(PGenericType::STRING);
    }
    EXPECT_THROW(DataTypeFactory::instance().create_data_type(file_meta), doris::Exception);
}

TEST(DataTypeSpatialTest, ReleaseFileThriftDescriptorRemainsUnsupported) {
    auto file_desc = create_type_desc(TYPE_GEOMETRY);
    // Intentionally inject branch-4.1's FILE wire value to test unknown-type rejection.
    // NOLINTNEXTLINE(clang-analyzer-optin.core.EnumCastOutOfRange)
    file_desc.types[0].scalar_type.type = static_cast<TPrimitiveType::type>(47);
    EXPECT_DEATH(DataTypeFactory::instance().create_data_type(file_desc), "meet unknown type 47");
}

static void check_spatial_multikey_sort(PrimitiveType primitive, int nullable, int direction,
                                        int nulls_direction, size_t limit) {
    auto values = ColumnSpatial::create(primitive);
    for (const char coordinate : {2, 1, 2, 3, 1}) {
        std::string point(21, '\0');
        point[0] = 1;
        point[1] = 1;
        point[13] = coordinate;
        values->insert_data(point.data(), point.size());
    }
    auto ids = ColumnInt32::create();
    ids->get_data() = {5, 4, 1, 3, 2};
    DataTypePtr type = std::make_shared<DataTypeSpatial>(primitive);
    ColumnPtr column = std::move(values);
    if (nullable != 0) {
        auto nulls = ColumnUInt8::create();
        nulls->get_data() =
                nullable == 2 ? IColumn::Filter {0, 1, 0, 1, 0} : IColumn::Filter {0, 0, 0, 0, 0};
        column = ColumnNullable::create(column, std::move(nulls));
        type = make_nullable(type);
    }
    Block input {{column, type, "shape"},
                 {std::move(ids), std::make_shared<DataTypeInt32>(), "id"}};
    Block output = input.clone_empty();
    HybridSorter sorter;
    ASSERT_NO_THROW(
            sort_block(input, output, {{0, direction, nulls_direction}, {1, 1, 1}}, sorter, limit));
    std::vector<int> expected;
    if (nullable == 2) {
        expected = direction == 1 ? std::vector<int> {2, 1, 5} : std::vector<int> {1, 5, 2};
        if (direction * nulls_direction < 0) {
            expected.insert(expected.begin(), {3, 4});
        } else {
            expected.insert(expected.end(), {3, 4});
        }
    } else {
        expected = direction == 1 ? std::vector<int> {2, 4, 1, 5, 3}
                                  : std::vector<int> {3, 1, 5, 2, 4};
    }
    if (limit != 0) {
        expected.resize(limit);
    }
    const auto& sorted_ids = assert_cast<const ColumnInt32&>(*output.get_by_position(1).column);
    ASSERT_EQ(expected.size(), output.rows());
    for (size_t row = 0; row < expected.size(); ++row) {
        EXPECT_EQ(expected[row], sorted_ids.get_element(row));
    }
}

TEST(DataTypeSpatialTest, MultiColumnSortSupportsNullableSpatial) {
    for (const auto primitive : {TYPE_GEOMETRY, TYPE_GEOGRAPHY}) {
        for (int nullable = 0; nullable < 3; ++nullable) {
            for (const int direction : {1, -1}) {
                for (const int nulls_direction : {1, -1}) {
                    for (const size_t limit : {0, 1, 3, 4}) {
                        check_spatial_multikey_sort(primitive, nullable, direction, nulls_direction,
                                                    limit);
                    }
                }
            }
        }
    }
}

TEST(DataTypeSpatialTest, DefaultCrsCaseIsCanonicalButCustomCrsIsPreserved) {
    for (const auto primitive : {TYPE_GEOMETRY, TYPE_GEOGRAPHY}) {
        const DataTypeSpatial canonical(primitive);
        for (const auto* crs : {"ogc:crs84", "OgC:CrS84"}) {
            const DataTypeSpatial imported(primitive, crs);
            EXPECT_EQ(imported.crs(), "OGC:CRS84");
            EXPECT_TRUE(imported.equals(canonical));
        }
        EXPECT_EQ(DataTypeSpatial(primitive, "projjson:CustomCrs").crs(), "projjson:CustomCrs");
    }
}

static void check_spatial_trim_memory(PrimitiveType primitive, size_t payload_size) {
    auto column = ColumnSpatial::create(primitive);
    const std::string first(payload_size, 'a');
    for (size_t row = 0; row < 16; ++row) {
        column->insert_data(first.data(), first.size());
    }
    for (size_t cycle = 0; cycle < 64; ++cycle) {
        const std::string next(payload_size, static_cast<char>('b' + cycle % 20));
        for (size_t row = 0; row < 16; ++row) {
            column->insert_data(next.data(), next.size());
        }
        column->erase(0, 16);
        EXPECT_EQ(column->size(), 16);
        EXPECT_EQ(column->get_data_at(0).to_string(), next);
        EXPECT_EQ(column->get_data_at(15).to_string(), next);
        EXPECT_LE(column->byte_size(), 16 * sizeof(StringView) + 32 * payload_size);
        EXPECT_LE(column->allocated_bytes(), 8 * (16 * payload_size + 4096));
    }
    column->erase(0, column->size());
    EXPECT_EQ(column->byte_size(), 0);
    column->insert_data(first.data(), first.size());
    EXPECT_EQ(column->get_data_at(0).to_string(), first);
}

TEST(DataTypeSpatialTest, ErasureReclaimsArenaAcrossAppendTrimCycles) {
    check_spatial_trim_memory(TYPE_GEOMETRY, 21);
    check_spatial_trim_memory(TYPE_GEOGRAPHY, 21);
    check_spatial_trim_memory(TYPE_GEOMETRY, 8192);
    check_spatial_trim_memory(TYPE_GEOGRAPHY, 8192);
}

TEST(DataTypeSpatialTest, FactoryCreatesDistinctSpatialColumns) {
    auto geometry = DataTypeFactory::instance().create_data_type(TYPE_GEOMETRY, false);
    auto geography = DataTypeFactory::instance().create_data_type(TYPE_GEOGRAPHY, false);

    ASSERT_NE(nullptr, geometry);
    ASSERT_NE(nullptr, geography);
    EXPECT_EQ(TYPE_GEOMETRY, geometry->get_primitive_type());
    EXPECT_EQ(TYPE_GEOGRAPHY, geography->get_primitive_type());
    EXPECT_FALSE(geometry->equals(*geography));

    auto geometry_column = geometry->create_column();
    auto geography_column = geography->create_column();
    EXPECT_TRUE(geometry->check_column(*geometry_column).ok());
    EXPECT_TRUE(geography->check_column(*geography_column).ok());
    EXPECT_FALSE(geometry->check_column(*geography_column).ok());
    EXPECT_FALSE(geometry->check_column(*ColumnVarbinary::create()).ok());

    const std::string wkb(
            "\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@", 21);
    auto& column = assert_cast<ColumnSpatial&>(*geometry_column);
    column.insert_data(wkb.data(), wkb.size());

    const auto value = column.get_data_at(0);
    EXPECT_EQ(wkb.size(), value.size);
    EXPECT_EQ(0, memcmp(wkb.data(), value.data, value.size));

    const auto version = BeExecVersionManager::get_newest_version();
    const auto bytes = geometry->get_uncompressed_serialized_bytes(*geometry_column, version);
    std::string buffer(bytes, '\0');
    auto* end = geometry->serialize(*geometry_column, buffer.data(), version);
    EXPECT_EQ(buffer.data() + buffer.size(), end);

    auto restored = geometry->create_column();
    const auto* restored_end = geometry->deserialize(buffer.data(), &restored, version);
    EXPECT_EQ(buffer.data() + buffer.size(), restored_end);
    const auto restored_value = restored->get_data_at(0);
    EXPECT_EQ(wkb.size(), restored_value.size);
    EXPECT_EQ(0, memcmp(wkb.data(), restored_value.data, restored_value.size));
}

static void expect_mutable_spatial_filter(PrimitiveType primitive) {
    auto column = ColumnSpatial::create(primitive);
    std::vector<std::string> values {std::string(4096, 'a'), std::string(8192, 'b'),
                                     std::string(4096, 'c')};
    for (const auto& value : values) {
        column->insert_data(value.data(), value.size());
    }
    const auto* first_payload = column->get_data_at(0).data;
    const auto* last_payload = column->get_data_at(2).data;
    const auto allocated = column->allocated_bytes();
    EXPECT_EQ(2, column->filter(IColumn::Filter {1, 0, 1}));
    EXPECT_EQ(first_payload, column->get_data_at(0).data);
    EXPECT_EQ(last_payload, column->get_data_at(1).data);
    EXPECT_EQ(values[2], column->get_data_at(1).to_string());
    EXPECT_EQ(allocated, column->allocated_bytes());
    EXPECT_EQ(1, column->filter(IColumn::Filter {0, 1}));
    EXPECT_EQ(last_payload, column->get_data_at(0).data);
    EXPECT_EQ(0, column->filter(IColumn::Filter {0}));
    column->insert_data(values[0].data(), values[0].size());
    EXPECT_EQ(values[0], column->get_data_at(0).to_string());
}

TEST(DataTypeSpatialTest, MutableFilterRetainsPayloadArena) {
    expect_mutable_spatial_filter(TYPE_GEOMETRY);
    expect_mutable_spatial_filter(TYPE_GEOGRAPHY);
}

TEST(DataTypeSpatialTest, MutableNullableFilterRetainsPayloadArena) {
    auto nullable_type = make_nullable(std::make_shared<DataTypeSpatial>(TYPE_GEOGRAPHY));
    auto nullable = nullable_type->create_column();
    const std::string payload(4096, 'p');
    nullable->insert_data(payload.data(), payload.size());
    nullable->insert_default();
    nullable->insert_data(payload.data(), payload.size());
    const auto& nested = assert_cast<const ColumnNullable&>(*nullable).get_nested_column();
    const auto* retained_payload = nested.get_data_at(2).data;
    EXPECT_EQ(2, nullable->filter(IColumn::Filter {0, 1, 1}));
    EXPECT_TRUE(nullable->is_null_at(0));
    EXPECT_FALSE(nullable->is_null_at(1));
    EXPECT_EQ(retained_payload, nested.get_data_at(1).data);
    EXPECT_EQ(payload, nested.get_data_at(1).to_string());
}

static void check_spatial_erasure(PrimitiveType primitive, bool nullable) {
    const auto spatial = std::make_shared<DataTypeSpatial>(primitive);
    const auto type = nullable ? make_nullable(spatial) : spatial;
    auto column = type->create_column();
    const std::string first(4096, 'a');
    const std::string last(4096, 'b');
    column->insert_data(first.data(), first.size());
    column->insert_default();
    column->insert_data(last.data(), last.size());
    column->erase(0, 1);
    ASSERT_EQ(column->size(), 2);
    EXPECT_EQ(column->is_null_at(0), nullable);
    EXPECT_EQ(column->get_data_at(1).to_string(), last);
    column->erase(0, 1);
    ASSERT_EQ(column->size(), 1);
    EXPECT_EQ(column->get_data_at(0).to_string(), last);
    column->erase(0, 1);
    EXPECT_EQ(column->size(), 0);
    column->insert_data(first.data(), first.size());
    EXPECT_EQ(column->get_data_at(0).to_string(), first);
}

TEST(DataTypeSpatialTest, ErasureRetainsSpatialAndNullablePayloads) {
    check_spatial_erasure(TYPE_GEOMETRY, false);
    check_spatial_erasure(TYPE_GEOGRAPHY, false);
    check_spatial_erasure(TYPE_GEOMETRY, true);
    check_spatial_erasure(TYPE_GEOGRAPHY, true);
}

TEST(DataTypeSpatialTest, FactoryPreservesSpatialMetadataFromTypeDescriptor) {
    auto geometry_desc = create_type_desc(TYPE_GEOMETRY);
    geometry_desc.types[0].scalar_type.__set_spatial_crs("EPSG:3857");
    const auto geometry = DataTypeFactory::instance().create_data_type(geometry_desc);
    const auto& geometry_type = assert_cast<const DataTypeSpatial&>(*geometry);
    EXPECT_EQ(TYPE_GEOMETRY, geometry_type.get_primitive_type());
    EXPECT_EQ("EPSG:3857", geometry_type.crs());
    EXPECT_TRUE(geometry_type.algorithm().empty());

    auto geography_desc = create_type_desc(TYPE_GEOGRAPHY);
    geography_desc.types[0].scalar_type.__set_spatial_crs("OGC:CRS84");
    geography_desc.types[0].scalar_type.__set_spatial_algorithm("spherical");
    const auto geography = DataTypeFactory::instance().create_data_type(geography_desc);
    const auto& geography_type = assert_cast<const DataTypeSpatial&>(*geography);
    EXPECT_EQ(TYPE_GEOGRAPHY, geography_type.get_primitive_type());
    EXPECT_EQ("OGC:CRS84", geography_type.crs());
    EXPECT_EQ("spherical", geography_type.algorithm());
}

TEST(DataTypeSpatialTest, ColumnPreservesWkbThroughFilterAndPermute) {
    auto column = ColumnSpatial::create(TYPE_GEOMETRY);
    const std::string first("\x01\x01\x00\x00\x00", 5);
    const std::string second("\x00\x02\x00\x00\x00\x10\x00", 7);
    const std::string third("\x01\x03\x00\x00\x00\x00", 6);
    column->insert_data(first.data(), first.size());
    column->insert_data(second.data(), second.size());
    column->insert_data(third.data(), third.size());

    const IColumn::Filter filter = {1, 0, 1};
    const auto filtered = column->filter(filter, -1);
    ASSERT_EQ(2, filtered->size());
    EXPECT_EQ(first, filtered->get_data_at(0).to_string());
    EXPECT_EQ(third, filtered->get_data_at(1).to_string());

    const IColumn::Permutation permutation = {2, 0, 1};
    const auto permuted = column->permute(permutation, 2);
    ASSERT_EQ(2, permuted->size());
    EXPECT_EQ(third, permuted->get_data_at(0).to_string());
    EXPECT_EQ(first, permuted->get_data_at(1).to_string());
}

static void expect_spatial_column_resize(ColumnSpatial& column, size_t payload_size) {
    column.pop_back(1);
    column.resize(2);
    EXPECT_EQ(column.size(), 2);
    EXPECT_GE(column.allocated_bytes(), column.byte_size());
    EXPECT_GT(column.byte_size(), payload_size);
    column.clear();
    EXPECT_EQ(column.size(), 0);
}

static void expect_spatial_storage_rejected(PrimitiveType primitive) {
    DataTypeSpatial type(primitive);
    EXPECT_THROW(static_cast<void>(type.get_storage_field_type()), Exception);
}

static void expect_spatial_copy_ownership(PrimitiveType primitive) {
    const std::string wkb(
            "\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@", 21);
    auto source = ColumnSpatial::create(primitive);
    source->insert_data(wkb.data(), wkb.size());
    source->insert_default();
    const IColumn& values = *source;
    auto copy = source->clone();
    auto appended = ColumnSpatial::create(primitive);
    appended->insert(values[0]);
    Field field;
    values.get(0, field);
    appended->insert(field);
    appended->insert_from(values, 0);
    const std::array<uint32_t, 2> indices {1, 0};
    appended->insert_indices_from(values, indices.data(), indices.data() + indices.size());
    EXPECT_EQ(appended->size(), 5);
    EXPECT_EQ(appended->get_data_at(3).size, 0);
    appended->replace_column_data(values, 0, 3);
    EXPECT_EQ(appended->get_data_at(3).to_string(), wkb);
    source->clear();
    EXPECT_EQ(copy->get_data_at(0).to_string(), wkb);
    EXPECT_EQ(appended->get_data_at(0).to_string(), wkb);
    EXPECT_EQ(assert_cast<const ColumnSpatial&>(*copy).get_primitive_type(), primitive);
    expect_spatial_column_resize(*appended, wkb.size());
    expect_spatial_storage_rejected(primitive);
}

TEST(DataTypeSpatialTest, ColumnCopiesOwnPayloadAndPreserveSpatialIdentity) {
    expect_spatial_copy_ownership(TYPE_GEOMETRY);
    expect_spatial_copy_ownership(TYPE_GEOGRAPHY);
}

TEST(DataTypeSpatialTest, SpatialCompositeKeysRoundTripThroughIColumn) {
    const std::string wkb(
            "\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@", 21);
    auto source = ColumnSpatial::create(TYPE_GEOMETRY);
    source->insert_data(wkb.data(), wkb.size());
    source->insert_default();
    auto trailing = ColumnSpatial::create(TYPE_GEOGRAPHY);
    trailing->insert_default();
    trailing->insert_data(wkb.data(), wkb.size());
    const IColumn& source_interface = *source;
    const IColumn& trailing_interface = *trailing;
    const auto key_capacity =
            source_interface.get_max_row_byte_size() + trailing_interface.get_max_row_byte_size();
    std::array<std::string, 2> buffers = {std::string(key_capacity, '\0'),
                                          std::string(key_capacity, '\0')};
    std::array<StringRef, 2> keys = {StringRef(buffers[0].data(), 0),
                                     StringRef(buffers[1].data(), 0)};
    source_interface.serialize(keys.data(), keys.size());
    trailing_interface.serialize(keys.data(), keys.size());
    const auto serialized_size = wkb.size() + 2 * sizeof(uint32_t);
    EXPECT_EQ(keys[0].size, serialized_size);
    EXPECT_EQ(keys[1].size, serialized_size);

    auto restored = ColumnSpatial::create(TYPE_GEOMETRY);
    auto restored_trailing = ColumnSpatial::create(TYPE_GEOGRAPHY);
    IColumn& restored_interface = *restored;
    IColumn& restored_trailing_interface = *restored_trailing;
    restored_interface.deserialize(keys.data(), keys.size());
    restored_trailing_interface.deserialize(keys.data(), keys.size());
    ASSERT_EQ(restored->size(), 2);
    ASSERT_EQ(restored_trailing->size(), 2);
    EXPECT_EQ(restored->get_data_at(0).to_string(), wkb);
    EXPECT_EQ(restored->get_data_at(1).size, 0);
    EXPECT_EQ(restored_trailing->get_data_at(0).size, 0);
    EXPECT_EQ(restored_trailing->get_data_at(1).to_string(), wkb);
    EXPECT_EQ(keys[0].size, 0);
    EXPECT_EQ(keys[1].size, 0);
    EXPECT_EQ(keys[0].data, buffers[0].data() + serialized_size);
    EXPECT_EQ(keys[1].data, buffers[1].data() + serialized_size);
}

namespace {

void expect_masked_spatial_hashes(const IColumn& actual, const IColumn& reference,
                                  PrimitiveType primitive, const uint8_t* mask) {
    std::array<uint32_t, 3> hashes = {17, 23, 31};
    auto expected = hashes;
    actual.update_crc32c_batch(hashes.data(), mask);
    reference.update_crc32c_batch(expected.data(), mask);
    EXPECT_EQ(hashes, expected);
    actual.update_crcs_with_value(hashes.data(), primitive, actual.size(), 0, mask);
    reference.update_crcs_with_value(expected.data(), TYPE_STRING, actual.size(), 0, mask);
    EXPECT_EQ(hashes, expected);
    std::array<uint64_t, 3> xx = {17, 23, 31};
    auto expected_xx = xx;
    actual.update_hashes_with_value(xx.data(), mask);
    reference.update_hashes_with_value(expected_xx.data(), mask);
    EXPECT_EQ(xx, expected_xx);
    uint32_t crc = 17;
    uint32_t expected_crc = crc;
    actual.update_crc32c_single(0, actual.size(), crc, mask);
    reference.update_crc32c_single(0, actual.size(), expected_crc, mask);
    EXPECT_EQ(crc, expected_crc);
    actual.update_crc_with_value(0, actual.size(), crc, mask);
    reference.update_crc_with_value(0, actual.size(), expected_crc, mask);
    EXPECT_EQ(crc, expected_crc);
    uint64_t range = 17;
    uint64_t expected_range = range;
    actual.update_xxHash_with_value(0, actual.size(), range, mask);
    reference.update_xxHash_with_value(0, actual.size(), expected_range, mask);
    EXPECT_EQ(range, expected_range);
}

void expect_spatial_hash_interfaces(PrimitiveType primitive) {
    const std::array<std::string, 3> values = {std::string("a\0b", 3), "different", ""};
    auto spatial = ColumnSpatial::create(primitive);
    auto binary = ColumnString::create();
    for (const auto& value : values) {
        spatial->insert_data(value.data(), value.size());
        binary->insert_data(value.data(), value.size());
    }
    const IColumn& actual = *spatial;
    const IColumn& reference = *binary;
    const std::array<uint8_t, 3> nulls = {0, 1, 0};
    for (const uint8_t* mask : {static_cast<const uint8_t*>(nullptr), nulls.data()}) {
        expect_masked_spatial_hashes(actual, reference, primitive, mask);
    }
    for (size_t row = 0; row < values.size(); ++row) {
        SipHash hash;
        SipHash expected;
        actual.update_hash_with_value(row, hash);
        reference.update_hash_with_value(row, expected);
        EXPECT_EQ(hash.get64(), expected.get64());
    }
}

void expect_spatial_block_round_trip(const DataTypePtr& type, const std::string& wkb,
                                     bool constant) {
    auto data = type->create_column();
    data->insert_data(wkb.data(), wkb.size());
    ColumnPtr column;
    if (constant) {
        column = ColumnConst::create(std::move(data), 2);
    } else {
        data->insert_default();
        column = std::move(data);
    }
    Block source({ColumnWithTypeAndName(column, type, "spatial")});
    PBlock protobuf;
    size_t uncompressed = 0;
    size_t compressed = 0;
    int64_t elapsed = 0;
    ASSERT_TRUE(source.serialize(BeExecVersionManager::get_newest_version(), &protobuf,
                                 &uncompressed, &compressed, &elapsed,
                                 segment_v2::CompressionTypePB::SNAPPY)
                        .ok());
    Block restored;
    ASSERT_TRUE(restored.deserialize(protobuf, &uncompressed, &elapsed).ok());
    ASSERT_EQ(restored.columns(), 1);
    ASSERT_EQ(restored.rows(), 2);
    const auto& result = restored.get_by_position(0);
    EXPECT_TRUE(type->equals(*result.type));
    EXPECT_EQ(result.name, "spatial");
    EXPECT_EQ(is_column_const(*result.column), constant);
    EXPECT_EQ(result.column->get_data_at(0).to_string(), wkb);
    EXPECT_EQ((*result.column)[1], (*column)[1]);
}

} // namespace

TEST(DataTypeSpatialTest, HashInterfacesPreserveRawBinaryIdentityAndNullMasks) {
    for (const auto primitive : {TYPE_GEOMETRY, TYPE_GEOGRAPHY}) {
        expect_spatial_hash_interfaces(primitive);
    }
}

TEST(DataTypeSpatialTest, FullBlockRoundTripPreservesParameterizedSpatialColumns) {
    const std::string wkb(
            "\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@", 21);
    const std::array<DataTypePtr, 2> spatial_types = {
            std::make_shared<DataTypeSpatial>(TYPE_GEOMETRY, "EPSG:3857"),
            std::make_shared<DataTypeSpatial>(TYPE_GEOGRAPHY, "OGC:CRS84", "vincenty")};
    for (const auto& spatial : spatial_types) {
        for (const bool nullable : {false, true}) {
            const auto type = nullable ? make_nullable(spatial) : spatial;
            for (const bool constant : {false, true}) {
                expect_spatial_block_round_trip(type, wkb, constant);
            }
        }
    }
}

TEST(DataTypeSpatialTest, PTypeDescRoundTripPreservesSpatialParametersAndNullability) {
    const std::array<DataTypePtr, 4> types {
            std::make_shared<DataTypeSpatial>(TYPE_GEOMETRY, "OGC:CRS84"),
            std::make_shared<DataTypeSpatial>(TYPE_GEOMETRY, "EPSG:3857"),
            std::make_shared<DataTypeSpatial>(TYPE_GEOGRAPHY, "OGC:CRS84", "spherical"),
            std::make_shared<DataTypeSpatial>(TYPE_GEOGRAPHY, "EPSG:4326", "VINCENTY")};
    for (const auto& type : types) {
        for (bool nullable : {false, true}) {
            const DataTypePtr expected = nullable ? make_nullable(type) : type;
            PTypeDesc serialized;
            expected->to_protobuf(&serialized);
            int index = 0;
            auto restored = DataTypeFactory::instance().create_data_type(serialized.types(), &index,
                                                                         nullable);
            EXPECT_TRUE(restored->equals(*expected))
                    << expected->get_name() << " became " << restored->get_name();
        }
        const DataTypePtr expected = std::make_shared<DataTypeArray>(make_nullable(type));
        PTypeDesc serialized;
        expected->to_protobuf(&serialized);
        int index = 0;
        auto restored =
                DataTypeFactory::instance().create_data_type(serialized.types(), &index, false);
        EXPECT_TRUE(restored->equals(*expected))
                << expected->get_name() << " became " << restored->get_name();
    }
}

TEST(DataTypeSpatialTest, PTypeDescMissingOptionalMetadataRestoresDefaultIdentity) {
    for (const auto primitive : {TYPE_GEOMETRY, TYPE_GEOGRAPHY}) {
        const auto expected = DataTypeFactory::instance().create_data_type(primitive, false);
        PTypeDesc serialized;
        expected->to_protobuf(&serialized);
        serialized.mutable_types(0)->mutable_scalar_type()->clear_spatial_crs();
        serialized.mutable_types(0)->mutable_scalar_type()->clear_spatial_algorithm();
        int index = 0;
        auto restored =
                DataTypeFactory::instance().create_data_type(serialized.types(), &index, false);
        EXPECT_TRUE(restored->equals(*expected));
    }
}

} // namespace doris
