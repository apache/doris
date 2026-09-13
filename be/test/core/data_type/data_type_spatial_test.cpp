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

#include <arrow/array/builder_binary.h>
#include <cctz/time_zone.h>
#include <gtest/gtest.h>

#include <cstring>
#include <string>

#include "agent/be_exec_version_manager.h"
#include "core/assert_cast.h"
#include "core/column/column_spatial.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_varbinary.h"
#include "exec/common/arrow_column_to_doris_column.h"

namespace doris {

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
            "\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00@", 21);
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

TEST(DataTypeSpatialTest, ProtobufRoundTripPreservesSpatialMetadata) {
    const auto geography = std::make_shared<DataTypeSpatial>(TYPE_GEOGRAPHY, "EPSG:4326", "vincenty");
    PTypeDesc descriptor;
    static_cast<const IDataType&>(*geography).to_protobuf(&descriptor);

    ASSERT_EQ(1, descriptor.types_size());
    ASSERT_TRUE(descriptor.types(0).has_spatial_crs());
    ASSERT_TRUE(descriptor.types(0).has_spatial_algorithm());
    EXPECT_EQ("EPSG:4326", descriptor.types(0).spatial_crs());
    EXPECT_EQ("vincenty", descriptor.types(0).spatial_algorithm());

    int index = 0;
    const auto restored = DataTypeFactory::instance().create_data_type(descriptor.types(), &index, false);
    const auto& restored_type = assert_cast<const DataTypeSpatial&>(*restored);
    EXPECT_EQ(TYPE_GEOGRAPHY, restored_type.get_primitive_type());
    EXPECT_EQ("EPSG:4326", restored_type.crs());
    EXPECT_EQ("vincenty", restored_type.algorithm());
}

TEST(DataTypeSpatialTest, ArrowBinaryReadPreservesWkbForSpatialTypes) {
    const std::string wkb(
            "\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00@", 21);
    arrow::BinaryBuilder builder;
    ASSERT_TRUE(builder.Append(wkb).ok());
    std::shared_ptr<arrow::Array> arrow_array;
    ASSERT_TRUE(builder.Finish(&arrow_array).ok());

    for (const auto primitive_type : {TYPE_GEOMETRY, TYPE_GEOGRAPHY}) {
        const auto type = DataTypeFactory::instance().create_data_type(primitive_type, false);
        ColumnPtr column = type->create_column();
        ASSERT_TRUE(arrow_column_to_doris_column(arrow_array.get(), 0, column, type, 1, "").ok());

        const auto& spatial = assert_cast<const ColumnSpatial&>(*column);
        EXPECT_EQ(primitive_type, spatial.get_primitive_type());
        ASSERT_EQ(1, spatial.size());
        const auto value = spatial.get_data_at(0);
        EXPECT_EQ(wkb.size(), value.size);
        EXPECT_EQ(0, memcmp(wkb.data(), value.data, value.size));
    }
}

TEST(DataTypeSpatialTest, ArrowBinaryWritePreservesWkbForSpatialTypes) {
    const std::string wkb(
            "\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@", 21);
    const NullMap null_map = {0, 1};
    cctz::time_zone timezone;

    for (const auto primitive_type : {TYPE_GEOMETRY, TYPE_GEOGRAPHY}) {
        const auto type = DataTypeFactory::instance().create_data_type(primitive_type, false);
        auto column = type->create_column();
        column->insert_data(wkb.data(), wkb.size());
        column->insert_data(wkb.data(), wkb.size());

        arrow::BinaryBuilder builder;
        ASSERT_TRUE(type->get_serde()
                            ->write_column_to_arrow(*column, &null_map, &builder, 0, column->size(),
                                                    timezone)
                            .ok());

        std::shared_ptr<arrow::Array> array;
        ASSERT_TRUE(builder.Finish(&array).ok());
        const auto* binary = dynamic_cast<const arrow::BinaryArray*>(array.get());
        ASSERT_NE(nullptr, binary);
        ASSERT_EQ(2, binary->length());
        ASSERT_FALSE(binary->IsNull(0));
        ASSERT_TRUE(binary->IsNull(1));
        ASSERT_EQ(wkb.size(), static_cast<size_t>(binary->value_length(0)));
        const auto* value = binary->value_data()->data() + binary->value_offset(0);
        EXPECT_EQ(0, memcmp(wkb.data(), value, wkb.size()));
    }
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

} // namespace doris
