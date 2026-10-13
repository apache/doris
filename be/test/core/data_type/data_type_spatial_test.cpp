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

#include <gtest/gtest.h>

#include <array>
#include <cstring>
#include <string>

#include "agent/be_exec_version_manager.h"
#include "core/assert_cast.h"
#include "core/column/column_spatial.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_varbinary.h"

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

} // namespace doris
