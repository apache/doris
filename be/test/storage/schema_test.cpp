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

#include "storage/schema.h"

#include <gen_cpp/Descriptors_types.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <utility>

#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_bitmap.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_ipv6.h"
#include "core/data_type/data_type_jsonb.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_variant_v2.h"
#include "runtime/descriptors.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

namespace {

// FE sends this length for a STRING type.
constexpr int FE_STRING_LENGTH = std::numeric_limits<int32_t>::max() - 4;

TSlotDescriptor create_tslot(const DataTypePtr& type, std::string name, int32_t unique_id) {
    TSlotDescriptor tslot;
    type->to_thrift(tslot.slotType);
    tslot.__set_nullIndicatorBit(type->is_nullable() ? 0 : -1);
    tslot.__set_colName(std::move(name));
    tslot.__set_col_unique_id(unique_id);
    return tslot;
}

} // namespace

// MAP<STRING, ARRAY<INT>>: the sub columns are built and named the way FE names them.
TEST(StorageSchemaTest, TabletColumnFromSlotBuildsSubColumns) {
    auto type = make_nullable(std::make_shared<DataTypeMap>(
            make_nullable(std::make_shared<DataTypeString>(FE_STRING_LENGTH, TYPE_STRING)),
            make_nullable(std::make_shared<DataTypeArray>(
                    make_nullable(std::make_shared<DataTypeInt32>())))));
    SlotDescriptor slot(create_tslot(type, "obj", 7));

    TabletColumn column = Schema::tablet_column_from_slot(slot);
    EXPECT_EQ("obj", column.name());
    EXPECT_EQ(7, column.unique_id());
    EXPECT_EQ(FieldType::OLAP_FIELD_TYPE_MAP, column.type());
    EXPECT_TRUE(column.is_nullable());
    EXPECT_FALSE(column.has_default_value());
    ASSERT_EQ(2, column.get_subtype_count());
    const TabletColumn& key = column.get_sub_column(0);
    EXPECT_EQ("key", key.name());
    EXPECT_EQ(FieldType::OLAP_FIELD_TYPE_STRING, key.type());
    EXPECT_TRUE(key.is_nullable());
    const TabletColumn& value = column.get_sub_column(1);
    EXPECT_EQ("value", value.name());
    EXPECT_EQ(FieldType::OLAP_FIELD_TYPE_ARRAY, value.type());
    ASSERT_EQ(1, value.get_subtype_count());
    EXPECT_EQ("item", value.get_sub_column(0).name());
    EXPECT_EQ(FieldType::OLAP_FIELD_TYPE_INT, value.get_sub_column(0).type());
}

// The column describes the slot's type: building the type back from it gives the same type.
TEST(StorageSchemaTest, TabletColumnFromSlotKeepsTheType) {
    const auto nullable_int = make_nullable(std::make_shared<DataTypeInt32>());
    const auto varchar = std::make_shared<DataTypeString>(10, TYPE_VARCHAR);
    const DataTypes types = {
            std::make_shared<DataTypeInt32>(),
            make_nullable(std::make_shared<DataTypeDecimal32>(9, 2)),
            make_nullable(std::make_shared<DataTypeDecimal128>(38, 10)),
            make_nullable(std::make_shared<DataTypeDateTimeV2>(3)),
            make_nullable(std::make_shared<DataTypeDateV2>()),
            make_nullable(std::make_shared<DataTypeIPv6>()),
            make_nullable(std::make_shared<DataTypeJsonb>()),
            std::make_shared<DataTypeBitMap>(),
            make_nullable(std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>())),
            make_nullable(std::make_shared<DataTypeArray>(
                    make_nullable(std::make_shared<DataTypeArray>(make_nullable(varchar))))),
            make_nullable(std::make_shared<DataTypeMap>(make_nullable(varchar), nullable_int)),
            make_nullable(std::make_shared<DataTypeStruct>(
                    DataTypes {std::make_shared<DataTypeInt32>(), make_nullable(varchar),
                               make_nullable(std::make_shared<DataTypeMap>(make_nullable(varchar),
                                                                           nullable_int))},
                    Strings {"a", "b", "c"})),
    };
    for (const auto& type : types) {
        SlotDescriptor slot(create_tslot(type, "c", 1));
        TabletColumn column = Schema::tablet_column_from_slot(slot);
        EXPECT_EQ(type->get_name(),
                  DataTypeFactory::instance().create_data_type(column)->get_name());
    }
}

// Length, precision, scale, variant settings and default value come out as for a column FE sends.
TEST(StorageSchemaTest, TabletColumnFromSlotKeepsTypeDetails) {
    TSlotDescriptor char_tslot =
            create_tslot(std::make_shared<DataTypeString>(5, TYPE_CHAR), "c", 1);
    char_tslot.__set_col_default_value("ab");
    TabletColumn char_column = Schema::tablet_column_from_slot(SlotDescriptor(char_tslot));
    EXPECT_EQ(FieldType::OLAP_FIELD_TYPE_CHAR, char_column.type());
    EXPECT_EQ(5, char_column.length());
    EXPECT_TRUE(char_column.has_default_value());
    EXPECT_EQ("ab", char_column.default_value());

    TabletColumn varchar_column = Schema::tablet_column_from_slot(SlotDescriptor(
            create_tslot(std::make_shared<DataTypeString>(10, TYPE_VARCHAR), "c", 1)));
    EXPECT_EQ(FieldType::OLAP_FIELD_TYPE_VARCHAR, varchar_column.type());
    EXPECT_EQ(12, varchar_column.length());

    TabletColumn string_column = Schema::tablet_column_from_slot(SlotDescriptor(
            create_tslot(std::make_shared<DataTypeString>(FE_STRING_LENGTH, TYPE_STRING), "c", 1)));
    EXPECT_EQ(std::numeric_limits<int32_t>::max(), string_column.length());

    // DECIMALV2 computes as (27, 9) but keeps the declared precision and scale.
    TSlotDescriptor decimalv2_tslot = create_tslot(std::make_shared<DataTypeDecimalV2>(), "c", 1);
    decimalv2_tslot.slotType.types[0].scalar_type.__set_precision(10);
    decimalv2_tslot.slotType.types[0].scalar_type.__set_scale(2);
    TabletColumn decimalv2_column =
            Schema::tablet_column_from_slot(SlotDescriptor(decimalv2_tslot));
    EXPECT_EQ(FieldType::OLAP_FIELD_TYPE_DECIMAL, decimalv2_column.type());
    EXPECT_EQ(10, decimalv2_column.precision());
    EXPECT_EQ(2, decimalv2_column.frac());

    TSlotDescriptor variant_tslot =
            create_tslot(make_nullable(std::make_shared<DataTypeVariantV2>()), "v", 1);
    variant_tslot.slotType.types[0].scalar_type.__set_variant_max_subcolumns_count(3);
    variant_tslot.slotType.types[0].scalar_type.__set_variant_enable_doc_mode(true);
    TabletColumn variant_column = Schema::tablet_column_from_slot(SlotDescriptor(variant_tslot));
    EXPECT_EQ(FieldType::OLAP_FIELD_TYPE_VARIANT, variant_column.type());
    EXPECT_EQ(3, variant_column.variant_max_subcolumns_count());
    EXPECT_TRUE(variant_column.variant_enable_doc_mode());
}

} // namespace doris
