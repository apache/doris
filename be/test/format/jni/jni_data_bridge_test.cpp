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

#include "format/jni/jni_data_bridge.h"

#include <gtest/gtest.h>

#include <array>
#include <memory>

#include "core/column/column_nullable.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_variant_v2.h"

namespace doris {
namespace {

template <typename T>
long address_of(T* pointer) {
    return reinterpret_cast<long>(pointer);
}

TEST(JniDataBridgeTest, VariantTypeUsesVariantJniName) {
    auto variant_type = std::make_shared<DataTypeVariantV2>();
    auto nullable_variant_type = make_nullable(variant_type);

    EXPECT_EQ(JniDataBridge::get_jni_type(variant_type), "variant");
    EXPECT_EQ(JniDataBridge::get_jni_type(nullable_variant_type), "variant");
    EXPECT_EQ(JniDataBridge::get_jni_type_with_different_string(variant_type), "variant");
    EXPECT_EQ(JniDataBridge::get_jni_type_with_different_string(nullable_variant_type), "variant");
}

TEST(JniDataBridgeTest, FillNullableVariantV2FromEncodedPaimonLayout) {
    std::array<bool, 3> null_map {false, true, false};
    // Paimon metadata contains an empty dictionary and a one-key dictionary for "a".
    std::array<uint32_t, 3> metadata_offsets {0, 3, 8};
    std::array<char, 8> metadata_bytes {char {0x01}, char {0x00}, char {0x00}, char {0x01},
                                        char {0x01}, char {0x00}, char {0x01}, 'a'};
    std::array<uint32_t, 3> metadata_ids {0, 0, 1};
    // Primitive values are true, Variant null (the SQL NULL placeholder), and false.
    std::array<uint32_t, 4> value_offsets {0, 1, 2, 3};
    std::array<char, 3> value_bytes {char {0x04}, char {0x00}, char {0x08}};

    std::array<long, 7> meta {
            address_of(null_map.data()),         2,
            address_of(metadata_offsets.data()), address_of(metadata_bytes.data()),
            address_of(metadata_ids.data()),     address_of(value_offsets.data()),
            address_of(value_bytes.data()),
    };
    JniDataBridge::TableMetaAddress meta_address(address_of(meta.data()));

    DataTypePtr data_type = make_nullable(std::make_shared<DataTypeVariantV2>());
    ColumnPtr column = data_type->create_column();
    const Status status = JniDataBridge::fill_column(meta_address, column, data_type, 3);
    ASSERT_TRUE(status.ok()) << status;

    const auto& nullable_column = assert_cast<const ColumnNullable&>(*column);
    EXPECT_EQ(nullable_column.get_null_map_data(), NullMap({0, 1, 0}));

    const auto& variant_column =
            assert_cast<const ColumnVariantV2&>(nullable_column.get_nested_column());
    ASSERT_EQ(variant_column.size(), 3);
    ASSERT_FALSE(variant_column.is_typed());
    const auto view = variant_column.read_view();
    EXPECT_EQ(view.metadata_count(), 2);
    EXPECT_EQ(view.metadata_id_at(0), view.metadata_id_at(1));
    EXPECT_NE(view.metadata_id_at(0), view.metadata_id_at(2));
    EXPECT_TRUE(variant_column.get_value_ref(0).get_bool());
    EXPECT_TRUE(variant_column.get_value_ref(1).is_null());
    EXPECT_FALSE(variant_column.get_value_ref(2).get_bool());
}

} // namespace
} // namespace doris
