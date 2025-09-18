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

#include <gtest/gtest.h>

#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_ipv4.h"
#include "vec/functions/complex_hash_map_dictionary.h"
#include "vec/functions/dictionary_factory.h"
#include "vec/functions/ip_address_dictionary.h"
#include "vec/runtime/ip_address_cidr.h"
#include "vec/runtime/ipv4_value.h"
#include "vec/runtime/ipv6_value.h"

namespace doris::vectorized {

template <typename DataType>
ColumnPtr create_column(const std::vector<typename DataType::FieldType>& datas) {
    auto column = DataType::ColumnType::create();
    if constexpr (std::is_same_v<DataTypeString, DataType>) {
        for (const auto& data : datas) {
            column->insert_data(data.data(), data.size());
        }
    } else {
        for (const auto& data : datas) {
            column->insert_value(data);
        }
    }
    return std::move(column);
}

TEST(DictionaryGetNullableTest, testHashMapDictionary) {
    auto dict = create_complex_hash_map_dict_from_column(
            "dict",
            {ColumnWithTypeAndName(create_column<DataTypeInt32>({1, 2, 3, 4, 5}),
                                   std::make_shared<DataTypeInt32>(), "key")},
            {ColumnWithTypeAndName(
                    ColumnNullable::create(
                            create_column<DataTypeInt32>({1, 2, 3, 4, 5}),
                            create_column<DataTypeUInt8>({false, true, false, true, false})),
                    std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()), "val")});

    {
        auto no_null_key = create_column<DataTypeInt32>({1, 2, 3, 4, 5, 6});
        auto res = dict->get_column("val", std::make_shared<DataTypeInt32>(), no_null_key,
                                    std::make_shared<DataTypeInt32>());
        const auto* res_column = assert_cast<const ColumnNullable*>(res.get());
        EXPECT_EQ(6, res_column->size());

        // found , value not null
        EXPECT_FALSE(res_column->is_null_at(0));
        EXPECT_EQ(res_column->get_nested_column().get_int(0), 1);

        // found ,  value is null
        EXPECT_TRUE(res_column->is_null_at(1));

        // found , value not null
        EXPECT_FALSE(res_column->is_null_at(2));
        EXPECT_EQ(res_column->get_nested_column().get_int(2), 3);

        // found ,  value is null
        EXPECT_TRUE(res_column->is_null_at(3));

        // found , value not null
        EXPECT_FALSE(res_column->is_null_at(4));
        EXPECT_EQ(res_column->get_nested_column().get_int(4), 5);

        EXPECT_TRUE(res_column->is_null_at(5)); // not found
    }

    {
        auto null_key =
                ColumnNullable::create(create_column<DataTypeInt32>({1, 2, 3, 6}),
                                       create_column<DataTypeUInt8>({true, false, false, false}));
        auto res = dict->get_column("val", std::make_shared<DataTypeInt32>(), std::move(null_key),
                                    std::make_shared<DataTypeInt32>());
        const auto* res_column = assert_cast<const ColumnNullable*>(res.get());
        EXPECT_EQ(4, res_column->size());
        EXPECT_TRUE(res_column->is_null_at(0));  // key is null
        EXPECT_TRUE(res_column->is_null_at(1));  // found , value is null
        EXPECT_FALSE(res_column->is_null_at(2)); // found , value not null
        EXPECT_EQ(res_column->get_nested_column().get_int(2), 3);
        EXPECT_TRUE(res_column->is_null_at(3)); // not found
    }
}

TEST(DictionaryGetNullableTest, testIpTrieDictionary) {
    auto dict = create_ip_trie_dict_from_column(
            "dict",
            ColumnWithTypeAndName(
                    create_column<DataTypeString>({"1.1.1.0/24", "2.1.1.0/24", "4.1.1.0/24",
                                                   "8.1.1.0/24", "16.1.1.0/24"}),
                    std::make_shared<DataTypeString>(), "key"),
            {ColumnWithTypeAndName(
                    ColumnNullable::create(
                            create_column<DataTypeInt32>({1, 2, 3, 4, 5}),
                            create_column<DataTypeUInt8>({false, true, false, true, false})),
                    std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()), "val")});

    auto to_ipv4 = [](const std::string& ips) -> IPv4 {
        IPv4Value ipv4_value;
        EXPECT_TRUE(ipv4_value.from_string(ips));
        return ipv4_value.value();
    };

    {
        auto no_null_key = create_column<DataTypeIPv4>(
                {to_ipv4("1.1.1.1"), to_ipv4("2.1.1.1"), to_ipv4("32.1.1.1")});
        auto res = dict->get_column("val", std::make_shared<DataTypeInt32>(), no_null_key,
                                    std::make_shared<DataTypeIPv4>());
        const auto* res_column = assert_cast<const ColumnNullable*>(res.get());
        EXPECT_EQ(3, res_column->size());

        // found , value not null
        EXPECT_FALSE(res_column->is_null_at(0));
        EXPECT_EQ(res_column->get_nested_column().get_int(0), 1);

        // found , vaule is null
        EXPECT_TRUE(res_column->is_null_at(1));

        // not found
        EXPECT_TRUE(res_column->is_null_at(2));
    }

    {
        auto null_key = ColumnNullable::create(
                create_column<DataTypeIPv4>({to_ipv4("1.1.1.1"), to_ipv4("2.1.1.1"),
                                             to_ipv4("32.1.1.1"), to_ipv4("1.1.1.1")}),
                create_column<DataTypeUInt8>({false, false, false, true}));
        auto res = dict->get_column("val", std::make_shared<DataTypeInt32>(), std::move(null_key),
                                    std::make_shared<DataTypeIPv4>());
        const auto* res_column = assert_cast<const ColumnNullable*>(res.get());
        EXPECT_EQ(4, res_column->size());

        // found , value not null
        EXPECT_FALSE(res_column->is_null_at(0));
        EXPECT_EQ(res_column->get_nested_column().get_int(0), 1);

        // found , vaule is null
        EXPECT_TRUE(res_column->is_null_at(1));

        // not found
        EXPECT_TRUE(res_column->is_null_at(2));

        // key is null
        EXPECT_TRUE(res_column->is_null_at(3));
    }

    {
        auto no_null_key = create_column<DataTypeIPv6>({ipv4_to_ipv6(to_ipv4("1.1.1.1")),
                                                        ipv4_to_ipv6(to_ipv4("2.1.1.1")),
                                                        ipv4_to_ipv6(to_ipv4("32.1.1.1"))});
        auto res = dict->get_column("val", std::make_shared<DataTypeInt32>(), no_null_key,
                                    std::make_shared<DataTypeIPv6>());
        const auto* res_column = assert_cast<const ColumnNullable*>(res.get());
        EXPECT_EQ(3, res_column->size());

        // found , value not null
        EXPECT_FALSE(res_column->is_null_at(0));
        EXPECT_EQ(res_column->get_nested_column().get_int(0), 1);

        // found , vaule is null
        EXPECT_TRUE(res_column->is_null_at(1));

        // not found
        EXPECT_TRUE(res_column->is_null_at(2));
    }

    {
        auto null_key = ColumnNullable::create(
                create_column<DataTypeIPv6>(
                        {ipv4_to_ipv6(to_ipv4("1.1.1.1")), ipv4_to_ipv6(to_ipv4("2.1.1.1")),
                         ipv4_to_ipv6(to_ipv4("32.1.1.1")), ipv4_to_ipv6(to_ipv4("1.1.1.1"))}),
                create_column<DataTypeUInt8>({false, false, false, true}));
        auto res = dict->get_column("val", std::make_shared<DataTypeInt32>(), std::move(null_key),
                                    std::make_shared<DataTypeIPv6>());
        const auto* res_column = assert_cast<const ColumnNullable*>(res.get());
        EXPECT_EQ(4, res_column->size());

        // found , value not null
        EXPECT_FALSE(res_column->is_null_at(0));
        EXPECT_EQ(res_column->get_nested_column().get_int(0), 1);

        // found , vaule is null
        EXPECT_TRUE(res_column->is_null_at(1));

        // not found
        EXPECT_TRUE(res_column->is_null_at(2));

        // key is null
        EXPECT_TRUE(res_column->is_null_at(3));
    }
}

TEST(DictionaryGetNullableTest, testHashMapDictionaryMultiKey) {
    auto dict = create_complex_hash_map_dict_from_column(
            "dict",
            {ColumnWithTypeAndName(create_column<DataTypeInt32>({1, 2, 3, 4, 5}),
                                   std::make_shared<DataTypeInt32>(), "key1"),
             ColumnWithTypeAndName(create_column<DataTypeInt32>({1, 2, 3, 4, 5}),
                                   std::make_shared<DataTypeInt32>(), "key2")},

            {ColumnWithTypeAndName(
                     ColumnNullable::create(
                             create_column<DataTypeInt32>({1, 2, 3, 4, 5}),
                             create_column<DataTypeUInt8>({false, true, false, true, false})),
                     std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()), "val1"),
             ColumnWithTypeAndName(create_column<DataTypeInt32>({10, 20, 30, 40, 50}),
                                   std::make_shared<DataTypeInt32>(), "val2")});

    {
        ColumnPtrs key_columns {create_column<DataTypeInt32>({1, 2, 6}),
                                create_column<DataTypeInt32>({1, 2, 6})};

        auto res = dict->get_tuple_columns(
                {"val1", "val2"},
                {std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeInt32>()}, key_columns,
                {std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeInt32>()});

        const auto* res_column1 = assert_cast<const ColumnNullable*>(res[0].get());
        const auto* res_column2 = assert_cast<const ColumnNullable*>(res[1].get());
        EXPECT_EQ(3, res_column1->size());
        EXPECT_EQ(3, res_column2->size());

        // found , value not null
        EXPECT_FALSE(res_column1->is_null_at(0));
        EXPECT_EQ(res_column1->get_nested_column().get_int(0), 1);
        EXPECT_FALSE(res_column2->is_null_at(0));
        EXPECT_EQ(res_column2->get_nested_column().get_int(0), 10);

        // found ,  val1 is null val2 is not null
        EXPECT_TRUE(res_column1->is_null_at(1));
        EXPECT_FALSE(res_column2->is_null_at(1));
        EXPECT_EQ(res_column2->get_nested_column().get_int(1), 20);

        // not found
        EXPECT_TRUE(res_column1->is_null_at(2));
        EXPECT_TRUE(res_column2->is_null_at(2));
    }

    {
        ColumnPtrs key_columns {
                create_column<DataTypeInt32>({1, 2, 6, 3}),
                ColumnNullable::create(create_column<DataTypeInt32>({1, 2, 6, 3}),
                                       create_column<DataTypeUInt8>({false, false, false, true})),
        };

        auto res = dict->get_tuple_columns(
                {"val1", "val2"},
                {std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeInt32>()}, key_columns,
                {std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeInt32>()});

        const auto* res_column1 = assert_cast<const ColumnNullable*>(res[0].get());
        const auto* res_column2 = assert_cast<const ColumnNullable*>(res[1].get());
        EXPECT_EQ(4, res_column1->size());
        EXPECT_EQ(4, res_column2->size());

        // found , value not null
        EXPECT_FALSE(res_column1->is_null_at(0));
        EXPECT_EQ(res_column1->get_nested_column().get_int(0), 1);
        EXPECT_FALSE(res_column2->is_null_at(0));
        EXPECT_EQ(res_column2->get_nested_column().get_int(0), 10);

        // found ,  val1 is null val2 is not null
        EXPECT_TRUE(res_column1->is_null_at(1));
        EXPECT_FALSE(res_column2->is_null_at(1));
        EXPECT_EQ(res_column2->get_nested_column().get_int(1), 20);

        // not found
        EXPECT_TRUE(res_column1->is_null_at(2));
        EXPECT_TRUE(res_column2->is_null_at(2));

        // key is null
        EXPECT_TRUE(res_column1->is_null_at(3));
        EXPECT_TRUE(res_column2->is_null_at(3));
    }
}
} // namespace doris::vectorized
