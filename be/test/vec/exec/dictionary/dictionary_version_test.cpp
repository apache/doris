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

#include <memory>

#include "vec/core/columns_with_type_and_name.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/complex_hash_map_dictionary.h"
#include "vec/functions/dictionary.h"
#include "vec/functions/dictionary_factory.h"

namespace doris::vectorized {

TEST(DictionaryVersionTest, refresh_dict) {
    auto dict_factory = std::make_shared<DictionaryFactory>();

    auto dict = create_complex_hash_map_dict_from_column(
            "ip dict",
            ColumnsWithTypeAndName {
                    {DataTypeInt32::ColumnType::create(), std::make_shared<DataTypeInt32>(), ""}},
            ColumnsWithTypeAndName {
                    ColumnWithTypeAndName {DataTypeInt32::ColumnType::create(),
                                           std::make_shared<DataTypeInt32>(), ""},
            });
    EXPECT_TRUE(dict_factory->refresh_dict(1, 1, dict));
    EXPECT_EQ(dict_factory->_refreshing_dict_map[1].first, 1);

    EXPECT_TRUE(dict_factory->refresh_dict(1, 114, dict));
    EXPECT_EQ(dict_factory->_refreshing_dict_map[1].first, 114);

    EXPECT_TRUE(dict_factory->refresh_dict(2, 114, dict));
    EXPECT_EQ(dict_factory->_refreshing_dict_map[2].first, 114);
}

TEST(DictionaryVersionTest, abort_refresh_dict) {
    auto dict_factory = std::make_shared<DictionaryFactory>();

    auto dict = create_complex_hash_map_dict_from_column(
            "ip dict",
            ColumnsWithTypeAndName {
                    {DataTypeInt32::ColumnType::create(), std::make_shared<DataTypeInt32>(), ""}},
            ColumnsWithTypeAndName {
                    ColumnWithTypeAndName {DataTypeInt32::ColumnType::create(),
                                           std::make_shared<DataTypeInt32>(), ""},
            });
    EXPECT_TRUE(dict_factory->refresh_dict(3, 5, dict));
    EXPECT_EQ(dict_factory->_refreshing_dict_map[3].first, 5);

    {
        auto status = dict_factory->abort_refresh_dict(2, 5);
        EXPECT_TRUE(status.ok());
    }

    {
        auto status = dict_factory->abort_refresh_dict(3, 6);
        EXPECT_FALSE(status.ok());
        std::cout << status.msg() << std::endl;
    }

    {
        auto status = dict_factory->abort_refresh_dict(3, 5);
        EXPECT_TRUE(status.ok());
        EXPECT_TRUE(!dict_factory->_refreshing_dict_map.contains(3));
    }
}

TEST(DictionaryVersionTest, commit_dict) {
    auto dict_factory = std::make_shared<DictionaryFactory>();

    auto dict = create_complex_hash_map_dict_from_column(
            "ip dict",
            ColumnsWithTypeAndName {
                    {DataTypeInt32::ColumnType::create(), std::make_shared<DataTypeInt32>(), ""}},
            ColumnsWithTypeAndName {
                    ColumnWithTypeAndName {DataTypeInt32::ColumnType::create(),
                                           std::make_shared<DataTypeInt32>(), ""},
            });
    EXPECT_TRUE(dict_factory->refresh_dict(3, 5, dict));
    EXPECT_EQ(dict_factory->_refreshing_dict_map[3].first, 5);

    {
        auto status = dict_factory->commit_refresh_dict(2, 5);
        EXPECT_FALSE(status.ok());
        std::cout << status.msg() << std::endl;
        EXPECT_EQ(dict_factory->_refreshing_dict_map[3].first, 5);
    }

    {
        auto status = dict_factory->commit_refresh_dict(3, 6);
        EXPECT_FALSE(status.ok());
        std::cout << status.msg() << std::endl;
        EXPECT_EQ(dict_factory->_refreshing_dict_map[3].first, 5);
    }

    {
        auto status = dict_factory->commit_refresh_dict(3, 5);
        EXPECT_TRUE(status.ok());
        EXPECT_TRUE(!dict_factory->_refreshing_dict_map.contains(3));
        EXPECT_TRUE(dict_factory->_dict_id_to_dict_map.contains(3));
        EXPECT_EQ(dict_factory->_dict_id_to_version_id_map[3], 5);
    }

    auto dict2 = create_complex_hash_map_dict_from_column(
            "ip dict",
            ColumnsWithTypeAndName {
                    {DataTypeInt32::ColumnType::create(), std::make_shared<DataTypeInt32>(), ""}},
            ColumnsWithTypeAndName {
                    ColumnWithTypeAndName {DataTypeInt32::ColumnType::create(),
                                           std::make_shared<DataTypeInt32>(), ""},
            });
    EXPECT_TRUE(dict_factory->refresh_dict(3, 6, dict));
    EXPECT_TRUE(dict_factory->_refreshing_dict_map.contains(3));

    {
        auto status = dict_factory->commit_refresh_dict(3, 5);
        EXPECT_FALSE(status.ok());
        std::cout << status.msg() << std::endl;
    }
    {
        auto status = dict_factory->commit_refresh_dict(3, 6);
        EXPECT_TRUE(status.ok());
        EXPECT_TRUE(!dict_factory->_refreshing_dict_map.contains(3));
        EXPECT_TRUE(dict_factory->_dict_id_to_dict_map.contains(3));
        EXPECT_EQ(dict_factory->_dict_id_to_version_id_map[3], 6);
    }

    auto dict3 = create_complex_hash_map_dict_from_column(
            "ip dict",
            ColumnsWithTypeAndName {
                    {DataTypeInt32::ColumnType::create(), std::make_shared<DataTypeInt32>(), ""}},
            ColumnsWithTypeAndName {
                    ColumnWithTypeAndName {DataTypeInt32::ColumnType::create(),
                                           std::make_shared<DataTypeInt32>(), ""},
            });
    EXPECT_TRUE(dict_factory->refresh_dict(3, 4, dict));
    EXPECT_TRUE(dict_factory->_refreshing_dict_map.contains(3));

    {
        auto status = dict_factory->commit_refresh_dict(3, 4);
        EXPECT_FALSE(status.ok());
        std::cout << status.msg() << std::endl;
    }
}

} // namespace doris::vectorized
