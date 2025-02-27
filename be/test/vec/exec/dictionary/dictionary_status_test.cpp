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

TEST(DictionaryStatusTest, test) {
    auto dict_factory = std::make_shared<DictionaryFactory>();

    {
        auto dict = create_complex_hash_map_dict_from_column(
                "dict1",
                ColumnsWithTypeAndName {{DataTypeInt32::ColumnType::create(),
                                         std::make_shared<DataTypeInt32>(), ""}},
                ColumnsWithTypeAndName {
                        ColumnWithTypeAndName {DataTypeInt32::ColumnType::create(),
                                               std::make_shared<DataTypeInt32>(), ""},
                });
        EXPECT_TRUE(dict_factory->refresh_dict(1, 2, dict));
        EXPECT_TRUE(dict_factory->commit_refresh_dict(1, 2));
    }

    {
        auto dict = create_complex_hash_map_dict_from_column(
                "dict2",
                ColumnsWithTypeAndName {{DataTypeInt32::ColumnType::create(),
                                         std::make_shared<DataTypeInt32>(), ""}},
                ColumnsWithTypeAndName {
                        ColumnWithTypeAndName {DataTypeInt32::ColumnType::create(),
                                               std::make_shared<DataTypeInt32>(), ""},
                });

        EXPECT_TRUE(dict_factory->refresh_dict(2, 2, dict));
        EXPECT_TRUE(dict_factory->commit_refresh_dict(2, 2));
    }

    {
        std::vector<TDictionaryStatus> result;
        std::vector<int64_t> dict_ids {};

        dict_factory->get_dictionary_status(result, dict_ids);
        EXPECT_EQ(result.size(), 2);
        for (auto& status : result) {
            std::cout << status.dictionary_memory_size << "\t" << status.dictionary_id << "\t"
                      << status.version_id << std::endl;
        }
    }

    {
        std::vector<TDictionaryStatus> result;
        std::vector<int64_t> dict_ids {1};

        dict_factory->get_dictionary_status(result, dict_ids);
        EXPECT_EQ(result.size(), 1);
        for (auto& status : result) {
            std::cout << status.dictionary_memory_size << "\t" << status.dictionary_id << "\t"
                      << status.version_id << std::endl;
        }
    }

    {
        std::vector<TDictionaryStatus> result;
        std::vector<int64_t> dict_ids {1, 2};

        dict_factory->get_dictionary_status(result, dict_ids);
        EXPECT_EQ(result.size(), 2);
        for (auto& status : result) {
            std::cout << status.dictionary_memory_size << "\t" << status.dictionary_id << "\t"
                      << status.version_id << std::endl;
        }
    }

    {
        std::vector<TDictionaryStatus> result;
        std::vector<int64_t> dict_ids {1, 2, 3};

        dict_factory->get_dictionary_status(result, dict_ids);
        EXPECT_EQ(result.size(), 2);
        for (auto& status : result) {
            std::cout << status.dictionary_memory_size << "\t" << status.dictionary_id << "\t"
                      << status.version_id << std::endl;
        }
    }
}

} // namespace doris::vectorized
