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
#include "vec/functions/dictionary.h"
#include "vec/functions/dictionary_factory.h"
#include "vec/functions/hash_map_dictionary.h"

namespace doris::vectorized {

TEST(DictionaryVersionTest, test) {
    auto dict_factory = std::make_shared<DictionaryFactory>();

    auto dict = create_hash_map_dict_from_column(
            "ip dict",
            ColumnWithTypeAndName {DataTypeInt32::ColumnType::create(),
                                   std::make_shared<DataTypeInt32>(), ""},
            ColumnsWithTypeAndName {
                    ColumnWithTypeAndName {DataTypeInt32::ColumnType::create(),
                                           std::make_shared<DataTypeInt32>(), ""},
            });

    {
        auto st = dict_factory->register_dict(1, 2, dict);
        EXPECT_TRUE(st.ok());
    }

    {
        auto st = dict_factory->register_dict(2, 2, dict);
        EXPECT_TRUE(st.ok());
    }

    {
        auto st = dict_factory->register_dict(1, 2, dict);
        EXPECT_FALSE(st.ok());
        std::cout << st.msg() << std::endl;
    }

    {
        auto dict = dict_factory->get(2, 2);
        EXPECT_TRUE(dict != nullptr);
    }

    {
        auto dict = dict_factory->get(2, 3);
        EXPECT_TRUE(dict == nullptr);
    }

    {
        auto dict = dict_factory->get(1, 2);
        EXPECT_TRUE(dict != nullptr);
    }

    {
        auto dict = dict_factory->get(1, 3);
        EXPECT_TRUE(dict == nullptr);
    }

    {
        auto st = dict_factory->delete_dict(3);
        EXPECT_TRUE(st.ok());
    }

    {
        auto st = dict_factory->delete_dict(1);
        EXPECT_TRUE(st.ok());
    }

    {
        auto dict = dict_factory->get(1, 2);
        EXPECT_TRUE(dict == nullptr);
    }
}

} // namespace doris::vectorized
