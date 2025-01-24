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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <memory>
#include <variant>
#include <vector>

#include "exec/hash_map/cast_type.h"
#include "pipeline/common/distinct_agg_utils.h"
#include "vec/columns/column.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
namespace doris::vectorized {

struct DistinctAggHashMapParams {
    ColumnRawPtrs key_columns;
    Columns key_columns_holder;
    DataTypes key_types;
    size_t except_size;

    vectorized::Arena arena;
};

template <typename DataType>
// column and unique size
std::pair<ColumnPtr, size_t> create_column_duplicates() {
    using FieldType = typename DataType::FieldType;

    std::vector<FieldType> datas;

    if constexpr (std::is_same_v<DataType, DataTypeString>) {
        std::vector<FieldType> {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}.swap(datas);
    } else {
        std::vector<FieldType> {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}.swap(datas);
    }

    auto column = DataType::ColumnType::create();

    if constexpr (std::is_same_v<DataType, DataTypeString>) {
        for (auto data : datas) {
            column->insert_data(data.data(), data.size());
        }
        for (auto data : datas) {
            column->insert_data(data.data(), data.size());
        }
    } else {
        for (auto data : datas) {
            column->insert_value(data);
        }
        for (auto data : datas) {
            column->insert_value(data);
        }
    }
    auto column_ptr = column->clone();
    return std::make_pair(std::move(column_ptr), datas.size());
}

void create_key_column(DistinctAggHashMapParams& params) {
    std::vector<ColumnPtr> key_columns;
    std::vector<ColumnPtr> key_columns_holder;
    std::vector<size_t> except_sizes;
    // using LeftDataType = std::decay_t<decltype(left)>;
    for (auto type : params.key_types) {
        auto vaild = cast_type(type.get(), [&](auto&& t) {
            using Type = std::decay_t<decltype(t)>;
            auto [column, size] = create_column_duplicates<Type>();

            params.key_columns.push_back(column.get());
            params.key_columns_holder.push_back(column);
            params.except_size = size; // except size
            return true;
        });
        EXPECT_TRUE(vaild);
    }
}

void test_for_params(DistinctAggHashMapParams& params) {
    DistinctDataVariants distinct_data;

    EXPECT_TRUE(init_hash_method(&distinct_data, params.key_types, true).ok());

    std::visit(vectorized::Overload {
                       [&](std::monostate& arg) -> void {
                           throw doris::Exception(ErrorCode::INTERNAL_ERROR, "uninited hash table");
                       },
                       [&](auto& agg_method) -> void {
                           using HashMethodType = std::decay_t<decltype(agg_method)>;
                           using AggState = typename HashMethodType::State;
                           auto& key_columns = params.key_columns;
                           auto num_rows = params.key_columns[0]->size();
                           auto& arena = params.arena;
                           AggState state(key_columns);
                           agg_method.init_serialized_keys(key_columns, num_rows);
                           size_t row = 0;
                           auto creator = [&](const auto& ctor, auto& key, auto& origin) {
                               HashMethodType::try_presis_key(key, origin, arena);
                               ctor(key);
                           };
                           auto creator_for_null_key = [&]() {};
                           for (; row < num_rows; ++row) {
                               agg_method.lazy_emplace(state, row, creator, creator_for_null_key);
                           }
                       }},
               distinct_data.method_variant);

    std::visit(
            vectorized::Overload {[&](std::monostate& arg) {
                                      // Do nothing
                                  },
                                  [&](auto& agg_method) {
                                      EXPECT_EQ(agg_method.hash_table->size(), params.except_size);
                                      std::cout << "hasmap size : " << agg_method.hash_table->size()
                                                << std::endl;
                                  }},
            distinct_data.method_variant);
}

void test_for_types(DataTypes types) {
    DistinctAggHashMapParams params;
    params.key_types = types;
    create_key_column(params);
    test_for_params(params);
}

TEST(DistinctAggHashMapTest, optimismtest) {
    test_for_types({std::make_shared<DataTypeInt32>()});
    test_for_types({std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeInt32>()});
    test_for_types({std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeInt32>(),
                    std::make_shared<DataTypeInt32>()});
    test_for_types({std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeInt32>(),
                    std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeInt32>()});
    test_for_types({std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeInt64>(),
                    std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeInt64>()});
}

TEST(DistinctAggHashMapTest, enumeratetest) {
    DataTypes all_types = {std::make_shared<DataTypeInt8>(),    std::make_shared<DataTypeInt16>(),
                           std::make_shared<DataTypeInt32>(),   std::make_shared<DataTypeInt64>(),
                           std::make_shared<DataTypeFloat32>(), std::make_shared<DataTypeFloat64>(),
                           std::make_shared<DataTypeString>()};

    for (size_t i = 0; i < all_types.size(); ++i) {
        test_for_types({all_types[i]});
        for (size_t j = i + 1; j < all_types.size(); ++j) {
            test_for_types({all_types[i], all_types[j]});
            for (size_t k = j + 1; k < all_types.size(); ++k) {
                test_for_types({all_types[i], all_types[j], all_types[k]});
                for (size_t l = k + 1; l < all_types.size(); ++l) {
                    test_for_types({all_types[i], all_types[j], all_types[k], all_types[l]});
                }
            }
        }
    }
}

TEST(DistinctAggHashMapTest, specialtest) {
    DistinctAggHashMapParams params;
    params.key_types = {std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeInt32>()};

    {
        auto column = ColumnInt32::create();
        column->insert_value(1);
        column->insert_value(1);
        column->insert_value(1);

        params.key_columns.push_back(column.get());
        params.key_columns_holder.push_back(std::move(column));
    }
    {
        auto column = ColumnInt32::create();
        column->insert_value(1);
        column->insert_value(2);
        column->insert_value(1);

        params.key_columns.push_back(column.get());
        params.key_columns_holder.push_back(std::move(column));
    }

    params.except_size = 2; // except size
    // {1,1} {1,2} {1,1}
    test_for_params(params);
}

} // namespace doris::vectorized
