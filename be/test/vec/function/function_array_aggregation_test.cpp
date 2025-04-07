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

#include <cstddef>
#include <string>
#include <utility>
#include <vector>

#include "function_test_util.h"
#include "testutil/any_type.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {

template <typename T>
struct AnyValue {
    T value {};
    bool is_null = false;

    AnyValue(T v) : value(std::move(v)) {}

    AnyValue(std::nullptr_t) : is_null(true) {}
};

using IntDataSet = std::vector<std::pair<std::vector<AnyValue<int>>, AnyValue<int>>>;

template <typename To, typename From>
constexpr decltype(auto) convert_to(From value) {
    return static_cast<To::FieldType>(value);
}

template <typename T, typename ReturnType = T>
void check_function(const std::string& func_name, const IntDataSet data_set,
                    bool nullable = false) {
    InputTypeSet input_types;
    if (!nullable) {
        input_types = {TypeIndex::Array, T{}.get_type_id()};
    } else {
        input_types = {TypeIndex::Array, TypeIndex::Nullable, T{}.get_type_id()};
    }
    DataSet converted_data_set;
    for (const auto& row : data_set) {
        Array array;
        for (auto any_value : row.first) {
            if (any_value.is_null) {
                array.push_back(Field());
            } else {
                array.push_back(convert_to<T>(any_value.value));
            }
        }
        if (!row.second.is_null) {
            converted_data_set.emplace_back(std::make_pair<InputCell, Expect>(
                    {array}, convert_to<ReturnType>(row.second.value)));
        } else {
            converted_data_set.emplace_back(std::make_pair<InputCell, Expect>({array}, Null()));
        }
    }
    static_cast<void>(check_function<ReturnType, true>(func_name, input_types, converted_data_set));
}

TEST(VFunctionArrayAggregationTest, TestArrayMin) {
    const std::string func_name = "array_min";
    IntDataSet data_set = {
            {{}, nullptr},
            {{1, 2, 3}, 1},
    };
    static_cast<void>(check_function<DataTypeInt8>(func_name, data_set));
    static_cast<void>(check_function<DataTypeInt16>(func_name, data_set));
    static_cast<void>(check_function<DataTypeInt32>(func_name, data_set));
    static_cast<void>(check_function<DataTypeInt64>(func_name, data_set));
    static_cast<void>(check_function<DataTypeInt128>(func_name, data_set));
    static_cast<void>(check_function<DataTypeFloat32>(func_name, data_set));
    static_cast<void>(check_function<DataTypeFloat64>(func_name, data_set));
}

TEST(VFunctionArrayAggregationTest, TestArrayMinNullable) {
    const std::string func_name = "array_min";
    IntDataSet data_set = {
            {{}, nullptr},
            {{nullptr}, nullptr},
            {{1, nullptr, 3}, 1},
    };
    static_cast<void>(check_function<DataTypeInt8>(func_name, data_set, true));
    static_cast<void>(check_function<DataTypeInt16>(func_name, data_set, true));
    static_cast<void>(check_function<DataTypeInt32>(func_name, data_set, true));
    static_cast<void>(check_function<DataTypeInt64>(func_name, data_set, true));
    static_cast<void>(check_function<DataTypeInt128>(func_name, data_set, true));
    static_cast<void>(check_function<DataTypeFloat32>(func_name, data_set, true));
    static_cast<void>(check_function<DataTypeFloat64>(func_name, data_set, true));
}

TEST(VFunctionArrayAggregationTest, TestArrayMax) {
    const std::string func_name = "array_max";
    IntDataSet data_set = {
            {{}, nullptr},
            {{1, 2, 3}, 3},
    };
    static_cast<void>(check_function<DataTypeInt8>(func_name, data_set));
    static_cast<void>(check_function<DataTypeInt16>(func_name, data_set));
    static_cast<void>(check_function<DataTypeInt32>(func_name, data_set));
    static_cast<void>(check_function<DataTypeInt64>(func_name, data_set));
    static_cast<void>(check_function<DataTypeInt128>(func_name, data_set));
    static_cast<void>(check_function<DataTypeFloat32>(func_name, data_set));
    static_cast<void>(check_function<DataTypeFloat64>(func_name, data_set));
}

TEST(VFunctionArrayAggregationTest, TestArrayMaxNullable) {
    const std::string func_name = "array_max";
    IntDataSet data_set = {
            {{}, nullptr},
            {{nullptr}, nullptr},
            {{1, nullptr, 3}, 3},
    };
    static_cast<void>(check_function<DataTypeInt8>(func_name, data_set, true));
    static_cast<void>(check_function<DataTypeInt16>(func_name, data_set, true));
    static_cast<void>(check_function<DataTypeInt32>(func_name, data_set, true));
    static_cast<void>(check_function<DataTypeInt64>(func_name, data_set, true));
    static_cast<void>(check_function<DataTypeInt128>(func_name, data_set, true));
    static_cast<void>(check_function<DataTypeFloat32>(func_name, data_set, true));
    static_cast<void>(check_function<DataTypeFloat64>(func_name, data_set, true));
}

TEST(VFunctionArrayAggregationTest, TestArraySum) {
    const std::string func_name = "array_sum";
    IntDataSet data_set = {
            {{}, nullptr},
            {{1, 2, 3}, 6},
    };
    static_cast<void>(check_function<DataTypeInt8, DataTypeInt64>(func_name, data_set));
    static_cast<void>(check_function<DataTypeInt16, DataTypeInt64>(func_name, data_set));
    static_cast<void>(check_function<DataTypeInt32, DataTypeInt64>(func_name, data_set));
    static_cast<void>(check_function<DataTypeInt64, DataTypeInt64>(func_name, data_set));
    static_cast<void>(check_function<DataTypeInt128, DataTypeInt128>(func_name, data_set));
    static_cast<void>(check_function<DataTypeFloat32, DataTypeFloat64>(func_name, data_set));
    static_cast<void>(check_function<DataTypeFloat64, DataTypeFloat64>(func_name, data_set));
}

TEST(VFunctionArrayAggregationTest, TestArraySumNullable) {
    const std::string func_name = "array_sum";
    IntDataSet data_set = {
            {{}, nullptr},
            {{nullptr}, nullptr},
            {{1, nullptr, 3}, 4},
    };
    static_cast<void>(check_function<DataTypeInt8, DataTypeInt64>(func_name, data_set, true));
    static_cast<void>(check_function<DataTypeInt16, DataTypeInt64>(func_name, data_set, true));
    static_cast<void>(check_function<DataTypeInt32, DataTypeInt64>(func_name, data_set, true));
    static_cast<void>(check_function<DataTypeInt64, DataTypeInt64>(func_name, data_set, true));
    static_cast<void>(check_function<DataTypeInt128, DataTypeInt128>(func_name, data_set, true));
    static_cast<void>(check_function<DataTypeFloat32, DataTypeFloat64>(func_name, data_set, true));
    static_cast<void>(check_function<DataTypeFloat64, DataTypeFloat64>(func_name, data_set, true));
}

TEST(VFunctionArrayAggregationTest, TestArrayAverage) {
    const std::string func_name = "array_avg";
    IntDataSet data_set = {
            {{}, nullptr},
            {{1, 2, 3}, 2},
    };
    static_cast<void>(check_function<DataTypeInt8, DataTypeFloat64>(func_name, data_set));
    static_cast<void>(check_function<DataTypeInt16, DataTypeFloat64>(func_name, data_set));
    static_cast<void>(check_function<DataTypeInt32, DataTypeFloat64>(func_name, data_set));
    static_cast<void>(check_function<DataTypeInt64, DataTypeFloat64>(func_name, data_set));
    static_cast<void>(check_function<DataTypeInt128, DataTypeFloat64>(func_name, data_set));
    static_cast<void>(check_function<DataTypeFloat32, DataTypeFloat64>(func_name, data_set));
    static_cast<void>(check_function<DataTypeFloat64, DataTypeFloat64>(func_name, data_set));
}

TEST(VFunctionArrayAggregationTest, TestArrayAverageNullable) {
    const std::string func_name = "array_avg";
    IntDataSet data_set = {
            {{}, nullptr},
            {{nullptr}, nullptr},
            {{1, nullptr, 3}, 2},
    };
    static_cast<void>(check_function<DataTypeInt8, DataTypeFloat64>(func_name, data_set, true));
    static_cast<void>(check_function<DataTypeInt16, DataTypeFloat64>(func_name, data_set, true));
    static_cast<void>(check_function<DataTypeInt32, DataTypeFloat64>(func_name, data_set, true));
    static_cast<void>(check_function<DataTypeInt64, DataTypeFloat64>(func_name, data_set, true));
    static_cast<void>(check_function<DataTypeInt128, DataTypeFloat64>(func_name, data_set, true));
    static_cast<void>(check_function<DataTypeFloat32, DataTypeFloat64>(func_name, data_set, true));
    static_cast<void>(check_function<DataTypeFloat64, DataTypeFloat64>(func_name, data_set, true));
}

TEST(VFunctionArrayAggregationTest, TestArrayProduct) {
    const std::string func_name = "array_product";
    IntDataSet data_set = {
            {{}, nullptr},
            {{1, 2, 3}, 6},
    };
    static_cast<void>(check_function<DataTypeInt8, DataTypeFloat64>(func_name, data_set));
    static_cast<void>(check_function<DataTypeInt16, DataTypeFloat64>(func_name, data_set));
    static_cast<void>(check_function<DataTypeInt32, DataTypeFloat64>(func_name, data_set));
    static_cast<void>(check_function<DataTypeInt64, DataTypeFloat64>(func_name, data_set));
    static_cast<void>(check_function<DataTypeInt128, DataTypeFloat64>(func_name, data_set));
    static_cast<void>(check_function<DataTypeFloat32, DataTypeFloat64>(func_name, data_set));
    static_cast<void>(check_function<DataTypeFloat64, DataTypeFloat64>(func_name, data_set));
}

TEST(VFunctionArrayAggregationTest, TestArrayProductNullable) {
    const std::string func_name = "array_product";
    IntDataSet data_set = {
            {{}, nullptr},
            {{nullptr}, nullptr},
            {{1, nullptr, 3}, 3},
    };
    static_cast<void>(check_function<DataTypeInt8, DataTypeFloat64>(func_name, data_set, true));
    static_cast<void>(check_function<DataTypeInt16, DataTypeFloat64>(func_name, data_set, true));
    static_cast<void>(check_function<DataTypeInt32, DataTypeFloat64>(func_name, data_set, true));
    static_cast<void>(check_function<DataTypeInt64, DataTypeFloat64>(func_name, data_set, true));
    static_cast<void>(check_function<DataTypeInt128, DataTypeFloat64>(func_name, data_set, true));
    static_cast<void>(check_function<DataTypeFloat32, DataTypeFloat64>(func_name, data_set, true));
    static_cast<void>(check_function<DataTypeFloat64, DataTypeFloat64>(func_name, data_set, true));
}

} // namespace doris::vectorized

