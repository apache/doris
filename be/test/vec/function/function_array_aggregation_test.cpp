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
#include "runtime/define_primitive_type.h"
#include "testutil/any_type.h"
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
//TODO: this interlayer could be removed totally
template <typename DataType, typename ReturnType = DataType>
void check_function_array_wrapper(const std::string& func_name, const IntDataSet data_set,
                                  PrimitiveType t_idx, PrimitiveType ret_t_idx,
                                  bool nullable = false) {
    InputTypeSet input_types;
    if (!nullable) {
        input_types = {PrimitiveType::TYPE_ARRAY, t_idx};
    } else {
        input_types = {PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_NULL, t_idx};
    }
    DataSet converted_data_set;
    for (const auto& row : data_set) {
        TestArray array;
        for (auto any_value : row.first) {
            if (any_value.is_null) {
                array.emplace_back(Null());
            } else {
                //ATTN: these static cast is necessary, because AnyType contains certain type! store int8 and get
                // int32 will cause error
                array.emplace_back(static_cast<DataType::FieldType>(any_value.value));
            }
        }
        if (!row.second.is_null) {
            converted_data_set.emplace_back(std::make_pair<InputCell, Expect>(
                    {AnyType {array}}, static_cast<ReturnType::FieldType>(row.second.value)));
        } else {
            converted_data_set.emplace_back(
                    std::make_pair<InputCell, Expect>({AnyType {array}}, Null()));
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
    static_cast<void>(check_function_array_wrapper<DataTypeInt8>(
            func_name, data_set, PrimitiveType::TYPE_TINYINT, PrimitiveType::TYPE_TINYINT));
    static_cast<void>(check_function_array_wrapper<DataTypeInt16>(
            func_name, data_set, PrimitiveType::TYPE_SMALLINT, PrimitiveType::TYPE_SMALLINT));
    static_cast<void>(check_function_array_wrapper<DataTypeInt32>(
            func_name, data_set, PrimitiveType::TYPE_INT, PrimitiveType::TYPE_INT));
    static_cast<void>(check_function_array_wrapper<DataTypeInt64>(
            func_name, data_set, PrimitiveType::TYPE_BIGINT, PrimitiveType::TYPE_BIGINT));
    static_cast<void>(check_function_array_wrapper<DataTypeInt128>(
            func_name, data_set, PrimitiveType::TYPE_LARGEINT, PrimitiveType::TYPE_LARGEINT));
    static_cast<void>(check_function_array_wrapper<DataTypeFloat32>(
            func_name, data_set, PrimitiveType::TYPE_FLOAT, PrimitiveType::TYPE_FLOAT));
    static_cast<void>(check_function_array_wrapper<DataTypeFloat64>(
            func_name, data_set, PrimitiveType::TYPE_DOUBLE, PrimitiveType::TYPE_DOUBLE));
    // test ipv4
    IntDataSet data_set1 = {
            {{}, nullptr},
            {{"192.168.1.1", "192.168.1.2", "192.168.1.3"}, "192.168.1.1"},
    };
    static_cast<void>(check_function_array_wrapper<DataTypeIPv4>(
            func_name, data_set1, PrimitiveType::TYPE_IPV4, PrimitiveType::TYPE_IPV4));
    // test ipv6
    IntDataSet data_set2 = {
            {{}, nullptr},
            {{"2001:db8:3333:4444:5555:6666:7777:8888", "2001:db8:3333:4444:5555:6666:7777:8888",
              "2001:db8:3333:4444:5555:6666:7777:8888"},
             "2001:db8:3333:4444:5555:6666:7777:8888"},
    };
    static_cast<void>(check_function_array_wrapper<DataTypeIPv6>(
            func_name, data_set2, PrimitiveType::TYPE_IPV6, PrimitiveType::TYPE_IPV6));
}

TEST(VFunctionArrayAggregationTest, TestArrayMinNullable) {
    const std::string func_name = "array_min";
    IntDataSet data_set = {
            {{}, nullptr},
            {{nullptr}, nullptr},
            {{1, nullptr, 3}, 1},
    };
    static_cast<void>(check_function_array_wrapper<DataTypeInt8>(
            func_name, data_set, PrimitiveType::TYPE_TINYINT, PrimitiveType::TYPE_TINYINT, true));
    static_cast<void>(check_function_array_wrapper<DataTypeInt16>(
            func_name, data_set, PrimitiveType::TYPE_SMALLINT, PrimitiveType::TYPE_SMALLINT, true));
    static_cast<void>(check_function_array_wrapper<DataTypeInt32>(
            func_name, data_set, PrimitiveType::TYPE_INT, PrimitiveType::TYPE_INT, true));
    static_cast<void>(check_function_array_wrapper<DataTypeInt64>(
            func_name, data_set, PrimitiveType::TYPE_BIGINT, PrimitiveType::TYPE_BIGINT, true));
    static_cast<void>(check_function_array_wrapper<DataTypeInt128>(
            func_name, data_set, PrimitiveType::TYPE_LARGEINT, PrimitiveType::TYPE_LARGEINT, true));
    static_cast<void>(check_function_array_wrapper<DataTypeFloat32>(
            func_name, data_set, PrimitiveType::TYPE_FLOAT, PrimitiveType::TYPE_FLOAT, true));
    static_cast<void>(check_function_array_wrapper<DataTypeFloat64>(
            func_name, data_set, PrimitiveType::TYPE_DOUBLE, PrimitiveType::TYPE_DOUBLE, true));
    // test ipv4
    IntDataSet data_set1 = {
            {{}, nullptr},
            {{nullptr}, nullptr},
            {{"192.168.1.1", nullptr, "192.168.1.3"}, "192.168.1.1"},
    };
    static_cast<void>(check_function_array_wrapper<DataTypeIPv4>(
            func_name, data_set1, PrimitiveType::TYPE_IPV4, PrimitiveType::TYPE_IPV4, true));
    // test ipv6
    IntDataSet data_set2 = {
            {{}, nullptr},
            {{nullptr}, nullptr},
            {{"2001:db8:3333:4444:5555:6666:7777:8888", nullptr,
              "2001:db8:3333:4444:5555:6666:7777:8888"},
             "2001:db8:3333:4444:5555:6666:7777:8888"},
    };
    static_cast<void>(check_function_array_wrapper<DataTypeIPv6>(
            func_name, data_set2, PrimitiveType::TYPE_IPV6, PrimitiveType::TYPE_IPV6, true));
}

TEST(VFunctionArrayAggregationTest, TestArrayMax) {
    const std::string func_name = "array_max";
    IntDataSet data_set = {
            {{}, nullptr},
            {{1, 2, 3}, 3},
    };
    static_cast<void>(check_function_array_wrapper<DataTypeInt8>(
            func_name, data_set, PrimitiveType::TYPE_TINYINT, PrimitiveType::TYPE_TINYINT));
    static_cast<void>(check_function_array_wrapper<DataTypeInt16>(
            func_name, data_set, PrimitiveType::TYPE_SMALLINT, PrimitiveType::TYPE_SMALLINT));
    static_cast<void>(check_function_array_wrapper<DataTypeInt32>(
            func_name, data_set, PrimitiveType::TYPE_INT, PrimitiveType::TYPE_INT));
    static_cast<void>(check_function_array_wrapper<DataTypeInt64>(
            func_name, data_set, PrimitiveType::TYPE_BIGINT, PrimitiveType::TYPE_BIGINT));
    static_cast<void>(check_function_array_wrapper<DataTypeInt128>(
            func_name, data_set, PrimitiveType::TYPE_LARGEINT, PrimitiveType::TYPE_LARGEINT));
    static_cast<void>(check_function_array_wrapper<DataTypeFloat32>(
            func_name, data_set, PrimitiveType::TYPE_FLOAT, PrimitiveType::TYPE_FLOAT));
    static_cast<void>(check_function_array_wrapper<DataTypeFloat64>(
            func_name, data_set, PrimitiveType::TYPE_DOUBLE, PrimitiveType::TYPE_DOUBLE));
    // test ipv4
    IntDataSet data_set1 = {
            {{}, nullptr},
            {{"192.168.1.1", "192.168.1.2", "192.168.1.3"}, "192.168.1.3"},
    };
    static_cast<void>(check_function_array_wrapper<DataTypeIPv4>(
            func_name, data_set1, PrimitiveType::TYPE_IPV4, PrimitiveType::TYPE_IPV4));
    // test ipv6
    IntDataSet data_set2 = {
            {{}, nullptr},
            {{"2001:db8:3333:4444:5555:6666:7777:8888", "2001:db8:3333:4444:5555:6666:7777:8888",
              "2001:db8:3333:4444:5555:6666:7777:8888"},
             "2001:db8:3333:4444:5555:6666:7777:8888"},
    };
    static_cast<void>(check_function_array_wrapper<DataTypeIPv6>(
            func_name, data_set2, PrimitiveType::TYPE_IPV6, PrimitiveType::TYPE_IPV6));
}

TEST(VFunctionArrayAggregationTest, TestArrayMaxNullable) {
    const std::string func_name = "array_max";
    IntDataSet data_set = {
            {{}, nullptr},
            {{nullptr}, nullptr},
            {{1, nullptr, 3}, 3},
    };
    static_cast<void>(check_function_array_wrapper<DataTypeInt8>(
            func_name, data_set, PrimitiveType::TYPE_TINYINT, PrimitiveType::TYPE_TINYINT, true));
    static_cast<void>(check_function_array_wrapper<DataTypeInt16>(
            func_name, data_set, PrimitiveType::TYPE_SMALLINT, PrimitiveType::TYPE_SMALLINT, true));
    static_cast<void>(check_function_array_wrapper<DataTypeInt32>(
            func_name, data_set, PrimitiveType::TYPE_INT, PrimitiveType::TYPE_INT, true));
    static_cast<void>(check_function_array_wrapper<DataTypeInt64>(
            func_name, data_set, PrimitiveType::TYPE_BIGINT, PrimitiveType::TYPE_BIGINT, true));
    static_cast<void>(check_function_array_wrapper<DataTypeInt128>(
            func_name, data_set, PrimitiveType::TYPE_LARGEINT, PrimitiveType::TYPE_LARGEINT, true));
    static_cast<void>(check_function_array_wrapper<DataTypeFloat32>(
            func_name, data_set, PrimitiveType::TYPE_FLOAT, PrimitiveType::TYPE_FLOAT, true));
    static_cast<void>(check_function_array_wrapper<DataTypeFloat64>(
            func_name, data_set, PrimitiveType::TYPE_DOUBLE, PrimitiveType::TYPE_DOUBLE, true));
    // test ipv4
    IntDataSet data_set1 = {
            {{}, nullptr},
            {{nullptr}, nullptr},
            {{"192.168.1.1", nullptr, "192.168.1.3"}, "192.168.1.3"},
    };
    static_cast<void>(check_function_array_wrapper<DataTypeIPv4>(
            func_name, data_set1, PrimitiveType::TYPE_IPV4, PrimitiveType::TYPE_IPV4, true));
    // test ipv6
    IntDataSet data_set2 = {
            {{}, nullptr},
            {{nullptr}, nullptr},
            {{"2001:db8:3333:4444:5555:6666:7777:8888", nullptr,
              "2001:db8:3333:4444:5555:6666:7777:8888"},
             "2001:db8:3333:4444:5555:6666:7777:8888"},
    };
    static_cast<void>(check_function_array_wrapper<DataTypeIPv6>(
            func_name, data_set2, PrimitiveType::TYPE_IPV6, PrimitiveType::TYPE_IPV6, true));
}

TEST(VFunctionArrayAggregationTest, TestArraySum) {
    const std::string func_name = "array_sum";
    IntDataSet data_set = {
            {{}, nullptr},
            {{1, 2, 3}, 6},
    };
    static_cast<void>(check_function_array_wrapper<DataTypeInt8, DataTypeInt64>(
            func_name, data_set, PrimitiveType::TYPE_TINYINT, PrimitiveType::TYPE_BIGINT));
    static_cast<void>(check_function_array_wrapper<DataTypeInt16, DataTypeInt64>(
            func_name, data_set, PrimitiveType::TYPE_SMALLINT, PrimitiveType::TYPE_BIGINT));
    static_cast<void>(check_function_array_wrapper<DataTypeInt32, DataTypeInt64>(
            func_name, data_set, PrimitiveType::TYPE_INT, PrimitiveType::TYPE_BIGINT));
    static_cast<void>(check_function_array_wrapper<DataTypeInt64, DataTypeInt64>(
            func_name, data_set, PrimitiveType::TYPE_BIGINT, PrimitiveType::TYPE_BIGINT));
    static_cast<void>(check_function_array_wrapper<DataTypeInt128, DataTypeInt128>(
            func_name, data_set, PrimitiveType::TYPE_LARGEINT, PrimitiveType::TYPE_LARGEINT));
    static_cast<void>(check_function_array_wrapper<DataTypeFloat32, DataTypeFloat64>(
            func_name, data_set, PrimitiveType::TYPE_FLOAT, PrimitiveType::TYPE_DOUBLE));
    static_cast<void>(check_function_array_wrapper<DataTypeFloat64, DataTypeFloat64>(
            func_name, data_set, PrimitiveType::TYPE_DOUBLE, PrimitiveType::TYPE_DOUBLE));
}

TEST(VFunctionArrayAggregationTest, TestArraySumNullable) {
    const std::string func_name = "array_sum";
    IntDataSet data_set = {
            {{}, nullptr},
            {{nullptr}, nullptr},
            {{1, nullptr, 3}, 4},
    };
    static_cast<void>(check_function_array_wrapper<DataTypeInt8, DataTypeInt64>(
            func_name, data_set, PrimitiveType::TYPE_TINYINT, PrimitiveType::TYPE_BIGINT, true));
    static_cast<void>(check_function_array_wrapper<DataTypeInt16, DataTypeInt64>(
            func_name, data_set, PrimitiveType::TYPE_SMALLINT, PrimitiveType::TYPE_BIGINT, true));
    static_cast<void>(check_function_array_wrapper<DataTypeInt32, DataTypeInt64>(
            func_name, data_set, PrimitiveType::TYPE_INT, PrimitiveType::TYPE_BIGINT, true));
    static_cast<void>(check_function_array_wrapper<DataTypeInt64, DataTypeInt64>(
            func_name, data_set, PrimitiveType::TYPE_BIGINT, PrimitiveType::TYPE_BIGINT, true));
    static_cast<void>(check_function_array_wrapper<DataTypeInt128, DataTypeInt128>(
            func_name, data_set, PrimitiveType::TYPE_LARGEINT, PrimitiveType::TYPE_LARGEINT, true));
    static_cast<void>(check_function_array_wrapper<DataTypeFloat32, DataTypeFloat64>(
            func_name, data_set, PrimitiveType::TYPE_FLOAT, PrimitiveType::TYPE_DOUBLE, true));
    static_cast<void>(check_function_array_wrapper<DataTypeFloat64, DataTypeFloat64>(
            func_name, data_set, PrimitiveType::TYPE_DOUBLE, PrimitiveType::TYPE_DOUBLE, true));
}

TEST(VFunctionArrayAggregationTest, TestArrayAverage) {
    const std::string func_name = "array_avg";
    IntDataSet data_set = {
            {{}, nullptr},
            {{1, 2, 3}, 2},
    };
    static_cast<void>(check_function_array_wrapper<DataTypeInt8, DataTypeFloat64>(
            func_name, data_set, PrimitiveType::TYPE_TINYINT, PrimitiveType::TYPE_DOUBLE));
    static_cast<void>(check_function_array_wrapper<DataTypeInt16, DataTypeFloat64>(
            func_name, data_set, PrimitiveType::TYPE_SMALLINT, PrimitiveType::TYPE_DOUBLE));
    static_cast<void>(check_function_array_wrapper<DataTypeInt32, DataTypeFloat64>(
            func_name, data_set, PrimitiveType::TYPE_INT, PrimitiveType::TYPE_DOUBLE));
    static_cast<void>(check_function_array_wrapper<DataTypeInt64, DataTypeFloat64>(
            func_name, data_set, PrimitiveType::TYPE_BIGINT, PrimitiveType::TYPE_DOUBLE));
    static_cast<void>(check_function_array_wrapper<DataTypeInt128, DataTypeFloat64>(
            func_name, data_set, PrimitiveType::TYPE_LARGEINT, PrimitiveType::TYPE_DOUBLE));
    static_cast<void>(check_function_array_wrapper<DataTypeFloat32, DataTypeFloat64>(
            func_name, data_set, PrimitiveType::TYPE_FLOAT, PrimitiveType::TYPE_DOUBLE));
    static_cast<void>(check_function_array_wrapper<DataTypeFloat64, DataTypeFloat64>(
            func_name, data_set, PrimitiveType::TYPE_DOUBLE, PrimitiveType::TYPE_DOUBLE));
}

TEST(VFunctionArrayAggregationTest, TestArrayAverageNullable) {
    const std::string func_name = "array_avg";
    IntDataSet data_set = {
            {{}, nullptr},
            {{nullptr}, nullptr},
            {{1, nullptr, 3}, 2},
    };
    static_cast<void>(check_function_array_wrapper<DataTypeInt8, DataTypeFloat64>(
            func_name, data_set, PrimitiveType::TYPE_TINYINT, PrimitiveType::TYPE_DOUBLE, true));
    static_cast<void>(check_function_array_wrapper<DataTypeInt16, DataTypeFloat64>(
            func_name, data_set, PrimitiveType::TYPE_SMALLINT, PrimitiveType::TYPE_DOUBLE, true));
    static_cast<void>(check_function_array_wrapper<DataTypeInt32, DataTypeFloat64>(
            func_name, data_set, PrimitiveType::TYPE_INT, PrimitiveType::TYPE_DOUBLE, true));
    static_cast<void>(check_function_array_wrapper<DataTypeInt64, DataTypeFloat64>(
            func_name, data_set, PrimitiveType::TYPE_BIGINT, PrimitiveType::TYPE_DOUBLE, true));
    static_cast<void>(check_function_array_wrapper<DataTypeInt128, DataTypeFloat64>(
            func_name, data_set, PrimitiveType::TYPE_LARGEINT, PrimitiveType::TYPE_DOUBLE, true));
    static_cast<void>(check_function_array_wrapper<DataTypeFloat32, DataTypeFloat64>(
            func_name, data_set, PrimitiveType::TYPE_FLOAT, PrimitiveType::TYPE_DOUBLE, true));
    static_cast<void>(check_function_array_wrapper<DataTypeFloat64, DataTypeFloat64>(
            func_name, data_set, PrimitiveType::TYPE_DOUBLE, PrimitiveType::TYPE_DOUBLE, true));
}

TEST(VFunctionArrayAggregationTest, TestArrayProduct) {
    const std::string func_name = "array_product";
    IntDataSet data_set = {
            {{}, nullptr},
            {{1, 2, 3}, 6},
    };
    static_cast<void>(check_function_array_wrapper<DataTypeInt8, DataTypeFloat64>(
            func_name, data_set, PrimitiveType::TYPE_TINYINT, PrimitiveType::TYPE_DOUBLE));
    static_cast<void>(check_function_array_wrapper<DataTypeInt16, DataTypeFloat64>(
            func_name, data_set, PrimitiveType::TYPE_SMALLINT, PrimitiveType::TYPE_DOUBLE));
    static_cast<void>(check_function_array_wrapper<DataTypeInt32, DataTypeFloat64>(
            func_name, data_set, PrimitiveType::TYPE_INT, PrimitiveType::TYPE_DOUBLE));
    static_cast<void>(check_function_array_wrapper<DataTypeInt64, DataTypeFloat64>(
            func_name, data_set, PrimitiveType::TYPE_BIGINT, PrimitiveType::TYPE_DOUBLE));
    static_cast<void>(check_function_array_wrapper<DataTypeInt128, DataTypeFloat64>(
            func_name, data_set, PrimitiveType::TYPE_LARGEINT, PrimitiveType::TYPE_DOUBLE));
    static_cast<void>(check_function_array_wrapper<DataTypeFloat32, DataTypeFloat64>(
            func_name, data_set, PrimitiveType::TYPE_FLOAT, PrimitiveType::TYPE_DOUBLE));
    static_cast<void>(check_function_array_wrapper<DataTypeFloat64, DataTypeFloat64>(
            func_name, data_set, PrimitiveType::TYPE_DOUBLE, PrimitiveType::TYPE_DOUBLE));
}

TEST(VFunctionArrayAggregationTest, TestArrayProductNullable) {
    const std::string func_name = "array_product";
    IntDataSet data_set = {
            {{}, nullptr},
            {{nullptr}, nullptr},
            {{1, nullptr, 3}, 3},
    };
    static_cast<void>(check_function_array_wrapper<DataTypeInt8, DataTypeFloat64>(
            func_name, data_set, PrimitiveType::TYPE_TINYINT, PrimitiveType::TYPE_DOUBLE, true));
    static_cast<void>(check_function_array_wrapper<DataTypeInt16, DataTypeFloat64>(
            func_name, data_set, PrimitiveType::TYPE_SMALLINT, PrimitiveType::TYPE_DOUBLE, true));
    static_cast<void>(check_function_array_wrapper<DataTypeInt32, DataTypeFloat64>(
            func_name, data_set, PrimitiveType::TYPE_INT, PrimitiveType::TYPE_DOUBLE, true));
    static_cast<void>(check_function_array_wrapper<DataTypeInt64, DataTypeFloat64>(
            func_name, data_set, PrimitiveType::TYPE_BIGINT, PrimitiveType::TYPE_DOUBLE, true));
    static_cast<void>(check_function_array_wrapper<DataTypeInt128, DataTypeFloat64>(
            func_name, data_set, PrimitiveType::TYPE_LARGEINT, PrimitiveType::TYPE_DOUBLE, true));
    static_cast<void>(check_function_array_wrapper<DataTypeFloat32, DataTypeFloat64>(
            func_name, data_set, PrimitiveType::TYPE_FLOAT, PrimitiveType::TYPE_DOUBLE, true));
    static_cast<void>(check_function_array_wrapper<DataTypeFloat64, DataTypeFloat64>(
            func_name, data_set, PrimitiveType::TYPE_DOUBLE, PrimitiveType::TYPE_DOUBLE, true));
}

} // namespace doris::vectorized
