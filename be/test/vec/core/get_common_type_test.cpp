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

#include <string>

#include "gtest/gtest_pred_impl.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_jsonb.h"
#include "vec/data_types/data_type_nothing.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_time_v2.h"
#include "vec/data_types/get_least_supertype.h"

namespace doris::vectorized {

static bool operator==(const IDataType& left, const IDataType& right) {
    return left.equals(right);
}

} // namespace doris::vectorized

using namespace doris::vectorized;

static DataTypePtr typeFromString(const std::string& str) {
    if (str == "Nothing") {
        return std::make_shared<DataTypeNothing>();
    } else if (str == "UInt8") {
        return std::make_shared<DataTypeUInt8>();
    } else if (str == "UInt16") {
        return std::make_shared<DataTypeUInt32>();
    } else if (str == "UInt32") {
        return std::make_shared<DataTypeUInt32>();
    } else if (str == "UInt64") {
        return std::make_shared<DataTypeUInt64>();
    } else if (str == "Int8") {
        return std::make_shared<DataTypeInt8>();
    } else if (str == "Int16") {
        return std::make_shared<DataTypeInt16>();
    } else if (str == "Int32") {
        return std::make_shared<DataTypeInt32>();
    } else if (str == "Int64") {
        return std::make_shared<DataTypeInt64>();
    } else if (str == "Float32") {
        return std::make_shared<DataTypeFloat32>();
    } else if (str == "Float64") {
        return std::make_shared<DataTypeFloat64>();
    } else if (str == "Date") {
        return std::make_shared<DataTypeDateV2>();
    } else if (str == "DateTime") {
        return std::make_shared<DataTypeDateTimeV2>();
    } else if (str == "String") {
        return std::make_shared<DataTypeString>();
    } else if (str == "Jsonb") {
        return std::make_shared<DataTypeJsonb>();
    }
    return nullptr;
}

static auto typesFromString(const std::string& str) {
    std::istringstream data_types_stream(str); // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    DataTypes data_types;
    std::string data_type;
    while (data_types_stream >> data_type) {
        data_types.push_back(typeFromString(data_type));
    }

    return data_types;
}

struct TypesTestCase {
    const char* from_types = nullptr;
    const char* expected_type = nullptr;
};

std::ostream& operator<<(std::ostream& ostr, const TypesTestCase& test_case) {
    ostr << "TypesTestCase{\"" << test_case.from_types << "\", ";
    if (test_case.expected_type) {
        ostr << "\"" << test_case.expected_type << "\"";
    } else {
        ostr << "nullptr";
    }
    return ostr << "}";
}

class TypeTest : public ::testing::TestWithParam<TypesTestCase> {
public:
    void SetUp() override {
        const auto& p = GetParam();
        from_types = typesFromString(p.from_types);

        if (p.expected_type) {
            expected_type = typeFromString(p.expected_type);
        } else {
            expected_type.reset();
        }
    }

    DataTypes from_types;
    DataTypePtr expected_type;
};

class LeastSuperTypeTest : public TypeTest {};

TEST_P(LeastSuperTypeTest, getLeastSupertype) {
    DataTypePtr result_type;
    if (this->expected_type) {
        get_least_supertype_jsonb(this->from_types, &result_type);
        std::cout << std::endl
                  << " " << this->expected_type->get_name() << " " << result_type->get_name()
                  << std::endl;
        ASSERT_EQ(*(this->expected_type), *result_type);
    } else {
        EXPECT_ANY_THROW(get_least_supertype_jsonb(this->from_types, &result_type));
    }
}

INSTANTIATE_TEST_SUITE_P(data_type, LeastSuperTypeTest,
                         ::testing::ValuesIn(std::initializer_list<TypesTestCase> {
                                 {"Nothing", "Nothing"},
                                 {"UInt8", "UInt8"},
                                 {"UInt8 UInt8", "UInt8"},
                                 {"Int8 Int8", "Int8"},
                                 {"UInt8 Int8", "Int16"},
                                 {"UInt8 Int16", "Int16"},
                                 {"UInt8 UInt32 UInt64", "UInt64"},
                                 {"Int8 Int32 Int64", "Int64"},
                                 {"UInt8 UInt32 Int64", "Int64"},
                                 {"Float32 Float64", "Float64"},
                                 {"Date Date", "Date"},
                                 {"Float32 UInt16 Int32", "Float64"},
                                 {"Float32 Int16 UInt32", "Float64"},
                                 {"String String String", "String"}}));
