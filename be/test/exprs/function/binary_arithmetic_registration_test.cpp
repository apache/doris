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
#include <string>
#include <vector>

#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_nullable.h"
#include "exprs/function/simple_function_factory.h"

namespace doris {

// Pins the decimal registration surface of the binary arithmetic functions.
//
// FE casts both children of Add/Subtract/Mod to exactly the return type
// (TypeCoercionUtils#processDecimalV3BinaryArithmetic), so BE registers only
// same-width decimal pairs for them. Multiply is exempt from that cast and
// must keep the full width cross product. If someone re-adds mixed-width
// add/subtract/mod registrations (paying ~2/3 of the template instantiations
// of those TUs for unreachable code) or drops a reachable signature, these
// tests fail.
class BinaryArithmeticRegistrationTest : public testing::Test {
protected:
    static FunctionBasePtr lookup(const std::string& name, const DataTypePtr& left,
                                  const DataTypePtr& right, const DataTypePtr& return_type) {
        ColumnsWithTypeAndName arguments {{nullptr, left, ""}, {nullptr, right, ""}};
        return SimpleFunctionFactory::instance().get_function(name, arguments, return_type);
    }

    static std::vector<DataTypePtr> v3_widths() {
        return {std::make_shared<DataTypeDecimal<TYPE_DECIMAL32>>(),
                std::make_shared<DataTypeDecimal<TYPE_DECIMAL64>>(),
                std::make_shared<DataTypeDecimal<TYPE_DECIMAL128I>>(),
                std::make_shared<DataTypeDecimal<TYPE_DECIMAL256>>()};
    }
};

TEST_F(BinaryArithmeticRegistrationTest, same_width_add_subtract_resolvable) {
    for (const auto& name : {std::string("add"), std::string("subtract")}) {
        for (const auto& type : v3_widths()) {
            auto function = lookup(name, type, type, type);
            ASSERT_NE(function, nullptr) << name << " " << type->get_name();
            EXPECT_EQ(function->get_name(), name) << type->get_name();
        }
    }
}

TEST_F(BinaryArithmeticRegistrationTest, same_width_mod_resolvable) {
    for (const auto& type : v3_widths()) {
        // FunctionMod always infers a nullable return type (mod by zero -> NULL).
        auto function = lookup("mod", type, type, make_nullable(type));
        ASSERT_NE(function, nullptr) << type->get_name();
        EXPECT_EQ(function->get_name(), "mod") << type->get_name();
    }
}

TEST_F(BinaryArithmeticRegistrationTest, mixed_width_add_subtract_mod_unresolvable) {
    auto widths = v3_widths();
    for (const auto& left : widths) {
        for (const auto& right : widths) {
            if (left->get_primitive_type() == right->get_primitive_type()) {
                continue;
            }
            for (const auto& name : {std::string("add"), std::string("subtract")}) {
                EXPECT_EQ(lookup(name, left, right, left), nullptr)
                        << name << " " << left->get_name() << " x " << right->get_name();
            }
            EXPECT_EQ(lookup("mod", left, right, make_nullable(left)), nullptr)
                    << "mod " << left->get_name() << " x " << right->get_name();
        }
    }
}

TEST_F(BinaryArithmeticRegistrationTest, multiply_full_cross_product_retained) {
    auto widths = v3_widths();
    for (const auto& left : widths) {
        for (const auto& right : widths) {
            auto function = lookup("multiply", left, right, left);
            ASSERT_NE(function, nullptr)
                    << "multiply " << left->get_name() << " x " << right->get_name();
            EXPECT_EQ(function->get_name(), "multiply");
        }
    }
}

TEST_F(BinaryArithmeticRegistrationTest, decimalv2_still_resolvable) {
    auto type = std::make_shared<DataTypeDecimalV2>();
    for (const auto& name :
         {std::string("add"), std::string("subtract"), std::string("multiply")}) {
        auto function = lookup(name, type, type, type);
        ASSERT_NE(function, nullptr) << name;
        EXPECT_EQ(function->get_name(), name);
    }
    auto mod_function = lookup("mod", type, type, make_nullable(type));
    ASSERT_NE(mod_function, nullptr);
    EXPECT_EQ(mod_function->get_name(), "mod");
}

} // namespace doris
