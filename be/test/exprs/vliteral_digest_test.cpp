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

#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "exprs/vliteral.h"
#include "testutil/mock/mock_fn_call.h"

namespace doris {

TEST(VLiteralDigestTest, ComplexNullDisablesParentDigest) {
    for (auto type : {TYPE_BITMAP, TYPE_HLL, TYPE_QUANTILE_STATE}) {
        SCOPED_TRACE(type_to_string(type));
        auto data_type = DataTypeFactory::instance().create_data_type(type, true);
        auto literal = VLiteral::create_shared(data_type, Field());
        EXPECT_EQ(literal->get_digest(42), 0);

        // A later scalar child must not revive a digest disabled by the complex literal.
        auto parent = MockFnCall::create("if");
        parent->add_child(literal);
        parent->add_child(VLiteral::create_shared(std::make_shared<DataTypeInt32>(),
                                                  Field::create_field<TYPE_INT>(1)));
        EXPECT_EQ(parent->get_digest(42), 0);
    }
}

TEST(VLiteralDigestTest, NonNullComplexValuesDisableDigest) {
    auto bitmap = VLiteral::create_shared(
            DataTypeFactory::instance().create_data_type(TYPE_BITMAP, false),
            Field::create_field<TYPE_BITMAP>(BitmapValue(1)));
    auto hll =
            VLiteral::create_shared(DataTypeFactory::instance().create_data_type(TYPE_HLL, false),
                                    Field::create_field<TYPE_HLL>(HyperLogLog()));
    auto quantile = VLiteral::create_shared(
            DataTypeFactory::instance().create_data_type(TYPE_QUANTILE_STATE, false),
            Field::create_field<TYPE_QUANTILE_STATE>(QuantileState()));
    EXPECT_EQ(bitmap->get_digest(42), 0);
    EXPECT_EQ(hll->get_digest(42), 0);
    EXPECT_EQ(quantile->get_digest(42), 0);
}

TEST(VLiteralDigestTest, ScalarValuesKeepDistinctStableDigests) {
    auto int_type = std::make_shared<DataTypeInt32>();
    auto one = VLiteral::create_shared(int_type, Field::create_field<TYPE_INT>(1));
    auto two = VLiteral::create_shared(int_type, Field::create_field<TYPE_INT>(2));
    EXPECT_NE(one->get_digest(42), 0);
    EXPECT_EQ(one->get_digest(42), one->get_digest(42));
    EXPECT_NE(one->get_digest(42), two->get_digest(42));
    EXPECT_NE(one->get_digest(42), one->get_digest(43));

    auto string_type = std::make_shared<DataTypeString>();
    auto a = VLiteral::create_shared(string_type, Field::create_field<TYPE_STRING>(String("a")));
    auto b = VLiteral::create_shared(string_type, Field::create_field<TYPE_STRING>(String("b")));
    EXPECT_NE(a->get_digest(42), 0);
    EXPECT_NE(a->get_digest(42), b->get_digest(42));

    auto scalar_null = VLiteral::create_shared(
            DataTypeFactory::instance().create_data_type(TYPE_INT, true), Field());
    EXPECT_NE(scalar_null->get_digest(42), 0);
}

} // namespace doris
