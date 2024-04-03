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

#include <gmock/gmock-actions.h>
#include <gmock/gmock-matchers.h>
#include <gmock/gmock-spec-builders.h>
#include <gtest/gtest-matchers.h>

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "exprs/mock_vexpr.h"
#include "gtest/gtest_pred_impl.h"
#include "testutil/any_type.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/exprs/table_function/vexplode.h"
#include "vec/exprs/table_function/vexplode_numbers.h"
#include "vec/exprs/table_function/vexplode_split.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/function/function_test_util.h"

namespace doris::vectorized {

using ::testing::_;
using ::testing::DoAll;
using ::testing::Return;
using ::testing::SetArgPointee;

class TableFunctionTest : public testing::Test {
protected:
    virtual void SetUp() {}
    virtual void TearDown() {}

    void clear() {
        _ctx = nullptr;
        _root = nullptr;
        _children.clear();
        _column_ids.clear();
    }

    void init_expr_context(int child_num) {
        clear();

        _root = std::make_shared<MockVExpr>();
        for (int i = 0; i < child_num; ++i) {
            _column_ids.push_back(i);
            _children.push_back(std::make_shared<MockVExpr>());
            EXPECT_CALL(*_children[i], execute(_, _, _))
                    .WillRepeatedly(DoAll(SetArgPointee<2>(_column_ids[i]), Return(Status::OK())));
            _root->add_child(_children[i]);
        }
        _ctx = std::make_shared<VExprContext>(_root);
    }

private:
    VExprContextSPtr _ctx;
    std::shared_ptr<MockVExpr> _root;
    std::vector<std::shared_ptr<MockVExpr>> _children;
    std::vector<int> _column_ids;
};

TEST_F(TableFunctionTest, vexplode_outer) {
    init_expr_context(1);
    VExplodeTableFunction explode_outer;
    explode_outer.set_outer();
    explode_outer.set_expr_context(_ctx);

    // explode_outer(Array<Int32>)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::Int32};
        Array vec = {Int32(1), Null(), Int32(2), Int32(3)};
        InputDataSet input_set = {{vec}, {Null()}, {Array()}};

        InputTypeSet output_types = {TypeIndex::Int32};
        InputDataSet output_set = {{Int32(1)}, {Null()}, {Int32(2)},
                                   {Int32(3)}, {Null()}, {Null()}};

        check_vec_table_function(&explode_outer, input_types, input_set, output_types, output_set);
    }

    // explode_outer(Array<String>)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::String};
        Array vec = {std::string("abc"), std::string(""), std::string("def")};
        InputDataSet input_set = {{Null()}, {Array()}, {vec}};

        InputTypeSet output_types = {TypeIndex::String};
        InputDataSet output_set = {
                {Null()}, {Null()}, {std::string("abc")}, {std::string("")}, {std::string("def")}};

        check_vec_table_function(&explode_outer, input_types, input_set, output_types, output_set);
    }

    // explode_outer(Array<Decimal>)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::Decimal128V2};
        Array vec = {ut_type::DECIMALFIELD(17014116.67), ut_type::DECIMALFIELD(-17014116.67)};
        InputDataSet input_set = {{Null()}, {Array()}, {vec}};

        InputTypeSet output_types = {TypeIndex::Decimal128V2};
        InputDataSet output_set = {{Null()},
                                   {Null()},
                                   {ut_type::DECIMAL(17014116.67)},
                                   {ut_type::DECIMAL(-17014116.67)}};

        check_vec_table_function(&explode_outer, input_types, input_set, output_types, output_set);
    }
}

TEST_F(TableFunctionTest, vexplode) {
    init_expr_context(1);
    VExplodeTableFunction explode;
    explode.set_expr_context(_ctx);

    // explode(Array<Int32>)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::Int32};

        Array vec = {Int32(1), Null(), Int32(2), Int32(3)};
        InputDataSet input_set = {{vec}, {Null()}, {Array()}};

        InputTypeSet output_types = {TypeIndex::Int32};
        InputDataSet output_set = {{Int32(1)}, {Null()}, {Int32(2)}, {Int32(3)}};

        check_vec_table_function(&explode, input_types, input_set, output_types, output_set);
    }

    // explode(Array<String>)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::String};
        Array vec = {std::string("abc"), std::string(""), std::string("def")};
        InputDataSet input_set = {{Null()}, {Array()}, {vec}};

        InputTypeSet output_types = {TypeIndex::String};
        InputDataSet output_set = {{std::string("abc")}, {std::string("")}, {std::string("def")}};

        check_vec_table_function(&explode, input_types, input_set, output_types, output_set);
    }

    // explode(Array<Date>)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::Date};
        Array vec = {Null(), str_to_date_time("2022-01-02", false)};
        InputDataSet input_set = {{Null()}, {Array()}, {vec}};

        InputTypeSet output_types = {TypeIndex::Date};
        InputDataSet output_set = {{Null()}, {std::string("2022-01-02")}};

        check_vec_table_function(&explode, input_types, input_set, output_types, output_set);
    }
}

TEST_F(TableFunctionTest, vexplode_numbers) {
    init_expr_context(1);
    VExplodeNumbersTableFunction tfn;
    tfn.set_expr_context(_ctx);

    {
        InputTypeSet input_types = {TypeIndex::Int32};
        InputDataSet input_set = {{Int32(2)}, {Int32(3)}, {Null()}, {Int32(0)}, {Int32(-2)}};

        InputTypeSet output_types = {TypeIndex::Int32};
        InputDataSet output_set = {{Int32(0)}, {Int32(1)}, {Int32(0)}, {Int32(1)}, {Int32(2)}};

        check_vec_table_function(&tfn, input_types, input_set, output_types, output_set);
    }
}

TEST_F(TableFunctionTest, vexplode_split) {
    init_expr_context(2);
    VExplodeSplitTableFunction tfn;
    tfn.set_expr_context(_ctx);

    {
        // Case 1: explode_split(null) --- null
        // Case 2: explode_split("a,b,c", ",") --> ["a", "b", "c"]
        // Case 3: explode_split("a,b,c", "a,")) --> ["", "b,c"]
        // Case 4: explode_split("", ",")) --> [""]
        InputTypeSet input_types = {TypeIndex::String, Consted {TypeIndex::String}};
        InputDataSet input_sets = {{std::string("a,b,c"), std::string(",")},
                                   {std::string("a,b,c"), std::string("a,")},
                                   {std::string(""), std::string(",")}};

        InputTypeSet output_types = {TypeIndex::String};
        InputDataSet output_sets = {{std::string("a"), std::string("b"), std::string("c")},
                                    {std::string(""), std::string("b,c")},
                                    {std::string("")}};

        for (int i = 0; i < input_sets.size(); ++i) {
            InputDataSet input_set {input_sets[i]};
            InputDataSet output_set {};
            for (const auto& data : output_sets[i]) {
                output_set.emplace_back(std::vector<AnyType> {data});
            }
            check_vec_table_function(&tfn, input_types, input_set, output_types, output_set);
        }
    }
}

} // namespace doris::vectorized
