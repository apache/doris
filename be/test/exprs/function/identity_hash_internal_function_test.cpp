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
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "common/status.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column_const.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "exprs/function/simple_function_factory.h"
#include "exprs/function_context.h"
#include "runtime/runtime_state.h"
#include "testutil/mock/mock_runtime_state.h"
#include "util/raw_value.h"

namespace doris {

// Unit tests for FunctionIdentityHashInternal's execute_impl defensive validation. FE rejects
// malformed calls at analysis time; the BE checks guard against any path that delivers a
// non-constant, missing, or non-positive bucket count constant (e.g. forged thrift), and must
// return an error instead of crashing (the previous DCHECK-based guards aborted the process).
class IdentityHashInternalFunctionTest : public ::testing::Test {
protected:
    void SetUp() override {
        _fn = SimpleFunctionFactory::instance().get_function(
                "identity_hash_internal", _block.get_columns_with_type_and_name(), _return_type, {},
                BeExecVersionManager::get_newest_version());
        ASSERT_NE(_fn, nullptr);
    }

    // A non-null, non-const Int32 argument column.
    static ColumnWithTypeAndName make_int32_column(const std::string& name,
                                                   const std::vector<int32_t>& values) {
        auto column = ColumnInt32::create();
        for (auto v : values) {
            column->insert_value(v);
        }
        ColumnPtr result = std::move(column);
        return {std::move(result), std::make_shared<DataTypeInt32>(), name};
    }

    // The trailing bucket-count argument as a BIGINT constant (what FE delivers for a literal).
    static ColumnWithTypeAndName make_bigint_const(int64_t value) {
        auto column = ColumnInt64::create();
        column->insert_value(value);
        ColumnPtr result = std::move(column);
        result = ColumnConst::create(result, 1);
        return {std::move(result), std::make_shared<DataTypeInt64>(), "mod"};
    }

    Status execute(const ColumnNumbers& arguments, uint32_t result) {
        auto context = FunctionContext::create_context(&_state, _return_type, _argument_types);
        context->set_constant_cols(_constant_cols);
        _block.insert({nullptr, _return_type, "result"});
        _status = _fn->execute(context.get(), _block, arguments, result, 3);
        return _status;
    }

    void set_constant_col(size_t index, const ColumnPtr& column) {
        if (_constant_cols.size() <= index) {
            _constant_cols.resize(index + 1);
        }
        _constant_cols[index] = std::make_shared<ColumnPtrWrapper>(column);
    }

    DataTypes _argument_types {std::make_shared<DataTypeInt32>(),
                               std::make_shared<DataTypeInt64>()};
    DataTypePtr _return_type = std::make_shared<DataTypeInt64>();
    FunctionBasePtr _fn;
    MockRuntimeState _state;
    Block _block {make_int32_column("c1", {1, 2, 3}), make_bigint_const(8)};
    std::vector<std::shared_ptr<ColumnPtrWrapper>> _constant_cols;
    Status _status;
};

// The happy path: a valid constant bucket count still computes the identity bucket.
TEST_F(IdentityHashInternalFunctionTest, ValidConstantBucketCount) {
    set_constant_col(1, _block.get_by_position(1).column);
    ASSERT_TRUE(execute({0, 1}, 2).ok()) << _status.to_string();
    // identity bucket of values 1,2,3 with mod 8: 1%8, 2%8, 3%8
    const auto* res = assert_cast<const ColumnInt64*>(_block.get_by_position(2).column.get());
    ASSERT_EQ(res->size(), 3);
    EXPECT_EQ(res->get_element(0), 1);
    EXPECT_EQ(res->get_element(1), 2);
    EXPECT_EQ(res->get_element(2), 3);
}

// Non-positive bucket counts must return an error, not crash (previously DCHECK abort / UB).
TEST_F(IdentityHashInternalFunctionTest, RejectsZeroBucketCount) {
    _block = ::doris::Block {make_int32_column("c1", {1, 2, 3}), make_bigint_const(0)};
    set_constant_col(1, _block.get_by_position(1).column);
    auto st = execute({0, 1}, 2);
    ASSERT_FALSE(st.ok());
    EXPECT_TRUE(st.is<ErrorCode::INVALID_ARGUMENT>());
    EXPECT_NE(st.to_string().find("positive integer"), std::string::npos) << st.to_string();
}

TEST_F(IdentityHashInternalFunctionTest, RejectsNegativeBucketCount) {
    _block = ::doris::Block {make_int32_column("c1", {1, 2, 3}), make_bigint_const(-8)};
    set_constant_col(1, _block.get_by_position(1).column);
    auto st = execute({0, 1}, 2);
    ASSERT_FALSE(st.ok());
    EXPECT_TRUE(st.is<ErrorCode::INVALID_ARGUMENT>());
    EXPECT_NE(st.to_string().find("positive integer"), std::string::npos) << st.to_string();
}

// A missing (nullptr) constant column for the bucket count must return an error, not
// dereference nullptr (previously the DCHECK guarded release builds nowhere).
TEST_F(IdentityHashInternalFunctionTest, RejectsMissingConstantColumn) {
    auto st = execute({0, 1}, 2);
    ASSERT_FALSE(st.ok());
    EXPECT_TRUE(st.is<ErrorCode::INVALID_ARGUMENT>());
}

} // namespace doris
