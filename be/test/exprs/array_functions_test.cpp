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

#include "exprs/array_functions.h"

#include <gtest/gtest.h>

#include "gmock/gmock.h"
#include "runtime/collection_value.h"
#include "runtime/free_pool.hpp"
#include "string"
#include "testutil/function_utils.h"
#include "udf/udf.h"
#include "udf/udf_internal.h"

#define private public

namespace doris {

class ArrayFunctionsTest : public testing::Test {
public:
    ArrayFunctionsTest() {
        _utils = new FunctionUtils();
        _context = _utils->get_fn_ctx();
    }
    ~ArrayFunctionsTest() { delete _utils; }

public:
    FunctionUtils* _utils;
    FunctionContext* _context;
};

TEST_F(ArrayFunctionsTest, array) {
    // Int array
    {
        FunctionContext::TypeDesc childTypeDesc {};
        childTypeDesc.type = FunctionContext::TYPE_INT;

        _context->impl()->_return_type.type = FunctionContext::TYPE_ARRAY;
        _context->impl()->_return_type.children.clear();
        _context->impl()->_return_type.children.push_back(childTypeDesc);

        IntVal v[10];

        for (int i = 0; i < 10; ++i) {
            v[i].val = i + 1;
        }

        CollectionVal cv = ArrayFunctions::array(_context, 10, v);

        CollectionValue value = CollectionValue::from_collection_val(cv);

        int i = 0;
        for (auto&& iter = value.iterator(TYPE_INT); iter.has_next(); iter.next()) {
            i++;
            IntVal a;
            iter.value(&a);
            EXPECT_EQ(i, a.val);
        }
    }
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
