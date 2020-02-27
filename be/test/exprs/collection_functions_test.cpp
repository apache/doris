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
#include "gmock/gmock.h"

#include "string"
#include "udf/udf.h"
#include "runtime/collection_value.h"
#include "exprs/collection_functions.h"
#include "runtime/free_pool.hpp"
#include "testutil/function_utils.h"

#define private public

namespace doris {

class CollectionFunctionsTest : public testing::Test {

public:
    CollectionFunctionsTest() {
        _utils = new FunctionUtils();
        _context = _utils->get_fn_ctx();
    }
    ~CollectionFunctionsTest() {
        delete _utils;
    }

public:
    FunctionUtils* _utils;
    FunctionContext* _context;
};

TEST_F(CollectionFunctionsTest, array) {
    
    void* a = new uint8_t[0];
    LOG(WARNING) << "a: " << a;
    // Int array
    {
        FunctionContext::TypeDesc childTypeDesc;
        childTypeDesc.type = FunctionContext::TYPE_INT;

        _context->impl()->_return_type.type = FunctionContext::TYPE_ARRAY;
        _context->impl()->_return_type.children.clear();
        _context->impl()->_return_type.children.push_back(childTypeDesc);

        IntVal v[10];

        for (int i = 0; i < 10; ++i) {
            v[i].val = i + 1;
        }
        
        CollectionVal cv = CollectionFunctions::array(_context, 10, v);
        
        CollectionValue value = CollectionValue::from_collection_val(cv);

        int i = 0;
        for (auto&& iter = value.iterator(TYPE_INT); iter.has_next() ; iter.next()) {
            i++;
            IntVal a;
            iter.value(&a);
            EXPECT_EQ(i, a.val);
        }
    }

    {
        FunctionContext::TypeDesc childTypeDesc;
        childTypeDesc.type = FunctionContext::TYPE_VARCHAR;

        _context->impl()->_return_type.type = FunctionContext::TYPE_ARRAY;
        _context->impl()->_return_type.children.clear();
        _context->impl()->_return_type.children.push_back(childTypeDesc);

        StringVal v[10];

        for (int i = 0; i < 10; ++i) {
            v[i] = StringVal(std::to_string(i).c_str());
        }

        CollectionVal cv = CollectionFunctions::array(_context, 10, v);

        CollectionValue value = CollectionValue::from_collection_val(cv);

        int i = 0;
        for (auto&& iter = value.iterator(TYPE_VARCHAR); iter.has_next() ; iter.next()) {
            StringVal a;
            iter.value(&a);
            EXPECT_EQ(0, strcmp(std::to_string(i).c_str(), (char*) a.ptr));
            i++;
        }
    }


}

}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
