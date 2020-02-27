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

#include "string"
#include "util/bitmap.h"
#include "runtime/collection_value.h"

#define private public

namespace doris {

TEST(CollectionValueTest, init) {
    {
        CollectionValue cv;

        ObjectPool pool;
        EXPECT_TRUE(CollectionValue::init_collection(&pool, 10, TYPE_INT, &cv).ok());

        EXPECT_EQ(10, cv.size());

        for (int j = 0; j < 10; ++j) {
            EXPECT_FALSE(*(cv._null_signs + j));
        }

        EXPECT_FALSE(CollectionValue::init_collection(&pool, 10, TYPE_INT, nullptr).ok());

        CollectionValue cv_null;
        EXPECT_TRUE(CollectionValue::init_collection(&pool, 0, TYPE_INT, &cv_null).ok());
        EXPECT_EQ(0, cv_null.size());
    }
    
    {
        CollectionValue cv;
        ObjectPool pool;
        EXPECT_TRUE(CollectionValue::init_collection(&pool, 10, TYPE_INT, &cv).ok());
    }
    
}

TEST(CollectionValueTest, set) {
    CollectionValue cv;
    ObjectPool pool;
    EXPECT_TRUE(CollectionValue::init_collection(&pool, 10, TYPE_INT, &cv).ok());

    // normal
    {
        IntVal v0 = IntVal::null();
        cv.set(0, TYPE_INT, &v0);
        for (int j = 1; j < cv.size(); ++j) {
            IntVal i(j + 10);
            ASSERT_TRUE(cv.set(j, TYPE_INT, &i).ok());
        }

    }

    {
        auto iter = cv.iterator(TYPE_INT);
        IntVal v0;
        iter.value(&v0);
        ASSERT_TRUE(v0.is_null);
        ASSERT_TRUE(iter.is_null());
        iter.next();
        for (int k = 1; k < cv.size(); ++k, iter.next()) {
            IntVal v;
            iter.value(&v);
            EXPECT_EQ(k + 10, v.val);
        }
    }
    
    // over size
    {
        IntVal intv(20);
        ASSERT_FALSE(cv.set(10, TYPE_INT, &intv).ok());
    }
}


TEST(CollectionValueTest, iter) {
    CollectionValue cv;
    ObjectPool pool;
    EXPECT_TRUE(CollectionValue::init_collection(&pool, 10, TYPE_VARCHAR, &cv).ok());

    // normal
    {
        StringVal v0 = StringVal::null();
        cv.set(0, TYPE_VARCHAR, &v0);
        for (int j = 1; j < cv.size(); ++j) {
            StringVal i(std::to_string(j).c_str());
            ASSERT_TRUE(cv.set(j, TYPE_VARCHAR, &i).ok());
        }
    }
    {
        auto iter = cv.iterator(TYPE_VARCHAR);
        EXPECT_TRUE(nullptr == iter.value());
        
        iter.next();
        for (int i = 0; iter.has_next(); iter.next(), i++) {
            EXPECT_TRUE(nullptr != iter.value());
            
            StringVal s;
            iter.value(&s);
            EXPECT_EQ(0, strncmp((char *)s.ptr, std::to_string(i).c_str(), s.len));
        }
    }
}

}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
