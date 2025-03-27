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

#include "exprs/hybrid_set.h"

#include <gtest/gtest.h>

#include <string>

#include "common/config.h"
#include "exprs/create_predicate_function.h"

namespace doris {

// mock
class HybridSetTest : public testing::Test {
public:
    HybridSetTest() {}

protected:
};

TEST_F(HybridSetTest, bool) {
    HybridSetBase* set = create_set(TYPE_BOOLEAN);
    bool a = true;
    set->insert(&a);
    a = false;
    set->insert(&a);
    a = true;
    set->insert(&a);
    a = false;
    set->insert(&a);

    EXPECT_EQ(2, set->size());
    HybridSetBase::IteratorBase* base = set->begin();

    while (base->has_next()) {
        LOG(INFO) << (*(bool*)base->get_value());
        base->next();
    }

    a = true;
    EXPECT_TRUE(set->find(&a));
    a = false;
    EXPECT_TRUE(set->find(&a));
}

TEST_F(HybridSetTest, tinyint) {
    HybridSetBase* set = create_set(TYPE_TINYINT);
    int8_t a = 0;
    set->insert(&a);
    a = 1;
    set->insert(&a);
    a = 2;
    set->insert(&a);
    a = 3;
    set->insert(&a);
    a = 4;
    set->insert(&a);
    a = 4;
    set->insert(&a);

    EXPECT_EQ(5, set->size());

    HybridSetBase::IteratorBase* base = set->begin();

    while (base->has_next()) {
        LOG(INFO) << (*(int8_t*)base->get_value());
        base->next();
    }

    a = 0;
    EXPECT_TRUE(set->find(&a));
    a = 1;
    EXPECT_TRUE(set->find(&a));
    a = 2;
    EXPECT_TRUE(set->find(&a));
    a = 3;
    EXPECT_TRUE(set->find(&a));
    a = 4;
    EXPECT_TRUE(set->find(&a));
    a = 5;
    EXPECT_FALSE(set->find(&a));
}
TEST_F(HybridSetTest, smallint) {
    HybridSetBase* set = create_set(TYPE_SMALLINT);
    int16_t a = 0;
    set->insert(&a);
    a = 1;
    set->insert(&a);
    a = 2;
    set->insert(&a);
    a = 3;
    set->insert(&a);
    a = 4;
    set->insert(&a);
    a = 4;
    set->insert(&a);

    EXPECT_EQ(5, set->size());
    HybridSetBase::IteratorBase* base = set->begin();

    while (base->has_next()) {
        LOG(INFO) << (*(int16_t*)base->get_value());
        base->next();
    }

    a = 0;
    EXPECT_TRUE(set->find(&a));
    a = 1;
    EXPECT_TRUE(set->find(&a));
    a = 2;
    EXPECT_TRUE(set->find(&a));
    a = 3;
    EXPECT_TRUE(set->find(&a));
    a = 4;
    EXPECT_TRUE(set->find(&a));
    a = 5;
    EXPECT_FALSE(set->find(&a));
}
TEST_F(HybridSetTest, int) {
    HybridSetBase* set = create_set(TYPE_INT);
    int32_t a = 0;
    set->insert(&a);
    a = 1;
    set->insert(&a);
    a = 2;
    set->insert(&a);
    a = 3;
    set->insert(&a);
    a = 4;
    set->insert(&a);
    a = 4;
    set->insert(&a);

    EXPECT_EQ(5, set->size());
    HybridSetBase::IteratorBase* base = set->begin();

    while (base->has_next()) {
        LOG(INFO) << (*(int32_t*)base->get_value());
        base->next();
    }

    a = 0;
    EXPECT_TRUE(set->find(&a));
    a = 1;
    EXPECT_TRUE(set->find(&a));
    a = 2;
    EXPECT_TRUE(set->find(&a));
    a = 3;
    EXPECT_TRUE(set->find(&a));
    a = 4;
    EXPECT_TRUE(set->find(&a));
    a = 5;
    EXPECT_FALSE(set->find(&a));
}
TEST_F(HybridSetTest, bigint) {
    HybridSetBase* set = create_set(TYPE_BIGINT);
    int64_t a = 0;
    set->insert(&a);
    a = 1;
    set->insert(&a);
    a = 2;
    set->insert(&a);
    a = 3;
    set->insert(&a);
    a = 4;
    set->insert(&a);
    a = 4;
    set->insert(&a);

    EXPECT_EQ(5, set->size());
    HybridSetBase::IteratorBase* base = set->begin();

    while (base->has_next()) {
        LOG(INFO) << (*(int64_t*)base->get_value());
        base->next();
    }

    a = 0;
    EXPECT_TRUE(set->find(&a));
    a = 1;
    EXPECT_TRUE(set->find(&a));
    a = 2;
    EXPECT_TRUE(set->find(&a));
    a = 3;
    EXPECT_TRUE(set->find(&a));
    a = 4;
    EXPECT_TRUE(set->find(&a));
    a = 5;
    EXPECT_FALSE(set->find(&a));
}
TEST_F(HybridSetTest, float) {
    HybridSetBase* set = create_set(TYPE_FLOAT);
    float a = 0;
    set->insert(&a);
    a = 1.1;
    set->insert(&a);
    a = 2.1;
    set->insert(&a);
    a = 3.1;
    set->insert(&a);
    a = 4.1;
    set->insert(&a);
    a = 4.1;
    set->insert(&a);

    EXPECT_EQ(5, set->size());
    HybridSetBase::IteratorBase* base = set->begin();

    while (base->has_next()) {
        LOG(INFO) << (*(float*)base->get_value());
        base->next();
    }

    a = 0;
    EXPECT_TRUE(set->find(&a));
    a = 1.1;
    EXPECT_TRUE(set->find(&a));
    a = 2.1;
    EXPECT_TRUE(set->find(&a));
    a = 3.1;
    EXPECT_TRUE(set->find(&a));
    a = 4.1;
    EXPECT_TRUE(set->find(&a));
    a = 5.1;
    EXPECT_FALSE(set->find(&a));
}
TEST_F(HybridSetTest, double) {
    HybridSetBase* set = create_set(TYPE_DOUBLE);
    double a = 0;
    set->insert(&a);
    a = 1.1;
    set->insert(&a);
    a = 2.1;
    set->insert(&a);
    a = 3.1;
    set->insert(&a);
    a = 4.1;
    set->insert(&a);
    a = 4.1;
    set->insert(&a);

    EXPECT_EQ(5, set->size());
    HybridSetBase::IteratorBase* base = set->begin();

    while (base->has_next()) {
        LOG(INFO) << (*(double*)base->get_value());
        base->next();
    }

    a = 0;
    EXPECT_TRUE(set->find(&a));
    a = 1.1;
    EXPECT_TRUE(set->find(&a));
    a = 2.1;
    EXPECT_TRUE(set->find(&a));
    a = 3.1;
    EXPECT_TRUE(set->find(&a));
    a = 4.1;
    EXPECT_TRUE(set->find(&a));
    a = 5.1;
    EXPECT_FALSE(set->find(&a));
}
TEST_F(HybridSetTest, string) {
    HybridSetBase* set = create_set(TYPE_VARCHAR);
    StringRef a;

    char buf[100];

    snprintf(buf, 100, "abcdefghigk");
    a.data = buf;

    a.size = 0;
    set->insert(&a);
    a.size = 1;
    set->insert(&a);
    a.size = 2;
    set->insert(&a);
    a.size = 3;
    set->insert(&a);
    a.size = 4;
    set->insert(&a);
    a.size = 4;
    set->insert(&a);

    EXPECT_EQ(5, set->size());
    HybridSetBase::IteratorBase* base = set->begin();

    while (base->has_next()) {
        LOG(INFO) << ((StringRef*)base->get_value())->data;
        base->next();
    }

    StringRef b;

    char buf1[100];

    snprintf(buf1, 100, "abcdefghigk");
    b.data = buf1;

    b.size = 0;
    EXPECT_TRUE(set->find(&b));
    b.size = 1;
    EXPECT_TRUE(set->find(&b));
    b.size = 2;
    EXPECT_TRUE(set->find(&b));
    b.size = 3;
    EXPECT_TRUE(set->find(&b));
    b.size = 4;
    EXPECT_TRUE(set->find(&b));
    b.size = 5;
    EXPECT_FALSE(set->find(&b));
}

} // namespace doris
