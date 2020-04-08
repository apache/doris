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

#include "exprs/hybird_set.h"

#include <string>
#include <gtest/gtest.h>
#include "common/configbase.h"
#include "util/logging.h"

namespace doris {

// mock
class HybirdSetTest : public testing::Test {
public:
    HybirdSetTest() {
    }

protected:
};

TEST_F(HybirdSetTest, bool) {
    HybirdSetBase* set = HybirdSetBase::create_set(TYPE_BOOLEAN);
    bool a = true;
    set->insert(&a);
    a = false;
    set->insert(&a);
    a = true;
    set->insert(&a);
    a = false;
    set->insert(&a);

    ASSERT_EQ(2, set->size());
    HybirdSetBase::IteratorBase* base = set->begin();

    while (base->has_next()) {
        LOG(INFO) << (*(bool*)base->get_value());
        base->next();
    }

    a = true;
    ASSERT_TRUE(set->find(&a));
    a = false;
    ASSERT_TRUE(set->find(&a));
}

TEST_F(HybirdSetTest, tinyint) {
    HybirdSetBase* set = HybirdSetBase::create_set(TYPE_TINYINT);
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

    ASSERT_EQ(5, set->size());

    HybirdSetBase::IteratorBase* base = set->begin();

    while (base->has_next()) {
        LOG(INFO) << (*(int8_t*)base->get_value());
        base->next();
    }

    a = 0;
    ASSERT_TRUE(set->find(&a));
    a = 1;
    ASSERT_TRUE(set->find(&a));
    a = 2;
    ASSERT_TRUE(set->find(&a));
    a = 3;
    ASSERT_TRUE(set->find(&a));
    a = 4;
    ASSERT_TRUE(set->find(&a));
    a = 5;
    ASSERT_FALSE(set->find(&a));
}
TEST_F(HybirdSetTest, smallint) {
    HybirdSetBase* set = HybirdSetBase::create_set(TYPE_SMALLINT);
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

    ASSERT_EQ(5, set->size());
    HybirdSetBase::IteratorBase* base = set->begin();

    while (base->has_next()) {
        LOG(INFO) << (*(int16_t*)base->get_value());
        base->next();
    }

    a = 0;
    ASSERT_TRUE(set->find(&a));
    a = 1;
    ASSERT_TRUE(set->find(&a));
    a = 2;
    ASSERT_TRUE(set->find(&a));
    a = 3;
    ASSERT_TRUE(set->find(&a));
    a = 4;
    ASSERT_TRUE(set->find(&a));
    a = 5;
    ASSERT_FALSE(set->find(&a));
}
TEST_F(HybirdSetTest, int) {
    HybirdSetBase* set = HybirdSetBase::create_set(TYPE_INT);
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

    ASSERT_EQ(5, set->size());
    HybirdSetBase::IteratorBase* base = set->begin();

    while (base->has_next()) {
        LOG(INFO) << (*(int32_t*)base->get_value());
        base->next();
    }

    a = 0;
    ASSERT_TRUE(set->find(&a));
    a = 1;
    ASSERT_TRUE(set->find(&a));
    a = 2;
    ASSERT_TRUE(set->find(&a));
    a = 3;
    ASSERT_TRUE(set->find(&a));
    a = 4;
    ASSERT_TRUE(set->find(&a));
    a = 5;
    ASSERT_FALSE(set->find(&a));
}
TEST_F(HybirdSetTest, bigint) {
    HybirdSetBase* set = HybirdSetBase::create_set(TYPE_BIGINT);
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

    ASSERT_EQ(5, set->size());
    HybirdSetBase::IteratorBase* base = set->begin();

    while (base->has_next()) {
        LOG(INFO) << (*(int64_t*)base->get_value());
        base->next();
    }

    a = 0;
    ASSERT_TRUE(set->find(&a));
    a = 1;
    ASSERT_TRUE(set->find(&a));
    a = 2;
    ASSERT_TRUE(set->find(&a));
    a = 3;
    ASSERT_TRUE(set->find(&a));
    a = 4;
    ASSERT_TRUE(set->find(&a));
    a = 5;
    ASSERT_FALSE(set->find(&a));
}
TEST_F(HybirdSetTest, float) {
    HybirdSetBase* set = HybirdSetBase::create_set(TYPE_FLOAT);
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

    ASSERT_EQ(5, set->size());
    HybirdSetBase::IteratorBase* base = set->begin();

    while (base->has_next()) {
        LOG(INFO) << (*(float*)base->get_value());
        base->next();
    }

    a = 0;
    ASSERT_TRUE(set->find(&a));
    a = 1.1;
    ASSERT_TRUE(set->find(&a));
    a = 2.1;
    ASSERT_TRUE(set->find(&a));
    a = 3.1;
    ASSERT_TRUE(set->find(&a));
    a = 4.1;
    ASSERT_TRUE(set->find(&a));
    a = 5.1;
    ASSERT_FALSE(set->find(&a));
}
TEST_F(HybirdSetTest, double) {
    HybirdSetBase* set = HybirdSetBase::create_set(TYPE_DOUBLE);
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

    ASSERT_EQ(5, set->size());
    HybirdSetBase::IteratorBase* base = set->begin();

    while (base->has_next()) {
        LOG(INFO) << (*(double*)base->get_value());
        base->next();
    }

    a = 0;
    ASSERT_TRUE(set->find(&a));
    a = 1.1;
    ASSERT_TRUE(set->find(&a));
    a = 2.1;
    ASSERT_TRUE(set->find(&a));
    a = 3.1;
    ASSERT_TRUE(set->find(&a));
    a = 4.1;
    ASSERT_TRUE(set->find(&a));
    a = 5.1;
    ASSERT_FALSE(set->find(&a));
}
TEST_F(HybirdSetTest, string) {
    HybirdSetBase* set = HybirdSetBase::create_set(TYPE_VARCHAR);
    StringValue a;

    char buf[100];

    snprintf(buf, 100, "abcdefghigk");
    a.ptr = buf;

    a.len = 0;
    set->insert(&a);
    a.len = 1;
    set->insert(&a);
    a.len = 2;
    set->insert(&a);
    a.len = 3;
    set->insert(&a);
    a.len = 4;
    set->insert(&a);
    a.len = 4;
    set->insert(&a);

    ASSERT_EQ(5, set->size());
    HybirdSetBase::IteratorBase* base = set->begin();

    while (base->has_next()) {
        LOG(INFO) << ((StringValue*)base->get_value())->ptr;
        base->next();
    }

    StringValue b;

    char buf1[100];

    snprintf(buf1, 100, "abcdefghigk");
    b.ptr = buf1;

    b.len = 0;
    ASSERT_TRUE(set->find(&b));
    b.len = 1;
    ASSERT_TRUE(set->find(&b));
    b.len = 2;
    ASSERT_TRUE(set->find(&b));
    b.len = 3;
    ASSERT_TRUE(set->find(&b));
    b.len = 4;
    ASSERT_TRUE(set->find(&b));
    b.len = 5;
    ASSERT_FALSE(set->find(&b));
}
TEST_F(HybirdSetTest, timestamp) {
    HybirdSetBase* set = HybirdSetBase::create_set(TYPE_DATETIME);
    char s1[] = "2012-01-20 01:10:01";
    char s2[] = "1990-10-20 10:10:10.123456  ";
    char s3[] = "  1990-10-20 10:10:10.123456";
    DateTimeValue v1;
    v1.from_date_str(s1, strlen(s1));
        LOG(INFO) << v1.debug_string();
    DateTimeValue v2;
    v2.from_date_str(s2, strlen(s2));
        LOG(INFO) << v2.debug_string();
    DateTimeValue v3;
    v3.from_date_str(s3, strlen(s3));
        LOG(INFO) << v3.debug_string();

    set->insert(&v1);
    set->insert(&v2);
    set->insert(&v3);

    HybirdSetBase::IteratorBase* base = set->begin();

    while (base->has_next()) {
        LOG(INFO) << ((DateTimeValue*)base->get_value())->debug_string();
        base->next();
    }
    ASSERT_EQ(2, set->size());

    char s11[] = "2012-01-20 01:10:01";
    char s12[] = "1990-10-20 10:10:10.123456  ";
    char s13[] = "1990-10-20 10:10:10.123456";
    DateTimeValue v11;
    v11.from_date_str(s11, strlen(s11));
    DateTimeValue v12;
    v12.from_date_str(s12, strlen(s12));
    DateTimeValue v13;
    v13.from_date_str(s13, strlen(s13));

    ASSERT_TRUE(set->find(&v11));
    ASSERT_TRUE(set->find(&v12));
    ASSERT_TRUE(set->find(&v13));

    char s23[] = "1992-10-20 10:10:10.123456";
    DateTimeValue v23;
    v23.from_date_str(s23, strlen(s23));
    ASSERT_FALSE(set->find(&v23));
}

}

int main(int argc, char** argv) {
    std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    doris::init_glog("be-test");
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

