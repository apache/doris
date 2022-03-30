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

#include <string>

#include "exprs/bloomfilter_predicate.h"
#include "exprs/create_predicate_function.h"
#include "gtest/gtest.h"
#include "runtime/string_value.h"

namespace doris {
class BloomFilterPredicateTest : public testing::Test {
public:
    BloomFilterPredicateTest() = default;
    virtual void SetUp() {}
    virtual void TearDown() {}
};

TEST_F(BloomFilterPredicateTest, bloom_filter_func_int_test) {
    std::unique_ptr<IBloomFilterFuncBase> func(create_bloom_filter(PrimitiveType::TYPE_INT));
    ASSERT_TRUE(func->init(1024, 0.05).ok());
    const int data_size = 1024;
    int data[data_size];
    for (int i = 0; i < data_size; i++) {
        data[i] = i;
        func->insert((const void*)&data[i]);
    }
    for (int i = 0; i < data_size; i++) {
        ASSERT_TRUE(func->find((const void*)&data[i]));
    }
    // test not exist val
    int not_exist_val = 0x3355ff;
    ASSERT_FALSE(func->find((const void*)&not_exist_val));
    // TEST null value
    func->insert(nullptr);
    func->find(nullptr);
}

TEST_F(BloomFilterPredicateTest, bloom_filter_func_stringval_test) {
    std::unique_ptr<IBloomFilterFuncBase> func(create_bloom_filter(PrimitiveType::TYPE_VARCHAR));
    ASSERT_TRUE(func->init(1024, 0.05).ok());
    ObjectPool obj_pool;
    const int data_size = 1024;
    StringValue data[data_size];
    for (int i = 0; i < data_size; i++) {
        auto str = obj_pool.add(new std::string(std::to_string(i)));
        data[i] = StringValue(*str);
        func->insert((const void*)&data[i]);
    }
    for (int i = 0; i < data_size; i++) {
        ASSERT_TRUE(func->find((const void*)&data[i]));
    }
    // test not exist value
    std::string not_exist_str = "0x3355ff";
    StringValue not_exist_val(not_exist_str);
    ASSERT_FALSE(func->find((const void*)&not_exist_val));

    // test fixed char
    func.reset(create_bloom_filter(PrimitiveType::TYPE_CHAR));
    ASSERT_TRUE(func->init(1024, 0.05).ok());

    auto varchar_true_str = obj_pool.add(new std::string("true"));
    StringValue varchar_true(*varchar_true_str);
    func->insert((const void*)&varchar_true);

    auto varchar_false_str = obj_pool.add(new std::string("false"));
    StringValue varchar_false(*varchar_false_str);
    func->insert((const void*)&varchar_false);

    StringValue fixed_char_true;
    char true_buf[100] = "true";
    memset(true_buf + strlen(true_buf), 0, 100 - strlen(true_buf));
    fixed_char_true.ptr = true_buf;
    fixed_char_true.len = 10;

    StringValue fixed_char_false;
    char false_buf[100] = "false";
    memset(false_buf + strlen(false_buf), 0, 100 - strlen(false_buf));
    fixed_char_false.ptr = false_buf;
    fixed_char_false.len = 10;

    ASSERT_TRUE(func->find_olap_engine((const void*)&fixed_char_true));
    ASSERT_TRUE(func->find_olap_engine((const void*)&fixed_char_false));

    func->find(nullptr);
}

TEST_F(BloomFilterPredicateTest, bloom_filter_size_test) {
    std::unique_ptr<IBloomFilterFuncBase> func(create_bloom_filter(PrimitiveType::TYPE_VARCHAR));
    int length = 4096;
    func->init_with_fixed_length(4096);
    char* data = nullptr;
    int len;
    func->get_data(&data, &len);
    ASSERT_EQ(length, len);
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
