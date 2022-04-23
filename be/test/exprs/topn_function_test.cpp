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

#include "exprs/topn_function.h"

#include <gtest/gtest.h>

#include <unordered_map>

#include "exprs/anyval_util.h"
#include "exprs/expr_context.h"
#include "testutil/function_utils.h"
#include "testutil/test_util.h"
#include "util/topn_counter.h"
#include "zipf_distribution.h"

namespace doris {

static const uint32_t TOPN_NUM = 100;
static const uint32_t TOTAL_RECORDS = LOOP_LESS_OR_MORE(1000, 1000000);
static const uint32_t PARALLEL = 10;

std::string gen_random(const int len) {
    std::string possible_characters =
            "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    std::random_device rd;
    std::mt19937 generator(rd());
    std::uniform_int_distribution<> dist(0, possible_characters.size() - 1);

    std::string rand_str(len, '\0');
    for (auto& dis : rand_str) {
        dis = possible_characters[dist(generator)];
    }
    return rand_str;
}

class TopNFunctionsTest : public testing::Test {
public:
    TopNFunctionsTest() = default;

    void SetUp() {
        utils = new FunctionUtils();
        ctx = utils->get_fn_ctx();
    }

    void TearDown() { delete utils; }

private:
    FunctionUtils* utils;
    FunctionContext* ctx;
};

void update_accuracy_map(const std::string& item,
                         std::unordered_map<std::string, uint32_t>& accuracy_map) {
    if (accuracy_map.find(item) != accuracy_map.end()) {
        ++accuracy_map[item];
    } else {
        accuracy_map.insert(std::make_pair(item, 1));
    }
}

void topn_single(FunctionContext* ctx, std::string& random_str, StringVal& dst,
                 std::unordered_map<std::string, uint32_t>& accuracy_map) {
    TopNFunctions::topn_update(ctx, StringVal(((uint8_t*)random_str.data()), random_str.length()),
                               TOPN_NUM, &dst);
    update_accuracy_map(random_str, accuracy_map);
}

void test_topn_accuracy(FunctionContext* ctx, int key_space, int space_expand_rate,
                        double zipf_distribution_exponent) {
    LOG(INFO) << "topn accuracy : "
              << "key space : " << key_space << " , space_expand_rate : " << space_expand_rate
              << " , zf exponent : " << zipf_distribution_exponent;
    std::unordered_map<std::string, uint32_t> accuracy_map;
    // prepare random data
    std::vector<std::string> random_strs(key_space);
    for (uint32_t i = 0; i < key_space; ++i) {
        random_strs[i] = gen_random(10);
    }

    zipf_distribution<uint64_t, double> zf(key_space - 1, zipf_distribution_exponent);
    std::random_device rd;
    std::mt19937 gen(rd());

    StringVal topn_column("placeholder");
    IntVal topn_num_column(TOPN_NUM);
    IntVal space_expand_rate_column(space_expand_rate);
    std::vector<doris_udf::AnyVal*> const_vals;
    const_vals.push_back(&topn_column);
    const_vals.push_back(&topn_num_column);
    const_vals.push_back(&space_expand_rate_column);
    ctx->impl()->set_constant_args(const_vals);
    // Compute topN in parallel
    StringVal dst;
    TopNFunctions::topn_init(ctx, &dst);

    StringVal single_dst_str[PARALLEL];
    for (uint32_t i = 0; i < PARALLEL; ++i) {
        TopNFunctions::topn_init(ctx, &single_dst_str[i]);
    }

    std::random_device random_rd;
    std::mt19937 random_gen(random_rd());
    std::uniform_int_distribution<> dist(0, PARALLEL - 1);
    for (uint32_t i = 0; i < TOTAL_RECORDS; ++i) {
        // generate zipf_distribution
        uint32_t index = zf(gen);
        // choose one single topn to update
        topn_single(ctx, random_strs[index], single_dst_str[dist(random_gen)], accuracy_map);
    }

    for (uint32_t i = 0; i < PARALLEL; ++i) {
        StringVal serialized_str = TopNFunctions::topn_serialize(ctx, single_dst_str[i]);
        TopNFunctions::topn_merge(ctx, serialized_str, &dst);
    }

    // get accuracy result
    std::vector<Counter> accuracy_sort_vec;
    for (std::unordered_map<std::string, uint32_t>::const_iterator it = accuracy_map.begin();
         it != accuracy_map.end(); ++it) {
        accuracy_sort_vec.emplace_back(it->first, it->second);
    }
    std::sort(accuracy_sort_vec.begin(), accuracy_sort_vec.end(), TopNComparator());

    // get topn result
    TopNCounter* topn_dst = reinterpret_cast<TopNCounter*>(dst.ptr);
    std::vector<Counter> topn_sort_vec;
    topn_dst->sort_retain(TOPN_NUM, &topn_sort_vec);

    uint32_t error = 0;
    int min_size = std::min(accuracy_sort_vec.size(), topn_sort_vec.size());
    for (uint32_t i = 0; i < min_size; ++i) {
        Counter& accuracy_counter = accuracy_sort_vec[i];
        Counter& topn_counter = topn_sort_vec[i];
        if (accuracy_counter.get_count() != topn_counter.get_count()) {
            ++error;
            LOG(INFO) << "Failed";
            LOG(INFO) << "accuracy counter : (" << accuracy_counter.get_item() << ", "
                      << accuracy_counter.get_count() << ")";
            LOG(INFO) << "topn counter : (" << topn_counter.get_item() << ", "
                      << topn_counter.get_count() << ")";
        }
    }
    error += std::abs((int32_t)(accuracy_sort_vec.size() - topn_sort_vec.size()));
    LOG(INFO) << "Total errors : " << error;
    TopNFunctions::topn_finalize(ctx, dst);
}

TEST_F(TopNFunctionsTest, topn_accuracy) {
    std::vector<int> small_key_space({100});
    std::vector<int> large_key_space({1000, 10000, 100000, 500000});
    std::vector<int> key_space_vec(LOOP_LESS_OR_MORE(small_key_space, large_key_space));
    std::vector<int> space_expand_rate_vec({20, 50, 100});
    std::vector<double> zipf_distribution_exponent_vec({0.5, 0.6, 1.0});
    for (auto ket_space : key_space_vec) {
        for (auto space_expand_rate : space_expand_rate_vec) {
            for (auto zipf_distribution_exponent : zipf_distribution_exponent_vec) {
                test_topn_accuracy(ctx, ket_space, space_expand_rate, zipf_distribution_exponent);
            }
        }
    }
}

TEST_F(TopNFunctionsTest, topn_update) {
    StringVal dst;
    TopNFunctions::topn_init(ctx, &dst);
    StringVal src1("a");
    for (uint32_t i = 0; i < 10; ++i) {
        TopNFunctions::topn_update(ctx, src1, 2, &dst);
    }

    StringVal src2("b");
    TopNFunctions::topn_update(ctx, src2, 2, &dst);
    TopNFunctions::topn_update(ctx, src2, 2, &dst);

    StringVal src3("c");
    TopNFunctions::topn_update(ctx, src3, 2, &dst);

    StringVal result = TopNFunctions::topn_finalize(ctx, dst);
    StringVal expected("{\"a\":10,\"b\":2}");
    EXPECT_EQ(expected, result);
}

TEST_F(TopNFunctionsTest, topn_merge) {
    StringVal dst1;
    TopNFunctions::topn_init(ctx, &dst1);
    StringVal dst2;
    TopNFunctions::topn_init(ctx, &dst2);

    StringVal src1("a");
    for (uint32_t i = 0; i < 10; ++i) {
        TopNFunctions::topn_update(ctx, src1, 2, &dst1);
        TopNFunctions::topn_update(ctx, src1, 2, &dst2);
    }
    StringVal src2("b");
    for (uint32_t i = 0; i < 8; ++i) {
        TopNFunctions::topn_update(ctx, src2, 2, &dst1);
    }
    StringVal src3("c");
    for (uint32_t i = 0; i < 6; ++i) {
        TopNFunctions::topn_update(ctx, src3, 2, &dst2);
    }

    StringVal val1 = TopNFunctions::topn_serialize(ctx, dst1);
    StringVal val2 = TopNFunctions::topn_serialize(ctx, dst2);

    StringVal dst;
    TopNFunctions::topn_init(ctx, &dst);
    TopNFunctions::topn_merge(ctx, val1, &dst);
    TopNFunctions::topn_merge(ctx, val2, &dst);
    StringVal result = TopNFunctions::topn_finalize(ctx, dst);
    StringVal expected("{\"a\":20,\"b\":8}");
    EXPECT_EQ(expected, result);
}

} // namespace doris
