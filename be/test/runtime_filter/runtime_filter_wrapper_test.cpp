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

#include "runtime_filter/runtime_filter_wrapper.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

namespace doris {

class RuntimeFilterWrapperTest : public testing::Test {
public:
    RuntimeFilterWrapperTest() = default;
    ~RuntimeFilterWrapperTest() override = default;
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(RuntimeFilterWrapperTest, basic) {
    int32_t filter_id = 0;
    RuntimeFilterType filter_type = RuntimeFilterType::IN_FILTER;
    PrimitiveType column_return_type = PrimitiveType::TYPE_INT;
    int32_t max_in_num = 0;
    int64_t runtime_bloom_filter_min_size = 0;
    int64_t runtime_bloom_filter_max_size = 0;
    bool build_bf_by_runtime_size = true;
    int64_t bloom_filter_size = 0;
    bool bloom_filter_size_calculated_by_ndv = true;
    bool null_aware = true;
    bool enable_fixed_len_to_uint32_v2 = true;
    bool bitmap_filter_not_in = false;

    RuntimeFilterParams params;
    params.filter_id = filter_id;
    params.filter_type = filter_type;
    params.column_return_type = column_return_type;
    params.max_in_num = max_in_num;
    params.runtime_bloom_filter_min_size = runtime_bloom_filter_min_size;
    params.runtime_bloom_filter_max_size = runtime_bloom_filter_max_size;
    params.build_bf_by_runtime_size = build_bf_by_runtime_size;
    params.bloom_filter_size_calculated_by_ndv = bloom_filter_size_calculated_by_ndv;
    params.bloom_filter_size = bloom_filter_size;
    params.null_aware = null_aware;
    params.enable_fixed_len_to_uint32_v2 = enable_fixed_len_to_uint32_v2;
    params.bitmap_filter_not_in = bitmap_filter_not_in;

    auto wrapper = std::make_shared<RuntimeFilterWrapper>(&params);
}

} // namespace doris
