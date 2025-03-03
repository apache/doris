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

#include "exprs/hybrid_set.h"
#include "testutil/column_helper.h"
#include "vec/data_types/data_type_number.h"

namespace doris {

class RuntimeFilterWrapperTest : public testing::Test {
public:
    RuntimeFilterWrapperTest() = default;
    ~RuntimeFilterWrapperTest() override = default;
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(RuntimeFilterWrapperTest, TestIn) {
    using DataType = vectorized::DataTypeInt32;
    int32_t filter_id = 0;
    RuntimeFilterType filter_type = RuntimeFilterType::IN_FILTER;
    bool null_aware = true;
    PrimitiveType column_return_type = PrimitiveType::TYPE_INT;

    int32_t max_in_num = 2;

    int64_t runtime_bloom_filter_min_size = 0;
    int64_t runtime_bloom_filter_max_size = 0;
    bool build_bf_by_runtime_size = true;
    int64_t bloom_filter_size = 0;
    bool bloom_filter_size_calculated_by_ndv = true;
    bool enable_fixed_len_to_uint32_v2 = true;

    bool bitmap_filter_not_in = false;

    PMergeFilterRequest valid_request;
    RuntimeFilterParams params {
            .filter_id = filter_id,
            .filter_type = filter_type,
            .column_return_type = column_return_type,
            .null_aware = null_aware,
            .max_in_num = max_in_num,
            .runtime_bloom_filter_min_size = runtime_bloom_filter_min_size,
            .runtime_bloom_filter_max_size = runtime_bloom_filter_max_size,
            .bloom_filter_size = bloom_filter_size,
            .build_bf_by_runtime_size = build_bf_by_runtime_size,
            .bloom_filter_size_calculated_by_ndv = bloom_filter_size_calculated_by_ndv,
            .enable_fixed_len_to_uint32_v2 = enable_fixed_len_to_uint32_v2,
            .bitmap_filter_not_in = bitmap_filter_not_in};

    auto wrapper = std::make_shared<RuntimeFilterWrapper>(&params);
    EXPECT_EQ(wrapper->minmax_func(), nullptr);
    EXPECT_EQ(wrapper->bloom_filter_func(), nullptr);
    EXPECT_EQ(wrapper->bitmap_filter_func(), nullptr);
    EXPECT_NE(wrapper->hybrid_set(), nullptr);
    {
        // Init
        EXPECT_TRUE(wrapper->init(2).ok());
        EXPECT_EQ(wrapper->get_state(), RuntimeFilterWrapper::State::UNINITED);

        EXPECT_TRUE(wrapper->init(3).ok());
        EXPECT_EQ(wrapper->get_state(), RuntimeFilterWrapper::State::DISABLED);

        wrapper->_state = RuntimeFilterWrapper::State::UNINITED;
        wrapper->_disabled_reason = "";
    }
    {
        // Insert
        auto col =
                vectorized::ColumnHelper::create_column<DataType>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        EXPECT_EQ(wrapper->insert(col, 0).code(), ErrorCode::INTERNAL_ERROR);
        wrapper->_state = RuntimeFilterWrapper::State::UNINITED;
        wrapper->_disabled_reason = "";

        col = vectorized::ColumnHelper::create_column<DataType>({0});
        EXPECT_TRUE(wrapper->insert(col, 0).ok());
        EXPECT_EQ(wrapper->hybrid_set()->size(), col->size());
        EXPECT_EQ(wrapper->get_state(), RuntimeFilterWrapper::State::UNINITED);
    }
    {
        // Merge 1 (valid filter)
        auto another_wrapper = std::make_shared<RuntimeFilterWrapper>(&params);
        EXPECT_TRUE(another_wrapper->init(2).ok());
        EXPECT_EQ(another_wrapper->get_state(), RuntimeFilterWrapper::State::UNINITED);

        auto col = vectorized::ColumnHelper::create_column<DataType>({1});
        EXPECT_TRUE(another_wrapper->insert(col, 0).ok());
        another_wrapper->_state = RuntimeFilterWrapper::State::READY;
        EXPECT_TRUE(wrapper->merge(another_wrapper.get()).ok());
        EXPECT_EQ(wrapper->hybrid_set()->size(), 2);
        EXPECT_EQ(wrapper->get_state(), RuntimeFilterWrapper::State::READY);
        wrapper->to_protobuf(valid_request.mutable_in_filter());

        // Merge 2 (ignored filter)
        another_wrapper = std::make_shared<RuntimeFilterWrapper>(&params);
        EXPECT_TRUE(another_wrapper->init(2).ok());
        EXPECT_EQ(another_wrapper->get_state(), RuntimeFilterWrapper::State::UNINITED);
        another_wrapper->_state = RuntimeFilterWrapper::State::IGNORED;
        EXPECT_TRUE(wrapper->merge(another_wrapper.get()).ok());
        EXPECT_EQ(wrapper->hybrid_set()->size(), 2);
        EXPECT_EQ(wrapper->get_state(), RuntimeFilterWrapper::State::READY);

        // Merge 3 (disabled filter)
        another_wrapper = std::make_shared<RuntimeFilterWrapper>(&params);
        EXPECT_TRUE(another_wrapper->init(2).ok());
        EXPECT_EQ(another_wrapper->get_state(), RuntimeFilterWrapper::State::UNINITED);
        another_wrapper->_state = RuntimeFilterWrapper::State::DISABLED;
        EXPECT_TRUE(wrapper->merge(another_wrapper.get()).ok());
        EXPECT_EQ(wrapper->hybrid_set()->size(), 0);
        EXPECT_EQ(wrapper->get_state(), RuntimeFilterWrapper::State::DISABLED);

        // Merge 4 (valid filter)
        another_wrapper = std::make_shared<RuntimeFilterWrapper>(&params);
        EXPECT_TRUE(another_wrapper->init(2).ok());
        EXPECT_EQ(another_wrapper->get_state(), RuntimeFilterWrapper::State::UNINITED);
        col = vectorized::ColumnHelper::create_column<DataType>({1});
        EXPECT_TRUE(another_wrapper->insert(col, 0).ok());
        another_wrapper->_state = RuntimeFilterWrapper::State::READY;
        EXPECT_TRUE(wrapper->merge(another_wrapper.get()).ok());
        EXPECT_EQ(wrapper->hybrid_set()->size(), 0);
        EXPECT_EQ(wrapper->get_state(), RuntimeFilterWrapper::State::DISABLED)
                << RuntimeFilterWrapper::to_string(wrapper->get_state());
        wrapper->_state = RuntimeFilterWrapper::State::UNINITED;
        wrapper->_disabled_reason = "";

        // Merge 5 (valid filter)
        another_wrapper = std::make_shared<RuntimeFilterWrapper>(&params);
        EXPECT_TRUE(another_wrapper->init(2).ok());
        EXPECT_EQ(another_wrapper->get_state(), RuntimeFilterWrapper::State::UNINITED);
        col = vectorized::ColumnHelper::create_column<DataType>({0, 1});
        EXPECT_TRUE(another_wrapper->insert(col, 0).ok());
        another_wrapper->_state = RuntimeFilterWrapper::State::READY;
        EXPECT_TRUE(wrapper->merge(another_wrapper.get()).ok());
        EXPECT_EQ(wrapper->hybrid_set()->size(), 2);
        EXPECT_EQ(wrapper->get_state(), RuntimeFilterWrapper::State::READY)
                << RuntimeFilterWrapper::to_string(wrapper->get_state());

        // Merge 6 (Exceed the max in set limitation)
        another_wrapper = std::make_shared<RuntimeFilterWrapper>(&params);
        EXPECT_TRUE(another_wrapper->init(2).ok());
        EXPECT_EQ(another_wrapper->get_state(), RuntimeFilterWrapper::State::UNINITED);
        col = vectorized::ColumnHelper::create_column<DataType>({3, 4});
        EXPECT_TRUE(another_wrapper->insert(col, 0).ok());
        another_wrapper->_state = RuntimeFilterWrapper::State::READY;
        EXPECT_TRUE(wrapper->merge(another_wrapper.get()).ok());
        EXPECT_EQ(wrapper->hybrid_set()->size(), 0);
        EXPECT_EQ(wrapper->get_state(), RuntimeFilterWrapper::State::DISABLED);
        wrapper->_state = RuntimeFilterWrapper::State::UNINITED;
        wrapper->_disabled_reason = "";
    }
    {
        // Assign disabled filter
        PMergeFilterRequest request;
        request.set_disabled(true);
        EXPECT_TRUE(wrapper->assign(request, nullptr).ok());
        EXPECT_EQ(wrapper->get_state(), RuntimeFilterWrapper::State::DISABLED);
        wrapper->_state = RuntimeFilterWrapper::State::UNINITED;
        wrapper->_disabled_reason = "";

        // Assign ignored filter
        PMergeFilterRequest request2;
        request2.set_ignored(true);
        EXPECT_TRUE(wrapper->assign(request2, nullptr).ok());
        EXPECT_EQ(wrapper->get_state(), RuntimeFilterWrapper::State::IGNORED);
        wrapper->_state = RuntimeFilterWrapper::State::UNINITED;
        wrapper->_disabled_reason = "";

        // Assign valid filter
        valid_request.set_contain_null(false);
        valid_request.set_filter_type(PFilterType::IN_FILTER);
        EXPECT_TRUE(wrapper->assign(valid_request, nullptr).ok());
        EXPECT_EQ(wrapper->get_state(), RuntimeFilterWrapper::State::READY);
        EXPECT_EQ(wrapper->hybrid_set()->size(), 2);
    }
}

TEST_F(RuntimeFilterWrapperTest, TestInAssign) {
    int32_t filter_id = 0;
    RuntimeFilterType filter_type = RuntimeFilterType::IN_FILTER;
    bool null_aware = true;

    int32_t max_in_num = 2;

    int64_t runtime_bloom_filter_min_size = 0;
    int64_t runtime_bloom_filter_max_size = 0;
    bool build_bf_by_runtime_size = true;
    int64_t bloom_filter_size = 0;
    bool bloom_filter_size_calculated_by_ndv = true;
    bool enable_fixed_len_to_uint32_v2 = true;

    bool bitmap_filter_not_in = false;

#define APPLY_FOR_PRIMITIVE_TYPE(TYPE, value1, value2)                                             \
    {                                                                                              \
        static constexpr PrimitiveType column_return_type = PrimitiveType::TYPE;                   \
        RuntimeFilterParams params {                                                               \
                .filter_id = filter_id,                                                            \
                .filter_type = filter_type,                                                        \
                .column_return_type = column_return_type,                                          \
                .null_aware = null_aware,                                                          \
                .max_in_num = max_in_num,                                                          \
                .runtime_bloom_filter_min_size = runtime_bloom_filter_min_size,                    \
                .runtime_bloom_filter_max_size = runtime_bloom_filter_max_size,                    \
                .bloom_filter_size = bloom_filter_size,                                            \
                .build_bf_by_runtime_size = build_bf_by_runtime_size,                              \
                .bloom_filter_size_calculated_by_ndv = bloom_filter_size_calculated_by_ndv,        \
                .enable_fixed_len_to_uint32_v2 = enable_fixed_len_to_uint32_v2,                    \
                .bitmap_filter_not_in = bitmap_filter_not_in};                                     \
        auto wrapper = std::make_shared<RuntimeFilterWrapper>(&params);                            \
        PMergeFilterRequest valid_request;                                                         \
        auto* in_filter = valid_request.mutable_in_filter();                                       \
        in_filter->set_column_type(to_proto(column_return_type));                                  \
        get_convertor<PrimitiveTypeTraits<column_return_type>::CppType>()(in_filter->add_values(), \
                                                                          value1);                 \
        get_convertor<PrimitiveTypeTraits<column_return_type>::CppType>()(in_filter->add_values(), \
                                                                          value2);                 \
        valid_request.set_contain_null(false);                                                     \
        valid_request.set_filter_type(PFilterType::IN_FILTER);                                     \
        EXPECT_TRUE(wrapper->assign(valid_request, nullptr).ok());                                 \
        EXPECT_EQ(wrapper->get_state(), RuntimeFilterWrapper::State::READY);                       \
        EXPECT_EQ(wrapper->hybrid_set()->size(), 2);                                               \
    }

#define APPLY_FOR_PRIMITIVE_BASE_TYPE(TYPE) APPLY_FOR_PRIMITIVE_TYPE(TYPE, 0, 1)

    APPLY_FOR_PRIMITIVE_BASE_TYPE(TYPE_BOOLEAN);
    APPLY_FOR_PRIMITIVE_BASE_TYPE(TYPE_TINYINT);
    APPLY_FOR_PRIMITIVE_BASE_TYPE(TYPE_SMALLINT);
    APPLY_FOR_PRIMITIVE_BASE_TYPE(TYPE_INT);
    APPLY_FOR_PRIMITIVE_BASE_TYPE(TYPE_BIGINT);
    APPLY_FOR_PRIMITIVE_BASE_TYPE(TYPE_LARGEINT);
    APPLY_FOR_PRIMITIVE_BASE_TYPE(TYPE_FLOAT);
    APPLY_FOR_PRIMITIVE_BASE_TYPE(TYPE_DOUBLE);
    APPLY_FOR_PRIMITIVE_BASE_TYPE(TYPE_DATEV2);
    APPLY_FOR_PRIMITIVE_BASE_TYPE(TYPE_DATETIMEV2);
    APPLY_FOR_PRIMITIVE_TYPE(TYPE_DATETIME, VecDateTimeValue(0, 3, 0, 0, 0, 2020, 1, 1),
                             VecDateTimeValue(0, 3, 0, 0, 0, 2020, 1, 2));
    APPLY_FOR_PRIMITIVE_TYPE(TYPE_DATE, VecDateTimeValue(0, 2, 0, 0, 0, 2020, 1, 1),
                             VecDateTimeValue(0, 2, 0, 0, 0, 2020, 1, 2));
    APPLY_FOR_PRIMITIVE_TYPE(TYPE_DECIMALV2, DecimalV2Value(1, 1), DecimalV2Value(1, 2));
    APPLY_FOR_PRIMITIVE_BASE_TYPE(TYPE_DECIMAL32);
    APPLY_FOR_PRIMITIVE_BASE_TYPE(TYPE_DECIMAL64);
    APPLY_FOR_PRIMITIVE_BASE_TYPE(TYPE_DECIMAL128I);
    APPLY_FOR_PRIMITIVE_TYPE(TYPE_DECIMAL256, vectorized::Decimal256(0), vectorized::Decimal256(1));
    APPLY_FOR_PRIMITIVE_TYPE(TYPE_VARCHAR, StringRef("1"), StringRef("2"));
    APPLY_FOR_PRIMITIVE_TYPE(TYPE_CHAR, StringRef("1"), StringRef("2"));
    APPLY_FOR_PRIMITIVE_TYPE(TYPE_STRING, StringRef("1"), StringRef("2"));
    APPLY_FOR_PRIMITIVE_BASE_TYPE(TYPE_IPV4);
    APPLY_FOR_PRIMITIVE_BASE_TYPE(TYPE_IPV6);
}

} // namespace doris
