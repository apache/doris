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

#include "exec/runtime_filter/runtime_filter_wrapper.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <string>
#include <utility>

#include "core/data_type/data_type_date.h"
#include "core/data_type/data_type_date_time.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/field.h"
#include "core/value/vdatetime_value.h"
#include "exec/runtime_filter/utils.h"
#include "exprs/bloom_filter_func.h"
#include "exprs/hybrid_set.h"
#include "exprs/minmax_predicate.h"
#include "exprs/vexpr.h"
#include "exprs/vliteral.h"
#include "testutil/column_helper.h"

namespace doris {

class RuntimeFilterWrapperTest : public testing::Test {
public:
    RuntimeFilterWrapperTest() = default;
    ~RuntimeFilterWrapperTest() override = default;
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(RuntimeFilterWrapperTest, TestIn) {
    using DataType = DataTypeInt32;
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
            .bloom_filter_size_calculated_by_ndv = bloom_filter_size_calculated_by_ndv};

    auto wrapper = std::make_shared<RuntimeFilterWrapper>(&params);
    EXPECT_EQ(wrapper->minmax_func(), nullptr);
    EXPECT_EQ(wrapper->bloom_filter_func(), nullptr);
    EXPECT_NE(wrapper->hybrid_set(), nullptr);
    EXPECT_FALSE(wrapper->build_bf_by_runtime_size());
    {
        // Init
        EXPECT_TRUE(wrapper->init(2).ok());
        EXPECT_EQ(wrapper->get_state(), RuntimeFilterWrapper::State::UNINITED);

        EXPECT_TRUE(wrapper->init(3).ok());
        EXPECT_EQ(wrapper->get_state(), RuntimeFilterWrapper::State::DISABLED);

        wrapper->_state = RuntimeFilterWrapper::State::UNINITED;
        wrapper->_reason.update({});
    }
    {
        // Insert
        auto col = ColumnHelper::create_column<DataType>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        EXPECT_EQ(wrapper->insert(col, 0).code(), ErrorCode::INTERNAL_ERROR);
        wrapper->_state = RuntimeFilterWrapper::State::UNINITED;
        wrapper->_reason.update({});

        col = ColumnHelper::create_column<DataType>({0});
        EXPECT_TRUE(wrapper->insert(col, 0).ok());
        EXPECT_EQ(wrapper->hybrid_set()->size(), col->size());
        EXPECT_EQ(wrapper->get_state(), RuntimeFilterWrapper::State::UNINITED);
    }
    {
        // Merge 1 (valid filter)
        auto another_wrapper = std::make_shared<RuntimeFilterWrapper>(&params);
        EXPECT_TRUE(another_wrapper->init(2).ok());
        EXPECT_EQ(another_wrapper->get_state(), RuntimeFilterWrapper::State::UNINITED);

        auto col = ColumnHelper::create_column<DataType>({1});
        EXPECT_TRUE(another_wrapper->insert(col, 0).ok());
        another_wrapper->_state = RuntimeFilterWrapper::State::READY;
        EXPECT_TRUE(wrapper->merge(another_wrapper.get()).ok());
        EXPECT_EQ(wrapper->hybrid_set()->size(), 2);
        EXPECT_EQ(wrapper->get_state(), RuntimeFilterWrapper::State::READY);
        EXPECT_TRUE(wrapper->to_protobuf(valid_request.mutable_in_filter()).ok());

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
        col = ColumnHelper::create_column<DataType>({1});
        EXPECT_TRUE(another_wrapper->insert(col, 0).ok());
        another_wrapper->_state = RuntimeFilterWrapper::State::READY;
        EXPECT_TRUE(wrapper->merge(another_wrapper.get()).ok());
        EXPECT_EQ(wrapper->hybrid_set()->size(), 0);
        EXPECT_EQ(wrapper->get_state(), RuntimeFilterWrapper::State::DISABLED)
                << RuntimeFilterWrapper::to_string(wrapper->get_state());
        wrapper->_state = RuntimeFilterWrapper::State::UNINITED;

        // Merge 5 (valid filter)
        another_wrapper = std::make_shared<RuntimeFilterWrapper>(&params);
        EXPECT_TRUE(another_wrapper->init(2).ok());
        EXPECT_EQ(another_wrapper->get_state(), RuntimeFilterWrapper::State::UNINITED);
        col = ColumnHelper::create_column<DataType>({0, 1});
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
        col = ColumnHelper::create_column<DataType>({3, 4});
        EXPECT_TRUE(another_wrapper->insert(col, 0).ok());
        another_wrapper->_state = RuntimeFilterWrapper::State::READY;
        EXPECT_TRUE(wrapper->merge(another_wrapper.get()).ok());
        EXPECT_EQ(wrapper->hybrid_set()->size(), 0);
        EXPECT_EQ(wrapper->get_state(), RuntimeFilterWrapper::State::DISABLED);
        wrapper->_state = RuntimeFilterWrapper::State::UNINITED;
        wrapper->_reason.update({});
    }
    {
        // Assign disabled filter
        PMergeFilterRequest request;
        request.set_disabled(true);
        EXPECT_TRUE(wrapper->assign(request, nullptr).ok());
        EXPECT_EQ(wrapper->get_state(), RuntimeFilterWrapper::State::DISABLED);
        wrapper->_state = RuntimeFilterWrapper::State::UNINITED;
        wrapper->_reason.update({});

        // Assign valid filter
        valid_request.set_contain_null(false);
        valid_request.set_filter_type(PFilterType::IN_FILTER);
        EXPECT_TRUE(wrapper->assign(valid_request, nullptr).ok());
        EXPECT_EQ(wrapper->get_state(), RuntimeFilterWrapper::State::READY);
        EXPECT_EQ(wrapper->hybrid_set()->size(), 2);
    }
    EXPECT_EQ(wrapper->get_real_type(), RuntimeFilterType::IN_FILTER);
    EXPECT_EQ(wrapper->column_type(), column_return_type);
    EXPECT_EQ(wrapper->contain_null(), false);
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
                .bloom_filter_size_calculated_by_ndv = bloom_filter_size_calculated_by_ndv};       \
        auto wrapper = std::make_shared<RuntimeFilterWrapper>(&params);                            \
        PMergeFilterRequest valid_request;                                                         \
        auto* in_filter = valid_request.mutable_in_filter();                                       \
        in_filter->set_column_type(PColumnType::COLUMN_TYPE_BOOL);                                 \
        get_convertor<PrimitiveTypeTraits<column_return_type>::CppType>()(in_filter->add_values(), \
                                                                          value1);                 \
        get_convertor<PrimitiveTypeTraits<column_return_type>::CppType>()(in_filter->add_values(), \
                                                                          value2);                 \
        valid_request.set_contain_null(true);                                                      \
        valid_request.set_filter_type(PFilterType::IN_FILTER);                                     \
        EXPECT_TRUE(wrapper->assign(valid_request, nullptr).ok());                                 \
        EXPECT_EQ(wrapper->get_state(), RuntimeFilterWrapper::State::READY);                       \
        EXPECT_EQ(wrapper->hybrid_set()->size(), 2);                                               \
    }

#define APPLY_FOR_PRIMITIVE_BASE_TYPE(TYPE)                                               \
    APPLY_FOR_PRIMITIVE_TYPE(TYPE, type_limit<PrimitiveTypeTraits<TYPE>::CppType>::min(), \
                             type_limit<PrimitiveTypeTraits<TYPE>::CppType>::max())

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
    APPLY_FOR_PRIMITIVE_BASE_TYPE(TYPE_TIMESTAMP_NS);
    APPLY_FOR_PRIMITIVE_TYPE(TYPE_DATETIME, VecDateTimeValue(0, TIME_DATETIME, 0, 0, 0, 2020, 1, 1),
                             VecDateTimeValue(0, TIME_DATETIME, 0, 0, 0, 2020, 1, 2));
    APPLY_FOR_PRIMITIVE_TYPE(TYPE_DATE, VecDateTimeValue(0, TIME_DATE, 0, 0, 0, 2020, 1, 1),
                             VecDateTimeValue(0, TIME_DATE, 0, 0, 0, 2020, 1, 2));
    APPLY_FOR_PRIMITIVE_TYPE(TYPE_DECIMALV2, DecimalV2Value(1, 1), DecimalV2Value(1, 2));
    APPLY_FOR_PRIMITIVE_BASE_TYPE(TYPE_DECIMAL32);
    APPLY_FOR_PRIMITIVE_BASE_TYPE(TYPE_DECIMAL64);
    APPLY_FOR_PRIMITIVE_BASE_TYPE(TYPE_DECIMAL128I);
    APPLY_FOR_PRIMITIVE_TYPE(TYPE_DECIMAL256, Decimal256(0), Decimal256(1));
    APPLY_FOR_PRIMITIVE_TYPE(TYPE_VARCHAR, "1", "2");
    APPLY_FOR_PRIMITIVE_TYPE(TYPE_CHAR, "1", "2");
    APPLY_FOR_PRIMITIVE_TYPE(TYPE_STRING, "1", "2");
    APPLY_FOR_PRIMITIVE_BASE_TYPE(TYPE_IPV4);
    APPLY_FOR_PRIMITIVE_BASE_TYPE(TYPE_IPV6);
#undef APPLY_FOR_PRIMITIVE_TYPE
#undef APPLY_FOR_PRIMITIVE_BASE_TYPE
}

TEST_F(RuntimeFilterWrapperTest, TestMinMaxAssign) {
    int32_t filter_id = 0;
    RuntimeFilterType filter_type = RuntimeFilterType::MINMAX_FILTER;
    bool null_aware = true;

    int32_t max_in_num = 2;

    int64_t runtime_bloom_filter_min_size = 0;
    int64_t runtime_bloom_filter_max_size = 0;
    bool build_bf_by_runtime_size = true;
    int64_t bloom_filter_size = 0;
    bool bloom_filter_size_calculated_by_ndv = true;

#define APPLY_FOR_PRIMITIVE_TYPE(TYPE, value1, value2)                                       \
    {                                                                                        \
        static constexpr PrimitiveType column_return_type = PrimitiveType::TYPE;             \
        RuntimeFilterParams params {                                                         \
                .filter_id = filter_id,                                                      \
                .filter_type = filter_type,                                                  \
                .column_return_type = column_return_type,                                    \
                .null_aware = null_aware,                                                    \
                .max_in_num = max_in_num,                                                    \
                .runtime_bloom_filter_min_size = runtime_bloom_filter_min_size,              \
                .runtime_bloom_filter_max_size = runtime_bloom_filter_max_size,              \
                .bloom_filter_size = bloom_filter_size,                                      \
                .build_bf_by_runtime_size = build_bf_by_runtime_size,                        \
                .bloom_filter_size_calculated_by_ndv = bloom_filter_size_calculated_by_ndv}; \
        auto wrapper = std::make_shared<RuntimeFilterWrapper>(&params);                      \
        PMergeFilterRequest valid_request;                                                   \
        auto* minmax_filter = valid_request.mutable_minmax_filter();                         \
        minmax_filter->set_column_type(PColumnType::COLUMN_TYPE_BOOL);                       \
        get_convertor<PrimitiveTypeTraits<column_return_type>::CppType>()(                   \
                minmax_filter->mutable_min_val(), value1);                                   \
        get_convertor<PrimitiveTypeTraits<column_return_type>::CppType>()(                   \
                minmax_filter->mutable_max_val(), value2);                                   \
        valid_request.set_contain_null(true);                                                \
        valid_request.set_filter_type(PFilterType::MINMAX_FILTER);                           \
        EXPECT_TRUE(wrapper->assign(valid_request, nullptr).ok());                           \
        EXPECT_EQ(wrapper->get_state(), RuntimeFilterWrapper::State::READY);                 \
        EXPECT_EQ(*(PrimitiveTypeTraits<column_return_type>::CppType*)wrapper->minmax_func() \
                           ->get_min(),                                                      \
                  value1);                                                                   \
        EXPECT_EQ(*(PrimitiveTypeTraits<column_return_type>::CppType*)wrapper->minmax_func() \
                           ->get_max(),                                                      \
                  value2);                                                                   \
    }

#define APPLY_FOR_PRIMITIVE_BASE_TYPE(TYPE)                                               \
    APPLY_FOR_PRIMITIVE_TYPE(TYPE, type_limit<PrimitiveTypeTraits<TYPE>::CppType>::min(), \
                             type_limit<PrimitiveTypeTraits<TYPE>::CppType>::max())

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
    APPLY_FOR_PRIMITIVE_BASE_TYPE(TYPE_TIMESTAMP_NS);
    APPLY_FOR_PRIMITIVE_TYPE(TYPE_DATETIME, VecDateTimeValue(0, TIME_DATETIME, 0, 0, 0, 2020, 1, 1),
                             VecDateTimeValue(0, TIME_DATETIME, 0, 0, 0, 2020, 1, 2));
    APPLY_FOR_PRIMITIVE_TYPE(TYPE_DATE, VecDateTimeValue(0, TIME_DATE, 0, 0, 0, 2020, 1, 1),
                             VecDateTimeValue(0, TIME_DATE, 0, 0, 0, 2020, 1, 2));
    APPLY_FOR_PRIMITIVE_TYPE(TYPE_DECIMALV2, DecimalV2Value(1, 1), DecimalV2Value(1, 2));
    APPLY_FOR_PRIMITIVE_BASE_TYPE(TYPE_DECIMAL32);
    APPLY_FOR_PRIMITIVE_BASE_TYPE(TYPE_DECIMAL64);
    APPLY_FOR_PRIMITIVE_BASE_TYPE(TYPE_DECIMAL128I);
    APPLY_FOR_PRIMITIVE_TYPE(TYPE_DECIMAL256, Decimal256(0), Decimal256(1));
    APPLY_FOR_PRIMITIVE_TYPE(TYPE_VARCHAR, "1", "2");
    APPLY_FOR_PRIMITIVE_TYPE(TYPE_CHAR, "1", "2");
    APPLY_FOR_PRIMITIVE_TYPE(TYPE_STRING, "1", "2");
    APPLY_FOR_PRIMITIVE_BASE_TYPE(TYPE_IPV4);
    APPLY_FOR_PRIMITIVE_BASE_TYPE(TYPE_IPV6);
#undef APPLY_FOR_PRIMITIVE_TYPE
#undef APPLY_FOR_PRIMITIVE_BASE_TYPE
}

TEST_F(RuntimeFilterWrapperTest, DateMinMaxAssignPreservesDateType) {
    RuntimeFilterParams params {.filter_id = 0,
                                .filter_type = RuntimeFilterType::MINMAX_FILTER,
                                .column_return_type = TYPE_DATE,
                                .null_aware = false};
    PMergeFilterRequest request;
    request.set_filter_type(PFilterType::MINMAX_FILTER);
    request.mutable_minmax_filter()->mutable_min_val()->set_stringval("2012-01-02");
    request.mutable_minmax_filter()->mutable_max_val()->set_stringval("2012-03-02");

    RuntimeFilterWrapper wrapper(&params);
    ASSERT_TRUE(wrapper.assign(request, nullptr).ok());
    EXPECT_EQ(static_cast<VecDateTimeValue*>(wrapper.minmax_func()->get_min())->type(), TIME_DATE);
    EXPECT_EQ(static_cast<VecDateTimeValue*>(wrapper.minmax_func()->get_max())->type(), TIME_DATE);
}

static void check_date_like_minmax_bound(const void* bound, TimeType time_type,
                                         PrimitiveType column_type, const std::string& text,
                                         const DataTypePtr& data_type) {
    const auto* value = static_cast<const VecDateTimeValue*>(bound);
    EXPECT_EQ(value->type(), time_type);
    char buffer[30];
    value->to_string(buffer);
    EXPECT_EQ(std::string(buffer), text);

    VExprSPtr literal;
    ASSERT_TRUE(create_literal(data_type, value, literal).ok());
    ASSERT_TRUE(literal->is_literal());
    EXPECT_EQ(literal->result_type(), column_type);
    Field field;
    static_cast<VLiteral*>(literal.get())->get_column_ptr()->get(0, field);
    EXPECT_EQ(field.get_type(), column_type);
}

template <typename Request>
static void check_date_like_minmax_request(RuntimeFilterWrapper& producer,
                                           const RuntimeFilterParams& params, TimeType time_type,
                                           const std::string& min_text, const std::string& max_text,
                                           const DataTypePtr& data_type) {
    Request request;
    request.set_filter_type(get_type(params.filter_type));
    ASSERT_TRUE(producer.to_protobuf(request.mutable_minmax_filter()).ok());
    RuntimeFilterWrapper receiver(&params);
    ASSERT_TRUE(receiver.assign(request, nullptr).ok());
    EXPECT_EQ(receiver.get_state(), RuntimeFilterWrapper::State::READY);
    EXPECT_EQ(receiver.column_type(), params.column_return_type);
    // MIN_FILTER and MAX_FILTER serialize only their owned bound, so the other bound is absent.
    if (params.filter_type != RuntimeFilterType::MAX_FILTER) {
        check_date_like_minmax_bound(receiver.minmax_func()->get_min(), time_type,
                                     params.column_return_type, min_text, data_type);
    }
    if (params.filter_type != RuntimeFilterType::MIN_FILTER) {
        check_date_like_minmax_bound(receiver.minmax_func()->get_max(), time_type,
                                     params.column_return_type, max_text, data_type);
    }
}

static void check_date_like_minmax_round_trip(
        RuntimeFilterType filter_type, PrimitiveType column_type, TimeType time_type,
        VecDateTimeValue min_value, VecDateTimeValue max_value, const std::string& min_text,
        const std::string& max_text, const DataTypePtr& data_type) {
    RuntimeFilterParams params {.filter_id = 0,
                                .filter_type = filter_type,
                                .column_return_type = column_type,
                                .null_aware = false};
    RuntimeFilterWrapper producer(&params);
    ASSERT_TRUE(producer.minmax_func()->assign(&min_value, &max_value).ok());
    check_date_like_minmax_request<PMergeFilterRequest>(producer, params, time_type, min_text,
                                                        max_text, data_type);
    check_date_like_minmax_request<PPublishFilterRequestV2>(producer, params, time_type, min_text,
                                                            max_text, data_type);
}

TEST_F(RuntimeFilterWrapperTest, DateLikeMinMaxRoundTripPreservesType) {
    for (const auto filter_type : {RuntimeFilterType::MIN_FILTER, RuntimeFilterType::MAX_FILTER,
                                   RuntimeFilterType::MINMAX_FILTER}) {
        check_date_like_minmax_round_trip(filter_type, TYPE_DATE, TIME_DATE,
                                          VecDateTimeValue(0, TIME_DATE, 0, 0, 0, 2012, 1, 2),
                                          VecDateTimeValue(0, TIME_DATE, 0, 0, 0, 2012, 3, 2),
                                          "2012-01-02", "2012-03-02",
                                          std::make_shared<DataTypeDate>());
        check_date_like_minmax_round_trip(filter_type, TYPE_DATETIME, TIME_DATETIME,
                                          VecDateTimeValue(0, TIME_DATETIME, 1, 2, 3, 2012, 1, 2),
                                          VecDateTimeValue(0, TIME_DATETIME, 4, 5, 6, 2012, 3, 2),
                                          "2012-01-02 01:02:03", "2012-03-02 04:05:06",
                                          std::make_shared<DataTypeDateTime>());
    }
}

TEST_F(RuntimeFilterWrapperTest, DateMinMaxAssignAcceptsMidnightDateTimeText) {
    RuntimeFilterParams params {.filter_id = 0,
                                .filter_type = RuntimeFilterType::MINMAX_FILTER,
                                .column_return_type = TYPE_DATE,
                                .null_aware = false};
    PMergeFilterRequest request;
    request.set_filter_type(PFilterType::MINMAX_FILTER);
    request.mutable_minmax_filter()->mutable_min_val()->set_stringval("2012-01-02 00:00:00");
    request.mutable_minmax_filter()->mutable_max_val()->set_stringval("2012-03-02 00:00:00");

    RuntimeFilterWrapper wrapper(&params);
    ASSERT_TRUE(wrapper.assign(request, nullptr).ok());
    auto data_type = std::make_shared<DataTypeDate>();
    for (const auto& [bound, expected] :
         {std::pair {wrapper.minmax_func()->get_min(), "2012-01-02"},
          std::pair {wrapper.minmax_func()->get_max(), "2012-03-02"}}) {
        const auto* value = static_cast<const VecDateTimeValue*>(bound);
        EXPECT_EQ(value->type(), TIME_DATE);
        char buffer[30];
        value->to_string(buffer);
        EXPECT_EQ(std::string(buffer), expected);
        VExprSPtr literal;
        ASSERT_TRUE(create_literal(data_type, value, literal).ok());
        EXPECT_EQ(literal->result_type(), TYPE_DATE);
        Field field;
        static_cast<VLiteral*>(literal.get())->get_column_ptr()->get(0, field);
        EXPECT_EQ(field.get_type(), TYPE_DATE);
    }
}

TEST_F(RuntimeFilterWrapperTest, TestBloom) {
    std::vector<int> data_vector(10);
    std::iota(data_vector.begin(), data_vector.end(), 0);
    using DataType = DataTypeInt32;
    int32_t filter_id = 0;
    auto runtime_size = 80;
    RuntimeFilterType filter_type = RuntimeFilterType::BLOOM_FILTER;
    bool null_aware = false;
    PrimitiveType column_return_type = PrimitiveType::TYPE_INT;

    int32_t max_in_num = 0;

    int64_t runtime_bloom_filter_min_size = 64;
    int64_t runtime_bloom_filter_max_size = 128;
    bool build_bf_by_runtime_size = false;
    int64_t bloom_filter_size = 0;
    bool bloom_filter_size_calculated_by_ndv = true;

    std::shared_ptr<RuntimeFilterWrapper> wrapper;
    {
        bloom_filter_size = 256;
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
                .bloom_filter_size_calculated_by_ndv = bloom_filter_size_calculated_by_ndv};
        wrapper = std::make_shared<RuntimeFilterWrapper>(&params);
        EXPECT_FALSE(wrapper->build_bf_by_runtime_size());
        EXPECT_EQ(wrapper->bloom_filter_func()->_bloom_filter_length,
                  runtime_bloom_filter_max_size);
    }
    {
        bloom_filter_size = 32;
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
                .bloom_filter_size_calculated_by_ndv = bloom_filter_size_calculated_by_ndv};
        wrapper = std::make_shared<RuntimeFilterWrapper>(&params);
        EXPECT_EQ(wrapper->bloom_filter_func()->_bloom_filter_length,
                  runtime_bloom_filter_min_size);
        // Init (set BF size by estimated size from FE)
        EXPECT_TRUE(wrapper->init(80).ok());
        EXPECT_EQ(wrapper->get_state(), RuntimeFilterWrapper::State::UNINITED);
        EXPECT_EQ(wrapper->bloom_filter_func()->_bloom_filter_length,
                  runtime_bloom_filter_min_size);
    }
    {
        build_bf_by_runtime_size = true;
        bloom_filter_size = 32;
        bloom_filter_size_calculated_by_ndv = false;
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
                .bloom_filter_size_calculated_by_ndv = bloom_filter_size_calculated_by_ndv};
        wrapper = std::make_shared<RuntimeFilterWrapper>(&params);
        EXPECT_TRUE(wrapper->build_bf_by_runtime_size());
        EXPECT_EQ(wrapper->bloom_filter_func()->_bloom_filter_length,
                  runtime_bloom_filter_min_size);
        // Init (set BF size by exact size from BE)
        double m = -BloomFilterFuncBase::K * runtime_size /
                   std::log(1 - std::pow(BloomFilterFuncBase::FPP, 1.0 / BloomFilterFuncBase::K));
        int log_filter_size = std::max(0, (int)(std::ceil(std::log(m / 8) / std::log(2))));
        auto be_calculate_size = (((int64_t)1) << log_filter_size);
        EXPECT_TRUE(wrapper->init(runtime_size).ok());
        EXPECT_EQ(wrapper->get_state(), RuntimeFilterWrapper::State::UNINITED);
        EXPECT_EQ(wrapper->bloom_filter_func()->_bloom_filter_length, be_calculate_size);
    }
    {
        build_bf_by_runtime_size = true;
        bloom_filter_size = 32;
        bloom_filter_size_calculated_by_ndv = true;
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
                .bloom_filter_size_calculated_by_ndv = bloom_filter_size_calculated_by_ndv};
        wrapper = std::make_shared<RuntimeFilterWrapper>(&params);
        EXPECT_EQ(wrapper->bloom_filter_func()->_bloom_filter_length,
                  runtime_bloom_filter_min_size);
        // Init (set BF size by exact size from BE)
        auto runtime_size = 80;
        double m = -BloomFilterFuncBase::K * runtime_size /
                   std::log(1 - std::pow(BloomFilterFuncBase::FPP, 1.0 / BloomFilterFuncBase::K));
        int log_filter_size = std::max(0, (int)(std::ceil(std::log(m / 8) / std::log(2))));
        auto be_calculate_size = (((int64_t)1) << log_filter_size);
        // Init (set BF size by min size of exact size from BE and the estimated size from FE)
        EXPECT_TRUE(wrapper->init(runtime_size).ok());
        EXPECT_EQ(wrapper->get_state(), RuntimeFilterWrapper::State::UNINITED);
        EXPECT_EQ(wrapper->bloom_filter_func()->_bloom_filter_length,
                  std::min(be_calculate_size, runtime_bloom_filter_min_size));
    }
    {
        // Insert
        auto col = ColumnHelper::create_column<DataType>(data_vector);
        EXPECT_TRUE(wrapper->insert(col, 0).ok());
        EXPECT_EQ(wrapper->get_state(), RuntimeFilterWrapper::State::UNINITED);

        col = ColumnHelper::create_column<DataType>(data_vector);
        std::vector<uint8_t> res(10);
        wrapper->bloom_filter_func()->find_fixed_len(col, res.data());
        EXPECT_TRUE(std::all_of(res.begin(), res.end(), [](uint8_t i) -> bool { return i; }));
    }
    {
        PMergeFilterRequest valid_request;
        valid_request.set_contain_null(false);
        valid_request.set_filter_type(PFilterType::BLOOM_FILTER);
        valid_request.set_filter_id(filter_id);
        char* data = nullptr;
        int len = 0;
        EXPECT_TRUE(wrapper->to_protobuf(valid_request.mutable_bloom_filter(), &data, &len).ok());

        const auto str = std::string(data, len);
        butil::IOBuf io_buf;
        io_buf.operator=(str);
        butil::IOBufAsZeroCopyInputStream stream(io_buf);
        RuntimeFilterParams new_params {
                .filter_id = filter_id,
                .filter_type = filter_type,
                .column_return_type = column_return_type,
                .null_aware = null_aware,
                .max_in_num = max_in_num,
                .runtime_bloom_filter_min_size = runtime_bloom_filter_min_size,
                .runtime_bloom_filter_max_size = runtime_bloom_filter_max_size,
                .bloom_filter_size = bloom_filter_size,
                .build_bf_by_runtime_size = build_bf_by_runtime_size,
                .bloom_filter_size_calculated_by_ndv = bloom_filter_size_calculated_by_ndv};
        auto new_wrapper = std::make_shared<RuntimeFilterWrapper>(&new_params);
        EXPECT_TRUE(new_wrapper->assign(valid_request, &stream).ok());

        auto col = ColumnHelper::create_column<DataType>(data_vector);
        std::vector<uint8_t> res(10);
        wrapper->bloom_filter_func()->find_fixed_len(col, res.data());
        EXPECT_TRUE(std::all_of(res.begin(), res.end(), [](uint8_t i) -> bool { return i; }));
    }
    {
        RuntimeFilterParams new_params {
                .filter_id = filter_id,
                .filter_type = filter_type,
                .column_return_type = column_return_type,
                .null_aware = null_aware,
                .max_in_num = max_in_num,
                .runtime_bloom_filter_min_size = runtime_bloom_filter_min_size,
                .runtime_bloom_filter_max_size = runtime_bloom_filter_max_size,
                .bloom_filter_size = bloom_filter_size,
                .build_bf_by_runtime_size = build_bf_by_runtime_size,
                .bloom_filter_size_calculated_by_ndv = bloom_filter_size_calculated_by_ndv};
        auto new_wrapper = std::make_shared<RuntimeFilterWrapper>(&new_params);
        EXPECT_TRUE(new_wrapper->init(runtime_size).ok());
        EXPECT_EQ(new_wrapper->get_state(), RuntimeFilterWrapper::State::UNINITED);
        EXPECT_EQ(new_wrapper->bloom_filter_func()->_bloom_filter_length,
                  wrapper->bloom_filter_func()->_bloom_filter_length);
        // Insert
        std::vector<int> new_data_vector(10);
        std::iota(new_data_vector.begin(), new_data_vector.end(), 10);
        auto col = ColumnHelper::create_column<DataType>(new_data_vector);
        EXPECT_TRUE(new_wrapper->insert(col, 0).ok());
        EXPECT_EQ(new_wrapper->get_state(), RuntimeFilterWrapper::State::UNINITED);
        new_wrapper->_state = RuntimeFilterWrapper::State::READY;
        // Merge
        std::vector<int> res_data_vector(20);
        std::iota(res_data_vector.begin(), res_data_vector.end(), 0);
        EXPECT_TRUE(wrapper->merge(new_wrapper.get()).ok());
        col = ColumnHelper::create_column<DataType>(res_data_vector);
        std::vector<uint8_t> res(20);
        wrapper->bloom_filter_func()->find_fixed_len(col, res.data());
        EXPECT_TRUE(std::all_of(res.begin(), res.end(), [](uint8_t i) -> bool { return i; }));
    }
    EXPECT_EQ(wrapper->filter_id(), filter_id);
    EXPECT_TRUE(wrapper->is_valid());
    EXPECT_EQ(wrapper->get_real_type(), RuntimeFilterType::BLOOM_FILTER);
    EXPECT_EQ(wrapper->column_type(), column_return_type);
    EXPECT_EQ(wrapper->contain_null(), false);
}

TEST_F(RuntimeFilterWrapperTest, TestMinMax) {
    const int min_val = -1;
    const int num_vals = 10;
    std::vector<int> data_vector(num_vals);
    std::iota(data_vector.begin(), data_vector.end(), min_val);
    using DataType = DataTypeInt32;
    int32_t filter_id = 0;
    RuntimeFilterType filter_type = RuntimeFilterType::MINMAX_FILTER;
    bool null_aware = false;
    PrimitiveType column_return_type = PrimitiveType::TYPE_INT;

    int32_t max_in_num = 0;

    int64_t runtime_bloom_filter_min_size = 64;
    int64_t runtime_bloom_filter_max_size = 128;
    bool build_bf_by_runtime_size = false;
    int64_t bloom_filter_size = 0;
    bool bloom_filter_size_calculated_by_ndv = true;

    std::shared_ptr<RuntimeFilterWrapper> wrapper;
    // MinMax
    {
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
                .bloom_filter_size_calculated_by_ndv = bloom_filter_size_calculated_by_ndv};
        {
            wrapper = std::make_shared<RuntimeFilterWrapper>(&params);
            EXPECT_TRUE(wrapper->init(80).ok());
            wrapper->check_state({RuntimeFilterWrapper::State::UNINITED});
            EXPECT_EQ(*(int*)wrapper->minmax_func()->get_max(), type_limit<int>::min());
            EXPECT_EQ(*(int*)wrapper->minmax_func()->get_min(), type_limit<int>::max());
        }
        {
            // Insert
            auto col = ColumnHelper::create_column<DataType>(data_vector);
            EXPECT_TRUE(wrapper->insert(col, 0).ok());
            wrapper->check_state({RuntimeFilterWrapper::State::UNINITED});
            EXPECT_EQ(*(int*)wrapper->minmax_func()->get_max(), min_val + num_vals - 1);
            EXPECT_EQ(*(int*)wrapper->minmax_func()->get_min(), min_val);
        }
        {
            PMergeFilterRequest valid_request;
            valid_request.set_contain_null(false);
            valid_request.set_filter_type(PFilterType::MINMAX_FILTER);
            valid_request.set_filter_id(filter_id);
            EXPECT_TRUE(wrapper->to_protobuf(valid_request.mutable_minmax_filter()).ok());

            auto new_wrapper = std::make_shared<RuntimeFilterWrapper>(&params);
            EXPECT_TRUE(new_wrapper->assign(valid_request, nullptr).ok());

            EXPECT_EQ(*(int*)wrapper->minmax_func()->get_max(),
                      *(int*)new_wrapper->minmax_func()->get_max());
            EXPECT_EQ(*(int*)wrapper->minmax_func()->get_min(),
                      *(int*)new_wrapper->minmax_func()->get_min());
        }
        {
            auto new_wrapper = std::make_shared<RuntimeFilterWrapper>(&params);
            EXPECT_TRUE(new_wrapper->init(12312).ok());
            EXPECT_EQ(new_wrapper->get_state(), RuntimeFilterWrapper::State::UNINITED);
            wrapper->check_state({RuntimeFilterWrapper::State::UNINITED});
            // Insert
            std::vector<int> new_data_vector {-100, 100};
            auto col = ColumnHelper::create_column<DataType>(new_data_vector);
            EXPECT_TRUE(new_wrapper->insert(col, 0).ok());
            EXPECT_EQ(new_wrapper->get_state(), RuntimeFilterWrapper::State::UNINITED);
            wrapper->check_state({RuntimeFilterWrapper::State::UNINITED});
            new_wrapper->_state = RuntimeFilterWrapper::State::READY;
            // Merge
            EXPECT_TRUE(wrapper->merge(new_wrapper.get()).ok());
            EXPECT_EQ(*(int*)wrapper->minmax_func()->get_max(), 100);
            EXPECT_EQ(*(int*)wrapper->minmax_func()->get_min(), -100);
        }
        EXPECT_EQ(wrapper->filter_id(), filter_id);
        EXPECT_TRUE(wrapper->is_valid());
        EXPECT_EQ(wrapper->get_real_type(), RuntimeFilterType::MINMAX_FILTER);
        EXPECT_EQ(wrapper->column_type(), column_return_type);
        EXPECT_EQ(wrapper->contain_null(), false);
    }
    // Min
    {
        filter_type = RuntimeFilterType::MIN_FILTER;
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
                .bloom_filter_size_calculated_by_ndv = bloom_filter_size_calculated_by_ndv};
        {
            wrapper = std::make_shared<RuntimeFilterWrapper>(&params);
            EXPECT_TRUE(wrapper->init(80).ok());
            EXPECT_EQ(wrapper->get_state(), RuntimeFilterWrapper::State::UNINITED);
            wrapper->check_state({RuntimeFilterWrapper::State::UNINITED});
            EXPECT_EQ(*(int*)wrapper->minmax_func()->get_max(), type_limit<int>::min());
            EXPECT_EQ(*(int*)wrapper->minmax_func()->get_min(), type_limit<int>::max());
        }
        {
            // Insert
            auto col = ColumnHelper::create_column<DataType>(data_vector);
            EXPECT_TRUE(wrapper->insert(col, 0).ok());
            EXPECT_EQ(wrapper->get_state(), RuntimeFilterWrapper::State::UNINITED);
            wrapper->check_state({RuntimeFilterWrapper::State::UNINITED});
            EXPECT_EQ(*(int*)wrapper->minmax_func()->get_max(), min_val + num_vals - 1);
            EXPECT_EQ(*(int*)wrapper->minmax_func()->get_min(), min_val);
        }
        {
            PMergeFilterRequest valid_request;
            valid_request.set_contain_null(false);
            valid_request.set_filter_type(PFilterType::MIN_FILTER);
            valid_request.set_filter_id(filter_id);
            EXPECT_TRUE(wrapper->to_protobuf(valid_request.mutable_minmax_filter()).ok());

            auto new_wrapper = std::make_shared<RuntimeFilterWrapper>(&params);
            EXPECT_TRUE(new_wrapper->assign(valid_request, nullptr).ok());

            EXPECT_EQ(*(int*)wrapper->minmax_func()->get_min(),
                      *(int*)new_wrapper->minmax_func()->get_min());
        }
        {
            auto new_wrapper = std::make_shared<RuntimeFilterWrapper>(&params);
            EXPECT_TRUE(new_wrapper->init(12312).ok());
            EXPECT_EQ(new_wrapper->get_state(), RuntimeFilterWrapper::State::UNINITED);
            wrapper->check_state({RuntimeFilterWrapper::State::UNINITED});
            // Insert
            std::vector<int> new_data_vector {-100, 100};
            auto col = ColumnHelper::create_column<DataType>(new_data_vector);
            EXPECT_TRUE(new_wrapper->insert(col, 0).ok());
            EXPECT_EQ(new_wrapper->get_state(), RuntimeFilterWrapper::State::UNINITED);
            wrapper->check_state({RuntimeFilterWrapper::State::UNINITED});
            new_wrapper->_state = RuntimeFilterWrapper::State::READY;
            // Merge
            EXPECT_TRUE(wrapper->merge(new_wrapper.get()).ok());
            EXPECT_EQ(*(int*)wrapper->minmax_func()->get_max(), 100);
            EXPECT_EQ(*(int*)wrapper->minmax_func()->get_min(), -100);
        }
        EXPECT_EQ(wrapper->filter_id(), filter_id);
        EXPECT_TRUE(wrapper->is_valid());
        EXPECT_EQ(wrapper->get_real_type(), RuntimeFilterType::MIN_FILTER);
        EXPECT_EQ(wrapper->column_type(), column_return_type);
        EXPECT_EQ(wrapper->contain_null(), false);
    }
    // Max
    {
        filter_type = RuntimeFilterType::MAX_FILTER;
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
                .bloom_filter_size_calculated_by_ndv = bloom_filter_size_calculated_by_ndv};
        {
            wrapper = std::make_shared<RuntimeFilterWrapper>(&params);
            EXPECT_TRUE(wrapper->init(80).ok());
            EXPECT_EQ(wrapper->get_state(), RuntimeFilterWrapper::State::UNINITED);
            wrapper->check_state({RuntimeFilterWrapper::State::UNINITED});
            EXPECT_EQ(*(int*)wrapper->minmax_func()->get_max(), type_limit<int>::min());
            EXPECT_EQ(*(int*)wrapper->minmax_func()->get_min(), type_limit<int>::max());
        }
        {
            // Insert
            auto col = ColumnHelper::create_column<DataType>(data_vector);
            EXPECT_TRUE(wrapper->insert(col, 0).ok());
            EXPECT_EQ(wrapper->get_state(), RuntimeFilterWrapper::State::UNINITED);
            wrapper->check_state({RuntimeFilterWrapper::State::UNINITED});
            EXPECT_EQ(*(int*)wrapper->minmax_func()->get_max(), min_val + num_vals - 1);
            EXPECT_EQ(*(int*)wrapper->minmax_func()->get_min(), min_val);
        }
        {
            PMergeFilterRequest valid_request;
            valid_request.set_contain_null(false);
            valid_request.set_filter_type(PFilterType::MAX_FILTER);
            valid_request.set_filter_id(filter_id);
            EXPECT_TRUE(wrapper->to_protobuf(valid_request.mutable_minmax_filter()).ok());

            auto new_wrapper = std::make_shared<RuntimeFilterWrapper>(&params);
            EXPECT_TRUE(new_wrapper->assign(valid_request, nullptr).ok());

            EXPECT_EQ(*(int*)wrapper->minmax_func()->get_max(),
                      *(int*)new_wrapper->minmax_func()->get_max());
        }
        {
            auto new_wrapper = std::make_shared<RuntimeFilterWrapper>(&params);
            EXPECT_TRUE(new_wrapper->init(12312).ok());
            EXPECT_EQ(new_wrapper->get_state(), RuntimeFilterWrapper::State::UNINITED);
            wrapper->check_state({RuntimeFilterWrapper::State::UNINITED});
            // Insert
            std::vector<int> new_data_vector {-100, 100};
            auto col = ColumnHelper::create_column<DataType>(new_data_vector);
            EXPECT_TRUE(new_wrapper->insert(col, 0).ok());
            EXPECT_EQ(new_wrapper->get_state(), RuntimeFilterWrapper::State::UNINITED);
            wrapper->check_state({RuntimeFilterWrapper::State::UNINITED});
            new_wrapper->_state = RuntimeFilterWrapper::State::READY;
            // Merge
            EXPECT_TRUE(wrapper->merge(new_wrapper.get()).ok());
            EXPECT_EQ(*(int*)wrapper->minmax_func()->get_max(), 100);
            EXPECT_EQ(*(int*)wrapper->minmax_func()->get_min(), -100);
        }
        EXPECT_EQ(wrapper->filter_id(), filter_id);
        EXPECT_TRUE(wrapper->is_valid());
        EXPECT_EQ(wrapper->get_real_type(), RuntimeFilterType::MAX_FILTER);
        EXPECT_EQ(wrapper->column_type(), column_return_type);
        EXPECT_EQ(wrapper->contain_null(), false);
    }
}

TEST_F(RuntimeFilterWrapperTest, TestInOrBloomLazyInitialization) {
    for (bool build_by_runtime_size : {false, true}) {
        RuntimeFilterParams params {.filter_id = 0,
                                    .filter_type = RuntimeFilterType::IN_OR_BLOOM_FILTER,
                                    .column_return_type = TYPE_INT,
                                    .null_aware = false,
                                    .max_in_num = 16,
                                    .runtime_bloom_filter_min_size = 64,
                                    .runtime_bloom_filter_max_size = 128,
                                    .bloom_filter_size = 64,
                                    .build_bf_by_runtime_size = build_by_runtime_size,
                                    .bloom_filter_size_calculated_by_ndv = false};
        for (size_t runtime_size : {0, 1, 16, 17}) {
            SCOPED_TRACE(runtime_size);
            SCOPED_TRACE(build_by_runtime_size);
            RuntimeFilterWrapper wrapper(&params);
            ASSERT_NE(wrapper.bloom_filter_func(), nullptr);
            ASSERT_EQ(wrapper.bloom_filter_func()->get_size(), 0);
            ASSERT_TRUE(wrapper.init(runtime_size).ok());
            if (runtime_size <= static_cast<size_t>(params.max_in_num)) {
                EXPECT_EQ(wrapper.get_real_type(), RuntimeFilterType::IN_FILTER);
                EXPECT_EQ(wrapper.bloom_filter_func()->get_size(), 0);
                EXPECT_EQ(wrapper.bloom_filter_func()->_bloom_filter_alloced, 0);
                EXPECT_EQ(wrapper.bloom_filter_func()->_bloom_filter, nullptr);
                PMergeFilterRequest request;
                ASSERT_TRUE(wrapper.to_protobuf(request.mutable_in_filter()).ok());
            } else {
                EXPECT_EQ(wrapper.get_real_type(), RuntimeFilterType::BLOOM_FILTER);
                EXPECT_GT(wrapper.bloom_filter_func()->get_size(), 0);
                auto col = ColumnHelper::create_column<DataTypeInt32>({1, 2, 3});
                ASSERT_TRUE(wrapper.insert(col, 0).ok());
                std::vector<uint8_t> matches(3);
                wrapper.bloom_filter_func()->find_fixed_len(col, matches.data());
                EXPECT_TRUE(std::all_of(matches.begin(), matches.end(),
                                        [](uint8_t match) { return match != 0; }));
            }
        }
    }
}

TEST_F(RuntimeFilterWrapperTest, TestInOrBloom) {
    std::vector<int> data_vector(10);
    std::iota(data_vector.begin(), data_vector.end(), 0);
    using DataType = DataTypeInt32;
    int32_t filter_id = 0;
    auto runtime_size = 80;
    RuntimeFilterType filter_type = RuntimeFilterType::IN_OR_BLOOM_FILTER;
    bool null_aware = false;
    PrimitiveType column_return_type = PrimitiveType::TYPE_INT;

    int32_t max_in_num = 64;

    int64_t runtime_bloom_filter_min_size = 64;
    int64_t runtime_bloom_filter_max_size = 128;
    bool build_bf_by_runtime_size = false;
    int64_t bloom_filter_size = 64;
    bool bloom_filter_size_calculated_by_ndv = false;

    std::shared_ptr<RuntimeFilterWrapper> wrapper;
    {
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
                .bloom_filter_size_calculated_by_ndv = bloom_filter_size_calculated_by_ndv};
        wrapper = std::make_shared<RuntimeFilterWrapper>(&params);
        EXPECT_EQ(wrapper->bloom_filter_func()->_bloom_filter_length, bloom_filter_size);
        EXPECT_EQ(wrapper->get_real_type(), RuntimeFilterType::IN_FILTER);
        // Init (change to bloom filter)
        EXPECT_TRUE(wrapper->init(runtime_size).ok());
        EXPECT_EQ(wrapper->get_real_type(), RuntimeFilterType::BLOOM_FILTER);
        EXPECT_EQ(wrapper->get_state(), RuntimeFilterWrapper::State::UNINITED);
        EXPECT_EQ(wrapper->bloom_filter_func()->_bloom_filter_alloced, bloom_filter_size);
        // Insert
        auto col = ColumnHelper::create_column<DataType>(data_vector);
        EXPECT_TRUE(wrapper->insert(col, 0).ok());
        EXPECT_EQ(wrapper->get_state(), RuntimeFilterWrapper::State::UNINITED);

        col = ColumnHelper::create_column<DataType>(data_vector);
        std::vector<uint8_t> res(10);
        wrapper->bloom_filter_func()->find_fixed_len(col, res.data());
        EXPECT_TRUE(std::all_of(res.begin(), res.end(), [](uint8_t i) -> bool { return i; }));
    }
    {
        runtime_size = 32;
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
                .bloom_filter_size_calculated_by_ndv = bloom_filter_size_calculated_by_ndv};
        wrapper = std::make_shared<RuntimeFilterWrapper>(&params);
        EXPECT_EQ(wrapper->bloom_filter_func()->_bloom_filter_length, bloom_filter_size);
        EXPECT_EQ(wrapper->get_real_type(), RuntimeFilterType::IN_FILTER);
        // Init (keep in filter)
        EXPECT_TRUE(wrapper->init(runtime_size).ok());
        EXPECT_EQ(wrapper->get_real_type(), RuntimeFilterType::IN_FILTER);
        EXPECT_EQ(wrapper->bloom_filter_func()->get_size(), 0);
        EXPECT_EQ(wrapper->get_state(), RuntimeFilterWrapper::State::UNINITED);
        // Insert
        auto col = ColumnHelper::create_column<DataType>(data_vector);
        EXPECT_TRUE(wrapper->insert(col, 0).ok());
        EXPECT_EQ(wrapper->get_state(), RuntimeFilterWrapper::State::UNINITED);
        EXPECT_EQ(wrapper->hybrid_set()->size(), col->size());
    }
    {
        // In + In -> In
        max_in_num = 128;
        runtime_size = 16;
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
                .bloom_filter_size_calculated_by_ndv = bloom_filter_size_calculated_by_ndv};
        wrapper = std::make_shared<RuntimeFilterWrapper>(&params);
        EXPECT_EQ(wrapper->get_real_type(), RuntimeFilterType::IN_FILTER);
        EXPECT_TRUE(wrapper->init(runtime_size).ok());
        EXPECT_EQ(wrapper->get_real_type(), RuntimeFilterType::IN_FILTER);
        RuntimeFilterParams new_params {
                .filter_id = filter_id,
                .filter_type = filter_type,
                .column_return_type = column_return_type,
                .null_aware = null_aware,
                .max_in_num = max_in_num,
                .runtime_bloom_filter_min_size = runtime_bloom_filter_min_size,
                .runtime_bloom_filter_max_size = runtime_bloom_filter_max_size,
                .bloom_filter_size = bloom_filter_size,
                .build_bf_by_runtime_size = build_bf_by_runtime_size,
                .bloom_filter_size_calculated_by_ndv = bloom_filter_size_calculated_by_ndv};
        auto new_wrapper = std::make_shared<RuntimeFilterWrapper>(&new_params);
        EXPECT_EQ(new_wrapper->get_real_type(), RuntimeFilterType::IN_FILTER);
        EXPECT_TRUE(new_wrapper->init(runtime_size).ok());
        EXPECT_EQ(new_wrapper->get_real_type(), RuntimeFilterType::IN_FILTER);
        // Insert
        auto col = ColumnHelper::create_column<DataType>(data_vector);
        EXPECT_TRUE(wrapper->insert(col, 0).ok());
        EXPECT_EQ(wrapper->get_state(), RuntimeFilterWrapper::State::UNINITED);
        EXPECT_EQ(wrapper->hybrid_set()->size(), col->size());

        std::vector<int> new_data_vector(10);
        std::iota(new_data_vector.begin(), new_data_vector.end(), 10);
        col = ColumnHelper::create_column<DataType>(new_data_vector);
        EXPECT_TRUE(new_wrapper->insert(col, 0).ok());
        EXPECT_EQ(new_wrapper->get_state(), RuntimeFilterWrapper::State::UNINITED);
        EXPECT_EQ(new_wrapper->hybrid_set()->size(), col->size());
        new_wrapper->_state = RuntimeFilterWrapper::State::READY;
        // Merge
        EXPECT_TRUE(wrapper->merge(new_wrapper.get()).ok());
        EXPECT_EQ(wrapper->hybrid_set()->size(), col->size() * 2);
        EXPECT_EQ(wrapper->get_real_type(), RuntimeFilterType::IN_FILTER);
        EXPECT_EQ(wrapper->bloom_filter_func()->get_size(), 0);
    }
    {
        // In + In -> Bloom
        max_in_num = 16;
        runtime_size = 10;
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
                .bloom_filter_size_calculated_by_ndv = bloom_filter_size_calculated_by_ndv};
        wrapper = std::make_shared<RuntimeFilterWrapper>(&params);
        EXPECT_EQ(wrapper->get_real_type(), RuntimeFilterType::IN_FILTER);
        EXPECT_TRUE(wrapper->init(runtime_size).ok());
        EXPECT_EQ(wrapper->get_real_type(), RuntimeFilterType::IN_FILTER);
        RuntimeFilterParams new_params {
                .filter_id = filter_id,
                .filter_type = filter_type,
                .column_return_type = column_return_type,
                .null_aware = null_aware,
                .max_in_num = max_in_num,
                .runtime_bloom_filter_min_size = runtime_bloom_filter_min_size,
                .runtime_bloom_filter_max_size = runtime_bloom_filter_max_size,
                .bloom_filter_size = bloom_filter_size,
                .build_bf_by_runtime_size = build_bf_by_runtime_size,
                .bloom_filter_size_calculated_by_ndv = bloom_filter_size_calculated_by_ndv};
        auto new_wrapper = std::make_shared<RuntimeFilterWrapper>(&new_params);
        EXPECT_EQ(new_wrapper->get_real_type(), RuntimeFilterType::IN_FILTER);
        EXPECT_TRUE(new_wrapper->init(runtime_size).ok());
        EXPECT_EQ(new_wrapper->get_real_type(), RuntimeFilterType::IN_FILTER);
        // Insert
        auto col = ColumnHelper::create_column<DataType>(data_vector);
        EXPECT_TRUE(wrapper->insert(col, 0).ok());
        EXPECT_EQ(wrapper->get_state(), RuntimeFilterWrapper::State::UNINITED);
        EXPECT_EQ(wrapper->hybrid_set()->size(), col->size());

        std::vector<int> new_data_vector(10);
        std::iota(new_data_vector.begin(), new_data_vector.end(), 10);
        col = ColumnHelper::create_column<DataType>(new_data_vector);
        EXPECT_TRUE(new_wrapper->insert(col, 0).ok());
        EXPECT_EQ(new_wrapper->get_state(), RuntimeFilterWrapper::State::UNINITED);
        EXPECT_EQ(new_wrapper->hybrid_set()->size(), col->size());
        new_wrapper->_state = RuntimeFilterWrapper::State::READY;
        // Merge
        EXPECT_TRUE(wrapper->merge(new_wrapper.get()).ok());

        std::vector<int> final_data_vector(20);
        std::iota(final_data_vector.begin(), final_data_vector.end(), 0);
        col = ColumnHelper::create_column<DataType>(final_data_vector);
        std::vector<uint8_t> final_res(20);
        wrapper->bloom_filter_func()->find_fixed_len(col, final_res.data());
        EXPECT_TRUE(std::all_of(final_res.begin(), final_res.end(),
                                [](uint8_t i) -> bool { return i; }));
        EXPECT_EQ(wrapper->get_real_type(), RuntimeFilterType::BLOOM_FILTER);
    }
    {
        // In + Bloom -> Bloom
        max_in_num = 18;
        runtime_size = 16;
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
                .bloom_filter_size_calculated_by_ndv = bloom_filter_size_calculated_by_ndv};
        wrapper = std::make_shared<RuntimeFilterWrapper>(&params);
        EXPECT_EQ(wrapper->get_real_type(), RuntimeFilterType::IN_FILTER);
        EXPECT_TRUE(wrapper->init(runtime_size).ok());
        EXPECT_EQ(wrapper->get_real_type(), RuntimeFilterType::IN_FILTER);

        max_in_num = 8;
        runtime_size = 16;
        RuntimeFilterParams new_params {
                .filter_id = filter_id,
                .filter_type = filter_type,
                .column_return_type = column_return_type,
                .null_aware = null_aware,
                .max_in_num = max_in_num,
                .runtime_bloom_filter_min_size = runtime_bloom_filter_min_size,
                .runtime_bloom_filter_max_size = runtime_bloom_filter_max_size,
                .bloom_filter_size = bloom_filter_size,
                .build_bf_by_runtime_size = build_bf_by_runtime_size,
                .bloom_filter_size_calculated_by_ndv = bloom_filter_size_calculated_by_ndv};
        auto new_wrapper = std::make_shared<RuntimeFilterWrapper>(&new_params);
        EXPECT_EQ(new_wrapper->get_real_type(), RuntimeFilterType::IN_FILTER);
        EXPECT_TRUE(new_wrapper->init(runtime_size).ok());
        EXPECT_EQ(new_wrapper->get_real_type(), RuntimeFilterType::BLOOM_FILTER);
        // Insert
        auto col = ColumnHelper::create_column<DataType>(data_vector);
        EXPECT_TRUE(wrapper->insert(col, 0).ok());
        EXPECT_EQ(wrapper->get_state(), RuntimeFilterWrapper::State::UNINITED);
        EXPECT_EQ(wrapper->hybrid_set()->size(), data_vector.size());

        std::vector<int> new_data_vector(10);
        std::iota(new_data_vector.begin(), new_data_vector.end(), 10);
        col = ColumnHelper::create_column<DataType>(new_data_vector);
        EXPECT_TRUE(new_wrapper->insert(col, 0).ok());
        EXPECT_EQ(new_wrapper->get_state(), RuntimeFilterWrapper::State::UNINITED);
        std::vector<uint8_t> res(10);
        new_wrapper->bloom_filter_func()->find_fixed_len(col, res.data());
        EXPECT_TRUE(std::all_of(res.begin(), res.end(), [](uint8_t i) -> bool { return i; }));
        new_wrapper->_state = RuntimeFilterWrapper::State::READY;
        // Merge
        EXPECT_TRUE(wrapper->merge(new_wrapper.get()).ok());

        std::vector<int> final_data_vector(20);
        std::iota(final_data_vector.begin(), final_data_vector.end(), 0);
        col = ColumnHelper::create_column<DataType>(final_data_vector);
        std::vector<uint8_t> final_res(20);
        wrapper->bloom_filter_func()->find_fixed_len(col, final_res.data());
        EXPECT_TRUE(std::all_of(final_res.begin(), final_res.end(),
                                [](uint8_t i) -> bool { return i; }));
        EXPECT_EQ(wrapper->get_real_type(), RuntimeFilterType::BLOOM_FILTER);
    }
    {
        // Bloom + In -> Bloom
        max_in_num = 8;
        runtime_size = 16;
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
                .bloom_filter_size_calculated_by_ndv = bloom_filter_size_calculated_by_ndv};
        wrapper = std::make_shared<RuntimeFilterWrapper>(&params);
        EXPECT_EQ(wrapper->get_real_type(), RuntimeFilterType::IN_FILTER);
        EXPECT_TRUE(wrapper->init(runtime_size).ok());
        EXPECT_EQ(wrapper->get_real_type(), RuntimeFilterType::BLOOM_FILTER);

        max_in_num = 32;
        runtime_size = 16;
        RuntimeFilterParams new_params {
                .filter_id = filter_id,
                .filter_type = filter_type,
                .column_return_type = column_return_type,
                .null_aware = null_aware,
                .max_in_num = max_in_num,
                .runtime_bloom_filter_min_size = runtime_bloom_filter_min_size,
                .runtime_bloom_filter_max_size = runtime_bloom_filter_max_size,
                .bloom_filter_size = bloom_filter_size,
                .build_bf_by_runtime_size = build_bf_by_runtime_size,
                .bloom_filter_size_calculated_by_ndv = bloom_filter_size_calculated_by_ndv};
        auto new_wrapper = std::make_shared<RuntimeFilterWrapper>(&new_params);
        EXPECT_EQ(new_wrapper->get_real_type(), RuntimeFilterType::IN_FILTER);
        EXPECT_TRUE(new_wrapper->init(runtime_size).ok());
        EXPECT_EQ(new_wrapper->get_real_type(), RuntimeFilterType::IN_FILTER);
        // Insert
        auto col = ColumnHelper::create_column<DataType>(data_vector);
        EXPECT_TRUE(wrapper->insert(col, 0).ok());
        EXPECT_EQ(wrapper->get_state(), RuntimeFilterWrapper::State::UNINITED);
        col = ColumnHelper::create_column<DataType>(data_vector);
        std::vector<uint8_t> res(10);
        wrapper->bloom_filter_func()->find_fixed_len(col, res.data());
        EXPECT_TRUE(std::all_of(res.begin(), res.end(), [](uint8_t i) -> bool { return i; }));

        std::vector<int> new_data_vector(10);
        std::iota(new_data_vector.begin(), new_data_vector.end(), 10);
        col = ColumnHelper::create_column<DataType>(new_data_vector);
        EXPECT_TRUE(new_wrapper->insert(col, 0).ok());
        EXPECT_EQ(new_wrapper->get_state(), RuntimeFilterWrapper::State::UNINITED);
        EXPECT_EQ(new_wrapper->hybrid_set()->size(), col->size());
        new_wrapper->_state = RuntimeFilterWrapper::State::READY;
        // Merge
        EXPECT_TRUE(wrapper->merge(new_wrapper.get()).ok());

        std::vector<int> final_data_vector(20);
        std::iota(final_data_vector.begin(), final_data_vector.end(), 0);
        col = ColumnHelper::create_column<DataType>(final_data_vector);
        std::vector<uint8_t> final_res(20);
        wrapper->bloom_filter_func()->find_fixed_len(col, final_res.data());
        EXPECT_TRUE(std::all_of(final_res.begin(), final_res.end(),
                                [](uint8_t i) -> bool { return i; }));
        EXPECT_EQ(wrapper->get_real_type(), RuntimeFilterType::BLOOM_FILTER);
    }
    {
        // Bloom + Bloom -> Bloom
        max_in_num = 8;
        runtime_size = 16;
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
                .bloom_filter_size_calculated_by_ndv = bloom_filter_size_calculated_by_ndv};
        wrapper = std::make_shared<RuntimeFilterWrapper>(&params);
        EXPECT_EQ(wrapper->get_real_type(), RuntimeFilterType::IN_FILTER);
        EXPECT_TRUE(wrapper->init(runtime_size).ok());
        EXPECT_EQ(wrapper->get_real_type(), RuntimeFilterType::BLOOM_FILTER);

        RuntimeFilterParams new_params {
                .filter_id = filter_id,
                .filter_type = filter_type,
                .column_return_type = column_return_type,
                .null_aware = null_aware,
                .max_in_num = max_in_num,
                .runtime_bloom_filter_min_size = runtime_bloom_filter_min_size,
                .runtime_bloom_filter_max_size = runtime_bloom_filter_max_size,
                .bloom_filter_size = bloom_filter_size,
                .build_bf_by_runtime_size = build_bf_by_runtime_size,
                .bloom_filter_size_calculated_by_ndv = bloom_filter_size_calculated_by_ndv};
        auto new_wrapper = std::make_shared<RuntimeFilterWrapper>(&new_params);
        EXPECT_EQ(new_wrapper->get_real_type(), RuntimeFilterType::IN_FILTER);
        EXPECT_TRUE(new_wrapper->init(runtime_size).ok());
        EXPECT_EQ(new_wrapper->get_real_type(), RuntimeFilterType::BLOOM_FILTER);
        // Insert
        auto col = ColumnHelper::create_column<DataType>(data_vector);
        EXPECT_TRUE(wrapper->insert(col, 0).ok());
        EXPECT_EQ(wrapper->get_state(), RuntimeFilterWrapper::State::UNINITED);
        col = ColumnHelper::create_column<DataType>(data_vector);
        std::vector<uint8_t> res(10);
        wrapper->bloom_filter_func()->find_fixed_len(col, res.data());
        EXPECT_TRUE(std::all_of(res.begin(), res.end(), [](uint8_t i) -> bool { return i; }));

        std::vector<int> new_data_vector(10);
        std::iota(new_data_vector.begin(), new_data_vector.end(), 10);
        col = ColumnHelper::create_column<DataType>(new_data_vector);
        EXPECT_TRUE(new_wrapper->insert(col, 0).ok());
        EXPECT_EQ(new_wrapper->get_state(), RuntimeFilterWrapper::State::UNINITED);
        col = ColumnHelper::create_column<DataType>(new_data_vector);
        new_wrapper->bloom_filter_func()->find_fixed_len(col, res.data());
        EXPECT_TRUE(std::all_of(res.begin(), res.end(), [](uint8_t i) -> bool { return i; }));
        new_wrapper->_state = RuntimeFilterWrapper::State::READY;
        // Merge
        EXPECT_TRUE(wrapper->merge(new_wrapper.get()).ok());

        std::vector<int> final_data_vector(20);
        std::iota(final_data_vector.begin(), final_data_vector.end(), 0);
        col = ColumnHelper::create_column<DataType>(final_data_vector);
        std::vector<uint8_t> final_res(20);
        wrapper->bloom_filter_func()->find_fixed_len(col, final_res.data());
        EXPECT_TRUE(std::all_of(final_res.begin(), final_res.end(),
                                [](uint8_t i) -> bool { return i; }));
        EXPECT_EQ(wrapper->get_real_type(), RuntimeFilterType::BLOOM_FILTER);
    }
    EXPECT_EQ(wrapper->filter_id(), filter_id);
    EXPECT_TRUE(wrapper->is_valid());
    EXPECT_EQ(wrapper->column_type(), column_return_type);
    EXPECT_EQ(wrapper->contain_null(), false);
}

TEST_F(RuntimeFilterWrapperTest, TestErrorPath) {
    using DataType = DataTypeInt32;
    int32_t filter_id = 0;
    RuntimeFilterType filter_type = RuntimeFilterType::UNKNOWN_FILTER;
    bool null_aware = false;
    PrimitiveType column_return_type = PrimitiveType::TYPE_INT;

    int32_t max_in_num = 64;

    int64_t runtime_bloom_filter_min_size = 64;
    int64_t runtime_bloom_filter_max_size = 128;
    bool build_bf_by_runtime_size = false;
    int64_t bloom_filter_size = 64;
    bool bloom_filter_size_calculated_by_ndv = false;
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
            .bloom_filter_size_calculated_by_ndv = bloom_filter_size_calculated_by_ndv};
    std::shared_ptr<RuntimeFilterWrapper> wrapper = std::make_shared<RuntimeFilterWrapper>(&params);
    auto col = ColumnHelper::create_column<DataType>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
    EXPECT_EQ(wrapper->insert(col, 0).code(), ErrorCode::INTERNAL_ERROR);
    {
        std::shared_ptr<RuntimeFilterWrapper> another_wrapper =
                std::make_shared<RuntimeFilterWrapper>(&params);
        another_wrapper->_state = RuntimeFilterWrapper::State::READY;
        EXPECT_EQ(wrapper->merge(another_wrapper.get()).code(), ErrorCode::INTERNAL_ERROR);
    }
    PMergeFilterRequest valid_request;
    EXPECT_FALSE(wrapper->to_protobuf(valid_request.mutable_in_filter()).ok());
    EXPECT_FALSE(wrapper->to_protobuf(valid_request.mutable_minmax_filter()).ok());
    EXPECT_FALSE(wrapper->to_protobuf(valid_request.mutable_bloom_filter(), nullptr, nullptr).ok());
    EXPECT_EQ(wrapper->minmax_func(), nullptr);
    EXPECT_EQ(wrapper->bloom_filter_func(), nullptr);
    EXPECT_EQ(wrapper->hybrid_set(), nullptr);
    wrapper->check_state({RuntimeFilterWrapper::State::UNINITED});
    bool ex = false;
    try {
        wrapper->check_state(
                {RuntimeFilterWrapper::State::READY, RuntimeFilterWrapper::State::DISABLED});
    } catch (std::exception) {
        ex = true;
    }
    EXPECT_TRUE(ex);
}

} // namespace doris
