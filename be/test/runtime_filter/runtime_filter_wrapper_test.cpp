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

#include "exprs/bloom_filter_func.h"
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
    EXPECT_FALSE(wrapper->build_bf_by_runtime_size());
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
        in_filter->set_column_type(PColumnType::COLUMN_TYPE_BOOL);                                 \
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

TEST_F(RuntimeFilterWrapperTest, TestBloom) {
    std::vector<int> data_vector(10);
    std::iota(data_vector.begin(), data_vector.end(), 0);
    using DataType = vectorized::DataTypeInt32;
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
    bool enable_fixed_len_to_uint32_v2 = true;

    bool bitmap_filter_not_in = false;

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
                .bloom_filter_size_calculated_by_ndv = bloom_filter_size_calculated_by_ndv,
                .enable_fixed_len_to_uint32_v2 = enable_fixed_len_to_uint32_v2,
                .bitmap_filter_not_in = bitmap_filter_not_in};
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
                .bloom_filter_size_calculated_by_ndv = bloom_filter_size_calculated_by_ndv,
                .enable_fixed_len_to_uint32_v2 = enable_fixed_len_to_uint32_v2,
                .bitmap_filter_not_in = bitmap_filter_not_in};
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
                .bloom_filter_size_calculated_by_ndv = bloom_filter_size_calculated_by_ndv,
                .enable_fixed_len_to_uint32_v2 = enable_fixed_len_to_uint32_v2,
                .bitmap_filter_not_in = bitmap_filter_not_in};
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
                .bloom_filter_size_calculated_by_ndv = bloom_filter_size_calculated_by_ndv,
                .enable_fixed_len_to_uint32_v2 = enable_fixed_len_to_uint32_v2,
                .bitmap_filter_not_in = bitmap_filter_not_in};
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
        auto col = vectorized::ColumnHelper::create_column<DataType>(data_vector);
        EXPECT_TRUE(wrapper->insert(col, 0).ok());
        EXPECT_EQ(wrapper->get_state(), RuntimeFilterWrapper::State::UNINITED);

        col = vectorized::ColumnHelper::create_column<DataType>(data_vector);
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
        wrapper->to_protobuf(valid_request.mutable_bloom_filter(), &data, &len);

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
                .bloom_filter_size_calculated_by_ndv = bloom_filter_size_calculated_by_ndv,
                .enable_fixed_len_to_uint32_v2 = enable_fixed_len_to_uint32_v2,
                .bitmap_filter_not_in = bitmap_filter_not_in};
        auto new_wrapper = std::make_shared<RuntimeFilterWrapper>(&new_params);
        EXPECT_TRUE(new_wrapper->assign(valid_request, &stream).ok());

        auto col = vectorized::ColumnHelper::create_column<DataType>(data_vector);
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
                .bloom_filter_size_calculated_by_ndv = bloom_filter_size_calculated_by_ndv,
                .enable_fixed_len_to_uint32_v2 = enable_fixed_len_to_uint32_v2,
                .bitmap_filter_not_in = bitmap_filter_not_in};
        auto new_wrapper = std::make_shared<RuntimeFilterWrapper>(&new_params);
        EXPECT_TRUE(new_wrapper->init(runtime_size).ok());
        EXPECT_EQ(new_wrapper->get_state(), RuntimeFilterWrapper::State::UNINITED);
        EXPECT_EQ(new_wrapper->bloom_filter_func()->_bloom_filter_length,
                  wrapper->bloom_filter_func()->_bloom_filter_length);
        // Insert
        std::vector<int> new_data_vector(10);
        std::iota(new_data_vector.begin(), new_data_vector.end(), 10);
        auto col = vectorized::ColumnHelper::create_column<DataType>(new_data_vector);
        EXPECT_TRUE(new_wrapper->insert(col, 0).ok());
        EXPECT_EQ(new_wrapper->get_state(), RuntimeFilterWrapper::State::UNINITED);
        new_wrapper->_state = RuntimeFilterWrapper::State::READY;
        // Merge
        std::vector<int> res_data_vector(20);
        std::iota(res_data_vector.begin(), res_data_vector.end(), 0);
        EXPECT_TRUE(wrapper->merge(new_wrapper.get()).ok());
        col = vectorized::ColumnHelper::create_column<DataType>(res_data_vector);
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

} // namespace doris
