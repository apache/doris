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

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/Types_types.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <thrift/protocol/TDebugProtocol.h>
#include <thrift/protocol/TJSONProtocol.h>

#include <iostream>
#include <memory>

#include "olap/rowset/segment_v2/ann_index/ann_search_params.h"
#include "olap/rowset/segment_v2/ann_index/ann_topn_runtime.h"
#include "runtime/primitive_type.h"
#include "vec/columns/column_nullable.h"
#include "vec/exprs/virtual_slot_ref.h"
#include "vector_search_utils.h"

using ::testing::_;
using ::testing::SetArgPointee;
using ::testing::Return;

namespace doris::vectorized {

TEST_F(VectorSearchTest, AnnTopNRuntimeConstructor) {
    int limit = 10;
    std::shared_ptr<VExprContext> distanc_calcu_fn_call_ctx;
    auto distance_function_call_thrift = read_from_json<TExpr>(_distance_function_call_thrift);
    ASSERT_TRUE(distance_function_call_thrift.nodes.empty() != true);
    auto st1 = vectorized::VExpr::create_expr_tree(distance_function_call_thrift,
                                                   distanc_calcu_fn_call_ctx);
    ASSERT_TRUE(st1.ok()) << fmt::format(
            "st: {}, expr {}", st1.to_string(),
            apache::thrift::ThriftDebugString(distance_function_call_thrift));
    ASSERT_TRUE(distanc_calcu_fn_call_ctx != nullptr) << "create expr tree failed";
    ASSERT_TRUE(distanc_calcu_fn_call_ctx->root() != nullptr);

    std::shared_ptr<VExprContext> virtual_slot_expr_ctx;
    ASSERT_TRUE(vectorized::VExpr::create_expr_tree(_virtual_slot_ref_expr, virtual_slot_expr_ctx)
                        .ok());

    ASSERT_TRUE(virtual_slot_expr_ctx != nullptr) << "create expr tree failed";
    ASSERT_TRUE(virtual_slot_expr_ctx->root() != nullptr);

    std::shared_ptr<VirtualSlotRef> v =
            std::dynamic_pointer_cast<VirtualSlotRef>(virtual_slot_expr_ctx->root());
    if (v == nullptr) {
        LOG(FATAL) << "VAnnTopNRuntime::SetUp() failed";
    }

    v->set_virtual_column_expr(distanc_calcu_fn_call_ctx->root());

    std::shared_ptr<segment_v2::AnnTopNRuntime> predicate;
    predicate = segment_v2::AnnTopNRuntime::create_shared(true, limit, virtual_slot_expr_ctx);
    ASSERT_TRUE(predicate != nullptr) << "AnnTopNRuntime::create_shared(true,) failed";
}

TEST_F(VectorSearchTest, AnnTopNRuntimePrepare) {
    int limit = 10;
    std::shared_ptr<VExprContext> distanc_calcu_fn_call_ctx;
    auto distance_function_call_thrift = read_from_json<TExpr>(_distance_function_call_thrift);
    Status st = vectorized::VExpr::create_expr_tree(distance_function_call_thrift,
                                                    distanc_calcu_fn_call_ctx);

    std::shared_ptr<VExprContext> virtual_slot_expr_ctx;
    st = vectorized::VExpr::create_expr_tree(_virtual_slot_ref_expr, virtual_slot_expr_ctx);
    std::shared_ptr<VirtualSlotRef> v =
            std::dynamic_pointer_cast<VirtualSlotRef>(virtual_slot_expr_ctx->root());
    if (v == nullptr) {
        LOG(FATAL) << "VAnnTopNRuntime::SetUp() failed";
    }

    v->set_virtual_column_expr(distanc_calcu_fn_call_ctx->root());
    std::shared_ptr<segment_v2::AnnTopNRuntime> predicate;
    predicate = segment_v2::AnnTopNRuntime::create_shared(true, limit, virtual_slot_expr_ctx);
    st = predicate->prepare(&_runtime_state, _row_desc);
    ASSERT_TRUE(st.ok()) << fmt::format("st: {}, expr {}", st.to_string(),
                                        predicate->get_order_by_expr_ctx()->root()->debug_string());

    std::cout << "predicate: " << predicate->debug_string() << std::endl;
}

TEST_F(VectorSearchTest, AnnTopNRuntimeEvaluateTopN) {
    int limit = 10;
    std::shared_ptr<VExprContext> distanc_calcu_fn_call_ctx;
    auto distance_function_call_thrift = read_from_json<TExpr>(_distance_function_call_thrift);
    Status st = vectorized::VExpr::create_expr_tree(distance_function_call_thrift,
                                                    distanc_calcu_fn_call_ctx);

    std::shared_ptr<VExprContext> virtual_slot_expr_ctx;
    st = vectorized::VExpr::create_expr_tree(_virtual_slot_ref_expr, virtual_slot_expr_ctx);
    std::shared_ptr<VirtualSlotRef> v =
            std::dynamic_pointer_cast<VirtualSlotRef>(virtual_slot_expr_ctx->root());
    if (v == nullptr) {
        LOG(FATAL) << "VAnnTopNRuntime::SetUp() failed";
    }

    v->set_virtual_column_expr(distanc_calcu_fn_call_ctx->root());
    std::shared_ptr<segment_v2::AnnTopNRuntime> predicate;
    predicate = segment_v2::AnnTopNRuntime::create_shared(true, limit, virtual_slot_expr_ctx);
    st = predicate->prepare(&_runtime_state, _row_desc);
    ASSERT_TRUE(st.ok()) << fmt::format("st: {}, expr {}", st.to_string(),
                                        predicate->get_order_by_expr_ctx()->root()->debug_string());

    const ColumnConst* const_column =
            assert_cast<const ColumnConst*>(predicate->_query_array.get());
    const ColumnArray* column_array =
            assert_cast<const ColumnArray*>(const_column->get_data_column_ptr().get());
    const ColumnNullable* column_nullable =
            assert_cast<const ColumnNullable*>(column_array->get_data_ptr().get());
    const ColumnFloat64* cf64 =
            assert_cast<const ColumnFloat64*>(column_nullable->get_nested_column_ptr().get());

    const double* query_value = cf64->get_data().data();
    const size_t query_value_size = cf64->get_data().size();
    ASSERT_EQ(query_value_size, 8);
    std::vector<float> query_value_f32;
    for (size_t i = 0; i < query_value_size; ++i) {
        query_value_f32.push_back(static_cast<float>(query_value[i]));
    }
    ASSERT_FLOAT_EQ(query_value_f32[0], 1.0f) << "query_value_f32[0] = " << query_value_f32[0];
    ASSERT_FLOAT_EQ(query_value_f32[1], 2.0f) << "query_value_f32[1] = " << query_value_f32[1];
    ASSERT_FLOAT_EQ(query_value_f32[2], 3.0f) << "query_value_f32[2] = " << query_value_f32[2];
    ASSERT_FLOAT_EQ(query_value_f32[3], 4.0f) << "query_value_f32[3] = " << query_value_f32[3];
    ASSERT_FLOAT_EQ(query_value_f32[4], 5.0f) << "query_value_f32[4] = " << query_value_f32[4];
    ASSERT_FLOAT_EQ(query_value_f32[5], 6.0f) << "query_value_f32[5] = " << query_value_f32[5];
    ASSERT_FLOAT_EQ(query_value_f32[6], 7.0f) << "query_value_f32[6] = " << query_value_f32[6];
    ASSERT_FLOAT_EQ(query_value_f32[7], 20.0f) << "query_value_f32[7] = " << query_value_f32[7];

    std::shared_ptr<std::vector<float>> query_vector =
            std::make_shared<std::vector<float>>(10, 0.0);
    for (size_t i = 0; i < 10; ++i) {
        (*query_vector)[i] = static_cast<float>(i);
    }

    std::cout << "query_vector: " << fmt::format("[{}]", fmt::join(*query_vector, ","))
              << std::endl;

    EXPECT_CALL(*_ann_index_iterator, read_from_index(testing::_))
            .Times(1)
            .WillOnce(testing::Invoke([](const segment_v2::IndexParam& value) {
                auto* ann_param = std::get<segment_v2::AnnTopNParam*>(value);
                ann_param->distance = std::make_unique<std::vector<float>>();
                ann_param->row_ids = std::make_unique<std::vector<uint64_t>>();
                for (size_t i = 0; i < 10; ++i) {
                    ann_param->distance->push_back(static_cast<float>(i));
                    ann_param->row_ids->push_back(i);
                }
                return Status::OK();
            }));

    _result_column = ColumnNullable::create(ColumnFloat64::create(0, 0), ColumnUInt8::create(0, 0));
    std::unique_ptr<std::vector<uint64_t>> row_ids = std::make_unique<std::vector<uint64_t>>();

    roaring::Roaring roaring;
    doris::segment_v2::AnnIndexStats ann_index_stats;
    // rows_of_segment is mocked as 10 to align with mocked iterator outputs
    size_t rows_of_segment = 10;
    st = predicate->evaluate_vector_ann_search(_ann_index_iterator.get(), &roaring, rows_of_segment,
                                               _result_column, row_ids, ann_index_stats);
    ColumnNullable* result_column_null = assert_cast<ColumnNullable*>(_result_column.get());
    ColumnFloat64* result_column_float =
            assert_cast<ColumnFloat64*>(result_column_null->get_nested_column_ptr().get());
    for (size_t i = 0; i < query_vector->size(); ++i) {
        EXPECT_EQ(result_column_float->get_data()[i], (*query_vector)[i]);
    }
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(row_ids->size(), 10);
}

} // namespace doris::vectorized