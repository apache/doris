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

#include "vec/exprs/vann_topn_predicate.h"
#include "vec/exprs/virtual_slot_ref.h"
#include "vector_search_utils.h"

using ::testing::_;
using ::testing::SetArgPointee;
using ::testing::Return;

namespace doris::vectorized {

TEST_F(VectorSearchTest, AnnTopNDescriptorConstructor) {
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
        LOG(FATAL) << "VAnnTopNDescriptor::SetUp() failed";
    }

    v->set_virtual_column_expr(distanc_calcu_fn_call_ctx->root());

    std::shared_ptr<AnnTopNDescriptor> predicate;
    predicate = AnnTopNDescriptor::create_shared(limit, virtual_slot_expr_ctx);
    ASSERT_TRUE(predicate != nullptr) << "AnnTopNDescriptor::create_shared() failed";
}

TEST_F(VectorSearchTest, AnnTopNDescriptorPrepare) {
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
        LOG(FATAL) << "VAnnTopNDescriptor::SetUp() failed";
    }

    v->set_virtual_column_expr(distanc_calcu_fn_call_ctx->root());
    std::shared_ptr<AnnTopNDescriptor> predicate;
    predicate = AnnTopNDescriptor::create_shared(limit, virtual_slot_expr_ctx);
    st = predicate->prepare(&_runtime_state, _row_desc);
    ASSERT_TRUE(st.ok()) << fmt::format("st: {}, expr {}", st.to_string(),
                                        predicate->get_order_by_expr_ctx()->root()->debug_string());

    std::cout << "predicate: " << predicate->debug_string() << std::endl;
}

TEST_F(VectorSearchTest, AnnTopNDescriptorEvaluateTopN) {
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
        LOG(FATAL) << "VAnnTopNDescriptor::SetUp() failed";
    }

    v->set_virtual_column_expr(distanc_calcu_fn_call_ctx->root());
    std::shared_ptr<AnnTopNDescriptor> predicate;
    predicate = AnnTopNDescriptor::create_shared(limit, virtual_slot_expr_ctx);
    st = predicate->prepare(&_runtime_state, _row_desc);
    ASSERT_TRUE(st.ok()) << fmt::format("st: {}, expr {}", st.to_string(),
                                        predicate->get_order_by_expr_ctx()->root()->debug_string());

    std::shared_ptr<std::vector<float>> query_value = std::make_shared<std::vector<float>>(10, 0.0);
    for (size_t i = 0; i < 10; ++i) {
        (*query_value)[i] = static_cast<float>(i);
    }

    EXPECT_CALL(*_ann_index_iterator, read_from_index(testing::_))
            .Times(1)
            .WillOnce(testing::Invoke([](const segment_v2::IndexParam& value) {
                auto* ann_param = std::get<segment_v2::AnnIndexParam*>(value);
                ann_param->distance = std::make_unique<std::vector<float>>();
                ann_param->row_ids = std::make_unique<std::vector<uint64_t>>();
                for (size_t i = 0; i < 10; ++i) {
                    ann_param->distance->push_back(static_cast<float>(i));
                    ann_param->row_ids->push_back(i);
                }
                return Status::OK();
            }));

    _result_column = ColumnFloat64::create(0, 0);
    std::unique_ptr<std::vector<uint64_t>> row_ids = std::make_unique<std::vector<uint64_t>>();
    // roaring is empry
    roaring::Roaring roaring;
    st = predicate->evaluate_vector_ann_search(_ann_index_iterator.get(), roaring, _result_column,
                                               row_ids);
    ColumnFloat64* result_column_float = assert_cast<ColumnFloat64*>(_result_column.get());
    for (size_t i = 0; i < query_value->size(); ++i) {
        EXPECT_EQ(result_column_float->get_data()[i], (*query_value)[i]);
    }
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(row_ids->size(), 0);
    ASSERT_EQ(row_ids->size(), roaring.cardinality());
}

} // namespace doris::vectorized