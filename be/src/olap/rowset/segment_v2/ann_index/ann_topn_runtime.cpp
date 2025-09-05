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

#include "ann_topn_runtime.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "common/exception.h"
#include "common/logging.h"
#include "common/status.h"
#include "olap/rowset/segment_v2/ann_index/ann_index_iterator.h"
#include "olap/rowset/segment_v2/ann_index/ann_search_params.h"
#include "olap/rowset/segment_v2/inverted_index_query_type.h"
#include "runtime/primitive_type.h"
#include "runtime/runtime_state.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vec/exprs/varray_literal.h"
#include "vec/exprs/vcast_expr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vexpr_fwd.h"
#include "vec/exprs/virtual_slot_ref.h"
#include "vec/exprs/vslot_ref.h"
#include "vec/functions/array/function_array_distance.h"

namespace doris::segment_v2 {
#include "common/compile_check_begin.h"

Result<vectorized::IColumn::Ptr> extract_query_vector(std::shared_ptr<vectorized::VExpr> arg_expr) {
    if (arg_expr->is_constant() == false) {
        return ResultError(Status::InvalidArgument("Ann topn expr must be constant, got\n{}",
                                                   arg_expr->debug_string()));
    }

    // Accept either ArrayLiteral([..]) or CAST('..' AS Nullable(Array(Nullable(Float32))))
    // First, check the expr node type for clarity.

    bool is_array_literal =
            std::dynamic_pointer_cast<vectorized::VArrayLiteral>(arg_expr) != nullptr;
    bool is_cast_expr = std::dynamic_pointer_cast<vectorized::VCastExpr>(arg_expr) != nullptr;
    if (!is_array_literal && !is_cast_expr) {
        return ResultError(
                Status::InvalidArgument("Constant must be ArrayLiteral or CAST to array, got\n{}",
                                        arg_expr->debug_string()));
    }

    // We'll validate shape by inspecting the materialized constant column below.

    std::shared_ptr<ColumnPtrWrapper> column_wrapper;
    auto st = arg_expr->get_const_col(nullptr, &column_wrapper);
    if (!st.ok()) {
        return ResultError(Status::InvalidArgument("Failed to get constant column, error: {}",
                                                   st.to_string()));
    }

    // Execute the constant array literal and extract its float elements into _query_array
    vectorized::IColumn::Ptr col_ptr =
            column_wrapper->column_ptr->convert_to_full_column_if_const();

    // The expected runtime column layout for the literal is:
    // Nullable(ColumnArray(Nullable(ColumnFloat32))) with exactly 1 row (one array literal)
    const vectorized::IColumn* top_col = col_ptr.get();
    const vectorized::IColumn* array_holder_col = top_col;
    // Handle outer Nullable and remember result nullability preference
    if (auto* nullable_col =
                vectorized::check_and_get_column<vectorized::ColumnNullable>(*top_col)) {
        if (nullable_col->has_null()) {
            return ResultError(Status::InvalidArgument("Ann query vector cannot be NULL"));
        }
        array_holder_col = &nullable_col->get_nested_column();
    }

    // Must be an array column with single row
    const auto* array_col =
            vectorized::check_and_get_column<vectorized::ColumnArray>(*array_holder_col);
    if (array_col == nullptr || array_col->size() != 1) {
        return ResultError(Status::InvalidArgument(
                "Ann topn expr constant should be an Array literal, got column: {}",
                array_holder_col->get_name()));
    }

    // Fetch nested data column: Nullable(ColumnFloat32) or ColumnFloat32
    const vectorized::IColumn& nested_data_any = array_col->get_data();
    vectorized::IColumn::Ptr values_holder_col = array_col->get_data_ptr();
    size_t value_count = array_col->get_offsets()[0];

    if (value_count == 0) {
        return ResultError(Status::InvalidArgument("Ann topn query vector cannot be empty"));
    }

    if (auto* value_nullable_col =
                vectorized::check_and_get_column<vectorized::ColumnNullable>(nested_data_any)) {
        if (value_nullable_col->has_null(0, value_count)) {
            return ResultError(Status::InvalidArgument(
                    "Ann topn query vector elements cannot contain NULL values"));
        }
        values_holder_col = value_nullable_col->get_nested_column_ptr();
    }

    return values_holder_col;
}

Status AnnTopNRuntime::prepare(RuntimeState* state, const RowDescriptor& row_desc) {
    RETURN_IF_ERROR(_order_by_expr_ctx->prepare(state, row_desc));
    RETURN_IF_ERROR(_order_by_expr_ctx->open(state));

    // Check the structure of the _order_by_expr_ctx

    /*
        vectorized::VirtualSlotRef
        |
        |
        FuncationCall
        |----------------
        |               |
        |               |
        SlotRef         CAST(String as Nullable<ArrayFloat>) OR ArrayLiteral
    */
    std::shared_ptr<vectorized::VirtualSlotRef> vir_slot_ref =
            std::dynamic_pointer_cast<vectorized::VirtualSlotRef>(_order_by_expr_ctx->root());
    DCHECK(vir_slot_ref != nullptr);
    if (vir_slot_ref == nullptr) {
        return Status::InvalidArgument(
                "root of order by expr of ann topn must be a vectorized::VirtualSlotRef, got\n{}",
                _order_by_expr_ctx->root()->debug_string());
    }
    DCHECK(vir_slot_ref->column_id() >= 0);
    _dest_column_idx = vir_slot_ref->column_id();
    auto vir_col_expr = vir_slot_ref->get_virtual_column_expr();
    std::shared_ptr<vectorized::VectorizedFnCall> distance_fn_call =
            std::dynamic_pointer_cast<vectorized::VectorizedFnCall>(vir_col_expr);

    if (distance_fn_call == nullptr) {
        return Status::InvalidArgument("Ann topn expr expect FuncationCall, got\n{}",
                                       vir_col_expr->debug_string());
    }

    std::shared_ptr<vectorized::VSlotRef> slot_ref =
            std::dynamic_pointer_cast<vectorized::VSlotRef>(distance_fn_call->children()[0]);
    if (slot_ref == nullptr) {
        return Status::InvalidArgument("Ann topn expr expect SlotRef, got\n{}",
                                       distance_fn_call->children()[0]->debug_string());
    }

    // slot_ref->column_id() is acutually the columnd idx in block.
    _src_column_idx = slot_ref->column_id();

    if (distance_fn_call->children()[1]->is_constant() == false) {
        return Status::InvalidArgument("Ann topn expr expect constant ArrayLiteral, got\n{}",
                                       distance_fn_call->children()[1]->debug_string());
    }

    // Accept either ArrayLiteral([..]) or CAST('..' AS Nullable(Array(Nullable(Float32))))
    // First, check the expr node type for clarity.
    auto arg_expr = distance_fn_call->children()[1];

    auto query_array_result = extract_query_vector(arg_expr);
    if (!query_array_result.has_value()) {
        return query_array_result.error();
    }
    _query_array = query_array_result.value();

    _user_params = state->get_vector_search_params();

    std::set<std::string> distance_func_names = {vectorized::L2DistanceApproximate::name,
                                                 vectorized::InnerProductApproximate::name};
    if (distance_func_names.contains(distance_fn_call->function_name()) == false) {
        return Status::InternalError("Ann topn expr expect distance function, got {}",
                                     distance_fn_call->function_name());
    }
    std::string metric_name = distance_fn_call->function_name();
    // Strip the "_approximate" suffix
    metric_name = metric_name.substr(0, metric_name.size() - 12);

    _metric_type = segment_v2::string_to_metric(metric_name);

    VLOG_DEBUG << "AnnTopNRuntime: {}" << this->debug_string();
    return Status::OK();
}

Status AnnTopNRuntime::evaluate_vector_ann_search(segment_v2::IndexIterator* ann_index_iterator,
                                                  roaring::Roaring* roaring, size_t rows_of_segment,
                                                  vectorized::IColumn::MutablePtr& result_column,
                                                  std::unique_ptr<std::vector<uint64_t>>& row_ids,
                                                  segment_v2::AnnIndexStats& ann_index_stats) {
    DCHECK(ann_index_iterator != nullptr);
    segment_v2::AnnIndexIterator* ann_index_iterator_casted =
            dynamic_cast<segment_v2::AnnIndexIterator*>(ann_index_iterator);
    DCHECK(ann_index_iterator_casted != nullptr);
    DCHECK(_order_by_expr_ctx != nullptr);
    DCHECK(_order_by_expr_ctx->root() != nullptr);
    size_t query_array_size = _query_array->size();
    if (_query_array.get() == nullptr || query_array_size == 0) {
        return Status::InternalError("Ann topn query vector is not initialized");
    }

    // TODO:(zhiqiang) Maybe we can move this dimension check to prepare phase.

    auto index_reader = ann_index_iterator_casted->get_reader(AnnIndexReaderType::ANN);
    auto ann_index_reader = std::dynamic_pointer_cast<AnnIndexReader>(index_reader);
    DCHECK(ann_index_reader != nullptr);
    if (ann_index_reader->get_dimension() != query_array_size) {
        return Status::InvalidArgument(
                "Ann topn query vector dimension {} does not match index dimension {}",
                query_array_size, ann_index_reader->get_dimension());
    }
    const vectorized::ColumnFloat32* query =
            assert_cast<const vectorized::ColumnFloat32*>(_query_array.get());
    segment_v2::AnnTopNParam ann_query_params {
            .query_value = query->get_data().data(),
            .query_value_size = query_array_size,
            .limit = _limit,
            ._user_params = _user_params,
            .roaring = roaring,
            .rows_of_segment = rows_of_segment,
            .distance = nullptr,
            .row_ids = nullptr,
            .stats = std::make_unique<segment_v2::AnnIndexStats>()};

    RETURN_IF_ERROR(ann_index_iterator->read_from_index(&ann_query_params));

    DCHECK(ann_query_params.distance != nullptr);
    DCHECK(ann_query_params.row_ids != nullptr);

    size_t num_results = ann_query_params.distance->size();
    auto result_column_float = vectorized::ColumnFloat32::create(num_results);
    for (size_t i = 0; i < num_results; ++i) {
        result_column_float->get_data()[i] = (*ann_query_params.distance)[i];
    }
    result_column = std::move(result_column_float);
    row_ids = std::move(ann_query_params.row_ids);
    ann_index_stats = *ann_query_params.stats;
    return Status::OK();
}

std::string AnnTopNRuntime::debug_string() const {
    return fmt::format(
            "AnnTopNRuntime: limit={}, src_col_idx={}, dest_col_idx={}, asc={}, user_params={}, "
            "metric_type={}, order_by_expr={}",
            _limit, _src_column_idx, _dest_column_idx, _asc, _user_params.to_string(),
            segment_v2::metric_to_string(_metric_type), _order_by_expr_ctx->root()->debug_string());
}
} // namespace doris::segment_v2
