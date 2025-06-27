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

#include "vec/exprs/ann_topn_runtime.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "common/logging.h"
#include "olap/rowset/segment_v2/ann_index/ann_search_params.h"
#include "olap/rowset/segment_v2/ann_index_iterator.h"
#include "runtime/runtime_state.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/assert_cast.h"
#include "vec/exprs/varray_literal.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vexpr_fwd.h"
#include "vec/exprs/virtual_slot_ref.h"
#include "vec/exprs/vslot_ref.h"
#include "vec/functions/array/function_array_distance_approximate.h"

namespace doris::vectorized {

Status AnnTopNRuntime::prepare(RuntimeState* state, const RowDescriptor& row_desc) {
    RETURN_IF_ERROR(_order_by_expr_ctx->prepare(state, row_desc));
    RETURN_IF_ERROR(_order_by_expr_ctx->open(state));

    // Check the structure of the _order_by_expr_ctx

    /*
        VirtualSlotRef
        |
        |
        FuncationCall
        |----------------
        |               |
        |               |
        CastToArray     ArrayLiteral
        |
        |
        SlotRef
    */
    std::shared_ptr<VirtualSlotRef> vir_slot_ref =
            std::dynamic_pointer_cast<VirtualSlotRef>(_order_by_expr_ctx->root());
    DCHECK(vir_slot_ref != nullptr);
    if (vir_slot_ref == nullptr) {
        return Status::InternalError(
                "root of order by expr of ann topn must be a VirtualSlotRef, got\n{}",
                _order_by_expr_ctx->root()->debug_string());
    }
    DCHECK(vir_slot_ref->column_id() >= 0);
    _dest_column_idx = vir_slot_ref->column_id();
    auto vir_col_expr = vir_slot_ref->get_virtual_column_expr();
    std::shared_ptr<VectorizedFnCall> distance_fn_call =
            std::dynamic_pointer_cast<VectorizedFnCall>(vir_col_expr);

    if (distance_fn_call == nullptr) {
        return Status::InternalError("Ann topn expr expect FuncationCall, got\n{}",
                                     vir_col_expr->debug_string());
    }

    std::shared_ptr<VCastExpr> cast_to_array_expr =
            std::dynamic_pointer_cast<VCastExpr>(distance_fn_call->children()[0]);

    if (cast_to_array_expr == nullptr) {
        return Status::InternalError("Ann topn expr expect cast_to_array_expr, got\n{}",
                                     distance_fn_call->children()[0]->debug_string());
    }

    std::shared_ptr<VSlotRef> slot_ref =
            std::dynamic_pointer_cast<VSlotRef>(cast_to_array_expr->children()[0]);
    if (slot_ref == nullptr) {
        return Status::InternalError("Ann topn expr expect SlotRef, got\n{}",
                                     cast_to_array_expr->children()[0]->debug_string());
    }

    // slot_ref->column_id() is acutually the columnd idx in block.
    _src_column_idx = slot_ref->column_id();

    std::shared_ptr<VArrayLiteral> array_literal =
            std::dynamic_pointer_cast<VArrayLiteral>(distance_fn_call->children()[1]);
    if (array_literal == nullptr) {
        return Status::InternalError("Ann topn expr expect ArrayLiteral, got\n{}",
                                     distance_fn_call->children()[1]->debug_string());
    }
    _query_array = array_literal->get_column_ptr();
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
                                                  roaring::Roaring& roaring,
                                                  vectorized::IColumn::MutablePtr& result_column,
                                                  std::unique_ptr<std::vector<uint64_t>>& row_ids) {
    DCHECK(ann_index_iterator != nullptr);
    segment_v2::AnnIndexIterator* ann_index_iterator_casted =
            dynamic_cast<segment_v2::AnnIndexIterator*>(ann_index_iterator);
    DCHECK(ann_index_iterator_casted != nullptr);
    DCHECK(_order_by_expr_ctx != nullptr);
    DCHECK(_order_by_expr_ctx->root() != nullptr);

    const ColumnConst* const_column = assert_cast<const ColumnConst*>(_query_array.get());
    const ColumnArray* column_array =
            assert_cast<const ColumnArray*>(const_column->get_data_column_ptr().get());
    const ColumnNullable* column_nullable =
            assert_cast<const ColumnNullable*>(column_array->get_data_ptr().get());
    const ColumnFloat64* cf64 =
            assert_cast<const ColumnFloat64*>(column_nullable->get_nested_column_ptr().get());

    const double* query_value = cf64->get_data().data();
    const size_t query_value_size = cf64->get_data().size();

    std::unique_ptr<float[]> query_value_f32 = std::make_unique<float[]>(query_value_size);
    for (size_t i = 0; i < query_value_size; ++i) {
        query_value_f32[i] = static_cast<float>(query_value[i]);
    }

    vectorized::AnnIndexParam ann_query_params {
            .query_value = query_value_f32.get(),
            .query_value_size = query_value_size,
            .limit = _limit,
            ._user_params = _user_params,
            .roaring = &roaring,
            .distance = nullptr,
            .row_ids = nullptr,
    };
    {
        RuntimeProfile::Counter search_counter {TUnit::TIME_NS};
        SCOPED_TIMER(&search_counter);
        RETURN_IF_ERROR(ann_index_iterator->read_from_index(&ann_query_params));
        LOG_INFO("Ann index search costs {} ms",
                 search_counter.value() / 1e6); // Convert to milliseconds
    }

    DCHECK(ann_query_params.distance != nullptr);
    DCHECK(ann_query_params.row_ids != nullptr);

    size_t num_results = ann_query_params.distance->size();
    auto result_column_double = ColumnFloat64::create(num_results);
    auto result_null_map = ColumnUInt8::create(num_results, 0);

    for (size_t i = 0; i < num_results; ++i) {
        result_column_double->get_data()[i] = (*ann_query_params.distance)[i];
    }

    result_column =
            ColumnNullable::create(std::move(result_column_double), std::move(result_null_map));
    row_ids = std::move(ann_query_params.row_ids);
    return Status::OK();
}

std::string AnnTopNRuntime::debug_string() const {
    return fmt::format(
            "AnnTopNRuntime: limit={}, src_col_idx={}, dest_col_idx={}, asc={}, user_params={}, "
            "metric_type={}, order_by_expr={}",
            _limit, _src_column_idx, _dest_column_idx, _asc, _user_params.to_string(),
            segment_v2::metric_to_string(_metric_type), _order_by_expr_ctx->root()->debug_string());
}
} // namespace doris::vectorized