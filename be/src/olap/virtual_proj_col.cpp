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
#include "virtual_proj_col.h"

#include <vec/exprs/vslot_ref.h>

namespace doris::v_proj {

bool has_v_proj_col(const vectorized::VectorizedFnCall* func) {
    for (const auto& c : func->children()) {
        if (c->expr_name().starts_with(BeConsts::VIRTUAL_PROJ_COL_PREFIX)) {
            return true;
        }
    }
    return false;
}

int find_v_proj_col(const vectorized::VectorizedFnCall* func,
                    vectorized::ColumnWithTypeAndName& v_proj_col) {
    for (const auto& c : func->children()) {
        if (c->expr_name() == v_proj_col.name) {
            std::shared_ptr<vectorized::VSlotRef> slot_ref =
                    std::dynamic_pointer_cast<vectorized::VSlotRef>(c);

            if (slot_ref) {
                return slot_ref->column_id();
            }
        }
    }

    return -1;
}

VirtualProjColIters VirtualProjColIterFactory::create(
        const std::shared_ptr<VirtualProjFuncDesc>& virtual_proj_func_desc) const {
    VirtualProjColIters v_col_iters = internal_create(virtual_proj_func_desc);

    if (UNLIKELY(v_col_iters.size() != virtual_proj_func_desc->v_cols.size())) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "Expected {} VirtualProjColumnIterators, "
                               "but got {} ones for function {}.",
                               virtual_proj_func_desc->v_cols.size(), v_col_iters.size(),
                               virtual_proj_func_desc->func->debug_string());
    }

    for (size_t i = 0; i < virtual_proj_func_desc->v_cols.size(); i++) {
        v_col_iters[i]->result_col_id = virtual_proj_func_desc->v_cols[i].result_col_id;
        v_col_iters[i]->col_name = virtual_proj_func_desc->v_cols[i].column.name;
        v_col_iters[i]->type = virtual_proj_func_desc->v_cols[i].column.type;
    }

    return v_col_iters;
}

PushdownFuncDesc VirtualProjColItersInitializer::create() const {
    if (const auto& it = REGISTERED_V_PROJ_ITER_FACTORIES.find(get_func()->fn().name.function_name);
        it != REGISTERED_V_PROJ_ITER_FACTORIES.end()) {
        auto v_col_iters = std::make_shared<VirtualProjColIters>(
                it->second->create(virtual_proj_func_desc));

        return {virtual_proj_func_desc->func, v_col_iters, virtual_proj_func_desc->is_predicate};
    }

    throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                           "No VirtualProjColIterFactory found for function {}",
                           get_func()->fn().name.function_name);
}

VirtualProjColIters BM25VirtualProjColIterFactory::internal_create(
        const std::shared_ptr<VirtualProjFuncDesc>& virtual_proj_func_desc) const {
    if (UNLIKELY(virtual_proj_func_desc->func->fn().name.function_name != "bm25")) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "Unexpected function {} for BM25VirtualProjColIterFactory.",
                               virtual_proj_func_desc->func->fn().name.function_name);
    }

    if (UNLIKELY(virtual_proj_func_desc->v_cols.size() != 1)) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "BM25 function expects 1 arguemt, but found {}. {}",
                               virtual_proj_func_desc->v_cols.size(),
                               virtual_proj_func_desc->func->debug_string());
    }

    VirtualProjColIters iters(1, nullptr);
    VirtualProjColumnIterator* iter = new BM25VirtualProjColumnIterator();
    iters[0].reset(iter);

    return iters;
}

Status BM25VirtualProjColumnIterator::read_by_rowids(const segment_v2::rowid_t* rowids, const size_t count,
                                                     vectorized::MutableColumnPtr& dst) {
    auto *nullable_res = typeid_cast<vectorized::ColumnNullable*>(dst.get());
    if (UNLIKELY(!nullable_res)) {
        return Status::Error(ErrorCode::INTERNAL_ERROR,
                       "Wrong type {} for BM25 virtual proj column.",
                       dst.get()->get_name());
    }
    auto *float_res = typeid_cast<vectorized::ColumnFloat32*>(&nullable_res->get_nested_column());
    if (UNLIKELY(!float_res)) {
        return Status::Error(ErrorCode::INTERNAL_ERROR,
                   "Wrong type {} for BM25 virtual proj column.",
                   dst.get()->get_name());
    }

    if (count == 0) {
        return Status::OK();
    }

    nullable_res->insert_null_elements(count);

    for (int i = 0; i < count; i++) {
        auto it = _bm25_scores.find(rowids[i]);
        if (it != _bm25_scores.end()) {
            nullable_res->get_null_map_data()[i] = 0;
            float_res->get_data()[i] = it->second;
        }
    }

    return Status::OK();
}

void BM25VirtualProjColumnIterator::cal_and_add_bm25_score(
        segment_v2::rowid_t row_id, uint64_t term_freq, uint64_t doc_term_cnt,
        float idf, float avg_token_cnt,
        float k1, float b, float boost) {
    float term_cnt_weight;

    if (UNLIKELY(avg_token_cnt == 0)) {
        term_cnt_weight = 0.0;
    } else {
        term_cnt_weight = k1 * (1.0 - b + b * (doc_term_cnt / avg_token_cnt));
    }

    float bm25_score = idf * (term_freq * (k1 + 1.0)) / (term_freq + term_cnt_weight);
    _bm25_scores[row_id] += boost * bm25_score;
}

VirtualProjColIters ApproxDistanceVirtualProjColIterFactory::internal_create(
        const std::shared_ptr<VirtualProjFuncDesc>& virtual_proj_func_desc) const {
    if (UNLIKELY(virtual_proj_func_desc->func->fn().name.function_name != this->name)) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "Unexpected function {} for ApproxDistanceVirtualProjColIterFactory.",
                               virtual_proj_func_desc->func->fn().name.function_name);
    }

    if (UNLIKELY(virtual_proj_func_desc->v_cols.size() != 1)) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                "ApproxDistance Function expects 1 virtual arguemt, but found {}. {}",
                                virtual_proj_func_desc->v_cols.size(),
                                virtual_proj_func_desc->func->debug_string());
    }

    VirtualProjColIters iters(1, nullptr);
    VirtualProjColumnIterator* iter = new ApproxDistanceVirtualProjColumnIterator();
    iters[0].reset(iter);

    return iters;
}

void ApproxDistanceVirtualProjColumnIterator::set_rowid_to_distance_map(
        std::unordered_map<segment_v2::rowid_t, float> id2distance_map) {
    _id2distance_map = std::move(id2distance_map);
}

Status ApproxDistanceVirtualProjColumnIterator::read_by_rowids(const segment_v2::rowid_t* rowids, const size_t count,
                                                     vectorized::MutableColumnPtr& dst) {
    auto *nullable_res = typeid_cast<vectorized::ColumnNullable*>(dst.get());
    if (UNLIKELY(!nullable_res)) {
        return Status::Error(ErrorCode::INTERNAL_ERROR,
                       "Wrong type {} for ApproxDistance virtual proj column.",
                       dst.get()->get_name());
    }
    auto *float_res = typeid_cast<vectorized::ColumnFloat64*>(&nullable_res->get_nested_column());
    if (UNLIKELY(!float_res)) {
        return Status::Error(ErrorCode::INTERNAL_ERROR,
                   "Wrong type {} for ApproxDistance virtual proj column.",
                   dst.get()->get_name());
    }

    if (count == 0) {
        return Status::OK();
    }

    nullable_res->insert_null_elements(count);
    for (int i = 0; i < count; i++) {
        const auto& it = _id2distance_map.find(rowids[i]);
        if (it != _id2distance_map.end()) {
            nullable_res->get_null_map_data()[i] = 0;
            float_res->get_data()[i] = it->second;
        }
    }

    return Status::OK();
}

} // namespace doris::v_proj
