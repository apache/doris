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
#pragma once

#include <utility>

#include "vec/exprs/vectorized_fn_call.h"
#include "rowset/segment_v2/common.h"


namespace doris::v_proj {
class VirtualProjColumnIterator;
class VirtualProjColItersInitializer;
class VirtualProjFuncDesc;
class PushdownFuncDesc;

using TermDocCnt =
    std::unordered_map<std::string /*term*/, uint64_t /*Number of documents containing the term*/>;
using FieldAverageTermCnt =
    std::unordered_map<std::string /*field*/, uint64_t /*Average number of terms*/>;
using VirtualProjColItersInitializers = std::vector<v_proj::VirtualProjColItersInitializer>;
using VirtualProjColIters = std::vector<std::shared_ptr<VirtualProjColumnIterator>>;
using VirtualProjFuncDescs =
        std::vector<std::shared_ptr<VirtualProjFuncDesc>>;
using PushdownFuncDescs = std::vector<PushdownFuncDesc>;

class PushdownFuncDesc {
public:
    std::shared_ptr<vectorized::VectorizedFnCall> func;
    std::shared_ptr<VirtualProjColIters> v_proj_col_iters;
    const bool is_predicate;

    PushdownFuncDesc(const std::shared_ptr<vectorized::VectorizedFnCall>& func,
                     const std::shared_ptr<VirtualProjColIters>& v_proj_col_iters,
                     const bool is_predicate)
            : func(func), v_proj_col_iters(v_proj_col_iters), is_predicate(is_predicate) {}
};

class VirtualProjColDesc {
public:
    /**
     * Column ID in the original blocks.
     */
    const size_t result_col_id;

    /**
     * Column ID in the final blocks (returned by ScanNodes to the next operators).
     */
    const size_t return_col_id;

    const vectorized::ColumnWithTypeAndName column;

    VirtualProjColDesc(size_t result_col_id, size_t return_col_id,
                       vectorized::ColumnWithTypeAndName&& column)
            : result_col_id(result_col_id),
              return_col_id(return_col_id),
              column(std::move(column)) {}
};

class VirtualProjFuncDesc {
public:
    const std::shared_ptr<vectorized::VectorizedFnCall> func;
    std::vector<VirtualProjColDesc> v_cols;
    const bool is_predicate;

    explicit VirtualProjFuncDesc(
        const std::shared_ptr<vectorized::VectorizedFnCall>& func,
        const bool is_predicate)
            : func(func), is_predicate(is_predicate) {
        v_cols.reserve(func->children().size());
    }
};

class VirtualProjColumnIterator {
public:
    VirtualProjColumnIterator() = default;

    virtual ~VirtualProjColumnIterator() = default;

    virtual Status read_by_rowids(
        const segment_v2::rowid_t* rowids,
        const size_t count,
        vectorized::MutableColumnPtr& dst) = 0;

    size_t get_result_col_id() const { return result_col_id; }

    vectorized::MutableColumnPtr create_column() const {
        return type->create_column();
    }

    friend class VirtualProjColIterFactory;
private:
    size_t result_col_id;
    std::string col_name;
    vectorized::DataTypePtr type;
};

class VirtualProjColIterFactory {
public:
    const std::string name;

    explicit VirtualProjColIterFactory(std::string name): name{std::move(name)} {}

    virtual ~VirtualProjColIterFactory() = default;

    /**
     * NOTE:
     * The order of iterators for proj columns must match the input virtual_proj_func_desc.v_cols.
     */
    VirtualProjColIters create(
        const std::shared_ptr<VirtualProjFuncDesc>& virtual_proj_func_desc) const;
protected:
    virtual VirtualProjColIters internal_create(
            const std::shared_ptr<VirtualProjFuncDesc>& virtual_proj_func_desc) const = 0;
};

class VirtualProjColItersInitializer {
public:
    explicit VirtualProjColItersInitializer(
            const std::shared_ptr<VirtualProjFuncDesc>& virtual_proj_func_desc)
            : virtual_proj_func_desc(virtual_proj_func_desc) {}

    PushdownFuncDesc create() const;

    size_t v_cols_size() const {
        return virtual_proj_func_desc->v_cols.size();
    }

    const std::vector<VirtualProjColDesc>& get_v_cols() const {
        return virtual_proj_func_desc->v_cols;
    }

    const vectorized::VectorizedFnCall* get_func() const {
        return virtual_proj_func_desc->func.get();
    }

    bool is_predicate() const {
        return virtual_proj_func_desc->is_predicate;
    }
private:
    const std::shared_ptr<VirtualProjFuncDesc> virtual_proj_func_desc;
};

// BM25 related implementations.
class BM25VirtualProjColumnIterator final : public VirtualProjColumnIterator {
public:
    BM25VirtualProjColumnIterator() {
        _bm25_scores.reserve(1024*16);
    }

    Status read_by_rowids(const segment_v2::rowid_t* rowids, const size_t count,
                          vectorized::MutableColumnPtr& dst) override;

    /**
     *  Calculate the BM25 score for the row with a specific row ID.
     *  A row may contain multiple full-text columns. The final BM25
     *  score is cumulative, by summing the BM25 score of each column
     *  with their respective boosts.
     *
     * @param row_id         The row ID in the tablet.
     * @param term_freq      The frequency of the term in the document.
     * @param doc_term_cnt   The number of terms in the document.
     * @param idf            The log inverterd document frequency of the term.
     * @param avg_term_cnt   The average terms of all documents.
     * @param k1             Term frequency saturation parameter
     * @param b              Length normalization parameter
     * @param boost          The weight for the relevance score of the column.
     */
    void cal_and_add_bm25_score(
            segment_v2::rowid_t row_id,
            uint64_t term_freq,
            uint64_t doc_term_cnt,
            float idf,
            float avg_term_cnt,
            float k1 = 1.2,
            float b = 0.75,
            float boost = 1.0);

    const phmap::flat_hash_map<segment_v2::rowid_t, float>& get_bm25_scores() const {
        return _bm25_scores;
    }

private:
    phmap::flat_hash_map<segment_v2::rowid_t, float> _bm25_scores;
};

class BM25VirtualProjColIterFactory final: public VirtualProjColIterFactory {
public:
    BM25VirtualProjColIterFactory() : VirtualProjColIterFactory("bm25") {}
protected:
    VirtualProjColIters internal_create(
        const std::shared_ptr<VirtualProjFuncDesc>& virtual_proj_func_desc) const override;
};

// approx distance related implementations.
class ApproxDistanceVirtualProjColumnIterator final: public VirtualProjColumnIterator {
public:
    Status read_by_rowids(
            const segment_v2::rowid_t* rowids,
            const size_t count,
            vectorized::MutableColumnPtr& dst) override;

    void set_rowid_to_distance_map(
            std::unordered_map<segment_v2::rowid_t, float> id2distance_map);

private:
    std::unordered_map<segment_v2::rowid_t, float> _id2distance_map;
};

class ApproxDistanceVirtualProjColIterFactory final: public VirtualProjColIterFactory {
public:
    ApproxDistanceVirtualProjColIterFactory(const std::string& func_name)
        :VirtualProjColIterFactory(func_name) {}

protected:
    VirtualProjColIters internal_create(
        const std::shared_ptr<VirtualProjFuncDesc>& virtual_proj_func_desc) const override;
};

// Utility functions.
bool has_v_proj_col(const vectorized::VectorizedFnCall* func);

/**
 * Return the column ID in blocks.
 */
int find_v_proj_col(const vectorized::VectorizedFnCall* func,
                    vectorized::ColumnWithTypeAndName& v_proj_col);

// Registered functions for projection pushdown optimization.
inline const std::unordered_map<std::string, std::unique_ptr<const VirtualProjColIterFactory>>
        REGISTERED_V_PROJ_ITER_FACTORIES = [] {
            std::unordered_map<std::string, std::unique_ptr<const VirtualProjColIterFactory>> map;

            map.emplace("bm25", std::make_unique<const BM25VirtualProjColIterFactory>());
            map.emplace("approx_l2_distance", std::make_unique<const ApproxDistanceVirtualProjColIterFactory>("approx_l2_distance"));
            map.emplace("approx_cosine_similarity", std::make_unique<const ApproxDistanceVirtualProjColIterFactory>("approx_cosine_similarity"));
            map.emplace("approx_inner_product", std::make_unique<const ApproxDistanceVirtualProjColIterFactory>("approx_inner_product"));

            return map;
        }();
} // namespace doris::v_proj
