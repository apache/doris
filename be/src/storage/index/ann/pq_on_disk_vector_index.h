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

#include <CLucene.h>
#include <CLucene/store/IndexInput.h>

// clang-format off
#include "common/compile_check_avoid_begin.h"
#include <faiss/impl/ProductQuantizer.h>
#include "common/compile_check_avoid_end.h"
// clang-format on

#include <cstdint>
#include <memory>
#include <mutex>
#include <roaring/roaring.hh>
#include <string>
#include <vector>

#include "common/status.h"
#include "core/custom_allocator.h"
#include "core/types.h"
#include "storage/cache/ann_index_pq_chunk_cache.h"
#include "storage/index/ann/ann_index.h"

namespace doris::io {
struct IOContext;
} // namespace doris::io

namespace doris::segment_v2 {

struct IndexSearchParameters;
struct IndexSearchResult;

/**
 * @brief Build parameters for PQ_ON_DISK vector index.
 *
 * PQ_ON_DISK stores Product Quantization codes on disk in rowid order and
 * performs ADC (Asymmetric Distance Computation) on small candidate sets
 * (1K~10K rows) after inverted index filtering.
 */
struct PqOnDiskBuildParameter {
    int dim = 0;
    int pq_m = 0;
    int pq_nbits = 8;
    AnnIndexMetric metric = AnnIndexMetric::L2;
};

/**
 * @brief PQ_ON_DISK vector index implementation.
 *
 * This class implements the VectorIndex interface using FAISS ProductQuantizer
 * for training/encoding, with a custom on-disk format (ann.pqmeta + ann.pqdata).
 * It does NOT inherit from FaissVectorIndex.
 *
 * Key design:
 * - PQ codes stored in rowid order for sequential I/O on filtered candidates
 * - Codebook (~768KB for dim=768) resides in memory; codes read on demand
 * - ADC search directly on bitmap-filtered rowid candidates
 * - No IVF/HNSW graph structure; purely post-filter approximate reranking
 *
 * Thread safety: concurrent searches share the codebook (read-only) but
 * serialise I/O on the CLucene IndexInput via _io_mutex.
 */
class PqOnDiskVectorIndex : public VectorIndex {
public:
    PqOnDiskVectorIndex();
    ~PqOnDiskVectorIndex() override;

    /// Configure build parameters and initialise the FAISS ProductQuantizer.
    void build(const PqOnDiskBuildParameter& params);

    // ---- VectorIndex interface ----

    doris::Status train(Int64 n, const float* x) override;
    doris::Status add(Int64 n, const float* x) override;
    Int64 get_min_train_rows() const override;

    doris::Status ann_topn_search(const float* query_vec, int k,
                                  const IndexSearchParameters& params,
                                  IndexSearchResult& result) override;

    doris::Status range_search(const float* query_vec, const float& radius,
                               const IndexSearchParameters& params,
                               IndexSearchResult& result) override;

    doris::Status save(lucene::store::Directory*) override;
    doris::Status load(lucene::store::Directory*) override;

    /// Set cache key prefix for pqdata I/O (call before load()).
    void set_pqdata_cache_key_prefix(std::string prefix) {
        _pqdata_cache_key_prefix = std::move(prefix);
    }

private:
    // ---- ADC search internals ----

    /// Quantized distance LUT: float dis_table compressed to uint8 for cache efficiency.
    /// Original float distance ≈ quant_sum * scale + M * bias, where quant_sum is the
    /// accumulated uint8 lookup values over all M sub-quantizers.
    /// This reduces LUT size by 4x (e.g. 384KB → 96KB for M=384, ksub=256),
    /// improving cache residency from L3 to L2 (or L2 to L1 for smaller M).
    struct QuantizedLUT {
        std::vector<uint8_t> table; // M * ksub quantized distances
        float scale = 0;            // per-entry scale: (max - min) / 255
        float bias = 0;             // per-entry bias: global minimum value
    };

    /// Build a QuantizedLUT from a float distance table (M * ksub entries).
    void _quantize_dis_table(const float* dis_table, QuantizedLUT& lut) const;

    /// L2 ADC TopN: iterate bitmap rowids, compute ADC distance via quantized LUT, maintain max-heap.
    doris::Status _adc_topn_l2(const QuantizedLUT& lut, int k, const roaring::Roaring& bitmap,
                               const io::IOContext* io_ctx, IndexSearchResult& result);

    /// IP ADC TopN: iterate bitmap rowids, compute ADC inner product via quantized LUT, maintain min-heap.
    doris::Status _adc_topn_ip(const QuantizedLUT& lut, int k, const roaring::Roaring& bitmap,
                               const io::IOContext* io_ctx, IndexSearchResult& result);

    /// L2 Range Search: iterate bitmap, ADC accumulate via quantized LUT, threshold compare.
    doris::Status _adc_range_l2(const QuantizedLUT& lut, float radius, bool is_le_or_lt,
                                const roaring::Roaring& bitmap, const roaring::Roaring& all_rows,
                                const io::IOContext* io_ctx, IndexSearchResult& result);

    /// IP Range Search: iterate bitmap, ADC accumulate via quantized LUT, threshold compare.
    doris::Status _adc_range_ip(const QuantizedLUT& lut, float radius, bool is_le_or_lt,
                                const roaring::Roaring& bitmap, const roaring::Roaring& all_rows,
                                const io::IOContext* io_ctx, IndexSearchResult& result);

    /// Read PQ codes for the rowids in bitmap from ann.pqdata via chunk cache.
    /// Iterates bitmap runs, maps to fixed-size chunks, uses LRU cache for
    /// zero-copy access. Visitor is called for each (rowid, code_ptr) pair
    /// where code_ptr points directly into cached chunk memory.
    template <typename Visitor>
    doris::Status _for_each_code_in_bitmap(const roaring::Roaring& bitmap,
                                           const io::IOContext* io_ctx, Visitor&& visitor);

    /// Compute ADC distance for a single PQ code using the quantized LUT.
    /// Iterates M sub-quantizers, accumulates uint8 lookups, reconstructs
    /// float distance = sum * scale + M * bias.
    float _adc_compute_distance(const uint8_t* code, const QuantizedLUT& lut) const;

    // ---- Members ----

    PqOnDiskBuildParameter _params;
    std::unique_ptr<faiss::ProductQuantizer> _pq;

    // Write phase: in-memory PQ codes buffer (flushed to ann.pqdata in save())
    doris::DorisVector<uint8_t> _codes_buffer;
    int64_t _ntotal = 0; // number of encoded vectors
    bool _is_trained = false;

    // Read phase: ann.pqdata CLucene IndexInput handle
    std::string _pqdata_cache_key_prefix;
    lucene::store::IndexInput* _pqdata_input = nullptr; // owned, cloned from Directory
    size_t _pqdata_file_size = 0;
    mutable std::mutex _io_mutex; // serialise seek+read on _pqdata_input

    // Chunk cache parameters (computed in load())
    static constexpr size_t kTargetChunkSize = 64 * 1024; // 64KB target
    size_t _chunk_size = 0;     // actual chunk size (code_size aligned), 0 before load()
    size_t _vecs_per_chunk = 0; // = _chunk_size / code_size
};
} // namespace doris::segment_v2
