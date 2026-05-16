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

#include <CLucene.h> // IWYU pragma: keep
#include <CLucene/analysis/LanguageBasedAnalyzer.h>
#include <CLucene/util/bkd/bkd_writer.h>
#include <glog/logging.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <roaring/roaring.hh>
#include <string>

#include "common/config.h"
#include "core/pod_array.h"
#include "runtime/collection_value.h"
#include "storage/index/ann/ann_build_memory_budget.h"
#include "storage/index/ann/ann_index.h"
#include "storage/index/ann/faiss_ann_index.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/index_writer.h"
#include "storage/index/inverted/inverted_index_fs_directory.h"
#include "storage/tablet/tablet_schema.h"

namespace doris::segment_v2 {
class AnnIndexColumnWriter : public IndexColumnWriter {
public:
    static inline int64_t chunk_size() {
#ifdef BE_TEST
        return 10;
#else
        return config::ann_index_build_chunk_size;
#endif
    }
    static size_t compute_chunk_rows(size_t dim);
    static constexpr const char* INDEX_TYPE = "index_type";
    static constexpr const char* METRIC_TYPE = "metric_type";
    static constexpr const char* DIM = "dim";
    static constexpr const char* MAX_DEGREE = "max_degree";
    static constexpr const char* EF_CONSTRUCTION = "ef_construction";
    static constexpr const char* NLIST = "nlist";
    static constexpr const char* QUANTIZER = "quantizer";
    static constexpr const char* PQ_M = "pq_m";
    static constexpr const char* PQ_NBITS = "pq_nbits";

    explicit AnnIndexColumnWriter(IndexFileWriter* index_file_writer,
                                  const TabletIndex* index_meta);

    ~AnnIndexColumnWriter() override;

    Status init() override;
    void close_on_error() override;
    Status add_nulls(uint32_t count) override;
    Status add_array_nulls(const uint8_t* null_map, size_t num_rows) override;
    Status add_values(const std::string fn, const void* values, size_t count) override;
    Status add_array_values(size_t field_size, const void* value_ptr, const uint8_t* null_map,
                            const uint8_t* offsets_ptr, size_t count) override;
    Status add_array_values(size_t field_size, const CollectionValue* values,
                            size_t count) override;
    int64_t size() const override;
    Status finish() override;

protected:
    void _reset_chunk_buffer(bool release_memory);
    size_t _current_chunk_capacity_elements() const { return _chunk_rows * _dimension; }
    // Train the underlying index on demand for the first batch; subsequent calls
    // are no-ops. Keeps IVF k-means from being re-run per chunk (which would
    // invalidate vectors already added) and skips lock acquisition for HNSW.
    Status _train_once_if_needed(Int64 n, const float* vec);
    // Reserve the estimated build-memory peak from AnnBuildMemoryBudget. On
    // success the reservation is held in _reservation until destruction. On
    // failure, applies ann_index_build_on_oom_action ("wait"/"skip"/"fail").
    Status _acquire_memory_budget(const FaissBuildParameter& params);
    Status _apply_oom_action(int64_t estimated_bytes, int64_t waited_ms);

    // VectorIndex shoule be managed by some cache.
    // VectorIndex should be weak shared by AnnIndexWriter and VectorIndexReader
    // This should be a weak_ptr
    std::shared_ptr<VectorIndex> _vector_index;
    // _float_array is used to buffer the float data before training/adding to vector index
    // if we dont do this, the performance(recall) will be very poor when adding small number of vectors one by one
    PODArray<float> _float_array;
    IndexFileWriter* _index_file_writer;
    const TabletIndex* _index_meta;
    std::shared_ptr<DorisFSDirectory> _dir;
    AnnBuildMemoryReservation _reservation;
    bool _need_save_index = false;
    bool _trained = false;
    // True after _apply_oom_action chose the "skip" path. Subsequent add_array_values
    // calls are no-ops and finish() deletes the index entry so the surrounding
    // segment write still succeeds.
    bool _skip_due_to_oom = false;
    size_t _dimension = 0;
    size_t _chunk_rows = 0;
};
} // namespace doris::segment_v2
