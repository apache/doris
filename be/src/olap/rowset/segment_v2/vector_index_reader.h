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

#include <common/factory_creator.h>

#include "common/status.h"
#include "olap/rowset/segment_v2/vector_index_file_reader.h"
#include "olap/tablet_schema.h"
#include "sparse_vector/sparse_inverted_index_searcher.h"
#include "tenann/common/seq_view.h"
#include "tenann/factory/index_factory.h"
#include "tenann/searcher/ann_searcher.h"
#include "tenann/store/index_meta.h"

namespace doris {
class RuntimeState;

namespace segment_v2 {
class VectorIndexIterator;

class VectorIndexReader : public std::enable_shared_from_this<VectorIndexReader> {
    ENABLE_FACTORY_CREATOR(VectorIndexReader);

public:
    VectorIndexReader(const TabletIndex* index_meta,
                      std::shared_ptr<VectorIndexFileReader>
                      vector_index_file_reader): _vector_index_file_reader(
                                                     std::move(vector_index_file_reader)),
                                                 _index_meta(index_meta) {
    }

    ~VectorIndexReader() = default;

    Status init();

    Status new_iterator(OlapReaderStatistics* stats,
                        RuntimeState* runtime_state,
                        std::unique_ptr<VectorIndexIterator>* iterator);

    [[nodiscard]] const std::map<string, string>&
    get_index_properties() const {
        return _index_meta->properties();
    }

    string get_index_property(string key);

    [[nodiscard]] uint64_t get_index_id() const {
        return _index_meta->index_id();
    }

    Status init_index_family();

    Status init_dense_index_searcher(const tenann::IndexMeta& meta,
                                     const std::string& index_path);

    Status init_sparse_index_searcher(const std::string& index_path);

    Status search(std::vector<int32_t>* query_vector_id,
                  std::vector<float>* query_vector_val,
                  int k,
                  int64_t* result_ids,
                  uint8_t* result_distances,
                  roaring::Roaring& _row_bitmap);

    Status range_search(std::vector<int32_t>* query_vector_id,
                        std::vector<float>* query_vector_val,
                        int k,
                        std::vector<int64_t>* result_ids,
                        std::vector<float>* result_distances,
                        roaring::Roaring& _row_bitmap,
                        float range,
                        int order);

    bool is_dense_vector_index();

private:
    friend class VectorIndexIterator;

    std::shared_ptr<VectorIndexFileReader> _vector_index_file_reader;
    const TabletIndex* _index_meta;

    u_int8_t _index_family = 0;

    // for faiss dense
    int _index_type;
    int _metric_type;
    std::shared_ptr<tenann::AnnSearcher> _dense_index_searcher;
    bool _use_empty_index_searcher = false;

    // for milvus sparse
    std::shared_ptr<sparse::SparseInvertedIndexSearcher> _sparse_index_searcher;
};


class VectorIndexIterator {
    ENABLE_FACTORY_CREATOR(VectorIndexIterator);

public:
    VectorIndexIterator(std::shared_ptr<VectorIndexReader> reader)
        : _reader(std::move(reader)) {
    }

    Status search(std::vector<int32_t>* query_vector_id,
                  std::vector<float>* query_vector_val,
                  int k,
                  int64_t*
                  result_ids,
                  uint8_t* result_distances,
                  roaring::Roaring& _row_bitmap);

    Status range_search(std::vector<int32_t>* query_vector_id,
                        std::vector<float>* query_vector_val,
                        int k,
                        std::vector<int64_t>* result_ids,
                        std::vector<float>* result_distances,
                        roaring::Roaring& _row_bitmap,
                        float range,
                        int order);

    bool is_dense_vector_index();

    // for now, pq index will not be created in the case of a small amount of data
    bool is_empty_index_iterator();

    int get_index_type();

    void set_approx_vector_distance_proj_iterator(
        std::shared_ptr<v_proj::ApproxDistanceVirtualProjColumnIterator> iter);

    void set_rowid_to_distance_map(
        std::unordered_map<segment_v2::rowid_t, float> id2distance_map);

private:
    std::shared_ptr<VectorIndexReader> _reader;

    // For approx vector distance function projection pushdown
    std::shared_ptr<v_proj::ApproxDistanceVirtualProjColumnIterator>
    _approx_vector_distance_v_proj_col_iter;
};
}
}
