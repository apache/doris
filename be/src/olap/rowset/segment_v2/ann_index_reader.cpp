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

#include "ann_index_reader.h"

#include <cstddef>
#include <memory>

#include "ann_index_iterator.h"
#include "common/config.h"
#include "io/io_common.h"
#include "olap/rowset/segment_v2/ann_index/ann_search_params.h"
#include "olap/rowset/segment_v2/index_file_reader.h"
#include "olap/rowset/segment_v2/inverted_index_compound_reader.h"
#include "runtime/runtime_state.h"
#include "util/once.h"
#include "util/runtime_profile.h"
#include "vector/faiss_vector_index.h"
#include "vector/vector_index.h"

namespace doris::segment_v2 {

void AnnIndexReader::update_result(const vectorized::IndexSearchResult& search_result,
                                   std::vector<float>& distance, roaring::Roaring& roaring) {
    DCHECK(search_result.distances != nullptr);
    DCHECK(search_result.roaring != nullptr);
    size_t limit = search_result.roaring->cardinality();
    // Use search result to update distance and row_id
    distance.resize(limit);
    for (size_t i = 0; i < limit; ++i) {
        distance[i] = search_result.distances[i];
    }
    roaring = *search_result.roaring;
}

AnnIndexReader::AnnIndexReader(const TabletIndex* index_meta,
                               std::shared_ptr<IndexFileReader> index_file_reader)
        : _index_meta(*index_meta), _index_file_reader(index_file_reader) {
    const auto index_properties = _index_meta.properties();
    auto it = index_properties.find("index_type");
    DCHECK(it != index_properties.end());
    _index_type = it->second;
    it = index_properties.find("metric_type");
    DCHECK(it != index_properties.end());
    _metric_type = string_to_metric(it->second);
}

Status AnnIndexReader::new_iterator(std::unique_ptr<IndexIterator>* iterator) {
    *iterator = AnnIndexIterator::create_unique(shared_from_this());
    return Status::OK();
}

Status AnnIndexReader::load_index(io::IOContext* io_ctx) {
    return _load_index_once.call([&]() {
        RETURN_IF_ERROR(_index_file_reader->init(config::inverted_index_read_buffer_size, io_ctx));

        Result<std::unique_ptr<DorisCompoundReader, DirectoryDeleter>> compound_dir =
                _index_file_reader->open(&_index_meta, io_ctx);
        if (!compound_dir.has_value()) {
            return Status::IOError("Failed to open index file: {}",
                                   compound_dir.error().to_string());
        }
        _vector_index = std::make_unique<FaissVectorIndex>();
        RETURN_IF_ERROR(_vector_index->load(compound_dir->get()));
        return Status::OK();
    });
}

Status AnnIndexReader::query(io::IOContext* io_ctx, vectorized::AnnIndexParam* param) {
#ifndef BE_TEST
    RETURN_IF_ERROR(load_index(io_ctx));
#endif
    DCHECK(_vector_index != nullptr);
    const float* query_vec = param->query_value;
    const int limit = static_cast<int>(param->limit);
    vectorized::IndexSearchResult index_search_result;
    if (_index_type == "hnsw") {
        vectorized::HNSWSearchParameters hnsw_search_params;
        hnsw_search_params.roaring = param->roaring;
        hnsw_search_params.ef_search = param->_user_params.hnsw_ef_search;
        hnsw_search_params.check_relative_distance =
                param->_user_params.hnsw_check_relative_distance;
        hnsw_search_params.bounded_queue = param->_user_params.hnsw_bounded_queue;
        RETURN_IF_ERROR(_vector_index->ann_topn_search(query_vec, limit, hnsw_search_params,
                                                       index_search_result));
    } else {
        throw Status::NotSupported("Unsupported index type: {}", _index_type);
    }

    DCHECK(index_search_result.roaring != nullptr);
    DCHECK(index_search_result.distances != nullptr);
    DCHECK(index_search_result.row_ids != nullptr);
    param->distance = std::make_unique<std::vector<float>>();
    update_result(index_search_result, *param->distance, *param->roaring);
    param->row_ids = std::move(index_search_result.row_ids);

    return Status::OK();
}

Status AnnIndexReader::range_search(const vectorized::RangeSearchParams& params,
                                    const VectorSearchUserParams& custom_params,
                                    vectorized::RangeSearchResult* result, io::IOContext* io_ctx) {
    SCOPED_TIMER(&(*result->stats).search_costs_ns);
#ifndef BE_TEST
    {
        SCOPED_TIMER(&(*result->stats).load_index_costs_ns);
        RETURN_IF_ERROR(load_index(io_ctx));
    }
#endif
    DCHECK(_vector_index != nullptr);
    vectorized::IndexSearchResult search_result;
    std::unique_ptr<vectorized::IndexSearchParameters> search_param = nullptr;

    if (_index_type == "hnsw") {
        auto hnsw_param = std::make_unique<vectorized::HNSWSearchParameters>();
        hnsw_param->ef_search = custom_params.hnsw_ef_search;
        hnsw_param->check_relative_distance = custom_params.hnsw_check_relative_distance;
        hnsw_param->bounded_queue = custom_params.hnsw_bounded_queue;
        search_param = std::move(hnsw_param);
    } else {
        throw Status::NotSupported("Unsupported index type: {}", _index_type);
    }

    search_param->is_le_or_lt = params.is_le_or_lt;
    search_param->roaring = params.roaring;
    DCHECK(search_param->roaring != nullptr);

    RETURN_IF_ERROR(_vector_index->range_search(params.query_value, params.radius, *search_param,
                                                search_result));

    DCHECK(search_result.roaring != nullptr);
    result->roaring = search_result.roaring;

    if (params.is_le_or_lt == false) {
        DCHECK(search_result.distances == nullptr);
        DCHECK(search_result.row_ids == nullptr);
    }

    if (search_result.row_ids != nullptr) {
        DCHECK(search_result.row_ids->size() == search_result.roaring->cardinality())
                << "Row ids size: " << search_result.row_ids->size()
                << ", roaring size: " << search_result.roaring->cardinality();
        result->row_ids = std::move(search_result.row_ids);
    } else {
        result->row_ids = nullptr;
    }

    if (search_result.distances != nullptr) {
        result->distance = std::move(search_result.distances);
    } else {
        result->distance = nullptr;
    }

    return Status::OK();
}

} // namespace doris::segment_v2