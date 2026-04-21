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

#include "storage/index/ann/ann_index_reader.h"

#include <cstddef>
#include <memory>

#include "common/config.h"
#include "common/metrics/doris_metrics.h"
#include "io/io_common.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "storage/index/ann/ann_index.h"
#include "storage/index/ann/ann_index_iterator.h"
#include "storage/index/ann/ann_index_writer.h"
#include "storage/index/ann/ann_search_params.h"
#include "storage/index/ann/faiss_ann_index.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/inverted/inverted_index_compound_reader.h"
#include "util/once.h"

namespace doris::segment_v2 {
void AnnIndexReader::update_result(const IndexSearchResult& search_result,
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
    _index_type = string_to_ann_index_type(it->second);
    it = index_properties.find("metric_type");
    DCHECK(it != index_properties.end());
    _metric_type = string_to_metric(it->second);
    it = index_properties.find(AnnIndexColumnWriter::DIM);
    DCHECK(it != index_properties.end());
    _dim = std::stoi(it->second);
}

Status AnnIndexReader::new_iterator(std::unique_ptr<IndexIterator>* iterator) {
    *iterator = AnnIndexIterator::create_unique(shared_from_this());
    return Status::OK();
}

Status AnnIndexReader::load_index(io::IOContext* io_ctx) {
    return _load_index_once.call([&]() {
        DorisMetrics::instance()->ann_index_load_cnt->increment(1);

        try {
            // An exception will be thrown if loading fails
            RETURN_IF_ERROR(
                    _index_file_reader->init(config::inverted_index_read_buffer_size, io_ctx));
            auto compound_dir = _index_file_reader->open(&_index_meta, io_ctx);
            if (!compound_dir.has_value()) {
                return Status::IOError("Failed to open index file: {}",
                                       compound_dir.error().to_string());
            }
            _vector_index = std::make_unique<FaissVectorIndex>();
            _vector_index->set_metric(_metric_type);
            _vector_index->set_type(_index_type);
            // Provide a cache key prefix so IVF_ON_DISK can cache ivfdata
            // blocks in StoragePageCache. Use cache_key (which includes
            // index_id) rather than file_path, because the idx file is a
            // compound file shared by multiple indexes.
            static_cast<FaissVectorIndex*>(_vector_index.get())
                    ->set_ivfdata_cache_key_prefix(
                            _index_file_reader->get_index_file_cache_key(&_index_meta));
            RETURN_IF_ERROR(_vector_index->load(compound_dir->get()));
            // Keep the compound directory alive. For IVF_ON_DISK the
            // CachedRandomAccessReader holds a cloned CSIndexInput whose
            // `base` raw pointer references the compound reader's underlying
            // stream. Destroying the directory would make `base` dangling.
            // For other types this is harmless (just holds a file handle).
            _compound_dir = std::move(*compound_dir);
        } catch (CLuceneError& err) {
            LOG_ERROR("Failed to load ann index: {}", err.what());
            return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                    "CLuceneError occur when open ann idx file, error msg: {}", err.what());
        }
        return Status::OK();
    });
}

bool AnnIndexReader::try_load_index(io::IOContext* io_ctx) {
#ifndef BE_TEST
    Status st = load_index(io_ctx);
    if (!st.ok()) {
        LOG_WARNING("Failed to load ann index, will fallback to brute force search: {}",
                    st.to_string());
        return false;
    }
#endif
    return true;
}

Status AnnIndexReader::query(io::IOContext* io_ctx, AnnTopNParam* param, AnnIndexStats* stats) {
    // Index should be loaded before calling query
    DCHECK(_vector_index != nullptr);

    {
        DorisMetrics::instance()->ann_index_search_cnt->increment(1);
        SCOPED_TIMER(&(stats->search_costs_ns));
        DCHECK(_vector_index != nullptr);
        const float* query_vec = param->query_value;
        const int limit = static_cast<int>(param->limit);
        IndexSearchResult index_search_result;
        if (_index_type == AnnIndexType::HNSW) {
            HNSWSearchParameters hnsw_search_params;
            hnsw_search_params.roaring = param->roaring;
            hnsw_search_params.rows_of_segment = param->rows_of_segment;
            hnsw_search_params.io_ctx = io_ctx;
            hnsw_search_params.ef_search = param->_user_params.hnsw_ef_search;
            hnsw_search_params.check_relative_distance =
                    param->_user_params.hnsw_check_relative_distance;
            hnsw_search_params.bounded_queue = param->_user_params.hnsw_bounded_queue;
            RETURN_IF_ERROR(_vector_index->ann_topn_search(query_vec, limit, hnsw_search_params,
                                                           index_search_result));
            // Accumulate detailed engine timings
            stats->engine_search_ns.update(index_search_result.engine_search_ns);
            stats->engine_convert_ns.update(index_search_result.engine_convert_ns);
            stats->engine_prepare_ns.update(index_search_result.engine_prepare_ns);
        } else if (_index_type == AnnIndexType::IVF || _index_type == AnnIndexType::IVF_ON_DISK) {
            IVFSearchParameters ivf_search_params;
            ivf_search_params.roaring = param->roaring;
            ivf_search_params.rows_of_segment = param->rows_of_segment;
            ivf_search_params.io_ctx = io_ctx;
            ivf_search_params.nprobe = param->_user_params.ivf_nprobe;
            RETURN_IF_ERROR(_vector_index->ann_topn_search(query_vec, limit, ivf_search_params,
                                                           index_search_result));
            // Accumulate detailed engine timings
            stats->engine_search_ns.update(index_search_result.engine_search_ns);
            stats->engine_convert_ns.update(index_search_result.engine_convert_ns);
            stats->engine_prepare_ns.update(index_search_result.engine_prepare_ns);
            if (_index_type == AnnIndexType::IVF_ON_DISK) {
                stats->ivf_on_disk_cache_hit_cnt.update(
                        index_search_result.ivf_on_disk_cache_hit_cnt);
                stats->ivf_on_disk_cache_miss_cnt.update(
                        index_search_result.ivf_on_disk_cache_miss_cnt);
                DorisMetrics::instance()->ann_ivf_on_disk_cache_hit_cnt->increment(
                        index_search_result.ivf_on_disk_cache_hit_cnt);
                DorisMetrics::instance()->ann_ivf_on_disk_cache_miss_cnt->increment(
                        index_search_result.ivf_on_disk_cache_miss_cnt);
            }
        } else {
            throw Exception(Status::NotSupported("Unsupported index type: {}",
                                                 ann_index_type_to_string(_index_type)));
        }

        DORIS_CHECK(index_search_result.roaring != nullptr);
        DORIS_CHECK(index_search_result.distances != nullptr);
        DORIS_CHECK(index_search_result.row_ids != nullptr);
        param->distance = std::make_unique<std::vector<float>>();
        {
            SCOPED_TIMER(&(stats->result_process_costs_ns));
            update_result(index_search_result, *param->distance, *param->roaring);
        }
        param->row_ids = std::move(index_search_result.row_ids);
    }

    double search_costs_ms = static_cast<double>(stats->search_costs_ns.value()) / 1000.0;
    DorisMetrics::instance()->ann_index_search_costs_ms->increment(
            static_cast<int64_t>(search_costs_ms));
    if (_index_type == AnnIndexType::IVF_ON_DISK) {
        stats->ivf_on_disk_search_costs_ns.update(stats->search_costs_ns.value());
        stats->ivf_on_disk_search_cnt.update(1);
        DorisMetrics::instance()->ann_ivf_on_disk_search_costs_ms->increment(
                static_cast<int64_t>(search_costs_ms));
        DorisMetrics::instance()->ann_ivf_on_disk_search_cnt->increment(1);
    }
    return Status::OK();
}

Status AnnIndexReader::range_search(const AnnRangeSearchParams& params,
                                    const VectorSearchUserParams& custom_params,
                                    segment_v2::AnnRangeSearchResult* result,
                                    segment_v2::AnnIndexStats* stats, io::IOContext* io_ctx) {
    // Index should be loaded before calling range_search
    DCHECK(_vector_index != nullptr);

    DCHECK(stats != nullptr);
    {
        DorisMetrics::instance()->ann_index_search_cnt->increment(1);
        SCOPED_TIMER(&(stats->search_costs_ns));
        DCHECK(_vector_index != nullptr);
        segment_v2::IndexSearchResult search_result;
        std::unique_ptr<segment_v2::IndexSearchParameters> search_param = nullptr;

        if (_index_type == AnnIndexType::HNSW) {
            auto hnsw_param = std::make_unique<segment_v2::HNSWSearchParameters>();
            hnsw_param->ef_search = custom_params.hnsw_ef_search;
            hnsw_param->check_relative_distance = custom_params.hnsw_check_relative_distance;
            hnsw_param->bounded_queue = custom_params.hnsw_bounded_queue;
            search_param = std::move(hnsw_param);
        } else if (_index_type == AnnIndexType::IVF || _index_type == AnnIndexType::IVF_ON_DISK) {
            auto ivf_param = std::make_unique<segment_v2::IVFSearchParameters>();
            ivf_param->nprobe = custom_params.ivf_nprobe;
            search_param = std::move(ivf_param);
        } else {
            throw Exception(Status::NotSupported("Unsupported index type: {}",
                                                 ann_index_type_to_string(_index_type)));
        }

        search_param->is_le_or_lt = params.is_le_or_lt;
        search_param->roaring = params.roaring;
        search_param->io_ctx = io_ctx;
        DCHECK(search_param->roaring != nullptr);

        RETURN_IF_ERROR(_vector_index->range_search(params.query_value, params.radius,
                                                    *search_param, search_result));
        // Accumulate detailed engine timings
        stats->engine_prepare_ns.update(search_result.engine_prepare_ns);
        stats->engine_search_ns.update(search_result.engine_search_ns);
        stats->engine_convert_ns.update(search_result.engine_convert_ns);
        if (_index_type == AnnIndexType::IVF_ON_DISK) {
            stats->ivf_on_disk_cache_hit_cnt.update(search_result.ivf_on_disk_cache_hit_cnt);
            stats->ivf_on_disk_cache_miss_cnt.update(search_result.ivf_on_disk_cache_miss_cnt);
            DorisMetrics::instance()->ann_ivf_on_disk_cache_hit_cnt->increment(
                    search_result.ivf_on_disk_cache_hit_cnt);
            DorisMetrics::instance()->ann_ivf_on_disk_cache_miss_cnt->increment(
                    search_result.ivf_on_disk_cache_miss_cnt);
        }

        DCHECK(search_result.roaring != nullptr);
        result->roaring = search_result.roaring;

#ifndef NDEBUG
        if (params.is_le_or_lt == false && _metric_type == AnnIndexMetric::L2) {
            DCHECK(search_result.distances == nullptr);
            DCHECK(search_result.row_ids == nullptr);
        }
        if (params.is_le_or_lt == true && _metric_type == AnnIndexMetric::IP) {
            DCHECK(search_result.distances == nullptr);
            DCHECK(search_result.row_ids == nullptr);
        }
#endif

        {
            SCOPED_TIMER(&(stats->result_process_costs_ns));
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
        }
    }

    double search_costs_ms = static_cast<double>(stats->search_costs_ns.value()) / 1000.0;
    DorisMetrics::instance()->ann_index_search_costs_ms->increment(
            static_cast<int64_t>(search_costs_ms));
    if (_index_type == AnnIndexType::IVF_ON_DISK) {
        stats->ivf_on_disk_search_costs_ns.update(stats->search_costs_ns.value());
        stats->ivf_on_disk_search_cnt.update(1);
        DorisMetrics::instance()->ann_ivf_on_disk_search_costs_ms->increment(
                static_cast<int64_t>(search_costs_ms));
        DorisMetrics::instance()->ann_ivf_on_disk_search_cnt->increment(1);
    }

    return Status::OK();
}

size_t AnnIndexReader::get_dimension() const {
    return _dim;
}

} // namespace doris::segment_v2
