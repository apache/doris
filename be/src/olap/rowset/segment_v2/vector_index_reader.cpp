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

#include "olap/rowset/segment_v2/vector_index_reader.h"

#include "common/config.h"
#include "common/status.h"
#include "mindann/mindann_id_filter.h"
#include "olap/rowset/segment_v2/mindann/mindann_index_utils.h"
#include "olap/rowset/segment_v2/vector_index_desc.h"
#include "olap/rowset/segment_v2/vector_index_file_reader.h"
#include "runtime/runtime_state.h"
#include "tenann/common/error.h"
#include "tenann/common/seq_view.h"
#include "tenann/factory/ann_searcher_factory.h"
#include "tenann/searcher/id_filter.h"

namespace doris::segment_v2 {
Status VectorIndexReader::init() {
    // todo: support query params
    std::string segment_path = _vector_index_file_reader->get_index_file_dir()/
                                 _vector_index_file_reader->get_segment_file_name();

    std::string index_path = VectorIndexDescriptor::get_index_file_name(segment_path,
        _index_meta->index_id(), _index_meta->get_index_suffix());

    bool path_exists = false;
    RETURN_IF_ERROR(_vector_index_file_reader->get_fs()->exists(index_path, &path_exists));
    if (!path_exists) {
        return Status::NotFound(fmt::format("index path {} not found", index_path));
    }

    RETURN_IF_ERROR(init_index_family());

    if (_index_family == 0) {
        auto meta = get_vector_meta(_index_meta, std::map<std::string, std::string>{});
        _index_type = meta.index_type();
        _metric_type = meta.common_params()[METRIC_TYPE];

        RETURN_IF_ERROR(init_dense_index_searcher(meta, index_path));
    } else {
        RETURN_IF_ERROR(init_sparse_index_searcher(index_path));
    }

    return Status::OK();
}

// 0 for faiss; 1 for milvus
Status VectorIndexReader::init_index_family() {
    string standard_type_string = boost::algorithm::to_lower_copy(get_index_property(INDEX_TYPE));
    if (standard_type_string == "sparse_wand" || standard_type_string == "sparse_inverted_index") {
        _index_family = 1;
    } else {
        _index_family = 0;
    }
    return Status::OK();
}

Status VectorIndexReader::init_dense_index_searcher(const tenann::IndexMeta& meta,
                                                    const std::string& index_path) {
    try {
        // if index is empty, we use empty index searcher
        io::FileSystemSPtr fs = _vector_index_file_reader->get_fs();
        io::FileReaderSPtr index_file_reader = nullptr;
        RETURN_IF_ERROR(fs->open_file(index_path, &index_file_reader));
        size_t file_size = index_file_reader->size();
        size_t bytes_read = 0;

        if (file_size == VectorIndexDescriptor::mark_word_len) {
            char read_buf[file_size];
            RETURN_IF_ERROR(index_file_reader->read_at(0, {read_buf, file_size}, &bytes_read));
            std::string_view buf_str = std::string_view(reinterpret_cast<char*>(read_buf), file_size);
            if (buf_str == VectorIndexDescriptor::mark_word) {
                _use_empty_index_searcher = true;
                return Status::OK();
            }
        }

        RETURN_IF_ERROR(index_file_reader->close());

        // init dense index searcher
        auto meta_copy = meta;
        if (meta.index_type() == tenann::IndexType::kFaissIvfPq) {
            if (config::enable_vector_index_block_cache) {
                // cache index blocks
                meta_copy.index_reader_options()[tenann::IndexReaderOptions::cache_index_file_key] =
                        false;
                meta_copy.index_reader_options()[tenann::IndexReaderOptions::cache_index_block_key]
                        = true;
            } else {
                // cache index file
                meta_copy.index_reader_options()[tenann::IndexReaderOptions::cache_index_file_key] =
                        true;
                meta_copy.index_reader_options()[tenann::IndexReaderOptions::cache_index_block_key]
                        = false;
            }
        } else {
            // cache index file
            meta_copy.index_reader_options()[tenann::IndexReaderOptions::cache_index_file_key] =
                    true;
        }

        tenann::IndexCache::GetGlobalInstance()->SetCapacity(
                config::vector_index_query_cache_capacity);

        _dense_index_searcher = tenann::AnnSearcherFactory::CreateSearcherFromMeta(meta_copy);
        _dense_index_searcher->index_reader()->SetIndexCache(tenann::IndexCache::GetGlobalInstance());
        _dense_index_searcher->ReadIndex(index_path);

        DCHECK(_dense_index_searcher->is_index_loaded());
    } catch (tenann::Error& e) {
        LOG(WARNING) << e.what();
        return Status::InternalError(e.what());
    } catch (tenann::FatalError& e) {
        LOG(WARNING) << e.what();
        return Status::InternalError(e.what());
    }
    return Status::OK();
}

Status VectorIndexReader::init_sparse_index_searcher(const std::string& index_path) {
    float drop_ratio_search = 0;
    string drop_ratio_search_str = get_index_property(sparse::DROP_RATIO_SEARCH);
    if (drop_ratio_search_str != "") {
        drop_ratio_search = std::stof(drop_ratio_search_str);
    }

    size_t refine_factor = 1;
    string refine_factor_str = get_index_property(sparse::REFINE_FACTOR);
    if (refine_factor_str != "") {
        refine_factor = std::stoul(refine_factor_str);
    }

    io::FileSystemSPtr fs = _vector_index_file_reader->get_fs();
    _sparse_index_searcher = std::make_shared<sparse::SparseInvertedIndexSearcher>(
            fs, index_path, drop_ratio_search, refine_factor);
    RETURN_IF_ERROR(_sparse_index_searcher->init());

    return Status::OK();
}

Status VectorIndexReader::search(std::vector<int32_t>* query_vector_id,
                                 std::vector<float>* query_vector_val,
                                 int k,
                                 int64_t* result_ids,
                                 uint8_t* result_distances,
                                 roaring::Roaring& _row_bitmap) {
    if (_index_family == 0) {
        try {
            auto query_view = tenann::PrimitiveSeqView{
                .data = reinterpret_cast<uint8_t*>(query_vector_val->data()),
                .size = static_cast<uint32_t>(query_vector_val->size()),
                .elem_type = tenann::PrimitiveType::kFloatType
            };

            MindAnnIdFilter id_filter(&_row_bitmap);

            _dense_index_searcher->AnnSearch(query_view, k, result_ids, result_distances, &id_filter);
        } catch (tenann::Error& e) {
            LOG(WARNING) << e.what();
            return Status::InternalError(e.what());
        } catch (tenann::FatalError& e) {
            LOG(WARNING) << e.what();
            return Status::InternalError(e.what());
        }
    } else if (_index_family == 1) {
        RETURN_IF_ERROR(_sparse_index_searcher->search(
                query_vector_id, query_vector_val, k, result_ids, result_distances, _row_bitmap));
    }

    return Status::OK();
};

Status VectorIndexReader::range_search(std::vector<int32_t>* query_vector_id,
                                       std::vector<float>* query_vector_val,
                                       int k,
                                       std::vector<int64_t>* result_ids,
                                       std::vector<float>* result_distances,
                                       roaring::Roaring& _row_bitmap,
                                       float range,
                                       int order) {
    if (_index_family == 0) {
        try {
            auto query_view = tenann::PrimitiveSeqView{
                .data = reinterpret_cast<uint8_t*>(query_vector_val->data()),
                .size = static_cast<uint32_t>(query_vector_val->size()),
                .elem_type = tenann::PrimitiveType::kFloatType
            };

            int actual_arder;
            if (_metric_type == tenann::MetricType::kL2Distance) {
                actual_arder = 0; //asc
            } else if (_metric_type == tenann::MetricType::kCosineSimilarity ||
                       _metric_type == tenann::MetricType::kInnerProduct) {
                actual_arder = 1;
            } else {
                actual_arder = order;
            }

            MindAnnIdFilter id_filter(&_row_bitmap);

            _dense_index_searcher->RangeSearch(query_view, range, k, tenann::AnnSearcher::ResultOrder(actual_arder),
                                   result_ids, result_distances, &id_filter);
        } catch (tenann::Error& e) {
            LOG(WARNING) << e.what();
            return Status::InternalError(e.what());
        } catch (tenann::FatalError& e) {
            LOG(WARNING) << e.what();
            return Status::InternalError(e.what());
        }
    } else if (_index_family == 1) {
        return Status::InternalError("sparse vector index do not support range search");
    }

    return Status::OK();
}

bool VectorIndexReader::is_dense_vector_index() {
    return _index_family == 0;
}

Status VectorIndexReader::new_iterator(OlapReaderStatistics* stats, RuntimeState* runtime_state,
                                       std::unique_ptr<VectorIndexIterator>* iterator) {
    *iterator = VectorIndexIterator::create_unique(shared_from_this());
    return Status::OK();
}

string VectorIndexReader::get_index_property(string key) {
    const std::map<string, string>& index_params = _index_meta->properties();

    auto it = index_params.find(key);
    if (it != index_params.end()) {
        return it->second;
    }
    return "";
}


//============= VectorIndexIterator =============


Status VectorIndexIterator::search(std::vector<int32_t>* query_vector_id,
                                   std::vector<float>* query_vector_val,
                                   int k,
                                   int64_t* result_ids,
                                   uint8_t* result_distances,
                                   roaring::Roaring& _row_bitmap) {

    return _reader->search(query_vector_id,
                           query_vector_val,
                           k,
                           result_ids,
                           result_distances,
                           _row_bitmap);
};

Status VectorIndexIterator::range_search(std::vector<int32_t>* query_vector_id,
                                         std::vector<float>* query_vector_val,
                                         int k,
                                         std::vector<int64_t>* result_ids,
                                         std::vector<float>* result_distances,
                                         roaring::Roaring& _row_bitmap,
                                         float range,
                                         int order) {

    return _reader->range_search(query_vector_id,
                                 query_vector_val,
                                 k,
                                 result_ids,
                                 result_distances,
                                 _row_bitmap,
                                 range,
                                 order);
}

bool VectorIndexIterator::is_dense_vector_index() {
    return _reader->is_dense_vector_index();
}

void VectorIndexIterator::set_approx_vector_distance_proj_iterator(
        std::shared_ptr<v_proj::ApproxDistanceVirtualProjColumnIterator> iter) {
    _approx_vector_distance_v_proj_col_iter = iter;
}

void VectorIndexIterator::set_rowid_to_distance_map(
        std::unordered_map<segment_v2::rowid_t, float> id2distance_map) {
    if (_approx_vector_distance_v_proj_col_iter != nullptr) {
        _approx_vector_distance_v_proj_col_iter->set_rowid_to_distance_map(
                std::move(id2distance_map));
    }
}

bool VectorIndexIterator::is_empty_index_iterator() {
    return _reader->_use_empty_index_searcher;
}

int VectorIndexIterator::get_index_type() {
    return _reader->_index_type;
}

}