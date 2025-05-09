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

#include "ann_index_iterator.h"
#include "olap/rowset/segment_v2/inverted_index_compound_reader.h"
#include "olap/rowset/segment_v2/x_index_file_reader.h"
#include "vector/faiss_vector_index.h"
#include "vector/vector_index.h"

namespace doris::segment_v2 {

void AnnIndexReader::update_result(const SearchResult& search_result, std::vector<float>& distance,
                                   roaring::Roaring& row_id) {
    DCHECK(search_result.distances != nullptr);
    DCHECK(search_result.row_ids != nullptr);
    size_t limit = search_result.row_ids->cardinality();
    // Use search result to update distance and row_id
    distance.resize(limit);
    memcpy(distance.data(), search_result.distances.get(), limit * sizeof(float));

    // Use the search result's row_ids to update the input row_id
    // by calculating the union in-place
    row_id &= *search_result.row_ids;
}

AnnIndexReader::AnnIndexReader(const TabletIndex* index_meta,
                               std::shared_ptr<XIndexFileReader> index_file_reader)
        : _index_meta(*index_meta), _index_file_reader(index_file_reader) {}

Status AnnIndexReader::new_iterator(const io::IOContext& io_ctx, OlapReaderStatistics* stats,
                                    RuntimeState* runtime_state,
                                    std::unique_ptr<IndexIterator>* iterator) {
    *iterator = AnnIndexIterator::create_unique(io_ctx, stats, runtime_state, shared_from_this());
    return Status::OK();
}

Status AnnIndexReader::load_index() {
    Result<std::unique_ptr<DorisCompoundReader>> compound_dir =
            _index_file_reader->open(&_index_meta, nullptr);
    if (!compound_dir.has_value()) {
        return Status::IOError("Failed to open index file: {}", compound_dir.error().to_string());
    }
    _vector_index = std::make_unique<FaissVectorIndex>();

    RETURN_IF_ERROR(_vector_index->load(compound_dir->get()));
    return Status::OK();
}

Status AnnIndexReader::query(AnnIndexParam* param) {
    const float* query_vec = param->query_value;
    const int limit = param->limit;
    SearchParameters search_params;
    SearchResult search_result;
    search_params.row_ids = param->roaring;
    RETURN_IF_ERROR(_vector_index->search(query_vec, limit, search_params, search_result));

    update_result(search_result, *param->distance, *param->roaring);

    return Status::OK();
}

} // namespace doris::segment_v2