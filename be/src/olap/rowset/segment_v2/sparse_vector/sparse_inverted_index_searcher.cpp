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

#include "sparse_inverted_index_searcher.h"

#include "io/fs/file_reader.h"
#include "olap/tablet_schema.h"

namespace doris::segment_v2::sparse {

Status SparseInvertedIndexSearcher::init() {
    if (!_file_reader) {
        RETURN_IF_ERROR(_fs->open_file(_index_path, &_file_reader, &io::FileReaderOptions::DEFAULT));
    }

    _index = std::make_shared<sparse::InvertedIndex<true>>();
    RETURN_IF_ERROR(_index->Load(_file_reader));

    return Status::OK();
}

Status SparseInvertedIndexSearcher::search(std::vector<int32_t>* query_vector_id,
                                           std::vector<float>* query_vector_val,
                                           int k,
                                           int64_t* result_ids,
                                           uint8_t* result_distances,
                                           roaring::Roaring& _row_bitmap) {
    size_t elem_size = query_vector_id->size();

    SparseRow query(elem_size);
    for(int i = 0; i < elem_size; i++) {
        query.set_at(i, query_vector_id->at(i), query_vector_val->at(i));
    }

    BitsetView id_filter(_row_bitmap);

    _index->Search(query, k, _drop_ratio_search, reinterpret_cast<float*>(result_distances),
            result_ids, _refine_factor, id_filter, GetDocValueOriginalComputer());

    return Status::OK();
}

void SparseInvertedIndexSearcher::close() {
    if (!_file_reader) {
        return;
    }
    (void)_file_reader->close();
    _file_reader.reset();
}

} // namespace doris::segment_v2::sparse