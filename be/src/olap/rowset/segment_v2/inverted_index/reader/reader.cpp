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

#include "olap/rowset/segment_v2/inverted_index/reader/reader.h"

#include "CLucene.h"

namespace doris::segment_v2::inverted_index {

Status InvertedIndexReader::init_index_reader() {
    auto close_directory = true;
    lucene::index::IndexReader* reader = nullptr;
    try {
        auto result = DORIS_TRY(_inverted_index_file_reader->open(&_index_meta));
        auto directory = result.release();
        reader = lucene::index::IndexReader::open(
                directory, config::inverted_index_read_buffer_size, close_directory);
        _CLDECDELETE(directory)
    } catch (const CLuceneError& e) {
        std::string msg = "FulltextIndexSearcherBuilder build error: " + std::string(e.what());
        if (e.number() == CL_ERR_EmptyIndexSegment) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>(msg);
        }
        return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(msg);
    }
    _index_reader = std::shared_ptr<lucene::index::IndexReader>(reader);
    return Status::OK();
}

} // namespace doris::segment_v2::inverted_index