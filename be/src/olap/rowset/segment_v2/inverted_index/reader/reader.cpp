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
#include "util/faststring.h"

namespace doris::segment_v2::inverted_index {

Status InvertedIndexReader::init_index_reader() {
    if (_inited) {
        return Status::OK();
    }
    auto close_directory = true;
    lucene::index::IndexReader* reader = nullptr;
    try {
        bool open_idx_file_cache = true;
        auto st = _inverted_index_file_reader->init(config::inverted_index_read_buffer_size,
                                                    open_idx_file_cache);
        if (!st.ok()) {
            LOG(WARNING) << st;
            return st;
        }
        auto result = DORIS_TRY(_inverted_index_file_reader->open(&_index_meta));
        auto* directory = result.release();
        reader = lucene::index::IndexReader::open(
                directory, config::inverted_index_read_buffer_size, close_directory);
        _CLDECDELETE(directory)
    } catch (const CLuceneError& e) {
        std::string msg = "InvertedIndexReader init_index_reader error: " + std::string(e.what());
        if (e.number() == CL_ERR_EmptyIndexSegment) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>(msg);
        }
        return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(msg);
    }
    _index_reader = std::shared_ptr<lucene::index::IndexReader>(reader);
    _inited = true;
    return Status::OK();
}

Result<std::shared_ptr<roaring::Roaring>> InvertedIndexReader::read_null_bitmap() {
    lucene::store::IndexInput* null_bitmap_in = nullptr;
    std::shared_ptr<roaring::Roaring> null_bitmap = std::make_shared<roaring::Roaring>();
    try {
        const char* null_bitmap_file_name =
                InvertedIndexDescriptor::get_temporary_null_bitmap_file_name();
        auto* dir = _index_reader->directory();
        if (dir->fileExists(null_bitmap_file_name)) {
            null_bitmap_in = dir->openInput(null_bitmap_file_name);
            size_t null_bitmap_size = null_bitmap_in->length();
            faststring buf;
            buf.resize(null_bitmap_size);
            null_bitmap_in->readBytes(buf.data(), null_bitmap_size);
            *null_bitmap = roaring::Roaring::read(reinterpret_cast<char*>(buf.data()), false);
            null_bitmap->runOptimize();
            FINALIZE_INPUT(null_bitmap_in);
        }
    } catch (CLuceneError& e) {
        FINALLY_FINALIZE_INPUT(null_bitmap_in);
        return ResultError(Status::Error<doris::ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                "Inverted index read null bitmap error occurred, reason={}", e.what()));
    }
    return null_bitmap;
}

} // namespace doris::segment_v2::inverted_index