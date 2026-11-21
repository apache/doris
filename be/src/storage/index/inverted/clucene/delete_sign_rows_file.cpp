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

#include "storage/index/inverted/clucene/delete_sign_rows_file.h"

#include <CLucene.h> // IWYU pragma: keep

#include <memory>
#include <roaring/roaring.hh>
#include <stdexcept>

#include "common/cast_set.h"
#include "storage/index/index_common.h"
#include "storage/index/index_common_impl.h"
#include "storage/index/inverted/common/inverted_index_desc.h"
#include "util/faststring.h"

namespace doris::segment_v2 {

Status write_clucene_delete_sign_rows(lucene::store::Directory* dir, const roaring::Roaring& rows) {
    roaring::Roaring optimized = rows;
    optimized.runOptimize();
    faststring buf;
    buf.resize(optimized.getSizeInBytes(true));
    optimized.write(reinterpret_cast<char*>(buf.data()), true);
    ErrorContext error_context;
    std::unique_ptr<lucene::store::IndexOutput> out;
    try {
        out.reset(dir->createOutput(InvertedIndexDescriptor::get_delete_sign_rows_file_name()));
        out->writeBytes(buf.data(), cast_set<int32_t>(buf.size()));
    } catch (CLuceneError& e) {
        error_context.eptr = std::current_exception();
        error_context.err_msg.append("CLuceneError writing the delete sign rows of an index: ");
        error_context.err_msg.append(e.what());
    }
    FINALLY({ FINALLY_CLOSE(out); })
    return Status::OK();
}

Status read_clucene_delete_sign_rows(lucene::store::Directory* dir, uint32_t num_docs,
                                     std::shared_ptr<const roaring::Roaring>* out) {
    const char* file_name = InvertedIndexDescriptor::get_delete_sign_rows_file_name();
    ErrorContext error_context;
    std::unique_ptr<lucene::store::IndexInput> in;
    faststring buf;
    bool recorded = false;
    try {
        if (dir->fileExists(file_name)) {
            recorded = true;
            in.reset(dir->openInput(file_name));
            buf.resize(cast_set<size_t>(in->length()));
            in->readBytes(buf.data(), cast_set<int32_t>(buf.size()));
        }
    } catch (CLuceneError& e) {
        error_context.eptr = std::current_exception();
        error_context.err_msg.append("CLuceneError reading the delete sign rows of an index: ");
        error_context.err_msg.append(e.what());
    }
    FINALLY({ FINALLY_CLOSE(in); })
    if (!recorded) {
        *out = nullptr;
        return Status::OK();
    }
    std::shared_ptr<const roaring::Roaring> rows;
    try {
        // readSafe checks the bitmap against the bytes the file holds, so a damaged file fails
        // instead of being read past its end.
        rows = std::make_shared<const roaring::Roaring>(
                roaring::Roaring::readSafe(reinterpret_cast<const char*>(buf.data()), buf.size()));
    } catch (const std::runtime_error& e) {
        // CRoaring throws std::runtime_error on a bitmap that does not fit its bytes.
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>(
                "the delete sign rows of an index are corrupted: {}", e.what());
    }
    // As SNII checks its record: a row past the segment would be moved past the rows of the
    // segment by index compaction and counted as hidden by BM25 statistics.
    if (!rows->isEmpty() && rows->maximum() >= num_docs) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>(
                "the delete sign rows of an index are corrupted: row {} of a segment of {} rows",
                rows->maximum(), num_docs);
    }
    *out = std::move(rows);
    return Status::OK();
}

} // namespace doris::segment_v2
