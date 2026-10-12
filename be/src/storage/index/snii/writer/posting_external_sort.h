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

#include <cstdint>
#include <memory>
#include <vector>

#include "storage/index/snii/writer/posting_byte_buffer.h"

namespace doris::snii::writer {

// Stable external sort for the compatibility API's out-of-order document ids.
// Production ingestion already supplies ascending ids. Sorting token records
// rather than document position vectors also bounds a revisited long document.
// All sorted chunks share one temporary file; merge passes use two cursors and
// one output file, so the number of open files and resident heads is constant.
class SortedPostingTokens {
public:
    explicit SortedPostingTokens(MemoryReporter* reporter);
    Status append(uint32_t document, uint32_t position);
    Status finish();
    Status next(uint32_t* document, uint32_t* position, bool* end);
    uint64_t document_count() const { return documents_; }
    uint64_t token_count() const { return tokens_; }

private:
    struct Token {
        uint32_t document;
        uint32_t position;
        uint32_t ordinal;
    };
    static constexpr size_t kChunkTokens = 16 * 1024;
    Status flush_chunk();
    Status merge_pass(uint64_t run_tokens, PostingByteBuffer* output);

    MemoryReporter* reporter_;
    MemoryReporter::Reservation reservation_;
    std::vector<Token> chunk_;
    std::unique_ptr<PostingByteBuffer> sorted_;
    std::unique_ptr<PostingByteCursor> cursor_;
    uint64_t tokens_ = 0;
    uint64_t documents_ = 0;
};

} // namespace doris::snii::writer
