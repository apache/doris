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

#include "storage/index/inverted/spimi/posting_store.h"

// clang-format off
#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wshadow-field"
#endif
#include <CLucene/StdHeader.h>
#include <CLucene/store/IndexInput.h>
#ifdef __clang__
#pragma clang diagnostic pop
#endif
// clang-format on

#include <fmt/format.h>

#include <cstring>
#include <string>

#include "storage/index/inverted/spimi/byte_parser_error.h"

namespace doris::segment_v2::inverted_index::spimi {

void MemPostingStore::read_at(int64_t offset, uint8_t* dst, size_t len) {
    if (offset < 0 || static_cast<uint64_t>(offset) > _len ||
        len > _len - static_cast<size_t>(offset)) [[unlikely]] {
        // offset + len > _len (computed without overflow).
        SPIMI_THROW_CORRUPT("SPIMI PostingStore: read_at out of bounds");
    }
    if (len != 0U) {
        std::memcpy(dst, _data + offset, len);
    }
    ++_read_count;
    _bytes_read += static_cast<int64_t>(len);
    _read_log.emplace_back(offset, len);
}

IndexInputPostingStore::IndexInputPostingStore(lucene::store::IndexInput* input)
        : _input(input), _length(input != nullptr ? input->length() : 0) {}

IndexInputPostingStore::~IndexInputPostingStore() {
    if (_input != nullptr) {
        // The clone shares the refcounted file handle; closing+deleting it
        // releases this clone's cursor without touching the shared handle's
        // other clones.
        _input->close();
        _CLDELETE(_input);
    }
}

void IndexInputPostingStore::read_at(int64_t offset, uint8_t* dst, size_t len) {
    if (offset < 0 || offset > _length || static_cast<int64_t>(len) > _length - offset)
            [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI PostingStore: read_at out of bounds");
    }
    if (len == 0U) {
        return;
    }
    try {
        // BufferedIndexInput::seek + readBytes -> readInternal -> Doris
        // read_at + FILE_BLOCK_CACHE (S3 range-GET). The shared handle's
        // read_at is internally mutex-guarded (`_shared_lock`).
        _input->seek(offset);
        _input->readBytes(dst, static_cast<int32_t>(len));
    } catch (const CLuceneError& e) {
        // Build the message with fmt (not the THROW_CORRUPT format path) so a
        // brace in `e.what()` is not misread as a format placeholder.
        const std::string msg =
                fmt::format("SPIMI PostingStore: IndexInput read failed: {}", e.what());
        throw ::doris::Exception(::doris::Status::Error<
                                 ::doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(msg));
    }
}

} // namespace doris::segment_v2::inverted_index::spimi
