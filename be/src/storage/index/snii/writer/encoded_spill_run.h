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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>

#include "common/status.h"
#include "storage/index/snii/writer/memory_reporter.h"

namespace doris::snii::writer {

struct TermPostings;

struct EncodedRunTerm {
    uint32_t term_id = 0;
    bool has_positions = false;
    // A boundary document can occur in several fragments. Its actual df is
    // determined by the final consumer, not by summing these upper bounds.
    uint64_t document_groups = 0;
    uint64_t tokens = 0;
};

// Private temporary format, independent of the persistent SNII index format.
// A term contains independently based compact-arena fragments; each fragment
// contains <=64 KiB payload blocks with CRCs. Neither a term nor a document has
// to fit in a block. Compaction preserves fragment order without decoding it.
class EncodedRunWriter {
public:
    explicit EncodedRunWriter(MemoryReporter* reporter = nullptr);
    ~EncodedRunWriter();
    // Append mode starts another independently sealed run in the same spool.
    Status open(const std::string& path, bool append = false);
    uint64_t file_offset() const;
    Status begin_term(const EncodedRunTerm& term);
    Status begin_fragment(uint64_t document_groups, uint64_t tokens);
    Status append_payload(std::span<const uint8_t> bytes);
    Status append_token(uint32_t docid, uint32_t position, bool new_document);
    Status end_fragment();
    Status end_term();
    Status write_term(uint32_t term_id, const TermPostings& postings);
    Status close();

private:
    class Impl;
    MemoryReporter* reporter_;
    MemoryReporter::Reservation metadata_;
    std::unique_ptr<Impl> impl_;
};

// Header-only term lookahead with bounded payload caches. It also reads the
// former raw-u32 temporary format for the explicit RunWriter test/diagnostic
// interface, using independent file cursors rather than whole-term vectors.
class StreamingRunReader {
public:
    static size_t object_bytes();
    explicit StreamingRunReader(MemoryReporter* reporter = nullptr);
    ~StreamingRunReader();
    Status open(const std::string& path, bool has_positions, bool allow_legacy = false,
                uint64_t begin = 0, uint64_t end = UINT64_MAX);
    Status advance();
    const EncodedRunTerm& current() const;
    bool exhausted() const;
    bool term_exhausted() const;
    Status next_token(uint32_t* docid, uint32_t* position, bool* end);
    Status copy_fragments_to(EncodedRunWriter* writer);

private:
    class Impl;
    MemoryReporter* reporter_;
    MemoryReporter::Reservation metadata_;
    std::unique_ptr<Impl> impl_;
};

} // namespace doris::snii::writer
