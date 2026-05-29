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
#include <string>

#include "gtest/gtest_prod.h"

#include "storage/index/inverted/spimi/fulltext_writer.h"
#include "storage/index/inverted/spimi/posting_buffer.h"
#include "storage/index/inverted/spimi/spill_manager.h"

namespace lucene::store {
class IndexOutput;
}

namespace doris::segment_v2 {
class DorisFSDirectory;

namespace inverted_index::spimi {

// Configuration for the SPIMI segment finalization.
struct SpimiFinishConfig {
    // True for V4 (pure SPIMI) storage format. Controls file naming
    // ("_0.*" vs "_spimi_0.*") and omit_norms behavior.
    bool is_v4 = false;
    // Whether to omit term frequencies and positions in the .frq stream.
    // Derived from the field's parser phrase-support property.
    bool omit_term_freq_and_positions = false;
    // Field name written to the .fnm.
    std::string field_name_utf8;
    // Total doc count for the merged segment's manifest (max doc_id + 1).
    int32_t doc_count = 0;
};

// Facade that encapsulates the full SPIMI write lifecycle within the
// inverted-index column writer. Extracted from InvertedIndexColumnWriter
// to keep the SPIMI-specific logic (buffer management, spill-to-memory,
// k-way merge, segment finalization, and on-disk validation) in one
// place under the spimi/ directory rather than scattered across the
// generic writer template.
//
// Usage:
//   auto writer = std::make_unique<SpimiIndexWriter>(field_name_utf8);
//   // During add_values:
//   writer->AppendToken(term, doc_id, position);
//   if (writer->ShouldFlush()) writer->FlushPending(doc_count);
//   // During finish:
//   writer->Finish(dir, config);
//   // On error:
//   writer->Cleanup();
class SpimiIndexWriter {
    FRIEND_TEST(SpimiIndexWriterTest, GetFileNamesV4);
    FRIEND_TEST(SpimiIndexWriterTest, GetFileNamesShadow);
public:
    explicit SpimiIndexWriter(std::string field_name);

    SpimiIndexWriter(const SpimiIndexWriter&) = delete;
    SpimiIndexWriter& operator=(const SpimiIndexWriter&) = delete;

    // --- Buffer access ---

    SpimiPostingBuffer* buffer() { return _buffer.get(); }
    const SpimiPostingBuffer* buffer() const { return _buffer.get(); }

    // Append one token occurrence. Thin wrapper over buffer->Append().
    void AppendToken(std::string_view term, uint32_t doc_id, uint32_t position);

    // Returns true if the buffer's hard limits have been reached and
    // no more Appends will succeed.
    bool Saturated() const;

    // Returns true when the buffer exceeded its soft memory budget
    // and should be flushed to a spill segment.
    bool ShouldFlush() const;

    // --- Spill lifecycle ---

    // Sorts the buffer, emits its contents as an in-memory spill
    // segment, and resets the buffer for the next batch.
    void FlushPending(int32_t doc_count);

    // --- Memory accounting ---

    // Resident bytes of the posting buffer + all spill segments.
    int64_t MemoryUsage() const;

    // Returns true if the writer has an active buffer (not cleaned up).
    bool HasBuffer() const { return _buffer != nullptr; }

    // Returns the spill manager (for SpillCount queries etc).
    SpillManager* spill_manager() { return _spill_manager.get(); }

    // --- Cleanup ---

    // Releases all in-memory spill data and resets the buffer.
    // Safe to call multiple times.
    void Cleanup();

    // --- Finalization ---

    // Emits the final merged segment into `dir`.
    //
    // If spill segments exist (or the buffer ShouldFlush), flushes
    // remaining buffer and performs a k-way merge. Otherwise, emits
    // the buffer directly as a single segment.
    //
    // Creates the eight segment files (.tis/.tii/.frq/.prx/.fnm/.nrm/
    // segments_N/segments.gen) in `dir`, writes through
    // IndexOutputByteOutput adapters, and validates on-disk byte
    // counts against what SPIMI intended to write.
    //
    // After Finish(), the internal buffer is reset.
    //
    // Throws CLuceneError on I/O failure; doris::Exception from
    // DORIS_CHECK on data corruption. The caller catches and routes
    // through the standard ErrorContext + FINALLY path.
    void Finish(DorisFSDirectory* dir, const SpimiFinishConfig& config);

private:
    // Derives segment file names based on V4 vs shadow mode.
    struct FileNames {
        const char* tis;
        const char* tii;
        const char* frq;
        const char* prx;
        const char* fnm;
        const char* nrm;
        const char* seg_n;
        const char* seg_gen;
    };
    static FileNames GetFileNames(bool is_v4);

    // Creates eight IndexOutput streams in `dir`.
    struct OutputStreams {
        std::unique_ptr<lucene::store::IndexOutput> tis;
        std::unique_ptr<lucene::store::IndexOutput> tii;
        std::unique_ptr<lucene::store::IndexOutput> frq;
        std::unique_ptr<lucene::store::IndexOutput> prx;
        std::unique_ptr<lucene::store::IndexOutput> fnm;
        std::unique_ptr<lucene::store::IndexOutput> nrm;
        std::unique_ptr<lucene::store::IndexOutput> seg_n;
        std::unique_ptr<lucene::store::IndexOutput> seg_gen;
    };
    static OutputStreams CreateOutputStreams(DorisFSDirectory* dir, const FileNames& names);

    // Emits directly from buffer (no spills).
    // Returns per-stream byte counts for post-close validation.
    EmittedSegmentByteCounts EmitDirect(const OutputStreams& streams,
                                        const SpimiFinishConfig& config);

    // Flushes remaining buffer + k-way merges all spills.
    void EmitMerged(const OutputStreams& streams, const SpimiFinishConfig& config);

    std::string _field_name;
    std::unique_ptr<SpimiPostingBuffer> _buffer;
    std::unique_ptr<SpillManager> _spill_manager;
};

} // namespace inverted_index::spimi
} // namespace doris::segment_v2
