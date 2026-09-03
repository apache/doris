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
#include <span>

#include "common/status.h"
#include "storage/index/snii/format/prx_pod.h"

namespace doris::snii {
namespace format {
struct DictEntry;
}
namespace io {
class FileWriter;
}
namespace writer {

class MemoryReporter;

// Synchronously borrowed posting arrays for one canonical posting window.
// position_offsets has docids.size()+1 entries when positions are present. The
// offsets may start above zero when the view borrows a sub-window; positions_flat
// covers exactly position_offsets.back()-position_offsets.front() values.
struct PostingRunView {
    std::span<const uint32_t> docids;
    std::span<const uint32_t> freqs;
    std::span<const uint32_t> position_offsets;
    std::span<const uint32_t> positions_flat;
};

struct TermAggregateStats {
    uint32_t df = 0;
    uint64_t total_freq = 0;
    uint32_t max_freq = 0;
};

// CommonGrams entries define semantic term frequency from documents or
// positions rather than the transient physical frequency array.
enum class TermFrequencySource : uint8_t {
    kFrequenciesOrDocuments,
    kDocuments,
    kPositions,
};

struct WindowEmitterOptions {
    io::FileWriter* posting_out = nullptr;
    uint64_t posting_region_offset = 0;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
    std::span<const uint8_t> encoded_norms;
    bool has_freq = false;
    bool has_prx = false;
    int prx_zstd_level = 3;
    format::PrxWindowLimits prx_window_limits = format::kReaderPrxWindowLimits;
    TermFrequencySource term_frequency_source = TermFrequencySource::kFrequenciesOrDocuments;
    MemoryReporter* memory_reporter = nullptr;
};

// The single owner of windowed DD/frequency/PRX encoding and prelude metadata.
// A failed emit poisons the instance; finish_term cannot publish a partial term.
class WindowEmitter {
public:
    explicit WindowEmitter(WindowEmitterOptions options);
    ~WindowEmitter();

    WindowEmitter(const WindowEmitter&) = delete;
    WindowEmitter& operator=(const WindowEmitter&) = delete;
    WindowEmitter(WindowEmitter&&) = delete;
    WindowEmitter& operator=(WindowEmitter&&) = delete;

    Status emit_window(const PostingRunView& window);
    Status finish_term(format::DictEntry* entry, TermAggregateStats* stats);

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

// Process-global test observability for the one emitter choke point and the
// existing bounded-work counters. Reset between tests.
namespace testing {
void note_window_norm_doc_visits(uint64_t count);
uint64_t window_norm_doc_visits();
void reset_window_norm_doc_visits();
void note_window_freq_doc_visits();
uint64_t window_freq_doc_visits();
void reset_window_freq_doc_visits();
uint64_t window_emitter_finished_terms();
uint64_t window_emitter_physical_windows();
void reset_window_emitter_counters();
} // namespace testing

} // namespace writer
} // namespace doris::snii
