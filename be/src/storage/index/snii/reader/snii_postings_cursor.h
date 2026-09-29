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
#include <optional>
#include <span>
#include <vector>

#include "common/status.h"
#include "storage/index/query/spi/postings_cursor.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/format/frq_prelude.h"
#include "storage/index/snii/format/norms_pod.h"
#include "storage/index/snii/format/prx_position_iterator.h"
#include "storage/index/snii/io/batch_range_fetcher.h"
#include "storage/index/snii/reader/logical_index_reader.h"

namespace doris::snii::reader {

// The postings of one term as the shared engine reads them: one block per window (the whole
// posting for a slim or inline term), term frequencies as the position counts of the window's
// PRX frame, norms from the norms section, and positions from the frame.
//
// Reads: an inline term needs none. A slim term reads its dd region, and its PRX frame when
// positions or frequencies are asked, in one round. A windowed term reads its prelude unless
// given one, then its whole posting span in one round; given candidates through prefetch, it
// reads only the windows covering them in one batched round, and a window no read covered on
// demand.
class SniiPostingsCursor final : public index_query::PostingsCursor,
                                 public index_query::PositionCursor {
public:
    SniiPostingsCursor(const LogicalIndexReader& idx, format::DictEntry entry, uint64_t frq_base,
                       uint64_t prx_base, bool positions, bool scoring,
                       const format::NormsPodReader* norms);

    // A prelude the caller already read; call before the first read.
    void set_prelude(std::shared_ptr<const format::FrqPreludeReader> prelude);
    // Reads the whole posting span; the block methods call it on first use.
    Status open();

    uint32_t doc_freq() const override { return _entry.df; }
    Status prefetch(const std::vector<uint32_t>& candidates, bool positions) override;
    Status next_block(index_query::PostingsBlock* block, bool* eof) override;
    Status seek_block(uint32_t target, index_query::PostingsBlock* block, bool* eof) override;
    Status shallow_seek(uint32_t target, bool* moved) override;
    index_query::BlockBound current_block_bound() const override;
    Status open_positions(uint32_t ordinal, index_query::PositionCursor** out) override;

    uint32_t frequency() const override;
    Status next_position(uint32_t* position, bool* available) override;
    Status finish_doc() override;
    std::optional<std::span<const uint32_t>> view() const override;

private:
    enum class Kind : uint8_t { kInline, kSlim, kWindowed };
    static constexpr uint32_t kNoWindow = UINT32_MAX;

    static Kind _posting_kind(const format::DictEntry& entry);

    // The bytes of one window, or of the whole slim or inline posting.
    struct WindowBytes {
        Slice dd;
        Slice prx;
        bool dd_available = false;
        bool prx_available = false;
    };

    bool _wants_prx() const { return _positions_wanted || _scoring; }
    io::BatchRangeFetcher& _new_round();
    Status _ensure_open();
    Status _open_slim();
    Status _read_prelude();
    Status _read_span(bool prx);
    Status _read_windows(const std::vector<uint32_t>& windows, bool prx);
    Status _ensure_dd(uint32_t window);
    Status _ensure_prx(uint32_t window);
    Status _decode_window(uint32_t window, index_query::PostingsBlock* block);
    Status _decode_single(index_query::PostingsBlock* block);
    Status _fill_scores(uint32_t window, uint32_t doc_count, uint32_t first_doc, bool dense);
    index_query::PostingsBlock _block_view(bool dense, uint32_t first_doc,
                                           uint64_t doc_count) const;
    uint32_t _block_window() const;

    const LogicalIndexReader& _idx;
    const format::DictEntry _entry;
    const uint64_t _frq_base;
    const uint64_t _prx_base;
    const bool _positions_wanted;
    const bool _scoring;
    const format::NormsPodReader* _norms;
    Kind _kind;

    std::shared_ptr<const format::FrqPreludeReader> _prelude;
    // Every batched round keeps its buffers while the window slices borrow them.
    std::vector<std::unique_ptr<io::BatchRangeFetcher>> _rounds;
    std::vector<std::vector<uint8_t>> _on_demand;
    std::vector<WindowBytes> _windows;
    uint32_t _window_count = 0;
    bool _opened = false;
    bool _span_dd_read = false;
    bool _span_prx_read = false;
    // The window next_block decodes next, and the window the current block came from.
    uint32_t _next_window = 0;
    uint32_t _current_window = kNoWindow;
    bool _single_decoded = false;

    std::vector<uint32_t> _docs;
    std::vector<uint32_t> _freqs;
    std::vector<uint32_t> _norm_values;
    std::vector<uint32_t> _pos_flat;
    std::vector<uint32_t> _pos_off;
    bool _dense = false;
    uint32_t _first_doc = 0;
    uint32_t _last_doc = 0;
    uint64_t _doc_count = 0;
    bool _bound_known = false;

    // Positions: the decoded CSR of a scored block, or the streaming iterator otherwise.
    format::PrxPositionIterator _positions;
    uint32_t _positions_window = kNoWindow;
    bool _csr_positions = false;
    std::span<const uint32_t> _doc_positions;
    size_t _doc_position_next = 0;
    bool _doc_open = false;
};

} // namespace doris::snii::reader
