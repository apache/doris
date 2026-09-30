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
#include <functional>
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
#include "storage/index/snii/format/prx_decode_stats.h"
#include "storage/index/snii/io/batch_range_fetcher.h"
#include "storage/index/snii/reader/logical_index_reader.h"

namespace doris::snii::reader {

// The reads several cursors register between two fetches, issued as one round. Every fetched
// round keeps its buffers while the cursors' slices borrow them.
class SniiReadWave {
public:
    explicit SniiReadWave(io::FileReader* reader) : _reader(reader) {}

    // The batch the next round fetches; ranges added to it are fetched by fetch().
    io::BatchRangeFetcher& batch();
    // Runs `completion` for `owner` after the next fetch, in registration order, with the
    // batch it registered into.
    void after_fetch(const void* owner,
                     std::function<Status(const io::BatchRangeFetcher&)> completion);
    // Forgets the completions of `owner`. Its ranges are still read with the others', and the
    // open round is forgotten once no completion waits for it.
    void drop(const void* owner);
    bool pending() const { return _open != nullptr && _open->pending() > 0; }
    // Issues the registered reads in one round and runs the completions.
    Status fetch();
    // The rounds fetched so far.
    size_t rounds() const { return _rounds.size(); }

private:
    struct Completion {
        const void* owner;
        std::function<Status(const io::BatchRangeFetcher&)> run;
    };

    io::FileReader* _reader;
    std::unique_ptr<io::BatchRangeFetcher> _open;
    std::vector<Completion> _completions;
    std::vector<std::unique_ptr<io::BatchRangeFetcher>> _rounds;
};

// The postings of one term as the shared engine reads them: one block per window (the whole
// posting for a slim or inline term), term frequencies as the position counts of the window's
// PRX frame (one per document on an index without positions), norms from the norms section,
// and positions from the frame, decoded once per window when they are first asked (with the
// frequencies when scoring). A cursor opened with
// positions keeps the docids it decodes, so listing again after a rewind decodes nothing twice.
//
// Reads: an inline term needs none. A slim term reads its dd region, and its PRX frame when
// positions or frequencies are asked, in one round. A windowed term reads its prelude unless
// given one, then its whole posting span in one round; given candidates through prefetch, it
// reads only the windows covering them in one round, and a window no read covered on demand.
// A cursor opened on a wave registers its reads there instead, so several cursors' reads make
// one round; it fetches the wave itself if it needs the bytes before the wave was fetched.
// Given `prx_stats`, it adds the work of every PRX frame it decodes there.
class SniiPostingsCursor final : public index_query::PostingsCursor,
                                 public index_query::PositionCursor {
public:
    SniiPostingsCursor(const LogicalIndexReader& idx, format::DictEntry entry, uint64_t frq_base,
                       uint64_t prx_base, bool positions, bool scoring,
                       const format::NormsPodReader* norms, SniiReadWave* wave = nullptr,
                       format::PrxDecodeStats* prx_stats = nullptr);
    ~SniiPostingsCursor() override;

    // A prelude the caller already read; call before the first read.
    void set_prelude(std::shared_ptr<const format::FrqPreludeReader> prelude);
    // The prelude of a windowed term once read; null before, and for other terms.
    const std::shared_ptr<const format::FrqPreludeReader>& prelude() const { return _prelude; }
    // Reads (or registers) the prelude of a windowed term, so prefetch can select windows.
    Status open_prelude();
    // Whether the prelude this cursor registered on its wave is still to arrive.
    bool prelude_pending() const { return _prelude_pending; }
    // Reads (or registers) the whole posting span; the block methods call it on first use.
    Status open();

    uint32_t doc_freq() const override { return _entry.df; }
    Status prefetch(const std::vector<uint32_t>* candidates, bool positions) override;
    Status rewind() override;
    Status next_block(index_query::PostingsBlock* block, bool* eof) override;
    Status seek_block(uint32_t target, index_query::PostingsBlock* block, bool* eof) override;
    Status shallow_seek(uint32_t target, bool* moved) override;
    index_query::BlockBound current_block_bound() const override;
    Status open_positions(uint32_t ordinal, index_query::PositionCursor** out) override;
    Status append_positions(uint32_t ordinal, uint32_t offset,
                            std::vector<uint32_t>& output) override;
    Status block_positions(std::span<const uint32_t> ordinals, index_query::PositionsBuffer* buffer,
                           index_query::BlockPositions* out) override;

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
    // One region a round reads for a window: its dd region or its PRX frame.
    struct Piece {
        uint64_t offset = 0;
        uint64_t length = 0;
        uint32_t window = 0;
        bool prx = false;
    };

    // Whether the frames are read: for positions, or for the frequencies of a positional
    // index; an index without positions scores one occurrence per document.
    bool _scores_from_prx() const { return _scoring && _idx.has_positions(); }
    bool _wants_prx() const { return _positions_wanted || _scores_from_prx(); }
    Status _fill_norms();
    Status _ensure_ready();
    Status _open_slim();
    Status _read_prelude();
    Status _read_span(bool prx);
    Status _read_windows(const std::vector<uint32_t>& windows, bool prx);
    // Registers the pieces, runs of them within the same-term gap as one range, and slices
    // each piece from its run after the fetch.
    Status _read_pieces(std::vector<Piece> pieces);
    Status _ensure_dd(uint32_t window);
    Status _ensure_prx(uint32_t window);
    Status _decode_window(uint32_t window, index_query::PostingsBlock* block);
    Status _decode_single(index_query::PostingsBlock* block);
    // The docids of a listed window, decoded once per cursor when positions are wanted.
    Status _window_docids(uint32_t window, const format::WindowMeta& meta,
                          std::span<const uint32_t>* docs);
    Status _fill_positions();
    Status _ensure_positions();
    // Decodes the positions of the current block's documents at `ordinals` only.
    Status _decode_selected(std::span<const uint32_t> ordinals, index_query::BlockPositions* out);
    index_query::PostingsBlock _block_view() const;
    uint32_t _block_window() const;
    std::span<const uint32_t> _positions_of(uint32_t ordinal) const;

    const LogicalIndexReader& _idx;
    const format::DictEntry _entry;
    const uint64_t _frq_base;
    const uint64_t _prx_base;
    const bool _positions_wanted;
    const bool _scoring;
    const format::NormsPodReader* _norms;
    SniiReadWave* _wave;
    format::PrxDecodeStats* _prx_stats;
    Kind _kind;

    std::shared_ptr<const format::FrqPreludeReader> _prelude;
    // The rounds this cursor fetched on its own; their buffers back the window slices.
    std::vector<std::unique_ptr<io::BatchRangeFetcher>> _rounds;
    std::vector<std::vector<uint8_t>> _on_demand;
    std::vector<WindowBytes> _windows;
    uint32_t _window_count = 0;
    bool _prelude_pending = false;
    bool _opened = false;
    bool _span_dd_read = false;
    bool _span_prx_read = false;
    // The window next_block decodes next, and the window the current block came from.
    uint32_t _next_window = 0;
    uint32_t _current_window = kNoWindow;
    bool _single_decoded = false;

    std::vector<uint32_t> _docs;
    std::vector<std::vector<uint32_t>> _kept_docs;
    std::span<const uint32_t> _block_docs;
    std::vector<uint32_t> _freqs;
    std::vector<uint32_t> _norm_values;
    std::vector<uint32_t> _pos_flat;
    std::vector<uint32_t> _pos_off;
    std::vector<uint32_t> _selected_flat;
    std::vector<uint32_t> _selected_off;
    bool _dense = false;
    uint32_t _first_doc = 0;
    uint32_t _last_doc = 0;
    uint64_t _doc_count = 0;
    bool _bound_known = false;
    bool _positions_decoded = false;

    // The open document's positions and the next one to hand out.
    std::span<const uint32_t> _doc_positions;
    size_t _doc_position_next = 0;
    bool _doc_open = false;
};

} // namespace doris::snii::reader
