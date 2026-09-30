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

#include "storage/index/snii/reader/snii_postings_cursor.h"

#include <algorithm>
#include <utility>

#include "common/check.h"
#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/format/frq_pod.h"
#include "storage/index/snii/format/prx_decode_stats.h"
#include "storage/index/snii/format/prx_pod.h"
#include "storage/index/snii/reader/windowed_posting.h"

namespace doris::snii::reader {

using format::DictEntryEnc;
using format::DictEntryKind;
using format::FrqPreludeReader;
using format::WindowMeta;

namespace {

Status posting_corrupted(const char* message) {
    return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(message);
}

} // namespace

io::BatchRangeFetcher& SniiReadWave::batch() {
    if (_open == nullptr) {
        _open = std::make_unique<io::BatchRangeFetcher>(_reader);
    }
    return *_open;
}

void SniiReadWave::after_fetch(const void* owner,
                               std::function<Status(const io::BatchRangeFetcher&)> completion) {
    _completions.push_back({.owner = owner, .run = std::move(completion)});
}

void SniiReadWave::drop(const void* owner) {
    std::erase_if(_completions,
                  [owner](const Completion& completion) { return completion.owner == owner; });
    // Every read on the open round came with a completion, so a round none waits for is dropped.
    if (_completions.empty()) {
        _open.reset();
    }
}

Status SniiReadWave::fetch() {
    if (_open == nullptr) {
        DORIS_CHECK(_completions.empty());
        return Status::OK();
    }
    std::unique_ptr<io::BatchRangeFetcher> round(_open.release());
    std::vector<Completion> completions;
    completions.swap(_completions);
    if (round->pending() > 0) {
        RETURN_IF_ERROR(round->fetch());
    }
    for (const Completion& completion : completions) {
        RETURN_IF_ERROR(completion.run(*round));
    }
    _rounds.push_back(std::move(round));
    return Status::OK();
}

SniiPostingsCursor::SniiPostingsCursor(const LogicalIndexReader& idx, format::DictEntry entry,
                                       uint64_t frq_base, uint64_t prx_base, bool positions,
                                       bool scoring, const format::NormsPodReader* norms,
                                       SniiReadWave* wave, format::PrxDecodeStats* prx_stats)
        : _idx(idx),
          _entry(std::move(entry)),
          _frq_base(frq_base),
          _prx_base(prx_base),
          _positions_wanted(positions),
          _scoring(scoring),
          _norms(norms),
          _wave(wave),
          _prx_stats(prx_stats),
          _kind(_posting_kind(_entry)) {
    if (_wave == nullptr) {
        _own_wave = std::make_unique<SniiReadWave>(_idx.reader());
        _wave = _own_wave.get();
    }
}

SniiPostingsCursor::~SniiPostingsCursor() {
    _wave->drop(this);
}

SniiPostingsCursor::Kind SniiPostingsCursor::_posting_kind(const format::DictEntry& entry) {
    if (entry.kind == DictEntryKind::kInline) {
        return Kind::kInline;
    }
    return entry.enc == DictEntryEnc::kWindowed ? Kind::kWindowed : Kind::kSlim;
}

void SniiPostingsCursor::set_prelude(std::shared_ptr<const FrqPreludeReader> prelude) {
    _prelude = std::move(prelude);
}

Status SniiPostingsCursor::prepare() {
    if (_kind == Kind::kWindowed) {
        return _read_prelude();
    }
    return _kind == Kind::kSlim ? open() : Status::OK();
}

Status SniiPostingsCursor::open() {
    if (_opened) {
        return Status::OK();
    }
    switch (_kind) {
    case Kind::kInline: {
        if (_entry.dd_meta.disk_len > _entry.frq_bytes.size()) {
            return posting_corrupted("snii postings: inline dd region exceeds frq bytes");
        }
        _windows.assign(1, WindowBytes {});
        _windows[0].dd =
                Slice(_entry.frq_bytes.data(), static_cast<size_t>(_entry.dd_meta.disk_len));
        _windows[0].dd_available = true;
        _windows[0].prx = Slice(_entry.prx_bytes);
        _windows[0].prx_available = true;
        _window_count = 1;
        break;
    }
    case Kind::kSlim:
        RETURN_IF_ERROR(_open_slim());
        break;
    case Kind::kWindowed:
        RETURN_IF_ERROR(_read_prelude());
        RETURN_IF_ERROR(_read_span(_wants_prx()));
        break;
    }
    _opened = true;
    return _fetch_own();
}

// Opens the span unless a prefetch chose the windows, and fetches the wave when this cursor's
// bytes are still on it.
Status SniiPostingsCursor::_ensure_ready() {
    if (!_opened) {
        RETURN_IF_ERROR(open());
    }
    if (_wave->pending()) {
        RETURN_IF_ERROR(_wave->fetch());
    }
    return Status::OK();
}

Status SniiPostingsCursor::_fetch_own() {
    return _own_wave == nullptr ? Status::OK() : _wave->fetch();
}

Status SniiPostingsCursor::prefetch(const std::vector<uint32_t>* candidates, bool positions) {
    if (_kind != Kind::kWindowed) {
        // One round holds the whole posting.
        return open();
    }
    // A scoring cursor decodes every block it lands on, so its frames come with the windows.
    const bool prx = positions ? _wants_prx() : _scores_from_prx();
    RETURN_IF_ERROR(_read_prelude());
    if (candidates == nullptr) {
        RETURN_IF_ERROR(_read_span(prx));
    } else {
        if (_prelude == nullptr) {
            // The windows are known only once the prelude arrives.
            RETURN_IF_ERROR(_wave->fetch());
        }
        std::vector<uint32_t> windows;
        _prelude->select_covering_windows(*candidates, &windows);
        RETURN_IF_ERROR(_read_windows(windows, prx));
    }
    _opened = true;
    return _fetch_own();
}

Status SniiPostingsCursor::rewind() {
    _next_window = 0;
    _current_window = kNoWindow;
    _single_decoded = false;
    _bound_known = false;
    _positions_decoded = false;
    _doc_open = false;
    return Status::OK();
}

Status SniiPostingsCursor::_open_slim() {
    uint64_t dd_off = 0;
    uint64_t dd_len = 0;
    RETURN_IF_ERROR(_idx.resolve_frq_window(_entry, _frq_base, &dd_off, &dd_len));
    _windows.assign(1, WindowBytes {});
    _window_count = 1;
    std::vector<Piece> pieces {{.offset = dd_off, .length = dd_len, .window = 0, .prx = false}};
    if (_wants_prx()) {
        uint64_t prx_off = 0;
        uint64_t prx_len = 0;
        RETURN_IF_ERROR(_idx.resolve_prx_window(_entry, _prx_base, &prx_off, &prx_len));
        pieces.push_back({.offset = prx_off, .length = prx_len, .window = 0, .prx = true});
    }
    return _read_pieces(std::move(pieces));
}

Status SniiPostingsCursor::_read_prelude() {
    if (!_windows.empty() || _prelude_pending) {
        return Status::OK();
    }
    const auto adopt = [this](std::shared_ptr<const FrqPreludeReader> prelude) -> Status {
        if (_wants_prx() && !prelude->has_prx()) {
            return posting_corrupted("snii postings: positions requested but prelude has none");
        }
        _prelude = std::move(prelude);
        _window_count = _prelude->n_windows();
        _windows.assign(_window_count, WindowBytes {});
        _prelude_pending = false;
        return Status::OK();
    };
    if (_prelude != nullptr) {
        return adopt(_prelude);
    }
    uint64_t prelude_abs = 0;
    RETURN_IF_ERROR(prelude_abs_offset(_idx, _entry, _frq_base, &prelude_abs));
    const size_t handle = _wave->batch().add(prelude_abs, _entry.prelude_len);
    _prelude_pending = true;
    _wave->after_fetch(this, [adopt, handle](const io::BatchRangeFetcher& batch) {
        auto prelude = std::make_shared<FrqPreludeReader>();
        RETURN_IF_ERROR(FrqPreludeReader::open(batch.get(handle), prelude.get()));
        return adopt(std::move(prelude));
    });
    return Status::OK();
}

// Reads the whole dd-block and, when asked, the PRX region next to it, in one round.
Status SniiPostingsCursor::_read_span(bool prx) {
    const bool need_dd = !_span_dd_read;
    const bool need_prx = prx && !_span_prx_read;
    if (!need_dd && !need_prx) {
        return Status::OK();
    }
    _span_dd_read = true;
    _span_prx_read = _span_prx_read || need_prx;
    if (_prelude == nullptr) {
        return _read_regions(need_dd, need_prx);
    }
    std::vector<Piece> pieces;
    for (uint32_t w = 0; w < _window_count; ++w) {
        WindowAbsRange range;
        RETURN_IF_ERROR(windowed_window_range(_idx, _entry, _frq_base, _prx_base, *_prelude, w,
                                              need_prx, &range));
        if (need_dd && !_windows[w].dd_available) {
            pieces.push_back({.offset = range.dd_off, .length = range.dd_len, .window = w});
        }
        if (need_prx && !_windows[w].prx_available) {
            pieces.push_back(
                    {.offset = range.prx_off, .length = range.prx_len, .window = w, .prx = true});
        }
    }
    return _read_pieces(std::move(pieces));
}

Status SniiPostingsCursor::_read_regions(bool dd, bool prx) {
    DCHECK(_prelude_pending);
    uint64_t dd_off = 0;
    uint64_t dd_len = 0;
    RETURN_IF_ERROR(_idx.resolve_frq_window(_entry, _frq_base, &dd_off, &dd_len));
    uint64_t prx_off = 0;
    uint64_t prx_len = 0;
    if (prx) {
        RETURN_IF_ERROR(_idx.resolve_prx_window(_entry, _prx_base, &prx_off, &prx_len));
    }
    io::BatchRangeFetcher& batch = _wave->batch();
    const size_t dd_handle = dd ? batch.add(dd_off, dd_len) : 0;
    const size_t prx_handle = prx ? batch.add(prx_off, prx_len) : 0;
    // The prelude's completion, registered before this one, has parsed it by now.
    const auto slice = [this, dd, prx, dd_handle, prx_handle, dd_off,
                        prx_off](const io::BatchRangeFetcher& fetched) -> Status {
        for (uint32_t w = 0; w < _window_count; ++w) {
            WindowAbsRange range;
            RETURN_IF_ERROR(windowed_window_range(_idx, _entry, _frq_base, _prx_base, *_prelude, w,
                                                  prx, &range));
            WindowBytes& window = _windows[w];
            if (dd) {
                window.dd =
                        fetched.get(dd_handle).subslice(static_cast<size_t>(range.dd_off - dd_off),
                                                        static_cast<size_t>(range.dd_len));
                window.dd_available = true;
            }
            if (prx) {
                window.prx = fetched.get(prx_handle)
                                     .subslice(static_cast<size_t>(range.prx_off - prx_off),
                                               static_cast<size_t>(range.prx_len));
                window.prx_available = true;
            }
        }
        return Status::OK();
    };
    _wave->after_fetch(this, slice);
    return Status::OK();
}

// Reads the docids of `windows` not yet read, and their PRX frames when `prx`, in one round.
Status SniiPostingsCursor::_read_windows(const std::vector<uint32_t>& windows, bool prx) {
    std::vector<Piece> pieces;
    for (const uint32_t w : windows) {
        WindowMeta meta;
        RETURN_IF_ERROR(_prelude->window(w, &meta));
        bool dense = false;
        RETURN_IF_ERROR(is_dense_full_window(meta, w, &dense));
        WindowBytes& bytes = _windows[w];
        if (dense) {
            // A full window lists its range; no read holds its docids.
            bytes.dd_available = true;
        }
        const bool need_dd = !bytes.dd_available;
        const bool need_prx = prx && !bytes.prx_available;
        if (!need_dd && !need_prx) {
            continue;
        }
        WindowAbsRange range;
        RETURN_IF_ERROR(windowed_window_range(_idx, _entry, _frq_base, _prx_base, *_prelude, w,
                                              need_prx, &range));
        if (need_dd) {
            pieces.push_back({.offset = range.dd_off, .length = range.dd_len, .window = w});
        }
        if (need_prx) {
            pieces.push_back(
                    {.offset = range.prx_off, .length = range.prx_len, .window = w, .prx = true});
        }
    }
    return _read_pieces(std::move(pieces));
}

Status SniiPostingsCursor::_read_pieces(std::vector<Piece> pieces) {
    if (pieces.empty()) {
        return Status::OK();
    }
    std::ranges::sort(pieces, {}, &Piece::offset);
    // Pieces within the same-term gap read as one range; each keeps its run and offset in it.
    struct Run {
        uint64_t offset = 0;
        uint64_t end = 0;
        size_t handle = 0;
    };
    std::vector<Run> runs;
    std::vector<size_t> run_of(pieces.size());
    for (size_t i = 0; i < pieces.size(); ++i) {
        const Piece& piece = pieces[i];
        if (runs.empty() || piece.offset > runs.back().end + kSameTermCoalesceGap) {
            runs.push_back({.offset = piece.offset, .end = piece.offset + piece.length});
        } else {
            runs.back().end = std::max(runs.back().end, piece.offset + piece.length);
        }
        run_of[i] = runs.size() - 1;
    }
    io::BatchRangeFetcher& batch = _wave->batch();
    for (Run& run : runs) {
        run.handle = batch.add(run.offset, run.end - run.offset);
    }
    const auto slice = [this, pieces = std::move(pieces), runs = std::move(runs),
                        run_of = std::move(run_of)](const io::BatchRangeFetcher& fetched) {
        for (size_t i = 0; i < pieces.size(); ++i) {
            const Piece& piece = pieces[i];
            const Run& run = runs[run_of[i]];
            const Slice bytes = fetched.get(run.handle)
                                        .subslice(static_cast<size_t>(piece.offset - run.offset),
                                                  static_cast<size_t>(piece.length));
            WindowBytes& window = _windows[piece.window];
            if (piece.prx) {
                window.prx = bytes;
                window.prx_available = true;
            } else {
                window.dd = bytes;
                window.dd_available = true;
            }
        }
        return Status::OK();
    };
    _wave->after_fetch(this, slice);
    return Status::OK();
}

// Reads on demand the docids of a window the opening reads did not cover.
Status SniiPostingsCursor::_ensure_dd(uint32_t window) {
    WindowBytes& bytes = _windows[window];
    if (bytes.dd_available) {
        return Status::OK();
    }
    WindowMeta meta;
    RETURN_IF_ERROR(_prelude->window(window, &meta));
    bool dense = false;
    RETURN_IF_ERROR(is_dense_full_window(meta, window, &dense));
    if (!dense) {
        WindowAbsRange range;
        RETURN_IF_ERROR(windowed_window_range(_idx, _entry, _frq_base, _prx_base, *_prelude, window,
                                              /*want_positions=*/false, &range));
        auto& buffer = _on_demand.emplace_back();
        RETURN_IF_ERROR(
                _idx.reader()->read_at(range.dd_off, static_cast<size_t>(range.dd_len), &buffer));
        bytes.dd = Slice(buffer);
    }
    bytes.dd_available = true;
    return Status::OK();
}

// Reads on demand the PRX frame of a window the opening reads did not cover.
Status SniiPostingsCursor::_ensure_prx(uint32_t window) {
    WindowBytes& bytes = _windows[window];
    if (bytes.prx_available) {
        return Status::OK();
    }
    WindowAbsRange range;
    RETURN_IF_ERROR(windowed_window_range(_idx, _entry, _frq_base, _prx_base, *_prelude, window,
                                          /*want_positions=*/true, &range));
    auto& buffer = _on_demand.emplace_back();
    RETURN_IF_ERROR(
            _idx.reader()->read_at(range.prx_off, static_cast<size_t>(range.prx_len), &buffer));
    bytes.prx = Slice(buffer);
    bytes.prx_available = true;
    return Status::OK();
}

index_query::PostingsBlock SniiPostingsCursor::_block_view() const {
    index_query::PostingsBlock block;
    if (_dense) {
        block.dense = true;
        block.range_begin = _first_doc;
        block.range_end = _first_doc + _doc_count;
    } else {
        block.docs = _block_docs;
    }
    if (_scoring) {
        block.freqs = _freqs;
        if (!_norm_values.empty()) {
            block.norms = _norm_values;
        }
    }
    return block;
}

// Decodes the current block's PRX frame once, for its positions and, when scoring, its
// frequencies and norms.
Status SniiPostingsCursor::_fill_positions() {
    const uint32_t window = _current_window;
    RETURN_IF_ERROR(_ensure_prx(window));
    ByteSource source(_windows[window].prx);
    format::PrxDecodeContext context {.stats = _prx_stats};
    RETURN_IF_ERROR(format::read_prx_window_csr(&source, &_pos_flat, &_pos_off, &context));
    if (!source.eof()) {
        return posting_corrupted("snii postings: trailing bytes after prx frame");
    }
    if (_pos_off.size() != static_cast<size_t>(_doc_count) + 1) {
        return posting_corrupted("snii postings: prx/frq doc-count mismatch");
    }
    _positions_decoded = true;
    if (!_scoring) {
        return Status::OK();
    }
    _freqs.resize(_doc_count);
    for (uint32_t i = 0; i < _doc_count; ++i) {
        _freqs[i] = _pos_off[i + 1] - _pos_off[i];
    }
    return _fill_norms();
}

// The norms of the current block's documents, when the index holds norms.
Status SniiPostingsCursor::_fill_norms() {
    _norm_values.clear();
    if (_norms == nullptr) {
        return Status::OK();
    }
    _norm_values.resize(_doc_count);
    for (uint32_t i = 0; i < _doc_count; ++i) {
        const uint32_t doc = _dense ? _first_doc + i : _block_docs[i];
        uint8_t norm = 0;
        RETURN_IF_ERROR(_norms->try_encoded_norm(doc, &norm));
        _norm_values[i] = norm;
    }
    return Status::OK();
}

Status SniiPostingsCursor::_ensure_positions() {
    if (_positions_decoded) {
        return Status::OK();
    }
    return _fill_positions();
}

Status SniiPostingsCursor::_window_docids(uint32_t window, const WindowMeta& meta,
                                          std::span<const uint32_t>* docs) {
    std::vector<uint32_t>* target = &_docs;
    if (_positions_wanted) {
        if (_kept_docs.empty()) {
            _kept_docs.resize(_window_count);
        }
        target = &_kept_docs[window];
        if (!target->empty()) {
            *docs = *target;
            return Status::OK();
        }
    }
    RETURN_IF_ERROR(_ensure_dd(window));
    RETURN_IF_ERROR(format::decode_dd_region(_windows[window].dd, dd_region_meta(meta),
                                             meta.win_base, target));
    if (target->size() != meta.doc_count) {
        return posting_corrupted("snii postings: frq doc_count mismatch");
    }
    *docs = *target;
    return Status::OK();
}

Status SniiPostingsCursor::_decode_window(uint32_t window, index_query::PostingsBlock* block) {
    _doc_open = false;
    WindowMeta meta;
    RETURN_IF_ERROR(_prelude->window(window, &meta));
    uint32_t first = 0;
    RETURN_IF_ERROR(first_docid_in_window(meta, window, &first));
    bool dense = false;
    RETURN_IF_ERROR(is_dense_full_window(meta, window, &dense));
    if (dense) {
        _block_docs = {};
    } else {
        RETURN_IF_ERROR(_window_docids(window, meta, &_block_docs));
    }
    _dense = dense;
    _first_doc = first;
    _last_doc = meta.last_docid;
    _doc_count = meta.doc_count;
    _bound_known = true;
    _positions_decoded = false;
    _current_window = window;
    if (_scoring) {
        RETURN_IF_ERROR(_scores_from_prx() ? _fill_positions() : _fill_norms());
    }
    *block = _block_view();
    return Status::OK();
}

Status SniiPostingsCursor::_decode_single(index_query::PostingsBlock* block) {
    _doc_open = false;
    if (_docs.empty()) {
        RETURN_IF_ERROR(
                format::decode_dd_region(_windows[0].dd, _entry.dd_meta, /*win_base=*/0, &_docs));
        if (_docs.size() != _entry.df) {
            return posting_corrupted("snii postings: posting doc count differs from df");
        }
    }
    _block_docs = _docs;
    _dense = false;
    _first_doc = _docs.empty() ? 0 : _docs.front();
    _last_doc = _docs.empty() ? 0 : _docs.back();
    _doc_count = _docs.size();
    _bound_known = true;
    _positions_decoded = false;
    _current_window = 0;
    if (_scoring) {
        RETURN_IF_ERROR(_scores_from_prx() ? _fill_positions() : _fill_norms());
    }
    *block = _block_view();
    return Status::OK();
}

Status SniiPostingsCursor::next_block(index_query::PostingsBlock* block, bool* eof) {
    *block = {};
    *eof = false;
    RETURN_IF_ERROR(_ensure_ready());
    if (_kind != Kind::kWindowed) {
        if (_single_decoded) {
            *eof = true;
            return Status::OK();
        }
        _single_decoded = true;
        return _decode_single(block);
    }
    if (_next_window >= _window_count) {
        *eof = true;
        return Status::OK();
    }
    RETURN_IF_ERROR(_decode_window(_next_window, block));
    ++_next_window;
    return Status::OK();
}

Status SniiPostingsCursor::seek_block(uint32_t target, index_query::PostingsBlock* block,
                                      bool* eof) {
    RETURN_IF_ERROR(_ensure_ready());
    if (_kind != Kind::kWindowed) {
        // The one block holds every document.
        return next_block(block, eof);
    }
    // A target the next window covers, past the window before it, needs no search.
    if (_next_window < _window_count && target <= _prelude->window_last_docid(_next_window) &&
        (_next_window == 0 || target > _prelude->window_last_docid(_next_window - 1))) {
        return next_block(block, eof);
    }
    bool found = false;
    uint32_t window = 0;
    RETURN_IF_ERROR(_prelude->locate_window(target, &found, &window));
    _next_window = found ? window : _window_count;
    return next_block(block, eof);
}

Status SniiPostingsCursor::shallow_seek(uint32_t target, bool* moved) {
    *moved = false;
    RETURN_IF_ERROR(_ensure_ready());
    if (_kind != Kind::kWindowed) {
        return Status::OK();
    }
    if (_current_window != kNoWindow && target <= _last_doc) {
        return Status::OK();
    }
    bool found = false;
    uint32_t window = 0;
    RETURN_IF_ERROR(_prelude->locate_window(target, &found, &window));
    const uint32_t next = found ? window : _window_count;
    if (_current_window == kNoWindow && next == _next_window) {
        return Status::OK();
    }
    _next_window = next;
    _current_window = kNoWindow;
    *moved = true;
    return Status::OK();
}

uint32_t SniiPostingsCursor::_block_window() const {
    return _current_window != kNoWindow ? _current_window : _next_window;
}

index_query::BlockBound SniiPostingsCursor::current_block_bound() const {
    if (_kind != Kind::kWindowed) {
        return {.last_doc = _last_doc, .last_doc_known = _bound_known};
    }
    const uint32_t window = _block_window();
    if (_prelude == nullptr || window >= _window_count) {
        return {};
    }
    return {.last_doc = _prelude->window_last_docid(window), .last_doc_known = true};
}

std::span<const uint32_t> SniiPostingsCursor::_positions_of(uint32_t ordinal) const {
    DCHECK(_positions_decoded);
    DCHECK_LT(ordinal, _doc_count);
    return std::span<const uint32_t>(_pos_flat).subspan(_pos_off[ordinal],
                                                        _pos_off[ordinal + 1] - _pos_off[ordinal]);
}

Status SniiPostingsCursor::open_positions(uint32_t ordinal, index_query::PositionCursor** out) {
    *out = nullptr;
    if (!_positions_wanted) {
        return Status::NotSupported("This posting type does not support positions");
    }
    DORIS_CHECK(_current_window != kNoWindow);
    RETURN_IF_ERROR(_ensure_positions());
    _doc_positions = _positions_of(ordinal);
    _doc_position_next = 0;
    _doc_open = true;
    *out = this;
    return Status::OK();
}

Status SniiPostingsCursor::append_positions(uint32_t ordinal, uint32_t offset,
                                            std::vector<uint32_t>& output) {
    if (!_positions_wanted) {
        return Status::NotSupported("This posting type does not support positions");
    }
    DORIS_CHECK(_current_window != kNoWindow);
    RETURN_IF_ERROR(_ensure_positions());
    for (const uint32_t position : _positions_of(ordinal)) {
        output.push_back(position + offset);
    }
    return Status::OK();
}

// Fewer than half of the block's documents decode alone; more decode the whole frame once, which
// answers by ordinal as it is.
Status SniiPostingsCursor::block_positions(std::span<const uint32_t> ordinals,
                                           index_query::PositionsBuffer* /*buffer*/,
                                           index_query::BlockPositions* out) {
    if (!_positions_wanted) {
        return Status::NotSupported("This posting type does not support positions");
    }
    DORIS_CHECK(_current_window != kNoWindow);
    DCHECK(ordinals.empty() || ordinals.back() < _doc_count);
    if (!_positions_decoded && ordinals.size() * 2 < _doc_count) {
        return _decode_selected(ordinals, out);
    }
    RETURN_IF_ERROR(_ensure_positions());
    *out = {.flat = _pos_flat, .offsets = _pos_off, .by_ordinal = true};
    return Status::OK();
}

Status SniiPostingsCursor::_decode_selected(std::span<const uint32_t> ordinals,
                                            index_query::BlockPositions* out) {
    RETURN_IF_ERROR(_ensure_prx(_current_window));
    ByteSource source(_windows[_current_window].prx);
    format::PrxDecodedShape shape;
    format::PrxDecodeContext context {.stats = _prx_stats, .shape = &shape};
    RETURN_IF_ERROR(format::read_prx_window_csr_selective(&source, ordinals, &_selected_flat,
                                                          &_selected_off, &context));
    if (!source.eof()) {
        return posting_corrupted("snii postings: trailing bytes after prx frame");
    }
    if (shape.total_docs != _doc_count || _selected_off.size() != ordinals.size() + 1) {
        return posting_corrupted("snii postings: prx/frq doc-count mismatch");
    }
    *out = {.flat = _selected_flat, .offsets = _selected_off};
    return Status::OK();
}

uint32_t SniiPostingsCursor::frequency() const {
    DORIS_CHECK(_doc_open);
    return static_cast<uint32_t>(_doc_positions.size());
}

Status SniiPostingsCursor::next_position(uint32_t* position, bool* available) {
    DORIS_CHECK(_doc_open);
    if (_doc_position_next >= _doc_positions.size()) {
        *available = false;
        return Status::OK();
    }
    *position = _doc_positions[_doc_position_next++];
    *available = true;
    return Status::OK();
}

Status SniiPostingsCursor::finish_doc() {
    DORIS_CHECK(_doc_open);
    _doc_open = false;
    return Status::OK();
}

std::optional<std::span<const uint32_t>> SniiPostingsCursor::view() const {
    DORIS_CHECK(_doc_open);
    return _doc_positions;
}

} // namespace doris::snii::reader
