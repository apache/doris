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
}

Status SniiReadWave::fetch() {
    if (_open == nullptr) {
        DORIS_CHECK(_completions.empty());
        return Status::OK();
    }
    std::unique_ptr<io::BatchRangeFetcher> round = std::move(_open);
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
                                       SniiReadWave* wave)
        : _idx(idx),
          _entry(std::move(entry)),
          _frq_base(frq_base),
          _prx_base(prx_base),
          _positions_wanted(positions),
          _scoring(scoring),
          _norms(norms),
          _wave(wave),
          _kind(_posting_kind(_entry)) {}

SniiPostingsCursor::~SniiPostingsCursor() {
    if (_wave != nullptr) {
        _wave->drop(this);
    }
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

Status SniiPostingsCursor::open_prelude() {
    if (_kind != Kind::kWindowed) {
        return Status::OK();
    }
    return _read_prelude();
}

Status SniiPostingsCursor::open() {
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
        if (_prelude_pending) {
            // The windows are known only once the prelude arrives; the span follows it.
            RETURN_IF_ERROR(_wave->fetch());
        }
        RETURN_IF_ERROR(_read_span(_wants_prx()));
        break;
    }
    _opened = true;
    return Status::OK();
}

// Opens the span unless a prefetch chose the windows, and fetches the wave when this cursor's
// bytes are still on it.
Status SniiPostingsCursor::_ensure_ready() {
    if (!_opened) {
        RETURN_IF_ERROR(open());
    }
    if (_wave != nullptr && _wave->pending()) {
        RETURN_IF_ERROR(_wave->fetch());
    }
    return Status::OK();
}

Status SniiPostingsCursor::prefetch(const std::vector<uint32_t>* candidates, bool positions) {
    if (_kind != Kind::kWindowed) {
        // One round holds the whole posting.
        if (!_opened) {
            RETURN_IF_ERROR(open());
        }
        return Status::OK();
    }
    const bool prx = positions && _wants_prx();
    RETURN_IF_ERROR(_read_prelude());
    if (_prelude_pending) {
        RETURN_IF_ERROR(_wave->fetch());
    }
    if (candidates == nullptr ||
        scan_all_windows(_idx, _entry.df, _window_count, candidates->size())) {
        RETURN_IF_ERROR(_read_span(prx));
    } else {
        std::vector<uint32_t> windows;
        _prelude->select_covering_windows(*candidates, &windows);
        RETURN_IF_ERROR(_read_windows(windows, prx));
    }
    _opened = true;
    return Status::OK();
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
    const auto parse = [adopt](Slice bytes) -> Status {
        auto prelude = std::make_shared<FrqPreludeReader>();
        RETURN_IF_ERROR(FrqPreludeReader::open(bytes, prelude.get()));
        return adopt(std::move(prelude));
    };
    if (_wave != nullptr) {
        const size_t handle = _wave->batch().add(prelude_abs, _entry.prelude_len);
        _prelude_pending = true;
        _wave->after_fetch(this, [parse, handle](const io::BatchRangeFetcher& batch) {
            return parse(batch.get(handle));
        });
        return Status::OK();
    }
    _rounds.push_back(std::make_unique<io::BatchRangeFetcher>(_idx.reader()));
    io::BatchRangeFetcher& batch = *_rounds.back();
    const size_t handle = batch.add(prelude_abs, _entry.prelude_len);
    RETURN_IF_ERROR(batch.fetch());
    return parse(batch.get(handle));
}

// Reads the whole dd-block and, when asked, the PRX region next to it, in one round.
Status SniiPostingsCursor::_read_span(bool prx) {
    const bool need_dd = !_span_dd_read;
    const bool need_prx = prx && !_span_prx_read;
    if (!need_dd && !need_prx) {
        return Status::OK();
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
    _span_dd_read = _span_dd_read || need_dd;
    _span_prx_read = _span_prx_read || need_prx;
    return _read_pieces(std::move(pieces));
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
    io::BatchRangeFetcher* batch = nullptr;
    if (_wave != nullptr) {
        batch = &_wave->batch();
    } else {
        _rounds.push_back(std::make_unique<io::BatchRangeFetcher>(_idx.reader()));
        batch = _rounds.back().get();
    }
    for (Run& run : runs) {
        run.handle = batch->add(run.offset, run.end - run.offset);
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
    if (_wave != nullptr) {
        _wave->after_fetch(this, slice);
        return Status::OK();
    }
    RETURN_IF_ERROR(batch->fetch());
    return slice(*batch);
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
    RETURN_IF_ERROR(format::read_prx_window_csr(&source, &_pos_flat, &_pos_off));
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
    _norm_values.clear();
    if (_norms != nullptr) {
        _norm_values.resize(_doc_count);
        for (uint32_t i = 0; i < _doc_count; ++i) {
            const uint32_t doc = _dense ? _first_doc + i : _block_docs[i];
            uint8_t norm = 0;
            RETURN_IF_ERROR(_norms->try_encoded_norm(doc, &norm));
            _norm_values[i] = norm;
        }
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
        RETURN_IF_ERROR(_fill_positions());
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
        RETURN_IF_ERROR(_fill_positions());
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

// The whole block is the decoded frame as it is. Fewer than half of its documents decode
// alone; more decode the whole frame once and copy the chosen ones.
Status SniiPostingsCursor::block_positions(std::span<const uint32_t> ordinals,
                                           index_query::PositionsBuffer* buffer,
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
    if (ordinals.size() == _doc_count) {
        *out = {.flat = _pos_flat, .offsets = _pos_off};
        return Status::OK();
    }
    buffer->flat.clear();
    buffer->offsets.assign(1, 0);
    for (const uint32_t ordinal : ordinals) {
        const auto positions = _positions_of(ordinal);
        buffer->flat.insert(buffer->flat.end(), positions.begin(), positions.end());
        buffer->offsets.push_back(static_cast<uint32_t>(buffer->flat.size()));
    }
    *out = {.flat = buffer->flat, .offsets = buffer->offsets};
    return Status::OK();
}

Status SniiPostingsCursor::_decode_selected(std::span<const uint32_t> ordinals,
                                            index_query::BlockPositions* out) {
    RETURN_IF_ERROR(_ensure_prx(_current_window));
    ByteSource source(_windows[_current_window].prx);
    format::PrxDecodedShape shape;
    format::PrxDecodeContext context {.shape = &shape};
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
