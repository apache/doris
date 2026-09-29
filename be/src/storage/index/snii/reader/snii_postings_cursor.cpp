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

#include <utility>

#include "common/check.h"
#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/format/frq_pod.h"
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

Status range_in_bounds(uint64_t offset, uint64_t length, size_t total, const char* message) {
    if (offset > total || length > total - offset) {
        return posting_corrupted(message);
    }
    return Status::OK();
}

} // namespace

SniiPostingsCursor::SniiPostingsCursor(const LogicalIndexReader& idx, format::DictEntry entry,
                                       uint64_t frq_base, uint64_t prx_base, bool positions,
                                       bool scoring, const format::NormsPodReader* norms)
        : _idx(idx),
          _entry(std::move(entry)),
          _frq_base(frq_base),
          _prx_base(prx_base),
          _positions_wanted(positions),
          _scoring(scoring),
          _norms(norms),
          _kind(_posting_kind(_entry)) {}

SniiPostingsCursor::Kind SniiPostingsCursor::_posting_kind(const format::DictEntry& entry) {
    if (entry.kind == DictEntryKind::kInline) {
        return Kind::kInline;
    }
    return entry.enc == DictEntryEnc::kWindowed ? Kind::kWindowed : Kind::kSlim;
}

void SniiPostingsCursor::set_prelude(std::shared_ptr<const FrqPreludeReader> prelude) {
    _prelude = std::move(prelude);
}

io::BatchRangeFetcher& SniiPostingsCursor::_new_round() {
    _rounds.push_back(std::make_unique<io::BatchRangeFetcher>(_idx.reader(), kSameTermCoalesceGap));
    return *_rounds.back();
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
        RETURN_IF_ERROR(_read_span(_wants_prx()));
        break;
    }
    _opened = true;
    return Status::OK();
}

Status SniiPostingsCursor::_ensure_open() {
    return _opened ? Status::OK() : open();
}

Status SniiPostingsCursor::prefetch(const std::vector<uint32_t>& candidates, bool positions) {
    if (_kind != Kind::kWindowed) {
        // One round holds the whole posting.
        return _ensure_open();
    }
    const bool prx = positions && _wants_prx();
    RETURN_IF_ERROR(_read_prelude());
    if (scan_all_windows(_idx, _entry.df, _window_count, candidates.size())) {
        RETURN_IF_ERROR(_read_span(prx));
    } else {
        std::vector<uint32_t> windows;
        _prelude->select_covering_windows(candidates, &windows);
        RETURN_IF_ERROR(_read_windows(windows, prx));
    }
    _opened = true;
    return Status::OK();
}

Status SniiPostingsCursor::_open_slim() {
    uint64_t dd_off = 0;
    uint64_t dd_len = 0;
    RETURN_IF_ERROR(_idx.resolve_frq_window(_entry, _frq_base, &dd_off, &dd_len));
    io::BatchRangeFetcher& batch = _new_round();
    const size_t dd_handle = batch.add(dd_off, dd_len);
    size_t prx_handle = 0;
    if (_wants_prx()) {
        uint64_t prx_off = 0;
        uint64_t prx_len = 0;
        RETURN_IF_ERROR(_idx.resolve_prx_window(_entry, _prx_base, &prx_off, &prx_len));
        prx_handle = batch.add(prx_off, prx_len);
    }
    RETURN_IF_ERROR(batch.fetch());
    _windows.assign(1, WindowBytes {});
    _windows[0].dd = batch.get(dd_handle);
    _windows[0].dd_available = true;
    if (_wants_prx()) {
        _windows[0].prx = batch.get(prx_handle);
        _windows[0].prx_available = true;
    }
    _window_count = 1;
    return Status::OK();
}

Status SniiPostingsCursor::_read_prelude() {
    if (!_windows.empty()) {
        return Status::OK();
    }
    if (_prelude == nullptr) {
        auto prelude = std::make_shared<FrqPreludeReader>();
        RETURN_IF_ERROR(fetch_windowed_prelude(_idx, _entry, _frq_base, prelude.get()));
        _prelude = std::move(prelude);
    }
    if (_wants_prx() && !_prelude->has_prx()) {
        return posting_corrupted("snii postings: positions requested but prelude has none");
    }
    _window_count = _prelude->n_windows();
    _windows.assign(_window_count, WindowBytes {});
    return Status::OK();
}

// Reads the whole dd-block and, when asked, the PRX region next to it, in one round.
Status SniiPostingsCursor::_read_span(bool prx) {
    const bool need_dd = !_span_dd_read;
    const bool need_prx = prx && !_span_prx_read;
    if (!need_dd && !need_prx) {
        return Status::OK();
    }
    uint64_t prelude_abs = 0;
    RETURN_IF_ERROR(prelude_abs_offset(_idx, _entry, _frq_base, &prelude_abs));
    const uint64_t dd_block_len = _entry.frq_len - _entry.prelude_len;
    if (_prelude->dd_block_len() != dd_block_len) {
        return posting_corrupted("snii postings: dd block does not fill frq region");
    }
    io::BatchRangeFetcher& batch = _new_round();
    size_t dd_handle = 0;
    size_t prx_handle = 0;
    if (need_dd) {
        dd_handle = batch.add(prelude_abs + _entry.prelude_len, dd_block_len);
    }
    if (need_prx) {
        const uint64_t prx_region =
                _idx.section_refs().posting_region.offset + _prx_base + _entry.prx_off_delta;
        prx_handle = batch.add(prx_region, _entry.prx_len);
    }
    RETURN_IF_ERROR(batch.fetch());
    const Slice dd_block = need_dd ? batch.get(dd_handle) : Slice();
    const Slice prx_region = need_prx ? batch.get(prx_handle) : Slice();
    for (uint32_t w = 0; w < _window_count; ++w) {
        WindowMeta meta;
        RETURN_IF_ERROR(_prelude->window(w, &meta));
        if (need_dd) {
            RETURN_IF_ERROR(range_in_bounds(meta.dd_off, meta.dd_disk_len, dd_block.size(),
                                            "snii postings: window dd range out of block"));
            _windows[w].dd = dd_block.subslice(static_cast<size_t>(meta.dd_off),
                                               static_cast<size_t>(meta.dd_disk_len));
            _windows[w].dd_available = true;
        }
        if (need_prx) {
            RETURN_IF_ERROR(range_in_bounds(meta.prx_off, meta.prx_len, prx_region.size(),
                                            "snii postings: window prx range out of region"));
            _windows[w].prx = prx_region.subslice(static_cast<size_t>(meta.prx_off),
                                                  static_cast<size_t>(meta.prx_len));
            _windows[w].prx_available = true;
        }
    }
    _span_dd_read = _span_dd_read || need_dd;
    _span_prx_read = _span_prx_read || need_prx;
    return Status::OK();
}

// Reads the docids of `windows` not yet read, and their PRX frames when `prx`, in one round.
Status SniiPostingsCursor::_read_windows(const std::vector<uint32_t>& windows, bool prx) {
    struct Pending {
        uint32_t window = 0;
        size_t dd_handle = 0;
        size_t prx_handle = 0;
        bool dd = false;
        bool prx = false;
    };
    std::vector<Pending> pending;
    pending.reserve(windows.size());
    io::BatchRangeFetcher* batch = nullptr;
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
        Pending item {.window = w, .dd = !bytes.dd_available, .prx = prx && !bytes.prx_available};
        if (!item.dd && !item.prx) {
            continue;
        }
        WindowAbsRange range;
        RETURN_IF_ERROR(windowed_window_range(_idx, _entry, _frq_base, _prx_base, *_prelude, w,
                                              item.prx, &range));
        if (batch == nullptr) {
            batch = &_new_round();
        }
        if (item.dd) {
            item.dd_handle = batch->add(range.dd_off, range.dd_len);
        }
        if (item.prx) {
            item.prx_handle = batch->add(range.prx_off, range.prx_len);
        }
        pending.push_back(item);
    }
    if (batch == nullptr) {
        return Status::OK();
    }
    RETURN_IF_ERROR(batch->fetch());
    for (const Pending& item : pending) {
        if (item.dd) {
            _windows[item.window].dd = batch->get(item.dd_handle);
            _windows[item.window].dd_available = true;
        }
        if (item.prx) {
            _windows[item.window].prx = batch->get(item.prx_handle);
            _windows[item.window].prx_available = true;
        }
    }
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

index_query::PostingsBlock SniiPostingsCursor::_block_view(bool dense, uint32_t first_doc,
                                                           uint64_t doc_count) const {
    index_query::PostingsBlock block;
    if (dense) {
        block.dense = true;
        block.range_begin = first_doc;
        block.range_end = first_doc + doc_count;
    } else {
        block.docs = _docs;
    }
    if (_scoring) {
        block.freqs = _freqs;
        if (!_norm_values.empty()) {
            block.norms = _norm_values;
        }
    }
    return block;
}

// Decodes the block's PRX frame for its frequencies, and its norms, keeping the positions.
Status SniiPostingsCursor::_fill_scores(uint32_t window, uint32_t doc_count, uint32_t first_doc,
                                        bool dense) {
    ByteSource source(_windows[window].prx);
    RETURN_IF_ERROR(format::read_prx_window_csr(&source, &_pos_flat, &_pos_off));
    if (!source.eof()) {
        return posting_corrupted("snii postings: trailing bytes after prx frame");
    }
    if (_pos_off.size() != static_cast<size_t>(doc_count) + 1) {
        return posting_corrupted("snii postings: prx/frq doc-count mismatch");
    }
    _freqs.resize(doc_count);
    for (uint32_t i = 0; i < doc_count; ++i) {
        _freqs[i] = _pos_off[i + 1] - _pos_off[i];
    }
    _norm_values.clear();
    if (_norms != nullptr) {
        _norm_values.resize(doc_count);
        for (uint32_t i = 0; i < doc_count; ++i) {
            const uint32_t doc = dense ? first_doc + i : _docs[i];
            uint8_t norm = 0;
            RETURN_IF_ERROR(_norms->try_encoded_norm(doc, &norm));
            _norm_values[i] = norm;
        }
    }
    _csr_positions = true;
    _positions_window = window;
    return Status::OK();
}

Status SniiPostingsCursor::_decode_window(uint32_t window, index_query::PostingsBlock* block) {
    if (_doc_open) {
        RETURN_IF_ERROR(finish_doc());
    }
    RETURN_IF_ERROR(_ensure_dd(window));
    WindowMeta meta;
    RETURN_IF_ERROR(_prelude->window(window, &meta));
    uint32_t first = 0;
    RETURN_IF_ERROR(first_docid_in_window(meta, window, &first));
    bool dense = false;
    RETURN_IF_ERROR(is_dense_full_window(meta, window, &dense));
    if (dense) {
        _docs.clear();
    } else {
        RETURN_IF_ERROR(format::decode_dd_region(_windows[window].dd, dd_region_meta(meta),
                                                 meta.win_base, &_docs));
        if (_docs.size() != meta.doc_count) {
            return posting_corrupted("snii postings: frq doc_count mismatch");
        }
    }
    _dense = dense;
    _first_doc = first;
    _last_doc = meta.last_docid;
    _doc_count = meta.doc_count;
    _bound_known = true;
    _csr_positions = false;
    if (_scoring) {
        RETURN_IF_ERROR(_ensure_prx(window));
        RETURN_IF_ERROR(_fill_scores(window, meta.doc_count, first, dense));
    }
    _current_window = window;
    *block = _block_view(dense, first, meta.doc_count);
    return Status::OK();
}

Status SniiPostingsCursor::_decode_single(index_query::PostingsBlock* block) {
    RETURN_IF_ERROR(
            format::decode_dd_region(_windows[0].dd, _entry.dd_meta, /*win_base=*/0, &_docs));
    if (_docs.size() != _entry.df) {
        return posting_corrupted("snii postings: posting doc count differs from df");
    }
    _dense = false;
    _first_doc = _docs.empty() ? 0 : _docs.front();
    _last_doc = _docs.empty() ? 0 : _docs.back();
    _doc_count = _docs.size();
    _bound_known = true;
    _csr_positions = false;
    if (_scoring) {
        RETURN_IF_ERROR(_fill_scores(0, static_cast<uint32_t>(_docs.size()), _first_doc, false));
    }
    _current_window = 0;
    *block = _block_view(false, _first_doc, _docs.size());
    return Status::OK();
}

Status SniiPostingsCursor::next_block(index_query::PostingsBlock* block, bool* eof) {
    *block = {};
    *eof = false;
    RETURN_IF_ERROR(_ensure_open());
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
    RETURN_IF_ERROR(_ensure_open());
    if (_kind != Kind::kWindowed) {
        // The one block holds every document.
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
    RETURN_IF_ERROR(_ensure_open());
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

Status SniiPostingsCursor::open_positions(uint32_t ordinal, index_query::PositionCursor** out) {
    *out = nullptr;
    if (!_positions_wanted) {
        return Status::NotSupported("This posting type does not support positions");
    }
    DORIS_CHECK(_current_window != kNoWindow);
    DORIS_CHECK(ordinal < _doc_count);
    if (_doc_open) {
        RETURN_IF_ERROR(finish_doc());
    }
    if (_csr_positions) {
        _doc_positions = std::span<const uint32_t>(_pos_flat).subspan(
                _pos_off[ordinal], _pos_off[ordinal + 1] - _pos_off[ordinal]);
        _doc_position_next = 0;
    } else {
        if (_positions_window != _current_window) {
            if (_kind == Kind::kWindowed) {
                RETURN_IF_ERROR(_ensure_prx(_current_window));
            }
            RETURN_IF_ERROR(_positions.reset(_windows[_current_window].prx,
                                             static_cast<uint32_t>(_doc_count), {}, nullptr));
            _positions_window = _current_window;
        }
        RETURN_IF_ERROR(_positions.seek(ordinal));
    }
    _doc_open = true;
    *out = this;
    return Status::OK();
}

uint32_t SniiPostingsCursor::frequency() const {
    DORIS_CHECK(_doc_open);
    return _csr_positions ? static_cast<uint32_t>(_doc_positions.size()) : _positions.freq();
}

Status SniiPostingsCursor::next_position(uint32_t* position, bool* available) {
    DORIS_CHECK(_doc_open);
    if (!_csr_positions) {
        return _positions.next_position(position, available);
    }
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
    if (_csr_positions) {
        return Status::OK();
    }
    return _positions.finish_doc();
}

std::optional<std::span<const uint32_t>> SniiPostingsCursor::view() const {
    if (!_csr_positions) {
        return std::nullopt;
    }
    return _doc_positions;
}

} // namespace doris::snii::reader
