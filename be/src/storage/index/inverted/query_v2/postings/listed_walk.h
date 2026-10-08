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

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

#include "common/check.h"
#include "common/compiler_util.h"
#include "common/status.h"
#include "storage/index/query/exec/cursor_chained_postings.h"
#include "storage/index/query/phrase/position_span.h"
#include "storage/index/query/spi/postings_cursor.h"

namespace doris::segment_v2::inverted_index::query_v2 {

// The listed rows a term holds, in ascending order, grouped by the term's blocks: entering the
// block holding a row lists the ordinals of the listed rows it holds.
class ListedBlocks {
protected:
    ListedBlocks(index_query::PostingsCursor& cursor, std::span<const uint32_t> rows,
                 const index_query::SelectedPostings* selected)
            : _cursor(cursor), _rows(rows), _selected(selected) {}

    // The listed rows in the block are some of its documents, and all of them when they are as
    // many.
    Status _enter_block(size_t row) {
        bool eof = false;
        RETURN_IF_ERROR(_cursor.seek_block(_rows[row], &_block, &eof));
        DORIS_CHECK(!eof);
        const uint32_t last = _block.doc_at(_block.size() - 1);
        _begin = row;
        _end = index_query::gallop_past(_rows, row, [last](uint32_t doc) { return doc <= last; });
        const size_t count = _end - _begin;
        if (count == _block.size()) {
            for (auto ordinal = static_cast<uint32_t>(_every.size()); ordinal < count; ++ordinal) {
                _every.push_back(ordinal);
            }
            _asked = std::span(_every).first(count);
            return Status::OK();
        }
        _ordinals.clear();
        const auto listed = _rows.subspan(_begin, count);
        if (_reuse_ordinals(listed, last)) {
            return Status::OK();
        }
        _ordinals.reserve(count);
        if (_block.dense) {
            for (const uint32_t doc : listed) {
                _ordinals.push_back(doc - _block.range_begin);
            }
        } else {
            index_query::intersect_block_ordinals(_block.docs, listed, &_ordinals);
        }
        DORIS_CHECK_EQ(_ordinals.size(), count);
        _asked = _ordinals;
        return Status::OK();
    }

    index_query::PostingsCursor& _cursor;
    std::span<const uint32_t> _rows;
    // The ordinals of the current block's listed rows, the first of which is _rows[_begin], and
    // the listed row after the block.
    std::span<const uint32_t> _asked;
    size_t _begin = 0;
    size_t _end = 0;

private:
    // Later terms may narrow the earlier intersection; keep the ordinals of surviving rows.
    bool _reuse_ordinals(std::span<const uint32_t> listed, uint32_t last) {
        if (_selected == nullptr) {
            return false;
        }
        const auto& blocks = _selected->blocks;
        while (_next_selected < blocks.size() && blocks[_next_selected].last_doc < last) {
            ++_next_selected;
        }
        if (_next_selected == blocks.size() || blocks[_next_selected].last_doc != last) {
            return false;
        }
        const size_t begin = _next_selected == 0 ? 0 : blocks[_next_selected - 1].end;
        const auto retained =
                std::span(_selected->ordinals).subspan(begin, blocks[_next_selected++].end - begin);
        if (retained.size() == listed.size()) {
            _asked = retained;
            return true;
        }
        _ordinals.reserve(listed.size());
        size_t next = 0;
        for (const uint32_t doc : listed) {
            while (next < retained.size() && _block.docs[retained[next]] < doc) {
                ++next;
            }
            DCHECK_LT(next, retained.size());
            DCHECK_EQ(_block.docs[retained[next]], doc);
            _ordinals.push_back(retained[next++]);
        }
        _asked = _ordinals;
        return true;
    }

    const index_query::SelectedPostings* _selected;
    size_t _next_selected = 0;
    index_query::PostingsBlock _block;
    // 0, 1, 2, ...: the ordinals of a block whose every document is listed.
    std::vector<uint32_t> _every;
    std::vector<uint32_t> _ordinals;
};

// One term walked over listed rows, all of which it holds, in ascending order. Entering a block
// reads the positions of every listed row it holds in one call; they stay viewed until the walk
// leaves the block.
class TermWalk : private ListedBlocks {
public:
    TermWalk(index_query::PostingsCursor& cursor, std::span<const uint32_t> rows,
             const index_query::SelectedPostings* selected = nullptr)
            : ListedBlocks(cursor, rows, selected) {}

    Status prepare(size_t row, uint32_t /*doc*/) {
        if (row >= _end) {
            RETURN_IF_ERROR(_read_block(row));
        }
        return Status::OK();
    }

    size_t end() const { return _end; }

    // Prepared rows share the current block's positions until prepare enters another block.
    index_query::PhrasePositionSpan positions(size_t row) const {
        DCHECK_GE(row, _begin);
        DCHECK_LT(row, _end);
        const size_t chosen = row - _begin;
        const size_t k = _positions.by_ordinal ? _asked[chosen] : chosen;
        return {_positions.flat.data() + _positions.offsets[k],
                _positions.flat.data() + _positions.offsets[k + 1]};
    }

private:
    Status _read_block(size_t row) {
        RETURN_IF_ERROR(_enter_block(row));
        RETURN_IF_ERROR(_cursor.block_positions(_asked, &_buffer, &_positions));
        // Selecting every document makes the position ordinal equal to the row ordinal.
        if (_asked.size() + 1 == _positions.offsets.size()) {
            _positions.by_ordinal = false;
        }
        return Status::OK();
    }

    index_query::PositionsBuffer _buffer;
    index_query::BlockPositions _positions;
};

// One term walked over listed rows, all of which it holds, in ascending order, as the streaming
// exact phrase matcher reads it: each row is opened in turn and its positions decode only as far
// as they are read, a chunk at a time.
class StreamWalk : private ListedBlocks {
public:
    StreamWalk(index_query::PostingsCursor& cursor, std::span<const uint32_t> rows,
               const index_query::SelectedPostings* selected = nullptr)
            : ListedBlocks(cursor, rows, selected) {}

    // Prepares the block containing the next row before its documents are opened.
    Status prepare() {
        if (_next >= _end) {
            RETURN_IF_ERROR(_enter_block(_next));
            RETURN_IF_ERROR(_cursor.stream_positions(_asked));
        }
        return Status::OK();
    }

    size_t end() const { return _end; }

    // Opens the next listed row in the prepared block and pulls its first chunk.
    Status seek(uint32_t doc) {
        DCHECK_LT(_next, _end);
        DCHECK_EQ(_rows[_next], doc);
        const uint32_t ordinal = _asked[_next - _begin];
        ++_next;
        RETURN_IF_ERROR(_cursor.open_position_stream(ordinal, _buffer, &_buffered, &_positions));
        _read = 0;
        _last_chunk = _positions == nullptr || _buffered < _buffer.size();
        return Status::OK();
    }

    // The open row's positions not passed yet, when the walk holds all of them.
    bool whole(index_query::PhrasePositionSpan* span) const {
        if (!_last_chunk) {
            return false;
        }
        *span = {_buffer.data() + _read, _buffer.data() + _buffered};
        return true;
    }

    bool available() const { return _read < _buffered; }
    bool needs_finish() const { return _positions != nullptr; }
    uint32_t position() const { return _buffer[_read]; }

    // Advances to the first position at or after target.
    ALWAYS_INLINE Status advance_to(uint32_t target) {
        if (!_scan(target) && !_last_chunk) {
            return _scan_next_chunks(target);
        }
        return Status::OK();
    }

    Status finish_doc() { return _positions == nullptr ? Status::OK() : _positions->finish_doc(); }

private:
    // Passes the buffered positions below `target`, from locals so the loop stays in registers;
    // whether the buffer has one left.
    bool _scan(uint32_t target) {
        size_t read = _read;
        const size_t buffered = _buffered;
        while (read < buffered && _buffer[read] < target) {
            ++read;
        }
        _read = read;
        return read < buffered;
    }

    // A chunk shorter than the buffer is the row's last.
    Status _pull() {
        RETURN_IF_ERROR(_positions->next_positions(_buffer, &_buffered));
        _read = 0;
        _last_chunk = _buffered < _buffer.size();
        return Status::OK();
    }

    // Pulls chunks until one holds a position at or after `target`.
    NO_INLINE Status _scan_next_chunks(uint32_t target) {
        do {
            RETURN_IF_ERROR(_pull());
        } while (!_scan(target) && !_last_chunk);
        return Status::OK();
    }

    size_t _next = 0;
    index_query::PositionCursor* _positions = nullptr;
    // The positions pulled from the open row, the first of them not passed yet, and whether the
    // row has none after them.
    std::array<uint32_t, 16> _buffer {};
    size_t _buffered = 0;
    size_t _read = 0;
    bool _last_chunk = false;
};

} // namespace doris::segment_v2::inverted_index::query_v2
