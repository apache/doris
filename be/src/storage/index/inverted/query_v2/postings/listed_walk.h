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
    ListedBlocks(index_query::PostingsCursor& cursor, std::span<const uint32_t> rows)
            : _cursor(cursor), _rows(rows) {}

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
        _ordinals.resize(count);
        _list_ordinals(_rows.subspan(_begin, count), _ordinals.data());
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
    // The ordinals of `listed`, some of the current block's documents, in the block. A few skip
    // ahead to each; at least half step through the block once, without a branch on each
    // comparison.
    void _list_ordinals(std::span<const uint32_t> listed, uint32_t* ordinals) const {
        if (_block.dense) {
            const uint32_t first = _block.range_begin;
            for (size_t i = 0; i < listed.size(); ++i) {
                ordinals[i] = listed[i] - first;
            }
            return;
        }
        const uint32_t* docs = _block.docs.data();
        uint32_t ordinal = 0;
        if (listed.size() * 2 < _block.docs.size()) {
            for (size_t i = 0; i < listed.size(); ++i) {
                while (docs[ordinal] < listed[i]) {
                    ++ordinal;
                }
                DCHECK_EQ(docs[ordinal], listed[i]);
                ordinals[i] = ordinal++;
            }
            return;
        }
        for (size_t i = 0; i < listed.size(); ++ordinal) {
            DCHECK_LT(ordinal, _block.docs.size());
            ordinals[i] = ordinal;
            i += docs[ordinal] == listed[i] ? 1 : 0;
        }
    }

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
    TermWalk(index_query::PostingsCursor& cursor, std::span<const uint32_t> rows)
            : ListedBlocks(cursor, rows) {}

    Status positions_of(size_t row, uint32_t /*doc*/, index_query::PhrasePositionSpan* span) {
        if (row >= _end) {
            RETURN_IF_ERROR(_enter_block(row));
            RETURN_IF_ERROR(_cursor.block_positions(_asked, &_buffer, &_positions));
        }
        const size_t chosen = row - _begin;
        const size_t k = _positions.by_ordinal ? _asked[chosen] : chosen;
        *span = {_positions.flat.data() + _positions.offsets[k],
                 _positions.flat.data() + _positions.offsets[k + 1]};
        return Status::OK();
    }

private:
    index_query::PositionsBuffer _buffer;
    index_query::BlockPositions _positions;
};

// One term walked over listed rows, all of which it holds, in ascending order, as the streaming
// exact phrase matcher reads it: each row is opened in turn and its positions decode only as far
// as they are read, a chunk at a time.
class StreamWalk : private ListedBlocks {
public:
    StreamWalk(index_query::PostingsCursor& cursor, std::span<const uint32_t> rows)
            : ListedBlocks(cursor, rows) {}

    // Opens the next listed row, which is `doc`, and pulls its first chunk.
    Status seek(uint32_t doc) {
        DCHECK_LT(_next, _rows.size());
        DCHECK_EQ(_rows[_next], doc);
        if (_next >= _end) {
            RETURN_IF_ERROR(_enter_block(_next));
            RETURN_IF_ERROR(_cursor.stream_positions(_asked));
        }
        const uint32_t ordinal = _asked[_next - _begin];
        ++_next;
        RETURN_IF_ERROR(_cursor.open_positions(ordinal, &_positions));
        return _pull();
    }

    // The open row's positions not passed yet, when the walk holds all of them.
    bool whole(index_query::PhrasePositionSpan* span) const {
        if (!_last_chunk) {
            return false;
        }
        *span = {_buffer.data() + _read, _buffer.data() + _buffered};
        return true;
    }

    // The open row's first position at or after `target`; the walk stays on it.
    ALWAYS_INLINE Status advance_to(uint32_t target, uint32_t* position, bool* available) {
        if (!_scan(target) && !_last_chunk) {
            RETURN_IF_ERROR(_scan_next_chunks(target));
        }
        *available = _read < _buffered;
        if (*available) {
            *position = _buffer[_read];
        }
        return Status::OK();
    }

    Status finish_doc() { return _positions->finish_doc(); }

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
