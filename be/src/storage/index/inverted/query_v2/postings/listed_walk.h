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
#include <span>
#include <vector>

#include "common/check.h"
#include "common/status.h"
#include "storage/index/query/exec/cursor_chained_postings.h"
#include "storage/index/query/phrase/position_span.h"
#include "storage/index/query/spi/postings_cursor.h"

namespace doris::segment_v2::inverted_index::query_v2 {

// One term walked over listed rows, all of which it holds, in ascending order. Entering a block
// reads the positions of every listed row it holds in one call; they stay viewed until the walk
// leaves the block.
class TermWalk {
public:
    TermWalk(index_query::PostingsCursor& cursor, std::span<const uint32_t> rows)
            : _cursor(cursor), _rows(rows) {}

    Status positions_of(size_t row, uint32_t /*doc*/, index_query::PhrasePositionSpan* span) {
        if (row >= _end) {
            RETURN_IF_ERROR(_enter_block(row));
        }
        const size_t chosen = row - _begin;
        const size_t k = _positions.by_ordinal ? _asked[chosen] : chosen;
        *span = {_positions.flat.data() + _positions.offsets[k],
                 _positions.flat.data() + _positions.offsets[k + 1]};
        return Status::OK();
    }

private:
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
            return _cursor.block_positions(_asked, &_buffer, &_positions);
        }
        _ordinals.resize(count);
        _list_ordinals(_rows.subspan(_begin, count), _ordinals.data());
        _asked = _ordinals;
        return _cursor.block_positions(_asked, &_buffer, &_positions);
    }

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

    index_query::PostingsCursor& _cursor;
    std::span<const uint32_t> _rows;
    index_query::PostingsBlock _block;
    // 0, 1, 2, ...: the ordinals of a block whose every document is listed.
    std::vector<uint32_t> _every;
    std::vector<uint32_t> _ordinals;
    // The ordinals the current block's positions were asked for.
    std::span<const uint32_t> _asked;
    index_query::PositionsBuffer _buffer;
    index_query::BlockPositions _positions;
    size_t _begin = 0;
    size_t _end = 0;
};

} // namespace doris::segment_v2::inverted_index::query_v2
