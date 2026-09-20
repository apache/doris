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

#include <algorithm>

#include "common/exception.h"
#include "storage/index/query/spi/postings_cursor.h"

namespace doris::index_query {

// Owns traversal state; the referenced cursor must outlive this object.
class BlockDocSet {
public:
    explicit BlockDocSet(PostingsCursor& cursor) : _cursor(cursor) { advance(); }

    BlockDocSet(const BlockDocSet&) = delete;
    BlockDocSet& operator=(const BlockDocSet&) = delete;
    BlockDocSet(BlockDocSet&&) = delete;
    BlockDocSet& operator=(BlockDocSet&&) = delete;

    bool advance() {
        if (!_block.dense) {
            if (_next < _block.docs.size()) {
                _doc = _block.docs[_next++];
                return true;
            }
        } else if (_next < _block.range_end - _block.range_begin) {
            _doc = static_cast<uint32_t>(_block.range_begin + _next++);
            return true;
        }
        return advance_block();
    }

    bool seek(uint32_t target) {
        if (_eof || target <= _doc) {
            return !_eof;
        }
        if (_seek_in_block(target)) {
            return true;
        }
        return _seek_next_block(target);
    }

    void shallow_seek(uint32_t target) {
        if (_eof || target <= _doc) {
            return;
        }
        bool moved = false;
        THROW_IF_ERROR(_cursor.shallow_seek(target, &moved));
        if (moved) {
            _block = {};
            _next = 0;
            ++_generation;
        }
    }

    bool advance_block() {
        if (_eof) {
            return false;
        }
        THROW_IF_ERROR(_cursor.next_block(&_block, &_eof));
        _next = 0;
        ++_generation;
        if (_eof) {
            return false;
        }
        DCHECK_GT(_block.size(), 0);
        _doc = _block.doc_at(_next++);
        return true;
    }

    uint32_t doc() const { return _doc; }
    bool exhausted() const { return _eof; }
    uint32_t size_hint() const { return _cursor.doc_freq(); }
    bool cheap_seek() const { return _cursor.cheap_seek(); }
    uint64_t ordinal() const { return _next - 1; }
    uint64_t generation() const { return _generation; }
    uint32_t freq() const { return _block.freq_at(ordinal()); }
    uint32_t norm() const { return _block.norm_at(ordinal()); }
    PostingsBlock remaining_block() const { return _block.suffix(ordinal()); }

    // Consumes the decoded block without invalidating its views.
    PostingsBlock take_remaining_block() {
        DCHECK(!_eof && _next != 0);
        auto result = remaining_block();
        _next = _block.size();
        _doc = _block.doc_at(_next - 1);
        return result;
    }

private:
    bool _seek_next_block(uint32_t target) {
        THROW_IF_ERROR(_cursor.seek_block(target, &_block, &_eof));
        _next = 0;
        ++_generation;
        while (!_eof) {
            if (_seek_in_block(target)) {
                return true;
            }
            THROW_IF_ERROR(_cursor.next_block(&_block, &_eof));
            _next = 0;
            ++_generation;
        }
        return false;
    }

    bool _seek_in_block(uint32_t target) {
        if (_block.dense) {
            if (target >= _block.range_end) {
                _next = _block.size();
                return false;
            }
            _next = std::max<uint64_t>(
                    _next, target > _block.range_begin ? target - _block.range_begin : 0);
            if (_next < _block.size()) {
                _doc = _block.doc_at(_next++);
                return true;
            }
        }
        while (_next < _block.size()) {
            _doc = _block.doc_at(_next++);
            if (_doc >= target) {
                return true;
            }
        }
        return false;
    }

    PostingsCursor& _cursor;
    PostingsBlock _block;
    uint64_t _next = 0;
    uint64_t _generation = 0;
    uint32_t _doc = 0;
    bool _eof = false;
};

} // namespace doris::index_query
