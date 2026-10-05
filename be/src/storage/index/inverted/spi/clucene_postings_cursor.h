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

#include <climits>
#include <optional>
#include <vector>

#include "CLucene/index/DocRange.h"
#include "common/check.h"
#include "common/compiler_util.h"
#include "common/exception.h"
#include "storage/index/inverted/inverted_index_common.h"
#include "storage/index/query/spi/postings_cursor.h"

namespace doris::segment_v2 {

class ClucenePostingsCursor final : public index_query::PostingsCursor,
                                    public index_query::PositionCursor {
public:
    explicit ClucenePostingsCursor(TermDocsPtr iter) : _iter(std::move(iter)) { _check_iterator(); }
    explicit ClucenePostingsCursor(TermPositionsPtr iter) {
        _raw_positions = iter.get();
        _iter = std::move(iter);
        _check_iterator();
    }

    uint32_t doc_freq() const override { return _iter->docFreq(); }

    Status next_block(index_query::PostingsBlock* out, bool* eof) override {
        *out = {};
        _block_available = false;
        _position_open = false;
        _position_remaining = 0;
        try {
            *eof = !_iter->readBlock(&_block) || _block.doc_many_size_ == 0;
            _bound.reset();
            if (!*eof) {
                if (_raw_positions != nullptr) {
                    DORIS_CHECK(_block.freq_many != nullptr);
                    DORIS_CHECK_GE(_block.freq_many_size_, _block.doc_many_size_);
                }
                *out = {.docs = {_block.doc_many->data(), _block.doc_many_size_},
                        .freqs = _block.freq_many
                                         ? std::span<const uint32_t>(_block.freq_many->data(),
                                                                     _block.freq_many_size_)
                                         : std::span<const uint32_t>(),
                        .norms = _block.norm_many
                                         ? std::span<const uint32_t>(_block.norm_many->data(),
                                                                     _block.norm_many_size_)
                                         : std::span<const uint32_t>()};
                if (_block.type_ == DocRangeType::kRange) {
                    out->dense = true;
                    out->range_begin = _block.doc_range.first;
                    out->range_end = static_cast<uint64_t>(out->range_begin) + out->docs.size();
                }
                _freqs = _block.freq_many ? _block.freq_many->data() : nullptr;
                _prox_cursor = 0;
                _block_available = true;
            }
        } catch (CLuceneError& error) {
            return clucene_error_status(error.what());
        }
        return Status::OK();
    }

    Status seek_block(uint32_t target, index_query::PostingsBlock* out, bool* eof) override {
        bool moved = false;
        RETURN_IF_ERROR(shallow_seek(target, &moved));
        return next_block(out, eof);
    }

    Status shallow_seek(uint32_t target, bool* moved) override {
        try {
            *moved = _iter->skipToBlock(target);
            if (*moved) {
                _bound.reset();
                _block_available = false;
                _position_open = false;
                _position_remaining = 0;
            }
        } catch (CLuceneError& error) {
            return clucene_error_status(error.what());
        }
        return Status::OK();
    }

    index_query::BlockBound current_block_bound() const override {
        if (!_bound) {
            const int32_t last = _iter->getLastDocInBlock();
            _bound = {.last_doc = static_cast<uint32_t>(last),
                      .max_freq = _iter->getMaxBlockFreq(),
                      .max_norm = _iter->getMaxBlockNorm(),
                      .last_doc_known = last >= 0 && last != INT_MAX};
        }
        return *_bound;
    }

    Status open_positions(uint32_t ordinal, index_query::PositionCursor** out) override {
        *out = nullptr;
        if (_raw_positions == nullptr) {
            return Status::NotSupported("This posting type does not support position information");
        }
        try {
            _open_positions(ordinal);
            *out = this;
        } catch (CLuceneError& error) {
            return clucene_error_status(error.what());
        }
        return Status::OK();
    }

    Status open_position_stream(uint32_t ordinal, std::span<uint32_t> first_chunk, size_t* count,
                                index_query::PositionCursor** out) override {
        *out = nullptr;
        *count = 0;
        if (_raw_positions == nullptr) {
            return Status::NotSupported("This posting type does not support position information");
        }
        uint32_t position = 0;
        uint32_t remaining = 0;
        size_t filled = 0;
        bool opened = false;
        Status status;
        try {
            remaining = _prepare_positions(ordinal);
            opened = true;
            while (filled < first_chunk.size() && remaining != 0) {
                position += static_cast<uint32_t>(_raw_positions->nextDeltaPosition());
                --remaining;
                first_chunk[filled++] = position;
            }
            *out = this;
        } catch (CLuceneError& error) {
            status = clucene_error_status(error.what());
        }
        if (opened) {
            _position = position;
            _position_remaining = remaining;
            _position_open = true;
            *count = filled;
        }
        return status;
    }

    Status next_position(uint32_t* position, bool* available) override {
        return next_position_at_least(0, position, available);
    }

    Status next_position_at_least(uint32_t target, uint32_t* position, bool* available) override {
        DCHECK(_position_open);
        *available = false;
        if (_position_remaining == 0) {
            return Status::OK();
        }
        uint32_t current = _position;
        uint32_t remaining = _position_remaining;
        Status status;
        try {
            while (remaining != 0) {
                current += static_cast<uint32_t>(_raw_positions->nextDeltaPosition());
                --remaining;
                if (current >= target) {
                    *position = current;
                    *available = true;
                    break;
                }
            }
        } catch (CLuceneError& error) {
            status = clucene_error_status(error.what());
        }
        _position = current;
        _position_remaining = remaining;
        return status;
    }

    Status finish_doc() override {
        DCHECK(_position_open);
        if (_position_remaining == 0) {
            return Status::OK();
        }
        try {
            _finish_positions();
        } catch (CLuceneError& error) {
            return clucene_error_status(error.what());
        }
        return Status::OK();
    }

    Status append_remaining_positions(uint32_t offset, std::vector<uint32_t>& output) override {
        DCHECK(_position_open);
        uint32_t position = _position;
        uint32_t remaining = _position_remaining;
        Status status;
        try {
            for (; remaining != 0; --remaining) {
                position += static_cast<uint32_t>(_raw_positions->nextDeltaPosition());
                output.push_back(position + offset);
            }
        } catch (CLuceneError& error) {
            status = clucene_error_status(error.what());
        }
        _position = position;
        _position_remaining = remaining;
        return status;
    }

    // Opens and drains the document in one CLucene call.
    Status append_positions(uint32_t ordinal, uint32_t offset,
                            std::vector<uint32_t>& output) override {
        if (_raw_positions == nullptr) {
            return Status::NotSupported("This posting type does not support position information");
        }
        uint32_t position = 0;
        uint32_t remaining = 0;
        bool opened = false;
        Status status;
        try {
            remaining = _prepare_positions(ordinal);
            opened = true;
            for (; remaining != 0; --remaining) {
                position += static_cast<uint32_t>(_raw_positions->nextDeltaPosition());
                output.push_back(position + offset);
            }
        } catch (CLuceneError& error) {
            status = clucene_error_status(error.what());
        }
        if (opened) {
            _position = position;
            _position_remaining = remaining;
            _position_open = true;
        }
        return status;
    }

private:
    void _finish_positions() {
        if (_position_remaining != 0) {
            _raw_positions->addLazySkipProxCount(_position_remaining);
            _position_remaining = 0;
        }
    }

    // Inlined into each open: it runs once per document a phrase reads.
    ALWAYS_INLINE void _open_positions(uint32_t ordinal) {
        _position_remaining = _prepare_positions(ordinal);
        _position = 0;
        _position_open = true;
    }

    ALWAYS_INLINE uint32_t _prepare_positions(uint32_t ordinal) {
        DCHECK(_block_available);
        DCHECK(_freqs != nullptr);
        DCHECK_LT(ordinal, _block.doc_many_size_);
        DCHECK_GE(ordinal, _prox_cursor);
        if (_position_open) {
            _finish_positions();
        }
        int32_t skip_count = 0;
        for (uint32_t i = _prox_cursor; i < ordinal; ++i) {
            skip_count += _freqs[i];
        }
        if (skip_count > 0) {
            _raw_positions->addLazySkipProxCount(skip_count);
        }
        _prox_cursor = ordinal + 1;
        return _freqs[ordinal];
    }

    void _check_iterator() const {
        if (!_iter) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "CLucene postings require a valid iterator");
        }
    }

    TermDocsPtr _iter;
    DocRange _block;
    // The current block's frequencies, when it holds them.
    const uint32_t* _freqs = nullptr;
    uint32_t _prox_cursor = 0;
    lucene::index::TermPositions* _raw_positions = nullptr;
    uint32_t _position_remaining = 0;
    uint32_t _position = 0;
    bool _position_open = false;
    bool _block_available = false;
    mutable std::optional<index_query::BlockBound> _bound;
};

} // namespace doris::segment_v2
