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
#include <variant>
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
    using IterVariant = std::variant<TermDocsPtr, TermPositionsPtr>;

    explicit ClucenePostingsCursor(TermDocsPtr iter) : _iter(std::move(iter)) {
        _raw_iter = std::get<TermDocsPtr>(_iter).get();
        _check_iterator();
    }
    explicit ClucenePostingsCursor(TermPositionsPtr iter) : _iter(std::move(iter)) {
        _raw_positions = std::get<TermPositionsPtr>(_iter).get();
        _raw_iter = _raw_positions;
        _check_iterator();
    }

    uint32_t doc_freq() const override { return _raw_iter->docFreq(); }

    Status next_block(index_query::PostingsBlock* out, bool* eof) override {
        ErrorContext error_context;
        *out = {};
        _block_available = false;
        _position_open = false;
        _position_remaining = 0;
        try {
            *eof = !_raw_iter->readBlock(&_block) || _block.doc_many_size_ == 0;
            _bound.reset();
            if (!*eof) {
                *out = {.docs = {_block.doc_many->data(), _block.doc_many_size_},
                        .freqs = _block.freq_many
                                         ? std::span<const uint32_t>(_block.freq_many->data(),
                                                                     _block.freq_many_size_)
                                         : std::span<const uint32_t>(),
                        .norms = _block.norm_many
                                         ? std::span<const uint32_t>(_block.norm_many->data(),
                                                                     _block.norm_many_size_)
                                         : std::span<const uint32_t>()};
                _freqs = _block.freq_many ? _block.freq_many->data() : nullptr;
                _prox_cursor = 0;
                _block_available = true;
            }
        } catch (CLuceneError& error) {
            error_context.eptr = std::current_exception();
            error_context.err_msg = error.what();
        }
        FINALLY({});
        return Status::OK();
    }

    Status seek_block(uint32_t target, index_query::PostingsBlock* out, bool* eof) override {
        bool moved = false;
        RETURN_IF_ERROR(shallow_seek(target, &moved));
        return next_block(out, eof);
    }

    Status shallow_seek(uint32_t target, bool* moved) override {
        ErrorContext error_context;
        try {
            *moved = _raw_iter->skipToBlock(target);
            if (*moved) {
                _bound.reset();
                _block_available = false;
                _position_open = false;
                _position_remaining = 0;
            }
        } catch (CLuceneError& error) {
            error_context.eptr = std::current_exception();
            error_context.err_msg = error.what();
        }
        FINALLY({});
        return Status::OK();
    }

    index_query::BlockBound current_block_bound() const override {
        if (!_bound) {
            const int32_t last = _raw_iter->getLastDocInBlock();
            _bound = {.last_doc = static_cast<uint32_t>(last),
                      .max_freq = _raw_iter->getMaxBlockFreq(),
                      .max_norm = _raw_iter->getMaxBlockNorm(),
                      .last_doc_known = last >= 0 && last != INT_MAX};
        }
        return *_bound;
    }

    Status open_positions(uint32_t ordinal, index_query::PositionCursor** out) override {
        *out = nullptr;
        if (_raw_positions == nullptr) {
            return Status::NotSupported("This posting type does not support position information");
        }
        ErrorContext error_context;
        try {
            _open_positions(ordinal);
            *out = this;
        } catch (CLuceneError& error) {
            error_context.eptr = std::current_exception();
            error_context.err_msg = error.what();
        }
        FINALLY({});
        return Status::OK();
    }

    uint32_t frequency() const override {
        DORIS_CHECK(_position_open);
        return _position_frequency;
    }

    Status next_position(uint32_t* position, bool* available) override {
        DORIS_CHECK(_position_open);
        *available = false;
        if (_position_remaining == 0) {
            return Status::OK();
        }
        ErrorContext error_context;
        try {
            *position = _read_next_position();
            *available = true;
        } catch (CLuceneError& error) {
            error_context.eptr = std::current_exception();
            error_context.err_msg = error.what();
        }
        FINALLY({});
        return Status::OK();
    }

    Status finish_doc() override {
        DORIS_CHECK(_position_open);
        if (_position_remaining == 0) {
            return Status::OK();
        }
        ErrorContext error_context;
        try {
            _finish_positions();
        } catch (CLuceneError& error) {
            error_context.eptr = std::current_exception();
            error_context.err_msg = error.what();
        }
        FINALLY({});
        return Status::OK();
    }

    Status append_remaining_positions(uint32_t offset, std::vector<uint32_t>& output) override {
        DORIS_CHECK(_position_open);
        ErrorContext error_context;
        uint32_t position = _position;
        uint32_t remaining = _position_remaining;
        try {
            for (; remaining != 0; --remaining) {
                position += static_cast<uint32_t>(_raw_positions->nextDeltaPosition());
                output.push_back(position + offset);
            }
        } catch (CLuceneError& error) {
            error_context.eptr = std::current_exception();
            error_context.err_msg = error.what();
        }
        FINALLY({
            _position = position;
            _position_remaining = remaining;
        });
        return Status::OK();
    }

    // The open and the drain in one CLucene error frame. It runs once per document a phrase reads,
    // so the error status is built only when CLucene throws.
    Status append_positions(uint32_t ordinal, uint32_t offset,
                            std::vector<uint32_t>& output) override {
        if (_raw_positions == nullptr) {
            return Status::NotSupported("This posting type does not support position information");
        }
        uint32_t position = 0;
        uint32_t remaining = 0;
        bool opened = false;
        try {
            _open_positions(ordinal);
            position = _position;
            remaining = _position_remaining;
            opened = true;
            for (; remaining != 0; --remaining) {
                position += static_cast<uint32_t>(_raw_positions->nextDeltaPosition());
                output.push_back(position + offset);
            }
        } catch (CLuceneError& error) {
            if (opened) {
                _position = position;
                _position_remaining = remaining;
            }
            return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>("{}", error.what());
        }
        _position = position;
        _position_remaining = remaining;
        return Status::OK();
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
        DORIS_CHECK(_block_available);
        DORIS_CHECK(_freqs != nullptr);
        DORIS_CHECK(ordinal < _block.freq_many_size_);
        DORIS_CHECK(ordinal >= _prox_cursor);
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
        _position_frequency = _freqs[ordinal];
        _position_remaining = _position_frequency;
        _position = 0;
        _position_open = true;
        _prox_cursor = ordinal + 1;
    }

    uint32_t _read_next_position() {
        _position += static_cast<uint32_t>(_raw_positions->nextDeltaPosition());
        --_position_remaining;
        return _position;
    }

    void _check_iterator() const {
        if (!_raw_iter) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "CLucene postings require a valid iterator");
        }
    }

    IterVariant _iter;
    lucene::index::TermDocs* _raw_iter = nullptr;
    DocRange _block;
    // The current block's frequencies, when it holds them.
    const uint32_t* _freqs = nullptr;
    uint32_t _prox_cursor = 0;
    lucene::index::TermPositions* _raw_positions = nullptr;
    uint32_t _position_frequency = 0;
    uint32_t _position_remaining = 0;
    uint32_t _position = 0;
    bool _position_open = false;
    bool _block_available = false;
    mutable std::optional<index_query::BlockBound> _bound;
};

} // namespace doris::segment_v2
