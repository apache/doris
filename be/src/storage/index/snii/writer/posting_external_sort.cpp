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

#include "storage/index/snii/writer/posting_external_sort.h"

#include <algorithm>
#include <array>
#include <limits>

namespace doris::snii::writer {
namespace {

constexpr size_t kSortMetadataBytes = 2 * sizeof(PostingByteBuffer) + sizeof(PostingByteCursor);

struct SortLookahead {
    PostingByteCursor* input;
    std::array<uint32_t, 2> token {};
    bool ready = false;

    Status peek() {
        if (ready || input->remaining() == 0) {
            return Status::OK();
        }
        RETURN_IF_ERROR(input->read_u32(token.data()));
        RETURN_IF_ERROR(input->read_u32(&token[1]));
        ready = true;
        return Status::OK();
    }
};

Status merge_sorted_pair(PostingByteCursor* left, PostingByteCursor* right,
                         PostingByteBuffer* output) {
    SortLookahead a {.input = left};
    SortLookahead b {.input = right};
    while (true) {
        RETURN_IF_ERROR(a.peek());
        RETURN_IF_ERROR(b.peek());
        if (!a.ready && !b.ready) {
            return Status::OK();
        }
        // Taking the left token on ties preserves original arrival order.
        auto* next = a.ready && (!b.ready || a.token[0] <= b.token[0]) ? &a : &b;
        RETURN_IF_ERROR(output->append_u32(next->token));
        next->ready = false;
    }
}

} // namespace

SortedPostingTokens::SortedPostingTokens(MemoryReporter* reporter)
        : reporter_(reporter),
          reservation_(reporter == nullptr ? MemoryReporter::Reservation()
                                           : reporter->make_postings_reservation()) {}

Status SortedPostingTokens::append(uint32_t document, uint32_t position) {
    DORIS_CHECK(cursor_ == nullptr);
    if (chunk_.capacity() == 0) {
        if (reporter_ != nullptr) {
            RETURN_IF_ERROR(
                    reservation_.set_bytes(kChunkTokens * sizeof(Token) + kSortMetadataBytes));
        }
        sorted_ = std::make_unique<PostingByteBuffer>(reporter_);
        chunk_.reserve(kChunkTokens);
    }
    chunk_.push_back({document, position, static_cast<uint32_t>(chunk_.size())});
    ++tokens_;
    if (chunk_.size() == kChunkTokens) {
        RETURN_IF_ERROR(flush_chunk());
    }
    return Status::OK();
}

Status SortedPostingTokens::flush_chunk() {
    std::ranges::sort(chunk_, [](const Token& a, const Token& b) {
        return a.document == b.document ? a.ordinal < b.ordinal : a.document < b.document;
    });
    for (const Token& token : chunk_) {
        const std::array<uint32_t, 2> pair {token.document, token.position};
        RETURN_IF_ERROR(sorted_->append_u32(pair));
    }
    chunk_.clear();
    return Status::OK();
}

Status SortedPostingTokens::merge_pass(uint64_t run_tokens, PostingByteBuffer* output) {
    PostingByteCursor left(sorted_.get());
    PostingByteCursor right(sorted_.get());
    for (uint64_t begin = 0; begin < tokens_; begin += 2 * run_tokens) {
        const uint64_t left_count = std::min(run_tokens, tokens_ - begin);
        const uint64_t right_count = std::min(run_tokens, tokens_ - begin - left_count);
        RETURN_IF_ERROR(left.reset(begin * 8, left_count * 8));
        RETURN_IF_ERROR(right.reset((begin + left_count) * 8, right_count * 8));
        RETURN_IF_ERROR(merge_sorted_pair(&left, &right, output));
    }
    return Status::OK();
}

Status SortedPostingTokens::finish() {
    DORIS_CHECK(cursor_ == nullptr);
    if (sorted_ == nullptr) {
        if (reporter_ != nullptr) {
            RETURN_IF_ERROR(reservation_.set_bytes(kSortMetadataBytes));
        }
        sorted_ = std::make_unique<PostingByteBuffer>(reporter_);
    }
    RETURN_IF_ERROR(flush_chunk());
    std::vector<Token>().swap(chunk_);
    if (reporter_ != nullptr) {
        RETURN_IF_ERROR(reservation_.set_bytes(kSortMetadataBytes));
    }
    for (uint64_t run_tokens = kChunkTokens; run_tokens < tokens_; run_tokens *= 2) {
        auto output = std::make_unique<PostingByteBuffer>(reporter_);
        RETURN_IF_ERROR(merge_pass(run_tokens, output.get()));
        sorted_ = std::move(output);
    }
    cursor_ = std::make_unique<PostingByteCursor>(sorted_.get());
    RETURN_IF_ERROR(cursor_->reset());
    bool first = true;
    uint32_t previous = 0;
    while (cursor_->remaining() != 0) {
        uint32_t document = 0;
        uint32_t position = 0;
        RETURN_IF_ERROR(cursor_->read_u32(&document));
        RETURN_IF_ERROR(cursor_->read_u32(&position));
        if (first || document != previous) {
            ++documents_;
        }
        first = false;
        previous = document;
    }
    return cursor_->reset();
}

Status SortedPostingTokens::next(uint32_t* document, uint32_t* position, bool* end) {
    DORIS_CHECK(cursor_ != nullptr);
    *end = cursor_->remaining() == 0;
    if (!*end) {
        RETURN_IF_ERROR(cursor_->read_u32(document));
        RETURN_IF_ERROR(cursor_->read_u32(position));
    }
    return Status::OK();
}

} // namespace doris::snii::writer
