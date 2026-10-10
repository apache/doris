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

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_sink.h"

namespace doris::snii::format {

// Stores one encoded length byte per document for BM25 normalization.
// SectionFramer wraps a payload of [varint64 doc_count][encoded_norm bytes].
class NormsPodWriter {
public:
    // Appends the encoded_norm for the next docid (docid is implicit, assigned in append order starting from 0).
    void add(uint8_t encoded_norm) { norms_.push_back(encoded_norm); }

    // Number of docs accumulated so far (i.e., the next docid to be assigned).
    size_t count() const { return norms_.size(); }

    // Writes [doc_count][bytes] framed by SectionFramer into sink (appends; does not clear sink).
    void finish(ByteSink* sink) const;
    // Zero-copy source overload used by streamed compaction.
    static void finish(std::span<const uint8_t> norms, ByteSink* sink);

private:
    std::vector<uint8_t> norms_;
};

// Read-only view: on open, verifies the framer CRC and checks that doc_count/payload length are consistent,
// afterwards encoded_norm(docid) is O(1) direct indexing (zero-copy, borrows the underlying buffer).
class NormsPodReader {
public:
    NormsPodReader() = default;

    // Parses the entire section (including the framer envelope). Returns Corruption on CRC mismatch, truncation, or length inconsistency.
    // On success, *out borrows the memory pointed to by framer_payload; the caller must ensure its lifetime.
    static Status open(Slice framed, NormsPodReader* out);

    uint32_t doc_count() const { return doc_count_; }

    // Precondition (hard contract): docid < doc_count(). Semantics match std::vector::operator[]:
    // the caller is responsible for guaranteeing this (docid comes from trusted postings decoded internally by SNII). Asserts in debug builds;
    // no check in Release (NDEBUG). Use try_encoded_norm when the docid is untrusted and needs validation.
    uint8_t encoded_norm(uint32_t docid) const {
        assert(docid < doc_count_);
        return norms_[docid];
    }

    // Checked access: returns InvalidArgument if docid is out of range; never reads out-of-range memory.
    Status try_encoded_norm(uint32_t docid, uint8_t* out) const {
        if (docid >= doc_count_)
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("norms: docid out of range");
        *out = norms_[docid];
        return Status::OK();
    }

private:
    const uint8_t* norms_ = nullptr;
    uint32_t doc_count_ = 0;
};

} // namespace doris::snii::format
