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
#include <vector>

// clang-format off
// CLucene `IndexInput` forward declaration only — the impl that uses it
// (`IndexInputPostingStore`) lives in posting_store.cpp, so callers that
// only need the interface (e.g. window_term_reader, the unit tests) do
// NOT pull the CLucene headers.
// clang-format on
namespace lucene::store {
class IndexInput;
}

namespace doris::segment_v2::inverted_index::spimi {

// A positioned-read byte source for a SPIMI `.frq` file. The lazy
// window-addressed reader (`SpimiWindowedTermDocs`) pulls ONLY the
// header+skip-table prefix and each covering window's framed byte range
// through this seam instead of holding the whole term resident, so a
// selective query on S3 transfers a fraction of the term.
//
// Two implementations:
//   - `IndexInputPostingStore` (posting_store.cpp): wraps a CLucene
//     `IndexInput*` whose `readInternal` does Doris `read_at` +
//     FILE_BLOCK_CACHE = S3 range-GET. Each query thread clones the
//     reader's template `.frq` input into its own store (independent
//     cursor; the shared file handle's `read_at` is mutex-guarded).
//   - `MemPostingStore` (below): vector-backed, used by the eager
//     fallback (a one-shot resident copy of the term block) and by the
//     unit tests (with read-count/byte instrumentation). Header-only so
//     tests need no extra link target.
//
// THREAD SAFETY: a PostingStore is NOT shared across threads. Each
// `SpimiQueryTermDocs` (minted per query thread) owns its own store.
class PostingStore {
public:
    PostingStore() = default;
    virtual ~PostingStore() = default;

    PostingStore(const PostingStore&) = delete;
    PostingStore& operator=(const PostingStore&) = delete;

    // Reads exactly `len` bytes starting at absolute `offset` into `dst`.
    // Throws `doris::Exception` (INVERTED_INDEX_FILE_CORRUPTED) if the
    // requested range is out of bounds — an out-of-range positioned read
    // means the on-disk skip table / freq_pointer is corrupt.
    virtual void read_at(int64_t offset, uint8_t* dst, size_t len) = 0;

    // Total length of the underlying byte source.
    virtual int64_t length() const = 0;
};

// Vector-backed PostingStore. Owns (or borrows) a contiguous byte buffer
// whose logical offset 0 corresponds to `buf[0]`. Records read-call count,
// total bytes served, and a [offset,len) read log for test assertions.
class MemPostingStore final : public PostingStore {
public:
    // Borrows `data` (must outlive this store). `len` is its length.
    MemPostingStore(const uint8_t* data, size_t len) : _data(data), _len(len) {}

    // Owns a copy of `bytes` (the eager-fallback one-shot read path moves a
    // term block in here so the borrowed-pointer overload below stays valid).
    explicit MemPostingStore(std::vector<uint8_t> bytes)
            : _owned(std::move(bytes)), _data(_owned.data()), _len(_owned.size()) {}

    void read_at(int64_t offset, uint8_t* dst, size_t len) override;

    int64_t length() const override { return static_cast<int64_t>(_len); }

    // --- Test instrumentation ---
    int64_t read_count() const { return _read_count; }
    int64_t bytes_read() const { return _bytes_read; }
    const std::vector<std::pair<int64_t, size_t>>& read_log() const { return _read_log; }
    void reset_counters() {
        _read_count = 0;
        _bytes_read = 0;
        _read_log.clear();
    }

private:
    std::vector<uint8_t> _owned; // non-empty only for the owning ctor
    const uint8_t* _data;
    size_t _len;

    int64_t _read_count = 0;
    int64_t _bytes_read = 0;
    std::vector<std::pair<int64_t, size_t>> _read_log;
};

// PostingStore over a CLucene `IndexInput*` (a per-reader clone of the
// `.frq` `FSIndexInput`). Owns the input and `_CLDELETE`s it on destruction.
class IndexInputPostingStore final : public PostingStore {
public:
    // Takes ownership of `input` (typically `template_input->clone()`).
    explicit IndexInputPostingStore(lucene::store::IndexInput* input);
    ~IndexInputPostingStore() override;

    void read_at(int64_t offset, uint8_t* dst, size_t len) override;
    int64_t length() const override { return _length; }

private:
    lucene::store::IndexInput* _input;
    int64_t _length;
};

} // namespace doris::segment_v2::inverted_index::spimi
