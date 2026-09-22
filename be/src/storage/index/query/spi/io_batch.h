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
#include <memory>
#include <span>
#include <vector>

#include "storage/index/query/spi/io_read_batch.h"

namespace doris::index_query {

// Owns one wave across readers and admits its physical buffers before I/O.
class IoBatch {
    struct Buffer {
        MemoryBudget::Reservation memory;
        std::vector<uint8_t> bytes;
    };

public:
    class Pin {
    public:
        std::span<const uint8_t> bytes() const {
            return owner_ == nullptr
                           ? std::span<const uint8_t>()
                           : std::span<const uint8_t>(owner_->bytes).subspan(offset_, length_);
        }

    private:
        friend class IoBatch;
        std::shared_ptr<const Buffer> owner_;
        size_t offset_ = 0;
        size_t length_ = 0;
    };

    struct Limits {
        uint64_t bytes;
        size_t ranges;
        // Requests of one reader separated by at most this many bytes share a read;
        // the gap is read and charged like the requested bytes.
        uint64_t coalesce_gap = 0;
    };

    IoBatch(MemoryBudget& budget, Limits limits) : budget_(budget), limits_(limits) {}
    IoBatch(const IoBatch&) = delete;
    IoBatch& operator=(const IoBatch&) = delete;

    // A rejected range leaves the handle and registered requests unchanged.
    // An oversized first range may use one wave; later ranges cannot increase its bytes.
    Status try_add(IoReader& reader, uint64_t offset, uint64_t len, bool* accepted, size_t* handle,
                   bool allow_oversized_first = false);
    Status fetch();
    // Views remain valid until the next fetch or clear.
    std::span<const uint8_t> get(size_t handle) const;
    // A pin keeps its physical bytes and memory charge alive across waves.
    Status pin(size_t handle, Pin* out);
    void clear();
    size_t pending() const { return handles_.size(); }
    MemoryBudget& memory_budget() const { return budget_; }

private:
    struct Handle {
        size_t reader;
        size_t range;
    };

    void release_buffers();

    MemoryBudget& budget_;
    Limits limits_;
    MemoryBudget::Reservation read_memory_;
    std::vector<std::unique_ptr<IoReadBatch>> readers_;
    std::vector<Handle> handles_;
    std::vector<std::vector<std::shared_ptr<Buffer>>> pins_;
    uint64_t bytes_ = 0;
    size_t ranges_ = 0;
};

} // namespace doris::index_query
