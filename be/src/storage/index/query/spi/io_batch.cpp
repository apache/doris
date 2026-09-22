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

#include "storage/index/query/spi/io_batch.h"

#include <algorithm>
#include <utility>

namespace doris::index_query {

Status IoBatch::try_add(IoReader& reader, uint64_t offset, uint64_t len, bool* accepted,
                        size_t* handle, bool allow_oversized_first) {
    DORIS_CHECK(accepted != nullptr);
    DORIS_CHECK(handle != nullptr);
    *accepted = false;
    const auto it = std::ranges::find_if(
            readers_, [&](const auto& batch) { return batch->reader() == &reader; });
    const size_t reader_index = it - readers_.begin();
    const bool added_reader = it == readers_.end();
    if (added_reader) {
        readers_.push_back(std::make_unique<IoReadBatch>(&reader, limits_.coalesce_gap));
    }
    IoReadBatch& batch = *readers_[reader_index];
    const uint64_t other_bytes = bytes_ - batch.bounded_bytes_;
    const size_t other_ranges = ranges_ - batch.bounded_ranges_.size();
    size_t range = 0;
    uint64_t max_bytes = std::max(limits_.bytes, bytes_);
    if (allow_oversized_first && handles_.empty()) {
        max_bytes = std::max(max_bytes, len);
    }
    Status status = batch.try_add(offset, len, max_bytes - other_bytes,
                                  limits_.ranges - other_ranges, accepted, &range);
    if (!status.ok() || !*accepted) {
        if (added_reader) {
            readers_.pop_back();
        }
        return status;
    }
    bytes_ = other_bytes + batch.bounded_bytes_;
    ranges_ = other_ranges + batch.bounded_ranges_.size();
    *handle = handles_.size();
    handles_.push_back({.reader = reader_index, .range = range});
    return Status::OK();
}

void IoBatch::release_buffers() {
    for (auto& reader : readers_) {
        reader->release_buffers();
    }
    pins_.clear();
    read_memory_.reset();
}

Status IoBatch::fetch() {
    release_buffers();
    RETURN_IF_ERROR(budget_.reserve(bytes_, &read_memory_));
    for (auto& reader : readers_) {
        Status status = reader->fetch();
        if (!status.ok()) {
            release_buffers();
            return status;
        }
    }
    return Status::OK();
}

std::span<const uint8_t> IoBatch::get(size_t handle) const {
    const Handle& ref = handles_[handle];
    const IoReadBatch& batch = *readers_[ref.reader];
    if (!pins_.empty() && !pins_[ref.reader].empty()) {
        const auto& request = batch.reqs_[ref.range];
        if (const auto& owner = pins_[ref.reader][request.phys_idx]; owner != nullptr) {
            return std::span<const uint8_t>(owner->bytes)
                    .subspan(request.sub_offset, request.len_size);
        }
    }
    return batch.get(ref.range);
}

Status IoBatch::pin(size_t handle, Pin* out) {
    DORIS_CHECK(out != nullptr);
    const Handle& ref = handles_[handle];
    IoReadBatch& batch = *readers_[ref.reader];
    const auto& request = batch.reqs_[ref.range];
    pins_.resize(readers_.size());
    auto& reader_pins = pins_[ref.reader];
    reader_pins.resize(batch.phys_.size());
    auto& owner = reader_pins[request.phys_idx];
    if (owner == nullptr) {
        auto& bytes = batch.phys_[request.phys_idx];
        owner = std::make_shared<Buffer>();
        owner->memory = read_memory_.split(bytes.size());
        owner->bytes = std::move(bytes);
    }
    out->owner_ = owner;
    out->offset_ = request.sub_offset;
    out->length_ = request.len_size;
    return Status::OK();
}

void IoBatch::clear() {
    readers_.clear();
    pins_.clear();
    read_memory_.reset();
    handles_.clear();
    bytes_ = 0;
    ranges_ = 0;
}

} // namespace doris::index_query
