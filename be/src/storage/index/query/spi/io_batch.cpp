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

namespace doris::index_query {

Status IoBatch::try_add(IoReader& reader, uint64_t offset, uint64_t len, bool* accepted,
                        size_t* handle) {
    DORIS_CHECK(accepted != nullptr);
    DORIS_CHECK(handle != nullptr);
    *accepted = false;
    const auto it = std::ranges::find_if(
            readers_, [&](const auto& batch) { return batch->reader() == &reader; });
    const size_t reader_index = it - readers_.begin();
    const bool added_reader = it == readers_.end();
    if (added_reader) {
        readers_.push_back(std::make_unique<IoReadBatch>(&reader));
    }
    IoReadBatch& batch = *readers_[reader_index];
    const uint64_t other_bytes = bytes_ - batch.bounded_bytes_;
    const size_t other_ranges = ranges_ - batch.bounded_ranges_.size();
    size_t range = 0;
    Status status = batch.try_add(offset, len, limits_.bytes - other_bytes,
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
    return readers_[ref.reader]->get(ref.range);
}

void IoBatch::clear() {
    readers_.clear();
    read_memory_.reset();
    handles_.clear();
    bytes_ = 0;
    ranges_ = 0;
}

} // namespace doris::index_query
