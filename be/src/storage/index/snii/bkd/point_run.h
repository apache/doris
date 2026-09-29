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

#include <cstdint>
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/common/slice.h"

// Stores one sorted run of fixed-width point records for sequential merge reads.
namespace doris::snii::bkd {

// Append-only sink for one run. The caller owns the path and its removal; this
// type owns only the descriptor.
class PointRunWriter {
public:
    PointRunWriter() = default;
    ~PointRunWriter();

    PointRunWriter(const PointRunWriter&) = delete;
    PointRunWriter& operator=(const PointRunWriter&) = delete;

    Status open(const std::string& path);
    // `records` is a whole number of records; partial writes are retried until
    // the slice is on disk.
    Status append(Slice records);
    // Flushes and releases the descriptor. Safe to call once; the destructor
    // closes an un-closed descriptor without reporting.
    Status close();

private:
    int fd_ = -1;
};

// Forward-only cursor that keeps at most buffer_records records resident.
class PointRunReader {
public:
    PointRunReader() = default;
    ~PointRunReader();

    PointRunReader(const PointRunReader&) = delete;
    PointRunReader& operator=(const PointRunReader&) = delete;

    // Positions the cursor on the first record, so current() is valid
    // immediately unless the run is empty.
    Status open(const std::string& path, uint32_t record_size, uint32_t buffer_records);

    bool exhausted() const { return cursor_ >= valid_bytes_ && eof_; }
    // The record under the cursor. Valid until the next advance(). Calling this
    // on an exhausted cursor is a caller bug, not a runtime condition.
    Slice current() const;
    Status advance();

    // Bytes this cursor actually holds resident. Reported rather than recomputed
    // from the open() arguments, so a memory bound can be checked against what
    // was allocated instead of against the arithmetic that asked for it.
    uint64_t resident_buffer_bytes() const { return buffer_.size(); }

private:
    Status fill();

    int fd_ = -1;
    uint32_t record_size_ = 0;
    std::vector<uint8_t> buffer_;
    size_t valid_bytes_ = 0;
    size_t cursor_ = 0;
    bool eof_ = false;
};

} // namespace doris::snii::bkd
