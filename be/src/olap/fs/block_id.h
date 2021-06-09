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

#include <cinttypes>
#include <cstddef>
#include <cstdint>
#include <iosfwd>
#include <string>
#include <unordered_set>
#include <vector>

#include "gutil/stringprintf.h"

namespace doris {

// Block is the smallest unit of reading and writing.
// In the future, each BlockId should become a relatively simple structure,
// such as a uint64_t. But now, we don't have a mapping management structure
// from uint64_t to files, so we need to save the file name in BlockId.
class BlockId {
public:
    BlockId() : _id(kInvalidId) {}

    explicit BlockId(uint64_t id) : _id(id) {}

    void set_id(uint64_t id) { _id = id; }

    bool is_null() const { return _id == kInvalidId; }

    std::string to_string() const { return StringPrintf("%016" PRIu64, _id); }

    bool operator==(const BlockId& other) const { return _id == other._id; }

    bool operator!=(const BlockId& other) const { return _id != other._id; }

    bool operator<(const BlockId& other) const { return _id < other._id; }

    // Returns the raw ID. Use with care; in most cases the BlockId should be
    // treated as a completely opaque value.
    uint64_t id() const { return _id; }

    // Join the given block IDs with ','. Useful for debug printouts.
    static std::string join_strings(const std::vector<BlockId>& blocks);

private:
    static const uint64_t kInvalidId;

    uint64_t _id;
};

std::ostream& operator<<(std::ostream& o, const BlockId& block_id);

struct BlockIdHash {
    // size_t is same as uint64_t
    size_t operator()(const BlockId& block_id) const { return block_id.id(); }
};

struct BlockIdCompare {
    bool operator()(const BlockId& first, const BlockId& second) const { return first < second; }
};

struct BlockIdEqual {
    bool operator()(const BlockId& first, const BlockId& second) const { return first == second; }
};

typedef std::unordered_set<BlockId, BlockIdHash, BlockIdEqual> BlockIdSet;
typedef std::vector<BlockId> BlockIdContainer;

} // end namespace doris
