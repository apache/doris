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

#include "common/status.h"

namespace doris {
class Block;
class RuntimeProfile;

enum class SortDirection : uint8_t {
    ASC,  // small to large (0 -> N)
    DESC, // large to small (N -> 0)
};

// One read mode of BlockReader: returns the rows of the read Block by Block.
class BlockIterator {
public:
    BlockIterator() = default;
    virtual ~BlockIterator() = default;

    BlockIterator(const BlockIterator&) = delete;
    BlockIterator& operator=(const BlockIterator&) = delete;
    BlockIterator(BlockIterator&&) = delete;
    BlockIterator& operator=(BlockIterator&&) = delete;

    // Fill `block`, which must be empty, with the next batch of rows; `*eof` once there are none.
    virtual Status next_block(Block* block, bool* eof) = 0;

    virtual void update_profile(RuntimeProfile* /*profile*/) {}
};

} // namespace doris
