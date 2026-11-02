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
#include <span>
#include <vector>

#include "common/status.h"

namespace doris::index_query {

// A borrowed view of one document's positions. Opening another document or
// changing the postings block invalidates this cursor.
class PositionCursor {
public:
    virtual ~PositionCursor() = default;
    virtual Status next_position(uint32_t* position, bool* available) = 0;
    // Consumes unread positions through the first one at or after target.
    virtual Status next_position_at_least(uint32_t target, uint32_t* position, bool* available) {
        *available = false;
        uint32_t next = 0;
        bool has_next = false;
        while (true) {
            RETURN_IF_ERROR(next_position(&next, &has_next));
            if (!has_next) {
                return Status::OK();
            }
            if (next >= target) {
                *position = next;
                *available = true;
                return Status::OK();
            }
        }
    }

    // The next positions into `out`, filling it unless fewer are left: a *count below out.size()
    // means the document has none after them.
    virtual Status next_positions(std::span<uint32_t> out, size_t* count) {
        size_t filled = 0;
        bool available = true;
        while (filled < out.size()) {
            RETURN_IF_ERROR(next_position(&out[filled], &available));
            if (!available) {
                break;
            }
            ++filled;
        }
        *count = filled;
        return Status::OK();
    }
    virtual Status finish_doc() = 0;

    virtual Status append_remaining_positions(uint32_t offset, std::vector<uint32_t>& output) {
        uint32_t position = 0;
        bool available = false;
        while (true) {
            RETURN_IF_ERROR(next_position(&position, &available));
            if (!available) {
                return finish_doc();
            }
            output.push_back(position + offset);
        }
    }
};

} // namespace doris::index_query
