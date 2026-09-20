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
#include <optional>
#include <span>
#include <vector>

#include "common/status.h"

namespace doris::index_query {

// A borrowed view of one document's positions. Opening another document or
// changing the postings block invalidates this cursor and any returned span.
class PositionCursor {
public:
    virtual ~PositionCursor() = default;
    virtual uint32_t frequency() const = 0;
    virtual Status next_position(uint32_t* position, bool* available) = 0;
    virtual Status finish_doc() = 0;

    // An absent view requires streaming; an empty span is a materialized empty document.
    virtual std::optional<std::span<const uint32_t>> view() const { return std::nullopt; }

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
