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

#include "storage/index/query/phrase/position_span.h"
#include "storage/index/query/spi/postings_cursor.h"

namespace doris::index_query {

// Retains the current position while the exact matcher advances a borrowed cursor.
class PositionStream {
public:
    Status reset(PostingsCursor& postings, uint32_t ordinal) {
        size_t count = 0;
        RETURN_IF_ERROR(postings.open_position_stream(ordinal, std::span(&_position, 1), &count,
                                                      &_positions));
        _available = count != 0;
        return Status::OK();
    }

    bool whole(PhrasePositionSpan* /*span*/) const { return false; }

    Status advance_to(uint32_t target, uint32_t* position, bool* available) {
        if (_available && _position < target) {
            RETURN_IF_ERROR(_positions->next_position_at_least(target, &_position, &_available));
        }
        *position = _position;
        *available = _available;
        return Status::OK();
    }

    Status finish_doc() { return _positions->finish_doc(); }

private:
    PositionCursor* _positions = nullptr;
    uint32_t _position = 0;
    bool _available = false;
};

} // namespace doris::index_query
