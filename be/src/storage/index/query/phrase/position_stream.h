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
#include "storage/index/query/spi/position_cursor.h"

namespace doris::index_query {

// Retains the current position while the exact matcher advances a borrowed cursor.
class PositionStream {
public:
    Status reset(PositionCursor* positions) {
        _positions = positions;
        _whole = positions->frequency() <= 1;
        return _positions->next_position(&_position, &_available);
    }

    bool whole(PhrasePositionSpan* span) const {
        if (!_whole) {
            return false;
        }
        *span = {&_position, &_position + (_available ? 1 : 0)};
        return true;
    }

    Status advance_to(uint32_t target, uint32_t* position, bool* available) {
        while (_available && _position < target) {
            RETURN_IF_ERROR(_positions->next_position(&_position, &_available));
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
    bool _whole = false;
};

} // namespace doris::index_query
