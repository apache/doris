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

#include "term_iterator.h"

CL_NS_USE(index)

namespace doris::segment_v2 {

class TermPositionIterator : public TermIterator {
public:
    TermPositionIterator() = default;
    TermPositionIterator(TermPositions* term_positions)
            : TermIterator(term_positions), _term_pos(term_positions) {}
    ~TermPositionIterator() override = default;

    int32_t next_position() const { return _term_pos->nextPosition(); }

    static TermPositions* ensure_term_position(IndexReader* reader, const std::wstring& field_name,
                                               const std::string& term) {
        std::wstring ws_term = StringUtil::string_to_wstring(term);
        return ensure_term_position(reader, field_name, ws_term);
    }

    static TermPositions* ensure_term_position(IndexReader* reader, const std::wstring& field_name,
                                               const std::wstring& ws_term) {
        auto* t = _CLNEW Term(field_name.c_str(), ws_term.c_str());
        auto* term_pos = reader->termPositions(t);
        _CLDECDELETE(t);
        return term_pos;
    }

private:
    TermPositions* _term_pos = nullptr;
};
using TermPosIterPtr = std::shared_ptr<TermPositionIterator>;

} // namespace doris::segment_v2