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

namespace doris::io {
struct IOContext;
} // namespace doris::io

namespace doris::segment_v2 {

class TermPositionsIterator;
using TermPositionsIterPtr = std::shared_ptr<TermPositionsIterator>;

class TermPositionsIterator : public TermIterator {
public:
    using TermPositionsPtr = std::unique_ptr<TermPositions, CLuceneDeleter>;

    TermPositionsIterator() = default;
    TermPositionsIterator(TermPositionsPtr term_positions)
            : TermIterator(std::move(term_positions)) {
        term_poss_ = dynamic_cast<TermPositions*>(term_docs_.get());
    }
    ~TermPositionsIterator() override = default;

    int32_t next_position() const { return term_poss_->nextPosition(); }

    static TermPositionsIterPtr create(const io::IOContext* io_ctx, IndexReader* reader,
                                       const std::wstring& field_name, const std::string& term) {
        return create(io_ctx, reader, field_name, StringUtil::string_to_wstring(term));
    }

    static TermPositionsIterPtr create(const io::IOContext* io_ctx, IndexReader* reader,
                                       const std::wstring& field_name,
                                       const std::wstring& ws_term) {
        auto* t = _CLNEW Term(field_name.c_str(), ws_term.c_str());
        auto* term_pos = reader->termPositions(t, io_ctx);
        _CLDECDELETE(t);
        return std::make_shared<TermPositionsIterator>(
                TermPositionsPtr(term_pos, CLuceneDeleter {}));
    }

private:
    TermPositions* term_poss_ = nullptr;
};

} // namespace doris::segment_v2