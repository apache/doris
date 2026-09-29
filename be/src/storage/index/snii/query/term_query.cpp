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

#include "storage/index/snii/query/term_query.h"

#include <vector>

#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/query/internal/docid_posting_reader.h"

namespace doris::snii::query {

using format::DictEntry;
using reader::LogicalIndexReader;

Status term_query(const LogicalIndexReader& idx, std::string_view term,
                  std::vector<uint32_t>* docids) {
    if (docids == nullptr)
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("term_query: null out");
    docids->clear();
    VectorDocIdSink sink(*docids);
    return term_query(idx, term, &sink);
}

Status term_query(const LogicalIndexReader& idx, std::string_view term, DocIdSink* sink) {
    if (sink == nullptr)
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("term_query: null sink");

    bool found = false;
    DictEntry entry;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
    RETURN_IF_ERROR(idx.lookup(term, &found, &entry, &frq_base, &prx_base));
    if (!found) return Status::OK();
    return internal::read_docid_posting(idx, entry, frq_base, prx_base, sink);
}

Status term_query(const LogicalIndexReader& idx, std::string_view term,
                  std::vector<uint32_t>* docids, QueryProfile* profile) {
    QueryProfileScope profile_scope(idx.reader(), profile);
    return term_query(idx, term, docids);
}

} // namespace doris::snii::query
