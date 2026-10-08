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

#include "storage/index/query/spi/index_source.h"

#include "storage/index/query/docid_sink.h"
#include "storage/index/query/exec/cursor_chained_postings.h"
#include "storage/index/query/exec/term_waves.h"

namespace doris::index_query {

Status IndexSource::collect_terms(std::span<const std::string> terms, DocIdSink& sink,
                                  bool* any_present) {
    bool present = false;
    RETURN_IF_ERROR(visit_term_postings(
            *this, terms, /*scoring=*/false, [&](size_t, PostingsCursor* cursor) -> Status {
                if (cursor == nullptr) {
                    return Status::OK();
                }
                present = true;
                return for_each_block(*cursor, [&sink](const PostingsBlock& block) {
                    return block.dense ? sink.append_range(block.range_begin, block.range_end)
                                       : sink.append_sorted(block.docs);
                });
            }));
    if (any_present != nullptr) {
        *any_present = present;
    }
    return Status::OK();
}

} // namespace doris::index_query
