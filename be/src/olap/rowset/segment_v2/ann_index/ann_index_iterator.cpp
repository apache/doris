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

#include "ann_index_iterator.h"

#include <memory>

#include "olap/rowset/segment_v2/ann_index/ann_search_params.h"

namespace doris::segment_v2 {
#include "common/compile_check_begin.h"
AnnIndexIterator::AnnIndexIterator(const IndexReaderPtr& reader) : IndexIterator() {
    _ann_reader = std::dynamic_pointer_cast<AnnIndexReader>(reader);
}

Status AnnIndexIterator::read_from_index(const IndexParam& param) {
    auto* a_param = std::get<segment_v2::AnnTopNParam*>(param);
    if (a_param == nullptr) {
        return Status::Error<ErrorCode::INDEX_INVALID_PARAMETERS>("a_param is null");
    }
    if (_ann_reader == nullptr) {
        return Status::Error<ErrorCode::INDEX_INVALID_PARAMETERS>("_ann_reader is null");
    }

    // _context may be unset in some test scenarios; pass nullptr IOContext in that case.
    io::IOContext* io_ctx = (_context != nullptr) ? _context->io_ctx : nullptr;
    return _ann_reader->query(io_ctx, a_param, a_param->stats.get());
}

Status AnnIndexIterator::range_search(const AnnRangeSearchParams& params,
                                      const VectorSearchUserParams& custom_params,
                                      segment_v2::AnnRangeSearchResult* result,
                                      segment_v2::AnnIndexStats* stats) {
    if (_ann_reader == nullptr) {
        return Status::Error<ErrorCode::INDEX_INVALID_PARAMETERS>("_ann_reader is null");
    }

    // _context may be null when iterator is used in isolation (e.g., unit tests).
    io::IOContext* io_ctx = (_context != nullptr) ? _context->io_ctx : nullptr;
    return _ann_reader->range_search(params, custom_params, result, stats, io_ctx);
}
#include "common/compile_check_end.h"
} // namespace doris::segment_v2