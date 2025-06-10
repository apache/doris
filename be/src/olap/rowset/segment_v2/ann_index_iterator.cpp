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

namespace doris::segment_v2 {

AnnIndexIterator::AnnIndexIterator(const io::IOContext& io_ctx, OlapReaderStatistics* stats,
                                   RuntimeState* runtime_state, const IndexReaderPtr& reader)
        : IndexIterator(io_ctx, stats, runtime_state) {
    _ann_reader = std::dynamic_pointer_cast<AnnIndexReader>(reader);
}

Status AnnIndexIterator::read_from_index(const IndexParam& param) {
    auto* a_param = std::get<AnnIndexParam*>(param);
    if (a_param == nullptr) {
        return Status::Error<ErrorCode::INDEX_INVALID_PARAMETERS>("a_param is null");
    }

    return _ann_reader->query(&_io_ctx, a_param);
}

Status AnnIndexIterator::range_search(const RangeSearchParams& params,
                                      const CustomSearchParams& custom_params,
                                      RangeSearchResult* result) {
    if (_ann_reader == nullptr) {
        return Status::Error<ErrorCode::INDEX_INVALID_PARAMETERS>("_ann_reader is null");
    }

    return _ann_reader->range_search(params, custom_params, result, &_io_ctx);
}

} // namespace doris::segment_v2