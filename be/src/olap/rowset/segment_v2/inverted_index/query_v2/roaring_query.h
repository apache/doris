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

#include "gutil/integral_types.h"
#include "olap/rowset/segment_v2/inverted_index/query_v2/query.h"

namespace doris::segment_v2::idx_query_v2 {

class RoaringQuery : public Query {
public:
    RoaringQuery(const std::shared_ptr<roaring::Roaring>& roaring);
    ~RoaringQuery() override = default;

    void execute(const std::shared_ptr<roaring::Roaring>& result) {}

    int32_t doc_id() const { return _doc; }

    int32_t next_doc() const {
        if (_iter == _end) {
            _iter = _roaring->begin();
        } else {
            ++_iter;
        }
        _doc = (_iter != _end) ? *_iter : INT_MAX;
        return _doc;
    }

    int32_t advance(int32_t target) const {
        while (_iter != _end && *_iter < target) {
            ++_iter;
        }
        _doc = (_iter != _end) ? *_iter : INT_MAX;
        return _doc;
    }

    int64_t cost() const { return _roaring->cardinality(); }

private:
    mutable int32_t _doc = -1;
    std::shared_ptr<roaring::Roaring> _roaring;
    mutable roaring::Roaring::const_iterator _iter;
    roaring::Roaring::const_iterator _end;
};

} // namespace doris::segment_v2::idx_query_v2