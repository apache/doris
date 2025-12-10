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

#include <memory>

#include "common/exception.h"
#include "olap/rowset/segment_v2/inverted_index/util/reader.h"

namespace doris::segment_v2::inverted_index {

class DorisCharFilter : public lucene::util::Reader {
public:
    DorisCharFilter(ReaderPtr reader) : _reader(std::move(reader)) {}
    ~DorisCharFilter() override = default;

    virtual void initialize() = 0;

    int64_t position() override {
        throw Exception(ErrorCode::INVERTED_INDEX_NOT_SUPPORTED, "CharFilter::position");
    }

    int64_t skip(int64_t ntoskip) override {
        throw Exception(ErrorCode::INVERTED_INDEX_NOT_SUPPORTED, "CharFilter::skip");
    }

    size_t size() override {
        throw Exception(ErrorCode::INVERTED_INDEX_NOT_SUPPORTED, "CharFilter::size");
    }

protected:
    ReaderPtr _reader;
};
using CharFilterPtr = std::shared_ptr<DorisCharFilter>;

} // namespace doris::segment_v2::inverted_index