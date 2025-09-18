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

#include <climits>
#include <cstdint>

#include "common/exception.h"

namespace doris::segment_v2::inverted_index::query_v2 {

static constexpr uint32_t TERMINATED = static_cast<uint32_t>(INT_MAX);

class DocSet {
public:
    DocSet() = default;
    virtual ~DocSet() = default;

    virtual uint32_t advance() {
        throw doris::Exception(doris::ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "advance() method not implemented in base DocSet class");
    }

    virtual uint32_t seek(uint32_t target) {
        throw doris::Exception(doris::ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "seek() method not implemented in base DocSet class");
    }

    virtual uint32_t doc() const {
        throw doris::Exception(doris::ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "doc() method not implemented in base DocSet class");
    }

    virtual uint32_t size_hint() const {
        throw doris::Exception(doris::ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "size_hint() method not implemented in base DocSet class");
    }
};

} // namespace doris::segment_v2::inverted_index::query_v2