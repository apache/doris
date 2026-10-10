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

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>

#include "storage/index/query/docid_sink.h"

namespace doris::index_query {

class DocIdBuffer {
public:
    explicit DocIdBuffer(DocIdSink& sink) : _sink(sink) {}

    Status append(uint32_t doc) {
        _docs[_count++] = doc;
        return _count == _docs.size() ? flush() : Status::OK();
    }

    Status flush() {
        if (_count == 0) {
            return Status::OK();
        }
        auto status = _sink.append_sorted(std::span(_docs).first(_count));
        _count = 0;
        return status;
    }

private:
    DocIdSink& _sink;
    std::array<uint32_t, 256> _docs {};
    size_t _count = 0;
};

} // namespace doris::index_query
