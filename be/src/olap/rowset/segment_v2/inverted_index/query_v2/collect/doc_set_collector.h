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
#include <vector>

namespace doris::segment_v2::inverted_index::query_v2 {

class DocSetCollector {
public:
    DocSetCollector() = default;
    explicit DocSetCollector(size_t capacity) { _docs.reserve(capacity); }

    void collect(uint32_t doc_id) { _docs.push_back(doc_id); }

    size_t size() const { return _docs.size(); }
    bool empty() const { return _docs.empty(); }

    std::vector<uint32_t>& docs() { return _docs; }
    const std::vector<uint32_t>& docs() const { return _docs; }

    [[nodiscard]] std::vector<uint32_t> into_vec() { return std::move(_docs); }

private:
    std::vector<uint32_t> _docs;
};

} // namespace doris::segment_v2::inverted_index::query_v2
