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

#include <CLucene.h>
#include <CLucene/index/IndexReader.h>

#include <ranges>

#include "common/exception.h"
#include "olap/rowset/segment_v2/inverted_index/util/string_helper.h"

CL_NS_USE(index)

namespace doris::segment_v2::inverted_index::query_v2 {

class CompositeReader {
public:
    CompositeReader() = default;
    ~CompositeReader() = default;

    void set_reader(const std::wstring& field, lucene::index::IndexReader* reader) {
        if (_field_readers.contains(field)) {
            throw Exception(ErrorCode::INDEX_INVALID_PARAMETERS, "Field {} already exists",
                            StringHelper::to_string(field));
        }
        _field_readers[field] = reader;
    }

    lucene::index::IndexReader* get_reader(const std::wstring& field) {
        if (!_field_readers.contains(field)) {
            throw Exception(ErrorCode::NOT_FOUND, "Field {} not found",
                            StringHelper::to_string(field));
        }
        return _field_readers[field];
    }

    void close() {
        for (auto* reader : std::views::values(_field_readers)) {
            reader->close();
        }
    }

private:
    std::unordered_map<std::wstring, lucene::index::IndexReader*> _field_readers;
};
using CompositeReaderPtr = std::unique_ptr<CompositeReader>;

} // namespace doris::segment_v2::inverted_index::query_v2