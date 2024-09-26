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

#include "olap/rowset/segment_v2/inverted_index_compound_reader.h"
#include "olap/rowset/segment_v2/inverted_index_file_reader.h"
#include "olap/tablet_schema.h"

#define FINALIZE_INPUT(x) \
    if (x != nullptr) {   \
        x->close();       \
        _CLDELETE(x);     \
    }
#define FINALLY_FINALIZE_INPUT(x) \
    try {                         \
        FINALIZE_INPUT(x)         \
    } catch (...) {               \
    }
namespace doris::segment_v2::inverted_index {
class InvertedIndexReader {
    ENABLE_FACTORY_CREATOR(InvertedIndexReader);

public:
    explicit InvertedIndexReader(
            const TabletIndex* index_meta,
            std::shared_ptr<InvertedIndexFileReader> inverted_index_file_reader)
            : _inverted_index_file_reader(std::move(inverted_index_file_reader)),
              _index_meta(*index_meta) {}

    ~InvertedIndexReader() = default;

    [[nodiscard]] Status init_index_reader();
    std::shared_ptr<lucene::index::IndexReader> get_index_reader() { return _index_reader; }
    [[nodiscard]] uint64_t get_index_id() const { return _index_meta.index_id(); }

    [[nodiscard]] const std::map<string, string>& get_index_properties() const {
        return _index_meta.properties();
    }

    Result<std::shared_ptr<roaring::Roaring>> read_null_bitmap();

private:
    std::shared_ptr<InvertedIndexFileReader> _inverted_index_file_reader = nullptr;
    std::shared_ptr<lucene::index::IndexReader> _index_reader = nullptr;
    TabletIndex _index_meta;
    bool _inited = false;
};
using InvertedIndexReaderPtr = std::shared_ptr<InvertedIndexReader>;
} // namespace doris::segment_v2::inverted_index