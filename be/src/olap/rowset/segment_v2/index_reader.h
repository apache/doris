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

#include "olap/metadata_adder.h"

namespace doris {
class RuntimeState;
}

namespace doris::segment_v2 {

class IndexIterator;

class InvertedIndexReader;
using InvertedIndexReaderPtr = std::shared_ptr<InvertedIndexReader>;

class AnnIndexReader;
using AnnIndexReaderPtr = std::shared_ptr<AnnIndexReader>;

class IndexReader : public std::enable_shared_from_this<IndexReader>,
                    public MetadataAdder<IndexReader> {
public:
    IndexReader() = default;
    ~IndexReader() override = default;

    virtual uint64_t get_index_id() const = 0;

    virtual Status new_iterator(const io::IOContext& io_ctx, OlapReaderStatistics* stats,
                                RuntimeState* runtime_state,
                                std::unique_ptr<IndexIterator>* iterator) = 0;

    virtual bool is_fulltext_index() { return false; }
    virtual bool is_string_index() { return false; }
    virtual bool is_bkd_index() { return false; }

    virtual bool is_support_phrase() { return false; }
};
using IndexReaderPtr = std::shared_ptr<IndexReader>;

} // namespace doris::segment_v2