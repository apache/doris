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
#include <variant>

#include "common/status.h"
#include "storage/index/index_reader.h"

namespace doris::segment_v2 {

struct AnnTopNParam;
struct IndexQueryContext;
using IndexQueryContextPtr = std::shared_ptr<IndexQueryContext>;

class InvertedIndexQueryCacheHandle;
enum class InvertedIndexReaderType;

struct InvertedIndexParam;
using IndexParam = std::variant<InvertedIndexParam*, segment_v2::AnnTopNParam*>;

enum class AnnIndexReaderType {
    ANN = 0,
};

using IndexReaderType = std::variant<InvertedIndexReaderType, AnnIndexReaderType>;
class IndexIterator {
public:
    IndexIterator() = default;
    virtual ~IndexIterator() = default;

    virtual IndexReaderPtr get_reader(IndexReaderType reader_type) const = 0;
    virtual Status read_from_index(const IndexParam& param) = 0;

    virtual Status read_null_bitmap(InvertedIndexQueryCacheHandle* cache_handle) = 0;
    virtual Result<bool> has_null() = 0;

    void set_context(const IndexQueryContextPtr& context) { _context = context; }
    IndexQueryContextPtr get_context() const { return _context; }

protected:
    IndexQueryContextPtr _context = nullptr;
};

} // namespace doris::segment_v2
