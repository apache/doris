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

#include <gen_cpp/olap_file.pb.h>

#include <variant>

#include "common/exception.h"
#include "common/factory_creator.h"
#include "olap/rowset/segment_v2/index_query_context.h"
#include "olap/rowset/segment_v2/index_reader.h"
#include "olap/rowset/segment_v2/inverted_index_query_type.h"
#include "runtime/runtime_state.h"

namespace doris::segment_v2 {

class InvertedIndexQueryCacheHandle;

struct InvertedIndexParam;
using IndexParam = std::variant<InvertedIndexParam*>;
using IndexReaderType = std::variant<InvertedIndexReaderType>;

class IndexIterator {
public:
    IndexIterator() = default;
    virtual ~IndexIterator() = default;

    virtual IndexReaderPtr get_reader(IndexReaderType reader_type) const = 0;
    virtual Status read_from_index(const IndexParam& param) = 0;

    virtual Status read_null_bitmap(InvertedIndexQueryCacheHandle* cache_handle) = 0;
    virtual Result<bool> has_null() = 0;

    void set_context(const IndexQueryContextPtr& context) { _context = context; }

protected:
    IndexQueryContextPtr _context = nullptr;
};

} // namespace doris::segment_v2