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
#include <type_traits>

#include "storage/segment/column_reader.h"

namespace doris::segment_v2 {

class ColumnReadAheadController;

// Both instantiations inherit their cursor, access paths and lazy-materialization state. The
// string specialization also inherits StringFileColumnIterator's OFFSET-only and CHAR rules.
template <typename Base>
class ReadAheadIterator final : public Base {
    static_assert(std::is_base_of_v<FileColumnIterator, Base>);

public:
    explicit ReadAheadIterator(std::shared_ptr<ColumnReader> reader);
    ~ReadAheadIterator() override;

    Status init(const ColumnIteratorOptions& opts) override;
    Status prepare_read_ahead(const ColumnReadAheadHint& hint) override;
    Status init_prefetcher(const SegmentPrefetchParams& params) override;
    void collect_prefetchers(
            std::map<PrefetcherInitMethod, std::vector<SegmentPrefetcher*>>& prefetchers,
            PrefetcherInitMethod init_method) override;

protected:
    using PageReadContext = typename Base::PageReadContext;
    PageReadContext _before_page_read(int32_t page_index) override;
    void _after_page_read(int32_t page_index, const PagePointer& page) override;

private:
    std::unique_ptr<ColumnReadAheadController> _read_ahead;
};

extern template class ReadAheadIterator<FileColumnIterator>;
extern template class ReadAheadIterator<StringFileColumnIterator>;

using ReadAheadFileColumnIterator = ReadAheadIterator<FileColumnIterator>;
using ReadAheadStringColumnIterator = ReadAheadIterator<StringFileColumnIterator>;

} // namespace doris::segment_v2
