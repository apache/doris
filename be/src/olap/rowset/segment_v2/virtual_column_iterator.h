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

#include <sys/types.h>

#include <cstdint>
#include <unordered_map>

#include "column_reader.h"
#include "common/be_mock_util.h"
#include "vec/columns/column.h"

namespace doris::segment_v2 {

class VirtualColumnIterator : public ColumnIterator {
public:
    VirtualColumnIterator();
    ~VirtualColumnIterator() override = default;

    MOCK_FUNCTION void prepare_materialization(vectorized::IColumn::Ptr column,
                                               std::unique_ptr<std::vector<uint64_t>> labels);

    Status init(const ColumnIteratorOptions& opts) override;

    Status seek_to_ordinal(ordinal_t ord_idx) override;

    Status next_batch(size_t* n, vectorized::MutableColumnPtr& dst, bool* has_null) override;

    Status read_by_rowids(const rowid_t* rowids, const size_t count,
                          vectorized::MutableColumnPtr& dst) override;

    ordinal_t get_current_ordinal() const override { return 0; }

#ifdef BE_TEST
    vectorized::IColumn::Ptr get_materialized_column() const { return _materialized_column_ptr; }
    const std::unordered_map<size_t, size_t>& get_row_id_to_idx() const { return _row_id_to_idx; }
#endif
private:
    vectorized::IColumn::Ptr _materialized_column_ptr;
    // segment rowid to index in column.
    std::unordered_map<size_t, size_t> _row_id_to_idx;
    doris::vectorized::IColumn::Filter _filter;
    size_t _size = 0;
    size_t _max_ordinal = 0;
    ordinal_t _current_ordinal = 0;
};

} // namespace doris::segment_v2