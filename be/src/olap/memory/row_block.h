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

#include "olap/memory/column_block.h"
#include "olap/memory/column_reader.h"
#include "olap/memory/common.h"

namespace doris {
namespace memory {

class RowBlock {
public:
    size_t num_rows() const { return _nrows; }
    size_t num_columns() const { return _columns.size(); }
    const ColumnBlock& get_column(size_t idx) const { return *_columns[idx].get(); }

private:
    friend class MemTabletScan;
    explicit RowBlock(size_t num_columns);

    size_t _nrows = 0;
    vector<ColumnBlockHolder> _columns;
};

} // namespace memory
} // namespace doris
