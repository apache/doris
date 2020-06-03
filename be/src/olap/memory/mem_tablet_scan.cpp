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

#include "olap/memory/mem_tablet_scan.h"

#include "olap/memory/column_reader.h"
#include "olap/memory/mem_sub_tablet.h"
#include "olap/memory/mem_tablet.h"

namespace doris {
namespace memory {

MemTabletScan::~MemTabletScan() {}

MemTabletScan::MemTabletScan(std::shared_ptr<MemTablet>&& tablet, std::unique_ptr<ScanSpec>* spec,
                             size_t num_rows, std::vector<std::unique_ptr<ColumnReader>>* readers)
        : _tablet(std::move(tablet)),
          _schema(_tablet->_mem_schema.get()),
          _spec(std::move(*spec)),
          _num_rows(num_rows),
          _readers(std::move(*readers)) {
    _row_block.reset(new RowBlock(_readers.size()));
    _next_block = 0;
    _num_blocks = num_block(_num_rows, Column::BLOCK_SIZE);
}

Status MemTabletScan::next_block(const RowBlock** block) {
    if (_next_block >= _num_blocks) {
        *block = nullptr;
        return Status::OK();
    }
    size_t rows_in_block =
            std::min((size_t)Column::BLOCK_SIZE, _num_rows - _next_block * Column::BLOCK_SIZE);
    _row_block->_nrows = rows_in_block;
    for (size_t i = 0; i < _readers.size(); ++i) {
        RETURN_IF_ERROR(
                _readers[i]->get_block(rows_in_block, _next_block, &_row_block->_columns[i]));
    }
    _next_block++;
    *block = _row_block.get();
    return Status::OK();
}

} // namespace memory
} // namespace doris
