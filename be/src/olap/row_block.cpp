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

#include "olap/row_block.h"

#include <sys/mman.h>

#include <algorithm>
#include <cstring>
#include <limits>

#include "exprs/expr.h"
#include "olap/field.h"
#include "olap/row_cursor.h"
#include "olap/utils.h"

using std::exception;
using std::lower_bound;
using std::nothrow;
using std::pair;
using std::upper_bound;
using std::vector;

namespace doris {

RowBlock::RowBlock(const TabletSchema* schema) : _capacity(0), _schema(schema) {
    _mem_pool.reset(new MemPool("RowBlock"));
}

RowBlock::~RowBlock() {
    delete[] _mem_buf;
}

void RowBlock::init(const RowBlockInfo& block_info) {
    _info = block_info;
    _null_supported = block_info.null_supported;
    _capacity = _info.row_num;
    _compute_layout();
    _mem_buf = new char[_mem_buf_bytes];
}

OLAPStatus RowBlock::finalize(uint32_t row_num) {
    if (row_num > _capacity) {
        OLAP_LOG_WARNING(
                "Input row num is larger than internal row num."
                "[row_num=%u; _info.row_num=%u]",
                row_num, _info.row_num);
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }
    _info.row_num = row_num;
    return OLAP_SUCCESS;
}

void RowBlock::clear() {
    _info.row_num = _capacity;
    _info.checksum = 0;
    _pos = 0;
    _limit = 0;
    _mem_pool->clear();
}

void RowBlock::_compute_layout() {
    std::unordered_set<uint32_t> column_set(_info.column_ids.begin(), _info.column_ids.end());
    size_t memory_size = 0;
    for (int col_id = 0; col_id < _schema->num_columns(); ++col_id) {
        const TabletColumn& column = _schema->column(col_id);
        if (!column_set.empty() && column_set.find(col_id) == column_set.end()) {
            // which may lead BE crash
            _field_offset_in_memory.push_back(std::numeric_limits<std::size_t>::max());
            continue;
        }

        _field_offset_in_memory.push_back(memory_size);

        // All field has a nullbyte in memory
        if (column.is_length_variable_type()) {
            // 变长部分额外计算下实际最大的字符串长度（此处length已经包括记录Length的2个字节）
            memory_size += sizeof(Slice) + sizeof(char);
        } else {
            memory_size += column.length() + sizeof(char);
        }
    }
    _mem_row_bytes = memory_size;
    _mem_buf_bytes = _mem_row_bytes * _info.row_num;
}

} // namespace doris
