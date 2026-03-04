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

#include <cctz/time_zone.h>

#include <memory>

#include "common/status.h"
#include "vec/columns/column.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type.h"

// This file will convert Doris Block to/from Arrow's RecordBatch
// Block is used by Doris query engine to exchange data between
// each execute node.

namespace arrow {

class MemoryPool;
class RecordBatch;
class Schema;

} // namespace arrow

namespace doris {

class FromBlockToRecordBatchConverter {
public:
    FromBlockToRecordBatchConverter(const vectorized::Block& block,
                                    const std::shared_ptr<arrow::Schema>& schema,
                                    arrow::MemoryPool* pool, const cctz::time_zone& timezone_obj)
            : _block(block),
              _schema(schema),
              _pool(pool),
              _cur_field_idx(-1),
              _timezone_obj(timezone_obj),
              _row_range_start(0),
              _row_range_end(0) {}

    FromBlockToRecordBatchConverter(const vectorized::Block& block,
                                    const std::shared_ptr<arrow::Schema>& schema,
                                    arrow::MemoryPool* pool, const cctz::time_zone& timezone_obj,
                                    size_t start_row, size_t end_row)
            : _block(block),
              _schema(schema),
              _pool(pool),
              _cur_field_idx(-1),
              _timezone_obj(timezone_obj),
              _row_range_start(start_row),
              _row_range_end(end_row) {}

    ~FromBlockToRecordBatchConverter() = default;

    Status convert(std::shared_ptr<arrow::RecordBatch>* out);

private:
    const vectorized::Block& _block;
    const std::shared_ptr<arrow::Schema>& _schema;
    arrow::MemoryPool* _pool;

    size_t _cur_field_idx;
    size_t _cur_start;
    size_t _cur_rows;
    vectorized::ColumnPtr _cur_col;
    vectorized::DataTypePtr _cur_type;
    arrow::ArrayBuilder* _cur_builder = nullptr;

    const cctz::time_zone& _timezone_obj;

    // Row range for zero-copy slicing (0 means use all rows from _row_range_start)
    size_t _row_range_start;
    size_t _row_range_end;

    std::vector<std::shared_ptr<arrow::Array>> _arrays;
};

class FromRecordBatchToBlockConverter {
public:
    FromRecordBatchToBlockConverter(const std::shared_ptr<arrow::RecordBatch>& batch,
                                    const vectorized::DataTypes& types,
                                    const cctz::time_zone& timezone_obj)
            : _batch(batch), _types(types), _timezone_obj(timezone_obj) {}

    ~FromRecordBatchToBlockConverter() = default;

    Status convert(vectorized::Block* block);

private:
    const std::shared_ptr<arrow::RecordBatch>& _batch;
    const vectorized::DataTypes& _types;
    const cctz::time_zone& _timezone_obj;
    vectorized::ColumnsWithTypeAndName _columns;
};

Status convert_to_arrow_batch(const vectorized::Block& block,
                              const std::shared_ptr<arrow::Schema>& schema, arrow::MemoryPool* pool,
                              std::shared_ptr<arrow::RecordBatch>* result,
                              const cctz::time_zone& timezone_obj);

Status convert_to_arrow_batch(const vectorized::Block& block,
                              const std::shared_ptr<arrow::Schema>& schema, arrow::MemoryPool* pool,
                              std::shared_ptr<arrow::RecordBatch>* result,
                              const cctz::time_zone& timezone_obj, size_t start_row,
                              size_t end_row);

Status convert_from_arrow_batch(const std::shared_ptr<arrow::RecordBatch>& batch,
                                const vectorized::DataTypes& types, vectorized::Block* block,
                                const cctz::time_zone& timezone_obj);

} // namespace doris
