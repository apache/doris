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

#include "util/arrow/block_convertor.h"

#include <arrow/array/builder_base.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_decimal.h>
#include <arrow/array/builder_nested.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/util/decimal.h>
#include <arrow/visit_type_inline.h>
#include <arrow/visitor.h>
#include <cctz/time_zone.h>
#include <glog/logging.h>

#include <ctime>
#include <memory>
#include <utility>
#include <vector>

#include "common/status.h"
#include "util/arrow/row_batch.h"
#include "util/arrow/utils.h"
#include "vec/columns/column.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/runtime/vdatetime_value.h"

namespace arrow {
class Array;
} // namespace arrow

namespace doris {
#include "common/compile_check_begin.h"

Status FromBlockToRecordBatchConverter::convert(std::shared_ptr<arrow::RecordBatch>* out) {
    int num_fields = _schema->num_fields();
    if (_block.columns() != num_fields) {
        return Status::InvalidArgument("number fields not match");
    }

    // Calculate actual row range to convert
    size_t actual_start = _row_range_start;
    size_t actual_rows = _row_range_end > 0 ? (_row_range_end - _row_range_start)
                                            : (_block.rows() - _row_range_start);

    // Validate range
    if (actual_start + actual_rows > _block.rows()) {
        return Status::InvalidArgument(
                "Row range out of bounds: start={}, num_rows={}, block_rows={}", actual_start,
                actual_rows, _block.rows());
    }

    _arrays.resize(num_fields);

    for (int idx = 0; idx < num_fields; ++idx) {
        _cur_field_idx = idx;
        _cur_start = actual_start;
        _cur_rows = actual_rows;
        _cur_col = _block.get_by_position(idx).column;
        _cur_type = _block.get_by_position(idx).type;
        auto column = _cur_col->convert_to_full_column_if_const();
        auto arrow_type = _schema->field(idx)->type();
        if (arrow_type->name() == "utf8" && column->byte_size() >= MAX_ARROW_UTF8) {
            arrow_type = arrow::large_utf8();
        }
        std::unique_ptr<arrow::ArrayBuilder> builder;
        auto arrow_st = arrow::MakeBuilder(_pool, arrow_type, &builder);
        if (!arrow_st.ok()) {
            return to_doris_status(arrow_st);
        }
        _cur_builder = builder.get();
        try {
            RETURN_IF_ERROR(_cur_type->get_serde()->write_column_to_arrow(
                    *column, nullptr, _cur_builder, _cur_start, _cur_start + _cur_rows,
                    _timezone_obj));
        } catch (std::exception& e) {
            return Status::InternalError(
                    "Fail to convert block data to arrow data, type: {}, name: {}, error: {}",
                    _cur_type->get_name(), _block.get_by_position(idx).name, e.what());
        }
        arrow_st = _cur_builder->Finish(&_arrays[_cur_field_idx]);
        if (!arrow_st.ok()) {
            return to_doris_status(arrow_st);
        }
    }
    *out = arrow::RecordBatch::Make(_schema, actual_rows, std::move(_arrays));
    return Status::OK();
}

Status FromRecordBatchToBlockConverter::convert(vectorized::Block* block) {
    DCHECK(block);
    int num_fields = _batch->num_columns();
    if ((size_t)num_fields != _types.size()) {
        return Status::InvalidArgument("number fields not match");
    }

    int64_t num_rows = _batch->num_rows();
    _columns.reserve(num_fields);

    for (int idx = 0; idx < num_fields; ++idx) {
        auto doris_type = _types[idx];
        auto doris_column = doris_type->create_column();
        auto arrow_column = _batch->column(idx);
        DCHECK_EQ(arrow_column->length(), num_rows);
        RETURN_IF_ERROR(doris_type->get_serde()->read_column_from_arrow(
                *doris_column, &*arrow_column, 0, num_rows, _timezone_obj));
        _columns.emplace_back(std::move(doris_column), std::move(doris_type), std::to_string(idx));
    }

    block->swap(_columns);
    return Status::OK();
}

Status convert_to_arrow_batch(const vectorized::Block& block,
                              const std::shared_ptr<arrow::Schema>& schema, arrow::MemoryPool* pool,
                              std::shared_ptr<arrow::RecordBatch>* result,
                              const cctz::time_zone& timezone_obj) {
    FromBlockToRecordBatchConverter converter(block, schema, pool, timezone_obj);
    return converter.convert(result);
}

Status convert_to_arrow_batch(const vectorized::Block& block,
                              const std::shared_ptr<arrow::Schema>& schema, arrow::MemoryPool* pool,
                              std::shared_ptr<arrow::RecordBatch>* result,
                              const cctz::time_zone& timezone_obj, size_t start_row,
                              size_t end_row) {
    FromBlockToRecordBatchConverter converter(block, schema, pool, timezone_obj, start_row,
                                              end_row);
    return converter.convert(result);
}

Status convert_from_arrow_batch(const std::shared_ptr<arrow::RecordBatch>& batch,
                                const vectorized::DataTypes& types, vectorized::Block* block,
                                const cctz::time_zone& timezone_obj) {
    FromRecordBatchToBlockConverter converter(batch, types, timezone_obj);
    return converter.convert(block);
}

#include "common/compile_check_end.h"
} // namespace doris
