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

class FromBlockConverter {
public:
    FromBlockConverter(const vectorized::Block& block, const std::shared_ptr<arrow::Schema>& schema,
                       arrow::MemoryPool* pool, const cctz::time_zone& timezone_obj)
            : _block(block),
              _schema(schema),
              _pool(pool),
              _cur_field_idx(-1),
              _timezone_obj(timezone_obj) {}

    ~FromBlockConverter() = default;

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

    std::vector<std::shared_ptr<arrow::Array>> _arrays;
};

Status FromBlockConverter::convert(std::shared_ptr<arrow::RecordBatch>* out) {
    size_t num_fields = _schema->num_fields();
    if (_block.columns() != num_fields) {
        return Status::InvalidArgument("number fields not match");
    }

    _arrays.resize(num_fields);

    for (size_t idx = 0; idx < num_fields; ++idx) {
        _cur_field_idx = idx;
        _cur_start = 0;
        _cur_rows = _block.rows();
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
    *out = arrow::RecordBatch::Make(_schema, _block.rows(), std::move(_arrays));
    return Status::OK();
}

Status convert_to_arrow_batch(const vectorized::Block& block,
                              const std::shared_ptr<arrow::Schema>& schema, arrow::MemoryPool* pool,
                              std::shared_ptr<arrow::RecordBatch>* result,
                              const cctz::time_zone& timezone_obj) {
    FromBlockConverter converter(block, schema, pool, timezone_obj);
    return converter.convert(result);
}

} // namespace doris
