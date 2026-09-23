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

#include "format/parquet/parquet_arrow_block_convertor.h"

#include <arrow/type.h>

#include "format/arrow/arrow_row_batch.h"

namespace doris {

Status ParquetArrowBlockConvertor::init() {
    if (_types.size() != _names.size()) {
        return Status::InvalidArgument("Parquet column names and types must have the same size");
    }
    std::vector<std::shared_ptr<arrow::Field>> fields;
    fields.reserve(_types.size());
    // Retain the declared Arrow timezone label; cctz's fixed-offset name is internal.
    // Preserve the existing timezone-bearing DATETIMEV2 schema for both Parquet encodings.
    for (size_t i = 0; i < _types.size(); ++i) {
        std::shared_ptr<arrow::DataType> type;
        RETURN_IF_ERROR(convert_to_arrow_type(_types[i], &type, _timezone_name));
        fields.emplace_back(arrow::field(_names[i], type, _types[i]->is_nullable()));
    }
    _arrow_schema = arrow::schema(std::move(fields));
    return Status::OK();
}

Status ParquetArrowBlockConvertor::write_column(const DataTypePtr& type, const DataTypeSerDe& serde,
                                                const IColumn& column, const NullMap* null_map,
                                                const std::shared_ptr<arrow::Field>& field,
                                                arrow::ArrayBuilder* builder, int64_t start,
                                                int64_t end,
                                                const cctz::time_zone& timezone) const {
    return write_plain_arrow_column(type, serde, column, null_map, field, builder, start, end,
                                    timezone);
}

} // namespace doris
