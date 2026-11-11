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

#include <arrow/extension/parquet_variant.h>
#include <arrow/type.h>

#include "core/data_type/data_type.h"
#include "core/data_type_serde/data_type_serde.h"

namespace doris {

std::string ParquetArrowSchemaConvertor::timestamp_timezone(PrimitiveType type) const {
    return type == TYPE_DATETIMEV2 && !_enable_int96_timestamps
                   ? ""
                   : DorisArrowSchemaConvertor::timestamp_timezone(type);
}

Status ParquetArrowSchemaConvertor::convert_to_arrow_type(
        const DataTypePtr& type, std::shared_ptr<arrow::DataType>* result) const {
    if (type->get_primitive_type() == TYPE_VARIANT) {
        static const auto parquet_variant = arrow::extension::variant(
                arrow::struct_({arrow::field("metadata", arrow::binary(), false),
                                arrow::field("value", arrow::binary(), false)}));
        *result = parquet_variant;
        return Status::OK();
    }
    return DorisArrowSchemaConvertor::convert_to_arrow_type(type, result);
}

Status ParquetArrowSchemaConvertor::get_arrow_schema(std::shared_ptr<arrow::Schema>* result) const {
    if (_types.size() != _names.size()) {
        return Status::InvalidArgument("Parquet column names and types must have the same size");
    }
    std::vector<std::shared_ptr<arrow::Field>> fields;
    fields.reserve(_types.size());
    // Retain the declared Arrow timezone label; cctz's fixed-offset name is internal.
    // INT96 normalization and schema construction must use the same instance's timezone.
    for (size_t i = 0; i < _types.size(); ++i) {
        std::shared_ptr<arrow::DataType> type;
        RETURN_IF_ERROR(convert_to_arrow_type(_types[i], &type));
        fields.emplace_back(arrow::field(_names[i], type, _types[i]->is_nullable()));
    }
    *result = arrow::schema(std::move(fields));
    return Status::OK();
}

Status ParquetArrowBlockConvertor::init() {
    return _schema_convertor.get_arrow_schema(&_arrow_schema);
}

Status ParquetArrowBlockConvertor::write_column(const DataTypePtr& type, const DataTypeSerDe& serde,
                                                const IColumn& column, const NullMap* null_map,
                                                const std::shared_ptr<arrow::Field>& field,
                                                arrow::ArrayBuilder* builder, int64_t start,
                                                int64_t end,
                                                const cctz::time_zone& timezone) const {
    // Check against the same schema rules used at open, including Variant leaves in containers.
    // The builder uses extension storage, so its struct type alone cannot establish this binding.
    std::shared_ptr<arrow::DataType> expected_type;
    RETURN_IF_ERROR(_schema_convertor.convert_to_arrow_type(type, &expected_type));
    if (!field->type()->Equals(expected_type)) {
        return Status::InvalidArgument(
                "Parquet writer is not bound for Doris type {} and Arrow field {}",
                type->get_name(), field->ToString());
    }
    return serde.write_column_to_arrow(column, null_map, builder, start, end, timezone);
}

} // namespace doris
