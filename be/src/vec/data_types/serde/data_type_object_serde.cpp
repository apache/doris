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

#include "data_type_object_serde.h"

#include <rapidjson/stringbuffer.h>

#include "common/exception.h"
#include "common/status.h"
#include "vec/columns/column.h"
#include "vec/columns/column_object.h"
#include "vec/common/assert_cast.h"
#include "vec/common/schema_util.h"
#include "vec/core/field.h"

#ifdef __AVX2__
#include "util/jsonb_parser_simd.h"
#else
#include "util/jsonb_parser.h"
#endif

namespace doris {

namespace vectorized {

template <bool is_binary_format>
Status DataTypeObjectSerDe::_write_column_to_mysql(const IColumn& column,
                                                   MysqlRowBuffer<is_binary_format>& row_buffer,
                                                   int row_idx, bool col_const) const {
    const auto& variant = assert_cast<const ColumnObject&>(column);
    if (!variant.is_finalized()) {
        const_cast<ColumnObject&>(variant).finalize();
    }
    RETURN_IF_ERROR(variant.sanitize());
    if (variant.is_scalar_variant()) {
        // Serialize scalar types, like int, string, array, faster path
        const auto& root = variant.get_subcolumn({});
        RETURN_IF_ERROR(root->get_least_common_type_serde()->write_column_to_mysql(
                root->get_finalized_column(), row_buffer, row_idx, col_const));
    } else {
        // Serialize hierarchy types to json format
        rapidjson::StringBuffer buffer;
        bool is_null = false;
        if (!variant.serialize_one_row_to_json_format(row_idx, &buffer, &is_null)) {
            return Status::InternalError("Invalid json format");
        }
        if (is_null) {
            row_buffer.push_null();
        } else {
            row_buffer.push_string(buffer.GetString(), buffer.GetLength());
        }
    }
    return Status::OK();
}

Status DataTypeObjectSerDe::write_column_to_mysql(const IColumn& column,
                                                  MysqlRowBuffer<true>& row_buffer, int row_idx,
                                                  bool col_const) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const);
}

Status DataTypeObjectSerDe::write_column_to_mysql(const IColumn& column,
                                                  MysqlRowBuffer<false>& row_buffer, int row_idx,
                                                  bool col_const) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const);
}

void DataTypeObjectSerDe::write_one_cell_to_jsonb(const IColumn& column, JsonbWriter& result,
                                                  Arena* mem_pool, int32_t col_id,
                                                  int row_num) const {
    const auto& variant = assert_cast<const ColumnObject&>(column);
    if (!variant.is_finalized()) {
        const_cast<ColumnObject&>(variant).finalize();
    }
    result.writeKey(col_id);
    JsonbParser json_parser;
    CHECK(variant.get_rowstore_column() != nullptr);
    // use original document
    const auto& data_ref = variant.get_rowstore_column()->get_data_at(row_num);
    // encode as jsonb
    bool succ = json_parser.parse(data_ref.data, data_ref.size);
    // maybe more graceful, it is ok to check here since data could be parsed
    CHECK(succ);
    result.writeStartBinary();
    result.writeBinary(json_parser.getWriter().getOutput()->getBuffer(),
                       json_parser.getWriter().getOutput()->getSize());
    result.writeEndBinary();
}

void DataTypeObjectSerDe::read_one_cell_from_jsonb(IColumn& column, const JsonbValue* arg) const {
    auto& variant = assert_cast<ColumnObject&>(column);
    Field field;
    auto blob = static_cast<const JsonbBlobVal*>(arg);
    field.assign_jsonb(blob->getBlob(), blob->getBlobLen());
    variant.insert(field);
}

void DataTypeObjectSerDe::write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                                                arrow::ArrayBuilder* array_builder, int start,
                                                int end, const cctz::time_zone& ctz) const {
    const auto* var = check_and_get_column<ColumnObject>(column);
    auto& builder = assert_cast<arrow::StringBuilder&>(*array_builder);
    for (size_t i = start; i < end; ++i) {
        if (null_map && (*null_map)[i]) {
            checkArrowStatus(builder.AppendNull(), column.get_name(),
                             array_builder->type()->name());
        } else {
            std::string serialized_value;
            if (!var->serialize_one_row_to_string(i, &serialized_value)) {
                throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Failed to serialize variant {}",
                                       var->dump_structure());
            }
            checkArrowStatus(builder.Append(serialized_value.data(),
                                            static_cast<int>(serialized_value.size())),
                             column.get_name(), array_builder->type()->name());
        }
    }
}

} // namespace vectorized

} // namespace doris