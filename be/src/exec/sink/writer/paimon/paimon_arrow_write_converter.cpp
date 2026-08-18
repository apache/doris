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

#include "exec/sink/writer/paimon/paimon_arrow_write_converter.h"

#include <arrow/type.h>

#include "core/assert_cast.h"
#include "core/data_type/data_type_agg_state.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_struct.h"
#include "format/arrow/arrow_row_batch.h"
#include "format/arrow/arrow_utils.h"

namespace doris {
#include "common/compile_check_begin.h"
namespace {

Status convert_to_paimon_arrow_type(const DataTypePtr& origin_type,
                                    std::shared_ptr<arrow::DataType>* result,
                                    const std::string& timezone) {
    const DataTypePtr type = get_serialized_type(origin_type);
    switch (type->get_primitive_type()) {
    case TYPE_VARIANT:
        *result = arrow::struct_({arrow::field("value", arrow::binary(), false),
                                  arrow::field("metadata", arrow::binary(), false)});
        return Status::OK();
    case TYPE_ARRAY: {
        const auto& array_type = assert_cast<const DataTypeArray&>(*remove_nullable(type));
        std::shared_ptr<arrow::DataType> element_type;
        RETURN_IF_ERROR(convert_to_paimon_arrow_type(array_type.get_nested_type(), &element_type,
                                                     timezone));
        *result = std::make_shared<arrow::ListType>(element_type);
        return Status::OK();
    }
    case TYPE_MAP: {
        const auto& map_type = assert_cast<const DataTypeMap&>(*remove_nullable(type));
        std::shared_ptr<arrow::DataType> key_type;
        std::shared_ptr<arrow::DataType> value_type;
        RETURN_IF_ERROR(convert_to_paimon_arrow_type(map_type.get_key_type(), &key_type, timezone));
        RETURN_IF_ERROR(
                convert_to_paimon_arrow_type(map_type.get_value_type(), &value_type, timezone));
        *result = std::make_shared<arrow::MapType>(key_type, value_type);
        return Status::OK();
    }
    case TYPE_STRUCT: {
        const auto& struct_type = assert_cast<const DataTypeStruct&>(*remove_nullable(type));
        std::vector<std::shared_ptr<arrow::Field>> fields;
        fields.reserve(struct_type.get_elements().size());
        for (size_t index = 0; index < struct_type.get_elements().size(); ++index) {
            const DataTypePtr& element = struct_type.get_element(index);
            std::shared_ptr<arrow::DataType> field_type;
            RETURN_IF_ERROR(convert_to_paimon_arrow_type(element, &field_type, timezone));
            fields.push_back(arrow::field(struct_type.get_element_name(index), field_type,
                                          element->is_nullable()));
        }
        *result = arrow::struct_(std::move(fields));
        return Status::OK();
    }
    default:
        return convert_to_arrow_type(origin_type, result, timezone);
    }
}

} // namespace

Status PaimonArrowWriteConverter::write_column(const std::shared_ptr<const IDataType>& type,
                                               const DataTypeSerDe& serde, const IColumn& column,
                                               const NullMap* null_map,
                                               const std::shared_ptr<arrow::Field>& field,
                                               arrow::ArrayBuilder* array_builder, int64_t start,
                                               int64_t end, const cctz::time_zone& ctz) const {
    return serde.write_column_to_paimon(type, column, null_map,
                                        field->WithType(array_builder->type()), array_builder,
                                        start, end, ctz);
}

const PaimonArrowWriteConverter& paimon_arrow_write_converter() {
    static const PaimonArrowWriteConverter converter;
    return converter;
}

Status get_paimon_arrow_schema_from_block(const Block& block,
                                          std::shared_ptr<arrow::Schema>* result) {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    fields.reserve(block.columns());
    for (const auto& type_and_name : block) {
        std::shared_ptr<arrow::DataType> arrow_type;
        RETURN_IF_ERROR(convert_to_paimon_arrow_type(type_and_name.type, &arrow_type, ""));
        fields.push_back(create_arrow_field_with_metadata(
                type_and_name.name, arrow_type, type_and_name.type->is_nullable(),
                type_and_name.type->get_primitive_type()));
    }
    *result = arrow::schema(std::move(fields));
    return Status::OK();
}

#include "common/compile_check_end.h"
} // namespace doris
