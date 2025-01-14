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

#include "vec/exec/format/table/iceberg/arrow_schema_util.h"

#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>

namespace doris {
namespace iceberg {

const char* ArrowSchemaUtil::PARQUET_FIELD_ID = "PARQUET:field_id";
const char* ArrowSchemaUtil::ORIGINAL_TYPE = "originalType";
const char* ArrowSchemaUtil::MAP_TYPE_VALUE = "mapType";

Status ArrowSchemaUtil::convert(const Schema* schema, const std::string& timezone,
                                std::vector<std::shared_ptr<arrow::Field>>& fields) {
    for (const auto& column : schema->columns()) {
        std::shared_ptr<arrow::Field> arrow_field;
        RETURN_IF_ERROR(convert_to(column, &arrow_field, timezone));
        fields.push_back(arrow_field);
    }
    return Status::OK();
}

Status ArrowSchemaUtil::convert_to(const iceberg::NestedField& field,
                                   std::shared_ptr<arrow::Field>* arrow_field,
                                   const std::string& timezone) {
    std::shared_ptr<arrow::DataType> arrow_type;
    std::unordered_map<std::string, std::string> metadata;
    metadata[PARQUET_FIELD_ID] = std::to_string(field.field_id());

    switch (field.field_type()->type_id()) {
    case iceberg::TypeID::BOOLEAN:
        arrow_type = arrow::boolean();
        break;

    case iceberg::TypeID::INTEGER:
        arrow_type = arrow::int32();
        break;

    case iceberg::TypeID::LONG:
        arrow_type = arrow::int64();
        break;

    case iceberg::TypeID::FLOAT:
        arrow_type = arrow::float32();
        break;

    case iceberg::TypeID::DOUBLE:
        arrow_type = arrow::float64();
        break;

    case iceberg::TypeID::DATE:
        arrow_type = arrow::date32();
        break;

    case iceberg::TypeID::TIMESTAMP: {
        arrow_type = std::make_shared<arrow::TimestampType>(arrow::TimeUnit::MICRO, timezone);
        break;
    }

    case iceberg::TypeID::BINARY:
    case iceberg::TypeID::STRING:
    case iceberg::TypeID::UUID:
    case iceberg::TypeID::FIXED:
        arrow_type = arrow::utf8();
        break;

    case iceberg::TypeID::DECIMAL: {
        auto dt = dynamic_cast<DecimalType*>(field.field_type());
        arrow_type = arrow::decimal(dt->get_precision(), dt->get_scale());
        break;
    }

    case iceberg::TypeID::STRUCT: {
        std::vector<std::shared_ptr<arrow::Field>> element_fields;
        StructType* st = field.field_type()->as_struct_type();
        for (const auto& column : st->fields()) {
            std::shared_ptr<arrow::Field> element_field;
            RETURN_IF_ERROR(convert_to(column, &element_field, timezone));
            element_fields.push_back(element_field);
        }
        arrow_type = arrow::struct_(element_fields);
        break;
    }

    case iceberg::TypeID::LIST: {
        std::shared_ptr<arrow::Field> item_field;
        ListType* list_type = field.field_type()->as_list_type();
        RETURN_IF_ERROR(convert_to(list_type->element_field(), &item_field, timezone));
        arrow_type = arrow::list(item_field);
        break;
    }

    case iceberg::TypeID::MAP: {
        std::shared_ptr<arrow::Field> key_field;
        std::shared_ptr<arrow::Field> value_field;
        MapType* map_type = field.field_type()->as_map_type();
        RETURN_IF_ERROR(convert_to(map_type->key_field(), &key_field, timezone));
        RETURN_IF_ERROR(convert_to(map_type->value_field(), &value_field, timezone));
        metadata[ORIGINAL_TYPE] = MAP_TYPE_VALUE;
        arrow_type = std::make_shared<arrow::MapType>(key_field, value_field);
        break;
    }

    case iceberg::TypeID::TIME:
    default:
        return Status::InternalError("Unsupported field type:" + field.field_type()->to_string());
    }

    std::shared_ptr<arrow::KeyValueMetadata> schema_metadata =
            std::make_shared<arrow::KeyValueMetadata>(metadata);
    *arrow_field =
            arrow::field(field.field_name(), arrow_type, field.is_optional(), schema_metadata);
    return Status::OK();
}

} // namespace iceberg
} // namespace doris