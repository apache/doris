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
#include "data_type_serde.h"

#include "common/cast_set.h"
#include "common/exception.h"
#include "common/status.h"
#include "runtime/descriptors.h"
#include "util/jsonb_document.h"
#include "util/jsonb_writer.h"
#include "vec/columns/column.h"
#include "vec/core/field.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/serde/data_type_jsonb_serde.h"
#include "vec/functions/cast/cast_base.h"
namespace doris {
namespace vectorized {
#include "common/compile_check_begin.h"
DataTypeSerDe::~DataTypeSerDe() = default;

DataTypeSerDeSPtrs create_data_type_serdes(const DataTypes& types) {
    DataTypeSerDeSPtrs serdes;
    serdes.reserve(types.size());
    for (const DataTypePtr& type : types) {
        serdes.push_back(type->get_serde());
    }
    return serdes;
}

DataTypeSerDeSPtrs create_data_type_serdes(const std::vector<SlotDescriptor*>& slots) {
    DataTypeSerDeSPtrs serdes;
    serdes.reserve(slots.size());
    for (const SlotDescriptor* slot : slots) {
        serdes.push_back(slot->get_data_type_ptr()->get_serde());
    }
    return serdes;
}

void DataTypeSerDe::convert_variant_map_to_rapidjson(
        const vectorized::VariantMap& map, rapidjson::Value& target,
        rapidjson::Document::AllocatorType& allocator) {
    target.SetObject();
    for (const auto& item : map) {
        if (item.second.is_null()) {
            continue;
        }
        rapidjson::Value key;
        key.SetString(item.first.get_path().data(),
                      cast_set<rapidjson::SizeType>(item.first.get_path().size()));
        rapidjson::Value val;
        convert_field_to_rapidjson(item.second, val, allocator);
        if (val.IsNull() && item.first.empty()) {
            // skip null value with empty key, indicate the null json value of root in variant map,
            // usally padding in nested arrays
            continue;
        }
        target.AddMember(key, val, allocator);
    }
}

void DataTypeSerDe::convert_array_to_rapidjson(const vectorized::Array& array,
                                               rapidjson::Value& target,
                                               rapidjson::Document::AllocatorType& allocator) {
    target.SetArray();
    for (const vectorized::Field& item : array) {
        rapidjson::Value val;
        convert_field_to_rapidjson(item, val, allocator);
        target.PushBack(val, allocator);
    }
}

void DataTypeSerDe::convert_field_to_rapidjson(const vectorized::Field& field,
                                               rapidjson::Value& target,
                                               rapidjson::Document::AllocatorType& allocator) {
    switch (field.get_type()) {
    case PrimitiveType::TYPE_NULL:
        target.SetNull();
        break;
    case PrimitiveType::TYPE_BIGINT:
        target.SetInt64(field.get<Int64>());
        break;
    case PrimitiveType::TYPE_DOUBLE:
        target.SetDouble(field.get<Float64>());
        break;
    case PrimitiveType::TYPE_JSONB: {
        const auto& val = field.get<JsonbField>();
        JsonbValue* json_val = JsonbDocument::createValue(val.get_value(), val.get_size());
        convert_jsonb_to_rapidjson(*json_val, target, allocator);
        break;
    }
    case PrimitiveType::TYPE_CHAR:
    case PrimitiveType::TYPE_VARCHAR:
    case PrimitiveType::TYPE_STRING: {
        const String& val = field.get<String>();
        target.SetString(val.data(), cast_set<rapidjson::SizeType>(val.size()));
        break;
    }
    case PrimitiveType::TYPE_ARRAY: {
        const vectorized::Array& array = field.get<Array>();
        convert_array_to_rapidjson(array, target, allocator);
        break;
    }
    case PrimitiveType::TYPE_VARIANT: {
        const vectorized::VariantMap& map = field.get<VariantMap>();
        convert_variant_map_to_rapidjson(map, target, allocator);
        break;
    }
    default:
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "unkown field type: {}",
                               field.get_type_name());
        break;
    }
}

Status DataTypeSerDe::serialize_column_to_jsonb_vector(const IColumn& from_column,
                                                       ColumnString& to_column) const {
    const auto size = from_column.size();
    JsonbWriter writer;
    for (int i = 0; i < size; i++) {
        writer.reset();
        RETURN_IF_ERROR(serialize_column_to_jsonb(from_column, i, writer));
        to_column.insert_data(writer.getOutput()->getBuffer(), writer.getOutput()->getSize());
    }
    return Status::OK();
}

Status DataTypeSerDe::parse_column_from_jsonb_string(IColumn& column, const JsonbValue* jsonb_value,
                                                     CastParameters& castParms) const {
    DCHECK(jsonb_value->isString());
    const auto* blob = jsonb_value->unpack<JsonbBinaryVal>();

    Slice slice(blob->getBlob(), blob->getBlobLen());

    DataTypeSerDe::FormatOptions format_options;
    format_options.converted_from_string = true;
    format_options.escape_char = '\\';

    return deserialize_one_cell_from_json(column, slice, format_options);
}

Status DataTypeSerDe::write_one_cell_to_json(const IColumn& column, rapidjson::Value& result,
                                             rapidjson::Document::AllocatorType& allocator,
                                             Arena& mem_pool, int64_t row_num) const {
    return Status::InternalError("Not support write {} to rapidjson", column.get_name());
}

Status DataTypeSerDe::read_one_cell_from_json(IColumn& column,
                                              const rapidjson::Value& result) const {
    return Status::NotSupported("Not support read {} from rapidjson", column.get_name());
}

const std::string DataTypeSerDe::NULL_IN_COMPLEX_TYPE = "null";
const std::string DataTypeSerDe::NULL_IN_CSV_FOR_ORDINARY_TYPE = "\\N";

} // namespace vectorized
} // namespace doris
