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

#include "data_type_jsonb_serde.h"

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "arrow/array/builder_binary.h"
#include "common/exception.h"
#include "common/status.h"
#include "exprs/json_functions.h"
#include "runtime/jsonb_value.h"

#ifdef __AVX2__
#include "util/jsonb_parser_simd.h"
#else
#include "util/jsonb_parser.h"
#endif
namespace doris {
namespace vectorized {

template <bool is_binary_format>
Status DataTypeJsonbSerDe::_write_column_to_mysql(const IColumn& column,
                                                  MysqlRowBuffer<is_binary_format>& result,
                                                  int row_idx, bool col_const,
                                                  const FormatOptions& options) const {
    auto& data = assert_cast<const ColumnString&>(column);
    const auto col_index = index_check_const(row_idx, col_const);
    const auto jsonb_val = data.get_data_at(col_index);
    // jsonb size == 0 is NULL
    if (jsonb_val.data == nullptr || jsonb_val.size == 0) {
        if (UNLIKELY(0 != result.push_null())) {
            return Status::InternalError("pack mysql buffer failed.");
        }
    } else {
        std::string json_str = JsonbToJson::jsonb_to_json_string(jsonb_val.data, jsonb_val.size);
        if (UNLIKELY(0 != result.push_string(json_str.c_str(), json_str.size()))) {
            return Status::InternalError("pack mysql buffer failed.");
        }
    }
    return Status::OK();
}

Status DataTypeJsonbSerDe::write_column_to_mysql(const IColumn& column,
                                                 MysqlRowBuffer<true>& row_buffer, int row_idx,
                                                 bool col_const,
                                                 const FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}

Status DataTypeJsonbSerDe::write_column_to_mysql(const IColumn& column,
                                                 MysqlRowBuffer<false>& row_buffer, int row_idx,
                                                 bool col_const,
                                                 const FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}

Status DataTypeJsonbSerDe::serialize_column_to_json(const IColumn& column, int start_idx,
                                                    int end_idx, BufferWritable& bw,
                                                    FormatOptions& options) const {
    SERIALIZE_COLUMN_TO_JSON();
}

Status DataTypeJsonbSerDe::serialize_one_cell_to_json(const IColumn& column, int row_num,
                                                      BufferWritable& bw,
                                                      FormatOptions& options) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;

    const StringRef& s = assert_cast<const ColumnString&>(*ptr).get_data_at(row_num);
    if (s.size > 0) {
        std::string str = JsonbToJson::jsonb_to_json_string(s.data, s.size);
        bw.write(str.c_str(), str.size());
    } else {
        bw.write(NULL_IN_CSV_FOR_ORDINARY_TYPE.c_str(),
                 strlen(NULL_IN_CSV_FOR_ORDINARY_TYPE.c_str()));
    }
    return Status::OK();
}

Status DataTypeJsonbSerDe::deserialize_column_from_json_vector(IColumn& column,
                                                               std::vector<Slice>& slices,
                                                               int* num_deserialized,
                                                               const FormatOptions& options) const {
    DESERIALIZE_COLUMN_FROM_JSON_VECTOR();
    return Status::OK();
}

Status DataTypeJsonbSerDe::deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                                          const FormatOptions& options) const {
    JsonBinaryValue value;
    RETURN_IF_ERROR(value.from_json_string(slice.data, slice.size));

    auto& column_string = assert_cast<ColumnString&>(column);
    column_string.insert_data(value.value(), value.size());
    return Status::OK();
}

void DataTypeJsonbSerDe::write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                                               arrow::ArrayBuilder* array_builder, int start,
                                               int end, const cctz::time_zone& ctz) const {
    const auto& string_column = assert_cast<const ColumnString&>(column);
    auto& builder = assert_cast<arrow::StringBuilder&>(*array_builder);
    for (size_t string_i = start; string_i < end; ++string_i) {
        if (null_map && (*null_map)[string_i]) {
            checkArrowStatus(builder.AppendNull(), column.get_name(),
                             array_builder->type()->name());
            continue;
        }
        std::string_view string_ref = string_column.get_data_at(string_i).to_string_view();
        std::string json_string =
                JsonbToJson::jsonb_to_json_string(string_ref.data(), string_ref.size());
        checkArrowStatus(builder.Append(json_string.data(), json_string.size()), column.get_name(),
                         array_builder->type()->name());
    }
}

Status DataTypeJsonbSerDe::write_column_to_orc(const std::string& timezone, const IColumn& column,
                                               const NullMap* null_map,
                                               orc::ColumnVectorBatch* orc_col_batch, int start,
                                               int end, std::vector<StringRef>& buffer_list) const {
    return Status::NotSupported("write_column_to_orc with type [{}]", column.get_name());
}

void convert_jsonb_to_rapidjson(const JsonbValue& val, rapidjson::Value& target,
                                rapidjson::Document::AllocatorType& allocator) {
    // convert type of jsonb to rapidjson::Value
    switch (val.type()) {
    case JsonbType::T_True:
        target.SetBool(true);
        break;
    case JsonbType::T_False:
        target.SetBool(false);
        break;
    case JsonbType::T_Null:
        target.SetNull();
        break;
    case JsonbType::T_Float:
        target.SetFloat(static_cast<const JsonbFloatVal&>(val).val());
        break;
    case JsonbType::T_Double:
        target.SetDouble(static_cast<const JsonbDoubleVal&>(val).val());
        break;
    case JsonbType::T_Int64:
        target.SetInt64(static_cast<const JsonbInt64Val&>(val).val());
        break;
    case JsonbType::T_Int32:
        target.SetInt(static_cast<const JsonbInt32Val&>(val).val());
        break;
    case JsonbType::T_Int16:
        target.SetInt(static_cast<const JsonbInt16Val&>(val).val());
        break;
    case JsonbType::T_Int8:
        target.SetInt(static_cast<const JsonbInt8Val&>(val).val());
        break;
    case JsonbType::T_String:
        target.SetString(static_cast<const JsonbStringVal&>(val).getBlob(),
                         static_cast<const JsonbStringVal&>(val).getBlobLen());
        break;
    case JsonbType::T_Array: {
        target.SetArray();
        const ArrayVal& array = static_cast<const ArrayVal&>(val);
        if (array.numElem() == 0) {
            target.SetNull();
            break;
        }
        target.Reserve(array.numElem(), allocator);
        for (auto it = array.begin(); it != array.end(); ++it) {
            rapidjson::Value val;
            convert_jsonb_to_rapidjson(*static_cast<const JsonbValue*>(it), val, allocator);
            target.PushBack(val, allocator);
        }
        break;
    }
    case JsonbType::T_Object: {
        target.SetObject();
        const ObjectVal& obj = static_cast<const ObjectVal&>(val);
        for (auto it = obj.begin(); it != obj.end(); ++it) {
            rapidjson::Value val;
            convert_jsonb_to_rapidjson(*it->value(), val, allocator);
            target.AddMember(rapidjson::GenericStringRef(it->getKeyStr(), it->klen()), val,
                             allocator);
        }
        break;
    }
    default:
        CHECK(false) << "unkown type " << static_cast<int>(val.type());
        break;
    }
}

Status DataTypeJsonbSerDe::write_one_cell_to_json(const IColumn& column, rapidjson::Value& result,
                                                  rapidjson::Document::AllocatorType& allocator,
                                                  Arena& mem_pool, int row_num) const {
    const auto& data = assert_cast<const ColumnString&>(column);
    const auto jsonb_val = data.get_data_at(row_num);
    if (jsonb_val.empty()) {
        return Status::OK();
    }
    JsonbValue* val = JsonbDocument::createValue(jsonb_val.data, jsonb_val.size);
    if (val == nullptr) {
        return Status::InternalError("Failed to get json document from jsonb");
    }
    rapidjson::Value value;
    convert_jsonb_to_rapidjson(*val, value, allocator);
    if (val->isObject() && result.IsObject()) {
        JsonFunctions::merge_objects(result, value, allocator);
    } else {
        result = std::move(value);
    }
    return Status::OK();
}

Status DataTypeJsonbSerDe::read_one_cell_from_json(IColumn& column,
                                                   const rapidjson::Value& result) const {
    // TODO improve performance
    auto& col = assert_cast<ColumnString&>(column);
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    result.Accept(writer);
    JsonbParser parser;
    bool ok = parser.parse(buffer.GetString(), buffer.GetLength());
    CHECK(ok);
    col.insert_data(parser.getWriter().getOutput()->getBuffer(),
                    parser.getWriter().getOutput()->getSize());
    return Status::OK();
}
Status DataTypeJsonbSerDe::write_column_to_pb(const IColumn& column, PValues& result, int start,
                                              int end) const {
    const auto& string_column = assert_cast<const ColumnString&>(column);
    result.mutable_string_value()->Reserve(end - start);
    auto* ptype = result.mutable_type();
    ptype->set_id(PGenericType::JSONB);
    for (size_t row_num = start; row_num < end; ++row_num) {
        const auto& string_ref = string_column.get_data_at(row_num);
        if (string_ref.size > 0) {
            result.add_string_value(
                    JsonbToJson::jsonb_to_json_string(string_ref.data, string_ref.size));
        } else {
            result.add_string_value(NULL_IN_CSV_FOR_ORDINARY_TYPE);
        }
    }
    return Status::OK();
}

Status DataTypeJsonbSerDe::read_column_from_pb(IColumn& column, const PValues& arg) const {
    auto& column_string = assert_cast<ColumnString&>(column);
    column_string.reserve(column_string.size() + arg.string_value_size());
    JsonBinaryValue value;
    for (int i = 0; i < arg.string_value_size(); ++i) {
        RETURN_IF_ERROR(
                value.from_json_string(arg.string_value(i).c_str(), arg.string_value(i).size()));
        column_string.insert_data(value.value(), value.size());
    }
    return Status::OK();
}
} // namespace vectorized
} // namespace doris
