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

#include "udf/python/python_udf_meta.h"

#include <arrow/util/base64.h>
#include <fmt/core.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <sstream>

#include "common/status.h"
#include "util/arrow/utils.h"
#include "util/string_util.h"

namespace doris {

Status PythonUDFMeta::convert_types_to_schema(const vectorized::DataTypes& types,
                                              const std::string& timezone,
                                              std::shared_ptr<arrow::Schema>* schema) {
    assert(!types.empty());
    arrow::SchemaBuilder builder;
    for (size_t i = 0; i < types.size(); ++i) {
        std::shared_ptr<arrow::DataType> arrow_type;
        RETURN_IF_ERROR(convert_to_arrow_type(types[i], &arrow_type, timezone));
        std::shared_ptr<arrow::Field> field = std::make_shared<arrow::Field>(
                "arg" + std::to_string(i), arrow_type, types[i]->is_nullable());
        RETURN_DORIS_STATUS_IF_ERROR(builder.AddField(field));
    }
    RETURN_DORIS_STATUS_IF_RESULT_ERROR(schema, builder.Finish());
    return Status::OK();
}

Status PythonUDFMeta::serialize_arrow_schema(const std::shared_ptr<arrow::Schema>& schema,
                                             std::shared_ptr<arrow::Buffer>* out) {
    RETURN_DORIS_STATUS_IF_RESULT_ERROR(
            out, arrow::ipc::SerializeSchema(*schema, arrow::default_memory_pool()));
    return Status::OK();
}

/*
    json format:
    {
        "name": "xxx",
        "symbol": "xxx",
        "location": "xxx",
        "udf_load_type": 0 or 1,
        "client_type": 0 (UDF) or 1 (UDAF) or 2 (UDTF),
        "runtime_version": "x.xx.xx",
        "always_nullable": true,
        "inline_code": "base64_inline_code",
        "input_types": "base64_input_types",
        "return_type": "base64_return_type"
    }
*/
Status PythonUDFMeta::serialize_to_json(std::string* json_str) const {
    rapidjson::Document doc;
    doc.SetObject();
    auto& allocator = doc.GetAllocator();
    doc.AddMember("name", rapidjson::Value().SetString(name.c_str(), allocator), allocator);
    doc.AddMember("symbol", rapidjson::Value().SetString(symbol.c_str(), allocator), allocator);
    doc.AddMember("location", rapidjson::Value().SetString(location.c_str(), allocator), allocator);
    doc.AddMember("udf_load_type", rapidjson::Value().SetInt(static_cast<int>(type)), allocator);
    doc.AddMember("client_type", rapidjson::Value().SetInt(static_cast<int>(client_type)),
                  allocator);
    doc.AddMember("runtime_version",
                  rapidjson::Value().SetString(runtime_version.c_str(), allocator), allocator);
    doc.AddMember("always_nullable", rapidjson::Value().SetBool(always_nullable), allocator);

    {
        // Serialize base64 inline code to json
        std::string base64_str = arrow::util::base64_encode(inline_code);
        doc.AddMember("inline_code", rapidjson::Value().SetString(base64_str.c_str(), allocator),
                      allocator);
    }
    {
        // Serialize base64 input types to json
        std::shared_ptr<arrow::Schema> input_schema;
        RETURN_IF_ERROR(convert_types_to_schema(input_types, TimezoneUtils::default_time_zone,
                                                &input_schema));
        std::shared_ptr<arrow::Buffer> input_schema_buffer;
        RETURN_IF_ERROR(serialize_arrow_schema(input_schema, &input_schema_buffer));
        std::string base64_str =
                arrow::util::base64_encode({input_schema_buffer->data_as<char>(),
                                            static_cast<size_t>(input_schema_buffer->size())});
        doc.AddMember("input_types", rapidjson::Value().SetString(base64_str.c_str(), allocator),
                      allocator);
    }
    {
        // Serialize base64 return type to json
        std::shared_ptr<arrow::Schema> return_schema;
        RETURN_IF_ERROR(convert_types_to_schema({return_type}, TimezoneUtils::default_time_zone,
                                                &return_schema));
        std::shared_ptr<arrow::Buffer> return_schema_buffer;
        RETURN_IF_ERROR(serialize_arrow_schema(return_schema, &return_schema_buffer));
        std::string base64_str =
                arrow::util::base64_encode({return_schema_buffer->data_as<char>(),
                                            static_cast<size_t>(return_schema_buffer->size())});
        doc.AddMember("return_type", rapidjson::Value().SetString(base64_str.c_str(), allocator),
                      allocator);
    }

    // Convert document to json string
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    doc.Accept(writer);
    *json_str = std::string(buffer.GetString(), buffer.GetSize());
    return Status::OK();
}

std::string PythonUDFMeta::to_string() const {
    std::stringstream input_types_ss;
    input_types_ss << "<";
    for (size_t i = 0; i < input_types.size(); ++i) {
        input_types_ss << input_types[i]->get_name();
        if (i != input_types.size() - 1) {
            input_types_ss << ", ";
        }
    }
    input_types_ss << ">";
    return fmt::format(
            "[name: {}, symbol: {}, location: {}, runtime_version: {}, always_nullable: {}, "
            "inline_code: {}][input_types: {}][return_type: {}]",
            name, symbol, location, runtime_version, always_nullable, inline_code,
            input_types_ss.str(), return_type->get_name());
}

Status PythonUDFMeta::check() const {
    if (trim(name).empty()) {
        return Status::InvalidArgument("Python UDF name is empty");
    }

    if (trim(symbol).empty()) {
        return Status::InvalidArgument("Python UDF symbol is empty");
    }

    if (trim(runtime_version).empty()) {
        return Status::InvalidArgument("Python UDF runtime version is empty");
    }

    if (input_types.empty()) {
        return Status::InvalidArgument("Python UDF input types is empty");
    }

    if (!return_type) {
        return Status::InvalidArgument("Python UDF return type is empty");
    }

    if (type == PythonUDFLoadType::UNKNOWN) {
        return Status::InvalidArgument(
                "Python UDF load type is invalid, please check inline code or file path");
    }

    if (type == PythonUDFLoadType::MODULE) {
        if (trim(location).empty()) {
            return Status::InvalidArgument("Non-inline Python UDF location is empty");
        }
        if (trim(checksum).empty()) {
            return Status::InvalidArgument("Non-inline Python UDF checksum is empty");
        }
    }

    return Status::OK();
}

} // namespace doris