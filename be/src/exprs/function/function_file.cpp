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

#include <cstring>
#include <memory>
#include <string>
#include <string_view>

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_numbers.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column_file.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type_file.h"
#include "core/data_type/file_schema_descriptor.h"
#include "exprs/function/function.h"
#include "exprs/function/simple_function_factory.h"
#include "io/fs/obj_storage_client.h"
#include "util/jsonb_writer.h"
#include "util/s3_uri.h"
#include "util/s3_util.h"

namespace doris {

class FunctionToFile : public IFunction {
public:
    static constexpr auto name = "to_file";

    static FunctionPtr create() { return std::make_shared<FunctionToFile>(); }

    String get_name() const override { return name; }

    bool is_variadic() const override { return true; }

    size_t get_number_of_arguments() const override { return 0; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeFile>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const size_t nargs = arguments.size();
        if (nargs != 5 && nargs != 7) {
            return Status::InvalidArgument(
                    "to_file requires 5 arguments (url, region, endpoint, ak, sk) or "
                    "7 arguments (url, region, endpoint, ak, sk, role_arn, external_id), "
                    "got {}",
                    nargs);
        }

        ColumnPtr url_holder, region_holder, endpoint_holder, ak_holder, sk_holder, role_arn_holder,
                external_id_holder;
        const ColumnString* uri_col =
                _unwrap_string_column(block.get_by_position(arguments[0]), url_holder);
        const ColumnString* region_col =
                _unwrap_string_column(block.get_by_position(arguments[1]), region_holder);
        const ColumnString* endpoint_col =
                _unwrap_string_column(block.get_by_position(arguments[2]), endpoint_holder);
        const ColumnString* ak_col =
                _unwrap_string_column(block.get_by_position(arguments[3]), ak_holder);
        const ColumnString* sk_col =
                _unwrap_string_column(block.get_by_position(arguments[4]), sk_holder);
        const ColumnString* role_arn_col = nullptr;
        const ColumnString* external_id_col = nullptr;
        if (nargs == 7) {
            role_arn_col =
                    _unwrap_string_column(block.get_by_position(arguments[5]), role_arn_holder);
            external_id_col =
                    _unwrap_string_column(block.get_by_position(arguments[6]), external_id_holder);
        }

        using S = FileSchemaDescriptor;
        const auto& schema = S::instance();
        auto result_col = ColumnFile::create(schema);
        auto& jsonb_col = assert_cast<ColumnString&>(result_col->get_jsonb_column());
        jsonb_col.reserve(input_rows_count);
        JsonbWriter writer;

        for (size_t row = 0; row < input_rows_count; ++row) {
            std::string uri = uri_col->get_data_at(row).to_string();
            std::string region = region_col->get_data_at(row).to_string();
            std::string endpoint = endpoint_col->get_data_at(row).to_string();
            std::string ak = ak_col->get_data_at(row).to_string();
            std::string sk = sk_col->get_data_at(row).to_string();
            std::string role_arn =
                    role_arn_col ? role_arn_col->get_data_at(row).to_string() : std::string {};
            std::string external_id = external_id_col
                                              ? external_id_col->get_data_at(row).to_string()
                                              : std::string {};
            std::string file_name = S::extract_file_name(uri);
            std::string content_type =
                    S::extension_to_content_type(S::extract_file_extension(file_name));

            // Ensure endpoint has http:// prefix for S3 SDK.
            std::string normalized_endpoint = _normalize_endpoint(endpoint);

            // Validate the object exists via HEAD request and get actual size.
            S3ClientConf s3_conf;
            s3_conf.endpoint = normalized_endpoint;
            s3_conf.region = region;
            s3_conf.ak = ak;
            s3_conf.sk = sk;
            s3_conf.role_arn = role_arn;
            s3_conf.external_id = external_id;
            auto s3_client = S3ClientFactory::instance().create(s3_conf);
            if (!s3_client) {
                return Status::InternalError(
                        "to_file: failed to create S3 client for endpoint '{}'", endpoint);
            }
            // Normalize oss:// etc. to s3:// for S3URI parser and storage.
            std::string normalized_uri = _normalize_uri_scheme(uri);
            S3URI s3_uri(normalized_uri);
            RETURN_IF_ERROR(s3_uri.parse());
            auto head_resp = s3_client->head_object(
                    {.bucket = s3_uri.get_bucket(), .key = s3_uri.get_key()});
            if (head_resp.resp.status.code != 0) {
                return Status::InvalidArgument("to_file: object '{}' is not accessible: {}", uri,
                                               head_resp.resp.status.msg);
            }
            int64_t file_size = head_resp.file_size;

            writer.reset();
            FileMetadata metadata {
                    .uri = normalized_uri,
                    .file_name = file_name,
                    .content_type = content_type,
                    .size = file_size,
                    .region = region,
                    .endpoint = normalized_endpoint,
                    .ak = ak,
                    .sk = sk,
                    .role_arn = role_arn,
                    .external_id = external_id,
            };
            S::write_file_jsonb(writer, metadata);
            jsonb_col.insert_data(writer.getOutput()->getBuffer(), writer.getOutput()->getSize());
        }
        block.replace_by_position(result, std::move(result_col));
        return Status::OK();
    }

private:
    static const ColumnString* _unwrap_string_column(const ColumnWithTypeAndName& col_with_type,
                                                     ColumnPtr& holder) {
        holder = col_with_type.column->convert_to_full_column_if_const();
        if (const auto* nullable = check_and_get_column<ColumnNullable>(holder.get())) {
            return &assert_cast<const ColumnString&>(nullable->get_nested_column());
        }
        return &assert_cast<const ColumnString&>(*holder);
    }

    // Ensure endpoint has http:// scheme prefix.
    static std::string _normalize_endpoint(const std::string& endpoint) {
        if (endpoint.substr(0, 7) == "http://" || endpoint.substr(0, 8) == "https://") {
            return endpoint;
        }
        return "http://" + endpoint;
    }

    // Normalize oss:// etc. to s3:// for S3URI parser and storage.
    static std::string _normalize_uri_scheme(const std::string& uri) {
        if (uri.substr(0, 6) == "oss://") {
            return "s3://" + uri.substr(6);
        }
        if (uri.substr(0, 6) == "cos://") {
            return "s3://" + uri.substr(6);
        }
        if (uri.substr(0, 6) == "obs://") {
            return "s3://" + uri.substr(6);
        }
        return uri;
    }
};

void register_function_file(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionToFile>();
}

} // namespace doris
