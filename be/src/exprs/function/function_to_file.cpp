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

#include "exprs/function/function_to_file.h"

#include <mutex>
#include <string_view>

#include "core/column/column_const.h"
#include "core/column/column_file.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_file.h"
#include "core/value/file_value.h"
#include "exprs/function/simple_function_factory.h"
#include "io/file_factory.h"
#include "runtime/query_context.h"
#include "runtime/runtime_state.h"
#include "util/s3_uri.h"

namespace doris {
namespace {

struct ToFileState {
    explicit ToFileState(std::shared_ptr<const TS3Resource> resource_)
            : resource(std::move(resource_)) {}

    const std::shared_ptr<const TS3Resource> resource;
    std::shared_ptr<io::FileSystem> filesystem;
    std::string bucket;
    std::mutex mutex;
    io::FileStatContext stat_context;
};

std::string to_file_content_type(std::string_view key, const io::FileStat& metadata) {
    if (metadata.content_type && is_valid_file_content_type(*metadata.content_type)) {
        return *metadata.content_type;
    }
    return std::string(infer_file_content_type_from_name(key));
}

} // namespace

DataTypePtr FunctionToFile::get_return_type_impl(const ColumnsWithTypeAndName& arguments) const {
    const auto& resource = arguments[0];
    const auto& uri = arguments[1];
    if (!is_string_type(remove_nullable(resource.type)->get_primitive_type()) || !resource.column ||
        !is_column_const(*resource.column) || resource.column->is_null_at(0)) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "TO_FILE resource must be a non-NULL constant string");
    }
    if (!is_string_type(remove_nullable(uri.type)->get_primitive_type()) &&
        !uri.type->is_null_literal()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "TO_FILE URI must be string-like or NULL");
    }
    DataTypePtr result = std::make_shared<DataTypeFile>();
    return uri.type->is_nullable() || uri.type->is_null_literal() ? make_nullable(result) : result;
}

Status FunctionToFile::create_filesystem(const TS3Resource& resource, const std::string& uri,
                                         std::shared_ptr<io::FileSystem>* filesystem) const {
    io::FSPropertiesRef properties(TFileType::FILE_S3);
    properties.properties = &resource.properties;
    io::FileDescription description;
    S3URI s3_uri(uri);
    RETURN_IF_ERROR(s3_uri.parse(true));
    description.path = "s3://" + s3_uri.get_bucket();
    *filesystem = DORIS_TRY(FileFactory::create_fs(properties, description));
    return Status::OK();
}

Status FunctionToFile::open(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::FunctionStateScope::THREAD_LOCAL ||
        context->get_function_state(scope)) {
        return Status::OK();
    }
    auto* runtime = context->state();
    if (!runtime || !runtime->get_query_ctx()) {
        return Status::InvalidArgument("TO_FILE requires query resources");
    }
    const auto& resources = runtime->get_query_ctx()->get_s3_resources();
    if (!resources) {
        return Status::InvalidArgument("TO_FILE requires query resources");
    }
    const auto* resource_column = context->get_constant_col(0);
    if (!resource_column || resource_column->column_ptr->is_null_at(0)) {
        return Status::InvalidArgument("TO_FILE resource must be a non-NULL constant string");
    }
    const auto resource_name = resource_column->column_ptr->get_data_at(0).to_string();
    const auto resource = resources->find(resource_name);
    if (resource == resources->end()) {
        return Status::InvalidArgument("TO_FILE resource '{}' is missing from query resources",
                                       resource_name);
    }
    // Defer client creation until a non-NULL URI needs it.
    // Share the immutable query map's ownership without copying this resource per function.
    auto state = std::make_shared<ToFileState>(
            std::shared_ptr<const TS3Resource>(resources, &resource->second));
    // RuntimeState outlives the fragment contexts, including their clones.
    state->stat_context.is_cancelled = [runtime] { return runtime->is_cancelled(); };
    context->set_function_state(scope, std::move(state));
    return Status::OK();
}

Status FunctionToFile::close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::FunctionStateScope::FRAGMENT_LOCAL) {
        context->set_function_state(scope, nullptr);
    }
    return Status::OK();
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity): Keep per-row cancellation and client lifetime checks together.
Status FunctionToFile::execute_impl(FunctionContext* context, Block& block,
                                    const ColumnNumbers& arguments, uint32_t result,
                                    size_t input_rows_count) const {
    auto* state = static_cast<ToFileState*>(
            context->get_function_state(FunctionContext::FunctionStateScope::FRAGMENT_LOCAL));
    if (!state) {
        return Status::InternalError("TO_FILE has no fragment-local state");
    }
    auto* runtime = context->state();
    const auto& source = block.get_by_position(arguments[1]).column;
    auto output = ColumnFile::create();
    auto nulls = ColumnUInt8::create(input_rows_count, 0);
    for (size_t row = 0; row < input_rows_count; ++row) {
        RETURN_IF_CANCELLED(runtime);
        if (source->is_null_at(row)) {
            output->insert_default();
            nulls->get_data()[row] = 1;
            continue;
        }
        const auto uri = source->get_data_at(row).to_string();
        io::FileStat metadata;
        std::string bucket;
        S3URI s3_uri(uri);
        RETURN_IF_ERROR(s3_uri.parse(true));
        bucket = s3_uri.get_bucket();
        std::shared_ptr<io::FileSystem> filesystem;
        {
            // FunctionContext clones share client initialization and bucket switching.
            std::scoped_lock lock(state->mutex);
            RETURN_IF_CANCELLED(runtime);
            // S3FileSystem binds its bucket at construction. Keep only the current
            // filesystem and switch when a row references a different bucket.
            if (!state->filesystem || state->bucket != bucket) {
                std::shared_ptr<io::FileSystem> new_filesystem;
                RETURN_IF_ERROR(create_filesystem(*state->resource, uri, &new_filesystem));
                DORIS_CHECK(new_filesystem != nullptr);
                state->filesystem = std::move(new_filesystem);
                state->bucket = std::move(bucket);
                RETURN_IF_CANCELLED(runtime);
            }
            filesystem = state->filesystem;
        }
        // Pin this request's filesystem across concurrent bucket switches. Remote
        // metadata calls must not serialize all scanners sharing the fragment state.
        const auto stat_status = filesystem->stat(uri, &metadata, &state->stat_context);
        RETURN_IF_CANCELLED(runtime);
        RETURN_IF_ERROR(stat_status);
        auto content_type = to_file_content_type(s3_uri.get_key(), metadata);
        File value(DataTypeFile::FIELD_COUNT);
        value[0] = Field::create_field<TYPE_STRING>(uri);
        value[2] = Field::create_field<TYPE_BIGINT>(metadata.size);
        value[3] = Field::create_field<TYPE_STRING>(std::move(content_type));
        if (metadata.checksum) {
            value[4] = Field::create_field<TYPE_STRING>(*metadata.checksum);
        }
        RETURN_IF_ERROR(validate_file(value));
        output->insert(Field::create_field<TYPE_FILE>(std::move(value)));
    }
    if (block.get_by_position(result).type->is_nullable()) {
        block.get_by_position(result).column =
                ColumnNullable::create(std::move(output), std::move(nulls));
    } else {
        block.get_by_position(result).column = std::move(output);
    }
    return Status::OK();
}

void register_function_to_file(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionToFile>();
}

} // namespace doris
