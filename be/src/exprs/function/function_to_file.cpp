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
#include "core/data_type/data_type_number.h"
#include "core/value/file_value.h"
#include "exprs/function/simple_function_factory.h"
#include "io/file_factory.h"
#include "runtime/runtime_state.h"
#include "util/s3_uri.h"

namespace doris {
namespace {

// Internal statistics helper: count logical payload bytes, without serialization overhead.
class FunctionFileDataSize final : public IFunction {
public:
    static constexpr auto name = "__file_data_size";
    static FunctionPtr create() { return std::make_shared<FunctionFileDataSize>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        if (remove_nullable(arguments[0].type)->get_primitive_type() != TYPE_FILE) {
            throw Exception(ErrorCode::INVALID_ARGUMENT, "__file_data_size requires FILE");
        }
        return std::make_shared<DataTypeInt64>();
    }

    Status execute_impl(FunctionContext*, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const auto& file =
                assert_cast<const ColumnFile&>(*block.get_by_position(arguments[0]).column);
        auto output = ColumnInt64::create(input_rows_count, 0);
        auto& sizes = output->get_data();
        for (size_t child = 0; child < ColumnFile::NUM_CHILDREN; ++child) {
            const auto& column = file.get_column(child);
            for (size_t row = 0; row < input_rows_count; ++row) {
                // Nullable children return an empty StringRef for NULL. Numeric fields
                // expose exactly eight bytes; strings and inline expose their payload.
                const auto bytes = column.get_data_at(row).size;
                // Validated VARCHAR lengths and VARBINARY's uint32 length fit in BIGINT.
                sizes[row] += static_cast<Int64>(bytes);
            }
        }
        block.replace_by_position(result, std::move(output));
        return Status::OK();
    }
};

struct ToFileState {
    explicit ToFileState(std::shared_ptr<const TFileResourceSnapshot> resource_)
            : resource(std::move(resource_)) {}

    const std::shared_ptr<const TFileResourceSnapshot> resource;
    std::shared_ptr<io::FileSystem> filesystem;
    std::string bucket;
    std::mutex mutex;
    io::FileStatContext stat_context;
};

// Only the path participates in extension inference. Keep the stored/stat URI unchanged.
std::string_view to_file_uri_path(std::string_view uri) {
    uri = uri.substr(0, uri.find_first_of("?#"));
    const auto colon = uri.find(':');
    if (colon == std::string_view::npos) return uri;
    auto path = uri.substr(colon + 1);
    if (path.starts_with("//")) {
        const auto slash = path.find('/', 2);
        return slash == std::string_view::npos ? std::string_view() : path.substr(slash);
    }
    return path;
}

Status to_file_content_type(std::string_view uri, const io::FileStat& metadata,
                            std::string& content_type) {
    if (metadata.content_type && is_valid_file_content_type(*metadata.content_type)) {
        content_type = *metadata.content_type;
        return Status::OK();
    }
    const auto path = to_file_uri_path(uri);
    const auto filename = path.substr(path.find_last_of('/') + 1);
    content_type = infer_file_content_type_from_name(filename);
    return Status::OK();
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

Status FunctionToFile::create_filesystem(const TFileResourceSnapshot& resource,
                                         const std::string& uri,
                                         std::shared_ptr<io::FileSystem>* filesystem) const {
    io::FSPropertiesRef properties(resource.file_type);
    properties.properties = &resource.properties;
    const std::vector<TNetworkAddress> broker_addresses;
    properties.broker_addresses = &broker_addresses;
    io::FileDescription description;
    if (resource.file_type == TFileType::FILE_S3) {
        description.path = uri;
    } else if (resource.file_type == TFileType::FILE_HDFS) {
        const auto fs_name = resource.properties.find("fs.defaultFS");
        if (fs_name == resource.properties.end() || fs_name->second.empty()) {
            return Status::InvalidArgument("TO_FILE HDFS resource snapshot requires fs.defaultFS");
        }
        description.fs_name = fs_name->second;
    }
    *filesystem = DORIS_TRY(FileFactory::create_fs(properties, description));
    return Status::OK();
}

Status FunctionToFile::open(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::THREAD_LOCAL || context->get_function_state(scope)) {
        return Status::OK();
    }
    const auto& resource = context->file_resource();
    if (!resource) return Status::InvalidArgument("TO_FILE requires a resource snapshot");
    const auto* resource_column = context->get_constant_col(0);
    if (!resource_column || resource_column->column_ptr->is_null_at(0) ||
        resource_column->column_ptr->get_data_at(0).to_string() != resource->resource_name) {
        return Status::InvalidArgument("TO_FILE resource literal does not match its snapshot");
    }
    // Creating a filesystem can connect remotely (for example HDFS). Pin the
    // snapshot here, and defer client creation until a non-NULL URI needs it.
    auto state = std::make_shared<ToFileState>(resource);
    if (auto* runtime = context->state()) {
        // RuntimeState outlives the fragment contexts, including their clones.
        state->stat_context.is_cancelled = [runtime] { return runtime->is_cancelled(); };
    }
    context->set_function_state(scope, std::move(state));
    return Status::OK();
}

Status FunctionToFile::close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::FRAGMENT_LOCAL) context->set_function_state(scope, nullptr);
    return Status::OK();
}

Status FunctionToFile::execute_impl(FunctionContext* context, Block& block,
                                    const ColumnNumbers& arguments, uint32_t result,
                                    size_t input_rows_count) const {
    auto* state =
            static_cast<ToFileState*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    if (!state) return Status::InternalError("TO_FILE has no fragment-local state");
    const auto& source = block.get_by_position(arguments[1]).column;
    auto output = ColumnFile::create();
    auto nulls = ColumnUInt8::create(input_rows_count, 0);
    for (size_t row = 0; row < input_rows_count; ++row) {
        if (context->state()) RETURN_IF_CANCELLED(context->state());
        if (source->is_null_at(row)) {
            output->insert_default();
            nulls->get_data()[row] = 1;
            continue;
        }
        const auto uri = source->get_data_at(row).to_string();
        io::FileStat metadata;
        Status stat_status;
        {
            // FunctionContext clones share filesystem initialization and metadata calls.
            std::lock_guard lock(state->mutex);
            if (context->state()) RETURN_IF_CANCELLED(context->state());
            std::string bucket;
            if (state->resource->file_type == TFileType::FILE_S3) {
                S3URI s3_uri(uri);
                RETURN_IF_ERROR(s3_uri.parse());
                bucket = s3_uri.get_bucket();
            }
            // S3FileSystem binds its bucket at construction. Keep only the current
            // filesystem and switch when a row references a different bucket.
            if (!state->filesystem || state->bucket != bucket) {
                std::shared_ptr<io::FileSystem> filesystem;
                RETURN_IF_ERROR(create_filesystem(*state->resource, uri, &filesystem));
                DORIS_CHECK(filesystem != nullptr);
                state->filesystem = std::move(filesystem);
                state->bucket = std::move(bucket);
                if (context->state()) RETURN_IF_CANCELLED(context->state());
            }
            stat_status = state->filesystem->stat(uri, &metadata, &state->stat_context);
        }
        if (context->state()) RETURN_IF_CANCELLED(context->state());
        RETURN_IF_ERROR(stat_status);
        std::string content_type;
        RETURN_IF_ERROR(to_file_content_type(uri, metadata, content_type));
        File value(DataTypeFile::FIELD_COUNT);
        value[0] = Field::create_field<TYPE_STRING>(uri);
        value[2] = Field::create_field<TYPE_BIGINT>(metadata.size);
        value[3] = Field::create_field<TYPE_STRING>(std::move(content_type));
        if (metadata.checksum) value[4] = Field::create_field<TYPE_STRING>(*metadata.checksum);
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
    factory.register_function<FunctionFileDataSize>();
}

} // namespace doris
