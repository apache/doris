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

#include "format_v2/table/iceberg_reader.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <gtest/gtest.h>
#include <parquet/api/reader.h>
#include <parquet/arrow/writer.h>

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <typeinfo>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/consts.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_timestamptz.h"
#include "core/data_type/data_type_varbinary.h"
#include "exec/common/endian.h"
#include "exprs/runtime_filter_expr.h"
#include "exprs/vectorized_fn_call.h"
#include "exprs/vexpr.h"
#include "exprs/vliteral.h"
#include "exprs/vslot_ref.h"
#include "format/format_common.h"
#include "format/table/deletion_vector_reader.h"
#include "format_v2/table_reader.h"
#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/ExternalTableSchema_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "io/fs/file_meta_cache.h"
#include "io/fs/local_file_system.h"
#include "io/io_common.h"
#include "roaring/roaring64map.hh"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "storage/segment/condition_cache.h"
#include "util/debug_points.h"

namespace doris::format {
namespace {

LocalColumnIndex field_projection(int32_t column_id) {
    return LocalColumnIndex {.index = column_id};
}

std::vector<int32_t> projection_ids(const std::vector<LocalColumnIndex>& projections) {
    std::vector<int32_t> ids;
    ids.reserve(projections.size());
    for (const auto& projection : projections) {
        ids.push_back(projection.index);
    }
    return ids;
}
VExprSPtr table_int32_slot_ref(int slot_id, int column_id, const std::string& column_name) {
    const auto nullable_int_type = make_nullable(std::make_shared<DataTypeInt32>());
    return VSlotRef::create_shared(slot_id, column_id, slot_id, nullable_int_type, column_name);
}

VExprSPtr table_int32_literal(int32_t value) {
    return VLiteral::create_shared(std::make_shared<DataTypeInt32>(),
                                   Field::create_field<TYPE_INT>(value));
}

VExprSPtr table_int64_literal(int64_t value) {
    return VLiteral::create_shared(std::make_shared<DataTypeInt64>(),
                                   Field::create_field<TYPE_BIGINT>(value));
}

TExprNode table_function_node(const std::string& function_name, const DataTypePtr& return_type,
                              const std::vector<DataTypePtr>& arg_types,
                              TExprNodeType::type node_type,
                              TExprOpcode::type opcode = TExprOpcode::INVALID_OPCODE,
                              bool short_circuit_evaluation = false) {
    TFunctionName fn_name;
    fn_name.__set_function_name(function_name);
    TFunction fn;
    fn.__set_name(fn_name);
    fn.__set_binary_type(TFunctionBinaryType::BUILTIN);
    std::vector<TTypeDesc> thrift_arg_types;
    thrift_arg_types.reserve(arg_types.size());
    for (const auto& arg_type : arg_types) {
        thrift_arg_types.push_back(arg_type->to_thrift());
    }
    fn.__set_arg_types(thrift_arg_types);
    fn.__set_ret_type(return_type->to_thrift());
    fn.__set_has_var_args(false);

    TExprNode node;
    node.__set_node_type(node_type);
    node.__set_opcode(opcode);
    node.__set_type(return_type->to_thrift());
    node.__set_fn(fn);
    node.__set_num_children(static_cast<int16_t>(arg_types.size()));
    node.__set_is_nullable(return_type->is_nullable());
    if (short_circuit_evaluation) {
        node.__set_short_circuit_evaluation(true);
    }
    return node;
}

VExprSPtr table_function_expr(const std::string& function_name, const DataTypePtr& return_type,
                              const std::vector<DataTypePtr>& arg_types,
                              TExprNodeType::type node_type = TExprNodeType::FUNCTION_CALL,
                              TExprOpcode::type opcode = TExprOpcode::INVALID_OPCODE) {
    const auto node = table_function_node(function_name, return_type, arg_types, node_type, opcode);
    return VectorizedFnCall::create_shared(node);
}

VExprSPtr table_int32_greater_than_expr(int slot_id, int column_id, int32_t value) {
    const auto int_type = std::make_shared<DataTypeInt32>();
    const auto nullable_int_type = make_nullable(int_type);
    auto expr = table_function_expr("gt", make_nullable(std::make_shared<DataTypeUInt8>()),
                                    {nullable_int_type, int_type}, TExprNodeType::BINARY_PRED,
                                    TExprOpcode::GT);
    expr->add_child(table_int32_slot_ref(slot_id, column_id, "id"));
    expr->add_child(table_int32_literal(value));
    return expr;
}

VExprSPtr table_nullable_int64_binary_predicate(const std::string& function_name,
                                                TExprOpcode::type opcode, int slot_id,
                                                int column_id, const std::string& column_name,
                                                int64_t value) {
    const auto int64_type = std::make_shared<DataTypeInt64>();
    const auto nullable_int64_type = make_nullable(int64_type);
    auto expr = table_function_expr(function_name, make_nullable(std::make_shared<DataTypeUInt8>()),
                                    {nullable_int64_type, int64_type}, TExprNodeType::BINARY_PRED,
                                    opcode);
    expr->add_child(
            VSlotRef::create_shared(slot_id, column_id, slot_id, nullable_int64_type, column_name));
    expr->add_child(table_int64_literal(value));
    return expr;
}

class IcebergTableReaderDeleteFileTestHelper final
        : public doris::format::iceberg::IcebergTableReader {
public:
    Status parse_deletion_vector_file(const TTableFormatFileDesc& t_desc, DeleteFileDesc* desc,
                                      bool* has_delete_file) {
        return _parse_deletion_vector_file(t_desc, desc, has_delete_file);
    }
};

class IcebergTableReaderScanRequestTestHelper final
        : public doris::format::iceberg::IcebergTableReader {
public:
    Status init_for_scan_request_test(std::vector<ColumnDefinition> projected_columns) {
        _query_options = std::make_unique<TQueryOptions>();
        _query_globals = std::make_unique<TQueryGlobals>();
        _state = std::make_unique<RuntimeState>(*_query_options, *_query_globals);
        RETURN_IF_ERROR(init({
                .projected_columns = std::move(projected_columns),
                .conjuncts = {},
                .format = FileFormat::PARQUET,
                .scan_params = nullptr,
                .io_ctx = nullptr,
                .runtime_state = _state.get(),
                .scanner_profile = nullptr,
        }));

        SplitReadOptions split_options;
        split_options.current_range.__set_path("scan-request-test.parquet");
        TTableFormatFileDesc table_format_params;
        TIcebergFileDesc iceberg_params;
        iceberg_params.__set_first_row_id(1000);
        table_format_params.__set_iceberg_params(iceberg_params);
        split_options.current_range.__set_table_format_params(table_format_params);
        RETURN_IF_ERROR(prepare_split(split_options));

        _delete_rows_storage = {1};
        _delete_rows = &_delete_rows_storage;
        return Status::OK();
    }

    Status customize_request(FileScanRequest* request) {
        return customize_file_scan_request(request);
    }

    bool current_data_file_is_immutable() const {
        DORIS_CHECK(_current_task != nullptr);
        DORIS_CHECK(_current_task->data_file != nullptr);
        return _current_task->data_file->is_immutable;
    }

private:
    std::unique_ptr<TQueryOptions> _query_options;
    std::unique_ptr<TQueryGlobals> _query_globals;
    std::unique_ptr<RuntimeState> _state;
    DeleteRows _delete_rows_storage;
};

class IcebergTableReaderMappingModeTestHelper final
        : public doris::format::iceberg::IcebergTableReader {
public:
    TableColumnMappingMode mapping_mode_for_schema(std::vector<ColumnDefinition> file_schema) {
        _data_reader.file_schema = std::move(file_schema);
        return mapping_mode();
    }
};

std::shared_ptr<arrow::Array> finish_array(arrow::ArrayBuilder* builder) {
    std::shared_ptr<arrow::Array> array;
    EXPECT_TRUE(builder->Finish(&array).ok());
    return array;
}

std::shared_ptr<arrow::Array> build_int32_array(const std::vector<int32_t>& values) {
    arrow::Int32Builder builder;
    for (const auto value : values) {
        EXPECT_TRUE(builder.Append(value).ok());
    }
    return finish_array(&builder);
}

std::shared_ptr<arrow::Array> build_int64_array(const std::vector<int64_t>& values) {
    arrow::Int64Builder builder;
    for (const auto value : values) {
        EXPECT_TRUE(builder.Append(value).ok());
    }
    return finish_array(&builder);
}

std::shared_ptr<arrow::Array> build_nullable_int64_array(
        const std::vector<std::optional<int64_t>>& values) {
    arrow::Int64Builder builder;
    for (const auto& value : values) {
        if (value.has_value()) {
            EXPECT_TRUE(builder.Append(*value).ok());
        } else {
            EXPECT_TRUE(builder.AppendNull().ok());
        }
    }
    return finish_array(&builder);
}

std::shared_ptr<arrow::Array> build_nullable_string_array(
        const std::vector<std::optional<std::string>>& values) {
    arrow::StringBuilder builder;
    for (const auto& value : values) {
        if (value.has_value()) {
            EXPECT_TRUE(builder.Append(*value).ok());
        } else {
            EXPECT_TRUE(builder.AppendNull().ok());
        }
    }
    return finish_array(&builder);
}

std::shared_ptr<arrow::Array> build_string_array(const std::vector<std::string>& values) {
    arrow::StringBuilder builder;
    for (const auto& value : values) {
        EXPECT_TRUE(builder.Append(value).ok());
    }
    return finish_array(&builder);
}

schema::external::TFieldPtr external_schema_field(
        std::string name, int32_t id, std::vector<std::string> aliases = {},
        std::optional<std::string> initial_default = std::nullopt,
        std::optional<TColumnType> type = std::nullopt, bool initial_default_is_base64 = false) {
    auto field = std::make_shared<schema::external::TField>();
    field->__set_name(name);
    field->__set_id(id);
    if (!aliases.empty()) {
        field->__set_name_mapping(aliases);
    }
    if (initial_default.has_value()) {
        field->__set_initial_default_value(*initial_default);
        if (initial_default_is_base64) {
            field->__set_initial_default_value_is_base64(true);
        }
    }
    if (type.has_value()) {
        field->__set_type(*type);
    }
    schema::external::TFieldPtr field_ptr;
    field_ptr.field_ptr = std::move(field);
    field_ptr.__isset.field_ptr = true;
    return field_ptr;
}

TColumnType external_primitive_type(TPrimitiveType::type type, int32_t len = -1,
                                    int32_t scale = -1) {
    TColumnType result;
    result.__set_type(type);
    if (len >= 0) {
        result.__set_len(len);
    }
    if (scale >= 0) {
        result.__set_scale(scale);
    }
    return result;
}

schema::external::TSchema external_schema(int64_t schema_id,
                                          std::vector<schema::external::TFieldPtr> fields) {
    schema::external::TStructField root_field;
    root_field.__set_fields(fields);
    schema::external::TSchema schema;
    schema.__set_schema_id(schema_id);
    schema.__set_root_field(root_field);
    return schema;
}

void write_iceberg_equality_delete_parquet_file(const std::string& file_path, int32_t field_id,
                                                int32_t value,
                                                const std::string& field_name = "id") {
    const auto metadata =
            arrow::key_value_metadata({"PARQUET:field_id"}, {std::to_string(field_id)});
    auto schema = arrow::schema({
            arrow::field(field_name, arrow::int32(), false)->WithMetadata(metadata),
    });
    auto table = arrow::Table::Make(schema, {build_int32_array({value})});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    builder.compression(::parquet::Compression::UNCOMPRESSED);
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out, 1,
                                                      builder.build()));
}

void write_iceberg_timestamp_equality_delete_parquet_file(const std::string& file_path,
                                                          int32_t field_id, int64_t value,
                                                          const std::string& field_name,
                                                          bool adjusted_to_utc = false) {
    const auto metadata =
            arrow::key_value_metadata({"PARQUET:field_id"}, {std::to_string(field_id)});
    const auto timestamp_type = adjusted_to_utc ? arrow::timestamp(arrow::TimeUnit::MICRO, "UTC")
                                                : arrow::timestamp(arrow::TimeUnit::MICRO);
    arrow::TimestampBuilder value_builder(timestamp_type, arrow::default_memory_pool());
    ASSERT_TRUE(value_builder.Append(value).ok());
    auto value_result = value_builder.Finish();
    ASSERT_TRUE(value_result.ok()) << value_result.status();
    auto schema = arrow::schema({
            arrow::field(field_name, timestamp_type, false)->WithMetadata(metadata),
    });
    auto table = arrow::Table::Make(schema, {*value_result});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;
    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    builder.compression(::parquet::Compression::UNCOMPRESSED);
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out, 1,
                                                      builder.build()));
}

void write_iceberg_binary_equality_delete_parquet_file(const std::string& file_path,
                                                       int32_t field_id, const std::string& value,
                                                       const std::string& field_name) {
    const auto metadata =
            arrow::key_value_metadata({"PARQUET:field_id"}, {std::to_string(field_id)});
    arrow::BinaryBuilder value_builder;
    ASSERT_TRUE(value_builder.Append(value).ok());
    auto value_result = value_builder.Finish();
    ASSERT_TRUE(value_result.ok()) << value_result.status();
    auto schema = arrow::schema({
            arrow::field(field_name, arrow::binary(), false)->WithMetadata(metadata),
    });
    auto table = arrow::Table::Make(schema, {*value_result});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;
    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    builder.compression(::parquet::Compression::UNCOMPRESSED);
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out, 1,
                                                      builder.build()));
}

void write_iceberg_null_equality_delete_parquet_file(const std::string& file_path, int32_t field_id,
                                                     const std::string& field_name) {
    const auto metadata =
            arrow::key_value_metadata({"PARQUET:field_id"}, {std::to_string(field_id)});
    auto schema = arrow::schema({
            arrow::field(field_name, arrow::int32(), true)->WithMetadata(metadata),
    });
    arrow::Int32Builder value_builder;
    ASSERT_TRUE(value_builder.AppendNull().ok());
    auto value_result = value_builder.Finish();
    ASSERT_TRUE(value_result.ok()) << value_result.status();
    auto table = arrow::Table::Make(schema, {*value_result});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    builder.compression(::parquet::Compression::UNCOMPRESSED);
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out, 1,
                                                      builder.build()));
}

void write_single_int_parquet_file(const std::string& file_path, const std::string& field_name,
                                   const std::vector<int32_t>& values,
                                   std::optional<int32_t> field_id) {
    auto field = arrow::field(field_name, arrow::int32(), false);
    if (field_id.has_value()) {
        field = field->WithMetadata(
                arrow::key_value_metadata({"PARQUET:field_id"}, {std::to_string(*field_id)}));
    }
    auto table = arrow::Table::Make(arrow::schema({field}), {build_int32_array(values)});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    builder.compression(::parquet::Compression::UNCOMPRESSED);
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out,
                                                      static_cast<int64_t>(values.size()),
                                                      builder.build()));
}

void write_two_int_parquet_file(const std::string& file_path, const std::string& first_name,
                                const std::vector<int32_t>& first_values,
                                std::optional<int32_t> first_field_id,
                                const std::string& second_name,
                                const std::vector<int32_t>& second_values,
                                std::optional<int32_t> second_field_id) {
    ASSERT_EQ(first_values.size(), second_values.size());
    auto first_field = arrow::field(first_name, arrow::int32(), false);
    if (first_field_id.has_value()) {
        first_field = first_field->WithMetadata(
                arrow::key_value_metadata({"PARQUET:field_id"}, {std::to_string(*first_field_id)}));
    }
    auto second_field = arrow::field(second_name, arrow::int32(), false);
    if (second_field_id.has_value()) {
        second_field = second_field->WithMetadata(arrow::key_value_metadata(
                {"PARQUET:field_id"}, {std::to_string(*second_field_id)}));
    }
    auto schema = arrow::schema({first_field, second_field});
    auto table = arrow::Table::Make(
            schema, {build_int32_array(first_values), build_int32_array(second_values)});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;
    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    builder.compression(::parquet::Compression::UNCOMPRESSED);
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out,
                                                      static_cast<int64_t>(first_values.size()),
                                                      builder.build()));
}

void write_timestamp_int_parquet_file(const std::string& file_path,
                                      const std::vector<int64_t>& timestamps,
                                      const std::vector<int32_t>& ids) {
    ASSERT_EQ(timestamps.size(), ids.size());
    const auto timestamp_metadata = arrow::key_value_metadata({"PARQUET:field_id"}, {"0"});
    const auto id_metadata = arrow::key_value_metadata({"PARQUET:field_id"}, {"1"});
    const auto timestamp_type = arrow::timestamp(arrow::TimeUnit::MICRO, "UTC");
    arrow::TimestampBuilder timestamp_builder(timestamp_type, arrow::default_memory_pool());
    ASSERT_TRUE(timestamp_builder.AppendValues(timestamps).ok());
    auto timestamp_result = timestamp_builder.Finish();
    ASSERT_TRUE(timestamp_result.ok()) << timestamp_result.status();
    auto schema = arrow::schema(
            {arrow::field("event_time", timestamp_type, false)->WithMetadata(timestamp_metadata),
             arrow::field("id", arrow::int32(), false)->WithMetadata(id_metadata)});
    auto table = arrow::Table::Make(schema, {*timestamp_result, build_int32_array(ids)});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;
    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    builder.compression(::parquet::Compression::UNCOMPRESSED);
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out,
                                                      static_cast<int64_t>(timestamps.size()),
                                                      builder.build()));
}

void write_iceberg_equality_delete_bigint_parquet_file(const std::string& file_path,
                                                       int32_t field_id, int64_t value) {
    const auto metadata =
            arrow::key_value_metadata({"PARQUET:field_id"}, {std::to_string(field_id)});
    auto schema = arrow::schema({
            arrow::field("id", arrow::int64(), false)->WithMetadata(metadata),
    });
    auto table = arrow::Table::Make(schema, {build_int64_array({value})});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    builder.compression(::parquet::Compression::UNCOMPRESSED);
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out, 1,
                                                      builder.build()));
}

void write_int_pair_parquet_file(const std::string& file_path, const std::vector<int32_t>& ids,
                                 const std::vector<int32_t>& scores,
                                 const std::vector<std::string>& values,
                                 int64_t row_group_size = -1) {
    const auto id_metadata = arrow::key_value_metadata({"PARQUET:field_id"}, {"0"});
    const auto score_metadata = arrow::key_value_metadata({"PARQUET:field_id"}, {"1"});
    const auto value_metadata = arrow::key_value_metadata({"PARQUET:field_id"}, {"2"});
    auto schema = arrow::schema({
            arrow::field("id", arrow::int32(), false)->WithMetadata(id_metadata),
            arrow::field("score", arrow::int32(), false)->WithMetadata(score_metadata),
            arrow::field("value", arrow::utf8(), false)->WithMetadata(value_metadata),
    });
    auto table = arrow::Table::Make(schema, {build_int32_array(ids), build_int32_array(scores),
                                             build_string_array(values)});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    builder.compression(::parquet::Compression::UNCOMPRESSED);
    const auto write_row_group_size =
            row_group_size > 0 ? row_group_size : static_cast<int64_t>(ids.size());
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out,
                                                      write_row_group_size, builder.build()));
}

void write_iceberg_row_lineage_parquet_file(
        const std::string& file_path, const std::vector<int32_t>& ids,
        const std::vector<std::optional<int64_t>>& row_ids,
        const std::vector<std::optional<int64_t>>& last_updated_sequence_numbers = {}) {
    ASSERT_EQ(ids.size(), row_ids.size());
    if (!last_updated_sequence_numbers.empty()) {
        ASSERT_EQ(ids.size(), last_updated_sequence_numbers.size());
    }
    const auto id_metadata = arrow::key_value_metadata({"PARQUET:field_id"}, {"0"});
    const auto row_id_metadata = arrow::key_value_metadata({"PARQUET:field_id"}, {"2147483540"});
    const auto last_updated_sequence_number_metadata =
            arrow::key_value_metadata({"PARQUET:field_id"}, {"2147483539"});
    auto schema = arrow::schema({
            arrow::field("id", arrow::int32(), false)->WithMetadata(id_metadata),
            arrow::field("_row_id", arrow::int64(), true)->WithMetadata(row_id_metadata),
    });
    std::vector<std::shared_ptr<arrow::Array>> arrays = {
            build_int32_array(ids),
            build_nullable_int64_array(row_ids),
    };
    if (!last_updated_sequence_numbers.empty()) {
        schema =
                schema->AddField(schema->num_fields(),
                                 arrow::field("_last_updated_sequence_number", arrow::int64(), true)
                                         ->WithMetadata(last_updated_sequence_number_metadata))
                        .ValueOrDie();
        arrays.push_back(build_nullable_int64_array(last_updated_sequence_numbers));
    }
    auto table = arrow::Table::Make(schema, arrays);

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    builder.compression(::parquet::Compression::UNCOMPRESSED);
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out,
                                                      static_cast<int64_t>(ids.size()),
                                                      builder.build()));
}

void write_position_delete_parquet_file(const std::string& file_path,
                                        const std::vector<std::string>& data_file_paths,
                                        const std::vector<int64_t>& positions) {
    auto schema = arrow::schema({
            arrow::field("file_path", arrow::utf8(), false),
            arrow::field("pos", arrow::int64(), false),
    });
    auto table = arrow::Table::Make(
            schema, {build_string_array(data_file_paths), build_int64_array(positions)});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    builder.compression(::parquet::Compression::UNCOMPRESSED);
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out,
                                                      static_cast<int64_t>(positions.size()),
                                                      builder.build()));
}

void write_nullable_position_delete_parquet_file(
        const std::string& file_path,
        const std::vector<std::optional<std::string>>& data_file_paths,
        const std::vector<std::optional<int64_t>>& positions) {
    ASSERT_EQ(data_file_paths.size(), positions.size());
    auto schema = arrow::schema({
            arrow::field("file_path", arrow::utf8(), true),
            arrow::field("pos", arrow::int64(), true),
    });
    auto table = arrow::Table::Make(schema, {build_nullable_string_array(data_file_paths),
                                             build_nullable_int64_array(positions)});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    builder.compression(::parquet::Compression::UNCOMPRESSED);
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out,
                                                      static_cast<int64_t>(positions.size()),
                                                      builder.build()));
}

int64_t write_iceberg_deletion_vector_file(const std::string& file_path,
                                           const std::vector<uint64_t>& deleted_positions) {
    roaring::Roaring64Map rows;
    for (const auto position : deleted_positions) {
        rows.add(position);
    }

    const size_t bitmap_size = rows.getSizeInBytes();
    std::vector<char> blob(4 + 4 + bitmap_size + 4);
    rows.write(blob.data() + 8);

    const uint32_t total_length = static_cast<uint32_t>(4 + bitmap_size);
    BigEndian::Store32(blob.data(), total_length);
    constexpr char DV_MAGIC[] = {'\xD1', '\xD3', '\x39', '\x64'};
    memcpy(blob.data() + 4, DV_MAGIC, 4);
    BigEndian::Store32(blob.data() + 8 + bitmap_size, 0);

    std::ofstream output(file_path, std::ios::binary);
    EXPECT_TRUE(output.is_open());
    output.write(blob.data(), static_cast<std::streamsize>(blob.size()));
    EXPECT_TRUE(output.good());
    return static_cast<int64_t>(blob.size());
}

class ScopedDebugPoint {
public:
    explicit ScopedDebugPoint(std::string name)
            : _name(std::move(name)), _enable_debug_points(config::enable_debug_points) {
        config::enable_debug_points = true;
        DebugPoints::instance()->add(_name);
    }

    ~ScopedDebugPoint() {
        DebugPoints::instance()->remove(_name);
        config::enable_debug_points = _enable_debug_points;
    }

private:
    std::string _name;
    bool _enable_debug_points;
};

Block build_table_block(const std::vector<ColumnDefinition>& columns) {
    Block block;
    for (const auto& column : columns) {
        block.insert({column.type->create_column(), column.type, column.name});
    }
    return block;
}

void expect_nullable_int64_column_values(const IColumn& column,
                                         const std::vector<int64_t>& expected_values) {
    const auto full_column = column.convert_to_full_column_if_const();
    const auto& nullable_column = assert_cast<const ColumnNullable&>(*full_column);
    const auto& values =
            assert_cast<const ColumnInt64&>(nullable_column.get_nested_column()).get_data();
    ASSERT_EQ(nullable_column.size(), expected_values.size());
    for (size_t row = 0; row < expected_values.size(); ++row) {
        EXPECT_EQ(nullable_column.get_null_map_data()[row], 0);
        EXPECT_EQ(values[row], expected_values[row]);
    }
}

void expect_nullable_int64_column_optional_values(
        const IColumn& column, const std::vector<std::optional<int64_t>>& expected_values) {
    const auto full_column = column.convert_to_full_column_if_const();
    const auto& nullable_column = assert_cast<const ColumnNullable&>(*full_column);
    const auto& values =
            assert_cast<const ColumnInt64&>(nullable_column.get_nested_column()).get_data();
    ASSERT_EQ(nullable_column.size(), expected_values.size());
    for (size_t row = 0; row < expected_values.size(); ++row) {
        if (expected_values[row].has_value()) {
            EXPECT_EQ(nullable_column.get_null_map_data()[row], 0);
            EXPECT_EQ(values[row], *expected_values[row]);
        } else {
            EXPECT_EQ(nullable_column.get_null_map_data()[row], 1);
        }
    }
}

const IColumn& expect_not_null_nullable_nested_column(const IColumn& column) {
    if (!column.is_nullable()) {
        return column;
    }
    const auto& nullable_column = assert_cast<const ColumnNullable&>(column);
    for (const auto is_null : nullable_column.get_null_map_data()) {
        EXPECT_EQ(is_null, 0);
    }
    return nullable_column.get_nested_column();
}

const IColumn& expect_not_null_table_column(const Block& block, size_t position) {
    return expect_not_null_nullable_nested_column(*block.get_by_position(position).column);
}

ColumnDefinition make_table_column(int32_t id, const std::string& name, const DataTypePtr& type);

DataTypePtr make_iceberg_rowid_type() {
    return make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {std::make_shared<DataTypeString>(), std::make_shared<DataTypeInt64>(),
                       std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeString>()},
            Strings {"file_path", "row_pos", "partition_spec_id", "partition_data_json"}));
}

ColumnDefinition make_iceberg_row_lineage_row_id_column() {
    return make_table_column(2147483540, "_row_id",
                             make_nullable(std::make_shared<DataTypeInt64>()));
}

ColumnDefinition make_iceberg_last_updated_sequence_number_column() {
    return make_table_column(2147483539, "_last_updated_sequence_number",
                             make_nullable(std::make_shared<DataTypeInt64>()));
}

void expect_iceberg_rowid_column_values(const IColumn& column, const std::string& file_path,
                                        const std::vector<int64_t>& row_positions,
                                        int32_t partition_spec_id,
                                        const std::string& partition_data_json) {
    const auto full_column = column.convert_to_full_column_if_const();
    const auto& nullable_column = assert_cast<const ColumnNullable&>(*full_column);
    const auto& struct_column =
            assert_cast<const ColumnStruct&>(nullable_column.get_nested_column());
    const auto& file_path_column = assert_cast<const ColumnString&>(
            expect_not_null_nullable_nested_column(struct_column.get_column(0)));
    const auto& row_pos_column = assert_cast<const ColumnInt64&>(
            expect_not_null_nullable_nested_column(struct_column.get_column(1)));
    const auto& spec_id_column = assert_cast<const ColumnInt32&>(
            expect_not_null_nullable_nested_column(struct_column.get_column(2)));
    const auto& partition_data_column = assert_cast<const ColumnString&>(
            expect_not_null_nullable_nested_column(struct_column.get_column(3)));

    ASSERT_EQ(nullable_column.size(), row_positions.size());
    for (size_t row = 0; row < row_positions.size(); ++row) {
        EXPECT_EQ(nullable_column.get_null_map_data()[row], 0);
        EXPECT_EQ(file_path_column.get_data_at(row).to_string(), file_path);
        EXPECT_EQ(row_pos_column.get_element(row), row_positions[row]);
        EXPECT_EQ(spec_id_column.get_element(row), partition_spec_id);
        EXPECT_EQ(partition_data_column.get_data_at(row).to_string(), partition_data_json);
    }
}

void expect_int32_column_values(const IColumn& column,
                                const std::vector<int32_t>& expected_values) {
    const auto full_column = column.convert_to_full_column_if_const();
    const auto& nested_column = expect_not_null_nullable_nested_column(*full_column);
    const auto& values = assert_cast<const ColumnInt32&>(nested_column).get_data();
    ASSERT_EQ(values.size(), expected_values.size());
    for (size_t row = 0; row < expected_values.size(); ++row) {
        EXPECT_EQ(values[row], expected_values[row]);
    }
}

SplitReadOptions build_split_options(const std::string& file_path) {
    SplitReadOptions options;
    EXPECT_EQ(options.cache, nullptr);
    options.current_range.__set_path(file_path);
    options.current_range.__set_file_size(
            static_cast<int64_t>(std::filesystem::file_size(file_path)));
    return options;
}

void set_table_level_row_count(SplitReadOptions* split_options, int64_t row_count) {
    split_options->current_range.__isset.table_format_params = true;
    split_options->current_range.table_format_params.__isset.table_level_row_count = true;
    split_options->current_range.table_format_params.table_level_row_count = row_count;
}

void set_iceberg_row_lineage_params(SplitReadOptions* split_options, int64_t first_row_id,
                                    int64_t last_updated_sequence_number) {
    TTableFormatFileDesc table_format_params;
    TIcebergFileDesc iceberg_params;
    iceberg_params.__set_first_row_id(first_row_id);
    iceberg_params.__set_last_updated_sequence_number(last_updated_sequence_number);
    table_format_params.__set_iceberg_params(iceberg_params);
    split_options->current_range.__set_table_format_params(table_format_params);
}

void set_iceberg_rowid_params(SplitReadOptions* split_options,
                              const std::string& original_file_path, int32_t partition_spec_id,
                              const std::string& partition_data_json) {
    TTableFormatFileDesc table_format_params;
    TIcebergFileDesc iceberg_params;
    iceberg_params.__set_original_file_path(original_file_path);
    iceberg_params.__set_partition_spec_id(partition_spec_id);
    iceberg_params.__set_partition_data_json(partition_data_json);
    table_format_params.__set_iceberg_params(iceberg_params);
    split_options->current_range.__set_table_format_params(table_format_params);
}

TIcebergDeleteFileDesc make_iceberg_deletion_vector(const std::string& path, int64_t offset,
                                                    int64_t size) {
    TIcebergDeleteFileDesc delete_file;
    delete_file.__set_content(3);
    delete_file.__set_path(path);
    delete_file.__set_content_offset(offset);
    delete_file.__set_content_size_in_bytes(size);
    return delete_file;
}

TIcebergDeleteFileDesc make_iceberg_position_delete_file(const std::string& path) {
    TIcebergDeleteFileDesc delete_file;
    delete_file.__set_content(1);
    delete_file.__set_path(path);
    delete_file.__set_file_format(TFileFormatType::FORMAT_PARQUET);
    return delete_file;
}

TIcebergDeleteFileDesc make_iceberg_equality_delete_file(const std::string& path,
                                                         const std::vector<int32_t>& field_ids) {
    TIcebergDeleteFileDesc delete_file;
    delete_file.__set_content(2);
    delete_file.__set_path(path);
    delete_file.__set_field_ids(field_ids);
    delete_file.__set_file_format(TFileFormatType::FORMAT_PARQUET);
    return delete_file;
}

TFileScanRangeParams make_local_parquet_scan_params() {
    TFileScanRangeParams scan_params;
    scan_params.__set_file_type(TFileType::FILE_LOCAL);
    scan_params.__set_format_type(TFileFormatType::FORMAT_PARQUET);
    return scan_params;
}

std::shared_ptr<io::IOContext> make_io_context(io::FileReaderStats* file_reader_stats,
                                               io::FileCacheStatistics* file_cache_stats) {
    auto io_ctx = std::make_shared<io::IOContext>();
    io_ctx->file_reader_stats = file_reader_stats;
    io_ctx->file_cache_stats = file_cache_stats;
    return io_ctx;
}

TTableFormatFileDesc make_iceberg_table_format_desc(
        const std::string& data_file_path,
        const std::vector<TIcebergDeleteFileDesc>& delete_files) {
    TTableFormatFileDesc table_format_params;
    TIcebergFileDesc iceberg_params;
    iceberg_params.__set_format_version(2);
    iceberg_params.__set_original_file_path(data_file_path);
    iceberg_params.__set_delete_files(delete_files);
    table_format_params.__set_iceberg_params(iceberg_params);
    return table_format_params;
}

std::vector<int32_t> read_iceberg_ids(doris::format::iceberg::IcebergTableReader* reader,
                                      const std::vector<ColumnDefinition>& projected_columns) {
    std::vector<int32_t> ids;
    bool eos = false;
    while (!eos) {
        Block block = build_table_block(projected_columns);
        auto status = reader->get_block(&block, &eos);
        if (!status.ok()) {
            ADD_FAILURE() << status;
            return ids;
        }
        if (block.rows() == 0) {
            continue;
        }
        const auto& id_column =
                assert_cast<const ColumnInt32&>(expect_not_null_table_column(block, 0));
        for (size_t row = 0; row < block.rows(); ++row) {
            ids.push_back(id_column.get_element(row));
        }
    }
    return ids;
}

void init_iceberg_reader(doris::format::iceberg::IcebergTableReader* reader,
                         const std::vector<ColumnDefinition>& projected_columns,
                         TFileScanRangeParams* scan_params,
                         const std::shared_ptr<io::IOContext>& io_ctx, RuntimeState* state,
                         RuntimeProfile* profile) {
    ASSERT_TRUE(reader->init({
                                     .projected_columns = projected_columns,
                                     .conjuncts = {},
                                     .format = FileFormat::PARQUET,
                                     .scan_params = scan_params,
                                     .io_ctx = io_ctx,
                                     .runtime_state = state,
                                     .scanner_profile = profile,
                             })
                        .ok());
}

DataTypePtr make_table_test_type(const DataTypePtr& type, bool nullable_root = true) {
    DORIS_CHECK(type != nullptr);
    const auto nested_type = remove_nullable(type);
    DataTypePtr result;
    if (const auto* struct_type = typeid_cast<const DataTypeStruct*>(nested_type.get())) {
        DataTypes child_types;
        child_types.reserve(struct_type->get_elements().size());
        for (const auto& child_type : struct_type->get_elements()) {
            child_types.push_back(make_table_test_type(child_type));
        }
        result = std::make_shared<DataTypeStruct>(child_types, struct_type->get_element_names());
    } else if (const auto* array_type = typeid_cast<const DataTypeArray*>(nested_type.get())) {
        result = std::make_shared<DataTypeArray>(
                make_table_test_type(array_type->get_nested_type()));
    } else if (const auto* map_type = typeid_cast<const DataTypeMap*>(nested_type.get())) {
        result = std::make_shared<DataTypeMap>(make_table_test_type(map_type->get_key_type()),
                                               make_table_test_type(map_type->get_value_type()));
    } else {
        result = nested_type;
    }
    return nullable_root ? make_nullable(result) : result;
}

ColumnDefinition make_table_column(int32_t id, const std::string& name, const DataTypePtr& type) {
    ColumnDefinition column;
    if (id >= 0) {
        column.identifier = Field::create_field<TYPE_INT>(id);
    }
    column.name = name;
    // TableReader tests model external table scan descriptors. Those table columns are nullable
    // even when the Parquet file field itself is required, so keep the test schema aligned with
    // the real scan contract at the construction boundary.
    column.type = make_table_test_type(type);
    return column;
}

ColumnDefinition make_file_column(int32_t id, const std::string& name, const DataTypePtr& type) {
    ColumnDefinition field;
    field.identifier = Field::create_field<TYPE_INT>(id);
    field.local_id = id;
    field.name = name;
    field.type = make_table_test_type(type);
    return field;
}

void set_name_identifiers(std::vector<ColumnDefinition>* columns);

void set_name_identifier(ColumnDefinition* column) {
    DORIS_CHECK(column != nullptr);
    column->identifier = Field::create_field<TYPE_STRING>(column->name);
    set_name_identifiers(&column->children);
}

void set_name_identifiers(std::vector<ColumnDefinition>* columns) {
    DORIS_CHECK(columns != nullptr);
    for (auto& column : *columns) {
        set_name_identifier(&column);
    }
}

VExprContextSPtr prepared_conjunct(RuntimeState* state, const VExprSPtr& expr) {
    auto ctx = VExprContext::create_shared(expr);
    auto status = ctx->prepare(state, RowDescriptor());
    EXPECT_TRUE(status.ok()) << status;
    status = ctx->open(state);
    EXPECT_TRUE(status.ok()) << status;
    return ctx;
}

void apply_final_conjuncts(Block* block, const VExprContextSPtrs& conjuncts) {
    const auto status = VExprContext::filter_block(conjuncts, block, block->columns());
    ASSERT_TRUE(status.ok()) << status;
}

TEST(IcebergV2ReaderTest, IcebergVirtualColumnsUseRowLineageMetadata) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_iceberg_virtual_columns_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3}, {10, 20, 30}, {"one", "two", "three"});

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_iceberg_row_lineage_row_id_column());
    projected_columns.push_back(make_iceberg_last_updated_sequence_number_column());
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    doris::format::iceberg::IcebergTableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .conjuncts = {prepared_conjunct(
                                            &state, table_int32_greater_than_expr(2, 2, 1))},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                            })
                        .ok());

    auto split_options = build_split_options(file_path);
    set_iceberg_row_lineage_params(&split_options, 1000, 77);
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);

    const auto& id_column = assert_cast<const ColumnInt32&>(expect_not_null_table_column(block, 2));

    ASSERT_EQ(block.rows(), 2);
    EXPECT_EQ(id_column.get_element(0), 2);
    EXPECT_EQ(id_column.get_element(1), 3);
    expect_nullable_int64_column_values(*block.get_by_position(0).column, {1001, 1002});
    expect_nullable_int64_column_values(*block.get_by_position(1).column, {77, 77});

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, IcebergRowLineageUsesPhysicalRowIdAndFillsNulls) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_iceberg_physical_row_id_fill_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_iceberg_row_lineage_parquet_file(file_path, {1, 2, 3}, {7000, std::nullopt, 7002},
                                           {80, std::nullopt, 82});

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(
            2147483540, "_row_id", make_nullable(std::make_shared<DataTypeInt64>())));
    projected_columns.push_back(
            make_table_column(2147483539, "_last_updated_sequence_number",
                              make_nullable(std::make_shared<DataTypeInt64>())));
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    doris::format::iceberg::IcebergTableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                            })
                        .ok());

    auto split_options = build_split_options(file_path);
    set_iceberg_row_lineage_params(&split_options, 1000, 77);
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);

    ASSERT_EQ(block.rows(), 3);
    expect_nullable_int64_column_values(*block.get_by_position(0).column, {7000, 1001, 7002});
    expect_nullable_int64_column_values(*block.get_by_position(1).column, {80, 77, 82});
    expect_int32_column_values(*block.get_by_position(2).column, {1, 2, 3});

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, IcebergPhysicalRowIdKeepsNullsWithoutFirstRowId) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_iceberg_physical_row_id_no_first_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_iceberg_row_lineage_parquet_file(file_path, {1, 2, 3}, {7000, std::nullopt, 7002},
                                           {80, std::nullopt, 82});

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(
            2147483540, "_row_id", make_nullable(std::make_shared<DataTypeInt64>())));
    projected_columns.push_back(
            make_table_column(2147483539, "_last_updated_sequence_number",
                              make_nullable(std::make_shared<DataTypeInt64>())));
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    doris::format::iceberg::IcebergTableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                            })
                        .ok());

    ASSERT_TRUE(reader.prepare_split(build_split_options(file_path)).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);

    ASSERT_EQ(block.rows(), 3);
    expect_nullable_int64_column_optional_values(
            *block.get_by_position(0).column,
            std::vector<std::optional<int64_t>> {7000, std::nullopt, 7002});
    expect_nullable_int64_column_optional_values(
            *block.get_by_position(1).column,
            std::vector<std::optional<int64_t>> {80, std::nullopt, 82});
    expect_int32_column_values(*block.get_by_position(2).column, {1, 2, 3});

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, IcebergMissingRowIdStaysNullWithoutFirstRowId) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_iceberg_missing_row_id_no_first_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3}, {10, 20, 30}, {"one", "two", "three"});

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_iceberg_row_lineage_row_id_column());
    projected_columns.push_back(make_iceberg_last_updated_sequence_number_column());
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    doris::format::iceberg::IcebergTableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                            })
                        .ok());

    ASSERT_TRUE(reader.prepare_split(build_split_options(file_path)).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);

    ASSERT_EQ(block.rows(), 3);
    expect_nullable_int64_column_optional_values(
            *block.get_by_position(0).column,
            std::vector<std::optional<int64_t>> {std::nullopt, std::nullopt, std::nullopt});
    expect_nullable_int64_column_optional_values(
            *block.get_by_position(1).column,
            std::vector<std::optional<int64_t>> {std::nullopt, std::nullopt, std::nullopt});
    expect_int32_column_values(*block.get_by_position(2).column, {1, 2, 3});

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, IcebergRowIdPredicateFiltersAfterRowLineageMaterialization) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_iceberg_row_id_finalize_filter_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_iceberg_row_lineage_parquet_file(file_path, {1, 2, 3}, {7000, std::nullopt, 7002},
                                           {80, std::nullopt, 82});

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(
            2147483540, "_row_id", make_nullable(std::make_shared<DataTypeInt64>())));
    projected_columns.push_back(
            make_table_column(2147483539, "_last_updated_sequence_number",
                              make_nullable(std::make_shared<DataTypeInt64>())));
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    VExprContextSPtrs conjuncts = {prepared_conjunct(
            &state,
            table_nullable_int64_binary_predicate("eq", TExprOpcode::EQ, 0, 0, "_row_id", 1001))};
    doris::format::iceberg::IcebergTableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .conjuncts = conjuncts,
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                            })
                        .ok());

    auto split_options = build_split_options(file_path);
    set_iceberg_row_lineage_params(&split_options, 1000, 77);
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);
    ASSERT_EQ(block.rows(), 3);

    apply_final_conjuncts(&block, conjuncts);
    ASSERT_EQ(block.rows(), 1);
    expect_nullable_int64_column_values(*block.get_by_position(0).column, {1001});
    expect_nullable_int64_column_values(*block.get_by_position(1).column, {77});
    expect_int32_column_values(*block.get_by_position(2).column, {2});

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, IcebergLastUpdatedSequencePredicateFiltersAfterMaterialization) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_iceberg_sequence_finalize_filter_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_iceberg_row_lineage_parquet_file(file_path, {1, 2, 3}, {7000, std::nullopt, 7002},
                                           {80, std::nullopt, 82});

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(
            2147483540, "_row_id", make_nullable(std::make_shared<DataTypeInt64>())));
    projected_columns.push_back(
            make_table_column(2147483539, "_last_updated_sequence_number",
                              make_nullable(std::make_shared<DataTypeInt64>())));
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    VExprContextSPtrs conjuncts = {prepared_conjunct(
            &state, table_nullable_int64_binary_predicate("eq", TExprOpcode::EQ, 1, 1,
                                                          "_last_updated_sequence_number", 77))};
    doris::format::iceberg::IcebergTableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .conjuncts = conjuncts,
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                            })
                        .ok());

    auto split_options = build_split_options(file_path);
    set_iceberg_row_lineage_params(&split_options, 1000, 77);
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);
    ASSERT_EQ(block.rows(), 3);

    apply_final_conjuncts(&block, conjuncts);
    ASSERT_EQ(block.rows(), 1);
    expect_nullable_int64_column_values(*block.get_by_position(0).column, {1001});
    expect_nullable_int64_column_values(*block.get_by_position(1).column, {77});
    expect_int32_column_values(*block.get_by_position(2).column, {2});

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, IcebergRowidVirtualColumnUsesDataFilePosition) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_iceberg_rowid_virtual_column_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3}, {10, 20, 30}, {"one", "two", "three"});

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(
            make_table_column(-1, BeConsts::ICEBERG_ROWID_COL, make_iceberg_rowid_type()));
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    doris::format::iceberg::IcebergTableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .conjuncts = {prepared_conjunct(
                                            &state, table_int32_greater_than_expr(1, 1, 1))},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                            })
                        .ok());

    auto split_options = build_split_options(file_path);
    const auto original_file_path = "s3://bucket/table/data/original.parquet";
    const auto partition_data_json = R"({"part":"p1"})";
    set_iceberg_rowid_params(&split_options, original_file_path, 17, partition_data_json);
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);

    ASSERT_EQ(block.rows(), 2);
    expect_iceberg_rowid_column_values(*block.get_by_position(0).column, original_file_path, {1, 2},
                                       17, partition_data_json);
    expect_int32_column_values(*block.get_by_position(1).column, {2, 3});

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, IcebergVirtualColumnsKeepRowLineageAfterConjunctFiltering) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_iceberg_virtual_columns_conjunct_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3}, {10, 20, 30}, {"one", "two", "three"});

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_iceberg_row_lineage_row_id_column());
    projected_columns.push_back(make_iceberg_last_updated_sequence_number_column());
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    doris::format::iceberg::IcebergTableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .conjuncts = {prepared_conjunct(
                                            &state, table_int32_greater_than_expr(2, 2, 1))},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                            })
                        .ok());

    auto split_options = build_split_options(file_path);
    set_iceberg_row_lineage_params(&split_options, 3000, 88);
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);

    const auto& id_column = assert_cast<const ColumnInt32&>(expect_not_null_table_column(block, 2));

    ASSERT_EQ(block.rows(), 2);
    EXPECT_EQ(id_column.get_element(0), 2);
    EXPECT_EQ(id_column.get_element(1), 3);
    expect_nullable_int64_column_values(*block.get_by_position(0).column, {3001, 3002});
    expect_nullable_int64_column_values(*block.get_by_position(1).column, {88, 88});

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, IcebergVirtualColumnsKeepRowLineageAfterRowGroupPredicatePruning) {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_iceberg_virtual_columns_row_group_predicate_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    // Keep one row per row group so the VExpr ZoneMap path can prune the first two row groups and
    // leave only the third file-local row.
    write_int_pair_parquet_file(file_path, {1, 2, 3}, {10, 20, 30}, {"one", "two", "three"}, 1);

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_iceberg_row_lineage_row_id_column());
    projected_columns.push_back(make_iceberg_last_updated_sequence_number_column());
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    doris::format::iceberg::IcebergTableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .conjuncts = {prepared_conjunct(
                                            &state, table_int32_greater_than_expr(2, 2, 2))},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                            })
                        .ok());

    auto split_options = build_split_options(file_path);
    set_iceberg_row_lineage_params(&split_options, 4000, 99);
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);

    const auto& id_column = assert_cast<const ColumnInt32&>(expect_not_null_table_column(block, 2));

    ASSERT_EQ(block.rows(), 1);
    EXPECT_EQ(id_column.get_element(0), 3);
    expect_nullable_int64_column_values(*block.get_by_position(0).column, {4002});
    expect_nullable_int64_column_values(*block.get_by_position(1).column, {99});

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, IcebergDeletionVectorUsesTableReaderDeleteFileInterface) {
    TTableFormatFileDesc table_format_desc;
    TIcebergFileDesc iceberg_desc;
    iceberg_desc.__set_format_version(2);
    iceberg_desc.__set_original_file_path("data-a.parquet");
    iceberg_desc.__set_delete_files({make_iceberg_deletion_vector("dv.bin", 8, 128)});
    table_format_desc.__set_iceberg_params(iceberg_desc);

    IcebergTableReaderDeleteFileTestHelper reader;
    DeleteFileDesc desc;
    bool has_delete_file = false;
    ASSERT_TRUE(reader.parse_deletion_vector_file(table_format_desc, &desc, &has_delete_file).ok());

    EXPECT_TRUE(has_delete_file);
    EXPECT_EQ(desc.path, "dv.bin");
    EXPECT_EQ(desc.start_offset, 8);
    EXPECT_EQ(desc.size, 128);
    EXPECT_EQ(desc.file_size, -1);
    EXPECT_EQ(desc.format, DeleteFileDesc::Format::ICEBERG);
}

TEST(IcebergV2ReaderTest, IcebergDeletionVectorCacheKeyIncludesDataFileAndRange) {
    // Scenario: Iceberg Puffin deletion vectors are scoped to the data file. The same Puffin blob
    // referenced by different data files, offsets or lengths must not share a DeleteFileDesc key.
    const auto shared_dv =
            make_iceberg_deletion_vector("s3://bucket/table/shared-dv.puffin", 8, 128);
    IcebergTableReaderDeleteFileTestHelper reader;

    DeleteFileDesc first_desc;
    bool has_delete_file = false;
    ASSERT_TRUE(reader.parse_deletion_vector_file(
                              make_iceberg_table_format_desc("s3://bucket/table/data-a.parquet",
                                                             {shared_dv}),
                              &first_desc, &has_delete_file)
                        .ok());
    EXPECT_TRUE(has_delete_file);

    DeleteFileDesc different_data_file_desc;
    ASSERT_TRUE(reader.parse_deletion_vector_file(
                              make_iceberg_table_format_desc("s3://bucket/table/data-b.parquet",
                                                             {shared_dv}),
                              &different_data_file_desc, &has_delete_file)
                        .ok());

    DeleteFileDesc different_offset_desc;
    ASSERT_TRUE(reader.parse_deletion_vector_file(
                              make_iceberg_table_format_desc(
                                      "s3://bucket/table/data-a.parquet",
                                      {make_iceberg_deletion_vector(
                                              "s3://bucket/table/shared-dv.puffin", 16, 128)}),
                              &different_offset_desc, &has_delete_file)
                        .ok());

    DeleteFileDesc different_length_desc;
    ASSERT_TRUE(reader.parse_deletion_vector_file(
                              make_iceberg_table_format_desc(
                                      "s3://bucket/table/data-a.parquet",
                                      {make_iceberg_deletion_vector(
                                              "s3://bucket/table/shared-dv.puffin", 8, 256)}),
                              &different_length_desc, &has_delete_file)
                        .ok());

    EXPECT_NE(first_desc.key, different_data_file_desc.key);
    EXPECT_NE(first_desc.key, different_offset_desc.key);
    EXPECT_NE(first_desc.key, different_length_desc.key);
}

TEST(IcebergV2ReaderTest, IcebergDeletionVectorRejectsMultipleDeleteFiles) {
    TTableFormatFileDesc table_format_desc;
    TIcebergFileDesc iceberg_desc;
    iceberg_desc.__set_format_version(2);
    iceberg_desc.__set_delete_files({make_iceberg_deletion_vector("dv-a.bin", 8, 128),
                                     make_iceberg_deletion_vector("dv-b.bin", 16, 256)});
    table_format_desc.__set_iceberg_params(iceberg_desc);

    IcebergTableReaderDeleteFileTestHelper reader;
    DeleteFileDesc desc;
    bool has_delete_file = false;
    auto status = reader.parse_deletion_vector_file(table_format_desc, &desc, &has_delete_file);

    EXPECT_FALSE(status.ok());
}

TEST(IcebergV2ReaderTest, IcebergDeletionVectorRejectsMissingRange) {
    TIcebergDeleteFileDesc delete_file;
    delete_file.__set_content(3);
    delete_file.__set_path("dv.bin");

    TTableFormatFileDesc table_format_desc;
    TIcebergFileDesc iceberg_desc;
    iceberg_desc.__set_format_version(2);
    iceberg_desc.__set_delete_files({delete_file});
    table_format_desc.__set_iceberg_params(iceberg_desc);

    IcebergTableReaderDeleteFileTestHelper reader;
    DeleteFileDesc desc;
    bool has_delete_file = false;
    auto status = reader.parse_deletion_vector_file(table_format_desc, &desc, &has_delete_file);

    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.is<ErrorCode::INTERNAL_ERROR>());
    EXPECT_NE(status.to_string().find("missing content offset or length"), std::string::npos);
    EXPECT_FALSE(has_delete_file);
}

TEST(IcebergV2ReaderTest, IcebergTableReaderAppliesDeletionVectorFile) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_iceberg_deletion_vector_file_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    const auto dv_path = (test_dir / "delete-vector.bin").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3, 4, 5}, {10, 20, 30, 40, 50},
                                {"one", "two", "three", "four", "five"});
    const auto dv_size = write_iceberg_deletion_vector_file(dv_path, {0, 4});

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::format::iceberg::IcebergTableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = &scan_params,
                                    .io_ctx = io_ctx,
                                    .runtime_state = &state,
                                    .scanner_profile = &profile,
                                    .push_down_agg_type = TPushAggOp::type::COUNT,
                            })
                        .ok());

    auto split_options = build_split_options(file_path);
    split_options.cache = &cache;
    split_options.current_range.__set_table_format_params(make_iceberg_table_format_desc(
            file_path, {make_iceberg_deletion_vector(dv_path, 0, dv_size)}));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    EXPECT_EQ(read_iceberg_ids(&reader, projected_columns), std::vector<int32_t>({2, 3, 4}));

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, IcebergTableReaderReportsInjectedDeletionVectorReadError) {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_iceberg_v2_deletion_vector_injected_read_error_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    const auto dv_path = (test_dir / "delete-vector.bin").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3}, {10, 20, 30}, {"one", "two", "three"});
    const auto dv_size = write_iceberg_deletion_vector_file(dv_path, {0});

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::format::iceberg::IcebergTableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = &scan_params,
                                    .io_ctx = io_ctx,
                                    .runtime_state = &state,
                                    .scanner_profile = &profile,
                            })
                        .ok());

    auto split_options = build_split_options(file_path);
    split_options.cache = &cache;
    split_options.current_range.__set_table_format_params(make_iceberg_table_format_desc(
            file_path, {make_iceberg_deletion_vector(dv_path, 0, dv_size)}));

    ScopedDebugPoint debug_point("TableReader.parse_deletion_vector.io_error");
    auto status = reader.prepare_split(split_options);

    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("injected format v2 deletion vector read failure"),
              std::string::npos);
    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, IcebergTableReaderStopsDuringDeletionVectorRead) {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_iceberg_v2_deletion_vector_should_stop_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    const auto dv_path = (test_dir / "delete-vector.bin").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3}, {10, 20, 30}, {"one", "two", "three"});
    const auto dv_size = write_iceberg_deletion_vector_file(dv_path, {0});

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::format::iceberg::IcebergTableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = &scan_params,
                                    .io_ctx = io_ctx,
                                    .runtime_state = &state,
                                    .scanner_profile = &profile,
                            })
                        .ok());

    auto split_options = build_split_options(file_path);
    split_options.cache = &cache;
    split_options.current_range.__set_table_format_params(make_iceberg_table_format_desc(
            file_path, {make_iceberg_deletion_vector(dv_path, 0, dv_size)}));

    ScopedDebugPoint debug_point("TableReader.parse_deletion_vector.should_stop");
    auto status = reader.prepare_split(split_options);

    EXPECT_TRUE(status.is<ErrorCode::END_OF_FILE>());
    EXPECT_NE(status.to_string().find("stop read"), std::string::npos);
    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, IcebergTableReaderRejectsCorruptDeletionVectorPayload) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_iceberg_v2_corrupt_dv_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    const auto dv_path = (test_dir / "delete-vector.bin").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3}, {10, 20, 30}, {"one", "two", "three"});
    std::vector<char> corrupted(12, 0);
    BigEndian::Store32(corrupted.data(), 4);
    memcpy(corrupted.data() + 4, "BAD!", 4);
    {
        std::ofstream output(dv_path, std::ios::binary);
        ASSERT_TRUE(output.is_open());
        output.write(corrupted.data(), static_cast<std::streamsize>(corrupted.size()));
        ASSERT_TRUE(output.good());
    }

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::format::iceberg::IcebergTableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = &scan_params,
                                    .io_ctx = io_ctx,
                                    .runtime_state = &state,
                                    .scanner_profile = &profile,
                            })
                        .ok());

    auto split_options = build_split_options(file_path);
    split_options.cache = &cache;
    split_options.current_range.__set_table_format_params(make_iceberg_table_format_desc(
            file_path,
            {make_iceberg_deletion_vector(dv_path, 0, static_cast<int64_t>(corrupted.size()))}));
    auto status = reader.prepare_split(split_options);

    EXPECT_TRUE(status.is<ErrorCode::DATA_QUALITY_ERROR>());
    EXPECT_NE(status.to_string().find("magic number mismatch"), std::string::npos);
    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, IcebergTableReaderDoesNotPushDownAggregateWithDeletes) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_iceberg_aggregate_delete_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    const auto dv_path = (test_dir / "delete-vector.bin").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3}, {10, 20, 30}, {"one", "two", "three"});
    const auto dv_size = write_iceberg_deletion_vector_file(dv_path, {0});

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::format::iceberg::IcebergTableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = &scan_params,
                                    .io_ctx = io_ctx,
                                    .runtime_state = &state,
                                    .scanner_profile = &profile,
                                    .push_down_agg_type = TPushAggOp::type::COUNT,
                            })
                        .ok());

    auto split_options = build_split_options(file_path);
    split_options.cache = &cache;
    split_options.current_range.__set_table_format_params(make_iceberg_table_format_desc(
            file_path, {make_iceberg_deletion_vector(dv_path, 0, dv_size)}));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);
    ASSERT_EQ(block.rows(), 2);
    const auto& id_column = assert_cast<const ColumnInt32&>(expect_not_null_table_column(block, 0));
    EXPECT_EQ(id_column.get_element(0), 2);
    EXPECT_EQ(id_column.get_element(1), 3);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

// Covers TopN lazy materialization on Iceberg schema-evolution tables. The first-phase scan adds a
// synthesized GLOBAL_ROWID column to the file schema. That virtual column must not make Iceberg
// fall back from field-id mapping to name mapping, otherwise renamed columns are read as defaults
// from old files.
TEST(IcebergV2ReaderTest, IcebergMappingModeIgnoresGlobalRowIdVirtualColumn) {
    IcebergTableReaderMappingModeTestHelper reader;
    std::vector<ColumnDefinition> file_schema {
            make_file_column(1, "id", std::make_shared<DataTypeInt32>()),
            make_file_column(2, "name", std::make_shared<DataTypeString>()),
            global_rowid_column_definition(),
    };

    EXPECT_EQ(reader.mapping_mode_for_schema(std::move(file_schema)),
              TableColumnMappingMode::BY_FIELD_ID);
}

// Covers the fallback side of the previous case. Only synthesized columns are ignored; a real data
// column without an Iceberg field id still disables field-id mapping.
TEST(IcebergV2ReaderTest, IcebergMappingModeRequiresFieldIdsForDataColumns) {
    IcebergTableReaderMappingModeTestHelper reader;
    std::vector<ColumnDefinition> file_schema {
            make_file_column(1, "id", std::make_shared<DataTypeInt32>()),
            make_file_column(2, "name", std::make_shared<DataTypeString>()),
            global_rowid_column_definition(),
    };
    file_schema[1].identifier = Field {};

    EXPECT_EQ(reader.mapping_mode_for_schema(std::move(file_schema)),
              TableColumnMappingMode::BY_NAME);
}

TEST(IcebergV2ReaderTest, IcebergTableReaderDoesNotPushDownAggregateWithPositionDelete) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_iceberg_aggregate_position_delete_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    const auto delete_file_path = (test_dir / "position-delete.parquet").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3}, {10, 20, 30}, {"one", "two", "three"});
    write_position_delete_parquet_file(delete_file_path, {file_path}, {1});

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::format::iceberg::IcebergTableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = &scan_params,
                                    .io_ctx = io_ctx,
                                    .runtime_state = &state,
                                    .scanner_profile = &profile,
                                    .push_down_agg_type = TPushAggOp::type::COUNT,
                            })
                        .ok());

    auto split_options = build_split_options(file_path);
    split_options.cache = &cache;
    split_options.current_range.__set_table_format_params(make_iceberg_table_format_desc(
            file_path, {make_iceberg_position_delete_file(delete_file_path)}));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);
    ASSERT_EQ(block.rows(), 2);
    const auto& id_column = assert_cast<const ColumnInt32&>(expect_not_null_table_column(block, 0));
    EXPECT_EQ(id_column.get_element(0), 1);
    EXPECT_EQ(id_column.get_element(1), 3);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, IcebergTableLevelCountUsesAssignedRowCountWithPositionDelete) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_iceberg_table_level_count_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    const auto delete_file_path = (test_dir / "position-delete.parquet").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3}, {10, 20, 30}, {"one", "two", "three"});
    write_position_delete_parquet_file(delete_file_path, {file_path}, {1});

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    TQueryOptions query_options;
    query_options.__set_batch_size(10);
    RuntimeState state {query_options, TQueryGlobals()};
    doris::format::iceberg::IcebergTableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                                    .push_down_agg_type = TPushAggOp::type::COUNT,
                            })
                        .ok());

    auto split_options = build_split_options(file_path);
    split_options.current_range.__set_table_format_params(make_iceberg_table_format_desc(
            file_path, {make_iceberg_position_delete_file(delete_file_path)}));
    set_table_level_row_count(&split_options, 5);
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);
    EXPECT_EQ(block.rows(), 5);

    block = build_table_block(projected_columns);
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    EXPECT_TRUE(eos);
    EXPECT_EQ(block.rows(), 0);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, IcebergPositionDeleteFallsBackToSplitPath) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_iceberg_position_delete_path_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    const auto delete_file_path = (test_dir / "position-delete.parquet").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3}, {10, 20, 30}, {"one", "two", "three"});
    write_position_delete_parquet_file(delete_file_path, {file_path}, {1});

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::format::iceberg::IcebergTableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = &scan_params,
                                    .io_ctx = io_ctx,
                                    .runtime_state = &state,
                                    .scanner_profile = &profile,
                            })
                        .ok());

    auto split_options = build_split_options(file_path);
    split_options.cache = &cache;
    TTableFormatFileDesc table_format_params;
    TIcebergFileDesc iceberg_params;
    iceberg_params.__set_format_version(2);
    iceberg_params.__set_delete_files({make_iceberg_position_delete_file(delete_file_path)});
    table_format_params.__set_iceberg_params(iceberg_params);
    split_options.current_range.__set_table_format_params(table_format_params);
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    EXPECT_EQ(read_iceberg_ids(&reader, projected_columns), std::vector<int32_t>({1, 3}));

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, IcebergTableReaderDoesNotPushDownAggregateWithEqualityDelete) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_iceberg_aggregate_equality_delete_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    const auto delete_file_path = (test_dir / "equality-delete.parquet").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3}, {10, 20, 30}, {"one", "two", "three"});
    write_iceberg_equality_delete_parquet_file(delete_file_path, 0, 2);

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::format::iceberg::IcebergTableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = &scan_params,
                                    .io_ctx = io_ctx,
                                    .runtime_state = &state,
                                    .scanner_profile = &profile,
                                    .push_down_agg_type = TPushAggOp::type::COUNT,
                            })
                        .ok());

    auto split_options = build_split_options(file_path);
    split_options.cache = &cache;
    split_options.current_range.__set_table_format_params(make_iceberg_table_format_desc(
            file_path, {make_iceberg_equality_delete_file(delete_file_path, {0})}));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);
    ASSERT_EQ(block.rows(), 2);
    const auto& id_column = assert_cast<const ColumnInt32&>(expect_not_null_table_column(block, 0));
    EXPECT_EQ(id_column.get_element(0), 1);
    EXPECT_EQ(id_column.get_element(1), 3);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, IcebergEqualityDeleteCastsDataColumnToDeleteKeyType) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_iceberg_equality_delete_cast_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    const auto delete_file_path = (test_dir / "equality-delete.parquet").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3}, {10, 20, 30}, {"one", "two", "three"});
    write_iceberg_equality_delete_bigint_parquet_file(delete_file_path, 0, 2);

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::format::iceberg::IcebergTableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = &scan_params,
                                    .io_ctx = io_ctx,
                                    .runtime_state = &state,
                                    .scanner_profile = &profile,
                            })
                        .ok());

    auto split_options = build_split_options(file_path);
    split_options.cache = &cache;
    split_options.current_range.__set_table_format_params(make_iceberg_table_format_desc(
            file_path, {make_iceberg_equality_delete_file(delete_file_path, {0})}));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    EXPECT_EQ(read_iceberg_ids(&reader, projected_columns), std::vector<int32_t>({1, 3}));

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, IcebergEqualityDeleteMatchesNullForMissingDataColumn) {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_iceberg_equality_delete_missing_column_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    const auto delete_file_path = (test_dir / "equality-delete.parquet").string();
    write_single_int_parquet_file(file_path, "id", {1, 2, 3}, 0);
    write_iceberg_null_equality_delete_parquet_file(delete_file_path, 1, "added_column");

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::format::iceberg::IcebergTableReader reader;
    init_iceberg_reader(&reader, projected_columns, &scan_params, io_ctx, &state, &profile);

    auto split_options = build_split_options(file_path);
    split_options.cache = &cache;
    split_options.current_range.__set_table_format_params(make_iceberg_table_format_desc(
            file_path, {make_iceberg_equality_delete_file(delete_file_path, {1})}));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    // Iceberg treats a field missing from an older data file as NULL. The NULL equality-delete
    // key therefore matches every row in this file.
    EXPECT_TRUE(read_iceberg_ids(&reader, projected_columns).empty());

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, IcebergEqualityDeleteMatchesInitialDefaultForMissingDataColumn) {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_iceberg_equality_delete_missing_default_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    const auto delete_file_path = (test_dir / "equality-delete.parquet").string();
    write_single_int_parquet_file(file_path, "id", {1, 2, 3}, 0);
    write_iceberg_equality_delete_parquet_file(delete_file_path, 1, 7, "added_column");

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    scan_params.__set_current_schema_id(100);
    scan_params.__set_history_schema_info(
            {external_schema(100, {external_schema_field("id", 0),
                                   external_schema_field("added_column", 1, {}, "7")})});
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::format::iceberg::IcebergTableReader reader;
    init_iceberg_reader(&reader, projected_columns, &scan_params, io_ctx, &state, &profile);

    auto split_options = build_split_options(file_path);
    split_options.cache = &cache;
    split_options.current_range.__set_table_format_params(make_iceberg_table_format_desc(
            file_path, {make_iceberg_equality_delete_file(delete_file_path, {1})}));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    // The old file has no physical added_column, but Iceberg defines every row as the initial
    // default 7. The equality delete key 7 therefore removes the entire file.
    EXPECT_TRUE(read_iceberg_ids(&reader, projected_columns).empty());

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, IcebergEqualityDeleteMatchesTimestampInitialDefaultForMissingColumn) {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_iceberg_equality_delete_missing_timestamp_default_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    const auto delete_file_path = (test_dir / "equality-delete.parquet").string();
    write_single_int_parquet_file(file_path, "id", {1, 2, 3}, 0);
    write_iceberg_timestamp_equality_delete_parquet_file(delete_file_path, 1,
                                                         1'704'067'200'123'456L, "added_timestamp");

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));
    auto scan_params = make_local_parquet_scan_params();
    scan_params.__set_current_schema_id(100);
    scan_params.__set_history_schema_info({external_schema(
            100,
            {external_schema_field("id", 0),
             external_schema_field("added_timestamp", 1, {}, "2024-01-01 00:00:00.123456",
                                   external_primitive_type(TPrimitiveType::DATETIMEV2, -1, 6))})});

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::format::iceberg::IcebergTableReader reader;
    init_iceberg_reader(&reader, projected_columns, &scan_params, io_ctx, &state, &profile);

    auto split_options = build_split_options(file_path);
    split_options.cache = &cache;
    split_options.current_range.__set_table_format_params(make_iceberg_table_format_desc(
            file_path, {make_iceberg_equality_delete_file(delete_file_path, {1})}));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());
    EXPECT_TRUE(read_iceberg_ids(&reader, projected_columns).empty());

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, IcebergEqualityDeleteMatchesVarbinaryInitialDefaultForMissingColumn) {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_iceberg_equality_delete_missing_varbinary_default_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    const auto delete_file_path = (test_dir / "equality-delete.parquet").string();
    write_single_int_parquet_file(file_path, "id", {1, 2, 3}, 0);
    const std::string binary_default("\x00\x01\x02\xff", 4);
    write_iceberg_binary_equality_delete_parquet_file(delete_file_path, 1, binary_default,
                                                      "added_binary");

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));
    auto scan_params = make_local_parquet_scan_params();
    scan_params.__set_current_schema_id(100);
    scan_params.__set_history_schema_info({external_schema(
            100,
            {external_schema_field("id", 0),
             external_schema_field("added_binary", 1, {}, "AAEC/w==",
                                   external_primitive_type(TPrimitiveType::VARBINARY, 4), true)})});

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::format::iceberg::IcebergTableReader reader;
    init_iceberg_reader(&reader, projected_columns, &scan_params, io_ctx, &state, &profile);

    auto split_options = build_split_options(file_path);
    split_options.cache = &cache;
    split_options.current_range.__set_table_format_params(make_iceberg_table_format_desc(
            file_path, {make_iceberg_equality_delete_file(delete_file_path, {1})}));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());
    EXPECT_TRUE(read_iceberg_ids(&reader, projected_columns).empty());

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, IcebergEqualityDeleteMatchesStringMappedBinaryInitialDefault) {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_iceberg_equality_delete_missing_binary_string_default_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    const auto delete_file_path = (test_dir / "equality-delete.parquet").string();
    write_single_int_parquet_file(file_path, "id", {1, 2, 3}, 0);
    const std::string binary_default("\x00\x01\x02\xff", 4);
    write_iceberg_binary_equality_delete_parquet_file(delete_file_path, 1, binary_default,
                                                      "added_binary");

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));
    auto scan_params = make_local_parquet_scan_params();
    scan_params.__set_current_schema_id(100);
    scan_params.__set_history_schema_info({external_schema(
            100, {external_schema_field("id", 0),
                  external_schema_field("added_binary", 1, {}, "AAEC/w==",
                                        external_primitive_type(TPrimitiveType::STRING), true)})});

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::format::iceberg::IcebergTableReader reader;
    init_iceberg_reader(&reader, projected_columns, &scan_params, io_ctx, &state, &profile);

    auto split_options = build_split_options(file_path);
    split_options.cache = &cache;
    split_options.current_range.__set_table_format_params(make_iceberg_table_format_desc(
            file_path, {make_iceberg_equality_delete_file(delete_file_path, {1})}));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());
    EXPECT_TRUE(read_iceberg_ids(&reader, projected_columns).empty());

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, IcebergEqualityDeleteDecodesBinaryDefaultMappedToString) {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_iceberg_equality_delete_missing_string_binary_default_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    const auto delete_file_path = (test_dir / "equality-delete.parquet").string();
    write_single_int_parquet_file(file_path, "id", {1, 2, 3}, 0);
    const std::string binary_default("\x00\x01\x02\xff", 4);
    write_iceberg_binary_equality_delete_parquet_file(delete_file_path, 1, binary_default,
                                                      "added_binary");

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));
    auto scan_params = make_local_parquet_scan_params();
    scan_params.__set_current_schema_id(100);
    scan_params.__set_history_schema_info({external_schema(
            100, {external_schema_field("id", 0),
                  external_schema_field("added_binary", 1, {}, "AAEC/w==",
                                        external_primitive_type(TPrimitiveType::STRING), true)})});

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::format::iceberg::IcebergTableReader reader;
    init_iceberg_reader(&reader, projected_columns, &scan_params, io_ctx, &state, &profile);

    auto split_options = build_split_options(file_path);
    split_options.cache = &cache;
    split_options.current_range.__set_table_format_params(make_iceberg_table_format_desc(
            file_path, {make_iceberg_equality_delete_file(delete_file_path, {1})}));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());
    EXPECT_TRUE(read_iceberg_ids(&reader, projected_columns).empty());

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, IcebergEqualityDeletePreservesTimestampTzAcrossDstFold) {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_iceberg_equality_delete_timestamp_tz_dst_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    const auto delete_file_path = (test_dir / "equality-delete.parquet").string();
    // These instants are the two occurrences of 01:30 during the 2024 New York DST fall-back.
    constexpr int64_t FIRST_0130_MICROS = 1'730'611'800'000'000L;
    constexpr int64_t SECOND_0130_MICROS = 1'730'615'400'000'000L;
    write_timestamp_int_parquet_file(file_path, {FIRST_0130_MICROS, SECOND_0130_MICROS}, {1, 2});
    write_iceberg_timestamp_equality_delete_parquet_file(delete_file_path, 0, FIRST_0130_MICROS,
                                                         "event_time", true);

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(1, "id", std::make_shared<DataTypeInt32>()));
    auto scan_params = make_local_parquet_scan_params();
    scan_params.__set_enable_mapping_timestamp_tz(true);
    scan_params.__set_current_schema_id(100);
    scan_params.__set_history_schema_info({external_schema(
            100,
            {external_schema_field("event_time", 0, {}, std::nullopt,
                                   external_primitive_type(TPrimitiveType::TIMESTAMPTZ, -1, 6)),
             external_schema_field("id", 1)})});

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    state.set_timezone("America/New_York");
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::format::iceberg::IcebergTableReader reader;
    init_iceberg_reader(&reader, projected_columns, &scan_params, io_ctx, &state, &profile);

    auto split_options = build_split_options(file_path);
    split_options.cache = &cache;
    split_options.current_range.__set_table_format_params(make_iceberg_table_format_desc(
            file_path, {make_iceberg_equality_delete_file(delete_file_path, {0})}));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());
    EXPECT_EQ(read_iceberg_ids(&reader, projected_columns), std::vector<int32_t>({2}));

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, IcebergEqualityDeleteUsesNameMappingWithoutFileFieldIds) {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_iceberg_equality_delete_name_mapping_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    const auto delete_file_path = (test_dir / "equality-delete.parquet").string();
    write_single_int_parquet_file(file_path, "legacy_id", {1, 2, 3}, std::nullopt);
    write_iceberg_equality_delete_parquet_file(delete_file_path, 0, 2, "current_id");

    std::vector<ColumnDefinition> projected_columns;
    auto id_column = make_table_column(0, "current_id", std::make_shared<DataTypeInt32>());
    id_column.name_mapping.push_back("legacy_id");
    projected_columns.push_back(std::move(id_column));

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::format::iceberg::IcebergTableReader reader;
    init_iceberg_reader(&reader, projected_columns, &scan_params, io_ctx, &state, &profile);

    auto split_options = build_split_options(file_path);
    split_options.cache = &cache;
    split_options.current_range.__set_table_format_params(make_iceberg_table_format_desc(
            file_path, {make_iceberg_equality_delete_file(delete_file_path, {0})}));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    EXPECT_EQ(read_iceberg_ids(&reader, projected_columns), std::vector<int32_t>({1, 3}));

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, IcebergEqualityDeleteByNameIgnoresStaleFileFieldId) {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_iceberg_equality_delete_stale_field_id_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    const auto delete_file_path = (test_dir / "equality-delete.parquet").string();
    // The real key has no id and is mapped by its historical name. A different physical column
    // carries the stale id 0, forcing the entire split into BY_NAME mode.
    write_two_int_parquet_file(file_path, "legacy_id", {1, 2, 3}, std::nullopt, "stale_key",
                               {100, 200, 300}, 0);
    write_iceberg_equality_delete_parquet_file(delete_file_path, 0, 2, "current_id");

    std::vector<ColumnDefinition> projected_columns;
    auto id_column = make_table_column(0, "current_id", std::make_shared<DataTypeInt32>());
    id_column.name_mapping.push_back("legacy_id");
    projected_columns.push_back(std::move(id_column));

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::format::iceberg::IcebergTableReader reader;
    init_iceberg_reader(&reader, projected_columns, &scan_params, io_ctx, &state, &profile);

    auto split_options = build_split_options(file_path);
    split_options.cache = &cache;
    split_options.current_range.__set_table_format_params(make_iceberg_table_format_desc(
            file_path, {make_iceberg_equality_delete_file(delete_file_path, {0})}));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());
    EXPECT_EQ(read_iceberg_ids(&reader, projected_columns), std::vector<int32_t>({1, 3}));

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, IcebergEqualityDeleteUsesUnprojectedTableSchemaNameMapping) {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_iceberg_equality_delete_hidden_name_mapping_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    const auto delete_file_path = (test_dir / "equality-delete.parquet").string();
    write_two_int_parquet_file(file_path, "LEGACY_ID", {1, 2, 3}, std::nullopt, "data",
                               {10, 20, 30}, std::nullopt);
    write_iceberg_equality_delete_parquet_file(delete_file_path, 0, 2, "current_id");

    // Model SELECT data: the equality key is deliberately absent from the query projection.
    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(1, "data", std::make_shared<DataTypeInt32>()));

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    scan_params.__set_current_schema_id(100);
    scan_params.__set_history_schema_info(
            {external_schema(100, {external_schema_field("current_id", 0, {"legacy_id"}),
                                   external_schema_field("data", 1)})});
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::format::iceberg::IcebergTableReader reader;
    init_iceberg_reader(&reader, projected_columns, &scan_params, io_ctx, &state, &profile);

    auto split_options = build_split_options(file_path);
    split_options.cache = &cache;
    split_options.current_range.__set_table_format_params(make_iceberg_table_format_desc(
            file_path, {make_iceberg_equality_delete_file(delete_file_path, {0})}));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    EXPECT_EQ(read_iceberg_ids(&reader, projected_columns), std::vector<int32_t>({10, 30}));

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, IcebergEqualityDeleteFileIsReusedAcrossSplits) {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_iceberg_equality_delete_split_cache_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto first_file_path = (test_dir / "first.parquet").string();
    const auto second_file_path = (test_dir / "second.parquet").string();
    const auto delete_file_path = (test_dir / "equality-delete.parquet").string();
    write_single_int_parquet_file(first_file_path, "id", {1, 2, 3}, 0);
    write_single_int_parquet_file(second_file_path, "id", {1, 2, 3}, 0);
    write_iceberg_equality_delete_parquet_file(delete_file_path, 0, 2);

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::format::iceberg::IcebergTableReader reader;
    init_iceberg_reader(&reader, projected_columns, &scan_params, io_ctx, &state, &profile);

    auto first_split = build_split_options(first_file_path);
    first_split.cache = &cache;
    first_split.current_range.__set_table_format_params(make_iceberg_table_format_desc(
            first_file_path, {make_iceberg_equality_delete_file(delete_file_path, {0})}));
    ASSERT_TRUE(reader.prepare_split(first_split).ok());
    EXPECT_EQ(read_iceberg_ids(&reader, projected_columns), std::vector<int32_t>({1, 3}));

    // Removing the source after the first split proves that the second split consumes the parsed
    // delete block from SplitReadOptions.cache instead of reopening the delete file.
    ASSERT_TRUE(std::filesystem::remove(delete_file_path));
    auto second_split = build_split_options(second_file_path);
    second_split.cache = &cache;
    second_split.current_range.__set_table_format_params(make_iceberg_table_format_desc(
            second_file_path, {make_iceberg_equality_delete_file(delete_file_path, {0})}));
    ASSERT_TRUE(reader.prepare_split(second_split).ok());
    EXPECT_EQ(read_iceberg_ids(&reader, projected_columns), std::vector<int32_t>({1, 3}));

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, IcebergEqualityDeleteCacheIsScopedByFileSystem) {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_iceberg_equality_delete_filesystem_cache_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto first_file_path = (test_dir / "first.parquet").string();
    const auto second_file_path = (test_dir / "second.parquet").string();
    const auto delete_file_path = (test_dir / "equality-delete.parquet").string();
    write_single_int_parquet_file(first_file_path, "id", {1, 2, 3}, 0);
    write_single_int_parquet_file(second_file_path, "id", {1, 2, 3}, 0);
    write_iceberg_equality_delete_parquet_file(delete_file_path, 0, 2);

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));
    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::format::iceberg::IcebergTableReader reader;
    init_iceberg_reader(&reader, projected_columns, &scan_params, io_ctx, &state, &profile);

    auto first_split = build_split_options(first_file_path);
    first_split.cache = &cache;
    first_split.current_range.__set_fs_name("filesystem-a");
    first_split.current_range.__set_table_format_params(make_iceberg_table_format_desc(
            first_file_path, {make_iceberg_equality_delete_file(delete_file_path, {0})}));
    ASSERT_TRUE(reader.prepare_split(first_split).ok());
    EXPECT_EQ(read_iceberg_ids(&reader, projected_columns), std::vector<int32_t>({1, 3}));

    // The same descriptor path resolves to different content in another filesystem namespace.
    write_iceberg_equality_delete_parquet_file(delete_file_path, 0, 3);
    auto second_split = build_split_options(second_file_path);
    second_split.cache = &cache;
    second_split.current_range.__set_fs_name("filesystem-b");
    second_split.current_range.__set_table_format_params(make_iceberg_table_format_desc(
            second_file_path, {make_iceberg_equality_delete_file(delete_file_path, {0})}));
    ASSERT_TRUE(reader.prepare_split(second_split).ok());
    EXPECT_EQ(read_iceberg_ids(&reader, projected_columns), std::vector<int32_t>({1, 2}));

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, IcebergPositionDeleteOnlyMatchesOriginalDataFilePath) {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_iceberg_position_delete_path_match_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    const auto other_file_path = (test_dir / "other.parquet").string();
    const auto delete_file_path = (test_dir / "position-delete.parquet").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3}, {10, 20, 30}, {"one", "two", "three"});
    write_position_delete_parquet_file(delete_file_path, {other_file_path, file_path}, {0, 1});

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::format::iceberg::IcebergTableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = &scan_params,
                                    .io_ctx = io_ctx,
                                    .runtime_state = &state,
                                    .scanner_profile = &profile,
                            })
                        .ok());

    auto split_options = build_split_options(file_path);
    split_options.cache = &cache;
    split_options.current_range.__set_table_format_params(make_iceberg_table_format_desc(
            file_path, {make_iceberg_position_delete_file(delete_file_path)}));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    EXPECT_EQ(read_iceberg_ids(&reader, projected_columns), std::vector<int32_t>({1, 3}));

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, IcebergRowLineageRemainsFileLocalAfterDeleteFiltering) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_iceberg_row_lineage_delete_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    const auto delete_file_path = (test_dir / "position-delete.parquet").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3}, {10, 20, 30}, {"one", "two", "three"});
    write_position_delete_parquet_file(delete_file_path, {file_path}, {1});

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_iceberg_row_lineage_row_id_column());
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::format::iceberg::IcebergTableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = &scan_params,
                                    .io_ctx = io_ctx,
                                    .runtime_state = &state,
                                    .scanner_profile = &profile,
                            })
                        .ok());

    auto split_options = build_split_options(file_path);
    split_options.cache = &cache;
    TTableFormatFileDesc table_format_params = make_iceberg_table_format_desc(
            file_path, {make_iceberg_position_delete_file(delete_file_path)});
    table_format_params.iceberg_params.__set_first_row_id(1000);
    split_options.current_range.__set_table_format_params(table_format_params);
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);
    ASSERT_EQ(block.rows(), 2);
    expect_nullable_int64_column_values(*block.get_by_position(0).column, {1000, 1002});
    const auto& id_column = assert_cast<const ColumnInt32&>(expect_not_null_table_column(block, 1));
    EXPECT_EQ(id_column.get_element(0), 1);
    EXPECT_EQ(id_column.get_element(1), 3);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, IcebergTableReaderAppliesPositionDeleteFile) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_iceberg_position_delete_file_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    const auto delete_file_path = (test_dir / "position-delete.parquet").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3, 4, 5}, {10, 20, 30, 40, 50},
                                {"one", "two", "three", "four", "five"});
    write_position_delete_parquet_file(delete_file_path, {file_path, file_path}, {1, 3});

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::format::iceberg::IcebergTableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = &scan_params,
                                    .io_ctx = io_ctx,
                                    .runtime_state = &state,
                                    .scanner_profile = &profile,
                            })
                        .ok());

    auto split_options = build_split_options(file_path);
    split_options.cache = &cache;
    split_options.current_range.__set_table_format_params(make_iceberg_table_format_desc(
            file_path, {make_iceberg_position_delete_file(delete_file_path)}));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    EXPECT_EQ(read_iceberg_ids(&reader, projected_columns), std::vector<int32_t>({1, 3, 5}));

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, IcebergPositionDeleteHonorsPositionBounds) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_iceberg_position_delete_bounds_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    const auto delete_file_path = (test_dir / "position-delete.parquet").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3, 4, 5}, {10, 20, 30, 40, 50},
                                {"one", "two", "three", "four", "five"});
    write_position_delete_parquet_file(delete_file_path, {file_path, file_path, file_path},
                                       {0, 2, 4});

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::format::iceberg::IcebergTableReader reader;
    init_iceberg_reader(&reader, projected_columns, &scan_params, io_ctx, &state, &profile);

    auto delete_file = make_iceberg_position_delete_file(delete_file_path);
    delete_file.__set_position_lower_bound(1);
    delete_file.__set_position_upper_bound(3);
    auto split_options = build_split_options(file_path);
    split_options.cache = &cache;
    split_options.current_range.__set_table_format_params(
            make_iceberg_table_format_desc(file_path, {delete_file}));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    EXPECT_EQ(read_iceberg_ids(&reader, projected_columns), std::vector<int32_t>({1, 2, 4, 5}));

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, IcebergPositionDeleteFileIsReusedAcrossSplits) {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_iceberg_position_delete_split_cache_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto first_file_path = (test_dir / "first.parquet").string();
    const auto second_file_path = (test_dir / "second.parquet").string();
    const auto delete_file_path = (test_dir / "position-delete.parquet").string();
    write_single_int_parquet_file(first_file_path, "id", {1, 2, 3}, 0);
    write_single_int_parquet_file(second_file_path, "id", {1, 2, 3}, 0);
    write_position_delete_parquet_file(delete_file_path, {first_file_path, second_file_path},
                                       {1, 0});

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::format::iceberg::IcebergTableReader reader;
    init_iceberg_reader(&reader, projected_columns, &scan_params, io_ctx, &state, &profile);

    auto first_split = build_split_options(first_file_path);
    first_split.cache = &cache;
    first_split.current_range.__set_table_format_params(make_iceberg_table_format_desc(
            first_file_path, {make_iceberg_position_delete_file(delete_file_path)}));
    ASSERT_TRUE(reader.prepare_split(first_split).ok());
    EXPECT_EQ(read_iceberg_ids(&reader, projected_columns), std::vector<int32_t>({1, 3}));

    // The cached delete file contains entries for every referenced data file, so another split can
    // select its own positions without rescanning the shared delete file.
    ASSERT_TRUE(std::filesystem::remove(delete_file_path));
    auto second_split = build_split_options(second_file_path);
    second_split.cache = &cache;
    second_split.current_range.__set_table_format_params(make_iceberg_table_format_desc(
            second_file_path, {make_iceberg_position_delete_file(delete_file_path)}));
    ASSERT_TRUE(reader.prepare_split(second_split).ok());
    EXPECT_EQ(read_iceberg_ids(&reader, projected_columns), std::vector<int32_t>({2, 3}));

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, IcebergPositionDeleteCacheIsScopedByFileSystem) {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_iceberg_position_delete_filesystem_cache_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto first_file_path = (test_dir / "first.parquet").string();
    const auto second_file_path = (test_dir / "second.parquet").string();
    const auto delete_file_path = (test_dir / "position-delete.parquet").string();
    write_single_int_parquet_file(first_file_path, "id", {1, 2, 3}, 0);
    write_single_int_parquet_file(second_file_path, "id", {1, 2, 3}, 0);
    write_position_delete_parquet_file(delete_file_path, {first_file_path}, {1});

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));
    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::format::iceberg::IcebergTableReader reader;
    init_iceberg_reader(&reader, projected_columns, &scan_params, io_ctx, &state, &profile);

    auto first_split = build_split_options(first_file_path);
    first_split.cache = &cache;
    first_split.current_range.__set_fs_name("filesystem-a");
    first_split.current_range.__set_table_format_params(make_iceberg_table_format_desc(
            first_file_path, {make_iceberg_position_delete_file(delete_file_path)}));
    ASSERT_TRUE(reader.prepare_split(first_split).ok());
    EXPECT_EQ(read_iceberg_ids(&reader, projected_columns), std::vector<int32_t>({1, 3}));

    write_position_delete_parquet_file(delete_file_path, {second_file_path}, {0});
    auto second_split = build_split_options(second_file_path);
    second_split.cache = &cache;
    second_split.current_range.__set_fs_name("filesystem-b");
    second_split.current_range.__set_table_format_params(make_iceberg_table_format_desc(
            second_file_path, {make_iceberg_position_delete_file(delete_file_path)}));
    ASSERT_TRUE(reader.prepare_split(second_split).ok());
    EXPECT_EQ(read_iceberg_ids(&reader, projected_columns), std::vector<int32_t>({2, 3}));

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, IcebergDeleteReaderInheritsFileMetaMemoryCachePolicy) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_iceberg_delete_file_meta_cache_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    const auto delete_file_path = (test_dir / "position-delete.parquet").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3}, {10, 20, 30}, {"one", "two", "three"});
    write_position_delete_parquet_file(delete_file_path, {file_path}, {1});

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));
    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache delete_file_cache(1);

    auto delete_footer_is_cached = [&](bool enable_file_meta_memory_cache) {
        FileMetaCache file_meta_cache(1024);
        doris::format::iceberg::IcebergTableReader reader;
        EXPECT_TRUE(
                reader.init({
                                    .projected_columns = projected_columns,
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = &scan_params,
                                    .io_ctx = io_ctx,
                                    .runtime_state = &state,
                                    .scanner_profile = &profile,
                                    .file_meta_cache = &file_meta_cache,
                                    .enable_file_meta_memory_cache = enable_file_meta_memory_cache,
                            })
                        .ok());

        auto split_options = build_split_options(file_path);
        split_options.cache = &delete_file_cache;
        split_options.current_range.__set_table_format_params(make_iceberg_table_format_desc(
                file_path, {make_iceberg_position_delete_file(delete_file_path)}));
        EXPECT_TRUE(reader.prepare_split(split_options).ok());

        io::FileReaderSPtr delete_file_reader;
        EXPECT_TRUE(io::global_local_filesystem()
                            ->open_file(delete_file_path, &delete_file_reader)
                            .ok());
        io::FileDescription file_description;
        file_description.mtime = 0;
        file_description.file_size = -1;
        const std::string meta_key = FileMetaCache::get_key(delete_file_reader, file_description);
        const std::string memory_key =
                FileMetaCache::get_memory_cache_key(FileMetaCacheFormat::PARQUET_V2, meta_key);
        ObjLRUCache::CacheHandle handle;
        const bool cached = file_meta_cache.lookup(memory_key, &handle);
        EXPECT_TRUE(reader.close().ok());
        return cached;
    };

    EXPECT_TRUE(delete_footer_is_cached(true));
    EXPECT_FALSE(delete_footer_is_cached(false));
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, IcebergTableReaderAppliesNullablePositionDeleteFileWithoutNulls) {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_iceberg_nullable_position_delete_file_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    const auto delete_file_path = (test_dir / "position-delete.parquet").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3, 4, 5}, {10, 20, 30, 40, 50},
                                {"one", "two", "three", "four", "five"});
    write_nullable_position_delete_parquet_file(delete_file_path, {file_path, file_path},
                                                {int64_t {1}, int64_t {3}});

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::format::iceberg::IcebergTableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = &scan_params,
                                    .io_ctx = io_ctx,
                                    .runtime_state = &state,
                                    .scanner_profile = &profile,
                            })
                        .ok());

    auto split_options = build_split_options(file_path);
    split_options.cache = &cache;
    split_options.current_range.__set_table_format_params(make_iceberg_table_format_desc(
            file_path, {make_iceberg_position_delete_file(delete_file_path)}));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    EXPECT_EQ(read_iceberg_ids(&reader, projected_columns), std::vector<int32_t>({1, 3, 5}));

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, IcebergTableReaderRejectsNullablePositionDeleteFileWithActualNulls) {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_iceberg_nullable_position_delete_actual_null_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    const auto delete_file_path = (test_dir / "position-delete.parquet").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3}, {10, 20, 30}, {"one", "two", "three"});
    write_nullable_position_delete_parquet_file(delete_file_path, {file_path, std::nullopt},
                                                {int64_t {1}, int64_t {2}});

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::format::iceberg::IcebergTableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = &scan_params,
                                    .io_ctx = io_ctx,
                                    .runtime_state = &state,
                                    .scanner_profile = &profile,
                            })
                        .ok());

    auto split_options = build_split_options(file_path);
    split_options.cache = &cache;
    split_options.current_range.__set_table_format_params(make_iceberg_table_format_desc(
            file_path, {make_iceberg_position_delete_file(delete_file_path)}));
    auto status = reader.prepare_split(split_options);
    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("file_path contains null values"), std::string::npos)
            << status.to_string();

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, IcebergTableReaderRejectsNullablePositionDeletePosWithActualNulls) {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_iceberg_nullable_position_delete_pos_null_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    const auto delete_file_path = (test_dir / "position-delete.parquet").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3}, {10, 20, 30}, {"one", "two", "three"});
    write_nullable_position_delete_parquet_file(delete_file_path, {file_path, file_path},
                                                {int64_t {1}, std::nullopt});

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::format::iceberg::IcebergTableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = &scan_params,
                                    .io_ctx = io_ctx,
                                    .runtime_state = &state,
                                    .scanner_profile = &profile,
                            })
                        .ok());

    auto split_options = build_split_options(file_path);
    split_options.cache = &cache;
    split_options.current_range.__set_table_format_params(make_iceberg_table_format_desc(
            file_path, {make_iceberg_position_delete_file(delete_file_path)}));
    auto status = reader.prepare_split(split_options);
    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("pos contains null values"), std::string::npos)
            << status.to_string();

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, IcebergTableReaderIgnoresPositionDeleteFilesWhenDeletionVectorPresent) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_iceberg_delete_files_merge_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    const auto dv_path = (test_dir / "delete-vector.bin").string();
    const auto position_delete_path = (test_dir / "position-delete.parquet").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3, 4, 5}, {10, 20, 30, 40, 50},
                                {"one", "two", "three", "four", "five"});
    const auto dv_size = write_iceberg_deletion_vector_file(dv_path, {0});
    write_position_delete_parquet_file(position_delete_path, {file_path, file_path}, {3, 3});

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::format::iceberg::IcebergTableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = &scan_params,
                                    .io_ctx = io_ctx,
                                    .runtime_state = &state,
                                    .scanner_profile = &profile,
                            })
                        .ok());

    auto split_options = build_split_options(file_path);
    split_options.cache = &cache;
    split_options.current_range.__set_table_format_params(make_iceberg_table_format_desc(
            file_path, {make_iceberg_deletion_vector(dv_path, 0, dv_size),
                        make_iceberg_position_delete_file(position_delete_path)}));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    EXPECT_EQ(read_iceberg_ids(&reader, projected_columns), std::vector<int32_t>({2, 3, 4, 5}));

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, RowPositionDeletePredicateColumnIsNotRepeatedAsOutputColumn) {
    const auto row_position_column_id = ROW_POSITION_COLUMN_ID;
    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_iceberg_row_lineage_row_id_column());
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    IcebergTableReaderScanRequestTestHelper reader;
    ASSERT_TRUE(reader.init_for_scan_request_test(projected_columns).ok());

    FileScanRequest request;
    request.non_predicate_columns.push_back(field_projection(0));
    request.local_positions.emplace(LocalColumnId(0), LocalIndex(0));

    ASSERT_TRUE(reader.customize_request(&request).ok());

    EXPECT_EQ(projection_ids(request.predicate_columns),
              std::vector<int32_t>({row_position_column_id}));
    EXPECT_EQ(projection_ids(request.non_predicate_columns), std::vector<int32_t>({0}));
    ASSERT_TRUE(request.local_positions.contains(LocalColumnId(row_position_column_id)));
    EXPECT_EQ(request.local_positions.at(LocalColumnId(row_position_column_id)).value(), 1);
    ASSERT_TRUE(request.conjuncts.empty());
    ASSERT_EQ(request.delete_conjuncts.size(), 1);
    EXPECT_NE(request.delete_conjuncts[0], nullptr);
}

TEST(IcebergV2ReaderTest, DataFileIsMarkedImmutableForPageCache) {
    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    IcebergTableReaderScanRequestTestHelper reader;
    ASSERT_TRUE(reader.init_for_scan_request_test(std::move(projected_columns)).ok());

    EXPECT_TRUE(reader.current_data_file_is_immutable());
}

} // namespace
} // namespace doris::format
