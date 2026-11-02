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
#include <parquet/api/writer.h>
#include <parquet/arrow/writer.h>

#include <algorithm>
#include <array>
#include <bit>
#include <chrono>
#include <cmath>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <thread>
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
#include "core/data_type/data_type_timestamp_ns.h"
#include "core/data_type/data_type_timestamptz.h"
#include "core/data_type/data_type_uuid.h"
#include "core/data_type/data_type_varbinary.h"
#include "core/data_type/data_type_variant_v2.h"
#include "core/value/uuid_value.h"
#include "exec/common/endian.h"
#include "exec/scan/access_path_parser.h"
#include "exprs/runtime_filter_expr.h"
#include "exprs/vectorized_fn_call.h"
#include "exprs/vexpr.h"
#include "exprs/vliteral.h"
#include "exprs/vslot_ref.h"
#include "exprs/vtopn_pred.h"
#include "format/format_common.h"
#include "format/orc/orc_memory_stream_test.h"
#include "format/table/deletion_vector_reader.h"
#include "format_v2/parquet/parquet_reader.h"
#include "format_v2/table_reader.h"
#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/ExternalTableSchema_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "io/io_common.h"
#include "roaring/roaring64map.hh"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "storage/segment/condition_cache.h"
#include "testutil/mock/mock_query_context.h"
#include "util/debug_points.h"
#include "util/defer_op.h"
#include "util/hash_util.hpp"
#include "util/timezone_utils.h"
#include "util/url_coding.h"

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
    TableColumnMappingMode mapping_mode_for_schema(
            std::vector<ColumnDefinition> file_schema, TFileScanRangeParams* scan_params = nullptr,
            std::vector<ColumnDefinition> projected_columns = {}) {
        _scan_params = scan_params;
        _data_reader.file_schema = std::move(file_schema);
        _projected_columns = std::move(projected_columns);
        return mapping_mode();
    }
};

class IcebergTableReaderAnnotateTestHelper final
        : public doris::format::iceberg::IcebergTableReader {
public:
    Status annotate(std::vector<ColumnDefinition>* file_schema, TFileScanRangeParams* scan_params,
                    FileFormat format = FileFormat::PARQUET) {
        _scan_params = scan_params;
        _format = format;
        return annotate_file_schema(file_schema);
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
        std::optional<TColumnType> type = std::nullopt, bool initial_default_is_base64 = false,
        std::optional<bool> is_optional = std::nullopt) {
    auto field = std::make_shared<schema::external::TField>();
    field->__set_name(name);
    field->__set_id(id);
    if (!aliases.empty()) {
        field->__set_name_mapping(aliases);
    }
    if (initial_default.has_value()) {
        field->__set_initial_default_value(*initial_default);
    }
    if (initial_default_is_base64) {
        field->__set_initial_default_value_is_base64(true);
    }
    if (type.has_value()) {
        field->__set_type(*type);
    }
    if (is_optional.has_value()) {
        field->__set_is_optional(*is_optional);
    }
    schema::external::TFieldPtr field_ptr;
    field_ptr.field_ptr = std::move(field);
    field_ptr.__isset.field_ptr = true;
    return field_ptr;
}

schema::external::TFieldPtr external_struct_schema_field(
        std::string name, int32_t id, std::vector<schema::external::TFieldPtr> fields,
        std::optional<bool> is_optional = std::nullopt,
        std::optional<std::string> initial_default = std::nullopt) {
    auto field = external_schema_field(std::move(name), id, {}, std::move(initial_default),
                                       std::nullopt, false, is_optional);
    schema::external::TStructField struct_field;
    struct_field.__set_fields(std::move(fields));
    field.field_ptr->nestedField.__set_struct_field(std::move(struct_field));
    field.field_ptr->__isset.nestedField = true;
    return field;
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

// Use the external formats' UUID representations: Parquet's logical annotation and ORC's
// iceberg.binary-type attribute. Both carry the same 16 bytes, including embedded zero bytes.
void write_iceberg_uuid_file(const std::string& file_path, FileFormat format,
                             const std::vector<std::optional<std::string>>& values,
                             bool include_id) {
    if (format == FileFormat::PARQUET) {
        ::parquet::schema::NodeVector fields;
        if (include_id) {
            fields.push_back(::parquet::schema::PrimitiveNode::Make(
                    "id", ::parquet::Repetition::REQUIRED, ::parquet::LogicalType::None(),
                    ::parquet::Type::INT32, -1, 0));
        }
        fields.push_back(::parquet::schema::PrimitiveNode::Make(
                "u", ::parquet::Repetition::OPTIONAL, ::parquet::LogicalType::UUID(),
                ::parquet::Type::FIXED_LEN_BYTE_ARRAY, 16, 1));
        auto schema = std::static_pointer_cast<::parquet::schema::GroupNode>(
                ::parquet::schema::GroupNode::Make("schema", ::parquet::Repetition::REQUIRED,
                                                   fields));
        auto out = arrow::io::FileOutputStream::Open(file_path).ValueOrDie();
        auto writer = ::parquet::ParquetFileWriter::Open(out, schema);
        auto* group = writer->AppendRowGroup();
        if (include_id) {
            auto* ids = static_cast<::parquet::Int32Writer*>(group->NextColumn());
            for (int32_t i = 0; i < static_cast<int32_t>(values.size()); ++i) {
                ids->WriteBatch(1, nullptr, nullptr, &i);
            }
            ids->Close();
        }
        auto* uuids = static_cast<::parquet::FixedLenByteArrayWriter*>(group->NextColumn());
        for (const auto& value : values) {
            const int16_t level = value.has_value() ? 1 : 0;
            ::parquet::FixedLenByteArray bytes;
            if (value.has_value()) {
                ASSERT_EQ(value->size(), 16);
                bytes.ptr = reinterpret_cast<const uint8_t*>(value->data());
            }
            uuids->WriteBatch(1, &level, nullptr, &bytes);
        }
        uuids->Close();
        group->Close();
        writer->Close();
        ASSERT_TRUE(out->Close().ok());
        return;
    }
    auto type = std::unique_ptr<::orc::Type>(::orc::Type::buildTypeFromString(
            include_id ? "struct<id:int,u:binary>" : "struct<u:binary>"));
    const size_t uuid_idx = include_id ? 1 : 0;
    if (include_id) {
        type->getSubtype(0)->setAttribute("iceberg.id", "0");
    }
    type->getSubtype(uuid_idx)->setAttribute("iceberg.id", "1");
    type->getSubtype(uuid_idx)->setAttribute("iceberg.binary-type", "UUID");
    MemoryOutputStream stream(1024 * 1024);
    ::orc::WriterOptions options;
    auto writer = ::orc::createWriter(*type, &stream, options);
    auto batch = writer->createRowBatch(values.size());
    auto& root = dynamic_cast<::orc::StructVectorBatch&>(*batch);
    root.numElements = values.size();
    if (include_id) {
        auto& ids = dynamic_cast<::orc::LongVectorBatch&>(*root.fields[0]);
        ids.numElements = values.size();
        for (size_t i = 0; i < values.size(); ++i) {
            ids.data[i] = i;
        }
    }
    auto& uuids = dynamic_cast<::orc::StringVectorBatch&>(*root.fields[uuid_idx]);
    uuids.numElements = values.size();
    uuids.hasNulls = true;
    for (size_t i = 0; i < values.size(); ++i) {
        uuids.notNull[i] = values[i].has_value();
        if (values[i].has_value()) {
            ASSERT_EQ(values[i]->size(), 16);
            uuids.data[i] = const_cast<char*>(values[i]->data());
            uuids.length[i] = values[i]->size();
        }
    }
    writer->add(*batch);
    writer->close();
    std::ofstream out(file_path, std::ios::binary);
    out.write(stream.getData(), static_cast<std::streamsize>(stream.getLength()));
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

void write_unsupported_time_first_int_parquet_file(const std::string& file_path,
                                                   const std::vector<int32_t>& ids) {
    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

    // Arrow writes time32 as isAdjustedToUTC=false, which Doris supports. Use Parquet's low-level
    // writer so the first physical column is the deliberately unsupported adjusted TIME_MILLIS
    // shape from the review report. The query will project only the following supported `id`.
    const auto unsupported_time = ::parquet::schema::PrimitiveNode::Make(
            "unsupported_time", ::parquet::Repetition::REQUIRED,
            ::parquet::LogicalType::Time(true, ::parquet::LogicalType::TimeUnit::MILLIS),
            ::parquet::Type::INT32);
    const auto id = ::parquet::schema::PrimitiveNode::Make("id", ::parquet::Repetition::REQUIRED,
                                                           ::parquet::Type::INT32);
    const auto schema_node = ::parquet::schema::GroupNode::Make(
            "schema", ::parquet::Repetition::REQUIRED, {unsupported_time, id});
    const auto schema = std::static_pointer_cast<::parquet::schema::GroupNode>(schema_node);

    auto writer = ::parquet::ParquetFileWriter::Open(out, schema);
    auto* row_group = writer->AppendRowGroup();
    auto* time_writer = static_cast<::parquet::Int32Writer*>(row_group->NextColumn());
    std::vector<int32_t> times(ids.size(), 1000);
    const auto row_count = cast_set<int64_t>(ids.size());
    EXPECT_EQ(time_writer->WriteBatch(row_count, nullptr, nullptr, times.data()), row_count);
    time_writer->Close();
    auto* id_writer = static_cast<::parquet::Int32Writer*>(row_group->NextColumn());
    EXPECT_EQ(id_writer->WriteBatch(row_count, nullptr, nullptr, ids.data()), row_count);
    id_writer->Close();
    row_group->Close();
    writer->Close();
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

// Parquet files written by non-Iceberg writers carry no `field_id` at all. Iceberg reads them by
// file ordinal through ParquetSchemaUtil.addFallbackIds/pruneColumnsFallback whenever the table has
// no `schema.name-mapping.default`; these helpers produce exactly that physical layout.
void write_idless_int_columns_parquet_file(const std::string& file_path,
                                           const std::vector<std::string>& names,
                                           const std::vector<std::vector<int32_t>>& column_values) {
    ASSERT_EQ(names.size(), column_values.size());
    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    for (size_t idx = 0; idx < names.size(); ++idx) {
        fields.push_back(arrow::field(names[idx], arrow::int32(), false));
        arrays.push_back(build_int32_array(column_values[idx]));
    }
    auto table = arrow::Table::Make(arrow::schema(fields), arrays);

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;
    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    builder.compression(::parquet::Compression::UNCOMPRESSED);
    const auto rows = column_values.empty() ? static_cast<int64_t>(0)
                                            : static_cast<int64_t>(column_values[0].size());
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out,
                                                      rows, builder.build()));
}

// A fully id-less nested file: `s STRUCT<inner INT>` with no `field_id` at any level.
void write_idless_struct_parquet_file(const std::string& file_path,
                                      const std::vector<int32_t>& values) {
    auto inner_field = arrow::field("inner", arrow::int32(), false);
    auto struct_result = arrow::StructArray::Make({build_int32_array(values)}, {inner_field});
    ASSERT_TRUE(struct_result.ok()) << struct_result.status();
    auto struct_field = arrow::field("s", arrow::struct_({inner_field}), false);
    auto table = arrow::Table::Make(arrow::schema({struct_field}), {*struct_result});

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

void write_two_int_orc_file(const std::string& file_path, const std::string& first_name,
                            const std::vector<int64_t>& first_values,
                            std::optional<int32_t> first_field_id, const std::string& second_name,
                            const std::vector<int64_t>& second_values,
                            std::optional<int32_t> second_field_id) {
    DORIS_CHECK(first_values.size() == second_values.size());
    auto type = std::unique_ptr<::orc::Type>(::orc::Type::buildTypeFromString(
            "struct<" + first_name + ":int," + second_name + ":int>"));
    const std::array field_ids = {first_field_id, second_field_id};
    for (size_t field_idx = 0; field_idx < field_ids.size(); ++field_idx) {
        if (field_ids[field_idx].has_value()) {
            type->getSubtype(field_idx)->setAttribute("iceberg.id",
                                                      std::to_string(*field_ids[field_idx]));
        }
    }

    MemoryOutputStream memory_stream(1024 * 1024);
    ::orc::WriterOptions options;
    options.setCompression(::orc::CompressionKind_NONE);
    options.setMemoryPool(::orc::getDefaultPool());
    auto writer = ::orc::createWriter(*type, &memory_stream, options);
    auto batch = writer->createRowBatch(first_values.size());
    auto& struct_batch = dynamic_cast<::orc::StructVectorBatch&>(*batch);
    const std::array value_sets = {&first_values, &second_values};
    for (size_t field_idx = 0; field_idx < value_sets.size(); ++field_idx) {
        auto& value_batch = dynamic_cast<::orc::LongVectorBatch&>(*struct_batch.fields[field_idx]);
        for (size_t row = 0; row < first_values.size(); ++row) {
            value_batch.data[row] = (*value_sets[field_idx])[row];
        }
        value_batch.numElements = first_values.size();
    }
    struct_batch.numElements = first_values.size();
    writer->add(*batch);
    writer->close();

    std::ofstream output(file_path, std::ios::binary);
    output.write(memory_stream.getData(), static_cast<std::streamsize>(memory_stream.getLength()));
}

void write_single_int_orc_file(const std::string& file_path, const std::string& field_name,
                               const std::vector<int64_t>& values,
                               std::optional<int32_t> field_id) {
    auto type = std::unique_ptr<::orc::Type>(
            ::orc::Type::buildTypeFromString("struct<" + field_name + ":int>"));
    if (field_id.has_value()) {
        type->getSubtype(0)->setAttribute("iceberg.id", std::to_string(*field_id));
    }

    MemoryOutputStream memory_stream(1024 * 1024);
    ::orc::WriterOptions options;
    options.setCompression(::orc::CompressionKind_NONE);
    options.setMemoryPool(::orc::getDefaultPool());
    auto writer = ::orc::createWriter(*type, &memory_stream, options);
    auto batch = writer->createRowBatch(values.size());
    auto& struct_batch = dynamic_cast<::orc::StructVectorBatch&>(*batch);
    auto& value_batch = dynamic_cast<::orc::LongVectorBatch&>(*struct_batch.fields[0]);
    for (size_t row = 0; row < values.size(); ++row) {
        value_batch.data[row] = values[row];
    }
    struct_batch.numElements = values.size();
    value_batch.numElements = values.size();
    writer->add(*batch);
    writer->close();

    std::ofstream output(file_path, std::ios::binary);
    output.write(memory_stream.getData(), static_cast<std::streamsize>(memory_stream.getLength()));
}

void write_recursive_idless_wrapper_parquet_file(const std::string& file_path, int32_t value,
                                                 bool outer_has_field_id = true) {
    const auto leaf_metadata = arrow::key_value_metadata({"PARQUET:field_id"}, {"30"});
    auto leaf_field = arrow::field("leaf", arrow::int32(), false)->WithMetadata(leaf_metadata);
    auto inner_result = arrow::StructArray::Make({build_int32_array({value})}, {leaf_field});
    ASSERT_TRUE(inner_result.ok()) << inner_result.status();
    auto inner_field = arrow::field("inner", arrow::struct_({leaf_field}), false);
    auto outer_result = arrow::StructArray::Make({*inner_result}, {inner_field});
    ASSERT_TRUE(outer_result.ok()) << outer_result.status();
    auto outer_field = arrow::field("outer", arrow::struct_({inner_field}), false);
    if (outer_has_field_id) {
        outer_field =
                outer_field->WithMetadata(arrow::key_value_metadata({"PARQUET:field_id"}, {"10"}));
    }
    auto table = arrow::Table::Make(arrow::schema({outer_field}), {*outer_result});

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

void write_nullable_idless_struct_with_sibling_id_parquet_file(const std::string& file_path) {
    const auto sibling_metadata = arrow::key_value_metadata({"PARQUET:field_id"}, {"2"});
    auto sibling_field = arrow::field("b", arrow::int32(), false)->WithMetadata(sibling_metadata);
    const auto null_bitmap = arrow::Buffer::FromString(std::string("\x02", 1));
    auto struct_result =
            arrow::StructArray::Make({build_int32_array({0, 42})}, {sibling_field}, null_bitmap, 1);
    ASSERT_TRUE(struct_result.ok()) << struct_result.status();
    auto struct_field = arrow::field("s", arrow::struct_({sibling_field}), true);
    auto table = arrow::Table::Make(arrow::schema({struct_field}), {*struct_result});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;
    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    builder.compression(::parquet::Compression::UNCOMPRESSED);
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out, 2,
                                                      builder.build()));
}

void write_nullable_renamed_struct_child_parquet_file(const std::string& file_path) {
    const auto child_metadata = arrow::key_value_metadata({"PARQUET:field_id"}, {"1"});
    auto child_field = arrow::field("b", arrow::int32(), false)->WithMetadata(child_metadata);
    const auto null_bitmap = arrow::Buffer::FromString(std::string("\x02", 1));
    auto struct_result =
            arrow::StructArray::Make({build_int32_array({0, 10})}, {child_field}, null_bitmap, 1);
    ASSERT_TRUE(struct_result.ok()) << struct_result.status();
    auto struct_field =
            arrow::field("s", arrow::struct_({child_field}), true)
                    ->WithMetadata(arrow::key_value_metadata({"PARQUET:field_id"}, {"10"}));
    auto table = arrow::Table::Make(arrow::schema({struct_field}), {*struct_result});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;
    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    builder.compression(::parquet::Compression::UNCOMPRESSED);
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out, 2,
                                                      builder.build()));
}

std::shared_ptr<arrow::Array> build_nullable_struct_int_array(
        const std::shared_ptr<arrow::Field>& child_field, const std::vector<int32_t>& values,
        const std::vector<bool>& parent_nulls) {
    DORIS_CHECK(values.size() == parent_nulls.size());
    auto child_builder = std::make_unique<arrow::Int32Builder>();
    arrow::StructBuilder builder(arrow::struct_({child_field}), arrow::default_memory_pool(),
                                 {std::move(child_builder)});
    auto* value_builder = assert_cast<arrow::Int32Builder*>(builder.field_builder(0));
    for (size_t row = 0; row < values.size(); ++row) {
        if (parent_nulls[row]) {
            DORIS_CHECK(builder.AppendNull().ok());
        } else {
            DORIS_CHECK(builder.Append().ok());
            DORIS_CHECK(value_builder->Append(values[row]).ok());
        }
    }
    return finish_array(&builder);
}

void write_nested_equality_parquet_file(
        const std::string& file_path, const std::vector<int32_t>& ids,
        const std::vector<int32_t>& values, const std::vector<bool>& parent_nulls,
        bool parent_optional = true, const std::string& child_name = "existing",
        int32_t child_id = 2, const std::string& parent_name = "payload",
        bool write_field_ids = true, bool parent_has_field_id = true) {
    ASSERT_TRUE(ids.empty() || ids.size() == values.size());
    auto child_field = arrow::field(child_name, arrow::int32(), false);
    auto payload_field = arrow::field(parent_name, arrow::struct_({child_field}), parent_optional);
    if (write_field_ids) {
        child_field = child_field->WithMetadata(
                arrow::key_value_metadata({"PARQUET:field_id"}, {std::to_string(child_id)}));
        payload_field = arrow::field(parent_name, arrow::struct_({child_field}), parent_optional);
        if (parent_has_field_id) {
            payload_field = payload_field->WithMetadata(
                    arrow::key_value_metadata({"PARQUET:field_id"}, {"1"}));
        }
    }
    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    if (!ids.empty()) {
        auto id_field = arrow::field("id", arrow::int32(), false);
        if (write_field_ids) {
            id_field =
                    id_field->WithMetadata(arrow::key_value_metadata({"PARQUET:field_id"}, {"0"}));
        }
        fields.push_back(std::move(id_field));
        arrays.push_back(build_int32_array(ids));
    }
    fields.push_back(payload_field);
    arrays.push_back(build_nullable_struct_int_array(child_field, values, parent_nulls));
    auto table = arrow::Table::Make(arrow::schema(fields), arrays);

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

void write_nested_equality_orc_file(const std::string& file_path, const std::vector<int64_t>& ids,
                                    const std::vector<int64_t>& values,
                                    const std::vector<bool>& parent_nulls,
                                    const std::string& child_name = "existing",
                                    int32_t child_id = 2,
                                    const std::string& parent_name = "payload",
                                    bool write_field_ids = true) {
    ASSERT_TRUE(ids.empty() || ids.size() == values.size());
    ASSERT_EQ(values.size(), parent_nulls.size());
    const std::string schema =
            ids.empty() ? "struct<" + parent_name + ":struct<" + child_name + ":int>>"
                        : "struct<id:int," + parent_name + ":struct<" + child_name + ":int>>";
    auto type = std::unique_ptr<::orc::Type>(::orc::Type::buildTypeFromString(schema));
    const size_t payload_index = ids.empty() ? 0 : 1;
    if (write_field_ids) {
        if (!ids.empty()) {
            type->getSubtype(0)->setAttribute("iceberg.id", "0");
        }
        type->getSubtype(payload_index)->setAttribute("iceberg.id", "1");
        type->getSubtype(payload_index)
                ->getSubtype(0)
                ->setAttribute("iceberg.id", std::to_string(child_id));
    }

    MemoryOutputStream memory_stream(1024 * 1024);
    ::orc::WriterOptions options;
    options.setCompression(::orc::CompressionKind_NONE);
    options.setMemoryPool(::orc::getDefaultPool());
    auto writer = ::orc::createWriter(*type, &memory_stream, options);
    auto batch = writer->createRowBatch(values.size());
    auto& root_batch = dynamic_cast<::orc::StructVectorBatch&>(*batch);
    if (!ids.empty()) {
        auto& id_batch = dynamic_cast<::orc::LongVectorBatch&>(*root_batch.fields[0]);
        for (size_t row = 0; row < ids.size(); ++row) {
            id_batch.data[row] = ids[row];
        }
        id_batch.numElements = ids.size();
    }
    auto& payload_batch =
            dynamic_cast<::orc::StructVectorBatch&>(*root_batch.fields[payload_index]);
    auto& value_batch = dynamic_cast<::orc::LongVectorBatch&>(*payload_batch.fields[0]);
    payload_batch.hasNulls = true;
    payload_batch.notNull.resize(values.size());
    for (size_t row = 0; row < values.size(); ++row) {
        payload_batch.notNull[row] = !parent_nulls[row];
        value_batch.data[row] = values[row];
    }
    root_batch.numElements = values.size();
    payload_batch.numElements = values.size();
    value_batch.numElements = values.size();
    writer->add(*batch);
    writer->close();

    std::ofstream output(file_path, std::ios::binary);
    output.write(memory_stream.getData(), static_cast<std::streamsize>(memory_stream.getLength()));
}

void write_nested_key_value_parquet_file(const std::string& file_path,
                                         const std::vector<int32_t>& keys,
                                         const std::vector<int32_t>& values) {
    ASSERT_EQ(keys.size(), values.size());
    auto key_field = arrow::field("delete_key", arrow::int32(), false)
                             ->WithMetadata(arrow::key_value_metadata({"PARQUET:field_id"}, {"2"}));
    auto value_field =
            arrow::field("visible_value", arrow::int32(), false)
                    ->WithMetadata(arrow::key_value_metadata({"PARQUET:field_id"}, {"3"}));
    auto payload_result = arrow::StructArray::Make(
            {build_int32_array(keys), build_int32_array(values)}, {key_field, value_field});
    ASSERT_TRUE(payload_result.ok()) << payload_result.status();
    auto payload_field =
            arrow::field("payload", arrow::struct_({key_field, value_field}), false)
                    ->WithMetadata(arrow::key_value_metadata({"PARQUET:field_id"}, {"1"}));
    auto table = arrow::Table::Make(arrow::schema({payload_field}), {*payload_result});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;
    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    builder.compression(::parquet::Compression::UNCOMPRESSED);
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out,
                                                      static_cast<int64_t>(keys.size()),
                                                      builder.build()));
}

void write_nested_key_value_orc_file(const std::string& file_path, const std::vector<int64_t>& keys,
                                     const std::vector<int64_t>& values) {
    ASSERT_EQ(keys.size(), values.size());
    auto type = std::unique_ptr<::orc::Type>(::orc::Type::buildTypeFromString(
            "struct<payload:struct<delete_key:int,visible_value:int>>"));
    type->getSubtype(0)->setAttribute("iceberg.id", "1");
    type->getSubtype(0)->getSubtype(0)->setAttribute("iceberg.id", "2");
    type->getSubtype(0)->getSubtype(1)->setAttribute("iceberg.id", "3");

    MemoryOutputStream memory_stream(1024 * 1024);
    ::orc::WriterOptions options;
    options.setCompression(::orc::CompressionKind_NONE);
    options.setMemoryPool(::orc::getDefaultPool());
    auto writer = ::orc::createWriter(*type, &memory_stream, options);
    auto batch = writer->createRowBatch(keys.size());
    auto& root_batch = dynamic_cast<::orc::StructVectorBatch&>(*batch);
    auto& payload_batch = dynamic_cast<::orc::StructVectorBatch&>(*root_batch.fields[0]);
    const std::array value_sets = {&keys, &values};
    for (size_t field_idx = 0; field_idx < value_sets.size(); ++field_idx) {
        auto& value_batch = dynamic_cast<::orc::LongVectorBatch&>(*payload_batch.fields[field_idx]);
        for (size_t row = 0; row < keys.size(); ++row) {
            value_batch.data[row] = (*value_sets[field_idx])[row];
        }
        value_batch.numElements = keys.size();
    }
    root_batch.numElements = keys.size();
    payload_batch.numElements = keys.size();
    writer->add(*batch);
    writer->close();

    std::ofstream output(file_path, std::ios::binary);
    output.write(memory_stream.getData(), static_cast<std::streamsize>(memory_stream.getLength()));
}

// Writes the adjusted-UTC `event_time` (field id 0) and `id` (field id 1). With `int96`,
// `event_time` is stored as INT96, as Spark and Hive write timestamps, which drops the annotation.
// Without `field_ids` the file looks like one Spark wrote and `add_files` attached to a table.
void write_timestamp_int_parquet_file(const std::string& file_path,
                                      const std::vector<int64_t>& timestamps,
                                      const std::vector<int32_t>& ids, bool int96 = false,
                                      bool field_ids = true) {
    ASSERT_EQ(timestamps.size(), ids.size());
    const auto timestamp_type = arrow::timestamp(arrow::TimeUnit::MICRO, "UTC");
    arrow::TimestampBuilder timestamp_builder(timestamp_type, arrow::default_memory_pool());
    ASSERT_TRUE(timestamp_builder.AppendValues(timestamps).ok());
    auto timestamp_result = timestamp_builder.Finish();
    ASSERT_TRUE(timestamp_result.ok()) << timestamp_result.status();
    auto timestamp_field = arrow::field("event_time", timestamp_type, false);
    auto id_field = arrow::field("id", arrow::int32(), false);
    if (field_ids) {
        timestamp_field = timestamp_field->WithMetadata(
                arrow::key_value_metadata({"PARQUET:field_id"}, {"0"}));
        id_field = id_field->WithMetadata(arrow::key_value_metadata({"PARQUET:field_id"}, {"1"}));
    }
    auto schema = arrow::schema({timestamp_field, id_field});
    auto table = arrow::Table::Make(schema, {*timestamp_result, build_int32_array(ids)});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;
    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    builder.compression(::parquet::Compression::UNCOMPRESSED);
    ::parquet::ArrowWriterProperties::Builder arrow_builder;
    if (int96) {
        arrow_builder.enable_force_write_int96_timestamps();
    }
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out,
                                                      static_cast<int64_t>(timestamps.size()),
                                                      builder.build(), arrow_builder.build()));
}

// Writes `id` (field id 1), `info` STRUCT<tz (3), wall (4)> (2) and `tzs` LIST<element (6)> (5)
// with every timestamp stored as INT96, as Spark writes nested timestamps.
void write_nested_int96_parquet_file(const std::string& file_path, int64_t timestamp) {
    const auto field_id = [](int32_t id) {
        return arrow::key_value_metadata({"PARQUET:field_id"}, {std::to_string(id)});
    };
    const auto timestamp_type = arrow::timestamp(arrow::TimeUnit::MICRO, "UTC");
    arrow::TimestampBuilder timestamp_builder(timestamp_type, arrow::default_memory_pool());
    ASSERT_TRUE(timestamp_builder.Append(timestamp).ok());
    auto timestamps = timestamp_builder.Finish();
    ASSERT_TRUE(timestamps.ok()) << timestamps.status();
    auto tz_field = arrow::field("tz", timestamp_type)->WithMetadata(field_id(3));
    auto wall_field = arrow::field("wall", timestamp_type)->WithMetadata(field_id(4));
    auto info = arrow::StructArray::Make({*timestamps, *timestamps}, {tz_field, wall_field});
    ASSERT_TRUE(info.ok()) << info.status();
    auto tzs_type = arrow::list(arrow::field("element", timestamp_type)->WithMetadata(field_id(6)));
    auto tzs = arrow::ListArray::FromArrays(tzs_type, *build_int32_array({0, 1}), **timestamps);
    ASSERT_TRUE(tzs.ok()) << tzs.status();
    auto schema = arrow::schema({
            arrow::field("id", arrow::int32(), false)->WithMetadata(field_id(1)),
            arrow::field("info", (*info)->type())->WithMetadata(field_id(2)),
            arrow::field("tzs", tzs_type)->WithMetadata(field_id(5)),
    });
    auto table = arrow::Table::Make(schema, {build_int32_array({1}), *info, *tzs});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;
    ::parquet::ArrowWriterProperties::Builder arrow_builder;
    arrow_builder.enable_force_write_int96_timestamps();
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(
            *table, arrow::default_memory_pool(), out, 1,
            ::parquet::WriterProperties::Builder().build(), arrow_builder.build()));
}

// Writes `id` (field id 1) and `outer` STRUCT (10) holding an ID-less `inner` STRUCT whose `ts` (30)
// is an INT96 timestamp: the ID-less wrapper Iceberg's Parquet projection keeps for a selected
// nested ID, holding a timestamp the way Spark writes it.
void write_idless_wrapper_int96_parquet_file(const std::string& file_path,
                                             const std::vector<int64_t>& timestamps,
                                             const std::vector<int32_t>& ids) {
    ASSERT_EQ(timestamps.size(), ids.size());
    const auto field_id = [](int32_t id) {
        return arrow::key_value_metadata({"PARQUET:field_id"}, {std::to_string(id)});
    };
    const auto timestamp_type = arrow::timestamp(arrow::TimeUnit::MICRO, "UTC");
    arrow::TimestampBuilder timestamp_builder(timestamp_type, arrow::default_memory_pool());
    ASSERT_TRUE(timestamp_builder.AppendValues(timestamps).ok());
    auto timestamp_result = timestamp_builder.Finish();
    ASSERT_TRUE(timestamp_result.ok()) << timestamp_result.status();
    auto ts_field = arrow::field("ts", timestamp_type, false)->WithMetadata(field_id(30));
    auto inner_result = arrow::StructArray::Make({*timestamp_result}, {ts_field});
    ASSERT_TRUE(inner_result.ok()) << inner_result.status();
    auto inner_field = arrow::field("inner", arrow::struct_({ts_field}), false);
    auto outer_result = arrow::StructArray::Make({*inner_result}, {inner_field});
    ASSERT_TRUE(outer_result.ok()) << outer_result.status();
    auto schema = arrow::schema({
            arrow::field("id", arrow::int32(), false)->WithMetadata(field_id(1)),
            arrow::field("outer", arrow::struct_({inner_field}), false)->WithMetadata(field_id(10)),
    });
    auto table = arrow::Table::Make(schema, {build_int32_array(ids), *outer_result});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;
    ::parquet::ArrowWriterProperties::Builder arrow_builder;
    arrow_builder.enable_force_write_int96_timestamps();
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(
            *table, arrow::default_memory_pool(), out, static_cast<int64_t>(ids.size()),
            ::parquet::WriterProperties::Builder().build(), arrow_builder.build()));
}

// Writes `id` (field id 0) and a nullable `payload` STRUCT (1) of `existing` (2) and `ts` (4), with
// `ts` stored as INT96 the way Spark writes a timestamptz. A true `parent_nulls` entry makes that
// row's `payload` NULL.
void write_nested_equality_int96_sibling_parquet_file(const std::string& file_path,
                                                      const std::vector<int32_t>& ids,
                                                      const std::vector<int32_t>& values,
                                                      const std::vector<int64_t>& timestamps,
                                                      const std::vector<bool>& parent_nulls) {
    ASSERT_EQ(ids.size(), values.size());
    ASSERT_EQ(ids.size(), timestamps.size());
    ASSERT_EQ(ids.size(), parent_nulls.size());
    const auto field_id = [](int32_t id) {
        return arrow::key_value_metadata({"PARQUET:field_id"}, {std::to_string(id)});
    };
    const auto timestamp_type = arrow::timestamp(arrow::TimeUnit::MICRO, "UTC");
    const auto payload_type = arrow::struct_(
            {arrow::field("existing", arrow::int32(), false)->WithMetadata(field_id(2)),
             arrow::field("ts", timestamp_type, false)->WithMetadata(field_id(4))});
    arrow::StructBuilder payload_builder(payload_type, arrow::default_memory_pool(),
                                         {std::make_shared<arrow::Int32Builder>(),
                                          std::make_shared<arrow::TimestampBuilder>(
                                                  timestamp_type, arrow::default_memory_pool())});
    auto* existing_builder = assert_cast<arrow::Int32Builder*>(payload_builder.field_builder(0));
    auto* ts_builder = assert_cast<arrow::TimestampBuilder*>(payload_builder.field_builder(1));
    for (size_t row = 0; row < ids.size(); ++row) {
        if (parent_nulls[row]) {
            ASSERT_TRUE(payload_builder.AppendNull().ok());
        } else {
            ASSERT_TRUE(payload_builder.Append().ok());
            ASSERT_TRUE(existing_builder->Append(values[row]).ok());
            ASSERT_TRUE(ts_builder->Append(timestamps[row]).ok());
        }
    }
    auto schema = arrow::schema({
            arrow::field("id", arrow::int32(), false)->WithMetadata(field_id(0)),
            arrow::field("payload", payload_type)->WithMetadata(field_id(1)),
    });
    auto table =
            arrow::Table::Make(schema, {build_int32_array(ids), finish_array(&payload_builder)});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;
    ::parquet::ArrowWriterProperties::Builder arrow_builder;
    arrow_builder.enable_force_write_int96_timestamps();
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(
            *table, arrow::default_memory_pool(), out, static_cast<int64_t>(ids.size()),
            ::parquet::WriterProperties::Builder().build(), arrow_builder.build()));
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

void write_nullable_int64_parquet_file(const std::string& file_path, int32_t field_id,
                                       const std::string& field_name,
                                       const std::vector<std::optional<int64_t>>& values) {
    const auto metadata =
            arrow::key_value_metadata({"PARQUET:field_id"}, {std::to_string(field_id)});
    auto schema = arrow::schema({
            arrow::field(field_name, arrow::int64(), true)->WithMetadata(metadata),
    });
    arrow::Int64Builder value_builder;
    for (const auto& value : values) {
        if (value.has_value()) {
            ASSERT_TRUE(value_builder.Append(*value).ok());
        } else {
            ASSERT_TRUE(value_builder.AppendNull().ok());
        }
    }
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
    const uint32_t crc = HashUtil::zlib_crc_hash(blob.data() + 4, total_length, 0);
    BigEndian::Store32(blob.data() + 8 + bitmap_size, crc);

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

    ScopedDebugPoint(std::string name, std::function<void()> callback)
            : _name(std::move(name)), _enable_debug_points(config::enable_debug_points) {
        config::enable_debug_points = true;
        DebugPoints::instance()->add_with_callback(_name, std::move(callback));
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

void expect_nullable_int32_column_is_null(const IColumn& column, size_t rows) {
    const auto full_column = column.convert_to_full_column_if_const();
    const auto& nullable_column = assert_cast<const ColumnNullable&>(*full_column);
    ASSERT_EQ(nullable_column.size(), rows);
    for (size_t row = 0; row < rows; ++row) {
        EXPECT_EQ(nullable_column.get_null_map_data()[row], 1);
    }
}

void expect_string_column_values(const IColumn& column,
                                 const std::vector<std::string>& expected_values) {
    const auto full_column = column.convert_to_full_column_if_const();
    const auto& nested_column = expect_not_null_nullable_nested_column(*full_column);
    const auto& values = assert_cast<const ColumnString&>(nested_column);
    ASSERT_EQ(values.size(), expected_values.size());
    for (size_t row = 0; row < expected_values.size(); ++row) {
        EXPECT_EQ(values.get_data_at(row).to_string(), expected_values[row]);
    }
}

void expect_int64_column_values(const IColumn& column,
                                const std::vector<int64_t>& expected_values) {
    const auto full_column = column.convert_to_full_column_if_const();
    const auto& nested_column = expect_not_null_nullable_nested_column(*full_column);
    const auto& values = assert_cast<const ColumnInt64&>(nested_column).get_data();
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

TIcebergDeleteFileDesc make_iceberg_equality_delete_file(
        const std::string& path, const std::vector<int32_t>& field_ids,
        TFileFormatType::type file_format = TFileFormatType::FORMAT_PARQUET,
        int64_t file_size = -1) {
    TIcebergDeleteFileDesc delete_file;
    delete_file.__set_content(2);
    delete_file.__set_path(path);
    delete_file.__set_field_ids(field_ids);
    delete_file.__set_file_format(file_format);
    // Default callers emulate an old FE that sends no file_size.
    if (file_size >= 0) {
        delete_file.__set_file_size(file_size);
    }
    return delete_file;
}

TFileScanRangeParams make_local_scan_params(FileFormat file_format) {
    TFileScanRangeParams scan_params;
    scan_params.__set_file_type(TFileType::FILE_LOCAL);
    scan_params.__set_format_type(file_format == FileFormat::PARQUET
                                          ? TFileFormatType::FORMAT_PARQUET
                                          : TFileFormatType::FORMAT_ORC);
    scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_1);
    return scan_params;
}

TFileScanRangeParams make_local_parquet_scan_params() {
    return make_local_scan_params(FileFormat::PARQUET);
}

TColumnAccessPath nested_data_access_path(std::vector<std::string> path) {
    TColumnAccessPath access_path;
    access_path.__set_type(TAccessPathType::DATA);
    TDataAccessPath data_path;
    data_path.__set_path(std::move(path));
    access_path.__set_data_access_path(std::move(data_path));
    return access_path;
}

std::shared_ptr<io::IOContext> make_io_context(io::FileReaderStats* file_reader_stats,
                                               io::FileCacheStatistics* file_cache_stats) {
    auto io_ctx = std::make_shared<io::IOContext>();
    io_ctx->file_reader_stats = file_reader_stats;
    io_ctx->file_cache_stats = file_cache_stats;
    return io_ctx;
}

TTableFormatFileDesc make_iceberg_table_format_desc(
        const std::string& data_file_path, const std::vector<TIcebergDeleteFileDesc>& delete_files,
        int32_t format_version = 2) {
    TTableFormatFileDesc table_format_params;
    TIcebergFileDesc iceberg_params;
    iceberg_params.__set_format_version(format_version);
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
                         RuntimeProfile* profile, FileFormat file_format = FileFormat::PARQUET) {
    ASSERT_TRUE(reader->init({
                                     .projected_columns = projected_columns,
                                     .conjuncts = {},
                                     .format = file_format,
                                     .scan_params = scan_params,
                                     .io_ctx = io_ctx,
                                     .runtime_state = state,
                                     .scanner_profile = profile,
                             })
                        .ok());
}

ColumnDefinition make_authoritatively_name_mapped_table_column(int32_t id, std::string name,
                                                               const DataTypePtr& type) {
    auto column = make_table_column(id, name, type);
    // Equality-delete tests need a readable result carrier while independently exercising an
    // ID-less hidden key. Make that carrier explicitly name-readable under the V2 mapping rules.
    column.name_mapping = {std::move(name)};
    column.has_name_mapping = true;
    return column;
}

void expect_idless_equality_key_uses_delete_file_name(FileFormat file_format,
                                                      bool authoritative_name_mapping) {
    const std::string format_name = file_format == FileFormat::PARQUET ? "parquet" : "orc";
    const auto test_dir = std::filesystem::temp_directory_path() /
                          ("doris_iceberg_v2_idless_equality_delete_name_" + format_name + "_" +
                           (authoritative_name_mapping ? "authoritative" : "fallback"));
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / ("split." + format_name)).string();
    const auto delete_file_path = (test_dir / ("equality-delete." + format_name)).string();
    if (file_format == FileFormat::PARQUET) {
        write_two_int_parquet_file(file_path, "id", {1, 2, 3}, std::nullopt, "old_name", {5, 7, 9},
                                   std::nullopt);
        write_iceberg_equality_delete_parquet_file(delete_file_path, 1, 7, "old_name");
    } else {
        write_two_int_orc_file(file_path, "id", {1, 2, 3}, std::nullopt, "old_name", {5, 7, 9},
                               std::nullopt);
        write_single_int_orc_file(delete_file_path, "old_name", {7}, 1);
    }

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_authoritatively_name_mapped_table_column(
            0, "id", std::make_shared<DataTypeInt32>()));

    auto equality_field = external_schema_field("future_name", 1, {}, "7");
    if (authoritative_name_mapping) {
        equality_field.field_ptr->__set_name_mapping({});
        equality_field.field_ptr->__set_name_mapping_is_authoritative(true);
    }
    auto scan_params = make_local_scan_params(file_format);
    scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
    scan_params.__set_current_schema_id(100);
    scan_params.__set_history_schema_info(
            {external_schema(100, {external_schema_field("id", 0), equality_field})});

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::format::iceberg::IcebergTableReader reader;
    init_iceberg_reader(&reader, projected_columns, &scan_params, io_ctx, &state, &profile,
                        file_format);

    auto split_options = build_split_options(file_path);
    split_options.cache = &cache;
    split_options.current_split_format = file_format;
    const auto thrift_file_format = file_format == FileFormat::PARQUET
                                            ? TFileFormatType::FORMAT_PARQUET
                                            : TFileFormatType::FORMAT_ORC;
    split_options.current_range.__set_table_format_params(make_iceberg_table_format_desc(
            file_path,
            {make_iceberg_equality_delete_file(delete_file_path, {1}, thrift_file_format)}, 3));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    const auto ids = read_iceberg_ids(&reader, projected_columns);
    if (authoritative_name_mapping) {
        // An explicit empty mapping says field ID 1 is absent from the ID-less data file. The
        // initial default therefore matches the delete key for every row.
        EXPECT_TRUE(ids.empty());
    } else {
        // Without authoritative mapping, the target-relative carrier may contain a later rename.
        // The delete file's old_name must bind the physical key instead of materializing 7.
        EXPECT_EQ(ids, std::vector<int32_t>({1, 3}));
    }

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
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

ColumnDefinition make_synthesized_table_column(const std::string& name, const DataTypePtr& type) {
    auto column = make_table_column(-1, name, type);
    column.is_synthesized = true;
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

struct EvolutionTopNFilter {
    std::shared_ptr<MockQueryContext> query_context;
    VExprContextSPtr conjunct;
};

EvolutionTopNFilter make_evolution_topn_filter(RuntimeState* state, bool ascending) {
    const auto type = make_nullable(std::make_shared<DataTypeInt32>());
    TExpr target;
    TExprNode slot;
    slot.__set_node_type(TExprNodeType::SLOT_REF);
    slot.__set_type(type->to_thrift());
    slot.__set_num_children(0);
    TSlotRef slot_ref;
    slot_ref.__set_slot_id(0);
    slot_ref.__set_tuple_id(0);
    slot.__set_slot_ref(slot_ref);
    target.nodes.push_back(slot);
    TTopnFilterDesc desc;
    desc.__set_source_node_id(10);
    desc.__set_is_asc(ascending);
    desc.__set_null_first(false);
    desc.__set_target_node_id_to_target_expr({{20, target}});
    auto query = MockQueryContext::create();
    state->_query_ctx = query.get();
    query->init_runtime_predicates({desc});
    auto& predicate = query->get_runtime_predicate(10);
    predicate.set_detected_source();
    DORIS_CHECK(predicate.init_target(20, {}, -1).ok());
    TExprNode topn_node;
    topn_node.__set_type(std::make_shared<DataTypeUInt8>()->to_thrift());
    topn_node.__set_is_nullable(true);
    auto topn = VTopNPred::create_shared(topn_node, 10, nullptr);
    topn->add_child(VSlotRef::create_shared(0, 0, -1, type, "k"));
    return {.query_context = std::move(query), .conjunct = prepared_conjunct(state, topn)};
}

void write_topn_evolution_file(const std::string& path, bool ascending, bool nested,
                               int32_t bad_date = -354285) {
    constexpr int GROUP_ROWS = 4096;
    arrow::Int32Builder keys;
    arrow::Date32Builder dates;
    for (int row = 0; row < GROUP_ROWS * 4; ++row) {
        const bool bad = row < GROUP_ROWS;
        const int key = (bad == ascending) ? 100 : 0;
        DORIS_CHECK(keys.Append(key).ok());
        DORIS_CHECK(dates.Append(bad ? bad_date : 19724).ok());
    }
    auto key_field = arrow::field("k", arrow::int32(), true,
                                  arrow::key_value_metadata({"PARQUET:field_id"}, {"1"}));
    auto date_field = arrow::field("ts", arrow::date32(), true,
                                   arrow::key_value_metadata({"PARQUET:field_id"}, {"2"}));
    std::shared_ptr<arrow::Array> date_values = dates.Finish().ValueOrDie();
    if (nested) {
        date_values = arrow::StructArray::Make({date_values}, {date_field}).ValueOrDie();
        date_field = arrow::field("outer", date_values->type(), true,
                                  arrow::key_value_metadata({"PARQUET:field_id"}, {"3"}));
    }
    auto table = arrow::Table::Make(arrow::schema({key_field, date_field}),
                                    {keys.Finish().ValueOrDie(), date_values});
    auto out = arrow::io::FileOutputStream::Open(path).ValueOrDie();
    ::parquet::WriterProperties::Builder properties;
    properties.disable_dictionary();
    properties.data_page_version(::parquet::ParquetDataPageVersion::V2);
    DORIS_CHECK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out, GROUP_ROWS,
                                             properties.build())
                        .ok());
    DORIS_CHECK(out->Close().ok());
}

std::vector<ColumnDefinition> topn_evolution_columns(bool promote, bool nested) {
    DataTypePtr date_type = promote ? DataTypePtr(std::make_shared<DataTypeTimeStampNs>())
                                    : DataTypePtr(std::make_shared<DataTypeDateV2>());
    auto date = make_table_column(2, "ts", date_type);
    if (nested) {
        auto outer = make_table_column(
                3, "outer",
                std::make_shared<DataTypeStruct>(DataTypes {date.type}, Strings {"ts"}));
        outer.children = {date};
        date = outer;
    }
    return {make_table_column(1, "k", std::make_shared<DataTypeInt32>()), date};
}

TEST(IcebergV2ReaderTest, TopNPrunesRowsBeforeUnneededEvolutionConversion) {
    const auto dir = std::filesystem::temp_directory_path() / "topn_evolution_projection";
    std::filesystem::create_directories(dir);
    Defer cleanup([&] { std::filesystem::remove_all(dir); });
    for (const bool ascending : {false, true}) {
        for (const bool nested : {false, true}) {
            const auto path = (dir / (std::string(ascending ? "asc" : "desc") +
                                      (nested ? "_nested.parquet" : ".parquet")))
                                      .string();
            // RG0 cannot beat any later group on k. Its DATE -> TIMESTAMP_NS conversion is not
            // needed for TopN, while the same conversion must still fail in an unfiltered scan.
            write_topn_evolution_file(path, ascending, nested);
            for (const bool promote : {false, true}) {
                for (const bool strict : {false, true}) {
                    SCOPED_TRACE(testing::Message()
                                 << "ascending=" << ascending << " nested=" << nested
                                 << " promote=" << promote << " strict=" << strict);
                    TQueryOptions options;
                    options.__set_enable_strict_cast(strict);
                    options.__set_batch_size(4096);
                    RuntimeState state {options, TQueryGlobals()};
                    auto filter = make_evolution_topn_filter(&state, ascending);
                    const auto columns = topn_evolution_columns(promote, nested);
                    auto scan_params = make_local_parquet_scan_params();
                    RuntimeProfile profile("topn_evolution");
                    iceberg::IcebergTableReader reader;
                    ASSERT_TRUE(reader.init({.projected_columns = columns,
                                             .conjuncts = {filter.conjunct},
                                             .format = FileFormat::PARQUET,
                                             .scan_params = &scan_params,
                                             .io_ctx = nullptr,
                                             .runtime_state = &state,
                                             .scanner_profile = &profile})
                                        .ok());
                    ASSERT_TRUE(reader.prepare_split(build_split_options(path)).ok());
                    bool eos = false;
                    size_t rows = 0;
                    Status status;
                    while (!eos) {
                        Block block = build_table_block(columns);
                        status = reader.get_block(&block, &eos);
                        if (!status.ok()) {
                            break;
                        }
                        rows += block.rows();
                        if (block.rows() > 0) {
                            const auto& values = assert_cast<const ColumnNullable&>(
                                                         *block.get_by_position(0).column)
                                                         .get_nested_column();
                            EXPECT_EQ(assert_cast<const ColumnInt32&>(values).get_element(0),
                                      ascending ? 0 : 100);
                            const IColumn* dates = block.get_by_position(1).column.get();
                            if (nested) {
                                dates = &assert_cast<const ColumnStruct&>(
                                                 expect_not_null_table_column(block, 1))
                                                 .get_column(0);
                            }
                            const auto type =
                                    promote ? DataTypePtr(std::make_shared<DataTypeTimeStampNs>())
                                            : DataTypePtr(std::make_shared<DataTypeDateV2>());
                            const auto& materialized =
                                    expect_not_null_nullable_nested_column(*dates);
                            for (size_t row = 0; row < block.rows(); ++row) {
                                EXPECT_EQ(type->to_string(materialized, row),
                                          promote ? "2024-01-02 00:00:00.000000000" : "2024-01-02");
                            }
                            ASSERT_TRUE(filter.query_context->get_runtime_predicate(10)
                                                .update(Field::create_field<TYPE_INT>(
                                                        ascending ? 0 : 100))
                                                .ok());
                        }
                    }
                    EXPECT_TRUE(status.ok()) << status;
                    EXPECT_EQ(rows, 3 * 4096);
                    ASSERT_TRUE(reader.close().ok());
                    filter.conjunct->close();
                }
            }
        }
    }
}

TEST(IcebergV2ReaderTest, TopNPruningSkipsNativeAndHiddenDeleteConversionsOnlyForExcludedRows) {
    const auto dir = std::filesystem::temp_directory_path() / "topn_conversion_pruning";
    std::filesystem::create_directories(dir);
    Defer cleanup([&] { std::filesystem::remove_all(dir); });
    for (const bool hidden_delete : {false, true}) {
        for (const bool ascending : {false, true}) {
            const auto suffix = std::string(hidden_delete ? "delete_" : "native_") +
                                (ascending ? "asc" : "desc");
            const auto path = (dir / (suffix + ".parquet")).string();
            // DATE's INT32 domain includes year 10000, which Doris DATE cannot materialize.
            // Year 1000 is valid as DATE but overflows the hidden equality key's TIMESTAMP_NS.
            write_topn_evolution_file(path, ascending, false, hidden_delete ? -354285 : 2932897);
            const auto delete_path = (dir / (suffix + "_delete.parquet")).string();
            if (hidden_delete) {
                const auto type = arrow::timestamp(arrow::TimeUnit::NANO);
                arrow::TimestampBuilder builder(type, arrow::default_memory_pool());
                ASSERT_TRUE(builder.Append(1'704'067'200'000'000'000L).ok());
                const auto field = arrow::field(
                        "ts", type, true, arrow::key_value_metadata({"PARQUET:field_id"}, {"2"}));
                const auto table =
                        arrow::Table::Make(arrow::schema({field}), {builder.Finish().ValueOrDie()});
                auto out = arrow::io::FileOutputStream::Open(delete_path).ValueOrDie();
                ASSERT_TRUE(
                        ::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out, 1)
                                .ok());
                ASSERT_TRUE(out->Close().ok());
            }
            for (const int scenario : {0, 1, 2}) {
                SCOPED_TRACE(testing::Message()
                             << "hidden_delete=" << hidden_delete << " ascending=" << ascending
                             << " scenario=" << scenario);
                TQueryOptions options;
                options.__set_enable_strict_cast(true);
                options.__set_batch_size(4096);
                RuntimeState state {options, TQueryGlobals()};
                // No filter and a reversed TopN both need RG0; only scenario 1 excludes it.
                auto filter =
                        make_evolution_topn_filter(&state, scenario == 2 ? !ascending : ascending);
                auto columns = topn_evolution_columns(false, false);
                if (hidden_delete) {
                    columns.resize(1);
                }
                auto scan_params = make_local_parquet_scan_params();
                scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
                RuntimeProfile profile("topn_conversion");
                ShardedKVCache cache(1);
                iceberg::IcebergTableReader reader;
                ASSERT_TRUE(reader.init({.projected_columns = columns,
                                         .conjuncts = scenario == 0
                                                              ? VExprContextSPtrs {}
                                                              : VExprContextSPtrs {filter.conjunct},
                                         .format = FileFormat::PARQUET,
                                         .scan_params = &scan_params,
                                         .io_ctx = nullptr,
                                         .runtime_state = &state,
                                         .scanner_profile = &profile})
                                    .ok());
                auto split = build_split_options(path);
                split.cache = &cache;
                if (hidden_delete) {
                    split.current_range.__set_table_format_params(make_iceberg_table_format_desc(
                            path, {make_iceberg_equality_delete_file(delete_path, {2})}, 3));
                }
                ASSERT_TRUE(reader.prepare_split(split).ok());
                bool eos = false;
                size_t rows = 0;
                Status status;
                while (!eos) {
                    Block block = build_table_block(columns);
                    status = reader.get_block(&block, &eos);
                    if (!status.ok()) {
                        break;
                    }
                    rows += block.rows();
                    if (block.rows() > 0) {
                        expect_int32_column_values(
                                *block.get_by_position(0).column,
                                std::vector<int32_t>(block.rows(), ascending ? 0 : 100));
                        ASSERT_TRUE(
                                filter.query_context->get_runtime_predicate(10)
                                        .update(Field::create_field<TYPE_INT>(ascending ? 0 : 100))
                                        .ok());
                    }
                }
                EXPECT_EQ(status.ok(), scenario == 1) << status;
                if (scenario == 1) {
                    EXPECT_EQ(rows, 3 * 4096);
                } else {
                    EXPECT_NE(status.to_string().find(
                                      hidden_delete ? "TIMESTAMP_NS overflow"
                                                    : "Parquet DATE value is out of range"),
                              std::string::npos)
                            << status;
                }
                ASSERT_TRUE(reader.close().ok());
                filter.conjunct->close();
            }
        }
    }
}

TEST(IcebergV2ReaderTest, DatePromotionToNanosecondsChecksOverflow) {
    const auto test_dir = std::filesystem::temp_directory_path() / "iceberg_date_promotion";
    std::filesystem::create_directories(test_dir);
    for (bool nested : {false, true}) {
        for (int32_t days : {19724, -354285}) {
            const auto file_path =
                    (test_dir / (std::to_string(days) + (nested ? "_nested.parquet" : ".parquet")))
                            .string();
            // Read an actual retained DATE file, including NULL, through field-ID schema evolution.
            auto field = arrow::field("ts", arrow::date32(), true,
                                      arrow::key_value_metadata({"PARQUET:field_id"}, {"1"}));
            arrow::Date32Builder builder;
            ASSERT_TRUE(builder.Append(days).ok());
            ASSERT_TRUE(builder.AppendNull().ok());
            std::shared_ptr<arrow::Array> values = builder.Finish().ValueOrDie();
            if (nested) {
                values = arrow::StructArray::Make({values}, {field}).ValueOrDie();
                field = arrow::field("outer", values->type(), false,
                                     arrow::key_value_metadata({"PARQUET:field_id"}, {"2"}));
            }
            auto out = arrow::io::FileOutputStream::Open(file_path).ValueOrDie();
            ASSERT_TRUE(::parquet::arrow::WriteTable(
                                *arrow::Table::Make(arrow::schema({field}), {values}),
                                arrow::default_memory_pool(), out, 2)
                                .ok());
            ASSERT_TRUE(out->Close().ok());
            for (bool filtered : {false, true}) {
                if (nested && filtered) {
                    continue;
                }
                for (bool strict : {false, true}) {
                    SCOPED_TRACE(testing::Message()
                                 << "nested=" << nested << " days=" << days << " strict=" << strict
                                 << " filtered=" << filtered);
                    TQueryOptions query_options;
                    query_options.__set_enable_strict_cast(strict);
                    RuntimeState state {query_options, TQueryGlobals()};
                    const auto type = make_nullable(std::make_shared<DataTypeTimeStampNs>());
                    std::vector<ColumnDefinition> columns {make_table_column(1, "ts", type)};
                    if (nested) {
                        auto outer = make_table_column(
                                2, "outer",
                                std::make_shared<DataTypeStruct>(DataTypes {type}, Strings {"ts"}));
                        outer.children = columns;
                        columns = {outer};
                    }
                    auto scan_params = make_local_parquet_scan_params();
                    RuntimeProfile profile("date_promotion");
                    io::FileReaderStats file_reader_stats;
                    io::FileCacheStatistics file_cache_stats;
                    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
                    doris::format::iceberg::IcebergTableReader reader;
                    VExprContextSPtrs conjuncts;
                    if (filtered) {
                        // A file-local filter must not hide overflow before strict table materialization.
                        auto predicate = table_function_expr(
                                "is_not_null_pred", std::make_shared<DataTypeUInt8>(), {type});
                        predicate->add_child(VSlotRef::create_shared(0, 0, 0, type, "ts"));
                        conjuncts.push_back(prepared_conjunct(&state, predicate));
                    }
                    ASSERT_TRUE(reader.init({.projected_columns = columns,
                                             .conjuncts = conjuncts,
                                             .format = FileFormat::PARQUET,
                                             .scan_params = &scan_params,
                                             .io_ctx = io_ctx,
                                             .runtime_state = &state,
                                             .scanner_profile = &profile})
                                        .ok());
                    ASSERT_TRUE(reader.prepare_split(build_split_options(file_path)).ok());
                    Block block = build_table_block(columns);
                    bool eos = false;
                    const auto status = reader.get_block(&block, &eos);
                    if (days < 0) {
                        EXPECT_FALSE(status.ok())
                                << "out-of-range promotion must fail even in non-strict mode";
                        EXPECT_NE(status.to_string().find("TIMESTAMP_NS overflow"),
                                  std::string::npos)
                                << status;
                    } else {
                        ASSERT_TRUE(status.ok()) << status;
                        ASSERT_EQ(block.rows(), filtered ? 1 : 2);
                        const IColumn* result = block.get_by_position(0).column.get();
                        if (nested) {
                            result = &assert_cast<const ColumnStruct&>(
                                              expect_not_null_table_column(block, 0))
                                              .get_column(0);
                        }
                        EXPECT_EQ(type->to_string(*result, 0), "2024-01-02 00:00:00.000000000");
                        if (!filtered) {
                            EXPECT_TRUE(result->is_null_at(1));
                        }
                    }
                    ASSERT_TRUE(reader.close().ok());
                }
            }
        }
    }
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, AnnotateBuildsTypedNestedInitialDefault) {
    const auto int_type = make_nullable(std::make_shared<DataTypeInt32>());
    const auto struct_type = make_nullable(
            std::make_shared<DataTypeStruct>(DataTypes {int_type}, Strings {"added"}));

    auto added_field = external_schema_field(
            "added", 2, {}, "7", external_primitive_type(TPrimitiveType::INT), false, false);
    auto struct_field = external_struct_schema_field("s", 1, {std::move(added_field)}, true);
    TFileScanRangeParams scan_params;
    scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
    scan_params.__set_current_schema_id(100);
    scan_params.__set_history_schema_info({external_schema(100, {std::move(struct_field)})});

    ColumnDefinition column;
    column.name = "s";
    column.type = struct_type;
    ProjectedColumnBuildContext context {.scan_params = &scan_params};
    doris::format::iceberg::IcebergTableReader reader;
    const auto status = reader.annotate_projected_column(TFileScanSlotInfo(), &context, &column);
    ASSERT_TRUE(status.ok()) << status;

    ASSERT_TRUE(column.is_optional.has_value());
    EXPECT_TRUE(*column.is_optional);
    ASSERT_TRUE(context.schema_column.has_value());
    ASSERT_EQ(context.schema_column->children.size(), 1);
    const auto& added = context.schema_column->children[0];
    ASSERT_TRUE(added.is_optional.has_value());
    EXPECT_FALSE(*added.is_optional);
    ASSERT_NE(added.default_expr, nullptr);
    const auto* literal = dynamic_cast<const VLiteral*>(added.default_expr->root().get());
    ASSERT_NE(literal, nullptr);
    ASSERT_TRUE(literal->get_data_type()->equals(*int_type));
    Field value;
    literal->get_column_ptr()->get(0, value);
    EXPECT_EQ(value.get<TYPE_INT>(), 7);
}

TEST(IcebergV2ReaderTest, PreparesIcebergNonFiniteInitialDefaults) {
    struct Case {
        DataTypePtr type;
        std::string value;
        bool nan;
        bool negative;
    };
    std::vector<Case> cases {{std::make_shared<DataTypeFloat32>(), "NaN", true, false},
                             {std::make_shared<DataTypeFloat64>(), "Infinity", false, false},
                             {std::make_shared<DataTypeFloat64>(), "-Infinity", false, true}};
    for (const auto& test_case : cases) {
        ColumnDefinition column;
        column.name = "value";
        column.type = test_case.type;
        column.initial_default_value = test_case.value;
        ASSERT_TRUE(iceberg::prepare_iceberg_initial_default_exprs(&column).ok());
        ASSERT_NE(column.default_expr, nullptr);
        const auto* literal = dynamic_cast<const VLiteral*>(column.default_expr->root().get());
        ASSERT_NE(literal, nullptr);
        Field value;
        literal->get_column_ptr()->get(0, value);
        const double number = test_case.type->get_primitive_type() == TYPE_FLOAT
                                      ? value.get<TYPE_FLOAT>()
                                      : value.get<TYPE_DOUBLE>();
        EXPECT_EQ(std::isnan(number), test_case.nan);
        if (!test_case.nan) {
            EXPECT_TRUE(std::isinf(number));
            EXPECT_EQ(std::signbit(number), test_case.negative);
        }
    }
}

TEST(IcebergV2ReaderTest, AnnotateVariantInitialDefaultMustBeNull) {
    const auto variant_type = make_nullable(std::make_shared<DataTypeVariantV2>());

    auto optional_variant =
            external_schema_field("payload", 1, {}, std::nullopt,
                                  external_primitive_type(TPrimitiveType::VARIANT), false, true);
    TFileScanRangeParams valid_scan_params;
    valid_scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
    valid_scan_params.__set_current_schema_id(100);
    valid_scan_params.__set_history_schema_info(
            {external_schema(100, {std::move(optional_variant)})});

    ColumnDefinition valid_column;
    valid_column.name = "payload";
    valid_column.type = variant_type;
    ProjectedColumnBuildContext valid_context {.scan_params = &valid_scan_params};
    doris::format::iceberg::IcebergTableReader reader;
    ASSERT_TRUE(reader.annotate_projected_column(TFileScanSlotInfo(), &valid_context, &valid_column)
                        .ok());
    ASSERT_TRUE(valid_column.is_optional.has_value());
    EXPECT_TRUE(*valid_column.is_optional);
    EXPECT_FALSE(valid_column.initial_default_value.has_value());
    EXPECT_EQ(valid_column.default_expr, nullptr);
    ASSERT_TRUE(valid_context.schema_column.has_value());
    EXPECT_TRUE(valid_context.schema_column->type->equals(*variant_type));

    auto malformed_variant =
            external_schema_field("payload", 1, {}, "{\"source\":\"malformed\"}",
                                  external_primitive_type(TPrimitiveType::VARIANT), false, true);
    TFileScanRangeParams malformed_scan_params;
    malformed_scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
    malformed_scan_params.__set_current_schema_id(101);
    malformed_scan_params.__set_history_schema_info(
            {external_schema(101, {std::move(malformed_variant)})});

    ColumnDefinition malformed_column;
    malformed_column.name = "payload";
    malformed_column.type = variant_type;
    ProjectedColumnBuildContext malformed_context {.scan_params = &malformed_scan_params};
    const auto status = reader.annotate_projected_column(TFileScanSlotInfo(), &malformed_context,
                                                         &malformed_column);
    EXPECT_TRUE(status.is<ErrorCode::INVALID_ARGUMENT>()) << status;
    EXPECT_NE(status.to_string().find("VARIANT initial-default for field 'payload' must be NULL"),
              std::string::npos)
            << status;
}

TEST(IcebergV2ReaderTest, StructInitialDefaultKeepsVariantChildNull) {
    const auto variant_type = make_nullable(std::make_shared<DataTypeVariantV2>());
    const auto struct_type = make_nullable(
            std::make_shared<DataTypeStruct>(DataTypes {variant_type}, Strings {"payload"}));

    auto optional_variant =
            external_schema_field("payload", 2, {}, std::nullopt,
                                  external_primitive_type(TPrimitiveType::VARIANT), false, true);
    auto valid_struct =
            external_struct_schema_field("event", 1, {std::move(optional_variant)}, true, "{}");
    TFileScanRangeParams valid_scan_params;
    valid_scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
    valid_scan_params.__set_current_schema_id(100);
    valid_scan_params.__set_history_schema_info({external_schema(100, {std::move(valid_struct)})});

    ColumnDefinition valid_column;
    valid_column.name = "event";
    valid_column.type = struct_type;
    ProjectedColumnBuildContext valid_context {.scan_params = &valid_scan_params};
    doris::format::iceberg::IcebergTableReader reader;
    ASSERT_TRUE(reader.annotate_projected_column(TFileScanSlotInfo(), &valid_context, &valid_column)
                        .ok());
    ASSERT_NE(valid_column.default_expr, nullptr);
    const auto* literal = dynamic_cast<const VLiteral*>(valid_column.default_expr->root().get());
    ASSERT_NE(literal, nullptr);
    EXPECT_TRUE(literal->get_data_type()->equals(*struct_type));
    Field value;
    literal->get_column_ptr()->get(0, value);
    const auto& struct_fields = value.get<TYPE_STRUCT>();
    ASSERT_EQ(struct_fields.size(), 1);
    EXPECT_TRUE(struct_fields[0].is_null());

    auto malformed_variant =
            external_schema_field("payload", 2, {}, std::nullopt,
                                  external_primitive_type(TPrimitiveType::VARIANT), false, true);
    auto malformed_struct = external_struct_schema_field(
            "event", 1, {std::move(malformed_variant)}, true, "{\"2\":{\"source\":\"malformed\"}}");
    TFileScanRangeParams malformed_scan_params;
    malformed_scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
    malformed_scan_params.__set_current_schema_id(101);
    malformed_scan_params.__set_history_schema_info(
            {external_schema(101, {std::move(malformed_struct)})});

    ColumnDefinition malformed_column;
    malformed_column.name = "event";
    malformed_column.type = struct_type;
    ProjectedColumnBuildContext malformed_context {.scan_params = &malformed_scan_params};
    const auto status = reader.annotate_projected_column(TFileScanSlotInfo(), &malformed_context,
                                                         &malformed_column);
    EXPECT_TRUE(status.is<ErrorCode::INVALID_ARGUMENT>()) << status;
    EXPECT_NE(status.to_string().find("VARIANT initial-default for field 'payload' must be NULL"),
              std::string::npos)
            << status;
}

TEST(IcebergV2ReaderTest, AnnotateBuildsComplexInitialDefaults) {
    const auto required_int_type = std::make_shared<DataTypeInt32>();
    const auto optional_string_type = make_nullable(std::make_shared<DataTypeString>());
    const auto struct_type = make_nullable(
            std::make_shared<DataTypeStruct>(DataTypes {required_int_type, optional_string_type},
                                             Strings {"required_added", "optional_added"}));
    const auto list_type = make_nullable(std::make_shared<DataTypeArray>(optional_string_type));
    const auto map_type = make_nullable(std::make_shared<DataTypeMap>(
            std::make_shared<DataTypeString>(), std::make_shared<DataTypeInt32>()));

    auto required_added =
            external_schema_field("required_added", 2, {}, "7",
                                  external_primitive_type(TPrimitiveType::INT), false, false);
    auto optional_added =
            external_schema_field("optional_added", 3, {}, std::nullopt,
                                  external_primitive_type(TPrimitiveType::STRING), false, true);
    auto struct_field = external_struct_schema_field(
            "struct_default", 1, {std::move(required_added), std::move(optional_added)}, true,
            "{}");

    auto list_element =
            external_schema_field("element", 5, {}, std::nullopt,
                                  external_primitive_type(TPrimitiveType::STRING), false, true);
    auto list_field = external_schema_field("list_default", 4, {}, "[\"alpha\",null]", std::nullopt,
                                            false, true);
    schema::external::TArrayField array_metadata;
    array_metadata.__set_item_field(std::move(list_element));
    list_field.field_ptr->nestedField.__set_array_field(std::move(array_metadata));
    list_field.field_ptr->__isset.nestedField = true;

    std::string binary_key_hex = "0011223344556677";
    binary_key_hex.append("8899aabbccddeeff0011");
    auto map_key =
            external_schema_field("key", 7, {}, std::nullopt,
                                  external_primitive_type(TPrimitiveType::STRING), true, false);
    auto map_value =
            external_schema_field("value", 8, {}, std::nullopt,
                                  external_primitive_type(TPrimitiveType::INT), false, false);
    auto map_field = external_schema_field("map_default", 6, {},
                                           "{\"keys\":[\"" + binary_key_hex + "\"],\"values\":[9]}",
                                           std::nullopt, false, true);
    schema::external::TMapField map_metadata;
    map_metadata.__set_key_field(std::move(map_key));
    map_metadata.__set_value_field(std::move(map_value));
    map_field.field_ptr->nestedField.__set_map_field(std::move(map_metadata));
    map_field.field_ptr->__isset.nestedField = true;

    TFileScanRangeParams scan_params;
    scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
    scan_params.__set_current_schema_id(100);
    scan_params.__set_history_schema_info({external_schema(
            100, {std::move(struct_field), std::move(list_field), std::move(map_field)})});

    auto build_literal = [&](const std::string& name, const DataTypePtr& type) -> VExprSPtr {
        ColumnDefinition column;
        column.name = name;
        column.type = type;
        ProjectedColumnBuildContext context {.scan_params = &scan_params};
        doris::format::iceberg::IcebergTableReader reader;
        const auto status =
                reader.annotate_projected_column(TFileScanSlotInfo(), &context, &column);
        if (!status.ok()) {
            ADD_FAILURE() << status;
            return nullptr;
        }
        if (column.default_expr == nullptr) {
            ADD_FAILURE() << "Missing default expression for " << name;
            return nullptr;
        }
        return column.default_expr->root();
    };

    const auto struct_expr = build_literal("struct_default", struct_type);
    const auto* struct_literal = dynamic_cast<const VLiteral*>(struct_expr.get());
    ASSERT_NE(struct_literal, nullptr);
    Field struct_value;
    struct_literal->get_column_ptr()->get(0, struct_value);
    const auto& struct_fields = struct_value.get<TYPE_STRUCT>();
    ASSERT_EQ(struct_fields.size(), 2);
    EXPECT_EQ(struct_fields[0].get<TYPE_INT>(), 7);
    EXPECT_TRUE(struct_fields[1].is_null());

    const auto pruned_struct_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {required_int_type}, Strings {"required_added"}));
    const auto pruned_struct_expr = build_literal("struct_default", pruned_struct_type);
    const auto* pruned_struct_literal = dynamic_cast<const VLiteral*>(pruned_struct_expr.get());
    ASSERT_NE(pruned_struct_literal, nullptr);
    Field pruned_struct_value;
    pruned_struct_literal->get_column_ptr()->get(0, pruned_struct_value);
    const auto& pruned_struct_fields = pruned_struct_value.get<TYPE_STRUCT>();
    ASSERT_EQ(pruned_struct_fields.size(), 1);
    EXPECT_EQ(pruned_struct_fields[0].get<TYPE_INT>(), 7);

    const auto reordered_struct_type = make_nullable(
            std::make_shared<DataTypeStruct>(DataTypes {optional_string_type, required_int_type},
                                             Strings {"optional_added", "required_added"}));
    const auto reordered_struct_expr = build_literal("struct_default", reordered_struct_type);
    const auto* reordered_struct_literal =
            dynamic_cast<const VLiteral*>(reordered_struct_expr.get());
    ASSERT_NE(reordered_struct_literal, nullptr);
    Field reordered_struct_value;
    reordered_struct_literal->get_column_ptr()->get(0, reordered_struct_value);
    const auto& reordered_struct_fields = reordered_struct_value.get<TYPE_STRUCT>();
    ASSERT_EQ(reordered_struct_fields.size(), 2);
    EXPECT_TRUE(reordered_struct_fields[0].is_null());
    EXPECT_EQ(reordered_struct_fields[1].get<TYPE_INT>(), 7);

    const auto list_expr = build_literal("list_default", list_type);
    const auto* list_literal = dynamic_cast<const VLiteral*>(list_expr.get());
    ASSERT_NE(list_literal, nullptr);
    Field list_value;
    list_literal->get_column_ptr()->get(0, list_value);
    const auto& list_fields = list_value.get<TYPE_ARRAY>();
    ASSERT_EQ(list_fields.size(), 2);
    EXPECT_EQ(list_fields[0].get<TYPE_STRING>(), "alpha");
    EXPECT_TRUE(list_fields[1].is_null());

    const auto map_expr = build_literal("map_default", map_type);
    const auto* map_literal = dynamic_cast<const VLiteral*>(map_expr.get());
    ASSERT_NE(map_literal, nullptr);
    Field decoded_map_value;
    map_literal->get_column_ptr()->get(0, decoded_map_value);
    const auto& map_fields = decoded_map_value.get<TYPE_MAP>();
    ASSERT_EQ(map_fields.size(), 2);
    const auto& keys = map_fields[0].get<TYPE_ARRAY>();
    const auto& values = map_fields[1].get<TYPE_ARRAY>();
    ASSERT_EQ(keys.size(), 1);
    ASSERT_EQ(values.size(), 1);
    const std::string expected_binary_key(
            "\x00\x11\x22\x33\x44\x55\x66\x77\x88\x99\xaa\xbb\xcc\xdd\xee\xff\x00\x11", 18);
    EXPECT_EQ(keys[0].get<TYPE_STRING>(), expected_binary_key);
    EXPECT_EQ(values[0].get<TYPE_INT>(), 9);
}

TEST(IcebergV2ReaderTest, AnnotateBuildsComplexDoubleDefaultAtFullPrecision) {
    const auto double_type = std::make_shared<DataTypeFloat64>();
    const auto list_type = make_nullable(std::make_shared<DataTypeArray>(double_type));
    auto list_element =
            external_schema_field("element", 2, {}, std::nullopt,
                                  external_primitive_type(TPrimitiveType::DOUBLE), false, false);
    auto list_field = external_schema_field("values", 1, {}, "[0.18172760479972437302]",
                                            std::nullopt, false, true);
    schema::external::TArrayField array_metadata;
    array_metadata.__set_item_field(std::move(list_element));
    list_field.field_ptr->nestedField.__set_array_field(std::move(array_metadata));
    list_field.field_ptr->__isset.nestedField = true;
    TFileScanRangeParams scan_params;
    scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
    scan_params.__set_current_schema_id(100);
    scan_params.__set_history_schema_info({external_schema(100, {std::move(list_field)})});

    ColumnDefinition column;
    column.name = "values";
    column.type = list_type;
    ProjectedColumnBuildContext context {.scan_params = &scan_params};
    doris::format::iceberg::IcebergTableReader reader;
    const auto status = reader.annotate_projected_column(TFileScanSlotInfo(), &context, &column);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_NE(column.default_expr, nullptr);
    const auto* literal = dynamic_cast<const VLiteral*>(column.default_expr->root().get());
    ASSERT_NE(literal, nullptr);
    const auto literal_column = literal->get_column_ptr()->convert_to_full_column_if_const();
    const auto& nullable = assert_cast<const ColumnNullable&>(*literal_column);
    ASSERT_FALSE(nullable.is_null_at(0));
    const auto& array = assert_cast<const ColumnArray&>(nullable.get_nested_column());
    const auto& nullable_doubles = assert_cast<const ColumnNullable&>(array.get_data());
    ASSERT_FALSE(nullable_doubles.is_null_at(0));
    const auto& doubles = assert_cast<const ColumnFloat64&>(nullable_doubles.get_nested_column());
    ASSERT_EQ(doubles.size(), 1);
    EXPECT_EQ(std::bit_cast<uint64_t>(doubles.get_data()[0]), 0x3fc742d9a3b296dcULL);
}

TEST(IcebergV2ReaderTest, ComplexInitialDefaultPrefersExactChildNameOverAlias) {
    const auto int_type = std::make_shared<DataTypeInt32>();
    const auto struct_type =
            make_nullable(std::make_shared<DataTypeStruct>(DataTypes {int_type}, Strings {"a"}));

    auto renamed_child = external_schema_field(
            "b", 2, {"a"}, "11", external_primitive_type(TPrimitiveType::INT), false, false);
    auto reused_name_child = external_schema_field(
            "a", 3, {}, "7", external_primitive_type(TPrimitiveType::INT), false, false);
    auto struct_field = external_struct_schema_field(
            "s", 1, {std::move(renamed_child), std::move(reused_name_child)}, true, "{}");
    TFileScanRangeParams scan_params;
    scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
    scan_params.__set_current_schema_id(100);
    scan_params.__set_history_schema_info({external_schema(100, {std::move(struct_field)})});

    ColumnDefinition column;
    column.name = "s";
    column.type = struct_type;
    ProjectedColumnBuildContext context {.scan_params = &scan_params};
    doris::format::iceberg::IcebergTableReader reader;
    const auto status = reader.annotate_projected_column(TFileScanSlotInfo(), &context, &column);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_NE(column.default_expr, nullptr);

    const auto* literal = dynamic_cast<const VLiteral*>(column.default_expr->root().get());
    ASSERT_NE(literal, nullptr);
    Field value;
    literal->get_column_ptr()->get(0, value);
    const auto& struct_fields = value.get<TYPE_STRUCT>();
    ASSERT_EQ(struct_fields.size(), 1);
    EXPECT_EQ(struct_fields[0].get<TYPE_INT>(), 7);
}

TEST(IcebergV2ReaderTest, AnnotateOwnsDecodedVarbinaryInitialDefault) {
    const std::string expected = "0123456789abcdef0123456789abcdef";
    std::string encoded;
    base64_encode(expected, &encoded);

    auto binary_field = external_schema_field("payload", 7, {}, encoded, std::nullopt, true, true);
    TFileScanRangeParams scan_params;
    scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
    scan_params.__set_current_schema_id(100);
    scan_params.__set_history_schema_info({external_schema(100, {std::move(binary_field)})});

    const auto binary_type = make_nullable(std::make_shared<DataTypeVarbinary>());
    ColumnDefinition column;
    column.name = "payload";
    column.type = binary_type;
    ProjectedColumnBuildContext context {.scan_params = &scan_params};
    doris::format::iceberg::IcebergTableReader reader;
    const auto status = reader.annotate_projected_column(TFileScanSlotInfo(), &context, &column);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_NE(column.default_expr, nullptr);
    const auto* literal = dynamic_cast<const VLiteral*>(column.default_expr->root().get());
    ASSERT_NE(literal, nullptr);
    ASSERT_TRUE(literal->get_data_type()->equals(*binary_type));

    // Drop the schema tree and churn heap allocations after annotate returns. The literal must own
    // the decoded bytes rather than retaining StringView storage from the decoder's local string.
    context.schema_column.reset();
    std::vector<std::string> allocator_churn(4096, std::string(expected.size(), 'x'));
    ASSERT_FALSE(allocator_churn.empty());
    EXPECT_EQ(literal->get_column_ptr()->get_data_at(0).to_string(), expected);
}

TEST(IcebergV2ReaderTest, AnnotateClearsGenericNullDefaultForRequiredField) {
    auto field = external_schema_field("required_value", 9, {}, std::nullopt, std::nullopt, false,
                                       false);
    TFileScanRangeParams scan_params;
    scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
    scan_params.__set_current_schema_id(100);
    scan_params.__set_history_schema_info({external_schema(100, {std::move(field)})});

    const auto int_type = make_nullable(std::make_shared<DataTypeInt32>());
    ColumnDefinition column;
    column.name = "required_value";
    column.type = int_type;
    column.default_expr = VExprContext::create_shared(VLiteral::create_shared(int_type, Field()));
    ProjectedColumnBuildContext context {.scan_params = &scan_params};
    doris::format::iceberg::IcebergTableReader reader;
    const auto status = reader.annotate_projected_column(TFileScanSlotInfo(), &context, &column);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_TRUE(column.is_optional.has_value());
    EXPECT_FALSE(*column.is_optional);
    EXPECT_EQ(column.default_expr, nullptr);
}

TEST(IcebergV2ReaderTest, SemanticsV1PreservesGenericRequiredFieldFallback) {
    TFileScanRangeParams scan_params;
    scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_1);
    EXPECT_TRUE(supports_iceberg_scan_semantics_v1(&scan_params));
    EXPECT_FALSE(supports_iceberg_scan_semantics_v2(&scan_params));

    auto field = external_schema_field("required_value", 9, {}, std::nullopt, std::nullopt, false,
                                       false);
    scan_params.__set_current_schema_id(100);
    scan_params.__set_history_schema_info({external_schema(100, {std::move(field)})});
    const auto int_type = make_nullable(std::make_shared<DataTypeInt32>());
    ColumnDefinition column;
    column.name = "required_value";
    column.type = int_type;
    column.default_expr = VExprContext::create_shared(VLiteral::create_shared(int_type, Field()));
    ProjectedColumnBuildContext context {.scan_params = &scan_params};
    doris::format::iceberg::IcebergTableReader reader;

    const auto status = reader.annotate_projected_column(TFileScanSlotInfo(), &context, &column);

    ASSERT_TRUE(status.ok()) << status;
    EXPECT_NE(column.default_expr, nullptr);

    scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
    EXPECT_TRUE(supports_iceberg_scan_semantics_v1(&scan_params));
    EXPECT_TRUE(supports_iceberg_scan_semantics_v2(&scan_params));
}

TEST(IcebergV2ReaderTest, AnnotateRejectsMalformedInitialDefault) {
    auto field =
            external_schema_field("added_int", 10, {}, "not-an-int", std::nullopt, false, true);
    TFileScanRangeParams scan_params;
    scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
    scan_params.__set_current_schema_id(100);
    scan_params.__set_history_schema_info({external_schema(100, {std::move(field)})});

    ColumnDefinition column;
    column.name = "added_int";
    column.type = make_nullable(std::make_shared<DataTypeInt32>());
    ProjectedColumnBuildContext context {.scan_params = &scan_params};
    doris::format::iceberg::IcebergTableReader reader;

    const auto status = reader.annotate_projected_column(TFileScanSlotInfo(), &context, &column);

    ASSERT_FALSE(status.ok());
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
    projected_columns.push_back(make_synthesized_table_column(BeConsts::ICEBERG_FILE_PATH_COL,
                                                              std::make_shared<DataTypeString>()));
    projected_columns.push_back(make_synthesized_table_column(BeConsts::ICEBERG_ROW_POSITION_COL,
                                                              std::make_shared<DataTypeInt64>()));

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
    expect_string_column_values(*block.get_by_position(2).column,
                                {original_file_path, original_file_path});
    expect_int64_column_values(*block.get_by_position(3).column, {1, 2});

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, IcebergMetadataColumnsMaterializeForParquetAndOrc) {
    for (const auto file_format : {FileFormat::PARQUET, FileFormat::ORC}) {
        const auto format_name = file_format == FileFormat::PARQUET ? "parquet" : "orc";
        const auto test_dir = std::filesystem::temp_directory_path() /
                              ("doris_iceberg_metadata_columns_" + std::string(format_name));
        std::filesystem::remove_all(test_dir);
        std::filesystem::create_directories(test_dir);

        const auto file_path = (test_dir / ("split." + std::string(format_name))).string();
        if (file_format == FileFormat::PARQUET) {
            write_single_int_parquet_file(file_path, "id", {1, 2, 3}, 0);
        } else {
            write_single_int_orc_file(file_path, "id", {1, 2, 3}, 0);
        }

        std::vector<ColumnDefinition> projected_columns;
        projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));
        projected_columns.push_back(make_synthesized_table_column(
                BeConsts::ICEBERG_FILE_PATH_COL, std::make_shared<DataTypeString>()));
        projected_columns.push_back(make_synthesized_table_column(
                BeConsts::ICEBERG_ROW_POSITION_COL, std::make_shared<DataTypeInt64>()));

        RuntimeState state {TQueryOptions(), TQueryGlobals()};
        doris::format::iceberg::IcebergTableReader reader;
        auto scan_params = make_local_scan_params(file_format);
        ASSERT_TRUE(reader.init({
                                        .projected_columns = projected_columns,
                                        .conjuncts = {prepared_conjunct(
                                                &state, table_int32_greater_than_expr(0, 0, 1))},
                                        .format = file_format,
                                        .scan_params = &scan_params,
                                        .io_ctx = nullptr,
                                        .runtime_state = &state,
                                        .scanner_profile = nullptr,
                                })
                            .ok());

        auto split_options = build_split_options(file_path);
        split_options.current_split_format = file_format;
        const auto original_file_path = "s3://bucket/table/data/" + std::string(format_name) +
                                        "/original." + std::string(format_name);
        set_iceberg_rowid_params(&split_options, original_file_path, 17, R"({"part":"p1"})");
        ASSERT_TRUE(reader.prepare_split(split_options).ok());

        Block block = build_table_block(projected_columns);
        bool eos = false;
        ASSERT_TRUE(reader.get_block(&block, &eos).ok());
        ASSERT_FALSE(eos);
        ASSERT_EQ(block.rows(), 2);
        expect_int32_column_values(*block.get_by_position(0).column, {2, 3});
        expect_string_column_values(*block.get_by_position(1).column,
                                    {original_file_path, original_file_path});
        expect_int64_column_values(*block.get_by_position(2).column, {1, 2});

        ASSERT_TRUE(reader.close().ok());
        std::filesystem::remove_all(test_dir);
    }
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
    EXPECT_TRUE(status.is<ErrorCode::DATA_QUALITY_ERROR>());
    EXPECT_NE(status.to_string().find("descriptor misses"), std::string::npos);
    EXPECT_FALSE(has_delete_file);
}

TEST(IcebergV2ReaderTest, IcebergDeletionVectorRejectsInvalidRange) {
    TTableFormatFileDesc table_format_desc;
    TIcebergFileDesc iceberg_desc;
    iceberg_desc.__set_format_version(2);
    iceberg_desc.__set_delete_files({make_iceberg_deletion_vector("dv.bin", -1, 12)});
    table_format_desc.__set_iceberg_params(iceberg_desc);

    IcebergTableReaderDeleteFileTestHelper reader;
    DeleteFileDesc desc;
    bool has_delete_file = false;
    auto status = reader.parse_deletion_vector_file(table_format_desc, &desc, &has_delete_file);

    EXPECT_TRUE(status.is<ErrorCode::DATA_QUALITY_ERROR>());
    EXPECT_NE(status.to_string().find("offset must be non-negative"), std::string::npos);
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

// Iceberg treats a schema as ID-bearing when any physical data field has an ID. Keeping ID mode
// preserves authoritative matches even when a sibling was written without an ID.
TEST(IcebergV2ReaderTest, IcebergMappingModeUsesAnyDataColumnFieldId) {
    IcebergTableReaderMappingModeTestHelper reader;
    TFileScanRangeParams scan_params;
    scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_1);
    std::vector<ColumnDefinition> file_schema {
            make_file_column(1, "id", std::make_shared<DataTypeInt32>()),
            make_file_column(2, "name", std::make_shared<DataTypeString>()),
            global_rowid_column_definition(),
    };
    file_schema[1].identifier = Field {};

    EXPECT_EQ(reader.mapping_mode_for_schema(file_schema, &scan_params),
              TableColumnMappingMode::BY_FIELD_ID);

    file_schema[0].identifier = Field {};
    file_schema[0].children.emplace_back(
            make_file_column(3, "nested", std::make_shared<DataTypeInt32>()));
    EXPECT_EQ(reader.mapping_mode_for_schema(std::move(file_schema), &scan_params),
              TableColumnMappingMode::BY_FIELD_ID);
}

TEST(IcebergV2ReaderTest, IcebergLegacyPlanKeepsAllFieldIdsMappingRule) {
    IcebergTableReaderMappingModeTestHelper reader;
    TFileScanRangeParams old_fe_scan_params;
    std::vector<ColumnDefinition> file_schema {
            make_file_column(1, "a", std::make_shared<DataTypeInt32>()),
            make_file_column(2, "b", std::make_shared<DataTypeInt32>()),
    };
    file_schema[1].identifier = Field {};

    EXPECT_EQ(reader.mapping_mode_for_schema(std::move(file_schema), &old_fe_scan_params),
              TableColumnMappingMode::BY_NAME);

    auto nested = make_file_column(10, "s", std::make_shared<DataTypeInt32>());
    nested.children = {
            make_file_column(1, "a", std::make_shared<DataTypeInt32>()),
            make_file_column(2, "b", std::make_shared<DataTypeInt32>()),
    };
    nested.children[1].identifier = Field {};
    EXPECT_EQ(reader.mapping_mode_for_schema({std::move(nested)}, &old_fe_scan_params),
              TableColumnMappingMode::BY_NAME);
}

// Mapping-mode contract for an id-less file under v2 semantics: without a table-level name mapping
// the file stays on the BY_FIELD_ID path (DORIS-29685 stamps Iceberg's positional fallback ids on
// its top-level fields), and an authoritative table-level mapping switches the split to BY_NAME.
TEST(IcebergV2ReaderTest, IcebergV2IdlessFileMappingModeSelection) {
    IcebergTableReaderMappingModeTestHelper reader;
    TFileScanRangeParams scan_params;
    scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);

    auto file_column = make_file_column(1, "legacy_name", std::make_shared<DataTypeInt32>());
    file_column.identifier = Field {};
    auto table_column = make_table_column(1, "current_name", std::make_shared<DataTypeInt32>());

    EXPECT_EQ(reader.mapping_mode_for_schema({file_column}, &scan_params, {table_column}),
              TableColumnMappingMode::BY_FIELD_ID);

    table_column.has_name_mapping = true;
    table_column.name_mapping = {"legacy_name"};
    EXPECT_EQ(reader.mapping_mode_for_schema({std::move(file_column)}, &scan_params,
                                             {std::move(table_column)}),
              TableColumnMappingMode::BY_NAME);
}

// Exposes the condition-cache decision of the current split so a test can assert that the
// positional fallback keeps its splits out of the cache.
class IcebergConditionCacheProbeReader final : public doris::format::iceberg::IcebergTableReader {
public:
    void TEST_force_condition_cache_digest(uint64_t digest) {
        _condition_cache_digest = digest;
        _condition_cache_digest_covers_current_split = true;
    }
    bool TEST_condition_cache_enabled_for_current_split() const {
        DORIS_CHECK(_file_scan_request != nullptr);
        return _should_enable_condition_cache(*_file_scan_request);
    }
};

// DORIS-29685: the condition cache key covers the file identity plus the table-level
// predicate/session digest only, so it cannot tell two physical bindings of the same table column
// apart. Iceberg's positional fallback can change that binding when
// `schema.name-mapping.default` is added or removed without touching the data file, so those
// splits must stay out of the condition cache entirely (neither read nor written).
TEST(IcebergV2ReaderTest, PositionalIdFallbackSplitsDisableConditionCache) {
    const auto run_case = [](bool idless_file, int32_t table_field_id) {
        const auto test_dir = std::filesystem::temp_directory_path() /
                              (idless_file ? "doris_iceberg_cache_fallback_test"
                                           : "doris_iceberg_cache_field_id_test");
        std::filesystem::remove_all(test_dir);
        std::filesystem::create_directories(test_dir);
        const auto file_path = (test_dir / "split.parquet").string();
        if (idless_file) {
            write_idless_int_columns_parquet_file(file_path, {"legacy_id"}, {{7}});
        } else {
            write_single_int_parquet_file(file_path, "id", {7}, 0);
        }

        std::vector<ColumnDefinition> projected_columns;
        projected_columns.push_back(
                make_table_column(table_field_id, "id", std::make_shared<DataTypeInt32>()));

        RuntimeProfile profile("test_profile");
        RuntimeState state {TQueryOptions(), TQueryGlobals()};
        auto scan_params = make_local_parquet_scan_params();
        scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
        io::FileReaderStats file_reader_stats;
        io::FileCacheStatistics file_cache_stats;
        auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
        ShardedKVCache cache(1);
        IcebergConditionCacheProbeReader reader;
        ASSERT_TRUE(reader.init({.projected_columns = projected_columns,
                                 .conjuncts = {prepared_conjunct(
                                         &state, table_int32_greater_than_expr(0, 0, 0))},
                                 .format = FileFormat::PARQUET,
                                 .scan_params = &scan_params,
                                 .io_ctx = io_ctx,
                                 .runtime_state = &state,
                                 .scanner_profile = &profile})
                            .ok());

        auto split_options = build_split_options(file_path);
        split_options.cache = &cache;
        split_options.current_range.__set_table_format_params(
                make_iceberg_table_format_desc(file_path, {}));
        ASSERT_TRUE(reader.prepare_split(split_options).ok());

        Block block = build_table_block(projected_columns);
        bool eos = false;
        ASSERT_TRUE(reader.get_block(&block, &eos).ok());
        ASSERT_FALSE(eos);
        // Force a digest so the base decision is not disabled for unrelated reasons: without the
        // override an ID-less split would be cacheable here.
        reader.TEST_force_condition_cache_digest(0x5f5e100);
        EXPECT_EQ(reader.TEST_condition_cache_enabled_for_current_split(), !idless_file);

        ASSERT_TRUE(reader.close().ok());
        std::filesystem::remove_all(test_dir);
    };

    // A file carrying real field ids keeps the table column bound to the same physical column, so
    // the condition cache stays available.
    run_case(false, 0);
    // The positional fallback binds by physical ordinal, which the cache key cannot represent.
    run_case(true, 1);
}

// DORIS-29685: an add_files split (no field ids) cherry-picked back into an evolved table read as a
// whole all-NULL row because the table no longer carried a usable name mapping. Iceberg's Parquet
// reader binds such files by position (`ParquetSchemaUtil.addFallbackIds` + `pruneColumnsFallback`,
// `iceberg.netflix.unsafe-parquet-id-fallback.enabled` defaults to true), so Doris must do the same
// instead of silently materializing NULL. The physical names deliberately differ from the table
// names: the fallback is "file ordinal k == Iceberg field id k", never name based.
TEST(IcebergV2ReaderTest, IcebergIdlessParquetWithoutNameMappingUsesPositionalIds) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_iceberg_positional_id_fallback_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);
    const auto file_path = (test_dir / "split.parquet").string();
    write_idless_int_columns_parquet_file(file_path, {"legacy_id", "legacy_value"},
                                          {{1, 2, 3}, {10, 20, 30}});

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(1, "id", std::make_shared<DataTypeInt32>()));
    projected_columns.push_back(make_table_column(2, "value", std::make_shared<DataTypeInt32>()));

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::format::iceberg::IcebergTableReader reader;
    init_iceberg_reader(&reader, projected_columns, &scan_params, io_ctx, &state, &profile);

    auto split_options = build_split_options(file_path);
    split_options.cache = &cache;
    split_options.current_range.__set_table_format_params(
            make_iceberg_table_format_desc(file_path, {}));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);
    EXPECT_NE(reader.debug_string().find("positional_id_fallback=1"), std::string::npos);
    ASSERT_EQ(block.rows(), 3);
    expect_int32_column_values(*block.get_by_position(0).column, {1, 2, 3});
    expect_int32_column_values(*block.get_by_position(1).column, {10, 20, 30});

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

// Iceberg's fallback matches a table field with id k to the k-th physical top-level field, so a
// dropped (or re-added) field does not shift the remaining bindings the way a table-position rule
// would. Table ids 1 and 3 must therefore read file ordinals 1 and 3.
TEST(IcebergV2ReaderTest, IcebergPositionalFallbackMatchesFieldIdNotColumnPosition) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_iceberg_positional_id_field_id_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);
    const auto file_path = (test_dir / "split.parquet").string();
    write_idless_int_columns_parquet_file(file_path, {"first", "second", "third"},
                                          {{1, 2}, {10, 20}, {100, 200}});

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(1, "first", std::make_shared<DataTypeInt32>()));
    projected_columns.push_back(make_table_column(3, "third", std::make_shared<DataTypeInt32>()));

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::format::iceberg::IcebergTableReader reader;
    init_iceberg_reader(&reader, projected_columns, &scan_params, io_ctx, &state, &profile);

    auto split_options = build_split_options(file_path);
    split_options.cache = &cache;
    split_options.current_range.__set_table_format_params(
            make_iceberg_table_format_desc(file_path, {}));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);
    ASSERT_EQ(block.rows(), 2);
    expect_int32_column_values(*block.get_by_position(0).column, {1, 2});
    expect_int32_column_values(*block.get_by_position(1).column, {100, 200});

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

// The positional fallback only applies while the table exposes no usable name mapping. When an
// authoritative mapping is present, Iceberg applies it and prunes by id: the covered alias binds,
// and a field whose mapping says "absent from this file" stays NULL even though a physical column
// exists at that ordinal.
TEST(IcebergV2ReaderTest, IcebergIdlessParquetKeepsAuthoritativeNameMappingPath) {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_iceberg_positional_id_mapping_precedence_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);
    const auto file_path = (test_dir / "split.parquet").string();
    write_idless_int_columns_parquet_file(file_path, {"legacy_id", "legacy_value"},
                                          {{1, 2}, {10, 20}});

    auto mapped_column = make_table_column(1, "id", std::make_shared<DataTypeInt32>());
    mapped_column.has_name_mapping = true;
    mapped_column.name_mapping = {"legacy_id"};
    auto unmapped_column = make_table_column(2, "value", std::make_shared<DataTypeInt32>());
    unmapped_column.has_name_mapping = true;
    std::vector<ColumnDefinition> projected_columns {mapped_column, unmapped_column};

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::format::iceberg::IcebergTableReader reader;
    init_iceberg_reader(&reader, projected_columns, &scan_params, io_ctx, &state, &profile);

    auto split_options = build_split_options(file_path);
    split_options.cache = &cache;
    split_options.current_range.__set_table_format_params(
            make_iceberg_table_format_desc(file_path, {}));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);
    EXPECT_NE(reader.debug_string().find("positional_id_fallback=0"), std::string::npos);
    ASSERT_EQ(block.rows(), 2);
    expect_int32_column_values(*block.get_by_position(0).column, {1, 2});
    expect_nullable_int32_column_is_null(*block.get_by_position(1).column, 2);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

// An old-FE plan (scan semantics version 1) keeps the historical name fallback for id-less files;
// the positional fallback must stay opt-in for version 2 so a rolling upgrade cannot change it.
TEST(IcebergV2ReaderTest, IcebergLegacyPlanKeepsNameFallbackForIdlessParquet) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_iceberg_positional_id_legacy_fe_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);
    const auto file_path = (test_dir / "split.parquet").string();
    write_idless_int_columns_parquet_file(file_path, {"id", "value"}, {{1, 2}, {10, 20}});

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(1, "id", std::make_shared<DataTypeInt32>()));
    projected_columns.push_back(make_table_column(2, "value", std::make_shared<DataTypeInt32>()));

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
    split_options.current_range.__set_table_format_params(
            make_iceberg_table_format_desc(file_path, {}));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);
    EXPECT_NE(reader.debug_string().find("positional_id_fallback=0"), std::string::npos);
    ASSERT_EQ(block.rows(), 2);
    expect_int32_column_values(*block.get_by_position(0).column, {1, 2});
    expect_int32_column_values(*block.get_by_position(1).column, {10, 20});

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

// Iceberg stamps ordinal ids on top-level fields only, so an id-less nested file still materializes
// the struct root by position while its children stay NULL until a name mapping (or a rewrite that
// persists field ids) resolves them. Doris must not invent nested positions either.
TEST(IcebergV2ReaderTest, IcebergIdlessParquetStructChildStaysNull) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_iceberg_positional_id_nested_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);
    const auto file_path = (test_dir / "split.parquet").string();
    write_idless_struct_parquet_file(file_path, {7});

    const auto int_type = std::make_shared<DataTypeInt32>();
    auto inner_type = std::make_shared<DataTypeStruct>(DataTypes {int_type}, Strings {"inner"});
    auto outer = make_table_column(1, "s", inner_type);
    outer.children = {make_table_column(2, "inner", int_type)};
    std::vector<ColumnDefinition> projected_columns {outer};

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::format::iceberg::IcebergTableReader reader;
    init_iceberg_reader(&reader, projected_columns, &scan_params, io_ctx, &state, &profile);

    auto split_options = build_split_options(file_path);
    split_options.cache = &cache;
    split_options.current_range.__set_table_format_params(
            make_iceberg_table_format_desc(file_path, {}));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);
    EXPECT_NE(reader.debug_string().find("positional_id_fallback=1"), std::string::npos);
    ASSERT_EQ(block.rows(), 1);
    const auto& struct_column =
            assert_cast<const ColumnStruct&>(expect_not_null_table_column(block, 0));
    expect_nullable_int32_column_is_null(struct_column.get_column(0), 1);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

// Iceberg's hasIds contract is existential: one real field id keeps the file on the id path, so
// neither the name mapping nor the positional fallback may rescue the remaining id-less fields.
TEST(IcebergV2ReaderTest, IcebergPartiallyIdedParquetKeepsIdOnlyMatching) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_iceberg_positional_id_partial_ids_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);
    const auto file_path = (test_dir / "split.parquet").string();
    write_two_int_parquet_file(file_path, "id", {1, 2}, 0, "extra", {9, 9}, std::nullopt);

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));
    projected_columns.push_back(make_table_column(1, "extra", std::make_shared<DataTypeInt32>()));

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::format::iceberg::IcebergTableReader reader;
    init_iceberg_reader(&reader, projected_columns, &scan_params, io_ctx, &state, &profile);

    auto split_options = build_split_options(file_path);
    split_options.cache = &cache;
    split_options.current_range.__set_table_format_params(
            make_iceberg_table_format_desc(file_path, {}));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);
    EXPECT_NE(reader.debug_string().find("positional_id_fallback=0"), std::string::npos);
    ASSERT_EQ(block.rows(), 2);
    expect_int32_column_values(*block.get_by_position(0).column, {1, 2});
    expect_nullable_int32_column_is_null(*block.get_by_position(1).column, 2);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, VariantFormatGateUsesPhysicalFileMappings) {
    ColumnMapping missing_variant;
    missing_variant.table_type = make_nullable(std::make_shared<DataTypeVariantV2>());
    EXPECT_TRUE(doris::format::iceberg::IcebergTableReader::validate_variant_file_mappings(
                        FileFormat::ORC, {missing_variant})
                        .ok());

    ColumnMapping physical_variant = missing_variant;
    physical_variant.file_local_id = 0;
    const auto orc_status =
            doris::format::iceberg::IcebergTableReader::validate_variant_file_mappings(
                    FileFormat::ORC, {physical_variant});
    EXPECT_TRUE(orc_status.is<ErrorCode::NOT_IMPLEMENTED_ERROR>()) << orc_status;
    EXPECT_TRUE(doris::format::iceberg::IcebergTableReader::validate_variant_file_mappings(
                        FileFormat::PARQUET, {physical_variant})
                        .ok());

    ColumnMapping projected_struct;
    projected_struct.table_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {make_nullable(std::make_shared<DataTypeString>()),
                       make_nullable(std::make_shared<DataTypeVariantV2>())},
            Strings {"label", "payload"}));
    projected_struct.file_local_id = 0;
    ColumnMapping label;
    label.table_type = make_nullable(std::make_shared<DataTypeString>());
    label.file_local_id = 1;
    projected_struct.child_mappings = {label, missing_variant};
    EXPECT_TRUE(doris::format::iceberg::IcebergTableReader::validate_variant_file_mappings(
                        FileFormat::ORC, {projected_struct})
                        .ok());

    projected_struct.child_mappings[1] = physical_variant;
    const auto nested_orc_status =
            doris::format::iceberg::IcebergTableReader::validate_variant_file_mappings(
                    FileFormat::ORC, {projected_struct});
    EXPECT_TRUE(nested_orc_status.is<ErrorCode::NOT_IMPLEMENTED_ERROR>()) << nested_orc_status;
}

TEST(IcebergV2ReaderTest, OrcVariantCountWithPositionDeleteRequiresPhysicalReadSupport) {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_iceberg_orc_variant_count_position_delete_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.orc").string();
    const auto delete_file_path = (test_dir / "position-delete.parquet").string();
    write_single_int_orc_file(file_path, "payload", {1, 2, 3}, 0);
    write_position_delete_parquet_file(delete_file_path, {file_path}, {1});

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(
            make_table_column(0, "payload", std::make_shared<DataTypeVariantV2>()));

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_scan_params(FileFormat::ORC);
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    iceberg::IcebergTableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .conjuncts = {},
                                    .format = FileFormat::ORC,
                                    .scan_params = &scan_params,
                                    .io_ctx = io_ctx,
                                    .runtime_state = &state,
                                    .scanner_profile = &profile,
                                    .push_down_agg_type = TPushAggOp::type::COUNT,
                                    .push_down_count_columns = std::vector<GlobalIndex> {},
                            })
                        .ok());

    auto split_options = build_split_options(file_path);
    split_options.cache = &cache;
    split_options.current_split_format = FileFormat::ORC;
    split_options.current_range.__set_table_format_params(make_iceberg_table_format_desc(
            file_path, {make_iceberg_position_delete_file(delete_file_path)}));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    const auto status = reader.get_block(&block, &eos);
    EXPECT_TRUE(status.is<ErrorCode::NOT_IMPLEMENTED_ERROR>()) << status;
    EXPECT_NE(status.to_string().find("Iceberg Variant is supported only for Parquet files"),
              std::string::npos)
            << status;

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
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
                                    .push_down_count_columns = std::vector<GlobalIndex> {},
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
    // COUNT(*) retains a non-semantic carrier column, so the row count verifies that position
    // deletes forced a real scan without depending on placeholder carrier values.
    ASSERT_EQ(block.rows(), 2);

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
                                    // An explicit empty argument list is the new FE marker for
                                    // COUNT(*)/COUNT(1); nullopt intentionally exercises fallback.
                                    .push_down_count_columns = std::vector<GlobalIndex> {},
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

TEST(IcebergV2ReaderTest, IcebergNestedEqualityDeleteFiltersCurrentAndDroppedFields) {
    const auto run_case = [](FileFormat file_format, bool dropped_from_current,
                             bool parent_optional = true) {
        const bool is_parquet = file_format == FileFormat::PARQUET;
        const std::string format_name = is_parquet ? "parquet" : "orc";
        const std::string schema_name = dropped_from_current ? "dropped" : "current";
        const auto test_dir =
                std::filesystem::temp_directory_path() /
                ("doris_v2_nested_equality_delete_" + format_name + "_" + schema_name);
        std::filesystem::remove_all(test_dir);
        std::filesystem::create_directories(test_dir);
        const auto file_path = (test_dir / ("split." + format_name)).string();
        const auto delete_file_path = (test_dir / ("equality-delete." + format_name)).string();
        if (is_parquet) {
            const std::vector<bool> data_parent_nulls =
                    parent_optional ? std::vector<bool> {false, true, false}
                                    : std::vector<bool> {false, false, false};
            const std::vector<int32_t> delete_values =
                    parent_optional ? std::vector<int32_t> {0} : std::vector<int32_t> {20};
            const std::vector<bool> delete_parent_nulls = {parent_optional};
            write_nested_equality_parquet_file(file_path, {1, 2, 3}, {10, 20, 30},
                                               data_parent_nulls, parent_optional);
            write_nested_equality_parquet_file(delete_file_path, {}, delete_values,
                                               delete_parent_nulls, parent_optional);
        } else {
            write_nested_equality_orc_file(file_path, {1, 2, 3}, {10, 20, 30},
                                           {false, true, false});
            write_nested_equality_orc_file(delete_file_path, {}, {0}, {true});
        }

        const auto make_payload_field = [] {
            return external_struct_schema_field(
                    "payload", 1,
                    {external_schema_field("existing", 2, {}, std::nullopt,
                                           external_primitive_type(TPrimitiveType::INT))});
        };
        std::vector<schema::external::TFieldPtr> current_fields = {external_schema_field("id", 0)};
        if (!dropped_from_current) {
            current_fields.push_back(make_payload_field());
        }
        std::vector<schema::external::TSchema> history = {
                external_schema(100, std::move(current_fields))};
        if (dropped_from_current) {
            history.push_back(
                    external_schema(99, {external_schema_field("id", 0), make_payload_field()}));
        }

        std::vector<ColumnDefinition> projected_columns;
        projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));
        auto scan_params = make_local_scan_params(file_format);
        scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
        scan_params.__set_current_schema_id(100);
        scan_params.__set_history_schema_info(history);

        RuntimeProfile profile("test_profile");
        RuntimeState state {TQueryOptions(), TQueryGlobals()};
        io::FileReaderStats file_reader_stats;
        io::FileCacheStatistics file_cache_stats;
        auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
        ShardedKVCache cache(1);
        doris::format::iceberg::IcebergTableReader reader;
        init_iceberg_reader(&reader, projected_columns, &scan_params, io_ctx, &state, &profile,
                            file_format);

        auto split_options = build_split_options(file_path);
        split_options.cache = &cache;
        split_options.current_split_format = file_format;
        const auto thrift_file_format =
                is_parquet ? TFileFormatType::FORMAT_PARQUET : TFileFormatType::FORMAT_ORC;
        split_options.current_range.__set_table_format_params(make_iceberg_table_format_desc(
                file_path,
                {make_iceberg_equality_delete_file(delete_file_path, {2}, thrift_file_format)}, 3));
        ASSERT_TRUE(reader.prepare_split(split_options).ok());
        EXPECT_EQ(read_iceberg_ids(&reader, projected_columns), std::vector<int32_t>({1, 3}));
        ASSERT_TRUE(reader.close().ok());
        std::filesystem::remove_all(test_dir);
    };

    for (const auto file_format : {FileFormat::PARQUET, FileFormat::ORC}) {
        run_case(file_format, false);
        run_case(file_format, true);
    }
    run_case(FileFormat::PARQUET, false, false);
}

// Keep the Parquet/ORC setup identical because both readers must preserve the projected child
// ordinal when an equality-delete key widens the same struct root after mapper localization.
// NOLINTNEXTLINE(readability-function-cognitive-complexity,readability-function-size)
TEST(IcebergV2ReaderTest, IcebergNestedEqualityDeletePreservesVisibleSiblingProjection) {
    const auto run_case = [](FileFormat file_format) {
        const bool is_parquet = file_format == FileFormat::PARQUET;
        const std::string format_name = is_parquet ? "parquet" : "orc";
        const auto test_dir = std::filesystem::temp_directory_path() /
                              ("doris_v2_nested_equality_visible_sibling_" + format_name);
        std::filesystem::remove_all(test_dir);
        std::filesystem::create_directories(test_dir);
        const auto file_path = (test_dir / ("split." + format_name)).string();
        const auto delete_file_path = (test_dir / ("equality-delete." + format_name)).string();
        if (is_parquet) {
            write_nested_key_value_parquet_file(file_path, {100, 200, 300}, {10, 20, 30});
            write_nested_equality_parquet_file(delete_file_path, {}, {200}, {false}, true,
                                               "delete_key", 2);
        } else {
            write_nested_key_value_orc_file(file_path, {100, 200, 300}, {10, 20, 30});
            write_nested_equality_orc_file(delete_file_path, {}, {200}, {false}, "delete_key", 2);
        }

        auto schema_payload = external_struct_schema_field(
                "payload", 1,
                {external_schema_field("delete_key", 2, {}, std::nullopt,
                                       external_primitive_type(TPrimitiveType::INT)),
                 external_schema_field("visible_value", 3, {}, std::nullopt,
                                       external_primitive_type(TPrimitiveType::INT))});
        auto scan_params = make_local_scan_params(file_format);
        scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
        scan_params.__set_current_schema_id(100);
        scan_params.__set_history_schema_info({external_schema(100, {schema_payload})});

        const auto int_type = std::make_shared<DataTypeInt32>();
        auto visible_value = make_table_column(3, "visible_value", int_type);
        auto payload_type =
                std::make_shared<DataTypeStruct>(DataTypes {int_type}, Strings {"visible_value"});
        auto payload = make_table_column(1, "payload", payload_type);
        payload.children = {visible_value};
        std::vector<ColumnDefinition> projected_columns = {payload};

        RuntimeProfile profile("test_profile");
        RuntimeState state {TQueryOptions(), TQueryGlobals()};
        io::FileReaderStats file_reader_stats;
        io::FileCacheStatistics file_cache_stats;
        auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
        ShardedKVCache cache(1);
        doris::format::iceberg::IcebergTableReader reader;
        init_iceberg_reader(&reader, projected_columns, &scan_params, io_ctx, &state, &profile,
                            file_format);

        auto split_options = build_split_options(file_path);
        split_options.cache = &cache;
        split_options.current_split_format = file_format;
        const auto thrift_file_format =
                is_parquet ? TFileFormatType::FORMAT_PARQUET : TFileFormatType::FORMAT_ORC;
        split_options.current_range.__set_table_format_params(make_iceberg_table_format_desc(
                file_path,
                {make_iceberg_equality_delete_file(delete_file_path, {2}, thrift_file_format)}, 3));
        ASSERT_TRUE(reader.prepare_split(split_options).ok());

        Block block = build_table_block(projected_columns);
        bool eos = false;
        ASSERT_TRUE(reader.get_block(&block, &eos).ok());
        ASSERT_FALSE(eos);
        const auto& payload_result =
                assert_cast<const ColumnStruct&>(expect_not_null_table_column(block, 0));
        expect_int32_column_values(payload_result.get_column(0), {10, 30});

        ASSERT_TRUE(reader.close().ok());
        std::filesystem::remove_all(test_dir);
    };

    for (const auto file_format : {FileFormat::PARQUET, FileFormat::ORC}) {
        run_case(file_format);
    }
}

// Metadata-only projections cannot carry schema aliases themselves. The complete Iceberg schema
// must still select name mapping for hidden equality-delete keys in both physical readers.
// NOLINTNEXTLINE(readability-function-cognitive-complexity,readability-function-size)
TEST(IcebergV2ReaderTest, IcebergMetadataOnlyEqualityDeleteUsesHistoryNameMapping) {
    const auto run_case = [](FileFormat file_format) {
        const bool is_parquet = file_format == FileFormat::PARQUET;
        const std::string format_name = is_parquet ? "parquet" : "orc";
        const auto test_dir = std::filesystem::temp_directory_path() /
                              ("doris_v2_metadata_only_equality_delete_" + format_name);
        std::filesystem::remove_all(test_dir);
        std::filesystem::create_directories(test_dir);
        const auto file_path = (test_dir / ("split." + format_name)).string();
        const auto delete_file_path = (test_dir / ("equality-delete." + format_name)).string();
        if (is_parquet) {
            write_single_int_parquet_file(file_path, "legacy_key", {5, 7, 9}, std::nullopt);
            write_iceberg_equality_delete_parquet_file(delete_file_path, 1, 7, "legacy_key");
        } else {
            write_single_int_orc_file(file_path, "legacy_key", {5, 7, 9}, std::nullopt);
            write_single_int_orc_file(delete_file_path, "legacy_key", {7}, 1);
        }

        auto key_field =
                external_schema_field("current_key", 1, {"legacy_key"}, std::nullopt,
                                      external_primitive_type(TPrimitiveType::INT), false, true);
        key_field.field_ptr->__set_name_mapping_is_authoritative(true);
        auto scan_params = make_local_scan_params(file_format);
        scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
        scan_params.__set_current_schema_id(100);
        scan_params.__set_history_schema_info({external_schema(100, {key_field})});

        std::vector<ColumnDefinition> projected_columns;
        projected_columns.push_back(make_synthesized_table_column(
                BeConsts::ICEBERG_FILE_PATH_COL, std::make_shared<DataTypeString>()));
        projected_columns.push_back(make_synthesized_table_column(
                BeConsts::ICEBERG_ROW_POSITION_COL, std::make_shared<DataTypeInt64>()));

        RuntimeProfile profile("test_profile");
        RuntimeState state {TQueryOptions(), TQueryGlobals()};
        io::FileReaderStats file_reader_stats;
        io::FileCacheStatistics file_cache_stats;
        auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
        ShardedKVCache cache(1);
        doris::format::iceberg::IcebergTableReader reader;
        init_iceberg_reader(&reader, projected_columns, &scan_params, io_ctx, &state, &profile,
                            file_format);

        auto split_options = build_split_options(file_path);
        split_options.cache = &cache;
        split_options.current_split_format = file_format;
        const auto thrift_file_format =
                is_parquet ? TFileFormatType::FORMAT_PARQUET : TFileFormatType::FORMAT_ORC;
        split_options.current_range.__set_table_format_params(make_iceberg_table_format_desc(
                file_path,
                {make_iceberg_equality_delete_file(delete_file_path, {1}, thrift_file_format)}, 3));
        ASSERT_TRUE(reader.prepare_split(split_options).ok());

        Block block = build_table_block(projected_columns);
        bool eos = false;
        ASSERT_TRUE(reader.get_block(&block, &eos).ok());
        ASSERT_FALSE(eos);
        ASSERT_EQ(block.rows(), 2);
        expect_string_column_values(*block.get_by_position(0).column, {file_path, file_path});
        expect_int64_column_values(*block.get_by_position(1).column, {0, 2});

        ASSERT_TRUE(reader.close().ok());
        std::filesystem::remove_all(test_dir);
    };

    for (const auto file_format : {FileFormat::PARQUET, FileFormat::ORC}) {
        run_case(file_format);
    }
}

// Keep the shared Parquet/ORC reader setup together so both V2 paths exercise identical ID-less
// nested-name resolution.
// NOLINTNEXTLINE(readability-function-cognitive-complexity,readability-function-size)
TEST(IcebergV2ReaderTest, IcebergIdlessNestedEqualityKeyUsesAliasPathAndDeleteLeafName) {
    const auto run_case = [](FileFormat file_format, bool dropped_from_current) {
        const bool is_parquet = file_format == FileFormat::PARQUET;
        const std::string format_name = is_parquet ? "parquet" : "orc";
        const auto test_dir = std::filesystem::temp_directory_path() /
                              ("doris_v2_idless_nested_equality_" + format_name +
                               (dropped_from_current ? "_dropped" : "_current"));
        std::filesystem::remove_all(test_dir);
        std::filesystem::create_directories(test_dir);
        const auto file_path = (test_dir / ("split." + format_name)).string();
        const auto delete_file_path = (test_dir / ("equality-delete." + format_name)).string();
        if (is_parquet) {
            write_nested_equality_parquet_file(file_path, {1, 2, 3}, {5, 7, 9},
                                               {false, false, false}, true, "legacy_key", 2,
                                               "legacy_payload", false);
            write_iceberg_equality_delete_parquet_file(delete_file_path, 2, 7, "legacy_key");
        } else {
            write_nested_equality_orc_file(file_path, {1, 2, 3}, {5, 7, 9}, {false, false, false},
                                           "legacy_key", 2, "legacy_payload", false);
            write_single_int_orc_file(delete_file_path, "legacy_key", {7}, 2);
        }

        const auto make_payload_field = [] {
            auto payload = external_struct_schema_field(
                    "current_payload", 1,
                    {external_schema_field("current_key", 2, {}, std::nullopt,
                                           external_primitive_type(TPrimitiveType::INT))});
            payload.field_ptr->__set_name_mapping({"legacy_payload"});
            payload.field_ptr->__set_name_mapping_is_authoritative(true);
            return payload;
        };
        std::vector<schema::external::TFieldPtr> current_fields = {external_schema_field("id", 0)};
        if (!dropped_from_current) {
            current_fields.push_back(make_payload_field());
        }
        std::vector<schema::external::TSchema> history = {
                external_schema(100, std::move(current_fields))};
        if (dropped_from_current) {
            history.push_back(
                    external_schema(99, {external_schema_field("id", 0), make_payload_field()}));
        }

        std::vector<ColumnDefinition> projected_columns;
        projected_columns.push_back(make_authoritatively_name_mapped_table_column(
                0, "id", std::make_shared<DataTypeInt32>()));
        auto scan_params = make_local_scan_params(file_format);
        scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
        scan_params.__set_current_schema_id(100);
        scan_params.__set_history_schema_info(history);

        RuntimeProfile profile("test_profile");
        RuntimeState state {TQueryOptions(), TQueryGlobals()};
        io::FileReaderStats file_reader_stats;
        io::FileCacheStatistics file_cache_stats;
        auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
        ShardedKVCache cache(1);
        doris::format::iceberg::IcebergTableReader reader;
        init_iceberg_reader(&reader, projected_columns, &scan_params, io_ctx, &state, &profile,
                            file_format);

        auto split_options = build_split_options(file_path);
        split_options.cache = &cache;
        split_options.current_split_format = file_format;
        const auto thrift_file_format =
                is_parquet ? TFileFormatType::FORMAT_PARQUET : TFileFormatType::FORMAT_ORC;
        split_options.current_range.__set_table_format_params(make_iceberg_table_format_desc(
                file_path,
                {make_iceberg_equality_delete_file(delete_file_path, {2}, thrift_file_format)}, 3));
        ASSERT_TRUE(reader.prepare_split(split_options).ok());
        EXPECT_EQ(read_iceberg_ids(&reader, projected_columns), std::vector<int32_t>({1, 3}));
        ASSERT_TRUE(reader.close().ok());
        std::filesystem::remove_all(test_dir);
    };

    for (const auto file_format : {FileFormat::PARQUET, FileFormat::ORC}) {
        run_case(file_format, false);
        run_case(file_format, true);
    }
}

TEST(IcebergV2ReaderTest, IcebergEqualityDeleteResolvesFieldIdThroughIdlessWrapper) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_v2_equality_key_through_idless_wrapper";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);
    const auto file_path = (test_dir / "split.parquet").string();
    const auto delete_file_path = (test_dir / "equality-delete.parquet").string();
    write_nested_equality_parquet_file(file_path, {1, 2, 3}, {5, 7, 9}, {false, false, false}, true,
                                       "legacy_key", 2, "legacy_payload", true, false);
    write_iceberg_equality_delete_parquet_file(delete_file_path, 2, 7, "legacy_key");

    auto current_payload = external_struct_schema_field(
            "current_payload", 1,
            {external_schema_field("current_key", 2, {}, std::nullopt,
                                   external_primitive_type(TPrimitiveType::INT))});
    std::vector<schema::external::TSchema> history = {
            external_schema(100, {external_schema_field("id", 0), std::move(current_payload)})};

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));
    auto scan_params = make_local_parquet_scan_params();
    scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
    scan_params.__set_current_schema_id(100);
    scan_params.__set_history_schema_info(std::move(history));

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
            file_path, {make_iceberg_equality_delete_file(delete_file_path, {2})}, 3));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());
    EXPECT_EQ(read_iceberg_ids(&reader, projected_columns), std::vector<int32_t>({1, 3}));
    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

// Keep the shared Parquet/ORC reader setup together so both V2 paths assert identical semantics.
// NOLINTNEXTLINE(readability-function-cognitive-complexity,readability-function-size)
TEST(IcebergV2ReaderTest, IcebergMissingNestedEqualityKeyPreservesNullableParent) {
    const auto run_case = [](FileFormat file_format) {
        const bool is_parquet = file_format == FileFormat::PARQUET;
        const std::string format_name = is_parquet ? "parquet" : "orc";
        const auto test_dir = std::filesystem::temp_directory_path() /
                              ("doris_v2_missing_nested_equality_key_" + format_name);
        std::filesystem::remove_all(test_dir);
        std::filesystem::create_directories(test_dir);
        const auto file_path = (test_dir / ("split." + format_name)).string();
        const auto delete_file_path = (test_dir / ("equality-delete." + format_name)).string();
        if (is_parquet) {
            write_nested_equality_parquet_file(file_path, {1, 2, 3}, {10, 20, 30},
                                               {false, true, false});
            write_nested_equality_parquet_file(delete_file_path, {}, {7}, {false}, true, "k", 3);
        } else {
            write_nested_equality_orc_file(file_path, {1, 2, 3}, {10, 20, 30},
                                           {false, true, false});
            write_nested_equality_orc_file(delete_file_path, {}, {7}, {false}, "k", 3);
        }

        auto payload = external_struct_schema_field(
                "payload", 1,
                {external_schema_field("existing", 2, {}, std::nullopt,
                                       external_primitive_type(TPrimitiveType::INT)),
                 external_schema_field("k", 3, {}, "7",
                                       external_primitive_type(TPrimitiveType::INT), false, true)},
                true);
        std::vector<schema::external::TSchema> history = {
                external_schema(100, {external_schema_field("id", 0), payload})};

        std::vector<ColumnDefinition> projected_columns;
        projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));
        auto scan_params = make_local_scan_params(file_format);
        scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
        scan_params.__set_current_schema_id(100);
        scan_params.__set_history_schema_info(std::move(history));

        RuntimeProfile profile("test_profile");
        RuntimeState state {TQueryOptions(), TQueryGlobals()};
        io::FileReaderStats file_reader_stats;
        io::FileCacheStatistics file_cache_stats;
        auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
        ShardedKVCache cache(1);
        doris::format::iceberg::IcebergTableReader reader;
        init_iceberg_reader(&reader, projected_columns, &scan_params, io_ctx, &state, &profile,
                            file_format);

        auto split_options = build_split_options(file_path);
        split_options.cache = &cache;
        split_options.current_split_format = file_format;
        const auto thrift_file_format =
                is_parquet ? TFileFormatType::FORMAT_PARQUET : TFileFormatType::FORMAT_ORC;
        split_options.current_range.__set_table_format_params(make_iceberg_table_format_desc(
                file_path,
                {make_iceberg_equality_delete_file(delete_file_path, {3}, thrift_file_format)}, 3));
        ASSERT_TRUE(reader.prepare_split(split_options).ok());
        EXPECT_EQ(read_iceberg_ids(&reader, projected_columns), std::vector<int32_t>({2}));
        ASSERT_TRUE(reader.close().ok());
        std::filesystem::remove_all(test_dir);
    };

    for (const auto file_format : {FileFormat::PARQUET, FileFormat::ORC}) {
        run_case(file_format);
    }
}

// An older Parquet file holds a nullable payload {existing, ts}, `ts` an INT96 timestamptz as
// Spark writes it, and the table later added payload.k with the default 7. An equality delete on k
// reads the whole `payload` for its NULLs: rows under a non-NULL payload take the key 7, the row
// under the NULL one keeps a NULL key. FE's marker turns `ts` into TIMESTAMPTZ in the annotated
// file schema, so that ancestor read must carry the marker too, or Parquet still decodes `ts` as
// DATETIMEV2 and the scan fails the reader's type check. A query projecting `payload` failed the
// same way: reading the root as a predicate column replaced the projection its mapping had marked.
// A plan without the marker keeps the file types and reads as before.
// NOLINTNEXTLINE(readability-function-cognitive-complexity): marker, projection, tombstone cases.
TEST(IcebergV2ReaderTest, IcebergMissingNestedEqualityKeyAncestorKeepsInt96TimestampTzMarker) {
    TimezoneUtils::load_timezones_to_cache();
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_v2_missing_nested_equality_key_int96_sibling";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);
    const auto file_path = (test_dir / "split.parquet").string();
    // 2024-01-01 00:00:00.123456 UTC, then one and two seconds later.
    constexpr int64_t T1_MICROS = 1'704'067'200'123'456L;
    write_nested_equality_int96_sibling_parquet_file(
            file_path, {1, 2, 3}, {10, 20, 30},
            {T1_MICROS, T1_MICROS + 1'000'000L, T1_MICROS + 2'000'000L}, {false, true, false});
    auto physical_reader = ::parquet::ParquetFileReader::OpenFile(file_path, false);
    ASSERT_EQ(::parquet::Type::INT96,
              physical_reader->metadata()->schema()->Column(2)->physical_type());
    physical_reader->Close();
    // The two delete files have the same size and may share an mtime second, so each tombstone
    // gets its own path: one rewritten path could serve the first one from the Parquet caches.
    const auto delete_file_path = [&test_dir](int32_t tombstone) {
        return (test_dir / ("equality-delete-" + std::to_string(tombstone) + ".parquet")).string();
    };
    for (const int32_t tombstone : {7, 8}) {
        write_nested_equality_parquet_file(delete_file_path(tombstone), {}, {tombstone}, {false},
                                           true, "k", 3);
    }

    const auto int_type = std::make_shared<DataTypeInt32>();
    const auto tz_type = std::make_shared<DataTypeTimeStampTz>(6);
    const auto nullable_tz_type = make_nullable(tz_type);
    auto payload = make_table_column(1, "payload",
                                     std::make_shared<DataTypeStruct>(DataTypes {int_type, tz_type},
                                                                      Strings {"existing", "ts"}));
    payload.children = {make_table_column(2, "existing", int_type),
                        make_table_column(4, "ts", tz_type)};

    struct Case {
        bool marked;
        bool project_payload;
        int32_t tombstone;
        std::vector<int32_t> ids;
        // `ts` of each surviving row when `payload` is projected; nullopt under a NULL payload.
        std::vector<std::optional<std::string>> timestamps;
    };
    const std::vector<Case> cases = {
            // A plan from an FE that sends no marker.
            {false, false, 7, {2}, {}},
            // The key is the default 7 under a non-NULL payload and NULL under the NULL one.
            {true, false, 7, {2}, {}},
            {true, true, 7, {2}, {std::nullopt}},
            // Instants, not session wall clocks read eight hours early in Asia/Shanghai.
            {true,
             true,
             8,
             {1, 2, 3},
             {"2024-01-01 00:00:00.123456+00:00", std::nullopt,
              "2024-01-01 00:00:02.123456+00:00"}},
    };
    for (const auto& c : cases) {
        SCOPED_TRACE("marked=" + std::to_string(c.marked) +
                     " project_payload=" + std::to_string(c.project_payload) +
                     " tombstone=" + std::to_string(c.tombstone));
        std::vector<ColumnDefinition> projected_columns {make_table_column(0, "id", int_type)};
        if (c.project_payload) {
            projected_columns.push_back(payload);
        }
        auto ts_field =
                external_schema_field("ts", 4, {}, std::nullopt,
                                      external_primitive_type(TPrimitiveType::TIMESTAMPTZ, -1, 6));
        if (c.marked) {
            ts_field.field_ptr->__set_timestamp_is_adjusted_to_utc(true);
        }
        auto payload_field = external_struct_schema_field(
                "payload", 1,
                {external_schema_field("existing", 2, {}, std::nullopt,
                                       external_primitive_type(TPrimitiveType::INT)),
                 std::move(ts_field),
                 external_schema_field("k", 3, {}, "7",
                                       external_primitive_type(TPrimitiveType::INT), false, true)},
                true);
        auto scan_params = make_local_parquet_scan_params();
        scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
        scan_params.__set_parquet_timestamp_semantics_version(1);
        scan_params.__set_enable_mapping_timestamp_tz(true);
        scan_params.__set_current_schema_id(100);
        scan_params.__set_history_schema_info(
                {external_schema(100, {external_schema_field("id", 0), std::move(payload_field)})});

        RuntimeProfile profile("test_profile");
        RuntimeState state {TQueryOptions(), TQueryGlobals()};
        state.set_timezone("Asia/Shanghai");
        io::FileReaderStats file_reader_stats;
        io::FileCacheStatistics file_cache_stats;
        auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
        ShardedKVCache cache(1);
        doris::format::iceberg::IcebergTableReader reader;
        init_iceberg_reader(&reader, projected_columns, &scan_params, io_ctx, &state, &profile);
        auto split_options = build_split_options(file_path);
        split_options.cache = &cache;
        split_options.current_range.__set_table_format_params(make_iceberg_table_format_desc(
                file_path, {make_iceberg_equality_delete_file(delete_file_path(c.tombstone), {3})},
                3));
        ASSERT_TRUE(reader.prepare_split(split_options).ok());

        std::vector<int32_t> ids;
        std::vector<std::optional<std::string>> timestamps;
        bool eos = false;
        while (!eos) {
            Block block = build_table_block(projected_columns);
            const auto status = reader.get_block(&block, &eos);
            ASSERT_TRUE(status.ok()) << status;
            if (block.rows() == 0) {
                continue;
            }
            const auto& id_column =
                    assert_cast<const ColumnInt32&>(expect_not_null_table_column(block, 0));
            for (size_t row = 0; row < block.rows(); ++row) {
                ids.push_back(id_column.get_element(row));
            }
            if (!c.project_payload) {
                continue;
            }
            const auto& payload_column =
                    assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
            const auto& payload_struct =
                    assert_cast<const ColumnStruct&>(payload_column.get_nested_column());
            for (size_t row = 0; row < block.rows(); ++row) {
                if (payload_column.is_null_at(row)) {
                    timestamps.emplace_back(std::nullopt);
                } else {
                    timestamps.emplace_back(
                            nullable_tz_type->to_string(payload_struct.get_column(1), row));
                }
            }
        }
        EXPECT_EQ(ids, c.ids);
        EXPECT_EQ(timestamps, c.timestamps);
        ASSERT_TRUE(reader.close().ok());
    }
    std::filesystem::remove_all(test_dir);
}

// Keep the shared Parquet/ORC reader setup together so both V2 paths materialize missing struct
// literals before traversing their children.
// NOLINTNEXTLINE(readability-function-cognitive-complexity,readability-function-size)
TEST(IcebergV2ReaderTest, IcebergMissingWholeStructEqualityKeyMaterializesDefault) {
    const auto run_case = [](FileFormat file_format,
                             const std::optional<std::string>& root_default) {
        const bool is_parquet = file_format == FileFormat::PARQUET;
        const std::string format_name = is_parquet ? "parquet" : "orc";
        const std::string default_name = root_default.has_value() ? "empty_struct" : "null";
        const auto test_dir =
                std::filesystem::temp_directory_path() /
                ("doris_v2_missing_whole_struct_equality_key_" + format_name + "_" + default_name);
        std::filesystem::remove_all(test_dir);
        std::filesystem::create_directories(test_dir);
        const auto file_path = (test_dir / ("split." + format_name)).string();
        const auto delete_file_path = (test_dir / ("equality-delete." + format_name)).string();
        if (is_parquet) {
            write_single_int_parquet_file(file_path, "id", {1, 2, 3}, 0);
            write_nested_equality_parquet_file(delete_file_path, {}, {7}, {false}, true, "k", 3);
        } else {
            write_single_int_orc_file(file_path, "id", {1, 2, 3}, 0);
            write_nested_equality_orc_file(delete_file_path, {}, {7}, {false}, "k", 3);
        }

        auto payload = external_struct_schema_field(
                "payload", 1,
                {external_schema_field("k", 3, {}, "7",
                                       external_primitive_type(TPrimitiveType::INT), false, true)},
                true, root_default);
        std::vector<schema::external::TSchema> history = {
                external_schema(100, {external_schema_field("id", 0), payload})};

        std::vector<ColumnDefinition> projected_columns;
        projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));
        auto scan_params = make_local_scan_params(file_format);
        scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
        scan_params.__set_current_schema_id(100);
        scan_params.__set_history_schema_info(std::move(history));

        RuntimeProfile profile("test_profile");
        RuntimeState state {TQueryOptions(), TQueryGlobals()};
        io::FileReaderStats file_reader_stats;
        io::FileCacheStatistics file_cache_stats;
        auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
        ShardedKVCache cache(1);
        doris::format::iceberg::IcebergTableReader reader;
        init_iceberg_reader(&reader, projected_columns, &scan_params, io_ctx, &state, &profile,
                            file_format);

        auto split_options = build_split_options(file_path);
        split_options.cache = &cache;
        split_options.current_split_format = file_format;
        const auto thrift_file_format =
                is_parquet ? TFileFormatType::FORMAT_PARQUET : TFileFormatType::FORMAT_ORC;
        split_options.current_range.__set_table_format_params(make_iceberg_table_format_desc(
                file_path,
                {make_iceberg_equality_delete_file(delete_file_path, {3}, thrift_file_format)}, 3));
        ASSERT_TRUE(reader.prepare_split(split_options).ok());
        const std::vector<int32_t> expected_ids =
                root_default.has_value() ? std::vector<int32_t> {} : std::vector {1, 2, 3};
        EXPECT_EQ(read_iceberg_ids(&reader, projected_columns), expected_ids);
        ASSERT_TRUE(reader.close().ok());
        std::filesystem::remove_all(test_dir);
    };

    for (const auto file_format : {FileFormat::PARQUET, FileFormat::ORC}) {
        run_case(file_format, std::nullopt);
        run_case(file_format, "{}");
    }
}

TEST(IcebergV2ReaderTest, IcebergProjectedStructDoesNotReuseReaddedEqualityKeyByName) {
    const auto run_case = [](FileFormat file_format) {
        const bool is_parquet = file_format == FileFormat::PARQUET;
        const std::string format_name = is_parquet ? "parquet" : "orc";
        const auto test_dir = std::filesystem::temp_directory_path() /
                              ("doris_v2_missing_reused_equality_key_" + format_name);
        std::filesystem::remove_all(test_dir);
        std::filesystem::create_directories(test_dir);
        const auto file_path = (test_dir / ("split." + format_name)).string();
        const auto delete_file_path = (test_dir / ("equality-delete." + format_name)).string();
        if (is_parquet) {
            write_nested_equality_parquet_file(file_path, {1, 2, 3}, {7, 8, 9},
                                               {false, false, false}, true, "k", 4);
            write_nested_equality_parquet_file(delete_file_path, {}, {7}, {false}, true, "k", 3);
        } else {
            write_nested_equality_orc_file(file_path, {1, 2, 3}, {7, 8, 9}, {false, false, false},
                                           "k", 4);
            write_nested_equality_orc_file(delete_file_path, {}, {7}, {false}, "k", 3);
        }

        auto payload = external_struct_schema_field(
                "payload", 1,
                {external_schema_field("k", 4, {}, std::nullopt,
                                       external_primitive_type(TPrimitiveType::INT), false, true),
                 external_schema_field("k", 3, {}, std::nullopt,
                                       external_primitive_type(TPrimitiveType::INT), false, true)},
                true);
        std::vector<schema::external::TSchema> history = {
                external_schema(-1, {external_schema_field("id", 0), std::move(payload)})};

        const auto int_type = std::make_shared<DataTypeInt32>();
        std::vector<ColumnDefinition> projected_columns;
        projected_columns.push_back(make_table_column(0, "id", int_type));
        auto current_key = make_table_column(4, "k", int_type);
        auto payload_type = std::make_shared<DataTypeStruct>(DataTypes {int_type}, Strings {"k"});
        auto projected_payload = make_table_column(1, "payload", payload_type);
        projected_payload.children = {std::move(current_key)};
        projected_columns.push_back(std::move(projected_payload));
        auto scan_params = make_local_scan_params(file_format);
        scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
        scan_params.__set_current_schema_id(-1);
        scan_params.__set_history_schema_info(std::move(history));

        RuntimeProfile profile("test_profile");
        RuntimeState state {TQueryOptions(), TQueryGlobals()};
        io::FileReaderStats file_reader_stats;
        io::FileCacheStatistics file_cache_stats;
        auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
        ShardedKVCache cache(1);
        doris::format::iceberg::IcebergTableReader reader;
        init_iceberg_reader(&reader, projected_columns, &scan_params, io_ctx, &state, &profile,
                            file_format);

        auto split_options = build_split_options(file_path);
        split_options.cache = &cache;
        split_options.current_split_format = file_format;
        const auto thrift_file_format =
                is_parquet ? TFileFormatType::FORMAT_PARQUET : TFileFormatType::FORMAT_ORC;
        split_options.current_range.__set_table_format_params(make_iceberg_table_format_desc(
                file_path,
                {make_iceberg_equality_delete_file(delete_file_path, {3}, thrift_file_format)}, 3));
        ASSERT_TRUE(reader.prepare_split(split_options).ok());
        EXPECT_EQ(read_iceberg_ids(&reader, projected_columns), std::vector<int32_t>({1, 2, 3}));
        ASSERT_TRUE(reader.close().ok());
        std::filesystem::remove_all(test_dir);
    };

    for (const auto file_format : {FileFormat::PARQUET, FileFormat::ORC}) {
        run_case(file_format);
    }
}

// A schema carrier may contain a dropped and a re-added child with the same name. Keep the
// Parquet/ORC setup identical so both readers prove that equality-key reconstruction follows the
// exact field-id path instead of attaching both same-name children to one synthetic struct slot.
// NOLINTNEXTLINE(readability-function-cognitive-complexity,readability-function-size)
TEST(IcebergV2ReaderTest, IcebergMissingWholeStructEqualityKeyUsesExactHistoricalChildId) {
    const auto run_case = [](FileFormat file_format) {
        const bool is_parquet = file_format == FileFormat::PARQUET;
        const std::string format_name = is_parquet ? "parquet" : "orc";
        const auto test_dir = std::filesystem::temp_directory_path() /
                              ("doris_v2_missing_reused_equality_key_" + format_name);
        std::filesystem::remove_all(test_dir);
        std::filesystem::create_directories(test_dir);
        const auto file_path = (test_dir / ("split." + format_name)).string();
        const auto delete_file_path = (test_dir / ("equality-delete." + format_name)).string();
        if (is_parquet) {
            write_single_int_parquet_file(file_path, "id", {1, 2, 3}, 0);
            write_nested_equality_parquet_file(delete_file_path, {}, {7}, {false}, true, "k", 3);
        } else {
            write_single_int_orc_file(file_path, "id", {1, 2, 3}, 0);
            write_nested_equality_orc_file(delete_file_path, {}, {7}, {false}, "k", 3);
        }

        auto payload = external_struct_schema_field(
                "payload", 1,
                {external_schema_field("k", 4, {}, std::nullopt,
                                       external_primitive_type(TPrimitiveType::INT), false, true),
                 external_schema_field("k", 3, {}, "7",
                                       external_primitive_type(TPrimitiveType::INT), false, true)},
                true);
        std::vector<schema::external::TSchema> history = {
                external_schema(100, {external_schema_field("id", 0), payload})};

        std::vector<ColumnDefinition> projected_columns;
        projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));
        auto scan_params = make_local_scan_params(file_format);
        scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
        scan_params.__set_current_schema_id(100);
        scan_params.__set_history_schema_info(std::move(history));

        RuntimeProfile profile("test_profile");
        RuntimeState state {TQueryOptions(), TQueryGlobals()};
        io::FileReaderStats file_reader_stats;
        io::FileCacheStatistics file_cache_stats;
        auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
        ShardedKVCache cache(1);
        doris::format::iceberg::IcebergTableReader reader;
        init_iceberg_reader(&reader, projected_columns, &scan_params, io_ctx, &state, &profile,
                            file_format);

        auto split_options = build_split_options(file_path);
        split_options.cache = &cache;
        split_options.current_split_format = file_format;
        const auto thrift_file_format =
                is_parquet ? TFileFormatType::FORMAT_PARQUET : TFileFormatType::FORMAT_ORC;
        split_options.current_range.__set_table_format_params(make_iceberg_table_format_desc(
                file_path,
                {make_iceberg_equality_delete_file(delete_file_path, {3}, thrift_file_format)}, 3));
        ASSERT_TRUE(reader.prepare_split(split_options).ok());
        EXPECT_EQ(read_iceberg_ids(&reader, projected_columns), std::vector<int32_t>({1, 2, 3}));
        ASSERT_TRUE(reader.close().ok());
        std::filesystem::remove_all(test_dir);
    };

    for (const auto file_format : {FileFormat::PARQUET, FileFormat::ORC}) {
        run_case(file_format);
    }
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

TEST(IcebergV2ReaderTest, IcebergEqualityDeletePromotesHistoricalDeleteKeyToCurrentType) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_iceberg_equality_delete_promotion_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    const auto delete_file_path = (test_dir / "equality-delete.parquet").string();
    // The data file was written after INT -> LONG promotion and holds a value outside the old INT
    // domain. The delete file was written under the old INT schema and deletes the NULL key.
    write_nullable_int64_parquet_file(
            file_path, 0, "x", {std::nullopt, int64_t {0}, int64_t {1}, int64_t {4294967296}});
    write_iceberg_null_equality_delete_parquet_file(delete_file_path, 0, "x");

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(
            make_table_column(0, "x", make_nullable(std::make_shared<DataTypeInt64>())));

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
    scan_params.__set_current_schema_id(100);
    scan_params.__set_history_schema_info({external_schema(
            100, {external_schema_field("x", 0, {}, std::nullopt,
                                        external_primitive_type(TPrimitiveType::BIGINT), false,
                                        true)})});
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

    std::vector<std::optional<int64_t>> values;
    bool eos = false;
    while (!eos) {
        Block block = build_table_block(projected_columns);
        ASSERT_TRUE(reader.get_block(&block, &eos).ok());
        if (block.rows() == 0) {
            continue;
        }
        const auto full_column = block.get_by_position(0).column->convert_to_full_column_if_const();
        const auto& nullable_column = assert_cast<const ColumnNullable&>(*full_column);
        const auto& data =
                assert_cast<const ColumnInt64&>(nullable_column.get_nested_column()).get_data();
        for (size_t row = 0; row < nullable_column.size(); ++row) {
            if (nullable_column.get_null_map_data()[row] != 0) {
                values.push_back(std::nullopt);
            } else {
                values.push_back(data[row]);
            }
        }
    }

    // The NULL row is deleted, while 4294967296 must not be narrowed into the old INT domain and
    // deleted together with it.
    EXPECT_EQ(values, (std::vector<std::optional<int64_t>> {int64_t {0}, int64_t {1},
                                                            int64_t {4294967296}}));

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

template <typename Builder, typename Value>
std::shared_ptr<arrow::Array> build_nullable_array(
        Builder* builder, const std::vector<std::optional<Value>>& values) {
    for (const auto& value : values) {
        if (value.has_value()) {
            EXPECT_TRUE(builder->Append(*value).ok());
        } else {
            EXPECT_TRUE(builder->AppendNull().ok());
        }
    }
    return finish_array(builder);
}

std::shared_ptr<arrow::Array> build_nullable_int32_keys(
        const std::vector<std::optional<int32_t>>& values) {
    arrow::Int32Builder builder;
    return build_nullable_array(&builder, values);
}

std::shared_ptr<arrow::Array> build_date32_keys(const std::vector<std::optional<int32_t>>& days) {
    arrow::Date32Builder builder;
    return build_nullable_array(&builder, days);
}

std::shared_ptr<arrow::Array> build_timestamp_keys(
        arrow::TimeUnit::type unit, const std::vector<std::optional<int64_t>>& ticks) {
    arrow::TimestampBuilder builder(arrow::timestamp(unit), arrow::default_memory_pool());
    return build_nullable_array(&builder, ticks);
}

void write_parquet_table(const std::string& file_path, const std::shared_ptr<arrow::Table>& table) {
    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;
    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    builder.compression(::parquet::Compression::UNCOMPRESSED);
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out,
                                                      std::max<int64_t>(table->num_rows(), 1),
                                                      builder.build()));
}

// Writes `id` (field id 0) and, unless `keys` is null, the nullable key `k` (field id 1).
void write_id_and_key_parquet_file(const std::string& file_path, const std::vector<int32_t>& ids,
                                   const std::shared_ptr<arrow::Array>& keys) {
    std::vector<std::shared_ptr<arrow::Field>> fields {
            arrow::field("id", arrow::int32(), false)
                    ->WithMetadata(arrow::key_value_metadata({"PARQUET:field_id"}, {"0"}))};
    std::vector<std::shared_ptr<arrow::Array>> columns {build_int32_array(ids)};
    if (keys != nullptr) {
        fields.push_back(
                arrow::field("k", keys->type(), true)
                        ->WithMetadata(arrow::key_value_metadata({"PARQUET:field_id"}, {"1"})));
        columns.push_back(keys);
    }
    write_parquet_table(file_path, arrow::Table::Make(arrow::schema(fields), columns));
}

struct KeyDataFile {
    std::vector<int32_t> ids;
    // nullptr: the data file has no `k`, as one written before the key was added.
    std::shared_ptr<arrow::Array> keys;
};

// Writes an equality delete file at the given path and returns its format.
using EqualityDeleteFileWriter = std::function<TFileFormatType::type(const std::string&)>;

// Reads each data file as one split of a single reader. Every split carries the same equality
// delete file on `equality_field_id`, whose one column has the name and field id of `key_field`
// and holds `delete_keys`; for a nested key that is a struct array whose child carries the key's
// field id. `write_delete_file`, when given, writes that file instead, for physical shapes Arrow
// does not produce. `key_field` describes that column in the scan's schema metadata, as the FE
// sends it. Returns the surviving ids of each split.
Status read_ids_after_key_equality_delete(const std::string& test_name,
                                          const std::vector<KeyDataFile>& data_files,
                                          const std::shared_ptr<arrow::Array>& delete_keys,
                                          schema::external::TFieldPtr key_field,
                                          std::vector<std::vector<int32_t>>* ids_per_split,
                                          int32_t equality_field_id = 1,
                                          bool enable_mapping_timestamp_tz = false,
                                          const EqualityDeleteFileWriter& write_delete_file = {}) {
    const auto test_dir = std::filesystem::temp_directory_path() / test_name;
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);
    const auto delete_file_path = (test_dir / "equality-delete.parquet").string();
    auto delete_file_format = TFileFormatType::FORMAT_PARQUET;
    if (write_delete_file) {
        delete_file_format = write_delete_file(delete_file_path);
    } else {
        const auto& delete_column = *key_field.field_ptr;
        write_parquet_table(
                delete_file_path,
                arrow::Table::Make(
                        arrow::schema({arrow::field(delete_column.name, delete_keys->type(), true)
                                               ->WithMetadata(arrow::key_value_metadata(
                                                       {"PARQUET:field_id"},
                                                       {std::to_string(delete_column.id)}))}),
                        {delete_keys}));
    }

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));
    auto scan_params = make_local_parquet_scan_params();
    scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
    if (enable_mapping_timestamp_tz) {
        scan_params.__set_enable_mapping_timestamp_tz(true);
    }
    scan_params.__set_current_schema_id(100);
    scan_params.__set_history_schema_info(
            {external_schema(100, {external_schema_field("id", 0), std::move(key_field)})});
    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::format::iceberg::IcebergTableReader reader;
    init_iceberg_reader(&reader, projected_columns, &scan_params, io_ctx, &state, &profile);
    for (size_t index = 0; index < data_files.size(); ++index) {
        const auto file_path = (test_dir / ("data-" + std::to_string(index) + ".parquet")).string();
        write_id_and_key_parquet_file(file_path, data_files[index].ids, data_files[index].keys);
        auto split_options = build_split_options(file_path);
        split_options.cache = &cache;
        split_options.current_range.__set_table_format_params(make_iceberg_table_format_desc(
                file_path, {make_iceberg_equality_delete_file(delete_file_path, {equality_field_id},
                                                              delete_file_format)}));
        RETURN_IF_ERROR(reader.prepare_split(split_options));
        std::vector<int32_t> ids;
        bool eos = false;
        while (!eos) {
            Block block = build_table_block(projected_columns);
            RETURN_IF_ERROR(reader.get_block(&block, &eos));
            if (block.rows() == 0) {
                continue;
            }
            const auto& id_column =
                    assert_cast<const ColumnInt32&>(expect_not_null_table_column(block, 0));
            for (size_t row = 0; row < block.rows(); ++row) {
                ids.push_back(id_column.get_element(row));
            }
        }
        ids_per_split->push_back(std::move(ids));
    }
    RETURN_IF_ERROR(reader.close());
    std::filesystem::remove_all(test_dir);
    return Status::OK();
}

constexpr int64_t MICROS_PER_DAY = 86400LL * 1000 * 1000;
constexpr int32_t DAYS_1000_01_01 = -354285;
constexpr int32_t DAYS_1600_01_01 = -135140;
constexpr int32_t DAYS_2000_01_01 = 10957;
constexpr int32_t DAYS_2024_01_02 = 19724;
constexpr int32_t DAYS_2024_01_03 = 19725;

// After expireSnapshots().cleanExpiredMetadata(true) removes the schema of a schema-only
// INT -> LONG promotion, the FE types a dropped key INT although its tombstones are LONG.
// Comparing in INT turned 4294967296 into NULL, which deleted the NULL key of the old file and
// the 4294967296 key of the new one together.
TEST(IcebergV2ReaderTest, IcebergEqualityDeleteKeepsWideTombstonesUnderNarrowTableType) {
    std::vector<std::vector<int32_t>> ids;
    const auto status = read_ids_after_key_equality_delete(
            "doris_iceberg_equality_delete_narrow_table_type",
            {{.ids = {1, 2, 3}, .keys = build_nullable_int32_keys({std::nullopt, 0, 1})},
             {.ids = {4, 5}, .keys = build_nullable_int64_array({std::nullopt, 4294967296})}},
            build_nullable_int64_array({4294967296}),
            external_schema_field("k", 1, {}, std::nullopt,
                                  external_primitive_type(TPrimitiveType::INT), false, true),
            &ids);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(ids, (std::vector<std::vector<int32_t>> {{1, 2, 3}, {4}}));
}

// INT tombstones written before an INT -> LONG promotion meet LONG data files and INT data files
// of the same scan. LONG files compare in LONG through a widened copy of the delete block, which
// the third split takes from the scanner cache; the INT file uses the delete block as read.
TEST(IcebergV2ReaderTest, IcebergEqualityDeleteWidensTombstonesPerDataFileType) {
    std::vector<std::vector<int32_t>> ids;
    const auto status = read_ids_after_key_equality_delete(
            "doris_iceberg_equality_delete_widened_per_data_file",
            {{.ids = {1, 2}, .keys = build_nullable_int64_array({7, 4294967303})},
             {.ids = {3, 4}, .keys = build_nullable_int32_keys({7, 8})},
             {.ids = {5, 6}, .keys = build_nullable_int64_array({4294967303, 7})}},
            build_nullable_int32_keys({7}),
            external_schema_field("k", 1, {}, std::nullopt,
                                  external_primitive_type(TPrimitiveType::BIGINT), false, true),
            &ids);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(ids, (std::vector<std::vector<int32_t>> {{2}, {4}, {5}}));
}

// A read pinned before a later DATE -> TIMESTAMP_NS promotion gets the promoted table type. The
// DATE tombstone and the DATE data compare as DATE, so 1000-01-01 does not have to fit
// TIMESTAMP_NS (1677-09-21..2262-04-11) and the scan no longer fails with an overflow.
TEST(IcebergV2ReaderTest, IcebergEqualityDeleteComparesDatesUnderNanosecondTableType) {
    std::vector<std::vector<int32_t>> ids;
    const auto status = read_ids_after_key_equality_delete(
            "doris_iceberg_equality_delete_wide_table_type",
            {{.ids = {1, 2}, .keys = build_date32_keys({DAYS_1000_01_01, DAYS_2000_01_01})}},
            build_date32_keys({DAYS_1000_01_01}),
            external_schema_field("k", 1, {}, std::nullopt,
                                  external_primitive_type(TPrimitiveType::TIMESTAMP_NS), false,
                                  true),
            &ids);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(ids, (std::vector<std::vector<int32_t>> {{2}}));
}

// The FE encodes an Iceberg `unknown` key as STRING when no retained schema gives it a concrete
// type. Comparing as STRING made the DATE key "2024-01-02" differ from the TIMESTAMP_NS tombstone
// "2024-01-02 00:00:00.000000000", so the deleted row stayed visible.
TEST(IcebergV2ReaderTest, IcebergEqualityDeleteIgnoresUnknownTableTypeForDateAndNanosKeys) {
    std::vector<std::vector<int32_t>> ids;
    const auto status = read_ids_after_key_equality_delete(
            "doris_iceberg_equality_delete_unknown_table_type",
            {{.ids = {1, 2}, .keys = build_date32_keys({DAYS_2024_01_02, DAYS_2024_01_03})}},
            build_timestamp_keys(arrow::TimeUnit::NANO, {DAYS_2024_01_02 * MICROS_PER_DAY * 1000}),
            external_schema_field("k", 1, {}, std::nullopt,
                                  external_primitive_type(TPrimitiveType::STRING), false, true),
            &ids);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(ids, (std::vector<std::vector<int32_t>> {{2}}));
}

// A delete prepared under a TIMESTAMP_NS schema that the table later abandoned holds nanosecond
// tombstones while the table type is TIMESTAMP. Rounding the tombstone 12:00:00.000000500 to
// microseconds matched one of the microsecond keys; in nanoseconds it matches neither.
TEST(IcebergV2ReaderTest, IcebergEqualityDeleteComparesNanosecondTombstonesInNanoseconds) {
    const int64_t noon_micros = DAYS_2024_01_02 * MICROS_PER_DAY + 12LL * 3600 * 1000 * 1000;
    std::vector<std::vector<int32_t>> ids;
    const auto status = read_ids_after_key_equality_delete(
            "doris_iceberg_equality_delete_abandoned_nanos",
            {{.ids = {1, 2},
              .keys = build_timestamp_keys(arrow::TimeUnit::MICRO,
                                           {noon_micros, noon_micros + 1})}},
            build_timestamp_keys(arrow::TimeUnit::NANO, {noon_micros * 1000 + 500}),
            external_schema_field("k", 1, {}, std::nullopt,
                                  external_primitive_type(TPrimitiveType::DATETIMEV2, -1, 6), false,
                                  true),
            &ids);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(ids, (std::vector<std::vector<int32_t>> {{1, 2}}));
}

// Comparing microsecond keys in nanoseconds must fail for a key outside 1677-09-21..2262-04-11.
// A non-strict cast would turn 1600-01-01 into NULL, and the NULL tombstone would delete it.
TEST(IcebergV2ReaderTest, IcebergEqualityDeleteRejectsMicrosOutsideNanosecondRange) {
    std::vector<std::vector<int32_t>> ids;
    const auto status = read_ids_after_key_equality_delete(
            "doris_iceberg_equality_delete_micros_outside_nanos",
            {{.ids = {1, 2},
              .keys = build_timestamp_keys(
                      arrow::TimeUnit::MICRO,
                      {DAYS_1600_01_01 * MICROS_PER_DAY, DAYS_2024_01_02 * MICROS_PER_DAY})}},
            build_timestamp_keys(arrow::TimeUnit::NANO, {std::nullopt}),
            external_schema_field("k", 1, {}, std::nullopt,
                                  external_primitive_type(TPrimitiveType::DATETIMEV2, -1, 6), false,
                                  true),
            &ids);
    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("TIMESTAMP_NS overflow"), std::string::npos) << status;
}

// An equality delete file without rows deletes nothing, so the strict comparison casts that a
// nonempty one needs must not fail the scan: DATE keys meeting TIMESTAMP_NS tombstones are cast to
// TIMESTAMP_NS, which 1000-01-01 overflows, both as a key of the first file and as the initial
// default for the second. Building a predicate for the empty file fails with
// "TIMESTAMP_NS overflow".
TEST(IcebergV2ReaderTest, IcebergEqualityDeleteSkipsEmptyDeleteFileBeforeKeyCasts) {
    std::vector<std::vector<int32_t>> ids;
    const auto status = read_ids_after_key_equality_delete(
            "doris_iceberg_equality_delete_empty_delete_file",
            {{.ids = {1, 2}, .keys = build_date32_keys({DAYS_1000_01_01, DAYS_2000_01_01})},
             {.ids = {3}, .keys = nullptr}},
            build_timestamp_keys(arrow::TimeUnit::NANO, {}),
            external_schema_field("k", 1, {}, "1000-01-01",
                                  external_primitive_type(TPrimitiveType::DATEV2), false, true),
            &ids);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(ids, (std::vector<std::vector<int32_t>> {{1, 2}, {3}}));
}

// A data file without the key compares its initial default, typed by the table, against the
// tombstones in the wider of the two types: INT default 5 matches the LONG tombstone 5.
TEST(IcebergV2ReaderTest, IcebergEqualityDeleteWidensMissingKeyDefaultToTombstoneType) {
    std::vector<std::vector<int32_t>> ids;
    const auto status = read_ids_after_key_equality_delete(
            "doris_iceberg_equality_delete_missing_key_default_widened",
            {{.ids = {1, 2, 3}, .keys = nullptr}}, build_nullable_int64_array({5}),
            external_schema_field("k", 1, {}, "5", external_primitive_type(TPrimitiveType::INT),
                                  false, true),
            &ids);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(ids, (std::vector<std::vector<int32_t>> {std::vector<int32_t> {}}));
}

// Without an initial default a missing key is NULL whatever the table type, so it is compared in
// the tombstones' type even when the table type cannot convert to it (`unknown` arrives as
// STRING): the NULL tombstone deletes every row, the DATE one deletes none.
TEST(IcebergV2ReaderTest, IcebergEqualityDeleteMatchesNullMissingKeyUnderUnknownTableType) {
    std::vector<std::vector<int32_t>> ids;
    const auto status = read_ids_after_key_equality_delete(
            "doris_iceberg_equality_delete_missing_key_unknown_type",
            {{.ids = {1, 2}, .keys = nullptr}}, build_date32_keys({std::nullopt, DAYS_2024_01_02}),
            external_schema_field("k", 1, {}, std::nullopt,
                                  external_primitive_type(TPrimitiveType::STRING), false, true),
            &ids);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(ids, (std::vector<std::vector<int32_t>> {std::vector<int32_t> {}}));
}

// The nested delete key `payload.k` (field id 3 under `payload`) holding `days` as DATE under
// non-NULL parents.
std::shared_ptr<arrow::Array> build_struct_date32_keys(
        const std::vector<std::optional<int32_t>>& days) {
    auto key_field = arrow::field("k", arrow::date32(), true)
                             ->WithMetadata(arrow::key_value_metadata({"PARQUET:field_id"}, {"3"}));
    auto keys = arrow::StructArray::Make({build_date32_keys(days)}, {key_field});
    DORIS_CHECK(keys.ok());
    return *keys;
}

// A missing struct whose initial default is `{}` takes each child's own default, so `payload.k`
// without one is NULL, and it compares in the tombstones' type like a missing key without a
// default. Here `k` is STRING, as the FE encodes Iceberg `unknown`, and DATE does not convert to
// it. The NULL tombstone deletes every row and the DATE one none. Deciding NULL by the struct's
// default alone compares STRING with DATE and fails the scan with NotSupported.
TEST(IcebergV2ReaderTest, IcebergMissingWholeStructEqualityKeyComparesNullLeafInTombstoneType) {
    const std::vector<std::pair<std::optional<int32_t>, std::vector<int32_t>>> cases {
            {std::nullopt, {}}, {DAYS_2024_01_02, {1, 2, 3}}};
    for (const auto& [tombstone, expected_ids] : cases) {
        SCOPED_TRACE(tombstone.has_value() ? "DATE tombstone" : "NULL tombstone");
        std::vector<std::vector<int32_t>> ids;
        const auto status = read_ids_after_key_equality_delete(
                "doris_iceberg_equality_delete_null_struct_default_leaf",
                {{.ids = {1, 2, 3}, .keys = nullptr}}, build_struct_date32_keys({tombstone}),
                external_struct_schema_field(
                        "payload", 1,
                        {external_schema_field("k", 3, {}, std::nullopt,
                                               external_primitive_type(TPrimitiveType::STRING),
                                               false, true)},
                        true, "{}"),
                &ids, 3);
        ASSERT_TRUE(status.ok()) << status;
        EXPECT_EQ(ids, (std::vector<std::vector<int32_t>> {expected_ids}));
    }
}

constexpr int64_t TIME_01_00_00_000001_MICROS = 3600LL * 1000 * 1000 + 1;
constexpr int64_t TIME_02_00_00_MICROS = 7200LL * 1000 * 1000;

// Iceberg's Parquet writer stores TIME as TIME(isAdjustedToUTC=false, MICROS), read as TIMEV2(6),
// and timestamptz_ns as TIMESTAMP(isAdjustedToUTC=true, NANOS), both INT64.
std::shared_ptr<const ::parquet::LogicalType> iceberg_parquet_time_type() {
    return ::parquet::LogicalType::Time(false, ::parquet::LogicalType::TimeUnit::MICROS);
}

// Writes the Iceberg TIME `k` (field id 1) as Iceberg stores it, microseconds from midnight in
// both formats: Parquet's TIME(MICROS) and ORC's LONG annotated iceberg.long-type=TIME, which reads
// as BIGINT. Nonempty `ids` add the INT `id` (field id 0) in front.
void write_iceberg_time_file(const std::string& file_path, FileFormat format,
                             const std::vector<int32_t>& ids,
                             const std::vector<std::optional<int64_t>>& micros) {
    ASSERT_TRUE(ids.empty() || ids.size() == micros.size());
    if (format == FileFormat::PARQUET) {
        ::parquet::schema::NodeVector fields;
        if (!ids.empty()) {
            fields.push_back(::parquet::schema::PrimitiveNode::Make(
                    "id", ::parquet::Repetition::REQUIRED, ::parquet::LogicalType::None(),
                    ::parquet::Type::INT32, -1, 0));
        }
        fields.push_back(::parquet::schema::PrimitiveNode::Make(
                "k", ::parquet::Repetition::OPTIONAL, iceberg_parquet_time_type(),
                ::parquet::Type::INT64, -1, 1));
        auto schema = std::static_pointer_cast<::parquet::schema::GroupNode>(
                ::parquet::schema::GroupNode::Make("schema", ::parquet::Repetition::REQUIRED,
                                                   fields));
        auto out = arrow::io::FileOutputStream::Open(file_path).ValueOrDie();
        auto writer = ::parquet::ParquetFileWriter::Open(out, schema);
        auto* group = writer->AppendRowGroup();
        if (!ids.empty()) {
            auto* id_writer = static_cast<::parquet::Int32Writer*>(group->NextColumn());
            for (const int32_t id : ids) {
                id_writer->WriteBatch(1, nullptr, nullptr, &id);
            }
            id_writer->Close();
        }
        auto* key_writer = static_cast<::parquet::Int64Writer*>(group->NextColumn());
        for (const auto& value : micros) {
            const int16_t level = value.has_value() ? 1 : 0;
            const int64_t micros_value = value.value_or(0);
            key_writer->WriteBatch(1, &level, nullptr, &micros_value);
        }
        key_writer->Close();
        group->Close();
        writer->Close();
        ASSERT_TRUE(out->Close().ok());
        return;
    }
    auto type = std::unique_ptr<::orc::Type>(::orc::Type::buildTypeFromString(
            ids.empty() ? "struct<k:bigint>" : "struct<id:int,k:bigint>"));
    const size_t key_index = ids.empty() ? 0 : 1;
    if (!ids.empty()) {
        type->getSubtype(0)->setAttribute("iceberg.id", "0");
    }
    type->getSubtype(key_index)->setAttribute("iceberg.id", "1");
    type->getSubtype(key_index)->setAttribute("iceberg.long-type", "TIME");
    MemoryOutputStream stream(1024 * 1024);
    ::orc::WriterOptions options;
    auto writer = ::orc::createWriter(*type, &stream, options);
    auto batch = writer->createRowBatch(micros.size());
    auto& root = dynamic_cast<::orc::StructVectorBatch&>(*batch);
    root.numElements = micros.size();
    if (!ids.empty()) {
        auto& id_batch = dynamic_cast<::orc::LongVectorBatch&>(*root.fields[0]);
        id_batch.numElements = ids.size();
        for (size_t row = 0; row < ids.size(); ++row) {
            id_batch.data[row] = ids[row];
        }
    }
    auto& key_batch = dynamic_cast<::orc::LongVectorBatch&>(*root.fields[key_index]);
    key_batch.numElements = micros.size();
    key_batch.hasNulls = true;
    for (size_t row = 0; row < micros.size(); ++row) {
        key_batch.notNull[row] = micros[row].has_value();
        key_batch.data[row] = micros[row].value_or(0);
    }
    writer->add(*batch);
    writer->close();
    std::ofstream out(file_path, std::ios::binary);
    out.write(stream.getData(), static_cast<std::streamsize>(stream.getLength()));
}

// The FE sends an Iceberg TIME, which Doris cannot represent, as a STRING placeholder, so a hidden
// TIME key compares in the files' own types: BIGINT microseconds in ORC, TIMEV2(6) in Parquet. The
// pair compares as TIMEV2(6) with the ORC value converted exactly, in either direction, so the NULL
// tombstone deletes row 1 and 01:00:00.000001 deletes row 2 (3600000001). Without that rule the
// mixed pairs fail with NotSupported; a SQL cast would read 3600000001 as HHMMSS digits, and the
// old STRING comparison never matched "3600000001" against "01:00:00.000001". Same-format pairs
// compare as they are.
TEST(IcebergV2ReaderTest, IcebergEqualityDeleteComparesTimeKeysAcrossOrcAndParquet) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_iceberg_equality_delete_time_formats";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);
    const std::vector<std::pair<std::optional<int64_t>, std::vector<int32_t>>> cases {
            {std::nullopt, {2, 3}}, {TIME_01_00_00_000001_MICROS, {1, 3}}};
    // Every run writes new files: the reader caches Parquet footers and pages by path and size, so
    // a same-size file rewritten at one path could be read as its earlier content.
    int run = 0;
    for (auto data_format : {FileFormat::ORC, FileFormat::PARQUET}) {
        for (auto delete_format : {FileFormat::ORC, FileFormat::PARQUET}) {
            for (const auto& [tombstone, expected_ids] : cases) {
                SCOPED_TRACE(::testing::Message()
                             << "data=" << static_cast<int>(data_format)
                             << " delete=" << static_cast<int>(delete_format) << " tombstone="
                             << (tombstone.has_value() ? std::to_string(*tombstone) : "NULL"));
                const auto file_path = (test_dir / ("data-" + std::to_string(run))).string();
                const auto delete_path = (test_dir / ("delete-" + std::to_string(run++))).string();
                write_iceberg_time_file(
                        file_path, data_format, {1, 2, 3},
                        {std::nullopt, TIME_01_00_00_000001_MICROS, TIME_02_00_00_MICROS});
                write_iceberg_time_file(delete_path, delete_format, {}, {tombstone});
                std::vector<ColumnDefinition> columns = {
                        make_table_column(0, "id", std::make_shared<DataTypeInt32>())};
                auto scan_params = make_local_scan_params(data_format);
                scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
                scan_params.__set_current_schema_id(100);
                scan_params.__set_history_schema_info({external_schema(
                        100, {external_schema_field("id", 0),
                              external_schema_field("k", 1, {}, std::nullopt,
                                                    external_primitive_type(TPrimitiveType::STRING),
                                                    false, true)})});
                RuntimeState state {TQueryOptions(), TQueryGlobals()};
                RuntimeProfile profile("time_formats");
                io::FileReaderStats stats;
                io::FileCacheStatistics cache_stats;
                auto io_ctx = make_io_context(&stats, &cache_stats);
                ShardedKVCache cache(1);
                iceberg::IcebergTableReader reader;
                init_iceberg_reader(&reader, columns, &scan_params, io_ctx, &state, &profile,
                                    data_format);
                auto split = build_split_options(file_path);
                split.current_split_format = data_format;
                split.cache = &cache;
                split.current_range.__set_table_format_params(make_iceberg_table_format_desc(
                        file_path, {make_iceberg_equality_delete_file(
                                           delete_path, {1},
                                           delete_format == FileFormat::PARQUET
                                                   ? TFileFormatType::FORMAT_PARQUET
                                                   : TFileFormatType::FORMAT_ORC)}));
                const auto status = reader.prepare_split(split);
                ASSERT_TRUE(status.ok()) << status;
                EXPECT_EQ(read_iceberg_ids(&reader, columns), expected_ids);
                ASSERT_TRUE(reader.close().ok());
            }
        }
    }
    std::filesystem::remove_all(test_dir);
}

// `payload` (field id 1) of a data file written before `payload.k` existed: a struct whose only
// child is the INT `a` (field id 2). std::nullopt is a NULL `payload`.
std::shared_ptr<arrow::Array> build_payload_without_key(
        const std::vector<std::optional<int32_t>>& values) {
    auto a_field = arrow::field("a", arrow::int32(), true)
                           ->WithMetadata(arrow::key_value_metadata({"PARQUET:field_id"}, {"2"}));
    std::unique_ptr<arrow::ArrayBuilder> builder;
    EXPECT_TRUE(
            arrow::MakeBuilder(arrow::default_memory_pool(), arrow::struct_({a_field}), &builder)
                    .ok());
    auto& struct_builder = static_cast<arrow::StructBuilder&>(*builder);
    auto& a_builder = static_cast<arrow::Int32Builder&>(*struct_builder.field_builder(0));
    for (const auto& value : values) {
        if (value.has_value()) {
            EXPECT_TRUE(struct_builder.Append().ok());
            EXPECT_TRUE(a_builder.Append(*value).ok());
        } else {
            EXPECT_TRUE(struct_builder.AppendNull().ok());
        }
    }
    std::shared_ptr<arrow::Array> result;
    EXPECT_TRUE(struct_builder.Finish(&result).ok());
    return result;
}

// Writes the nested equality delete column `payload` (field id 1) whose child `k` (field id 3) is
// an INT64 leaf of `logical_type`, holding `keys` under non-NULL parents.
void write_nested_int64_key_delete_parquet_file(
        const std::string& file_path,
        const std::shared_ptr<const ::parquet::LogicalType>& logical_type,
        const std::vector<std::optional<int64_t>>& keys) {
    ::parquet::schema::NodeVector key_fields {::parquet::schema::PrimitiveNode::Make(
            "k", ::parquet::Repetition::OPTIONAL, logical_type, ::parquet::Type::INT64, -1, 3)};
    ::parquet::schema::NodeVector fields {
            ::parquet::schema::GroupNode::Make("payload", ::parquet::Repetition::OPTIONAL,
                                               key_fields, ::parquet::ConvertedType::NONE, 1)};
    auto schema = std::static_pointer_cast<::parquet::schema::GroupNode>(
            ::parquet::schema::GroupNode::Make("schema", ::parquet::Repetition::REQUIRED, fields));
    auto out = arrow::io::FileOutputStream::Open(file_path).ValueOrDie();
    auto writer = ::parquet::ParquetFileWriter::Open(out, schema);
    auto* group = writer->AppendRowGroup();
    auto* key_writer = static_cast<::parquet::Int64Writer*>(group->NextColumn());
    for (const auto& key : keys) {
        // Definition level 2: `payload` and `k` present; 1: `payload` present with a NULL `k`.
        const int16_t level = key.has_value() ? 2 : 1;
        const int64_t value = key.value_or(0);
        key_writer->WriteBatch(1, &level, nullptr, &value);
    }
    key_writer->Close();
    group->Close();
    writer->Close();
    ASSERT_TRUE(out->Close().ok());
}

// The same column in ORC, where `k` is an Iceberg TIME: a LONG annotated iceberg.long-type=TIME.
void write_nested_time_key_delete_orc_file(const std::string& file_path,
                                           const std::vector<std::optional<int64_t>>& micros) {
    auto type = std::unique_ptr<::orc::Type>(
            ::orc::Type::buildTypeFromString("struct<payload:struct<k:bigint>>"));
    type->getSubtype(0)->setAttribute("iceberg.id", "1");
    type->getSubtype(0)->getSubtype(0)->setAttribute("iceberg.id", "3");
    type->getSubtype(0)->getSubtype(0)->setAttribute("iceberg.long-type", "TIME");
    MemoryOutputStream stream(1024 * 1024);
    ::orc::WriterOptions options;
    auto writer = ::orc::createWriter(*type, &stream, options);
    auto batch = writer->createRowBatch(micros.size());
    auto& root = dynamic_cast<::orc::StructVectorBatch&>(*batch);
    root.numElements = micros.size();
    auto& payload = dynamic_cast<::orc::StructVectorBatch&>(*root.fields[0]);
    payload.numElements = micros.size();
    auto& keys = dynamic_cast<::orc::LongVectorBatch&>(*payload.fields[0]);
    keys.numElements = micros.size();
    keys.hasNulls = true;
    for (size_t row = 0; row < micros.size(); ++row) {
        keys.notNull[row] = micros[row].has_value();
        keys.data[row] = micros[row].value_or(0);
    }
    writer->add(*batch);
    writer->close();
    std::ofstream out(file_path, std::ios::binary);
    out.write(stream.getData(), static_cast<std::streamsize>(stream.getLength()));
}

// Iceberg TIME has no Doris type, so the FE sends `payload.k` as a STRING placeholder whose initial
// default is Iceberg's text of the value. That text is parsed in the tombstones' type (an ORC TIME
// tombstone, BIGINT, compares as TIMEV2(6)). Rows under the data file's NULL `payload` keep a NULL
// key and the others take the default, so a NULL tombstone deletes only row 1, the default's
// tombstone rows 2 and 3, and another value none. Comparing the STRING default with the TIMEV2
// tombstones failed the split with NotSupported.
TEST(IcebergV2ReaderTest, IcebergMissingNestedPlaceholderKeyParsesTextDefaultInTombstoneType) {
    struct PlaceholderDefault {
        std::string name;
        std::string text_default;
        // Writes a delete file holding the tombstone, in the file's unit.
        std::function<TFileFormatType::type(const std::string&, std::optional<int64_t>)> writer;
        int64_t default_value;
        int64_t other_value;
        bool enable_mapping_timestamp_tz;
    };
    const std::vector<PlaceholderDefault> kinds {
            {"Parquet TIME", "00:00",
             [](const std::string& path, std::optional<int64_t> key) {
                 write_nested_int64_key_delete_parquet_file(path, iceberg_parquet_time_type(),
                                                            {key});
                 return TFileFormatType::FORMAT_PARQUET;
             },
             0, TIME_01_00_00_000001_MICROS, false},
            {"ORC TIME", "00:00",
             [](const std::string& path, std::optional<int64_t> key) {
                 write_nested_time_key_delete_orc_file(path, {key});
                 return TFileFormatType::FORMAT_ORC;
             },
             0, TIME_01_00_00_000001_MICROS, false}};
    // A directory per run, as in IcebergEqualityDeleteComparesTimeKeysAcrossOrcAndParquet: the
    // 1-row INT64 delete files of one kind have the same size whatever the tombstone.
    int run = 0;
    for (const auto& kind : kinds) {
        const std::vector<std::pair<std::optional<int64_t>, std::vector<int32_t>>> cases {
                {std::nullopt, {2, 3}}, {kind.default_value, {1}}, {kind.other_value, {1, 2, 3}}};
        for (const auto& [tombstone, expected_ids] : cases) {
            SCOPED_TRACE(::testing::Message()
                         << kind.name << " tombstone="
                         << (tombstone.has_value() ? std::to_string(*tombstone) : "NULL"));
            std::vector<std::vector<int32_t>> ids;
            const auto status = read_ids_after_key_equality_delete(
                    "doris_iceberg_equality_delete_placeholder_text_default_" +
                            std::to_string(run++),
                    {{.ids = {1, 2, 3}, .keys = build_payload_without_key({std::nullopt, 1, 2})}},
                    nullptr,
                    external_struct_schema_field(
                            "payload", 1,
                            {external_schema_field("a", 2, {}, std::nullopt,
                                                   external_primitive_type(TPrimitiveType::INT),
                                                   false, true),
                             external_schema_field("k", 3, {}, kind.text_default,
                                                   external_primitive_type(TPrimitiveType::STRING),
                                                   false, true)},
                            true),
                    &ids, 3, kind.enable_mapping_timestamp_tz,
                    [&kind, tombstone = tombstone](const std::string& path) {
                        return kind.writer(path, tombstone);
                    });
            ASSERT_TRUE(status.ok()) << status;
            EXPECT_EQ(ids, (std::vector<std::vector<int32_t>> {expected_ids}));
        }
    }
}

// A timestamptz_ns `payload.k` behind the STRING placeholder: its Parquet tombstones are read as
// TIMESTAMPTZ(6), the nanoseconds floored, so a non-NULL default cannot be compared exactly and the
// scan fails. Parsing the default instead deleted the rows for the default `...0000001` against an
// epoch tombstone and for the default epoch against a tombstone 100 ns later, and kept them for the
// default `...0000005` against its own tombstone. Without a default the key is NULL: a NULL
// tombstone deletes every row, an epoch tombstone none.
TEST(IcebergV2ReaderTest, IcebergMissingPlaceholderTimestampTzNsDefaultFailsTheScan) {
    const auto write_tombstone = [](std::optional<int64_t> nanos) {
        return [nanos](const std::string& path) {
            write_nested_int64_key_delete_parquet_file(
                    path,
                    ::parquet::LogicalType::Timestamp(true,
                                                      ::parquet::LogicalType::TimeUnit::NANOS),
                    {nanos});
            return TFileFormatType::FORMAT_PARQUET;
        };
    };
    const auto read_ids = [](const std::string& test_name,
                             const std::optional<std::string>& text_default,
                             const EqualityDeleteFileWriter& write_delete_file,
                             std::vector<std::vector<int32_t>>* ids) {
        return read_ids_after_key_equality_delete(
                test_name,
                {{.ids = {1, 2, 3}, .keys = build_payload_without_key({std::nullopt, 1, 2})}},
                nullptr,
                external_struct_schema_field(
                        "payload", 1,
                        {external_schema_field("a", 2, {}, std::nullopt,
                                               external_primitive_type(TPrimitiveType::INT), false,
                                               true),
                         external_schema_field("k", 3, {}, text_default,
                                               external_primitive_type(TPrimitiveType::STRING),
                                               false, true)},
                        true),
                ids, 3, true, write_delete_file);
    };
    const std::vector<std::pair<std::string, int64_t>> rejected {
            {"1970-01-01 00:00:00.0000001+00:00", 0},
            {"1970-01-01 00:00:00+00:00", 100},
            {"1970-01-01 00:00:00.0000005+00:00", 500}};
    int run = 0;
    for (const auto& [text_default, tombstone] : rejected) {
        SCOPED_TRACE(::testing::Message() << text_default << " tombstone=" << tombstone);
        std::vector<std::vector<int32_t>> ids;
        const auto status = read_ids(
                "doris_iceberg_equality_delete_timestamptz_ns_default_" + std::to_string(run++),
                text_default, write_tombstone(tombstone), &ids);
        ASSERT_FALSE(status.ok()) << "ids=" << ::testing::PrintToString(ids);
        EXPECT_TRUE(status.is<ErrorCode::NOT_IMPLEMENTED_ERROR>()) << status;
        EXPECT_NE(status.to_string().find("timestamptz_ns key"), std::string::npos) << status;
    }

    const std::vector<std::pair<std::optional<int64_t>, std::vector<int32_t>>> without_default {
            {std::nullopt, {}}, {0, {1, 2, 3}}};
    for (const auto& [tombstone, expected_ids] : without_default) {
        SCOPED_TRACE(::testing::Message()
                     << "no default, tombstone="
                     << (tombstone.has_value() ? std::to_string(*tombstone) : "NULL"));
        std::vector<std::vector<int32_t>> ids;
        const auto status = read_ids(
                "doris_iceberg_equality_delete_timestamptz_ns_default_" + std::to_string(run++),
                std::nullopt, write_tombstone(tombstone), &ids);
        ASSERT_TRUE(status.ok()) << status;
        EXPECT_EQ(ids, (std::vector<std::vector<int32_t>> {expected_ids}));
    }
}

// Keys whose types do not convert exactly in either direction fail the scan instead of being
// compared in a third type.
TEST(IcebergV2ReaderTest, IcebergEqualityDeleteRejectsIncomparableKeyTypes) {
    std::vector<std::vector<int32_t>> ids;
    const auto status = read_ids_after_key_equality_delete(
            "doris_iceberg_equality_delete_incomparable_types",
            {{.ids = {1}, .keys = build_nullable_int32_keys({1})}},
            build_nullable_string_array({"1"}),
            external_schema_field("k", 1, {}, std::nullopt,
                                  external_primitive_type(TPrimitiveType::INT), false, true),
            &ids);
    ASSERT_FALSE(status.ok());
    EXPECT_TRUE(status.is<ErrorCode::NOT_IMPLEMENTED_ERROR>()) << status;
    EXPECT_NE(status.to_string().find("neither type converts exactly to the other"),
              std::string::npos)
            << status;
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
    scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
    scan_params.__set_current_schema_id(100);
    scan_params.__set_history_schema_info(
            {external_schema(100, {external_schema_field("id", 0),
                                   external_schema_field("added_column", 1, {}, std::nullopt,
                                                         std::nullopt, false, true)})});
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

TEST(IcebergV2ReaderTest, IcebergEqualityDeleteMissingKeyDoesNotReadUnsupportedUnprojectedCarrier) {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_iceberg_equality_delete_virtual_carrier_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    const auto delete_file_path = (test_dir / "equality-delete.parquet").string();
    write_unsupported_time_first_int_parquet_file(file_path, {1, 2, 3});
    write_iceberg_equality_delete_parquet_file(delete_file_path, 1, 7, "added_column");

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_authoritatively_name_mapped_table_column(
            0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
    scan_params.__set_current_schema_id(100);
    scan_params.__set_history_schema_info(
            {external_schema(100, {external_schema_field("id", 0),
                                   external_schema_field("added_column", 1, {}, std::nullopt,
                                                         std::nullopt, false, true)})});
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

    // The missing data key is NULL, so it does not match delete key 7. More importantly, the
    // hidden row-count dependency must use virtual row position instead of the first physical
    // TIME_MILLIS column, which is unsupported and was not requested by this `id` projection.
    EXPECT_EQ(read_iceberg_ids(&reader, projected_columns), std::vector<int32_t>({1, 2, 3}));

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
    scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
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

TEST(IcebergV2ReaderTest, IcebergEqualityDeleteUsesDroppedFieldHistoricalInitialDefault) {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_iceberg_equality_delete_historical_default_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    const auto delete_file_path = (test_dir / "equality-delete.parquet").string();
    write_single_int_parquet_file(file_path, "id", {1, 2, 3}, 0);
    write_iceberg_equality_delete_parquet_file(delete_file_path, 1, 7, "dropped_column");

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
    scan_params.__set_current_schema_id(200);
    scan_params.__set_history_schema_info(
            {external_schema(200, {external_schema_field("id", 0)}),
             external_schema(100,
                             {external_schema_field("id", 0),
                              external_schema_field("dropped_column", 1, {}, "7",
                                                    external_primitive_type(TPrimitiveType::INT),
                                                    false, true)})});
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

TEST(IcebergV2ReaderTest, IcebergEqualityDeleteRejectsDroppedFieldWithoutSchemaMetadata) {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_iceberg_equality_delete_missing_metadata_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    const auto delete_file_path = (test_dir / "equality-delete.parquet").string();
    write_single_int_parquet_file(file_path, "id", {1, 2, 3}, 0);
    write_iceberg_equality_delete_parquet_file(delete_file_path, 1, 7, "dropped_column");

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
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
    Block block = build_table_block(projected_columns);
    bool eos = false;
    const auto status = reader.get_block(&block, &eos);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("absent from current and historical table schema metadata"),
              std::string::npos);

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

TEST(IcebergV2ReaderTest, UuidNativeValuesAcrossDataAndEqualityDeleteFormats) {
    const auto test_dir = std::filesystem::temp_directory_path() / "iceberg_uuid_binary_carrier";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);
    const std::string normal(
            "\x00\x11\x22\x33\x44\x55\x66\x77"
            "\x88\x99\xaa\xbb\xcc\xdd\xee\xff",
            16);
    const std::string high = std::string("\x80", 1) + std::string(15, '\0');
    const std::vector<std::optional<std::string>> values = {
            std::string(16, '\0'),   normal,       high,
            std::string(16, '\xff'), std::nullopt, "abcdefghijklmnop"};
    const std::vector<std::optional<std::string>> deletes = {values[0], values[1], values[3],
                                                             values[4]};
    for (auto data_format : {FileFormat::PARQUET, FileFormat::ORC}) {
        for (auto delete_format : {FileFormat::PARQUET, FileFormat::ORC}) {
            const auto file_path = (test_dir / "data").string();
            const auto delete_path = (test_dir / "delete").string();
            write_iceberg_uuid_file(file_path, data_format, values, true);
            write_iceberg_uuid_file(delete_path, delete_format, deletes, false);
            for (bool mapping : {false, true}) {
                for (bool strict : {false, true}) {
                    SCOPED_TRACE(::testing::Message()
                                 << "data=" << static_cast<int>(data_format)
                                 << " delete=" << static_cast<int>(delete_format)
                                 << " mapping=" << mapping << " strict=" << strict);
                    DataTypePtr uuid_type = std::make_shared<DataTypeUUID>();
                    std::vector<ColumnDefinition> columns = {
                            make_table_column(0, "id", std::make_shared<DataTypeInt32>()),
                            make_table_column(1, "u", uuid_type)};
                    auto scan_params = make_local_scan_params(data_format);
                    scan_params.__set_iceberg_scan_semantics_version(
                            ICEBERG_SCAN_SEMANTICS_VERSION_2);
                    scan_params.__set_enable_mapping_varbinary(mapping);
                    TQueryOptions query_options;
                    query_options.__set_enable_strict_cast(strict);
                    RuntimeState state {query_options, TQueryGlobals()};
                    RuntimeProfile profile("uuid_carrier");
                    io::FileReaderStats stats;
                    io::FileCacheStatistics cache_stats;
                    auto io_ctx = make_io_context(&stats, &cache_stats);
                    ShardedKVCache cache(1);
                    iceberg::IcebergTableReader reader;
                    init_iceberg_reader(&reader, columns, &scan_params, io_ctx, &state, &profile,
                                        data_format);
                    reader.set_batch_size(2);
                    auto split = build_split_options(file_path);
                    split.current_split_format = data_format;
                    split.cache = &cache;
                    // Check every original byte before applying deletes, including NULL and boundaries.
                    ASSERT_TRUE(reader.prepare_split(split).ok());
                    size_t total = 0;
                    bool eos = false;
                    while (!eos) {
                        Block block = build_table_block(columns);
                        auto status = reader.get_block(&block, &eos);
                        ASSERT_TRUE(status.ok()) << status;
                        const auto& u = assert_cast<const ColumnNullable&>(
                                *block.get_by_position(1).column);
                        for (size_t row = 0; row < block.rows(); ++row, ++total) {
                            ASSERT_LT(total, values.size());
                            EXPECT_EQ(u.is_null_at(row), !values[total].has_value());
                            if (values[total].has_value()) {
                                EXPECT_EQ(uuid_type->to_string(u.get_nested_column(), row),
                                          UUIDValue::to_string(UUIDValue::from_big_endian(
                                                  reinterpret_cast<const uint8_t*>(
                                                          values[total]->data()))));
                            }
                        }
                    }
                    EXPECT_EQ(total, values.size());
                    split.current_range.__set_table_format_params(make_iceberg_table_format_desc(
                            file_path, {make_iceberg_equality_delete_file(
                                               delete_path, {1},
                                               delete_format == FileFormat::PARQUET
                                                       ? TFileFormatType::FORMAT_PARQUET
                                                       : TFileFormatType::FORMAT_ORC)}));
                    for (int repeat = 0; repeat < 2; ++repeat) {
                        ASSERT_TRUE(reader.prepare_split(split).ok());
                        EXPECT_EQ(read_iceberg_ids(&reader, columns),
                                  (std::vector<int32_t> {2, 5}));
                    }
                    ASSERT_TRUE(reader.close().ok());
                }
            }
        }
    }
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, UuidEqualityDeleteMatchesMissingColumnInitialDefault) {
    const auto test_dir = std::filesystem::temp_directory_path() / "iceberg_uuid_initial_default";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);
    const std::string value(
            "\x00\x11\x22\x33\x44\x55\x66\x77"
            "\x88\x99\xaa\xbb\xcc\xdd\xee\xff",
            16);
    for (auto data_format : {FileFormat::PARQUET, FileFormat::ORC}) {
        for (auto delete_format : {FileFormat::PARQUET, FileFormat::ORC}) {
            const auto file_path = (test_dir / "data").string();
            const auto delete_path = (test_dir / "delete").string();
            if (data_format == FileFormat::PARQUET) {
                write_single_int_parquet_file(file_path, "id", {0, 1, 2}, 0);
            } else {
                write_single_int_orc_file(file_path, "id", {0, 1, 2}, 0);
            }
            write_iceberg_uuid_file(delete_path, delete_format, {value}, false);
            for (bool mapping : {false, true}) {
                for (bool strict : {false, true}) {
                    SCOPED_TRACE(::testing::Message()
                                 << "data=" << static_cast<int>(data_format)
                                 << " delete=" << static_cast<int>(delete_format)
                                 << " mapping=" << mapping << " strict=" << strict);
                    std::vector<ColumnDefinition> columns = {
                            make_table_column(0, "id", std::make_shared<DataTypeInt32>())};
                    auto scan_params = make_local_scan_params(data_format);
                    scan_params.__set_iceberg_scan_semantics_version(
                            ICEBERG_SCAN_SEMANTICS_VERSION_2);
                    scan_params.__set_enable_mapping_varbinary(mapping);
                    scan_params.__set_current_schema_id(100);
                    scan_params.__set_history_schema_info({external_schema(
                            100, {external_schema_field("id", 0),
                                  external_schema_field(
                                          "u", 1, {}, "00112233-4455-6677-8899-aabbccddeeff",
                                          external_primitive_type(TPrimitiveType::UUID), false)})});
                    TQueryOptions query_options;
                    query_options.__set_enable_strict_cast(strict);
                    RuntimeState state {query_options, TQueryGlobals()};
                    RuntimeProfile profile("uuid_default");
                    io::FileReaderStats stats;
                    io::FileCacheStatistics cache_stats;
                    auto io_ctx = make_io_context(&stats, &cache_stats);
                    ShardedKVCache cache(1);
                    iceberg::IcebergTableReader reader;
                    init_iceberg_reader(&reader, columns, &scan_params, io_ctx, &state, &profile,
                                        data_format);
                    auto split = build_split_options(file_path);
                    split.current_split_format = data_format;
                    split.cache = &cache;
                    split.current_range.__set_table_format_params(make_iceberg_table_format_desc(
                            file_path,
                            {make_iceberg_equality_delete_file(
                                    delete_path, {1},
                                    delete_format == FileFormat::PARQUET
                                            ? TFileFormatType::FORMAT_PARQUET
                                            : TFileFormatType::FORMAT_ORC)},
                            3));
                    ASSERT_TRUE(reader.prepare_split(split).ok());
                    EXPECT_TRUE(read_iceberg_ids(&reader, columns).empty());
                    ASSERT_TRUE(reader.close().ok());
                }
            }
        }
    }
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
    static_assert(sizeof("0123456789abcdef0123456789abcdef") - 1 > StringView::kInlineSize);
    const std::string binary_default = "0123456789abcdef0123456789abcdef";
    write_iceberg_binary_equality_delete_parquet_file(delete_file_path, 1, binary_default,
                                                      "added_binary");

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));
    auto scan_params = make_local_parquet_scan_params();
    scan_params.__set_current_schema_id(100);
    scan_params.__set_history_schema_info({external_schema(
            100, {external_schema_field("id", 0),
                  external_schema_field(
                          "added_binary", 1, {}, "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=",
                          external_primitive_type(TPrimitiveType::VARBINARY,
                                                  static_cast<int32_t>(binary_default.size())),
                          true)})});

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

// Iceberg imports a Spark or Hive INT96 timestamp as timestamptz, but INT96 reads as DATETIMEV2
// while the adjusted-UTC tombstone of the same field reads as TIMESTAMPTZ, and neither widens to
// the other. They compare as TIMESTAMPTZ, the INT96 key converted in the session time zone as the
// projection converts it, so the tombstone T1 deletes the row holding T1. This plan carries no
// Parquet timestamp-semantics version, so INT96 also decodes in the session time zone; converting
// it back in a zone other than Asia/Shanghai, such as UTC, would miss T1 by eight hours. Without
// the TIMESTAMPTZ rule the split fails with NotSupported, with or without the key projected.
TEST(IcebergV2ReaderTest, IcebergEqualityDeleteComparesInt96DataKeyAsTimestampTz) {
    TimezoneUtils::load_timezones_to_cache();
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_iceberg_equality_delete_int96_timestamp_tz_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    const auto delete_file_path = (test_dir / "equality-delete.parquet").string();
    // 2024-01-01 00:00:00.123456 UTC and one second later.
    constexpr int64_t T1_MICROS = 1'704'067'200'123'456L;
    constexpr int64_t T2_MICROS = T1_MICROS + 1'000'000L;
    write_timestamp_int_parquet_file(file_path, {T1_MICROS, T2_MICROS}, {1, 2}, true);
    auto physical_reader = ::parquet::ParquetFileReader::OpenFile(file_path, false);
    ASSERT_EQ(::parquet::Type::INT96,
              physical_reader->metadata()->schema()->Column(0)->physical_type());
    physical_reader->Close();
    write_iceberg_timestamp_equality_delete_parquet_file(delete_file_path, 0, T1_MICROS,
                                                         "event_time", true);

    for (const bool project_key : {false, true}) {
        SCOPED_TRACE(project_key ? "id and event_time projected" : "id projected");
        std::vector<ColumnDefinition> projected_columns;
        projected_columns.push_back(make_table_column(1, "id", std::make_shared<DataTypeInt32>()));
        if (project_key) {
            projected_columns.push_back(
                    make_table_column(0, "event_time", std::make_shared<DataTypeTimeStampTz>(6)));
        }
        auto scan_params = make_local_parquet_scan_params();
        scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
        scan_params.__set_enable_mapping_timestamp_tz(true);
        scan_params.__set_current_schema_id(100);
        scan_params.__set_history_schema_info({external_schema(
                100,
                {external_schema_field("event_time", 0, {}, std::nullopt,
                                       external_primitive_type(TPrimitiveType::TIMESTAMPTZ, -1, 6)),
                 external_schema_field("id", 1)})});

        RuntimeProfile profile("test_profile");
        RuntimeState state {TQueryOptions(), TQueryGlobals()};
        state.set_timezone("Asia/Shanghai");
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
        std::vector<int32_t> ids;
        std::vector<std::string> event_times;
        bool eos = false;
        while (!eos) {
            Block block = build_table_block(projected_columns);
            const auto status = reader.get_block(&block, &eos);
            ASSERT_TRUE(status.ok()) << status;
            if (block.rows() == 0) {
                continue;
            }
            const auto& id_column =
                    assert_cast<const ColumnInt32&>(expect_not_null_table_column(block, 0));
            for (size_t row = 0; row < block.rows(); ++row) {
                ids.push_back(id_column.get_element(row));
                if (project_key) {
                    event_times.push_back(projected_columns[1].type->to_string(
                            *block.get_by_position(1).column, row));
                }
            }
        }
        EXPECT_EQ(ids, std::vector<int32_t>({2}));
        if (project_key) {
            // The projection converts the INT96 value back in the same zone, giving T2.
            EXPECT_EQ(event_times, std::vector<std::string>({"2024-01-01 00:00:01.123456+00:00"}));
        }
        ASSERT_TRUE(reader.close().ok());
    }
    std::filesystem::remove_all(test_dir);
}

// FE marks every Iceberg timestamptz field with timestamp_is_adjusted_to_utc. The marker lands on
// the file leaf the mapper selects for the field, by field id at every nesting level: a DATETIMEV2
// leaf becomes TIMESTAMPTZ with its precision and nullability, its containers are rebuilt around
// it, and an unmarked timestamp field stays a wall clock. A TIMESTAMP_NS leaf (INT64 NANOS without
// the UTC annotation) keeps its type and takes no marker; the Parquet reader rewrites every marked
// timestamp leaf, so a marker there would make it decode TIMESTAMPTZ under a TIMESTAMP_NS file
// schema. ORC files keep their types, since ORC's file type already separates the two semantics.
TEST(IcebergV2ReaderTest, IcebergAnnotatesTimestampTzMarkerOnMatchedParquetLeaves) {
    const auto marked = [](schema::external::TFieldPtr field) {
        field.field_ptr->__set_timestamp_is_adjusted_to_utc(true);
        return field;
    };
    auto info = external_struct_schema_field(
            "info", 4, {marked(external_schema_field("tz", 5)), external_schema_field("wall", 6)});
    auto tzs = external_schema_field("tzs", 7);
    schema::external::TArrayField array_field;
    array_field.__set_item_field(marked(external_schema_field("element", 8)));
    tzs.field_ptr->nestedField.__set_array_field(std::move(array_field));
    tzs.field_ptr->__isset.nestedField = true;
    auto by_tz = external_schema_field("by_tz", 9);
    schema::external::TMapField map_field;
    map_field.__set_key_field(marked(external_schema_field("key", 10)));
    map_field.__set_value_field(external_schema_field("value", 11));
    by_tz.field_ptr->nestedField.__set_map_field(std::move(map_field));
    by_tz.field_ptr->__isset.nestedField = true;

    TFileScanRangeParams scan_params;
    scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
    scan_params.__set_current_schema_id(100);
    scan_params.__set_history_schema_info({external_schema(
            100, {marked(external_schema_field("tz", 1)), external_schema_field("wall", 2),
                  marked(external_schema_field("ns", 3)), info, tzs, by_tz})});

    const auto datetime = std::make_shared<DataTypeDateTimeV2>(6);
    const auto build_file_schema = [&]() {
        auto info_column =
                make_file_column(4, "info",
                                 std::make_shared<DataTypeStruct>(DataTypes {datetime, datetime},
                                                                  Strings {"tz", "wall"}));
        info_column.children = {make_file_column(5, "tz", datetime),
                                make_file_column(6, "wall", datetime)};
        auto tzs_column = make_file_column(7, "tzs", std::make_shared<DataTypeArray>(datetime));
        tzs_column.children = {make_file_column(8, "element", datetime)};
        auto by_tz_column =
                make_file_column(9, "by_tz", std::make_shared<DataTypeMap>(datetime, datetime));
        by_tz_column.children = {make_file_column(10, "key", datetime),
                                 make_file_column(11, "value", datetime)};
        return std::vector<ColumnDefinition> {
                make_file_column(1, "tz", datetime),
                make_file_column(2, "wall", datetime),
                make_file_column(3, "ns", std::make_shared<DataTypeTimeStampNs>()),
                std::move(info_column),
                std::move(tzs_column),
                std::move(by_tz_column)};
    };
    const auto primitive = [](const DataTypePtr& type) {
        return remove_nullable(type)->get_primitive_type();
    };

    auto file_schema = build_file_schema();
    IcebergTableReaderAnnotateTestHelper reader;
    ASSERT_TRUE(reader.annotate(&file_schema, &scan_params).ok());
    ASSERT_EQ(file_schema[0].timestamp_is_adjusted_to_utc, std::optional<bool>(true));
    EXPECT_EQ(primitive(file_schema[0].type), TYPE_TIMESTAMPTZ);
    EXPECT_EQ(file_schema[0].type->get_scale(), 6);
    EXPECT_TRUE(file_schema[0].type->is_nullable());
    EXPECT_FALSE(file_schema[1].timestamp_is_adjusted_to_utc.has_value());
    EXPECT_EQ(primitive(file_schema[1].type), TYPE_DATETIMEV2);
    EXPECT_FALSE(file_schema[2].timestamp_is_adjusted_to_utc.has_value());
    EXPECT_EQ(primitive(file_schema[2].type), TYPE_TIMESTAMP_NS);

    const auto& struct_type =
            assert_cast<const DataTypeStruct&>(*remove_nullable(file_schema[3].type));
    EXPECT_EQ(primitive(struct_type.get_element(0)), TYPE_TIMESTAMPTZ);
    EXPECT_EQ(primitive(struct_type.get_element(1)), TYPE_DATETIMEV2);
    EXPECT_EQ(file_schema[3].children[0].timestamp_is_adjusted_to_utc, std::optional<bool>(true));
    EXPECT_FALSE(file_schema[3].children[1].timestamp_is_adjusted_to_utc.has_value());

    const auto& array_type =
            assert_cast<const DataTypeArray&>(*remove_nullable(file_schema[4].type));
    EXPECT_EQ(primitive(array_type.get_nested_type()), TYPE_TIMESTAMPTZ);
    EXPECT_EQ(file_schema[4].children[0].timestamp_is_adjusted_to_utc, std::optional<bool>(true));

    const auto& map_type = assert_cast<const DataTypeMap&>(*remove_nullable(file_schema[5].type));
    EXPECT_EQ(primitive(map_type.get_key_type()), TYPE_TIMESTAMPTZ);
    EXPECT_EQ(primitive(map_type.get_value_type()), TYPE_DATETIMEV2);
    EXPECT_EQ(file_schema[5].children[0].timestamp_is_adjusted_to_utc, std::optional<bool>(true));
    EXPECT_FALSE(file_schema[5].children[1].timestamp_is_adjusted_to_utc.has_value());

    auto orc_schema = build_file_schema();
    ASSERT_TRUE(reader.annotate(&orc_schema, &scan_params, FileFormat::ORC).ok());
    EXPECT_FALSE(orc_schema[0].timestamp_is_adjusted_to_utc.has_value());
    EXPECT_EQ(primitive(orc_schema[0].type), TYPE_DATETIMEV2);
    EXPECT_EQ(primitive(assert_cast<const DataTypeArray&>(*remove_nullable(orc_schema[4].type))
                                .get_nested_type()),
              TYPE_DATETIMEV2);
}

// Spark and Hive store an Iceberg timestamptz as INT96, which carries no instant annotation, and FE
// always sends Parquet timestamp-semantics version 1, under which INT96 decodes as a UTC wall
// clock. Without FE's timestamptz marker the projection then casts that wall clock to TIMESTAMPTZ
// in the session time zone, so in Asia/Shanghai 2024-01-01T00:00:00.123456Z reads as
// 2023-12-31 16:00:00.123456 UTC, eight hours early. With the marker the column reads as the
// instant whether the file has field ids, relies on the table's name mapping (an `add_files`
// import), or falls back to positional ids, and whatever the catalog's timestamp mapping flag is.
// A plan from an older FE carries no marker and keeps the previous result.
// NOLINTNEXTLINE(readability-function-cognitive-complexity): one file per match rule, two plans.
TEST(IcebergV2ReaderTest, IcebergInt96TimestampTzReadsInstantWithTableMarker) {
    TimezoneUtils::load_timezones_to_cache();
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_iceberg_int96_timestamp_tz_marker_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);
    // 2024-01-01 00:00:00.123456 UTC and 2024-07-01 12:34:56.654321 UTC.
    constexpr int64_t T1_MICROS = 1'704'067'200'123'456L;
    constexpr int64_t T2_MICROS = 1'719'837'296'654'321L;

    enum class Match { FIELD_ID, NAME_MAPPING, POSITION };
    for (const auto match : {Match::FIELD_ID, Match::NAME_MAPPING, Match::POSITION}) {
        const auto file_path =
                (test_dir / ("split_" + std::to_string(static_cast<int>(match)) + ".parquet"))
                        .string();
        write_timestamp_int_parquet_file(file_path, {T1_MICROS, T2_MICROS}, {1, 2}, true,
                                         match == Match::FIELD_ID);
        // The positional fallback numbers the physical top-level columns from 1.
        const int32_t event_time_id = match == Match::POSITION ? 1 : 0;
        const int32_t id_id = match == Match::POSITION ? 2 : 1;
        for (const bool marked : {true, false}) {
            for (const bool mapping_timestamp_tz : {true, false}) {
                SCOPED_TRACE("match=" + std::to_string(static_cast<int>(match)) +
                             " marked=" + std::to_string(marked) +
                             " mapping_timestamp_tz=" + std::to_string(mapping_timestamp_tz));
                auto event_time = external_schema_field(
                        "event_time", event_time_id, {}, std::nullopt,
                        external_primitive_type(TPrimitiveType::TIMESTAMPTZ, -1, 6));
                auto id = external_schema_field("id", id_id);
                if (marked) {
                    event_time.field_ptr->__set_timestamp_is_adjusted_to_utc(true);
                }
                std::vector<ColumnDefinition> projected_columns {
                        make_table_column(id_id, "id", std::make_shared<DataTypeInt32>()),
                        make_table_column(event_time_id, "event_time",
                                          std::make_shared<DataTypeTimeStampTz>(6))};
                if (match == Match::NAME_MAPPING) {
                    for (auto* field : {&event_time, &id}) {
                        field->field_ptr->__set_name_mapping({field->field_ptr->name});
                        field->field_ptr->__set_name_mapping_is_authoritative(true);
                    }
                    for (auto& column : projected_columns) {
                        column.name_mapping = {column.name};
                        column.has_name_mapping = true;
                    }
                }
                auto scan_params = make_local_parquet_scan_params();
                scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
                scan_params.__set_parquet_timestamp_semantics_version(1);
                scan_params.__set_enable_mapping_timestamp_tz(mapping_timestamp_tz);
                scan_params.__set_current_schema_id(100);
                scan_params.__set_history_schema_info({external_schema(100, {event_time, id})});

                RuntimeProfile profile("test_profile");
                RuntimeState state {TQueryOptions(), TQueryGlobals()};
                state.set_timezone("Asia/Shanghai");
                io::FileReaderStats file_reader_stats;
                io::FileCacheStatistics file_cache_stats;
                auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
                ShardedKVCache cache(1);
                doris::format::iceberg::IcebergTableReader reader;
                init_iceberg_reader(&reader, projected_columns, &scan_params, io_ctx, &state,
                                    &profile);
                auto split_options = build_split_options(file_path);
                split_options.cache = &cache;
                split_options.current_range.__set_table_format_params(
                        make_iceberg_table_format_desc(file_path, {}));
                ASSERT_TRUE(reader.prepare_split(split_options).ok());

                std::vector<std::string> event_times;
                bool eos = false;
                while (!eos) {
                    Block block = build_table_block(projected_columns);
                    const auto status = reader.get_block(&block, &eos);
                    ASSERT_TRUE(status.ok()) << status;
                    for (size_t row = 0; row < block.rows(); ++row) {
                        event_times.push_back(projected_columns[1].type->to_string(
                                *block.get_by_position(1).column, row));
                    }
                }
                EXPECT_EQ(event_times,
                          marked ? std::vector<std::string>({"2024-01-01 00:00:00.123456+00:00",
                                                             "2024-07-01 12:34:56.654321+00:00"})
                                 : std::vector<std::string>({"2023-12-31 16:00:00.123456+00:00",
                                                             "2024-07-01 04:34:56.654321+00:00"}));
                ASSERT_TRUE(reader.close().ok());
            }
        }
    }
    std::filesystem::remove_all(test_dir);
}

// The marker also reaches INT96 leaves inside a STRUCT and a LIST, while a timestamp sibling in the
// same struct keeps reading as the UTC wall clock Iceberg gives a timestamp field.
TEST(IcebergV2ReaderTest, IcebergNestedInt96TimestampTzReadsInstantWithTableMarker) {
    TimezoneUtils::load_timezones_to_cache();
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_iceberg_nested_int96_timestamp_tz_marker_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);
    const auto file_path = (test_dir / "split.parquet").string();
    // 2024-01-01 00:00:00.123456 UTC.
    write_nested_int96_parquet_file(file_path, 1'704'067'200'123'456L);
    auto physical_reader = ::parquet::ParquetFileReader::OpenFile(file_path, false);
    for (int leaf = 1; leaf < 4; ++leaf) {
        ASSERT_EQ(::parquet::Type::INT96,
                  physical_reader->metadata()->schema()->Column(leaf)->physical_type());
    }
    physical_reader->Close();

    const auto tz_type = std::make_shared<DataTypeTimeStampTz>(6);
    const auto wall_type = std::make_shared<DataTypeDateTimeV2>(6);
    auto info = make_table_column(2, "info",
                                  std::make_shared<DataTypeStruct>(DataTypes {tz_type, wall_type},
                                                                   Strings {"tz", "wall"}));
    info.children = {make_table_column(3, "tz", tz_type), make_table_column(4, "wall", wall_type)};
    auto tzs = make_table_column(5, "tzs", std::make_shared<DataTypeArray>(tz_type));
    tzs.children = {make_table_column(6, "element", tz_type)};
    std::vector<ColumnDefinition> projected_columns {
            make_table_column(1, "id", std::make_shared<DataTypeInt32>()), info, tzs};

    auto tz_field = external_schema_field("tz", 3);
    tz_field.field_ptr->__set_timestamp_is_adjusted_to_utc(true);
    auto element_field = external_schema_field("element", 6);
    element_field.field_ptr->__set_timestamp_is_adjusted_to_utc(true);
    auto tzs_field = external_schema_field("tzs", 5);
    schema::external::TArrayField array_field;
    array_field.__set_item_field(std::move(element_field));
    tzs_field.field_ptr->nestedField.__set_array_field(std::move(array_field));
    tzs_field.field_ptr->__isset.nestedField = true;
    auto scan_params = make_local_parquet_scan_params();
    scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
    scan_params.__set_parquet_timestamp_semantics_version(1);
    scan_params.__set_enable_mapping_timestamp_tz(true);
    scan_params.__set_current_schema_id(100);
    scan_params.__set_history_schema_info({external_schema(
            100, {external_schema_field("id", 1),
                  external_struct_schema_field(
                          "info", 2, {std::move(tz_field), external_schema_field("wall", 4)}),
                  std::move(tzs_field)})});

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    state.set_timezone("Asia/Shanghai");
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::format::iceberg::IcebergTableReader reader;
    init_iceberg_reader(&reader, projected_columns, &scan_params, io_ctx, &state, &profile);
    auto split_options = build_split_options(file_path);
    split_options.cache = &cache;
    split_options.current_range.__set_table_format_params(
            make_iceberg_table_format_desc(file_path, {}));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    // Table children are nullable, as in an external scan descriptor.
    const auto nullable_tz_type = make_nullable(tz_type);
    const auto nullable_wall_type = make_nullable(wall_type);
    std::vector<std::string> values;
    bool eos = false;
    while (!eos) {
        Block block = build_table_block(projected_columns);
        const auto status = reader.get_block(&block, &eos);
        ASSERT_TRUE(status.ok()) << status;
        if (block.rows() == 0) {
            continue;
        }
        ASSERT_EQ(block.rows(), 1);
        const auto& info_column =
                assert_cast<const ColumnStruct&>(expect_not_null_table_column(block, 1));
        values.push_back(nullable_tz_type->to_string(info_column.get_column(0), 0));
        values.push_back(nullable_wall_type->to_string(info_column.get_column(1), 0));
        const auto& tzs_column =
                assert_cast<const ColumnArray&>(expect_not_null_table_column(block, 2));
        ASSERT_EQ(tzs_column.get_offsets()[0], 1);
        values.push_back(nullable_tz_type->to_string(tzs_column.get_data(), 0));
    }
    EXPECT_EQ(values, std::vector<std::string>({"2024-01-01 00:00:00.123456+00:00",
                                                "2024-01-01 00:00:00.123456",
                                                "2024-01-01 00:00:00.123456+00:00"}));
    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

// TableColumnMapper resolves a nested field ID through an ID-less STRUCT wrapper below the root as
// well (ParquetRecursivelyRetainsIdlessWrapperForSelectedLeafId), so the table's timestamptz marker
// has to reach the leaf the same way. Otherwise the INT96 leaf decodes as a UTC wall clock that the
// projection reads in the session time zone, eight hours early in Asia/Shanghai, and an equality
// key through the wrapper compares that wall clock with the adjusted-UTC tombstone an Iceberg
// writer stores, so the tombstone misses its row.
// NOLINTNEXTLINE(readability-function-cognitive-complexity): read and equality-delete matrix.
TEST(IcebergV2ReaderTest, IcebergInt96TimestampTzUnderIdlessWrapperReadsInstantWithTableMarker) {
    TimezoneUtils::load_timezones_to_cache();
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_iceberg_idless_wrapper_int96_timestamp_tz_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);
    const auto file_path = (test_dir / "split.parquet").string();
    const auto delete_file_path = (test_dir / "equality-delete.parquet").string();
    // 2024-01-01 00:00:00.123456 UTC and one second later.
    constexpr int64_t T1_MICROS = 1'704'067'200'123'456L;
    constexpr int64_t T2_MICROS = T1_MICROS + 1'000'000L;
    write_idless_wrapper_int96_parquet_file(file_path, {T1_MICROS, T2_MICROS}, {1, 2});
    write_iceberg_timestamp_equality_delete_parquet_file(delete_file_path, 30, T1_MICROS, "ts",
                                                         true);

    const auto tz_type = std::make_shared<DataTypeTimeStampTz>(6);
    const auto nullable_tz_type = make_nullable(tz_type);
    auto inner_type = std::make_shared<DataTypeStruct>(DataTypes {tz_type}, Strings {"ts"});
    auto inner = make_table_column(20, "inner", inner_type);
    inner.children = {make_table_column(30, "ts", tz_type)};
    auto outer = make_table_column(
            10, "outer",
            std::make_shared<DataTypeStruct>(DataTypes {inner_type}, Strings {"inner"}));
    outer.children = {inner};

    struct Case {
        bool with_delete;
        bool project_outer;
        std::vector<int32_t> ids;
        std::vector<std::string> timestamps;
    };
    const std::vector<Case> cases = {
            {false,
             true,
             {1, 2},
             {"2024-01-01 00:00:00.123456+00:00", "2024-01-01 00:00:01.123456+00:00"}},
            {true, true, {2}, {"2024-01-01 00:00:01.123456+00:00"}},
            {true, false, {2}, {}},
    };
    for (const auto& c : cases) {
        SCOPED_TRACE("with_delete=" + std::to_string(c.with_delete) +
                     " project_outer=" + std::to_string(c.project_outer));
        std::vector<ColumnDefinition> projected_columns {
                make_table_column(1, "id", std::make_shared<DataTypeInt32>())};
        if (c.project_outer) {
            projected_columns.push_back(outer);
        }
        auto ts_field =
                external_schema_field("ts", 30, {}, std::nullopt,
                                      external_primitive_type(TPrimitiveType::TIMESTAMPTZ, -1, 6));
        ts_field.field_ptr->__set_timestamp_is_adjusted_to_utc(true);
        auto inner_field = external_struct_schema_field("inner", 20, {std::move(ts_field)});
        auto scan_params = make_local_parquet_scan_params();
        scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
        scan_params.__set_parquet_timestamp_semantics_version(1);
        scan_params.__set_enable_mapping_timestamp_tz(true);
        scan_params.__set_current_schema_id(100);
        scan_params.__set_history_schema_info({external_schema(
                100, {external_schema_field("id", 1),
                      external_struct_schema_field("outer", 10, {std::move(inner_field)})})});

        RuntimeProfile profile("test_profile");
        RuntimeState state {TQueryOptions(), TQueryGlobals()};
        state.set_timezone("Asia/Shanghai");
        io::FileReaderStats file_reader_stats;
        io::FileCacheStatistics file_cache_stats;
        auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
        ShardedKVCache cache(1);
        doris::format::iceberg::IcebergTableReader reader;
        init_iceberg_reader(&reader, projected_columns, &scan_params, io_ctx, &state, &profile);
        std::vector<TIcebergDeleteFileDesc> delete_files;
        if (c.with_delete) {
            delete_files.push_back(make_iceberg_equality_delete_file(delete_file_path, {30}));
        }
        auto split_options = build_split_options(file_path);
        split_options.cache = &cache;
        split_options.current_range.__set_table_format_params(
                make_iceberg_table_format_desc(file_path, delete_files));
        ASSERT_TRUE(reader.prepare_split(split_options).ok());

        std::vector<int32_t> ids;
        std::vector<std::string> timestamps;
        bool eos = false;
        while (!eos) {
            Block block = build_table_block(projected_columns);
            const auto status = reader.get_block(&block, &eos);
            ASSERT_TRUE(status.ok()) << status;
            if (block.rows() == 0) {
                continue;
            }
            const auto& id_column =
                    assert_cast<const ColumnInt32&>(expect_not_null_table_column(block, 0));
            for (size_t row = 0; row < block.rows(); ++row) {
                ids.push_back(id_column.get_element(row));
            }
            if (!c.project_outer) {
                continue;
            }
            const auto& outer_column =
                    assert_cast<const ColumnStruct&>(expect_not_null_table_column(block, 1));
            const auto& inner_column = assert_cast<const ColumnStruct&>(
                    expect_not_null_nullable_nested_column(outer_column.get_column(0)));
            for (size_t row = 0; row < block.rows(); ++row) {
                timestamps.push_back(nullable_tz_type->to_string(inner_column.get_column(0), row));
            }
        }
        EXPECT_EQ(ids, c.ids);
        EXPECT_EQ(timestamps, c.timestamps);
        ASSERT_TRUE(reader.close().ok());
    }
    std::filesystem::remove_all(test_dir);
}

// A Spark INT96 key and the adjusted-UTC tombstone an Iceberg writer stores for the same
// timestamptz instant decode to the same TIMESTAMPTZ once FE marks the field, in both the data file
// and the delete file. Under timestamp-semantics version 1 the data key used to read as a UTC wall
// clock and was converted back in the session time zone, so in Asia/Shanghai the tombstone T1 missed
// the row holding T1 by eight hours; with enable_mapping_timestamp_tz=false the tombstone also read
// as a Shanghai wall clock, and the two wall clocks never matched.
// NOLINTNEXTLINE(readability-function-cognitive-complexity): projection and mapping-flag matrix.
TEST(IcebergV2ReaderTest, IcebergEqualityDeleteMatchesInt96TimestampTzKeyWithTableMarker) {
    TimezoneUtils::load_timezones_to_cache();
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_iceberg_equality_delete_int96_marker_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);
    const auto file_path = (test_dir / "split.parquet").string();
    const auto delete_file_path = (test_dir / "equality-delete.parquet").string();
    // 2024-01-01 00:00:00.123456 UTC and one second later.
    constexpr int64_t T1_MICROS = 1'704'067'200'123'456L;
    constexpr int64_t T2_MICROS = T1_MICROS + 1'000'000L;
    write_timestamp_int_parquet_file(file_path, {T1_MICROS, T2_MICROS}, {1, 2}, true);
    write_iceberg_timestamp_equality_delete_parquet_file(delete_file_path, 0, T1_MICROS,
                                                         "event_time", true);

    for (const bool project_key : {false, true}) {
        for (const bool mapping_timestamp_tz : {true, false}) {
            SCOPED_TRACE("project_key=" + std::to_string(project_key) +
                         " mapping_timestamp_tz=" + std::to_string(mapping_timestamp_tz));
            std::vector<ColumnDefinition> projected_columns {
                    make_table_column(1, "id", std::make_shared<DataTypeInt32>())};
            if (project_key) {
                projected_columns.push_back(make_table_column(
                        0, "event_time", std::make_shared<DataTypeTimeStampTz>(6)));
            }
            auto event_time = external_schema_field(
                    "event_time", 0, {}, std::nullopt,
                    external_primitive_type(TPrimitiveType::TIMESTAMPTZ, -1, 6));
            event_time.field_ptr->__set_timestamp_is_adjusted_to_utc(true);
            auto scan_params = make_local_parquet_scan_params();
            scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
            scan_params.__set_parquet_timestamp_semantics_version(1);
            scan_params.__set_enable_mapping_timestamp_tz(mapping_timestamp_tz);
            scan_params.__set_current_schema_id(100);
            scan_params.__set_history_schema_info(
                    {external_schema(100, {event_time, external_schema_field("id", 1)})});

            RuntimeProfile profile("test_profile");
            RuntimeState state {TQueryOptions(), TQueryGlobals()};
            state.set_timezone("Asia/Shanghai");
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

            std::vector<int32_t> ids;
            std::vector<std::string> event_times;
            bool eos = false;
            while (!eos) {
                Block block = build_table_block(projected_columns);
                const auto status = reader.get_block(&block, &eos);
                ASSERT_TRUE(status.ok()) << status;
                if (block.rows() == 0) {
                    continue;
                }
                const auto& id_column =
                        assert_cast<const ColumnInt32&>(expect_not_null_table_column(block, 0));
                for (size_t row = 0; row < block.rows(); ++row) {
                    ids.push_back(id_column.get_element(row));
                    if (project_key) {
                        event_times.push_back(projected_columns[1].type->to_string(
                                *block.get_by_position(1).column, row));
                    }
                }
            }
            EXPECT_EQ(ids, std::vector<int32_t>({2}));
            if (project_key) {
                EXPECT_EQ(event_times,
                          std::vector<std::string>({"2024-01-01 00:00:01.123456+00:00"}));
            }
            ASSERT_TRUE(reader.close().ok());
        }
    }
    std::filesystem::remove_all(test_dir);
}

// A catalog with enable_mapping_timestamp_tz=false used to read both sides of a timestamptz equality
// delete as session-local wall clocks. During the 2024 New York fall-back 01:30 happens twice, so
// either tombstone deleted both rows. Once FE marks the field, data keys and tombstones both decode
// as instants and each tombstone deletes only its own row. Decoding only the data keys as instants
// is not enough: the tombstone's wall clock maps back to one of the two instants, so one of these
// two cases would delete the wrong row.
TEST(IcebergV2ReaderTest, IcebergEqualityDeleteKeepsDstFoldInstantsApartWithTableMarker) {
    TimezoneUtils::load_timezones_to_cache();
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_iceberg_equality_delete_dst_fold_marker_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);
    const auto file_path = (test_dir / "split.parquet").string();
    // These instants are the two occurrences of 01:30 during the 2024 New York DST fall-back.
    constexpr int64_t FIRST_0130_MICROS = 1'730'611'800'000'000L;
    constexpr int64_t SECOND_0130_MICROS = 1'730'615'400'000'000L;
    write_timestamp_int_parquet_file(file_path, {FIRST_0130_MICROS, SECOND_0130_MICROS}, {1, 2});

    for (const auto& [tombstone, remaining_id] :
         {std::pair {FIRST_0130_MICROS, 2}, std::pair {SECOND_0130_MICROS, 1}}) {
        SCOPED_TRACE(tombstone);
        // Same size and second-granularity mtime: one path for both files could serve the first
        // tombstone from the Parquet caches.
        const auto delete_file_path =
                (test_dir / ("equality-delete-" + std::to_string(remaining_id) + ".parquet"))
                        .string();
        write_iceberg_timestamp_equality_delete_parquet_file(delete_file_path, 0, tombstone,
                                                             "event_time", true);
        std::vector<ColumnDefinition> projected_columns {
                make_table_column(1, "id", std::make_shared<DataTypeInt32>())};
        auto event_time =
                external_schema_field("event_time", 0, {}, std::nullopt,
                                      external_primitive_type(TPrimitiveType::TIMESTAMPTZ, -1, 6));
        event_time.field_ptr->__set_timestamp_is_adjusted_to_utc(true);
        auto scan_params = make_local_parquet_scan_params();
        scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
        scan_params.__set_parquet_timestamp_semantics_version(1);
        scan_params.__set_enable_mapping_timestamp_tz(false);
        scan_params.__set_current_schema_id(100);
        scan_params.__set_history_schema_info(
                {external_schema(100, {event_time, external_schema_field("id", 1)})});

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
        EXPECT_EQ(read_iceberg_ids(&reader, projected_columns),
                  std::vector<int32_t>({remaining_id}));
        ASSERT_TRUE(reader.close().ok());
    }
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

TEST(IcebergV2ReaderTest, IcebergEqualityDeleteUsesDeleteFileNameForIdlessParquet) {
    expect_idless_equality_key_uses_delete_file_name(FileFormat::PARQUET, false);
    expect_idless_equality_key_uses_delete_file_name(FileFormat::PARQUET, true);
}

TEST(IcebergV2ReaderTest, IcebergEqualityDeleteUsesDeleteFileNameForIdlessOrc) {
    expect_idless_equality_key_uses_delete_file_name(FileFormat::ORC, false);
    expect_idless_equality_key_uses_delete_file_name(FileFormat::ORC, true);
}

TEST(IcebergV2ReaderTest, ParquetRecursivelyRetainsIdlessWrapperForSelectedLeafId) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_iceberg_recursive_idless_wrapper_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);
    const auto file_path = (test_dir / "split.parquet").string();
    write_recursive_idless_wrapper_parquet_file(file_path, 42);

    const auto int_type = std::make_shared<DataTypeInt32>();
    auto leaf = make_table_column(30, "leaf", int_type);
    auto inner_type = std::make_shared<DataTypeStruct>(DataTypes {int_type}, Strings {"leaf"});
    auto inner = make_table_column(20, "inner", inner_type);
    inner.children = {leaf};
    auto outer_type = std::make_shared<DataTypeStruct>(DataTypes {inner_type}, Strings {"inner"});
    auto outer = make_table_column(10, "outer", outer_type);
    outer.children = {inner};
    std::vector<ColumnDefinition> projected_columns = {outer};

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
    split_options.current_range.__set_table_format_params(
            make_iceberg_table_format_desc(file_path, {}));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);
    const auto& outer_result =
            assert_cast<const ColumnStruct&>(expect_not_null_table_column(block, 0));
    const auto& inner_result = assert_cast<const ColumnStruct&>(
            expect_not_null_nullable_nested_column(outer_result.get_column(0)));
    const auto& leaf_result = assert_cast<const ColumnInt32&>(
            expect_not_null_nullable_nested_column(inner_result.get_column(0)));
    ASSERT_EQ(leaf_result.size(), 1);
    EXPECT_EQ(leaf_result.get_element(0), 42);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, ParquetReadsIdlessWrapperWithAuthoritativeEmptyMapping) {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_iceberg_authoritative_empty_idless_wrapper_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);
    const auto file_path = (test_dir / "split.parquet").string();
    write_recursive_idless_wrapper_parquet_file(file_path, 42, false);

    const auto int_type = std::make_shared<DataTypeInt32>();
    auto leaf = make_table_column(30, "leaf", int_type);
    auto inner_type = std::make_shared<DataTypeStruct>(DataTypes {int_type}, Strings {"leaf"});
    auto inner = make_table_column(20, "inner", inner_type);
    inner.children = {leaf};
    inner.has_name_mapping = true;
    auto outer_type = std::make_shared<DataTypeStruct>(DataTypes {inner_type}, Strings {"inner"});
    auto outer = make_table_column(10, "outer", outer_type);
    outer.children = {inner};
    outer.has_name_mapping = true;
    std::vector<ColumnDefinition> projected_columns = {outer};

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
    split_options.current_range.__set_table_format_params(
            make_iceberg_table_format_desc(file_path, {}));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);
    const auto& outer_result =
            assert_cast<const ColumnStruct&>(expect_not_null_table_column(block, 0));
    const auto& inner_result = assert_cast<const ColumnStruct&>(
            expect_not_null_nullable_nested_column(outer_result.get_column(0)));
    const auto& leaf_result = assert_cast<const ColumnInt32&>(
            expect_not_null_nullable_nested_column(inner_result.get_column(0)));
    ASSERT_EQ(leaf_result.size(), 1);
    EXPECT_EQ(leaf_result.get_element(0), 42);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, ParquetTreatsIdlessNestedFileAsMissingWithoutNameMapping) {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_iceberg_idless_nested_without_name_mapping_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);
    const auto file_path = (test_dir / "split.parquet").string();
    write_nested_equality_parquet_file(file_path, {}, {42}, {false}, true, "leaf", 30, "outer",
                                       false, false);

    const auto int_type = std::make_shared<DataTypeInt32>();
    auto leaf = make_table_column(30, "leaf", int_type);
    auto outer_type = std::make_shared<DataTypeStruct>(DataTypes {int_type}, Strings {"leaf"});
    auto outer = make_table_column(10, "outer", outer_type);
    outer.children = {leaf};
    std::vector<ColumnDefinition> projected_columns = {outer};

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::format::iceberg::IcebergTableReader reader;
    init_iceberg_reader(&reader, projected_columns, &scan_params, io_ctx, &state, &profile);
    auto split_options = build_split_options(file_path);
    split_options.cache = &cache;
    split_options.current_range.__set_table_format_params(
            make_iceberg_table_format_desc(file_path, {}));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);
    const auto& result = assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result.get_null_map_data()[0], 1);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, ParquetUsesUnprojectedSiblingIdToRetainNullableWrapper) {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_iceberg_unprojected_sibling_id_wrapper_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);
    const auto file_path = (test_dir / "split.parquet").string();
    write_nullable_idless_struct_with_sibling_id_parquet_file(file_path);

    const auto int_type = std::make_shared<DataTypeInt32>();
    auto projected_struct_type =
            std::make_shared<DataTypeStruct>(DataTypes {int_type}, Strings {"a"});
    auto projected_s = make_table_column(10, "s", projected_struct_type);

    auto schema_a =
            external_schema_field("a", 1, {}, "7", external_primitive_type(TPrimitiveType::INT));
    auto schema_b = external_schema_field("b", 2, {}, std::nullopt,
                                          external_primitive_type(TPrimitiveType::INT));
    schema::external::TStructField struct_children;
    struct_children.__set_fields({schema_a, schema_b});
    auto schema_s = external_schema_field("s", 10, {}, std::nullopt,
                                          external_primitive_type(TPrimitiveType::STRUCT));
    schema_s.field_ptr->nestedField.__set_struct_field(std::move(struct_children));
    schema_s.field_ptr->__isset.nestedField = true;

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
    scan_params.__set_current_schema_id(100);
    scan_params.__set_history_schema_info({external_schema(100, {schema_s})});
    ProjectedColumnBuildContext context {.scan_params = &scan_params};
    TFileScanSlotInfo slot_info;
    doris::format::iceberg::IcebergTableReader annotation_reader;
    ASSERT_TRUE(
            annotation_reader.annotate_projected_column(slot_info, &context, &projected_s).ok());
    ASSERT_TRUE(context.schema_column.has_value());
    ASSERT_TRUE(AccessPathParser::build_nested_children(
                        &projected_s,
                        std::vector<TColumnAccessPath> {nested_data_access_path({"s", "a"})},
                        &*context.schema_column)
                        .ok());
    std::vector<ColumnDefinition> projected_columns = {projected_s};
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::format::iceberg::IcebergTableReader reader;
    init_iceberg_reader(&reader, projected_columns, &scan_params, io_ctx, &state, &profile);
    auto split_options = build_split_options(file_path);
    split_options.cache = &cache;
    split_options.current_range.__set_table_format_params(
            make_iceberg_table_format_desc(file_path, {}));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);
    const auto result = block.get_by_position(0).column->convert_to_full_column_if_const();
    const auto& nullable_s = assert_cast<const ColumnNullable&>(*result);
    ASSERT_EQ(nullable_s.size(), 2);
    EXPECT_TRUE(nullable_s.is_null_at(0));
    EXPECT_FALSE(nullable_s.is_null_at(1));
    const auto& struct_s = assert_cast<const ColumnStruct&>(nullable_s.get_nested_column());
    const auto& nullable_a = assert_cast<const ColumnNullable&>(struct_s.get_column(0));
    EXPECT_FALSE(nullable_a.is_null_at(1));
    const auto& values = assert_cast<const ColumnInt32&>(nullable_a.get_nested_column());
    EXPECT_EQ(values.get_element(1), 7);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, ReusedRootNameReadsNewFieldInitialDefault) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_iceberg_reused_root_name_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);
    const auto file_path = (test_dir / "split.parquet").string();
    write_single_int_parquet_file(file_path, "b", {10}, 1);

    auto renamed_b = external_schema_field("renamed_b", 1, {"b"}, std::nullopt,
                                           external_primitive_type(TPrimitiveType::INT));
    renamed_b.field_ptr->__set_name_mapping_is_authoritative(true);
    auto current_b =
            external_schema_field("b", 2, {}, "7", external_primitive_type(TPrimitiveType::INT));
    current_b.field_ptr->__set_name_mapping({});
    current_b.field_ptr->__set_name_mapping_is_authoritative(true);

    auto scan_params = make_local_parquet_scan_params();
    scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
    scan_params.__set_current_schema_id(100);
    scan_params.__set_history_schema_info({external_schema(100, {renamed_b, current_b})});

    auto projected_b = make_table_column(-1, "b", std::make_shared<DataTypeInt32>());
    ProjectedColumnBuildContext context {.scan_params = &scan_params};
    TFileScanSlotInfo slot_info;
    doris::format::iceberg::IcebergTableReader annotation_reader;
    ASSERT_TRUE(
            annotation_reader.annotate_projected_column(slot_info, &context, &projected_b).ok());
    std::vector<ColumnDefinition> projected_columns = {projected_b};

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
    split_options.current_range.__set_table_format_params(
            make_iceberg_table_format_desc(file_path, {}));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    EXPECT_EQ(read_iceberg_ids(&reader, projected_columns), std::vector<int32_t>({7}));

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, LegacyPlanRetainsOrderedRootNameAndAliasLookupWithAllFieldIds) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_iceberg_legacy_reused_root_name_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);
    const auto file_path = (test_dir / "split.parquet").string();
    write_two_int_parquet_file(file_path, "renamed_b", {10}, 1, "b", {20}, 2);

    auto renamed_b = external_schema_field("renamed_b", 1, {"b"}, std::nullopt,
                                           external_primitive_type(TPrimitiveType::INT));
    renamed_b.field_ptr->__set_name_mapping_is_authoritative(true);
    auto current_b = external_schema_field("b", 2, {}, std::nullopt,
                                           external_primitive_type(TPrimitiveType::INT));
    current_b.field_ptr->__set_name_mapping({});
    current_b.field_ptr->__set_name_mapping_is_authoritative(true);

    TFileScanRangeParams old_fe_scan_params;
    old_fe_scan_params.__set_file_type(TFileType::FILE_LOCAL);
    old_fe_scan_params.__set_format_type(TFileFormatType::FORMAT_PARQUET);
    old_fe_scan_params.__set_current_schema_id(100);
    old_fe_scan_params.__set_history_schema_info({external_schema(100, {renamed_b, current_b})});

    auto projected_b = make_table_column(-1, "b", std::make_shared<DataTypeInt32>());
    ProjectedColumnBuildContext context {.scan_params = &old_fe_scan_params};
    TFileScanSlotInfo slot_info;
    doris::format::iceberg::IcebergTableReader annotation_reader;
    ASSERT_TRUE(
            annotation_reader.annotate_projected_column(slot_info, &context, &projected_b).ok());
    ASSERT_EQ(projected_b.get_identifier_field_id(), 1);
    std::vector<ColumnDefinition> projected_columns = {projected_b};

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::format::iceberg::IcebergTableReader reader;
    init_iceberg_reader(&reader, projected_columns, &old_fe_scan_params, io_ctx, &state, &profile);
    auto split_options = build_split_options(file_path);
    split_options.cache = &cache;
    split_options.current_range.__set_table_format_params(
            make_iceberg_table_format_desc(file_path, {}));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    EXPECT_EQ(read_iceberg_ids(&reader, projected_columns), std::vector<int32_t>({10}));

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, ReusedNestedNameReadsNewFieldInitialDefault) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_iceberg_reused_nested_name_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);
    const auto file_path = (test_dir / "split.parquet").string();
    write_nullable_renamed_struct_child_parquet_file(file_path);

    auto renamed_b = external_schema_field("renamed_b", 1, {"b"}, std::nullopt,
                                           external_primitive_type(TPrimitiveType::INT));
    renamed_b.field_ptr->__set_name_mapping_is_authoritative(true);
    auto current_b =
            external_schema_field("b", 2, {}, "7", external_primitive_type(TPrimitiveType::INT));
    current_b.field_ptr->__set_name_mapping({});
    current_b.field_ptr->__set_name_mapping_is_authoritative(true);
    schema::external::TStructField struct_children;
    struct_children.__set_fields({renamed_b, current_b});
    auto schema_s = external_schema_field("s", 10, {}, std::nullopt,
                                          external_primitive_type(TPrimitiveType::STRUCT));
    schema_s.field_ptr->nestedField.__set_struct_field(std::move(struct_children));
    schema_s.field_ptr->__isset.nestedField = true;

    auto scan_params = make_local_parquet_scan_params();
    scan_params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
    scan_params.__set_current_schema_id(100);
    scan_params.__set_history_schema_info({external_schema(100, {schema_s})});

    const auto int_type = std::make_shared<DataTypeInt32>();
    auto struct_type = std::make_shared<DataTypeStruct>(DataTypes {int_type}, Strings {"b"});
    auto projected_s = make_table_column(-1, "s", struct_type);
    ProjectedColumnBuildContext context {.scan_params = &scan_params};
    TFileScanSlotInfo slot_info;
    doris::format::iceberg::IcebergTableReader annotation_reader;
    ASSERT_TRUE(
            annotation_reader.annotate_projected_column(slot_info, &context, &projected_s).ok());
    ASSERT_TRUE(context.schema_column.has_value());
    ASSERT_TRUE(AccessPathParser::build_nested_children(
                        &projected_s,
                        std::vector<TColumnAccessPath> {nested_data_access_path({"s", "b"})},
                        &*context.schema_column)
                        .ok());
    std::vector<ColumnDefinition> projected_columns = {projected_s};

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
    split_options.current_range.__set_table_format_params(
            make_iceberg_table_format_desc(file_path, {}));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);
    const auto result = block.get_by_position(0).column->convert_to_full_column_if_const();
    const auto& nullable_s = assert_cast<const ColumnNullable&>(*result);
    ASSERT_EQ(nullable_s.size(), 2);
    EXPECT_TRUE(nullable_s.is_null_at(0));
    EXPECT_FALSE(nullable_s.is_null_at(1));
    const auto& struct_s = assert_cast<const ColumnStruct&>(nullable_s.get_nested_column());
    const auto& nullable_b = assert_cast<const ColumnNullable&>(struct_s.get_column(0));
    EXPECT_FALSE(nullable_b.is_null_at(1));
    const auto& values = assert_cast<const ColumnInt32&>(nullable_b.get_nested_column());
    EXPECT_EQ(values.get_element(1), 7);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, LegacyPlanUsesNestedNameMappingForMixedFieldIds) {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_iceberg_legacy_nested_name_mapping_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);
    const auto file_path = (test_dir / "split.parquet").string();
    write_nullable_idless_struct_with_sibling_id_parquet_file(file_path);

    const auto int_type = std::make_shared<DataTypeInt32>();
    auto projected_a = make_table_column(1, "a", int_type);
    projected_a.name_mapping = {"b"};
    projected_a.has_name_mapping = true;
    auto projected_struct_type =
            std::make_shared<DataTypeStruct>(DataTypes {int_type}, Strings {"a"});
    auto projected_s = make_table_column(10, "s", projected_struct_type);
    projected_s.children = {projected_a};
    std::vector<ColumnDefinition> projected_columns = {projected_s};

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    TFileScanRangeParams old_fe_scan_params;
    old_fe_scan_params.__set_file_type(TFileType::FILE_LOCAL);
    old_fe_scan_params.__set_format_type(TFileFormatType::FORMAT_PARQUET);
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::format::iceberg::IcebergTableReader reader;
    init_iceberg_reader(&reader, projected_columns, &old_fe_scan_params, io_ctx, &state, &profile);
    auto split_options = build_split_options(file_path);
    split_options.cache = &cache;
    split_options.current_range.__set_table_format_params(
            make_iceberg_table_format_desc(file_path, {}));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);
    const auto result = block.get_by_position(0).column->convert_to_full_column_if_const();
    const auto& nullable_s = assert_cast<const ColumnNullable&>(*result);
    ASSERT_EQ(nullable_s.size(), 2);
    EXPECT_TRUE(nullable_s.is_null_at(0));
    EXPECT_FALSE(nullable_s.is_null_at(1));
    const auto& struct_s = assert_cast<const ColumnStruct&>(nullable_s.get_nested_column());
    const auto& nullable_a = assert_cast<const ColumnNullable&>(struct_s.get_column(0));
    EXPECT_FALSE(nullable_a.is_null_at(1));
    const auto& values = assert_cast<const ColumnInt32&>(nullable_a.get_nested_column());
    EXPECT_EQ(values.get_element(1), 42);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, IcebergEqualityDeletePrefersExistingFieldIdInMixedSchema) {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_iceberg_equality_delete_stale_field_id_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    const auto delete_file_path = (test_dir / "equality-delete.parquet").string();
    // Iceberg's existential hasIds contract makes the physical field carrying ID 0 authoritative;
    // an ID-less alias cannot switch a mixed schema back to name projection.
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
    EXPECT_EQ(read_iceberg_ids(&reader, projected_columns), std::vector<int32_t>({100, 200, 300}));

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
    auto* cache_misses = profile.get_counter("EqualityDeleteIndexCacheMissCount");
    auto* cache_hits = profile.get_counter("EqualityDeleteIndexCacheHitCount");
    auto* index_memory = profile.get_counter("EqualityDeleteHashIndexMemory");
    ASSERT_NE(cache_misses, nullptr);
    ASSERT_NE(cache_hits, nullptr);
    ASSERT_NE(index_memory, nullptr);
    EXPECT_EQ(cache_misses->value(), 1);
    EXPECT_EQ(cache_hits->value(), 0);
    EXPECT_GT(index_memory->value(), 0);
    const auto first_index_memory = index_memory->value();

    // Removing the source after the first split proves that the second split consumes the parsed
    // delete block from SplitReadOptions.cache instead of reopening the delete file.
    ASSERT_TRUE(std::filesystem::remove(delete_file_path));
    auto second_split = build_split_options(second_file_path);
    second_split.cache = &cache;
    second_split.current_range.__set_table_format_params(make_iceberg_table_format_desc(
            second_file_path, {make_iceberg_equality_delete_file(delete_file_path, {0})}));
    ASSERT_TRUE(reader.prepare_split(second_split).ok());
    EXPECT_EQ(read_iceberg_ids(&reader, projected_columns), std::vector<int32_t>({1, 3}));
    EXPECT_EQ(cache_misses->value(), 1);
    EXPECT_EQ(cache_hits->value(), 1);
    EXPECT_EQ(index_memory->value(), first_index_memory);

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
    const auto* total_timer = profile.get_counter("TableReader");
    const auto* prepare_timer = profile.get_counter("PrepareSplitTime");
    ASSERT_NE(total_timer, nullptr);
    ASSERT_NE(prepare_timer, nullptr);
    const int64_t total_before = total_timer->value();
    const int64_t prepare_before = prepare_timer->value();
    constexpr int64_t DELETE_FILE_DELAY_NS = 8'000'000;
    {
        ScopedDebugPoint delay_delete_file_scan(
                "IcebergTableReader.prepare_split.before_delete_file_scan",
                [] { std::this_thread::sleep_for(std::chrono::milliseconds(8)); });
        ASSERT_TRUE(reader.prepare_split(first_split).ok());
    }
    const int64_t total_delta = total_timer->value() - total_before;
    const int64_t prepare_delta = prepare_timer->value() - prepare_before;
    // A cache-miss delete-file scan is derived prepare work; both common parent timers must
    // contain it so the expensive miss cannot disappear between profile levels.
    EXPECT_GE(prepare_delta, DELETE_FILE_DELAY_NS);
    EXPECT_GE(total_delta, DELETE_FILE_DELAY_NS);
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

TEST(IcebergV2ReaderTest, IcebergEqualityDeleteFileSizePropagatedToReader) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_iceberg_eq_delete_file_size_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    const auto delete_file_path = (test_dir / "equality-delete.parquet").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3}, {10, 20, 30}, {"one", "two", "three"});
    write_iceberg_equality_delete_parquet_file(delete_file_path, 0, 2);

    const auto delete_file_size =
            static_cast<int64_t>(std::filesystem::file_size(delete_file_path));
    ASSERT_GT(delete_file_size, 0) << "delete file should not be empty";

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

    // A truncated size must break the read; a correct size is indistinguishable from the stat fallback.
    auto split_options = build_split_options(file_path);
    split_options.cache = &cache;
    split_options.current_range.__set_table_format_params(make_iceberg_table_format_desc(
            file_path, {make_iceberg_equality_delete_file(delete_file_path, {0},
                                                          TFileFormatType::FORMAT_PARQUET,
                                                          delete_file_size - 16)}));

    const bool prepare_ok = reader.prepare_split(split_options).ok();
    Block block = build_table_block(projected_columns);
    bool eos = false;
    const bool read_ok = prepare_ok && reader.get_block(&block, &eos).ok();
    ASSERT_FALSE(read_ok) << "truncated file_size must fail the delete-file read";

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(IcebergV2ReaderTest, IcebergEqualityDeleteFileSizeUnknownFallsBackToStat) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_iceberg_eq_delete_no_file_size_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    const auto delete_file_path = (test_dir / "equality-delete.parquet").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3}, {10, 20, 30}, {"one", "two", "three"});
    write_iceberg_equality_delete_parquet_file(delete_file_path, 0, 2);

    TIcebergDeleteFileDesc delete_file;
    delete_file.__set_content(2);
    delete_file.__set_path(delete_file_path);
    delete_file.__set_field_ids({0});
    delete_file.__set_file_format(TFileFormatType::FORMAT_PARQUET);

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
    split_options.current_range.__set_table_format_params(
            make_iceberg_table_format_desc(file_path, {delete_file}));
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

TEST(IcebergTableReaderTest, DeleteParquetFactoryReusesScannerCounters) {
    RuntimeProfile profile("iceberg");
    doris::format::parquet::ParquetProfileCache cache(&profile);
    doris::format::iceberg::IcebergTableReader reader;
    reader._scanner_profile = &profile;
    reader._parquet_profile_cache = &cache;
    TFileScanRangeParams params;
    TQueryOptions options;
    TQueryGlobals globals;
    RuntimeState state(options, globals);
    reader._runtime_state = &state;
    IcebergDeleteFileIOContext io_context(&state);
    const auto file_path =
            (std::filesystem::temp_directory_path() /
             ("iceberg_profile_" +
              std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) +
              ".parquet"))
                    .string();
    write_single_int_parquet_file(file_path, "id", {1}, 1);
    for (int i = 0; i < 2; ++i) {
        TIcebergDeleteFileDesc desc;
        desc.__set_file_format(TFileFormatType::FORMAT_PARQUET);
        desc.__set_path(file_path);
        desc.__set_file_size(std::filesystem::file_size(file_path));
        std::unique_ptr<doris::format::FileReader> file;
        const auto status = reader._create_delete_file_reader(desc, params, &io_context, &file);
        ASSERT_TRUE(status.ok()) << status.to_string();
        auto* parquet = dynamic_cast<doris::format::parquet::ParquetReader*>(file.get());
        ASSERT_NE(parquet, nullptr);
        EXPECT_EQ(parquet->_profile_cache, &cache);
        parquet->_init_profile();
        EXPECT_EQ(parquet->_parquet_profile.total_time, cache.get().total_time);
    }
    std::filesystem::remove(file_path);
}

} // namespace
} // namespace doris::format
