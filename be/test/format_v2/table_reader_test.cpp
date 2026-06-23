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

#include "format_v2/table_reader.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <gtest/gtest.h>
#include <parquet/api/reader.h>
#include <parquet/arrow/writer.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <typeinfo>
#include <vector>

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
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "exec/common/endian.h"
#include "exprs/runtime_filter_expr.h"
#include "exprs/vectorized_fn_call.h"
#include "exprs/vexpr.h"
#include "exprs/vliteral.h"
#include "exprs/vslot_ref.h"
#include "format/format_common.h"
#include "format/table/deletion_vector_reader.h"
#include "format_v2/table/iceberg_reader.h"
#include "format_v2/table/paimon_reader.h"
#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "io/io_common.h"
#include "roaring/roaring64map.hh"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "storage/predicate/predicate_creator.h"
#include "storage/segment/condition_cache.h"

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

TEST(LocalColumnIndexTest, MergeUnionsPartialChildrenAndFullProjectionDominates) {
    LocalColumnIndex target {.index = 10, .project_all_children = false};
    target.children.push_back({.index = 1});
    target.children.push_back({.index = 2, .project_all_children = false});
    target.children.back().children.push_back({.index = 20});

    LocalColumnIndex source {.index = 10, .project_all_children = false};
    source.children.push_back({.index = 2, .project_all_children = false});
    source.children.back().children.push_back({.index = 21});
    source.children.push_back({.index = 3});

    ASSERT_TRUE(merge_local_column_index(&target, source).ok());
    ASSERT_FALSE(target.project_all_children);
    ASSERT_EQ(std::vector<int32_t>({1, 2, 3}), projection_ids(target.children));
    ASSERT_FALSE(target.children[1].project_all_children);
    ASSERT_EQ(std::vector<int32_t>({20, 21}), projection_ids(target.children[1].children));
    ASSERT_TRUE(target.children[2].project_all_children);

    LocalColumnIndex full_source {.index = 10};
    ASSERT_TRUE(merge_local_column_index(&target, full_source).ok());
    ASSERT_TRUE(target.project_all_children);
    ASSERT_TRUE(target.children.empty());
}

TEST(LocalColumnIndexTest, FindsProjectedChildren) {
    LocalColumnIndex projection {.index = 10, .project_all_children = false};
    projection.children.push_back({.index = 1});
    projection.children.push_back({.index = 2});

    EXPECT_TRUE(is_full_projection(nullptr));
    EXPECT_FALSE(is_full_projection(&projection));
    EXPECT_TRUE(is_partial_projection(&projection));
    ASSERT_NE(find_child_projection(&projection, 2), nullptr);
    EXPECT_EQ(find_child_projection(&projection, 2)->local_id(), 2);
    EXPECT_EQ(find_child_projection(&projection, 3), nullptr);
    EXPECT_TRUE(is_child_projected(nullptr, 3));
    EXPECT_TRUE(is_child_projected(&projection, 1));
    EXPECT_FALSE(is_child_projected(&projection, 3));
}

TEST(LocalColumnIndexTest, ProjectColumnDefinitionMatchesChildrenByLocalId) {
    auto int_type = std::make_shared<DataTypeInt32>();
    auto string_type = std::make_shared<DataTypeString>();
    ColumnDefinition field;
    field.identifier = Field::create_field<TYPE_INT>(5);
    field.name = "root";
    field.type =
            std::make_shared<DataTypeStruct>(DataTypes {int_type, string_type}, Strings {"a", "b"});
    ColumnDefinition a_child;
    a_child.identifier = Field::create_field<TYPE_INT>(10);
    a_child.local_id = 0;
    a_child.name = "a";
    a_child.type = int_type;
    ColumnDefinition b_child;
    b_child.identifier = Field::create_field<TYPE_INT>(20);
    b_child.local_id = 1;
    b_child.name = "b";
    b_child.type = string_type;
    field.children = {
            a_child,
            b_child,
    };
    LocalColumnIndex projection {.index = 5, .project_all_children = false};
    projection.children.push_back({.index = 1});

    ColumnDefinition projected_field;
    ASSERT_TRUE(project_column_definition(field, projection, &projected_field).ok());
    ASSERT_EQ(projected_field.children.size(), 1);
    EXPECT_EQ(projected_field.children[0].get_identifier_field_id(), 20);
    EXPECT_EQ(projected_field.children[0].name, "b");

    const auto* projected_type =
            assert_cast<const DataTypeStruct*>(remove_nullable(projected_field.type).get());
    ASSERT_EQ(projected_type->get_elements().size(), 1);
    EXPECT_EQ(projected_type->get_element_name(0), "b");
    EXPECT_TRUE(projected_type->get_element(0)->equals(*string_type));
}

TEST(LocalColumnIndexTest, ProjectColumnDefinitionKeepsFileChildOrder) {
    auto int_type = std::make_shared<DataTypeInt32>();
    auto string_type = std::make_shared<DataTypeString>();
    ColumnDefinition a_child;
    a_child.identifier = Field::create_field<TYPE_INT>(10);
    a_child.local_id = 0;
    a_child.name = "a";
    a_child.type = int_type;
    ColumnDefinition b_child;
    b_child.identifier = Field::create_field<TYPE_INT>(20);
    b_child.local_id = 1;
    b_child.name = "b";
    b_child.type = string_type;

    ColumnDefinition field;
    field.identifier = Field::create_field<TYPE_INT>(5);
    field.name = "root";
    field.type =
            std::make_shared<DataTypeStruct>(DataTypes {int_type, string_type}, Strings {"a", "b"});
    field.children = {a_child, b_child};

    LocalColumnIndex projection {.index = 5, .project_all_children = false};
    projection.children.push_back({.index = 1});
    projection.children.push_back({.index = 0});

    ColumnDefinition projected_field;
    ASSERT_TRUE(project_column_definition(field, projection, &projected_field).ok());
    ASSERT_EQ(projected_field.children.size(), 2);
    EXPECT_EQ(projected_field.children[0].name, "a");
    EXPECT_EQ(projected_field.children[1].name, "b");

    const auto* projected_type =
            assert_cast<const DataTypeStruct*>(remove_nullable(projected_field.type).get());
    ASSERT_EQ(projected_type->get_elements().size(), 2);
    EXPECT_EQ(projected_type->get_element_name(0), "a");
    EXPECT_EQ(projected_type->get_element_name(1), "b");
}

VExprSPtr table_int32_slot_ref(int slot_id, int column_id, const std::string& column_name) {
    return VSlotRef::create_shared(slot_id, column_id, slot_id, std::make_shared<DataTypeInt32>(),
                                   column_name);
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

VExprSPtr create_expr_from_node(const TExprNode& node) {
    VExprSPtr expr;
    auto status = VExpr::create_expr(node, expr);
    DORIS_CHECK(status.ok()) << status.to_string();
    return expr;
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
    auto expr = table_function_expr("gt", std::make_shared<DataTypeUInt8>(), {int_type, int_type},
                                    TExprNodeType::BINARY_PRED, TExprOpcode::GT);
    expr->add_child(table_int32_slot_ref(slot_id, column_id, "id"));
    expr->add_child(table_int32_literal(value));
    return expr;
}

VExprSPtr runtime_filter_wrapper_expr(VExprSPtr impl) {
    TExprNode node;
    node.__set_node_type(TExprNodeType::SLOT_REF);
    node.__set_type(std::make_shared<DataTypeUInt8>()->to_thrift());
    node.__set_num_children(1);
    return RuntimeFilterExpr::create_shared(node, std::move(impl), 0, false, /*filter_id=*/1);
}

VExprSPtr table_nullable_int64_binary_predicate(const std::string& function_name,
                                                TExprOpcode::type opcode, int slot_id,
                                                int column_id, const std::string& column_name,
                                                int64_t value) {
    const auto int64_type = std::make_shared<DataTypeInt64>();
    const auto nullable_int64_type = make_nullable(int64_type);
    auto expr = table_function_expr(function_name, std::make_shared<DataTypeUInt8>(),
                                    {nullable_int64_type, int64_type}, TExprNodeType::BINARY_PRED,
                                    opcode);
    expr->add_child(
            VSlotRef::create_shared(slot_id, column_id, slot_id, nullable_int64_type, column_name));
    expr->add_child(table_int64_literal(value));
    return expr;
}

class NullableArrayBigintDefaultExpr final : public VExpr {
public:
    explicit NullableArrayBigintDefaultExpr(DataTypePtr data_type)
            : _name("single_element_groups") {
        _data_type = std::move(data_type);
    }

    const std::string& expr_name() const override { return _name; }

    bool is_constant() const override { return false; }

    Status execute_column_impl(VExprContext*, const Block*, const Selector* selector, size_t count,
                               ColumnPtr& result_column) const override {
        DCHECK(selector == nullptr || selector->size() == count);
        auto values = ColumnInt64::create();
        auto offsets = ColumnArray::ColumnOffsets::create();
        auto null_map = ColumnUInt8::create();
        for (size_t i = 0; i < count; ++i) {
            values->insert_value(7);
            offsets->insert_value(static_cast<Int64>(i + 1));
            null_map->insert_value(0);
        }
        auto array_column = ColumnArray::create(std::move(values), std::move(offsets));
        result_column = ColumnNullable::create(std::move(array_column), std::move(null_map));
        return Status::OK();
    }

private:
    std::string _name;
};

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
                .column_predicates = {},
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

class TableReaderMaterializeTestHelper final : public TableReader {
public:
    using TableReader::_materialize_map_mapping_column;
};

VExprSPtr table_int32_sum_expr(int left_slot_id, int left_column_id, int right_slot_id,
                               int right_column_id) {
    const auto int_type = std::make_shared<DataTypeInt32>();
    auto expr = table_function_expr("add", int_type, {int_type, int_type});
    expr->add_child(table_int32_slot_ref(left_slot_id, left_column_id, "id"));
    expr->add_child(table_int32_slot_ref(right_slot_id, right_column_id, "score"));
    return expr;
}

VExprSPtr table_int32_sum_greater_than_expr(int left_slot_id, int left_column_id, int right_slot_id,
                                            int right_column_id, int32_t value) {
    const auto int_type = std::make_shared<DataTypeInt32>();
    auto expr = table_function_expr("gt", std::make_shared<DataTypeUInt8>(), {int_type, int_type},
                                    TExprNodeType::BINARY_PRED, TExprOpcode::GT);
    expr->add_child(
            table_int32_sum_expr(left_slot_id, left_column_id, right_slot_id, right_column_id));
    expr->add_child(table_int32_literal(value));
    return expr;
}

VExprSPtr table_int32_sum_less_than_expr(int left_slot_id, int left_column_id, int right_slot_id,
                                         int right_column_id, int32_t value) {
    const auto int_type = std::make_shared<DataTypeInt32>();
    auto expr = table_function_expr("lt", std::make_shared<DataTypeUInt8>(), {int_type, int_type},
                                    TExprNodeType::BINARY_PRED, TExprOpcode::LT);
    expr->add_child(
            table_int32_sum_expr(left_slot_id, left_column_id, right_slot_id, right_column_id));
    expr->add_child(table_int32_literal(value));
    return expr;
}

VExprSPtr table_condition_function_expr(const std::string& function_name, bool short_circuit) {
    const auto int_type = std::make_shared<DataTypeInt32>();
    std::vector<DataTypePtr> arg_types;
    if (function_name == "if") {
        arg_types = {std::make_shared<DataTypeUInt8>(), int_type, int_type};
    } else {
        arg_types = {int_type, int_type};
    }
    auto expr = create_expr_from_node(
            table_function_node(function_name, int_type, arg_types, TExprNodeType::FUNCTION_CALL,
                                TExprOpcode::INVALID_OPCODE, short_circuit));
    if (function_name == "if") {
        expr->add_child(table_int32_greater_than_expr(0, 0, 0));
        expr->add_child(table_int32_literal(1));
        expr->add_child(table_int32_literal(0));
    } else {
        expr->add_child(table_int32_slot_ref(0, 0, "id"));
        expr->add_child(table_int32_literal(0));
    }
    return expr;
}

VExprSPtr table_case_expr(bool short_circuit) {
    const auto int_type = std::make_shared<DataTypeInt32>();
    TCaseExpr case_node;
    case_node.__set_has_case_expr(false);
    case_node.__set_has_else_expr(true);

    TExprNode node;
    node.__set_node_type(TExprNodeType::CASE_EXPR);
    node.__set_type(int_type->to_thrift());
    node.__set_is_nullable(false);
    node.__set_num_children(3);
    node.__set_case_expr(case_node);
    if (short_circuit) {
        node.__set_short_circuit_evaluation(true);
    }

    auto expr = create_expr_from_node(node);
    expr->add_child(table_int32_greater_than_expr(0, 0, 0));
    expr->add_child(table_int32_literal(1));
    expr->add_child(table_int32_literal(0));
    return expr;
}

TEST(CloneTableExprTreeTest, ClonesConditionalExpressions) {
    const std::vector<VExprSPtr> expressions {
            table_condition_function_expr("if", false),
            table_condition_function_expr("if", true),
            table_condition_function_expr("ifnull", false),
            table_condition_function_expr("ifnull", true),
            table_condition_function_expr("coalesce", false),
            table_condition_function_expr("coalesce", true),
            table_case_expr(false),
            table_case_expr(true),
    };

    for (const auto& expr : expressions) {
        VExprSPtr cloned;
        const auto status = clone_table_expr_tree(expr, &cloned);
        ASSERT_TRUE(status.ok()) << expr->debug_string() << ": " << status.to_string();
        ASSERT_NE(cloned, nullptr);
        const auto* original_expr = expr.get();
        const auto* cloned_expr = cloned.get();
        EXPECT_TRUE(typeid(*original_expr) == typeid(*cloned_expr))
                << expr->expr_name() << " cloned as " << typeid(*cloned_expr).name();
        EXPECT_EQ(expr->expr_name(), cloned->expr_name());
        EXPECT_EQ(expr->get_num_children(), cloned->get_num_children());
        EXPECT_NE(original_expr, cloned_expr);
    }
}

// Scenario: cloning a VectorizedFnCall whose return type is complex must not reconstruct the expr
// from TExprNode, because DataTypeFactory rejects nested types through the primitive-type path.
TEST(CloneTableExprTreeTest, ClonesVectorizedFnCallWithComplexReturnType) {
    const auto int_type = std::make_shared<DataTypeInt32>();
    const auto string_type = std::make_shared<DataTypeString>();
    const auto struct_type =
            std::make_shared<DataTypeStruct>(DataTypes {int_type, string_type}, Strings {"a", "b"});
    const auto array_type = std::make_shared<DataTypeArray>(struct_type);

    auto expr = table_function_expr("element_at", struct_type, {array_type, int_type});
    expr->add_child(VSlotRef::create_shared(0, 0, -1, array_type, "array_of_struct"));
    expr->add_child(table_int32_literal(1));

    VExprSPtr cloned;
    const auto status = clone_table_expr_tree(expr, &cloned);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_NE(cloned, nullptr);
    EXPECT_EQ(cloned->expr_name(), expr->expr_name());
    EXPECT_TRUE(cloned->data_type()->equals(*struct_type));
    EXPECT_EQ(cloned->get_num_children(), 2);
    EXPECT_NE(cloned.get(), expr.get());
}

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

std::shared_ptr<arrow::Array> build_string_array(const std::vector<std::string>& values) {
    arrow::StringBuilder builder;
    for (const auto& value : values) {
        EXPECT_TRUE(builder.Append(value).ok());
    }
    return finish_array(&builder);
}

void write_parquet_file(const std::string& file_path, int32_t id, const std::string& value) {
    auto schema = arrow::schema({
            arrow::field("id", arrow::int32(), false),
            arrow::field("value", arrow::utf8(), false),
    });
    auto table = arrow::Table::Make(schema, {build_int32_array({id}), build_string_array({value})});

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

void write_iceberg_equality_delete_parquet_file(const std::string& file_path, int32_t field_id,
                                                int32_t value) {
    const auto metadata =
            arrow::key_value_metadata({"PARQUET:field_id"}, {std::to_string(field_id)});
    auto schema = arrow::schema({
            arrow::field("id", arrow::int32(), false)->WithMetadata(metadata),
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

void write_struct_parquet_file(const std::string& file_path, int32_t id) {
    auto struct_type = arrow::struct_({arrow::field("id", arrow::int32(), false)});
    arrow::StructBuilder builder(
            struct_type, arrow::default_memory_pool(),
            {std::make_shared<arrow::Int32Builder>(arrow::default_memory_pool())});
    auto* id_builder = assert_cast<arrow::Int32Builder*>(builder.field_builder(0));
    EXPECT_TRUE(builder.Append().ok());
    EXPECT_TRUE(id_builder->Append(id).ok());

    auto schema = arrow::schema({
            arrow::field("s", struct_type, false),
    });
    auto table = arrow::Table::Make(schema, {finish_array(&builder)});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

    ::parquet::WriterProperties::Builder writer_builder;
    writer_builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    writer_builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    writer_builder.compression(::parquet::Compression::UNCOMPRESSED);
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out, 1,
                                                      writer_builder.build()));
}

void write_struct_parquet_file(const std::string& file_path, const std::vector<int32_t>& ids,
                               int64_t row_group_size = -1) {
    auto struct_type = arrow::struct_({arrow::field("id", arrow::int32(), false)});
    arrow::StructBuilder builder(
            struct_type, arrow::default_memory_pool(),
            {std::make_shared<arrow::Int32Builder>(arrow::default_memory_pool())});
    auto* id_builder = assert_cast<arrow::Int32Builder*>(builder.field_builder(0));
    for (const auto id : ids) {
        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(id_builder->Append(id).ok());
    }

    auto schema = arrow::schema({
            arrow::field("s", struct_type, false),
    });
    auto table = arrow::Table::Make(schema, {finish_array(&builder)});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

    ::parquet::WriterProperties::Builder writer_builder;
    writer_builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    writer_builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    writer_builder.compression(::parquet::Compression::UNCOMPRESSED);
    const auto write_row_group_size =
            row_group_size > 0 ? row_group_size : static_cast<int64_t>(ids.size());
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out,
                                                      write_row_group_size,
                                                      writer_builder.build()));
}

void write_struct_with_nullable_child_parquet_file(const std::string& file_path) {
    auto struct_type = arrow::struct_({
            arrow::field("id", arrow::int32(), false),
            arrow::field("note", arrow::utf8(), true),
    });
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> field_builders;
    auto id_builder = std::make_unique<arrow::Int32Builder>();
    field_builders.push_back(std::shared_ptr<arrow::ArrayBuilder>(std::move(id_builder)));
    auto note_builder = std::make_unique<arrow::StringBuilder>();
    field_builders.push_back(std::shared_ptr<arrow::ArrayBuilder>(std::move(note_builder)));
    arrow::StructBuilder builder(struct_type, arrow::default_memory_pool(),
                                 std::move(field_builders));
    auto* struct_id_builder = assert_cast<arrow::Int32Builder*>(builder.field_builder(0));
    auto* struct_note_builder = assert_cast<arrow::StringBuilder*>(builder.field_builder(1));

    EXPECT_TRUE(builder.Append().ok());
    EXPECT_TRUE(struct_id_builder->Append(7).ok());
    EXPECT_TRUE(struct_note_builder->Append("seven").ok());
    EXPECT_TRUE(builder.Append().ok());
    EXPECT_TRUE(struct_id_builder->Append(8).ok());
    EXPECT_TRUE(struct_note_builder->AppendNull().ok());

    auto schema = arrow::schema({
            arrow::field("s", struct_type, false),
    });
    auto table = arrow::Table::Make(schema, {finish_array(&builder)});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

    ::parquet::WriterProperties::Builder writer_builder;
    writer_builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    writer_builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    writer_builder.compression(::parquet::Compression::UNCOMPRESSED);
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out, 2,
                                                      writer_builder.build()));
}

void write_list_struct_parquet_file(const std::string& file_path) {
    auto struct_type = arrow::struct_(
            {arrow::field("a", arrow::int32(), false), arrow::field("b", arrow::int32(), false)});
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> field_builders;
    auto a_array_builder = std::make_unique<arrow::Int32Builder>();
    field_builders.push_back(std::shared_ptr<arrow::ArrayBuilder>(std::move(a_array_builder)));
    auto b_array_builder = std::make_unique<arrow::Int32Builder>();
    field_builders.push_back(std::shared_ptr<arrow::ArrayBuilder>(std::move(b_array_builder)));
    auto struct_builder = std::make_shared<arrow::StructBuilder>(
            struct_type, arrow::default_memory_pool(), std::move(field_builders));
    auto list_type = arrow::list(arrow::field("element", struct_type, true));
    arrow::ListBuilder builder(arrow::default_memory_pool(), struct_builder, list_type);
    auto* a_builder = assert_cast<arrow::Int32Builder*>(struct_builder->field_builder(0));
    auto* b_builder = assert_cast<arrow::Int32Builder*>(struct_builder->field_builder(1));

    EXPECT_TRUE(builder.Append().ok());
    EXPECT_TRUE(struct_builder->Append().ok());
    EXPECT_TRUE(a_builder->Append(10).ok());
    EXPECT_TRUE(b_builder->Append(11).ok());
    EXPECT_TRUE(struct_builder->Append().ok());
    EXPECT_TRUE(a_builder->Append(20).ok());
    EXPECT_TRUE(b_builder->Append(21).ok());

    EXPECT_TRUE(builder.Append().ok());
    EXPECT_TRUE(struct_builder->Append().ok());
    EXPECT_TRUE(a_builder->Append(30).ok());
    EXPECT_TRUE(b_builder->Append(31).ok());

    EXPECT_TRUE(builder.Append().ok());
    EXPECT_TRUE(struct_builder->Append().ok());
    EXPECT_TRUE(a_builder->Append(40).ok());
    EXPECT_TRUE(b_builder->Append(41).ok());

    auto schema = arrow::schema({
            arrow::field("xs", list_type, false),
    });
    auto table = arrow::Table::Make(schema, {finish_array(&builder)});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

    ::parquet::WriterProperties::Builder writer_builder;
    writer_builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    writer_builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    writer_builder.compression(::parquet::Compression::UNCOMPRESSED);
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out, 3,
                                                      writer_builder.build()));
}

void write_map_struct_parquet_file(const std::string& file_path) {
    auto key_builder = std::make_shared<arrow::Int32Builder>();
    auto struct_type = arrow::struct_(
            {arrow::field("a", arrow::int32(), false), arrow::field("b", arrow::utf8(), false)});
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> field_builders;
    auto a_array_builder = std::make_unique<arrow::Int32Builder>();
    field_builders.push_back(std::shared_ptr<arrow::ArrayBuilder>(std::move(a_array_builder)));
    auto b_array_builder = std::make_unique<arrow::StringBuilder>();
    field_builders.push_back(std::shared_ptr<arrow::ArrayBuilder>(std::move(b_array_builder)));
    auto value_builder = std::make_shared<arrow::StructBuilder>(
            struct_type, arrow::default_memory_pool(), std::move(field_builders));
    auto map_type = arrow::map(arrow::int32(), arrow::field("value", struct_type, false));
    arrow::MapBuilder builder(arrow::default_memory_pool(), key_builder, value_builder, map_type);
    auto* a_builder = assert_cast<arrow::Int32Builder*>(value_builder->field_builder(0));
    auto* b_builder = assert_cast<arrow::StringBuilder*>(value_builder->field_builder(1));

    EXPECT_TRUE(builder.Append().ok());
    EXPECT_TRUE(key_builder->Append(1).ok());
    EXPECT_TRUE(value_builder->Append().ok());
    EXPECT_TRUE(a_builder->Append(10).ok());
    EXPECT_TRUE(b_builder->Append("ma").ok());
    EXPECT_TRUE(key_builder->Append(2).ok());
    EXPECT_TRUE(value_builder->Append().ok());
    EXPECT_TRUE(a_builder->Append(20).ok());
    EXPECT_TRUE(b_builder->Append("mb").ok());

    EXPECT_TRUE(builder.Append().ok());
    EXPECT_TRUE(key_builder->Append(3).ok());
    EXPECT_TRUE(value_builder->Append().ok());
    EXPECT_TRUE(a_builder->Append(30).ok());
    EXPECT_TRUE(b_builder->Append("mc").ok());

    EXPECT_TRUE(builder.AppendEmptyValue().ok());

    auto schema = arrow::schema({
            arrow::field("kv", map_type, false),
    });
    auto table = arrow::Table::Make(schema, {finish_array(&builder)});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

    ::parquet::WriterProperties::Builder writer_builder;
    writer_builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    writer_builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    writer_builder.compression(::parquet::Compression::UNCOMPRESSED);
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out, 3,
                                                      writer_builder.build()));
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

int64_t write_paimon_deletion_vector_file(const std::string& file_path,
                                          const std::vector<uint32_t>& deleted_positions) {
    roaring::Roaring rows;
    for (const auto position : deleted_positions) {
        rows.add(position);
    }

    const size_t bitmap_size = rows.getSizeInBytes();
    const uint32_t total_length = static_cast<uint32_t>(4 + bitmap_size);
    std::vector<char> blob(4 + total_length);
    BigEndian::Store32(blob.data(), total_length);
    constexpr char PAIMON_BITMAP_MAGIC[] = {'\x5E', '\x43', '\xF2', '\xD0'};
    memcpy(blob.data() + 4, PAIMON_BITMAP_MAGIC, 4);
    rows.write(blob.data() + 8);

    std::ofstream output(file_path, std::ios::binary);
    EXPECT_TRUE(output.is_open());
    output.write(blob.data(), static_cast<std::streamsize>(blob.size()));
    EXPECT_TRUE(output.good());
    // Paimon DeletionFile.length is magic + bitmap length, excluding the leading length field.
    return static_cast<int64_t>(total_length);
}

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
    const auto& nullable_column = assert_cast<const ColumnNullable&>(column);
    for (const auto is_null : nullable_column.get_null_map_data()) {
        EXPECT_EQ(is_null, 0);
    }
    return nullable_column.get_nested_column();
}

DataTypePtr make_iceberg_rowid_type() {
    return make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {std::make_shared<DataTypeString>(), std::make_shared<DataTypeInt64>(),
                       std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeString>()},
            Strings {"file_path", "row_pos", "partition_spec_id", "partition_data_json"}));
}

void expect_iceberg_rowid_column_values(const IColumn& column, const std::string& file_path,
                                        const std::vector<int64_t>& row_positions,
                                        int32_t partition_spec_id,
                                        const std::string& partition_data_json) {
    const auto full_column = column.convert_to_full_column_if_const();
    const auto& nullable_column = assert_cast<const ColumnNullable&>(*full_column);
    const auto& struct_column =
            assert_cast<const ColumnStruct&>(nullable_column.get_nested_column());
    const auto& file_path_column = assert_cast<const ColumnString&>(struct_column.get_column(0));
    const auto& row_pos_column = assert_cast<const ColumnInt64&>(struct_column.get_column(1));
    const auto& spec_id_column = assert_cast<const ColumnInt32&>(struct_column.get_column(2));
    const auto& partition_data_column =
            assert_cast<const ColumnString&>(struct_column.get_column(3));

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
    const auto& values = assert_cast<const ColumnInt32&>(*full_column).get_data();
    ASSERT_EQ(values.size(), expected_values.size());
    for (size_t row = 0; row < expected_values.size(); ++row) {
        EXPECT_EQ(values[row], expected_values[row]);
    }
}

SplitReadOptions build_split_options(const std::string& file_path) {
    SplitReadOptions options;
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

TTableFormatFileDesc make_paimon_table_format_desc(const std::string& deletion_file_path,
                                                   int64_t offset, int64_t length) {
    TTableFormatFileDesc table_format_params;
    TPaimonFileDesc paimon_params;
    paimon_params.__set_file_format("parquet");
    TPaimonDeletionFileDesc deletion_file;
    deletion_file.__set_path(deletion_file_path);
    deletion_file.__set_offset(offset);
    deletion_file.__set_length(length);
    paimon_params.__set_deletion_file(deletion_file);
    table_format_params.__set_paimon_params(paimon_params);
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
        const auto& id_column = assert_cast<const ColumnInt32&>(*block.get_by_position(0).column);
        for (size_t row = 0; row < block.rows(); ++row) {
            ids.push_back(id_column.get_element(row));
        }
    }
    return ids;
}

int64_t parquet_column_start_offset(const ::parquet::ColumnChunkMetaData& column_metadata) {
    return column_metadata.has_dictionary_page()
                   ? static_cast<int64_t>(column_metadata.dictionary_page_offset())
                   : static_cast<int64_t>(column_metadata.data_page_offset());
}

SplitReadOptions build_split_options_for_row_group_mid(const std::string& file_path,
                                                       int row_group_idx) {
    auto options = build_split_options(file_path);
    auto reader = ::parquet::ParquetFileReader::OpenFile(file_path, false);
    auto metadata = reader->metadata();
    auto row_group_metadata = metadata->RowGroup(row_group_idx);
    auto first_column = row_group_metadata->ColumnChunk(0);
    auto last_column = row_group_metadata->ColumnChunk(row_group_metadata->num_columns() - 1);
    const int64_t row_group_start_offset = parquet_column_start_offset(*first_column);
    const int64_t row_group_end_offset =
            parquet_column_start_offset(*last_column) + last_column->total_compressed_size();
    const int64_t row_group_mid_offset =
            row_group_start_offset + (row_group_end_offset - row_group_start_offset) / 2;
    options.current_range.__set_start_offset(row_group_mid_offset);
    options.current_range.__set_size(1);
    return options;
}

ColumnDefinition make_table_column(int32_t id, const std::string& name, const DataTypePtr& type) {
    ColumnDefinition column;
    if (id >= 0) {
        column.identifier = Field::create_field<TYPE_INT>(id);
    }
    column.name = name;
    column.type = type;
    return column;
}

ColumnDefinition make_file_column(int32_t id, const std::string& name, const DataTypePtr& type) {
    ColumnDefinition field;
    field.identifier = Field::create_field<TYPE_INT>(id);
    field.local_id = id;
    field.name = name;
    field.type = type;
    return field;
}

class TableReaderCharVarcharTestHelper final : public TableReader {
public:
    using TableReader::_should_truncate_char_or_varchar_column;
    using TableReader::_truncate_char_or_varchar_column;
};

TEST(TableReaderTest, TruncateCharOrVarcharPredicateOnlyAppliesToParquetStringWidthMismatch) {
    ColumnMapping mapping;
    mapping.table_type = std::make_shared<DataTypeString>(3, TYPE_VARCHAR);
    mapping.file_type = std::make_shared<DataTypeString>(10, TYPE_VARCHAR);
    EXPECT_TRUE(TableReaderCharVarcharTestHelper::_should_truncate_char_or_varchar_column(mapping));

    mapping.file_type = std::make_shared<DataTypeString>(2, TYPE_VARCHAR);
    EXPECT_FALSE(
            TableReaderCharVarcharTestHelper::_should_truncate_char_or_varchar_column(mapping));

    mapping.file_type = std::make_shared<DataTypeString>();
    EXPECT_TRUE(TableReaderCharVarcharTestHelper::_should_truncate_char_or_varchar_column(mapping));

    mapping.file_type = std::make_shared<DataTypeInt32>();
    EXPECT_TRUE(TableReaderCharVarcharTestHelper::_should_truncate_char_or_varchar_column(mapping));

    mapping.table_type = std::make_shared<DataTypeString>();
    EXPECT_FALSE(
            TableReaderCharVarcharTestHelper::_should_truncate_char_or_varchar_column(mapping));
}

TEST(TableReaderTest, TruncateCharOrVarcharColumnKeepsNullMap) {
    auto nested = ColumnString::create();
    nested->insert_data("abcdef", 6);
    nested->insert_data("xyz", 3);
    auto null_map = ColumnUInt8::create();
    null_map->insert_value(0);
    null_map->insert_value(1);

    auto type = make_nullable(std::make_shared<DataTypeString>(3, TYPE_VARCHAR));
    Block block;
    block.insert({ColumnNullable::create(std::move(nested), std::move(null_map)), type, "v"});

    TableReaderCharVarcharTestHelper::_truncate_char_or_varchar_column(&block, 0, 3);

    ASSERT_EQ(block.columns(), 1);
    ASSERT_EQ(block.rows(), 2);
    const auto* nullable_column =
            assert_cast<const ColumnNullable*>(block.get_by_position(0).column.get());
    EXPECT_EQ(nullable_column->get_nested_column().get_data_at(0).to_string(), "abc");
    EXPECT_FALSE(nullable_column->is_null_at(0));
    EXPECT_TRUE(nullable_column->is_null_at(1));
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

void add_column_predicate(TableColumnPredicates* column_predicates, GlobalIndex global_index,
                          std::shared_ptr<ColumnPredicate> predicate) {
    auto& entry = (*column_predicates)[global_index];
    entry.push_back(std::move(predicate));
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

struct FakeFileReaderState {
    int init_count = 0;
    int open_count = 0;
    int close_count = 0;
    int64_t total_rows = 2;
    bool has_delete_operations = false;
    bool eof_with_first_batch = true;
    std::shared_ptr<FileScanRequest> last_request;
    std::shared_ptr<ConditionCacheContext> condition_cache_ctx;
};

class FakeFileReader final : public FileReader {
public:
    FakeFileReader(std::shared_ptr<io::FileSystemProperties>& system_properties,
                   std::unique_ptr<io::FileDescription>& file_description,
                   std::vector<ColumnDefinition> schema, std::shared_ptr<FakeFileReaderState> state)
            : FileReader(system_properties, file_description, nullptr, nullptr),
              _schema(std::move(schema)),
              _state(std::move(state)) {}

    Status init(RuntimeState* state) override {
        (void)state;
        ++_state->init_count;
        _eof = false;
        return Status::OK();
    }

    Status get_schema(std::vector<ColumnDefinition>* file_schema) const override {
        DORIS_CHECK(file_schema != nullptr);
        *file_schema = _schema;
        return Status::OK();
    }

    Status open(std::shared_ptr<FileScanRequest> request) override {
        RETURN_IF_ERROR(FileReader::open(std::move(request)));
        _state->last_request = _request;
        ++_state->open_count;
        _returned_batch = false;
        return Status::OK();
    }

    Status get_block(Block* file_block, size_t* rows, bool* eof) override {
        DORIS_CHECK(file_block != nullptr);
        DORIS_CHECK(rows != nullptr);
        DORIS_CHECK(eof != nullptr);
        DORIS_CHECK(_request != nullptr);
        if (_returned_batch) {
            *rows = 0;
            *eof = true;
            return Status::OK();
        }

        for (const auto& [file_column_id, block_position] : _request->local_positions) {
            if (file_column_id == LocalColumnId(0)) {
                auto column = ColumnInt32::create();
                column->insert_value(1);
                column->insert_value(2);
                file_block->replace_by_position(block_position.value(), std::move(column));
            } else if (file_column_id == LocalColumnId(1)) {
                auto column = ColumnString::create();
                column->insert_data("one", 3);
                column->insert_data("two", 3);
                file_block->replace_by_position(block_position.value(), std::move(column));
            } else if (file_column_id == LocalColumnId(2)) {
                auto country_values = ColumnString::create();
                country_values->insert_data("USA", 3);
                country_values->insert_data("UK", 2);
                auto country_null_map = ColumnUInt8::create();
                country_null_map->insert_value(0);
                country_null_map->insert_value(0);
                auto country_column = ColumnNullable::create(std::move(country_values),
                                                             std::move(country_null_map));

                auto city_column = ColumnString::create();
                city_column->insert_data("New York", 8);
                city_column->insert_data("London", 6);

                MutableColumns struct_children;
                struct_children.push_back(std::move(country_column));
                struct_children.push_back(std::move(city_column));
                auto struct_column = ColumnStruct::create(std::move(struct_children));

                auto root_null_map = ColumnUInt8::create();
                root_null_map->insert_value(0);
                root_null_map->insert_value(0);
                auto nullable_struct =
                        ColumnNullable::create(std::move(struct_column), std::move(root_null_map));
                file_block->replace_by_position(block_position.value(), std::move(nullable_struct));
            } else {
                return Status::InvalidArgument("Unexpected fake file column id {}",
                                               file_column_id.value());
            }
        }

        _returned_batch = true;
        *rows = 2;
        *eof = _state->eof_with_first_batch;
        if (_state->condition_cache_ctx != nullptr && !_state->condition_cache_ctx->is_hit &&
            _state->condition_cache_ctx->filter_result != nullptr &&
            !_state->condition_cache_ctx->filter_result->empty()) {
            // The real file reader marks a granule after local row-level predicates keep at least
            // one row from that granule. The fake reader does it here so TableReader tests can
            // focus on condition-cache lifecycle decisions without depending on Parquet internals.
            (*_state->condition_cache_ctx->filter_result)[0] = true;
        }
        return Status::OK();
    }

    void set_condition_cache_context(std::shared_ptr<ConditionCacheContext> ctx) override {
        _state->condition_cache_ctx = std::move(ctx);
    }

    int64_t get_total_rows() const override { return _state->total_rows; }

    bool has_delete_operations() const override { return _state->has_delete_operations; }

    Status close() override {
        ++_state->close_count;
        _request.reset();
        _eof = true;
        return Status::OK();
    }

private:
    std::vector<ColumnDefinition> _schema;
    std::shared_ptr<FakeFileReaderState> _state;
    bool _returned_batch = false;
};

class FakeTableReader final : public TableReader {
public:
    FakeTableReader(std::vector<ColumnDefinition> file_schema,
                    std::shared_ptr<FakeFileReaderState> state)
            : _file_schema(std::move(file_schema)), _state(std::move(state)) {}

protected:
    Status create_file_reader(std::unique_ptr<FileReader>* reader) override {
        DORIS_CHECK(reader != nullptr);
        auto system_properties = std::make_shared<io::FileSystemProperties>();
        system_properties->system_type = TFileType::FILE_LOCAL;
        auto file_description = std::make_unique<io::FileDescription>();
        file_description->path = "fake-table-reader-input";
        *reader = std::make_unique<FakeFileReader>(system_properties, file_description,
                                                   _file_schema, _state);
        return Status::OK();
    }

private:
    std::vector<ColumnDefinition> _file_schema;
    std::shared_ptr<FakeFileReaderState> _state;
};

class ScopedConditionCacheForTest {
public:
    ScopedConditionCacheForTest()
            : _previous(ExecEnv::GetInstance()->get_condition_cache()),
              _cache(segment_v2::ConditionCache::create_global_cache(1024 * 1024, 4)) {
        ExecEnv::GetInstance()->set_condition_cache(_cache.get());
    }

    ~ScopedConditionCacheForTest() {
        ExecEnv::GetInstance()->set_condition_cache(_previous);
    }

    segment_v2::ConditionCache* get() { return _cache.get(); }

private:
    segment_v2::ConditionCache* _previous = nullptr;
    std::unique_ptr<segment_v2::ConditionCache> _cache;
};

TEST(TableReaderTest, CanUseInjectedFileReaderForStandaloneUnitTest) {
    std::vector<ColumnDefinition> file_schema;
    file_schema.push_back(make_file_column(0, "id", std::make_shared<DataTypeInt32>()));
    file_schema.push_back(make_file_column(1, "value", std::make_shared<DataTypeString>()));

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(1, "value", std::make_shared<DataTypeString>()));
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));
    set_name_identifiers(&projected_columns);

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto fake_state = std::make_shared<FakeFileReaderState>();
    FakeTableReader reader(file_schema, fake_state);
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                            })
                        .ok());

    SplitReadOptions split_options;
    split_options.current_range.__set_path("fake-table-reader-input");
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    EXPECT_FALSE(eos);

    ASSERT_EQ(fake_state->init_count, 1);
    ASSERT_EQ(fake_state->open_count, 1);
    ASSERT_EQ(fake_state->close_count, 1);
    ASSERT_NE(fake_state->last_request, nullptr);
    ASSERT_EQ(fake_state->last_request->local_positions.at(LocalColumnId(1)).value(), 0);
    ASSERT_EQ(fake_state->last_request->local_positions.at(LocalColumnId(0)).value(), 1);
    EXPECT_EQ(projection_ids(fake_state->last_request->non_predicate_columns),
              std::vector<int32_t>({1, 0}));
    EXPECT_TRUE(fake_state->last_request->predicate_columns.empty());

    const auto& value_column = assert_cast<const ColumnString&>(*block.get_by_position(0).column);
    const auto& id_column = assert_cast<const ColumnInt32&>(*block.get_by_position(1).column);
    ASSERT_EQ(block.rows(), 2);
    EXPECT_EQ(value_column.get_data_at(0).to_string(), "one");
    EXPECT_EQ(value_column.get_data_at(1).to_string(), "two");
    EXPECT_EQ(id_column.get_element(0), 1);
    EXPECT_EQ(id_column.get_element(1), 2);

    block = build_table_block(projected_columns);
    eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    EXPECT_TRUE(eos);
}

TEST(TableReaderTest, ComplexRematerializeCastsScalarChildToTableType) {
    const auto string_type = std::make_shared<DataTypeString>();
    const auto nullable_string_type = make_nullable(string_type);
    const auto file_struct_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {string_type, string_type}, Strings {"country", "city"}));
    auto file_struct_column = make_file_column(2, "struct_column", file_struct_type);
    file_struct_column.children = {make_file_column(0, "country", string_type),
                                   make_file_column(1, "city", string_type)};
    std::vector<ColumnDefinition> file_schema = {file_struct_column};

    const auto table_struct_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {nullable_string_type, nullable_string_type}, Strings {"country", "city"}));
    auto country_child = make_table_column(0, "country", nullable_string_type);
    auto city_child = make_table_column(1, "city", nullable_string_type);
    auto table_struct_column = make_table_column(2, "struct_column", table_struct_type);
    table_struct_column.children = {country_child, city_child};
    std::vector<ColumnDefinition> projected_columns = {table_struct_column};
    set_name_identifiers(&projected_columns);

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto fake_state = std::make_shared<FakeFileReaderState>();
    FakeTableReader reader(file_schema, fake_state);
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                            })
                        .ok());

    SplitReadOptions split_options;
    split_options.current_range.__set_path("fake-table-reader-input");
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    const auto status = reader.get_block(&block, &eos);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_FALSE(eos);
    ASSERT_TRUE(block.check_type_and_column().ok()) << block.dump_structure();

    const auto& result_nullable =
            assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
    const auto& struct_result =
            assert_cast<const ColumnStruct&>(result_nullable.get_nested_column());
    ASSERT_EQ(struct_result.get_columns().size(), 2);
    const auto& country_column = assert_cast<const ColumnNullable&>(struct_result.get_column(0));
    const auto& city_column = assert_cast<const ColumnNullable&>(struct_result.get_column(1));
    const auto& country_values =
            assert_cast<const ColumnString&>(country_column.get_nested_column());
    const auto& city_values = assert_cast<const ColumnString&>(city_column.get_nested_column());
    ASSERT_EQ(city_column.size(), 2);
    EXPECT_FALSE(city_column.is_null_at(0));
    EXPECT_FALSE(city_column.is_null_at(1));
    EXPECT_EQ(country_values.get_data_at(0).to_string(), "USA");
    EXPECT_EQ(country_values.get_data_at(1).to_string(), "UK");
    EXPECT_EQ(city_values.get_data_at(0).to_string(), "New York");
    EXPECT_EQ(city_values.get_data_at(1).to_string(), "London");
}

TEST(TableReaderTest, ReopenSplitAfterClose) {
    const auto test_dir = std::filesystem::temp_directory_path() / "doris_table_reader_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const std::vector<std::string> file_paths = {
            (test_dir / "split_1.parquet").string(),
            (test_dir / "split_2.parquet").string(),
            (test_dir / "split_3.parquet").string(),
    };
    write_parquet_file(file_paths[0], 1, "one");
    write_parquet_file(file_paths[1], 2, "two");
    write_parquet_file(file_paths[2], 3, "three");

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(1, "value", std::make_shared<DataTypeString>()));
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    set_name_identifiers(&projected_columns);
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {prepared_conjunct(
                                            &state, table_int32_greater_than_expr(1, 1, 0))},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                            })
                        .ok());

    // Simulate the scanner lifecycle for three different splits:
    // init() once, then repeat prepare_split() -> get_block() -> close().
    // This verifies TableReader::close() fully releases the previous low-level reader and task
    // state, so a later prepare_split() can open and read a new split on the same TableReader.
    // The table-level conjunct is also rebuilt for each split. The projection order puts value
    // before id, so the pushed conjunct has to be rewritten to the ParquetReader file-local block
    // position every time a new split is opened.
    std::vector<int32_t> ids;
    std::vector<std::string> values;
    for (const auto& file_path : file_paths) {
        auto split_options = build_split_options(file_path);
        ASSERT_TRUE(reader.prepare_split(split_options).ok());

        Block block = build_table_block(projected_columns);
        bool eos = false;
        ASSERT_TRUE(reader.get_block(&block, &eos).ok());
        ASSERT_FALSE(eos);

        const auto& value_column =
                assert_cast<const ColumnString&>(*block.get_by_position(0).column);
        const auto& id_column = assert_cast<const ColumnInt32&>(*block.get_by_position(1).column);
        ASSERT_EQ(id_column.size(), 1);
        ASSERT_EQ(value_column.size(), 1);
        ids.push_back(id_column.get_element(0));
        values.push_back(value_column.get_data_at(0).to_string());

        ASSERT_TRUE(reader.close().ok());
    }

    EXPECT_EQ(ids, std::vector<int32_t>({1, 2, 3}));
    EXPECT_EQ(values, std::vector<std::string>({"one", "two", "three"}));

    std::filesystem::remove_all(test_dir);
}

// Scenario: column predicates are pruning hints only. They do not produce a row-level survivor
// bitmap, so TableReader must not enable condition cache when the scan request has no conjuncts.
TEST(TableReaderTest, ConditionCacheSkipsColumnPredicateOnlyRequest) {
    std::vector<ColumnDefinition> file_schema;
    file_schema.push_back(make_file_column(0, "id", std::make_shared<DataTypeInt32>()));

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));
    set_name_identifiers(&projected_columns);

    TableColumnPredicates column_predicates;
    add_column_predicate(&column_predicates, GlobalIndex(0),
                         create_comparison_predicate<PredicateType::GT>(
                                 0, "id", std::make_shared<DataTypeInt32>(),
                                 Field::create_field<TYPE_INT>(0), false));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto fake_state = std::make_shared<FakeFileReaderState>();
    FakeTableReader reader(file_schema, fake_state);
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = std::move(column_predicates),
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                                    .condition_cache_digest = 7,
                            })
                        .ok());

    SplitReadOptions split_options;
    split_options.current_range.__set_path("fake-table-reader-input");
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    EXPECT_EQ(fake_state->condition_cache_ctx, nullptr);
    EXPECT_EQ(reader.condition_cache_hit_count(), 0);
    ASSERT_TRUE(reader.close().ok());
}

// Scenario: runtime filters can arrive late and are not represented by the stable predicate digest.
// A MISS must not insert a bitmap for `stable predicate AND runtime filter` under the stable digest.
TEST(TableReaderTest, ConditionCacheSkipsRuntimeFilterConjunct) {
    std::vector<ColumnDefinition> file_schema;
    file_schema.push_back(make_file_column(0, "id", std::make_shared<DataTypeInt32>()));

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));
    set_name_identifiers(&projected_columns);

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto fake_state = std::make_shared<FakeFileReaderState>();
    FakeTableReader reader(file_schema, fake_state);
    ASSERT_TRUE(
            reader.init({
                                .projected_columns = projected_columns,
                                .column_predicates = {},
                                .conjuncts = {prepared_conjunct(
                                        &state, runtime_filter_wrapper_expr(
                                                        table_int32_greater_than_expr(0, 0, 0)))},
                                .format = FileFormat::PARQUET,
                                .scan_params = nullptr,
                                .io_ctx = nullptr,
                                .runtime_state = &state,
                                .scanner_profile = nullptr,
                                .condition_cache_digest = 7,
                        })
                    .ok());

    SplitReadOptions split_options;
    split_options.current_range.__set_path("fake-table-reader-input");
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    EXPECT_EQ(fake_state->condition_cache_ctx, nullptr);
    EXPECT_EQ(reader.condition_cache_hit_count(), 0);
    ASSERT_TRUE(reader.close().ok());
}

// Scenario: table-format delete files/deletion vectors are outside the data-file cache key. When a
// reader reports delete operations, TableReader must keep condition cache disabled for that split.
TEST(TableReaderTest, ConditionCacheSkipsReaderWithDeleteOperations) {
    std::vector<ColumnDefinition> file_schema;
    file_schema.push_back(make_file_column(0, "id", std::make_shared<DataTypeInt32>()));

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));
    set_name_identifiers(&projected_columns);

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto fake_state = std::make_shared<FakeFileReaderState>();
    fake_state->has_delete_operations = true;
    FakeTableReader reader(file_schema, fake_state);
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {prepared_conjunct(
                                            &state, table_int32_greater_than_expr(0, 0, 0))},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                                    .condition_cache_digest = 7,
                            })
                        .ok());

    SplitReadOptions split_options;
    split_options.current_range.__set_path("fake-table-reader-input");
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    EXPECT_EQ(fake_state->condition_cache_ctx, nullptr);
    EXPECT_EQ(reader.condition_cache_hit_count(), 0);
    ASSERT_TRUE(reader.close().ok());
}

// Scenario: a MISS bitmap is safe to publish only after the physical reader reaches EOF. This test
// returns EOF together with the first batch and verifies TableReader publishes the marked bitmap.
TEST(TableReaderTest, ConditionCacheMissPublishesBitmapAfterReaderEof) {
    ScopedConditionCacheForTest cache;

    std::vector<ColumnDefinition> file_schema;
    file_schema.push_back(make_file_column(0, "id", std::make_shared<DataTypeInt32>()));

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));
    set_name_identifiers(&projected_columns);

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto fake_state = std::make_shared<FakeFileReaderState>();
    fake_state->total_rows = ConditionCacheContext::GRANULE_SIZE;
    FakeTableReader reader(file_schema, fake_state);
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {prepared_conjunct(
                                            &state, table_int32_greater_than_expr(0, 0, 0))},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                                    .condition_cache_digest = 7,
                            })
                        .ok());

    SplitReadOptions split_options;
    split_options.current_range.__set_path("fake-table-reader-input");
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_NE(fake_state->condition_cache_ctx, nullptr);
    EXPECT_FALSE(fake_state->condition_cache_ctx->is_hit);

    segment_v2::ConditionCache::ExternalCacheKey key("fake-table-reader-input", 0, -1, 7, 0, -1);
    segment_v2::ConditionCacheHandle handle;
    ASSERT_TRUE(cache.get()->lookup(key, &handle));
    const auto cached_bitmap = handle.get_filter_result();
    ASSERT_NE(cached_bitmap, nullptr);
    ASSERT_FALSE(cached_bitmap->empty());
    EXPECT_TRUE((*cached_bitmap)[0]);

    ASSERT_TRUE(reader.close().ok());
}

// Scenario: LIMIT/cancel can close a reader before it reaches EOF. TableReader must drop the MISS
// bitmap because unvisited granules would still be false and unsafe for future cache hits.
TEST(TableReaderTest, ConditionCacheMissIsDroppedWhenReaderClosesBeforeEof) {
    ScopedConditionCacheForTest cache;

    std::vector<ColumnDefinition> file_schema;
    file_schema.push_back(make_file_column(0, "id", std::make_shared<DataTypeInt32>()));

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));
    set_name_identifiers(&projected_columns);

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto fake_state = std::make_shared<FakeFileReaderState>();
    fake_state->total_rows = ConditionCacheContext::GRANULE_SIZE;
    fake_state->eof_with_first_batch = false;
    FakeTableReader reader(file_schema, fake_state);
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {prepared_conjunct(
                                            &state, table_int32_greater_than_expr(0, 0, 0))},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                                    .condition_cache_digest = 7,
                            })
                        .ok());

    SplitReadOptions split_options;
    split_options.current_range.__set_path("fake-table-reader-input");
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_NE(fake_state->condition_cache_ctx, nullptr);
    EXPECT_FALSE(fake_state->condition_cache_ctx->is_hit);

    ASSERT_TRUE(reader.close().ok());
    segment_v2::ConditionCache::ExternalCacheKey key("fake-table-reader-input", 0, -1, 7, 0, -1);
    segment_v2::ConditionCacheHandle handle;
    EXPECT_FALSE(cache.get()->lookup(key, &handle));
}

TEST(TableReaderTest, PushDownCountFromNewParquetReader) {
    const auto test_dir = std::filesystem::temp_directory_path() / "doris_table_reader_count_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3, 4, 5}, {10, 20, 30, 40, 50},
                                {"one", "two", "three", "four", "five"}, 2);

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    set_name_identifiers(&projected_columns);
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                                    .push_down_agg_type = TPushAggOp::type::COUNT,
                            })
                        .ok());
    ASSERT_TRUE(reader.prepare_split(build_split_options(file_path)).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);
    ASSERT_EQ(block.rows(), 5);
    EXPECT_FALSE(is_column_const(*block.get_by_position(0).column));

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, TableLevelCountUsesAssignedRowCount) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_table_reader_table_count_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3}, {10, 20, 30}, {"one", "two", "three"});

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    TQueryOptions query_options;
    query_options.__set_batch_size(2);
    RuntimeState state {query_options, TQueryGlobals()};
    set_name_identifiers(&projected_columns);
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
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
    set_table_level_row_count(&split_options, 5);
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);
    EXPECT_EQ(block.rows(), 2);

    block = build_table_block(projected_columns);
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);
    EXPECT_EQ(block.rows(), 2);

    block = build_table_block(projected_columns);
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);
    EXPECT_EQ(block.rows(), 1);

    block = build_table_block(projected_columns);
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    EXPECT_TRUE(eos);
    EXPECT_EQ(block.rows(), 0);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, PushDownMinMaxFromNewParquetReader) {
    const auto test_dir = std::filesystem::temp_directory_path() / "doris_table_reader_minmax_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_int_pair_parquet_file(file_path, {3, 1, 5, 2}, {30, 10, 50, 20},
                                {"three", "one", "five", "two"}, 2);

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));
    projected_columns.push_back(make_table_column(1, "score", std::make_shared<DataTypeInt32>()));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    set_name_identifiers(&projected_columns);
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                                    .push_down_agg_type = TPushAggOp::type::MINMAX,
                            })
                        .ok());
    ASSERT_TRUE(reader.prepare_split(build_split_options(file_path)).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);
    ASSERT_EQ(block.rows(), 2);
    const auto& id_column = assert_cast<const ColumnInt32&>(*block.get_by_position(0).column);
    const auto& score_column = assert_cast<const ColumnInt32&>(*block.get_by_position(1).column);
    EXPECT_EQ(id_column.get_element(0), 1);
    EXPECT_EQ(id_column.get_element(1), 5);
    EXPECT_EQ(score_column.get_element(0), 10);
    EXPECT_EQ(score_column.get_element(1), 50);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, PushDownMinMaxCastsFileValueToTableType) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_table_reader_minmax_cast_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_int_pair_parquet_file(file_path, {3, 1, 5, 2}, {30, 10, 50, 20},
                                {"three", "one", "five", "two"}, 2);

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt64>()));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    set_name_identifiers(&projected_columns);
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                                    .push_down_agg_type = TPushAggOp::type::MINMAX,
                            })
                        .ok());
    ASSERT_TRUE(reader.prepare_split(build_split_options(file_path)).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    auto status = reader.get_block(&block, &eos);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_FALSE(eos);
    ASSERT_EQ(block.rows(), 2);
    const auto& id_column = assert_cast<const ColumnInt64&>(*block.get_by_position(0).column);
    EXPECT_EQ(id_column.get_element(0), 1);
    EXPECT_EQ(id_column.get_element(1), 5);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, PushDownMinMaxFromProjectedStructLeaf) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_table_reader_minmax_struct_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_struct_parquet_file(file_path, {3, 1, 5, 2}, 2);

    const auto int_type = std::make_shared<DataTypeInt32>();
    auto id_child = make_table_column(0, "id", int_type);
    auto struct_type = std::make_shared<DataTypeStruct>(DataTypes {int_type}, Strings {"id"});
    auto struct_column = make_table_column(100, "s", struct_type);
    struct_column.children = {id_child};
    std::vector<ColumnDefinition> projected_columns = {struct_column};

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    set_name_identifiers(&projected_columns);
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                                    .push_down_agg_type = TPushAggOp::type::MINMAX,
                            })
                        .ok());
    ASSERT_TRUE(reader.prepare_split(build_split_options(file_path)).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    auto status = reader.get_block(&block, &eos);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_FALSE(eos);
    ASSERT_EQ(block.rows(), 2);
    const auto& struct_result = assert_cast<const ColumnStruct&>(*block.get_by_position(0).column);
    ASSERT_EQ(struct_result.get_columns().size(), 1);
    const auto& ids = assert_cast<const ColumnInt32&>(
            expect_not_null_nullable_nested_column(struct_result.get_column(0)));
    EXPECT_EQ(ids.get_element(0), 1);
    EXPECT_EQ(ids.get_element(1), 5);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, PushDownMinMaxFallsBackForProjectedListStructLeaf) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_table_reader_minmax_list_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_list_struct_parquet_file(file_path);

    const auto int_type = std::make_shared<DataTypeInt32>();
    const auto nullable_int_type = make_nullable(int_type);
    auto element_type = std::make_shared<DataTypeStruct>(
            DataTypes {nullable_int_type, nullable_int_type}, Strings {"a", "b"});
    auto nullable_element_type = make_nullable(element_type);
    auto list_column =
            make_table_column(100, "xs", std::make_shared<DataTypeArray>(nullable_element_type));
    std::vector<ColumnDefinition> projected_columns = {list_column};

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    set_name_identifiers(&projected_columns);
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                                    .push_down_agg_type = TPushAggOp::type::MINMAX,
                            })
                        .ok());
    ASSERT_TRUE(reader.prepare_split(build_split_options(file_path)).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    auto status = reader.get_block(&block, &eos);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_FALSE(eos);
    ASSERT_EQ(block.rows(), 3);
    const auto& array_result = assert_cast<const ColumnArray&>(*block.get_by_position(0).column);
    EXPECT_EQ(array_result.get_offsets()[0], 2);
    EXPECT_EQ(array_result.get_offsets()[1], 3);
    EXPECT_EQ(array_result.get_offsets()[2], 4);
    const auto& nullable_elements = assert_cast<const ColumnNullable&>(array_result.get_data());
    for (const auto is_null : nullable_elements.get_null_map_data()) {
        EXPECT_EQ(is_null, 0);
    }
    const auto& element_struct =
            assert_cast<const ColumnStruct&>(nullable_elements.get_nested_column());
    ASSERT_EQ(element_struct.get_columns().size(), 2);
    const auto& a_values = assert_cast<const ColumnInt32&>(
            expect_not_null_nullable_nested_column(element_struct.get_column(0)));
    EXPECT_EQ(a_values.get_element(0), 10);
    EXPECT_EQ(a_values.get_element(1), 20);
    EXPECT_EQ(a_values.get_element(2), 30);
    EXPECT_EQ(a_values.get_element(3), 40);
    const auto& b_values = assert_cast<const ColumnInt32&>(
            expect_not_null_nullable_nested_column(element_struct.get_column(1)));
    EXPECT_EQ(b_values.get_element(0), 11);
    EXPECT_EQ(b_values.get_element(1), 21);
    EXPECT_EQ(b_values.get_element(2), 31);
    EXPECT_EQ(b_values.get_element(3), 41);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, ProjectedListStructReadsSelectedElementChild) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_table_reader_list_projection_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_list_struct_parquet_file(file_path);

    const auto int_type = std::make_shared<DataTypeInt32>();
    auto a_child = make_table_column(0, "a", int_type);
    auto element_type = std::make_shared<DataTypeStruct>(DataTypes {int_type}, Strings {"a"});
    auto nullable_element_type = make_nullable(element_type);
    auto element_child = make_table_column(0, "element", nullable_element_type);
    element_child.children = {a_child};
    auto list_column =
            make_table_column(100, "xs", std::make_shared<DataTypeArray>(nullable_element_type));
    list_column.children = {element_child};
    std::vector<ColumnDefinition> projected_columns = {list_column};

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    set_name_identifiers(&projected_columns);
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
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
    const auto& array_result = assert_cast<const ColumnArray&>(*block.get_by_position(0).column);
    EXPECT_EQ(array_result.get_offsets()[0], 2);
    EXPECT_EQ(array_result.get_offsets()[1], 3);
    EXPECT_EQ(array_result.get_offsets()[2], 4);
    const auto& nullable_elements = assert_cast<const ColumnNullable&>(array_result.get_data());
    const auto& element_struct =
            assert_cast<const ColumnStruct&>(nullable_elements.get_nested_column());
    ASSERT_EQ(element_struct.get_columns().size(), 1);
    const auto& a_values = assert_cast<const ColumnInt32&>(element_struct.get_column(0));
    EXPECT_EQ(a_values.get_element(0), 10);
    EXPECT_EQ(a_values.get_element(1), 20);
    EXPECT_EQ(a_values.get_element(2), 30);
    EXPECT_EQ(a_values.get_element(3), 40);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, ProjectedListStructReordersRenamedAndMissingElementChildren) {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_table_reader_list_schema_evolution_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_list_struct_parquet_file(file_path);

    const auto int_type = std::make_shared<DataTypeInt32>();
    const auto nullable_int_type = make_nullable(int_type);
    const auto string_type = std::make_shared<DataTypeString>();
    auto b_child = make_table_column(1, "renamed_b", nullable_int_type);
    b_child.name_mapping = {"b"};
    auto missing_child = make_table_column(99, "missing_child", string_type);
    auto a_child = make_table_column(0, "renamed_a", nullable_int_type);
    a_child.name_mapping = {"a"};
    auto element_type = std::make_shared<DataTypeStruct>(
            DataTypes {nullable_int_type, string_type, nullable_int_type},
            Strings {"renamed_b", "missing_child", "renamed_a"});
    auto nullable_element_type = make_nullable(element_type);
    auto element_child = make_table_column(0, "element", nullable_element_type);
    element_child.children = {b_child, missing_child, a_child};
    auto list_column =
            make_table_column(100, "xs", std::make_shared<DataTypeArray>(nullable_element_type));
    list_column.children = {element_child};
    std::vector<ColumnDefinition> projected_columns = {list_column};

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    set_name_identifiers(&projected_columns);
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
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
    const auto& array_result = assert_cast<const ColumnArray&>(*block.get_by_position(0).column);
    EXPECT_EQ(array_result.get_offsets()[0], 2);
    EXPECT_EQ(array_result.get_offsets()[1], 3);
    EXPECT_EQ(array_result.get_offsets()[2], 4);
    const auto& nullable_elements = assert_cast<const ColumnNullable&>(array_result.get_data());
    const auto& element_struct =
            assert_cast<const ColumnStruct&>(nullable_elements.get_nested_column());
    ASSERT_EQ(element_struct.get_columns().size(), 3);
    const auto& b_values = assert_cast<const ColumnInt32&>(
            expect_not_null_nullable_nested_column(element_struct.get_column(0)));
    const auto& missing_values = assert_cast<const ColumnString&>(element_struct.get_column(1));
    const auto& a_values = assert_cast<const ColumnInt32&>(
            expect_not_null_nullable_nested_column(element_struct.get_column(2)));
    EXPECT_EQ(b_values.get_element(0), 11);
    EXPECT_EQ(b_values.get_element(1), 21);
    EXPECT_EQ(b_values.get_element(2), 31);
    EXPECT_EQ(b_values.get_element(3), 41);
    for (size_t row = 0; row < missing_values.size(); ++row) {
        EXPECT_EQ(missing_values.get_data_at(row).to_string(), "");
    }
    EXPECT_EQ(a_values.get_element(0), 10);
    EXPECT_EQ(a_values.get_element(1), 20);
    EXPECT_EQ(a_values.get_element(2), 30);
    EXPECT_EQ(a_values.get_element(3), 40);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

// Scenario: when every projected array-element struct child is missing/default-only, the reader
// still receives a full element projection and can materialize the default child without crashing.
TEST(TableReaderTest, ProjectedListStructOnlyMissingElementChildFallsBackToFullElement) {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_table_reader_list_only_missing_child_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_list_struct_parquet_file(file_path);

    const auto string_type = std::make_shared<DataTypeString>();
    auto missing_child = make_table_column(99, "missing_child", string_type);
    auto element_type =
            std::make_shared<DataTypeStruct>(DataTypes {string_type}, Strings {"missing_child"});
    auto nullable_element_type = make_nullable(element_type);
    auto element_child = make_table_column(0, "element", nullable_element_type);
    element_child.children = {missing_child};
    auto list_column =
            make_table_column(100, "xs", std::make_shared<DataTypeArray>(nullable_element_type));
    list_column.children = {element_child};
    std::vector<ColumnDefinition> projected_columns = {list_column};

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    set_name_identifiers(&projected_columns);
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
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
    const auto& array_result = assert_cast<const ColumnArray&>(*block.get_by_position(0).column);
    EXPECT_EQ(array_result.get_offsets()[0], 2);
    EXPECT_EQ(array_result.get_offsets()[1], 3);
    EXPECT_EQ(array_result.get_offsets()[2], 4);
    const auto& nullable_elements = assert_cast<const ColumnNullable&>(array_result.get_data());
    const auto& element_struct =
            assert_cast<const ColumnStruct&>(nullable_elements.get_nested_column());
    ASSERT_EQ(element_struct.get_columns().size(), 1);
    const auto& missing_values = assert_cast<const ColumnString&>(element_struct.get_column(0));
    for (size_t row = 0; row < missing_values.size(); ++row) {
        EXPECT_EQ(missing_values.get_data_at(row).to_string(), "");
    }

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, PushDownMinMaxFallsBackForProjectedMapValueStructLeaf) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_table_reader_minmax_map_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_map_struct_parquet_file(file_path);

    const auto key_type = std::make_shared<DataTypeInt32>();
    const auto string_type = std::make_shared<DataTypeString>();
    const auto nullable_string_type = make_nullable(string_type);
    auto b_child = make_table_column(1, "b", nullable_string_type);
    auto value_type =
            std::make_shared<DataTypeStruct>(DataTypes {nullable_string_type}, Strings {"b"});
    auto nullable_value_type = make_nullable(value_type);
    auto value_child = make_table_column(1, "value", nullable_value_type);
    value_child.children = {b_child};
    auto map_column = make_table_column(
            100, "kv", std::make_shared<DataTypeMap>(key_type, nullable_value_type));
    map_column.children = {value_child};
    std::vector<ColumnDefinition> projected_columns = {map_column};

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    set_name_identifiers(&projected_columns);
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                                    .push_down_agg_type = TPushAggOp::type::MINMAX,
                            })
                        .ok());
    ASSERT_TRUE(reader.prepare_split(build_split_options(file_path)).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);
    ASSERT_EQ(block.rows(), 3);
    const auto& map_result = assert_cast<const ColumnMap&>(*block.get_by_position(0).column);
    EXPECT_EQ(map_result.get_offsets()[0], 2);
    EXPECT_EQ(map_result.get_offsets()[1], 3);
    EXPECT_EQ(map_result.get_offsets()[2], 3);
    const auto& keys = assert_cast<const ColumnInt32&>(
            expect_not_null_nullable_nested_column(map_result.get_keys()));
    EXPECT_EQ(keys.get_element(0), 1);
    EXPECT_EQ(keys.get_element(1), 2);
    EXPECT_EQ(keys.get_element(2), 3);
    const auto& nullable_values = assert_cast<const ColumnNullable&>(map_result.get_values());
    for (const auto is_null : nullable_values.get_null_map_data()) {
        EXPECT_EQ(is_null, 0);
    }
    const auto& value_struct =
            assert_cast<const ColumnStruct&>(nullable_values.get_nested_column());
    ASSERT_EQ(value_struct.get_columns().size(), 1);
    const auto& b_values = assert_cast<const ColumnString&>(
            expect_not_null_nullable_nested_column(value_struct.get_column(0)));
    EXPECT_EQ(b_values.get_data_at(0).to_string(), "ma");
    EXPECT_EQ(b_values.get_data_at(1).to_string(), "mb");
    EXPECT_EQ(b_values.get_data_at(2).to_string(), "mc");

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, ProjectedMapValueStructReordersRenamedAndMissingChildren) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_table_reader_map_schema_evolution_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_map_struct_parquet_file(file_path);

    const auto key_type = std::make_shared<DataTypeInt32>();
    const auto int_type = std::make_shared<DataTypeInt32>();
    const auto nullable_int_type = make_nullable(int_type);
    const auto string_type = std::make_shared<DataTypeString>();
    const auto nullable_string_type = make_nullable(string_type);
    auto b_child = make_table_column(1, "renamed_b", nullable_string_type);
    b_child.name_mapping = {"b"};
    auto missing_child = make_table_column(99, "missing_child", string_type);
    auto a_child = make_table_column(0, "renamed_a", nullable_int_type);
    a_child.name_mapping = {"a"};
    auto value_type = std::make_shared<DataTypeStruct>(
            DataTypes {nullable_string_type, string_type, nullable_int_type},
            Strings {"renamed_b", "missing_child", "renamed_a"});
    auto nullable_value_type = make_nullable(value_type);
    auto value_child = make_table_column(1, "value", nullable_value_type);
    value_child.children = {b_child, missing_child, a_child};
    auto map_column = make_table_column(
            100, "kv", std::make_shared<DataTypeMap>(key_type, nullable_value_type));
    map_column.children = {value_child};
    std::vector<ColumnDefinition> projected_columns = {map_column};

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    set_name_identifiers(&projected_columns);
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
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
    const auto& map_result = assert_cast<const ColumnMap&>(*block.get_by_position(0).column);
    EXPECT_EQ(map_result.get_offsets()[0], 2);
    EXPECT_EQ(map_result.get_offsets()[1], 3);
    EXPECT_EQ(map_result.get_offsets()[2], 3);
    const auto& keys = assert_cast<const ColumnInt32&>(
            expect_not_null_nullable_nested_column(map_result.get_keys()));
    EXPECT_EQ(keys.get_element(0), 1);
    EXPECT_EQ(keys.get_element(1), 2);
    EXPECT_EQ(keys.get_element(2), 3);
    const auto& nullable_values = assert_cast<const ColumnNullable&>(map_result.get_values());
    const auto& value_struct =
            assert_cast<const ColumnStruct&>(nullable_values.get_nested_column());
    ASSERT_EQ(value_struct.get_columns().size(), 3);
    const auto& b_values = assert_cast<const ColumnString&>(
            expect_not_null_nullable_nested_column(value_struct.get_column(0)));
    const auto& missing_values = assert_cast<const ColumnString&>(value_struct.get_column(1));
    const auto& a_values = assert_cast<const ColumnInt32&>(
            expect_not_null_nullable_nested_column(value_struct.get_column(2)));
    EXPECT_EQ(b_values.get_data_at(0).to_string(), "ma");
    EXPECT_EQ(b_values.get_data_at(1).to_string(), "mb");
    EXPECT_EQ(b_values.get_data_at(2).to_string(), "mc");
    for (size_t row = 0; row < missing_values.size(); ++row) {
        EXPECT_EQ(missing_values.get_data_at(row).to_string(), "");
    }
    EXPECT_EQ(a_values.get_element(0), 10);
    EXPECT_EQ(a_values.get_element(1), 20);
    EXPECT_EQ(a_values.get_element(2), 30);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, MaterializeMapKeyStructReordersRenamedChildren) {
    const auto int_type = std::make_shared<DataTypeInt32>();
    const auto string_type = std::make_shared<DataTypeString>();
    const auto file_key_type =
            std::make_shared<DataTypeStruct>(DataTypes {int_type, string_type}, Strings {"a", "b"});
    const auto table_key_type = std::make_shared<DataTypeStruct>(
            DataTypes {string_type, int_type}, Strings {"renamed_b", "renamed_a"});
    const auto file_map_type = std::make_shared<DataTypeMap>(file_key_type, int_type);
    const auto table_map_type = std::make_shared<DataTypeMap>(table_key_type, int_type);

    ColumnMapping a_mapping;
    a_mapping.table_column_name = "renamed_a";
    a_mapping.file_column_name = "a";
    a_mapping.file_local_id = 0;
    a_mapping.table_type = int_type;
    a_mapping.file_type = int_type;
    a_mapping.is_trivial = true;

    ColumnMapping b_mapping;
    b_mapping.table_column_name = "renamed_b";
    b_mapping.file_column_name = "b";
    b_mapping.file_local_id = 1;
    b_mapping.table_type = string_type;
    b_mapping.file_type = string_type;
    b_mapping.is_trivial = true;

    ColumnMapping key_mapping;
    key_mapping.table_column_name = "key";
    key_mapping.file_column_name = "key";
    key_mapping.file_local_id = 0;
    key_mapping.table_type = table_key_type;
    key_mapping.file_type = file_key_type;
    key_mapping.is_trivial = false;
    key_mapping.child_mappings = {b_mapping, a_mapping};

    ColumnMapping value_mapping;
    value_mapping.table_column_name = "value";
    value_mapping.file_column_name = "value";
    value_mapping.file_local_id = 1;
    value_mapping.table_type = int_type;
    value_mapping.file_type = int_type;
    value_mapping.is_trivial = true;

    ColumnMapping map_mapping;
    map_mapping.table_column_name = "kv";
    map_mapping.file_column_name = "kv";
    map_mapping.table_type = table_map_type;
    map_mapping.file_type = file_map_type;
    map_mapping.is_trivial = false;
    map_mapping.child_mappings = {key_mapping, value_mapping};

    auto a_keys = ColumnInt32::create();
    a_keys->insert_value(10);
    a_keys->insert_value(20);
    a_keys->insert_value(30);
    auto b_keys = ColumnString::create();
    b_keys->insert_value("x");
    b_keys->insert_value("y");
    b_keys->insert_value("z");
    MutableColumns key_children;
    key_children.push_back(std::move(a_keys));
    key_children.push_back(std::move(b_keys));
    auto key_column = ColumnStruct::create(std::move(key_children));

    auto value_column = ColumnInt32::create();
    value_column->insert_value(100);
    value_column->insert_value(200);
    value_column->insert_value(300);
    auto offsets_column = ColumnArray::ColumnOffsets::create();
    offsets_column->insert_value(2);
    offsets_column->insert_value(3);
    ColumnPtr file_column = ColumnMap::create(std::move(key_column), std::move(value_column),
                                              std::move(offsets_column));

    TableReaderMaterializeTestHelper reader;
    ColumnPtr result_column;
    ASSERT_TRUE(reader._materialize_map_mapping_column(map_mapping, file_column, 2, &result_column)
                        .ok());

    const auto& result_map = assert_cast<const ColumnMap&>(*result_column);
    EXPECT_EQ(result_map.get_offsets()[0], 2);
    EXPECT_EQ(result_map.get_offsets()[1], 3);
    const auto& result_key = assert_cast<const ColumnStruct&>(result_map.get_keys());
    ASSERT_EQ(result_key.get_columns().size(), 2);
    const auto& b_result = assert_cast<const ColumnString&>(result_key.get_column(0));
    const auto& a_result = assert_cast<const ColumnInt32&>(result_key.get_column(1));
    EXPECT_EQ(b_result.get_data_at(0).to_string(), "x");
    EXPECT_EQ(b_result.get_data_at(1).to_string(), "y");
    EXPECT_EQ(b_result.get_data_at(2).to_string(), "z");
    EXPECT_EQ(a_result.get_element(0), 10);
    EXPECT_EQ(a_result.get_element(1), 20);
    EXPECT_EQ(a_result.get_element(2), 30);

    const auto& result_value = assert_cast<const ColumnInt32&>(result_map.get_values());
    EXPECT_EQ(result_value.get_element(0), 100);
    EXPECT_EQ(result_value.get_element(1), 200);
    EXPECT_EQ(result_value.get_element(2), 300);
}

// Scenario: map value struct materialization follows DataTypeStruct field order even when
// ColumnMapping children arrive in a different order from projected ColumnDefinition children.
TEST(TableReaderTest, MaterializeMapValueStructUsesTableTypeOrder) {
    const auto key_type = std::make_shared<DataTypeString>();
    const auto string_type = std::make_shared<DataTypeString>();
    const auto file_value_type = std::make_shared<DataTypeStruct>(
            DataTypes {string_type, string_type}, Strings {"full_name", "gender"});
    const auto table_value_type = std::make_shared<DataTypeStruct>(
            DataTypes {string_type, string_type}, Strings {"full_name", "gender"});
    const auto file_map_type = std::make_shared<DataTypeMap>(key_type, file_value_type);
    const auto table_map_type = std::make_shared<DataTypeMap>(key_type, table_value_type);

    ColumnMapping full_name_mapping;
    full_name_mapping.table_column_name = "full_name";
    full_name_mapping.file_column_name = "full_name";
    full_name_mapping.file_local_id = 0;
    full_name_mapping.table_type = string_type;
    full_name_mapping.file_type = string_type;
    full_name_mapping.is_trivial = true;

    ColumnMapping gender_mapping;
    gender_mapping.table_column_name = "gender";
    gender_mapping.file_column_name = "gender";
    gender_mapping.file_local_id = 1;
    gender_mapping.table_type = string_type;
    gender_mapping.file_type = string_type;
    gender_mapping.is_trivial = true;

    ColumnMapping value_mapping;
    value_mapping.table_column_name = "value";
    value_mapping.file_column_name = "value";
    value_mapping.file_local_id = 1;
    value_mapping.table_type = table_value_type;
    value_mapping.file_type = file_value_type;
    value_mapping.is_trivial = false;
    value_mapping.child_mappings = {gender_mapping, full_name_mapping};

    ColumnMapping key_mapping;
    key_mapping.table_column_name = "key";
    key_mapping.file_column_name = "key";
    key_mapping.file_local_id = 0;
    key_mapping.table_type = key_type;
    key_mapping.file_type = key_type;
    key_mapping.is_trivial = true;

    ColumnMapping map_mapping;
    map_mapping.table_column_name = "new_map_column";
    map_mapping.file_column_name = "new_map_column";
    map_mapping.table_type = table_map_type;
    map_mapping.file_type = file_map_type;
    map_mapping.is_trivial = false;
    map_mapping.child_mappings = {key_mapping, value_mapping};

    auto key_column = ColumnString::create();
    key_column->insert_value("person10");
    key_column->insert_value("person20");

    auto full_name_column = ColumnString::create();
    full_name_column->insert_value("Jack");
    full_name_column->insert_value("James Lee");
    auto gender_column = ColumnString::create();
    gender_column->insert_value("Male");
    gender_column->insert_value("Male");
    MutableColumns value_children;
    value_children.push_back(std::move(full_name_column));
    value_children.push_back(std::move(gender_column));
    auto value_column = ColumnStruct::create(std::move(value_children));

    auto offsets_column = ColumnArray::ColumnOffsets::create();
    offsets_column->insert_value(1);
    offsets_column->insert_value(2);
    ColumnPtr file_column = ColumnMap::create(std::move(key_column), std::move(value_column),
                                              std::move(offsets_column));

    TableReaderMaterializeTestHelper reader;
    ColumnPtr result_column;
    ASSERT_TRUE(reader._materialize_map_mapping_column(map_mapping, file_column, 2, &result_column)
                        .ok());

    const auto& result_map = assert_cast<const ColumnMap&>(*result_column);
    const auto& result_value = assert_cast<const ColumnStruct&>(result_map.get_values());
    ASSERT_EQ(result_value.get_columns().size(), 2);
    const auto& full_name_result = assert_cast<const ColumnString&>(result_value.get_column(0));
    const auto& gender_result = assert_cast<const ColumnString&>(result_value.get_column(1));
    EXPECT_EQ(full_name_result.get_data_at(0).to_string(), "Jack");
    EXPECT_EQ(full_name_result.get_data_at(1).to_string(), "James Lee");
    EXPECT_EQ(gender_result.get_data_at(0).to_string(), "Male");
    EXPECT_EQ(gender_result.get_data_at(1).to_string(), "Male");
}

TEST(TableReaderTest, PushDownMinMaxOnlyUsesSelectedRowGroupInFileRange) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_table_reader_minmax_range_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_int_pair_parquet_file(file_path, {10, 1, 100}, {100, 10, 1000}, {"ten", "one", "hundred"},
                                1);

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    set_name_identifiers(&projected_columns);
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                                    .push_down_agg_type = TPushAggOp::type::MINMAX,
                            })
                        .ok());
    ASSERT_TRUE(reader.prepare_split(build_split_options_for_row_group_mid(file_path, 1)).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);
    ASSERT_EQ(block.rows(), 2);
    const auto& id_column = assert_cast<const ColumnInt32&>(*block.get_by_position(0).column);
    EXPECT_EQ(id_column.get_element(0), 1);
    EXPECT_EQ(id_column.get_element(1), 1);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, PushDownCountOnlyUsesSelectedRowGroupInFileRange) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_table_reader_count_range_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3}, {10, 20, 30}, {"one", "two", "three"}, 1);

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    set_name_identifiers(&projected_columns);
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                                    .push_down_agg_type = TPushAggOp::type::COUNT,
                            })
                        .ok());
    ASSERT_TRUE(reader.prepare_split(build_split_options_for_row_group_mid(file_path, 2)).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);
    ASSERT_EQ(block.rows(), 1);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, PushDownCountFallsBackWithTableConjunct) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_table_reader_count_conjunct_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3}, {10, 20, 30}, {"one", "two", "three"});

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    set_name_identifiers(&projected_columns);
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {prepared_conjunct(
                                            &state, table_int32_greater_than_expr(0, 0, 2))},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                                    .push_down_agg_type = TPushAggOp::type::COUNT,
                            })
                        .ok());
    ASSERT_TRUE(reader.prepare_split(build_split_options(file_path)).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);
    ASSERT_EQ(block.rows(), 1);
    const auto& id_column = assert_cast<const ColumnInt32&>(*block.get_by_position(0).column);
    EXPECT_EQ(id_column.get_element(0), 3);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, PushDownCountFallsBackWithColumnPredicate) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_table_reader_count_predicate_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3}, {10, 20, 30}, {"one", "two", "three"}, 1);

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    TableColumnPredicates column_predicates;
    add_column_predicate(&column_predicates, GlobalIndex(0),
                         create_comparison_predicate<PredicateType::GT>(
                                 0, "id", std::make_shared<DataTypeInt32>(),
                                 Field::create_field<TYPE_INT>(2), false));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    set_name_identifiers(&projected_columns);
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = std::move(column_predicates),
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                                    .push_down_agg_type = TPushAggOp::type::COUNT,
                            })
                        .ok());
    ASSERT_TRUE(reader.prepare_split(build_split_options(file_path)).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);
    ASSERT_EQ(block.rows(), 1);
    const auto& id_column = assert_cast<const ColumnInt32&>(*block.get_by_position(0).column);
    EXPECT_EQ(id_column.get_element(0), 3);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, PushDownMinMaxFallsBackWithoutDirectFileMapping) {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_table_reader_minmax_missing_mapping_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_parquet_file(file_path, 1, "one");

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(
            make_table_column(99, "missing_id", std::make_shared<DataTypeInt32>()));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    set_name_identifiers(&projected_columns);
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                                    .push_down_agg_type = TPushAggOp::type::MINMAX,
                            })
                        .ok());
    ASSERT_TRUE(reader.prepare_split(build_split_options(file_path)).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);
    ASSERT_EQ(block.rows(), 1);
    EXPECT_EQ(block.get_by_position(0).column->get_int(0), 0);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, OpenReaderBuildsTableFiltersFromConjuncts) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_table_reader_conjunct_filter_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_parquet_file(file_path, 3, "three");

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(1, "value", std::make_shared<DataTypeString>()));
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    set_name_identifiers(&projected_columns);
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {prepared_conjunct(
                                            &state, table_int32_greater_than_expr(1, 1, 2))},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                            })
                        .ok());

    ASSERT_TRUE(reader.prepare_split(build_split_options(file_path)).ok());

    // open_reader() should convert the table-level conjunct on projected column id 1 into
    // _table_filters before ColumnMapper creates the FileScanRequest. ColumnMapper then rewrites
    // the conjunct's slot ref from table column id 1 to the file-local block position used by
    // ParquetReader. The projection order intentionally puts value before id, so the id filter
    // column is not at position 0 in the file block.
    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);
    const auto& id_column = assert_cast<const ColumnInt32&>(*block.get_by_position(1).column);
    ASSERT_EQ(id_column.size(), 1);
    EXPECT_EQ(id_column.get_element(0), 3);

    ASSERT_TRUE(reader.close().ok());

    TableReader filtered_reader;
    ASSERT_TRUE(filtered_reader
                        .init({
                                .projected_columns = projected_columns,
                                .column_predicates = {},
                                .conjuncts = {prepared_conjunct(
                                        &state, table_int32_greater_than_expr(1, 1, 4))},
                                .format = FileFormat::PARQUET,
                                .scan_params = nullptr,
                                .io_ctx = nullptr,
                                .runtime_state = &state,
                                .scanner_profile = nullptr,
                        })
                        .ok());
    ASSERT_TRUE(filtered_reader.prepare_split(build_split_options(file_path)).ok());

    block = build_table_block(projected_columns);
    eos = false;
    ASSERT_TRUE(filtered_reader.get_block(&block, &eos).ok());
    EXPECT_TRUE(eos);
    EXPECT_EQ(block.get_by_position(1).column->size(), 0);

    ASSERT_TRUE(filtered_reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, OpenReaderBuildsColumnPredicateFilters) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_table_reader_column_predicate_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    // ColumnPredicate is only used for row-group/statistics pruning. Keep one row per row
    // group so the predicate can prune the first two row groups and leave only id = 3.
    write_int_pair_parquet_file(file_path, {1, 2, 3}, {1, 5, 8}, {"one", "two", "three"}, 1);

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(2, "value", std::make_shared<DataTypeString>()));
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    TableColumnPredicates column_predicates;
    add_column_predicate(&column_predicates, GlobalIndex(1),
                         create_comparison_predicate<PredicateType::GT>(
                                 0, "id", std::make_shared<DataTypeInt32>(),
                                 Field::create_field<TYPE_INT>(2), false));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    set_name_identifiers(&projected_columns);
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = std::move(column_predicates),
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

    const auto& value_column = assert_cast<const ColumnString&>(*block.get_by_position(0).column);
    const auto& id_column = assert_cast<const ColumnInt32&>(*block.get_by_position(1).column);
    ASSERT_EQ(id_column.size(), 1);
    ASSERT_EQ(value_column.size(), 1);
    EXPECT_EQ(id_column.get_element(0), 3);
    EXPECT_EQ(value_column.get_data_at(0).to_string(), "three");

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, ColumnPredicateSurvivesReopenSplit) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_table_reader_predicate_reopen_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const std::vector<std::string> file_paths = {
            (test_dir / "split_1.parquet").string(),
            (test_dir / "split_2.parquet").string(),
    };
    write_int_pair_parquet_file(file_paths[0], {1, 3}, {10, 30}, {"one", "three"}, 1);
    write_int_pair_parquet_file(file_paths[1], {2, 4}, {20, 40}, {"two", "four"}, 1);

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    TableColumnPredicates column_predicates;
    add_column_predicate(&column_predicates, GlobalIndex(0),
                         create_comparison_predicate<PredicateType::GT>(
                                 0, "id", std::make_shared<DataTypeInt32>(),
                                 Field::create_field<TYPE_INT>(2), false));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    set_name_identifiers(&projected_columns);
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = std::move(column_predicates),
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                            })
                        .ok());

    std::vector<int32_t> ids;
    for (const auto& file_path : file_paths) {
        ASSERT_TRUE(reader.prepare_split(build_split_options(file_path)).ok());

        Block block = build_table_block(projected_columns);
        bool eos = false;
        ASSERT_TRUE(reader.get_block(&block, &eos).ok());
        ASSERT_FALSE(eos);
        const auto& id_column = assert_cast<const ColumnInt32&>(*block.get_by_position(0).column);
        ASSERT_EQ(id_column.size(), 1);
        ids.push_back(id_column.get_element(0));

        ASSERT_TRUE(reader.close().ok());
    }

    EXPECT_EQ(ids, std::vector<int32_t>({3, 4}));
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, CreateScanRequestDeduplicatesSharedPredicateColumns) {
    const auto int_type = std::make_shared<DataTypeInt32>();
    const std::vector<ColumnDefinition> projected_columns = {
            make_table_column(0, "a", int_type),
            make_table_column(1, "b", int_type),
            make_table_column(2, "c", int_type),
            make_table_column(3, "value", std::make_shared<DataTypeString>()),
    };
    const std::vector<ColumnDefinition> file_schema = {
            make_file_column(0, "a", int_type),
            make_file_column(1, "b", int_type),
            make_file_column(2, "c", int_type),
            make_file_column(3, "value", std::make_shared<DataTypeString>()),
    };

    TableColumnMapper mapper;
    ASSERT_TRUE(mapper.create_mapping(projected_columns, {}, file_schema).ok());

    std::vector<TableFilter> table_filters;
    table_filters.push_back({
            .conjunct =
                    VExprContext::create_shared(table_int32_sum_greater_than_expr(0, 0, 1, 1, 1)),
            .global_indices = {GlobalIndex(0), GlobalIndex(1)},
    });
    table_filters.push_back({
            .conjunct = VExprContext::create_shared(table_int32_sum_less_than_expr(0, 0, 2, 2, 3)),
            .global_indices = {GlobalIndex(0), GlobalIndex(2)},
    });

    FileScanRequest file_request;
    ASSERT_TRUE(
            mapper.create_scan_request(table_filters, {}, projected_columns, &file_request).ok());

    // Both filters reference column a. It must still be read once as a predicate column, and a
    // predicate column must not be repeated as a non-predicate column.
    EXPECT_EQ(projection_ids(file_request.predicate_columns), std::vector<int32_t>({0, 1, 2}));
    EXPECT_EQ(projection_ids(file_request.non_predicate_columns), std::vector<int32_t>({3}));
    ASSERT_EQ(file_request.local_positions.size(), 4);
    EXPECT_EQ(file_request.local_positions.at(LocalColumnId(3)).value(), 0);
    EXPECT_EQ(file_request.local_positions.at(LocalColumnId(0)).value(), 1);
    EXPECT_EQ(file_request.local_positions.at(LocalColumnId(1)).value(), 2);
    EXPECT_EQ(file_request.local_positions.at(LocalColumnId(2)).value(), 3);
    const auto predicate_column_ids = projection_ids(file_request.predicate_columns);
    const auto non_predicate_column_ids = projection_ids(file_request.non_predicate_columns);
    for (const auto predicate_column_id : predicate_column_ids) {
        EXPECT_TRUE(std::find(non_predicate_column_ids.begin(), non_predicate_column_ids.end(),
                              predicate_column_id) == non_predicate_column_ids.end());
    }
}

TEST(TableReaderTest, CreateScanRequestPromotesProjectedColumnToPredicateColumn) {
    const auto int_type = std::make_shared<DataTypeInt32>();
    const std::vector<ColumnDefinition> projected_columns = {
            make_table_column(0, "id", int_type),
            make_table_column(1, "score", int_type),
    };
    const std::vector<ColumnDefinition> file_schema = {
            make_file_column(0, "id", int_type),
            make_file_column(1, "score", int_type),
    };

    TableColumnMapper mapper;
    ASSERT_TRUE(mapper.create_mapping(projected_columns, {}, file_schema).ok());

    TableFilter table_filter {
            .conjunct = VExprContext::create_shared(table_int32_greater_than_expr(0, 0, 1)),
            .global_indices = {GlobalIndex(0)},
    };

    FileScanRequest file_request;
    ASSERT_TRUE(
            mapper.create_scan_request({table_filter}, {}, projected_columns, &file_request).ok());

    EXPECT_EQ(projection_ids(file_request.predicate_columns), std::vector<int32_t>({0}));
    EXPECT_EQ(projection_ids(file_request.non_predicate_columns), std::vector<int32_t>({1}));
    ASSERT_EQ(file_request.local_positions.size(), 2);
    EXPECT_EQ(file_request.local_positions.at(LocalColumnId(0)).value(), 1);
    EXPECT_EQ(file_request.local_positions.at(LocalColumnId(1)).value(), 0);
}

TEST(TableReaderTest, CreateScanRequestUsesColumnNameForByNamePredicateMapping) {
    const auto int_type = std::make_shared<DataTypeInt32>();
    std::vector<ColumnDefinition> projected_columns = {
            make_table_column(10, "id", int_type),
            make_table_column(11, "score", int_type),
    };
    const std::vector<ColumnDefinition> file_schema = {
            make_file_column(0, "ID", int_type),
            make_file_column(1, "score", int_type),
    };

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    set_name_identifiers(&projected_columns);
    ASSERT_TRUE(mapper.create_mapping(projected_columns, {}, file_schema).ok());

    TableFilter table_filter {
            .conjunct = VExprContext::create_shared(table_int32_greater_than_expr(0, 0, 1)),
            .global_indices = {GlobalIndex(0)},
    };

    FileScanRequest file_request;
    ASSERT_TRUE(
            mapper.create_scan_request({table_filter}, {}, projected_columns, &file_request).ok());

    EXPECT_EQ(projection_ids(file_request.predicate_columns), std::vector<int32_t>({0}));
    EXPECT_EQ(projection_ids(file_request.non_predicate_columns), std::vector<int32_t>({1}));
    ASSERT_EQ(file_request.conjuncts.size(), 1);
    const auto* localized_slot =
            assert_cast<const VSlotRef*>(file_request.conjuncts[0]->root()->children()[0].get());
    EXPECT_EQ(localized_slot->slot_id(), 0);
    EXPECT_EQ(localized_slot->column_id(), 1);
}

TEST(TableReaderTest, ColumnPredicateFilterUsesColumnNameForByNameMapping) {
    const auto int_type = std::make_shared<DataTypeInt32>();
    std::vector<ColumnDefinition> projected_columns = {
            make_table_column(10, "id", int_type),
            make_table_column(11, "score", int_type),
    };
    const std::vector<ColumnDefinition> file_schema = {
            make_file_column(0, "ID", int_type),
            make_file_column(1, "score", int_type),
    };

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    set_name_identifiers(&projected_columns);
    ASSERT_TRUE(mapper.create_mapping(projected_columns, {}, file_schema).ok());

    TableColumnPredicates column_predicates;
    add_column_predicate(&column_predicates, GlobalIndex(0),
                         create_comparison_predicate<PredicateType::GT>(
                                 10, "id", int_type, Field::create_field<TYPE_INT>(2), false));

    FileScanRequest file_request;
    ASSERT_TRUE(mapper.create_scan_request({}, column_predicates, projected_columns, &file_request)
                        .ok());

    ASSERT_EQ(file_request.column_predicate_filters.size(), 1);
    EXPECT_EQ(file_request.column_predicate_filters[0].file_column_id.value(), 0);
    EXPECT_EQ(projection_ids(file_request.non_predicate_columns), std::vector<int32_t>({0, 1}));
    EXPECT_TRUE(file_request.predicate_columns.empty());
}

TEST(TableReaderTest, OpenReaderPushesMultiColumnConjunctToParquetReader) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_table_reader_multi_conjunct_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3}, {1, 5, 8}, {"one", "two", "three"});

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(2, "value", std::make_shared<DataTypeString>()));
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));
    projected_columns.push_back(make_table_column(1, "score", std::make_shared<DataTypeInt32>()));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    set_name_identifiers(&projected_columns);
    TableReader reader;
    ASSERT_TRUE(
            reader.init({
                                .projected_columns = projected_columns,
                                .column_predicates = {},
                                .conjuncts = {prepared_conjunct(
                                        &state, table_int32_sum_greater_than_expr(1, 1, 2, 2, 8))},
                                .format = FileFormat::PARQUET,
                                .scan_params = nullptr,
                                .io_ctx = nullptr,
                                .runtime_state = &state,
                                .scanner_profile = nullptr,
                        })
                    .ok());

    ASSERT_TRUE(reader.prepare_split(build_split_options(file_path)).ok());

    // The conjunct references both id and score, so ColumnMapper must put both file columns into
    // predicate_columns and rewrite both slot refs to ParquetReader's file-local block positions.
    // ParquetReader then evaluates the expression after all predicate columns have been read.
    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);

    const auto& value_column = assert_cast<const ColumnString&>(*block.get_by_position(0).column);
    const auto& id_column = assert_cast<const ColumnInt32&>(*block.get_by_position(1).column);
    const auto& score_column = assert_cast<const ColumnInt32&>(*block.get_by_position(2).column);
    ASSERT_EQ(id_column.size(), 1);
    ASSERT_EQ(score_column.size(), 1);
    ASSERT_EQ(value_column.size(), 1);
    EXPECT_EQ(id_column.get_element(0), 3);
    EXPECT_EQ(score_column.get_element(0), 8);
    EXPECT_EQ(value_column.get_data_at(0).to_string(), "three");

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, ProjectedColumnsFillDefaultForParquetSchemaMismatch) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_table_reader_schema_mismatch_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_parquet_file(file_path, 1, "one");

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(
            make_table_column(99, "missing_value", std::make_shared<DataTypeString>()));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    set_name_identifiers(&projected_columns);
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                            })
                        .ok());

    ASSERT_TRUE(reader.prepare_split(build_split_options(file_path)).ok());

    // The table projection asks for field id 99, but the ParquetReader exposes only file-local
    // fields 0 and 1. Missing columns are allowed by the current mapper options, so TableReader
    // should still use the Parquet row count and fill a default column in table schema.
    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);
    EXPECT_EQ(block.get_by_position(0).column->size(), 1);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, DefaultExprResultMatchesNullableTableType) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_table_reader_nullable_default_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_parquet_file(file_path, 1, "one");

    const auto int_type = std::make_shared<DataTypeInt32>();
    auto missing_column = make_table_column(99, "c_new", make_nullable(int_type));
    missing_column.default_expr = VExprContext::create_shared(
            VLiteral::create_shared(int_type, Field::create_field<TYPE_INT>(42)));
    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(std::move(missing_column));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    set_name_identifiers(&projected_columns);
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
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
    auto status = reader.get_block(&block, &eos);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_FALSE(eos);

    const auto& result = block.get_by_position(0);
    ASSERT_TRUE(result.check_type_and_column_match().ok());
    EXPECT_TRUE(result.type->is_nullable());
    ASSERT_TRUE(result.column->is_nullable());
    const auto& nullable_column = assert_cast<const ColumnNullable&>(*result.column);
    ASSERT_EQ(nullable_column.size(), 1);
    EXPECT_EQ(nullable_column.get_null_map_data()[0], 0);
    const auto& values = assert_cast<const ColumnInt32&>(nullable_column.get_nested_column());
    EXPECT_EQ(values.get_element(0), 42);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, DefaultExprAlignsNestedNullableArrayTableType) {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_table_reader_nested_nullable_array_default_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_parquet_file(file_path, 1, "one");

    const auto bigint_type = std::make_shared<DataTypeInt64>();
    const auto array_type = std::make_shared<DataTypeArray>(make_nullable(bigint_type));
    const auto table_type = make_nullable(array_type);
    auto missing_column = make_table_column(99, "single_element_groups", table_type);
    missing_column.default_expr = VExprContext::create_shared(
            std::make_shared<NullableArrayBigintDefaultExpr>(table_type));
    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(std::move(missing_column));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    set_name_identifiers(&projected_columns);
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
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
    auto status = reader.get_block(&block, &eos);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_FALSE(eos);

    const auto& result = block.get_by_position(0);
    ASSERT_TRUE(result.check_type_and_column_match().ok());
    ASSERT_TRUE(result.column->is_nullable());
    const auto& nullable_column = assert_cast<const ColumnNullable&>(*result.column);
    ASSERT_EQ(nullable_column.size(), 1);
    EXPECT_EQ(nullable_column.get_null_map_data()[0], 0);

    const auto& array_column = assert_cast<const ColumnArray&>(nullable_column.get_nested_column());
    ASSERT_EQ(array_column.size(), 1);
    EXPECT_EQ(array_column.get_offsets()[0], 1);
    ASSERT_TRUE(array_column.get_data().is_nullable());
    const auto& nested_nullable = assert_cast<const ColumnNullable&>(array_column.get_data());
    ASSERT_EQ(nested_nullable.size(), 1);
    EXPECT_EQ(nested_nullable.get_null_map_data()[0], 0);
    const auto& values = assert_cast<const ColumnInt64&>(nested_nullable.get_nested_column());
    EXPECT_EQ(values.get_element(0), 7);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, ProjectedColumnsFillMissingParquetColumnWithDefault) {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_table_reader_schema_mismatch_reject_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_parquet_file(file_path, 1, "one");

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(
            make_table_column(99, "missing_value", std::make_shared<DataTypeString>()));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    set_name_identifiers(&projected_columns);
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
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
    const auto status = reader.get_block(&block, &eos);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_FALSE(eos);

    const auto& result = block.get_by_position(0);
    ASSERT_TRUE(result.check_type_and_column_match().ok());
    const auto& missing_values = assert_cast<const ColumnString&>(*result.column);
    ASSERT_EQ(missing_values.size(), 1);
    EXPECT_EQ(missing_values.get_data_at(0).to_string(), "");

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, ProjectedStructFillsMissingChildWithDefault) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_table_reader_struct_missing_child_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_struct_parquet_file(file_path, 7);

    const auto int_type = std::make_shared<DataTypeInt32>();
    const auto string_type = std::make_shared<DataTypeString>();
    auto id_child = make_table_column(0, "id", int_type);
    auto missing_child = make_table_column(99, "missing_child", string_type);
    auto struct_type = std::make_shared<DataTypeStruct>(DataTypes {int_type, string_type},
                                                        Strings {"id", "missing_child"});
    auto struct_column = make_table_column(100, "s", struct_type);
    struct_column.children = {id_child, missing_child};
    std::vector<ColumnDefinition> projected_columns = {struct_column};

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    set_name_identifiers(&projected_columns);
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
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

    const auto& struct_result = assert_cast<const ColumnStruct&>(*block.get_by_position(0).column);
    ASSERT_EQ(struct_result.get_columns().size(), 2);
    const auto& ids = assert_cast<const ColumnInt32&>(struct_result.get_column(0));
    const auto& missing_values = assert_cast<const ColumnString&>(struct_result.get_column(1));
    ASSERT_EQ(struct_result.size(), 1);
    EXPECT_EQ(ids.get_element(0), 7);
    EXPECT_EQ(missing_values.get_data_at(0).to_string(), "");

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, ReusedBlockClearsProjectedStructWithNullableChild) {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_table_reader_struct_nullable_child_reuse_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_struct_with_nullable_child_parquet_file(file_path);

    const auto int_type = std::make_shared<DataTypeInt32>();
    const auto string_type = std::make_shared<DataTypeString>();
    const auto nullable_string_type = make_nullable(string_type);
    auto id_child = make_table_column(0, "id", int_type);
    auto note_child = make_table_column(1, "note", nullable_string_type);
    auto missing_child = make_table_column(99, "missing_child", string_type);
    auto struct_type = std::make_shared<DataTypeStruct>(
            DataTypes {int_type, nullable_string_type, string_type},
            Strings {"id", "note", "missing_child"});
    auto struct_column = make_table_column(100, "s", struct_type);
    struct_column.children = {id_child, note_child, missing_child};
    std::vector<ColumnDefinition> projected_columns = {struct_column};

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    set_name_identifiers(&projected_columns);
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
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
    ASSERT_EQ(block.rows(), 2);
    const auto& struct_result = assert_cast<const ColumnStruct&>(*block.get_by_position(0).column);
    const auto& notes = assert_cast<const ColumnNullable&>(struct_result.get_column(1));
    EXPECT_FALSE(notes.is_null_at(0));
    EXPECT_TRUE(notes.is_null_at(1));

    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    EXPECT_TRUE(eos);
    EXPECT_EQ(block.rows(), 0);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, ProjectedPartitionColumnUsesSplitPartitionValue) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_table_reader_partition_value_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_parquet_file(file_path, 1, "one");

    std::vector<ColumnDefinition> projected_columns;
    auto partition_column = make_table_column(1, "value", std::make_shared<DataTypeString>());
    partition_column.is_partition_key = true;
    projected_columns.push_back(std::move(partition_column));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    set_name_identifiers(&projected_columns);
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                            })
                        .ok());

    auto split_options = build_split_options(file_path);
    split_options.partition_values.emplace("value", Field::create_field<TYPE_STRING>("p1"));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    // The file has a physical column with the same id/name. The split partition value should still
    // take precedence and be materialized by TableReader.
    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);

    const auto partition_value = block.get_by_position(0).column->convert_to_full_column_if_const();
    const auto& partition_value_data = assert_cast<const ColumnString&>(*partition_value);
    ASSERT_EQ(partition_value_data.size(), 1);
    EXPECT_EQ(partition_value_data.get_data_at(0).to_string(), "p1");

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, ConstantPartitionFilterSkipsSplitWhenFalse) {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_table_reader_constant_partition_filter_skip_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_parquet_file(file_path, 1, "one");

    std::vector<ColumnDefinition> projected_columns;
    auto partition_column = make_table_column(0, "part", std::make_shared<DataTypeInt32>());
    partition_column.is_partition_key = true;
    projected_columns.push_back(std::move(partition_column));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    set_name_identifiers(&projected_columns);
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {prepared_conjunct(
                                            &state, table_int32_greater_than_expr(0, 0, 10))},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                            })
                        .ok());

    auto split_options = build_split_options(file_path);
    split_options.partition_values.emplace("part", Field::create_field<TYPE_INT>(7));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    EXPECT_TRUE(eos);
    EXPECT_EQ(block.get_by_position(0).column->size(), 0);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, ConstantPartitionFilterKeepsSplitWhenTrue) {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_table_reader_constant_partition_filter_keep_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_parquet_file(file_path, 1, "one");

    std::vector<ColumnDefinition> projected_columns;
    auto partition_column = make_table_column(0, "part", std::make_shared<DataTypeInt32>());
    partition_column.is_partition_key = true;
    projected_columns.push_back(std::move(partition_column));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    set_name_identifiers(&projected_columns);
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
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
    split_options.partition_values.emplace("part", Field::create_field<TYPE_INT>(7));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);

    expect_int32_column_values(*block.get_by_position(0).column, {7});

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, RuntimeFilterOnConstantPartitionIsNotPreExecuted) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_table_reader_constant_runtime_filter";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_parquet_file(file_path, 1, "one");

    std::vector<ColumnDefinition> projected_columns;
    auto partition_column = make_table_column(0, "part", std::make_shared<DataTypeInt32>());
    partition_column.is_partition_key = true;
    projected_columns.push_back(std::move(partition_column));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    set_name_identifiers(&projected_columns);
    TableReader reader;
    ASSERT_TRUE(
            reader.init({
                                .projected_columns = projected_columns,
                                .column_predicates = {},
                                .conjuncts = {prepared_conjunct(
                                        &state, runtime_filter_wrapper_expr(
                                                        table_int32_greater_than_expr(0, 0, 1)))},
                                .format = FileFormat::PARQUET,
                                .scan_params = nullptr,
                                .io_ctx = nullptr,
                                .runtime_state = &state,
                                .scanner_profile = nullptr,
                        })
                    .ok());

    auto split_options = build_split_options(file_path);
    split_options.partition_values.emplace("part", Field::create_field<TYPE_INT>(7));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    const auto status = reader.get_block(&block, &eos);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_FALSE(eos);
    expect_int32_column_values(*block.get_by_position(0).column, {7});

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, IcebergVirtualColumnsUseRowLineageMetadata) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_iceberg_virtual_columns_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3}, {10, 20, 30}, {"one", "two", "three"});

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(
            make_table_column(100, "_row_id", make_nullable(std::make_shared<DataTypeInt64>())));
    projected_columns.push_back(
            make_table_column(101, "_last_updated_sequence_number",
                              make_nullable(std::make_shared<DataTypeInt64>())));
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    doris::format::iceberg::IcebergTableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
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

    const auto& id_column = assert_cast<const ColumnInt32&>(*block.get_by_position(2).column);

    ASSERT_EQ(block.rows(), 2);
    EXPECT_EQ(id_column.get_element(0), 2);
    EXPECT_EQ(id_column.get_element(1), 3);
    expect_nullable_int64_column_values(*block.get_by_position(0).column, {1001, 1002});
    expect_nullable_int64_column_values(*block.get_by_position(1).column, {77, 77});

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, IcebergRowLineageUsesPhysicalRowIdAndFillsNulls) {
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
                                    .column_predicates = {},
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

TEST(TableReaderTest, IcebergPhysicalRowIdKeepsNullsWithoutFirstRowId) {
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
                                    .column_predicates = {},
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

TEST(TableReaderTest, IcebergMissingRowIdStaysNullWithoutFirstRowId) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_iceberg_missing_row_id_no_first_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3}, {10, 20, 30}, {"one", "two", "three"});

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(
            make_table_column(100, "_row_id", make_nullable(std::make_shared<DataTypeInt64>())));
    projected_columns.push_back(
            make_table_column(101, "_last_updated_sequence_number",
                              make_nullable(std::make_shared<DataTypeInt64>())));
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    doris::format::iceberg::IcebergTableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
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

TEST(TableReaderTest, IcebergRowIdPredicateFiltersAfterRowLineageMaterialization) {
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
                                    .column_predicates = {},
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

TEST(TableReaderTest, IcebergLastUpdatedSequencePredicateFiltersAfterMaterialization) {
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
                                    .column_predicates = {},
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

TEST(TableReaderTest, IcebergRowidVirtualColumnUsesDataFilePosition) {
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
                                    .column_predicates = {},
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

TEST(TableReaderTest, IcebergVirtualColumnsKeepRowLineageAfterConjunctFiltering) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_iceberg_virtual_columns_conjunct_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3}, {10, 20, 30}, {"one", "two", "three"});

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(
            make_table_column(100, "_row_id", make_nullable(std::make_shared<DataTypeInt64>())));
    projected_columns.push_back(
            make_table_column(101, "_last_updated_sequence_number",
                              make_nullable(std::make_shared<DataTypeInt64>())));
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    doris::format::iceberg::IcebergTableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
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

    const auto& id_column = assert_cast<const ColumnInt32&>(*block.get_by_position(2).column);

    ASSERT_EQ(block.rows(), 2);
    EXPECT_EQ(id_column.get_element(0), 2);
    EXPECT_EQ(id_column.get_element(1), 3);
    expect_nullable_int64_column_values(*block.get_by_position(0).column, {3001, 3002});
    expect_nullable_int64_column_values(*block.get_by_position(1).column, {88, 88});

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, IcebergVirtualColumnsKeepRowLineageAfterRowGroupPredicatePruning) {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_iceberg_virtual_columns_row_group_predicate_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    // ColumnPredicate is used for row-group/statistics pruning. Keep one row per row group so
    // id > 2 prunes the first two row groups and leaves only the third file-local row.
    write_int_pair_parquet_file(file_path, {1, 2, 3}, {10, 20, 30}, {"one", "two", "three"}, 1);

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(
            make_table_column(100, "_row_id", make_nullable(std::make_shared<DataTypeInt64>())));
    projected_columns.push_back(
            make_table_column(101, "_last_updated_sequence_number",
                              make_nullable(std::make_shared<DataTypeInt64>())));
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    TableColumnPredicates column_predicates;
    add_column_predicate(&column_predicates, GlobalIndex(2),
                         create_comparison_predicate<PredicateType::GT>(
                                 0, "id", std::make_shared<DataTypeInt32>(),
                                 Field::create_field<TYPE_INT>(2), false));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    doris::format::iceberg::IcebergTableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = std::move(column_predicates),
                                    .conjuncts = {},
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

    const auto& id_column = assert_cast<const ColumnInt32&>(*block.get_by_position(2).column);

    ASSERT_EQ(block.rows(), 1);
    EXPECT_EQ(id_column.get_element(0), 3);
    expect_nullable_int64_column_values(*block.get_by_position(0).column, {4002});
    expect_nullable_int64_column_values(*block.get_by_position(1).column, {99});

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, IcebergDeletionVectorUsesTableReaderDeleteFileInterface) {
    TTableFormatFileDesc table_format_desc;
    TIcebergFileDesc iceberg_desc;
    iceberg_desc.__set_format_version(2);
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

TEST(TableReaderTest, IcebergDeletionVectorRejectsMultipleDeleteFiles) {
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

TEST(TableReaderTest, IcebergTableReaderAppliesDeletionVectorFile) {
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
                                    .column_predicates = {},
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

TEST(TableReaderTest, PaimonTableReaderAppliesBitmapDeletionVectorFile) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_paimon_deletion_vector_file_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    const auto dv_path = (test_dir / "delete-vector.bin").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3, 4, 5}, {10, 20, 30, 40, 50},
                                {"one", "two", "three", "four", "five"});
    const auto dv_length = write_paimon_deletion_vector_file(dv_path, {0, 4});

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeProfile profile("test_profile");
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto scan_params = make_local_parquet_scan_params();
    io::FileReaderStats file_reader_stats;
    io::FileCacheStatistics file_cache_stats;
    auto io_ctx = make_io_context(&file_reader_stats, &file_cache_stats);
    ShardedKVCache cache(1);
    doris::format::paimon::PaimonReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
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
            make_paimon_table_format_desc(dv_path, 0, dv_length));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    std::vector<int32_t> ids;
    bool eos = false;
    while (!eos) {
        Block block = build_table_block(projected_columns);
        ASSERT_TRUE(reader.get_block(&block, &eos).ok());
        if (block.rows() == 0) {
            continue;
        }
        const auto& id_column = assert_cast<const ColumnInt32&>(*block.get_by_position(0).column);
        for (size_t row = 0; row < block.rows(); ++row) {
            ids.push_back(id_column.get_element(row));
        }
    }
    EXPECT_EQ(ids, std::vector<int32_t>({2, 3, 4}));

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, IcebergTableReaderDoesNotPushDownAggregateWithDeletes) {
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
                                    .column_predicates = {},
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
    const auto& id_column = assert_cast<const ColumnInt32&>(*block.get_by_position(0).column);
    EXPECT_EQ(id_column.get_element(0), 2);
    EXPECT_EQ(id_column.get_element(1), 3);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

// Covers TopN lazy materialization on Iceberg schema-evolution tables. The first-phase scan adds a
// synthesized GLOBAL_ROWID column to the file schema. That virtual column must not make Iceberg
// fall back from field-id mapping to name mapping, otherwise renamed columns are read as defaults
// from old files.
TEST(TableReaderTest, IcebergMappingModeIgnoresGlobalRowIdVirtualColumn) {
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
TEST(TableReaderTest, IcebergMappingModeRequiresFieldIdsForDataColumns) {
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

TEST(TableReaderTest, IcebergTableReaderDoesNotPushDownAggregateWithPositionDelete) {
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
                                    .column_predicates = {},
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
    const auto& id_column = assert_cast<const ColumnInt32&>(*block.get_by_position(0).column);
    EXPECT_EQ(id_column.get_element(0), 1);
    EXPECT_EQ(id_column.get_element(1), 3);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, IcebergTableLevelCountUsesAssignedRowCountWithPositionDelete) {
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
                                    .column_predicates = {},
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

TEST(TableReaderTest, IcebergPositionDeleteFallsBackToSplitPath) {
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
                                    .column_predicates = {},
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

TEST(TableReaderTest, IcebergTableReaderDoesNotPushDownAggregateWithEqualityDelete) {
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
                                    .column_predicates = {},
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
    const auto& id_column = assert_cast<const ColumnInt32&>(*block.get_by_position(0).column);
    EXPECT_EQ(id_column.get_element(0), 1);
    EXPECT_EQ(id_column.get_element(1), 3);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, IcebergEqualityDeleteCastsDataColumnToDeleteKeyType) {
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
                                    .column_predicates = {},
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

TEST(TableReaderTest, IcebergPositionDeleteOnlyMatchesOriginalDataFilePath) {
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
                                    .column_predicates = {},
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

TEST(TableReaderTest, IcebergRowLineageRemainsFileLocalAfterDeleteFiltering) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_iceberg_row_lineage_delete_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    const auto delete_file_path = (test_dir / "position-delete.parquet").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3}, {10, 20, 30}, {"one", "two", "three"});
    write_position_delete_parquet_file(delete_file_path, {file_path}, {1});

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(
            make_table_column(100, "_row_id", make_nullable(std::make_shared<DataTypeInt64>())));
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
                                    .column_predicates = {},
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
    const auto& id_column = assert_cast<const ColumnInt32&>(*block.get_by_position(1).column);
    EXPECT_EQ(id_column.get_element(0), 1);
    EXPECT_EQ(id_column.get_element(1), 3);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, IcebergTableReaderAppliesPositionDeleteFile) {
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
                                    .column_predicates = {},
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

TEST(TableReaderTest, IcebergTableReaderMergesDeletionVectorAndPositionDeleteFiles) {
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
                                    .column_predicates = {},
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

    EXPECT_EQ(read_iceberg_ids(&reader, projected_columns), std::vector<int32_t>({2, 3, 5}));

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, RowPositionDeletePredicateColumnIsNotRepeatedAsOutputColumn) {
    const auto row_position_column_id = ROW_POSITION_COLUMN_ID;
    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(
            make_table_column(100, "_row_id", make_nullable(std::make_shared<DataTypeInt64>())));
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

TEST(TableReaderTest, ParquetReaderReadsOnlyRowGroupsInFileRange) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_table_reader_file_range_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_int_pair_parquet_file(file_path, {1, 2, 3}, {10, 20, 30},
                                {"range_group_one", "range_group_two", "range_group_three"}, 1);

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));
    projected_columns.push_back(make_table_column(2, "value", std::make_shared<DataTypeString>()));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    set_name_identifiers(&projected_columns);
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                            })
                        .ok());

    ASSERT_TRUE(reader.prepare_split(build_split_options_for_row_group_mid(file_path, 1)).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);

    const auto& id_column = assert_cast<const ColumnInt32&>(*block.get_by_position(0).column);
    const auto& value_column = assert_cast<const ColumnString&>(*block.get_by_position(1).column);
    ASSERT_EQ(block.rows(), 1);
    EXPECT_EQ(id_column.get_element(0), 2);
    EXPECT_EQ(value_column.get_data_at(0).to_string(), "range_group_two");

    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    EXPECT_TRUE(eos);
    EXPECT_EQ(block.rows(), 0);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, ProjectedColumnsUseMapperExpressionForSameNameDifferentIdParquetSchema) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_table_reader_same_name_diff_id_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_parquet_file(file_path, 1, "one");

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(99, "id", std::make_shared<DataTypeInt32>()));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    set_name_identifiers(&projected_columns);
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                            })
                        .ok());

    ASSERT_TRUE(reader.prepare_split(build_split_options(file_path)).ok());

    // The table column has the same name as the Parquet field, but a different field id.
    // ColumnMapper should still resolve it by name and build a SlotRef projection from the file
    // column into the requested table column.
    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);

    const auto& id_column = assert_cast<const ColumnInt32&>(*block.get_by_position(0).column);
    ASSERT_EQ(id_column.size(), 1);
    EXPECT_EQ(id_column.get_element(0), 1);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, ProjectedColumnsUseMapperExpressionsForParquetSchemaMismatch) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_table_reader_mapper_expr_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_parquet_file(file_path, 7, "seven");

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt64>()));
    projected_columns.push_back(make_table_column(1, "value", std::make_shared<DataTypeString>()));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    set_name_identifiers(&projected_columns);
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .column_predicates = {},
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                            })
                        .ok());

    ASSERT_TRUE(reader.prepare_split(build_split_options(file_path)).ok());

    // The table projection requests id as BIGINT instead of the file INT, so ColumnMapper should
    // build a Cast expression. The second field has the same type and should build a SlotRef
    // projection. Both columns should still materialize in table schema order.
    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);

    ASSERT_EQ(block.get_by_position(0).name, "id");
    ASSERT_EQ(block.get_by_position(1).name, "value");
    const auto& id_column = assert_cast<const ColumnInt64&>(*block.get_by_position(0).column);
    const auto& value_column = assert_cast<const ColumnString&>(*block.get_by_position(1).column);
    ASSERT_EQ(id_column.size(), 1);
    ASSERT_EQ(value_column.size(), 1);
    EXPECT_EQ(id_column.get_element(0), 7);
    EXPECT_EQ(value_column.get_data_at(0).to_string(), "seven");

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

} // namespace
} // namespace doris::format
