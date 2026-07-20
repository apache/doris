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
#include "exprs/vectorized_fn_call.h"
#include "exprs/vexpr.h"
#include "exprs/vliteral.h"
#include "exprs/vruntimefilter_wrapper.h"
#include "exprs/vslot_ref.h"
#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/ExternalTableSchema_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "io/io_common.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "storage/segment/condition_cache.h"

namespace doris::format {
namespace {

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
    const auto nullable_int_type = make_nullable(std::make_shared<DataTypeInt32>());
    return VSlotRef::create_shared(slot_id, column_id, slot_id, nullable_int_type, column_name);
}

VExprSPtr table_int32_literal(int32_t value) {
    return VLiteral::create_shared(std::make_shared<DataTypeInt32>(),
                                   Field::create_field<TYPE_INT>(value));
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
    const auto nullable_int_type = make_nullable(int_type);
    auto expr = table_function_expr("gt", make_nullable(std::make_shared<DataTypeUInt8>()),
                                    {nullable_int_type, int_type}, TExprNodeType::BINARY_PRED,
                                    TExprOpcode::GT);
    expr->add_child(table_int32_slot_ref(slot_id, column_id, "id"));
    expr->add_child(table_int32_literal(value));
    return expr;
}

VExprSPtr runtime_filter_wrapper_expr(VExprSPtr impl) {
    TExprNode node;
    node.__set_node_type(TExprNodeType::SLOT_REF);
    node.__set_type(std::make_shared<DataTypeUInt8>()->to_thrift());
    node.__set_num_children(1);
    return VRuntimeFilterWrapper::create_shared(node, std::move(impl), 0, false,
                                                /*filter_id=*/1);
}

class NonDeterministicPartitionPredicate final : public VExpr {
public:
    explicit NonDeterministicPartitionPredicate(bool* executed)
            : VExpr(std::make_shared<DataTypeUInt8>(), false), _executed(executed) {}

    Status execute_column_impl(VExprContext*, const Block*, const Selector*, size_t count,
                               ColumnPtr& result_column) const override {
        DORIS_CHECK(_executed != nullptr);
        *_executed = true;
        auto result = ColumnUInt8::create();
        result->get_data().resize_fill(count, 0);
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }
    bool is_deterministic() const override { return false; }

    Status clone_node(VExprSPtr* cloned_expr) const override {
        DORIS_CHECK(cloned_expr != nullptr);
        *cloned_expr = std::make_shared<NonDeterministicPartitionPredicate>(_executed);
        return Status::OK();
    }

private:
    bool* const _executed;
    const std::string _expr_name = "NonDeterministicPartitionPredicate";
};

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

class TableReaderMaterializeTestHelper final : public TableReader {
public:
    using TableReader::_materialize_map_mapping_column;
};

VExprSPtr table_int32_sum_expr(int left_slot_id, int left_column_id, int right_slot_id,
                               int right_column_id) {
    const auto int_type = std::make_shared<DataTypeInt32>();
    const auto nullable_int_type = make_nullable(int_type);
    auto expr =
            table_function_expr("add", nullable_int_type, {nullable_int_type, nullable_int_type});
    expr->add_child(table_int32_slot_ref(left_slot_id, left_column_id, "id"));
    expr->add_child(table_int32_slot_ref(right_slot_id, right_column_id, "score"));
    return expr;
}

VExprSPtr table_int32_sum_greater_than_expr(int left_slot_id, int left_column_id, int right_slot_id,
                                            int right_column_id, int32_t value) {
    const auto int_type = std::make_shared<DataTypeInt32>();
    const auto nullable_int_type = make_nullable(int_type);
    auto expr = table_function_expr("gt", make_nullable(std::make_shared<DataTypeUInt8>()),
                                    {nullable_int_type, int_type}, TExprNodeType::BINARY_PRED,
                                    TExprOpcode::GT);
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

Block build_table_block(const std::vector<ColumnDefinition>& columns) {
    Block block;
    for (const auto& column : columns) {
        block.insert({column.type->create_column(), column.type, column.name});
    }
    return block;
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

void expect_nullable_column_all_null(const IColumn& column) {
    const auto full_column = column.convert_to_full_column_if_const();
    const auto& nullable_column = assert_cast<const ColumnNullable&>(*full_column);
    for (const auto is_null : nullable_column.get_null_map_data()) {
        EXPECT_EQ(is_null, 1);
    }
}

const IColumn& expect_not_null_table_column(const Block& block, size_t position) {
    return expect_not_null_nullable_nested_column(*block.get_by_position(position).column);
}

ColumnDefinition make_table_column(int32_t id, const std::string& name, const DataTypePtr& type);

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

schema::external::TFieldPtr external_schema_field(std::string name, int32_t id,
                                                  std::vector<std::string> aliases = {}) {
    auto field = std::make_shared<schema::external::TField>();
    field->__set_name(std::move(name));
    field->__set_id(id);
    if (!aliases.empty()) {
        field->__set_name_mapping(std::move(aliases));
    }
    schema::external::TFieldPtr field_ptr;
    field_ptr.field_ptr = std::move(field);
    field_ptr.__isset.field_ptr = true;
    return field_ptr;
}

schema::external::TFieldPtr external_array_field(std::string name, int32_t id,
                                                 schema::external::TFieldPtr item_field,
                                                 std::vector<std::string> aliases = {}) {
    auto field = external_schema_field(std::move(name), id, std::move(aliases));
    schema::external::TArrayField array_field;
    array_field.__set_item_field(std::move(item_field));
    field.field_ptr->nestedField.__set_array_field(std::move(array_field));
    field.field_ptr->__isset.nestedField = true;
    return field;
}

schema::external::TFieldPtr external_map_field(std::string name, int32_t id,
                                               schema::external::TFieldPtr key_field,
                                               schema::external::TFieldPtr value_field,
                                               std::vector<std::string> aliases = {}) {
    auto field = external_schema_field(std::move(name), id, std::move(aliases));
    schema::external::TMapField map_field;
    map_field.__set_key_field(std::move(key_field));
    map_field.__set_value_field(std::move(value_field));
    field.field_ptr->nestedField.__set_map_field(std::move(map_field));
    field.field_ptr->__isset.nestedField = true;
    return field;
}

schema::external::TFieldPtr external_struct_field(std::string name, int32_t id,
                                                  std::vector<schema::external::TFieldPtr> fields,
                                                  std::vector<std::string> aliases = {}) {
    auto field = external_schema_field(std::move(name), id, std::move(aliases));
    schema::external::TStructField struct_field;
    struct_field.__set_fields(std::move(fields));
    field.field_ptr->nestedField.__set_struct_field(std::move(struct_field));
    field.field_ptr->__isset.nestedField = true;
    return field;
}

schema::external::TSchema external_schema(int64_t schema_id,
                                          std::vector<schema::external::TFieldPtr> fields) {
    schema::external::TStructField root_field;
    root_field.__set_fields(std::move(fields));
    schema::external::TSchema schema;
    schema.__set_schema_id(schema_id);
    schema.__set_root_field(std::move(root_field));
    return schema;
}

ColumnDefinition make_nullable_column_definition(ColumnDefinition column) {
    column.type = make_table_test_type(column.type);
    for (auto& child : column.children) {
        child = make_nullable_column_definition(std::move(child));
    }
    return column;
}

MutableColumnPtr make_not_null_nullable_column(MutableColumnPtr nested_column) {
    auto null_map = ColumnUInt8::create();
    for (size_t i = 0; i < nested_column->size(); ++i) {
        null_map->insert_value(0);
    }
    return ColumnNullable::create(std::move(nested_column), std::move(null_map));
}

class TableReaderCharVarcharTestHelper final : public TableReader {
public:
    using TableReader::_should_truncate_char_or_varchar_column;
    using TableReader::_truncate_char_or_varchar_column;
};

class TableReaderCastTestHelper final : public TableReader {
public:
    using TableReader::_cast_column_to_type;
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

VExprContextSPtr prepared_conjunct(RuntimeState* state, const VExprSPtr& expr) {
    auto ctx = VExprContext::create_shared(expr);
    auto status = ctx->prepare(state, RowDescriptor());
    EXPECT_TRUE(status.ok()) << status;
    status = ctx->open(state);
    EXPECT_TRUE(status.ok()) << status;
    return ctx;
}

struct FakeFileReaderState {
    int init_count = 0;
    int open_count = 0;
    int close_count = 0;
    int64_t total_rows = 2;
    int64_t aggregate_count = -1;
    int64_t condition_cache_base_granule = 0;
    size_t condition_cache_num_granules = 0;
    bool eof_with_first_batch = true;
    bool inject_delete_conjunct = false;
    bool stop_during_aggregate = false;
    bool stop_during_read = false;
    bool not_found_during_init = false;
    std::shared_ptr<FileScanRequest> last_request;
    std::shared_ptr<ConditionCacheContext> condition_cache_ctx;
    std::shared_ptr<io::IOContext> io_ctx;
};

class FakeFileReader final : public FileReader {
public:
    FakeFileReader(std::shared_ptr<io::FileSystemProperties>& system_properties,
                   std::unique_ptr<io::FileDescription>& file_description,
                   std::vector<ColumnDefinition> schema, std::shared_ptr<FakeFileReaderState> state)
            : FileReader(system_properties, file_description, state->io_ctx, nullptr),
              _schema(std::move(schema)),
              _state(std::move(state)) {}

    Status init(RuntimeState* state) override {
        (void)state;
        ++_state->init_count;
        if (_state->not_found_during_init) {
            return Status::NotFound("fake table reader input is missing");
        }
        _eof = false;
        return Status::OK();
    }

    Status get_schema(std::vector<ColumnDefinition>* file_schema) const override {
        DORIS_CHECK(file_schema != nullptr);
        *file_schema = _schema;
        for (auto& column : *file_schema) {
            column = make_nullable_column_definition(std::move(column));
        }
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
                file_block->replace_by_position(block_position.value(),
                                                make_not_null_nullable_column(std::move(column)));
            } else if (file_column_id == LocalColumnId(1)) {
                auto column = ColumnString::create();
                column->insert_data("one", 3);
                column->insert_data("two", 3);
                file_block->replace_by_position(block_position.value(),
                                                make_not_null_nullable_column(std::move(column)));
            } else if (file_column_id == LocalColumnId(2)) {
                auto country_values = ColumnString::create();
                country_values->insert_data("USA", 3);
                country_values->insert_data("UK", 2);
                auto country_column = make_not_null_nullable_column(std::move(country_values));

                auto city_column = ColumnString::create();
                city_column->insert_data("New York", 8);
                city_column->insert_data("London", 6);

                MutableColumns struct_children;
                struct_children.push_back(std::move(country_column));
                struct_children.push_back(make_not_null_nullable_column(std::move(city_column)));
                auto struct_column = ColumnStruct::create(std::move(struct_children));

                file_block->replace_by_position(
                        block_position.value(),
                        make_not_null_nullable_column(std::move(struct_column)));
            } else {
                return Status::InvalidArgument("Unexpected fake file column id {}",
                                               file_column_id.value());
            }
        }

        if (_state->stop_during_read) {
            DORIS_CHECK(_state->io_ctx != nullptr);
            _state->io_ctx->should_stop = true;
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

    Status get_aggregate_result(const FileAggregateRequest& request,
                                FileAggregateResult* result) override {
        DORIS_CHECK(result != nullptr);
        if (_state->aggregate_count < 0) {
            return FileReader::get_aggregate_result(request, result);
        }
        if (request.agg_type != TPushAggOp::type::COUNT) {
            return Status::NotSupported("fake reader only supports COUNT aggregate pushdown");
        }
        if (_state->stop_during_aggregate) {
            DORIS_CHECK(_state->io_ctx != nullptr);
            _state->io_ctx->should_stop = true;
            return Status::EndOfFile("stop");
        }
        result->count = _state->aggregate_count;
        result->columns.clear();
        _record_scan_rows(_state->aggregate_count);
        _eof = true;
        return Status::OK();
    }

    void set_condition_cache_context(std::shared_ptr<ConditionCacheContext> ctx) override {
        _state->condition_cache_ctx = std::move(ctx);
        if (_state->condition_cache_ctx != nullptr && !_state->condition_cache_ctx->is_hit) {
            _state->condition_cache_ctx->base_granule = _state->condition_cache_base_granule;
            if (_state->condition_cache_num_granules > 0) {
                _state->condition_cache_ctx->num_granules = _state->condition_cache_num_granules;
            }
        }
    }

    int64_t get_total_rows() const override { return _state->total_rows; }

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

    Status customize_file_scan_request(FileScanRequest* file_request) override {
        RETURN_IF_ERROR(TableReader::customize_file_scan_request(file_request));
        if (_state->inject_delete_conjunct) {
            // Table-format delete handling is represented in v2 by TableReader injecting
            // delete_conjuncts into the file scan request. The fake reader does not execute it;
            // this only tests that condition cache is disabled once such table-level delete state
            // is present in the request.
            file_request->delete_conjuncts.push_back(
                    VExprContext::create_shared(table_int32_literal(1)));
        }
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
        ExecEnv::GetInstance()->_condition_cache = _cache.get();
    }

    ~ScopedConditionCacheForTest() { ExecEnv::GetInstance()->_condition_cache = _previous; }

    segment_v2::ConditionCache* get() { return _cache.get(); }

private:
    segment_v2::ConditionCache* _previous = nullptr;
    std::unique_ptr<segment_v2::ConditionCache> _cache;
};

TEST(TableReaderTest, PrepareSplitPrunesPartitionRuntimeFilter) {
    std::vector<ColumnDefinition> projected_columns;
    auto partition_column = make_table_column(0, "part", std::make_shared<DataTypeInt32>());
    partition_column.is_partition_key = true;
    projected_columns.push_back(std::move(partition_column));
    set_name_identifiers(&projected_columns);

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    RuntimeProfile profile("scanner");
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = &profile,
                            })
                        .ok());

    SplitReadOptions pruned_split;
    pruned_split.current_range.__set_path("unused-pruned-file");
    pruned_split.partition_values.emplace("part", Field::create_field<TYPE_INT>(7));
    pruned_split.partition_prune_conjuncts.push_back(VExprContext::create_shared(
            runtime_filter_wrapper_expr(table_int32_greater_than_expr(0, 0, 10))));
    ASSERT_TRUE(reader.prepare_split(pruned_split).ok());
    EXPECT_TRUE(reader.current_split_pruned());
    ASSERT_NE(profile.get_counter("RuntimeFilterPartitionPrunedRangeNum"), nullptr);
    EXPECT_EQ(profile.get_counter("RuntimeFilterPartitionPrunedRangeNum")->value(), 1);

    SplitReadOptions retained_split;
    retained_split.current_range.__set_path("unused-retained-file");
    retained_split.partition_values.emplace("part", Field::create_field<TYPE_INT>(11));
    retained_split.partition_prune_conjuncts.push_back(VExprContext::create_shared(
            runtime_filter_wrapper_expr(table_int32_greater_than_expr(0, 0, 10))));
    ASSERT_TRUE(reader.prepare_split(retained_split).ok());
    EXPECT_FALSE(reader.current_split_pruned());
}

TEST(TableReaderTest, PrepareSplitDoesNotEvaluateNonDeterministicPartitionPredicate) {
    std::vector<ColumnDefinition> projected_columns;
    auto partition_column = make_table_column(0, "part", std::make_shared<DataTypeInt32>());
    partition_column.is_partition_key = true;
    projected_columns.push_back(std::move(partition_column));
    set_name_identifiers(&projected_columns);

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    RuntimeProfile profile("scanner");
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = &profile,
                            })
                        .ok());

    bool predicate_executed = false;
    auto predicate = std::make_shared<NonDeterministicPartitionPredicate>(&predicate_executed);
    predicate->add_child(table_int32_slot_ref(0, 0, "part"));
    SplitReadOptions split;
    split.current_range.__set_path("unused-nondeterministic-file");
    split.partition_values.emplace("part", Field::create_field<TYPE_INT>(7));
    split.partition_prune_conjuncts.push_back(
            VExprContext::create_shared(runtime_filter_wrapper_expr(std::move(predicate))));
    split.partition_prune_conjuncts.push_back(VExprContext::create_shared(
            runtime_filter_wrapper_expr(table_int32_greater_than_expr(0, 0, 10))));

    ASSERT_TRUE(reader.prepare_split(split).ok());
    EXPECT_FALSE(predicate_executed);
    EXPECT_FALSE(reader.current_split_pruned());
    ASSERT_NE(profile.get_counter("RuntimeFilterPartitionPrunedRangeNum"), nullptr);
    EXPECT_EQ(profile.get_counter("RuntimeFilterPartitionPrunedRangeNum")->value(), 0);
}

TEST(TableReaderTest, ConstantPruningStopsAtUnsafePredicate) {
    std::vector<ColumnDefinition> projected_columns;
    auto partition_column = make_table_column(0, "part", std::make_shared<DataTypeInt32>());
    partition_column.is_partition_key = true;
    projected_columns.push_back(std::move(partition_column));
    set_name_identifiers(&projected_columns);

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    bool predicate_executed = false;
    auto unsafe_predicate =
            std::make_shared<NonDeterministicPartitionPredicate>(&predicate_executed);
    unsafe_predicate->add_child(table_int32_slot_ref(0, 0, "part"));
    auto fake_state = std::make_shared<FakeFileReaderState>();
    FakeTableReader reader({}, fake_state);
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .conjuncts =
                                            {
                                                    prepared_conjunct(&state, unsafe_predicate),
                                                    prepared_conjunct(&state,
                                                                      table_int32_greater_than_expr(
                                                                              0, 0, 10)),
                                            },
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                            })
                        .ok());

    SplitReadOptions split;
    split.current_range.__set_path("fake-table-reader-input");
    split.partition_values.emplace("part", Field::create_field<TYPE_INT>(7));
    ASSERT_TRUE(reader.prepare_split(split).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    EXPECT_FALSE(predicate_executed);
    EXPECT_FALSE(eos);
    EXPECT_EQ(fake_state->open_count, 1);
    ASSERT_TRUE(reader.close().ok());
}

TEST(TableReaderTest, ConstantPruningStopsAtUnsafeSlotlessPredicate) {
    std::vector<ColumnDefinition> projected_columns;
    auto partition_column = make_table_column(0, "part", std::make_shared<DataTypeInt32>());
    partition_column.is_partition_key = true;
    projected_columns.push_back(std::move(partition_column));
    set_name_identifiers(&projected_columns);

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    bool predicate_executed = false;
    auto unsafe_slotless_predicate =
            std::make_shared<NonDeterministicPartitionPredicate>(&predicate_executed);
    auto fake_state = std::make_shared<FakeFileReaderState>();
    FakeTableReader reader({}, fake_state);
    ASSERT_TRUE(
            reader
                    .init({
                            .projected_columns = projected_columns,
                            .conjuncts =
                                    {
                                            prepared_conjunct(&state, unsafe_slotless_predicate),
                                            prepared_conjunct(&state, table_int32_greater_than_expr(
                                                                              0, 0, 10)),
                                    },
                            .format = FileFormat::PARQUET,
                            .scan_params = nullptr,
                            .io_ctx = nullptr,
                            .runtime_state = &state,
                            .scanner_profile = nullptr,
                    })
                    .ok());

    SplitReadOptions split;
    split.current_range.__set_path("fake-table-reader-input");
    split.partition_values.emplace("part", Field::create_field<TYPE_INT>(7));
    ASSERT_TRUE(reader.prepare_split(split).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    EXPECT_TRUE(predicate_executed);
    EXPECT_FALSE(eos);
    // The later partition predicate is false for part=7. Opening the file proves constant pruning
    // stopped at the earlier unsafe expression even though that expression had no slot and thus no
    // entry in `_table_filters`.
    EXPECT_EQ(fake_state->open_count, 1);
    ASSERT_NE(fake_state->last_request, nullptr);
    // A slotless unsafe conjunct is an ordering barrier even though it has no TableFilter entry.
    // The later predicate must stay on the scanner's row-level path instead of running inside the
    // file reader before the unsafe conjunct.
    EXPECT_TRUE(fake_state->last_request->conjuncts.empty());
    ASSERT_TRUE(reader.close().ok());
}

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

    const auto& value_column =
            assert_cast<const ColumnString&>(expect_not_null_table_column(block, 0));
    const auto& id_column = assert_cast<const ColumnInt32&>(expect_not_null_table_column(block, 1));
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

TEST(TableReaderTest, PrepareSplitReplacesInitialConjunctSnapshot) {
    std::vector<ColumnDefinition> file_schema;
    file_schema.push_back(make_file_column(0, "id", std::make_shared<DataTypeInt32>()));

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));
    set_name_identifiers(&projected_columns);

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto fake_state = std::make_shared<FakeFileReaderState>();
    FakeTableReader reader(file_schema, fake_state);
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .conjuncts = {VExprContext::create_shared(
                                            table_int32_greater_than_expr(0, 0, 0))},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                            })
                        .ok());

    SplitReadOptions split_options;
    split_options.current_range.__set_path("fake-table-reader-input");
    split_options.conjuncts = VExprContextSPtrs {VExprContext::create_shared(
            runtime_filter_wrapper_expr(table_int32_greater_than_expr(0, 0, 1)))};
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_NE(fake_state->last_request, nullptr);
    ASSERT_EQ(fake_state->last_request->conjuncts.size(), 1);
    EXPECT_TRUE(fake_state->last_request->conjuncts.front()->root()->is_rf_wrapper());
    ASSERT_TRUE(reader.close().ok());
}

TEST(TableReaderTest, RefreshedConjunctDisablesTableLevelCount) {
    std::vector<ColumnDefinition> file_schema;
    file_schema.push_back(make_file_column(0, "id", std::make_shared<DataTypeInt32>()));

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));
    set_name_identifiers(&projected_columns);

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto fake_state = std::make_shared<FakeFileReaderState>();
    FakeTableReader reader(file_schema, fake_state);
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

    SplitReadOptions split_options;
    split_options.current_range.__set_path("fake-table-reader-input");
    split_options.conjuncts = VExprContextSPtrs {VExprContext::create_shared(
            runtime_filter_wrapper_expr(table_int32_greater_than_expr(0, 0, 1)))};
    set_table_level_row_count(&split_options, 5);
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    // The metadata count advertises five rows, while the fake reader contains two. Opening the
    // reader and returning its rows proves the fresh runtime filter did not take the synthetic
    // table-level COUNT path that would bypass all row predicates.
    EXPECT_EQ(fake_state->open_count, 1);
    EXPECT_EQ(block.rows(), 2);
    ASSERT_TRUE(reader.close().ok());
}

TEST(TableReaderTest, PendingRuntimeFilterDisablesTableLevelCount) {
    std::vector<ColumnDefinition> file_schema;
    file_schema.push_back(make_file_column(0, "id", std::make_shared<DataTypeInt32>()));

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));
    set_name_identifiers(&projected_columns);

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto fake_state = std::make_shared<FakeFileReaderState>();
    fake_state->aggregate_count = state.batch_size() + 5;
    FakeTableReader reader(file_schema, fake_state);
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

    SplitReadOptions split_options;
    split_options.current_range.__set_path("fake-table-reader-input");
    // A pending runtime filter makes metadata COUNT ineligible before its first synthetic batch.
    // This prevents the filter from arriving between scheduler reads after unfiltered rows have
    // already escaped.
    split_options.all_runtime_filters_applied = false;
    set_table_level_row_count(&split_options, state.batch_size() + 5);
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    EXPECT_EQ(fake_state->open_count, 1);
    EXPECT_EQ(block.rows(), 2);
    ASSERT_TRUE(reader.close().ok());
}

TEST(TableReaderTest, PendingRuntimeFilterDisablesMinMaxPushdown) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_table_reader_pending_rf_minmax_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);
    const auto file_path = (test_dir / "split.parquet").string();
    write_int_pair_parquet_file(file_path, {3, 1, 5, 2}, {30, 10, 50, 20},
                                {"three", "one", "five", "two"}, 2);

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));
    projected_columns.push_back(make_table_column(1, "score", std::make_shared<DataTypeInt32>()));
    set_name_identifiers(&projected_columns);

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                                    .push_down_agg_type = TPushAggOp::type::MINMAX,
                            })
                        .ok());
    auto split_options = build_split_options(file_path);
    split_options.all_runtime_filters_applied = false;
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    bool eos = false;
    size_t total_rows = 0;
    bool checked_first_batch = false;
    while (!eos) {
        Block block = build_table_block(projected_columns);
        ASSERT_TRUE(reader.get_block(&block, &eos).ok());
        total_rows += block.rows();
        if (!checked_first_batch && block.rows() > 0) {
            const auto& ids =
                    assert_cast<const ColumnInt32&>(expect_not_null_table_column(block, 0));
            ASSERT_EQ(ids.size(), 2);
            EXPECT_EQ(ids.get_element(0), 3);
            EXPECT_EQ(ids.get_element(1), 1);
            checked_first_batch = true;
        }
    }
    // MIN/MAX pushdown would return the two synthetic extrema [1, 5]. Reading the original first
    // row group [3, 1] and all four rows proves a pending RF kept the physical reader active.
    EXPECT_TRUE(checked_first_batch);
    EXPECT_EQ(total_rows, 4);
    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, SlotlessConjunctDisablesAggregatePushdown) {
    std::vector<ColumnDefinition> file_schema;
    file_schema.push_back(make_file_column(0, "id", std::make_shared<DataTypeInt32>()));

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));
    set_name_identifiers(&projected_columns);

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    bool predicate_executed = false;
    auto fake_state = std::make_shared<FakeFileReaderState>();
    fake_state->aggregate_count = 3;
    FakeTableReader reader(file_schema, fake_state);
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .conjuncts = {prepared_conjunct(
                                            &state,
                                            std::make_shared<NonDeterministicPartitionPredicate>(
                                                    &predicate_executed))},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                                    .push_down_agg_type = TPushAggOp::type::COUNT,
                            })
                        .ok());

    SplitReadOptions split_options;
    split_options.current_range.__set_path("fake-table-reader-input");
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    // The slotless predicate cannot become a TableFilter or a file-reader conjunct, but its
    // presence still prevents the fake aggregate count (3) from replacing the two physical rows.
    ASSERT_NE(fake_state->last_request, nullptr);
    EXPECT_TRUE(fake_state->last_request->conjuncts.empty());
    EXPECT_EQ(block.rows(), 2);
    EXPECT_TRUE(predicate_executed);
    ASSERT_TRUE(reader.close().ok());
}

TEST(TableReaderTest, AbortSplitClearsReaderAfterIgnorableNotFound) {
    std::vector<ColumnDefinition> file_schema;
    file_schema.push_back(make_file_column(0, "id", std::make_shared<DataTypeInt32>()));
    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));
    set_name_identifiers(&projected_columns);

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto fake_state = std::make_shared<FakeFileReaderState>();
    fake_state->not_found_during_init = true;
    FakeTableReader reader(file_schema, fake_state);
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

    SplitReadOptions split_options;
    split_options.current_range.__set_path("missing-fake-table-reader-input");
    ASSERT_TRUE(reader.prepare_split(split_options).ok());
    Block block = build_table_block(projected_columns);
    bool eos = false;
    const auto status = reader.get_block(&block, &eos);
    ASSERT_TRUE(status.is<ErrorCode::NOT_FOUND>()) << status;
    ASSERT_TRUE(reader.abort_split().ok());
    EXPECT_EQ(fake_state->init_count, 1);
    EXPECT_EQ(fake_state->close_count, 1);

    fake_state->not_found_during_init = false;
    split_options.current_range.__set_path("existing-fake-table-reader-input");
    ASSERT_TRUE(reader.prepare_split(split_options).ok());
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    EXPECT_EQ(fake_state->init_count, 2);
    EXPECT_EQ(fake_state->close_count, 2);
    ASSERT_TRUE(reader.close().ok());
}

TEST(TableReaderTest, PushDownCountRecordsReaderRowsBeforeClosingReader) {
    std::vector<ColumnDefinition> file_schema;
    file_schema.push_back(make_file_column(0, "id", std::make_shared<DataTypeInt32>()));

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));
    set_name_identifiers(&projected_columns);

    io::FileReaderStats file_reader_stats;
    auto io_ctx = std::make_shared<io::IOContext>();
    io_ctx->file_reader_stats = &file_reader_stats;

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto fake_state = std::make_shared<FakeFileReaderState>();
    fake_state->aggregate_count = 3;
    fake_state->io_ctx = io_ctx;
    FakeTableReader reader(file_schema, fake_state);
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = io_ctx,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                                    .push_down_agg_type = TPushAggOp::type::COUNT,
                            })
                        .ok());

    SplitReadOptions split_options;
    split_options.current_range.__set_path("fake-table-reader-input");
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    EXPECT_FALSE(eos);
    EXPECT_EQ(block.rows(), 3);
    EXPECT_EQ(file_reader_stats.read_rows, 3);
    EXPECT_EQ(fake_state->close_count, 1);
}

TEST(TableReaderTest, PushDownCountStopConvertsAggregateEndOfFileToEos) {
    std::vector<ColumnDefinition> file_schema;
    file_schema.push_back(make_file_column(0, "id", std::make_shared<DataTypeInt32>()));

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));
    set_name_identifiers(&projected_columns);

    auto io_ctx = std::make_shared<io::IOContext>();

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto fake_state = std::make_shared<FakeFileReaderState>();
    fake_state->aggregate_count = 3;
    fake_state->io_ctx = io_ctx;
    fake_state->stop_during_aggregate = true;
    FakeTableReader reader(file_schema, fake_state);
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = io_ctx,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                                    .push_down_agg_type = TPushAggOp::type::COUNT,
                            })
                        .ok());

    SplitReadOptions split_options;
    split_options.current_range.__set_path("fake-table-reader-input");
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    EXPECT_TRUE(eos);
    EXPECT_EQ(block.rows(), 0);
    EXPECT_EQ(fake_state->close_count, 0);
}

TEST(TableReaderTest, DebugStringCoversReaderStateAndEnumNames) {
    std::vector<ColumnDefinition> file_schema;
    file_schema.push_back(make_file_column(0, "id", std::make_shared<DataTypeInt32>()));
    file_schema.push_back(make_file_column(1, "value", std::make_shared<DataTypeString>()));

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));
    projected_columns.push_back(make_table_column(1, "value", std::make_shared<DataTypeString>()));
    projected_columns[0].name_mapping = {"legacy_id"};
    set_name_identifiers(&projected_columns);

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto fake_state = std::make_shared<FakeFileReaderState>();
    fake_state->eof_with_first_batch = false;
    FakeTableReader reader(file_schema, fake_state);
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .conjuncts = {prepared_conjunct(
                                            &state, table_int32_greater_than_expr(0, 0, 0))},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = std::make_shared<io::IOContext>(),
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                                    .push_down_agg_type = TPushAggOp::type::COUNT,
                            })
                        .ok());

    SplitReadOptions split_options;
    split_options.partition_values.emplace("dt", Field::create_field<TYPE_STRING>("2026-06-29"));
    split_options.current_range.__set_path("fake-table-reader-input");
    split_options.current_range.__set_file_size(64);
    split_options.current_range.__set_start_offset(7);
    split_options.current_range.__set_size(11);
    split_options.current_range.__set_modification_time(13);
    split_options.current_range.__set_fs_name("local-fs");
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);

    const auto debug = reader.debug_string();
    EXPECT_NE(debug.find("format=PARQUET"), std::string::npos);
    EXPECT_NE(debug.find("push_down_agg_type=COUNT"), std::string::npos);
    EXPECT_NE(debug.find("current_file=FileDescription{path=fake-table-reader-input"),
              std::string::npos);
    EXPECT_NE(debug.find("partition_values={dt}"), std::string::npos);
    EXPECT_NE(debug.find("table_filters=[TableFilter{conjunct=VExprContext"), std::string::npos);
    EXPECT_NE(debug.find("ColumnDefinition{name=id"), std::string::npos);
    EXPECT_NE(debug.find("name_mapping=[legacy_id]"), std::string::npos);
    EXPECT_NE(debug.find("ColumnMapping{global_index=0"), std::string::npos);
    EXPECT_NE(debug.find("FileBlockColumn{file_column_id=0"), std::string::npos);
    ASSERT_TRUE(reader.close().ok());

    const std::vector<FileFormat> formats {FileFormat::ORC,  FileFormat::CSV, FileFormat::JSON,
                                           FileFormat::TEXT, FileFormat::JNI, FileFormat::NATIVE,
                                           FileFormat::ARROW};
    const std::vector<std::string> format_names {"ORC", "CSV",    "JSON", "TEXT",
                                                 "JNI", "NATIVE", "ARROW"};
    for (size_t idx = 0; idx < formats.size(); ++idx) {
        TableReader enum_reader;
        ASSERT_TRUE(enum_reader
                            .init({
                                    .projected_columns = {},
                                    .conjuncts = {},
                                    .format = formats[idx],
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                            })
                            .ok());
        EXPECT_NE(enum_reader.debug_string().find("format=" + format_names[idx]),
                  std::string::npos);
    }

    const std::vector<TPushAggOp::type> agg_ops {TPushAggOp::type::NONE, TPushAggOp::type::MINMAX,
                                                 TPushAggOp::type::MIX,
                                                 TPushAggOp::type::COUNT_ON_INDEX};
    const std::vector<std::string> agg_names {"NONE", "MINMAX", "MIX", "COUNT_ON_INDEX"};
    for (size_t idx = 0; idx < agg_ops.size(); ++idx) {
        TableReader enum_reader;
        ASSERT_TRUE(enum_reader
                            .init({
                                    .projected_columns = {},
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                                    .push_down_agg_type = agg_ops[idx],
                            })
                            .ok());
        EXPECT_NE(enum_reader.debug_string().find("push_down_agg_type=" + agg_names[idx]),
                  std::string::npos);
    }
}

TEST(TableReaderTest, AnnotateProjectedColumnUsesCurrentHistorySchemaForNestedTypes) {
    TFileScanRangeParams scan_params;
    scan_params.__set_current_schema_id(200);

    auto profile_field = external_struct_field(
            "profile", 20,
            {external_array_field("old_scores", 21, external_schema_field("old_score", 22),
                                  {"scores"}),
             external_map_field("old_props", 23, external_schema_field("old_key", 24),
                                external_schema_field("old_value", 25), {"props"})},
            {"user_profile"});
    scan_params.__set_history_schema_info(
            {external_schema(100, {external_schema_field("ignored_profile", 10)}),
             external_schema(200, {profile_field})});

    const auto int_type = std::make_shared<DataTypeInt32>();
    const auto string_type = std::make_shared<DataTypeString>();
    auto scores_type = std::make_shared<DataTypeArray>(int_type);
    auto props_type = std::make_shared<DataTypeMap>(string_type, string_type);
    auto profile_type = std::make_shared<DataTypeStruct>(DataTypes {scores_type, props_type},
                                                         Strings {"scores", "props"});

    ColumnDefinition profile_column = make_table_column(-1, "user_profile", profile_type);
    ProjectedColumnBuildContext context;
    context.scan_params = &scan_params;
    TFileScanSlotInfo slot_info;
    TableReader reader;
    ASSERT_TRUE(reader.annotate_projected_column(slot_info, &context, &profile_column).ok());

    EXPECT_EQ(profile_column.get_identifier_field_id(), 20);
    EXPECT_EQ(profile_column.name_mapping, std::vector<std::string>({"user_profile"}));
    ASSERT_TRUE(context.schema_column.has_value());
    ASSERT_EQ(context.schema_column->children.size(), 2);
    EXPECT_EQ(context.schema_column->children[0].name, "old_scores");
    EXPECT_EQ(context.schema_column->children[0].get_identifier_field_id(), 21);
    ASSERT_EQ(context.schema_column->children[0].children.size(), 1);
    EXPECT_EQ(context.schema_column->children[0].children[0].name, "element");
    EXPECT_EQ(context.schema_column->children[0].children[0].get_identifier_field_id(), 22);
    ASSERT_EQ(context.schema_column->children[1].children.size(), 2);
    EXPECT_EQ(context.schema_column->children[1].name, "old_props");
    EXPECT_EQ(context.schema_column->children[1].children[0].name, "key");
    EXPECT_EQ(context.schema_column->children[1].children[0].get_identifier_field_id(), 24);
    EXPECT_EQ(context.schema_column->children[1].children[1].name, "value");
    EXPECT_EQ(context.schema_column->children[1].children[1].get_identifier_field_id(), 25);
}

TEST(TableReaderTest, ComplexRematerializeCastsScalarChildToTableType) {
    const auto string_type = std::make_shared<DataTypeString>();
    const auto nullable_string_type = make_nullable(string_type);
    const auto file_struct_type = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {nullable_string_type, string_type}, Strings {"country", "city"}));
    auto file_struct_column = make_file_column(2, "struct_column", file_struct_type);
    file_struct_column.children = {make_file_column(0, "country", nullable_string_type),
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

TEST(TableReaderTest, ComplexRematerializeCastsNonNullableScalarChildWithNullableFileType) {
    const auto int_type = std::make_shared<DataTypeInt32>();
    const auto bigint_type = std::make_shared<DataTypeInt64>();
    const auto nullable_int_type = make_nullable(int_type);
    const auto nullable_bigint_type = make_nullable(bigint_type);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    TableReaderCastTestHelper reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = {},
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                            })
                        .ok());

    auto column = ColumnInt32::create();
    column->insert_value(10);
    column->insert_value(20);
    ColumnPtr result_column = std::move(column);
    const auto status = reader._cast_column_to_type(&result_column, nullable_int_type,
                                                    nullable_bigint_type, "struct_column.a");
    ASSERT_TRUE(status.ok()) << status.to_string();

    const auto& result_nullable = assert_cast<const ColumnNullable&>(*result_column);
    const auto& child_values = assert_cast<const ColumnInt64&>(result_nullable.get_nested_column());
    ASSERT_EQ(result_nullable.size(), 2);
    EXPECT_FALSE(result_nullable.is_null_at(0));
    EXPECT_FALSE(result_nullable.is_null_at(1));
    EXPECT_EQ(child_values.get_element(0), 10);
    EXPECT_EQ(child_values.get_element(1), 20);
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
                assert_cast<const ColumnString&>(expect_not_null_table_column(block, 0));
        const auto& id_column =
                assert_cast<const ColumnInt32&>(expect_not_null_table_column(block, 1));
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

// Scenario: requests without file-local row conjuncts do not produce a row-level survivor bitmap,
// so TableReader must not enable condition cache.
TEST(TableReaderTest, ConditionCacheSkipsRequestWithoutFileLocalConjuncts) {
    std::vector<ColumnDefinition> file_schema;
    file_schema.push_back(make_file_column(0, "id", std::make_shared<DataTypeInt32>()));

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));
    set_name_identifiers(&projected_columns);

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto fake_state = std::make_shared<FakeFileReaderState>();
    FakeTableReader reader(file_schema, fake_state);
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
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

// Scenario: a standalone caller has only the initial digest for stable predicate P, while its
// current conjunct snapshot also contains an RF. Without an explicit split digest, TableReader must
// not store P AND RF under P's stale key.
TEST(TableReaderTest, ConditionCacheSkipsRuntimeFilterWithoutSplitDigest) {
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

// Scenario: FileScannerV2 supplies a non-zero digest computed from the exact Ready RF payload in
// this split. The RF wrapper is no longer a reason to disable condition cache; a MISS context is
// created and can be published under that payload-specific key after EOF.
TEST(TableReaderTest, ConditionCacheAllowsRuntimeFilterCoveredBySplitDigest) {
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
    ASSERT_TRUE(
            reader.init({
                                .projected_columns = projected_columns,
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
    split_options.condition_cache_digest = 11;
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_NE(fake_state->condition_cache_ctx, nullptr);
    EXPECT_FALSE(fake_state->condition_cache_ctx->is_hit);

    segment_v2::ConditionCacheHandle handle;
    segment_v2::ConditionCache::ExternalCacheKey initial_digest_key(
            "fake-table-reader-input", 0, -1, 7, 0, -1,
            segment_v2::ConditionCache::ExternalCacheKey::BASE_GRANULE_AWARE_VERSION);
    EXPECT_FALSE(cache.get()->lookup(initial_digest_key, &handle));
    segment_v2::ConditionCache::ExternalCacheKey split_digest_key(
            "fake-table-reader-input", 0, -1, 11, 0, -1,
            segment_v2::ConditionCache::ExternalCacheKey::BASE_GRANULE_AWARE_VERSION);
    EXPECT_TRUE(cache.get()->lookup(split_digest_key, &handle));
    ASSERT_TRUE(reader.close().ok());
}

// Scenario: table-format delete files/deletion vectors are outside the data-file cache key. When
// TableReader injects delete conjuncts into the file scan request, condition cache must be disabled
// for that split.
TEST(TableReaderTest, ConditionCacheSkipsRequestWithDeleteConjuncts) {
    std::vector<ColumnDefinition> file_schema;
    file_schema.push_back(make_file_column(0, "id", std::make_shared<DataTypeInt32>()));

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));
    set_name_identifiers(&projected_columns);

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto fake_state = std::make_shared<FakeFileReaderState>();
    fake_state->inject_delete_conjunct = true;
    FakeTableReader reader(file_schema, fake_state);
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
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
    fake_state->condition_cache_base_granule = 7;
    fake_state->condition_cache_num_granules = 1;
    FakeTableReader reader(file_schema, fake_state);
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
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

    segment_v2::ConditionCache::ExternalCacheKey legacy_key("fake-table-reader-input", 0, -1, 7, 0,
                                                            -1);
    segment_v2::ConditionCacheHandle handle;
    EXPECT_FALSE(cache.get()->lookup(legacy_key, &handle));
    segment_v2::ConditionCache::ExternalCacheKey key(
            "fake-table-reader-input", 0, -1, 7, 0, -1,
            segment_v2::ConditionCache::ExternalCacheKey::BASE_GRANULE_AWARE_VERSION);
    ASSERT_TRUE(cache.get()->lookup(key, &handle));
    const auto cached_bitmap = handle.get_filter_result();
    ASSERT_NE(cached_bitmap, nullptr);
    ASSERT_FALSE(cached_bitmap->empty());
    EXPECT_EQ(cached_bitmap->size(), 1);
    EXPECT_TRUE((*cached_bitmap)[0]);
    EXPECT_EQ(handle.get_base_granule(), 7);

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
    segment_v2::ConditionCache::ExternalCacheKey key(
            "fake-table-reader-input", 0, -1, 7, 0, -1,
            segment_v2::ConditionCache::ExternalCacheKey::BASE_GRANULE_AWARE_VERSION);
    segment_v2::ConditionCacheHandle handle;
    EXPECT_FALSE(cache.get()->lookup(key, &handle));
}

// Scenario: a stop request can arrive while a physical read is in progress. Even if the reader
// converts that stop into eof=true, TableReader must not publish the partially visited MISS bitmap.
TEST(TableReaderTest, ConditionCacheMissIsDroppedWhenStopTurnsReadIntoEof) {
    ScopedConditionCacheForTest cache;

    std::vector<ColumnDefinition> file_schema;
    file_schema.push_back(make_file_column(0, "id", std::make_shared<DataTypeInt32>()));

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));
    set_name_identifiers(&projected_columns);

    auto io_ctx = std::make_shared<io::IOContext>();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    auto fake_state = std::make_shared<FakeFileReaderState>();
    fake_state->total_rows = ConditionCacheContext::GRANULE_SIZE * 2;
    fake_state->condition_cache_num_granules = 2;
    fake_state->stop_during_read = true;
    fake_state->io_ctx = io_ctx;
    FakeTableReader reader(file_schema, fake_state);
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .conjuncts = {prepared_conjunct(
                                            &state, table_int32_greater_than_expr(0, 0, 0))},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = io_ctx,
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
    EXPECT_TRUE(io_ctx->should_stop);
    EXPECT_EQ(block.rows(), 2);
    ASSERT_NE(fake_state->condition_cache_ctx, nullptr);
    EXPECT_FALSE(fake_state->condition_cache_ctx->is_hit);

    segment_v2::ConditionCache::ExternalCacheKey key(
            "fake-table-reader-input", 0, -1, 7, 0, -1,
            segment_v2::ConditionCache::ExternalCacheKey::BASE_GRANULE_AWARE_VERSION);
    segment_v2::ConditionCacheHandle handle;
    EXPECT_FALSE(cache.get()->lookup(key, &handle));

    ASSERT_TRUE(reader.close().ok());
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
    const auto& id_column = assert_cast<const ColumnInt32&>(expect_not_null_table_column(block, 0));
    const auto& score_column =
            assert_cast<const ColumnInt32&>(expect_not_null_table_column(block, 1));
    EXPECT_EQ(id_column.get_element(0), 1);
    EXPECT_EQ(id_column.get_element(1), 5);
    EXPECT_EQ(score_column.get_element(0), 10);
    EXPECT_EQ(score_column.get_element(1), 50);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, PushDownMinMaxFallsBackForFileToTableCast) {
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
    const auto& id_column = assert_cast<const ColumnInt64&>(expect_not_null_table_column(block, 0));
    EXPECT_EQ(id_column.get_element(0), 3);
    EXPECT_EQ(id_column.get_element(1), 1);

    block = build_table_block(projected_columns);
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);
    ASSERT_EQ(block.rows(), 2);
    const auto& second_id_column =
            assert_cast<const ColumnInt64&>(expect_not_null_table_column(block, 0));
    EXPECT_EQ(second_id_column.get_element(0), 5);
    EXPECT_EQ(second_id_column.get_element(1), 2);

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
    const auto& struct_result =
            assert_cast<const ColumnStruct&>(expect_not_null_table_column(block, 0));
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
    const auto& array_result =
            assert_cast<const ColumnArray&>(expect_not_null_table_column(block, 0));
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
    const auto& array_result =
            assert_cast<const ColumnArray&>(expect_not_null_table_column(block, 0));
    EXPECT_EQ(array_result.get_offsets()[0], 2);
    EXPECT_EQ(array_result.get_offsets()[1], 3);
    EXPECT_EQ(array_result.get_offsets()[2], 4);
    const auto& nullable_elements = assert_cast<const ColumnNullable&>(array_result.get_data());
    const auto& element_struct =
            assert_cast<const ColumnStruct&>(nullable_elements.get_nested_column());
    ASSERT_EQ(element_struct.get_columns().size(), 1);
    const auto& a_values = assert_cast<const ColumnInt32&>(
            expect_not_null_nullable_nested_column(element_struct.get_column(0)));
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
    const auto& array_result =
            assert_cast<const ColumnArray&>(expect_not_null_table_column(block, 0));
    EXPECT_EQ(array_result.get_offsets()[0], 2);
    EXPECT_EQ(array_result.get_offsets()[1], 3);
    EXPECT_EQ(array_result.get_offsets()[2], 4);
    const auto& nullable_elements = assert_cast<const ColumnNullable&>(array_result.get_data());
    const auto& element_struct =
            assert_cast<const ColumnStruct&>(nullable_elements.get_nested_column());
    ASSERT_EQ(element_struct.get_columns().size(), 3);
    const auto& b_values = assert_cast<const ColumnInt32&>(
            expect_not_null_nullable_nested_column(element_struct.get_column(0)));
    const auto& missing_values = element_struct.get_column(1);
    const auto& a_values = assert_cast<const ColumnInt32&>(
            expect_not_null_nullable_nested_column(element_struct.get_column(2)));
    EXPECT_EQ(b_values.get_element(0), 11);
    EXPECT_EQ(b_values.get_element(1), 21);
    EXPECT_EQ(b_values.get_element(2), 31);
    EXPECT_EQ(b_values.get_element(3), 41);
    expect_nullable_column_all_null(missing_values);
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
    const auto& array_result =
            assert_cast<const ColumnArray&>(expect_not_null_table_column(block, 0));
    EXPECT_EQ(array_result.get_offsets()[0], 2);
    EXPECT_EQ(array_result.get_offsets()[1], 3);
    EXPECT_EQ(array_result.get_offsets()[2], 4);
    const auto& nullable_elements = assert_cast<const ColumnNullable&>(array_result.get_data());
    const auto& element_struct =
            assert_cast<const ColumnStruct&>(nullable_elements.get_nested_column());
    ASSERT_EQ(element_struct.get_columns().size(), 1);
    expect_nullable_column_all_null(element_struct.get_column(0));

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
    const auto& map_result = assert_cast<const ColumnMap&>(expect_not_null_table_column(block, 0));
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
    const auto& map_result = assert_cast<const ColumnMap&>(expect_not_null_table_column(block, 0));
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
    const auto& missing_values = value_struct.get_column(1);
    const auto& a_values = assert_cast<const ColumnInt32&>(
            expect_not_null_nullable_nested_column(value_struct.get_column(2)));
    EXPECT_EQ(b_values.get_data_at(0).to_string(), "ma");
    EXPECT_EQ(b_values.get_data_at(1).to_string(), "mb");
    EXPECT_EQ(b_values.get_data_at(2).to_string(), "mc");
    expect_nullable_column_all_null(missing_values);
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
    const auto& id_column = assert_cast<const ColumnInt32&>(expect_not_null_table_column(block, 0));
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
    const auto& id_column = assert_cast<const ColumnInt32&>(expect_not_null_table_column(block, 0));
    EXPECT_EQ(id_column.get_element(0), 3);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, PushDownCountFallsBackWithFilter) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_table_reader_count_predicate_test";
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
    const auto& id_column = assert_cast<const ColumnInt32&>(expect_not_null_table_column(block, 0));
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
    expect_nullable_column_all_null(*block.get_by_position(0).column);

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
    const auto& id_column = assert_cast<const ColumnInt32&>(expect_not_null_table_column(block, 1));
    ASSERT_EQ(id_column.size(), 1);
    EXPECT_EQ(id_column.get_element(0), 3);

    ASSERT_TRUE(reader.close().ok());

    TableReader filtered_reader;
    ASSERT_TRUE(filtered_reader
                        .init({
                                .projected_columns = projected_columns,
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

TEST(TableReaderTest, OpenReaderPushesVExprPredicateToParquetReader) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_table_reader_column_predicate_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    // Keep one row per row group so the VExpr ZoneMap path can prune the first two row groups and
    // leave only id = 3.
    write_int_pair_parquet_file(file_path, {1, 2, 3}, {1, 5, 8}, {"one", "two", "three"}, 1);

    std::vector<ColumnDefinition> projected_columns;
    projected_columns.push_back(make_table_column(2, "value", std::make_shared<DataTypeString>()));
    projected_columns.push_back(make_table_column(0, "id", std::make_shared<DataTypeInt32>()));

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    set_name_identifiers(&projected_columns);
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
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

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);

    const auto& value_column =
            assert_cast<const ColumnString&>(expect_not_null_table_column(block, 0));
    const auto& id_column = assert_cast<const ColumnInt32&>(expect_not_null_table_column(block, 1));
    ASSERT_EQ(id_column.size(), 1);
    ASSERT_EQ(value_column.size(), 1);
    EXPECT_EQ(id_column.get_element(0), 3);
    EXPECT_EQ(value_column.get_data_at(0).to_string(), "three");

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, VExprPredicateSurvivesReopenSplit) {
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

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    set_name_identifiers(&projected_columns);
    TableReader reader;
    ASSERT_TRUE(reader.init({
                                    .projected_columns = projected_columns,
                                    .conjuncts = {prepared_conjunct(
                                            &state, table_int32_greater_than_expr(0, 0, 2))},
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
        const auto& id_column =
                assert_cast<const ColumnInt32&>(expect_not_null_table_column(block, 0));
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
            .conjunct =
                    VExprContext::create_shared(table_int32_sum_greater_than_expr(0, 0, 2, 2, 1)),
            .global_indices = {GlobalIndex(0), GlobalIndex(2)},
    });

    FileScanRequest file_request;
    ASSERT_TRUE(mapper.create_scan_request(table_filters, projected_columns, &file_request).ok());

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
    ASSERT_TRUE(mapper.create_scan_request({table_filter}, projected_columns, &file_request).ok());

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
    ASSERT_TRUE(mapper.create_scan_request({table_filter}, projected_columns, &file_request).ok());

    EXPECT_EQ(projection_ids(file_request.predicate_columns), std::vector<int32_t>({0}));
    EXPECT_EQ(projection_ids(file_request.non_predicate_columns), std::vector<int32_t>({1}));
    ASSERT_EQ(file_request.conjuncts.size(), 1);
    const auto* localized_slot =
            assert_cast<const VSlotRef*>(file_request.conjuncts[0]->root()->children()[0].get());
    EXPECT_EQ(localized_slot->slot_id(), 0);
    EXPECT_EQ(localized_slot->column_id(), 1);
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

    const auto& value_column =
            assert_cast<const ColumnString&>(expect_not_null_table_column(block, 0));
    const auto& id_column = assert_cast<const ColumnInt32&>(expect_not_null_table_column(block, 1));
    const auto& score_column =
            assert_cast<const ColumnInt32&>(expect_not_null_table_column(block, 2));
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
    // A missing scalar column without an explicit default is materialized as a default-value
    // column. It may stay constant, so verify through the IColumn interface instead of assuming a
    // concrete ColumnString instance.
    ASSERT_EQ(result.column->size(), 1);
    EXPECT_EQ(result.column->get_data_at(0).to_string(), "");

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

    const auto& struct_result =
            assert_cast<const ColumnStruct&>(expect_not_null_table_column(block, 0));
    ASSERT_EQ(struct_result.get_columns().size(), 2);
    const auto& ids = assert_cast<const ColumnInt32&>(
            expect_not_null_nullable_nested_column(struct_result.get_column(0)));
    ASSERT_EQ(struct_result.size(), 1);
    EXPECT_EQ(ids.get_element(0), 7);
    expect_nullable_column_all_null(struct_result.get_column(1));

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
    const auto& struct_result =
            assert_cast<const ColumnStruct&>(expect_not_null_table_column(block, 0));
    const auto& notes = assert_cast<const ColumnNullable&>(struct_result.get_column(1));
    EXPECT_FALSE(notes.is_null_at(0));
    EXPECT_TRUE(notes.is_null_at(1));

    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    EXPECT_TRUE(eos);
    EXPECT_EQ(block.rows(), 0);

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, ProjectedRenamedStructPreservesParentDeclaredChildNullability) {
    const auto test_dir = std::filesystem::temp_directory_path() /
                          "doris_table_reader_struct_parent_child_nullability_test";
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);

    const auto file_path = (test_dir / "split.parquet").string();
    write_struct_with_nullable_child_parquet_file(file_path);

    const auto string_type = std::make_shared<DataTypeString>();
    const auto nullable_string_type = make_nullable(string_type);
    auto renamed_note = make_table_column(1, "renamed_note", nullable_string_type);
    renamed_note.name_mapping = {"note"};
    // Iceberg nested schema metadata can omit nullability on this child while the parent DataType
    // remains authoritative and declares it nullable.
    renamed_note.type = string_type;
    auto struct_type = std::make_shared<DataTypeStruct>(DataTypes {nullable_string_type},
                                                        Strings {"renamed_note"});
    auto struct_column = make_table_column(100, "s", struct_type);
    struct_column.children = {renamed_note};
    std::vector<ColumnDefinition> projected_columns = {struct_column};

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    set_name_identifiers(&projected_columns);
    TableReader reader;
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
    const auto status = reader.get_block(&block, &eos);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_FALSE(eos);

    const auto& struct_result =
            assert_cast<const ColumnStruct&>(expect_not_null_table_column(block, 0));
    ASSERT_EQ(struct_result.get_columns().size(), 1);
    const auto& notes = assert_cast<const ColumnNullable&>(struct_result.get_column(0));
    EXPECT_FALSE(notes.is_null_at(0));
    EXPECT_EQ(notes.get_data_at(0).to_string(), "seven");
    EXPECT_TRUE(notes.is_null_at(1));
    ASSERT_TRUE(block.get_by_position(0).check_type_and_column_match().ok());

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
    const auto& partition_value_data = assert_cast<const ColumnString&>(
            expect_not_null_nullable_nested_column(*partition_value));
    ASSERT_EQ(partition_value_data.size(), 1);
    EXPECT_EQ(partition_value_data.get_data_at(0).to_string(), "p1");

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

TEST(TableReaderTest, ProjectedNullPartitionColumnPreservesNull) {
    const auto test_dir =
            std::filesystem::temp_directory_path() / "doris_table_reader_null_partition_value_test";
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
                                    .conjuncts = {},
                                    .format = FileFormat::PARQUET,
                                    .scan_params = nullptr,
                                    .io_ctx = nullptr,
                                    .runtime_state = &state,
                                    .scanner_profile = nullptr,
                            })
                        .ok());

    auto split_options = build_split_options(file_path);
    split_options.partition_values.emplace("value", Field::create_field<TYPE_NULL>(Null()));
    ASSERT_TRUE(reader.prepare_split(split_options).ok());

    Block block = build_table_block(projected_columns);
    bool eos = false;
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    ASSERT_FALSE(eos);

    expect_nullable_column_all_null(*block.get_by_position(0).column);

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
                                    .conjuncts = {prepared_conjunct(
                                            &state, table_int32_greater_than_expr(0, 0, 1))},
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

    const auto& id_column = assert_cast<const ColumnInt32&>(expect_not_null_table_column(block, 0));
    const auto& value_column =
            assert_cast<const ColumnString&>(expect_not_null_table_column(block, 1));
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

    const auto& id_column = assert_cast<const ColumnInt32&>(expect_not_null_table_column(block, 0));
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
    const auto& id_column = assert_cast<const ColumnInt64&>(expect_not_null_table_column(block, 0));
    const auto& value_column =
            assert_cast<const ColumnString&>(expect_not_null_table_column(block, 1));
    ASSERT_EQ(id_column.size(), 1);
    ASSERT_EQ(value_column.size(), 1);
    EXPECT_EQ(id_column.get_element(0), 7);
    EXPECT_EQ(value_column.get_data_at(0).to_string(), "seven");

    ASSERT_TRUE(reader.close().ok());
    std::filesystem::remove_all(test_dir);
}

} // namespace
} // namespace doris::format
