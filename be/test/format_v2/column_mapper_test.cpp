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

#include "format_v2/column_mapper.h"

#include <gtest/gtest.h>

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/consts.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_timestamptz.h"
#include "core/data_type/data_type_varbinary.h"
#include "exprs/vectorized_fn_call.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "exprs/vin_predicate.h"
#include "exprs/vliteral.h"
#include "exprs/vslot_ref.h"
#include "format_v2/column_mapper_nested.h"
#include "format_v2/expr/cast.h"
#include "format_v2/schema_projection.h"
#include "format_v2/table_reader.h"
#include "gen_cpp/Exprs_types.h"
#include "runtime/descriptors.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris::format {
namespace {

DataTypePtr i32() {
    return std::make_shared<DataTypeInt32>();
}

DataTypePtr i64() {
    return std::make_shared<DataTypeInt64>();
}

DataTypePtr f32() {
    return std::make_shared<DataTypeFloat32>();
}

DataTypePtr f64() {
    return std::make_shared<DataTypeFloat64>();
}

DataTypePtr dec32(uint32_t precision, uint32_t scale) {
    return std::make_shared<DataTypeDecimal32>(precision, scale);
}

DataTypePtr str() {
    return std::make_shared<DataTypeString>();
}

DataTypePtr varbinary() {
    return std::make_shared<DataTypeVarbinary>();
}

DataTypePtr timestamptz(uint32_t scale) {
    return std::make_shared<DataTypeTimeStampTz>(scale);
}

DataTypePtr u8() {
    return std::make_shared<DataTypeUInt8>();
}

ColumnDefinition field_id_col(const std::string& name, int32_t field_id, DataTypePtr type,
                              int32_t local_id = -1) {
    ColumnDefinition column;
    column.identifier = Field::create_field<TYPE_INT>(field_id);
    column.local_id = local_id;
    column.name = name;
    column.type = std::move(type);
    return column;
}

ColumnDefinition name_col(const std::string& name, DataTypePtr type, int32_t local_id = -1) {
    ColumnDefinition column;
    column.identifier = Field::create_field<TYPE_STRING>(name);
    column.local_id = local_id;
    column.name = name;
    column.type = std::move(type);
    return column;
}

ColumnDefinition name_id_col(const std::string& name, const std::string& identifier,
                             DataTypePtr type, int32_t local_id = -1) {
    ColumnDefinition column = name_col(name, std::move(type), local_id);
    column.identifier = Field::create_field<TYPE_STRING>(identifier);
    return column;
}

ColumnDefinition position_col(const std::string& name, int32_t file_position, DataTypePtr type) {
    return field_id_col(name, file_position, std::move(type));
}

ColumnDefinition struct_col(const std::string& name, int32_t field_id,
                            std::vector<ColumnDefinition> children, int32_t local_id = -1) {
    DataTypes child_types;
    Strings child_names;
    child_types.reserve(children.size());
    child_names.reserve(children.size());
    for (const auto& child : children) {
        child_types.push_back(child.type);
        child_names.push_back(child.name);
    }
    auto column = field_id_col(
            name, field_id, std::make_shared<DataTypeStruct>(child_types, child_names), local_id);
    column.children = std::move(children);
    return column;
}

ColumnDefinition struct_name_col(const std::string& name, std::vector<ColumnDefinition> children,
                                 int32_t local_id = -1) {
    auto column = struct_col(name, -1, std::move(children), local_id);
    column.identifier = Field::create_field<TYPE_STRING>(name);
    return column;
}

ColumnDefinition array_col(const std::string& name, int32_t field_id, ColumnDefinition element,
                           int32_t local_id = -1) {
    auto column =
            field_id_col(name, field_id, std::make_shared<DataTypeArray>(element.type), local_id);
    column.children = {std::move(element)};
    return column;
}

ColumnDefinition map_col(const std::string& name, int32_t field_id,
                         std::vector<ColumnDefinition> children, const DataTypePtr& key_type,
                         const DataTypePtr& value_type, int32_t local_id = -1) {
    auto column = field_id_col(name, field_id, std::make_shared<DataTypeMap>(key_type, value_type),
                               local_id);
    column.children = std::move(children);
    return column;
}

void set_name_identifiers(ColumnDefinition* column, int32_t local_id) {
    DORIS_CHECK(column != nullptr);
    column->identifier = Field::create_field<TYPE_STRING>(column->name);
    column->local_id = local_id;
    for (size_t idx = 0; idx < column->children.size(); ++idx) {
        set_name_identifiers(&column->children[idx], static_cast<int32_t>(idx));
    }
}

std::vector<int32_t> projection_ids(const std::vector<LocalColumnIndex>& projections) {
    std::vector<int32_t> ids;
    ids.reserve(projections.size());
    for (const auto& projection : projections) {
        ids.push_back(projection.local_id());
    }
    return ids;
}

TEST(ColumnMapperDebugTest, CoversDebugStringEnumAndNestedBranches) {
    ColumnDefinition child = field_id_col("child", 2, str(), 3);
    child.name_mapping = {"legacy_child"};

    ColumnDefinition column = field_id_col(
            "root", 1,
            std::make_shared<DataTypeStruct>(DataTypes {child.type}, Strings {child.name}));
    column.name_mapping = {"legacy_root"};
    column.children = {child};
    column.default_expr = VExprContext::create_shared(VLiteral::create_shared(
            std::make_shared<DataTypeString>(), Field::create_field<TYPE_STRING>("fallback")));
    column.is_partition_key = true;

    const auto column_debug = column.debug_string();
    EXPECT_NE(column_debug.find("ColumnDefinition{name=root"), std::string::npos);
    EXPECT_NE(column_debug.find("name_mapping=[legacy_root]"), std::string::npos);
    EXPECT_NE(column_debug.find("children=[ColumnDefinition{name=child"), std::string::npos);
    EXPECT_NE(column_debug.find("has_default_expr=1"), std::string::npos);
    EXPECT_NE(column_debug.find("is_partition_key=1"), std::string::npos);

    LocalColumnIndex projection = LocalColumnIndex::partial_local(4);
    projection.children.push_back(LocalColumnIndex::local(7));
    EXPECT_NE(projection.debug_string().find("children=[LocalColumnIndex{index=7"),
              std::string::npos);

    const std::vector<TableColumnMappingMode> modes {TableColumnMappingMode::BY_FIELD_ID,
                                                     TableColumnMappingMode::BY_NAME,
                                                     TableColumnMappingMode::BY_INDEX};
    const std::vector<std::string> mode_names {"BY_FIELD_ID", "BY_NAME", "BY_INDEX"};
    for (size_t idx = 0; idx < modes.size(); ++idx) {
        TableColumnMapperOptions options {.mode = modes[idx]};
        EXPECT_NE(options.debug_string().find(mode_names[idx]), std::string::npos);
    }

    const std::vector<FilterConversionType> conversions {
            FilterConversionType::COPY_DIRECTLY, FilterConversionType::CAST_FILTER,
            FilterConversionType::READER_EXPRESSION, FilterConversionType::FINALIZE_ONLY,
            FilterConversionType::CONSTANT};
    const std::vector<std::string> conversion_names {
            "COPY_DIRECTLY", "CAST_FILTER", "READER_EXPRESSION", "FINALIZE_ONLY", "CONSTANT"};
    for (size_t idx = 0; idx < conversions.size(); ++idx) {
        ColumnMapping mapping;
        mapping.global_index = GlobalIndex(idx);
        mapping.table_column_name = "table_col";
        mapping.file_local_id = 8;
        mapping.constant_index = ConstantIndex(9);
        mapping.file_column_name = "file_col";
        mapping.original_file_type = str();
        mapping.original_file_children = {child};
        mapping.file_type = str();
        mapping.table_type = str();
        mapping.is_trivial = idx % 2 == 0;
        mapping.filter_conversion = conversions[idx];
        mapping.virtual_column_type = static_cast<TableVirtualColumnType>(
                idx % (TableVirtualColumnType::ICEBERG_ROWID + 1));
        mapping.default_expr = column.default_expr;

        ColumnMapping child_mapping;
        child_mapping.global_index = GlobalIndex(10 + idx);
        child_mapping.table_column_name = "child_col";
        child_mapping.file_column_name = "child_file";
        child_mapping.file_type = i32();
        child_mapping.table_type = i32();
        mapping.child_mappings.push_back(std::move(child_mapping));

        const auto debug = mapping.debug_string();
        EXPECT_NE(debug.find("file_local_id=8"), std::string::npos);
        EXPECT_NE(debug.find("constant_index=9"), std::string::npos);
        EXPECT_NE(debug.find(conversion_names[idx]), std::string::npos);
        EXPECT_NE(debug.find("child_mappings=[ColumnMapping{global_index="), std::string::npos);
        EXPECT_NE(debug.find("has_default_expr=1"), std::string::npos);
    }
}

TEST(ColumnMapperTest, ParquetRetainsIdlessComplexWrapperWithNestedFieldId) {
    auto table_child = field_id_col("a", 1, i32());
    auto table_struct = struct_col("s", 10, {table_child});
    auto file_child = field_id_col("legacy_a", 1, i32(), 0);
    auto file_struct = struct_name_col("s", {file_child}, 0);

    TableColumnMapper parquet_mapper({.mode = TableColumnMappingMode::BY_FIELD_ID,
                                      .allow_idless_complex_wrapper_projection = true});
    ASSERT_TRUE(parquet_mapper.create_mapping({table_struct}, {}, {file_struct}).ok());
    ASSERT_EQ(parquet_mapper.mappings().size(), 1);
    ASSERT_TRUE(parquet_mapper.mappings()[0].file_local_id.has_value());
    EXPECT_EQ(*parquet_mapper.mappings()[0].file_local_id, 0);
    ASSERT_EQ(parquet_mapper.mappings()[0].child_mappings.size(), 1);
    ASSERT_TRUE(parquet_mapper.mappings()[0].child_mappings[0].file_local_id.has_value());
    EXPECT_EQ(*parquet_mapper.mappings()[0].child_mappings[0].file_local_id, 0);

    TableColumnMapper orc_mapper({.mode = TableColumnMappingMode::BY_FIELD_ID});
    ASSERT_TRUE(orc_mapper.create_mapping({table_struct}, {}, {file_struct}).ok());
    EXPECT_FALSE(orc_mapper.mappings()[0].file_local_id.has_value());
}

TEST(ColumnMapperTest, ParquetDescendantIdRetainsWrapperWithAuthoritativeEmptyMapping) {
    auto table_struct = struct_col("s", 10, {field_id_col("a", 2, i32())});
    table_struct.has_name_mapping = true;
    auto file_struct = struct_name_col("s", {field_id_col("a", 2, i32(), 0)}, 0);

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_FIELD_ID,
                              .allow_idless_complex_wrapper_projection = true});
    ASSERT_TRUE(mapper.create_mapping({table_struct}, {}, {file_struct}).ok());

    ASSERT_EQ(mapper.mappings().size(), 1);
    ASSERT_TRUE(mapper.mappings()[0].file_local_id.has_value());
    EXPECT_EQ(*mapper.mappings()[0].file_local_id, 0);
    ASSERT_EQ(mapper.mappings()[0].child_mappings.size(), 1);
    EXPECT_TRUE(mapper.mappings()[0].child_mappings[0].file_local_id.has_value());
}

TEST(ColumnMapperTest, ParquetRetainsRecursiveIdlessWrapperWithNestedFieldId) {
    auto table_inner = struct_col("inner", 20, {field_id_col("leaf", 30, i32())});
    auto table_outer = struct_col("outer", 10, {table_inner});
    auto file_inner = struct_name_col("inner", {field_id_col("leaf", 30, i32(), 0)}, 0);
    auto file_outer = struct_col("outer", 10, {file_inner}, 0);

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_FIELD_ID,
                              .allow_idless_complex_wrapper_projection = true});
    ASSERT_TRUE(mapper.create_mapping({table_outer}, {}, {file_outer}).ok());

    const auto& outer_mapping = mapper.mappings()[0];
    ASSERT_TRUE(outer_mapping.file_local_id.has_value());
    ASSERT_EQ(outer_mapping.child_mappings.size(), 1);
    const auto& inner_mapping = outer_mapping.child_mappings[0];
    ASSERT_TRUE(inner_mapping.file_local_id.has_value());
    ASSERT_EQ(inner_mapping.child_mappings.size(), 1);
    EXPECT_TRUE(inner_mapping.child_mappings[0].file_local_id.has_value());
}

TEST(ColumnMapperTest, MissingNestedChildRetainsBinaryInitialDefault) {
    auto defaulted_child = field_id_col("data", 2, varbinary());
    defaulted_child.initial_default_value = "Ej5FZ+ibEtOkVkJmFBdAAA==";
    defaulted_child.initial_default_value_is_base64 = true;
    auto table_struct = struct_col("s", 10, {field_id_col("a", 1, i32()), defaulted_child});
    auto file_struct = struct_col("s", 10, {field_id_col("a", 1, i32(), 0)}, 0);

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_FIELD_ID});
    ASSERT_TRUE(mapper.create_mapping({table_struct}, {}, {file_struct}).ok());
    ASSERT_EQ(mapper.mappings()[0].child_mappings.size(), 2);
    const auto& missing = mapper.mappings()[0].child_mappings[1];
    ASSERT_TRUE(missing.initial_default_column);
    Field value;
    missing.initial_default_column->get(0, value);
    EXPECT_EQ(value.get_type(), TYPE_VARBINARY);
    EXPECT_EQ(std::string(value.get<TYPE_VARBINARY>()),
              std::string("\x12\x3e\x45\x67\xe8\x9b\x12\xd3\xa4\x56\x42\x66\x14\x17\x40\x00", 16));
}

void expect_mapping(const ColumnMapping& mapping, size_t global_index,
                    const std::string& table_name, int32_t file_local_id,
                    const std::string& file_name, const DataTypePtr& file_type,
                    const DataTypePtr& table_type) {
    EXPECT_EQ(mapping.global_index, GlobalIndex(global_index));
    EXPECT_EQ(mapping.table_column_name, table_name);
    ASSERT_TRUE(mapping.file_local_id.has_value());
    EXPECT_EQ(*mapping.file_local_id, file_local_id);
    EXPECT_EQ(mapping.file_column_name, file_name);
    ASSERT_NE(mapping.file_type, nullptr);
    ASSERT_NE(mapping.table_type, nullptr);
    EXPECT_TRUE(mapping.file_type->equals(*file_type));
    EXPECT_TRUE(mapping.table_type->equals(*table_type));
}

void expect_constant(const TableColumnMapper& mapper, const ColumnMapping& mapping,
                     size_t global_index, const DataTypePtr& table_type) {
    EXPECT_FALSE(mapping.file_local_id.has_value());
    ASSERT_TRUE(mapping.constant_index.has_value());
    ASSERT_LT(mapping.constant_index->value(), mapper.constant_map().size());
    const auto& entry = mapper.constant_map().get(*mapping.constant_index);
    EXPECT_EQ(entry.global_index, GlobalIndex(global_index));
    EXPECT_TRUE(entry.type->equals(*table_type));
    EXPECT_EQ(entry.expr, mapping.default_expr);
}

void expect_missing(const ColumnMapping& mapping) {
    EXPECT_FALSE(mapping.file_local_id.has_value());
    EXPECT_FALSE(mapping.constant_index.has_value());
    EXPECT_EQ(mapping.virtual_column_type, TableVirtualColumnType::INVALID);
}

class TestFunctionExpr final : public VExpr {
public:
    TestFunctionExpr(std::string function_name, DataTypePtr data_type,
                     TExprNodeType::type node_type = TExprNodeType::FUNCTION_CALL,
                     TExprOpcode::type opcode = TExprOpcode::INVALID_OPCODE)
            : VExpr(std::move(data_type), false), _expr_name(std::move(function_name)) {
        set_node_type(node_type);
        _opcode = opcode;
        TFunctionName fn_name;
        fn_name.__set_function_name(_expr_name);
        _fn.__set_name(fn_name);
    }

    const std::string& expr_name() const override { return _expr_name; }

    Status clone_node(VExprSPtr* cloned_expr) const override {
        DORIS_CHECK(cloned_expr != nullptr);
        *cloned_expr =
                std::make_shared<TestFunctionExpr>(_expr_name, data_type(), node_type(), _opcode);
        return Status::OK();
    }

    Status execute_column_impl(VExprContext*, const Block*, const Selector*, size_t,
                               ColumnPtr&) const override {
        return Status::NotSupported("TestFunctionExpr is only used for ColumnMapper analysis");
    }

private:
    std::string _expr_name;
};

class ExecutableStructElementExpr final : public VExpr {
public:
    explicit ExecutableStructElementExpr(DataTypePtr child_type)
            : VExpr(std::move(child_type), false) {
        set_node_type(TExprNodeType::FUNCTION_CALL);
        TFunctionName fn_name;
        fn_name.__set_function_name(_expr_name);
        _fn.__set_name(fn_name);
    }

    const std::string& expr_name() const override { return _expr_name; }

    Status clone_node(VExprSPtr* cloned_expr) const override {
        DORIS_CHECK(cloned_expr != nullptr);
        *cloned_expr = std::make_shared<ExecutableStructElementExpr>(data_type());
        return Status::OK();
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        ColumnPtr struct_column;
        RETURN_IF_ERROR(
                get_child(0)->execute_column(context, block, selector, count, struct_column));
        const auto& input = assert_cast<const ColumnStruct&>(*struct_column);
        result_column = input.get_column_ptr(0);
        return Status::OK();
    }

private:
    const std::string _expr_name = "element_at";
};

VExprSPtr table_slot(int slot_id, int column_id, DataTypePtr type, const std::string& name) {
    return VSlotRef::create_shared(slot_id, column_id, -1, std::move(type), name);
}

VExprSPtr literal(DataTypePtr type, Field value) {
    return VLiteral::create_shared(std::move(type), std::move(value));
}

VExprSPtr struct_element(const VExprSPtr& parent, DataTypePtr child_type,
                         const std::string& child_name) {
    auto expr = std::make_shared<TestFunctionExpr>("struct_element", child_type);
    expr->add_child(parent);
    expr->add_child(literal(str(), Field::create_field<TYPE_STRING>(child_name)));
    return expr;
}

VExprSPtr element_at(const VExprSPtr& parent, DataTypePtr child_type,
                     const std::string& child_name) {
    auto expr = std::make_shared<TestFunctionExpr>("element_at", std::move(child_type));
    expr->add_child(parent);
    expr->add_child(literal(str(), Field::create_field<TYPE_STRING>(child_name)));
    return expr;
}

VExprSPtr executable_struct_element(const VExprSPtr& parent, DataTypePtr child_type,
                                    const std::string& child_name) {
    auto expr = std::make_shared<ExecutableStructElementExpr>(std::move(child_type));
    expr->add_child(parent);
    expr->add_child(literal(str(), Field::create_field<TYPE_STRING>(child_name)));
    return expr;
}

VExprSPtr executable_binary_predicate(TExprOpcode::type opcode, const VExprSPtr& left,
                                      const VExprSPtr& right) {
    const auto result_type = u8();
    TFunctionName fn_name;
    fn_name.__set_function_name(opcode == TExprOpcode::GT ? "gt" : "eq");
    TFunction fn;
    fn.__set_name(fn_name);
    fn.__set_binary_type(TFunctionBinaryType::BUILTIN);
    fn.__set_arg_types({left->data_type()->to_thrift(), right->data_type()->to_thrift()});
    fn.__set_ret_type(result_type->to_thrift());
    fn.__set_has_var_args(false);

    TExprNode node;
    node.__set_node_type(TExprNodeType::BINARY_PRED);
    node.__set_opcode(opcode);
    node.__set_type(result_type->to_thrift());
    node.__set_fn(fn);
    node.__set_num_children(2);
    node.__set_is_nullable(false);

    auto expr = VectorizedFnCall::create_shared(node);
    expr->add_child(left);
    expr->add_child(right);
    return expr;
}

VExprSPtr array_element_at(const VExprSPtr& parent, DataTypePtr child_type, int64_t ordinal) {
    auto expr = std::make_shared<TestFunctionExpr>("element_at", std::move(child_type));
    expr->add_child(parent);
    expr->add_child(literal(i64(), Field::create_field<TYPE_BIGINT>(ordinal)));
    return expr;
}

VExprSPtr map_values(const VExprSPtr& parent, DataTypePtr value_type) {
    auto expr = std::make_shared<TestFunctionExpr>(
            "map_values", std::make_shared<DataTypeArray>(std::move(value_type)));
    expr->add_child(parent);
    return expr;
}

VExprSPtr map_keys(const VExprSPtr& parent, DataTypePtr key_type) {
    auto expr = std::make_shared<TestFunctionExpr>(
            "map_keys", std::make_shared<DataTypeArray>(std::move(key_type)));
    expr->add_child(parent);
    return expr;
}

VExprSPtr array_contains(const VExprSPtr& array, const VExprSPtr& value) {
    auto expr = std::make_shared<TestFunctionExpr>("array_contains", u8());
    expr->add_child(array);
    expr->add_child(value);
    return expr;
}

VExprSPtr like_expr(const VExprSPtr& left, const std::string& pattern) {
    auto expr = std::make_shared<TestFunctionExpr>("like", u8());
    expr->add_child(left);
    expr->add_child(literal(str(), Field::create_field<TYPE_STRING>(pattern)));
    return expr;
}

VExprSPtr struct_element_by_selector(const VExprSPtr& parent, DataTypePtr child_type,
                                     const VExprSPtr& selector) {
    auto expr = std::make_shared<TestFunctionExpr>("struct_element", std::move(child_type));
    expr->add_child(parent);
    expr->add_child(selector);
    return expr;
}

VExprSPtr int_gt(const VExprSPtr& left, int32_t value) {
    auto expr = std::make_shared<TestFunctionExpr>("gt", u8(), TExprNodeType::BINARY_PRED,
                                                   TExprOpcode::GT);
    expr->add_child(left);
    expr->add_child(literal(i32(), Field::create_field<TYPE_INT>(value)));
    return expr;
}

VExprSPtr binary_predicate(TExprOpcode::type opcode, const VExprSPtr& left,
                           const VExprSPtr& right) {
    auto expr = std::make_shared<TestFunctionExpr>("binary_predicate", u8(),
                                                   TExprNodeType::BINARY_PRED, opcode);
    expr->add_child(left);
    expr->add_child(right);
    return expr;
}

VExprSPtr in_predicate(const VExprSPtr& probe, const DataTypePtr& literal_type,
                       const std::vector<Field>& values) {
    auto expr = std::make_shared<TestFunctionExpr>("in", u8(), TExprNodeType::IN_PRED);
    expr->add_child(probe);
    for (const auto& value : values) {
        expr->add_child(literal(literal_type, value));
    }
    return expr;
}

VExprSPtr null_predicate(const VExprSPtr& child, bool is_null) {
    auto expr =
            std::make_shared<TestFunctionExpr>(is_null ? "is_null_pred" : "is_not_null_pred", u8());
    expr->add_child(child);
    return expr;
}

VExprSPtr cast_expr(const VExprSPtr& child, DataTypePtr target_type) {
    auto expr = Cast::create_shared(std::move(target_type));
    expr->add_child(child);
    return expr;
}

VExprSPtr compound_predicate(TExprOpcode::type opcode, const VExprSPtr& left,
                             const VExprSPtr& right) {
    auto expr = std::make_shared<TestFunctionExpr>("compound", u8(), TExprNodeType::COMPOUND_PRED,
                                                   opcode);
    expr->add_child(left);
    expr->add_child(right);
    return expr;
}

std::vector<NestedStructPath> collect_paths(const VExprSPtr& expr) {
    std::vector<NestedStructPath> paths;
    collect_nested_struct_paths(expr, &paths);
    return paths;
}

void expect_name_selector(const StructChildSelector& selector, const std::string& name) {
    EXPECT_TRUE(selector.by_name);
    EXPECT_EQ(selector.name, name);
}

void expect_ordinal_selector(const StructChildSelector& selector, size_t ordinal) {
    EXPECT_FALSE(selector.by_name);
    EXPECT_EQ(selector.ordinal, ordinal);
}

void expect_path_root(const NestedStructPath& path, size_t global_index) {
    EXPECT_EQ(path.root_global_index, GlobalIndex(global_index));
}

class ColumnMapperCastTest : public testing::Test {
protected:
    void SetUp() override { state.set_enable_strict_cast(true); }

    Status prepare_open_execute(VExprContext* context, Block* block, int* result_column_id) {
        RETURN_IF_ERROR(context->prepare(&state, RowDescriptor()));
        RETURN_IF_ERROR(context->open(&state));
        return context->execute(block, result_column_id);
    }

    MockRuntimeState state;
};

class Int64ChildGreaterThanExpr final : public VExpr {
public:
    explicit Int64ChildGreaterThanExpr(int64_t value)
            : VExpr(std::make_shared<DataTypeUInt8>(), false), _value(value) {}

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        ColumnPtr child_column;
        RETURN_IF_ERROR(
                get_child(0)->execute_column(context, block, selector, count, child_column));
        const auto& input = assert_cast<const ColumnInt64&>(*child_column);
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            result_data[row] = input.get_element(row) > _value;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

    Status clone_node(VExprSPtr* cloned_expr) const override {
        DORIS_CHECK(cloned_expr != nullptr);
        *cloned_expr = std::make_shared<Int64ChildGreaterThanExpr>(_value);
        return Status::OK();
    }

private:
    const int64_t _value;
    const std::string _expr_name = "Int64ChildGreaterThanExpr";
};

class Int64BinaryPredicateExpr final : public VExpr {
public:
    explicit Int64BinaryPredicateExpr(TExprOpcode::type opcode)
            : VExpr(std::make_shared<DataTypeUInt8>(), false) {
        set_node_type(TExprNodeType::BINARY_PRED);
        _opcode = opcode;
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        ColumnPtr left_column;
        RETURN_IF_ERROR(get_child(0)->execute_column(context, block, selector, count, left_column));
        ColumnPtr right_column;
        RETURN_IF_ERROR(
                get_child(1)->execute_column(context, block, selector, count, right_column));

        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const auto left = left_column->get_int(row);
            const auto right = right_column->get_int(row);
            switch (_opcode) {
            case TExprOpcode::GT:
                result_data[row] = left > right;
                break;
            case TExprOpcode::LT:
                result_data[row] = left < right;
                break;
            default:
                return Status::InternalError("Unsupported test opcode {}", _opcode);
            }
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

    Status clone_node(VExprSPtr* cloned_expr) const override {
        DORIS_CHECK(cloned_expr != nullptr);
        *cloned_expr = std::make_shared<Int64BinaryPredicateExpr>(_opcode);
        return Status::OK();
    }

private:
    const std::string _expr_name = "Int64BinaryPredicateExpr";
};

VExprSPtr create_in_predicate() {
    TExprNode node;
    node.__set_node_type(TExprNodeType::IN_PRED);
    node.__set_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
    node.__set_is_nullable(false);
    node.__set_num_children(0);
    TInPredicate in_predicate;
    in_predicate.__set_is_not_in(false);
    node.__set_in_predicate(in_predicate);
    return VInPredicate::create_shared(node);
}

// ----------------------------------------------------------------------
// L0 schema projection helper tests.
// These tests isolate LocalColumnIndex projection semantics before
// TableColumnMapper starts mutating ColumnMapping state.
// ----------------------------------------------------------------------

TEST(ColumnMapperSchemaProjectionTest, ProjectsStructByLocalIdAndKeepsFileOrder) {
    auto a = field_id_col("a", 101, i32(), 0);
    auto b = field_id_col("b", 102, str(), 1);
    auto root = struct_col("s", 100, {a, b}, 7);

    LocalColumnIndex projection = LocalColumnIndex::partial_local(7);
    projection.children.push_back(LocalColumnIndex::local(1));
    projection.children.push_back(LocalColumnIndex::local(0));

    ColumnDefinition projected;
    ASSERT_TRUE(project_column_definition(root, projection, &projected).ok());
    ASSERT_EQ(projected.children.size(), 2);
    EXPECT_EQ(projected.children[0].name, "a");
    EXPECT_EQ(projected.children[1].name, "b");

    const auto* projected_type =
            assert_cast<const DataTypeStruct*>(remove_nullable(projected.type).get());
    ASSERT_EQ(projected_type->get_elements().size(), 2);
    EXPECT_EQ(projected_type->get_element_name(0), "a");
    EXPECT_EQ(projected_type->get_element_name(1), "b");
}

TEST(ColumnMapperSchemaProjectionTest, ProjectsArrayElementStructLeaf) {
    auto a = field_id_col("a", 1, i32(), 0);
    auto b = field_id_col("b", 2, str(), 1);
    auto element = struct_col("element", 10, {a, b}, 0);
    auto array = array_col("items", 100, element, 5);

    LocalColumnIndex projection = LocalColumnIndex::partial_local(5);
    auto element_projection = LocalColumnIndex::partial_local(0);
    element_projection.children.push_back(LocalColumnIndex::local(1));
    projection.children.push_back(std::move(element_projection));

    ColumnDefinition projected;
    ASSERT_TRUE(project_column_definition(array, projection, &projected).ok());
    ASSERT_EQ(projected.children.size(), 1);
    ASSERT_EQ(projected.children[0].children.size(), 1);
    EXPECT_EQ(projected.children[0].children[0].name, "b");

    const auto* array_type =
            assert_cast<const DataTypeArray*>(remove_nullable(projected.type).get());
    const auto* element_type = assert_cast<const DataTypeStruct*>(
            remove_nullable(array_type->get_nested_type()).get());
    ASSERT_EQ(element_type->get_elements().size(), 1);
    EXPECT_EQ(element_type->get_element_name(0), "b");
}

TEST(ColumnMapperSchemaProjectionTest, ProjectsMapValueStructLeaf) {
    auto key = field_id_col("key", 1, str(), 0);
    auto value_a = field_id_col("a", 2, i32(), 0);
    auto value_b = field_id_col("b", 3, str(), 1);
    auto value_type =
            std::make_shared<DataTypeStruct>(DataTypes {i32(), str()}, Strings {"a", "b"});
    ColumnDefinition value = field_id_col("value", 4, value_type, 1);
    value.children = {value_a, value_b};
    auto map = map_col("m", 100, {key, value}, str(), value_type, 9);

    LocalColumnIndex projection = LocalColumnIndex::partial_local(9);
    projection.children.push_back(LocalColumnIndex::local(0));
    auto value_projection = LocalColumnIndex::partial_local(1);
    value_projection.children.push_back(LocalColumnIndex::local(1));
    projection.children.push_back(std::move(value_projection));

    ColumnDefinition projected;
    ASSERT_TRUE(project_column_definition(map, projection, &projected).ok());
    ASSERT_EQ(projected.children.size(), 2);
    EXPECT_EQ(projected.children[0].name, "key");
    EXPECT_TRUE(projected.children[0].children.empty());
    EXPECT_EQ(projected.children[1].name, "value");
    ASSERT_EQ(projected.children[1].children.size(), 1);
    EXPECT_EQ(projected.children[1].children[0].name, "b");

    const auto* map_type = assert_cast<const DataTypeMap*>(remove_nullable(projected.type).get());
    const auto* projected_value =
            assert_cast<const DataTypeStruct*>(remove_nullable(map_type->get_value_type()).get());
    ASSERT_EQ(projected_value->get_elements().size(), 1);
    EXPECT_EQ(projected_value->get_element_name(0), "b");
}

TEST(ColumnMapperSchemaProjectionTest, RejectsMapKeyOnlyProjection) {
    auto key = field_id_col("key", 1, str(), 0);
    auto value = field_id_col("value", 2, i32(), 1);
    auto map = map_col("m", 100, {key, value}, str(), i32(), 9);

    LocalColumnIndex projection = LocalColumnIndex::partial_local(9);
    projection.children.push_back(LocalColumnIndex::local(0));

    ColumnDefinition projected;
    const auto status = project_column_definition(map, projection, &projected);
    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("contains no value child"), std::string::npos);
}

TEST(ColumnMapperSchemaProjectionTest, RejectsInvalidProjectionChildIdWithFieldName) {
    auto root = struct_col("s", 100, {field_id_col("a", 101, i32(), 0)}, 7);

    LocalColumnIndex projection = LocalColumnIndex::partial_local(7);
    projection.children.push_back(LocalColumnIndex::local(99));

    ColumnDefinition projected;
    const auto status = project_column_definition(root, projection, &projected);
    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("Invalid projection child id 99 for field s"),
              std::string::npos);
}

TEST(ColumnMapperSchemaProjectionTest, RejectsEmptyProjectionPathWithFieldName) {
    auto root = struct_col("s", 100, {field_id_col("a", 101, i32(), 0)}, 7);

    LocalColumnIndex projection = LocalColumnIndex::partial_local(7);
    projection.children.push_back(LocalColumnIndex::local(-1));

    ColumnDefinition projected;
    const auto status = project_column_definition(root, projection, &projected);
    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("Empty projection path for field s"), std::string::npos);
}

TEST(ColumnMapperSchemaProjectionTest, RejectsInvalidChildProjectionForPrimitiveField) {
    auto root = field_id_col("i", 1, i32(), 7);
    LocalColumnIndex projection = LocalColumnIndex::partial_local(7);
    projection.children.push_back(LocalColumnIndex::local(0));

    ColumnDefinition projected;
    const auto status = project_column_definition(root, projection, &projected);
    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("Invalid projection child id 0 for field i"),
              std::string::npos);
}

// ----------------------------------------------------------------------
// L0 nested helper tests.
// These tests cover child ordering, direct schema path resolution, and
// predicate-filter merging without going through create_scan_request().
// ----------------------------------------------------------------------

TEST(ColumnMapperNestedHelperTest, PresentChildMappingsAreSortedByFileLocalId) {
    ColumnMapping b;
    b.table_column_name = "b";
    b.file_local_id = 2;
    ColumnMapping missing;
    missing.table_column_name = "missing";
    ColumnMapping a;
    a.table_column_name = "a";
    a.file_local_id = 1;

    const std::vector<ColumnMapping> child_mappings = {b, missing, a};
    const auto present = present_child_mappings_in_file_order(child_mappings);
    ASSERT_EQ(present.size(), 2);
    EXPECT_EQ(present[0]->table_column_name, "a");
    EXPECT_EQ(present[1]->table_column_name, "b");
}

TEST(ColumnMapperNestedHelperTest, BuildsProjectionByNameAndOrdinalSelectors) {
    auto leaf = field_id_col("leaf", 3, i32(), 0);
    auto nested = struct_col("nested", 2, {leaf}, 1);
    auto first = field_id_col("first", 1, str(), 0);
    const std::vector<ColumnDefinition> children = {first, nested};

    const std::vector<StructChildSelector> by_name = {
            {.by_name = true, .name = "nested", .ordinal = 0},
            {.by_name = true, .name = "leaf", .ordinal = 0},
    };
    LocalColumnIndex named_projection;
    ASSERT_TRUE(build_file_child_projection_from_schema(children, by_name, &named_projection).ok());
    EXPECT_EQ(named_projection.local_id(), 1);
    ASSERT_EQ(named_projection.children.size(), 1);
    EXPECT_EQ(named_projection.children[0].local_id(), 0);

    const std::vector<StructChildSelector> by_ordinal = {
            {.by_name = false, .name = "", .ordinal = 2},
            {.by_name = false, .name = "", .ordinal = 1},
    };
    LocalColumnIndex ordinal_projection;
    ASSERT_TRUE(build_file_child_projection_from_schema(children, by_ordinal, &ordinal_projection)
                        .ok());
    EXPECT_EQ(ordinal_projection.local_id(), 1);
    ASSERT_EQ(ordinal_projection.children.size(), 1);
    EXPECT_EQ(ordinal_projection.children[0].local_id(), 0);
}

// ----------------------------------------------------------------------
// collect_nested_struct_paths() helper tests.
// These tests assert the entry helper for nested scan projection: it only discovers
// table-side struct paths. Later localization decides how to add scan projections.
// ----------------------------------------------------------------------

TEST(ColumnMapperCollectNestedStructPathsTest, CollectsNameOrdinalAndBooleanSelectors) {
    const auto leaf_type = i32();
    const auto inner_type =
            std::make_shared<DataTypeStruct>(DataTypes {leaf_type, leaf_type}, Strings {"x", "y"});
    const auto root_type = std::make_shared<DataTypeStruct>(DataTypes {inner_type, leaf_type},
                                                            Strings {"nested", "missing"});
    const auto root = table_slot(0, 3, root_type, "s");

    const auto nested_by_ordinal = struct_element_by_selector(
            struct_element_by_selector(root, inner_type,
                                       literal(i32(), Field::create_field<TYPE_INT>(1))),
            leaf_type, literal(i32(), Field::create_field<TYPE_INT>(2)));
    auto paths = collect_paths(nested_by_ordinal);
    ASSERT_EQ(paths.size(), 1);
    expect_path_root(paths[0], 3);
    ASSERT_EQ(paths[0].selectors.size(), 2);
    expect_ordinal_selector(paths[0].selectors[0], 1);
    expect_ordinal_selector(paths[0].selectors[1], 2);

    const std::vector<VExprSPtr> positive_ordinal_selectors = {
            literal(std::make_shared<DataTypeInt8>(),
                    Field::create_field<TYPE_TINYINT>(static_cast<int8_t>(1))),
            literal(std::make_shared<DataTypeInt16>(),
                    Field::create_field<TYPE_SMALLINT>(static_cast<int16_t>(2))),
            literal(i32(), Field::create_field<TYPE_INT>(3)),
            literal(i64(), Field::create_field<TYPE_BIGINT>(4)),
            literal(u8(), Field::create_field<TYPE_BOOLEAN>(true)),
    };
    for (size_t idx = 0; idx < positive_ordinal_selectors.size(); ++idx) {
        const auto selected =
                struct_element_by_selector(root, leaf_type, positive_ordinal_selectors[idx]);
        paths = collect_paths(selected);
        ASSERT_EQ(paths.size(), 1);
        ASSERT_EQ(paths[0].selectors.size(), 1);
        expect_ordinal_selector(paths[0].selectors[0], idx == 4 ? 1 : idx + 1);
    }

    paths = collect_paths(struct_element(root, leaf_type, "missing"));
    ASSERT_EQ(paths.size(), 1);
    ASSERT_EQ(paths[0].selectors.size(), 1);
    expect_name_selector(paths[0].selectors[0], "missing");
}

TEST(ColumnMapperCollectNestedStructPathsTest, IgnoresInvalidSelectorsAndNonPathRoots) {
    const auto leaf_type = i32();
    const auto root_type = std::make_shared<DataTypeStruct>(DataTypes {leaf_type}, Strings {"a"});
    const auto root = table_slot(0, 0, root_type, "s");

    const std::vector<VExprSPtr> invalid_selectors = {
            literal(i32(), Field::create_field<TYPE_INT>(0)),
            literal(i32(), Field::create_field<TYPE_INT>(-1)),
            literal(u8(), Field::create_field<TYPE_BOOLEAN>(false)),
            literal(f32(), Field::create_field<TYPE_FLOAT>(1.0F)),
            literal(f64(), Field::create_field<TYPE_DOUBLE>(1.0)),
            table_slot(1, 1, i32(), "selector"),
    };
    for (const auto& selector : invalid_selectors) {
        EXPECT_TRUE(collect_paths(struct_element_by_selector(root, leaf_type, selector)).empty());
    }

    auto wrong_arity = std::make_shared<TestFunctionExpr>("struct_element", leaf_type);
    wrong_arity->add_child(root);
    EXPECT_TRUE(collect_paths(wrong_arity).empty());

    auto not_struct_element = std::make_shared<TestFunctionExpr>("other_function", leaf_type);
    not_struct_element->add_child(root);
    not_struct_element->add_child(literal(str(), Field::create_field<TYPE_STRING>("a")));
    EXPECT_TRUE(collect_paths(not_struct_element).empty());

    EXPECT_TRUE(collect_paths(struct_element(literal(str(), Field::create_field<TYPE_STRING>("x")),
                                             leaf_type, "a"))
                        .empty());
    EXPECT_TRUE(collect_paths(nullptr).empty());
}

TEST(ColumnMapperCollectNestedStructPathsTest, RecursesThroughExpressionsAndKeepsCompletePath) {
    const auto leaf_type = i32();
    const auto inner_type = std::make_shared<DataTypeStruct>(DataTypes {leaf_type}, Strings {"b"});
    const auto root_type =
            std::make_shared<DataTypeStruct>(DataTypes {inner_type, leaf_type}, Strings {"a", "c"});
    const auto root = table_slot(0, 2, root_type, "s");
    const auto path_a = struct_element_by_selector(
            root, inner_type, literal(str(), Field::create_field<TYPE_STRING>("a")));
    const auto path_ab = struct_element_by_selector(
            path_a, leaf_type, literal(str(), Field::create_field<TYPE_STRING>("b")));
    const auto path_c = struct_element_by_selector(
            root, leaf_type, literal(str(), Field::create_field<TYPE_STRING>("c")));

    auto paths = collect_paths(binary_predicate(
            TExprOpcode::GT, path_ab, literal(leaf_type, Field::create_field<TYPE_INT>(1))));
    ASSERT_EQ(paths.size(), 1);
    expect_path_root(paths[0], 2);
    ASSERT_EQ(paths[0].selectors.size(), 2);
    expect_name_selector(paths[0].selectors[0], "a");
    expect_name_selector(paths[0].selectors[1], "b");

    paths = collect_paths(compound_predicate(
            TExprOpcode::COMPOUND_OR,
            binary_predicate(TExprOpcode::GT, path_ab,
                             literal(leaf_type, Field::create_field<TYPE_INT>(1))),
            binary_predicate(TExprOpcode::LT, path_c,
                             literal(leaf_type, Field::create_field<TYPE_INT>(2)))));
    ASSERT_EQ(paths.size(), 2);
    ASSERT_EQ(paths[0].selectors.size(), 2);
    ASSERT_EQ(paths[1].selectors.size(), 1);
    expect_name_selector(paths[0].selectors[0], "a");
    expect_name_selector(paths[0].selectors[1], "b");
    expect_name_selector(paths[1].selectors[0], "c");

    auto fn = std::make_shared<TestFunctionExpr>("fn", leaf_type);
    fn->add_child(path_ab);
    fn->add_child(table_slot(3, 4, leaf_type, "other"));
    paths = collect_paths(fn);
    ASSERT_EQ(paths.size(), 1);
    ASSERT_EQ(paths[0].selectors.size(), 2);

    auto if_expr = std::make_shared<TestFunctionExpr>("if", leaf_type);
    if_expr->add_child(literal(u8(), Field::create_field<TYPE_BOOLEAN>(true)));
    if_expr->add_child(path_ab);
    if_expr->add_child(path_c);
    paths = collect_paths(if_expr);
    ASSERT_EQ(paths.size(), 2);

    paths = collect_paths(compound_predicate(TExprOpcode::COMPOUND_AND, path_ab, path_ab));
    ASSERT_EQ(paths.size(), 2);

    paths = collect_paths(path_ab);
    ASSERT_EQ(paths.size(), 1);
    ASSERT_EQ(paths[0].selectors.size(), 2);
}

TEST(ColumnMapperCollectNestedStructPathsTest, CastBehaviorSeparatesProjectionAndPruningRules) {
    const auto int_type = i32();
    const auto bigint_type = i64();
    const auto float_type = f32();
    const auto double_type = f64();
    const auto decimal_small = dec32(8, 2);
    const auto decimal_wide = dec32(9, 2);
    const auto decimal_changed_scale = dec32(9, 3);

    const auto root_type = std::make_shared<DataTypeStruct>(
            DataTypes {int_type, float_type, decimal_small}, Strings {"i", "f", "d"});
    const auto root = table_slot(0, 0, root_type, "s");
    const auto int_path = struct_element(root, int_type, "i");
    const auto float_path = struct_element(root, float_type, "f");
    const auto decimal_path = struct_element(root, decimal_small, "d");

    auto paths = collect_paths(cast_expr(int_path, bigint_type));
    ASSERT_EQ(paths.size(), 1);
    expect_name_selector(paths[0].selectors[0], "i");

    paths = collect_paths(cast_expr(float_path, double_type));
    ASSERT_EQ(paths.size(), 1);
    expect_name_selector(paths[0].selectors[0], "f");

    paths = collect_paths(cast_expr(decimal_path, decimal_wide));
    ASSERT_EQ(paths.size(), 1);
    expect_name_selector(paths[0].selectors[0], "d");

    paths = collect_paths(
            cast_expr(struct_element(root, make_nullable(int_type), "i"), make_nullable(int_type)));
    ASSERT_EQ(paths.size(), 1);
    expect_name_selector(paths[0].selectors[0], "i");

    // Unsafe casts are not accepted as pruning paths, but collect_nested_struct_paths() still
    // recurses into children so scan projection can read the column needed by row-level filters.
    paths = collect_paths(cast_expr(struct_element(root, bigint_type, "i"), int_type));
    ASSERT_EQ(paths.size(), 1);
    expect_name_selector(paths[0].selectors[0], "i");

    paths = collect_paths(cast_expr(decimal_path, decimal_changed_scale));
    ASSERT_EQ(paths.size(), 1);
    expect_name_selector(paths[0].selectors[0], "d");

    EXPECT_TRUE(collect_paths(cast_expr(table_slot(1, 1, int_type, "plain"), bigint_type)).empty());
}

TEST(ColumnMapperCollectNestedStructPathsTest, ProjectionMergeKeepsFilterOnlyPathAndDeduplicates) {
    const auto int_type = i32();
    const auto string_type = str();
    auto table_a = name_col("a", int_type);
    auto table_b = name_col("b", int_type);
    auto table_output = struct_name_col("s", {table_a});
    auto full_table_struct = struct_name_col("s", {table_a, table_b});

    auto file_a = name_col("a", int_type, 0);
    auto file_b = name_col("b", int_type, 1);
    auto file_struct = struct_name_col("s", {file_a, file_b, name_col("c", string_type, 2)}, 5);

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    ASSERT_TRUE(mapper.create_mapping({table_output}, {}, {file_struct}).ok());

    const auto path_b =
            struct_element(table_slot(0, 0, full_table_struct.type, "s"), int_type, "b");
    auto filter_expr = compound_predicate(
            TExprOpcode::COMPOUND_AND,
            binary_predicate(TExprOpcode::GT, path_b,
                             literal(int_type, Field::create_field<TYPE_INT>(1))),
            binary_predicate(TExprOpcode::LT, path_b,
                             literal(int_type, Field::create_field<TYPE_INT>(10))));
    TableFilter filter {.conjunct = VExprContext::create_shared(filter_expr),
                        .global_indices = {GlobalIndex(0)}};

    FileScanRequest request;
    ASSERT_TRUE(mapper.create_scan_request({filter}, {table_output}, &request).ok());

    EXPECT_TRUE(request.non_predicate_columns.empty());
    ASSERT_EQ(request.predicate_columns.size(), 1);
    EXPECT_EQ(request.predicate_columns[0].column_id(), LocalColumnId(5));
    ASSERT_FALSE(request.predicate_columns[0].project_all_children);
    EXPECT_EQ(projection_ids(request.predicate_columns[0].children), std::vector<int32_t>({0, 1}));
}

// Scenario: row-oriented readers such as CSV/Text cannot lazy-read predicate columns separately.
// For a complex root that is both projected and referenced by a filter, the materialized mapper
// keeps one non-predicate scan entry and asks the reader to read the full top-level struct.
TEST(ColumnMapperScanRequestTest, MaterializedMapperUsesSingleScanColumnList) {
    const auto int_type = i32();
    const auto string_type = str();
    auto table_a = name_col("a", int_type, 0);
    auto table_b = name_col("b", int_type, 1);
    auto full_table_struct = struct_name_col("s", {table_a, table_b});
    auto table_output = struct_name_col("s", {table_a});

    auto file_a = name_col("a", int_type, 0);
    auto file_b = name_col("b", int_type, 1);
    auto file_struct = struct_name_col("s", {file_a, file_b, name_col("c", string_type, 2)}, 5);

    MaterializedColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    ASSERT_TRUE(mapper.create_mapping({table_output}, {}, {file_struct}).ok());

    const auto path_b =
            struct_element(table_slot(0, 0, full_table_struct.type, "s"), int_type, "b");
    auto filter_expr = binary_predicate(TExprOpcode::GT, path_b,
                                        literal(int_type, Field::create_field<TYPE_INT>(1)));
    TableFilter filter {.conjunct = VExprContext::create_shared(filter_expr),
                        .global_indices = {GlobalIndex(0)}};

    FileScanRequest request;
    ASSERT_TRUE(mapper.create_scan_request({filter}, {table_output}, &request).ok());

    EXPECT_TRUE(request.predicate_columns.empty());
    ASSERT_EQ(request.non_predicate_columns.size(), 1);
    EXPECT_EQ(request.non_predicate_columns[0].column_id(), LocalColumnId(5));
    EXPECT_TRUE(request.non_predicate_columns[0].project_all_children);
    EXPECT_TRUE(request.non_predicate_columns[0].children.empty());
}

// Scenario: a FileReader must expose semantic children for complex file columns. If it returns a
// complex DataType but leaves ColumnDefinition::children empty, mapper should return a diagnostic
// error instead of aborting inside ARRAY/MAP/STRUCT child lookup.
TEST(ColumnMapperScanRequestTest, MalformedComplexFileSchemaReturnsError) {
    const auto int_type = i32();
    const auto string_type = str();
    auto table_a = name_col("a", int_type, 0);
    auto table_b = name_col("b", string_type, 1);
    auto table_struct = struct_name_col("s", {table_a, table_b});
    auto file_struct_type =
            std::make_shared<DataTypeStruct>(DataTypes {int_type, string_type}, Strings {"a", "b"});
    auto malformed_file_struct = name_col("s", file_struct_type, 5);

    MaterializedColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    const auto status = mapper.create_mapping({table_struct}, {}, {malformed_file_struct});

    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("Malformed complex file schema"), std::string::npos)
            << status;
}

// Scenario: when the projected table schema contains the child referenced by the filter, the
// materialized mapper can still rewrite the table-level struct child predicate into a file-local
// conjunct. It remains a single full-root scan column; only the expression is localized.
TEST(ColumnMapperScanRequestTest, MaterializedMapperLocalizesMappedStructChildConjunct) {
    const auto int_type = i32();
    const auto string_type = str();
    auto table_a = name_col("a", int_type, 0);
    auto table_b = name_col("b", int_type, 1);
    auto table_struct = struct_name_col("s", {table_a, table_b});

    auto file_a = name_col("a", int_type, 0);
    auto file_b = name_col("b", int_type, 1);
    auto file_struct = struct_name_col("s", {file_a, file_b, name_col("c", string_type, 2)}, 5);

    MaterializedColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    ASSERT_TRUE(mapper.create_mapping({table_struct}, {}, {file_struct}).ok());

    const auto path_b = struct_element(table_slot(0, 0, table_struct.type, "s"), int_type, "b");
    auto filter_expr = binary_predicate(TExprOpcode::GT, path_b,
                                        literal(int_type, Field::create_field<TYPE_INT>(1)));
    TableFilter filter {.conjunct = VExprContext::create_shared(filter_expr),
                        .global_indices = {GlobalIndex(0)}};

    FileScanRequest request;
    ASSERT_TRUE(mapper.create_scan_request({filter}, {table_struct}, &request).ok());

    EXPECT_TRUE(request.predicate_columns.empty());
    ASSERT_EQ(request.non_predicate_columns.size(), 1);
    EXPECT_EQ(request.non_predicate_columns[0].column_id(), LocalColumnId(5));
    EXPECT_TRUE(request.non_predicate_columns[0].project_all_children);
    EXPECT_TRUE(request.non_predicate_columns[0].children.empty());
    ASSERT_EQ(request.conjuncts.size(), 1);
}

// Scenario: even output-only partial complex projections such as `SELECT s.a` must scan the full
// top-level struct for materialized readers, because delimited text formats cannot physically read
// only one nested child from a single text field.
TEST(ColumnMapperScanRequestTest, MaterializedMapperScansFullComplexRootForOutputOnlyProjection) {
    const auto int_type = i32();
    const auto string_type = str();
    auto table_a = name_col("a", int_type, 0);
    auto table_output = struct_name_col("s", {table_a});

    auto file_a = name_col("a", int_type, 0);
    auto file_b = name_col("b", int_type, 1);
    auto file_struct = struct_name_col("s", {file_a, file_b, name_col("c", string_type, 2)}, 5);

    MaterializedColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    ASSERT_TRUE(mapper.create_mapping({table_output}, {}, {file_struct}).ok());

    FileScanRequest request;
    ASSERT_TRUE(mapper.create_scan_request({}, {table_output}, &request).ok());

    ASSERT_EQ(request.non_predicate_columns.size(), 1);
    EXPECT_EQ(request.non_predicate_columns[0].column_id(), LocalColumnId(5));
    EXPECT_TRUE(request.non_predicate_columns[0].project_all_children);
    EXPECT_TRUE(request.non_predicate_columns[0].children.empty());
    EXPECT_TRUE(request.predicate_columns.empty());
}

// Scenario: array/map nested projections also scan the full top-level complex root for
// materialized readers. This keeps row-oriented formats from receiving Parquet-style partial
// projections for `array<struct>` elements or map value structs.
TEST(ColumnMapperScanRequestTest, MaterializedMapperScansFullArrayAndMapRoots) {
    const auto key_type = str();
    const auto int_type = i32();
    const auto string_type = str();

    auto table_array_child = name_col("b", string_type);
    auto table_array_element = struct_name_col("element", {table_array_child});
    auto table_array = array_col("items", -1, table_array_element);
    table_array.identifier = Field::create_field<TYPE_STRING>("items");
    set_name_identifiers(&table_array, -1);

    auto file_array_a = name_col("a", int_type, 0);
    auto file_array_b = name_col("b", string_type, 1);
    auto file_array_element = struct_name_col("element", {file_array_a, file_array_b}, 0);
    auto file_array = array_col("items", -1, file_array_element, 4);
    file_array.identifier = Field::create_field<TYPE_STRING>("items");
    set_name_identifiers(&file_array, 4);

    auto table_value_b = name_col("b", string_type);
    auto table_value = struct_name_col("value", {table_value_b});
    auto table_map = map_col("m", -1, {table_value}, key_type, table_value.type);
    table_map.identifier = Field::create_field<TYPE_STRING>("m");
    set_name_identifiers(&table_map, -1);

    auto file_key = name_col("key", key_type, 0);
    auto file_value_a = name_col("a", int_type, 0);
    auto file_value_b = name_col("b", string_type, 1);
    auto file_value = struct_name_col("value", {file_value_a, file_value_b}, 1);
    auto file_map = map_col("m", -1, {file_key, file_value}, key_type, file_value.type, 6);
    file_map.identifier = Field::create_field<TYPE_STRING>("m");
    set_name_identifiers(&file_map, 6);

    MaterializedColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    ASSERT_TRUE(mapper.create_mapping({table_array, table_map}, {}, {file_array, file_map}).ok());

    FileScanRequest request;
    ASSERT_TRUE(mapper.create_scan_request({}, {table_array, table_map}, &request).ok());

    ASSERT_EQ(request.non_predicate_columns.size(), 2);
    EXPECT_EQ(request.non_predicate_columns[0].column_id(), LocalColumnId(4));
    EXPECT_TRUE(request.non_predicate_columns[0].project_all_children);
    EXPECT_TRUE(request.non_predicate_columns[0].children.empty());
    EXPECT_EQ(request.non_predicate_columns[1].column_id(), LocalColumnId(6));
    EXPECT_TRUE(request.non_predicate_columns[1].project_all_children);
    EXPECT_TRUE(request.non_predicate_columns[1].children.empty());
    EXPECT_TRUE(request.predicate_columns.empty());
}

// ----------------------------------------------------------------------
// L1 create_mapping root matching tests.
// These cases cover the three supported root matching modes and the
// missing/default behavior that each mode feeds into later scan requests.
// ----------------------------------------------------------------------

TEST(ColumnMapperCreateMappingTest, ByNameMatchesCaseIdentifierAndAliases) {
    const auto int_type = i32();
    const std::vector<ColumnDefinition> table_schema = {
            name_col("ID", int_type),
            name_id_col("renamed", "legacy_name", int_type),
            [] {
                auto column = name_col("current_alias", i32());
                column.name_mapping = {"old_alias"};
                return column;
            }(),
            name_col("file_alias", int_type),
    };
    std::vector<ColumnDefinition> file_schema = {
            name_col("id", int_type, 0),
            name_col("legacy_name", int_type, 1),
            name_col("old_alias", int_type, 2),
            [] {
                auto column = name_col("physical_name", i32(), 3);
                column.name_mapping = {"file_alias"};
                return column;
            }(),
    };

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    ASSERT_TRUE(mapper.create_mapping(table_schema, {}, file_schema).ok());

    ASSERT_EQ(mapper.mappings().size(), 4);
    expect_mapping(mapper.mappings()[0], 0, "ID", 0, "id", int_type, int_type);
    expect_mapping(mapper.mappings()[1], 1, "renamed", 1, "legacy_name", int_type, int_type);
    expect_mapping(mapper.mappings()[2], 2, "current_alias", 2, "old_alias", int_type, int_type);
    expect_mapping(mapper.mappings()[3], 3, "file_alias", 3, "physical_name", int_type, int_type);
}

TEST(ColumnMapperCreateMappingTest, ByNameUsesFirstMatchingFileFieldWhenAmbiguous) {
    const auto int_type = i32();
    const std::vector<ColumnDefinition> table_schema = {
            name_col("id", int_type),
    };
    const std::vector<ColumnDefinition> file_schema = {
            name_col("ID", int_type, 0),
            name_col("id", int_type, 1),
    };

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    ASSERT_TRUE(mapper.create_mapping(table_schema, {}, file_schema).ok());

    ASSERT_EQ(mapper.mappings().size(), 1);
    expect_mapping(mapper.mappings()[0], 0, "id", 0, "ID", int_type, int_type);
}

TEST(ColumnMapperCreateMappingTest, TimestampTzScaleMismatchKeepsFilterAboveReader) {
    // Scenario: HDFS TVF may expose a table slot as TIMESTAMPTZ(0), while a Parquet logical UTC
    // timestamp file schema is materialized as TIMESTAMPTZ(6). Finalization must not add a SQL
    // cast from scale 6 to scale 0, because that cast rounds fractional seconds:
    //   2025-06-01 12:34:56.789+08:00 -> 2025-06-01 12:34:57+08:00
    // Reader finalization should pass the column through; the output slot type controls display
    // scale and hides the fractional part without changing the stored instant.
    const auto table_type = timestamptz(0);
    const auto file_type = timestamptz(6);
    const std::vector<ColumnDefinition> table_schema = {name_col("ts_tz", table_type)};
    const std::vector<ColumnDefinition> file_schema = {name_col("ts_tz", file_type, 0)};

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    ASSERT_TRUE(mapper.create_mapping(table_schema, {}, file_schema).ok());

    ASSERT_EQ(mapper.mappings().size(), 1);
    expect_mapping(mapper.mappings()[0], 0, "ts_tz", 0, "ts_tz", file_type, table_type);
    EXPECT_TRUE(mapper.mappings()[0].is_trivial);
    EXPECT_EQ(mapper.mappings()[0].filter_conversion, FilterConversionType::FINALIZE_ONLY);

    TableFilter filter {
            .conjunct = VExprContext::create_shared(table_slot(0, 0, table_type, "ts_tz")),
            .global_indices = {GlobalIndex(0)}};
    FileScanRequest request;
    ASSERT_TRUE(mapper.create_scan_request({filter}, table_schema, &request).ok());
    EXPECT_TRUE(request.predicate_columns.empty());
    ASSERT_EQ(request.non_predicate_columns.size(), 1);
    EXPECT_EQ(request.non_predicate_columns[0].column_id(), LocalColumnId(0));
    EXPECT_TRUE(request.conjuncts.empty());
}

TEST(ColumnMapperCreateMappingTest, ByNameUsesNameMappingForRenamedColumn) {
    const auto int_type = i32();
    auto table_column = name_col("current_id", int_type);
    table_column.name_mapping = {"legacy_id"};
    const std::vector<ColumnDefinition> file_schema = {
            name_col("legacy_id", int_type, 0),
    };

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    ASSERT_TRUE(mapper.create_mapping({table_column}, {}, file_schema).ok());

    ASSERT_EQ(mapper.mappings().size(), 1);
    expect_mapping(mapper.mappings()[0], 0, "current_id", 0, "legacy_id", int_type, int_type);
}

TEST(ColumnMapperCreateMappingTest, ByNameUsesNameMappingForNestedSchemaEvolution) {
    const auto int_type = i32();
    const auto string_type = str();

    auto table_country = name_col("country", string_type);
    table_country.name_mapping = {"old_country"};
    auto table_city = name_col("city", string_type);
    auto table_struct = struct_name_col("struct_column", {table_country, table_city});
    set_name_identifiers(&table_struct, -1);

    auto table_item = name_col("item", string_type);
    table_item.name_mapping = {"product"};
    auto table_quantity = name_col("quantity", int_type);
    auto table_element = struct_name_col("element", {table_item, table_quantity});
    auto table_array = array_col("array_column", -1, table_element);
    set_name_identifiers(&table_array, -1);

    auto table_key = name_col("key", string_type);
    auto table_full_name = name_col("full_name", string_type);
    table_full_name.name_mapping = {"name"};
    auto table_age = name_col("age", int_type);
    auto table_value = struct_name_col("value", {table_full_name, table_age});
    auto table_map =
            map_col("new_map_column", -1, {table_key, table_value}, string_type, table_value.type);
    table_map.name_mapping = {"map_column"};
    set_name_identifiers(&table_map, -1);

    auto file_old_country = name_col("old_country", string_type, 0);
    auto file_city = name_col("city", string_type, 1);
    auto file_struct = struct_name_col("struct_column", {file_old_country, file_city}, 3);
    set_name_identifiers(&file_struct, 3);

    auto file_product = name_col("product", string_type, 0);
    auto file_element = struct_name_col("list", {file_product}, 0);
    auto file_array = array_col("array_column", -1, file_element, 4);
    set_name_identifiers(&file_array, 4);

    auto file_key = name_col("key", string_type, 0);
    auto file_name = name_col("name", string_type, 0);
    auto file_age = name_col("age", int_type, 1);
    auto file_value = struct_name_col("value", {file_name, file_age}, 1);
    auto file_map =
            map_col("map_column", -1, {file_key, file_value}, string_type, file_value.type, 5);
    set_name_identifiers(&file_map, 5);

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    ASSERT_TRUE(mapper.create_mapping({table_struct, table_array, table_map}, {},
                                      {file_struct, file_array, file_map})
                        .ok());

    ASSERT_EQ(mapper.mappings().size(), 3);
    const auto& struct_mapping = mapper.mappings()[0];
    expect_mapping(struct_mapping, 0, "struct_column", 3, "struct_column", file_struct.type,
                   table_struct.type);
    ASSERT_EQ(struct_mapping.child_mappings.size(), 2);
    EXPECT_EQ(struct_mapping.child_mappings[0].file_column_name, "old_country");
    EXPECT_EQ(*struct_mapping.child_mappings[0].file_local_id, 0);
    EXPECT_EQ(struct_mapping.child_mappings[1].file_column_name, "city");
    EXPECT_EQ(*struct_mapping.child_mappings[1].file_local_id, 1);

    const auto& array_mapping = mapper.mappings()[1];
    expect_mapping(array_mapping, 1, "array_column", 4, "array_column", file_array.type,
                   table_array.type);
    ASSERT_EQ(array_mapping.child_mappings.size(), 1);
    const auto& element_mapping = array_mapping.child_mappings[0];
    EXPECT_EQ(element_mapping.file_column_name, "list");
    EXPECT_EQ(*element_mapping.file_local_id, 0);
    ASSERT_EQ(element_mapping.child_mappings.size(), 2);
    EXPECT_EQ(element_mapping.child_mappings[0].file_column_name, "product");
    EXPECT_EQ(*element_mapping.child_mappings[0].file_local_id, 0);
    expect_missing(element_mapping.child_mappings[1]);

    const auto& map_mapping = mapper.mappings()[2];
    expect_mapping(map_mapping, 2, "new_map_column", 5, "map_column", file_map.type,
                   table_map.type);
    ASSERT_EQ(map_mapping.child_mappings.size(), 2);
    EXPECT_EQ(map_mapping.child_mappings[0].file_column_name, "key");
    EXPECT_EQ(*map_mapping.child_mappings[0].file_local_id, 0);
    const auto& value_mapping = map_mapping.child_mappings[1];
    EXPECT_EQ(value_mapping.file_column_name, "value");
    EXPECT_EQ(*value_mapping.file_local_id, 1);
    ASSERT_EQ(value_mapping.child_mappings.size(), 2);
    EXPECT_EQ(value_mapping.child_mappings[0].file_column_name, "name");
    EXPECT_EQ(*value_mapping.child_mappings[0].file_local_id, 0);
    EXPECT_EQ(value_mapping.child_mappings[1].file_column_name, "age");
    EXPECT_EQ(*value_mapping.child_mappings[1].file_local_id, 1);
}

// Scenario: SELECT * can carry only the full complex DataType without expanded nested
// ColumnDefinitions. When an old file has map value STRUCT<age, name> and the table type is
// STRUCT<age, full_name, gender>, the mapper must still build child mappings instead of letting
// TableReader cast between incompatible struct shapes.
TEST(ColumnMapperCreateMappingTest, SynthesizesMissingMapValueStructChildrenFromType) {
    const auto int_type = i32();
    const auto string_type = str();
    const auto table_value_type = std::make_shared<DataTypeStruct>(
            DataTypes {int_type, string_type, string_type}, Strings {"age", "full_name", "gender"});
    const auto file_value_type = std::make_shared<DataTypeStruct>(DataTypes {int_type, string_type},
                                                                  Strings {"age", "name"});

    auto table_map = name_col("new_map_column",
                              std::make_shared<DataTypeMap>(string_type, table_value_type));
    table_map.name_mapping = {"map_column"};
    set_name_identifiers(&table_map, -1);

    auto file_age = name_col("age", int_type, 0);
    auto file_name = name_col("name", string_type, 1);
    auto file_value = struct_name_col("value", {file_age, file_name}, 1);
    auto file_key = name_col("key", string_type, 0);
    auto file_map =
            map_col("map_column", -1, {file_key, file_value}, string_type, file_value_type, 5);
    set_name_identifiers(&file_map, 5);

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    ASSERT_TRUE(mapper.create_mapping({table_map}, {}, {file_map}).ok());

    ASSERT_EQ(mapper.mappings().size(), 1);
    const auto& map_mapping = mapper.mappings()[0];
    ASSERT_EQ(map_mapping.child_mappings.size(), 2);
    EXPECT_EQ(map_mapping.child_mappings[0].table_column_name, "key");
    EXPECT_EQ(map_mapping.child_mappings[0].file_column_name, "key");
    EXPECT_EQ(*map_mapping.child_mappings[0].file_local_id, 0);

    const auto& value_mapping = map_mapping.child_mappings[1];
    EXPECT_EQ(value_mapping.table_column_name, "value");
    EXPECT_EQ(value_mapping.file_column_name, "value");
    EXPECT_EQ(*value_mapping.file_local_id, 1);
    ASSERT_EQ(value_mapping.child_mappings.size(), 3);
    EXPECT_EQ(value_mapping.child_mappings[0].table_column_name, "age");
    EXPECT_EQ(value_mapping.child_mappings[0].file_column_name, "age");
    EXPECT_EQ(*value_mapping.child_mappings[0].file_local_id, 0);
    EXPECT_EQ(value_mapping.child_mappings[1].table_column_name, "full_name");
    EXPECT_EQ(value_mapping.child_mappings[1].file_column_name, "name");
    EXPECT_EQ(*value_mapping.child_mappings[1].file_local_id, 1);
    EXPECT_EQ(value_mapping.child_mappings[2].table_column_name, "gender");
    expect_missing(value_mapping.child_mappings[2]);
    EXPECT_FALSE(value_mapping.is_trivial);
}

// Scenario: MAP_KEYS(new_map_column) may build a key-only nested projection, while SELECT * still
// needs the whole map root. The mapper must add a synthetic value child and recursively map the old
// value struct instead of treating Struct(name, age) as a leaf to CAST into the table value struct.
TEST(ColumnMapperCreateMappingTest, KeyOnlyMapProjectionStillMapsEvolvedValueStruct) {
    const auto int_type = i32();
    const auto string_type = str();
    const auto table_value_type = std::make_shared<DataTypeStruct>(
            DataTypes {int_type, string_type, string_type}, Strings {"age", "full_name", "gender"});
    const auto file_value_type = std::make_shared<DataTypeStruct>(DataTypes {string_type, int_type},
                                                                  Strings {"name", "age"});

    auto table_key = name_col("key", string_type);
    auto table_map = map_col("new_map_column", -1, {table_key}, string_type, table_value_type);
    table_map.name_mapping = {"map_column"};
    set_name_identifiers(&table_map, -1);

    auto file_key = name_col("key", string_type, 0);
    auto file_name = name_col("name", string_type, 0);
    auto file_age = name_col("age", int_type, 1);
    auto file_value = struct_name_col("value", {file_name, file_age}, 1);
    auto file_map =
            map_col("map_column", -1, {file_key, file_value}, string_type, file_value_type, 5);
    set_name_identifiers(&file_map, 5);

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    ASSERT_TRUE(mapper.create_mapping({table_map}, {}, {file_map}).ok());

    ASSERT_EQ(mapper.mappings().size(), 1);
    const auto& map_mapping = mapper.mappings()[0];
    ASSERT_EQ(map_mapping.child_mappings.size(), 2);
    EXPECT_EQ(map_mapping.child_mappings[0].table_column_name, "key");
    EXPECT_EQ(map_mapping.child_mappings[0].file_column_name, "key");
    EXPECT_EQ(*map_mapping.child_mappings[0].file_local_id, 0);

    const auto& value_mapping = map_mapping.child_mappings[1];
    EXPECT_EQ(value_mapping.table_column_name, "value");
    EXPECT_EQ(value_mapping.file_column_name, "value");
    EXPECT_EQ(*value_mapping.file_local_id, 1);
    ASSERT_EQ(value_mapping.child_mappings.size(), 3);
    EXPECT_EQ(value_mapping.child_mappings[0].table_column_name, "age");
    EXPECT_EQ(value_mapping.child_mappings[0].file_column_name, "age");
    EXPECT_EQ(*value_mapping.child_mappings[0].file_local_id, 1);
    EXPECT_EQ(value_mapping.child_mappings[1].table_column_name, "full_name");
    EXPECT_EQ(value_mapping.child_mappings[1].file_column_name, "name");
    EXPECT_EQ(*value_mapping.child_mappings[1].file_local_id, 0);
    EXPECT_EQ(value_mapping.child_mappings[2].table_column_name, "gender");
    expect_missing(value_mapping.child_mappings[2]);
    EXPECT_FALSE(value_mapping.is_trivial);
}

// Scenario: Iceberg uses field-id mapping, but a key-only map projection may force the mapper to
// synthesize the missing value struct from DataType names, which do not carry field ids. The mapper
// must name-match synthesized children before ordinal fallback, otherwise `age` would read old
// file child `name` and the later materialization would build the value struct incorrectly.
TEST(ColumnMapperCreateMappingTest,
     KeyOnlyMapProjectionSynthesizedValueStructNameMatchesBeforeOrdinalFallback) {
    const auto int_type = i32();
    const auto string_type = str();
    const auto table_value_type = std::make_shared<DataTypeStruct>(
            DataTypes {int_type, string_type, string_type}, Strings {"age", "full_name", "gender"});
    const auto file_value_type = std::make_shared<DataTypeStruct>(DataTypes {string_type, int_type},
                                                                  Strings {"name", "age"});

    auto table_key = field_id_col("key", 10, string_type, 0);
    auto table_map = map_col("new_map_column", 2, {table_key}, string_type, table_value_type);

    auto file_key = field_id_col("key", 10, string_type, 0);
    auto file_name = field_id_col("name", 7, string_type, 0);
    auto file_age = field_id_col("age", 8, int_type, 1);
    auto file_value = struct_col("value", 11, {file_name, file_age}, 1);
    auto file_map =
            map_col("new_map_column", 2, {file_key, file_value}, string_type, file_value_type, 5);

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_FIELD_ID});
    ASSERT_TRUE(mapper.create_mapping({table_map}, {}, {file_map}).ok());

    ASSERT_EQ(mapper.mappings().size(), 1);
    const auto& map_mapping = mapper.mappings()[0];
    ASSERT_EQ(map_mapping.child_mappings.size(), 2);
    EXPECT_EQ(map_mapping.child_mappings[0].table_column_name, "key");
    EXPECT_EQ(map_mapping.child_mappings[0].file_column_name, "key");
    EXPECT_EQ(*map_mapping.child_mappings[0].file_local_id, 0);

    const auto& value_mapping = map_mapping.child_mappings[1];
    EXPECT_EQ(value_mapping.table_column_name, "value");
    EXPECT_EQ(value_mapping.file_column_name, "value");
    EXPECT_EQ(*value_mapping.file_local_id, 1);
    ASSERT_EQ(value_mapping.child_mappings.size(), 3);
    EXPECT_EQ(value_mapping.child_mappings[0].table_column_name, "age");
    EXPECT_EQ(value_mapping.child_mappings[0].file_column_name, "age");
    EXPECT_EQ(*value_mapping.child_mappings[0].file_local_id, 1);
    EXPECT_EQ(value_mapping.child_mappings[1].table_column_name, "full_name");
    EXPECT_EQ(value_mapping.child_mappings[1].file_column_name, "name");
    EXPECT_EQ(*value_mapping.child_mappings[1].file_local_id, 0);
    EXPECT_EQ(value_mapping.child_mappings[2].table_column_name, "gender");
    expect_missing(value_mapping.child_mappings[2]);
    EXPECT_FALSE(value_mapping.is_trivial);
}

TEST(ColumnMapperCreateMappingTest, ByFieldIdDoesNotFallbackToNameAndUsesFirstDuplicate) {
    const auto int_type = i32();
    const std::vector<ColumnDefinition> table_schema = {
            field_id_col("renamed", 10, int_type),
            name_col("same_name", int_type),
            field_id_col("negative", -7, int_type),
    };
    const std::vector<ColumnDefinition> file_schema = {
            field_id_col("first", 10, int_type, 0),
            field_id_col("second", 10, int_type, 1),
            field_id_col("same_name", 99, int_type, 2),
            field_id_col("negative_file", -7, int_type, 3),
    };

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_FIELD_ID});
    ASSERT_TRUE(mapper.create_mapping(table_schema, {}, file_schema).ok());

    ASSERT_EQ(mapper.mappings().size(), 3);
    expect_mapping(mapper.mappings()[0], 0, "renamed", 0, "first", int_type, int_type);
    expect_missing(mapper.mappings()[1]);
    expect_mapping(mapper.mappings()[2], 2, "negative", 3, "negative_file", int_type, int_type);
}

// Scenario: Iceberg TopN lazy materialization uses BY_FIELD_ID for schema evolution and also asks
// the file reader to synthesize GLOBAL_ROWID. GLOBAL_ROWID is matched by ColumnType before the
// field-id matcher, so keeping BY_FIELD_ID does not make the mapper look for a numeric field id for
// that virtual column.
TEST(ColumnMapperCreateMappingTest, ByFieldIdMapsGlobalRowIdByVirtualColumnType) {
    const auto int_type = i32();
    auto table_rowid = global_rowid_column_definition();
    table_rowid.name = BeConsts::GLOBAL_ROWID_COL + "equality_delete_par_1";
    table_rowid.identifier = Field::create_field<TYPE_STRING>(table_rowid.name);

    const std::vector<ColumnDefinition> table_schema = {
            field_id_col("new_new_id", 1, int_type),
            table_rowid,
    };
    const std::vector<ColumnDefinition> file_schema = {
            field_id_col("id", 1, int_type, 0),
            global_rowid_column_definition(),
    };

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_FIELD_ID});
    ASSERT_TRUE(mapper.create_mapping(table_schema, {}, file_schema).ok());

    ASSERT_EQ(mapper.mappings().size(), 2);
    expect_mapping(mapper.mappings()[0], 0, "new_new_id", 0, "id", int_type, int_type);
    expect_mapping(mapper.mappings()[1], 1, table_rowid.name, GLOBAL_ROWID_COLUMN_ID,
                   BeConsts::GLOBAL_ROWID_COL, str(), str());
}

TEST(ColumnMapperCreateMappingTest, ByFieldIdTreatsSameNameDifferentFieldIdAsMissing) {
    const auto int_type = i32();
    const std::vector<ColumnDefinition> table_schema = {
            field_id_col("same_name", 10, int_type),
    };
    const std::vector<ColumnDefinition> file_schema = {
            field_id_col("same_name", 20, int_type, 0),
    };

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_FIELD_ID});
    const auto status = mapper.create_mapping(table_schema, {}, file_schema);
    ASSERT_TRUE(status.ok()) << status.to_string();

    ASSERT_EQ(mapper.mappings().size(), 1);
    expect_missing(mapper.mappings()[0]);
}

TEST(ColumnMapperCreateMappingTest, NestedFieldIdTreatsSameNameDifferentFieldIdAsMissing) {
    const auto int_type = i32();
    auto table_child = field_id_col("child", 10, int_type);
    auto table_root = struct_col("root", 1, {table_child});

    auto file_child = field_id_col("child", 20, int_type, 0);
    auto file_root = struct_col("root", 1, {file_child}, 0);

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_FIELD_ID});
    const auto status = mapper.create_mapping({table_root}, {}, {file_root});
    ASSERT_TRUE(status.ok()) << status.to_string();

    ASSERT_EQ(mapper.mappings().size(), 1);
    expect_mapping(mapper.mappings()[0], 0, "root", 0, "root", file_root.type, table_root.type);
    ASSERT_EQ(mapper.mappings()[0].child_mappings.size(), 1);
    expect_missing(mapper.mappings()[0].child_mappings[0]);
}

TEST(ColumnMapperCreateMappingTest, ByIndexMapsTopLevelColumnsByPositionIgnoringFileNames) {
    const auto int_type = i32();
    const auto string_type = str();
    const std::vector<ColumnDefinition> table_schema = {
            position_col("user_id", 0, int_type),
            position_col("user_name", 1, string_type),
            position_col("age", 2, int_type),
    };
    const std::vector<ColumnDefinition> file_schema = {
            field_id_col("_col0", 100, int_type, 0),
            field_id_col("_col1", 101, string_type, 1),
            field_id_col("_col2", 102, int_type, 2),
    };

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_INDEX});
    ASSERT_TRUE(mapper.create_mapping(table_schema, {}, file_schema).ok());

    ASSERT_EQ(mapper.mappings().size(), 3);
    expect_mapping(mapper.mappings()[0], 0, "user_id", 0, "_col0", int_type, int_type);
    expect_mapping(mapper.mappings()[1], 1, "user_name", 1, "_col1", string_type, string_type);
    expect_mapping(mapper.mappings()[2], 2, "age", 2, "_col2", int_type, int_type);
}

TEST(ColumnMapperCreateMappingTest, ByIndexSupportsSparseProjection) {
    const auto int_type = i32();
    const std::vector<ColumnDefinition> table_schema = {
            position_col("age", 2, int_type),
            position_col("score", 4, int_type),
    };
    const std::vector<ColumnDefinition> file_schema = {
            field_id_col("_col0", 100, int_type, 0), field_id_col("_col1", 101, int_type, 1),
            field_id_col("_col2", 102, int_type, 2), field_id_col("_col3", 103, int_type, 3),
            field_id_col("_col4", 104, int_type, 4),
    };

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_INDEX});
    ASSERT_TRUE(mapper.create_mapping(table_schema, {}, file_schema).ok());

    ASSERT_EQ(mapper.mappings().size(), 2);
    expect_mapping(mapper.mappings()[0], 0, "age", 2, "_col2", int_type, int_type);
    expect_mapping(mapper.mappings()[1], 1, "score", 4, "_col4", int_type, int_type);
}

TEST(ColumnMapperCreateMappingTest,
     ByIndexMatchesNestedStructChildrenByNameEvenWhenChildrenHaveFieldIds) {
    const auto int_type = i32();
    const auto string_type = str();
    // Hive positional mapping only applies to top-level columns. FE/history schema metadata can
    // still put field-id style integer identifiers on nested struct children. Those nested
    // identifiers must not be interpreted as file positions.
    auto table_root = struct_col("profile", 1,
                                 {
                                         field_id_col("id", 100, int_type),
                                         field_id_col("name", 101, string_type),
                                 });
    // Reverse the file child order so a wrong positional match either misses the child or reads
    // the wrong physical child. The expected mapping below proves the children are matched by name.
    auto file_root = struct_name_col("_col1",
                                     {
                                             name_col("name", string_type, 0),
                                             name_col("id", int_type, 1),
                                     },
                                     1);
    const std::vector<ColumnDefinition> table_schema = {table_root};
    const std::vector<ColumnDefinition> file_schema = {
            field_id_col("_col0", 1000, string_type, 0),
            file_root,
    };

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_INDEX});
    const auto status = mapper.create_mapping(table_schema, {}, file_schema);
    ASSERT_TRUE(status.ok()) << status.to_string();

    ASSERT_EQ(mapper.mappings().size(), 1);
    expect_mapping(mapper.mappings()[0], 0, "profile", 1, "_col1", file_root.type, table_root.type);
    ASSERT_EQ(mapper.mappings()[0].child_mappings.size(), 2);
    expect_mapping(mapper.mappings()[0].child_mappings[0], 0, "id", 1, "id", int_type, int_type);
    expect_mapping(mapper.mappings()[0].child_mappings[1], 0, "name", 0, "name", string_type,
                   string_type);
}

TEST(ColumnMapperCreateMappingTest, ByIndexNestedStructDoesNotUseChildOrdinalIdentifier) {
    const auto int_type = i32();
    const auto string_type = str();
    // This is the dangerous variant of the previous case: the nested integer identifiers happen
    // to be valid child ordinals. BY_INDEX must still ignore them below the top-level root.
    auto table_root = struct_col("profile", 1,
                                 {
                                         field_id_col("id", 0, int_type),
                                         field_id_col("name", 1, string_type),
                                 });
    // If the implementation uses child ordinal matching, id/name will be swapped here.
    auto file_root = struct_name_col("_col1",
                                     {
                                             name_col("name", string_type, 0),
                                             name_col("id", int_type, 1),
                                     },
                                     1);
    const std::vector<ColumnDefinition> table_schema = {table_root};
    const std::vector<ColumnDefinition> file_schema = {
            field_id_col("_col0", 1000, string_type, 0),
            file_root,
    };

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_INDEX});
    const auto status = mapper.create_mapping(table_schema, {}, file_schema);
    ASSERT_TRUE(status.ok()) << status.to_string();

    ASSERT_EQ(mapper.mappings().size(), 1);
    expect_mapping(mapper.mappings()[0], 0, "profile", 1, "_col1", file_root.type, table_root.type);
    ASSERT_EQ(mapper.mappings()[0].child_mappings.size(), 2);
    expect_mapping(mapper.mappings()[0].child_mappings[0], 0, "id", 1, "id", int_type, int_type);
    expect_mapping(mapper.mappings()[0].child_mappings[1], 0, "name", 0, "name", string_type,
                   string_type);
}

TEST(ColumnMapperCreateMappingTest, ByIndexArrayElementStructChildrenMatchByName) {
    const auto int_type = i32();
    const auto string_type = str();
    // The top-level ARRAY column is selected by file position. After that, ARRAY has a single
    // structural child, and the element STRUCT should use Hive's nested-by-name behavior.
    auto table_element = struct_col("element", 10,
                                    {
                                            field_id_col("id", 100, int_type),
                                            field_id_col("name", 101, string_type),
                                    });
    auto table_root = array_col("profiles", 1, table_element);
    // Reverse the element struct children to distinguish name matching from position matching.
    auto file_element = struct_name_col("element",
                                        {
                                                name_col("name", string_type, 0),
                                                name_col("id", int_type, 1),
                                        },
                                        0);
    auto file_root = array_col("_col1", 1001, file_element, 1);
    const std::vector<ColumnDefinition> table_schema = {table_root};
    const std::vector<ColumnDefinition> file_schema = {
            field_id_col("_col0", 1000, string_type, 0),
            file_root,
    };

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_INDEX});
    const auto status = mapper.create_mapping(table_schema, {}, file_schema);
    ASSERT_TRUE(status.ok()) << status.to_string();

    ASSERT_EQ(mapper.mappings().size(), 1);
    expect_mapping(mapper.mappings()[0], 0, "profiles", 1, "_col1", file_root.type,
                   table_root.type);
    ASSERT_EQ(mapper.mappings()[0].child_mappings.size(), 1);
    const auto& element_mapping = mapper.mappings()[0].child_mappings[0];
    expect_mapping(element_mapping, 0, "element", 0, "element", file_element.type,
                   table_element.type);
    ASSERT_EQ(element_mapping.child_mappings.size(), 2);
    expect_mapping(element_mapping.child_mappings[0], 0, "id", 1, "id", int_type, int_type);
    expect_mapping(element_mapping.child_mappings[1], 0, "name", 0, "name", string_type,
                   string_type);
}

TEST(ColumnMapperCreateMappingTest, ByIndexMapValueStructChildrenMatchByName) {
    const auto int_type = i32();
    const auto string_type = str();
    const auto key_type = str();
    // MAP key/value are structural children, so BY_INDEX should not reinterpret their nested
    // integer identifiers as arbitrary positions. The value STRUCT then follows name matching.
    auto table_key = field_id_col("key", 10, key_type);
    auto table_value = struct_col("value", 11,
                                  {
                                          field_id_col("id", 100, int_type),
                                          field_id_col("name", 101, string_type),
                                  });
    auto table_root = map_col("profiles", 1, {table_key, table_value}, key_type, table_value.type);
    auto file_key = name_col("key", key_type, 0);
    // Reverse value struct children. A positional nested match would produce name/id swapped.
    auto file_value = struct_name_col("value",
                                      {
                                              name_col("name", string_type, 0),
                                              name_col("id", int_type, 1),
                                      },
                                      1);
    auto file_root = map_col("_col1", 1001, {file_key, file_value}, key_type, file_value.type, 1);
    const std::vector<ColumnDefinition> table_schema = {table_root};
    const std::vector<ColumnDefinition> file_schema = {
            field_id_col("_col0", 1000, string_type, 0),
            file_root,
    };

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_INDEX});
    const auto status = mapper.create_mapping(table_schema, {}, file_schema);
    ASSERT_TRUE(status.ok()) << status.to_string();

    ASSERT_EQ(mapper.mappings().size(), 1);
    expect_mapping(mapper.mappings()[0], 0, "profiles", 1, "_col1", file_root.type,
                   table_root.type);
    ASSERT_EQ(mapper.mappings()[0].child_mappings.size(), 2);
    expect_mapping(mapper.mappings()[0].child_mappings[0], 0, "key", 0, "key", key_type, key_type);
    const auto& value_mapping = mapper.mappings()[0].child_mappings[1];
    expect_mapping(value_mapping, 0, "value", 1, "value", file_value.type, table_value.type);
    ASSERT_EQ(value_mapping.child_mappings.size(), 2);
    expect_mapping(value_mapping.child_mappings[0], 0, "id", 1, "id", int_type, int_type);
    expect_mapping(value_mapping.child_mappings[1], 0, "name", 0, "name", string_type, string_type);
}

TEST(ColumnMapperCreateMappingTest,
     ByIndexPartitionColumnsTakeConstantAndDoNotConsumeFilePosition) {
    const auto int_type = i32();
    const auto string_type = str();
    auto partition = name_col("dt", string_type);
    partition.is_partition_key = true;
    const std::vector<ColumnDefinition> table_schema = {
            partition,
            position_col("user_id", 0, int_type),
            position_col("score", 1, int_type),
    };
    const std::vector<ColumnDefinition> file_schema = {
            field_id_col("_col0", 100, int_type, 0),
            field_id_col("_col1", 101, int_type, 1),
    };

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_INDEX});
    ASSERT_TRUE(mapper.create_mapping(table_schema,
                                      {{"dt", Field::create_field<TYPE_STRING>("2026-06-11")}},
                                      file_schema)
                        .ok());

    ASSERT_EQ(mapper.mappings().size(), 3);
    expect_constant(mapper, mapper.mappings()[0], 0, string_type);
    expect_mapping(mapper.mappings()[1], 1, "user_id", 0, "_col0", int_type, int_type);
    expect_mapping(mapper.mappings()[2], 2, "score", 1, "_col1", int_type, int_type);
}

TEST(ColumnMapperCreateMappingTest, ByIndexOutOfRangeFallsBackToDefaultOrMissing) {
    const auto int_type = i32();
    auto with_default = position_col("extra_default", 5, int_type);
    const auto literal_expr =
            VExprContext::create_shared(literal(int_type, Field::create_field<TYPE_INT>(42)));
    with_default.default_expr = literal_expr;
    const std::vector<ColumnDefinition> table_schema = {
            position_col("a", 0, int_type),
            with_default,
            position_col("extra_missing", 99, int_type),
    };
    const std::vector<ColumnDefinition> file_schema = {
            field_id_col("_col0", 100, int_type, 0),
            field_id_col("_col1", 101, int_type, 1),
    };

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_INDEX});
    ASSERT_TRUE(mapper.create_mapping(table_schema, {}, file_schema).ok());

    ASSERT_EQ(mapper.mappings().size(), 3);
    expect_mapping(mapper.mappings()[0], 0, "a", 0, "_col0", int_type, int_type);
    expect_constant(mapper, mapper.mappings()[1], 1, int_type);
    EXPECT_EQ(mapper.mappings()[1].default_expr, literal_expr);
    expect_missing(mapper.mappings()[2]);
}

TEST(ColumnMapperCreateMappingTest, ByIndexMissingIdentifierFallsBackToDefaultOrMissing) {
    const auto int_type = i32();
    auto with_default = name_col("extra_default", int_type);
    const auto literal_expr =
            VExprContext::create_shared(literal(int_type, Field::create_field<TYPE_INT>(42)));
    with_default.default_expr = literal_expr;
    const std::vector<ColumnDefinition> table_schema = {
            position_col("a", 0, int_type),
            with_default,
            name_col("extra_missing", int_type),
    };
    const std::vector<ColumnDefinition> file_schema = {
            field_id_col("_col0", 100, int_type, 0),
    };

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_INDEX});
    ASSERT_TRUE(mapper.create_mapping(table_schema, {}, file_schema).ok());

    ASSERT_EQ(mapper.mappings().size(), 3);
    expect_mapping(mapper.mappings()[0], 0, "a", 0, "_col0", int_type, int_type);
    expect_constant(mapper, mapper.mappings()[1], 1, int_type);
    EXPECT_EQ(mapper.mappings()[1].default_expr, literal_expr);
    expect_missing(mapper.mappings()[2]);
}

TEST(ColumnMapperCreateMappingTest, ByIndexOutOfRangeFallsBackToMissing) {
    const auto int_type = i32();
    const std::vector<ColumnDefinition> table_schema = {
            position_col("a", 0, int_type),
            position_col("b", 5, int_type),
    };
    const std::vector<ColumnDefinition> file_schema = {
            field_id_col("_col0", 100, int_type, 0),
    };

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_INDEX});
    const auto status = mapper.create_mapping(table_schema, {}, file_schema);
    ASSERT_TRUE(status.ok()) << status.to_string();

    ASSERT_EQ(mapper.mappings().size(), 2);
    expect_mapping(mapper.mappings()[0], 0, "a", 0, "_col0", int_type, int_type);
    expect_missing(mapper.mappings()[1]);
}

TEST(ColumnMapperCreateMappingTest, ByIndexIgnoresExtraFileColumns) {
    const auto int_type = i32();
    const std::vector<ColumnDefinition> table_schema = {
            position_col("a", 0, int_type),
    };
    const std::vector<ColumnDefinition> file_schema = {
            field_id_col("_col0", 100, int_type, 0),
            field_id_col("_col1", 101, int_type, 1),
            field_id_col("_col2", 102, int_type, 2),
    };

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_INDEX});
    ASSERT_TRUE(mapper.create_mapping(table_schema, {}, file_schema).ok());

    ASSERT_EQ(mapper.mappings().size(), 1);
    expect_mapping(mapper.mappings()[0], 0, "a", 0, "_col0", int_type, int_type);
}

TEST(ColumnMapperCreateMappingTest, ByIndexIgnoresFileColumnNames) {
    const auto int_type = i32();
    const std::vector<ColumnDefinition> table_schema = {
            position_col("a", 1, int_type),
    };
    const std::vector<ColumnDefinition> file_schema = {
            field_id_col("a", 100, int_type, 10),
            field_id_col("b", 101, int_type, 20),
    };

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_INDEX});
    ASSERT_TRUE(mapper.create_mapping(table_schema, {}, file_schema).ok());

    ASSERT_EQ(mapper.mappings().size(), 1);
    expect_mapping(mapper.mappings()[0], 0, "a", 20, "b", int_type, int_type);
}

TEST(ColumnMapperCreateMappingTest, MissingColumnFallsBackToMissingMapping) {
    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    const auto status = mapper.create_mapping({name_col("missing", i32())}, {},
                                              {name_col("present", i32(), 0)});
    ASSERT_TRUE(status.ok()) << status.to_string();

    ASSERT_EQ(mapper.mappings().size(), 1);
    expect_missing(mapper.mappings()[0]);
}

// ----------------------------------------------------------------------
// L1 constants and virtual columns.
// These tests verify non-file-backed mappings before TableReader materializes
// their final values.
// ----------------------------------------------------------------------

TEST(ColumnMapperConstantTest, PartitionDefaultAndVirtualColumnsUseDedicatedBranches) {
    auto partition_column = name_col("dt", str());
    partition_column.is_partition_key = true;

    auto default_column = name_col("new_value", i32());
    default_column.default_expr =
            VExprContext::create_shared(literal(i32(), Field::create_field<TYPE_INT>(42)));

    auto row_id_column = name_col("_row_id", make_nullable(i64()));
    auto sequence_column = name_col("_last_updated_sequence_number", make_nullable(i64()));
    auto iceberg_rowid_column = name_col(BeConsts::ICEBERG_ROWID_COL, str());

    const std::vector<ColumnDefinition> table_schema = {
            partition_column, default_column, row_id_column, sequence_column, iceberg_rowid_column};
    const std::map<std::string, Field> partition_values = {
            {"dt", Field::create_field<TYPE_STRING>("2026-06-11")},
    };

    TableColumnMapper mapper(
            {.mode = TableColumnMappingMode::BY_NAME, .enable_row_lineage_virtual_columns = true});
    ASSERT_TRUE(mapper.create_mapping(table_schema, partition_values, {}).ok());

    ASSERT_EQ(mapper.mappings().size(), 5);
    expect_constant(mapper, mapper.mappings()[0], 0, str());
    expect_constant(mapper, mapper.mappings()[1], 1, i32());
    EXPECT_EQ(mapper.mappings()[2].virtual_column_type, TableVirtualColumnType::ROW_ID);
    EXPECT_EQ(mapper.mappings()[3].virtual_column_type,
              TableVirtualColumnType::LAST_UPDATED_SEQUENCE_NUMBER);
    EXPECT_EQ(mapper.mappings()[4].virtual_column_type, TableVirtualColumnType::ICEBERG_ROWID);
}

TEST(ColumnMapperConstantTest, PhysicalRowLineageFiltersStayFinalizeOnly) {
    auto row_id_column = name_col("_row_id", make_nullable(i64()));
    auto sequence_column = name_col("_last_updated_sequence_number", make_nullable(i64()));
    const std::vector<ColumnDefinition> table_schema = {row_id_column, sequence_column};
    const std::vector<ColumnDefinition> file_schema = {
            name_col("_row_id", make_nullable(i64()), 2147483540),
            name_col("_last_updated_sequence_number", make_nullable(i64()), 2147483539),
    };

    TableColumnMapper mapper(
            {.mode = TableColumnMappingMode::BY_NAME, .enable_row_lineage_virtual_columns = true});
    ASSERT_TRUE(mapper.create_mapping(table_schema, {}, file_schema).ok());

    ASSERT_EQ(mapper.mappings().size(), 2);
    EXPECT_EQ(mapper.mappings()[0].virtual_column_type, TableVirtualColumnType::ROW_ID);
    EXPECT_EQ(mapper.mappings()[0].filter_conversion, FilterConversionType::FINALIZE_ONLY);
    EXPECT_EQ(mapper.mappings()[1].virtual_column_type,
              TableVirtualColumnType::LAST_UPDATED_SEQUENCE_NUMBER);
    EXPECT_EQ(mapper.mappings()[1].filter_conversion, FilterConversionType::FINALIZE_ONLY);

    auto row_id_filter =
            binary_predicate(TExprOpcode::EQ, table_slot(0, 0, make_nullable(i64()), "_row_id"),
                             literal(i64(), Field::create_field<TYPE_BIGINT>(1001)));
    auto sequence_filter = binary_predicate(
            TExprOpcode::EQ,
            table_slot(1, 1, make_nullable(i64()), "_last_updated_sequence_number"),
            literal(i64(), Field::create_field<TYPE_BIGINT>(77)));
    TableFilter row_id_table_filter {.conjunct = VExprContext::create_shared(row_id_filter),
                                     .global_indices = {GlobalIndex(0)}};
    TableFilter sequence_table_filter {.conjunct = VExprContext::create_shared(sequence_filter),
                                       .global_indices = {GlobalIndex(1)}};

    FileScanRequest request;
    ASSERT_TRUE(mapper.create_scan_request({row_id_table_filter, sequence_table_filter},
                                           table_schema, &request)
                        .ok());

    EXPECT_TRUE(request.conjuncts.empty());
    EXPECT_TRUE(request.predicate_columns.empty());
    EXPECT_EQ(projection_ids(request.non_predicate_columns),
              std::vector<int32_t>({2147483540, 2147483539}));
}

TEST(ColumnMapperConstantTest, GenericByNameKeepsRowLineageNamesPhysical) {
    const std::vector<ColumnDefinition> table_schema = {
            name_col("_row_id", make_nullable(i64())),
            name_col("_last_updated_sequence_number", make_nullable(i64())),
    };
    const std::vector<ColumnDefinition> file_schema = {
            name_col("_row_id", make_nullable(i64()), 0),
            name_col("_last_updated_sequence_number", make_nullable(i64()), 1),
    };

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    ASSERT_TRUE(mapper.create_mapping(table_schema, {}, file_schema).ok());
    ASSERT_EQ(mapper.mappings().size(), 2);
    EXPECT_EQ(mapper.mappings()[0].virtual_column_type, TableVirtualColumnType::INVALID);
    EXPECT_EQ(mapper.mappings()[0].filter_conversion, FilterConversionType::COPY_DIRECTLY);
    EXPECT_EQ(mapper.mappings()[1].virtual_column_type, TableVirtualColumnType::INVALID);
    EXPECT_EQ(mapper.mappings()[1].filter_conversion, FilterConversionType::COPY_DIRECTLY);
}

TEST(ColumnMapperConstantTest, MissingRowLineageDefaultExprStillUsesVirtualMapping) {
    auto id_column = field_id_col("id", 1, make_nullable(i32()));
    auto row_id_column = field_id_col("renamed_row_id", 2147483540, make_nullable(i64()));
    row_id_column.default_expr = VExprContext::create_shared(
            literal(make_nullable(i64()), Field::create_field<TYPE_BIGINT>(0)));
    auto sequence_column =
            field_id_col("renamed_last_updated_sequence_number", 2147483539, make_nullable(i64()));
    sequence_column.default_expr = VExprContext::create_shared(
            literal(make_nullable(i64()), Field::create_field<TYPE_BIGINT>(0)));

    const std::vector<ColumnDefinition> table_schema = {id_column, row_id_column, sequence_column};
    const std::vector<ColumnDefinition> file_schema = {
            field_id_col("id", 1, make_nullable(i32()), 0),
            field_id_col("name", 2, make_nullable(str()), 1),
    };

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_FIELD_ID,
                              .enable_row_lineage_virtual_columns = true});
    ASSERT_TRUE(mapper.create_mapping(table_schema, {}, file_schema).ok());

    ASSERT_EQ(mapper.mappings().size(), 3);
    expect_mapping(mapper.mappings()[0], 0, "id", 0, "id", make_nullable(i32()),
                   make_nullable(i32()));
    EXPECT_EQ(mapper.mappings()[1].virtual_column_type, TableVirtualColumnType::ROW_ID);
    EXPECT_FALSE(mapper.mappings()[1].constant_index.has_value());
    EXPECT_EQ(mapper.mappings()[2].virtual_column_type,
              TableVirtualColumnType::LAST_UPDATED_SEQUENCE_NUMBER);
    EXPECT_FALSE(mapper.mappings()[2].constant_index.has_value());
    EXPECT_TRUE(mapper.constant_map().empty());
}

TEST(ColumnMapperConstantTest, ByFieldIdDoesNotTreatSameNameDifferentIdAsRowLineage) {
    const std::vector<ColumnDefinition> table_schema = {
            field_id_col("_row_id", 100, make_nullable(i64())),
            field_id_col("_last_updated_sequence_number", 101, make_nullable(i64())),
    };
    const std::vector<ColumnDefinition> file_schema = {
            field_id_col("_row_id", 100, make_nullable(i64()), 0),
            field_id_col("_last_updated_sequence_number", 101, make_nullable(i64()), 1),
    };

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_FIELD_ID});
    ASSERT_TRUE(mapper.create_mapping(table_schema, {}, file_schema).ok());

    ASSERT_EQ(mapper.mappings().size(), 2);
    expect_mapping(mapper.mappings()[0], 0, "_row_id", 0, "_row_id", make_nullable(i64()),
                   make_nullable(i64()));
    EXPECT_EQ(mapper.mappings()[0].virtual_column_type, TableVirtualColumnType::INVALID);
    EXPECT_EQ(mapper.mappings()[0].filter_conversion, FilterConversionType::COPY_DIRECTLY);
    expect_mapping(mapper.mappings()[1], 1, "_last_updated_sequence_number", 1,
                   "_last_updated_sequence_number", make_nullable(i64()), make_nullable(i64()));
    EXPECT_EQ(mapper.mappings()[1].virtual_column_type, TableVirtualColumnType::INVALID);
    EXPECT_EQ(mapper.mappings()[1].filter_conversion, FilterConversionType::COPY_DIRECTLY);
}

TEST(ColumnMapperConstantTest, PartitionAliasResolvesRenamedValue) {
    auto partition_column = name_col("current_dt", str());
    partition_column.name_mapping = {"legacy_dt"};
    partition_column.is_partition_key = true;

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    ASSERT_TRUE(mapper.create_mapping(
                              {partition_column},
                              {{"legacy_dt", Field::create_field<TYPE_STRING>("2026-06-11")}}, {})
                        .ok());

    ASSERT_EQ(mapper.mappings().size(), 1);
    expect_constant(mapper, mapper.mappings()[0], 0, str());
}

TEST(ColumnMapperConstantTest, PartitionConstantFilterEntryDoesNotReadFileColumns) {
    auto partition_column = name_col("part", i32());
    partition_column.is_partition_key = true;

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    ASSERT_TRUE(mapper.create_mapping({partition_column},
                                      {{"part", Field::create_field<TYPE_INT>(7)}}, {})
                        .ok());

    TableFilter filter {
            .conjunct = VExprContext::create_shared(int_gt(table_slot(0, 0, i32(), "part"), 1)),
            .global_indices = {GlobalIndex(0)}};

    FileScanRequest request;
    ASSERT_TRUE(mapper.create_scan_request({filter}, {partition_column}, &request).ok());

    ASSERT_EQ(mapper.filter_entries().size(), 1);
    ASSERT_TRUE(mapper.filter_entries().at(GlobalIndex(0)).is_constant());
    EXPECT_EQ(mapper.filter_entries().at(GlobalIndex(0)).constant_index(),
              *mapper.mappings()[0].constant_index);
    EXPECT_TRUE(request.local_positions.empty());
    EXPECT_TRUE(request.predicate_columns.empty());
    EXPECT_TRUE(request.non_predicate_columns.empty());
    EXPECT_TRUE(request.conjuncts.empty());
}

TEST(ColumnMapperConstantTest, DefaultConstantFilterEntryUsesDefaultExpression) {
    auto default_column = name_col("new_value", i32());
    default_column.default_expr =
            VExprContext::create_shared(literal(i32(), Field::create_field<TYPE_INT>(42)));

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    ASSERT_TRUE(mapper.create_mapping({default_column}, {}, {}).ok());

    TableFilter filter {.conjunct = VExprContext::create_shared(
                                int_gt(table_slot(0, 0, i32(), "new_value"), 1)),
                        .global_indices = {GlobalIndex(0)}};

    FileScanRequest request;
    ASSERT_TRUE(mapper.create_scan_request({filter}, {default_column}, &request).ok());

    ASSERT_EQ(mapper.filter_entries().size(), 1);
    ASSERT_TRUE(mapper.filter_entries().at(GlobalIndex(0)).is_constant());
    const auto constant_index = mapper.filter_entries().at(GlobalIndex(0)).constant_index();
    EXPECT_EQ(constant_index, *mapper.mappings()[0].constant_index);
    EXPECT_EQ(mapper.constant_map().get(constant_index).expr, default_column.default_expr);
    EXPECT_TRUE(request.local_positions.empty());
    EXPECT_TRUE(request.predicate_columns.empty());
    EXPECT_TRUE(request.non_predicate_columns.empty());
    EXPECT_TRUE(request.conjuncts.empty());
}

TEST(ColumnMapperConstantTest, MixedConstantAndFileFilterKeepsOnlyFileScanColumn) {
    auto partition_column = name_col("part", i32());
    partition_column.is_partition_key = true;
    const auto file_column = name_col("score", i32(), 3);
    const std::vector<ColumnDefinition> table_schema = {partition_column, file_column};
    const std::vector<ColumnDefinition> file_schema = {file_column};

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    ASSERT_TRUE(mapper.create_mapping(table_schema, {{"part", Field::create_field<TYPE_INT>(7)}},
                                      file_schema)
                        .ok());

    TableFilter constant_filter {
            .conjunct = VExprContext::create_shared(int_gt(table_slot(0, 0, i32(), "part"), 1)),
            .global_indices = {GlobalIndex(0)}};
    TableFilter file_filter {
            .conjunct = VExprContext::create_shared(int_gt(table_slot(1, 1, i32(), "score"), 10)),
            .global_indices = {GlobalIndex(1)}};

    FileScanRequest request;
    ASSERT_TRUE(mapper.create_scan_request({constant_filter, file_filter}, table_schema, &request)
                        .ok());

    ASSERT_EQ(mapper.filter_entries().size(), 2);
    ASSERT_TRUE(mapper.filter_entries().at(GlobalIndex(0)).is_constant());
    ASSERT_TRUE(mapper.filter_entries().at(GlobalIndex(1)).is_local());
    EXPECT_EQ(mapper.filter_entries().at(GlobalIndex(1)).local_index(), LocalIndex(0));
    ASSERT_EQ(request.predicate_columns.size(), 1);
    EXPECT_EQ(request.predicate_columns[0].column_id(), LocalColumnId(3));
    EXPECT_TRUE(request.non_predicate_columns.empty());
}

// ----------------------------------------------------------------------
// L1 direct filter localization tests.
// These tests call localize_filters() directly to pin the core interface
// contract apart from create_scan_request() initialization.
// ----------------------------------------------------------------------

TEST(ColumnMapperLocalizeFiltersTest, VisibleLocalFilterAddsPredicateColumnAndConjunct) {
    const auto int_type = i32();
    const std::vector<ColumnDefinition> table_schema = {name_col("id", int_type)};
    const std::vector<ColumnDefinition> file_schema = {name_col("id", int_type, 7)};

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    ASSERT_TRUE(mapper.create_mapping(table_schema, {}, file_schema).ok());

    TableFilter filter {.conjunct = VExprContext::create_shared(table_slot(11, 0, int_type, "id")),
                        .global_indices = {GlobalIndex(0)}};

    FileScanRequest request;
    ASSERT_TRUE(mapper.localize_filters({filter}, &request).ok());

    EXPECT_TRUE(request.non_predicate_columns.empty());
    ASSERT_EQ(request.predicate_columns.size(), 1);
    EXPECT_EQ(request.predicate_columns[0].column_id(), LocalColumnId(7));
    ASSERT_EQ(request.local_positions.size(), 1);
    EXPECT_EQ(request.local_positions.at(LocalColumnId(7)), LocalIndex(0));
    ASSERT_EQ(mapper.filter_entries().size(), 1);
    ASSERT_TRUE(mapper.filter_entries().at(GlobalIndex(0)).is_local());
    EXPECT_EQ(mapper.filter_entries().at(GlobalIndex(0)).local_index(), LocalIndex(0));

    ASSERT_EQ(request.conjuncts.size(), 1);
    const auto* localized_slot = assert_cast<const VSlotRef*>(request.conjuncts[0]->root().get());
    EXPECT_EQ(localized_slot->slot_id(), 11);
    EXPECT_EQ(localized_slot->column_id(), 0);
    EXPECT_EQ(localized_slot->column_name(), "id");
    EXPECT_TRUE(localized_slot->data_type()->equals(*int_type));
}

TEST(ColumnMapperLocalizeFiltersTest, ReportsLocalizationForEachSplitMapping) {
    const auto int_type = i32();
    auto table_column = name_col("id", int_type);
    const std::vector<ColumnDefinition> table_schema = {table_column};
    TableFilter filter {
            .conjunct = VExprContext::create_shared(int_gt(table_slot(0, 0, int_type, "id"), 1)),
            .global_indices = {GlobalIndex(0)}};

    TableColumnMapper local_mapper({.mode = TableColumnMappingMode::BY_NAME});
    ASSERT_TRUE(local_mapper.create_mapping(table_schema, {}, {name_col("id", int_type, 7)}).ok());
    FileScanRequest local_request;
    FilterLocalizationResult local_result;
    ASSERT_TRUE(local_mapper
                        .create_scan_request({filter}, table_schema, &local_request, nullptr,
                                             &local_result)
                        .ok());
    ASSERT_EQ(local_result.localized_filters.size(), 1);
    EXPECT_TRUE(local_result.localized_filters[0]);
    ASSERT_EQ(local_request.conjuncts.size(), 1);

    TableColumnMapper missing_mapper({.mode = TableColumnMappingMode::BY_NAME});
    ASSERT_TRUE(missing_mapper.create_mapping(table_schema, {}, {}).ok());
    FileScanRequest missing_request;
    FilterLocalizationResult missing_result;
    ASSERT_TRUE(missing_mapper
                        .create_scan_request({filter}, table_schema, &missing_request, nullptr,
                                             &missing_result)
                        .ok());
    ASSERT_EQ(missing_result.localized_filters.size(), 1);
    EXPECT_FALSE(missing_result.localized_filters[0]);
    EXPECT_TRUE(missing_request.conjuncts.empty());
}

TEST(ColumnMapperLocalizeFiltersTest, VarbinaryFilterStaysAboveFileReader) {
    const auto binary_type = varbinary();
    const auto table_column = name_col("partition_key", binary_type);
    const auto file_column = name_col("partition_key", binary_type, 7);

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    ASSERT_TRUE(mapper.create_mapping({table_column}, {}, {file_column}).ok());
    ASSERT_EQ(mapper.mappings().size(), 1);
    EXPECT_TRUE(mapper.mappings()[0].is_trivial);
    EXPECT_EQ(mapper.mappings()[0].filter_conversion, FilterConversionType::FINALIZE_ONLY);

    const auto value = Field::create_field<TYPE_VARBINARY>(StringView("binary-value"));
    TableFilter filter {.conjunct = VExprContext::create_shared(binary_predicate(
                                TExprOpcode::EQ, table_slot(0, 0, binary_type, "partition_key"),
                                literal(binary_type, value))),
                        .global_indices = {GlobalIndex(0)}};

    FileScanRequest request;
    ASSERT_TRUE(mapper.create_scan_request({filter}, {table_column}, &request).ok());
    EXPECT_TRUE(request.predicate_columns.empty());
    ASSERT_EQ(request.non_predicate_columns.size(), 1);
    EXPECT_EQ(request.non_predicate_columns[0].column_id(), LocalColumnId(7));
    EXPECT_TRUE(request.conjuncts.empty());
}

TEST(ColumnMapperLocalizeFiltersTest, VarcharWidthTruncationFilterStaysAboveFileReader) {
    const auto table_type = std::make_shared<DataTypeString>(3, TYPE_VARCHAR);
    const auto file_type = std::make_shared<DataTypeString>(10, TYPE_VARCHAR);
    const auto table_column = name_col("value", table_type);
    const auto file_column = name_col("value", file_type, 7);

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    ASSERT_TRUE(mapper.create_mapping({table_column}, {}, {file_column}).ok());

    TableFilter filter {.conjunct = VExprContext::create_shared(binary_predicate(
                                TExprOpcode::EQ, table_slot(0, 0, table_type, "value"),
                                literal(table_type, Field::create_field<TYPE_STRING>("abc")))),
                        .global_indices = {GlobalIndex(0)}};
    TQueryOptions query_options;
    query_options.__set_truncate_char_or_varchar_columns(true);
    RuntimeState state {query_options, TQueryGlobals()};
    FileScanRequest request;
    FilterLocalizationResult localization_result;

    ASSERT_TRUE(mapper.create_scan_request({filter}, {table_column}, &request, &state,
                                           &localization_result)
                        .ok());
    ASSERT_EQ(localization_result.localized_filters.size(), 1);
    EXPECT_FALSE(localization_result.localized_filters[0]);
    EXPECT_TRUE(request.conjuncts.empty());
    ASSERT_EQ(request.non_predicate_columns.size(), 1);
    EXPECT_EQ(request.non_predicate_columns[0].column_id(), LocalColumnId(7));
}

TEST(ColumnMapperLocalizeFiltersTest, NestedVarbinaryFilterStaysAboveFileReader) {
    const auto table_column = struct_name_col(
            "payload", {name_col("id", i32()), name_col("binary_value", varbinary())});
    const auto file_column = struct_name_col(
            "payload", {name_col("id", i32(), 0), name_col("binary_value", varbinary(), 1)}, 7);

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    ASSERT_TRUE(mapper.create_mapping({table_column}, {}, {file_column}).ok());
    ASSERT_EQ(mapper.mappings().size(), 1);
    EXPECT_EQ(mapper.mappings()[0].filter_conversion, FilterConversionType::FINALIZE_ONLY);

    TableFilter filter {
            .conjunct = VExprContext::create_shared(table_slot(0, 0, table_column.type, "payload")),
            .global_indices = {GlobalIndex(0)}};
    FileScanRequest request;
    ASSERT_TRUE(mapper.create_scan_request({filter}, {table_column}, &request).ok());
    EXPECT_TRUE(request.predicate_columns.empty());
    ASSERT_EQ(request.non_predicate_columns.size(), 1);
    EXPECT_EQ(request.non_predicate_columns[0].column_id(), LocalColumnId(7));
    EXPECT_TRUE(request.conjuncts.empty());
}

TEST(ColumnMapperLocalizeFiltersTest, ConstantFilterBuildsEntryWithoutFileScanColumn) {
    auto partition_column = name_col("part", i32());
    partition_column.is_partition_key = true;

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    ASSERT_TRUE(mapper.create_mapping({partition_column},
                                      {{"part", Field::create_field<TYPE_INT>(7)}}, {})
                        .ok());

    TableFilter filter {.conjunct = VExprContext::create_shared(table_slot(3, 0, i32(), "part")),
                        .global_indices = {GlobalIndex(0)}};

    FileScanRequest request;
    ASSERT_TRUE(mapper.localize_filters({filter}, &request).ok());

    EXPECT_TRUE(request.predicate_columns.empty());
    EXPECT_TRUE(request.non_predicate_columns.empty());
    EXPECT_TRUE(request.local_positions.empty());
    EXPECT_TRUE(request.conjuncts.empty());
    ASSERT_EQ(mapper.filter_entries().size(), 1);
    ASSERT_TRUE(mapper.filter_entries().at(GlobalIndex(0)).is_constant());
    EXPECT_EQ(mapper.filter_entries().at(GlobalIndex(0)).constant_index(),
              mapper.mappings()[0].constant_index);
}

TEST(ColumnMapperLocalizeFiltersTest, NestedFilterOnlyChildMergesIntoPredicateProjection) {
    const auto int_type = i32();
    const auto string_type = str();

    auto table_a = name_col("a", int_type);
    auto table_b = name_col("b", string_type);
    auto table_struct = struct_name_col("s", {table_b});
    auto full_table_struct = struct_name_col("s", {table_a, table_b});

    auto file_a = name_col("a", int_type, 0);
    auto file_b = name_col("b", string_type, 1);
    auto file_struct = struct_name_col("s", {file_a, file_b}, 5);

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    ASSERT_TRUE(mapper.create_mapping({table_struct}, {}, {file_struct}).ok());

    auto filter_expr = int_gt(
            struct_element(table_slot(0, 0, full_table_struct.type, "s"), int_type, "a"), 10);
    TableFilter filter {.conjunct = VExprContext::create_shared(filter_expr),
                        .global_indices = {GlobalIndex(0)}};

    FileScanRequest request;
    ASSERT_TRUE(mapper.localize_filters({filter}, &request).ok());

    EXPECT_TRUE(request.non_predicate_columns.empty());
    ASSERT_EQ(request.predicate_columns.size(), 1);
    EXPECT_EQ(request.predicate_columns[0].column_id(), LocalColumnId(5));
    ASSERT_FALSE(request.predicate_columns[0].project_all_children);
    EXPECT_EQ(projection_ids(request.predicate_columns[0].children), std::vector<int32_t>({0, 1}));
    ASSERT_EQ(request.local_positions.size(), 1);
    EXPECT_EQ(request.local_positions.at(LocalColumnId(5)), LocalIndex(0));
    ASSERT_TRUE(mapper.filter_entries().at(GlobalIndex(0)).is_local());
    EXPECT_EQ(mapper.filter_entries().at(GlobalIndex(0)).local_index(), LocalIndex(0));
}

TEST(ColumnMapperLocalizeFiltersTest, PreservesExistingScanStateWhenAddingPredicateColumn) {
    const auto int_type = i32();
    const std::vector<ColumnDefinition> table_schema = {
            name_col("id", int_type),
            name_col("score", int_type),
    };
    const std::vector<ColumnDefinition> file_schema = {
            name_col("id", int_type, 3),
            name_col("score", int_type, 4),
    };

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    ASSERT_TRUE(mapper.create_mapping(table_schema, {}, file_schema).ok());

    TableFilter filter {.conjunct = VExprContext::create_shared(table_slot(2, 0, int_type, "id")),
                        .global_indices = {GlobalIndex(0)}};

    FileScanRequest request;
    request.non_predicate_columns.push_back(LocalColumnIndex::top_level(LocalColumnId(4)));
    request.local_positions.emplace(LocalColumnId(4), LocalIndex(0));
    ASSERT_TRUE(mapper.localize_filters({filter}, &request).ok());

    ASSERT_EQ(request.non_predicate_columns.size(), 1);
    EXPECT_EQ(request.non_predicate_columns[0].column_id(), LocalColumnId(4));
    ASSERT_EQ(request.predicate_columns.size(), 1);
    EXPECT_EQ(request.predicate_columns[0].column_id(), LocalColumnId(3));
    ASSERT_EQ(request.local_positions.size(), 2);
    EXPECT_EQ(request.local_positions.at(LocalColumnId(4)), LocalIndex(0));
    EXPECT_EQ(request.local_positions.at(LocalColumnId(3)), LocalIndex(1));
    ASSERT_TRUE(mapper.filter_entries().at(GlobalIndex(0)).is_local());
    EXPECT_EQ(mapper.filter_entries().at(GlobalIndex(0)).local_index(), LocalIndex(1));
}

// ----------------------------------------------------------------------
// L1 scan request and filter localization tests.
// These tests assert predicate/non-predicate split, local positions, hidden
// filter mappings, and nested predicate targets.
// ----------------------------------------------------------------------

TEST(ColumnMapperScanRequestTest, HiddenTopLevelFilterMappingUsesNameFallback) {
    const auto int_type = i32();
    const std::vector<ColumnDefinition> table_schema = {
            field_id_col("id", 1, int_type),
    };
    const std::vector<ColumnDefinition> file_schema = {
            field_id_col("id", 1, int_type, 0),
            field_id_col("score", 2, int_type, 1),
    };

    auto filter_expr = int_gt(table_slot(7, 1, int_type, "score"), 10);
    TableFilter filter {.conjunct = VExprContext::create_shared(filter_expr),
                        .global_indices = {GlobalIndex(1)}};

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_FIELD_ID});
    ASSERT_TRUE(mapper.create_mapping(table_schema, {}, file_schema).ok());

    FileScanRequest request;
    ASSERT_TRUE(mapper.create_scan_request({filter}, table_schema, &request).ok());

    ASSERT_EQ(request.non_predicate_columns.size(), 1);
    EXPECT_EQ(request.non_predicate_columns[0].column_id(), LocalColumnId(0));
    ASSERT_EQ(request.predicate_columns.size(), 1);
    EXPECT_EQ(request.predicate_columns[0].column_id(), LocalColumnId(1));
    EXPECT_EQ(request.predicate_only_columns, std::vector<LocalColumnId>({LocalColumnId(1)}));
    ASSERT_TRUE(mapper.filter_entries().at(GlobalIndex(1)).is_local());
    EXPECT_EQ(mapper.filter_entries().at(GlobalIndex(1)).local_index(), LocalIndex(1));
}

TEST(ColumnMapperScanRequestTest, OrdinaryPredicateSlotRetainsOutputPayload) {
    const auto int_type = i32();
    auto quantity = name_col("ss_quantity", int_type);
    auto tax = name_col("ss_ext_tax", int_type);
    const std::vector<ColumnDefinition> table_schema = {quantity, tax};
    const std::vector<ColumnDefinition> file_schema = {
            name_col("ss_quantity", int_type, 0),
            name_col("ss_ext_tax", int_type, 1),
    };

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    ASSERT_TRUE(mapper.create_mapping(table_schema, {}, file_schema).ok());
    ASSERT_EQ(mapper.mappings().size(), 2);

    auto filter_expr = int_gt(table_slot(7, 0, int_type, "ss_quantity"), 20);
    TableFilter filter {.conjunct = VExprContext::create_shared(filter_expr),
                        .global_indices = {GlobalIndex(0)}};

    FileScanRequest request;
    ASSERT_TRUE(mapper.create_scan_request({filter}, table_schema, &request).ok());

    ASSERT_EQ(request.predicate_columns.size(), 1);
    EXPECT_EQ(request.predicate_columns[0].column_id(), LocalColumnId(0));
    ASSERT_EQ(request.non_predicate_columns.size(), 1);
    EXPECT_EQ(request.non_predicate_columns[0].column_id(), LocalColumnId(1));
    // A visible predicate slot is still part of the table output and cannot be replaced with a
    // default-valued placeholder after file-local filtering.
    EXPECT_TRUE(request.predicate_only_columns.empty());
}

TEST(ColumnMapperScanRequestTest, StructOutputAndFilterOnlyChildAreMerged) {
    const auto int_type = i32();
    const auto string_type = str();

    auto table_a = name_col("a", int_type);
    auto table_b = name_col("b", string_type);
    auto table_struct = struct_name_col("s", {table_b});
    auto full_table_struct = struct_name_col("s", {table_a, table_b});

    auto file_a = name_col("a", int_type, 0);
    auto file_b = name_col("b", string_type, 1);
    auto file_struct = struct_name_col("s", {file_a, file_b}, 5);

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    ASSERT_TRUE(mapper.create_mapping({table_struct}, {}, {file_struct}).ok());

    auto filter_expr = int_gt(
            struct_element(table_slot(0, 0, full_table_struct.type, "s"), int_type, "a"), 10);
    TableFilter filter {.conjunct = VExprContext::create_shared(filter_expr),
                        .global_indices = {GlobalIndex(0)}};

    FileScanRequest request;
    ASSERT_TRUE(mapper.create_scan_request({filter}, {table_struct}, &request).ok());

    EXPECT_TRUE(request.non_predicate_columns.empty());
    ASSERT_EQ(request.predicate_columns.size(), 1);
    EXPECT_EQ(request.predicate_columns[0].column_id(), LocalColumnId(5));
    ASSERT_FALSE(request.predicate_columns[0].project_all_children);
    EXPECT_EQ(projection_ids(request.predicate_columns[0].children), std::vector<int32_t>({0, 1}));
}

TEST(ColumnMapperScanRequestTest, RenamedNestedPredicateTargetsMappedFileChild) {
    const auto int_type = i32();

    auto table_a = field_id_col("a", 1, int_type);
    auto table_renamed_b = field_id_col("renamed_b", 2, int_type);
    auto table_struct = struct_col("s", 10, {table_a, table_renamed_b});
    auto file_a = field_id_col("a", 1, int_type, 0);
    auto file_b = field_id_col("b", 2, int_type, 1);
    auto file_struct = struct_col("s", 10, {file_a, file_b}, 5);

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_FIELD_ID});
    ASSERT_TRUE(mapper.create_mapping({table_struct}, {}, {file_struct}).ok());

    auto filter_expr = int_gt(
            struct_element(table_slot(0, 0, table_struct.type, "s"), int_type, "renamed_b"), 10);
    TableFilter filter {.conjunct = VExprContext::create_shared(filter_expr),
                        .global_indices = {GlobalIndex(0)}};

    FileScanRequest request;
    ASSERT_TRUE(mapper.create_scan_request({filter}, {table_struct}, &request).ok());
}

TEST(ColumnMapperScanRequestTest, NestedInNullAndReverseComparisonFiltersAreMerged) {
    const auto int_type = i32();
    const auto string_type = str();

    auto table_a = name_col("a", int_type);
    auto table_b = name_col("b", string_type);
    auto table_struct = struct_name_col("s", {table_b});
    auto full_table_struct = struct_name_col("s", {table_a, table_b});

    auto file_a = name_col("a", int_type, 0);
    auto file_b = name_col("b", string_type, 1);
    auto file_struct = struct_name_col("s", {file_a, file_b}, 5);

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    ASSERT_TRUE(mapper.create_mapping({table_struct}, {}, {file_struct}).ok());

    const auto nested_a =
            struct_element(table_slot(0, 0, full_table_struct.type, "s"), int_type, "a");
    auto in_filter =
            in_predicate(nested_a, int_type,
                         {Field::create_field<TYPE_INT>(5), Field::create_field<TYPE_INT>(7)});
    auto reverse_filter = binary_predicate(
            TExprOpcode::LT, literal(int_type, Field::create_field<TYPE_INT>(3)), nested_a);
    auto null_filter = null_predicate(nested_a, true);
    auto not_null_filter = null_predicate(nested_a, false);
    auto filter_expr = compound_predicate(
            TExprOpcode::COMPOUND_AND,
            compound_predicate(TExprOpcode::COMPOUND_AND, in_filter, reverse_filter),
            compound_predicate(TExprOpcode::COMPOUND_AND, null_filter, not_null_filter));
    TableFilter filter {.conjunct = VExprContext::create_shared(filter_expr),
                        .global_indices = {GlobalIndex(0)}};

    FileScanRequest request;
    ASSERT_TRUE(mapper.create_scan_request({filter}, {table_struct}, &request).ok());
}

TEST(ColumnMapperScanRequestTest, NestedPredicateFilterThroughSafeCast) {
    const auto file_int_type = i32();
    const auto table_bigint_type = i64();
    const auto string_type = str();

    auto table_b = name_col("b", string_type);
    auto table_struct = struct_name_col("s", {table_b});
    auto full_table_struct = std::make_shared<DataTypeStruct>(
            DataTypes {table_bigint_type, string_type}, Strings {"a", "b"});

    auto file_a = name_col("a", file_int_type, 0);
    auto file_b = name_col("b", string_type, 1);
    auto file_struct = struct_name_col("s", {file_a, file_b}, 5);

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    ASSERT_TRUE(mapper.create_mapping({table_struct}, {}, {file_struct}).ok());

    const auto nested_a =
            struct_element(table_slot(0, 0, full_table_struct, "s"), file_int_type, "a");
    auto filter_expr =
            binary_predicate(TExprOpcode::GT, cast_expr(nested_a, table_bigint_type),
                             literal(table_bigint_type, Field::create_field<TYPE_BIGINT>(5)));
    TableFilter filter {.conjunct = VExprContext::create_shared(filter_expr),
                        .global_indices = {GlobalIndex(0)}};

    FileScanRequest request;
    ASSERT_TRUE(mapper.create_scan_request({filter}, {table_struct}, &request).ok());
}

TEST(ColumnMapperScanRequestTest, UnsafeCastDoesNotBuildNestedPredicateFilter) {
    const auto file_bigint_type = i64();
    const auto table_int_type = i32();
    const auto string_type = str();

    auto table_b = name_col("b", string_type);
    auto table_struct = struct_name_col("s", {table_b});
    auto full_table_struct = std::make_shared<DataTypeStruct>(
            DataTypes {table_int_type, string_type}, Strings {"a", "b"});

    auto file_a = name_col("a", file_bigint_type, 0);
    auto file_b = name_col("b", string_type, 1);
    auto file_struct = struct_name_col("s", {file_a, file_b}, 5);

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    ASSERT_TRUE(mapper.create_mapping({table_struct}, {}, {file_struct}).ok());

    const auto nested_a =
            struct_element(table_slot(0, 0, full_table_struct, "s"), file_bigint_type, "a");
    auto filter_expr = binary_predicate(TExprOpcode::GT, cast_expr(nested_a, table_int_type),
                                        literal(table_int_type, Field::create_field<TYPE_INT>(5)));
    TableFilter filter {.conjunct = VExprContext::create_shared(filter_expr),
                        .global_indices = {GlobalIndex(0)}};

    FileScanRequest request;
    ASSERT_TRUE(mapper.create_scan_request({filter}, {table_struct}, &request).ok());
    ASSERT_EQ(request.predicate_columns.size(), 1);
    EXPECT_EQ(request.predicate_columns[0].column_id(), LocalColumnId(5));
    EXPECT_EQ(projection_ids(request.predicate_columns[0].children), std::vector<int32_t>({0, 1}));
}

TEST(ColumnMapperScanRequestTest, DeepNestedPredicateTargetsLeafPath) {
    const auto id_type = i32();
    const auto name_type = str();
    const auto string_type = str();
    auto table_b = name_col("b", string_type);
    auto table_struct = struct_name_col("s", {table_b});

    auto full_table_inner_type =
            std::make_shared<DataTypeStruct>(DataTypes {id_type, name_type}, Strings {"id", "n"});
    auto full_table_struct_type = std::make_shared<DataTypeStruct>(
            DataTypes {full_table_inner_type, string_type}, Strings {"a", "b"});

    auto file_id = name_col("id", id_type, 0);
    auto file_name = name_col("n", name_type, 1);
    auto file_a = struct_name_col("a", {file_id, file_name}, 0);
    auto file_b = name_col("b", string_type, 1);
    auto file_struct = struct_name_col("s", {file_a, file_b}, 5);

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    ASSERT_TRUE(mapper.create_mapping({table_struct}, {}, {file_struct}).ok());

    const auto nested_id =
            struct_element(struct_element(table_slot(0, 0, full_table_struct_type, "s"),
                                          full_table_inner_type, "a"),
                           id_type, "id");
    auto filter_expr =
            in_predicate(nested_id, id_type,
                         {Field::create_field<TYPE_INT>(5), Field::create_field<TYPE_INT>(7)});
    TableFilter filter {.conjunct = VExprContext::create_shared(filter_expr),
                        .global_indices = {GlobalIndex(0)}};

    FileScanRequest request;
    ASSERT_TRUE(mapper.create_scan_request({filter}, {table_struct}, &request).ok());
}

TEST(ColumnMapperScanRequestTest, ArrayStructProjectionPrunesElementChildren) {
    const auto int_type = i32();
    const auto string_type = str();
    auto table_b = name_col("b", string_type);
    auto table_element = struct_name_col("element", {table_b});
    auto table_array = array_col("items", -1, table_element);
    table_array.identifier = Field::create_field<TYPE_STRING>("items");
    set_name_identifiers(&table_array, -1);

    auto file_a = name_col("a", int_type, 0);
    auto file_b = name_col("b", string_type, 1);
    auto file_element = struct_name_col("element", {file_a, file_b}, 0);
    auto file_array = array_col("items", -1, file_element, 4);
    file_array.identifier = Field::create_field<TYPE_STRING>("items");
    set_name_identifiers(&file_array, 4);

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    ASSERT_TRUE(mapper.create_mapping({table_array}, {}, {file_array}).ok());

    FileScanRequest request;
    ASSERT_TRUE(mapper.create_scan_request({}, {table_array}, &request).ok());

    ASSERT_EQ(request.non_predicate_columns.size(), 1);
    const auto& projection = request.non_predicate_columns[0];
    EXPECT_EQ(projection.column_id(), LocalColumnId(4));
    ASSERT_FALSE(projection.project_all_children);
    ASSERT_EQ(projection.children.size(), 1);
    EXPECT_EQ(projection.children[0].local_id(), 0);
    ASSERT_EQ(projection.children[0].children.size(), 1);
    EXPECT_EQ(projection.children[0].children[0].local_id(), 1);

    const auto* mapped_array = assert_cast<const DataTypeArray*>(
            remove_nullable(mapper.mappings()[0].file_type).get());
    const auto* mapped_element = assert_cast<const DataTypeStruct*>(
            remove_nullable(mapped_array->get_nested_type()).get());
    ASSERT_EQ(mapped_element->get_elements().size(), 1);
    EXPECT_EQ(mapped_element->get_element_name(0), "b");
}

TEST(ColumnMapperScanRequestTest, MapValueStructProjectionPrunesValueChildren) {
    const auto key_type = str();
    const auto int_type = i32();
    const auto string_type = str();

    auto table_value_b = name_col("b", string_type);
    auto table_value = struct_name_col("value", {table_value_b});
    auto table_map = map_col("m", -1, {table_value}, key_type, table_value.type);
    table_map.identifier = Field::create_field<TYPE_STRING>("m");
    set_name_identifiers(&table_map, -1);

    auto file_key = name_col("key", key_type, 0);
    auto file_value_a = name_col("a", int_type, 0);
    auto file_value_b = name_col("b", string_type, 1);
    auto file_value = struct_name_col("value", {file_value_a, file_value_b}, 1);
    auto file_map = map_col("m", -1, {file_key, file_value}, key_type, file_value.type, 6);
    file_map.identifier = Field::create_field<TYPE_STRING>("m");
    set_name_identifiers(&file_map, 6);

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    ASSERT_TRUE(mapper.create_mapping({table_map}, {}, {file_map}).ok());

    FileScanRequest request;
    ASSERT_TRUE(mapper.create_scan_request({}, {table_map}, &request).ok());

    ASSERT_EQ(request.non_predicate_columns.size(), 1);
    const auto& projection = request.non_predicate_columns[0];
    EXPECT_EQ(projection.column_id(), LocalColumnId(6));
    ASSERT_FALSE(projection.project_all_children);
    ASSERT_EQ(projection.children.size(), 1);
    EXPECT_EQ(projection.children[0].local_id(), 1);
    ASSERT_EQ(projection.children[0].children.size(), 1);
    EXPECT_EQ(projection.children[0].children[0].local_id(), 1);

    const auto* mapped_map =
            assert_cast<const DataTypeMap*>(remove_nullable(mapper.mappings()[0].file_type).get());
    const auto* mapped_value =
            assert_cast<const DataTypeStruct*>(remove_nullable(mapped_map->get_value_type()).get());
    ASSERT_EQ(mapped_value->get_elements().size(), 1);
    EXPECT_EQ(mapped_value->get_element_name(0), "b");
}

// Scenario: a table struct projects only child `b`, while the file struct stores `a,b`.
// BY_NAME mapping should read only the physical child `b` and rebuild the mapped file type to the
// projected struct shape.
TEST(ColumnMapperScanRequestTest, StructProjectionPrunesChildrenByName) {
    const auto int_type = i32();
    const auto string_type = str();

    auto table_b = name_col("b", string_type);
    auto table_struct = struct_name_col("s", {table_b});
    set_name_identifiers(&table_struct, 0);

    auto file_a = name_col("a", int_type, 0);
    auto file_b = name_col("b", string_type, 1);
    auto file_struct = struct_name_col("s", {file_a, file_b}, 0);
    set_name_identifiers(&file_struct, 0);

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    ASSERT_TRUE(mapper.create_mapping({table_struct}, {}, {file_struct}).ok());

    FileScanRequest request;
    ASSERT_TRUE(mapper.create_scan_request({}, {table_struct}, &request).ok());

    ASSERT_EQ(request.non_predicate_columns.size(), 1);
    const auto& projection = request.non_predicate_columns[0];
    EXPECT_EQ(projection.column_id(), LocalColumnId(0));
    ASSERT_FALSE(projection.project_all_children);
    ASSERT_EQ(projection.children.size(), 1);
    EXPECT_EQ(projection.children[0].local_id(), 1);

    ASSERT_EQ(mapper.mappings().size(), 1);
    const auto* projected_type = assert_cast<const DataTypeStruct*>(
            remove_nullable(mapper.mappings()[0].file_type).get());
    ASSERT_EQ(projected_type->get_elements().size(), 1);
    EXPECT_EQ(projected_type->get_element_name(0), "b");
}

// Scenario: a row filter reaches a struct child through an array wrapper
// (`items.item.a > 5`). The mapper cannot localize the filter, so it keeps the full array root in
// the lazy non-predicate set for table-level evaluation.
TEST(ColumnMapperScanRequestTest, ArrayWrapperDoesNotBuildNestedPredicateFilter) {
    const auto int_type = i32();
    const auto string_type = str();

    auto file_a = name_col("a", int_type, 0);
    auto file_b = name_col("b", string_type, 1);
    auto file_element = struct_name_col("item", {file_a, file_b}, 0);
    auto file_array = array_col("items", -1, file_element, 0);
    set_name_identifiers(&file_array, 0);

    auto table_array = file_array;

    const auto item_type = file_element.type;
    auto item_expr = struct_element(table_slot(0, 0, table_array.type, "items"), item_type, "item");
    auto filter_expr = int_gt(struct_element(item_expr, int_type, "a"), 5);
    TableFilter filter {.conjunct = VExprContext::create_shared(filter_expr),
                        .global_indices = {GlobalIndex(0)}};

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    ASSERT_TRUE(mapper.create_mapping({table_array}, {}, {file_array}).ok());

    FileScanRequest request;
    ASSERT_TRUE(mapper.create_scan_request({filter}, {table_array}, &request).ok());

    EXPECT_TRUE(request.conjuncts.empty());
    EXPECT_TRUE(request.predicate_columns.empty());
    ASSERT_EQ(request.non_predicate_columns.size(), 1);
    EXPECT_EQ(request.non_predicate_columns[0].column_id(), LocalColumnId(0));
    EXPECT_TRUE(request.non_predicate_columns[0].project_all_children);
    EXPECT_TRUE(request.non_predicate_columns[0].children.empty());
}

// Scenario: a map value struct projects child `b`, while a row filter reads value child `a`.
// The filter is too complex to become a file-local nested predicate. Lazy demotion must move the
// merged projection to the non-predicate set without dropping either physical value child.
TEST(ColumnMapperScanRequestTest, MapFilterOnlyValueChildMergesWithOutputProjection) {
    const auto key_type = i32();
    const auto int_type = i32();
    const auto string_type = str();

    auto table_value_b = name_col("b", string_type);
    auto table_value = struct_name_col("value", {table_value_b});
    auto table_map = map_col("m", -1, {table_value}, key_type, table_value.type);
    set_name_identifiers(&table_map, 0);

    auto file_key = name_col("key", key_type, 0);
    auto file_value_a = name_col("a", int_type, 0);
    auto file_value_b = name_col("b", string_type, 1);
    auto file_value = struct_name_col("value", {file_value_a, file_value_b}, 1);
    auto file_map = map_col("m", -1, {file_key, file_value}, key_type, file_value.type, 0);
    set_name_identifiers(&file_map, 0);

    auto full_value_type =
            std::make_shared<DataTypeStruct>(DataTypes {int_type, string_type}, Strings {"a", "b"});
    auto full_map_type = std::make_shared<DataTypeMap>(key_type, full_value_type);
    auto value_expr =
            struct_element(table_slot(0, 0, full_map_type, "m"), full_value_type, "value");
    auto filter_expr = int_gt(struct_element(value_expr, int_type, "a"), 5);
    TableFilter filter {.conjunct = VExprContext::create_shared(filter_expr),
                        .global_indices = {GlobalIndex(0)}};

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    ASSERT_TRUE(mapper.create_mapping({table_map}, {}, {file_map}).ok());

    FileScanRequest request;
    ASSERT_TRUE(mapper.create_scan_request({filter}, {table_map}, &request).ok());

    EXPECT_TRUE(request.predicate_columns.empty());
    ASSERT_EQ(request.non_predicate_columns.size(), 1);
    const auto& projection = request.non_predicate_columns[0];
    EXPECT_EQ(projection.column_id(), LocalColumnId(0));
    ASSERT_FALSE(projection.project_all_children);
    ASSERT_EQ(projection.children.size(), 1);
    EXPECT_EQ(projection.children[0].local_id(), 1);
    EXPECT_EQ(projection_ids(projection.children[0].children), std::vector<int32_t>({0, 1}));
}

// Scenario: when projected struct children are an in-order prefix of the file struct, the mapper can
// read those physical children directly without rebuilding the file-side complex type.
TEST(ColumnMapperScanRequestTest, MatchingProjectedStructDoesNotNeedComplexRematerialize) {
    const auto int_type = i32();
    const auto string_type = str();

    auto table_a = field_id_col("a", 1, int_type);
    auto table_b = field_id_col("b", 2, string_type);
    auto table_struct = struct_col("s", 10, {table_a, table_b});

    auto file_a = field_id_col("a", 1, int_type, 0);
    auto file_b = field_id_col("b", 2, string_type, 1);
    auto file_c = field_id_col("c", 3, int_type, 2);
    auto file_struct = struct_col("s", 10, {file_a, file_b, file_c}, 5);

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_FIELD_ID});
    ASSERT_TRUE(mapper.create_mapping({table_struct}, {}, {file_struct}).ok());

    ASSERT_EQ(mapper.mappings().size(), 1);
    EXPECT_TRUE(mapper.mappings()[0].is_trivial);

    FileScanRequest request;
    ASSERT_TRUE(mapper.create_scan_request({}, {table_struct}, &request).ok());

    ASSERT_EQ(request.non_predicate_columns.size(), 1);
    const auto& projection = request.non_predicate_columns[0];
    EXPECT_FALSE(projection.project_all_children);
    EXPECT_EQ(projection_ids(projection.children), std::vector<int32_t>({0, 1}));
    EXPECT_TRUE(mapper.mappings()[0].is_trivial);
}

// Scenario: Iceberg field-id mapping sees a renamed struct child, but the physical child order and
// types still match, so projection remains a full physical read instead of rebuilding a new type.
TEST(ColumnMapperScanRequestTest, RenameOnlyProjectedStructDoesNotRebuildFileProjection) {
    const auto int_type = i32();

    auto table_a = field_id_col("a", 1, int_type);
    auto table_renamed_b = field_id_col("renamed_b", 2, int_type);
    auto table_struct = struct_col("s", 10, {table_a, table_renamed_b});

    auto file_a = field_id_col("a", 1, int_type, 0);
    auto file_b = field_id_col("b", 2, int_type, 1);
    auto file_struct = struct_col("s", 10, {file_a, file_b}, 5);

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_FIELD_ID});
    ASSERT_TRUE(mapper.create_mapping({table_struct}, {}, {file_struct}).ok());

    ASSERT_EQ(mapper.mappings().size(), 1);
    EXPECT_TRUE(mapper.mappings()[0].is_trivial);
    EXPECT_EQ(mapper.mappings()[0].projected_file_children.size(),
              mapper.mappings()[0].original_file_children.size());
    ASSERT_EQ(mapper.mappings()[0].child_mappings.size(), 2);
    EXPECT_EQ(mapper.mappings()[0].child_mappings[1].table_column_name, "renamed_b");
    EXPECT_EQ(mapper.mappings()[0].child_mappings[1].file_column_name, "b");

    FileScanRequest request;
    ASSERT_TRUE(mapper.create_scan_request({}, {table_struct}, &request).ok());

    ASSERT_EQ(request.non_predicate_columns.size(), 1);
    EXPECT_TRUE(request.non_predicate_columns[0].project_all_children);
    EXPECT_TRUE(request.non_predicate_columns[0].children.empty());
    EXPECT_TRUE(mapper.mappings()[0].is_trivial);
}

// Scenario: a row filter references an unprojected struct child, so the predicate projection is
// merged with the output projection and the mapper rebuilds the projected file struct type.
TEST(ColumnMapperScanRequestTest, PredicateProjectionRebuildsProjectedStructFileType) {
    const auto int_type = i32();
    const auto string_type = str();

    auto table_a = field_id_col("a", 1, int_type);
    auto table_b = field_id_col("b", 2, string_type);
    auto table_struct = struct_col("s", 10, {table_a, table_b});
    auto full_table_c = field_id_col("c", 3, int_type);
    auto full_table_struct = struct_col("s", 10, {table_a, table_b, full_table_c});

    auto file_a = field_id_col("a", 1, int_type, 0);
    auto file_b = field_id_col("b", 2, string_type, 1);
    auto file_c = field_id_col("c", 3, int_type, 2);
    auto file_struct = struct_col("s", 10, {file_a, file_b, file_c}, 5);

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_FIELD_ID});
    ASSERT_TRUE(mapper.create_mapping({table_struct}, {}, {file_struct}).ok());

    auto filter_expr =
            int_gt(struct_element(table_slot(0, 0, full_table_struct.type, "s"), int_type, "c"), 0);
    TableFilter filter {.conjunct = VExprContext::create_shared(filter_expr),
                        .global_indices = {GlobalIndex(0)}};

    FileScanRequest request;
    ASSERT_TRUE(mapper.create_scan_request({filter}, {table_struct}, &request).ok());

    ASSERT_EQ(request.predicate_columns.size(), 1);
    EXPECT_TRUE(request.non_predicate_columns.empty());
    const auto& projection = request.predicate_columns[0];
    EXPECT_FALSE(projection.project_all_children);
    EXPECT_EQ(projection_ids(projection.children), std::vector<int32_t>({0, 1, 2}));

    const auto* mapped_type = assert_cast<const DataTypeStruct*>(
            remove_nullable(mapper.mappings()[0].file_type).get());
    ASSERT_EQ(mapped_type->get_elements().size(), 3);
    EXPECT_EQ(mapped_type->get_element_name(0), "a");
    EXPECT_EQ(mapped_type->get_element_name(1), "b");
    EXPECT_EQ(mapped_type->get_element_name(2), "c");
    EXPECT_FALSE(mapper.mappings()[0].is_trivial);
}

// Scenario: a filter references a top-level column that is not projected by the query; the mapper
// creates a hidden filter mapping without adding that hidden column to visible table mappings.
TEST(ColumnMapperScanRequestTest, PredicateOnlyTopLevelColumnUsesHiddenMapping) {
    const auto int_type = i32();

    auto table_id = field_id_col("id", 0, int_type);
    auto table_c = field_id_col("c", 11, int_type);
    auto table_struct = struct_col("s", 10, {table_c});

    auto file_id = field_id_col("id", 0, int_type, 0);
    auto file_c = field_id_col("c", 11, int_type, 0);
    auto file_struct = struct_col("s", 10, {file_c}, 10);

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_FIELD_ID});
    ASSERT_TRUE(mapper.create_mapping({table_id}, {}, {file_id, file_struct}).ok());
    ASSERT_EQ(mapper.mappings().size(), 1);
    EXPECT_EQ(mapper.mappings()[0].table_column_name, "id");

    auto filter_expr =
            int_gt(struct_element(table_slot(7, 1, table_struct.type, "s"), int_type, "c"), 0);
    TableFilter filter {.conjunct = VExprContext::create_shared(filter_expr),
                        .global_indices = {GlobalIndex(1)}};

    FileScanRequest request;
    ASSERT_TRUE(mapper.create_scan_request({filter}, {table_id}, &request).ok());

    ASSERT_EQ(mapper.mappings().size(), 1);
    EXPECT_EQ(mapper.mappings()[0].table_column_name, "id");

    ASSERT_EQ(request.non_predicate_columns.size(), 1);
    EXPECT_EQ(request.non_predicate_columns[0].column_id(), LocalColumnId(0));
    ASSERT_EQ(request.predicate_columns.size(), 1);
    EXPECT_EQ(request.predicate_columns[0].column_id(), LocalColumnId(10));
    EXPECT_EQ(request.predicate_only_columns, std::vector<LocalColumnId>({LocalColumnId(10)}));
    EXPECT_TRUE(request.predicate_columns[0].project_all_children);
    EXPECT_TRUE(request.predicate_columns[0].children.empty());

    ASSERT_EQ(request.conjuncts.size(), 1);
}

// Scenario: a nested predicate targets a table-side renamed struct field; scan projection must
// resolve that field to the old physical file child.
TEST(ColumnMapperScanRequestTest, NestedPredicateProjectionUsesMappedRenamedChild) {
    const auto int_type = i32();

    auto table_a = field_id_col("a", 1, int_type);
    auto table_renamed_b = field_id_col("renamed_b", 2, int_type);
    auto table_struct = struct_col("s", 10, {table_a, table_renamed_b});

    auto file_a = field_id_col("a", 1, int_type, 0);
    auto file_b = field_id_col("b", 2, int_type, 1);
    auto file_struct = struct_col("s", 10, {file_a, file_b}, 10);

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_FIELD_ID});
    ASSERT_TRUE(mapper.create_mapping({table_struct}, {}, {file_struct}).ok());

    auto filter_expr = int_gt(
            struct_element(table_slot(0, 0, table_struct.type, "s"), int_type, "renamed_b"), 0);
    TableFilter filter {.conjunct = VExprContext::create_shared(filter_expr),
                        .global_indices = {GlobalIndex(0)}};

    FileScanRequest request;
    ASSERT_TRUE(mapper.create_scan_request({filter}, {table_struct}, &request).ok());
    ASSERT_EQ(request.predicate_columns.size(), 1);
    EXPECT_TRUE(request.predicate_columns[0].project_all_children);
    EXPECT_TRUE(request.predicate_columns[0].children.empty());
}

// Scenario: element_at(struct, 'table_name') in a row filter is localized to the physical file
// child name, matching the struct_element rewrite path.
TEST(ColumnMapperScanRequestTest,
     FileLocalElementAtConjunctUsesFileChildNameForRenamedStructField) {
    const auto int_type = i32();

    auto table_a = field_id_col("a", 1, int_type);
    auto table_renamed_b = field_id_col("renamed_b", 2, int_type);
    auto table_struct = struct_col("s", 10, {table_a, table_renamed_b});

    auto file_a = field_id_col("a", 1, int_type, 0);
    auto file_b = field_id_col("b", 2, int_type, 1);
    auto file_struct = struct_col("s", 10, {file_a, file_b}, 10);

    auto child_expr = element_at(table_slot(0, 0, table_struct.type, table_struct.name), int_type,
                                 "renamed_b");
    auto filter_expr = int_gt(child_expr, 0);
    TableFilter filter {.conjunct = VExprContext::create_shared(filter_expr),
                        .global_indices = {GlobalIndex(0)}};

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_FIELD_ID});
    ASSERT_TRUE(mapper.create_mapping({table_struct}, {}, {file_struct}).ok());

    FileScanRequest request;
    ASSERT_TRUE(mapper.create_scan_request({filter}, {table_struct}, &request).ok());

    ASSERT_EQ(request.conjuncts.size(), 1);
    const auto& localized_child = request.conjuncts[0]->root()->children()[0];
    EXPECT_EQ(localized_child->expr_name(), "element_at");
    const auto* localized_slot = assert_cast<const VSlotRef*>(localized_child->children()[0].get());
    EXPECT_EQ(localized_slot->column_name(), "s");
    EXPECT_EQ(localized_slot->column_id(), 0);

    const auto* localized_literal =
            assert_cast<const VLiteral*>(localized_child->children()[1].get());
    Field localized_field;
    localized_literal->get_column_ptr()->get(0, localized_field);
    ASSERT_EQ(localized_field.get_type(), TYPE_STRING);
    EXPECT_EQ(std::string(localized_field.as_string_view()), "b");
}

// Scenario: nested element_at(struct, name) localization rewrites both selector names and
// intermediate return types. The outer selector must be prepared against the projected file child
// struct, not the table child struct or the full historical file child struct.
TEST(ColumnMapperScanRequestTest, NestedElementAtConjunctUsesFileChildTypeForRenamedLeaf) {
    const auto int_type = i32();
    const auto string_type = str();

    auto table_new_aa = field_id_col("new_aa", 23, int_type);
    auto table_bb = field_id_col("bb", 24, string_type);
    auto table_new_a = struct_col("new_a", 20, {table_new_aa, table_bb});
    auto table_struct = struct_col("struct_column2", 19, {table_new_a});

    auto file_aa = field_id_col("aa", 23, int_type, 0);
    auto file_bb = field_id_col("bb", 24, string_type, 1);
    auto file_new_a = struct_col("new_a", 20, {file_aa, file_bb}, 0);
    auto file_struct = struct_col("struct_column2", 19, {file_new_a}, 10);

    const auto table_slot_expr = table_slot(0, 0, table_struct.type, "struct_column2");
    const auto table_parent_expr = element_at(table_slot_expr, table_new_a.type, "new_a");
    const auto table_leaf_expr = element_at(table_parent_expr, int_type, "new_aa");
    auto filter_expr = binary_predicate(TExprOpcode::EQ, table_leaf_expr,
                                        literal(int_type, Field::create_field<TYPE_INT>(50)));
    TableFilter filter {.conjunct = VExprContext::create_shared(filter_expr),
                        .global_indices = {GlobalIndex(0)}};

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_FIELD_ID});
    ASSERT_TRUE(mapper.create_mapping({table_struct}, {}, {file_struct}).ok());

    FileScanRequest request;
    ASSERT_TRUE(mapper.create_scan_request({filter}, {table_struct}, &request).ok());
    ASSERT_EQ(request.conjuncts.size(), 1);

    const auto& localized_leaf = request.conjuncts[0]->root()->children()[0];
    ASSERT_EQ(localized_leaf->expr_name(), "element_at");
    const auto& localized_parent = localized_leaf->children()[0];
    ASSERT_EQ(localized_parent->expr_name(), "element_at");

    const auto* localized_leaf_selector =
            assert_cast<const VLiteral*>(localized_leaf->children()[1].get());
    Field localized_leaf_field;
    localized_leaf_selector->get_column_ptr()->get(0, localized_leaf_field);
    ASSERT_EQ(localized_leaf_field.get_type(), TYPE_STRING);
    EXPECT_EQ(std::string(localized_leaf_field.as_string_view()), "aa");

    const auto* localized_parent_type = assert_cast<const DataTypeStruct*>(
            remove_nullable(localized_parent->data_type()).get());
    ASSERT_EQ(localized_parent_type->get_elements().size(), 2);
    EXPECT_EQ(localized_parent_type->get_element_name(0), "aa");
    EXPECT_EQ(localized_parent_type->get_element_name(1), "bb");
}

// Scenario: Iceberg promotes a nested struct leaf from INT to BIGINT while an old file still
// stores INT. Because 15 is exactly representable as INT and every INT survives promotion to
// BIGINT, localize `s.b::BIGINT > 15::BIGINT` as `file_s.b::INT > 15::INT`. Rewriting one literal
// avoids casting every file value while preserving the table predicate exactly.
TEST_F(ColumnMapperCastTest, NestedElementAtConjunctRewritesExactLiteralToFileType) {
    const auto file_int_type = i32();
    const auto table_bigint_type = i64();

    auto table_b = field_id_col("b", 11, table_bigint_type);
    auto table_struct = struct_col("s", 10, {table_b});
    auto file_b = field_id_col("b", 11, file_int_type, 0);
    auto file_struct = struct_col("s", 10, {file_b}, 5);

    auto table_leaf = executable_struct_element(
            table_slot(0, 0, table_struct.type, table_struct.name), table_bigint_type, "b");
    auto filter_expr = executable_binary_predicate(
            TExprOpcode::GT, table_leaf,
            literal(table_bigint_type, Field::create_field<TYPE_BIGINT>(15)));
    TableFilter filter {.conjunct = VExprContext::create_shared(filter_expr),
                        .global_indices = {GlobalIndex(0)}};

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_FIELD_ID});
    ASSERT_TRUE(mapper.create_mapping({table_struct}, {}, {file_struct}).ok());

    FileScanRequest request;
    ASSERT_TRUE(mapper.create_scan_request({filter}, {table_struct}, &request, &state).ok());
    ASSERT_EQ(request.conjuncts.size(), 1);
    const auto& localized_root = request.conjuncts[0]->root();
    ASSERT_EQ(localized_root->get_num_children(), 2);
    const auto& localized_leaf = localized_root->children()[0];
    EXPECT_EQ(localized_leaf->expr_name(), "element_at");
    EXPECT_TRUE(localized_leaf->data_type()->equals(*file_int_type));
    const auto& localized_literal = localized_root->children()[1];
    EXPECT_TRUE(localized_literal->is_literal());
    EXPECT_TRUE(localized_literal->data_type()->equals(*file_int_type));

    auto values = ColumnInt32::create();
    values->insert_value(10);
    values->insert_value(20);
    MutableColumns children;
    children.push_back(std::move(values));
    Block block;
    block.insert({ColumnStruct::create(std::move(children)), mapper.mappings()[0].file_type, "s"});

    auto* conjunct = request.conjuncts[0].get();
    auto status = conjunct->prepare(&state, RowDescriptor());
    ASSERT_TRUE(status.ok()) << status;
    status = conjunct->open(&state);
    ASSERT_TRUE(status.ok()) << status;
    IColumn::Filter result(block.rows(), 1);
    bool can_filter_all = false;
    status = conjunct->execute_filter(&block, result.data(), block.rows(), false, &can_filter_all);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_FALSE(can_filter_all);
    EXPECT_EQ(result, IColumn::Filter({0, 1}));
    conjunct->close();
}

// Scenario: an old file allows NULL for a nested leaf that the current table declares required.
// Although every non-NULL INT value and the literal 15 can be promoted to BIGINT exactly, the
// predicate must stay above TableReader. If `file_s.b > 15` ran first for rows [NULL, 20], it would
// discard NULL and prevent table-schema materialization from reporting the nullable-to-required
// contract violation.
TEST_F(ColumnMapperCastTest,
       NestedElementAtConjunctStaysTableLevelForNullableFileLeafMappedToRequiredTableLeaf) {
    const auto file_nullable_int_type = make_nullable(i32());
    const auto table_bigint_type = i64();

    auto table_b = field_id_col("b", 11, table_bigint_type);
    auto table_struct = struct_col("s", 10, {table_b});
    auto file_b = field_id_col("b", 11, file_nullable_int_type, 0);
    auto file_struct = struct_col("s", 10, {file_b}, 5);

    auto table_leaf = executable_struct_element(
            table_slot(0, 0, table_struct.type, table_struct.name), table_bigint_type, "b");
    auto filter_expr = executable_binary_predicate(
            TExprOpcode::GT, table_leaf,
            literal(table_bigint_type, Field::create_field<TYPE_BIGINT>(15)));
    TableFilter filter {.conjunct = VExprContext::create_shared(filter_expr),
                        .global_indices = {GlobalIndex(0)}};

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_FIELD_ID});
    ASSERT_TRUE(mapper.create_mapping({table_struct}, {}, {file_struct}).ok());

    FileScanRequest request;
    ASSERT_TRUE(mapper.create_scan_request({filter}, {table_struct}, &request, &state).ok());
    ASSERT_EQ(request.predicate_columns.size() + request.non_predicate_columns.size(), 1);
    const auto& scan_column = request.predicate_columns.empty() ? request.non_predicate_columns[0]
                                                                : request.predicate_columns[0];
    EXPECT_EQ(scan_column.column_id(), LocalColumnId(5));
    EXPECT_TRUE(request.conjuncts.empty());
}

// Scenario: a narrowing file-to-table cast can produce NULL or an error for values that do not fit
// the table leaf. Evaluating that cast below TableReader can filter those rows before
// _align_column_nullability() validates the required table child. Keep the predicate at table level
// so schema materialization observes every source row first.
TEST_F(ColumnMapperCastTest, NestedElementAtConjunctStaysTableLevelForNonLosslessFileToTableCast) {
    const auto file_bigint_type = i64();
    const auto table_int_type = i32();

    auto table_a = field_id_col("a", 11, table_int_type);
    auto table_struct = struct_col("s", 10, {table_a});
    auto file_a = field_id_col("a", 11, file_bigint_type, 0);
    auto file_struct = struct_col("s", 10, {file_a}, 5);

    auto table_leaf = executable_struct_element(
            table_slot(0, 0, table_struct.type, table_struct.name), table_int_type, "a");
    auto filter_expr = executable_binary_predicate(
            TExprOpcode::EQ, table_leaf, literal(table_int_type, Field::create_field<TYPE_INT>(1)));
    TableFilter filter {.conjunct = VExprContext::create_shared(filter_expr),
                        .global_indices = {GlobalIndex(0)}};

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_FIELD_ID});
    ASSERT_TRUE(mapper.create_mapping({table_struct}, {}, {file_struct}).ok());

    FileScanRequest request;
    ASSERT_TRUE(mapper.create_scan_request({filter}, {table_struct}, &request, &state).ok());
    ASSERT_EQ(request.predicate_columns.size() + request.non_predicate_columns.size(), 1);
    const auto& scan_column = request.predicate_columns.empty() ? request.non_predicate_columns[0]
                                                                : request.predicate_columns[0];
    EXPECT_EQ(scan_column.column_id(), LocalColumnId(5));
    EXPECT_TRUE(request.conjuncts.empty());
}

// Scenario: the table literal is outside the old file leaf's INT range. Rewriting
// BIGINT 2147483648 to INT would change the predicate, so keep the literal as BIGINT and cast the
// file leaf instead: `CAST(file_s.b::INT AS BIGINT) = 2147483648::BIGINT`.
TEST_F(ColumnMapperCastTest, NestedElementAtConjunctFallsBackForOutOfRangeLiteral) {
    const auto file_int_type = i32();
    const auto table_bigint_type = i64();

    auto table_b = field_id_col("b", 11, table_bigint_type);
    auto table_struct = struct_col("s", 10, {table_b});
    auto file_b = field_id_col("b", 11, file_int_type, 0);
    auto file_struct = struct_col("s", 10, {file_b}, 5);
    auto table_leaf = executable_struct_element(
            table_slot(0, 0, table_struct.type, table_struct.name), table_bigint_type, "b");
    auto filter_expr = executable_binary_predicate(
            TExprOpcode::EQ, table_leaf,
            literal(table_bigint_type, Field::create_field<TYPE_BIGINT>(2147483648LL)));
    TableFilter filter {.conjunct = VExprContext::create_shared(filter_expr),
                        .global_indices = {GlobalIndex(0)}};

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_FIELD_ID});
    ASSERT_TRUE(mapper.create_mapping({table_struct}, {}, {file_struct}).ok());
    FileScanRequest request;
    ASSERT_TRUE(mapper.create_scan_request({filter}, {table_struct}, &request, &state).ok());

    ASSERT_EQ(request.conjuncts.size(), 1);
    const auto& localized_root = request.conjuncts[0]->root();
    ASSERT_EQ(localized_root->get_num_children(), 2);
    const auto& localized_cast = localized_root->children()[0];
    ASSERT_NE(dynamic_cast<const Cast*>(localized_cast.get()), nullptr);
    EXPECT_TRUE(localized_cast->data_type()->equals(*table_bigint_type));
    ASSERT_EQ(localized_cast->get_num_children(), 1);
    EXPECT_EQ(localized_cast->children()[0]->expr_name(), "element_at");
    EXPECT_TRUE(localized_cast->children()[0]->data_type()->equals(*file_int_type));
    EXPECT_TRUE(localized_root->children()[1]->data_type()->equals(*table_bigint_type));
}

// Scenario: the struct leaf is on the right side of the comparison. Literal localization must not
// depend on operand order: `15::BIGINT > s.b::BIGINT` becomes `15::INT > file_s.b::INT`.
TEST_F(ColumnMapperCastTest, NestedElementAtConjunctRewritesReverseComparisonLiteral) {
    const auto file_int_type = i32();
    const auto table_bigint_type = i64();

    auto table_b = field_id_col("b", 11, table_bigint_type);
    auto table_struct = struct_col("s", 10, {table_b});
    auto file_b = field_id_col("b", 11, file_int_type, 0);
    auto file_struct = struct_col("s", 10, {file_b}, 5);
    auto table_leaf = executable_struct_element(
            table_slot(0, 0, table_struct.type, table_struct.name), table_bigint_type, "b");
    auto filter_expr = executable_binary_predicate(
            TExprOpcode::GT, literal(table_bigint_type, Field::create_field<TYPE_BIGINT>(15)),
            table_leaf);
    TableFilter filter {.conjunct = VExprContext::create_shared(filter_expr),
                        .global_indices = {GlobalIndex(0)}};

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_FIELD_ID});
    ASSERT_TRUE(mapper.create_mapping({table_struct}, {}, {file_struct}).ok());
    FileScanRequest request;
    ASSERT_TRUE(mapper.create_scan_request({filter}, {table_struct}, &request, &state).ok());

    ASSERT_EQ(request.conjuncts.size(), 1);
    const auto& localized_root = request.conjuncts[0]->root();
    EXPECT_TRUE(localized_root->children()[0]->data_type()->equals(*file_int_type));
    EXPECT_TRUE(localized_root->children()[0]->is_literal());
    EXPECT_EQ(localized_root->children()[1]->expr_name(), "element_at");
    EXPECT_TRUE(localized_root->children()[1]->data_type()->equals(*file_int_type));
}

// Scenario: IN uses one probe type for every candidate. All exact literals may move to the INT
// file domain, but one out-of-range literal makes the complete IN predicate fall back to BIGINT.
TEST_F(ColumnMapperCastTest, NestedElementAtInPredicateUsesAllOrNothingLiteralRewrite) {
    const auto file_int_type = i32();
    const auto table_bigint_type = i64();
    auto table_b = field_id_col("b", 11, table_bigint_type);
    auto table_struct = struct_col("s", 10, {table_b});
    auto file_b = field_id_col("b", 11, file_int_type, 0);
    auto file_struct = struct_col("s", 10, {file_b}, 5);

    const auto build_filter = [&](int64_t second_value) {
        auto table_leaf = executable_struct_element(
                table_slot(0, 0, table_struct.type, table_struct.name), table_bigint_type, "b");
        auto predicate = create_in_predicate();
        predicate->add_child(table_leaf);
        predicate->add_child(literal(table_bigint_type, Field::create_field<TYPE_BIGINT>(10)));
        predicate->add_child(
                literal(table_bigint_type, Field::create_field<TYPE_BIGINT>(second_value)));
        return TableFilter {.conjunct = VExprContext::create_shared(predicate),
                            .global_indices = {GlobalIndex(0)}};
    };

    TableColumnMapper exact_mapper({.mode = TableColumnMappingMode::BY_FIELD_ID});
    ASSERT_TRUE(exact_mapper.create_mapping({table_struct}, {}, {file_struct}).ok());
    FileScanRequest exact_request;
    ASSERT_TRUE(
            exact_mapper
                    .create_scan_request({build_filter(20)}, {table_struct}, &exact_request, &state)
                    .ok());
    ASSERT_EQ(exact_request.conjuncts.size(), 1);
    const auto& exact_root = exact_request.conjuncts[0]->root();
    EXPECT_EQ(exact_root->children()[0]->expr_name(), "element_at");
    for (const auto& child : exact_root->children()) {
        EXPECT_TRUE(child->data_type()->equals(*file_int_type));
    }

    TableColumnMapper fallback_mapper({.mode = TableColumnMappingMode::BY_FIELD_ID});
    ASSERT_TRUE(fallback_mapper.create_mapping({table_struct}, {}, {file_struct}).ok());
    FileScanRequest fallback_request;
    ASSERT_TRUE(fallback_mapper
                        .create_scan_request({build_filter(2147483648LL)}, {table_struct},
                                             &fallback_request, &state)
                        .ok());
    ASSERT_EQ(fallback_request.conjuncts.size(), 1);
    const auto& fallback_root = fallback_request.conjuncts[0]->root();
    const auto& fallback_cast = fallback_root->children()[0];
    ASSERT_NE(dynamic_cast<const Cast*>(fallback_cast.get()), nullptr);
    EXPECT_TRUE(fallback_cast->data_type()->equals(*table_bigint_type));
    EXPECT_TRUE(fallback_cast->children()[0]->data_type()->equals(*file_int_type));
    EXPECT_TRUE(fallback_root->children()[1]->data_type()->equals(*table_bigint_type));
    EXPECT_TRUE(fallback_root->children()[2]->data_type()->equals(*table_bigint_type));
}

// Scenario: output projection reads one struct child while the row filter reads a different nested
// struct child. File-local conjunct rewrite must use the merged scan projection type. In the SQL
// shape below, `SELECT element_at(s, 'c') WHERE element_at(element_at(s, 'b'), 'cc') LIKE ...`
// reads file children `b.cc` and `c`; the localized inner `element_at(s, 'b')` returns
// `Struct(cc)`, not the full old file child `Struct(cc, new_dd)`.
TEST(ColumnMapperScanRequestTest, NestedElementAtConjunctUsesMergedScanProjectionChildType) {
    const auto string_type = str();
    const auto int_type = i32();

    auto table_cc = field_id_col("cc", 23, string_type);
    auto table_new_dd = field_id_col("new_dd", 24, int_type);
    auto table_b = struct_col("b", 20, {table_cc, table_new_dd});
    auto table_c = field_id_col("c", 25, string_type);
    auto full_table_struct = struct_col("struct_column2", 19, {table_b, table_c});
    auto projected_table_struct = struct_col("struct_column2", 19, {table_c});

    auto file_cc = field_id_col("cc", 23, string_type, 0);
    auto file_new_dd = field_id_col("new_dd", 24, int_type, 1);
    auto file_b = struct_col("b", 20, {file_cc, file_new_dd}, 0);
    auto file_c = field_id_col("c", 25, string_type, 1);
    auto file_struct = struct_col("new_struct_column", 19, {file_b, file_c}, 10);

    const auto table_slot_expr = table_slot(0, 0, full_table_struct.type, "struct_column2");
    const auto table_parent_expr = element_at(table_slot_expr, table_b.type, "b");
    const auto table_leaf_expr = element_at(table_parent_expr, string_type, "cc");
    auto filter_expr = like_expr(table_leaf_expr, "NestedC%");
    TableFilter filter {.conjunct = VExprContext::create_shared(filter_expr),
                        .global_indices = {GlobalIndex(0)}};

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_FIELD_ID});
    ASSERT_TRUE(mapper.create_mapping({projected_table_struct}, {}, {file_struct}).ok());

    FileScanRequest request;
    ASSERT_TRUE(mapper.create_scan_request({filter}, {projected_table_struct}, &request).ok());
    ASSERT_EQ(request.conjuncts.size(), 1);
    ASSERT_EQ(request.predicate_columns.size(), 1);
    EXPECT_EQ(request.predicate_columns[0].column_id(), LocalColumnId(10));

    const auto& localized_leaf = request.conjuncts[0]->root()->children()[0];
    ASSERT_EQ(localized_leaf->expr_name(), "element_at");
    const auto& localized_parent = localized_leaf->children()[0];
    ASSERT_EQ(localized_parent->expr_name(), "element_at");

    const auto* localized_slot =
            assert_cast<const VSlotRef*>(localized_parent->children()[0].get());
    EXPECT_EQ(localized_slot->column_name(), "new_struct_column");
    // The scan projection keeps the top-level file column id above, while the localized conjunct
    // executes on the file-reader Block. The VSlotRef column id is therefore the block position of
    // `new_struct_column` in this request, not the file schema id 10.
    EXPECT_EQ(localized_slot->column_id(), 0);

    const auto* localized_parent_type = assert_cast<const DataTypeStruct*>(
            remove_nullable(localized_parent->data_type()).get());
    ASSERT_EQ(localized_parent_type->get_elements().size(), 1);
    EXPECT_EQ(localized_parent_type->get_element_name(0), "cc");
}

// Scenario: struct child access through a computed map/array parent is not localized as a file
// conjunct, because the projected value struct can have a different physical child order.
TEST(ColumnMapperScanRequestTest, MapValuesStructChildConjunctStaysTableLevel) {
    const auto key_type = str();
    const auto string_type = str();
    const auto int_type = i32();

    auto table_gender = field_id_col("gender", 17, string_type);
    auto table_full_name = field_id_col("full_name", 7, string_type);
    auto table_value = struct_col("value", 6, {table_gender, table_full_name});
    auto table_map = map_col("new_map_column", 2, {table_value}, key_type, table_value.type);

    auto file_key = field_id_col("key", 5, key_type, 0);
    auto file_age = field_id_col("age", 8, int_type, 0);
    auto file_full_name = field_id_col("full_name", 7, string_type, 1);
    auto file_gender = field_id_col("gender", 17, string_type, 2);
    auto file_value = struct_col("value", 6, {file_age, file_full_name, file_gender}, 1);
    auto file_map =
            map_col("new_map_column", 2, {file_key, file_value}, key_type, file_value.type, 1);

    const auto map_slot = table_slot(0, 0, table_map.type, "new_map_column");
    const auto values_expr = map_values(map_slot, table_value.type);
    const auto first_value = array_element_at(values_expr, table_value.type, 1);
    const auto full_name_expr = element_at(first_value, string_type, "full_name");
    auto filter_expr = like_expr(full_name_expr, "J%");
    TableFilter filter {.conjunct = VExprContext::create_shared(filter_expr),
                        .global_indices = {GlobalIndex(0)}};

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_FIELD_ID});
    ASSERT_TRUE(mapper.create_mapping({table_map}, {}, {file_map}).ok());

    FileScanRequest request;
    ASSERT_TRUE(mapper.create_scan_request({filter}, {table_map}, &request).ok());

    EXPECT_TRUE(request.conjuncts.empty());
    EXPECT_TRUE(request.predicate_columns.empty());
    ASSERT_EQ(request.non_predicate_columns.size(), 1);
    EXPECT_EQ(request.non_predicate_columns[0].column_id(), LocalColumnId(1));
}

// Scenario: MAP_KEYS only reads map keys, but localizing it by wrapping the evolved file map slot
// in CAST(file_map AS table_map) would still cast the old value struct to the new value struct.
// Keep the conjunct table-level when the map value schema changed.
TEST(ColumnMapperScanRequestTest, MapKeysConjunctWithEvolvedValueStructStaysTableLevel) {
    const auto key_type = str();
    const auto string_type = str();
    const auto int_type = i32();

    auto table_age = field_id_col("age", 8, int_type);
    auto table_full_name = field_id_col("full_name", 7, string_type);
    auto table_gender = field_id_col("gender", 17, string_type);
    auto table_value = struct_col("value", 6, {table_age, table_full_name, table_gender});
    auto table_key = field_id_col("key", 5, key_type);
    auto table_map =
            map_col("new_map_column", 2, {table_key, table_value}, key_type, table_value.type);

    auto file_key = field_id_col("key", 5, key_type, 0);
    auto file_name = field_id_col("name", 18, string_type, 0);
    auto file_age = field_id_col("age", 8, int_type, 1);
    auto file_value = struct_col("value", 6, {file_name, file_age}, 1);
    auto file_map = map_col("map_column", 2, {file_key, file_value}, key_type, file_value.type, 1);

    const auto map_slot = table_slot(0, 0, table_map.type, "new_map_column");
    const auto keys_expr = map_keys(map_slot, key_type);
    auto filter_expr = array_contains(
            keys_expr, literal(key_type, Field::create_field<TYPE_STRING>("person5")));
    TableFilter filter {.conjunct = VExprContext::create_shared(filter_expr),
                        .global_indices = {GlobalIndex(0)}};

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_FIELD_ID});
    ASSERT_TRUE(mapper.create_mapping({table_map}, {}, {file_map}).ok());

    FileScanRequest request;
    ASSERT_TRUE(mapper.create_scan_request({filter}, {table_map}, &request).ok());

    EXPECT_TRUE(request.conjuncts.empty());
    EXPECT_TRUE(request.predicate_columns.empty());
    ASSERT_EQ(request.non_predicate_columns.size(), 1);
    EXPECT_EQ(request.non_predicate_columns[0].column_id(), LocalColumnId(1));
}

// Scenario: an array element struct projection only contains missing/default children; the mapper
// falls back to reading the full physical element so the reader never gets an empty projection.
TEST(ColumnMapperScanRequestTest, ArrayStructOnlyMissingElementChildUsesFullFileProjection) {
    const auto int_type = i32();
    const auto string_type = str();

    auto file_a = field_id_col("a", 1, int_type, 0);
    auto file_b = field_id_col("b", 2, int_type, 1);
    auto file_element = struct_col("element", 0, {file_a, file_b}, 0);
    auto file_array = array_col("xs", 10, file_element, 10);

    auto missing_child = field_id_col("missing_child", 99, string_type);
    auto table_element = struct_col("element", 0, {missing_child});
    auto table_array = array_col("xs", 10, table_element);

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_FIELD_ID});
    ASSERT_TRUE(mapper.create_mapping({table_array}, {}, {file_array}).ok());

    FileScanRequest request;
    ASSERT_TRUE(mapper.create_scan_request({}, {table_array}, &request).ok());

    ASSERT_EQ(request.non_predicate_columns.size(), 1);
    EXPECT_EQ(request.non_predicate_columns[0].column_id(), LocalColumnId(10));
    EXPECT_TRUE(request.non_predicate_columns[0].project_all_children);
    EXPECT_TRUE(request.non_predicate_columns[0].children.empty());
    ASSERT_EQ(mapper.mappings().size(), 1);
    EXPECT_FALSE(mapper.mappings()[0].is_trivial);
}

// Scenario: a map value struct projection only contains missing/default children; the mapper keeps
// the map key/value shape and reads the full physical value struct instead of an empty value child.
TEST(ColumnMapperScanRequestTest, MapValueStructOnlyMissingChildUsesFullValueProjection) {
    const auto key_type = i32();
    const auto int_type = i32();
    const auto string_type = str();

    auto file_key = field_id_col("key", 0, key_type, 0);
    auto file_a = field_id_col("a", 1, int_type, 0);
    auto file_b = field_id_col("b", 2, int_type, 1);
    auto file_value = struct_col("value", 1, {file_a, file_b}, 1);
    auto file_map = map_col("m", 10, {file_key, file_value}, key_type, file_value.type, 10);

    auto missing_child = field_id_col("missing_child", 99, string_type);
    auto table_value = struct_col("value", 1, {missing_child});
    auto table_map = map_col("m", 10, {table_value}, key_type, table_value.type);

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_FIELD_ID});
    ASSERT_TRUE(mapper.create_mapping({table_map}, {}, {file_map}).ok());

    FileScanRequest request;
    ASSERT_TRUE(mapper.create_scan_request({}, {table_map}, &request).ok());

    ASSERT_EQ(request.non_predicate_columns.size(), 1);
    const auto& projection = request.non_predicate_columns[0];
    EXPECT_EQ(projection.column_id(), LocalColumnId(10));
    ASSERT_FALSE(projection.project_all_children);
    ASSERT_EQ(projection.children.size(), 1);
    EXPECT_EQ(projection.children[0].local_id(), 1);
    EXPECT_TRUE(projection.children[0].project_all_children);
    EXPECT_TRUE(projection.children[0].children.empty());
    ASSERT_EQ(mapper.mappings().size(), 1);
    EXPECT_FALSE(mapper.mappings()[0].is_trivial);
}

// ----------------------------------------------------------------------
// L1 complex schema evolution and split isolation.
// These tests call the mapper repeatedly with different file schemas and
// verify that split-local state is rebuilt instead of leaked.
// ----------------------------------------------------------------------

TEST(ColumnMapperSchemaEvolutionTest, StructChildrenHandleMissingRenameReorderAndDroppedFields) {
    const auto int_type = i32();
    const auto string_type = str();
    auto table_a = field_id_col("a", 1, int_type);
    auto table_renamed_b = field_id_col("renamed_b", 2, string_type);
    auto table_c = field_id_col("c", 3, int_type);
    auto table_struct = struct_col("s", 10, {table_a, table_renamed_b, table_c});

    auto v1_a = field_id_col("a", 1, int_type, 0);
    auto v1_b = field_id_col("b", 2, string_type, 1);
    auto file_v1 = struct_col("s", 10, {v1_a, v1_b}, 5);

    auto v2_b = field_id_col("b", 2, string_type, 0);
    auto v2_a = field_id_col("a", 1, int_type, 1);
    auto v2_c = field_id_col("c", 3, int_type, 2);
    auto file_v2 = struct_col("s", 10, {v2_b, v2_a, v2_c}, 8);

    TableColumnMapper v1_mapper({.mode = TableColumnMappingMode::BY_FIELD_ID});
    ASSERT_TRUE(v1_mapper.create_mapping({table_struct}, {}, {file_v1}).ok());
    FileScanRequest v1_request;
    ASSERT_TRUE(v1_mapper.create_scan_request({}, {table_struct}, &v1_request).ok());

    const auto& v1_mapping = v1_mapper.mappings()[0];
    ASSERT_EQ(v1_mapping.child_mappings.size(), 3);
    EXPECT_EQ(*v1_mapping.child_mappings[0].file_local_id, 0);
    EXPECT_EQ(*v1_mapping.child_mappings[1].file_local_id, 1);
    EXPECT_FALSE(v1_mapping.child_mappings[2].file_local_id.has_value());
    ASSERT_EQ(v1_request.non_predicate_columns.size(), 1);
    EXPECT_EQ(v1_request.non_predicate_columns[0].column_id(), LocalColumnId(5));
    EXPECT_TRUE(v1_request.non_predicate_columns[0].project_all_children);

    TableColumnMapper v2_mapper({.mode = TableColumnMappingMode::BY_FIELD_ID});
    ASSERT_TRUE(v2_mapper.create_mapping({table_struct}, {}, {file_v2}).ok());
    FileScanRequest v2_request;
    ASSERT_TRUE(v2_mapper.create_scan_request({}, {table_struct}, &v2_request).ok());

    const auto& v2_mapping = v2_mapper.mappings()[0];
    ASSERT_EQ(v2_mapping.child_mappings.size(), 3);
    EXPECT_EQ(*v2_mapping.child_mappings[0].file_local_id, 1);
    EXPECT_EQ(*v2_mapping.child_mappings[1].file_local_id, 0);
    EXPECT_EQ(*v2_mapping.child_mappings[2].file_local_id, 2);
    ASSERT_EQ(v2_request.non_predicate_columns.size(), 1);
    EXPECT_EQ(v2_request.non_predicate_columns[0].column_id(), LocalColumnId(8));
    EXPECT_TRUE(v2_request.non_predicate_columns[0].project_all_children);
}

TEST(ColumnMapperSchemaEvolutionTest, DroppedStructChildrenAreNotRead) {
    const auto int_type = i32();
    const auto string_type = str();
    auto table_a = field_id_col("a", 1, int_type);
    auto table_struct = struct_col("s", 10, {table_a});

    auto file_a = field_id_col("a", 1, int_type, 0);
    auto file_b = field_id_col("b", 2, string_type, 1);
    auto file_c = field_id_col("c", 3, int_type, 2);
    auto file_struct = struct_col("s", 10, {file_a, file_b, file_c}, 5);

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_FIELD_ID});
    ASSERT_TRUE(mapper.create_mapping({table_struct}, {}, {file_struct}).ok());

    FileScanRequest request;
    ASSERT_TRUE(mapper.create_scan_request({}, {table_struct}, &request).ok());

    ASSERT_EQ(request.non_predicate_columns.size(), 1);
    const auto& projection = request.non_predicate_columns[0];
    EXPECT_EQ(projection.column_id(), LocalColumnId(5));
    ASSERT_FALSE(projection.project_all_children);
    EXPECT_EQ(projection_ids(projection.children), std::vector<int32_t>({0}));
}

TEST(ColumnMapperSchemaEvolutionTest, ReusedMapperClearsSplitLocalConstantsAndFileIds) {
    const auto int_type = i32();
    auto id = name_col("id", int_type);
    auto added = name_col("added", int_type);
    added.default_expr =
            VExprContext::create_shared(literal(int_type, Field::create_field<TYPE_INT>(7)));
    const std::vector<ColumnDefinition> table_schema = {id, added};

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    ASSERT_TRUE(mapper.create_mapping(table_schema, {}, {name_col("id", int_type, 0)}).ok());
    ASSERT_EQ(mapper.mappings().size(), 2);
    EXPECT_EQ(*mapper.mappings()[0].file_local_id, 0);
    expect_constant(mapper, mapper.mappings()[1], 1, int_type);

    ASSERT_TRUE(mapper.create_mapping(table_schema, {},
                                      {name_col("id", int_type, 3), name_col("added", int_type, 4)})
                        .ok());
    ASSERT_EQ(mapper.mappings().size(), 2);
    EXPECT_EQ(*mapper.mappings()[0].file_local_id, 3);
    EXPECT_EQ(*mapper.mappings()[1].file_local_id, 4);
    EXPECT_TRUE(mapper.constant_map().empty());
}

// ----------------------------------------------------------------------
// L2 cast-aware filter localization tests.
// These tests belong to TableColumnMapper rather than Cast: they assert when the mapper builds
// projection casts, rewrites table predicates to file-local slot casts, converts literals to the
// current split's file type, and keeps repeated scan-request rewrites idempotent.
// ----------------------------------------------------------------------

// Scenario: table/file primitive types differ, so the visible mapping must build a cast projection.
TEST_F(ColumnMapperCastTest, ColumnMapperBuildsCastProjectionForTypeMismatch) {
    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    auto table_column = name_col("value", i64());
    std::vector<ColumnDefinition> projected_columns {table_column};

    auto file_field = name_col("value", i32(), 0);
    std::vector<ColumnDefinition> file_schema {file_field};

    auto status = mapper.create_mapping(projected_columns, {}, file_schema);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(mapper.mappings().size(), 1);
    FileScanRequest file_request;
    status = mapper.create_scan_request({}, projected_columns, &file_request);
    ASSERT_TRUE(status.ok()) << status;
    const auto& mapping = mapper.mappings()[0];
    EXPECT_FALSE(mapping.is_trivial);
    ASSERT_NE(mapping.projection, nullptr);

    Block block;
    block.insert(ColumnHelper::create_column_with_name<DataTypeInt32>({11, 22}));
    int result_column_id = -1;
    status = prepare_open_execute(mapping.projection.get(), &block, &result_column_id);
    ASSERT_TRUE(status.ok()) << status;

    const auto& result_column =
            assert_cast<const ColumnInt64&>(*block.get_by_position(result_column_id).column);
    EXPECT_EQ(result_column.get_data()[0], 11);
    EXPECT_EQ(result_column.get_data()[1], 22);

    mapping.projection->close();
}

// Scenario: equivalent table/file types keep the mapping trivial and avoid unnecessary projection casts.
TEST_F(ColumnMapperCastTest, ColumnMapperTreatsEquivalentTypesAsTrivial) {
    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    auto table_column = name_col("value", i32());
    std::vector<ColumnDefinition> projected_columns {table_column};

    auto file_field = name_col("value", i32(), 0);
    std::vector<ColumnDefinition> file_schema {file_field};

    auto status = mapper.create_mapping(projected_columns, {}, file_schema);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(mapper.mappings().size(), 1);
    EXPECT_TRUE(mapper.mappings()[0].is_trivial);
}

// Scenario: a table predicate on a widened type is localized by casting the file slot to table type.
TEST_F(ColumnMapperCastTest, ColumnMapperBuildsCastFilterForTypeMismatch) {
    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    auto table_column = name_col("value", i64());
    std::vector<ColumnDefinition> projected_columns {table_column};

    auto file_field = name_col("value", i32(), 0);
    std::vector<ColumnDefinition> file_schema {file_field};

    auto status = mapper.create_mapping(projected_columns, {}, file_schema);
    ASSERT_TRUE(status.ok()) << status;

    auto predicate = std::make_shared<Int64ChildGreaterThanExpr>(15);
    predicate->add_child(VSlotRef::create_shared(0, 0, -1, table_column.type, "value"));
    TableFilter table_filter;
    table_filter.conjunct = VExprContext::create_shared(predicate);
    table_filter.global_indices = {GlobalIndex(0)};

    FileScanRequest file_request;
    ASSERT_TRUE(mapper.create_scan_request({table_filter}, projected_columns, &file_request, &state)
                        .ok());
    ASSERT_EQ(file_request.conjuncts.size(), 1);
    ASSERT_EQ(projection_ids(file_request.predicate_columns), std::vector<int32_t>({0}));
    const auto& localized_expr = file_request.conjuncts[0]->root();
    ASSERT_EQ(localized_expr->get_num_children(), 1);
    const auto& localized_child = localized_expr->children()[0];
    ASSERT_NE(dynamic_cast<const Cast*>(localized_child.get()), nullptr);
    ASSERT_EQ(localized_child->get_num_children(), 1);
    const auto* localized_slot = assert_cast<const VSlotRef*>(localized_child->children()[0].get());
    EXPECT_EQ(localized_slot->column_id(), 0);
    EXPECT_TRUE(localized_slot->data_type()->equals(*file_field.type));
    EXPECT_TRUE(localized_child->data_type()->equals(*table_column.type));

    Block block;
    block.insert(ColumnHelper::create_column_with_name<DataTypeInt32>({11, 22}));
    auto* conjunct = file_request.conjuncts[0].get();
    status = conjunct->prepare(&state, RowDescriptor());
    ASSERT_TRUE(status.ok()) << status;
    status = conjunct->open(&state);
    ASSERT_TRUE(status.ok()) << status;
    IColumn::Filter filter(block.rows(), 1);
    bool can_filter_all = false;
    status = conjunct->execute_filter(&block, filter.data(), block.rows(), false, &can_filter_all);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_FALSE(can_filter_all);
    ASSERT_EQ(filter.size(), 2);
    EXPECT_EQ(filter[0], 0);
    EXPECT_EQ(filter[1], 1);

    file_request.conjuncts[0]->close();
}

// Scenario: an already prepared table filter can still be cloned, rewritten, prepared, and opened as a file-local filter.
TEST_F(ColumnMapperCastTest, ColumnMapperRepreparesRewrittenPreparedFilter) {
    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    auto table_column = name_col("value", i64());
    std::vector<ColumnDefinition> projected_columns {table_column};

    auto file_field = name_col("value", i32(), 0);
    std::vector<ColumnDefinition> file_schema {file_field};

    auto status = mapper.create_mapping(projected_columns, {}, file_schema);
    ASSERT_TRUE(status.ok()) << status;

    auto cast = Cast::create_shared(table_column.type);
    cast->add_child(VSlotRef::create_shared(0, 0, -1, table_column.type, "value"));
    TableFilter table_filter;
    table_filter.conjunct = VExprContext::create_shared(cast);
    table_filter.global_indices = {GlobalIndex(0)};
    status = table_filter.conjunct->prepare(&state, RowDescriptor());
    ASSERT_TRUE(status.ok()) << status;
    status = table_filter.conjunct->open(&state);
    ASSERT_TRUE(status.ok()) << status;

    FileScanRequest file_request;
    ASSERT_TRUE(mapper.create_scan_request({table_filter}, projected_columns, &file_request, &state)
                        .ok());
    ASSERT_EQ(file_request.conjuncts.size(), 1);
    const auto& localized_expr = file_request.conjuncts[0]->root();
    ASSERT_NE(dynamic_cast<const Cast*>(localized_expr.get()), nullptr);
    ASSERT_EQ(localized_expr->get_num_children(), 1);
    const auto* localized_slot = assert_cast<const VSlotRef*>(localized_expr->children()[0].get());
    EXPECT_EQ(localized_slot->column_id(), 0);
    EXPECT_TRUE(localized_slot->data_type()->equals(*file_field.type));

    status = file_request.conjuncts[0]->prepare(&state, RowDescriptor());
    ASSERT_TRUE(status.ok()) << status;
    status = file_request.conjuncts[0]->open(&state);
    ASSERT_TRUE(status.ok()) << status;

    file_request.conjuncts[0]->close();
}

// Scenario: slot-literal comparison rewrites the literal to the current file type when conversion is safe.
TEST_F(ColumnMapperCastTest, ColumnMapperCastsLiteralForSlotLiteralPredicateTypeMismatch) {
    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    auto table_column = name_col("value", i64());
    std::vector<ColumnDefinition> projected_columns {table_column};

    auto file_field = name_col("value", i32(), 0);
    std::vector<ColumnDefinition> file_schema {file_field};

    auto status = mapper.create_mapping(projected_columns, {}, file_schema);
    ASSERT_TRUE(status.ok()) << status;

    auto predicate = std::make_shared<Int64BinaryPredicateExpr>(TExprOpcode::GT);
    predicate->add_child(VSlotRef::create_shared(0, 0, -1, table_column.type, "value"));
    predicate->add_child(
            VLiteral::create_shared(table_column.type, Field::create_field<TYPE_BIGINT>(15)));
    TableFilter table_filter;
    table_filter.conjunct = VExprContext::create_shared(predicate);
    table_filter.global_indices = {GlobalIndex(0)};

    FileScanRequest file_request;
    ASSERT_TRUE(mapper.create_scan_request({table_filter}, projected_columns, &file_request, &state)
                        .ok());
    ASSERT_EQ(file_request.conjuncts.size(), 1);
    ASSERT_EQ(projection_ids(file_request.predicate_columns), std::vector<int32_t>({0}));
    const auto& localized_expr = file_request.conjuncts[0]->root();
    ASSERT_EQ(localized_expr->get_num_children(), 2);
    const auto* localized_slot = assert_cast<const VSlotRef*>(localized_expr->children()[0].get());
    EXPECT_EQ(localized_slot->column_id(), 0);
    EXPECT_TRUE(localized_slot->data_type()->equals(*file_field.type));
    const auto& localized_literal = localized_expr->children()[1];
    EXPECT_TRUE(localized_literal->is_literal());
    EXPECT_TRUE(localized_literal->data_type()->equals(*file_field.type));

    Block block;
    block.insert(ColumnHelper::create_column_with_name<DataTypeInt32>({11, 22}));
    auto* conjunct = file_request.conjuncts[0].get();
    status = conjunct->prepare(&state, RowDescriptor());
    ASSERT_TRUE(status.ok()) << status;
    status = conjunct->open(&state);
    ASSERT_TRUE(status.ok()) << status;
    IColumn::Filter filter(block.rows(), 1);
    bool can_filter_all = false;
    status = conjunct->execute_filter(&block, filter.data(), block.rows(), false, &can_filter_all);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_FALSE(can_filter_all);
    ASSERT_EQ(filter.size(), 2);
    EXPECT_EQ(filter[0], 0);
    EXPECT_EQ(filter[1], 1);

    file_request.conjuncts[0]->close();
}

// Scenario: literal-slot comparison also rewrites the literal side and preserves operand order.
TEST_F(ColumnMapperCastTest, ColumnMapperCastsLiteralForLiteralSlotPredicateTypeMismatch) {
    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    auto table_column = name_col("value", i64());
    std::vector<ColumnDefinition> projected_columns {table_column};

    auto file_field = name_col("value", i32(), 0);
    std::vector<ColumnDefinition> file_schema {file_field};

    auto status = mapper.create_mapping(projected_columns, {}, file_schema);
    ASSERT_TRUE(status.ok()) << status;

    auto predicate = std::make_shared<Int64BinaryPredicateExpr>(TExprOpcode::LT);
    predicate->add_child(
            VLiteral::create_shared(table_column.type, Field::create_field<TYPE_BIGINT>(15)));
    predicate->add_child(VSlotRef::create_shared(0, 0, -1, table_column.type, "value"));
    TableFilter table_filter;
    table_filter.conjunct = VExprContext::create_shared(predicate);
    table_filter.global_indices = {GlobalIndex(0)};

    FileScanRequest file_request;
    ASSERT_TRUE(mapper.create_scan_request({table_filter}, projected_columns, &file_request, &state)
                        .ok());
    ASSERT_EQ(file_request.conjuncts.size(), 1);
    const auto& localized_expr = file_request.conjuncts[0]->root();
    ASSERT_EQ(localized_expr->get_num_children(), 2);
    const auto& localized_literal = localized_expr->children()[0];
    EXPECT_TRUE(localized_literal->is_literal());
    EXPECT_TRUE(localized_literal->data_type()->equals(*file_field.type));
    const auto* localized_slot = assert_cast<const VSlotRef*>(localized_expr->children()[1].get());
    EXPECT_EQ(localized_slot->column_id(), 0);
    EXPECT_TRUE(localized_slot->data_type()->equals(*file_field.type));

    Block block;
    block.insert(ColumnHelper::create_column_with_name<DataTypeInt32>({11, 22}));
    auto* conjunct = file_request.conjuncts[0].get();
    status = conjunct->prepare(&state, RowDescriptor());
    ASSERT_TRUE(status.ok()) << status;
    status = conjunct->open(&state);
    ASSERT_TRUE(status.ok()) << status;
    IColumn::Filter filter(block.rows(), 1);
    bool can_filter_all = false;
    status = conjunct->execute_filter(&block, filter.data(), block.rows(), false, &can_filter_all);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_FALSE(can_filter_all);
    ASSERT_EQ(filter.size(), 2);
    EXPECT_EQ(filter[0], 0);
    EXPECT_EQ(filter[1], 1);

    file_request.conjuncts[0]->close();
}

// Scenario: a fractional table literal cannot be localized to an integral file type without
// changing the predicate boundary, so the mapper must cast the file slot instead.
TEST_F(ColumnMapperCastTest, ColumnMapperRejectsLossyBinaryLiteralConversion) {
    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    auto table_column = name_col("value", f64());
    std::vector<ColumnDefinition> projected_columns {table_column};

    auto file_field = name_col("value", i32(), 0);
    std::vector<ColumnDefinition> file_schema {file_field};

    auto status = mapper.create_mapping(projected_columns, {}, file_schema);
    ASSERT_TRUE(status.ok()) << status;

    auto predicate = binary_predicate(
            TExprOpcode::LT, VSlotRef::create_shared(0, 0, -1, table_column.type, "value"),
            VLiteral::create_shared(table_column.type, Field::create_field<TYPE_DOUBLE>(1.5)));
    TableFilter table_filter;
    table_filter.conjunct = VExprContext::create_shared(predicate);
    table_filter.global_indices = {GlobalIndex(0)};

    FileScanRequest file_request;
    ASSERT_TRUE(mapper.create_scan_request({table_filter}, projected_columns, &file_request, &state)
                        .ok());
    ASSERT_EQ(file_request.conjuncts.size(), 1);
    const auto& localized_expr = file_request.conjuncts[0]->root();
    ASSERT_EQ(localized_expr->get_num_children(), 2);
    const auto& localized_slot_cast = localized_expr->children()[0];
    ASSERT_NE(dynamic_cast<const Cast*>(localized_slot_cast.get()), nullptr);
    EXPECT_TRUE(localized_slot_cast->data_type()->equals(*table_column.type));
    ASSERT_EQ(localized_slot_cast->get_num_children(), 1);
    const auto* localized_slot =
            assert_cast<const VSlotRef*>(localized_slot_cast->children()[0].get());
    EXPECT_EQ(localized_slot->column_id(), 0);
    EXPECT_TRUE(localized_slot->data_type()->equals(*file_field.type));
    EXPECT_TRUE(localized_expr->children()[1]->is_literal());
    EXPECT_TRUE(localized_expr->children()[1]->data_type()->equals(*table_column.type));
}

// Scenario: an exactly representable literal is still unsafe to localize when arbitrary file
// values lose information during materialization to the table type.
TEST_F(ColumnMapperCastTest, ColumnMapperRejectsLossyFileToTableConversion) {
    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    auto table_column = name_col("value", i64());
    std::vector<ColumnDefinition> projected_columns {table_column};

    auto file_field = name_col("value", f64(), 0);
    std::vector<ColumnDefinition> file_schema {file_field};

    auto status = mapper.create_mapping(projected_columns, {}, file_schema);
    ASSERT_TRUE(status.ok()) << status;

    auto predicate = binary_predicate(
            TExprOpcode::EQ, VSlotRef::create_shared(0, 0, -1, table_column.type, "value"),
            VLiteral::create_shared(table_column.type, Field::create_field<TYPE_BIGINT>(1)));
    TableFilter table_filter;
    table_filter.conjunct = VExprContext::create_shared(predicate);
    table_filter.global_indices = {GlobalIndex(0)};

    FileScanRequest file_request;
    ASSERT_TRUE(mapper.create_scan_request({table_filter}, projected_columns, &file_request, &state)
                        .ok());
    ASSERT_EQ(file_request.conjuncts.size(), 1);
    const auto& localized_expr = file_request.conjuncts[0]->root();
    ASSERT_EQ(localized_expr->get_num_children(), 2);
    const auto& localized_slot_cast = localized_expr->children()[0];
    ASSERT_NE(dynamic_cast<const Cast*>(localized_slot_cast.get()), nullptr);
    EXPECT_TRUE(localized_slot_cast->data_type()->equals(*table_column.type));
    ASSERT_EQ(localized_slot_cast->get_num_children(), 1);
    const auto* localized_slot =
            assert_cast<const VSlotRef*>(localized_slot_cast->children()[0].get());
    EXPECT_EQ(localized_slot->column_id(), 0);
    EXPECT_TRUE(localized_slot->data_type()->equals(*file_field.type));
    EXPECT_TRUE(localized_expr->children()[1]->data_type()->equals(*table_column.type));
}

// Scenario: complex Field equality does not compare nested values, so complex literals must not
// use the scalar round-trip guard.
TEST_F(ColumnMapperCastTest, ColumnMapperRejectsComplexLiteralLocalization) {
    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    auto table_column = array_col("value", -1, name_col("element", f64()));
    set_name_identifiers(&table_column, -1);
    const auto& table_type = table_column.type;
    std::vector<ColumnDefinition> projected_columns {table_column};

    auto file_field = array_col("value", -1, name_col("element", i32()), 0);
    set_name_identifiers(&file_field, 0);
    std::vector<ColumnDefinition> file_schema {file_field};

    auto status = mapper.create_mapping(projected_columns, {}, file_schema);
    ASSERT_TRUE(status.ok()) << status;

    Array literal_values {Field::create_field<TYPE_DOUBLE>(1.5)};
    auto predicate = binary_predicate(
            TExprOpcode::EQ, VSlotRef::create_shared(0, 0, -1, table_type, "value"),
            VLiteral::create_shared(table_type, Field::create_field<TYPE_ARRAY>(literal_values)));
    TableFilter table_filter;
    table_filter.conjunct = VExprContext::create_shared(predicate);
    table_filter.global_indices = {GlobalIndex(0)};

    FileScanRequest file_request;
    ASSERT_TRUE(mapper.create_scan_request({table_filter}, projected_columns, &file_request, &state)
                        .ok());
    EXPECT_TRUE(file_request.conjuncts.empty());
}

// Scenario: IN predicate literals are all rewritten to file type when every literal conversion is safe.
TEST_F(ColumnMapperCastTest, ColumnMapperCastsInPredicateLiteralsForTypeMismatch) {
    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    auto table_column = name_col("value", i64());
    std::vector<ColumnDefinition> projected_columns {table_column};

    auto file_field = name_col("value", i32(), 0);
    std::vector<ColumnDefinition> file_schema {file_field};

    auto status = mapper.create_mapping(projected_columns, {}, file_schema);
    ASSERT_TRUE(status.ok()) << status;

    auto predicate = create_in_predicate();
    predicate->add_child(VSlotRef::create_shared(0, 0, -1, table_column.type, "value"));
    predicate->add_child(
            VLiteral::create_shared(table_column.type, Field::create_field<TYPE_BIGINT>(15)));
    predicate->add_child(
            VLiteral::create_shared(table_column.type, Field::create_field<TYPE_BIGINT>(22)));
    TableFilter table_filter;
    table_filter.conjunct = VExprContext::create_shared(predicate);
    table_filter.global_indices = {GlobalIndex(0)};

    FileScanRequest file_request;
    ASSERT_TRUE(mapper.create_scan_request({table_filter}, projected_columns, &file_request, &state)
                        .ok());
    ASSERT_EQ(file_request.conjuncts.size(), 1);
    ASSERT_EQ(projection_ids(file_request.predicate_columns), std::vector<int32_t>({0}));
    const auto& localized_expr = file_request.conjuncts[0]->root();
    ASSERT_EQ(localized_expr->get_num_children(), 3);
    const auto* localized_slot = assert_cast<const VSlotRef*>(localized_expr->children()[0].get());
    EXPECT_EQ(localized_slot->column_id(), 0);
    EXPECT_TRUE(localized_slot->data_type()->equals(*file_field.type));
    EXPECT_TRUE(localized_expr->children()[1]->is_literal());
    EXPECT_TRUE(localized_expr->children()[1]->data_type()->equals(*file_field.type));
    EXPECT_TRUE(localized_expr->children()[2]->is_literal());
    EXPECT_TRUE(localized_expr->children()[2]->data_type()->equals(*file_field.type));
}

// Scenario: one lossy IN literal prevents the entire predicate from being localized to file type.
TEST_F(ColumnMapperCastTest, ColumnMapperRejectsLossyInPredicateLiteralConversion) {
    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    auto table_column = name_col("value", f64());
    std::vector<ColumnDefinition> projected_columns {table_column};

    auto file_field = name_col("value", i32(), 0);
    std::vector<ColumnDefinition> file_schema {file_field};

    auto status = mapper.create_mapping(projected_columns, {}, file_schema);
    ASSERT_TRUE(status.ok()) << status;

    auto predicate = create_in_predicate();
    predicate->add_child(VSlotRef::create_shared(0, 0, -1, table_column.type, "value"));
    predicate->add_child(
            VLiteral::create_shared(table_column.type, Field::create_field<TYPE_DOUBLE>(1.0)));
    predicate->add_child(
            VLiteral::create_shared(table_column.type, Field::create_field<TYPE_DOUBLE>(1.5)));
    TableFilter table_filter;
    table_filter.conjunct = VExprContext::create_shared(predicate);
    table_filter.global_indices = {GlobalIndex(0)};

    FileScanRequest file_request;
    ASSERT_TRUE(mapper.create_scan_request({table_filter}, projected_columns, &file_request, &state)
                        .ok());
    ASSERT_EQ(file_request.conjuncts.size(), 1);
    const auto& localized_expr = file_request.conjuncts[0]->root();
    ASSERT_EQ(localized_expr->get_num_children(), 3);
    const auto& localized_slot_cast = localized_expr->children()[0];
    ASSERT_NE(dynamic_cast<const Cast*>(localized_slot_cast.get()), nullptr);
    EXPECT_TRUE(localized_slot_cast->data_type()->equals(*table_column.type));
    ASSERT_EQ(localized_slot_cast->get_num_children(), 1);
    const auto* localized_slot =
            assert_cast<const VSlotRef*>(localized_slot_cast->children()[0].get());
    EXPECT_EQ(localized_slot->column_id(), 0);
    EXPECT_TRUE(localized_slot->data_type()->equals(*file_field.type));
    EXPECT_TRUE(localized_expr->children()[1]->is_literal());
    EXPECT_TRUE(localized_expr->children()[1]->data_type()->equals(*table_column.type));
    EXPECT_TRUE(localized_expr->children()[2]->is_literal());
    EXPECT_TRUE(localized_expr->children()[2]->data_type()->equals(*table_column.type));
}

// Scenario: IN predicate falls back to casting the file slot when any literal cannot be converted safely.
TEST_F(ColumnMapperCastTest, ColumnMapperFallsBackToSlotCastWhenInPredicateLiteralRewriteFails) {
    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    auto table_column = name_col("value", str());
    std::vector<ColumnDefinition> projected_columns {table_column};

    auto file_field = name_col("value", i32(), 0);
    std::vector<ColumnDefinition> file_schema {file_field};

    auto status = mapper.create_mapping(projected_columns, {}, file_schema);
    ASSERT_TRUE(status.ok()) << status;

    auto predicate = create_in_predicate();
    predicate->add_child(VSlotRef::create_shared(0, 0, -1, table_column.type, "value"));
    predicate->add_child(
            VLiteral::create_shared(table_column.type, Field::create_field<TYPE_STRING>("10")));
    predicate->add_child(
            VLiteral::create_shared(table_column.type, Field::create_field<TYPE_STRING>("bad")));
    TableFilter table_filter;
    table_filter.conjunct = VExprContext::create_shared(predicate);
    table_filter.global_indices = {GlobalIndex(0)};

    FileScanRequest file_request;
    ASSERT_TRUE(mapper.create_scan_request({table_filter}, projected_columns, &file_request, &state)
                        .ok());
    ASSERT_EQ(file_request.conjuncts.size(), 1);
    const auto& localized_expr = file_request.conjuncts[0]->root();
    ASSERT_EQ(localized_expr->get_num_children(), 3);
    const auto& localized_child = localized_expr->children()[0];
    ASSERT_NE(dynamic_cast<const Cast*>(localized_child.get()), nullptr);
    ASSERT_EQ(localized_child->get_num_children(), 1);
    const auto* localized_slot = assert_cast<const VSlotRef*>(localized_child->children()[0].get());
    EXPECT_EQ(localized_slot->column_id(), 0);
    EXPECT_TRUE(localized_slot->data_type()->equals(*file_field.type));
    EXPECT_TRUE(localized_child->data_type()->equals(*table_column.type));
    EXPECT_TRUE(localized_expr->children()[1]->is_literal());
    EXPECT_TRUE(localized_expr->children()[1]->data_type()->equals(*table_column.type));
    EXPECT_TRUE(localized_expr->children()[2]->is_literal());
    EXPECT_TRUE(localized_expr->children()[2]->data_type()->equals(*table_column.type));
}

// Scenario: split-local IN literal rewrites do not mutate the original table filter across different file schemas.
TEST_F(ColumnMapperCastTest, ColumnMapperDoesNotLeakRewrittenInPredicateLiteralAcrossSplits) {
    auto table_column = name_col("value", i64());
    std::vector<ColumnDefinition> projected_columns {table_column};

    auto predicate = create_in_predicate();
    predicate->add_child(VSlotRef::create_shared(0, 0, -1, table_column.type, "value"));
    predicate->add_child(
            VLiteral::create_shared(table_column.type, Field::create_field<TYPE_BIGINT>(15)));
    predicate->add_child(
            VLiteral::create_shared(table_column.type, Field::create_field<TYPE_BIGINT>(22)));
    TableFilter table_filter;
    table_filter.conjunct = VExprContext::create_shared(predicate);
    table_filter.global_indices = {GlobalIndex(0)};

    auto int_file_field = name_col("value", i32(), 0);
    TableColumnMapper int_mapper({.mode = TableColumnMappingMode::BY_NAME});
    ASSERT_TRUE(int_mapper.create_mapping(projected_columns, {}, {int_file_field}).ok());
    FileScanRequest int_request;
    ASSERT_TRUE(
            int_mapper.create_scan_request({table_filter}, projected_columns, &int_request, &state)
                    .ok());
    ASSERT_EQ(int_request.conjuncts.size(), 1);
    const auto& int_localized_expr = int_request.conjuncts[0]->root();
    ASSERT_EQ(int_localized_expr->get_num_children(), 3);
    EXPECT_TRUE(int_localized_expr->children()[1]->is_literal());
    EXPECT_TRUE(int_localized_expr->children()[1]->data_type()->equals(*int_file_field.type));
    EXPECT_TRUE(int_localized_expr->children()[2]->is_literal());
    EXPECT_TRUE(int_localized_expr->children()[2]->data_type()->equals(*int_file_field.type));

    auto bigint_file_field = name_col("value", i64(), 0);
    TableColumnMapper bigint_mapper({.mode = TableColumnMappingMode::BY_NAME});
    ASSERT_TRUE(bigint_mapper.create_mapping(projected_columns, {}, {bigint_file_field}).ok());
    FileScanRequest bigint_request;
    ASSERT_TRUE(
            bigint_mapper
                    .create_scan_request({table_filter}, projected_columns, &bigint_request, &state)
                    .ok());
    ASSERT_EQ(bigint_request.conjuncts.size(), 1);
    const auto& bigint_localized_expr = bigint_request.conjuncts[0]->root();
    ASSERT_EQ(bigint_localized_expr->get_num_children(), 3);
    const auto* localized_slot =
            assert_cast<const VSlotRef*>(bigint_localized_expr->children()[0].get());
    EXPECT_EQ(localized_slot->column_id(), 0);
    EXPECT_TRUE(localized_slot->data_type()->equals(*bigint_file_field.type));
    EXPECT_TRUE(bigint_localized_expr->children()[1]->is_literal());
    EXPECT_TRUE(bigint_localized_expr->children()[1]->data_type()->equals(*bigint_file_field.type));
    EXPECT_TRUE(bigint_localized_expr->children()[2]->is_literal());
    EXPECT_TRUE(bigint_localized_expr->children()[2]->data_type()->equals(*bigint_file_field.type));
}

// Scenario: binary predicate falls back to casting the file slot when literal conversion fails.
TEST_F(ColumnMapperCastTest, ColumnMapperFallsBackToSlotCastWhenLiteralRewriteFails) {
    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    auto table_column = name_col("value", str());
    std::vector<ColumnDefinition> projected_columns {table_column};

    auto file_field = name_col("value", i32(), 0);
    std::vector<ColumnDefinition> file_schema {file_field};

    auto status = mapper.create_mapping(projected_columns, {}, file_schema);
    ASSERT_TRUE(status.ok()) << status;

    auto predicate = std::make_shared<Int64BinaryPredicateExpr>(TExprOpcode::GT);
    predicate->add_child(VSlotRef::create_shared(0, 0, -1, table_column.type, "value"));
    predicate->add_child(
            VLiteral::create_shared(table_column.type, Field::create_field<TYPE_STRING>("bad")));
    TableFilter table_filter;
    table_filter.conjunct = VExprContext::create_shared(predicate);
    table_filter.global_indices = {GlobalIndex(0)};

    FileScanRequest file_request;
    ASSERT_TRUE(mapper.create_scan_request({table_filter}, projected_columns, &file_request, &state)
                        .ok());
    ASSERT_EQ(file_request.conjuncts.size(), 1);
    const auto& localized_expr = file_request.conjuncts[0]->root();
    ASSERT_EQ(localized_expr->get_num_children(), 2);
    const auto& localized_child = localized_expr->children()[0];
    ASSERT_NE(dynamic_cast<const Cast*>(localized_child.get()), nullptr);
    ASSERT_EQ(localized_child->get_num_children(), 1);
    const auto* localized_slot = assert_cast<const VSlotRef*>(localized_child->children()[0].get());
    EXPECT_EQ(localized_slot->column_id(), 0);
    EXPECT_TRUE(localized_slot->data_type()->equals(*file_field.type));
    EXPECT_TRUE(localized_child->data_type()->equals(*table_column.type));
    EXPECT_TRUE(localized_expr->children()[1]->is_literal());
    EXPECT_TRUE(localized_expr->children()[1]->data_type()->equals(*table_column.type));
}

// Scenario: split-local binary literal rewrite does not leak into a later split with a different file type.
TEST_F(ColumnMapperCastTest, ColumnMapperDoesNotLeakRewrittenLiteralAcrossSplits) {
    auto table_column = name_col("value", i64());
    std::vector<ColumnDefinition> projected_columns {table_column};

    auto predicate = std::make_shared<Int64BinaryPredicateExpr>(TExprOpcode::GT);
    predicate->add_child(VSlotRef::create_shared(0, 0, -1, table_column.type, "value"));
    predicate->add_child(
            VLiteral::create_shared(table_column.type, Field::create_field<TYPE_BIGINT>(15)));
    TableFilter table_filter;
    table_filter.conjunct = VExprContext::create_shared(predicate);
    table_filter.global_indices = {GlobalIndex(0)};

    auto int_file_field = name_col("value", i32(), 0);
    TableColumnMapper int_mapper({.mode = TableColumnMappingMode::BY_NAME});
    ASSERT_TRUE(int_mapper.create_mapping(projected_columns, {}, {int_file_field}).ok());
    FileScanRequest int_request;
    ASSERT_TRUE(
            int_mapper.create_scan_request({table_filter}, projected_columns, &int_request, &state)
                    .ok());
    ASSERT_EQ(int_request.conjuncts.size(), 1);
    const auto& int_localized_expr = int_request.conjuncts[0]->root();
    ASSERT_EQ(int_localized_expr->get_num_children(), 2);
    EXPECT_TRUE(int_localized_expr->children()[1]->is_literal());
    EXPECT_TRUE(int_localized_expr->children()[1]->data_type()->equals(*int_file_field.type));

    auto bigint_file_field = name_col("value", i64(), 0);
    TableColumnMapper bigint_mapper({.mode = TableColumnMappingMode::BY_NAME});
    ASSERT_TRUE(bigint_mapper.create_mapping(projected_columns, {}, {bigint_file_field}).ok());
    FileScanRequest bigint_request;
    ASSERT_TRUE(
            bigint_mapper
                    .create_scan_request({table_filter}, projected_columns, &bigint_request, &state)
                    .ok());
    ASSERT_EQ(bigint_request.conjuncts.size(), 1);
    const auto& bigint_localized_expr = bigint_request.conjuncts[0]->root();
    ASSERT_EQ(bigint_localized_expr->get_num_children(), 2);
    const auto* localized_slot =
            assert_cast<const VSlotRef*>(bigint_localized_expr->children()[0].get());
    EXPECT_EQ(localized_slot->column_id(), 0);
    EXPECT_TRUE(localized_slot->data_type()->equals(*bigint_file_field.type));
    EXPECT_TRUE(bigint_localized_expr->children()[1]->is_literal());
    EXPECT_TRUE(bigint_localized_expr->children()[1]->data_type()->equals(*bigint_file_field.type));
}

// Scenario: an explicit user/table cast is preserved while the underlying slot is localized correctly.
TEST_F(ColumnMapperCastTest, ColumnMapperKeepsExplicitSlotCastInSlotLiteralPredicate) {
    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    auto table_column = name_col("value", i64());
    std::vector<ColumnDefinition> projected_columns {table_column};

    auto file_field = name_col("value", i32(), 0);
    std::vector<ColumnDefinition> file_schema {file_field};

    auto status = mapper.create_mapping(projected_columns, {}, file_schema);
    ASSERT_TRUE(status.ok()) << status;

    auto explicit_cast = Cast::create_shared(std::make_shared<DataTypeString>());
    explicit_cast->add_child(VSlotRef::create_shared(0, 0, -1, table_column.type, "value"));
    auto predicate = std::make_shared<Int64BinaryPredicateExpr>(TExprOpcode::GT);
    predicate->add_child(explicit_cast);
    predicate->add_child(
            VLiteral::create_shared(table_column.type, Field::create_field<TYPE_BIGINT>(15)));
    TableFilter table_filter;
    table_filter.conjunct = VExprContext::create_shared(predicate);
    table_filter.global_indices = {GlobalIndex(0)};

    FileScanRequest file_request;
    ASSERT_TRUE(mapper.create_scan_request({table_filter}, projected_columns, &file_request, &state)
                        .ok());
    ASSERT_EQ(file_request.conjuncts.size(), 1);
    const auto& localized_expr = file_request.conjuncts[0]->root();
    ASSERT_EQ(localized_expr->get_num_children(), 2);
    const auto& localized_cast = localized_expr->children()[0];
    ASSERT_NE(dynamic_cast<const Cast*>(localized_cast.get()), nullptr);
    EXPECT_TRUE(localized_cast->data_type()->equals(DataTypeString()));
    ASSERT_EQ(localized_cast->get_num_children(), 1);
    ASSERT_NE(dynamic_cast<const Cast*>(localized_cast->children()[0].get()), nullptr);
    const auto* localized_slot =
            assert_cast<const VSlotRef*>(localized_cast->children()[0]->children()[0].get());
    EXPECT_EQ(localized_slot->column_id(), 0);
    EXPECT_TRUE(localized_slot->data_type()->equals(*file_field.type));
}

// Scenario: repeated scan request creation stays idempotent and does not wrap Cast(Cast(slot)).
TEST_F(ColumnMapperCastTest, ColumnMapperDoesNotNestCastFilterAcrossScanRequests) {
    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    auto table_column = name_col("value", i64());
    std::vector<ColumnDefinition> projected_columns {table_column};

    auto file_field = name_col("value", i32(), 0);
    std::vector<ColumnDefinition> file_schema {file_field};

    auto status = mapper.create_mapping(projected_columns, {}, file_schema);
    ASSERT_TRUE(status.ok()) << status;

    auto predicate = std::make_shared<Int64ChildGreaterThanExpr>(15);
    predicate->add_child(VSlotRef::create_shared(0, 0, -1, table_column.type, "value"));
    TableFilter table_filter;
    table_filter.conjunct = VExprContext::create_shared(predicate);
    table_filter.global_indices = {GlobalIndex(0)};

    FileScanRequest first_request;
    ASSERT_TRUE(
            mapper.create_scan_request({table_filter}, projected_columns, &first_request, &state)
                    .ok());
    FileScanRequest second_request;
    ASSERT_TRUE(
            mapper.create_scan_request({table_filter}, projected_columns, &second_request, &state)
                    .ok());

    ASSERT_EQ(second_request.conjuncts.size(), 1);
    const auto& localized_expr = second_request.conjuncts[0]->root();
    ASSERT_EQ(localized_expr->get_num_children(), 1);
    const auto& localized_child = localized_expr->children()[0];
    ASSERT_NE(dynamic_cast<const Cast*>(localized_child.get()), nullptr);
    ASSERT_EQ(localized_child->get_num_children(), 1);
    const auto* localized_slot = assert_cast<const VSlotRef*>(localized_child->children()[0].get());
    EXPECT_EQ(localized_slot->column_id(), 0);
}

// Scenario: a filter cloned from a previous cast rewrite is adjusted to the next split's matching file type.
TEST_F(ColumnMapperCastTest, ColumnMapperRewritesPreviousCastFilterToMatchingSplitType) {
    auto table_column = name_col("value", i64());
    std::vector<ColumnDefinition> projected_columns {table_column};

    auto predicate = std::make_shared<Int64ChildGreaterThanExpr>(15);
    predicate->add_child(VSlotRef::create_shared(0, 0, -1, table_column.type, "value"));
    TableFilter table_filter;
    table_filter.conjunct = VExprContext::create_shared(predicate);
    table_filter.global_indices = {GlobalIndex(0)};

    auto int_file_field = name_col("value", i32(), 0);

    TableColumnMapper int_mapper({.mode = TableColumnMappingMode::BY_NAME});
    ASSERT_TRUE(int_mapper.create_mapping(projected_columns, {}, {int_file_field}).ok());
    FileScanRequest int_request;
    ASSERT_TRUE(
            int_mapper.create_scan_request({table_filter}, projected_columns, &int_request, &state)
                    .ok());

    const auto& int_localized_expr = int_request.conjuncts[0]->root();
    ASSERT_EQ(int_localized_expr->get_num_children(), 1);
    ASSERT_NE(dynamic_cast<const Cast*>(int_localized_expr->children()[0].get()), nullptr);

    auto bigint_file_field = name_col("value", i64(), 0);

    TableColumnMapper bigint_mapper({.mode = TableColumnMappingMode::BY_NAME});
    ASSERT_TRUE(bigint_mapper.create_mapping(projected_columns, {}, {bigint_file_field}).ok());
    FileScanRequest bigint_request;
    ASSERT_TRUE(
            bigint_mapper
                    .create_scan_request({table_filter}, projected_columns, &bigint_request, &state)
                    .ok());

    const auto& bigint_localized_expr = bigint_request.conjuncts[0]->root();
    ASSERT_EQ(bigint_localized_expr->get_num_children(), 1);
    const auto& bigint_localized_child = bigint_localized_expr->children()[0];
    const auto* localized_slot = assert_cast<const VSlotRef*>(bigint_localized_child.get());
    EXPECT_EQ(localized_slot->column_id(), 0);
    EXPECT_TRUE(localized_slot->data_type()->equals(*bigint_file_field.type));

    Block block;
    block.insert(ColumnHelper::create_column_with_name<DataTypeInt64>({11, 22}));
    auto* conjunct = bigint_request.conjuncts[0].get();
    auto status = conjunct->prepare(&state, RowDescriptor());
    ASSERT_TRUE(status.ok()) << status;
    status = conjunct->open(&state);
    ASSERT_TRUE(status.ok()) << status;
    IColumn::Filter filter(block.rows(), 1);
    bool can_filter_all = false;
    status = conjunct->execute_filter(&block, filter.data(), block.rows(), false, &can_filter_all);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_FALSE(can_filter_all);
    ASSERT_EQ(filter.size(), 2);
    EXPECT_EQ(filter[0], 0);
    EXPECT_EQ(filter[1], 1);
    conjunct->close();
}

// Scenario: localized slot keeps table slot id while column id tracks the file block position.
TEST_F(ColumnMapperCastTest, ColumnMapperKeepsTableSlotIdWhenFileBlockPositionChanges) {
    auto table_column = name_col("value", i64());
    std::vector<ColumnDefinition> projected_columns {table_column};

    auto file_field = name_col("value", i64(), 10);

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    ASSERT_TRUE(mapper.create_mapping(projected_columns, {}, {file_field}).ok());

    auto predicate = std::make_shared<Int64ChildGreaterThanExpr>(15);
    predicate->add_child(VSlotRef::create_shared(7, 0, -1, table_column.type, "value"));
    TableFilter table_filter;
    table_filter.conjunct = VExprContext::create_shared(predicate);
    table_filter.global_indices = {GlobalIndex(0)};

    FileScanRequest first_request;
    ASSERT_TRUE(mapper.localize_filters({table_filter}, &first_request, &state).ok());
    ASSERT_EQ(first_request.conjuncts.size(), 1);
    const auto* first_slot =
            assert_cast<const VSlotRef*>(first_request.conjuncts[0]->root()->children()[0].get());
    EXPECT_EQ(first_slot->slot_id(), 7);
    EXPECT_EQ(first_slot->column_id(), 0);

    FileScanRequest second_request;
    second_request.local_positions.emplace(LocalColumnId(9), LocalIndex(0));
    second_request.local_positions.emplace(LocalColumnId(10), LocalIndex(1));
    second_request.non_predicate_columns.push_back(LocalColumnIndex::top_level(LocalColumnId(9)));
    ASSERT_TRUE(mapper.localize_filters({table_filter}, &second_request, &state).ok());
    ASSERT_EQ(second_request.conjuncts.size(), 1);
    const auto* second_slot =
            assert_cast<const VSlotRef*>(second_request.conjuncts[0]->root()->children()[0].get());
    EXPECT_EQ(second_slot->slot_id(), 7);
    EXPECT_EQ(second_slot->column_id(), 1);

    Block block;
    block.insert(ColumnHelper::create_column_with_name<DataTypeInt64>({100, 100}));
    block.insert(ColumnHelper::create_column_with_name<DataTypeInt64>({11, 22}));
    auto* conjunct = second_request.conjuncts[0].get();
    auto status = conjunct->prepare(&state, RowDescriptor());
    ASSERT_TRUE(status.ok()) << status;
    status = conjunct->open(&state);
    ASSERT_TRUE(status.ok()) << status;
    IColumn::Filter filter(block.rows(), 1);
    bool can_filter_all = false;
    status = conjunct->execute_filter(&block, filter.data(), block.rows(), false, &can_filter_all);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_FALSE(can_filter_all);
    ASSERT_EQ(filter.size(), 2);
    EXPECT_EQ(filter[0], 0);
    EXPECT_EQ(filter[1], 1);
    conjunct->close();
}

} // namespace
} // namespace doris::format
