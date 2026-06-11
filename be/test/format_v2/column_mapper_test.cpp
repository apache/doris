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
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "format_v2/column_mapper_nested.h"
#include "format_v2/expr/cast.h"
#include "format_v2/expr/literal.h"
#include "format_v2/expr/slot_ref.h"
#include "format_v2/schema_projection.h"
#include "format_v2/table_reader.h"
#include "gen_cpp/Exprs_types.h"
#include "storage/predicate/predicate_creator.h"

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

ColumnDefinition map_col(const std::string& name, int32_t field_id, ColumnDefinition entry,
                         const DataTypePtr& key_type, const DataTypePtr& value_type,
                         int32_t local_id = -1) {
    auto column = field_id_col(name, field_id, std::make_shared<DataTypeMap>(key_type, value_type),
                               local_id);
    column.children = {std::move(entry)};
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

std::vector<std::string> target_names(const FileStructPredicateTarget* target) {
    std::vector<std::string> names;
    for (const auto* current = target; current != nullptr; current = current->child.get()) {
        names.push_back(current->file_child_name);
    }
    return names;
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

    Status execute_column_impl(VExprContext*, const Block*, const Selector*, size_t,
                               ColumnPtr&) const override {
        return Status::NotSupported("TestFunctionExpr is only used for ColumnMapper analysis");
    }

private:
    std::string _expr_name;
};

VExprSPtr table_slot(int slot_id, int column_id, DataTypePtr type, const std::string& name) {
    return TableSlotRef::create_shared(slot_id, column_id, -1, std::move(type), name);
}

VExprSPtr literal(DataTypePtr type, Field value) {
    return TableLiteral::create_shared(std::move(type), std::move(value));
}

VExprSPtr struct_element(const VExprSPtr& parent, DataTypePtr child_type,
                         const std::string& child_name) {
    auto expr = std::make_shared<TestFunctionExpr>("struct_element", child_type);
    expr->add_child(parent);
    expr->add_child(literal(str(), Field::create_field<TYPE_STRING>(child_name)));
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

ColumnMapping mapped_struct_column(int32_t root_file_local_id, const std::string& child_name,
                                   int32_t child_file_local_id, DataTypePtr child_type) {
    ColumnDefinition file_child = name_col(child_name, child_type, child_file_local_id);
    ColumnMapping root;
    root.global_index = GlobalIndex(0);
    root.table_column_name = "s";
    root.file_local_id = root_file_local_id;
    root.file_column_name = "s";
    root.table_type =
            std::make_shared<DataTypeStruct>(DataTypes {child_type}, Strings {child_name});
    root.file_type = root.table_type;
    root.original_file_type = root.table_type;
    root.original_file_children = {file_child};
    root.projected_file_children = {file_child};
    return root;
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
    auto entry_type = std::make_shared<DataTypeStruct>(DataTypes {str(), value_type},
                                                       Strings {"key", "value"});
    ColumnDefinition entry = field_id_col("entries", 5, entry_type, 0);
    entry.children = {key, value};
    auto map = map_col("m", 100, entry, str(), value_type, 9);

    LocalColumnIndex projection = LocalColumnIndex::partial_local(9);
    auto entry_projection = LocalColumnIndex::partial_local(0);
    auto value_projection = LocalColumnIndex::partial_local(1);
    value_projection.children.push_back(LocalColumnIndex::local(1));
    entry_projection.children.push_back(std::move(value_projection));
    projection.children.push_back(std::move(entry_projection));

    ColumnDefinition projected;
    ASSERT_TRUE(project_column_definition(map, projection, &projected).ok());
    ASSERT_EQ(projected.children.size(), 1);
    ASSERT_EQ(projected.children[0].children.size(), 1);
    ASSERT_EQ(projected.children[0].children[0].children.size(), 1);
    EXPECT_EQ(projected.children[0].children[0].children[0].name, "b");

    const auto* map_type = assert_cast<const DataTypeMap*>(remove_nullable(projected.type).get());
    const auto* projected_value =
            assert_cast<const DataTypeStruct*>(remove_nullable(map_type->get_value_type()).get());
    ASSERT_EQ(projected_value->get_elements().size(), 1);
    EXPECT_EQ(projected_value->get_element_name(0), "b");
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

TEST(ColumnMapperSchemaProjectionTest, RejectsChildProjectionForPrimitiveType) {
    DataTypePtr projected_type;
    const auto status = rebuild_projected_type(i32(), {i32()}, {"a"}, &projected_type);
    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("Cannot project children from non-complex type"),
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

TEST(ColumnMapperNestedHelperTest, MergesPredicateFiltersForSameNestedTarget) {
    FileColumnPredicateFilter gt_filter;
    gt_filter.target = FileNestedPredicateTarget(
            LocalColumnId(7), std::make_unique<FileStructPredicateTarget>(2, "score"));
    gt_filter.file_column_id = LocalColumnId(7);
    gt_filter.file_child_id_path = {2};
    gt_filter.predicates.push_back(create_comparison_predicate<PredicateType::GT>(
            7, "score", i32(), Field::create_field<TYPE_INT>(10), false));

    FileColumnPredicateFilter lt_filter;
    lt_filter.target = FileNestedPredicateTarget(
            LocalColumnId(7), std::make_unique<FileStructPredicateTarget>(2, "score"));
    lt_filter.file_column_id = LocalColumnId(7);
    lt_filter.file_child_id_path = {2};
    lt_filter.predicates.push_back(create_comparison_predicate<PredicateType::LT>(
            7, "score", i32(), Field::create_field<TYPE_INT>(100), false));

    std::vector<FileColumnPredicateFilter> filters;
    merge_column_predicate_filter(std::move(gt_filter), &filters);
    merge_column_predicate_filter(std::move(lt_filter), &filters);

    ASSERT_EQ(filters.size(), 1);
    EXPECT_EQ(filters[0].effective_file_column_id(), LocalColumnId(7));
    EXPECT_EQ(filters[0].effective_file_child_id_path(), std::vector<int32_t>({2}));
    ASSERT_EQ(filters[0].predicates.size(), 2);
    EXPECT_EQ(target_names(filters[0].target.struct_target.get()),
              std::vector<std::string>({"score"}));
}

TEST(ColumnMapperNestedHelperTest, DoesNotExtractPredicateFiltersFromOr) {
    const auto int_type = i32();
    const auto struct_type = std::make_shared<DataTypeStruct>(DataTypes {int_type}, Strings {"a"});
    const auto slot = table_slot(0, 0, struct_type, "s");
    const auto left = int_gt(struct_element(slot, int_type, "a"), 10);
    const auto right = int_gt(struct_element(slot, int_type, "a"), 20);
    const auto or_expr = compound_predicate(TExprOpcode::COMPOUND_OR, left, right);

    std::vector<FileColumnPredicateFilter> filters;
    collect_nested_column_predicate_filters(or_expr, {mapped_struct_column(5, "a", 0, int_type)},
                                            &filters);

    EXPECT_TRUE(filters.empty());
}

TEST(ColumnMapperNestedHelperTest, DoesNotExtractPredicateFiltersFromUnsupportedExpression) {
    const auto int_type = i32();
    const auto struct_type = std::make_shared<DataTypeStruct>(DataTypes {int_type}, Strings {"a"});
    auto add_expr = std::make_shared<TestFunctionExpr>("add", int_type);
    add_expr->add_child(struct_element(table_slot(0, 0, struct_type, "s"), int_type, "a"));
    add_expr->add_child(literal(int_type, Field::create_field<TYPE_INT>(1)));

    std::vector<FileColumnPredicateFilter> filters;
    collect_nested_column_predicate_filters(add_expr, {mapped_struct_column(5, "a", 0, int_type)},
                                            &filters);

    EXPECT_TRUE(filters.empty());
}

TEST(ColumnMapperNestedHelperTest, DoesNotExtractPredicateFiltersThroughUnsafeCast) {
    const auto file_type = i64();
    const auto table_type = i32();
    const auto struct_type = std::make_shared<DataTypeStruct>(DataTypes {file_type}, Strings {"a"});
    const auto nested_leaf = struct_element(table_slot(0, 0, struct_type, "s"), file_type, "a");
    const auto filter_expr = int_gt(cast_expr(nested_leaf, table_type), 10);

    std::vector<FileColumnPredicateFilter> filters;
    collect_nested_column_predicate_filters(filter_expr,
                                            {mapped_struct_column(5, "a", 0, file_type)}, &filters);

    EXPECT_TRUE(filters.empty());
}

// ----------------------------------------------------------------------
// collect_nested_struct_paths() helper tests.
// These tests assert the entry helper for nested scan projection: it only discovers
// table-side struct paths. Later localization decides whether to build pruning predicates.
// ----------------------------------------------------------------------

TEST(ColumnMapperCollectNestedStructPathsTest, CollectsNameOrdinalAndBooleanSelectors) {
    const auto leaf_type = i32();
    const auto inner_type =
            std::make_shared<DataTypeStruct>(DataTypes {leaf_type, leaf_type}, Strings {"x", "y"});
    const auto root_type =
            std::make_shared<DataTypeStruct>(DataTypes {inner_type, leaf_type},
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
    const auto root_type =
            std::make_shared<DataTypeStruct>(DataTypes {leaf_type}, Strings {"a"});
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
    const auto inner_type =
            std::make_shared<DataTypeStruct>(DataTypes {leaf_type}, Strings {"b"});
    const auto root_type =
            std::make_shared<DataTypeStruct>(DataTypes {inner_type, leaf_type},
                                             Strings {"a", "c"});
    const auto root = table_slot(0, 2, root_type, "s");
    const auto path_a = struct_element_by_selector(root, inner_type,
                                                  literal(str(), Field::create_field<TYPE_STRING>("a")));
    const auto path_ab = struct_element_by_selector(
            path_a, leaf_type, literal(str(), Field::create_field<TYPE_STRING>("b")));
    const auto path_c = struct_element_by_selector(root, leaf_type,
                                                  literal(str(), Field::create_field<TYPE_STRING>("c")));

    auto paths = collect_paths(binary_predicate(TExprOpcode::GT, path_ab,
                                                literal(leaf_type, Field::create_field<TYPE_INT>(1))));
    ASSERT_EQ(paths.size(), 1);
    expect_path_root(paths[0], 2);
    ASSERT_EQ(paths[0].selectors.size(), 2);
    expect_name_selector(paths[0].selectors[0], "a");
    expect_name_selector(paths[0].selectors[1], "b");

    paths = collect_paths(compound_predicate(TExprOpcode::COMPOUND_OR,
                                             binary_predicate(TExprOpcode::GT, path_ab,
                                                              literal(leaf_type,
                                                                      Field::create_field<TYPE_INT>(1))),
                                             binary_predicate(TExprOpcode::LT, path_c,
                                                              literal(leaf_type,
                                                                      Field::create_field<TYPE_INT>(2)))));
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

    const auto root_type =
            std::make_shared<DataTypeStruct>(DataTypes {int_type, float_type, decimal_small},
                                             Strings {"i", "f", "d"});
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

    paths = collect_paths(cast_expr(struct_element(root, make_nullable(int_type), "i"),
                                    make_nullable(int_type)));
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
    ASSERT_TRUE(mapper.create_scan_request({filter}, {}, {table_output}, &request).ok());

    EXPECT_TRUE(request.non_predicate_columns.empty());
    ASSERT_EQ(request.predicate_columns.size(), 1);
    EXPECT_EQ(request.predicate_columns[0].column_id(), LocalColumnId(5));
    ASSERT_FALSE(request.predicate_columns[0].project_all_children);
    EXPECT_EQ(projection_ids(request.predicate_columns[0].children),
              std::vector<int32_t>({0, 1}));
    ASSERT_EQ(request.column_predicate_filters.size(), 1);
    EXPECT_EQ(request.column_predicate_filters[0].effective_file_child_id_path(),
              std::vector<int32_t>({1}));
    ASSERT_EQ(request.column_predicate_filters[0].predicates.size(), 2);
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

    TableColumnMapper mapper(
            {.mode = TableColumnMappingMode::BY_FIELD_ID, .allow_missing_columns = true});
    ASSERT_TRUE(mapper.create_mapping(table_schema, {}, file_schema).ok());

    ASSERT_EQ(mapper.mappings().size(), 3);
    expect_mapping(mapper.mappings()[0], 0, "renamed", 0, "first", int_type, int_type);
    expect_missing(mapper.mappings()[1]);
    expect_mapping(mapper.mappings()[2], 2, "negative", 3, "negative_file", int_type, int_type);
}

TEST(ColumnMapperCreateMappingTest, ByFieldIdRejectsSameNameWithDifferentFieldId) {
    const auto int_type = i32();
    const std::vector<ColumnDefinition> table_schema = {
            field_id_col("same_name", 10, int_type),
    };
    const std::vector<ColumnDefinition> file_schema = {
            field_id_col("same_name", 20, int_type, 0),
    };

    TableColumnMapper mapper(
            {.mode = TableColumnMappingMode::BY_FIELD_ID, .allow_missing_columns = false});
    const auto status = mapper.create_mapping(table_schema, {}, file_schema);
    EXPECT_FALSE(status.ok());
}

TEST(ColumnMapperCreateMappingTest, NestedFieldIdRejectsSameNameWithDifferentFieldId) {
    const auto int_type = i32();
    auto table_child = field_id_col("child", 10, int_type);
    auto table_root = struct_col("root", 1, {table_child});

    auto file_child = field_id_col("child", 20, int_type, 0);
    auto file_root = struct_col("root", 1, {file_child}, 0);

    TableColumnMapper mapper(
            {.mode = TableColumnMappingMode::BY_FIELD_ID, .allow_missing_columns = false});
    const auto status = mapper.create_mapping({table_root}, {}, {file_root});
    EXPECT_FALSE(status.ok());
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

    TableColumnMapper mapper(
            {.mode = TableColumnMappingMode::BY_INDEX, .allow_missing_columns = true});
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

    TableColumnMapper mapper(
            {.mode = TableColumnMappingMode::BY_INDEX, .allow_missing_columns = true});
    ASSERT_TRUE(mapper.create_mapping(table_schema, {}, file_schema).ok());

    ASSERT_EQ(mapper.mappings().size(), 3);
    expect_mapping(mapper.mappings()[0], 0, "a", 0, "_col0", int_type, int_type);
    expect_constant(mapper, mapper.mappings()[1], 1, int_type);
    EXPECT_EQ(mapper.mappings()[1].default_expr, literal_expr);
    expect_missing(mapper.mappings()[2]);
}

TEST(ColumnMapperCreateMappingTest, ByIndexOutOfRangeFailsWhenMissingIsDisallowed) {
    const auto int_type = i32();
    const std::vector<ColumnDefinition> table_schema = {
            position_col("a", 0, int_type),
            position_col("b", 5, int_type),
    };
    const std::vector<ColumnDefinition> file_schema = {
            field_id_col("_col0", 100, int_type, 0),
    };

    TableColumnMapper mapper(
            {.mode = TableColumnMappingMode::BY_INDEX, .allow_missing_columns = false});
    const auto status = mapper.create_mapping(table_schema, {}, file_schema);
    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("file_index=5"), std::string::npos);
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

TEST(ColumnMapperCreateMappingTest, MissingColumnFailsWhenDisallowed) {
    TableColumnMapper mapper(
            {.mode = TableColumnMappingMode::BY_NAME, .allow_missing_columns = false});
    const auto status = mapper.create_mapping({name_col("missing", i32())}, {},
                                              {name_col("present", i32(), 0)});
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("global_index=0"), std::string::npos);
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

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    ASSERT_TRUE(mapper.create_mapping(table_schema, partition_values, {}).ok());

    ASSERT_EQ(mapper.mappings().size(), 5);
    expect_constant(mapper, mapper.mappings()[0], 0, str());
    expect_constant(mapper, mapper.mappings()[1], 1, i32());
    EXPECT_EQ(mapper.mappings()[2].virtual_column_type, TableVirtualColumnType::ROW_ID);
    EXPECT_EQ(mapper.mappings()[3].virtual_column_type,
              TableVirtualColumnType::LAST_UPDATED_SEQUENCE_NUMBER);
    EXPECT_EQ(mapper.mappings()[4].virtual_column_type, TableVirtualColumnType::ICEBERG_ROWID);
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
    ASSERT_TRUE(mapper.create_scan_request({filter}, {}, {partition_column}, &request).ok());

    ASSERT_EQ(mapper.filter_entries().size(), 1);
    ASSERT_TRUE(mapper.filter_entries().at(GlobalIndex(0)).is_constant());
    EXPECT_EQ(mapper.filter_entries().at(GlobalIndex(0)).constant_index(),
              *mapper.mappings()[0].constant_index);
    EXPECT_TRUE(request.local_positions.empty());
    EXPECT_TRUE(request.predicate_columns.empty());
    EXPECT_TRUE(request.non_predicate_columns.empty());
    EXPECT_TRUE(request.conjuncts.empty());
    EXPECT_TRUE(request.column_predicate_filters.empty());
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
    ASSERT_TRUE(mapper.create_scan_request({filter}, {}, {default_column}, &request).ok());

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
    ASSERT_TRUE(
            mapper.create_scan_request({constant_filter, file_filter}, {}, table_schema, &request)
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

    TableFilter filter {.conjunct =
                                VExprContext::create_shared(table_slot(11, 0, int_type, "id")),
                        .global_indices = {GlobalIndex(0)}};

    FileScanRequest request;
    ASSERT_TRUE(mapper.localize_filters({filter}, {}, &request).ok());

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

TEST(ColumnMapperLocalizeFiltersTest, ConstantFilterBuildsEntryWithoutFileScanColumn) {
    auto partition_column = name_col("part", i32());
    partition_column.is_partition_key = true;

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    ASSERT_TRUE(mapper.create_mapping({partition_column},
                                      {{"part", Field::create_field<TYPE_INT>(7)}}, {})
                        .ok());

    TableFilter filter {.conjunct =
                                VExprContext::create_shared(table_slot(3, 0, i32(), "part")),
                        .global_indices = {GlobalIndex(0)}};

    FileScanRequest request;
    ASSERT_TRUE(mapper.localize_filters({filter}, {}, &request).ok());

    EXPECT_TRUE(request.predicate_columns.empty());
    EXPECT_TRUE(request.non_predicate_columns.empty());
    EXPECT_TRUE(request.local_positions.empty());
    EXPECT_TRUE(request.conjuncts.empty());
    ASSERT_EQ(mapper.filter_entries().size(), 1);
    ASSERT_TRUE(mapper.filter_entries().at(GlobalIndex(0)).is_constant());
    EXPECT_EQ(mapper.filter_entries().at(GlobalIndex(0)).constant_index(),
              mapper.mappings()[0].constant_index);
}

TEST(ColumnMapperLocalizeFiltersTest, ColumnPredicatesUseOnlyExistingLocalPositions) {
    const auto int_type = i32();
    const std::vector<ColumnDefinition> table_schema = {name_col("id", int_type)};
    const std::vector<ColumnDefinition> file_schema = {name_col("id", int_type, 3)};

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    ASSERT_TRUE(mapper.create_mapping(table_schema, {}, file_schema).ok());

    TableColumnPredicates predicates;
    predicates[GlobalIndex(0)] = {create_comparison_predicate<PredicateType::GT>(
            0, "id", int_type, Field::create_field<TYPE_INT>(10), false)};

    FileScanRequest request_without_local_position;
    ASSERT_TRUE(mapper.localize_filters({}, predicates, &request_without_local_position).ok());
    EXPECT_TRUE(request_without_local_position.column_predicate_filters.empty());
    ASSERT_EQ(mapper.filter_entries().size(), 1);
    EXPECT_FALSE(mapper.filter_entries().at(GlobalIndex(0)).is_local());

    FileScanRequest request_with_local_position;
    request_with_local_position.non_predicate_columns.push_back(
            LocalColumnIndex::top_level(LocalColumnId(3)));
    request_with_local_position.local_positions.emplace(LocalColumnId(3), LocalIndex(0));
    ASSERT_TRUE(mapper.localize_filters({}, predicates, &request_with_local_position).ok());

    ASSERT_EQ(request_with_local_position.non_predicate_columns.size(), 1);
    EXPECT_EQ(request_with_local_position.non_predicate_columns[0].column_id(), LocalColumnId(3));
    EXPECT_TRUE(request_with_local_position.predicate_columns.empty());
    ASSERT_EQ(request_with_local_position.column_predicate_filters.size(), 1);
    EXPECT_EQ(request_with_local_position.column_predicate_filters[0].effective_file_column_id(),
              LocalColumnId(3));
    ASSERT_EQ(request_with_local_position.column_predicate_filters[0].predicates.size(), 1);
    EXPECT_EQ(request_with_local_position.column_predicate_filters[0].predicates[0]->type(),
              PredicateType::GT);
    ASSERT_TRUE(mapper.filter_entries().at(GlobalIndex(0)).is_local());
    EXPECT_EQ(mapper.filter_entries().at(GlobalIndex(0)).local_index(), LocalIndex(0));
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
    ASSERT_TRUE(mapper.localize_filters({filter}, {}, &request).ok());

    EXPECT_TRUE(request.non_predicate_columns.empty());
    ASSERT_EQ(request.predicate_columns.size(), 1);
    EXPECT_EQ(request.predicate_columns[0].column_id(), LocalColumnId(5));
    ASSERT_FALSE(request.predicate_columns[0].project_all_children);
    EXPECT_EQ(projection_ids(request.predicate_columns[0].children), std::vector<int32_t>({1, 0}));
    ASSERT_EQ(request.local_positions.size(), 1);
    EXPECT_EQ(request.local_positions.at(LocalColumnId(5)), LocalIndex(0));
    ASSERT_TRUE(mapper.filter_entries().at(GlobalIndex(0)).is_local());
    EXPECT_EQ(mapper.filter_entries().at(GlobalIndex(0)).local_index(), LocalIndex(0));
    ASSERT_EQ(request.column_predicate_filters.size(), 1);
    EXPECT_EQ(request.column_predicate_filters[0].effective_file_column_id(), LocalColumnId(5));
    EXPECT_EQ(request.column_predicate_filters[0].effective_file_child_id_path(),
              std::vector<int32_t>({0}));
    EXPECT_EQ(target_names(request.column_predicate_filters[0].target.struct_target.get()),
              std::vector<std::string>({"a"}));
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

    TableFilter filter {.conjunct =
                                VExprContext::create_shared(table_slot(2, 0, int_type, "id")),
                        .global_indices = {GlobalIndex(0)}};

    FileScanRequest request;
    request.non_predicate_columns.push_back(LocalColumnIndex::top_level(LocalColumnId(4)));
    request.local_positions.emplace(LocalColumnId(4), LocalIndex(0));
    ASSERT_TRUE(mapper.localize_filters({filter}, {}, &request).ok());

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

TEST(ColumnMapperScanRequestTest, ColumnPredicatesDoNotForceRowPredicateMaterialization) {
    const auto int_type = i32();
    const auto string_type = str();
    const std::vector<ColumnDefinition> table_schema = {
            name_col("id", int_type),
            name_col("name", string_type),
    };
    const std::vector<ColumnDefinition> file_schema = {
            name_col("id", int_type, 0),
            name_col("name", string_type, 1),
    };

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    ASSERT_TRUE(mapper.create_mapping(table_schema, {}, file_schema).ok());

    TableColumnPredicates predicates;
    predicates[GlobalIndex(0)] = {create_comparison_predicate<PredicateType::GT>(
            0, "id", int_type, Field::create_field<TYPE_INT>(10), false)};

    FileScanRequest request;
    ASSERT_TRUE(mapper.create_scan_request({}, predicates, table_schema, &request).ok());

    EXPECT_TRUE(request.predicate_columns.empty());
    EXPECT_EQ(projection_ids(request.non_predicate_columns), std::vector<int32_t>({0, 1}));
    ASSERT_EQ(request.local_positions.size(), 2);
    EXPECT_EQ(request.local_positions.at(LocalColumnId(0)), LocalIndex(0));
    EXPECT_EQ(request.local_positions.at(LocalColumnId(1)), LocalIndex(1));
    ASSERT_EQ(request.column_predicate_filters.size(), 1);
    EXPECT_EQ(request.column_predicate_filters[0].effective_file_column_id(), LocalColumnId(0));
}

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
    ASSERT_TRUE(mapper.create_scan_request({filter}, {}, table_schema, &request).ok());

    ASSERT_EQ(request.non_predicate_columns.size(), 1);
    EXPECT_EQ(request.non_predicate_columns[0].column_id(), LocalColumnId(0));
    ASSERT_EQ(request.predicate_columns.size(), 1);
    EXPECT_EQ(request.predicate_columns[0].column_id(), LocalColumnId(1));
    ASSERT_TRUE(mapper.filter_entries().at(GlobalIndex(1)).is_local());
    EXPECT_EQ(mapper.filter_entries().at(GlobalIndex(1)).local_index(), LocalIndex(1));
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
    ASSERT_TRUE(mapper.create_scan_request({filter}, {}, {table_struct}, &request).ok());

    EXPECT_TRUE(request.non_predicate_columns.empty());
    ASSERT_EQ(request.predicate_columns.size(), 1);
    EXPECT_EQ(request.predicate_columns[0].column_id(), LocalColumnId(5));
    ASSERT_FALSE(request.predicate_columns[0].project_all_children);
    EXPECT_EQ(projection_ids(request.predicate_columns[0].children), std::vector<int32_t>({1, 0}));
    ASSERT_EQ(request.column_predicate_filters.size(), 1);
    EXPECT_EQ(request.column_predicate_filters[0].effective_file_child_id_path(),
              std::vector<int32_t>({0}));
    EXPECT_EQ(target_names(request.column_predicate_filters[0].target.struct_target.get()),
              std::vector<std::string>({"a"}));
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
    ASSERT_TRUE(mapper.create_scan_request({filter}, {}, {table_struct}, &request).ok());

    ASSERT_EQ(request.column_predicate_filters.size(), 1);
    EXPECT_EQ(request.column_predicate_filters[0].effective_file_column_id(), LocalColumnId(5));
    EXPECT_EQ(request.column_predicate_filters[0].effective_file_child_id_path(),
              std::vector<int32_t>({1}));
    EXPECT_EQ(target_names(request.column_predicate_filters[0].target.struct_target.get()),
              std::vector<std::string>({"b"}));
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
    ASSERT_TRUE(mapper.create_scan_request({filter}, {}, {table_struct}, &request).ok());

    ASSERT_EQ(request.column_predicate_filters.size(), 1);
    EXPECT_EQ(request.column_predicate_filters[0].effective_file_column_id(), LocalColumnId(5));
    EXPECT_EQ(request.column_predicate_filters[0].effective_file_child_id_path(),
              std::vector<int32_t>({0}));
    EXPECT_EQ(target_names(request.column_predicate_filters[0].target.struct_target.get()),
              std::vector<std::string>({"a"}));
    ASSERT_EQ(request.column_predicate_filters[0].predicates.size(), 4);
    EXPECT_EQ(request.column_predicate_filters[0].predicates[0]->type(), PredicateType::IN_LIST);
    EXPECT_EQ(request.column_predicate_filters[0].predicates[1]->type(), PredicateType::GT);
    EXPECT_EQ(request.column_predicate_filters[0].predicates[2]->type(), PredicateType::IS_NULL);
    EXPECT_EQ(request.column_predicate_filters[0].predicates[3]->type(),
              PredicateType::IS_NOT_NULL);
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
    ASSERT_TRUE(mapper.create_scan_request({filter}, {}, {table_struct}, &request).ok());

    ASSERT_EQ(request.column_predicate_filters.size(), 1);
    EXPECT_EQ(request.column_predicate_filters[0].effective_file_column_id(), LocalColumnId(5));
    EXPECT_EQ(request.column_predicate_filters[0].effective_file_child_id_path(),
              std::vector<int32_t>({0}));
    ASSERT_EQ(request.column_predicate_filters[0].predicates.size(), 1);
    EXPECT_EQ(request.column_predicate_filters[0].predicates[0]->type(), PredicateType::GT);
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
    ASSERT_TRUE(mapper.create_scan_request({filter}, {}, {table_struct}, &request).ok());

    EXPECT_TRUE(request.column_predicate_filters.empty());
    ASSERT_EQ(request.predicate_columns.size(), 1);
    EXPECT_EQ(request.predicate_columns[0].column_id(), LocalColumnId(5));
    EXPECT_EQ(projection_ids(request.predicate_columns[0].children), std::vector<int32_t>({1, 0}));
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
    ASSERT_TRUE(mapper.create_scan_request({filter}, {}, {table_struct}, &request).ok());

    ASSERT_EQ(request.column_predicate_filters.size(), 1);
    EXPECT_EQ(request.column_predicate_filters[0].effective_file_column_id(), LocalColumnId(5));
    EXPECT_EQ(request.column_predicate_filters[0].effective_file_child_id_path(),
              std::vector<int32_t>({0, 0}));
    EXPECT_EQ(target_names(request.column_predicate_filters[0].target.struct_target.get()),
              std::vector<std::string>({"a", "id"}));
    ASSERT_EQ(request.column_predicate_filters[0].predicates.size(), 1);
    EXPECT_EQ(request.column_predicate_filters[0].predicates[0]->type(), PredicateType::IN_LIST);
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
    ASSERT_TRUE(mapper.create_scan_request({}, {}, {table_array}, &request).ok());

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
    auto table_entry = struct_name_col("entries", {table_value});
    auto table_map = map_col("m", -1, table_entry, key_type, table_value.type);
    table_map.identifier = Field::create_field<TYPE_STRING>("m");
    set_name_identifiers(&table_map, -1);

    auto file_key = name_col("key", key_type, 0);
    auto file_value_a = name_col("a", int_type, 0);
    auto file_value_b = name_col("b", string_type, 1);
    auto file_value = struct_name_col("value", {file_value_a, file_value_b}, 1);
    auto file_entry = struct_name_col("key_value", {file_key, file_value}, 0);
    auto file_map = map_col("m", -1, file_entry, key_type, file_value.type, 6);
    file_map.identifier = Field::create_field<TYPE_STRING>("m");
    set_name_identifiers(&file_map, 6);

    TableColumnMapper mapper({.mode = TableColumnMappingMode::BY_NAME});
    ASSERT_TRUE(mapper.create_mapping({table_map}, {}, {file_map}).ok());

    FileScanRequest request;
    ASSERT_TRUE(mapper.create_scan_request({}, {}, {table_map}, &request).ok());

    ASSERT_EQ(request.non_predicate_columns.size(), 1);
    const auto& projection = request.non_predicate_columns[0];
    EXPECT_EQ(projection.column_id(), LocalColumnId(6));
    ASSERT_FALSE(projection.project_all_children);
    ASSERT_EQ(projection.children.size(), 1);
    EXPECT_EQ(projection.children[0].local_id(), 0);
    ASSERT_EQ(projection.children[0].children.size(), 1);
    EXPECT_EQ(projection.children[0].children[0].local_id(), 1);
    ASSERT_EQ(projection.children[0].children[0].children.size(), 1);
    EXPECT_EQ(projection.children[0].children[0].children[0].local_id(), 1);

    const auto* mapped_map =
            assert_cast<const DataTypeMap*>(remove_nullable(mapper.mappings()[0].file_type).get());
    const auto* mapped_value =
            assert_cast<const DataTypeStruct*>(remove_nullable(mapped_map->get_value_type()).get());
    ASSERT_EQ(mapped_value->get_elements().size(), 1);
    EXPECT_EQ(mapped_value->get_element_name(0), "b");
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
    ASSERT_TRUE(v1_mapper.create_scan_request({}, {}, {table_struct}, &v1_request).ok());

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
    ASSERT_TRUE(v2_mapper.create_scan_request({}, {}, {table_struct}, &v2_request).ok());

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
    ASSERT_TRUE(mapper.create_scan_request({}, {}, {table_struct}, &request).ok());

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

} // namespace
} // namespace doris::format
