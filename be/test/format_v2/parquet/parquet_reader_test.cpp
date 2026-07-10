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

#include "format_v2/parquet/parquet_reader.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <gtest/gtest.h>
#include <parquet/api/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/page_index.h>

#include <cstring>
#include <filesystem>
#include <map>
#include <memory>
#include <numeric>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/primitive_type.h"
#include "core/field.h"
#include "exprs/vcompound_pred.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "exprs/vslot_ref.h"
#include "format_v2/column_mapper.h"
#include "format_v2/expr/delete_predicate.h"
#include "format_v2/file_reader.h"
#include "format_v2/parquet/parquet_column_schema.h"
#include "format_v2/parquet/parquet_scan.h"
#include "format_v2/parquet/reader/column_reader.h"
#include "format_v2/table_reader.h"
#include "gen_cpp/Types_types.h"
#include "io/fs/file_meta_cache.h"
#include "io/io_common.h"
#include "runtime/runtime_state.h"
#include "storage/index/zone_map/zonemap_eval_context.h"
#include "storage/index/zone_map/zonemap_filter_result.h"
#include "storage/segment/condition_cache.h"
#include "storage/utils.h"
#include "util/defer_op.h"

namespace doris {
namespace {

constexpr int64_t ROW_COUNT = 5;

format::LocalColumnIndex field_projection(int32_t column_id) {
    return format::LocalColumnIndex {.index = column_id};
}

template <typename ColumnType>
const ColumnType& nullable_nested_column(const Block& block, size_t position) {
    const IColumn* column = block.get_by_position(position).column.get();
    int nullable_depth = 0;
    while (const auto* nullable = check_and_get_column<ColumnNullable>(*column)) {
        const auto& null_map = nullable->get_null_map_data();
        for (size_t row = 0; row < null_map.size(); ++row) {
            EXPECT_EQ(null_map[row], 0) << "Unexpected null at row " << row << ", column position "
                                        << position << ", nullable depth " << nullable_depth;
        }
        column = &nullable->get_nested_column();
        ++nullable_depth;
    }
    EXPECT_GT(nullable_depth, 0) << "Expected a nullable file-local column at position "
                                 << position;
    return assert_cast<const ColumnType&>(*column);
}

class Int32GreaterThanExpr final : public VExpr {
public:
    Int32GreaterThanExpr(int column_id, int32_t value)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _value(value) {}

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& input = nullable_nested_column<ColumnInt32>(*block, _column_id);
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            result_data[row] = input.get_element(input_row) > _value;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

    bool can_evaluate_zonemap_filter() const override { return true; }

    void collect_slot_column_ids(std::set<int>& column_ids) const override {
        column_ids.insert(_column_id);
    }

    ZoneMapFilterResult evaluate_zonemap_filter(const ZoneMapEvalContext& ctx) const override {
        auto zone_map = ctx.zone_map(_column_id);
        if (zone_map == nullptr) {
            return unsupported_zonemap_filter(ctx);
        }
        if (!zone_map->has_not_null) {
            return ZoneMapFilterResult::kNoMatch;
        }
        const auto literal = Field::create_field<TYPE_INT>(_value);
        return zone_map->max_value <= literal ? ZoneMapFilterResult::kNoMatch
                                              : ZoneMapFilterResult::kMayMatch;
    }

private:
    const int _column_id;
    const int32_t _value;
    const std::string _expr_name = "Int32GreaterThanExpr";
};

class Int32SumGreaterThanExpr final : public VExpr {
public:
    Int32SumGreaterThanExpr(int left_column_id, int right_column_id, int32_t value)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _left_column_id(left_column_id),
              _right_column_id(right_column_id),
              _value(value) {}

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& left_input = nullable_nested_column<ColumnInt32>(*block, _left_column_id);
        const auto& right_input = nullable_nested_column<ColumnInt32>(*block, _right_column_id);
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            result_data[row] =
                    left_input.get_element(input_row) + right_input.get_element(input_row) > _value;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

    void collect_slot_column_ids(std::set<int>& column_ids) const override {
        column_ids.insert(_left_column_id);
        column_ids.insert(_right_column_id);
    }

private:
    const int _left_column_id;
    const int _right_column_id;
    const int32_t _value;
    const std::string _expr_name = "Int32SumGreaterThanExpr";
};

class NonDeterministicCountingInt32Expr final : public VExpr {
public:
    NonDeterministicCountingInt32Expr(int column_id, std::vector<size_t>* executed_rows)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _executed_rows(executed_rows) {}

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        DORIS_CHECK(_executed_rows != nullptr);
        DORIS_CHECK(block != nullptr);
        (void)nullable_nested_column<ColumnInt32>(*block, _column_id);
        _executed_rows->push_back(count);
        auto result = ColumnUInt8::create();
        result->get_data().resize_fill(count, 1);
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

    bool is_deterministic() const override { return false; }

    void collect_slot_column_ids(std::set<int>& column_ids) const override {
        column_ids.insert(_column_id);
    }

private:
    const int _column_id;
    std::vector<size_t>* const _executed_rows;
    const std::string _expr_name = "NonDeterministicCountingInt32Expr";
};

class SelectedRowsUnsafeCountingInt32Expr final : public VExpr {
public:
    SelectedRowsUnsafeCountingInt32Expr(int column_id, std::vector<size_t>* executed_rows)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _executed_rows(executed_rows) {}

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        DORIS_CHECK(_executed_rows != nullptr);
        DORIS_CHECK(block != nullptr);
        (void)nullable_nested_column<ColumnInt32>(*block, _column_id);
        _executed_rows->push_back(count);
        auto result = ColumnUInt8::create();
        result->get_data().resize_fill(count, 1);
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

    bool is_safe_to_execute_on_selected_rows() const override { return false; }

    void collect_slot_column_ids(std::set<int>& column_ids) const override {
        column_ids.insert(_column_id);
    }

private:
    const int _column_id;
    std::vector<size_t>* const _executed_rows;
    const std::string _expr_name = "SelectedRowsUnsafeCountingInt32Expr";
};

class StringInExpr final : public VExpr {
public:
    StringInExpr(int column_id, std::vector<std::string> values)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _values(std::move(values)) {}

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& input = nullable_nested_column<ColumnString>(*block, _column_id);
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            const auto value = input.get_data_at(input_row).to_string();
            result_data[row] = std::find(_values.begin(), _values.end(), value) != _values.end();
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

    bool can_evaluate_dictionary_filter() const override { return true; }

    ZoneMapFilterResult evaluate_dictionary_filter(
            const DictionaryEvalContext& ctx) const override {
        const auto* dictionary = ctx.slot(_column_id);
        if (dictionary == nullptr) {
            return ZoneMapFilterResult::kUnsupported;
        }
        for (const auto& value : _values) {
            const auto field = Field::create_field<TYPE_STRING>(value);
            for (const auto& dictionary_value : dictionary->values) {
                if (dictionary_value == field) {
                    return ZoneMapFilterResult::kMayMatch;
                }
            }
        }
        return ZoneMapFilterResult::kNoMatch;
    }

    void collect_slot_column_ids(std::set<int>& column_ids) const override {
        column_ids.insert(_column_id);
    }

private:
    const int _column_id;
    const std::vector<std::string> _values;
    const std::string _expr_name = "StringInExpr";
};

class StringEqualsExpr final : public VExpr {
public:
    StringEqualsExpr(int column_id, std::string row_value)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _row_value(std::move(row_value)) {}

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& input = nullable_nested_column<ColumnString>(*block, _column_id);
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            result_data[row] = input.get_data_at(input_row).to_string() == _row_value;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

    void collect_slot_column_ids(std::set<int>& column_ids) const override {
        column_ids.insert(_column_id);
    }

private:
    const int _column_id;
    const std::string _row_value;
    const std::string _expr_name = "StringEqualsExpr";
};

class StringEqualsOrLengthEqualsExpr final : public VExpr {
public:
    StringEqualsOrLengthEqualsExpr(int column_id, std::string row_value, size_t length)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _row_value(std::move(row_value)),
              _length(length) {}

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& input = nullable_nested_column<ColumnString>(*block, _column_id);
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            const auto value = input.get_data_at(input_row);
            result_data[row] = value.to_string() == _row_value || value.size == _length;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

    void collect_slot_column_ids(std::set<int>& column_ids) const override {
        column_ids.insert(_column_id);
    }

private:
    const int _column_id;
    const std::string _row_value;
    const size_t _length;
    const std::string _expr_name = "StringEqualsOrLengthEqualsExpr";
};

VExprContextSPtr create_int32_greater_than_conjunct(int column_id, int32_t value) {
    auto ctx =
            VExprContext::create_shared(std::make_shared<Int32GreaterThanExpr>(column_id, value));
    ctx->_prepared = true;
    ctx->_opened = true;
    return ctx;
}

VExprContextSPtr create_int32_sum_greater_than_conjunct(int left_column_id, int right_column_id,
                                                        int32_t value) {
    auto ctx = VExprContext::create_shared(
            std::make_shared<Int32SumGreaterThanExpr>(left_column_id, right_column_id, value));
    ctx->_prepared = true;
    ctx->_opened = true;
    return ctx;
}

VExprContextSPtr create_non_deterministic_counting_int32_conjunct(
        int column_id, std::vector<size_t>* executed_rows) {
    auto ctx = VExprContext::create_shared(
            std::make_shared<NonDeterministicCountingInt32Expr>(column_id, executed_rows));
    ctx->_prepared = true;
    ctx->_opened = true;
    return ctx;
}

VExprContextSPtr create_selected_rows_unsafe_counting_int32_conjunct(
        int column_id, std::vector<size_t>* executed_rows) {
    auto ctx = VExprContext::create_shared(
            std::make_shared<SelectedRowsUnsafeCountingInt32Expr>(column_id, executed_rows));
    ctx->_prepared = true;
    ctx->_opened = true;
    return ctx;
}

VExprContextSPtr create_string_in_conjunct(int column_id, std::vector<std::string> values) {
    auto ctx = VExprContext::create_shared(
            std::make_shared<StringInExpr>(column_id, std::move(values)));
    ctx->_prepared = true;
    ctx->_opened = true;
    return ctx;
}

TExprNode make_compound_node(TExprOpcode::type opcode, int num_children) {
    TExprNode node;
    node.__set_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
    node.__set_node_type(TExprNodeType::COMPOUND_PRED);
    node.__set_opcode(opcode);
    node.__set_num_children(num_children);
    node.__set_is_nullable(false);
    return node;
}

VExprContextSPtr create_string_dictionary_and_residual_conjunct(
        int column_id, std::vector<std::string> dictionary_values, std::string row_value) {
    auto compound = VCompoundPred::create_shared(make_compound_node(TExprOpcode::COMPOUND_AND, 2));
    compound->add_child(std::make_shared<StringInExpr>(column_id, std::move(dictionary_values)));
    compound->add_child(std::make_shared<StringEqualsExpr>(column_id, std::move(row_value)));
    auto ctx = VExprContext::create_shared(std::move(compound));
    ctx->_prepared = true;
    ctx->_opened = true;
    return ctx;
}

VExprContextSPtr create_nested_or_dictionary_and_residual_conjunct(int column_id) {
    auto root = VCompoundPred::create_shared(make_compound_node(TExprOpcode::COMPOUND_AND, 2));
    root->add_child(
            std::make_shared<StringInExpr>(column_id, std::vector<std::string> {"az", "za"}));
    root->add_child(std::make_shared<StringEqualsOrLengthEqualsExpr>(column_id, "az", 1));

    auto ctx = VExprContext::create_shared(std::move(root));
    ctx->_prepared = true;
    ctx->_opened = true;
    return ctx;
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

std::shared_ptr<arrow::Array> build_timestamp_array(const std::shared_ptr<arrow::DataType>& type,
                                                    const std::vector<int64_t>& values) {
    arrow::TimestampBuilder builder(type, arrow::default_memory_pool());
    for (const auto value : values) {
        EXPECT_TRUE(builder.Append(value).ok());
    }
    return finish_array(&builder);
}

std::shared_ptr<arrow::Array> build_struct_array(const std::vector<int32_t>& ids,
                                                 const std::vector<std::string>& names) {
    auto struct_type = arrow::struct_({arrow::field("id", arrow::int32(), false),
                                       arrow::field("name", arrow::utf8(), false)});
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> field_builders;
    auto id_builder = std::make_unique<arrow::Int32Builder>();
    field_builders.push_back(std::shared_ptr<arrow::ArrayBuilder>(std::move(id_builder)));
    auto name_builder = std::make_unique<arrow::StringBuilder>();
    field_builders.push_back(std::shared_ptr<arrow::ArrayBuilder>(std::move(name_builder)));
    arrow::StructBuilder builder(struct_type, arrow::default_memory_pool(),
                                 std::move(field_builders));
    auto* struct_id_builder = assert_cast<arrow::Int32Builder*>(builder.field_builder(0));
    auto* struct_name_builder = assert_cast<arrow::StringBuilder*>(builder.field_builder(1));
    for (size_t row = 0; row < ids.size(); ++row) {
        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(struct_id_builder->Append(ids[row]).ok());
        EXPECT_TRUE(struct_name_builder->Append(names[row]).ok());
    }
    return finish_array(&builder);
}

void write_parquet_file(const std::string& file_path, int64_t row_group_size = ROW_COUNT) {
    auto schema = arrow::schema({
            arrow::field("id", arrow::int32(), false),
            arrow::field("value", arrow::utf8(), false),
    });
    auto table = arrow::Table::Make(schema,
                                    {build_int32_array({1, 2, 3, 4, 5}),
                                     build_string_array({"one", "two", "three", "four", "five"})});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    builder.compression(::parquet::Compression::UNCOMPRESSED);
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out,
                                                      row_group_size, builder.build()));
}

std::shared_ptr<arrow::Array> build_nullable_int_string_map_array() {
    auto key_builder = std::make_shared<arrow::Int32Builder>();
    auto value_builder = std::make_shared<arrow::StringBuilder>();
    auto map_type = arrow::map(arrow::int32(), arrow::field("value", arrow::utf8(), true));
    arrow::MapBuilder builder(arrow::default_memory_pool(), key_builder, value_builder, map_type);

    EXPECT_TRUE(builder.Append().ok());
    EXPECT_TRUE(key_builder->Append(10).ok());
    EXPECT_TRUE(value_builder->Append("small").ok());

    EXPECT_TRUE(builder.AppendNull().ok());
    EXPECT_TRUE(builder.AppendEmptyValue().ok());

    EXPECT_TRUE(builder.Append().ok());
    EXPECT_TRUE(key_builder->Append(20).ok());
    EXPECT_TRUE(value_builder->Append(std::string(4096, 'x')).ok());

    EXPECT_TRUE(builder.Append().ok());
    EXPECT_TRUE(key_builder->Append(30).ok());
    EXPECT_TRUE(value_builder->AppendNull().ok());
    return finish_array(&builder);
}

std::shared_ptr<arrow::Array> build_nullable_string_list_array() {
    auto value_builder = std::make_shared<arrow::StringBuilder>();
    arrow::ListBuilder builder(arrow::default_memory_pool(), value_builder,
                               arrow::list(arrow::field("element", arrow::utf8(), true)));

    EXPECT_TRUE(builder.Append().ok());
    EXPECT_TRUE(value_builder->Append("small").ok());
    EXPECT_TRUE(value_builder->Append(std::string(4096, 'a')).ok());

    EXPECT_TRUE(builder.AppendNull().ok());
    EXPECT_TRUE(builder.AppendEmptyValue().ok());

    EXPECT_TRUE(builder.Append().ok());
    EXPECT_TRUE(value_builder->AppendNull().ok());

    EXPECT_TRUE(builder.Append().ok());
    EXPECT_TRUE(value_builder->Append(std::string(4096, 'b')).ok());
    return finish_array(&builder);
}

std::shared_ptr<arrow::Array> build_nullable_string_struct_array() {
    auto struct_type = arrow::struct_({arrow::field("payload", arrow::utf8(), true),
                                       arrow::field("id", arrow::int32(), false)});
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> field_builders;
    auto payload_builder = std::make_unique<arrow::StringBuilder>();
    field_builders.push_back(std::shared_ptr<arrow::ArrayBuilder>(std::move(payload_builder)));
    auto id_builder = std::make_unique<arrow::Int32Builder>();
    field_builders.push_back(std::shared_ptr<arrow::ArrayBuilder>(std::move(id_builder)));
    arrow::StructBuilder builder(struct_type, arrow::default_memory_pool(),
                                 std::move(field_builders));
    auto* struct_payload_builder = assert_cast<arrow::StringBuilder*>(builder.field_builder(0));
    auto* struct_id_builder = assert_cast<arrow::Int32Builder*>(builder.field_builder(1));

    EXPECT_TRUE(builder.Append().ok());
    EXPECT_TRUE(struct_payload_builder->Append("small").ok());
    EXPECT_TRUE(struct_id_builder->Append(1).ok());

    EXPECT_TRUE(builder.AppendNull().ok());

    EXPECT_TRUE(builder.Append().ok());
    EXPECT_TRUE(struct_payload_builder->Append(std::string(4096, 'c')).ok());
    EXPECT_TRUE(struct_id_builder->Append(2).ok());

    EXPECT_TRUE(builder.Append().ok());
    EXPECT_TRUE(struct_payload_builder->AppendNull().ok());
    EXPECT_TRUE(struct_id_builder->Append(3).ok());

    EXPECT_TRUE(builder.Append().ok());
    EXPECT_TRUE(struct_payload_builder->Append(std::string(4096, 'd')).ok());
    EXPECT_TRUE(struct_id_builder->Append(4).ok());
    return finish_array(&builder);
}

std::shared_ptr<arrow::Array> build_nullable_struct_with_list_array(bool list_first) {
    auto list_type = arrow::list(arrow::field("element", arrow::int32(), false));
    auto scalar_field = arrow::field("scalar", arrow::int32(), false);
    auto list_field = arrow::field("items", list_type, true);
    auto struct_type = arrow::struct_(list_first ? arrow::FieldVector {list_field, scalar_field}
                                                 : arrow::FieldVector {scalar_field, list_field});

    auto scalar_builder = std::make_shared<arrow::Int32Builder>();
    auto list_value_builder = std::make_shared<arrow::Int32Builder>();
    auto list_builder = std::make_shared<arrow::ListBuilder>(arrow::default_memory_pool(),
                                                             list_value_builder, list_type);
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> field_builders =
            list_first ? std::vector<std::shared_ptr<arrow::ArrayBuilder>> {list_builder,
                                                                            scalar_builder}
                       : std::vector<std::shared_ptr<arrow::ArrayBuilder>> {scalar_builder,
                                                                            list_builder};
    arrow::StructBuilder builder(struct_type, arrow::default_memory_pool(),
                                 std::move(field_builders));

    EXPECT_TRUE(builder.Append().ok());
    EXPECT_TRUE(scalar_builder->Append(1).ok());
    EXPECT_TRUE(list_builder->Append().ok());
    EXPECT_TRUE(list_value_builder->Append(10).ok());
    EXPECT_TRUE(list_value_builder->Append(11).ok());

    EXPECT_TRUE(builder.AppendNull().ok());

    EXPECT_TRUE(builder.Append().ok());
    EXPECT_TRUE(scalar_builder->Append(2).ok());
    EXPECT_TRUE(list_builder->AppendEmptyValue().ok());

    EXPECT_TRUE(builder.Append().ok());
    EXPECT_TRUE(scalar_builder->Append(3).ok());
    EXPECT_TRUE(list_builder->AppendNull().ok());

    EXPECT_TRUE(builder.Append().ok());
    EXPECT_TRUE(scalar_builder->Append(4).ok());
    EXPECT_TRUE(list_builder->Append().ok());
    EXPECT_TRUE(list_value_builder->Append(20).ok());
    return finish_array(&builder);
}

void write_nullable_map_parquet_file(const std::string& file_path) {
    auto array = build_nullable_int_string_map_array();
    auto field = arrow::field("arr", array->type(), true);
    auto table = arrow::Table::Make(arrow::schema({field}), {array});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    builder.compression(::parquet::Compression::UNCOMPRESSED);
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out,
                                                      ROW_COUNT, builder.build()));
}

void write_nullable_string_list_parquet_file(const std::string& file_path) {
    auto array = build_nullable_string_list_array();
    auto field = arrow::field("arr", array->type(), true);
    auto table = arrow::Table::Make(arrow::schema({field}), {array});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    builder.compression(::parquet::Compression::UNCOMPRESSED);
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out,
                                                      ROW_COUNT, builder.build()));
}

void write_nullable_string_struct_parquet_file(const std::string& file_path) {
    auto array = build_nullable_string_struct_array();
    auto field = arrow::field("s", array->type(), true);
    auto table = arrow::Table::Make(arrow::schema({field}), {array});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    builder.compression(::parquet::Compression::UNCOMPRESSED);
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out,
                                                      ROW_COUNT, builder.build()));
}

void write_nullable_struct_with_list_parquet_file(const std::string& file_path) {
    auto scalar_first = build_nullable_struct_with_list_array(false);
    auto list_first = build_nullable_struct_with_list_array(true);
    auto table = arrow::Table::Make(
            arrow::schema({arrow::field("scalar_first", scalar_first->type(), true),
                           arrow::field("list_first", list_first->type(), true)}),
            {scalar_first, list_first});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    builder.compression(::parquet::Compression::UNCOMPRESSED);
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out,
                                                      ROW_COUNT, builder.build()));
}

void write_int96_timestamp_parquet_file(const std::string& file_path) {
    auto field = arrow::field("ts_tz", arrow::timestamp(arrow::TimeUnit::MICRO), true);
    auto array =
            build_timestamp_array(arrow::timestamp(arrow::TimeUnit::MICRO),
                                  {1735660800000000LL, 1735660800123456LL, 1735689600000000LL});
    auto table = arrow::Table::Make(arrow::schema({field}), {array});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

    ::parquet::WriterProperties::Builder writer_builder;
    writer_builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    writer_builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    writer_builder.compression(::parquet::Compression::UNCOMPRESSED);
    ::parquet::ArrowWriterProperties::Builder arrow_builder;
    arrow_builder.enable_force_write_int96_timestamps();
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out,
                                                      ROW_COUNT, writer_builder.build(),
                                                      arrow_builder.build()));
}

void write_int_pair_parquet_file(const std::string& file_path, int64_t row_group_size = ROW_COUNT) {
    auto schema = arrow::schema({
            arrow::field("id", arrow::int32(), false),
            arrow::field("score", arrow::int32(), false),
            arrow::field("value", arrow::utf8(), false),
    });
    auto table = arrow::Table::Make(
            schema, {build_int32_array({1, 2, 3, 4, 5}), build_int32_array({1, 2, 3, 4, 5}),
                     build_string_array({"one", "two", "three", "four", "five"})});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    builder.compression(::parquet::Compression::UNCOMPRESSED);
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out,
                                                      row_group_size, builder.build()));
}

void write_condition_cache_parquet_file(const std::string& file_path) {
    constexpr int64_t row_count = ConditionCacheContext::GRANULE_SIZE * 2;
    std::vector<int32_t> ids(row_count);
    std::iota(ids.begin(), ids.end(), 0);

    auto schema = arrow::schema({arrow::field("id", arrow::int32(), false)});
    auto table = arrow::Table::Make(schema, {build_int32_array(ids)});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    builder.compression(::parquet::Compression::UNCOMPRESSED);
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out,
                                                      row_count, builder.build()));
}

void write_struct_filter_parquet_file(const std::string& file_path) {
    auto id_field = arrow::field("id", arrow::int32(), false);
    auto name_field = arrow::field("name", arrow::utf8(), false);
    auto struct_type = arrow::struct_({id_field, name_field});
    auto schema = arrow::schema({
            arrow::field("s", struct_type, false),
    });
    auto table = arrow::Table::Make(
            schema, {build_struct_array({1, 2, 10, 11}, {"one", "two", "ten", "eleven"})});

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

void write_dictionary_filter_parquet_file(const std::string& file_path) {
    auto schema = arrow::schema({
            arrow::field("id", arrow::int32(), false),
            arrow::field("value", arrow::utf8(), false),
    });
    auto table =
            arrow::Table::Make(schema, {build_int32_array({1, 2, 3, 4, 5, 6}),
                                        build_string_array({"aa", "az", "lm", "lz", "za", "zz"})});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    builder.compression(::parquet::Compression::UNCOMPRESSED);
    builder.enable_dictionary("value");
    builder.disable_dictionary("id");
    builder.disable_statistics();
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out, 1,
                                                      builder.build()));
}

void write_single_row_group_dictionary_filter_parquet_file(const std::string& file_path) {
    auto schema = arrow::schema({
            arrow::field("id", arrow::int32(), false),
            arrow::field("value", arrow::utf8(), false),
    });
    auto table =
            arrow::Table::Make(schema, {build_int32_array({1, 2, 3, 4, 5, 6}),
                                        build_string_array({"aa", "az", "lm", "lz", "za", "zz"})});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    builder.compression(::parquet::Compression::UNCOMPRESSED);
    builder.enable_dictionary("value");
    builder.disable_dictionary("id");
    builder.disable_statistics();
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out, 6,
                                                      builder.build()));
}

void write_dictionary_filter_with_trailing_column_parquet_file(const std::string& file_path) {
    auto schema = arrow::schema({
            arrow::field("id", arrow::int32(), false),
            arrow::field("value", arrow::utf8(), false),
            arrow::field("payload", arrow::int32(), false),
    });
    auto table =
            arrow::Table::Make(schema, {build_int32_array({1, 2, 3, 4, 5, 6}),
                                        build_string_array({"aa", "az", "lm", "lz", "za", "zz"}),
                                        build_int32_array({10, 20, 30, 40, 50, 60})});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    builder.compression(::parquet::Compression::UNCOMPRESSED);
    builder.disable_dictionary("id");
    builder.enable_dictionary("value");
    builder.disable_dictionary("payload");
    builder.disable_statistics();
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out, 6,
                                                      builder.build()));
}

void write_nested_dictionary_filter_parquet_file(const std::string& file_path) {
    auto id_field = arrow::field("id", arrow::int32(), false);
    auto name_field = arrow::field("name", arrow::utf8(), false);
    auto struct_type = arrow::struct_({id_field, name_field});
    auto schema = arrow::schema({
            arrow::field("s", struct_type, false),
    });
    auto table = arrow::Table::Make(
            schema, {build_struct_array({1, 2, 3, 4, 5, 6}, {"aa", "az", "lm", "lz", "za", "zz"})});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    builder.compression(::parquet::Compression::UNCOMPRESSED);
    builder.enable_dictionary("s.name");
    builder.disable_dictionary("s.identifier.field_id");
    builder.disable_statistics();
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out, 1,
                                                      builder.build()));
}

void write_dictionary_edge_parquet_file(const std::string& file_path) {
    auto schema = arrow::schema({
            arrow::field("id", arrow::int32(), false),
            arrow::field("value", arrow::utf8(), false),
    });
    auto table = arrow::Table::Make(
            schema,
            {build_int32_array({1, 2, 3, 4, 5, 6, 7, 8}),
             build_string_array({"", "same", "other", "long-value", "", "tail", "same", "last"})});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    builder.compression(::parquet::Compression::UNCOMPRESSED);
    builder.enable_dictionary("value");
    builder.disable_dictionary("id");
    builder.disable_statistics();
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out, 2,
                                                      builder.build()));
}

void write_nested_page_index_filter_parquet_file(const std::string& file_path) {
    std::vector<int32_t> ids(128);
    std::iota(ids.begin(), ids.end(), 0);
    std::vector<std::string> names;
    names.reserve(ids.size());
    for (const auto id : ids) {
        names.push_back("name-" + std::to_string(id));
    }
    auto id_field = arrow::field("id", arrow::int32(), false);
    auto name_field = arrow::field("name", arrow::utf8(), false);
    auto struct_type = arrow::struct_({id_field, name_field});
    auto schema = arrow::schema({
            arrow::field("s", struct_type, false),
    });
    auto table = arrow::Table::Make(schema, {build_struct_array(ids, names)});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    builder.compression(::parquet::Compression::UNCOMPRESSED);
    builder.disable_dictionary();
    builder.enable_write_page_index();
    builder.write_batch_size(8);
    builder.data_pagesize(10);
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out,
                                                      ids.size(), builder.build()));
}

void write_page_index_filter_parquet_file(const std::string& file_path) {
    std::vector<int32_t> ids(128);
    std::iota(ids.begin(), ids.end(), 0);
    auto schema = arrow::schema({
            arrow::field("id", arrow::int32(), false),
    });
    auto table = arrow::Table::Make(schema, {build_int32_array(ids)});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    builder.compression(::parquet::Compression::UNCOMPRESSED);
    builder.disable_dictionary();
    builder.enable_write_page_index();
    builder.write_batch_size(8);
    builder.data_pagesize(10);
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out,
                                                      ids.size(), builder.build()));
}

void write_page_index_filter_pair_parquet_file(const std::string& file_path) {
    std::vector<int32_t> ids(128);
    std::iota(ids.begin(), ids.end(), 0);
    std::vector<int32_t> payloads;
    payloads.reserve(ids.size());
    for (const auto id : ids) {
        payloads.push_back(id + 1000);
    }
    auto schema = arrow::schema({
            arrow::field("id", arrow::int32(), false),
            arrow::field("payload", arrow::int32(), false),
    });
    auto table = arrow::Table::Make(schema, {build_int32_array(ids), build_int32_array(payloads)});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    builder.compression(::parquet::Compression::UNCOMPRESSED);
    builder.disable_dictionary();
    builder.enable_write_page_index();
    builder.write_batch_size(8);
    builder.data_pagesize(10);
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out,
                                                      ids.size(), builder.build()));
}

Block build_file_block(const std::vector<format::ColumnDefinition>& schema) {
    Block block;
    for (const auto& field : schema) {
        block.insert({field.type->create_column(), field.type, field.name});
    }
    return block;
}

Block build_file_block_with_row_position(const std::vector<format::ColumnDefinition>& schema) {
    auto block = build_file_block(schema);
    const auto row_position_field = format::row_position_column_definition();
    block.insert({row_position_field.type->create_column(), row_position_field.type,
                  row_position_field.name});
    return block;
}

void use_schema_order_positions(format::FileScanRequest* request,
                                const std::vector<format::ColumnDefinition>& schema) {
    DORIS_CHECK(request != nullptr);
    for (size_t idx = 0; idx < schema.size(); ++idx) {
        request->local_positions.emplace(format::LocalColumnId(schema[idx].local_id),
                                         format::LocalIndex(idx));
    }
}

int64_t parquet_column_start_offset(const ::parquet::ColumnChunkMetaData& column_metadata) {
    return column_metadata.has_dictionary_page()
                   ? static_cast<int64_t>(column_metadata.dictionary_page_offset())
                   : static_cast<int64_t>(column_metadata.data_page_offset());
}

std::pair<int64_t, int64_t> row_group_mid_range(const std::string& file_path, int row_group_idx) {
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
    return {row_group_mid_offset, 1};
}

GlobalRowLoacationV2 decode_rowid(const ColumnString& column, size_t row) {
    const auto ref = column.get_data_at(row);
    EXPECT_EQ(ref.size, sizeof(GlobalRowLoacationV2));
    GlobalRowLoacationV2 location(0, 0, 0, 0);
    std::memcpy(&location, ref.data, sizeof(GlobalRowLoacationV2));
    return location;
}

class TestFileReader final : public format::FileReader {
public:
    TestFileReader(std::shared_ptr<io::FileSystemProperties>& system_properties,
                   std::unique_ptr<io::FileDescription>& file_description,
                   std::shared_ptr<io::IOContext> io_ctx)
            : format::FileReader(system_properties, file_description, io_ctx, nullptr) {}

    Status get_schema(std::vector<format::ColumnDefinition>* file_schema) const override {
        file_schema->clear();
        format::ColumnDefinition field;
        field.identifier = Field::create_field<TYPE_INT>(0);
        field.name = "id";
        field.type = std::make_shared<DataTypeInt32>();
        file_schema->push_back(std::move(field));
        return Status::OK();
    }

    bool has_request() const { return _request != nullptr; }

    bool eof() const { return _eof; }

    bool has_io_context() const { return _io_ctx != nullptr; }

    long io_context_use_count() const { return _io_ctx.use_count(); }
};

class TestPersistentFileMetaCache final : public FileMetaPersistentCache {
public:
    Status read(FileMetaCacheFormat format, const std::string& key, int64_t modification_time,
                int64_t file_size, std::string* payload) override {
        const auto it = _entries.find(entry_key(format, key));
        if (it == _entries.end()) {
            return Status::NotFound("test file meta cache entry not found");
        }
        if (it->second.modification_time != modification_time ||
            it->second.file_size != file_size) {
            return Status::NotFound("test file meta cache entry stale");
        }
        *payload = it->second.payload;
        return Status::OK();
    }

    Status write(FileMetaCacheFormat format, const std::string& key, int64_t modification_time,
                 int64_t file_size, std::string_view payload) override {
        _entries[entry_key(format, key)] = Entry {.modification_time = modification_time,
                                                  .file_size = file_size,
                                                  .payload = std::string(payload)};
        return Status::OK();
    }

    void remove(FileMetaCacheFormat format, const std::string& key) override {
        _entries.erase(entry_key(format, key));
        ++_remove_count;
    }

    bool contains(FileMetaCacheFormat format, const std::string& key) const {
        return _entries.contains(entry_key(format, key));
    }

    std::string payload(FileMetaCacheFormat format, const std::string& key) const {
        const auto it = _entries.find(entry_key(format, key));
        return it == _entries.end() ? std::string {} : it->second.payload;
    }

    int remove_count() const { return _remove_count; }

private:
    struct Entry {
        int64_t modification_time = 0;
        int64_t file_size = 0;
        std::string payload;
    };

    static std::string entry_key(FileMetaCacheFormat format, const std::string& key) {
        std::string result;
        result.reserve(key.size() + 4);
        result.append(std::to_string(static_cast<uint8_t>(format)));
        result.push_back(':');
        result.append(key);
        return result;
    }

    std::map<std::string, Entry> _entries;
    int _remove_count = 0;
};

TEST(FileReaderTest, OpenStoresRequestAndCloseKeepsRequest) {
    auto system_properties = std::make_shared<io::FileSystemProperties>();
    system_properties->system_type = TFileType::FILE_LOCAL;
    auto file_description = std::make_unique<io::FileDescription>();
    auto io_ctx = std::make_shared<io::IOContext>();
    TestFileReader reader(system_properties, file_description, io_ctx);

    auto request = std::make_shared<format::FileScanRequest>();
    request->non_predicate_columns.push_back(field_projection(0));
    ASSERT_TRUE(reader.open(request).ok());
    EXPECT_NE(request, nullptr);
    EXPECT_TRUE(reader.has_request());

    ASSERT_TRUE(reader.close().ok());
    EXPECT_TRUE(reader.has_request());
    EXPECT_TRUE(reader.eof());
}

TEST(FileReaderTest, CloseReleasesSharedIOContext) {
    auto system_properties = std::make_shared<io::FileSystemProperties>();
    system_properties->system_type = TFileType::FILE_LOCAL;
    auto file_description = std::make_unique<io::FileDescription>();
    auto io_ctx = std::make_shared<io::IOContext>();
    std::weak_ptr<io::IOContext> weak_io_ctx = io_ctx;
    TestFileReader reader(system_properties, file_description, io_ctx);

    EXPECT_TRUE(reader.has_io_context());
    EXPECT_EQ(reader.io_context_use_count(), 2);
    io_ctx.reset();
    EXPECT_FALSE(weak_io_ctx.expired());
    EXPECT_EQ(reader.io_context_use_count(), 1);

    ASSERT_TRUE(reader.close().ok());
    EXPECT_FALSE(reader.has_io_context());
    EXPECT_TRUE(weak_io_ctx.expired());
}

class NewParquetReaderTest : public testing::Test {
protected:
    void SetUp() override {
        _test_dir = std::filesystem::temp_directory_path() / "doris_format_v2_parquet_reader_test";
        std::filesystem::remove_all(_test_dir);
        std::filesystem::create_directories(_test_dir);
        _file_path = (_test_dir / "reader.parquet").string();
        write_parquet_file(_file_path);
    }

    void TearDown() override { std::filesystem::remove_all(_test_dir); }

    std::unique_ptr<format::parquet::ParquetReader> create_reader(
            int64_t range_start_offset = 0, int64_t range_size = -1,
            RuntimeProfile* profile = nullptr, bool enable_mapping_timestamp_tz = false,
            std::shared_ptr<io::IOContext> io_ctx = nullptr,
            std::optional<format::GlobalRowIdContext> global_rowid_context = std::nullopt,
            bool is_immutable = false, FileMetaCache* file_meta_cache = nullptr,
            bool enable_file_meta_memory_cache = true, int64_t mtime = 123) const {
        auto system_properties = std::make_shared<io::FileSystemProperties>();
        system_properties->system_type = TFileType::FILE_LOCAL;
        auto file_description = std::make_unique<io::FileDescription>();
        file_description->path = _file_path;
        file_description->file_size = static_cast<int64_t>(std::filesystem::file_size(_file_path));
        file_description->mtime = mtime;
        file_description->range_start_offset = range_start_offset;
        file_description->range_size = range_size;
        file_description->is_immutable = is_immutable;
        return std::make_unique<format::parquet::ParquetReader>(
                system_properties, file_description, std::move(io_ctx), profile,
                global_rowid_context, enable_mapping_timestamp_tz, file_meta_cache,
                enable_file_meta_memory_cache);
    }

    std::filesystem::path _test_dir;
    std::string _file_path;
};

TEST_F(NewParquetReaderTest, GetSchemaReturnsFileLocalColumns) {
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);
    EXPECT_EQ(schema[0].local_id, 0);
    EXPECT_EQ(schema[0].name, "id");
    ASSERT_TRUE(schema[0].type->is_nullable());
    EXPECT_EQ(remove_nullable(schema[0].type)->get_primitive_type(), TYPE_INT);
    EXPECT_EQ(schema[1].local_id, 1);
    EXPECT_EQ(schema[1].name, "value");
    ASSERT_TRUE(schema[1].type->is_nullable());
    EXPECT_EQ(remove_nullable(schema[1].type)->get_primitive_type(), TYPE_STRING);
}

TEST_F(NewParquetReaderTest, UsesFileMetaCacheForFooterMetadata) {
    FileMetaCache file_meta_cache(1024 * 1024);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};

    RuntimeProfile first_profile("new_parquet_reader_file_meta_cache_first");
    auto first_reader = create_reader(0, -1, &first_profile, false, nullptr, std::nullopt, false,
                                      &file_meta_cache);
    ASSERT_TRUE(first_reader->init(&state).ok());
    ASSERT_NE(first_profile.get_counter("FileFooterReadCalls"), nullptr);
    EXPECT_EQ(first_profile.get_counter("FileFooterReadCalls")->value(), 1);

    const auto file_size = static_cast<int64_t>(std::filesystem::file_size(_file_path));
    const std::string file_meta_cache_key = FileMetaCache::get_key(_file_path, 123, file_size);
    const std::string memory_cache_key = FileMetaCache::get_memory_cache_key(
            FileMetaCacheFormat::PARQUET_V2, file_meta_cache_key);
    ObjLRUCache::CacheHandle handle;
    EXPECT_TRUE(file_meta_cache.lookup(memory_cache_key, &handle));
    EXPECT_TRUE(handle.valid());

    RuntimeProfile second_profile("new_parquet_reader_file_meta_cache_second");
    auto second_reader =
            create_reader(0, -1, &second_profile, false, nullptr, std::nullopt, false,
                          &file_meta_cache);
    ASSERT_TRUE(second_reader->init(&state).ok());
    ASSERT_NE(second_profile.get_counter("FileFooterReadCalls"), nullptr);
    ASSERT_NE(second_profile.get_counter("FileFooterHitCache"), nullptr);
    ASSERT_NE(second_profile.get_counter("FileFooterHitMemoryCache"), nullptr);
    ASSERT_NE(second_profile.get_counter("FileFooterHitDiskCache"), nullptr);
    EXPECT_EQ(second_profile.get_counter("FileFooterReadCalls")->value(), 0);
    EXPECT_EQ(second_profile.get_counter("FileFooterHitCache")->value(), 1);
    EXPECT_EQ(second_profile.get_counter("FileFooterHitMemoryCache")->value(), 1);
    EXPECT_EQ(second_profile.get_counter("FileFooterHitDiskCache")->value(), 0);
    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(second_reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);
}

TEST_F(NewParquetReaderTest, ReportsPersistentFileMetaCacheProfile) {
    const bool old_enable_external_file_meta_disk_cache =
            config::enable_external_file_meta_disk_cache;
    const int64_t old_external_file_meta_disk_cache_max_entry_bytes =
            config::external_file_meta_disk_cache_max_entry_bytes;
    Defer defer {[&] {
        config::enable_external_file_meta_disk_cache = old_enable_external_file_meta_disk_cache;
        config::external_file_meta_disk_cache_max_entry_bytes =
                old_external_file_meta_disk_cache_max_entry_bytes;
    }};
    config::enable_external_file_meta_disk_cache = true;
    config::external_file_meta_disk_cache_max_entry_bytes = 64 * 1024 * 1024;

    FileMetaCache file_meta_cache(0, std::make_unique<TestPersistentFileMetaCache>());
    RuntimeState state {TQueryOptions(), TQueryGlobals()};

    RuntimeProfile first_profile("new_parquet_reader_persistent_file_meta_cache_first");
    auto first_reader =
            create_reader(0, -1, &first_profile, false, nullptr, std::nullopt, &file_meta_cache);
    ASSERT_TRUE(first_reader->init(&state).ok());
    ASSERT_NE(first_profile.get_counter("FileFooterReadCalls"), nullptr);
    ASSERT_NE(first_profile.get_counter("FileFooterHitCache"), nullptr);
    ASSERT_NE(first_profile.get_counter("FileFooterHitMemoryCache"), nullptr);
    ASSERT_NE(first_profile.get_counter("FileFooterHitDiskCache"), nullptr);
    ASSERT_NE(first_profile.get_counter("FileFooterMissDiskCache"), nullptr);
    ASSERT_NE(first_profile.get_counter("FileFooterWriteDiskCache"), nullptr);
    ASSERT_NE(first_profile.get_counter("FileFooterReadDiskCacheTime"), nullptr);
    ASSERT_NE(first_profile.get_counter("FileFooterWriteDiskCacheTime"), nullptr);
    EXPECT_EQ(first_profile.get_counter("FileFooterReadCalls")->value(), 1);
    EXPECT_EQ(first_profile.get_counter("FileFooterHitCache")->value(), 0);
    EXPECT_EQ(first_profile.get_counter("FileFooterHitMemoryCache")->value(), 0);
    EXPECT_EQ(first_profile.get_counter("FileFooterHitDiskCache")->value(), 0);
    EXPECT_EQ(first_profile.get_counter("FileFooterMissDiskCache")->value(), 1);
    EXPECT_EQ(first_profile.get_counter("FileFooterWriteDiskCache")->value(), 1);

    RuntimeProfile second_profile("new_parquet_reader_persistent_file_meta_cache_second");
    auto second_reader =
            create_reader(0, -1, &second_profile, false, nullptr, std::nullopt, &file_meta_cache);
    ASSERT_TRUE(second_reader->init(&state).ok());
    ASSERT_NE(second_profile.get_counter("FileFooterReadCalls"), nullptr);
    ASSERT_NE(second_profile.get_counter("FileFooterHitCache"), nullptr);
    ASSERT_NE(second_profile.get_counter("FileFooterHitMemoryCache"), nullptr);
    ASSERT_NE(second_profile.get_counter("FileFooterHitDiskCache"), nullptr);
    ASSERT_NE(second_profile.get_counter("FileFooterMissDiskCache"), nullptr);
    ASSERT_NE(second_profile.get_counter("FileFooterWriteDiskCache"), nullptr);
    ASSERT_NE(second_profile.get_counter("FileFooterReadDiskCacheTime"), nullptr);
    ASSERT_NE(second_profile.get_counter("FileFooterWriteDiskCacheTime"), nullptr);
    EXPECT_EQ(second_profile.get_counter("FileFooterReadCalls")->value(), 0);
    EXPECT_EQ(second_profile.get_counter("FileFooterHitCache")->value(), 1);
    EXPECT_EQ(second_profile.get_counter("FileFooterHitMemoryCache")->value(), 0);
    EXPECT_EQ(second_profile.get_counter("FileFooterHitDiskCache")->value(), 1);
    EXPECT_EQ(second_profile.get_counter("FileFooterMissDiskCache")->value(), 0);
    EXPECT_EQ(second_profile.get_counter("FileFooterWriteDiskCache")->value(), 0);
}

TEST_F(NewParquetReaderTest, InvalidPersistentFileMetaCachePayloadFallsBackToFile) {
    const bool old_enable_external_file_meta_disk_cache =
            config::enable_external_file_meta_disk_cache;
    const int64_t old_external_file_meta_disk_cache_max_entry_bytes =
            config::external_file_meta_disk_cache_max_entry_bytes;
    Defer defer {[&] {
        config::enable_external_file_meta_disk_cache = old_enable_external_file_meta_disk_cache;
        config::external_file_meta_disk_cache_max_entry_bytes =
                old_external_file_meta_disk_cache_max_entry_bytes;
    }};
    config::enable_external_file_meta_disk_cache = true;
    config::external_file_meta_disk_cache_max_entry_bytes = 64 * 1024 * 1024;

    auto persistent_cache = std::make_unique<TestPersistentFileMetaCache>();
    TestPersistentFileMetaCache* persistent_cache_ptr = persistent_cache.get();
    FileMetaCache file_meta_cache(0, std::move(persistent_cache));

    const auto file_size = static_cast<int64_t>(std::filesystem::file_size(_file_path));
    const std::string file_meta_cache_key = FileMetaCache::get_key(_file_path, 123, file_size);
    ASSERT_TRUE(persistent_cache_ptr
                        ->write(FileMetaCacheFormat::PARQUET_V2, file_meta_cache_key, 123,
                                file_size, "not a serialized parquet footer")
                        .ok());

    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    RuntimeProfile profile("new_parquet_reader_invalid_persistent_file_meta_cache_payload");
    auto reader = create_reader(0, -1, &profile, false, nullptr, std::nullopt, &file_meta_cache);
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);
    EXPECT_EQ(profile.get_counter("FileFooterReadCalls")->value(), 1);
    EXPECT_EQ(profile.get_counter("FileFooterWriteDiskCache")->value(), 1);
    EXPECT_EQ(persistent_cache_ptr->remove_count(), 1);
    EXPECT_TRUE(
            persistent_cache_ptr->contains(FileMetaCacheFormat::PARQUET_V2, file_meta_cache_key));
    EXPECT_NE(persistent_cache_ptr->payload(FileMetaCacheFormat::PARQUET_V2, file_meta_cache_key),
              "not a serialized parquet footer");
}

TEST_F(NewParquetReaderTest, PersistentFileMetaCacheCanSkipMemoryCache) {
    const bool old_enable_external_file_meta_disk_cache =
            config::enable_external_file_meta_disk_cache;
    const int64_t old_external_file_meta_disk_cache_max_entry_bytes =
            config::external_file_meta_disk_cache_max_entry_bytes;
    Defer defer {[&] {
        config::enable_external_file_meta_disk_cache = old_enable_external_file_meta_disk_cache;
        config::external_file_meta_disk_cache_max_entry_bytes =
                old_external_file_meta_disk_cache_max_entry_bytes;
    }};
    config::enable_external_file_meta_disk_cache = true;
    config::external_file_meta_disk_cache_max_entry_bytes = 64 * 1024 * 1024;

    FileMetaCache file_meta_cache(1024 * 1024, std::make_unique<TestPersistentFileMetaCache>());
    RuntimeState state {TQueryOptions(), TQueryGlobals()};

    RuntimeProfile first_profile("new_parquet_reader_skip_memory_file_meta_cache_first");
    auto first_reader = create_reader(0, -1, &first_profile, false, nullptr, std::nullopt,
                                      &file_meta_cache, false);
    ASSERT_TRUE(first_reader->init(&state).ok());
    const auto file_size = static_cast<int64_t>(std::filesystem::file_size(_file_path));
    const std::string file_meta_cache_key = FileMetaCache::get_key(_file_path, 123, file_size);
    const std::string memory_cache_key = FileMetaCache::get_memory_cache_key(
            FileMetaCacheFormat::PARQUET_V2, file_meta_cache_key);
    ObjLRUCache::CacheHandle handle;
    EXPECT_FALSE(file_meta_cache.lookup(memory_cache_key, &handle));
    ASSERT_NE(first_profile.get_counter("FileFooterReadCalls"), nullptr);
    ASSERT_NE(first_profile.get_counter("FileFooterWriteDiskCache"), nullptr);
    EXPECT_EQ(first_profile.get_counter("FileFooterReadCalls")->value(), 1);
    EXPECT_EQ(first_profile.get_counter("FileFooterWriteDiskCache")->value(), 1);

    RuntimeProfile second_profile("new_parquet_reader_skip_memory_file_meta_cache_second");
    auto second_reader = create_reader(0, -1, &second_profile, false, nullptr, std::nullopt,
                                       &file_meta_cache, false);
    ASSERT_TRUE(second_reader->init(&state).ok());
    ASSERT_NE(second_profile.get_counter("FileFooterReadCalls"), nullptr);
    ASSERT_NE(second_profile.get_counter("FileFooterHitMemoryCache"), nullptr);
    ASSERT_NE(second_profile.get_counter("FileFooterHitDiskCache"), nullptr);
    EXPECT_EQ(second_profile.get_counter("FileFooterReadCalls")->value(), 0);
    EXPECT_EQ(second_profile.get_counter("FileFooterHitMemoryCache")->value(), 0);
    EXPECT_EQ(second_profile.get_counter("FileFooterHitDiskCache")->value(), 1);
    EXPECT_FALSE(file_meta_cache.lookup(memory_cache_key, &handle));
}

// Scenario: Parquet is columnar and supports predicate/non-predicate split, nested projection and
// file-layer pruning hints. The reader declares those scan-request capabilities by choosing
// ParquetColumnMapper itself.
TEST_F(NewParquetReaderTest, CreatesParquetColumnMapper) {
    auto reader = create_reader();
    auto mapper =
            reader->create_column_mapper({.mode = format::TableColumnMappingMode::BY_FIELD_ID});

    ASSERT_NE(dynamic_cast<format::ParquetColumnMapper*>(mapper.get()), nullptr);
}

TEST_F(NewParquetReaderTest, CountComplexColumnUsesShapeOnlyPath) {
    write_nullable_map_parquet_file(_file_path);
    RuntimeProfile profile("count_map_shape_only_path");
    auto reader = create_reader(0, -1, &profile);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());
    ASSERT_TRUE(reader->open(std::make_shared<format::FileScanRequest>()).ok());

    format::FileAggregateRequest request;
    request.agg_type = TPushAggOp::type::COUNT;
    request.columns.push_back(
            {.projection = format::LocalColumnIndex::top_level(format::LocalColumnId(0))});
    format::FileAggregateResult result;
    ASSERT_TRUE(reader->get_aggregate_result(request, &result).ok());

    // Rows are: non-empty map, NULL map, empty map, non-empty map with large value string,
    // non-empty map with NULL value. COUNT(arr) excludes only the top-level NULL map.
    EXPECT_EQ(result.count, 4);
    ASSERT_NE(profile.get_counter("MaterializationTime"), nullptr);
    EXPECT_EQ(profile.get_counter("MaterializationTime")->value(), 0);
}

TEST_F(NewParquetReaderTest, CountArrayColumnUsesLevelsOnlyPath) {
    write_nullable_string_list_parquet_file(_file_path);
    RuntimeProfile profile("count_array_levels_only_path");
    auto reader = create_reader(0, -1, &profile);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());
    ASSERT_TRUE(reader->open(std::make_shared<format::FileScanRequest>()).ok());

    format::FileAggregateRequest request;
    request.agg_type = TPushAggOp::type::COUNT;
    request.columns.push_back(
            {.projection = format::LocalColumnIndex::top_level(format::LocalColumnId(0))});
    format::FileAggregateResult result;
    ASSERT_TRUE(reader->get_aggregate_result(request, &result).ok());

    // Rows are: non-empty array with a large string, NULL array, empty array, non-empty array
    // with NULL element, non-empty array with a large string. Only the top-level NULL is excluded.
    EXPECT_EQ(result.count, 4);
    ASSERT_NE(profile.get_counter("MaterializationTime"), nullptr);
    EXPECT_EQ(profile.get_counter("MaterializationTime")->value(), 0);
}

TEST_F(NewParquetReaderTest, CountStructColumnUsesLevelsOnlyPath) {
    write_nullable_string_struct_parquet_file(_file_path);
    RuntimeProfile profile("count_struct_levels_only_path");
    auto reader = create_reader(0, -1, &profile);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());
    ASSERT_TRUE(reader->open(std::make_shared<format::FileScanRequest>()).ok());

    format::FileAggregateRequest request;
    request.agg_type = TPushAggOp::type::COUNT;
    request.columns.push_back(
            {.projection = format::LocalColumnIndex::top_level(format::LocalColumnId(0))});
    format::FileAggregateResult result;
    ASSERT_TRUE(reader->get_aggregate_result(request, &result).ok());

    // The representative STRUCT leaf is the first child, a nullable STRING payload. A row with
    // NULL payload but non-NULL struct still counts; only the top-level NULL struct is excluded.
    EXPECT_EQ(result.count, 4);
    ASSERT_NE(profile.get_counter("MaterializationTime"), nullptr);
    EXPECT_EQ(profile.get_counter("MaterializationTime")->value(), 0);
}

TEST_F(NewParquetReaderTest, CountStructWithRepeatedChildUsesTopLevelRowBoundaries) {
    write_nullable_struct_with_list_parquet_file(_file_path);

    for (int32_t column_id = 0; column_id < 2; ++column_id) {
        auto reader = create_reader();
        RuntimeState state {TQueryOptions(), TQueryGlobals()};
        ASSERT_TRUE(reader->init(&state).ok());
        ASSERT_TRUE(reader->open(std::make_shared<format::FileScanRequest>()).ok());

        format::FileAggregateRequest request;
        request.agg_type = TPushAggOp::type::COUNT;
        request.columns.push_back({.projection = format::LocalColumnIndex::top_level(
                                           format::LocalColumnId(column_id))});
        format::FileAggregateResult result;
        ASSERT_TRUE(reader->get_aggregate_result(request, &result).ok());

        // Rows are: non-empty ARRAY, NULL STRUCT, empty ARRAY, NULL ARRAY, non-empty ARRAY.
        // COUNT(struct) excludes only the NULL STRUCT regardless of child field order.
        EXPECT_EQ(result.count, 4);
    }
}

TEST_F(NewParquetReaderTest, GetSchemaReturnsNullableNestedChildren) {
    write_struct_filter_parquet_file(_file_path);
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 1);
    EXPECT_EQ(schema[0].name, "s");
    ASSERT_TRUE(schema[0].type->is_nullable());
    ASSERT_EQ(schema[0].children.size(), 2);
    EXPECT_EQ(schema[0].children[0].name, "id");
    ASSERT_TRUE(schema[0].children[0].type->is_nullable());
    EXPECT_EQ(remove_nullable(schema[0].children[0].type)->get_primitive_type(), TYPE_INT);
    EXPECT_EQ(schema[0].children[1].name, "name");
    ASSERT_TRUE(schema[0].children[1].type->is_nullable());
    EXPECT_EQ(remove_nullable(schema[0].children[1].type)->get_primitive_type(), TYPE_STRING);

    const auto* struct_type =
            assert_cast<const DataTypeStruct*>(remove_nullable(schema[0].type).get());
    ASSERT_EQ(struct_type->get_elements().size(), 2);
    EXPECT_TRUE(struct_type->get_element(0)->is_nullable());
    EXPECT_TRUE(struct_type->get_element(1)->is_nullable());
}

TEST_F(NewParquetReaderTest, GetSchemaMapsInt96ToTimestampTzWhenTimestampTzMappingEnabled) {
    write_int96_timestamp_parquet_file(_file_path);
    auto reader = create_reader(0, -1, nullptr, true);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 1);
    EXPECT_EQ(schema[0].name, "ts_tz");
    ASSERT_TRUE(schema[0].type->is_nullable());
    EXPECT_EQ(remove_nullable(schema[0].type)->get_primitive_type(), TYPE_TIMESTAMPTZ);
    EXPECT_EQ(remove_nullable(schema[0].type)->get_scale(), 6);
}

TEST_F(NewParquetReaderTest, ReadSingleRowGroupThenEof) {
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    Block block = build_file_block(schema);

    auto request = std::make_shared<format::FileScanRequest>();
    request->non_predicate_columns = {field_projection(0), field_projection(1)};
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_FALSE(eof);
    ASSERT_EQ(rows, ROW_COUNT);

    const auto& ids = nullable_nested_column<ColumnInt32>(block, 0);
    const auto& values = nullable_nested_column<ColumnString>(block, 1);
    ASSERT_EQ(ids.size(), ROW_COUNT);
    ASSERT_EQ(values.size(), ROW_COUNT);
    EXPECT_EQ(ids.get_element(0), 1);
    EXPECT_EQ(ids.get_element(4), 5);
    EXPECT_EQ(values.get_data_at(0).to_string(), "one");
    EXPECT_EQ(values.get_data_at(4).to_string(), "five");

    rows = 0;
    eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_TRUE(eof);
    EXPECT_EQ(rows, 0);
}

TEST_F(NewParquetReaderTest, RespectsConfiguredBatchSize) {
    auto reader = create_reader();
    reader->set_batch_size(1);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());

    auto request = std::make_shared<format::FileScanRequest>();
    request->non_predicate_columns = {field_projection(0), field_projection(1)};
    ASSERT_TRUE(reader->open(request).ok());

    for (int32_t expected_id = 1; expected_id <= ROW_COUNT; ++expected_id) {
        Block block = build_file_block(schema);
        size_t rows = 0;
        bool eof = false;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        EXPECT_FALSE(eof);
        ASSERT_EQ(rows, 1);
        const auto& ids = nullable_nested_column<ColumnInt32>(block, 0);
        ASSERT_EQ(ids.size(), 1);
        EXPECT_EQ(ids.get_element(0), expected_id);
    }

    Block block = build_file_block(schema);
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_TRUE(eof);
    EXPECT_EQ(rows, 0);
}

TEST_F(NewParquetReaderTest, ConditionCacheMissMarksSurvivingGranules) {
    write_condition_cache_parquet_file(_file_path);
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 1);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(
            create_int32_greater_than_conjunct(0, ConditionCacheContext::GRANULE_SIZE - 1));
    use_schema_order_positions(request.get(), schema);
    ASSERT_TRUE(reader->open(request).ok());

    auto ctx = std::make_shared<ConditionCacheContext>();
    ctx->is_hit = false;
    ctx->filter_result = std::make_shared<std::vector<bool>>(3, false);
    reader->set_condition_cache_context(ctx);

    std::vector<int32_t> ids;
    bool eof = false;
    while (!eof) {
        Block block = build_file_block(schema);
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (rows == 0) {
            continue;
        }
        const auto& id_column = nullable_nested_column<ColumnInt32>(block, 0);
        for (size_t row = 0; row < rows; ++row) {
            ids.push_back(id_column.get_element(row));
        }
    }

    ASSERT_EQ(ids.size(), ConditionCacheContext::GRANULE_SIZE);
    EXPECT_EQ(ids.front(), ConditionCacheContext::GRANULE_SIZE);
    EXPECT_EQ(ids.back(), ConditionCacheContext::GRANULE_SIZE * 2 - 1);
    EXPECT_FALSE((*ctx->filter_result)[0]);
    EXPECT_TRUE((*ctx->filter_result)[1]);
    EXPECT_FALSE((*ctx->filter_result)[2]);
}

TEST_F(NewParquetReaderTest, ConditionCacheHitSkipsFalseGranulesBeforeColumnRead) {
    write_condition_cache_parquet_file(_file_path);
    auto io_ctx = std::make_shared<io::IOContext>();
    auto reader = create_reader(0, -1, nullptr, false, io_ctx);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 1);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(
            create_int32_greater_than_conjunct(0, ConditionCacheContext::GRANULE_SIZE - 1));
    use_schema_order_positions(request.get(), schema);
    ASSERT_TRUE(reader->open(request).ok());

    auto ctx = std::make_shared<ConditionCacheContext>();
    ctx->is_hit = true;
    ctx->filter_result =
            std::make_shared<std::vector<bool>>(std::vector<bool> {false, true, false});
    reader->set_condition_cache_context(ctx);

    Block block = build_file_block(schema);
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_FALSE(eof);
    ASSERT_EQ(rows, ConditionCacheContext::GRANULE_SIZE);
    EXPECT_EQ(io_ctx->condition_cache_filtered_rows, ConditionCacheContext::GRANULE_SIZE);

    const auto& ids = nullable_nested_column<ColumnInt32>(block, 0);
    EXPECT_EQ(ids.get_element(0), ConditionCacheContext::GRANULE_SIZE);
    EXPECT_EQ(ids.get_element(rows - 1), ConditionCacheContext::GRANULE_SIZE * 2 - 1);

    block = build_file_block(schema);
    rows = 0;
    eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_TRUE(eof);
    EXPECT_EQ(rows, 0);
}

TEST_F(NewParquetReaderTest, ReadMultipleRowGroups) {
    write_parquet_file(_file_path, 2);
    auto parquet_file_reader = ::parquet::ParquetFileReader::OpenFile(_file_path, false);
    ASSERT_EQ(parquet_file_reader->metadata()->num_row_groups(), 3);

    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<format::FileScanRequest>();
    request->non_predicate_columns = {field_projection(0), field_projection(1)};
    ASSERT_TRUE(reader->open(request).ok());

    std::vector<int32_t> ids;
    std::vector<std::string> values;
    bool eof = false;
    while (!eof) {
        Block block = build_file_block(schema);
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (rows == 0) {
            continue;
        }
        const auto& id_column = nullable_nested_column<ColumnInt32>(block, 0);
        const auto& value_column = nullable_nested_column<ColumnString>(block, 1);
        for (size_t row = 0; row < rows; ++row) {
            ids.push_back(id_column.get_element(row));
            values.push_back(value_column.get_data_at(row).to_string());
        }
    }

    EXPECT_EQ(ids, std::vector<int32_t>({1, 2, 3, 4, 5}));
    EXPECT_EQ(values, std::vector<std::string>({"one", "two", "three", "four", "five"}));
}

TEST_F(NewParquetReaderTest, UnknownMtimeSkipsPageCacheForMutableFile) {
    _file_path = (_test_dir / "mutable_unknown_mtime.parquet").string();
    write_parquet_file(_file_path);

    RuntimeProfile profile("new_parquet_reader_mutable_unknown_mtime");
    auto reader = create_reader(0, -1, &profile);
    TQueryOptions query_options;
    query_options.__set_enable_parquet_file_page_cache(true);
    RuntimeState state {query_options, TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<format::FileScanRequest>();
    request->non_predicate_columns = {field_projection(0), field_projection(1)};
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    while (!eof) {
        Block block = build_file_block(schema);
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    }

    ASSERT_NE(profile.get_counter("PageReadCount"), nullptr);
    ASSERT_NE(profile.get_counter("PageCacheWriteCount"), nullptr);
    EXPECT_EQ(profile.get_counter("PageReadCount")->value(), 0);
    EXPECT_EQ(profile.get_counter("PageCacheWriteCount")->value(), 0);
}

TEST_F(NewParquetReaderTest, UnknownMtimeUsesPageCacheForImmutableFile) {
    _file_path = (_test_dir / "unknown_mtime_page_cache.parquet").string();
    write_parquet_file(_file_path);

    RuntimeProfile first_profile("new_parquet_reader_first_unknown_mtime");
    {
        auto reader =
                create_reader(0, -1, &first_profile, false, nullptr, std::nullopt, true, nullptr,
                              true, 0);
        TQueryOptions query_options;
        query_options.__set_enable_parquet_file_page_cache(true);
        RuntimeState state {query_options, TQueryGlobals()};
        ASSERT_TRUE(reader->init(&state).ok());

        std::vector<format::ColumnDefinition> schema;
        ASSERT_TRUE(reader->get_schema(&schema).ok());
        auto request = std::make_shared<format::FileScanRequest>();
        request->non_predicate_columns = {field_projection(0), field_projection(1)};
        ASSERT_TRUE(reader->open(request).ok());

        bool eof = false;
        while (!eof) {
            Block block = build_file_block(schema);
            size_t rows = 0;
            ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        }
    }

    ASSERT_NE(first_profile.get_counter("PageReadCount"), nullptr);
    ASSERT_NE(first_profile.get_counter("PageCacheWriteCount"), nullptr);
    EXPECT_GT(first_profile.get_counter("PageReadCount")->value(), 0);
    EXPECT_GT(first_profile.get_counter("PageCacheWriteCount")->value(), 0);

    RuntimeProfile second_profile("new_parquet_reader_second_unknown_mtime");
    auto reader = create_reader(0, -1, &second_profile, false, nullptr, std::nullopt, true,
                                nullptr, true, 0);
    TQueryOptions query_options;
    query_options.__set_enable_parquet_file_page_cache(true);
    RuntimeState state {query_options, TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<format::FileScanRequest>();
    request->non_predicate_columns = {field_projection(0), field_projection(1)};
    ASSERT_TRUE(reader->open(request).ok());

    std::vector<int32_t> ids;
    std::vector<std::string> values;
    bool eof = false;
    while (!eof) {
        Block block = build_file_block(schema);
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (rows == 0) {
            continue;
        }
        const auto& id_column = nullable_nested_column<ColumnInt32>(block, 0);
        const auto& value_column = nullable_nested_column<ColumnString>(block, 1);
        for (size_t row = 0; row < rows; ++row) {
            ids.push_back(id_column.get_element(row));
            values.push_back(value_column.get_data_at(row).to_string());
        }
    }

    EXPECT_EQ(ids, std::vector<int32_t>({1, 2, 3, 4, 5}));
    EXPECT_EQ(values, std::vector<std::string>({"one", "two", "three", "four", "five"}));
    ASSERT_NE(second_profile.get_counter("PageReadCount"), nullptr);
    ASSERT_NE(second_profile.get_counter("PageCacheHitCount"), nullptr);
    EXPECT_GT(second_profile.get_counter("PageReadCount")->value(), 0);
    EXPECT_GT(second_profile.get_counter("PageCacheHitCount")->value(), 0);
}

TEST_F(NewParquetReaderTest, ReadPredicateAndNonPredicateColumnsWithSelection) {
    RuntimeProfile profile("new_parquet_reader_filter_profile");
    auto reader = create_reader(0, -1, &profile);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    Block block = build_file_block(schema);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->non_predicate_columns = {field_projection(1)};
    request->conjuncts.push_back(create_int32_greater_than_conjunct(0, 2));
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_FALSE(eof);
    ASSERT_EQ(rows, 3);

    const auto& ids = nullable_nested_column<ColumnInt32>(block, 0);
    const auto& values = nullable_nested_column<ColumnString>(block, 1);
    ASSERT_EQ(ids.size(), 3);
    ASSERT_EQ(values.size(), 3);
    EXPECT_EQ(ids.get_element(0), 3);
    EXPECT_EQ(ids.get_element(1), 4);
    EXPECT_EQ(ids.get_element(2), 5);
    EXPECT_EQ(values.get_data_at(0).to_string(), "three");
    EXPECT_EQ(values.get_data_at(1).to_string(), "four");
    EXPECT_EQ(values.get_data_at(2).to_string(), "five");

    ASSERT_NE(profile.get_counter("FileReaderCreateTime"), nullptr);
    ASSERT_NE(profile.get_counter("FileNum"), nullptr);
    ASSERT_NE(profile.get_counter("RawRowsRead"), nullptr);
    ASSERT_NE(profile.get_counter("SelectedRows"), nullptr);
    ASSERT_NE(profile.get_counter("RowsFilteredByConjunct"), nullptr);
    ASSERT_NE(profile.get_counter("TotalBatches"), nullptr);
    ASSERT_NE(profile.get_counter("EmptySelectionBatches"), nullptr);
    ASSERT_NE(profile.get_counter("ReaderReadRows"), nullptr);
    ASSERT_NE(profile.get_counter("ReaderSkipRows"), nullptr);
    ASSERT_NE(profile.get_counter("ReaderSelectRows"), nullptr);
    ASSERT_NE(profile.get_counter("ArrowReadRecordsTime"), nullptr);
    ASSERT_NE(profile.get_counter("MaterializationTime"), nullptr);
    ASSERT_GT(profile.get_counter("FileReaderCreateTime")->value(), 0);
    EXPECT_EQ(profile.get_counter("FileNum")->value(), 1);
    EXPECT_EQ(profile.get_counter("RawRowsRead")->value(), ROW_COUNT);
    EXPECT_EQ(profile.get_counter("SelectedRows")->value(), 3);
    EXPECT_EQ(profile.get_counter("RowsFilteredByConjunct")->value(), 2);
    EXPECT_EQ(profile.get_counter("TotalBatches")->value(), 1);
    EXPECT_EQ(profile.get_counter("EmptySelectionBatches")->value(), 0);
    EXPECT_EQ(profile.get_counter("ReaderReadRows")->value(), ROW_COUNT + 3);
    EXPECT_EQ(profile.get_counter("ReaderSkipRows")->value(), 2);
    EXPECT_EQ(profile.get_counter("ReaderSelectRows")->value(), 3);
    EXPECT_GT(profile.get_counter("ArrowReadRecordsTime")->value(), 0);
    EXPECT_GT(profile.get_counter("MaterializationTime")->value(), 0);

    rows = 0;
    eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_TRUE(eof);
    EXPECT_EQ(rows, 0);
}

TEST_F(NewParquetReaderTest, GlobalRowIdSchemaAndSelectionUseFileRowPosition) {
    format::GlobalRowIdContext context {.version = 7, .backend_id = 123456789, .file_id = 42};
    auto reader = create_reader(0, -1, nullptr, false, nullptr, context);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 3);
    EXPECT_EQ(schema[2].local_id, format::GLOBAL_ROWID_COLUMN_ID);
    EXPECT_EQ(schema[2].column_type, format::GLOBAL_ROWID);
    Block block = build_file_block(schema);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->non_predicate_columns = {field_projection(1),
                                      field_projection(format::GLOBAL_ROWID_COLUMN_ID)};
    request->conjuncts.push_back(create_int32_greater_than_conjunct(0, 2));
    use_schema_order_positions(request.get(), schema);
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_FALSE(eof);
    ASSERT_EQ(rows, 3);

    const auto& ids = nullable_nested_column<ColumnInt32>(block, 0);
    const auto& values = nullable_nested_column<ColumnString>(block, 1);
    const auto& rowids = assert_cast<const ColumnString&>(*block.get_by_position(2).column);
    ASSERT_EQ(ids.size(), 3);
    ASSERT_EQ(values.size(), 3);
    ASSERT_EQ(rowids.size(), 3);
    EXPECT_EQ(ids.get_element(0), 3);
    EXPECT_EQ(ids.get_element(1), 4);
    EXPECT_EQ(ids.get_element(2), 5);
    EXPECT_EQ(values.get_data_at(0).to_string(), "three");
    EXPECT_EQ(values.get_data_at(1).to_string(), "four");
    EXPECT_EQ(values.get_data_at(2).to_string(), "five");

    for (size_t row = 0; row < rows; ++row) {
        const auto location = decode_rowid(rowids, row);
        EXPECT_EQ(location.version, context.version);
        EXPECT_EQ(location.backend_id, context.backend_id);
        EXPECT_EQ(location.file_id, context.file_id);
        EXPECT_EQ(location.row_id, static_cast<uint32_t>(row + 2));
    }
}

TEST_F(NewParquetReaderTest, ScanWithoutConjunctDoesNotFilterRowsInsideRowGroup) {
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    Block block = build_file_block(schema);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->non_predicate_columns = {field_projection(1)};
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_FALSE(eof);
    ASSERT_EQ(rows, ROW_COUNT);

    const auto& ids = nullable_nested_column<ColumnInt32>(block, 0);
    const auto& values = nullable_nested_column<ColumnString>(block, 1);
    ASSERT_EQ(ids.size(), ROW_COUNT);
    ASSERT_EQ(values.size(), ROW_COUNT);
    EXPECT_EQ(ids.get_element(0), 1);
    EXPECT_EQ(ids.get_element(4), 5);
    EXPECT_EQ(values.get_data_at(0).to_string(), "one");
    EXPECT_EQ(values.get_data_at(4).to_string(), "five");
}

TEST_F(NewParquetReaderTest, EmptySelectionUpdatesProfileCounters) {
    RuntimeProfile profile("new_parquet_reader_empty_selection_profile");
    auto reader = create_reader(0, -1, &profile);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    Block block = build_file_block(schema);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->non_predicate_columns = {field_projection(1)};
    request->conjuncts.push_back(create_int32_sum_greater_than_conjunct(0, 0, 10));
    use_schema_order_positions(request.get(), schema);
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_TRUE(eof);
    EXPECT_EQ(rows, 0);

    ASSERT_NE(profile.get_counter("RawRowsRead"), nullptr);
    ASSERT_NE(profile.get_counter("SelectedRows"), nullptr);
    ASSERT_NE(profile.get_counter("RowsFilteredByConjunct"), nullptr);
    ASSERT_NE(profile.get_counter("TotalBatches"), nullptr);
    ASSERT_NE(profile.get_counter("EmptySelectionBatches"), nullptr);
    EXPECT_EQ(profile.get_counter("RawRowsRead")->value(), ROW_COUNT);
    EXPECT_EQ(profile.get_counter("SelectedRows")->value(), 0);
    EXPECT_EQ(profile.get_counter("RowsFilteredByConjunct")->value(), ROW_COUNT);
    EXPECT_EQ(profile.get_counter("TotalBatches")->value(), 1);
    EXPECT_EQ(profile.get_counter("EmptySelectionBatches")->value(), 1);
}

TEST_F(NewParquetReaderTest, ReadMultiPredicateColumnsBeforeExpressionFilter) {
    write_int_pair_parquet_file(_file_path);
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    Block block = build_file_block(schema);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0), field_projection(1)};
    request->non_predicate_columns = {};
    request->conjuncts.push_back(create_int32_sum_greater_than_conjunct(0, 1, 7));
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_FALSE(eof);
    ASSERT_EQ(rows, 2);

    const auto& ids = nullable_nested_column<ColumnInt32>(block, 0);
    const auto& scores = nullable_nested_column<ColumnInt32>(block, 1);
    ASSERT_EQ(ids.size(), 2);
    ASSERT_EQ(scores.size(), 2);
    EXPECT_EQ(ids.get_element(0), 4);
    EXPECT_EQ(ids.get_element(1), 5);
    EXPECT_EQ(scores.get_element(0), 4);
    EXPECT_EQ(scores.get_element(1), 5);
}

TEST_F(NewParquetReaderTest, NonDeterministicPredicateKeepsFullBatchEvaluation) {
    write_int_pair_parquet_file(_file_path);
    RuntimeProfile profile("new_parquet_reader_non_deterministic_predicate_profile");
    auto reader = create_reader(0, -1, &profile);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    Block block = build_file_block(schema);

    std::vector<size_t> non_deterministic_executed_rows;
    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0), field_projection(1)};
    request->conjuncts.push_back(create_int32_greater_than_conjunct(0, 2));
    request->conjuncts.push_back(
            create_non_deterministic_counting_int32_conjunct(1, &non_deterministic_executed_rows));
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_FALSE(eof);
    ASSERT_EQ(rows, 3);

    const auto& ids = nullable_nested_column<ColumnInt32>(block, 0);
    const auto& scores = nullable_nested_column<ColumnInt32>(block, 1);
    EXPECT_EQ(ids.get_element(0), 3);
    EXPECT_EQ(ids.get_element(1), 4);
    EXPECT_EQ(ids.get_element(2), 5);
    EXPECT_EQ(scores.get_element(0), 3);
    EXPECT_EQ(scores.get_element(1), 4);
    EXPECT_EQ(scores.get_element(2), 5);

    // A non-deterministic predicate must stay on the old full-batch path. If it were left as a
    // remaining conjunct while earlier deterministic predicates compacted later predicate columns,
    // this expression would only see the three surviving rows instead of the original five.
    EXPECT_EQ(non_deterministic_executed_rows,
              std::vector<size_t>({static_cast<size_t>(ROW_COUNT)}));
    ASSERT_NE(profile.get_counter("ReaderSelectRows"), nullptr);
    EXPECT_EQ(profile.get_counter("ReaderSelectRows")->value(), 0);
}

TEST_F(NewParquetReaderTest, SelectedRowsUnsafePredicateKeepsFullBatchEvaluation) {
    write_int_pair_parquet_file(_file_path);
    RuntimeProfile profile("new_parquet_reader_selected_rows_unsafe_predicate_profile");
    auto reader = create_reader(0, -1, &profile);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    Block block = build_file_block(schema);

    std::vector<size_t> unsafe_executed_rows;
    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0), field_projection(1)};
    request->conjuncts.push_back(create_int32_greater_than_conjunct(0, 2));
    request->conjuncts.push_back(
            create_selected_rows_unsafe_counting_int32_conjunct(1, &unsafe_executed_rows));
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_FALSE(eof);
    ASSERT_EQ(rows, 3);

    const auto& ids = nullable_nested_column<ColumnInt32>(block, 0);
    const auto& scores = nullable_nested_column<ColumnInt32>(block, 1);
    EXPECT_EQ(ids.get_element(0), 3);
    EXPECT_EQ(ids.get_element(1), 4);
    EXPECT_EQ(ids.get_element(2), 5);
    EXPECT_EQ(scores.get_element(0), 3);
    EXPECT_EQ(scores.get_element(1), 4);
    EXPECT_EQ(scores.get_element(2), 5);

    // Error-preserving functions such as assert_true are deterministic, but moving them after an
    // earlier predicate's compacted selection can hide errors from rows filtered by that earlier
    // predicate. Such conjuncts therefore keep the old full-batch execution path.
    EXPECT_EQ(unsafe_executed_rows, std::vector<size_t>({static_cast<size_t>(ROW_COUNT)}));
    ASSERT_NE(profile.get_counter("ReaderSelectRows"), nullptr);
    EXPECT_EQ(profile.get_counter("ReaderSelectRows")->value(), 0);
}

TEST_F(NewParquetReaderTest, PredicateColumnFiltersBeforeNonPredicateRead) {
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    Block block = build_file_block(schema);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->non_predicate_columns = {field_projection(1)};
    request->conjuncts.push_back(create_int32_greater_than_conjunct(0, 2));
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_FALSE(eof);
    ASSERT_EQ(rows, 3);

    const auto& ids = nullable_nested_column<ColumnInt32>(block, 0);
    const auto& values = nullable_nested_column<ColumnString>(block, 1);
    ASSERT_EQ(ids.size(), 3);
    ASSERT_EQ(values.size(), 3);
    EXPECT_EQ(ids.get_element(0), 3);
    EXPECT_EQ(ids.get_element(1), 4);
    EXPECT_EQ(ids.get_element(2), 5);
    EXPECT_EQ(values.get_data_at(0).to_string(), "three");
    EXPECT_EQ(values.get_data_at(1).to_string(), "four");
    EXPECT_EQ(values.get_data_at(2).to_string(), "five");
}

TEST_F(NewParquetReaderTest, NonPredicateColumnKeepsSelectionFromPredicateColumn) {
    write_int_pair_parquet_file(_file_path);
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    Block block = build_file_block(schema);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->non_predicate_columns = {field_projection(1)};
    request->conjuncts.push_back(create_int32_greater_than_conjunct(0, 2));
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_FALSE(eof);
    ASSERT_EQ(rows, 3);

    const auto& ids = nullable_nested_column<ColumnInt32>(block, 0);
    const auto& scores = nullable_nested_column<ColumnInt32>(block, 1);
    ASSERT_EQ(ids.size(), 3);
    ASSERT_EQ(scores.size(), 3);
    EXPECT_EQ(ids.get_element(0), 3);
    EXPECT_EQ(ids.get_element(1), 4);
    EXPECT_EQ(ids.get_element(2), 5);
    EXPECT_EQ(scores.get_element(0), 3);
    EXPECT_EQ(scores.get_element(1), 4);
    EXPECT_EQ(scores.get_element(2), 5);
}

TEST_F(NewParquetReaderTest, PredicateFiltersRowGroupsByStatistics) {
    write_parquet_file(_file_path, 2);
    auto parquet_file_reader = ::parquet::ParquetFileReader::OpenFile(_file_path, false);
    ASSERT_EQ(parquet_file_reader->metadata()->num_row_groups(), 3);

    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->non_predicate_columns = {field_projection(1)};
    request->conjuncts.push_back(create_int32_greater_than_conjunct(0, 2));
    ASSERT_TRUE(reader->open(request).ok());

    std::vector<int32_t> ids;
    std::vector<std::string> values;
    bool eof = false;
    while (!eof) {
        Block block = build_file_block(schema);
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (rows == 0) {
            continue;
        }
        const auto& id_column = nullable_nested_column<ColumnInt32>(block, 0);
        const auto& value_column = nullable_nested_column<ColumnString>(block, 1);
        for (size_t row = 0; row < rows; ++row) {
            ids.push_back(id_column.get_element(row));
            values.push_back(value_column.get_data_at(row).to_string());
        }
    }

    EXPECT_EQ(ids, std::vector<int32_t>({3, 4, 5}));
    EXPECT_EQ(values, std::vector<std::string>({"three", "four", "five"}));
}

TEST_F(NewParquetReaderTest, PredicateFiltersRowGroupsByDictionary) {
    write_dictionary_filter_parquet_file(_file_path);
    auto parquet_file_reader = ::parquet::ParquetFileReader::OpenFile(_file_path, false);
    ASSERT_EQ(parquet_file_reader->metadata()->num_row_groups(), 6);
    for (int row_group_idx = 0; row_group_idx < 6; ++row_group_idx) {
        auto row_group = parquet_file_reader->metadata()->RowGroup(row_group_idx);
        ASSERT_NE(row_group, nullptr);
        auto value_chunk = row_group->ColumnChunk(1);
        ASSERT_NE(value_chunk, nullptr);
        ASSERT_TRUE(value_chunk->has_dictionary_page());
        ASSERT_TRUE(value_chunk->statistics() == nullptr ||
                    !value_chunk->statistics()->HasMinMax());
    }

    std::vector<std::unique_ptr<format::parquet::ParquetColumnSchema>> file_schema;
    auto schema_descriptor = parquet_file_reader->metadata()->schema();
    ASSERT_NE(schema_descriptor, nullptr);
    ASSERT_TRUE(
            format::parquet::build_parquet_column_schema(*schema_descriptor, &file_schema).ok());
    ASSERT_EQ(file_schema.size(), 2);

    format::FileScanRequest plan_request;
    plan_request.local_positions.emplace(format::LocalColumnId(1), format::LocalIndex(1));
    plan_request.conjuncts.push_back(create_string_in_conjunct(1, {"lm"}));

    format::parquet::RowGroupScanPlan plan;
    format::parquet::ParquetScanRange scan_range;
    ASSERT_TRUE(format::parquet::plan_parquet_row_groups(*parquet_file_reader->metadata(),
                                                         parquet_file_reader.get(), file_schema,
                                                         plan_request, scan_range, false, &plan)
                        .ok());
    EXPECT_EQ(plan.pruning_stats.total_row_groups, 6);
    EXPECT_EQ(plan.pruning_stats.selected_row_groups, 1);
    EXPECT_EQ(plan.pruning_stats.filtered_row_groups_by_dictionary, 5);
    EXPECT_EQ(plan.pruning_stats.filtered_group_rows, 5);
    EXPECT_EQ(plan.pruning_stats.selected_row_ranges, 1);

    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(1)};
    request->non_predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(create_string_in_conjunct(1, {"lm"}));
    use_schema_order_positions(request.get(), schema);
    ASSERT_TRUE(reader->open(request).ok());

    std::vector<int32_t> ids;
    std::vector<std::string> values;
    bool eof = false;
    while (!eof) {
        Block block = build_file_block(schema);
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (rows == 0) {
            continue;
        }
        const auto& id_column = nullable_nested_column<ColumnInt32>(block, 0);
        const auto& value_column = nullable_nested_column<ColumnString>(block, 1);
        for (size_t row = 0; row < rows; ++row) {
            ids.push_back(id_column.get_element(row));
            values.push_back(value_column.get_data_at(row).to_string());
        }
    }

    EXPECT_EQ(ids, std::vector<int32_t>({3}));
    EXPECT_EQ(values, std::vector<std::string>({"lm"}));
}

TEST_F(NewParquetReaderTest, DictionaryPredicateFiltersRowsInsideRowGroup) {
    write_single_row_group_dictionary_filter_parquet_file(_file_path);
    auto parquet_file_reader = ::parquet::ParquetFileReader::OpenFile(_file_path, false);
    ASSERT_EQ(parquet_file_reader->metadata()->num_row_groups(), 1);
    auto row_group = parquet_file_reader->metadata()->RowGroup(0);
    ASSERT_NE(row_group, nullptr);
    ASSERT_TRUE(row_group->ColumnChunk(1)->has_dictionary_page());

    RuntimeProfile profile("new_parquet_reader_dictionary_filter_profile");
    auto reader = create_reader(0, -1, &profile);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(1)};
    request->non_predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(create_string_in_conjunct(1, {"az", "za"}));
    use_schema_order_positions(request.get(), schema);
    ASSERT_TRUE(reader->open(request).ok());

    std::vector<int32_t> ids;
    std::vector<std::string> values;
    bool eof = false;
    while (!eof) {
        Block block = build_file_block(schema);
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (rows == 0) {
            continue;
        }
        const auto& id_column = nullable_nested_column<ColumnInt32>(block, 0);
        const auto& value_column = nullable_nested_column<ColumnString>(block, 1);
        for (size_t row = 0; row < rows; ++row) {
            ids.push_back(id_column.get_element(row));
            values.push_back(value_column.get_data_at(row).to_string());
        }
    }

    EXPECT_EQ(ids, std::vector<int32_t>({2, 5}));
    EXPECT_EQ(values, std::vector<std::string>({"az", "za"}));
    EXPECT_EQ(profile.get_counter("RowsFilteredByConjunct")->value(), 4);
    EXPECT_EQ(profile.get_counter("RowsFilteredByDictFilter")->value(), 4);
    EXPECT_EQ(profile.get_counter("DictFilterCandidateColumns")->value(), 1);
    EXPECT_EQ(profile.get_counter("DictFilterColumns")->value(), 1);
    EXPECT_EQ(profile.get_counter("DictFilterUnsupportedColumns")->value(), 0);
    EXPECT_EQ(profile.get_counter("DictFilterReadFailures")->value(), 0);
    ASSERT_NE(profile.get_counter("DictFilterExprRewriteTime"), nullptr);
    ASSERT_NE(profile.get_counter("DictFilterReadDictTime"), nullptr);
    ASSERT_NE(profile.get_counter("DictFilterBuildTime"), nullptr);
    EXPECT_EQ(profile.get_counter("SelectedRows")->value(), 2);
    EXPECT_GE(profile.get_counter("ReaderSelectRows")->value(), 8);
}

TEST_F(NewParquetReaderTest, DictionaryPredicateProbeDoesNotUseMergeRangeReader) {
    write_dictionary_filter_with_trailing_column_parquet_file(_file_path);

    RuntimeProfile profile("new_parquet_reader_dictionary_filter_merge_profile");
    auto reader = create_reader(0, -1, &profile);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(1)};
    request->non_predicate_columns = {field_projection(0), field_projection(2)};
    request->conjuncts.push_back(create_string_in_conjunct(1, {"az", "za"}));
    use_schema_order_positions(request.get(), schema);
    ASSERT_TRUE(reader->open(request).ok());

    std::vector<int32_t> ids;
    std::vector<std::string> values;
    std::vector<int32_t> payloads;
    bool eof = false;
    while (!eof) {
        Block block = build_file_block(schema);
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (rows == 0) {
            continue;
        }
        const auto& id_column = nullable_nested_column<ColumnInt32>(block, 0);
        const auto& value_column = nullable_nested_column<ColumnString>(block, 1);
        const auto& payload_column = nullable_nested_column<ColumnInt32>(block, 2);
        for (size_t row = 0; row < rows; ++row) {
            ids.push_back(id_column.get_element(row));
            values.push_back(value_column.get_data_at(row).to_string());
            payloads.push_back(payload_column.get_element(row));
        }
    }

    EXPECT_EQ(ids, std::vector<int32_t>({2, 5}));
    EXPECT_EQ(values, std::vector<std::string>({"az", "za"}));
    EXPECT_EQ(payloads, std::vector<int32_t>({20, 50}));
    EXPECT_EQ(profile.get_counter("RowsFilteredByDictFilter")->value(), 4);
    ASSERT_NE(profile.get_counter("MergedIO"), nullptr);
    ASSERT_NE(profile.get_counter("MergedBytes"), nullptr);
}

TEST_F(NewParquetReaderTest, DictionaryPredicateWorksWithoutRuntimeProfile) {
    write_single_row_group_dictionary_filter_parquet_file(_file_path);

    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(1)};
    request->non_predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(create_string_in_conjunct(1, {"az", "za"}));
    use_schema_order_positions(request.get(), schema);
    ASSERT_TRUE(reader->open(request).ok());

    std::vector<int32_t> ids;
    std::vector<std::string> values;
    bool eof = false;
    while (!eof) {
        Block block = build_file_block(schema);
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (rows == 0) {
            continue;
        }
        const auto& id_column = nullable_nested_column<ColumnInt32>(block, 0);
        const auto& value_column = nullable_nested_column<ColumnString>(block, 1);
        for (size_t row = 0; row < rows; ++row) {
            ids.push_back(id_column.get_element(row));
            values.push_back(value_column.get_data_at(row).to_string());
        }
    }

    EXPECT_EQ(ids, std::vector<int32_t>({2, 5}));
    EXPECT_EQ(values, std::vector<std::string>({"az", "za"}));
}

TEST_F(NewParquetReaderTest, DictionaryPredicateSkipsRemainingPredicateColumnsWhenEmpty) {
    write_single_row_group_dictionary_filter_parquet_file(_file_path);

    RuntimeProfile profile("new_parquet_reader_dictionary_filter_empty_profile");
    auto reader = create_reader(0, -1, &profile);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(1), field_projection(0)};
    request->conjuncts.push_back(
            create_string_dictionary_and_residual_conjunct(1, {"az"}, "not_present"));
    request->conjuncts.push_back(create_int32_greater_than_conjunct(0, 0));
    use_schema_order_positions(request.get(), schema);
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    size_t total_rows = 0;
    while (!eof) {
        Block block = build_file_block(schema);
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        total_rows += rows;
    }

    EXPECT_EQ(total_rows, 0);
    EXPECT_EQ(profile.get_counter("RowsFilteredByConjunct")->value(), 6);
    EXPECT_EQ(profile.get_counter("RowsFilteredByDictFilter")->value(), 5);
    EXPECT_EQ(profile.get_counter("DictFilterCandidateColumns")->value(), 1);
    EXPECT_EQ(profile.get_counter("DictFilterColumns")->value(), 1);
    EXPECT_EQ(profile.get_counter("DictFilterUnsupportedColumns")->value(), 0);
    EXPECT_EQ(profile.get_counter("DictFilterReadFailures")->value(), 0);
    EXPECT_EQ(profile.get_counter("SelectedRows")->value(), 0);
    // The first dictionary predicate column is read once to produce a compact row filter. The
    // second predicate column is skipped after the selection becomes empty, which verifies the
    // StarRocks-style round-by-round policy: only rows surviving previous predicates are read.
    EXPECT_EQ(profile.get_counter("ReaderSelectRows")->value(), 6);
    EXPECT_EQ(profile.get_counter("ReaderSkipRows")->value(), 6);
}

TEST_F(NewParquetReaderTest, DictionaryPredicateRunsResidualConjunctOnSurvivors) {
    write_single_row_group_dictionary_filter_parquet_file(_file_path);

    RuntimeProfile profile("new_parquet_reader_dictionary_prefilter_residual_profile");
    auto reader = create_reader(0, -1, &profile);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(1)};
    request->non_predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(
            create_string_dictionary_and_residual_conjunct(1, {"az", "za"}, "za"));
    use_schema_order_positions(request.get(), schema);
    ASSERT_TRUE(reader->open(request).ok());

    std::vector<int32_t> ids;
    std::vector<std::string> values;
    bool eof = false;
    while (!eof) {
        Block block = build_file_block(schema);
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (rows == 0) {
            continue;
        }
        const auto& id_column = nullable_nested_column<ColumnInt32>(block, 0);
        const auto& value_column = nullable_nested_column<ColumnString>(block, 1);
        for (size_t row = 0; row < rows; ++row) {
            ids.push_back(id_column.get_element(row));
            values.push_back(value_column.get_data_at(row).to_string());
        }
    }

    EXPECT_EQ(ids, std::vector<int32_t>({5}));
    EXPECT_EQ(values, std::vector<std::string>({"za"}));
    EXPECT_EQ(profile.get_counter("RowsFilteredByDictFilter")->value(), 4);
    EXPECT_EQ(profile.get_counter("RowsFilteredByConjunct")->value(), 5);
    EXPECT_EQ(profile.get_counter("SelectedRows")->value(), 1);
}

TEST_F(NewParquetReaderTest, DictionaryPredicateKeepsNestedOrResidualConjunct) {
    write_single_row_group_dictionary_filter_parquet_file(_file_path);

    RuntimeProfile profile("new_parquet_reader_dictionary_nested_or_residual_profile");
    auto reader = create_reader(0, -1, &profile);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(1)};
    request->non_predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(create_nested_or_dictionary_and_residual_conjunct(1));
    use_schema_order_positions(request.get(), schema);
    ASSERT_TRUE(reader->open(request).ok());

    std::vector<int32_t> ids;
    std::vector<std::string> values;
    bool eof = false;
    while (!eof) {
        Block block = build_file_block(schema);
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (rows == 0) {
            continue;
        }
        const auto& id_column = nullable_nested_column<ColumnInt32>(block, 0);
        const auto& value_column = nullable_nested_column<ColumnString>(block, 1);
        for (size_t row = 0; row < rows; ++row) {
            ids.push_back(id_column.get_element(row));
            values.push_back(value_column.get_data_at(row).to_string());
        }
    }

    EXPECT_EQ(ids, std::vector<int32_t>({2}));
    EXPECT_EQ(values, std::vector<std::string>({"az"}));
    EXPECT_EQ(profile.get_counter("RowsFilteredByDictFilter")->value(), 4);
    EXPECT_EQ(profile.get_counter("RowsFilteredByConjunct")->value(), 5);
    EXPECT_EQ(profile.get_counter("SelectedRows")->value(), 1);
}

TEST_F(NewParquetReaderTest, ScanRangeFiltersRowGroupsBeforeDictionaryPruning) {
    write_dictionary_filter_parquet_file(_file_path);
    auto parquet_file_reader = ::parquet::ParquetFileReader::OpenFile(_file_path, false);
    ASSERT_EQ(parquet_file_reader->metadata()->num_row_groups(), 6);

    std::vector<std::unique_ptr<format::parquet::ParquetColumnSchema>> file_schema;
    auto schema_descriptor = parquet_file_reader->metadata()->schema();
    ASSERT_NE(schema_descriptor, nullptr);
    ASSERT_TRUE(
            format::parquet::build_parquet_column_schema(*schema_descriptor, &file_schema).ok());

    format::FileScanRequest request;
    request.local_positions.emplace(format::LocalColumnId(1), format::LocalIndex(1));
    request.conjuncts.push_back(create_string_in_conjunct(1, {"lm"}));

    const auto [range_start_offset, range_size] = row_group_mid_range(_file_path, 2);
    format::parquet::ParquetScanRange scan_range;
    scan_range.start_offset = range_start_offset;
    scan_range.size = range_size;
    scan_range.file_size = static_cast<int64_t>(std::filesystem::file_size(_file_path));

    format::parquet::RowGroupScanPlan plan;
    ASSERT_TRUE(format::parquet::plan_parquet_row_groups(*parquet_file_reader->metadata(),
                                                         parquet_file_reader.get(), file_schema,
                                                         request, scan_range, false, &plan)
                        .ok());
    ASSERT_EQ(plan.row_groups.size(), 1);
    EXPECT_EQ(plan.row_groups[0].row_group_id, 2);
    EXPECT_EQ(plan.pruning_stats.total_row_groups, 6);
    EXPECT_EQ(plan.pruning_stats.selected_row_groups, 1);
    EXPECT_EQ(plan.pruning_stats.filtered_row_groups_by_dictionary, 0);
    EXPECT_EQ(plan.pruning_stats.filtered_group_rows, 0);
}

TEST_F(NewParquetReaderTest, NestedStructPredicateDoesNotFilterRowGroupsByStatistics) {
    write_struct_filter_parquet_file(_file_path);
    auto parquet_file_reader = ::parquet::ParquetFileReader::OpenFile(_file_path, false);
    ASSERT_EQ(parquet_file_reader->metadata()->num_row_groups(), 2);

    std::vector<std::unique_ptr<format::parquet::ParquetColumnSchema>> file_schema;
    auto schema_descriptor = parquet_file_reader->metadata()->schema();
    ASSERT_NE(schema_descriptor, nullptr);
    ASSERT_TRUE(
            format::parquet::build_parquet_column_schema(*schema_descriptor, &file_schema).ok());
    ASSERT_EQ(file_schema.size(), 1);
    ASSERT_EQ(file_schema[0]->children.size(), 2);
    ASSERT_EQ(file_schema[0]->children[0]->name, "id");

    format::FileScanRequest request;

    format::parquet::RowGroupScanPlan plan;
    format::parquet::ParquetScanRange scan_range;
    ASSERT_TRUE(format::parquet::plan_parquet_row_groups(*parquet_file_reader->metadata(),
                                                         parquet_file_reader.get(), file_schema,
                                                         request, scan_range, false, &plan)
                        .ok());
    ASSERT_EQ(plan.row_groups.size(), 2);
    EXPECT_EQ(plan.pruning_stats.total_row_groups, 2);
    EXPECT_EQ(plan.pruning_stats.selected_row_groups, 2);
    EXPECT_EQ(plan.pruning_stats.filtered_row_groups_by_statistics, 0);
    EXPECT_EQ(plan.pruning_stats.filtered_group_rows, 0);
}

TEST_F(NewParquetReaderTest, NestedStructPredicateDoesNotFilterRowGroupsByDictionary) {
    write_nested_dictionary_filter_parquet_file(_file_path);
    auto parquet_file_reader = ::parquet::ParquetFileReader::OpenFile(_file_path, false);
    ASSERT_EQ(parquet_file_reader->metadata()->num_row_groups(), 6);
    for (int row_group_idx = 0; row_group_idx < 6; ++row_group_idx) {
        auto row_group = parquet_file_reader->metadata()->RowGroup(row_group_idx);
        ASSERT_NE(row_group, nullptr);
        auto name_chunk = row_group->ColumnChunk(1);
        ASSERT_NE(name_chunk, nullptr);
        ASSERT_TRUE(name_chunk->has_dictionary_page());
        ASSERT_TRUE(name_chunk->statistics() == nullptr || !name_chunk->statistics()->HasMinMax());
    }

    std::vector<std::unique_ptr<format::parquet::ParquetColumnSchema>> file_schema;
    auto schema_descriptor = parquet_file_reader->metadata()->schema();
    ASSERT_NE(schema_descriptor, nullptr);
    ASSERT_TRUE(
            format::parquet::build_parquet_column_schema(*schema_descriptor, &file_schema).ok());
    ASSERT_EQ(file_schema.size(), 1);
    ASSERT_EQ(file_schema[0]->children.size(), 2);
    ASSERT_EQ(file_schema[0]->children[1]->name, "name");

    format::FileScanRequest request;

    format::parquet::RowGroupScanPlan plan;
    format::parquet::ParquetScanRange scan_range;
    ASSERT_TRUE(format::parquet::plan_parquet_row_groups(*parquet_file_reader->metadata(),
                                                         parquet_file_reader.get(), file_schema,
                                                         request, scan_range, false, &plan)
                        .ok());
    ASSERT_EQ(plan.row_groups.size(), 6);
    EXPECT_EQ(plan.pruning_stats.total_row_groups, 6);
    EXPECT_EQ(plan.pruning_stats.selected_row_groups, 6);
    EXPECT_EQ(plan.pruning_stats.filtered_row_groups_by_dictionary, 0);
    EXPECT_EQ(plan.pruning_stats.filtered_group_rows, 0);
}

TEST_F(NewParquetReaderTest, PlannerNarrowsRowRangesByPageIndex) {
    write_page_index_filter_parquet_file(_file_path);
    auto parquet_file_reader = ::parquet::ParquetFileReader::OpenFile(_file_path, false);
    ASSERT_EQ(parquet_file_reader->metadata()->num_row_groups(), 1);
    auto page_index_reader = parquet_file_reader->GetPageIndexReader();
    ASSERT_NE(page_index_reader, nullptr);
    auto row_group_index_reader = page_index_reader->RowGroup(0);
    ASSERT_NE(row_group_index_reader, nullptr);
    auto offset_index = row_group_index_reader->GetOffsetIndex(0);
    ASSERT_NE(offset_index, nullptr);
    ASSERT_GT(offset_index->page_locations().size(), 1);

    std::vector<std::unique_ptr<format::parquet::ParquetColumnSchema>> file_schema;
    auto schema_descriptor = parquet_file_reader->metadata()->schema();
    ASSERT_NE(schema_descriptor, nullptr);
    ASSERT_TRUE(
            format::parquet::build_parquet_column_schema(*schema_descriptor, &file_schema).ok());
    ASSERT_EQ(file_schema.size(), 1);

    format::FileScanRequest request;
    request.predicate_columns = {field_projection(0)};
    request.local_positions.emplace(format::LocalColumnId(0), format::LocalIndex(0));
    request.conjuncts.push_back(create_int32_greater_than_conjunct(0, 63));

    format::parquet::RowGroupScanPlan plan;
    format::parquet::ParquetScanRange scan_range;
    ASSERT_TRUE(format::parquet::plan_parquet_row_groups(*parquet_file_reader->metadata(),
                                                         parquet_file_reader.get(), file_schema,
                                                         request, scan_range, false, &plan)
                        .ok());
    ASSERT_EQ(plan.row_groups.size(), 1);
    ASSERT_FALSE(plan.row_groups[0].selected_ranges.empty());
    EXPECT_GT(plan.row_groups[0].selected_ranges.front().start, 0);
    EXPECT_LT(plan.row_groups[0].selected_ranges.front().length, 128);
    auto skip_plan_it = plan.row_groups[0].page_skip_plans.find(0);
    ASSERT_NE(skip_plan_it, plan.row_groups[0].page_skip_plans.end());
    EXPECT_EQ(skip_plan_it->second.leaf_column_id, 0);
    EXPECT_GT(skip_plan_it->second.skipped_ranges.size(), 0);
    EXPECT_GT(skip_plan_it->second.skipped_pages.size(), 1);
    ASSERT_EQ(skip_plan_it->second.skipped_pages.size(),
              skip_plan_it->second.skipped_page_compressed_sizes.size());
    int64_t skipped_compressed_bytes = 0;
    for (size_t page_idx = 0; page_idx < skip_plan_it->second.skipped_pages.size(); ++page_idx) {
        if (skip_plan_it->second.should_skip_page(page_idx)) {
            skipped_compressed_bytes += skip_plan_it->second.skipped_page_compressed_size(page_idx);
        }
    }
    EXPECT_GT(skipped_compressed_bytes, 0);
    EXPECT_EQ(plan.pruning_stats.total_row_groups, 1);
    EXPECT_EQ(plan.pruning_stats.selected_row_groups, 1);
    EXPECT_EQ(plan.pruning_stats.filtered_row_groups_by_page_index, 0);
    EXPECT_GT(plan.pruning_stats.filtered_page_rows, 0);
    EXPECT_EQ(plan.pruning_stats.selected_row_ranges, plan.row_groups[0].selected_ranges.size());
}

TEST_F(NewParquetReaderTest, NestedStructPredicateDoesNotNarrowRowRangesByPageIndex) {
    write_nested_page_index_filter_parquet_file(_file_path);
    auto parquet_file_reader = ::parquet::ParquetFileReader::OpenFile(_file_path, false);
    ASSERT_EQ(parquet_file_reader->metadata()->num_row_groups(), 1);
    auto page_index_reader = parquet_file_reader->GetPageIndexReader();
    ASSERT_NE(page_index_reader, nullptr);
    auto row_group_index_reader = page_index_reader->RowGroup(0);
    ASSERT_NE(row_group_index_reader, nullptr);
    auto offset_index = row_group_index_reader->GetOffsetIndex(0);
    ASSERT_NE(offset_index, nullptr);
    ASSERT_GT(offset_index->page_locations().size(), 1);

    std::vector<std::unique_ptr<format::parquet::ParquetColumnSchema>> file_schema;
    auto schema_descriptor = parquet_file_reader->metadata()->schema();
    ASSERT_NE(schema_descriptor, nullptr);
    ASSERT_TRUE(
            format::parquet::build_parquet_column_schema(*schema_descriptor, &file_schema).ok());
    ASSERT_EQ(file_schema.size(), 1);
    ASSERT_EQ(file_schema[0]->children.size(), 2);
    ASSERT_EQ(file_schema[0]->children[0]->name, "id");

    format::FileScanRequest request;
    request.local_positions.emplace(format::LocalColumnId(0), format::LocalIndex(0));
    request.conjuncts.push_back(create_int32_greater_than_conjunct(0, 63));

    format::parquet::RowGroupScanPlan plan;
    format::parquet::ParquetScanRange scan_range;
    ASSERT_TRUE(format::parquet::plan_parquet_row_groups(*parquet_file_reader->metadata(),
                                                         parquet_file_reader.get(), file_schema,
                                                         request, scan_range, false, &plan)
                        .ok());
    ASSERT_EQ(plan.row_groups.size(), 1);
    ASSERT_FALSE(plan.row_groups[0].selected_ranges.empty());
    EXPECT_EQ(plan.row_groups[0].selected_ranges.front().start, 0);
    EXPECT_EQ(plan.row_groups[0].selected_ranges.front().length,
              parquet_file_reader->metadata()->RowGroup(0)->num_rows());
    EXPECT_TRUE(plan.row_groups[0].page_skip_plans.empty());
    EXPECT_EQ(plan.pruning_stats.total_row_groups, 1);
    EXPECT_EQ(plan.pruning_stats.selected_row_groups, 1);
    EXPECT_EQ(plan.pruning_stats.filtered_row_groups_by_page_index, 0);
    EXPECT_EQ(plan.pruning_stats.filtered_page_rows, 0);
    EXPECT_EQ(plan.pruning_stats.selected_row_ranges, plan.row_groups[0].selected_ranges.size());
}

TEST_F(NewParquetReaderTest, PageIndexFilteredPagesDoNotDoubleSkipOutputColumns) {
    write_page_index_filter_pair_parquet_file(_file_path);
    RuntimeProfile profile("new_parquet_reader_page_skip");
    auto reader = create_reader(0, -1, &profile);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);
    Block block = build_file_block(schema);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->non_predicate_columns = {field_projection(1)};
    request->conjuncts.push_back(create_int32_greater_than_conjunct(0, 63));
    ASSERT_TRUE(reader->open(request).ok());

    std::vector<int32_t> ids;
    std::vector<int32_t> payloads;
    bool eof = false;
    while (!eof) {
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (rows == 0) {
            continue;
        }
        const auto& id_column = nullable_nested_column<ColumnInt32>(block, 0);
        const auto& payload_column = nullable_nested_column<ColumnInt32>(block, 1);
        for (size_t row = 0; row < rows; ++row) {
            ids.push_back(id_column.get_element(row));
            payloads.push_back(payload_column.get_element(row));
        }
    }

    ASSERT_NE(profile.get_counter("PagesSkippedByDataPageFilter"), nullptr);
    ASSERT_NE(profile.get_counter("DataPageFilterSkipBytes"), nullptr);
    ASSERT_NE(profile.get_counter("RawRowsRead"), nullptr);
    ASSERT_NE(profile.get_counter("SelectedRows"), nullptr);
    ASSERT_NE(profile.get_counter("RangeGapSkippedRows"), nullptr);
    ASSERT_NE(profile.get_counter("ReaderSkipRows"), nullptr);
    ASSERT_NE(profile.get_counter("RowGroupFilterTime"), nullptr);
    ASSERT_NE(profile.get_counter("PageIndexFilterTime"), nullptr);
    ASSERT_NE(profile.get_counter("PageIndexReadTime"), nullptr);
    EXPECT_GT(profile.get_counter("PagesSkippedByDataPageFilter")->value(), 0);
    EXPECT_GT(profile.get_counter("DataPageFilterSkipBytes")->value(), 0);
    EXPECT_EQ(profile.get_counter("RawRowsRead")->value(), 64);
    EXPECT_EQ(profile.get_counter("SelectedRows")->value(), 64);
    EXPECT_GT(profile.get_counter("RangeGapSkippedRows")->value(), 0);
    EXPECT_EQ(profile.get_counter("ReaderSkipRows")->value(), 0);
    EXPECT_GT(profile.get_counter("RowGroupFilterTime")->value(), 0);
    EXPECT_GT(profile.get_counter("PageIndexFilterTime")->value(), 0);
    EXPECT_GT(profile.get_counter("PageIndexReadTime")->value(), 0);

    ASSERT_EQ(ids.size(), 64);
    ASSERT_EQ(payloads.size(), ids.size());
    for (size_t row = 0; row < ids.size(); ++row) {
        EXPECT_EQ(ids[row], static_cast<int32_t>(row + 64));
        EXPECT_EQ(payloads[row], ids[row] + 1000);
    }
}

TEST_F(NewParquetReaderTest, InPredicateFiltersRowGroupsByDictionary) {
    write_dictionary_filter_parquet_file(_file_path);
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(1)};
    request->non_predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(create_string_in_conjunct(1, {"az", "za"}));
    use_schema_order_positions(request.get(), schema);
    ASSERT_TRUE(reader->open(request).ok());

    std::vector<int32_t> ids;
    std::vector<std::string> values;
    bool eof = false;
    while (!eof) {
        Block block = build_file_block(schema);
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (rows == 0) {
            continue;
        }
        const auto& id_column = nullable_nested_column<ColumnInt32>(block, 0);
        const auto& value_column = nullable_nested_column<ColumnString>(block, 1);
        for (size_t row = 0; row < rows; ++row) {
            ids.push_back(id_column.get_element(row));
            values.push_back(value_column.get_data_at(row).to_string());
        }
    }

    EXPECT_EQ(ids, std::vector<int32_t>({2, 5}));
    EXPECT_EQ(values, std::vector<std::string>({"az", "za"}));
}

TEST_F(NewParquetReaderTest, DictionaryPageV2StringEdgesSurviveSelection) {
    write_dictionary_edge_parquet_file(_file_path);
    auto parquet_file_reader = ::parquet::ParquetFileReader::OpenFile(_file_path, false);
    ASSERT_EQ(parquet_file_reader->metadata()->num_row_groups(), 4);
    for (int row_group_idx = 0; row_group_idx < 4; ++row_group_idx) {
        auto row_group = parquet_file_reader->metadata()->RowGroup(row_group_idx);
        ASSERT_NE(row_group, nullptr);
        ASSERT_TRUE(row_group->ColumnChunk(1)->has_dictionary_page());
    }

    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(1)};
    request->non_predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(create_string_in_conjunct(1, {"", "same"}));
    use_schema_order_positions(request.get(), schema);
    ASSERT_TRUE(reader->open(request).ok());

    std::vector<int32_t> ids;
    std::vector<std::string> values;
    bool eof = false;
    while (!eof) {
        Block block = build_file_block(schema);
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (rows == 0) {
            continue;
        }
        const auto& id_column = nullable_nested_column<ColumnInt32>(block, 0);
        const auto& value_column = nullable_nested_column<ColumnString>(block, 1);
        for (size_t row = 0; row < rows; ++row) {
            ids.push_back(id_column.get_element(row));
            values.push_back(value_column.get_data_at(row).to_string());
        }
    }

    EXPECT_EQ(ids, std::vector<int32_t>({1, 2, 5, 7}));
    EXPECT_EQ(values, std::vector<std::string>({"", "same", "", "same"}));
}

TEST_F(NewParquetReaderTest, StatisticsPruningSkipsPrefixRowGroupsAndReadsLaterGroups) {
    write_parquet_file(_file_path, 1);
    auto parquet_file_reader = ::parquet::ParquetFileReader::OpenFile(_file_path, false);
    ASSERT_EQ(parquet_file_reader->metadata()->num_row_groups(), 5);

    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->non_predicate_columns = {field_projection(1)};
    request->conjuncts.push_back(create_int32_greater_than_conjunct(0, 3));
    ASSERT_TRUE(reader->open(request).ok());

    std::vector<int32_t> ids;
    std::vector<std::string> values;
    bool eof = false;
    while (!eof) {
        Block block = build_file_block(schema);
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (rows == 0) {
            continue;
        }
        const auto& id_column = nullable_nested_column<ColumnInt32>(block, 0);
        const auto& value_column = nullable_nested_column<ColumnString>(block, 1);
        for (size_t row = 0; row < rows; ++row) {
            ids.push_back(id_column.get_element(row));
            values.push_back(value_column.get_data_at(row).to_string());
        }
    }

    EXPECT_EQ(ids, std::vector<int32_t>({4, 5}));
    EXPECT_EQ(values, std::vector<std::string>({"four", "five"}));
}

TEST_F(NewParquetReaderTest, RowPositionReaderReturnsFileLocalPositions) {
    write_parquet_file(_file_path, 2);
    auto parquet_file_reader = ::parquet::ParquetFileReader::OpenFile(_file_path, false);
    ASSERT_EQ(parquet_file_reader->metadata()->num_row_groups(), 3);

    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<format::FileScanRequest>();
    request->non_predicate_columns = {field_projection(format::ROW_POSITION_COLUMN_ID),
                                      field_projection(0)};
    request->local_positions = {
            {format::LocalColumnId(0), format::LocalIndex(0)},
            {format::LocalColumnId(format::ROW_POSITION_COLUMN_ID), format::LocalIndex(2)},
    };
    ASSERT_TRUE(reader->open(request).ok());

    std::vector<int64_t> row_positions;
    std::vector<int32_t> ids;
    bool eof = false;
    while (!eof) {
        Block block = build_file_block_with_row_position(schema);
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (rows == 0) {
            continue;
        }
        const auto& id_column = nullable_nested_column<ColumnInt32>(block, 0);
        const auto& row_position_column =
                assert_cast<const ColumnInt64&>(*block.get_by_position(2).column);
        for (size_t row = 0; row < rows; ++row) {
            ids.push_back(id_column.get_element(row));
            row_positions.push_back(row_position_column.get_element(row));
        }
    }

    EXPECT_EQ(ids, std::vector<int32_t>({1, 2, 3, 4, 5}));
    EXPECT_EQ(row_positions, std::vector<int64_t>({0, 1, 2, 3, 4}));
}

TEST_F(NewParquetReaderTest, RowPositionReaderKeepsPositionsAfterSelection) {
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    Block block = build_file_block_with_row_position(schema);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->non_predicate_columns = {field_projection(format::ROW_POSITION_COLUMN_ID)};
    request->local_positions = {
            {format::LocalColumnId(0), format::LocalIndex(0)},
            {format::LocalColumnId(format::ROW_POSITION_COLUMN_ID), format::LocalIndex(2)},
    };
    request->conjuncts.push_back(create_int32_greater_than_conjunct(0, 2));
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_FALSE(eof);
    ASSERT_EQ(rows, 3);

    const auto& id_column = nullable_nested_column<ColumnInt32>(block, 0);
    const auto& row_position_column =
            assert_cast<const ColumnInt64&>(*block.get_by_position(2).column);
    EXPECT_EQ(id_column.get_element(0), 3);
    EXPECT_EQ(id_column.get_element(1), 4);
    EXPECT_EQ(id_column.get_element(2), 5);
    EXPECT_EQ(row_position_column.get_element(0), 2);
    EXPECT_EQ(row_position_column.get_element(1), 3);
    EXPECT_EQ(row_position_column.get_element(2), 4);
}

TEST_F(NewParquetReaderTest, DeletePredicateFiltersRowPositions) {
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    Block block = build_file_block_with_row_position(schema);

    static const std::vector<int64_t> deleted_rows {1, 3};
    auto delete_predicate = std::make_shared<format::DeletePredicate>(deleted_rows);
    delete_predicate->add_child(VSlotRef::create_shared(2, 2, -1, std::make_shared<DataTypeInt64>(),
                                                        format::ROW_POSITION_COLUMN_NAME));

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(format::ROW_POSITION_COLUMN_ID)};
    request->non_predicate_columns = {field_projection(0)};
    request->local_positions = {
            {format::LocalColumnId(0), format::LocalIndex(0)},
            {format::LocalColumnId(format::ROW_POSITION_COLUMN_ID), format::LocalIndex(2)},
    };
    request->delete_conjuncts.push_back(VExprContext::create_shared(std::move(delete_predicate)));
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_FALSE(eof);
    ASSERT_EQ(rows, 3);

    const auto& id_column = nullable_nested_column<ColumnInt32>(block, 0);
    const auto& row_position_column =
            assert_cast<const ColumnInt64&>(*block.get_by_position(2).column);
    EXPECT_EQ(id_column.get_element(0), 1);
    EXPECT_EQ(id_column.get_element(1), 3);
    EXPECT_EQ(id_column.get_element(2), 5);
    EXPECT_EQ(row_position_column.get_element(0), 0);
    EXPECT_EQ(row_position_column.get_element(1), 2);
    EXPECT_EQ(row_position_column.get_element(2), 4);
}

TEST_F(NewParquetReaderTest, QueryPredicateAndDeletePredicateFilterRowPositions) {
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    Block block = build_file_block_with_row_position(schema);

    static const std::vector<int64_t> deleted_rows {3};
    auto delete_predicate = std::make_shared<format::DeletePredicate>(deleted_rows);
    delete_predicate->add_child(VSlotRef::create_shared(2, 2, -1, std::make_shared<DataTypeInt64>(),
                                                        format::ROW_POSITION_COLUMN_NAME));

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0),
                                  field_projection(format::ROW_POSITION_COLUMN_ID)};
    request->non_predicate_columns = {};
    request->local_positions = {
            {format::LocalColumnId(0), format::LocalIndex(0)},
            {format::LocalColumnId(format::ROW_POSITION_COLUMN_ID), format::LocalIndex(2)},
    };
    request->conjuncts.push_back(create_int32_greater_than_conjunct(0, 2));
    request->delete_conjuncts.push_back(VExprContext::create_shared(std::move(delete_predicate)));
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_FALSE(eof);
    ASSERT_EQ(rows, 2);

    const auto& id_column = nullable_nested_column<ColumnInt32>(block, 0);
    const auto& row_position_column =
            assert_cast<const ColumnInt64&>(*block.get_by_position(2).column);
    EXPECT_EQ(id_column.get_element(0), 3);
    EXPECT_EQ(id_column.get_element(1), 5);
    EXPECT_EQ(row_position_column.get_element(0), 2);
    EXPECT_EQ(row_position_column.get_element(1), 4);
}

TEST_F(NewParquetReaderTest, RowPositionReaderUsesFileLocalPositionsForScanRange) {
    write_parquet_file(_file_path, 2);
    auto parquet_file_reader = ::parquet::ParquetFileReader::OpenFile(_file_path, false);
    ASSERT_EQ(parquet_file_reader->metadata()->num_row_groups(), 3);

    const std::vector<std::vector<int32_t>> expected_ids = {{1, 2}, {3, 4}, {5}};
    const std::vector<std::vector<int64_t>> expected_row_positions = {{0, 1}, {2, 3}, {4}};
    for (int row_group_idx = 0; row_group_idx < 3; ++row_group_idx) {
        const auto [range_start_offset, range_size] =
                row_group_mid_range(_file_path, row_group_idx);
        auto reader = create_reader(range_start_offset, range_size);
        RuntimeState state {TQueryOptions(), TQueryGlobals()};
        ASSERT_TRUE(reader->init(&state).ok());

        std::vector<format::ColumnDefinition> schema;
        ASSERT_TRUE(reader->get_schema(&schema).ok());
        auto request = std::make_shared<format::FileScanRequest>();
        request->non_predicate_columns = {field_projection(format::ROW_POSITION_COLUMN_ID),
                                          field_projection(0)};
        request->local_positions = {
                {format::LocalColumnId(0), format::LocalIndex(0)},
                {format::LocalColumnId(format::ROW_POSITION_COLUMN_ID), format::LocalIndex(2)},
        };
        ASSERT_TRUE(reader->open(request).ok());

        std::vector<int32_t> ids;
        std::vector<int64_t> row_positions;
        bool eof = false;
        while (!eof) {
            Block block = build_file_block_with_row_position(schema);
            size_t rows = 0;
            ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
            if (rows == 0) {
                continue;
            }
            const auto& id_column = nullable_nested_column<ColumnInt32>(block, 0);
            const auto& row_position_column =
                    assert_cast<const ColumnInt64&>(*block.get_by_position(2).column);
            for (size_t row = 0; row < rows; ++row) {
                ids.push_back(id_column.get_element(row));
                row_positions.push_back(row_position_column.get_element(row));
            }
        }

        EXPECT_EQ(ids, expected_ids[row_group_idx]);
        EXPECT_EQ(row_positions, expected_row_positions[row_group_idx]);
    }
}

} // namespace
} // namespace doris
