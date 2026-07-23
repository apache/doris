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

#include "format_v2/parquet/parquet_scan.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <gtest/gtest.h>
#include <parquet/api/reader.h>
#include <parquet/api/writer.h>
#include <parquet/arrow/writer.h>
#include <parquet/encoding.h>

#include <cstring>
#include <filesystem>
#include <fstream>
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
#include "core/column/column_array.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/field.h"
#include "exprs/vectorized_fn_call.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "exprs/vliteral.h"
#include "exprs/vslot_ref.h"
#include "format_v2/expr/delete_predicate.h"
#include "format_v2/file_reader.h"
#include "format_v2/parquet/parquet_column_schema.h"
#include "format_v2/parquet/parquet_reader.h"
#include "format_v2/parquet/reader/native_column_reader.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"
#include "io/io_common.h"
#include "runtime/runtime_state.h"
#include "storage/index/zone_map/zonemap_eval_context.h"
#include "storage/index/zone_map/zonemap_filter_result.h"
#include "storage/utils.h"
#include "util/coding.h"
#include "util/thrift_util.h"

namespace doris {
namespace {

format::LocalColumnIndex field_projection(int32_t column_id) {
    return format::LocalColumnIndex {.index = column_id};
}

const ColumnInt32& int32_data_column(const IColumn& column) {
    if (const auto* nullable_column = check_and_get_column<ColumnNullable>(&column)) {
        return assert_cast<const ColumnInt32&>(nullable_column->get_nested_column());
    }
    return assert_cast<const ColumnInt32&>(column);
}

const ColumnInt64& int64_data_column(const IColumn& column) {
    if (const auto* nullable_column = check_and_get_column<ColumnNullable>(&column)) {
        return assert_cast<const ColumnInt64&>(nullable_column->get_nested_column());
    }
    return assert_cast<const ColumnInt64&>(column);
}

const ColumnString& string_data_column(const IColumn& column) {
    if (const auto* nullable_column = check_and_get_column<ColumnNullable>(&column)) {
        return assert_cast<const ColumnString&>(nullable_column->get_nested_column());
    }
    return assert_cast<const ColumnString&>(column);
}

TEST(ParquetScanMetadataSafetyTest, CheckedChunkRangesDrivePrefetchAndSplitAssignment) {
    tparquet::FileMetaData metadata;
    tparquet::SchemaElement root;
    root.__set_name("schema");
    root.__set_num_children(1);
    tparquet::SchemaElement leaf;
    leaf.__set_name("value");
    leaf.__set_type(tparquet::Type::INT32);
    leaf.__set_repetition_type(tparquet::FieldRepetitionType::REQUIRED);
    metadata.__set_schema({root, leaf});

    tparquet::ColumnMetaData column;
    column.__set_type(tparquet::Type::INT32);
    column.__set_data_page_offset(100);
    column.__set_dictionary_page_offset(-1);
    column.__set_total_compressed_size(20);
    tparquet::ColumnChunk chunk;
    chunk.__set_meta_data(column);
    tparquet::RowGroup row_group;
    row_group.__set_num_rows(1);
    row_group.__set_columns({chunk});
    metadata.__set_row_groups({row_group});

    auto schema = std::make_unique<format::parquet::ParquetColumnSchema>();
    schema->local_id = 0;
    schema->leaf_column_id = 0;
    schema->type = std::make_shared<DataTypeInt32>();
    std::vector<std::unique_ptr<format::parquet::ParquetColumnSchema>> file_schema;
    file_schema.push_back(std::move(schema));
    std::vector<format::parquet::ParquetPageCacheRange> ranges;
    ASSERT_TRUE(format::parquet::detail::build_native_prefetch_ranges(
                        metadata, file_schema, {field_projection(0)}, 0, 200, false, &ranges)
                        .ok());
    ASSERT_EQ(ranges.size(), 1);
    EXPECT_EQ(ranges[0].offset, 100);
    EXPECT_EQ(ranges[0].size, 20);

    std::vector<int64_t> first_rows;
    std::vector<int> selected;
    format::parquet::ParquetScanRange first_split {
            .start_offset = 0, .size = 100, .file_size = 200};
    ASSERT_TRUE(format::parquet::detail::select_native_row_groups_by_scan_range(
                        metadata, first_split, &first_rows, &selected)
                        .ok());
    EXPECT_TRUE(selected.empty());
    format::parquet::ParquetScanRange second_split {
            .start_offset = 100, .size = 100, .file_size = 200};
    ASSERT_TRUE(format::parquet::detail::select_native_row_groups_by_scan_range(
                        metadata, second_split, &first_rows, &selected)
                        .ok());
    EXPECT_EQ(selected, std::vector<int>({0}));

    metadata.row_groups[0].columns[0].meta_data.__set_data_page_offset(190);
    EXPECT_FALSE(format::parquet::detail::build_native_prefetch_ranges(
                         metadata, file_schema, {field_projection(0)}, 0, 200, false, &ranges)
                         .ok());
}

class Int32ZoneMapExpr final : public VExpr {
public:
    enum class Op { GE, GT, LT };

    Int32ZoneMapExpr(int column_id, Op op, int32_t value)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _op(op),
              _value(value) {}

    const std::string& expr_name() const override { return _expr_name; }

    Status execute_column_impl(VExprContext*, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        DORIS_CHECK(block != nullptr);
        DORIS_CHECK(selector == nullptr);
        DORIS_CHECK(_column_id >= 0 && _column_id < static_cast<int>(block->columns()));
        const auto& data_column = int32_data_column(*block->get_by_position(_column_id).column);
        DORIS_CHECK(data_column.size() >= count);

        auto result = ColumnUInt8::create(count, 0);
        auto& result_data = result->get_data();
        for (size_t row = 0; row < count; ++row) {
            const auto value = data_column.get_element(row);
            if (_op == Op::GE) {
                result_data[row] = value >= _value;
            } else if (_op == Op::GT) {
                result_data[row] = value > _value;
            } else {
                result_data[row] = value < _value;
            }
        }
        result_column = std::move(result);
        return Status::OK();
    }

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
        if (_op == Op::GE) {
            return zone_map->max_value < literal ? ZoneMapFilterResult::kNoMatch
                                                 : ZoneMapFilterResult::kMayMatch;
        }
        if (_op == Op::GT) {
            return zone_map->max_value <= literal ? ZoneMapFilterResult::kNoMatch
                                                  : ZoneMapFilterResult::kMayMatch;
        }
        return zone_map->min_value >= literal ? ZoneMapFilterResult::kNoMatch
                                              : ZoneMapFilterResult::kMayMatch;
    }

private:
    int _column_id;
    Op _op;
    int32_t _value;
    const std::string _expr_name = "Int32ZoneMapExpr";
};

class Int32PairSumExpr final : public VExpr {
public:
    Int32PairSumExpr(int left_column_id, int right_column_id, int32_t upper_bound)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _left_column_id(left_column_id),
              _right_column_id(right_column_id),
              _upper_bound(upper_bound) {}

    const std::string& expr_name() const override { return _expr_name; }

    Status execute_column_impl(VExprContext*, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        DORIS_CHECK(block != nullptr);
        DORIS_CHECK(selector == nullptr);
        DORIS_CHECK(_left_column_id >= 0 && _left_column_id < static_cast<int>(block->columns()));
        DORIS_CHECK(_right_column_id >= 0 && _right_column_id < static_cast<int>(block->columns()));
        const auto& left_column =
                int32_data_column(*block->get_by_position(_left_column_id).column);
        const auto& right_column =
                int32_data_column(*block->get_by_position(_right_column_id).column);
        DORIS_CHECK(left_column.size() >= count);
        DORIS_CHECK(right_column.size() >= count);

        auto result = ColumnUInt8::create(count, 0);
        auto& result_data = result->get_data();
        for (size_t row = 0; row < count; ++row) {
            result_data[row] =
                    left_column.get_element(row) + right_column.get_element(row) < _upper_bound;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    void collect_slot_column_ids(std::set<int>& column_ids) const override {
        column_ids.insert(_left_column_id);
        column_ids.insert(_right_column_id);
    }

private:
    int _left_column_id;
    int _right_column_id;
    int32_t _upper_bound;
    const std::string _expr_name = "Int32PairSumExpr";
};

class Int32DirectGreaterExpr final : public VExpr {
public:
    Int32DirectGreaterExpr(int column_id, int32_t lower_bound)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _lower_bound(lower_bound) {}

    const std::string& expr_name() const override { return _expr_name; }

    Status execute_column_impl(VExprContext*, const Block* block, const Selector*, size_t count,
                               ColumnPtr& result_column) const override {
        DORIS_CHECK(block != nullptr);
        const auto& input = int32_data_column(*block->get_by_position(_column_id).column);
        auto result = ColumnUInt8::create(count, 0);
        for (size_t row = 0; row < count; ++row) {
            result->get_data()[row] = input.get_element(row) > _lower_bound;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    bool can_execute_on_raw_fixed_values(const DataTypePtr& data_type,
                                         int column_id) const override {
        return column_id == _column_id &&
               remove_nullable(data_type)->get_primitive_type() == TYPE_INT;
    }

    Status execute_on_raw_fixed_values(const uint8_t* values, size_t num_values, size_t value_width,
                                       const DataTypePtr&, int, uint8_t* matches) const override {
        DORIS_CHECK_EQ(value_width, sizeof(int32_t));
        for (size_t row = 0; row < num_values; ++row) {
            matches[row] &= unaligned_load<int32_t>(values + row * sizeof(int32_t)) > _lower_bound;
        }
        return Status::OK();
    }

    void collect_slot_column_ids(std::set<int>& column_ids) const override {
        column_ids.insert(_column_id);
    }

private:
    int _column_id;
    int32_t _lower_bound;
    const std::string _expr_name = "Int32DirectGreaterExpr";
};

class Int64DirectGreaterExpr final : public VExpr {
public:
    Int64DirectGreaterExpr(int column_id, int64_t lower_bound)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _lower_bound(lower_bound) {}

    const std::string& expr_name() const override { return _expr_name; }

    Status execute_column_impl(VExprContext*, const Block* block, const Selector*, size_t count,
                               ColumnPtr& result_column) const override {
        DORIS_CHECK(block != nullptr);
        const auto& input = int64_data_column(*block->get_by_position(_column_id).column);
        auto result = ColumnUInt8::create(count, 0);
        for (size_t row = 0; row < count; ++row) {
            result->get_data()[row] = input.get_element(row) > _lower_bound;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    bool can_execute_on_raw_fixed_values(const DataTypePtr& data_type,
                                         int column_id) const override {
        return column_id == _column_id &&
               remove_nullable(data_type)->get_primitive_type() == TYPE_BIGINT;
    }

    Status execute_on_raw_fixed_values(const uint8_t* values, size_t num_values, size_t value_width,
                                       const DataTypePtr&, int, uint8_t* matches) const override {
        if (value_width != sizeof(int64_t)) {
            return Status::Corruption("BIGINT raw predicate received {}-byte values", value_width);
        }
        for (size_t row = 0; row < num_values; ++row) {
            matches[row] &= unaligned_load<int64_t>(values + row * sizeof(int64_t)) > _lower_bound;
        }
        return Status::OK();
    }

    void collect_slot_column_ids(std::set<int>& column_ids) const override {
        column_ids.insert(_column_id);
    }

private:
    int _column_id;
    int64_t _lower_bound;
    const std::string _expr_name = "Int64DirectGreaterExpr";
};

class AlwaysTrueSingleColumnExpr final : public VExpr {
public:
    explicit AlwaysTrueSingleColumnExpr(int column_id)
            : VExpr(std::make_shared<DataTypeUInt8>(), false), _column_id(column_id) {}

    const std::string& expr_name() const override { return _expr_name; }

    Status execute_column_impl(VExprContext*, const Block*, const Selector*, size_t count,
                               ColumnPtr& result_column) const override {
        result_column = ColumnUInt8::create(count, 1);
        return Status::OK();
    }

    void collect_slot_column_ids(std::set<int>& column_ids) const override {
        column_ids.insert(_column_id);
    }

private:
    int _column_id;
    const std::string _expr_name = "AlwaysTrueSingleColumnExpr";
};

VExprContextSPtr create_int32_zonemap_conjunct(int column_id, Int32ZoneMapExpr::Op op,
                                               int32_t value) {
    return VExprContext::create_shared(std::make_shared<Int32ZoneMapExpr>(column_id, op, value));
}

VExprContextSPtr create_int32_function_conjunct(int column_id, const std::string& function_name,
                                                TExprOpcode::type opcode, int32_t value) {
    const auto int_type = std::make_shared<DataTypeInt32>();
    const auto nullable_int_type = make_nullable(int_type);
    const auto result_type = make_nullable(std::make_shared<DataTypeUInt8>());
    TFunctionName fn_name;
    fn_name.__set_function_name(function_name);
    TFunction fn;
    fn.__set_name(fn_name);
    fn.__set_binary_type(TFunctionBinaryType::BUILTIN);
    fn.__set_arg_types({nullable_int_type->to_thrift(), int_type->to_thrift()});
    fn.__set_ret_type(result_type->to_thrift());
    fn.__set_has_var_args(false);
    TExprNode node;
    node.__set_node_type(TExprNodeType::BINARY_PRED);
    node.__set_opcode(opcode);
    node.__set_type(result_type->to_thrift());
    node.__set_fn(fn);
    node.__set_num_children(2);
    node.__set_is_nullable(true);
    auto root = VectorizedFnCall::create_shared(node);
    root->add_child(
            VSlotRef::create_shared(column_id, column_id, -1, nullable_int_type, "plain_id"));
    root->add_child(VLiteral::create_shared(int_type, Field::create_field<TYPE_INT>(value)));
    auto context = VExprContext::create_shared(std::move(root));
    // Direct evaluation does not execute the expression, but a fallback must still fail this test
    // instead of silently using an unprepared test-only context.
    context->_prepared = true;
    context->_opened = true;
    return context;
}

VExprContextSPtr create_int32_mod_greater_than_conjunct(int column_id) {
    const auto int_type = std::make_shared<DataTypeInt32>();
    const auto nullable_int_type = make_nullable(int_type);
    const auto nullable_bool_type = make_nullable(std::make_shared<DataTypeUInt8>());
    auto function_node = [](const std::string& name, TExprNodeType::type node_type,
                            TExprOpcode::type opcode, const DataTypePtr& return_type,
                            const std::vector<DataTypePtr>& argument_types) {
        TFunctionName function_name;
        function_name.__set_function_name(name);
        TFunction function;
        function.__set_name(function_name);
        function.__set_binary_type(TFunctionBinaryType::BUILTIN);
        std::vector<TTypeDesc> thrift_argument_types;
        for (const auto& argument_type : argument_types) {
            thrift_argument_types.push_back(argument_type->to_thrift());
        }
        function.__set_arg_types(thrift_argument_types);
        function.__set_ret_type(return_type->to_thrift());
        function.__set_has_var_args(false);
        TExprNode node;
        node.__set_node_type(node_type);
        node.__set_opcode(opcode);
        node.__set_type(return_type->to_thrift());
        node.__set_fn(function);
        node.__set_num_children(static_cast<int16_t>(argument_types.size()));
        node.__set_is_nullable(return_type->is_nullable());
        return node;
    };

    auto modulo = VectorizedFnCall::create_shared(
            function_node("mod", TExprNodeType::ARITHMETIC_EXPR, TExprOpcode::MOD,
                          nullable_int_type, {nullable_int_type, int_type}));
    modulo->add_child(
            VSlotRef::create_shared(column_id, column_id, -1, nullable_int_type, "plain_id"));
    modulo->add_child(VLiteral::create_shared(int_type, Field::create_field<TYPE_INT>(-1)));

    auto greater_than = VectorizedFnCall::create_shared(
            function_node("gt", TExprNodeType::BINARY_PRED, TExprOpcode::GT, nullable_bool_type,
                          {nullable_int_type, int_type}));
    greater_than->add_child(std::move(modulo));
    greater_than->add_child(VLiteral::create_shared(int_type, Field::create_field<TYPE_INT>(0)));
    return VExprContext::create_shared(std::move(greater_than));
}

VExprContextSPtr create_int32_pair_sum_conjunct(int left_column_id, int right_column_id,
                                                int32_t upper_bound) {
    return VExprContext::create_shared(
            std::make_shared<Int32PairSumExpr>(left_column_id, right_column_id, upper_bound));
}

VExprContextSPtr create_int32_direct_greater_conjunct(int column_id, int32_t lower_bound) {
    return VExprContext::create_shared(
            std::make_shared<Int32DirectGreaterExpr>(column_id, lower_bound));
}

VExprContextSPtr create_int64_direct_greater_conjunct(int column_id, int64_t lower_bound) {
    return VExprContext::create_shared(
            std::make_shared<Int64DirectGreaterExpr>(column_id, lower_bound));
}

VExprContextSPtr create_always_true_single_column_conjunct(int column_id) {
    return VExprContext::create_shared(std::make_shared<AlwaysTrueSingleColumnExpr>(column_id));
}

int64_t counter_value(RuntimeProfile& profile, const std::string& name) {
    auto* counter = profile.get_counter(name);
    DORIS_CHECK(counter != nullptr);
    return counter->value();
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

std::shared_ptr<arrow::Array> build_uint32_array(const std::vector<uint32_t>& values) {
    arrow::UInt32Builder builder;
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

std::shared_ptr<arrow::Array> build_fixed_binary_array(const std::vector<std::string>& values,
                                                       int byte_width) {
    auto type = arrow::fixed_size_binary(byte_width);
    arrow::FixedSizeBinaryBuilder builder(type, arrow::default_memory_pool());
    for (const auto& value : values) {
        EXPECT_EQ(value.size(), byte_width);
        EXPECT_TRUE(builder.Append(reinterpret_cast<const uint8_t*>(value.data())).ok());
    }
    return finish_array(&builder);
}

std::shared_ptr<arrow::Array> build_struct_array(const std::vector<int32_t>& ids,
                                                 const std::vector<std::string>& names) {
    auto struct_type = arrow::struct_({arrow::field("id", arrow::int32(), false),
                                       arrow::field("name", arrow::utf8(), false)});
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> field_builders;
    field_builders.push_back(std::shared_ptr<arrow::ArrayBuilder>(
            std::make_unique<arrow::Int32Builder>().release()));
    field_builders.push_back(std::shared_ptr<arrow::ArrayBuilder>(
            std::make_unique<arrow::StringBuilder>().release()));
    arrow::StructBuilder builder(struct_type, arrow::default_memory_pool(),
                                 std::move(field_builders));
    auto* id_builder = assert_cast<arrow::Int32Builder*>(builder.field_builder(0));
    auto* name_builder = assert_cast<arrow::StringBuilder*>(builder.field_builder(1));
    for (size_t row = 0; row < ids.size(); ++row) {
        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(id_builder->Append(ids[row]).ok());
        EXPECT_TRUE(name_builder->Append(names[row]).ok());
    }
    return finish_array(&builder);
}

std::shared_ptr<arrow::Array> build_list_array() {
    auto value_builder = std::make_unique<arrow::Int32Builder>();
    arrow::ListBuilder builder(arrow::default_memory_pool(), std::move(value_builder));
    auto* int_builder = assert_cast<arrow::Int32Builder*>(builder.value_builder());
    EXPECT_TRUE(builder.Append().ok());
    EXPECT_TRUE(int_builder->Append(1).ok());
    EXPECT_TRUE(int_builder->Append(2).ok());
    EXPECT_TRUE(builder.Append().ok());
    EXPECT_TRUE(int_builder->Append(3).ok());
    EXPECT_TRUE(builder.Append().ok());
    return finish_array(&builder);
}

void write_table(const std::string& file_path, const std::shared_ptr<arrow::Table>& table,
                 int64_t row_group_size, bool enable_dictionary = false,
                 bool enable_page_index = false, bool enable_statistics = true) {
    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    builder.compression(::parquet::Compression::UNCOMPRESSED);
    if (enable_dictionary) {
        builder.enable_dictionary();
    } else {
        builder.disable_dictionary();
    }
    if (enable_page_index) {
        builder.enable_write_page_index();
        builder.write_batch_size(8);
        builder.data_pagesize(10);
    }
    if (!enable_statistics) {
        builder.disable_statistics();
    }
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out,
                                                      row_group_size, builder.build()));
}

void write_required_adjusted_time_parquet_file(const std::string& file_path) {
    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

    // Arrow intentionally writes time32 as local time (isAdjustedToUTC=false), so use Parquet's
    // low-level writer to produce the unsupported required logical type needed by this aggregate
    // regression. Required is important: Nereids may push COUNT(col) because NULL filtering cannot
    // change its value, which is precisely the path where an empty aggregate request lost `col`.
    const auto adjusted_time = ::parquet::schema::PrimitiveNode::Make(
            "unsupported_time", ::parquet::Repetition::REQUIRED,
            ::parquet::LogicalType::Time(true, ::parquet::LogicalType::TimeUnit::MILLIS),
            ::parquet::Type::INT32);
    const auto schema_node = ::parquet::schema::GroupNode::Make(
            "schema", ::parquet::Repetition::REQUIRED, {adjusted_time});
    const auto schema = std::static_pointer_cast<::parquet::schema::GroupNode>(schema_node);
    auto writer = ::parquet::ParquetFileWriter::Open(out, schema);
    auto* row_group = writer->AppendRowGroup();
    auto* time_writer = static_cast<::parquet::Int32Writer*>(row_group->NextColumn());
    const int32_t values[] = {1000, 2000};
    EXPECT_EQ(time_writer->WriteBatch(2, nullptr, nullptr, values), 2);
    time_writer->Close();
    row_group->Close();
    writer->Close();
}

void write_int_pair_parquet_file(const std::string& file_path, int64_t row_group_size = 2,
                                 bool enable_statistics = true) {
    auto schema = arrow::schema({
            arrow::field("id", arrow::int32(), false),
            arrow::field("score", arrow::int32(), false),
    });
    auto table = arrow::Table::Make(schema, {build_int32_array({1, 2, 3, 4, 5, 6}),
                                             build_int32_array({10, 20, 30, 40, 50, 60})});
    write_table(file_path, table, row_group_size, false, false, enable_statistics);
}

void write_uint32_pair_parquet_file(const std::string& file_path) {
    auto schema = arrow::schema({arrow::field("id", arrow::uint32(), false),
                                 arrow::field("score", arrow::int32(), false)});
    auto table = arrow::Table::Make(schema, {build_uint32_array({1, 2147483648U, 4294957294U}),
                                             build_int32_array({10, 20, 30})});
    write_table(file_path, table, 3, false, false, false);
}

std::vector<uint8_t> serialize_test_page(tparquet::PageHeader header,
                                         const std::vector<uint8_t>& payload) {
    std::vector<uint8_t> bytes;
    ThriftSerializer serializer(/*compact=*/true, 128);
    DORIS_CHECK(serializer.serialize(&header, &bytes).ok());
    bytes.insert(bytes.end(), payload.begin(), payload.end());
    return bytes;
}

void write_misdeclared_two_page_parquet_file(const std::string& file_path) {
    const std::array<int32_t, 2> first_values {1, 2};
    std::vector<uint8_t> first_payload(sizeof(first_values));
    memcpy(first_payload.data(), first_values.data(), first_payload.size());
    tparquet::PageHeader first_header;
    first_header.type = tparquet::PageType::DATA_PAGE_V2;
    first_header.__set_compressed_page_size(first_payload.size());
    first_header.__set_uncompressed_page_size(first_payload.size());
    first_header.__isset.data_page_header_v2 = true;
    first_header.data_page_header_v2.__set_num_values(first_values.size());
    first_header.data_page_header_v2.__set_num_nulls(0);
    first_header.data_page_header_v2.__set_num_rows(first_values.size());
    first_header.data_page_header_v2.__set_encoding(tparquet::Encoding::PLAIN);
    first_header.data_page_header_v2.__set_definition_levels_byte_length(0);
    first_header.data_page_header_v2.__set_repetition_levels_byte_length(0);
    first_header.data_page_header_v2.__set_is_compressed(false);
    auto column_bytes = serialize_test_page(first_header, first_payload);

    auto node = ::parquet::schema::PrimitiveNode::Make("id", ::parquet::Repetition::REQUIRED,
                                                       ::parquet::Type::INT32);
    ::parquet::ColumnDescriptor descriptor(node, 0, 0);
    auto encoder = ::parquet::MakeTypedEncoder<::parquet::Int32Type>(
            ::parquet::Encoding::DELTA_BINARY_PACKED, false, &descriptor);
    const int32_t second_values[] = {3, 4};
    encoder->Put(second_values, std::size(second_values));
    auto second_buffer = encoder->FlushValues();
    std::vector<uint8_t> second_payload(second_buffer->data(),
                                        second_buffer->data() + second_buffer->size());
    tparquet::PageHeader second_header = first_header;
    second_header.__set_compressed_page_size(second_payload.size());
    second_header.__set_uncompressed_page_size(second_payload.size());
    second_header.data_page_header_v2.__set_encoding(tparquet::Encoding::DELTA_BINARY_PACKED);
    auto second_page = serialize_test_page(second_header, second_payload);
    column_bytes.insert(column_bytes.end(), second_page.begin(), second_page.end());

    tparquet::SchemaElement root;
    root.__set_name("schema");
    root.__set_num_children(1);
    tparquet::SchemaElement leaf;
    leaf.__set_name("id");
    leaf.__set_type(tparquet::Type::INT32);
    leaf.__set_repetition_type(tparquet::FieldRepetitionType::REQUIRED);
    tparquet::ColumnMetaData column;
    column.__set_type(tparquet::Type::INT32);
    // Deliberately omit DELTA_BINARY_PACKED to emulate untrusted/incomplete footer metadata.
    column.__set_encodings({tparquet::Encoding::PLAIN, tparquet::Encoding::RLE});
    column.__set_path_in_schema({"id"});
    column.__set_codec(tparquet::CompressionCodec::UNCOMPRESSED);
    column.__set_num_values(4);
    column.__set_total_uncompressed_size(column_bytes.size());
    column.__set_total_compressed_size(column_bytes.size());
    column.__set_data_page_offset(4);
    tparquet::ColumnChunk chunk;
    chunk.__set_file_offset(4);
    chunk.__set_meta_data(column);
    tparquet::RowGroup row_group;
    row_group.__set_columns({chunk});
    row_group.__set_total_byte_size(column_bytes.size());
    row_group.__set_num_rows(4);
    tparquet::FileMetaData metadata;
    metadata.__set_version(2);
    metadata.__set_schema({root, leaf});
    metadata.__set_num_rows(4);
    metadata.__set_row_groups({row_group});

    std::vector<uint8_t> footer;
    ThriftSerializer serializer(/*compact=*/true, 1024);
    DORIS_CHECK(serializer.serialize(&metadata, &footer).ok());
    std::ofstream output(file_path, std::ios::binary | std::ios::trunc);
    output.write("PAR1", 4);
    output.write(reinterpret_cast<const char*>(column_bytes.data()), column_bytes.size());
    output.write(reinterpret_cast<const char*>(footer.data()), footer.size());
    std::array<uint8_t, sizeof(uint32_t)> footer_size {};
    encode_fixed32_le(footer_size.data(), cast_set<uint32_t>(footer.size()));
    output.write(reinterpret_cast<const char*>(footer_size.data()), footer_size.size());
    output.write("PAR1", 4);
    output.close();
    DORIS_CHECK(output.good());
}

void write_long_prefix_parquet_file(const std::string& file_path, size_t rows) {
    std::vector<int32_t> ids(rows);
    std::vector<int32_t> first(rows);
    std::vector<int32_t> second(rows);
    std::iota(ids.begin(), ids.end(), 0);
    for (size_t row = 0; row < rows; ++row) {
        first[row] = static_cast<int32_t>(row + 10);
        second[row] = static_cast<int32_t>(row + 20);
    }
    auto schema = arrow::schema({arrow::field("id", arrow::int32(), false),
                                 arrow::field("first", arrow::int32(), false),
                                 arrow::field("second", arrow::int32(), false)});
    auto table = arrow::Table::Make(
            schema, {build_int32_array(ids), build_int32_array(first), build_int32_array(second)});
    write_table(file_path, table, rows, false, false, false);
}

void write_binary_minmax_parquet_file(const std::string& file_path) {
    auto schema = arrow::schema({
            arrow::field("text", arrow::utf8(), false),
            arrow::field("fixed", arrow::fixed_size_binary(4), false),
    });
    auto table = arrow::Table::Make(schema, {build_string_array({"alpha", "omega"}),
                                             build_fixed_binary_array({"aaaa", "zzzz"}, 4)});
    write_table(file_path, table, 2);
}

void write_struct_parquet_file(const std::string& file_path) {
    auto struct_type = arrow::struct_({arrow::field("id", arrow::int32(), false),
                                       arrow::field("name", arrow::utf8(), false)});
    auto schema = arrow::schema({
            arrow::field("s", struct_type, false),
    });
    auto table = arrow::Table::Make(
            schema, {build_struct_array({1, 2, 10, 11}, {"one", "two", "ten", "eleven"})});
    write_table(file_path, table, 2);
}

void write_list_parquet_file(const std::string& file_path) {
    auto schema = arrow::schema({
            arrow::field("xs", arrow::list(arrow::int32()), false),
    });
    auto table = arrow::Table::Make(schema, {build_list_array()});
    write_table(file_path, table, 2);
}

void write_int_list_parquet_file(const std::string& file_path) {
    auto schema = arrow::schema({
            arrow::field("id", arrow::int32(), false),
            arrow::field("xs", arrow::list(arrow::int32()), false),
    });
    auto table = arrow::Table::Make(schema, {build_int32_array({1, 2, 3}), build_list_array()});
    write_table(file_path, table, 3);
}

void write_page_index_parquet_file(const std::string& file_path) {
    std::vector<int32_t> ids(128);
    std::iota(ids.begin(), ids.end(), 0);
    auto schema = arrow::schema({
            arrow::field("id", arrow::int32(), false),
    });
    auto table = arrow::Table::Make(schema, {build_int32_array(ids)});
    write_table(file_path, table, ids.size(), false, true);
}

void write_multi_row_group_page_index_parquet_file(const std::string& file_path) {
    std::vector<int32_t> ids(384);
    std::iota(ids.begin(), ids.end(), 0);
    auto schema = arrow::schema({arrow::field("id", arrow::int32(), false)});
    auto table = arrow::Table::Make(schema, {build_int32_array(ids)});
    write_table(file_path, table, 128, false, true);
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

Block build_file_block(const std::vector<format::ColumnDefinition>& schema) {
    Block block;
    for (const auto& field : schema) {
        block.insert({field.type->create_column(), field.type, field.name});
    }
    return block;
}

GlobalRowLoacationV2 decode_rowid(const ColumnString& column, size_t row) {
    const auto ref = column.get_data_at(row);
    EXPECT_EQ(ref.size, sizeof(GlobalRowLoacationV2));
    GlobalRowLoacationV2 location(0, 0, 0, 0);
    std::memcpy(&location, ref.data, sizeof(GlobalRowLoacationV2));
    return location;
}

void use_schema_order_positions(format::FileScanRequest* request,
                                const std::vector<format::ColumnDefinition>& schema) {
    DORIS_CHECK(request != nullptr);
    for (size_t idx = 0; idx < schema.size(); ++idx) {
        request->local_positions.emplace(format::LocalColumnId(schema[idx].local_id),
                                         format::LocalIndex(idx));
    }
}

class ParquetScanTest : public testing::Test {
protected:
    void SetUp() override {
        _test_dir = std::filesystem::temp_directory_path() / "doris_format_v2_parquet_scan_test";
        std::filesystem::remove_all(_test_dir);
        std::filesystem::create_directories(_test_dir);
        _file_path = (_test_dir / "scan.parquet").string();
    }

    void TearDown() override { std::filesystem::remove_all(_test_dir); }

    std::unique_ptr<format::parquet::ParquetReader> create_reader(
            int64_t range_start_offset = 0, int64_t range_size = -1,
            RuntimeProfile* profile = nullptr,
            std::optional<format::GlobalRowIdContext> global_rowid_context = std::nullopt,
            std::shared_ptr<io::IOContext> io_ctx = nullptr) const {
        auto system_properties = std::make_shared<io::FileSystemProperties>();
        system_properties->system_type = TFileType::FILE_LOCAL;
        auto file_description = std::make_unique<io::FileDescription>();
        file_description->path = _file_path;
        file_description->file_size = static_cast<int64_t>(std::filesystem::file_size(_file_path));
        file_description->range_start_offset = range_start_offset;
        file_description->range_size = range_size;
        return std::make_unique<format::parquet::ParquetReader>(system_properties, file_description,
                                                                std::move(io_ctx), profile,
                                                                global_rowid_context);
    }

    std::shared_ptr<format::FileScanRequest> open_all_row_groups(
            format::parquet::ParquetReader* reader) {
        auto request = std::make_shared<format::FileScanRequest>();
        EXPECT_TRUE(reader->open(request).ok());
        return request;
    }

    std::filesystem::path _test_dir;
    std::string _file_path;
};

TEST(ParquetScanSelectionTest, CompactFilterShrinksCurrentSelection) {
    format::parquet::SelectionVector selection(4);
    selection.set_index(0, 0);
    selection.set_index(1, 2);
    selection.set_index(2, 4);
    selection.set_index(3, 5);

    const IColumn::Filter compact_filter {1, 0, 1, 0};
    const auto selected_rows =
            format::parquet::apply_compact_filter_to_selection(compact_filter, &selection, 4);

    ASSERT_EQ(selected_rows, 2);
    EXPECT_EQ(selection.get_index(0), 0);
    EXPECT_EQ(selection.get_index(1), 4);
    EXPECT_TRUE(selection.verify(selected_rows, 6).ok());
}

TEST(ParquetScanAdaptivePredicateTest, OrdersByObservedCostPerRejectedRow) {
    using format::parquet::detail::AdaptivePredicateStats;
    std::unordered_map<size_t, AdaptivePredicateStats> stats;
    stats.emplace(0, AdaptivePredicateStats {
                             .cost_per_input_row_ns = 10, .survival_ratio = 0.8, .samples = 3});
    stats.emplace(1, AdaptivePredicateStats {
                             .cost_per_input_row_ns = 20, .survival_ratio = 0.1, .samples = 3});
    stats.emplace(2, AdaptivePredicateStats {
                             .cost_per_input_row_ns = 50, .survival_ratio = 0.5, .samples = 3});

    const auto order = format::parquet::detail::order_adaptive_predicates({0, 1, 2}, stats);
    EXPECT_EQ(order, std::vector<size_t>({1, 0, 2}));
    const auto prefetched = format::parquet::detail::adaptive_prefetch_prefix(order, stats, 0.25);
    EXPECT_EQ(prefetched, std::vector<size_t>({1}));
}

TEST(ParquetScanAdaptivePredicateTest, SamplesWarmupThenAtLowFrequency) {
    using format::parquet::detail::should_sample_adaptive_predicate;
    for (size_t samples = 0; samples < 8; ++samples) {
        EXPECT_TRUE(should_sample_adaptive_predicate(samples, samples));
    }
    EXPECT_FALSE(should_sample_adaptive_predicate(8, 9));
    EXPECT_TRUE(should_sample_adaptive_predicate(8, 16));
    EXPECT_FALSE(should_sample_adaptive_predicate(9, 17));
    EXPECT_TRUE(should_sample_adaptive_predicate(9, 32));
}

TEST(ParquetScanAdaptivePredicateTest, ThrowingNestedFunctionDisablesSelectedRowReordering) {
    using format::parquet::detail::AdaptivePredicateStats;
    std::unordered_map<size_t, AdaptivePredicateStats> first_batch_stats;
    first_batch_stats.emplace(
            0, AdaptivePredicateStats {
                       .cost_per_input_row_ns = 20, .survival_ratio = 0.99, .samples = 1});
    first_batch_stats.emplace(
            1, AdaptivePredicateStats {
                       .cost_per_input_row_ns = 1, .survival_ratio = 0.1, .samples = 1});
    EXPECT_EQ(format::parquet::detail::order_adaptive_predicates({0, 1}, first_batch_stats),
              (std::vector<size_t> {1, 0}));

    const auto total_comparison = create_int32_function_conjunct(0, "gt", TExprOpcode::GT, 0);
    const auto throwing_comparison = create_int32_mod_greater_than_conjunct(0);
    EXPECT_TRUE(total_comparison->root()->is_safe_to_execute_on_selected_rows());
    // The second-batch order above can reject MIN_INT before mod(MIN_INT, -1) runs. The nested
    // partial function must therefore keep the request on full-batch, error-preserving execution.
    EXPECT_FALSE(throwing_comparison->root()->is_safe_to_execute_on_selected_rows());
}

TEST(ParquetScanSmallFileTest, StagesOnlyBoundedHttpObjects) {
    using format::parquet::detail::should_stage_small_http_file;
    EXPECT_TRUE(should_stage_small_http_file("http://host/tiny.parquet", 512, 1024));
    EXPECT_TRUE(should_stage_small_http_file("https://host/tiny.parquet", 1024, 1024));
    EXPECT_FALSE(should_stage_small_http_file("https://host/large.parquet", 1025, 1024));
    EXPECT_FALSE(should_stage_small_http_file("/tmp/tiny.parquet", 512, 1024));
    EXPECT_FALSE(should_stage_small_http_file("s3://bucket/tiny.parquet", 512, 1024));
}

TEST(ParquetScanSelectionTest, NativeLazySkipBitmapIsBounded) {
    using format::parquet::detail::MAX_NATIVE_LAZY_SKIP_ROWS;
    using format::parquet::detail::bounded_native_lazy_skip_rows;
    EXPECT_EQ(bounded_native_lazy_skip_rows(1), 1);
    EXPECT_EQ(bounded_native_lazy_skip_rows(MAX_NATIVE_LAZY_SKIP_ROWS + 1),
              MAX_NATIVE_LAZY_SKIP_ROWS);
}

TEST(ParquetScanConditionCacheTest, HitKeepsCachedBaseWhenCurrentPlanStartsLater) {
    format::parquet::RowGroupScanPlan plan;
    plan.row_groups.push_back(
            {.row_group_id = 1,
             .first_file_row = ConditionCacheContext::GRANULE_SIZE,
             .row_group_rows = ConditionCacheContext::GRANULE_SIZE,
             .selected_ranges = {{.start = 0, .length = ConditionCacheContext::GRANULE_SIZE}},
             .page_skip_plans = {},
             .offset_indexes = {}});

    format::parquet::ParquetScanScheduler scheduler;
    scheduler.set_plan(std::move(plan));
    auto ctx = std::make_shared<ConditionCacheContext>();
    ctx->is_hit = true;
    ctx->base_granule = 0;
    ctx->filter_result = std::make_shared<std::vector<bool>>(std::vector<bool> {false});
    scheduler.set_condition_cache_context(ctx);

    EXPECT_FALSE(scheduler.empty());
    EXPECT_EQ(scheduler.condition_cache_filtered_rows(), 0);
    EXPECT_EQ(ctx->base_granule, 0);
}

TEST_F(ParquetScanTest, AggregateCountAndMinMaxUseAllSelectedRowGroups) {
    write_int_pair_parquet_file(_file_path);
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());
    open_all_row_groups(reader.get());

    format::FileAggregateResult count_result;
    format::FileAggregateRequest count_request;
    count_request.agg_type = TPushAggOp::COUNT;
    ASSERT_TRUE(reader->get_aggregate_result(count_request, &count_result).ok());
    EXPECT_EQ(count_result.count, 6);
    EXPECT_TRUE(count_result.columns.empty());

    format::FileAggregateResult required_count_result;
    format::FileAggregateRequest required_count_request;
    required_count_request.agg_type = TPushAggOp::COUNT;
    required_count_request.columns.push_back({.projection = field_projection(0)});
    ASSERT_TRUE(reader->get_aggregate_result(required_count_request, &required_count_result).ok());
    // The required scalar projection is retained for logical-type validation, but after that
    // validation COUNT(id) can reuse the same selected-row-group footer count as COUNT(*).
    EXPECT_EQ(required_count_result.count, 6);

    format::FileAggregateResult minmax_result;
    format::FileAggregateRequest minmax_request;
    minmax_request.agg_type = TPushAggOp::MINMAX;
    minmax_request.columns.push_back({.projection = field_projection(0)});
    minmax_request.columns.push_back({.projection = field_projection(1)});
    ASSERT_TRUE(reader->get_aggregate_result(minmax_request, &minmax_result).ok());
    EXPECT_EQ(minmax_result.count, 6);
    ASSERT_EQ(minmax_result.columns.size(), 2);
    EXPECT_TRUE(minmax_result.columns[0].has_min);
    EXPECT_TRUE(minmax_result.columns[0].has_max);
    EXPECT_EQ(minmax_result.columns[0].min_value.get<TYPE_INT>(), 1);
    EXPECT_EQ(minmax_result.columns[0].max_value.get<TYPE_INT>(), 6);
    EXPECT_EQ(minmax_result.columns[1].min_value.get<TYPE_INT>(), 10);
    EXPECT_EQ(minmax_result.columns[1].max_value.get<TYPE_INT>(), 60);
}

TEST_F(ParquetScanTest, AggregateCountRejectsRequiredUnsupportedScalarProjection) {
    write_required_adjusted_time_parquet_file(_file_path);
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());
    open_all_row_groups(reader.get());

    format::FileAggregateRequest request;
    request.agg_type = TPushAggOp::COUNT;
    request.columns.push_back({.projection = field_projection(0)});
    format::FileAggregateResult result;
    const auto status = reader->get_aggregate_result(request, &result);
    EXPECT_TRUE(status.is<ErrorCode::NOT_IMPLEMENTED_ERROR>()) << status;
    EXPECT_NE(status.to_string().find("Parquet TIME with isAdjustedToUTC=true is not supported"),
              std::string::npos);
}

TEST_F(ParquetScanTest, CountStarIgnoresUnsupportedPlannerPlaceholder) {
    write_required_adjusted_time_parquet_file(_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};

    // A normal projection still requests the TIME_MILLIS value and must fail at open, before
    // row-group pruning or physical INT32 statistics can hide the unsupported logical type.
    auto projected_reader = create_reader();
    ASSERT_TRUE(projected_reader->init(&state).ok());
    auto projected_request = std::make_shared<format::FileScanRequest>();
    format::FileScanRequestBuilder projected_builder(projected_request.get());
    ASSERT_TRUE(projected_builder.add_non_predicate_column(format::LocalColumnId(0)).ok());
    const auto projected_status = projected_reader->open(projected_request);
    EXPECT_TRUE(projected_status.is<ErrorCode::NOT_IMPLEMENTED_ERROR>()) << projected_status;

    // Metadata COUNT(*) carries the same retained scan slot, but its aggregate request has no
    // semantic column. The placeholder must not make open fail before footer row counts are used.
    auto metadata_count_reader = create_reader();
    ASSERT_TRUE(metadata_count_reader->init(&state).ok());
    auto metadata_count_request = std::make_shared<format::FileScanRequest>();
    format::FileScanRequestBuilder metadata_count_builder(metadata_count_request.get());
    ASSERT_TRUE(metadata_count_builder.add_non_predicate_column(format::LocalColumnId(0)).ok());
    metadata_count_request->count_star_placeholder_columns.push_back(format::LocalColumnId(0));
    ASSERT_TRUE(metadata_count_reader->open(metadata_count_request).ok());

    format::FileAggregateRequest aggregate_request;
    aggregate_request.agg_type = TPushAggOp::COUNT;
    format::FileAggregateResult aggregate_result;
    ASSERT_TRUE(
            metadata_count_reader->get_aggregate_result(aggregate_request, &aggregate_result).ok());
    EXPECT_EQ(aggregate_result.count, 2);

    // The marker is independent of aggregate eligibility. Simulate a position delete,
    // which disables metadata COUNT and requires the reader to produce surviving rows. Parquet
    // reads only the virtual row position, filters row 1, and fills a default placeholder for row 0
    // without validating or decoding either unsupported TIME_MILLIS value.
    auto count_star_reader = create_reader();
    ASSERT_TRUE(count_star_reader->init(&state).ok());
    auto count_star_request = std::make_shared<format::FileScanRequest>();
    format::FileScanRequestBuilder count_star_builder(count_star_request.get());
    ASSERT_TRUE(count_star_builder.add_non_predicate_column(format::LocalColumnId(0)).ok());
    ASSERT_TRUE(count_star_builder
                        .add_predicate_column(format::LocalColumnId(format::ROW_POSITION_COLUMN_ID))
                        .ok());
    count_star_request->count_star_placeholder_columns.push_back(format::LocalColumnId(0));

    const std::vector<int64_t> deleted_rows {1};
    auto delete_predicate = std::make_shared<format::DeletePredicate>(deleted_rows);
    const auto row_position = count_star_request->local_positions.at(
            format::LocalColumnId(format::ROW_POSITION_COLUMN_ID));
    delete_predicate->add_child(VSlotRef::create_shared(
            cast_set<int>(row_position.value()), cast_set<int>(row_position.value()), -1,
            std::make_shared<DataTypeInt64>(), format::ROW_POSITION_COLUMN_NAME));
    auto delete_context = VExprContext::create_shared(std::move(delete_predicate));
    ASSERT_TRUE(delete_context->prepare(&state, RowDescriptor()).ok());
    ASSERT_TRUE(delete_context->open(&state).ok());
    count_star_request->delete_conjuncts.push_back(std::move(delete_context));
    ASSERT_TRUE(count_star_reader->open(count_star_request).ok());

    std::vector<format::ColumnDefinition> file_schema;
    ASSERT_TRUE(count_star_reader->get_schema(&file_schema).ok());
    file_schema.push_back(format::row_position_column_definition());
    auto block = build_file_block(file_schema);
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(count_star_reader->get_block(&block, &rows, &eof).ok());
    EXPECT_EQ(rows, 1);
    EXPECT_EQ(block.get_by_position(0).column->size(), 1);
    const auto& positions =
            assert_cast<const ColumnInt64&>(*block.get_by_position(row_position.value()).column);
    ASSERT_EQ(positions.size(), 1);
    EXPECT_EQ(positions.get_element(0), 0);
}

TEST_F(ParquetScanTest, AggregateMinMaxRejectsInexactBinaryStatistics) {
    write_binary_minmax_parquet_file(_file_path);
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());
    open_all_row_groups(reader.get());

    for (int32_t column_id = 0; column_id < 2; ++column_id) {
        format::FileAggregateRequest request;
        request.agg_type = TPushAggOp::MINMAX;
        request.columns.push_back({.projection = field_projection(column_id)});
        format::FileAggregateResult result;
        const auto status = reader->get_aggregate_result(request, &result);
        EXPECT_TRUE(status.is<ErrorCode::NOT_IMPLEMENTED_ERROR>()) << status;
    }
}

TEST_F(ParquetScanTest, AggregateRespectsStatisticsPrunedRowGroups) {
    write_int_pair_parquet_file(_file_path);
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    auto request = std::make_shared<format::FileScanRequest>();
    request->local_positions.emplace(format::LocalColumnId(0), format::LocalIndex(0));
    request->conjuncts.push_back(create_int32_zonemap_conjunct(0, Int32ZoneMapExpr::Op::GE, 5));
    ASSERT_TRUE(reader->open(request).ok());

    format::FileAggregateRequest aggregate_request;
    aggregate_request.agg_type = TPushAggOp::MINMAX;
    aggregate_request.columns.push_back({.projection = field_projection(0)});
    format::FileAggregateResult result;
    ASSERT_TRUE(reader->get_aggregate_result(aggregate_request, &result).ok());
    EXPECT_EQ(result.count, 2);
    ASSERT_EQ(result.columns.size(), 1);
    EXPECT_EQ(result.columns[0].min_value.get<TYPE_INT>(), 5);
    EXPECT_EQ(result.columns[0].max_value.get<TYPE_INT>(), 6);
}

TEST_F(ParquetScanTest, AggregateCountKeepsRowGroupRowsAfterPageIndexPruning) {
    write_page_index_parquet_file(_file_path);
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    auto request = std::make_shared<format::FileScanRequest>();
    request->local_positions.emplace(format::LocalColumnId(0), format::LocalIndex(0));
    request->conjuncts.push_back(create_int32_zonemap_conjunct(0, Int32ZoneMapExpr::Op::GT, 63));
    ASSERT_TRUE(reader->open(request).ok());

    format::FileAggregateRequest aggregate_request;
    aggregate_request.agg_type = TPushAggOp::COUNT;
    format::FileAggregateResult result;
    ASSERT_TRUE(reader->get_aggregate_result(aggregate_request, &result).ok());
    EXPECT_EQ(result.count, 128);
}

TEST_F(ParquetScanTest, AggregateMinMaxSupportsNestedSingleLeafProjection) {
    write_struct_parquet_file(_file_path);
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());
    open_all_row_groups(reader.get());

    format::LocalColumnIndex nested_id = format::LocalColumnIndex::partial_local(0);
    nested_id.children.push_back(field_projection(0));
    format::FileAggregateRequest aggregate_request;
    aggregate_request.agg_type = TPushAggOp::MINMAX;
    aggregate_request.columns.push_back({.projection = nested_id});
    format::FileAggregateResult result;
    ASSERT_TRUE(reader->get_aggregate_result(aggregate_request, &result).ok());
    EXPECT_EQ(result.count, 4);
    ASSERT_EQ(result.columns.size(), 1);
    EXPECT_EQ(result.columns[0].min_value.get<TYPE_INT>(), 1);
    EXPECT_EQ(result.columns[0].max_value.get<TYPE_INT>(), 11);
}

TEST_F(ParquetScanTest, AggregateCountOnStructRecordsSelectedRowsRead) {
    write_struct_parquet_file(_file_path);
    io::FileReaderStats file_reader_stats;
    auto io_ctx = std::make_shared<io::IOContext>();
    io_ctx->file_reader_stats = &file_reader_stats;
    auto reader = create_reader(0, -1, nullptr, std::nullopt, io_ctx);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());
    open_all_row_groups(reader.get());

    format::FileAggregateRequest aggregate_request;
    aggregate_request.agg_type = TPushAggOp::COUNT;
    aggregate_request.columns.push_back({.projection = field_projection(0)});
    format::FileAggregateResult result;
    ASSERT_TRUE(reader->get_aggregate_result(aggregate_request, &result).ok());
    EXPECT_EQ(result.count, 4);
    EXPECT_EQ(file_reader_stats.read_rows, 4);
}

TEST_F(ParquetScanTest, AggregateCountOnStructReturnsEndOfFileWhenStopped) {
    write_struct_parquet_file(_file_path);
    io::FileReaderStats file_reader_stats;
    auto io_ctx = std::make_shared<io::IOContext>();
    io_ctx->file_reader_stats = &file_reader_stats;
    auto reader = create_reader(0, -1, nullptr, std::nullopt, io_ctx);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());
    open_all_row_groups(reader.get());
    io_ctx->should_stop = true;

    format::FileAggregateRequest aggregate_request;
    aggregate_request.agg_type = TPushAggOp::COUNT;
    aggregate_request.columns.push_back({.projection = field_projection(0)});
    format::FileAggregateResult result;
    const auto status = reader->get_aggregate_result(aggregate_request, &result);
    EXPECT_TRUE(status.is<ErrorCode::END_OF_FILE>()) << status;
    EXPECT_EQ(file_reader_stats.read_rows, 0);
}

TEST_F(ParquetScanTest, AggregateRejectsRepeatedMissingStatisticsAndInvalidRequests) {
    write_list_parquet_file(_file_path);
    auto repeated_reader = create_reader();
    RuntimeState repeated_state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(repeated_reader->init(&repeated_state).ok());
    open_all_row_groups(repeated_reader.get());

    format::FileAggregateRequest repeated_request;
    repeated_request.agg_type = TPushAggOp::MINMAX;
    repeated_request.columns.push_back({.projection = field_projection(0)});
    format::FileAggregateResult repeated_result;
    EXPECT_FALSE(repeated_reader->get_aggregate_result(repeated_request, &repeated_result).ok());

    write_int_pair_parquet_file(_file_path, 2, false);
    auto no_stats_reader = create_reader();
    RuntimeState no_stats_state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(no_stats_reader->init(&no_stats_state).ok());
    open_all_row_groups(no_stats_reader.get());
    format::FileAggregateRequest no_stats_request;
    no_stats_request.agg_type = TPushAggOp::MINMAX;
    no_stats_request.columns.push_back({.projection = field_projection(0)});
    format::FileAggregateResult no_stats_result;
    EXPECT_FALSE(no_stats_reader->get_aggregate_result(no_stats_request, &no_stats_result).ok());

    format::FileAggregateRequest invalid_type_request;
    invalid_type_request.agg_type = TPushAggOp::MIX;
    format::FileAggregateResult invalid_type_result;
    EXPECT_FALSE(
            no_stats_reader->get_aggregate_result(invalid_type_request, &invalid_type_result).ok());

    format::FileAggregateRequest invalid_column_request;
    invalid_column_request.agg_type = TPushAggOp::MINMAX;
    invalid_column_request.columns.push_back({.projection = field_projection(100)});
    format::FileAggregateResult invalid_column_result;
    EXPECT_FALSE(
            no_stats_reader->get_aggregate_result(invalid_column_request, &invalid_column_result)
                    .ok());
}

TEST_F(ParquetScanTest, GlobalRowIdUsesFileLocalPositionForScanRange) {
    write_int_pair_parquet_file(_file_path, 2);
    auto parquet_file_reader = ::parquet::ParquetFileReader::OpenFile(_file_path, false);
    ASSERT_EQ(parquet_file_reader->metadata()->num_row_groups(), 3);
    const auto [range_start_offset, range_size] = row_group_mid_range(_file_path, 1);
    format::GlobalRowIdContext context {.version = 7, .backend_id = 123456789, .file_id = 42};
    auto reader = create_reader(range_start_offset, range_size, nullptr, context);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 3);
    auto request = std::make_shared<format::FileScanRequest>();
    request->non_predicate_columns = {field_projection(0),
                                      field_projection(format::GLOBAL_ROWID_COLUMN_ID)};
    use_schema_order_positions(request.get(), schema);
    ASSERT_TRUE(reader->open(request).ok());

    std::vector<int32_t> ids;
    std::vector<uint32_t> row_ids;
    bool eof = false;
    while (!eof) {
        Block block = build_file_block(schema);
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (rows == 0) {
            continue;
        }
        const auto& id_column = int32_data_column(*block.get_by_position(0).column);
        const auto& rowid_column = string_data_column(*block.get_by_position(2).column);
        for (size_t row = 0; row < rows; ++row) {
            ids.push_back(id_column.get_element(row));
            const auto location = decode_rowid(rowid_column, row);
            EXPECT_EQ(location.version, context.version);
            EXPECT_EQ(location.backend_id, context.backend_id);
            EXPECT_EQ(location.file_id, context.file_id);
            row_ids.push_back(location.row_id);
        }
    }

    EXPECT_EQ(ids, std::vector<int32_t>({3, 4}));
    EXPECT_EQ(row_ids, std::vector<uint32_t>({2, 3}));
}

TEST_F(ParquetScanTest, PredicateOnlyGlobalRowIdKeepsSignedFileLocalId) {
    write_int_pair_parquet_file(_file_path, 6, false);
    format::GlobalRowIdContext context {.version = 7, .backend_id = 123456789, .file_id = 42};
    auto reader = create_reader(0, -1, nullptr, context);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<format::FileScanRequest>();
    format::FileScanRequestBuilder request_builder(request.get());
    const auto global_rowid = format::LocalColumnId(format::GLOBAL_ROWID_COLUMN_ID);
    ASSERT_TRUE(request_builder.add_non_predicate_column(format::LocalColumnId(0)).ok());
    ASSERT_TRUE(request_builder.add_non_predicate_column(format::LocalColumnId(1)).ok());
    ASSERT_TRUE(request_builder.add_predicate_column(global_rowid).ok());
    request->predicate_only_columns.push_back(global_rowid);
    const auto global_rowid_position = request->local_positions.at(global_rowid);
    request->conjuncts.push_back(create_always_true_single_column_conjunct(
            cast_set<int>(global_rowid_position.value())));
    ASSERT_TRUE(reader->open(request).ok());

    Block block = build_file_block(schema);
    size_t rows = 0;
    bool eof = false;
    const auto status = reader->get_block(&block, &rows, &eof);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(rows, 6);
    EXPECT_EQ(int32_data_column(*block.get_by_position(0).column).get_data(),
              (ColumnInt32::Container {1, 2, 3, 4, 5, 6}));
}

TEST_F(ParquetScanTest, EmptyScanPlanReturnsEofWithoutReadingColumns) {
    write_int_pair_parquet_file(_file_path, 2);
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<format::FileScanRequest>();
    request->local_positions.emplace(format::LocalColumnId(0), format::LocalIndex(0));
    request->conjuncts.push_back(create_int32_zonemap_conjunct(0, Int32ZoneMapExpr::Op::GE, 100));
    ASSERT_TRUE(reader->open(request).ok());

    Block block = build_file_block(schema);
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_EQ(rows, 0);
    EXPECT_TRUE(eof);
}

TEST_F(ParquetScanTest, NoRequestedColumnsReturnsRowsOnlyAcrossRowGroups) {
    write_int_pair_parquet_file(_file_path, 2);
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    auto request = std::make_shared<format::FileScanRequest>();
    ASSERT_TRUE(reader->open(request).ok());

    size_t total_rows = 0;
    bool eof = false;
    while (!eof) {
        Block block;
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        EXPECT_EQ(block.columns(), 0);
        total_rows += rows;
    }
    EXPECT_EQ(total_rows, 6);
}

TEST_F(ParquetScanTest, PredicateColumnsFilterRoundByRound) {
    write_int_pair_parquet_file(_file_path, 6, false);
    RuntimeProfile profile("profile");
    auto reader = create_reader(0, -1, &profile);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<format::FileScanRequest>();
    format::FileScanRequestBuilder request_builder(request.get());
    ASSERT_TRUE(request_builder.add_predicate_column(format::LocalColumnId(0)).ok());
    ASSERT_TRUE(request_builder.add_predicate_column(format::LocalColumnId(1)).ok());
    request->conjuncts.push_back(create_int32_zonemap_conjunct(0, Int32ZoneMapExpr::Op::GT, 2));
    request->conjuncts.push_back(create_int32_zonemap_conjunct(1, Int32ZoneMapExpr::Op::LT, 50));
    ASSERT_TRUE(reader->open(request).ok());

    std::vector<int32_t> ids;
    std::vector<int32_t> scores;
    bool eof = false;
    while (!eof) {
        Block block = build_file_block(schema);
        size_t rows = 0;
        const auto status = reader->get_block(&block, &rows, &eof);
        ASSERT_TRUE(status.ok()) << status;
        if (rows == 0) {
            continue;
        }
        const auto& id_column = int32_data_column(*block.get_by_position(0).column);
        const auto& score_column = int32_data_column(*block.get_by_position(1).column);
        ASSERT_EQ(id_column.size(), rows);
        ASSERT_EQ(score_column.size(), rows);
        for (size_t row = 0; row < rows; ++row) {
            ids.push_back(id_column.get_element(row));
            scores.push_back(score_column.get_element(row));
        }
    }

    EXPECT_EQ(ids, std::vector<int32_t>({3, 4}));
    EXPECT_EQ(scores, std::vector<int32_t>({30, 40}));
    EXPECT_EQ(counter_value(profile, "RawRowsRead"), 6);
    EXPECT_EQ(counter_value(profile, "SelectedRows"), 2);
    EXPECT_EQ(counter_value(profile, "RowsFilteredByConjunct"), 4);
    EXPECT_EQ(counter_value(profile, "PredicateCompactionCount"), 1);
    EXPECT_GT(counter_value(profile, "PredicateCompactionBytes"), 0);
    ASSERT_NE(profile.get_counter("PredicateCompactionTime"), nullptr);
    EXPECT_EQ(counter_value(profile, "ReaderReadRows"), 10);
    EXPECT_EQ(counter_value(profile, "ReaderSelectRows"), 4);
    EXPECT_EQ(counter_value(profile, "ReaderSkipRows"), 2);
}

TEST_F(ParquetScanTest, PredicateColumnsSkipUnreadColumnsWhenFirstPredicateFiltersAll) {
    write_int_pair_parquet_file(_file_path, 6, false);
    RuntimeProfile profile("profile");
    auto reader = create_reader(0, -1, &profile);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<format::FileScanRequest>();
    format::FileScanRequestBuilder request_builder(request.get());
    ASSERT_TRUE(request_builder.add_predicate_column(format::LocalColumnId(0)).ok());
    ASSERT_TRUE(request_builder.add_predicate_column(format::LocalColumnId(1)).ok());
    request->conjuncts.push_back(create_int32_zonemap_conjunct(0, Int32ZoneMapExpr::Op::GT, 100));
    request->conjuncts.push_back(create_int32_zonemap_conjunct(1, Int32ZoneMapExpr::Op::LT, 50));
    ASSERT_TRUE(reader->open(request).ok());

    size_t total_rows = 0;
    bool eof = false;
    while (!eof) {
        Block block = build_file_block(schema);
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        total_rows += rows;
    }

    EXPECT_EQ(total_rows, 0);
    EXPECT_EQ(counter_value(profile, "RawRowsRead"), 6);
    EXPECT_EQ(counter_value(profile, "SelectedRows"), 0);
    EXPECT_EQ(counter_value(profile, "RowsFilteredByConjunct"), 6);
    EXPECT_EQ(counter_value(profile, "EmptySelectionBatches"), 1);
    EXPECT_EQ(counter_value(profile, "ReaderReadRows"), 6);
    EXPECT_EQ(counter_value(profile, "ReaderSelectRows"), 0);
    EXPECT_EQ(counter_value(profile, "ReaderSkipRows"), 6);
}

TEST_F(ParquetScanTest, PredicateOnlyColumnDropsPayloadAfterFiltering) {
    write_int_pair_parquet_file(_file_path, 6, false);
    RuntimeProfile profile("profile");
    auto reader = create_reader(0, -1, &profile);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<format::FileScanRequest>();
    format::FileScanRequestBuilder request_builder(request.get());
    ASSERT_TRUE(request_builder.add_predicate_column(format::LocalColumnId(0)).ok());
    ASSERT_TRUE(request_builder.add_non_predicate_column(format::LocalColumnId(1)).ok());
    request->predicate_only_columns.push_back(format::LocalColumnId(0));
    request->conjuncts.push_back(create_int32_zonemap_conjunct(0, Int32ZoneMapExpr::Op::GT, 2));
    ASSERT_TRUE(reader->open(request).ok());

    Block block = build_file_block(schema);
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    ASSERT_EQ(rows, 4);
    EXPECT_EQ(int32_data_column(*block.get_by_position(1).column).get_data(),
              (ColumnInt32::Container {30, 40, 50, 60}));
    EXPECT_EQ(block.get_by_position(0).column->size(), rows);
    EXPECT_EQ(counter_value(profile, "PredicateCompactionCount"), 0);
    EXPECT_EQ(counter_value(profile, "PredicateCompactionBytes"), 0);
}

TEST_F(ParquetScanTest, PredicateOnlyPlainComparisonUsesPhysicalDirectPath) {
    write_int_pair_parquet_file(_file_path, 6, false);
    RuntimeProfile profile("profile");
    auto reader = create_reader(0, -1, &profile);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<format::FileScanRequest>();
    format::FileScanRequestBuilder request_builder(request.get());
    ASSERT_TRUE(request_builder.add_predicate_column(format::LocalColumnId(0)).ok());
    ASSERT_TRUE(request_builder.add_non_predicate_column(format::LocalColumnId(1)).ok());
    request->predicate_only_columns.push_back(format::LocalColumnId(0));
    request->conjuncts.push_back(create_int32_function_conjunct(0, "gt", TExprOpcode::GT, 2));
    ASSERT_TRUE(reader->open(request).ok());

    Block block = build_file_block(schema);
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    ASSERT_EQ(rows, 4);
    EXPECT_EQ(int32_data_column(*block.get_by_position(1).column).get_data(),
              (ColumnInt32::Container {30, 40, 50, 60}));
    EXPECT_EQ(block.get_by_position(0).column->size(), rows);
    EXPECT_EQ(counter_value(profile, "PlainPredicateDirectBatches"), 1);
    EXPECT_EQ(counter_value(profile, "PlainPredicateDirectRows"), 6);
    EXPECT_EQ(counter_value(profile, "PredicateCompactionCount"), 0);
    EXPECT_EQ(counter_value(profile, "PredicateCompactionBytes"), 0);
}

TEST_F(ParquetScanTest, ProjectedPlainComparisonUsesPhysicalFilterAndProjectPath) {
    write_int_pair_parquet_file(_file_path, 6, false);
    RuntimeProfile profile("profile");
    auto reader = create_reader(0, -1, &profile);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<format::FileScanRequest>();
    format::FileScanRequestBuilder request_builder(request.get());
    ASSERT_TRUE(request_builder.add_predicate_column(format::LocalColumnId(0)).ok());
    ASSERT_TRUE(request_builder.add_non_predicate_column(format::LocalColumnId(1)).ok());
    request->conjuncts.push_back(create_int32_direct_greater_conjunct(0, 2));
    ASSERT_TRUE(reader->open(request).ok());

    Block block = build_file_block(schema);
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    ASSERT_EQ(rows, 4);
    EXPECT_EQ(int32_data_column(*block.get_by_position(0).column).get_data(),
              (ColumnInt32::Container {3, 4, 5, 6}));
    EXPECT_EQ(int32_data_column(*block.get_by_position(1).column).get_data(),
              (ColumnInt32::Container {30, 40, 50, 60}));
    EXPECT_EQ(counter_value(profile, "PlainPredicateDirectBatches"), 1);
    EXPECT_EQ(counter_value(profile, "PlainPredicateDirectRows"), 6);
    EXPECT_EQ(counter_value(profile, "PredicateCompactionCount"), 0);
    EXPECT_EQ(counter_value(profile, "PredicateCompactionBytes"), 0);
}

TEST_F(ParquetScanTest, PredicateOnlyUint32FallsBackBeforeRawPlainDecode) {
    write_uint32_pair_parquet_file(_file_path);
    RuntimeProfile profile("profile");
    auto reader = create_reader(0, -1, &profile);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);
    EXPECT_EQ(remove_nullable(schema[0].type)->get_primitive_type(), TYPE_BIGINT);
    auto request = std::make_shared<format::FileScanRequest>();
    format::FileScanRequestBuilder request_builder(request.get());
    ASSERT_TRUE(request_builder.add_predicate_column(format::LocalColumnId(0)).ok());
    ASSERT_TRUE(request_builder.add_non_predicate_column(format::LocalColumnId(1)).ok());
    request->predicate_only_columns.push_back(format::LocalColumnId(0));
    request->conjuncts.push_back(
            create_int64_direct_greater_conjunct(0, std::numeric_limits<int32_t>::max()));
    ASSERT_TRUE(reader->open(request).ok());

    Block block = build_file_block(schema);
    size_t rows = 0;
    bool eof = false;
    const auto status = reader->get_block(&block, &rows, &eof);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(rows, 2);
    EXPECT_EQ(int32_data_column(*block.get_by_position(1).column).get_data(),
              (ColumnInt32::Container {20, 30}));
    EXPECT_EQ(counter_value(profile, "PlainPredicateDirectBatches"), 0);
}

TEST_F(ParquetScanTest, PlainPredicateReportsFooterEncodingMismatchAfterProgress) {
    write_misdeclared_two_page_parquet_file(_file_path);
    RuntimeProfile profile("profile");
    auto reader = create_reader(0, -1, &profile);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<format::FileScanRequest>();
    format::FileScanRequestBuilder request_builder(request.get());
    ASSERT_TRUE(request_builder.add_predicate_column(format::LocalColumnId(0)).ok());
    request->predicate_only_columns.push_back(format::LocalColumnId(0));
    request->conjuncts.push_back(create_int32_function_conjunct(0, "gt", TExprOpcode::GT, 0));
    ASSERT_TRUE(reader->open(request).ok());

    Block block = build_file_block(schema);
    size_t rows = 0;
    bool eof = false;
    const auto status = reader->get_block(&block, &rows, &eof);
    EXPECT_TRUE(status.is<ErrorCode::CORRUPTION>()) << status;
    EXPECT_NE(status.to_string().find("encoding"), std::string::npos) << status;
}

TEST_F(ParquetScanTest, PlainDirectPathKeepsPayloadForMultiColumnResidual) {
    write_int_pair_parquet_file(_file_path, 6, false);
    RuntimeProfile profile("profile");
    auto reader = create_reader(0, -1, &profile);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<format::FileScanRequest>();
    format::FileScanRequestBuilder request_builder(request.get());
    ASSERT_TRUE(request_builder.add_predicate_column(format::LocalColumnId(0)).ok());
    ASSERT_TRUE(request_builder.add_predicate_column(format::LocalColumnId(1)).ok());
    request->predicate_only_columns.push_back(format::LocalColumnId(0));
    request->conjuncts.push_back(create_int32_direct_greater_conjunct(0, 2));
    request->conjuncts.push_back(create_int32_pair_sum_conjunct(0, 1, 42));
    ASSERT_TRUE(reader->open(request).ok());

    Block block = build_file_block(schema);
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    ASSERT_EQ(rows, 1);
    EXPECT_EQ(int32_data_column(*block.get_by_position(1).column).get_data(),
              (ColumnInt32::Container {30}));
    EXPECT_EQ(block.get_by_position(0).column->size(), rows);
    // A residual expression still consumes id, so raw filtering must leave its payload available.
    EXPECT_EQ(counter_value(profile, "PlainPredicateDirectBatches"), 0);
}

// Scenario: every physical batch in every row group is rejected. Predicate readers reach each row
// group boundary, while the lazy score reader remains at row 0. The boundary reset must discard that
// reader and its pending lag instead of issuing SkipRecords for values that can never be observed.
TEST_F(ParquetScanTest, FullyFilteredRowGroupsDropPendingLazyReaders) {
    write_int_pair_parquet_file(_file_path, 2, false);
    RuntimeProfile profile("profile");
    auto reader = create_reader(0, -1, &profile);
    reader->set_batch_size(1);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<format::FileScanRequest>();
    format::FileScanRequestBuilder request_builder(request.get());
    ASSERT_TRUE(request_builder.add_predicate_column(format::LocalColumnId(0)).ok());
    ASSERT_TRUE(request_builder.add_non_predicate_column(format::LocalColumnId(1)).ok());
    request->conjuncts.push_back(create_int32_zonemap_conjunct(0, Int32ZoneMapExpr::Op::GT, 100));
    ASSERT_TRUE(reader->open(request).ok());

    size_t total_rows = 0;
    bool eof = false;
    while (!eof) {
        Block block = build_file_block(schema);
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        total_rows += rows;
    }

    EXPECT_EQ(total_rows, 0);
    // The first one-row probe grows after rejection and remains grown across consecutive empty
    // row groups, so later two-row groups are consumed in one predicate batch each.
    EXPECT_EQ(counter_value(profile, "EmptySelectionBatches"), 4);
    EXPECT_EQ(counter_value(profile, "ReaderSkipRows"), 0);
    ASSERT_NE(profile.get_counter("LevelOnlySkipTime"), nullptr);
    EXPECT_EQ(profile.get_counter("LevelOnlySkipTime")->value(), 0);
}

// Scenario: row group 0 is fully filtered and leaves two pending lazy rows. Reset must discard that
// lag before row group 1 creates fresh readers at its own row 0; otherwise score 30 would be skipped
// as if it belonged to the rejected prefix from the previous row group.
TEST_F(ParquetScanTest, PendingLazySkipDoesNotCrossRowGroupReset) {
    write_int_pair_parquet_file(_file_path, 2, false);
    RuntimeProfile profile("profile");
    auto reader = create_reader(0, -1, &profile);
    reader->set_batch_size(1);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<format::FileScanRequest>();
    format::FileScanRequestBuilder request_builder(request.get());
    ASSERT_TRUE(request_builder.add_predicate_column(format::LocalColumnId(0)).ok());
    ASSERT_TRUE(request_builder.add_non_predicate_column(format::LocalColumnId(1)).ok());
    request->conjuncts.push_back(create_int32_zonemap_conjunct(0, Int32ZoneMapExpr::Op::GT, 2));
    ASSERT_TRUE(reader->open(request).ok());

    std::vector<int32_t> scores;
    bool eof = false;
    while (!eof) {
        Block block = build_file_block(schema);
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (rows == 0) {
            continue;
        }
        const auto& score_column = int32_data_column(*block.get_by_position(1).column);
        for (size_t row = 0; row < rows; ++row) {
            scores.push_back(score_column.get_element(row));
        }
    }

    EXPECT_EQ(scores, std::vector<int32_t>({30, 40, 50, 60}));
    EXPECT_EQ(counter_value(profile, "ReaderSkipRows"), 0);
}

TEST_F(ParquetScanTest, LongFilteredPrefixSkipsMultipleLazyColumnsInBoundedChunks) {
    constexpr size_t ROWS = 70000;
    write_long_prefix_parquet_file(_file_path, ROWS);
    RuntimeProfile profile("profile");
    auto reader = create_reader(0, -1, &profile);
    reader->set_batch_size(32);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<format::FileScanRequest>();
    format::FileScanRequestBuilder request_builder(request.get());
    ASSERT_TRUE(request_builder.add_predicate_column(format::LocalColumnId(0)).ok());
    ASSERT_TRUE(request_builder.add_non_predicate_column(format::LocalColumnId(1)).ok());
    ASSERT_TRUE(request_builder.add_non_predicate_column(format::LocalColumnId(2)).ok());
    request->conjuncts.push_back(
            create_int32_zonemap_conjunct(0, Int32ZoneMapExpr::Op::GE, ROWS - 1));
    ASSERT_TRUE(reader->open(request).ok());

    Block block = build_file_block(schema);
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    ASSERT_EQ(rows, 1);
    EXPECT_EQ(int32_data_column(*block.get_by_position(0).column).get_element(0), ROWS - 1);
    EXPECT_EQ(int32_data_column(*block.get_by_position(1).column).get_element(0), ROWS + 9);
    EXPECT_EQ(int32_data_column(*block.get_by_position(2).column).get_element(0), ROWS + 19);
    EXPECT_LT(counter_value(profile, "TotalBatches"), 32);
}

TEST_F(ParquetScanTest, EmptyPrefixNeverWidensReturnedBatchPastCallerCap) {
    constexpr size_t ROWS = 300;
    write_long_prefix_parquet_file(_file_path, ROWS);
    RuntimeProfile profile("profile");
    auto reader = create_reader(0, -1, &profile);
    reader->set_batch_size(32);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<format::FileScanRequest>();
    format::FileScanRequestBuilder request_builder(request.get());
    ASSERT_TRUE(request_builder.add_predicate_column(format::LocalColumnId(0)).ok());
    ASSERT_TRUE(request_builder.add_non_predicate_column(format::LocalColumnId(1)).ok());
    ASSERT_TRUE(request_builder.add_non_predicate_column(format::LocalColumnId(2)).ok());
    request->conjuncts.push_back(create_int32_zonemap_conjunct(0, Int32ZoneMapExpr::Op::GE, 32));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    int32_t expected_id = 32;
    size_t total_rows = 0;
    while (!eof) {
        Block block = build_file_block(schema);
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (rows == 0) {
            continue;
        }
        // The first 32-row probe is empty. Its feedback may accelerate later predicate work, but
        // each pending-output slice must preserve the caller's memory-derived row cap.
        ASSERT_LE(rows, 32);
        const auto& ids = int32_data_column(*block.get_by_position(0).column);
        const auto& first_lazy = int32_data_column(*block.get_by_position(1).column);
        const auto& second_lazy = int32_data_column(*block.get_by_position(2).column);
        ASSERT_EQ(first_lazy.size(), rows);
        ASSERT_EQ(second_lazy.size(), rows);
        for (size_t row = 0; row < rows; ++row, ++expected_id) {
            EXPECT_EQ(ids.get_element(row), expected_id);
            EXPECT_EQ(first_lazy.get_element(row), expected_id + 10);
            EXPECT_EQ(second_lazy.get_element(row), expected_id + 20);
        }
        total_rows += rows;
    }
    EXPECT_EQ(total_rows, ROWS - 32);
    EXPECT_EQ(expected_id, ROWS);
}

// Scenario: a nested lazy column stays behind while id=1 is rejected. Flushing skip(1) must consume
// the complete repetition/definition-level span for the first list, then materialize the remaining
// two parent rows without corrupting their child boundaries.
TEST_F(ParquetScanTest, PendingLazySkipPreservesNestedRowBoundaries) {
    write_int_list_parquet_file(_file_path);
    RuntimeProfile profile("profile");
    auto reader = create_reader(0, -1, &profile);
    reader->set_batch_size(1);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<format::FileScanRequest>();
    format::FileScanRequestBuilder request_builder(request.get());
    ASSERT_TRUE(request_builder.add_predicate_column(format::LocalColumnId(0)).ok());
    ASSERT_TRUE(request_builder.add_non_predicate_column(format::LocalColumnId(1)).ok());
    request->conjuncts.push_back(create_int32_zonemap_conjunct(0, Int32ZoneMapExpr::Op::GT, 1));
    ASSERT_TRUE(reader->open(request).ok());

    std::vector<size_t> list_lengths;
    std::vector<int32_t> list_values;
    bool eof = false;
    while (!eof) {
        Block block = build_file_block(schema);
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        const IColumn* list_column = block.get_by_position(1).column.get();
        if (const auto* nullable = check_and_get_column<ColumnNullable>(list_column)) {
            list_column = &nullable->get_nested_column();
        }
        const auto& array_column = assert_cast<const ColumnArray&>(*list_column);
        const auto& value_column = int32_data_column(array_column.get_data());
        for (size_t row = 0; row < rows; ++row) {
            const size_t list_start = array_column.offset_at(row);
            const size_t list_length = array_column.size_at(row);
            list_lengths.push_back(list_length);
            for (size_t element = 0; element < list_length; ++element) {
                list_values.push_back(value_column.get_element(list_start + element));
            }
        }
    }

    EXPECT_EQ(list_lengths, std::vector<size_t>({1, 0}));
    EXPECT_EQ(list_values, std::vector<int32_t>({3}));
    EXPECT_EQ(counter_value(profile, "ReaderSkipRows"), 1);
}

TEST_F(ParquetScanTest, MultiColumnPredicateWaitsForAllPredicateColumns) {
    write_int_pair_parquet_file(_file_path, 6, false);
    RuntimeProfile profile("profile");
    auto reader = create_reader(0, -1, &profile);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<format::FileScanRequest>();
    format::FileScanRequestBuilder request_builder(request.get());
    ASSERT_TRUE(request_builder.add_predicate_column(format::LocalColumnId(0)).ok());
    ASSERT_TRUE(request_builder.add_predicate_column(format::LocalColumnId(1)).ok());
    request->conjuncts.push_back(create_int32_pair_sum_conjunct(0, 1, 45));
    ASSERT_TRUE(reader->open(request).ok());

    std::vector<int32_t> ids;
    std::vector<int32_t> scores;
    bool eof = false;
    while (!eof) {
        Block block = build_file_block(schema);
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (rows == 0) {
            continue;
        }
        const auto& id_column = int32_data_column(*block.get_by_position(0).column);
        const auto& score_column = int32_data_column(*block.get_by_position(1).column);
        ASSERT_EQ(id_column.size(), rows);
        ASSERT_EQ(score_column.size(), rows);
        for (size_t row = 0; row < rows; ++row) {
            ids.push_back(id_column.get_element(row));
            scores.push_back(score_column.get_element(row));
        }
    }

    EXPECT_EQ(ids, std::vector<int32_t>({1, 2, 3, 4}));
    EXPECT_EQ(scores, std::vector<int32_t>({10, 20, 30, 40}));
    EXPECT_EQ(counter_value(profile, "RawRowsRead"), 6);
    EXPECT_EQ(counter_value(profile, "SelectedRows"), 4);
    EXPECT_EQ(counter_value(profile, "RowsFilteredByConjunct"), 2);
    EXPECT_EQ(counter_value(profile, "ReaderReadRows"), 12);
    EXPECT_EQ(counter_value(profile, "ReaderSelectRows"), 0);
}

TEST_F(ParquetScanTest, ProfileCountersReflectPageIndexAndRangeGapPruning) {
    write_page_index_parquet_file(_file_path);
    RuntimeProfile profile("profile");
    auto reader = create_reader(0, -1, &profile);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<format::FileScanRequest>();
    format::FileScanRequestBuilder request_builder(request.get());
    ASSERT_TRUE(request_builder.add_predicate_column(format::LocalColumnId(0)).ok());
    request->conjuncts.push_back(create_int32_zonemap_conjunct(0, Int32ZoneMapExpr::Op::GT, 63));
    ASSERT_TRUE(reader->open(request).ok());

    size_t total_rows = 0;
    bool eof = false;
    while (!eof) {
        Block block = build_file_block(schema);
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        total_rows += rows;
    }

    EXPECT_EQ(total_rows, 64);
    ASSERT_NE(profile.get_counter("RowGroupsTotalNum"), nullptr);
    ASSERT_NE(profile.get_counter("RowGroupsReadNum"), nullptr);
    ASSERT_NE(profile.get_counter("FilteredRowsByPage"), nullptr);
    ASSERT_NE(profile.get_counter("SelectedRowRanges"), nullptr);
    ASSERT_NE(profile.get_counter("PageIndexReadCalls"), nullptr);
    ASSERT_NE(profile.get_counter("RawRowsRead"), nullptr);
    ASSERT_NE(profile.get_counter("RangeGapSkippedRows"), nullptr);
    EXPECT_EQ(profile.get_counter("RowGroupsTotalNum")->value(), 1);
    EXPECT_EQ(profile.get_counter("RowGroupsReadNum")->value(), 1);
    EXPECT_GT(profile.get_counter("FilteredRowsByPage")->value(), 0);
    EXPECT_GT(profile.get_counter("SelectedRowRanges")->value(), 0);
    EXPECT_GT(profile.get_counter("PageIndexReadCalls")->value(), 0);
    EXPECT_EQ(profile.get_counter("RawRowsRead")->value(), 64);
    EXPECT_GT(profile.get_counter("RangeGapSkippedRows")->value(), 0);
}

TEST_F(ParquetScanTest, OpenDefersPageIndexProbeToCurrentRowGroup) {
    write_multi_row_group_page_index_parquet_file(_file_path);
    RuntimeProfile profile("lazy_page_index_profile");
    auto reader = create_reader(0, -1, &profile);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<format::FileScanRequest>();
    format::FileScanRequestBuilder request_builder(request.get());
    ASSERT_TRUE(request_builder.add_predicate_column(format::LocalColumnId(0)).ok());
    request->conjuncts.push_back(create_int32_zonemap_conjunct(0, Int32ZoneMapExpr::Op::GT, -1));
    ASSERT_TRUE(reader->open(request).ok());

    // LIMIT can stop after the first block, so open must not pay remote index I/O for later groups.
    EXPECT_EQ(counter_value(profile, "PageIndexReadCalls"), 0);
    Block block = build_file_block(schema);
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_EQ(rows, 128);
    EXPECT_EQ(counter_value(profile, "PageIndexReadCalls"), 1);
}

} // namespace
} // namespace doris
