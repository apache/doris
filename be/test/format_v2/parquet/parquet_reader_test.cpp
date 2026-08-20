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
#include <parquet/api/writer.h>
#include <parquet/arrow/writer.h>
#include <parquet/column_page.h>
#include <parquet/page_index.h>

#include <array>
#include <cstring>
#include <filesystem>
#include <fstream>
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
#include "core/column/column_array.h"
#include "core/column/column_decimal.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_variant_v2.h"
#include "core/data_type/primitive_type.h"
#include "core/field.h"
#include "core/string_buffer.hpp"
#include "exprs/vcompound_pred.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "exprs/vliteral.h"
#include "exprs/vslot_ref.h"
#include "format_v2/column_mapper.h"
#include "format_v2/expr/delete_predicate.h"
#include "format_v2/file_reader.h"
#include "format_v2/parquet/parquet_column_schema.h"
#include "format_v2/parquet/parquet_scan.h"
#include "format_v2/parquet/reader/column_reader.h"
#include "format_v2/schema_projection.h"
#include "format_v2/table_reader.h"
#include "gen_cpp/Types_types.h"
#include "io/io_common.h"
#include "runtime/runtime_state.h"
#include "storage/index/zone_map/zonemap_eval_context.h"
#include "storage/index/zone_map/zonemap_filter_result.h"
#include "storage/segment/condition_cache.h"
#include "storage/utils.h"
#include "util/coding.h"
#include "util/defer_op.h"
#include "util/thrift_util.h"

namespace doris {
namespace {

constexpr int64_t ROW_COUNT = 5;

void annotate_variant_schema(const std::string& file_path) {
    std::ifstream input(file_path, std::ios::binary | std::ios::ate);
    DORIS_CHECK(input.good());
    const auto input_size = static_cast<std::streamoff>(input.tellg());
    DORIS_CHECK(input_size >= static_cast<std::streamoff>(8));
    std::vector<uint8_t> file_bytes(cast_set<size_t>(input_size));
    input.seekg(0);
    input.read(reinterpret_cast<char*>(file_bytes.data()), cast_set<std::streamsize>(input_size));
    DORIS_CHECK(input.good());
    DORIS_CHECK(memcmp(file_bytes.data() + file_bytes.size() - 4, "PAR1", 4) == 0);

    const uint32_t footer_size = decode_fixed32_le(file_bytes.data() + file_bytes.size() - 8);
    DORIS_CHECK(footer_size <= file_bytes.size() - 8);
    const size_t footer_offset = file_bytes.size() - 8 - footer_size;
    uint32_t thrift_size = footer_size;
    tparquet::FileMetaData metadata;
    DORIS_CHECK(
            deserialize_thrift_msg(file_bytes.data() + footer_offset, &thrift_size, true, &metadata)
                    .ok());
    input.close();
    const auto schema_it = std::ranges::find_if(
            metadata.schema, [](const auto& element) { return element.name == "v"; });
    DORIS_CHECK(schema_it != metadata.schema.end());
    schema_it->__set_logicalType(tparquet::LogicalType());
    schema_it->logicalType.__set_VARIANT(tparquet::VariantType());
    schema_it->logicalType.VARIANT.__set_specification_version(1);

    file_bytes.resize(footer_offset);
    std::vector<uint8_t> footer;
    ThriftSerializer serializer(/*compact=*/true, 1024);
    DORIS_CHECK(serializer.serialize(&metadata, &footer).ok());
    file_bytes.insert(file_bytes.end(), footer.begin(), footer.end());
    std::array<uint8_t, sizeof(uint32_t)> encoded_footer_size {};
    encode_fixed32_le(encoded_footer_size.data(), cast_set<uint32_t>(footer.size()));
    file_bytes.insert(file_bytes.end(), encoded_footer_size.begin(), encoded_footer_size.end());
    file_bytes.insert(file_bytes.end(), {'P', 'A', 'R', '1'});

    std::ofstream output(file_path, std::ios::binary | std::ios::trunc);
    output.write(reinterpret_cast<const char*>(file_bytes.data()),
                 cast_set<std::streamsize>(file_bytes.size()));
    output.close();
    DORIS_CHECK(output.good());
}

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

class VariantPathMetadataExpr : public VExpr {
public:
    VariantPathMetadataExpr(std::string name, DataTypePtr type,
                            TExprNodeType::type node_type = TExprNodeType::FUNCTION_CALL)
            : VExpr(std::move(type), false), _name(std::move(name)) {
        set_node_type(node_type);
    }

    const std::string& expr_name() const override { return _name; }
    Status execute_column_impl(VExprContext*, const Block*, const Selector*, size_t,
                               ColumnPtr&) const override {
        return Status::InternalError("VariantPathMetadataExpr is not executable");
    }

private:
    std::string _name;
};

class VariantInt32PathGreaterThanExpr final : public VariantPathMetadataExpr {
public:
    VariantInt32PathGreaterThanExpr(int column_id, std::string key, int32_t value)
            : VariantPathMetadataExpr("gt", std::make_shared<DataTypeUInt8>(),
                                      TExprNodeType::BINARY_PRED),
              _column_id(column_id),
              _key(std::move(key)),
              _value(value) {}

    Status execute_column_impl(VExprContext*, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& nullable =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_column_id).column);
        const auto& variants = assert_cast<const ColumnVariantV2&>(nullable.get_nested_column());
        const std::array path {VariantShreddedPathSegment {
                .kind = VariantShreddedPathSegment::Kind::OBJECT_KEY, .key = StringRef(_key)}};
        const auto typed = variants.find_shredded_typed_value(path);
        if (!typed.has_value()) {
            return Status::InternalError("Expected the projected Variant typed leaf");
        }
        const auto& typed_nullable = assert_cast<const ColumnNullable&>(*typed->column);
        const auto& values =
                assert_cast<const ColumnInt32&>(typed_nullable.get_nested_column()).get_data();
        auto result = ColumnUInt8::create();
        auto& output = result->get_data();
        output.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            output[row] = !nullable.is_null_at(input_row) &&
                          !typed_nullable.is_null_at(input_row) && values[input_row] > _value;
        }
        result_column = std::move(result);
        return Status::OK();
    }

private:
    int _column_id;
    std::string _key;
    int32_t _value;
};

VExprContextSPtr create_variant_int32_path_greater_than_conjunct(int column_id, std::string key,
                                                                 int32_t value) {
    auto slot = VSlotRef::create_shared(0, column_id, -1,
                                        make_nullable(std::make_shared<DataTypeVariantV2>()), "v");
    auto key_literal = VLiteral::create_shared(std::make_shared<DataTypeString>(),
                                               Field::create_field<TYPE_STRING>(key));
    auto element_at = std::make_shared<VariantPathMetadataExpr>(
            "element_at", make_nullable(std::make_shared<DataTypeVariantV2>()));
    element_at->add_child(slot);
    element_at->add_child(key_literal);
    auto cast = std::make_shared<VariantPathMetadataExpr>(
            "CAST", make_nullable(std::make_shared<DataTypeInt32>()), TExprNodeType::CAST_EXPR);
    cast->add_child(element_at);
    auto literal = VLiteral::create_shared(std::make_shared<DataTypeInt32>(),
                                           Field::create_field<TYPE_INT>(value));
    auto gt = std::make_shared<VariantInt32PathGreaterThanExpr>(column_id, std::move(key), value);
    gt->add_child(cast);
    gt->add_child(literal);
    return VExprContext::create_shared(std::move(gt));
}

class StructInt32ChildGreaterThanExpr final : public VExpr {
public:
    StructInt32ChildGreaterThanExpr(int column_id, int32_t value)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _value(value) {}

    Status execute_column_impl(VExprContext*, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& nullable =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_column_id).column);
        const auto& structure = assert_cast<const ColumnStruct&>(nullable.get_nested_column());
        const auto& child = assert_cast<const ColumnNullable&>(structure.get_column(0));
        const auto& values = assert_cast<const ColumnInt32&>(child.get_nested_column()).get_data();
        auto result = ColumnUInt8::create();
        auto& output = result->get_data();
        output.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            output[row] = !nullable.is_null_at(input_row) && !child.is_null_at(input_row) &&
                          values[input_row] > _value;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

    void collect_slot_column_ids(std::set<int>& column_ids) const override {
        column_ids.insert(_column_id);
    }

private:
    int _column_id;
    int32_t _value;
    const std::string _expr_name = "StructInt32ChildGreaterThanExpr";
};

VExprContextSPtr create_struct_int32_child_greater_than_conjunct(int column_id, int32_t value) {
    auto context = VExprContext::create_shared(
            std::make_shared<StructInt32ChildGreaterThanExpr>(column_id, value));
    context->_prepared = true;
    context->_opened = true;
    return context;
}

class Int32DictionaryEqualsExpr final : public VExpr {
public:
    Int32DictionaryEqualsExpr(int column_id, int32_t value)
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
            result_data[row] = input.get_element(input_row) == _value;
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
        const auto expected = Field::create_field<TYPE_INT>(_value);
        return std::ranges::any_of(dictionary->values,
                                   [&](const Field& value) { return value == expected; })
                       ? ZoneMapFilterResult::kMayMatch
                       : ZoneMapFilterResult::kNoMatch;
    }

    void collect_slot_column_ids(std::set<int>& column_ids) const override {
        column_ids.insert(_column_id);
    }

private:
    const int _column_id;
    const int32_t _value;
    const std::string _expr_name = "Int32DictionaryEqualsExpr";
};

class DictionaryAcceptAllExpr final : public VExpr {
public:
    explicit DictionaryAcceptAllExpr(int column_id)
            : VExpr(std::make_shared<DataTypeUInt8>(), false), _column_id(column_id) {}

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        auto result = ColumnUInt8::create();
        result->get_data().resize_fill(count, 1);
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

    bool can_evaluate_dictionary_filter() const override { return true; }

    ZoneMapFilterResult evaluate_dictionary_filter(
            const DictionaryEvalContext& ctx) const override {
        return ctx.slot(_column_id) == nullptr ? ZoneMapFilterResult::kUnsupported
                                               : ZoneMapFilterResult::kMayMatch;
    }

    void collect_slot_column_ids(std::set<int>& column_ids) const override {
        column_ids.insert(_column_id);
    }

private:
    const int _column_id;
    const std::string _expr_name = "DictionaryAcceptAllExpr";
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

VExprContextSPtr create_int32_dictionary_equals_conjunct(int column_id, int32_t value) {
    auto ctx = VExprContext::create_shared(
            std::make_shared<Int32DictionaryEqualsExpr>(column_id, value));
    ctx->_prepared = true;
    ctx->_opened = true;
    return ctx;
}

VExprContextSPtr create_dictionary_accept_all_conjunct(int column_id) {
    auto ctx = VExprContext::create_shared(std::make_shared<DictionaryAcceptAllExpr>(column_id));
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

std::shared_ptr<arrow::Array> build_nullable_int32_array(
        const std::vector<std::optional<int32_t>>& values) {
    arrow::Int32Builder builder;
    for (const auto value : values) {
        EXPECT_TRUE(value.has_value() ? builder.Append(*value).ok() : builder.AppendNull().ok());
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

std::shared_ptr<arrow::Array> build_float_array(const std::vector<float>& values) {
    arrow::FloatBuilder builder;
    for (const auto value : values) {
        EXPECT_TRUE(builder.Append(value).ok());
    }
    return finish_array(&builder);
}

std::shared_ptr<arrow::Array> build_double_array(const std::vector<double>& values) {
    arrow::DoubleBuilder builder;
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

std::shared_ptr<arrow::Array> build_binary_array(const std::vector<std::string>& values) {
    arrow::BinaryBuilder builder;
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

std::shared_ptr<arrow::Array> build_decimal_array(const std::shared_ptr<arrow::DataType>& type,
                                                  const std::vector<int64_t>& values) {
    arrow::Decimal128Builder builder(type, arrow::default_memory_pool());
    for (const auto value : values) {
        EXPECT_TRUE(builder.Append(arrow::Decimal128(value)).ok());
    }
    return finish_array(&builder);
}

std::shared_ptr<arrow::Array> build_fixed_binary_array(const std::shared_ptr<arrow::DataType>& type,
                                                       const std::vector<std::string>& values) {
    arrow::FixedSizeBinaryBuilder builder(type, arrow::default_memory_pool());
    const int32_t byte_width =
            std::static_pointer_cast<arrow::FixedSizeBinaryType>(type)->byte_width();
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

void write_mixed_variant_row_groups(const std::string& file_path) {
    auto n_type = arrow::struct_({arrow::field("value", arrow::binary(), true),
                                  arrow::field("typed_value", arrow::int32(), true)});
    auto padding_type = arrow::struct_({arrow::field("value", arrow::binary(), true),
                                        arrow::field("typed_value", arrow::utf8(), true)});
    auto typed_value_type = arrow::struct_(
            {arrow::field("n", n_type, false), arrow::field("padding", padding_type, false)});
    auto variant_type = arrow::struct_({arrow::field("metadata", arrow::binary(), false),
                                        arrow::field("value", arrow::binary(), true),
                                        arrow::field("typed_value", typed_value_type, true)});

    auto metadata_builder = std::make_shared<arrow::BinaryBuilder>();
    auto root_value_builder = std::make_shared<arrow::BinaryBuilder>();
    auto n_value_builder = std::make_shared<arrow::BinaryBuilder>();
    auto n_typed_builder = std::make_shared<arrow::Int32Builder>();
    auto n_builder = std::make_shared<arrow::StructBuilder>(
            n_type, arrow::default_memory_pool(),
            std::vector<std::shared_ptr<arrow::ArrayBuilder>> {n_value_builder, n_typed_builder});
    auto padding_value_builder = std::make_shared<arrow::BinaryBuilder>();
    auto padding_typed_builder = std::make_shared<arrow::StringBuilder>();
    auto padding_builder = std::make_shared<arrow::StructBuilder>(
            padding_type, arrow::default_memory_pool(),
            std::vector<std::shared_ptr<arrow::ArrayBuilder>> {padding_value_builder,
                                                               padding_typed_builder});
    auto typed_value_builder = std::make_shared<arrow::StructBuilder>(
            typed_value_type, arrow::default_memory_pool(),
            std::vector<std::shared_ptr<arrow::ArrayBuilder>> {n_builder, padding_builder});
    arrow::StructBuilder variant_builder(
            variant_type, arrow::default_memory_pool(),
            std::vector<std::shared_ptr<arrow::ArrayBuilder>> {metadata_builder, root_value_builder,
                                                               typed_value_builder});

    const std::string metadata("\x11\x02\x00\x01\x08npadding", 13);
    for (int row = 0; row < 2; ++row) {
        ASSERT_TRUE(variant_builder.Append().ok());
        ASSERT_TRUE(metadata_builder->Append(metadata).ok());
        ASSERT_TRUE(root_value_builder->AppendNull().ok());
        ASSERT_TRUE(typed_value_builder->Append().ok());
        ASSERT_TRUE(n_builder->Append().ok());
        if (row == 0) {
            ASSERT_TRUE(n_value_builder->AppendNull().ok());
            ASSERT_TRUE(n_typed_builder->Append(1).ok());
        } else {
            const std::string residual("\x0dn/a", 4);
            ASSERT_TRUE(n_value_builder->Append(residual).ok());
            ASSERT_TRUE(n_typed_builder->AppendNull().ok());
        }
        ASSERT_TRUE(padding_builder->Append().ok());
        ASSERT_TRUE(padding_value_builder->AppendNull().ok());
        ASSERT_TRUE(padding_typed_builder->Append("x").ok());
    }

    auto schema = arrow::schema(
            {arrow::field("id", arrow::int32(), false), arrow::field("v", variant_type, false)});
    auto table =
            arrow::Table::Make(schema, {build_int32_array({1, 2}), finish_array(&variant_builder)});
    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;
    ::parquet::WriterProperties::Builder writer_properties;
    writer_properties.compression(::parquet::Compression::UNCOMPRESSED);
    writer_properties.disable_dictionary();
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out, 1,
                                                      writer_properties.build()));
    ASSERT_TRUE(out->Close().ok());
    annotate_variant_schema(file_path);
}

void write_unannotated_binary_parquet_file(const std::string& file_path) {
    auto schema = arrow::schema({arrow::field("raw_bytes", arrow::binary(), false)});
    auto table = arrow::Table::Make(schema, {build_binary_array({"否", "是", "测试"})});
    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;
    ::parquet::WriterProperties::Builder builder;
    builder.compression(::parquet::Compression::UNCOMPRESSED);
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out, 3,
                                                      builder.build()));
}

void write_decimal_and_fixed_binary_parquet_file(const std::string& file_path) {
    auto decimal_type = arrow::decimal128(38, 6);
    auto fixed_type = arrow::fixed_size_binary(4);
    auto schema = arrow::schema({arrow::field("decimal_value", decimal_type, false),
                                 arrow::field("fixed_value", fixed_type, false)});
    auto table = arrow::Table::Make(
            schema,
            {build_decimal_array(decimal_type, {1234567, -1, 0, -987654321, 42}),
             build_fixed_binary_array(fixed_type, {"ABCD", std::string("\0x\0y", 4), "wxyz", "1234",
                                                   std::string("\xff\x00\x7f\x80", 4)})});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;
    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    builder.compression(::parquet::Compression::UNCOMPRESSED);
    builder.disable_dictionary();
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out, 2,
                                                      builder.build()));
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

constexpr size_t SPANNING_NESTED_VALUES = 128;

void write_sparse_filter_nested_parquet_file(const std::string& file_path) {
    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

    const auto id = ::parquet::schema::PrimitiveNode::Make("id", ::parquet::Repetition::REQUIRED,
                                                           ::parquet::LogicalType::None(),
                                                           ::parquet::Type::INT32);
    const auto map_key = ::parquet::schema::PrimitiveNode::Make(
            "key", ::parquet::Repetition::REQUIRED, ::parquet::LogicalType::None(),
            ::parquet::Type::INT32);
    const auto map_value = ::parquet::schema::PrimitiveNode::Make(
            "value", ::parquet::Repetition::OPTIONAL, ::parquet::LogicalType::String(),
            ::parquet::Type::BYTE_ARRAY);
    const auto key_value = ::parquet::schema::GroupNode::Make(
            "key_value", ::parquet::Repetition::REPEATED, {map_key, map_value});
    const auto map = ::parquet::schema::GroupNode::Make("m", ::parquet::Repetition::OPTIONAL,
                                                        {key_value}, ::parquet::LogicalType::Map());
    const auto element = ::parquet::schema::PrimitiveNode::Make(
            "element", ::parquet::Repetition::OPTIONAL, ::parquet::LogicalType::None(),
            ::parquet::Type::INT32);
    const auto list =
            ::parquet::schema::GroupNode::Make("list", ::parquet::Repetition::REPEATED, {element});
    const auto items = ::parquet::schema::GroupNode::Make("items", ::parquet::Repetition::OPTIONAL,
                                                          {list}, ::parquet::LogicalType::List());
    const auto marker = ::parquet::schema::PrimitiveNode::Make(
            "marker", ::parquet::Repetition::REQUIRED, ::parquet::LogicalType::None(),
            ::parquet::Type::INT32);
    const auto nested_struct = ::parquet::schema::GroupNode::Make(
            "s", ::parquet::Repetition::OPTIONAL, {items, marker});
    const auto schema_node = ::parquet::schema::GroupNode::Make(
            "schema", ::parquet::Repetition::REQUIRED, {id, map, nested_struct});
    const auto schema = std::static_pointer_cast<::parquet::schema::GroupNode>(schema_node);

    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    // Arrow 24 enables page indexes by default. V2 and page-index writers preserve record
    // boundaries, so use V1 without a page index to produce the continuation pages that the
    // reader must still handle correctly.
    builder.data_page_version(::parquet::ParquetDataPageVersion::V1);
    builder.disable_write_page_index();
    builder.compression(::parquet::Compression::UNCOMPRESSED);
    builder.disable_dictionary();
    builder.write_batch_size(8);
    // Arrow 24 checks repeated-column page limits only at record or WriteBatch boundaries.
    // A one-byte target deterministically flushes at every eligible boundary, including the
    // deliberately split wide record below.
    builder.data_pagesize(1);
    auto writer = ::parquet::ParquetFileWriter::Open(out, schema, builder.build());
    auto* row_group = writer->AppendRowGroup();

    // Split each wide record across WriteBatch calls. The first half flushes at the one-byte page
    // limit, so the second call deterministically creates a page whose first repetition level is 1.
    constexpr int64_t SPANNING_BATCH_VALUES = SPANNING_NESTED_VALUES / 2;

    auto* id_writer = static_cast<::parquet::Int32Writer*>(row_group->NextColumn());
    const int32_t ids[] = {1, 2, 3, 4, 5, 6};
    EXPECT_EQ(id_writer->WriteBatch(6, nullptr, nullptr, ids), 6);
    id_writer->Close();

    std::vector<int16_t> map_repetition_levels {0, 0, 0, 0};
    std::vector<int16_t> map_key_definition_levels {2, 0, 2, 1};
    std::vector<int16_t> map_value_definition_levels {3, 0, 2, 1};
    std::vector<int32_t> map_keys {10, 30};
    std::vector<::parquet::ByteArray> map_values;
    const std::string rejected_value = "rejected";
    const std::string selected_value = "selected-wide-value";
    map_values.emplace_back(static_cast<uint32_t>(rejected_value.size()),
                            reinterpret_cast<const uint8_t*>(rejected_value.data()));
    for (size_t value = 0; value < SPANNING_NESTED_VALUES; ++value) {
        map_repetition_levels.push_back(value == 0 ? 0 : 1);
        map_key_definition_levels.push_back(2);
        map_value_definition_levels.push_back(3);
        map_keys.push_back(static_cast<int32_t>(5000 + value));
        map_values.emplace_back(static_cast<uint32_t>(selected_value.size()),
                                reinterpret_cast<const uint8_t*>(selected_value.data()));
    }
    map_repetition_levels.push_back(0);
    map_key_definition_levels.push_back(2);
    map_value_definition_levels.push_back(2);
    map_keys.push_back(6000);

    auto* map_key_writer = static_cast<::parquet::Int32Writer*>(row_group->NextColumn());
    constexpr int64_t MAP_PREFIX_LEVELS = 4;
    constexpr int64_t MAP_KEY_PREFIX_VALUES = 2;
    constexpr int64_t MAP_SPLIT_LEVELS = MAP_PREFIX_LEVELS + SPANNING_BATCH_VALUES;
    constexpr int64_t MAP_KEY_SPLIT_VALUES = MAP_KEY_PREFIX_VALUES + SPANNING_BATCH_VALUES;
    EXPECT_EQ(map_key_writer->WriteBatch(MAP_SPLIT_LEVELS, map_key_definition_levels.data(),
                                         map_repetition_levels.data(), map_keys.data()),
              MAP_KEY_SPLIT_VALUES);
    EXPECT_EQ(map_key_writer->WriteBatch(
                      static_cast<int64_t>(map_repetition_levels.size()) - MAP_SPLIT_LEVELS,
                      map_key_definition_levels.data() + MAP_SPLIT_LEVELS,
                      map_repetition_levels.data() + MAP_SPLIT_LEVELS,
                      map_keys.data() + MAP_KEY_SPLIT_VALUES),
              static_cast<int64_t>(map_keys.size()) - MAP_KEY_SPLIT_VALUES);
    map_key_writer->Close();
    auto* map_value_writer = static_cast<::parquet::ByteArrayWriter*>(row_group->NextColumn());
    constexpr int64_t MAP_VALUE_PREFIX_VALUES = 1;
    constexpr int64_t MAP_VALUE_SPLIT_VALUES = MAP_VALUE_PREFIX_VALUES + SPANNING_BATCH_VALUES;
    EXPECT_EQ(map_value_writer->WriteBatch(MAP_SPLIT_LEVELS, map_value_definition_levels.data(),
                                           map_repetition_levels.data(), map_values.data()),
              MAP_VALUE_SPLIT_VALUES);
    EXPECT_EQ(map_value_writer->WriteBatch(
                      static_cast<int64_t>(map_repetition_levels.size()) - MAP_SPLIT_LEVELS,
                      map_value_definition_levels.data() + MAP_SPLIT_LEVELS,
                      map_repetition_levels.data() + MAP_SPLIT_LEVELS,
                      map_values.data() + MAP_VALUE_SPLIT_VALUES),
              static_cast<int64_t>(map_values.size()) - MAP_VALUE_SPLIT_VALUES);
    map_value_writer->Close();

    std::vector<int16_t> element_repetition_levels {0, 0, 0, 1, 0};
    std::vector<int16_t> element_definition_levels {4, 0, 4, 3, 1};
    std::vector<int32_t> element_values {10, 30};
    for (size_t value = 0; value < SPANNING_NESTED_VALUES; ++value) {
        element_repetition_levels.push_back(value == 0 ? 0 : 1);
        element_definition_levels.push_back(4);
        element_values.push_back(static_cast<int32_t>(5000 + value));
    }
    element_repetition_levels.push_back(0);
    element_definition_levels.push_back(4);
    element_values.push_back(6000);
    element_repetition_levels.push_back(1);
    element_definition_levels.push_back(3);

    auto* element_writer = static_cast<::parquet::Int32Writer*>(row_group->NextColumn());
    constexpr int64_t ELEMENT_PREFIX_LEVELS = 5;
    constexpr int64_t ELEMENT_PREFIX_VALUES = 2;
    constexpr int64_t ELEMENT_SPLIT_LEVELS = ELEMENT_PREFIX_LEVELS + SPANNING_BATCH_VALUES;
    constexpr int64_t ELEMENT_SPLIT_VALUES = ELEMENT_PREFIX_VALUES + SPANNING_BATCH_VALUES;
    EXPECT_EQ(element_writer->WriteBatch(ELEMENT_SPLIT_LEVELS, element_definition_levels.data(),
                                         element_repetition_levels.data(), element_values.data()),
              ELEMENT_SPLIT_VALUES);
    EXPECT_EQ(element_writer->WriteBatch(
                      static_cast<int64_t>(element_repetition_levels.size()) - ELEMENT_SPLIT_LEVELS,
                      element_definition_levels.data() + ELEMENT_SPLIT_LEVELS,
                      element_repetition_levels.data() + ELEMENT_SPLIT_LEVELS,
                      element_values.data() + ELEMENT_SPLIT_VALUES),
              static_cast<int64_t>(element_values.size()) - ELEMENT_SPLIT_VALUES);
    element_writer->Close();
    auto* marker_writer = static_cast<::parquet::Int32Writer*>(row_group->NextColumn());
    const int16_t marker_definition_levels[] = {1, 0, 1, 1, 1, 1};
    const int32_t marker_values[] = {10, 30, 40, 50, 60};
    EXPECT_EQ(marker_writer->WriteBatch(6, marker_definition_levels, nullptr, marker_values), 5);
    marker_writer->Close();
    row_group->Close();
    writer->Close();
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

void write_nullable_complex_parquet_file(const std::string& file_path) {
    auto map_array = build_nullable_int_string_map_array();
    auto list_array = build_nullable_string_list_array();
    auto struct_array = build_nullable_string_struct_array();
    auto table = arrow::Table::Make(arrow::schema({arrow::field("m", map_array->type(), true),
                                                   arrow::field("a", list_array->type(), true),
                                                   arrow::field("s", struct_array->type(), true)}),
                                    {map_array, list_array, struct_array});

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

void write_nested_complex_under_struct_parquet_file(const std::string& file_path) {
    auto nested_struct = build_nullable_string_struct_array();
    auto nested_array = build_nullable_string_list_array();
    auto nested_map = build_nullable_int_string_map_array();
    auto marker = build_int32_array({10, 20, 30, 40, 50});
    auto struct_field = arrow::field("nested_struct", nested_struct->type(), true);
    auto array_field = arrow::field("nested_array", nested_array->type(), true);
    auto map_field = arrow::field("nested_map", nested_map->type(), true);
    auto marker_field = arrow::field("marker", arrow::int32(), false);
    auto outer_result =
            arrow::StructArray::Make({nested_struct, nested_array, nested_map, marker},
                                     {struct_field, array_field, map_field, marker_field});
    ASSERT_TRUE(outer_result.ok()) << outer_result.status();
    auto outer = *outer_result;
    auto table = arrow::Table::Make(arrow::schema({arrow::field("outer", outer->type(), false)}),
                                    {outer});

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

void write_dictionary_filter_parquet_file(
        const std::string& file_path,
        ::parquet::Compression::type compression = ::parquet::Compression::UNCOMPRESSED) {
    auto schema = arrow::schema({
            arrow::field("id", arrow::int32(), false),
            arrow::field("value", arrow::utf8(), false),
    });
    const std::vector<std::string> values =
            compression == ::parquet::Compression::UNCOMPRESSED
                    ? std::vector<std::string> {"aa", "az", "lm", "lz", "za", "zz"}
                    : std::vector<std::string> {std::string(4096, 'a'), std::string(4096, 'b'),
                                                std::string(4096, 'c'), std::string(4096, 'd'),
                                                std::string(4096, 'e'), std::string(4096, 'f')};
    auto table = arrow::Table::Make(
            schema, {build_int32_array({1, 2, 3, 4, 5, 6}), build_string_array(values)});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    builder.compression(compression);
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

void write_fixed_width_dictionary_filter_parquet_file(const std::string& file_path) {
    auto schema = arrow::schema({
            arrow::field("id", arrow::int32(), false),
            arrow::field("value", arrow::int32(), true),
    });
    auto table = arrow::Table::Make(
            schema, {build_int32_array({1, 2, 3, 4, 5, 6}),
                     build_nullable_int32_array({10, 20, std::nullopt, 20, 40, 20})});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    builder.compression(::parquet::Compression::UNCOMPRESSED);
    builder.disable_dictionary("id");
    builder.enable_dictionary("value");
    builder.disable_statistics();
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out, 6,
                                                      builder.build()));
}

void write_all_fixed_width_dictionary_filter_parquet_file(const std::string& file_path) {
    auto fixed_type = arrow::fixed_size_binary(4);
    auto timestamp_type = arrow::timestamp(arrow::TimeUnit::MICRO);
    auto schema = arrow::schema({
            arrow::field("id", arrow::int32(), false),
            arrow::field("int64_value", arrow::int64(), false),
            arrow::field("float_value", arrow::float32(), false),
            arrow::field("double_value", arrow::float64(), false),
            arrow::field("fixed_value", fixed_type, false),
            arrow::field("int96_value", timestamp_type, false),
    });
    auto table = arrow::Table::Make(
            schema,
            {build_int32_array({1, 2, 3, 4, 5, 6}), build_int64_array({10, 20, 10, 30, 20, 10}),
             build_float_array({1.5F, 2.5F, 1.5F, 3.5F, 2.5F, 1.5F}),
             build_double_array({10.25, 20.25, 10.25, 30.25, 20.25, 10.25}),
             build_fixed_binary_array(fixed_type, {"AAAA", "BBBB", "AAAA", "CCCC", "BBBB", "AAAA"}),
             build_timestamp_array(timestamp_type, {1000, 2000, 1000, 3000, 2000, 1000})});

    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

    ::parquet::WriterProperties::Builder writer_builder;
    writer_builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    writer_builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    writer_builder.compression(::parquet::Compression::UNCOMPRESSED);
    writer_builder.disable_dictionary("id");
    writer_builder.enable_dictionary("int64_value");
    writer_builder.enable_dictionary("float_value");
    writer_builder.enable_dictionary("double_value");
    writer_builder.enable_dictionary("fixed_value");
    writer_builder.enable_dictionary("int96_value");
    writer_builder.disable_statistics();
    ::parquet::ArrowWriterProperties::Builder arrow_builder;
    arrow_builder.enable_force_write_int96_timestamps();
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out, 6,
                                                      writer_builder.build(),
                                                      arrow_builder.build()));
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
            bool is_immutable = false, bool enable_mapping_varbinary = false,
            std::string fs_name = {}, int64_t mtime = 0) const {
        auto system_properties = std::make_shared<io::FileSystemProperties>();
        system_properties->system_type = TFileType::FILE_LOCAL;
        auto file_description = std::make_unique<io::FileDescription>();
        file_description->path = _file_path;
        file_description->file_size = static_cast<int64_t>(std::filesystem::file_size(_file_path));
        file_description->range_start_offset = range_start_offset;
        file_description->range_size = range_size;
        file_description->is_immutable = is_immutable;
        file_description->fs_name = std::move(fs_name);
        file_description->mtime = mtime;
        return std::make_unique<format::parquet::ParquetReader>(
                system_properties, file_description, std::move(io_ctx), profile,
                global_rowid_context, enable_mapping_timestamp_tz, enable_mapping_varbinary);
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

TEST_F(NewParquetReaderTest, RawByteArrayMappingFollowsV2ScanOption) {
    write_unannotated_binary_parquet_file(_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};

    auto string_reader = create_reader();
    const auto string_init_status = string_reader->init(&state);
    ASSERT_TRUE(string_init_status.ok()) << string_init_status;
    std::vector<format::ColumnDefinition> string_schema;
    ASSERT_TRUE(string_reader->get_schema(&string_schema).ok());
    ASSERT_EQ(string_schema.size(), 1);
    EXPECT_EQ(remove_nullable(string_schema[0].type)->get_primitive_type(), TYPE_STRING);

    RuntimeProfile binary_profile("raw_binary_mapping_profile");
    auto binary_reader =
            create_reader(0, -1, &binary_profile, false, nullptr, std::nullopt, false, true);
    const auto binary_init_status = binary_reader->init(&state);
    ASSERT_TRUE(binary_init_status.ok()) << binary_init_status;
    std::vector<format::ColumnDefinition> binary_schema;
    ASSERT_TRUE(binary_reader->get_schema(&binary_schema).ok());
    ASSERT_EQ(binary_schema.size(), 1);
    // The explicit VARBINARY mapping must remain available for scans whose table contract asks for it.
    EXPECT_EQ(remove_nullable(binary_schema[0].type)->get_primitive_type(), TYPE_VARBINARY);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->local_positions.emplace(format::LocalColumnId(0), format::LocalIndex(0));
    request->conjuncts.push_back(create_string_in_conjunct(0, {"是"}));
    ASSERT_TRUE(binary_reader->open(request).ok());
    // A table-side STRING comparison may be rewritten through this VARBINARY file slot. Native
    // dictionary pruning must leave the row group available for the mapping expression.
    EXPECT_EQ(binary_profile.get_counter("RowGroupsFilteredByDictionary")->value(), 0);
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

TEST(ParquetVariantProjectionTest, ResidualStatisticsGuardPhysicalLeafProjection) {
    using format::parquet::ParquetColumnSchema;
    using format::parquet::ParquetColumnSchemaKind;
    auto node = [](std::string name, int32_t local_id, ParquetColumnSchemaKind kind,
                   int leaf_id = -1) {
        auto result = std::make_unique<ParquetColumnSchema>();
        result->name = std::move(name);
        result->local_id = local_id;
        result->kind = kind;
        result->leaf_column_id = leaf_id;
        return result;
    };
    auto root = node("v", 0, ParquetColumnSchemaKind::VARIANT);
    root->children.push_back(node("metadata", 0, ParquetColumnSchemaKind::PRIMITIVE, 0));
    root->children.push_back(node("value", 1, ParquetColumnSchemaKind::PRIMITIVE, 1));
    auto root_typed = node("typed_value", 2, ParquetColumnSchemaKind::STRUCT);
    auto wrapper = node("n", 0, ParquetColumnSchemaKind::STRUCT);
    wrapper->children.push_back(node("value", 0, ParquetColumnSchemaKind::PRIMITIVE, 2));
    wrapper->children.push_back(node("typed_value", 1, ParquetColumnSchemaKind::PRIMITIVE, 3));
    root_typed->children.push_back(std::move(wrapper));
    root->children.push_back(std::move(root_typed));

    auto projection = format::LocalColumnIndex::partial_local(0);
    projection.children.push_back(format::LocalColumnIndex::partial_local(2));
    projection.children.back().children.push_back(format::LocalColumnIndex::partial_local(0));
    projection.children.back().children.back().children.push_back(
            format::LocalColumnIndex::local(1));

    tparquet::RowGroup row_group;
    row_group.__set_num_rows(10);
    for (int leaf = 0; leaf < 4; ++leaf) {
        tparquet::Statistics statistics;
        statistics.__set_null_count(leaf == 1 || leaf == 2 ? 10 : 0);
        tparquet::ColumnMetaData column_metadata;
        column_metadata.__set_num_values(10);
        column_metadata.__set_statistics(std::move(statistics));
        tparquet::ColumnChunk chunk;
        chunk.__set_meta_data(std::move(column_metadata));
        row_group.columns.push_back(std::move(chunk));
    }
    auto row_group_projection = projection;
    size_t residual_projections = 0;
    EXPECT_EQ(format::parquet::detail::finalize_variant_leaf_projection_for_row_group(
                      row_group, *root, &row_group_projection, &residual_projections),
              1);
    EXPECT_FALSE(row_group_projection.project_all_children);
    EXPECT_EQ(residual_projections, 0);

    // A conforming partially shredded object keeps unrelated keys in the ancestor residual. The
    // requested field is still complete when its own value column is all null.
    row_group.columns[1].meta_data.statistics.__set_null_count(9);
    row_group_projection = projection;
    residual_projections = 0;
    EXPECT_EQ(format::parquet::detail::finalize_variant_leaf_projection_for_row_group(
                      row_group, *root, &row_group_projection, &residual_projections),
              1);
    EXPECT_FALSE(row_group_projection.project_all_children);
    EXPECT_EQ(residual_projections, 0);
    row_group.columns[1].meta_data.statistics.__set_null_count(10);

    // This projection does not read the residual beside the leaf, so a residual that carries
    // values cannot be served from leaves and the complete Variant comes back.
    size_t full_projections = 0;
    row_group.columns[2].meta_data.statistics.__set_null_count(9);
    row_group_projection = projection;
    residual_projections = 0;
    EXPECT_EQ(format::parquet::detail::finalize_variant_leaf_projection_for_row_group(
                      row_group, *root, &row_group_projection, &residual_projections,
                      &full_projections),
              0);
    EXPECT_TRUE(row_group_projection.project_all_children);
    EXPECT_EQ(residual_projections, 0);
    EXPECT_EQ(full_projections, 1);
    row_group.columns[2].meta_data.__set_num_values(9);
    row_group.columns[2].meta_data.statistics.__set_null_count(9);
    row_group_projection = projection;
    residual_projections = 0;
    full_projections = 0;
    EXPECT_EQ(format::parquet::detail::finalize_variant_leaf_projection_for_row_group(
                      row_group, *root, &row_group_projection, &residual_projections,
                      &full_projections),
              0);
    EXPECT_TRUE(row_group_projection.project_all_children);
    EXPECT_EQ(full_projections, 1);
    row_group.columns[2].meta_data.__set_num_values(10);
    row_group.columns[2].meta_data.__isset.statistics = false;
    row_group_projection = projection;
    residual_projections = 0;
    full_projections = 0;
    EXPECT_EQ(format::parquet::detail::finalize_variant_leaf_projection_for_row_group(
                      row_group, *root, &row_group_projection, &residual_projections,
                      &full_projections),
              0);
    EXPECT_TRUE(row_group_projection.project_all_children);
    EXPECT_EQ(full_projections, 1);
}

TEST(ParquetVariantProjectionTest, PrunesResidualColumnsOnlyWhenStatisticsProveThemNull) {
    using format::parquet::ParquetColumnSchema;
    using format::parquet::ParquetColumnSchemaKind;
    auto node = [](std::string name, int32_t local_id, ParquetColumnSchemaKind kind,
                   int leaf_id = -1) {
        auto result = std::make_unique<ParquetColumnSchema>();
        result->name = std::move(name);
        result->local_id = local_id;
        result->kind = kind;
        result->leaf_column_id = leaf_id;
        return result;
    };
    auto root = node("v", 0, ParquetColumnSchemaKind::VARIANT);
    root->children.push_back(node("metadata", 0, ParquetColumnSchemaKind::PRIMITIVE, 0));
    root->children.push_back(node("value", 1, ParquetColumnSchemaKind::PRIMITIVE, 1));
    auto root_typed = node("typed_value", 2, ParquetColumnSchemaKind::STRUCT);
    auto wrapper = node("n", 0, ParquetColumnSchemaKind::STRUCT);
    wrapper->children.push_back(node("value", 0, ParquetColumnSchemaKind::PRIMITIVE, 2));
    wrapper->children.push_back(node("typed_value", 1, ParquetColumnSchemaKind::PRIMITIVE, 3));
    root_typed->children.push_back(std::move(wrapper));
    root->children.push_back(std::move(root_typed));

    // The projection the mapper now produces: root metadata beside typed_value, and the terminal
    // wrapper's residual beside its typed leaf.
    auto projection = format::LocalColumnIndex::partial_local(0);
    projection.children.push_back(format::LocalColumnIndex::local(0));
    projection.children.push_back(format::LocalColumnIndex::partial_local(2));
    auto& typed_projection = projection.children.back();
    typed_projection.children.push_back(format::LocalColumnIndex::partial_local(0));
    auto& wrapper_projection = typed_projection.children.back();
    wrapper_projection.children.push_back(format::LocalColumnIndex::local(0));
    wrapper_projection.children.push_back(format::LocalColumnIndex::local(1));

    tparquet::RowGroup row_group;
    row_group.__set_num_rows(10);
    for (int leaf = 0; leaf < 4; ++leaf) {
        tparquet::Statistics statistics;
        statistics.__set_null_count(leaf == 2 ? 10 : 0);
        tparquet::ColumnMetaData column_metadata;
        column_metadata.__set_num_values(10);
        column_metadata.__set_statistics(std::move(statistics));
        tparquet::ColumnChunk chunk;
        chunk.__set_meta_data(std::move(column_metadata));
        row_group.columns.push_back(std::move(chunk));
    }

    // The terminal residual is entirely NULL, so the shredded leaf alone answers every row and
    // both the residual and the root dictionary can be dropped from the read.
    auto pruned = projection;
    EXPECT_TRUE(format::parquet::detail::variant_residual_columns_are_prunable_for_row_group(
            row_group, *root, pruned));
    format::parquet::detail::prune_variant_residual_columns(*root, &pruned);
    ASSERT_EQ(pruned.children.size(), 1);
    EXPECT_EQ(pruned.children[0].local_id(), 2);
    ASSERT_EQ(pruned.children[0].children.size(), 1);
    ASSERT_EQ(pruned.children[0].children[0].children.size(), 1);
    EXPECT_EQ(pruned.children[0].children[0].children[0].local_id(), 1);

    // One residual row is enough to keep the columns, but the projection stays a leaf projection:
    // the reader merges those rows instead of rebuilding the complete Variant.
    row_group.columns[2].meta_data.statistics.__set_null_count(9);
    auto retained = projection;
    EXPECT_FALSE(format::parquet::detail::variant_residual_columns_are_prunable_for_row_group(
            row_group, *root, retained));
    size_t residual_projections = 0;
    EXPECT_EQ(format::parquet::detail::finalize_variant_leaf_projection_for_row_group(
                      row_group, *root, &retained, &residual_projections),
              1);
    EXPECT_FALSE(retained.project_all_children);
    EXPECT_EQ(residual_projections, 1);
    ASSERT_EQ(retained.children.size(), 2);
    EXPECT_EQ(retained.children[0].local_id(), 0);
}

TEST(ParquetVariantProjectionTest, FinalizesPhysicalProjectionBeforeFooterPruning) {
    using format::parquet::ParquetColumnSchema;
    using format::parquet::ParquetColumnSchemaKind;
    auto primitive = [](std::string name, int32_t local_id, int leaf_id,
                        tparquet::Type::type physical_type, DataTypePtr type) {
        auto result = std::make_unique<ParquetColumnSchema>();
        result->name = std::move(name);
        result->local_id = local_id;
        result->kind = ParquetColumnSchemaKind::PRIMITIVE;
        result->leaf_column_id = leaf_id;
        result->type = std::move(type);
        result->type_descriptor.doris_type = result->type;
        result->type_descriptor.physical_type = physical_type;
        return result;
    };
    auto bytes = [&](std::string name, int32_t local_id, int leaf_id) {
        return primitive(std::move(name), local_id, leaf_id, tparquet::Type::BYTE_ARRAY,
                         make_nullable(std::make_shared<DataTypeString>()));
    };

    std::vector<std::unique_ptr<ParquetColumnSchema>> schema;
    schema.push_back(primitive("id", 0, 0, tparquet::Type::INT32,
                               make_nullable(std::make_shared<DataTypeInt32>())));
    auto variant = std::make_unique<ParquetColumnSchema>();
    variant->name = "v";
    variant->local_id = 1;
    variant->kind = ParquetColumnSchemaKind::VARIANT;
    variant->contains_variant = true;
    variant->type = make_nullable(std::make_shared<DataTypeVariantV2>());
    variant->children.push_back(bytes("metadata", 0, 1));
    variant->children.push_back(bytes("value", 1, 2));
    auto typed_object = std::make_unique<ParquetColumnSchema>();
    typed_object->name = "typed_value";
    typed_object->local_id = 2;
    typed_object->kind = ParquetColumnSchemaKind::STRUCT;
    auto field = std::make_unique<ParquetColumnSchema>();
    field->name = "n";
    field->local_id = 0;
    field->kind = ParquetColumnSchemaKind::STRUCT;
    field->children.push_back(bytes("value", 0, 3));
    field->children.push_back(primitive("typed_value", 1, 4, tparquet::Type::INT32,
                                        make_nullable(std::make_shared<DataTypeInt32>())));
    typed_object->children.push_back(std::move(field));
    variant->children.push_back(std::move(typed_object));
    schema.push_back(std::move(variant));

    auto chunk = [](tparquet::Type::type type, int64_t null_count, int64_t compressed_size,
                    std::optional<int32_t> min_value = std::nullopt,
                    std::optional<int32_t> max_value = std::nullopt) {
        tparquet::Statistics statistics;
        statistics.__set_null_count(null_count);
        if (min_value.has_value() && max_value.has_value()) {
            std::string min_bytes(sizeof(int32_t), '\0');
            std::string max_bytes(sizeof(int32_t), '\0');
            encode_fixed32_le(reinterpret_cast<uint8_t*>(min_bytes.data()), *min_value);
            encode_fixed32_le(reinterpret_cast<uint8_t*>(max_bytes.data()), *max_value);
            statistics.__set_min_value(std::move(min_bytes));
            statistics.__set_max_value(std::move(max_bytes));
        }
        tparquet::ColumnMetaData column_metadata;
        column_metadata.__set_type(type);
        column_metadata.__set_num_values(10);
        column_metadata.__set_total_compressed_size(compressed_size);
        column_metadata.__set_statistics(std::move(statistics));
        tparquet::ColumnChunk result;
        result.__set_meta_data(std::move(column_metadata));
        return result;
    };
    tparquet::RowGroup row_group;
    row_group.__set_num_rows(10);
    row_group.__set_columns(
            {chunk(tparquet::Type::INT32, 0, 10, 1, 2), chunk(tparquet::Type::BYTE_ARRAY, 0, 20),
             chunk(tparquet::Type::BYTE_ARRAY, 10, 30), chunk(tparquet::Type::BYTE_ARRAY, 9, 40),
             chunk(tparquet::Type::INT32, 1, 50)});
    auto leaf_row_group = row_group;
    leaf_row_group.columns[3] = chunk(tparquet::Type::BYTE_ARRAY, 10, 40);
    tparquet::ColumnOrder order;
    order.__set_TYPE_ORDER(tparquet::TypeDefinedOrder());
    tparquet::FileMetaData thrift_metadata;
    thrift_metadata.__set_num_rows(20);
    thrift_metadata.__set_row_groups({row_group, leaf_row_group});
    thrift_metadata.__set_column_orders({order, order, order, order, order});
    format::parquet::NativeParquetMetadata metadata(std::move(thrift_metadata), 0);

    // The shape the mapper builds: the root dictionary and the terminal residual travel with the
    // typed leaf so a row group whose residual carries values can still be served from leaves.
    auto leaf_projection = format::LocalColumnIndex::partial_local(1);
    leaf_projection.children.push_back(format::LocalColumnIndex::local(0));
    leaf_projection.children.push_back(format::LocalColumnIndex::partial_local(2));
    leaf_projection.children.back().children.push_back(format::LocalColumnIndex::partial_local(0));
    leaf_projection.children.back().children.back().children.push_back(
            format::LocalColumnIndex::local(0));
    leaf_projection.children.back().children.back().children.push_back(
            format::LocalColumnIndex::local(1));
    format::FileScanRequest request;
    request.predicate_columns = {format::LocalColumnIndex::local(0)};
    request.non_predicate_columns = {std::move(leaf_projection)};
    request.local_positions.emplace(format::LocalColumnId(0), format::LocalIndex(0));
    request.local_positions.emplace(format::LocalColumnId(1), format::LocalIndex(1));
    request.conjuncts.push_back(create_int32_greater_than_conjunct(0, 50));

    format::parquet::ParquetFileContext file_context;
    file_context.native_metadata = &metadata;
    file_context.contains_variant = true;
    format::parquet::RowGroupScanPlan plan;
    ASSERT_TRUE(format::parquet::plan_parquet_row_groups(metadata, schema, request,
                                                         {.start_offset = 0, .size = -1}, false,
                                                         &plan, nullptr, nullptr, &file_context)
                        .ok());
    EXPECT_TRUE(plan.row_groups.empty());
    // Footer accounting uses id plus the projected Variant leaves - dictionary, residual and typed
    // leaf - for the first row group (120 bytes). The fully shredded row group proves its residual
    // is NULL, so it drops the dictionary and the residual and reads id plus the typed leaf
    // (60 bytes).
    EXPECT_EQ(plan.pruning_stats.filtered_bytes, 180);

    request.conjuncts.clear();
    ASSERT_TRUE(format::parquet::plan_parquet_row_groups(metadata, schema, request,
                                                         {.start_offset = 0, .size = -1}, false,
                                                         &plan, nullptr, nullptr, &file_context)
                        .ok());
    ASSERT_EQ(plan.row_groups.size(), 2);
    // Both row groups keep a leaf projection. Only the fully shredded one can additionally drop
    // the residual columns, so only it carries a row-group specific physical projection.
    EXPECT_FALSE(plan.row_groups[0].has_row_group_physical_projection());
    EXPECT_TRUE(plan.row_groups[0].prunable_variant_projection_ordinals.empty());
    EXPECT_EQ(plan.row_groups[0].variant_leaf_projection_columns, 1);
    EXPECT_EQ(plan.row_groups[0].variant_residual_projection_columns, 1);
    EXPECT_TRUE(plan.row_groups[1].has_row_group_physical_projection());
    EXPECT_EQ(plan.row_groups[1].prunable_variant_projection_ordinals, std::vector<size_t> {0});
    EXPECT_EQ(plan.row_groups[1].variant_leaf_projection_columns, 1);
    EXPECT_EQ(plan.row_groups[1].variant_residual_projection_columns, 0);
}

TEST(ParquetVariantProjectionTest, ReusesWideNonVariantLeafSetAcrossRowGroups) {
    constexpr int COLUMN_COUNT = 64;
    constexpr int ROW_GROUP_COUNT = 3;
    std::vector<std::unique_ptr<format::parquet::ParquetColumnSchema>> schema;
    format::FileScanRequest request;
    for (int column = 0; column < COLUMN_COUNT; ++column) {
        auto field = std::make_unique<format::parquet::ParquetColumnSchema>();
        field->name = fmt::format("c{}", column);
        field->local_id = column;
        field->leaf_column_id = column;
        field->kind = format::parquet::ParquetColumnSchemaKind::PRIMITIVE;
        field->type = make_nullable(std::make_shared<DataTypeInt32>());
        field->type_descriptor.doris_type = field->type;
        field->type_descriptor.physical_type = tparquet::Type::INT32;
        schema.push_back(std::move(field));
        request.non_predicate_columns.push_back(format::LocalColumnIndex::local(column));
        request.local_positions.emplace(format::LocalColumnId(column), format::LocalIndex(column));
    }

    tparquet::ColumnMetaData column_metadata;
    column_metadata.__set_type(tparquet::Type::INT32);
    column_metadata.__set_num_values(10);
    column_metadata.__set_total_compressed_size(1);
    tparquet::ColumnChunk chunk;
    chunk.__set_meta_data(std::move(column_metadata));
    tparquet::RowGroup row_group;
    row_group.__set_num_rows(10);
    row_group.__set_columns(std::vector<tparquet::ColumnChunk>(COLUMN_COUNT, chunk));
    tparquet::FileMetaData thrift_metadata;
    thrift_metadata.__set_num_rows(ROW_GROUP_COUNT * 10);
    thrift_metadata.__set_row_groups(std::vector<tparquet::RowGroup>(ROW_GROUP_COUNT, row_group));
    format::parquet::NativeParquetMetadata metadata(std::move(thrift_metadata), 0);
    format::parquet::ParquetFileContext file_context;
    file_context.native_metadata = &metadata;
    file_context.contains_variant = false;
    format::parquet::RowGroupScanPlan plan;

    format::parquet::detail::reset_physical_leaf_set_build_count();
    ASSERT_TRUE(format::parquet::plan_parquet_row_groups(metadata, schema, request,
                                                         {.start_offset = 0, .size = -1}, false,
                                                         &plan, nullptr, nullptr, &file_context)
                        .ok());
    EXPECT_EQ(plan.row_groups.size(), ROW_GROUP_COUNT);
    EXPECT_EQ(format::parquet::detail::physical_leaf_set_build_count(), 1);
}

TEST(ParquetVariantProjectionTest, FinalizesNestedVariantProjectionPerRowGroup) {
    using format::parquet::ParquetColumnSchema;
    using format::parquet::ParquetColumnSchemaKind;
    auto node = [](std::string name, int32_t local_id, ParquetColumnSchemaKind kind,
                   int leaf_id = -1) {
        auto result = std::make_unique<ParquetColumnSchema>();
        result->name = std::move(name);
        result->local_id = local_id;
        result->kind = kind;
        result->leaf_column_id = leaf_id;
        return result;
    };
    auto root = node("info", 0, ParquetColumnSchemaKind::STRUCT);
    auto variant = node("payload", 0, ParquetColumnSchemaKind::VARIANT);
    variant->children.push_back(node("metadata", 0, ParquetColumnSchemaKind::PRIMITIVE, 0));
    variant->children.push_back(node("value", 1, ParquetColumnSchemaKind::PRIMITIVE, 1));
    auto typed_object = node("typed_value", 2, ParquetColumnSchemaKind::STRUCT);
    auto wrapper = node("n", 0, ParquetColumnSchemaKind::STRUCT);
    wrapper->children.push_back(node("value", 0, ParquetColumnSchemaKind::PRIMITIVE, 2));
    wrapper->children.push_back(node("typed_value", 1, ParquetColumnSchemaKind::PRIMITIVE, 3));
    typed_object->children.push_back(std::move(wrapper));
    variant->children.push_back(std::move(typed_object));
    root->children.push_back(std::move(variant));

    auto projection = format::LocalColumnIndex::partial_local(0);
    projection.children.push_back(format::LocalColumnIndex::partial_local(0));
    projection.children.back().children.push_back(format::LocalColumnIndex::partial_local(2));
    projection.children.back().children.back().children.push_back(
            format::LocalColumnIndex::partial_local(0));
    projection.children.back().children.back().children.back().children.push_back(
            format::LocalColumnIndex::local(1));

    tparquet::RowGroup row_group;
    row_group.__set_num_rows(10);
    for (int leaf = 0; leaf < 4; ++leaf) {
        tparquet::Statistics statistics;
        statistics.__set_null_count(leaf == 1 || leaf == 2 ? 10 : 0);
        tparquet::ColumnMetaData column_metadata;
        column_metadata.__set_num_values(10);
        column_metadata.__set_statistics(std::move(statistics));
        tparquet::ColumnChunk chunk;
        chunk.__set_meta_data(std::move(column_metadata));
        row_group.columns.push_back(std::move(chunk));
    }
    EXPECT_EQ(format::parquet::detail::finalize_variant_leaf_projection_for_row_group(
                      row_group, *root, &projection),
              1);
    EXPECT_FALSE(projection.children[0].project_all_children);

    // This projection does not read the residual, so a residual that carries values restores the
    // complete Variant.
    auto fallback = projection;
    row_group.columns[2].meta_data.statistics.__set_null_count(9);
    EXPECT_EQ(format::parquet::detail::finalize_variant_leaf_projection_for_row_group(
                      row_group, *root, &fallback),
              0);
    EXPECT_TRUE(fallback.children[0].project_all_children);

    // Row-group null counts are per value, not per row, so a repeated Variant can never prove its
    // residual is empty either.
    auto repeated = projection;
    root->children[0]->max_repetition_level = 1;
    EXPECT_EQ(format::parquet::detail::finalize_variant_leaf_projection_for_row_group(
                      row_group, *root, &repeated),
              0);
    EXPECT_TRUE(repeated.children[0].project_all_children);
}

TEST_F(NewParquetReaderTest, ReadsFullyShreddedVariantTypedLeafProjection) {
    const char* source_root = std::getenv("ROOT");
    ASSERT_NE(source_root, nullptr);
    _file_path = std::string(source_root) +
                 "/regression-test/data/external_table_p0/iceberg/"
                 "iceberg_variant_shredded.parquet";
    ASSERT_TRUE(std::filesystem::exists(_file_path));

    RuntimeProfile profile("variant_typed_leaf_projection");
    auto reader = create_reader(0, -1, &profile);
    reader->set_batch_size(1024);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());
    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);
    ASSERT_EQ(remove_nullable(schema[1].type)->get_primitive_type(), TYPE_VARIANT);

    auto find_child = [](const std::vector<format::ColumnDefinition>& children,
                         std::string_view name) -> const format::ColumnDefinition* {
        const auto it = std::ranges::find_if(
                children, [name](const auto& child) { return child.name == name; });
        return it == children.end() ? nullptr : &*it;
    };
    const auto* root_typed = find_child(schema[1].children, "typed_value");
    ASSERT_NE(root_typed, nullptr);
    const auto* n_wrapper = find_child(root_typed->children, "n");
    ASSERT_NE(n_wrapper, nullptr);
    const auto* n_typed = find_child(n_wrapper->children, "typed_value");
    ASSERT_NE(n_typed, nullptr);

    auto projection = format::LocalColumnIndex::partial_local(schema[1].local_id);
    projection.children.push_back(format::LocalColumnIndex::partial_local(root_typed->local_id));
    projection.children.back().children.push_back(
            format::LocalColumnIndex::partial_local(n_wrapper->local_id));
    projection.children.back().children.back().children.push_back(
            format::LocalColumnIndex::local(n_typed->local_id));
    auto request = std::make_shared<format::FileScanRequest>();
    request->non_predicate_columns.push_back(std::move(projection));
    request->local_positions.emplace(format::LocalColumnId(schema[1].local_id),
                                     format::LocalIndex(0));
    ASSERT_TRUE(reader->open(request).ok());
    ASSERT_NE(profile.get_counter("VariantLeafProjections"), nullptr);
    EXPECT_EQ(profile.get_counter("VariantLeafProjections")->value(), 1);

    Block block;
    block.insert({schema[1].type->create_column(), schema[1].type, "v"});
    size_t rows = 0;
    bool eof = false;
    while (!eof) {
        size_t batch_rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &batch_rows, &eof).ok());
        rows += batch_rows;
    }
    ASSERT_EQ(rows, 4096);
    const auto& nullable = assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
    const auto& variants = assert_cast<const ColumnVariantV2&>(nullable.get_nested_column());
    const std::array path {VariantShreddedPathSegment {
            .kind = VariantShreddedPathSegment::Kind::OBJECT_KEY, .key = StringRef("n")}};
    const auto match = variants.find_shredded_typed_value(path);
    ASSERT_TRUE(match.has_value());
    EXPECT_EQ(match->type->get_primitive_type(), TYPE_INT);
    EXPECT_EQ(match->column->size(), rows);
    ASSERT_NE(profile.get_counter("VariantDirectLeafRows"), nullptr);
    EXPECT_EQ(profile.get_counter("VariantDirectLeafRows")->value(), rows);
    ASSERT_NE(profile.get_counter("VariantReconstructedRows"), nullptr);
    EXPECT_EQ(profile.get_counter("VariantReconstructedRows")->value(), 0);
    ASSERT_NE(profile.get_counter("VariantLeafProjectionRowGroupColumns"), nullptr);
    EXPECT_EQ(profile.get_counter("VariantLeafProjectionRowGroupColumns")->value(), 1);
    ASSERT_NE(profile.get_counter("VariantResidualProjectionRowGroupColumns"), nullptr);
    EXPECT_EQ(profile.get_counter("VariantResidualProjectionRowGroupColumns")->value(), 0);
    const std::array missing_path {VariantShreddedPathSegment {
            .kind = VariantShreddedPathSegment::Kind::OBJECT_KEY, .key = StringRef("missing")}};
    EXPECT_FALSE(variants.find_shredded_typed_value(missing_path).has_value());
    ASSERT_NE(profile.get_counter("VariantDirectLeafPathMisses"), nullptr);
    EXPECT_EQ(profile.get_counter("VariantDirectLeafPathMisses")->value(), 1);
    const auto first_value =
            assert_cast<const ColumnInt32&>(
                    assert_cast<const ColumnNullable&>(*match->column).get_nested_column())
                    .get_data()[0];

    IColumn::Filter keep(rows, 0);
    keep[0] = 1;
    const ColumnPtr filtered = variants.filter(keep, 1);
    const auto& filtered_variants = assert_cast<const ColumnVariantV2&>(*filtered);
    ASSERT_TRUE(filtered_variants.is_shredded());
    const auto filtered_match = filtered_variants.find_shredded_typed_value(path);
    ASSERT_TRUE(filtered_match.has_value());
    EXPECT_EQ(filtered_match->column->size(), 1);
    EXPECT_EQ(
            assert_cast<const ColumnInt32&>(
                    assert_cast<const ColumnNullable&>(*filtered_match->column).get_nested_column())
                    .get_data()[0],
            first_value);
    EXPECT_TRUE(variants.clone_resized(0)->empty());
    auto mutable_filtered = variants.clone_resized(variants.size());
    EXPECT_EQ(mutable_filtered->filter(keep), 1);
    EXPECT_TRUE(assert_cast<const ColumnVariantV2&>(*mutable_filtered).is_shredded());

    // TableReader detaches mapped output columns before upper expressions run. Detachment must
    // preserve an incomplete leaf projection because it has no canonical Variant to materialize.
    auto detached = IColumn::mutate(block.get_by_position(0).column);
    const auto& detached_variants = assert_cast<const ColumnVariantV2&>(
            assert_cast<const ColumnNullable&>(*detached).get_nested_column());
    ASSERT_TRUE(detached_variants.is_shredded());
    ASSERT_TRUE(detached_variants.find_shredded_typed_value(path).has_value());

    // Adaptive predicate probing cuts retained output columns into proper subsets. Keep that row
    // selection in the physical shredded state as well.
    const ColumnPtr sliced = variants.cut(1, 2);
    const auto& sliced_variants = assert_cast<const ColumnVariantV2&>(*sliced);
    ASSERT_TRUE(sliced_variants.is_shredded());
    const auto sliced_match = sliced_variants.find_shredded_typed_value(path);
    ASSERT_TRUE(sliced_match.has_value());
    ASSERT_EQ(sliced_match->column->size(), 2);
    EXPECT_EQ(assert_cast<const ColumnInt32&>(
                      assert_cast<const ColumnNullable&>(*sliced_match->column).get_nested_column())
                      .get_data()[0],
              first_value + 1);

    const std::array<uint32_t, 2> indices {2, 0};
    MutableColumnPtr gathered = variants.clone_empty();
    gathered->insert_indices_from(variants, indices.data(), indices.data() + indices.size());
    const auto& gathered_variants = assert_cast<const ColumnVariantV2&>(*gathered);
    ASSERT_TRUE(gathered_variants.is_shredded());
    const auto gathered_match = gathered_variants.find_shredded_typed_value(path);
    ASSERT_TRUE(gathered_match.has_value());
    ASSERT_EQ(gathered_match->column->size(), indices.size());
    EXPECT_EQ(
            assert_cast<const ColumnInt32&>(
                    assert_cast<const ColumnNullable&>(*gathered_match->column).get_nested_column())
                    .get_data()[0],
            first_value + 2);
}

TEST_F(NewParquetReaderTest, SwitchesVariantLeafProjectionPerRowGroup) {
    _file_path = (_test_dir / "iceberg_variant_mixed_row_groups.parquet").string();
    write_mixed_variant_row_groups(_file_path);

    RuntimeProfile profile("variant_row_group_projection_switch");
    auto reader = create_reader(0, -1, &profile);
    reader->set_batch_size(1024);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());
    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);

    auto find_child = [](const std::vector<format::ColumnDefinition>& children,
                         std::string_view name) -> const format::ColumnDefinition* {
        const auto it = std::ranges::find_if(
                children, [name](const auto& child) { return child.name == name; });
        return it == children.end() ? nullptr : &*it;
    };
    const auto* root_typed = find_child(schema[1].children, "typed_value");
    ASSERT_NE(root_typed, nullptr);
    const auto* n_wrapper = find_child(root_typed->children, "n");
    ASSERT_NE(n_wrapper, nullptr);
    const auto* n_typed = find_child(n_wrapper->children, "typed_value");
    ASSERT_NE(n_typed, nullptr);

    auto projection = format::LocalColumnIndex::partial_local(schema[1].local_id);
    projection.children.push_back(format::LocalColumnIndex::partial_local(root_typed->local_id));
    projection.children.back().children.push_back(
            format::LocalColumnIndex::partial_local(n_wrapper->local_id));
    projection.children.back().children.back().children.push_back(
            format::LocalColumnIndex::local(n_typed->local_id));
    auto request = std::make_shared<format::FileScanRequest>();
    request->non_predicate_columns.push_back(std::move(projection));
    request->local_positions.emplace(format::LocalColumnId(schema[1].local_id),
                                     format::LocalIndex(0));
    ASSERT_TRUE(reader->open(request).ok());
    ASSERT_NE(profile.get_counter("VariantLeafProjections"), nullptr);
    EXPECT_EQ(profile.get_counter("VariantLeafProjections")->value(), 1);

    Block block;
    block.insert({schema[1].type->create_column(), schema[1].type, "v"});
    size_t rows = 0;
    bool eof = false;
    while (!eof) {
        size_t batch_rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &batch_rows, &eof).ok());
        rows += batch_rows;
    }
    EXPECT_EQ(rows, 2);
    // This request projects the typed leaf without the residual beside it, so the row group whose
    // residual carries values cannot be served from leaves and restores the complete Variant.
    ASSERT_NE(profile.get_counter("VariantLeafProjectionRowGroupColumns"), nullptr);
    EXPECT_EQ(profile.get_counter("VariantLeafProjectionRowGroupColumns")->value(), 1);
    ASSERT_NE(profile.get_counter("VariantResidualProjectionRowGroupColumns"), nullptr);
    EXPECT_EQ(profile.get_counter("VariantResidualProjectionRowGroupColumns")->value(), 0);
    ASSERT_NE(profile.get_counter("VariantFullProjectionRowGroupColumns"), nullptr);
    EXPECT_EQ(profile.get_counter("VariantFullProjectionRowGroupColumns")->value(), 1);

    const auto& nullable = assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
    const auto& variants = assert_cast<const ColumnVariantV2&>(nullable.get_nested_column());
    const std::array path {VariantShreddedPathSegment {
            .kind = VariantShreddedPathSegment::Kind::OBJECT_KEY, .key = StringRef("n")}};
    const auto match = variants.find_shredded_typed_value(path);
    ASSERT_TRUE(match.has_value());
    ASSERT_TRUE(match->normalized);
    EXPECT_EQ(match->normalized->size(), rows);
    std::vector<std::string> values;
    auto serialized = ColumnString::create();
    BufferWritable writer(*serialized);
    auto options = DataTypeSerDe::get_default_format_options();
    const auto serde = schema[1].type->get_serde();
    for (size_t row = 0; row < rows; ++row) {
        const auto status =
                serde->serialize_one_cell_to_json(*match->normalized, row, writer, options);
        ASSERT_TRUE(status.ok()) << status.to_string();
        writer.commit();
        values.emplace_back(serialized->get_data_at(row).to_string());
    }
    EXPECT_EQ(values, std::vector<std::string>({"1", R"("n/a")"}));
}

TEST_F(NewParquetReaderTest, ShreddedVariantPredicateUsesTypedLeafPageIndexWithRootOutput) {
    const char* source_root = std::getenv("ROOT");
    ASSERT_NE(source_root, nullptr);
    _file_path = std::string(source_root) +
                 "/regression-test/data/external_table_p0/iceberg/"
                 "iceberg_variant_shredded.parquet";
    ASSERT_TRUE(std::filesystem::exists(_file_path));

    RuntimeProfile profile("variant_page_pruning_with_root_output");
    auto reader = create_reader(0, -1, &profile);
    reader->set_batch_size(1024);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());
    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);

    auto request = std::make_shared<format::FileScanRequest>();
    request->non_predicate_columns.push_back(
            format::LocalColumnIndex::top_level(format::LocalColumnId(schema[0].local_id)));
    // The root output deliberately retains the complete wrapper; its predicate may still use the
    // typed leaf's page index without converting the output to a leaf-only Variant projection.
    request->predicate_columns.push_back(
            format::LocalColumnIndex::top_level(format::LocalColumnId(schema[1].local_id)));
    request->local_positions.emplace(format::LocalColumnId(schema[0].local_id),
                                     format::LocalIndex(0));
    request->local_positions.emplace(format::LocalColumnId(schema[1].local_id),
                                     format::LocalIndex(1));
    request->conjuncts.push_back(create_variant_int32_path_greater_than_conjunct(1, "n", 3000));
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    while (!eof) {
        Block block = build_file_block(schema);
        size_t batch_rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &batch_rows, &eof).ok());
        rows += batch_rows;
        if (batch_rows > 0) {
            const auto& nullable =
                    assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
            auto canonical = IColumn::mutate(nullable.get_nested_column_ptr());
            assert_cast<ColumnVariantV2&>(*canonical).ensure_encoded();
        }
    }
    EXPECT_EQ(rows, 1095);
    ASSERT_NE(profile.get_counter("FilteredRowsByPage"), nullptr);
    EXPECT_GT(profile.get_counter("FilteredRowsByPage")->value(), 0);
    ASSERT_NE(profile.get_counter("VariantLeafProjections"), nullptr);
    EXPECT_EQ(profile.get_counter("VariantLeafProjections")->value(), 0);
    ASSERT_NE(profile.get_counter("VariantDirectLeafRows"), nullptr);
    EXPECT_GT(profile.get_counter("VariantDirectLeafRows")->value(), 0);
    ASSERT_NE(profile.get_counter("VariantReconstructedRows"), nullptr);
    EXPECT_EQ(profile.get_counter("VariantReconstructedRows")->value(), rows);
    ASSERT_NE(profile.get_counter("VariantReconstructionTime"), nullptr);
    EXPECT_GT(profile.get_counter("VariantReconstructionTime")->value(), 0);
}

TEST_F(NewParquetReaderTest, ReadsVariantPredicateLeafBeforeDeferredRootOutput) {
    const char* source_root = std::getenv("ROOT");
    ASSERT_NE(source_root, nullptr);
    _file_path = std::string(source_root) +
                 "/regression-test/data/external_table_p0/iceberg/"
                 "iceberg_variant_shredded.parquet";
    ASSERT_TRUE(std::filesystem::exists(_file_path));

    RuntimeProfile profile("variant_predicate_leaf_deferred_root");
    auto reader = create_reader(0, -1, &profile);
    reader->set_batch_size(1024);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());
    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);

    auto find_child = [](const std::vector<format::ColumnDefinition>& children,
                         std::string_view name) -> const format::ColumnDefinition* {
        const auto it = std::ranges::find_if(
                children, [name](const auto& child) { return child.name == name; });
        return it == children.end() ? nullptr : &*it;
    };
    const auto* root_typed = find_child(schema[1].children, "typed_value");
    ASSERT_NE(root_typed, nullptr);
    const auto* n_wrapper = find_child(root_typed->children, "n");
    ASSERT_NE(n_wrapper, nullptr);
    const auto* n_typed = find_child(n_wrapper->children, "typed_value");
    ASSERT_NE(n_typed, nullptr);

    auto predicate_projection = format::LocalColumnIndex::partial_local(schema[1].local_id);
    predicate_projection.children.push_back(
            format::LocalColumnIndex::partial_local(root_typed->local_id));
    predicate_projection.children.back().children.push_back(
            format::LocalColumnIndex::partial_local(n_wrapper->local_id));
    predicate_projection.children.back().children.back().children.push_back(
            format::LocalColumnIndex::local(n_typed->local_id));

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns.push_back(std::move(predicate_projection));
    request->non_predicate_columns.push_back(
            format::LocalColumnIndex::top_level(format::LocalColumnId(schema[1].local_id)));
    request->predicate_only_columns.push_back(format::LocalColumnId(schema[1].local_id));
    request->local_positions.emplace(format::LocalColumnId(schema[1].local_id),
                                     format::LocalIndex(0));
    request->non_predicate_positions.emplace(format::LocalColumnId(schema[1].local_id),
                                             format::LocalIndex(1));
    request->conjuncts.push_back(create_variant_int32_path_greater_than_conjunct(0, "n", 3000));
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    while (!eof) {
        Block block;
        block.insert({schema[1].type->create_column(), schema[1].type, "v_predicate"});
        block.insert({schema[1].type->create_column(), schema[1].type, "v_output"});
        size_t batch_rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &batch_rows, &eof).ok());
        rows += batch_rows;
        ASSERT_EQ(block.get_by_position(0).column->size(), batch_rows);
        ASSERT_EQ(block.get_by_position(1).column->size(), batch_rows);
        if (batch_rows > 0) {
            const auto& nullable =
                    assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
            auto canonical = IColumn::mutate(nullable.get_nested_column_ptr());
            assert_cast<ColumnVariantV2&>(*canonical).ensure_encoded();
        }
    }
    EXPECT_EQ(rows, 1095);
    ASSERT_NE(profile.get_counter("VariantLeafProjections"), nullptr);
    EXPECT_EQ(profile.get_counter("VariantLeafProjections")->value(), 1);
    ASSERT_NE(profile.get_counter("FilteredRowsByLazyRead"), nullptr);
    EXPECT_GT(profile.get_counter("FilteredRowsByLazyRead")->value(), 0);
    ASSERT_NE(profile.get_counter("VariantReconstructedRows"), nullptr);
    EXPECT_EQ(profile.get_counter("VariantReconstructedRows")->value(), rows);
}

TEST_F(NewParquetReaderTest, ReadsStructPredicateChildBeforeDeferredRootOutput) {
    write_struct_filter_parquet_file(_file_path);
    RuntimeProfile profile("struct_predicate_child_deferred_root");
    auto reader = create_reader(0, -1, &profile);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());
    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 1);
    ASSERT_EQ(schema[0].children.size(), 2);

    auto predicate_projection = format::LocalColumnIndex::partial_local(schema[0].local_id);
    predicate_projection.children.push_back(
            format::LocalColumnIndex::local(schema[0].children[0].local_id));
    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns.push_back(predicate_projection);
    request->non_predicate_columns.push_back(
            format::LocalColumnIndex::top_level(format::LocalColumnId(schema[0].local_id)));
    request->predicate_only_columns.push_back(format::LocalColumnId(schema[0].local_id));
    request->local_positions.emplace(format::LocalColumnId(schema[0].local_id),
                                     format::LocalIndex(0));
    request->non_predicate_positions.emplace(format::LocalColumnId(schema[0].local_id),
                                             format::LocalIndex(1));
    request->conjuncts.push_back(create_struct_int32_child_greater_than_conjunct(0, 2));
    ASSERT_TRUE(reader->open(request).ok());

    format::ColumnDefinition predicate_field;
    ASSERT_TRUE(format::project_column_definition(schema[0], predicate_projection, &predicate_field)
                        .ok());
    size_t total_rows = 0;
    std::vector<std::string> names;
    bool eof = false;
    while (!eof) {
        Block block;
        block.insert({predicate_field.type->create_column(), predicate_field.type, "s_predicate"});
        block.insert({schema[0].type->create_column(), schema[0].type, "s_output"});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        total_rows += rows;
        ASSERT_EQ(block.get_by_position(0).column->size(), rows);
        ASSERT_EQ(block.get_by_position(1).column->size(), rows);
        const auto& output_nullable =
                assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
        const auto& output_struct =
                assert_cast<const ColumnStruct&>(output_nullable.get_nested_column());
        ASSERT_EQ(output_struct.tuple_size(), 2);
        const auto& name_nullable = assert_cast<const ColumnNullable&>(output_struct.get_column(1));
        const auto& name_values =
                assert_cast<const ColumnString&>(name_nullable.get_nested_column());
        for (size_t row = 0; row < rows; ++row) {
            names.push_back(name_values.get_data_at(row).to_string());
        }
    }
    EXPECT_EQ(total_rows, 2);
    EXPECT_EQ(names, (std::vector<std::string> {"ten", "eleven"}));
    ASSERT_NE(profile.get_counter("FilteredRowsByLazyRead"), nullptr);
    EXPECT_GT(profile.get_counter("FilteredRowsByLazyRead")->value(), 0);
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
    ASSERT_NE(profile.get_counter("PageReadCount"), nullptr);
    EXPECT_GT(profile.get_counter("PageReadCount")->value(), 0);
    ASSERT_NE(profile.get_counter("ParsePageHeaderNum"), nullptr);
    EXPECT_GT(profile.get_counter("ParsePageHeaderNum")->value(), 0);
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

TEST_F(NewParquetReaderTest, NativeComplexColumnsMaterializeDirectlyAcrossBatchChanges) {
    write_nullable_complex_parquet_file(_file_path);
    RuntimeProfile profile("native_complex_materialization");
    auto reader = create_reader(0, -1, &profile);
    reader->set_batch_size(2);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 3);
    auto request = std::make_shared<format::FileScanRequest>();
    request->non_predicate_columns = {field_projection(0), field_projection(1),
                                      field_projection(2)};
    ASSERT_TRUE(reader->open(request).ok());

    MutableColumns output;
    output.reserve(schema.size());
    for (const auto& field : schema) {
        output.push_back(field.type->create_column());
    }
    bool eof = false;
    int batch = 0;
    while (!eof) {
        Block block = build_file_block(schema);
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (rows == 0) {
            continue;
        }
        for (size_t column = 0; column < output.size(); ++column) {
            output[column]->insert_range_from(*block.get_by_position(column).column, 0, rows);
        }
        if (++batch == 1) {
            // Adaptive sizing changes only the logical row cap. The persistent native readers and
            // their level/string scratch must continue from the same page cursors.
            reader->set_batch_size(3);
        }
    }

    const auto& nullable_map = assert_cast<const ColumnNullable&>(*output[0]);
    ASSERT_EQ(nullable_map.size(), ROW_COUNT);
    EXPECT_FALSE(nullable_map.is_null_at(0));
    EXPECT_TRUE(nullable_map.is_null_at(1));
    EXPECT_FALSE(nullable_map.is_null_at(2));
    const auto& map = assert_cast<const ColumnMap&>(nullable_map.get_nested_column());
    EXPECT_EQ(map.get_offsets(), ColumnArray::Offsets64({1, 1, 1, 2, 3}));
    const auto& map_keys = assert_cast<const ColumnNullable&>(map.get_keys());
    const auto& key_values = assert_cast<const ColumnInt32&>(map_keys.get_nested_column());
    ASSERT_EQ(key_values.size(), 3);
    EXPECT_EQ(key_values.get_element(0), 10);
    EXPECT_EQ(key_values.get_element(2), 30);
    const auto& map_values = assert_cast<const ColumnNullable&>(map.get_values());
    const auto& value_strings = assert_cast<const ColumnString&>(map_values.get_nested_column());
    EXPECT_EQ(value_strings.get_data_at(0).to_string(), "small");
    EXPECT_EQ(value_strings.get_data_at(1).size, 4096);
    EXPECT_TRUE(map_values.is_null_at(2));

    const auto& nullable_array = assert_cast<const ColumnNullable&>(*output[1]);
    ASSERT_EQ(nullable_array.size(), ROW_COUNT);
    EXPECT_TRUE(nullable_array.is_null_at(1));
    const auto& array = assert_cast<const ColumnArray&>(nullable_array.get_nested_column());
    EXPECT_EQ(array.get_offsets(), ColumnArray::Offsets64({2, 2, 2, 3, 4}));
    const auto& array_values = assert_cast<const ColumnNullable&>(array.get_data());
    const auto& array_strings = assert_cast<const ColumnString&>(array_values.get_nested_column());
    EXPECT_EQ(array_strings.get_data_at(0).to_string(), "small");
    EXPECT_EQ(array_strings.get_data_at(1).size, 4096);
    EXPECT_TRUE(array_values.is_null_at(2));
    EXPECT_EQ(array_strings.get_data_at(3).size, 4096);

    const auto& nullable_struct = assert_cast<const ColumnNullable&>(*output[2]);
    ASSERT_EQ(nullable_struct.size(), ROW_COUNT);
    EXPECT_TRUE(nullable_struct.is_null_at(1));
    const auto& struct_column =
            assert_cast<const ColumnStruct&>(nullable_struct.get_nested_column());
    const auto& payload = assert_cast<const ColumnNullable&>(struct_column.get_column(0));
    const auto& payload_strings = assert_cast<const ColumnString&>(payload.get_nested_column());
    EXPECT_EQ(payload_strings.get_data_at(0).to_string(), "small");
    EXPECT_EQ(payload_strings.get_data_at(2).size, 4096);
    EXPECT_TRUE(payload.is_null_at(3));
    EXPECT_EQ(payload_strings.get_data_at(4).size, 4096);
    const auto& ids = assert_cast<const ColumnNullable&>(struct_column.get_column(1));
    const auto& id_values = assert_cast<const ColumnInt32&>(ids.get_nested_column());
    EXPECT_EQ(id_values.get_element(0), 1);
    EXPECT_EQ(id_values.get_element(4), 4);

    ASSERT_NE(profile.get_counter("LevelOnlyReadTime"), nullptr);
    EXPECT_EQ(profile.get_counter("LevelOnlyReadTime")->value(), 0);
    ASSERT_NE(profile.get_counter("NativeReadCalls"), nullptr);
    EXPECT_GT(profile.get_counter("NativeReadCalls")->value(), 0);
    ASSERT_NE(profile.get_counter("NestedBatches"), nullptr);
    EXPECT_GT(profile.get_counter("NestedBatches")->value(), 0);
}

TEST_F(NewParquetReaderTest, SparseFilterPreservesNestedShapeAcrossPhysicalPages) {
    write_sparse_filter_nested_parquet_file(_file_path);

    auto physical_reader = ::parquet::ParquetFileReader::OpenFile(_file_path, false);
    auto row_group_metadata = physical_reader->metadata()->RowGroup(0);
    auto row_group_reader = physical_reader->RowGroup(0);
    for (const std::string path :
         {"m.key_value.key", "m.key_value.value", "s.items.list.element"}) {
        int column_ordinal = -1;
        for (int column = 0; column < row_group_metadata->num_columns(); ++column) {
            if (row_group_metadata->ColumnChunk(column)->path_in_schema()->ToDotString() == path) {
                column_ordinal = column;
                break;
            }
        }
        ASSERT_GE(column_ordinal, 0) << path;
        auto page_reader = row_group_reader->GetColumnPageReader(column_ordinal);
        bool saw_continuation_page = false;
        std::vector<int16_t> first_repetition_levels;
        while (auto page = page_reader->NextPage()) {
            if (page->type() != ::parquet::PageType::DATA_PAGE) {
                continue;
            }
            auto data_page = std::static_pointer_cast<::parquet::DataPageV1>(page);
            ::parquet::LevelDecoder repetition_decoder;
            repetition_decoder.SetData(data_page->repetition_level_encoding(), 1,
                                       data_page->num_values(), data_page->data(),
                                       data_page->size());
            int16_t first_repetition_level = 0;
            ASSERT_EQ(repetition_decoder.Decode(1, &first_repetition_level), 1);
            first_repetition_levels.push_back(first_repetition_level);
            saw_continuation_page |= first_repetition_level > 0;
        }
        EXPECT_TRUE(saw_continuation_page)
                << path << " must contain a parent row split across data pages; page starts: "
                << testing::PrintToString(first_repetition_levels);
    }
    physical_reader.reset();

    RuntimeProfile profile("sparse_filter_nested_pages");
    auto reader = create_reader(0, -1, &profile);
    reader->set_batch_size(2);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());
    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 3);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->non_predicate_columns = {field_projection(1), field_projection(2)};
    request->conjuncts.push_back(create_int32_greater_than_conjunct(0, 3));
    use_schema_order_positions(request.get(), schema);
    ASSERT_TRUE(reader->open(request).ok());

    MutableColumns output;
    for (const auto& field : schema) {
        output.push_back(field.type->create_column());
    }
    bool eof = false;
    while (!eof) {
        Block block = build_file_block(schema);
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        for (size_t column = 0; column < output.size(); ++column) {
            output[column]->insert_range_from(*block.get_by_position(column).column, 0, rows);
        }
    }

    const auto& ids = assert_cast<const ColumnNullable&>(*output[0]);
    const auto& id_values = assert_cast<const ColumnInt32&>(ids.get_nested_column());
    EXPECT_EQ(std::vector<int32_t>(id_values.get_data().begin(), id_values.get_data().end()),
              std::vector<int32_t>({4, 5, 6}));

    const auto& nullable_map = assert_cast<const ColumnNullable&>(*output[1]);
    EXPECT_EQ(nullable_map.get_null_map_data(), NullMap({0, 0, 0}));
    const auto& map = assert_cast<const ColumnMap&>(nullable_map.get_nested_column());
    EXPECT_EQ(map.get_offsets(),
              ColumnArray::Offsets64({0, SPANNING_NESTED_VALUES, SPANNING_NESTED_VALUES + 1}));
    const auto& map_keys = assert_cast<const ColumnNullable&>(map.get_keys());
    const auto& key_values = assert_cast<const ColumnInt32&>(map_keys.get_nested_column());
    ASSERT_EQ(key_values.size(), SPANNING_NESTED_VALUES + 1);
    EXPECT_EQ(std::count(map_keys.get_null_map_data().begin(), map_keys.get_null_map_data().end(),
                         uint8_t {1}),
              0);
    for (size_t value = 0; value < SPANNING_NESTED_VALUES; ++value) {
        EXPECT_EQ(key_values.get_element(value), 5000 + value);
    }
    EXPECT_EQ(key_values.get_element(SPANNING_NESTED_VALUES), 6000);
    const auto& map_values = assert_cast<const ColumnNullable&>(map.get_values());
    const auto& map_strings = assert_cast<const ColumnString&>(map_values.get_nested_column());
    EXPECT_EQ(std::count(map_values.get_null_map_data().begin(),
                         map_values.get_null_map_data().end(), uint8_t {1}),
              1);
    for (size_t value = 0; value < SPANNING_NESTED_VALUES; ++value) {
        EXPECT_EQ(map_strings.get_data_at(value).to_string(), "selected-wide-value");
    }
    EXPECT_TRUE(map_values.is_null_at(SPANNING_NESTED_VALUES));

    const auto& nullable_struct = assert_cast<const ColumnNullable&>(*output[2]);
    EXPECT_EQ(nullable_struct.get_null_map_data(), NullMap({0, 0, 0}));
    const auto& struct_column =
            assert_cast<const ColumnStruct&>(nullable_struct.get_nested_column());
    const auto& nullable_list = assert_cast<const ColumnNullable&>(struct_column.get_column(0));
    EXPECT_EQ(nullable_list.get_null_map_data(), NullMap({1, 0, 0}));
    const auto& list = assert_cast<const ColumnArray&>(nullable_list.get_nested_column());
    EXPECT_EQ(list.get_offsets(),
              ColumnArray::Offsets64({0, SPANNING_NESTED_VALUES, SPANNING_NESTED_VALUES + 2}));
    const auto& nullable_elements = assert_cast<const ColumnNullable&>(list.get_data());
    const auto& element_values =
            assert_cast<const ColumnInt32&>(nullable_elements.get_nested_column());
    ASSERT_EQ(element_values.size(), SPANNING_NESTED_VALUES + 2);
    for (size_t value = 0; value < SPANNING_NESTED_VALUES; ++value) {
        EXPECT_EQ(element_values.get_element(value), 5000 + value);
        EXPECT_FALSE(nullable_elements.is_null_at(value));
    }
    EXPECT_EQ(element_values.get_element(SPANNING_NESTED_VALUES), 6000);
    EXPECT_TRUE(nullable_elements.is_null_at(SPANNING_NESTED_VALUES + 1));
    const auto& markers = assert_cast<const ColumnNullable&>(struct_column.get_column(1));
    const auto& marker_values = assert_cast<const ColumnInt32&>(markers.get_nested_column());
    EXPECT_EQ(std::count(markers.get_null_map_data().begin(), markers.get_null_map_data().end(),
                         uint8_t {1}),
              0);
    EXPECT_EQ(
            std::vector<int32_t>(marker_values.get_data().begin(), marker_values.get_data().end()),
            std::vector<int32_t>({40, 50, 60}));

    ASSERT_NE(profile.get_counter("SelectedRows"), nullptr);
    EXPECT_EQ(profile.get_counter("SelectedRows")->value(), 3);
    ASSERT_NE(profile.get_counter("NestedBatches"), nullptr);
    EXPECT_GT(profile.get_counter("NestedBatches")->value(), 0);
}

TEST_F(NewParquetReaderTest, FullComplexChildUnderPartialParentReadsItsWholeSubtree) {
    write_nested_complex_under_struct_parquet_file(_file_path);
    for (size_t child_index = 0; child_index < 3; ++child_index) {
        auto reader = create_reader();
        RuntimeState state {TQueryOptions(), TQueryGlobals()};
        ASSERT_TRUE(reader->init(&state).ok());

        std::vector<format::ColumnDefinition> schema;
        ASSERT_TRUE(reader->get_schema(&schema).ok());
        ASSERT_EQ(schema.size(), 1);
        ASSERT_EQ(schema[0].children.size(), 4);
        auto projection = format::LocalColumnIndex::partial_local(0);
        projection.children.push_back(
                format::LocalColumnIndex::local(schema[0].children[child_index].file_local_id()));
        auto request = std::make_shared<format::FileScanRequest>();
        request->non_predicate_columns = {projection};
        request->local_positions.emplace(format::LocalColumnId(0), format::LocalIndex(0));
        ASSERT_TRUE(reader->open(request).ok());

        format::ColumnDefinition projected;
        ASSERT_TRUE(format::project_column_definition(schema[0], projection, &projected).ok());
        Block block;
        block.insert(ColumnWithTypeAndName(projected.type->create_column(), projected.type,
                                           projected.name));
        size_t rows = 0;
        bool eof = false;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        ASSERT_GT(rows, 0);
        const auto& outer = nullable_nested_column<ColumnStruct>(block, 0);
        ASSERT_EQ(outer.tuple_size(), 1);
        const auto& nullable_nested = assert_cast<const ColumnNullable&>(outer.get_column(0));
        if (child_index == 0) {
            const auto& nested =
                    assert_cast<const ColumnStruct&>(nullable_nested.get_nested_column());
            ASSERT_EQ(nested.tuple_size(), 2);
            const auto& payload = assert_cast<const ColumnNullable&>(nested.get_column(0));
            EXPECT_EQ(assert_cast<const ColumnString&>(payload.get_nested_column())
                              .get_data_at(0)
                              .to_string(),
                      "small");
            const auto& ids = assert_cast<const ColumnNullable&>(nested.get_column(1));
            EXPECT_EQ(ids.get_nested_column().get_int(0), 1);
        } else if (child_index == 1) {
            const auto& nested =
                    assert_cast<const ColumnArray&>(nullable_nested.get_nested_column());
            EXPECT_EQ(nested.get_offsets()[0], 2);
            EXPECT_EQ(nested.get_data().size(), 4);
        } else {
            const auto& nested = assert_cast<const ColumnMap&>(nullable_nested.get_nested_column());
            EXPECT_EQ(nested.get_offsets()[0], 1);
            EXPECT_EQ(nested.get_keys().size(), 3);
            EXPECT_EQ(nested.get_values().size(), 3);
        }
    }
}

TEST_F(NewParquetReaderTest, NativeNestedMapUsesOuterKeyRepetitionShape) {
    const char* source_root = std::getenv("ROOT");
    ASSERT_NE(source_root, nullptr);
    _file_path =
            std::string(source_root) + "/regression-test/data/external_table_p0/tvf/comp.parquet";
    ASSERT_TRUE(std::filesystem::exists(_file_path));

    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());
    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<format::FileScanRequest>();
    for (size_t position = 0; position < schema.size(); ++position) {
        request->non_predicate_columns.push_back(field_projection(cast_set<int32_t>(position)));
    }
    ASSERT_TRUE(reader->open(request).ok());

    size_t total_rows = 0;
    bool eof = false;
    while (!eof) {
        Block block = build_file_block(schema);
        size_t rows = 0;
        const Status status = reader->get_block(&block, &rows, &eof);
        ASSERT_TRUE(status.ok()) << status;
        total_rows += rows;
    }
    EXPECT_GT(total_rows, 0);
}

TEST_F(NewParquetReaderTest, NativeDecimalAndFixedBinaryMaterializeDirectly) {
    write_decimal_and_fixed_binary_parquet_file(_file_path);
    RuntimeProfile profile("native_decimal_fixed_binary_materialization");
    auto reader = create_reader(0, -1, &profile);
    reader->set_batch_size(2);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);
    auto request = std::make_shared<format::FileScanRequest>();
    request->non_predicate_columns = {field_projection(0), field_projection(1)};
    ASSERT_TRUE(reader->open(request).ok());

    MutableColumns output;
    output.reserve(schema.size());
    for (const auto& field : schema) {
        output.push_back(field.type->create_column());
    }
    bool eof = false;
    while (!eof) {
        Block block = build_file_block(schema);
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        for (size_t column = 0; column < output.size(); ++column) {
            output[column]->insert_range_from(*block.get_by_position(column).column, 0, rows);
        }
        reader->set_batch_size(3);
    }

    const auto& decimals = assert_cast<const ColumnDecimal128V3&>(
            assert_cast<const ColumnNullable&>(*output[0]).get_nested_column());
    ASSERT_EQ(decimals.size(), ROW_COUNT);
    EXPECT_EQ(decimals.get_element(0), Decimal128V3(1234567));
    EXPECT_EQ(decimals.get_element(1), Decimal128V3(-1));
    EXPECT_EQ(decimals.get_element(3), Decimal128V3(-987654321));

    const auto& fixed_values = assert_cast<const ColumnString&>(
            assert_cast<const ColumnNullable&>(*output[1]).get_nested_column());
    ASSERT_EQ(fixed_values.size(), ROW_COUNT);
    EXPECT_EQ(fixed_values.get_data_at(0).to_string(), "ABCD");
    EXPECT_EQ(fixed_values.get_data_at(1).to_string(), std::string("\0x\0y", 4));
    EXPECT_EQ(fixed_values.get_data_at(4).to_string(), std::string("\xff\x00\x7f\x80", 4));

    ASSERT_NE(profile.get_counter("LevelOnlyReadTime"), nullptr);
    EXPECT_EQ(profile.get_counter("LevelOnlyReadTime")->value(), 0);
    EXPECT_EQ(profile.get_counter("ConvertTime"), nullptr);
    ASSERT_NE(profile.get_counter("NativeReadCalls"), nullptr);
    EXPECT_GT(profile.get_counter("NativeReadCalls")->value(), 0);
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

TEST_F(NewParquetReaderTest, ComplexColumnDoesNotMisreportSiblingPagesAsCrossing) {
    write_struct_filter_parquet_file(_file_path);
    RuntimeProfile profile("new_parquet_reader_complex_page_fragments");
    auto reader = create_reader(0, -1, &profile);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<format::FileScanRequest>();
    request->non_predicate_columns = {field_projection(0)};
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
    EXPECT_EQ(total_rows, 4);
    // Each STRUCT child reads one page per Row Group, but no individual leaf crosses a boundary in
    // either batch. The aggregate fragment count remains useful without inflating crossing batches.
    EXPECT_EQ(profile.get_counter("NativePageFragments")->value(), 8);
    EXPECT_EQ(profile.get_counter("PageCrossingBatches")->value(), 0);
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

TEST_F(NewParquetReaderTest, ConditionCacheHitKeepsBatchAcrossAdjacentTrueGranules) {
    write_condition_cache_parquet_file(_file_path);
    auto reader = create_reader();
    reader->set_batch_size(ConditionCacheContext::GRANULE_SIZE * 2);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 1);

    auto request = std::make_shared<format::FileScanRequest>();
    request->non_predicate_columns = {field_projection(0)};
    ASSERT_TRUE(reader->open(request).ok());

    auto ctx = std::make_shared<ConditionCacheContext>();
    ctx->is_hit = true;
    ctx->filter_result = std::make_shared<std::vector<bool>>(std::vector<bool> {true, true});
    reader->set_condition_cache_context(ctx);

    Block block = build_file_block(schema);
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_FALSE(eof);
    EXPECT_EQ(rows, ConditionCacheContext::GRANULE_SIZE * 2);
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
    EXPECT_GT(profile.get_counter("PageReadCount")->value(), 0);
    EXPECT_EQ(profile.get_counter("PageCacheWriteCount")->value(), 0);
}

TEST_F(NewParquetReaderTest, NativeFooterCacheIdentityIncludesFilesystemAndVersion) {
    const auto first = format::parquet::detail::build_native_file_cache_key(
            "hdfs://nameservice-a", "/warehouse/shared.parquet", 10, 0, 1024, 1024, false);
    const auto other_fs = format::parquet::detail::build_native_file_cache_key(
            "hdfs://nameservice-b", "/warehouse/shared.parquet", 10, 0, 1024, 1024, false);
    const auto replaced = format::parquet::detail::build_native_file_cache_key(
            "hdfs://nameservice-a", "/warehouse/shared.parquet", 11, 0, 1024, 1024, false);
    EXPECT_FALSE(first.empty());
    EXPECT_NE(first, other_fs);
    EXPECT_NE(first, replaced);
}

TEST_F(NewParquetReaderTest, NativeFooterCacheDoesNotCrossFilesystems) {
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    RuntimeProfile first_profile("native_footer_cache_nameservice_a");
    auto first = create_reader(0, -1, &first_profile, false, nullptr, std::nullopt, false, false,
                               "hdfs://nameservice-a", 1234567);
    ASSERT_TRUE(first->init(&state).ok());
    EXPECT_EQ(first_profile.get_counter("FileFooterReadCalls")->value(), 1);
    EXPECT_EQ(first_profile.get_counter("FileFooterHitCache")->value(), 0);

    RuntimeProfile second_profile("native_footer_cache_nameservice_b");
    auto second = create_reader(0, -1, &second_profile, false, nullptr, std::nullopt, false, false,
                                "hdfs://nameservice-b", 1234567);
    ASSERT_TRUE(second->init(&state).ok());
    EXPECT_EQ(second_profile.get_counter("FileFooterReadCalls")->value(), 1);
    EXPECT_EQ(second_profile.get_counter("FileFooterHitCache")->value(), 0);
}

TEST_F(NewParquetReaderTest, NativeFooterCacheSkipsMutableUnknownVersion) {
    EXPECT_TRUE(format::parquet::detail::build_native_file_cache_key("hdfs://nameservice-a",
                                                                     "/warehouse/mutable.parquet",
                                                                     0, 0, 1024, 1024, false)
                        .empty());
    EXPECT_FALSE(
            format::parquet::detail::build_native_file_cache_key(
                    "hdfs://nameservice-a", "/warehouse/immutable.parquet", 0, 0, 1024, 1024, true)
                    .empty());
}

TEST_F(NewParquetReaderTest, NativeFooterCacheDoesNotReuseMutableUnknownVersion) {
    _file_path = (_test_dir / "mutable_footer_cache.parquet").string();
    write_parquet_file(_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};

    RuntimeProfile first_profile("native_footer_cache_mutable_first");
    auto first = create_reader(0, -1, &first_profile);
    ASSERT_TRUE(first->init(&state).ok());
    EXPECT_EQ(first_profile.get_counter("FileFooterReadCalls")->value(), 1);
    EXPECT_EQ(first_profile.get_counter("FileFooterHitCache")->value(), 0);

    write_parquet_file(_file_path);
    RuntimeProfile second_profile("native_footer_cache_mutable_second");
    auto second = create_reader(0, -1, &second_profile);
    ASSERT_TRUE(second->init(&state).ok());
    EXPECT_EQ(second_profile.get_counter("FileFooterReadCalls")->value(), 1);
    EXPECT_EQ(second_profile.get_counter("FileFooterHitCache")->value(), 0);
}

TEST_F(NewParquetReaderTest, NativeFooterSizeIsBoundedBeforeMetadataAllocation) {
    constexpr size_t file_size = 256UL << 20;
    constexpr size_t metadata_limit = 100UL << 20;
    const auto status = format::parquet::detail::validate_native_footer_size(
            static_cast<uint32_t>(metadata_limit + 1), file_size, metadata_limit);
    EXPECT_TRUE(status.is<ErrorCode::CORRUPTION>()) << status;
    EXPECT_NE(status.to_string().find("metadata limit"), std::string::npos);
}

TEST_F(NewParquetReaderTest, UnknownMtimeUsesPageCacheForImmutableFile) {
    _file_path = (_test_dir / "unknown_mtime_page_cache.parquet").string();
    write_parquet_file(_file_path);

    RuntimeProfile first_profile("new_parquet_reader_first_unknown_mtime");
    {
        auto reader = create_reader(0, -1, &first_profile, false, nullptr, std::nullopt, true);
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
    auto reader = create_reader(0, -1, &second_profile, false, nullptr, std::nullopt, true);
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
    ASSERT_NE(profile.get_counter("DenseBatches"), nullptr);
    ASSERT_NE(profile.get_counter("SelectedBatches"), nullptr);
    ASSERT_NE(profile.get_counter("EmptySelectionBatches"), nullptr);
    ASSERT_NE(profile.get_counter("ReaderReadRows"), nullptr);
    ASSERT_NE(profile.get_counter("ReaderSkipRows"), nullptr);
    ASSERT_NE(profile.get_counter("ReaderSelectRows"), nullptr);
    ASSERT_NE(profile.get_counter("LevelOnlyReadTime"), nullptr);
    ASSERT_NE(profile.get_counter("MaterializationTime"), nullptr);
    ASSERT_NE(profile.get_counter("NativeReadCalls"), nullptr);
    ASSERT_NE(profile.get_counter("FileFooterReadCalls"), nullptr);
    ASSERT_NE(profile.get_counter("FileFooterHitCache"), nullptr);
    ASSERT_GT(profile.get_counter("FileReaderCreateTime")->value(), 0);
    EXPECT_EQ(profile.get_counter("FileNum")->value(), 1);
    EXPECT_EQ(profile.get_counter("RawRowsRead")->value(), ROW_COUNT);
    EXPECT_EQ(profile.get_counter("SelectedRows")->value(), 3);
    EXPECT_EQ(profile.get_counter("RowsFilteredByConjunct")->value(), 2);
    TRuntimeProfileTree profile_tree;
    profile.to_thrift(&profile_tree);
    ASSERT_FALSE(profile_tree.nodes.empty());
    const auto parquet_children =
            profile_tree.nodes.front().child_counters_map.find("ParquetReader");
    ASSERT_NE(parquet_children, profile_tree.nodes.front().child_counters_map.end());
    EXPECT_TRUE(parquet_children->second.contains("RowsFilteredByConjunct"));
    EXPECT_EQ(profile.get_counter("TotalBatches")->value(), 1);
    EXPECT_EQ(profile.get_counter("DenseBatches")->value(), 0);
    EXPECT_EQ(profile.get_counter("SelectedBatches")->value(), 1);
    EXPECT_EQ(profile.get_counter("EmptySelectionBatches")->value(), 0);
    EXPECT_EQ(profile.get_counter("ReaderReadRows")->value(), ROW_COUNT + 3);
    EXPECT_EQ(profile.get_counter("ReaderSkipRows")->value(), 2);
    EXPECT_EQ(profile.get_counter("ReaderSelectRows")->value(), 3);
    EXPECT_EQ(profile.get_counter("LevelOnlyReadTime")->value(), 0);
    EXPECT_GT(profile.get_counter("MaterializationTime")->value(), 0);
    EXPECT_GT(profile.get_counter("NativeReadCalls")->value(), 0);
    EXPECT_EQ(profile.get_counter("FileFooterReadCalls")->value() +
                      profile.get_counter("FileFooterHitCache")->value(),
              1);

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
        EXPECT_EQ(location.file_local.file_id, context.file_id);
        EXPECT_EQ(location.file_local.row_id, static_cast<uint32_t>(row + 2));
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
    ASSERT_NE(profile.get_counter("DenseBatches"), nullptr);
    ASSERT_NE(profile.get_counter("SelectedBatches"), nullptr);
    ASSERT_NE(profile.get_counter("EmptySelectionBatches"), nullptr);
    EXPECT_EQ(profile.get_counter("RawRowsRead")->value(), ROW_COUNT);
    EXPECT_EQ(profile.get_counter("SelectedRows")->value(), 0);
    EXPECT_EQ(profile.get_counter("RowsFilteredByConjunct")->value(), ROW_COUNT);
    EXPECT_EQ(profile.get_counter("TotalBatches")->value(), 1);
    EXPECT_EQ(profile.get_counter("DenseBatches")->value(), 0);
    EXPECT_EQ(profile.get_counter("SelectedBatches")->value(), 0);
    EXPECT_EQ(profile.get_counter("EmptySelectionBatches")->value(), 1);
}

TEST_F(NewParquetReaderTest, ProfileNestsFormatReaderBelowFileReaderAndRecordsTotalTime) {
    RuntimeProfile profile("new_parquet_reader_hierarchy_profile");
    auto reader = create_reader(0, -1, &profile);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    Block block = build_file_block(schema);
    auto request = std::make_shared<format::FileScanRequest>();
    request->non_predicate_columns = {field_projection(0), field_projection(1)};
    use_schema_order_positions(request.get(), schema);
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());

    auto* parquet_total = profile.get_counter("ParquetReader");
    ASSERT_NE(parquet_total, nullptr);
    EXPECT_GT(parquet_total->value(), 0);

    TRuntimeProfileTree tree;
    profile.to_thrift(&tree, 3);
    ASSERT_FALSE(tree.nodes.empty());
    const auto& children = tree.nodes[0].child_counters_map;
    ASSERT_TRUE(children.contains(RuntimeProfile::ROOT_COUNTER));
    EXPECT_TRUE(children.at(RuntimeProfile::ROOT_COUNTER).contains("FileScannerV2"));
    ASSERT_TRUE(children.contains("FileScannerV2"));
    EXPECT_TRUE(children.at("FileScannerV2").contains("TableReader"));
    ASSERT_TRUE(children.contains("TableReader"));
    EXPECT_TRUE(children.at("TableReader").contains("FileReader"));
    ASSERT_TRUE(children.contains("FileReader"));
    EXPECT_TRUE(children.at("FileReader").contains("IO"));
    EXPECT_TRUE(children.at("FileReader").contains("ParquetReader"));
    ASSERT_TRUE(children.contains("ParquetReader"));
    EXPECT_TRUE(children.at("ParquetReader").contains("ColumnReadTime"));
    EXPECT_TRUE(children.at("ParquetReader").contains("RowGroupsReadNum"));
    EXPECT_TRUE(children.at("ParquetReader").contains("FilteredRowsByGroup"));
    EXPECT_TRUE(children.at("ParquetReader").contains("FilteredBytes"));
    EXPECT_TRUE(children.at("ParquetReader").contains("FileNum"));
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

TEST_F(NewParquetReaderTest, DictionaryPruningPublishesColdAndWarmNativePageProfile) {
    const double old_cache_threshold = config::parquet_page_cache_decompress_threshold;
    config::parquet_page_cache_decompress_threshold = 100.0;
    Defer restore_cache_threshold {
            [&] { config::parquet_page_cache_decompress_threshold = old_cache_threshold; }};
    write_dictionary_filter_parquet_file(_file_path, ::parquet::Compression::SNAPPY);

    auto open_pruned_reader = [&](RuntimeProfile* profile) {
        auto reader = create_reader(0, -1, profile, false, nullptr, std::nullopt, true);
        TQueryOptions query_options;
        query_options.__set_enable_parquet_file_page_cache(true);
        RuntimeState state {query_options, TQueryGlobals()};
        RETURN_IF_ERROR(reader->init(&state));
        std::vector<format::ColumnDefinition> schema;
        RETURN_IF_ERROR(reader->get_schema(&schema));
        auto request = std::make_shared<format::FileScanRequest>();
        request->predicate_columns = {field_projection(1)};
        request->non_predicate_columns = {field_projection(0)};
        request->conjuncts.push_back(create_string_in_conjunct(1, {"not-present"}));
        use_schema_order_positions(request.get(), schema);
        RETURN_IF_ERROR(reader->open(request));
        EXPECT_EQ(profile->get_counter("RowGroupsFilteredByDictionary")->value(), 0);
        EXPECT_EQ(profile->get_counter("PageReadCount")->value(), 0);
        Block block = build_file_block(schema);
        size_t rows = 0;
        bool eof = false;
        // Expensive dictionary probes are current-row-group work now, so advance the scheduler
        // once before inspecting page-cache and pruning counters.
        return reader->get_block(&block, &rows, &eof);
    };

    RuntimeProfile cold_profile("dictionary_pruning_cold_profile");
    ASSERT_TRUE(open_pruned_reader(&cold_profile).ok());
    for (const auto* counter_name :
         {"RowGroupsFilteredByDictionary", "PageReadCount", "PageCacheWriteCount",
          "ParsePageHeaderNum", "DecompressCount", "DecodeDictTime"}) {
        ASSERT_NE(cold_profile.get_counter(counter_name), nullptr) << counter_name;
        EXPECT_GT(cold_profile.get_counter(counter_name)->value(), 0) << counter_name;
    }

    RuntimeProfile warm_profile("dictionary_pruning_warm_profile");
    ASSERT_TRUE(open_pruned_reader(&warm_profile).ok());
    for (const auto* counter_name : {"RowGroupsFilteredByDictionary", "PageReadCount",
                                     "PageCacheHitCount", "ParsePageHeaderNum", "DecodeDictTime"}) {
        ASSERT_NE(warm_profile.get_counter(counter_name), nullptr) << counter_name;
        EXPECT_GT(warm_profile.get_counter(counter_name)->value(), 0) << counter_name;
    }
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

TEST_F(NewParquetReaderTest, FixedWidthDictionaryPredicateFiltersRowsByDictionaryId) {
    write_fixed_width_dictionary_filter_parquet_file(_file_path);

    RuntimeProfile profile("new_parquet_reader_fixed_width_dictionary_filter_profile");
    auto reader = create_reader(0, -1, &profile);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(1)};
    request->non_predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(create_int32_dictionary_equals_conjunct(1, 20));
    use_schema_order_positions(request.get(), schema);
    ASSERT_TRUE(reader->open(request).ok());

    std::vector<int32_t> ids;
    std::vector<int32_t> values;
    bool eof = false;
    while (!eof) {
        Block block = build_file_block(schema);
        size_t rows = 0;
        const auto status = reader->get_block(&block, &rows, &eof);
        ASSERT_TRUE(status.ok()) << status;
        if (rows == 0) {
            continue;
        }
        const auto& id_column = nullable_nested_column<ColumnInt32>(block, 0);
        const auto& value_column = nullable_nested_column<ColumnInt32>(block, 1);
        for (size_t row = 0; row < rows; ++row) {
            ids.push_back(id_column.get_element(row));
            values.push_back(value_column.get_element(row));
        }
    }

    EXPECT_EQ(ids, std::vector<int32_t>({2, 4, 6}));
    EXPECT_EQ(values, std::vector<int32_t>({20, 20, 20}));
    EXPECT_EQ(profile.get_counter("RowsFilteredByDictFilter")->value(), 3);
    EXPECT_EQ(profile.get_counter("DictFilterCandidateColumns")->value(), 1);
    EXPECT_EQ(profile.get_counter("DictFilterColumns")->value(), 1);
    EXPECT_EQ(profile.get_counter("DictFilterUnsupportedColumns")->value(), 0);
    EXPECT_EQ(profile.get_counter("DictFilterReadFailures")->value(), 0);
}

TEST_F(NewParquetReaderTest, AllFixedWidthDictionaryTypesDecodeThroughDictionaryIds) {
    write_all_fixed_width_dictionary_filter_parquet_file(_file_path);

    auto parquet_file_reader = ::parquet::ParquetFileReader::OpenFile(_file_path, false);
    auto row_group = parquet_file_reader->metadata()->RowGroup(0);
    ASSERT_NE(row_group, nullptr);
    ASSERT_EQ(row_group->num_columns(), 6);
    const std::array<::parquet::Type::type, 5> expected_types {
            ::parquet::Type::INT64, ::parquet::Type::FLOAT, ::parquet::Type::DOUBLE,
            ::parquet::Type::FIXED_LEN_BYTE_ARRAY, ::parquet::Type::INT96};
    for (int column = 1; column < row_group->num_columns(); ++column) {
        ASSERT_TRUE(row_group->ColumnChunk(column)->has_dictionary_page()) << column;
        EXPECT_EQ(row_group->ColumnChunk(column)->type(), expected_types[column - 1]) << column;
    }

    RuntimeProfile profile("new_parquet_reader_all_fixed_width_dictionary_filter_profile");
    auto reader = create_reader(0, -1, &profile);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 6);
    auto request = std::make_shared<format::FileScanRequest>();
    request->non_predicate_columns = {field_projection(0)};
    for (int column = 1; column < 6; ++column) {
        request->predicate_columns.push_back(field_projection(column));
        request->conjuncts.push_back(create_dictionary_accept_all_conjunct(column));
    }
    use_schema_order_positions(request.get(), schema);
    ASSERT_TRUE(reader->open(request).ok());

    size_t total_rows = 0;
    bool eof = false;
    while (!eof) {
        Block block = build_file_block(schema);
        size_t rows = 0;
        const auto status = reader->get_block(&block, &rows, &eof);
        ASSERT_TRUE(status.ok()) << status;
        total_rows += rows;
    }

    EXPECT_EQ(total_rows, 6);
    EXPECT_EQ(profile.get_counter("RowsFilteredByDictFilter")->value(), 0);
    EXPECT_EQ(profile.get_counter("DictFilterCandidateColumns")->value(), 5);
    EXPECT_EQ(profile.get_counter("DictFilterColumns")->value(), 5);
    EXPECT_EQ(profile.get_counter("DictFilterUnsupportedColumns")->value(), 0);
    EXPECT_EQ(profile.get_counter("DictFilterReadFailures")->value(), 0);
}

TEST_F(NewParquetReaderTest, DictionaryPredicateReaderIsSharedOutsideMergeRangeReader) {
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
    // The native dictionary probe keeps its reader and cursor for predicate data pages. Other
    // projected chunks still use the row-group merge reader, so probing never duplicates the
    // dictionary read or perturbs the merge range's sequential access order.
    ASSERT_NE(profile.get_counter("MergedIO"), nullptr);
    ASSERT_NE(profile.get_counter("MergedBytes"), nullptr);
    EXPECT_GT(profile.get_counter("MergedIO")->value(), 0);
    ASSERT_NE(profile.get_counter("NativeReadCalls"), nullptr);
    EXPECT_GT(profile.get_counter("NativeReadCalls")->value(), 0);
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
    // Five dictionary ids are rejected before the residual predicate; the remaining predicate
    // reader then skips all six logical rows once the selection is empty.
    EXPECT_EQ(profile.get_counter("ReaderSkipRows")->value(), 11);
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

// Scenario: the selected range starts after page-index-pruned rows. The scheduler defers that range
// gap for the non-predicate payload reader, then flushes it exactly once before materialization. The
// native RowRanges/OffsetIndex plan advances both readers without decoding the rejected pages or
// double-skipping row 64.
TEST_F(NewParquetReaderTest, PageIndexFilteredGapFlushesPendingOutputSkipOnce) {
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
        auto status = reader->get_block(&block, &rows, &eof);
        ASSERT_TRUE(status.ok()) << status;
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
    ASSERT_NE(profile.get_counter("LevelOnlySkipTime"), nullptr);
    ASSERT_NE(profile.get_counter("RowGroupFilterTime"), nullptr);
    ASSERT_NE(profile.get_counter("PageIndexFilterTime"), nullptr);
    ASSERT_NE(profile.get_counter("PageIndexReadTime"), nullptr);
    EXPECT_GT(profile.get_counter("PagesSkippedByDataPageFilter")->value(), 0);
    EXPECT_GT(profile.get_counter("DataPageFilterSkipBytes")->value(), 0);
    EXPECT_EQ(profile.get_counter("RawRowsRead")->value(), 64);
    EXPECT_EQ(profile.get_counter("SelectedRows")->value(), 64);
    EXPECT_GT(profile.get_counter("RangeGapSkippedRows")->value(), 0);
    EXPECT_EQ(profile.get_counter("ReaderSkipRows")->value(), 0);
    EXPECT_EQ(profile.get_counter("LevelOnlySkipTime")->value(), 0);
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
