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

#include "format_v2/parquet/parquet_statistics.h"

#include <gtest/gtest.h>

#include <bit>
#include <cstdint>
#include <cstring>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_date.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_time.h"
#include "core/data_type/data_type_variant_v2.h"
#include "core/field.h"
#include "exprs/create_predicate_function.h"
#include "exprs/expr_zonemap_filter.h"
#include "exprs/function/functions_comparison.h"
#include "exprs/hybrid_set.h"
#include "exprs/hybrid_set_min_max.h"
#include "exprs/vcompound_pred.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "exprs/vliteral.h"
#include "exprs/vslot_ref.h"
#include "format_v2/file_reader.h"
#include "format_v2/parquet/parquet_column_schema.h"
#include "format_v2/parquet/parquet_file_context.h"
#include "format_v2/parquet/reader/native/block_split_bloom_filter.h"
#include "io/fs/file_reader.h"
#include "runtime/runtime_state.h"
#include "util/thrift_util.h"
namespace doris {
namespace {

class StatisticsMemoryFileReader final : public io::FileReader {
public:
    explicit StatisticsMemoryFileReader(std::vector<uint8_t> bytes, bool fail_reads = false)
            : _bytes(std::move(bytes)),
              _path("native-bloom-filter.parquet"),
              _fail_reads(fail_reads) {}

    Status close() override {
        _closed = true;
        return Status::OK();
    }
    const io::Path& path() const override { return _path; }
    size_t size() const override { return _bytes.size(); }
    bool closed() const override { return _closed; }
    int64_t mtime() const override { return 1; }
    int read_count() const { return _read_count; }

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const io::IOContext*) override {
        ++_read_count;
        if (_fail_reads) {
            return Status::IOError("injected native Bloom read failure");
        }
        if (offset > _bytes.size() || result.size > _bytes.size() - offset) {
            return Status::IOError("native Bloom test read exceeds memory file");
        }
        memcpy(result.data, _bytes.data() + offset, result.size);
        *bytes_read = result.size;
        return Status::OK();
    }

private:
    std::vector<uint8_t> _bytes;
    io::Path _path;
    bool _closed = false;
    bool _fail_reads = false;
    int _read_count = 0;
};

std::shared_ptr<HybridSetBase> hybrid_set_from_fields(const DataTypePtr& data_type,
                                                      const std::vector<Field>& values) {
    const auto primitive_type = remove_nullable(data_type)->get_primitive_type();
    std::shared_ptr<HybridSetBase> set(create_set(primitive_type, false));
    for (const auto& field : values) {
        if (field.is_null()) {
            continue;
        }
        DORIS_CHECK(expr_zonemap::field_types_compatible(field.get_type(), primitive_type));
        switch (primitive_type) {
        case TYPE_BOOLEAN:
            set->insert(&field.get<TYPE_BOOLEAN>());
            break;
        case TYPE_INT:
            set->insert(&field.get<TYPE_INT>());
            break;
        case TYPE_BIGINT:
            set->insert(&field.get<TYPE_BIGINT>());
            break;
        case TYPE_FLOAT:
            set->insert(&field.get<TYPE_FLOAT>());
            break;
        case TYPE_DOUBLE:
            set->insert(&field.get<TYPE_DOUBLE>());
            break;
        case TYPE_CHAR:
        case TYPE_VARCHAR:
        case TYPE_STRING: {
            const auto value = field.as_string_view();
            StringRef ref(value.data(), value.size());
            set->insert(&ref);
            break;
        }
        default:
            DORIS_CHECK(false) << "Unsupported Bloom IN test type " << primitive_type;
        }
    }
    return set;
}

class BloomInExpr final : public VExpr {
public:
    BloomInExpr(int column_id, DataTypePtr data_type, std::vector<Field> values)
            : BloomInExpr(VSlotRef::create_shared(0, column_id, -1, data_type, "c0"),
                          hybrid_set_from_fields(data_type, values)) {}

    BloomInExpr(VExprSPtr probe, std::vector<Field> values)
            : BloomInExpr(probe, hybrid_set_from_fields(probe->data_type(), values)) {}

    BloomInExpr(VExprSPtr probe, std::shared_ptr<HybridSetBase> values)
            : VExpr(std::make_shared<DataTypeUInt8>(), false), _values(std::move(values)) {
        add_child(std::move(probe));
    }

    const std::string& expr_name() const override { return _expr_name; }

    Status execute_column_impl(VExprContext*, const Block*, const Selector*, size_t,
                               ColumnPtr&) const override {
        return Status::InternalError("BloomInExpr is only used by parquet statistics tests");
    }

    bool can_evaluate_bloom_filter() const override { return true; }

    ZoneMapFilterResult evaluate_bloom_filter(const BloomFilterEvalContext& ctx) const override {
        return expr_zonemap::eval_in_bloom_filter(ctx, get_child(0), false, *_values);
    }

    void collect_slot_column_ids(std::set<int>& column_ids) const override {
        get_child(0)->collect_slot_column_ids(column_ids);
    }

private:
    std::shared_ptr<HybridSetBase> _values;
    const std::string _expr_name = "BloomInExpr";
};

class BloomEqExpr final : public VExpr {
public:
    BloomEqExpr(int column_id, DataTypePtr data_type, Field value)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _slot(VSlotRef::create_shared(0, column_id, -1, std::move(data_type), "c0")),
              _value(std::move(value)) {}

    const std::string& expr_name() const override { return _expr_name; }
    Status execute_column_impl(VExprContext*, const Block*, const Selector*, size_t,
                               ColumnPtr&) const override {
        return Status::InternalError("BloomEqExpr is only used by parquet statistics tests");
    }
    bool can_evaluate_bloom_filter() const override { return true; }
    ZoneMapFilterResult evaluate_bloom_filter(const BloomFilterEvalContext& ctx) const override {
        return expr_zonemap::eval_eq_bloom_filter(
                ctx, expr_zonemap::SlotLiteral {.slot_index = _slot->column_id(),
                                                .slot_type = _slot->data_type(),
                                                .literal = _value,
                                                .literal_type = _slot->data_type(),
                                                .literal_on_left = false});
    }
    void collect_slot_column_ids(std::set<int>& column_ids) const override {
        _slot->collect_slot_column_ids(column_ids);
    }

private:
    std::shared_ptr<VSlotRef> _slot;
    Field _value;
    const std::string _expr_name = "BloomEqExpr";
};

class MetadataFloatingEqualityExpr final : public VExpr {
public:
    enum class Mode { EQ, IN, NE, GT, GE, REVERSED_LT, REVERSED_LE, NOT_IN };

    MetadataFloatingEqualityExpr(int column_id, DataTypePtr data_type, Field nan_value, Mode mode)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _slot(VSlotRef::create_shared(0, column_id, -1, data_type, "c0")),
              _nan_literal(VLiteral::create_shared(create_texpr_node_from(
                      nan_value, remove_nullable(data_type)->get_primitive_type(), 0, 0))),
              _mode(mode),
              _values {Field::create_field<TYPE_DOUBLE>(10.0), std::move(nan_value)} {
        const auto primitive_type = remove_nullable(data_type)->get_primitive_type();
        if (primitive_type == TYPE_FLOAT) {
            _values[0] = Field::create_field<TYPE_FLOAT>(10.0F);
            _zero_literal = VLiteral::create_shared(create_texpr_node_from(
                    Field::create_field<TYPE_FLOAT>(0.0F), TYPE_FLOAT, 0, 0));
            _one_literal = VLiteral::create_shared(create_texpr_node_from(
                    Field::create_field<TYPE_FLOAT>(1.0F), TYPE_FLOAT, 0, 0));
            _not_in_values = {Field::create_field<TYPE_FLOAT>(0.0F)};
        } else {
            _zero_literal = VLiteral::create_shared(create_texpr_node_from(
                    Field::create_field<TYPE_DOUBLE>(0.0), TYPE_DOUBLE, 0, 0));
            _one_literal = VLiteral::create_shared(create_texpr_node_from(
                    Field::create_field<TYPE_DOUBLE>(1.0), TYPE_DOUBLE, 0, 0));
            _not_in_values = {Field::create_field<TYPE_DOUBLE>(0.0)};
        }
        _values_set = hybrid_set_from_fields(data_type, _values);
        _not_in_values_set = hybrid_set_from_fields(data_type, _not_in_values);
        expr_zonemap::get_hybrid_set_min_max_for_zonemap_filter(_values_set, data_type,
                                                                _values_min_max);
        expr_zonemap::get_hybrid_set_min_max_for_zonemap_filter(_not_in_values_set, data_type,
                                                                _not_in_values_min_max);
    }

    const std::string& expr_name() const override { return _expr_name; }
    Status execute_column_impl(VExprContext*, const Block*, const Selector*, size_t,
                               ColumnPtr&) const override {
        return Status::InternalError("MetadataFloatingEqualityExpr is metadata-only");
    }
    bool can_evaluate_zonemap_filter() const override { return true; }
    ZoneMapFilterResult evaluate_zonemap_filter(const ZoneMapEvalContext& ctx) const override {
        switch (_mode) {
        case Mode::EQ:
            return comparison_zonemap_detail::evaluate(ctx, {_slot, _nan_literal},
                                                       comparison_zonemap_detail::Op::EQ);
        case Mode::IN:
            return expr_zonemap::eval_in_zonemap(ctx, _slot, false, _values_min_max, *_values_set);
        case Mode::NE:
            return comparison_zonemap_detail::evaluate(ctx, {_slot, _zero_literal},
                                                       comparison_zonemap_detail::Op::NE);
        case Mode::GT:
            return comparison_zonemap_detail::evaluate(ctx, {_slot, _one_literal},
                                                       comparison_zonemap_detail::Op::GT);
        case Mode::GE:
            return comparison_zonemap_detail::evaluate(ctx, {_slot, _one_literal},
                                                       comparison_zonemap_detail::Op::GE);
        case Mode::REVERSED_LT:
            return comparison_zonemap_detail::evaluate(ctx, {_one_literal, _slot},
                                                       comparison_zonemap_detail::Op::LT);
        case Mode::REVERSED_LE:
            return comparison_zonemap_detail::evaluate(ctx, {_one_literal, _slot},
                                                       comparison_zonemap_detail::Op::LE);
        case Mode::NOT_IN:
            return expr_zonemap::eval_in_zonemap(ctx, _slot, true, _not_in_values_min_max,
                                                 *_not_in_values_set);
        }
        __builtin_unreachable();
    }
    void collect_slot_column_ids(std::set<int>& column_ids) const override {
        _slot->collect_slot_column_ids(column_ids);
    }

private:
    VExprSPtr _slot;
    VExprSPtr _nan_literal;
    VExprSPtr _zero_literal;
    VExprSPtr _one_literal;
    Mode _mode;
    std::vector<Field> _values;
    std::vector<Field> _not_in_values;
    std::shared_ptr<HybridSetBase> _values_set;
    std::shared_ptr<HybridSetBase> _not_in_values_set;
    HybridSetMinMax _values_min_max;
    HybridSetMinMax _not_in_values_min_max;
    const std::string _expr_name = "MetadataFloatingEqualityExpr";
};

class MetadataAccessorExpr final : public VExpr {
public:
    MetadataAccessorExpr(DataTypePtr result_type, VExprSPtr parent, VExprSPtr selector)
            : VExpr(std::move(result_type), false) {
        _fn.name.function_name = "element_at";
        add_child(std::move(parent));
        add_child(std::move(selector));
    }

    const std::string& expr_name() const override { return _expr_name; }

    Status execute_column_impl(VExprContext*, const Block*, const Selector*, size_t,
                               ColumnPtr&) const override {
        return Status::InternalError("MetadataAccessorExpr is metadata-only");
    }

private:
    const std::string _expr_name = "MetadataAccessorExpr";
};

class DictionaryStringInExpr final : public VExpr {
public:
    DictionaryStringInExpr() : VExpr(std::make_shared<DataTypeUInt8>(), false) {}

    const std::string& expr_name() const override { return _expr_name; }

    Status execute_column_impl(VExprContext*, const Block*, const Selector*, size_t,
                               ColumnPtr&) const override {
        return Status::InternalError("DictionaryStringInExpr is metadata-only");
    }

    bool can_evaluate_dictionary_filter() const override { return true; }

    ZoneMapFilterResult evaluate_dictionary_filter(const DictionaryEvalContext&) const override {
        return ZoneMapFilterResult::kNoMatch;
    }

    void collect_slot_column_ids(std::set<int>& column_ids) const override { column_ids.insert(0); }

private:
    const std::string _expr_name = "DictionaryStringInExpr";
};

class MetadataInt32GreaterThanExpr final : public VExpr {
public:
    explicit MetadataInt32GreaterThanExpr(int32_t value)
            : VExpr(std::make_shared<DataTypeUInt8>(), false), _value(value) {}

    const std::string& expr_name() const override { return _expr_name; }
    Status execute_column_impl(VExprContext*, const Block*, const Selector*, size_t,
                               ColumnPtr&) const override {
        return Status::InternalError("MetadataInt32GreaterThanExpr is metadata-only");
    }
    bool can_evaluate_zonemap_filter() const override { return true; }
    void collect_slot_column_ids(std::set<int>& column_ids) const override { column_ids.insert(0); }
    ZoneMapFilterResult evaluate_zonemap_filter(const ZoneMapEvalContext& ctx) const override {
        const auto zone_map = ctx.zone_map(0);
        if (zone_map == nullptr) {
            return unsupported_zonemap_filter(ctx);
        }
        if (!zone_map->has_not_null) {
            return ZoneMapFilterResult::kNoMatch;
        }
        return zone_map->max_value <= Field::create_field<TYPE_INT>(_value)
                       ? ZoneMapFilterResult::kNoMatch
                       : ZoneMapFilterResult::kMayMatch;
    }

private:
    int32_t _value;
    const std::string _expr_name = "MetadataInt32GreaterThanExpr";
};

class MetadataSlotInt32GreaterThanExpr final : public VExpr {
public:
    MetadataSlotInt32GreaterThanExpr(int slot_index, int32_t value)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _slot_index(slot_index),
              _value(value) {}

    const std::string& expr_name() const override { return _expr_name; }
    Status execute_column_impl(VExprContext*, const Block*, const Selector*, size_t,
                               ColumnPtr&) const override {
        return Status::InternalError("MetadataSlotInt32GreaterThanExpr is metadata-only");
    }
    bool can_evaluate_zonemap_filter() const override { return true; }
    void collect_slot_column_ids(std::set<int>& column_ids) const override {
        column_ids.insert(_slot_index);
    }
    ZoneMapFilterResult evaluate_zonemap_filter(const ZoneMapEvalContext& ctx) const override {
        const auto zone_map = ctx.zone_map(_slot_index);
        if (zone_map == nullptr) {
            return unsupported_zonemap_filter(ctx);
        }
        if (!zone_map->has_not_null) {
            return ZoneMapFilterResult::kNoMatch;
        }
        return zone_map->max_value <= Field::create_field<TYPE_INT>(_value)
                       ? ZoneMapFilterResult::kNoMatch
                       : ZoneMapFilterResult::kMayMatch;
    }

private:
    int _slot_index;
    int32_t _value;
    const std::string _expr_name = "MetadataSlotInt32GreaterThanExpr";
};

class MetadataBoundsProbeExpr final : public VExpr {
public:
    explicit MetadataBoundsProbeExpr(bool require_false_boolean = false)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _require_false_boolean(require_false_boolean) {}

    const std::string& expr_name() const override { return _expr_name; }
    Status execute_column_impl(VExprContext*, const Block*, const Selector*, size_t,
                               ColumnPtr&) const override {
        return Status::InternalError("MetadataBoundsProbeExpr is metadata-only");
    }
    bool can_evaluate_zonemap_filter() const override { return true; }
    void collect_slot_column_ids(std::set<int>& column_ids) const override { column_ids.insert(0); }
    ZoneMapFilterResult evaluate_zonemap_filter(const ZoneMapEvalContext& ctx) const override {
        const auto zone_map = ctx.zone_map(0);
        if (zone_map == nullptr || !zone_map->has_not_null || zone_map->min_value.is_null() ||
            zone_map->max_value.is_null()) {
            return ZoneMapFilterResult::kMayMatch;
        }
        if (_require_false_boolean &&
            zone_map->min_value == Field::create_field<TYPE_BOOLEAN>(false) &&
            zone_map->max_value == Field::create_field<TYPE_BOOLEAN>(false)) {
            return ZoneMapFilterResult::kMayMatch;
        }
        return ZoneMapFilterResult::kNoMatch;
    }

private:
    bool _require_false_boolean;
    const std::string _expr_name = "MetadataBoundsProbeExpr";
};

class VariantPathTestExpr final : public VExpr {
public:
    VariantPathTestExpr(std::string name, DataTypePtr type,
                        TExprNodeType::type node_type = TExprNodeType::FUNCTION_CALL)
            : VExpr(std::move(type), false), _name(std::move(name)) {
        set_node_type(node_type);
    }

    const std::string& expr_name() const override { return _name; }
    Status execute_column_impl(VExprContext*, const Block*, const Selector*, size_t,
                               ColumnPtr&) const override {
        return Status::InternalError("VariantPathTestExpr is metadata-only");
    }

private:
    std::string _name;
};

VExprContextSPtr variant_path_gt_conjunct(int32_t literal_value,
                                          bool add_narrowing_intermediate_cast = false,
                                          bool decimal_comparison = false) {
    auto slot = VSlotRef::create_shared(0, 0, -1,
                                        make_nullable(std::make_shared<DataTypeVariantV2>()), "v");
    auto key = VLiteral::create_shared(std::make_shared<DataTypeString>(),
                                       Field::create_field<TYPE_STRING>("col"));
    auto element_at = std::make_shared<VariantPathTestExpr>(
            "element_at", make_nullable(std::make_shared<DataTypeVariantV2>()));
    element_at->add_child(slot);
    element_at->add_child(key);
    DataTypePtr comparison_type = decimal_comparison
                                          ? DataTypePtr(std::make_shared<DataTypeDecimal128>(38, 9))
                                          : DataTypePtr(std::make_shared<DataTypeInt32>());
    auto cast = std::make_shared<VariantPathTestExpr>("CAST", make_nullable(comparison_type),
                                                      TExprNodeType::CAST_EXPR);
    if (add_narrowing_intermediate_cast) {
        auto narrowing = std::make_shared<VariantPathTestExpr>(
                "CAST", make_nullable(std::make_shared<DataTypeInt8>()), TExprNodeType::CAST_EXPR);
        narrowing->add_child(element_at);
        cast->add_child(narrowing);
    } else {
        cast->add_child(element_at);
    }
    auto literal = decimal_comparison
                           ? VLiteral::create_shared(
                                     comparison_type,
                                     Field::create_field<TYPE_DECIMAL128I>(Decimal128V3(
                                             static_cast<__int128>(literal_value) * 1'000'000'000)))
                           : VLiteral::create_shared(comparison_type,
                                                     Field::create_field<TYPE_INT>(literal_value));
    auto gt = std::make_shared<VariantPathTestExpr>("gt", std::make_shared<DataTypeUInt8>(),
                                                    TExprNodeType::BINARY_PRED);
    gt->add_child(cast);
    gt->add_child(literal);
    return VExprContext::create_shared(std::move(gt));
}

VExprContextSPtr nested_variant_path_gt_conjunct(int32_t literal_value) {
    auto slot = VSlotRef::create_shared(0, 0, -1,
                                        make_nullable(std::make_shared<DataTypeVariantV2>()), "v");
    auto element_at = [](VExprSPtr parent, std::string key_name) {
        auto key = VLiteral::create_shared(std::make_shared<DataTypeString>(),
                                           Field::create_field<TYPE_STRING>(std::move(key_name)));
        auto result = std::make_shared<VariantPathTestExpr>(
                "element_at", make_nullable(std::make_shared<DataTypeVariantV2>()));
        result->add_child(std::move(parent));
        result->add_child(std::move(key));
        return result;
    };
    auto nested = element_at(element_at(slot, "a"), "b");
    auto cast = std::make_shared<VariantPathTestExpr>(
            "CAST", make_nullable(std::make_shared<DataTypeInt32>()), TExprNodeType::CAST_EXPR);
    cast->add_child(std::move(nested));
    auto literal = VLiteral::create_shared(std::make_shared<DataTypeInt32>(),
                                           Field::create_field<TYPE_INT>(literal_value));
    auto gt = std::make_shared<VariantPathTestExpr>("gt", std::make_shared<DataTypeUInt8>(),
                                                    TExprNodeType::BINARY_PRED);
    gt->add_child(std::move(cast));
    gt->add_child(std::move(literal));
    return VExprContext::create_shared(std::move(gt));
}

VExprContextSPtr variant_path_float_gt_conjunct(float literal_value) {
    auto slot = VSlotRef::create_shared(0, 0, -1,
                                        make_nullable(std::make_shared<DataTypeVariantV2>()), "v");
    auto key = VLiteral::create_shared(std::make_shared<DataTypeString>(),
                                       Field::create_field<TYPE_STRING>("col"));
    auto element_at = std::make_shared<VariantPathTestExpr>(
            "element_at", make_nullable(std::make_shared<DataTypeVariantV2>()));
    element_at->add_child(slot);
    element_at->add_child(key);
    auto comparison_type = std::make_shared<DataTypeFloat32>();
    auto cast = std::make_shared<VariantPathTestExpr>("CAST", make_nullable(comparison_type),
                                                      TExprNodeType::CAST_EXPR);
    cast->add_child(element_at);
    auto literal = VLiteral::create_shared(comparison_type,
                                           Field::create_field<TYPE_FLOAT>(literal_value));
    auto gt = std::make_shared<VariantPathTestExpr>("gt", std::make_shared<DataTypeUInt8>(),
                                                    TExprNodeType::BINARY_PRED);
    gt->add_child(cast);
    gt->add_child(literal);
    return VExprContext::create_shared(std::move(gt));
}

VExprContextSPtr variant_path_string_gt_conjunct(std::string literal_value) {
    auto slot = VSlotRef::create_shared(0, 0, -1,
                                        make_nullable(std::make_shared<DataTypeVariantV2>()), "v");
    auto key = VLiteral::create_shared(std::make_shared<DataTypeString>(),
                                       Field::create_field<TYPE_STRING>("col"));
    auto element_at = std::make_shared<VariantPathTestExpr>(
            "element_at", make_nullable(std::make_shared<DataTypeVariantV2>()));
    element_at->add_child(slot);
    element_at->add_child(key);
    auto comparison_type = std::make_shared<DataTypeString>();
    auto cast = std::make_shared<VariantPathTestExpr>("CAST", make_nullable(comparison_type),
                                                      TExprNodeType::CAST_EXPR);
    cast->add_child(element_at);
    auto literal = VLiteral::create_shared(
            comparison_type, Field::create_field<TYPE_STRING>(std::move(literal_value)));
    auto gt = std::make_shared<VariantPathTestExpr>("gt", std::make_shared<DataTypeUInt8>(),
                                                    TExprNodeType::BINARY_PRED);
    gt->add_child(cast);
    gt->add_child(literal);
    return VExprContext::create_shared(std::move(gt));
}

class UnsafeMetadataExpr final : public VExpr {
public:
    UnsafeMetadataExpr() : VExpr(std::make_shared<DataTypeUInt8>(), false) {}

    const std::string& expr_name() const override { return _expr_name; }
    Status execute_column_impl(VExprContext*, const Block*, const Selector*, size_t,
                               ColumnPtr&) const override {
        return Status::InternalError("UnsafeMetadataExpr is metadata-only");
    }
    bool is_safe_to_execute_on_selected_rows() const override { return false; }

private:
    const std::string _expr_name = "UnsafeMetadataExpr";
};

VExprContextSPtrs bloom_conjuncts(DataTypePtr data_type, std::vector<Field> values) {
    return {VExprContext::create_shared(
            std::make_shared<BloomInExpr>(0, std::move(data_type), std::move(values)))};
}

VExprContextSPtrs bloom_eq_conjunct(DataTypePtr data_type, Field value) {
    return {VExprContext::create_shared(
            std::make_shared<BloomEqExpr>(0, std::move(data_type), std::move(value)))};
}

format::FileScanRequest request_with_bloom_conjunct(DataTypePtr data_type,
                                                    std::vector<Field> values) {
    format::FileScanRequest request;
    request.local_positions.emplace(format::LocalColumnId(0), format::LocalIndex(0));
    request.conjuncts = bloom_conjuncts(std::move(data_type), std::move(values));
    return request;
}
format::parquet::ParquetColumnSchema uint32_parquet_bloom_schema() {
    format::parquet::ParquetColumnSchema column_schema;
    column_schema.type = std::make_shared<DataTypeInt64>();
    column_schema.type_descriptor.doris_type = column_schema.type;
    column_schema.type_descriptor.physical_type = tparquet::Type::INT32;
    column_schema.type_descriptor.integer_bit_width = 32;
    column_schema.type_descriptor.is_unsigned_integer = true;
    return column_schema;
}

TEST(NativeParquetStatisticsTest, InvalidNullableDateBoundsDisableMinMax) {
    format::parquet::ParquetColumnSchema column_schema;
    column_schema.type = make_nullable(std::make_shared<DataTypeDateV2>());
    column_schema.type_descriptor.doris_type = column_schema.type;
    column_schema.type_descriptor.physical_type = tparquet::Type::INT32;

    const int32_t invalid_date = std::numeric_limits<int32_t>::min();
    tparquet::Statistics statistics;
    statistics.__set_null_count(0);
    statistics.__set_min_value(
            std::string(reinterpret_cast<const char*>(&invalid_date), sizeof(invalid_date)));
    statistics.__set_max_value(
            std::string(reinterpret_cast<const char*>(&invalid_date), sizeof(invalid_date)));

    const auto result = format::parquet::ParquetStatisticsUtils::TransformColumnStatistics(
            column_schema, &statistics, 1, nullptr);
    EXPECT_FALSE(result.has_min_max);
}

TEST(NativeParquetStatisticsTest, FloatingNanEqualityKeepsFiniteOnlyFooterAndPageRanges) {
    const auto check_type = []<PrimitiveType Type, typename DataType, typename UInt>(
                                    tparquet::Type::type physical_type, UInt nan_bits) {
        using T = typename PrimitiveTypeTraits<Type>::CppType;
        auto column_schema = std::make_unique<format::parquet::ParquetColumnSchema>();
        column_schema->kind = format::parquet::ParquetColumnSchemaKind::PRIMITIVE;
        column_schema->local_id = 0;
        column_schema->leaf_column_id = 0;
        column_schema->type = std::make_shared<DataType>();
        column_schema->type_descriptor.doris_type = column_schema->type;
        column_schema->type_descriptor.physical_type = physical_type;
        std::vector<std::unique_ptr<format::parquet::ParquetColumnSchema>> schema;
        schema.push_back(std::move(column_schema));

        const T finite_bound = T {0};
        const std::string encoded_bound(reinterpret_cast<const char*>(&finite_bound), sizeof(T));
        tparquet::Statistics statistics;
        statistics.__set_min_value(encoded_bound);
        statistics.__set_max_value(encoded_bound);
        statistics.__set_null_count(0);
        tparquet::ColumnMetaData column_metadata;
        column_metadata.__set_type(physical_type);
        column_metadata.__set_num_values(2);
        column_metadata.__set_total_compressed_size(0);
        column_metadata.__set_statistics(statistics);
        tparquet::ColumnChunk chunk;
        chunk.__set_meta_data(column_metadata);
        tparquet::RowGroup row_group;
        row_group.__set_columns({chunk});
        row_group.__set_num_rows(2);
        tparquet::ColumnOrder order;
        order.__set_TYPE_ORDER(tparquet::TypeDefinedOrder());
        tparquet::FileMetaData metadata;
        metadata.__set_column_orders({order});
        metadata.__set_row_groups({row_group});

        format::parquet::NativeParquetPageIndex page_index;
        page_index.column_index.__set_min_values({encoded_bound});
        page_index.column_index.__set_max_values({encoded_bound});
        page_index.column_index.__set_null_pages({false});
        page_index.column_index.__set_null_counts({0});
        tparquet::PageLocation location;
        location.__set_offset(0);
        location.__set_compressed_page_size(10);
        location.__set_first_row_index(0);
        page_index.offset_index.__set_page_locations({location});
        std::unordered_map<int, format::parquet::NativeParquetPageIndex> page_indexes;
        page_indexes.emplace(0, std::move(page_index));

        const auto nan_field = Field::create_field<Type>(std::bit_cast<T>(nan_bits));
        for (const auto mode :
             {MetadataFloatingEqualityExpr::Mode::EQ, MetadataFloatingEqualityExpr::Mode::IN,
              MetadataFloatingEqualityExpr::Mode::NE, MetadataFloatingEqualityExpr::Mode::GT,
              MetadataFloatingEqualityExpr::Mode::GE,
              MetadataFloatingEqualityExpr::Mode::REVERSED_LT,
              MetadataFloatingEqualityExpr::Mode::REVERSED_LE,
              MetadataFloatingEqualityExpr::Mode::NOT_IN}) {
            format::FileScanRequest request;
            request.local_positions.emplace(format::LocalColumnId(0), format::LocalIndex(0));
            request.predicate_columns = {
                    format::LocalColumnIndex::top_level(format::LocalColumnId(0))};
            request.conjuncts = {
                    VExprContext::create_shared(std::make_shared<MetadataFloatingEqualityExpr>(
                            0, schema[0]->type, nan_field, mode))};

            std::vector<int> selected_row_groups;
            ASSERT_TRUE(format::parquet::select_row_groups_by_metadata(
                                metadata, schema, request, nullptr, &selected_row_groups, false,
                                nullptr)
                                .ok());
            EXPECT_EQ(selected_row_groups, std::vector<int>({0}));

            std::vector<format::parquet::RowRange> selected_ranges;
            std::map<int, format::parquet::ParquetPageSkipPlan> skip_plans;
            ASSERT_TRUE(format::parquet::select_row_group_ranges_by_native_page_index(
                                metadata, metadata.row_groups[0], page_indexes, schema, request, 2,
                                &selected_ranges, &skip_plans, nullptr)
                                .ok());
            ASSERT_EQ(1, selected_ranges.size());
            EXPECT_EQ(0, selected_ranges[0].start);
            EXPECT_EQ(2, selected_ranges[0].length);
        }
    };

    check_type.template operator()<TYPE_FLOAT, DataTypeFloat32>(tparquet::Type::FLOAT,
                                                                uint32_t {0x7fc00002U});
    check_type.template operator()<TYPE_DOUBLE, DataTypeFloat64>(tparquet::Type::DOUBLE,
                                                                 uint64_t {0x7ff8000000000002ULL});
}

TEST(NativeParquetStatisticsTest, InvalidNullableDecimalBoundsDisableMinMax) {
    format::parquet::ParquetColumnSchema column_schema;
    column_schema.type = make_nullable(std::make_shared<DataTypeDecimal32>(2, 0));
    column_schema.type_descriptor.doris_type = column_schema.type;
    column_schema.type_descriptor.physical_type = tparquet::Type::INT32;
    column_schema.type_descriptor.is_decimal = true;
    column_schema.type_descriptor.decimal_precision = 2;
    column_schema.type_descriptor.decimal_scale = 0;

    const int32_t invalid_decimal = 1000;
    tparquet::Statistics statistics;
    statistics.__set_null_count(0);
    statistics.__set_min_value(
            std::string(reinterpret_cast<const char*>(&invalid_decimal), sizeof(invalid_decimal)));
    statistics.__set_max_value(
            std::string(reinterpret_cast<const char*>(&invalid_decimal), sizeof(invalid_decimal)));

    const auto result = format::parquet::ParquetStatisticsUtils::TransformColumnStatistics(
            column_schema, &statistics, 1, nullptr);
    EXPECT_FALSE(result.has_min_max);
}

TEST(NativeParquetStatisticsTest, InvalidTimeBoundsDisableFooterMinMax) {
    format::parquet::ParquetColumnSchema column_schema;
    column_schema.type = std::make_shared<DataTypeTimeV2>(3);
    column_schema.type_descriptor.doris_type = column_schema.type;
    column_schema.type_descriptor.physical_type = tparquet::Type::INT32;
    column_schema.type_descriptor.time_unit = format::parquet::ParquetTimeUnit::MILLIS;

    const int32_t invalid_time = 90000000;
    tparquet::Statistics statistics;
    statistics.__set_null_count(0);
    statistics.__set_min_value(
            std::string(reinterpret_cast<const char*>(&invalid_time), sizeof(invalid_time)));
    statistics.__set_max_value(
            std::string(reinterpret_cast<const char*>(&invalid_time), sizeof(invalid_time)));

    const auto result = format::parquet::ParquetStatisticsUtils::TransformColumnStatistics(
            column_schema, &statistics, 1, nullptr);
    EXPECT_FALSE(result.has_min_max);
}

TEST(NativeParquetStatisticsTest, BooleanFooterBoundsDecodeOnlyTheValueBit) {
    format::parquet::ParquetColumnSchema column_schema;
    column_schema.type = std::make_shared<DataTypeUInt8>();
    column_schema.type_descriptor.doris_type = column_schema.type;
    column_schema.type_descriptor.physical_type = tparquet::Type::BOOLEAN;

    tparquet::Statistics statistics;
    statistics.__set_null_count(0);
    statistics.__set_min_value(std::string(1, '\x02'));
    statistics.__set_max_value(std::string(1, '\x02'));

    const auto result = format::parquet::ParquetStatisticsUtils::TransformColumnStatistics(
            column_schema, &statistics, 1, nullptr);
    ASSERT_TRUE(result.has_min_max);
    EXPECT_EQ(result.min_value, Field::create_field<TYPE_BOOLEAN>(false));
    EXPECT_EQ(result.max_value, Field::create_field<TYPE_BOOLEAN>(false));
}

TEST(NativeParquetStatisticsTest, InvalidTimeAndPaddedBooleanPageBoundsCannotPrune) {
    auto run_page_index = [](std::unique_ptr<format::parquet::ParquetColumnSchema> column_schema,
                             std::string min_value, std::string max_value,
                             bool require_false_boolean) {
        column_schema->kind = format::parquet::ParquetColumnSchemaKind::PRIMITIVE;
        column_schema->local_id = 0;
        column_schema->leaf_column_id = 0;
        std::vector<std::unique_ptr<format::parquet::ParquetColumnSchema>> schema;
        schema.push_back(std::move(column_schema));

        tparquet::ColumnOrder order;
        order.__set_TYPE_ORDER(tparquet::TypeDefinedOrder());
        tparquet::FileMetaData metadata;
        metadata.__set_column_orders({order});
        format::FileScanRequest request;
        request.local_positions.emplace(format::LocalColumnId(0), format::LocalIndex(0));
        request.predicate_columns = {format::LocalColumnIndex::top_level(format::LocalColumnId(0))};
        request.conjuncts = {VExprContext::create_shared(
                std::make_shared<MetadataBoundsProbeExpr>(require_false_boolean))};

        format::parquet::NativeParquetPageIndex page_index;
        page_index.column_index.__set_min_values({std::move(min_value)});
        page_index.column_index.__set_max_values({std::move(max_value)});
        page_index.column_index.__set_null_pages({false});
        page_index.column_index.__set_null_counts({0});
        tparquet::PageLocation location;
        location.__set_offset(0);
        location.__set_compressed_page_size(10);
        location.__set_first_row_index(0);
        page_index.offset_index.__set_page_locations({location});
        std::unordered_map<int, format::parquet::NativeParquetPageIndex> page_indexes;
        page_indexes.emplace(0, std::move(page_index));
        std::vector<format::parquet::RowRange> selected_ranges;
        std::map<int, format::parquet::ParquetPageSkipPlan> skip_plans;
        tparquet::RowGroup row_group;
        EXPECT_TRUE(format::parquet::select_row_group_ranges_by_native_page_index(
                            metadata, row_group, page_indexes, schema, request, 1, &selected_ranges,
                            &skip_plans, nullptr)
                            .ok());
        return selected_ranges;
    };

    auto time_schema = std::make_unique<format::parquet::ParquetColumnSchema>();
    time_schema->type = std::make_shared<DataTypeTimeV2>(3);
    time_schema->type_descriptor.doris_type = time_schema->type;
    time_schema->type_descriptor.physical_type = tparquet::Type::INT32;
    time_schema->type_descriptor.time_unit = format::parquet::ParquetTimeUnit::MILLIS;
    const int32_t invalid_time = 90000000;
    const std::string invalid_time_bytes(reinterpret_cast<const char*>(&invalid_time),
                                         sizeof(invalid_time));
    const auto time_ranges =
            run_page_index(std::move(time_schema), invalid_time_bytes, invalid_time_bytes, false);
    ASSERT_EQ(time_ranges.size(), 1);
    EXPECT_EQ(time_ranges[0].start, 0);
    EXPECT_EQ(time_ranges[0].length, 1);

    auto bool_schema = std::make_unique<format::parquet::ParquetColumnSchema>();
    bool_schema->type = std::make_shared<DataTypeUInt8>();
    bool_schema->type_descriptor.doris_type = bool_schema->type;
    bool_schema->type_descriptor.physical_type = tparquet::Type::BOOLEAN;
    const auto bool_ranges = run_page_index(std::move(bool_schema), std::string(1, '\x02'),
                                            std::string(1, '\x02'), true);
    ASSERT_EQ(bool_ranges.size(), 1);
    EXPECT_EQ(bool_ranges[0].start, 0);
    EXPECT_EQ(bool_ranges[0].length, 1);
}

TEST(NativeParquetStatisticsTest, MultiColumnOrUnionsPageIndexRanges) {
    auto encode_int32 = [](int32_t value) {
        std::string bytes(sizeof(value), '\0');
        memcpy(bytes.data(), &value, sizeof(value));
        return bytes;
    };
    auto make_schema = [](int local_id, int leaf_column_id) {
        auto column = std::make_unique<format::parquet::ParquetColumnSchema>();
        column->kind = format::parquet::ParquetColumnSchemaKind::PRIMITIVE;
        column->local_id = local_id;
        column->leaf_column_id = leaf_column_id;
        column->type = std::make_shared<DataTypeInt32>();
        column->type_descriptor.doris_type = column->type;
        column->type_descriptor.physical_type = tparquet::Type::INT32;
        return column;
    };
    auto make_page_index = [&](const std::vector<int32_t>& values) {
        format::parquet::NativeParquetPageIndex page_index;
        std::vector<std::string> encoded;
        encoded.reserve(values.size());
        for (const auto value : values) {
            encoded.push_back(encode_int32(value));
        }
        page_index.column_index.__set_min_values(encoded);
        page_index.column_index.__set_max_values(encoded);
        page_index.column_index.__set_null_pages(std::vector<bool>(values.size(), false));
        page_index.column_index.__set_null_counts(std::vector<int64_t>(values.size(), 0));
        std::vector<tparquet::PageLocation> locations;
        for (size_t page_idx = 0; page_idx < values.size(); ++page_idx) {
            tparquet::PageLocation location;
            location.__set_offset(static_cast<int64_t>(page_idx * 100));
            location.__set_compressed_page_size(100);
            location.__set_first_row_index(static_cast<int64_t>(page_idx * 10));
            locations.push_back(location);
        }
        page_index.offset_index.__set_page_locations(std::move(locations));
        return page_index;
    };

    std::vector<std::unique_ptr<format::parquet::ParquetColumnSchema>> schema;
    schema.push_back(make_schema(0, 0));
    schema.push_back(make_schema(1, 1));
    tparquet::ColumnOrder order;
    order.__set_TYPE_ORDER(tparquet::TypeDefinedOrder());
    tparquet::FileMetaData metadata;
    metadata.__set_column_orders({order, order});

    auto make_compound_expr = [](TExprOpcode::type opcode, VExprSPtr left, VExprSPtr right) {
        TExprNode compound_node;
        compound_node.__set_node_type(TExprNodeType::COMPOUND_PRED);
        compound_node.__set_opcode(opcode);
        compound_node.__set_type(std::make_shared<DataTypeUInt8>()->to_thrift());
        compound_node.__set_num_children(2);
        compound_node.__set_is_nullable(false);
        auto compound = VCompoundPred::create_shared(compound_node);
        compound->add_child(std::move(left));
        compound->add_child(std::move(right));
        return compound;
    };
    auto make_compound = [&](TExprOpcode::type opcode) {
        return VExprContext::create_shared(make_compound_expr(
                opcode, std::make_shared<MetadataSlotInt32GreaterThanExpr>(0, 50),
                std::make_shared<MetadataSlotInt32GreaterThanExpr>(1, 50)));
    };

    format::FileScanRequest request;
    request.local_positions.emplace(format::LocalColumnId(0), format::LocalIndex(0));
    request.local_positions.emplace(format::LocalColumnId(1), format::LocalIndex(1));
    request.predicate_columns = {format::LocalColumnIndex::top_level(format::LocalColumnId(0)),
                                 format::LocalColumnIndex::top_level(format::LocalColumnId(1))};
    request.conjuncts = {make_compound(TExprOpcode::COMPOUND_OR)};

    std::unordered_map<int, format::parquet::NativeParquetPageIndex> page_indexes;
    page_indexes.emplace(0, make_page_index({100, 0, 0}));
    page_indexes.emplace(1, make_page_index({0, 0, 100}));
    std::vector<format::parquet::RowRange> selected_ranges;
    std::map<int, format::parquet::ParquetPageSkipPlan> skip_plans;
    ASSERT_TRUE(format::parquet::select_row_group_ranges_by_native_page_index(
                        metadata, tparquet::RowGroup {}, page_indexes, schema, request, 30,
                        &selected_ranges, &skip_plans, nullptr)
                        .ok());
    ASSERT_EQ(selected_ranges.size(), 2);
    EXPECT_EQ(selected_ranges[0].start, 0);
    EXPECT_EQ(selected_ranges[0].length, 10);
    EXPECT_EQ(selected_ranges[1].start, 20);
    EXPECT_EQ(selected_ranges[1].length, 10);

    // A compound predicate after the safety fence must not participate in metadata pruning.
    request.metadata_pruning_safe_conjunct_count = 0;
    ASSERT_TRUE(format::parquet::select_row_group_ranges_by_native_page_index(
                        metadata, tparquet::RowGroup {}, page_indexes, schema, request, 30,
                        &selected_ranges, &skip_plans, nullptr)
                        .ok());
    ASSERT_EQ(selected_ranges.size(), 1);
    EXPECT_EQ(selected_ranges[0].start, 0);
    EXPECT_EQ(selected_ranges[0].length, 30);
    request.metadata_pruning_safe_conjunct_count = request.conjuncts.size();

    page_indexes.erase(1);
    ASSERT_TRUE(format::parquet::select_row_group_ranges_by_native_page_index(
                        metadata, tparquet::RowGroup {}, page_indexes, schema, request, 30,
                        &selected_ranges, &skip_plans, nullptr)
                        .ok());
    ASSERT_EQ(selected_ranges.size(), 1);
    EXPECT_EQ(selected_ranges[0].start, 0);
    EXPECT_EQ(selected_ranges[0].length, 30);

    request.conjuncts = {make_compound(TExprOpcode::COMPOUND_AND)};
    ASSERT_TRUE(format::parquet::select_row_group_ranges_by_native_page_index(
                        metadata, tparquet::RowGroup {}, page_indexes, schema, request, 30,
                        &selected_ranges, &skip_plans, nullptr)
                        .ok());
    ASSERT_EQ(selected_ranges.size(), 1);
    EXPECT_EQ(selected_ranges[0].start, 0);
    EXPECT_EQ(selected_ranges[0].length, 10);

    page_indexes.emplace(1, make_page_index({0, 0, 100}));
    auto first_branch = make_compound_expr(
            TExprOpcode::COMPOUND_AND, std::make_shared<MetadataSlotInt32GreaterThanExpr>(0, 50),
            std::make_shared<MetadataSlotInt32GreaterThanExpr>(1, 50));
    auto second_branch = make_compound_expr(
            TExprOpcode::COMPOUND_AND, std::make_shared<MetadataSlotInt32GreaterThanExpr>(0, -1),
            std::make_shared<MetadataSlotInt32GreaterThanExpr>(1, 50));
    request.conjuncts = {VExprContext::create_shared(make_compound_expr(
            TExprOpcode::COMPOUND_OR, std::move(first_branch), std::move(second_branch)))};
    ASSERT_TRUE(format::parquet::select_row_group_ranges_by_native_page_index(
                        metadata, tparquet::RowGroup {}, page_indexes, schema, request, 30,
                        &selected_ranges, &skip_plans, nullptr)
                        .ok());
    ASSERT_EQ(selected_ranges.size(), 1);
    EXPECT_EQ(selected_ranges[0].start, 20);
    EXPECT_EQ(selected_ranges[0].length, 10);
}

TEST(ParquetBloomFilterPruningTest, NativeUint32BloomUsesPhysicalInt32Hash) {
    const auto column_schema = uint32_parquet_bloom_schema();
    format::parquet::native::BlockSplitBloomFilter bloom_filter;
    ASSERT_TRUE(bloom_filter
                        .init(segment_v2::BloomFilter::MINIMUM_BYTES,
                              segment_v2::HashStrategyPB::XX_HASH_64)
                        .ok());

    const uint32_t present_value = 4000000000U;
    int32_t physical_value;
    memcpy(&physical_value, &present_value, sizeof(physical_value));
    bloom_filter.add_bytes(reinterpret_cast<const char*>(&physical_value), sizeof(physical_value));

    EXPECT_FALSE(format::parquet::ParquetStatisticsUtils::NativeBloomFilterExcludes(
            column_schema, 0,
            bloom_conjuncts(column_schema.type, {Field::create_field<TYPE_BIGINT>(
                                                        static_cast<int64_t>(present_value))}),
            bloom_filter));
    EXPECT_TRUE(format::parquet::ParquetStatisticsUtils::NativeBloomFilterExcludes(
            column_schema, 0,
            bloom_conjuncts(column_schema.type, {Field::create_field<TYPE_BIGINT>(-1)}),
            bloom_filter));
}

TEST(ParquetBloomFilterPruningTest, NativeFloatingBloomPreservesDorisEquality) {
    const auto check_type = []<PrimitiveType Type, typename DataType>(
                                    tparquet::Type::type physical_type,
                                    typename PrimitiveTypeTraits<Type>::CppType stored_value,
                                    typename PrimitiveTypeTraits<Type>::CppType predicate_value) {
        format::parquet::ParquetColumnSchema column_schema;
        column_schema.type = std::make_shared<DataType>();
        column_schema.type_descriptor.doris_type = column_schema.type;
        column_schema.type_descriptor.physical_type = physical_type;

        format::parquet::native::BlockSplitBloomFilter bloom_filter;
        ASSERT_TRUE(bloom_filter
                            .init(segment_v2::BloomFilter::MINIMUM_BYTES,
                                  segment_v2::HashStrategyPB::XX_HASH_64)
                            .ok());
        // These raw PLAIN bytes model an external writer; the predicate uses a different physical
        // representation from the same Doris equality class.
        bloom_filter.add_bytes(reinterpret_cast<const char*>(&stored_value), sizeof(stored_value));
        ASSERT_FALSE(bloom_filter.test_bytes(reinterpret_cast<const char*>(&predicate_value),
                                             sizeof(predicate_value)));
        const auto field = Field::create_field<Type>(predicate_value);

        EXPECT_FALSE(format::parquet::ParquetStatisticsUtils::NativeBloomFilterExcludes(
                column_schema, 0, bloom_eq_conjunct(column_schema.type, field), bloom_filter));
        EXPECT_FALSE(format::parquet::ParquetStatisticsUtils::NativeBloomFilterExcludes(
                column_schema, 0, bloom_conjuncts(column_schema.type, {field}), bloom_filter));
    };

    check_type.template operator()<TYPE_FLOAT, DataTypeFloat32>(tparquet::Type::FLOAT, -0.0F, 0.0F);
    check_type.template operator()<TYPE_FLOAT, DataTypeFloat32>(tparquet::Type::FLOAT, 0.0F, -0.0F);
    check_type.template operator()<TYPE_DOUBLE, DataTypeFloat64>(tparquet::Type::DOUBLE, -0.0, 0.0);
    check_type.template operator()<TYPE_DOUBLE, DataTypeFloat64>(tparquet::Type::DOUBLE, 0.0, -0.0);
    check_type.template operator()<TYPE_FLOAT, DataTypeFloat32>(
            tparquet::Type::FLOAT, std::bit_cast<float>(uint32_t {0x7fc00001U}),
            std::bit_cast<float>(uint32_t {0x7fc00002U}));
    check_type.template operator()<TYPE_DOUBLE, DataTypeFloat64>(
            tparquet::Type::DOUBLE, std::bit_cast<double>(uint64_t {0x7ff8000000000001ULL}),
            std::bit_cast<double>(uint64_t {0x7ff8000000000002ULL}));
}

TEST(ParquetBloomFilterPruningTest, NativeRowGroupKeepsDorisEqualFloatingValues) {
    const auto check_type = []<PrimitiveType Type, typename DataType>(
                                    tparquet::Type::type physical_type,
                                    typename PrimitiveTypeTraits<Type>::CppType stored_value,
                                    typename PrimitiveTypeTraits<Type>::CppType predicate_value) {
        format::parquet::native::BlockSplitBloomFilter bloom_filter;
        ASSERT_TRUE(bloom_filter
                            .init(segment_v2::BloomFilter::MINIMUM_BYTES,
                                  segment_v2::HashStrategyPB::XX_HASH_64)
                            .ok());
        bloom_filter.add_bytes(reinterpret_cast<const char*>(&stored_value), sizeof(stored_value));
        tparquet::BloomFilterAlgorithm algorithm;
        algorithm.__set_BLOCK(tparquet::SplitBlockAlgorithm());
        tparquet::BloomFilterHash hash;
        hash.__set_XXHASH(tparquet::XxHash());
        tparquet::BloomFilterCompression compression;
        compression.__set_UNCOMPRESSED(tparquet::Uncompressed());
        tparquet::BloomFilterHeader bloom_header;
        bloom_header.__set_numBytes(static_cast<int32_t>(bloom_filter.size()));
        bloom_header.__set_algorithm(algorithm);
        bloom_header.__set_hash(hash);
        bloom_header.__set_compression(compression);
        std::vector<uint8_t> bloom_bytes;
        ThriftSerializer serializer(/*compact=*/true, 64);
        ASSERT_TRUE(serializer.serialize(&bloom_header, &bloom_bytes).ok());
        bloom_bytes.insert(bloom_bytes.end(), bloom_filter.data(),
                           bloom_filter.data() + bloom_filter.size());

        tparquet::ColumnMetaData column_metadata;
        column_metadata.__set_type(physical_type);
        column_metadata.__set_codec(tparquet::CompressionCodec::UNCOMPRESSED);
        column_metadata.__set_num_values(1);
        column_metadata.__set_total_compressed_size(0);
        column_metadata.__set_data_page_offset(0);
        column_metadata.__set_bloom_filter_offset(0);
        column_metadata.__set_bloom_filter_length(static_cast<int32_t>(bloom_bytes.size()));
        tparquet::ColumnChunk chunk;
        chunk.__set_meta_data(column_metadata);
        tparquet::RowGroup row_group;
        row_group.__set_columns({chunk});
        row_group.__set_total_byte_size(0);
        row_group.__set_num_rows(1);
        tparquet::FileMetaData metadata;
        metadata.__set_version(1);
        metadata.__set_num_rows(1);
        metadata.__set_row_groups({row_group});

        const auto field = Field::create_field<Type>(predicate_value);
        for (const bool use_eq : {true, false}) {
            auto column_schema = std::make_unique<format::parquet::ParquetColumnSchema>();
            column_schema->type = std::make_shared<DataType>();
            column_schema->type_descriptor.doris_type = column_schema->type;
            column_schema->type_descriptor.physical_type = physical_type;
            column_schema->local_id = 0;
            column_schema->leaf_column_id = 0;

            format::FileScanRequest request;
            request.local_positions.emplace(format::LocalColumnId(0), format::LocalIndex(0));
            request.conjuncts = use_eq ? bloom_eq_conjunct(column_schema->type, field)
                                       : bloom_conjuncts(column_schema->type, {field});
            std::vector<std::unique_ptr<format::parquet::ParquetColumnSchema>> schema;
            schema.push_back(std::move(column_schema));
            format::parquet::ParquetFileContext file_context;
            file_context.native_file = std::make_shared<StatisticsMemoryFileReader>(bloom_bytes);
            std::vector<int> selected_row_groups;
            format::parquet::ParquetPruningStats pruning_stats;
            ASSERT_TRUE(format::parquet::select_row_groups_by_metadata(
                                metadata, schema, request, nullptr, &selected_row_groups, true,
                                &pruning_stats, nullptr, nullptr, &file_context)
                                .ok());
            EXPECT_EQ(selected_row_groups, std::vector<int>({0}));
            EXPECT_EQ(pruning_stats.filtered_row_groups_by_bloom_filter, 0);
        }
    };

    check_type.template operator()<TYPE_FLOAT, DataTypeFloat32>(tparquet::Type::FLOAT, -0.0F, 0.0F);
    check_type.template operator()<TYPE_DOUBLE, DataTypeFloat64>(tparquet::Type::DOUBLE, 0.0, -0.0);
    check_type.template operator()<TYPE_FLOAT, DataTypeFloat32>(
            tparquet::Type::FLOAT, std::bit_cast<float>(uint32_t {0x7fc00001U}),
            std::bit_cast<float>(uint32_t {0x7fc00002U}));
    check_type.template operator()<TYPE_DOUBLE, DataTypeFloat64>(
            tparquet::Type::DOUBLE, std::bit_cast<double>(uint64_t {0x7ff8000000000001ULL}),
            std::bit_cast<double>(uint64_t {0x7ff8000000000002ULL}));
}

TEST(ParquetBloomFilterPruningTest, NativeBloomResolvesStructAndListLeaves) {
    const auto run_case = [](format::parquet::ParquetColumnSchemaKind root_kind,
                             int32_t predicate_value, bool path_exists, int conjunct_count,
                             bool expected_pruned, int expected_reads,
                             bool add_unsafe_barrier = false) {
        auto leaf_type = std::make_shared<DataTypeInt32>();
        DataTypePtr root_type;
        VExprSPtr selector;
        if (root_kind == format::parquet::ParquetColumnSchemaKind::STRUCT) {
            root_type = std::make_shared<DataTypeStruct>(DataTypes {leaf_type}, Strings {"value"});
            selector = VLiteral::create_shared(std::make_shared<DataTypeString>(),
                                               Field::create_field<TYPE_STRING>("value"));
        } else {
            root_type = std::make_shared<DataTypeArray>(leaf_type);
            selector = VLiteral::create_shared(leaf_type, Field::create_field<TYPE_INT>(1));
        }

        auto root_schema = std::make_unique<format::parquet::ParquetColumnSchema>();
        root_schema->kind = root_kind;
        root_schema->local_id = 0;
        root_schema->name = "root";
        root_schema->type = root_type;
        auto leaf_schema = std::make_unique<format::parquet::ParquetColumnSchema>();
        leaf_schema->kind = format::parquet::ParquetColumnSchemaKind::PRIMITIVE;
        leaf_schema->local_id = 0;
        leaf_schema->name = root_kind == format::parquet::ParquetColumnSchemaKind::STRUCT
                                    ? (path_exists ? "value" : "renamed_value")
                                    : "element";
        leaf_schema->leaf_column_id = 0;
        leaf_schema->type = leaf_type;
        leaf_schema->type_descriptor.doris_type = leaf_type;
        leaf_schema->type_descriptor.physical_type = tparquet::Type::INT32;
        if (root_kind == format::parquet::ParquetColumnSchemaKind::LIST) {
            root_schema->max_repetition_level = 1;
            leaf_schema->max_repetition_level = 1;
        }
        root_schema->children.push_back(std::move(leaf_schema));

        format::parquet::native::BlockSplitBloomFilter bloom_filter;
        ASSERT_TRUE(bloom_filter
                            .init(segment_v2::BloomFilter::MINIMUM_BYTES,
                                  segment_v2::HashStrategyPB::XX_HASH_64)
                            .ok());
        const int32_t present_value = 1;
        bloom_filter.add_bytes(reinterpret_cast<const char*>(&present_value),
                               sizeof(present_value));
        tparquet::BloomFilterAlgorithm algorithm;
        algorithm.__set_BLOCK(tparquet::SplitBlockAlgorithm());
        tparquet::BloomFilterHash hash;
        hash.__set_XXHASH(tparquet::XxHash());
        tparquet::BloomFilterCompression compression;
        compression.__set_UNCOMPRESSED(tparquet::Uncompressed());
        tparquet::BloomFilterHeader bloom_header;
        bloom_header.__set_numBytes(static_cast<int32_t>(bloom_filter.size()));
        bloom_header.__set_algorithm(algorithm);
        bloom_header.__set_hash(hash);
        bloom_header.__set_compression(compression);
        std::vector<uint8_t> bloom_bytes;
        ThriftSerializer serializer(/*compact=*/true, 64);
        ASSERT_TRUE(serializer.serialize(&bloom_header, &bloom_bytes).ok());
        bloom_bytes.insert(bloom_bytes.end(), bloom_filter.data(),
                           bloom_filter.data() + bloom_filter.size());

        tparquet::ColumnMetaData column_metadata;
        column_metadata.__set_type(tparquet::Type::INT32);
        column_metadata.__set_codec(tparquet::CompressionCodec::UNCOMPRESSED);
        column_metadata.__set_num_values(1);
        column_metadata.__set_total_compressed_size(0);
        column_metadata.__set_data_page_offset(0);
        column_metadata.__set_bloom_filter_offset(0);
        column_metadata.__set_bloom_filter_length(static_cast<int32_t>(bloom_bytes.size()));
        tparquet::ColumnChunk chunk;
        chunk.__set_meta_data(column_metadata);
        tparquet::RowGroup row_group;
        row_group.__set_columns({chunk});
        row_group.__set_total_byte_size(0);
        row_group.__set_num_rows(1);
        tparquet::FileMetaData metadata;
        metadata.__set_version(1);
        metadata.__set_num_rows(1);
        metadata.__set_row_groups({row_group});

        format::FileScanRequest request;
        request.local_positions.emplace(format::LocalColumnId(0), format::LocalIndex(0));
        if (add_unsafe_barrier) {
            request.conjuncts.push_back(
                    VExprContext::create_shared(std::make_shared<UnsafeMetadataExpr>()));
            request.metadata_pruning_safe_conjunct_count = 0;
        }
        for (int conjunct_idx = 0; conjunct_idx < conjunct_count; ++conjunct_idx) {
            auto slot = VSlotRef::create_shared(0, 0, -1, root_type, "root");
            auto accessor =
                    std::make_shared<MetadataAccessorExpr>(leaf_type, std::move(slot), selector);
            request.conjuncts.push_back(VExprContext::create_shared(std::make_shared<BloomInExpr>(
                    std::move(accessor),
                    std::vector<Field> {Field::create_field<TYPE_INT>(predicate_value)})));
        }
        std::vector<std::unique_ptr<format::parquet::ParquetColumnSchema>> schema;
        schema.push_back(std::move(root_schema));
        format::parquet::ParquetFileContext file_context;
        auto file_reader = std::make_shared<StatisticsMemoryFileReader>(std::move(bloom_bytes));
        file_context.native_file = file_reader;
        std::vector<int> selected_row_groups;
        format::parquet::ParquetPruningStats pruning_stats;
        ASSERT_TRUE(format::parquet::select_row_groups_by_metadata(
                            metadata, schema, request, nullptr, &selected_row_groups, true,
                            &pruning_stats, nullptr, nullptr, &file_context)
                            .ok());
        EXPECT_EQ(selected_row_groups.empty(), expected_pruned);
        EXPECT_EQ(pruning_stats.filtered_row_groups_by_bloom_filter, expected_pruned ? 1 : 0);
        EXPECT_EQ(file_reader->read_count(), expected_reads);
        EXPECT_EQ(pruning_stats.bloom_filter_probe_attempts, expected_reads == 0 ? 0 : 1);
        EXPECT_EQ(pruning_stats.bloom_filter_probe_successes, expected_reads == 0 ? 0 : 1);
        EXPECT_EQ(pruning_stats.bloom_filter_conservative_fallbacks, 0);
        EXPECT_EQ(pruning_stats.bloom_filter_corrupt_rejections, 0);
    };

    run_case(format::parquet::ParquetColumnSchemaKind::STRUCT, 2, true, 1, true, 2);
    run_case(format::parquet::ParquetColumnSchemaKind::STRUCT, 1, true, 1, false, 2);
    run_case(format::parquet::ParquetColumnSchemaKind::STRUCT, 2, false, 1, false, 0);
    run_case(format::parquet::ParquetColumnSchemaKind::LIST, 2, true, 1, true, 2);
    run_case(format::parquet::ParquetColumnSchemaKind::LIST, 1, true, 1, false, 2);
    run_case(format::parquet::ParquetColumnSchemaKind::STRUCT, 1, true, 2, false, 2);
    run_case(format::parquet::ParquetColumnSchemaKind::STRUCT, 2, true, 1, false, 0, true);
}

TEST(ParquetBloomFilterPruningTest, NativeBloomReportsConservativeReadOutcomes) {
    const auto make_valid_bloom = [] {
        format::parquet::native::BlockSplitBloomFilter bloom_filter;
        EXPECT_TRUE(bloom_filter
                            .init(segment_v2::BloomFilter::MINIMUM_BYTES,
                                  segment_v2::HashStrategyPB::XX_HASH_64)
                            .ok());
        const int32_t present_value = 1;
        bloom_filter.add_bytes(reinterpret_cast<const char*>(&present_value),
                               sizeof(present_value));
        tparquet::BloomFilterAlgorithm algorithm;
        algorithm.__set_BLOCK(tparquet::SplitBlockAlgorithm());
        tparquet::BloomFilterHash hash;
        hash.__set_XXHASH(tparquet::XxHash());
        tparquet::BloomFilterCompression compression;
        compression.__set_UNCOMPRESSED(tparquet::Uncompressed());
        tparquet::BloomFilterHeader header;
        header.__set_numBytes(static_cast<int32_t>(bloom_filter.size()));
        header.__set_algorithm(algorithm);
        header.__set_hash(hash);
        header.__set_compression(compression);
        std::vector<uint8_t> bytes;
        ThriftSerializer serializer(/*compact=*/true, 64);
        EXPECT_TRUE(serializer.serialize(&header, &bytes).ok());
        bytes.insert(bytes.end(), bloom_filter.data(), bloom_filter.data() + bloom_filter.size());
        return bytes;
    };

    const auto run_case = [&](std::vector<uint8_t> bytes, bool has_offset, bool fail_reads,
                              int64_t expected_corrupt_rejections) {
        auto type = std::make_shared<DataTypeInt32>();
        auto column_schema = std::make_unique<format::parquet::ParquetColumnSchema>();
        column_schema->kind = format::parquet::ParquetColumnSchemaKind::PRIMITIVE;
        column_schema->local_id = 0;
        column_schema->leaf_column_id = 0;
        column_schema->type = type;
        column_schema->type_descriptor.doris_type = type;
        column_schema->type_descriptor.physical_type = tparquet::Type::INT32;

        tparquet::ColumnMetaData column_metadata;
        column_metadata.__set_type(tparquet::Type::INT32);
        column_metadata.__set_codec(tparquet::CompressionCodec::UNCOMPRESSED);
        column_metadata.__set_num_values(1);
        column_metadata.__set_total_compressed_size(0);
        column_metadata.__set_data_page_offset(0);
        if (has_offset) {
            column_metadata.__set_bloom_filter_offset(0);
            column_metadata.__set_bloom_filter_length(static_cast<int32_t>(bytes.size()));
        }
        tparquet::ColumnChunk chunk;
        chunk.__set_meta_data(column_metadata);
        tparquet::RowGroup row_group;
        row_group.__set_columns({chunk});
        row_group.__set_total_byte_size(0);
        row_group.__set_num_rows(1);
        tparquet::FileMetaData metadata;
        metadata.__set_version(1);
        metadata.__set_num_rows(1);
        metadata.__set_row_groups({row_group});

        auto request = request_with_bloom_conjunct(type, {Field::create_field<TYPE_INT>(2)});
        std::vector<std::unique_ptr<format::parquet::ParquetColumnSchema>> schema;
        schema.push_back(std::move(column_schema));
        format::parquet::ParquetFileContext file_context;
        file_context.native_file =
                std::make_shared<StatisticsMemoryFileReader>(std::move(bytes), fail_reads);
        std::vector<int> selected_row_groups;
        format::parquet::ParquetPruningStats pruning_stats;
        ASSERT_TRUE(format::parquet::select_row_groups_by_metadata(
                            metadata, schema, request, nullptr, &selected_row_groups, true,
                            &pruning_stats, nullptr, nullptr, &file_context)
                            .ok());
        EXPECT_EQ(selected_row_groups, std::vector<int>({0}));
        EXPECT_EQ(pruning_stats.bloom_filter_probe_attempts, 1);
        EXPECT_EQ(pruning_stats.bloom_filter_probe_successes, 0);
        EXPECT_EQ(pruning_stats.bloom_filter_conservative_fallbacks, 1);
        EXPECT_EQ(pruning_stats.bloom_filter_corrupt_rejections, expected_corrupt_rejections);
    };

    run_case({}, false, false, 0);                // missing metadata offset
    run_case({0xff, 0xff, 0xff}, true, false, 1); // malformed header
    auto truncated = make_valid_bloom();
    truncated.resize(truncated.size() - 16);
    run_case(std::move(truncated), true, false, 1); // truncated payload
    run_case(make_valid_bloom(), true, true, 0);    // I/O failure
}

TEST(ParquetBloomFilterPruningTest, NativeBloomPreservesFirstLogicalProbeOrder) {
    const auto make_bloom = [] {
        format::parquet::native::BlockSplitBloomFilter bloom_filter;
        EXPECT_TRUE(bloom_filter
                            .init(segment_v2::BloomFilter::MINIMUM_BYTES,
                                  segment_v2::HashStrategyPB::XX_HASH_64)
                            .ok());
        const int32_t present_value = 1;
        bloom_filter.add_bytes(reinterpret_cast<const char*>(&present_value),
                               sizeof(present_value));
        tparquet::BloomFilterAlgorithm algorithm;
        algorithm.__set_BLOCK(tparquet::SplitBlockAlgorithm());
        tparquet::BloomFilterHash hash;
        hash.__set_XXHASH(tparquet::XxHash());
        tparquet::BloomFilterCompression compression;
        compression.__set_UNCOMPRESSED(tparquet::Uncompressed());
        tparquet::BloomFilterHeader header;
        header.__set_numBytes(static_cast<int32_t>(bloom_filter.size()));
        header.__set_algorithm(algorithm);
        header.__set_hash(hash);
        header.__set_compression(compression);
        std::vector<uint8_t> bytes;
        ThriftSerializer serializer(/*compact=*/true, 64);
        EXPECT_TRUE(serializer.serialize(&header, &bytes).ok());
        bytes.insert(bytes.end(), bloom_filter.data(), bloom_filter.data() + bloom_filter.size());
        return bytes;
    };
    const auto bloom = make_bloom();
    std::vector<uint8_t> file_bytes = bloom;
    file_bytes.insert(file_bytes.end(), bloom.begin(), bloom.end());

    const auto type = std::make_shared<DataTypeInt32>();
    std::vector<std::unique_ptr<format::parquet::ParquetColumnSchema>> schema;
    for (int local_id = 0; local_id < 2; ++local_id) {
        auto column = std::make_unique<format::parquet::ParquetColumnSchema>();
        column->kind = format::parquet::ParquetColumnSchemaKind::PRIMITIVE;
        column->local_id = local_id;
        column->leaf_column_id = 1 - local_id;
        column->type = type;
        column->type_descriptor.doris_type = type;
        column->type_descriptor.physical_type = tparquet::Type::INT32;
        schema.push_back(std::move(column));
    }

    std::vector<tparquet::ColumnChunk> chunks;
    for (int leaf_id = 0; leaf_id < 2; ++leaf_id) {
        tparquet::ColumnMetaData column_metadata;
        column_metadata.__set_type(tparquet::Type::INT32);
        column_metadata.__set_codec(tparquet::CompressionCodec::UNCOMPRESSED);
        column_metadata.__set_num_values(1);
        column_metadata.__set_total_compressed_size(0);
        column_metadata.__set_data_page_offset(0);
        column_metadata.__set_bloom_filter_offset(leaf_id * bloom.size());
        column_metadata.__set_bloom_filter_length(static_cast<int32_t>(bloom.size()));
        tparquet::ColumnChunk chunk;
        chunk.__set_meta_data(column_metadata);
        chunks.push_back(std::move(chunk));
    }
    tparquet::RowGroup row_group;
    row_group.__set_columns(std::move(chunks));
    row_group.__set_total_byte_size(0);
    row_group.__set_num_rows(1);
    tparquet::FileMetaData metadata;
    metadata.__set_version(1);
    metadata.__set_num_rows(1);
    metadata.__set_row_groups({row_group});

    format::FileScanRequest request;
    request.local_positions.emplace(format::LocalColumnId(0), format::LocalIndex(0));
    request.local_positions.emplace(format::LocalColumnId(1), format::LocalIndex(1));
    request.conjuncts = {VExprContext::create_shared(std::make_shared<BloomInExpr>(
                                 1, type, std::vector<Field> {Field::create_field<TYPE_INT>(2)})),
                         VExprContext::create_shared(std::make_shared<BloomInExpr>(
                                 0, type, std::vector<Field> {Field::create_field<TYPE_INT>(1)}))};

    format::parquet::ParquetFileContext file_context;
    auto file_reader = std::make_shared<StatisticsMemoryFileReader>(std::move(file_bytes));
    file_context.native_file = file_reader;
    std::vector<int> selected_row_groups;
    format::parquet::ParquetPruningStats pruning_stats;
    ASSERT_TRUE(format::parquet::select_row_groups_by_metadata(
                        metadata, schema, request, nullptr, &selected_row_groups, true,
                        &pruning_stats, nullptr, nullptr, &file_context)
                        .ok());
    EXPECT_TRUE(selected_row_groups.empty());
    EXPECT_EQ(file_reader->read_count(), 2);
    EXPECT_EQ(pruning_stats.bloom_filter_probe_attempts, 1);
    EXPECT_EQ(pruning_stats.bloom_filter_probe_successes, 1);
}

TEST(ParquetBloomFilterPruningTest, NativeBloomPreservesOrderAcrossNestedAndDirectProbes) {
    const auto make_bloom = [] {
        format::parquet::native::BlockSplitBloomFilter bloom_filter;
        EXPECT_TRUE(bloom_filter
                            .init(segment_v2::BloomFilter::MINIMUM_BYTES,
                                  segment_v2::HashStrategyPB::XX_HASH_64)
                            .ok());
        const int32_t present_value = 1;
        bloom_filter.add_bytes(reinterpret_cast<const char*>(&present_value),
                               sizeof(present_value));
        tparquet::BloomFilterAlgorithm algorithm;
        algorithm.__set_BLOCK(tparquet::SplitBlockAlgorithm());
        tparquet::BloomFilterHash hash;
        hash.__set_XXHASH(tparquet::XxHash());
        tparquet::BloomFilterCompression compression;
        compression.__set_UNCOMPRESSED(tparquet::Uncompressed());
        tparquet::BloomFilterHeader header;
        header.__set_numBytes(static_cast<int32_t>(bloom_filter.size()));
        header.__set_algorithm(algorithm);
        header.__set_hash(hash);
        header.__set_compression(compression);
        std::vector<uint8_t> bytes;
        ThriftSerializer serializer(/*compact=*/true, 64);
        EXPECT_TRUE(serializer.serialize(&header, &bytes).ok());
        bytes.insert(bytes.end(), bloom_filter.data(), bloom_filter.data() + bloom_filter.size());
        return bytes;
    };
    const auto bloom = make_bloom();
    std::vector<uint8_t> file_bytes = bloom;
    file_bytes.insert(file_bytes.end(), bloom.begin(), bloom.end());

    const auto int_type = std::make_shared<DataTypeInt32>();
    const auto struct_type =
            std::make_shared<DataTypeStruct>(DataTypes {int_type}, Strings {"value"});
    auto direct_schema = std::make_unique<format::parquet::ParquetColumnSchema>();
    direct_schema->kind = format::parquet::ParquetColumnSchemaKind::PRIMITIVE;
    direct_schema->local_id = 0;
    direct_schema->leaf_column_id = 1;
    direct_schema->type = int_type;
    direct_schema->type_descriptor.doris_type = int_type;
    direct_schema->type_descriptor.physical_type = tparquet::Type::INT32;

    auto struct_schema = std::make_unique<format::parquet::ParquetColumnSchema>();
    struct_schema->kind = format::parquet::ParquetColumnSchemaKind::STRUCT;
    struct_schema->local_id = 1;
    struct_schema->name = "nested";
    struct_schema->type = struct_type;
    auto nested_leaf_schema = std::make_unique<format::parquet::ParquetColumnSchema>();
    nested_leaf_schema->kind = format::parquet::ParquetColumnSchemaKind::PRIMITIVE;
    nested_leaf_schema->local_id = 0;
    nested_leaf_schema->name = "value";
    nested_leaf_schema->leaf_column_id = 0;
    nested_leaf_schema->type = int_type;
    nested_leaf_schema->type_descriptor.doris_type = int_type;
    nested_leaf_schema->type_descriptor.physical_type = tparquet::Type::INT32;
    struct_schema->children.push_back(std::move(nested_leaf_schema));

    std::vector<tparquet::ColumnChunk> chunks;
    for (int leaf_id = 0; leaf_id < 2; ++leaf_id) {
        tparquet::ColumnMetaData column_metadata;
        column_metadata.__set_type(tparquet::Type::INT32);
        column_metadata.__set_codec(tparquet::CompressionCodec::UNCOMPRESSED);
        column_metadata.__set_num_values(1);
        column_metadata.__set_total_compressed_size(0);
        column_metadata.__set_data_page_offset(0);
        column_metadata.__set_bloom_filter_offset(leaf_id * bloom.size());
        column_metadata.__set_bloom_filter_length(static_cast<int32_t>(bloom.size()));
        tparquet::ColumnChunk chunk;
        chunk.__set_meta_data(column_metadata);
        chunks.push_back(std::move(chunk));
    }
    tparquet::RowGroup row_group;
    row_group.__set_columns(std::move(chunks));
    row_group.__set_total_byte_size(0);
    row_group.__set_num_rows(1);
    tparquet::FileMetaData metadata;
    metadata.__set_version(1);
    metadata.__set_num_rows(1);
    metadata.__set_row_groups({row_group});

    format::FileScanRequest request;
    request.local_positions.emplace(format::LocalColumnId(0), format::LocalIndex(0));
    request.local_positions.emplace(format::LocalColumnId(1), format::LocalIndex(1));
    auto nested_slot = VSlotRef::create_shared(0, 1, -1, struct_type, "nested");
    auto selector = VLiteral::create_shared(std::make_shared<DataTypeString>(),
                                            Field::create_field<TYPE_STRING>("value"));
    auto nested_accessor =
            std::make_shared<MetadataAccessorExpr>(int_type, std::move(nested_slot), selector);
    request.conjuncts = {
            VExprContext::create_shared(std::make_shared<BloomInExpr>(
                    std::move(nested_accessor),
                    std::vector<Field> {Field::create_field<TYPE_INT>(2)})),
            VExprContext::create_shared(std::make_shared<BloomInExpr>(
                    0, int_type, std::vector<Field> {Field::create_field<TYPE_INT>(1)}))};

    std::vector<std::unique_ptr<format::parquet::ParquetColumnSchema>> schema;
    schema.push_back(std::move(direct_schema));
    schema.push_back(std::move(struct_schema));
    format::parquet::ParquetFileContext file_context;
    auto file_reader = std::make_shared<StatisticsMemoryFileReader>(std::move(file_bytes));
    file_context.native_file = file_reader;
    std::vector<int> selected_row_groups;
    format::parquet::ParquetPruningStats pruning_stats;
    ASSERT_TRUE(format::parquet::select_row_groups_by_metadata(
                        metadata, schema, request, nullptr, &selected_row_groups, true,
                        &pruning_stats, nullptr, nullptr, &file_context)
                        .ok());
    EXPECT_TRUE(selected_row_groups.empty());
    EXPECT_EQ(file_reader->read_count(), 2);
}

TEST(ParquetBloomFilterPruningTest, NativeRowGroupKeepsPresentUint32AboveInt32Max) {
    auto column_schema =
            std::make_unique<format::parquet::ParquetColumnSchema>(uint32_parquet_bloom_schema());
    column_schema->local_id = 0;
    column_schema->leaf_column_id = 0;

    format::parquet::native::BlockSplitBloomFilter bloom_filter;
    ASSERT_TRUE(bloom_filter
                        .init(segment_v2::BloomFilter::MINIMUM_BYTES,
                              segment_v2::HashStrategyPB::XX_HASH_64)
                        .ok());
    const uint32_t present_value = 4000000000U;
    int32_t physical_value;
    memcpy(&physical_value, &present_value, sizeof(physical_value));
    bloom_filter.add_bytes(reinterpret_cast<const char*>(&physical_value), sizeof(physical_value));

    tparquet::BloomFilterAlgorithm algorithm;
    algorithm.__set_BLOCK(tparquet::SplitBlockAlgorithm());
    tparquet::BloomFilterHash hash;
    hash.__set_XXHASH(tparquet::XxHash());
    tparquet::BloomFilterCompression compression;
    compression.__set_UNCOMPRESSED(tparquet::Uncompressed());
    tparquet::BloomFilterHeader bloom_header;
    bloom_header.__set_numBytes(static_cast<int32_t>(bloom_filter.size()));
    bloom_header.__set_algorithm(algorithm);
    bloom_header.__set_hash(hash);
    bloom_header.__set_compression(compression);
    std::vector<uint8_t> bloom_bytes;
    ThriftSerializer serializer(/*compact=*/true, 64);
    ASSERT_TRUE(serializer.serialize(&bloom_header, &bloom_bytes).ok());
    bloom_bytes.insert(bloom_bytes.end(), bloom_filter.data(),
                       bloom_filter.data() + bloom_filter.size());

    tparquet::ColumnMetaData column_metadata;
    column_metadata.__set_type(tparquet::Type::INT32);
    column_metadata.__set_codec(tparquet::CompressionCodec::UNCOMPRESSED);
    column_metadata.__set_num_values(1);
    column_metadata.__set_total_compressed_size(0);
    column_metadata.__set_data_page_offset(0);
    column_metadata.__set_bloom_filter_offset(0);
    column_metadata.__set_bloom_filter_length(static_cast<int32_t>(bloom_bytes.size()));
    tparquet::ColumnChunk chunk;
    chunk.__set_meta_data(column_metadata);
    tparquet::RowGroup row_group;
    row_group.__set_columns({chunk});
    row_group.__set_total_byte_size(0);
    row_group.__set_num_rows(1);
    tparquet::FileMetaData metadata;
    metadata.__set_version(1);
    metadata.__set_num_rows(1);
    metadata.__set_row_groups({row_group});

    format::parquet::ParquetFileContext file_context;
    file_context.native_file = std::make_shared<StatisticsMemoryFileReader>(std::move(bloom_bytes));
    auto request = request_with_bloom_conjunct(
            column_schema->type,
            {Field::create_field<TYPE_BIGINT>(static_cast<int64_t>(present_value))});
    std::vector<std::unique_ptr<format::parquet::ParquetColumnSchema>> schema;
    schema.push_back(std::move(column_schema));
    std::vector<int> selected_row_groups;
    format::parquet::ParquetPruningStats pruning_stats;
    ASSERT_TRUE(format::parquet::select_row_groups_by_metadata(
                        metadata, schema, request, nullptr, &selected_row_groups, true,
                        &pruning_stats, nullptr, nullptr, &file_context)
                        .ok());
    EXPECT_EQ(selected_row_groups, std::vector<int>({0}));
    EXPECT_EQ(pruning_stats.filtered_row_groups_by_bloom_filter, 0);
}

TEST(NativeParquetStatisticsTest, EmptyDictionaryRowGroupIsSkippedBeforeMetadataProbes) {
    tparquet::SchemaElement root;
    root.__set_name("schema");
    root.__set_num_children(1);
    tparquet::SchemaElement leaf;
    leaf.__set_name("value");
    leaf.__set_type(tparquet::Type::BYTE_ARRAY);
    leaf.__set_repetition_type(tparquet::FieldRepetitionType::REQUIRED);

    tparquet::ColumnMetaData column_metadata;
    column_metadata.__set_type(tparquet::Type::BYTE_ARRAY);
    column_metadata.__set_codec(tparquet::CompressionCodec::UNCOMPRESSED);
    column_metadata.__set_num_values(0);
    column_metadata.__set_total_compressed_size(0);
    column_metadata.__set_data_page_offset(0);
    column_metadata.__set_dictionary_page_offset(0);
    column_metadata.__set_encodings({tparquet::Encoding::RLE_DICTIONARY});
    tparquet::ColumnChunk chunk;
    chunk.__set_meta_data(column_metadata);
    tparquet::RowGroup row_group;
    row_group.__set_columns({chunk});
    row_group.__set_total_byte_size(0);
    row_group.__set_num_rows(0);
    tparquet::FileMetaData thrift_metadata;
    thrift_metadata.__set_version(1);
    thrift_metadata.__set_schema({root, leaf});
    thrift_metadata.__set_num_rows(0);
    thrift_metadata.__set_row_groups({row_group});

    format::parquet::NativeParquetMetadata native_metadata(thrift_metadata, 0);
    ASSERT_TRUE(native_metadata.init_schema(false, false).ok());
    format::parquet::ParquetFileContext file_context;
    file_context.native_file =
            std::make_shared<StatisticsMemoryFileReader>(std::vector<uint8_t> {});
    file_context.native_metadata = &native_metadata;

    auto column_schema = std::make_unique<format::parquet::ParquetColumnSchema>();
    column_schema->local_id = 0;
    column_schema->leaf_column_id = 0;
    column_schema->type = std::make_shared<DataTypeString>();
    column_schema->type_descriptor.doris_type = column_schema->type;
    column_schema->type_descriptor.physical_type = tparquet::Type::BYTE_ARRAY;
    column_schema->type_descriptor.is_string_like = true;
    std::vector<std::unique_ptr<format::parquet::ParquetColumnSchema>> schema;
    schema.push_back(std::move(column_schema));

    format::FileScanRequest request;
    request.local_positions.emplace(format::LocalColumnId(0), format::LocalIndex(0));
    request.predicate_columns = {format::LocalColumnIndex::top_level(format::LocalColumnId(0))};
    request.conjuncts = {VExprContext::create_shared(std::make_shared<DictionaryStringInExpr>())};
    std::vector<int> selected_row_groups;
    format::parquet::ParquetPruningStats pruning_stats;
    ASSERT_TRUE(format::parquet::select_row_groups_by_metadata(
                        thrift_metadata, schema, request, nullptr, &selected_row_groups, true,
                        &pruning_stats, nullptr, nullptr, &file_context)
                        .ok());
    EXPECT_TRUE(selected_row_groups.empty());
}

TEST(NativeParquetStatisticsTest, InvalidCandidateRowGroupReturnsCorruption) {
    tparquet::RowGroup row_group;
    row_group.__set_num_rows(1);
    tparquet::FileMetaData metadata;
    metadata.__set_row_groups({row_group});
    format::FileScanRequest request;
    const std::vector<std::unique_ptr<format::parquet::ParquetColumnSchema>> schema;
    const std::vector<int> candidates {1};
    std::vector<int> selected_row_groups;

    const auto status = format::parquet::select_row_groups_by_metadata(
            metadata, schema, request, &candidates, &selected_row_groups, false, nullptr, nullptr,
            nullptr, nullptr);
    EXPECT_TRUE(status.is<ErrorCode::CORRUPTION>()) << status;
}
TEST(NativeParquetStatisticsTest, LegacyBinaryFooterBoundsRequireComparableOrdering) {
    format::parquet::ParquetTypeDescriptor binary_type;
    binary_type.physical_type = tparquet::Type::BYTE_ARRAY;

    tparquet::Statistics max_only;
    max_only.__set_max("III");
    EXPECT_FALSE(
            format::parquet::detail::can_use_native_footer_min_max(binary_type, max_only, false));

    tparquet::Statistics legacy_different;
    legacy_different.__set_min("III");
    legacy_different.__set_max("\xe6\x98\xaf");
    EXPECT_FALSE(format::parquet::detail::can_use_native_footer_min_max(binary_type,
                                                                        legacy_different, false));

    tparquet::Statistics legacy_equal;
    legacy_equal.__set_min("same");
    legacy_equal.__set_max("same");
    EXPECT_TRUE(format::parquet::detail::can_use_native_footer_min_max(binary_type, legacy_equal,
                                                                       false));

    tparquet::Statistics type_defined;
    type_defined.__set_min_value("III");
    type_defined.__set_max_value("\xe6\x98\xaf");
    EXPECT_FALSE(format::parquet::detail::can_use_native_footer_min_max(binary_type, type_defined,
                                                                        false));
    EXPECT_TRUE(format::parquet::detail::can_use_native_footer_min_max(binary_type, type_defined,
                                                                       true));

    tparquet::Statistics mixed_fields;
    mixed_fields.__set_min_value("III");
    mixed_fields.__set_max("\xe6\x98\xaf");
    EXPECT_FALSE(format::parquet::detail::can_use_native_footer_min_max(binary_type, mixed_fields,
                                                                        true));
}

TEST(NativeParquetStatisticsTest, ExplicitlyInexactNumericBoundsCannotBackMinMaxAggregate) {
    format::parquet::ParquetTypeDescriptor int32_type;
    int32_type.physical_type = tparquet::Type::INT32;
    auto encode_int32 = [](int32_t value) {
        std::string bytes(sizeof(value), '\0');
        memcpy(bytes.data(), &value, sizeof(value));
        return bytes;
    };

    tparquet::Statistics statistics;
    statistics.__set_min_value(encode_int32(0));
    statistics.__set_max_value(encode_int32(100));
    EXPECT_TRUE(
            format::parquet::detail::can_use_native_footer_min_max(int32_type, statistics, true));

    statistics.__set_is_min_value_exact(false);
    statistics.__set_is_max_value_exact(true);
    EXPECT_FALSE(
            format::parquet::detail::can_use_native_footer_min_max(int32_type, statistics, true));

    statistics.__set_is_min_value_exact(true);
    statistics.__set_is_max_value_exact(false);
    EXPECT_FALSE(
            format::parquet::detail::can_use_native_footer_min_max(int32_type, statistics, true));
}

TEST(NativeParquetStatisticsTest, TypeDefinedBoundsRequireSupportedColumnOrder) {
    auto encode_int32 = [](int32_t value) {
        std::string bytes(sizeof(value), '\0');
        memcpy(bytes.data(), &value, sizeof(value));
        return bytes;
    };

    auto column_schema = std::make_unique<format::parquet::ParquetColumnSchema>();
    column_schema->kind = format::parquet::ParquetColumnSchemaKind::PRIMITIVE;
    column_schema->local_id = 0;
    column_schema->leaf_column_id = 0;
    column_schema->type = std::make_shared<DataTypeInt32>();
    column_schema->type_descriptor.doris_type = column_schema->type;
    column_schema->type_descriptor.physical_type = tparquet::Type::INT32;
    std::vector<std::unique_ptr<format::parquet::ParquetColumnSchema>> schema;
    schema.push_back(std::move(column_schema));

    tparquet::Statistics statistics;
    statistics.__set_min_value(encode_int32(1));
    statistics.__set_max_value(encode_int32(2));
    statistics.__set_null_count(0);
    tparquet::ColumnMetaData column_metadata;
    column_metadata.__set_type(tparquet::Type::INT32);
    column_metadata.__set_num_values(1);
    column_metadata.__set_statistics(statistics);
    tparquet::ColumnChunk chunk;
    chunk.__set_meta_data(column_metadata);
    tparquet::RowGroup row_group;
    row_group.__set_columns({chunk});
    row_group.__set_num_rows(1);
    tparquet::FileMetaData metadata;
    metadata.__set_row_groups({row_group});

    format::FileScanRequest request;
    request.local_positions.emplace(format::LocalColumnId(0), format::LocalIndex(0));
    request.predicate_columns = {format::LocalColumnIndex::top_level(format::LocalColumnId(0))};
    request.conjuncts = {
            VExprContext::create_shared(std::make_shared<MetadataInt32GreaterThanExpr>(100))};
    std::vector<int> selected_row_groups;
    ASSERT_TRUE(format::parquet::select_row_groups_by_metadata(metadata, schema, request, nullptr,
                                                               &selected_row_groups, false, nullptr)
                        .ok());
    EXPECT_EQ(selected_row_groups, std::vector<int>({0}));

    format::parquet::NativeParquetPageIndex page_index;
    page_index.column_index.__set_min_values({encode_int32(1)});
    page_index.column_index.__set_max_values({encode_int32(2)});
    page_index.column_index.__set_null_pages({false});
    page_index.column_index.__set_null_counts({0});
    tparquet::PageLocation location;
    location.__set_offset(0);
    location.__set_compressed_page_size(10);
    location.__set_first_row_index(0);
    page_index.offset_index.__set_page_locations({location});
    std::unordered_map<int, format::parquet::NativeParquetPageIndex> page_indexes;
    page_indexes.emplace(0, page_index);
    std::vector<format::parquet::RowRange> selected_ranges;
    std::map<int, format::parquet::ParquetPageSkipPlan> skip_plans;
    ASSERT_TRUE(format::parquet::select_row_group_ranges_by_native_page_index(
                        metadata, metadata.row_groups[0], page_indexes, schema, request, 1,
                        &selected_ranges, &skip_plans, nullptr)
                        .ok());
    EXPECT_EQ(selected_ranges.size(), 1);

    tparquet::ColumnOrder order;
    order.__set_TYPE_ORDER(tparquet::TypeDefinedOrder());
    metadata.__set_column_orders({order});
    selected_row_groups.clear();
    ASSERT_TRUE(format::parquet::select_row_groups_by_metadata(metadata, schema, request, nullptr,
                                                               &selected_row_groups, false, nullptr)
                        .ok());
    EXPECT_TRUE(selected_row_groups.empty());
    ASSERT_TRUE(format::parquet::select_row_group_ranges_by_native_page_index(
                        metadata, metadata.row_groups[0], page_indexes, schema, request, 1,
                        &selected_ranges, &skip_plans, nullptr)
                        .ok());
    EXPECT_TRUE(selected_ranges.empty());
}

TEST(NativeParquetStatisticsTest, ZonemapPruningIgnoresDisabledSessionSwitch) {
    auto encode_int32 = [](int32_t value) {
        std::string bytes(sizeof(value), '\0');
        memcpy(bytes.data(), &value, sizeof(value));
        return bytes;
    };

    auto column_schema = std::make_unique<format::parquet::ParquetColumnSchema>();
    column_schema->kind = format::parquet::ParquetColumnSchemaKind::PRIMITIVE;
    column_schema->local_id = 0;
    column_schema->leaf_column_id = 0;
    column_schema->type = std::make_shared<DataTypeInt32>();
    column_schema->type_descriptor.doris_type = column_schema->type;
    column_schema->type_descriptor.physical_type = tparquet::Type::INT32;
    std::vector<std::unique_ptr<format::parquet::ParquetColumnSchema>> schema;
    schema.push_back(std::move(column_schema));

    tparquet::Statistics statistics;
    statistics.__set_min_value(encode_int32(1));
    statistics.__set_max_value(encode_int32(2));
    statistics.__set_null_count(0);
    tparquet::ColumnMetaData column_metadata;
    column_metadata.__set_type(tparquet::Type::INT32);
    column_metadata.__set_num_values(1);
    column_metadata.__set_statistics(statistics);
    tparquet::ColumnChunk chunk;
    chunk.__set_meta_data(column_metadata);
    tparquet::RowGroup row_group;
    row_group.__set_columns({chunk});
    row_group.__set_num_rows(1);
    tparquet::ColumnOrder order;
    order.__set_TYPE_ORDER(tparquet::TypeDefinedOrder());
    tparquet::FileMetaData metadata;
    metadata.__set_column_orders({order});
    metadata.__set_row_groups({row_group});

    format::FileScanRequest request;
    request.local_positions.emplace(format::LocalColumnId(0), format::LocalIndex(0));
    request.predicate_columns = {format::LocalColumnIndex::top_level(format::LocalColumnId(0))};
    request.conjuncts = {
            VExprContext::create_shared(std::make_shared<MetadataInt32GreaterThanExpr>(100))};

    TQueryOptions query_options;
    query_options.__set_enable_expr_zonemap_filter(false);
    RuntimeState state {query_options, TQueryGlobals()};
    std::vector<int> selected_row_groups;
    ASSERT_TRUE(format::parquet::select_row_groups_by_metadata(metadata, schema, request, nullptr,
                                                               &selected_row_groups, false, nullptr,
                                                               nullptr, &state)
                        .ok());
    EXPECT_TRUE(selected_row_groups.empty());

    format::parquet::NativeParquetPageIndex page_index;
    page_index.column_index.__set_min_values({encode_int32(1)});
    page_index.column_index.__set_max_values({encode_int32(2)});
    page_index.column_index.__set_null_pages({false});
    page_index.column_index.__set_null_counts({0});
    tparquet::PageLocation location;
    location.__set_offset(0);
    location.__set_compressed_page_size(10);
    location.__set_first_row_index(0);
    page_index.offset_index.__set_page_locations({location});
    std::unordered_map<int, format::parquet::NativeParquetPageIndex> page_indexes;
    page_indexes.emplace(0, std::move(page_index));
    std::vector<format::parquet::RowRange> selected_ranges;
    std::map<int, format::parquet::ParquetPageSkipPlan> skip_plans;
    ASSERT_TRUE(format::parquet::select_row_group_ranges_by_native_page_index(
                        metadata, metadata.row_groups[0], page_indexes, schema, request, 1,
                        &selected_ranges, &skip_plans, nullptr, nullptr, &state)
                        .ok());
    EXPECT_TRUE(selected_ranges.empty());
}

TEST(NativeParquetStatisticsTest, ContradictoryAllNullPageCountsDisablePruning) {
    auto column_schema = std::make_unique<format::parquet::ParquetColumnSchema>();
    column_schema->kind = format::parquet::ParquetColumnSchemaKind::PRIMITIVE;
    column_schema->local_id = 0;
    column_schema->leaf_column_id = 0;
    column_schema->type = std::make_shared<DataTypeInt32>();
    column_schema->type_descriptor.doris_type = column_schema->type;
    column_schema->type_descriptor.physical_type = tparquet::Type::INT32;
    std::vector<std::unique_ptr<format::parquet::ParquetColumnSchema>> schema;
    schema.push_back(std::move(column_schema));

    tparquet::ColumnOrder order;
    order.__set_TYPE_ORDER(tparquet::TypeDefinedOrder());
    tparquet::FileMetaData metadata;
    metadata.__set_column_orders({order});
    format::FileScanRequest request;
    request.local_positions.emplace(format::LocalColumnId(0), format::LocalIndex(0));
    request.predicate_columns = {format::LocalColumnIndex::top_level(format::LocalColumnId(0))};
    request.conjuncts = {
            VExprContext::create_shared(std::make_shared<MetadataInt32GreaterThanExpr>(0))};

    for (const int64_t contradictory_null_count : {5, 11}) {
        SCOPED_TRACE(contradictory_null_count);
        format::parquet::NativeParquetPageIndex page_index;
        page_index.column_index.__set_null_pages({true});
        page_index.column_index.__set_null_counts({contradictory_null_count});
        tparquet::PageLocation location;
        location.__set_offset(0);
        location.__set_compressed_page_size(10);
        location.__set_first_row_index(0);
        page_index.offset_index.__set_page_locations({location});
        std::unordered_map<int, format::parquet::NativeParquetPageIndex> page_indexes;
        page_indexes.emplace(0, std::move(page_index));
        std::vector<format::parquet::RowRange> selected_ranges;
        std::map<int, format::parquet::ParquetPageSkipPlan> skip_plans;

        ASSERT_TRUE(format::parquet::select_row_group_ranges_by_native_page_index(
                            metadata, tparquet::RowGroup {}, page_indexes, schema, request, 10,
                            &selected_ranges, &skip_plans, nullptr)
                            .ok());
        // ColumnIndex is optional. An impossible all-null claim must fall back to reading the
        // ten-row data page instead of proving that no value can satisfy the predicate.
        ASSERT_EQ(selected_ranges.size(), 1);
        EXPECT_EQ(selected_ranges[0].start, 0);
        EXPECT_EQ(selected_ranges[0].length, 10);
    }
}

TEST(NativeParquetStatisticsTest, ShreddedVariantTypedValueDrivesPageFiltering) {
    auto encode_int32 = [](int32_t value) {
        std::string bytes(sizeof(value), '\0');
        memcpy(bytes.data(), &value, sizeof(value));
        return bytes;
    };
    auto primitive = [](std::string name, int local_id, int leaf_id) {
        auto schema = std::make_unique<format::parquet::ParquetColumnSchema>();
        schema->name = std::move(name);
        schema->local_id = local_id;
        schema->leaf_column_id = leaf_id;
        schema->kind = format::parquet::ParquetColumnSchemaKind::PRIMITIVE;
        schema->type = make_nullable(std::make_shared<DataTypeInt32>());
        schema->type_descriptor.doris_type = schema->type;
        schema->type_descriptor.physical_type = tparquet::Type::INT32;
        return schema;
    };
    auto bytes = [&](std::string name, int local_id, int leaf_id) {
        auto schema = primitive(std::move(name), local_id, leaf_id);
        schema->type = make_nullable(std::make_shared<DataTypeString>());
        schema->type_descriptor.doris_type = schema->type;
        schema->type_descriptor.physical_type = tparquet::Type::BYTE_ARRAY;
        return schema;
    };

    auto variant = std::make_unique<format::parquet::ParquetColumnSchema>();
    variant->name = "v";
    variant->local_id = 0;
    variant->kind = format::parquet::ParquetColumnSchemaKind::VARIANT;
    variant->contains_variant = true;
    variant->type = make_nullable(std::make_shared<DataTypeVariantV2>());
    variant->children.push_back(bytes("metadata", 0, 0));
    variant->children.push_back(bytes("value", 1, 1));
    auto typed_object = std::make_unique<format::parquet::ParquetColumnSchema>();
    typed_object->name = "typed_value";
    typed_object->local_id = 2;
    typed_object->kind = format::parquet::ParquetColumnSchemaKind::STRUCT;
    auto field = std::make_unique<format::parquet::ParquetColumnSchema>();
    field->name = "col";
    field->local_id = 0;
    field->kind = format::parquet::ParquetColumnSchemaKind::STRUCT;
    field->children.push_back(bytes("value", 0, 2));
    field->children.push_back(primitive("typed_value", 1, 3));
    typed_object->children.push_back(std::move(field));
    variant->children.push_back(std::move(typed_object));
    std::vector<std::unique_ptr<format::parquet::ParquetColumnSchema>> schema;
    schema.push_back(std::move(variant));

    auto chunk = [&](tparquet::Type::type type, int64_t num_values, int64_t null_count,
                     std::optional<int32_t> min_value = std::nullopt,
                     std::optional<int32_t> max_value = std::nullopt) {
        tparquet::Statistics statistics;
        statistics.__set_null_count(null_count);
        if (min_value.has_value() && max_value.has_value()) {
            statistics.__set_min_value(encode_int32(*min_value));
            statistics.__set_max_value(encode_int32(*max_value));
        }
        tparquet::ColumnMetaData metadata;
        metadata.__set_type(type);
        metadata.__set_num_values(num_values);
        metadata.__set_statistics(std::move(statistics));
        tparquet::ColumnChunk result;
        result.__set_meta_data(std::move(metadata));
        return result;
    };
    tparquet::RowGroup row_group;
    row_group.__set_num_rows(100);
    row_group.__set_columns({chunk(tparquet::Type::BYTE_ARRAY, 100, 0),
                             chunk(tparquet::Type::BYTE_ARRAY, 100, 100),
                             chunk(tparquet::Type::BYTE_ARRAY, 100, 100),
                             chunk(tparquet::Type::INT32, 100, 0, 1, 200)});
    tparquet::ColumnOrder order;
    order.__set_TYPE_ORDER(tparquet::TypeDefinedOrder());
    tparquet::FileMetaData metadata;
    metadata.__set_row_groups({row_group});
    metadata.__set_column_orders({order, order, order, order});

    format::FileScanRequest request;
    request.local_positions.emplace(format::LocalColumnId(0), format::LocalIndex(0));
    request.predicate_columns = {format::LocalColumnIndex::top_level(format::LocalColumnId(0))};
    request.conjuncts = {variant_path_gt_conjunct(50)};

    auto footer_only_metadata = metadata;
    footer_only_metadata.row_groups[0].columns[3].meta_data.statistics.__set_max_value(
            encode_int32(2));
    std::vector<int> selected_row_groups;
    ASSERT_TRUE(format::parquet::select_row_groups_by_metadata(
                        footer_only_metadata, schema, request, nullptr, &selected_row_groups, false,
                        nullptr, nullptr, nullptr, nullptr, {},
                        format::parquet::ParquetMetadataProbeMode::FOOTER_ONLY)
                        .ok());
    EXPECT_TRUE(selected_row_groups.empty());

    // The same predicate can be localized after an earlier unsafe conjunct. Metadata pruning must
    // preserve that earlier expression's row-level error instead of skipping the whole row group.
    request.metadata_pruning_safe_conjunct_count = 0;
    ASSERT_TRUE(format::parquet::select_row_groups_by_metadata(
                        footer_only_metadata, schema, request, nullptr, &selected_row_groups, false,
                        nullptr, nullptr, nullptr, nullptr, {},
                        format::parquet::ParquetMetadataProbeMode::FOOTER_ONLY)
                        .ok());
    EXPECT_EQ(selected_row_groups, std::vector<int>({0}));
    request.metadata_pruning_safe_conjunct_count = request.conjuncts.size();

    auto leaf_projection = format::LocalColumnIndex::partial_local(0);
    auto typed_object_projection = format::LocalColumnIndex::partial_local(2);
    auto field_projection = format::LocalColumnIndex::partial_local(0);
    field_projection.children.push_back(format::LocalColumnIndex::local(1));
    typed_object_projection.children.push_back(std::move(field_projection));
    leaf_projection.children.push_back(std::move(typed_object_projection));
    request.predicate_columns = {std::move(leaf_projection)};
    for (int leaf = 0; leaf < 4; ++leaf) {
        footer_only_metadata.row_groups[0].columns[leaf].meta_data.__set_total_compressed_size(
                (leaf + 1) * 10);
    }
    format::parquet::ParquetPruningStats leaf_pruning_stats;
    ASSERT_TRUE(format::parquet::select_row_groups_by_metadata(
                        footer_only_metadata, schema, request, nullptr, &selected_row_groups, false,
                        &leaf_pruning_stats, nullptr, nullptr, nullptr, {},
                        format::parquet::ParquetMetadataProbeMode::FOOTER_ONLY)
                        .ok());
    EXPECT_TRUE(selected_row_groups.empty());
    EXPECT_EQ(leaf_pruning_stats.filtered_bytes, 40);

    request.conjuncts = {variant_path_gt_conjunct(50, false, true)};
    ASSERT_TRUE(format::parquet::select_row_groups_by_metadata(
                        footer_only_metadata, schema, request, nullptr, &selected_row_groups, false,
                        nullptr, nullptr, nullptr, nullptr, {},
                        format::parquet::ParquetMetadataProbeMode::FOOTER_ONLY)
                        .ok());
    EXPECT_TRUE(selected_row_groups.empty());
    request.conjuncts = {variant_path_gt_conjunct(50)};

    // Missing typed statistics provide no proof and must retain the row group.
    footer_only_metadata.row_groups[0].columns[3].meta_data.__isset.statistics = false;
    ASSERT_TRUE(format::parquet::select_row_groups_by_metadata(
                        footer_only_metadata, schema, request, nullptr, &selected_row_groups, false,
                        nullptr, nullptr, nullptr, nullptr, {},
                        format::parquet::ParquetMetadataProbeMode::FOOTER_ONLY)
                        .ok());
    EXPECT_EQ(selected_row_groups, std::vector<int>({0}));

    // A populated fallback for the same path invalidates both footer and page pruning.
    footer_only_metadata = metadata;
    footer_only_metadata.row_groups[0].columns[3].meta_data.statistics.__set_max_value(
            encode_int32(2));
    footer_only_metadata.row_groups[0].columns[2].meta_data.statistics.__set_null_count(99);
    ASSERT_TRUE(format::parquet::select_row_groups_by_metadata(
                        footer_only_metadata, schema, request, nullptr, &selected_row_groups, false,
                        nullptr, nullptr, nullptr, nullptr, {},
                        format::parquet::ParquetMetadataProbeMode::FOOTER_ONLY)
                        .ok());
    EXPECT_EQ(selected_row_groups, std::vector<int>({0}));

    // A contradictory non-repeated value count cannot prove that every row lacks fallback bytes.
    footer_only_metadata.row_groups[0].columns[2].meta_data.__set_num_values(99);
    footer_only_metadata.row_groups[0].columns[2].meta_data.statistics.__set_null_count(99);
    ASSERT_TRUE(format::parquet::select_row_groups_by_metadata(
                        footer_only_metadata, schema, request, nullptr, &selected_row_groups, false,
                        nullptr, nullptr, nullptr, nullptr, {},
                        format::parquet::ParquetMetadataProbeMode::FOOTER_ONLY)
                        .ok());
    EXPECT_EQ(selected_row_groups, std::vector<int>({0}));

    format::parquet::NativeParquetPageIndex typed_pages;
    typed_pages.column_index.__set_min_values({encode_int32(1), encode_int32(100)});
    typed_pages.column_index.__set_max_values({encode_int32(2), encode_int32(200)});
    typed_pages.column_index.__set_null_pages({false, false});
    typed_pages.column_index.__set_null_counts({0, 0});
    tparquet::PageLocation first;
    first.__set_offset(0);
    first.__set_compressed_page_size(10);
    first.__set_first_row_index(0);
    tparquet::PageLocation second;
    second.__set_offset(10);
    second.__set_compressed_page_size(10);
    second.__set_first_row_index(50);
    typed_pages.offset_index.__set_page_locations({first, second});
    std::unordered_map<int, format::parquet::NativeParquetPageIndex> page_indexes;
    page_indexes.emplace(3, std::move(typed_pages));

    std::vector<format::parquet::RowRange> selected_ranges;
    std::map<int, format::parquet::ParquetPageSkipPlan> skip_plans;
    format::parquet::ParquetPruningStats pruning_stats;
    ASSERT_TRUE(format::parquet::can_use_parquet_page_index(request, nullptr));
    TQueryOptions query_options;
    query_options.__set_enable_expr_zonemap_filter(false);
    RuntimeState generic_zonemap_disabled {query_options, TQueryGlobals()};
    EXPECT_TRUE(format::parquet::can_use_parquet_page_index(request, &generic_zonemap_disabled));
    ASSERT_TRUE(format::parquet::select_row_group_ranges_by_native_page_index(
                        metadata, metadata.row_groups[0], page_indexes, schema, request, 100,
                        &selected_ranges, &skip_plans, &pruning_stats, nullptr,
                        &generic_zonemap_disabled)
                        .ok());
    ASSERT_EQ(selected_ranges.size(), 1);
    EXPECT_EQ(selected_ranges[0].start, 50);
    EXPECT_EQ(selected_ranges[0].length, 50);
    EXPECT_EQ(pruning_stats.page_index_read_calls, 1);
    EXPECT_EQ(pruning_stats.filtered_page_rows, 50);

    request.metadata_pruning_safe_conjunct_count = 0;
    ASSERT_TRUE(format::parquet::select_row_group_ranges_by_native_page_index(
                        metadata, metadata.row_groups[0], page_indexes, schema, request, 100,
                        &selected_ranges, &skip_plans, nullptr)
                        .ok());
    ASSERT_EQ(selected_ranges.size(), 1);
    EXPECT_EQ(selected_ranges[0].start, 0);
    EXPECT_EQ(selected_ranges[0].length, 100);
    request.metadata_pruning_safe_conjunct_count = request.conjuncts.size();

    // Direct Variant numeric comparisons coerce integral literals to a wide DECIMAL domain.
    request.conjuncts = {variant_path_gt_conjunct(50, false, true)};
    ASSERT_TRUE(format::parquet::select_row_group_ranges_by_native_page_index(
                        metadata, metadata.row_groups[0], page_indexes, schema, request, 100,
                        &selected_ranges, &skip_plans, nullptr)
                        .ok());
    ASSERT_EQ(selected_ranges.size(), 1);
    EXPECT_EQ(selected_ranges[0].start, 50);
    EXPECT_EQ(selected_ranges[0].length, 50);

    // Bounds for the raw INT32 typed leaf are not valid for CAST(CAST(v['col'] AS TINYINT) AS INT).
    request.conjuncts = {variant_path_gt_conjunct(50, true)};
    ASSERT_TRUE(format::parquet::select_row_group_ranges_by_native_page_index(
                        metadata, metadata.row_groups[0], page_indexes, schema, request, 100,
                        &selected_ranges, &skip_plans, nullptr)
                        .ok());
    ASSERT_EQ(selected_ranges.size(), 1);
    EXPECT_EQ(selected_ranges[0].start, 0);
    EXPECT_EQ(selected_ranges[0].length, 100);

    // Raw binary and UUID bounds are physical bytes, while the residual Variant-to-STRING cast
    // compares their rendered values. Those domains differ, so neither footer nor page metadata
    // may exclude a row solely from the raw byte interval.
    auto assert_binary_identity_does_not_prune = [&](bool is_uuid) {
        auto* binary_leaf = schema[0]->children[2]->children[0]->children[1].get();
        binary_leaf->type = make_nullable(std::make_shared<DataTypeString>());
        binary_leaf->type_descriptor = {};
        binary_leaf->type_descriptor.doris_type = binary_leaf->type;
        binary_leaf->type_descriptor.physical_type =
                is_uuid ? tparquet::Type::FIXED_LEN_BYTE_ARRAY : tparquet::Type::BYTE_ARRAY;
        binary_leaf->type_descriptor.fixed_length = is_uuid ? 16 : -1;
        binary_leaf->type_descriptor.is_string_like = true;
        binary_leaf->type_descriptor.is_uuid = is_uuid;

        auto& binary_chunk = metadata.row_groups[0].columns[3].meta_data;
        binary_chunk.__set_type(binary_leaf->type_descriptor.physical_type);
        binary_chunk.statistics.__set_min_value("a");
        binary_chunk.statistics.__set_max_value("b");
        metadata.row_groups[0].columns[2].meta_data.statistics.__set_null_count(100);
        request.conjuncts = {variant_path_string_gt_conjunct("z")};
        ASSERT_TRUE(format::parquet::select_row_groups_by_metadata(
                            metadata, schema, request, nullptr, &selected_row_groups, false,
                            nullptr, nullptr, nullptr, nullptr, {},
                            format::parquet::ParquetMetadataProbeMode::FOOTER_ONLY)
                            .ok());
        EXPECT_EQ(selected_row_groups, std::vector<int>({0}));

        format::parquet::NativeParquetPageIndex binary_pages;
        binary_pages.column_index.__set_min_values({"a"});
        binary_pages.column_index.__set_max_values({"b"});
        binary_pages.column_index.__set_null_pages({false});
        binary_pages.column_index.__set_null_counts({0});
        tparquet::PageLocation binary_location;
        binary_location.__set_offset(0);
        binary_location.__set_compressed_page_size(10);
        binary_location.__set_first_row_index(0);
        binary_pages.offset_index.__set_page_locations({binary_location});
        page_indexes.clear();
        page_indexes.emplace(3, std::move(binary_pages));
        ASSERT_TRUE(format::parquet::select_row_group_ranges_by_native_page_index(
                            metadata, metadata.row_groups[0], page_indexes, schema, request, 100,
                            &selected_ranges, &skip_plans, nullptr)
                            .ok());
        ASSERT_EQ(selected_ranges.size(), 1);
        EXPECT_EQ(selected_ranges[0].start, 0);
        EXPECT_EQ(selected_ranges[0].length, 100);
    };
    assert_binary_identity_does_not_prune(false);
    assert_binary_identity_does_not_prune(true);

    // Parquet floating min/max omits NaN values. Without an explicit no-NaN proof, [0, NaN]
    // cannot be represented by max=0 and must not prune a Variant comparison that retains NaN.
    auto encode_float = [](float value) {
        std::string bytes(sizeof(value), '\0');
        memcpy(bytes.data(), &value, sizeof(value));
        return bytes;
    };
    auto* float_leaf = schema[0]->children[2]->children[0]->children[1].get();
    float_leaf->type = make_nullable(std::make_shared<DataTypeFloat32>());
    float_leaf->type_descriptor.doris_type = float_leaf->type;
    float_leaf->type_descriptor.physical_type = tparquet::Type::FLOAT;
    auto& float_chunk = metadata.row_groups[0].columns[3].meta_data;
    float_chunk.__set_type(tparquet::Type::FLOAT);
    float_chunk.statistics.__set_min_value(encode_float(0.0F));
    float_chunk.statistics.__set_max_value(encode_float(0.0F));
    metadata.row_groups[0].columns[2].meta_data.statistics.__set_null_count(100);
    request.conjuncts = {variant_path_float_gt_conjunct(1.0F)};
    ASSERT_TRUE(format::parquet::select_row_groups_by_metadata(
                        metadata, schema, request, nullptr, &selected_row_groups, false, nullptr,
                        nullptr, nullptr, nullptr, {},
                        format::parquet::ParquetMetadataProbeMode::FOOTER_ONLY)
                        .ok());
    EXPECT_EQ(selected_row_groups, std::vector<int>({0}));

    format::parquet::NativeParquetPageIndex float_pages;
    float_pages.column_index.__set_min_values({encode_float(0.0F)});
    float_pages.column_index.__set_max_values({encode_float(0.0F)});
    float_pages.column_index.__set_null_pages({false});
    float_pages.column_index.__set_null_counts({0});
    tparquet::PageLocation float_location;
    float_location.__set_offset(0);
    float_location.__set_compressed_page_size(10);
    float_location.__set_first_row_index(0);
    float_pages.offset_index.__set_page_locations({float_location});
    page_indexes.clear();
    page_indexes.emplace(3, std::move(float_pages));
    ASSERT_TRUE(format::parquet::select_row_group_ranges_by_native_page_index(
                        metadata, metadata.row_groups[0], page_indexes, schema, request, 100,
                        &selected_ranges, &skip_plans, nullptr)
                        .ok());
    ASSERT_EQ(selected_ranges.size(), 1);
    EXPECT_EQ(selected_ranges[0].start, 0);
    EXPECT_EQ(selected_ranges[0].length, 100);

    // A fallback value in the same row group may have a different Variant type. In that case the
    // typed bounds cannot prove anything about the SQL comparison, so all pages must be read.
    metadata.row_groups[0].columns[2].meta_data.statistics.__set_null_count(99);
    request.conjuncts = {variant_path_gt_conjunct(50)};
    ASSERT_TRUE(format::parquet::select_row_group_ranges_by_native_page_index(
                        metadata, metadata.row_groups[0], page_indexes, schema, request, 100,
                        &selected_ranges, &skip_plans, nullptr)
                        .ok());
    ASSERT_EQ(selected_ranges.size(), 1);
    EXPECT_EQ(selected_ranges[0].start, 0);
    EXPECT_EQ(selected_ranges[0].length, 100);
}

TEST(NativeParquetStatisticsTest, ShreddedVariantAncestorFallbackDisablesPruning) {
    auto encode_int32 = [](int32_t value) {
        std::string bytes(sizeof(value), '\0');
        memcpy(bytes.data(), &value, sizeof(value));
        return bytes;
    };
    auto primitive = [](std::string name, int local_id, int leaf_id, DataTypePtr type) {
        auto schema = std::make_unique<format::parquet::ParquetColumnSchema>();
        schema->name = std::move(name);
        schema->local_id = local_id;
        schema->leaf_column_id = leaf_id;
        schema->kind = format::parquet::ParquetColumnSchemaKind::PRIMITIVE;
        schema->type = make_nullable(std::move(type));
        schema->type_descriptor.doris_type = schema->type;
        schema->type_descriptor.physical_type =
                leaf_id == 4 ? tparquet::Type::INT32 : tparquet::Type::BYTE_ARRAY;
        return schema;
    };

    auto variant = std::make_unique<format::parquet::ParquetColumnSchema>();
    variant->name = "v";
    variant->local_id = 0;
    variant->kind = format::parquet::ParquetColumnSchemaKind::VARIANT;
    variant->contains_variant = true;
    variant->type = make_nullable(std::make_shared<DataTypeVariantV2>());
    variant->children.push_back(primitive("metadata", 0, 0, std::make_shared<DataTypeString>()));
    variant->children.push_back(primitive("value", 1, 1, std::make_shared<DataTypeString>()));

    auto root_typed = std::make_unique<format::parquet::ParquetColumnSchema>();
    root_typed->name = "typed_value";
    root_typed->local_id = 2;
    root_typed->kind = format::parquet::ParquetColumnSchemaKind::STRUCT;
    auto ancestor = std::make_unique<format::parquet::ParquetColumnSchema>();
    ancestor->name = "a";
    ancestor->local_id = 0;
    ancestor->kind = format::parquet::ParquetColumnSchemaKind::STRUCT;
    ancestor->children.push_back(primitive("value", 0, 2, std::make_shared<DataTypeString>()));
    auto ancestor_typed = std::make_unique<format::parquet::ParquetColumnSchema>();
    ancestor_typed->name = "typed_value";
    ancestor_typed->local_id = 1;
    ancestor_typed->kind = format::parquet::ParquetColumnSchemaKind::STRUCT;
    auto leaf_wrapper = std::make_unique<format::parquet::ParquetColumnSchema>();
    leaf_wrapper->name = "b";
    leaf_wrapper->local_id = 0;
    leaf_wrapper->kind = format::parquet::ParquetColumnSchemaKind::STRUCT;
    leaf_wrapper->children.push_back(primitive("value", 0, 3, std::make_shared<DataTypeString>()));
    leaf_wrapper->children.push_back(
            primitive("typed_value", 1, 4, std::make_shared<DataTypeInt32>()));
    ancestor_typed->children.push_back(std::move(leaf_wrapper));
    ancestor->children.push_back(std::move(ancestor_typed));
    root_typed->children.push_back(std::move(ancestor));
    variant->children.push_back(std::move(root_typed));
    std::vector<std::unique_ptr<format::parquet::ParquetColumnSchema>> schema;
    schema.push_back(std::move(variant));

    auto chunk = [&](tparquet::Type::type type, int64_t null_count,
                     std::optional<int32_t> min_value = std::nullopt,
                     std::optional<int32_t> max_value = std::nullopt) {
        tparquet::Statistics statistics;
        statistics.__set_null_count(null_count);
        if (min_value.has_value() && max_value.has_value()) {
            statistics.__set_min_value(encode_int32(*min_value));
            statistics.__set_max_value(encode_int32(*max_value));
        }
        tparquet::ColumnMetaData column_metadata;
        column_metadata.__set_type(type);
        column_metadata.__set_num_values(100);
        column_metadata.__set_statistics(std::move(statistics));
        tparquet::ColumnChunk result;
        result.__set_meta_data(std::move(column_metadata));
        return result;
    };
    tparquet::RowGroup row_group;
    row_group.__set_num_rows(100);
    row_group.__set_columns(
            {chunk(tparquet::Type::BYTE_ARRAY, 0), chunk(tparquet::Type::BYTE_ARRAY, 100),
             chunk(tparquet::Type::BYTE_ARRAY, 99), chunk(tparquet::Type::BYTE_ARRAY, 100),
             chunk(tparquet::Type::INT32, 0, 1, 2)});
    tparquet::ColumnOrder order;
    order.__set_TYPE_ORDER(tparquet::TypeDefinedOrder());
    tparquet::FileMetaData metadata;
    metadata.__set_row_groups({row_group});
    metadata.__set_column_orders({order, order, order, order, order});

    format::FileScanRequest request;
    request.local_positions.emplace(format::LocalColumnId(0), format::LocalIndex(0));
    request.predicate_columns = {format::LocalColumnIndex::top_level(format::LocalColumnId(0))};
    request.conjuncts = {nested_variant_path_gt_conjunct(50)};

    std::vector<int> selected_row_groups;
    ASSERT_TRUE(format::parquet::select_row_groups_by_metadata(
                        metadata, schema, request, nullptr, &selected_row_groups, false, nullptr,
                        nullptr, nullptr, nullptr, {},
                        format::parquet::ParquetMetadataProbeMode::FOOTER_ONLY)
                        .ok());
    EXPECT_EQ(selected_row_groups, std::vector<int>({0}));

    format::parquet::NativeParquetPageIndex typed_pages;
    typed_pages.column_index.__set_min_values({encode_int32(1)});
    typed_pages.column_index.__set_max_values({encode_int32(2)});
    typed_pages.column_index.__set_null_pages({false});
    typed_pages.column_index.__set_null_counts({0});
    tparquet::PageLocation location;
    location.__set_offset(0);
    location.__set_compressed_page_size(10);
    location.__set_first_row_index(0);
    typed_pages.offset_index.__set_page_locations({location});
    std::unordered_map<int, format::parquet::NativeParquetPageIndex> page_indexes;
    page_indexes.emplace(4, std::move(typed_pages));
    std::vector<format::parquet::RowRange> selected_ranges;
    std::map<int, format::parquet::ParquetPageSkipPlan> skip_plans;
    ASSERT_TRUE(format::parquet::select_row_group_ranges_by_native_page_index(
                        metadata, metadata.row_groups[0], page_indexes, schema, request, 100,
                        &selected_ranges, &skip_plans, nullptr)
                        .ok());
    ASSERT_EQ(selected_ranges.size(), 1);
    EXPECT_EQ(selected_ranges[0].start, 0);
    EXPECT_EQ(selected_ranges[0].length, 100);
}

} // namespace
} // namespace doris
