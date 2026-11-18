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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <hll.hpp>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "common/check.h"
#include "common/exception.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_numbers.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_varbinary.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_varbinary.h"
#include "core/string_ref.h"
#include "exprs/datasketches_hll_util.h"
#include "exprs/function/function.h"
#include "exprs/function/simple_function_factory.h"
#include "exprs/function_context.h"
#include "util/string_util.h"

namespace doris {

// datasketches_hll_downsample(sketch, lg_k [, tgt_type]) rewrites a serialized DataSketches HLL
// sketch so that its lgK is min(lg_k, source lgK) and its HLL type is tgt_type (HLL_4 by default).
//
// A sketch that already has lgK <= lg_k and type tgt_type is returned byte for byte after it is
// fully deserialized. Callers rely on this: `datasketches_hll_downsample(sk, k, t) <> sk` is true
// exactly when sk's lgK is above k or its type differs from t, which tells whether a row still
// needs to be rewritten. For example, with lg_k = 12 and tgt_type = HLL_4:
//   lgK 14 HLL_8 -> lgK 12 HLL_4 (new bytes)
//   lgK 10 HLL_8 -> lgK 10 HLL_4 (new bytes, only the type changes)
//   lgK 10 HLL_4 -> the input bytes
class FunctionDataSketchesHllDownsample : public IFunction {
public:
    static constexpr auto name = "datasketches_hll_downsample";

    static FunctionPtr create() { return std::make_shared<FunctionDataSketchesHllDownsample>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 0; }

    bool is_variadic() const override { return true; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        // use_default_implementation_for_nulls() is disabled below, so, unlike the usual
        // get_return_type_impl contract, arguments here keep their original nullability and the
        // wrapping is not automatic: replicate it by hand from every argument, as FE does for a
        // PropagateNullable function.
        const bool nullable = std::ranges::any_of(
                arguments, [](const DataTypePtr& type) { return type->is_nullable(); });
        DataTypePtr base = remove_nullable(arguments[0]);
        return nullable ? make_nullable(base) : base;
    }

    // lg_k and tgt_type are validated before the NULL check of the sketch, so the generic
    // nullable-argument shortcut must not return NULL for, e.g., downsample(NULL, 30) first.
    bool use_default_implementation_for_nulls() const override { return false; }

    Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        if (scope == FunctionContext::FunctionStateScope::THREAD_LOCAL) {
            return Status::OK();
        }
        auto state = std::make_shared<DownsampleState>();
        // FE folds the arguments to literals when it can. A constant FE cannot fold, e.g.
        // `10 + crc32('') % 2`, may not be evaluated in open either, then it is read from the
        // first row in execute.
        if (context->is_col_constant(1)) {
            RETURN_IF_ERROR(parse_lg_k(*context->get_constant_col(1)->column_ptr(), state->lg_k));
        }
        if (context->get_num_args() == 2) {
            state->tgt_type = datasketches::HLL_4;
        } else if (context->is_col_constant(2)) {
            RETURN_IF_ERROR(
                    parse_tgt_type(*context->get_constant_col(2)->column_ptr(), state->tgt_type));
        }
        context->set_function_state(scope, state);
        return Status::OK();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const auto* state = reinterpret_cast<const DownsampleState*>(
                context->get_function_state(FunctionContext::FunctionStateScope::FRAGMENT_LOCAL));
        DORIS_CHECK(state != nullptr);
        uint8_t lg_k = 0;
        if (state->lg_k.has_value()) {
            lg_k = *state->lg_k;
        } else {
            RETURN_IF_ERROR(read_lg_k(*block.get_by_position(arguments[1]).column, lg_k));
        }
        datasketches::target_hll_type tgt_type = datasketches::HLL_4;
        if (state->tgt_type.has_value()) {
            tgt_type = *state->tgt_type;
        } else {
            RETURN_IF_ERROR(read_tgt_type(*block.get_by_position(arguments[2]).column, tgt_type));
        }

        const ColumnWithTypeAndName& sketch_arg = block.get_by_position(arguments[0]);
        if (sketch_arg.type->is_nullable() && sketch_arg.get_nullable_column_info().only_null) {
            block.get_by_position(result).column =
                    block.get_by_position(result).type->create_column_const(input_rows_count,
                                                                            Field());
            return Status::OK();
        }
        const auto [sketch_column, sketch_const] = unpack_if_const(sketch_arg.column);
        const NullMap* null_map = nullptr;
        ColumnPtr sketch_values = sketch_column;
        if (const auto* nullable = check_and_get_column<ColumnNullable>(sketch_column.get())) {
            null_map = &nullable->get_null_map_data();
            sketch_values = nullable->get_nested_column_ptr();
        }

        MutableColumnPtr res;
        if (const auto* strings = check_and_get_column<ColumnString>(sketch_values.get())) {
            res = execute_column(*strings, null_map, sketch_const, input_rows_count, lg_k,
                                 tgt_type);
        } else if (const auto* binaries =
                           check_and_get_column<ColumnVarbinary>(sketch_values.get())) {
            res = execute_column(*binaries, null_map, sketch_const, input_rows_count, lg_k,
                                 tgt_type);
        } else {
            return Status::RuntimeError("Illegal column {} of argument of function {}",
                                        sketch_arg.column->get_name(), get_name());
        }

        if (!block.get_by_position(result).type->is_nullable()) {
            block.replace_by_position(result, std::move(res));
            return Status::OK();
        }
        if (sketch_const) {
            // wrap_in_nullable skips constant arguments; a constant sketch here is not NULL, since
            // an all-NULL sketch returned above.
            block.replace_by_position(
                    result, ColumnNullable::create(std::move(res),
                                                   ColumnUInt8::create(input_rows_count, 0)));
            return Status::OK();
        }
        block.replace_by_position(result, wrap_in_nullable(ColumnPtr(std::move(res)), block,
                                                           arguments, input_rows_count));
        return Status::OK();
    }

private:
    // Same bounds as the DataSketches library accepts for a sketch.
    static constexpr int64_t MIN_LG_K = datasketches::hll_constants::MIN_LOG_K;
    static constexpr int64_t MAX_LG_K = datasketches::hll_constants::MAX_LOG_K;

    struct DownsampleState {
        // Empty when the argument is not evaluated in open; it is then read in execute.
        std::optional<uint8_t> lg_k;
        std::optional<datasketches::target_hll_type> tgt_type;
    };

    static Status parse_lg_k(const IColumn& column, std::optional<uint8_t>& lg_k) {
        uint8_t value = 0;
        RETURN_IF_ERROR(read_lg_k(column, value));
        lg_k = value;
        return Status::OK();
    }

    static Status parse_tgt_type(const IColumn& column,
                                 std::optional<datasketches::target_hll_type>& tgt_type) {
        datasketches::target_hll_type value = datasketches::HLL_4;
        RETURN_IF_ERROR(read_tgt_type(column, value));
        tgt_type = value;
        return Status::OK();
    }

    // Reads lg_k from the first row. FE rejects a literal outside the range or NULL, so these
    // errors are only reachable through a constant evaluated by BE.
    static Status read_lg_k(const IColumn& column, uint8_t& lg_k) {
        if (column.is_null_at(0)) {
            return Status::InvalidArgument("{}'s lg_k must be between {} and {}, but was NULL",
                                           name, MIN_LG_K, MAX_LG_K);
        }
        const int64_t value = remove_nullable(column.get_ptr())->get_int(0);
        if (value < MIN_LG_K || value > MAX_LG_K) {
            return Status::InvalidArgument("{}'s lg_k must be between {} and {}, but was {}", name,
                                           MIN_LG_K, MAX_LG_K, value);
        }
        lg_k = static_cast<uint8_t>(value);
        return Status::OK();
    }

    static Status read_tgt_type(const IColumn& column, datasketches::target_hll_type& tgt_type) {
        if (column.is_null_at(0)) {
            return Status::InvalidArgument(
                    "{}'s tgt_type must be one of HLL_4, HLL_6, HLL_8, but was NULL", name);
        }
        const std::string value = remove_nullable(column.get_ptr())->get_data_at(0).to_string();
        const std::string lower = to_lower(value);
        if (lower == "hll_4") {
            tgt_type = datasketches::HLL_4;
        } else if (lower == "hll_6") {
            tgt_type = datasketches::HLL_6;
        } else if (lower == "hll_8") {
            tgt_type = datasketches::HLL_8;
        } else {
            return Status::InvalidArgument(
                    "{}'s tgt_type must be one of HLL_4, HLL_6, HLL_8, but was '{}'", name, value);
        }
        return Status::OK();
    }

    template <typename ColumnType>
    static MutableColumnPtr execute_column(const ColumnType& sketches, const NullMap* null_map,
                                           bool sketch_const, size_t input_rows_count, uint8_t lg_k,
                                           datasketches::target_hll_type tgt_type) {
        auto res = ColumnType::create();
        for (size_t i = 0; i < input_rows_count; ++i) {
            const size_t row = index_check_const(i, sketch_const);
            if (null_map != nullptr && (*null_map)[row]) {
                res->insert_default();
                continue;
            }
            downsample(sketches.get_data_at(row), lg_k, tgt_type, *res);
        }
        return res;
    }

    template <typename ColumnType>
    static void downsample(const StringRef& value, uint8_t lg_k,
                           datasketches::target_hll_type tgt_type, ColumnType& res) {
        const datasketches_hll::Sketch sketch = datasketches_hll::deserialize(value, "downsample");
        if (sketch.get_lg_config_k() <= lg_k && sketch.get_target_type() == tgt_type) {
            res.insert_data(value.data, value.size);
            return;
        }
        try {
            // A union capped at min(lg_k, source lgK) folds the registers of a dense sketch and
            // replays the coupons of a LIST/SET sketch, without ever raising the source lgK.
            datasketches_hll::Union hll_union(std::min(lg_k, sketch.get_lg_config_k()),
                                              datasketches_hll::Alloc());
            hll_union.update(sketch);
            const auto bytes = hll_union.get_result(tgt_type).serialize_compact();
            res.insert_data(reinterpret_cast<const char*>(bytes.data()), bytes.size());
        } catch (const doris::Exception& e) {
            throw Exception(e.code(), "Internal error happened when downsample HLL sketch: {}",
                            e.to_string());
        } catch (const std::exception& e) {
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "Internal error happened when downsample HLL sketch: {}", e.what());
        } catch (...) {
            throw Exception(
                    ErrorCode::INTERNAL_ERROR,
                    "Internal error happened when downsample HLL sketch: unknown exception.");
        }
    }
};

void register_function_datasketches_hll(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionDataSketchesHllDownsample>();
}

} // namespace doris
