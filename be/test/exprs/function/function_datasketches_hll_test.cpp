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

#include <gtest/gtest.h>

#include <algorithm>
#include <cmath>
#include <hll.hpp>
#include <optional>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_varbinary.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_varbinary.h"
#include "exprs/datasketches_hll_util.h"
#include "exprs/function/function.h"
#include "exprs/function/simple_function_factory.h"
#include "exprs/function_context.h"
#include "testutil/function_utils.h"

namespace doris {

class FunctionDataSketchesHllDownsampleTest : public testing::Test {
protected:
    static std::string sketch_bytes(int lg_k, datasketches::target_hll_type type, uint64_t n,
                                    bool compact = true, bool full = false) {
        datasketches::hll_sketch sketch(lg_k, type, full);
        for (uint64_t i = 0; i < n; ++i) {
            sketch.update(i);
        }
        auto bytes = compact ? sketch.serialize_compact() : sketch.serialize_updatable();
        return {reinterpret_cast<const char*>(bytes.data()), bytes.size()};
    }

    static ColumnPtr integer(int value, size_t rows = 1) {
        auto column = ColumnInt32::create();
        column->insert_value(value);
        return ColumnConst::create(std::move(column), rows);
    }

    static ColumnPtr text(const std::string& value, size_t rows = 1) {
        auto column = ColumnString::create();
        column->insert_data(value.data(), value.size());
        return ColumnConst::create(std::move(column), rows);
    }

    static Status execute(Block& block, bool constants_in_open = true) {
        const auto args = block.get_columns_with_type_and_name();
        DataTypes types;
        ColumnNumbers indexes;
        std::vector<std::shared_ptr<ColumnPtrWrapper>> constants;
        bool nullable = false;
        for (size_t i = 0; i < args.size(); ++i) {
            types.push_back(args[i].type);
            indexes.push_back(i);
            nullable |= args[i].type->is_nullable();
            constants.push_back(constants_in_open && is_column_const(*args[i].column)
                                        ? std::make_shared<ColumnPtrWrapper>(args[i].column)
                                        : nullptr);
        }
        auto result_type = nullable ? make_nullable(remove_nullable(types[0])) : types[0];
        auto function = SimpleFunctionFactory::instance().get_function(
                "datasketches_hll_downsample", args, result_type);
        EXPECT_NE(function, nullptr);
        FunctionUtils utils(result_type, types, false);
        auto* context = utils.get_fn_ctx();
        context->set_constant_cols(constants);
        RETURN_IF_ERROR(
                function->open(context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL));
        RETURN_IF_ERROR(function->open(context, FunctionContext::FunctionStateScope::THREAD_LOCAL));
        auto rows = block.rows();
        block.insert({nullptr, result_type, "result"});
        auto status = function->execute(context, block, indexes, args.size(), rows);
        EXPECT_TRUE(
                function->close(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());
        EXPECT_TRUE(
                function->close(context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
        return status;
    }

    static Block input(const std::vector<std::optional<std::string>>& values, int lg_k,
                       const std::optional<std::string>& type = std::nullopt,
                       PrimitiveType primitive = TYPE_STRING, bool constant = false,
                       bool nullable = false) {
        DataTypePtr data_type = primitive == TYPE_VARBINARY
                                        ? DataTypePtr(std::make_shared<DataTypeVarbinary>())
                                        : std::make_shared<DataTypeString>(-1, primitive);
        auto column = data_type->create_column();
        auto null_map = ColumnUInt8::create();
        for (const auto& value : values) {
            if (value.has_value()) {
                column->insert_data(value->data(), value->size());
            } else {
                column->insert_default();
            }
            null_map->insert_value(!value.has_value());
        }
        ColumnPtr data = std::move(column);
        if (nullable) {
            data = ColumnNullable::create(data, std::move(null_map));
            data_type = make_nullable(data_type);
        }
        if (constant) {
            data = ColumnConst::create(data, values.size());
        }
        Block block;
        block.insert({data, data_type, "sk"});
        block.insert({integer(lg_k, values.size()), std::make_shared<DataTypeInt32>(), "lg_k"});
        if (type.has_value()) {
            block.insert({text(*type, values.size()), std::make_shared<DataTypeString>(), "type"});
        }
        return block;
    }

    static std::string result(const Block& block, size_t row = 0) {
        return block.get_by_position(block.columns() - 1).column->get_data_at(row).to_string();
    }

    static void verify_conversion(const std::string& src, int src_k,
                                  datasketches::target_hll_type src_type, uint64_t n, int target,
                                  datasketches::target_hll_type target_type,
                                  const std::string& name) {
        SCOPED_TRACE(testing::Message()
                     << src_k << '/' << target << '/' << n << '/' << src_type << '/' << name);
        Block block = input({src}, target, name);
        ASSERT_TRUE(execute(block).ok());
        auto out = result(block);
        auto sketch = datasketches::hll_sketch::deserialize(out.data(), out.size());
        EXPECT_EQ(sketch.get_lg_config_k(), std::min(src_k, target));
        EXPECT_EQ(sketch.get_target_type(), target_type);
        EXPECT_EQ(sketch.is_empty(), n == 0);
        EXPECT_EQ(out == src, src_k <= target && src_type == target_type);
        auto again = input({out}, target, name);
        ASSERT_TRUE(execute(again).ok());
        EXPECT_EQ(result(again), out);
    }

    static void verify_nullable(PrimitiveType type, bool constant, const std::string& src) {
        auto block = input({src}, 10, "HLL_4", type, constant, true);
        ASSERT_TRUE(execute(block).ok());
        auto output = result(block);
        auto mixed = input({std::nullopt, src, std::nullopt, src}, 10, "HLL_4", type, false, true);
        ASSERT_TRUE(execute(mixed).ok());
        const auto& col = mixed.get_by_position(3).column;
        EXPECT_TRUE(col->is_null_at(0));
        EXPECT_TRUE(col->is_null_at(2));
        EXPECT_EQ(result(mixed, 1), output);
        EXPECT_EQ(result(mixed, 3), output);
        auto all_null = input({std::nullopt}, 10, "HLL_4", type, constant, true);
        ASSERT_TRUE(execute(all_null).ok());
        EXPECT_TRUE(all_null.get_by_position(3).column->is_null_at(0));
    }
};

TEST_F(FunctionDataSketchesHllDownsampleTest, ModesTypesPrecisionAndIdempotence) {
    const std::vector<datasketches::target_hll_type> types = {
            datasketches::HLL_4, datasketches::HLL_6, datasketches::HLL_8};
    const std::vector<std::string> names = {"HLL_4", "hll_6", "HlL_8"};
    for (auto src_type : types) {
        for (int src_k : {4, 7, 10, 15, 21}) {
            for (uint64_t n : {0, 5, 20, 4000}) {
                for (bool compact : {false, true}) {
                    auto src = sketch_bytes(src_k, src_type, n, compact);
                    for (int target : {4, 10, 15, 21}) {
                        for (size_t t = 0; t < types.size(); ++t) {
                            verify_conversion(src, src_k, src_type, n, target, types[t], names[t]);
                        }
                    }
                }
            }
        }
    }
}

TEST_F(FunctionDataSketchesHllDownsampleTest, DenseHighPrecisionAndDefaultType) {
    for (const auto& pair : {std::pair {21, 15}, std::pair {15, 10}}) {
        auto src = sketch_bytes(pair.first, datasketches::HLL_8, 100000, true, true);
        auto block = input({src}, pair.second);
        ASSERT_TRUE(execute(block).ok());
        auto out = result(block);
        auto low = datasketches::hll_sketch::deserialize(out.data(), out.size());
        EXPECT_EQ(low.get_lg_config_k(), pair.second);
        EXPECT_EQ(low.get_target_type(), datasketches::HLL_4);
        EXPECT_LT(out.size(), src.size() / 10);
        auto direct_bytes = sketch_bytes(pair.second, datasketches::HLL_4, 100000);
        auto direct =
                datasketches::hll_sketch::deserialize(direct_bytes.data(), direct_bytes.size());
        EXPECT_NEAR(low.get_composite_estimate(), direct.get_composite_estimate(), 1e-8);
        auto empty = input({sketch_bytes(21, datasketches::HLL_8, 0, true, true)}, pair.second);
        ASSERT_TRUE(execute(empty).ok());
        EXPECT_EQ(result(empty).size(), 8);
    }
}

TEST_F(FunctionDataSketchesHllDownsampleTest, NullableConstAndBinaryColumns) {
    for (auto type : {TYPE_STRING, TYPE_VARCHAR, TYPE_VARBINARY}) {
        auto src = sketch_bytes(12, datasketches::HLL_8, 1000);
        for (bool constant : {false, true}) {
            verify_nullable(type, constant, src);
        }
    }
}

TEST_F(FunctionDataSketchesHllDownsampleTest, BeEvaluatedConstantsAndInvalidParameters) {
    auto src = sketch_bytes(15, datasketches::HLL_8, 4000);
    auto block = input({src, src}, 10, "HLL_6");
    // Constant expressions can arrive as full columns and may be unavailable in open().
    block.get_by_position(1).column = integer(10, 2)->convert_to_full_column_if_const();
    block.get_by_position(2).column = text("HLL_6", 2)->convert_to_full_column_if_const();
    ASSERT_TRUE(execute(block, false).ok());
    auto out = result(block);
    EXPECT_EQ(result(block, 1), out);
    EXPECT_EQ(datasketches::hll_sketch::deserialize(out.data(), out.size()).get_lg_config_k(), 10);
    for (bool in_open : {false, true}) {
        for (int k : {3, 22, 268}) {
            auto invalid = input({std::nullopt}, k, "HLL_4", TYPE_STRING, true, true);
            EXPECT_EQ(execute(invalid, in_open).code(), ErrorCode::INVALID_ARGUMENT);
        }
        auto invalid = input({src}, 10, "bad");
        EXPECT_EQ(execute(invalid, in_open).code(), ErrorCode::INVALID_ARGUMENT);
        for (size_t arg : {1, 2}) {
            auto null_arg = input({src}, 10, "HLL_4");
            auto& c = null_arg.get_by_position(arg);
            auto data = remove_nullable(c.type)->create_column();
            data->insert_default();
            c.column = ColumnConst::create(
                    ColumnNullable::create(std::move(data), ColumnUInt8::create(1, 1)), 1);
            c.type = make_nullable(c.type);
            EXPECT_EQ(execute(null_arg, in_open).code(), ErrorCode::INVALID_ARGUMENT);
        }
    }
}

TEST_F(FunctionDataSketchesHllDownsampleTest, InvalidSketchIncludingFastPath) {
    auto dense = sketch_bytes(12, datasketches::HLL_4, 1000);
    std::vector<std::string> invalid {"", "x", dense.substr(0, dense.size() / 2)};
    for (size_t pos : {1, 2, 3}) {
        auto bad = dense;
        bad[pos] = static_cast<char>(255);
        invalid.push_back(bad);
    }
    // Missing auxiliary map with an AUX_TOKEN is accepted by upstream deserialize, but would
    // dereference a null map in union/conversion. Reject it even on the unchanged-byte path.
    auto missing_aux = dense;
    std::fill_n(missing_aux.begin() + datasketches::hll_constants::AUX_COUNT_INT, 4, '\0');
    missing_aux[datasketches::hll_constants::HLL_BYTE_ARR_START] |= 0xf;
    invalid.push_back(missing_aux);
    for (const auto& bytes : invalid) {
        auto block = input({bytes}, 21, "HLL_4");
        EXPECT_EQ(execute(block).code(), ErrorCode::CORRUPTION);
    }
}

TEST_F(FunctionDataSketchesHllDownsampleTest, AllocationFailureKeepsErrorCode) {
    auto bytes = sketch_bytes(12, datasketches::HLL_8, 1000);
    const auto old = config::mem_alloc_fault_probability;
    config::mem_alloc_fault_probability = 1;
    ++enable_thread_catch_bad_alloc;
    try {
        static_cast<void>(datasketches_hll::deserialize(StringRef(bytes), "downsample"));
        ADD_FAILURE() << "Expected allocation failure";
    } catch (const Exception& e) {
        EXPECT_EQ(e.code(), ErrorCode::MEM_ALLOC_FAILED);
    }
    --enable_thread_catch_bad_alloc;
    config::mem_alloc_fault_probability = old;
}

} // namespace doris
