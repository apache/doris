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

#include <bthread/bthread.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <cstring>
#include <functional>
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
#include "core/string_buffer.hpp"
#include "exprs/aggregate/aggregate_function_datasketches_hll_union_agg.h"
#include "exprs/datasketches_hll_util.h"
#include "exprs/function/function.h"
#include "exprs/function/simple_function_factory.h"
#include "exprs/function_context.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"
#include "testutil/function_utils.h"
#include "util/debug_points.h"

namespace doris {

namespace {

class ScopedSketchAllocationFailure {
public:
    explicit ScopedSketchAllocationFailure(size_t fail_at)
            : _old_enabled(config::enable_debug_points) {
        config::enable_debug_points = true;
        DebugPoints::instance()->add_with_callback(
                "datasketches_hll.before_allocate", std::function<void()>([this, fail_at] {
                    ++calls;
                    if (calls == fail_at) {
                        throw Exception(ErrorCode::MEM_ALLOC_FAILED,
                                        "Injected HLL allocation failure");
                    }
                }));
    }

    ~ScopedSketchAllocationFailure() {
        DebugPoints::instance()->remove("datasketches_hll.before_allocate");
        config::enable_debug_points = _old_enabled;
    }

    size_t calls = 0;

private:
    bool _old_enabled;
};

} // namespace

class FunctionDataSketchesHllDownsampleTest : public testing::Test {
protected:
    static void put_uint32(std::string& bytes, size_t offset, uint32_t value) {
        std::memcpy(bytes.data() + offset, &value, sizeof(value));
    }

    static std::string aux_sketch_bytes(int lg_k, bool compact) {
        // Force genuine HLL_4 exceptions without searching for extremely rare hash values.
        datasketches::Hll4Array<std::allocator<uint8_t>> sketch(lg_k, true, {});
        sketch.couponUpdate(datasketches::HllUtil<>::pair(1, 20));
        sketch.couponUpdate(datasketches::HllUtil<>::pair(3, 21));
        auto bytes = sketch.serialize(compact, 0);
        return {reinterpret_cast<const char*>(bytes.data()), bytes.size()};
    }

    static std::string missing_aux_slot(bool compact) {
        using namespace datasketches::hll_constants;
        auto bytes = aux_sketch_bytes(8, compact);
        const size_t offset = HLL_BYTE_ARR_START + (1 << 7);
        if (compact) {
            bytes.resize(offset + sizeof(uint32_t));
        }
        std::fill(bytes.begin() + offset, bytes.end(), '\0');
        put_uint32(bytes, AUX_COUNT_INT, 1);
        // Registers 1 and 3 are AUX_TOKEN, but only register 3 has an auxiliary record.
        put_uint32(bytes, offset, datasketches::HllUtil<>::pair(3, 21));
        return bytes;
    }

    static std::string empty_compact_aux_entry() {
        using namespace datasketches::hll_constants;
        datasketches::Hll4Array<std::allocator<uint8_t>> sketch(8, true, {});
        sketch.couponUpdate(datasketches::HllUtil<>::pair(0, 20));
        auto serialized = sketch.serialize(true, 0);
        std::string bytes(reinterpret_cast<const char*>(serialized.data()), serialized.size());
        put_uint32(bytes, HLL_BYTE_ARR_START + (1 << 7), EMPTY);
        return bytes;
    }

    static void verify_unaligned_conversion(const std::string& prefix, const std::string& bytes,
                                            PrimitiveType primitive, int target,
                                            datasketches::target_hll_type type,
                                            const std::string& name) {
        auto block = input({prefix, bytes}, target, name, primitive);
        const auto raw = block.get_by_position(0).column->get_data_at(1);
        ASSERT_NE(reinterpret_cast<uintptr_t>(raw.data) % alignof(uint32_t), 0);
        ASSERT_TRUE(execute(block).ok());
        if (target >= 8 && type == datasketches::HLL_4) {
            EXPECT_EQ(result(block, 1), bytes);
            return;
        }
        const auto reference = datasketches::hll_sketch::deserialize(bytes.data(), bytes.size());
        datasketches::hll_union expected_union(std::min(target, 8));
        expected_union.update(reference);
        const auto expected = expected_union.get_result(type).serialize_compact();
        EXPECT_EQ(result(block, 1),
                  std::string(reinterpret_cast<const char*>(expected.data()), expected.size()));
    }

    static void verify_set_count_corruption(const std::string& bytes) {
        for (auto primitive : {TYPE_STRING, TYPE_VARCHAR, TYPE_VARBINARY}) {
            for (int target : {4, 8, 21}) {
                for (const auto& type : {"HLL_4", "HLL_6", "HLL_8"}) {
                    auto block = input({bytes}, target, type, primitive);
                    const auto status = execute(block);
                    EXPECT_EQ(status.code(), ErrorCode::CORRUPTION);
                    EXPECT_NE(status.to_string().find("coupon count does not match occupied slots"),
                              std::string::npos);
                }
            }
        }
    }

    static void verify_conversion_allocation_failure(const std::string& bytes,
                                                     const std::string& type, size_t fail_at) {
        SCOPED_TRACE(testing::Message() << type << '/' << fail_at);
        ScopedSketchAllocationFailure failure(fail_at);
        auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER,
                                                        "HllConversionAllocationFailure");
        auto switch_tracker = SwitchThreadMemTrackerLimiter(tracker);
        thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        const auto baseline = tracker->consumption();
        {
            auto block = input({bytes}, 8, type);
            EXPECT_EQ(execute(block).code(), ErrorCode::MEM_ALLOC_FAILED);
        }
        EXPECT_EQ(failure.calls, fail_at);
        thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        EXPECT_EQ(tracker->consumption(), baseline);
    }

    static void expect_corruption(const std::string& bytes, const std::string& message,
                                  const std::string& type = "HLL_4") {
        // The matching type at lgK=21 also covers the unchanged-byte path.
        for (int target : {4, 21}) {
            for (auto primitive : {TYPE_STRING, TYPE_VARBINARY}) {
                auto block = input({bytes}, target, type, primitive);
                auto status = execute(block);
                EXPECT_EQ(status.code(), ErrorCode::CORRUPTION);
                EXPECT_NE(status.to_string().find(message), std::string::npos);
            }
        }
    }

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

TEST_F(FunctionDataSketchesHllDownsampleTest, RejectAuxMapsOnHll6AndHll8) {
    using namespace datasketches::hll_constants;
    for (auto type : {datasketches::HLL_6, datasketches::HLL_8}) {
        for (bool compact : {false, true}) {
            auto bytes = sketch_bytes(8, type, 1, compact, true);
            const auto offset = bytes.size();
            // Supply the full auxiliary payload: a short image would throw before the unsafe
            // Hll4Array cast, hiding the regression this test is meant to exercise.
            bytes.resize(offset + sizeof(uint32_t) * (compact ? 1 : 4), '\0');
            put_uint32(bytes, AUX_COUNT_INT, 1);
            bytes[LG_ARR_BYTE] = 2;
            put_uint32(bytes, offset, datasketches::HllUtil<>::pair(3, 20));
            expect_corruption(bytes, "cannot contain an auxiliary map",
                              type == datasketches::HLL_6 ? "HLL_6" : "HLL_8");
        }
    }
}

TEST_F(FunctionDataSketchesHllDownsampleTest, ArrayExponentsAndTruncatedPayloads) {
    using namespace datasketches::hll_constants;
    const auto set = sketch_bytes(8, datasketches::HLL_4, 10, false);
    ASSERT_EQ(static_cast<uint8_t>(set[PREAMBLE_INTS_BYTE]), HASH_SET_PREINTS);
    const auto aux = aux_sketch_bytes(8, false);
    for (uint8_t exponent : {31, 32, 63, 64, 255}) {
        SCOPED_TRACE(static_cast<int>(exponent));
        auto bad_set = set;
        bad_set[LG_ARR_BYTE] = static_cast<char>(exponent);
        bad_set.resize(HASH_SET_INT_ARR_START);
        expect_corruption(bad_set, "SET array exponent");
        auto bad_aux = aux;
        bad_aux[LG_ARR_BYTE] = static_cast<char>(exponent);
        expect_corruption(bad_aux, "auxiliary array exponent");
    }
    auto small_aux = aux;
    small_aux[LG_ARR_BYTE] = 0;
    expect_corruption(small_aux, "auxiliary array exponent");
    // Valid exponents still require a complete array before the library allocates anything.
    expect_corruption(set.substr(0, set.size() - 1), "corrupted");
    expect_corruption(aux.substr(0, aux.size() - 1), "corrupted");
    auto compact_aux = aux_sketch_bytes(8, true);
    expect_corruption(compact_aux.substr(0, compact_aux.size() - 1), "corrupted");
    auto huge_count = set;
    huge_count[LG_ARR_BYTE] = 0;
    put_uint32(huge_count, HASH_SET_COUNT_INT, UINT32_MAX);
    expect_corruption(huge_count, "SET coupon count");
    put_uint32(compact_aux, AUX_COUNT_INT, UINT32_MAX);
    expect_corruption(compact_aux, "auxiliary count exceeds register count");
}

TEST_F(FunctionDataSketchesHllDownsampleTest, AuxiliarySlotsAreUniqueAndDoNotLeak) {
    using namespace datasketches::hll_constants;
    for (bool compact : {false, true}) {
        auto bytes = aux_sketch_bytes(8, compact);
        const size_t offset = HLL_BYTE_ARR_START + (1 << 7);
        std::fill(bytes.begin() + offset, bytes.end(), '\0');
        put_uint32(bytes, offset, datasketches::HllUtil<>::pair(3, 20));
        // Raw keys differ but deserialize masks both down to register 3.
        put_uint32(bytes, offset + 4, datasketches::HllUtil<>::pair(3 + (1 << 8), 21));
        expect_corruption(bytes, "duplicate slots");

        auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER,
                                                        "HllAuxiliaryMapError");
        auto switch_tracker = SwitchThreadMemTrackerLimiter(tracker);
        thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        const auto baseline = tracker->consumption();
        for (int i = 0; i < 32; ++i) {
            try {
                static_cast<void>(datasketches_hll::deserialize(StringRef(bytes), "test"));
                ADD_FAILURE() << "Expected duplicate auxiliary slots to fail";
            } catch (const Exception& e) {
                EXPECT_EQ(e.code(), ErrorCode::CORRUPTION);
            }
        }
        thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        EXPECT_EQ(tracker->consumption(), baseline);
    }
}

TEST_F(FunctionDataSketchesHllDownsampleTest, ValidAuxiliaryMapsAndLegacyArrayExponents) {
    using namespace datasketches::hll_constants;
    for (int lg_k : {4, 8, 12}) {
        for (bool compact : {false, true}) {
            auto bytes = aux_sketch_bytes(lg_k, compact);
            if (compact) {
                // Compact auxiliary images historically omit lgArr; the library ignores it.
                bytes[LG_ARR_BYTE] = static_cast<char>(255);
            }
            for (const auto& [type, name] : {std::pair {datasketches::HLL_4, "HLL_4"},
                                             std::pair {datasketches::HLL_6, "HLL_6"},
                                             std::pair {datasketches::HLL_8, "HLL_8"}}) {
                verify_conversion(bytes, lg_k, datasketches::HLL_4, 2, 4, type, name);
                verify_conversion(bytes, lg_k, datasketches::HLL_4, 2, 21, type, name);
            }
        }
    }
    for (bool compact : {false, true}) {
        auto bytes = sketch_bytes(8, datasketches::HLL_4, 10, compact);
        bytes[LG_ARR_BYTE] = compact ? static_cast<char>(255) : 0;
        verify_conversion(bytes, 8, datasketches::HLL_4, 10, 4, datasketches::HLL_4, "HLL_4");
        verify_conversion(bytes, 8, datasketches::HLL_4, 10, 21, datasketches::HLL_4, "HLL_4");
    }
}

TEST_F(FunctionDataSketchesHllDownsampleTest, RejectMissingAuxiliarySlotsWithoutLeaks) {
    for (bool compact : {false, true}) {
        const auto bytes = missing_aux_slot(compact);
        expect_corruption(bytes, "no matching auxiliary slot");
        for (const auto& type : {"HLL_6", "HLL_8"}) {
            expect_corruption(bytes, "no matching auxiliary slot", type);
        }
        auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER,
                                                        "HllMissingAuxiliarySlot");
        auto switch_tracker = SwitchThreadMemTrackerLimiter(tracker);
        thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        const auto baseline = tracker->consumption();
        for (int i = 0; i < 32; ++i) {
            try {
                static_cast<void>(datasketches_hll::deserialize(StringRef(bytes), "test"));
                ADD_FAILURE() << "Expected missing auxiliary slot to fail";
            } catch (const Exception& e) {
                EXPECT_EQ(e.code(), ErrorCode::CORRUPTION);
            }
        }
        thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        EXPECT_EQ(tracker->consumption(), baseline);
    }
}

TEST_F(FunctionDataSketchesHllDownsampleTest, UnalignedAuxiliaryMapsKeepSketchContents) {
    for (int lg_k : {4, 8, 12}) {
        for (bool compact : {false, true}) {
            const auto bytes = aux_sketch_bytes(lg_k, compact);
            const auto reference =
                    datasketches::hll_sketch::deserialize(bytes.data(), bytes.size());
            const auto expected = reference.serialize_updatable();
            for (size_t offset = 0; offset < alignof(uint32_t); ++offset) {
                SCOPED_TRACE(testing::Message() << lg_k << '/' << compact << '/' << offset);
                DorisVector<uint32_t> storage((bytes.size() + offset + 3) / sizeof(uint32_t));
                auto* start = reinterpret_cast<char*>(storage.data()) + offset;
                std::memcpy(start, bytes.data(), bytes.size());
                const auto sketch =
                        datasketches_hll::deserialize(StringRef(start, bytes.size()), "test");
                const auto actual = sketch.serialize_updatable();
                EXPECT_EQ(std::string(reinterpret_cast<const char*>(actual.data()), actual.size()),
                          std::string(reinterpret_cast<const char*>(expected.data()),
                                      expected.size()));
                EXPECT_DOUBLE_EQ(sketch.get_composite_estimate(),
                                 reference.get_composite_estimate());
            }
        }
    }
}

TEST_F(FunctionDataSketchesHllDownsampleTest, MixedRowsKeepUnalignedConversionResults) {
    const auto prefix = sketch_bytes(8, datasketches::HLL_6, 1000, true, true);
    ASSERT_EQ(prefix.size(), 233);
    for (bool compact : {false, true}) {
        const auto bytes = aux_sketch_bytes(8, compact);
        for (auto primitive : {TYPE_STRING, TYPE_VARCHAR, TYPE_VARBINARY}) {
            for (int target : {4, 8, 21}) {
                for (const auto& [type, name] : {std::pair {datasketches::HLL_4, "HLL_4"},
                                                 std::pair {datasketches::HLL_6, "HLL_6"},
                                                 std::pair {datasketches::HLL_8, "HLL_8"}}) {
                    verify_unaligned_conversion(prefix, bytes, primitive, target, type, name);
                }
            }
        }
    }
}

TEST_F(FunctionDataSketchesHllDownsampleTest, UnalignedAggregateStateKeepsEstimate) {
    using Data = AggregateFunctionHllSketchData<TYPE_STRING>;
    for (bool compact : {false, true}) {
        const auto bytes = aux_sketch_bytes(8, compact);
        const auto source = datasketches_hll::deserialize(StringRef(bytes), "test");
        Data expected;
        expected.merge(source);
        auto buffer = ColumnString::create();
        BufferWritable writer(*buffer);
        writer.write_binary(StringRef(bytes));
        writer.commit();
        BufferReadable inspect(buffer->get_data_at(0));
        StringRef payload;
        inspect.read_binary(payload);
        ASSERT_NE(reinterpret_cast<uintptr_t>(payload.data) % alignof(uint32_t), 0);
        BufferReadable reader(buffer->get_data_at(0));
        Data actual;
        actual.read(reader);
        EXPECT_DOUBLE_EQ(actual.get_result(), expected.get_result());
        const auto actual_bytes = actual.hll_union_data->get_result().serialize_compact();
        const auto expected_bytes = expected.hll_union_data->get_result().serialize_compact();
        EXPECT_EQ(std::string(reinterpret_cast<const char*>(actual_bytes.data()),
                              actual_bytes.size()),
                  std::string(reinterpret_cast<const char*>(expected_bytes.data()),
                              expected_bytes.size()));
    }
}

TEST_F(FunctionDataSketchesHllDownsampleTest, UnalignedDeserializationFailuresAreBalanced) {
    const auto bytes = aux_sketch_bytes(8, true);
    DorisVector<uint32_t> storage((bytes.size() + 4) / sizeof(uint32_t));
    auto* start = reinterpret_cast<char*>(storage.data()) + 1;
    std::memcpy(start, bytes.data(), bytes.size());
    const StringRef input(start, bytes.size());
    size_t allocations = 0;
    {
        ScopedSketchAllocationFailure count_only(0);
        const auto sketch = datasketches_hll::deserialize(input, "test");
        allocations = count_only.calls;
    }
    ASSERT_GT(allocations, 1);
    for (size_t fail_at = 1; fail_at <= allocations; ++fail_at) {
        auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER,
                                                        "HllUnalignedAllocationFailure");
        auto switch_tracker = SwitchThreadMemTrackerLimiter(tracker);
        thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        const auto baseline = tracker->consumption();
        ScopedSketchAllocationFailure failure(fail_at);
        try {
            static_cast<void>(datasketches_hll::deserialize(input, "test"));
            ADD_FAILURE() << "Expected allocation failure";
        } catch (const Exception& e) {
            EXPECT_EQ(e.code(), ErrorCode::MEM_ALLOC_FAILED);
        }
        thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        EXPECT_EQ(tracker->consumption(), baseline);
    }
}

TEST_F(FunctionDataSketchesHllDownsampleTest, RejectCompactEmptyAuxiliaryEntry) {
    const auto bytes = empty_compact_aux_entry();
    for (const auto& type : {"HLL_4", "HLL_6", "HLL_8"}) {
        expect_corruption(bytes, "compact auxiliary map contains an EMPTY entry", type);
    }
    using Data = AggregateFunctionHllSketchData<TYPE_STRING>;
    auto buffer = ColumnString::create();
    BufferWritable writer(*buffer);
    writer.write_binary(StringRef(bytes));
    writer.commit();
    BufferReadable reader(buffer->get_data_at(0));
    Data data;
    try {
        data.read(reader);
        ADD_FAILURE() << "Expected compact EMPTY auxiliary entry to fail";
    } catch (const Exception& e) {
        EXPECT_EQ(e.code(), ErrorCode::CORRUPTION);
        EXPECT_NE(e.to_string().find("compact auxiliary map contains an EMPTY entry"),
                  std::string::npos);
    }
    EXPECT_FALSE(data.hll_union_data.has_value());
}

TEST_F(FunctionDataSketchesHllDownsampleTest, RejectUpdatableSetCountMismatch) {
    using namespace datasketches::hll_constants;
    for (auto type : {datasketches::HLL_4, datasketches::HLL_6, datasketches::HLL_8}) {
        const auto valid = sketch_bytes(8, type, 10, false);
        ASSERT_EQ(static_cast<uint8_t>(valid[PREAMBLE_INTS_BYTE]), HASH_SET_PREINTS);
        ASSERT_EQ(valid.size(), HASH_SET_INT_ARR_START + 32 * sizeof(uint32_t));
        for (uint32_t count : {0, 1, 9, 11, 24}) {
            auto bytes = valid;
            put_uint32(bytes, HASH_SET_COUNT_INT, count);
            verify_set_count_corruption(bytes);
        }
        // The legacy unset lgArr is reconstructed by the library from count=1 as 32 slots;
        // the payload is still complete, so the occupied-slot check must reject it too.
        auto legacy = valid;
        legacy[LG_ARR_BYTE] = 0;
        put_uint32(legacy, HASH_SET_COUNT_INT, 1);
        verify_set_count_corruption(legacy);
    }
}

TEST_F(FunctionDataSketchesHllDownsampleTest,
       SetCountMismatchFailsBeforeAllocationAndAggregateRead) {
    using namespace datasketches::hll_constants;
    auto bytes = sketch_bytes(8, datasketches::HLL_4, 10, false);
    put_uint32(bytes, HASH_SET_COUNT_INT, 1);
    auto tracker =
            MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER, "HllSetCountMismatch");
    auto switch_tracker = SwitchThreadMemTrackerLimiter(tracker);
    thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
    const auto baseline = tracker->consumption();
    {
        ScopedSketchAllocationFailure count_only(0);
        for (int i = 0; i < 32; ++i) {
            try {
                static_cast<void>(datasketches_hll::deserialize(StringRef(bytes), "test"));
                ADD_FAILURE() << "Expected SET count mismatch to fail";
            } catch (const Exception& e) {
                EXPECT_EQ(e.code(), ErrorCode::CORRUPTION);
            }
        }
        EXPECT_EQ(count_only.calls, 0);
        auto buffer = ColumnString::create();
        BufferWritable writer(*buffer);
        writer.write_binary(StringRef(bytes));
        writer.commit();
        BufferReadable reader(buffer->get_data_at(0));
        AggregateFunctionHllSketchData<TYPE_STRING> data;
        try {
            data.read(reader);
            ADD_FAILURE() << "Expected aggregate SET count mismatch to fail";
        } catch (const Exception& e) {
            EXPECT_EQ(e.code(), ErrorCode::CORRUPTION);
            EXPECT_NE(e.to_string().find("coupon count does not match occupied slots"),
                      std::string::npos);
        }
        EXPECT_FALSE(data.hll_union_data.has_value());
        EXPECT_EQ(count_only.calls, 0);
    }
    thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
    EXPECT_EQ(tracker->consumption(), baseline);
}

TEST_F(FunctionDataSketchesHllDownsampleTest, ValidSetCountsAndLegacyExponentsKeepResults) {
    using namespace datasketches::hll_constants;
    for (auto type : {datasketches::HLL_4, datasketches::HLL_6, datasketches::HLL_8}) {
        for (uint64_t count : {10, 80, 300}) {
            for (bool compact : {false, true}) {
                auto bytes = sketch_bytes(12, type, count, compact);
                ASSERT_EQ(static_cast<uint8_t>(bytes[PREAMBLE_INTS_BYTE]), HASH_SET_PREINTS);
                const auto reference =
                        datasketches::hll_sketch::deserialize(bytes.data(), bytes.size());
                // Preserve the library's reconstruction of an unset serialized array exponent.
                bytes[LG_ARR_BYTE] = 0;
                const auto parsed = datasketches_hll::deserialize(StringRef(bytes), "test");
                const auto expected = reference.serialize_compact();
                const auto actual = parsed.serialize_compact();
                EXPECT_EQ(std::string(reinterpret_cast<const char*>(actual.data()), actual.size()),
                          std::string(reinterpret_cast<const char*>(expected.data()),
                                      expected.size()));
                EXPECT_DOUBLE_EQ(parsed.get_estimate(), reference.get_estimate());
            }
        }
    }
}

TEST_F(FunctionDataSketchesHllDownsampleTest, EveryDeserializationAllocationFailureIsBalanced) {
    for (const auto& bytes : {sketch_bytes(8, datasketches::HLL_8, 1, true, true),
                              sketch_bytes(8, datasketches::HLL_4, 10, false),
                              aux_sketch_bytes(8, true), aux_sketch_bytes(8, false)}) {
        size_t allocations = 0;
        {
            ScopedSketchAllocationFailure count_only(0);
            auto sketch = datasketches_hll::deserialize(StringRef(bytes), "test");
            EXPECT_FALSE(sketch.is_empty());
            allocations = count_only.calls;
        }
        ASSERT_GT(allocations, 1);
        for (size_t fail_at = 1; fail_at <= allocations; ++fail_at) {
            SCOPED_TRACE(fail_at);
            ScopedSketchAllocationFailure failure(fail_at);
            auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER,
                                                            "HllDeserializeAllocationFailure");
            auto switch_tracker = SwitchThreadMemTrackerLimiter(tracker);
            thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
            const auto baseline = tracker->consumption();
            try {
                static_cast<void>(datasketches_hll::deserialize(StringRef(bytes), "test"));
                ADD_FAILURE() << "Expected allocation failure";
            } catch (const Exception& e) {
                EXPECT_EQ(e.code(), ErrorCode::MEM_ALLOC_FAILED);
            }
            EXPECT_EQ(failure.calls, fail_at);
            thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
            EXPECT_EQ(tracker->consumption(), baseline);
        }
    }
}

TEST_F(FunctionDataSketchesHllDownsampleTest, EveryConversionAllocationFailureIsBalanced) {
    for (const auto& bytes :
         {aux_sketch_bytes(12, true), sketch_bytes(12, datasketches::HLL_4, 200, true)}) {
        for (const auto& type : {"HLL_4", "HLL_6", "HLL_8"}) {
            size_t allocations = 0;
            {
                auto block = input({bytes}, 8, type);
                ScopedSketchAllocationFailure count_only(0);
                ASSERT_TRUE(execute(block).ok());
                allocations = count_only.calls;
            }
            ASSERT_GT(allocations, 1);
            for (size_t fail_at = 1; fail_at <= allocations; ++fail_at) {
                verify_conversion_allocation_failure(bytes, type, fail_at);
            }
        }
    }
}

TEST_F(FunctionDataSketchesHllDownsampleTest, AggregateUpdateFailureResetsStateAndMemory) {
    using Data = AggregateFunctionHllSketchData<TYPE_STRING>;
    const auto large_bytes = sketch_bytes(12, datasketches::HLL_8, 1000, true, true);
    const auto small_bytes = sketch_bytes(8, datasketches::HLL_8, 1000, true, true);
    auto large = datasketches_hll::deserialize(StringRef(large_bytes), "test");
    auto small = datasketches_hll::deserialize(StringRef(small_bytes), "test");
    size_t allocations = 0;
    {
        Data data;
        data.merge(large);
        ScopedSketchAllocationFailure count_only(0);
        data.merge(small);
        allocations = count_only.calls;
    }
    ASSERT_GT(allocations, 1);
    for (size_t fail_at = 1; fail_at <= allocations; ++fail_at) {
        SCOPED_TRACE(fail_at);
        auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER,
                                                        "HllAggregateAllocationFailure");
        auto switch_tracker = SwitchThreadMemTrackerLimiter(tracker);
        thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        const auto baseline = tracker->consumption();
        {
            Data data;
            data.merge(large);
            ScopedSketchAllocationFailure failure(fail_at);
            try {
                data.merge(small);
                ADD_FAILURE() << "Expected aggregate allocation failure";
            } catch (const Exception& e) {
                EXPECT_EQ(e.code(), ErrorCode::MEM_ALLOC_FAILED);
            }
            EXPECT_FALSE(data.hll_union_data.has_value());
        }
        thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        EXPECT_EQ(tracker->consumption(), baseline);
    }
}

TEST_F(FunctionDataSketchesHllDownsampleTest, SuccessfulAllocationScopeTransfersOwnership) {
    const auto bytes = aux_sketch_bytes(8, true);
    auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER,
                                                    "HllAllocationScopeOwnership");
    auto switch_tracker = SwitchThreadMemTrackerLimiter(tracker);
    thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
    const auto baseline = tracker->consumption();
    {
        // A successful deserialize returns a sketch whose allocations outlive its scope.
        auto sketch = datasketches_hll::deserialize(StringRef(bytes), "test");
        thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        EXPECT_GT(tracker->consumption(), baseline);
        EXPECT_EQ(sketch.get_target_type(), datasketches::HLL_4);
    }
    thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
    EXPECT_EQ(tracker->consumption(), baseline);
}

TEST_F(FunctionDataSketchesHllDownsampleTest, UnionCopyAndMoveKeepIndependentOwnership) {
    const auto bytes = aux_sketch_bytes(8, true);
    auto sketch = datasketches_hll::deserialize(StringRef(bytes), "test");
    auto tracker =
            MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER, "HllUnionCopyMove");
    auto switch_tracker = SwitchThreadMemTrackerLimiter(tracker);
    thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
    const auto baseline = tracker->consumption();
    {
        datasketches_hll::Union source(8);
        source.update(sketch);
        const auto estimate = source.get_estimate();
        datasketches_hll::Union copy(source);
        datasketches_hll::Union moved(std::move(copy));
        EXPECT_DOUBLE_EQ(moved.get_estimate(), estimate);
        moved = source;
        EXPECT_DOUBLE_EQ(moved.get_estimate(), estimate);
    }
    thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
    EXPECT_EQ(tracker->consumption(), baseline);
}

TEST_F(FunctionDataSketchesHllDownsampleTest, NestedAllocationScopesReleaseOnlyTheirOwnMemory) {
    auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER,
                                                    "HllNestedAllocationScope");
    auto switch_tracker = SwitchThreadMemTrackerLimiter(tracker);
    thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
    const auto baseline = tracker->consumption();
    {
        datasketches_hll::AllocationScope outer;
        auto* parent = datasketches_hll::Alloc().allocate(32);
        try {
            datasketches_hll::AllocationScope inner;
            static_cast<void>(datasketches_hll::Alloc().allocate(64));
            throw Exception(ErrorCode::MEM_ALLOC_FAILED, "Simulated orphan in nested call");
        } catch (const Exception&) {
        }
        parent[0] = 42;
        EXPECT_EQ(parent[0], 42);
        datasketches_hll::Alloc().deallocate(parent, 32);
    }
    thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
    EXPECT_EQ(tracker->consumption(), baseline);
}

TEST_F(FunctionDataSketchesHllDownsampleTest, AllocationScopesAreIsolatedAcrossBthreads) {
    const auto work = +[](void*) -> void* {
        ScopedInitThreadContext thread;
        auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER,
                                                        "HllBthreadAllocationScope");
        auto switch_tracker = SwitchThreadMemTrackerLimiter(tracker);
        thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        const auto baseline = tracker->consumption();
        for (int i = 0; i < 8; ++i) {
            try {
                datasketches_hll::AllocationScope scope;
                static_cast<void>(datasketches_hll::Alloc().allocate(64));
                // Yield while the call owns allocations, so another bthread can enter its
                // own scope on the same worker. A pthread-local scope alone is insufficient.
                bthread_usleep(1000);
                throw Exception(ErrorCode::MEM_ALLOC_FAILED, "Simulated bthread orphan");
            } catch (const Exception&) {
            }
        }
        thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        EXPECT_EQ(tracker->consumption(), baseline);
        return nullptr;
    };
    std::array<bthread_t, 2> threads;
    for (auto& thread : threads) {
        ASSERT_EQ(bthread_start_background(&thread, nullptr, work, nullptr), 0);
    }
    for (auto thread : threads) {
        EXPECT_EQ(bthread_join(thread, nullptr), 0);
    }
}

} // namespace doris
