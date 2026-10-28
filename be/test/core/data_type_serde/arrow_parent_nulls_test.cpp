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

#include "core/data_type_serde/arrow_parent_nulls.h"

#include <arrow/api.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "runtime/exec_env.h"
#include "util/defer_op.h"

namespace doris {
namespace {

std::shared_ptr<arrow::Array> int32_array(const std::vector<std::optional<int32_t>>& values) {
    arrow::Int32Builder builder;
    for (const auto& value : values) {
        EXPECT_TRUE((value.has_value() ? builder.Append(*value) : builder.AppendNull()).ok());
    }
    std::shared_ptr<arrow::Array> array;
    EXPECT_TRUE(builder.Finish(&array).ok());
    return array;
}

std::vector<bool> nulls_of(const arrow::Array& array) {
    std::vector<bool> nulls;
    for (int64_t row = 0; row < array.length(); ++row) {
        nulls.push_back(array.IsNull(row));
    }
    return nulls;
}

std::pair<int64_t, int64_t> pairs_of_two(int64_t row) {
    return {row * 2, row * 2 + 2};
}

std::shared_ptr<arrow::Buffer> bitmap_of(uint8_t bits) {
    return arrow::Buffer::FromVector(std::vector<uint8_t> {bits});
}

std::shared_ptr<arrow::Array> string_array(const std::vector<std::string>& values) {
    arrow::StringBuilder builder;
    EXPECT_TRUE(builder.AppendValues(values).ok());
    std::shared_ptr<arrow::Array> array;
    EXPECT_TRUE(builder.Finish(&array).ok());
    return array;
}

// The entries of MAP<STRING, INT32> with the keys "a", "b", "c" and the given items.
std::shared_ptr<arrow::Array> map_entries(const std::shared_ptr<arrow::Array>& items) {
    auto entries = arrow::StructArray::Make(
            {string_array({"a", "b", "c"}), items},
            {arrow::field("key", arrow::utf8(), false), arrow::field("value", arrow::int32())});
    EXPECT_TRUE(entries.ok()) << entries.status().ToString();
    return std::move(entries).ValueOrDie();
}

} // namespace

TEST(ArrowParentNullsTest, ReturnsChildUnchangedWithoutNullParents) {
    const auto child = int32_array({1, 2, 3, 4});
    const auto parent = arrow::MakeArray(arrow::ArrayData::Make(
            arrow::fixed_size_list(arrow::int32(), 2), 2, {nullptr}, {child->data()}, 0));
    std::shared_ptr<arrow::Array> masked;
    ASSERT_TRUE(
            mask_arrow_children_hidden_by_null_parents(*parent, 0, 2, child, pairs_of_two, &masked)
                    .ok());
    EXPECT_EQ(child.get(), masked.get());
}

// Rows [1, 2] and [3, 4] of FIXED_SIZE_LIST<INT32, 2>; the NULL row 1 owns child slots 2 and 3.
TEST(ArrowParentNullsTest, ClearsOnlyTheSlotsOfNullParents) {
    const auto child = int32_array({1, 2, 3, 4, std::nullopt, 6});
    const auto parent = arrow::MakeArray(arrow::ArrayData::Make(
            arrow::fixed_size_list(arrow::int32(), 2), 3,
            {arrow::Buffer::FromVector(std::vector<uint8_t> {0x05})}, {child->data()}, 1));
    std::shared_ptr<arrow::Array> masked;
    ASSERT_TRUE(
            mask_arrow_children_hidden_by_null_parents(*parent, 0, 3, child, pairs_of_two, &masked)
                    .ok());
    ASSERT_NE(child.get(), masked.get());
    EXPECT_EQ((std::vector<bool> {false, false, true, true, true, false}), nulls_of(*masked));
    EXPECT_EQ(3, masked->null_count());
    const auto& values = static_cast<const arrow::Int32Array&>(*masked);
    EXPECT_EQ(1, values.Value(0));
    EXPECT_EQ(6, values.Value(5));
    // The source array keeps its own validity.
    EXPECT_EQ((std::vector<bool> {false, false, false, false, true, false}), nulls_of(*child));
}

// A struct sliced at row 1 hands out fields with offset 1: the bits are cleared after it.
TEST(ArrowParentNullsTest, HonorsTheChildArrayOffset) {
    const auto field = int32_array({7, 8, 9});
    auto struct_result =
            arrow::StructArray::Make({field}, {arrow::field("v", arrow::int32())},
                                     arrow::Buffer::FromVector(std::vector<uint8_t> {0x05}), 1);
    ASSERT_TRUE(struct_result.ok()) << struct_result.status().ToString();
    const auto sliced = (*struct_result)->Slice(1, 2);
    const auto& sliced_struct = static_cast<const arrow::StructArray&>(*sliced);
    const auto child = sliced_struct.field(0);
    ASSERT_EQ(1, child->offset());
    std::shared_ptr<arrow::Array> masked;
    ASSERT_TRUE(mask_arrow_children_hidden_by_null_parents(
                        sliced_struct, 0, 2, child,
                        [](int64_t row) { return std::pair<int64_t, int64_t>(row, row + 1); },
                        &masked)
                        .ok());
    EXPECT_EQ((std::vector<bool> {true, false}), nulls_of(*masked));
    EXPECT_EQ(9, static_cast<const arrow::Int32Array&>(*masked).Value(1));
}

// The masked bitmap comes from ExecEnv's Arrow memory pool, the tracked doris::ArrowMemoryPool in
// a BE, and is released with the masked array; a parent without NULL rows allocates nothing.
// Allocating from Arrow's default pool, as before, leaves the proxy pool at zero bytes.
TEST(ArrowParentNullsTest, AllocatesTheMaskFromTheDorisArrowPool) {
    arrow::ProxyMemoryPool pool(arrow::default_memory_pool());
    auto* env = ExecEnv::GetInstance();
    auto* previous_pool = env->_arrow_memory_pool;
    env->_arrow_memory_pool = &pool;
    Defer restore_pool {[&] { env->_arrow_memory_pool = previous_pool; }};
    const auto child = int32_array({1, 2, 3, 4, std::nullopt, 6});
    const auto parent = arrow::MakeArray(arrow::ArrayData::Make(
            arrow::fixed_size_list(arrow::int32(), 2), 3, {bitmap_of(0x05)}, {child->data()}, 1));
    std::shared_ptr<arrow::Array> masked;
    ASSERT_TRUE(
            mask_arrow_children_hidden_by_null_parents(*parent, 0, 3, child, pairs_of_two, &masked)
                    .ok());
    ASSERT_NE(child.get(), masked.get());
    EXPECT_GT(pool.bytes_allocated(), 0);
    masked.reset();
    EXPECT_EQ(0, pool.bytes_allocated());

    const auto all_valid = arrow::MakeArray(arrow::ArrayData::Make(
            arrow::fixed_size_list(arrow::int32(), 2), 3, {nullptr}, {child->data()}, 0));
    const auto allocations = pool.num_allocations();
    ASSERT_TRUE(mask_arrow_children_hidden_by_null_parents(*all_valid, 0, 3, child, pairs_of_two,
                                                           &masked)
                        .ok());
    EXPECT_EQ(child.get(), masked.get());
    EXPECT_EQ(allocations, pool.num_allocations());
}

// A pool that refuses the bitmap fails either mask with a Status. If a helper allocated from
// another pool than the one it is meant to use, its call here would succeed.
TEST(ArrowParentNullsTest, ReportsMaskAllocationFailure) {
    arrow::CappedMemoryPool pool(arrow::default_memory_pool(), 0);
    auto* env = ExecEnv::GetInstance();
    auto* previous_pool = env->_arrow_memory_pool;
    env->_arrow_memory_pool = &pool;
    Defer restore_pool {[&] { env->_arrow_memory_pool = previous_pool; }};
    const auto child = int32_array({1, 2, 3, 4, 5, 6});
    const auto parent = arrow::MakeArray(arrow::ArrayData::Make(
            arrow::fixed_size_list(arrow::int32(), 2), 3, {bitmap_of(0x05)}, {child->data()}, 1));
    std::shared_ptr<arrow::Array> masked;
    auto status =
            mask_arrow_children_hidden_by_null_parents(*parent, 0, 3, child, pairs_of_two, &masked);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(std::string::npos,
              status.to_string().find("allocate Arrow child validity bitmap failed"))
            << status.to_string();
    status = mask_arrow_child_slots_not_visible(*parent, 0, &pool, &masked);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(std::string::npos,
              status.to_string().find("allocate Arrow child validity bitmap failed"))
            << status.to_string();
}

// LIST and LARGE_LIST rows [1, 2], NULL over {3, 4} and [NULL, 6]: the slots of the NULL row become
// NULL, the others keep their own validity, and the bitmap comes from the given pool. Skipping the
// NULL rows leaves 3 and 4 valid.
TEST(ArrowParentNullsTest, MasksTheSlotsOfNullListRows) {
    const auto child = int32_array({1, 2, 3, 4, std::nullopt, 6});
    const std::vector<std::shared_ptr<arrow::Array>> parents {
            arrow::MakeArray(arrow::ArrayData::Make(
                    arrow::list(arrow::int32()), 3,
                    {bitmap_of(0x05), arrow::Buffer::FromVector(std::vector<int32_t> {0, 2, 4, 6})},
                    {child->data()}, 1)),
            arrow::MakeArray(arrow::ArrayData::Make(
                    arrow::large_list(arrow::int32()), 3,
                    {bitmap_of(0x05), arrow::Buffer::FromVector(std::vector<int64_t> {0, 2, 4, 6})},
                    {child->data()}, 1)),
    };
    for (const auto& parent : parents) {
        SCOPED_TRACE(parent->type()->ToString());
        arrow::ProxyMemoryPool pool(arrow::default_memory_pool());
        std::shared_ptr<arrow::Array> masked;
        ASSERT_TRUE(mask_arrow_child_slots_not_visible(*parent, 0, &pool, &masked).ok());
        EXPECT_EQ((std::vector<bool> {false, false, true, true, true, false}), nulls_of(*masked));
        EXPECT_EQ(6, static_cast<const arrow::Int32Array&>(*masked).Value(5));
        EXPECT_GT(pool.bytes_allocated(), 0);
        // The source child keeps its own validity.
        EXPECT_EQ((std::vector<bool> {false, false, false, false, true, false}), nulls_of(*child));
    }
}

// FIXED_SIZE_LIST<INT32, 2> rows [1, 2], NULL, [5, 6] and STRUCT<INT32> rows 7, NULL, 9: the NULL
// row owns child slots 2 and 3 of the list and slot 1 of the struct. Skipping the NULL rows leaves
// those slots valid.
TEST(ArrowParentNullsTest, MasksTheSlotsOfNullFixedSizeListAndStructRows) {
    const auto list = arrow::MakeArray(
            arrow::ArrayData::Make(arrow::fixed_size_list(arrow::int32(), 2), 3, {bitmap_of(0x05)},
                                   {int32_array({1, 2, 3, 4, 5, 6})->data()}, 1));
    std::shared_ptr<arrow::Array> masked;
    ASSERT_TRUE(mask_arrow_child_slots_not_visible(*list, 0, arrow::default_memory_pool(), &masked)
                        .ok());
    EXPECT_EQ((std::vector<bool> {false, false, true, true, false, false}), nulls_of(*masked));

    const auto structs = arrow::MakeArray(
            arrow::ArrayData::Make(arrow::struct_({arrow::field("v", arrow::int32())}), 3,
                                   {bitmap_of(0x05)}, {int32_array({7, 8, 9})->data()}, 1));
    ASSERT_TRUE(
            mask_arrow_child_slots_not_visible(*structs, 0, arrow::default_memory_pool(), &masked)
                    .ok());
    EXPECT_EQ((std::vector<bool> {false, true, false}), nulls_of(*masked));
}

// MAP rows {a: 1}, NULL over {b: 2} and {c: 3}: the hidden entry is marked NULL in the items, while
// the entries struct and the keys stay NULL-free, so the map rebuilt around the masked entries is
// still valid. Marking the entries struct or the keys instead makes MapArray::SetData() abort.
TEST(ArrowParentNullsTest, MasksTheItemsOfNullMapRows) {
    const auto entries = map_entries(int32_array({1, 2, 3}));
    const auto map = arrow::MakeArray(arrow::ArrayData::Make(
            arrow::map(arrow::utf8(), arrow::int32()), 3,
            {bitmap_of(0x05), arrow::Buffer::FromVector(std::vector<int32_t> {0, 1, 2, 3})},
            {entries->data()}, 1));
    std::shared_ptr<arrow::Array> masked;
    ASSERT_TRUE(mask_arrow_child_slots_not_visible(*map, 0, arrow::default_memory_pool(), &masked)
                        .ok());
    EXPECT_FALSE(masked->data()->MayHaveNulls());
    const auto& masked_entries = static_cast<const arrow::StructArray&>(*masked);
    EXPECT_EQ(entries->data()->child_data[0].get(), masked_entries.field(0)->data().get());
    EXPECT_EQ((std::vector<bool> {false, true, false}), nulls_of(*masked_entries.field(1)));
    auto rebuilt = map->data()->Copy();
    rebuilt->child_data[0] = masked->data();
    const auto status = arrow::MakeArray(rebuilt)->ValidateFull();
    EXPECT_TRUE(status.ok()) << status.ToString();
}

// The slice [1, 3) of LIST rows [1], [2, 3], NULL over {4} keeps only child slots 1 and 2: slot 0
// belongs to row 0, outside the slice, and slot 3 to the NULL row 2. Likewise the slice [1, 2) of
// three STRUCT rows keeps only child slot 1. Masking only the NULL rows leaves slot 0 of the list
// and slots 0 and 2 of the struct valid.
TEST(ArrowParentNullsTest, MasksTheChildSlotsOutsideASlicedParent) {
    const auto list = arrow::MakeArray(arrow::ArrayData::Make(
            arrow::list(arrow::int32()), 3,
            {bitmap_of(0x03), arrow::Buffer::FromVector(std::vector<int32_t> {0, 1, 3, 4})},
            {int32_array({1, 2, 3, 4})->data()}, 1));
    std::shared_ptr<arrow::Array> masked;
    ASSERT_TRUE(mask_arrow_child_slots_not_visible(*list->Slice(1, 2), 0,
                                                   arrow::default_memory_pool(), &masked)
                        .ok());
    EXPECT_EQ((std::vector<bool> {true, false, false, true}), nulls_of(*masked));

    const auto structs = arrow::MakeArray(
            arrow::ArrayData::Make(arrow::struct_({arrow::field("v", arrow::int32())}), 3,
                                   {nullptr}, {int32_array({7, 8, 9})->data()}, 0));
    ASSERT_TRUE(mask_arrow_child_slots_not_visible(*structs->Slice(1, 1), 0,
                                                   arrow::default_memory_pool(), &masked)
                        .ok());
    EXPECT_EQ((std::vector<bool> {true, false, true}), nulls_of(*masked));
}

// A list whose only NULL row owns no slots, and fixed-size lists, structs and maps without NULL
// rows that reference their whole child, return the child's own data and allocate nothing.
// Allocating before a hidden slot is found makes num_allocations() non-zero.
TEST(ArrowParentNullsTest, ReturnsTheChildUnchangedWhenEverySlotIsVisible) {
    const auto values = int32_array({1, 2, 3});
    const std::vector<std::shared_ptr<arrow::Array>> parents {
            // Rows [1, 2], NULL and empty, [3].
            arrow::MakeArray(arrow::ArrayData::Make(
                    arrow::list(arrow::int32()), 3,
                    {bitmap_of(0x05), arrow::Buffer::FromVector(std::vector<int32_t> {0, 2, 2, 3})},
                    {values->data()}, 1)),
            arrow::MakeArray(arrow::ArrayData::Make(arrow::fixed_size_list(arrow::int32(), 1), 3,
                                                    {nullptr}, {values->data()}, 0)),
            arrow::MakeArray(
                    arrow::ArrayData::Make(arrow::struct_({arrow::field("v", arrow::int32())}), 3,
                                           {nullptr}, {values->data()}, 0)),
            arrow::MakeArray(arrow::ArrayData::Make(
                    arrow::map(arrow::utf8(), arrow::int32()), 3,
                    {nullptr, arrow::Buffer::FromVector(std::vector<int32_t> {0, 1, 2, 3})},
                    {map_entries(values)->data()}, 0)),
    };
    for (const auto& parent : parents) {
        SCOPED_TRACE(parent->type()->ToString());
        arrow::ProxyMemoryPool pool(arrow::default_memory_pool());
        std::shared_ptr<arrow::Array> masked;
        ASSERT_TRUE(mask_arrow_child_slots_not_visible(*parent, 0, &pool, &masked).ok());
        EXPECT_EQ(parent->data()->child_data[0].get(), masked->data().get());
        EXPECT_EQ(0, pool.num_allocations());
    }
}

// Offsets that reach past the child, a NULL row whose offsets run backwards, and a parent layout
// the mask does not know are rejected before any bit is written. Without the range checks the
// NULL row [3, 1) clears a negative number of bits.
TEST(ArrowParentNullsTest, RejectsSlotsOutsideTheChildAndUnknownParents) {
    const auto values = int32_array({1, 2, 3});
    const std::vector<std::shared_ptr<arrow::Array>> parents {
            // Row 1 ends at slot 5 of a three-slot child.
            arrow::MakeArray(arrow::ArrayData::Make(
                    arrow::list(arrow::int32()), 2,
                    {nullptr, arrow::Buffer::FromVector(std::vector<int32_t> {0, 1, 5})},
                    {values->data()}, 0)),
            // Row 1 is NULL over the backwards range [3, 1).
            arrow::MakeArray(arrow::ArrayData::Make(
                    arrow::list(arrow::int32()), 3,
                    {bitmap_of(0x05), arrow::Buffer::FromVector(std::vector<int32_t> {0, 3, 1, 3})},
                    {values->data()}, 1)),
            arrow::MakeArray(arrow::ArrayData::Make(
                    arrow::list_view(arrow::int32()), 1,
                    {nullptr, arrow::Buffer::FromVector(std::vector<int32_t> {0}),
                     arrow::Buffer::FromVector(std::vector<int32_t> {3})},
                    {values->data()}, 0)),
    };
    for (const auto& parent : parents) {
        SCOPED_TRACE(parent->type()->ToString());
        std::shared_ptr<arrow::Array> masked;
        const auto status = mask_arrow_child_slots_not_visible(
                *parent, 0, arrow::default_memory_pool(), &masked);
        EXPECT_TRUE(status.is<ErrorCode::INVALID_ARGUMENT>()) << status.to_string();
    }
}

} // namespace doris
