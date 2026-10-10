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

#include "core/column/column_file.h"

#include <gtest/gtest.h>

#include <array>
#include <string>
#include <type_traits>
#include <vector>

#include "common/exception.h"
#include "core/arena.h"
#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/column/column_varbinary.h"
#include "core/column/column_vector.h"
#include "core/data_type/primitive_type.h"
#include "exec/common/sip_hash.h"
#include "exec/sort/hybrid_sorter.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"

namespace doris {

class ColumnFileTest : public ::testing::Test {
protected:
    // Exercise both StringView's small value and arena-backed representations.
    const std::string small_bytes = std::string("\0\xff\x80z", 4);
    const std::string large_bytes = std::string(4096, '\xff') + std::string("\0end", 4);

    static MutableColumns children() {
        MutableColumns result;
        result.push_back(ColumnNullable::create(ColumnString::create(), ColumnUInt8::create()));
        result.push_back(ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create()));
        result.push_back(ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create()));
        result.push_back(ColumnNullable::create(ColumnString::create(), ColumnUInt8::create()));
        result.push_back(ColumnNullable::create(ColumnString::create(), ColumnUInt8::create()));
        result.push_back(ColumnNullable::create(ColumnVarbinary::create(), ColumnUInt8::create()));
        return result;
    }

    static Field file(const std::string* bytes, bool metadata = true) {
        File value(6);
        value[0] = Field::create_field<TYPE_STRING>("s3://bucket/a%2Fb?versionId=AbC");
        if (metadata) {
            value[1] = Field::create_field<TYPE_BIGINT>(7);
            value[2] = Field::create_field<TYPE_BIGINT>(1024);
            value[3] = Field::create_field<TYPE_STRING>("application/octet-stream");
            value[4] = Field::create_field<TYPE_STRING>("ETAG:opaque-2");
        }
        if (bytes != nullptr) {
            value[5] = Field::create_field<TYPE_VARBINARY>(
                    StringView(bytes->data(), cast_set<uint32_t>(bytes->size())));
        }
        return Field::create_field<TYPE_FILE>(std::move(value));
    }

    static void expect_file(const IColumn& column, size_t row, const Field& expected) {
        const auto actual = column[row];
        ASSERT_EQ(actual.get_type(), TYPE_FILE);
        const auto& fields = actual.get<TYPE_FILE>();
        const auto& expected_fields = expected.get<TYPE_FILE>();
        ASSERT_EQ(fields.size(), 6);
        for (size_t i = 0; i < fields.size(); ++i) {
            SCOPED_TRACE(i);
            ASSERT_EQ(fields[i].is_null(), expected_fields[i].is_null());
            if (expected_fields[i].is_null()) {
                continue;
            }
            ASSERT_EQ(fields[i].get_type(), expected_fields[i].get_type());
            if (i == 5) {
                EXPECT_EQ(fields[i].get<TYPE_VARBINARY>().to_string_ref().to_string(),
                          expected_fields[i].get<TYPE_VARBINARY>().to_string_ref().to_string());
            } else if (i == 1 || i == 2) {
                EXPECT_EQ(fields[i].get<TYPE_BIGINT>(), expected_fields[i].get<TYPE_BIGINT>());
            } else {
                EXPECT_EQ(fields[i].get<TYPE_STRING>(), expected_fields[i].get<TYPE_STRING>());
            }
        }
    }

    static uint64_t sip_hash(const IColumn& column, size_t row) {
        SipHash hash;
        column.update_hash_with_value(row, hash);
        return hash.get64();
    }

    template <typename F>
    static void expect_not_implemented(F&& operation) {
        try {
            operation();
            FAIL() << "FILE ordering must fail";
        } catch (const Exception& e) {
            EXPECT_EQ(e.code(), ErrorCode::NOT_IMPLEMENTED_ERROR);
        }
    }
};

TEST_F(ColumnFileTest, PredicateSelectorPreservesRowsAndOwnedInline) {
    auto source = ColumnFile::create();
    source->insert(file(&large_bytes));
    source->insert(file(nullptr, false));
    source->insert(file(&small_bytes));
    const std::array<uint16_t, 4> selector {2, 0, 2, 1};
    auto selected = ColumnFile::create();
    ASSERT_TRUE(source->filter_by_selector(selector.data(), selector.size(), selected.get()).ok());
    source->clear();
    ASSERT_EQ(selected->size(), 4);
    expect_file(*selected, 0, file(&small_bytes));
    expect_file(*selected, 1, file(&large_bytes));
    expect_file(*selected, 2, file(&small_bytes));
    expect_file(*selected, 3, file(nullptr, false));
    auto empty = ColumnFile::create();
    ASSERT_TRUE(selected->filter_by_selector(nullptr, 0, empty.get()).ok());
    EXPECT_TRUE(empty->empty());
}

TEST_F(ColumnFileTest, PredicateSelectorPreservesParentNullsAndEmptyInline) {
    auto source = ColumnNullable::create(ColumnFile::create(), ColumnUInt8::create());
    const std::string empty;
    source->insert(file(&large_bytes));
    source->insert_default();
    source->insert(file(&empty));
    source->insert(file(nullptr, false));
    const std::array<uint16_t, 5> selector {3, 1, 2, 0, 1};
    auto selected = source->clone_empty();
    ASSERT_TRUE(source->filter_by_selector(selector.data(), selector.size(), selected.get()).ok());
    source->clear();
    ASSERT_EQ(selected->size(), 5);
    expect_file(*selected, 0, file(nullptr, false));
    EXPECT_TRUE(selected->is_null_at(1));
    expect_file(*selected, 2, file(&empty));
    expect_file(*selected, 3, file(&large_bytes));
    EXPECT_TRUE(selected->is_null_at(4));
}

TEST_F(ColumnFileTest, OwnedInlineRemainsTrackedUntilLastFieldCopyIsReleased) {
    auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER,
                                                    "FILE-inline-ownership-test");
    SCOPED_ATTACH_TASK(tracker);
    const auto consumption = [&] {
        thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        return tracker->consumption();
    };
    const auto baseline = consumption();
    Field retained;
    {
        auto source = ColumnFile::create();
        source->insert(file(&large_bytes));
        retained = (*source)[0];
    }
    // The source's tracked arena is gone, but the extracted FILE owns its payload.
    const auto owned = consumption();
    EXPECT_GE(owned - baseline, large_bytes.size());
    Field copy = retained;
    // VARBINARY Field copies own and track their bytes independently.
    EXPECT_GE(consumption() - owned, large_bytes.size());
    retained = Field();
    EXPECT_EQ(consumption(), owned);
    EXPECT_EQ(copy.get<TYPE_FILE>()[5].get<TYPE_VARBINARY>().to_string_ref().to_string(),
              large_bytes);
    copy = Field();
    EXPECT_EQ(consumption(), baseline);
}

TEST_F(ColumnFileTest, FixedNullableShapeAndIndependentIdentity) {
    static_assert(!std::is_base_of_v<ColumnStruct, ColumnFile>);
    auto column = ColumnFile::create(children());
    EXPECT_EQ(column->tuple_size(), 6);
    EXPECT_EQ(column->get_columns().size(), 6);
    EXPECT_EQ(column->size(), 0);
    EXPECT_TRUE(column->structure_equals(*ColumnFile::create()));
    EXPECT_FALSE(column->structure_equals(*ColumnStruct::create(children())));
    auto wide_children = children();
    wide_children[0] = ColumnNullable::create(ColumnString64::create(), ColumnUInt8::create());
    auto wide = ColumnFile::create(std::move(wide_children));
    EXPECT_FALSE(column->structure_equals(*wide));
    EXPECT_FALSE(wide->structure_equals(*column));
    EXPECT_TRUE(wide->structure_equals(*wide->clone_empty()));
    column->insert_default();
    for (size_t i = 0; i < 6; ++i) {
        const auto& child = assert_cast<const ColumnNullable&>(column->get_column(i));
        EXPECT_TRUE(child.is_null_at(0));
        if (i == 5) {
            EXPECT_NE(dynamic_cast<const ColumnVarbinary*>(&child.get_nested_column()), nullptr);
        } else if (i == 1 || i == 2) {
            EXPECT_NE(dynamic_cast<const ColumnInt64*>(&child.get_nested_column()), nullptr);
        } else {
            EXPECT_NE(dynamic_cast<const ColumnString*>(&child.get_nested_column()), nullptr);
        }
    }
    column->sanity_check();
}

TEST_F(ColumnFileTest, RejectMalformedPhysicalChildren) {
    auto wrong_count = children();
    wrong_count.pop_back();
    EXPECT_THROW(ColumnFile::create(std::move(wrong_count)), Exception);
    auto non_nullable = children();
    non_nullable[0] = ColumnString::create();
    EXPECT_THROW(ColumnFile::create(std::move(non_nullable)), Exception);
    auto wrong_binary = children();
    wrong_binary[5] = ColumnNullable::create(ColumnString::create(), ColumnUInt8::create());
    EXPECT_THROW(ColumnFile::create(std::move(wrong_binary)), Exception);
    auto wrong_integer = children();
    wrong_integer[1] = ColumnNullable::create(ColumnInt32::create(), ColumnUInt8::create());
    EXPECT_THROW(ColumnFile::create(std::move(wrong_integer)), Exception);
    auto constant = children();
    constant[0]->insert_default();
    constant[0] = ColumnConst::create(std::move(constant[0]), 0);
    EXPECT_THROW(ColumnFile::create(std::move(constant)), Exception);
    auto uneven = children();
    uneven[2]->insert_default();
    EXPECT_THROW(ColumnFile::create(std::move(uneven)), Exception);

    auto column = ColumnFile::create();
    EXPECT_THROW(column->insert(Field::create_field<TYPE_FILE>(File(5))), Exception);
    EXPECT_EQ(column->size(), 0);
}

TEST_F(ColumnFileTest, FieldRoundTripPreservesNullEmptyAndBinary) {
    const std::string empty;
    auto column = ColumnFile::create();
    std::vector<Field> values {file(nullptr, false), file(&empty), file(&small_bytes),
                               file(&large_bytes)};
    for (const auto& value : values) {
        column->insert(value);
    }
    for (size_t i = 0; i < values.size(); ++i) {
        expect_file(*column, i, values[i]);
    }
    Field result;
    column->get(3, result);
    auto copy = ColumnFile::create();
    copy->insert(result);
    column->clear();
    expect_file(*copy, 0, values[3]);
}

TEST_F(ColumnFileTest, FileFieldsOwnInlineBytesAcrossColumnReuseAndFieldCopies) {
    std::string source = large_bytes;
    auto value = file(&source);
    source.assign(source.size(), 'x');
    const auto& bytes = value.get<TYPE_FILE>()[5].get<TYPE_VARBINARY>();
    EXPECT_EQ(bytes.str(), large_bytes);

    auto column = ColumnFile::create();
    column->insert(value);
    Field extracted;
    column->get(0, extracted);
    Field copied(extracted);
    Field assigned = file(nullptr);
    assigned = extracted;
    Field nested = Field::create_field<TYPE_ARRAY>(Array {extracted});
    column->clear();
    column->insert(file(&source));
    extracted = Field();
    value = Field();

    Field moved(std::move(copied));
    Field move_assigned = file(nullptr);
    move_assigned = std::move(assigned);
    auto restored = ColumnFile::create();
    restored->insert(moved);
    restored->insert(move_assigned);
    restored->insert(nested.get<TYPE_ARRAY>()[0]);
    const auto expected = file(&large_bytes);
    for (size_t row = 0; row < restored->size(); ++row) {
        expect_file(*restored, row, expected);
    }
}

TEST_F(ColumnFileTest, TransportAndReplicationKeepEveryChildAligned) {
    auto source = ColumnFile::create();
    const auto a = file(&small_bytes);
    const auto b = file(&large_bytes, false);
    source->insert(a);
    source->insert(b);
    auto target = ColumnFile::create();
    target->insert_from(*source, 1);
    target->insert_range_from(*source, 0, 2);
    target->insert_range_from_ignore_overflow(*source, 1, 1);
    target->insert_many_from(*source, 0, 2);
    const uint32_t indices[] = {1, 0, 1};
    target->insert_indices_from(*source, indices, indices + 3);
    target->insert_duplicate_fields(b, 2);
    const std::vector<Field> expected {b, a, b, b, a, a, b, a, b, b, b};
    source->clear();
    ASSERT_EQ(target->size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        expect_file(*target, i, expected[i]);
    }
    target->sanity_check();
}

TEST_F(ColumnFileTest, FilterPermuteEraseAndResize) {
    auto source = ColumnFile::create();
    const auto a = file(nullptr, false);
    const auto b = file(&small_bytes);
    const auto c = file(&large_bytes);
    source->insert(a);
    source->insert(b);
    source->insert(c);
    const IColumn::Filter filter {1, 0, 1};
    auto filtered = source->filter(filter, -1);
    ASSERT_EQ(filtered->size(), 2);
    expect_file(*filtered, 0, a);
    expect_file(*filtered, 1, c);
    IColumn::Permutation permutation {2, 0, 1};
    auto permuted = source->permute(permutation, 2);
    ASSERT_EQ(permuted->size(), 2);
    expect_file(*permuted, 0, c);
    expect_file(*permuted, 1, a);
    EXPECT_EQ(source->filter(filter), 2);
    expect_file(*source, 1, c);
    source->erase(0, 1);
    expect_file(*source, 0, c);
    source->reserve(8);
    source->resize(3);
    auto resized = source->clone_resized(5);
    const auto placeholder = Field::create_field<TYPE_FILE>(File(6));
    for (size_t i = 1; i < 5; ++i) {
        expect_file(*resized, i, placeholder);
    }
    source->resize(1);
    expect_file(*source, 0, c);
    source->pop_back(1);
    EXPECT_EQ(source->size(), 0);
    source->sanity_check();
    auto empty = source->clone_empty();
    EXPECT_TRUE(empty->structure_equals(*source));
}

TEST_F(ColumnFileTest, CopyOnWriteDetachesParentAndSharedChild) {
    auto original = ColumnFile::create();
    const auto value = file(&large_bytes);
    original->insert(value);
    ColumnPtr snapshot = std::move(original);
    ColumnPtr shared_child = assert_cast<const ColumnFile&>(*snapshot).get_column_ptr(5);
    auto changed = IColumn::mutate(snapshot);
    changed->clear();
    changed->insert(file(&small_bytes));
    expect_file(*snapshot, 0, value);
    expect_file(*changed, 0, file(&small_bytes));
    EXPECT_EQ((*shared_child)[0].get<TYPE_VARBINARY>().to_string_ref().to_string(), large_bytes);

    auto unique = ColumnFile::create();
    unique->insert(value);
    ColumnPtr child_only = std::as_const(*unique).get_column_ptr(5);
    ColumnPtr owner = std::move(unique);
    auto detached = IColumn::mutate(std::move(owner));
    EXPECT_TRUE(detached->is_exclusive());
    detached->clear();
    EXPECT_EQ((*child_only)[0].get<TYPE_VARBINARY>().to_string_ref().to_string(), large_bytes);
}

TEST_F(ColumnFileTest, ArenaAndBatchRowSerialization) {
    const std::string empty;
    const std::vector<Field> values {file(nullptr), file(&empty), file(&small_bytes),
                                     file(&large_bytes, false)};
    auto source = ColumnFile::create();
    for (const auto& value : values) {
        source->insert(value);
    }
    Arena arena;
    auto arena_copy = ColumnFile::create();
    std::vector<std::vector<char>> buffers(values.size());
    std::vector<StringRef> keys(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        const char* begin = nullptr;
        const auto serialized = source->serialize_value_into_arena(i, arena, begin);
        EXPECT_EQ(serialized.size, source->serialize_size_at(i));
        EXPECT_LE(serialized.size, source->get_max_row_byte_size());
        EXPECT_EQ(arena_copy->deserialize_and_insert_from_arena(serialized.data),
                  serialized.data + serialized.size);
        // A preceding serialized column already occupies one byte in each row.
        buffers[i].resize(serialized.size + 1);
        buffers[i][0] = 'p';
        keys[i] = StringRef(buffers[i].data(), 1);
    }
    source->serialize(keys.data(), keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        EXPECT_EQ(buffers[i][0], 'p');
        EXPECT_EQ(keys[i].size, buffers[i].size());
        ++keys[i].data;
        --keys[i].size;
    }
    auto batch_copy = ColumnFile::create();
    batch_copy->deserialize(keys.data(), keys.size());
    source->clear();
    for (size_t i = 0; i < values.size(); ++i) {
        EXPECT_EQ(keys[i].size, 0);
        expect_file(*arena_copy, i, values[i]);
        expect_file(*batch_copy, i, values[i]);
    }
}

TEST_F(ColumnFileTest, OuterNullAndNestedArraySerialization) {
    auto nullable = ColumnNullable::create(ColumnFile::create(), ColumnUInt8::create());
    const auto value = file(&large_bytes);
    nullable->insert_default();
    nullable->insert(value);
    auto array = ColumnArray::create(std::move(nullable), ColumnArray::ColumnOffsets::create());
    array->get_offsets().push_back(2);
    array->get_offsets().push_back(2);
    Arena arena;
    auto copy = array->clone_empty();
    for (size_t i = 0; i < 2; ++i) {
        const char* begin = nullptr;
        auto serialized = array->serialize_value_into_arena(i, arena, begin);
        EXPECT_EQ(copy->deserialize_and_insert_from_arena(serialized.data),
                  serialized.data + serialized.size);
    }
    const auto& copied_array = assert_cast<const ColumnArray&>(*copy);
    EXPECT_EQ(copied_array.get_offsets()[0], 2);
    EXPECT_EQ(copied_array.get_offsets()[1], 2);
    const auto& data = assert_cast<const ColumnNullable&>(copied_array.get_data());
    EXPECT_TRUE(data.is_null_at(0));
    EXPECT_FALSE(data.is_null_at(1));
    expect_file(data.get_nested_column(), 1, value);
}

TEST_F(ColumnFileTest, IntegrityHashIncludesEveryChildAndInlineNullness) {
    auto column = ColumnFile::create();
    const auto original = file(&large_bytes);
    column->insert(original);
    for (size_t i = 0; i < 6; ++i) {
        auto modified = original;
        modified.get<TYPE_FILE>()[i] = Field();
        column->insert(modified);
        EXPECT_NE(sip_hash(*column, 0), sip_hash(*column, i + 1));
    }
    const std::string empty;
    column->insert(file(&empty));
    column->insert(file(&small_bytes));
    EXPECT_NE(sip_hash(*column, 6), sip_hash(*column, 7));
    EXPECT_NE(sip_hash(*column, 0), sip_hash(*column, 8));
    auto copy = column->clone_resized(column->size());
    for (size_t i = 0; i < column->size(); ++i) {
        EXPECT_EQ(sip_hash(*column, i), sip_hash(*copy, i));
    }
}

TEST_F(ColumnFileTest, BatchAndRangeHashesSkipOuterNullPayload) {
    auto column = ColumnFile::create();
    column->insert(file(&small_bytes));
    column->insert(file(&large_bytes));
    const uint8_t null_map[] = {0, 1};
    std::array<uint64_t, 2> xx {17, 17};
    std::array<uint32_t, 2> crc {17, 17};
    std::array<uint32_t, 2> crc32c {17, 17};
    column->update_hashes_with_value(xx.data(), null_map);
    column->update_crcs_with_value(crc.data(), TYPE_FILE, 2, 0, null_map);
    column->update_crc32c_batch(crc32c.data(), null_map);
    uint64_t expected_xx = 17;
    uint32_t expected_crc = 17;
    uint32_t expected_crc32c = 17;
    column->update_xxHash_with_value(0, 1, expected_xx, nullptr);
    column->update_crc_with_value(0, 1, expected_crc, nullptr);
    column->update_crc32c_single(0, 1, expected_crc32c, nullptr);
    EXPECT_EQ(xx[0], expected_xx);
    EXPECT_EQ(crc[0], expected_crc);
    EXPECT_EQ(crc32c[0], expected_crc32c);
    EXPECT_NE(xx[0], 17);
    EXPECT_NE(crc[0], 17);
    EXPECT_NE(crc32c[0], 17);
    EXPECT_EQ(xx[1], 17);
    EXPECT_EQ(crc[1], 17);
    EXPECT_EQ(crc32c[1], 17);
    uint64_t masked_xx = 17;
    uint32_t masked_crc = 17;
    uint32_t masked_crc32c = 17;
    column->update_xxHash_with_value(0, 2, masked_xx, null_map);
    column->update_crc_with_value(0, 2, masked_crc, null_map);
    column->update_crc32c_single(0, 2, masked_crc32c, null_map);
    EXPECT_EQ(masked_xx, expected_xx);
    EXPECT_EQ(masked_crc, expected_crc);
    EXPECT_EQ(masked_crc32c, expected_crc32c);
}

TEST_F(ColumnFileTest, RejectComparisonAndSortingEvenWhenEmpty) {
    auto column = ColumnFile::create();
    HybridSorter sorter;
    IColumn::Permutation permutation;
    EqualFlags flags;
    EqualRange range {0, 0};
    for (size_t rows = 0; rows < 2; ++rows) {
        expect_not_implemented([&] { column->compare_at(0, 0, *column, 1); });
        expect_not_implemented([&] { column->get_permutation(false, 0, 1, sorter, permutation); });
        expect_not_implemented(
                [&] { column->sort_column(nullptr, flags, permutation, range, true); });
        column->insert(file(&small_bytes));
    }
}

TEST_F(ColumnFileTest, OuterNullHashesIgnoreHiddenPayload) {
    auto first = ColumnNullable::create(ColumnFile::create(), ColumnUInt8::create());
    auto second = ColumnNullable::create(ColumnFile::create(), ColumnUInt8::create());
    first->insert(file(&small_bytes));
    second->insert(file(&large_bytes));
    first->get_null_map_data()[0] = 1;
    second->get_null_map_data()[0] = 1;
    EXPECT_EQ(sip_hash(*first, 0), sip_hash(*second, 0));
    uint64_t first_xx = 17;
    uint64_t second_xx = 17;
    uint32_t first_crc = 17;
    uint32_t second_crc = 17;
    uint32_t first_crc32c = 17;
    uint32_t second_crc32c = 17;
    first->update_hashes_with_value(&first_xx, nullptr);
    second->update_hashes_with_value(&second_xx, nullptr);
    first->update_crcs_with_value(&first_crc, TYPE_FILE, 1, 0, nullptr);
    second->update_crcs_with_value(&second_crc, TYPE_FILE, 1, 0, nullptr);
    first->update_crc32c_batch(&first_crc32c, nullptr);
    second->update_crc32c_batch(&second_crc32c, nullptr);
    EXPECT_EQ(first_xx, second_xx);
    EXPECT_EQ(first_crc, second_crc);
    EXPECT_EQ(first_crc32c, second_crc32c);
    EXPECT_NE(first_xx, 17);
    EXPECT_NE(first_crc, 17);
    EXPECT_NE(first_crc32c, 17);
}

} // namespace doris
