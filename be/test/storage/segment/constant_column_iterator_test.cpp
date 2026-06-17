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

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/column/column.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/field.h"
#include "storage/segment/column_reader.h"
#include "storage/segment/common.h"

using namespace doris::segment_v2;

namespace doris {

class ConstantColumnIteratorTest : public testing::Test {};

// next_batch fills every row with the constant value, advances the ordinal,
// and reports has_null = false for a non-null value.
TEST_F(ConstantColumnIteratorTest, NextBatchFillsConstant) {
    const int64_t kValue = 12345;
    ConstantColumnIterator it(Field::create_field<TYPE_BIGINT>(kValue));

    MutableColumnPtr dst = ColumnVector<TYPE_BIGINT>::create();
    size_t n = 5;
    bool has_null = true;
    ASSERT_TRUE(it.next_batch(&n, dst, &has_null).ok());

    ASSERT_EQ(5, dst->size());
    ASSERT_FALSE(has_null);
    auto* col = assert_cast<ColumnInt64*>(dst.get());
    for (size_t i = 0; i < 5; i++) {
        ASSERT_EQ(kValue, col->get_element(i));
    }
    ASSERT_EQ(5, it.get_current_ordinal());
}

// read_by_rowids fills count rows with the constant value regardless of rowids.
TEST_F(ConstantColumnIteratorTest, ReadByRowidsFillsConstant) {
    const int64_t kValue = 777;
    ConstantColumnIterator it(Field::create_field<TYPE_BIGINT>(kValue));

    MutableColumnPtr dst = ColumnVector<TYPE_BIGINT>::create();
    rowid_t rowids[] = {3, 0, 9, 1};
    size_t count = sizeof(rowids) / sizeof(rowids[0]);
    ASSERT_TRUE(it.read_by_rowids(rowids, count, dst).ok());

    ASSERT_EQ(count, dst->size());
    auto* col = assert_cast<ColumnInt64*>(dst.get());
    for (size_t i = 0; i < count; i++) {
        ASSERT_EQ(kValue, col->get_element(i));
    }
}

// seek_to_ordinal only moves the cursor; the constant value is independent of position.
TEST_F(ConstantColumnIteratorTest, SeekThenNextBatch) {
    const int64_t kValue = 42;
    ConstantColumnIterator it(Field::create_field<TYPE_BIGINT>(kValue));

    ASSERT_TRUE(it.seek_to_ordinal(10).ok());
    ASSERT_EQ(10, it.get_current_ordinal());

    MutableColumnPtr dst = ColumnVector<TYPE_BIGINT>::create();
    size_t n = 3;
    bool has_null = false;
    ASSERT_TRUE(it.next_batch(&n, dst, &has_null).ok());
    ASSERT_EQ(13, it.get_current_ordinal());
    auto* col = assert_cast<ColumnInt64*>(dst.get());
    for (size_t i = 0; i < 3; i++) {
        ASSERT_EQ(kValue, col->get_element(i));
    }
}

// A predicate-column destination (used when the column has a pushed-down predicate,
// e.g. __DORIS_COMMIT_TSO_COL__ <= t) is, for BIGINT, the canonical ColumnInt64 and is
// already covered by NextBatchFillsConstant.

// next_batch_of_zone_map delegates to next_batch (constant column has min == max).
TEST_F(ConstantColumnIteratorTest, NextBatchOfZoneMap) {
    const int64_t kValue = 555;
    ConstantColumnIterator it(Field::create_field<TYPE_BIGINT>(kValue));

    MutableColumnPtr dst = ColumnVector<TYPE_BIGINT>::create();
    size_t n = 2;
    ASSERT_TRUE(it.next_batch_of_zone_map(&n, dst).ok());
    ASSERT_EQ(2, dst->size());
    auto* col = assert_cast<ColumnInt64*>(dst.get());
    ASSERT_EQ(kValue, col->get_element(0));
    ASSERT_EQ(kValue, col->get_element(1));
}

// A null Field reports has_null = true and inserts defaults (null on a nullable column).
TEST_F(ConstantColumnIteratorTest, NullValueInsertsNull) {
    ConstantColumnIterator it {Field()};

    MutableColumnPtr dst =
            ColumnNullable::create(ColumnVector<TYPE_BIGINT>::create(), ColumnUInt8::create());
    size_t n = 3;
    bool has_null = false;
    ASSERT_TRUE(it.next_batch(&n, dst, &has_null).ok());

    ASSERT_EQ(3, dst->size());
    ASSERT_TRUE(has_null);
    auto* nullable = assert_cast<ColumnNullable*>(dst.get());
    for (size_t i = 0; i < 3; i++) {
        ASSERT_TRUE(nullable->is_null_at(i));
    }
}

} // namespace doris
