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

#include "vec/columns/column_array.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include "vec/columns/column.h"
#include "vec/columns/columns_number.h"
#include "vec/columns/common_column_test.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"

// this test is gonna to be a column test template for all column which should make ut test to coverage the function defined in column
// for example column_array should test this function:
// size, reserve, resize, empty, byte_size, allocated_bytes, clone_resized,
// get_shrinked_column, filter, filter_by_selector, serialize_vec, deserialize_vec, get_max_row_byte_size
//
namespace doris::vectorized {
class ColumnArrayTest : public CommonColumnTest {
protected:
    void dump_size(ColumnArray::MutablePtr& arr) {
        std::cout << "size: " << arr->size() << std::endl;
        std::cout << "offset size: " << arr->get_offsets().size() << std::endl;
        std::cout << "data size: " << arr->get_data_ptr()->size() << std::endl;
        std::cout << "allocated_bytes: " << arr->allocated_bytes() << std::endl;
        std::cout << "byte_size: " << arr->byte_size() << std::endl;
    }

    // in array we should always check size with data and offset column
    void check_size(IColumn& arr, size_t expect_size) {
        auto& a = assert_cast<ColumnArray&>(arr);
        EXPECT_EQ(a.size(), expect_size);
        EXPECT_EQ(a.get_offsets().size(), expect_size);
        EXPECT_EQ(a.get_offsets().back(), a.get_data_ptr()->size());
    }

    void SetUp() override {
        col_int_arr =
                ColumnArray::create(ColumnInt64::create(), ColumnArray::ColumnOffsets::create());
        Array array1 = {1, 2, 3};
        Array array2 = {4};
        col_int_arr->insert(array1);
        col_int_arr->insert(Array());
        col_int_arr->insert(array2);
        col_int_arr->insert(Null());

        col_string_arr =
                ColumnArray::create(ColumnString::create(), ColumnArray::ColumnOffsets::create());
        Array array3 = {"1\\0", "2", "3"};
        Array array4 = {"4"};
        col_string_arr->insert(array3);
        col_string_arr->insert(Array());
        col_string_arr->insert(array4);
    }

    ColumnArray::MutablePtr col_int_arr;
    ColumnArray::MutablePtr col_string_arr;
};

TEST_F(ColumnArrayTest, SizeTest) {
    // now column_array size() use offsets_column size as the size of itself,
    // but we should make sure the length of data_column is the same as the size of itself
    check_size(*col_int_arr, 4);
}

TEST_F(ColumnArrayTest, ReserveTest) {
    // reserve now in ColumnArray make offsets column and data column reserve the same size
    // reserve bigger ?
    col_int_arr->reserve(10);
    EXPECT_GE(col_int_arr->size(), 4);
    col_int_arr->reserve(0);
    EXPECT_GE(col_int_arr->size(), 0);
    // reserve to -1 will fatal with  Check failed: false Amount of memory requested to allocate is more than allowed, num_elements 18446744073709551615, ELEMENT_SIZE 8
    //    col_int_arr->reserve(-1);
    //    dump_size(col_int_arr);
}

TEST_F(ColumnArrayTest, ResizeTest) {
    // resize now in ColumnArray make offsets column
    // and data column resize to offsets.back()
    // resize bigger ?
    col_int_arr->resize(10);
    check_size(*col_int_arr, 10);
    col_int_arr->resize(0);
    check_size(*col_int_arr, 0);
    // resize to -1 will fatal with
    // Check failed: false Amount of memory requested to allocate is more than allowed, num_elements 18446744073709551615, ELEMENT_SIZE 8
    //     EXPECT_ANY_THROW(col_int_arr->resize(-1));
}

TEST_F(ColumnArrayTest, ReplicateTest) {
    // replicate now in ColumnArray make offsets column and data column replicate the same size
    // replicate bigger ?
    auto rep = col_int_arr->replicate(IColumn::Offsets(4, 10));
    check_size(*col_int_arr, 4);
    check_size(rep->assume_mutable_ref(), 10);
    rep = col_int_arr->replicate(IColumn::Offsets(4, 0));
    check_size(*col_int_arr, 4);
    check_size(rep->assume_mutable_ref(), 0);
    // replicate to -1 will hang this interface, so we should not set the size to -1
    //    col_int_arr->replicate(IColumn::Offsets(4, -1));
}

TEST_F(ColumnArrayTest, ByteSizeTest) {
    // column_array byte_size means the byte size of
    // the data column + offsets_size * size_of(offsets_type)
    size_t expect_size = 5 * sizeof(int64_t) + 4 * sizeof(int64_t);
    byteSizeAssert(col_int_arr->get_ptr(), expect_size);
}

TEST_F(ColumnArrayTest, AllocatedBytesTest) {
    // column_array allocated_bytes means the allocated bytes of the data column + allocated bytes of the offsets column
    // which should satisfied the real size of the column
    EXPECT_EQ(col_int_arr->get_data_ptr()->allocated_bytes(), 4096);
    EXPECT_EQ(col_int_arr->get_offsets().allocated_bytes(), 4096);
    allocatedBytesAssert(col_int_arr->get_ptr(), 8192); // contains data pod and offsets pod
}

TEST_F(ColumnArrayTest, CloneResizedTest) {
    // clone_resized will clone the column_array and resize the new column_array to the size of the original column_array
    auto new_col_int_arr = col_int_arr->clone_resized(10);
    check_size(*new_col_int_arr, 10);
    new_col_int_arr = col_int_arr->clone_resized(0);
    check_size(*new_col_int_arr, 0);
    // resize to -1 will fatal with
    // Check failed: false Amount of memory requested to allocate is more than allowed, num_elements 18446744073709551615, ELEMENT_SIZE 8
    //    new_col_int_arr = col_int_arr->clone_resized(-1);
}

TEST_F(ColumnArrayTest, GetShrinkedColumnTest) {
    // get_shrinked_column should only happened in char-type column or nested char-type column
    EXPECT_ANY_THROW(col_int_arr->get_shrinked_column());
    MutableColumnPtr shrinked_col = col_string_arr->get_shrinked_column();
    check_size(*shrinked_col, 3);
}

TEST_F(ColumnArrayTest, FilterTest) {
    std::vector<uint8_t> filter = {1, 0, 1, 0};
    filterAssert(col_int_arr->get_ptr(), filter, 2);
}

// array should not make a PredicateColumnType
TEST_F(ColumnArrayTest, FilterBySelectorTest) {}

// array do not implement permutation
TEST_F(ColumnArrayTest, PermuteTest) {
    IColumn::Permutation permutation = {3, 2, 1, 0};
    EXPECT_ANY_THROW(assertColumnPermutation(*col_int_arr, true, 0, 0, permutation, permutation));
}

TEST_F(ColumnArrayTest, SortColumnTest) {
    IColumn::Permutation permutation = {3, 2, 1, 0};
    assertSortColumn(*col_int_arr, permutation, 0);
}

} // namespace doris::vectorized