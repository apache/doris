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

#include "vec/columns/common_column_test.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

namespace doris::vectorized {

// MOCK SITUATION TEST -- here ut will test the common column function for column type, this function called in mocked situation to test multiple column type.
TEST_F(CommonColumnTest, SeDeserializeWithArena) {
    MutableColumns columns(4);
    columns[0] = col_str->clone();
    columns[1] = col_int->clone();
    columns[2] = col_arr->clone();
    columns[3] = col_map->clone();
    DataTypes data_types = {std::make_shared<DataTypeString>(), std::make_shared<DataTypeInt64>(),
                            std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt64>()),
                            std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(),
                                                          std::make_shared<DataTypeInt64>())};
    ser_deserialize_with_arena_impl(columns, data_types);
}

TEST_F(CommonColumnTest, SeDeserializeVec) {
    MutableColumns columns(2);
    columns[0] = col_str->clone();
    columns[1] = col_int->clone();
    // array | map | struct get_max_row_byte_size does not implement
    //    columns[2] = col_arr->clone();
    //    columns[3] = col_map->clone();
    ser_deser_vec(columns, {std::make_shared<DataTypeString>(), std::make_shared<DataTypeInt64>()});
}

TEST_F(CommonColumnTest, FilterBySelector) {
    // make a PredictColumn
    auto ptr = Schema::get_predicate_column_ptr(FieldType::OLAP_FIELD_TYPE_BIGINT, false,
                                                ReaderType::READER_QUERY);
    auto dt = std::make_shared<DataTypeInt64>();
    // 1. 空选择器
    std::vector<uint16_t> selector_empty = {};
    filterBySelectorAssert(ptr->get_ptr(), selector_empty, *dt, col_int->get_ptr(), 0);

    // 2. 全选
    std::vector<uint16_t> selector_all = {1, 1, 1, 1, 1};
    filterBySelectorAssert(ptr->get_ptr(), selector_all, *dt, col_int->get_ptr(), 0);

    // 3. 全不选
    std::vector<uint16_t> select_none = {0, 0, 0, 0, 0};
    filterBySelectorAssert(ptr->get_ptr(), select_none, *dt, col_int->get_ptr(), 0);

    // 4. 部分选择
    std::vector<uint16_t> selector_partial = {1, 0, 1, 0, 1};
    filterBySelectorAssert(ptr->get_ptr(), selector_partial, *dt, col_int->get_ptr(), 0);

    // 5. 选择器长度
    // 不匹配
    std::vector<uint16_t> selector_invalid = {1, 1, 1, 1};
    filterBySelectorAssert(ptr->get_ptr(), selector_invalid, *dt, col_int->get_ptr(), 0);
}

TEST_F(CommonColumnTest, Permute) {
    // 1. generate same rows of columns
    auto columnInt64ValueGetter = [](size_t range_index, size_t index_in_range) {
        return Field(static_cast<Int64>(range_index * index_in_range));
    };

    auto columnFloat64ValueGetter = [](size_t range_index, size_t index_in_range) -> Field {
        if (range_index % 2 == 0 && index_in_range % 4 == 0) {
            // quiet_NaN 初始化浮点数，以表明该值当前无效或者尚未定义，
            // 并且不会在传递该值时触发错误。程序可以在之后检查这些值是否为 NaN 来决定下一步的操作。
            return std::numeric_limits<Float64>::quiet_NaN();
        } else if (range_index % 2 == 0 && index_in_range % 5 == 0) {
            // 负无穷大
            return -std::numeric_limits<Float64>::infinity();
        } else if (range_index % 2 == 0 && index_in_range % 6 == 0) {
            // 正无穷大
            return std::numeric_limits<Float64>::infinity();
        }
        Float64 value = static_cast<Float64>(range_index * index_in_range);
        return Field(value);
    };

    auto columnDecimal64ValueGetter = [](size_t range_index, size_t index_in_range) -> Field {
        Decimal64 val = static_cast<Decimal64>(range_index * index_in_range);
        return DecimalField(val, 2);
    };

    auto columnStringGetter = [](size_t range_index, size_t index_in_range) -> Field {
        return Field(std::to_string(range_index * index_in_range));
    };
    ColumnString::MutablePtr col_s = ColumnString::create();
    ColumnInt64::MutablePtr col_i = ColumnInt64::create();
    ColumnFloat64::MutablePtr col_f = ColumnFloat64::create();
    ColumnDecimal64::MutablePtr col_d = ColumnDecimal64::create(0, 2);
    MutableColumns columns;
    columns.emplace_back(col_s->get_ptr());
    columns.emplace_back(col_i->get_ptr());
    columns.emplace_back(col_f->get_ptr());
    columns.emplace_back(col_d->get_ptr());

    vectorized::IColumn::Permutation permutation;

    size_t num_rows = 10;
    for (size_t i = 0; i < num_rows; ++i) permutation.emplace_back(num_rows - 1 - i);

    std::cout << "permutation size:" << permutation.size() << std::endl;

    std::vector<std::vector<Field>> ranges(num_rows);
    generateRanges(ranges, num_rows, columnStringGetter);
    insertRangesIntoColumn(ranges, permutation, *col_s);
    generateRanges(ranges, num_rows, columnInt64ValueGetter);
    insertRangesIntoColumn(ranges, permutation, *col_i);
    generateRanges(ranges, num_rows, columnFloat64ValueGetter);
    insertRangesIntoColumn(ranges, permutation, *col_f);
    generateRanges(ranges, num_rows, columnDecimal64ValueGetter);
    insertRangesIntoColumn(ranges, permutation, *col_d);
    assertPermute(columns, permutation, num_rows);
}

TEST_F(CommonColumnTest, SortColumnDescription) {
    ColumnString::MutablePtr col_s = ColumnString::create();

    auto columnStringGetter = [](size_t range_index, size_t index_in_range) -> Field {
        return Field(std::to_string(range_index * index_in_range));
    };
    size_t num_rows = 10;
    std::vector<std::vector<Field>> ranges(num_rows);
    IColumn::Permutation permutation;

    for (size_t i = 0; i < num_rows; ++i) permutation.emplace_back(num_rows - 1 - i);
    generateRanges(ranges, num_rows, columnStringGetter);
    insertRangesIntoColumn(ranges, permutation, *col_s);

    assertSortColumn(*col_s, permutation, num_rows);
}

} // namespace doris::vectorized