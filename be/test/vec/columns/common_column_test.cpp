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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include "olap/schema.h"
#include "runtime/define_primitive_type.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/columns_number.h"
#include "vec/columns/predicate_column.h"
#include "vec/core/field.h"
#include "vec/core/sort_block.h"
#include "vec/core/sort_description.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_struct.h"

// this test is gonna to be a column test template for all column which should make ut test to coverage the function defined in column
// for example column_array should test this function:
// size, reserve, resize, empty, byte_size, allocated_bytes, clone_resized,
// get_shrinked_column, filter, filter_by_selector, serialize_vec, deserialize_vec, get_max_row_byte_size
//
namespace doris::vectorized {
class CommonColumnTest : public ::testing::Test {
public:
    void SetUp() override {
        col_str = ColumnString::create();
        col_str->insert_data("aaa", 3);
        col_str->insert_data("bb", 2);
        col_str->insert_data("cccc", 4);

        col_int = ColumnInt64::create();
        col_int->insert_value(1);
        col_int->insert_value(2);
        col_int->insert_value(3);

        col_dcm = ColumnDecimal64::create(0, 3);
        col_dcm->insert_value(1.23);
        col_dcm->insert_value(4.56);
        col_dcm->insert_value(7.89);

        col_arr = ColumnArray::create(ColumnInt64::create(), ColumnArray::ColumnOffsets::create());
        Array array1 = {1, 2, 3};
        Array array2 = {4};
        col_arr->insert(array1);
        col_arr->insert(Array());
        col_arr->insert(array2);

        col_map = ColumnMap::create(ColumnString::create(), ColumnInt64::create(),
                                    ColumnArray::ColumnOffsets::create());
        Array k1 = {"a", "b", "c"};
        Array v1 = {1, 2, 3};
        Array k2 = {"d"};
        Array v2 = {4};
        Array a = Array();
        Map map1, map2, map3;
        map1.push_back(k1);
        map1.push_back(v1);
        col_map->insert(map1);
        map3.push_back(a);
        map3.push_back(a);
        col_map->insert(map3);
        map2.push_back(k2);
        map2.push_back(v2);
        col_map->insert(map2);
    }

public:
    ColumnString::MutablePtr col_str;
    ColumnInt64::MutablePtr col_int;
    ColumnDecimal64::MutablePtr col_dcm;
    ColumnArray::MutablePtr col_arr;
    ColumnMap::MutablePtr col_map;

    void printColumn(const IColumn& column) {
        size_t column_size = column.size();
        std::cout << column.get_name() << ": " << std::endl;
        for (size_t i = 0; i < column_size; ++i) {
            std::cout << column.get_data_at(i) << ' ';
        }
        std::cout << std::endl;
    }
    // column size changed calculation:
    //  size, reserve, resize, empty, byte_size, allocated_bytes, clone_resized, get_shrinked_column
    //  cut(LIMIT operation), shrink
    void sizeAssert(MutableColumnPtr col, size_t expect_size) {
        EXPECT_EQ(col->size(), expect_size);
    }

    // empty just use size() == 0 to impl as default behavior
    void emptyAssert(MutableColumnPtr col) { EXPECT_EQ(col->size(), 0); }

    // reserve, resize, byte_size, allocated_bytes, clone_resized, get_shrinked_column
    void reserveAssert(MutableColumnPtr col, size_t expect_size) {
        col->reserve(expect_size);
        EXPECT_EQ(col->allocated_bytes(), expect_size);
    }

    //  cut(LIMIT operation) will cut the column with the given from and to, and return the new column
    //  notice return column is clone from origin column
    void cutAssert(MutableColumnPtr col, size_t from, size_t to, size_t expect_size) {
        auto ori = col->size();
        auto ptr = col->cut(from, to);
        EXPECT_EQ(ptr->size(), expect_size);
        EXPECT_EQ(col->size(), ori);
    }

    // shrink is cut/append the column with the given size, which called from Block::set_num_rows
    // and some Operator may call this set_num_rows to make rows satisfied, like limit operation
    // but different from cut behavior which
    // return column is mutate from origin column
    void shrinkAssert(MutableColumnPtr col, size_t shrink_size) {
        auto ptr = col->shrink(shrink_size);
        EXPECT_EQ(ptr->size(), shrink_size);
        EXPECT_EQ(col->size(), shrink_size);
    }

    // resize has fixed-column implementation and variable-column implementation
    // like string column, the resize will resize the offsets column but not the data column (because it doesn't matter the size of data column, all operation for string column is based on the offsets column)
    // like vector column, the resize will resize the data column
    // like array column, the resize will resize the offsets column and the data column (which in creator we have check staff for the size of data column is the same as the size of offsets column)
    void resizeAssert(MutableColumnPtr col, size_t expect_size) {
        col->resize(expect_size);
        EXPECT_EQ(col->size(), expect_size);
    }

    // replicate is clone with new column from the origin column, always from ColumnConst to expand the column
    void replicateAssert(MutableColumnPtr col, IColumn::Offsets& offsets) {
        auto new_col = col->replicate(offsets);
        EXPECT_EQ(new_col->size(), offsets.back());
    }

    // byte size is just appriximate size of the column
    //  as fixed column type, like column_vector, the byte size is sizeof(columnType) * size()
    //  as variable column type, like column_string, the byte size is sum of chars size() and offsets size * sizeof(offsetType)
    void byteSizeAssert(MutableColumnPtr col, size_t expect_size) {
        EXPECT_EQ(col->byte_size(), expect_size);
    }

    // allocated bytes is the real size of the column
    void allocatedBytesAssert(MutableColumnPtr col, size_t expect_size) {
        EXPECT_EQ(col->allocated_bytes(), expect_size);
    }

    // clone_resized will clone the column and cut/append to the new column with the size of the original column
    void cloneResizedAssert(MutableColumnPtr col, size_t expect_size) {
        auto new_col = col->clone_resized(expect_size);
        EXPECT_EQ(new_col->size(), expect_size);
    }

    // get_shrinked_column should only happened in char-type column or nested char-type column
    //  just shrink the end zeros for char-type column which happened in segmentIterator
    //    eg. column_desc: char(6), insert into char(3), the char(3) will padding the 3 zeros at the end for writing to disk.
    //       but we select should just print the char(3) without the padding zeros
    //  limit and topN operation will trigger this function call
    void getShrinkedColumnAssert(MutableColumnPtr col, size_t spcific_size_defined) {
        EXPECT_TRUE(col->could_shrinked_column());
        auto new_col = col->get_shrinked_column();
        for (size_t i = 0; i < new_col->size(); i++) {
            EXPECT_EQ(col->get_data_at(i).size, spcific_size_defined);
        }
    }

    //serialize and deserialize which usually used in AGG function:
    //  serialize_value_into_arena, deserialize_and_insert_from_arena (called by AggregateFunctionDistinctMultipleGenericData, group_array_intersect, nested-types serder like: DataTypeArraySerDe::write_one_cell_to_jsonb)
    void ser_deserialize_with_arena_impl(MutableColumns& columns, const DataTypes& data_types) {
        /// check serialization is reversible.
        Arena arena;
        MutableColumns argument_columns(data_types.size());
        const char* pos = nullptr;
        StringRef key(pos, 0);
        {
            // serialize
            for (size_t i = 0; i < columns.size(); ++i) {
                auto cur_ref = columns[i]->serialize_value_into_arena(0, arena, pos);
                key.data = cur_ref.data - key.size;
                key.size += cur_ref.size;
            }
        }

        {
            // deserialize
            for (size_t i = 0; i < data_types.size(); ++i) {
                argument_columns[i] = data_types[i]->create_column();
            }
            const char* begin = key.data;
            for (auto& column : argument_columns) {
                begin = column->deserialize_and_insert_from_arena(begin);
            }
        }
        {
            // check column data equal
            for (size_t i = 0; i < columns.size(); ++i) {
                EXPECT_EQ(columns[i]->size(), argument_columns[i]->size());
                for (size_t j = 0; j < columns[i]->size(); ++j) {
                    EXPECT_EQ(columns[i]->get_data_at(j), argument_columns[i]->get_data_at(j));
                }
            }
        }
    }

    //  serialize_vec, deserialize_vec (called by MethodSerialized.init_serialized_keys), here are some scenarios:
    //    1/ AggState: groupby key column which be serialized to hash-table key, eg.AggLocalState::_emplace_into_hash_table
    //    2/ JoinState: hash join key column which be serialized to hash-table key, or probe column which be serialized to hash-table key, eg.ProcessHashTableBuild, ProcessHashTableProbe<JoinOpType>::probe_side_output_column
    //  serialize_vec_with_null_map, deserialize_vec_with_null_map which only called by ColumnNullable serialize_vec and deserialize_vec, and derived by other columns
    //  get_max_row_byte_size used in MethodSerialized which calculating the memory size for vectorized serialization of aggregation keys.
    void ser_deser_vec(MutableColumns& columns) {
        // step1. make input_keys with given rows for a block
        size_t rows = columns[0]->size();
        std::vector<StringRef> input_keys;
        input_keys.resize(rows);
        MutableColumns check_columns(4);
        int c = 0;
        for (auto& column : columns) {
            check_columns[c] = column->clone_empty();
            ++c;
        }
        // serialize the keys into arena
        {
            // step2. calculate the needed memory size for vectorized serialization of aggregation keys
            size_t max_one_row_byte_size = 0;
            for (const auto& column : columns) {
                max_one_row_byte_size += column->get_max_row_byte_size();
            }
            size_t memory_size = max_one_row_byte_size * rows;
            Arena arena(memory_size);
            auto* serialized_key_buffer = reinterpret_cast<uint8_t*>(arena.alloc(memory_size));

            // step3. serialize the keys into arena
            for (size_t i = 0; i < rows; ++i) {
                input_keys[i].data =
                        reinterpret_cast<char*>(serialized_key_buffer + i * max_one_row_byte_size);
                input_keys[i].size = 0;
            }
            for (const auto& column : columns) {
                column->serialize_vec(input_keys, rows, max_one_row_byte_size);
            }
        }
        // deserialize the keys from arena into columns
        {
            // step4. deserialize the keys from arena into columns
            for (auto& column : check_columns) {
                column->deserialize_vec(input_keys, rows);
            }
        }
        // check the deserialized columns
        {
            for (size_t i = 0; i < columns.size(); ++i) {
                EXPECT_EQ(columns[i]->size(), check_columns[i]->size());
                for (size_t j = 0; j < columns[i]->size(); ++j) {
                    EXPECT_EQ(columns[i]->get_data_at(j), check_columns[i]->get_data_at(j));
                }
            }
        }
    }

    PaddedPODArray<UInt8> create_filter(std::vector<uint8_t> data) {
        PaddedPODArray<UInt8> filter;
        filter.insert(filter.end(), data.begin(), data.end());
        return filter;
    }

    // filter calculation:
    //  filter (called in Block::filter_block_internal to filter data with filter filled with 0or1 array, like: [0,1,0,1])
    //   used in join to filter next block by row_ids, and filter column by row_ids in first read which called in SegmentIterator
    void filterAssert(MutableColumnPtr col, std::vector<uint8_t> filter, size_t expect_size) {
        EXPECT_EQ(col->size(), filter.size());
        auto filted_col = col->filter(create_filter(filter), expect_size);
        EXPECT_EQ(filted_col, expect_size);
        int iter = 0;
        for (size_t i = 0; i < filter.size(); i++) {
            if (filter[i]) {
                EXPECT_EQ(col->get_data_at(i), filted_col->get_data_at(iter));
                iter++;
            }
        }
    }

    //  filter_by_selector (called SegmentIterator::copy_column_data_by_selector,
    //  now just used in filter column, according to the selector to
    //  select size of row_ids for column by given column, which only used for predict_column and column_dictionary, column_nullable sometimes in Schema::get_predicate_column_ptr() also will return)
    void filterBySelectorAssert(vectorized::IColumn::MutablePtr col, std::vector<uint16_t> selector,
                                MutableColumnPtr sel_col, MutableColumnPtr should_sel_col,
                                size_t expect_size) {
        // only used in column_nullable and predict_column, column_dictionary
        EXPECT_TRUE(col->is_nullable() || col->is_column_dictionary() || col->is_predict_column());
        col->insert_from(*sel_col, sel_col->size());
        Status st = col->filter_by_selector(selector.data(), expect_size, should_sel_col);
        EXPECT_EQ(st, Status::OK());
        EXPECT_EQ(should_sel_col->size(), expect_size);
        for (size_t i = 0; i < expect_size; i++) {
            std::cout << "i: " << i << " " << col->get_data_at(i) << " "
                      << should_sel_col->get_data_at(i) << std::endl;
        }
    }

    void assertPermutationsWithLimit(const IColumn::Permutation& lhs,
                                     const IColumn::Permutation& rhs, size_t limit) {
        if (limit == 0) {
            limit = lhs.size();
        }

        for (size_t i = 0; i < limit; ++i) {
            ASSERT_EQ(lhs[i], rhs[i]);
        }
    }

    // this function is common function to produce column, according to ColumnValueGetter
    // this range_size can be set, and it is helpfully make common column data which can be inserted into columns.
    template <typename ColumnValueGetter>
    void generateRanges(std::vector<std::vector<Field>>& ranges, size_t range_size,
                        ColumnValueGetter getter) {
        for (auto& range : ranges) {
            range.clear();
        }

        size_t ranges_size = ranges.size();

        for (size_t range_index = 0; range_index < ranges_size; ++range_index) {
            for (size_t index_in_range = 0; index_in_range < range_size; ++index_in_range) {
                auto value = getter(range_index, index_in_range);
                ranges[range_index].emplace_back(value);
            }
        }
    }

    void insertRangesIntoColumn(std::vector<std::vector<Field>>& ranges,
                                IColumn::Permutation& ranges_permutations,
                                vectorized::IColumn& column) {
        for (const auto& range_permutation : ranges_permutations) {
            auto& range = ranges[range_permutation];

            for (auto& value : range) {
                column.insert(value);
            }
        }
    }

    // this function helps to check sort permutation behavior for column
    void stableGetColumnPermutation(const IColumn& column, bool ascending, size_t limit,
                                    int nan_direction_hint, IColumn::Permutation& out_permutation) {
        (void)(limit);

        size_t size = column.size();
        out_permutation.resize(size);
        std::iota(out_permutation.begin(), out_permutation.end(),
                  IColumn::Permutation::value_type(0));

        std::stable_sort(out_permutation.begin(), out_permutation.end(),
                         [&](size_t lhs, size_t rhs) {
                             int res = column.compare_at(lhs, rhs, column, nan_direction_hint);
                             // to check element in column is sorted or not
                             if (ascending)
                                 return res < 0;
                             else
                                 return res > 0;
                         });
    }

    // sort calculation: (which used in sort_block )
    //   get_permutation
    // this function helps check permutation result with sort & limit
    //  by given a ColumnCreateFunc and ColumnValueGetter which how to generate a column value
    template <typename ColumnCreateFunc, typename ColumnValueGetter>
    void assertColumnPermutations(vectorized::IColumn& column,
                                  ColumnValueGetter columnValueGetter) {
        static constexpr size_t ranges_size = 3;
        static const std::vector<size_t> range_sizes = {1, 5, 50, 500};

        std::vector<std::vector<Field>> ranges(ranges_size);
        std::vector<size_t> ranges_permutations(ranges_size);
        std::iota(ranges_permutations.begin(), ranges_permutations.end(),
                  IColumn::Permutation::value_type(0));

        IColumn::Permutation actual_permutation;
        IColumn::Permutation expected_permutation;

        for (const auto& range_size : range_sizes) {
            // step1. generate range field data for column
            generateRanges(ranges, range_size, columnValueGetter);
            std::sort(ranges_permutations.begin(), ranges_permutations.end());
            IColumn::Permutation permutation(ranges_permutations.size());
            for (size_t i = 0; i < ranges_permutations.size(); ++i) {
                permutation[i] = ranges_permutations[i];
            }

            while (true) {
                // step2. insert range field data into column
                insertRangesIntoColumn(ranges, permutation, column);
                static constexpr size_t limit_parts = 4;

                size_t column_size = column.size();
                size_t column_limit_part = (column_size / limit_parts) + 1;

                for (size_t limit = 0; limit < column_size; limit += column_limit_part) {
                    assertColumnPermutation(column, true, limit, -1, actual_permutation,
                                            expected_permutation);
                    assertColumnPermutation(column, true, limit, 1, actual_permutation,
                                            expected_permutation);

                    assertColumnPermutation(column, false, limit, -1, actual_permutation,
                                            expected_permutation);
                    assertColumnPermutation(column, false, limit, 1, actual_permutation,
                                            expected_permutation);
                }

                assertColumnPermutation(column, true, 0, -1, actual_permutation,
                                        expected_permutation);
                assertColumnPermutation(column, true, 0, 1, actual_permutation,
                                        expected_permutation);

                assertColumnPermutation(column, false, 0, -1, actual_permutation,
                                        expected_permutation);
                assertColumnPermutation(column, false, 0, 1, actual_permutation,
                                        expected_permutation);

                if (!std::next_permutation(ranges_permutations.begin(), ranges_permutations.end()))
                    break;
            }
        }
    }

    // sort calculation: (which used in sort_block )
    //    get_permutation means sort data in Column as sort order
    //    limit should be set to limit the sort result
    //    nan_direction_hint deal with null|NaN value
    void assertColumnPermutation(const IColumn& column, bool ascending, size_t limit,
                                 int nan_direction_hint, IColumn::Permutation& actual_permutation,
                                 IColumn::Permutation& expected_permutation) {
        // step1. get expect permutation as stabled sort
        stableGetColumnPermutation(column, ascending, limit, nan_direction_hint,
                                   expected_permutation);

        // step2. get permutation by column
        column.get_permutation(ascending, limit, nan_direction_hint, expected_permutation);

        if (limit == 0) {
            limit = actual_permutation.size();
        }

        // step3. check the permutation result
        assertPermutationsWithLimit(actual_permutation, expected_permutation, limit);
    }

    //  permute()
    //   1/ Key topN set read_orderby_key_reverse = true; SegmentIterator::next_batch will permute the column by the given permutation(which reverse the rows of current segment)
    //  should check rows with the given permutation
    void assertPermute(MutableColumns& cols, IColumn::Permutation& permutation, size_t num_rows) {
        std::vector<ColumnPtr> res_permuted;
        for (auto& col : cols) {
            res_permuted.emplace_back(col->permute(permutation, num_rows));
        }
        // check the permutation result for rowsize
        size_t res_rows = res_permuted[0]->size();
        for (auto& col : res_permuted) {
            EXPECT_EQ(col->size(), res_rows);
            printColumn(*col);
        }
    }

    // sort_column
    //  1/ sort_column (called in sort_block to sort the column by given permutation)
    void assertSortColumn(IColumn& column, IColumn::Permutation& permutation, size_t num_rows) {
        // just make a simple sort function to sort the column
        std::vector<ColumnPtr> res_sorted;
        // SortColumnDescription:
        //    Column number;
        //    int direction;           /// 1 - ascending, -1 - descending.
        //    int nulls_direction;     /// 1 - NULLs and NaNs are greater, -1 - less.
        std::vector<SortColumnDescription> sort_desc;
        SortColumnDescription _column_with_descend_null_greater = {1, -1, 1};
        SortColumnDescription _column_with_descend_null_less = {1, -1, -1};
        SortColumnDescription _column_with_ascend_null_greater = {1, 1, 1};
        SortColumnDescription _column_with_ascend_null_less = {1, 1, -1};
        sort_desc.emplace_back(_column_with_descend_null_greater);
        sort_desc.emplace_back(_column_with_descend_null_less);
        sort_desc.emplace_back(_column_with_ascend_null_greater);
        sort_desc.emplace_back(_column_with_ascend_null_less);
        EqualFlags flags(num_rows, 1);
        EqualRange range {0, num_rows};
        for (auto& column_with_sort_desc : sort_desc) {
            ColumnSorter sorter({&column, column_with_sort_desc}, 0);
            column.sort_column(&sorter, flags, permutation, range, num_rows);
        };
        // check the sort result for flags and ranges
        for (size_t i = 0; i < num_rows; i++) {
            std::cout << "i: " << i << " " << flags[i] << std::endl;
        }
    }
};

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
    MutableColumns columns(4);
    columns[0] = col_str->clone();
    columns[1] = col_int->clone();
    columns[2] = col_arr->clone();
    columns[3] = col_map->clone();
    ser_deser_vec(columns);
}

TEST_F(CommonColumnTest, FilterBySelector) {
    // make a PredictColumn
    auto ptr = Schema::get_predicate_column_ptr(FieldType::OLAP_FIELD_TYPE_INT, false,
                                                ReaderType::READER_QUERY);
    // 1. 空选择器
    std::vector<uint16_t> selector_empty = {};
    filterBySelectorAssert(ptr->get_ptr(), selector_empty, col_int->get_ptr(), col_dcm->get_ptr(),
                           0);

    // 2. 全选
    std::vector<uint16_t> selector_all = {1, 1, 1, 1, 1};
    filterBySelectorAssert(ptr->get_ptr(), selector_empty, col_int->get_ptr(), col_dcm->get_ptr(),
                           0);

    // 3. 全不选
    std::vector<uint16_t> select_none = {0, 0, 0, 0, 0};
    filterBySelectorAssert(ptr->get_ptr(), select_none, col_int->get_ptr(), col_dcm->get_ptr(), 0);

    // 4. 部分选择
    std::vector<uint16_t> selector_partial = {1, 0, 1, 0, 1};
    filterBySelectorAssert(ptr->get_ptr(), selector_partial, col_int->get_ptr(), col_dcm->get_ptr(),
                           0);

    // 5. 选择器长度不匹配
    std::vector<uint16_t> selector_invalid = {1, 1, 1, 1};
    EXPECT_ANY_THROW(ptr->filter_by_selector(selector_invalid.data(), 10, col_dcm));
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

    static constexpr size_t ranges_size = 3;
    static const std::vector<size_t> range_sizes = {1, 5, 50, 500};

    std::vector<std::vector<Field>> ranges(ranges_size);
    generateRanges(ranges, ranges_size, columnStringGetter);
    insertRangesIntoColumn(ranges, permutation, *col_s);
    generateRanges(ranges, ranges_size, columnInt64ValueGetter);
    insertRangesIntoColumn(ranges, permutation, *col_i);
    generateRanges(ranges, ranges_size, columnFloat64ValueGetter);
    insertRangesIntoColumn(ranges, permutation, *col_f);
    generateRanges(ranges, ranges_size, columnDecimal64ValueGetter);
    insertRangesIntoColumn(ranges, permutation, *col_d);

    assertPermute(columns, permutation, num_rows);
}

TEST_F(CommonColumnTest, SortColumnDescription) {
    ColumnString::MutablePtr col_s = ColumnString::create();
    static constexpr size_t ranges_size = 3;
    static const std::vector<size_t> range_sizes = {1, 5, 50, 500};

    auto columnStringGetter = [](size_t range_index, size_t index_in_range) -> Field {
        return Field(std::to_string(range_index * index_in_range));
    };
    std::vector<std::vector<Field>> ranges(ranges_size);
    IColumn::Permutation permutation;
    size_t num_rows = 10;
    for (size_t i = 0; i < num_rows; ++i) permutation.emplace_back(num_rows - 1 - i);
    generateRanges(ranges, ranges_size, columnStringGetter);
    insertRangesIntoColumn(ranges, permutation, *col_s);

    assertSortColumn(*col_s, permutation, num_rows);
}

} // namespace doris::vectorized