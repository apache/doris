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

#include "vec/columns/column.h"
#include "vec/columns/common_column_test.h"
#include "vec/core/types.h"
#include "vec/function/function_test_util.h"

// this test is gonna to make a template ColumnTest
// for example column_ip should test these functions

namespace doris::vectorized {
class ColumnArrayTest : public CommonColumnTest {
protected:
    void SetUp() override {
        // insert from data csv and assert insert result
        std::string data_file_dir = "regression-test/data/nereids_function_p0/array/";
        MutableColumns array_cols;
        // we need to load data from csv file into column_array list
        // step1. create data type for array nested type (const and nullable)
        // array<tinyint>
        BaseInputTypeSet array_tinyint = {TypeIndex::Array, TypeIndex::Int8};
        // array<smallint>
        BaseInputTypeSet array_smallint = {TypeIndex::Array, TypeIndex::Int16};
        // array<int>
        BaseInputTypeSet array_int = {TypeIndex::Array, TypeIndex::Int32};
        // array<bigint>
        BaseInputTypeSet array_bigint = {TypeIndex::Array, TypeIndex::Int64};
        // array<largeint>
        BaseInputTypeSet array_largeint = {TypeIndex::Array, TypeIndex::Int128};
        // array<float>
        BaseInputTypeSet array_float = {TypeIndex::Array, TypeIndex::Float32};
        // array<double>
        BaseInputTypeSet array_double = {TypeIndex::Array, TypeIndex::Float64};
        // array<ipv4>
        BaseInputTypeSet array_ipv4 = {TypeIndex::Array, TypeIndex::IPv4};
        // array<ipv6>
        BaseInputTypeSet array_ipv6 = {TypeIndex::Array, TypeIndex::IPv6};
        // array<date>
        BaseInputTypeSet array_date = {TypeIndex::Array, TypeIndex::Date};
        // array<datetime>
        BaseInputTypeSet array_datetime = {TypeIndex::Array, TypeIndex::DateTime};
        // array<datev2>
        BaseInputTypeSet array_datev2 = {TypeIndex::Array, TypeIndex::DateV2};
        // array<datetimev2>
        BaseInputTypeSet array_datetimev2 = {TypeIndex::Array, TypeIndex::DateTimeV2};
        // array<varchar>
        BaseInputTypeSet array_varchar = {TypeIndex::Array, TypeIndex::String};
        // array<decimal32(9, 5)> UT
        BaseInputTypeSet array_decimal = {TypeIndex::Array, TypeIndex::Decimal32};
        // array<decimal64(18, 9)> UT
        BaseInputTypeSet array_decimal64 = {TypeIndex::Array, TypeIndex::Decimal64};
        // array<decimal128(38, 20)> UT
        BaseInputTypeSet array_decimal128 = {TypeIndex::Array, TypeIndex::Decimal128V3};
        // array<decimal256(76, 40)> UT
        BaseInputTypeSet array_decimal256 = {TypeIndex::Array, TypeIndex::Decimal256};
        std::vector<BaseInputTypeSet> array_typeIndex = {
                array_tinyint,   array_smallint,   array_int,        array_bigint,  array_largeint,
                array_float,     array_double,     array_ipv4,       array_ipv6,    array_date,
                array_datetime,  array_datev2,     array_datetimev2, array_varchar, array_decimal,
                array_decimal64, array_decimal128, array_decimal256};

        vector<ut_type::UTDataTypeDescs> descs;
        descs.reserve(array_typeIndex.size());
        for (int i = 0; i < array_typeIndex.size(); i++) {
            descs.push_back(ut_type::UTDataTypeDescs());
            InputTypeSet input_types {};
            input_types.push_back(array_typeIndex[i][0]);
            input_types.push_back(Nullable {static_cast<TypeIndex>(array_typeIndex[i][1])});
            EXPECT_EQ(input_types[1].type(), &typeid(Nullable)) << "nested type is not nullable";
            EXPECT_TRUE(parse_ut_data_type(input_types, descs[i]));
        }

        // create column_array for each data type
        vector<string> data_files = {data_file_dir + "test_array_tinyint.csv",
                                     data_file_dir + "test_array_smallint.csv",
                                     data_file_dir + "test_array_int.csv",
                                     data_file_dir + "test_array_bigint.csv",
                                     data_file_dir + "test_array_largeint.csv",
                                     data_file_dir + "test_array_float.csv",
                                     data_file_dir + "test_array_double.csv",
                                     data_file_dir + "test_array_ipv4.csv",
                                     data_file_dir + "test_array_ipv6.csv",
                                     data_file_dir + "test_array_date.csv",
                                     data_file_dir + "test_array_datetime.csv",
                                     data_file_dir + "test_array_date.csv",
                                     data_file_dir + "test_array_datetimev2(6).csv",
                                     data_file_dir + "test_array_varchar(65535).csv",
                                     data_file_dir + "test_array_decimalv3(7,4).csv",
                                     data_file_dir + "test_array_decimalv3(16,10).csv",
                                     data_file_dir + "test_array_decimalv3(38,30).csv",
                                     data_file_dir + "test_array_decimalv3(76,56).csv"};

        // step2. according to the datatype to make column_array
        //          && load data from csv file into column_array
        EXPECT_EQ(descs.size(), data_files.size());
        for (int i = 0; i < array_typeIndex.size(); i++) {
            auto& desc = descs[i];
            auto& data_file = data_files[i];
            // first is array type
            auto& type = desc[0].data_type;
            std::cout << "type: " << type->get_name() << " with file: " << data_file << std::endl;
            MutableColumns columns;
            columns.push_back(type->create_column());
            auto serde = type->get_serde(1);
            load_data_from_csv({serde}, columns, data_file, ';');
            array_columns.push_back(std::move(columns[0]));
            array_types.push_back(type);
            serdes.push_back(serde);
        }
        // step3. show array column data
        for (int i = 0; i < array_columns.size(); i++) {
            //            auto& column = array_columns[i];
            //            printColumn(*column, *descs[i][0].data_type);
        }
    }

    MutableColumns array_columns; // column_array list
    DataTypes array_types;
    DataTypeSerDeSPtrs serdes;
};

//////////////////////// basic function from column.h ////////////////////////
TEST_F(ColumnArrayTest, InsertRangeFromTest) {
    assert_insert_range_from_callback(array_columns, serdes);
}

TEST_F(ColumnArrayTest, InsertManyFromTest) {
    assert_insert_many_from_callback(array_columns, serdes);
}

TEST_F(ColumnArrayTest, InsertIndicesFromTest) {
    assert_insert_indices_from_callback(array_columns, serdes);
}

TEST_F(ColumnArrayTest, InsertDefaultTest) {
    assert_insert_default_callback(array_columns, serdes);
}

TEST_F(ColumnArrayTest, InsertManyDefaultsTest) {
    assert_insert_many_defaults_callback(array_columns, serdes);
}

TEST_F(ColumnArrayTest, GetDataAtTest) {
    assert_get_data_at_callback(array_columns, serdes);
}

TEST_F(ColumnArrayTest, FieldTest) {
    assert_field_callback(array_columns, serdes);
}

TEST_F(ColumnArrayTest, GetRawDataTest) {
    EXPECT_ANY_THROW({ array_columns[0]->get_raw_data(); });
}

TEST_F(ColumnArrayTest, GetBoolTest) {
    EXPECT_ANY_THROW({ array_columns[0]->get_bool(0); });
}

TEST_F(ColumnArrayTest, GetIntTest) {
    EXPECT_ANY_THROW({ array_columns[0]->get_int(0); });
}

TEST_F(ColumnArrayTest, SerDeVecTest) {
    ser_deser_vec(array_columns, array_types);
}

TEST_F(ColumnArrayTest, serDeserializeWithArenaImpl) {
    ser_deserialize_with_arena_impl(array_columns, array_types);
}

TEST_F(ColumnArrayTest, SizeTest) {
    assert_size_callback(array_columns, serdes);
}

TEST_F(ColumnArrayTest, ByteSizeTest) {
    assert_byte_size_callback(array_columns, serdes);
}

TEST_F(ColumnArrayTest, AllocateBytesTest) {
    assert_allocated_bytes_callback(array_columns, serdes);
}

TEST_F(ColumnArrayTest, PopbackTest) {
    assert_pop_back_callback(array_columns, serdes);
}

TEST_F(ColumnArrayTest, CloneTest) {
    assert_clone_resized_callback(array_columns, serdes);
}

TEST_F(ColumnArrayTest, CutTest) {
    assert_cut_callback(array_columns, serdes);
}

TEST_F(ColumnArrayTest, ResizeTest) {
    assert_resize_callback(array_columns, serdes);
}

TEST_F(ColumnArrayTest, ReserveTest) {
    assert_reserve_callback(array_columns, serdes);
}

TEST_F(ColumnArrayTest, ReplicateTest) {
    assert_replicate_callback(array_columns, serdes);
}

TEST_F(ColumnArrayTest, ReplaceColumnTest) {
    assert_replace_column_data_callback(array_columns, serdes);
    assert_replace_column_null_data_callback(array_columns, serdes);
}

TEST_F(ColumnArrayTest, AppendDataBySelectorTest) {
    assert_append_data_by_selector_callback(array_columns, serdes);
}

TEST_F(ColumnArrayTest, PermutationAndSortTest) {
    for (int i = 0; i < array_columns.size(); i++) {
        auto& column = array_columns[i];
        auto& type = array_types[i];
        auto column_type = type->get_name();
        std::cout << "column_type: " << column_type << std::endl;
        // permutation
        assert_column_permutations(column->assume_mutable_ref(), type);
    }
}

TEST_F(ColumnArrayTest, FilterTest) {
    assert_filter_callback(array_columns, serdes);
}

// HASH Interfaces
TEST_F(ColumnArrayTest, HashTest) {
    // XXHash
    assert_update_hashes_with_value_callback(array_columns, serdes);
    // XXhash with null_data

    // CrcHash
    std::vector<PrimitiveType> pts(array_columns.size(), PrimitiveType::TYPE_ARRAY);
    assert_update_crc_hashes_callback(array_columns, serdes, pts);
    // CrcHash with null_data
};

//////////////////////// special function from column_array.h ////////////////////////
TEST_F(ColumnArrayTest, CreateArrayTest) {
    // test create_array : nested_column && offsets_column should not be const, and convert_to_full_column_if_const should not impl in array
    // in some situation,
    //  like join_probe_operator.cpp::_build_output_block,
    //  we call column.convert_to_full_column_if_const,
    //  then we may call clear_column_data() to clear the column (eg. in HashJoinProbeOperatorX::pull() which call local_state._probe_block.clear_column_data after filter_data_and_build_output())
    //  in clear_column_data() if use_count() == 1, we will call column->clear() to clear the column data
    //
    //  however in array impl for convert_to_full_column_if_const: ``` ColumnArray::create(data->convert_to_full_column_if_const(), offsets);```
    //  may make the nested_column use_count() more than 1 which means it is shared with other block, but return ColumnArray is new which use_count() is 1,
    //  then in clear_column_data() if we will call array_column->use_count() == 1 will be true to clear the column with nested_column, and shared nested_column block will meet undefined behavior cause maybe core
    //
    //  so actually according to the semantics of the function, it should not impl in array,
    //  but we should make sure in creation of array, the nested_column && offsets_column should not be const
    for (int i = 0; i < array_columns.size(); i++) {
        auto column = check_and_get_column<ColumnArray>(
                remove_nullable(array_columns[i]->assume_mutable()));
        auto& type = array_types[i];
        auto column_size = column->size();
        auto column_type = type->get_name();
        std::cout << "column_type: " << column_type << std::endl;
        // test create_array
        auto last_offset = column->get_offsets().back();
        EXPECT_ANY_THROW(
                { auto const_col = ColumnConst::create(column->get_data_ptr(), last_offset); });
        auto tmp_data_col = column->get_data_ptr()->clone_resized(1);
        auto const_col = ColumnConst::create(tmp_data_col->assume_mutable(), last_offset);
        EXPECT_ANY_THROW({
            // const_col is not empty
            auto new_array_column = ColumnArray::create(const_col->assume_mutable());
        });
        auto new_array_column =
                ColumnArray::create(const_col->assume_mutable(), column->get_offsets_ptr());
        EXPECT_EQ(new_array_column->size(), column_size)
                << "array_column size is not equal to column size";
        EXPECT_EQ(new_array_column->get_data_ptr()->size(), column->get_data_ptr()->size());
        EXPECT_EQ(new_array_column->get_offsets_ptr()->size(), column->get_offsets_ptr()->size());
    }
}

TEST_F(ColumnArrayTest, MetaInfoTest) {
    // test is_variable_length which should all be true
    for (int i = 0; i < array_columns.size(); i++) {
        auto& column = array_columns[i];
        auto& type = array_types[i];
        auto column_type = type->get_name();
        std::cout << "column_type: " << column_type << std::endl;
        EXPECT_TRUE(column->is_variable_length()) << "column is not variable length";
    }
}

TEST_F(ColumnArrayTest, ConvertIfOverflowAndInsertTest) {
    // test nested string in array which like ColumnArray<ColumnString> only use in join
    // test convert_column_if_overflow && insert_range_from_ignore_overflow
    for (int i = 0; i < array_columns.size(); i++) {
        auto& column = array_columns[i];
        DataTypeArray type = array_types[i];
        if (!is_string(type.get_nested_type())) {
            // check ptr is itself
            auto ptr = column->convert_column_if_overflow();
            EXPECT_EQ(ptr.get(), column.get());
            auto* array_col = check_and_get_column<ColumnArray>(column);
            auto nested_col = array_col->get_data_ptr();
            auto* array_col1 = check_and_get_column<ColumnArray>(ptr);
            auto nested_col1 = array_col1->get_data_ptr();
            EXPECT_EQ(nested_col.get(), nested_col1.get());
        } else {
            auto ptr = column->convert_column_if_overflow();
        }
    }
}

TEST_F(ColumnArrayTest, GetNumberOfDimensionsTest) {
    // test dimension of array
    for (int i = 0; i < array_columns.size(); i++) {
        auto column = check_and_get_column<ColumnArray>(
                remove_nullable(array_columns[i]->assume_mutable()));
        auto check_type = remove_nullable(array_types[i]);
        auto dimension = 0;
        while (is_array(check_type)) {
            auto nested_type = reinterpret_cast<const vectorized::DataTypeArray&>(*check_type)
                                       .get_nested_type();
            dimension++;
            check_type = nested_type;
        }
        EXPECT_EQ(column->get_number_of_dimensions(), dimension)
                << "column dimension is not equal to check_type dimension";
    }
}

TEST_F(ColumnArrayTest, MaxArraySizeAsFieldTest) {
    // test array max_array_size_as_field which is set to 100w
    //  in operator[] and get()
    for (int i = 0; i < array_columns.size(); i++) {
        auto column = check_and_get_column<ColumnArray>(
                remove_nullable(array_columns[i]->assume_mutable()));
        auto check_type = remove_nullable(array_types[i]);
        Field a;
        column->get(column->size() - 1, a);
        Array af = a.get<Array>();
        if (af.size() > 0) {
            Field ef = af[0];
            for (int j = 0; j < max_array_size_as_field; ++j) {
                af.push_back(ef);
            }
            std::cout << "array size: " << af.size() << std::endl;
            auto cloned = column->clone_resized(0);
            cloned->insert(af);
            std::cout << "cloned size: " << cloned->size() << std::endl;
            // get cloned offset size
            auto cloned_offset_size =
                    check_and_get_column<ColumnArray>(cloned)->get_offsets().back();
            std::cout << "cloned offset size: " << cloned_offset_size << std::endl;

            Field f;
            // test get
            EXPECT_ANY_THROW({ cloned->get(0, f); });
            // test operator[]
            EXPECT_ANY_THROW({ cloned->operator[](0); });
        }
    }
}

TEST_F(ColumnArrayTest, IsDefaultAtTest) {
    // default means meet empty array row in column_array, now just only used in ColumnObject.
    // test is_default_at
    for (int i = 0; i < array_columns.size(); i++) {
        auto column = check_and_get_column<ColumnArray>(
                remove_nullable(array_columns[i]->assume_mutable()));
        auto column_size = column->size();
        for (int j = 0; j < column_size; j++) {
            auto is_default = column->is_default_at(j);
            if (is_default) {
                // check field Array is empty
                Field f;
                column->get(j, f);
                auto array = f.get<Array>();
                EXPECT_EQ(array.size(), 0) << "array is not empty";
            }
        }
    }
}

TEST_F(ColumnArrayTest, HasEqualOffsetsTest) {
    // test has_equal_offsets which more likely used in function, eg: function_array_zip
    for (int i = 0; i < array_columns.size(); i++) {
        auto column = check_and_get_column<ColumnArray>(
                remove_nullable(array_columns[i]->assume_mutable()));
        auto cloned = array_columns[i]->clone_resized(array_columns[i]->size());
        auto cloned_arr =
                check_and_get_column<ColumnArray>(remove_nullable(cloned->assume_mutable()));
        // test expect true
        EXPECT_EQ(column->get_offsets().size(), cloned_arr->get_offsets().size());
        EXPECT_TRUE(column->has_equal_offsets(*cloned_arr));
        // cloned column size is not equal to column size
        cloned->pop_back(1);
        EXPECT_FALSE(column->has_equal_offsets(*cloned_arr));
        cloned->insert_default();
        EXPECT_FALSE(column->has_equal_offsets(*cloned_arr));
    }
}

} // namespace doris::vectorized