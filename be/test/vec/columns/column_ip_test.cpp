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
#include "vec/columns/column_array.h"
#include "vec/columns/columns_number.h"
#include "vec/columns/common_column_test.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_nullable.h"

// this test is gonna to make a template ColumnTest
// for example column_ip should test these functions

namespace doris::vectorized {
class ColumnIPTest : public CommonColumnTest {
protected:
    void SetUp() override {
        // insert from data csv and assert insert result
        column_ipv4 = ColumnIPv4::create();
        column_ipv6 = ColumnIPv6::create();

        // insert from data csv and assert insert result with
        // regression-test/data/nereids_function_p0/fn_test_ip_invalid.csv
        // regression-test/data/nereids_function_p0/fn_test_ip_normal.csv
        // regression-test/data/nereids_function_p0/fn_test_ip_nullable.csv
        // regression-test/data/nereids_function_p0/fn_test_ip_special.csv
        // regression-test/data/nereids_function_p0/fn_test_ip_special_no_null.csv
        data_files = {"regression-test/data/nereids_function_p0/fn_test_ip_invalid.csv",
                      "regression-test/data/nereids_function_p0/fn_test_ip_normal.csv",
                      "regression-test/data/nereids_function_p0/fn_test_ip_nullable.csv",
                      "regression-test/data/nereids_function_p0/fn_test_ip_special.csv",
                      "regression-test/data/nereids_function_p0/fn_test_ip_special_no_null.csv"};
        MutableColumns ip_cols;
        ip_cols.push_back(column_ipv4->get_ptr());
        ip_cols.push_back(column_ipv6->get_ptr());
        //        check_data(ip_cols, serde, ';', {1, 2},
        //                   "regression-test/data/nereids_function_p0/fn_test_ip_invalid.csv",
        //                   assert_insert_from_callback);
    }

    DataTypePtr dt_ipv4 =
            DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_IPV4, 0, 0);
    DataTypePtr dt_ipv6 =
            DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_IPV6, 0, 0);
    DataTypePtr dt_ipv4_nullable = std::make_shared<vectorized::DataTypeNullable>(dt_ipv4);
    DataTypePtr dt_ipv6_nullable = std::make_shared<vectorized::DataTypeNullable>(dt_ipv6);

    ColumnIPv4::MutablePtr column_ipv4;
    ColumnIPv6::MutablePtr column_ipv6;
    ColumnConst::MutablePtr column_ipv4_const;
    ColumnConst::MutablePtr column_ipv6_const;
    DataTypeSerDeSPtrs serde = {dt_ipv4->get_serde(), dt_ipv6->get_serde()};

    // common ip data
    std::vector<string> data_files;
};

TEST_F(ColumnIPTest, InsertRangeFromTest) {
    // insert from data csv and assert insert result
    MutableColumns ip_cols;
    ip_cols.push_back(column_ipv4->get_ptr());
    ip_cols.push_back(column_ipv6->get_ptr());
    check_data(ip_cols, serde, ';', {1, 2}, data_files[0], assert_insert_range_from_callback);
}

TEST_F(ColumnIPTest, InsertManyFromTest) {
    // insert from data csv and assert insert result
    MutableColumns ip_cols;
    ip_cols.push_back(column_ipv4->get_ptr());
    ip_cols.push_back(column_ipv6->get_ptr());
    check_data(ip_cols, serde, ';', {1, 2}, data_files[0], assert_insert_many_from_callback);
}

TEST_F(ColumnIPTest, InsertIndicesFromTest) {
    // insert from data csv and assert insert result
    MutableColumns ip_cols;
    ip_cols.push_back(column_ipv4->get_ptr());
    ip_cols.push_back(column_ipv6->get_ptr());
    check_data(ip_cols, serde, ';', {1, 2}, data_files[0], assert_insert_indices_from_callback);
}

TEST_F(ColumnIPTest, InsertDefaultTest) {
    // insert from data csv and assert insert result
    MutableColumns ip_cols;
    ip_cols.push_back(column_ipv4->get_ptr());
    ip_cols.push_back(column_ipv6->get_ptr());
    // ipv4 default value is '0.0.0.0' and ipv6 default value is '::'
    check_data(ip_cols, serde, ';', {1, 2}, data_files[0], assert_insert_default_callback);
}

TEST_F(ColumnIPTest, InsertManyDefaultsTest) {
    // insert from data csv and assert insert result
    MutableColumns ip_cols;
    ip_cols.push_back(column_ipv4->get_ptr());
    ip_cols.push_back(column_ipv6->get_ptr());
    check_data(ip_cols, serde, ';', {1, 2}, data_files[0], assert_insert_many_defaults_callback);
}

TEST_F(ColumnIPTest, GetDataAtTest) {
    // insert from data csv and assert insert result
    MutableColumns ip_cols;
    ip_cols.push_back(column_ipv4->get_ptr());
    ip_cols.push_back(column_ipv6->get_ptr());
    check_data(ip_cols, serde, ';', {1, 2}, data_files[0], assert_get_data_at_callback);
}

TEST_F(ColumnIPTest, FieldTest) {
    // insert from data csv and assert insert result
    MutableColumns ip_cols;
    ip_cols.push_back(column_ipv4->get_ptr());
    ip_cols.push_back(column_ipv6->get_ptr());
    check_data(ip_cols, serde, ';', {1, 2}, data_files[0], assert_field_callback);
}

TEST_F(ColumnIPTest, GetRawDataTest) {
    // insert from data csv and assert insert result
    MutableColumns ip_cols;
    ip_cols.push_back(column_ipv6->get_ptr());
    check_data(ip_cols, {serde[1]}, ';', {2}, data_files[0], assert_get_raw_data_callback<IPv6>);
}

TEST_F(ColumnIPTest, GetBoolTest) {
    // insert from data csv and assert insert result
    MutableColumns ip_cols;
    ip_cols.push_back(column_ipv4->get_ptr());
    ip_cols.push_back(column_ipv6->get_ptr());
    check_data(ip_cols, serde, ';', {1, 2}, data_files[0], assert_get_bool_callback);
}

TEST_F(ColumnIPTest, GetIntTest) {
    // insert from data csv and assert insert result
    MutableColumns ip_cols;
    ip_cols.push_back(column_ipv4->get_ptr());
    ip_cols.push_back(column_ipv6->get_ptr());
    check_data(ip_cols, serde, ';', {1, 2}, data_files[0], assert_get_int_callback);
}

TEST_F(ColumnIPTest, SerDeVecTest) {
    // insert from data csv and assert insert result
    MutableColumns ip_cols;
    ip_cols.push_back(column_ipv4->get_ptr());
    ip_cols.push_back(column_ipv6->get_ptr());
    load_data_from_csv(serde, ip_cols, data_files[0], ';', {1, 2});
    ser_deser_vec(ip_cols, {dt_ipv4, dt_ipv6});
}

TEST_F(ColumnIPTest, serDeserializeWithArenaImpl) {
    // insert from data csv and assert insert result
    MutableColumns ip_cols;
    ip_cols.push_back(column_ipv4->get_ptr());
    ip_cols.push_back(column_ipv6->get_ptr());

    load_data_from_csv(serde, ip_cols, data_files[0], ';', {1, 2});
    ser_deserialize_with_arena_impl(ip_cols, {dt_ipv4, dt_ipv6});
}

TEST_F(ColumnIPTest, SizeTest) {
    // insert from data csv and assert insert result
    MutableColumns ip_cols;
    ip_cols.push_back(column_ipv4->get_ptr());
    ip_cols.push_back(column_ipv6->get_ptr());
    check_data(ip_cols, serde, ';', {1, 2}, data_files[0], assert_size_callback);
}

TEST_F(ColumnIPTest, ByteSizeTest) {
    // insert from data csv and assert insert result
    MutableColumns ip_cols;
    ip_cols.push_back(column_ipv4->get_ptr());
    ip_cols.push_back(column_ipv6->get_ptr());

    check_data(ip_cols, serde, ';', {1, 2}, data_files[0], assert_byte_size_callback);
}

TEST_F(ColumnIPTest, AllocateBytesTest) {
    // insert from data csv and assert insert result
    MutableColumns ip_cols;
    ip_cols.push_back(column_ipv4->get_ptr());
    ip_cols.push_back(column_ipv6->get_ptr());

    check_data(ip_cols, serde, ';', {1, 2}, data_files[0], assert_allocated_bytes_callback);
}

TEST_F(ColumnIPTest, PopbackTest) {
    // insert from data csv and assert insert result
    MutableColumns ip_cols;
    ip_cols.push_back(column_ipv4->get_ptr());
    ip_cols.push_back(column_ipv6->get_ptr());

    check_data(ip_cols, serde, ';', {1, 2}, data_files[0], assert_pop_back_callback);
}

TEST_F(ColumnIPTest, CloneTest) {
    // we test the column with clone_resize, clone_empty for assert size and ptr
    // insert from data csv and assert insert result
    MutableColumns ip_cols;
    ip_cols.push_back(column_ipv4->get_ptr());
    ip_cols.push_back(column_ipv6->get_ptr());
    load_data_from_csv(serde, ip_cols, data_files[0], ';', {1, 2});
    assert_clone_empty(column_ipv4->assume_mutable_ref());
    assert_clone_empty(column_ipv6->assume_mutable_ref());
    check_data(ip_cols, serde, ';', {1, 2}, data_files[0], assert_clone_resized_callback);
}

TEST_F(ColumnIPTest, CutTest) {
    MutableColumns ip_cols;
    ip_cols.push_back(column_ipv4->get_ptr());
    ip_cols.push_back(column_ipv6->get_ptr());
    load_data_from_csv(serde, ip_cols, data_files[0], ';', {1, 2});
    check_data(ip_cols, serde, ';', {1, 2}, data_files[0], assert_cut_callback);
}

TEST_F(ColumnIPTest, ResizeTest) {
    // insert from data csv and assert insert result
    MutableColumns ip_cols;
    ip_cols.push_back(column_ipv4->get_ptr());
    ip_cols.push_back(column_ipv6->get_ptr());
    check_data(ip_cols, serde, ';', {1, 2}, data_files[0], assert_resize_callback);
}

TEST_F(ColumnIPTest, ReserveTest) {
    // insert from data csv and assert insert result
    MutableColumns ip_cols;
    ip_cols.push_back(column_ipv4->get_ptr());
    ip_cols.push_back(column_ipv6->get_ptr());
    check_data(ip_cols, serde, ';', {1, 2}, data_files[0], assert_reserve_callback);
}

TEST_F(ColumnIPTest, ReplicateTest) {
    // insert from data csv and assert insert result
    MutableColumns ip_cols;
    ip_cols.push_back(column_ipv4->get_ptr());
    ip_cols.push_back(column_ipv6->get_ptr());

    load_data_from_csv(serde, ip_cols, data_files[0], ';', {1, 2});
    check_data(ip_cols, serde, ';', {1, 2}, data_files[0], assert_replicate_callback);
}

TEST_F(ColumnIPTest, ReplaceColumnTest) {
    // insert from data csv and assert insert result
    MutableColumns ip_cols;
    ip_cols.push_back(column_ipv4->get_ptr());
    ip_cols.push_back(column_ipv6->get_ptr());

    load_data_from_csv(serde, ip_cols, data_files[0], ';', {1, 2});
    // replace_column_data
    check_data(ip_cols, serde, ';', {1, 2}, data_files[0], assert_replace_column_data_callback);
    // replace_nullable_column_data
    check_data(ip_cols, serde, ';', {1, 2}, data_files[0],
               assert_replace_column_null_data_callback);
}

TEST_F(ColumnIPTest, AppendDataBySelectorTest) {
    // insert from data csv and assert insert result
    MutableColumns ip_cols;
    ip_cols.push_back(column_ipv4->get_ptr());
    ip_cols.push_back(column_ipv6->get_ptr());
    check_data(ip_cols, serde, ';', {1, 2}, data_files[0], assert_append_data_by_selector_callback);
}

TEST_F(ColumnIPTest, PermutationAndSortTest) {
    // insert from data csv and assert insert result
    MutableColumns ip_cols;
    ip_cols.push_back(column_ipv4->get_ptr());
    ip_cols.push_back(column_ipv6->get_ptr());
    load_data_from_csv(serde, ip_cols, data_files[1], ';', {1, 2});
    assert_column_permutations(column_ipv4->assume_mutable_ref(), dt_ipv4);
    assert_column_permutations(column_ipv6->assume_mutable_ref(), dt_ipv6);
}

TEST_F(ColumnIPTest, FilterTest) {
    // insert from data csv and assert insert result
    MutableColumns ip_cols;
    ip_cols.push_back(column_ipv4->get_ptr());
    ip_cols.push_back(column_ipv6->get_ptr());
    check_data(ip_cols, serde, ';', {1, 2}, data_files[0], assert_filter_callback);
}

// HASH Interfaces
TEST_F(ColumnIPTest, HashTest) {
    // XXHash
    // insert from data csv and assert insert result
    MutableColumns ip_cols;
    ip_cols.push_back(column_ipv4->get_ptr());
    ip_cols.push_back(column_ipv6->get_ptr());

    load_data_from_csv(serde, ip_cols, data_files[0], ';', {1, 2});
    // update_hashes_with_value
    check_data(ip_cols, serde, ';', {1, 2}, data_files[0],
               assert_update_hashes_with_value_callback);
    // CrcHash
    std::vector<PrimitiveType> pts = {PrimitiveType::TYPE_IPV4, PrimitiveType::TYPE_IPV6};
    assert_update_crc_hashes_callback(ip_cols, serde, pts);
};

} // namespace doris::vectorized