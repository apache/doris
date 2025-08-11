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

#include "vec/columns/column_complex.h"

#include <glog/logging.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <cstddef>
#include <cstdlib>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "util/bitmap_value.h"
#include "vec/columns/column.h"
#include "vec/common/string_ref.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_bitmap.h"
#include "vec/data_types/data_type_quantilestate.h"

namespace doris::vectorized {

TEST(ColumnComplexTest, GetDataAtTest) {
    auto column_bitmap = ColumnBitmap::create();
    auto column_hll = ColumnHLL::create();
    auto column_quantile_state = ColumnQuantileState::create();
    ASSERT_EQ(column_bitmap->byte_size(), 0);

    auto column_bitmap_verify = ColumnBitmap::create();
    auto column_hll_verify = ColumnHLL::create();
    auto column_quantile_state_verify = ColumnQuantileState::create();

    column_bitmap->reserve(10);
    // empty data value
    column_bitmap->insert_value(BitmapValue::empty_bitmap());
    column_hll->insert_value(HyperLogLog());
    column_quantile_state->insert_value(QuantileState());

    column_bitmap_verify->insert_default();
    column_hll_verify->insert_default();
    column_quantile_state_verify->insert_many_defaults(1);
    ASSERT_EQ(column_bitmap_verify->byte_size(), 80);
    ASSERT_EQ(column_hll_verify->byte_size(), 64);
    ASSERT_EQ(column_quantile_state_verify->byte_size(), 64);
    ASSERT_EQ(column_bitmap_verify->allocated_bytes(), 80);
    ASSERT_EQ(column_hll_verify->allocated_bytes(), 64);
    ASSERT_EQ(column_quantile_state_verify->allocated_bytes(), 64);
    std::cout << "1. test byte_size/allocated_bytes empty success" << std::endl;

    //column_bitmap have reserve 10, but only insert 1, so the capacity is 10
    ASSERT_TRUE(column_bitmap->has_enough_capacity(*column_bitmap_verify));
    ASSERT_FALSE(column_hll->has_enough_capacity(*column_hll_verify));
    ASSERT_FALSE(column_quantile_state->has_enough_capacity(*column_quantile_state_verify));
    std::cout << "2. test has_enough_capacity success" << std::endl;

    ASSERT_EQ(column_bitmap->size(), 1);
    ASSERT_EQ(column_bitmap_verify->size(), 1);
    ASSERT_EQ(column_bitmap->get_data_at(0), column_bitmap_verify->get_data_at(0));
    ASSERT_EQ(column_hll->get_data_at(0), column_hll_verify->get_data_at(0));
    ASSERT_EQ(column_quantile_state->operator[](0), column_quantile_state_verify->operator[](0));
    std::cout << "3. test insert_default/insert_value empty success" << std::endl;

    std::srand(std::time(nullptr));
    auto lambda_function = []() -> std::tuple<UInt64, double> {
        auto rand_val = random();
        auto uint64_val = UInt64(rand_val % std::numeric_limits<UInt64>::max());
        auto double_val = static_cast<double>(rand_val) / std::numeric_limits<int>::max();
        // std::cout << "val: " << rand_val << " " << rand_val << " " << double_val << std::endl;
        return std::tuple {uint64_val, double_val};
    };
    UInt64 uint64_val = 0;
    double double_val = 0;
    std::tie(uint64_val, double_val) = lambda_function();
    // single data value
    auto bitmap = BitmapValue();
    bitmap.add(uint64_val);
    auto hll = HyperLogLog();
    hll.update(uint64_val);
    auto quantile_state = QuantileState();
    quantile_state.add_value(double_val);

    // insert random value to idx 1
    column_bitmap->insert_value(bitmap);
    column_hll->insert_value(hll);
    column_quantile_state->insert_value(quantile_state);

    auto& bitmap_verify_data = column_bitmap_verify->get_data();
    auto& hll_verify_data = column_hll_verify->get_data();
    auto& quantile_state_verify_data = column_quantile_state_verify->get_data();
    // update verify data at idx 0
    bitmap_verify_data[0].add(uint64_val);
    hll_verify_data[0].update(uint64_val);
    quantile_state_verify_data[0].add_value(double_val);
    ASSERT_EQ(column_bitmap->size(), 2);
    ASSERT_EQ(column_bitmap_verify->size(), 1);
    ASSERT_EQ(column_bitmap->get_element(1).to_string(), bitmap_verify_data[0].to_string());
    ASSERT_EQ(column_hll->get_element(1).to_string(),
              column_hll_verify->get_element(0).to_string());
    ASSERT_EQ(column_quantile_state->get_data_at(1), column_quantile_state_verify->get_data_at(0));
    std::cout << "4. test insert/update/get_element data success" << std::endl;

    // insert data from column idx 1
    column_bitmap_verify->insert_from(*column_bitmap, 1);
    column_hll_verify->insert_from(*column_hll, 1);
    column_quantile_state_verify->insert_from(*column_quantile_state, 1);
    ASSERT_EQ(column_bitmap->size(), 2);
    ASSERT_EQ(column_bitmap_verify->size(), 2);
    ASSERT_EQ(column_bitmap->get_element(1).to_string(),
              column_bitmap_verify->get_element(1).to_string());
    ASSERT_EQ(column_hll->get_element(1).to_string(),
              column_hll_verify->get_element(1).to_string());
    ASSERT_EQ(column_quantile_state->operator[](1), column_quantile_state_verify->operator[](1));
    ASSERT_EQ(column_bitmap->operator[](1), column_bitmap_verify->operator[](1));
    ASSERT_EQ(column_hll->operator[](1), column_hll_verify->operator[](1));
    std::cout << "5. test insert_from data success" << std::endl;

    Field field1, field2, field3;
    column_bitmap->get(1, field1);
    column_hll->get(1, field2);
    column_quantile_state->get(1, field3);

    //pop_back data from column idx 1
    column_bitmap_verify->pop_back(1);
    column_hll_verify->pop_back(1);
    column_quantile_state_verify->pop_back(1);
    //insert data from field
    column_bitmap_verify->insert(field1);
    column_hll_verify->insert(field2);
    column_quantile_state_verify->insert(field3);

    ASSERT_EQ(column_bitmap->size(), 2);
    ASSERT_EQ(column_bitmap_verify->size(), 2);
    ASSERT_EQ(column_bitmap->get_element(1).to_string(),
              column_bitmap_verify->get_element(1).to_string());
    ASSERT_EQ(column_hll->get_element(1).to_string(),
              column_hll_verify->get_element(1).to_string());
    ASSERT_EQ(column_quantile_state->operator[](1), column_quantile_state_verify->operator[](1));
    ASSERT_EQ(column_bitmap->operator[](1), column_bitmap_verify->operator[](1));
    ASSERT_EQ(column_hll->operator[](1), column_hll_verify->operator[](1));
    std::cout << "6. test get/insert data and pop_back success" << std::endl;

    std::tie(uint64_val, double_val) = lambda_function();
    // two val in data value
    bitmap.add(uint64_val);
    hll.update(uint64_val);
    quantile_state.add_value(double_val);
    column_bitmap->insert_value(bitmap);
    column_hll->insert_value(hll);
    column_quantile_state->insert_value(quantile_state);

    column_bitmap_verify->insert_data(column_bitmap->get_data_at(2).data,
                                      column_bitmap->get_data_at(2).size);
    column_hll_verify->insert_data(column_hll->get_data_at(2).data,
                                   column_hll->get_data_at(2).size);
    column_quantile_state_verify->insert_data(column_quantile_state->get_data_at(2).data,
                                              column_quantile_state->get_data_at(2).size);
    ASSERT_EQ(column_bitmap->size(), 3);
    ASSERT_EQ(column_bitmap_verify->size(), 3);
    ASSERT_EQ(column_bitmap->get_element(2).to_string(),
              column_bitmap_verify->get_element(2).to_string());
    ASSERT_EQ(column_hll->get_element(2).to_string(),
              column_hll_verify->get_element(2).to_string());
    ASSERT_EQ(column_quantile_state->operator[](2), column_quantile_state_verify->operator[](2));
    ASSERT_EQ(column_bitmap->operator[](2), column_bitmap_verify->operator[](2));
    ASSERT_EQ(column_hll->operator[](2), column_hll_verify->operator[](2));
    std::cout << "7. test two val data value and insert_data success" << std::endl;

    // more val in data value
    for (int range = 1; range < 100; ++range) {
        std::tie(uint64_val, double_val) = lambda_function();
        bitmap.add(uint64_val);
        hll.update(uint64_val);
        quantile_state.add_value(double_val);
    }
    column_bitmap->insert_value(bitmap);
    column_hll->insert_value(hll);
    column_quantile_state->insert_value(quantile_state);

    std::string buffer;
    buffer.resize(bitmap.getSizeInBytes(), '0');
    bitmap.write_to(const_cast<char*>(buffer.data()));
    column_bitmap_verify->insert_binary_data(buffer.data(), buffer.size());

    buffer.resize(hll.max_serialized_size(), '0');
    size_t actual_size = hll.serialize((uint8_t*)buffer.data());
    buffer.resize(actual_size);
    column_hll_verify->insert_binary_data(buffer.data(), buffer.size());

    buffer.resize(quantile_state.get_serialized_size());
    quantile_state.serialize(const_cast<uint8_t*>(reinterpret_cast<uint8_t*>(buffer.data())));
    column_quantile_state_verify->insert_binary_data(buffer.data(), buffer.size());

    ASSERT_EQ(column_bitmap->size(), 4);
    ASSERT_EQ(column_bitmap_verify->size(), 4);
    ASSERT_EQ(column_bitmap->get_element(3).to_string(),
              column_bitmap_verify->get_element(3).to_string());
    ASSERT_EQ(column_hll->get_element(3).to_string(),
              column_hll_verify->get_element(3).to_string());
    ASSERT_EQ(column_quantile_state->operator[](3), column_quantile_state_verify->operator[](3));
    ASSERT_EQ(column_bitmap->operator[](3), column_bitmap_verify->operator[](3));
    ASSERT_EQ(column_hll->operator[](3), column_hll_verify->operator[](3));
    std::cout << "8. test more val data value and insert_binary_data success" << std::endl;

    column_bitmap_verify->clear();
    column_hll_verify->clear();
    column_quantile_state_verify->clear();
    ASSERT_EQ(column_bitmap_verify->size(), 0);

    std::vector<StringRef> bitmap_strings, hll_strings, quantile_state_strings;
    auto rows = column_bitmap->size();
    std::vector<std::string> bitmap_buffers(rows), hll_buffers(rows), quantile_state_buffers(rows);
    for (int i = 0; i < column_bitmap->size(); ++i) {
        auto bitmap = column_bitmap->get_element(i);
        bitmap_buffers[i].resize(bitmap.getSizeInBytes(), '0');
        bitmap.write_to(const_cast<char*>(bitmap_buffers[i].data()));
        bitmap_strings.emplace_back(bitmap_buffers[i].data(), bitmap_buffers[i].size());

        auto hll = column_hll->get_element(i);
        hll_buffers[i].resize(hll.max_serialized_size(), '0');
        size_t actual_size = hll.serialize((uint8_t*)hll_buffers[i].data());
        hll_buffers[i].resize(actual_size);
        hll_strings.emplace_back(hll_buffers[i].data(), hll_buffers[i].size());

        auto quantile_state = column_quantile_state->get_element(i);
        quantile_state_buffers[i].resize(quantile_state.get_serialized_size());
        quantile_state.serialize(
                const_cast<uint8_t*>(reinterpret_cast<uint8_t*>(quantile_state_buffers[i].data())));
        quantile_state_strings.emplace_back(quantile_state_buffers[i].data(),
                                            quantile_state_buffers[i].size());
    }
    column_bitmap_verify->insert_many_strings(bitmap_strings.data(), column_bitmap->size());
    column_hll_verify->insert_many_strings(hll_strings.data(), column_hll->size());

    ASSERT_EQ(column_hll_verify->clone_resized(0)->size(), 0);
    ASSERT_EQ(column_hll_verify->clone_resized(1)->size(), 1);
    ASSERT_EQ(column_hll_verify->clone_resized(1024)->size(), 1024);

    column_quantile_state_verify->insert_many_strings(quantile_state_strings.data(),
                                                      column_quantile_state->size());
    ASSERT_EQ(rows, column_bitmap_verify->size());
    ASSERT_EQ(rows, column_hll_verify->size());
    ASSERT_EQ(rows, column_quantile_state_verify->size());
    for (int i = 0; i < rows; ++i) {
        ASSERT_EQ(column_bitmap->get_element(i).to_string(),
                  column_bitmap_verify->get_element(i).to_string());
        ASSERT_EQ(column_hll->get_element(i).to_string(),
                  column_hll_verify->get_element(i).to_string());
        ASSERT_EQ(column_quantile_state->operator[](i),
                  column_quantile_state_verify->operator[](i));
        ASSERT_EQ(column_bitmap->operator[](i), column_bitmap_verify->operator[](i));
        ASSERT_EQ(column_hll->operator[](i), column_hll_verify->operator[](i));
    }
    std::cout << "9. test more val data value and insert_many_strings success" << std::endl;

    auto column_bitmap_verify2 = column_bitmap_verify->clone_empty();
    auto column_hll_verify2 = column_hll_verify->clone_resized(0);
    auto column_quantile_state_verify2 = column_quantile_state_verify->clone_empty();

    ASSERT_EQ(column_bitmap_verify2->size(), 0);
    ASSERT_EQ(column_hll_verify2->size(), 0);
    ASSERT_EQ(column_quantile_state_verify2->size(), 0);
    std::string bitmap_buffer, hll_buffer, quantile_state_buffer;
    //the offset should be more than one value
    std::vector<uint32_t> bitmap_offsets(rows + 1, 0), hll_offsets(rows + 1, 0),
            quantile_state_offsets(rows + 1, 0);
    for (int i = 0; i < rows; ++i) {
        bitmap_buffer.append(bitmap_strings[i]);
        hll_buffer.append(hll_buffers[i]);
        quantile_state_buffer.append(quantile_state_buffers[i]);
        bitmap_offsets[i + 1] = bitmap_offsets[i] + bitmap_strings[i].size;
        hll_offsets[i + 1] = hll_offsets[i] + hll_strings[i].size;
        quantile_state_offsets[i + 1] = quantile_state_offsets[i] + quantile_state_strings[i].size;
    }
    column_bitmap_verify2->insert_many_continuous_binary_data(bitmap_buffer.data(),
                                                              bitmap_offsets.data(), rows);
    column_hll_verify2->insert_many_continuous_binary_data(hll_buffer.data(), hll_offsets.data(),
                                                           rows);
    column_quantile_state_verify2->insert_many_continuous_binary_data(
            quantile_state_buffer.data(), quantile_state_offsets.data(), rows);
    ASSERT_EQ(rows, column_bitmap_verify2->size());
    ASSERT_EQ(rows, column_hll_verify2->size());
    ASSERT_EQ(rows, column_quantile_state_verify2->size());
    for (int i = 0; i < rows; ++i) {
        ASSERT_EQ(column_quantile_state->operator[](i),
                  column_quantile_state_verify2->operator[](i));
        ASSERT_EQ(column_bitmap->operator[](i), column_bitmap_verify2->operator[](i));
        ASSERT_EQ(column_hll->operator[](i), column_hll_verify2->operator[](i));
    }
    std::cout << "10. test more val data value and insert_many_continuous_binary_data success"
              << std::endl;

    column_bitmap_verify->resize(rows);
    column_hll_verify->resize(rows);
    column_quantile_state_verify->resize(rows);
    column_bitmap_verify->clear();
    column_hll_verify->clear();
    column_quantile_state_verify->clear();
    column_bitmap_verify->insert_range_from(*column_bitmap, 0, rows);
    column_hll_verify->insert_range_from(*column_hll, 0, rows);
    column_quantile_state_verify->insert_range_from(*column_quantile_state, 0, rows);
    ASSERT_EQ(rows, column_bitmap_verify->size());
    ASSERT_EQ(rows, column_hll_verify->size());
    ASSERT_EQ(rows, column_quantile_state_verify->size());
    for (int i = 0; i < rows; ++i) {
        ASSERT_EQ(column_quantile_state->operator[](i),
                  column_quantile_state_verify->operator[](i));
        ASSERT_EQ(column_bitmap->operator[](i), column_bitmap_verify->operator[](i));
        ASSERT_EQ(column_hll->operator[](i), column_hll_verify->operator[](i));
    }
    std::cout << "11. test more val data value and insert_range_from success" << std::endl;

    column_bitmap_verify->clear();
    column_hll_verify->clear();
    column_quantile_state_verify->clear();
    std::vector<uint32_t> indices;
    for (int i = 0; i < rows; ++i) {
        indices.push_back(i);
    }
    column_bitmap_verify->insert_indices_from(*column_bitmap, indices.data(),
                                              indices.data() + rows);
    column_hll_verify->insert_indices_from(*column_hll, indices.data(), indices.data() + rows);
    column_quantile_state_verify->insert_indices_from(*column_quantile_state, indices.data(),
                                                      indices.data() + rows);
    ASSERT_EQ(rows, column_bitmap_verify->size());
    ASSERT_EQ(rows, column_hll_verify->size());
    ASSERT_EQ(rows, column_quantile_state_verify->size());
    for (int i = 0; i < rows; ++i) {
        ASSERT_EQ(column_quantile_state->operator[](i),
                  column_quantile_state_verify->operator[](i));
        ASSERT_EQ(column_bitmap->operator[](i), column_bitmap_verify->operator[](i));
        ASSERT_EQ(column_hll->operator[](i), column_hll_verify->operator[](i));
    }
    std::cout << "12. test more val data value and insert_indices_from success" << std::endl;

    column_bitmap->insert_default();
    column_hll->insert_default();
    column_quantile_state->insert_default();
    column_bitmap_verify->insert_many_from(*column_bitmap, rows, 1);
    column_hll_verify->insert_many_from(*column_hll, rows, 1);
    column_quantile_state_verify->insert_many_from(*column_quantile_state, rows, 1);
    rows = rows + 1;
    ASSERT_EQ(rows, column_bitmap_verify->size());
    ASSERT_EQ(rows, column_hll_verify->size());
    ASSERT_EQ(rows, column_quantile_state_verify->size());
    for (int i = 0; i < rows; ++i) {
        ASSERT_EQ(column_quantile_state->operator[](i),
                  column_quantile_state_verify->operator[](i));
        ASSERT_EQ(column_bitmap->operator[](i), column_bitmap_verify->operator[](i));
        ASSERT_EQ(column_hll->operator[](i), column_hll_verify->operator[](i));
    }
    std::cout << "13. test more val data value and insert_many_from success" << std::endl;

    column_bitmap->replace_column_data(*column_bitmap_verify, 0, 4);
    column_hll->replace_column_data(*column_hll_verify, 0, 4);
    column_quantile_state->replace_column_data(*column_quantile_state_verify, 0, 4);
    for (int i = 0; i < rows; ++i) {
        ASSERT_EQ(column_quantile_state->operator[](i),
                  column_quantile_state_verify->operator[](i));
        ASSERT_EQ(column_bitmap->operator[](i), column_bitmap_verify->operator[](i));
        ASSERT_EQ(column_hll->operator[](i), column_hll_verify->operator[](i));
    }
    std::cout << "14. test more val data value and replace_column_data success" << std::endl;

    vectorized::IColumn::Filter filter;
    for (int i = 0; i < rows; i++) {
        filter.push_back(i % 2);
    }
    // filter data 0 1 0 1 0
    ASSERT_EQ(column_bitmap_verify->clone()->filter(filter), 2);
    ASSERT_EQ(column_hll_verify->clone()->filter(filter), 2);
    ASSERT_EQ(column_quantile_state_verify->clone()->filter(filter), 2);
    auto column_filter_res_bitmap = column_bitmap->filter(filter, 0);
    auto column_filter_res_hll = column_hll->filter(filter, 0);
    auto column_filter_res_quantile_state = column_quantile_state->filter(filter, 0);

    ASSERT_EQ(column_filter_res_bitmap->size(), 2);
    ASSERT_EQ(column_filter_res_hll->size(), 2);
    ASSERT_EQ(column_filter_res_quantile_state->size(), 2);
    for (int i = 0, j = 0; i < rows; ++i) {
        if (i % 2) {
            ASSERT_EQ(column_quantile_state->operator[](i),
                      column_filter_res_quantile_state->operator[](j));
            ASSERT_EQ(column_bitmap->operator[](i), column_filter_res_bitmap->operator[](j));
            ASSERT_EQ(column_hll->operator[](i), column_filter_res_hll->operator[](j));
            j++;
        }
    }

    filter.clear();
    filter.resize_fill(rows, 1);
    // filter data 1 1 1 1 1
    ASSERT_EQ(column_bitmap_verify->clone()->filter(filter), rows);
    ASSERT_EQ(column_hll_verify->clone()->filter(filter), rows);
    ASSERT_EQ(column_quantile_state_verify->clone()->filter(filter), rows);
    column_filter_res_bitmap = column_bitmap->filter(filter, 0);
    column_filter_res_hll = column_hll->filter(filter, 0);
    column_filter_res_quantile_state = column_quantile_state->filter(filter, 0);
    ASSERT_EQ(column_filter_res_bitmap->size(), rows);
    ASSERT_EQ(column_filter_res_hll->size(), rows);
    ASSERT_EQ(column_filter_res_quantile_state->size(), rows);
    for (int i = 0, j = 0; i < rows; ++i) {
        if (i % 2) {
            ASSERT_EQ(column_quantile_state->operator[](i),
                      column_filter_res_quantile_state->operator[](j));
            ASSERT_EQ(column_bitmap->operator[](i), column_filter_res_bitmap->operator[](j));
            ASSERT_EQ(column_hll->operator[](i), column_filter_res_hll->operator[](j));
            j++;
        }
    }

    filter.clear();
    filter.resize_fill(rows, 0);
    // filter data 0 0 0 0 0
    ASSERT_EQ(column_bitmap_verify->clone()->filter(filter), 0);
    ASSERT_EQ(column_hll_verify->clone()->filter(filter), 0);
    ASSERT_EQ(column_quantile_state_verify->clone()->filter(filter), 0);
    column_filter_res_bitmap = column_bitmap->filter(filter, 0);
    column_filter_res_hll = column_hll->filter(filter, 0);
    column_filter_res_quantile_state = column_quantile_state->filter(filter, 0);
    ASSERT_EQ(column_filter_res_bitmap->size(), 0);
    ASSERT_EQ(column_filter_res_hll->size(), 0);
    ASSERT_EQ(column_filter_res_quantile_state->size(), 0);
    std::cout << "15. test more val data value and filter success" << std::endl;

    IColumn::Permutation perm {4, 3, 2, 1, 0};
    auto column_bitmap_perm = column_bitmap->permute(perm, rows);
    auto column_hll_perm = column_hll->permute(perm, rows);
    auto column_quantile_state_perm = column_quantile_state->permute(perm, rows);
    ASSERT_EQ(column_bitmap_perm->size(), 5);
    ASSERT_EQ(column_hll_perm->size(), 5);
    ASSERT_EQ(column_quantile_state_perm->size(), 5);
    for (int i = 0, j = 4; i < rows; ++i, --j) {
        ASSERT_EQ(column_quantile_state->operator[](i), column_quantile_state_perm->operator[](j));
        ASSERT_EQ(column_bitmap->operator[](i), column_bitmap_perm->operator[](j));
        ASSERT_EQ(column_hll->operator[](i), column_hll_perm->operator[](j));
    }

    IColumn::Permutation perm2 {0, 1, 2, 3, 4};
    auto column_bitmap_perm2 = column_bitmap->permute(perm, rows);
    auto column_hll_perm2 = column_hll->permute(perm, rows);
    auto column_quantile_state_perm2 = column_quantile_state->permute(perm, rows);
    ASSERT_EQ(column_bitmap_perm2->size(), 5);
    ASSERT_EQ(column_hll_perm2->size(), 5);
    ASSERT_EQ(column_quantile_state_perm2->size(), 5);
    for (int i = 0; i < rows; ++i) {
        ASSERT_EQ(column_quantile_state->operator[](i), column_quantile_state_perm->operator[](i));
        ASSERT_EQ(column_bitmap->operator[](i), column_bitmap_perm->operator[](i));
        ASSERT_EQ(column_hll->operator[](i), column_hll_perm->operator[](i));
    }

    IColumn::Permutation perm3 {4, 2, 0, 1, 3};
    std::vector<int> permute_idx {2, 3, 1, 4, 0};
    auto column_bitmap_perm3 = column_bitmap->permute(perm, rows);
    auto column_hll_perm3 = column_hll->permute(perm, rows);
    auto column_quantile_state_perm3 = column_quantile_state->permute(perm, rows);
    ASSERT_EQ(column_bitmap_perm3->size(), 5);
    ASSERT_EQ(column_hll_perm3->size(), 5);
    ASSERT_EQ(column_quantile_state_perm3->size(), 5);
    for (int i = 0; i < rows; ++i) {
        ASSERT_EQ(column_quantile_state->operator[](i),
                  column_quantile_state_perm3->operator[](permute_idx[i]));
        ASSERT_EQ(column_bitmap->operator[](i), column_bitmap_perm3->operator[](permute_idx[i]));
        ASSERT_EQ(column_hll->operator[](i), column_hll_perm3->operator[](permute_idx[i]));
    }
    std::cout << "16. test more val data value and permute success" << std::endl;
}

class ColumnBitmapTest : public testing::Test {
public:
    virtual void SetUp() override {}
    virtual void TearDown() override {}

    void check_bitmap_column(const IColumn& l, const IColumn& r) {
        ASSERT_EQ(l.size(), r.size());
        const auto& l_col = assert_cast<const ColumnBitmap&>(l);
        const auto& r_col = assert_cast<const ColumnBitmap&>(r);
        for (size_t i = 0; i < l_col.size(); ++i) {
            auto& l_bitmap = const_cast<BitmapValue&>(l_col.get_element(i));
            auto& r_bitmap = const_cast<BitmapValue&>(r_col.get_element(i));
            ASSERT_EQ(l_bitmap.and_cardinality(r_bitmap), r_bitmap.cardinality());
            auto or_cardinality = l_bitmap.or_cardinality(r_bitmap);
            ASSERT_EQ(or_cardinality, l_bitmap.cardinality());
            ASSERT_EQ(or_cardinality, r_bitmap.cardinality());
        }
    }

    void check_serialize_and_deserialize(MutableColumnPtr& col) {
        auto* column = assert_cast<ColumnBitmap*>(col.get());
        auto size = _bitmap_type.get_uncompressed_serialized_bytes(
                *column, BeExecVersionManager::get_newest_version());
        std::unique_ptr<char[]> buf = std::make_unique<char[]>(size);
        auto* result = _bitmap_type.serialize(*column, buf.get(),
                                              BeExecVersionManager::get_newest_version());
        ASSERT_EQ(result, buf.get() + size);

        auto column2 = _bitmap_type.create_column();
        _bitmap_type.deserialize(buf.get(), &column2, BeExecVersionManager::get_newest_version());
        check_bitmap_column(*column, *column2.get());
    }

    void check_field_type(MutableColumnPtr& col) {
        auto& column = assert_cast<ColumnBitmap&>(*col.get());
        auto dst_column = ColumnBitmap::create();
        const auto rows = column.size();
        for (size_t i = 0; i != rows; ++i) {
            auto field = column[i];
            ASSERT_EQ(field.get_type(), PrimitiveType::TYPE_BITMAP);
            dst_column->insert(field);
        }

        check_bitmap_column(column, *dst_column);
    }

private:
    DataTypeBitMap _bitmap_type;
};

class ColumnQuantileStateTest : public testing::Test {
public:
    virtual void SetUp() override {}
    virtual void TearDown() override {}

    void check_quantile_state_column(const IColumn& l, const IColumn& r) {
        ASSERT_EQ(l.size(), r.size());
        const auto& l_col = assert_cast<const ColumnQuantileState&>(l);
        const auto& r_col = assert_cast<const ColumnQuantileState&>(r);
        for (size_t i = 0; i < l_col.size(); ++i) {
            auto& l_value = const_cast<QuantileState&>(l_col.get_element(i));
            auto& r_value = const_cast<QuantileState&>(r_col.get_element(i));
            ASSERT_EQ(l_value.get_serialized_size(), r_value.get_serialized_size());
        }
    }

    void check_serialize_and_deserialize(MutableColumnPtr& col) {
        auto column = assert_cast<ColumnQuantileState*>(col.get());
        auto size = _quantile_state_type.get_uncompressed_serialized_bytes(
                *column, BeExecVersionManager::get_newest_version());
        std::unique_ptr<char[]> buf = std::make_unique<char[]>(size);
        auto result = _quantile_state_type.serialize(*column, buf.get(),
                                                     BeExecVersionManager::get_newest_version());
        ASSERT_EQ(result, buf.get() + size);

        auto column2 = _quantile_state_type.create_column();
        _quantile_state_type.deserialize(buf.get(), &column2,
                                         BeExecVersionManager::get_newest_version());
        check_quantile_state_column(*column, *column2.get());
    }

    void check_field_type(MutableColumnPtr& col) {
        auto& column = assert_cast<ColumnQuantileState&>(*col.get());
        auto dst_column = ColumnQuantileState::create();
        const auto rows = column.size();
        for (size_t i = 0; i != rows; ++i) {
            auto field = column[i];
            ASSERT_EQ(field.get_type(), PrimitiveType::TYPE_QUANTILE_STATE);
            dst_column->insert(field);
        }

        check_quantile_state_column(column, *dst_column);
    }

private:
    DataTypeQuantileState _quantile_state_type;
};

TEST_F(ColumnBitmapTest, ColumnBitmapReadWrite) {
    auto column = _bitmap_type.create_column();

    // empty column
    check_serialize_and_deserialize(column);

    // bitmap with lots of rows
    const size_t row_size = 20000;
    auto& data = assert_cast<ColumnBitmap&>(*column.get()).get_data();
    data.resize(row_size);
    check_serialize_and_deserialize(column);

    // bitmap with values case 1
    data[0].add(10);
    data[0].add(1000000);
    check_serialize_and_deserialize(column);

    // bitmap with values case 2
    data[row_size - 1].add(33333);
    data[row_size - 1].add(0);
    check_serialize_and_deserialize(column);

    Field field;
    column->get(0, field);
    auto bitmap = field.get<BitmapValue>();
    EXPECT_TRUE(bitmap.contains(10));
    EXPECT_TRUE(bitmap.contains(1000000));
}

TEST_F(ColumnBitmapTest, OperatorValidate) {
    auto column = _bitmap_type.create_column();

    // empty column
    check_serialize_and_deserialize(column);

    // bitmap with lots of rows
    const size_t row_size = 128;
    auto& data = assert_cast<ColumnBitmap&>(*column.get()).get_data();
    data.reserve(row_size);

    for (size_t i = 0; i != row_size; ++i) {
        BitmapValue bitmap_value;
        for (size_t j = 0; j <= i; ++j) {
            bitmap_value.add(j);
        }
        data.emplace_back(std::move(bitmap_value));
    }

    auto& bitmap_column = assert_cast<ColumnBitmap&>(*column.get());
    for (size_t i = 0; i != row_size; ++i) {
        auto field = bitmap_column[i];
        ASSERT_EQ(field.get_type(), PrimitiveType::TYPE_BITMAP);
        const auto& bitmap = vectorized::get<BitmapValue&>(field);

        ASSERT_EQ(bitmap.cardinality(), i + 1);
        for (size_t j = 0; j <= i; ++j) {
            ASSERT_TRUE(bitmap.contains(j));
        }
    }
}

TEST_F(ColumnQuantileStateTest, ColumnQuantileStateReadWrite) {
    auto column = _quantile_state_type.create_column();
    // empty column
    check_serialize_and_deserialize(column);

    // quantile column with lots of rows
    const size_t row_size = 20000;
    auto& data = assert_cast<ColumnQuantileState&>(*column.get()).get_data();
    data.resize(row_size);
    // EMPTY type
    check_serialize_and_deserialize(column);
    // SINGLE type
    for (size_t i = 0; i < row_size; ++i) {
        data[i].add_value(i);
    }
    check_serialize_and_deserialize(column);
    // EXPLICIT type
    for (size_t i = 0; i < row_size; ++i) {
        data[i].add_value(i + 1);
    }
    // TDIGEST type
    for (size_t i = 0; i < QUANTILE_STATE_EXPLICIT_NUM; ++i) {
        data[0].add_value(i);
    }
    check_serialize_and_deserialize(column);
}

TEST_F(ColumnQuantileStateTest, OperatorValidate) {
    auto column = _quantile_state_type.create_column();

    // empty column
    check_serialize_and_deserialize(column);

    // bitmap with lots of rows
    const size_t row_size = 20000;
    auto& data = assert_cast<ColumnQuantileState&>(*column.get()).get_data();
    data.resize(row_size);
    check_serialize_and_deserialize(column);

    check_field_type(column);
}

TEST(ColumnComplexTest, TestErase) {
    using ColumnTest = ColumnComplexType<TYPE_BITMAP>;

    auto column_test = ColumnTest::create();

    column_test->data.push_back(BitmapValue {});
    column_test->data.push_back(BitmapValue {});
    column_test->data.push_back(BitmapValue {});
    column_test->data.push_back(BitmapValue {});
    column_test->data.push_back(BitmapValue {});

    column_test->erase(1, 0);

    column_test->erase(3, 1);

    EXPECT_EQ(column_test->size(), 4);
}

} // namespace doris::vectorized
