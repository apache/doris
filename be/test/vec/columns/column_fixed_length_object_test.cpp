
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

#include "vec/columns/column_fixed_length_object.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>
#include <stddef.h>

#include <memory>

#include "gtest/gtest_pred_impl.h"
#include "vec/columns/column_vector.h"
#include "vec/common/sip_hash.h"
#include "vec/common/string_ref.h"

namespace doris::vectorized {

TEST(ColumnFixedLenghtObjectTest, InsertRangeFrom) {
    auto column1 = ColumnFixedLengthObject::create(sizeof(size_t));
    EXPECT_EQ(sizeof(size_t), column1->item_size());
    const size_t count = 1000;

    column1->resize(count);
    auto& data = column1->get_data();
    for (size_t i = 0; i < count; ++i) {
        *((size_t*)&data[i * sizeof(size_t)]) = i;
    }

    auto column2 = ColumnFixedLengthObject::create(sizeof(size_t));
    EXPECT_EQ(sizeof(size_t), column2->item_size());

    column2->insert_range_from(*column1, 100, 0);
    EXPECT_EQ(column2->size(), 0);

    column2->insert_range_from(*column1, 97, 100);
    EXPECT_EQ(column2->size(), 100);

    auto& data2 = column2->get_data();
    for (size_t i = 0; i < 100; ++i) {
        EXPECT_EQ(*((size_t*)&data2[i * sizeof(size_t)]), i + 97);
    }
}

TEST(ColumnFixedLenghtObjectTest, UpdateHashWithValue) {
    auto column1 = ColumnFixedLengthObject::create(sizeof(int64_t));
    EXPECT_EQ(sizeof(int64_t), column1->item_size());
    const size_t count = 1000;

    column1->resize(count);
    auto& data = column1->get_data();
    for (size_t i = 0; i != count; ++i) {
        *((int64_t*)&data[i * column1->item_size()]) = i;
    }

    SipHash hash1;
    for (size_t i = 0; i != count; ++i) {
        column1->update_hash_with_value(i, hash1);
    }

    auto column2 = ColumnInt64::create();
    column2->resize(count);
    for (size_t i = 0; i != count; ++i) {
        column2->get_data()[i] = i;
    }

    SipHash hash2;
    for (size_t i = 0; i != count; ++i) {
        column2->update_hash_with_value(i, hash2);
    }

    EXPECT_EQ(hash1.get64(), hash2.get64());
}

TEST(ColumnFixedLenghtObjectTest, GetDataAtTest) {
    auto column_fixed = ColumnFixedLengthObject::create(sizeof(int64_t));
    EXPECT_EQ(sizeof(int64_t), column_fixed->item_size());
    EXPECT_EQ(8, column_fixed->item_size());
    column_fixed->set_item_size(8);
    ASSERT_EQ(column_fixed->get_name(), "ColumnFixedLengthObject");
    ASSERT_EQ(column_fixed->size(), 0);
    ASSERT_EQ(column_fixed->get_data().size(), 0);
    ASSERT_EQ(column_fixed->byte_size(), 0);
    ASSERT_EQ(column_fixed->allocated_bytes(), 0);
    std::cout << "1. test name item_size size success" << std::endl;

    column_fixed->insert_default();
    ASSERT_EQ(*reinterpret_cast<const int64_t*>(column_fixed->get_data_at(0).data), 0);
    ASSERT_EQ(column_fixed->size(), 1);
    ASSERT_EQ(column_fixed->byte_size(), 8);
    ASSERT_EQ(column_fixed->allocated_bytes(), 64);
    column_fixed->pop_back(1);
    ASSERT_EQ(column_fixed->size(), 0);
    ASSERT_EQ(column_fixed->byte_size(), 0);
    ASSERT_EQ(column_fixed->allocated_bytes(), 64);
    std::cout << "2. test byte_size allocated_bytes success" << std::endl;

    auto column_fixed2 = ColumnFixedLengthObject::create(sizeof(int64_t));
    column_fixed->resize(2);
    ASSERT_TRUE(column_fixed->has_enough_capacity(*column_fixed2));
    *((int64_t*)column_fixed->get_data().data()) = 11;
    *((int64_t*)&(column_fixed->get_data()[column_fixed->item_size()])) = 22;
    ASSERT_EQ(column_fixed->size(), 2);
    ASSERT_EQ(column_fixed->byte_size(), 16);
    ASSERT_EQ(column_fixed->allocated_bytes(), 64);
    std::cout << "3. test has_enough_capacity and value data success" << std::endl;

    Field res;
    column_fixed->get(0, res);
    column_fixed2->insert(res);
    ASSERT_EQ(column_fixed2->size(), 1);
    ASSERT_EQ(column_fixed->operator[](0), column_fixed2->operator[](0));
    column_fixed2->insert_from(*column_fixed, 1);
    ASSERT_EQ(column_fixed2->size(), 2);
    ASSERT_EQ(column_fixed->operator[](1), column_fixed2->operator[](1));
    //capacity and size is 32 16 16
    ASSERT_TRUE(column_fixed->has_enough_capacity(*column_fixed2));
    std::cout << "4. test get/insert/insert_from/has_enough_capacity data success" << std::endl;

    column_fixed2->clear();
    ASSERT_EQ(column_fixed2->size(), 0);
    column_fixed2->insert_range_from(*column_fixed, 0, 2);
    ASSERT_EQ(column_fixed2->size(), 2);
    ASSERT_EQ(column_fixed->operator[](0), column_fixed2->operator[](0));
    ASSERT_EQ(column_fixed->operator[](1), column_fixed2->operator[](1));
    ASSERT_EQ(*reinterpret_cast<const int64_t*>(column_fixed2->get_data_at(0).data), 11);
    ASSERT_EQ(*reinterpret_cast<const int64_t*>(column_fixed2->get_data_at(1).data), 22);
    std::cout << "5. test clear/insert_range_from data success" << std::endl;

    int64_t val = 33;
    column_fixed2->insert_data(reinterpret_cast<const char*>(&val), sizeof(val));
    ASSERT_EQ(column_fixed2->size(), 3);
    auto value = column_fixed2->get_data_at(2);
    ASSERT_EQ(*reinterpret_cast<const int64_t*>(value.data), 33);
    std::cout << "6. test insert_data data success" << std::endl;

    auto column_fixed3 = ColumnFixedLengthObject::create(sizeof(int64_t));
    std::vector<uint32_t> indexs = {0, 1, 2};
    column_fixed3->insert_indices_from(*column_fixed2, indexs.data(),
                                       indexs.data() + indexs.size());
    ASSERT_EQ(column_fixed3->size(), 3);
    ASSERT_EQ(column_fixed2->operator[](0), column_fixed3->operator[](0));
    ASSERT_EQ(column_fixed2->operator[](1), column_fixed3->operator[](1));
    ASSERT_EQ(column_fixed2->operator[](2), column_fixed3->operator[](2));
    ASSERT_EQ(*reinterpret_cast<const int64_t*>(column_fixed3->get_data_at(2).data), 33);
    std::cout << "7. test insert_indices_from data success" << std::endl;

    auto column_fixed4 = ColumnFixedLengthObject::create(sizeof(int64_t));
    std::vector<StringRef> strings;
    for (int i = 0; i < 3; i++) {
        strings.push_back(column_fixed2->get_data_at(i));
    }
    column_fixed4->insert_many_strings(strings.data(), 3);
    ASSERT_EQ(column_fixed4->size(), 3);
    ASSERT_EQ(column_fixed2->operator[](0), column_fixed4->operator[](0));
    ASSERT_EQ(column_fixed2->operator[](1), column_fixed4->operator[](1));
    ASSERT_EQ(*reinterpret_cast<const int64_t*>(column_fixed4->get_data_at(0).data), 11);
    ASSERT_EQ(*reinterpret_cast<const int64_t*>(column_fixed4->get_data_at(1).data), 22);
    ASSERT_EQ(*reinterpret_cast<const int64_t*>(column_fixed2->get_data_at(2).data), 33);
    // ASSERT_EQ(*reinterpret_cast<const int64_t*>(column_fixed4->get_data_at(2).data), 33);
    // ASSERT_EQ(column_fixed2->get_data_at(2).to_string(), column_fixed4->get_data_at(2).to_string());
    std::cout << "8. test insert_many_strings data success" << std::endl;

    auto column_fixed5 = ColumnFixedLengthObject::create(sizeof(int64_t));
    std::string buffer;
    std::vector<uint32_t> buffer_offsets(4, 0);
    for (int i = 0; i < 3; ++i) {
        buffer.append(strings[i]);
        buffer_offsets[i + 1] = buffer_offsets[i] + strings[i].size;
    }
    column_fixed5->insert_many_continuous_binary_data(buffer.data(), buffer_offsets.data(), 3);
    ASSERT_EQ(column_fixed5->size(), 3);
    ASSERT_EQ(column_fixed2->operator[](0), column_fixed5->operator[](0));
    ASSERT_EQ(column_fixed2->operator[](1), column_fixed5->operator[](1));
    ASSERT_EQ(*reinterpret_cast<const int64_t*>(column_fixed5->get_data_at(0).data), 11);
    ASSERT_EQ(*reinterpret_cast<const int64_t*>(column_fixed5->get_data_at(1).data), 22);
    ASSERT_EQ(*reinterpret_cast<const int64_t*>(column_fixed5->get_data_at(2).data), 33);
    std::cout << "9. test insert_many_continuous_binary_data data success" << std::endl;

    auto column_fixed6 = ColumnFixedLengthObject::create(sizeof(int64_t));
    column_fixed6->insert_range_from(*column_fixed5, 0, 3);
    ASSERT_EQ(*reinterpret_cast<const int64_t*>(column_fixed6->get_data_at(0).data), 11);
    ASSERT_EQ(*reinterpret_cast<const int64_t*>(column_fixed6->get_data_at(1).data), 22);
    ASSERT_EQ(*reinterpret_cast<const int64_t*>(column_fixed6->get_data_at(2).data), 33);
    column_fixed6->replace_column_data(*column_fixed5, 2, 0);
    ASSERT_EQ(*reinterpret_cast<const int64_t*>(column_fixed6->get_data_at(0).data), 33);
    ASSERT_EQ(*reinterpret_cast<const int64_t*>(column_fixed6->get_data_at(1).data), 22);
    ASSERT_EQ(*reinterpret_cast<const int64_t*>(column_fixed6->get_data_at(2).data), 33);
    std::cout << "10. test replace_column_data data success" << std::endl;

    vectorized::IColumn::Filter filter {0, 1, 0};
    ASSERT_EQ(column_fixed5->clone()->filter(filter), 1);
    auto column_filter_res = column_fixed6->filter(filter, 0);
    ASSERT_EQ(column_filter_res->size(), 1);
    ASSERT_EQ(*reinterpret_cast<const int64_t*>(column_filter_res->get_data_at(0).data), 22);
    std::cout << "11. test filter data success" << std::endl;

    IColumn::Permutation perm {2, 1, 0};
    auto column_permute = column_fixed6->permute(perm, 3);
    ASSERT_EQ(column_permute->size(), 3);
    ASSERT_EQ(*reinterpret_cast<const int64_t*>(column_permute->get_data_at(0).data), 33);
    ASSERT_EQ(*reinterpret_cast<const int64_t*>(column_permute->get_data_at(1).data), 22);
    ASSERT_EQ(*reinterpret_cast<const int64_t*>(column_permute->get_data_at(2).data), 33);
    std::cout << "12. test permute data success" << std::endl;

    Arena arena;
    const char* pos = nullptr;
    StringRef key(pos, 0);
    for (int i = 0; i < 3; ++i) {
        auto cur_ref = column_fixed6->serialize_value_into_arena(i, arena, pos);
        key.data = cur_ref.data - key.size;
        key.size += cur_ref.size;
    }
    ASSERT_EQ(key.size, 24);
    ASSERT_EQ(*reinterpret_cast<const int64_t*>(key.data), 33);
    std::cout << "14. test serialize_value_into_arena data success" << std::endl;

    auto column_fixed7 = ColumnFixedLengthObject::create(sizeof(int64_t));

    const char* begin = key.data;
    for (size_t i = 0; i < 3; ++i) {
        begin = column_fixed7->deserialize_and_insert_from_arena(begin);
    }
    ASSERT_EQ(column_fixed7->size(), 3);
    ASSERT_EQ(*reinterpret_cast<const int64_t*>(column_fixed7->get_data_at(0).data), 33);
    ASSERT_EQ(*reinterpret_cast<const int64_t*>(column_fixed7->get_data_at(1).data), 22);
    ASSERT_EQ(*reinterpret_cast<const int64_t*>(column_fixed7->get_data_at(2).data), 33);
    std::cout << "15. test deserialize_and_insert_from_arena data success" << std::endl;
}
} // namespace doris::vectorized
