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

#include "olap/column_vector.h"

#include <gtest/gtest.h>

#include "olap/field.h"
#include "olap/tablet_schema_helper.h"
#include "olap/types.cpp"
#include "runtime/collection_value.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"

namespace doris {

class ColumnVectorTest : public testing::Test {
public:
    ColumnVectorTest() : _pool(&_tracker) {}

protected:
    void SetUp() {}
    void TearDown() {}

private:
    MemTracker _tracker;
    MemPool _pool;
};

template <FieldType type>
void test_read_write_scalar_column_vector(const TypeInfo* type_info, const uint8_t* src_data,
                                          size_t data_size) {
    using Type = typename TypeTraits<type>::CppType;
    Type* src = (Type*)src_data;
    size_t TYPE_SIZE = sizeof(Type);

    size_t init_size = data_size / 2;
    std::unique_ptr<ColumnVectorBatch> cvb;
    ASSERT_TRUE(ColumnVectorBatch::create(init_size, true, type_info, nullptr, &cvb).ok());
    memcpy(cvb->mutable_cell_ptr(0), src, init_size * TYPE_SIZE);
    cvb->set_null_bits(0, init_size, false);
    ASSERT_TRUE(cvb->resize(data_size).ok());
    size_t second_write_size = data_size - init_size;
    memcpy(cvb->mutable_cell_ptr(init_size), src + init_size, second_write_size * TYPE_SIZE);
    cvb->set_null_bits(init_size, second_write_size, false);
    for (size_t idx = 0; idx < data_size; ++idx) {
        if (type_info->type() == OLAP_FIELD_TYPE_VARCHAR ||
            type_info->type() == OLAP_FIELD_TYPE_CHAR) {
            Slice* src_slice = (Slice*)src_data;

            ASSERT_EQ(src_slice[idx].to_string(),
                      reinterpret_cast<const Slice*>(cvb->cell_ptr(idx))->to_string())
                    << "idx:" << idx;
        } else {
            ASSERT_EQ(src[idx], *reinterpret_cast<const Type*>(cvb->cell_ptr(idx)));
        }
    }
}

template <FieldType item_type>
void test_read_write_array_column_vector(const TypeInfo* array_type_info, size_t array_size,
                                         CollectionValue* result) {
    DCHECK(array_size > 1);

    using ItemType = typename TypeTraits<item_type>::CppType;
    size_t ITEM_TYPE_SIZE = sizeof(ItemType);

    TabletColumn array_column(OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_ARRAY);
    TabletColumn item_column(OLAP_FIELD_AGGREGATION_NONE, item_type, true, 0, 0);
    array_column.add_sub_column(item_column);
    Field* field = FieldFactory::create(array_column);

    size_t array_init_size = array_size / 2;
    std::unique_ptr<ColumnVectorBatch> cvb;
    ASSERT_TRUE(
            ColumnVectorBatch::create(array_init_size, true, array_type_info, field, &cvb).ok());

    auto* array_cvb = reinterpret_cast<ArrayColumnVectorBatch*>(cvb.get());
    ColumnVectorBatch* item_cvb = array_cvb->elements();
    ColumnVectorBatch* offset_cvb = array_cvb->offsets();

    // first write
    for (size_t i = 0; i < array_init_size; ++i) {
        uint32_t len = result[i].length();
        memcpy(offset_cvb->mutable_cell_ptr(1 + i), &len, sizeof(uint32_t));
    }
    array_cvb->set_null_bits(0, array_init_size, false);
    array_cvb->get_offset_by_length(0, array_init_size);

    size_t first_write_item = array_cvb->item_offset(array_init_size) - array_cvb->item_offset(0);
    ASSERT_TRUE(item_cvb->resize(first_write_item).ok());
    for (size_t i = 0; i < array_init_size; ++i) {
        memcpy(item_cvb->mutable_cell_ptr(array_cvb->item_offset(i)), result[i].data(),
               result[i].length() * ITEM_TYPE_SIZE);
    }

    item_cvb->set_null_bits(0, first_write_item, false);
    array_cvb->prepare_for_read(0, array_init_size, false);

    // second write
    ASSERT_TRUE(array_cvb->resize(array_size).ok());
    for (int i = array_init_size; i < array_size; ++i) {
        uint32_t len = result[i].length();
        memcpy(offset_cvb->mutable_cell_ptr(i + 1), &len, sizeof(uint32_t));
    }
    array_cvb->set_null_bits(array_init_size, array_size - array_init_size, false);
    array_cvb->get_offset_by_length(array_init_size, array_size - array_init_size);

    size_t total_item_size = array_cvb->item_offset(array_size);
    ASSERT_TRUE(item_cvb->resize(total_item_size).ok());

    for (size_t i = array_init_size; i < array_size; ++i) {
        memcpy(item_cvb->mutable_cell_ptr(array_cvb->item_offset(i)), result[i].data(),
               result[i].length() * ITEM_TYPE_SIZE);
    }
    size_t second_write_item = total_item_size - first_write_item;
    item_cvb->set_null_bits(first_write_item, second_write_item, false);
    array_cvb->prepare_for_read(0, array_size, false);

    for (size_t idx = 0; idx < array_size; ++idx) {
        ASSERT_TRUE(array_type_info->equal(&result[idx], array_cvb->cell_ptr(idx)))
                << "idx:" << idx;
    }
    delete field;
}

TEST_F(ColumnVectorTest, scalar_column_vector_test) {
    {
        size_t size = 1024;
        auto* val = new uint8_t[size];
        for (int i = 0; i < size; ++i) {
            val[i] = i;
        }
        const auto* type_info = get_scalar_type_info<OLAP_FIELD_TYPE_TINYINT>();
        test_read_write_scalar_column_vector<OLAP_FIELD_TYPE_TINYINT>(type_info, val, size);
        delete[] val;
    }
    {
        size_t size = 1024;
        auto* char_vals = new Slice[size];
        for (int i = 0; i < size; ++i) {
            set_column_value_by_type(OLAP_FIELD_TYPE_CHAR, i, (char*)&char_vals[i], &_pool, 8);
        }
        const auto* ti = get_scalar_type_info<OLAP_FIELD_TYPE_CHAR>();
        test_read_write_scalar_column_vector<OLAP_FIELD_TYPE_CHAR>(ti, (uint8_t*)char_vals, size);
        delete[] char_vals;
    }
}

TEST_F(ColumnVectorTest, array_column_vector_test) {
    size_t num_array = 1024;
    size_t num_item = num_array * 3;
    {
        auto* array_val = new CollectionValue[num_array];
        bool null_signs[3] = {false, false, false};

        auto* item_val = new uint8_t[num_item];
        memset(null_signs, 0, sizeof(bool) * 3);
        for (int i = 0; i < num_item; ++i) {
            item_val[i] = i;
            if (i % 3 == 0) {
                size_t array_index = i / 3;
                array_val[array_index].set_data(&item_val[i]);
                array_val[array_index].set_null_signs(null_signs);
                array_val[array_index].set_length(3);
            }
        }
        const auto* type_info = get_collection_type_info<OLAP_FIELD_TYPE_TINYINT>();
        test_read_write_array_column_vector<OLAP_FIELD_TYPE_TINYINT>(type_info, num_array,
                                                                     array_val);

        // Test hash_code in CollectionValue
        type_info->hash_code(array_val, 0);
        delete[] array_val;
        delete[] item_val;
    }
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
