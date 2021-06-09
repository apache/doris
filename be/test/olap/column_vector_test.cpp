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

#include "olap/collection.h"
#include "olap/field.h"
#include "olap/tablet_schema_helper.h"
#include "olap/types.cpp"
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
void test_read_write_list_column_vector(const ArrayTypeInfo* list_type_info,
                                        segment_v2::ordinal_t* ordinals, // n + 1
                                        size_t list_size, const uint8_t* src_item_data,
                                        Collection* result) {
    DCHECK(list_size > 1);

    using ItemType = typename TypeTraits<item_type>::CppType;
    ItemType* src_item = (ItemType*)src_item_data;
    size_t ITEM_TYPE_SIZE = sizeof(ItemType);

    TabletColumn list_column(OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_ARRAY);
    TabletColumn item_column(OLAP_FIELD_AGGREGATION_NONE, item_type, true, 0, 0);
    list_column.add_sub_column(item_column);
    Field* field = FieldFactory::create(list_column);

    size_t list_init_size = list_size / 2;
    std::unique_ptr<ColumnVectorBatch> cvb;
    ASSERT_TRUE(ColumnVectorBatch::create(list_init_size, true, list_type_info, field, &cvb).ok());

    ArrayColumnVectorBatch* list_cvb = reinterpret_cast<ArrayColumnVectorBatch*>(cvb.get());
    ColumnVectorBatch* item_cvb = list_cvb->elements();

    // first write
    list_cvb->put_item_ordinal(ordinals, 0, list_init_size + 1);
    list_cvb->set_null_bits(0, list_init_size, false);
    size_t first_write_item = ordinals[list_init_size] - ordinals[0];
    ASSERT_TRUE(item_cvb->resize(first_write_item).ok());
    memcpy(item_cvb->mutable_cell_ptr(0), src_item, first_write_item * ITEM_TYPE_SIZE);
    item_cvb->set_null_bits(0, first_write_item, false);
    list_cvb->prepare_for_read(0, list_init_size, false);

    // second write
    ASSERT_TRUE(list_cvb->resize(list_size).ok());
    list_cvb->put_item_ordinal(ordinals + list_init_size, list_init_size,
                               list_size - list_init_size + 1);
    list_cvb->set_null_bits(list_init_size, list_size - list_init_size, false);
    size_t item_size = ordinals[list_size] - ordinals[0];
    ASSERT_TRUE(item_cvb->resize(item_size).ok());
    size_t second_write_item = item_size - first_write_item;
    memcpy(item_cvb->mutable_cell_ptr(first_write_item), src_item + first_write_item,
           second_write_item * ITEM_TYPE_SIZE);
    item_cvb->set_null_bits(first_write_item, second_write_item, false);
    list_cvb->prepare_for_read(0, list_size, false);

    for (size_t idx = 0; idx < list_size; ++idx) {
        ASSERT_TRUE(list_type_info->equal(&result[idx], list_cvb->cell_ptr(idx))) << "idx:" << idx;
    }
    delete field;
}

TEST_F(ColumnVectorTest, scalar_column_vector_test) {
    {
        size_t size = 1024;
        uint8_t* val = new uint8_t[size];
        for (int i = 0; i < size; ++i) {
            val[i] = i;
        }
        const TypeInfo* ti = get_scalar_type_info(OLAP_FIELD_TYPE_TINYINT);
        test_read_write_scalar_column_vector<OLAP_FIELD_TYPE_TINYINT>(ti, val, size);
        delete[] val;
    }
    {
        size_t size = 1024;
        Slice* char_vals = new Slice[size];
        for (int i = 0; i < size; ++i) {
            set_column_value_by_type(OLAP_FIELD_TYPE_CHAR, i, (char*)&char_vals[i], &_pool, 8);
        }
        const TypeInfo* ti = get_scalar_type_info(OLAP_FIELD_TYPE_CHAR);
        test_read_write_scalar_column_vector<OLAP_FIELD_TYPE_CHAR>(ti, (uint8_t*)char_vals, size);
        delete[] char_vals;
    }
}

TEST_F(ColumnVectorTest, list_column_vector_test) {
    size_t num_list = 1024;
    size_t num_item = num_list * 3;
    {
        Collection* list_val = new Collection[num_list];
        uint8_t* item_val = new uint8_t[num_item];
        segment_v2::ordinal_t* ordinals = new segment_v2::ordinal_t[num_list + 1];
        bool null_signs[3] = {false, false, false};
        memset(null_signs, 0, sizeof(bool) * 3);
        for (int i = 0; i < num_item; ++i) {
            item_val[i] = i;
            if (i % 3 == 0) {
                size_t list_index = i / 3;
                list_val[list_index].data = &item_val[i];
                list_val[list_index].null_signs = null_signs;
                list_val[list_index].length = 3;
                ordinals[list_index] = i;
            }
        }
        ordinals[num_list] = num_item;
        auto type_info = reinterpret_cast<ArrayTypeInfo*>(
                ArrayTypeInfoResolver::instance()->get_type_info(OLAP_FIELD_TYPE_TINYINT));
        test_read_write_list_column_vector<OLAP_FIELD_TYPE_TINYINT>(type_info, ordinals, num_list,
                                                                    item_val, list_val);

        delete[] ordinals;
        delete[] list_val;
        delete[] item_val;
    }
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
