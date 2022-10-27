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

#include "vec/core/block.h"
#include "vec/core/types.h"
#define private public
#include "olap/tablet_schema.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_bitmap.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/jsonb/serialize.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris::vectorized {

void fill_block_with_array_int(vectorized::Block& block) {
    auto off_column = vectorized::ColumnVector<vectorized::ColumnArray::Offset64>::create();
    auto data_column = vectorized::ColumnVector<int32_t>::create();
    // init column array with [[1,2,3],[],[4],[5,6]]
    std::vector<vectorized::ColumnArray::Offset64> offs = {0, 3, 3, 4, 6};
    std::vector<int32_t> vals = {1, 2, 3, 4, 5, 6};
    for (size_t i = 1; i < offs.size(); ++i) {
        off_column->insert_data((const char*)(&offs[i]), 0);
    }
    for (auto& v : vals) {
        data_column->insert_data((const char*)(&v), 0);
    }

    auto column_array_ptr =
            vectorized::ColumnArray::create(std::move(data_column), std::move(off_column));
    vectorized::DataTypePtr nested_type(std::make_shared<vectorized::DataTypeInt32>());
    vectorized::DataTypePtr array_type(std::make_shared<vectorized::DataTypeArray>(nested_type));
    vectorized::ColumnWithTypeAndName test_array_int(std::move(column_array_ptr), array_type,
                                                     "test_array_int");
    block.insert(test_array_int);
}

void fill_block_with_array_string(vectorized::Block& block) {
    auto off_column = vectorized::ColumnVector<vectorized::ColumnArray::Offset64>::create();
    auto data_column = vectorized::ColumnString::create();
    // init column array with [["abc","de"],["fg"],[], [""]];
    std::vector<vectorized::ColumnArray::Offset64> offs = {0, 2, 3, 3, 4};
    std::vector<std::string> vals = {"abc", "de", "fg", ""};
    for (size_t i = 1; i < offs.size(); ++i) {
        off_column->insert_data((const char*)(&offs[i]), 0);
    }
    for (auto& v : vals) {
        data_column->insert_data(v.data(), v.size());
    }

    auto column_array_ptr =
            vectorized::ColumnArray::create(std::move(data_column), std::move(off_column));
    vectorized::DataTypePtr nested_type(std::make_shared<vectorized::DataTypeString>());
    vectorized::DataTypePtr array_type(std::make_shared<vectorized::DataTypeArray>(nested_type));
    vectorized::ColumnWithTypeAndName test_array_string(std::move(column_array_ptr), array_type,
                                                        "test_array_string");
    block.insert(test_array_string);
}

TEST(BlockSerializeTest, Array) {
    TabletSchema schema;
    TabletColumn c1;
    TabletColumn c2;
    c1.set_name("k1");
    c1.set_unique_id(1);
    c1.set_type(OLAP_FIELD_TYPE_ARRAY);
    c2.set_name("k2");
    c2.set_unique_id(2);
    c2.set_type(OLAP_FIELD_TYPE_ARRAY);
    schema.append_column(c1);
    schema.append_column(c2);
    // array int and array string
    vectorized::Block block;
    fill_block_with_array_int(block);
    fill_block_with_array_string(block);
    MutableColumnPtr col = ColumnString::create();
    // serialize
    JsonbSerializeUtil::block_to_jsonb(schema, block, static_cast<ColumnString&>(*col.get()),
                                       block.columns());
    // deserialize
    TupleDescriptor read_desc(PTupleDescriptor(), true);
    // slot1
    PSlotDescriptor pslot1;
    pslot1.set_col_name("k1");
    TypeDescriptor type_desc(TYPE_ARRAY);
    type_desc.children.push_back(TypeDescriptor(TYPE_INT));
    type_desc.to_protobuf(pslot1.mutable_slot_type());
    pslot1.set_col_unique_id(1);
    SlotDescriptor* slot = new SlotDescriptor(pslot1);
    read_desc.add_slot(slot);

    // slot2
    PSlotDescriptor pslot2;
    pslot2.set_col_name("k2");
    TypeDescriptor type_desc2(TYPE_ARRAY);
    type_desc2.children.push_back(TypeDescriptor(TYPE_STRING));
    type_desc2.to_protobuf(pslot2.mutable_slot_type());
    pslot2.set_col_unique_id(2);
    SlotDescriptor* slot2 = new SlotDescriptor(pslot2);
    read_desc.add_slot(slot2);

    Block new_block = block.clone_empty();
    JsonbSerializeUtil::jsonb_to_block(read_desc, static_cast<ColumnString&>(*col.get()),
                                       new_block);
    std::cout << block.dump_data() << std::endl;
    std::cout << new_block.dump_data() << std::endl;
    EXPECT_EQ(block.dump_data(), new_block.dump_data());
}

TEST(BlockSerializeTest, JsonbBlock) {
    vectorized::Block block;
    TabletSchema schema;
    std::vector<std::tuple<std::string, FieldType, int, PrimitiveType>> cols {
            {"k1", OLAP_FIELD_TYPE_INT, 1, TYPE_INT},
            {"k2", OLAP_FIELD_TYPE_STRING, 2, TYPE_STRING},
            {"k3", OLAP_FIELD_TYPE_DECIMAL128, 3, TYPE_DECIMAL128},
            {"k4", OLAP_FIELD_TYPE_STRING, 4, TYPE_STRING},
            {"k5", OLAP_FIELD_TYPE_DECIMAL128, 5, TYPE_DECIMAL128},
            {"k6", OLAP_FIELD_TYPE_INT, 6, TYPE_INT},
            {"k9", OLAP_FIELD_TYPE_DATEV2, 9, TYPE_DATEV2}};
    for (auto t : cols) {
        TabletColumn c;
        c.set_name(std::get<0>(t));
        c.set_type(std::get<1>(t));
        c.set_unique_id(std::get<2>(t));
        schema.append_column(c);
    }
    // int
    {
        auto vec = vectorized::ColumnVector<Int32>::create();
        auto& data = vec->get_data();
        for (int i = 0; i < 1024; ++i) {
            data.push_back(i);
        }
        vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeInt32>());
        vectorized::ColumnWithTypeAndName type_and_name(vec->get_ptr(), data_type, "test_int");
        block.insert(type_and_name);
    }
    // string
    {
        auto strcol = vectorized::ColumnString::create();
        for (int i = 0; i < 1024; ++i) {
            std::string is = std::to_string(i);
            strcol->insert_data(is.c_str(), is.size());
        }
        vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeString>());
        vectorized::ColumnWithTypeAndName type_and_name(strcol->get_ptr(), data_type,
                                                        "test_string");
        block.insert(type_and_name);
    }
    // decimal
    {
        vectorized::DataTypePtr decimal_data_type(doris::vectorized::create_decimal(27, 9));
        auto decimal_column = decimal_data_type->create_column();
        auto& data = ((vectorized::ColumnDecimal<vectorized::Decimal<vectorized::Int128>>*)
                              decimal_column.get())
                             ->get_data();
        for (int i = 0; i < 1024; ++i) {
            __int128_t value = i * pow(10, 9) + i * pow(10, 8);
            data.push_back(value);
        }
        vectorized::ColumnWithTypeAndName type_and_name(decimal_column->get_ptr(),
                                                        decimal_data_type, "test_decimal");
        block.insert(type_and_name);
    }
    // nullable string
    {
        vectorized::DataTypePtr string_data_type(std::make_shared<vectorized::DataTypeString>());
        vectorized::DataTypePtr nullable_data_type(
                std::make_shared<vectorized::DataTypeNullable>(string_data_type));
        auto nullable_column = nullable_data_type->create_column();
        ((vectorized::ColumnNullable*)nullable_column.get())->insert_null_elements(1024);
        vectorized::ColumnWithTypeAndName type_and_name(nullable_column->get_ptr(),
                                                        nullable_data_type, "test_nullable");
        block.insert(type_and_name);
    }
    // nullable decimal
    {
        vectorized::DataTypePtr decimal_data_type(doris::vectorized::create_decimal(27, 9));
        vectorized::DataTypePtr nullable_data_type(
                std::make_shared<vectorized::DataTypeNullable>(decimal_data_type));
        auto nullable_column = nullable_data_type->create_column();
        ((vectorized::ColumnNullable*)nullable_column.get())->insert_null_elements(1024);
        vectorized::ColumnWithTypeAndName type_and_name(
                nullable_column->get_ptr(), nullable_data_type, "test_nullable_decimal");
        block.insert(type_and_name);
    }
    // int with 1024 batch size
    {
        auto column_vector_int32 = vectorized::ColumnVector<Int32>::create();
        auto column_nullable_vector = vectorized::make_nullable(std::move(column_vector_int32));
        auto mutable_nullable_vector = std::move(*column_nullable_vector).mutate();
        for (int i = 0; i < 1024; i++) {
            mutable_nullable_vector->insert(vectorized::cast_to_nearest_field_type(i));
        }
        auto data_type = vectorized::make_nullable(std::make_shared<vectorized::DataTypeInt32>());
        vectorized::ColumnWithTypeAndName type_and_name(mutable_nullable_vector->get_ptr(),
                                                        data_type, "test_nullable_int32");
        block.insert(type_and_name);
    }
    // fill with datev2
    {
        auto column_vector_date_v2 = vectorized::ColumnVector<vectorized::UInt32>::create();
        auto& date_v2_data = column_vector_date_v2->get_data();
        for (int i = 0; i < 1024; ++i) {
            vectorized::DateV2Value<doris::vectorized::DateV2ValueType> value;
            value.from_date((uint32_t)((2022 << 9) | (6 << 5) | 6));
            date_v2_data.push_back(*reinterpret_cast<vectorized::UInt32*>(&value));
        }
        vectorized::DataTypePtr date_v2_type(std::make_shared<vectorized::DataTypeDateV2>());
        vectorized::ColumnWithTypeAndName test_date_v2(column_vector_date_v2->get_ptr(),
                                                       date_v2_type, "test_datev2");
        block.insert(test_date_v2);
    }
    MutableColumnPtr col = ColumnString::create();
    // serialize
    JsonbSerializeUtil::block_to_jsonb(schema, block, static_cast<ColumnString&>(*col.get()),
                                       block.columns());
    // deserialize
    TupleDescriptor read_desc(PTupleDescriptor(), true);
    for (auto t : cols) {
        PSlotDescriptor pslot;
        pslot.set_col_name(std::get<0>(t));
        if (std::get<3>(t) == TYPE_DECIMAL128) {
            TypeDescriptor type_desc(std::get<3>(t));
            type_desc.precision = 27;
            type_desc.scale = 9;
            type_desc.to_protobuf(pslot.mutable_slot_type());
        } else {
            TypeDescriptor type_desc(std::get<3>(t));
            type_desc.to_protobuf(pslot.mutable_slot_type());
        }
        pslot.set_col_unique_id(std::get<2>(t));
        SlotDescriptor* slot = new SlotDescriptor(pslot);
        read_desc.add_slot(slot);
    }
    Block new_block = block.clone_empty();
    JsonbSerializeUtil::jsonb_to_block(read_desc, static_cast<const ColumnString&>(*col.get()),
                                       new_block);
    std::cout << block.dump_data() << std::endl;
    std::cout << new_block.dump_data() << std::endl;
    EXPECT_EQ(block.dump_data(), new_block.dump_data());
}
} // namespace doris::vectorized