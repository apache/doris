
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

#include <arrow/array/builder_base.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_decimal.h>
#include <arrow/array/builder_nested.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/util/decimal.h>
#include <arrow/visit_type_inline.h>
#include <arrow/visitor.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/types.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <math.h>
#include <stdint.h>
#include <stdlib.h>
#include <time.h>

#include <iostream>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "gtest/gtest_pred_impl.h"
#include "olap/hll.h"
#include "runtime/descriptors.cpp"
#include "runtime/descriptors.h"
#include "util/arrow/block_convertor.h"
#include "util/arrow/row_batch.h"
#include "util/bitmap_value.h"
#include "util/quantile_state.h"
#include "vec/columns/column.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/core/block.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_bitmap.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_hll.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_quantilestate.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_time_v2.h"
#include "vec/runtime/vdatetime_value.h"
#include "vec/utils/arrow_column_to_doris_column.h"
namespace doris::vectorized {

void serialize_and_deserialize_arrow_test() {
    vectorized::Block block;
    std::vector<std::tuple<std::string, FieldType, int, PrimitiveType, bool>> cols {
            {"k1", FieldType::OLAP_FIELD_TYPE_INT, 1, TYPE_INT, false},
            {"k2", FieldType::OLAP_FIELD_TYPE_STRING, 2, TYPE_STRING, false},
            {"k3", FieldType::OLAP_FIELD_TYPE_DECIMAL128I, 3, TYPE_DECIMAL128I, false},
            {"k9", FieldType::OLAP_FIELD_TYPE_DATEV2, 9, TYPE_DATEV2, false},
            {"k4", FieldType::OLAP_FIELD_TYPE_BOOL, 4, TYPE_BOOLEAN, false}};

    // make desc and generate block
    TupleDescriptor tuple_desc(PTupleDescriptor(), true);
    for (auto t : cols) {
        TSlotDescriptor tslot;
        std::string col_name = std::get<0>(t);
        tslot.__set_colName(col_name);
        TypeDescriptor type_desc(std::get<3>(t));
        switch (std::get<3>(t)) {
        case TYPE_BOOLEAN:
            tslot.__set_slotType(type_desc.to_thrift());
            {
                auto vec = vectorized::ColumnVector<UInt8>::create();
                auto& data = vec->get_data();
                for (int i = 0; i < 7; ++i) {
                    data.push_back(i % 2);
                }
                vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeUInt8>());
                vectorized::ColumnWithTypeAndName type_and_name(vec->get_ptr(), data_type,
                                                                col_name);
                block.insert(std::move(type_and_name));
            }
            break;
        case TYPE_INT:
            tslot.__set_slotType(type_desc.to_thrift());
            {
                auto vec = vectorized::ColumnVector<Int32>::create();
                auto& data = vec->get_data();
                for (int i = 0; i < 7; ++i) {
                    data.push_back(i);
                }
                vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeInt32>());
                vectorized::ColumnWithTypeAndName type_and_name(vec->get_ptr(), data_type,
                                                                col_name);
                block.insert(std::move(type_and_name));
            }
            break;
        case TYPE_DECIMAL128I:
            type_desc.precision = 27;
            type_desc.scale = 9;
            tslot.__set_slotType(type_desc.to_thrift());
            {
                vectorized::DataTypePtr decimal_data_type(
                        doris::vectorized::create_decimal(27, 9, true));
                auto decimal_column = decimal_data_type->create_column();
                auto& data = ((vectorized::ColumnDecimal<vectorized::Decimal<vectorized::Int128>>*)
                                      decimal_column.get())
                                     ->get_data();
                for (int i = 0; i < 7; ++i) {
                    __int128_t value = i * pow(10, 9) + i * pow(10, 8);
                    data.push_back(value);
                }
                vectorized::ColumnWithTypeAndName type_and_name(decimal_column->get_ptr(),
                                                                decimal_data_type, col_name);
                block.insert(type_and_name);
            }
            break;
        case TYPE_STRING:
            tslot.__set_slotType(type_desc.to_thrift());
            {
                auto strcol = vectorized::ColumnString::create();
                for (int i = 0; i < 7; ++i) {
                    std::string is = std::to_string(i);
                    strcol->insert_data(is.c_str(), is.size());
                }
                vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeString>());
                vectorized::ColumnWithTypeAndName type_and_name(strcol->get_ptr(), data_type,
                                                                col_name);
                block.insert(type_and_name);
            }
            break;
        case TYPE_HLL:
            tslot.__set_slotType(type_desc.to_thrift());
            {
                vectorized::DataTypePtr hll_data_type(std::make_shared<vectorized::DataTypeHLL>());
                auto hll_column = hll_data_type->create_column();
                std::vector<HyperLogLog>& container =
                        ((vectorized::ColumnHLL*)hll_column.get())->get_data();
                for (int i = 0; i < 7; ++i) {
                    HyperLogLog hll;
                    hll.update(i);
                    container.push_back(hll);
                }
                vectorized::ColumnWithTypeAndName type_and_name(hll_column->get_ptr(),
                                                                hll_data_type, col_name);

                block.insert(type_and_name);
            }
            break;
        case TYPE_DATEV2:
            tslot.__set_slotType(type_desc.to_thrift());
            {
                auto column_vector_date_v2 = vectorized::ColumnVector<vectorized::UInt32>::create();
                auto& date_v2_data = column_vector_date_v2->get_data();
                for (int i = 0; i < 7; ++i) {
                    vectorized::DateV2Value<doris::vectorized::DateV2ValueType> value;
                    value.from_date((uint32_t)((2022 << 9) | (6 << 5) | 6));
                    date_v2_data.push_back(*reinterpret_cast<vectorized::UInt32*>(&value));
                }
                vectorized::DataTypePtr date_v2_type(
                        std::make_shared<vectorized::DataTypeDateV2>());
                vectorized::ColumnWithTypeAndName test_date_v2(column_vector_date_v2->get_ptr(),
                                                               date_v2_type, col_name);
                block.insert(test_date_v2);
            }
            break;
        default:
            break;
        }

        tslot.__set_col_unique_id(std::get<2>(t));
        SlotDescriptor* slot = new SlotDescriptor(tslot);
        tuple_desc.add_slot(slot);
    }

    RowDescriptor row_desc(&tuple_desc, true);
    // arrow schema
    std::shared_ptr<arrow::Schema> _arrow_schema;
    EXPECT_EQ(convert_to_arrow_schema(row_desc, &_arrow_schema), Status::OK());

    // serialize
    std::shared_ptr<arrow::RecordBatch> result;
    std::cout << "block structure: " << block.dump_structure() << std::endl;
    std::cout << "_arrow_schema: " << _arrow_schema->ToString(true) << std::endl;

    convert_to_arrow_batch(block, _arrow_schema, arrow::default_memory_pool(), &result);

    std::string res_string;
    serialize_record_batch(*result.get(), &res_string);
    std::cout << "arrow result" << res_string << std::endl;

    Block new_block = block.clone_empty();
    // deserialize
    for (auto t : cols) {
        std::string real_column_name = std::get<0>(t);
        auto* array = result->GetColumnByName(real_column_name).get();
        auto& column_with_type_and_name = new_block.get_by_name(real_column_name);
        arrow_column_to_doris_column(array, 0, column_with_type_and_name.column,
                                     column_with_type_and_name.type, block.rows(), "UTC");
    }

    std::cout << block.dump_data() << std::endl;
    std::cout << new_block.dump_data() << std::endl;
    EXPECT_EQ(block.dump_data(), new_block.dump_data());
}

TEST(DataTypeSerDeArrowTest, DataTypeScalaSerDeTest) {
    serialize_and_deserialize_arrow_test();
}

} // namespace doris::vectorized
