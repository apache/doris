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
#pragma once

#include "vec/columns/column_array.h"
#include "vec/data_types/data_type_array.h"
#include "vec/functions/function.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/data_types/data_type.h"

#include "vec/columns/column_const.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"
namespace doris::vectorized {

class FunctionHasAll: public IFunction {
public:
    static constexpr auto name = "has_all";
    using NullMapType = PaddedPODArray<UInt8>;
    String get_name() const override { return name; }
    bool is_variadic() const override { return false; }
    size_t get_number_of_arguments() const override { return 2; }
    static FunctionPtr create() { return std::make_shared<FunctionHasAll>(); }
    //bool is_suitable_for_short_circuit_arguments_execution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    DataTypePtr get_return_type_impl(const DataTypes & arguments) const override{
       return std::make_shared<DataTypeUInt8>();
    }
    template <typename NestedColumnType>
    void execute_has_all_string(const IColumn& src1_column,const IColumn& src2_column,const NullMapType* src1_null_map,
        const NullMapType* src2_null_map,ColumnUInt8::Container& result,const ColumnArray::Offsets64& offsets1,const ColumnArray::Offsets64& offsets2){
            
            const ColumnString* src1_data_concrete = reinterpret_cast<const ColumnString*>(&src1_column);
            const ColumnString* src2_data_concrete = reinterpret_cast<const ColumnString*>(&src2_column);
            ColumnArray::Offset64 src_offsets_size = offsets1.size();
            size_t pos_1 = 0;
            size_t pos_2 = 0;
            for (auto m=0;m<src_offsets_size;++m){
                auto offset_1=offsets1[m];
                auto offset_2=offsets2[m];
                bool res=true;
                //check src1_null_map is or not has true
                bool src1_null_flag=false;
                for(size_t n = pos_1; n < offset_1; ++n){
                    if(src1_null_map && (*src1_null_map)[n]){
                        src1_null_flag=true;
                        break;
                    }
                }
                for (size_t i = pos_2; i < offset_2; ++i){
                    for (size_t j = pos_1; j < offset_1; ++j){
                        if(src2_null_map&&(*src2_null_map)[i]&&src1_null_flag){
                            break;
                        }
                        if(src2_null_map&&(*src2_null_map)[i]&&!src1_null_flag){
                            res=false;
                            break;
                        }
                        if(src1_null_map&&(*src1_null_map)[j]){
                            break;
                        }
                        if(src1_data_concrete->get_data_at(j)==src2_data_concrete->get_data_at(i)){
                                break;
                        }
                        if(j==offset_1-1){
                                res=false;
                                break;
                        }
                    }
                    if(res==false)
                        break;
                }
                result[m]=res;
                res=true;
                pos_1=offset_1;
                pos_2=offset_2;
            }
    }
    template <typename NestedColumnType>
    void execute_has_all_num(const IColumn& src1_column,const IColumn& src2_column,const NullMapType* src1_null_map,
        const NullMapType* src2_null_map,ColumnUInt8::Container& result,const ColumnArray::Offsets64& offsets1,const ColumnArray::Offsets64& offsets2){
        using NestType = typename NestedColumnType::value_type;
        const NestedColumnType* src1_data_concrete = reinterpret_cast<const NestedColumnType*>(&src1_column);
        if (!src1_data_concrete) {
            return;
        }
        const NestedColumnType* src2_data_concrete = reinterpret_cast<const NestedColumnType*>(&src2_column);
        if (!src2_data_concrete) {
            return;
        }
        const PaddedPODArray<NestType>& src1_datas = src1_data_concrete->get_data();
        const PaddedPODArray<NestType>& src2_datas = src2_data_concrete->get_data();
        size_t pos_1 = 0;
        size_t pos_2 = 0;
        for (auto m=0;m<offsets1.size();++m){
            auto offset_1=offsets1[m];
            auto offset_2=offsets2[m];
            bool res=true;
            //check src1_null_map is or not has true
            bool src1_null_flag=false;
            for(size_t n = pos_1; n < offset_1; ++n){
                if(src1_null_map && (*src1_null_map)[n]){
                    src1_null_flag=true;
                    break;
                }
            }
            for (size_t i = pos_2; i < offset_2; ++i){
                for (size_t j = pos_1; j < offset_1; ++j){
                    if(src2_null_map&&(*src2_null_map)[i]&&src1_null_flag){
                        break;
                    }
                    if(src2_null_map&&(*src2_null_map)[i]&&!src1_null_flag){
                        res=false;
                        break;
                    }
                    if(src1_datas[j]==src2_datas[i]){
                            break;
                        }
                        else if(j==offset_1-1){
                            res=false;
                            break;
                        }
                }
                if(res==false)
                    break;
            }
            result[m]=res;
            res=true;
            pos_1=offset_1;
            pos_2=offset_2;
        }
    }
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override{
        ColumnPtr src1_column =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const auto& src1_column_array = check_and_get_column<ColumnArray>(*src1_column);
        const auto& offsets1 = src1_column_array->get_offsets();
        const auto* src1_nested_column = &src1_column_array->get_data();

         ColumnPtr src2_column =
                block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();
        const auto& src2_column_array = check_and_get_column<ColumnArray>(*src2_column);
        const auto& offsets2 = src2_column_array->get_offsets();
        const auto* src2_nested_column = &src2_column_array->get_data();
        
        DataTypePtr src1_column_type = block.get_by_position(arguments[0]).type;
        auto nested1_type = assert_cast<const DataTypeArray&>(*src1_column_type).get_nested_type();
        DataTypePtr src2_column_type = block.get_by_position(arguments[1]).type;
        auto nested2_type = assert_cast<const DataTypeArray&>(*src2_column_type).get_nested_type();

        const NullMapType* nested_null_map_1 = nullptr;
        if (src1_nested_column->is_nullable()) {
            const ColumnNullable* nested_null_column_1 =
                    check_and_get_column<ColumnNullable>(*src1_nested_column);
            src1_nested_column = nested_null_column_1->get_nested_column_ptr();
            nested_null_map_1 = &nested_null_column_1->get_null_map_column().get_data();
        } 
       
        const NullMapType* nested_null_map_2 = nullptr;
        if (src2_nested_column->is_nullable()) {
            const ColumnNullable* nested_null_column_2 =
                    check_and_get_column<ColumnNullable>(*src2_nested_column);
            src2_nested_column = nested_null_column_2->get_nested_column_ptr();
            nested_null_map_2 = &nested_null_column_2->get_null_map_column().get_data();
        } 

        //return value
        auto res = ColumnUInt8::create();
        ColumnUInt8::Container& vec_res = res->get_data();
        // set default value to 0, and match functions only need to set 1/true
        vec_res.resize_fill(input_rows_count); //default 0
        if(nested1_type->get_type_id()!=nested2_type->get_type_id()){
            block.replace_by_position(result, std::move(res));
            return Status::OK();
        }

        if (check_column<ColumnUInt8>(*src1_nested_column)) {
            execute_has_all_num<ColumnUInt8>(*src1_nested_column,*src2_nested_column,nested_null_map_1,
                                               nested_null_map_2,vec_res,offsets1,offsets2);
        } else if (check_column<ColumnInt8>(*src1_nested_column)) {
            execute_has_all_num<ColumnInt8>(*src1_nested_column,*src2_nested_column,nested_null_map_1,
                                               nested_null_map_2,vec_res,offsets1,offsets2);
        } else if (check_column<ColumnInt16>(*src1_nested_column)) {
            execute_has_all_num<ColumnInt16>(*src1_nested_column,*src2_nested_column,nested_null_map_1,
                                               nested_null_map_2,vec_res,offsets1,offsets2);
        } else if (check_column<ColumnInt32>(*src1_nested_column)) {
            execute_has_all_num<ColumnInt32>(*src1_nested_column,*src2_nested_column,nested_null_map_1,
                                               nested_null_map_2,vec_res,offsets1,offsets2);
        } else if (check_column<ColumnInt64>(*src1_nested_column)) {
            execute_has_all_num<ColumnInt64>(*src1_nested_column,*src2_nested_column,nested_null_map_1,
                                               nested_null_map_2,vec_res,offsets1,offsets2);
        } else if (check_column<ColumnInt128>(*src1_nested_column)) {
            execute_has_all_num<ColumnInt128>(*src1_nested_column,*src2_nested_column,nested_null_map_1,
                                               nested_null_map_2,vec_res,offsets1,offsets2);
        } 
        else if (check_column<ColumnFloat32>(*src1_nested_column)) {
            execute_has_all_num<ColumnFloat32>(*src1_nested_column,*src2_nested_column,nested_null_map_1,
                                               nested_null_map_2,vec_res,offsets1,offsets2);
        } else if (check_column<ColumnFloat64>(*src1_nested_column)) {
            execute_has_all_num<ColumnFloat64>(*src1_nested_column,*src2_nested_column,nested_null_map_1,
                                               nested_null_map_2,vec_res,offsets1,offsets2);
        } 
        else if (check_column<ColumnDecimal128>(*src1_nested_column)) {
            execute_has_all_num<ColumnDecimal128>(*src1_nested_column,*src2_nested_column,nested_null_map_1,
                                               nested_null_map_2,vec_res,offsets1,offsets2);
        } 
        else if (check_column<ColumnString>(*src1_nested_column)) {
            execute_has_all_string<ColumnString>(*src1_nested_column,*src2_nested_column,nested_null_map_1,
                                               nested_null_map_2,vec_res,offsets1,offsets2);
        }
        block.replace_by_position(result, std::move(res));
        return Status::OK();
    }
    bool use_default_implementation_for_constants() const override { return true; }
};
}