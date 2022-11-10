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
    bool impl(const NestedColumnType* __restrict src1, const NestedColumnType* __restrict src2, size_t begin_1,
                     size_t end_1, size_t begin_2,size_t end_2){
        for (size_t i = begin_2; i < end_2; ++i){
            for (size_t j = begin_1; j < end_1; ++j){
                //std::cout<<(src1->get_data()).size()<<std::endl;
                const auto & d1=src1->get_element(j);
               // std::cout<<int(d1)<<std::endl;
                const auto & d2=src2->get_element(i);
               // std::cout<<int(d2)<<std::endl;
                 if(d1==d2){
                        break;
                    }
                    else if(j==end_1-1){
                        return false;
                    }
            }
        }
        return true;
     }
    template <typename NestedColumnType>
    void execute_has_all(const IColumn& src1_column,const IColumn& src2_column,ColumnPtr src1_null_map,
        ColumnPtr src2_null_map,ColumnUInt8::Container& result,const ColumnArray::Offsets64& offsets1,const ColumnArray::Offsets64& offsets2){
        // check array nested column type and get data
        using ColVecType = ColumnVectorOrDecimal<NestedColumnType>;
        const auto& src1_data = reinterpret_cast<const ColVecType&>(src1_column).get_data();
        const auto& src2_data = reinterpret_cast<const ColVecType&>(src2_column).get_data();

        size_t pos_1 = 0;
        size_t pos_2 = 0;
        for (auto i=0;i<offsets1.size();++i){
            auto offset_1=offsets1[i];
            auto offset_2=offsets2[i];
            std::cout<<((src1_data.data())->get_data()).size()<<std::endl;
            bool res=impl(src1_data.data(), src2_data.data(), pos_1,
                     offset_1, pos_2,offset_2);
            pos_1=offset_1;
            pos_2=offset_2;
            result[i]=res;
        }
    }
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override{
        ColumnPtr src1_column =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const auto& src1_column_array = reinterpret_cast<const ColumnArray&>(*src1_column);
       // const auto& src1_column_array = check_and_get_column<ColumnArray>(*src1_column);
        const auto& offsets1 = src1_column_array.get_offsets();

         ColumnPtr src2_column =
                block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();
        const auto& src2_column_array = reinterpret_cast<const ColumnArray&>(*src2_column);
        //const auto& src2_column_array = check_and_get_column<ColumnArray>(*src2_column);
        const auto& offsets2 = src2_column_array.get_offsets();
        
        ColumnPtr nested_column_1 = nullptr;
        ColumnPtr nested_null_map_1 = nullptr;
        if (is_column_nullable(src1_column_array.get_data())) {
            const auto& nested_null_column_1 =
                    reinterpret_cast<const ColumnNullable&>(src1_column_array.get_data());
            nested_column_1 = nested_null_column_1.get_nested_column_ptr();
            nested_null_map_1 = nested_null_column_1.get_null_map_column_ptr();
        } else {
            nested_column_1 = src1_column_array.get_data_ptr();
        }

        ColumnPtr nested_column_2 = nullptr;
        ColumnPtr nested_null_map_2 = nullptr;
        if (is_column_nullable(src2_column_array.get_data())) {
            const auto& nested_null_column_2 =
                    reinterpret_cast<const ColumnNullable&>(src2_column_array.get_data());
            nested_column_2 = nested_null_column_2.get_nested_column_ptr();
            nested_null_map_2 = nested_null_column_2.get_null_map_column_ptr();
        } else {
            nested_column_2 = src2_column_array.get_data_ptr();
        }

        //return value
        auto res = ColumnUInt8::create();
        ColumnUInt8::Container& vec_res = res->get_data();
        // set default value to 0, and match functions only need to set 1/true
        vec_res.resize_fill(input_rows_count);

       
        // execute_has_all(const IColumn& src1_column,const IColumn& src2_column,const NullMapType* src1_null_map,
        // const NullMapType* src2_null_map,ColumnUInt8::Container& result,
        // const ColumnArray::Offsets64& offsets1,const ColumnArray::Offsets64& offsets2)
        if (check_column<ColumnUInt8>(*nested_column_1)) {
            execute_has_all<ColumnUInt8>(*nested_column_1,*nested_column_2,nested_null_map_1,
                                               nested_null_map_2,vec_res,offsets1,offsets2);
        } else if (check_column<ColumnInt8>(*nested_column_1)) {
            execute_has_all<ColumnInt8>(*nested_column_1,*nested_column_2,nested_null_map_1,
                                               nested_null_map_2,vec_res,offsets1,offsets2);
        } else if (check_column<ColumnInt16>(*nested_column_1)) {
            execute_has_all<ColumnInt16>(*nested_column_1,*nested_column_2,nested_null_map_1,
                                               nested_null_map_2,vec_res,offsets1,offsets2);
        } else if (check_column<ColumnInt32>(*nested_column_1)) {
            execute_has_all<ColumnInt32>(*nested_column_1,*nested_column_2,nested_null_map_1,
                                               nested_null_map_2,vec_res,offsets1,offsets2);
        } else if (check_column<ColumnInt64>(*nested_column_1)) {
            execute_has_all<ColumnInt64>(*nested_column_1,*nested_column_2,nested_null_map_1,
                                               nested_null_map_2,vec_res,offsets1,offsets2);
        } else if (check_column<ColumnInt128>(*nested_column_1)) {
            execute_has_all<ColumnInt128>(*nested_column_1,*nested_column_2,nested_null_map_1,
                                               nested_null_map_2,vec_res,offsets1,offsets2);
        } 
        else if (check_column<ColumnFloat32>(*nested_column_1)) {
            execute_has_all<ColumnFloat32>(*nested_column_1,*nested_column_2,nested_null_map_1,
                                               nested_null_map_2,vec_res,offsets1,offsets2);
        } else if (check_column<ColumnFloat64>(*nested_column_1)) {
            execute_has_all<ColumnFloat64>(*nested_column_1,*nested_column_2,nested_null_map_1,
                                               nested_null_map_2,vec_res,offsets1,offsets2);
        } 
        // else if (check_column<ColumnDecimal128>(*nested_column_1)) {
        //     execute_has_all<ColumnDecimal128>(*nested_column_1,*nested_column_2,nested_null_map_1,
        //                                        nested_null_map_2,vec_res,offsets1,offsets2);
        // } 
        // else if (check_column<ColumnString>(*src1_nested_column)) {
        //     execute_has_all<ColumnString>(*src1_nested_column,*src2_nested_column,src1_null_map,
        //                                        src2_null_map,vec_res,offsets1,offsets2);
        // }
        block.replace_by_position(result, std::move(res));
        return Status::OK();
    }
    bool use_default_implementation_for_constants() const override { return true; }
};
}