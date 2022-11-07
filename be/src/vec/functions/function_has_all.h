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
    void execute_has_all(const IColumn& src1_column,const IColumn& src2_column,const NullMapType* src1_null_map,
        const NullMapType* src2_null_map,ColumnUInt8::Container& result,const ColumnArray::Offsets64& offsets1,const ColumnArray::Offsets64& offsets2){
        // check array nested column type and get data
        const auto& src1_data = reinterpret_cast<const NestedColumnType&>(src1_column).get_data();
        const auto& src2_data = reinterpret_cast<const NestedColumnType&>(src2_column).get_data();
       
       // const PaddedPODArray<NestType>& src1_datas = src1_data_concrete->get_data();
       // const PaddedPODArray<NestType>& src2_datas = src2_data_concrete->get_data();
        for (size_t row = 0; row < offsets2.size(); ++row) {
            size_t off1 = offsets1[row - 1];
            size_t len1 = offsets1[row] - off1;
            size_t off2 = offsets1[row - 1];
            size_t len2 = offsets1[row] - off2;
            if(len2==0){
                result[row]=1;
                continue;
            }
             if(src1_null_map&&(*src1_null_map)[row]&&src2_null_map&&(*src2_null_map)[row]){ //if the second is null
                result[row]=1;
                continue;
            }
            bool res=true;
            for (size_t pos2 = 0; pos2 < len2; ++pos2){
                for(size_t pos1=0;pos1<len1;++pos1){
                    if(src1_data[off1 + pos1]==src2_data[off2 + pos2]){
                        continue;
                    }
                    else if(pos1==len1-1){
                        res=false;
                        break;
                    }
                }
                result[row]=res;
                res=true;
            }
        }   
    }
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override{
        ColumnPtr src1_column =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const auto& src1_column_array = check_and_get_column<ColumnArray>(*src1_column);
        if (!src1_column_array) {
            return Status::RuntimeError(
                    fmt::format("unsupported types for function {}({})", get_name(),
                                block.get_by_position(arguments[0]).type->get_name()));
        }
        const auto& offsets1 = src1_column_array->get_offsets();

         ColumnPtr src2_column =
                block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();
        const auto& src2_column_array = check_and_get_column<ColumnArray>(*src2_column);
        if (!src2_column_array) {
            return Status::RuntimeError(
                    fmt::format("unsupported types for function {}({})", get_name(),
                                block.get_by_position(arguments[0]).type->get_name()));
        }
        const auto& offsets2 = src2_column_array->get_offsets();

        //real data(IColum)
        const auto* src1_nested_column = &src1_column_array->get_data();
        const auto* src2_nested_column = &src2_column_array->get_data();
        
        //return value
        auto res = ColumnUInt8::create();
        ColumnUInt8::Container& vec_res = res->get_data();
        // set default value to 0, and match functions only need to set 1/true
        vec_res.resize_fill(input_rows_count);

        //null_map
        const NullMapType* src1_null_map = nullptr;
        if (src1_nested_column->is_nullable()) {
            const ColumnNullable* src1_nested_nullable_col =
                    check_and_get_column<ColumnNullable>(*src1_nested_column);
            src1_nested_column = src1_nested_nullable_col->get_nested_column_ptr();
            src1_null_map = &src1_nested_nullable_col->get_null_map_column().get_data();
        }
        const NullMapType* src2_null_map = nullptr;
        if (src2_nested_column->is_nullable()) {
            const ColumnNullable* src2_nested_nullable_col =
                    check_and_get_column<ColumnNullable>(*src2_nested_column);
            src2_nested_column = src2_nested_nullable_col->get_nested_column_ptr();
            src2_null_map = &src2_nested_nullable_col->get_null_map_column().get_data();
        }

        // execute_has_all(const IColumn& src1_column,const IColumn& src2_column,const NullMapType* src1_null_map,
        // const NullMapType* src2_null_map,ColumnUInt8::Container& result,
        // const ColumnArray::Offsets64& offsets1,const ColumnArray::Offsets64& offsets2)
        if (check_column<ColumnUInt8>(*src1_nested_column)) {
            execute_has_all<ColumnUInt8>(*src1_nested_column,*src2_nested_column,src1_null_map,
                                               src2_null_map,vec_res,offsets1,offsets2);
        } else if (check_column<ColumnInt8>(*src1_nested_column)) {
            execute_has_all<ColumnInt8>(*src1_nested_column,*src2_nested_column,src1_null_map,
                                               src2_null_map,vec_res,offsets1,offsets2);
        } else if (check_column<ColumnInt16>(*src1_nested_column)) {
            execute_has_all<ColumnInt16>(*src1_nested_column,*src2_nested_column,src1_null_map,
                                               src2_null_map,vec_res,offsets1,offsets2);
        } else if (check_column<ColumnInt32>(*src1_nested_column)) {
            execute_has_all<ColumnInt32>(*src1_nested_column,*src2_nested_column,src1_null_map,
                                               src2_null_map,vec_res,offsets1,offsets2);
        } else if (check_column<ColumnInt64>(*src1_nested_column)) {
            execute_has_all<ColumnInt64>(*src1_nested_column,*src2_nested_column,src1_null_map,
                                               src2_null_map,vec_res,offsets1,offsets2);
        } else if (check_column<ColumnInt128>(*src1_nested_column)) {
            execute_has_all<ColumnInt128>(*src1_nested_column,*src2_nested_column,src1_null_map,
                                               src2_null_map,vec_res,offsets1,offsets2);
        } else if (check_column<ColumnFloat32>(*src1_nested_column)) {
            execute_has_all<ColumnFloat32>(*src1_nested_column,*src2_nested_column,src1_null_map,
                                                 src2_null_map,vec_res,offsets1,offsets2);
        } else if (check_column<ColumnFloat64>(*src1_nested_column)) {
            execute_has_all<ColumnFloat64>(*src1_nested_column,*src2_nested_column,src1_null_map,
                                                 src2_null_map,vec_res,offsets1,offsets2);
        } else if (check_column<ColumnDecimal128>(*src1_nested_column)) {
            execute_has_all<ColumnDecimal128>(*src1_nested_column,*src2_nested_column,src1_null_map,
                                               src2_null_map,vec_res,offsets1,offsets2);
        } 
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