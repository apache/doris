#pragma once

#include "vec/data_types/data_type.h"
#include "vec/data_types/number_traits.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_number.h"
#include "common/status.h"
#include "vec/common/assert_cast.h"
#include "vec/functions/function.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/columns_number.h"
#include "vec/common/typeid_cast.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized{

// //template <bool is_first_line_zero>
// struct FunctionRunningDifferenceName;

// //template <>
// struct FunctionRunningDifferenceName{
//     static constexpr auto name = "running_difference";
// };


//template <bool is_first_line_zero=true>
class FunctionRunningDifference : public IFunction
{
private:
    /// It is possible to track value from previous columns, to calculate continuously across all columnss. Not implemented.
    //NO_SANITIZE_UNDEFINED
    template <typename Src, typename Dst>
    static void process(const PaddedPODArray<Src> & src, PaddedPODArray<Dst> & dst, const NullMap * null_map)
    {
        size_t size = src.size();
        dst.resize(size);

        if (size == 0)
            return;

        /// It is possible to SIMD optimize this loop. By no need for that in practice.

        Src prev{};
        bool has_prev_value = false;

        for (size_t i = 0; i < size; ++i)
        {
            if (null_map && (*null_map)[i])
            {
                dst[i] = Dst{};
                continue;
            }

            if (!has_prev_value)
            {
                dst[i] = 0;
                prev = src[i];
                has_prev_value = true;
            }
            else
            {
                auto cur = src[i];
                /// Overflow is Ok.
                dst[i] = static_cast<Dst>(cur) - prev;
                prev = cur;
            }
        }
    }

    /// Result type is same as result of subtraction of argument types.
    template <typename SrcFieldType>
    using DstFieldType = typename NumberTraits::ResultOfSubtraction<SrcFieldType, SrcFieldType>::Type;

    /// Call polymorphic lambda with tag argument of concrete field type of src_type.
    template <typename F>
    void dispatchForSourceType(const IDataType & src_type, F && f) const
    {
        WhichDataType which(src_type);

        if (which.is_uint8())
            f(UInt8());
        else if (which.is_uint16())
            f(UInt16());
        else if (which.is_uint32())
            f(UInt32());
        else if (which.is_uint64())
            f(UInt64());
        else if (which.is_int8())
            f(Int8());
        else if (which.is_int16())
            f(Int16());
        else if (which.is_int32())
            f(Int32());
        else if (which.is_int64())
            f(Int64());
        else if (which.is_float32())
            f(Float32());
        else if (which.is_float64())
            f(Float64());
        else if (which.is_date())
            f(DataTypeDate::FieldType());
       // else if (which.is_date32())
          //  f(DataTypeDate32::FieldType());
       // else if (which.is_date_time())
            //f(DataTypeDateTime::FieldType());
        //else
            throw Exception("Argument for function " + get_name() + " must have numeric type.", 1/*ErrorCode::ILLEGAL_TYPE_OF_ARGUMENT*/);
    }

public:
    static constexpr auto name = "running_difference";

    static FunctionPtr create(){
        return std::make_shared<FunctionRunningDifference>();
    }

    String get_name() const override {
        return name;
    }

    bool is_stateful() const override{
        return true;
    } 

    size_t get_number_of_arguments() const override
    {
        return 1;
    }

    bool is_deterministic() const override { return false; }
    bool is_deterministic_in_scope_of_query() const override
    {
         return false;
    } 

    // bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    bool use_default_implementation_for_nulls() const override { return false; }

    DataTypePtr get_return_type_impl(const DataTypes & arguments) const override
    {
        DataTypePtr res;
        dispatchForSourceType(*remove_nullable(arguments[0]), [&](auto field_type_tag)
        {
            res = std::make_shared<DataTypeNumber<DstFieldType<decltype(field_type_tag)>>>();
        });

        if (arguments[0]->is_nullable())
            res = make_nullable(res);

        return res;
    }
   
    //ColumnPtr execute_impl(const ColumnNumbers& arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    Status execute_impl(FunctionContext* context, Block& block,
                                 const ColumnNumbers& arguments, size_t result,
                                size_t input_rows_count) override
    {
        const auto  src =  block.get_by_position(arguments[0]); //取出第一个参数
        //auto src=assert_cast<const IColumn*>(src_m.get());
        const DataTypePtr result_type=src.type;
        /// When column is constant, its difference is zero.
        if (is_column_const(*src.column)) {
            auto res=result_type->create_column_const_with_default_value(input_rows_count);
            block.replace_by_position(result, std::move(res));
            return Status::OK();
        }   
        auto res_column = remove_nullable(result_type)->create_column();

        const auto * src_column = src.column.get(); //获取src的行
        ColumnPtr null_map_column = nullptr;
        const NullMap * null_map = nullptr;
        
        if (const auto * nullable_column = check_and_get_column<ColumnNullable>(src_column))
        {
            src_column = &nullable_column->get_nested_column();
            null_map_column = nullable_column->get_null_map_column_ptr();
            null_map = &nullable_column->get_null_map_data();
        }
        //removeNullable?
        dispatchForSourceType(*remove_nullable(src.type), [&](auto field_type_tag)
        {
            using SrcFieldType = decltype(field_type_tag);
            
            process(assert_cast<const ColumnVector<SrcFieldType> &>(*src_column).get_data(),
                assert_cast<ColumnVector<DstFieldType<SrcFieldType>> &>(*res_column).get_data(), null_map);
        });

        if(null_map_column){
           // auto res=ColumnNullable::create(std::move(res_column), null_map_column);
           // block.replace_by_position(result, std::move(res));
        }
        else{
            block.replace_by_position(result, std::move(res_column));
        }
        return Status::OK();
    }
};

}