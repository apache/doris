#include <fmt/core.h>
#include <cstddef>
#include <limits>
#include <type_traits>

#include "common/exception.h"
#include "common/status.h"
#include "runtime/primitive_type.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_vector.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

template <typename IntegerType>
class FunctionDecompressVarchar : public IFunction {
    static inline void reverse_bytes(uint8_t* __restrict s, size_t length) {
        if (length == 0) {
            return;
        }

        int c, i, j;

        for (i = 0, j = length - 1; i < j; i++, j--) {
            c = s[i];
            s[i] = s[j];
            s[j] = c;
        }
    }

public:
    static constexpr auto name = "decompress_varchar";
    static FunctionPtr create() { return std::make_shared<FunctionDecompressVarchar>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    bool is_variadic() const override { return true; }

    DataTypes get_variadic_argument_types_impl() const override {
        if constexpr (std::is_same_v<IntegerType, Int8>) {
            return {std::make_shared<DataTypeInt8>()};
        } else if constexpr (std::is_same_v<IntegerType, Int32>) {
            return {std::make_shared<DataTypeInt32>()};
        } else if constexpr (std::is_same_v<IntegerType, Int64>) {
            return {std::make_shared<DataTypeInt64>()};
        } else if constexpr (std::is_same_v<IntegerType, Int128>) {
            return {std::make_shared<DataTypeInt128>()};
        } else {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT, "Invalid IntegerType");
        }
    }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        if (arguments.size() != 1) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                   "Function {} requires 1 arguments, got {}", name,
                                   arguments.size());
        }

        return std::make_shared<DataTypeString>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        const ColumnVector<IntegerType>* col_source = assert_cast<const ColumnVector<IntegerType>*>(
                block.get_by_position(arguments[0]).column.get());

        auto col_res = ColumnString::create();

        ColumnString::Chars& col_res_data = col_res->get_chars();
        ColumnString::Offsets& col_res_offset = col_res->get_offsets();
        col_res_data.resize(input_rows_count * sizeof(IntegerType));
        col_res_offset.resize(input_rows_count);

        if constexpr (std::is_same_v<IntegerType, Int8>) {
            for (Int32 i = 0; i < input_rows_count; ++i) {
                const Int8& value = col_source->get_element(i);
                UInt32 str_size = value == 0 ? 0 : 1;
                // col_res_offset[-1] is valid for PaddedPODArray, will get 0
                col_res_offset[i] = col_res_offset[i - 1] + str_size;
                memcpy(col_res_data.data() + col_res_offset[i - 1], &value, str_size);
            }
        } else {
            for (Int32 i = 0; i < input_rows_count; ++i) {
                IntegerType value = col_source->get_element(i);
                const UInt8* const __restrict ui8_ptr = reinterpret_cast<const UInt8*>(&value);
                UInt32 str_size = static_cast<UInt32>(*ui8_ptr) & 0x7F;

                if (str_size >= sizeof(IntegerType)) {
                    auto type_ptr = block.get_by_position(arguments[0]).type;
                    throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                           "Invalid input of function {}, input type {} value {}, "
                                           "string size {}, should not be larger than {}",
                                           name, type_ptr->get_name(), value, str_size,
                                           sizeof(IntegerType));
                }

                // col_res_offset[-1] is valid for PaddedPODArray, will get 0
                col_res_offset[i] = col_res_offset[i - 1] + str_size;
                value <<= 1;

                memcpy(col_res_data.data() + col_res_offset[i - 1],
                       ui8_ptr + sizeof(IntegerType) - str_size, str_size);

                reverse_bytes(col_res_data.data() + col_res_offset[i - 1], str_size);
            }
        }

        block.get_by_position(result).column = std::move(col_res);

        return Status::OK();
    }
};

void register_function_decompress_varchar(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionDecompressVarchar<Int8>>();
    factory.register_function<FunctionDecompressVarchar<Int32>>();
    factory.register_function<FunctionDecompressVarchar<Int64>>();
    factory.register_function<FunctionDecompressVarchar<Int128>>();
}

} // namespace doris::vectorized
