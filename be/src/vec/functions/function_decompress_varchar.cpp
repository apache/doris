// #include <cstddef>
// #include <limits>
// #include <type_traits>

// #include "common/exception.h"
// #include "common/status.h"
// #include "runtime/primitive_type.h"
// #include "vec/columns/column_const.h"
// #include "vec/columns/column_vector.h"
// #include "vec/core/types.h"
// #include "vec/data_types/data_type.h"
// #include "vec/data_types/data_type_number.h"
// #include "vec/data_types/data_type_string.h"
// #include "vec/functions/function.h"
// #include "vec/functions/function_helpers.h"
// #include "vec/functions/simple_function_factory.h"

// namespace doris::vectorized {

// template <typename IntegerType>
// struct CompressVarcharImpl {
//     static inline void compress(const char* str, UInt8 size, IntegerType* res) {
//         UInt8* __restrict ui8_ptr = reinterpret_cast<UInt8*>(res);

//         for (size_t i = 0; i < size; ++i) {
//             memcpy(ui8_ptr + sizeof(IntegerType) - 1 - i, str + i, 1);
//         }

//         memset(ui8_ptr, size << 1, 1);
//         if constexpr (std::is_same_v<IntegerType, Int8>) {
//             *res &= 0x7F;
//         } else if constexpr (std::is_same_v<IntegerType, Int16>) {
//             *res &= 0x7FFF;
//         } else if constexpr (std::is_same_v<IntegerType, Int32>) {
//             *res &= 0x7FFFFFFF;
//         } else if constexpr (std::is_same_v<IntegerType, Int64>) {
//             *res &= 0x7FFFFFFFFFFFFFFF;
//         } else if constexpr (std::is_same_v<IntegerType, Int128>) {
//             res &= std::numeric_limits<Int128>::max();
//         }

//         *res = (*res >> 1);
//     }
// };

// template <typename IntegerType>
// struct DecompressVarcharImpl {
//     static inline void decompress(IntegerType val, std::string* res) {
//         auto ui8_ptr = reinterpret_cast<uint8_t*>(&val);
//         int strSize = *ui8_ptr;

//         res->reserve(strSize);
//         val = val << 1;
//         for (int i = strSize - 1, j = 0; i >= 0; --i, ++j) {
//             res->push_back(*(ui8_ptr + sizeof(val) - 1 - j));
//         }
//     }
// };

// template <typename IntegerType>
// class FunctionDecompressVarchar : public IFunction {
// public:
//     static constexpr auto name = "decompress_varchar";
//     static FunctionPtr create() { return std::make_shared<FunctionDecompressVarchar>(); }

//     String get_name() const override { return name; }

//     size_t get_number_of_arguments() const override { return 1; }

//     bool is_variadic() const override { return true; }

//     DataTypes get_variadic_argument_types_impl() const override {
//         if constexpr (std::is_same_v<IntegerType, Int8>) {
//             return {std::make_shared<DataTypeInt8>()};
//         } else if constexpr (std::is_same_v<IntegerType, Int16>) {
//             return {std::make_shared<DataTypeInt16>()};
//         } else if constexpr (std::is_same_v<IntegerType, Int32>) {
//             return {std::make_shared<DataTypeInt32>()};
//         } else if constexpr (std::is_same_v<IntegerType, Int64>) {
//             return {std::make_shared<DataTypeInt64>()};
//         } else if constexpr (std::is_same_v<IntegerType, Int128>) {
//             return {std::make_shared<DataTypeInt128>()};
//         } else {
//             throw doris::Exception(ErrorCode::INVALID_ARGUMENT, "Invalid IntegerType");
//         }
//     }

//     DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
//         if (arguments.size() != 1) {
//             throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
//                                    "Function {} requires 2 arguments, got {}", name,
//                                    arguments.size());
//         }

//         return std::make_shared<DataTypeString>();
//     }

//     Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
//                         size_t result, size_t input_rows_count) const override {
//         const ColumnVector<IntegerType>* col_str =
//                 assert_cast<const ColumnVector<IntegerType>*>(block.get_by_position(arguments[0]).column.get());

//         auto col_res = ColumnString::create(input_rows_count);
//         auto& col_res_data = col_res->get_data();

//         for (size_t i = 0; i < input_rows_count; ++i) {
//             CompressVarcharImpl<IntegerType>::compress(
//                     col_str->get_data_at(i).data, static_cast<UInt8>(col_str->get_data_at(i).size), &col_res_data[i]);
//         }

//         block.get_by_position(result).column = std::move(col_res);

//         return Status::OK();
//     }
// };

// void register_function_compress_varchar(SimpleFunctionFactory& factory) {
//     factory.register_function<FunctionDecompressVarchar<Int8>>();
//     factory.register_function<FunctionDecompressVarchar<Int16>>();
//     factory.register_function<FunctionDecompressVarchar<Int32>>();
//     factory.register_function<FunctionDecompressVarchar<Int64>>();
//     factory.register_function<FunctionDecompressVarchar<Int128>>();
// }

// } // namespace doris::vectorized
