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

#include "vec/functions/function_string.h"

#include <ctype.h>
#include <math.h>
#include <re2/stringpiece.h>

#include <cstddef>
#include <string_view>

#include "common/status.h"
#include "runtime/string_search.hpp"
#include "util/url_coding.h"
#include "vec/columns/column.h"
#include "vec/columns/column_string.h"
#include "vec/common/pod_array_fwd.h"
#include "vec/common/string_ref.h"
#include "vec/functions/function_reverse.h"
#include "vec/functions/function_string_to_string.h"
#include "vec/functions/function_totype.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {
struct NameStringASCII {
    static constexpr auto name = "ascii";
};

struct StringASCII {
    using ReturnType = DataTypeInt32;
    static constexpr auto TYPE_INDEX = TypeIndex::String;
    using Type = String;
    using ReturnColumnType = ColumnVector<Int32>;

    static Status vector(const ColumnString::Chars& data, const ColumnString::Offsets& offsets,
                         PaddedPODArray<Int32>& res) {
        auto size = offsets.size();
        res.resize(size);
        for (int i = 0; i < size; ++i) {
            const char* raw_str = reinterpret_cast<const char*>(&data[offsets[i - 1]]);
            res[i] = (offsets[i] == offsets[i - 1]) ? 0 : static_cast<uint8_t>(raw_str[0]);
        }
        return Status::OK();
    }
};

struct NameQuote {
    static constexpr auto name = "quote";
};

struct NameQuoteImpl {
    static Status vector(const ColumnString::Chars& data, const ColumnString::Offsets& offsets,
                         ColumnString::Chars& res_data, ColumnString::Offsets& res_offsets) {
        size_t offset_size = offsets.size();
        size_t pos = 0;
        res_offsets.resize(offset_size);
        res_data.resize(data.size() + offset_size * 2);
        for (int i = 0; i < offset_size; i++) {
            const unsigned char* raw_str = &data[offsets[i - 1]];
            ColumnString::Offset size = offsets[i] - offsets[i - 1];
            res_data[pos] = '\'';
            std::memcpy(res_data.data() + pos + 1, raw_str, size);
            res_data[pos + size + 1] = '\'';
            pos += size + 2;
            res_offsets[i] = pos;
        }
        return Status::OK();
    }
};

struct NameStringLength {
    static constexpr auto name = "length";
};

struct StringLengthImpl {
    using ReturnType = DataTypeInt32;
    static constexpr auto TYPE_INDEX = TypeIndex::String;
    using Type = String;
    using ReturnColumnType = ColumnVector<Int32>;

    static Status vector(const ColumnString::Chars& data, const ColumnString::Offsets& offsets,
                         PaddedPODArray<Int32>& res) {
        auto size = offsets.size();
        res.resize(size);
        for (int i = 0; i < size; ++i) {
            int str_size = offsets[i] - offsets[i - 1];
            res[i] = str_size;
        }
        return Status::OK();
    }
};

struct NameCrc32 {
    static constexpr auto name = "crc32";
};

struct Crc32Impl {
    using ReturnType = DataTypeInt64;
    static constexpr auto TYPE_INDEX = TypeIndex::String;
    using Type = String;
    using ReturnColumnType = ColumnVector<Int64>;

    static Status vector(const ColumnString::Chars& data, const ColumnString::Offsets& offsets,
                         PaddedPODArray<Int64>& res) {
        auto size = offsets.size();
        res.resize(size);
        for (int i = 0; i < size; ++i) {
            res[i] = crc32_z(0L, (const unsigned char*)data.data() + offsets[i - 1],
                             offsets[i] - offsets[i - 1]);
        }
        return Status::OK();
    }
};

struct NameStringUtf8Length {
    static constexpr auto name = "char_length";
};

struct StringUtf8LengthImpl {
    using ReturnType = DataTypeInt32;
    static constexpr auto TYPE_INDEX = TypeIndex::String;
    using Type = String;
    using ReturnColumnType = ColumnVector<Int32>;

    static Status vector(const ColumnString::Chars& data, const ColumnString::Offsets& offsets,
                         PaddedPODArray<Int32>& res) {
        auto size = offsets.size();
        res.resize(size);
        for (int i = 0; i < size; ++i) {
            const char* raw_str = reinterpret_cast<const char*>(&data[offsets[i - 1]]);
            int str_size = offsets[i] - offsets[i - 1];
            res[i] = simd::VStringFunctions::get_char_len(raw_str, str_size);
        }
        return Status::OK();
    }
};

struct NameStartsWith {
    static constexpr auto name = "starts_with";
};

struct StartsWithOp {
    using ResultDataType = DataTypeUInt8;
    using ResultPaddedPODArray = PaddedPODArray<UInt8>;

    static void execute(const std::string_view& strl, const std::string_view& strr, uint8_t& res) {
        re2::StringPiece str_sp(const_cast<char*>(strl.data()), strl.length());
        re2::StringPiece prefix_sp(const_cast<char*>(strr.data()), strr.length());
        res = str_sp.starts_with(prefix_sp);
    }
};

struct NameEndsWith {
    static constexpr auto name = "ends_with";
};

struct EndsWithOp {
    using ResultDataType = DataTypeUInt8;
    using ResultPaddedPODArray = PaddedPODArray<UInt8>;

    static void execute(const std::string_view& strl, const std::string_view& strr, uint8_t& res) {
        re2::StringPiece str_sp(const_cast<char*>(strl.data()), strl.length());
        re2::StringPiece prefix_sp(const_cast<char*>(strr.data()), strr.length());
        res = str_sp.ends_with(prefix_sp);
    }
};

struct NameFindInSet {
    static constexpr auto name = "find_in_set";
};

struct FindInSetOp {
    using ResultDataType = DataTypeInt32;
    using ResultPaddedPODArray = PaddedPODArray<Int32>;
    static void execute(const std::string_view& strl, const std::string_view& strr, int32_t& res) {
        for (const auto& c : strl) {
            if (c == ',') {
                res = 0;
                return;
            }
        }

        int32_t token_index = 1;
        int32_t start = 0;
        int32_t end;

        do {
            end = start;
            // Position end.
            while (end < strr.length() && strr[end] != ',') {
                ++end;
            }

            if (strl == std::string_view {strr.data() + start, (size_t)end - start}) {
                res = token_index;
                return;
            }

            // Re-position start and end past ','
            start = end + 1;
            ++token_index;
        } while (start < strr.length());
        res = 0;
    }
};

struct NameInstr {
    static constexpr auto name = "instr";
};

// LeftDataType and RightDataType are DataTypeString
template <typename LeftDataType, typename RightDataType>
struct StringInStrImpl {
    using ResultDataType = DataTypeInt32;
    using ResultPaddedPODArray = PaddedPODArray<Int32>;

    static Status scalar_vector(const StringRef& ldata, const ColumnString::Chars& rdata,
                                const ColumnString::Offsets& roffsets, ResultPaddedPODArray& res) {
        StringRef lstr_ref(ldata.data, ldata.size);

        auto size = roffsets.size();
        res.resize(size);
        for (int i = 0; i < size; ++i) {
            const char* r_raw_str = reinterpret_cast<const char*>(&rdata[roffsets[i - 1]]);
            int r_str_size = roffsets[i] - roffsets[i - 1];

            StringRef rstr_ref(r_raw_str, r_str_size);

            res[i] = execute(lstr_ref, rstr_ref);
        }

        return Status::OK();
    }

    static Status vector_scalar(const ColumnString::Chars& ldata,
                                const ColumnString::Offsets& loffsets, const StringRef& rdata,
                                ResultPaddedPODArray& res) {
        auto size = loffsets.size();
        res.resize(size);

        if (rdata.size == 0) {
            std::fill(res.begin(), res.end(), 1);
            return Status::OK();
        }

        const UInt8* begin = ldata.data();
        const UInt8* end = begin + ldata.size();
        const UInt8* pos = begin;

        /// Current index in the array of strings.
        size_t i = 0;
        std::fill(res.begin(), res.end(), 0);

        StringRef rstr_ref(rdata.data, rdata.size);
        StringSearch search(&rstr_ref);

        while (pos < end) {
            // search return matched substring start offset
            pos = (UInt8*)search.search((char*)pos, end - pos);
            if (pos >= end) {
                break;
            }

            /// Determine which index it refers to.
            /// begin + value_offsets[i] is the start offset of string at i+1
            while (begin + loffsets[i] < pos) {
                ++i;
            }

            /// We check that the entry does not pass through the boundaries of strings.
            if (pos + rdata.size <= begin + loffsets[i]) {
                int loc = pos - begin - loffsets[i - 1];
                int l_str_size = loffsets[i] - loffsets[i - 1];
                size_t len = std::min(l_str_size, loc);
                loc = simd::VStringFunctions::get_char_len((char*)(begin + loffsets[i - 1]), len);
                res[i] = loc + 1;
            }

            // move to next string offset
            pos = begin + loffsets[i];
            ++i;
        }

        return Status::OK();
    }

    static Status vector_vector(const ColumnString::Chars& ldata,
                                const ColumnString::Offsets& loffsets,
                                const ColumnString::Chars& rdata,
                                const ColumnString::Offsets& roffsets, ResultPaddedPODArray& res) {
        DCHECK_EQ(loffsets.size(), roffsets.size());

        auto size = loffsets.size();
        res.resize(size);
        for (int i = 0; i < size; ++i) {
            const char* l_raw_str = reinterpret_cast<const char*>(&ldata[loffsets[i - 1]]);
            int l_str_size = loffsets[i] - loffsets[i - 1];
            StringRef lstr_ref(l_raw_str, l_str_size);

            const char* r_raw_str = reinterpret_cast<const char*>(&rdata[roffsets[i - 1]]);
            int r_str_size = roffsets[i] - roffsets[i - 1];
            StringRef rstr_ref(r_raw_str, r_str_size);

            res[i] = execute(lstr_ref, rstr_ref);
        }

        return Status::OK();
    }

    static int execute(const StringRef& strl, const StringRef& strr) {
        if (strr.size == 0) {
            return 1;
        }

        StringSearch search(&strr);
        // Hive returns positions starting from 1.
        int loc = search.search(&strl);
        if (loc > 0) {
            size_t len = std::min((size_t)loc, strl.size);
            loc = simd::VStringFunctions::get_char_len(strl.data, len);
        }

        return loc + 1;
    }
};

// the same impl as instr
struct NameLocate {
    static constexpr auto name = "locate";
};

// LeftDataType and RightDataType are DataTypeString
template <typename LeftDataType, typename RightDataType>
struct StringLocateImpl {
    using ResultDataType = DataTypeInt32;
    using ResultPaddedPODArray = PaddedPODArray<Int32>;

    static Status scalar_vector(const StringRef& ldata, const ColumnString::Chars& rdata,
                                const ColumnString::Offsets& roffsets, ResultPaddedPODArray& res) {
        return StringInStrImpl<LeftDataType, RightDataType>::vector_scalar(rdata, roffsets, ldata,
                                                                           res);
    }

    static Status vector_scalar(const ColumnString::Chars& ldata,
                                const ColumnString::Offsets& loffsets, const StringRef& rdata,
                                ResultPaddedPODArray& res) {
        return StringInStrImpl<LeftDataType, RightDataType>::scalar_vector(rdata, ldata, loffsets,
                                                                           res);
    }

    static Status vector_vector(const ColumnString::Chars& ldata,
                                const ColumnString::Offsets& loffsets,
                                const ColumnString::Chars& rdata,
                                const ColumnString::Offsets& roffsets, ResultPaddedPODArray& res) {
        return StringInStrImpl<LeftDataType, RightDataType>::vector_vector(rdata, roffsets, ldata,
                                                                           loffsets, res);
    }
};

// LeftDataType and RightDataType are DataTypeString
template <typename LeftDataType, typename RightDataType, typename OP>
struct StringFunctionImpl {
    using ResultDataType = typename OP::ResultDataType;
    using ResultPaddedPODArray = typename OP::ResultPaddedPODArray;

    static void vector_vector(const ColumnString::Chars& ldata,
                              const ColumnString::Offsets& loffsets,
                              const ColumnString::Chars& rdata,
                              const ColumnString::Offsets& roffsets, ResultPaddedPODArray& res) {
        DCHECK_EQ(loffsets.size(), roffsets.size());

        auto size = loffsets.size();
        res.resize(size);
        for (int i = 0; i < size; ++i) {
            const char* l_raw_str = reinterpret_cast<const char*>(&ldata[loffsets[i - 1]]);
            int l_str_size = loffsets[i] - loffsets[i - 1];

            const char* r_raw_str = reinterpret_cast<const char*>(&rdata[roffsets[i - 1]]);
            int r_str_size = roffsets[i] - roffsets[i - 1];

            std::string_view lview(l_raw_str, l_str_size);
            std::string_view rview(r_raw_str, r_str_size);

            OP::execute(lview, rview, res[i]);
        }
    }
    static void vector_scalar(const ColumnString::Chars& ldata,
                              const ColumnString::Offsets& loffsets, const StringRef& rdata,
                              ResultPaddedPODArray& res) {
        auto size = loffsets.size();
        res.resize(size);
        std::string_view rview(rdata.data, rdata.size);
        for (int i = 0; i < size; ++i) {
            const char* l_raw_str = reinterpret_cast<const char*>(&ldata[loffsets[i - 1]]);
            int l_str_size = loffsets[i] - loffsets[i - 1];
            std::string_view lview(l_raw_str, l_str_size);

            OP::execute(lview, rview, res[i]);
        }
    }
    static void scalar_vector(const StringRef& ldata, const ColumnString::Chars& rdata,
                              const ColumnString::Offsets& roffsets, ResultPaddedPODArray& res) {
        auto size = roffsets.size();
        res.resize(size);
        std::string_view lview(ldata.data, ldata.size);
        for (int i = 0; i < size; ++i) {
            const char* r_raw_str = reinterpret_cast<const char*>(&rdata[roffsets[i - 1]]);
            int r_str_size = roffsets[i] - roffsets[i - 1];
            std::string_view rview(r_raw_str, r_str_size);

            OP::execute(lview, rview, res[i]);
        }
    }
};

struct NameToLower {
    static constexpr auto name = "lower";
};

struct NameToUpper {
    static constexpr auto name = "upper";
};

template <typename OpName>
struct TransferImpl {
    static Status vector(const ColumnString::Chars& data, const ColumnString::Offsets& offsets,
                         ColumnString::Chars& res_data, ColumnString::Offsets& res_offsets) {
        size_t offset_size = offsets.size();
        if (UNLIKELY(!offset_size)) {
            return Status::OK();
        }

        res_offsets.resize(offset_size);
        memcpy_small_allow_read_write_overflow15(
                res_offsets.data(), offsets.data(),
                offset_size * sizeof(ColumnString::Offsets::value_type));

        size_t data_length = data.size();
        res_data.resize(data_length);
        if constexpr (std::is_same_v<OpName, NameToUpper>) {
            simd::VStringFunctions::to_upper(data.data(), data_length, res_data.data());
        } else if constexpr (std::is_same_v<OpName, NameToLower>) {
            simd::VStringFunctions::to_lower(data.data(), data_length, res_data.data());
        }
        return Status::OK();
    }
};

// Capitalize first letter
struct NameToInitcap {
    static constexpr auto name = "initcap";
};

struct InitcapImpl {
    static Status vector(const ColumnString::Chars& data, const ColumnString::Offsets& offsets,
                         ColumnString::Chars& res_data, ColumnString::Offsets& res_offsets) {
        size_t offset_size = offsets.size();
        res_offsets.resize(offsets.size());
        memcpy_small_allow_read_write_overflow15(
                res_offsets.data(), offsets.data(),
                offset_size * sizeof(ColumnString::Offsets::value_type));

        size_t data_length = data.size();
        res_data.resize(data_length);
        simd::VStringFunctions::to_lower(data.data(), data_length, res_data.data());

        bool need_capitalize = true;
        for (size_t offset_index = 0, start_index = 0; offset_index < offset_size; ++offset_index) {
            auto end_index = res_offsets[offset_index];
            need_capitalize = true;

            for (size_t i = start_index; i < end_index; ++i) {
                if (!::isalnum(res_data[i])) {
                    need_capitalize = true;
                } else if (need_capitalize) {
                    res_data[i] = ::toupper(res_data[i]);
                    need_capitalize = false;
                }
            }

            start_index = end_index;
        }
        return Status::OK();
    }
};

struct NameTrim {
    static constexpr auto name = "trim";
};
struct NameLTrim {
    static constexpr auto name = "ltrim";
};
struct NameRTrim {
    static constexpr auto name = "rtrim";
};
template <bool is_ltrim, bool is_rtrim, bool trim_single>
struct TrimUtil {
    static Status vector(const ColumnString::Chars& str_data,
                         const ColumnString::Offsets& str_offsets, const StringRef& remove_str,
                         ColumnString::Chars& res_data, ColumnString::Offsets& res_offsets) {
        const size_t offset_size = str_offsets.size();
        res_offsets.resize(offset_size);
        res_data.reserve(str_data.size());
        for (size_t i = 0; i < offset_size; ++i) {
            const auto* str_begin = str_data.data() + str_offsets[i - 1];
            const auto* str_end = str_data.data() + str_offsets[i];

            if constexpr (is_ltrim) {
                str_begin =
                        simd::VStringFunctions::ltrim<trim_single>(str_begin, str_end, remove_str);
            }
            if constexpr (is_rtrim) {
                str_end =
                        simd::VStringFunctions::rtrim<trim_single>(str_begin, str_end, remove_str);
            }

            res_data.insert_assume_reserved(str_begin, str_end);
            res_offsets[i] = res_data.size();
        }
        return Status::OK();
    }
};
// This is an implementation of a parameter for the Trim function.
template <bool is_ltrim, bool is_rtrim, typename Name>
struct Trim1Impl {
    static constexpr auto name = Name::name;

    static DataTypes get_variadic_argument_types() { return {std::make_shared<DataTypeString>()}; }

    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          size_t result, size_t input_rows_count) {
        const ColumnPtr column = block.get_by_position(arguments[0]).column;
        if (const auto* col = assert_cast<const ColumnString*>(column.get())) {
            auto col_res = ColumnString::create();
            char blank[] = " ";
            const StringRef remove_str(blank, 1);
            RETURN_IF_ERROR((TrimUtil<is_ltrim, is_rtrim, true>::vector(
                    col->get_chars(), col->get_offsets(), remove_str, col_res->get_chars(),
                    col_res->get_offsets())));
            block.replace_by_position(result, std::move(col_res));
        } else {
            return Status::RuntimeError("Illegal column {} of argument of function {}",
                                        block.get_by_position(arguments[0]).column->get_name(),
                                        name);
        }
        return Status::OK();
    }
};

// This is an implementation of two parameters for the Trim function.
template <bool is_ltrim, bool is_rtrim, typename Name>
struct Trim2Impl {
    static constexpr auto name = Name::name;

    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()};
    }

    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          size_t result, size_t input_rows_count) {
        const ColumnPtr column = block.get_by_position(arguments[0]).column;
        const auto& rcol =
                assert_cast<const ColumnConst*>(block.get_by_position(arguments[1]).column.get())
                        ->get_data_column_ptr();
        if (const auto* col = assert_cast<const ColumnString*>(column.get())) {
            if (const auto* col_right = assert_cast<const ColumnString*>(rcol.get())) {
                auto col_res = ColumnString::create();
                const auto* remove_str_raw = col_right->get_chars().data();
                const ColumnString::Offset remove_str_size = col_right->get_offsets()[0];
                const StringRef remove_str(remove_str_raw, remove_str_size);
                if (remove_str.size == 1) {
                    RETURN_IF_ERROR((TrimUtil<is_ltrim, is_rtrim, true>::vector(
                            col->get_chars(), col->get_offsets(), remove_str, col_res->get_chars(),
                            col_res->get_offsets())));
                } else {
                    RETURN_IF_ERROR((TrimUtil<is_ltrim, is_rtrim, false>::vector(
                            col->get_chars(), col->get_offsets(), remove_str, col_res->get_chars(),
                            col_res->get_offsets())));
                }
                block.replace_by_position(result, std::move(col_res));
            } else {
                return Status::RuntimeError("Illegal column {} of argument of function {}",
                                            block.get_by_position(arguments[1]).column->get_name(),
                                            name);
            }

        } else {
            return Status::RuntimeError("Illegal column {} of argument of function {}",
                                        block.get_by_position(arguments[0]).column->get_name(),
                                        name);
        }
        return Status::OK();
    }
};

template <typename impl>
class FunctionTrim : public IFunction {
public:
    static constexpr auto name = impl::name;
    static FunctionPtr create() { return std::make_shared<FunctionTrim<impl>>(); }
    String get_name() const override { return impl::name; }

    size_t get_number_of_arguments() const override {
        return get_variadic_argument_types_impl().size();
    }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        if (!is_string_or_fixed_string(arguments[0])) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                   "Illegal type {} of argument of function {}",
                                   arguments[0]->get_name(), get_name());
        }
        return arguments[0];
    }
    // The second parameter of "trim" is a constant.
    ColumnNumbers get_arguments_that_are_always_constant() const override { return {1}; }

    DataTypes get_variadic_argument_types_impl() const override {
        return impl::get_variadic_argument_types();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        return impl::execute(context, block, arguments, result, input_rows_count);
    }
};

static constexpr int MAX_STACK_CIPHER_LEN = 1024 * 64;
struct UnHexImpl {
    static constexpr auto name = "unhex";
    using ReturnType = DataTypeString;
    using ColumnType = ColumnString;

    static bool check_and_decode_one(char& c, const char src_c, bool flag) {
        int k = flag ? 16 : 1;
        int value = src_c - '0';
        // 9 = ('9'-'0')
        if (value >= 0 && value <= 9) {
            c += value * k;
            return true;
        }

        value = src_c - 'A';
        // 5 = ('F'-'A')
        if (value >= 0 && value <= 5) {
            c += (value + 10) * k;
            return true;
        }

        value = src_c - 'a';
        // 5 = ('f'-'a')
        if (value >= 0 && value <= 5) {
            c += (value + 10) * k;
            return true;
        }
        // not in ( ['0','9'], ['a','f'], ['A','F'] )
        return false;
    }

    static int hex_decode(const char* src_str, size_t src_len, char* dst_str) {
        // if str length is odd or 0, return empty string like mysql dose.
        if ((src_len & 1) != 0 or src_len == 0) {
            return 0;
        }
        //check and decode one character at the same time
        // character in ( ['0','9'], ['a','f'], ['A','F'] ), return 'NULL' like mysql dose.
        for (auto i = 0, dst_index = 0; i < src_len; i += 2, dst_index++) {
            char c = 0;
            // combine two character into dst_str one character
            bool left_4bits_flag = check_and_decode_one(c, *(src_str + i), true);
            bool right_4bits_flag = check_and_decode_one(c, *(src_str + i + 1), false);

            if (!left_4bits_flag || !right_4bits_flag) {
                return 0;
            }
            *(dst_str + dst_index) = c;
        }
        return src_len / 2;
    }

    static Status vector(const ColumnString::Chars& data, const ColumnString::Offsets& offsets,
                         ColumnString::Chars& dst_data, ColumnString::Offsets& dst_offsets) {
        auto rows_count = offsets.size();
        dst_offsets.resize(rows_count);

        for (int i = 0; i < rows_count; ++i) {
            const auto* source = reinterpret_cast<const char*>(&data[offsets[i - 1]]);
            size_t srclen = offsets[i] - offsets[i - 1];

            if (srclen == 0) {
                StringOP::push_empty_string(i, dst_data, dst_offsets);
                continue;
            }

            char dst_array[MAX_STACK_CIPHER_LEN];
            char* dst = dst_array;

            int cipher_len = srclen / 2;
            std::unique_ptr<char[]> dst_uptr;
            if (cipher_len > MAX_STACK_CIPHER_LEN) {
                dst_uptr.reset(new char[cipher_len]);
                dst = dst_uptr.get();
            }

            int outlen = hex_decode(source, srclen, dst);

            StringOP::push_value_string(std::string_view(dst, outlen), i, dst_data, dst_offsets);
        }

        return Status::OK();
    }
};

struct NameStringSpace {
    static constexpr auto name = "space";
};

struct StringSpace {
    using ReturnType = DataTypeString;
    static constexpr auto TYPE_INDEX = TypeIndex::Int32;
    using Type = Int32;
    using ReturnColumnType = ColumnString;

    static Status vector(const ColumnInt32::Container& data, ColumnString::Chars& res_data,
                         ColumnString::Offsets& res_offsets) {
        res_offsets.resize(data.size());
        size_t input_size = res_offsets.size();
        std::vector<char, Allocator_<char>> buffer;
        for (size_t i = 0; i < input_size; ++i) {
            buffer.clear();
            if (data[i] > 0) {
                buffer.resize(data[i]);
                for (size_t j = 0; j < data[i]; ++j) {
                    buffer[i] = ' ';
                }
                StringOP::push_value_string(std::string_view(buffer.data(), buffer.size()), i,
                                            res_data, res_offsets);
            } else {
                StringOP::push_empty_string(i, res_data, res_offsets);
            }
        }
        return Status::OK();
    }
};

struct ToBase64Impl {
    static constexpr auto name = "to_base64";
    using ReturnType = DataTypeString;
    using ColumnType = ColumnString;

    static Status vector(const ColumnString::Chars& data, const ColumnString::Offsets& offsets,
                         ColumnString::Chars& dst_data, ColumnString::Offsets& dst_offsets) {
        auto rows_count = offsets.size();
        dst_offsets.resize(rows_count);

        for (int i = 0; i < rows_count; ++i) {
            const auto* source = reinterpret_cast<const char*>(&data[offsets[i - 1]]);
            size_t srclen = offsets[i] - offsets[i - 1];

            if (srclen == 0) {
                StringOP::push_empty_string(i, dst_data, dst_offsets);
                continue;
            }

            char dst_array[MAX_STACK_CIPHER_LEN];
            char* dst = dst_array;

            int cipher_len = (int)(4.0 * ceil((double)srclen / 3.0));
            std::unique_ptr<char[]> dst_uptr;
            if (cipher_len > MAX_STACK_CIPHER_LEN) {
                dst_uptr.reset(new char[cipher_len]);
                dst = dst_uptr.get();
            }

            auto outlen = base64_encode((const unsigned char*)source, srclen, (unsigned char*)dst);

            StringOP::push_value_string(std::string_view(dst, outlen), i, dst_data, dst_offsets);
        }
        return Status::OK();
    }
};

struct FromBase64Impl {
    static constexpr auto name = "from_base64";
    using ReturnType = DataTypeString;
    using ColumnType = ColumnString;

    static Status vector(const ColumnString::Chars& data, const ColumnString::Offsets& offsets,
                         ColumnString::Chars& dst_data, ColumnString::Offsets& dst_offsets,
                         NullMap& null_map) {
        auto rows_count = offsets.size();
        dst_offsets.resize(rows_count);

        for (int i = 0; i < rows_count; ++i) {
            if (null_map[i]) {
                StringOP::push_null_string(i, dst_data, dst_offsets, null_map);
                continue;
            }

            const auto* source = reinterpret_cast<const char*>(&data[offsets[i - 1]]);
            size_t srclen = offsets[i] - offsets[i - 1];

            if (srclen == 0) {
                StringOP::push_empty_string(i, dst_data, dst_offsets);
                continue;
            }

            char dst_array[MAX_STACK_CIPHER_LEN];
            char* dst = dst_array;

            int cipher_len = srclen;
            std::unique_ptr<char[]> dst_uptr;
            if (cipher_len > MAX_STACK_CIPHER_LEN) {
                dst_uptr.reset(new char[cipher_len]);
                dst = dst_uptr.get();
            }
            auto outlen = base64_decode(source, srclen, dst);

            if (outlen < 0) {
                StringOP::push_null_string(i, dst_data, dst_offsets, null_map);
            } else {
                StringOP::push_value_string(std::string_view(dst, outlen), i, dst_data,
                                            dst_offsets);
            }
        }

        return Status::OK();
    }
};

struct StringAppendTrailingCharIfAbsent {
    static constexpr auto name = "append_trailing_char_if_absent";
    using Chars = ColumnString::Chars;
    using Offsets = ColumnString::Offsets;
    using ReturnType = DataTypeString;
    using ColumnType = ColumnString;
    static void vector_vector(FunctionContext* context, const Chars& ldata, const Offsets& loffsets,
                              const Chars& rdata, const Offsets& roffsets, Chars& res_data,
                              Offsets& res_offsets, NullMap& null_map_data) {
        DCHECK_EQ(loffsets.size(), roffsets.size());
        size_t input_rows_count = loffsets.size();
        res_offsets.resize(input_rows_count);
        fmt::memory_buffer buffer;

        for (size_t i = 0; i < input_rows_count; ++i) {
            buffer.clear();

            int l_size = loffsets[i] - loffsets[i - 1];
            const auto l_raw = reinterpret_cast<const char*>(&ldata[loffsets[i - 1]]);

            int r_size = roffsets[i] - roffsets[i - 1];
            const auto r_raw = reinterpret_cast<const char*>(&rdata[roffsets[i - 1]]);

            if (r_size != 1) {
                StringOP::push_null_string(i, res_data, res_offsets, null_map_data);
                continue;
            }
            if (l_raw[l_size - 1] == r_raw[0]) {
                StringOP::push_value_string(std::string_view(l_raw, l_size), i, res_data,
                                            res_offsets);
                continue;
            }

            buffer.append(l_raw, l_raw + l_size);
            buffer.append(r_raw, r_raw + 1);
            StringOP::push_value_string(std::string_view(buffer.data(), buffer.size()), i, res_data,
                                        res_offsets);
        }
    }
    static void vector_scalar(FunctionContext* context, const Chars& ldata, const Offsets& loffsets,
                              const StringRef& rdata, Chars& res_data, Offsets& res_offsets,
                              NullMap& null_map_data) {
        size_t input_rows_count = loffsets.size();
        res_offsets.resize(input_rows_count);
        fmt::memory_buffer buffer;

        if (rdata.size != 1) {
            for (size_t i = 0; i < input_rows_count; ++i) {
                StringOP::push_null_string(i, res_data, res_offsets, null_map_data);
            }
            return;
        }

        for (size_t i = 0; i < input_rows_count; ++i) {
            buffer.clear();

            int l_size = loffsets[i] - loffsets[i - 1];
            const auto l_raw = reinterpret_cast<const char*>(&ldata[loffsets[i - 1]]);

            if (l_raw[l_size - 1] == rdata.data[0]) {
                StringOP::push_value_string(std::string_view(l_raw, l_size), i, res_data,
                                            res_offsets);
                continue;
            }

            buffer.append(l_raw, l_raw + l_size);
            buffer.append(rdata.begin(), rdata.end());
            StringOP::push_value_string(std::string_view(buffer.data(), buffer.size()), i, res_data,
                                        res_offsets);
        }
    }
    static void scalar_vector(FunctionContext* context, const StringRef& ldata, const Chars& rdata,
                              const Offsets& roffsets, Chars& res_data, Offsets& res_offsets,
                              NullMap& null_map_data) {
        size_t input_rows_count = roffsets.size();
        res_offsets.resize(input_rows_count);
        fmt::memory_buffer buffer;

        for (size_t i = 0; i < input_rows_count; ++i) {
            buffer.clear();

            int r_size = roffsets[i] - roffsets[i - 1];
            const auto r_raw = reinterpret_cast<const char*>(&rdata[roffsets[i - 1]]);

            if (r_size != 1) {
                StringOP::push_null_string(i, res_data, res_offsets, null_map_data);
                continue;
            }
            if (ldata.size == 0 || ldata.back() == r_raw[0]) {
                StringOP::push_value_string(ldata.to_string_view(), i, res_data, res_offsets);
                continue;
            }

            buffer.append(ldata.begin(), ldata.end());
            buffer.append(r_raw, r_raw + 1);
            StringOP::push_value_string(std::string_view(buffer.data(), buffer.size()), i, res_data,
                                        res_offsets);
        }
    }
};

struct StringLPad {
    static constexpr auto name = "lpad";
    static constexpr auto is_lpad = true;
};

struct StringRPad {
    static constexpr auto name = "rpad";
    static constexpr auto is_lpad = false;
};

template <typename LeftDataType, typename RightDataType>
using StringStartsWithImpl = StringFunctionImpl<LeftDataType, RightDataType, StartsWithOp>;

template <typename LeftDataType, typename RightDataType>
using StringEndsWithImpl = StringFunctionImpl<LeftDataType, RightDataType, EndsWithOp>;

template <typename LeftDataType, typename RightDataType>
using StringFindInSetImpl = StringFunctionImpl<LeftDataType, RightDataType, FindInSetOp>;

// ready for regist function
using FunctionStringASCII = FunctionUnaryToType<StringASCII, NameStringASCII>;
using FunctionStringLength = FunctionUnaryToType<StringLengthImpl, NameStringLength>;
using FunctionCrc32 = FunctionUnaryToType<Crc32Impl, NameCrc32>;
using FunctionStringUTF8Length = FunctionUnaryToType<StringUtf8LengthImpl, NameStringUtf8Length>;
using FunctionStringSpace = FunctionUnaryToType<StringSpace, NameStringSpace>;
using FunctionStringStartsWith =
        FunctionBinaryToType<DataTypeString, DataTypeString, StringStartsWithImpl, NameStartsWith>;
using FunctionStringEndsWith =
        FunctionBinaryToType<DataTypeString, DataTypeString, StringEndsWithImpl, NameEndsWith>;
using FunctionStringInstr =
        FunctionBinaryToType<DataTypeString, DataTypeString, StringInStrImpl, NameInstr>;
using FunctionStringLocate =
        FunctionBinaryToType<DataTypeString, DataTypeString, StringLocateImpl, NameLocate>;
using FunctionStringFindInSet =
        FunctionBinaryToType<DataTypeString, DataTypeString, StringFindInSetImpl, NameFindInSet>;

using FunctionQuote = FunctionStringToString<NameQuoteImpl, NameQuote>;

using FunctionToLower = FunctionStringToString<TransferImpl<NameToLower>, NameToLower>;

using FunctionToUpper = FunctionStringToString<TransferImpl<NameToUpper>, NameToUpper>;

using FunctionToInitcap = FunctionStringToString<InitcapImpl, NameToInitcap>;

using FunctionUnHex = FunctionStringEncode<UnHexImpl>;
using FunctionToBase64 = FunctionStringEncode<ToBase64Impl>;
using FunctionFromBase64 = FunctionStringOperateToNullType<FromBase64Impl>;

using FunctionStringAppendTrailingCharIfAbsent =
        FunctionBinaryStringOperateToNullType<StringAppendTrailingCharIfAbsent>;

using FunctionStringLPad = FunctionStringPad<StringLPad>;
using FunctionStringRPad = FunctionStringPad<StringRPad>;

void register_function_string(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionStringASCII>();
    factory.register_function<FunctionStringLength>();
    factory.register_function<FunctionCrc32>();
    factory.register_function<FunctionStringUTF8Length>();
    factory.register_function<FunctionStringSpace>();
    factory.register_function<FunctionStringStartsWith>();
    factory.register_function<FunctionStringEndsWith>();
    factory.register_function<FunctionStringInstr>();
    factory.register_function<FunctionStringFindInSet>();
    factory.register_function<FunctionStringLocate>();
    factory.register_function<FunctionStringLocatePos>();
    factory.register_function<FunctionQuote>();
    factory.register_function<FunctionAutoPartitionName>();
    factory.register_function<FunctionReverseCommon>();
    factory.register_function<FunctionUnHex>();
    factory.register_function<FunctionToLower>();
    factory.register_function<FunctionToUpper>();
    factory.register_function<FunctionToInitcap>();
    factory.register_function<FunctionTrim<Trim1Impl<true, true, NameTrim>>>();
    factory.register_function<FunctionTrim<Trim1Impl<true, false, NameLTrim>>>();
    factory.register_function<FunctionTrim<Trim1Impl<false, true, NameRTrim>>>();
    factory.register_function<FunctionTrim<Trim2Impl<true, true, NameTrim>>>();
    factory.register_function<FunctionTrim<Trim2Impl<true, false, NameLTrim>>>();
    factory.register_function<FunctionTrim<Trim2Impl<false, true, NameRTrim>>>();
    factory.register_function<FunctionConvertTo>();
    factory.register_function<FunctionSubstring<Substr3Impl>>();
    factory.register_function<FunctionSubstring<Substr2Impl>>();
    factory.register_function<FunctionLeft>();
    factory.register_function<FunctionRight>();
    factory.register_function<FunctionNullOrEmpty>();
    factory.register_function<FunctionNotNullOrEmpty>();
    factory.register_function<FunctionStringConcat>();
    factory.register_function<FunctionIntToChar>();
    factory.register_function<FunctionStringElt>();
    factory.register_function<FunctionStringConcatWs>();
    factory.register_function<FunctionStringAppendTrailingCharIfAbsent>();
    factory.register_function<FunctionStringRepeat>();
    factory.register_function<FunctionStringLPad>();
    factory.register_function<FunctionStringRPad>();
    factory.register_function<FunctionToBase64>();
    factory.register_function<FunctionFromBase64>();
    factory.register_function<FunctionSplitPart>();
    factory.register_function<FunctionSplitByString>();
    factory.register_function<FunctionSubstringIndex>();
    factory.register_function<FunctionExtractURLParameter>();
    factory.register_function<FunctionStringParseUrl>();
    factory.register_function<FunctionUrlDecode>();
    factory.register_function<FunctionRandomBytes>();
    factory.register_function<FunctionMoneyFormat<MoneyFormatDoubleImpl>>();
    factory.register_function<FunctionMoneyFormat<MoneyFormatInt64Impl>>();
    factory.register_function<FunctionMoneyFormat<MoneyFormatInt128Impl>>();
    factory.register_function<FunctionMoneyFormat<MoneyFormatDecimalImpl>>();
    factory.register_function<FunctionStringDigestOneArg<SM3Sum>>();
    factory.register_function<FunctionStringDigestOneArg<MD5Sum>>();
    factory.register_function<FunctionStringDigestSHA1>();
    factory.register_function<FunctionStringDigestSHA2>();
    factory.register_function<FunctionReplace<ReplaceImpl, true>>();
    factory.register_function<FunctionReplace<ReplaceEmptyImpl, false>>();
    factory.register_function<FunctionMask>();
    factory.register_function<FunctionMaskPartial<true>>();
    factory.register_function<FunctionMaskPartial<false>>();
    factory.register_function<FunctionSubReplace<SubReplaceThreeImpl>>();
    factory.register_function<FunctionSubReplace<SubReplaceFourImpl>>();
    factory.register_function<FunctionOverlay>();
    factory.register_function<FunctionStrcmp>();
    factory.register_function<FunctionNgramSearch>();

    factory.register_alias(FunctionLeft::name, "strleft");
    factory.register_alias(FunctionRight::name, "strright");
    factory.register_alias(SubstringUtil::name, "substr");
    factory.register_alias(FunctionToLower::name, "lcase");
    factory.register_alias(FunctionToUpper::name, "ucase");
    factory.register_alias(FunctionStringDigestOneArg<MD5Sum>::name, "md5");
    factory.register_alias(FunctionStringUTF8Length::name, "character_length");
    factory.register_alias(FunctionStringDigestOneArg<SM3Sum>::name, "sm3");
    factory.register_alias(FunctionStringDigestSHA1::name, "sha");
}

} // namespace doris::vectorized
