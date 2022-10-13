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

#include <re2/re2.h>

#include <cstddef>
#include <cstdlib>
#include <string_view>

#include "runtime/string_search.hpp"
#include "util/url_coding.h"
#include "vec/common/pod_array_fwd.h"
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

struct NameStringLenght {
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
            res[i] = get_char_len(StringValue(raw_str, str_size), str_size);
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
        for (int i = 0; i < strl.length(); ++i) {
            if (strl[i] == ',') {
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

// the same impl as instr
struct NameLocate {
    static constexpr auto name = "locate";
};

struct InStrOP {
    using ResultDataType = DataTypeInt32;
    using ResultPaddedPODArray = PaddedPODArray<Int32>;
    static void execute(const std::string_view& strl, const std::string_view& strr, int32_t& res) {
        if (strr.length() == 0) {
            res = 1;
            return;
        }

        StringValue str_sv(strl.data(), strl.length());
        StringValue substr_sv(strr.data(), strr.length());
        StringSearch search(&substr_sv);
        // Hive returns positions starting from 1.
        int loc = search.search(&str_sv);
        if (loc > 0) {
            loc = get_char_len(str_sv, loc);
        }

        res = loc + 1;
    }
};
struct LocateOP {
    using ResultDataType = DataTypeInt32;
    using ResultPaddedPODArray = PaddedPODArray<Int32>;
    static void execute(const std::string_view& strl, const std::string_view& strr, int32_t& res) {
        InStrOP::execute(strr, strl, res);
    }
};

// LeftDataType and RightDataType are DataTypeString
template <typename LeftDataType, typename RightDataType, typename OP>
struct StringFunctionImpl {
    using ResultDataType = typename OP::ResultDataType;
    using ResultPaddedPODArray = typename OP::ResultPaddedPODArray;

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

            const char* r_raw_str = reinterpret_cast<const char*>(&rdata[roffsets[i - 1]]);
            int r_str_size = roffsets[i] - roffsets[i - 1];

            std::string_view lview(l_raw_str, l_str_size);
            std::string_view rview(r_raw_str, r_str_size);

            OP::execute(lview, rview, res[i]);
        }

        return Status::OK();
    }
};

struct NameToLower {
    static constexpr auto name = "lower";
};

struct NameToUpper {
    static constexpr auto name = "upper";
};

using char_transter_op = int (*)(int);
template <char_transter_op op>
struct TransferImpl {
    static Status vector(const ColumnString::Chars& data, const ColumnString::Offsets& offsets,
                         ColumnString::Chars& res_data, ColumnString::Offsets& res_offsets) {
        size_t offset_size = offsets.size();
        res_offsets.resize(offsets.size());
        for (size_t i = 0; i < offset_size; ++i) {
            res_offsets[i] = offsets[i];
        }

        size_t data_length = data.size();
        res_data.resize(data_length);
        for (size_t i = 0; i < data_length; ++i) {
            res_data[i] = op(data[i]);
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
        memcpy(res_offsets.data(), offsets.data(), offset_size * sizeof(offsets.data()));

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

template <bool is_ltrim, bool is_rtrim>
struct TrimImpl {
    static Status vector(const ColumnString::Chars& data, const ColumnString::Offsets& offsets,
                         ColumnString::Chars& res_data, ColumnString::Offsets& res_offsets) {
        size_t offset_size = offsets.size();
        res_offsets.resize(offsets.size());

        for (size_t i = 0; i < offset_size; ++i) {
            const char* raw_str = reinterpret_cast<const char*>(&data[offsets[i - 1]]);
            ColumnString::Offset size = offsets[i] - offsets[i - 1];
            StringVal str(raw_str, size);
            if constexpr (is_ltrim) {
                str = simd::VStringFunctions::ltrim(str);
            }
            if constexpr (is_rtrim) {
                str = simd::VStringFunctions::rtrim(str);
            }
            StringOP::push_value_string(std::string_view((char*)str.ptr, str.len), i, res_data,
                                        res_offsets);
        }
        return Status::OK();
    }
};

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
                         ColumnString::Chars& dst_data, ColumnString::Offsets& dst_offsets,
                         NullMap& null_map) {
        auto rows_count = offsets.size();
        dst_offsets.resize(rows_count);

        for (int i = 0; i < rows_count; ++i) {
            if (null_map[i]) {
                StringOP::push_null_string(i, dst_data, dst_offsets, null_map);
                continue;
            }

            auto source = reinterpret_cast<const char*>(&data[offsets[i - 1]]);
            size_t srclen = offsets[i] - offsets[i - 1];

            if (srclen == 0) {
                StringOP::push_empty_string(i, dst_data, dst_offsets);
                continue;
            }

            int cipher_len = srclen / 2;
            char dst[cipher_len];
            int outlen = hex_decode(source, srclen, dst);

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
        fmt::memory_buffer buffer;
        for (size_t i = 0; i < input_size; ++i) {
            buffer.clear();
            if (data[i] > 0) {
                for (size_t j = 0; j < data[i]; ++j) {
                    buffer.push_back(' ');
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
                         ColumnString::Chars& dst_data, ColumnString::Offsets& dst_offsets,
                         NullMap& null_map) {
        auto rows_count = offsets.size();
        dst_offsets.resize(rows_count);

        for (int i = 0; i < rows_count; ++i) {
            if (null_map[i]) {
                StringOP::push_null_string(i, dst_data, dst_offsets, null_map);
                continue;
            }

            auto source = reinterpret_cast<const char*>(&data[offsets[i - 1]]);
            size_t srclen = offsets[i] - offsets[i - 1];

            if (srclen == 0) {
                StringOP::push_null_string(i, dst_data, dst_offsets, null_map);
                continue;
            }

            int cipher_len = (int)(4.0 * ceil((double)srclen / 3.0));
            char dst[cipher_len];
            int outlen = base64_encode((const unsigned char*)source, srclen, (unsigned char*)dst);

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

            auto source = reinterpret_cast<const char*>(&data[offsets[i - 1]]);
            size_t srclen = offsets[i] - offsets[i - 1];

            if (srclen == 0) {
                StringOP::push_null_string(i, dst_data, dst_offsets, null_map);
                continue;
            }

            int cipher_len = srclen;
            char dst[cipher_len];
            int outlen = base64_decode(source, srclen, dst);

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
using StringInstrImpl = StringFunctionImpl<LeftDataType, RightDataType, InStrOP>;

template <typename LeftDataType, typename RightDataType>
using StringLocateImpl = StringFunctionImpl<LeftDataType, RightDataType, LocateOP>;

template <typename LeftDataType, typename RightDataType>
using StringFindInSetImpl = StringFunctionImpl<LeftDataType, RightDataType, FindInSetOp>;

// ready for regist function
using FunctionStringASCII = FunctionUnaryToType<StringASCII, NameStringASCII>;
using FunctionStringLength = FunctionUnaryToType<StringLengthImpl, NameStringLenght>;
using FunctionStringUTF8Length = FunctionUnaryToType<StringUtf8LengthImpl, NameStringUtf8Length>;
using FunctionStringSpace = FunctionUnaryToType<StringSpace, NameStringSpace>;
using FunctionStringStartsWith =
        FunctionBinaryToType<DataTypeString, DataTypeString, StringStartsWithImpl, NameStartsWith>;
using FunctionStringEndsWith =
        FunctionBinaryToType<DataTypeString, DataTypeString, StringEndsWithImpl, NameEndsWith>;
using FunctionStringInstr =
        FunctionBinaryToType<DataTypeString, DataTypeString, StringInstrImpl, NameInstr>;
using FunctionStringLocate =
        FunctionBinaryToType<DataTypeString, DataTypeString, StringLocateImpl, NameLocate>;
using FunctionStringFindInSet =
        FunctionBinaryToType<DataTypeString, DataTypeString, StringFindInSetImpl, NameFindInSet>;

using FunctionUnHex = FunctionStringOperateToNullType<UnHexImpl>;

using FunctionToLower = FunctionStringToString<TransferImpl<::tolower>, NameToLower>;

using FunctionToUpper = FunctionStringToString<TransferImpl<::toupper>, NameToUpper>;

using FunctionToInitcap = FunctionStringToString<InitcapImpl, NameToInitcap>;

using FunctionLTrim = FunctionStringToString<TrimImpl<true, false>, NameLTrim>;

using FunctionRTrim = FunctionStringToString<TrimImpl<false, true>, NameRTrim>;

using FunctionTrim = FunctionStringToString<TrimImpl<true, true>, NameTrim>;

using FunctionToBase64 = FunctionStringOperateToNullType<ToBase64Impl>;

using FunctionFromBase64 = FunctionStringOperateToNullType<FromBase64Impl>;

using FunctionStringAppendTrailingCharIfAbsent =
        FunctionBinaryStringOperateToNullType<StringAppendTrailingCharIfAbsent>;

using FunctionStringLPad = FunctionStringPad<StringLPad>;
using FunctionStringRPad = FunctionStringPad<StringRPad>;

void register_function_string(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionStringASCII>();
    factory.register_function<FunctionStringLength>();
    factory.register_function<FunctionStringUTF8Length>();
    factory.register_function<FunctionStringSpace>();
    factory.register_function<FunctionStringStartsWith>();
    factory.register_function<FunctionStringEndsWith>();
    factory.register_function<FunctionStringInstr>();
    factory.register_function<FunctionStringFindInSet>();
    factory.register_function<FunctionStringLocate>();
    factory.register_function<FunctionStringLocatePos>();
    factory.register_function<FunctionReverseCommon>();
    factory.register_function<FunctionUnHex>();
    factory.register_function<FunctionToLower>();
    factory.register_function<FunctionToUpper>();
    factory.register_function<FunctionToInitcap>();
    factory.register_function<FunctionLTrim>();
    factory.register_function<FunctionRTrim>();
    factory.register_function<FunctionTrim>();
    factory.register_function<FunctionSubstring<Substr3Impl>>();
    factory.register_function<FunctionSubstring<Substr2Impl>>();
    factory.register_function<FunctionLeft>();
    factory.register_function<FunctionRight>();
    factory.register_function<FunctionNullOrEmpty>();
    factory.register_function<FunctionStringConcat>();
    factory.register_function<FunctionStringElt>();
    factory.register_function<FunctionStringConcatWs>();
    factory.register_function<FunctionStringAppendTrailingCharIfAbsent>();
    factory.register_function<FunctionStringRepeat>();
    factory.register_function<FunctionStringLPad>();
    factory.register_function<FunctionStringRPad>();
    factory.register_function<FunctionToBase64>();
    factory.register_function<FunctionFromBase64>();
    factory.register_function<FunctionSplitPart>();
    factory.register_function<FunctionStringMd5AndSM3<MD5Sum>>();
    factory.register_function<FunctionStringParseUrl>();
    factory.register_function<FunctionMoneyFormat<MoneyFormatDoubleImpl>>();
    factory.register_function<FunctionMoneyFormat<MoneyFormatInt64Impl>>();
    factory.register_function<FunctionMoneyFormat<MoneyFormatInt128Impl>>();
    factory.register_function<FunctionMoneyFormat<MoneyFormatDecimalImpl>>();
    factory.register_function<FunctionStringMd5AndSM3<SM3Sum>>();
    factory.register_function<FunctionReplace>();

    factory.register_alias(FunctionLeft::name, "strleft");
    factory.register_alias(FunctionRight::name, "strright");
    factory.register_alias(SubstringUtil::name, "substr");
    factory.register_alias(FunctionToLower::name, "lcase");
    factory.register_alias(FunctionToUpper::name, "ucase");
    factory.register_alias(FunctionStringMd5AndSM3<MD5Sum>::name, "md5");
    factory.register_alias(FunctionStringUTF8Length::name, "character_length");
    factory.register_alias(FunctionStringMd5AndSM3<SM3Sum>::name, "sm3");
}

} // namespace doris::vectorized
