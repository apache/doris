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
#include "vec/common/pod_array_fwd.h"
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
            // if strlen(raw_str) == 0, raw_str[0] is '\0'
            res[i] = raw_str[0];
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
            int str_size = offsets[i] - offsets[i - 1] - 1;
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
            int str_size = offsets[i] - offsets[i - 1] - 1;

            size_t char_len = 0;
            for (size_t i = 0, char_size = 0; i < str_size; i += char_size) {
                char_size = get_utf8_byte_length((unsigned)(raw_str)[i]);
                ++char_len;
            }

            res[i] = char_len;
        }
        return Status::OK();
    }
};

struct NameStartsWith {
    static constexpr auto name = "starts_with";
};

struct StartsWithOp {
    using ResultDataType = DataTypeInt8;
    using ResultPaddedPODArray = PaddedPODArray<Int8>;

    static void execute(const std::string_view& strl, const std::string_view& strr, int8_t& res) {
        re2::StringPiece str_sp(const_cast<char*>(strl.data()), strl.length());
        re2::StringPiece prefix_sp(const_cast<char*>(strr.data()), strr.length());
        res = str_sp.starts_with(prefix_sp);
    }
};

struct NameEndsWith {
    static constexpr auto name = "ends_with";
};

struct EndsWithOp {
    using ResultDataType = DataTypeInt8;
    using ResultPaddedPODArray = PaddedPODArray<Int8>;

    static void execute(const std::string_view& strl, const std::string_view& strr, int8_t& res) {
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
            while (strr[end] != ',' && end < strr.length()) {
                ++end;
            }

            if (strl == std::string_view{strr.data() + start, (size_t)end - start}) {
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

        StringValue str_sv(const_cast<char*>(strl.data()), strl.length());
        StringValue substr_sv(const_cast<char*>(strr.data()), strr.length());
        StringSearch search(&substr_sv);
        // Hive returns positions starting from 1.
        int loc = search.search(&str_sv);
        if (loc > 0) {
            size_t char_len = 0;
            for (size_t i = 0, char_size = 0; i < loc; i += char_size) {
                char_size = get_utf8_byte_length((unsigned)(strl.data())[i]);
                ++char_len;
            }
            loc = char_len;
        }

        res = loc + 1;
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
            int l_str_size = loffsets[i] - loffsets[i - 1] - 1;

            const char* r_raw_str = reinterpret_cast<const char*>(&rdata[roffsets[i - 1]]);
            int r_str_size = roffsets[i] - roffsets[i - 1] - 1;

            std::string_view lview(l_raw_str, l_str_size);
            std::string_view rview(r_raw_str, r_str_size);

            OP::execute(lview, rview, res[i]);
        }

        return Status::OK();
    }
};

struct NameReverse {
    static constexpr auto name = "reverse";
};

struct ReverseImpl {
    static Status vector(const ColumnString::Chars& data, const ColumnString::Offsets& offsets,
                         ColumnString::Chars& res_data, ColumnString::Offsets& res_offsets) {
        auto size = offsets.size();
        res_offsets.resize(size);
        for (int i = 0; i < size; ++i) {
            res_offsets[i] = offsets[i];
        }

        size_t data_length = data.size();
        res_data.resize(data_length);
        for (int i = 0; i < size; ++i) {
            const char* l_raw_str = reinterpret_cast<const char*>(&data[offsets[i - 1]]);
            int l_str_size = offsets[i] - offsets[i - 1] - 1;

            char* r_raw_str = reinterpret_cast<char*>(&res_data[res_offsets[i - 1]]);
            memcpy(r_raw_str, l_raw_str, l_str_size);

            // reserve
            for (size_t j = 0, char_size = 0; j < l_str_size; j += char_size) {
                char_size = get_utf8_byte_length((unsigned)(r_raw_str)[j]);
                std::copy(l_raw_str + j, l_raw_str + j + char_size,
                          r_raw_str + l_str_size - j - char_size);
            }
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
        res_data.reserve(data.size());

        for (size_t i = 0; i < offset_size; ++i) {
            const char* raw_str = reinterpret_cast<const char*>(&data[offsets[i - 1]]);
            int str_size = offsets[i] - offsets[i - 1] - 1;

            int32_t begin = 0;
            if constexpr (is_ltrim) {
                while (begin < str_size && raw_str[begin] == ' ') {
                    ++begin;
                }
            }

            int32_t end = str_size - 1;
            if constexpr (is_rtrim) {
                while (end > begin && raw_str[end] == ' ') {
                    --end;
                }
            }

            int nstr_size = end - begin + 1;
            res_offsets[i] = res_offsets[i - 1] + nstr_size + 1;
            res_data.insert_assume_reserved(raw_str + begin, raw_str + begin + nstr_size);
            // memcpy(reinterpret_cast<char*>(&res_data[res_offsets[i - 1]]) ,raw_str + begin, nstr_size);
            res_data[res_offsets[i] - 1] = '0';
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

struct StringAppendTrailingCharIfAbsent {
    static constexpr auto name = "append_trailing_char_if_absent";
    using Chars = ColumnString::Chars;
    using Offsets = ColumnString::Offsets;
    using ReturnType = DataTypeString;
    using ColumnType = ColumnString;
    static void vector_vector(const Chars& ldata, const Offsets& loffsets, const Chars& rdata,
                              const Offsets& roffsets, Chars& res_data, Offsets& res_offsets,
                              NullMap& null_map_data) {
        DCHECK_EQ(loffsets.size(), roffsets.size());
        size_t input_rows_count = loffsets.size();
        res_offsets.resize(input_rows_count);
        for (size_t i = 0; i < input_rows_count; ++i) {
            int l_size = loffsets[i] - loffsets[i - 1] - 1;
            const auto l_raw = reinterpret_cast<const char*>(&ldata[loffsets[i - 1]]);

            int r_size = roffsets[i] - roffsets[i - 1] - 1;
            const auto r_raw = reinterpret_cast<const char*>(&rdata[loffsets[i - 1]]);

            if (r_size == 0) {
                StringOP::push_null_string(i, res_data, res_offsets, null_map_data);
                continue;
            }
            if (l_raw[l_size - 1] == r_raw[0]) {
                StringOP::push_value_string(std::string_view(l_raw, l_size), i, res_data,
                                            res_offsets);
                continue;
            }
            StringOP::push_value_string(std::string_view(l_raw, l_size), i, res_data, res_offsets);
            res_data[res_data.size() - 1] = *r_raw;
            res_data.push_back('\0');
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
        FunctionBinaryToType<DataTypeString, DataTypeString, StringInstrImpl, NameLocate>;
using FunctionStringFindInSet =
        FunctionBinaryToType<DataTypeString, DataTypeString, StringFindInSetImpl, NameFindInSet>;

using FunctionReverse = FunctionStringToString<ReverseImpl, NameReverse>;

using FunctionToLower = FunctionStringToString<TransferImpl<::tolower>, NameToLower>;

using FunctionToUpper = FunctionStringToString<TransferImpl<::toupper>, NameToUpper>;

using FunctionLTrim = FunctionStringToString<TrimImpl<true, false>, NameLTrim>;

using FunctionRTrim = FunctionStringToString<TrimImpl<false, true>, NameRTrim>;

using FunctionTrim = FunctionStringToString<TrimImpl<true, true>, NameTrim>;

using FunctionStringAppendTrailingCharIfAbsent =
        FunctionBinaryStringOperateToNullType<StringAppendTrailingCharIfAbsent>;

using FunctionStringLPad = FunctionStringPad<StringLPad>;
using FunctionStringRPad = FunctionStringPad<StringRPad>;

void register_function_string(SimpleFunctionFactory& factory) {
    // factory.register_function<>();
    factory.register_function<FunctionStringASCII>();
    factory.register_function<FunctionStringLength>();
    factory.register_function<FunctionStringUTF8Length>();
    factory.register_function<FunctionStringSpace>();
    factory.register_function<FunctionStringStartsWith>();
    factory.register_function<FunctionStringEndsWith>();
    factory.register_function<FunctionStringInstr>();
    factory.register_function<FunctionStringFindInSet>();
    factory.register_function<FunctionStringLocate>();
    factory.register_function<FunctionReverse>();
    factory.register_function<FunctionToLower>();
    factory.register_function<FunctionToUpper>();
    factory.register_function<FunctionLTrim>();
    factory.register_function<FunctionRTrim>();
    factory.register_function<FunctionTrim>();
    factory.register_function<FunctionSubstring>();
    factory.register_function<FunctionLeft>();
    factory.register_function<FunctionRight>();
    factory.register_function<FunctionNullOrEmpty>();
    factory.register_function<FunctionStringConcat>();
    factory.register_function<FunctionStringConcatWs>();
    factory.register_function<FunctionStringAppendTrailingCharIfAbsent>();
    factory.register_function<FunctionStringRepeat>();
    factory.register_function<FunctionStringLPad>();
    factory.register_function<FunctionStringRPad>();

    factory.register_alias(FunctionLeft::name, "strleft");
    factory.register_alias(FunctionRight::name, "strright");
    factory.register_alias(FunctionSubstring::name, "substr");
    factory.register_alias(FunctionToLower::name, "lcase");
}

} // namespace doris::vectorized