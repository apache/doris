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

#include "util/string_parser.hpp"
#include "vec/functions/function_totype.h"
#include "vec/functions/function_const.h"
#include "vec/functions/simple_function_factory.h"
#include "gutil/strings/numbers.h"
#include "gutil/strings/split.h"

namespace doris::vectorized {

struct BitmapEmpty {
    static constexpr auto name = "bitmap_empty";
    using ReturnColVec = ColumnBitmap;
    static DataTypePtr get_return_type() {
        return std::make_shared<DataTypeBitMap>();
    }
    static auto init_value() {
        return BitmapValue{};
    }
};

struct NameToBitmap {
    static constexpr auto name = "to_bitmap";
};

struct ToBitmapImpl {
    using ReturnType = DataTypeBitMap;
    static constexpr auto TYPE_INDEX = TypeIndex::String;
    using Type = String;
    using ReturnColumnType = ColumnBitmap;

    static Status vector(const ColumnString::Chars& data, const ColumnString::Offsets& offsets,
                         std::vector<BitmapValue>& res) {
        auto size = offsets.size();
        res.reserve(size);
        for (int i = 0; i < size; ++i) {
            const char* raw_str = reinterpret_cast<const char*>(&data[offsets[i - 1]]);
            int str_size = offsets[i] - offsets[i - 1] - 1;
            StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
            uint64_t int_value = StringParser::string_to_unsigned_int<uint64_t>(raw_str, str_size,
                                                                                &parse_result);

            if (UNLIKELY(parse_result != StringParser::PARSE_SUCCESS)) {
                return Status::RuntimeError(
                        fmt::format("The input: {:.{}} is not valid, to_bitmap only support bigint "
                                    "value from 0 to 18446744073709551615 currently",
                                    raw_str, str_size));
            }
            res.emplace_back();
            res.back().add(int_value);
        }
        return Status::OK();
    }
};

struct NameBitmapFromString {
    static constexpr auto name = "bitmap_from_string";
};

struct BitmapFromString {
    using ReturnType = DataTypeBitMap;
    static constexpr auto TYPE_INDEX = TypeIndex::String;
    using Type = String;
    using ReturnColumnType = ColumnBitmap;
    static Status vector(const ColumnString::Chars& data, const ColumnString::Offsets& offsets,
                         std::vector<BitmapValue>& res) {
        auto size = offsets.size();
        res.reserve(size);
        std::vector<uint64_t> bits;
        for (int i = 0; i < size; ++i) {
            const char* raw_str = reinterpret_cast<const char*>(&data[offsets[i - 1]]);
            int str_size = offsets[i] - offsets[i - 1] - 1;
            if (SplitStringAndParse({raw_str, str_size},
                                                        ",", &safe_strtou64, &bits)) {
                res.emplace_back(bits);
            } else {
                res.emplace_back();
            }
            bits.clear();
        }
        return Status::OK();
    }
};

struct NameBitmapHash {
    static constexpr auto name = "bitmap_hash";
};

struct BitmapHash {
    using ReturnType = DataTypeBitMap;
    static constexpr auto TYPE_INDEX = TypeIndex::String;
    using Type = String;
    using ReturnColumnType = ColumnBitmap;
    static Status vector(const ColumnString::Chars& data, const ColumnString::Offsets& offsets,
                         std::vector<BitmapValue>& res) {
        auto size = offsets.size();
        res.reserve(size);
        for (int i = 0; i < size; ++i) {
            const char* raw_str = reinterpret_cast<const char*>(&data[offsets[i - 1]]);
            int str_size = offsets[i] - offsets[i - 1] - 1;
            uint32_t hash_value =
                HashUtil::murmur_hash3_32(raw_str, str_size, HashUtil::MURMUR3_32_SEED);
            res.emplace_back();
            res.back().add(hash_value);
        }
        return Status::OK();
    }
};

struct NameBitmapCount {
    static constexpr auto name = "bitmap_count";
};

struct BitmapCount {
    using ReturnType = DataTypeInt64;
    static constexpr auto TYPE_INDEX = TypeIndex::BitMap;
    using Type = DataTypeBitMap::FieldType;
    using ReturnColumnType = ColumnVector<Int64>;
    using ReturnColumnContainer = ColumnVector<Int64>::Container;

    static Status vector(const std::vector<BitmapValue>& data, ReturnColumnContainer& res) {
        int size = data.size();
        res.reserve(size);
        for (int i = 0; i < size; ++i) {
            res.push_back(data[i].cardinality());
        }
        return Status::OK();
    }
};

struct NameBitmapAnd {
    static constexpr auto name = "bitmap_and";
};

template <typename LeftDataType, typename RightDataType>
struct BitmapAnd {
    using ResultDataType = DataTypeBitMap;
    using T0 = typename LeftDataType::FieldType;
    using T1 = typename RightDataType::FieldType;
    using TData = std::vector<BitmapValue>;

    static Status vector_vector(const TData& lvec, const TData& rvec, TData& res) {
        int size = lvec.size();
        for (int i = 0; i < size; ++i) {
            res[i] = lvec[i];
            res[i] &= rvec[i];
        }
        return Status::OK();
    }
};

struct NameBitmapOr {
    static constexpr auto name = "bitmap_or";
};

template <typename LeftDataType, typename RightDataType>
struct BitmapOr {
    using ResultDataType = DataTypeBitMap;
    using T0 = typename LeftDataType::FieldType;
    using T1 = typename RightDataType::FieldType;
    using TData = std::vector<BitmapValue>;

    static Status vector_vector(const TData& lvec, const TData& rvec, TData& res) {
        int size = lvec.size();
        for (int i = 0; i < size; ++i) {
            res[i] = lvec[i];
            res[i] |= rvec[i];
        }
        return Status::OK();
    }
};

struct NameBitmapXor {
    static constexpr auto name = "bitmap_xor";
};

template <typename LeftDataType, typename RightDataType>
struct BitmapXor {
    using ResultDataType = DataTypeBitMap;
    using T0 = typename LeftDataType::FieldType;
    using T1 = typename RightDataType::FieldType;
    using TData = std::vector<BitmapValue>;

    static Status vector_vector(const TData& lvec, const TData& rvec, TData& res) {
        int size = lvec.size();
        for (int i = 0; i < size; ++i) {
            res[i] = lvec[i];
            res[i] ^= rvec[i];
        }
        return Status::OK();
    }
};

struct NameBitmapNot {
    static constexpr auto name = "bitmap_not";
};

template <typename LeftDataType, typename RightDataType>
struct BitmapNot {
    using ResultDataType = DataTypeBitMap;
    using T0 = typename LeftDataType::FieldType;
    using T1 = typename RightDataType::FieldType;
    using TData = std::vector<BitmapValue>;

    static Status vector_vector(const TData& lvec, const TData& rvec, TData& res) {
        int size = lvec.size();
        for (int i = 0; i < size; ++i) {
            res[i] = lvec[i];
            res[i] -= rvec[i];
        }
        return Status::OK();
    }
};

struct NameBitmapContains {
    static constexpr auto name = "bitmap_contains";
};

template <typename LeftDataType, typename RightDataType>
struct BitmapContains {
    using ResultDataType = DataTypeInt8;
    using T0 = typename LeftDataType::FieldType;
    using T1 = typename RightDataType::FieldType;
    using LTData = std::vector<BitmapValue>;
    using RTData = typename ColumnVector<T1>::Container;
    using ResTData = typename ColumnVector<Int8>::Container;

    static Status vector_vector(const LTData& lvec, const RTData& rvec, ResTData& res) {
        int size = lvec.size();
        for (int i = 0; i < size; ++i) {
            res[i] = lvec[i].contains(rvec[i]);
        }
        return Status::OK();
    }
};

struct NameBitmapHasAny {
    static constexpr auto name = "bitmap_has_any";
};

template <typename LeftDataType, typename RightDataType>
struct BitmapHasAny {
    using ResultDataType = DataTypeInt8;
    using T0 = typename LeftDataType::FieldType;
    using T1 = typename RightDataType::FieldType;
    using TData = std::vector<BitmapValue>;
    using ResTData = typename ColumnVector<Int8>::Container;

    static Status vector_vector(const TData& lvec, const TData& rvec, ResTData& res) {
        int size = lvec.size();
        for (int i = 0; i < size; ++i) {
            BitmapValue bitmap = lvec[i];
            bitmap &= rvec[i];
            res[i] = bitmap.cardinality() != 0;
        }
        return Status::OK();
    }
};

using FunctionBitmapEmpty = FunctionConst<BitmapEmpty, false>;
using FunctionToBitmap = FunctionUnaryToType<ToBitmapImpl, NameToBitmap>;
using FunctionBitmapFromString = FunctionUnaryToType<BitmapFromString,NameBitmapFromString>;
using FunctionBitmapHash = FunctionUnaryToType<BitmapHash, NameBitmapHash>;

using FunctionBitmapCount = FunctionUnaryToType<BitmapCount, NameBitmapCount>;

using FunctionBitmapAnd =
        FunctionBinaryToType<DataTypeBitMap, DataTypeBitMap, BitmapAnd, NameBitmapAnd>;
using FunctionBitmapOr =
        FunctionBinaryToType<DataTypeBitMap, DataTypeBitMap, BitmapOr, NameBitmapOr>;
using FunctionBitmapXor =
        FunctionBinaryToType<DataTypeBitMap, DataTypeBitMap, BitmapXor, NameBitmapXor>;
using FunctionBitmapNot =
        FunctionBinaryToType<DataTypeBitMap, DataTypeBitMap, BitmapNot, NameBitmapNot>;

using FunctionBitmapContains =
        FunctionBinaryToType<DataTypeBitMap, DataTypeInt64, BitmapContains, NameBitmapContains>;

using FunctionBitmapHasAny =
        FunctionBinaryToType<DataTypeBitMap, DataTypeBitMap, BitmapHasAny, NameBitmapHasAny>;

void register_function_bitmap(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionBitmapEmpty>();
    factory.register_function<FunctionToBitmap>();
    factory.register_function<FunctionBitmapFromString>();
    factory.register_function<FunctionBitmapHash>();
    factory.register_function<FunctionBitmapCount>();
    factory.register_function<FunctionBitmapAnd>();
    factory.register_function<FunctionBitmapOr>();
    factory.register_function<FunctionBitmapXor>();
    factory.register_function<FunctionBitmapNot>();
    factory.register_function<FunctionBitmapContains>();
    factory.register_function<FunctionBitmapHasAny>();
}

} // namespace doris::vectorized
