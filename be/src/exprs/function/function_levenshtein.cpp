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

#include <algorithm>
#include <array>
#include <limits>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "core/custom_allocator.h"
#include "core/data_type/data_type_number.h"
#include "core/pod_array.h"
#include "core/string_ref.h"
#include "exprs/function/function_totype.h"
#include "exprs/function/simple_function_factory.h"
#include "util/simd/vstring_function.h"

namespace doris {

struct NameLevenshtein {
    static constexpr auto name = "levenshtein";
};

struct NameDamerauLevenshteinDistance {
    static constexpr auto name = "damerau_levenshtein_distance";
};

// 64MB limit for the distance matrix.
static constexpr size_t MAX_DAMERAU_LEVENSHTEIN_MATRIX_CELLS = 16 * 1024 * 1024;

using Utf8Offsets = DorisVector<size_t>;

static StringRef string_ref_at(const ColumnString::Chars& data,
                               const ColumnString::Offsets& offsets, size_t i) {
    DCHECK_LT(i, offsets.size());
    const auto previous_offset = i == 0 ? 0 : offsets[i - 1];
    return StringRef(data.data() + previous_offset, offsets[i] - previous_offset)
            .trim_tail_padding_zero();
}

static void get_utf8_char_offsets(const StringRef& ref, Utf8Offsets& offsets) {
    offsets.clear();
    offsets.reserve(ref.size);
    for (size_t i = 0, char_size = 0; i < ref.size; i += char_size) {
        char_size = UTF8_BYTE_LENGTH[static_cast<unsigned char>(ref.data[i])];
        offsets.push_back(i);
    }
}

struct LevenshteinDistance {
    static Status ascii(const StringRef& left, const StringRef& right, Int32& result) {
        const StringRef* left_ref = &left;
        const StringRef* right_ref = &right;
        size_t m = left.size;
        size_t n = right.size;

        if (n > m) {
            std::swap(left_ref, right_ref);
            std::swap(m, n);
        }

        std::vector<Int32> prev(n + 1);
        std::vector<Int32> curr(n + 1);
        for (size_t j = 0; j <= n; ++j) {
            prev[j] = static_cast<Int32>(j);
        }

        for (size_t i = 1; i <= m; ++i) {
            curr[0] = static_cast<Int32>(i);
            const char left_char = left_ref->data[i - 1];

            for (size_t j = 1; j <= n; ++j) {
                const Int32 cost = left_char == right_ref->data[j - 1] ? 0 : 1;
                const Int32 insert_cost = curr[j - 1] + 1;
                const Int32 delete_cost = prev[j] + 1;
                const Int32 replace_cost = prev[j - 1] + cost;
                curr[j] = std::min(std::min(insert_cost, delete_cost), replace_cost);
            }
            std::swap(prev, curr);
        }

        result = prev[n];
        return Status::OK();
    }

    static Status utf8(const StringRef& left, const Utf8Offsets& left_offsets,
                       const StringRef& right, const Utf8Offsets& right_offsets, Int32& result) {
        const StringRef* left_ref = &left;
        const StringRef* right_ref = &right;
        const Utf8Offsets* left_offsets_ref = &left_offsets;
        const Utf8Offsets* right_offsets_ref = &right_offsets;
        if (right_offsets_ref->size() > left_offsets_ref->size()) {
            std::swap(left_offsets_ref, right_offsets_ref);
            std::swap(left_ref, right_ref);
        }

        const size_t m = left_offsets_ref->size();
        const size_t n = right_offsets_ref->size();

        std::vector<Int32> prev(n + 1);
        std::vector<Int32> curr(n + 1);
        for (size_t j = 0; j <= n; ++j) {
            prev[j] = static_cast<Int32>(j);
        }

        for (size_t i = 1; i <= m; ++i) {
            curr[0] = static_cast<Int32>(i);
            const size_t left_off = (*left_offsets_ref)[i - 1];
            const size_t left_next = i < m ? (*left_offsets_ref)[i] : left_ref->size;

            for (size_t j = 1; j <= n; ++j) {
                const size_t right_off = (*right_offsets_ref)[j - 1];
                const size_t right_next = j < n ? (*right_offsets_ref)[j] : right_ref->size;

                const Int32 cost =
                        simd::VStringFunctions::utf8_char_equal(*left_ref, left_off, left_next,
                                                                *right_ref, right_off, right_next)
                                ? 0
                                : 1;

                const Int32 insert_cost = curr[j - 1] + 1;
                const Int32 delete_cost = prev[j] + 1;
                const Int32 replace_cost = prev[j - 1] + cost;
                curr[j] = std::min(std::min(insert_cost, delete_cost), replace_cost);
            }
            std::swap(prev, curr);
        }

        result = prev[n];
        return Status::OK();
    }
};

struct DamerauLevenshteinDistance {
    using SymbolId = Int32;
    using SymbolVector = DorisVector<SymbolId>;
    using SymbolMap =
            std::unordered_map<std::string_view, SymbolId, std::hash<std::string_view>,
                               std::equal_to<std::string_view>,
                               CustomStdAllocator<std::pair<const std::string_view, SymbolId>>>;

    // Do not use absl::strings_internal::CappedDamerauLevenshteinDistance here:
    // 1. It is capped: distances greater than cutoff return cutoff + 1, not exact distance.
    // 2. It returns uint8_t and is intended for short strings.
    // 3. It implements the restricted/OSA variant, not full Damerau-Levenshtein.
    // 4. It works on bytes, cannot handle UTF-8 characters.
    static Status ascii(const StringRef& left, const StringRef& right, Int32& result) {
        const size_t m = left.size;
        const size_t n = right.size;
        if (m == 0) {
            result = static_cast<Int32>(n);
            return Status::OK();
        }
        if (n == 0) {
            result = static_cast<Int32>(m);
            return Status::OK();
        }

        size_t matrix_cells = 0;
        RETURN_IF_ERROR(get_matrix_cell_count(m, n, matrix_cells));

        const Int32 max_distance = static_cast<Int32>(m + n);
        std::array<Int32, 256> last_row {};
        PaddedPODArray<Int32> dist;
        dist.resize_fill(matrix_cells, 0);
        auto at = [&](size_t i, size_t j) -> Int32& { return dist[i * (n + 2) + j]; };

        at(0, 0) = max_distance;
        for (size_t i = 0; i <= m; ++i) {
            at(i + 1, 0) = max_distance;
            at(i + 1, 1) = static_cast<Int32>(i);
        }
        for (size_t j = 0; j <= n; ++j) {
            at(0, j + 1) = max_distance;
            at(1, j + 1) = static_cast<Int32>(j);
        }

        for (size_t i = 1; i <= m; ++i) {
            Int32 last_match_col = 0;
            const auto left_symbol = static_cast<unsigned char>(left.data[i - 1]);
            for (size_t j = 1; j <= n; ++j) {
                const auto right_symbol = static_cast<unsigned char>(right.data[j - 1]);
                const Int32 last_match_row = last_row[right_symbol];
                const Int32 transposition_col = last_match_col;
                Int32 cost = 1;
                if (left_symbol == right_symbol) {
                    cost = 0;
                    last_match_col = static_cast<Int32>(j);
                }

                const Int32 replace_cost = at(i, j) + cost;
                const Int32 insert_cost = at(i + 1, j) + 1;
                const Int32 delete_cost = at(i, j + 1) + 1;
                const Int32 transpose_cost = at(last_match_row, transposition_col) +
                                             static_cast<Int32>(i - last_match_row - 1) + 1 +
                                             static_cast<Int32>(j - transposition_col - 1);
                at(i + 1, j + 1) = std::min(std::min(replace_cost, insert_cost),
                                            std::min(delete_cost, transpose_cost));
            }
            last_row[left_symbol] = static_cast<Int32>(i);
        }

        result = at(m + 1, n + 1);
        return Status::OK();
    }

    /*
    * Keep the UTF-8 symbol mapping path. The benchmark below shows that mapping each UTF-8
    * character to an integer symbol first is faster than comparing UTF-8 slices inside the
    * dp loop, because the hot loop only needs integer equality and indexed last-row lookup.
    *
    * --------------------------------------------------------------------------
    * Benchmark                                             Time             CPU
    * --------------------------------------------------------------------------
    * BM_DamerauLevenshtein_UTF8/SymbolMapped_16         1.85 us         1.82 us
    * BM_DamerauLevenshtein_UTF8/DirectCompare_16        6.48 us         6.43 us
    * BM_DamerauLevenshtein_UTF8/SymbolMapped_64         12.6 us         12.6 us
    * BM_DamerauLevenshtein_UTF8/DirectCompare_64         119 us          118 us
    * BM_DamerauLevenshtein_UTF8/SymbolMapped_128        43.4 us         43.1 us
    * BM_DamerauLevenshtein_UTF8/DirectCompare_128        478 us          475 us
    * BM_DamerauLevenshtein_UTF8/SymbolMapped_256         160 us          159 us
    * BM_DamerauLevenshtein_UTF8/DirectCompare_256       1892 us         1882 us
    */
    static Status utf8(const StringRef& left, const Utf8Offsets& left_offsets,
                       const StringRef& right, const Utf8Offsets& right_offsets, Int32& result) {
        SymbolVector left_symbols;
        SymbolVector right_symbols;
        SymbolMap symbol_ids;
        symbol_ids.reserve(left_offsets.size() + right_offsets.size());
        SymbolId next_symbol = 0;
        append_utf8_symbols(left, left_offsets, left_symbols, symbol_ids, next_symbol);
        append_utf8_symbols(right, right_offsets, right_symbols, symbol_ids, next_symbol);
        return by_symbols(left_symbols, right_symbols, next_symbol, result);
    }

private:
    static Status by_symbols(const SymbolVector& left, const SymbolVector& right,
                             size_t alphabet_size, Int32& result) {
        const size_t m = left.size();
        const size_t n = right.size();
        if (m == 0) {
            result = static_cast<Int32>(n);
            return Status::OK();
        }
        if (n == 0) {
            result = static_cast<Int32>(m);
            return Status::OK();
        }

        size_t matrix_cells = 0;
        RETURN_IF_ERROR(get_matrix_cell_count(m, n, matrix_cells));

        const Int32 max_distance = static_cast<Int32>(m + n);
        DorisVector<Int32> last_row(alphabet_size, 0);
        PaddedPODArray<Int32> dist;
        dist.resize_fill(matrix_cells, 0);
        auto at = [&](size_t i, size_t j) -> Int32& { return dist[i * (n + 2) + j]; };

        at(0, 0) = max_distance;
        for (size_t i = 0; i <= m; ++i) {
            at(i + 1, 0) = max_distance;
            at(i + 1, 1) = static_cast<Int32>(i);
        }
        for (size_t j = 0; j <= n; ++j) {
            at(0, j + 1) = max_distance;
            at(1, j + 1) = static_cast<Int32>(j);
        }

        for (size_t i = 1; i <= m; ++i) {
            Int32 last_match_col = 0;
            for (size_t j = 1; j <= n; ++j) {
                const Int32 last_match_row = last_row[right[j - 1]];
                const Int32 transposition_col = last_match_col;
                Int32 cost = 1;
                if (left[i - 1] == right[j - 1]) {
                    cost = 0;
                    last_match_col = static_cast<Int32>(j);
                }

                const Int32 replace_cost = at(i, j) + cost;
                const Int32 insert_cost = at(i + 1, j) + 1;
                const Int32 delete_cost = at(i, j + 1) + 1;
                const Int32 transpose_cost = at(last_match_row, transposition_col) +
                                             static_cast<Int32>(i - last_match_row - 1) + 1 +
                                             static_cast<Int32>(j - transposition_col - 1);
                at(i + 1, j + 1) = std::min(std::min(replace_cost, insert_cost),
                                            std::min(delete_cost, transpose_cost));
            }
            last_row[left[i - 1]] = static_cast<Int32>(i);
        }

        result = at(m + 1, n + 1);
        return Status::OK();
    }

    static Status get_matrix_cell_count(size_t m, size_t n, size_t& matrix_cells) {
        const auto max_size = std::numeric_limits<size_t>::max();
        if (m > max_size - 2 || n > max_size - 2) {
            return Status::InvalidArgument(
                    "damerau_levenshtein_distance input is too large to allocate distance matrix");
        }

        const size_t rows = m + 2;
        const size_t cols = n + 2;
        if (rows > max_size / cols) {
            return Status::InvalidArgument(
                    "damerau_levenshtein_distance distance matrix size overflows");
        }

        matrix_cells = rows * cols;
        if (matrix_cells > MAX_DAMERAU_LEVENSHTEIN_MATRIX_CELLS) {
            return Status::InvalidArgument(
                    "damerau_levenshtein_distance distance matrix is too large: {} cells exceeds "
                    "limit {}",
                    matrix_cells, MAX_DAMERAU_LEVENSHTEIN_MATRIX_CELLS);
        }
        return Status::OK();
    }

    static void append_utf8_symbols(const StringRef& str, const Utf8Offsets& offsets,
                                    SymbolVector& symbols, SymbolMap& symbol_ids,
                                    SymbolId& next_symbol) {
        symbols.reserve(symbols.size() + offsets.size());
        for (size_t i = 0; i < offsets.size(); ++i) {
            const size_t offset = offsets[i];
            const size_t next_offset = i + 1 < offsets.size() ? offsets[i + 1] : str.size;
            const std::string_view symbol(str.data + offset, next_offset - offset);
            auto [it, inserted] = symbol_ids.emplace(symbol, next_symbol);
            if (inserted) {
                ++next_symbol;
            }
            symbols.push_back(it->second);
        }
    }
};

template <typename Distance>
struct StringDistanceImplBase {
    using ResultDataType = DataTypeInt32;
    using ResultPaddedPODArray = PaddedPODArray<Int32>;

    static Status vector_vector(const ColumnString::Chars& ldata,
                                const ColumnString::Offsets& loffsets,
                                const ColumnString::Chars& rdata,
                                const ColumnString::Offsets& roffsets, ResultPaddedPODArray& res) {
        DCHECK_EQ(loffsets.size(), roffsets.size());

        const size_t size = loffsets.size();
        res.resize(size);
        Utf8Offsets left_offsets;
        Utf8Offsets right_offsets;
        for (size_t i = 0; i < size; ++i) {
            RETURN_IF_ERROR(distance(string_ref_at(ldata, loffsets, i),
                                     string_ref_at(rdata, roffsets, i), left_offsets, right_offsets,
                                     res[i]));
        }
        return Status::OK();
    }

    static Status vector_scalar(const ColumnString::Chars& data,
                                const ColumnString::Offsets& offsets, const StringRef& constant,
                                ResultPaddedPODArray& res) {
        return vector_const(data, offsets, constant, res);
    }

    static Status scalar_vector(const StringRef& constant, const ColumnString::Chars& data,
                                const ColumnString::Offsets& offsets, ResultPaddedPODArray& res) {
        return vector_const(data, offsets, constant, res);
    }

private:
    static Status vector_const(const ColumnString::Chars& data,
                               const ColumnString::Offsets& offsets, const StringRef& constant,
                               ResultPaddedPODArray& res) {
        const size_t size = offsets.size();
        res.resize(size);
        const auto constant_ref = constant.trim_tail_padding_zero();
        const bool constant_ascii = simd::VStringFunctions::is_ascii(constant_ref);
        Utf8Offsets constant_offsets;
        get_utf8_char_offsets(constant_ref, constant_offsets);
        Utf8Offsets value_offsets;
        for (size_t i = 0; i < size; ++i) {
            RETURN_IF_ERROR(distance_with_const_offsets(string_ref_at(data, offsets, i),
                                                        value_offsets, constant_ref,
                                                        constant_offsets, constant_ascii, res[i]));
        }
        return Status::OK();
    }

    static Status distance(const StringRef& left, const StringRef& right, Utf8Offsets& left_offsets,
                           Utf8Offsets& right_offsets, Int32& result) {
        const bool left_ascii = simd::VStringFunctions::is_ascii(left);
        const bool right_ascii = simd::VStringFunctions::is_ascii(right);
        if (left_ascii && right_ascii) {
            return Distance::ascii(left, right, result);
        }

        if (left.size == 0) {
            result = static_cast<Int32>(
                    simd::VStringFunctions::get_char_len(right.data, right.size));
            return Status::OK();
        }
        if (right.size == 0) {
            result = static_cast<Int32>(simd::VStringFunctions::get_char_len(left.data, left.size));
            return Status::OK();
        }

        get_utf8_char_offsets(left, left_offsets);
        get_utf8_char_offsets(right, right_offsets);
        return Distance::utf8(left, left_offsets, right, right_offsets, result);
    }

    static Status distance_with_const_offsets(const StringRef& value, Utf8Offsets& value_offsets,
                                              const StringRef& constant,
                                              const Utf8Offsets& constant_offsets,
                                              bool constant_ascii, Int32& result) {
        const bool value_ascii = simd::VStringFunctions::is_ascii(value);
        if (value_ascii && constant_ascii) {
            return Distance::ascii(value, constant, result);
        }

        if (value.size == 0) {
            result = static_cast<Int32>(constant_offsets.size());
            return Status::OK();
        }
        if (constant.size == 0) {
            result = value_ascii ? static_cast<Int32>(value.size)
                                 : static_cast<Int32>(simd::VStringFunctions::get_char_len(
                                           value.data, value.size));
            return Status::OK();
        }

        get_utf8_char_offsets(value, value_offsets);
        return Distance::utf8(value, value_offsets, constant, constant_offsets, result);
    }
};

template <typename LeftDataType, typename RightDataType>
struct LevenshteinImpl : public StringDistanceImplBase<LevenshteinDistance> {};

template <typename LeftDataType, typename RightDataType>
struct DamerauLevenshteinDistanceImpl : public StringDistanceImplBase<DamerauLevenshteinDistance> {
};

using FunctionLevenshtein =
        FunctionBinaryToType<DataTypeString, DataTypeString, LevenshteinImpl, NameLevenshtein>;
using FunctionDamerauLevenshteinDistance =
        FunctionBinaryToType<DataTypeString, DataTypeString, DamerauLevenshteinDistanceImpl,
                             NameDamerauLevenshteinDistance>;

void register_function_levenshtein(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionLevenshtein>();
    factory.register_alias(FunctionLevenshtein::name, "levenshtein_distance");
    factory.register_alias(FunctionLevenshtein::name, "edit_distance");
    factory.register_function<FunctionDamerauLevenshteinDistance>();
}

} // namespace doris
