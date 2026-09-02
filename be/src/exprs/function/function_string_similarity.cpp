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
#include <bitset>
#include <string_view>
#include <unordered_set>
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

struct NameJaro {
    static constexpr auto name = "jaro";
};

struct NameJaroWinkler {
    static constexpr auto name = "jaro_winkler";
};

struct NameJaccardSimilarity {
    static constexpr auto name = "jaccard_similarity";
};

// Guards the O(m * n) Jaro matching window and the character-set construction in
// jaccard_similarity against pathologically large STRING inputs (up to ~2GB), matching the
// VARCHAR length limit.
static constexpr size_t MAX_SIMILARITY_INPUT_LEN = 65535;

using Utf8Offsets = DorisVector<size_t>;

static StringRef string_ref_at(const ColumnString::Chars& data,
                               const ColumnString::Offsets& offsets, size_t i) {
    DCHECK_LT(i, offsets.size());
    const auto previous_offset = i == 0 ? 0 : offsets[i - 1];
    return StringRef(data.data() + previous_offset, offsets[i] - previous_offset);
}

static void get_utf8_char_offsets(const StringRef& ref, Utf8Offsets& offsets) {
    offsets.clear();
    offsets.reserve(ref.size);
    for (size_t i = 0, char_size = 0; i < ref.size; i += char_size) {
        char_size = UTF8_BYTE_LENGTH[static_cast<unsigned char>(ref.data[i])];
        offsets.push_back(i);
    }
}

// Core Jaro similarity algorithm, parameterized over an `equal(i, j)` predicate that compares
// the i-th unit of the left input to the j-th unit of the right input, where a "unit" is a byte
// (ASCII path) or a decoded UTF-8 character (UTF-8 path). Kept independent of the encoding so
// jaro_winkler can reuse it instead of duplicating the matching/transposition logic.
struct Jaro {
    template <typename Equal>
    static double similarity(size_t m, size_t n, Equal&& equal, size_t* out_prefix = nullptr,
                             size_t max_prefix = 4) {
        if (out_prefix) {
            *out_prefix = 0;
        }
        if (m == 0 && n == 0) {
            return 1.0;
        }
        if (m == 0 || n == 0) {
            return 0.0;
        }

        // Two characters can only be matched to each other if they are no farther apart than
        // this many positions; this is the standard Jaro "matching window" definition.
        const size_t max_len = std::max(m, n);
        const size_t match_distance = max_len / 2 - (max_len >= 2 ? 1 : 0);

        DorisVector<uint8_t> left_matched(m, 0);
        DorisVector<uint8_t> right_matched(n, 0);
        size_t matches = 0;
        for (size_t i = 0; i < m; ++i) {
            const size_t start = i > match_distance ? i - match_distance : 0;
            const size_t end = std::min(i + match_distance + 1, n);
            for (size_t j = start; j < end; ++j) {
                if (!right_matched[j] && equal(i, j)) {
                    left_matched[i] = 1;
                    right_matched[j] = 1;
                    ++matches;
                    break;
                }
            }
        }

        if (out_prefix) {
            const size_t limit = std::min({max_prefix, m, n});
            size_t prefix = 0;
            while (prefix < limit && equal(prefix, prefix)) {
                ++prefix;
            }
            *out_prefix = prefix;
        }

        if (matches == 0) {
            return 0.0;
        }

        // Matched characters that appear in a different relative order in the two strings each
        // count as half a transposition.
        size_t transpositions = 0;
        for (size_t i = 0, k = 0; i < m; ++i) {
            if (!left_matched[i]) {
                continue;
            }
            while (!right_matched[k]) {
                ++k;
            }
            if (!equal(i, k)) {
                ++transpositions;
            }
            ++k;
        }

        const double match_count = static_cast<double>(matches);
        const double transposition_pairs = static_cast<double>(transpositions) / 2.0;
        return (match_count / static_cast<double>(m) + match_count / static_cast<double>(n) +
               (match_count - transposition_pairs) / match_count) /
              3.0;
    }
};

static bool ascii_equal_at(const StringRef& left, const StringRef& right, size_t i, size_t j) {
    return left.data[i] == right.data[j];
}

static bool utf8_equal_at(const StringRef& left, const Utf8Offsets& left_offsets,
                          const StringRef& right, const Utf8Offsets& right_offsets, size_t i,
                          size_t j) {
    const size_t left_next = i + 1 < left_offsets.size() ? left_offsets[i + 1] : left.size;
    const size_t right_next = j + 1 < right_offsets.size() ? right_offsets[j + 1] : right.size;
    return simd::VStringFunctions::utf8_char_equal(left, left_offsets[i], left_next, right,
                                                    right_offsets[j], right_next);
}

struct JaroDistance {
    static constexpr auto name = NameJaro::name;

    static Status ascii(const StringRef& left, const StringRef& right, double& result) {
        result = Jaro::similarity(left.size, right.size,
                                  [&](size_t i, size_t j) { return ascii_equal_at(left, right, i, j); });
        return Status::OK();
    }

    static Status utf8(const StringRef& left, const Utf8Offsets& left_offsets,
                       const StringRef& right, const Utf8Offsets& right_offsets, double& result) {
        result = Jaro::similarity(left_offsets.size(), right_offsets.size(), [&](size_t i, size_t j) {
            return utf8_equal_at(left, left_offsets, right, right_offsets, i, j);
        });
        return Status::OK();
    }
};

struct JaroWinklerDistance {
    static constexpr auto name = NameJaroWinkler::name;

    // Jaro-Winkler boosts the Jaro similarity when the strings share a common leading prefix
    // (up to 4 characters), which improves matching for typos near the end of names/identifiers.
    static constexpr double PREFIX_WEIGHT = 0.1;
    static constexpr size_t MAX_PREFIX = 4;

    static Status ascii(const StringRef& left, const StringRef& right, double& result) {
        size_t prefix = 0;
        const double jaro = Jaro::similarity(
                left.size, right.size, [&](size_t i, size_t j) { return ascii_equal_at(left, right, i, j); },
                &prefix, MAX_PREFIX);
        result = jaro + static_cast<double>(prefix) * PREFIX_WEIGHT * (1.0 - jaro);
        return Status::OK();
    }

    static Status utf8(const StringRef& left, const Utf8Offsets& left_offsets,
                       const StringRef& right, const Utf8Offsets& right_offsets, double& result) {
        size_t prefix = 0;
        const double jaro = Jaro::similarity(
                left_offsets.size(), right_offsets.size(),
                [&](size_t i, size_t j) {
                    return utf8_equal_at(left, left_offsets, right, right_offsets, i, j);
                },
                &prefix, MAX_PREFIX);
        result = jaro + static_cast<double>(prefix) * PREFIX_WEIGHT * (1.0 - jaro);
        return Status::OK();
    }
};

// Jaccard similarity over the *sets* of distinct bytes (ASCII path) or distinct Unicode
// characters (UTF-8 path) making up each string, i.e. |A ∩ B| / |A ∪ B|. This mirrors
// ClickHouse's stringJaccardIndex (see FunctionsStringDistance.cpp), which likewise builds a
// character set per input rather than n-grams.
struct JaccardSimilarityDistance {
    static constexpr auto name = NameJaccardSimilarity::name;

    static Status ascii(const StringRef& left, const StringRef& right, double& result) {
        if (left.size == 0 && right.size == 0) {
            result = 1.0;
            return Status::OK();
        }
        std::bitset<256> left_set;
        std::bitset<256> right_set;
        for (size_t i = 0; i < left.size; ++i) {
            left_set.set(static_cast<uint8_t>(left.data[i]));
        }
        for (size_t i = 0; i < right.size; ++i) {
            right_set.set(static_cast<uint8_t>(right.data[i]));
        }
        const size_t intersection = (left_set & right_set).count();
        const size_t union_size = (left_set | right_set).count();
        result = static_cast<double>(intersection) / static_cast<double>(union_size);
        return Status::OK();
    }

    static Status utf8(const StringRef& left, const Utf8Offsets& left_offsets,
                       const StringRef& right, const Utf8Offsets& right_offsets, double& result) {
        if (left_offsets.empty() && right_offsets.empty()) {
            result = 1.0;
            return Status::OK();
        }
        std::unordered_set<std::string_view> left_set;
        std::unordered_set<std::string_view> right_set;
        fill_char_set(left, left_offsets, left_set);
        fill_char_set(right, right_offsets, right_set);

        const auto* smaller = &left_set;
        const auto* larger = &right_set;
        if (larger->size() < smaller->size()) {
            std::swap(smaller, larger);
        }
        size_t intersection = 0;
        for (const auto& ch : *smaller) {
            intersection += larger->count(ch);
        }
        const size_t union_size = left_set.size() + right_set.size() - intersection;
        result = static_cast<double>(intersection) / static_cast<double>(union_size);
        return Status::OK();
    }

private:
    static void fill_char_set(const StringRef& str, const Utf8Offsets& offsets,
                              std::unordered_set<std::string_view>& out) {
        out.reserve(offsets.size());
        for (size_t i = 0; i < offsets.size(); ++i) {
            const size_t off = offsets[i];
            const size_t next = i + 1 < offsets.size() ? offsets[i + 1] : str.size;
            out.emplace(str.data + off, next - off);
        }
    }
};

template <typename Distance>
struct SimilarityImplBase {
    using ResultDataType = DataTypeFloat64;
    using ResultPaddedPODArray = PaddedPODArray<Float64>;

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
            RETURN_IF_ERROR(similarity(string_ref_at(ldata, loffsets, i),
                                       string_ref_at(rdata, roffsets, i), left_offsets,
                                       right_offsets, res[i]));
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
        RETURN_IF_ERROR(check_length(constant));
        const size_t size = offsets.size();
        res.resize(size);
        const bool constant_ascii = simd::VStringFunctions::is_ascii(constant);
        Utf8Offsets constant_offsets;
        get_utf8_char_offsets(constant, constant_offsets);
        Utf8Offsets value_offsets;
        for (size_t i = 0; i < size; ++i) {
            RETURN_IF_ERROR(similarity_with_const_offsets(string_ref_at(data, offsets, i),
                                                           value_offsets, constant, constant_offsets,
                                                           constant_ascii, res[i]));
        }
        return Status::OK();
    }

    static Status check_length(const StringRef& value) {
        if (value.size > MAX_SIMILARITY_INPUT_LEN) {
            return Status::InvalidArgument(
                    "Input string too long for {}, max {} bytes", Distance::name,
                    MAX_SIMILARITY_INPUT_LEN);
        }
        return Status::OK();
    }

    static Status similarity(const StringRef& left, const StringRef& right,
                             Utf8Offsets& left_offsets, Utf8Offsets& right_offsets, double& result) {
        RETURN_IF_ERROR(check_length(left));
        RETURN_IF_ERROR(check_length(right));
        const bool left_ascii = simd::VStringFunctions::is_ascii(left);
        const bool right_ascii = simd::VStringFunctions::is_ascii(right);
        if (left_ascii && right_ascii) {
            return Distance::ascii(left, right, result);
        }
        get_utf8_char_offsets(left, left_offsets);
        get_utf8_char_offsets(right, right_offsets);
        return Distance::utf8(left, left_offsets, right, right_offsets, result);
    }

    static Status similarity_with_const_offsets(const StringRef& value, Utf8Offsets& value_offsets,
                                                const StringRef& constant,
                                                const Utf8Offsets& constant_offsets,
                                                bool constant_ascii, double& result) {
        RETURN_IF_ERROR(check_length(value));
        const bool value_ascii = simd::VStringFunctions::is_ascii(value);
        if (value_ascii && constant_ascii) {
            return Distance::ascii(value, constant, result);
        }
        get_utf8_char_offsets(value, value_offsets);
        return Distance::utf8(value, value_offsets, constant, constant_offsets, result);
    }
};

template <typename LeftDataType, typename RightDataType>
struct JaroImpl : public SimilarityImplBase<JaroDistance> {};

template <typename LeftDataType, typename RightDataType>
struct JaroWinklerImpl : public SimilarityImplBase<JaroWinklerDistance> {};

template <typename LeftDataType, typename RightDataType>
struct JaccardSimilarityImpl : public SimilarityImplBase<JaccardSimilarityDistance> {};

using FunctionJaro = FunctionBinaryToType<DataTypeString, DataTypeString, JaroImpl, NameJaro>;
using FunctionJaroWinkler =
        FunctionBinaryToType<DataTypeString, DataTypeString, JaroWinklerImpl, NameJaroWinkler>;
using FunctionJaccardSimilarity = FunctionBinaryToType<DataTypeString, DataTypeString,
                                                        JaccardSimilarityImpl, NameJaccardSimilarity>;

void register_function_string_similarity(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionJaro>();
    factory.register_function<FunctionJaroWinkler>();
    factory.register_function<FunctionJaccardSimilarity>();
}

} // namespace doris
