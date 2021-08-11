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

/*
 * This implementation of RadixSort is copied from ClickHouse.
 * We only reserve some functions which is useful to us and solve some c++11 incompatibility problem.
 * We can use this implementation to sort float, double, int, uint and other complex object.
 * See original code: https://github.com/ClickHouse/ClickHouse/blob/master/dbms/src/Common/RadixSort.h
 *
 */

#ifndef RADIXSORT_H_
#define RADIXSORT_H_

#include <malloc.h>
#include <string.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <type_traits>

#include "common/compiler_util.h"

namespace doris {

template <typename T>
using decay_t = typename std::decay<T>::type;

template <bool cond, typename T, typename F>
using conditional_t = typename std::conditional<cond, T, F>::type;

template <typename T>
using make_unsigned_t = typename std::make_unsigned<T>::type;

template <typename T>
using is_integral_v = typename std::is_integral<T>::value;

template <typename T>
using is_unsigned_v = typename std::is_unsigned<T>::value;

template <typename To, typename From>
decay_t<To> bit_cast(const From& from) {
    To res{};
    memcpy(static_cast<void*>(&res), &from, std::min(sizeof(res), sizeof(from)));
    return res;
}

/** Radix sort, has the following functionality:
  * Can sort unsigned, signed numbers, and floats.
  * Can sort an array of fixed length elements that contain something else besides the key.
  * Customizable radix size.
  *
  * LSB, stable.
  * NOTE For some applications it makes sense to add MSB-radix-sort,
  *  as well as radix-select, radix-partial-sort, radix-get-permutation algorithms based on it.
  */

/** Used as a template parameter. See below.
  */
struct RadixSortMallocAllocator {
    void* allocate(size_t size) { return malloc(size); }

    void deallocate(void* ptr, size_t /*size*/) { return free(ptr); }
};

/** A transformation that transforms the bit representation of a key into an unsigned integer number,
  *  that the order relation over the keys will match the order relation over the obtained unsigned numbers.
  * For floats this conversion does the following:
  *  if the signed bit is set, it flips all other bits.
  * In this case, NaN-s are bigger than all normal numbers.
  */
template <typename KeyBits>
struct RadixSortFloatTransform {
    /// Is it worth writing the result in memory, or is it better to do calculation every time again?
    static constexpr bool transform_is_simple = false;

    static KeyBits forward(KeyBits x) {
        return x ^
               ((-(x >> (sizeof(KeyBits) * 8 - 1))) | (KeyBits(1) << (sizeof(KeyBits) * 8 - 1)));
    }

    static KeyBits backward(KeyBits x) {
        return x ^
               (((x >> (sizeof(KeyBits) * 8 - 1)) - 1) | (KeyBits(1) << (sizeof(KeyBits) * 8 - 1)));
    }
};

template <typename TElement>
struct RadixSortFloatTraits {
    using Element =
            TElement; /// The type of the element. It can be a structure with a key and some other payload. Or just a key.
    using Key = Element; /// The key to sort by.

    /// Type for calculating histograms. In the case of a known small number of elements, it can be less than size_t.
    using CountType = uint32_t;

    /// The type to which the key is transformed to do bit operations. This UInt is the same size as the key.
    using KeyBits = conditional_t<sizeof(Key) == 8, uint64_t, uint32_t>;

    static constexpr size_t PART_SIZE_BITS =
            8; /// With what pieces of the key, in bits, to do one pass - reshuffle of the array.

    /// Converting a key into KeyBits is such that the order relation over the key corresponds to the order relation over KeyBits.
    using Transform = RadixSortFloatTransform<KeyBits>;

    /// An object with the functions allocate and deallocate.
    /// Can be used, for example, to allocate memory for a temporary array on the stack.
    /// To do this, the allocator itself is created on the stack.
    using Allocator = RadixSortMallocAllocator;

    /// The function to get the key from an array element.
    static Key& extractKey(Element& elem) { return elem; }

    /// Used when fallback to comparison based sorting is needed.
    /// TODO: Correct handling of NaNs, NULLs, etc
    static bool less(Key x, Key y) { return x < y; }
};

template <typename KeyBits>
struct RadixSortIdentityTransform {
    static constexpr bool transform_is_simple = true;

    static KeyBits forward(KeyBits x) { return x; }
    static KeyBits backward(KeyBits x) { return x; }
};

template <typename TElement>
struct RadixSortUIntTraits {
    using Element = TElement;
    using Key = Element;
    using CountType = uint32_t;
    using KeyBits = Key;

    static constexpr size_t PART_SIZE_BITS = 8;

    using Transform = RadixSortIdentityTransform<KeyBits>;
    using Allocator = RadixSortMallocAllocator;

    static Key& extractKey(Element& elem) { return elem; }

    static bool less(Key x, Key y) { return x < y; }
};

template <typename KeyBits>
struct RadixSortSignedTransform {
    static constexpr bool transform_is_simple = true;

    static KeyBits forward(KeyBits x) { return x ^ (KeyBits(1) << (sizeof(KeyBits) * 8 - 1)); }
    static KeyBits backward(KeyBits x) { return x ^ (KeyBits(1) << (sizeof(KeyBits) * 8 - 1)); }
};

template <typename TElement>
struct RadixSortIntTraits {
    using Element = TElement;
    using Key = Element;
    using CountType = uint32_t;
    using KeyBits = make_unsigned_t<Key>;

    static constexpr size_t PART_SIZE_BITS = 8;

    using Transform = RadixSortSignedTransform<KeyBits>;
    using Allocator = RadixSortMallocAllocator;

    static Key& extractKey(Element& elem) { return elem; }

    static bool less(Key x, Key y) { return x < y; }
};

template <typename T>
using RadixSortNumTraits = conditional_t<
        std::is_integral<T>::value,
        conditional_t<std::is_unsigned<T>::value, RadixSortUIntTraits<T>, RadixSortIntTraits<T>>,
        RadixSortFloatTraits<T>>;

/*
 * To use RadixSort, you should define `Traits` to give out the information for sorting.
 * `RadixSortFloatTraits` is a good example to refer to.
 * Then you can run it as follows:
 *           RadixSort<YourTraits>::execute_lsd(arr, size);
 *
 * In particular, if you want to sort an array of numeric, you can use it easily as follows:
 *           radixSortLSD(array_of_numeric, array_size);
 *
 * See more use cases: be/test/util/radix_sort_test.cpp
 *
 */
template <typename Traits>
struct RadixSort {
private:
    using Element = typename Traits::Element;
    using Key = typename Traits::Key;
    using CountType = typename Traits::CountType;
    using KeyBits = typename Traits::KeyBits;

    // Use insertion sort if the size of the array is less than equal to this threshold
    static constexpr size_t INSERTION_SORT_THRESHOLD = 64;

    static constexpr size_t HISTOGRAM_SIZE = 1 << Traits::PART_SIZE_BITS;
    static constexpr size_t PART_BITMASK = HISTOGRAM_SIZE - 1;
    static constexpr size_t KEY_BITS = sizeof(Key) * 8;
    static constexpr size_t NUM_PASSES =
            (KEY_BITS + (Traits::PART_SIZE_BITS - 1)) / Traits::PART_SIZE_BITS;

    static ALWAYS_INLINE KeyBits getPart(size_t N, KeyBits x) {
        if (Traits::Transform::transform_is_simple) x = Traits::Transform::forward(x);

        return (x >> (N * Traits::PART_SIZE_BITS)) & PART_BITMASK;
    }

    static KeyBits keyToBits(Key x) { return bit_cast<KeyBits>(x); }
    static Key bitsToKey(KeyBits x) { return bit_cast<Key>(x); }

public:
    /// Least significant digit radix sort (stable)
    static void executeLSD(Element* arr, size_t size) {
        /// If the array is smaller than 256, then it is better to use another algorithm.

        /// There are loops of NUM_PASSES. It is very important that they are unfolded at compile-time.

        /// For each of the NUM_PASSES bit ranges of the key, consider how many times each value of this bit range met.
        CountType histograms[HISTOGRAM_SIZE * NUM_PASSES] = {0};

        typename Traits::Allocator allocator;

        /// We will do several passes through the array. On each pass, the data is transferred to another array. Let's allocate this temporary array.
        Element* swap_buffer =
                reinterpret_cast<Element*>(allocator.allocate(size * sizeof(Element)));

        /// Transform the array and calculate the histogram.
        /// NOTE This is slightly suboptimal. Look at https://github.com/powturbo/TurboHist
        for (size_t i = 0; i < size; ++i) {
            if (!Traits::Transform::transform_is_simple)
                Traits::extractKey(arr[i]) = bitsToKey(
                        Traits::Transform::forward(keyToBits(Traits::extractKey(arr[i]))));

            for (size_t pass = 0; pass < NUM_PASSES; ++pass)
                ++histograms[pass * HISTOGRAM_SIZE +
                             getPart(pass, keyToBits(Traits::extractKey(arr[i])))];
        }

        {
            /// Replace the histograms with the accumulated sums: the value in position i is the sum of the previous positions minus one.
            size_t sums[NUM_PASSES] = {0};

            for (size_t i = 0; i < HISTOGRAM_SIZE; ++i) {
                for (size_t pass = 0; pass < NUM_PASSES; ++pass) {
                    size_t tmp = histograms[pass * HISTOGRAM_SIZE + i] + sums[pass];
                    histograms[pass * HISTOGRAM_SIZE + i] = sums[pass] - 1;
                    sums[pass] = tmp;
                }
            }
        }

        /// Move the elements in the order starting from the least bit piece, and then do a few passes on the number of pieces.
        for (size_t pass = 0; pass < NUM_PASSES; ++pass) {
            Element* writer = pass % 2 ? arr : swap_buffer;
            Element* reader = pass % 2 ? swap_buffer : arr;

            for (size_t i = 0; i < size; ++i) {
                size_t pos = getPart(pass, keyToBits(Traits::extractKey(reader[i])));

                /// Place the element on the next free position.
                auto& dest = writer[++histograms[pass * HISTOGRAM_SIZE + pos]];
                dest = reader[i];

                /// On the last pass, we do the reverse transformation.
                if (!Traits::Transform::transform_is_simple && pass == NUM_PASSES - 1)
                    Traits::extractKey(dest) = bitsToKey(
                            Traits::Transform::backward(keyToBits(Traits::extractKey(reader[i]))));
            }
        }

        /// If the number of passes is odd, the result array is in a temporary buffer. Copy it to the place of the original array.
        /// NOTE Sometimes it will be more optimal to provide non-destructive interface, that will not modify original array.
        if (NUM_PASSES % 2) memcpy(arr, swap_buffer, size * sizeof(Element));

        allocator.deallocate(swap_buffer, size * sizeof(Element));
    }
};

/// Helper functions for numeric types.
/// Use RadixSort with custom traits for complex types instead.

template <typename T>
void radixSortLSD(T* arr, size_t size) {
    RadixSort<RadixSortNumTraits<T>>::executeLSD(arr, size);
}

} // namespace doris

#endif // RADIXSORT_H_
