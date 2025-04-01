/**
 * MIT License
 *
 * Copyright (c) 2017 Thibaut Goetghebuer-Planchon <tessil@gmx.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
#ifndef TSL_SPARSE_HASH_H
#define TSL_SPARSE_HASH_H

#include <algorithm>
#include <cassert>
#include <climits>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <limits>
#include <memory>
#include <stdexcept>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "sparse_growth_policy.h"

#ifdef __INTEL_COMPILER
#include <immintrin.h>  // For _popcnt32 and _popcnt64
#endif

#ifdef _MSC_VER
#include <intrin.h>  // For __cpuid, __popcnt and __popcnt64
#endif

#ifdef TSL_DEBUG
#define tsl_sh_assert(expr) assert(expr)
#else
#define tsl_sh_assert(expr) (static_cast<void>(0))
#endif

namespace tsl {

namespace sh {
enum class probing { linear, quadratic };

enum class exception_safety { basic, strong };

enum class sparsity { high, medium, low };
}  // namespace sh

namespace detail_popcount {
/**
 * Define the popcount(ll) methods and pick-up the best depending on the
 * compiler.
 */

// From Wikipedia: https://en.wikipedia.org/wiki/Hamming_weight
inline int fallback_popcountll(unsigned long long int x) {
  static_assert(
      sizeof(unsigned long long int) == sizeof(std::uint64_t),
      "sizeof(unsigned long long int) must be equal to sizeof(std::uint64_t). "
      "Open a feature request if you need support for a platform where it "
      "isn't the case.");

  const std::uint64_t m1 = 0x5555555555555555ull;
  const std::uint64_t m2 = 0x3333333333333333ull;
  const std::uint64_t m4 = 0x0f0f0f0f0f0f0f0full;
  const std::uint64_t h01 = 0x0101010101010101ull;

  x -= (x >> 1ull) & m1;
  x = (x & m2) + ((x >> 2ull) & m2);
  x = (x + (x >> 4ull)) & m4;
  return static_cast<int>((x * h01) >> (64ull - 8ull));
}

inline int fallback_popcount(unsigned int x) {
  static_assert(sizeof(unsigned int) == sizeof(std::uint32_t) ||
                    sizeof(unsigned int) == sizeof(std::uint64_t),
                "sizeof(unsigned int) must be equal to sizeof(std::uint32_t) "
                "or sizeof(std::uint64_t). "
                "Open a feature request if you need support for a platform "
                "where it isn't the case.");

  if (sizeof(unsigned int) == sizeof(std::uint32_t)) {
    const std::uint32_t m1 = 0x55555555;
    const std::uint32_t m2 = 0x33333333;
    const std::uint32_t m4 = 0x0f0f0f0f;
    const std::uint32_t h01 = 0x01010101;

    x -= (x >> 1) & m1;
    x = (x & m2) + ((x >> 2) & m2);
    x = (x + (x >> 4)) & m4;
    return static_cast<int>((x * h01) >> (32 - 8));
  } else {
    return fallback_popcountll(x);
  }
}

#if defined(__clang__) || defined(__GNUC__)
inline int popcountll(unsigned long long int value) {
  return __builtin_popcountll(value);
}

inline int popcount(unsigned int value) { return __builtin_popcount(value); }

#elif defined(_MSC_VER)
/**
 * We need to check for popcount support at runtime on Windows with __cpuid
 * See https://msdn.microsoft.com/en-us/library/bb385231.aspx
 */
inline bool has_popcount_support() {
  int cpu_infos[4];
  __cpuid(cpu_infos, 1);
  return (cpu_infos[2] & (1 << 23)) != 0;
}

inline int popcountll(unsigned long long int value) {
#ifdef _WIN64
  static_assert(
      sizeof(unsigned long long int) == sizeof(std::int64_t),
      "sizeof(unsigned long long int) must be equal to sizeof(std::int64_t). ");

  static const bool has_popcount = has_popcount_support();
  return has_popcount
             ? static_cast<int>(__popcnt64(static_cast<std::int64_t>(value)))
             : fallback_popcountll(value);
#else
  return fallback_popcountll(value);
#endif
}

inline int popcount(unsigned int value) {
  static_assert(sizeof(unsigned int) == sizeof(std::int32_t),
                "sizeof(unsigned int) must be equal to sizeof(std::int32_t). ");

  static const bool has_popcount = has_popcount_support();
  return has_popcount
             ? static_cast<int>(__popcnt(static_cast<std::int32_t>(value)))
             : fallback_popcount(value);
}

#elif defined(__INTEL_COMPILER)
inline int popcountll(unsigned long long int value) {
  static_assert(sizeof(unsigned long long int) == sizeof(__int64), "");
  return _popcnt64(static_cast<__int64>(value));
}

inline int popcount(unsigned int value) {
  return _popcnt32(static_cast<int>(value));
}

#else
inline int popcountll(unsigned long long int x) {
  return fallback_popcountll(x);
}

inline int popcount(unsigned int x) { return fallback_popcount(x); }

#endif
}  // namespace detail_popcount

namespace detail_sparse_hash {

template <typename T>
struct make_void {
  using type = void;
};

template <typename T, typename = void>
struct has_is_transparent : std::false_type {};

template <typename T>
struct has_is_transparent<T,
                          typename make_void<typename T::is_transparent>::type>
    : std::true_type {};

template <typename U>
struct is_power_of_two_policy : std::false_type {};

template <std::size_t GrowthFactor>
struct is_power_of_two_policy<tsl::sh::power_of_two_growth_policy<GrowthFactor>>
    : std::true_type {};

inline constexpr bool is_power_of_two(std::size_t value) {
  return value != 0 && (value & (value - 1)) == 0;
}

inline std::size_t round_up_to_power_of_two(std::size_t value) {
  if (is_power_of_two(value)) {
    return value;
  }

  if (value == 0) {
    return 1;
  }

  --value;
  for (std::size_t i = 1; i < sizeof(std::size_t) * CHAR_BIT; i *= 2) {
    value |= value >> i;
  }

  return value + 1;
}

template <typename T, typename U>
static T numeric_cast(U value,
                      const char *error_message = "numeric_cast() failed.") {
  T ret = static_cast<T>(value);
  if (static_cast<U>(ret) != value) {
    throw std::runtime_error(error_message);
  }

  const bool is_same_signedness =
      (std::is_unsigned<T>::value && std::is_unsigned<U>::value) ||
      (std::is_signed<T>::value && std::is_signed<U>::value);
  if (!is_same_signedness && (ret < T{}) != (value < U{})) {
    throw std::runtime_error(error_message);
  }

  return ret;
}

/**
 * Fixed size type used to represent size_type values on serialization. Need to
 * be big enough to represent a std::size_t on 32 and 64 bits platforms, and
 * must be the same size on both platforms.
 */
using slz_size_type = std::uint64_t;
static_assert(std::numeric_limits<slz_size_type>::max() >=
                  std::numeric_limits<std::size_t>::max(),
              "slz_size_type must be >= std::size_t");

template <class T, class Deserializer>
static T deserialize_value(Deserializer &deserializer) {
  // MSVC < 2017 is not conformant, circumvent the problem by removing the
  // template keyword
#if defined(_MSC_VER) && _MSC_VER < 1910
  return deserializer.Deserializer::operator()<T>();
#else
  return deserializer.Deserializer::template operator()<T>();
#endif
}

/**
 * WARNING: the sparse_array class doesn't free the ressources allocated through
 * the allocator passed in parameter in each method. You have to manually call
 * `clear(Allocator&)` when you don't need a sparse_array object anymore.
 *
 * The reason is that the sparse_array doesn't store the allocator to avoid
 * wasting space in each sparse_array when the allocator has a size > 0. It only
 * allocates/deallocates objects with the allocator that is passed in parameter.
 *
 *
 *
 * Index denotes a value between [0, BITMAP_NB_BITS), it is an index similar to
 * std::vector. Offset denotes the real position in `m_values` corresponding to
 * an index.
 *
 * We are using raw pointers instead of std::vector to avoid loosing
 * 2*sizeof(size_t) bytes to store the capacity and size of the vector in each
 * sparse_array. We know we can only store up to BITMAP_NB_BITS elements in the
 * array, we don't need such big types.
 *
 *
 * T must be nothrow move constructible and/or copy constructible.
 * Behaviour is undefined if the destructor of T throws an exception.
 *
 * See https://smerity.com/articles/2015/google_sparsehash.html for details on
 * the idea behinds the implementation.
 *
 * TODO Check to use std::realloc and std::memmove when possible
 */
template <typename T, typename Allocator, tsl::sh::sparsity Sparsity>
class sparse_array {
 public:
  using value_type = T;
  using size_type = std::uint_least8_t;
  using allocator_type = Allocator;
  using iterator = value_type *;
  using const_iterator = const value_type *;

 private:
  static const size_type CAPACITY_GROWTH_STEP =
      (Sparsity == tsl::sh::sparsity::high) ? 2
      : (Sparsity == tsl::sh::sparsity::medium)
          ? 4
          : 8;  // (Sparsity == tsl::sh::sparsity::low)

  /**
   * Bitmap size configuration.
   * Use 32 bits for the bitmap on 32-bits or less environnement as popcount on
   * 64 bits numbers is slow on these environnement. Use 64 bits bitmap
   * otherwise.
   */
#if SIZE_MAX <= UINT32_MAX
  using bitmap_type = std::uint_least32_t;
  static const std::size_t BITMAP_NB_BITS = 32;
  static const std::size_t BUCKET_SHIFT = 5;
#else
  using bitmap_type = std::uint_least64_t;
  static const std::size_t BITMAP_NB_BITS = 64;
  static const std::size_t BUCKET_SHIFT = 6;
#endif

  static const std::size_t BUCKET_MASK = BITMAP_NB_BITS - 1;

  static_assert(is_power_of_two(BITMAP_NB_BITS),
                "BITMAP_NB_BITS must be a power of two.");
  static_assert(std::numeric_limits<bitmap_type>::digits >= BITMAP_NB_BITS,
                "bitmap_type must be able to hold at least BITMAP_NB_BITS.");
  static_assert((std::size_t(1) << BUCKET_SHIFT) == BITMAP_NB_BITS,
                "(1 << BUCKET_SHIFT) must be equal to BITMAP_NB_BITS.");
  static_assert(std::numeric_limits<size_type>::max() >= BITMAP_NB_BITS,
                "size_type must be big enough to hold BITMAP_NB_BITS.");
  static_assert(std::is_unsigned<bitmap_type>::value,
                "bitmap_type must be unsigned.");
  static_assert((std::numeric_limits<bitmap_type>::max() & BUCKET_MASK) ==
                    BITMAP_NB_BITS - 1,
                "");

 public:
  /**
   * Map an ibucket [0, bucket_count) in the hash table to a sparse_ibucket
   * (a sparse_array holds multiple buckets, so there is less sparse_array than
   * bucket_count).
   *
   * The bucket ibucket is in
   * m_sparse_buckets[sparse_ibucket(ibucket)][index_in_sparse_bucket(ibucket)]
   * instead of something like m_buckets[ibucket] in a classical hash table.
   */
  static std::size_t sparse_ibucket(std::size_t ibucket) {
    return ibucket >> BUCKET_SHIFT;
  }

  /**
   * Map an ibucket [0, bucket_count) in the hash table to an index in the
   * sparse_array which corresponds to the bucket.
   *
   * The bucket ibucket is in
   * m_sparse_buckets[sparse_ibucket(ibucket)][index_in_sparse_bucket(ibucket)]
   * instead of something like m_buckets[ibucket] in a classical hash table.
   */
  static typename sparse_array::size_type index_in_sparse_bucket(
      std::size_t ibucket) {
    return static_cast<typename sparse_array::size_type>(
        ibucket & sparse_array::BUCKET_MASK);
  }

  static std::size_t nb_sparse_buckets(std::size_t bucket_count) noexcept {
    if (bucket_count == 0) {
      return 0;
    }

    return std::max<std::size_t>(
        1, sparse_ibucket(tsl::detail_sparse_hash::round_up_to_power_of_two(
               bucket_count)));
  }

 public:
  sparse_array() noexcept
      : m_values(nullptr),
        m_bitmap_vals(0),
        m_bitmap_deleted_vals(0),
        m_nb_elements(0),
        m_capacity(0),
        m_last_array(false) {}

  explicit sparse_array(bool last_bucket) noexcept
      : m_values(nullptr),
        m_bitmap_vals(0),
        m_bitmap_deleted_vals(0),
        m_nb_elements(0),
        m_capacity(0),
        m_last_array(last_bucket) {}

  sparse_array(size_type capacity, Allocator &alloc)
      : m_values(nullptr),
        m_bitmap_vals(0),
        m_bitmap_deleted_vals(0),
        m_nb_elements(0),
        m_capacity(capacity),
        m_last_array(false) {
    if (m_capacity > 0) {
      m_values = alloc.allocate(m_capacity);
      tsl_sh_assert(m_values !=
                    nullptr);  // allocate should throw if there is a failure
    }
  }

  sparse_array(const sparse_array &other, Allocator &alloc)
      : m_values(nullptr),
        m_bitmap_vals(other.m_bitmap_vals),
        m_bitmap_deleted_vals(other.m_bitmap_deleted_vals),
        m_nb_elements(0),
        m_capacity(other.m_capacity),
        m_last_array(other.m_last_array) {
    tsl_sh_assert(other.m_capacity >= other.m_nb_elements);
    if (m_capacity == 0) {
      return;
    }

    m_values = alloc.allocate(m_capacity);
    tsl_sh_assert(m_values !=
                  nullptr);  // allocate should throw if there is a failure
    try {
      for (size_type i = 0; i < other.m_nb_elements; i++) {
        construct_value(alloc, m_values + i, other.m_values[i]);
        m_nb_elements++;
      }
    } catch (...) {
      clear(alloc);
      throw;
    }
  }

  sparse_array(sparse_array &&other) noexcept
      : m_values(other.m_values),
        m_bitmap_vals(other.m_bitmap_vals),
        m_bitmap_deleted_vals(other.m_bitmap_deleted_vals),
        m_nb_elements(other.m_nb_elements),
        m_capacity(other.m_capacity),
        m_last_array(other.m_last_array) {
    other.m_values = nullptr;
    other.m_bitmap_vals = 0;
    other.m_bitmap_deleted_vals = 0;
    other.m_nb_elements = 0;
    other.m_capacity = 0;
  }

  sparse_array(sparse_array &&other, Allocator &alloc)
      : m_values(nullptr),
        m_bitmap_vals(other.m_bitmap_vals),
        m_bitmap_deleted_vals(other.m_bitmap_deleted_vals),
        m_nb_elements(0),
        m_capacity(other.m_capacity),
        m_last_array(other.m_last_array) {
    tsl_sh_assert(other.m_capacity >= other.m_nb_elements);
    if (m_capacity == 0) {
      return;
    }

    m_values = alloc.allocate(m_capacity);
    tsl_sh_assert(m_values !=
                  nullptr);  // allocate should throw if there is a failure
    try {
      for (size_type i = 0; i < other.m_nb_elements; i++) {
        construct_value(alloc, m_values + i, std::move(other.m_values[i]));
        m_nb_elements++;
      }
    } catch (...) {
      clear(alloc);
      throw;
    }
  }

  sparse_array &operator=(const sparse_array &) = delete;
  sparse_array &operator=(sparse_array &&) = delete;

  ~sparse_array() noexcept {
    // The code that manages the sparse_array must have called clear before
    // destruction. See documentation of sparse_array for more details.
    tsl_sh_assert(m_capacity == 0 && m_nb_elements == 0 && m_values == nullptr);
  }

  iterator begin() noexcept { return m_values; }
  iterator end() noexcept { return m_values + m_nb_elements; }
  const_iterator begin() const noexcept { return cbegin(); }
  const_iterator end() const noexcept { return cend(); }
  const_iterator cbegin() const noexcept { return m_values; }
  const_iterator cend() const noexcept { return m_values + m_nb_elements; }

  bool empty() const noexcept { return m_nb_elements == 0; }

  size_type size() const noexcept { return m_nb_elements; }

  void clear(allocator_type &alloc) noexcept {
    destroy_and_deallocate_values(alloc, m_values, m_nb_elements, m_capacity);

    m_values = nullptr;
    m_bitmap_vals = 0;
    m_bitmap_deleted_vals = 0;
    m_nb_elements = 0;
    m_capacity = 0;
  }

  bool last() const noexcept { return m_last_array; }

  void set_as_last() noexcept { m_last_array = true; }

  bool has_value(size_type index) const noexcept {
    tsl_sh_assert(index < BITMAP_NB_BITS);
    return (m_bitmap_vals & (bitmap_type(1) << index)) != 0;
  }

  bool has_deleted_value(size_type index) const noexcept {
    tsl_sh_assert(index < BITMAP_NB_BITS);
    return (m_bitmap_deleted_vals & (bitmap_type(1) << index)) != 0;
  }

  iterator value(size_type index) noexcept {
    tsl_sh_assert(has_value(index));
    return m_values + index_to_offset(index);
  }

  const_iterator value(size_type index) const noexcept {
    tsl_sh_assert(has_value(index));
    return m_values + index_to_offset(index);
  }

  /**
   * Return iterator to set value.
   */
  template <typename... Args>
  iterator set(allocator_type &alloc, size_type index, Args &&...value_args) {
    tsl_sh_assert(!has_value(index));

    const size_type offset = index_to_offset(index);
    insert_at_offset(alloc, offset, std::forward<Args>(value_args)...);

    m_bitmap_vals = (m_bitmap_vals | (bitmap_type(1) << index));
    m_bitmap_deleted_vals =
        (m_bitmap_deleted_vals & ~(bitmap_type(1) << index));

    m_nb_elements++;

    tsl_sh_assert(has_value(index));
    tsl_sh_assert(!has_deleted_value(index));

    return m_values + offset;
  }

  iterator erase(allocator_type &alloc, iterator position) {
    const size_type offset =
        static_cast<size_type>(std::distance(begin(), position));
    return erase(alloc, position, offset_to_index(offset));
  }

  // Return the next value or end if no next value
  iterator erase(allocator_type &alloc, iterator position, size_type index) {
    tsl_sh_assert(has_value(index));
    tsl_sh_assert(!has_deleted_value(index));

    const size_type offset =
        static_cast<size_type>(std::distance(begin(), position));
    erase_at_offset(alloc, offset);

    m_bitmap_vals = (m_bitmap_vals & ~(bitmap_type(1) << index));
    m_bitmap_deleted_vals = (m_bitmap_deleted_vals | (bitmap_type(1) << index));

    m_nb_elements--;

    tsl_sh_assert(!has_value(index));
    tsl_sh_assert(has_deleted_value(index));

    return m_values + offset;
  }

  void swap(sparse_array &other) {
    using std::swap;

    swap(m_values, other.m_values);
    swap(m_bitmap_vals, other.m_bitmap_vals);
    swap(m_bitmap_deleted_vals, other.m_bitmap_deleted_vals);
    swap(m_nb_elements, other.m_nb_elements);
    swap(m_capacity, other.m_capacity);
    swap(m_last_array, other.m_last_array);
  }

  static iterator mutable_iterator(const_iterator pos) {
    return const_cast<iterator>(pos);
  }

  template <class Serializer>
  void serialize(Serializer &serializer) const {
    const slz_size_type sparse_bucket_size = m_nb_elements;
    serializer(sparse_bucket_size);

    const slz_size_type bitmap_vals = m_bitmap_vals;
    serializer(bitmap_vals);

    const slz_size_type bitmap_deleted_vals = m_bitmap_deleted_vals;
    serializer(bitmap_deleted_vals);

    for (const value_type &value : *this) {
      serializer(value);
    }
  }

  template <class Deserializer>
  static sparse_array deserialize_hash_compatible(Deserializer &deserializer,
                                                  Allocator &alloc) {
    const slz_size_type sparse_bucket_size =
        deserialize_value<slz_size_type>(deserializer);
    const slz_size_type bitmap_vals =
        deserialize_value<slz_size_type>(deserializer);
    const slz_size_type bitmap_deleted_vals =
        deserialize_value<slz_size_type>(deserializer);

    if (sparse_bucket_size > BITMAP_NB_BITS) {
      throw std::runtime_error(
          "Deserialized sparse_bucket_size is too big for the platform. "
          "Maximum should be BITMAP_NB_BITS.");
    }

    sparse_array sarray;
    if (sparse_bucket_size == 0) {
      return sarray;
    }

    sarray.m_bitmap_vals = numeric_cast<bitmap_type>(
        bitmap_vals, "Deserialized bitmap_vals is too big.");
    sarray.m_bitmap_deleted_vals = numeric_cast<bitmap_type>(
        bitmap_deleted_vals, "Deserialized bitmap_deleted_vals is too big.");

    sarray.m_capacity = numeric_cast<size_type>(
        sparse_bucket_size, "Deserialized sparse_bucket_size is too big.");
    sarray.m_values = alloc.allocate(sarray.m_capacity);

    try {
      for (size_type ivalue = 0; ivalue < sarray.m_capacity; ivalue++) {
        construct_value(alloc, sarray.m_values + ivalue,
                        deserialize_value<value_type>(deserializer));
        sarray.m_nb_elements++;
      }
    } catch (...) {
      sarray.clear(alloc);
      throw;
    }

    return sarray;
  }

  /**
   * Deserialize the values of the bucket and insert them all in sparse_hash
   * through sparse_hash.insert(...).
   */
  template <class Deserializer, class SparseHash>
  static void deserialize_values_into_sparse_hash(Deserializer &deserializer,
                                                  SparseHash &sparse_hash) {
    const slz_size_type sparse_bucket_size =
        deserialize_value<slz_size_type>(deserializer);

    const slz_size_type bitmap_vals =
        deserialize_value<slz_size_type>(deserializer);
    static_cast<void>(bitmap_vals);  // Ignore, not needed

    const slz_size_type bitmap_deleted_vals =
        deserialize_value<slz_size_type>(deserializer);
    static_cast<void>(bitmap_deleted_vals);  // Ignore, not needed

    for (slz_size_type ivalue = 0; ivalue < sparse_bucket_size; ivalue++) {
      sparse_hash.insert(deserialize_value<value_type>(deserializer));
    }
  }

 private:
  template <typename... Args>
  static void construct_value(allocator_type &alloc, value_type *value,
                              Args &&...value_args) {
    std::allocator_traits<allocator_type>::construct(
        alloc, value, std::forward<Args>(value_args)...);
  }

  static void destroy_value(allocator_type &alloc, value_type *value) noexcept {
    std::allocator_traits<allocator_type>::destroy(alloc, value);
  }

  static void destroy_and_deallocate_values(
      allocator_type &alloc, value_type *values, size_type nb_values,
      size_type capacity_values) noexcept {
    for (size_type i = 0; i < nb_values; i++) {
      destroy_value(alloc, values + i);
    }

    alloc.deallocate(values, capacity_values);
  }

  static size_type popcount(bitmap_type val) noexcept {
    if (sizeof(bitmap_type) <= sizeof(unsigned int)) {
      return static_cast<size_type>(
          tsl::detail_popcount::popcount(static_cast<unsigned int>(val)));
    } else {
      return static_cast<size_type>(tsl::detail_popcount::popcountll(val));
    }
  }

  size_type index_to_offset(size_type index) const noexcept {
    tsl_sh_assert(index < BITMAP_NB_BITS);
    return popcount(m_bitmap_vals &
                    ((bitmap_type(1) << index) - bitmap_type(1)));
  }

  // TODO optimize
  size_type offset_to_index(size_type offset) const noexcept {
    tsl_sh_assert(offset < m_nb_elements);

    bitmap_type bitmap_vals = m_bitmap_vals;
    size_type index = 0;
    size_type nb_ones = 0;

    while (bitmap_vals != 0) {
      if ((bitmap_vals & 0x1) == 1) {
        if (nb_ones == offset) {
          break;
        }

        nb_ones++;
      }

      index++;
      bitmap_vals = bitmap_vals >> 1;
    }

    return index;
  }

  size_type next_capacity() const noexcept {
    return static_cast<size_type>(m_capacity + CAPACITY_GROWTH_STEP);
  }

  /**
   * Insertion
   *
   * Two situations:
   * - Either we are in a situation where
   * std::is_nothrow_move_constructible<value_type>::value is true. In this
   * case, on insertion we just reallocate m_values when we reach its capacity
   * (i.e. m_nb_elements == m_capacity), otherwise we just put the new value at
   * its appropriate place. We can easily keep the strong exception guarantee as
   * moving the values around is safe.
   * - Otherwise we are in a situation where
   * std::is_nothrow_move_constructible<value_type>::value is false. In this
   * case on EACH insertion we allocate a new area of m_nb_elements + 1 where we
   * copy the values of m_values into it and put the new value there. On
   * success, we set m_values to this new area. Even if slower, it's the only
   * way to preserve to strong exception guarantee.
   */
  template <typename... Args, typename U = value_type,
            typename std::enable_if<
                std::is_nothrow_move_constructible<U>::value>::type * = nullptr>
  void insert_at_offset(allocator_type &alloc, size_type offset,
                        Args &&...value_args) {
    if (m_nb_elements < m_capacity) {
      insert_at_offset_no_realloc(alloc, offset,
                                  std::forward<Args>(value_args)...);
    } else {
      insert_at_offset_realloc(alloc, offset, next_capacity(),
                               std::forward<Args>(value_args)...);
    }
  }

  template <typename... Args, typename U = value_type,
            typename std::enable_if<!std::is_nothrow_move_constructible<
                U>::value>::type * = nullptr>
  void insert_at_offset(allocator_type &alloc, size_type offset,
                        Args &&...value_args) {
    insert_at_offset_realloc(alloc, offset, m_nb_elements + 1,
                             std::forward<Args>(value_args)...);
  }

  template <typename... Args, typename U = value_type,
            typename std::enable_if<
                std::is_nothrow_move_constructible<U>::value>::type * = nullptr>
  void insert_at_offset_no_realloc(allocator_type &alloc, size_type offset,
                                   Args &&...value_args) {
    tsl_sh_assert(offset <= m_nb_elements);
    tsl_sh_assert(m_nb_elements < m_capacity);

    for (size_type i = m_nb_elements; i > offset; i--) {
      construct_value(alloc, m_values + i, std::move(m_values[i - 1]));
      destroy_value(alloc, m_values + i - 1);
    }

    try {
      construct_value(alloc, m_values + offset,
                      std::forward<Args>(value_args)...);
    } catch (...) {
      for (size_type i = offset; i < m_nb_elements; i++) {
        construct_value(alloc, m_values + i, std::move(m_values[i + 1]));
        destroy_value(alloc, m_values + i + 1);
      }
      throw;
    }
  }

  template <typename... Args, typename U = value_type,
            typename std::enable_if<
                std::is_nothrow_move_constructible<U>::value>::type * = nullptr>
  void insert_at_offset_realloc(allocator_type &alloc, size_type offset,
                                size_type new_capacity, Args &&...value_args) {
    tsl_sh_assert(new_capacity > m_nb_elements);

    value_type *new_values = alloc.allocate(new_capacity);
    // Allocate should throw if there is a failure
    tsl_sh_assert(new_values != nullptr);

    try {
      construct_value(alloc, new_values + offset,
                      std::forward<Args>(value_args)...);
    } catch (...) {
      alloc.deallocate(new_values, new_capacity);
      throw;
    }

    // Should not throw from here
    for (size_type i = 0; i < offset; i++) {
      construct_value(alloc, new_values + i, std::move(m_values[i]));
    }

    for (size_type i = offset; i < m_nb_elements; i++) {
      construct_value(alloc, new_values + i + 1, std::move(m_values[i]));
    }

    destroy_and_deallocate_values(alloc, m_values, m_nb_elements, m_capacity);

    m_values = new_values;
    m_capacity = new_capacity;
  }

  template <typename... Args, typename U = value_type,
            typename std::enable_if<!std::is_nothrow_move_constructible<
                U>::value>::type * = nullptr>
  void insert_at_offset_realloc(allocator_type &alloc, size_type offset,
                                size_type new_capacity, Args &&...value_args) {
    tsl_sh_assert(new_capacity > m_nb_elements);

    value_type *new_values = alloc.allocate(new_capacity);
    // Allocate should throw if there is a failure
    tsl_sh_assert(new_values != nullptr);

    size_type nb_new_values = 0;
    try {
      for (size_type i = 0; i < offset; i++) {
        construct_value(alloc, new_values + i, m_values[i]);
        nb_new_values++;
      }

      construct_value(alloc, new_values + offset,
                      std::forward<Args>(value_args)...);
      nb_new_values++;

      for (size_type i = offset; i < m_nb_elements; i++) {
        construct_value(alloc, new_values + i + 1, m_values[i]);
        nb_new_values++;
      }
    } catch (...) {
      destroy_and_deallocate_values(alloc, new_values, nb_new_values,
                                    new_capacity);
      throw;
    }

    tsl_sh_assert(nb_new_values == m_nb_elements + 1);

    destroy_and_deallocate_values(alloc, m_values, m_nb_elements, m_capacity);

    m_values = new_values;
    m_capacity = new_capacity;
  }

  /**
   * Erasure
   *
   * Two situations:
   * - Either we are in a situation where
   * std::is_nothrow_move_constructible<value_type>::value is true. Simply
   * destroy the value and left-shift move the value on the right of offset.
   * - Otherwise we are in a situation where
   * std::is_nothrow_move_constructible<value_type>::value is false. Copy all
   * the values except the one at offset into a new heap area. On success, we
   * set m_values to this new area. Even if slower, it's the only way to
   * preserve to strong exception guarantee.
   */
  template <typename... Args, typename U = value_type,
            typename std::enable_if<
                std::is_nothrow_move_constructible<U>::value>::type * = nullptr>
  void erase_at_offset(allocator_type &alloc, size_type offset) noexcept {
    tsl_sh_assert(offset < m_nb_elements);

    destroy_value(alloc, m_values + offset);

    for (size_type i = offset + 1; i < m_nb_elements; i++) {
      construct_value(alloc, m_values + i - 1, std::move(m_values[i]));
      destroy_value(alloc, m_values + i);
    }
  }

  template <typename... Args, typename U = value_type,
            typename std::enable_if<!std::is_nothrow_move_constructible<
                U>::value>::type * = nullptr>
  void erase_at_offset(allocator_type &alloc, size_type offset) {
    tsl_sh_assert(offset < m_nb_elements);

    // Erasing the last element, don't need to reallocate. We keep the capacity.
    if (offset + 1 == m_nb_elements) {
      destroy_value(alloc, m_values + offset);
      return;
    }

    tsl_sh_assert(m_nb_elements > 1);
    const size_type new_capacity = m_nb_elements - 1;

    value_type *new_values = alloc.allocate(new_capacity);
    // Allocate should throw if there is a failure
    tsl_sh_assert(new_values != nullptr);

    size_type nb_new_values = 0;
    try {
      for (size_type i = 0; i < m_nb_elements; i++) {
        if (i != offset) {
          construct_value(alloc, new_values + nb_new_values, m_values[i]);
          nb_new_values++;
        }
      }
    } catch (...) {
      destroy_and_deallocate_values(alloc, new_values, nb_new_values,
                                    new_capacity);
      throw;
    }

    tsl_sh_assert(nb_new_values == m_nb_elements - 1);

    destroy_and_deallocate_values(alloc, m_values, m_nb_elements, m_capacity);

    m_values = new_values;
    m_capacity = new_capacity;
  }

 private:
  value_type *m_values;

  bitmap_type m_bitmap_vals;
  bitmap_type m_bitmap_deleted_vals;

  size_type m_nb_elements;
  size_type m_capacity;
  bool m_last_array;
};

/**
 * Internal common class used by `sparse_map` and `sparse_set`.
 *
 * `ValueType` is what will be stored by `sparse_hash` (usually `std::pair<Key,
 * T>` for map and `Key` for set).
 *
 * `KeySelect` should be a `FunctionObject` which takes a `ValueType` in
 * parameter and returns a reference to the key.
 *
 * `ValueSelect` should be a `FunctionObject` which takes a `ValueType` in
 * parameter and returns a reference to the value. `ValueSelect` should be void
 * if there is no value (in a set for example).
 *
 * The strong exception guarantee only holds if `ExceptionSafety` is set to
 * `tsl::sh::exception_safety::strong`.
 *
 * `ValueType` must be nothrow move constructible and/or copy constructible.
 * Behaviour is undefined if the destructor of `ValueType` throws.
 *
 *
 * The class holds its buckets in a 2-dimensional fashion. Instead of having a
 * linear `std::vector<bucket>` for [0, bucket_count) where each bucket stores
 * one value, we have a `std::vector<sparse_array>` (m_sparse_buckets_data)
 * where each `sparse_array` stores multiple values (up to
 * `sparse_array::BITMAP_NB_BITS`). To convert a one dimensional `ibucket`
 * position to a position in `std::vector<sparse_array>` and a position in
 * `sparse_array`, use respectively the methods
 * `sparse_array::sparse_ibucket(ibucket)` and
 * `sparse_array::index_in_sparse_bucket(ibucket)`.
 */
template <class ValueType, class KeySelect, class ValueSelect, class Hash,
          class KeyEqual, class Allocator, class GrowthPolicy,
          tsl::sh::exception_safety ExceptionSafety, tsl::sh::sparsity Sparsity,
          tsl::sh::probing Probing>
class sparse_hash : private Allocator,
                    private Hash,
                    private KeyEqual,
                    private GrowthPolicy {
 private:
  template <typename U>
  using has_mapped_type =
      typename std::integral_constant<bool, !std::is_same<U, void>::value>;

  static_assert(
      noexcept(std::declval<GrowthPolicy>().bucket_for_hash(std::size_t(0))),
      "GrowthPolicy::bucket_for_hash must be noexcept.");
  static_assert(noexcept(std::declval<GrowthPolicy>().clear()),
                "GrowthPolicy::clear must be noexcept.");

 public:
  template <bool IsConst>
  class sparse_iterator;

  using key_type = typename KeySelect::key_type;
  using value_type = ValueType;
  using size_type = std::size_t;
  using difference_type = std::ptrdiff_t;
  using hasher = Hash;
  using key_equal = KeyEqual;
  using allocator_type = Allocator;
  using reference = value_type &;
  using const_reference = const value_type &;
  using pointer = value_type *;
  using const_pointer = const value_type *;
  using iterator = sparse_iterator<false>;
  using const_iterator = sparse_iterator<true>;

 private:
  using sparse_array =
      tsl::detail_sparse_hash::sparse_array<ValueType, Allocator, Sparsity>;

  using sparse_buckets_allocator = typename std::allocator_traits<
      allocator_type>::template rebind_alloc<sparse_array>;
  using sparse_buckets_container =
      std::vector<sparse_array, sparse_buckets_allocator>;

 public:
  /**
   * The `operator*()` and `operator->()` methods return a const reference and
   * const pointer respectively to the stored value type (`Key` for a set,
   * `std::pair<Key, T>` for a map).
   *
   * In case of a map, to get a mutable reference to the value `T` associated to
   * a key (the `.second` in the stored pair), you have to call `value()`.
   */
  template <bool IsConst>
  class sparse_iterator {
    friend class sparse_hash;

   private:
    using sparse_bucket_iterator = typename std::conditional<
        IsConst, typename sparse_buckets_container::const_iterator,
        typename sparse_buckets_container::iterator>::type;

    using sparse_array_iterator =
        typename std::conditional<IsConst,
                                  typename sparse_array::const_iterator,
                                  typename sparse_array::iterator>::type;

    /**
     * sparse_array_it should be nullptr if sparse_bucket_it ==
     * m_sparse_buckets_data.end(). (TODO better way?)
     */
    sparse_iterator(sparse_bucket_iterator sparse_bucket_it,
                    sparse_array_iterator sparse_array_it)
        : m_sparse_buckets_it(sparse_bucket_it),
          m_sparse_array_it(sparse_array_it) {}

   public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = const typename sparse_hash::value_type;
    using difference_type = std::ptrdiff_t;
    using reference = value_type &;
    using pointer = value_type *;

    sparse_iterator() noexcept {}

    // Copy constructor from iterator to const_iterator.
    template <bool TIsConst = IsConst,
              typename std::enable_if<TIsConst>::type * = nullptr>
    sparse_iterator(const sparse_iterator<!TIsConst> &other) noexcept
        : m_sparse_buckets_it(other.m_sparse_buckets_it),
          m_sparse_array_it(other.m_sparse_array_it) {}

    sparse_iterator(const sparse_iterator &other) = default;
    sparse_iterator(sparse_iterator &&other) = default;
    sparse_iterator &operator=(const sparse_iterator &other) = default;
    sparse_iterator &operator=(sparse_iterator &&other) = default;

    const typename sparse_hash::key_type &key() const {
      return KeySelect()(*m_sparse_array_it);
    }

    template <class U = ValueSelect,
              typename std::enable_if<has_mapped_type<U>::value &&
                                      IsConst>::type * = nullptr>
    const typename U::value_type &value() const {
      return U()(*m_sparse_array_it);
    }

    template <class U = ValueSelect,
              typename std::enable_if<has_mapped_type<U>::value &&
                                      !IsConst>::type * = nullptr>
    typename U::value_type &value() {
      return U()(*m_sparse_array_it);
    }

    reference operator*() const { return *m_sparse_array_it; }

    pointer operator->() const { return std::addressof(*m_sparse_array_it); }

    sparse_iterator &operator++() {
      tsl_sh_assert(m_sparse_array_it != nullptr);
      ++m_sparse_array_it;

      if (m_sparse_array_it == m_sparse_buckets_it->end()) {
        do {
          if (m_sparse_buckets_it->last()) {
            ++m_sparse_buckets_it;
            m_sparse_array_it = nullptr;
            return *this;
          }

          ++m_sparse_buckets_it;
        } while (m_sparse_buckets_it->empty());

        m_sparse_array_it = m_sparse_buckets_it->begin();
      }

      return *this;
    }

    sparse_iterator operator++(int) {
      sparse_iterator tmp(*this);
      ++*this;

      return tmp;
    }

    friend bool operator==(const sparse_iterator &lhs,
                           const sparse_iterator &rhs) {
      return lhs.m_sparse_buckets_it == rhs.m_sparse_buckets_it &&
             lhs.m_sparse_array_it == rhs.m_sparse_array_it;
    }

    friend bool operator!=(const sparse_iterator &lhs,
                           const sparse_iterator &rhs) {
      return !(lhs == rhs);
    }

   private:
    sparse_bucket_iterator m_sparse_buckets_it;
    sparse_array_iterator m_sparse_array_it;
  };

 public:
  sparse_hash(size_type bucket_count, const Hash &hash, const KeyEqual &equal,
              const Allocator &alloc, float max_load_factor)
      : Allocator(alloc),
        Hash(hash),
        KeyEqual(equal),
        GrowthPolicy(bucket_count),
        m_sparse_buckets_data(alloc),
        m_sparse_buckets(static_empty_sparse_bucket_ptr()),
        m_bucket_count(bucket_count),
        m_nb_elements(0),
        m_nb_deleted_buckets(0) {
    if (m_bucket_count > max_bucket_count()) {
      throw std::length_error("The map exceeds its maximum size.");
    }

    if (m_bucket_count > 0) {
      /*
       * We can't use the `vector(size_type count, const Allocator& alloc)`
       * constructor as it's only available in C++14 and we need to support
       * C++11. We thus must resize after using the `vector(const Allocator&
       * alloc)` constructor.
       *
       * We can't use `vector(size_type count, const T& value, const Allocator&
       * alloc)` as it requires the value T to be copyable.
       */
      m_sparse_buckets_data.resize(
          sparse_array::nb_sparse_buckets(bucket_count));
      m_sparse_buckets = m_sparse_buckets_data.data();

      tsl_sh_assert(!m_sparse_buckets_data.empty());
      m_sparse_buckets_data.back().set_as_last();
    }

    this->max_load_factor(max_load_factor);

    // Check in the constructor instead of outside of a function to avoid
    // compilation issues when value_type is not complete.
    static_assert(std::is_nothrow_move_constructible<value_type>::value ||
                      std::is_copy_constructible<value_type>::value,
                  "Key, and T if present, must be nothrow move constructible "
                  "and/or copy constructible.");
  }

  ~sparse_hash() { clear(); }

  sparse_hash(const sparse_hash &other)
      : Allocator(std::allocator_traits<
                  Allocator>::select_on_container_copy_construction(other)),
        Hash(other),
        KeyEqual(other),
        GrowthPolicy(other),
        m_sparse_buckets_data(
            std::allocator_traits<
                Allocator>::select_on_container_copy_construction(other)),
        m_bucket_count(other.m_bucket_count),
        m_nb_elements(other.m_nb_elements),
        m_nb_deleted_buckets(other.m_nb_deleted_buckets),
        m_load_threshold_rehash(other.m_load_threshold_rehash),
        m_load_threshold_clear_deleted(other.m_load_threshold_clear_deleted),
        m_max_load_factor(other.m_max_load_factor) {
    copy_buckets_from(other),
        m_sparse_buckets = m_sparse_buckets_data.empty()
                               ? static_empty_sparse_bucket_ptr()
                               : m_sparse_buckets_data.data();
  }

  sparse_hash(sparse_hash &&other) noexcept(
      std::is_nothrow_move_constructible<Allocator>::value
          &&std::is_nothrow_move_constructible<Hash>::value
              &&std::is_nothrow_move_constructible<KeyEqual>::value
                  &&std::is_nothrow_move_constructible<GrowthPolicy>::value
                      &&std::is_nothrow_move_constructible<
                          sparse_buckets_container>::value)
      : Allocator(std::move(other)),
        Hash(std::move(other)),
        KeyEqual(std::move(other)),
        GrowthPolicy(std::move(other)),
        m_sparse_buckets_data(std::move(other.m_sparse_buckets_data)),
        m_sparse_buckets(m_sparse_buckets_data.empty()
                             ? static_empty_sparse_bucket_ptr()
                             : m_sparse_buckets_data.data()),
        m_bucket_count(other.m_bucket_count),
        m_nb_elements(other.m_nb_elements),
        m_nb_deleted_buckets(other.m_nb_deleted_buckets),
        m_load_threshold_rehash(other.m_load_threshold_rehash),
        m_load_threshold_clear_deleted(other.m_load_threshold_clear_deleted),
        m_max_load_factor(other.m_max_load_factor) {
    other.GrowthPolicy::clear();
    other.m_sparse_buckets_data.clear();
    other.m_sparse_buckets = static_empty_sparse_bucket_ptr();
    other.m_bucket_count = 0;
    other.m_nb_elements = 0;
    other.m_nb_deleted_buckets = 0;
    other.m_load_threshold_rehash = 0;
    other.m_load_threshold_clear_deleted = 0;
  }

  sparse_hash &operator=(const sparse_hash &other) {
    if (this != &other) {
      clear();

      if (std::allocator_traits<
              Allocator>::propagate_on_container_copy_assignment::value) {
        Allocator::operator=(other);
      }

      Hash::operator=(other);
      KeyEqual::operator=(other);
      GrowthPolicy::operator=(other);

      if (std::allocator_traits<
              Allocator>::propagate_on_container_copy_assignment::value) {
        m_sparse_buckets_data =
            sparse_buckets_container(static_cast<const Allocator &>(other));
      } else {
        if (m_sparse_buckets_data.size() !=
            other.m_sparse_buckets_data.size()) {
          m_sparse_buckets_data =
              sparse_buckets_container(static_cast<const Allocator &>(*this));
        } else {
          m_sparse_buckets_data.clear();
        }
      }

      copy_buckets_from(other);
      m_sparse_buckets = m_sparse_buckets_data.empty()
                             ? static_empty_sparse_bucket_ptr()
                             : m_sparse_buckets_data.data();

      m_bucket_count = other.m_bucket_count;
      m_nb_elements = other.m_nb_elements;
      m_nb_deleted_buckets = other.m_nb_deleted_buckets;
      m_load_threshold_rehash = other.m_load_threshold_rehash;
      m_load_threshold_clear_deleted = other.m_load_threshold_clear_deleted;
      m_max_load_factor = other.m_max_load_factor;
    }

    return *this;
  }

  sparse_hash &operator=(sparse_hash &&other) {
    clear();

    if (std::allocator_traits<
            Allocator>::propagate_on_container_move_assignment::value) {
      static_cast<Allocator &>(*this) =
          std::move(static_cast<Allocator &>(other));
      m_sparse_buckets_data = std::move(other.m_sparse_buckets_data);
    } else if (static_cast<Allocator &>(*this) !=
               static_cast<Allocator &>(other)) {
      move_buckets_from(std::move(other));
    } else {
      static_cast<Allocator &>(*this) =
          std::move(static_cast<Allocator &>(other));
      m_sparse_buckets_data = std::move(other.m_sparse_buckets_data);
    }

    m_sparse_buckets = m_sparse_buckets_data.empty()
                           ? static_empty_sparse_bucket_ptr()
                           : m_sparse_buckets_data.data();

    static_cast<Hash &>(*this) = std::move(static_cast<Hash &>(other));
    static_cast<KeyEqual &>(*this) = std::move(static_cast<KeyEqual &>(other));
    static_cast<GrowthPolicy &>(*this) =
        std::move(static_cast<GrowthPolicy &>(other));
    m_bucket_count = other.m_bucket_count;
    m_nb_elements = other.m_nb_elements;
    m_nb_deleted_buckets = other.m_nb_deleted_buckets;
    m_load_threshold_rehash = other.m_load_threshold_rehash;
    m_load_threshold_clear_deleted = other.m_load_threshold_clear_deleted;
    m_max_load_factor = other.m_max_load_factor;

    other.GrowthPolicy::clear();
    other.m_sparse_buckets_data.clear();
    other.m_sparse_buckets = static_empty_sparse_bucket_ptr();
    other.m_bucket_count = 0;
    other.m_nb_elements = 0;
    other.m_nb_deleted_buckets = 0;
    other.m_load_threshold_rehash = 0;
    other.m_load_threshold_clear_deleted = 0;

    return *this;
  }

  allocator_type get_allocator() const {
    return static_cast<const Allocator &>(*this);
  }

  /*
   * Iterators
   */
  iterator begin() noexcept {
    auto begin = m_sparse_buckets_data.begin();
    while (begin != m_sparse_buckets_data.end() && begin->empty()) {
      ++begin;
    }

    return iterator(begin, (begin != m_sparse_buckets_data.end())
                               ? begin->begin()
                               : nullptr);
  }

  const_iterator begin() const noexcept { return cbegin(); }

  const_iterator cbegin() const noexcept {
    auto begin = m_sparse_buckets_data.cbegin();
    while (begin != m_sparse_buckets_data.cend() && begin->empty()) {
      ++begin;
    }

    return const_iterator(begin, (begin != m_sparse_buckets_data.cend())
                                     ? begin->cbegin()
                                     : nullptr);
  }

  iterator end() noexcept {
    return iterator(m_sparse_buckets_data.end(), nullptr);
  }

  const_iterator end() const noexcept { return cend(); }

  const_iterator cend() const noexcept {
    return const_iterator(m_sparse_buckets_data.cend(), nullptr);
  }

  /*
   * Capacity
   */
  bool empty() const noexcept { return m_nb_elements == 0; }

  size_type size() const noexcept { return m_nb_elements; }

  size_type max_size() const noexcept {
    return std::min(std::allocator_traits<Allocator>::max_size(),
                    m_sparse_buckets_data.max_size());
  }

  /*
   * Modifiers
   */
  void clear() noexcept {
    for (auto &bucket : m_sparse_buckets_data) {
      bucket.clear(*this);
    }

    m_nb_elements = 0;
    m_nb_deleted_buckets = 0;
  }

  template <typename P>
  std::pair<iterator, bool> insert(P &&value) {
    return insert_impl(KeySelect()(value), std::forward<P>(value));
  }

  template <typename P>
  iterator insert_hint(const_iterator hint, P &&value) {
    if (hint != cend() &&
        compare_keys(KeySelect()(*hint), KeySelect()(value))) {
      return mutable_iterator(hint);
    }

    return insert(std::forward<P>(value)).first;
  }

  template <class InputIt>
  void insert(InputIt first, InputIt last) {
    if (std::is_base_of<
            std::forward_iterator_tag,
            typename std::iterator_traits<InputIt>::iterator_category>::value) {
      const auto nb_elements_insert = std::distance(first, last);
      const size_type nb_free_buckets = m_load_threshold_rehash - size();
      tsl_sh_assert(m_load_threshold_rehash >= size());

      if (nb_elements_insert > 0 &&
          nb_free_buckets < size_type(nb_elements_insert)) {
        reserve(size() + size_type(nb_elements_insert));
      }
    }

    for (; first != last; ++first) {
      insert(*first);
    }
  }

  template <class K, class M>
  std::pair<iterator, bool> insert_or_assign(K &&key, M &&obj) {
    auto it = try_emplace(std::forward<K>(key), std::forward<M>(obj));
    if (!it.second) {
      it.first.value() = std::forward<M>(obj);
    }

    return it;
  }

  template <class K, class M>
  iterator insert_or_assign(const_iterator hint, K &&key, M &&obj) {
    if (hint != cend() && compare_keys(KeySelect()(*hint), key)) {
      auto it = mutable_iterator(hint);
      it.value() = std::forward<M>(obj);

      return it;
    }

    return insert_or_assign(std::forward<K>(key), std::forward<M>(obj)).first;
  }

  template <class... Args>
  std::pair<iterator, bool> emplace(Args &&...args) {
    return insert(value_type(std::forward<Args>(args)...));
  }

  template <class... Args>
  iterator emplace_hint(const_iterator hint, Args &&...args) {
    return insert_hint(hint, value_type(std::forward<Args>(args)...));
  }

  template <class K, class... Args>
  std::pair<iterator, bool> try_emplace(K &&key, Args &&...args) {
    return insert_impl(key, std::piecewise_construct,
                       std::forward_as_tuple(std::forward<K>(key)),
                       std::forward_as_tuple(std::forward<Args>(args)...));
  }

  template <class K, class... Args>
  iterator try_emplace_hint(const_iterator hint, K &&key, Args &&...args) {
    if (hint != cend() && compare_keys(KeySelect()(*hint), key)) {
      return mutable_iterator(hint);
    }

    return try_emplace(std::forward<K>(key), std::forward<Args>(args)...).first;
  }

  /**
   * Here to avoid `template<class K> size_type erase(const K& key)` being used
   * when we use an iterator instead of a const_iterator.
   */
  iterator erase(iterator pos) {
    tsl_sh_assert(pos != end() && m_nb_elements > 0);
    auto it_sparse_array_next =
        pos.m_sparse_buckets_it->erase(*this, pos.m_sparse_array_it);
    m_nb_elements--;
    m_nb_deleted_buckets++;

    if (it_sparse_array_next == pos.m_sparse_buckets_it->end()) {
      auto it_sparse_buckets_next = pos.m_sparse_buckets_it;
      do {
        ++it_sparse_buckets_next;
      } while (it_sparse_buckets_next != m_sparse_buckets_data.end() &&
               it_sparse_buckets_next->empty());

      if (it_sparse_buckets_next == m_sparse_buckets_data.end()) {
        return end();
      } else {
        return iterator(it_sparse_buckets_next,
                        it_sparse_buckets_next->begin());
      }
    } else {
      return iterator(pos.m_sparse_buckets_it, it_sparse_array_next);
    }
  }

  iterator erase(const_iterator pos) { return erase(mutable_iterator(pos)); }

  iterator erase(const_iterator first, const_iterator last) {
    if (first == last) {
      return mutable_iterator(first);
    }

    // TODO Optimize, could avoid the call to std::distance.
    const size_type nb_elements_to_erase =
        static_cast<size_type>(std::distance(first, last));
    auto to_delete = mutable_iterator(first);
    for (size_type i = 0; i < nb_elements_to_erase; i++) {
      to_delete = erase(to_delete);
    }

    return to_delete;
  }

  template <class K>
  size_type erase(const K &key) {
    return erase(key, hash_key(key));
  }

  template <class K>
  size_type erase(const K &key, std::size_t hash) {
    return erase_impl(key, hash);
  }

  void swap(sparse_hash &other) {
    using std::swap;

    if (std::allocator_traits<Allocator>::propagate_on_container_swap::value) {
      swap(static_cast<Allocator &>(*this), static_cast<Allocator &>(other));
    } else {
      tsl_sh_assert(static_cast<Allocator &>(*this) ==
                    static_cast<Allocator &>(other));
    }

    swap(static_cast<Hash &>(*this), static_cast<Hash &>(other));
    swap(static_cast<KeyEqual &>(*this), static_cast<KeyEqual &>(other));
    swap(static_cast<GrowthPolicy &>(*this),
         static_cast<GrowthPolicy &>(other));
    swap(m_sparse_buckets_data, other.m_sparse_buckets_data);
    swap(m_sparse_buckets, other.m_sparse_buckets);
    swap(m_bucket_count, other.m_bucket_count);
    swap(m_nb_elements, other.m_nb_elements);
    swap(m_nb_deleted_buckets, other.m_nb_deleted_buckets);
    swap(m_load_threshold_rehash, other.m_load_threshold_rehash);
    swap(m_load_threshold_clear_deleted, other.m_load_threshold_clear_deleted);
    swap(m_max_load_factor, other.m_max_load_factor);
  }

  /*
   * Lookup
   */
  template <
      class K, class U = ValueSelect,
      typename std::enable_if<has_mapped_type<U>::value>::type * = nullptr>
  typename U::value_type &at(const K &key) {
    return at(key, hash_key(key));
  }

  template <
      class K, class U = ValueSelect,
      typename std::enable_if<has_mapped_type<U>::value>::type * = nullptr>
  typename U::value_type &at(const K &key, std::size_t hash) {
    return const_cast<typename U::value_type &>(
        static_cast<const sparse_hash *>(this)->at(key, hash));
  }

  template <
      class K, class U = ValueSelect,
      typename std::enable_if<has_mapped_type<U>::value>::type * = nullptr>
  const typename U::value_type &at(const K &key) const {
    return at(key, hash_key(key));
  }

  template <
      class K, class U = ValueSelect,
      typename std::enable_if<has_mapped_type<U>::value>::type * = nullptr>
  const typename U::value_type &at(const K &key, std::size_t hash) const {
    auto it = find(key, hash);
    if (it != cend()) {
      return it.value();
    } else {
      throw std::out_of_range("Couldn't find key.");
    }
  }

  template <
      class K, class U = ValueSelect,
      typename std::enable_if<has_mapped_type<U>::value>::type * = nullptr>
  typename U::value_type &operator[](K &&key) {
    return try_emplace(std::forward<K>(key)).first.value();
  }

  template <class K>
  bool contains(const K &key) const {
    return contains(key, hash_key(key));
  }

  template <class K>
  bool contains(const K &key, std::size_t hash) const {
    return count(key, hash) != 0;
  }

  template <class K>
  size_type count(const K &key) const {
    return count(key, hash_key(key));
  }

  template <class K>
  size_type count(const K &key, std::size_t hash) const {
    if (find(key, hash) != cend()) {
      return 1;
    } else {
      return 0;
    }
  }

  template <class K>
  iterator find(const K &key) {
    return find_impl(key, hash_key(key));
  }

  template <class K>
  iterator find(const K &key, std::size_t hash) {
    return find_impl(key, hash);
  }

  template <class K>
  const_iterator find(const K &key) const {
    return find_impl(key, hash_key(key));
  }

  template <class K>
  const_iterator find(const K &key, std::size_t hash) const {
    return find_impl(key, hash);
  }

  template <class K>
  std::pair<iterator, iterator> equal_range(const K &key) {
    return equal_range(key, hash_key(key));
  }

  template <class K>
  std::pair<iterator, iterator> equal_range(const K &key, std::size_t hash) {
    iterator it = find(key, hash);
    return std::make_pair(it, (it == end()) ? it : std::next(it));
  }

  template <class K>
  std::pair<const_iterator, const_iterator> equal_range(const K &key) const {
    return equal_range(key, hash_key(key));
  }

  template <class K>
  std::pair<const_iterator, const_iterator> equal_range(
      const K &key, std::size_t hash) const {
    const_iterator it = find(key, hash);
    return std::make_pair(it, (it == cend()) ? it : std::next(it));
  }

  /*
   * Bucket interface
   */
  size_type bucket_count() const { return m_bucket_count; }

  size_type max_bucket_count() const {
    return m_sparse_buckets_data.max_size();
  }

  /*
   * Hash policy
   */
  float load_factor() const {
    if (bucket_count() == 0) {
      return 0;
    }

    return float(m_nb_elements) / float(bucket_count());
  }

  float max_load_factor() const { return m_max_load_factor; }

  void max_load_factor(float ml) {
    m_max_load_factor = std::max(0.1f, std::min(ml, 0.8f));
    m_load_threshold_rehash =
        size_type(float(bucket_count()) * m_max_load_factor);

    const float max_load_factor_with_deleted_buckets =
        m_max_load_factor + 0.5f * (1.0f - m_max_load_factor);
    tsl_sh_assert(max_load_factor_with_deleted_buckets > 0.0f &&
                  max_load_factor_with_deleted_buckets <= 1.0f);
    m_load_threshold_clear_deleted =
        size_type(float(bucket_count()) * max_load_factor_with_deleted_buckets);
  }

  void rehash(size_type count) {
    count = std::max(count,
                     size_type(std::ceil(float(size()) / max_load_factor())));
    rehash_impl(count);
  }

  void reserve(size_type count) {
    rehash(size_type(std::ceil(float(count) / max_load_factor())));
  }

  /*
   * Observers
   */
  hasher hash_function() const { return static_cast<const Hash &>(*this); }

  key_equal key_eq() const { return static_cast<const KeyEqual &>(*this); }

  /*
   * Other
   */
  iterator mutable_iterator(const_iterator pos) {
    auto it_sparse_buckets =
        m_sparse_buckets_data.begin() +
        std::distance(m_sparse_buckets_data.cbegin(), pos.m_sparse_buckets_it);

    return iterator(it_sparse_buckets,
                    sparse_array::mutable_iterator(pos.m_sparse_array_it));
  }

  template <class Serializer>
  void serialize(Serializer &serializer) const {
    serialize_impl(serializer);
  }

  template <class Deserializer>
  void deserialize(Deserializer &deserializer, bool hash_compatible) {
    deserialize_impl(deserializer, hash_compatible);
  }

 private:
  template <class K>
  std::size_t hash_key(const K &key) const {
    return Hash::operator()(key);
  }

  template <class K1, class K2>
  bool compare_keys(const K1 &key1, const K2 &key2) const {
    return KeyEqual::operator()(key1, key2);
  }

  size_type bucket_for_hash(std::size_t hash) const {
    const std::size_t bucket = GrowthPolicy::bucket_for_hash(hash);
    tsl_sh_assert(sparse_array::sparse_ibucket(bucket) <
                      m_sparse_buckets_data.size() ||
                  (bucket == 0 && m_sparse_buckets_data.empty()));

    return bucket;
  }

  template <class U = GrowthPolicy,
            typename std::enable_if<is_power_of_two_policy<U>::value>::type * =
                nullptr>
  size_type next_bucket(size_type ibucket, size_type iprobe) const {
    (void)iprobe;
    if (Probing == tsl::sh::probing::linear) {
      return (ibucket + 1) & this->m_mask;
    } else {
      tsl_sh_assert(Probing == tsl::sh::probing::quadratic);
      return (ibucket + iprobe) & this->m_mask;
    }
  }

  template <class U = GrowthPolicy,
            typename std::enable_if<!is_power_of_two_policy<U>::value>::type * =
                nullptr>
  size_type next_bucket(size_type ibucket, size_type iprobe) const {
    (void)iprobe;
    if (Probing == tsl::sh::probing::linear) {
      ibucket++;
      return (ibucket != bucket_count()) ? ibucket : 0;
    } else {
      tsl_sh_assert(Probing == tsl::sh::probing::quadratic);
      ibucket += iprobe;
      return (ibucket < bucket_count()) ? ibucket : ibucket % bucket_count();
    }
  }

  // TODO encapsulate m_sparse_buckets_data to avoid the managing the allocator
  void copy_buckets_from(const sparse_hash &other) {
    m_sparse_buckets_data.reserve(other.m_sparse_buckets_data.size());

    try {
      for (const auto &bucket : other.m_sparse_buckets_data) {
        m_sparse_buckets_data.emplace_back(bucket,
                                           static_cast<Allocator &>(*this));
      }
    } catch (...) {
      clear();
      throw;
    }

    tsl_sh_assert(m_sparse_buckets_data.empty() ||
                  m_sparse_buckets_data.back().last());
  }

  void move_buckets_from(sparse_hash &&other) {
    m_sparse_buckets_data.reserve(other.m_sparse_buckets_data.size());

    try {
      for (auto &&bucket : other.m_sparse_buckets_data) {
        m_sparse_buckets_data.emplace_back(std::move(bucket),
                                           static_cast<Allocator &>(*this));
      }
    } catch (...) {
      clear();
      throw;
    }

    tsl_sh_assert(m_sparse_buckets_data.empty() ||
                  m_sparse_buckets_data.back().last());
  }

  template <class K, class... Args>
  std::pair<iterator, bool> insert_impl(const K &key,
                                        Args &&...value_type_args) {
    if (size() >= m_load_threshold_rehash) {
      rehash_impl(GrowthPolicy::next_bucket_count());
    } else if (size() + m_nb_deleted_buckets >=
               m_load_threshold_clear_deleted) {
      clear_deleted_buckets();
    }
    tsl_sh_assert(!m_sparse_buckets_data.empty());

    /**
     * We must insert the value in the first empty or deleted bucket we find. If
     * we first find a deleted bucket, we still have to continue the search
     * until we find an empty bucket or until we have searched all the buckets
     * to be sure that the value is not in the hash table. We thus remember the
     * position, if any, of the first deleted bucket we have encountered so we
     * can insert it there if needed.
     */
    bool found_first_deleted_bucket = false;
    std::size_t sparse_ibucket_first_deleted = 0;
    typename sparse_array::size_type index_in_sparse_bucket_first_deleted = 0;

    const std::size_t hash = hash_key(key);
    std::size_t ibucket = bucket_for_hash(hash);

    std::size_t probe = 0;
    while (true) {
      std::size_t sparse_ibucket = sparse_array::sparse_ibucket(ibucket);
      auto index_in_sparse_bucket =
          sparse_array::index_in_sparse_bucket(ibucket);

      if (m_sparse_buckets[sparse_ibucket].has_value(index_in_sparse_bucket)) {
        auto value_it =
            m_sparse_buckets[sparse_ibucket].value(index_in_sparse_bucket);
        if (compare_keys(key, KeySelect()(*value_it))) {
          return std::make_pair(
              iterator(m_sparse_buckets_data.begin() + sparse_ibucket,
                       value_it),
              false);
        }
      } else if (m_sparse_buckets[sparse_ibucket].has_deleted_value(
                     index_in_sparse_bucket) &&
                 probe < m_bucket_count) {
        if (!found_first_deleted_bucket) {
          found_first_deleted_bucket = true;
          sparse_ibucket_first_deleted = sparse_ibucket;
          index_in_sparse_bucket_first_deleted = index_in_sparse_bucket;
        }
      } else if (found_first_deleted_bucket) {
        auto it = insert_in_bucket(sparse_ibucket_first_deleted,
                                   index_in_sparse_bucket_first_deleted,
                                   std::forward<Args>(value_type_args)...);
        m_nb_deleted_buckets--;

        return it;
      } else {
        return insert_in_bucket(sparse_ibucket, index_in_sparse_bucket,
                                std::forward<Args>(value_type_args)...);
      }

      probe++;
      ibucket = next_bucket(ibucket, probe);
    }
  }

  template <class... Args>
  std::pair<iterator, bool> insert_in_bucket(
      std::size_t sparse_ibucket,
      typename sparse_array::size_type index_in_sparse_bucket,
      Args &&...value_type_args) {
    auto value_it = m_sparse_buckets[sparse_ibucket].set(
        *this, index_in_sparse_bucket, std::forward<Args>(value_type_args)...);
    m_nb_elements++;

    return std::make_pair(
        iterator(m_sparse_buckets_data.begin() + sparse_ibucket, value_it),
        true);
  }

  template <class K>
  size_type erase_impl(const K &key, std::size_t hash) {
    std::size_t ibucket = bucket_for_hash(hash);

    std::size_t probe = 0;
    while (true) {
      const std::size_t sparse_ibucket = sparse_array::sparse_ibucket(ibucket);
      const auto index_in_sparse_bucket =
          sparse_array::index_in_sparse_bucket(ibucket);

      if (m_sparse_buckets[sparse_ibucket].has_value(index_in_sparse_bucket)) {
        auto value_it =
            m_sparse_buckets[sparse_ibucket].value(index_in_sparse_bucket);
        if (compare_keys(key, KeySelect()(*value_it))) {
          m_sparse_buckets[sparse_ibucket].erase(*this, value_it,
                                                 index_in_sparse_bucket);
          m_nb_elements--;
          m_nb_deleted_buckets++;

          return 1;
        }
      } else if (!m_sparse_buckets[sparse_ibucket].has_deleted_value(
                     index_in_sparse_bucket) ||
                 probe >= m_bucket_count) {
        return 0;
      }

      probe++;
      ibucket = next_bucket(ibucket, probe);
    }
  }

  template <class K>
  iterator find_impl(const K &key, std::size_t hash) {
    return mutable_iterator(
        static_cast<const sparse_hash *>(this)->find(key, hash));
  }

  template <class K>
  const_iterator find_impl(const K &key, std::size_t hash) const {
    std::size_t ibucket = bucket_for_hash(hash);

    std::size_t probe = 0;
    while (true) {
      const std::size_t sparse_ibucket = sparse_array::sparse_ibucket(ibucket);
      const auto index_in_sparse_bucket =
          sparse_array::index_in_sparse_bucket(ibucket);

      if (m_sparse_buckets[sparse_ibucket].has_value(index_in_sparse_bucket)) {
        auto value_it =
            m_sparse_buckets[sparse_ibucket].value(index_in_sparse_bucket);
        if (compare_keys(key, KeySelect()(*value_it))) {
          return const_iterator(m_sparse_buckets_data.cbegin() + sparse_ibucket,
                                value_it);
        }
      } else if (!m_sparse_buckets[sparse_ibucket].has_deleted_value(
                     index_in_sparse_bucket) ||
                 probe >= m_bucket_count) {
        return cend();
      }

      probe++;
      ibucket = next_bucket(ibucket, probe);
    }
  }

  void clear_deleted_buckets() {
    // TODO could be optimized, we could do it in-place instead of allocating a
    // new bucket array.
    rehash_impl(m_bucket_count);
    tsl_sh_assert(m_nb_deleted_buckets == 0);
  }

  template <tsl::sh::exception_safety U = ExceptionSafety,
            typename std::enable_if<U == tsl::sh::exception_safety::basic>::type
                * = nullptr>
  void rehash_impl(size_type count) {
    sparse_hash new_table(count, static_cast<Hash &>(*this),
                          static_cast<KeyEqual &>(*this),
                          static_cast<Allocator &>(*this), m_max_load_factor);

    for (auto &bucket : m_sparse_buckets_data) {
      for (auto &val : bucket) {
        new_table.insert_on_rehash(std::move(val));
      }

      // TODO try to reuse some of the memory
      bucket.clear(*this);
    }

    new_table.swap(*this);
  }

  /**
   * TODO: For now we copy each element into the new map. We could move
   * them if they are nothrow_move_constructible without triggering
   * any exception if we reserve enough space in the sparse arrays beforehand.
   */
  template <tsl::sh::exception_safety U = ExceptionSafety,
            typename std::enable_if<
                U == tsl::sh::exception_safety::strong>::type * = nullptr>
  void rehash_impl(size_type count) {
    sparse_hash new_table(count, static_cast<Hash &>(*this),
                          static_cast<KeyEqual &>(*this),
                          static_cast<Allocator &>(*this), m_max_load_factor);

    for (const auto &bucket : m_sparse_buckets_data) {
      for (const auto &val : bucket) {
        new_table.insert_on_rehash(val);
      }
    }

    new_table.swap(*this);
  }

  template <typename K>
  void insert_on_rehash(K &&key_value) {
    const key_type &key = KeySelect()(key_value);

    const std::size_t hash = hash_key(key);
    std::size_t ibucket = bucket_for_hash(hash);

    std::size_t probe = 0;
    while (true) {
      std::size_t sparse_ibucket = sparse_array::sparse_ibucket(ibucket);
      auto index_in_sparse_bucket =
          sparse_array::index_in_sparse_bucket(ibucket);

      if (!m_sparse_buckets[sparse_ibucket].has_value(index_in_sparse_bucket)) {
        m_sparse_buckets[sparse_ibucket].set(*this, index_in_sparse_bucket,
                                             std::forward<K>(key_value));
        m_nb_elements++;

        return;
      } else {
        tsl_sh_assert(!compare_keys(
            key, KeySelect()(*m_sparse_buckets[sparse_ibucket].value(
                     index_in_sparse_bucket))));
      }

      probe++;
      ibucket = next_bucket(ibucket, probe);
    }
  }

  template <class Serializer>
  void serialize_impl(Serializer &serializer) const {
    const slz_size_type version = SERIALIZATION_PROTOCOL_VERSION;
    serializer(version);

    const slz_size_type bucket_count = m_bucket_count;
    serializer(bucket_count);

    const slz_size_type nb_sparse_buckets = m_sparse_buckets_data.size();
    serializer(nb_sparse_buckets);

    const slz_size_type nb_elements = m_nb_elements;
    serializer(nb_elements);

    const slz_size_type nb_deleted_buckets = m_nb_deleted_buckets;
    serializer(nb_deleted_buckets);

    const float max_load_factor = m_max_load_factor;
    serializer(max_load_factor);

    for (const auto &bucket : m_sparse_buckets_data) {
      bucket.serialize(serializer);
    }
  }

  template <class Deserializer>
  void deserialize_impl(Deserializer &deserializer, bool hash_compatible) {
    tsl_sh_assert(
        m_bucket_count == 0 &&
        m_sparse_buckets_data.empty());  // Current hash table must be empty

    const slz_size_type version =
        deserialize_value<slz_size_type>(deserializer);
    // For now we only have one version of the serialization protocol.
    // If it doesn't match there is a problem with the file.
    if (version != SERIALIZATION_PROTOCOL_VERSION) {
      throw std::runtime_error(
          "Can't deserialize the sparse_map/set. The "
          "protocol version header is invalid.");
    }

    const slz_size_type bucket_count_ds =
        deserialize_value<slz_size_type>(deserializer);
    const slz_size_type nb_sparse_buckets =
        deserialize_value<slz_size_type>(deserializer);
    const slz_size_type nb_elements =
        deserialize_value<slz_size_type>(deserializer);
    const slz_size_type nb_deleted_buckets =
        deserialize_value<slz_size_type>(deserializer);
    const float max_load_factor = deserialize_value<float>(deserializer);

    if (!hash_compatible) {
      this->max_load_factor(max_load_factor);
      reserve(numeric_cast<size_type>(nb_elements,
                                      "Deserialized nb_elements is too big."));
      for (slz_size_type ibucket = 0; ibucket < nb_sparse_buckets; ibucket++) {
        sparse_array::deserialize_values_into_sparse_hash(deserializer, *this);
      }
    } else {
      m_bucket_count = numeric_cast<size_type>(
          bucket_count_ds, "Deserialized bucket_count is too big.");

      GrowthPolicy::operator=(GrowthPolicy(m_bucket_count));
      // GrowthPolicy should not modify the bucket count we got from
      // deserialization
      if (m_bucket_count != bucket_count_ds) {
        throw std::runtime_error(
            "The GrowthPolicy is not the same even though "
            "hash_compatible is true.");
      }

      if (nb_sparse_buckets !=
          sparse_array::nb_sparse_buckets(m_bucket_count)) {
        throw std::runtime_error("Deserialized nb_sparse_buckets is invalid.");
      }

      m_nb_elements = numeric_cast<size_type>(
          nb_elements, "Deserialized nb_elements is too big.");
      m_nb_deleted_buckets = numeric_cast<size_type>(
          nb_deleted_buckets, "Deserialized nb_deleted_buckets is too big.");

      m_sparse_buckets_data.reserve(numeric_cast<size_type>(
          nb_sparse_buckets, "Deserialized nb_sparse_buckets is too big."));
      for (slz_size_type ibucket = 0; ibucket < nb_sparse_buckets; ibucket++) {
        m_sparse_buckets_data.emplace_back(
            sparse_array::deserialize_hash_compatible(
                deserializer, static_cast<Allocator &>(*this)));
      }

      if (!m_sparse_buckets_data.empty()) {
        m_sparse_buckets_data.back().set_as_last();
        m_sparse_buckets = m_sparse_buckets_data.data();
      }

      this->max_load_factor(max_load_factor);
      if (load_factor() > this->max_load_factor()) {
        throw std::runtime_error(
            "Invalid max_load_factor. Check that the serializer and "
            "deserializer support "
            "floats correctly as they can be converted implicitely to ints.");
      }
    }
  }

 public:
  static const size_type DEFAULT_INIT_BUCKET_COUNT = 0;
  static constexpr float DEFAULT_MAX_LOAD_FACTOR = 0.5f;

  /**
   * Protocol version currenlty used for serialization.
   */
  static const slz_size_type SERIALIZATION_PROTOCOL_VERSION = 1;

  /**
   * Return an always valid pointer to an static empty bucket_entry with
   * last_bucket() == true.
   */
  sparse_array *static_empty_sparse_bucket_ptr() {
    static sparse_array empty_sparse_bucket(true);
    return &empty_sparse_bucket;
  }

 private:
  sparse_buckets_container m_sparse_buckets_data;

  /**
   * Points to m_sparse_buckets_data.data() if !m_sparse_buckets_data.empty()
   * otherwise points to static_empty_sparse_bucket_ptr. This variable is useful
   * to avoid the cost of checking if m_sparse_buckets_data is empty when trying
   * to find an element.
   *
   * TODO Remove m_sparse_buckets_data and only use a pointer instead of a
   * pointer+vector to save some space in the sparse_hash object.
   */
  sparse_array *m_sparse_buckets;

  size_type m_bucket_count;
  size_type m_nb_elements;
  size_type m_nb_deleted_buckets;

  /**
   * Maximum that m_nb_elements can reach before a rehash occurs automatically
   * to grow the hash table.
   */
  size_type m_load_threshold_rehash;

  /**
   * Maximum that m_nb_elements + m_nb_deleted_buckets can reach before cleaning
   * up the buckets marked as deleted.
   */
  size_type m_load_threshold_clear_deleted;
  float m_max_load_factor;
};

}  // namespace detail_sparse_hash
}  // namespace tsl

#endif
