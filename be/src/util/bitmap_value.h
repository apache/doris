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

#ifndef DORIS_BE_SRC_UTIL_BITMAP_VALUE_H
#define DORIS_BE_SRC_UTIL_BITMAP_VALUE_H

#include <algorithm>
#include <cstdarg>
#include <cstdio>
#include <limits>
#include <map>
#include <new>
#include <numeric>
#include <roaring/roaring.hh>
#include <stdexcept>
#include <string>
#include <utility>

#include "common/logging.h"
#include "udf/udf.h"
#include "util/coding.h"

namespace doris {

// serialized bitmap := TypeCode(1), Payload
// The format of payload depends on value of TypeCode which is defined below
struct BitmapTypeCode {
    enum type {
        // An empty bitmap. Payload is 0 byte.
        // added in 0.11
        EMPTY = 0,
        // A bitmap containing only one element that is in [0, UINT32_MAX]
        // Payload := UInt32LittleEndian(4 byte)
        // added in 0.11
        SINGLE32 = 1,
        // A bitmap whose maximum element is in [0, UINT32_MAX]
        // Payload := the standard RoaringBitmap format described by
        // https://github.com/RoaringBitmap/RoaringFormatSpec/
        // added in 0.11
        BITMAP32 = 2,
        // A bitmap containing only one element that is in (UINT32_MAX, UINT64_MAX]
        // Payload := UInt64LittleEndian(8 byte)
        // added in 0.12
        SINGLE64 = 3,
        // A bitmap whose maximum element is in (UINT32_MAX, UINT64_MAX].
        //
        // To support 64-bits elements, all elements with the same high 32 bits are stored in a
        // RoaringBitmap containing only the lower 32 bits. Thus we could use
        // map<uint32_t, RoaringBitmap> to represent bitmap of 64-bits ints.
        //
        // Since there is no standard format for 64-bits RoaringBitmap, we define our own as below
        // Payload := NumRoaring(vint64), { MapKey, MapValue }^NumRoaring
        // - MapKey := the shared high 32 bits in UInt32LittleEndian(4 byte)
        // - MapValue := the standard RoaringBitmap format
        //
        // added in 0.12
        BITMAP64 = 4
    };
};

namespace detail {

class Roaring64MapSetBitForwardIterator;

// Forked from https://github.com/RoaringBitmap/CRoaring/blob/v0.2.60/cpp/roaring64map.hh
// What we change includes
// - a custom serialization format is used inside read()/write()/getSizeInBytes()
// - added clear() and is32BitsEnough()
class Roaring64Map {
public:
    /**
     * Create an empty bitmap
     */
    Roaring64Map() = default;

    /**
     * Construct a bitmap from a list of 32-bit integer values.
     */
    Roaring64Map(size_t n, const uint32_t* data) { addMany(n, data); }

    /**
     * Construct a bitmap from a list of 64-bit integer values.
     */
    Roaring64Map(size_t n, const uint64_t* data) { addMany(n, data); }

    /**
     * Construct a 64-bit map from a 32-bit one
     */
    Roaring64Map(const Roaring& r) { emplaceOrInsert(0, r); }

    /**
     * Construct a roaring object from the C struct.
     *
     * Passing a NULL point is unsafe.
     */
    Roaring64Map(roaring_bitmap_t* s) { emplaceOrInsert(0, s); }

    /**
     * Construct a bitmap from a list of integer values.
     */
    static Roaring64Map bitmapOf(size_t n...) {
        Roaring64Map ans;
        va_list vl;
        va_start(vl, n);
        for (size_t i = 0; i < n; i++) {
            ans.add(va_arg(vl, uint64_t));
        }
        va_end(vl);
        return ans;
    }

    /**
     * Add value x
     *
     */
    void add(uint32_t x) {
        roarings[0].add(x);
        roarings[0].setCopyOnWrite(copyOnWrite);
    }
    void add(uint64_t x) {
        roarings[highBytes(x)].add(lowBytes(x));
        roarings[highBytes(x)].setCopyOnWrite(copyOnWrite);
    }

    /**
     * Add value x
     * Returns true if a new value was added, false if the value was already existing.
     */
    bool addChecked(uint32_t x) {
        bool result = roarings[0].addChecked(x);
        roarings[0].setCopyOnWrite(copyOnWrite);
        return result;
    }
    bool addChecked(uint64_t x) {
        bool result = roarings[highBytes(x)].addChecked(lowBytes(x));
        roarings[highBytes(x)].setCopyOnWrite(copyOnWrite);
        return result;
    }

    /**
     * Add value n_args from pointer vals
     *
     */
    void addMany(size_t n_args, const uint32_t* vals) {
        for (size_t lcv = 0; lcv < n_args; lcv++) {
            roarings[0].add(vals[lcv]);
            roarings[0].setCopyOnWrite(copyOnWrite);
        }
    }
    void addMany(size_t n_args, const uint64_t* vals) {
        for (size_t lcv = 0; lcv < n_args; lcv++) {
            roarings[highBytes(vals[lcv])].add(lowBytes(vals[lcv]));
            roarings[highBytes(vals[lcv])].setCopyOnWrite(copyOnWrite);
        }
    }

    /**
     * Remove value x
     *
     */
    void remove(uint32_t x) { roarings[0].remove(x); }
    void remove(uint64_t x) {
        auto roaring_iter = roarings.find(highBytes(x));
        if (roaring_iter != roarings.cend()) roaring_iter->second.remove(lowBytes(x));
    }

    /**
     * Remove value x
     * Returns true if a new value was removed, false if the value was not existing.
     */
    bool removeChecked(uint32_t x) { return roarings[0].removeChecked(x); }
    bool removeChecked(uint64_t x) {
        auto roaring_iter = roarings.find(highBytes(x));
        if (roaring_iter != roarings.cend()) return roaring_iter->second.removeChecked(lowBytes(x));
        return false;
    }

    /**
     * Return the largest value (if not empty)
     *
     */
    uint64_t maximum() const {
        for (auto roaring_iter = roarings.crbegin(); roaring_iter != roarings.crend();
             ++roaring_iter) {
            if (!roaring_iter->second.isEmpty()) {
                return uniteBytes(roaring_iter->first, roaring_iter->second.maximum());
            }
        }
        // we put std::numeric_limits<>::max/min in parenthesis
        // to avoid a clash with the Windows.h header under Windows
        return (std::numeric_limits<uint64_t>::min)();
    }

    /**
     * Return the smallest value (if not empty)
     *
     */
    uint64_t minimum() const {
        for (auto roaring_iter = roarings.cbegin(); roaring_iter != roarings.cend();
             ++roaring_iter) {
            if (!roaring_iter->second.isEmpty()) {
                return uniteBytes(roaring_iter->first, roaring_iter->second.minimum());
            }
        }
        // we put std::numeric_limits<>::max/min in parenthesis
        // to avoid a clash with the Windows.h header under Windows
        return (std::numeric_limits<uint64_t>::max)();
    }

    /**
     * Check if value x is present
     */
    bool contains(uint32_t x) const {
        return roarings.count(0) == 0 ? false : roarings.at(0).contains(x);
    }
    bool contains(uint64_t x) const {
        return roarings.count(highBytes(x)) == 0 ? false
                                                 : roarings.at(highBytes(x)).contains(lowBytes(x));
    }

    /**
     * Compute the intersection between the current bitmap and the provided
     * bitmap,
     * writing the result in the current bitmap. The provided bitmap is not
     * modified.
     */
    Roaring64Map& operator&=(const Roaring64Map& r) {
        for (auto& map_entry : roarings) {
            if (r.roarings.count(map_entry.first) == 1)
                map_entry.second &= r.roarings.at(map_entry.first);
            else
                map_entry.second = Roaring();
        }
        return *this;
    }

    /**
     * Compute the difference between the current bitmap and the provided
     * bitmap,
     * writing the result in the current bitmap. The provided bitmap is not
     * modified.
     */
    Roaring64Map& operator-=(const Roaring64Map& r) {
        for (auto& map_entry : roarings) {
            if (r.roarings.count(map_entry.first) == 1)
                map_entry.second -= r.roarings.at(map_entry.first);
        }
        return *this;
    }

    /**
     * Compute the union between the current bitmap and the provided bitmap,
     * writing the result in the current bitmap. The provided bitmap is not
     * modified.
     *
     * See also the fastunion function to aggregate many bitmaps more quickly.
     */
    Roaring64Map& operator|=(const Roaring64Map& r) {
        for (const auto& map_entry : r.roarings) {
            if (roarings.count(map_entry.first) == 0) {
                roarings[map_entry.first] = map_entry.second;
                roarings[map_entry.first].setCopyOnWrite(copyOnWrite);
            } else
                roarings[map_entry.first] |= map_entry.second;
        }
        return *this;
    }

    /**
     * Compute the symmetric union between the current bitmap and the provided
     * bitmap,
     * writing the result in the current bitmap. The provided bitmap is not
     * modified.
     */
    Roaring64Map& operator^=(const Roaring64Map& r) {
        for (const auto& map_entry : r.roarings) {
            if (roarings.count(map_entry.first) == 0) {
                roarings[map_entry.first] = map_entry.second;
                roarings[map_entry.first].setCopyOnWrite(copyOnWrite);
            } else
                roarings[map_entry.first] ^= map_entry.second;
        }
        return *this;
    }

    /**
     * Exchange the content of this bitmap with another.
     */
    void swap(Roaring64Map& r) { roarings.swap(r.roarings); }

    /**
     * Get the cardinality of the bitmap (number of elements).
     * Throws std::length_error in the special case where the bitmap is full
     * (cardinality() == 2^64). Check isFull() before calling to avoid
     * exception.
     */
    uint64_t cardinality() const {
        if (isFull()) {
            throw std::length_error(
                    "bitmap is full, cardinality is 2^64, "
                    "unable to represent in a 64-bit integer");
        }
        return std::accumulate(
                roarings.cbegin(), roarings.cend(), (uint64_t)0,
                [](uint64_t previous, const std::pair<const uint32_t, Roaring>& map_entry) {
                    return previous + map_entry.second.cardinality();
                });
    }

    /**
    * Returns true if the bitmap is empty (cardinality is zero).
    */
    bool isEmpty() const {
        return std::all_of(roarings.cbegin(), roarings.cend(),
                           [](const std::pair<const uint32_t, Roaring>& map_entry) {
                               return map_entry.second.isEmpty();
                           });
    }

    /**
    * Returns true if the bitmap is full (cardinality is max uint64_t + 1).
    */
    bool isFull() const {
        // only bother to check if map is fully saturated
        //
        // we put std::numeric_limits<>::max/min in parenthesis
        // to avoid a clash with the Windows.h header under Windows
        return roarings.size() == ((size_t)(std::numeric_limits<uint32_t>::max)()) + 1
                       ? std::all_of(roarings.cbegin(), roarings.cend(),
                                     [](const std::pair<const uint32_t, Roaring>& roaring_map_entry) {
                                         // roarings within map are saturated if cardinality
                                         // is uint32_t max + 1
                                         return roaring_map_entry.second.cardinality() ==
                                                ((uint64_t)(std::numeric_limits<uint32_t>::max)()) +
                                                        1;
                                     })
                       : false;
    }

    /**
    * Returns true if the bitmap is subset of the other.
    */
    bool isSubset(const Roaring64Map& r) const {
        for (const auto& map_entry : roarings) {
            auto roaring_iter = r.roarings.find(map_entry.first);
            if (roaring_iter == roarings.cend())
                return false;
            else if (!map_entry.second.isSubset(roaring_iter->second))
                return false;
        }
        return true;
    }

    /**
    * Returns true if the bitmap is strict subset of the other.
    * Throws std::length_error in the special case where the bitmap is full
    * (cardinality() == 2^64). Check isFull() before calling to avoid exception.
    */
    bool isStrictSubset(const Roaring64Map& r) const {
        return isSubset(r) && cardinality() != r.cardinality();
    }

    /**
     * Convert the bitmap to an array. Write the output to "ans",
     * caller is responsible to ensure that there is enough memory
     * allocated
     * (e.g., ans = new uint32[mybitmap.cardinality()];)
     */
    void toUint64Array(uint64_t* ans) const {
        // Annoyingly, VS 2017 marks std::accumulate() as [[nodiscard]]
        (void)std::accumulate(
                roarings.cbegin(), roarings.cend(), ans,
                [](uint64_t* previous, const std::pair<const uint32_t, Roaring>& map_entry) {
                    for (uint32_t low_bits : map_entry.second)
                        *previous++ = uniteBytes(map_entry.first, low_bits);
                    return previous;
                });
    }

    /**
     * Return true if the two bitmaps contain the same elements.
     */
    bool operator==(const Roaring64Map& r) const {
        // we cannot use operator == on the map because either side may contain
        // empty Roaring Bitmaps
        auto lhs_iter = roarings.cbegin();
        auto rhs_iter = r.roarings.cbegin();
        do {
            // if the left map has reached its end, ensure that the right map
            // contains only empty Bitmaps
            if (lhs_iter == roarings.cend()) {
                while (rhs_iter != r.roarings.cend()) {
                    if (rhs_iter->second.isEmpty()) {
                        ++rhs_iter;
                        continue;
                    }
                    return false;
                }
                return true;
            }
            // if the left map has an empty bitmap, skip it
            if (lhs_iter->second.isEmpty()) {
                ++lhs_iter;
                continue;
            }

            do {
                // if the right map has reached its end, ensure that the right
                // map contains only empty Bitmaps
                if (rhs_iter == r.roarings.cend()) {
                    while (lhs_iter != roarings.cend()) {
                        if (lhs_iter->second.isEmpty()) {
                            ++lhs_iter;
                            continue;
                        }
                        return false;
                    }
                    return true;
                }
                // if the right map has an empty bitmap, skip it
                if (rhs_iter->second.isEmpty()) {
                    ++rhs_iter;
                    continue;
                }
            } while (false);
            // if neither map has reached its end ensure elements are equal and
            // move to the next element in both
        } while (lhs_iter++->second == rhs_iter++->second);
        return false;
    }

    /**
     * compute the negation of the roaring bitmap within a specified interval.
     * areas outside the range are passed through unchanged.
     */
    void flip(uint64_t range_start, uint64_t range_end) {
        uint32_t start_high = highBytes(range_start);
        uint32_t start_low = lowBytes(range_start);
        uint32_t end_high = highBytes(range_end);
        uint32_t end_low = lowBytes(range_end);

        if (start_high == end_high) {
            roarings[start_high].flip(start_low, end_low);
            return;
        }
        // we put std::numeric_limits<>::max/min in parenthesis
        // to avoid a clash with the Windows.h header under Windows
        roarings[start_high].flip(start_low, (std::numeric_limits<uint32_t>::max)());
        roarings[start_high++].setCopyOnWrite(copyOnWrite);

        for (; start_high <= highBytes(range_end) - 1; ++start_high) {
            roarings[start_high].flip((std::numeric_limits<uint32_t>::min)(),
                                      (std::numeric_limits<uint32_t>::max)());
            roarings[start_high].setCopyOnWrite(copyOnWrite);
        }

        roarings[start_high].flip((std::numeric_limits<uint32_t>::min)(), end_low);
        roarings[start_high].setCopyOnWrite(copyOnWrite);
    }

    /**
     *  Remove run-length encoding even when it is more space efficient
     *  return whether a change was applied
     */
    bool removeRunCompression() {
        return std::accumulate(roarings.begin(), roarings.end(), false,
                               [](bool previous, std::pair<const uint32_t, Roaring>& map_entry) {
                                   return map_entry.second.removeRunCompression() && previous;
                               });
    }

    /** convert array and bitmap containers to run containers when it is more
     * efficient;
     * also convert from run containers when more space efficient.  Returns
     * true if the result has at least one run container.
     * Additional savings might be possible by calling shrinkToFit().
     */
    bool runOptimize() {
        return std::accumulate(roarings.begin(), roarings.end(), false,
                               [](bool previous, std::pair<const uint32_t, Roaring>& map_entry) {
                                   return map_entry.second.runOptimize() && previous;
                               });
    }

    /**
     * If needed, reallocate memory to shrink the memory usage. Returns
     * the number of bytes saved.
    */
    size_t shrinkToFit() {
        size_t savedBytes = 0;
        auto iter = roarings.begin();
        while (iter != roarings.cend()) {
            if (iter->second.isEmpty()) {
                // empty Roarings are 84 bytes
                savedBytes += 88;
                roarings.erase(iter++);
            } else {
                savedBytes += iter->second.shrinkToFit();
                iter++;
            }
        }
        return savedBytes;
    }

    /**
     * Iterate over the bitmap elements. The function iterator is called once
     * for all the values with ptr (can be NULL) as the second parameter of each
     * call.
     *
     * roaring_iterator is simply a pointer to a function that returns bool
     * (true means that the iteration should continue while false means that it
     * should stop), and takes (uint32_t,void*) as inputs.
     */
    void iterate(roaring_iterator64 iterator, void* ptr) const {
        std::for_each(roarings.begin(), roarings.cend(),
                      [=](const std::pair<const uint32_t, Roaring>& map_entry) {
                          roaring_iterate64(&map_entry.second.roaring, iterator,
                                            uint64_t(map_entry.first) << 32, ptr);
                      });
    }

    /**
     * If the size of the roaring bitmap is strictly greater than rank, then
     this
       function returns true and set element to the element of given rank.
       Otherwise, it returns false.
     */
    bool select(uint64_t rnk, uint64_t* element) const {
        for (const auto& map_entry : roarings) {
            uint64_t sub_cardinality = (uint64_t)map_entry.second.cardinality();
            if (rnk < sub_cardinality) {
                *element = ((uint64_t)map_entry.first) << 32;
                // assuming little endian
                return map_entry.second.select((uint32_t)rnk, ((uint32_t*)element));
            }
            rnk -= sub_cardinality;
        }
        return false;
    }

    /**
    * Returns the number of integers that are smaller or equal to x.
    */
    uint64_t rank(uint64_t x) const {
        uint64_t result = 0;
        auto roaring_destination = roarings.find(highBytes(x));
        if (roaring_destination != roarings.cend()) {
            for (auto roaring_iter = roarings.cbegin(); roaring_iter != roaring_destination;
                 ++roaring_iter) {
                result += roaring_iter->second.cardinality();
            }
            result += roaring_destination->second.rank(lowBytes(x));
            return result;
        }
        roaring_destination = roarings.lower_bound(highBytes(x));
        for (auto roaring_iter = roarings.cbegin(); roaring_iter != roaring_destination;
             ++roaring_iter) {
            result += roaring_iter->second.cardinality();
        }
        return result;
    }

    /**
     * write a bitmap to a char buffer.
     * Returns how many bytes were written which should be getSizeInBytes().
     */
    size_t write(char* buf) const {
        if (is32BitsEnough()) {
            *(buf++) = BitmapTypeCode::type::BITMAP32;
            auto it = roarings.find(0);
            if (it == roarings.end()) { // empty bitmap
                Roaring r;
                return r.write(buf) + 1;
            }
            return it->second.write(buf) + 1;
        }

        const char* orig = buf;
        // put type code
        *(buf++) = BitmapTypeCode::type::BITMAP64;
        // push map size
        buf = (char*)encode_varint64((uint8_t*)buf, roarings.size());
        std::for_each(roarings.cbegin(), roarings.cend(),
                      [&buf](const std::pair<const uint32_t, Roaring>& map_entry) {
                          // push map key
                          encode_fixed32_le((uint8_t*)buf, map_entry.first);
                          buf += sizeof(uint32_t);
                          // push map value Roaring
                          buf += map_entry.second.write(buf);
                      });
        return buf - orig;
    }

    /**
     * read a bitmap from a serialized version.
     *
     * This function is unsafe in the sense that if you provide bad data,
     * many bytes could be read, possibly causing a buffer overflow. See also readSafe.
     */
    static Roaring64Map read(const char* buf) {
        Roaring64Map result;

        if (*buf == BitmapTypeCode::BITMAP32) {
            Roaring read = Roaring::read(buf + 1);
            result.emplaceOrInsert(0, read);
            return result;
        }

        DCHECK_EQ(BitmapTypeCode::BITMAP64, *buf);
        buf++;

        // get map size (varint64 took 1~10 bytes)
        uint64_t map_size;
        buf = reinterpret_cast<const char*>(
                decode_varint64_ptr(reinterpret_cast<const uint8_t*>(buf),
                                    reinterpret_cast<const uint8_t*>(buf + 10), &map_size));
        DCHECK(buf != nullptr);
        for (uint64_t lcv = 0; lcv < map_size; lcv++) {
            // get map key
            uint32_t key = decode_fixed32_le(reinterpret_cast<const uint8_t*>(buf));
            buf += sizeof(uint32_t);
            // read map value Roaring
            Roaring read = Roaring::read(buf);
            result.emplaceOrInsert(key, read);
            // forward buffer past the last Roaring Bitmap
            buf += read.getSizeInBytes();
        }
        return result;
    }

    /**
     * How many bytes are required to serialize this bitmap
     */
    size_t getSizeInBytes() const {
        if (is32BitsEnough()) {
            auto it = roarings.find(0);
            if (it == roarings.end()) { // empty bitmap
                Roaring r;
                return r.getSizeInBytes() + 1;
            }
            return it->second.getSizeInBytes() + 1;
        }
        // start with type code, map size and size of keys for each map entry
        size_t init = 1 + varint_length(roarings.size()) + roarings.size() * sizeof(uint32_t);
        return std::accumulate(roarings.cbegin(), roarings.cend(), init,
                               [=](size_t previous, const std::pair<const uint32_t, Roaring>& map_entry) {
                                   // add in bytes used by each Roaring
                                   return previous + map_entry.second.getSizeInBytes();
                               });
    }

    /**
     * remove all elements
     */
    void clear() { roarings.clear(); }

    /**
     * Return whether all elements can be represented in 32 bits
     */
    bool is32BitsEnough() const { return maximum() <= std::numeric_limits<uint32_t>::max(); }

    /**
     * Computes the intersection between two bitmaps and returns new bitmap.
     * The current bitmap and the provided bitmap are unchanged.
     */
    Roaring64Map operator&(const Roaring64Map& o) const { return Roaring64Map(*this) &= o; }

    /**
     * Computes the difference between two bitmaps and returns new bitmap.
     * The current bitmap and the provided bitmap are unchanged.
     */
    Roaring64Map operator-(const Roaring64Map& o) const { return Roaring64Map(*this) -= o; }

    /**
     * Computes the union between two bitmaps and returns new bitmap.
     * The current bitmap and the provided bitmap are unchanged.
     */
    Roaring64Map operator|(const Roaring64Map& o) const { return Roaring64Map(*this) |= o; }

    /**
     * Computes the symmetric union between two bitmaps and returns new bitmap.
     * The current bitmap and the provided bitmap are unchanged.
     */
    Roaring64Map operator^(const Roaring64Map& o) const { return Roaring64Map(*this) ^= o; }

    /**
     * Whether or not we apply copy and write.
     */
    void setCopyOnWrite(bool val) {
        if (copyOnWrite == val) return;
        copyOnWrite = val;
        std::for_each(roarings.begin(), roarings.end(),
                      [=](std::pair<const uint32_t, Roaring>& map_entry) {
                          map_entry.second.setCopyOnWrite(val);
                      });
    }

    /**
     * Print the content of the bitmap
     */
    void printf() const {
        if (!isEmpty()) {
            auto map_iter = roarings.cbegin();
            while (map_iter->second.isEmpty()) ++map_iter;
            struct iter_data {
                uint32_t high_bits;
                char first_char = '{';
            } outer_iter_data;
            outer_iter_data.high_bits = roarings.begin()->first;
            map_iter->second.iterate(
                    [](uint32_t low_bits, void* inner_iter_data) -> bool {
                        std::printf("%c%llu", ((iter_data*)inner_iter_data)->first_char,
                                    (long long unsigned)uniteBytes(
                                            ((iter_data*)inner_iter_data)->high_bits, low_bits));
                        ((iter_data*)inner_iter_data)->first_char = ',';
                        return true;
                    },
                    (void*)&outer_iter_data);
            std::for_each(
                    ++map_iter, roarings.cend(), [](const std::pair<const uint32_t, Roaring>& map_entry) {
                        map_entry.second.iterate(
                                [](uint32_t low_bits, void* high_bits) -> bool {
                                    std::printf(",%llu", (long long unsigned)uniteBytes(
                                                                 *(uint32_t*)high_bits, low_bits));
                                    return true;
                                },
                                (void*)&map_entry.first);
                    });
        } else
            std::printf("{");
        std::printf("}\n");
    }

    /**
     * Print the content of the bitmap into a string
     */
    std::string toString() const {
        struct iter_data {
            std::string str;
            uint32_t high_bits;
            char first_char = '{';
        } outer_iter_data;
        if (!isEmpty()) {
            auto map_iter = roarings.cbegin();
            while (map_iter->second.isEmpty()) ++map_iter;
            outer_iter_data.high_bits = roarings.begin()->first;
            map_iter->second.iterate(
                    [](uint32_t low_bits, void* inner_iter_data) -> bool {
                        ((iter_data*)inner_iter_data)->str +=
                                ((iter_data*)inner_iter_data)->first_char;
                        ((iter_data*)inner_iter_data)->str += std::to_string(
                                uniteBytes(((iter_data*)inner_iter_data)->high_bits, low_bits));
                        ((iter_data*)inner_iter_data)->first_char = ',';
                        return true;
                    },
                    (void*)&outer_iter_data);
            std::for_each(
                    ++map_iter, roarings.cend(),
                    [&outer_iter_data](const std::pair<const uint32_t, Roaring>& map_entry) {
                        outer_iter_data.high_bits = map_entry.first;
                        map_entry.second.iterate(
                                [](uint32_t low_bits, void* inner_iter_data) -> bool {
                                    ((iter_data*)inner_iter_data)->str +=
                                            ((iter_data*)inner_iter_data)->first_char;
                                    ((iter_data*)inner_iter_data)->str += std::to_string(uniteBytes(
                                            ((iter_data*)inner_iter_data)->high_bits, low_bits));
                                    return true;
                                },
                                (void*)&outer_iter_data);
                    });
        } else
            outer_iter_data.str = '{';
        outer_iter_data.str += '}';
        return outer_iter_data.str;
    }

    /**
     * Whether or not copy and write is active.
     */
    bool getCopyOnWrite() const { return copyOnWrite; }

    /**
     * computes the logical or (union) between "n" bitmaps (referenced by a
     * pointer).
     */
    static Roaring64Map fastunion(size_t n, const Roaring64Map** inputs) {
        Roaring64Map ans;
        // not particularly fast
        for (size_t lcv = 0; lcv < n; ++lcv) {
            ans |= *(inputs[lcv]);
        }
        return ans;
    }

    friend class Roaring64MapSetBitForwardIterator;
    typedef Roaring64MapSetBitForwardIterator const_iterator;

    /**
    * Returns an iterator that can be used to access the position of the
    * set bits. The running time complexity of a full scan is proportional to
    * the
    * number
    * of set bits: be aware that if you have long strings of 1s, this can be
    * very inefficient.
    *
    * It can be much faster to use the toArray method if you want to
    * retrieve the set bits.
    */
    const_iterator begin() const;

    /**
    * A bogus iterator that can be used together with begin()
    * for constructions such as for(auto i = b.begin();
    * i!=b.end(); ++i) {}
    */
    const_iterator end() const;

private:
    std::map<uint32_t, Roaring> roarings;
    bool copyOnWrite = false;
    static uint32_t highBytes(const uint64_t in) { return uint32_t(in >> 32); }
    static uint32_t lowBytes(const uint64_t in) { return uint32_t(in); }
    static uint64_t uniteBytes(const uint32_t highBytes, const uint32_t lowBytes) {
        return (uint64_t(highBytes) << 32) | uint64_t(lowBytes);
    }
    // this is needed to tolerate gcc's C++11 libstdc++ lacking emplace
    // prior to version 4.8
    void emplaceOrInsert(const uint32_t key, const Roaring& value) {
#if defined(__GLIBCXX__) && __GLIBCXX__ < 20130322
        roarings.insert(std::make_pair(key, value));
#else
        roarings.emplace(std::make_pair(key, value));
#endif
    }
};

// Forked from https://github.com/RoaringBitmap/CRoaring/blob/v0.2.60/cpp/roaring64map.hh
// Used to go through the set bits. Not optimally fast, but convenient.
class Roaring64MapSetBitForwardIterator final {
public:
    typedef std::forward_iterator_tag iterator_category;
    typedef uint64_t* pointer;
    typedef uint64_t& reference_type;
    typedef uint64_t value_type;
    typedef int64_t difference_type;
    typedef Roaring64MapSetBitForwardIterator type_of_iterator;

    /**
     * Provides the location of the set bit.
     */
    value_type operator*() const {
        return Roaring64Map::uniteBytes(map_iter->first, i.current_value);
    }

    bool operator<(const type_of_iterator& o) {
        if (map_iter == map_end) return false;
        if (o.map_iter == o.map_end) return true;
        return **this < *o;
    }

    bool operator<=(const type_of_iterator& o) {
        if (o.map_iter == o.map_end) return true;
        if (map_iter == map_end) return false;
        return **this <= *o;
    }

    bool operator>(const type_of_iterator& o) {
        if (o.map_iter == o.map_end) return false;
        if (map_iter == map_end) return true;
        return **this > *o;
    }

    bool operator>=(const type_of_iterator& o) {
        if (map_iter == map_end) return true;
        if (o.map_iter == o.map_end) return false;
        return **this >= *o;
    }

    type_of_iterator& operator++() { // ++i, must returned inc. value
        if (i.has_value == true) roaring_advance_uint32_iterator(&i);
        while (!i.has_value) {
            map_iter++;
            if (map_iter == map_end) return *this;
            roaring_init_iterator(&map_iter->second.roaring, &i);
        }
        return *this;
    }

    type_of_iterator operator++(int) { // i++, must return orig. value
        Roaring64MapSetBitForwardIterator orig(*this);
        roaring_advance_uint32_iterator(&i);
        while (!i.has_value) {
            map_iter++;
            if (map_iter == map_end) return orig;
            roaring_init_iterator(&map_iter->second.roaring, &i);
        }
        return orig;
    }

    bool operator==(const Roaring64MapSetBitForwardIterator& o) {
        if (map_iter == map_end && o.map_iter == o.map_end) return true;
        if (o.map_iter == o.map_end) return false;
        return **this == *o;
    }

    bool operator!=(const Roaring64MapSetBitForwardIterator& o) {
        if (map_iter == map_end && o.map_iter == o.map_end) return false;
        if (o.map_iter == o.map_end) return true;
        return **this != *o;
    }

    Roaring64MapSetBitForwardIterator(const Roaring64Map& parent, bool exhausted = false)
            : map_end(parent.roarings.cend()) {
        if (exhausted || parent.roarings.empty()) {
            map_iter = parent.roarings.cend();
        } else {
            map_iter = parent.roarings.cbegin();
            roaring_init_iterator(&map_iter->second.roaring, &i);
            while (!i.has_value) {
                map_iter++;
                if (map_iter == map_end) return;
                roaring_init_iterator(&map_iter->second.roaring, &i);
            }
        }
    }

private:
    std::map<uint32_t, Roaring>::const_iterator map_iter;
    std::map<uint32_t, Roaring>::const_iterator map_end;
    roaring_uint32_iterator_t i;
};

inline Roaring64MapSetBitForwardIterator Roaring64Map::begin() const {
    return Roaring64MapSetBitForwardIterator(*this);
}

inline Roaring64MapSetBitForwardIterator Roaring64Map::end() const {
    return Roaring64MapSetBitForwardIterator(*this, true);
}

} // namespace detail

// Represent the in-memory and on-disk structure of Doris's BITMAP data type.
// Optimize for the case where the bitmap contains 0 or 1 element which is common
// for streaming load scenario.
class BitmapValue {
public:
    // Construct an empty bitmap.
    BitmapValue() : _type(EMPTY) {}

    // Construct a bitmap with one element.
    explicit BitmapValue(uint64_t value) : _sv(value), _type(SINGLE) {}

    // Construct a bitmap from serialized data.
    explicit BitmapValue(const char* src) {
        bool res = deserialize(src);
        DCHECK(res);
    }

    // Construct a bitmap from given elements.
    explicit BitmapValue(const std::vector<uint64_t>& bits) {
        switch (bits.size()) {
        case 0:
            _type = EMPTY;
            break;
        case 1:
            _type = SINGLE;
            _sv = bits[0];
            break;
        default:
            _type = BITMAP;
            _bitmap.addMany(bits.size(), &bits[0]);
        }
    }

    void add(uint64_t value) {
        switch (_type) {
        case EMPTY:
            _sv = value;
            _type = SINGLE;
            break;
        case SINGLE:
            //there is no need to convert the type if two variables are equal
            if (_sv == value) {
                break;
            }
            _bitmap.add(_sv);
            _bitmap.add(value);
            _type = BITMAP;
            break;
        case BITMAP:
            _bitmap.add(value);
        }
    }

    void remove(uint64_t value) {
        switch (_type) {
            case EMPTY:
                break;
            case SINGLE:
                //there is need to convert the type if two variables are equal
                if (_sv == value) {
                    _type = EMPTY;
                }
                break;
            case BITMAP:
                _bitmap.remove(value);
                _convert_to_smaller_type();
        }
    }

    // Compute the union between the current bitmap and the provided bitmap.
    BitmapValue& operator-=(const BitmapValue& rhs) {
        switch (rhs._type) {
            case EMPTY:
                break;
            case SINGLE:
                remove(rhs._sv);
                break;
            case BITMAP:
                switch (_type) {
                    case EMPTY:
                        break;
                    case SINGLE:
                        if (rhs._bitmap.contains(_sv)) {
                            _type = EMPTY;
                        }
                        break;
                    case BITMAP:
                        _bitmap -= rhs._bitmap;
                        _convert_to_smaller_type();
                        break;
                }
                break;
        }
        return *this;
    }

    // Compute the union between the current bitmap and the provided bitmap.
    // Possible type transitions are:
    // EMPTY  -> SINGLE
    // EMPTY  -> BITMAP
    // SINGLE -> BITMAP
    BitmapValue& operator|=(const BitmapValue& rhs) {
        switch (rhs._type) {
        case EMPTY:
            break;
        case SINGLE:
            add(rhs._sv);
            break;
        case BITMAP:
            switch (_type) {
            case EMPTY:
                _bitmap = rhs._bitmap;
                _type = BITMAP;
                break;
            case SINGLE:
                _bitmap = rhs._bitmap;
                _bitmap.add(_sv);
                _type = BITMAP;
                break;
            case BITMAP:
                _bitmap |= rhs._bitmap;
            }
            break;
        }
        return *this;
    }

    // Compute the intersection between the current bitmap and the provided bitmap.
    // Possible type transitions are:
    // SINGLE -> EMPTY
    // BITMAP -> EMPTY
    // BITMAP -> SINGLE
    BitmapValue& operator&=(const BitmapValue& rhs) {
        switch (rhs._type) {
        case EMPTY:
            _type = EMPTY;
            _bitmap.clear();
            break;
        case SINGLE:
            switch (_type) {
            case EMPTY:
                break;
            case SINGLE:
                if (_sv != rhs._sv) {
                    _type = EMPTY;
                }
                break;
            case BITMAP:
                if (!_bitmap.contains(rhs._sv)) {
                    _type = EMPTY;
                } else {
                    _type = SINGLE;
                    _sv = rhs._sv;
                }
                _bitmap.clear();
                break;
            }
            break;
        case BITMAP:
            switch (_type) {
            case EMPTY:
                break;
            case SINGLE:
                if (!rhs._bitmap.contains(_sv)) {
                    _type = EMPTY;
                }
                break;
            case BITMAP:
                _bitmap &= rhs._bitmap;
                _convert_to_smaller_type();
                break;
            }
            break;
        }
        return *this;
    }

    // Compute the symmetric union between the current bitmap and the provided bitmap.
    // Possible type transitions are:
    // SINGLE -> EMPTY
    // BITMAP -> EMPTY
    // BITMAP -> SINGLE
    BitmapValue& operator^=(const BitmapValue& rhs) {
        switch (rhs._type) {
            case EMPTY:
                break;
            case SINGLE:
                switch (_type) {
                    case EMPTY:
                        add(rhs._sv);
                        break;
                    case SINGLE:
                        if (_sv == rhs._sv) {
                            _type = EMPTY;
                            _bitmap.clear();
                        } else {
                            add(rhs._sv);
                        }
                        break;
                    case BITMAP:
                        if (!_bitmap.contains(rhs._sv)) {
                            add(rhs._sv);
                        } else {
                            _bitmap.remove(rhs._sv);
                        }
                        break;
                }
                break;
            case BITMAP:
                switch (_type) {
                    case EMPTY:
                        _bitmap = rhs._bitmap;
                        _type = BITMAP;
                        break;
                    case SINGLE:
                        _bitmap = rhs._bitmap;
                        _type = BITMAP;
                        if (!rhs._bitmap.contains(_sv)) {
                            _bitmap.add(_sv);
                        } else {
                            _bitmap.remove(_sv);
                        }
                        break;
                    case BITMAP:
                        _bitmap ^= rhs._bitmap;
                        _convert_to_smaller_type();
                        break;
                }
                break;
        }
        return *this;
    }


    // check if value x is present
    bool contains(uint64_t x) const {
        switch (_type) {
        case EMPTY:
            return false;
        case SINGLE:
            return _sv == x;
        case BITMAP:
            return _bitmap.contains(x);
        }
        return false;
    }

    // TODO should the return type be uint64_t?
    int64_t cardinality() const {
        switch (_type) {
        case EMPTY:
            return 0;
        case SINGLE:
            return 1;
        case BITMAP:
            return _bitmap.cardinality();
        }
        return 0;
    }

    // Return how many bytes are required to serialize this bitmap.
    // See BitmapTypeCode for the serialized format.
    size_t getSizeInBytes() {
        size_t res = 0;
        switch (_type) {
        case EMPTY:
            res = 1;
            break;
        case SINGLE:
            if (_sv <= std::numeric_limits<uint32_t>::max()) {
                res = 1 + sizeof(uint32_t);
            } else {
                res = 1 + sizeof(uint64_t);
            }
            break;
        case BITMAP:
            DCHECK(_bitmap.cardinality() > 1);
            _bitmap.runOptimize();
            _bitmap.shrinkToFit();
            res = _bitmap.getSizeInBytes();
            break;
        }
        return res;
    }

    // Serialize the bitmap value to dst, which should be large enough.
    // Client should call `getSizeInBytes` first to get the serialized size.
    void write(char* dst) {
        switch (_type) {
        case EMPTY:
            *dst = BitmapTypeCode::EMPTY;
            break;
        case SINGLE:
            if (_sv <= std::numeric_limits<uint32_t>::max()) {
                *(dst++) = BitmapTypeCode::SINGLE32;
                encode_fixed32_le(reinterpret_cast<uint8_t*>(dst), static_cast<uint32_t>(_sv));
            } else {
                *(dst++) = BitmapTypeCode::SINGLE64;
                encode_fixed64_le(reinterpret_cast<uint8_t*>(dst), _sv);
            }
            break;
        case BITMAP:
            _bitmap.write(dst);
            break;
        }
    }

    // Deserialize a bitmap value from `src`.
    // Return false if `src` begins with unknown type code, true otherwise.
    bool deserialize(const char* src) {
        DCHECK(*src >= BitmapTypeCode::EMPTY && *src <= BitmapTypeCode::BITMAP64);
        switch (*src) {
        case BitmapTypeCode::EMPTY:
            _type = EMPTY;
            break;
        case BitmapTypeCode::SINGLE32:
            _type = SINGLE;
            _sv = decode_fixed32_le(reinterpret_cast<const uint8_t*>(src + 1));
            break;
        case BitmapTypeCode::SINGLE64:
            _type = SINGLE;
            _sv = decode_fixed64_le(reinterpret_cast<const uint8_t*>(src + 1));
            break;
        case BitmapTypeCode::BITMAP32:
        case BitmapTypeCode::BITMAP64:
            _type = BITMAP;
            _bitmap = detail::Roaring64Map::read(src);
            break;
        default:
            return false;
        }
        return true;
    }

    doris_udf::BigIntVal minimum() {
        switch (_type) {
        case SINGLE:
            return doris_udf::BigIntVal(_sv);
        case BITMAP:
            return doris_udf::BigIntVal(_bitmap.minimum());
        default:
            return doris_udf::BigIntVal::null();
        }
    }

    // TODO limit string size to avoid OOM
    std::string to_string() const {
        std::stringstream ss;
        switch (_type) {
        case EMPTY:
            break;
        case SINGLE:
            ss << _sv;
            break;
        case BITMAP: {
            struct IterCtx {
                std::stringstream* ss = nullptr;
                bool first = true;
            } iter_ctx;
            iter_ctx.ss = &ss;

            _bitmap.iterate(
                    [](uint64_t value, void* c) -> bool {
                        auto ctx = reinterpret_cast<IterCtx*>(c);
                        if (ctx->first) {
                            ctx->first = false;
                        } else {
                            (*ctx->ss) << ",";
                        }
                        (*ctx->ss) << value;
                        return true;
                    },
                    &iter_ctx);
            break;
        }
        }
        return ss.str();
    }

private:
    void _convert_to_smaller_type() {
        if (_type == BITMAP) {
            uint64_t c = _bitmap.cardinality();
            if (c > 1) return;
            if (c == 0) {
                _type = EMPTY;
            } else {
                _type = SINGLE;
                _sv = _bitmap.minimum();
            }
            _bitmap.clear();
        }
    }

    enum BitmapDataType {
        EMPTY = 0,
        SINGLE = 1, // single element
        BITMAP = 2  // more than one elements
    };
    uint64_t _sv = 0;             // store the single value when _type == SINGLE
    detail::Roaring64Map _bitmap; // used when _type == BITMAP
    BitmapDataType _type;
};

} // namespace doris

#endif //DORIS_BE_SRC_UTIL_BITMAP_VALUE_H
