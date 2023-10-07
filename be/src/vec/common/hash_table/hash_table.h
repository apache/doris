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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/HashTable/HashTable.h
// and modified by Doris

#pragma once

#include <math.h>
#include <string.h>

#include <boost/noncopyable.hpp>
#include <utility>

#include "common/exception.h"
#include "common/status.h"
#include "util/runtime_profile.h"
#include "vec/common/hash_table/hash_table_allocator.h"
#include "vec/common/hash_table/hash_table_key_holder.h"
#include "vec/core/types.h"
#include "vec/io/io_helper.h"

/** NOTE HashTable could only be used for memmoveable (position independent) types.
  * Example: std::string is not position independent in libstdc++ with C++11 ABI or in libc++.
  * Also, key in hash table must be of type, that zero bytes is compared equals to zero key.
  */

/** The state of the hash table that affects the properties of its cells.
  * Used as a template parameter.
  * For example, there is an implementation of an instantly clearable hash table - ClearableHashMap.
  * For it, each cell holds the version number, and in the hash table itself is the current version.
  *  When clearing, the current version simply increases; All cells with a mismatching version are considered empty.
  *  Another example: for an approximate calculation of the number of unique visitors, there is a hash table for UniquesHashSet.
  *  It has the concept of "degree". At each overflow, cells with keys that do not divide by the corresponding power of the two are deleted.
  */
struct HashTableNoState {
    /// Serialization, in binary and text form.
    void write(doris::vectorized::BufferWritable&) const {}

    // /// Deserialization, in binary and text form.
    void read(doris::vectorized::BufferReadable&) {}
};

/// These functions can be overloaded for custom types.
namespace ZeroTraits {

template <typename T>
bool check(const T x) {
    return x == 0;
}

template <typename T>
void set(T& x) {
    x = 0;
}

} // namespace ZeroTraits

/**
  * lookup_result_get_key/Mapped -- functions to get key/"mapped" values from the
  * LookupResult returned by find() and emplace() methods of HashTable.
  * Must not be called for a null LookupResult.
  *
  * We don't use iterators for lookup result to avoid creating temporary
  * objects. Instead, LookupResult is a pointer of some kind. There are global
  * functions lookup_result_get_key/Mapped, overloaded for this pointer type, that
  * return pointers to key/"mapped" values. They are implemented as global
  * functions and not as methods, because they have to be overloaded for POD
  * types, e.g. in StringHashTable where different components have different
  * Cell format.
  *
  * Different hash table implementations support this interface to a varying
  * degree:
  *
  * 1) Hash tables that store neither the key in its original form, nor a
  *    "mapped" value: FixedHashTable or StringHashTable.
  *    Neither GetKey nor GetMapped are supported, the only valid operation is
  *    checking LookupResult for null.
  *
  * 2) Hash maps that do not store the key, e.g. FixedHashMap or StringHashMap.
  *    Only GetMapped is supported.
  *
  * 3) Hash tables that store the key and do not have a "mapped" value, e.g. the
  *    normal HashTable.
  *    GetKey returns the key, and GetMapped returns a zero void pointer. This
  *    simplifies generic code that works with mapped values: it can overload
  *    on the return type of GetMapped(), and doesn't need other parameters. One
  *    example is insert_set_mapped() function.
  *
  * 4) Hash tables that store both the key and the "mapped" value, e.g. HashMap.
  *    Both GetKey and GetMapped are supported.
  *
  * The implementation side goes as follows:
  * for (1), LookupResult = void *, no getters;
  * for (2), LookupResult = Mapped *, GetMapped is a default implementation that
  * takes any pointer-like object;
  * for (3) and (4), LookupResult = Cell *, and both getters are implemented.
  * They have to be specialized for each particular Cell class to supersede the
  * default version that takes a generic pointer-like object.
  */
struct VoidKey {};
struct VoidMapped {
    template <typename T>
    auto& operator=(const T&) {
        return *this;
    }
};

/**
  * The default implementation of GetMapped that is used for the above case (2).
  */
template <typename PointerLike>
ALWAYS_INLINE inline auto lookup_result_get_mapped(PointerLike&& ptr) {
    return &*ptr;
}

/**
  * Generic const wrapper for lookup_result_get_mapped, that calls a non-const
  * version. Should be safe, given that these functions only do pointer
  * arithmetics.
  */
template <typename T>
ALWAYS_INLINE inline auto lookup_result_get_mapped(const T* obj) {
    auto mapped_ptr = lookup_result_get_mapped(const_cast<T*>(obj));
    const auto const_mapped_ptr = mapped_ptr;
    return const_mapped_ptr;
}

/** Compile-time interface for cell of the hash table.
  * Different cell types are used to implement different hash tables.
  * The cell must contain a key.
  * It can also contain a value and arbitrary additional data
  *  (example: the stored hash value; version number for ClearableHashMap).
  */
template <typename Key, typename Hash, typename TState = HashTableNoState>
struct HashTableCell {
    using State = TState;

    using key_type = Key;
    using value_type = Key;
    using mapped_type = void;

    Key key;

    HashTableCell() = default;

    /// Create a cell with the given key / key and value.
    HashTableCell(const Key& key_, const State&) : key(key_) {}

    /// Get what the value_type of the container will be.
    const value_type& get_value() const { return key; }

    /// Get the key.
    static const Key& get_key(const value_type& value) { return value; }

    /// Are the keys at the cells equal?
    bool key_equals(const Key& key_) const { return key == key_; }
    bool key_equals(const Key& key_, size_t /*hash_*/) const { return key == key_; }
    bool key_equals(const Key& key_, size_t /*hash_*/, const State& /*state*/) const {
        return key == key_;
    }

    /// If the cell can remember the value of the hash function, then remember it.
    void set_hash(size_t /*hash_value*/) {}

    /// If the cell can store the hash value in itself, then return the stored value.
    /// It must be at least once calculated before.
    /// If storing the hash value is not provided, then just compute the hash.
    size_t get_hash(const Hash& hash) const { return hash(key); }

    /// Whether the key is zero. In the main buffer, cells with a zero key are considered empty.
    /// If zero keys can be inserted into the table, then the cell for the zero key is stored separately, not in the main buffer.
    /// Zero keys must be such that the zeroed-down piece of memory is a zero key.
    bool is_zero(const State& state) const { return is_zero(key, state); }
    static bool is_zero(const Key& key, const State& /*state*/) { return ZeroTraits::check(key); }

    /// Set the key value to zero.
    void set_zero() { ZeroTraits::set(key); }

    /// Do the hash table need to store the zero key separately (that is, can a zero key be inserted into the hash table).
    static constexpr bool need_zero_value_storage = true;

    /// Set the mapped value, if any (for HashMap), to the corresponding `value`.
    void set_mapped(const value_type& /*value*/) {}

    /// Serialization, in binary and text form.
    void write(doris::vectorized::BufferWritable& wb) const {
        doris::vectorized::write_binary(key, wb);
    }

    /// Deserialization, in binary and text form.
    void read(doris::vectorized::BufferReadable& rb) { doris::vectorized::read_binary(key, rb); }
};

template <typename Key, typename Hash, typename State>
ALWAYS_INLINE inline auto lookup_result_get_key(HashTableCell<Key, Hash, State>* cell) {
    return &cell->key;
}

template <typename Key, typename Hash, typename State>
ALWAYS_INLINE inline void* lookup_result_get_mapped(HashTableCell<Key, Hash, State>*) {
    return nullptr;
}

/**
  * A helper function for HashTable::insert() to set the "mapped" value.
  * Overloaded on the mapped type, does nothing if it's void.
  */
template <typename ValueType>
void insert_set_mapped(void* /* dest */, const ValueType& /* src */) {}

template <typename MappedType, typename ValueType>
void insert_set_mapped(MappedType* dest, const ValueType& src) {
    *dest = src.second;
}

static doris::vectorized::Int32 double_resize_threshold = doris::config::double_resize_threshold;

/** Determines the size of the hash table, and when and how much it should be resized.
  */
template <size_t initial_size_degree = 10>
struct HashTableGrower {
    /// The state of this structure is enough to get the buffer size of the hash table.
    doris::vectorized::UInt8 size_degree = initial_size_degree;
    doris::vectorized::Int64 double_grow_degree = doris::config::hash_table_double_grow_degree;

    doris::vectorized::Int32 max_fill_rate = doris::config::max_fill_rate;

    /// The size of the hash table in the cells.
    size_t buf_size() const { return 1ULL << size_degree; }

    // When capacity is greater than 2^double_grow_degree, grow when 75% of the capacity is satisfied.
    size_t max_fill() const {
        return size_degree < double_grow_degree
                       ? 1ULL << (size_degree - 1)
                       : (1ULL << size_degree) - (1ULL << (size_degree - max_fill_rate));
    }

    size_t mask() const { return buf_size() - 1; }

    /// From the hash value, get the cell number in the hash table.
    size_t place(size_t x) const { return x & mask(); }

    /// The next cell in the collision resolution chain.
    size_t next(size_t pos) const {
        ++pos;
        return pos & mask();
    }

    /// Whether the hash table is sufficiently full. You need to increase the size of the hash table, or remove something unnecessary from it.
    bool overflow(size_t elems) const { return elems > max_fill(); }

    /// Increase the size of the hash table.
    void increase_size() { size_degree += size_degree >= double_resize_threshold ? 1 : 2; }

    /// Set the buffer size by the number of elements in the hash table. Used when deserializing a hash table.
    void set(size_t num_elems) {
        size_t fill_capacity = static_cast<size_t>(log2(num_elems - 1)) + 1;
        fill_capacity =
                fill_capacity < double_grow_degree
                        ? fill_capacity + 1
                        : (num_elems < (1ULL << fill_capacity) - (1ULL << (fill_capacity - 2))
                                   ? fill_capacity
                                   : fill_capacity + 1);

        size_degree = num_elems <= 1 ? initial_size_degree
                                     : (initial_size_degree > fill_capacity ? initial_size_degree
                                                                            : fill_capacity);
    }

    void set_buf_size(size_t buf_size_) {
        size_degree = static_cast<size_t>(log2(buf_size_ - 1) + 1);
    }
};

/** Determines the size of the hash table, and when and how much it should be resized.
  * This structure is aligned to cache line boundary and also occupies it all.
  * Precalculates some values to speed up lookups and insertion into the HashTable (and thus has bigger memory footprint than HashTableGrower).
  */
template <size_t initial_size_degree = 8>
class alignas(64) HashTableGrowerWithPrecalculation {
    /// The state of this structure is enough to get the buffer size of the hash table.

    doris::vectorized::UInt8 size_degree_ = initial_size_degree;
    size_t precalculated_mask = (1ULL << initial_size_degree) - 1;
    size_t precalculated_max_fill = 1ULL << (initial_size_degree - 1);
    doris::vectorized::Int64 double_grow_degree = doris::config::hash_table_double_grow_degree;

public:
    doris::vectorized::UInt8 size_degree() const { return size_degree_; }

    void increase_size_degree(doris::vectorized::UInt8 delta) {
        size_degree_ += delta;
        DCHECK(size_degree_ <= 64);
        precalculated_mask = (1ULL << size_degree_) - 1;
        precalculated_max_fill = size_degree_ < double_grow_degree
                                         ? 1ULL << (size_degree_ - 1)
                                         : (1ULL << size_degree_) - (1ULL << (size_degree_ - 2));
    }

    static constexpr auto initial_count = 1ULL << initial_size_degree;

    /// If collision resolution chains are contiguous, we can implement erase operation by moving the elements.
    static constexpr auto performs_linear_probing_with_single_step = true;

    /// The size of the hash table in the cells.
    size_t buf_size() const { return 1ULL << size_degree_; }

    /// From the hash value, get the cell number in the hash table.
    size_t place(size_t x) const { return x & precalculated_mask; }

    /// The next cell in the collision resolution chain.
    size_t next(size_t pos) const { return (pos + 1) & precalculated_mask; }

    /// Whether the hash table is sufficiently full. You need to increase the size of the hash table, or remove something unnecessary from it.
    bool overflow(size_t elems) const { return elems > precalculated_max_fill; }

    /// Increase the size of the hash table.
    void increase_size() { increase_size_degree(size_degree_ >= double_resize_threshold ? 1 : 2); }

    /// Set the buffer size by the number of elements in the hash table. Used when deserializing a hash table.
    void set(size_t num_elems) {
        size_t fill_capacity = static_cast<size_t>(log2(num_elems - 1)) + 1;
        fill_capacity =
                fill_capacity < double_grow_degree
                        ? fill_capacity + 1
                        : (num_elems < (1ULL << fill_capacity) - (1ULL << (fill_capacity - 2))
                                   ? fill_capacity
                                   : fill_capacity + 1);

        size_degree_ = num_elems <= 1 ? initial_size_degree
                                      : (initial_size_degree > fill_capacity ? initial_size_degree
                                                                             : fill_capacity);
        increase_size_degree(0);
    }

    void set_buf_size(size_t buf_size_) {
        size_degree_ = static_cast<size_t>(log2(buf_size_ - 1) + 1);
        increase_size_degree(0);
    }
};

static_assert(sizeof(HashTableGrowerWithPrecalculation<>) == 64);

/** When used as a Grower, it turns a hash table into something like a lookup table.
  * It remains non-optimal - the cells store the keys.
  * Also, the compiler can not completely remove the code of passing through the collision resolution chain, although it is not needed.
  * TODO Make a proper lookup table.
  */
template <size_t key_bits>
struct HashTableFixedGrower {
    size_t buf_size() const { return 1ULL << key_bits; }
    size_t place(size_t x) const { return x; }
    /// You could write __builtin_unreachable(), but the compiler does not optimize everything, and it turns out less efficiently.
    size_t next(size_t pos) const { return pos + 1; }
    bool overflow(size_t /*elems*/) const { return false; }

    void increase_size() {
        LOG(FATAL) << "__builtin_unreachable";
        __builtin_unreachable();
    }
    void set(size_t /*num_elems*/) {}
    void set_buf_size(size_t /*buf_size_*/) {}
};

/** If you want to store the zero key separately - a place to store it. */
template <bool need_zero_value_storage, typename Cell>
struct ZeroValueStorage;

template <typename Cell>
struct ZeroValueStorage<true, Cell> {
private:
    bool has_zero = false;
    std::aligned_storage_t<sizeof(Cell), alignof(Cell)>
            zero_value_storage; /// Storage of element with zero key.

public:
    bool get_has_zero() const { return has_zero; }

    void set_get_has_zero() {
        has_zero = true;
        new (zero_value()) Cell();
    }

    void clear_get_has_zero() {
        has_zero = false;
        zero_value()->~Cell();
    }

    Cell* zero_value() { return reinterpret_cast<Cell*>(&zero_value_storage); }
    const Cell* zero_value() const { return reinterpret_cast<const Cell*>(&zero_value_storage); }
};

template <typename Cell>
struct ZeroValueStorage<false, Cell> {
    bool get_has_zero() const { return false; }
    void set_get_has_zero() {
        throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT, "HashTable: logical error");
    }
    void clear_get_has_zero() {}

    Cell* zero_value() { return nullptr; }
    const Cell* zero_value() const { return nullptr; }
};

template <typename Key, typename Cell, typename HashMethod, typename Grower, typename Allocator>
class HashTable : private boost::noncopyable,
                  protected HashMethod,
                  protected Allocator,
                  protected Cell::State,
                  protected ZeroValueStorage<Cell::need_zero_value_storage,
                                             Cell> /// empty base optimization
{
protected:
    friend class Reader;

    template <typename, size_t>
    friend class PartitionedHashTable;

    template <typename SubMaps>
    friend class StringHashTable;

    using HashValue = size_t;
    using Self = HashTable;
    using cell_type = Cell;

    size_t m_size = 0;   /// Amount of elements
    Cell* buf {nullptr}; /// A piece of memory for all elements except the element with zero key.
    Grower grower;
    int64_t _resize_timer_ns;

    // the bucket count threshold above which it's converted to partioned hash table
    // > 0: enable convert dynamically
    // 0: convert is disabled
    int _partitioned_threshold = 0;
    // if need resize and bucket count after resize will be >= _partitioned_threshold,
    // this flag is set to true, and resize does not actually happen,
    // PartitionedHashTable will convert this hash table to partitioned hash table
    bool _need_partition = false;

    //factor that will trigger growing the hash table on insert.
    static constexpr float MAX_BUCKET_OCCUPANCY_FRACTION = 0.5f;

    mutable size_t collisions = 0;

    void set_partitioned_threshold(int threshold) { _partitioned_threshold = threshold; }

    bool check_if_need_partition(size_t bucket_count) {
        return _partitioned_threshold > 0 && bucket_count >= _partitioned_threshold;
    }

    bool need_partition() { return _need_partition; }

    /// Find a cell with the same key or an empty cell, starting from the specified position and further along the collision resolution chain.
    size_t ALWAYS_INLINE find_cell(const Key& x, size_t hash_value, size_t place_value) const {
        while (!buf[place_value].is_zero(*this) &&
               !buf[place_value].key_equals(x, hash_value, *this)) {
            place_value = grower.next(place_value);
            ++collisions;
        }

        return place_value;
    }

    std::pair<bool, size_t> ALWAYS_INLINE find_cell_opt(const Key& x, size_t hash_value,
                                                        size_t place_value) const {
        bool is_zero = false;
        do {
            is_zero = buf[place_value].is_zero(*this);
            if (is_zero || buf[place_value].key_equals(x, hash_value, *this)) break;
            place_value = grower.next(place_value);
        } while (true);

        return {is_zero, place_value};
    }

    /// Find an empty cell, starting with the specified position and further along the collision resolution chain.
    size_t ALWAYS_INLINE find_empty_cell(size_t place_value) const {
        while (!buf[place_value].is_zero(*this)) {
            place_value = grower.next(place_value);
            ++collisions;
        }

        return place_value;
    }

    void alloc(const Grower& new_grower) {
        buf = reinterpret_cast<Cell*>(Allocator::alloc(new_grower.buf_size() * sizeof(Cell)));
        grower = new_grower;
    }

    void free() {
        if (buf) {
            Allocator::free(buf, get_buffer_size_in_bytes());
            buf = nullptr;
        }
    }

    /** Paste into the new buffer the value that was in the old buffer.
      * Used when increasing the buffer size.
      */
    void reinsert(Cell& x, size_t hash_value) {
        size_t place_value = grower.place(hash_value);

        /// If the element is in its place.
        if (&x == &buf[place_value]) return;

        /// Compute a new location, taking into account the collision resolution chain.
        place_value = find_cell(Cell::get_key(x.get_value()), hash_value, place_value);

        /// If the item remains in its place in the old collision resolution chain.
        if (!buf[place_value].is_zero(*this)) return;

        /// Copy to a new location and zero the old one.
        x.set_hash(hash_value);
        memcpy(static_cast<void*>(&buf[place_value]), &x, sizeof(x));
        x.set_zero();

        /// Then the elements that previously were in collision with this can move to the old place.
    }

    void destroy_elements() {
        if (!std::is_trivially_destructible_v<Cell>)
            for (iterator it = begin(), it_end = end(); it != it_end; ++it) it.ptr->~Cell();
    }

    template <typename Derived, bool is_const>
    class iterator_base {
        using Container = std::conditional_t<is_const, const Self, Self>;
        using cell_type = std::conditional_t<is_const, const Cell, Cell>;

        Container* container;
        cell_type* ptr;

        friend class HashTable;

    public:
        iterator_base() {}
        iterator_base(Container* container_, cell_type* ptr_) : container(container_), ptr(ptr_) {}

        bool operator==(const iterator_base& rhs) const { return ptr == rhs.ptr; }
        bool operator!=(const iterator_base& rhs) const { return ptr != rhs.ptr; }

        Derived& operator++() {
            /// If iterator was pointed to ZeroValueStorage, move it to the beginning of the main buffer.
            if (UNLIKELY(ptr->is_zero(*container)))
                ptr = container->buf;
            else
                ++ptr;

            /// Skip empty cells in the main buffer.
            auto buf_end = container->buf + container->grower.buf_size();
            while (ptr < buf_end && ptr->is_zero(*container)) ++ptr;

            return static_cast<Derived&>(*this);
        }

        auto& operator*() const { return *ptr; }
        auto* operator->() const { return ptr; }

        auto get_ptr() const { return ptr; }
        size_t get_hash() const { return ptr->get_hash(*container); }

        size_t get_collision_chain_length() const {
            return container->grower.place((ptr - container->buf) -
                                           container->grower.place(get_hash()));
        }

        /**
          * A hack for HashedDictionary.
          *
          * The problem: std-like find() returns an iterator, which has to be
          * compared to end(). On the other hand, HashMap::find() returns
          * LookupResult, which is compared to nullptr. HashedDictionary has to
          * support both hash maps with the same code, hence the need for this
          * hack.
          *
          * The proper way would be to remove iterator interface from our
          * HashMap completely, change all its users to the existing internal
          * iteration interface, and redefine end() to return LookupResult for
          * compatibility with std find(). Unfortunately, now is not the time to
          * do this.
          */
        operator Cell*() const { return nullptr; }
    };

public:
    using key_type = Key;
    using value_type = typename Cell::value_type;
    using mapped_type = value_type;
    using Hash = HashMethod;

    // Use lookup_result_get_mapped/Key to work with these values.
    using LookupResult = Cell*;
    using ConstLookupResult = const Cell*;

    void reset_resize_timer() { _resize_timer_ns = 0; }
    int64_t get_resize_timer_value() const { return _resize_timer_ns; }

    size_t hash(const Key& x) const { return Hash::operator()(x); }

    HashTable() {
        if (Cell::need_zero_value_storage) this->zero_value()->set_zero();
        alloc(grower);
    }

    HashTable(size_t reserve_for_num_elements) {
        if (Cell::need_zero_value_storage) this->zero_value()->set_zero();
        grower.set(reserve_for_num_elements);
        alloc(grower);
    }

    HashTable(HashTable&& rhs) : buf(nullptr) { *this = std::move(rhs); }

    ~HashTable() {
        destroy_elements();
        free();
    }

    HashTable& operator=(HashTable&& rhs) {
        destroy_elements();
        free();

        std::swap(buf, rhs.buf);
        std::swap(m_size, rhs.m_size);
        std::swap(grower, rhs.grower);
        std::swap(_need_partition, rhs._need_partition);
        std::swap(_partitioned_threshold, rhs._partitioned_threshold);

        Hash::operator=(std::move(rhs));
        Allocator::operator=(std::move(rhs));
        Cell::State::operator=(std::move(rhs));
        ZeroValueStorage<Cell::need_zero_value_storage, Cell>::operator=(std::move(rhs));

        return *this;
    }

    class iterator : public iterator_base<iterator, false> {
    public:
        using iterator_base<iterator, false>::iterator_base;
    };

    class const_iterator : public iterator_base<const_iterator, true> {
    public:
        using iterator_base<const_iterator, true>::iterator_base;
    };

    const_iterator begin() const {
        if (!buf) return end();

        if (this->get_has_zero()) return iterator_to_zero();

        const Cell* ptr = buf;
        auto buf_end = buf + grower.buf_size();
        while (ptr < buf_end && ptr->is_zero(*this)) ++ptr;

        return const_iterator(this, ptr);
    }

    const_iterator cbegin() const { return begin(); }

    iterator begin() {
        if (!buf) return end();

        if (this->get_has_zero()) return iterator_to_zero();

        Cell* ptr = buf;
        auto buf_end = buf + grower.buf_size();
        while (ptr < buf_end && ptr->is_zero(*this)) ++ptr;

        return iterator(this, ptr);
    }

    const_iterator end() const { return const_iterator(this, buf + grower.buf_size()); }
    const_iterator cend() const { return end(); }
    iterator end() { return iterator(this, buf + grower.buf_size()); }

protected:
    const_iterator iterator_to(const Cell* ptr) const { return const_iterator(this, ptr); }
    iterator iterator_to(Cell* ptr) { return iterator(this, ptr); }
    const_iterator iterator_to_zero() const { return iterator_to(this->zero_value()); }
    iterator iterator_to_zero() { return iterator_to(this->zero_value()); }

    /// If the key is zero, insert it into a special place and return true.
    /// We don't have to persist a zero key, because it's not actually inserted.
    /// That's why we just take a Key by value, an not a key holder.
    bool ALWAYS_INLINE emplace_if_zero(Key x, LookupResult& it, bool& inserted, size_t hash_value) {
        /// If it is claimed that the zero key can not be inserted into the table.
        if (!Cell::need_zero_value_storage) return false;

        if (Cell::is_zero(x, *this)) {
            it = this->zero_value();

            if (!this->get_has_zero()) {
                ++m_size;
                this->set_get_has_zero();
                this->zero_value()->set_hash(hash_value);
                inserted = true;
            } else
                inserted = false;

            return true;
        }

        return false;
    }

    template <typename Func>
    bool ALWAYS_INLINE lazy_emplace_if_zero(const Key& x, LookupResult& it, size_t hash_value,
                                            Func&& f) {
        /// If it is claimed that the zero key can not be inserted into the table.
        if (!Cell::need_zero_value_storage) return false;

        if (Cell::is_zero(x, *this)) {
            it = this->zero_value();
            if (!this->get_has_zero()) {
                ++m_size;
                this->set_get_has_zero();
                std::forward<Func>(f)(Constructor(it), x);
                this->zero_value()->set_hash(hash_value);
            }

            return true;
        }

        return false;
    }

    template <typename KeyHolder>
    void ALWAYS_INLINE emplace_non_zero_impl(size_t place_value, KeyHolder&& key_holder,
                                             LookupResult& it, bool& inserted, size_t hash_value) {
        it = &buf[place_value];

        if (!buf[place_value].is_zero(*this)) {
            key_holder_discard_key(key_holder);
            inserted = false;
            return;
        }

        key_holder_persist_key(key_holder);
        const auto& key = key_holder_get_key(key_holder);

        new (&buf[place_value]) Cell(key, *this);
        buf[place_value].set_hash(hash_value);
        inserted = true;
        ++m_size;

        if (UNLIKELY(grower.overflow(m_size))) {
            try {
                resize();
            } catch (...) {
                /** If we have not resized successfully, then there will be problems.
                  * There remains a key, but uninitialized mapped-value,
                  *  which, perhaps, can not even be called a destructor.
                  */
                --m_size;
                buf[place_value].set_zero();
                throw;
            }

            if (LIKELY(!_need_partition)) {
                // The hash table was rehashed, so we have to re-find the key.
                size_t new_place = find_cell(key, hash_value, grower.place(hash_value));
                assert(!buf[new_place].is_zero(*this));
                it = &buf[new_place];
            }
        }
    }

    template <typename KeyHolder, typename Func>
    void ALWAYS_INLINE lazy_emplace_non_zero_impl(size_t place_value, KeyHolder&& key_holder,
                                                  LookupResult& it, size_t hash_value, Func&& f) {
        it = &buf[place_value];

        if (!buf[place_value].is_zero(*this)) {
            key_holder_discard_key(key_holder);
            return;
        }

        key_holder_persist_key(key_holder);
        const auto& key = key_holder_get_key(key_holder);

        f(Constructor(&buf[place_value]), key);
        buf[place_value].set_hash(hash_value);
        ++m_size;

        if (UNLIKELY(grower.overflow(m_size))) {
            try {
                resize();
            } catch (...) {
                /** If we have not resized successfully, then there will be problems.
                  * There remains a key, but uninitialized mapped-value,
                  *  which, perhaps, can not even be called a destructor.
                  */
                --m_size;
                buf[place_value].set_zero();
                throw;
            }

            if (LIKELY(!_need_partition)) {
                // The hash table was rehashed, so we have to re-find the key.
                size_t new_place = find_cell(key, hash_value, grower.place(hash_value));
                assert(!buf[new_place].is_zero(*this));
                it = &buf[new_place];
            }
        }
    }

    /// Only for non-zero keys. Find the right place, insert the key there, if it does not already exist. Set iterator to the cell in output parameter.
    template <typename KeyHolder>
    void ALWAYS_INLINE emplace_non_zero(KeyHolder&& key_holder, LookupResult& it, bool& inserted,
                                        size_t hash_value) {
        const auto& key = key_holder_get_key(key_holder);
        size_t place_value = find_cell(key, hash_value, grower.place(hash_value));
        emplace_non_zero_impl(place_value, key_holder, it, inserted, hash_value);
    }

    template <typename KeyHolder, typename Func>
    void ALWAYS_INLINE lazy_emplace_non_zero(KeyHolder&& key_holder, LookupResult& it,
                                             size_t hash_value, Func&& f) {
        const auto& key = key_holder_get_key(key_holder);
        size_t place_value = find_cell(key, hash_value, grower.place(hash_value));
        lazy_emplace_non_zero_impl(place_value, key_holder, it, hash_value, std::forward<Func>(f));
    }

public:
    void expanse_for_add_elem(size_t num_elem) {
        if (add_elem_size_overflow(num_elem)) {
            resize(grower.buf_size() + num_elem);
        }
    }

    /// Insert a value. In the case of any more complex values, it is better to use the `emplace` function.
    std::pair<LookupResult, bool> ALWAYS_INLINE insert(const value_type& x) {
        std::pair<LookupResult, bool> res;

        size_t hash_value = hash(Cell::get_key(x));
        if (!emplace_if_zero(Cell::get_key(x), res.first, res.second, hash_value)) {
            emplace_non_zero(Cell::get_key(x), res.first, res.second, hash_value);
        }

        if (res.second) insert_set_mapped(lookup_result_get_mapped(res.first), x);

        return res;
    }

    template <bool READ>
    void ALWAYS_INLINE prefetch_by_hash(size_t hash_value) {
        // Two optional arguments:
        // 'rw': 1 means the memory access is write
        // 'locality': 0-3. 0 means no temporal locality. 3 means high temporal locality.
        auto place_value = grower.place(hash_value);
        __builtin_prefetch(&buf[place_value], READ ? 0 : 1, 1);
    }

    /// Reinsert node pointed to by iterator
    void ALWAYS_INLINE reinsert(iterator& it, size_t hash_value) {
        reinsert(*it.get_ptr(), hash_value);
    }

    class Constructor {
    public:
        friend class HashTable;
        template <typename... Args>
        void operator()(Args&&... args) const {
            new (_cell) Cell(std::forward<Args>(args)...);
        }

    private:
        Constructor(Cell* cell) : _cell(cell) {}
        Cell* _cell;
    };

    /** Insert the key.
      * Return values:
      * 'it' -- a LookupResult pointing to the corresponding key/mapped pair.
      * 'inserted' -- whether a new key was inserted.
      *
      * You have to make `placement new` of value if you inserted a new key,
      * since when destroying a hash table, it will call the destructor!
      *
      * Example usage:
      *
      * Map::iterator it;
      * bool inserted;
      * map.emplace(key, it, inserted);
      * if (inserted)
      *     new(&it->second) Mapped(value);
      */
    template <typename KeyHolder>
    void ALWAYS_INLINE emplace(KeyHolder&& key_holder, LookupResult& it, bool& inserted) {
        const auto& key = key_holder_get_key(key_holder);
        emplace(key_holder, it, inserted, hash(key));
    }

    template <typename KeyHolder>
    void ALWAYS_INLINE emplace(KeyHolder&& key_holder, LookupResult& it, bool& inserted,
                               size_t hash_value) {
        const auto& key = key_holder_get_key(key_holder);
        if (!emplace_if_zero(key, it, inserted, hash_value))
            emplace_non_zero(key_holder, it, inserted, hash_value);
    }

    template <typename KeyHolder>
    void ALWAYS_INLINE emplace(KeyHolder&& key_holder, LookupResult& it, size_t hash_value,
                               bool& inserted) {
        emplace(key_holder, it, inserted, hash_value);
    }

    template <typename KeyHolder, typename Func>
    void ALWAYS_INLINE lazy_emplace(KeyHolder&& key_holder, LookupResult& it, Func&& f) {
        const auto& key = key_holder_get_key(key_holder);
        lazy_emplace(key_holder, it, hash(key), std::forward<Func>(f));
    }

    template <typename KeyHolder, typename Func>
    void ALWAYS_INLINE lazy_emplace(KeyHolder&& key_holder, LookupResult& it, size_t hash_value,
                                    Func&& f) {
        const auto& key = key_holder_get_key(key_holder);
        if (!lazy_emplace_if_zero(key, it, hash_value, std::forward<Func>(f)))
            lazy_emplace_non_zero(key_holder, it, hash_value, std::forward<Func>(f));
    }

    /// Copy the cell from another hash table. It is assumed that the cell is not zero, and also that there was no such key in the table yet.
    void ALWAYS_INLINE insert_unique_non_zero(const Cell* cell, size_t hash_value) {
        size_t place_value = find_empty_cell(grower.place(hash_value));

        memcpy(static_cast<void*>(&buf[place_value]), cell, sizeof(*cell));
        ++m_size;

        if (UNLIKELY(grower.overflow(m_size))) resize();
    }

    LookupResult ALWAYS_INLINE find(Key x) {
        if (Cell::is_zero(x, *this)) return this->get_has_zero() ? this->zero_value() : nullptr;

        size_t hash_value = hash(x);
        auto [is_zero, place_value] = find_cell_opt(x, hash_value, grower.place(hash_value));
        return !is_zero ? &buf[place_value] : nullptr;
    }

    ConstLookupResult ALWAYS_INLINE find(Key x) const {
        return const_cast<std::decay_t<decltype(*this)>*>(this)->find(x);
    }

    LookupResult ALWAYS_INLINE find(Key x, size_t hash_value) {
        if (Cell::is_zero(x, *this)) return this->get_has_zero() ? this->zero_value() : nullptr;

        size_t place_value = find_cell(x, hash_value, grower.place(hash_value));
        return !buf[place_value].is_zero(*this) ? &buf[place_value] : nullptr;
    }

    bool ALWAYS_INLINE has(Key x) const {
        if (Cell::is_zero(x, *this)) return this->get_has_zero();

        size_t hash_value = hash(x);
        size_t place_value = find_cell(x, hash_value, grower.place(hash_value));
        return !buf[place_value].is_zero(*this);
    }

    bool ALWAYS_INLINE has(Key x, size_t hash_value) const {
        if (Cell::is_zero(x, *this)) return this->get_has_zero();

        size_t place_value = find_cell(x, hash_value, grower.place(hash_value));
        return !buf[place_value].is_zero(*this);
    }

    void write(doris::vectorized::BufferWritable& wb) const {
        Cell::State::write(wb);
        doris::vectorized::write_var_uint(m_size, wb);

        if (this->get_has_zero()) this->zero_value()->write(wb);

        for (auto ptr = buf, buf_end = buf + grower.buf_size(); ptr < buf_end; ++ptr)
            if (!ptr->is_zero(*this)) ptr->write(wb);
    }

    void read(doris::vectorized::BufferReadable& rb) {
        Cell::State::read(rb);

        destroy_elements();
        this->clear_get_has_zero();
        m_size = 0;

        doris::vectorized::UInt64 new_size = 0;
        doris::vectorized::read_var_uint(new_size, rb);

        free();
        Grower new_grower = grower;
        new_grower.set(new_size);
        alloc(new_grower);

        for (size_t i = 0; i < new_size; ++i) {
            Cell x;
            x.read(rb);
            insert(Cell::get_key(x.get_value()));
        }
    }

    size_t size() const { return m_size; }

    bool empty() const { return 0 == m_size; }

    float get_factor() const { return MAX_BUCKET_OCCUPANCY_FRACTION; }

    bool should_be_shrink(int64_t valid_row) const {
        return valid_row < get_factor() * (size() / 2.0);
    }

    void init_buf_size(size_t reserve_for_num_elements) {
        free();
        grower.set(reserve_for_num_elements);
        alloc(grower);
    }

    void delete_zero_key(Key key) {
        if (this->get_has_zero() && Cell::is_zero(key, *this)) {
            --m_size;
            this->clear_get_has_zero();
        }
    }

    void clear() {
        destroy_elements();
        this->clear_get_has_zero();
        m_size = 0;

        memset(static_cast<void*>(buf), 0, grower.buf_size() * sizeof(*buf));
    }

    /// After executing this function, the table can only be destroyed,
    ///  and also you can use the methods `size`, `empty`, `begin`, `end`.
    void clear_and_shrink() {
        destroy_elements();
        this->clear_get_has_zero();
        m_size = 0;
        free();
    }

    size_t get_buffer_size_in_bytes() const { return grower.buf_size() * sizeof(Cell); }

    size_t get_buffer_size_in_cells() const { return grower.buf_size(); }

    bool add_elem_size_overflow(size_t add_size) const {
        return grower.overflow(add_size + m_size);
    }
    int64_t get_collisions() const { return collisions; }

private:
    /// Increase the size of the buffer.
    void resize(size_t for_num_elems = 0, size_t for_buf_size = 0) {
        SCOPED_RAW_TIMER(&_resize_timer_ns);

        size_t old_size = grower.buf_size();

        /** In case of exception for the object to remain in the correct state,
          *  changing the variable `grower` (which determines the buffer size of the hash table)
          *  is postponed for a moment after a real buffer change.
          * The temporary variable `new_grower` is used to determine the new size.
          */
        Grower new_grower = grower;
        if (for_num_elems) {
            new_grower.set(for_num_elems);
            if (new_grower.buf_size() <= old_size) return;
        } else if (for_buf_size) {
            new_grower.set_buf_size(for_buf_size);
            if (new_grower.buf_size() <= old_size) return;
        } else
            new_grower.increase_size();

        // new bucket count exceed partitioned hash table bucket count threshold,
        // don't resize and set need partition flag
        if (check_if_need_partition(new_grower.buf_size())) {
            _need_partition = true;
            return;
        }

        /// Expand the space.
        buf = reinterpret_cast<Cell*>(Allocator::realloc(buf, get_buffer_size_in_bytes(),
                                                         new_grower.buf_size() * sizeof(Cell)));
        grower = new_grower;

        /** Now some items may need to be moved to a new location.
          * The element can stay in place, or move to a new location "on the right",
          *  or move to the left of the collision resolution chain, because the elements to the left of it have been moved to the new "right" location.
          */
        size_t i = 0;
        for (; i < old_size; ++i) {
            if (!buf[i].is_zero(*this)) {
                reinsert(buf[i], buf[i].get_hash(*this));
            }
        }

        /** There is also a special case:
          *    if the element was to be at the end of the old buffer,                  [        x]
          *    but is at the beginning because of the collision resolution chain,      [o       x]
          *    then after resizing, it will first be out of place again,               [        xo        ]
          *    and in order to transfer it where necessary,
          *    after transferring all the elements from the old halves you need to     [         o   x    ]
          *    process tail from the collision resolution chain immediately after it   [        o    x    ]
          */
        for (; !buf[i].is_zero(*this); ++i) {
            reinsert(buf[i], buf[i].get_hash(*this));
        }
    }
};
