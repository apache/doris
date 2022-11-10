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

#pragma once

#include "vec/common/allocator.h"
#include "vec/common/hash_table/hash_table.h"

/** NOTE JoinHashTable could only be used for memmoveable (position independent) types.
  * Example: std::string is not position independent in libstdc++ with C++11 ABI or in libc++.
  * Also, key in hash table must be of type, that zero bytes is compared equals to zero key.
  */

/** Determines the size of the join hash table, and when and how much it should be resized.
  */
template <size_t initial_size_degree = 10>
struct JoinHashTableGrower {
    /// The state of this structure is enough to get the buffer size of the join hash table.
    doris::vectorized::UInt8 size_degree = initial_size_degree;
    doris::vectorized::Int64 double_grow_degree = 31; // 2GB

    size_t bucket_size() const { return 1ULL << (size_degree - 1); }

    /// The size of the join hash table in the cells.
    size_t buf_size() const { return 1ULL << size_degree; }

    size_t max_fill() const { return buf_size(); }

    size_t mask() const { return bucket_size() - 1; }

    /// From the hash value, get the bucket id (first index) in the join hash table.
    size_t place(size_t x) const { return x & mask(); }

    /// Whether the join hash table is full. You need to increase the size of the hash table, or remove something unnecessary from it.
    bool overflow(size_t elems) const { return elems >= max_fill(); }

    /// Increase the size of the join hash table.
    void increase_size() { size_degree += size_degree >= 23 ? 1 : 2; }

    /// Set the buffer size by the number of elements in the join hash table. Used when deserializing a join hash table.
    void set(size_t num_elems) {
#ifndef STRICT_MEMORY_USE
        size_t fill_capacity = static_cast<size_t>(log2(num_elems - 1)) + 2;
#else
        size_t fill_capacity = static_cast<size_t>(log2(num_elems - 1)) + 1;
        fill_capacity =
                fill_capacity < double_grow_degree
                        ? fill_capacity + 1
                        : (num_elems < (1ULL << fill_capacity) - (1ULL << (fill_capacity - 2))
                                   ? fill_capacity
                                   : fill_capacity + 1);
#endif
        size_degree = num_elems <= 1 ? initial_size_degree
                                     : (initial_size_degree > fill_capacity ? initial_size_degree
                                                                            : fill_capacity);
    }

    void set_buf_size(size_t buf_size_) {
        size_degree = static_cast<size_t>(log2(buf_size_ - 1) + 1);
    }
};

/** Determines the size of the join hash table, and when and how much it should be resized.
  * This structure is aligned to cache line boundary and also occupies it all.
  * Precalculates some values to speed up lookups and insertion into the JoinHashTable (and thus has bigger memory footprint than JoinHashTableGrower).
  */
template <size_t initial_size_degree = 8>
class alignas(64) JoinHashTableGrowerWithPrecalculation {
    /// The state of this structure is enough to get the buffer size of the join hash table.

    doris::vectorized::UInt8 size_degree_ = initial_size_degree;
    size_t precalculated_mask = (1ULL << (initial_size_degree - 1)) - 1;
    size_t precalculated_max_fill = 1ULL << initial_size_degree;

public:
    doris::vectorized::UInt8 size_degree() const { return size_degree_; }

    void increase_size_degree(doris::vectorized::UInt8 delta) {
        size_degree_ += delta;
        precalculated_mask = (1ULL << (size_degree_ - 1)) - 1;
        precalculated_max_fill = 1ULL << size_degree_;
    }

    static constexpr auto initial_count = 1ULL << initial_size_degree;

    /// If collision resolution chains are contiguous, we can implement erase operation by moving the elements.
    static constexpr auto performs_linear_probing_with_single_step = true;

    size_t bucket_size() const { return 1ULL << (size_degree_ - 1); }

    /// The size of the join hash table in the cells.
    size_t buf_size() const { return 1ULL << size_degree_; }

    /// From the hash value, get the cell number in the join hash table.
    size_t place(size_t x) const { return x & precalculated_mask; }

    /// Whether the join hash table is full. You need to increase the size of the hash table, or remove something unnecessary from it.
    bool overflow(size_t elems) const { return elems >= precalculated_max_fill; }

    /// Increase the size of the join hash table.
    void increase_size() { increase_size_degree(size_degree_ >= 23 ? 1 : 2); }

    /// Set the buffer size by the number of elements in the join hash table. Used when deserializing a join hash table.
    void set(size_t num_elems) {
        size_degree_ =
                num_elems <= 1
                        ? initial_size_degree
                        : ((initial_size_degree > static_cast<size_t>(log2(num_elems - 1)) + 2)
                                   ? initial_size_degree
                                   : (static_cast<size_t>(log2(num_elems - 1)) + 2));
        increase_size_degree(0);
    }

    void set_buf_size(size_t buf_size_) {
        size_degree_ = static_cast<size_t>(log2(buf_size_ - 1) + 1);
        increase_size_degree(0);
    }
};

static_assert(sizeof(JoinHashTableGrowerWithPrecalculation<>) == 64);

/** When used as a Grower, it turns a hash table into something like a lookup table.
  * It remains non-optimal - the cells store the keys.
  * Also, the compiler can not completely remove the code of passing through the collision resolution chain, although it is not needed.
  * TODO Make a proper lookup table.
  */
template <size_t key_bits>
struct JoinHashTableFixedGrower {
    size_t bucket_size() const { return 1ULL << (key_bits - 1); }
    size_t buf_size() const { return 1ULL << key_bits; }
    size_t place(size_t x) const { return x & (bucket_size() - 1); }
    bool overflow(size_t /*elems*/) const { return false; }

    void increase_size() { __builtin_unreachable(); }
    void set(size_t /*num_elems*/) {}
    void set_buf_size(size_t /*buf_size_*/) {}
};

template <typename Key, typename Cell, typename Hash, typename Grower, typename Allocator>
class JoinHashTable : private boost::noncopyable,
                      protected Hash,
                      protected Allocator,
                      protected Cell::State,
                      protected ZeroValueStorage<Cell::need_zero_value_storage,
                                                 Cell> /// empty base optimization
{
protected:
    friend class const_iterator;
    friend class iterator;
    friend class Reader;

    template <typename, typename, typename, typename, typename, typename, size_t>
    friend class TwoLevelHashTable;

    template <typename SubMaps>
    friend class StringHashTable;

    using HashValue = size_t;
    using Self = JoinHashTable;
    using cell_type = Cell;

    size_t m_size = 0;         /// Amount of elements
    size_t m_no_zero_size = 0; /// Amount of elements except the element with zero key.
    Cell* buf; /// A piece of memory for all elements except the element with zero key.

    // bucket-chained hash table
    // "first" is the buckets of the hash map, and it holds the index of the first key value saved in each bucket,
    // while other keys can be found by following the indices saved in
    // "next". "next[0]" represents the end of the list of keys in a bucket.
    // https://dare.uva.nl/search?identifier=5ccbb60a-38b8-4eeb-858a-e7735dd37487
    size_t* first;
    size_t* next;

    Grower grower;
    int64_t _resize_timer_ns;

    //factor that will trigger growing the hash table on insert.
    static constexpr float MAX_BUCKET_OCCUPANCY_FRACTION = 1.0f;

#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
    mutable size_t collisions = 0;
#endif

    /// Find a cell with the same key or an empty cell, starting from the specified position and further along the collision resolution chain.
    size_t ALWAYS_INLINE find_cell(const Key& x, size_t hash_value, size_t place_value) const {
        while (place_value && !buf[place_value - 1].is_zero(*this) &&
               !buf[place_value - 1].key_equals(x, hash_value, *this)) {
            place_value = next[place_value];
#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
            ++collisions;
#endif
        }

        return place_value;
    }

    std::pair<bool, size_t> ALWAYS_INLINE find_cell_opt(const Key& x, size_t hash_value,
                                                        size_t place_value) const {
        bool is_zero = false;
        do {
            if (!place_value) return {true, place_value};
            is_zero = buf[place_value - 1].is_zero(*this); ///
            if (is_zero || buf[place_value - 1].key_equals(x, hash_value, *this)) break;
            place_value = next[place_value];
        } while (true);

        return {is_zero, place_value};
    }

    /// Find an empty cell, starting with the specified position and further along the collision resolution chain.
    size_t ALWAYS_INLINE find_empty_cell(size_t place_value) const {
        while (place_value && !buf[place_value - 1].is_zero(*this)) {
            place_value = next[place_value];
#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
            ++collisions;
#endif
        }

        return place_value;
    }

    void alloc(const Grower& new_grower) {
        buf = reinterpret_cast<Cell*>(Allocator::alloc(new_grower.buf_size() * sizeof(Cell)));
        first = reinterpret_cast<size_t*>(
                Allocator::alloc(new_grower.bucket_size() * sizeof(size_t)));
        memset(first, 0, new_grower.bucket_size() * sizeof(size_t));
        next = reinterpret_cast<size_t*>(
                Allocator::alloc((new_grower.buf_size() + 1) * sizeof(size_t)));
        memset(next, 0, (new_grower.buf_size() + 1) * sizeof(size_t));
        grower = new_grower;
    }

    void free() {
        if (buf) {
            Allocator::free(buf, get_buffer_size_in_bytes());
            buf = nullptr;
        }
        if (first) {
            Allocator::free(first, grower.bucket_size() * sizeof(size_t));
            first = nullptr;
        }
        if (next) {
            Allocator::free(next, (grower.buf_size() + 1) * sizeof(size_t));
            next = nullptr;
        }
    }

    /// Increase the size of the buffer.
    void resize(size_t for_num_elems = 0, size_t for_buf_size = 0) {
        SCOPED_RAW_TIMER(&_resize_timer_ns);
#ifdef DBMS_HASH_MAP_DEBUG_RESIZES
        Stopwatch watch;
#endif

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

        /// Expand the space.
        buf = reinterpret_cast<Cell*>(Allocator::realloc(buf, get_buffer_size_in_bytes(),
                                                         new_grower.buf_size() * sizeof(Cell)));
        first = reinterpret_cast<size_t*>(Allocator::realloc(
                first, get_bucket_size_in_bytes(), new_grower.bucket_size() * sizeof(size_t)));
        memset(first, 0, new_grower.bucket_size() * sizeof(size_t));
        next = reinterpret_cast<size_t*>(Allocator::realloc(
                next, get_buffer_size_in_bytes(), (new_grower.buf_size() + 1) * sizeof(size_t)));
        memset(next, 0, (new_grower.buf_size() + 1) * sizeof(size_t));
        grower = new_grower;

        /** Now some items may need to be moved to a new location.
          * The element can stay in place, or move to a new location "on the right",
          *  or move to the left of the collision resolution chain, because the elements to the left of it have been moved to the new "right" location.
          */
        size_t i = 0;
        for (; i < m_no_zero_size; ++i)
            if (!buf[i].is_zero(*this)) reinsert(i + 1, buf[i], buf[i].get_hash(*this));

#ifdef DBMS_HASH_MAP_DEBUG_RESIZES
        watch.stop();
        std::cerr << std::fixed << std::setprecision(3) << "Resize from " << old_size << " to "
                  << grower.buf_size() << " took " << watch.elapsedSeconds() << " sec."
                  << std::endl;
#endif
    }

    /** Paste into the new buffer the value that was in the old buffer.
      * Used when increasing the buffer size.
      */
    void reinsert(size_t place_value, Cell& x, size_t hash_value) {
        size_t bucket_value = grower.place(hash_value);
        next[place_value] = first[bucket_value];
        first[bucket_value] = place_value;
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

        friend class JoinHashTable;

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
            auto buf_end = container->buf + container->m_no_zero_size;
            while (ptr < buf_end && ptr->is_zero(*container)) ++ptr;

            return static_cast<Derived&>(*this);
        }

        auto& operator*() const { return *ptr; }
        auto* operator->() const { return ptr; }

        auto get_ptr() const { return ptr; }
        size_t get_hash() const { return ptr->get_hash(*container); }

        size_t get_collision_chain_length() const { ////////////// ?????????
            return 0;
        }

        operator Cell*() const { return nullptr; }
    };

public:
    using key_type = Key;
    using value_type = typename Cell::value_type;

    // Use lookup_result_get_mapped/Key to work with these values.
    using LookupResult = Cell*;
    using ConstLookupResult = const Cell*;

    void reset_resize_timer() { _resize_timer_ns = 0; }
    int64_t get_resize_timer_value() const { return _resize_timer_ns; }

    size_t hash(const Key& x) const { return Hash::operator()(x); }

    JoinHashTable() {
        if (Cell::need_zero_value_storage) this->zero_value()->set_zero();
        alloc(grower);
    }

    JoinHashTable(size_t reserve_for_num_elements) {
        if (Cell::need_zero_value_storage) this->zero_value()->set_zero();
        grower.set(reserve_for_num_elements);
        alloc(grower);
    }

    JoinHashTable(JoinHashTable&& rhs) : buf(nullptr) { *this = std::move(rhs); }

    ~JoinHashTable() {
        destroy_elements();
        free();
    }

    JoinHashTable& operator=(JoinHashTable&& rhs) {
        destroy_elements();
        free();

        std::swap(buf, rhs.buf);
        std::swap(m_size, rhs.m_size);
        std::swap(m_no_zero_size, rhs.m_no_zero_size);
        std::swap(first, rhs.first);
        std::swap(next, rhs.next);
        std::swap(grower, rhs.grower);

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
        auto buf_end = buf + m_no_zero_size;
        while (ptr < buf_end && ptr->is_zero(*this)) ++ptr;

        return const_iterator(this, ptr);
    }

    const_iterator cbegin() const { return begin(); }

    iterator begin() {
        if (!buf) return end();

        if (this->get_has_zero()) return iterator_to_zero();

        Cell* ptr = buf;
        auto buf_end = buf + m_no_zero_size;
        while (ptr < buf_end && ptr->is_zero(*this)) ++ptr;

        return iterator(this, ptr);
    }

    const_iterator end() const { return const_iterator(this, buf + m_no_zero_size); }
    const_iterator cend() const { return end(); }
    iterator end() { return iterator(this, buf + m_no_zero_size); }

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

    /// Only for non-zero keys. Find the right place, insert the key there, if it does not already exist. Set iterator to the cell in output parameter.
    template <typename KeyHolder>
    void ALWAYS_INLINE emplace_non_zero(KeyHolder&& key_holder, LookupResult& it, bool& inserted,
                                        size_t hash_value) {
        it = &buf[m_no_zero_size];

        if (!buf[m_no_zero_size].is_zero(*this)) {
            key_holder_discard_key(key_holder);
            inserted = false;
            return;
        }

        key_holder_persist_key(key_holder);
        const auto& key = key_holder_get_key(key_holder);

        new (&buf[m_no_zero_size]) Cell(key, *this);
        buf[m_no_zero_size].set_hash(hash_value);
        size_t bucket_value = grower.place(hash_value);
        inserted = true;
        ++m_size;
        ++m_no_zero_size;
        next[m_no_zero_size] = first[bucket_value];
        first[bucket_value] = m_no_zero_size;

        if (UNLIKELY(grower.overflow(m_size))) {
            try {
                resize();
            } catch (...) {
                /** If we have not resized successfully, then there will be problems.
                  * There remains a key, but uninitialized mapped-value,
                  *  which, perhaps, can not even be called a destructor.
                  */
                first[bucket_value] = next[m_no_zero_size];
                next[m_no_zero_size] = 0;
                --m_size;
                --m_no_zero_size;
                buf[m_no_zero_size].set_zero();
                throw;
            }
        }
    }

public:
    void expanse_for_add_elem(size_t num_elem) {
        std::cout << "expanse_for_add_elem\n";
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

    template <typename KeyHolder>
    void ALWAYS_INLINE prefetch(KeyHolder& key_holder) {
        key_holder_get_key(key_holder);
        __builtin_prefetch(&buf[m_no_zero_size]);
    }

    /// Reinsert node pointed to by iterator
    // void ALWAYS_INLINE reinsert(iterator& it, size_t hash_value) {
    //     reinsert(*it.get_ptr(), hash_value);
    // }

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

    /// Copy the cell from another hash table. It is assumed that the cell is not zero, and also that there was no such key in the table yet.
    void ALWAYS_INLINE insert_unique_non_zero(const Cell* cell, size_t hash_value) {
        memcpy(static_cast<void*>(&buf[m_no_zero_size]), cell, sizeof(*cell));
        size_t bucket_value = grower.place();
        ++m_size;
        ++m_no_zero_size;
        next[m_no_zero_size] = first[bucket_value];
        first[bucket_value] = m_no_zero_size;

        if (UNLIKELY(grower.overflow(m_size))) resize();
    }

    LookupResult ALWAYS_INLINE find(Key x) {
        if (Cell::is_zero(x, *this)) return this->get_has_zero() ? this->zero_value() : nullptr;

        size_t hash_value = hash(x);
        auto [is_zero, place_value] = find_cell_opt(x, hash_value, first[grower.place(hash_value)]);

        if (!place_value) return nullptr;

        return !is_zero ? &buf[place_value - 1] : nullptr;
    }

    ConstLookupResult ALWAYS_INLINE find(Key x) const {
        return const_cast<std::decay_t<decltype(*this)>*>(this)->find(x);
    }

    LookupResult ALWAYS_INLINE find(Key x, size_t hash_value) {
        if (Cell::is_zero(x, *this)) return this->get_has_zero() ? this->zero_value() : nullptr;

        size_t place_value = find_cell(x, hash_value, first[grower.place(hash_value)]);

        if (!place_value) return nullptr;

        return !buf[place_value - 1].is_zero(*this) ? &buf[place_value - 1] : nullptr;
    }

    bool ALWAYS_INLINE has(Key x) const {
        if (Cell::is_zero(x, *this)) return this->get_has_zero();

        size_t hash_value = hash(x);
        size_t place_value = find_cell(x, hash_value, first[grower.place(hash_value)]);
        return !place_value && !buf[place_value - 1].is_zero(*this);
    }

    bool ALWAYS_INLINE has(Key x, size_t hash_value) const {
        if (Cell::is_zero(x, *this)) return this->get_has_zero();

        size_t place_value = find_cell(x, hash_value, first[grower.place(hash_value)]);
        return !place_value && !buf[place_value - 1].is_zero(*this);
    }

    void write(doris::vectorized::BufferWritable& wb) const {
        Cell::State::write(wb);
        doris::vectorized::write_var_uint(m_size, wb);

        if (this->get_has_zero()) this->zero_value()->write(wb);

        for (auto ptr = buf, buf_end = buf + m_no_zero_size; ptr < buf_end; ++ptr)
            if (!ptr->is_zero(*this)) ptr->write(wb);
    }

    void read(doris::vectorized::BufferReadable& rb) {
        Cell::State::read(rb);

        destroy_elements();
        this->clear_get_has_zero();
        m_size = 0;

        size_t new_size = 0;
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

    size_t no_zero_size() const { return m_no_zero_size; }

    bool empty() const { return 0 == m_size; }

    float get_factor() const { return MAX_BUCKET_OCCUPANCY_FRACTION; }

    bool should_be_shrink(int64_t valid_row) { return valid_row < get_factor() * (size() / 2.0); }

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
        m_no_zero_size = 0;

        memset(static_cast<void*>(buf), 0, grower.buf_size() * sizeof(*buf));
    }

    /// After executing this function, the table can only be destroyed,
    ///  and also you can use the methods `size`, `empty`, `begin`, `end`.
    void clear_and_shrink() {
        destroy_elements();
        this->clear_get_has_zero();
        m_size = 0;
        m_no_zero_size = 0;
        free();
    }

    size_t get_buffer_size_in_bytes() const { return grower.buf_size() * sizeof(Cell); }

    size_t get_bucket_size_in_bytes() const { return grower.bucket_size() * sizeof(Cell); }

    size_t get_buffer_size_in_cells() const { return grower.buf_size(); }

    bool add_elem_size_overflow(size_t add_size) const {
        return grower.overflow(add_size + m_size);
    }
#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
    size_t getCollisions() const { return collisions; }
#endif
};