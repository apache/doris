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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/HashTable/FixedHashTable.h
// and modified by Doris

#pragma once

#include "vec/common/hash_table/hash_table.h"

/// How to obtain the size of the table.

template <typename Cell>
struct FixedHashTableStoredSize {
    size_t m_size = 0;

    size_t get_size(const Cell*, const typename Cell::State&, size_t) const { return m_size; }
    bool is_empty(const Cell*, const typename Cell::State&, size_t) const { return m_size == 0; }

    void increase_size() { ++m_size; }
    void clear_size() { m_size = 0; }
    void set_size(size_t to) { m_size = to; }
};

template <typename Cell>
struct FixedHashTableCalculatedSize {
    size_t get_size(const Cell* buf, const typename Cell::State& state, size_t num_cells) const {
        size_t res = 0;
        for (const Cell* end = buf + num_cells; buf != end; ++buf)
            if (!buf->is_zero(state)) ++res;
        return res;
    }

    bool isEmpty(const Cell* buf, const typename Cell::State& state, size_t num_cells) const {
        for (const Cell* end = buf + num_cells; buf != end; ++buf)
            if (!buf->is_zero(state)) return false;
        return true;
    }

    void increase_size() {}
    void clear_size() {}
    void set_size(size_t) {}
};

/** Used as a lookup table for small keys such as UInt8, UInt16. It's different
  *  than a HashTable in that keys are not stored in the Cell buf, but inferred
  *  inside each iterator. There are a bunch of to make it faster than using
  *  HashTable: a) It doesn't have a conflict chain; b) There is no key
  *  comparison; c) The number of cycles for checking cell empty is halved; d)
  *  Memory layout is tighter, especially the Clearable variants.
  *
  * NOTE: For Set variants this should always be better. For Map variants
  *  however, as we need to assemble the real cell inside each iterator, there
  *  might be some cases we fall short.
  *
  * TODO: Deprecate the cell API so that end users don't rely on the structure
  *  of cell. Instead iterator should be used for operations such as cell
  *  transfer, key updates (f.g. StringRef) and serde. This will allow
  *  TwoLevelHashSet(Map) to contain different type of sets(maps).
  */
template <typename Key, typename Cell, typename Size, typename Allocator>
class FixedHashTable : private boost::noncopyable,
                       protected Allocator,
                       protected Cell::State,
                       protected Size {
    static constexpr size_t NUM_CELLS = 1ULL << (sizeof(Key) * 8);

protected:
    using Self = FixedHashTable;

    Cell* buf; /// A piece of memory for all elements.

    void alloc() { buf = reinterpret_cast<Cell*>(Allocator::alloc(NUM_CELLS * sizeof(Cell))); }

    void free() {
        if (buf) {
            Allocator::free(buf, get_buffer_size_in_bytes());
            buf = nullptr;
        }
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

        friend class FixedHashTable;

    public:
        iterator_base() {}
        iterator_base(Container* container_, cell_type* ptr_) : container(container_), ptr(ptr_) {
            cell.update(ptr - container->buf, ptr);
        }

        bool operator==(const iterator_base& rhs) const { return ptr == rhs.ptr; }
        bool operator!=(const iterator_base& rhs) const { return ptr != rhs.ptr; }

        Derived& operator++() {
            ++ptr;

            /// Skip empty cells in the main buffer.
            auto buf_end = container->buf + container->NUM_CELLS;
            while (ptr < buf_end && ptr->is_zero(*container)) ++ptr;

            return static_cast<Derived&>(*this);
        }

        auto& operator*() {
            if (cell.key != ptr - container->buf) cell.update(ptr - container->buf, ptr);
            return cell;
        }
        auto* operator->() {
            if (cell.key != ptr - container->buf) cell.update(ptr - container->buf, ptr);
            return &cell;
        }

        auto get_ptr() const { return ptr; }
        size_t get_hash() const { return ptr - container->buf; }
        size_t get_collision_chain_length() const { return 0; }
        typename cell_type::CellExt cell;
    };

public:
    using key_type = Key;
    using mapped_type = typename Cell::mapped_type;
    using value_type = typename Cell::value_type;
    using cell_type = Cell;

    using LookupResult = Cell*;
    using ConstLookupResult = const Cell*;

    size_t hash(const Key& x) const { return x; }

    FixedHashTable() { alloc(); }

    FixedHashTable(FixedHashTable&& rhs) : buf(nullptr) { *this = std::move(rhs); }

    ~FixedHashTable() {
        destroy_elements();
        free();
    }

    FixedHashTable& operator=(FixedHashTable&& rhs) {
        destroy_elements();
        free();

        const auto new_size = rhs.size();
        std::swap(buf, rhs.buf);
        this->set_size(new_size);

        Allocator::operator=(std::move(rhs));
        Cell::State::operator=(std::move(rhs));

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

        const Cell* ptr = buf;
        auto buf_end = buf + NUM_CELLS;
        while (ptr < buf_end && ptr->is_zero(*this)) ++ptr;

        return const_iterator(this, ptr);
    }

    const_iterator cbegin() const { return begin(); }

    iterator begin() {
        if (!buf) return end();

        Cell* ptr = buf;
        auto buf_end = buf + NUM_CELLS;
        while (ptr < buf_end && ptr->is_zero(*this)) ++ptr;

        return iterator(this, ptr);
    }

    const_iterator end() const {
        /// Avoid UBSan warning about adding zero to nullptr. It is valid in C++20 (and earlier) but not valid in C.
        return const_iterator(this, buf ? buf + NUM_CELLS : buf);
    }

    const_iterator cend() const { return end(); }

    iterator end() { return iterator(this, buf ? buf + NUM_CELLS : buf); }

public:
    /// The last parameter is unused but exists for compatibility with HashTable interface.
    void ALWAYS_INLINE emplace(const Key& x, LookupResult& it, bool& inserted,
                               size_t /* hash */ = 0) {
        it = &buf[x];

        if (!buf[x].is_zero(*this)) {
            inserted = false;
            return;
        }

        new (&buf[x]) Cell(x, *this);
        inserted = true;
        this->increase_size();
    }

    class Constructor {
    public:
        friend class FixedHashTable;
        template <typename... Args>
        void operator()(Args&&... args) const {
            new (_cell) Cell(std::forward<Args>(args)...);
        }

    private:
        Constructor(Cell* cell) : _cell(cell) {}
        Cell* _cell;
    };

    template <typename Func>
    void ALWAYS_INLINE lazy_emplace(const Key& x, LookupResult& it, Func&& f) {
        it = &buf[x];

        if (!buf[x].is_zero(*this)) {
            return;
        }

        f(Constructor(&buf[x]), x, x);
        this->increase_size();
    }

    template <typename Func>
    void ALWAYS_INLINE lazy_emplace(const Key& x, LookupResult& it, size_t hash_value, Func&& f) {
        lazy_emplace(x, it, std::forward<Func>(f));
    }

    template <bool READ>
    void ALWAYS_INLINE prefetch(const Key& key, size_t hash_value) {
        // Two optional arguments:
        // 'rw': 1 means the memory access is write
        // 'locality': 0-3. 0 means no temporal locality. 3 means high temporal locality.
        __builtin_prefetch(&buf[hash_value], READ ? 0 : 1, 1);
    }

    std::pair<LookupResult, bool> ALWAYS_INLINE insert(const value_type& x) {
        std::pair<LookupResult, bool> res;
        emplace(Cell::get_key(x), res.first, res.second);
        if (res.second) insert_set_mapped(res.first->get_mapped(), x);

        return res;
    }

    LookupResult ALWAYS_INLINE find(const Key& x) {
        return !buf[x].is_zero(*this) ? &buf[x] : nullptr;
    }

    ConstLookupResult ALWAYS_INLINE find(const Key& x) const {
        return const_cast<std::decay_t<decltype(*this)>*>(this)->find(x);
    }

    LookupResult ALWAYS_INLINE find(const Key&, size_t hash_value) {
        return !buf[hash_value].is_zero(*this) ? &buf[hash_value] : nullptr;
    }

    ConstLookupResult ALWAYS_INLINE find(const Key& key, size_t hash_value) const {
        return const_cast<std::decay_t<decltype(*this)>*>(this)->find(key, hash_value);
    }

    bool ALWAYS_INLINE has(const Key& x) const { return !buf[x].is_zero(*this); }
    bool ALWAYS_INLINE has(const Key&, size_t hash_value) const {
        return !buf[hash_value].is_zero(*this);
    }

    void write(doris::vectorized::BufferWritable& wb) const {
        Cell::State::write(wb);
        doris::vectorized::write_var_uint(size(), wb);

        if (!buf) return;

        for (auto ptr = buf, buf_end = buf + NUM_CELLS; ptr < buf_end; ++ptr) {
            if (!ptr->is_zero(*this)) {
                doris::vectorized::write_var_uint(ptr - buf, wb);
                ptr->write(wb);
            }
        }
    }

    void read(doris::vectorized::BufferReadable& rb) {
        Cell::State::read(rb);
        destroy_elements();
        doris::vectorized::UInt64 m_size;
        doris::vectorized::read_var_uint(m_size, rb);
        this->set_size(m_size);
        free();
        alloc();

        for (size_t i = 0; i < m_size; ++i) {
            doris::vectorized::UInt64 place_value = 0;
            doris::vectorized::read_var_uint(place_value, rb);
            Cell x;
            x.read(rb);
            new (&buf[place_value]) Cell(x, *this);
        }
    }

    size_t size() const { return this->get_size(buf, *this, NUM_CELLS); }
    bool empty() const { return this->is_empty(buf, *this, NUM_CELLS); }

    void clear() {
        destroy_elements();
        this->clear_size();

        memset(static_cast<void*>(buf), 0, NUM_CELLS * sizeof(*buf));
    }

    /// After executing this function, the table can only be destroyed,
    ///  and also you can use the methods `size`, `empty`, `begin`, `end`.
    void clear_and_shrink() {
        destroy_elements();
        this->clear_size();
        free();
    }

    size_t get_buffer_size_in_bytes() const { return NUM_CELLS * sizeof(Cell); }

    size_t get_buffer_size_in_cells() const { return NUM_CELLS; }

    /// Return offset for result in internal buffer.
    /// Result can have value up to `getBufferSizeInCells() + 1`
    /// because offset for zero value considered to be 0
    /// and for other values it will be `offset in buffer + 1`
    size_t offset_internal(ConstLookupResult ptr) const {
        if (ptr->is_zero(*this)) return 0;
        return ptr - buf + 1;
    }

    const Cell* data() const { return buf; }
    Cell* data() { return buf; }

#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
    size_t get_collisions() const { return 0; }
#endif
};
