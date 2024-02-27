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

#include "common/object_pool.h"
#include "exprs/runtime_filter.h"
#include "runtime/decimalv2_value.h"
#include "runtime/define_primitive_type.h"
#include "runtime/primitive_type.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/common/hash_table/phmap_fwd_decl.h"
#include "vec/common/string_ref.h"

namespace doris {

constexpr int FIXED_CONTAINER_MAX_SIZE = 8;

/**
 * Fix Container can use simd to improve performance. 1 <= N <= 8 can be improved performance by test. FIXED_CONTAINER_MAX_SIZE = 8.
 * @tparam T Element Type
 * @tparam N Fixed Number
 */
template <typename T, size_t N>
class FixedContainer {
public:
    using Self = FixedContainer;
    using ElementType = T;

    class Iterator;

    FixedContainer() { static_assert(N >= 0 && N <= FIXED_CONTAINER_MAX_SIZE); }

    ~FixedContainer() = default;

    void insert(const T& value) {
        DCHECK(_size < N);
        _data[_size++] = value;
    }

    void insert(Iterator begin, Iterator end) {
        for (auto iter = begin; iter != end; ++iter) {
            DCHECK(_size < N);
            _data[_size++] = (*iter);
        }
    }

    // Use '|' instead of '||' has better performance by test.
    ALWAYS_INLINE bool find(const T& value) const {
        if constexpr (N == 0) {
            return false;
        }
        if constexpr (N == 1) {
            return (value == _data[0]);
        }
        if constexpr (N == 2) {
            return (uint8_t)(value == _data[0]) | (uint8_t)(value == _data[1]);
        }
        if constexpr (N == 3) {
            return (uint8_t)(value == _data[0]) | (uint8_t)(value == _data[1]) |
                   (uint8_t)(value == _data[2]);
        }
        if constexpr (N == 4) {
            return (uint8_t)(value == _data[0]) | (uint8_t)(value == _data[1]) |
                   (uint8_t)(value == _data[2]) | (uint8_t)(value == _data[3]);
        }
        if constexpr (N == 5) {
            return (uint8_t)(value == _data[0]) | (uint8_t)(value == _data[1]) |
                   (uint8_t)(value == _data[2]) | (uint8_t)(value == _data[3]) |
                   (uint8_t)(value == _data[4]);
        }
        if constexpr (N == 6) {
            return (uint8_t)(value == _data[0]) | (uint8_t)(value == _data[1]) |
                   (uint8_t)(value == _data[2]) | (uint8_t)(value == _data[3]) |
                   (uint8_t)(value == _data[4]) | (uint8_t)(value == _data[5]);
        }
        if constexpr (N == 7) {
            return (uint8_t)(value == _data[0]) | (uint8_t)(value == _data[1]) |
                   (uint8_t)(value == _data[2]) | (uint8_t)(value == _data[3]) |
                   (uint8_t)(value == _data[4]) | (uint8_t)(value == _data[5]) |
                   (uint8_t)(value == _data[6]);
        }
        if constexpr (N == FIXED_CONTAINER_MAX_SIZE) {
            return (uint8_t)(value == _data[0]) | (uint8_t)(value == _data[1]) |
                   (uint8_t)(value == _data[2]) | (uint8_t)(value == _data[3]) |
                   (uint8_t)(value == _data[4]) | (uint8_t)(value == _data[5]) |
                   (uint8_t)(value == _data[6]) | (uint8_t)(value == _data[7]);
        }
        CHECK(false) << "unreachable path";
        return false;
    }

    size_t size() const { return _size; }

    class Iterator {
    public:
        explicit Iterator(std::array<T, N>& data, size_t index) : _data(data), _index(index) {}
        Iterator& operator++() {
            ++_index;
            return *this;
        }
        Iterator operator++(int) {
            Iterator ret_val = *this;
            ++(*this);
            return ret_val;
        }
        bool operator==(Iterator other) const { return _index == other._index; }
        bool operator!=(Iterator other) const { return !(*this == other); }
        T& operator*() const { return _data[_index]; }

        T* operator->() const { return &operator*(); }

        // iterator traits
        using iterator_category = std::forward_iterator_tag;
        using difference_type = std::ptrdiff_t;
        using value_type = T;
        using pointer = T*;
        using reference = T&;

    private:
        std::array<T, N>& _data;
        size_t _index;
    };
    Iterator begin() { return Iterator(_data, 0); }
    Iterator end() { return Iterator(_data, _size); }

private:
    std::array<T, N> _data;
    size_t _size {};
};

/**
 * Dynamic Container uses phmap::flat_hash_set.
 * @tparam T Element Type
 */
template <typename T>
class DynamicContainer {
public:
    using Self = DynamicContainer;
    using Iterator = typename vectorized::flat_hash_set<T>::iterator;
    using ElementType = T;

    DynamicContainer() = default;
    ~DynamicContainer() = default;

    void insert(const T& value) { _set.insert(value); }

    void insert(Iterator begin, Iterator end) { _set.insert(begin, end); }

    bool find(const T& value) const { return _set.contains(value); }

    Iterator begin() { return _set.begin(); }

    Iterator end() { return _set.end(); }

    size_t size() const { return _set.size(); }

private:
    vectorized::flat_hash_set<T> _set;
};

// TODO Maybe change void* parameter to template parameter better.
class HybridSetBase : public FilterFuncBase {
public:
    HybridSetBase() = default;
    virtual ~HybridSetBase() = default;
    virtual void insert(const void* data) = 0;
    // use in vectorize execute engine
    virtual void insert(void* data, size_t) = 0;

    virtual void insert_fixed_len(const vectorized::ColumnPtr& column, size_t start) = 0;

    virtual void insert(HybridSetBase* set) {
        HybridSetBase::IteratorBase* iter = set->begin();
        while (iter->has_next()) {
            const void* value = iter->get_value();
            insert(value);
            iter->next();
        }
    }

    virtual int size() = 0;
    virtual bool find(const void* data) const = 0;
    // use in vectorize execute engine
    virtual bool find(const void* data, size_t) const = 0;

    virtual void find_batch(const doris::vectorized::IColumn& column, size_t rows,
                            doris::vectorized::ColumnUInt8::Container& results) {
        LOG(FATAL) << "HybridSetBase not support find_batch";
    }

    virtual void find_batch_negative(const doris::vectorized::IColumn& column, size_t rows,
                                     doris::vectorized::ColumnUInt8::Container& results) {
        LOG(FATAL) << "HybridSetBase not support find_batch_negative";
    }

    virtual void find_batch_nullable(const doris::vectorized::IColumn& column, size_t rows,
                                     const doris::vectorized::NullMap& null_map,
                                     doris::vectorized::ColumnUInt8::Container& results) {
        LOG(FATAL) << "HybridSetBase not support find_batch_nullable";
    }

    virtual void find_batch_nullable_negative(const doris::vectorized::IColumn& column, size_t rows,
                                              const doris::vectorized::NullMap& null_map,
                                              doris::vectorized::ColumnUInt8::Container& results) {
        LOG(FATAL) << "HybridSetBase not support find_batch_nullable_negative";
    }

    class IteratorBase {
    public:
        IteratorBase() = default;
        virtual ~IteratorBase() = default;
        virtual const void* get_value() = 0;
        virtual bool has_next() const = 0;
        virtual void next() = 0;
    };

    virtual IteratorBase* begin() = 0;
};

template <typename Type>
const Type* check_and_get_hybrid_set(const HybridSetBase& column) {
    return typeid_cast<const Type*>(&column);
}

template <typename Type>
const Type* check_and_get_hybrid_set(const HybridSetBase* column) {
    return typeid_cast<const Type*>(column);
}

template <typename Type>
bool check_hybrid_set(const HybridSetBase& column) {
    return check_and_get_hybrid_set<Type>(&column);
}

template <typename Type>
bool check_hybrid_set(const HybridSetBase* column) {
    return check_and_get_hybrid_set<Type>(column);
}

template <PrimitiveType T,
          typename _ContainerType = DynamicContainer<typename PrimitiveTypeTraits<T>::CppType>,
          typename _ColumnType = typename PrimitiveTypeTraits<T>::ColumnType>
class HybridSet : public HybridSetBase {
public:
    using ContainerType = _ContainerType;
    using ElementType = typename ContainerType::ElementType;
    using ColumnType = _ColumnType;

    HybridSet() = default;

    ~HybridSet() override = default;

    void insert(const void* data) override {
        if (data == nullptr) {
            return;
        }
        _set.insert(*reinterpret_cast<const ElementType*>(data));
    }
    void insert(void* data, size_t /*unused*/) override { insert(data); }

    void insert_fixed_len(const vectorized::ColumnPtr& column, size_t start) override {
        if (column->is_nullable()) {
            const auto* nullable = assert_cast<const vectorized::ColumnNullable*>(column.get());
            const auto& col = nullable->get_nested_column();
            const auto& nullmap =
                    assert_cast<const vectorized::ColumnUInt8&>(nullable->get_null_map_column())
                            .get_data();

            const ElementType* data = (ElementType*)col.get_raw_data().data;
            for (size_t i = start; i < column->size(); i++) {
                if (!nullmap[i]) {
                    _set.insert(*(data + i));
                }
            }
        } else {
            const ElementType* data = (ElementType*)column->get_raw_data().data;
            for (size_t i = start; i < column->size(); i++) {
                _set.insert(*(data + i));
            }
        }
    }

    int size() override { return _set.size(); }

    bool find(const void* data) const override {
        if (data == nullptr) {
            return false;
        }

        return _set.find(*reinterpret_cast<const ElementType*>(data));
    }

    bool find(const void* data, size_t /*unused*/) const override { return find(data); }

    void find_batch(const doris::vectorized::IColumn& column, size_t rows,
                    doris::vectorized::ColumnUInt8::Container& results) override {
        _find_batch<false, false>(column, rows, nullptr, results);
    }

    void find_batch_negative(const doris::vectorized::IColumn& column, size_t rows,
                             doris::vectorized::ColumnUInt8::Container& results) override {
        _find_batch<false, true>(column, rows, nullptr, results);
    }

    void find_batch_nullable(const doris::vectorized::IColumn& column, size_t rows,
                             const doris::vectorized::NullMap& null_map,
                             doris::vectorized::ColumnUInt8::Container& results) override {
        _find_batch<true, false>(column, rows, &null_map, results);
    }

    void find_batch_nullable_negative(const doris::vectorized::IColumn& column, size_t rows,
                                      const doris::vectorized::NullMap& null_map,
                                      doris::vectorized::ColumnUInt8::Container& results) override {
        _find_batch<true, true>(column, rows, &null_map, results);
    }

    template <bool is_nullable, bool is_negative>
    void _find_batch(const doris::vectorized::IColumn& column, size_t rows,
                     const doris::vectorized::NullMap* null_map,
                     doris::vectorized::ColumnUInt8::Container& results) {
        auto& col = assert_cast<const ColumnType&>(column);
        const auto* __restrict data = (ElementType*)col.get_data().data();
        const uint8_t* __restrict null_map_data;
        if constexpr (is_nullable) {
            null_map_data = null_map->data();
        }
        auto* __restrict result_data = results.data();
        for (size_t i = 0; i < rows; ++i) {
            if constexpr (!is_nullable && !is_negative) {
                result_data[i] = _set.find(data[i]);
            } else if constexpr (!is_nullable && is_negative) {
                result_data[i] = !_set.find(data[i]);
            } else if constexpr (is_nullable && !is_negative) {
                result_data[i] = _set.find(data[i]) & (!null_map_data[i]);
            } else { // (is_nullable && is_negative)
                result_data[i] = !(_set.find(data[i]) & (!null_map_data[i]));
            }
        }
    }

    class Iterator : public IteratorBase {
    public:
        Iterator(typename ContainerType::Iterator begin, typename ContainerType::Iterator end)
                : _begin(begin), _end(end) {}
        ~Iterator() override = default;
        bool has_next() const override { return !(_begin == _end); }
        const void* get_value() override { return _begin.operator->(); }
        void next() override { ++_begin; }

    private:
        typename ContainerType::Iterator _begin;
        typename ContainerType::Iterator _end;
    };

    IteratorBase* begin() override {
        return _pool.add(new (std::nothrow) Iterator(_set.begin(), _set.end()));
    }

    ContainerType* get_inner_set() { return &_set; }

private:
    ContainerType _set;
    ObjectPool _pool;
};

template <typename _ContainerType = DynamicContainer<std::string>>
class StringSet : public HybridSetBase {
public:
    using ContainerType = _ContainerType;

    StringSet() = default;

    ~StringSet() override = default;

    void insert(const void* data) override {
        if (data == nullptr) {
            return;
        }

        const auto* value = reinterpret_cast<const StringRef*>(data);
        std::string str_value(value->data, value->size);
        _set.insert(str_value);
    }

    void insert(void* data, size_t size) override {
        std::string str_value(reinterpret_cast<char*>(data), size);
        _set.insert(str_value);
    }

    void insert_fixed_len(const vectorized::ColumnPtr& column, size_t start) override {
        if (column->is_nullable()) {
            const auto* nullable = assert_cast<const vectorized::ColumnNullable*>(column.get());
            const auto& col =
                    assert_cast<const vectorized::ColumnString&>(nullable->get_nested_column());
            const auto& nullmap =
                    assert_cast<const vectorized::ColumnUInt8&>(nullable->get_null_map_column())
                            .get_data();

            for (size_t i = start; i < column->size(); i++) {
                if (!nullmap[i]) {
                    _set.insert(col.get_data_at(i).to_string());
                }
            }
        } else {
            const auto& col = assert_cast<const vectorized::ColumnString*>(column.get());
            for (size_t i = start; i < column->size(); i++) {
                _set.insert(col->get_data_at(i).to_string());
            }
        }
    }

    int size() override { return _set.size(); }

    bool find(const void* data) const override {
        if (data == nullptr) {
            return false;
        }

        const auto* value = reinterpret_cast<const StringRef*>(data);
        std::string str_value(const_cast<const char*>(value->data), value->size);
        return _set.find(str_value);
    }

    bool find(const void* data, size_t size) const override {
        std::string str_value(reinterpret_cast<const char*>(data), size);
        return _set.find(str_value);
    }

    void find_batch(const doris::vectorized::IColumn& column, size_t rows,
                    doris::vectorized::ColumnUInt8::Container& results) override {
        _find_batch<false, false>(column, rows, nullptr, results);
    }

    void find_batch_negative(const doris::vectorized::IColumn& column, size_t rows,
                             doris::vectorized::ColumnUInt8::Container& results) override {
        _find_batch<false, true>(column, rows, nullptr, results);
    }

    void find_batch_nullable(const doris::vectorized::IColumn& column, size_t rows,
                             const doris::vectorized::NullMap& null_map,
                             doris::vectorized::ColumnUInt8::Container& results) override {
        _find_batch<true, false>(column, rows, &null_map, results);
    }

    void find_batch_nullable_negative(const doris::vectorized::IColumn& column, size_t rows,
                                      const doris::vectorized::NullMap& null_map,
                                      doris::vectorized::ColumnUInt8::Container& results) override {
        _find_batch<true, true>(column, rows, &null_map, results);
    }

    template <bool is_nullable, bool is_negative>
    void _find_batch(const doris::vectorized::IColumn& column, size_t rows,
                     const doris::vectorized::NullMap* null_map,
                     doris::vectorized::ColumnUInt8::Container& results) {
        const auto& col = assert_cast<const doris::vectorized::ColumnString&>(column);
        const uint8_t* __restrict null_map_data;
        if constexpr (is_nullable) {
            null_map_data = null_map->data();
        }
        auto* __restrict result_data = results.data();
        for (size_t i = 0; i < rows; ++i) {
            const auto& string_data = col.get_data_at(i).to_string();
            if constexpr (!is_nullable && !is_negative) {
                result_data[i] = _set.find(string_data);
            } else if constexpr (!is_nullable && is_negative) {
                result_data[i] = !_set.find(string_data);
            } else if constexpr (is_nullable && !is_negative) {
                result_data[i] = _set.find(string_data) & (!null_map_data[i]);
            } else { // (is_nullable && is_negative)
                result_data[i] = !(_set.find(string_data) & (!null_map_data[i]));
            }
        }
    }

    class Iterator : public IteratorBase {
    public:
        Iterator(typename ContainerType::Iterator begin, typename ContainerType::Iterator end)
                : _begin(begin), _end(end) {}
        ~Iterator() override = default;
        bool has_next() const override { return !(_begin == _end); }
        const void* get_value() override {
            _value.data = const_cast<char*>(_begin->data());
            _value.size = _begin->length();
            return &_value;
        }
        void next() override { ++_begin; }

    private:
        typename ContainerType::Iterator _begin;
        typename ContainerType::Iterator _end;
        StringRef _value;
    };

    IteratorBase* begin() override {
        return _pool.add(new (std::nothrow) Iterator(_set.begin(), _set.end()));
    }

    ContainerType* get_inner_set() { return &_set; }

private:
    ContainerType _set;
    ObjectPool _pool;
};

// note: Two difference from StringSet
// 1 StringRef has better comparison performance than std::string
// 2 std::string keeps its own memory, bug StringRef just keeps ptr and len, so you the caller should manage memory of StringRef
template <typename _ContainerType = DynamicContainer<StringRef>>
class StringValueSet : public HybridSetBase {
public:
    using ContainerType = _ContainerType;

    StringValueSet() = default;

    ~StringValueSet() override = default;

    void insert(const void* data) override {
        if (data == nullptr) {
            return;
        }

        const auto* value = reinterpret_cast<const StringRef*>(data);
        StringRef sv(value->data, value->size);
        _set.insert(sv);
    }

    void insert(void* data, size_t size) override {
        StringRef sv(reinterpret_cast<char*>(data), size);
        _set.insert(sv);
    }

    void insert_fixed_len(const vectorized::ColumnPtr& column, size_t start) override {
        if (column->is_nullable()) {
            const auto* nullable = assert_cast<const vectorized::ColumnNullable*>(column.get());
            const auto& col =
                    assert_cast<const vectorized::ColumnString&>(nullable->get_nested_column());
            const auto& nullmap =
                    assert_cast<const vectorized::ColumnUInt8&>(nullable->get_null_map_column())
                            .get_data();

            for (size_t i = start; i < column->size(); i++) {
                if (!nullmap[i]) {
                    _set.insert(col.get_data_at(i));
                }
            }
        } else {
            const auto& col = assert_cast<const vectorized::ColumnString*>(column.get());
            for (size_t i = start; i < column->size(); i++) {
                _set.insert(col->get_data_at(i));
            }
        }
    }

    int size() override { return _set.size(); }

    bool find(const void* data) const override {
        if (data == nullptr) {
            return false;
        }

        const auto* value = reinterpret_cast<const StringRef*>(data);
        return _set.find(*value);
    }

    bool find(const void* data, size_t size) const override {
        if (data == nullptr) {
            return false;
        }

        StringRef sv(reinterpret_cast<const char*>(data), size);
        return _set.find(sv);
    }

    void find_batch(const doris::vectorized::IColumn& column, size_t rows,
                    doris::vectorized::ColumnUInt8::Container& results) override {
        _find_batch<false, false>(column, rows, nullptr, results);
    }

    void find_batch_negative(const doris::vectorized::IColumn& column, size_t rows,
                             doris::vectorized::ColumnUInt8::Container& results) override {
        _find_batch<false, true>(column, rows, nullptr, results);
    }

    void find_batch_nullable(const doris::vectorized::IColumn& column, size_t rows,
                             const doris::vectorized::NullMap& null_map,
                             doris::vectorized::ColumnUInt8::Container& results) override {
        _find_batch<true, false>(column, rows, &null_map, results);
    }

    void find_batch_nullable_negative(const doris::vectorized::IColumn& column, size_t rows,
                                      const doris::vectorized::NullMap& null_map,
                                      doris::vectorized::ColumnUInt8::Container& results) override {
        _find_batch<true, true>(column, rows, &null_map, results);
    }

    template <bool is_nullable, bool is_negative>
    void _find_batch(const doris::vectorized::IColumn& column, size_t rows,
                     const doris::vectorized::NullMap* null_map,
                     doris::vectorized::ColumnUInt8::Container& results) {
        const auto& col = assert_cast<const doris::vectorized::ColumnString&>(column);
        const uint32_t* __restrict offset = col.get_offsets().data();
        const uint8_t* __restrict data = col.get_chars().data();
        auto* __restrict cursor = const_cast<uint8_t*>(data);
        const uint8_t* __restrict null_map_data;
        if constexpr (is_nullable) {
            null_map_data = null_map->data();
        }
        auto* __restrict result_data = results.data();
        for (size_t i = 0; i < rows; ++i) {
            uint32_t len = offset[i] - offset[i - 1];
            if constexpr (!is_nullable && !is_negative) {
                result_data[i] = _set.find(StringRef(cursor, len));
            } else if constexpr (!is_nullable && is_negative) {
                result_data[i] = !_set.find(StringRef(cursor, len));
            } else if constexpr (is_nullable && !is_negative) {
                result_data[i] = (!null_map_data[i]) & _set.find(StringRef(cursor, len));
            } else { // (is_nullable && is_negative)
                result_data[i] = !((!null_map_data[i]) & _set.find(StringRef(cursor, len)));
            }
            cursor += len;
        }
    }

    class Iterator : public IteratorBase {
    public:
        Iterator(typename ContainerType::Iterator begin, typename ContainerType::Iterator end)
                : _begin(begin), _end(end) {}
        ~Iterator() override = default;
        bool has_next() const override { return !(_begin == _end); }
        const void* get_value() override {
            _value.data = const_cast<char*>(_begin->data);
            _value.size = _begin->size;
            return &_value;
        }
        void next() override { ++_begin; }

    private:
        typename ContainerType::Iterator _begin;
        typename ContainerType::Iterator _end;
        StringRef _value;
    };

    IteratorBase* begin() override {
        return _pool.add(new (std::nothrow) Iterator(_set.begin(), _set.end()));
    }

    ContainerType* get_inner_set() { return &_set; }

private:
    ContainerType _set;
    ObjectPool _pool;
};

} // namespace doris
