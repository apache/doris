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

#include <cstring>

#include "common/cast_set.h"
#include "common/compare.h"
#include "common/logging.h"
#include "core/assert_cast.h"
#include "core/column/column.h"
#include "core/column/column_decimal.h"
#include "core/column/column_string.h"
#include "core/custom_allocator.h"
#include "core/data_type/data_type.h"
#include "core/data_type/primitive_type.h"
#include "core/string_buffer.hpp"
#include "core/string_ref.h"
#include "core/types.h"

namespace doris {

class Arena;

/// Stores one fixed-size scalar value directly in the aggregate state.
template <PrimitiveType T>
struct SingleValueDataFixed {
private:
    using Self = SingleValueDataFixed;
    using ValueType = typename PrimitiveTypeTraits<T>::CppType;

    static const ValueType& _value_at(const IColumn& column, size_t row_num) {
        return assert_cast<const typename PrimitiveTypeTraits<T>::ColumnType&,
                           TypeCheckOnRelease::DISABLE>(column)
                .get_data()[row_num];
    }

    // AggregateFunctionIf and state merging must distinguish an empty state from a default value.
    bool has_value = false;
    ValueType value {};

public:
    using ColVecType = typename PrimitiveTypeTraits<T>::ColumnType;
    static constexpr bool NeedCheckColumnType = true;

    SingleValueDataFixed() = default;
    bool has() const { return has_value; }

    static constexpr bool UsesFixedLengthStateSerialization = true;

    void set_value_to_min() { value = Compare::min_value<ValueType>(); }

    void set_value_to_max() { value = Compare::max_value<ValueType>(); }

    void insert_result_into(IColumn& to) const {
        if (has()) {
            assert_cast<typename PrimitiveTypeTraits<T>::ColumnType&, TypeCheckOnRelease::DISABLE>(
                    to)
                    .insert_value(value);
        } else {
            assert_cast<typename PrimitiveTypeTraits<T>::ColumnType&, TypeCheckOnRelease::DISABLE>(
                    to)
                    .insert_default();
        }
    }

    void reset() { has_value = false; }

    void write(BufferWritable& buf, const DataTypePtr&, int) const {
        buf.write_binary(has());
        if (has()) {
            buf.write_binary(value);
        }
    }

    void read(BufferReadable& buf, const DataTypePtr&, int, Arena&) {
        buf.read_binary(has_value);
        if (has()) {
            buf.read_binary(value);
        }
    }

    void set(const IColumn& column, size_t row_num, Arena&) {
        has_value = true;
        value = _value_at(column, row_num);
    }

    void set(const Self& to, Arena&) {
        DORIS_CHECK(to.has());
        has_value = true;
        value = to.value;
    }

    bool set_if_smaller(const IColumn& column, size_t row_num, Arena& arena) {
        if (!has() || Compare::less(_value_at(column, row_num), value)) {
            set(column, row_num, arena);
            return true;
        }
        return false;
    }

    bool set_if_smaller(const Self& to, Arena& arena) {
        if (to.has() && (!has() || Compare::less(to.value, value))) {
            set(to, arena);
            return true;
        }
        return false;
    }

    bool set_if_greater(const IColumn& column, size_t row_num, Arena& arena) {
        if (!has() || Compare::greater(_value_at(column, row_num), value)) {
            set(column, row_num, arena);
            return true;
        }
        return false;
    }

    bool is_equal_to(const IColumn& column, size_t row_num) const {
        if (!has()) {
            return false;
        }
        return Compare::equal(_value_at(column, row_num), value);
    }

    bool set_if_greater(const Self& to, Arena& arena) {
        if (to.has() && (!has() || Compare::greater(to.value, value))) {
            set(to, arena);
            return true;
        }
        return false;
    }
};

/// Stores short strings inline and allocates a separate buffer for long strings.
struct SingleValueDataString {
private:
    using Self = SingleValueDataString;
    // Keep the signed 32-bit size because -1 represents an empty state in the serialized format.
    Int32 size = -1;    /// -1 indicates that there is no value.
    Int32 capacity = 0; /// power of two or zero
    DorisUniqueBufferPtr<char> large_data;

public:
    static constexpr Int32 AUTOMATIC_STORAGE_SIZE = 64;
    static constexpr Int32 MAX_SMALL_STRING_SIZE =
            AUTOMATIC_STORAGE_SIZE - sizeof(size) - sizeof(capacity) - sizeof(large_data);

private:
    char small_data[MAX_SMALL_STRING_SIZE];

public:
    using ColVecType = ColumnString;
    static constexpr bool NeedCheckColumnType = true;

    ~SingleValueDataString() = default;

    static constexpr bool UsesFixedLengthStateSerialization = false;

    bool has() const { return size >= 0; }

private:
    static StringRef _value_at(const IColumn& column, size_t row_num) {
        return assert_cast<const ColumnString&, TypeCheckOnRelease::DISABLE>(column).get_data_at(
                row_num);
    }

    const char* _data() const {
        return size <= MAX_SMALL_STRING_SIZE ? small_data : large_data.get();
    }

    StringRef _value() const { return StringRef(_data(), size); }

    void _set(StringRef source) {
        Int32 value_size = cast_set<Int32>(source.size);
        if (value_size <= MAX_SMALL_STRING_SIZE) {
            /// Don't free large_data here.
            size = value_size;

            if (size > 0) {
                memcpy(small_data, source.data, size);
            }
        } else {
            if (capacity < value_size) {
                /// Don't free large_data here.
                capacity = (Int32)round_up_to_power_of_two_or_zero(value_size);
                large_data = DorisUniqueBufferPtr<char>(capacity);
            }

            size = value_size;
            memcpy(large_data.get(), source.data, size);
        }
    }

public:
    void insert_result_into(IColumn& to) const {
        if (has()) {
            assert_cast<ColumnString&, TypeCheckOnRelease::DISABLE>(to).insert_data(_data(), size);
        } else {
            assert_cast<ColumnString&, TypeCheckOnRelease::DISABLE>(to).insert_default();
        }
    }

    void reset() {
        size = -1;
        capacity = 0;
        large_data.reset();
    }

    void write(BufferWritable& buf, const DataTypePtr&, int) const {
        buf.write_binary(size);
        if (has()) {
            buf.write(_data(), size);
        }
    }

    void read(BufferReadable& buf, const DataTypePtr&, int, Arena&) {
        Int32 rhs_size;
        buf.read_binary(rhs_size);

        if (rhs_size >= 0) {
            if (rhs_size <= MAX_SMALL_STRING_SIZE) {
                /// Don't free large_data here.

                size = rhs_size;

                if (size > 0) {
                    buf.read(small_data, size);
                }
            } else {
                if (capacity < rhs_size) {
                    capacity = (Int32)round_up_to_power_of_two_or_zero(rhs_size);
                    large_data = DorisUniqueBufferPtr<char>(capacity);
                }

                size = rhs_size;
                buf.read(large_data.get(), size);
            }
        } else {
            /// Don't free large_data here.
            size = rhs_size;
        }
    }

    void set(const IColumn& column, size_t row_num, Arena&) { _set(_value_at(column, row_num)); }

    void set(const Self& to, Arena&) {
        DORIS_CHECK(to.has());
        _set(to._value());
    }

    bool set_if_smaller(const IColumn& column, size_t row_num, Arena& arena) {
        if (!has() || _value_at(column, row_num) < _value()) {
            set(column, row_num, arena);
            return true;
        }
        return false;
    }

    bool set_if_greater(const IColumn& column, size_t row_num, Arena& arena) {
        if (!has() || _value_at(column, row_num) > _value()) {
            set(column, row_num, arena);
            return true;
        }
        return false;
    }

    bool set_if_smaller(const Self& to, Arena& arena) {
        if (to.has() && (!has() || to._value() < _value())) {
            set(to, arena);
            return true;
        }
        return false;
    }

    bool set_if_greater(const Self& to, Arena& arena) {
        if (to.has() && (!has() || to._value() > _value())) {
            set(to, arena);
            return true;
        }
        return false;
    }

    bool is_equal_to(const IColumn& column, size_t row_num) const {
        if (!has()) {
            return false;
        }
        return _value_at(column, row_num) == _value();
    }
};

static_assert(sizeof(SingleValueDataString) == SingleValueDataString::AUTOMATIC_STORAGE_SIZE);

/// Owns a materialized one-row column for values without a dedicated representation above.
class SingleValueDataColumn {
private:
    using Self = SingleValueDataColumn;

    ColumnPtr _value;

public:
    static constexpr bool NeedCheckColumnType = false;
    static constexpr bool UsesFixedLengthStateSerialization = false;

    bool has() const { return _value.get() != nullptr; }

    size_t allocated_bytes() const { return has() ? _value->allocated_bytes() : 0; }

    void set(const IColumn& column, size_t row_num) {
        auto value = column.clone_empty();
        DCHECK(value->empty());
        value->reserve(1);
        value->insert_from(column, row_num);
        DCHECK_EQ(value->size(), 1);
        _value = std::move(value);
    }

    void insert_result_into(IColumn& to) const {
        if (has()) {
            to.insert_from(*_value, 0);
        } else {
            to.insert_default();
        }
    }

    void reset() { _value.reset(); }

    void write(BufferWritable& buf, const DataTypePtr& data_type, int be_exec_version) const {
        buf.write_binary(has());
        if (!has()) {
            return;
        }
        auto size_bytes = data_type->get_uncompressed_serialized_bytes(*_value, be_exec_version);
        buf.write_binary(size_bytes);
        buf.resize(size_bytes);
        auto* p = data_type->serialize(*_value, buf.data(), be_exec_version);
        DCHECK_EQ(p, buf.data() + size_bytes);
        buf.add_offset(size_bytes);
    }

    void read(BufferReadable& buf, const DataTypePtr& data_type, int be_exec_version, Arena&) {
        bool has_value = false;
        buf.read_binary(has_value);
        if (!has_value) {
            reset();
            return;
        }
        int64_t size = 0;
        buf.read_binary(size);
        auto value = data_type->create_column();
        value->reserve(1);
        const auto* p = data_type->deserialize(buf.data(), &value, be_exec_version);
        DCHECK_EQ(p, buf.data() + size);
        buf.add_offset(size);
        _value = std::move(value);
    }

    void set(const IColumn& column, size_t row_num, Arena&) { set(column, row_num); }

    void set(const Self& to, Arena&) {
        DORIS_CHECK(to.has());
        // Stored one-row columns are immutable, so merged states can share ownership.
        _value = to._value;
    }

    bool set_if_smaller(const IColumn& column, size_t row_num, Arena& arena) {
        if (!has() || column.compare_at(row_num, 0, *_value, 1) < 0) {
            set(column, row_num, arena);
            return true;
        }
        return false;
    }

    bool set_if_smaller(const Self& to, Arena& arena) {
        if (to.has() && (!has() || to._value->compare_at(0, 0, *_value, 1) < 0)) {
            set(to, arena);
            return true;
        }
        return false;
    }

    bool set_if_greater(const IColumn& column, size_t row_num, Arena& arena) {
        if (!has() || column.compare_at(row_num, 0, *_value, 1) > 0) {
            set(column, row_num, arena);
            return true;
        }
        return false;
    }

    bool set_if_greater(const Self& to, Arena& arena) {
        if (to.has() && (!has() || to._value->compare_at(0, 0, *_value, 1) > 0)) {
            set(to, arena);
            return true;
        }
        return false;
    }

    bool is_equal_to(const IColumn& column, size_t row_num) const {
        return has() && column.compare_at(row_num, 0, *_value, 1) == 0;
    }
};

} // namespace doris
