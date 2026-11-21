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

#include "exprs/function/flat_dictionary.h"

#include <vector>

#include "core/assert_cast.h"
#include "core/column/column.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_decimal.h" // IWYU pragma: keep
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h" // IWYU pragma: keep
#include "core/data_type/primitive_type.h"
#include "core/types.h"
#include "exec/common/template_helpers.hpp"
#include "exprs/function/dictionary.h"
#include "runtime/thread_context.h"

namespace doris {

FlatDictionary::~FlatDictionary() {
    if (_mem_tracker) {
        // These buffers were allocated under the dictionary factory's tracker; switch
        // back to it before freeing so the memory is credited to the tracker that owns
        // it, instead of whatever query/RPC tracker happens to be active on this thread
        // when the dictionary is replaced/deleted/releases its last reference.
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_mem_tracker);
        std::vector<size_t> {}.swap(_value_row_index);
        std::vector<UInt8> {}.swap(_loaded_keys);
    }
}

size_t FlatDictionary::allocated_bytes() const {
    auto vec_mem = [](const auto& vec) {
        return vec.capacity() * sizeof(typename std::decay_t<decltype(vec)>::value_type);
    };
    return IDictionary::allocated_bytes() + vec_mem(_value_row_index) + vec_mem(_loaded_keys);
}

// Invoke func with the concrete integer key column (ColumnInt8/16/32/64/128).
// Returns false if the primitive type is not an integer type.
template <typename Func>
static bool visit_int_key_column(PrimitiveType type, const IColumn* key_column, Func&& func) {
    switch (type) {
    case TYPE_TINYINT:
        func(assert_cast<const ColumnInt8*>(key_column));
        return true;
    case TYPE_SMALLINT:
        func(assert_cast<const ColumnInt16*>(key_column));
        return true;
    case TYPE_INT:
        func(assert_cast<const ColumnInt32*>(key_column));
        return true;
    case TYPE_BIGINT:
        func(assert_cast<const ColumnInt64*>(key_column));
        return true;
    case TYPE_LARGEINT:
        func(assert_cast<const ColumnInt128*>(key_column));
        return true;
    default:
        return false;
    }
}

void FlatDictionary::load_data(const ColumnPtr& key_column, const DataTypePtr& key_type,
                               const std::vector<ColumnPtr>& values_column) {
    // load value columns first (column-wise storage in the base class)
    load_values(values_column);

    const auto* real_key_column = remove_nullable(key_column).get();
    const auto rows = real_key_column->size();

    auto load_keys = [&](const auto* typed_key_column) {
        for (size_t i = 0; i < rows; i++) {
            auto raw_key = typed_key_column->get_element(i);
            // A flat dictionary key is used directly as an array index. Negative
            // keys have no valid slot; reject them as unqualified dictionary data.
            if (raw_key < 0) {
                throw doris::Exception(
                        ErrorCode::INVALID_ARGUMENT,
                        DICT_DATA_ERROR_TAG +
                                "FlatDictionary key must be non-negative, got a negative key");
            }
            // Reject keys that would force an oversized array BEFORE any resize,
            // so a single sparse-huge key cannot explode memory. The per-dict
            // memory_limit check only happens after allocation, so it is not enough.
            // NOTE: compare in 128-bit arithmetic by promoting raw_key; narrowing
            // raw_key to 64 bits first would drop high bits (e.g. 2^64 -> 0) and let an
            // out-of-range key slip through.
            if (static_cast<__int128>(raw_key) >= static_cast<__int128>(MAX_ARRAY_SIZE)) {
                throw doris::Exception(
                        ErrorCode::INVALID_ARGUMENT,
                        DICT_DATA_ERROR_TAG + "FlatDictionary key exceeds max array size {}",
                        MAX_ARRAY_SIZE);
            }
            // raw_key is now guaranteed in [0, MAX_ARRAY_SIZE), so narrowing is safe.
            auto key = static_cast<size_t>(raw_key);
            if (key >= _loaded_keys.size()) {
                _loaded_keys.resize(key + 1, false);
                _value_row_index.resize(key + 1, 0);
            }
            // Duplicate keys map ambiguously to a single slot; reject them, mirroring
            // HashMapDictionary's duplicate-key rejection.
            if (_loaded_keys[key]) {
                throw doris::Exception(
                        ErrorCode::INVALID_ARGUMENT,
                        DICT_DATA_ERROR_TAG + "The key has duplicate data in FlatDictionary");
            }
            _loaded_keys[key] = true;
            _value_row_index[key] = i;
        }
    };

    if (!visit_int_key_column(key_type->get_primitive_type(), real_key_column, load_keys)) {
        throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                               DICT_DATA_ERROR_TAG +
                                       "FlatDictionary only support integer key , input key type "
                                       "is {} ",
                               key_type->get_name());
    }
}

ColumnPtr FlatDictionary::get_column(const std::string& attribute_name,
                                     const DataTypePtr& attribute_type, const ColumnPtr& key_column,
                                     const DataTypePtr& key_type) const {
    if (have_nullable({attribute_type}) || have_nullable({key_type})) {
        throw doris::Exception(
                ErrorCode::INTERNAL_ERROR,
                "FlatDictionary get_column attribute_type or key_type must not be nullable type");
    }
    if (!is_int(key_type->get_primitive_type())) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "FlatDictionary only support integer type key , input key type is "
                               "{} ",
                               key_type->get_name());
    }

    const auto rows = key_column->size();
    MutableColumnPtr res_column = attribute_type->create_column();
    ColumnUInt8::MutablePtr res_null = ColumnUInt8::create(rows, false);
    auto& res_null_map = res_null->get_data();
    const auto& value_data = _values_data[attribute_index(attribute_name)];

    // resolve each query key to a value row index, or mark it as not found
    IColumn::Selector value_index = IColumn::Selector(rows);
    NullMap key_not_found = NullMap(rows, false);

    const auto* real_key_column = remove_nullable(key_column).get();
    const auto* null_key = check_and_get_column<ColumnNullable>(key_column.get());

    auto resolve_keys = [&](const auto* typed_key_column) {
        for (size_t i = 0; i < rows; i++) {
            if (null_key != nullptr && null_key->is_null_at(i)) {
                key_not_found[i] = true;
                continue;
            }
            auto raw_key = typed_key_column->get_element(i);
            // Compare in 128-bit arithmetic by promoting raw_key before narrowing;
            // narrowing first would let a large key (e.g. 2^64, low bits 0) alias to a
            // present slot instead of resolving to not-found.
            if (raw_key < 0 ||
                static_cast<__int128>(raw_key) >= static_cast<__int128>(_loaded_keys.size()) ||
                !_loaded_keys[static_cast<size_t>(raw_key)]) {
                key_not_found[i] = true;
            } else {
                value_index[i] =
                        static_cast<uint32_t>(_value_row_index[static_cast<size_t>(raw_key)]);
            }
        }
    };

    if (!visit_int_key_column(key_type->get_primitive_type(), real_key_column, resolve_keys)) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "FlatDictionary unexpected key type {} ",
                               key_type->get_name());
    }

    std::visit(
            [&](auto&& arg, auto value_is_nullable) {
                using ValueDataType = std::decay_t<decltype(arg)>;
                using OutputColumnType = ValueDataType::OutputColumnType;
                auto* res_real_column = assert_cast<OutputColumnType*>(res_column.get());
                const auto* value_column = arg.get();
                const auto* value_null_map = arg.get_null_map();
                for (size_t i = 0; i < rows; i++) {
                    if (key_not_found[i]) {
                        // if input key is not found, set the result column to null
                        res_real_column->insert_default();
                        res_null_map[i] = true;
                    } else {
                        set_value_data<value_is_nullable>(res_real_column, res_null_map[i],
                                                          value_column, value_null_map,
                                                          value_index[i]);
                    }
                }
            },
            value_data, attribute_nullable_variant(attribute_index(attribute_name)));

    return ColumnNullable::create(std::move(res_column), std::move(res_null));
}

} // namespace doris
