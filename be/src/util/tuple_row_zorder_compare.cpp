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

#include "util/tuple_row_zorder_compare.h"

namespace doris {

    RowComparator::RowComparator(Schema* schema) {

    }

    int RowComparator::operator()(const char* left, const char* right) const {
        return -1;
    }

    TupleRowZOrderComparator::TupleRowZOrderComparator() {
        _schema = nullptr;
        _sort_col_num = 0;
    }

    TupleRowZOrderComparator::TupleRowZOrderComparator(int sort_col_num) {
        _schema = nullptr;
        _sort_col_num = sort_col_num;
    }

    TupleRowZOrderComparator::TupleRowZOrderComparator(Schema* schema, int sort_col_num)
                            :_schema(schema), _sort_col_num(sort_col_num) {
        _max_col_size = get_type_byte_size(_schema->column(0)->type());
        for (size_t i = 1; i < _sort_col_num; ++i) {
            if (_max_col_size <  get_type_byte_size(_schema->column(i)->type())) {
                _max_col_size =  get_type_byte_size(_schema->column(i)->type());
            }
        }
    }

    int TupleRowZOrderComparator::compare(const char* lhs, const char* rhs) const {
        ContiguousRow lhs_row(_schema, lhs);
        ContiguousRow rhs_row(_schema, rhs);
        if (_max_col_size <= 4) {
            return compare_based_on_size<uint32_t, ContiguousRow>(lhs_row, rhs_row);
        } else if (_max_col_size <= 8) {
            return compare_based_on_size<uint64_t, ContiguousRow>(lhs_row, rhs_row);
        } else {
            return compare_based_on_size<uint128_t, ContiguousRow>(lhs_row, rhs_row);
        }
    }

    void TupleRowZOrderComparator::max_col_size(const RowCursor& rc) {
        _max_col_size = get_type_byte_size(rc.schema()->column(0)->type());
        for (size_t i = 1; i < _sort_col_num; ++i) {
            if (_max_col_size <  get_type_byte_size(rc.schema()->column(i)->type())) {
                _max_col_size =  get_type_byte_size(rc.schema()->column(i)->type());
            }
        }
    }

    int TupleRowZOrderComparator::compare_row(const RowCursor& lhs, const RowCursor& rhs) {
        max_col_size(lhs);
        if (_max_col_size <= 4) {
            return compare_based_on_size<uint32_t, const RowCursor>(lhs, rhs);
        } else if (_max_col_size <= 8) {
            return compare_based_on_size<uint64_t, const RowCursor>(lhs, lhs);
        } else {
            return compare_based_on_size<uint128_t, const RowCursor>(lhs, lhs);
        }
    }

    template<typename U, typename LhsRowType>
    int TupleRowZOrderComparator::compare_based_on_size(LhsRowType& lhs, LhsRowType& rhs) const {
        auto less_msb = [](U x, U y) { return x < y && x < (x ^ y); };
        FieldType type = lhs.schema()->column(0)->type();
        U msd_lhs = get_shared_representation<U>(lhs.cell(0).is_null() ? nullptr : lhs.cell(0).cell_ptr(),
                                                 type);
        U msd_rhs = get_shared_representation<U>(rhs.cell(0).is_null() ? nullptr : rhs.cell(0).cell_ptr(),
                                                 type);
        for (int i = 1; i < _sort_col_num; ++i) {
            type = lhs.schema()->column(i)->type();
            const void *lhs_v = lhs.cell(i).is_null() ? nullptr : lhs.cell(i).cell_ptr();
            const void *rhs_v = rhs.cell(i).is_null() ? nullptr : rhs.cell(i).cell_ptr();
            U lhsi = get_shared_representation<U>(lhs_v, type);
            U rhsi = get_shared_representation<U>(rhs_v, type);
            if (less_msb(msd_lhs ^ msd_rhs, lhsi ^ rhsi)) {
                msd_lhs = lhsi;
                msd_rhs = rhsi;
            }
        }
        return msd_lhs < msd_rhs ? -1 : (msd_lhs > msd_rhs ? 1 : 0);
    }

    template<typename U>
    U TupleRowZOrderComparator::get_shared_representation(const void *val, FieldType type) const {
        // The mask used for setting the sign bit correctly.
        if (val == NULL) return 0;
        constexpr U mask = (U) 1 << (sizeof(U) * 8 - 1);
        switch (type) {
            case FieldType::OLAP_FIELD_TYPE_NONE:
                return 0;
            case FieldType::OLAP_FIELD_TYPE_BOOL:
                return static_cast<U>(*reinterpret_cast<const bool *>(val)) << (sizeof(U) * 8 - 1);
            case FieldType::OLAP_FIELD_TYPE_UNSIGNED_TINYINT:
            case FieldType::OLAP_FIELD_TYPE_TINYINT:
                return get_shared_int_representation<U, int8_t>(
                        *reinterpret_cast<const int8_t *>(val), mask);
            case FieldType::OLAP_FIELD_TYPE_SMALLINT:
            case FieldType::OLAP_FIELD_TYPE_UNSIGNED_SMALLINT:
                return get_shared_int_representation<U, int16_t>(
                        *reinterpret_cast<const int16_t *>(val), mask);
            case FieldType::OLAP_FIELD_TYPE_INT:
                return get_shared_int_representation<U, int32_t>(
                        *reinterpret_cast<const int32_t *>(val), mask);
            case FieldType::OLAP_FIELD_TYPE_DATETIME:
            case FieldType::OLAP_FIELD_TYPE_DATE:
            case FieldType::OLAP_FIELD_TYPE_BIGINT:
            case FieldType::OLAP_FIELD_TYPE_UNSIGNED_BIGINT:
                return get_shared_int_representation<U, int64_t>(
                        *reinterpret_cast<const int64_t *>(val), mask);
            case FieldType::OLAP_FIELD_TYPE_LARGEINT:
                return static_cast<U>(*reinterpret_cast<const int128_t *>(val)) ^ mask;
            case FieldType::OLAP_FIELD_TYPE_FLOAT:
                return get_shared_float_representation<U, float>(val, mask);
            case FieldType::OLAP_FIELD_TYPE_DISCRETE_DOUBLE:
            case FieldType::OLAP_FIELD_TYPE_DOUBLE:
                return get_shared_float_representation<U, double>(val, mask);
            case FieldType::OLAP_FIELD_TYPE_CHAR:
            case FieldType::OLAP_FIELD_TYPE_VARCHAR:{
                const StringValue *string_value = reinterpret_cast<const StringValue *>(val);
                return get_shared_string_representation<U>(string_value->ptr, string_value->len);
            }
            case FieldType::OLAP_FIELD_TYPE_DECIMAL: {
                decimal12_t decimal_val = *reinterpret_cast<const decimal12_t*>(val);
                int128_t value = decimal_val.integer*DecimalV2Value::ONE_BILLION + decimal_val.fraction;
                return static_cast<U>(value) ^ mask;
            }
            default:
                return 0;
        }
    }

    template<typename U, typename T>
    U inline TupleRowZOrderComparator::get_shared_int_representation(const T val, U mask) const {
        uint64_t shift_size = static_cast<uint64_t>(
                std::max(static_cast<int64_t>((sizeof(U) - sizeof(T)) * 8), (int64_t) 0));
        return (static_cast<U>(val) << shift_size) ^ mask;
    }

    template<typename U, typename T>
    U inline TupleRowZOrderComparator::get_shared_float_representation(const void *val, U mask) const {
        int64_t tmp;
        T floating_value = *reinterpret_cast<const T *>(val);
        memcpy(&tmp, &floating_value, sizeof(T));
        if (UNLIKELY(std::isnan(floating_value))) return 0;
        // "int" is enough because U and T are only primitive type
        int s = (int)((sizeof(U) - sizeof(T)) * 8);
        if (floating_value < 0.0) {
            // Flipping all bits for negative values.
            return static_cast<U>(~tmp) << std::max(s, 0);
        } else {
            // Flipping only first bit.
            return (static_cast<U>(tmp) << std::max(s, 0)) ^ mask;
        }
    }

    template<typename U>
    U inline TupleRowZOrderComparator::get_shared_string_representation(const char *char_ptr,
                                              int length) const {
        int len = length < sizeof(U) ? length : sizeof(U);
        if (len == 0) return 0;
        U dst = 0;
        // We copy the bytes from the string but swap the bytes because of integer endianness.
        BitUtil::ByteSwapScalar(&dst, char_ptr, len);
        return dst << ((sizeof(U) - len) * 8);
    }

    int TupleRowZOrderComparator::operator()(const char* lhs, const char* rhs) const {
        int result = compare(lhs, rhs);
        return result;
    }

    int TupleRowZOrderComparator::get_type_byte_size(FieldType type) const {
        switch (type) {
            case FieldType::OLAP_FIELD_TYPE_OBJECT:
            case FieldType::OLAP_FIELD_TYPE_HLL:
            case FieldType::OLAP_FIELD_TYPE_STRUCT:
            case FieldType::OLAP_FIELD_TYPE_ARRAY:
            case FieldType::OLAP_FIELD_TYPE_MAP:
            case FieldType::OLAP_FIELD_TYPE_CHAR:
            case FieldType::OLAP_FIELD_TYPE_VARCHAR:
                return 0;
            case FieldType::OLAP_FIELD_TYPE_NONE:
            case FieldType::OLAP_FIELD_TYPE_BOOL:
            case FieldType::OLAP_FIELD_TYPE_UNSIGNED_TINYINT:
            case FieldType::OLAP_FIELD_TYPE_TINYINT:
                return 1;
            case FieldType::OLAP_FIELD_TYPE_SMALLINT:
            case FieldType::OLAP_FIELD_TYPE_UNSIGNED_SMALLINT:
                return 2;
            case FieldType::OLAP_FIELD_TYPE_FLOAT:
            case FieldType::OLAP_FIELD_TYPE_INT:
            case FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT:
                return 4;
            case FieldType::OLAP_FIELD_TYPE_DISCRETE_DOUBLE:
            case FieldType::OLAP_FIELD_TYPE_DOUBLE:
            case FieldType::OLAP_FIELD_TYPE_BIGINT:
            case FieldType::OLAP_FIELD_TYPE_UNSIGNED_BIGINT:
                return 8;
            case FieldType::OLAP_FIELD_TYPE_DECIMAL:
            case FieldType::OLAP_FIELD_TYPE_LARGEINT:
            case FieldType::OLAP_FIELD_TYPE_DATETIME:
            case FieldType::OLAP_FIELD_TYPE_DATE:
                return 16;
            case FieldType::OLAP_FIELD_TYPE_UNKNOWN:
                DCHECK(false);
                break;
            default:
                DCHECK(false);
        }
        return -1;
    }

}
