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

#include "olap/types.h"

#include <memory>

namespace doris {

void (*FieldTypeTraits<OLAP_FIELD_TYPE_CHAR>::set_to_max)(void*) = nullptr;

bool is_scalar_type(FieldType field_type) {
    switch (field_type) {
    case OLAP_FIELD_TYPE_STRUCT:
    case OLAP_FIELD_TYPE_ARRAY:
    case OLAP_FIELD_TYPE_MAP:
        return false;
    default:
        return true;
    }
}

bool is_olap_string_type(FieldType field_type) {
    switch (field_type) {
    case OLAP_FIELD_TYPE_CHAR:
    case OLAP_FIELD_TYPE_VARCHAR:
    case OLAP_FIELD_TYPE_HLL:
    case OLAP_FIELD_TYPE_OBJECT:
    case OLAP_FIELD_TYPE_STRING:
        return true;
    default:
        return false;
    }
}

const TypeInfo* get_scalar_type_info(FieldType field_type) {
    switch (field_type) {
    case OLAP_FIELD_TYPE_TINYINT:
        return get_scalar_type_info<OLAP_FIELD_TYPE_TINYINT>();
    case OLAP_FIELD_TYPE_SMALLINT:
        return get_scalar_type_info<OLAP_FIELD_TYPE_SMALLINT>();
    case OLAP_FIELD_TYPE_INT:
        return get_scalar_type_info<OLAP_FIELD_TYPE_INT>();
    case OLAP_FIELD_TYPE_UNSIGNED_INT:
        return get_scalar_type_info<OLAP_FIELD_TYPE_UNSIGNED_INT>();
    case OLAP_FIELD_TYPE_BIGINT:
        return get_scalar_type_info<OLAP_FIELD_TYPE_BIGINT>();
    case OLAP_FIELD_TYPE_UNSIGNED_BIGINT:
        return get_scalar_type_info<OLAP_FIELD_TYPE_UNSIGNED_BIGINT>();
    case OLAP_FIELD_TYPE_LARGEINT:
        return get_scalar_type_info<OLAP_FIELD_TYPE_LARGEINT>();
    case OLAP_FIELD_TYPE_FLOAT:
        return get_scalar_type_info<OLAP_FIELD_TYPE_FLOAT>();
    case OLAP_FIELD_TYPE_DOUBLE:
        return get_scalar_type_info<OLAP_FIELD_TYPE_DOUBLE>();
    case OLAP_FIELD_TYPE_DATE:
        return get_scalar_type_info<OLAP_FIELD_TYPE_DATE>();
    case OLAP_FIELD_TYPE_DATETIME:
        return get_scalar_type_info<OLAP_FIELD_TYPE_DATETIME>();
    case OLAP_FIELD_TYPE_DECIMAL:
        return get_scalar_type_info<OLAP_FIELD_TYPE_DECIMAL>();
    case OLAP_FIELD_TYPE_CHAR:
        return get_scalar_type_info<OLAP_FIELD_TYPE_CHAR>();
    case OLAP_FIELD_TYPE_VARCHAR:
        return get_scalar_type_info<OLAP_FIELD_TYPE_VARCHAR>();
    case OLAP_FIELD_TYPE_STRING:
        return get_scalar_type_info<OLAP_FIELD_TYPE_STRING>();
    case OLAP_FIELD_TYPE_BOOL:
        return get_scalar_type_info<OLAP_FIELD_TYPE_BOOL>();
    case OLAP_FIELD_TYPE_HLL:
        return get_scalar_type_info<OLAP_FIELD_TYPE_HLL>();
    case OLAP_FIELD_TYPE_OBJECT:
        return get_scalar_type_info<OLAP_FIELD_TYPE_OBJECT>();
    case OLAP_FIELD_TYPE_QUANTILE_STATE:
        return get_scalar_type_info<OLAP_FIELD_TYPE_QUANTILE_STATE>();
    default:
        DCHECK(false) << "Bad field type: " << field_type;
        break;
    }
    return nullptr;
}

#define INIT_ARRAY_TYPE_INFO_LIST(type)                        \
    {                                                          \
        *get_init_array_type_info<type>(0),                    \
        *get_init_array_type_info<type>(1),                    \
        *get_init_array_type_info<type>(2),                    \
        *get_init_array_type_info<type>(3),                    \
        *get_init_array_type_info<type>(4),                    \
        *get_init_array_type_info<type>(5),                    \
        *get_init_array_type_info<type>(6),                    \
        *get_init_array_type_info<type>(7),                    \
        *get_init_array_type_info<type>(8)                     \
    }

template <FieldType field_type>
inline const ArrayTypeInfo* get_init_array_type_info(int32_t iterations) {
    static ArrayTypeInfo info0(get_scalar_type_info<field_type>());
    static ArrayTypeInfo info1(&info0);
    static ArrayTypeInfo info2(&info1);
    static ArrayTypeInfo info3(&info2);
    static ArrayTypeInfo info4(&info3);
    static ArrayTypeInfo info5(&info4);
    static ArrayTypeInfo info6(&info5);
    static ArrayTypeInfo info7(&info6);
    static ArrayTypeInfo info8(&info7);
    switch (iterations) {
        case 0:
            return &info0;
        case 1:
            return &info1;
        case 2:
            return &info2;
        case 3:
            return &info3;
        case 4:
            return &info4;
        case 5:
            return &info5;
        case 6:
            return &info6;
        case 7:
            return &info7;
        case 8:
            return &info8;
        default:
            DCHECK(false) << "the depth of nested array type should not be less than 0 and larger than 9";
    }
    return nullptr;
}

const TypeInfo* get_array_type_info(FieldType leaf_type, int32_t iterations) {
    DCHECK(iterations <= 8) << "the depth of nested array type should not be larger than 9";
    static constexpr int32_t depth = 9;
    static ArrayTypeInfo tinyint_type_info[depth] = INIT_ARRAY_TYPE_INFO_LIST(OLAP_FIELD_TYPE_TINYINT);
    static ArrayTypeInfo smallint_type_info[depth] = INIT_ARRAY_TYPE_INFO_LIST(OLAP_FIELD_TYPE_SMALLINT);
    static ArrayTypeInfo int_type_info[depth] = INIT_ARRAY_TYPE_INFO_LIST(OLAP_FIELD_TYPE_INT);
    static ArrayTypeInfo unsigned_int_type_info[depth] = INIT_ARRAY_TYPE_INFO_LIST(OLAP_FIELD_TYPE_UNSIGNED_INT);
    static ArrayTypeInfo bool_type_info[depth] = INIT_ARRAY_TYPE_INFO_LIST(OLAP_FIELD_TYPE_BOOL);
    static ArrayTypeInfo bigint_type_info[depth] = INIT_ARRAY_TYPE_INFO_LIST(OLAP_FIELD_TYPE_BIGINT);;
    static ArrayTypeInfo largeint_type_info[depth] = INIT_ARRAY_TYPE_INFO_LIST(OLAP_FIELD_TYPE_LARGEINT);
    static ArrayTypeInfo float_type_info[depth] = INIT_ARRAY_TYPE_INFO_LIST(OLAP_FIELD_TYPE_FLOAT);
    static ArrayTypeInfo double_type_info[depth] = INIT_ARRAY_TYPE_INFO_LIST(OLAP_FIELD_TYPE_DOUBLE);
    static ArrayTypeInfo decimal_type_info[depth] = INIT_ARRAY_TYPE_INFO_LIST(OLAP_FIELD_TYPE_DECIMAL);
    static ArrayTypeInfo date_type_info[depth] = INIT_ARRAY_TYPE_INFO_LIST(OLAP_FIELD_TYPE_DATE);
    static ArrayTypeInfo datetime_type_info[depth] = INIT_ARRAY_TYPE_INFO_LIST(OLAP_FIELD_TYPE_DATETIME);;
    static ArrayTypeInfo char_type_info[depth] = INIT_ARRAY_TYPE_INFO_LIST(OLAP_FIELD_TYPE_CHAR);
    static ArrayTypeInfo varchar_type_info[depth] = INIT_ARRAY_TYPE_INFO_LIST(OLAP_FIELD_TYPE_VARCHAR);
    static ArrayTypeInfo string_type_info[depth] = INIT_ARRAY_TYPE_INFO_LIST(OLAP_FIELD_TYPE_STRING);
    switch (leaf_type) {
    case OLAP_FIELD_TYPE_TINYINT:
        return &tinyint_type_info[iterations];
    case OLAP_FIELD_TYPE_SMALLINT:
        return &smallint_type_info[iterations];
    case OLAP_FIELD_TYPE_INT:
        return &int_type_info[iterations];
    case OLAP_FIELD_TYPE_UNSIGNED_INT:
        return &unsigned_int_type_info[iterations];
    case OLAP_FIELD_TYPE_BOOL:
        return &bool_type_info[iterations];
    case OLAP_FIELD_TYPE_BIGINT:
        return &bigint_type_info[iterations];
    case OLAP_FIELD_TYPE_LARGEINT:
        return &largeint_type_info[iterations];
    case OLAP_FIELD_TYPE_FLOAT:
        return &float_type_info[iterations];
    case OLAP_FIELD_TYPE_DOUBLE:
        return &double_type_info[iterations];
    case OLAP_FIELD_TYPE_DECIMAL:
        return &decimal_type_info[iterations];
    case OLAP_FIELD_TYPE_DATE:
        return &date_type_info[iterations];
    case OLAP_FIELD_TYPE_DATETIME:
        return &datetime_type_info[iterations];
    case OLAP_FIELD_TYPE_CHAR:
        return &char_type_info[iterations];
    case OLAP_FIELD_TYPE_VARCHAR:
        return &varchar_type_info[iterations];
    case OLAP_FIELD_TYPE_STRING:
        return &string_type_info[iterations];
    default:
        DCHECK(false) << "Bad field type: " << leaf_type;
    }
    return nullptr;
}

const TypeInfo* get_type_info(segment_v2::ColumnMetaPB* column_meta_pb) {
    FieldType type = (FieldType) column_meta_pb->type();
    switch (type) {
    case OLAP_FIELD_TYPE_ARRAY: {
        int32_t iterations = 0;
        const auto* child_column = &column_meta_pb->children_columns(0);
        while (child_column->type() == OLAP_FIELD_TYPE_ARRAY) {
            iterations++;
            child_column = &child_column->children_columns(0);
        }
        return get_array_type_info((FieldType) child_column->type(), iterations);
    }
    [[likely]] default:
        return get_scalar_type_info(type);
    }
}

const TypeInfo* get_type_info(const TabletColumn* col) {
    auto type = col->type();
    switch (type) {
    case OLAP_FIELD_TYPE_ARRAY: {
        int32_t iterations = 0;
        const auto* child_column = &col->get_sub_column(0);
        while (child_column->type() == OLAP_FIELD_TYPE_ARRAY) {
            iterations++;
            child_column = &child_column->get_sub_column(0);
        }
        return get_array_type_info(child_column->type(), iterations);
    }
    [[likely]] default:
        return get_scalar_type_info(type);
    }
}

} // namespace doris
