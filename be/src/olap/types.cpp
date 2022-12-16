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

#include "gen_cpp/segment_v2.pb.h"
#include "olap/tablet_schema.h"

namespace doris {

void (*FieldTypeTraits<OLAP_FIELD_TYPE_CHAR>::set_to_max)(void*) = nullptr;

static TypeInfoPtr create_type_info_ptr(const TypeInfo* type_info, bool should_reclaim_memory);

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
    case OLAP_FIELD_TYPE_JSONB:
        return true;
    default:
        return false;
    }
}

const TypeInfo* get_scalar_type_info(FieldType field_type) {
    // nullptr means that there is no TypeInfo implementation for the corresponding field_type
    static const TypeInfo* field_type_array[] = {
            nullptr,
            get_scalar_type_info<OLAP_FIELD_TYPE_TINYINT>(),
            nullptr,
            get_scalar_type_info<OLAP_FIELD_TYPE_SMALLINT>(),
            nullptr,
            get_scalar_type_info<OLAP_FIELD_TYPE_INT>(),
            get_scalar_type_info<OLAP_FIELD_TYPE_UNSIGNED_INT>(),
            get_scalar_type_info<OLAP_FIELD_TYPE_BIGINT>(),
            get_scalar_type_info<OLAP_FIELD_TYPE_UNSIGNED_BIGINT>(),
            get_scalar_type_info<OLAP_FIELD_TYPE_LARGEINT>(),
            get_scalar_type_info<OLAP_FIELD_TYPE_FLOAT>(),
            get_scalar_type_info<OLAP_FIELD_TYPE_DOUBLE>(),
            nullptr,
            get_scalar_type_info<OLAP_FIELD_TYPE_CHAR>(),
            get_scalar_type_info<OLAP_FIELD_TYPE_DATE>(),
            get_scalar_type_info<OLAP_FIELD_TYPE_DATETIME>(),
            get_scalar_type_info<OLAP_FIELD_TYPE_DECIMAL>(),
            get_scalar_type_info<OLAP_FIELD_TYPE_VARCHAR>(),
            nullptr,
            nullptr,
            nullptr,
            nullptr,
            nullptr,
            get_scalar_type_info<OLAP_FIELD_TYPE_HLL>(),
            get_scalar_type_info<OLAP_FIELD_TYPE_BOOL>(),
            get_scalar_type_info<OLAP_FIELD_TYPE_OBJECT>(),
            get_scalar_type_info<OLAP_FIELD_TYPE_STRING>(),
            get_scalar_type_info<OLAP_FIELD_TYPE_QUANTILE_STATE>(),
            get_scalar_type_info<OLAP_FIELD_TYPE_DATEV2>(),
            get_scalar_type_info<OLAP_FIELD_TYPE_DATETIMEV2>(),
            get_scalar_type_info<OLAP_FIELD_TYPE_TIMEV2>(),
            get_scalar_type_info<OLAP_FIELD_TYPE_DECIMAL32>(),
            get_scalar_type_info<OLAP_FIELD_TYPE_DECIMAL64>(),
            get_scalar_type_info<OLAP_FIELD_TYPE_DECIMAL128I>(),
            get_scalar_type_info<OLAP_FIELD_TYPE_JSONB>(),
    };
    return field_type_array[field_type];
}

#define INIT_ARRAY_TYPE_INFO_LIST(type)                                               \
    {                                                                                 \
        get_init_array_type_info<type>(0), get_init_array_type_info<type>(1),         \
                get_init_array_type_info<type>(2), get_init_array_type_info<type>(3), \
                get_init_array_type_info<type>(4), get_init_array_type_info<type>(5), \
                get_init_array_type_info<type>(6), get_init_array_type_info<type>(7), \
                get_init_array_type_info<type>(8)                                     \
    }

template <FieldType field_type>
inline const ArrayTypeInfo* get_init_array_type_info(int32_t iterations) {
    static ArrayTypeInfo nested_type_info_0(
            create_static_type_info_ptr(get_scalar_type_info<field_type>()));
    static ArrayTypeInfo nested_type_info_1(create_static_type_info_ptr(&nested_type_info_0));
    static ArrayTypeInfo nested_type_info_2(create_static_type_info_ptr(&nested_type_info_1));
    static ArrayTypeInfo nested_type_info_3(create_static_type_info_ptr(&nested_type_info_2));
    static ArrayTypeInfo nested_type_info_4(create_static_type_info_ptr(&nested_type_info_3));
    static ArrayTypeInfo nested_type_info_5(create_static_type_info_ptr(&nested_type_info_4));
    static ArrayTypeInfo nested_type_info_6(create_static_type_info_ptr(&nested_type_info_5));
    static ArrayTypeInfo nested_type_info_7(create_static_type_info_ptr(&nested_type_info_6));
    static ArrayTypeInfo nested_type_info_8(create_static_type_info_ptr(&nested_type_info_7));
    static ArrayTypeInfo* nested_type_info_array[] = {
            &nested_type_info_0, &nested_type_info_1, &nested_type_info_2,
            &nested_type_info_3, &nested_type_info_4, &nested_type_info_5,
            &nested_type_info_6, &nested_type_info_7, &nested_type_info_8};
    return nested_type_info_array[iterations];
}

const TypeInfo* get_array_type_info(FieldType leaf_type, int32_t iterations) {
    DCHECK(iterations <= 8) << "the depth of nested array type should not be larger than 8";
    static constexpr int32_t depth = 9;
    static const ArrayTypeInfo* array_type_Info_arr[][depth] = {
            {nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr},
            INIT_ARRAY_TYPE_INFO_LIST(OLAP_FIELD_TYPE_TINYINT),
            {nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr},
            INIT_ARRAY_TYPE_INFO_LIST(OLAP_FIELD_TYPE_SMALLINT),
            {nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr},
            INIT_ARRAY_TYPE_INFO_LIST(OLAP_FIELD_TYPE_INT),
            INIT_ARRAY_TYPE_INFO_LIST(OLAP_FIELD_TYPE_UNSIGNED_INT),
            INIT_ARRAY_TYPE_INFO_LIST(OLAP_FIELD_TYPE_BIGINT),
            INIT_ARRAY_TYPE_INFO_LIST(OLAP_FIELD_TYPE_UNSIGNED_BIGINT),
            INIT_ARRAY_TYPE_INFO_LIST(OLAP_FIELD_TYPE_LARGEINT),
            INIT_ARRAY_TYPE_INFO_LIST(OLAP_FIELD_TYPE_FLOAT),
            INIT_ARRAY_TYPE_INFO_LIST(OLAP_FIELD_TYPE_DOUBLE),
            {nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr},
            INIT_ARRAY_TYPE_INFO_LIST(OLAP_FIELD_TYPE_CHAR),
            INIT_ARRAY_TYPE_INFO_LIST(OLAP_FIELD_TYPE_DATE),
            INIT_ARRAY_TYPE_INFO_LIST(OLAP_FIELD_TYPE_DATETIME),
            INIT_ARRAY_TYPE_INFO_LIST(OLAP_FIELD_TYPE_DECIMAL),
            INIT_ARRAY_TYPE_INFO_LIST(OLAP_FIELD_TYPE_VARCHAR),
            {nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr},
            {nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr},
            {nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr},
            {nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr},
            {nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr},
            INIT_ARRAY_TYPE_INFO_LIST(OLAP_FIELD_TYPE_HLL),
            INIT_ARRAY_TYPE_INFO_LIST(OLAP_FIELD_TYPE_BOOL),
            INIT_ARRAY_TYPE_INFO_LIST(OLAP_FIELD_TYPE_OBJECT),
            INIT_ARRAY_TYPE_INFO_LIST(OLAP_FIELD_TYPE_STRING),
            INIT_ARRAY_TYPE_INFO_LIST(OLAP_FIELD_TYPE_QUANTILE_STATE),
            INIT_ARRAY_TYPE_INFO_LIST(OLAP_FIELD_TYPE_DATEV2),
            INIT_ARRAY_TYPE_INFO_LIST(OLAP_FIELD_TYPE_DATETIMEV2),
            INIT_ARRAY_TYPE_INFO_LIST(OLAP_FIELD_TYPE_TIMEV2),
            INIT_ARRAY_TYPE_INFO_LIST(OLAP_FIELD_TYPE_DECIMAL32),
            INIT_ARRAY_TYPE_INFO_LIST(OLAP_FIELD_TYPE_DECIMAL64),
            INIT_ARRAY_TYPE_INFO_LIST(OLAP_FIELD_TYPE_DECIMAL128I),
            INIT_ARRAY_TYPE_INFO_LIST(OLAP_FIELD_TYPE_JSONB),
    };
    return array_type_Info_arr[leaf_type][iterations];
}

// TODO: Support the type info of the nested array with more than 9 depths.
TypeInfoPtr get_type_info(segment_v2::ColumnMetaPB* column_meta_pb) {
    FieldType type = (FieldType)column_meta_pb->type();
    if (UNLIKELY(type == OLAP_FIELD_TYPE_ARRAY)) {
        int32_t iterations = 0;
        const auto* child_column = &column_meta_pb->children_columns(0);
        while (child_column->type() == OLAP_FIELD_TYPE_ARRAY) {
            iterations++;
            child_column = &child_column->children_columns(0);
        }
        return create_static_type_info_ptr(
                get_array_type_info((FieldType)child_column->type(), iterations));
    } else {
        return create_static_type_info_ptr(get_scalar_type_info(type));
    }
}

TypeInfoPtr create_static_type_info_ptr(const TypeInfo* type_info) {
    return create_type_info_ptr(type_info, false);
}

TypeInfoPtr create_dynamic_type_info_ptr(const TypeInfo* type_info) {
    return create_type_info_ptr(type_info, true);
}

TypeInfoPtr create_type_info_ptr(const TypeInfo* type_info, bool should_reclaim_memory) {
    if (!should_reclaim_memory) {
        return TypeInfoPtr(type_info, [](const TypeInfo*) {});
    } else {
        return TypeInfoPtr(type_info, [](const TypeInfo* type_info) { delete type_info; });
    }
}

// TODO: Support the type info of the nested array with more than 9 depths.
TypeInfoPtr get_type_info(const TabletColumn* col) {
    auto type = col->type();
    if (UNLIKELY(type == OLAP_FIELD_TYPE_ARRAY)) {
        int32_t iterations = 0;
        const auto* child_column = &col->get_sub_column(0);
        while (child_column->type() == OLAP_FIELD_TYPE_ARRAY) {
            iterations++;
            child_column = &child_column->get_sub_column(0);
        }
        return create_static_type_info_ptr(get_array_type_info(child_column->type(), iterations));
    } else {
        return create_static_type_info_ptr(get_scalar_type_info(type));
    }
}

TypeInfoPtr clone_type_info(const TypeInfo* type_info) {
    if (is_scalar_type(type_info->type())) {
        return create_static_type_info_ptr(type_info);
    } else {
        const auto array_type_info = dynamic_cast<const ArrayTypeInfo*>(type_info);
        return create_dynamic_type_info_ptr(
                new ArrayTypeInfo(clone_type_info(array_type_info->item_type_info())));
    }
}

} // namespace doris
