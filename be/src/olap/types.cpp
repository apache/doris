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

#include <gen_cpp/segment_v2.pb.h>

#include <memory>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "olap/tablet_schema.h"

namespace doris {

void (*FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_CHAR>::set_to_max)(void*) = nullptr;

static TypeInfoPtr create_type_info_ptr(const TypeInfo* type_info, bool should_reclaim_memory);

bool is_scalar_type(FieldType field_type) {
    switch (field_type) {
    case FieldType::OLAP_FIELD_TYPE_STRUCT:
    case FieldType::OLAP_FIELD_TYPE_ARRAY:
    case FieldType::OLAP_FIELD_TYPE_MAP:
    case FieldType::OLAP_FIELD_TYPE_VARIANT:
        return false;
    default:
        return true;
    }
}

bool is_olap_string_type(FieldType field_type) {
    switch (field_type) {
    case FieldType::OLAP_FIELD_TYPE_CHAR:
    case FieldType::OLAP_FIELD_TYPE_VARCHAR:
    case FieldType::OLAP_FIELD_TYPE_HLL:
    case FieldType::OLAP_FIELD_TYPE_OBJECT:
    case FieldType::OLAP_FIELD_TYPE_STRING:
    case FieldType::OLAP_FIELD_TYPE_JSONB:
    case FieldType::OLAP_FIELD_TYPE_AGG_STATE:
        return true;
    default:
        return false;
    }
}

const TypeInfo* get_scalar_type_info(FieldType field_type) {
    // nullptr means that there is no TypeInfo implementation for the corresponding field_type
    static const TypeInfo* field_type_array[] = {
            nullptr,
            get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_TINYINT>(),
            nullptr,
            get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_SMALLINT>(),
            nullptr,
            get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_INT>(),
            get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT>(),
            get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_BIGINT>(),
            get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_UNSIGNED_BIGINT>(),
            get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_LARGEINT>(),
            get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_FLOAT>(),
            get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_DOUBLE>(),
            nullptr,
            get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_CHAR>(),
            get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_DATE>(),
            get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_DATETIME>(),
            get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_DECIMAL>(),
            get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_VARCHAR>(),
            nullptr,
            nullptr,
            nullptr,
            nullptr,
            nullptr,
            get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_HLL>(),
            get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_BOOL>(),
            get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_OBJECT>(),
            get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_STRING>(),
            get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_QUANTILE_STATE>(),
            get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_DATEV2>(),
            get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_DATETIMEV2>(),
            get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_TIMEV2>(),
            get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_DECIMAL32>(),
            get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_DECIMAL64>(),
            get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_DECIMAL128I>(),
            get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_JSONB>(),
            get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_VARIANT>(),
            get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_AGG_STATE>(),
            nullptr};
    return field_type_array[int(field_type)];
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
const ArrayTypeInfo* get_init_array_type_info(int32_t iterations) {
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
            INIT_ARRAY_TYPE_INFO_LIST(FieldType::OLAP_FIELD_TYPE_TINYINT),
            {nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr},
            INIT_ARRAY_TYPE_INFO_LIST(FieldType::OLAP_FIELD_TYPE_SMALLINT),
            {nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr},
            INIT_ARRAY_TYPE_INFO_LIST(FieldType::OLAP_FIELD_TYPE_INT),
            INIT_ARRAY_TYPE_INFO_LIST(FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT),
            INIT_ARRAY_TYPE_INFO_LIST(FieldType::OLAP_FIELD_TYPE_BIGINT),
            INIT_ARRAY_TYPE_INFO_LIST(FieldType::OLAP_FIELD_TYPE_UNSIGNED_BIGINT),
            INIT_ARRAY_TYPE_INFO_LIST(FieldType::OLAP_FIELD_TYPE_LARGEINT),
            INIT_ARRAY_TYPE_INFO_LIST(FieldType::OLAP_FIELD_TYPE_FLOAT),
            INIT_ARRAY_TYPE_INFO_LIST(FieldType::OLAP_FIELD_TYPE_DOUBLE),
            {nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr},
            INIT_ARRAY_TYPE_INFO_LIST(FieldType::OLAP_FIELD_TYPE_CHAR),
            INIT_ARRAY_TYPE_INFO_LIST(FieldType::OLAP_FIELD_TYPE_DATE),
            INIT_ARRAY_TYPE_INFO_LIST(FieldType::OLAP_FIELD_TYPE_DATETIME),
            INIT_ARRAY_TYPE_INFO_LIST(FieldType::OLAP_FIELD_TYPE_DECIMAL),
            INIT_ARRAY_TYPE_INFO_LIST(FieldType::OLAP_FIELD_TYPE_VARCHAR),
            {nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr},
            {nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr},
            {nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr},
            {nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr},
            {nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr},
            INIT_ARRAY_TYPE_INFO_LIST(FieldType::OLAP_FIELD_TYPE_HLL),
            INIT_ARRAY_TYPE_INFO_LIST(FieldType::OLAP_FIELD_TYPE_BOOL),
            INIT_ARRAY_TYPE_INFO_LIST(FieldType::OLAP_FIELD_TYPE_OBJECT),
            INIT_ARRAY_TYPE_INFO_LIST(FieldType::OLAP_FIELD_TYPE_STRING),
            INIT_ARRAY_TYPE_INFO_LIST(FieldType::OLAP_FIELD_TYPE_QUANTILE_STATE),
            INIT_ARRAY_TYPE_INFO_LIST(FieldType::OLAP_FIELD_TYPE_DATEV2),
            INIT_ARRAY_TYPE_INFO_LIST(FieldType::OLAP_FIELD_TYPE_DATETIMEV2),
            INIT_ARRAY_TYPE_INFO_LIST(FieldType::OLAP_FIELD_TYPE_TIMEV2),
            INIT_ARRAY_TYPE_INFO_LIST(FieldType::OLAP_FIELD_TYPE_DECIMAL32),
            INIT_ARRAY_TYPE_INFO_LIST(FieldType::OLAP_FIELD_TYPE_DECIMAL64),
            INIT_ARRAY_TYPE_INFO_LIST(FieldType::OLAP_FIELD_TYPE_DECIMAL128I),
            INIT_ARRAY_TYPE_INFO_LIST(FieldType::OLAP_FIELD_TYPE_JSONB),
            INIT_ARRAY_TYPE_INFO_LIST(FieldType::OLAP_FIELD_TYPE_VARIANT),
            {nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr},
            INIT_ARRAY_TYPE_INFO_LIST(FieldType::OLAP_FIELD_TYPE_AGG_STATE)};
    return array_type_Info_arr[int(leaf_type)][iterations];
}

// Produce a struct type info
// TODO(xy): Need refactor to this produce method
const TypeInfo* get_struct_type_info(std::vector<FieldType> field_types) {
    std::vector<TypeInfoPtr> type_infos;
    type_infos.reserve(field_types.size());
    for (FieldType& type : field_types) {
        if (is_scalar_type(type)) {
            type_infos.push_back(create_static_type_info_ptr(get_scalar_type_info(type)));
        } else {
            // TODO(xy): Not supported nested complex type now
        }
    }
    return new StructTypeInfo(type_infos);
}

// TODO: Support the type info of the nested array with more than 9 depths.
// TODO(xy): Support the type info of the nested struct
TypeInfoPtr get_type_info(segment_v2::ColumnMetaPB* column_meta_pb) {
    FieldType type = (FieldType)column_meta_pb->type();
    if (UNLIKELY(type == FieldType::OLAP_FIELD_TYPE_STRUCT)) {
        std::vector<FieldType> field_types;
        for (uint32_t i = 0; i < column_meta_pb->children_columns_size(); i++) {
            const auto* child_column = &column_meta_pb->children_columns(i);
            field_types.push_back((FieldType)child_column->type());
        }
        return create_dynamic_type_info_ptr(get_struct_type_info(field_types));
    } else if (UNLIKELY(type == FieldType::OLAP_FIELD_TYPE_ARRAY)) {
        segment_v2::ColumnMetaPB child_column = column_meta_pb->children_columns(0);
        TypeInfoPtr child_info = get_type_info(&child_column);
        ArrayTypeInfo* array_type_info = new ArrayTypeInfo(std::move(child_info));
        return create_dynamic_type_info_ptr(array_type_info);
    } else if (UNLIKELY(type == FieldType::OLAP_FIELD_TYPE_MAP)) {
        segment_v2::ColumnMetaPB key_meta = column_meta_pb->children_columns(0);
        TypeInfoPtr key_type_info = get_type_info(&key_meta);
        segment_v2::ColumnMetaPB value_meta = column_meta_pb->children_columns(1);
        TypeInfoPtr value_type_info = get_type_info(&value_meta);

        MapTypeInfo* map_type_info =
                new MapTypeInfo(std::move(key_type_info), std::move(value_type_info));
        return create_dynamic_type_info_ptr(map_type_info);
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
    if (UNLIKELY(type == FieldType::OLAP_FIELD_TYPE_STRUCT)) {
        std::vector<FieldType> field_types;
        for (uint32_t i = 0; i < col->get_subtype_count(); i++) {
            const auto* child_column = &col->get_sub_column(i);
            field_types.push_back(child_column->type());
        }
        return create_dynamic_type_info_ptr(get_struct_type_info(field_types));
    } else if (UNLIKELY(type == FieldType::OLAP_FIELD_TYPE_ARRAY)) {
        const auto* child_column = &col->get_sub_column(0);
        TypeInfoPtr item_type = get_type_info(child_column);
        ArrayTypeInfo* array_type_info = new ArrayTypeInfo(std::move(item_type));
        return create_dynamic_type_info_ptr(array_type_info);
    } else if (UNLIKELY(type == FieldType::OLAP_FIELD_TYPE_MAP)) {
        const auto* key_column = &col->get_sub_column(0);
        TypeInfoPtr key_type = get_type_info(key_column);
        const auto* val_column = &col->get_sub_column(1);
        TypeInfoPtr value_type = get_type_info(val_column);
        MapTypeInfo* map_type_info = new MapTypeInfo(std::move(key_type), std::move(value_type));
        return create_dynamic_type_info_ptr(map_type_info);
    } else {
        return create_static_type_info_ptr(get_scalar_type_info(type));
    }
}

TypeInfoPtr clone_type_info(const TypeInfo* type_info) {
    auto type = type_info->type();
    if (UNLIKELY(type == FieldType::OLAP_FIELD_TYPE_MAP)) {
        const auto map_type_info = dynamic_cast<const MapTypeInfo*>(type_info);
        return create_dynamic_type_info_ptr(
                new MapTypeInfo(clone_type_info(map_type_info->get_key_type_info()),
                                clone_type_info(map_type_info->get_value_type_info())));
    } else if (UNLIKELY(type == FieldType::OLAP_FIELD_TYPE_STRUCT)) {
        const auto struct_type_info = dynamic_cast<const StructTypeInfo*>(type_info);
        std::vector<TypeInfoPtr> clone_type_infos;
        const std::vector<TypeInfoPtr>* sub_type_infos = struct_type_info->type_infos();
        clone_type_infos.reserve(sub_type_infos->size());
        for (size_t i = 0; i < sub_type_infos->size(); i++) {
            clone_type_infos.push_back(clone_type_info((*sub_type_infos)[i].get()));
        }
        return create_dynamic_type_info_ptr(new StructTypeInfo(clone_type_infos));
    } else if (UNLIKELY(type == FieldType::OLAP_FIELD_TYPE_ARRAY)) {
        const auto array_type_info = dynamic_cast<const ArrayTypeInfo*>(type_info);
        return create_dynamic_type_info_ptr(
                new ArrayTypeInfo(clone_type_info(array_type_info->item_type_info())));
    } else {
        return create_static_type_info_ptr(type_info);
    }
}

} // namespace doris
