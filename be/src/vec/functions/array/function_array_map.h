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

#include <type_traits>

#include "vec/columns/column_array.h"
#include "vec/columns/column_string.h"
#include "vec/common/hash_table/hash_map.h"
#include "vec/common/hash_table/hash_set.h"
#include "vec/data_types/data_type_array.h"
#include "vec/functions/array/function_array_utils.h"
#include "vec/functions/function_helpers.h"

#define FILL_MAP_DATA_INTO_DEFAULT_COLUMN()                                \
    ++dst_off;                                                             \
    auto& dst_data = static_cast<ColumnType&>(*dst.nested_col).get_data(); \
    dst_data.push_back(entry.get_first());                                 \
    if (dst.nested_nullmap_data) {                                         \
        dst.nested_nullmap_data->push_back(0);                             \
    }

#define FILL_MAP_DATA_INTO_STRING_COLUMN()                       \
    auto& dst_col = static_cast<ColumnString&>(*dst.nested_col); \
    StringRef key = entry.get_first();                           \
    ++dst_off;                                                   \
    dst_col.insert_data(key.data, key.size);                     \
    if (dst.nested_nullmap_data) {                               \
        dst.nested_nullmap_data->push_back(0);                   \
    }

namespace doris::vectorized {

enum class MapOperation { INTERSECT, UNION };

template <typename Map, typename ColumnType>
struct IntersectAction;

template <typename Map, typename ColumnType>
struct UnionAction;

template <typename Map, typename ColumnType, MapOperation operation>
struct MapActionImpl;

template <typename Map, typename ColumnType>
struct MapActionImpl<Map, ColumnType, MapOperation::INTERSECT> {
    using Action = IntersectAction<Map, ColumnType>;
};

template <typename Map, typename ColumnType>
struct MapActionImpl<Map, ColumnType, MapOperation::UNION> {
    using Action = UnionAction<Map, ColumnType>;
};

template <MapOperation operation, typename ColumnType>
struct OpenMapImpl {
    using Element = typename ColumnType::value_type;
    using ElementNativeType = typename NativeType<Element>::Type;
    using Map =
            HashMapWithStackMemory<ElementNativeType, size_t, DefaultHash<ElementNativeType>, 6>;
    using Action = typename MapActionImpl<Map, ColumnType, operation>::Action;

    Action action;
    Map map;
    void reset() {
        map.clear();
        action.reset();
    }

    // this method calculate rows to get a rest dst data
    void apply(ColumnArrayMutableData& dst, const ColumnArrayExecutionDatas params,
               std::vector<bool>& col_const, int start_row, int end_row) {
        size_t dst_off = 0;
        for (int row = start_row; row < end_row; ++row) {
            reset();
            for (int i = 0; i < params.size(); ++i) {
                action.apply(map, i, index_check_const(row, col_const[i]), params[i]);
            }
            // nullmap
            if (action.apply_null()) {
                ++dst_off;
                dst.nested_col->insert_default();
                if (dst.nested_nullmap_data) {
                    dst.nested_nullmap_data->push_back(1);
                }
            }
            // make map result to dst
            for (const auto& entry : map) {
                if constexpr (operation == MapOperation::INTERSECT) {
                    if (entry.get_mapped() == params.size()) {
                        FILL_MAP_DATA_INTO_DEFAULT_COLUMN()
                    }
                } else if constexpr (operation == MapOperation::UNION) {
                    // union in map all key
                    FILL_MAP_DATA_INTO_DEFAULT_COLUMN()
                }
            }
            dst.offsets_ptr->push_back(dst_off);
        }
    }
};

template <MapOperation operation>
struct OpenMapImpl<operation, ColumnString> {
    using Map = HashMapWithStackMemory<StringRef, size_t, StringRefHash, 6>;
    using Action = typename MapActionImpl<Map, ColumnString, operation>::Action;

    Action action;
    Map map;

    void reset() {
        map.clear();
        action.reset();
    }

    void apply(ColumnArrayMutableData& dst, const ColumnArrayExecutionDatas params,
               std::vector<bool>& col_const, int start_row, int end_row) {
        size_t dst_off = 0;
        for (int row = start_row; row < end_row; ++row) {
            reset();
            for (int i = 0; i < params.size(); ++i) {
                action.apply(map, i, index_check_const(row, col_const[i]), params[i]);
            }
            // nullmap
            if (action.apply_null()) {
                ++dst_off;
                dst.nested_col->insert_default();
                if (dst.nested_nullmap_data) {
                    dst.nested_nullmap_data->push_back(1);
                }
            }
            // make map result to dst
            for (const auto& entry : map) {
                if constexpr (operation == MapOperation::INTERSECT) {
                    if (entry.get_mapped() == params.size()) {
                        FILL_MAP_DATA_INTO_STRING_COLUMN()
                    }
                } else if constexpr (operation == MapOperation::UNION) {
                    FILL_MAP_DATA_INTO_STRING_COLUMN()
                }
            }
            dst.offsets_ptr->push_back(dst_off);
        }
    }
};

template <MapOperation operation>
struct ArrayMapImpl {
public:
    static DataTypePtr get_return_type(const DataTypes& arguments) {
        DataTypePtr res;
        // if any nested type of array arguments is nullable then return array with
        // nullable nested type.
        for (const auto& arg : arguments) {
            const DataTypeArray* array_type = check_and_get_data_type<DataTypeArray>(arg.get());
            if (array_type->get_nested_type()->is_nullable()) {
                res = arg;
                break;
            }
        }
        res = res ? res : arguments[0];
        return res;
    }

    static Status execute(ColumnPtr& res_ptr, ColumnArrayExecutionDatas datas,
                          std::vector<bool>& col_const, int start_row, int end_row) {
        ColumnArrayMutableData dst =
                create_mutable_data(datas[0].nested_col, datas[0].nested_nullmap_data);
        if (_execute_internal<ALL_COLUMNS_SIMPLE>(dst, datas, col_const, start_row, end_row)) {
            res_ptr = assemble_column_array(dst);
            return Status::OK();
        }
        return Status::RuntimeError("Unexpected columns");
    }

private:
    template <typename ColumnType>
    static bool _execute_internal(ColumnArrayMutableData& dst, ColumnArrayExecutionDatas datas,
                                  std::vector<bool>& col_const, int start_row, int end_row) {
        for (auto data : datas) {
            if (!check_column<ColumnType>(*data.nested_col)) {
                return false;
            }
        }
        // do check staff
        using Impl = OpenMapImpl<operation, ColumnType>;
        Impl impl;
        ColumnPtr res_column;
        impl.apply(dst, datas, col_const, start_row, end_row);
        return true;
    }

    template <typename T, typename... Ts>
        requires(sizeof...(Ts) > 0)
    static bool _execute_internal(ColumnArrayMutableData& dst, ColumnArrayExecutionDatas datas,
                                  std::vector<bool>& col_const, int start_row, int end_row) {
        return _execute_internal<T>(dst, datas, col_const, start_row, end_row) ||
               _execute_internal<Ts...>(dst, datas, col_const, start_row, end_row);
    }
};

} // namespace doris::vectorized
