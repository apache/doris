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

#include <thrift/protocol/TJSONProtocol.h>

#include <boost/shared_ptr.hpp>

#include "runtime/descriptors.h"
#include "util/simd/bits.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/core/block.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

namespace doris::vectorized {
class VectorizedUtils {
public:
    static Block create_empty_columnswithtypename(const RowDescriptor& row_desc) {
        // Block block;
        return create_columns_with_type_and_name(row_desc);
    }
    static MutableBlock build_mutable_mem_reuse_block(Block* block, const RowDescriptor& row_desc) {
        if (!block->mem_reuse()) {
            MutableBlock tmp(VectorizedUtils::create_columns_with_type_and_name(row_desc));
            block->swap(tmp.to_block());
        }
        return MutableBlock::build_mutable_block(block);
    }
    static MutableBlock build_mutable_mem_reuse_block(Block* block, const Block& other) {
        if (!block->mem_reuse()) {
            MutableBlock tmp(other.clone_empty());
            block->swap(tmp.to_block());
        }
        return MutableBlock::build_mutable_block(block);
    }
    static MutableBlock build_mutable_mem_reuse_block(Block* block,
                                                      std::vector<SlotDescriptor*>& slots) {
        if (!block->mem_reuse()) {
            size_t column_size = slots.size();
            MutableColumns columns(column_size);
            for (size_t i = 0; i < column_size; i++) {
                columns[i] = slots[i]->get_empty_mutable_column();
            }
            int n_columns = 0;
            for (const auto slot_desc : slots) {
                block->insert(ColumnWithTypeAndName(std::move(columns[n_columns++]),
                                                    slot_desc->get_data_type_ptr(),
                                                    slot_desc->col_name()));
            }
        }
        return MutableBlock(block);
    }

    static ColumnsWithTypeAndName create_columns_with_type_and_name(const RowDescriptor& row_desc) {
        ColumnsWithTypeAndName columns_with_type_and_name;
        for (const auto& tuple_desc : row_desc.tuple_descriptors()) {
            for (const auto& slot_desc : tuple_desc->slots()) {
                if (!slot_desc->need_materialize()) {
                    continue;
                }
                columns_with_type_and_name.emplace_back(nullptr, slot_desc->get_data_type_ptr(),
                                                        slot_desc->col_name());
            }
        }
        return columns_with_type_and_name;
    }

    static NameAndTypePairs create_name_and_data_types(const RowDescriptor& row_desc) {
        NameAndTypePairs name_with_types;
        for (const auto& tuple_desc : row_desc.tuple_descriptors()) {
            for (const auto& slot_desc : tuple_desc->slots()) {
                if (!slot_desc->need_materialize()) {
                    continue;
                }
                name_with_types.emplace_back(slot_desc->col_name(), slot_desc->get_data_type_ptr());
            }
        }
        return name_with_types;
    }

    static ColumnsWithTypeAndName create_empty_block(const RowDescriptor& row_desc,
                                                     bool ignore_trivial_slot = true) {
        ColumnsWithTypeAndName columns_with_type_and_name;
        for (const auto& tuple_desc : row_desc.tuple_descriptors()) {
            for (const auto& slot_desc : tuple_desc->slots()) {
                if (ignore_trivial_slot && !slot_desc->need_materialize()) {
                    continue;
                }
                columns_with_type_and_name.emplace_back(
                        slot_desc->get_data_type_ptr()->create_column(),
                        slot_desc->get_data_type_ptr(), slot_desc->col_name());
            }
        }
        return columns_with_type_and_name;
    }

    // is_single: whether src is null map of a ColumnConst
    static void update_null_map(NullMap& dst, const NullMap& src, bool is_single = false) {
        size_t size = dst.size();
        auto* __restrict l = dst.data();
        auto* __restrict r = src.data();
        if (is_single && r[0]) {
            for (size_t i = 0; i < size; ++i) {
                l[i] = 1;
            }
        } else {
            for (size_t i = 0; i < size; ++i) {
                l[i] |= r[i];
            }
        }
    }

    static DataTypes get_data_types(const RowDescriptor& row_desc) {
        DataTypes data_types;
        for (const auto& tuple_desc : row_desc.tuple_descriptors()) {
            for (const auto& slot_desc : tuple_desc->slots()) {
                data_types.push_back(slot_desc->get_data_type_ptr());
            }
        }
        return data_types;
    }

    static std::vector<std::string> get_column_names(const RowDescriptor& row_desc) {
        std::vector<std::string> column_names;
        for (const auto& tuple_desc : row_desc.tuple_descriptors()) {
            for (const auto& slot_desc : tuple_desc->slots()) {
                column_names.push_back(slot_desc->col_name());
            }
        }
        return column_names;
    }

    static bool all_arguments_are_constant(const Block& block, const ColumnNumbers& args) {
        for (const auto& arg : args) {
            if (!is_column_const(*block.get_by_position(arg).column)) {
                return false;
            }
        }
        return true;
    }
};

inline bool match_suffix(const std::string& name, const std::string& suffix) {
    if (name.length() < suffix.length()) {
        return false;
    }
    return name.substr(name.length() - suffix.length()) == suffix;
}

inline std::string remove_suffix(const std::string& name, const std::string& suffix) {
    CHECK(match_suffix(name, suffix))
            << ", suffix not match, name=" << name << ", suffix=" << suffix;
    return name.substr(0, name.length() - suffix.length());
};

inline ColumnPtr create_always_true_column(size_t size, bool is_nullable) {
    auto res_data_column = ColumnUInt8::create(size, 1);
    if (is_nullable) {
        auto null_map = ColumnVector<UInt8>::create(size, 0);
        return ColumnNullable::create(std::move(res_data_column), std::move(null_map));
    }
    return res_data_column;
}

// change null element to true element
inline void change_null_to_true(ColumnPtr column, ColumnPtr argument = nullptr) {
    size_t rows = column->size();
    if (is_column_const(*column)) {
        change_null_to_true(assert_cast<const ColumnConst*>(column.get())->get_data_column_ptr());
    } else if (column->has_null()) {
        auto* nullable =
                const_cast<ColumnNullable*>(assert_cast<const ColumnNullable*>(column.get()));
        auto* __restrict data = assert_cast<ColumnUInt8*>(nullable->get_nested_column_ptr().get())
                                        ->get_data()
                                        .data();
        auto* __restrict null_map = const_cast<uint8_t*>(nullable->get_null_map_data().data());
        for (size_t i = 0; i < rows; ++i) {
            data[i] |= null_map[i];
        }
        memset(null_map, 0, rows);
    } else if (argument != nullptr && argument->has_null()) {
        const auto* __restrict null_map =
                assert_cast<const ColumnNullable*>(argument.get())->get_null_map_data().data();
        auto* __restrict data =
                const_cast<ColumnUInt8*>(assert_cast<const ColumnUInt8*>(column.get()))
                        ->get_data()
                        .data();
        for (size_t i = 0; i < rows; ++i) {
            data[i] |= null_map[i];
        }
    }
}

inline size_t calculate_false_number(ColumnPtr column) {
    size_t rows = column->size();
    if (is_column_const(*column)) {
        return calculate_false_number(
                       assert_cast<const ColumnConst*>(column.get())->get_data_column_ptr()) *
               rows;
    } else if (column->is_nullable()) {
        const auto* nullable = assert_cast<const ColumnNullable*>(column.get());
        const auto* data = assert_cast<const ColumnUInt8*>(nullable->get_nested_column_ptr().get())
                                   ->get_data()
                                   .data();
        const auto* __restrict null_map = nullable->get_null_map_data().data();
        return simd::count_zero_num(reinterpret_cast<const int8_t* __restrict>(data), null_map,
                                    rows);
    } else {
        const auto* data = assert_cast<const ColumnUInt8*>(column.get())->get_data().data();
        return simd::count_zero_num(reinterpret_cast<const int8_t* __restrict>(data), rows);
    }
}

} // namespace doris::vectorized

namespace apache::thrift {
template <typename ThriftStruct>
ThriftStruct from_json_string(const std::string& json_val) {
    using namespace apache::thrift::transport;
    using namespace apache::thrift::protocol;
    ThriftStruct ts;
    std::shared_ptr<TTransport> trans =
            std::make_shared<TMemoryBuffer>((uint8_t*)json_val.c_str(), (uint32_t)json_val.size());
    TJSONProtocol protocol(trans);
    ts.read(&protocol);
    return ts;
}

} // namespace apache::thrift
