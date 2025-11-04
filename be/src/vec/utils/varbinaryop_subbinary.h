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

#include "vec/columns/column_const.h"
#include "vec/columns/column_varbinary.h"
#include "vec/core/block.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_varbinary.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

constexpr auto SIZE_OF_UINT = sizeof(uint32_t);

struct VarBinaryOP {
    static void check_and_insert_data(doris::StringView& sView, const char* data, uint32_t len,
                                      bool before_is_inline) {
        if (before_is_inline) {
            sView.set_size(len);
        } else {
            sView = doris::StringView(data, len);
        }
    }

    static std::pair<bool, char*> alloc(ColumnVarbinary* res_col, size_t index, uint32_t len) {
        bool is_inline = StringView::isInline(len);
        char* dst = nullptr;
        if (is_inline) {
            dst = reinterpret_cast<char*>(&(res_col->get_data()[index])) + SIZE_OF_UINT;
        } else {
            dst = res_col->alloc(len);
        }
        return {is_inline, dst};
    }
};

struct SubBinaryUtil {
    static void sub_binary_execute(Block& block, const ColumnNumbers& arguments, uint32_t result,
                                   size_t input_rows_count) {
        DCHECK_EQ(arguments.size(), 3);
        auto res = ColumnVarbinary::create();

        bool col_const[3];
        ColumnPtr argument_columns[3];
        for (int i = 0; i < 3; ++i) {
            std::tie(argument_columns[i], col_const[i]) =
                    unpack_if_const(block.get_by_position(arguments[i]).column);
        }

        const auto* specific_binary_column =
                assert_cast<const ColumnVarbinary*>(argument_columns[0].get());
        const auto* specific_start_column =
                assert_cast<const ColumnInt32*>(argument_columns[1].get());
        const auto* specific_len_column =
                assert_cast<const ColumnInt32*>(argument_columns[2].get());

        std::visit(
                [&](auto binary_const, auto start_const, auto len_const) {
                    vectors<binary_const, start_const, len_const>(
                            specific_binary_column, specific_start_column, specific_len_column,
                            res.get(), input_rows_count);
                },
                vectorized::make_bool_variant(col_const[0]),
                vectorized::make_bool_variant(col_const[1]),
                vectorized::make_bool_variant(col_const[2]));
        block.get_by_position(result).column = std::move(res);
    }

private:
    template <bool binary_const, bool start_const, bool len_const>
    static void vectors(const ColumnVarbinary* binarys, const ColumnInt32* start,
                        const ColumnInt32* len, ColumnVarbinary* res, size_t size) {
        res->get_data().reserve(size);

        for (size_t i = 0; i < size; ++i) {
            doris::StringView binary = binarys->get_data()[index_check_const<binary_const>(i)];
            int binary_size = static_cast<int>(binary.size());

            int start_value = start->get_data()[index_check_const<start_const>(i)];
            int len_value = len->get_data()[index_check_const<len_const>(i)];

            bool start_out_of_range = (start_value > binary_size) || (start_value < -binary_size);
            bool len_non_positive = len_value <= 0;
            bool input_empty = binary_size == 0;

            if (start_out_of_range || len_non_positive || input_empty) {
                res->insert_default();
                continue;
            }
            int fixed_pos = start_value - 1;
            if (fixed_pos < 0) {
                fixed_pos = binary_size + fixed_pos + 1;
            }
            int fixed_len = std::min(binary_size - fixed_pos, len_value);

            res->insert_data(binary.data() + fixed_pos, fixed_len);
        }
    }
};

#include "common/compile_check_end.h"
} // namespace doris::vectorized
