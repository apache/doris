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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/If.cpp
// and modified by Doris

#include <glog/logging.h>
#include <stddef.h>

#include <boost/iterator/iterator_facade.hpp>

#include "vec/columns/column.h"
#include "vec/columns/column_vector.h"
#include "vec/common/pod_array_fwd.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/functions/function_helpers.h"

#ifdef __ARM_NEON
#include <arm_acle.h>
#include <arm_neon.h>
#endif

namespace doris::vectorized {

template <PrimitiveType PType>
struct NumIfImpl {
private:
    using Type = typename PrimitiveTypeTraits<PType>::ColumnItemType;
    using ArrayCond = PaddedPODArray<UInt8>;
    using Array = PaddedPODArray<Type>;
    using ColVecResult = ColumnVector<PType>;
    using ColVecT = ColumnVector<PType>;

public:
    static const Array& get_data_from_column_const(const ColumnConst* column) {
        return assert_cast<const ColVecT&>(column->get_data_column()).get_data();
    }

    static ColumnPtr execute_if(const ArrayCond& cond, const ColumnPtr& then_col,
                                const ColumnPtr& else_col) {
        if (const auto* col_then = check_and_get_column<ColVecT>(then_col.get())) {
            if (const auto* col_else = check_and_get_column<ColVecT>(else_col.get())) {
                return execute_impl<false, false>(cond, col_then->get_data(), col_else->get_data());
            } else if (const auto* col_const_else =
                               check_and_get_column_const<ColVecT>(else_col.get())) {
                return execute_impl<false, true>(cond, col_then->get_data(),
                                                 get_data_from_column_const(col_const_else));
            }
        } else if (const auto* col_const_then =
                           check_and_get_column_const<ColVecT>(then_col.get())) {
            if (const auto* col_else = check_and_get_column<ColVecT>(else_col.get())) {
                return execute_impl<true, false>(cond, get_data_from_column_const(col_const_then),
                                                 col_else->get_data());
            } else if (const auto* col_const_else =
                               check_and_get_column_const<ColVecT>(else_col.get())) {
                return execute_impl<true, true>(cond, get_data_from_column_const(col_const_then),
                                                get_data_from_column_const(col_const_else));
            }
        }
        return nullptr;
    }

private:
    template <bool is_const_a, bool is_const_b>
    static ColumnPtr execute_impl(const ArrayCond& cond, const Array& a, const Array& b) {
#ifdef __ARM_NEON
        if constexpr (can_use_neon_opt()) {
            auto col_res = ColVecResult::create(cond.size());
            auto res = col_res->get_data().data();
            neon_execute<is_const_a, is_const_b>(cond.data(), res, a.data(), b.data(), cond.size());
            return col_res;
        }
#endif
        return native_execute<is_const_a, is_const_b>(cond, a, b);
    }

    // res[i] = cond[i] ? a[i] : b[i];
    template <bool is_const_a, bool is_const_b>
    static ColumnPtr native_execute(const ArrayCond& cond, const Array& a, const Array& b) {
        size_t size = cond.size();
        auto col_res = ColVecResult::create(size);
        typename ColVecResult::Container& res = col_res->get_data();
        for (size_t i = 0; i < size; ++i) {
            res[i] = cond[i] ? a[index_check_const<is_const_a>(i)]
                             : b[index_check_const<is_const_b>(i)];
        }
        return col_res;
    }

#ifdef __ARM_NEON
    constexpr static bool can_use_neon_opt() {
        return sizeof(Type) == 1 || sizeof(Type) == 2 || sizeof(Type) == 4 || sizeof(Type) == 8;
    }

    template <bool is_a_const, bool is_b_const>
    static void neon_execute(const uint8_t* cond, Type* res, const Type* a, const Type* b,
                             size_t size) {
        const Type* res_end = res + size;
        constexpr int data_size = sizeof(Type);
        constexpr int elements_per_process = 16;

        // Process 16 element of data at a time
        while (res + elements_per_process < res_end) {
            // Load 16 cond masks
            uint8x16_t loaded_mask = vld1q_u8(cond);
            //[1, 0, 1, 0, 1, 0, 1, 0, ...]

            loaded_mask = vceqq_u8(loaded_mask, vdupq_n_u8(0));
            //[0, FF, 0, FF, 0, FF, 0, FF, ...]

            loaded_mask = vmvnq_u8(loaded_mask);
            //[FF, 0, FF, 0, FF, 0, FF, 0, ...]

            if constexpr (data_size == 1) {
                uint8x16_t vec_a;
                if constexpr (!is_a_const) {
                    vec_a = vld1q_u8(reinterpret_cast<const uint8_t*>(a));
                } else {
                    vec_a = vdupq_n_u8(*reinterpret_cast<const uint8_t*>(a));
                }

                uint8x16_t vec_b;
                if constexpr (!is_b_const) {
                    vec_b = vld1q_u8(reinterpret_cast<const uint8_t*>(b));
                } else {
                    vec_b = vdupq_n_u8(*reinterpret_cast<const uint8_t*>(b));
                }

                uint8x16_t result = vbslq_u8(loaded_mask, vec_a, vec_b);

                vst1q_u8(reinterpret_cast<uint8_t*>(res), result);

            } else if constexpr (data_size == 2) {
                // Process 2 groups, each handling 8 int16
                for (int i = 0; i < 2; i++) {
                    uint16x8_t vec_a;
                    if constexpr (!is_a_const) {
                        vec_a = vld1q_u16(reinterpret_cast<const uint16_t*>(a) + i * 8);
                    } else {
                        vec_a = vdupq_n_u16(*reinterpret_cast<const uint16_t*>(a));
                    }

                    uint16x8_t vec_b;
                    if constexpr (!is_b_const) {
                        vec_b = vld1q_u16(reinterpret_cast<const uint16_t*>(b) + i * 8);
                    } else {
                        vec_b = vdupq_n_u16(*reinterpret_cast<const uint16_t*>(b));
                    }

                    uint8x16_t index = {0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7};
                    uint8x16_t mask = vqtbl1q_u8(loaded_mask, index);
                    // loaded_mask = [A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P] : u8
                    // index       = [0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7] : u8
                    // mask        = [A, A, B, B, C, C, D, D, E, E, F, F, G, G, H, H] : u8

                    uint16x8_t result = vbslq_u16(vreinterpretq_u16_u8(mask), vec_a, vec_b);

                    vst1q_u16(reinterpret_cast<uint16_t*>(res) + i * 8, result);
                    loaded_mask = vextq_u8(loaded_mask, loaded_mask, 8);
                }
            } else if constexpr (data_size == 4) {
                // Process 4 groups, each handling 4 int32
                for (int i = 0; i < 4; i++) {
                    uint32x4_t vec_a;
                    if constexpr (!is_a_const) {
                        vec_a = vld1q_u32(reinterpret_cast<const uint32_t*>(a) + i * 4);
                    } else {
                        vec_a = vdupq_n_u32(*reinterpret_cast<const uint32_t*>(a));
                    }

                    uint32x4_t vec_b;
                    if constexpr (!is_b_const) {
                        vec_b = vld1q_u32(reinterpret_cast<const uint32_t*>(b) + i * 4);
                    } else {
                        vec_b = vdupq_n_u32(*reinterpret_cast<const uint32_t*>(b));
                    }

                    uint8x16_t index = {0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3};
                    uint8x16_t mask = vqtbl1q_u8(loaded_mask, index);

                    uint32x4_t result = vbslq_u32(vreinterpretq_u32_u8(mask), vec_a, vec_b);

                    vst1q_u32(reinterpret_cast<uint32_t*>(res) + i * 4, result);
                    loaded_mask = vextq_u8(loaded_mask, loaded_mask, 4);
                }
            } else if constexpr (data_size == 8) {
                // Process 8 groups, each handling 2 int64
                for (int i = 0; i < 8; i++) {
                    uint64x2_t vec_a;
                    if constexpr (!is_a_const) {
                        vec_a = vld1q_u64(reinterpret_cast<const uint64_t*>(a) + i * 2);
                    } else {
                        vec_a = vdupq_n_u64(*reinterpret_cast<const uint64_t*>(a));
                    }

                    uint64x2_t vec_b;
                    if constexpr (!is_b_const) {
                        vec_b = vld1q_u64(reinterpret_cast<const uint64_t*>(b) + i * 2);
                    } else {
                        vec_b = vdupq_n_u64(*reinterpret_cast<const uint64_t*>(b));
                    }

                    uint8x16_t index = {0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1};
                    uint8x16_t mask = vqtbl1q_u8(loaded_mask, index);

                    uint64x2_t result = vbslq_u64(vreinterpretq_u64_u8(mask), vec_a, vec_b);

                    vst1q_u64(reinterpret_cast<uint64_t*>(res) + i * 2, result);

                    loaded_mask = vextq_u8(loaded_mask, loaded_mask, 2);
                }
            }

            res += elements_per_process;
            cond += elements_per_process;
            if (!is_a_const) {
                a += elements_per_process;
            }
            if (!is_b_const) {
                b += elements_per_process;
            }
        }

        // for the remaining data
        while (res < res_end) {
            *res = *cond ? *a : *b;
            res++;
            cond++;
            if (!is_a_const) {
                a++;
            }
            if (!is_b_const) {
                b++;
            }
        }
    }

#endif
};

} // namespace doris::vectorized
