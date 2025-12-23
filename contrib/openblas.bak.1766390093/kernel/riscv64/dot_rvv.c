/***************************************************************************
Copyright (c) 2022, The OpenBLAS Project
All rights reserved.
Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:
1. Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in
the documentation and/or other materials provided with the
distribution.
3. Neither the name of the OpenBLAS project nor the names of
its contributors may be used to endorse or promote products
derived from this software without specific prior written permission.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE OPENBLAS PROJECT OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*****************************************************************************/

#include "common.h"

#if defined(DSDOT)
double CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y)
#else
FLOAT CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y)
#endif
{
    double dot = 0.0;

    if ( n <= 0 ) return(dot);

    size_t vlmax = __riscv_vsetvlmax_e64m8();
    vfloat64m8_t vr = __riscv_vfmv_v_f_f64m8(0, vlmax);

    if(inc_x == 1 && inc_y == 1) {

        for (size_t vl; n > 0; n -= vl, x += vl, y += vl) {
            vl = __riscv_vsetvl_e64m8(n);

#if !defined(DOUBLE)
            vfloat32m4_t vx = __riscv_vle32_v_f32m4(x, vl);
            vfloat32m4_t vy = __riscv_vle32_v_f32m4(y, vl);

            vr = __riscv_vfwmacc_vv_f64m8_tu(vr, vx, vy, vl);
#else
            vfloat64m8_t vx = __riscv_vle64_v_f64m8(x, vl);
            vfloat64m8_t vy = __riscv_vle64_v_f64m8(y, vl);

            vr = __riscv_vfmacc_vv_f64m8_tu(vr, vx, vy, vl);
#endif
        }

    } else if (1 == inc_x) {
            
        BLASLONG stride_y = inc_y * sizeof(FLOAT);

        for (size_t vl; n > 0; n -= vl, x += vl, y += vl*inc_y) {
            vl = __riscv_vsetvl_e64m8(n);

#if !defined(DOUBLE)
            vfloat32m4_t vx = __riscv_vle32_v_f32m4(x, vl);
            vfloat32m4_t vy = __riscv_vlse32_v_f32m4(y, stride_y, vl);

            vr = __riscv_vfwmacc_vv_f64m8_tu(vr, vx, vy, vl);
#else
            vfloat64m8_t vx = __riscv_vle64_v_f64m8(x, vl);
            vfloat64m8_t vy = __riscv_vlse64_v_f64m8(y, stride_y, vl);

            vr = __riscv_vfmacc_vv_f64m8_tu(vr, vx, vy, vl);
#endif
        }
    } else if (1 == inc_y) {
            
        BLASLONG stride_x = inc_x * sizeof(FLOAT);

        for (size_t vl; n > 0; n -= vl, x += vl*inc_x, y += vl) {
            vl = __riscv_vsetvl_e64m8(n);

#if !defined(DOUBLE)
            vfloat32m4_t vx = __riscv_vlse32_v_f32m4(x, stride_x, vl);
            vfloat32m4_t vy = __riscv_vle32_v_f32m4(y, vl);

            vr = __riscv_vfwmacc_vv_f64m8_tu(vr, vx, vy, vl);
#else
            vfloat64m8_t vx = __riscv_vlse64_v_f64m8(x, stride_x, vl);
            vfloat64m8_t vy = __riscv_vle64_v_f64m8(y, vl);

            vr = __riscv_vfmacc_vv_f64m8_tu(vr, vx, vy, vl);
#endif
        }
    } else {
            
        BLASLONG stride_x = inc_x * sizeof(FLOAT);
        BLASLONG stride_y = inc_y * sizeof(FLOAT);

        for (size_t vl; n > 0; n -= vl, x += vl*inc_x, y += vl*inc_y) {
            vl = __riscv_vsetvl_e64m8(n);

#if !defined(DOUBLE)
            vfloat32m4_t vx = __riscv_vlse32_v_f32m4(x, stride_x, vl);
            vfloat32m4_t vy = __riscv_vlse32_v_f32m4(y, stride_y, vl);

            vr = __riscv_vfwmacc_vv_f64m8_tu(vr, vx, vy, vl);
#else
            vfloat64m8_t vx = __riscv_vlse64_v_f64m8(x, stride_x, vl);
            vfloat64m8_t vy = __riscv_vlse64_v_f64m8(y, stride_y, vl);

            vr = __riscv_vfmacc_vv_f64m8_tu(vr, vx, vy, vl);
#endif
        }
    }

    vfloat64m1_t vec_zero = __riscv_vfmv_v_f_f64m1(0, vlmax);
    vfloat64m1_t vec_sum = __riscv_vfredusum_vs_f64m8_f64m1(vr, vec_zero, vlmax);
    dot = __riscv_vfmv_f_s_f64m1_f64(vec_sum);

    return(dot);
}
