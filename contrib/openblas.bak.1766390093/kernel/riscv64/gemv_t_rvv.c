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

#if !defined(DOUBLE)
#define VSETVL(n)               __riscv_vsetvl_e32m8(n)
#define VSETVL_MAX              __riscv_vsetvlmax_e32m8()
#define VSETVL_MAX_M1           __riscv_vsetvlmax_e32m1()
#define FLOAT_V_T               vfloat32m8_t
#define FLOAT_V_T_M1            vfloat32m1_t
#define VLEV_FLOAT              __riscv_vle32_v_f32m8
#define VLSEV_FLOAT             __riscv_vlse32_v_f32m8
#define VFREDSUM_FLOAT          __riscv_vfredusum_vs_f32m8_f32m1
#define VFMACCVV_FLOAT_TU       __riscv_vfmacc_vv_f32m8_tu
#define VFMVVF_FLOAT            __riscv_vfmv_v_f_f32m8
#define VFMVVF_FLOAT_M1         __riscv_vfmv_v_f_f32m1
#define VFMVFS_FLOAT_M1         __riscv_vfmv_f_s_f32m1_f32
#else
#define VSETVL(n)               __riscv_vsetvl_e64m8(n)
#define VSETVL_MAX              __riscv_vsetvlmax_e64m8()
#define VSETVL_MAX_M1           __riscv_vsetvlmax_e64m1()
#define FLOAT_V_T               vfloat64m8_t
#define FLOAT_V_T_M1            vfloat64m1_t
#define VLEV_FLOAT              __riscv_vle64_v_f64m8
#define VLSEV_FLOAT             __riscv_vlse64_v_f64m8
#define VFREDSUM_FLOAT          __riscv_vfredusum_vs_f64m8_f64m1
#define VFMACCVV_FLOAT_TU       __riscv_vfmacc_vv_f64m8_tu
#define VFMVVF_FLOAT            __riscv_vfmv_v_f_f64m8
#define VFMVVF_FLOAT_M1         __riscv_vfmv_v_f_f64m1
#define VFMVFS_FLOAT_M1         __riscv_vfmv_f_s_f64m1_f64
#endif

int CNAME(BLASLONG m, BLASLONG n, BLASLONG dummy1, FLOAT alpha, FLOAT *a, BLASLONG lda, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *buffer)
{
    BLASLONG i, j;
    FLOAT *a_ptr, *x_ptr;

    FLOAT_V_T va, vx, vr;
    FLOAT_V_T_M1 v_res, v_z0;
    size_t vlmax = VSETVL_MAX_M1;
    v_z0 = VFMVVF_FLOAT_M1(0, vlmax);
    vlmax = VSETVL_MAX;

    if(inc_x == 1) {

        for(i = 0; i < n; i++) {
            j = m;
            a_ptr = a;
            x_ptr = x;
            vr = VFMVVF_FLOAT(0, vlmax);

            for (size_t vl; j > 0; j -= vl, a_ptr += vl, x_ptr += vl) {
                vl = VSETVL(j);

                va = VLEV_FLOAT(a_ptr, vl);
                vx = VLEV_FLOAT(x_ptr, vl);
                vr = VFMACCVV_FLOAT_TU(vr, va, vx, vl);
            }

            v_res = VFREDSUM_FLOAT(vr, v_z0, vlmax);
            *y += alpha * VFMVFS_FLOAT_M1(v_res);
            y += inc_y;
            a += lda;
        }

    } else {

        BLASLONG stride_x = inc_x * sizeof(FLOAT);
  
        for(i = 0; i < n; i++) {
            j = m;
            a_ptr = a;
            x_ptr = x;
            vr = VFMVVF_FLOAT(0, vlmax);

            for (size_t vl; j > 0; j -= vl, a_ptr += vl, x_ptr += vl*inc_x) {
                vl = VSETVL(j);

                va = VLEV_FLOAT(a_ptr, vl);
                vx = VLSEV_FLOAT(x_ptr, stride_x, vl);
                vr = VFMACCVV_FLOAT_TU(vr, va, vx, vl);
            }

            v_res = VFREDSUM_FLOAT(vr, v_z0, vlmax);
            *y += alpha * VFMVFS_FLOAT_M1(v_res);
            y += inc_y;
            a += lda;
        }

    }

    return(0);
}
