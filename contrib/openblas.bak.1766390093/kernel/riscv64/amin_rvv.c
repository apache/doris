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
#include <float.h>

#if !defined(DOUBLE)
#define VSETVL(n)               __riscv_vsetvl_e32m8(n)
#define VSETVL_MAX              __riscv_vsetvlmax_e32m8()
#define VSETVL_MAX_M1           __riscv_vsetvlmax_e32m1()
#define FLOAT_V_T               vfloat32m8_t
#define FLOAT_V_T_M1            vfloat32m1_t
#define VLEV_FLOAT              __riscv_vle32_v_f32m8
#define VLSEV_FLOAT             __riscv_vlse32_v_f32m8
#define VFREDMINVS_FLOAT        __riscv_vfredmin_vs_f32m8_f32m1
#define VFMVVF_FLOAT            __riscv_vfmv_v_f_f32m8
#define VFMVVF_FLOAT_M1         __riscv_vfmv_v_f_f32m1
#define VFMINVV_FLOAT_TU        __riscv_vfmin_vv_f32m8_tu
#define VFABSV_FLOAT            __riscv_vfabs_v_f32m8
#define VFMVFS_FLOAT_M1         __riscv_vfmv_f_s_f32m1_f32
#else
#define VSETVL(n)               __riscv_vsetvl_e64m8(n)
#define VSETVL_MAX              __riscv_vsetvlmax_e64m8()
#define VSETVL_MAX_M1           __riscv_vsetvlmax_e64m1()
#define FLOAT_V_T               vfloat64m8_t
#define FLOAT_V_T_M1            vfloat64m1_t
#define VLEV_FLOAT              __riscv_vle64_v_f64m8
#define VLSEV_FLOAT             __riscv_vlse64_v_f64m8
#define VFREDMINVS_FLOAT        __riscv_vfredmin_vs_f64m8_f64m1
#define VFMVVF_FLOAT            __riscv_vfmv_v_f_f64m8
#define VFMVVF_FLOAT_M1         __riscv_vfmv_v_f_f64m1
#define VFMINVV_FLOAT_TU        __riscv_vfmin_vv_f64m8_tu
#define VFABSV_FLOAT            __riscv_vfabs_v_f64m8
#define VFMVFS_FLOAT_M1         __riscv_vfmv_f_s_f64m1_f64
#endif

FLOAT CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x)
{
    FLOAT minf = 0.0;

    if (n <= 0 || inc_x <= 0) return(minf);

    FLOAT_V_T vx, vmin;
    FLOAT_V_T_M1 v_res;
    
    v_res = VFMVVF_FLOAT_M1(FLT_MAX, VSETVL_MAX_M1);
    size_t vlmax = VSETVL_MAX;
    vmin = VFMVVF_FLOAT(FLT_MAX, vlmax);

    if(inc_x == 1) {

        for (size_t vl; n > 0; n -= vl, x += vl) {
            vl = VSETVL(n);

            vx = VLEV_FLOAT(x, vl);
            vx = VFABSV_FLOAT(vx, vl);
            vmin = VFMINVV_FLOAT_TU(vmin, vmin, vx, vl);
        }

    } else {

        BLASLONG stride_x = inc_x * sizeof(FLOAT);

        for (size_t vl; n > 0; n -= vl, x += vl*inc_x) {
            vl = VSETVL(n);

            vx = VLSEV_FLOAT(x, stride_x, vl);
            vx = VFABSV_FLOAT(vx, vl);
            vmin = VFMINVV_FLOAT_TU(vmin, vmin, vx, vl);
        }

    }

    v_res = VFREDMINVS_FLOAT(vmin, v_res, vlmax);
    minf = VFMVFS_FLOAT_M1(v_res);

    return(minf);
}
