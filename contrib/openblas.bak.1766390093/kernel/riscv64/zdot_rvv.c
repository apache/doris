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
#define VSETVL(n)               __riscv_vsetvl_e32m4(n)
#define VSETVL_MAX              __riscv_vsetvlmax_e32m4()
#define VSETVL_MAX_M1           __riscv_vsetvlmax_e32m1()
#define FLOAT_V_T               vfloat32m4_t
#define FLOAT_V_T_M1            vfloat32m1_t
#define FLOAT_VX2_T             vfloat32m4x2_t
#define VGET_VX2                __riscv_vget_v_f32m4x2_f32m4
#define VLSEG_FLOAT             __riscv_vlseg2e32_v_f32m4x2
#define VLSSEG_FLOAT            __riscv_vlsseg2e32_v_f32m4x2
#define VFREDSUM_FLOAT          __riscv_vfredusum_vs_f32m4_f32m1
#define VFMACCVV_FLOAT_TU       __riscv_vfmacc_vv_f32m4_tu
#define VFMVVF_FLOAT            __riscv_vfmv_v_f_f32m4
#define VFMVVF_FLOAT_M1         __riscv_vfmv_v_f_f32m1
#define VFMULVV_FLOAT           __riscv_vfmul_vv_f32m4
#define VFMSACVV_FLOAT          __riscv_vfmsac_vv_f32m4
#define VFNMSACVV_FLOAT_TU      __riscv_vfnmsac_vv_f32m4_tu
#define VFMVFS_FLOAT_M1         __riscv_vfmv_f_s_f32m1_f32
#else
#define VSETVL(n)               __riscv_vsetvl_e64m4(n)
#define VSETVL_MAX              __riscv_vsetvlmax_e64m4()
#define VSETVL_MAX_M1           __riscv_vsetvlmax_e64m1()
#define FLOAT_V_T               vfloat64m4_t
#define FLOAT_V_T_M1            vfloat64m1_t
#define FLOAT_VX2_T             vfloat64m4x2_t
#define VGET_VX2                __riscv_vget_v_f64m4x2_f64m4
#define VLSEG_FLOAT             __riscv_vlseg2e64_v_f64m4x2
#define VLSSEG_FLOAT            __riscv_vlsseg2e64_v_f64m4x2
#define VFREDSUM_FLOAT          __riscv_vfredusum_vs_f64m4_f64m1
#define VFMACCVV_FLOAT_TU       __riscv_vfmacc_vv_f64m4_tu
#define VFMVVF_FLOAT            __riscv_vfmv_v_f_f64m4
#define VFMVVF_FLOAT_M1         __riscv_vfmv_v_f_f64m1
#define VFMULVV_FLOAT           __riscv_vfmul_vv_f64m4
#define VFMSACVV_FLOAT          __riscv_vfmsac_vv_f64m4
#define VFNMSACVV_FLOAT_TU      __riscv_vfnmsac_vv_f64m4_tu
#define VFMVFS_FLOAT_M1         __riscv_vfmv_f_s_f64m1_f64
#endif

OPENBLAS_COMPLEX_FLOAT CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y)
{
    OPENBLAS_COMPLEX_FLOAT result;
    CREAL(result) = 0.0;
    CIMAG(result) = 0.0;

    if ( n <= 0 ) return(result);

    FLOAT_V_T vr0, vr1, vx0, vx1, vy0, vy1;
    FLOAT_V_T_M1 v_res, v_z0;
    FLOAT_VX2_T vxx2, vyx2;
    size_t vlmax_m1 = VSETVL_MAX_M1;
    v_z0 = VFMVVF_FLOAT_M1(0, vlmax_m1);

    size_t vlmax = VSETVL_MAX;
    vr0 = VFMVVF_FLOAT(0, vlmax);
    vr1 = VFMVVF_FLOAT(0, vlmax);
 
    if(inc_x == 1 && inc_y == 1) {

        for (size_t vl; n > 0; n -= vl, x += vl*2, y += vl*2) {
            vl = VSETVL(n);

            vxx2 = VLSEG_FLOAT(x, vl);
            vyx2 = VLSEG_FLOAT(y, vl);

            vx0 = VGET_VX2(vxx2, 0);
            vx1 = VGET_VX2(vxx2, 1);
            vy0 = VGET_VX2(vyx2, 0);
            vy1 = VGET_VX2(vyx2, 1);

            vr0 = VFMACCVV_FLOAT_TU(vr0, vx0, vy0, vl);
            vr1 = VFMACCVV_FLOAT_TU(vr1, vx0, vy1, vl);
        #if !defined(CONJ)
            vr0 = VFNMSACVV_FLOAT_TU(vr0, vx1, vy1, vl);
            vr1 = VFMACCVV_FLOAT_TU(vr1, vx1, vy0, vl);
        #else
            vr0 = VFMACCVV_FLOAT_TU(vr0, vx1, vy1, vl);
            vr1 = VFNMSACVV_FLOAT_TU(vr1, vx1, vy0, vl);
        #endif
        }

    }  else if (inc_x == 1){

        BLASLONG stride_y = inc_y * 2 * sizeof(FLOAT);

        for (size_t vl; n > 0; n -= vl, x += vl*2, y += vl*inc_y*2) {
            vl = VSETVL(n);

            vxx2 = VLSEG_FLOAT(x, vl);
            vyx2 = VLSSEG_FLOAT(y, stride_y, vl);

            vx0 = VGET_VX2(vxx2, 0);
            vx1 = VGET_VX2(vxx2, 1);
            vy0 = VGET_VX2(vyx2, 0);
            vy1 = VGET_VX2(vyx2, 1);

            vr0 = VFMACCVV_FLOAT_TU(vr0, vx0, vy0, vl);
            vr1 = VFMACCVV_FLOAT_TU(vr1, vx0, vy1, vl);
        #if !defined(CONJ)
            vr0 = VFNMSACVV_FLOAT_TU(vr0, vx1, vy1, vl);
            vr1 = VFMACCVV_FLOAT_TU(vr1, vx1, vy0, vl);
        #else
            vr0 = VFMACCVV_FLOAT_TU(vr0, vx1, vy1, vl);
            vr1 = VFNMSACVV_FLOAT_TU(vr1, vx1, vy0, vl);
        #endif
        }
    } else if (inc_y == 1){

        BLASLONG stride_x = inc_x * 2 * sizeof(FLOAT);

        for (size_t vl; n > 0; n -= vl, x += vl*inc_x*2, y += vl*2) {
            vl = VSETVL(n);

            vxx2 = VLSSEG_FLOAT(x, stride_x, vl);
            vyx2 = VLSEG_FLOAT(y, vl);

            vx0 = VGET_VX2(vxx2, 0);
            vx1 = VGET_VX2(vxx2, 1);
            vy0 = VGET_VX2(vyx2, 0);
            vy1 = VGET_VX2(vyx2, 1);

            vr0 = VFMACCVV_FLOAT_TU(vr0, vx0, vy0, vl);
            vr1 = VFMACCVV_FLOAT_TU(vr1, vx0, vy1, vl);
        #if !defined(CONJ)
            vr0 = VFNMSACVV_FLOAT_TU(vr0, vx1, vy1, vl);
            vr1 = VFMACCVV_FLOAT_TU(vr1, vx1, vy0, vl);
        #else
            vr0 = VFMACCVV_FLOAT_TU(vr0, vx1, vy1, vl);
            vr1 = VFNMSACVV_FLOAT_TU(vr1, vx1, vy0, vl);
        #endif
        }
    }else {

        BLASLONG stride_x = inc_x * 2 * sizeof(FLOAT);
        BLASLONG stride_y = inc_y * 2 * sizeof(FLOAT);

        for (size_t vl; n > 0; n -= vl, x += vl*inc_x*2, y += vl*inc_y*2) {
            vl = VSETVL(n);

            vxx2 = VLSSEG_FLOAT(x, stride_x, vl);
            vyx2 = VLSSEG_FLOAT(y, stride_y, vl);

            vx0 = VGET_VX2(vxx2, 0);
            vx1 = VGET_VX2(vxx2, 1);
            vy0 = VGET_VX2(vyx2, 0);
            vy1 = VGET_VX2(vyx2, 1);

            vr0 = VFMACCVV_FLOAT_TU(vr0, vx0, vy0, vl);
            vr1 = VFMACCVV_FLOAT_TU(vr1, vx0, vy1, vl);
        #if !defined(CONJ)
            vr0 = VFNMSACVV_FLOAT_TU(vr0, vx1, vy1, vl);
            vr1 = VFMACCVV_FLOAT_TU(vr1, vx1, vy0, vl);
        #else
            vr0 = VFMACCVV_FLOAT_TU(vr0, vx1, vy1, vl);
            vr1 = VFNMSACVV_FLOAT_TU(vr1, vx1, vy0, vl);
        #endif
        }
    }

    v_res = VFREDSUM_FLOAT(vr0, v_z0, vlmax);
    CREAL(result) = VFMVFS_FLOAT_M1(v_res);
    v_res = VFREDSUM_FLOAT(vr1, v_z0, vlmax);
    CIMAG(result) = VFMVFS_FLOAT_M1(v_res);
 
   return(result);
}
