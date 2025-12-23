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
#define VSETVL_MAX_M1           __riscv_vsetvlmax_e32m1()
#define FLOAT_V_T               vfloat32m4_t
#define FLOAT_V_T_M1            vfloat32m1_t
#define FLOAT_VX2_T             vfloat32m4x2_t
#define VGET_VX2                __riscv_vget_v_f32m4x2_f32m4
#define VLSEG_FLOAT             __riscv_vlseg2e32_v_f32m4x2
#define VLSSEG_FLOAT            __riscv_vlsseg2e32_v_f32m4x2
#define VFREDSUM_FLOAT_TU       __riscv_vfredusum_vs_f32m4_f32m1_tu
#define VFMACCVV_FLOAT_TU       __riscv_vfmacc_vv_f32m4_tu
#define VFNMSACVV_FLOAT_TU      __riscv_vfnmsac_vv_f32m4_tu
#define VFMVVF_FLOAT            __riscv_vfmv_v_f_f32m4
#define VFMVVF_FLOAT_M1         __riscv_vfmv_v_f_f32m1
#define VFMULVV_FLOAT           __riscv_vfmul_vv_f32m4
#define VFMVFS_FLOAT_M1         __riscv_vfmv_f_s_f32m1_f32
#else
#define VSETVL(n)               __riscv_vsetvl_e64m4(n)
#define VSETVL_MAX_M1           __riscv_vsetvlmax_e64m1()
#define FLOAT_V_T               vfloat64m4_t
#define FLOAT_V_T_M1            vfloat64m1_t
#define FLOAT_VX2_T             vfloat64m4x2_t
#define VGET_VX2                __riscv_vget_v_f64m4x2_f64m4
#define VLSEG_FLOAT             __riscv_vlseg2e64_v_f64m4x2
#define VLSSEG_FLOAT            __riscv_vlsseg2e64_v_f64m4x2
#define VFREDSUM_FLOAT_TU       __riscv_vfredusum_vs_f64m4_f64m1_tu
#define VFMACCVV_FLOAT_TU       __riscv_vfmacc_vv_f64m4_tu
#define VFNMSACVV_FLOAT_TU      __riscv_vfnmsac_vv_f64m4_tu
#define VFMVVF_FLOAT            __riscv_vfmv_v_f_f64m4
#define VFMVVF_FLOAT_M1         __riscv_vfmv_v_f_f64m1
#define VFMULVV_FLOAT           __riscv_vfmul_vv_f64m4
#define VFMVFS_FLOAT_M1         __riscv_vfmv_f_s_f64m1_f64
#endif

int CNAME(BLASLONG m, BLASLONG n, BLASLONG dummy1, FLOAT alpha_r, FLOAT alpha_i, FLOAT *a, BLASLONG lda, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *buffer)
{
    BLASLONG i = 0, j = 0;
    BLASLONG ix = 0, iy = 0;
    FLOAT *a_ptr = a;
    FLOAT temp_r, temp_i;

    FLOAT_V_T va0, va1, vx0, vx1, vr, vi; 
    FLOAT_V_T_M1 v_res, v_z0;
    FLOAT_VX2_T vxx2, vax2;

    BLASLONG stride_x = inc_x * sizeof(FLOAT) * 2;
    //BLASLONG stride_a = sizeof(FLOAT) * 2;
    BLASLONG inc_y2 = inc_y * 2;
    BLASLONG lda2 = lda * 2;

    size_t vlmax = VSETVL_MAX_M1;
    v_res = VFMVVF_FLOAT_M1(0, vlmax);
    v_z0 = VFMVVF_FLOAT_M1(0, vlmax);
    vlmax = VSETVL(m);

    if (inc_x == 1)
    {
        for(i = 0; i < n; i++) {    
            j = 0;
            ix = 0;
            vr = VFMVVF_FLOAT(0, vlmax);
            vi = VFMVVF_FLOAT(0, vlmax);
            for(size_t vl, k = m; k > 0; k -= vl) {
                vl = VSETVL(k);

                vax2 = VLSEG_FLOAT(&a_ptr[j], vl);
                vxx2 = VLSEG_FLOAT(&x[ix], vl);

                va0 = VGET_VX2(vax2, 0);
                va1 = VGET_VX2(vax2, 1);
                vx0 = VGET_VX2(vxx2, 0);
                vx1 = VGET_VX2(vxx2, 1);

#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
                vr = VFMACCVV_FLOAT_TU(vr, va0, vx0, vl);
                vr = VFNMSACVV_FLOAT_TU(vr, va1, vx1, vl);
                vi = VFMACCVV_FLOAT_TU(vi, va0, vx1, vl);
                vi = VFMACCVV_FLOAT_TU(vi, va1, vx0, vl);
#else
                vr = VFMACCVV_FLOAT_TU(vr, va0, vx0, vl);
                vr = VFMACCVV_FLOAT_TU(vr, va1, vx1, vl);
                vi = VFMACCVV_FLOAT_TU(vi, va0, vx1, vl);
                vi = VFNMSACVV_FLOAT_TU(vi, va1, vx0, vl);
#endif
                j += vl * 2;
                ix += vl * inc_x * 2;
            }
            
            v_res = VFREDSUM_FLOAT_TU(v_res, vr, v_z0, vlmax);
            temp_r = VFMVFS_FLOAT_M1(v_res);
            v_res = VFREDSUM_FLOAT_TU(v_res, vi, v_z0, vlmax);
            temp_i = VFMVFS_FLOAT_M1(v_res);

#if !defined(XCONJ)
            y[iy]   += alpha_r * temp_r - alpha_i * temp_i;
            y[iy+1] += alpha_r * temp_i + alpha_i * temp_r;
#else
            y[iy]   += alpha_r * temp_r + alpha_i * temp_i;
            y[iy+1] -= alpha_r * temp_i - alpha_i * temp_r;
#endif
            iy += inc_y2;
            a_ptr += lda2;
        }
    }
    else
    {
        for(i = 0; i < n; i++) {    
            j = 0;
            ix = 0;
            vr = VFMVVF_FLOAT(0, vlmax);
            vi = VFMVVF_FLOAT(0, vlmax);
            for(size_t vl, k = m; k > 0; k -= vl) {
                vl = VSETVL(k);
    
                vax2 = VLSEG_FLOAT(&a_ptr[j], vl);
                vxx2 = VLSSEG_FLOAT(&x[ix], stride_x, vl);

                va0 = VGET_VX2(vax2, 0);
                va1 = VGET_VX2(vax2, 1);
                vx0 = VGET_VX2(vxx2, 0);
                vx1 = VGET_VX2(vxx2, 1);
    
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
                vr = VFMACCVV_FLOAT_TU(vr, va0, vx0, vl);
                vr = VFNMSACVV_FLOAT_TU(vr, va1, vx1, vl);
                vi = VFMACCVV_FLOAT_TU(vi, va0, vx1, vl);
                vi = VFMACCVV_FLOAT_TU(vi, va1, vx0, vl);
#else
                vr = VFMACCVV_FLOAT_TU(vr, va0, vx0, vl);
                vr = VFMACCVV_FLOAT_TU(vr, va1, vx1, vl);
                vi = VFMACCVV_FLOAT_TU(vi, va0, vx1, vl);
                vi = VFNMSACVV_FLOAT_TU(vi, va1, vx0, vl);
#endif
                j += vl * 2;
                ix += vl * inc_x * 2;
            }
            
            v_res = VFREDSUM_FLOAT_TU(v_res, vr, v_z0, vlmax);
            temp_r = VFMVFS_FLOAT_M1(v_res);
            v_res = VFREDSUM_FLOAT_TU(v_res, vi, v_z0, vlmax);
            temp_i = VFMVFS_FLOAT_M1(v_res);
    
#if !defined(XCONJ)
            y[iy]   += alpha_r * temp_r - alpha_i * temp_i;
            y[iy+1] += alpha_r * temp_i + alpha_i * temp_r;
#else
            y[iy]   += alpha_r * temp_r + alpha_i * temp_i;
            y[iy+1] -= alpha_r * temp_i - alpha_i * temp_r;
#endif
            iy += inc_y2;
            a_ptr += lda2;
        }

    }


    return(0);
}
