/***************************************************************************
Copyright (c) 2013, The OpenBLAS Project
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
#define VSETVL(n) RISCV_RVV(vsetvl_e32m2)(n)
#define VSETVL_MAX RISCV_RVV(vsetvlmax_e32m1)()
#define FLOAT_V_T vfloat32m2_t
#define FLOAT_V_T_M1 vfloat32m1_t
#define VFMVFS_FLOAT RISCV_RVV(vfmv_f_s_f32m1_f32)
#define VLSEV_FLOAT RISCV_RVV(vlse32_v_f32m2)
#ifdef RISCV_0p10_INTRINSICS
#define VFREDSUM_FLOAT(vr, va, vb, gvl) RISCV_RVV(vfredusum_vs_f32m2_f32m1)(vr, va, vb, gvl)
#else
#define VFREDSUM_FLOAT(vr, va, vb, gvl) RISCV_RVV(vfredusum_vs_f32m2_f32m1)(va, vb, gvl)
#endif
#define VFMACCVV_FLOAT RISCV_RVV(vfmacc_vv_f32m2)
#define VFNMSACVV_FLOAT RISCV_RVV(vfnmsac_vv_f32m2)
#define VFMVVF_FLOAT RISCV_RVV(vfmv_v_f_f32m2)
#define VFMVVF_FLOAT_M1 RISCV_RVV(vfmv_v_f_f32m1)
#define VFMULVV_FLOAT RISCV_RVV(vfmul_vv_f32m2)
#else
#define VSETVL(n) RISCV_RVV(vsetvl_e64m2)(n)
#define VSETVL_MAX RISCV_RVV(vsetvlmax_e64m1)()
#define FLOAT_V_T vfloat64m2_t
#define FLOAT_V_T_M1 vfloat64m1_t
#define VFMVFS_FLOAT RISCV_RVV(vfmv_f_s_f64m1_f64)
#define VLSEV_FLOAT RISCV_RVV(vlse64_v_f64m2)
#ifdef RISCV_0p10_INTRINSICS
#define VFREDSUM_FLOAT(vr, va, vb, gvl) RISCV_RVV(vfredusum_vs_f64m2_f64m1)(vr, va, vb, gvl)
#else
#define VFREDSUM_FLOAT(vr, va, vb, gvl) RISCV_RVV(vfredusum_vs_f64m2_f64m1)(va, vb, gvl)
#endif
#define VFMACCVV_FLOAT RISCV_RVV(vfmacc_vv_f64m2)
#define VFNMSACVV_FLOAT RISCV_RVV(vfnmsac_vv_f64m2)
#define VFMVVF_FLOAT RISCV_RVV(vfmv_v_f_f64m2)
#define VFMVVF_FLOAT_M1 RISCV_RVV(vfmv_v_f_f64m1)
#define VFMULVV_FLOAT RISCV_RVV(vfmul_vv_f64m2)
#endif

int CNAME(BLASLONG m, BLASLONG n, BLASLONG dummy1, FLOAT alpha_r, FLOAT alpha_i, FLOAT *a, BLASLONG lda, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *buffer)
{
	BLASLONG i = 0, j = 0, k = 0;
	BLASLONG ix = 0, iy = 0;
	FLOAT *a_ptr = a;
        FLOAT temp_r, temp_i;

        FLOAT_V_T va0, va1, vx0, vx1, vr, vi;
        unsigned int gvl = VSETVL(m);
        FLOAT_V_T_M1 v_res_r, v_res_i;
        BLASLONG stride_x = inc_x * sizeof(FLOAT) * 2;
        BLASLONG stride_a = sizeof(FLOAT) * 2;
        BLASLONG inc_xv = inc_x * gvl * 2;
        BLASLONG inc_av = gvl * 2;
        BLASLONG inc_y2 = inc_y * 2;
        BLASLONG lda2 = lda * 2;

        for(i = 0; i < n; i++){
                v_res_r = VFMVVF_FLOAT_M1(0, 1);
                v_res_i = VFMVVF_FLOAT_M1(0, 1);
                gvl = VSETVL(m);
                j = 0;
                ix = 0;
                for(k = 0; k < m/gvl; k++){
                        va0 = VLSEV_FLOAT(&a_ptr[j], stride_a, gvl);
                        va1 = VLSEV_FLOAT(&a_ptr[j+1], stride_a, gvl);
                        vx0 = VLSEV_FLOAT(&x[ix], stride_x, gvl);
                        vx1 = VLSEV_FLOAT(&x[ix+1], stride_x, gvl);
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
                        vr = VFMULVV_FLOAT(va0, vx0, gvl);
                        vi = VFMULVV_FLOAT(va0, vx1, gvl);
                        vr = VFNMSACVV_FLOAT(vr, va1, vx1, gvl);
                        vi = VFMACCVV_FLOAT(vi, va1, vx0, gvl);
#else
                        vr = VFMULVV_FLOAT(va0, vx0, gvl);
                        vi = VFMULVV_FLOAT(va0, vx1, gvl);
                        vr = VFMACCVV_FLOAT(vr, va1, vx1, gvl);
                        vi = VFNMSACVV_FLOAT(vi, va1, vx0, gvl);
#endif
                        v_res_r = VFREDSUM_FLOAT(v_res_r, vr, v_res_r, gvl);
                        v_res_i = VFREDSUM_FLOAT(v_res_i, vi, v_res_i, gvl);

                        j += inc_av;
                        ix += inc_xv;
                }
                if(j/2 < m){
                        gvl = VSETVL(m-j/2);
                        va0 = VLSEV_FLOAT(&a_ptr[j], stride_a, gvl);
                        va1 = VLSEV_FLOAT(&a_ptr[j+1], stride_a, gvl);
                        vx0 = VLSEV_FLOAT(&x[ix], stride_x, gvl);
                        vx1 = VLSEV_FLOAT(&x[ix+1], stride_x, gvl);
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
                        vr = VFMULVV_FLOAT(va0, vx0, gvl);
                        vi = VFMULVV_FLOAT(va0, vx1, gvl);
                        vr = VFNMSACVV_FLOAT(vr, va1, vx1, gvl);
                        vi = VFMACCVV_FLOAT(vi, va1, vx0, gvl);
#else
                        vr = VFMULVV_FLOAT(va0, vx0, gvl);
                        vi = VFMULVV_FLOAT(va0, vx1, gvl);
                        vr = VFMACCVV_FLOAT(vr, va1, vx1, gvl);
                        vi = VFNMSACVV_FLOAT(vi, va1, vx0, gvl);

#endif
                        v_res_r = VFREDSUM_FLOAT(v_res_r, vr, v_res_r, gvl);
                        v_res_i = VFREDSUM_FLOAT(v_res_i, vi, v_res_i, gvl);
                }

                temp_r = VFMVFS_FLOAT(v_res_r);
                temp_i = VFMVFS_FLOAT(v_res_i);

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
	return(0);
}

