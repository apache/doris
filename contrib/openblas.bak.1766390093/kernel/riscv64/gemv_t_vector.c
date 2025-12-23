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
#define FLOAT_V_T vfloat32m2_t
#define FLOAT_V_T_M1 vfloat32m1_t
#define VLEV_FLOAT RISCV_RVV(vle32_v_f32m2)
#define VLSEV_FLOAT RISCV_RVV(vlse32_v_f32m2)
#ifdef RISCV_0p10_INTRINSICS
#define VFREDSUM_FLOAT(va, vb, gvl) vfredusum_vs_f32m2_f32m1(v_res, va, vb, gvl)
#else
#define VFREDSUM_FLOAT RISCV_RVV(vfredusum_vs_f32m2_f32m1)
#endif
#define VFMACCVV_FLOAT RISCV_RVV(vfmacc_vv_f32m2)
#define VFMVVF_FLOAT RISCV_RVV(vfmv_v_f_f32m2)
#define VFMVVF_FLOAT_M1 RISCV_RVV(vfmv_v_f_f32m1)
#define VFMULVV_FLOAT RISCV_RVV(vfmul_vv_f32m2)
#define xint_t int
#else
#define VSETVL(n) RISCV_RVV(vsetvl_e64m2)(n)
#define FLOAT_V_T vfloat64m2_t
#define FLOAT_V_T_M1 vfloat64m1_t
#define VLEV_FLOAT RISCV_RVV(vle64_v_f64m2)
#define VLSEV_FLOAT RISCV_RVV(vlse64_v_f64m2)
#ifdef RISCV_0p10_INTRINSICS
#define VFREDSUM_FLOAT(va, vb, gvl) vfredusum_vs_f64m2_f64m1(v_res, va, vb, gvl)
#else
#define VFREDSUM_FLOAT RISCV_RVV(vfredusum_vs_f64m2_f64m1)
#endif
#define VFMACCVV_FLOAT RISCV_RVV(vfmacc_vv_f64m2)
#define VFMVVF_FLOAT RISCV_RVV(vfmv_v_f_f64m2)
#define VFMVVF_FLOAT_M1 RISCV_RVV(vfmv_v_f_f64m1)
#define VFMULVV_FLOAT RISCV_RVV(vfmul_vv_f64m2)
#define xint_t long long
#endif

int CNAME(BLASLONG m, BLASLONG n, BLASLONG dummy1, FLOAT alpha, FLOAT *a, BLASLONG lda, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *buffer)
{
	BLASLONG i = 0, j = 0, k = 0;
	BLASLONG ix = 0, iy = 0;
	FLOAT *a_ptr = a;
        FLOAT temp;

        FLOAT_V_T va, vr, vx;
        unsigned int gvl = 0;
        FLOAT_V_T_M1 v_res;


        if(inc_x == 1){
                for(i = 0; i < n; i++){
                        v_res = VFMVVF_FLOAT_M1(0, 1);
                        gvl = VSETVL(m);
                        j = 0;
                        vr = VFMVVF_FLOAT(0, gvl);
                        for(k = 0; k < m/gvl; k++){
                                va = VLEV_FLOAT(&a_ptr[j], gvl);
                                vx = VLEV_FLOAT(&x[j], gvl);
                                vr = VFMULVV_FLOAT(va, vx, gvl);                // could vfmacc here and reduce outside loop
                                v_res = VFREDSUM_FLOAT(vr, v_res, gvl);         // but that reordering diverges far enough from scalar path to make tests fail
                                j += gvl;
                        }
                        if(j < m){
                                gvl = VSETVL(m-j);
                                va = VLEV_FLOAT(&a_ptr[j], gvl);
                                vx = VLEV_FLOAT(&x[j], gvl);
                                vr = VFMULVV_FLOAT(va, vx, gvl);
                                v_res = VFREDSUM_FLOAT(vr, v_res, gvl);
                        }
                        temp = (FLOAT)EXTRACT_FLOAT(v_res);
                        y[iy] += alpha * temp;


                        iy += inc_y;
                        a_ptr += lda;
                }
        }else{
                BLASLONG stride_x = inc_x * sizeof(FLOAT);
                for(i = 0; i < n; i++){
                        v_res = VFMVVF_FLOAT_M1(0, 1);
                        gvl = VSETVL(m);
                        j = 0;
                        ix = 0;
                        vr = VFMVVF_FLOAT(0, gvl);
                        for(k = 0; k < m/gvl; k++){
                                va = VLEV_FLOAT(&a_ptr[j], gvl);
                                vx = VLSEV_FLOAT(&x[ix], stride_x, gvl);
                                vr = VFMULVV_FLOAT(va, vx, gvl);
                                v_res = VFREDSUM_FLOAT(vr, v_res, gvl);
                                j += gvl;
                                ix += inc_x * gvl;
                        }
                        if(j < m){
                                gvl = VSETVL(m-j);
                                va = VLEV_FLOAT(&a_ptr[j], gvl);
                                vx = VLSEV_FLOAT(&x[ix], stride_x, gvl);
                                vr = VFMULVV_FLOAT(va, vx, gvl);
                                v_res = VFREDSUM_FLOAT(vr, v_res, gvl);
                        }
                        temp = (FLOAT)EXTRACT_FLOAT(v_res);
                        y[iy] += alpha * temp;


                        iy += inc_y;
                        a_ptr += lda;
                }
        }


	return(0);
}
