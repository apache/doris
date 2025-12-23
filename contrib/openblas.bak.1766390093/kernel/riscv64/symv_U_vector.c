/***************************************************************************
Copyright (c) 2020, The OpenBLAS Project
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
#define VSETVL(n) RISCV_RVV(vsetvl_e32m4)(n)
#define VSETVL_MAX RISCV_RVV(vsetvlmax_e32m1)()
#define FLOAT_V_T vfloat32m4_t
#define FLOAT_V_T_M1 vfloat32m1_t
#define VLEV_FLOAT RISCV_RVV(vle32_v_f32m4)
#define VLSEV_FLOAT RISCV_RVV(vlse32_v_f32m4)
#define VSEV_FLOAT RISCV_RVV(vse32_v_f32m4)
#define VSSEV_FLOAT RISCV_RVV(vsse32_v_f32m4)
#ifdef RISCV_0p10_INTRINSICS
#define VFREDSUM_FLOAT(va, vb, gvl) vfredusum_vs_f32m4_f32m1(v_res, va, vb, gvl)
#else
#define VFREDSUM_FLOAT RISCV_RVV(vfredusum_vs_f32m4_f32m1)
#endif
#define VFMACCVV_FLOAT RISCV_RVV(vfmacc_vv_f32m4)
#define VFMACCVF_FLOAT RISCV_RVV(vfmacc_vf_f32m4)
#define VFMVVF_FLOAT RISCV_RVV(vfmv_v_f_f32m4)
#define VFMVVF_FLOAT_M1 RISCV_RVV(vfmv_v_f_f32m1)
#define VFDOTVV_FLOAT RISCV_RVV(vfdot_vv_f32m4)
#define VFMULVV_FLOAT RISCV_RVV(vfmul_vv_f32m4)
#else
#define VSETVL(n) RISCV_RVV(vsetvl_e64m4)(n)
#define VSETVL_MAX RISCV_RVV(vsetvlmax_e64m1)()
#define FLOAT_V_T vfloat64m4_t
#define FLOAT_V_T_M1 vfloat64m1_t
#define VLEV_FLOAT RISCV_RVV(vle64_v_f64m4)
#define VLSEV_FLOAT RISCV_RVV(vlse64_v_f64m4)
#define VSEV_FLOAT RISCV_RVV(vse64_v_f64m4)
#define VSSEV_FLOAT RISCV_RVV(vsse64_v_f64m4)
#ifdef RISCV_0p10_INTRINSICS
#define VFREDSUM_FLOAT(va, vb, gvl) vfredusum_vs_f64m4_f64m1(v_res, va, vb, gvl)
#else
#define VFREDSUM_FLOAT RISCV_RVV(vfredusum_vs_f64m4_f64m1)
#endif
#define VFMACCVV_FLOAT RISCV_RVV(vfmacc_vv_f64m4)
#define VFMACCVF_FLOAT RISCV_RVV(vfmacc_vf_f64m4)
#define VFMVVF_FLOAT RISCV_RVV(vfmv_v_f_f64m4)
#define VFMVVF_FLOAT_M1 RISCV_RVV(vfmv_v_f_f64m1)
#define VFDOTVV_FLOAT RISCV_RVV(vfdot_vv_f64m4)
#define VFMULVV_FLOAT RISCV_RVV(vfmul_vv_f64m4)
#endif

int CNAME(BLASLONG m, BLASLONG offset, FLOAT alpha, FLOAT *a, BLASLONG lda, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *buffer)
{
        BLASLONG i, j, k;
        BLASLONG ix,iy;
        BLASLONG jx,jy;
        FLOAT temp1;
        FLOAT temp2;
        FLOAT *a_ptr = a;
        unsigned int gvl = 0;
        FLOAT_V_T_M1 v_res, v_z0;
        gvl = VSETVL_MAX;
        v_res = VFMVVF_FLOAT_M1(0, gvl);
        v_z0 = VFMVVF_FLOAT_M1(0, gvl);

        FLOAT_V_T va, vx, vy, vr;
        BLASLONG stride_x, stride_y, inc_xv, inc_yv;

        BLASLONG m1 = m - offset;
        if(inc_x == 1 && inc_y == 1){
                a_ptr += m1 * lda;
                for (j=m1; j<m; j++)
                {
                        temp1 = alpha * x[j];
                        temp2 = 0.0;
                        if(j > 0){
                                i = 0;
                                gvl = VSETVL(j);
                                vr = VFMVVF_FLOAT(0, gvl);
                                for(k = 0; k < j / gvl; k++){
                                        vy = VLEV_FLOAT(&y[i], gvl);
                                        va = VLEV_FLOAT(&a_ptr[i], gvl);
                                        vy = VFMACCVF_FLOAT(vy, temp1, va, gvl);
                                        VSEV_FLOAT(&y[i], vy, gvl);

                                        vx = VLEV_FLOAT(&x[i], gvl);
                                        vr = VFMACCVV_FLOAT(vr, vx, va, gvl);

                                        i += gvl;
                                }
                                v_res = VFREDSUM_FLOAT(vr, v_z0, gvl);
                                temp2 = EXTRACT_FLOAT(v_res);
                                if(i < j){
                                        gvl = VSETVL(j-i);
                                        vy = VLEV_FLOAT(&y[i], gvl);
                                        va = VLEV_FLOAT(&a_ptr[i], gvl);
                                        vy = VFMACCVF_FLOAT(vy, temp1, va, gvl);
                                        VSEV_FLOAT(&y[i], vy, gvl);

                                        vx = VLEV_FLOAT(&x[i], gvl);
                                        vr = VFMULVV_FLOAT(vx, va, gvl);
                                        v_res = VFREDSUM_FLOAT(vr, v_z0, gvl);
                                        temp2 += EXTRACT_FLOAT(v_res);
                                }
                        }
                        y[j] += temp1 * a_ptr[j] + alpha * temp2;
                        a_ptr += lda;
                }
        }else if(inc_x == 1){
                jy = m1 * inc_y;
                a_ptr += m1 * lda;
                stride_y = inc_y * sizeof(FLOAT);
                for (j=m1; j<m; j++)
                {
                        temp1 = alpha * x[j];
                        temp2 = 0.0;
                        if(j > 0){
                                iy = 0;
                                i = 0;
                                gvl = VSETVL(j);
                                inc_yv = inc_y * gvl;
                                vr = VFMVVF_FLOAT(0, gvl);
                                for(k = 0; k < j / gvl; k++){
                                        vy = VLSEV_FLOAT(&y[iy], stride_y, gvl);
                                        va = VLEV_FLOAT(&a_ptr[i], gvl);
                                        vy = VFMACCVF_FLOAT(vy, temp1, va, gvl);
                                        VSSEV_FLOAT(&y[iy], stride_y, vy, gvl);

                                        vx = VLEV_FLOAT(&x[i], gvl);
                                        vr = VFMACCVV_FLOAT(vr, vx, va, gvl);

                                        i += gvl;
                                        iy += inc_yv;
                                }
                                v_res = VFREDSUM_FLOAT(vr, v_z0, gvl);
                                temp2 = EXTRACT_FLOAT(v_res);
                                if(i < j){
                                        gvl = VSETVL(j-i);
                                        vy = VLSEV_FLOAT(&y[iy], stride_y, gvl);
                                        va = VLEV_FLOAT(&a_ptr[i], gvl);
                                        vy = VFMACCVF_FLOAT(vy, temp1, va, gvl);
                                        VSSEV_FLOAT(&y[iy], stride_y, vy, gvl);

                                        vx = VLEV_FLOAT(&x[i], gvl);
                                        vr = VFMULVV_FLOAT(vx, va, gvl);
                                        v_res = VFREDSUM_FLOAT(vr, v_z0, gvl);
                                        temp2 += EXTRACT_FLOAT(v_res);
                                }
                        }
                        y[jy] += temp1 * a_ptr[j] + alpha * temp2;
                        a_ptr += lda;
                        jy    += inc_y;
                }
        }else if(inc_y == 1){
                jx = m1 * inc_x;
                a_ptr += m1 * lda;
                stride_x = inc_x * sizeof(FLOAT);
                for (j=m1; j<m; j++)
                {
                        temp1 = alpha * x[jx];
                        temp2 = 0.0;
                        if(j > 0){
                                ix = 0;
                                i = 0;
                                gvl = VSETVL(j);
                                inc_xv = inc_x * gvl;
                                vr = VFMVVF_FLOAT(0, gvl);
                                for(k = 0; k < j / gvl; k++){
                                        vy = VLEV_FLOAT(&y[i], gvl);
                                        va = VLEV_FLOAT(&a_ptr[i], gvl);
                                        vy = VFMACCVF_FLOAT(vy, temp1, va, gvl);
                                        VSEV_FLOAT(&y[i], vy, gvl);

                                        vx = VLSEV_FLOAT(&x[ix], stride_x, gvl);
                                        vr = VFMACCVV_FLOAT(vr, vx, va, gvl);

                                        i += gvl;
                                        ix += inc_xv;
                                }
                                v_res = VFREDSUM_FLOAT(vr, v_z0, gvl);
                                temp2 = EXTRACT_FLOAT(v_res);
                                if(i < j){
                                        gvl = VSETVL(j-i);
                                        vy = VLEV_FLOAT(&y[i], gvl);
                                        va = VLEV_FLOAT(&a_ptr[i], gvl);
                                        vy = VFMACCVF_FLOAT(vy, temp1, va, gvl);
                                        VSEV_FLOAT(&y[i], vy, gvl);

                                        vx = VLSEV_FLOAT(&x[ix], stride_x, gvl);
                                        vr = VFMULVV_FLOAT(vx, va, gvl);
                                        v_res = VFREDSUM_FLOAT(vr, v_z0, gvl);
                                        temp2 += EXTRACT_FLOAT(v_res);
                                }
                        }
                        y[j] += temp1 * a_ptr[j] + alpha * temp2;
                        a_ptr += lda;
                        jx    += inc_x;
                }
        }else{
                jx = m1 * inc_x;
                jy = m1 * inc_y;
                a_ptr += m1 * lda;
                stride_x = inc_x * sizeof(FLOAT);
                stride_y = inc_y * sizeof(FLOAT);
                for (j=m1; j<m; j++)
                {
                        temp1 = alpha * x[jx];
                        temp2 = 0.0;
                        if(j > 0){
                                ix = 0;
                                iy = 0;
                                i = 0;
                                gvl = VSETVL(j);
                                inc_xv = inc_x * gvl;
                                inc_yv = inc_y * gvl;
                                vr = VFMVVF_FLOAT(0, gvl);
                                for(k = 0; k < j / gvl; k++){
                                        vy = VLSEV_FLOAT(&y[iy], stride_y, gvl);
                                        va = VLEV_FLOAT(&a_ptr[i], gvl);
                                        vy = VFMACCVF_FLOAT(vy, temp1, va, gvl);
                                        VSSEV_FLOAT(&y[iy], stride_y, vy, gvl);

                                        vx = VLSEV_FLOAT(&x[ix], stride_x, gvl);
                                        vr = VFMACCVV_FLOAT(vr, vx, va, gvl);

                                        i += gvl;
                                        ix += inc_xv;
                                        iy += inc_yv;
                                }
                                v_res = VFREDSUM_FLOAT(vr, v_z0, gvl);
                                temp2 = EXTRACT_FLOAT(v_res);
                                if(i < j){
                                        gvl = VSETVL(j-i);
                                        vy = VLSEV_FLOAT(&y[iy], stride_y, gvl);
                                        va = VLEV_FLOAT(&a_ptr[i], gvl);
                                        vy = VFMACCVF_FLOAT(vy, temp1, va, gvl);
                                        VSSEV_FLOAT(&y[iy], stride_y, vy, gvl);

                                        vx = VLSEV_FLOAT(&x[ix], stride_x, gvl);
                                        vr = VFMULVV_FLOAT(vx, va, gvl);
                                        v_res = VFREDSUM_FLOAT(vr, v_z0, gvl);
                                        temp2 += EXTRACT_FLOAT(v_res);
                                }
                        }
                        y[jy] += temp1 * a_ptr[j] + alpha * temp2;
                        a_ptr += lda;
                        jx    += inc_x;
                        jy    += inc_y;
                }
        }
        return(0);
}

