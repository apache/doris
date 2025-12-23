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
#ifdef RISCV_0p10_INTRINSICS
#define VFREDSUM_FLOAT(va, vb, gvl) vfredusum_vs_f32m4_f32m1(v_res, va, vb, gvl)
#else
#define VFREDSUM_FLOAT RISCV_RVV(vfredusum_vs_f32m4_f32m1)
#endif
#define VFMACCVV_FLOAT RISCV_RVV(vfmacc_vv_f32m4)
#define VFMVVF_FLOAT RISCV_RVV(vfmv_v_f_f32m4)
#define VFMVVF_FLOAT_M1 RISCV_RVV(vfmv_v_f_f32m1)
#define VFDOTVV_FLOAT RISCV_RVV(vfdot_vv_f32m4)
#else
#define VSETVL(n) RISCV_RVV(vsetvl_e64m4)(n)
#define VSETVL_MAX RISCV_RVV(vsetvlmax_e64m1)()
#define FLOAT_V_T vfloat64m4_t
#define FLOAT_V_T_M1 vfloat64m1_t
#define VLEV_FLOAT RISCV_RVV(vle64_v_f64m4)
#define VLSEV_FLOAT RISCV_RVV(vlse64_v_f64m4)
#ifdef RISCV_0p10_INTRINSICS
#define VFREDSUM_FLOAT(va, vb, gvl) vfredusum_vs_f64m4_f64m1(v_res, va, vb, gvl)
#else
#define VFREDSUM_FLOAT RISCV_RVV(vfredusum_vs_f64m4_f64m1)
#endif
#define VFMACCVV_FLOAT RISCV_RVV(vfmacc_vv_f64m4)
#define VFMVVF_FLOAT RISCV_RVV(vfmv_v_f_f64m4)
#define VFMVVF_FLOAT_M1 RISCV_RVV(vfmv_v_f_f64m1)
#define VFDOTVV_FLOAT RISCV_RVV(vfdot_vv_f64m4)
#endif

#if defined(DSDOT)
double CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y)
#else
FLOAT CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y)
#endif
{
	BLASLONG i=0, j=0;
	double dot = 0.0 ;

	if ( n < 1 )  return(dot);

        FLOAT_V_T vr, vx, vy;
        unsigned int gvl = 0;
        FLOAT_V_T_M1 v_res, v_z0;
        gvl = VSETVL_MAX;
        v_res = VFMVVF_FLOAT_M1(0, gvl);
        v_z0 = VFMVVF_FLOAT_M1(0, gvl);

        if(inc_x == 1 && inc_y == 1){
                gvl = VSETVL(n);
                vr = VFMVVF_FLOAT(0, gvl);
                for(i=0,j=0; i<n/gvl; i++){
                        vx = VLEV_FLOAT(&x[j], gvl);
                        vy = VLEV_FLOAT(&y[j], gvl);
                        vr = VFMACCVV_FLOAT(vr, vx, vy, gvl);
                        j += gvl;
                }
                if(j > 0){
                        v_res = VFREDSUM_FLOAT(vr, v_z0, gvl);
                        dot += (double)EXTRACT_FLOAT(v_res);
                }
                //tail
                if(j < n){
                        gvl = VSETVL(n-j);
                        vx = VLEV_FLOAT(&x[j], gvl);
                        vy = VLEV_FLOAT(&y[j], gvl);
                        FLOAT_V_T vz = VFMVVF_FLOAT(0, gvl);
                        //vr = VFDOTVV_FLOAT(vx, vy, gvl);
                        vr = VFMACCVV_FLOAT(vz, vx, vy, gvl);
                        v_res = VFREDSUM_FLOAT(vr, v_z0, gvl);
                        dot += (double)EXTRACT_FLOAT(v_res);
                }
        }else if(inc_y == 1){
                gvl = VSETVL(n);
                vr = VFMVVF_FLOAT(0, gvl);
                BLASLONG stride_x = inc_x * sizeof(FLOAT);
                for(i=0,j=0; i<n/gvl; i++){
                        vx = VLSEV_FLOAT(&x[j*inc_x], stride_x, gvl);
                        vy = VLEV_FLOAT(&y[j], gvl);
                        vr = VFMACCVV_FLOAT(vr, vx, vy, gvl);
                        j += gvl;
                }
                if(j > 0){
                        v_res = VFREDSUM_FLOAT(vr, v_z0, gvl);
                        dot += (double)EXTRACT_FLOAT(v_res);
                }
                //tail
                if(j < n){
                        gvl = VSETVL(n-j);
                        vx = VLSEV_FLOAT(&x[j*inc_x], stride_x, gvl);
                        vy = VLEV_FLOAT(&y[j], gvl);
                        FLOAT_V_T vz = VFMVVF_FLOAT(0, gvl);
                        //vr = VFDOTVV_FLOAT(vx, vy, gvl);
                        vr = VFMACCVV_FLOAT(vz, vx, vy, gvl);
                        v_res = VFREDSUM_FLOAT(vr, v_z0, gvl);
                        dot += (double)EXTRACT_FLOAT(v_res);
                }
        }else if(inc_x == 1){
                gvl = VSETVL(n);
                vr = VFMVVF_FLOAT(0, gvl);
                BLASLONG stride_y = inc_y * sizeof(FLOAT);
                for(i=0,j=0; i<n/gvl; i++){
                        vx = VLEV_FLOAT(&x[j], gvl);
                        vy = VLSEV_FLOAT(&y[j*inc_y], stride_y, gvl);
                        vr = VFMACCVV_FLOAT(vr, vx, vy, gvl);
                        j += gvl;
                }
                if(j > 0){
                        v_res = VFREDSUM_FLOAT(vr, v_z0, gvl);
                        dot += (double)EXTRACT_FLOAT(v_res);
                }
                //tail
                if(j < n){
                        gvl = VSETVL(n-j);
                        vx = VLEV_FLOAT(&x[j], gvl);
                        vy = VLSEV_FLOAT(&y[j*inc_y], stride_y, gvl);
                        FLOAT_V_T vz = VFMVVF_FLOAT(0, gvl);
                        //vr = VFDOTVV_FLOAT(vx, vy, gvl);
                        vr = VFMACCVV_FLOAT(vz, vx, vy, gvl);
                        v_res = VFREDSUM_FLOAT(vr, v_z0, gvl);
                        dot += (double)EXTRACT_FLOAT(v_res);
                }
        }else{
                gvl = VSETVL(n);
                vr = VFMVVF_FLOAT(0, gvl);
                BLASLONG stride_x = inc_x * sizeof(FLOAT);
                BLASLONG stride_y = inc_y * sizeof(FLOAT);
                for(i=0,j=0; i<n/gvl; i++){
                        vx = VLSEV_FLOAT(&x[j*inc_x], stride_x, gvl);
                        vy = VLSEV_FLOAT(&y[j*inc_y], stride_y, gvl);
                        vr = VFMACCVV_FLOAT(vr, vx, vy, gvl);
                        j += gvl;
                }
                if(j > 0){
                        v_res = VFREDSUM_FLOAT(vr, v_z0, gvl);
                        dot += (double)EXTRACT_FLOAT(v_res);
                }
                //tail
                if(j < n){
                        gvl = VSETVL(n-j);
                        vx = VLSEV_FLOAT(&x[j*inc_x], stride_x, gvl);
                        vy = VLSEV_FLOAT(&y[j*inc_y], stride_y, gvl);
                        FLOAT_V_T vz = VFMVVF_FLOAT(0, gvl);
                        //vr = VFDOTVV_FLOAT(vx, vy, gvl);
                        vr = VFMACCVV_FLOAT(vz, vx, vy, gvl);
                        v_res = VFREDSUM_FLOAT(vr, v_z0, gvl);
                        dot += (double)EXTRACT_FLOAT(v_res);
                }
        }
	return(dot);
}


