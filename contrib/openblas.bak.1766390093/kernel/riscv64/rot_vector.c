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
#define VLEV_FLOAT RISCV_RVV(vle32_v_f32m4)
#define VLSEV_FLOAT RISCV_RVV(vlse32_v_f32m4)
#define VSEV_FLOAT RISCV_RVV(vse32_v_f32m4)
#define VSSEV_FLOAT RISCV_RVV(vsse32_v_f32m4)
#define VFMACCVF_FLOAT RISCV_RVV(vfmacc_vf_f32m4)
#define VFMULVF_FLOAT RISCV_RVV(vfmul_vf_f32m4)
#define VFMSACVF_FLOAT RISCV_RVV(vfmsac_vf_f32m4)
#else
#define VSETVL(n) RISCV_RVV(vsetvl_e64m4)(n)
#define VSETVL_MAX RISCV_RVV(vsetvlmax_e64m1)()
#define FLOAT_V_T vfloat64m4_t
#define VLEV_FLOAT RISCV_RVV(vle64_v_f64m4)
#define VLSEV_FLOAT RISCV_RVV(vlse64_v_f64m4)
#define VSEV_FLOAT RISCV_RVV(vse64_v_f64m4)
#define VSSEV_FLOAT RISCV_RVV(vsse64_v_f64m4)
#define VFMACCVF_FLOAT RISCV_RVV(vfmacc_vf_f64m4)
#define VFMULVF_FLOAT RISCV_RVV(vfmul_vf_f64m4)
#define VFMSACVF_FLOAT RISCV_RVV(vfmsac_vf_f64m4)
#endif

int CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT c, FLOAT s)
{
	BLASLONG i=0, j=0;
	BLASLONG ix=0,iy=0;

	if(n <= 0)  return(0);
        unsigned int gvl = VSETVL((inc_x != 0 && inc_y != 0) ? n : 1);
        FLOAT_V_T v0, v1, vx, vy;

        if(inc_x == 1 && inc_y == 1){
                for(i=0,j=0; i<n/gvl; i++){
                        vx = VLEV_FLOAT(&x[j], gvl);
                        vy = VLEV_FLOAT(&y[j], gvl);

                        v0 = VFMULVF_FLOAT(vx, c, gvl);
                        v0 = VFMACCVF_FLOAT(v0, s, vy, gvl);
                        VSEV_FLOAT(&x[j], v0, gvl);

                        v1 = VFMULVF_FLOAT(vx, s, gvl);
                        v1 = VFMSACVF_FLOAT(v1, c, vy, gvl);
                        VSEV_FLOAT(&y[j], v1, gvl);

                        j += gvl;
                }
                if(j<n){
                        gvl = VSETVL(n-j);
                        vx = VLEV_FLOAT(&x[j], gvl);
                        vy = VLEV_FLOAT(&y[j], gvl);

                        v0 = VFMULVF_FLOAT(vx, c, gvl);
                        v0 = VFMACCVF_FLOAT(v0, s, vy, gvl);
                        VSEV_FLOAT(&x[j], v0, gvl);

                        v1 = VFMULVF_FLOAT(vx, s, gvl);
                        v1 = VFMSACVF_FLOAT(v1, c, vy, gvl);
                        VSEV_FLOAT(&y[j], v1, gvl);
                }
        }else if(inc_y == 1){
                BLASLONG stride_x = inc_x * sizeof(FLOAT);
                BLASLONG inc_xv = inc_x * gvl;
                for(i=0,j=0; i<n/gvl; i++){
                        vx = VLSEV_FLOAT(&x[ix], stride_x, gvl);
                        vy = VLEV_FLOAT(&y[j], gvl);

                        v0 = VFMULVF_FLOAT(vx, c, gvl);
                        v0 = VFMACCVF_FLOAT(v0, s, vy, gvl);
                        VSSEV_FLOAT(&x[ix], stride_x, v0, gvl);

                        v1 = VFMULVF_FLOAT(vx, s, gvl);
                        v1 = VFMSACVF_FLOAT(v1, c, vy, gvl);
                        VSEV_FLOAT(&y[j], v1, gvl);

                        j += gvl;
                        ix += inc_xv;
                }
                if(j<n){
                        gvl = VSETVL(n-j);
                        vx = VLSEV_FLOAT(&x[j*inc_x], stride_x, gvl);
                        vy = VLEV_FLOAT(&y[j], gvl);

                        v0 = VFMULVF_FLOAT(vx, c, gvl);
                        v0 = VFMACCVF_FLOAT(v0, s, vy, gvl);
                        VSSEV_FLOAT(&x[j*inc_x], stride_x, v0, gvl);

                        v1 = VFMULVF_FLOAT(vx, s, gvl);
                        v1 = VFMSACVF_FLOAT(v1, c, vy, gvl);
                        VSEV_FLOAT(&y[j], v1, gvl);
                }
        }else if(inc_x == 1){
                BLASLONG stride_y = inc_y * sizeof(FLOAT);
                BLASLONG inc_yv = inc_y * gvl;
                for(i=0,j=0; i<n/gvl; i++){
                        vx = VLEV_FLOAT(&x[j], gvl);
                        vy = VLSEV_FLOAT(&y[iy], stride_y, gvl);

                        v0 = VFMULVF_FLOAT(vx, c, gvl);
                        v0 = VFMACCVF_FLOAT(v0, s, vy, gvl);
                        VSEV_FLOAT(&x[j], v0, gvl);

                        v1 = VFMULVF_FLOAT(vx, s, gvl);
                        v1 = VFMSACVF_FLOAT(v1, c, vy, gvl);
                        VSSEV_FLOAT(&y[iy], stride_y, v1, gvl);

                        j += gvl;
                        iy += inc_yv;
                }
                if(j<n){
                        gvl = VSETVL(n-j);
                        vx = VLEV_FLOAT(&x[j], gvl);
                        vy = VLSEV_FLOAT(&y[j*inc_y],stride_y, gvl);

                        v0 = VFMULVF_FLOAT(vx, c, gvl);
                        v0 = VFMACCVF_FLOAT(v0, s, vy, gvl);
                        VSEV_FLOAT(&x[j], v0, gvl);

                        v1 = VFMULVF_FLOAT(vx, s, gvl);
                        v1 = VFMSACVF_FLOAT(v1, c, vy, gvl);
                        VSSEV_FLOAT(&y[j*inc_y], stride_y, v1, gvl);
                }
        }else{
                BLASLONG stride_x = inc_x * sizeof(FLOAT);
                BLASLONG stride_y = inc_y * sizeof(FLOAT);
                BLASLONG inc_xv = inc_x * gvl;
                BLASLONG inc_yv = inc_y * gvl;
                for(i=0,j=0; i<n/gvl; i++){
                        vx = VLSEV_FLOAT(&x[ix], stride_x, gvl);
                        vy = VLSEV_FLOAT(&y[iy], stride_y, gvl);

                        v0 = VFMULVF_FLOAT(vx, c, gvl);
                        v0 = VFMACCVF_FLOAT(v0, s, vy, gvl);
                        VSSEV_FLOAT(&x[ix], stride_x, v0, gvl);

                        v1 = VFMULVF_FLOAT(vx, s, gvl);
                        v1 = VFMSACVF_FLOAT(v1, c, vy, gvl);
                        VSSEV_FLOAT(&y[iy], stride_y, v1, gvl);

                        j += gvl;
                        ix += inc_xv;
                        iy += inc_yv;
                }
                if(j<n){
                        gvl = VSETVL(n-j);
                        vx = VLSEV_FLOAT(&x[j*inc_x],stride_x, gvl);
                        vy = VLSEV_FLOAT(&y[j*inc_y],stride_y, gvl);

                        v0 = VFMULVF_FLOAT(vx, c, gvl);
                        v0 = VFMACCVF_FLOAT(v0, s, vy, gvl);
                        VSSEV_FLOAT(&x[j*inc_x], stride_x, v0, gvl);

                        v1 = VFMULVF_FLOAT(vx, s, gvl);
                        v1 = VFMSACVF_FLOAT(v1, c, vy, gvl);
                        VSSEV_FLOAT(&y[j*inc_y], stride_y, v1, gvl);
                }
        }
	return(0);

}


