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
#define VLSEV_FLOAT RISCV_RVV(vlse32_v_f32m4)
#define VSSEV_FLOAT RISCV_RVV(vsse32_v_f32m4)
#define VFMACCVF_FLOAT RISCV_RVV(vfmacc_vf_f32m4)
#define VFMULVF_FLOAT RISCV_RVV(vfmul_vf_f32m4)
#define VFNMSACVF_FLOAT RISCV_RVV(vfnmsac_vf_f32m4)
#define VFMVVF_FLOAT RISCV_RVV(vfmv_v_f_f32m4)
#else
#define VSETVL(n) RISCV_RVV(vsetvl_e64m4)(n)
#define VSETVL_MAX RISCV_RVV(vsetvlmax_e64m1)()
#define FLOAT_V_T vfloat64m4_t
#define VLSEV_FLOAT RISCV_RVV(vlse64_v_f64m4)
#define VSSEV_FLOAT RISCV_RVV(vsse64_v_f64m4)
#define VFMACCVF_FLOAT RISCV_RVV(vfmacc_vf_f64m4)
#define VFMULVF_FLOAT RISCV_RVV(vfmul_vf_f64m4)
#define VFNMSACVF_FLOAT RISCV_RVV(vfnmsac_vf_f64m4)
#define VFMVVF_FLOAT RISCV_RVV(vfmv_v_f_f64m4)
#endif

int CNAME(BLASLONG n, BLASLONG dummy0, BLASLONG dummy1, FLOAT da_r,FLOAT da_i, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *dummy, BLASLONG dummy2)
{
        BLASLONG i=0, j=0;
        BLASLONG ix=0;


        if((n <= 0) || (inc_x <= 0))
                return(0);

	if (dummy2 == 0 && da_r == 0. && da_i == 0.) {
		int i,inc_x2,ix;
		inc_x2 = 2*inc_x;
		ix=0;
		for (i=0;i<n;i++){x[ix]=0.;x[ix+1]=0.;ix+=inc_x2;}
	} else {
            unsigned int gvl = 0;
            FLOAT_V_T vt, v0, v1;
            {
                gvl = VSETVL(n);
                BLASLONG stride_x = inc_x * 2 * sizeof(FLOAT);
                BLASLONG inc_xv = inc_x * 2 * gvl;
                for(i=0,j=0; i < n/gvl; i++){
                        v0 = VLSEV_FLOAT(&x[ix], stride_x, gvl);
                        v1 = VLSEV_FLOAT(&x[ix+1], stride_x, gvl);

                        vt = VFMULVF_FLOAT(v0, da_r, gvl);
                        vt = VFNMSACVF_FLOAT(vt, da_i, v1, gvl);
                        v1 = VFMULVF_FLOAT(v1, da_r, gvl);
                        v1 = VFMACCVF_FLOAT(v1, da_i, v0, gvl);

                        VSSEV_FLOAT(&x[ix], stride_x, vt, gvl);
                        VSSEV_FLOAT(&x[ix+1], stride_x, v1, gvl);

                        j += gvl;
                        ix += inc_xv;
                }
                if(j < n){
                        gvl = VSETVL(n-j);
                        v0 = VLSEV_FLOAT(&x[ix], stride_x, gvl);
                        v1 = VLSEV_FLOAT(&x[ix+1], stride_x, gvl);

                        vt = VFMULVF_FLOAT(v0, da_r, gvl);
                        vt = VFNMSACVF_FLOAT(vt, da_i, v1, gvl);
                        v1 = VFMULVF_FLOAT(v1, da_r, gvl);
                        v1 = VFMACCVF_FLOAT(v1, da_i, v0, gvl);

                        VSSEV_FLOAT(&x[ix], stride_x, vt, gvl);
                        VSSEV_FLOAT(&x[ix+1], stride_x, v1, gvl);
                }
            }
        }
        return(0);
}
