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
#define VFNMSACVF_FLOAT RISCV_RVV(vfnmsac_vf_f32m4)
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
#define VFNMSACVF_FLOAT RISCV_RVV(vfnmsac_vf_f64m4)
#endif

int CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT c, FLOAT s)
{
        BLASLONG i=0, j=0;
        BLASLONG ix=0,iy=0;

        if (n < 1)  return(0);
        unsigned int gvl = 0;

        FLOAT_V_T vt0, vt1, vx0, vx1, vy0, vy1;
        gvl = VSETVL((inc_x != 0 && inc_y != 0) ? n : 1);
        BLASLONG stride_x = inc_x * 2 * sizeof(FLOAT);
        BLASLONG stride_y = inc_y * 2 * sizeof(FLOAT);
        BLASLONG inc_xv = inc_x * 2 * gvl;
        BLASLONG inc_yv = inc_y * 2 * gvl;

	if(inc_x==1 && inc_y==1){
		for(i=0,j=0; i < n/gvl; i++){
			vx0 = VLEV_FLOAT(&x[ix], gvl);
			vx1 = VLEV_FLOAT(&x[ix+gvl], gvl);
			vy0 = VLEV_FLOAT(&y[ix], gvl);
			vy1 = VLEV_FLOAT(&y[ix+gvl], gvl);

			vt0 = VFMULVF_FLOAT(vx0, c, gvl);
			vt0 = VFMACCVF_FLOAT(vt0, s, vy0, gvl);
			vt1 = VFMULVF_FLOAT(vx1, c, gvl);
			vt1 = VFMACCVF_FLOAT(vt1, s, vy1, gvl);
			vy0 = VFMULVF_FLOAT(vy0, c, gvl);
			vy0 = VFNMSACVF_FLOAT(vy0, s, vx0, gvl);
			vy1 = VFMULVF_FLOAT(vy1, c, gvl);
			vy1 = VFNMSACVF_FLOAT(vy1, s, vx1, gvl);

			VSEV_FLOAT(&x[ix], vt0, gvl);
			VSEV_FLOAT(&x[ix+gvl], vt1, gvl);
			VSEV_FLOAT(&y[ix], vy0, gvl);
			VSEV_FLOAT(&y[ix+gvl], vy1, gvl);

			j += gvl;
			ix += 2*gvl;
		}
		if(j < n){
			gvl = VSETVL(n-j);
						vx0 = VLEV_FLOAT(&x[ix], gvl);
			vx1 = VLEV_FLOAT(&x[ix+gvl], gvl);
			vy0 = VLEV_FLOAT(&y[ix], gvl);
			vy1 = VLEV_FLOAT(&y[ix+gvl], gvl);

			vt0 = VFMULVF_FLOAT(vx0, c, gvl);
			vt0 = VFMACCVF_FLOAT(vt0, s, vy0, gvl);
			vt1 = VFMULVF_FLOAT(vx1, c, gvl);
			vt1 = VFMACCVF_FLOAT(vt1, s, vy1, gvl);
			vy0 = VFMULVF_FLOAT(vy0, c, gvl);
			vy0 = VFNMSACVF_FLOAT(vy0, s, vx0, gvl);
			vy1 = VFMULVF_FLOAT(vy1, c, gvl);
			vy1 = VFNMSACVF_FLOAT(vy1, s, vx1, gvl);

			VSEV_FLOAT(&x[ix], vt0, gvl);
			VSEV_FLOAT(&x[ix+gvl], vt1, gvl);
			VSEV_FLOAT(&y[ix], vy0, gvl);
			VSEV_FLOAT(&y[ix+gvl], vy1, gvl);
		}
		
	}else{
		for(i=0,j=0; i < n/gvl; i++){
			vx0 = VLSEV_FLOAT(&x[ix], stride_x, gvl);
			vx1 = VLSEV_FLOAT(&x[ix+1], stride_x, gvl);
			vy0 = VLSEV_FLOAT(&y[iy], stride_y, gvl);
			vy1 = VLSEV_FLOAT(&y[iy+1], stride_y, gvl);

			vt0 = VFMULVF_FLOAT(vx0, c, gvl);
			vt0 = VFMACCVF_FLOAT(vt0, s, vy0, gvl);
			vt1 = VFMULVF_FLOAT(vx1, c, gvl);
			vt1 = VFMACCVF_FLOAT(vt1, s, vy1, gvl);
			vy0 = VFMULVF_FLOAT(vy0, c, gvl);
			vy0 = VFNMSACVF_FLOAT(vy0, s, vx0, gvl);
			vy1 = VFMULVF_FLOAT(vy1, c, gvl);
			vy1 = VFNMSACVF_FLOAT(vy1, s, vx1, gvl);

			VSSEV_FLOAT(&x[ix], stride_x, vt0, gvl);
			VSSEV_FLOAT(&x[ix+1], stride_x, vt1, gvl);
			VSSEV_FLOAT(&y[iy], stride_y, vy0, gvl);
			VSSEV_FLOAT(&y[iy+1], stride_y, vy1, gvl);

			j += gvl;
			ix += inc_xv;
			iy += inc_yv;
		}
		if(j < n){
			gvl = VSETVL(n-j);
			vx0 = VLSEV_FLOAT(&x[ix], stride_x, gvl);
			vx1 = VLSEV_FLOAT(&x[ix+1], stride_x, gvl);
			vy0 = VLSEV_FLOAT(&y[iy], stride_y, gvl);
			vy1 = VLSEV_FLOAT(&y[iy+1], stride_y, gvl);

			vt0 = VFMULVF_FLOAT(vx0, c, gvl);
			vt0 = VFMACCVF_FLOAT(vt0, s, vy0, gvl);
			vt1 = VFMULVF_FLOAT(vx1, c, gvl);
			vt1 = VFMACCVF_FLOAT(vt1, s, vy1, gvl);
			vy0 = VFMULVF_FLOAT(vy0, c, gvl);
			vy0 = VFNMSACVF_FLOAT(vy0, s, vx0, gvl);
			vy1 = VFMULVF_FLOAT(vy1, c, gvl);
			vy1 = VFNMSACVF_FLOAT(vy1, s, vx1, gvl);

			VSSEV_FLOAT(&x[ix], stride_x, vt0, gvl);
			VSSEV_FLOAT(&x[ix+1], stride_x, vt1, gvl);
			VSSEV_FLOAT(&y[iy], stride_y, vy0, gvl);
			VSSEV_FLOAT(&y[iy+1], stride_y, vy1, gvl);
		}
	}
        return(0);
}
