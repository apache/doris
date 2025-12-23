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
#define VSETVL(n) RISCV_RVV(vsetvl_e32m4)(n)
#define FLOAT_V_T vfloat32m4_t
#define VLSEV_FLOAT RISCV_RVV(vlse32_v_f32m4)
#define VSSEV_FLOAT RISCV_RVV(vsse32_v_f32m4)
#define VFMACCVF_FLOAT RISCV_RVV(vfmacc_vf_f32m4)
#define VFNMSACVF_FLOAT RISCV_RVV(vfnmsac_vf_f32m4)
#else
#define VSETVL(n) RISCV_RVV(vsetvl_e64m4)(n)
#define FLOAT_V_T vfloat64m4_t
#define VLSEV_FLOAT RISCV_RVV(vlse64_v_f64m4)
#define VSSEV_FLOAT RISCV_RVV(vsse64_v_f64m4)
#define VFMACCVF_FLOAT RISCV_RVV(vfmacc_vf_f64m4)
#define VFNMSACVF_FLOAT RISCV_RVV(vfnmsac_vf_f64m4)
#endif

#if !defined(DOUBLE)
inline int  small_caxpy_kernel(BLASLONG n, BLASLONG dummy0, BLASLONG dummy1, FLOAT da_r, FLOAT da_i, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *dummy, BLASLONG dummy2)
#else
inline int  small_zaxpy_kernel(BLASLONG n, BLASLONG dummy0, BLASLONG dummy1, FLOAT da_r, FLOAT da_i, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *dummy, BLASLONG dummy2)
#endif
{
	BLASLONG i=0;
	BLASLONG ix,iy;
	BLASLONG inc_x2;
	BLASLONG inc_y2;

	if ( n <= 0     )  return(0);
	if ( da_r == 0.0 && da_i == 0.0 ) return(0);

	ix = 0;
	iy = 0;

	inc_x2 = 2 * inc_x;
	inc_y2 = 2 * inc_y;

	while(i < n)
	{
#if !defined(CONJ)
		y[iy]   += ( da_r * x[ix]   - da_i * x[ix+1] ) ;
		y[iy+1] += ( da_r * x[ix+1] + da_i * x[ix]   ) ;
#else
		y[iy]   += ( da_r * x[ix]   + da_i * x[ix+1] ) ;
		y[iy+1] -= ( da_r * x[ix+1] - da_i * x[ix]   ) ;
#endif
		ix += inc_x2 ;
		iy += inc_y2 ;
		i++ ;

	}
	return(0);

}

int CNAME(BLASLONG n, BLASLONG dummy0, BLASLONG dummy1, FLOAT da_r, FLOAT da_i, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *dummy, BLASLONG dummy2)
{
#if !defined(DOUBLE)
        if(n < 16) {
                return small_caxpy_kernel(n, dummy0, dummy1, da_r, da_i, x, inc_x, y, inc_y, dummy, dummy2);
        }
#else
        if(n < 8) {
                return small_zaxpy_kernel(n, dummy0, dummy1, da_r, da_i, x, inc_x, y, inc_y, dummy, dummy2);
        }
#endif
        BLASLONG i = 0, j = 0;
        BLASLONG ix = 0,iy = 0;
        if(n <= 0) return(0);
        if(da_r == 0.0 && da_i == 0.0) return(0);
        unsigned int gvl = 0;
        BLASLONG stride_x = inc_x * 2 * sizeof(FLOAT);
        BLASLONG stride_y = inc_y * 2 * sizeof(FLOAT);

        FLOAT_V_T vx0, vx1, vy0, vy1;
        gvl = VSETVL(n);
        BLASLONG inc_xv = inc_x * 2 * gvl;
        BLASLONG inc_yv = inc_y * 2 * gvl;
        for(i=0,j=0; i < n/gvl; i++){
                vx0 = VLSEV_FLOAT(&x[ix], stride_x, gvl);
                vx1 = VLSEV_FLOAT(&x[ix+1], stride_x, gvl);
                vy0 = VLSEV_FLOAT(&y[iy], stride_y, gvl);
                vy1 = VLSEV_FLOAT(&y[iy+1], stride_y, gvl);
#if !defined(CONJ)
                vy0 = VFMACCVF_FLOAT(vy0, da_r, vx0, gvl);
                vy0 = VFNMSACVF_FLOAT(vy0, da_i, vx1, gvl);
                vy1 = VFMACCVF_FLOAT(vy1, da_r, vx1, gvl);
                vy1 = VFMACCVF_FLOAT(vy1, da_i, vx0, gvl);
#else
                vy0 = VFMACCVF_FLOAT(vy0, da_r, vx0, gvl);
                vy0 = VFMACCVF_FLOAT(vy0, da_i, vx1, gvl);
                vy1 = VFNMSACVF_FLOAT(vy1, da_r, vx1, gvl);
                vy1 = VFMACCVF_FLOAT(vy1, da_i, vx0, gvl);
#endif
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
#if !defined(CONJ)
                vy0 = VFMACCVF_FLOAT(vy0, da_r, vx0, gvl);
                vy0 = VFNMSACVF_FLOAT(vy0, da_i, vx1, gvl);
                vy1 = VFMACCVF_FLOAT(vy1, da_r, vx1, gvl);
                vy1 = VFMACCVF_FLOAT(vy1, da_i, vx0, gvl);
#else
                vy0 = VFMACCVF_FLOAT(vy0, da_r, vx0, gvl);
                vy0 = VFMACCVF_FLOAT(vy0, da_i, vx1, gvl);
                vy1 = VFNMSACVF_FLOAT(vy1, da_r, vx1, gvl);
                vy1 = VFMACCVF_FLOAT(vy1, da_i, vx0, gvl);
#endif
                VSSEV_FLOAT(&y[iy], stride_y, vy0, gvl);
                VSSEV_FLOAT(&y[iy+1], stride_y, vy1, gvl);
        }
	return(0);
}


