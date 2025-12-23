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
#else
#define VSETVL(n) RISCV_RVV(vsetvl_e64m4)(n)
#define FLOAT_V_T vfloat64m4_t
#define VLSEV_FLOAT RISCV_RVV(vlse64_v_f64m4)
#define VSSEV_FLOAT RISCV_RVV(vsse64_v_f64m4)
#endif


int CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y)
{
	BLASLONG i = 0, j = 0;
	BLASLONG ix = 0,iy = 0;
	if(n < 0) return(0);

        unsigned int gvl = 0;
        if(inc_x == 1 && inc_y == 1){
                memcpy(&y[0], &x[0], n * 2 * sizeof(FLOAT));
        }else{
                FLOAT_V_T vx0, vx1, vx2, vx3;
                gvl = VSETVL(n);
                BLASLONG stride_x = inc_x * 2 * sizeof(FLOAT);
                BLASLONG stride_y = inc_y * 2 * sizeof(FLOAT);
                if(gvl <= n/2){
                        BLASLONG inc_xv = inc_x * gvl * 2;
                        BLASLONG inc_yv = inc_y * gvl * 2;
                        for(i=0,j=0; i < n/(2*gvl); i++){
                                vx0 = VLSEV_FLOAT(&x[ix], stride_x, gvl);
                                vx1 = VLSEV_FLOAT(&x[ix+1], stride_x, gvl);
                                VSSEV_FLOAT(&y[iy], stride_y, vx0, gvl);
                                VSSEV_FLOAT(&y[iy+1], stride_y, vx1, gvl);

                                vx2 = VLSEV_FLOAT(&x[ix+inc_xv], stride_x, gvl);
                                vx3 = VLSEV_FLOAT(&x[ix+1+inc_xv], stride_x, gvl);
                                VSSEV_FLOAT(&y[iy+inc_yv], stride_y, vx2, gvl);
                                VSSEV_FLOAT(&y[iy+1+inc_yv], stride_y, vx3, gvl);

                                j += gvl * 2;
                                ix += inc_xv * 2;
                                iy += inc_yv * 2;
                        }
                }
                for(;j<n;){
                        gvl = VSETVL(n-j);
                        vx0 = VLSEV_FLOAT(&x[ix], stride_x, gvl);
                        vx1 = VLSEV_FLOAT(&x[ix+1], stride_x, gvl);
                        VSSEV_FLOAT(&y[iy], stride_y, vx0, gvl);
                        VSSEV_FLOAT(&y[iy+1], stride_y, vx1, gvl);

                        j += gvl;
                        ix += inc_x * 2 * gvl;
                        iy += inc_y * 2 * gvl;
                }
        }
	return(0);
}


