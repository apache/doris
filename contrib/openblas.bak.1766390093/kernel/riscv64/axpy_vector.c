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

#ifdef RISCV64_ZVL256B
#       define LMUL m2
#       if defined(DOUBLE)
#               define ELEN 64
#       else
#               define ELEN 32
#       endif
#else
#       define LMUL m4
#       if defined(DOUBLE)
#               define ELEN 64
#       else
#               define ELEN 32
#       endif
#endif

#define _
#define JOIN2_X(x, y) x ## y
#define JOIN2(x, y) JOIN2_X(x, y)
#define JOIN(v, w, x, y, z) JOIN2( JOIN2( JOIN2( JOIN2( v, w ), x), y), z)

#define VSETVL          JOIN(RISCV_RVV(vsetvl),    _e,     ELEN,   LMUL,   _)
#define FLOAT_V_T       JOIN(vfloat,    	ELEN,   LMUL,   _t,     _)
#define FLOAT_V_M1_T    JOIN(vfloat,    	ELEN,   m1,   _t,     _)
#define VLEV_FLOAT      JOIN(RISCV_RVV(vle),       ELEN,   _v_f,   ELEN,   LMUL)
#define VLSEV_FLOAT     JOIN(RISCV_RVV(vlse),      ELEN,   _v_f,   ELEN,   LMUL)
#define VSEV_FLOAT      JOIN(RISCV_RVV(vse),       ELEN,   _v_f,   ELEN,   LMUL)
#define VSSEV_FLOAT     JOIN(RISCV_RVV(vsse),      ELEN,   _v_f,   ELEN,   LMUL)
#define VFMACCVF_FLOAT  JOIN(RISCV_RVV(vfmacc),    _vf_f, 	ELEN,   LMUL,   _)
#define VFMVVF_FLOAT    JOIN(RISCV_RVV(vfmv),      _v_f_f, ELEN,   LMUL,   _)
#define VFMVVF_FLOAT_M1 JOIN(RISCV_RVV(vfmv),      _v_f_f, ELEN,   m1,     _)

#ifdef RISCV_0p10_INTRINSICS
#define VFREDSUMVS_FLOAT(va, vb, gvl) JOIN(RISCV_RVV(vfredusum_vs_f),  ELEN,   LMUL,   _f, JOIN2( ELEN,   m1))(v_res, va, vb, gvl)
#else
#define VFREDSUMVS_FLOAT JOIN(RISCV_RVV(vfredusum_vs_f),  ELEN,   LMUL,   _f, JOIN2( ELEN,   m1))
#endif

int CNAME(BLASLONG n, BLASLONG dummy0, BLASLONG dummy1, FLOAT da, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *dummy, BLASLONG dummy2)
{
	BLASLONG i=0, j=0, jx=0, jy=0;
	unsigned int gvl = 0;
	FLOAT_V_T vx0, vx1;
	FLOAT_V_T vy0, vy1;
	BLASLONG stride_x, stride_y;

	if (n <= 0)  return(0);
	if (da == 0.0) return(0);

	if (inc_x == 1 && inc_y == 1) {

		gvl = VSETVL(n);

		if (gvl <= n/2) {
			for (i = 0, j=0; i < n/(2*gvl); i++, j+=2*gvl) {
				vx0 = VLEV_FLOAT(&x[j], gvl);
				vy0 = VLEV_FLOAT(&y[j], gvl);
				vy0 = VFMACCVF_FLOAT(vy0, da, vx0, gvl);
				VSEV_FLOAT(&y[j], vy0, gvl);

				vx1 = VLEV_FLOAT(&x[j+gvl], gvl);
				vy1 = VLEV_FLOAT(&y[j+gvl], gvl);
				vy1 = VFMACCVF_FLOAT(vy1, da, vx1, gvl);
				VSEV_FLOAT(&y[j+gvl], vy1, gvl);
			}
		}
		//tail
		for (; j < n; ) {
			gvl = VSETVL(n - j);
			vx0 = VLEV_FLOAT(&x[j], gvl);
			vy0 = VLEV_FLOAT(&y[j], gvl);
			vy0 = VFMACCVF_FLOAT(vy0, da, vx0, gvl);
			VSEV_FLOAT(&y[j], vy0, gvl);

			j += gvl;
		}
	}else if (inc_y == 1) {
		stride_x = inc_x * sizeof(FLOAT);
                gvl = VSETVL(n);
                if(gvl <= n/2){
                        BLASLONG inc_xv = inc_x * gvl;
                        for(i=0,j=0; i<n/(2*gvl); i++){
			        vx0 = VLSEV_FLOAT(&x[jx], stride_x, gvl);
                                vy0 = VLEV_FLOAT(&y[j], gvl);
                                vy0 = VFMACCVF_FLOAT(vy0, da, vx0, gvl);
                                VSEV_FLOAT(&y[j], vy0, gvl);

			        vx1 = VLSEV_FLOAT(&x[jx+inc_xv], stride_x, gvl);
                                vy1 = VLEV_FLOAT(&y[j+gvl], gvl);
                                vy1 = VFMACCVF_FLOAT(vy1, da, vx1, gvl);
                                VSEV_FLOAT(&y[j+gvl], vy1, gvl);

                                j += gvl * 2;
                                jx += inc_xv * 2;
                        }
                }
		for (; j<n; ) {
			gvl = VSETVL(n - j);
			vx0 = VLSEV_FLOAT(&x[j*inc_x], stride_x, gvl);
			vy0 = VLEV_FLOAT(&y[j], gvl);
			vy0 = VFMACCVF_FLOAT(vy0, da, vx0, gvl);
			VSEV_FLOAT(&y[j], vy0, gvl);
			j += gvl;
		}
	} else if (1 == inc_x && 0 != inc_y) {
		stride_y = inc_y * sizeof(FLOAT);
                gvl = VSETVL(n);
                if(gvl <= n/2){
                        BLASLONG inc_yv = inc_y * gvl;
                        for(i=0,j=0; i<n/(2*gvl); i++){
			        vx0 = VLEV_FLOAT(&x[j], gvl);
                                vy0 = VLSEV_FLOAT(&y[jy], stride_y, gvl);
                                vy0 = VFMACCVF_FLOAT(vy0, da, vx0, gvl);
                                VSSEV_FLOAT(&y[jy], stride_y, vy0, gvl);

			        vx1 = VLEV_FLOAT(&x[j+gvl], gvl);
                                vy1 = VLSEV_FLOAT(&y[jy+inc_yv], stride_y, gvl);
                                vy1 = VFMACCVF_FLOAT(vy1, da, vx1, gvl);
                                VSSEV_FLOAT(&y[jy+inc_yv], stride_y, vy1, gvl);

                                j += gvl * 2;
                                jy += inc_yv * 2;
                        }
                }
		for (; j<n; ) {
			gvl = VSETVL(n - j);
			vx0 = VLEV_FLOAT(&x[j], gvl);
			vy0 = VLSEV_FLOAT(&y[j*inc_y], stride_y, gvl);
			vy0 = VFMACCVF_FLOAT(vy0, da, vx0, gvl);
			VSSEV_FLOAT(&y[j*inc_y], stride_y, vy0, gvl);
			j += gvl;
		}
	} else if( 0 == inc_y ) {
	        BLASLONG stride_x = inc_x * sizeof(FLOAT);
	        size_t in_vl = VSETVL(n);
	        vy0 = VFMVVF_FLOAT( y[0], in_vl );

	        for (size_t vl; n > 0; n -= vl, x += vl*inc_x) {
	            vl = VSETVL(n);
	            vx0 = VLSEV_FLOAT(x, stride_x, vl);
	            vy0 = VFMACCVF_FLOAT(vy0, da, vx0, vl);
	        }
	        FLOAT_V_M1_T v_res = VFMVVF_FLOAT_M1( 0.0f, 1 );
	        v_res = VFREDSUMVS_FLOAT( vy0, v_res, in_vl );
	        y[0] = EXTRACT_FLOAT(v_res);
	}else{
		stride_x = inc_x * sizeof(FLOAT);
		stride_y = inc_y * sizeof(FLOAT);
                gvl = VSETVL(n);
                if(gvl <= n/2){
                        BLASLONG inc_xv = inc_x * gvl;
                        BLASLONG inc_yv = inc_y * gvl;
                        for(i=0,j=0; i<n/(2*gvl); i++){
			        vx0 = VLSEV_FLOAT(&x[jx], stride_x, gvl);
                                vy0 = VLSEV_FLOAT(&y[jy], stride_y, gvl);
                                vy0 = VFMACCVF_FLOAT(vy0, da, vx0, gvl);
                                VSSEV_FLOAT(&y[jy], stride_y, vy0, gvl);

			        vx1 = VLSEV_FLOAT(&x[jx+inc_xv], stride_x, gvl);
                                vy1 = VLSEV_FLOAT(&y[jy+inc_yv], stride_y, gvl);
                                vy1 = VFMACCVF_FLOAT(vy1, da, vx1, gvl);
                                VSSEV_FLOAT(&y[jy+inc_yv], stride_y, vy1, gvl);

                                j += gvl * 2;
                                jx += inc_xv * 2;
                                jy += inc_yv * 2;
                        }
                }
		for (; j<n; ) {
			gvl = VSETVL(n - j);
			vx0 = VLSEV_FLOAT(&x[j*inc_x], stride_x, gvl);
			vy0 = VLSEV_FLOAT(&y[j*inc_y], stride_y, gvl);
			vy0 = VFMACCVF_FLOAT(vy0, da, vx0, gvl);
			VSSEV_FLOAT(&y[j*inc_y], stride_y, vy0, gvl);
			j += gvl;
		}
	}
	return(0);
}


