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
#define FLOAT_V_T       JOIN(vfloat,            ELEN,   LMUL,   _t,     _)
#define VLEV_FLOAT      JOIN(RISCV_RVV(vle),       ELEN,   _v_f,   ELEN,   LMUL)
#define VLSEV_FLOAT     JOIN(RISCV_RVV(vlse),      ELEN,   _v_f,   ELEN,   LMUL)
#define VSEV_FLOAT      JOIN(RISCV_RVV(vse),       ELEN,   _v_f,   ELEN,   LMUL)
#define VSSEV_FLOAT     JOIN(RISCV_RVV(vsse),      ELEN,   _v_f,   ELEN,   LMUL)
#define VFMACCVF_FLOAT  JOIN(RISCV_RVV(vfmacc),    _vf_f,  ELEN,   LMUL,   _)
#define VFMVVF_FLOAT    JOIN(RISCV_RVV(vfmv),      _v_f_f, ELEN,   LMUL,   _)
#define VFMULVF_FLOAT   JOIN(RISCV_RVV(vfmul),     _vf_f,  ELEN,   LMUL,   _)

int CNAME(BLASLONG n, FLOAT alpha, FLOAT *x, BLASLONG inc_x, FLOAT beta, FLOAT *y, BLASLONG inc_y)
{
	if (n <= 0)  return(0);

	BLASLONG i=0, j=0;
	unsigned int gvl = 0;
	FLOAT_V_T vx0, vx1;
        FLOAT_V_T vy0, vy1;

	BLASLONG stride_x, stride_y, ix = 0, iy = 0;

	if (inc_x == 0 || inc_y == 0) { /* use trivial non-vectorized loop if either increment is zero */

	if ( beta == 0.0 )
	{

		if ( alpha == 0.0 )
		{
			while(i < n)
			{
				y[iy] = 0.0 ;
				iy += inc_y ;
				i++ ;
			}
		}
		else
		{
			while(i < n)
			{
				y[iy] = alpha * x[ix] ;
				ix += inc_x ;
				iy += inc_y ;
				i++ ;
			}


		}

	}
	else
	{

		if ( alpha == 0.0 )
		{
			while(i < n)
			{
				y[iy] =  beta * y[iy] ;
				iy += inc_y ;
				i++ ;
			}
		}
		else
		{
			while(i < n)
			{
				y[iy] = alpha * x[ix] + beta * y[iy] ;
				ix += inc_x ;
				iy += inc_y ;
				i++ ;
			}
		}

	}

	return(0);

	} else { /* vectorized approach for non-zero increments */

        if(beta == 0.0){
                if(alpha == 0.0){//alpha == 0 && beta == 0
                        if(inc_y == 1){
                                gvl = VSETVL(n);
                                if(gvl <= n/2){
                                        vy0 = VFMVVF_FLOAT(0.0, gvl);
                                        for(i=0,j=0;i<n/(gvl*2);i++){
                                                VSEV_FLOAT(&y[j], vy0, gvl);
                                                VSEV_FLOAT(&y[j+gvl], vy0, gvl);
                                                j += gvl * 2;
                                        }
                                }
                                for(;j<n;){
                                        gvl = VSETVL(n-j);
                                        vy0 = VFMVVF_FLOAT(0.0, gvl);
                                        VSEV_FLOAT(&y[j], vy0, gvl);
                                        j += gvl;
                                }
                        }else{
                                gvl = VSETVL(n);
                                stride_y = inc_y * sizeof(FLOAT);
                                if(gvl <= n/2){
                                        vy0 = VFMVVF_FLOAT(0.0, gvl);
                                        BLASLONG inc_yv = inc_y * gvl;
                                        for(i=0,j=0;i<n/(gvl*2);i++){
                                                VSSEV_FLOAT(&y[iy], stride_y, vy0, gvl);
                                                VSSEV_FLOAT(&y[iy+inc_yv], stride_y, vy0, gvl);
                                                j += gvl * 2;
                                                iy += inc_yv * 2;
                                        }
                                }
                                for(;j<n;){
                                        gvl = VSETVL(n-j);
                                        vy0 = VFMVVF_FLOAT(0.0, gvl);
                                        VSSEV_FLOAT(&y[j*inc_y], stride_y, vy0, gvl);
                                        j += gvl;
                                }
                        }

                }else{//alpha != 0 && beta == 0, y = ax
			if(inc_x == 1 && inc_y == 1){
                                gvl = VSETVL(n);
                                if(gvl <= n/2){
                                        for(i=0,j=0;i<n/(2*gvl);i++){
                                                vx0 = VLEV_FLOAT(&x[j], gvl);
                                                vy0 = VFMULVF_FLOAT(vx0, alpha, gvl);
                                                VSEV_FLOAT(&y[j], vy0, gvl);

                                                vx1 = VLEV_FLOAT(&x[j+gvl], gvl);
                                                vy1 = VFMULVF_FLOAT(vx1, alpha, gvl);
                                                VSEV_FLOAT(&y[j+gvl], vy1, gvl);
                                                j += gvl * 2;
                                        }
                                }
                                for(;j<n;){
                                        gvl = VSETVL(n-j);
                                        vx0 = VLEV_FLOAT(&x[j], gvl);
                                        vy0 = VFMULVF_FLOAT(vx0, alpha, gvl);
                                        VSEV_FLOAT(&y[j], vy0, gvl);
                                        j += gvl;
                                }
			}else if(inc_y == 1){
                                gvl = VSETVL(n);
                                stride_x = inc_x * sizeof(FLOAT);
                                if(gvl <= n/2){
                                        BLASLONG inc_xv = inc_x * gvl;
                                        for(i=0,j=0;i<n/(2*gvl);i++){
                                                vx0 = VLSEV_FLOAT(&x[ix], stride_x, gvl);
                                                vy0 = VFMULVF_FLOAT(vx0, alpha, gvl);
                                                VSEV_FLOAT(&y[j], vy0, gvl);

                                                vx1 = VLSEV_FLOAT(&x[ix+inc_xv], stride_x, gvl);
                                                vy1 = VFMULVF_FLOAT(vx1, alpha, gvl);
                                                VSEV_FLOAT(&y[j+gvl], vy1, gvl);
                                                j += gvl * 2;
                                                ix += inc_xv * 2;
                                        }
                                }
                                for(;j<n;){
                                        gvl = VSETVL(n-j);
                                        vx0 = VLSEV_FLOAT(&x[j*inc_x], stride_x, gvl);
                                        vy0 = VFMULVF_FLOAT(vx0, alpha, gvl);
                                        VSEV_FLOAT(&y[j], vy0, gvl);
                                        j += gvl;
                                }
                        }else if(inc_x == 1){
                                gvl = VSETVL(n);
                                stride_y = inc_y * sizeof(FLOAT);
                                if(gvl <= n/2){
                                        BLASLONG inc_yv = inc_y * gvl;
                                        for(i=0,j=0;i<n/(2*gvl);i++){
                                                vx0 = VLEV_FLOAT(&x[j], gvl);
                                                vy0 = VFMULVF_FLOAT(vx0, alpha, gvl);
                                                VSSEV_FLOAT(&y[iy], stride_y, vy0, gvl);

                                                vx1 = VLEV_FLOAT(&x[j+gvl], gvl);
                                                vy1 = VFMULVF_FLOAT(vx1, alpha, gvl);
                                                VSSEV_FLOAT(&y[iy+inc_yv], stride_y, vy1, gvl);
                                                j += gvl * 2;
                                                iy += inc_yv * 2;
                                        }
                                }
                                for(;j<n;){
                                        gvl = VSETVL(n-j);
                                        vx0 = VLEV_FLOAT(&x[j], gvl);
                                        vy0 = VFMULVF_FLOAT(vx0, alpha, gvl);
                                        VSSEV_FLOAT(&y[j*inc_y], stride_y, vy0, gvl);
                                        j += gvl;
                                }
                        }else{//inc_x !=1 && inc_y != 1
                                gvl = VSETVL(n);
                                stride_x = inc_x * sizeof(FLOAT);
                                stride_y = inc_y * sizeof(FLOAT);
                                if(gvl <= n/2){
                                        BLASLONG inc_xv = inc_x * gvl;
                                        BLASLONG inc_yv = inc_y * gvl;
                                        for(i=0,j=0;i<n/(2*gvl);i++){
                                                vx0 = VLSEV_FLOAT(&x[ix], stride_x, gvl);
                                                vy0 = VFMULVF_FLOAT(vx0, alpha, gvl);
                                                VSSEV_FLOAT(&y[iy], stride_y, vy0, gvl);

                                                vx1 = VLSEV_FLOAT(&x[ix+inc_xv], stride_x, gvl);
                                                vy1 = VFMULVF_FLOAT(vx1, alpha, gvl);
                                                VSSEV_FLOAT(&y[iy+inc_yv], stride_y, vy1, gvl);
                                                j += gvl * 2;
                                                ix += inc_xv * 2;
                                                iy += inc_yv * 2;
                                        }
                                }
                                for(;j<n;){
                                        gvl = VSETVL(n-j);
                                        vx0 = VLSEV_FLOAT(&x[j*inc_x], stride_x, gvl);
                                        vy0 = VFMULVF_FLOAT(vx0, alpha, gvl);
                                        VSSEV_FLOAT(&y[j*inc_y], stride_y, vy0, gvl);
                                        j += gvl;
                                }
                        }
		}
        }else{//beta != 0
		if(alpha == 0.0){//alpha == 0 && beta != 0; y = by
			if(inc_y == 1){
                                gvl = VSETVL(n);
                                if(gvl <= n/2){
                                        for(i=0,j=0;i<n/(2*gvl);i++){
                                                vy0 = VLEV_FLOAT(&y[j], gvl);
                                                vy0 = VFMULVF_FLOAT(vy0, beta, gvl);
                                                VSEV_FLOAT(&y[j], vy0, gvl);

                                                vy1 = VLEV_FLOAT(&y[j+gvl], gvl);
                                                vy1 = VFMULVF_FLOAT(vy1, beta, gvl);
                                                VSEV_FLOAT(&y[j+gvl], vy1, gvl);
                                                j += gvl * 2;
                                        }
                                }
                                for(;j<n;){
                                        gvl = VSETVL(n-j);
                                        vy0 = VLEV_FLOAT(&y[j], gvl);
                                        vy0 = VFMULVF_FLOAT(vy0, beta, gvl);
                                        VSEV_FLOAT(&y[j], vy0, gvl);
                                        j += gvl;
                                }
			}else{
                                gvl = VSETVL(n);
                                stride_y = inc_y * sizeof(FLOAT);
                                if(gvl <= n/2){
                                        BLASLONG inc_yv = inc_y * gvl;
                                        for(i=0,j=0;i<n/(2*gvl);i++){
                                                vy0 = VLSEV_FLOAT(&y[iy], stride_y, gvl);
                                                vy0 = VFMULVF_FLOAT(vy0, beta, gvl);
                                                VSSEV_FLOAT(&y[iy], stride_y, vy0, gvl);

                                                vy1 = VLSEV_FLOAT(&y[iy+inc_yv], stride_y, gvl);
                                                vy1 = VFMULVF_FLOAT(vy1, beta, gvl);
                                                VSSEV_FLOAT(&y[iy+inc_yv], stride_y, vy1, gvl);
                                                j += gvl * 2;
                                                iy += inc_yv * 2;
                                        }
                                }
                                for(;j<n;){
                                        gvl = VSETVL(n-j);
                                        vy0 = VLSEV_FLOAT(&y[j*inc_y], stride_y, gvl);
                                        vy0 = VFMULVF_FLOAT(vy0, beta, gvl);
                                        VSSEV_FLOAT(&y[j*inc_y], stride_y, vy0, gvl);
                                        j += gvl;
                                }
			}

		}else{//alpha != 0 && beta != 0; y = ax + by
			if(inc_x == 1 && inc_y == 1){
                                gvl = VSETVL(n);
                                if(gvl <= n/2){
                                        for(i=0,j=0;i<n/(2*gvl);i++){
                                                vx0 = VLEV_FLOAT(&x[j], gvl);
                                                vx0 = VFMULVF_FLOAT(vx0, alpha, gvl);
                                                vy0 = VLEV_FLOAT(&y[j], gvl);
                                                vy0 = VFMACCVF_FLOAT(vx0, beta, vy0, gvl);
                                                VSEV_FLOAT(&y[j], vy0, gvl);

                                                vx1 = VLEV_FLOAT(&x[j+gvl], gvl);
                                                vx1 = VFMULVF_FLOAT(vx1, alpha, gvl);
                                                vy1 = VLEV_FLOAT(&y[j+gvl], gvl);
                                                vy1 = VFMACCVF_FLOAT(vx1, beta, vy1,gvl);
                                                VSEV_FLOAT(&y[j+gvl], vy1, gvl);
                                                j += gvl * 2;
                                        }
                                }
                                for(;j<n;){
                                        gvl = VSETVL(n-j);
                                        vx0 = VLEV_FLOAT(&x[j], gvl);
                                        vx0 = VFMULVF_FLOAT(vx0, alpha, gvl);
                                        vy0 = VLEV_FLOAT(&y[j], gvl);
                                        vy0 = VFMACCVF_FLOAT(vx0, beta, vy0, gvl);
                                        VSEV_FLOAT(&y[j], vy0, gvl);
                                        j += gvl;
                                }
			}else if(inc_y == 1){
                                gvl = VSETVL(n);
                                stride_x = inc_x * sizeof(FLOAT);
                                if(gvl <= n/2){
                                        BLASLONG inc_xv = inc_x * gvl;
                                        for(i=0,j=0;i<n/(2*gvl);i++){
                                                vx0 = VLSEV_FLOAT(&x[ix], stride_x, gvl);
                                                vx0 = VFMULVF_FLOAT(vx0, alpha, gvl);
                                                vy0 = VLEV_FLOAT(&y[j], gvl);
                                                vy0 = VFMACCVF_FLOAT(vx0, beta, vy0, gvl);
                                                VSEV_FLOAT(&y[j], vy0, gvl);

                                                vx1 = VLSEV_FLOAT(&x[ix+inc_xv], stride_x, gvl);
                                                vx1 = VFMULVF_FLOAT(vx1, alpha, gvl);
                                                vy1 = VLEV_FLOAT(&y[j+gvl], gvl);
                                                vy1 = VFMACCVF_FLOAT(vx1, beta, vy1, gvl);
                                                VSEV_FLOAT(&y[j+gvl], vy1, gvl);
                                                j += gvl * 2;
                                                ix += inc_xv * 2;
                                        }
                                }
                                for(;j<n;){
                                        gvl = VSETVL(n-j);
                                        vx0 = VLSEV_FLOAT(&x[j*inc_x], stride_x, gvl);
                                        vx0 = VFMULVF_FLOAT(vx0, alpha, gvl);
                                        vy0 = VLEV_FLOAT(&y[j], gvl);
                                        vy0 = VFMACCVF_FLOAT(vx0, beta, vy0, gvl);
                                        VSEV_FLOAT(&y[j], vy0, gvl);
                                        j += gvl;
                                }
                        }else if(inc_x == 1){
                                gvl = VSETVL(n);
                                stride_y = inc_y * sizeof(FLOAT);
                                if(gvl <= n/2){
                                        BLASLONG inc_yv = inc_y * gvl;
                                        for(i=0,j=0;i<n/(2*gvl);i++){
                                                vx0 = VLEV_FLOAT(&x[j], gvl);
                                                vx0 = VFMULVF_FLOAT(vx0, alpha, gvl);
                                                vy0 = VLSEV_FLOAT(&y[iy], stride_y, gvl);
                                                vy0 = VFMACCVF_FLOAT(vx0, beta, vy0, gvl);
                                                VSSEV_FLOAT(&y[iy], stride_y, vy0, gvl);

                                                vx1 = VLEV_FLOAT(&x[j+gvl], gvl);
                                                vx1 = VFMULVF_FLOAT(vx1, alpha, gvl);
                                                vy1 = VLSEV_FLOAT(&y[iy+inc_yv], stride_y, gvl);
                                                vy1 = VFMACCVF_FLOAT(vx1, beta, vy1, gvl);
                                                VSSEV_FLOAT(&y[iy+inc_yv], stride_y, vy1, gvl);
                                                j += gvl * 2;
                                                iy += inc_yv * 2;
                                        }
                                }
                                for(;j<n;){
                                        gvl = VSETVL(n-j);
                                        vx0 = VLEV_FLOAT(&x[j], gvl);
                                        vx0 = VFMULVF_FLOAT(vx0, alpha, gvl);
                                        vy0 = VLSEV_FLOAT(&y[j*inc_y], stride_y, gvl);
                                        vy0 = VFMACCVF_FLOAT(vx0, beta, vy0, gvl);
                                        VSSEV_FLOAT(&y[j*inc_y], stride_y, vy0, gvl);
                                        j += gvl;
                                }
                        }else{//inc_x != 1 && inc_y != 1
                                gvl = VSETVL(n);
                                stride_x = inc_x * sizeof(FLOAT);
                                stride_y = inc_y * sizeof(FLOAT);
                                if(gvl <= n/2){
                                        BLASLONG inc_xv = inc_x * gvl;
                                        BLASLONG inc_yv = inc_y * gvl;
                                        for(i=0,j=0;i<n/(2*gvl);i++){
                                                vx0 = VLSEV_FLOAT(&x[ix], stride_x, gvl);
                                                vx0 = VFMULVF_FLOAT(vx0, alpha, gvl);
                                                vy0 = VLSEV_FLOAT(&y[iy], stride_y, gvl);
                                                vy0 = VFMACCVF_FLOAT(vx0, beta, vy0, gvl);
                                                VSSEV_FLOAT(&y[iy], stride_y, vy0, gvl);

                                                vx1 = VLSEV_FLOAT(&x[ix+inc_xv], stride_x, gvl);
                                                vx1 = VFMULVF_FLOAT(vx1, alpha, gvl);
                                                vy1 = VLSEV_FLOAT(&y[iy+inc_yv], stride_y, gvl);
                                                vy1 = VFMACCVF_FLOAT(vx1, beta, vy1, gvl);
                                                VSSEV_FLOAT(&y[iy+inc_yv], stride_y, vy1, gvl);
                                                j += gvl * 2;
                                                ix += inc_xv * 2;
                                                iy += inc_yv * 2;
                                        }
                                }
                                for(;j<n;){
                                        gvl = VSETVL(n-j);
                                        vx0 = VLSEV_FLOAT(&x[j*inc_x], stride_x, gvl);
                                        vx0 = VFMULVF_FLOAT(vx0, alpha, gvl);
                                        vy0 = VLSEV_FLOAT(&y[j*inc_y], stride_y, gvl);
                                        vy0 = VFMACCVF_FLOAT(vx0, beta, vy0, gvl);
                                        VSSEV_FLOAT(&y[j*inc_y], stride_y, vy0, gvl);
                                        j += gvl;
                                }
                        }
                }
        }
	return(0);
	}
}

