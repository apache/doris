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
#               define MLEN 32
#       else
#               define ELEN 32
#               define MLEN 16
#       endif
#else
#       define LMUL m8
#       if defined(DOUBLE)
#               define ELEN 64
#               define MLEN 8
#       else
#               define ELEN 32
#               define MLEN 4
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
#define VFMVVF_FLOAT    JOIN(RISCV_RVV(vfmv),      _v_f_f, ELEN,   LMUL,   _)
#define VFMULVF_FLOAT   JOIN(RISCV_RVV(vfmul),     _vf_f,  ELEN,   LMUL,   _)

int CNAME(BLASLONG n, BLASLONG dummy0, BLASLONG dummy1, FLOAT da, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *dummy, BLASLONG dummy2)
{
	BLASLONG i=0,j=0;

	if ( (n <= 0) || (inc_x <= 0))
		return(0);

        FLOAT_V_T v0, v1;
        unsigned int gvl = 0;
        if(inc_x == 1){
                if(dummy2 == 0 && da == 0.0){
                        memset(&x[0], 0, n * sizeof(FLOAT));
                }else{
                        gvl = VSETVL(n);
                        if(gvl <= n / 2){
                                for(i = 0, j = 0; i < n/(2*gvl); i++, j+=2*gvl){
                                        v0 = VLEV_FLOAT(&x[j], gvl);
                                        v0 = VFMULVF_FLOAT(v0, da,gvl);
                                        VSEV_FLOAT(&x[j], v0, gvl);

                                        v1 = VLEV_FLOAT(&x[j+gvl], gvl);
                                        v1 = VFMULVF_FLOAT(v1, da, gvl);
                                        VSEV_FLOAT(&x[j+gvl], v1, gvl);
                                }
                        }
                        //tail
                        for(; j <n; ){
                                gvl = VSETVL(n-j);
                                v0 = VLEV_FLOAT(&x[j], gvl);
                                v0 = VFMULVF_FLOAT(v0, da, gvl);
                                VSEV_FLOAT(&x[j], v0, gvl);
                                j += gvl;
                        }
                }
        }else{
                if(dummy2 == 0 && da == 0.0){
                        BLASLONG stride_x = inc_x * sizeof(FLOAT);
                        BLASLONG ix = 0;
                        gvl = VSETVL(n);
                        v0 = VFMVVF_FLOAT(0, gvl);

                        for(i = 0; i < n/(gvl*2); ++i ){
                                VSSEV_FLOAT(&x[ix], stride_x, v0, gvl);
                                ix += inc_x * gvl;
                                VSSEV_FLOAT(&x[ix], stride_x, v0, gvl);
                                ix += inc_x * gvl;
                        }

                        i *= gvl*2;
                        while( i < n ){
                                gvl = VSETVL(n-i);
                                v0 = VFMVVF_FLOAT(0, gvl);
                                VSSEV_FLOAT(&x[ix], stride_x, v0, gvl);
                                i += gvl;
                                ix += inc_x * gvl;
                        }
                }else{
                        gvl = VSETVL(n);
                        BLASLONG stride_x = inc_x * sizeof(FLOAT);
                        BLASLONG ix = 0;
                        if(gvl < n / 2){
                                BLASLONG inc_xv = gvl * inc_x;
                                for(i = 0, j = 0; i < n/(2*gvl); i++, j+=2*gvl){
                                        v0 = VLSEV_FLOAT(&x[ix], stride_x, gvl);
                                        v0 = VFMULVF_FLOAT(v0, da,gvl);
                                        VSSEV_FLOAT(&x[ix], stride_x, v0, gvl);

                                        v1 = VLSEV_FLOAT(&x[ix+inc_xv], stride_x, gvl);
                                        v1 = VFMULVF_FLOAT(v1, da, gvl);
                                        VSSEV_FLOAT(&x[ix+inc_xv], stride_x, v1, gvl);
                                        ix += inc_xv * 2;
                                }
                        }
                        //tail
                        for(; j <n; ){
                                gvl = VSETVL(n-j);
                                v0 = VLSEV_FLOAT(&x[ix], stride_x, gvl);
                                v0 = VFMULVF_FLOAT(v0, da, gvl);
                                VSSEV_FLOAT(&x[ix], stride_x, v0, gvl);
                                j += gvl;
                                ix += inc_x * gvl;
                        }
                }
        }
	return 0;
}


