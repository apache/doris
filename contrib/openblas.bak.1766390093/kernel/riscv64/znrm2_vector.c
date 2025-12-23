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
#       define LMUL m1
#       if defined(DOUBLE)
#               define ELEN 64
#               define MLEN 64
#       else
#               define ELEN 32
#               define MLEN 32
#       endif
#else
#       define LMUL m4
#       if defined(DOUBLE)
#               define ELEN 64
#               define MLEN 16
#       else
#               define ELEN 32
#               define MLEN 8
#       endif
#endif

#define _
#define JOIN2_X(x, y) x ## y
#define JOIN2(x, y) JOIN2_X(x, y)
#define JOIN(v, w, x, y, z) JOIN2( JOIN2( JOIN2( JOIN2( v, w ), x), y), z)

#define VSETVL          JOIN(RISCV_RVV(vsetvl),    _e,     ELEN,   LMUL,   _)
#define FLOAT_V_T       JOIN(vfloat,            ELEN,   LMUL,   _t,     _)
#define FLOAT_V_T_M1    JOIN(vfloat,            ELEN,   m1,     _t,     _)
#define VLEV_FLOAT      JOIN(RISCV_RVV(vle),       ELEN,   _v_f,   ELEN,   LMUL)
#define VLSEV_FLOAT     JOIN(RISCV_RVV(vlse),      ELEN,   _v_f,   ELEN,   LMUL)
#define VFMVVF_FLOAT    JOIN(RISCV_RVV(vfmv),      _v_f_f, ELEN,   LMUL,   _)
#define VFMVVF_FLOAT_M1 JOIN(RISCV_RVV(vfmv),      _v_f_f, ELEN,   m1,     _)
#define MASK_T          JOIN(vbool,             MLEN,   _t,     _,      _)
#define VFABS           JOIN(RISCV_RVV(vfabs),     _v_f,   ELEN,   LMUL,   _)
#define VMFNE           JOIN(RISCV_RVV(vmfne_vf_f),ELEN,   LMUL,   _b,     MLEN)
#define VMFGT           JOIN(RISCV_RVV(vmfgt_vv_f),ELEN,   LMUL,   _b,     MLEN)
#define VMFEQ           JOIN(RISCV_RVV(vmfeq_vv_f),ELEN,   LMUL,   _b,     MLEN)
#define VCPOP           JOIN(RISCV_RVV(vcpop),     _m_b,   MLEN,   _,      _)
#ifdef RISCV_0p10_INTRINSICS
#define VFREDMAX(va, vb, gvl) JOIN(RISCV_RVV(vfredmax_vs_f),ELEN,LMUL,   JOIN2(_f, ELEN), m1)(v_res, va, vb, gvl)
#define VFREDUSUM(va, vb, gvl) JOIN(RISCV_RVV(vfredusum_vs_f),ELEN,LMUL,  JOIN2(_f, ELEN), m1)(v_res, va, vb, gvl)
#define VFDIV_M         JOIN(RISCV_RVV(vfdiv),     _vv_f,  ELEN,   LMUL,   _m)
#define VFMACC_M        JOIN(RISCV_RVV(vfmacc),    _vv_f,  ELEN,   LMUL,   _m)
#else
#define VFREDMAX        JOIN(RISCV_RVV(vfredmax_vs_f),ELEN,LMUL,   JOIN2(_f, ELEN), m1)
#define VFREDUSUM       JOIN(RISCV_RVV(vfredusum_vs_f),ELEN,LMUL,  JOIN2(_f, ELEN), m1)
#define VFDIV_M         JOIN(RISCV_RVV(vfdiv),     _vv_f,  ELEN,   LMUL,   _mu)
#define VFMACC_M        JOIN(RISCV_RVV(vfmacc),    _vv_f,  ELEN,   LMUL,   _mu)
#endif
#define VFIRST          JOIN(RISCV_RVV(vfirst),    _m_b,   MLEN,   _,      _)
#define VRGATHER        JOIN(RISCV_RVV(vrgather),  _vx_f,  ELEN,   LMUL,   _)
#define VFDIV           JOIN(RISCV_RVV(vfdiv),     _vf_f,  ELEN,   LMUL,   _)
#define VFMUL           JOIN(RISCV_RVV(vfmul),     _vv_f,  ELEN,   LMUL,   _)
#define VFMACC          JOIN(RISCV_RVV(vfmacc),    _vv_f,  ELEN,   LMUL,   _)
#define VMSOF           JOIN(RISCV_RVV(vmsof),     _m_b,   MLEN,   _,      _)
#define VMANDN          JOIN(RISCV_RVV(vmandn),    _mm_b,  MLEN,   _,      _)
#if defined(DOUBLE)
#define ABS fabs
#else
#define ABS fabsf
#endif

#define EXTRACT_FLOAT0_V(v) JOIN(RISCV_RVV(vfmv_f_s_f), ELEN, LMUL, _f, ELEN)(v)


FLOAT CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x)
{
        BLASLONG i=0;

	if (n <= 0 || inc_x == 0) return(0.0);

        FLOAT_V_T v_ssq, v_scale, v0, v1, v_zero;
        unsigned int gvl = 0;
        FLOAT_V_T_M1 v_res, v_z0;

        v_res = VFMVVF_FLOAT_M1(0, 1);
        v_z0 = VFMVVF_FLOAT_M1(0, 1);

        gvl = VSETVL(n);
        v_ssq = VFMVVF_FLOAT(0, gvl);
        v_scale = VFMVVF_FLOAT(0, gvl);
        v_zero = VFMVVF_FLOAT(0, gvl);

        unsigned int stride_x = inc_x * sizeof(FLOAT) * 2;
        int idx = 0;

        for(i=0; i<n/gvl; i++){
                v0 = VLSEV_FLOAT( &x[idx], stride_x, gvl );
                v1 = VLSEV_FLOAT( &x[idx+1], stride_x, gvl );
                v0 = VFABS( v0, gvl );
                v1 = VFABS( v1, gvl );

                MASK_T scale_mask0 = VMFGT( v0, v_scale, gvl );
                MASK_T scale_mask1 = VMFGT( v1, v_scale, gvl );
                if( VCPOP( scale_mask0, gvl ) + VCPOP( scale_mask1, gvl ) > 0 ){ // scale change?
                        // find largest element in v0 and v1
                        v_res = VFREDMAX( v0, v_z0, gvl );
                        v_res = VFREDMAX( v1, v_res, gvl );
                        FLOAT const largest_elt = EXTRACT_FLOAT( v_res );

                        v_scale = VFDIV( v_scale, largest_elt, gvl );   // scale/largest_elt
                        v_scale = VFMUL( v_scale, v_scale, gvl );       // (scale/largest_elt)*(scale/largest_elt)
                        v_ssq = VFMUL( v_scale, v_ssq, gvl );           // ssq*(scale/largest_elt)*(scale/largest_elt)

                        v_scale = VFMVVF_FLOAT( largest_elt, gvl );     // splated largest_elt becomes new scale
                }

                MASK_T nonzero_mask0 = VMFNE( v0, 0, gvl );
                MASK_T nonzero_mask1 = VMFNE( v1, 0, gvl );
                v0 = VFDIV_M( nonzero_mask0, v_zero, v0, v_scale, gvl );
                v1 = VFDIV_M( nonzero_mask1, v_zero, v1, v_scale, gvl );
                v_ssq = VFMACC_M( nonzero_mask0, v_ssq, v0, v0, gvl );
                v_ssq = VFMACC_M( nonzero_mask1, v_ssq, v1, v1, gvl );

                idx += inc_x * gvl * 2;
        }

        v_res = VFREDUSUM(v_ssq, v_z0, gvl);
        FLOAT ssq = EXTRACT_FLOAT(v_res);
        FLOAT scale = EXTRACT_FLOAT0_V(v_scale);

        //finish any tail using scalar ops
        i*=gvl;
        if(i<n){
                i *= inc_x*2;
                n *= inc_x*2;
                FLOAT temp;
                do{
                        if ( x[i] != 0.0 ){
                                temp = ABS( x[i] );
                                if ( scale < temp ){
                                        ssq = 1 + ssq * ( scale / temp ) * ( scale / temp );
                                        scale = temp ;
                                }else{
                                        ssq += ( temp / scale ) * ( temp / scale );
                                }
                        }

                        if ( x[i+1] != 0.0 ){
                                temp = ABS( x[i+1] );
                                if ( scale < temp ){
                                        ssq = 1 + ssq * ( scale / temp ) * ( scale / temp );
                                        scale = temp ;
                                }else{
                                        ssq += ( temp / scale ) * ( temp / scale );
                                }
                        }

                        i += inc_x*2;
                }while(abs(i)<abs(n));
        }

        return(scale * sqrt(ssq));
}
