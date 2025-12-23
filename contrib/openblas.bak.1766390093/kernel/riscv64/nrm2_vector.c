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
#define VFMVSF_FLOAT    JOIN(RISCV_RVV(vfmv),      _s_f_f, ELEN,   LMUL,   _)
#define VFMVVF_FLOAT_M1 JOIN(RISCV_RVV(vfmv),      _v_f_f, ELEN,   m1,     _)
#define MASK_T          JOIN(vbool,             MLEN,   _t,     _,      _)
#define VFABS           JOIN(RISCV_RVV(vfabs),     _v_f,   ELEN,   LMUL,   _)
#define VMFNE           JOIN(RISCV_RVV(vmfne_vf_f),ELEN,   LMUL,   _b,     MLEN)
#define VMFGT           JOIN(RISCV_RVV(vmfgt_vv_f),ELEN,   LMUL,   _b,     MLEN)
#define VMFEQ           JOIN(RISCV_RVV(vmfeq_vf_f),ELEN,   LMUL,   _b,     MLEN)
#define VCPOP           JOIN(RISCV_RVV(vcpop),     _m_b,   MLEN,   _,      _)
#ifdef RISCV_0p10_INTRINSICS
#define VFDIV_M         JOIN(RISCV_RVV(vfdiv),     _vv_f,  ELEN,   LMUL,   _m)
#define VFMUL_M         JOIN(RISCV_RVV(vfmul),     _vv_f,  ELEN,   LMUL,   _m)
#define VFMACC_M        JOIN(RISCV_RVV(vfmacc),    _vv_f,  ELEN,   LMUL,   _m)
#define VMERGE(a, b, mask, gvl)       JOIN(RISCV_RVV(vmerge),    _vvm_f, ELEN,   LMUL,   _)(mask, a, b, gvl)
#else
#define VFDIV_M         JOIN(RISCV_RVV(vfdiv),     _vv_f,  ELEN,   LMUL,   _mu)
#define VFMUL_M         JOIN(RISCV_RVV(vfmul),     _vv_f,  ELEN,   LMUL,   _mu)
#define VFMACC_M        JOIN(RISCV_RVV(vfmacc),    _vv_f,  ELEN,   LMUL,   _mu)
#define VMERGE          JOIN(RISCV_RVV(vmerge),    _vvm_f, ELEN,   LMUL,   _)
#endif
#define VFIRST          JOIN(RISCV_RVV(vfirst),    _m_b,   MLEN,   _,      _)
#define VRGATHER        JOIN(RISCV_RVV(vrgather),  _vx_f,  ELEN,   LMUL,   _)
#define VFDIV           JOIN(RISCV_RVV(vfdiv),     _vv_f,  ELEN,   LMUL,   _)
#define VFMUL           JOIN(RISCV_RVV(vfmul),     _vv_f,  ELEN,   LMUL,   _)
#define VFMACC          JOIN(RISCV_RVV(vfmacc),    _vv_f,  ELEN,   LMUL,   _)
#define VMSBF           JOIN(RISCV_RVV(vmsbf),     _m_b,   MLEN,   _,      _)
#define VMSOF           JOIN(RISCV_RVV(vmsof),     _m_b,   MLEN,   _,      _)
#define VMAND           JOIN(RISCV_RVV(vmand),     _mm_b,  MLEN,   _,      _)
#define VMANDN          JOIN(RISCV_RVV(vmandn),    _mm_b,  MLEN,   _,      _)

#define VSEV_FLOAT      JOIN(RISCV_RVV(vse),       ELEN,   _v_f,   ELEN,   LMUL)

#if defined(DOUBLE)
#define ABS fabs
#else
#define ABS fabsf
#endif

#define EXTRACT_FLOAT0_V(v) JOIN(RISCV_RVV(vfmv_f_s_f), ELEN, LMUL, _f, ELEN)(v)

//#define DUMP( label, v0, gvl )
#define DUMP( label, v0, gvl ) do{ FLOAT x[16]; VSEV_FLOAT( x, v0, gvl ); printf ("%s(%d): %s [ ", __FILE__, __LINE__, label); for( int xxx = 0; xxx < gvl; ++xxx ) { printf("%f, ", x[xxx]); } printf(" ]\n"); } while(0)

FLOAT CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x)
{
	BLASLONG i=0;

	if (n <= 0 || inc_x == 0) return(0.0);
        if(n == 1) return (ABS(x[0]));

        unsigned int gvl = 0;

        MASK_T nonzero_mask;
        MASK_T scale_mask;

        gvl = VSETVL(n);
        FLOAT_V_T v0;
        FLOAT_V_T v_ssq = VFMVVF_FLOAT(0, gvl);
        FLOAT_V_T v_scale = VFMVVF_FLOAT(0, gvl);

        FLOAT scale = 0;
        FLOAT ssq = 0;
        unsigned int stride_x = inc_x * sizeof(FLOAT);
        int idx = 0;

        if( n >= gvl && inc_x > 0) // don't pay overheads if we're not doing useful work
        {
                for(i=0; i<n/gvl; i++){
                        v0 = VLSEV_FLOAT( &x[idx], stride_x, gvl );
                        nonzero_mask = VMFNE( v0, 0, gvl );
                        v0 = VFABS( v0, gvl );
                        scale_mask = VMFGT( v0, v_scale, gvl );

                        // assume scale changes are relatively infrequent

                        // unclear if the vcpop+branch is actually a win
                        // since the operations being skipped are predicated anyway
                        // need profiling to confirm
                        if( VCPOP(scale_mask, gvl) ) 
                        {
                                v_scale = VFDIV_M( scale_mask, v_scale, v_scale, v0, gvl );
                                v_scale = VFMUL_M( scale_mask, v_scale, v_scale, v_scale, gvl );
                                v_ssq = VFMUL_M( scale_mask, v_ssq, v_ssq, v_scale, gvl );
                                v_scale = VMERGE( v_scale, v0, scale_mask, gvl );
                        }
                        v0 = VFDIV_M( nonzero_mask, v0, v0, v_scale, gvl );
                        v_ssq = VFMACC_M( nonzero_mask, v_ssq, v0, v0, gvl );
                        idx += inc_x * gvl;
                }

                // we have gvl elements which we accumulated independently, with independent scales
                // we need to combine these
                // naive sort so we process small values first to avoid losing information
                // could use vector sort extensions where available, but we're dealing with gvl elts at most

                FLOAT * out_ssq = alloca(gvl*sizeof(FLOAT));
                FLOAT * out_scale = alloca(gvl*sizeof(FLOAT));
                VSEV_FLOAT( out_ssq, v_ssq, gvl );
                VSEV_FLOAT( out_scale, v_scale, gvl );
                for( int a = 0; a < (gvl-1); ++a )
                {
                        int smallest = a;
                        for( size_t b = a+1; b < gvl; ++b )
                                if( out_scale[b] < out_scale[smallest] )
                                        smallest = b;
                        if( smallest != a )
                        {
                                FLOAT tmp1 = out_ssq[a];
                                FLOAT tmp2 = out_scale[a];
                                out_ssq[a] = out_ssq[smallest];
                                out_scale[a] = out_scale[smallest];
                                out_ssq[smallest] = tmp1;
                                out_scale[smallest] = tmp2;
                        }
                }

                int a = 0;
                while( a<gvl && out_scale[a] == 0 )
                        ++a;

                if( a < gvl ) 
                {
                        ssq = out_ssq[a];
                        scale = out_scale[a];
                        ++a;
                        for( ; a < gvl; ++a ) 
                        {
                                ssq = ssq * ( scale / out_scale[a] ) * ( scale / out_scale[a] ) + out_ssq[a];
                                scale = out_scale[a];
                        }
                }
        }

        //finish any tail using scalar ops
        i*=gvl*inc_x;
        n*=inc_x;
        while(abs(i)< abs(n)){
                if ( x[i] != 0.0 ){
                        FLOAT absxi = ABS( x[i] );
                        if ( scale < absxi ){
                                ssq = 1 + ssq * ( scale / absxi ) * ( scale / absxi );
                                scale = absxi ;
                        }
                        else{
                                ssq += ( absxi/scale ) * ( absxi/scale );
                        }

                }

                i += inc_x;
        }

	return(scale * sqrt(ssq));
}


