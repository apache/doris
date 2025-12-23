/***************************************************************************
Copyright (c) 2022, The OpenBLAS Project
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
#define VSETVL(n)               __riscv_vsetvl_e32m2(n)
#define FLOAT_V_T               vfloat32m2_t
#define FLOAT_VX2_T             vfloat32m2x2_t
#define VGET_VX2                __riscv_vget_v_f32m2x2_f32m2
#define VSET_VX2                __riscv_vset_v_f32m2_f32m2x2
#define VLEV_FLOAT              __riscv_vle32_v_f32m2
#define VSEV_FLOAT              __riscv_vse32_v_f32m2
#define VLSEG2_FLOAT            __riscv_vlseg2e32_v_f32m2x2
#define VSSEG2_FLOAT            __riscv_vsseg2e32_v_f32m2x2
#define VFMVVF_FLOAT            __riscv_vfmv_v_f_f32m2
#define VFMACCVF_FLOAT          __riscv_vfmacc_vf_f32m2
#define VFNMSACVF_FLOAT         __riscv_vfnmsac_vf_f32m2
#define VFMULVF_FLOAT           __riscv_vfmul_vf_f32m2
#else
#define VSETVL(n)               __riscv_vsetvl_e64m2(n)
#define FLOAT_V_T               vfloat64m2_t
#define FLOAT_VX2_T             vfloat64m2x2_t
#define VGET_VX2                __riscv_vget_v_f64m2x2_f64m2
#define VSET_VX2                __riscv_vset_v_f64m2_f64m2x2
#define VLEV_FLOAT              __riscv_vle64_v_f64m2
#define VSEV_FLOAT              __riscv_vse64_v_f64m2
#define VLSEG2_FLOAT            __riscv_vlseg2e64_v_f64m2x2
#define VSSEG2_FLOAT            __riscv_vsseg2e64_v_f64m2x2
#define VFMVVF_FLOAT            __riscv_vfmv_v_f_f64m2
#define VFMACCVF_FLOAT          __riscv_vfmacc_vf_f64m2
#define VFNMSACVF_FLOAT         __riscv_vfnmsac_vf_f64m2
#define VFMULVF_FLOAT           __riscv_vfmul_vf_f64m2
#endif

#if defined(NN) || defined(NT) || defined(TN) || defined(TT)
#define OP_rr       VFMACCVF_FLOAT
#define OP_ir       VFMACCVF_FLOAT
#define OP_ii       VFNMSACVF_FLOAT
#define OP_ri       VFMACCVF_FLOAT
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
#define OP_rr       VFMACCVF_FLOAT
#define OP_ir       VFMACCVF_FLOAT
#define OP_ii       VFMACCVF_FLOAT
#define OP_ri       VFNMSACVF_FLOAT
#elif defined(RN) || defined(RT) || defined(CN) || defined(CT)
#define OP_rr       VFMACCVF_FLOAT
#define OP_ir       VFNMSACVF_FLOAT
#define OP_ii       VFMACCVF_FLOAT
#define OP_ri       VFMACCVF_FLOAT
#elif defined(RR) || defined(RC) || defined(CR) || defined(CC)
#define OP_rr       VFMACCVF_FLOAT
#define OP_ir       VFNMSACVF_FLOAT
#define OP_ii       VFNMSACVF_FLOAT
#define OP_ri       VFNMSACVF_FLOAT
#endif

int CNAME(BLASLONG bm,BLASLONG bn,BLASLONG bk,FLOAT alphar,FLOAT alphai,FLOAT* ba,FLOAT* bb,FLOAT* C, BLASLONG ldc, BLASLONG offset)
{
    BLASLONG i,j,k;
    FLOAT *C0, *C1, *C2, *C3, *ptrba,*ptrbb;
	BLASLONG off, temp;

#if defined(TRMMKERNEL) && !defined(LEFT)
	off = -offset;
#else
	off = 0;
#endif

    FLOAT_VX2_T vax2;
    FLOAT_V_T va0, va1, va2, va3, va4, va5, va6, va7;
    FLOAT_V_T vres0, vres1, vres2, vres3, vres4, vres5, vres6, vres7;

    //fprintf(stderr, "%s, bn=%ld bm=%ld bk=%ld alphar=%f alphai=%f ldc=%ld, offset=%ld\n", __FUNCTION__, bn, bm, bk, alphar, alphai, ldc, offset); // Debug

    size_t vl;
    for (j = bn/4; j > 0; j--)
    {
        C0 = C;
        C1 = C0 + 2 * ldc;
        C2 = C1 + 2 * ldc;
        C3 = C2 + 2 * ldc;
#if defined(TRMMKERNEL) && defined(LEFT)
        off = offset;
#endif
        ptrba = ba;
        for (i = bm; i > 0; i -= vl)
        {
            vl = VSETVL(i);
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            ptrbb = bb;
#else
            ptrba += off*vl*2;
            ptrbb = bb + off*4*2;
#endif

            vres0 = VFMVVF_FLOAT(0.0, vl);
            vres1 = VFMVVF_FLOAT(0.0, vl);
            vres2 = VFMVVF_FLOAT(0.0, vl);
            vres3 = VFMVVF_FLOAT(0.0, vl);
            vres4 = VFMVVF_FLOAT(0.0, vl);
            vres5 = VFMVVF_FLOAT(0.0, vl);
            vres6 = VFMVVF_FLOAT(0.0, vl);
            vres7 = VFMVVF_FLOAT(0.0, vl);

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
            temp = bk-off;
#elif defined(LEFT)
            temp = off+vl;  // number of values in A
#else
            temp = off+4;   // number of values in B
#endif

            for (k = temp/4; k > 0; k--)
            {
                vax2 = VLSEG2_FLOAT(ptrba, vl);
                va0 = VGET_VX2(vax2, 0);
                va1 = VGET_VX2(vax2, 1);
                ptrba += vl*2;

                vax2 = VLSEG2_FLOAT(ptrba, vl);
                va2 = VGET_VX2(vax2, 0);
                va3 = VGET_VX2(vax2, 1);
                ptrba += vl*2;

                vres0 =  OP_rr(vres0, *(ptrbb + 0), va0, vl);
                vres1 =  OP_ir(vres1, *(ptrbb + 0), va1, vl);
                vres0 =  OP_ii(vres0, *(ptrbb + 1), va1, vl);
                vres1 =  OP_ri(vres1, *(ptrbb + 1), va0, vl);

                vres2 =  OP_rr(vres2, *(ptrbb + 2), va0, vl);
                vres3 =  OP_ir(vres3, *(ptrbb + 2), va1, vl);
                vres2 =  OP_ii(vres2, *(ptrbb + 3), va1, vl);
                vres3 =  OP_ri(vres3, *(ptrbb + 3), va0, vl);

                vres4 =  OP_rr(vres4, *(ptrbb + 4), va0, vl);
                vres5 =  OP_ir(vres5, *(ptrbb + 4), va1, vl);
                vres4 =  OP_ii(vres4, *(ptrbb + 5), va1, vl);
                vres5 =  OP_ri(vres5, *(ptrbb + 5), va0, vl);

                vres6 =  OP_rr(vres6, *(ptrbb + 6), va0, vl);
                vres7 =  OP_ir(vres7, *(ptrbb + 6), va1, vl);
                vres6 =  OP_ii(vres6, *(ptrbb + 7), va1, vl);
                vres7 =  OP_ri(vres7, *(ptrbb + 7), va0, vl);

                ptrbb += 8;

                vax2 = VLSEG2_FLOAT(ptrba, vl);
                va4 = VGET_VX2(vax2, 0);
                va5 = VGET_VX2(vax2, 1);
                ptrba += vl*2;
                
                vres0 =  OP_rr(vres0, *(ptrbb + 0), va2, vl);
                vres1 =  OP_ir(vres1, *(ptrbb + 0), va3, vl);
                vres0 =  OP_ii(vres0, *(ptrbb + 1), va3, vl);
                vres1 =  OP_ri(vres1, *(ptrbb + 1), va2, vl);
                
                vres2 =  OP_rr(vres2, *(ptrbb + 2), va2, vl);
                vres3 =  OP_ir(vres3, *(ptrbb + 2), va3, vl);
                vres2 =  OP_ii(vres2, *(ptrbb + 3), va3, vl);
                vres3 =  OP_ri(vres3, *(ptrbb + 3), va2, vl);
                
                vres4 =  OP_rr(vres4, *(ptrbb + 4), va2, vl);
                vres5 =  OP_ir(vres5, *(ptrbb + 4), va3, vl);
                vres4 =  OP_ii(vres4, *(ptrbb + 5), va3, vl);
                vres5 =  OP_ri(vres5, *(ptrbb + 5), va2, vl);
                
                vres6 =  OP_rr(vres6, *(ptrbb + 6), va2, vl);
                vres7 =  OP_ir(vres7, *(ptrbb + 6), va3, vl);
                vres6 =  OP_ii(vres6, *(ptrbb + 7), va3, vl);
                vres7 =  OP_ri(vres7, *(ptrbb + 7), va2, vl);
                
                ptrbb += 8;

                vax2 = VLSEG2_FLOAT(ptrba, vl);
                va6 = VGET_VX2(vax2, 0);
                va7 = VGET_VX2(vax2, 1);
                ptrba += vl*2;
                
                vres0 =  OP_rr(vres0, *(ptrbb + 0), va4, vl);
                vres1 =  OP_ir(vres1, *(ptrbb + 0), va5, vl);
                vres0 =  OP_ii(vres0, *(ptrbb + 1), va5, vl);
                vres1 =  OP_ri(vres1, *(ptrbb + 1), va4, vl);
                
                vres2 =  OP_rr(vres2, *(ptrbb + 2), va4, vl);
                vres3 =  OP_ir(vres3, *(ptrbb + 2), va5, vl);
                vres2 =  OP_ii(vres2, *(ptrbb + 3), va5, vl);
                vres3 =  OP_ri(vres3, *(ptrbb + 3), va4, vl);
                
                vres4 =  OP_rr(vres4, *(ptrbb + 4), va4, vl);
                vres5 =  OP_ir(vres5, *(ptrbb + 4), va5, vl);
                vres4 =  OP_ii(vres4, *(ptrbb + 5), va5, vl);
                vres5 =  OP_ri(vres5, *(ptrbb + 5), va4, vl);
                
                vres6 =  OP_rr(vres6, *(ptrbb + 6), va4, vl);
                vres7 =  OP_ir(vres7, *(ptrbb + 6), va5, vl);
                vres6 =  OP_ii(vres6, *(ptrbb + 7), va5, vl);
                vres7 =  OP_ri(vres7, *(ptrbb + 7), va4, vl);
                
                ptrbb += 8;
                
                vres0 =  OP_rr(vres0, *(ptrbb + 0), va6, vl);
                vres1 =  OP_ir(vres1, *(ptrbb + 0), va7, vl);
                vres0 =  OP_ii(vres0, *(ptrbb + 1), va7, vl);
                vres1 =  OP_ri(vres1, *(ptrbb + 1), va6, vl);
                
                vres2 =  OP_rr(vres2, *(ptrbb + 2), va6, vl);
                vres3 =  OP_ir(vres3, *(ptrbb + 2), va7, vl);
                vres2 =  OP_ii(vres2, *(ptrbb + 3), va7, vl);
                vres3 =  OP_ri(vres3, *(ptrbb + 3), va6, vl);
                
                vres4 =  OP_rr(vres4, *(ptrbb + 4), va6, vl);
                vres5 =  OP_ir(vres5, *(ptrbb + 4), va7, vl);
                vres4 =  OP_ii(vres4, *(ptrbb + 5), va7, vl);
                vres5 =  OP_ri(vres5, *(ptrbb + 5), va6, vl);
                
                vres6 =  OP_rr(vres6, *(ptrbb + 6), va6, vl);
                vres7 =  OP_ir(vres7, *(ptrbb + 6), va7, vl);
                vres6 =  OP_ii(vres6, *(ptrbb + 7), va7, vl);
                vres7 =  OP_ri(vres7, *(ptrbb + 7), va6, vl);
                
                ptrbb += 8;
            }

            for (k = temp & 3; k > 0; k--)
            {
                vax2 = VLSEG2_FLOAT(ptrba, vl);
                va0 = VGET_VX2(vax2, 0);
                va1 = VGET_VX2(vax2, 1);
                ptrba += vl*2;

                vres0 =  OP_rr(vres0, *(ptrbb + 0), va0, vl);
                vres1 =  OP_ir(vres1, *(ptrbb + 0), va1, vl);
                vres0 =  OP_ii(vres0, *(ptrbb + 1), va1, vl);
                vres1 =  OP_ri(vres1, *(ptrbb + 1), va0, vl);

                vres2 =  OP_rr(vres2, *(ptrbb + 2), va0, vl);
                vres3 =  OP_ir(vres3, *(ptrbb + 2), va1, vl);
                vres2 =  OP_ii(vres2, *(ptrbb + 3), va1, vl);
                vres3 =  OP_ri(vres3, *(ptrbb + 3), va0, vl);

                vres4 =  OP_rr(vres4, *(ptrbb + 4), va0, vl);
                vres5 =  OP_ir(vres5, *(ptrbb + 4), va1, vl);
                vres4 =  OP_ii(vres4, *(ptrbb + 5), va1, vl);
                vres5 =  OP_ri(vres5, *(ptrbb + 5), va0, vl);

                vres6 =  OP_rr(vres6, *(ptrbb + 6), va0, vl);
                vres7 =  OP_ir(vres7, *(ptrbb + 6), va1, vl);
                vres6 =  OP_ii(vres6, *(ptrbb + 7), va1, vl);
                vres7 =  OP_ri(vres7, *(ptrbb + 7), va0, vl);

                ptrbb += 8;
            }
            va0 =  VFMULVF_FLOAT(vres0, alphar, vl);
            va1 =  VFMULVF_FLOAT(vres1, alphar, vl);
            va0 = VFNMSACVF_FLOAT(va0, alphai, vres1, vl);
            va1 =  VFMACCVF_FLOAT(va1, alphai, vres0, vl);
            
            vax2 = VSET_VX2(vax2, 0, va0);
            vax2 = VSET_VX2(vax2, 1, va1);
            VSSEG2_FLOAT(C0, vax2, vl);

            va2 =  VFMULVF_FLOAT(vres2, alphar, vl);
            va3 =  VFMULVF_FLOAT(vres3, alphar, vl);
            va2 = VFNMSACVF_FLOAT(va2, alphai, vres3, vl);
            va3 =  VFMACCVF_FLOAT(va3, alphai, vres2, vl);

            vax2 = VSET_VX2(vax2, 0, va2);
            vax2 = VSET_VX2(vax2, 1, va3);
            VSSEG2_FLOAT(C1, vax2, vl);

            va0 =  VFMULVF_FLOAT(vres4, alphar, vl);
            va1 =  VFMULVF_FLOAT(vres5, alphar, vl);
            va0 = VFNMSACVF_FLOAT(va0, alphai, vres5, vl);
            va1 =  VFMACCVF_FLOAT(va1, alphai, vres4, vl);

            vax2 = VSET_VX2(vax2, 0, va0);
            vax2 = VSET_VX2(vax2, 1, va1);
            VSSEG2_FLOAT(C2, vax2, vl);

            va2 =  VFMULVF_FLOAT(vres6, alphar, vl);
            va3 =  VFMULVF_FLOAT(vres7, alphar, vl);
            va2 = VFNMSACVF_FLOAT(va2, alphai, vres7, vl);
            va3 =  VFMACCVF_FLOAT(va3, alphai, vres6, vl);

            vax2 = VSET_VX2(vax2, 0, va2);
            vax2 = VSET_VX2(vax2, 1, va3);
            VSSEG2_FLOAT(C3, vax2, vl);

#if ( defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            temp = bk - off;
#ifdef LEFT
            temp -= vl; // number of values in A
#else
            temp -= 4; // number of values in B
#endif
            ptrba += temp*vl*2;
            ptrbb += temp*4*2;
#endif

#ifdef LEFT
            off += vl; // number of values in A
#endif

            C0 += vl * 2;
            C1 += vl * 2;
            C2 += vl * 2;
            C3 += vl * 2;
        }
#if defined(TRMMKERNEL) && !defined(LEFT)
        off += 4;
#endif

        bb += (bk << 3);
        C  += (ldc << 3);
    }

    if (bn & 2)
    {
        C0 = C;
        C1 = C0 + 2 * ldc;
#if defined(TRMMKERNEL) && defined(LEFT)
        off = offset;
#endif
        ptrba = ba;
        for (i = bm; i > 0; i -= vl)
        {
            vl = VSETVL(i);
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            ptrbb = bb;
#else
            ptrba += off*vl*2;
            ptrbb = bb + off*2*2;
#endif

            vres0 = VFMVVF_FLOAT(0.0, vl);
            vres1 = VFMVVF_FLOAT(0.0, vl);
            vres2 = VFMVVF_FLOAT(0.0, vl);
            vres3 = VFMVVF_FLOAT(0.0, vl);

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
            temp = bk-off;
#elif defined(LEFT)
            temp = off+vl;  // number of values in A
#else
            temp = off+2;   // number of values in B
#endif
            for (k = temp/4; k > 0; k--)
            {
                vax2 = VLSEG2_FLOAT(ptrba, vl);
                va0 = VGET_VX2(vax2, 0);
                va1 = VGET_VX2(vax2, 1);
                ptrba += vl*2;

                vax2 = VLSEG2_FLOAT(ptrba, vl);
                va2 = VGET_VX2(vax2, 0);
                va3 = VGET_VX2(vax2, 1);
                ptrba += vl*2;

                vres0 =  OP_rr(vres0, *(ptrbb + 0), va0, vl);
                vres1 =  OP_ir(vres1, *(ptrbb + 0), va1, vl);
                vres0 =  OP_ii(vres0, *(ptrbb + 1), va1, vl);
                vres1 =  OP_ri(vres1, *(ptrbb + 1), va0, vl);

                vres2 =  OP_rr(vres2, *(ptrbb + 2), va0, vl);
                vres3 =  OP_ir(vres3, *(ptrbb + 2), va1, vl);
                vres2 =  OP_ii(vres2, *(ptrbb + 3), va1, vl);
                vres3 =  OP_ri(vres3, *(ptrbb + 3), va0, vl);

                ptrbb += 4;

                vax2 = VLSEG2_FLOAT(ptrba, vl);
                va4 = VGET_VX2(vax2, 0);
                va5 = VGET_VX2(vax2, 1);
                ptrba += vl*2;
                
                vres0 =  OP_rr(vres0, *(ptrbb + 0), va2, vl);
                vres1 =  OP_ir(vres1, *(ptrbb + 0), va3, vl);
                vres0 =  OP_ii(vres0, *(ptrbb + 1), va3, vl);
                vres1 =  OP_ri(vres1, *(ptrbb + 1), va2, vl);
                
                vres2 =  OP_rr(vres2, *(ptrbb + 2), va2, vl);
                vres3 =  OP_ir(vres3, *(ptrbb + 2), va3, vl);
                vres2 =  OP_ii(vres2, *(ptrbb + 3), va3, vl);
                vres3 =  OP_ri(vres3, *(ptrbb + 3), va2, vl);

                ptrbb += 4;

                vax2 = VLSEG2_FLOAT(ptrba, vl);
                va6 = VGET_VX2(vax2, 0);
                va7 = VGET_VX2(vax2, 1);
                ptrba += vl*2;
                
                vres0 =  OP_rr(vres0, *(ptrbb + 0), va4, vl);
                vres1 =  OP_ir(vres1, *(ptrbb + 0), va5, vl);
                vres0 =  OP_ii(vres0, *(ptrbb + 1), va5, vl);
                vres1 =  OP_ri(vres1, *(ptrbb + 1), va4, vl);
                
                vres2 =  OP_rr(vres2, *(ptrbb + 2), va4, vl);
                vres3 =  OP_ir(vres3, *(ptrbb + 2), va5, vl);
                vres2 =  OP_ii(vres2, *(ptrbb + 3), va5, vl);
                vres3 =  OP_ri(vres3, *(ptrbb + 3), va4, vl);
                
                ptrbb += 4;
                
                vres0 =  OP_rr(vres0, *(ptrbb + 0), va6, vl);
                vres1 =  OP_ir(vres1, *(ptrbb + 0), va7, vl);
                vres0 =  OP_ii(vres0, *(ptrbb + 1), va7, vl);
                vres1 =  OP_ri(vres1, *(ptrbb + 1), va6, vl);
                
                vres2 =  OP_rr(vres2, *(ptrbb + 2), va6, vl);
                vres3 =  OP_ir(vres3, *(ptrbb + 2), va7, vl);
                vres2 =  OP_ii(vres2, *(ptrbb + 3), va7, vl);
                vres3 =  OP_ri(vres3, *(ptrbb + 3), va6, vl);
                
                ptrbb += 4;
            }

            for (k = temp & 3; k > 0; k--)
            {
                vax2 = VLSEG2_FLOAT(ptrba, vl);
                va0 = VGET_VX2(vax2, 0);
                va1 = VGET_VX2(vax2, 1);
                ptrba += vl*2;

                vres0 =  OP_rr(vres0, *(ptrbb + 0), va0, vl);
                vres1 =  OP_ir(vres1, *(ptrbb + 0), va1, vl);
                vres0 =  OP_ii(vres0, *(ptrbb + 1), va1, vl);
                vres1 =  OP_ri(vres1, *(ptrbb + 1), va0, vl);

                vres2 =  OP_rr(vres2, *(ptrbb + 2), va0, vl);
                vres3 =  OP_ir(vres3, *(ptrbb + 2), va1, vl);
                vres2 =  OP_ii(vres2, *(ptrbb + 3), va1, vl);
                vres3 =  OP_ri(vres3, *(ptrbb + 3), va0, vl);

                ptrbb += 4;
            }

            va0 =  VFMULVF_FLOAT(vres0, alphar, vl);
            va1 =  VFMULVF_FLOAT(vres1, alphar, vl);
            va0 = VFNMSACVF_FLOAT(va0, alphai, vres1, vl);
            va1 =  VFMACCVF_FLOAT(va1, alphai, vres0, vl);
            
            vax2 = VSET_VX2(vax2, 0, va0);
            vax2 = VSET_VX2(vax2, 1, va1);
            VSSEG2_FLOAT(C0, vax2, vl);

            va2 =  VFMULVF_FLOAT(vres2, alphar, vl);
            va3 =  VFMULVF_FLOAT(vres3, alphar, vl);
            va2 = VFNMSACVF_FLOAT(va2, alphai, vres3, vl);
            va3 =  VFMACCVF_FLOAT(va3, alphai, vres2, vl);

            vax2 = VSET_VX2(vax2, 0, va2);
            vax2 = VSET_VX2(vax2, 1, va3);
            VSSEG2_FLOAT(C1, vax2, vl);

#if ( defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            temp = bk - off;
#ifdef LEFT
            temp -= vl; // number of values in A
#else
            temp -= 2; // number of values in B
#endif
            ptrba += temp*vl*2;
            ptrbb += temp*2*2;
#endif

#ifdef LEFT
            off += vl; // number of values in A
#endif
            C0 += vl * 2;
            C1 += vl * 2;
        }

#if defined(TRMMKERNEL) && !defined(LEFT)
        off += 2;
#endif
        bb += (bk << 2);
        C  += (ldc << 2);
    }

    if (bn & 1)
    {
        C0 = C;
#if defined(TRMMKERNEL) &&  defined(LEFT)
        off = offset;
#endif
        ptrba = ba;
        for (i = bm; i > 0; i -= vl)
        {
            vl = VSETVL(i);
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            ptrbb = bb;
#else
            ptrba += off*vl*2;
            ptrbb = bb + off*2;
#endif

            vres0 = VFMVVF_FLOAT(0.0, vl);
            vres1 = VFMVVF_FLOAT(0.0, vl);

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
            temp = bk-off;
#elif defined(LEFT)
            temp = off+vl;  // number of values in A
#else
            temp = off+1;   // number of values in B
#endif
            for (k = temp/4; k > 0; k--)
            {
                vax2 = VLSEG2_FLOAT(ptrba, vl);
                va0 = VGET_VX2(vax2, 0);
                va1 = VGET_VX2(vax2, 1);
                ptrba += vl*2;

                vax2 = VLSEG2_FLOAT(ptrba, vl);
                va2 = VGET_VX2(vax2, 0);
                va3 = VGET_VX2(vax2, 1);
                ptrba += vl*2;

                vres0 =  OP_rr(vres0, *(ptrbb + 0), va0, vl);
                vres1 =  OP_ir(vres1, *(ptrbb + 0), va1, vl);
                vres0 =  OP_ii(vres0, *(ptrbb + 1), va1, vl);
                vres1 =  OP_ri(vres1, *(ptrbb + 1), va0, vl);

                ptrbb += 2;

                vax2 = VLSEG2_FLOAT(ptrba, vl);
                va4 = VGET_VX2(vax2, 0);
                va5 = VGET_VX2(vax2, 1);
                ptrba += vl*2;
                
                vres0 =  OP_rr(vres0, *(ptrbb + 0), va2, vl);
                vres1 =  OP_ir(vres1, *(ptrbb + 0), va3, vl);
                vres0 =  OP_ii(vres0, *(ptrbb + 1), va3, vl);
                vres1 =  OP_ri(vres1, *(ptrbb + 1), va2, vl);

                ptrbb += 2;

                vax2 = VLSEG2_FLOAT(ptrba, vl);
                va6 = VGET_VX2(vax2, 0);
                va7 = VGET_VX2(vax2, 1);
                ptrba += vl*2;
                
                vres0 =  OP_rr(vres0, *(ptrbb + 0), va4, vl);
                vres1 =  OP_ir(vres1, *(ptrbb + 0), va5, vl);
                vres0 =  OP_ii(vres0, *(ptrbb + 1), va5, vl);
                vres1 =  OP_ri(vres1, *(ptrbb + 1), va4, vl);
                
                ptrbb += 2;
                
                vres0 =  OP_rr(vres0, *(ptrbb + 0), va6, vl);
                vres1 =  OP_ir(vres1, *(ptrbb + 0), va7, vl);
                vres0 =  OP_ii(vres0, *(ptrbb + 1), va7, vl);
                vres1 =  OP_ri(vres1, *(ptrbb + 1), va6, vl);
                
                ptrbb += 2;
            }

            for (k = temp & 3; k > 0; k--)
            {
                vax2 = VLSEG2_FLOAT(ptrba, vl);
                va0 = VGET_VX2(vax2, 0);
                va1 = VGET_VX2(vax2, 1);
                ptrba += vl*2;

                vres0 =  OP_rr(vres0, *(ptrbb + 0), va0, vl);
                vres1 =  OP_ir(vres1, *(ptrbb + 0), va1, vl);
                vres0 =  OP_ii(vres0, *(ptrbb + 1), va1, vl);
                vres1 =  OP_ri(vres1, *(ptrbb + 1), va0, vl);

                ptrbb += 2;
            }

            va0 =  VFMULVF_FLOAT(vres0, alphar, vl);
            va1 =  VFMULVF_FLOAT(vres1, alphar, vl);
            va0 = VFNMSACVF_FLOAT(va0, alphai, vres1, vl);
            va1 =  VFMACCVF_FLOAT(va1, alphai, vres0, vl);
            
            vax2 = VSET_VX2(vax2, 0, va0);
            vax2 = VSET_VX2(vax2, 1, va1);
            VSSEG2_FLOAT(C0, vax2, vl);

#if ( defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            temp = bk - off;
#ifdef LEFT
            temp -= vl; // number of values in A
#else
            temp -= 1; // number of values in B
#endif
            ptrba += temp*vl*2;
            ptrbb += temp*2;
#endif

#ifdef LEFT
            off += vl; // number of values in A
#endif
            C0 += vl * 2;
        }

#if defined(TRMMKERNEL) && !defined(LEFT)
        off += 1;
#endif
        bb += bk << 1;
        C  += ldc << 1;
   }
   return 0;
}
