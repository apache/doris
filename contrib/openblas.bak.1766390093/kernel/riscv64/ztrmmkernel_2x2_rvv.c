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
#define VSETVL_MAX              __riscv_vsetvlmax_e32m2()
#define VSETVL_MAX_M1           __riscv_vsetvlmax_e32m1()
#define FLOAT_V_T               vfloat32m2_t
#define FLOAT_V_T_M1            vfloat32m1_t
#define VLEV_FLOAT              __riscv_vle32_v_f32m2
#define VLSEG4_FLOAT            __riscv_vlseg4e32_v_f32m2
#define VLSEG2_FLOAT            __riscv_vlseg2e32_v_f32m2
#define VFMVVF_FLOAT            __riscv_vfmv_v_f_f32m2
#define VFMACCVF_FLOAT          __riscv_vfmacc_vf_f32m2
#define VFMACCVV_FLOAT          __riscv_vfmacc_vv_f32m2
#define VFNMSACVV_FLOAT         __riscv_vfnmsac_vv_f32m2
#define VFREDSUMVS_FLOAT        __riscv_vfredusum_vs_f32m2_f32m1
#define VFMVVF_FLOAT_M1         __riscv_vfmv_v_f_f32m1
#define VFMVFS_FLOAT_M1         __riscv_vfmv_f_s_f32m1_f32
#else
#define VSETVL(n)               __riscv_vsetvl_e64m2(n)
#define VSETVL_MAX              __riscv_vsetvlmax_e64m2()
#define VSETVL_MAX_M1           __riscv_vsetvlmax_e64m1()
#define FLOAT_V_T               vfloat64m2_t
#define FLOAT_V_T_M1            vfloat64m1_t
#define VLEV_FLOAT              __riscv_vle64_v_f64m2
#define VLSEG4_FLOAT            __riscv_vlseg4e64_v_f64m2
#define VLSEG2_FLOAT            __riscv_vlseg2e64_v_f64m2
#define VFMVVF_FLOAT            __riscv_vfmv_v_f_f64m2
#define VFMACCVF_FLOAT          __riscv_vfmacc_vf_f64m2
#define VFMACCVV_FLOAT          __riscv_vfmacc_vv_f64m2
#define VFNMSACVV_FLOAT         __riscv_vfnmsac_vv_f64m2
#define VFREDSUMVS_FLOAT        __riscv_vfredusum_vs_f64m2_f64m1
#define VFMVVF_FLOAT_M1         __riscv_vfmv_v_f_f64m1
#define VFMVFS_FLOAT_M1         __riscv_vfmv_f_s_f64m1_f64
#endif

// Optimizes the implementation in ../generic/ztrmmkernel_2x2.c


/********************************
  ADD1 a*c
  ADD2 b*c
  ADD3 a*d
  ADD4 b*d
 *********************************/
int CNAME(BLASLONG bm,BLASLONG bn,BLASLONG bk,FLOAT alphar,FLOAT alphai,FLOAT* ba,FLOAT* bb,
        FLOAT* C,BLASLONG ldc, BLASLONG offset)
{
    BLASLONG i,j,k;
    FLOAT *C0,*C1,*ptrba,*ptrbb;
    FLOAT res0,res1;
    BLASLONG off, temp;
    
    FLOAT_V_T va0, va1, va2, va3, vb0, vb1, vb2, vb3;
    FLOAT_V_T vres0, vres1, vres2, vres3, vres4, vres5, vres6, vres7;
    FLOAT_V_T_M1 v_m1_res0, v_m1_res1;
    FLOAT_V_T_M1 v_z0 = VFMVVF_FLOAT_M1(0, VSETVL_MAX_M1);

    size_t vl;
    size_t vlmax = VSETVL_MAX;

#if defined(TRMMKERNEL) && !defined(LEFT)
    off = -offset;
#else
    off = 0;
#endif

    for (j = bn/2; j > 0; j--)
    {
#if defined(TRMMKERNEL) &&  defined(LEFT)
        off = offset;
#endif
        C0 = C;
        C1 = C0+2*ldc;
        ptrba = ba;

        for (i = bm/2; i > 0; i--)
        {
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            ptrbb = bb;
#else
            ptrba += off*2*2;
            ptrbb = bb+off*2*2;
#endif

            vres0 = VFMVVF_FLOAT(0.0, vlmax);
            vres1 = VFMVVF_FLOAT(0.0, vlmax);
            vres2 = VFMVVF_FLOAT(0.0, vlmax);
            vres3 = VFMVVF_FLOAT(0.0, vlmax);
            vres4 = VFMVVF_FLOAT(0.0, vlmax);
            vres5 = VFMVVF_FLOAT(0.0, vlmax);
            vres6 = VFMVVF_FLOAT(0.0, vlmax);
            vres7 = VFMVVF_FLOAT(0.0, vlmax);

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
            temp = bk - off;
#elif defined(LEFT)
            temp = off + 2;
#else
            temp = off + 2;
#endif

            for (k = temp; k > 0; k -= vl)
            {
                vl = VSETVL(k);
                VLSEG4_FLOAT(&va0, &va1, &va2, &va3, ptrba, vl);
                VLSEG4_FLOAT(&vb0, &vb1, &vb2, &vb3, ptrbb, vl);

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
                vres0 = VFMACCVV_FLOAT(vres0, va0, vb0, vl);
                vres1 = VFMACCVV_FLOAT(vres1, va1, vb0, vl);
                vres0 = VFNMSACVV_FLOAT(vres0, va1, vb1, vl);
                vres1 = VFMACCVV_FLOAT(vres1, va0, vb1, vl);

                vres2 = VFMACCVV_FLOAT(vres2, va2, vb0, vl);
                vres3 = VFMACCVV_FLOAT(vres3, va3, vb0, vl);
                vres2 = VFNMSACVV_FLOAT(vres2, va3, vb1, vl);
                vres3 = VFMACCVV_FLOAT(vres3, va2, vb1, vl);

                vres4 = VFMACCVV_FLOAT(vres4, va0, vb2, vl);
                vres5 = VFMACCVV_FLOAT(vres5, va1, vb2, vl);
                vres4 = VFNMSACVV_FLOAT(vres4, va1, vb3, vl);
                vres5 = VFMACCVV_FLOAT(vres5, va0, vb3, vl);

                vres6 = VFMACCVV_FLOAT(vres6, va2, vb2, vl);
                vres7 = VFMACCVV_FLOAT(vres7, va3, vb2, vl);
                vres6 = VFNMSACVV_FLOAT(vres6, va3, vb3, vl);
                vres7 = VFMACCVV_FLOAT(vres7, va2, vb3, vl);
#endif
#if   defined(NR) || defined(NC) || defined(TR) || defined(TC)
                vres0 = VFMACCVV_FLOAT(vres0, va0, vb0, vl);
                vres1 = VFMACCVV_FLOAT(vres1, va1, vb0, vl);
                vres0 = VFMACCVV_FLOAT(vres0, va1, vb1, vl);
                vres1 = VFNMSACVV_FLOAT(vres1, va0, vb1, vl);

                vres2 = VFMACCVV_FLOAT(vres2, va2, vb0, vl);
                vres3 = VFMACCVV_FLOAT(vres3, va3, vb0, vl);
                vres2 = VFMACCVV_FLOAT(vres2, va3, vb1, vl);
                vres3 = VFNMSACVV_FLOAT(vres3, va2, vb1, vl);

                vres4 = VFMACCVV_FLOAT(vres4, va0, vb2, vl);
                vres5 = VFMACCVV_FLOAT(vres5, va1, vb2, vl);
                vres4 = VFMACCVV_FLOAT(vres4, va1, vb3, vl);
                vres5 = VFNMSACVV_FLOAT(vres5, va0, vb3, vl);

                vres6 = VFMACCVV_FLOAT(vres6, va2, vb2, vl);
                vres7 = VFMACCVV_FLOAT(vres7, va3, vb2, vl);
                vres6 = VFMACCVV_FLOAT(vres6, va3, vb3, vl);
                vres7 = VFNMSACVV_FLOAT(vres7, va2, vb3, vl);
#endif
#if   defined(RN) || defined(RT) || defined(CN) || defined(CT)
                vres0 = VFMACCVV_FLOAT(vres0, va0, vb0, vl);
                vres1 = VFNMSACVV_FLOAT(vres1, va1, vb0, vl);
                vres0 = VFMACCVV_FLOAT(vres0, va1, vb1, vl);
                vres1 = VFMACCVV_FLOAT(vres1, va0, vb1, vl);

                vres2 = VFMACCVV_FLOAT(vres2, va2, vb0, vl);
                vres3 = VFNMSACVV_FLOAT(vres3, va3, vb0, vl);
                vres2 = VFMACCVV_FLOAT(vres2, va3, vb1, vl);
                vres3 = VFMACCVV_FLOAT(vres3, va2, vb1, vl);

                vres4 = VFMACCVV_FLOAT(vres4, va0, vb2, vl);
                vres5 = VFNMSACVV_FLOAT(vres5, va1, vb2, vl);
                vres4 = VFMACCVV_FLOAT(vres4, va1, vb3, vl);
                vres5 = VFMACCVV_FLOAT(vres5, va0, vb3, vl);

                vres6 = VFMACCVV_FLOAT(vres6, va2, vb2, vl);
                vres7 = VFNMSACVV_FLOAT(vres7, va3, vb2, vl);
                vres6 = VFMACCVV_FLOAT(vres6, va3, vb3, vl);
                vres7 = VFMACCVV_FLOAT(vres7, va2, vb3, vl);
#endif
#if   defined(RR) || defined(RC) || defined(CR) || defined(CC)
                vres0 = VFMACCVV_FLOAT(vres0, va0, vb0, vl);
                vres1 = VFNMSACVV_FLOAT(vres1, va1, vb0, vl);
                vres0 = VFMACCVV_FLOAT(vres0, va1, vb1, vl);
                vres1 = VFNMSACVV_FLOAT(vres1, va0, vb1, vl);

                vres2 = VFMACCVV_FLOAT(vres2, va2, vb0, vl);
                vres3 = VFNMSACVV_FLOAT(vres3, va3, vb0, vl);
                vres2 = VFMACCVV_FLOAT(vres2, va3, vb1, vl);
                vres3 = VFNMSACVV_FLOAT(vres3, va2, vb1, vl);

                vres4 = VFMACCVV_FLOAT(vres4, va0, vb2, vl);
                vres5 = VFNMSACVV_FLOAT(vres5, va1, vb2, vl);
                vres4 = VFMACCVV_FLOAT(vres4, va1, vb3, vl);
                vres5 = VFNMSACVV_FLOAT(vres5, va0, vb3, vl);

                vres6 = VFMACCVV_FLOAT(vres6, va2, vb2, vl);
                vres7 = VFNMSACVV_FLOAT(vres7, va3, vb2, vl);
                vres6 = VFMACCVV_FLOAT(vres6, va3, vb3, vl);
                vres7 = VFNMSACVV_FLOAT(vres7, va2, vb3, vl);

#endif
                ptrba += vl * 4;
                ptrbb += vl * 4;
            }
            
            v_m1_res0 = VFREDSUMVS_FLOAT(v_m1_res0, vres0, v_z0, vlmax);
            v_m1_res1 = VFREDSUMVS_FLOAT(v_m1_res1, vres1, v_z0, vlmax);
            res0 = VFMVFS_FLOAT_M1(v_m1_res0);
            res1 = VFMVFS_FLOAT_M1(v_m1_res1);
            C0[0] = res0 * alphar - res1 * alphai;
            C0[1] = res1 * alphar + res0 * alphai;

            v_m1_res0 = VFREDSUMVS_FLOAT(v_m1_res0, vres2, v_z0, vlmax);
            v_m1_res1 = VFREDSUMVS_FLOAT(v_m1_res1, vres3, v_z0, vlmax);
            res0 = VFMVFS_FLOAT_M1(v_m1_res0);
            res1 = VFMVFS_FLOAT_M1(v_m1_res1);
            C0[2] = res0 * alphar - res1 * alphai;
            C0[3] = res1 * alphar + res0 * alphai;

            v_m1_res0 = VFREDSUMVS_FLOAT(v_m1_res0, vres4, v_z0, vlmax);
            v_m1_res1 = VFREDSUMVS_FLOAT(v_m1_res1, vres5, v_z0, vlmax);
            res0 = VFMVFS_FLOAT_M1(v_m1_res0);
            res1 = VFMVFS_FLOAT_M1(v_m1_res1);
            C1[0] = res0 * alphar - res1 * alphai;
            C1[1] = res1 * alphar + res0 * alphai;

            v_m1_res0 = VFREDSUMVS_FLOAT(v_m1_res0, vres6, v_z0, vlmax);
            v_m1_res1 = VFREDSUMVS_FLOAT(v_m1_res1, vres7, v_z0, vlmax);
            res0 = VFMVFS_FLOAT_M1(v_m1_res0);
            res1 = VFMVFS_FLOAT_M1(v_m1_res1);
            C1[2] = res0 * alphar - res1 * alphai;
            C1[3] = res1 * alphar + res0 * alphai;
#if ( defined(LEFT) &&  defined(TRANSA)) || \
            (!defined(LEFT) && !defined(TRANSA))
            temp = bk - off;
#ifdef LEFT
            temp -= 2;
#else
            temp -= 2;
#endif

            ptrba += temp*2*2;
            ptrbb += temp*2*2;

#endif

#ifdef LEFT
            off += 2;
#endif

            C0 = C0+4;
            C1 = C1+4;
        }

        if (bm & 1)
        {
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            ptrbb = bb;
#else
            ptrba += off*2;
            ptrbb = bb + off*2*2;
#endif
            vres0 = VFMVVF_FLOAT(0.0, vlmax);
            vres1 = VFMVVF_FLOAT(0.0, vlmax);
            vres2 = VFMVVF_FLOAT(0.0, vlmax);
            vres3 = VFMVVF_FLOAT(0.0, vlmax);

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
            temp = bk - off;
#elif defined(LEFT)
            temp = off+1;
#else
            temp = off+2;
#endif
            for (k = temp; k > 0; k -= vl)
            {
                vl = VSETVL(k);
                VLSEG2_FLOAT(&va0, &va1, ptrba, vl);
                VLSEG4_FLOAT(&vb0, &vb1, &vb2, &vb3, ptrbb, vl);

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
                vres0 = VFMACCVV_FLOAT(vres0, va0, vb0, vl);
                vres1 = VFMACCVV_FLOAT(vres1, va1, vb0, vl);
                vres0 = VFNMSACVV_FLOAT(vres0, va1, vb1, vl);
                vres1 = VFMACCVV_FLOAT(vres1, va0, vb1, vl);

                vres2 = VFMACCVV_FLOAT(vres2, va0, vb2, vl);
                vres3 = VFMACCVV_FLOAT(vres3, va1, vb2, vl);
                vres2 = VFNMSACVV_FLOAT(vres2, va1, vb3, vl);
                vres3 = VFMACCVV_FLOAT(vres3, va0, vb3, vl);
#endif
#if   defined(NR) || defined(NC) || defined(TR) || defined(TC)

                vres0 = VFMACCVV_FLOAT(vres0, va0, vb0, vl);
                vres1 = VFMACCVV_FLOAT(vres1, va1, vb0, vl);
                vres0 = VFMACCVV_FLOAT(vres0, va1, vb1, vl);
                vres1 = VFNMSACVV_FLOAT(vres1, va0, vb1, vl);

                vres2 = VFMACCVV_FLOAT(vres2, va0, vb2, vl);
                vres3 = VFMACCVV_FLOAT(vres3, va1, vb2, vl);
                vres2 = VFMACCVV_FLOAT(vres2, va1, vb3, vl);
                vres3 = VFNMSACVV_FLOAT(vres3, va0, vb3, vl);

#endif
#if   defined(RN) || defined(RT) || defined(CN) || defined(CT)

                vres0 = VFMACCVV_FLOAT(vres0, va0, vb0, vl);
                vres1 = VFNMSACVV_FLOAT(vres1, va1, vb0, vl);
                vres0 = VFMACCVV_FLOAT(vres0, va1, vb1, vl);
                vres1 = VFMACCVV_FLOAT(vres1, va0, vb1, vl);

                vres2 = VFMACCVV_FLOAT(vres2, va0, vb2, vl);
                vres3 = VFNMSACVV_FLOAT(vres3, va1, vb2, vl);
                vres2 = VFMACCVV_FLOAT(vres2, va1, vb3, vl);
                vres3 = VFMACCVV_FLOAT(vres3, va0, vb3, vl);

#endif
#if   defined(RR) || defined(RC) || defined(CR) || defined(CC)

                vres0 = VFMACCVV_FLOAT(vres0, va0, vb0, vl);
                vres1 = VFNMSACVV_FLOAT(vres1, va1, vb0, vl);
                vres0 = VFNMSACVV_FLOAT(vres0, va1, vb1, vl);
                vres1 = VFNMSACVV_FLOAT(vres1, va0, vb1, vl);

                vres2 = VFMACCVV_FLOAT(vres2, va0, vb2, vl);
                vres3 = VFNMSACVV_FLOAT(vres3, va1, vb2, vl);
                vres2 = VFNMSACVV_FLOAT(vres2, va1, vb3, vl);
                vres3 = VFNMSACVV_FLOAT(vres3, va0, vb3, vl);

#endif
                ptrba += vl * 2;
                ptrbb += vl * 4;
            }
            v_m1_res0 = VFREDSUMVS_FLOAT(v_m1_res0, vres0, v_z0, vlmax);
            v_m1_res1 = VFREDSUMVS_FLOAT(v_m1_res1, vres1, v_z0, vlmax);
            res0 = VFMVFS_FLOAT_M1(v_m1_res0);
            res1 = VFMVFS_FLOAT_M1(v_m1_res1);
            C0[0] = res0 * alphar - res1 * alphai;
            C0[1] = res1 * alphar + res0 * alphai;

            v_m1_res0 = VFREDSUMVS_FLOAT(v_m1_res0, vres2, v_z0, vlmax);
            v_m1_res1 = VFREDSUMVS_FLOAT(v_m1_res1, vres3, v_z0, vlmax);
            res0 = VFMVFS_FLOAT_M1(v_m1_res0);
            res1 = VFMVFS_FLOAT_M1(v_m1_res1);
            C1[0] = res0 * alphar - res1 * alphai;
            C1[1] = res1 * alphar + res0 * alphai;

#if ( defined(LEFT) &&  defined(TRANSA)) || \
            (!defined(LEFT) && !defined(TRANSA))
            temp = bk - off;
#ifdef LEFT
            temp -= 1;
#else
            temp -= 2;
#endif
            ptrba += temp*2;
            ptrbb += temp*2*2;
#endif
#ifdef LEFT
            off += 1;
#endif
            C0 = C0+2;
            C1 = C1+2;
        }
#if defined(TRMMKERNEL) && !defined(LEFT)
        off += 2;
#endif
        k = (bk<<2);
        bb = bb+k;
        i = (ldc<<2);
        C = C+i;
    }

    if (bn & 1)
    {
        C0 = C;
#if defined(TRMMKERNEL) &&  defined(LEFT)
        off = offset;
#endif
        ptrba = ba;

        for (i = bm/2; i > 0; i--)
        {
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            ptrbb = bb;
#else
            ptrba += off*2*2;
            ptrbb = bb+off*2;
#endif
            vres0 = VFMVVF_FLOAT(0.0, vlmax);
            vres1 = VFMVVF_FLOAT(0.0, vlmax);
            vres2 = VFMVVF_FLOAT(0.0, vlmax);
            vres3 = VFMVVF_FLOAT(0.0, vlmax);
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
            temp = bk - off;
#elif defined(LEFT)
            temp = off + 2;
#else
            temp = off + 1;
#endif

            for (k = temp; k > 0; k -= vl)
            {
                vl = VSETVL(k);
                VLSEG4_FLOAT(&va0, &va1, &va2, &va3, ptrba, vl);
                VLSEG2_FLOAT(&vb0, &vb1, ptrbb, vl);

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
                vres0 = VFMACCVV_FLOAT(vres0, va0, vb0, vl);
                vres1 = VFMACCVV_FLOAT(vres1, va1, vb0, vl);
                vres0 = VFNMSACVV_FLOAT(vres0, va1, vb1, vl);
                vres1 = VFMACCVV_FLOAT(vres1, va0, vb1, vl);

                vres2 = VFMACCVV_FLOAT(vres2, va2, vb0, vl);
                vres3 = VFMACCVV_FLOAT(vres3, va3, vb0, vl);
                vres2 = VFNMSACVV_FLOAT(vres2, va3, vb1, vl);
                vres3 = VFMACCVV_FLOAT(vres3, va2, vb1, vl);
#endif
#if   defined(NR) || defined(NC) || defined(TR) || defined(TC)
                vres0 = VFMACCVV_FLOAT(vres0, va0, vb0, vl);
                vres1 = VFMACCVV_FLOAT(vres1, va1, vb0, vl);
                vres0 = VFMACCVV_FLOAT(vres0, va1, vb1, vl);
                vres1 = VFNMSACVV_FLOAT(vres1, va0, vb1, vl);

                vres2 = VFMACCVV_FLOAT(vres2, va2, vb0, vl);
                vres3 = VFMACCVV_FLOAT(vres3, va3, vb0, vl);
                vres2 = VFMACCVV_FLOAT(vres2, va3, vb1, vl);
                vres3 = VFNMSACVV_FLOAT(vres3, va2, vb1, vl);
#endif
#if   defined(RN) || defined(RT) || defined(CN) || defined(CT)
                vres0 = VFMACCVV_FLOAT(vres0, va0, vb0, vl);
                vres1 = VFNMSACVV_FLOAT(vres1, va1, vb0, vl);
                vres0 = VFMACCVV_FLOAT(vres0, va1, vb1, vl);
                vres1 = VFMACCVV_FLOAT(vres1, va0, vb1, vl);

                vres2 = VFMACCVV_FLOAT(vres2, va2, vb0, vl);
                vres3 = VFNMSACVV_FLOAT(vres3, va3, vb0, vl);
                vres2 = VFMACCVV_FLOAT(vres2, va3, vb1, vl);
                vres3 = VFMACCVV_FLOAT(vres3, va2, vb1, vl);

#endif
#if   defined(RR) || defined(RC) || defined(CR) || defined(CC)
                vres0 = VFMACCVV_FLOAT(vres0, va0, vb0, vl);
                vres1 = VFNMSACVV_FLOAT(vres1, va1, vb0, vl);
                vres0 = VFNMSACVV_FLOAT(vres0, va1, vb1, vl);
                vres1 = VFNMSACVV_FLOAT(vres1, va0, vb1, vl);

                vres2 = VFMACCVV_FLOAT(vres2, va2, vb0, vl);
                vres3 = VFNMSACVV_FLOAT(vres3, va3, vb0, vl);
                vres2 = VFNMSACVV_FLOAT(vres2, va3, vb1, vl);
                vres3 = VFNMSACVV_FLOAT(vres3, va2, vb1, vl);

#endif
                ptrba += vl * 4;
                ptrbb += vl * 2;
            }
            v_m1_res0 = VFREDSUMVS_FLOAT(v_m1_res0, vres0, v_z0, vlmax);
            v_m1_res1 = VFREDSUMVS_FLOAT(v_m1_res1, vres1, v_z0, vlmax);
            res0 = VFMVFS_FLOAT_M1(v_m1_res0);
            res1 = VFMVFS_FLOAT_M1(v_m1_res1);
            C0[0] = res0 * alphar - res1 * alphai;
            C0[1] = res1 * alphar + res0 * alphai;

            v_m1_res0 = VFREDSUMVS_FLOAT(v_m1_res0, vres2, v_z0, vlmax);
            v_m1_res1 = VFREDSUMVS_FLOAT(v_m1_res1, vres3, v_z0, vlmax);
            res0 = VFMVFS_FLOAT_M1(v_m1_res0);
            res1 = VFMVFS_FLOAT_M1(v_m1_res1);
            C0[2] = res0 * alphar - res1 * alphai;
            C0[3] = res1 * alphar + res0 * alphai;

#if ( defined(LEFT) &&  defined(TRANSA)) || \
            (!defined(LEFT) && !defined(TRANSA))
            temp = bk-off;
#ifdef LEFT
            temp -= 2;
#else
            temp -= 1;
#endif
            ptrba += temp*2*2;
            ptrbb += temp*2;
#endif
#ifdef LEFT
            off += 2;
#endif
            C0 = C0+4;
        }

        if (bm & 1)
        {
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            ptrbb = bb;
#else
            ptrba += off*2;
            ptrbb = bb + off*2;
#endif
            vres0 = VFMVVF_FLOAT(0.0, vlmax);
            vres1 = VFMVVF_FLOAT(0.0, vlmax);

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
            temp = bk-off;
#elif defined(LEFT)
            temp = off + 1;
#else
            temp = off + 1;
#endif

            for (k = temp; k > 0; k -= vl)
            {
                vl = VSETVL(k);
                VLSEG2_FLOAT(&va0, &va1, ptrba, vl);
                VLSEG2_FLOAT(&vb0, &vb1, ptrbb, vl);

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
                vres0 = VFMACCVV_FLOAT(vres0, va0, vb0, vl);
                vres1 = VFMACCVV_FLOAT(vres1, va1, vb0, vl);
                vres0 = VFNMSACVV_FLOAT(vres0, va1, vb1, vl);
                vres1 = VFMACCVV_FLOAT(vres1, va0, vb1, vl);

#endif
#if   defined(NR) || defined(NC) || defined(TR) || defined(TC)
                vres0 = VFMACCVV_FLOAT(vres0, va0, vb0, vl);
                vres1 = VFMACCVV_FLOAT(vres1, va1, vb0, vl);
                vres0 = VFMACCVV_FLOAT(vres0, va1, vb1, vl);
                vres1 = VFNMSACVV_FLOAT(vres1, va0, vb1, vl);

#endif
#if   defined(RN) || defined(RT) || defined(CN) || defined(CT)
                vres0 = VFMACCVV_FLOAT(vres0, va0, vb0, vl);
                vres1 = VFNMSACVV_FLOAT(vres1, va1, vb0, vl);
                vres0 = VFMACCVV_FLOAT(vres0, va1, vb1, vl);
                vres1 = VFMACCVV_FLOAT(vres1, va0, vb1, vl);

#endif
#if   defined(RR) || defined(RC) || defined(CR) || defined(CC)
                vres0 = VFMACCVV_FLOAT(vres0, va0, vb0, vl);
                vres1 = VFNMSACVV_FLOAT(vres1, va1, vb0, vl);
                vres0 = VFNMSACVV_FLOAT(vres0, va1, vb1, vl);
                vres1 = VFNMSACVV_FLOAT(vres1, va0, vb1, vl);

#endif
                ptrba += vl * 2;
                ptrbb += vl * 2;
                
            }
            
            v_m1_res0 = VFREDSUMVS_FLOAT(v_m1_res0, vres0, v_z0, vlmax);
            v_m1_res1 = VFREDSUMVS_FLOAT(v_m1_res1, vres1, v_z0, vlmax);
            res0 = VFMVFS_FLOAT_M1(v_m1_res0);
            res1 = VFMVFS_FLOAT_M1(v_m1_res1);
            
            C0[0] = res0 * alphar - res1 * alphai;
            C0[1] = res1 * alphar + res0 * alphai;

#if ( defined(LEFT) &&  defined(TRANSA)) || \
            (!defined(LEFT) && !defined(TRANSA))
            temp = bk - off;
#ifdef LEFT
            temp -= 1;
#else
            temp -= 1;
#endif
            ptrba += temp*2;
            ptrbb += temp*2;
            
#endif
#ifdef LEFT
            off += 1;
#endif
            C0 = C0+2;
        }
        k = (bk<<1);
        bb = bb+k;
        i = (ldc<<1);
        C = C+i;
    }
    return 0;
}
