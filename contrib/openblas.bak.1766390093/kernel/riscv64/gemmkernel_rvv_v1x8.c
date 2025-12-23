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
#define VLEV_FLOAT              __riscv_vle32_v_f32m2
#define VSEV_FLOAT              __riscv_vse32_v_f32m2
#define VFMVVF_FLOAT            __riscv_vfmv_v_f_f32m2
#define VFMACCVF_FLOAT          __riscv_vfmacc_vf_f32m2
#else
#define VSETVL(n)               __riscv_vsetvl_e64m2(n)
#define FLOAT_V_T               vfloat64m2_t
#define VLEV_FLOAT              __riscv_vle64_v_f64m2
#define VSEV_FLOAT              __riscv_vse64_v_f64m2
#define VFMVVF_FLOAT            __riscv_vfmv_v_f_f64m2
#define VFMACCVF_FLOAT          __riscv_vfmacc_vf_f64m2
#endif

int CNAME(BLASLONG bm, BLASLONG bn, BLASLONG bk, FLOAT alpha, IFLOAT* ba, IFLOAT* bb, FLOAT* C, BLASLONG ldc
#ifdef TRMMKERNEL
		,BLASLONG offset
#endif
		)
{
    BLASLONG i,j,k;
    FLOAT *C0,*C1,*C2,*C3,*C4,*C5,*C6,*C7;
    IFLOAT *ptrba,*ptrbb;

    //fprintf(stderr, "%s, bm=%ld bn=%ld bk=%ld alpha=%f ldc=%ld\n", __FUNCTION__, bm, bn, bk, alpha, ldc); // Debug

    FLOAT_V_T va0, va1, va2, va3, va4, va5, va6, va7;
    FLOAT_V_T vres0, vres1, vres2, vres3, vres4, vres5, vres6, vres7;
    size_t vl;

    // N:8
    for (j = bn/8; j > 0; j--) {
        C0 = C;
        C1 = C0 + ldc;
        C2 = C1 + ldc;
        C3 = C2 + ldc;
        C4 = C3 + ldc;
        C5 = C4 + ldc;
        C6 = C5 + ldc;
        C7 = C6 + ldc;
        ptrba = ba;

        for (i = bm; i > 0; i -= vl) {
            vl = VSETVL(i);

            ptrbb = bb;

            vres0 = VFMVVF_FLOAT(0.0, vl);
            vres1 = VFMVVF_FLOAT(0.0, vl);
            vres2 = VFMVVF_FLOAT(0.0, vl);
            vres3 = VFMVVF_FLOAT(0.0, vl);
            vres4 = VFMVVF_FLOAT(0.0, vl);
            vres5 = VFMVVF_FLOAT(0.0, vl);
            vres6 = VFMVVF_FLOAT(0.0, vl);
            vres7 = VFMVVF_FLOAT(0.0, vl);
#if 0
            for (k = bk; k > 0; k--) {
                va0 = VLEV_FLOAT(ptrba, vl); 

                vres0 = VFMACCVF_FLOAT(vres0, *(ptrbb + 0), va0, vl);
                vres1 = VFMACCVF_FLOAT(vres1, *(ptrbb + 1), va0, vl);
                vres2 = VFMACCVF_FLOAT(vres2, *(ptrbb + 2), va0, vl);
                vres3 = VFMACCVF_FLOAT(vres3, *(ptrbb + 3), va0, vl);
                vres4 = VFMACCVF_FLOAT(vres4, *(ptrbb + 4), va0, vl);
                vres5 = VFMACCVF_FLOAT(vres5, *(ptrbb + 5), va0, vl);
                vres6 = VFMACCVF_FLOAT(vres6, *(ptrbb + 6), va0, vl);
                vres7 = VFMACCVF_FLOAT(vres7, *(ptrbb + 7), va0, vl);

                ptrba += vl;
                ptrbb += 8;
            }
#else
            // Unroll K
            for (k = bk/8; k > 0; k--) {
                va0 = VLEV_FLOAT(ptrba, vl);
                ptrba += vl;
                va1 = VLEV_FLOAT(ptrba, vl);
                ptrba += vl;
 
                vres0 = VFMACCVF_FLOAT(vres0, *(ptrbb + 0), va0, vl);
                vres1 = VFMACCVF_FLOAT(vres1, *(ptrbb + 1), va0, vl);
                vres2 = VFMACCVF_FLOAT(vres2, *(ptrbb + 2), va0, vl);
                vres3 = VFMACCVF_FLOAT(vres3, *(ptrbb + 3), va0, vl);
                vres4 = VFMACCVF_FLOAT(vres4, *(ptrbb + 4), va0, vl);
                vres5 = VFMACCVF_FLOAT(vres5, *(ptrbb + 5), va0, vl);
                vres6 = VFMACCVF_FLOAT(vres6, *(ptrbb + 6), va0, vl);
                vres7 = VFMACCVF_FLOAT(vres7, *(ptrbb + 7), va0, vl);
                ptrbb += 8;
  
                va2 = VLEV_FLOAT(ptrba, vl);
                ptrba += vl;

                vres0 = VFMACCVF_FLOAT(vres0, *(ptrbb + 0), va1, vl);
                vres1 = VFMACCVF_FLOAT(vres1, *(ptrbb + 1), va1, vl);
                vres2 = VFMACCVF_FLOAT(vres2, *(ptrbb + 2), va1, vl);
                vres3 = VFMACCVF_FLOAT(vres3, *(ptrbb + 3), va1, vl);
                vres4 = VFMACCVF_FLOAT(vres4, *(ptrbb + 4), va1, vl);
                vres5 = VFMACCVF_FLOAT(vres5, *(ptrbb + 5), va1, vl);
                vres6 = VFMACCVF_FLOAT(vres6, *(ptrbb + 6), va1, vl);
                vres7 = VFMACCVF_FLOAT(vres7, *(ptrbb + 7), va1, vl);
                ptrbb += 8;

                va3 = VLEV_FLOAT(ptrba, vl);
                ptrba += vl;

                vres0 = VFMACCVF_FLOAT(vres0, *(ptrbb + 0), va2, vl);
                vres1 = VFMACCVF_FLOAT(vres1, *(ptrbb + 1), va2, vl);
                vres2 = VFMACCVF_FLOAT(vres2, *(ptrbb + 2), va2, vl);
                vres3 = VFMACCVF_FLOAT(vres3, *(ptrbb + 3), va2, vl);
                vres4 = VFMACCVF_FLOAT(vres4, *(ptrbb + 4), va2, vl);
                vres5 = VFMACCVF_FLOAT(vres5, *(ptrbb + 5), va2, vl);
                vres6 = VFMACCVF_FLOAT(vres6, *(ptrbb + 6), va2, vl);
                vres7 = VFMACCVF_FLOAT(vres7, *(ptrbb + 7), va2, vl);
                ptrbb += 8;

                va4 = VLEV_FLOAT(ptrba, vl);
                ptrba += vl;

                vres0 = VFMACCVF_FLOAT(vres0, *(ptrbb + 0), va3, vl);
                vres1 = VFMACCVF_FLOAT(vres1, *(ptrbb + 1), va3, vl);
                vres2 = VFMACCVF_FLOAT(vres2, *(ptrbb + 2), va3, vl);
                vres3 = VFMACCVF_FLOAT(vres3, *(ptrbb + 3), va3, vl);
                vres4 = VFMACCVF_FLOAT(vres4, *(ptrbb + 4), va3, vl);
                vres5 = VFMACCVF_FLOAT(vres5, *(ptrbb + 5), va3, vl);
                vres6 = VFMACCVF_FLOAT(vres6, *(ptrbb + 6), va3, vl);
                vres7 = VFMACCVF_FLOAT(vres7, *(ptrbb + 7), va3, vl);
                ptrbb += 8;

                va5 = VLEV_FLOAT(ptrba, vl);
                ptrba += vl;

                vres0 = VFMACCVF_FLOAT(vres0, *(ptrbb + 0), va4, vl);
                vres1 = VFMACCVF_FLOAT(vres1, *(ptrbb + 1), va4, vl);
                vres2 = VFMACCVF_FLOAT(vres2, *(ptrbb + 2), va4, vl);
                vres3 = VFMACCVF_FLOAT(vres3, *(ptrbb + 3), va4, vl);
                vres4 = VFMACCVF_FLOAT(vres4, *(ptrbb + 4), va4, vl);
                vres5 = VFMACCVF_FLOAT(vres5, *(ptrbb + 5), va4, vl);
                vres6 = VFMACCVF_FLOAT(vres6, *(ptrbb + 6), va4, vl);
                vres7 = VFMACCVF_FLOAT(vres7, *(ptrbb + 7), va4, vl);
                ptrbb += 8;

                va6 = VLEV_FLOAT(ptrba, vl);
                ptrba += vl;

                vres0 = VFMACCVF_FLOAT(vres0, *(ptrbb + 0), va5, vl);
                vres1 = VFMACCVF_FLOAT(vres1, *(ptrbb + 1), va5, vl);
                vres2 = VFMACCVF_FLOAT(vres2, *(ptrbb + 2), va5, vl);
                vres3 = VFMACCVF_FLOAT(vres3, *(ptrbb + 3), va5, vl);
                vres4 = VFMACCVF_FLOAT(vres4, *(ptrbb + 4), va5, vl);
                vres5 = VFMACCVF_FLOAT(vres5, *(ptrbb + 5), va5, vl);
                vres6 = VFMACCVF_FLOAT(vres6, *(ptrbb + 6), va5, vl);
                vres7 = VFMACCVF_FLOAT(vres7, *(ptrbb + 7), va5, vl);
                ptrbb += 8;

                va7 = VLEV_FLOAT(ptrba, vl);
                ptrba += vl;

                vres0 = VFMACCVF_FLOAT(vres0, *(ptrbb + 0), va6, vl);
                vres1 = VFMACCVF_FLOAT(vres1, *(ptrbb + 1), va6, vl);
                vres2 = VFMACCVF_FLOAT(vres2, *(ptrbb + 2), va6, vl);
                vres3 = VFMACCVF_FLOAT(vres3, *(ptrbb + 3), va6, vl);
                vres4 = VFMACCVF_FLOAT(vres4, *(ptrbb + 4), va6, vl);
                vres5 = VFMACCVF_FLOAT(vres5, *(ptrbb + 5), va6, vl);
                vres6 = VFMACCVF_FLOAT(vres6, *(ptrbb + 6), va6, vl);
                vres7 = VFMACCVF_FLOAT(vres7, *(ptrbb + 7), va6, vl);
                ptrbb += 8;

                vres0 = VFMACCVF_FLOAT(vres0, *(ptrbb + 0), va7, vl);
                vres1 = VFMACCVF_FLOAT(vres1, *(ptrbb + 1), va7, vl);
                vres2 = VFMACCVF_FLOAT(vres2, *(ptrbb + 2), va7, vl);
                vres3 = VFMACCVF_FLOAT(vres3, *(ptrbb + 3), va7, vl);
                vres4 = VFMACCVF_FLOAT(vres4, *(ptrbb + 4), va7, vl);
                vres5 = VFMACCVF_FLOAT(vres5, *(ptrbb + 5), va7, vl);
                vres6 = VFMACCVF_FLOAT(vres6, *(ptrbb + 6), va7, vl);
                vres7 = VFMACCVF_FLOAT(vres7, *(ptrbb + 7), va7, vl);
                ptrbb += 8;
            }

            // K remainder
            for (k = bk&7; k > 0; k--) {
                va0 = VLEV_FLOAT(ptrba, vl);

                vres0 = VFMACCVF_FLOAT(vres0, *(ptrbb + 0), va0, vl);
                vres1 = VFMACCVF_FLOAT(vres1, *(ptrbb + 1), va0, vl);
                vres2 = VFMACCVF_FLOAT(vres2, *(ptrbb + 2), va0, vl);
                vres3 = VFMACCVF_FLOAT(vres3, *(ptrbb + 3), va0, vl);
                vres4 = VFMACCVF_FLOAT(vres4, *(ptrbb + 4), va0, vl);
                vres5 = VFMACCVF_FLOAT(vres5, *(ptrbb + 5), va0, vl);
                vres6 = VFMACCVF_FLOAT(vres6, *(ptrbb + 6), va0, vl);
                vres7 = VFMACCVF_FLOAT(vres7, *(ptrbb + 7), va0, vl);

                ptrbb += 8;
                ptrba += vl;
            }
#endif
            va0 = VLEV_FLOAT(C0, vl);
            va0 = VFMACCVF_FLOAT(va0, alpha, vres0, vl);
            VSEV_FLOAT(C0, va0, vl);

            va1 = VLEV_FLOAT(C1, vl);
            va1 = VFMACCVF_FLOAT(va1, alpha, vres1, vl);
            VSEV_FLOAT(C1, va1, vl);

            va2 = VLEV_FLOAT(C2, vl);
            va2 = VFMACCVF_FLOAT(va2, alpha, vres2, vl);
            VSEV_FLOAT(C2, va2, vl);

            va3 = VLEV_FLOAT(C3, vl);
            va3 = VFMACCVF_FLOAT(va3, alpha, vres3, vl);
            VSEV_FLOAT(C3, va3, vl);

            va4 = VLEV_FLOAT(C4, vl);
            va4 = VFMACCVF_FLOAT(va4, alpha, vres4, vl);
            VSEV_FLOAT(C4, va4, vl);

            va5 = VLEV_FLOAT(C5, vl);
            va5 = VFMACCVF_FLOAT(va5, alpha, vres5, vl);
            VSEV_FLOAT(C5, va5, vl);

            va6 = VLEV_FLOAT(C6, vl);
            va6 = VFMACCVF_FLOAT(va6, alpha, vres6, vl);
            VSEV_FLOAT(C6, va6, vl);

            va7 = VLEV_FLOAT(C7, vl);
            va7 = VFMACCVF_FLOAT(va7, alpha, vres7, vl);
            VSEV_FLOAT(C7, va7, vl);

            C0 += vl;
            C1 += vl;
            C2 += vl;
            C3 += vl;
            C4 += vl;
            C5 += vl;
            C6 += vl;
            C7 += vl;
        }

        bb += (bk<<3);
        C += (ldc<<3);
    }

    // N:4
    if (bn & 4) {
        C0 = C;
        C1 = C0 + ldc;
        C2 = C1 + ldc;
        C3 = C2 + ldc;
        ptrba = ba;

        for (i = bm; i > 0; i -= vl) {
            vl = VSETVL(i);

            ptrbb = bb;

            vres0 = VFMVVF_FLOAT(0.0, vl);
            vres1 = VFMVVF_FLOAT(0.0, vl);
            vres2 = VFMVVF_FLOAT(0.0, vl);
            vres3 = VFMVVF_FLOAT(0.0, vl);

#if 0
            for (k = bk; k > 0; k--) {
                va0 = VLEV_FLOAT(ptrba, vl); 

                vres0 = VFMACCVF_FLOAT(vres0, *(ptrbb + 0), va0, vl);
                vres1 = VFMACCVF_FLOAT(vres1, *(ptrbb + 1), va0, vl);
                vres2 = VFMACCVF_FLOAT(vres2, *(ptrbb + 2), va0, vl);
                vres3 = VFMACCVF_FLOAT(vres3, *(ptrbb + 3), va0, vl);

                ptrba += vl;
                ptrbb += 4;
            }
#else
            // Unroll K
            for (k = bk/8; k > 0; k--) {
                va0 = VLEV_FLOAT(ptrba, vl);
                ptrba += vl;
                va1 = VLEV_FLOAT(ptrba, vl);
                ptrba += vl;

                vres0 = VFMACCVF_FLOAT(vres0, *(ptrbb + 0), va0, vl);
                vres1 = VFMACCVF_FLOAT(vres1, *(ptrbb + 1), va0, vl);
                vres2 = VFMACCVF_FLOAT(vres2, *(ptrbb + 2), va0, vl);
                vres3 = VFMACCVF_FLOAT(vres3, *(ptrbb + 3), va0, vl);
                ptrbb += 4;
                va2 = VLEV_FLOAT(ptrba, vl);
                ptrba += vl;
  
                vres0 = VFMACCVF_FLOAT(vres0, *(ptrbb + 0), va1, vl);
                vres1 = VFMACCVF_FLOAT(vres1, *(ptrbb + 1), va1, vl);
                vres2 = VFMACCVF_FLOAT(vres2, *(ptrbb + 2), va1, vl);
                vres3 = VFMACCVF_FLOAT(vres3, *(ptrbb + 3), va1, vl);
                ptrbb += 4;
                va3 = VLEV_FLOAT(ptrba, vl);
                ptrba += vl;

                vres0 = VFMACCVF_FLOAT(vres0, *(ptrbb + 0), va2, vl);
                vres1 = VFMACCVF_FLOAT(vres1, *(ptrbb + 1), va2, vl);
                vres2 = VFMACCVF_FLOAT(vres2, *(ptrbb + 2), va2, vl);
                vres3 = VFMACCVF_FLOAT(vres3, *(ptrbb + 3), va2, vl);
                ptrbb += 4;
                va4 = VLEV_FLOAT(ptrba, vl);
                ptrba += vl;

                vres0 = VFMACCVF_FLOAT(vres0, *(ptrbb + 0), va3, vl);
                vres1 = VFMACCVF_FLOAT(vres1, *(ptrbb + 1), va3, vl);
                vres2 = VFMACCVF_FLOAT(vres2, *(ptrbb + 2), va3, vl);
                vres3 = VFMACCVF_FLOAT(vres3, *(ptrbb + 3), va3, vl);
                ptrbb += 4;
                va5 = VLEV_FLOAT(ptrba, vl);
                ptrba += vl;

                vres0 = VFMACCVF_FLOAT(vres0, *(ptrbb + 0), va4, vl);
                vres1 = VFMACCVF_FLOAT(vres1, *(ptrbb + 1), va4, vl);
                vres2 = VFMACCVF_FLOAT(vres2, *(ptrbb + 2), va4, vl);
                vres3 = VFMACCVF_FLOAT(vres3, *(ptrbb + 3), va4, vl);
                ptrbb += 4;
                va6 = VLEV_FLOAT(ptrba, vl);
                ptrba += vl;

                vres0 = VFMACCVF_FLOAT(vres0, *(ptrbb + 0), va5, vl);
                vres1 = VFMACCVF_FLOAT(vres1, *(ptrbb + 1), va5, vl);
                vres2 = VFMACCVF_FLOAT(vres2, *(ptrbb + 2), va5, vl);
                vres3 = VFMACCVF_FLOAT(vres3, *(ptrbb + 3), va5, vl);
                ptrbb += 4;
                va7 = VLEV_FLOAT(ptrba, vl);
                ptrba += vl;

                vres0 = VFMACCVF_FLOAT(vres0, *(ptrbb + 0), va6, vl);
                vres1 = VFMACCVF_FLOAT(vres1, *(ptrbb + 1), va6, vl);
                vres2 = VFMACCVF_FLOAT(vres2, *(ptrbb + 2), va6, vl);
                vres3 = VFMACCVF_FLOAT(vres3, *(ptrbb + 3), va6, vl);
                ptrbb += 4;

                vres0 = VFMACCVF_FLOAT(vres0, *(ptrbb + 0), va7, vl);
                vres1 = VFMACCVF_FLOAT(vres1, *(ptrbb + 1), va7, vl);
                vres2 = VFMACCVF_FLOAT(vres2, *(ptrbb + 2), va7, vl);
                vres3 = VFMACCVF_FLOAT(vres3, *(ptrbb + 3), va7, vl);
                ptrbb += 4;
            }

            // K remainder
            for (k = bk&7; k > 0; k--) {
                va0 = VLEV_FLOAT(ptrba, vl);

                vres0 = VFMACCVF_FLOAT(vres0, *(ptrbb + 0), va0, vl);
                vres1 = VFMACCVF_FLOAT(vres1, *(ptrbb + 1), va0, vl);
                vres2 = VFMACCVF_FLOAT(vres2, *(ptrbb + 2), va0, vl);
                vres3 = VFMACCVF_FLOAT(vres3, *(ptrbb + 3), va0, vl);

                ptrbb += 4;
                ptrba += vl;
            }
#endif
            va0 = VLEV_FLOAT(C0, vl);
            va0 = VFMACCVF_FLOAT(va0, alpha, vres0, vl);
            VSEV_FLOAT(C0, va0, vl);

            va1 = VLEV_FLOAT(C1, vl);
            va1 = VFMACCVF_FLOAT(va1, alpha, vres1, vl);
            VSEV_FLOAT(C1, va1, vl);

            va2 = VLEV_FLOAT(C2, vl);
            va2 = VFMACCVF_FLOAT(va2, alpha, vres2, vl);
            VSEV_FLOAT(C2, va2, vl);

            va3 = VLEV_FLOAT(C3, vl);
            va3 = VFMACCVF_FLOAT(va3, alpha, vres3, vl);
            VSEV_FLOAT(C3, va3, vl);

            C0 += vl;
            C1 += vl;
            C2 += vl;
            C3 += vl;
        }

        bb += (bk<<2);
        C += (ldc<<2);
    }

    // N:2
    if (bn & 2) {
        C0 = C;
        C1 = C0 + ldc;
        ptrba = ba;

        for (i = bm; i > 0; i -= vl) {
            vl = VSETVL(i);

            ptrbb = bb;

            vres0 = VFMVVF_FLOAT(0.0, vl);
            vres1 = VFMVVF_FLOAT(0.0, vl);
#if 0
            for (k = bk; k > 0; k--) {
                va0 = VLEV_FLOAT(ptrba, vl); 

                vres0 = VFMACCVF_FLOAT(vres0, *(ptrbb + 0), va0, vl);
                vres1 = VFMACCVF_FLOAT(vres1, *(ptrbb + 1), va0, vl);

                ptrba += vl;
                ptrbb += 2;
            }
#else
            // Unroll K
            for (k = bk/8; k > 0; k--) {
                va0 = VLEV_FLOAT(ptrba, vl);
                ptrba += vl;
                va1 = VLEV_FLOAT(ptrba, vl);
                ptrba += vl;

                vres0 = VFMACCVF_FLOAT(vres0, *(ptrbb + 0), va0, vl);
                vres1 = VFMACCVF_FLOAT(vres1, *(ptrbb + 1), va0, vl);
                ptrbb += 2;
                va2 = VLEV_FLOAT(ptrba, vl);
                ptrba += vl;

                vres0 = VFMACCVF_FLOAT(vres0, *(ptrbb + 0), va1, vl);
                vres1 = VFMACCVF_FLOAT(vres1, *(ptrbb + 1), va1, vl);
                ptrbb += 2;
                va3 = VLEV_FLOAT(ptrba, vl);
                ptrba += vl;

                vres0 = VFMACCVF_FLOAT(vres0, *(ptrbb + 0), va2, vl);
                vres1 = VFMACCVF_FLOAT(vres1, *(ptrbb + 1), va2, vl);
                ptrbb += 2;
                va4 = VLEV_FLOAT(ptrba, vl);
                ptrba += vl;

                vres0 = VFMACCVF_FLOAT(vres0, *(ptrbb + 0), va3, vl);
                vres1 = VFMACCVF_FLOAT(vres1, *(ptrbb + 1), va3, vl);
                ptrbb += 2;
                va5 = VLEV_FLOAT(ptrba, vl);
                ptrba += vl;

                vres0 = VFMACCVF_FLOAT(vres0, *(ptrbb + 0), va4, vl);
                vres1 = VFMACCVF_FLOAT(vres1, *(ptrbb + 1), va4, vl);
                ptrbb += 2;
                va6 = VLEV_FLOAT(ptrba, vl);
                ptrba += vl;

                vres0 = VFMACCVF_FLOAT(vres0, *(ptrbb + 0), va5, vl);
                vres1 = VFMACCVF_FLOAT(vres1, *(ptrbb + 1), va5, vl);
                ptrbb += 2;
                va7 = VLEV_FLOAT(ptrba, vl);
                ptrba += vl; 

                vres0 = VFMACCVF_FLOAT(vres0, *(ptrbb + 0), va6, vl);
                vres1 = VFMACCVF_FLOAT(vres1, *(ptrbb + 1), va6, vl);
                ptrbb += 2;

                vres0 = VFMACCVF_FLOAT(vres0, *(ptrbb + 0), va7, vl);
                vres1 = VFMACCVF_FLOAT(vres1, *(ptrbb + 1), va7, vl);
                ptrbb += 2;
            }

            // K remainder
            for (k = bk&7; k > 0; k--) {
                va0 = VLEV_FLOAT(ptrba, vl);

                vres0 = VFMACCVF_FLOAT(vres0, *(ptrbb + 0), va0, vl);
                vres1 = VFMACCVF_FLOAT(vres1, *(ptrbb + 1), va0, vl);

                ptrbb += 2;
                ptrba += vl;
            }
#endif
            va0 = VLEV_FLOAT(C0, vl);
            va0 = VFMACCVF_FLOAT(va0, alpha, vres0, vl);
            VSEV_FLOAT(C0, va0, vl);

            va1 = VLEV_FLOAT(C1, vl);
            va1 = VFMACCVF_FLOAT(va1, alpha, vres1, vl);
            VSEV_FLOAT(C1, va1, vl);

            C0 += vl;
            C1 += vl;
        }

        bb += (bk<<1);
        C += (ldc<<1);
    }

    // N:1
    if (bn & 1) {
        C0 = C;
        ptrba = ba;

        for (i = bm; i > 0; i -= vl) {
            vl = VSETVL(i);

            ptrbb = bb;

            vres0 = VFMVVF_FLOAT(0.0, vl);
#if 0
            for (k = bk; k > 0; k--) {
                va0 = VLEV_FLOAT(ptrba, vl); 

                vres0 = VFMACCVF_FLOAT(vres0, *(ptrbb + 0), va0, vl);
 
                ptrba += vl;
                ptrbb += 1;
            }
#else
            // Unroll K
            for (k = bk/8; k > 0; k--) {
                va0 = VLEV_FLOAT(ptrba, vl);
                ptrba += vl;
                va1 = VLEV_FLOAT(ptrba, vl);
                ptrba += vl;

                vres0 = VFMACCVF_FLOAT(vres0, *(ptrbb + 0), va0, vl);
                ptrbb += 1;
                va2 = VLEV_FLOAT(ptrba, vl);
                ptrba += vl;

                vres0 = VFMACCVF_FLOAT(vres0, *(ptrbb + 0), va1, vl);
                ptrbb += 1;
                va3 = VLEV_FLOAT(ptrba, vl);
                ptrba += vl;

                vres0 = VFMACCVF_FLOAT(vres0, *(ptrbb + 0), va2, vl);
                ptrbb += 1;
                va4 = VLEV_FLOAT(ptrba, vl);
                ptrba += vl;

                vres0 = VFMACCVF_FLOAT(vres0, *(ptrbb + 0), va3, vl);
                ptrbb += 1;
                va5 = VLEV_FLOAT(ptrba, vl);
                ptrba += vl;

                vres0 = VFMACCVF_FLOAT(vres0, *(ptrbb + 0), va4, vl);
                ptrbb += 1;
                va6 = VLEV_FLOAT(ptrba, vl);
                ptrba += vl;

                vres0 = VFMACCVF_FLOAT(vres0, *(ptrbb + 0), va5, vl);
                ptrbb += 1;
                va7 = VLEV_FLOAT(ptrba, vl);
                ptrba += vl;

                vres0 = VFMACCVF_FLOAT(vres0, *(ptrbb + 0), va6, vl);
                ptrbb += 1;

                vres0 = VFMACCVF_FLOAT(vres0, *(ptrbb + 0), va7, vl);
                ptrbb += 1;
            }

            // K remainder
            for (k = bk&7; k > 0; k--) {
                va0 = VLEV_FLOAT(ptrba, vl);

                vres0 = VFMACCVF_FLOAT(vres0, *(ptrbb + 0), va0, vl);

                ptrbb += 1;
                ptrba += vl;
            }
#endif
            va0 = VLEV_FLOAT(C0, vl);
            va0 = VFMACCVF_FLOAT(va0, alpha, vres0, vl);
            VSEV_FLOAT(C0, va0, vl);
  
            C0 += vl;
        }

        bb += (bk);
        C += (ldc);
    }

    return 0;
}
