/*********************************************************************/
/* Copyright 2009, 2010 The University of Texas at Austin.           */
/* All rights reserved.                                              */
/*                                                                   */
/* Redistribution and use in source and binary forms, with or        */
/* without modification, are permitted provided that the following   */
/* conditions are met:                                               */
/*                                                                   */
/*   1. Redistributions of source code must retain the above         */
/*      copyright notice, this list of conditions and the following  */
/*      disclaimer.                                                  */
/*                                                                   */
/*   2. Redistributions in binary form must reproduce the above      */
/*      copyright notice, this list of conditions and the following  */
/*      disclaimer in the documentation and/or other materials       */
/*      provided with the distribution.                              */
/*                                                                   */
/*    THIS  SOFTWARE IS PROVIDED  BY THE  UNIVERSITY OF  TEXAS AT    */
/*    AUSTIN  ``AS IS''  AND ANY  EXPRESS OR  IMPLIED WARRANTIES,    */
/*    INCLUDING, BUT  NOT LIMITED  TO, THE IMPLIED  WARRANTIES OF    */
/*    MERCHANTABILITY  AND FITNESS FOR  A PARTICULAR  PURPOSE ARE    */
/*    DISCLAIMED.  IN  NO EVENT SHALL THE UNIVERSITY  OF TEXAS AT    */
/*    AUSTIN OR CONTRIBUTORS BE  LIABLE FOR ANY DIRECT, INDIRECT,    */
/*    INCIDENTAL,  SPECIAL, EXEMPLARY,  OR  CONSEQUENTIAL DAMAGES    */
/*    (INCLUDING, BUT  NOT LIMITED TO,  PROCUREMENT OF SUBSTITUTE    */
/*    GOODS  OR  SERVICES; LOSS  OF  USE,  DATA,  OR PROFITS;  OR    */
/*    BUSINESS INTERRUPTION) HOWEVER CAUSED  AND ON ANY THEORY OF    */
/*    LIABILITY, WHETHER  IN CONTRACT, STRICT  LIABILITY, OR TORT    */
/*    (INCLUDING NEGLIGENCE OR OTHERWISE)  ARISING IN ANY WAY OUT    */
/*    OF  THE  USE OF  THIS  SOFTWARE,  EVEN  IF ADVISED  OF  THE    */
/*    POSSIBILITY OF SUCH DAMAGE.                                    */
/*                                                                   */
/* The views and conclusions contained in the software and           */
/* documentation are those of the authors and should not be          */
/* interpreted as representing official policies, either expressed   */
/* or implied, of The University of Texas at Austin.                 */
/*********************************************************************/

#include "common.h"
#include <altivec.h>

static FLOAT dm1 = -1.;

#ifdef CONJ
#define GEMM_KERNEL   GEMM_KERNEL_R
#else
#define GEMM_KERNEL   GEMM_KERNEL_N
#endif

#if GEMM_DEFAULT_UNROLL_M == 1
#define GEMM_UNROLL_M_SHIFT 0
#endif

#if GEMM_DEFAULT_UNROLL_M == 2
#define GEMM_UNROLL_M_SHIFT 1
#endif

#if GEMM_DEFAULT_UNROLL_M == 4
#define GEMM_UNROLL_M_SHIFT 2
#endif

#if GEMM_DEFAULT_UNROLL_M == 6
#define GEMM_UNROLL_M_SHIFT 2
#endif


#if GEMM_DEFAULT_UNROLL_M == 8
#define GEMM_UNROLL_M_SHIFT 3
#endif

#if GEMM_DEFAULT_UNROLL_M == 16
#define GEMM_UNROLL_M_SHIFT 4
#endif

#if GEMM_DEFAULT_UNROLL_N == 1
#define GEMM_UNROLL_N_SHIFT 0
#endif

#if GEMM_DEFAULT_UNROLL_N == 2
#define GEMM_UNROLL_N_SHIFT 1
#endif

#if GEMM_DEFAULT_UNROLL_N == 4
#define GEMM_UNROLL_N_SHIFT 2
#endif

#if GEMM_DEFAULT_UNROLL_N == 8
#define GEMM_UNROLL_N_SHIFT 3
#endif

#if GEMM_DEFAULT_UNROLL_N == 16
#define GEMM_UNROLL_N_SHIFT 4
#endif

#ifndef COMPLEX

#ifdef DOUBLE

static inline __attribute__ ((always_inline)) void solve8x8(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc) {
   FLOAT *c0, *c1, *c2, *c3, *c4, *c5, *c6, *c7;
   c0 = &c[0*ldc];
   c1 = &c[1*ldc];
   c2 = &c[2*ldc];
   c3 = &c[3*ldc];
   c4 = &c[4*ldc];
   c5 = &c[5*ldc];
   c6 = &c[6*ldc];
   c7 = &c[7*ldc];
   vector FLOAT *Vb = (vector FLOAT *) b;
   vector FLOAT *Vc0 = (vector FLOAT *) c0;
   vector FLOAT *Vc1 = (vector FLOAT *) c1;
   vector FLOAT *Vc2 = (vector FLOAT *) c2;
   vector FLOAT *Vc3 = (vector FLOAT *) c3;
   vector FLOAT *Vc4 = (vector FLOAT *) c4;
   vector FLOAT *Vc5 = (vector FLOAT *) c5;
   vector FLOAT *Vc6 = (vector FLOAT *) c6;
   vector FLOAT *Vc7 = (vector FLOAT *) c7;
   vector FLOAT VbS0, VbS1, VbS2, VbS3, VbS4, VbS5, VbS6;

   a[56] = (c7[0] *= b[63]);
   a[57] = (c7[1] *= b[63]);
   a[58] = (c7[2] *= b[63]);
   a[59] = (c7[3] *= b[63]);
   a[60] = (c7[4] *= b[63]);
   a[61] = (c7[5] *= b[63]);
   a[62] = (c7[6] *= b[63]);
   a[63] = (c7[7] *= b[63]);
   VbS0 = vec_splat(Vb[28], 0);
   VbS1 = vec_splat(Vb[28], 1);
   VbS2 = vec_splat(Vb[29], 0);
   VbS3 = vec_splat(Vb[29], 1);
   VbS4 = vec_splat(Vb[30], 0);
   VbS5 = vec_splat(Vb[30], 1);
   VbS6 = vec_splat(Vb[31], 0);
   Vc0[0] = vec_nmsub(Vc7[0], VbS0, Vc0[0]);
   Vc0[1] = vec_nmsub(Vc7[1], VbS0, Vc0[1]);
   Vc0[2] = vec_nmsub(Vc7[2], VbS0, Vc0[2]);
   Vc0[3] = vec_nmsub(Vc7[3], VbS0, Vc0[3]);
   Vc1[0] = vec_nmsub(Vc7[0], VbS1, Vc1[0]);
   Vc1[1] = vec_nmsub(Vc7[1], VbS1, Vc1[1]);
   Vc1[2] = vec_nmsub(Vc7[2], VbS1, Vc1[2]);
   Vc1[3] = vec_nmsub(Vc7[3], VbS1, Vc1[3]);
   Vc2[0] = vec_nmsub(Vc7[0], VbS2, Vc2[0]);
   Vc2[1] = vec_nmsub(Vc7[1], VbS2, Vc2[1]);
   Vc2[2] = vec_nmsub(Vc7[2], VbS2, Vc2[2]);
   Vc2[3] = vec_nmsub(Vc7[3], VbS2, Vc2[3]);
   Vc3[0] = vec_nmsub(Vc7[0], VbS3, Vc3[0]);
   Vc3[1] = vec_nmsub(Vc7[1], VbS3, Vc3[1]);
   Vc3[2] = vec_nmsub(Vc7[2], VbS3, Vc3[2]);
   Vc3[3] = vec_nmsub(Vc7[3], VbS3, Vc3[3]);
   Vc4[0] = vec_nmsub(Vc7[0], VbS4, Vc4[0]);
   Vc4[1] = vec_nmsub(Vc7[1], VbS4, Vc4[1]);
   Vc4[2] = vec_nmsub(Vc7[2], VbS4, Vc4[2]);
   Vc4[3] = vec_nmsub(Vc7[3], VbS4, Vc4[3]);
   Vc5[0] = vec_nmsub(Vc7[0], VbS5, Vc5[0]);
   Vc5[1] = vec_nmsub(Vc7[1], VbS5, Vc5[1]);
   Vc5[2] = vec_nmsub(Vc7[2], VbS5, Vc5[2]);
   Vc5[3] = vec_nmsub(Vc7[3], VbS5, Vc5[3]);
   Vc6[0] = vec_nmsub(Vc7[0], VbS6, Vc6[0]);
   Vc6[1] = vec_nmsub(Vc7[1], VbS6, Vc6[1]);
   Vc6[2] = vec_nmsub(Vc7[2], VbS6, Vc6[2]);
   Vc6[3] = vec_nmsub(Vc7[3], VbS6, Vc6[3]);

   a[48] = (c6[0] *= b[54]);
   a[49] = (c6[1] *= b[54]);
   a[50] = (c6[2] *= b[54]);
   a[51] = (c6[3] *= b[54]);
   a[52] = (c6[4] *= b[54]);
   a[53] = (c6[5] *= b[54]);
   a[54] = (c6[6] *= b[54]);
   a[55] = (c6[7] *= b[54]);
   VbS0 = vec_splat(Vb[24], 0);
   VbS1 = vec_splat(Vb[24], 1);
   VbS2 = vec_splat(Vb[25], 0);
   VbS3 = vec_splat(Vb[25], 1);
   VbS4 = vec_splat(Vb[26], 0);
   VbS5 = vec_splat(Vb[26], 1);
   Vc0[0] = vec_nmsub(Vc6[0], VbS0, Vc0[0]);
   Vc0[1] = vec_nmsub(Vc6[1], VbS0, Vc0[1]);
   Vc0[2] = vec_nmsub(Vc6[2], VbS0, Vc0[2]);
   Vc0[3] = vec_nmsub(Vc6[3], VbS0, Vc0[3]);
   Vc1[0] = vec_nmsub(Vc6[0], VbS1, Vc1[0]);
   Vc1[1] = vec_nmsub(Vc6[1], VbS1, Vc1[1]);
   Vc1[2] = vec_nmsub(Vc6[2], VbS1, Vc1[2]);
   Vc1[3] = vec_nmsub(Vc6[3], VbS1, Vc1[3]);
   Vc2[0] = vec_nmsub(Vc6[0], VbS2, Vc2[0]);
   Vc2[1] = vec_nmsub(Vc6[1], VbS2, Vc2[1]);
   Vc2[2] = vec_nmsub(Vc6[2], VbS2, Vc2[2]);
   Vc2[3] = vec_nmsub(Vc6[3], VbS2, Vc2[3]);
   Vc3[0] = vec_nmsub(Vc6[0], VbS3, Vc3[0]);
   Vc3[1] = vec_nmsub(Vc6[1], VbS3, Vc3[1]);
   Vc3[2] = vec_nmsub(Vc6[2], VbS3, Vc3[2]);
   Vc3[3] = vec_nmsub(Vc6[3], VbS3, Vc3[3]);
   Vc4[0] = vec_nmsub(Vc6[0], VbS4, Vc4[0]);
   Vc4[1] = vec_nmsub(Vc6[1], VbS4, Vc4[1]);
   Vc4[2] = vec_nmsub(Vc6[2], VbS4, Vc4[2]);
   Vc4[3] = vec_nmsub(Vc6[3], VbS4, Vc4[3]);
   Vc5[0] = vec_nmsub(Vc6[0], VbS5, Vc5[0]);
   Vc5[1] = vec_nmsub(Vc6[1], VbS5, Vc5[1]);
   Vc5[2] = vec_nmsub(Vc6[2], VbS5, Vc5[2]);
   Vc5[3] = vec_nmsub(Vc6[3], VbS5, Vc5[3]);

   a[40] = (c5[0] *= b[45]);
   a[41] = (c5[1] *= b[45]);
   a[42] = (c5[2] *= b[45]);
   a[43] = (c5[3] *= b[45]);
   a[44] = (c5[4] *= b[45]);
   a[45] = (c5[5] *= b[45]);
   a[46] = (c5[6] *= b[45]);
   a[47] = (c5[7] *= b[45]);
   VbS0 = vec_splat(Vb[20], 0);
   VbS1 = vec_splat(Vb[20], 1);
   VbS2 = vec_splat(Vb[21], 0);
   VbS3 = vec_splat(Vb[21], 1);
   VbS4 = vec_splat(Vb[22], 0);
   Vc0[0] = vec_nmsub(Vc5[0], VbS0, Vc0[0]);
   Vc0[1] = vec_nmsub(Vc5[1], VbS0, Vc0[1]);
   Vc0[2] = vec_nmsub(Vc5[2], VbS0, Vc0[2]);
   Vc0[3] = vec_nmsub(Vc5[3], VbS0, Vc0[3]);
   Vc1[0] = vec_nmsub(Vc5[0], VbS1, Vc1[0]);
   Vc1[1] = vec_nmsub(Vc5[1], VbS1, Vc1[1]);
   Vc1[2] = vec_nmsub(Vc5[2], VbS1, Vc1[2]);
   Vc1[3] = vec_nmsub(Vc5[3], VbS1, Vc1[3]);
   Vc2[0] = vec_nmsub(Vc5[0], VbS2, Vc2[0]);
   Vc2[1] = vec_nmsub(Vc5[1], VbS2, Vc2[1]);
   Vc2[2] = vec_nmsub(Vc5[2], VbS2, Vc2[2]);
   Vc2[3] = vec_nmsub(Vc5[3], VbS2, Vc2[3]);
   Vc3[0] = vec_nmsub(Vc5[0], VbS3, Vc3[0]);
   Vc3[1] = vec_nmsub(Vc5[1], VbS3, Vc3[1]);
   Vc3[2] = vec_nmsub(Vc5[2], VbS3, Vc3[2]);
   Vc3[3] = vec_nmsub(Vc5[3], VbS3, Vc3[3]);
   Vc4[0] = vec_nmsub(Vc5[0], VbS4, Vc4[0]);
   Vc4[1] = vec_nmsub(Vc5[1], VbS4, Vc4[1]);
   Vc4[2] = vec_nmsub(Vc5[2], VbS4, Vc4[2]);
   Vc4[3] = vec_nmsub(Vc5[3], VbS4, Vc4[3]);

   a[32] = (c4[0] *= b[36]);
   a[33] = (c4[1] *= b[36]);
   a[34] = (c4[2] *= b[36]);
   a[35] = (c4[3] *= b[36]);
   a[36] = (c4[4] *= b[36]);
   a[37] = (c4[5] *= b[36]);
   a[38] = (c4[6] *= b[36]);
   a[39] = (c4[7] *= b[36]);
   VbS0 = vec_splat(Vb[16], 0);
   VbS1 = vec_splat(Vb[16], 1);
   VbS2 = vec_splat(Vb[17], 0);
   VbS3 = vec_splat(Vb[17], 1);
   Vc0[0] = vec_nmsub(Vc4[0], VbS0, Vc0[0]);
   Vc0[1] = vec_nmsub(Vc4[1], VbS0, Vc0[1]);
   Vc0[2] = vec_nmsub(Vc4[2], VbS0, Vc0[2]);
   Vc0[3] = vec_nmsub(Vc4[3], VbS0, Vc0[3]);
   Vc1[0] = vec_nmsub(Vc4[0], VbS1, Vc1[0]);
   Vc1[1] = vec_nmsub(Vc4[1], VbS1, Vc1[1]);
   Vc1[2] = vec_nmsub(Vc4[2], VbS1, Vc1[2]);
   Vc1[3] = vec_nmsub(Vc4[3], VbS1, Vc1[3]);
   Vc2[0] = vec_nmsub(Vc4[0], VbS2, Vc2[0]);
   Vc2[1] = vec_nmsub(Vc4[1], VbS2, Vc2[1]);
   Vc2[2] = vec_nmsub(Vc4[2], VbS2, Vc2[2]);
   Vc2[3] = vec_nmsub(Vc4[3], VbS2, Vc2[3]);
   Vc3[0] = vec_nmsub(Vc4[0], VbS3, Vc3[0]);
   Vc3[1] = vec_nmsub(Vc4[1], VbS3, Vc3[1]);
   Vc3[2] = vec_nmsub(Vc4[2], VbS3, Vc3[2]);
   Vc3[3] = vec_nmsub(Vc4[3], VbS3, Vc3[3]);

   a[24] = (c3[0] *= b[27]);
   a[25] = (c3[1] *= b[27]);
   a[26] = (c3[2] *= b[27]);
   a[27] = (c3[3] *= b[27]);
   a[28] = (c3[4] *= b[27]);
   a[29] = (c3[5] *= b[27]);
   a[30] = (c3[6] *= b[27]);
   a[31] = (c3[7] *= b[27]);
   VbS0 = vec_splat(Vb[12], 0);
   VbS1 = vec_splat(Vb[12], 1);
   VbS2 = vec_splat(Vb[13], 0);
   Vc0[0] = vec_nmsub(Vc3[0], VbS0, Vc0[0]);
   Vc0[1] = vec_nmsub(Vc3[1], VbS0, Vc0[1]);
   Vc0[2] = vec_nmsub(Vc3[2], VbS0, Vc0[2]);
   Vc0[3] = vec_nmsub(Vc3[3], VbS0, Vc0[3]);
   Vc1[0] = vec_nmsub(Vc3[0], VbS1, Vc1[0]);
   Vc1[1] = vec_nmsub(Vc3[1], VbS1, Vc1[1]);
   Vc1[2] = vec_nmsub(Vc3[2], VbS1, Vc1[2]);
   Vc1[3] = vec_nmsub(Vc3[3], VbS1, Vc1[3]);
   Vc2[0] = vec_nmsub(Vc3[0], VbS2, Vc2[0]);
   Vc2[1] = vec_nmsub(Vc3[1], VbS2, Vc2[1]);
   Vc2[2] = vec_nmsub(Vc3[2], VbS2, Vc2[2]);
   Vc2[3] = vec_nmsub(Vc3[3], VbS2, Vc2[3]);

   a[16] = (c2[0] *= b[18]);
   a[17] = (c2[1] *= b[18]);
   a[18] = (c2[2] *= b[18]);
   a[19] = (c2[3] *= b[18]);
   a[20] = (c2[4] *= b[18]);
   a[21] = (c2[5] *= b[18]);
   a[22] = (c2[6] *= b[18]);
   a[23] = (c2[7] *= b[18]);
   VbS0 = vec_splat(Vb[8], 0);
   VbS1 = vec_splat(Vb[8], 1);
   Vc0[0] = vec_nmsub(Vc2[0], VbS0, Vc0[0]);
   Vc0[1] = vec_nmsub(Vc2[1], VbS0, Vc0[1]);
   Vc0[2] = vec_nmsub(Vc2[2], VbS0, Vc0[2]);
   Vc0[3] = vec_nmsub(Vc2[3], VbS0, Vc0[3]);
   Vc1[0] = vec_nmsub(Vc2[0], VbS1, Vc1[0]);
   Vc1[1] = vec_nmsub(Vc2[1], VbS1, Vc1[1]);
   Vc1[2] = vec_nmsub(Vc2[2], VbS1, Vc1[2]);
   Vc1[3] = vec_nmsub(Vc2[3], VbS1, Vc1[3]);

   a[ 8] = (c1[0] *= b[9]);
   a[ 9] = (c1[1] *= b[9]);
   a[10] = (c1[2] *= b[9]);
   a[11] = (c1[3] *= b[9]);
   a[12] = (c1[4] *= b[9]);
   a[13] = (c1[5] *= b[9]);
   a[14] = (c1[6] *= b[9]);
   a[15] = (c1[7] *= b[9]);
   VbS0 = vec_splat(Vb[4], 0);
   Vc0[0] = vec_nmsub(Vc1[0], VbS0, Vc0[0]);
   Vc0[1] = vec_nmsub(Vc1[1], VbS0, Vc0[1]);
   Vc0[2] = vec_nmsub(Vc1[2], VbS0, Vc0[2]);
   Vc0[3] = vec_nmsub(Vc1[3], VbS0, Vc0[3]);

   a[0] = (c0[0] *= b[0]);
   a[1] = (c0[1] *= b[0]);
   a[2] = (c0[2] *= b[0]);
   a[3] = (c0[3] *= b[0]);
   a[4] = (c0[4] *= b[0]);
   a[5] = (c0[5] *= b[0]);
   a[6] = (c0[6] *= b[0]);
   a[7] = (c0[7] *= b[0]);
}

#else

static inline __attribute__ ((always_inline)) void solve16x8(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc) {
   FLOAT *c0, *c1, *c2, *c3, *c4, *c5, *c6, *c7;
   c0 = &c[0*ldc];
   c1 = &c[1*ldc];
   c2 = &c[2*ldc];
   c3 = &c[3*ldc];
   c4 = &c[4*ldc];
   c5 = &c[5*ldc];
   c6 = &c[6*ldc];
   c7 = &c[7*ldc];

   vector FLOAT *Va = (vector FLOAT *) a;
   vector FLOAT *Vb = (vector FLOAT *) b;
   vector FLOAT *Vc0 = (vector FLOAT *) c0;
   vector FLOAT *Vc1 = (vector FLOAT *) c1;
   vector FLOAT *Vc2 = (vector FLOAT *) c2;
   vector FLOAT *Vc3 = (vector FLOAT *) c3;
   vector FLOAT *Vc4 = (vector FLOAT *) c4;
   vector FLOAT *Vc5 = (vector FLOAT *) c5;
   vector FLOAT *Vc6 = (vector FLOAT *) c6;
   vector FLOAT *Vc7 = (vector FLOAT *) c7;
   vector FLOAT VbS0, VbS1, VbS2, VbS3, VbS4, VbS5, VbS6, VbS7;

   VbS0 = vec_splat(Vb[14], 0);
   VbS1 = vec_splat(Vb[14], 1);
   VbS2 = vec_splat(Vb[14], 2);
   VbS3 = vec_splat(Vb[14], 3);
   VbS4 = vec_splat(Vb[15], 0);
   VbS5 = vec_splat(Vb[15], 1);
   VbS6 = vec_splat(Vb[15], 2);
   VbS7 = vec_splat(Vb[15], 3);

   Vc7[0] = vec_mul(VbS7, Vc7[0]);
   Vc7[1] = vec_mul(VbS7, Vc7[1]);
   Vc7[2] = vec_mul(VbS7, Vc7[2]);
   Vc7[3] = vec_mul(VbS7, Vc7[3]);
   Va[28] = Vc7[0];
   Va[29] = Vc7[1];
   Va[30] = Vc7[2];
   Va[31] = Vc7[3];
   Vc0[0] = vec_nmsub(VbS0, Va[28], Vc0[0]);
   Vc0[1] = vec_nmsub(VbS0, Va[29], Vc0[1]);
   Vc0[2] = vec_nmsub(VbS0, Va[30], Vc0[2]);
   Vc0[3] = vec_nmsub(VbS0, Va[31], Vc0[3]);
   Vc1[0] = vec_nmsub(VbS1, Va[28], Vc1[0]);
   Vc1[1] = vec_nmsub(VbS1, Va[29], Vc1[1]);
   Vc1[2] = vec_nmsub(VbS1, Va[30], Vc1[2]);
   Vc1[3] = vec_nmsub(VbS1, Va[31], Vc1[3]);
   Vc2[0] = vec_nmsub(VbS2, Va[28], Vc2[0]);
   Vc2[1] = vec_nmsub(VbS2, Va[29], Vc2[1]);
   Vc2[2] = vec_nmsub(VbS2, Va[30], Vc2[2]);
   Vc2[3] = vec_nmsub(VbS2, Va[31], Vc2[3]);
   Vc3[0] = vec_nmsub(VbS3, Va[28], Vc3[0]);
   Vc3[1] = vec_nmsub(VbS3, Va[29], Vc3[1]);
   Vc3[2] = vec_nmsub(VbS3, Va[30], Vc3[2]);
   Vc3[3] = vec_nmsub(VbS3, Va[31], Vc3[3]);
   Vc4[0] = vec_nmsub(VbS4, Va[28], Vc4[0]);
   Vc4[1] = vec_nmsub(VbS4, Va[29], Vc4[1]);
   Vc4[2] = vec_nmsub(VbS4, Va[30], Vc4[2]);
   Vc4[3] = vec_nmsub(VbS4, Va[31], Vc4[3]);
   Vc5[0] = vec_nmsub(VbS5, Va[28], Vc5[0]);
   Vc5[1] = vec_nmsub(VbS5, Va[29], Vc5[1]);
   Vc5[2] = vec_nmsub(VbS5, Va[30], Vc5[2]);
   Vc5[3] = vec_nmsub(VbS5, Va[31], Vc5[3]);
   Vc6[0] = vec_nmsub(VbS6, Va[28], Vc6[0]);
   Vc6[1] = vec_nmsub(VbS6, Va[29], Vc6[1]);
   Vc6[2] = vec_nmsub(VbS6, Va[30], Vc6[2]);
   Vc6[3] = vec_nmsub(VbS6, Va[31], Vc6[3]);

   VbS0 = vec_splat(Vb[12], 0);
   VbS1 = vec_splat(Vb[12], 1);
   VbS2 = vec_splat(Vb[12], 2);
   VbS3 = vec_splat(Vb[12], 3);
   VbS4 = vec_splat(Vb[13], 0);
   VbS5 = vec_splat(Vb[13], 1);
   VbS6 = vec_splat(Vb[13], 2);

   Vc6[0] = vec_mul(VbS6, Vc6[0]);
   Vc6[1] = vec_mul(VbS6, Vc6[1]);
   Vc6[2] = vec_mul(VbS6, Vc6[2]);
   Vc6[3] = vec_mul(VbS6, Vc6[3]);
   Va[24] = Vc6[0];
   Va[25] = Vc6[1];
   Va[26] = Vc6[2];
   Va[27] = Vc6[3];
   Vc0[0] = vec_nmsub(VbS0, Va[24], Vc0[0]);
   Vc0[1] = vec_nmsub(VbS0, Va[25], Vc0[1]);
   Vc0[2] = vec_nmsub(VbS0, Va[26], Vc0[2]);
   Vc0[3] = vec_nmsub(VbS0, Va[27], Vc0[3]);
   Vc1[0] = vec_nmsub(VbS1, Va[24], Vc1[0]);
   Vc1[1] = vec_nmsub(VbS1, Va[25], Vc1[1]);
   Vc1[2] = vec_nmsub(VbS1, Va[26], Vc1[2]);
   Vc1[3] = vec_nmsub(VbS1, Va[27], Vc1[3]);
   Vc2[0] = vec_nmsub(VbS2, Va[24], Vc2[0]);
   Vc2[1] = vec_nmsub(VbS2, Va[25], Vc2[1]);
   Vc2[2] = vec_nmsub(VbS2, Va[26], Vc2[2]);
   Vc2[3] = vec_nmsub(VbS2, Va[27], Vc2[3]);
   Vc3[0] = vec_nmsub(VbS3, Va[24], Vc3[0]);
   Vc3[1] = vec_nmsub(VbS3, Va[25], Vc3[1]);
   Vc3[2] = vec_nmsub(VbS3, Va[26], Vc3[2]);
   Vc3[3] = vec_nmsub(VbS3, Va[27], Vc3[3]);
   Vc4[0] = vec_nmsub(VbS4, Va[24], Vc4[0]);
   Vc4[1] = vec_nmsub(VbS4, Va[25], Vc4[1]);
   Vc4[2] = vec_nmsub(VbS4, Va[26], Vc4[2]);
   Vc4[3] = vec_nmsub(VbS4, Va[27], Vc4[3]);
   Vc5[0] = vec_nmsub(VbS5, Va[24], Vc5[0]);
   Vc5[1] = vec_nmsub(VbS5, Va[25], Vc5[1]);
   Vc5[2] = vec_nmsub(VbS5, Va[26], Vc5[2]);
   Vc5[3] = vec_nmsub(VbS5, Va[27], Vc5[3]);

   VbS0 = vec_splat(Vb[10], 0);
   VbS1 = vec_splat(Vb[10], 1);
   VbS2 = vec_splat(Vb[10], 2);
   VbS3 = vec_splat(Vb[10], 3);
   VbS4 = vec_splat(Vb[11], 0);
   VbS5 = vec_splat(Vb[11], 1);

   Vc5[0] = vec_mul(VbS5, Vc5[0]);
   Vc5[1] = vec_mul(VbS5, Vc5[1]);
   Vc5[2] = vec_mul(VbS5, Vc5[2]);
   Vc5[3] = vec_mul(VbS5, Vc5[3]);
   Va[20] = Vc5[0];
   Va[21] = Vc5[1];
   Va[22] = Vc5[2];
   Va[23] = Vc5[3];
   Vc0[0] = vec_nmsub(VbS0, Va[20], Vc0[0]);
   Vc0[1] = vec_nmsub(VbS0, Va[21], Vc0[1]);
   Vc0[2] = vec_nmsub(VbS0, Va[22], Vc0[2]);
   Vc0[3] = vec_nmsub(VbS0, Va[23], Vc0[3]);
   Vc1[0] = vec_nmsub(VbS1, Va[20], Vc1[0]);
   Vc1[1] = vec_nmsub(VbS1, Va[21], Vc1[1]);
   Vc1[2] = vec_nmsub(VbS1, Va[22], Vc1[2]);
   Vc1[3] = vec_nmsub(VbS1, Va[23], Vc1[3]);
   Vc2[0] = vec_nmsub(VbS2, Va[20], Vc2[0]);
   Vc2[1] = vec_nmsub(VbS2, Va[21], Vc2[1]);
   Vc2[2] = vec_nmsub(VbS2, Va[22], Vc2[2]);
   Vc2[3] = vec_nmsub(VbS2, Va[23], Vc2[3]);
   Vc3[0] = vec_nmsub(VbS3, Va[20], Vc3[0]);
   Vc3[1] = vec_nmsub(VbS3, Va[21], Vc3[1]);
   Vc3[2] = vec_nmsub(VbS3, Va[22], Vc3[2]);
   Vc3[3] = vec_nmsub(VbS3, Va[23], Vc3[3]);
   Vc4[0] = vec_nmsub(VbS4, Va[20], Vc4[0]);
   Vc4[1] = vec_nmsub(VbS4, Va[21], Vc4[1]);
   Vc4[2] = vec_nmsub(VbS4, Va[22], Vc4[2]);
   Vc4[3] = vec_nmsub(VbS4, Va[23], Vc4[3]);

   VbS0 = vec_splat(Vb[8], 0);
   VbS1 = vec_splat(Vb[8], 1);
   VbS2 = vec_splat(Vb[8], 2);
   VbS3 = vec_splat(Vb[8], 3);
   VbS4 = vec_splat(Vb[9], 0);

   Vc4[0] = vec_mul(VbS4, Vc4[0]);
   Vc4[1] = vec_mul(VbS4, Vc4[1]);
   Vc4[2] = vec_mul(VbS4, Vc4[2]);
   Vc4[3] = vec_mul(VbS4, Vc4[3]);
   Va[16] = Vc4[0];
   Va[17] = Vc4[1];
   Va[18] = Vc4[2];
   Va[19] = Vc4[3];
   Vc0[0] = vec_nmsub(VbS0, Va[16], Vc0[0]);
   Vc0[1] = vec_nmsub(VbS0, Va[17], Vc0[1]);
   Vc0[2] = vec_nmsub(VbS0, Va[18], Vc0[2]);
   Vc0[3] = vec_nmsub(VbS0, Va[19], Vc0[3]);
   Vc1[0] = vec_nmsub(VbS1, Va[16], Vc1[0]);
   Vc1[1] = vec_nmsub(VbS1, Va[17], Vc1[1]);
   Vc1[2] = vec_nmsub(VbS1, Va[18], Vc1[2]);
   Vc1[3] = vec_nmsub(VbS1, Va[19], Vc1[3]);
   Vc2[0] = vec_nmsub(VbS2, Va[16], Vc2[0]);
   Vc2[1] = vec_nmsub(VbS2, Va[17], Vc2[1]);
   Vc2[2] = vec_nmsub(VbS2, Va[18], Vc2[2]);
   Vc2[3] = vec_nmsub(VbS2, Va[19], Vc2[3]);
   Vc3[0] = vec_nmsub(VbS3, Va[16], Vc3[0]);
   Vc3[1] = vec_nmsub(VbS3, Va[17], Vc3[1]);
   Vc3[2] = vec_nmsub(VbS3, Va[18], Vc3[2]);
   Vc3[3] = vec_nmsub(VbS3, Va[19], Vc3[3]);

   VbS0 = vec_splat(Vb[6], 0);
   VbS1 = vec_splat(Vb[6], 1);
   VbS2 = vec_splat(Vb[6], 2);
   VbS3 = vec_splat(Vb[6], 3);

   Vc3[0] = vec_mul(VbS3, Vc3[0]);
   Vc3[1] = vec_mul(VbS3, Vc3[1]);
   Vc3[2] = vec_mul(VbS3, Vc3[2]);
   Vc3[3] = vec_mul(VbS3, Vc3[3]);
   Va[12] = Vc3[0];
   Va[13] = Vc3[1];
   Va[14] = Vc3[2];
   Va[15] = Vc3[3];
   Vc0[0] = vec_nmsub(VbS0, Va[12], Vc0[0]);
   Vc0[1] = vec_nmsub(VbS0, Va[13], Vc0[1]);
   Vc0[2] = vec_nmsub(VbS0, Va[14], Vc0[2]);
   Vc0[3] = vec_nmsub(VbS0, Va[15], Vc0[3]);
   Vc1[0] = vec_nmsub(VbS1, Va[12], Vc1[0]);
   Vc1[1] = vec_nmsub(VbS1, Va[13], Vc1[1]);
   Vc1[2] = vec_nmsub(VbS1, Va[14], Vc1[2]);
   Vc1[3] = vec_nmsub(VbS1, Va[15], Vc1[3]);
   Vc2[0] = vec_nmsub(VbS2, Va[12], Vc2[0]);
   Vc2[1] = vec_nmsub(VbS2, Va[13], Vc2[1]);
   Vc2[2] = vec_nmsub(VbS2, Va[14], Vc2[2]);
   Vc2[3] = vec_nmsub(VbS2, Va[15], Vc2[3]);

   VbS0 = vec_splat(Vb[4], 0);
   VbS1 = vec_splat(Vb[4], 1);
   VbS2 = vec_splat(Vb[4], 2);

   Vc2[0] = vec_mul(VbS2, Vc2[0]);
   Vc2[1] = vec_mul(VbS2, Vc2[1]);
   Vc2[2] = vec_mul(VbS2, Vc2[2]);
   Vc2[3] = vec_mul(VbS2, Vc2[3]);
   Va[ 8] = Vc2[0];
   Va[ 9] = Vc2[1];
   Va[10] = Vc2[2];
   Va[11] = Vc2[3];
   Vc0[0] = vec_nmsub(VbS0, Va[ 8], Vc0[0]);
   Vc0[1] = vec_nmsub(VbS0, Va[ 9], Vc0[1]);
   Vc0[2] = vec_nmsub(VbS0, Va[10], Vc0[2]);
   Vc0[3] = vec_nmsub(VbS0, Va[11], Vc0[3]);
   Vc1[0] = vec_nmsub(VbS1, Va[ 8], Vc1[0]);
   Vc1[1] = vec_nmsub(VbS1, Va[ 9], Vc1[1]);
   Vc1[2] = vec_nmsub(VbS1, Va[10], Vc1[2]);
   Vc1[3] = vec_nmsub(VbS1, Va[11], Vc1[3]);

   VbS0 = vec_splat(Vb[2], 0);
   VbS1 = vec_splat(Vb[2], 1);

   Vc1[0] = vec_mul(VbS1, Vc1[0]);
   Vc1[1] = vec_mul(VbS1, Vc1[1]);
   Vc1[2] = vec_mul(VbS1, Vc1[2]);
   Vc1[3] = vec_mul(VbS1, Vc1[3]);
   Va[4] = Vc1[0];
   Va[5] = Vc1[1];
   Va[6] = Vc1[2];
   Va[7] = Vc1[3];
   Vc0[0] = vec_nmsub(VbS0, Va[4], Vc0[0]);
   Vc0[1] = vec_nmsub(VbS0, Va[5], Vc0[1]);
   Vc0[2] = vec_nmsub(VbS0, Va[6], Vc0[2]);
   Vc0[3] = vec_nmsub(VbS0, Va[7], Vc0[3]);

   VbS0 = vec_splat(Vb[0], 0);

   Vc0[0] = vec_mul(VbS0, Vc0[0]);
   Vc0[1] = vec_mul(VbS0, Vc0[1]);
   Vc0[2] = vec_mul(VbS0, Vc0[2]);
   Vc0[3] = vec_mul(VbS0, Vc0[3]);
   Va[0] = Vc0[0];
   Va[1] = Vc0[1];
   Va[2] = Vc0[2];
   Va[3] = Vc0[3];
}

#endif

static inline __attribute__ ((always_inline)) void solve(BLASLONG m, BLASLONG n, FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc) {

  FLOAT aa,  bb;

  int i, j, k;

  a += (n - 1) * m;
  b += (n - 1) * n;

  for (i = n - 1; i >= 0; i--) {

    bb = *(b + i);

    for (j = 0; j < m; j ++) {
      aa = *(c + j + i * ldc);
      aa *= bb;
      *a   = aa;
      *(c + j + i * ldc) = aa;
      a ++;

      for (k = 0; k < i; k ++){
	*(c + j + k * ldc) -= aa * *(b + k);
      }

    }
    b -= n;
    a -= 2 * m;
  }

}

#else

static inline __attribute__ ((always_inline)) void solve(BLASLONG m, BLASLONG n, FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc) {

  FLOAT aa1, aa2;
  FLOAT bb1, bb2;
  FLOAT cc1, cc2;

  int i, j, k;

  ldc *= 2;

  a += (n - 1) * m * 2;
  b += (n - 1) * n * 2;

  for (i = n - 1; i >= 0; i--) {

    bb1 = *(b + i * 2 + 0);
    bb2 = *(b + i * 2 + 1);

    for (j = 0; j < m; j ++) {

      aa1 = *(c + j * 2 + 0 + i * ldc);
      aa2 = *(c + j * 2 + 1 + i * ldc);

#ifndef CONJ
      cc1 = aa1 * bb1 - aa2 * bb2;
      cc2 = aa1 * bb2 + aa2 * bb1;
#else
      cc1 =  aa1 * bb1  + aa2 * bb2;
      cc2 = - aa1 * bb2 + aa2 * bb1;
#endif

      *(a + 0) = cc1;
      *(a + 1) = cc2;

      *(c + j * 2 + 0 + i * ldc) = cc1;
      *(c + j * 2 + 1 + i * ldc) = cc2;
      a += 2;

      for (k = 0; k < i; k ++){
#ifndef CONJ
	*(c + j * 2 + 0 + k * ldc) -= cc1 * *(b + k * 2 + 0) - cc2 * *(b + k * 2 + 1);
	*(c + j * 2 + 1 + k * ldc) -= cc1 * *(b + k * 2 + 1) + cc2 * *(b + k * 2 + 0);
#else
	*(c + j * 2 + 0 + k * ldc) -=   cc1 * *(b + k * 2 + 0) + cc2 * *(b + k * 2 + 1);
	*(c + j * 2 + 1 + k * ldc) -=  -cc1 * *(b + k * 2 + 1) + cc2 * *(b + k * 2 + 0);
#endif
      }

    }
    b -= n * 2;
    a -= 4 * m;
  }

}

#endif

int CNAME(BLASLONG m, BLASLONG n, BLASLONG k,  FLOAT dummy1,
#ifdef COMPLEX
	   FLOAT dummy2,
#endif
	   FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG offset){

  BLASLONG i, j;
  FLOAT *aa, *cc;
  BLASLONG  kk;

#if 0
  fprintf(stderr, "TRSM RT KERNEL m = %3ld  n = %3ld  k = %3ld offset = %3ld\n",
	  m, n, k, offset);
#endif

#ifdef DOUBLE
  int well_aligned = (GEMM_UNROLL_M==8) && (GEMM_UNROLL_N==8) && ((((unsigned long) a) & 0x7) == 0);
#else
  int well_aligned = (GEMM_UNROLL_M==16) && (GEMM_UNROLL_N==8) && ((((unsigned long) a) & 0x7) == 0);
#endif

  kk = n - offset;
  c += n * ldc * COMPSIZE;
  b += n * k   * COMPSIZE;

  if (n & (GEMM_UNROLL_N - 1)) {

    j = 1;
    while (j < GEMM_UNROLL_N) {
      if (n & j) {

	aa  = a;
	b -= j * k  * COMPSIZE;
	c -= j * ldc* COMPSIZE;
	cc  = c;

	i = (m >> GEMM_UNROLL_M_SHIFT);
	if (i > 0) {

	  do {
	    if (k - kk > 0) {
	      GEMM_KERNEL(GEMM_UNROLL_M, j, k - kk, dm1,
#ifdef COMPLEX
			  ZERO,
#endif
			  aa + GEMM_UNROLL_M * kk * COMPSIZE,
			  b  +  j            * kk * COMPSIZE,
			  cc,
			  ldc);
	    }

	    solve(GEMM_UNROLL_M, j,
		  aa + (kk - j) * GEMM_UNROLL_M * COMPSIZE,
		  b  + (kk - j) * j             * COMPSIZE,
		  cc, ldc);

	    aa += GEMM_UNROLL_M * k * COMPSIZE;
	    cc += GEMM_UNROLL_M     * COMPSIZE;
	    i --;
	  } while (i > 0);
	}

	if (m & (GEMM_UNROLL_M - 1)) {
	  i = (GEMM_UNROLL_M >> 1);
	  do {
	    if (m & i) {

	      if (k - kk > 0) {
		GEMM_KERNEL(i, j, k - kk, dm1,
#ifdef COMPLEX
			    ZERO,
#endif
			    aa + i * kk * COMPSIZE,
			    b  + j * kk * COMPSIZE,
			    cc, ldc);
	      }

	      solve(i, j,
		    aa + (kk - j) * i * COMPSIZE,
		    b  + (kk - j) * j * COMPSIZE,
		    cc, ldc);

	      aa += i * k * COMPSIZE;
	      cc += i     * COMPSIZE;

	    }
	    i >>= 1;
	  } while (i > 0);
	}
	kk -= j;
      }
      j <<= 1;
    }
  }

  j = (n >> GEMM_UNROLL_N_SHIFT);

  if (j > 0) {

    do {
      aa  = a;
      b -= GEMM_UNROLL_N * k   * COMPSIZE;
      c -= GEMM_UNROLL_N * ldc * COMPSIZE;
      cc  = c;

      i = (m >> GEMM_UNROLL_M_SHIFT);
      if (i > 0) {
	do {
	  if (k - kk > 0) {
	    GEMM_KERNEL(GEMM_UNROLL_M, GEMM_UNROLL_N, k - kk, dm1,
#ifdef COMPLEX
			ZERO,
#endif
			aa + GEMM_UNROLL_M * kk * COMPSIZE,
			b  + GEMM_UNROLL_N * kk * COMPSIZE,
			cc,
			ldc);
	  }

	  if (well_aligned) { 
#ifdef DOUBLE
	  solve8x8(aa + (kk - GEMM_UNROLL_N) * GEMM_UNROLL_M * COMPSIZE,
		   b  + (kk - GEMM_UNROLL_N) * GEMM_UNROLL_N * COMPSIZE, cc, ldc);
#else
	  solve16x8(aa + (kk - GEMM_UNROLL_N) * GEMM_UNROLL_M * COMPSIZE,
		   b  + (kk - GEMM_UNROLL_N) * GEMM_UNROLL_N * COMPSIZE, cc, ldc);
#endif
	  }
	  else {
	  solve(GEMM_UNROLL_M, GEMM_UNROLL_N,
		aa + (kk - GEMM_UNROLL_N) * GEMM_UNROLL_M * COMPSIZE,
		b  + (kk - GEMM_UNROLL_N) * GEMM_UNROLL_N * COMPSIZE,
		cc, ldc);
	  }

	  aa += GEMM_UNROLL_M * k * COMPSIZE;
	  cc += GEMM_UNROLL_M     * COMPSIZE;
	  i --;
	} while (i > 0);
      }

      if (m & (GEMM_UNROLL_M - 1)) {
	i = (GEMM_UNROLL_M >> 1);
	do {
	  if (m & i) {
	    if (k - kk > 0) {
	      GEMM_KERNEL(i, GEMM_UNROLL_N, k - kk, dm1,
#ifdef COMPLEX
			  ZERO,
#endif
			  aa + i             * kk * COMPSIZE,
			  b  + GEMM_UNROLL_N * kk * COMPSIZE,
			  cc,
			  ldc);
	    }

	    solve(i, GEMM_UNROLL_N,
		  aa + (kk - GEMM_UNROLL_N) * i             * COMPSIZE,
		  b  + (kk - GEMM_UNROLL_N) * GEMM_UNROLL_N * COMPSIZE,
		  cc, ldc);

	    aa += i * k * COMPSIZE;
	    cc += i     * COMPSIZE;
	  }
	  i >>= 1;
	} while (i > 0);
      }

      kk -= GEMM_UNROLL_N;
      j --;
    } while (j > 0);
  }

  return 0;
}


