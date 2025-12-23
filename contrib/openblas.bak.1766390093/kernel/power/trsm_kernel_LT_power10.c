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
#define GEMM_KERNEL   GEMM_KERNEL_L
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

   b[0] = (c0[0] *= a[0]);
   b[1] = (c1[0] *= a[0]);
   b[2] = (c2[0] *= a[0]);
   b[3] = (c3[0] *= a[0]);
   b[4] = (c4[0] *= a[0]);
   b[5] = (c5[0] *= a[0]);
   b[6] = (c6[0] *= a[0]);
   b[7] = (c7[0] *= a[0]);
   VbS0 = vec_splat(Vb[0], 0);
   VbS1 = vec_splat(Vb[0], 1);
   VbS2 = vec_splat(Vb[1], 0);
   VbS3 = vec_splat(Vb[1], 1);
   VbS4 = vec_splat(Vb[2], 0);
   VbS5 = vec_splat(Vb[2], 1);
   VbS6 = vec_splat(Vb[3], 0);
   VbS7 = vec_splat(Vb[3], 1);
   Vc0[1] = vec_nmsub(VbS0, Va[1], Vc0[1]);
   Vc0[2] = vec_nmsub(VbS0, Va[2], Vc0[2]);
   Vc0[3] = vec_nmsub(VbS0, Va[3], Vc0[3]);
   Vc1[1] = vec_nmsub(VbS1, Va[1], Vc1[1]);
   Vc1[2] = vec_nmsub(VbS1, Va[2], Vc1[2]);
   Vc1[3] = vec_nmsub(VbS1, Va[3], Vc1[3]);
   Vc2[1] = vec_nmsub(VbS2, Va[1], Vc2[1]);
   Vc2[2] = vec_nmsub(VbS2, Va[2], Vc2[2]);
   Vc2[3] = vec_nmsub(VbS2, Va[3], Vc2[3]);
   Vc3[1] = vec_nmsub(VbS3, Va[1], Vc3[1]);
   Vc3[2] = vec_nmsub(VbS3, Va[2], Vc3[2]);
   Vc3[3] = vec_nmsub(VbS3, Va[3], Vc3[3]);
   Vc4[1] = vec_nmsub(VbS4, Va[1], Vc4[1]);
   Vc4[2] = vec_nmsub(VbS4, Va[2], Vc4[2]);
   Vc4[3] = vec_nmsub(VbS4, Va[3], Vc4[3]);
   Vc5[1] = vec_nmsub(VbS5, Va[1], Vc5[1]);
   Vc5[2] = vec_nmsub(VbS5, Va[2], Vc5[2]);
   Vc5[3] = vec_nmsub(VbS5, Va[3], Vc5[3]);
   Vc6[1] = vec_nmsub(VbS6, Va[1], Vc6[1]);
   Vc6[2] = vec_nmsub(VbS6, Va[2], Vc6[2]);
   Vc6[3] = vec_nmsub(VbS6, Va[3], Vc6[3]);
   Vc7[1] = vec_nmsub(VbS7, Va[1], Vc7[1]);
   Vc7[2] = vec_nmsub(VbS7, Va[2], Vc7[2]);
   Vc7[3] = vec_nmsub(VbS7, Va[3], Vc7[3]);
   c0[1] -= c0[0] * a[1];
   c1[1] -= c1[0] * a[1];
   c2[1] -= c2[0] * a[1];
   c3[1] -= c3[0] * a[1];
   c4[1] -= c4[0] * a[1];
   c5[1] -= c5[0] * a[1];
   c6[1] -= c6[0] * a[1];
   c7[1] -= c7[0] * a[1];

   b[ 8] = (c0[1] *= a[9]);
   b[ 9] = (c1[1] *= a[9]);
   b[10] = (c2[1] *= a[9]);
   b[11] = (c3[1] *= a[9]);
   b[12] = (c4[1] *= a[9]);
   b[13] = (c5[1] *= a[9]);
   b[14] = (c6[1] *= a[9]);
   b[15] = (c7[1] *= a[9]);
   VbS0 = vec_splat(Vb[4], 0);
   VbS1 = vec_splat(Vb[4], 1);
   VbS2 = vec_splat(Vb[5], 0);
   VbS3 = vec_splat(Vb[5], 1);
   VbS4 = vec_splat(Vb[6], 0);
   VbS5 = vec_splat(Vb[6], 1);
   VbS6 = vec_splat(Vb[7], 0);
   VbS7 = vec_splat(Vb[7], 1);
   Vc0[1] = vec_nmsub(VbS0, Va[5], Vc0[1]);
   Vc0[2] = vec_nmsub(VbS0, Va[6], Vc0[2]);
   Vc0[3] = vec_nmsub(VbS0, Va[7], Vc0[3]);
   Vc1[1] = vec_nmsub(VbS1, Va[5], Vc1[1]);
   Vc1[2] = vec_nmsub(VbS1, Va[6], Vc1[2]);
   Vc1[3] = vec_nmsub(VbS1, Va[7], Vc1[3]);
   Vc2[1] = vec_nmsub(VbS2, Va[5], Vc2[1]);
   Vc2[2] = vec_nmsub(VbS2, Va[6], Vc2[2]);
   Vc2[3] = vec_nmsub(VbS2, Va[7], Vc2[3]);
   Vc3[1] = vec_nmsub(VbS3, Va[5], Vc3[1]);
   Vc3[2] = vec_nmsub(VbS3, Va[6], Vc3[2]);
   Vc3[3] = vec_nmsub(VbS3, Va[7], Vc3[3]);
   Vc4[1] = vec_nmsub(VbS4, Va[5], Vc4[1]);
   Vc4[2] = vec_nmsub(VbS4, Va[6], Vc4[2]);
   Vc4[3] = vec_nmsub(VbS4, Va[7], Vc4[3]);
   Vc5[1] = vec_nmsub(VbS5, Va[5], Vc5[1]);
   Vc5[2] = vec_nmsub(VbS5, Va[6], Vc5[2]);
   Vc5[3] = vec_nmsub(VbS5, Va[7], Vc5[3]);
   Vc6[1] = vec_nmsub(VbS6, Va[5], Vc6[1]);
   Vc6[2] = vec_nmsub(VbS6, Va[6], Vc6[2]);
   Vc6[3] = vec_nmsub(VbS6, Va[7], Vc6[3]);
   Vc7[1] = vec_nmsub(VbS7, Va[5], Vc7[1]);
   Vc7[2] = vec_nmsub(VbS7, Va[6], Vc7[2]);
   Vc7[3] = vec_nmsub(VbS7, Va[7], Vc7[3]);

   b[16] = (c0[2] *= a[18]);
   b[17] = (c1[2] *= a[18]);
   b[18] = (c2[2] *= a[18]);
   b[19] = (c3[2] *= a[18]);
   b[20] = (c4[2] *= a[18]);
   b[21] = (c5[2] *= a[18]);
   b[22] = (c6[2] *= a[18]);
   b[23] = (c7[2] *= a[18]);
   VbS0 = vec_splat(Vb[ 8], 0);
   VbS1 = vec_splat(Vb[ 8], 1);
   VbS2 = vec_splat(Vb[ 9], 0);
   VbS3 = vec_splat(Vb[ 9], 1);
   VbS4 = vec_splat(Vb[10], 0);
   VbS5 = vec_splat(Vb[10], 1);
   VbS6 = vec_splat(Vb[11], 0);
   VbS7 = vec_splat(Vb[11], 1);
   Vc0[2] = vec_nmsub(VbS0, Va[10], Vc0[2]);
   Vc0[3] = vec_nmsub(VbS0, Va[11], Vc0[3]);
   Vc1[2] = vec_nmsub(VbS1, Va[10], Vc1[2]);
   Vc1[3] = vec_nmsub(VbS1, Va[11], Vc1[3]);
   Vc2[2] = vec_nmsub(VbS2, Va[10], Vc2[2]);
   Vc2[3] = vec_nmsub(VbS2, Va[11], Vc2[3]);
   Vc3[2] = vec_nmsub(VbS3, Va[10], Vc3[2]);
   Vc3[3] = vec_nmsub(VbS3, Va[11], Vc3[3]);
   Vc4[2] = vec_nmsub(VbS4, Va[10], Vc4[2]);
   Vc4[3] = vec_nmsub(VbS4, Va[11], Vc4[3]);
   Vc5[2] = vec_nmsub(VbS5, Va[10], Vc5[2]);
   Vc5[3] = vec_nmsub(VbS5, Va[11], Vc5[3]);
   Vc6[2] = vec_nmsub(VbS6, Va[10], Vc6[2]);
   Vc6[3] = vec_nmsub(VbS6, Va[11], Vc6[3]);
   Vc7[2] = vec_nmsub(VbS7, Va[10], Vc7[2]);
   Vc7[3] = vec_nmsub(VbS7, Va[11], Vc7[3]);
   c0[3] -= c0[2] * a[19];
   c1[3] -= c1[2] * a[19];
   c2[3] -= c2[2] * a[19];
   c3[3] -= c3[2] * a[19];
   c4[3] -= c4[2] * a[19];
   c5[3] -= c5[2] * a[19];
   c6[3] -= c6[2] * a[19];
   c7[3] -= c7[2] * a[19];

   b[24] = (c0[3] *= a[27]);
   b[25] = (c1[3] *= a[27]);
   b[26] = (c2[3] *= a[27]);
   b[27] = (c3[3] *= a[27]);
   b[28] = (c4[3] *= a[27]);
   b[29] = (c5[3] *= a[27]);
   b[30] = (c6[3] *= a[27]);
   b[31] = (c7[3] *= a[27]);
   VbS0 = vec_splat(Vb[12], 0);
   VbS1 = vec_splat(Vb[12], 1);
   VbS2 = vec_splat(Vb[13], 0);
   VbS3 = vec_splat(Vb[13], 1);
   VbS4 = vec_splat(Vb[14], 0);
   VbS5 = vec_splat(Vb[14], 1);
   VbS6 = vec_splat(Vb[15], 0);
   VbS7 = vec_splat(Vb[15], 1);
   Vc0[2] = vec_nmsub(VbS0, Va[14], Vc0[2]);
   Vc0[3] = vec_nmsub(VbS0, Va[15], Vc0[3]);
   Vc1[2] = vec_nmsub(VbS1, Va[14], Vc1[2]);
   Vc1[3] = vec_nmsub(VbS1, Va[15], Vc1[3]);
   Vc2[2] = vec_nmsub(VbS2, Va[14], Vc2[2]);
   Vc2[3] = vec_nmsub(VbS2, Va[15], Vc2[3]);
   Vc3[2] = vec_nmsub(VbS3, Va[14], Vc3[2]);
   Vc3[3] = vec_nmsub(VbS3, Va[15], Vc3[3]);
   Vc4[2] = vec_nmsub(VbS4, Va[14], Vc4[2]);
   Vc4[3] = vec_nmsub(VbS4, Va[15], Vc4[3]);
   Vc5[2] = vec_nmsub(VbS5, Va[14], Vc5[2]);
   Vc5[3] = vec_nmsub(VbS5, Va[15], Vc5[3]);
   Vc6[2] = vec_nmsub(VbS6, Va[14], Vc6[2]);
   Vc6[3] = vec_nmsub(VbS6, Va[15], Vc6[3]);
   Vc7[2] = vec_nmsub(VbS7, Va[14], Vc7[2]);
   Vc7[3] = vec_nmsub(VbS7, Va[15], Vc7[3]);

   b[32] = (c0[4] *= a[36]);
   b[33] = (c1[4] *= a[36]);
   b[34] = (c2[4] *= a[36]);
   b[35] = (c3[4] *= a[36]);
   b[36] = (c4[4] *= a[36]);
   b[37] = (c5[4] *= a[36]);
   b[38] = (c6[4] *= a[36]);
   b[39] = (c7[4] *= a[36]);
   VbS0 = vec_splat(Vb[16], 0);
   VbS1 = vec_splat(Vb[16], 1);
   VbS2 = vec_splat(Vb[17], 0);
   VbS3 = vec_splat(Vb[17], 1);
   VbS4 = vec_splat(Vb[18], 0);
   VbS5 = vec_splat(Vb[18], 1);
   VbS6 = vec_splat(Vb[19], 0);
   VbS7 = vec_splat(Vb[19], 1);
   Vc0[3] = vec_nmsub(VbS0, Va[19], Vc0[3]);
   Vc1[3] = vec_nmsub(VbS1, Va[19], Vc1[3]);
   Vc2[3] = vec_nmsub(VbS2, Va[19], Vc2[3]);
   Vc3[3] = vec_nmsub(VbS3, Va[19], Vc3[3]);
   Vc4[3] = vec_nmsub(VbS4, Va[19], Vc4[3]);
   Vc5[3] = vec_nmsub(VbS5, Va[19], Vc5[3]);
   Vc6[3] = vec_nmsub(VbS6, Va[19], Vc6[3]);
   Vc7[3] = vec_nmsub(VbS7, Va[19], Vc7[3]);
   c0[5] -= c0[4] * a[37];
   c1[5] -= c1[4] * a[37];
   c2[5] -= c2[4] * a[37];
   c3[5] -= c3[4] * a[37];
   c4[5] -= c4[4] * a[37];
   c5[5] -= c5[4] * a[37];
   c6[5] -= c6[4] * a[37];
   c7[5] -= c7[4] * a[37];

   b[40] = (c0[5] *= a[45]);
   b[41] = (c1[5] *= a[45]);
   b[42] = (c2[5] *= a[45]);
   b[43] = (c3[5] *= a[45]);
   b[44] = (c4[5] *= a[45]);
   b[45] = (c5[5] *= a[45]);
   b[46] = (c6[5] *= a[45]);
   b[47] = (c7[5] *= a[45]);
   VbS0 = vec_splat(Vb[20], 0);
   VbS1 = vec_splat(Vb[20], 1);
   VbS2 = vec_splat(Vb[21], 0);
   VbS3 = vec_splat(Vb[21], 1);
   VbS4 = vec_splat(Vb[22], 0);
   VbS5 = vec_splat(Vb[22], 1);
   VbS6 = vec_splat(Vb[23], 0);
   VbS7 = vec_splat(Vb[23], 1);
   Vc0[3] = vec_nmsub(VbS0, Va[23], Vc0[3]);
   Vc1[3] = vec_nmsub(VbS1, Va[23], Vc1[3]);
   Vc2[3] = vec_nmsub(VbS2, Va[23], Vc2[3]);
   Vc3[3] = vec_nmsub(VbS3, Va[23], Vc3[3]);
   Vc4[3] = vec_nmsub(VbS4, Va[23], Vc4[3]);
   Vc5[3] = vec_nmsub(VbS5, Va[23], Vc5[3]);
   Vc6[3] = vec_nmsub(VbS6, Va[23], Vc6[3]);
   Vc7[3] = vec_nmsub(VbS7, Va[23], Vc7[3]);

   b[48] = (c0[6] *= a[54]);
   b[49] = (c1[6] *= a[54]);
   b[50] = (c2[6] *= a[54]);
   b[51] = (c3[6] *= a[54]);
   b[52] = (c4[6] *= a[54]);
   b[53] = (c5[6] *= a[54]);
   b[54] = (c6[6] *= a[54]);
   b[55] = (c7[6] *= a[54]);
   c0[7] -= c0[6] * a[55];
   c1[7] -= c1[6] * a[55];
   c2[7] -= c2[6] * a[55];
   c3[7] -= c3[6] * a[55];
   c4[7] -= c4[6] * a[55];
   c5[7] -= c5[6] * a[55];
   c6[7] -= c6[6] * a[55];
   c7[7] -= c7[6] * a[55];

   b[56] = (c0[7] *= a[63]);
   b[57] = (c1[7] *= a[63]);
   b[58] = (c2[7] *= a[63]);
   b[59] = (c3[7] *= a[63]);
   b[60] = (c4[7] *= a[63]);
   b[61] = (c5[7] *= a[63]);
   b[62] = (c6[7] *= a[63]);
   b[63] = (c7[7] *= a[63]);
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

   b[0] = (c0[0] *= a[0]);
   b[1] = (c1[0] *= a[0]);
   b[2] = (c2[0] *= a[0]);
   b[3] = (c3[0] *= a[0]);
   b[4] = (c4[0] *= a[0]);
   b[5] = (c5[0] *= a[0]);
   b[6] = (c6[0] *= a[0]);
   b[7] = (c7[0] *= a[0]);
   VbS0 = vec_splat(Vb[0], 0);
   VbS1 = vec_splat(Vb[0], 1);
   VbS2 = vec_splat(Vb[0], 2);
   VbS3 = vec_splat(Vb[0], 3);
   VbS4 = vec_splat(Vb[1], 0);
   VbS5 = vec_splat(Vb[1], 1);
   VbS6 = vec_splat(Vb[1], 2);
   VbS7 = vec_splat(Vb[1], 3);
   Vc0[1] = vec_nmsub(VbS0, Va[1], Vc0[1]);
   Vc0[2] = vec_nmsub(VbS0, Va[2], Vc0[2]);
   Vc0[3] = vec_nmsub(VbS0, Va[3], Vc0[3]);
   Vc1[1] = vec_nmsub(VbS1, Va[1], Vc1[1]);
   Vc1[2] = vec_nmsub(VbS1, Va[2], Vc1[2]);
   Vc1[3] = vec_nmsub(VbS1, Va[3], Vc1[3]);
   Vc2[1] = vec_nmsub(VbS2, Va[1], Vc2[1]);
   Vc2[2] = vec_nmsub(VbS2, Va[2], Vc2[2]);
   Vc2[3] = vec_nmsub(VbS2, Va[3], Vc2[3]);
   Vc3[1] = vec_nmsub(VbS3, Va[1], Vc3[1]);
   Vc3[2] = vec_nmsub(VbS3, Va[2], Vc3[2]);
   Vc3[3] = vec_nmsub(VbS3, Va[3], Vc3[3]);
   Vc4[1] = vec_nmsub(VbS4, Va[1], Vc4[1]);
   Vc4[2] = vec_nmsub(VbS4, Va[2], Vc4[2]);
   Vc4[3] = vec_nmsub(VbS4, Va[3], Vc4[3]);
   Vc5[1] = vec_nmsub(VbS5, Va[1], Vc5[1]);
   Vc5[2] = vec_nmsub(VbS5, Va[2], Vc5[2]);
   Vc5[3] = vec_nmsub(VbS5, Va[3], Vc5[3]);
   Vc6[1] = vec_nmsub(VbS6, Va[1], Vc6[1]);
   Vc6[2] = vec_nmsub(VbS6, Va[2], Vc6[2]);
   Vc6[3] = vec_nmsub(VbS6, Va[3], Vc6[3]);
   Vc7[1] = vec_nmsub(VbS7, Va[1], Vc7[1]);
   Vc7[2] = vec_nmsub(VbS7, Va[2], Vc7[2]);
   Vc7[3] = vec_nmsub(VbS7, Va[3], Vc7[3]);
   c0[1] -= b[0] * a[ 1];
   c0[2] -= b[0] * a[ 2];
   c0[3] -= b[0] * a[ 3];
   c1[1] -= b[1] * a[ 1];
   c1[2] -= b[1] * a[ 2];
   c1[3] -= b[1] * a[ 3];
   c2[1] -= b[2] * a[ 1];
   c2[2] -= b[2] * a[ 2];
   c2[3] -= b[2] * a[ 3];
   c3[1] -= b[3] * a[ 1];
   c3[2] -= b[3] * a[ 2];
   c3[3] -= b[3] * a[ 3];
   c4[1] -= b[4] * a[ 1];
   c4[2] -= b[4] * a[ 2];
   c4[3] -= b[4] * a[ 3];
   c5[1] -= b[5] * a[ 1];
   c5[2] -= b[5] * a[ 2];
   c5[3] -= b[5] * a[ 3];
   c6[1] -= b[6] * a[ 1];
   c6[2] -= b[6] * a[ 2];
   c6[3] -= b[6] * a[ 3];
   c7[1] -= b[7] * a[ 1];
   c7[2] -= b[7] * a[ 2];
   c7[3] -= b[7] * a[ 3];
 
   b[ 8] = (c0[1] *= a[17]);
   b[ 9] = (c1[1] *= a[17]);
   b[10] = (c2[1] *= a[17]);
   b[11] = (c3[1] *= a[17]);
   b[12] = (c4[1] *= a[17]);
   b[13] = (c5[1] *= a[17]);
   b[14] = (c6[1] *= a[17]);
   b[15] = (c7[1] *= a[17]);
   VbS0 = vec_splat(Vb[2], 0);
   VbS1 = vec_splat(Vb[2], 1);
   VbS2 = vec_splat(Vb[2], 2);
   VbS3 = vec_splat(Vb[2], 3);
   VbS4 = vec_splat(Vb[3], 0);
   VbS5 = vec_splat(Vb[3], 1);
   VbS6 = vec_splat(Vb[3], 2);
   VbS7 = vec_splat(Vb[3], 3);
   Vc0[1] = vec_nmsub(VbS0, Va[5], Vc0[1]);
   Vc0[2] = vec_nmsub(VbS0, Va[6], Vc0[2]);
   Vc0[3] = vec_nmsub(VbS0, Va[7], Vc0[3]);
   Vc1[1] = vec_nmsub(VbS1, Va[5], Vc1[1]);
   Vc1[2] = vec_nmsub(VbS1, Va[6], Vc1[2]);
   Vc1[3] = vec_nmsub(VbS1, Va[7], Vc1[3]);
   Vc2[1] = vec_nmsub(VbS2, Va[5], Vc2[1]);
   Vc2[2] = vec_nmsub(VbS2, Va[6], Vc2[2]);
   Vc2[3] = vec_nmsub(VbS2, Va[7], Vc2[3]);
   Vc3[1] = vec_nmsub(VbS3, Va[5], Vc3[1]);
   Vc3[2] = vec_nmsub(VbS3, Va[6], Vc3[2]);
   Vc3[3] = vec_nmsub(VbS3, Va[7], Vc3[3]);
   Vc4[1] = vec_nmsub(VbS4, Va[5], Vc4[1]);
   Vc4[2] = vec_nmsub(VbS4, Va[6], Vc4[2]);
   Vc4[3] = vec_nmsub(VbS4, Va[7], Vc4[3]);
   Vc5[1] = vec_nmsub(VbS5, Va[5], Vc5[1]);
   Vc5[2] = vec_nmsub(VbS5, Va[6], Vc5[2]);
   Vc5[3] = vec_nmsub(VbS5, Va[7], Vc5[3]);
   Vc6[1] = vec_nmsub(VbS6, Va[5], Vc6[1]);
   Vc6[2] = vec_nmsub(VbS6, Va[6], Vc6[2]);
   Vc6[3] = vec_nmsub(VbS6, Va[7], Vc6[3]);
   Vc7[1] = vec_nmsub(VbS7, Va[5], Vc7[1]);
   Vc7[2] = vec_nmsub(VbS7, Va[6], Vc7[2]);
   Vc7[3] = vec_nmsub(VbS7, Va[7], Vc7[3]);
   c0[2] -= b[ 8] * a[18];
   c0[3] -= b[ 8] * a[19];
   c1[2] -= b[ 9] * a[18];
   c1[3] -= b[ 9] * a[19];
   c2[2] -= b[10] * a[18];
   c2[3] -= b[10] * a[19];
   c3[2] -= b[11] * a[18];
   c3[3] -= b[11] * a[19];
   c4[2] -= b[12] * a[18];
   c4[3] -= b[12] * a[19];
   c5[2] -= b[13] * a[18];
   c5[3] -= b[13] * a[19];
   c6[2] -= b[14] * a[18];
   c6[3] -= b[14] * a[19];
   c7[2] -= b[15] * a[18];
   c7[3] -= b[15] * a[19];

   b[16] = (c0[2] *= a[34]);
   b[17] = (c1[2] *= a[34]);
   b[18] = (c2[2] *= a[34]);
   b[19] = (c3[2] *= a[34]);
   b[20] = (c4[2] *= a[34]);
   b[21] = (c5[2] *= a[34]);
   b[22] = (c6[2] *= a[34]);
   b[23] = (c7[2] *= a[34]);
   VbS0 = vec_splat(Vb[4], 0);
   VbS1 = vec_splat(Vb[4], 1);
   VbS2 = vec_splat(Vb[4], 2);
   VbS3 = vec_splat(Vb[4], 3);
   VbS4 = vec_splat(Vb[5], 0);
   VbS5 = vec_splat(Vb[5], 1);
   VbS6 = vec_splat(Vb[5], 2);
   VbS7 = vec_splat(Vb[5], 3);
   Vc0[1] = vec_nmsub(VbS0, Va[ 9], Vc0[1]);
   Vc0[2] = vec_nmsub(VbS0, Va[10], Vc0[2]);
   Vc0[3] = vec_nmsub(VbS0, Va[11], Vc0[3]);
   Vc1[1] = vec_nmsub(VbS1, Va[ 9], Vc1[1]);
   Vc1[2] = vec_nmsub(VbS1, Va[10], Vc1[2]);
   Vc1[3] = vec_nmsub(VbS1, Va[11], Vc1[3]);
   Vc2[1] = vec_nmsub(VbS2, Va[ 9], Vc2[1]);
   Vc2[2] = vec_nmsub(VbS2, Va[10], Vc2[2]);
   Vc2[3] = vec_nmsub(VbS2, Va[11], Vc2[3]);
   Vc3[1] = vec_nmsub(VbS3, Va[ 9], Vc3[1]);
   Vc3[2] = vec_nmsub(VbS3, Va[10], Vc3[2]);
   Vc3[3] = vec_nmsub(VbS3, Va[11], Vc3[3]);
   Vc4[1] = vec_nmsub(VbS4, Va[ 9], Vc4[1]);
   Vc4[2] = vec_nmsub(VbS4, Va[10], Vc4[2]);
   Vc4[3] = vec_nmsub(VbS4, Va[11], Vc4[3]);
   Vc5[1] = vec_nmsub(VbS5, Va[ 9], Vc5[1]);
   Vc5[2] = vec_nmsub(VbS5, Va[10], Vc5[2]);
   Vc5[3] = vec_nmsub(VbS5, Va[11], Vc5[3]);
   Vc6[1] = vec_nmsub(VbS6, Va[ 9], Vc6[1]);
   Vc6[2] = vec_nmsub(VbS6, Va[10], Vc6[2]);
   Vc6[3] = vec_nmsub(VbS6, Va[11], Vc6[3]);
   Vc7[1] = vec_nmsub(VbS7, Va[ 9], Vc7[1]);
   Vc7[2] = vec_nmsub(VbS7, Va[10], Vc7[2]);
   Vc7[3] = vec_nmsub(VbS7, Va[11], Vc7[3]);
   c0[3] -= b[16] * a[35];
   c1[3] -= b[17] * a[35];
   c2[3] -= b[18] * a[35];
   c3[3] -= b[19] * a[35];
   c4[3] -= b[20] * a[35];
   c5[3] -= b[21] * a[35];
   c6[3] -= b[22] * a[35];
   c7[3] -= b[23] * a[35];

   b[24] = (c0[3] *= a[51]);
   b[25] = (c1[3] *= a[51]);
   b[26] = (c2[3] *= a[51]);
   b[27] = (c3[3] *= a[51]);
   b[28] = (c4[3] *= a[51]);
   b[29] = (c5[3] *= a[51]);
   b[30] = (c6[3] *= a[51]);
   b[31] = (c7[3] *= a[51]);
   VbS0 = vec_splat(Vb[6], 0);
   VbS1 = vec_splat(Vb[6], 1);
   VbS2 = vec_splat(Vb[6], 2);
   VbS3 = vec_splat(Vb[6], 3);
   VbS4 = vec_splat(Vb[7], 0);
   VbS5 = vec_splat(Vb[7], 1);
   VbS6 = vec_splat(Vb[7], 2);
   VbS7 = vec_splat(Vb[7], 3);
   Vc0[1] = vec_nmsub(VbS0, Va[13], Vc0[1]);
   Vc0[2] = vec_nmsub(VbS0, Va[14], Vc0[2]);
   Vc0[3] = vec_nmsub(VbS0, Va[15], Vc0[3]);
   Vc1[1] = vec_nmsub(VbS1, Va[13], Vc1[1]);
   Vc1[2] = vec_nmsub(VbS1, Va[14], Vc1[2]);
   Vc1[3] = vec_nmsub(VbS1, Va[15], Vc1[3]);
   Vc2[1] = vec_nmsub(VbS2, Va[13], Vc2[1]);
   Vc2[2] = vec_nmsub(VbS2, Va[14], Vc2[2]);
   Vc2[3] = vec_nmsub(VbS2, Va[15], Vc2[3]);
   Vc3[1] = vec_nmsub(VbS3, Va[13], Vc3[1]);
   Vc3[2] = vec_nmsub(VbS3, Va[14], Vc3[2]);
   Vc3[3] = vec_nmsub(VbS3, Va[15], Vc3[3]);
   Vc4[1] = vec_nmsub(VbS4, Va[13], Vc4[1]);
   Vc4[2] = vec_nmsub(VbS4, Va[14], Vc4[2]);
   Vc4[3] = vec_nmsub(VbS4, Va[15], Vc4[3]);
   Vc5[1] = vec_nmsub(VbS5, Va[13], Vc5[1]);
   Vc5[2] = vec_nmsub(VbS5, Va[14], Vc5[2]);
   Vc5[3] = vec_nmsub(VbS5, Va[15], Vc5[3]);
   Vc6[1] = vec_nmsub(VbS6, Va[13], Vc6[1]);
   Vc6[2] = vec_nmsub(VbS6, Va[14], Vc6[2]);
   Vc6[3] = vec_nmsub(VbS6, Va[15], Vc6[3]);
   Vc7[1] = vec_nmsub(VbS7, Va[13], Vc7[1]);
   Vc7[2] = vec_nmsub(VbS7, Va[14], Vc7[2]);
   Vc7[3] = vec_nmsub(VbS7, Va[15], Vc7[3]);

   b[32] = (c0[4] *= a[68]);
   b[33] = (c1[4] *= a[68]);
   b[34] = (c2[4] *= a[68]);
   b[35] = (c3[4] *= a[68]);
   b[36] = (c4[4] *= a[68]);
   b[37] = (c5[4] *= a[68]);
   b[38] = (c6[4] *= a[68]);
   b[39] = (c7[4] *= a[68]);
   VbS0 = vec_splat(Vb[8], 0);
   VbS1 = vec_splat(Vb[8], 1);
   VbS2 = vec_splat(Vb[8], 2);
   VbS3 = vec_splat(Vb[8], 3);
   VbS4 = vec_splat(Vb[9], 0);
   VbS5 = vec_splat(Vb[9], 1);
   VbS6 = vec_splat(Vb[9], 2);
   VbS7 = vec_splat(Vb[9], 3);
   Vc0[2] = vec_nmsub(VbS0, Va[18], Vc0[2]);
   Vc0[3] = vec_nmsub(VbS0, Va[19], Vc0[3]);
   Vc1[2] = vec_nmsub(VbS1, Va[18], Vc1[2]);
   Vc1[3] = vec_nmsub(VbS1, Va[19], Vc1[3]);
   Vc2[2] = vec_nmsub(VbS2, Va[18], Vc2[2]);
   Vc2[3] = vec_nmsub(VbS2, Va[19], Vc2[3]);
   Vc3[2] = vec_nmsub(VbS3, Va[18], Vc3[2]);
   Vc3[3] = vec_nmsub(VbS3, Va[19], Vc3[3]);
   Vc4[2] = vec_nmsub(VbS4, Va[18], Vc4[2]);
   Vc4[3] = vec_nmsub(VbS4, Va[19], Vc4[3]);
   Vc5[2] = vec_nmsub(VbS5, Va[18], Vc5[2]);
   Vc5[3] = vec_nmsub(VbS5, Va[19], Vc5[3]);
   Vc6[2] = vec_nmsub(VbS6, Va[18], Vc6[2]);
   Vc6[3] = vec_nmsub(VbS6, Va[19], Vc6[3]);
   Vc7[2] = vec_nmsub(VbS7, Va[18], Vc7[2]);
   Vc7[3] = vec_nmsub(VbS7, Va[19], Vc7[3]);
   c0[5] -= b[32] * a[69];
   c0[6] -= b[32] * a[70];
   c0[7] -= b[32] * a[71];
   c1[5] -= b[33] * a[69];
   c1[6] -= b[33] * a[70];
   c1[7] -= b[33] * a[71];
   c2[5] -= b[34] * a[69];
   c2[6] -= b[34] * a[70];
   c2[7] -= b[34] * a[71];
   c3[5] -= b[35] * a[69];
   c3[6] -= b[35] * a[70];
   c3[7] -= b[35] * a[71];
   c4[5] -= b[36] * a[69];
   c4[6] -= b[36] * a[70];
   c4[7] -= b[36] * a[71];
   c5[5] -= b[37] * a[69];
   c5[6] -= b[37] * a[70];
   c5[7] -= b[37] * a[71];
   c6[5] -= b[38] * a[69];
   c6[6] -= b[38] * a[70];
   c6[7] -= b[38] * a[71];
   c7[5] -= b[39] * a[69];
   c7[6] -= b[39] * a[70];
   c7[7] -= b[39] * a[71];

   b[40] = (c0[5] *= a[85]);
   b[41] = (c1[5] *= a[85]);
   b[42] = (c2[5] *= a[85]);
   b[43] = (c3[5] *= a[85]);
   b[44] = (c4[5] *= a[85]);
   b[45] = (c5[5] *= a[85]);
   b[46] = (c6[5] *= a[85]);
   b[47] = (c7[5] *= a[85]);
   VbS0 = vec_splat(Vb[10], 0);
   VbS1 = vec_splat(Vb[10], 1);
   VbS2 = vec_splat(Vb[10], 2);
   VbS3 = vec_splat(Vb[10], 3);
   VbS4 = vec_splat(Vb[11], 0);
   VbS5 = vec_splat(Vb[11], 1);
   VbS6 = vec_splat(Vb[11], 2);
   VbS7 = vec_splat(Vb[11], 3);
   Vc0[2] = vec_nmsub(VbS0, Va[22], Vc0[2]);
   Vc0[3] = vec_nmsub(VbS0, Va[23], Vc0[3]);
   Vc1[2] = vec_nmsub(VbS1, Va[22], Vc1[2]);
   Vc1[3] = vec_nmsub(VbS1, Va[23], Vc1[3]);
   Vc2[2] = vec_nmsub(VbS2, Va[22], Vc2[2]);
   Vc2[3] = vec_nmsub(VbS2, Va[23], Vc2[3]);
   Vc3[2] = vec_nmsub(VbS3, Va[22], Vc3[2]);
   Vc3[3] = vec_nmsub(VbS3, Va[23], Vc3[3]);
   Vc4[2] = vec_nmsub(VbS4, Va[22], Vc4[2]);
   Vc4[3] = vec_nmsub(VbS4, Va[23], Vc4[3]);
   Vc5[2] = vec_nmsub(VbS5, Va[22], Vc5[2]);
   Vc5[3] = vec_nmsub(VbS5, Va[23], Vc5[3]);
   Vc6[2] = vec_nmsub(VbS6, Va[22], Vc6[2]);
   Vc6[3] = vec_nmsub(VbS6, Va[23], Vc6[3]);
   Vc7[2] = vec_nmsub(VbS7, Va[22], Vc7[2]);
   Vc7[3] = vec_nmsub(VbS7, Va[23], Vc7[3]);
   c0[6] -= b[40] * a[86];
   c0[7] -= b[40] * a[87];
   c1[6] -= b[41] * a[86];
   c1[7] -= b[41] * a[87];
   c2[6] -= b[42] * a[86];
   c2[7] -= b[42] * a[87];
   c3[6] -= b[43] * a[86];
   c3[7] -= b[43] * a[87];
   c4[6] -= b[44] * a[86];
   c4[7] -= b[44] * a[87];
   c5[6] -= b[45] * a[86];
   c5[7] -= b[45] * a[87];
   c6[6] -= b[46] * a[86];
   c6[7] -= b[46] * a[87];
   c7[6] -= b[47] * a[86];
   c7[7] -= b[47] * a[87];

   b[48] = (c0[6] *= a[102]);
   b[49] = (c1[6] *= a[102]);
   b[50] = (c2[6] *= a[102]);
   b[51] = (c3[6] *= a[102]);
   b[52] = (c4[6] *= a[102]);
   b[53] = (c5[6] *= a[102]);
   b[54] = (c6[6] *= a[102]);
   b[55] = (c7[6] *= a[102]);
   VbS0 = vec_splat(Vb[12], 0);
   VbS1 = vec_splat(Vb[12], 1);
   VbS2 = vec_splat(Vb[12], 2);
   VbS3 = vec_splat(Vb[12], 3);
   VbS4 = vec_splat(Vb[13], 0);
   VbS5 = vec_splat(Vb[13], 1);
   VbS6 = vec_splat(Vb[13], 2);
   VbS7 = vec_splat(Vb[13], 3);
   Vc0[2] = vec_nmsub(VbS0, Va[26], Vc0[2]);
   Vc0[3] = vec_nmsub(VbS0, Va[27], Vc0[3]);
   Vc1[2] = vec_nmsub(VbS1, Va[26], Vc1[2]);
   Vc1[3] = vec_nmsub(VbS1, Va[27], Vc1[3]);
   Vc2[2] = vec_nmsub(VbS2, Va[26], Vc2[2]);
   Vc2[3] = vec_nmsub(VbS2, Va[27], Vc2[3]);
   Vc3[2] = vec_nmsub(VbS3, Va[26], Vc3[2]);
   Vc3[3] = vec_nmsub(VbS3, Va[27], Vc3[3]);
   Vc4[2] = vec_nmsub(VbS4, Va[26], Vc4[2]);
   Vc4[3] = vec_nmsub(VbS4, Va[27], Vc4[3]);
   Vc5[2] = vec_nmsub(VbS5, Va[26], Vc5[2]);
   Vc5[3] = vec_nmsub(VbS5, Va[27], Vc5[3]);
   Vc6[2] = vec_nmsub(VbS6, Va[26], Vc6[2]);
   Vc6[3] = vec_nmsub(VbS6, Va[27], Vc6[3]);
   Vc7[2] = vec_nmsub(VbS7, Va[26], Vc7[2]);
   Vc7[3] = vec_nmsub(VbS7, Va[27], Vc7[3]);
   c0[7] -= b[48] * a[103];
   c1[7] -= b[49] * a[103];
   c2[7] -= b[50] * a[103];
   c3[7] -= b[51] * a[103];
   c4[7] -= b[52] * a[103];
   c5[7] -= b[53] * a[103];
   c6[7] -= b[54] * a[103];
   c7[7] -= b[55] * a[103];

   b[56] = (c0[7] *= a[119]);
   b[57] = (c1[7] *= a[119]);
   b[58] = (c2[7] *= a[119]);
   b[59] = (c3[7] *= a[119]);
   b[60] = (c4[7] *= a[119]);
   b[61] = (c5[7] *= a[119]);
   b[62] = (c6[7] *= a[119]);
   b[63] = (c7[7] *= a[119]);
   VbS0 = vec_splat(Vb[14], 0);
   VbS1 = vec_splat(Vb[14], 1);
   VbS2 = vec_splat(Vb[14], 2);
   VbS3 = vec_splat(Vb[14], 3);
   VbS4 = vec_splat(Vb[15], 0);
   VbS5 = vec_splat(Vb[15], 1);
   VbS6 = vec_splat(Vb[15], 2);
   VbS7 = vec_splat(Vb[15], 3);
   Vc0[2] = vec_nmsub(VbS0, Va[30], Vc0[2]);
   Vc0[3] = vec_nmsub(VbS0, Va[31], Vc0[3]);
   Vc1[2] = vec_nmsub(VbS1, Va[30], Vc1[2]);
   Vc1[3] = vec_nmsub(VbS1, Va[31], Vc1[3]);
   Vc2[2] = vec_nmsub(VbS2, Va[30], Vc2[2]);
   Vc2[3] = vec_nmsub(VbS2, Va[31], Vc2[3]);
   Vc3[2] = vec_nmsub(VbS3, Va[30], Vc3[2]);
   Vc3[3] = vec_nmsub(VbS3, Va[31], Vc3[3]);
   Vc4[2] = vec_nmsub(VbS4, Va[30], Vc4[2]);
   Vc4[3] = vec_nmsub(VbS4, Va[31], Vc4[3]);
   Vc5[2] = vec_nmsub(VbS5, Va[30], Vc5[2]);
   Vc5[3] = vec_nmsub(VbS5, Va[31], Vc5[3]);
   Vc6[2] = vec_nmsub(VbS6, Va[30], Vc6[2]);
   Vc6[3] = vec_nmsub(VbS6, Va[31], Vc6[3]);
   Vc7[2] = vec_nmsub(VbS7, Va[30], Vc7[2]);
   Vc7[3] = vec_nmsub(VbS7, Va[31], Vc7[3]);

   b[64] = (c0[8] *= a[136]);
   b[65] = (c1[8] *= a[136]);
   b[66] = (c2[8] *= a[136]);
   b[67] = (c3[8] *= a[136]);
   b[68] = (c4[8] *= a[136]);
   b[69] = (c5[8] *= a[136]);
   b[70] = (c6[8] *= a[136]);
   b[71] = (c7[8] *= a[136]);
   VbS0 = vec_splat(Vb[16], 0);
   VbS1 = vec_splat(Vb[16], 1);
   VbS2 = vec_splat(Vb[16], 2);
   VbS3 = vec_splat(Vb[16], 3);
   VbS4 = vec_splat(Vb[17], 0);
   VbS5 = vec_splat(Vb[17], 1);
   VbS6 = vec_splat(Vb[17], 2);
   VbS7 = vec_splat(Vb[17], 3);
   Vc0[3] = vec_nmsub(VbS0, Va[35], Vc0[3]);
   Vc1[3] = vec_nmsub(VbS1, Va[35], Vc1[3]);
   Vc2[3] = vec_nmsub(VbS2, Va[35], Vc2[3]);
   Vc3[3] = vec_nmsub(VbS3, Va[35], Vc3[3]);
   Vc4[3] = vec_nmsub(VbS4, Va[35], Vc4[3]);
   Vc5[3] = vec_nmsub(VbS5, Va[35], Vc5[3]);
   Vc6[3] = vec_nmsub(VbS6, Va[35], Vc6[3]);
   Vc7[3] = vec_nmsub(VbS7, Va[35], Vc7[3]);
   c0[ 9] -= b[64] * a[137];
   c0[10] -= b[64] * a[138];
   c0[11] -= b[64] * a[139];
   c1[ 9] -= b[65] * a[137];
   c1[10] -= b[65] * a[138];
   c1[11] -= b[65] * a[139];
   c2[ 9] -= b[66] * a[137];
   c2[10] -= b[66] * a[138];
   c2[11] -= b[66] * a[139];
   c3[ 9] -= b[67] * a[137];
   c3[10] -= b[67] * a[138];
   c3[11] -= b[67] * a[139];
   c4[ 9] -= b[68] * a[137];
   c4[10] -= b[68] * a[138];
   c4[11] -= b[68] * a[139];
   c5[ 9] -= b[69] * a[137];
   c5[10] -= b[69] * a[138];
   c5[11] -= b[69] * a[139];
   c6[ 9] -= b[70] * a[137];
   c6[10] -= b[70] * a[138];
   c6[11] -= b[70] * a[139];
   c7[ 9] -= b[71] * a[137];
   c7[10] -= b[71] * a[138];
   c7[11] -= b[71] * a[139];

   b[72] = (c0[9] *= a[153]);
   b[73] = (c1[9] *= a[153]);
   b[74] = (c2[9] *= a[153]);
   b[75] = (c3[9] *= a[153]);
   b[76] = (c4[9] *= a[153]);
   b[77] = (c5[9] *= a[153]);
   b[78] = (c6[9] *= a[153]);
   b[79] = (c7[9] *= a[153]);
   VbS0 = vec_splat(Vb[18], 0);
   VbS1 = vec_splat(Vb[18], 1);
   VbS2 = vec_splat(Vb[18], 2);
   VbS3 = vec_splat(Vb[18], 3);
   VbS4 = vec_splat(Vb[19], 0);
   VbS5 = vec_splat(Vb[19], 1);
   VbS6 = vec_splat(Vb[19], 2);
   VbS7 = vec_splat(Vb[19], 3);
   Vc0[3] = vec_nmsub(VbS0, Va[39], Vc0[3]);
   Vc1[3] = vec_nmsub(VbS1, Va[39], Vc1[3]);
   Vc2[3] = vec_nmsub(VbS2, Va[39], Vc2[3]);
   Vc3[3] = vec_nmsub(VbS3, Va[39], Vc3[3]);
   Vc4[3] = vec_nmsub(VbS4, Va[39], Vc4[3]);
   Vc5[3] = vec_nmsub(VbS5, Va[39], Vc5[3]);
   Vc6[3] = vec_nmsub(VbS6, Va[39], Vc6[3]);
   Vc7[3] = vec_nmsub(VbS7, Va[39], Vc7[3]);
   c0[10] -= b[72] * a[154];
   c0[11] -= b[72] * a[155];
   c1[10] -= b[73] * a[154];
   c1[11] -= b[73] * a[155];
   c2[10] -= b[74] * a[154];
   c2[11] -= b[74] * a[155];
   c3[10] -= b[75] * a[154];
   c3[11] -= b[75] * a[155];
   c4[10] -= b[76] * a[154];
   c4[11] -= b[76] * a[155];
   c5[10] -= b[77] * a[154];
   c5[11] -= b[77] * a[155];
   c6[10] -= b[78] * a[154];
   c6[11] -= b[78] * a[155];
   c7[10] -= b[79] * a[154];
   c7[11] -= b[79] * a[155];

   b[80] = (c0[10] *= a[170]);
   b[81] = (c1[10] *= a[170]);
   b[82] = (c2[10] *= a[170]);
   b[83] = (c3[10] *= a[170]);
   b[84] = (c4[10] *= a[170]);
   b[85] = (c5[10] *= a[170]);
   b[86] = (c6[10] *= a[170]);
   b[87] = (c7[10] *= a[170]);
   VbS0 = vec_splat(Vb[20], 0);
   VbS1 = vec_splat(Vb[20], 1);
   VbS2 = vec_splat(Vb[20], 2);
   VbS3 = vec_splat(Vb[20], 3);
   VbS4 = vec_splat(Vb[21], 0);
   VbS5 = vec_splat(Vb[21], 1);
   VbS6 = vec_splat(Vb[21], 2);
   VbS7 = vec_splat(Vb[21], 3);
   Vc0[3] = vec_nmsub(VbS0, Va[43], Vc0[3]);
   Vc1[3] = vec_nmsub(VbS1, Va[43], Vc1[3]);
   Vc2[3] = vec_nmsub(VbS2, Va[43], Vc2[3]);
   Vc3[3] = vec_nmsub(VbS3, Va[43], Vc3[3]);
   Vc4[3] = vec_nmsub(VbS4, Va[43], Vc4[3]);
   Vc5[3] = vec_nmsub(VbS5, Va[43], Vc5[3]);
   Vc6[3] = vec_nmsub(VbS6, Va[43], Vc6[3]);
   Vc7[3] = vec_nmsub(VbS7, Va[43], Vc7[3]);
   c0[11] -= b[80] * a[171];
   c1[11] -= b[81] * a[171];
   c2[11] -= b[82] * a[171];
   c3[11] -= b[83] * a[171];
   c4[11] -= b[84] * a[171];
   c5[11] -= b[85] * a[171];
   c6[11] -= b[86] * a[171];
   c7[11] -= b[87] * a[171];

   b[88] = (c0[11] *= a[187]);
   b[89] = (c1[11] *= a[187]);
   b[90] = (c2[11] *= a[187]);
   b[91] = (c3[11] *= a[187]);
   b[92] = (c4[11] *= a[187]);
   b[93] = (c5[11] *= a[187]);
   b[94] = (c6[11] *= a[187]);
   b[95] = (c7[11] *= a[187]);
   VbS0 = vec_splat(Vb[22], 0);
   VbS1 = vec_splat(Vb[22], 1);
   VbS2 = vec_splat(Vb[22], 2);
   VbS3 = vec_splat(Vb[22], 3);
   VbS4 = vec_splat(Vb[23], 0);
   VbS5 = vec_splat(Vb[23], 1);
   VbS6 = vec_splat(Vb[23], 2);
   VbS7 = vec_splat(Vb[23], 3);
   Vc0[3] = vec_nmsub(VbS0, Va[47], Vc0[3]);
   Vc1[3] = vec_nmsub(VbS1, Va[47], Vc1[3]);
   Vc2[3] = vec_nmsub(VbS2, Va[47], Vc2[3]);
   Vc3[3] = vec_nmsub(VbS3, Va[47], Vc3[3]);
   Vc4[3] = vec_nmsub(VbS4, Va[47], Vc4[3]);
   Vc5[3] = vec_nmsub(VbS5, Va[47], Vc5[3]);
   Vc6[3] = vec_nmsub(VbS6, Va[47], Vc6[3]);
   Vc7[3] = vec_nmsub(VbS7, Va[47], Vc7[3]);

   b[ 96] = (c0[12] *= a[204]);
   b[ 97] = (c1[12] *= a[204]);
   b[ 98] = (c2[12] *= a[204]);
   b[ 99] = (c3[12] *= a[204]);
   b[100] = (c4[12] *= a[204]);
   b[101] = (c5[12] *= a[204]);
   b[102] = (c6[12] *= a[204]);
   b[103] = (c7[12] *= a[204]);
   c0[13] -= b[ 96] * a[205];
   c0[14] -= b[ 96] * a[206];
   c0[15] -= b[ 96] * a[207];
   c1[13] -= b[ 97] * a[205];
   c1[14] -= b[ 97] * a[206];
   c1[15] -= b[ 97] * a[207];
   c2[13] -= b[ 98] * a[205];
   c2[14] -= b[ 98] * a[206];
   c2[15] -= b[ 98] * a[207];
   c3[13] -= b[ 99] * a[205];
   c3[14] -= b[ 99] * a[206];
   c3[15] -= b[ 99] * a[207];
   c4[13] -= b[100] * a[205];
   c4[14] -= b[100] * a[206];
   c4[15] -= b[100] * a[207];
   c5[13] -= b[101] * a[205];
   c5[14] -= b[101] * a[206];
   c5[15] -= b[101] * a[207];
   c6[13] -= b[102] * a[205];
   c6[14] -= b[102] * a[206];
   c6[15] -= b[102] * a[207];
   c7[13] -= b[103] * a[205];
   c7[14] -= b[103] * a[206];
   c7[15] -= b[103] * a[207];

   b[104] = (c0[13] *= a[221]);
   b[105] = (c1[13] *= a[221]);
   b[106] = (c2[13] *= a[221]);
   b[107] = (c3[13] *= a[221]);
   b[108] = (c4[13] *= a[221]);
   b[109] = (c5[13] *= a[221]);
   b[110] = (c6[13] *= a[221]);
   b[111] = (c7[13] *= a[221]);
   c0[14] -= b[104] * a[222];
   c0[15] -= b[104] * a[223];
   c1[14] -= b[105] * a[222];
   c1[15] -= b[105] * a[223];
   c2[14] -= b[106] * a[222];
   c2[15] -= b[106] * a[223];
   c3[14] -= b[107] * a[222];
   c3[15] -= b[107] * a[223];
   c4[14] -= b[108] * a[222];
   c4[15] -= b[108] * a[223];
   c5[14] -= b[109] * a[222];
   c5[15] -= b[109] * a[223];
   c6[14] -= b[110] * a[222];
   c6[15] -= b[110] * a[223];
   c7[14] -= b[111] * a[222];
   c7[15] -= b[111] * a[223];

   b[112] = (c0[14] *= a[238]);
   b[113] = (c1[14] *= a[238]);
   b[114] = (c2[14] *= a[238]);
   b[115] = (c3[14] *= a[238]);
   b[116] = (c4[14] *= a[238]);
   b[117] = (c5[14] *= a[238]);
   b[118] = (c6[14] *= a[238]);
   b[119] = (c7[14] *= a[238]);
   c0[15] -= b[112] * a[239];
   c1[15] -= b[113] * a[239];
   c2[15] -= b[114] * a[239];
   c3[15] -= b[115] * a[239];
   c4[15] -= b[116] * a[239];
   c5[15] -= b[117] * a[239];
   c6[15] -= b[118] * a[239];
   c7[15] -= b[119] * a[239];

   b[120] = (c0[15] *= a[255]);
   b[121] = (c1[15] *= a[255]);
   b[122] = (c2[15] *= a[255]);
   b[123] = (c3[15] *= a[255]);
   b[124] = (c4[15] *= a[255]);
   b[125] = (c5[15] *= a[255]);
   b[126] = (c6[15] *= a[255]);
   b[127] = (c7[15] *= a[255]);
}

#endif

static inline __attribute__ ((always_inline)) void solve(BLASLONG m, BLASLONG n, FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc) {

  FLOAT aa, bb;

  int i, j, k;

  for (i = 0; i < m; i++) {

    aa = *(a + i);

    for (j = 0; j < n; j ++) {
      bb = *(c + i + j * ldc);
      bb *= aa;
      *b             = bb;
      *(c + i + j * ldc) = bb;
      b ++;

      for (k = i + 1; k < m; k ++){
	*(c + k + j * ldc) -= bb * *(a + k);
      }

    }
    a += m;
  }
}

#else

static inline __attribute__ ((always_inline)) void solve(BLASLONG m, BLASLONG n, FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc) {

  FLOAT aa1, aa2;
  FLOAT bb1, bb2;
  FLOAT cc1, cc2;

  int i, j, k;

  ldc *= 2;

  for (i = 0; i < m; i++) {

    aa1 = *(a + i * 2 + 0);
    aa2 = *(a + i * 2 + 1);

    for (j = 0; j < n; j ++) {
      bb1 = *(c + i * 2 + 0 + j * ldc);
      bb2 = *(c + i * 2 + 1 + j * ldc);

#ifndef CONJ
      cc1 = aa1 * bb1 - aa2 * bb2;
      cc2 = aa1 * bb2 + aa2 * bb1;
#else
      cc1 = aa1 * bb1 + aa2 * bb2;
      cc2 = aa1 * bb2 - aa2 * bb1;
#endif

      *(b + 0) = cc1;
      *(b + 1) = cc2;
      *(c + i * 2 + 0 + j * ldc) = cc1;
      *(c + i * 2 + 1 + j * ldc) = cc2;
      b += 2;

      for (k = i + 1; k < m; k ++){
#ifndef CONJ
	*(c + k * 2 + 0 + j * ldc) -= cc1 * *(a + k * 2 + 0) - cc2 * *(a + k * 2 + 1);
	*(c + k * 2 + 1 + j * ldc) -= cc1 * *(a + k * 2 + 1) + cc2 * *(a + k * 2 + 0);
#else
	*(c + k * 2 + 0 + j * ldc) -= cc1 * *(a + k * 2 + 0) + cc2 * *(a + k * 2 + 1);
	*(c + k * 2 + 1 + j * ldc) -= -cc1 * *(a + k * 2 + 1) + cc2 * *(a + k * 2 + 0);
#endif
      }

    }
    a += m * 2;
  }
}

#endif


int CNAME(BLASLONG m, BLASLONG n, BLASLONG k, FLOAT dummy1,
#ifdef COMPLEX
	   FLOAT dummy2,
#endif
	   FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG offset){

  FLOAT *aa, *cc;
  BLASLONG  kk;
  BLASLONG i, j, jj;

#if 0
  fprintf(stderr, "TRSM KERNEL LT : m = %3ld  n = %3ld  k = %3ld offset = %3ld\n",
	  m, n, k, offset);
#endif

  jj = 0;

  j = (n >> GEMM_UNROLL_N_SHIFT);

#ifdef DOUBLE
  int well_aligned = (GEMM_UNROLL_M==8) && (GEMM_UNROLL_N==8) && ((((unsigned long) a) & 0x7) == 0);
#else
  int well_aligned = (GEMM_UNROLL_M==16) && (GEMM_UNROLL_N==8) && ((((unsigned long) a) & 0x7) == 0);
#endif

  while (j > 0) {

    kk = offset;
    aa = a;
    cc = c;

    i = (m >> GEMM_UNROLL_M_SHIFT);

    while (i > 0) {

	if (kk > 0) {
	  GEMM_KERNEL(GEMM_UNROLL_M, GEMM_UNROLL_N, kk, dm1,
#ifdef COMPLEX
		      ZERO,
#endif
		      aa, b, cc, ldc);
	}

      if (well_aligned) {
#ifdef DOUBLE
	solve8x8(aa + kk * GEMM_UNROLL_M * COMPSIZE,
		 b  + kk * GEMM_UNROLL_N * COMPSIZE, cc, ldc);
#else
	solve16x8(aa + kk * GEMM_UNROLL_M * COMPSIZE,
		  b  + kk * GEMM_UNROLL_N * COMPSIZE, cc, ldc);
#endif
      }
      else {
	solve(GEMM_UNROLL_M, GEMM_UNROLL_N,
	      aa + kk * GEMM_UNROLL_M * COMPSIZE,
	      b  + kk * GEMM_UNROLL_N * COMPSIZE,
	      cc, ldc);
      }

      aa += GEMM_UNROLL_M * k * COMPSIZE;
      cc += GEMM_UNROLL_M     * COMPSIZE;
      kk += GEMM_UNROLL_M;
      i --;
    }

    if (m & (GEMM_UNROLL_M - 1)) {
      i = (GEMM_UNROLL_M >> 1);
      while (i > 0) {
	if (m & i) {
	    if (kk > 0) {
	      GEMM_KERNEL(i, GEMM_UNROLL_N, kk, dm1,
#ifdef COMPLEX
			  ZERO,
#endif
			  aa, b, cc, ldc);
	    }
	  solve(i, GEMM_UNROLL_N,
		aa + kk * i             * COMPSIZE,
		b  + kk * GEMM_UNROLL_N * COMPSIZE,
		cc, ldc);

	  aa += i * k * COMPSIZE;
	  cc += i     * COMPSIZE;
	  kk += i;
	}
	i >>= 1;
      }
    }

    b += GEMM_UNROLL_N * k   * COMPSIZE;
    c += GEMM_UNROLL_N * ldc * COMPSIZE;
    j --;
    jj += GEMM_UNROLL_M;
  }

  if (n & (GEMM_UNROLL_N - 1)) {

    j = (GEMM_UNROLL_N >> 1);
    while (j > 0) {
      if (n & j) {

	kk = offset;
	aa = a;
	cc = c;

	i = (m >> GEMM_UNROLL_M_SHIFT);

	while (i > 0) {
	  if (kk > 0) {
	    GEMM_KERNEL(GEMM_UNROLL_M, j, kk, dm1,
#ifdef COMPLEX
			ZERO,
#endif
			aa,
			b,
			cc,
			ldc);
	  }

	  solve(GEMM_UNROLL_M, j,
		aa + kk * GEMM_UNROLL_M * COMPSIZE,
		b  + kk * j             * COMPSIZE, cc, ldc);

	  aa += GEMM_UNROLL_M * k * COMPSIZE;
	  cc += GEMM_UNROLL_M     * COMPSIZE;
	  kk += GEMM_UNROLL_M;
	  i --;
	}

	if (m & (GEMM_UNROLL_M - 1)) {
	  i = (GEMM_UNROLL_M >> 1);
	  while (i > 0) {
	    if (m & i) {
	      if (kk > 0) {
		GEMM_KERNEL(i, j, kk, dm1,
#ifdef COMPLEX
			    ZERO,
#endif
			    aa,
			    b,
			    cc,
			    ldc);
	      }

	      solve(i, j,
		    aa + kk * i * COMPSIZE,
		    b  + kk * j * COMPSIZE, cc, ldc);

	      aa += i * k * COMPSIZE;
	      cc += i     * COMPSIZE;
	      kk += i;
	      }
	    i >>= 1;
	  }
	}

	b += j * k   * COMPSIZE;
	c += j * ldc * COMPSIZE;
      }
      j >>= 1;
    }
  }

  return 0;
}
