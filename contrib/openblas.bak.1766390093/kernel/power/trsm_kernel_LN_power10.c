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

   b[56] = (c0[7] *= a[63]);
   b[57] = (c1[7] *= a[63]);
   b[58] = (c2[7] *= a[63]);
   b[59] = (c3[7] *= a[63]);
   b[60] = (c4[7] *= a[63]);
   b[61] = (c5[7] *= a[63]);
   b[62] = (c6[7] *= a[63]);
   b[63] = (c7[7] *= a[63]);
   VbS0 = vec_splat(Vb[28], 0);
   VbS1 = vec_splat(Vb[28], 1);
   VbS2 = vec_splat(Vb[29], 0);
   VbS3 = vec_splat(Vb[29], 1);
   VbS4 = vec_splat(Vb[30], 0);
   VbS5 = vec_splat(Vb[30], 1);
   VbS6 = vec_splat(Vb[31], 0);
   VbS7 = vec_splat(Vb[31], 1);
   Vc0[0] = vec_nmsub(VbS0, Va[28], Vc0[0]);
   Vc0[1] = vec_nmsub(VbS0, Va[29], Vc0[1]);
   Vc0[2] = vec_nmsub(VbS0, Va[30], Vc0[2]);
   Vc1[0] = vec_nmsub(VbS1, Va[28], Vc1[0]);
   Vc1[1] = vec_nmsub(VbS1, Va[29], Vc1[1]);
   Vc1[2] = vec_nmsub(VbS1, Va[30], Vc1[2]);
   Vc2[0] = vec_nmsub(VbS2, Va[28], Vc2[0]);
   Vc2[1] = vec_nmsub(VbS2, Va[29], Vc2[1]);
   Vc2[2] = vec_nmsub(VbS2, Va[30], Vc2[2]);
   Vc3[0] = vec_nmsub(VbS3, Va[28], Vc3[0]);
   Vc3[1] = vec_nmsub(VbS3, Va[29], Vc3[1]);
   Vc3[2] = vec_nmsub(VbS3, Va[30], Vc3[2]);
   Vc4[0] = vec_nmsub(VbS4, Va[28], Vc4[0]);
   Vc4[1] = vec_nmsub(VbS4, Va[29], Vc4[1]);
   Vc4[2] = vec_nmsub(VbS4, Va[30], Vc4[2]);
   Vc5[0] = vec_nmsub(VbS5, Va[28], Vc5[0]);
   Vc5[1] = vec_nmsub(VbS5, Va[29], Vc5[1]);
   Vc5[2] = vec_nmsub(VbS5, Va[30], Vc5[2]);
   Vc6[0] = vec_nmsub(VbS6, Va[28], Vc6[0]);
   Vc6[1] = vec_nmsub(VbS6, Va[29], Vc6[1]);
   Vc6[2] = vec_nmsub(VbS6, Va[30], Vc6[2]);
   Vc7[0] = vec_nmsub(VbS7, Va[28], Vc7[0]);
   Vc7[1] = vec_nmsub(VbS7, Va[29], Vc7[1]);
   Vc7[2] = vec_nmsub(VbS7, Va[30], Vc7[2]);
   c0[6] -= c0[7] * a[62];
   c1[6] -= c1[7] * a[62];
   c2[6] -= c2[7] * a[62];
   c3[6] -= c3[7] * a[62];
   c4[6] -= c4[7] * a[62];
   c5[6] -= c5[7] * a[62];
   c6[6] -= c6[7] * a[62];
   c7[6] -= c7[7] * a[62];

   b[48] = (c0[6] *= a[54]);
   b[49] = (c1[6] *= a[54]);
   b[50] = (c2[6] *= a[54]);
   b[51] = (c3[6] *= a[54]);
   b[52] = (c4[6] *= a[54]);
   b[53] = (c5[6] *= a[54]);
   b[54] = (c6[6] *= a[54]);
   b[55] = (c7[6] *= a[54]);
   VbS0 = vec_splat(Vb[24], 0);
   VbS1 = vec_splat(Vb[24], 1);
   VbS2 = vec_splat(Vb[25], 0);
   VbS3 = vec_splat(Vb[25], 1);
   VbS4 = vec_splat(Vb[26], 0);
   VbS5 = vec_splat(Vb[26], 1);
   VbS6 = vec_splat(Vb[27], 0);
   VbS7 = vec_splat(Vb[27], 1);
   Vc0[0] = vec_nmsub(VbS0, Va[24], Vc0[0]);
   Vc0[1] = vec_nmsub(VbS0, Va[25], Vc0[1]);
   Vc0[2] = vec_nmsub(VbS0, Va[26], Vc0[2]);
   Vc1[0] = vec_nmsub(VbS1, Va[24], Vc1[0]);
   Vc1[1] = vec_nmsub(VbS1, Va[25], Vc1[1]);
   Vc1[2] = vec_nmsub(VbS1, Va[26], Vc1[2]);
   Vc2[0] = vec_nmsub(VbS2, Va[24], Vc2[0]);
   Vc2[1] = vec_nmsub(VbS2, Va[25], Vc2[1]);
   Vc2[2] = vec_nmsub(VbS2, Va[26], Vc2[2]);
   Vc3[0] = vec_nmsub(VbS3, Va[24], Vc3[0]);
   Vc3[1] = vec_nmsub(VbS3, Va[25], Vc3[1]);
   Vc3[2] = vec_nmsub(VbS3, Va[26], Vc3[2]);
   Vc4[0] = vec_nmsub(VbS4, Va[24], Vc4[0]);
   Vc4[1] = vec_nmsub(VbS4, Va[25], Vc4[1]);
   Vc4[2] = vec_nmsub(VbS4, Va[26], Vc4[2]);
   Vc5[0] = vec_nmsub(VbS5, Va[24], Vc5[0]);
   Vc5[1] = vec_nmsub(VbS5, Va[25], Vc5[1]);
   Vc5[2] = vec_nmsub(VbS5, Va[26], Vc5[2]);
   Vc6[0] = vec_nmsub(VbS6, Va[24], Vc6[0]);
   Vc6[1] = vec_nmsub(VbS6, Va[25], Vc6[1]);
   Vc6[2] = vec_nmsub(VbS6, Va[26], Vc6[2]);
   Vc7[0] = vec_nmsub(VbS7, Va[24], Vc7[0]);
   Vc7[1] = vec_nmsub(VbS7, Va[25], Vc7[1]);
   Vc7[2] = vec_nmsub(VbS7, Va[26], Vc7[2]);

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
   Vc0[0] = vec_nmsub(VbS0, Va[20], Vc0[0]);
   Vc0[1] = vec_nmsub(VbS0, Va[21], Vc0[1]);
   Vc1[0] = vec_nmsub(VbS1, Va[20], Vc1[0]);
   Vc1[1] = vec_nmsub(VbS1, Va[21], Vc1[1]);
   Vc2[0] = vec_nmsub(VbS2, Va[20], Vc2[0]);
   Vc2[1] = vec_nmsub(VbS2, Va[21], Vc2[1]);
   Vc3[0] = vec_nmsub(VbS3, Va[20], Vc3[0]);
   Vc3[1] = vec_nmsub(VbS3, Va[21], Vc3[1]);
   Vc4[0] = vec_nmsub(VbS4, Va[20], Vc4[0]);
   Vc4[1] = vec_nmsub(VbS4, Va[21], Vc4[1]);
   Vc5[0] = vec_nmsub(VbS5, Va[20], Vc5[0]);
   Vc5[1] = vec_nmsub(VbS5, Va[21], Vc5[1]);
   Vc6[0] = vec_nmsub(VbS6, Va[20], Vc6[0]);
   Vc6[1] = vec_nmsub(VbS6, Va[21], Vc6[1]);
   Vc7[0] = vec_nmsub(VbS7, Va[20], Vc7[0]);
   Vc7[1] = vec_nmsub(VbS7, Va[21], Vc7[1]);
   c0[4] -= c0[5] * a[44];
   c1[4] -= c1[5] * a[44];
   c2[4] -= c2[5] * a[44];
   c3[4] -= c3[5] * a[44];
   c4[4] -= c4[5] * a[44];
   c5[4] -= c5[5] * a[44];
   c6[4] -= c6[5] * a[44];
   c7[4] -= c7[5] * a[44];

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
   Vc0[0] = vec_nmsub(VbS0, Va[16], Vc0[0]);
   Vc0[1] = vec_nmsub(VbS0, Va[17], Vc0[1]);
   Vc1[0] = vec_nmsub(VbS1, Va[16], Vc1[0]);
   Vc1[1] = vec_nmsub(VbS1, Va[17], Vc1[1]);
   Vc2[0] = vec_nmsub(VbS2, Va[16], Vc2[0]);
   Vc2[1] = vec_nmsub(VbS2, Va[17], Vc2[1]);
   Vc3[0] = vec_nmsub(VbS3, Va[16], Vc3[0]);
   Vc3[1] = vec_nmsub(VbS3, Va[17], Vc3[1]);
   Vc4[0] = vec_nmsub(VbS4, Va[16], Vc4[0]);
   Vc4[1] = vec_nmsub(VbS4, Va[17], Vc4[1]);
   Vc5[0] = vec_nmsub(VbS5, Va[16], Vc5[0]);
   Vc5[1] = vec_nmsub(VbS5, Va[17], Vc5[1]);
   Vc6[0] = vec_nmsub(VbS6, Va[16], Vc6[0]);
   Vc6[1] = vec_nmsub(VbS6, Va[17], Vc6[1]);
   Vc7[0] = vec_nmsub(VbS7, Va[16], Vc7[0]);
   Vc7[1] = vec_nmsub(VbS7, Va[17], Vc7[1]);
   
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
   Vc0[0] = vec_nmsub(VbS0, Va[12], Vc0[0]);
   Vc1[0] = vec_nmsub(VbS1, Va[12], Vc1[0]);
   Vc2[0] = vec_nmsub(VbS2, Va[12], Vc2[0]);
   Vc3[0] = vec_nmsub(VbS3, Va[12], Vc3[0]);
   Vc4[0] = vec_nmsub(VbS4, Va[12], Vc4[0]);
   Vc5[0] = vec_nmsub(VbS5, Va[12], Vc5[0]);
   Vc6[0] = vec_nmsub(VbS6, Va[12], Vc6[0]);
   Vc7[0] = vec_nmsub(VbS7, Va[12], Vc7[0]);
   c0[2] -= c0[3] * a[26];
   c1[2] -= c1[3] * a[26];
   c2[2] -= c2[3] * a[26];
   c3[2] -= c3[3] * a[26];
   c4[2] -= c4[3] * a[26];
   c5[2] -= c5[3] * a[26];
   c6[2] -= c6[3] * a[26];
   c7[2] -= c7[3] * a[26];

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
   Vc0[0] = vec_nmsub(VbS0, Va[8], Vc0[0]);
   Vc1[0] = vec_nmsub(VbS1, Va[8], Vc1[0]);
   Vc2[0] = vec_nmsub(VbS2, Va[8], Vc2[0]);
   Vc3[0] = vec_nmsub(VbS3, Va[8], Vc3[0]);
   Vc4[0] = vec_nmsub(VbS4, Va[8], Vc4[0]);
   Vc5[0] = vec_nmsub(VbS5, Va[8], Vc5[0]);
   Vc6[0] = vec_nmsub(VbS6, Va[8], Vc6[0]);
   Vc7[0] = vec_nmsub(VbS7, Va[8], Vc7[0]);

   b[ 8] = (c0[1] *= a[9]);
   b[ 9] = (c1[1] *= a[9]);
   b[10] = (c2[1] *= a[9]);
   b[11] = (c3[1] *= a[9]);
   b[12] = (c4[1] *= a[9]);
   b[13] = (c5[1] *= a[9]);
   b[14] = (c6[1] *= a[9]);
   b[15] = (c7[1] *= a[9]);
   c0[0] -= c0[1] * a[8];
   c1[0] -= c1[1] * a[8];
   c2[0] -= c2[1] * a[8];
   c3[0] -= c3[1] * a[8];
   c4[0] -= c4[1] * a[8];
   c5[0] -= c5[1] * a[8];
   c6[0] -= c6[1] * a[8];
   c7[0] -= c7[1] * a[8];

   b[0] = (c0[0] *= a[0]);
   b[1] = (c1[0] *= a[0]);
   b[2] = (c2[0] *= a[0]);
   b[3] = (c3[0] *= a[0]);
   b[4] = (c4[0] *= a[0]);
   b[5] = (c5[0] *= a[0]);
   b[6] = (c6[0] *= a[0]);
   b[7] = (c7[0] *= a[0]);
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

   b[120] = (c0[15] *= a[255]);
   b[121] = (c1[15] *= a[255]);
   b[122] = (c2[15] *= a[255]);
   b[123] = (c3[15] *= a[255]);
   b[124] = (c4[15] *= a[255]);
   b[125] = (c5[15] *= a[255]);
   b[126] = (c6[15] *= a[255]);
   b[127] = (c7[15] *= a[255]);
   VbS0 = vec_splat(Vb[30], 0);
   VbS1 = vec_splat(Vb[30], 1);
   VbS2 = vec_splat(Vb[30], 2);
   VbS3 = vec_splat(Vb[30], 3);
   VbS4 = vec_splat(Vb[31], 0);
   VbS5 = vec_splat(Vb[31], 1);
   VbS6 = vec_splat(Vb[31], 2);
   VbS7 = vec_splat(Vb[31], 3);
   Vc0[0] = vec_nmsub(VbS0, Va[60], Vc0[0]);
   Vc0[1] = vec_nmsub(VbS0, Va[61], Vc0[1]);
   Vc0[2] = vec_nmsub(VbS0, Va[62], Vc0[2]);
   Vc1[0] = vec_nmsub(VbS1, Va[60], Vc1[0]);
   Vc1[1] = vec_nmsub(VbS1, Va[61], Vc1[1]);
   Vc1[2] = vec_nmsub(VbS1, Va[62], Vc1[2]);
   Vc2[0] = vec_nmsub(VbS2, Va[60], Vc2[0]);
   Vc2[1] = vec_nmsub(VbS2, Va[61], Vc2[1]);
   Vc2[2] = vec_nmsub(VbS2, Va[62], Vc2[2]);
   Vc3[0] = vec_nmsub(VbS3, Va[60], Vc3[0]);
   Vc3[1] = vec_nmsub(VbS3, Va[61], Vc3[1]);
   Vc3[2] = vec_nmsub(VbS3, Va[62], Vc3[2]);
   Vc4[0] = vec_nmsub(VbS4, Va[60], Vc4[0]);
   Vc4[1] = vec_nmsub(VbS4, Va[61], Vc4[1]);
   Vc4[2] = vec_nmsub(VbS4, Va[62], Vc4[2]);
   Vc5[0] = vec_nmsub(VbS5, Va[60], Vc5[0]);
   Vc5[1] = vec_nmsub(VbS5, Va[61], Vc5[1]);
   Vc5[2] = vec_nmsub(VbS5, Va[62], Vc5[2]);
   Vc6[0] = vec_nmsub(VbS6, Va[60], Vc6[0]);
   Vc6[1] = vec_nmsub(VbS6, Va[61], Vc6[1]);
   Vc6[2] = vec_nmsub(VbS6, Va[62], Vc6[2]);
   Vc7[0] = vec_nmsub(VbS7, Va[60], Vc7[0]);
   Vc7[1] = vec_nmsub(VbS7, Va[61], Vc7[1]);
   Vc7[2] = vec_nmsub(VbS7, Va[62], Vc7[2]);
   c0[12] -= b[120] * a[252];
   c0[13] -= b[120] * a[253];
   c0[14] -= b[120] * a[254];
   c1[12] -= b[121] * a[252];
   c1[13] -= b[121] * a[253];
   c1[14] -= b[121] * a[254];
   c2[12] -= b[122] * a[252];
   c2[13] -= b[122] * a[253];
   c2[14] -= b[122] * a[254];
   c3[12] -= b[123] * a[252];
   c3[13] -= b[123] * a[253];
   c3[14] -= b[123] * a[254];
   c4[12] -= b[124] * a[252];
   c4[13] -= b[124] * a[253];
   c4[14] -= b[124] * a[254];
   c5[12] -= b[125] * a[252];
   c5[13] -= b[125] * a[253];
   c5[14] -= b[125] * a[254];
   c6[12] -= b[126] * a[252];
   c6[13] -= b[126] * a[253];
   c6[14] -= b[126] * a[254];
   c7[12] -= b[127] * a[252];
   c7[13] -= b[127] * a[253];
   c7[14] -= b[127] * a[254];

   b[112] = (c0[14] *= a[238]);
   b[113] = (c1[14] *= a[238]);
   b[114] = (c2[14] *= a[238]);
   b[115] = (c3[14] *= a[238]);
   b[116] = (c4[14] *= a[238]);
   b[117] = (c5[14] *= a[238]);
   b[118] = (c6[14] *= a[238]);
   b[119] = (c7[14] *= a[238]);
   VbS0 = vec_splat(Vb[28], 0);
   VbS1 = vec_splat(Vb[28], 1);
   VbS2 = vec_splat(Vb[28], 2);
   VbS3 = vec_splat(Vb[28], 3);
   VbS4 = vec_splat(Vb[29], 0);
   VbS5 = vec_splat(Vb[29], 1);
   VbS6 = vec_splat(Vb[29], 2);
   VbS7 = vec_splat(Vb[29], 3);
   Vc0[0] = vec_nmsub(VbS0, Va[56], Vc0[0]);
   Vc0[1] = vec_nmsub(VbS0, Va[57], Vc0[1]);
   Vc0[2] = vec_nmsub(VbS0, Va[58], Vc0[2]);
   Vc1[0] = vec_nmsub(VbS1, Va[56], Vc1[0]);
   Vc1[1] = vec_nmsub(VbS1, Va[57], Vc1[1]);
   Vc1[2] = vec_nmsub(VbS1, Va[58], Vc1[2]);
   Vc2[0] = vec_nmsub(VbS2, Va[56], Vc2[0]);
   Vc2[1] = vec_nmsub(VbS2, Va[57], Vc2[1]);
   Vc2[2] = vec_nmsub(VbS2, Va[58], Vc2[2]);
   Vc3[0] = vec_nmsub(VbS3, Va[56], Vc3[0]);
   Vc3[1] = vec_nmsub(VbS3, Va[57], Vc3[1]);
   Vc3[2] = vec_nmsub(VbS3, Va[58], Vc3[2]);
   Vc4[0] = vec_nmsub(VbS4, Va[56], Vc4[0]);
   Vc4[1] = vec_nmsub(VbS4, Va[57], Vc4[1]);
   Vc4[2] = vec_nmsub(VbS4, Va[58], Vc4[2]);
   Vc5[0] = vec_nmsub(VbS5, Va[56], Vc5[0]);
   Vc5[1] = vec_nmsub(VbS5, Va[57], Vc5[1]);
   Vc5[2] = vec_nmsub(VbS5, Va[58], Vc5[2]);
   Vc6[0] = vec_nmsub(VbS6, Va[56], Vc6[0]);
   Vc6[1] = vec_nmsub(VbS6, Va[57], Vc6[1]);
   Vc6[2] = vec_nmsub(VbS6, Va[58], Vc6[2]);
   Vc7[0] = vec_nmsub(VbS7, Va[56], Vc7[0]);
   Vc7[1] = vec_nmsub(VbS7, Va[57], Vc7[1]);
   Vc7[2] = vec_nmsub(VbS7, Va[58], Vc7[2]);
   c0[12] -= b[112] * a[236];
   c0[13] -= b[112] * a[237];
   c1[12] -= b[113] * a[236];
   c1[13] -= b[113] * a[237];
   c2[12] -= b[114] * a[236];
   c2[13] -= b[114] * a[237];
   c3[12] -= b[115] * a[236];
   c3[13] -= b[115] * a[237];
   c4[12] -= b[116] * a[236];
   c4[13] -= b[116] * a[237];
   c5[12] -= b[117] * a[236];
   c5[13] -= b[117] * a[237];
   c6[12] -= b[118] * a[236];
   c6[13] -= b[118] * a[237];
   c7[12] -= b[119] * a[236];
   c7[13] -= b[119] * a[237];

   b[104] = (c0[13] *= a[221]);
   b[105] = (c1[13] *= a[221]);
   b[106] = (c2[13] *= a[221]);
   b[107] = (c3[13] *= a[221]);
   b[108] = (c4[13] *= a[221]);
   b[109] = (c5[13] *= a[221]);
   b[110] = (c6[13] *= a[221]);
   b[111] = (c7[13] *= a[221]);
   VbS0 = vec_splat(Vb[26], 0);
   VbS1 = vec_splat(Vb[26], 1);
   VbS2 = vec_splat(Vb[26], 2);
   VbS3 = vec_splat(Vb[26], 3);
   VbS4 = vec_splat(Vb[27], 0);
   VbS5 = vec_splat(Vb[27], 1);
   VbS6 = vec_splat(Vb[27], 2);
   VbS7 = vec_splat(Vb[27], 3);
   Vc0[0] = vec_nmsub(VbS0, Va[52], Vc0[0]);
   Vc0[1] = vec_nmsub(VbS0, Va[53], Vc0[1]);
   Vc0[2] = vec_nmsub(VbS0, Va[54], Vc0[2]);
   Vc1[0] = vec_nmsub(VbS1, Va[52], Vc1[0]);
   Vc1[1] = vec_nmsub(VbS1, Va[53], Vc1[1]);
   Vc1[2] = vec_nmsub(VbS1, Va[54], Vc1[2]);
   Vc2[0] = vec_nmsub(VbS2, Va[52], Vc2[0]);
   Vc2[1] = vec_nmsub(VbS2, Va[53], Vc2[1]);
   Vc2[2] = vec_nmsub(VbS2, Va[54], Vc2[2]);
   Vc3[0] = vec_nmsub(VbS3, Va[52], Vc3[0]);
   Vc3[1] = vec_nmsub(VbS3, Va[53], Vc3[1]);
   Vc3[2] = vec_nmsub(VbS3, Va[54], Vc3[2]);
   Vc4[0] = vec_nmsub(VbS4, Va[52], Vc4[0]);
   Vc4[1] = vec_nmsub(VbS4, Va[53], Vc4[1]);
   Vc4[2] = vec_nmsub(VbS4, Va[54], Vc4[2]);
   Vc5[0] = vec_nmsub(VbS5, Va[52], Vc5[0]);
   Vc5[1] = vec_nmsub(VbS5, Va[53], Vc5[1]);
   Vc5[2] = vec_nmsub(VbS5, Va[54], Vc5[2]);
   Vc6[0] = vec_nmsub(VbS6, Va[52], Vc6[0]);
   Vc6[1] = vec_nmsub(VbS6, Va[53], Vc6[1]);
   Vc6[2] = vec_nmsub(VbS6, Va[54], Vc6[2]);
   Vc7[0] = vec_nmsub(VbS7, Va[52], Vc7[0]);
   Vc7[1] = vec_nmsub(VbS7, Va[53], Vc7[1]);
   Vc7[2] = vec_nmsub(VbS7, Va[54], Vc7[2]);
   c0[12] -= b[104] * a[220];
   c1[12] -= b[105] * a[220];
   c2[12] -= b[106] * a[220];
   c3[12] -= b[107] * a[220];
   c4[12] -= b[108] * a[220];
   c5[12] -= b[109] * a[220];
   c6[12] -= b[110] * a[220];
   c7[12] -= b[111] * a[220];

   b[ 96] = (c0[12] *= a[204]);
   b[ 97] = (c1[12] *= a[204]);
   b[ 98] = (c2[12] *= a[204]);
   b[ 99] = (c3[12] *= a[204]);
   b[100] = (c4[12] *= a[204]);
   b[101] = (c5[12] *= a[204]);
   b[102] = (c6[12] *= a[204]);
   b[103] = (c7[12] *= a[204]);
   VbS0 = vec_splat(Vb[24], 0);
   VbS1 = vec_splat(Vb[24], 1);
   VbS2 = vec_splat(Vb[24], 2);
   VbS3 = vec_splat(Vb[24], 3);
   VbS4 = vec_splat(Vb[25], 0);
   VbS5 = vec_splat(Vb[25], 1);
   VbS6 = vec_splat(Vb[25], 2);
   VbS7 = vec_splat(Vb[25], 3);
   Vc0[0] = vec_nmsub(VbS0, Va[48], Vc0[0]);
   Vc0[1] = vec_nmsub(VbS0, Va[49], Vc0[1]);
   Vc0[2] = vec_nmsub(VbS0, Va[50], Vc0[2]);
   Vc1[0] = vec_nmsub(VbS1, Va[48], Vc1[0]);
   Vc1[1] = vec_nmsub(VbS1, Va[49], Vc1[1]);
   Vc1[2] = vec_nmsub(VbS1, Va[50], Vc1[2]);
   Vc2[0] = vec_nmsub(VbS2, Va[48], Vc2[0]);
   Vc2[1] = vec_nmsub(VbS2, Va[49], Vc2[1]);
   Vc2[2] = vec_nmsub(VbS2, Va[50], Vc2[2]);
   Vc3[0] = vec_nmsub(VbS3, Va[48], Vc3[0]);
   Vc3[1] = vec_nmsub(VbS3, Va[49], Vc3[1]);
   Vc3[2] = vec_nmsub(VbS3, Va[50], Vc3[2]);
   Vc4[0] = vec_nmsub(VbS4, Va[48], Vc4[0]);
   Vc4[1] = vec_nmsub(VbS4, Va[49], Vc4[1]);
   Vc4[2] = vec_nmsub(VbS4, Va[50], Vc4[2]);
   Vc5[0] = vec_nmsub(VbS5, Va[48], Vc5[0]);
   Vc5[1] = vec_nmsub(VbS5, Va[49], Vc5[1]);
   Vc5[2] = vec_nmsub(VbS5, Va[50], Vc5[2]);
   Vc6[0] = vec_nmsub(VbS6, Va[48], Vc6[0]);
   Vc6[1] = vec_nmsub(VbS6, Va[49], Vc6[1]);
   Vc6[2] = vec_nmsub(VbS6, Va[50], Vc6[2]);
   Vc7[0] = vec_nmsub(VbS7, Va[48], Vc7[0]);
   Vc7[1] = vec_nmsub(VbS7, Va[49], Vc7[1]);
   Vc7[2] = vec_nmsub(VbS7, Va[50], Vc7[2]);

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
   Vc0[0] = vec_nmsub(VbS0, Va[44], Vc0[0]);
   Vc0[1] = vec_nmsub(VbS0, Va[45], Vc0[1]);
   Vc1[0] = vec_nmsub(VbS1, Va[44], Vc1[0]);
   Vc1[1] = vec_nmsub(VbS1, Va[45], Vc1[1]);
   Vc2[0] = vec_nmsub(VbS2, Va[44], Vc2[0]);
   Vc2[1] = vec_nmsub(VbS2, Va[45], Vc2[1]);
   Vc3[0] = vec_nmsub(VbS3, Va[44], Vc3[0]);
   Vc3[1] = vec_nmsub(VbS3, Va[45], Vc3[1]);
   Vc4[0] = vec_nmsub(VbS4, Va[44], Vc4[0]);
   Vc4[1] = vec_nmsub(VbS4, Va[45], Vc4[1]);
   Vc5[0] = vec_nmsub(VbS5, Va[44], Vc5[0]);
   Vc5[1] = vec_nmsub(VbS5, Va[45], Vc5[1]);
   Vc6[0] = vec_nmsub(VbS6, Va[44], Vc6[0]);
   Vc6[1] = vec_nmsub(VbS6, Va[45], Vc6[1]);
   Vc7[0] = vec_nmsub(VbS7, Va[44], Vc7[0]);
   Vc7[1] = vec_nmsub(VbS7, Va[45], Vc7[1]);
   c0[ 8] -= b[88] * a[184];
   c0[ 9] -= b[88] * a[185];
   c0[10] -= b[88] * a[186];
   c1[ 8] -= b[89] * a[184];
   c1[ 9] -= b[89] * a[185];
   c1[10] -= b[89] * a[186];
   c2[ 8] -= b[90] * a[184];
   c2[ 9] -= b[90] * a[185];
   c2[10] -= b[90] * a[186];
   c3[ 8] -= b[91] * a[184];
   c3[ 9] -= b[91] * a[185];
   c3[10] -= b[91] * a[186];
   c4[ 8] -= b[92] * a[184];
   c4[ 9] -= b[92] * a[185];
   c4[10] -= b[92] * a[186];
   c5[ 8] -= b[93] * a[184];
   c5[ 9] -= b[93] * a[185];
   c5[10] -= b[93] * a[186];
   c6[ 8] -= b[94] * a[184];
   c6[ 9] -= b[94] * a[185];
   c6[10] -= b[94] * a[186];
   c7[ 8] -= b[95] * a[184];
   c7[ 9] -= b[95] * a[185];
   c7[10] -= b[95] * a[186];

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
   Vc0[0] = vec_nmsub(VbS0, Va[40], Vc0[0]);
   Vc0[1] = vec_nmsub(VbS0, Va[41], Vc0[1]);
   Vc1[0] = vec_nmsub(VbS1, Va[40], Vc1[0]);
   Vc1[1] = vec_nmsub(VbS1, Va[41], Vc1[1]);
   Vc2[0] = vec_nmsub(VbS2, Va[40], Vc2[0]);
   Vc2[1] = vec_nmsub(VbS2, Va[41], Vc2[1]);
   Vc3[0] = vec_nmsub(VbS3, Va[40], Vc3[0]);
   Vc3[1] = vec_nmsub(VbS3, Va[41], Vc3[1]);
   Vc4[0] = vec_nmsub(VbS4, Va[40], Vc4[0]);
   Vc4[1] = vec_nmsub(VbS4, Va[41], Vc4[1]);
   Vc5[0] = vec_nmsub(VbS5, Va[40], Vc5[0]);
   Vc5[1] = vec_nmsub(VbS5, Va[41], Vc5[1]);
   Vc6[0] = vec_nmsub(VbS6, Va[40], Vc6[0]);
   Vc6[1] = vec_nmsub(VbS6, Va[41], Vc6[1]);
   Vc7[0] = vec_nmsub(VbS7, Va[40], Vc7[0]);
   Vc7[1] = vec_nmsub(VbS7, Va[41], Vc7[1]);
   c0[8] -= b[80] * a[168];
   c0[9] -= b[80] * a[169];
   c1[8] -= b[81] * a[168];
   c1[9] -= b[81] * a[169];
   c2[8] -= b[82] * a[168];
   c2[9] -= b[82] * a[169];
   c3[8] -= b[83] * a[168];
   c3[9] -= b[83] * a[169];
   c4[8] -= b[84] * a[168];
   c4[9] -= b[84] * a[169];
   c5[8] -= b[85] * a[168];
   c5[9] -= b[85] * a[169];
   c6[8] -= b[86] * a[168];
   c6[9] -= b[86] * a[169];
   c7[8] -= b[87] * a[168];
   c7[9] -= b[87] * a[169];

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
   Vc0[0] = vec_nmsub(VbS0, Va[36], Vc0[0]);
   Vc0[1] = vec_nmsub(VbS0, Va[37], Vc0[1]);
   Vc1[0] = vec_nmsub(VbS1, Va[36], Vc1[0]);
   Vc1[1] = vec_nmsub(VbS1, Va[37], Vc1[1]);
   Vc2[0] = vec_nmsub(VbS2, Va[36], Vc2[0]);
   Vc2[1] = vec_nmsub(VbS2, Va[37], Vc2[1]);
   Vc3[0] = vec_nmsub(VbS3, Va[36], Vc3[0]);
   Vc3[1] = vec_nmsub(VbS3, Va[37], Vc3[1]);
   Vc4[0] = vec_nmsub(VbS4, Va[36], Vc4[0]);
   Vc4[1] = vec_nmsub(VbS4, Va[37], Vc4[1]);
   Vc5[0] = vec_nmsub(VbS5, Va[36], Vc5[0]);
   Vc5[1] = vec_nmsub(VbS5, Va[37], Vc5[1]);
   Vc6[0] = vec_nmsub(VbS6, Va[36], Vc6[0]);
   Vc6[1] = vec_nmsub(VbS6, Va[37], Vc6[1]);
   Vc7[0] = vec_nmsub(VbS7, Va[36], Vc7[0]);
   Vc7[1] = vec_nmsub(VbS7, Va[37], Vc7[1]);
   c0[8] -= b[72] * a[152];
   c1[8] -= b[73] * a[152];
   c2[8] -= b[74] * a[152];
   c3[8] -= b[75] * a[152];
   c4[8] -= b[76] * a[152];
   c5[8] -= b[77] * a[152];
   c6[8] -= b[78] * a[152];
   c7[8] -= b[79] * a[152];

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
   Vc0[0] = vec_nmsub(VbS0, Va[32], Vc0[0]);
   Vc0[1] = vec_nmsub(VbS0, Va[33], Vc0[1]);
   Vc1[0] = vec_nmsub(VbS1, Va[32], Vc1[0]);
   Vc1[1] = vec_nmsub(VbS1, Va[33], Vc1[1]);
   Vc2[0] = vec_nmsub(VbS2, Va[32], Vc2[0]);
   Vc2[1] = vec_nmsub(VbS2, Va[33], Vc2[1]);
   Vc3[0] = vec_nmsub(VbS3, Va[32], Vc3[0]);
   Vc3[1] = vec_nmsub(VbS3, Va[33], Vc3[1]);
   Vc4[0] = vec_nmsub(VbS4, Va[32], Vc4[0]);
   Vc4[1] = vec_nmsub(VbS4, Va[33], Vc4[1]);
   Vc5[0] = vec_nmsub(VbS5, Va[32], Vc5[0]);
   Vc5[1] = vec_nmsub(VbS5, Va[33], Vc5[1]);
   Vc6[0] = vec_nmsub(VbS6, Va[32], Vc6[0]);
   Vc6[1] = vec_nmsub(VbS6, Va[33], Vc6[1]);
   Vc7[0] = vec_nmsub(VbS7, Va[32], Vc7[0]);
   Vc7[1] = vec_nmsub(VbS7, Va[33], Vc7[1]);

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
   Vc0[0] = vec_nmsub(VbS0, Va[28], Vc0[0]);
   Vc1[0] = vec_nmsub(VbS1, Va[28], Vc1[0]);
   Vc2[0] = vec_nmsub(VbS2, Va[28], Vc2[0]);
   Vc3[0] = vec_nmsub(VbS3, Va[28], Vc3[0]);
   Vc4[0] = vec_nmsub(VbS4, Va[28], Vc4[0]);
   Vc5[0] = vec_nmsub(VbS5, Va[28], Vc5[0]);
   Vc6[0] = vec_nmsub(VbS6, Va[28], Vc6[0]);
   Vc7[0] = vec_nmsub(VbS7, Va[28], Vc7[0]);
   c0[4] -= b[56] * a[116];
   c0[5] -= b[56] * a[117];
   c0[6] -= b[56] * a[118];
   c1[4] -= b[57] * a[116];
   c1[5] -= b[57] * a[117];
   c1[6] -= b[57] * a[118];
   c2[4] -= b[58] * a[116];
   c2[5] -= b[58] * a[117];
   c2[6] -= b[58] * a[118];
   c3[4] -= b[59] * a[116];
   c3[5] -= b[59] * a[117];
   c3[6] -= b[59] * a[118];
   c4[4] -= b[60] * a[116];
   c4[5] -= b[60] * a[117];
   c4[6] -= b[60] * a[118];
   c5[4] -= b[61] * a[116];
   c5[5] -= b[61] * a[117];
   c5[6] -= b[61] * a[118];
   c6[4] -= b[62] * a[116];
   c6[5] -= b[62] * a[117];
   c6[6] -= b[62] * a[118];
   c7[4] -= b[63] * a[116];
   c7[5] -= b[63] * a[117];
   c7[6] -= b[63] * a[118];

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
   Vc0[0] = vec_nmsub(VbS0, Va[24], Vc0[0]);
   Vc1[0] = vec_nmsub(VbS1, Va[24], Vc1[0]);
   Vc2[0] = vec_nmsub(VbS2, Va[24], Vc2[0]);
   Vc3[0] = vec_nmsub(VbS3, Va[24], Vc3[0]);
   Vc4[0] = vec_nmsub(VbS4, Va[24], Vc4[0]);
   Vc5[0] = vec_nmsub(VbS5, Va[24], Vc5[0]);
   Vc6[0] = vec_nmsub(VbS6, Va[24], Vc6[0]);
   Vc7[0] = vec_nmsub(VbS7, Va[24], Vc7[0]);
   c0[4] -= b[48] * a[100];
   c0[5] -= b[48] * a[101];
   c1[4] -= b[49] * a[100];
   c1[5] -= b[49] * a[101];
   c2[4] -= b[50] * a[100];
   c2[5] -= b[50] * a[101];
   c3[4] -= b[51] * a[100];
   c3[5] -= b[51] * a[101];
   c4[4] -= b[52] * a[100];
   c4[5] -= b[52] * a[101];
   c5[4] -= b[53] * a[100];
   c5[5] -= b[53] * a[101];
   c6[4] -= b[54] * a[100];
   c6[5] -= b[54] * a[101];
   c7[4] -= b[55] * a[100];
   c7[5] -= b[55] * a[101];

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
   Vc0[0] = vec_nmsub(VbS0, Va[20], Vc0[0]);
   Vc1[0] = vec_nmsub(VbS1, Va[20], Vc1[0]);
   Vc2[0] = vec_nmsub(VbS2, Va[20], Vc2[0]);
   Vc3[0] = vec_nmsub(VbS3, Va[20], Vc3[0]);
   Vc4[0] = vec_nmsub(VbS4, Va[20], Vc4[0]);
   Vc5[0] = vec_nmsub(VbS5, Va[20], Vc5[0]);
   Vc6[0] = vec_nmsub(VbS6, Va[20], Vc6[0]);
   Vc7[0] = vec_nmsub(VbS7, Va[20], Vc7[0]);
   c0[4] -= b[40] * a[84];
   c1[4] -= b[41] * a[84];
   c2[4] -= b[42] * a[84];
   c3[4] -= b[43] * a[84];
   c4[4] -= b[44] * a[84];
   c5[4] -= b[45] * a[84];
   c6[4] -= b[46] * a[84];
   c7[4] -= b[47] * a[84];

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
   Vc0[0] = vec_nmsub(VbS0, Va[16], Vc0[0]);
   Vc1[0] = vec_nmsub(VbS1, Va[16], Vc1[0]);
   Vc2[0] = vec_nmsub(VbS2, Va[16], Vc2[0]);
   Vc3[0] = vec_nmsub(VbS3, Va[16], Vc3[0]);
   Vc4[0] = vec_nmsub(VbS4, Va[16], Vc4[0]);
   Vc5[0] = vec_nmsub(VbS5, Va[16], Vc5[0]);
   Vc6[0] = vec_nmsub(VbS6, Va[16], Vc6[0]);
   Vc7[0] = vec_nmsub(VbS7, Va[16], Vc7[0]);

   b[24] = (c0[3] *= a[51]);
   b[25] = (c1[3] *= a[51]);
   b[26] = (c2[3] *= a[51]);
   b[27] = (c3[3] *= a[51]);
   b[28] = (c4[3] *= a[51]);
   b[29] = (c5[3] *= a[51]);
   b[30] = (c6[3] *= a[51]);
   b[31] = (c7[3] *= a[51]);
   c0[0] -= b[24] * a[48];
   c0[1] -= b[24] * a[49];
   c0[2] -= b[24] * a[50];
   c1[0] -= b[25] * a[48];
   c1[1] -= b[25] * a[49];
   c1[2] -= b[25] * a[50];
   c2[0] -= b[26] * a[48];
   c2[1] -= b[26] * a[49];
   c2[2] -= b[26] * a[50];
   c3[0] -= b[27] * a[48];
   c3[1] -= b[27] * a[49];
   c3[2] -= b[27] * a[50];
   c4[0] -= b[28] * a[48];
   c4[1] -= b[28] * a[49];
   c4[2] -= b[28] * a[50];
   c5[0] -= b[29] * a[48];
   c5[1] -= b[29] * a[49];
   c5[2] -= b[29] * a[50];
   c6[0] -= b[30] * a[48];
   c6[1] -= b[30] * a[49];
   c6[2] -= b[30] * a[50];
   c7[0] -= b[31] * a[48];
   c7[1] -= b[31] * a[49];
   c7[2] -= b[31] * a[50];

   b[16] = (c0[2] *= a[34]);
   b[17] = (c1[2] *= a[34]);
   b[18] = (c2[2] *= a[34]);
   b[19] = (c3[2] *= a[34]);
   b[20] = (c4[2] *= a[34]);
   b[21] = (c5[2] *= a[34]);
   b[22] = (c6[2] *= a[34]);
   b[23] = (c7[2] *= a[34]);
   c0[0] -= b[16] * a[32];
   c0[1] -= b[16] * a[33];
   c1[0] -= b[17] * a[32];
   c1[1] -= b[17] * a[33];
   c2[0] -= b[18] * a[32];
   c2[1] -= b[18] * a[33];
   c3[0] -= b[19] * a[32];
   c3[1] -= b[19] * a[33];
   c4[0] -= b[20] * a[32];
   c4[1] -= b[20] * a[33];
   c5[0] -= b[21] * a[32];
   c5[1] -= b[21] * a[33];
   c6[0] -= b[22] * a[32];
   c6[1] -= b[22] * a[33];
   c7[0] -= b[23] * a[32];
   c7[1] -= b[23] * a[33];

   b[ 8] = (c0[1] *= a[17]);
   b[ 9] = (c1[1] *= a[17]);
   b[10] = (c2[1] *= a[17]);
   b[11] = (c3[1] *= a[17]);
   b[12] = (c4[1] *= a[17]);
   b[13] = (c5[1] *= a[17]);
   b[14] = (c6[1] *= a[17]);
   b[15] = (c7[1] *= a[17]);
   c0[0] -= b[ 8] * a[16];
   c1[0] -= b[ 9] * a[16];
   c2[0] -= b[10] * a[16];
   c3[0] -= b[11] * a[16];
   c4[0] -= b[12] * a[16];
   c5[0] -= b[13] * a[16];
   c6[0] -= b[14] * a[16];
   c7[0] -= b[15] * a[16];

   b[0] = (c0[0] *= a[0]);
   b[1] = (c1[0] *= a[0]);
   b[2] = (c2[0] *= a[0]);
   b[3] = (c3[0] *= a[0]);
   b[4] = (c4[0] *= a[0]);
   b[5] = (c5[0] *= a[0]);
   b[6] = (c6[0] *= a[0]);
   b[7] = (c7[0] *= a[0]);
}

#endif

static inline __attribute__ ((always_inline)) void solve(BLASLONG m, BLASLONG n, FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc) {

  FLOAT aa,  bb;

  int i, j, k;

  a += (m - 1) * m;
  b += (m - 1) * n;

  for (i = m - 1; i >= 0; i--) {

    aa = *(a + i);

    for (j = 0; j < n; j ++) {
      bb = *(c + i + j * ldc);
      bb *= aa;
      *b             = bb;
      *(c + i + j * ldc) = bb;
      b ++;

      for (k = 0; k < i; k ++){
	*(c + k + j * ldc) -= bb * *(a + k);
      }

    }
    a -= m;
    b -= 2 * n;
  }

}

#else

static inline __attribute__ ((always_inline)) void solve(BLASLONG m, BLASLONG n, FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc) {

  FLOAT aa1, aa2;
  FLOAT bb1, bb2;
  FLOAT cc1, cc2;

  int i, j, k;

  ldc *= 2;
  a += (m - 1) * m * 2;
  b += (m - 1) * n * 2;

  for (i = m - 1; i >= 0; i--) {

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

      for (k = 0; k < i; k ++){
#ifndef CONJ
	*(c + k * 2 + 0 + j * ldc) -= cc1 * *(a + k * 2 + 0) - cc2 * *(a + k * 2 + 1);
	*(c + k * 2 + 1 + j * ldc) -= cc1 * *(a + k * 2 + 1) + cc2 * *(a + k * 2 + 0);
#else
	*(c + k * 2 + 0 + j * ldc) -=   cc1 * *(a + k * 2 + 0) + cc2 * *(a + k * 2 + 1);
	*(c + k * 2 + 1 + j * ldc) -= - cc1 * *(a + k * 2 + 1) + cc2 * *(a + k * 2 + 0);
#endif
      }

    }
    a -= m * 2;
    b -= 4 * n;
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
  fprintf(stderr, "TRSM KERNEL LN : m = %3ld  n = %3ld  k = %3ld offset = %3ld\n",
	  m, n, k, offset);
#endif

#ifdef DOUBLE
  int well_aligned = (GEMM_UNROLL_M==8) && (GEMM_UNROLL_N==8) && ((((unsigned long) a) & 0x7) == 0);
#else
  int well_aligned = (GEMM_UNROLL_M==16) && (GEMM_UNROLL_N==8) && ((((unsigned long) a) & 0x7) == 0);
#endif

  j = (n >> GEMM_UNROLL_N_SHIFT);

  while (j > 0) {

    kk = m + offset;

    if (m & (GEMM_UNROLL_M - 1)) {
      for (i = 1; i < GEMM_UNROLL_M; i *= 2){
	if (m & i) {
	  aa = a + ((m & ~(i - 1)) - i) * k * COMPSIZE;
	  cc = c + ((m & ~(i - 1)) - i)     * COMPSIZE;

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
		aa + (kk - i) * i             * COMPSIZE,
		b  + (kk - i) * GEMM_UNROLL_N * COMPSIZE,
		cc, ldc);

	  kk -= i;
	}
      }
    }

    i = (m >> GEMM_UNROLL_M_SHIFT);
    if (i > 0) {
      aa = a + ((m & ~(GEMM_UNROLL_M - 1)) - GEMM_UNROLL_M) * k * COMPSIZE;
      cc = c + ((m & ~(GEMM_UNROLL_M - 1)) - GEMM_UNROLL_M)     * COMPSIZE;

      do {
	if (k - kk > 0) {
	  GEMM_KERNEL(GEMM_UNROLL_M, GEMM_UNROLL_N, k - kk, dm1,
#ifdef COMPLEX
		      ZERO,
#endif
		      aa + GEMM_UNROLL_M * kk * COMPSIZE,
		      b +  GEMM_UNROLL_N * kk * COMPSIZE,
		      cc,
		      ldc);
	}

	if (well_aligned) {
#ifdef DOUBLE
	  solve8x8(aa + (kk - GEMM_UNROLL_M) * GEMM_UNROLL_M * COMPSIZE,
	           b  + (kk - GEMM_UNROLL_M) * GEMM_UNROLL_N * COMPSIZE, cc, ldc);
#else
	  solve16x8(aa + (kk - GEMM_UNROLL_M) * GEMM_UNROLL_M * COMPSIZE,
	           b  + (kk - GEMM_UNROLL_M) * GEMM_UNROLL_N * COMPSIZE, cc, ldc);
#endif
	}
	else {
	solve(GEMM_UNROLL_M, GEMM_UNROLL_N,
	      aa + (kk - GEMM_UNROLL_M) * GEMM_UNROLL_M * COMPSIZE,
	      b  + (kk - GEMM_UNROLL_M) * GEMM_UNROLL_N * COMPSIZE,
	      cc, ldc);
	}

	aa -= GEMM_UNROLL_M * k * COMPSIZE;
	cc -= GEMM_UNROLL_M     * COMPSIZE;
	kk -= GEMM_UNROLL_M;
	i --;
      } while (i > 0);
    }

    b += GEMM_UNROLL_N * k * COMPSIZE;
    c += GEMM_UNROLL_N * ldc * COMPSIZE;
    j --;
  }

  if (n & (GEMM_UNROLL_N - 1)) {

    j = (GEMM_UNROLL_N >> 1);
    while (j > 0) {
      if (n & j) {

	kk = m + offset;

	if (m & (GEMM_UNROLL_M - 1)) {
	  for (i = 1; i < GEMM_UNROLL_M; i *= 2){
	    if (m & i) {
	      aa = a + ((m & ~(i - 1)) - i) * k * COMPSIZE;
	      cc = c + ((m & ~(i - 1)) - i)     * COMPSIZE;

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
		    aa + (kk - i) * i * COMPSIZE,
		    b  + (kk - i) * j * COMPSIZE,
		    cc, ldc);

	      kk -= i;
	    }
	  }
	}

	i = (m >> GEMM_UNROLL_M_SHIFT);
	if (i > 0) {
	  aa = a + ((m & ~(GEMM_UNROLL_M - 1)) - GEMM_UNROLL_M) * k * COMPSIZE;
	  cc = c + ((m & ~(GEMM_UNROLL_M - 1)) - GEMM_UNROLL_M)     * COMPSIZE;

	  do {
	    if (k - kk > 0) {
	      GEMM_KERNEL(GEMM_UNROLL_M, j, k - kk, dm1,
#ifdef COMPLEX
			  ZERO,
#endif
			  aa + GEMM_UNROLL_M * kk * COMPSIZE,
			  b +  j             * kk * COMPSIZE,
			  cc,
			  ldc);
	    }

	    solve(GEMM_UNROLL_M, j,
		  aa + (kk - GEMM_UNROLL_M) * GEMM_UNROLL_M * COMPSIZE,
		  b  + (kk - GEMM_UNROLL_M) * j             * COMPSIZE,
		  cc, ldc);

	    aa -= GEMM_UNROLL_M * k * COMPSIZE;
	    cc -= GEMM_UNROLL_M     * COMPSIZE;
	    kk -= GEMM_UNROLL_M;
	    i --;
	  } while (i > 0);
	}

	b += j * k   * COMPSIZE;
	c += j * ldc * COMPSIZE;
      }
      j >>= 1;
    }
  }

  return 0;
}
