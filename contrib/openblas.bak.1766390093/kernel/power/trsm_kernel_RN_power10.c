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

   a[0] = (c0[0] *= b[0]);
   a[1] = (c0[1] *= b[0]);
   a[2] = (c0[2] *= b[0]);
   a[3] = (c0[3] *= b[0]);
   a[4] = (c0[4] *= b[0]);
   a[5] = (c0[5] *= b[0]);
   a[6] = (c0[6] *= b[0]);
   a[7] = (c0[7] *= b[0]);
   VbS0 = vec_splat(Vb[0], 1);
   VbS1 = vec_splat(Vb[1], 0);
   VbS2 = vec_splat(Vb[1], 1);
   VbS3 = vec_splat(Vb[2], 0);
   VbS4 = vec_splat(Vb[2], 1);
   VbS5 = vec_splat(Vb[3], 0);
   VbS6 = vec_splat(Vb[3], 1);
   Vc1[0] = vec_nmsub(Vc0[ 0], VbS0, Vc1[0]);
   Vc1[1] = vec_nmsub(Vc0[ 1], VbS0, Vc1[1]);
   Vc1[2] = vec_nmsub(Vc0[ 2], VbS0, Vc1[2]);
   Vc1[3] = vec_nmsub(Vc0[ 3], VbS0, Vc1[3]);
   Vc2[0] = vec_nmsub(Vc0[ 0], VbS1, Vc2[0]);
   Vc2[1] = vec_nmsub(Vc0[ 1], VbS1, Vc2[1]);
   Vc2[2] = vec_nmsub(Vc0[ 2], VbS1, Vc2[2]);
   Vc2[3] = vec_nmsub(Vc0[ 3], VbS1, Vc2[3]);
   Vc3[0] = vec_nmsub(Vc0[ 0], VbS2, Vc3[0]);
   Vc3[1] = vec_nmsub(Vc0[ 1], VbS2, Vc3[1]);
   Vc3[2] = vec_nmsub(Vc0[ 2], VbS2, Vc3[2]);
   Vc3[3] = vec_nmsub(Vc0[ 3], VbS2, Vc3[3]);
   Vc4[0] = vec_nmsub(Vc0[ 0], VbS3, Vc4[0]);
   Vc4[1] = vec_nmsub(Vc0[ 1], VbS3, Vc4[1]);
   Vc4[2] = vec_nmsub(Vc0[ 2], VbS3, Vc4[2]);
   Vc4[3] = vec_nmsub(Vc0[ 3], VbS3, Vc4[3]);
   Vc5[0] = vec_nmsub(Vc0[ 0], VbS4, Vc5[0]);
   Vc5[1] = vec_nmsub(Vc0[ 1], VbS4, Vc5[1]);
   Vc5[2] = vec_nmsub(Vc0[ 2], VbS4, Vc5[2]);
   Vc5[3] = vec_nmsub(Vc0[ 3], VbS4, Vc5[3]);
   Vc6[0] = vec_nmsub(Vc0[ 0], VbS5, Vc6[0]);
   Vc6[1] = vec_nmsub(Vc0[ 1], VbS5, Vc6[1]);
   Vc6[2] = vec_nmsub(Vc0[ 2], VbS5, Vc6[2]);
   Vc6[3] = vec_nmsub(Vc0[ 3], VbS5, Vc6[3]);
   Vc7[0] = vec_nmsub(Vc0[ 0], VbS6, Vc7[0]);
   Vc7[1] = vec_nmsub(Vc0[ 1], VbS6, Vc7[1]);
   Vc7[2] = vec_nmsub(Vc0[ 2], VbS6, Vc7[2]);
   Vc7[3] = vec_nmsub(Vc0[ 3], VbS6, Vc7[3]);

   a[ 8] = (c1[0] *= b[9]);
   a[ 9] = (c1[1] *= b[9]);
   a[10] = (c1[2] *= b[9]);
   a[11] = (c1[3] *= b[9]);
   a[12] = (c1[4] *= b[9]);
   a[13] = (c1[5] *= b[9]);
   a[14] = (c1[6] *= b[9]);
   a[15] = (c1[7] *= b[9]);
   VbS0 = vec_splat(Vb[5], 0);
   VbS1 = vec_splat(Vb[5], 1);
   VbS2 = vec_splat(Vb[6], 0);
   VbS3 = vec_splat(Vb[6], 1);
   VbS4 = vec_splat(Vb[7], 0);
   VbS5 = vec_splat(Vb[7], 1);
   Vc2[0] = vec_nmsub(Vc1[0], VbS0, Vc2[0]);
   Vc2[1] = vec_nmsub(Vc1[1], VbS0, Vc2[1]);
   Vc2[2] = vec_nmsub(Vc1[2], VbS0, Vc2[2]);
   Vc2[3] = vec_nmsub(Vc1[3], VbS0, Vc2[3]);
   Vc3[0] = vec_nmsub(Vc1[0], VbS1, Vc3[0]);
   Vc3[1] = vec_nmsub(Vc1[1], VbS1, Vc3[1]);
   Vc3[2] = vec_nmsub(Vc1[2], VbS1, Vc3[2]);
   Vc3[3] = vec_nmsub(Vc1[3], VbS1, Vc3[3]);
   Vc4[0] = vec_nmsub(Vc1[0], VbS2, Vc4[0]);
   Vc4[1] = vec_nmsub(Vc1[1], VbS2, Vc4[1]);
   Vc4[2] = vec_nmsub(Vc1[2], VbS2, Vc4[2]);
   Vc4[3] = vec_nmsub(Vc1[3], VbS2, Vc4[3]);
   Vc5[0] = vec_nmsub(Vc1[0], VbS3, Vc5[0]);
   Vc5[1] = vec_nmsub(Vc1[1], VbS3, Vc5[1]);
   Vc5[2] = vec_nmsub(Vc1[2], VbS3, Vc5[2]);
   Vc5[3] = vec_nmsub(Vc1[3], VbS3, Vc5[3]);
   Vc6[0] = vec_nmsub(Vc1[0], VbS4, Vc6[0]);
   Vc6[1] = vec_nmsub(Vc1[1], VbS4, Vc6[1]);
   Vc6[2] = vec_nmsub(Vc1[2], VbS4, Vc6[2]);
   Vc6[3] = vec_nmsub(Vc1[3], VbS4, Vc6[3]);
   Vc7[0] = vec_nmsub(Vc1[0], VbS5, Vc7[0]);
   Vc7[1] = vec_nmsub(Vc1[1], VbS5, Vc7[1]);
   Vc7[2] = vec_nmsub(Vc1[2], VbS5, Vc7[2]);
   Vc7[3] = vec_nmsub(Vc1[3], VbS5, Vc7[3]);

   a[16] = (c2[0] *= b[18]);
   a[17] = (c2[1] *= b[18]);
   a[18] = (c2[2] *= b[18]);
   a[19] = (c2[3] *= b[18]);
   a[20] = (c2[4] *= b[18]);
   a[21] = (c2[5] *= b[18]);
   a[22] = (c2[6] *= b[18]);
   a[23] = (c2[7] *= b[18]);
   VbS0 = vec_splat(Vb[ 9], 1);
   VbS1 = vec_splat(Vb[10], 0);
   VbS2 = vec_splat(Vb[10], 1);
   VbS3 = vec_splat(Vb[11], 0);
   VbS4 = vec_splat(Vb[11], 1);
   Vc3[0] = vec_nmsub(Vc2[0], VbS0, Vc3[0]);
   Vc3[1] = vec_nmsub(Vc2[1], VbS0, Vc3[1]);
   Vc3[2] = vec_nmsub(Vc2[2], VbS0, Vc3[2]);
   Vc3[3] = vec_nmsub(Vc2[3], VbS0, Vc3[3]);
   Vc4[0] = vec_nmsub(Vc2[0], VbS1, Vc4[0]);
   Vc4[1] = vec_nmsub(Vc2[1], VbS1, Vc4[1]);
   Vc4[2] = vec_nmsub(Vc2[2], VbS1, Vc4[2]);
   Vc4[3] = vec_nmsub(Vc2[3], VbS1, Vc4[3]);
   Vc5[0] = vec_nmsub(Vc2[0], VbS2, Vc5[0]);
   Vc5[1] = vec_nmsub(Vc2[1], VbS2, Vc5[1]);
   Vc5[2] = vec_nmsub(Vc2[2], VbS2, Vc5[2]);
   Vc5[3] = vec_nmsub(Vc2[3], VbS2, Vc5[3]);
   Vc6[0] = vec_nmsub(Vc2[0], VbS3, Vc6[0]);
   Vc6[1] = vec_nmsub(Vc2[1], VbS3, Vc6[1]);
   Vc6[2] = vec_nmsub(Vc2[2], VbS3, Vc6[2]);
   Vc6[3] = vec_nmsub(Vc2[3], VbS3, Vc6[3]);
   Vc7[0] = vec_nmsub(Vc2[0], VbS4, Vc7[0]);
   Vc7[1] = vec_nmsub(Vc2[1], VbS4, Vc7[1]);
   Vc7[2] = vec_nmsub(Vc2[2], VbS4, Vc7[2]);
   Vc7[3] = vec_nmsub(Vc2[3], VbS4, Vc7[3]);

   a[24] = (c3[0] *= b[27]);
   a[25] = (c3[1] *= b[27]);
   a[26] = (c3[2] *= b[27]);
   a[27] = (c3[3] *= b[27]);
   a[28] = (c3[4] *= b[27]);
   a[29] = (c3[5] *= b[27]);
   a[30] = (c3[6] *= b[27]);
   a[31] = (c3[7] *= b[27]);
   VbS0 = vec_splat(Vb[14], 0);
   VbS1 = vec_splat(Vb[14], 1);
   VbS2 = vec_splat(Vb[15], 0);
   VbS3 = vec_splat(Vb[15], 1);
   Vc4[0] = vec_nmsub(Vc3[0], VbS0, Vc4[0]);
   Vc4[1] = vec_nmsub(Vc3[1], VbS0, Vc4[1]);
   Vc4[2] = vec_nmsub(Vc3[2], VbS0, Vc4[2]);
   Vc4[3] = vec_nmsub(Vc3[3], VbS0, Vc4[3]);
   Vc5[0] = vec_nmsub(Vc3[0], VbS1, Vc5[0]);
   Vc5[1] = vec_nmsub(Vc3[1], VbS1, Vc5[1]);
   Vc5[2] = vec_nmsub(Vc3[2], VbS1, Vc5[2]);
   Vc5[3] = vec_nmsub(Vc3[3], VbS1, Vc5[3]);
   Vc6[0] = vec_nmsub(Vc3[0], VbS2, Vc6[0]);
   Vc6[1] = vec_nmsub(Vc3[1], VbS2, Vc6[1]);
   Vc6[2] = vec_nmsub(Vc3[2], VbS2, Vc6[2]);
   Vc6[3] = vec_nmsub(Vc3[3], VbS2, Vc6[3]);
   Vc7[0] = vec_nmsub(Vc3[0], VbS3, Vc7[0]);
   Vc7[1] = vec_nmsub(Vc3[1], VbS3, Vc7[1]);
   Vc7[2] = vec_nmsub(Vc3[2], VbS3, Vc7[2]);
   Vc7[3] = vec_nmsub(Vc3[3], VbS3, Vc7[3]);

   a[32] = (c4[0] *= b[36]);
   a[33] = (c4[1] *= b[36]);
   a[34] = (c4[2] *= b[36]);
   a[35] = (c4[3] *= b[36]);
   a[36] = (c4[4] *= b[36]);
   a[37] = (c4[5] *= b[36]);
   a[38] = (c4[6] *= b[36]);
   a[39] = (c4[7] *= b[36]);
   VbS0 = vec_splat(Vb[18], 1);
   VbS1 = vec_splat(Vb[19], 0);
   VbS2 = vec_splat(Vb[19], 1);
   Vc5[0] = vec_nmsub(Vc4[0], VbS0, Vc5[0]);
   Vc5[1] = vec_nmsub(Vc4[1], VbS0, Vc5[1]);
   Vc5[2] = vec_nmsub(Vc4[2], VbS0, Vc5[2]);
   Vc5[3] = vec_nmsub(Vc4[3], VbS0, Vc5[3]);
   Vc6[0] = vec_nmsub(Vc4[0], VbS1, Vc6[0]);
   Vc6[1] = vec_nmsub(Vc4[1], VbS1, Vc6[1]);
   Vc6[2] = vec_nmsub(Vc4[2], VbS1, Vc6[2]);
   Vc6[3] = vec_nmsub(Vc4[3], VbS1, Vc6[3]);
   Vc7[0] = vec_nmsub(Vc4[0], VbS2, Vc7[0]);
   Vc7[1] = vec_nmsub(Vc4[1], VbS2, Vc7[1]);
   Vc7[2] = vec_nmsub(Vc4[2], VbS2, Vc7[2]);
   Vc7[3] = vec_nmsub(Vc4[3], VbS2, Vc7[3]);

   a[40] = (c5[0] *= b[45]);
   a[41] = (c5[1] *= b[45]);
   a[42] = (c5[2] *= b[45]);
   a[43] = (c5[3] *= b[45]);
   a[44] = (c5[4] *= b[45]);
   a[45] = (c5[5] *= b[45]);
   a[46] = (c5[6] *= b[45]);
   a[47] = (c5[7] *= b[45]);
   VbS0 = vec_splat(Vb[23], 0);
   VbS1 = vec_splat(Vb[23], 1);
   Vc6[0] = vec_nmsub(Vc5[0], VbS0, Vc6[0]);
   Vc6[1] = vec_nmsub(Vc5[1], VbS0, Vc6[1]);
   Vc6[2] = vec_nmsub(Vc5[2], VbS0, Vc6[2]);
   Vc6[3] = vec_nmsub(Vc5[3], VbS0, Vc6[3]);
   Vc7[0] = vec_nmsub(Vc5[0], VbS1, Vc7[0]);
   Vc7[1] = vec_nmsub(Vc5[1], VbS1, Vc7[1]);
   Vc7[2] = vec_nmsub(Vc5[2], VbS1, Vc7[2]);
   Vc7[3] = vec_nmsub(Vc5[3], VbS1, Vc7[3]);

   a[48] = (c6[0] *= b[54]);
   a[49] = (c6[1] *= b[54]);
   a[50] = (c6[2] *= b[54]);
   a[51] = (c6[3] *= b[54]);
   a[52] = (c6[4] *= b[54]);
   a[53] = (c6[5] *= b[54]);
   a[54] = (c6[6] *= b[54]);
   a[55] = (c6[7] *= b[54]);
   VbS0 = vec_splat(Vb[27], 1);
   Vc7[0] = vec_nmsub(Vc6[0], VbS0, Vc7[0]);
   Vc7[1] = vec_nmsub(Vc6[1], VbS0, Vc7[1]);
   Vc7[2] = vec_nmsub(Vc6[2], VbS0, Vc7[2]);
   Vc7[3] = vec_nmsub(Vc6[3], VbS0, Vc7[3]);

   a[56] = (c7[0] *= b[63]);
   a[57] = (c7[1] *= b[63]);
   a[58] = (c7[2] *= b[63]);
   a[59] = (c7[3] *= b[63]);
   a[60] = (c7[4] *= b[63]);
   a[61] = (c7[5] *= b[63]);
   a[62] = (c7[6] *= b[63]);
   a[63] = (c7[7] *= b[63]);
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

   VbS0 = vec_splat(Vb[0], 0);
   VbS1 = vec_splat(Vb[0], 1);
   VbS2 = vec_splat(Vb[0], 2);
   VbS3 = vec_splat(Vb[0], 3);
   VbS4 = vec_splat(Vb[1], 0);
   VbS5 = vec_splat(Vb[1], 1);
   VbS6 = vec_splat(Vb[1], 2);
   VbS7 = vec_splat(Vb[1], 3);
   
   Vc0[ 0] = vec_mul(VbS0, Vc0[ 0]);
   Vc0[ 1] = vec_mul(VbS0, Vc0[ 1]);
   Vc0[ 2] = vec_mul(VbS0, Vc0[ 2]);
   Vc0[ 3] = vec_mul(VbS0, Vc0[ 3]);
   Va[0] = Vc0[0];
   Va[1] = Vc0[1];
   Va[2] = Vc0[2];
   Va[3] = Vc0[3];
   Vc1[0] = vec_nmsub(VbS1, Va[0], Vc1[0]);
   Vc1[1] = vec_nmsub(VbS1, Va[1], Vc1[1]);
   Vc1[2] = vec_nmsub(VbS1, Va[2], Vc1[2]);
   Vc1[3] = vec_nmsub(VbS1, Va[3], Vc1[3]);
   Vc2[0] = vec_nmsub(VbS2, Va[0], Vc2[0]);
   Vc2[1] = vec_nmsub(VbS2, Va[1], Vc2[1]);
   Vc2[2] = vec_nmsub(VbS2, Va[2], Vc2[2]);
   Vc2[3] = vec_nmsub(VbS2, Va[3], Vc2[3]);
   Vc3[0] = vec_nmsub(VbS3, Va[0], Vc3[0]);
   Vc3[1] = vec_nmsub(VbS3, Va[1], Vc3[1]);
   Vc3[2] = vec_nmsub(VbS3, Va[2], Vc3[2]);
   Vc3[3] = vec_nmsub(VbS3, Va[3], Vc3[3]);
   Vc4[0] = vec_nmsub(VbS4, Va[0], Vc4[0]);
   Vc4[1] = vec_nmsub(VbS4, Va[1], Vc4[1]);
   Vc4[2] = vec_nmsub(VbS4, Va[2], Vc4[2]);
   Vc4[3] = vec_nmsub(VbS4, Va[3], Vc4[3]);
   Vc5[0] = vec_nmsub(VbS5, Va[0], Vc5[0]);
   Vc5[1] = vec_nmsub(VbS5, Va[1], Vc5[1]);
   Vc5[2] = vec_nmsub(VbS5, Va[2], Vc5[2]);
   Vc5[3] = vec_nmsub(VbS5, Va[3], Vc5[3]);
   Vc6[0] = vec_nmsub(VbS6, Va[0], Vc6[0]);
   Vc6[1] = vec_nmsub(VbS6, Va[1], Vc6[1]);
   Vc6[2] = vec_nmsub(VbS6, Va[2], Vc6[2]);
   Vc6[3] = vec_nmsub(VbS6, Va[3], Vc6[3]);
   Vc7[0] = vec_nmsub(VbS7, Va[0], Vc7[0]);
   Vc7[1] = vec_nmsub(VbS7, Va[1], Vc7[1]);
   Vc7[2] = vec_nmsub(VbS7, Va[2], Vc7[2]);
   Vc7[3] = vec_nmsub(VbS7, Va[3], Vc7[3]);

   VbS0 = vec_splat(Vb[2], 1);
   VbS1 = vec_splat(Vb[2], 2);
   VbS2 = vec_splat(Vb[2], 3);
   VbS3 = vec_splat(Vb[3], 0);
   VbS4 = vec_splat(Vb[3], 1);
   VbS5 = vec_splat(Vb[3], 2);
   VbS6 = vec_splat(Vb[3], 3);
   
   Vc1[0] = vec_mul(VbS0, Vc1[0]);
   Vc1[1] = vec_mul(VbS0, Vc1[1]);
   Vc1[2] = vec_mul(VbS0, Vc1[2]);
   Vc1[3] = vec_mul(VbS0, Vc1[3]);
   Va[4] = Vc1[0];
   Va[5] = Vc1[1];
   Va[6] = Vc1[2];
   Va[7] = Vc1[3];
   Vc2[0] = vec_nmsub(VbS1, Va[4], Vc2[0]);
   Vc2[1] = vec_nmsub(VbS1, Va[5], Vc2[1]);
   Vc2[2] = vec_nmsub(VbS1, Va[6], Vc2[2]);
   Vc2[3] = vec_nmsub(VbS1, Va[7], Vc2[3]);
   Vc3[0] = vec_nmsub(VbS2, Va[4], Vc3[0]);
   Vc3[1] = vec_nmsub(VbS2, Va[5], Vc3[1]);
   Vc3[2] = vec_nmsub(VbS2, Va[6], Vc3[2]);
   Vc3[3] = vec_nmsub(VbS2, Va[7], Vc3[3]);
   Vc4[0] = vec_nmsub(VbS3, Va[4], Vc4[0]);
   Vc4[1] = vec_nmsub(VbS3, Va[5], Vc4[1]);
   Vc4[2] = vec_nmsub(VbS3, Va[6], Vc4[2]);
   Vc4[3] = vec_nmsub(VbS3, Va[7], Vc4[3]);
   Vc5[0] = vec_nmsub(VbS4, Va[4], Vc5[0]);
   Vc5[1] = vec_nmsub(VbS4, Va[5], Vc5[1]);
   Vc5[2] = vec_nmsub(VbS4, Va[6], Vc5[2]);
   Vc5[3] = vec_nmsub(VbS4, Va[7], Vc5[3]);
   Vc6[0] = vec_nmsub(VbS5, Va[4], Vc6[0]);
   Vc6[1] = vec_nmsub(VbS5, Va[5], Vc6[1]);
   Vc6[2] = vec_nmsub(VbS5, Va[6], Vc6[2]);
   Vc6[3] = vec_nmsub(VbS5, Va[7], Vc6[3]);
   Vc7[0] = vec_nmsub(VbS6, Va[4], Vc7[0]);
   Vc7[1] = vec_nmsub(VbS6, Va[5], Vc7[1]);
   Vc7[2] = vec_nmsub(VbS6, Va[6], Vc7[2]);
   Vc7[3] = vec_nmsub(VbS6, Va[7], Vc7[3]);

   VbS0 = vec_splat(Vb[4], 2);
   VbS1 = vec_splat(Vb[4], 3);
   VbS2 = vec_splat(Vb[5], 0);
   VbS3 = vec_splat(Vb[5], 1);
   VbS4 = vec_splat(Vb[5], 2);
   VbS5 = vec_splat(Vb[5], 3);
   
   Vc2[0] = vec_mul(VbS0, Vc2[0]);
   Vc2[1] = vec_mul(VbS0, Vc2[1]);
   Vc2[2] = vec_mul(VbS0, Vc2[2]);
   Vc2[3] = vec_mul(VbS0, Vc2[3]);
   Va[ 8] = Vc2[0];
   Va[ 9] = Vc2[1];
   Va[10] = Vc2[2];
   Va[11] = Vc2[3];
   Vc3[0] = vec_nmsub(VbS1, Va[ 8], Vc3[0]);
   Vc3[1] = vec_nmsub(VbS1, Va[ 9], Vc3[1]);
   Vc3[2] = vec_nmsub(VbS1, Va[10], Vc3[2]);
   Vc3[3] = vec_nmsub(VbS1, Va[11], Vc3[3]);
   Vc4[0] = vec_nmsub(VbS2, Va[ 8], Vc4[0]);
   Vc4[1] = vec_nmsub(VbS2, Va[ 9], Vc4[1]);
   Vc4[2] = vec_nmsub(VbS2, Va[10], Vc4[2]);
   Vc4[3] = vec_nmsub(VbS2, Va[11], Vc4[3]);
   Vc5[0] = vec_nmsub(VbS3, Va[ 8], Vc5[0]);
   Vc5[1] = vec_nmsub(VbS3, Va[ 9], Vc5[1]);
   Vc5[2] = vec_nmsub(VbS3, Va[10], Vc5[2]);
   Vc5[3] = vec_nmsub(VbS3, Va[11], Vc5[3]);
   Vc6[0] = vec_nmsub(VbS4, Va[ 8], Vc6[0]);
   Vc6[1] = vec_nmsub(VbS4, Va[ 9], Vc6[1]);
   Vc6[2] = vec_nmsub(VbS4, Va[10], Vc6[2]);
   Vc6[3] = vec_nmsub(VbS4, Va[11], Vc6[3]);
   Vc7[0] = vec_nmsub(VbS5, Va[ 8], Vc7[0]);
   Vc7[1] = vec_nmsub(VbS5, Va[ 9], Vc7[1]);
   Vc7[2] = vec_nmsub(VbS5, Va[10], Vc7[2]);
   Vc7[3] = vec_nmsub(VbS5, Va[11], Vc7[3]);

   VbS0 = vec_splat(Vb[6], 3);
   VbS1 = vec_splat(Vb[7], 0);
   VbS2 = vec_splat(Vb[7], 1);
   VbS3 = vec_splat(Vb[7], 2);
   VbS4 = vec_splat(Vb[7], 3);
   
   Vc3[0] = vec_mul(VbS0, Vc3[0]);
   Vc3[1] = vec_mul(VbS0, Vc3[1]);
   Vc3[2] = vec_mul(VbS0, Vc3[2]);
   Vc3[3] = vec_mul(VbS0, Vc3[3]);
   Va[12] = Vc3[0];
   Va[13] = Vc3[1];
   Va[14] = Vc3[2];
   Va[15] = Vc3[3];
   Vc4[0] = vec_nmsub(VbS1, Va[12], Vc4[0]);
   Vc4[1] = vec_nmsub(VbS1, Va[13], Vc4[1]);
   Vc4[2] = vec_nmsub(VbS1, Va[14], Vc4[2]);
   Vc4[3] = vec_nmsub(VbS1, Va[15], Vc4[3]);
   Vc5[0] = vec_nmsub(VbS2, Va[12], Vc5[0]);
   Vc5[1] = vec_nmsub(VbS2, Va[13], Vc5[1]);
   Vc5[2] = vec_nmsub(VbS2, Va[14], Vc5[2]);
   Vc5[3] = vec_nmsub(VbS2, Va[15], Vc5[3]);
   Vc6[0] = vec_nmsub(VbS3, Va[12], Vc6[0]);
   Vc6[1] = vec_nmsub(VbS3, Va[13], Vc6[1]);
   Vc6[2] = vec_nmsub(VbS3, Va[14], Vc6[2]);
   Vc6[3] = vec_nmsub(VbS3, Va[15], Vc6[3]);
   Vc7[0] = vec_nmsub(VbS4, Va[12], Vc7[0]);
   Vc7[1] = vec_nmsub(VbS4, Va[13], Vc7[1]);
   Vc7[2] = vec_nmsub(VbS4, Va[14], Vc7[2]);
   Vc7[3] = vec_nmsub(VbS4, Va[15], Vc7[3]);

   VbS0 = vec_splat(Vb[9], 0);
   VbS1 = vec_splat(Vb[9], 1);
   VbS2 = vec_splat(Vb[9], 2);
   VbS3 = vec_splat(Vb[9], 3);
   
   Vc4[0] = vec_mul(VbS0, Vc4[0]);
   Vc4[1] = vec_mul(VbS0, Vc4[1]);
   Vc4[2] = vec_mul(VbS0, Vc4[2]);
   Vc4[3] = vec_mul(VbS0, Vc4[3]);
   Va[16] = Vc4[0];
   Va[17] = Vc4[1];
   Va[18] = Vc4[2];
   Va[19] = Vc4[3];
   Vc5[0] = vec_nmsub(VbS1, Va[16], Vc5[0]);
   Vc5[1] = vec_nmsub(VbS1, Va[17], Vc5[1]);
   Vc5[2] = vec_nmsub(VbS1, Va[18], Vc5[2]);
   Vc5[3] = vec_nmsub(VbS1, Va[19], Vc5[3]);
   Vc6[0] = vec_nmsub(VbS2, Va[16], Vc6[0]);
   Vc6[1] = vec_nmsub(VbS2, Va[17], Vc6[1]);
   Vc6[2] = vec_nmsub(VbS2, Va[18], Vc6[2]);
   Vc6[3] = vec_nmsub(VbS2, Va[19], Vc6[3]);
   Vc7[0] = vec_nmsub(VbS3, Va[16], Vc7[0]);
   Vc7[1] = vec_nmsub(VbS3, Va[17], Vc7[1]);
   Vc7[2] = vec_nmsub(VbS3, Va[18], Vc7[2]);
   Vc7[3] = vec_nmsub(VbS3, Va[19], Vc7[3]);

   VbS0 = vec_splat(Vb[11], 1);
   VbS1 = vec_splat(Vb[11], 2);
   VbS2 = vec_splat(Vb[11], 3);
   
   Vc5[0] = vec_mul(VbS0, Vc5[0]);
   Vc5[1] = vec_mul(VbS0, Vc5[1]);
   Vc5[2] = vec_mul(VbS0, Vc5[2]);
   Vc5[3] = vec_mul(VbS0, Vc5[3]);
   Va[20] = Vc5[0];
   Va[21] = Vc5[1];
   Va[22] = Vc5[2];
   Va[23] = Vc5[3];
   Vc6[0] = vec_nmsub(VbS1, Va[20], Vc6[0]);
   Vc6[1] = vec_nmsub(VbS1, Va[21], Vc6[1]);
   Vc6[2] = vec_nmsub(VbS1, Va[22], Vc6[2]);
   Vc6[3] = vec_nmsub(VbS1, Va[23], Vc6[3]);
   Vc7[0] = vec_nmsub(VbS2, Va[20], Vc7[0]);
   Vc7[1] = vec_nmsub(VbS2, Va[21], Vc7[1]);
   Vc7[2] = vec_nmsub(VbS2, Va[22], Vc7[2]);
   Vc7[3] = vec_nmsub(VbS2, Va[23], Vc7[3]);

   VbS0 = vec_splat(Vb[13], 2);
   VbS1 = vec_splat(Vb[13], 3);
   
   Vc6[0] = vec_mul(VbS0, Vc6[0]);
   Vc6[1] = vec_mul(VbS0, Vc6[1]);
   Vc6[2] = vec_mul(VbS0, Vc6[2]);
   Vc6[3] = vec_mul(VbS0, Vc6[3]);
   Va[24] = Vc6[0];
   Va[25] = Vc6[1];
   Va[26] = Vc6[2];
   Va[27] = Vc6[3];
   Vc7[0] = vec_nmsub(VbS1, Va[24], Vc7[0]);
   Vc7[1] = vec_nmsub(VbS1, Va[25], Vc7[1]);
   Vc7[2] = vec_nmsub(VbS1, Va[26], Vc7[2]);
   Vc7[3] = vec_nmsub(VbS1, Va[27], Vc7[3]);

   VbS0 = vec_splat(Vb[15], 3);
   
   Vc7[0] = vec_mul(VbS0, Vc7[0]);
   Vc7[1] = vec_mul(VbS0, Vc7[1]);
   Vc7[2] = vec_mul(VbS0, Vc7[2]);
   Vc7[3] = vec_mul(VbS0, Vc7[3]);
   Va[28] = Vc7[0];
   Va[29] = Vc7[1];
   Va[30] = Vc7[2];
   Va[31] = Vc7[3];
}

#endif

static inline __attribute__ ((always_inline)) void solve(BLASLONG m, BLASLONG n, FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc) {

  FLOAT aa, bb;

  int i, j, k;

  for (i = 0; i < n; i++) {

    bb = *(b + i);

    for (j = 0; j < m; j ++) {
      aa = *(c + j + i * ldc);
      aa *= bb;
      *a  = aa;
      *(c + j + i * ldc) = aa;
      a ++;

      for (k = i + 1; k < n; k ++){
	*(c + j + k * ldc) -= aa * *(b + k);
      }

    }
    b += n;
  }
}

#else

static inline __attribute__ ((always_inline)) void solve(BLASLONG m, BLASLONG n, FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc) {

  FLOAT aa1, aa2;
  FLOAT bb1, bb2;
  FLOAT cc1, cc2;

  int i, j, k;

  ldc *= 2;

  for (i = 0; i < n; i++) {

    bb1 = *(b + i * 2 + 0);
    bb2 = *(b + i * 2 + 1);

    for (j = 0; j < m; j ++) {
      aa1 = *(c + j * 2 + 0 + i * ldc);
      aa2 = *(c + j * 2 + 1 + i * ldc);

#ifndef CONJ
      cc1 = aa1 * bb1 - aa2 * bb2;
      cc2 = aa1 * bb2 + aa2 * bb1;
#else
      cc1 =  aa1 * bb1 + aa2 * bb2;
      cc2 = -aa1 * bb2 + aa2 * bb1;
#endif

      *(a + 0) = cc1;
      *(a + 1) = cc2;
      *(c + j * 2 + 0 + i * ldc) = cc1;
      *(c + j * 2 + 1 + i * ldc) = cc2;
      a += 2;

      for (k = i + 1; k < n; k ++){
#ifndef CONJ
	*(c + j * 2 + 0 + k * ldc) -= cc1 * *(b + k * 2 + 0) - cc2 * *(b + k * 2 + 1);
	*(c + j * 2 + 1 + k * ldc) -= cc1 * *(b + k * 2 + 1) + cc2 * *(b + k * 2 + 0);
#else
	*(c + j * 2 + 0 + k * ldc) -=   cc1 * *(b + k * 2 + 0) + cc2 * *(b + k * 2 + 1);
	*(c + j * 2 + 1 + k * ldc) -= - cc1 * *(b + k * 2 + 1) + cc2 * *(b + k * 2 + 0);
#endif
      }

    }
    b += n * 2;
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
  fprintf(stderr, "TRSM RN KERNEL m = %3ld  n = %3ld  k = %3ld offset = %3ld\n",
	  m, n, k, offset);
#endif

  jj = 0;
  j = (n >> GEMM_UNROLL_N_SHIFT);
  kk = -offset;

#ifdef DOUBLE
  int well_aligned = (GEMM_UNROLL_M==8) && (GEMM_UNROLL_N==8) && ((((unsigned long) a) & 0x7) == 0);
#else
  int well_aligned = (GEMM_UNROLL_M==16) && (GEMM_UNROLL_N==8) && ((((unsigned long) a) & 0x7) == 0);
#endif

  while (j > 0) {

    aa = a;
    cc = c;

    i = (m >> GEMM_UNROLL_M_SHIFT);

    if (i > 0) {
      do {
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
	i --;
      } while (i > 0);
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
	}
	i >>= 1;
      }
    }

    kk += GEMM_UNROLL_N;
    b += GEMM_UNROLL_N * k   * COMPSIZE;
    c += GEMM_UNROLL_N * ldc * COMPSIZE;
    j --;
    jj += GEMM_UNROLL_M;
  }

  if (n & (GEMM_UNROLL_N - 1)) {

    j = (GEMM_UNROLL_N >> 1);
    while (j > 0) {
      if (n & j) {

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
	      }
	    i >>= 1;
	  }
	}

	b += j * k   * COMPSIZE;
	c += j * ldc * COMPSIZE;
	kk += j;
      }
      j >>= 1;
    }
  }

  return 0;
}
