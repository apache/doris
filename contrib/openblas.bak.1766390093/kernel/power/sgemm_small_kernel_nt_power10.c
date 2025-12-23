/***************************************************************************
Copyright (c) 2021, The OpenBLAS Project
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
#include <altivec.h>

typedef __vector unsigned char vec_t;

#if !defined(B0)
#define SAVE_4x4_ACC(ACC, N, M)                       \
  __builtin_mma_disassemble_acc((void *)result, ACC); \
  rc0 = vec_xl(0, C+(N+0)*ldc+M);                     \
  rc0 = vec_mul(rc0, vbeta);                          \
  result[0] = vec_madd(result[0], valpha, rc0);       \
  vec_xst(result[0], 0, C+(N+0)*ldc+M);               \
  rc0 = vec_xl(0, C+(N+1)*ldc+M);                     \
  rc0 = vec_mul(rc0, vbeta);                          \
  result[1] = vec_madd(result[1], valpha, rc0);       \
  vec_xst(result[1], 0, C+(N+1)*ldc+M);               \
  rc0 = vec_xl(0, C+(N+2)*ldc+M);                     \
  rc0 = vec_mul(rc0, vbeta);                          \
  result[2] = vec_madd(result[2], valpha, rc0);       \
  vec_xst(result[2], 0, C+(N+2)*ldc+M);               \
  rc0 = vec_xl(0, C+(N+3)*ldc+M);                     \
  rc0 = vec_mul(rc0, vbeta);                          \
  result[3] = vec_madd(result[3], valpha, rc0);       \
  vec_xst(result[3], 0, C+(N+3)*ldc+M);

#define SAVE_4x2_ACC(ACC, N, M)                       \
  __builtin_mma_disassemble_acc((void *)result, ACC); \
  rc0 = vec_xl_len(C+(N+0)*ldc+M, 8);                 \
  rc0 = vec_mul(rc0, vbeta);                          \
  result[0] = vec_madd(result[0], valpha, rc0);       \
  vec_xst_len(result[0], C+(N+0)*ldc+M, 8);           \
  rc0 = vec_xl_len(C+(N+1)*ldc+M, 8);                 \
  rc0 = vec_mul(rc0, vbeta);                          \
  result[1] = vec_madd(result[1], valpha, rc0);       \
  vec_xst_len(result[1], C+(N+1)*ldc+M, 8);           \
  rc0 = vec_xl_len(C+(N+2)*ldc+M, 8);                 \
  rc0 = vec_mul(rc0, vbeta);                          \
  result[2] = vec_madd(result[2], valpha, rc0);       \
  vec_xst_len(result[2], C+(N+2)*ldc+M, 8);           \
  rc0 = vec_xl_len(C+(N+3)*ldc+M, 8);                 \
  rc0 = vec_mul(rc0, vbeta);                          \
  result[3] = vec_madd(result[3], valpha, rc0);       \
  vec_xst_len(result[3], C+(N+3)*ldc+M, 8);

#define SAVE_2x4_ACC(ACC, N, M)                       \
  __builtin_mma_disassemble_acc((void *)result, ACC); \
  rc0 = vec_xl(0, C+(N+0)*ldc+M);                     \
  rc0 = vec_mul(rc0, vbeta);                          \
  result[0] = vec_madd(result[0], valpha, rc0);       \
  vec_xst(result[0], 0, C+(N+0)*ldc+M);               \
  rc0 = vec_xl(0, C+(N+1)*ldc+M);                     \
  rc0 = vec_mul(rc0, vbeta);                          \
  result[1] = vec_madd(result[1], valpha, rc0);       \
  vec_xst(result[1], 0, C+(N+1)*ldc+M);

#define SAVE_1x4_VSR(result, N, M)        \
  rc0 = vec_xl(0, C+((N)*ldc)+M);         \
  rc0 = vec_mul(rc0, vbeta);              \
  result = vec_madd(result, valpha, rc0); \
  vec_xst(result, 0, C+((N)*ldc)+M);

#define SAVE_2x2_VSR(result, N, M)            \
  rc0 = vec_xl_len(C+(N*ldc)+M, 8);           \
  rc0 = vec_insert(C[(N+1)*ldc+M+0], rc0, 2); \
  rc0 = vec_insert(C[(N+1)*ldc+M+1], rc0, 3); \
  rc0 = vec_mul(rc0, vbeta);                  \
  result = vec_madd(result, valpha, rc0);     \
  vec_xst_len(result, C+(N*ldc)+M, 8);        \
  C[(N+1)*ldc+M+0] = result[2];               \
  C[(N+1)*ldc+M+1] = result[3];

#define SAVE_1x2_VSR(result, N, M)        \
  rc0 = vec_xl_len(C+(N*ldc)+M, 8);       \
  rc0 = vec_mul(rc0, vbeta);              \
  result = vec_madd(result, valpha, rc0); \
  vec_xst_len(result, C+(N*ldc)+M, 8);

#define SAVE_4x1_VSR(result, N, M)                      \
  result = vec_mul(result, valpha);                     \
  C[(N+0)*ldc+M] = (C[(N+0)*ldc+M] * beta) + result[0]; \
  C[(N+1)*ldc+M] = (C[(N+1)*ldc+M] * beta) + result[1]; \
  C[(N+2)*ldc+M] = (C[(N+2)*ldc+M] * beta) + result[2]; \
  C[(N+3)*ldc+M] = (C[(N+3)*ldc+M] * beta) + result[3];

#define SAVE_2x1_VSR(result, N, M)                      \
  result = vec_mul(result, valpha);                     \
  C[(N+0)*ldc+M] = (C[(N+0)*ldc+M] * beta) + result[0]; \
  C[(N+1)*ldc+M] = (C[(N+1)*ldc+M] * beta) + result[1];

#else

#define SAVE_4x4_ACC(ACC, N, M)                       \
  __builtin_mma_disassemble_acc((void *)result, ACC); \
  result[0] = vec_mul(result[0], valpha);             \
  vec_xst(result[0], 0, C+(N+0)*ldc+M);               \
  result[1] = vec_mul(result[1], valpha);             \
  vec_xst(result[1], 0, C+(N+1)*ldc+M);               \
  result[2] = vec_mul(result[2], valpha);             \
  vec_xst(result[2], 0, C+(N+2)*ldc+M);               \
  result[3] = vec_mul(result[3], valpha);             \
  vec_xst(result[3], 0, C+(N+3)*ldc+M);

#define SAVE_4x2_ACC(ACC, N, M)                       \
  __builtin_mma_disassemble_acc((void *)result, ACC); \
  result[0] = vec_mul(result[0], valpha);             \
  vec_xst_len(result[0], C+(N+0)*ldc+M, 8);           \
  result[1] = vec_mul(result[1], valpha);             \
  vec_xst_len(result[1], C+(N+1)*ldc+M, 8);           \
  result[2] = vec_mul(result[2], valpha);             \
  vec_xst_len(result[2], C+(N+2)*ldc+M, 8);           \
  result[3] = vec_mul(result[3], valpha);             \
  vec_xst_len(result[3], C+(N+3)*ldc+M, 8);

#define SAVE_2x4_ACC(ACC, N, M)                       \
  __builtin_mma_disassemble_acc((void *)result, ACC); \
  result[0] = vec_mul(result[0], valpha);             \
  vec_xst(result[0], 0, C+(N+0)*ldc+M);               \
  result[1] = vec_mul(result[1], valpha);             \
  vec_xst(result[1], 0, C+(N+1)*ldc+M);

#define SAVE_1x4_VSR(result, N, M)    \
  result = vec_mul(result, valpha);   \
  vec_xst(result, 0, C+((N)*ldc)+M);

#define SAVE_2x2_VSR(result, N, M)      \
  result = vec_mul(result, valpha);     \
  vec_xst_len(result, C+(N*ldc)+M, 8);  \
  C[(N+1)*ldc+M+0] = result[2];         \
  C[(N+1)*ldc+M+1] = result[3];

#define SAVE_1x2_VSR(result, N, M)    \
  result = vec_mul(result, valpha);   \
  vec_xst_len(result, C+(N*ldc)+M, 8);

#define SAVE_4x1_VSR(result, N, M)  \
  result = vec_mul(result, valpha); \
  C[(N+0)*ldc+M] = result[0];       \
  C[(N+1)*ldc+M] = result[1];       \
  C[(N+2)*ldc+M] = result[2];       \
  C[(N+3)*ldc+M] = result[3];

#define SAVE_2x1_VSR(result, N, M)  \
  result = vec_mul(result, valpha); \
  C[(N+0)*ldc+M] = result[0];       \
  C[(N+1)*ldc+M] = result[1];

#endif

#define INIT_8ACCS()              \
  __builtin_mma_xxsetaccz(&acc0); \
  __builtin_mma_xxsetaccz(&acc1); \
  __builtin_mma_xxsetaccz(&acc2); \
  __builtin_mma_xxsetaccz(&acc3); \
  __builtin_mma_xxsetaccz(&acc4); \
  __builtin_mma_xxsetaccz(&acc5); \
  __builtin_mma_xxsetaccz(&acc6); \
  __builtin_mma_xxsetaccz(&acc7);

#define INIT_4ACCS()              \
  __builtin_mma_xxsetaccz(&acc0); \
  __builtin_mma_xxsetaccz(&acc1); \
  __builtin_mma_xxsetaccz(&acc2); \
  __builtin_mma_xxsetaccz(&acc3);

#define INIT_2ACCS()              \
  __builtin_mma_xxsetaccz(&acc0); \
  __builtin_mma_xxsetaccz(&acc1);

#define INIT_1ACC() __builtin_mma_xxsetaccz(&acc0);

#define LOAD_A_1x16(K, M)         \
  ra0 = vec_xl(0, A+(K*lda)+M+0); \
  ra1 = vec_xl(0, A+(K*lda)+M+4); \
  ra2 = vec_xl(0, A+(K*lda)+M+8); \
  ra3 = vec_xl(0, A+(K*lda)+M+12);

#define LOAD_A_1x8(K, M)          \
  ra0 = vec_xl(0, A+(K*lda)+M+0); \
  ra1 = vec_xl(0, A+(K*lda)+M+4);

#define LOAD_A_1x4(K, M) ra0 = vec_xl(0, A+(K*lda)+M);

#define LOAD_A_2x2(K, M)                  \
  ra0 = vec_splats(A[K*lda+M+0]);         \
  ra0 = vec_insert(A[K*lda+M+1], ra0, 1); \
  ra0 = vec_insert(A[K*lda+M+1], ra0, 3);

#define LOAD_A_1x2(K, M) ra0 = vec_xl_len(A+(K*lda)+M, 8);

#define LOAD_A_1x1(K, M) ra0 = vec_splats(A[K*lda+M+0]);

#define LOAD_B_1x16(K, N)         \
  rb0 = vec_xl(0, B+(K*ldb)+N+0); \
  rb1 = vec_xl(0, B+(K*ldb)+N+4); \
  rb2 = vec_xl(0, B+(K*ldb)+N+8); \
  rb3 = vec_xl(0, B+(K*ldb)+N+12);

#define LOAD_B_1x8(K, N)          \
  rb0 = vec_xl(0, B+(K*ldb)+N+0); \
  rb1 = vec_xl(0, B+(K*ldb)+N+4);

#define LOAD_B_1x4(K, N) rb0 = vec_xl(0, B+(K*ldb)+N);

#define LOAD_B_2x2(K, N)                  \
  rb0 = vec_splats(B[K*ldb+N]);           \
  rb0 = vec_insert(B[K*ldb+N+1], rb0, 2); \
  rb0 = vec_insert(B[K*ldb+N+1], rb0, 3);

#define LOAD_B_1x2(K, N) rb0 = vec_xl_len(B+(K*ldb)+N, 8);

#define LOAD_B_1x1(K, N) rb0 = vec_splats(B[K*ldb+N]);

#define KERNEL_MMA_8ACC(b0, b1, b2, b3, b4, b5, b6, b7,  \
                        a0, a1, a2, a3, a4, a5, a6, a7)  \
  __builtin_mma_xvf32gerpp(&acc0, (vec_t)b0, (vec_t)a0); \
  __builtin_mma_xvf32gerpp(&acc1, (vec_t)b1, (vec_t)a1); \
  __builtin_mma_xvf32gerpp(&acc2, (vec_t)b2, (vec_t)a2); \
  __builtin_mma_xvf32gerpp(&acc3, (vec_t)b3, (vec_t)a3); \
  __builtin_mma_xvf32gerpp(&acc4, (vec_t)b4, (vec_t)a4); \
  __builtin_mma_xvf32gerpp(&acc5, (vec_t)b5, (vec_t)a5); \
  __builtin_mma_xvf32gerpp(&acc6, (vec_t)b6, (vec_t)a6); \
  __builtin_mma_xvf32gerpp(&acc7, (vec_t)b7, (vec_t)a7);

#define KERNEL_MMA_4ACC(b0, b1, b2, b3, a0, a1, a2, a3)  \
  __builtin_mma_xvf32gerpp(&acc0, (vec_t)b0, (vec_t)a0); \
  __builtin_mma_xvf32gerpp(&acc1, (vec_t)b1, (vec_t)a1); \
  __builtin_mma_xvf32gerpp(&acc2, (vec_t)b2, (vec_t)a2); \
  __builtin_mma_xvf32gerpp(&acc3, (vec_t)b3, (vec_t)a3);

#define KERNEL_MMA_2ACC(b0, b1, a0, a1)                  \
  __builtin_mma_xvf32gerpp(&acc0, (vec_t)b0, (vec_t)a0); \
  __builtin_mma_xvf32gerpp(&acc1, (vec_t)b1, (vec_t)a1);

#define KERNEL_MMA_1ACC(b0, a0) \
  __builtin_mma_xvf32gerpp(&acc0, (vec_t)b0, (vec_t)a0);

#define KERNEL_VMADD_4VSR(a0, a1, a2, a3, b0, b1, b2, b3) \
  result = vec_madd(a0, b0, result);                      \
  result1 = vec_madd(a1, b1, result1);                    \
  result2 = vec_madd(a2, b2, result2);                    \
  result3 = vec_madd(a3, b3, result3);

#define KERNEL_VMADD_2VSR(a0, a1, b0, b1) \
  result = vec_madd(a0, b0, result);      \
  result1 = vec_madd(a1, b1, result1);

#define KERNEL_VMADD_1VSR(a0, b0)     \
  result = vec_madd(a0, b0, result);

#define PACK_A(ra0, ra1, ra2, ra3, offset) \
  vec_xst(ra0, 0, packA+(k*16)+0+offset);  \
  vec_xst(ra1, 0, packA+(k*16)+4+offset);  \
  vec_xst(ra2, 0, packA+(k*16)+8+offset);  \
  vec_xst(ra3, 0, packA+(k*16)+12+offset);

#define LOAD_PACKED_A(ra0, ra1, ra2, ra3, offset) \
  ra0 = vec_xl(0, packA+(k*16)+0+offset);         \
  ra1 = vec_xl(0, packA+(k*16)+4+offset);         \
  ra2 = vec_xl(0, packA+(k*16)+8+offset);         \
  ra3 = vec_xl(0, packA+(k*16)+12+offset);

#ifdef B0
int CNAME(BLASLONG M, BLASLONG N, BLASLONG K, IFLOAT * A, BLASLONG lda, FLOAT alpha, IFLOAT * B, BLASLONG ldb, FLOAT * C, BLASLONG ldc)
#else
int CNAME(BLASLONG M, BLASLONG N, BLASLONG K, IFLOAT * A, BLASLONG lda, FLOAT alpha, IFLOAT * B, BLASLONG ldb, FLOAT beta, FLOAT * C, BLASLONG ldc)
#endif
{
  BLASLONG m, n, k;

  BLASLONG m16 = M & ~15;
  BLASLONG m8 = M & ~7;
  BLASLONG m4 = M & ~3;
  BLASLONG m2 = M & ~1;

  BLASLONG n16 = N & ~15;
  BLASLONG n8 = N & ~7;
  BLASLONG n4 = N & ~3;
  BLASLONG n2 = N & ~1;

  vector float valpha = vec_splats(alpha);
#if !defined(B0)
  vector float vbeta = vec_splats(beta);
#endif

#if defined(__GNUC__) && !defined(__clang__)
  int has_packing = (M >= 40 && N >= 40 && K >= 40) ? 1 : 0;
#else
  int has_packing = 0;
#endif

  float *packA;
  if (has_packing) packA = (float *)malloc(K*16*sizeof(float));

  for (m = 0; m < m16; m += 16) {
    for (n = 0; n < n8; n += 8) {
      __vector_quad acc0, acc1, acc2, acc3, acc4, acc5, acc6, acc7;

      INIT_8ACCS();

      register vector float ra0, ra1, ra2, ra3;
      register vector float rb0, rb1;

      if (has_packing) {
        if (n == 0) {
          for (k = 0; k < K; k++) {
            LOAD_A_1x16(k, m);
            LOAD_B_1x8(k, n);
            KERNEL_MMA_8ACC(rb0, rb1, rb0, rb1, rb0, rb1, rb0, rb1,
                            ra0, ra0, ra1, ra1, ra2, ra2, ra3, ra3);
            PACK_A(ra0, ra1, ra2, ra3, 0);
          }
        } else {
          for (k = 0; k < K; k++) {
            LOAD_PACKED_A(ra0, ra1, ra2, ra3, 0);
            LOAD_B_1x8(k, n);
            KERNEL_MMA_8ACC(rb0, rb1, rb0, rb1, rb0, rb1, rb0, rb1,
                            ra0, ra0, ra1, ra1, ra2, ra2, ra3, ra3);
          }
        }
      } else {
        for (k = 0; k < K; k++) {
          LOAD_A_1x16(k, m);
          LOAD_B_1x8(k, n);
          KERNEL_MMA_8ACC(rb0, rb1, rb0, rb1, rb0, rb1, rb0, rb1,
                          ra0, ra0, ra1, ra1, ra2, ra2, ra3, ra3);
        }
      }

#if !defined(B0)
      register vector float rc0;
#endif
      vector float result[4];
      SAVE_4x4_ACC(&acc0, n+0, m+0);
      SAVE_4x4_ACC(&acc2, n+0, m+4);
      SAVE_4x4_ACC(&acc4, n+0, m+8);
      SAVE_4x4_ACC(&acc6, n+0, m+12);
      SAVE_4x4_ACC(&acc1, n+4, m+0);
      SAVE_4x4_ACC(&acc3, n+4, m+4);
      SAVE_4x4_ACC(&acc5, n+4, m+8);
      SAVE_4x4_ACC(&acc7, n+4, m+12);
    }

    for (; n < n4; n += 4) {
      __vector_quad acc0, acc1, acc2, acc3;

      INIT_4ACCS();

      register vector float ra0, ra1, ra2, ra3;
      register vector float rb0;

      if (!has_packing) {
        for (k = 0; k < K; k++) {
          LOAD_A_1x16(k, m);
          LOAD_B_1x4(k, n);
          KERNEL_MMA_4ACC(rb0, rb0, rb0, rb0, ra0, ra1, ra2, ra3);
        }
      } else {
        for (k = 0; k < K; k++) {
          LOAD_PACKED_A(ra0, ra1, ra2, ra3, 0);
          LOAD_B_1x4(k, n);
          KERNEL_MMA_4ACC(rb0, rb0, rb0, rb0, ra0, ra1, ra2, ra3);
        }
      }

#if !defined(B0)
      register vector float rc0;
#endif
      vector float result[4];
      SAVE_4x4_ACC(&acc0, n+0, m+0);
      SAVE_4x4_ACC(&acc1, n+0, m+4);
      SAVE_4x4_ACC(&acc2, n+0, m+8);
      SAVE_4x4_ACC(&acc3, n+0, m+12);
    }

    for (; n < n2; n += 2) {
      __vector_quad acc0, acc1, acc2, acc3;

      INIT_4ACCS();

      register vector float ra0, ra1, ra2, ra3;
      register vector float rb0;

      if (!has_packing) {
        for (k = 0; k < K; k++) {
          LOAD_A_1x16(k, m);
          LOAD_B_1x2(k, n);
          KERNEL_MMA_4ACC(rb0, rb0, rb0, rb0, ra0, ra1, ra2, ra3);
        }
      } else {
        for (k = 0; k < K; k++) {
          LOAD_PACKED_A(ra0, ra1, ra2, ra3, 0);
          LOAD_B_1x2(k, n);
          KERNEL_MMA_4ACC(rb0, rb0, rb0, rb0, ra0, ra1, ra2, ra3);
        }
      }

#if !defined(B0)
      register vector float rc0;
#endif
      vector float result[4];
      SAVE_2x4_ACC(&acc0, n, m+0);
      SAVE_2x4_ACC(&acc1, n, m+4);
      SAVE_2x4_ACC(&acc2, n, m+8);
      SAVE_2x4_ACC(&acc3, n, m+12);
    }

    for (; n < N; n++) {
      vector float result = ((vector float){0., 0., 0., 0.});
      vector float result1 = ((vector float){0., 0., 0., 0.});
      vector float result2 = ((vector float){0., 0., 0., 0.});
      vector float result3 = ((vector float){0., 0., 0., 0.});

      register vector float ra0, ra1, ra2, ra3;
      register vector float rb0;

      if (!has_packing) {
        for (k = 0; k < K; k++) {
          LOAD_A_1x16(k, m);
          LOAD_B_1x1(k, n);
          KERNEL_VMADD_4VSR(ra0, ra1, ra2, ra3, rb0, rb0, rb0, rb0);
        }
      } else {
        for (k = 0; k < K; k++) {
          LOAD_PACKED_A(ra0, ra1, ra2, ra3, 0);
          LOAD_B_1x1(k, n);
          KERNEL_VMADD_4VSR(ra0, ra1, ra2, ra3, rb0, rb0, rb0, rb0);
        }
      }

#if !defined(B0)
      register vector float rc0;
#endif
      SAVE_1x4_VSR(result, n, m);
      SAVE_1x4_VSR(result1, n, m+4);
      SAVE_1x4_VSR(result2, n, m+8);
      SAVE_1x4_VSR(result3, n, m+12);
    }
  }

  for (; m < m8; m += 8) {
    for (n = 0; n < n16; n += 16) {
      __vector_quad acc0, acc1, acc2, acc3, acc4, acc5, acc6, acc7;

      INIT_8ACCS();

      register vector float ra0, ra1;
      register vector float rb0, rb1, rb2, rb3;

      for (k = 0; k < K; k++) {
        LOAD_A_1x8(k, m);
        LOAD_B_1x16(k, n);
        KERNEL_MMA_8ACC(rb0, rb1, rb2, rb3, rb0, rb1, rb2, rb3,
                        ra0, ra0, ra0, ra0, ra1, ra1, ra1, ra1);
      }

#if !defined(B0)
      register vector float rc0;
#endif
      vector float result[4];
      SAVE_4x4_ACC(&acc0, n+0, m+0);
      SAVE_4x4_ACC(&acc4, n+0, m+4);
      SAVE_4x4_ACC(&acc1, n+4, m+0);
      SAVE_4x4_ACC(&acc5, n+4, m+4);
      SAVE_4x4_ACC(&acc2, n+8, m+0);
      SAVE_4x4_ACC(&acc6, n+8, m+4);
      SAVE_4x4_ACC(&acc3, n+12, m+0);
      SAVE_4x4_ACC(&acc7, n+12, m+4);
    }

    for (; n < n8; n += 8) {
      __vector_quad acc0, acc1, acc2, acc3;

      INIT_4ACCS();

      register vector float ra0, ra1;
      register vector float rb0, rb1;

      for (k = 0; k < K; k++) {
        LOAD_A_1x8(k, m);
        LOAD_B_1x8(k, n);
        KERNEL_MMA_4ACC(rb0, rb1, rb0, rb1, ra0, ra0, ra1, ra1);
      }

#if !defined(B0)
      register vector float rc0;
#endif
      vector float result[4];
      SAVE_4x4_ACC(&acc0, n+0, m+0);
      SAVE_4x4_ACC(&acc2, n+0, m+4);
      SAVE_4x4_ACC(&acc1, n+4, m+0);
      SAVE_4x4_ACC(&acc3, n+4, m+4);
    }

    for (; n < n4; n += 4) {
      __vector_quad acc0, acc1;

      INIT_2ACCS();

      register vector float ra0, ra1;
      register vector float rb0;

      for (k = 0; k < K; k++) {
        LOAD_A_1x8(k, m);
        LOAD_B_1x4(k, n);
        KERNEL_MMA_2ACC(rb0, rb0, ra0, ra1);
      }

#if !defined(B0)
      register vector float rc0;
#endif
      vector float result[4];
      SAVE_4x4_ACC(&acc0, n+0, m+0);
      SAVE_4x4_ACC(&acc1, n+0, m+4);
    }

    for (; n < n2; n += 2) {
      __vector_quad acc0, acc1;

      INIT_2ACCS();

      register vector float ra0, ra1;
      register vector float rb0;

      for (k = 0; k < K; k++) {
        LOAD_A_1x8(k, m);
        LOAD_B_1x2(k, n);
        KERNEL_MMA_2ACC(rb0, rb0, ra0, ra1);
      }

#if !defined(B0)
      register vector float rc0;
#endif
      vector float result[4];
      SAVE_2x4_ACC(&acc0, n, m+0);
      SAVE_2x4_ACC(&acc1, n, m+4);
    }

    for (; n < N; n++) {
      vector float result = ((vector float){0.,0.,0.,0.});
      vector float result1 = ((vector float){0.,0.,0.,0.});

      register vector float ra0, ra1;
      register vector float rb0;

      for (k = 0; k < K; k++) {
        LOAD_A_1x8(k, m);
        LOAD_B_1x1(k, n);
        KERNEL_VMADD_2VSR(ra0, ra1, rb0, rb0);
      }

#if !defined(B0)
      register vector float rc0;
#endif
      SAVE_1x4_VSR(result, n, m);
      SAVE_1x4_VSR(result1, n, m+4);
    }
  }

  for (; m < m4; m += 4) {
    for (n = 0; n < n16; n += 16) {
      __vector_quad acc0, acc1, acc2, acc3;

      INIT_4ACCS();

      register vector float ra0;
      register vector float rb0, rb1, rb2, rb3;

      for (k = 0; k < K; k++) {
        LOAD_A_1x4(k, m);
        LOAD_B_1x16(k, n);
        KERNEL_MMA_4ACC(rb0, rb1, rb2, rb3, ra0, ra0, ra0, ra0);
      }

#if !defined(B0)
      register vector float rc0;
#endif
      vector float result[4];
      SAVE_4x4_ACC(&acc0, n+0, m+0);
      SAVE_4x4_ACC(&acc1, n+4, m+0);
      SAVE_4x4_ACC(&acc2, n+8, m+0);
      SAVE_4x4_ACC(&acc3, n+12, m+0);
    }

    for (; n < n8; n += 8) {
      __vector_quad acc0, acc1;

      INIT_2ACCS();

      register vector float ra0;
      register vector float rb0, rb1;

      for (k = 0; k < K; k++) {
        LOAD_A_1x4(k, m);
        LOAD_B_1x8(k, n);
        KERNEL_MMA_2ACC(rb0, rb1, ra0, ra0);
      }

#if !defined(B0)
      register vector float rc0;
#endif
      vector float result[4];
      SAVE_4x4_ACC(&acc0, n+0, m+0);
      SAVE_4x4_ACC(&acc1, n+4, m+0);
    }

    for (; n < n4; n += 4) {
      __vector_quad acc0;

      INIT_1ACC();

      register vector float ra0;
      register vector float rb0;

      for (k = 0; k < K; k++) {
        LOAD_A_1x4(k, m);
        LOAD_B_1x4(k, n);
        KERNEL_MMA_1ACC(rb0, ra0);
      }

#if !defined(B0)
      register vector float rc0;
#endif
      vector float result[4];
      SAVE_4x4_ACC(&acc0, n+0, m+0);
    }

    for (; n < n2; n += 2) {
      __vector_quad acc0;

      INIT_1ACC();

      register vector float ra0;
      register vector float rb0;

      for (k = 0; k < K; k++) {
        LOAD_A_1x4(k, m);
        LOAD_B_1x2(k, n);
        KERNEL_MMA_1ACC(rb0, ra0);
      }

#if !defined(B0)
      register vector float rc0;
#endif
      vector float result[4];
      SAVE_2x4_ACC(&acc0, n, m);
    }

    for (; n < N; n++) {
      vector float result = ((vector float){0.,0.,0.,0.});

      register vector float ra0;
      register vector float rb0;

      for (k = 0; k < K; k++) {
        LOAD_A_1x4(k, m);
        LOAD_B_1x1(k, n);
        KERNEL_VMADD_1VSR(ra0, rb0);
      }

#if !defined(B0)
      register vector float rc0;
#endif
      SAVE_1x4_VSR(result, n, m);
    }
  }

  for (; m < m2; m += 2) {
    for (n = 0; n < n16; n += 16) {
      __vector_quad acc0, acc1, acc2, acc3;

      INIT_4ACCS();

      register vector float ra0;
      register vector float rb0, rb1, rb2, rb3;

      for (k = 0; k < K; k++) {
        LOAD_A_1x2(k, m);
        LOAD_B_1x16(k, n);
        KERNEL_MMA_4ACC(rb0, rb1, rb2, rb3, ra0, ra0, ra0, ra0);
      }

#if !defined(B0)
      register vector float rc0;
#endif
      vector float result[4];
      SAVE_4x2_ACC(&acc0, n+0, m+0);
      SAVE_4x2_ACC(&acc1, n+4, m+0);
      SAVE_4x2_ACC(&acc2, n+8, m+0);
      SAVE_4x2_ACC(&acc3, n+12, m+0);
    }

    for (; n < n8; n += 8) {
      __vector_quad acc0, acc1;

      INIT_2ACCS();

      register vector float ra0;
      register vector float rb0, rb1;

      for (k = 0; k < K; k++) {
        LOAD_A_1x2(k, m);
        LOAD_B_1x8(k, n);
        KERNEL_MMA_2ACC(rb0, rb1, ra0, ra0);
      }

#if !defined(B0)
      register vector float rc0;
#endif
      vector float result[4];
      SAVE_4x2_ACC(&acc0, n+0, m+0);
      SAVE_4x2_ACC(&acc1, n+4, m+0);
    }

    for (; n < n4; n += 4) {
      __vector_quad acc0;

      INIT_1ACC();

      register vector float ra0;
      register vector float rb0;

      for (k = 0; k < K; k++) {
        LOAD_A_1x2(k, m);
        LOAD_B_1x4(k, n);
        KERNEL_MMA_1ACC(rb0, ra0);
      }

#if !defined(B0)
      register vector float rc0;
#endif
      vector float result[4];
      SAVE_4x2_ACC(&acc0, n+0, m+0);
    }

    for (; n < n2; n += 2) {
      vector float result = ((vector float){0.,0.,0.,0.});

      register vector float ra0;
      register vector float rb0;

      for (k = 0; k < K; k++) {
        LOAD_A_2x2(k, m);
        LOAD_B_2x2(k, n);
        KERNEL_VMADD_1VSR(ra0, rb0);
      }

#if !defined(B0)
      register vector float rc0;
#endif
      SAVE_2x2_VSR(result, n, m);
    }

    for (; n < N; n++) {
      vector float result = ((vector float){0.,0.,0.,0.});

      register vector float ra0;
      register vector float rb0;

      for (k = 0; k < K; k++) {
        LOAD_A_1x2(k, m);
        LOAD_B_1x1(k, n);
        KERNEL_VMADD_1VSR(ra0, rb0);
      }

#if !defined(B0)
      register vector float rc0;
#endif
      SAVE_1x2_VSR(result, n, m);
    }
  }

  for (; m < M; m++) {
    for (n = 0; n < n16; n += 16) {
      vector float result = ((vector float){0.,0.,0.,0.});
      vector float result1 = ((vector float){0.,0.,0.,0.});
      vector float result2 = ((vector float){0.,0.,0.,0.});
      vector float result3 = ((vector float){0.,0.,0.,0.});

      register vector float ra0;
      register vector float rb0, rb1, rb2, rb3;

      for (k = 0; k < K; k++) {
        LOAD_A_1x1(k, m);
        LOAD_B_1x16(k, n);
        KERNEL_VMADD_4VSR(ra0, ra0, ra0, ra0, rb0, rb1, rb2, rb3);
      }

      SAVE_4x1_VSR(result, n+0, m);
      SAVE_4x1_VSR(result1, n+4, m);
      SAVE_4x1_VSR(result2, n+8, m);
      SAVE_4x1_VSR(result3, n+12, m);
    }

    for (; n < n8; n += 8) {
      vector float result = ((vector float){0.,0.,0.,0.});
      vector float result1 = ((vector float){0.,0.,0.,0.});

      register vector float ra0;
      register vector float rb0, rb1;

      for (k = 0; k < K; k++) {
        LOAD_A_1x1(k, m);
        LOAD_B_1x8(k, n);
        KERNEL_VMADD_2VSR(ra0, ra0, rb0, rb1);
      }

      SAVE_4x1_VSR(result, n+0, m);
      SAVE_4x1_VSR(result1, n+4, m);
    }

    for (; n < n4; n += 4) {
      vector float result = ((vector float){0.,0.,0.,0.});

      register vector float ra0;
      register vector float rb0;

      for (k = 0; k < K; k++) {
        LOAD_A_1x1(k, m);
        LOAD_B_1x4(k, n);
        KERNEL_VMADD_1VSR(ra0, rb0);
      }

      SAVE_4x1_VSR(result, n+0, m);
    }

    for (; n < n2; n += 2) {
      vector float result = ((vector float){0.,0.,0.,0.});

      register vector float ra0;
      register vector float rb0;

      for (k = 0; k < K; k++) {
        LOAD_A_1x1(k, m);
        LOAD_B_1x2(k, n);
        KERNEL_VMADD_1VSR(ra0, rb0);
      }

      SAVE_2x1_VSR(result, n+0, m);
    }

    for (; n < N; n++) {
      FLOAT result = 0.0f;

      for (k = 0; k < K; k++) {
        result += A[k*lda+m] * B[k*ldb+n];
      }
      result = result * alpha;

#if !defined(B0)
      C[n*ldc+m] = (C[n*ldc+m] * beta) + result;
#else
      C[n*ldc+m] = result;
#endif
    }
  }

  if (has_packing) free (packA);

  return 0;
}
