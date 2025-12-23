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

#if !__has_builtin(__builtin_vsx_assemble_pair)
#define __builtin_vsx_assemble_pair __builtin_mma_assemble_pair
#endif

#if !defined(B0)
#define SAVE_4x2_ACC(ACC, N, M)                       \
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

#define SAVE_4x1_ACC(ACC, N, M)                       \
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

#define SAVE_2x2_ACC(ACC, N, M)                       \
  __builtin_mma_disassemble_acc((void *)result, ACC); \
  rc0 = vec_xl(0, C+(N+0)*ldc+M);                     \
  rc0 = vec_mul(rc0, vbeta);                          \
  result[0] = vec_madd(result[0], valpha, rc0);       \
  vec_xst(result[0], 0, C+(N+0)*ldc+M);               \
  rc0 = vec_xl(0, C+(N+1)*ldc+M);                     \
  rc0 = vec_mul(rc0, vbeta);                          \
  result[1] = vec_madd(result[1], valpha, rc0);       \
  vec_xst(result[1], 0, C+(N+1)*ldc+M);

#define SAVE_2x1_ACC(ACC, N, M)                       \
  __builtin_mma_disassemble_acc((void *)result, ACC); \
  rc0 = vec_xl_len(C+(N+0)*ldc+M, 8);                 \
  rc0 = vec_mul(rc0, vbeta);                          \
  result[0] = vec_madd(result[0], valpha, rc0);       \
  vec_xst_len(result[0], C+(N+0)*ldc+M, 8);           \
  rc0 = vec_xl_len(C+(N+1)*ldc+M, 8);                 \
  rc0 = vec_mul(rc0, vbeta);                          \
  result[1] = vec_madd(result[1], valpha, rc0);       \
  vec_xst_len(result[1], C+(N+1)*ldc+M, 8);

#define SAVE_1x4_VSR(result, N, M)        \
  rc0 = vec_xl(0, C+((N)*ldc)+M);         \
  rc0 = vec_mul(rc0, vbeta);              \
  result = vec_madd(result, valpha, rc0); \
  vec_xst(result, 0, C+((N)*ldc)+M);

#else

#define SAVE_4x2_ACC(ACC, N, M)                       \
  __builtin_mma_disassemble_acc((void *)result, ACC); \
  result[0] = vec_mul(result[0], valpha);             \
  vec_xst(result[0], 0, C+(N+0)*ldc+M);               \
  result[1] = vec_mul(result[1], valpha);             \
  vec_xst(result[1], 0, C+(N+1)*ldc+M);               \
  result[2] = vec_mul(result[2], valpha);             \
  vec_xst(result[2], 0, C+(N+2)*ldc+M);               \
  result[3] = vec_mul(result[3], valpha);             \
  vec_xst(result[3], 0, C+(N+3)*ldc+M);

#define SAVE_4x1_ACC(ACC, N, M)                       \
  __builtin_mma_disassemble_acc((void *)result, ACC); \
  result[0] = vec_mul(result[0], valpha);             \
  vec_xst_len(result[0], C+(N+0)*ldc+M, 8);           \
  result[1] = vec_mul(result[1], valpha);             \
  vec_xst_len(result[1], C+(N+1)*ldc+M, 8);           \
  result[2] = vec_mul(result[2], valpha);             \
  vec_xst_len(result[2], C+(N+2)*ldc+M, 8);           \
  result[3] = vec_mul(result[3], valpha);             \
  vec_xst_len(result[3], C+(N+3)*ldc+M, 8);

#define SAVE_2x2_ACC(ACC, N, M)                       \
  __builtin_mma_disassemble_acc((void *)result, ACC); \
  result[0] = vec_mul(result[0], valpha);             \
  vec_xst(result[0], 0, C+(N+0)*ldc+M);               \
  result[1] = vec_mul(result[1], valpha);             \
  vec_xst(result[1], 0, C+(N+1)*ldc+M);

#define SAVE_2x1_ACC(ACC, N, M)                       \
  __builtin_mma_disassemble_acc((void *)result, ACC); \
  result[0] = vec_mul(result[0], valpha);             \
  vec_xst_len(result[0], C+(N+0)*ldc+M, 8);           \
  result[1] = vec_mul(result[1], valpha);             \
  vec_xst_len(result[1], C+(N+1)*ldc+M, 8);

#define SAVE_1x4_VSR(result, N, M)    \
  result = vec_mul(result, valpha);   \
  vec_xst(result, 0, C+((N)*ldc)+M);

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

#if (defined(__GNUC__) && (__GNUC__ == 10 || (__GNUC__ == 11 && __GNUC_MINOR__ <= 2)))
#if defined(_AIX)
#define LOAD_PAIR(pair, v0, v1)                             \
  __builtin_vsx_assemble_pair(&pair, (vec_t)v0, (vec_t)v1);
#else
#define LOAD_PAIR(pair, v0, v1)                             \
  __builtin_vsx_assemble_pair(&pair, (vec_t)v1, (vec_t)v0);
#endif
#else
#define LOAD_PAIR(pair, v0, v1)                             \
  __builtin_vsx_build_pair(&pair, (vec_t)v0, (vec_t)v1);
#endif

#define LOAD_A_1x8(K, M)            \
  ra0 = vec_xl(0, A+((K)*lda)+M+0); \
  ra1 = vec_xl(0, A+((K)*lda)+M+2); \
  ra2 = vec_xl(0, A+((K)*lda)+M+4); \
  ra3 = vec_xl(0, A+((K)*lda)+M+6);

#define LOAD_A_1x4(K, M)            \
  ra0 = vec_xl(0, A+((K)*lda)+M+0); \
  ra1 = vec_xl(0, A+((K)*lda)+M+2); \

#define LOAD_A_1x2(K, M)            \
  ra0 = vec_xl(0, A+((K)*lda)+M+0);

#define LOAD_A_1x1(K, M)              \
  ra0 = vec_splats(A[((K)*lda)+M+0]);

#define LOAD_BTP_8x2(N, K)          \
  rb0 = vec_xl(0, B+(N+0)*ldb+K+0); \
  rb1 = vec_xl(0, B+(N+1)*ldb+K+0); \
  rb2 = vec_xl(0, B+(N+2)*ldb+K+0); \
  rb3 = vec_xl(0, B+(N+3)*ldb+K+0); \
  t0 = vec_mergeh(rb0, rb1);        \
  t1 = vec_mergeh(rb2, rb3);        \
  LOAD_PAIR(pb0, t0, t1);           \
  t0 = vec_mergel(rb0, rb1);        \
  t1 = vec_mergel(rb2, rb3);        \
  LOAD_PAIR(pb2, t0, t1);           \
  rb4 = vec_xl(0, B+(N+4)*ldb+K+0); \
  rb5 = vec_xl(0, B+(N+5)*ldb+K+0); \
  rb6 = vec_xl(0, B+(N+6)*ldb+K+0); \
  rb7 = vec_xl(0, B+(N+7)*ldb+K+0); \
  t0 = vec_mergeh(rb4, rb5);        \
  t1 = vec_mergeh(rb6, rb7);        \
  LOAD_PAIR(pb1, t0, t1);           \
  t0 = vec_mergel(rb4, rb5);        \
  t1 = vec_mergel(rb6, rb7);        \
  LOAD_PAIR(pb3, t0, t1);

#define LOAD_BTP_8x1(N, K)                  \
  rb0 = vec_xor(rb0, rb0);                  \
  rb0 = vec_insert(B[(N+0)*ldb+K], rb0, 0); \
  rb0 = vec_insert(B[(N+1)*ldb+K], rb0, 1); \
  rb1 = vec_xor(rb1, rb1);                  \
  rb1 = vec_insert(B[(N+2)*ldb+K], rb1, 0); \
  rb1 = vec_insert(B[(N+3)*ldb+K], rb1, 1); \
  LOAD_PAIR(pb0, rb0, rb1);                 \
  rb2 = vec_xor(rb2, rb2);                  \
  rb2 = vec_insert(B[(N+4)*ldb+K], rb2, 0); \
  rb2 = vec_insert(B[(N+5)*ldb+K], rb2, 1); \
  rb3 = vec_xor(rb3, rb3);                  \
  rb3 = vec_insert(B[(N+6)*ldb+K], rb3, 0); \
  rb3 = vec_insert(B[(N+7)*ldb+K], rb3, 1); \
  LOAD_PAIR(pb1, rb2, rb3);

#define LOAD_BTP_4x2(N, K)          \
  rb0 = vec_xl(0, B+(N+0)*ldb+K+0); \
  rb1 = vec_xl(0, B+(N+1)*ldb+K+0); \
  rb2 = vec_xl(0, B+(N+2)*ldb+K+0); \
  rb3 = vec_xl(0, B+(N+3)*ldb+K+0); \
  t0 = vec_mergeh(rb0, rb1);        \
  t1 = vec_mergeh(rb2, rb3);        \
  LOAD_PAIR(pb0, t0, t1);           \
  t0 = vec_mergel(rb0, rb1);        \
  t1 = vec_mergel(rb2, rb3);        \
  LOAD_PAIR(pb1, t0, t1);

#define LOAD_BTP_4x1(N, K)                  \
  rb0 = vec_xor(rb0, rb0);                  \
  rb0 = vec_insert(B[(N+0)*ldb+K], rb0, 0); \
  rb0 = vec_insert(B[(N+1)*ldb+K], rb0, 1); \
  rb1 = vec_xor(rb1, rb1);                  \
  rb1 = vec_insert(B[(N+2)*ldb+K], rb1, 0); \
  rb1 = vec_insert(B[(N+3)*ldb+K], rb1, 1); \
  LOAD_PAIR(pb0, rb0, rb1);

#define LOAD_BTP_2x2(N, K)                                  \
  rb0 = vec_xl(0, B+(N+0)*ldb+K+0);                         \
  rb1 = vec_xl(0, B+(N+1)*ldb+K+0);                         \
  t0 = vec_mergeh(rb0, rb1);                                \
  __builtin_vsx_assemble_pair(&pb0, (vec_t)t0, (vec_t)t0);  \
  t1 = vec_mergel(rb0, rb1);                                \
  __builtin_vsx_assemble_pair(&pb1, (vec_t)t1, (vec_t)t1);

#define LOAD_BTP_2x1(N, K)                                  \
  rb0 = vec_xor(rb0, rb0);                                  \
  rb0 = vec_insert(B[(N+0)*ldb+K], rb0, 0);                 \
  rb0 = vec_insert(B[(N+1)*ldb+K], rb0, 1);                 \
  __builtin_vsx_assemble_pair(&pb0, (vec_t)rb0, (vec_t)rb0);

#define LOAD_B_1x1(N, K)              \
  rb0 = vec_splats(B[((N)*ldb)+K]);

#define KERNEL_MMA_8ACC(b0, b1, b2, b3, b4, b5, b6, b7, \
                        a0, a1, a2, a3, a4, a5, a6, a7) \
  __builtin_mma_xvf64gerpp(&acc0, b0, (vec_t)a0);       \
  __builtin_mma_xvf64gerpp(&acc1, b1, (vec_t)a1);       \
  __builtin_mma_xvf64gerpp(&acc2, b2, (vec_t)a2);       \
  __builtin_mma_xvf64gerpp(&acc3, b3, (vec_t)a3);       \
  __builtin_mma_xvf64gerpp(&acc4, b4, (vec_t)a4);       \
  __builtin_mma_xvf64gerpp(&acc5, b5, (vec_t)a5);       \
  __builtin_mma_xvf64gerpp(&acc6, b6, (vec_t)a6);       \
  __builtin_mma_xvf64gerpp(&acc7, b7, (vec_t)a7);

#define KERNEL_MMA_4ACC(b0, b1, b2, b3, a0, a1, a2, a3) \
  __builtin_mma_xvf64gerpp(&acc0, b0, (vec_t)a0);       \
  __builtin_mma_xvf64gerpp(&acc1, b1, (vec_t)a1);       \
  __builtin_mma_xvf64gerpp(&acc2, b2, (vec_t)a2);       \
  __builtin_mma_xvf64gerpp(&acc3, b3, (vec_t)a3);

#define KERNEL_MMA_2ACC(b0, b1, a0, a1)           \
  __builtin_mma_xvf64gerpp(&acc0, b0, (vec_t)a0); \
  __builtin_mma_xvf64gerpp(&acc1, b1, (vec_t)a1);

#define KERNEL_MMA_1ACC(b0, a0)                   \
  __builtin_mma_xvf64gerpp(&acc0, b0, (vec_t)a0);

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

#define PACK_B(pb0, pb1, offset)                            \
  *((__vector_pair *)(void *)(packB+(k*8)+0+offset)) = pb0; \
  *((__vector_pair *)(void *)(packB+(k*8)+4+offset)) = pb1;

#define LOAD_PACKED_B(pb0, pb1, offset)                        \
  pb0 = *((__vector_pair *)((void *)(packB+(k*8)+0+offset)));  \
  pb1 = *((__vector_pair *)((void *)(packB+(k*8)+4+offset)));

#ifdef B0
int CNAME(BLASLONG M, BLASLONG N, BLASLONG K, IFLOAT * A, BLASLONG lda, FLOAT alpha, IFLOAT * B, BLASLONG ldb, FLOAT * C, BLASLONG ldc)
#else
int CNAME(BLASLONG M, BLASLONG N, BLASLONG K, IFLOAT * A, BLASLONG lda, FLOAT alpha, IFLOAT * B, BLASLONG ldb, FLOAT beta, FLOAT * C, BLASLONG ldc)
#endif
{
  BLASLONG m, n, k;

  BLASLONG m8 = M & ~7;
  BLASLONG m4 = M & ~3;
  BLASLONG m2 = M & ~1;

  BLASLONG n8 = N & ~7;
  BLASLONG n4 = N & ~3;
  BLASLONG n2 = N & ~1;

  BLASLONG k2 = K & ~1;

#if defined(__GNUC__) && !defined(__clang__)
  int has_packing = (M >= 32 && N >= 32 && K >= 32) ? 1 : 0;
#else
  int has_packing = 0;
#endif

  double *packB;
  if (has_packing) packB = (double *)malloc(K*8*sizeof(double));

  vector double valpha = vec_splats(alpha);
#if !defined(B0)
  vector double vbeta = vec_splats(beta);
#endif

  for (n = 0; n < n8; n += 8) {
    for (m = 0; m < m8; m += 8) {
      __vector_quad acc0, acc1, acc2, acc3, acc4, acc5, acc6, acc7;

      INIT_8ACCS();

      register vector double ra0, ra1, ra2, ra3;
      register vector double rb0, rb1, rb2, rb3, rb4, rb5, rb6, rb7;
      register vector double t0, t1;

      __vector_pair pb0, pb1, pb2, pb3;

      if (has_packing) {
        if (m == 0) {
          for (k = 0; k < k2; k += 2) {
            LOAD_A_1x8(k, m);
            LOAD_BTP_8x2(n, k);
            KERNEL_MMA_8ACC(pb0, pb1, pb0, pb1, pb0, pb1, pb0, pb1,
                            ra0, ra0, ra1, ra1, ra2, ra2, ra3, ra3);
            PACK_B(pb0, pb1, 0);
            LOAD_A_1x8(k+1, m);
            KERNEL_MMA_8ACC(pb2, pb3, pb2, pb3, pb2, pb3, pb2, pb3,
                            ra0, ra0, ra1, ra1, ra2, ra2, ra3, ra3);
            PACK_B(pb2, pb3, 8);
          }
          for (; k < K; k++) {
            LOAD_A_1x8(k, m);
            LOAD_BTP_8x1(n, k);
            KERNEL_MMA_8ACC(pb0, pb1, pb0, pb1, pb0, pb1, pb0, pb1,
                            ra0, ra0, ra1, ra1, ra2, ra2, ra3, ra3);
            PACK_B(pb0, pb1, 0);
          }
        } else {
          for (k = 0; k < k2; k += 2) {
            LOAD_A_1x8(k, m);
            LOAD_PACKED_B(pb0, pb1, 0);
            KERNEL_MMA_8ACC(pb0, pb1, pb0, pb1, pb0, pb1, pb0, pb1,
                            ra0, ra0, ra1, ra1, ra2, ra2, ra3, ra3);
            LOAD_A_1x8(k+1, m);
            LOAD_PACKED_B(pb2, pb3, 8);
            KERNEL_MMA_8ACC(pb2, pb3, pb2, pb3, pb2, pb3, pb2, pb3,
                            ra0, ra0, ra1, ra1, ra2, ra2, ra3, ra3);
          }
          for (; k < K; k++) {
            LOAD_A_1x8(k, m);
            LOAD_PACKED_B(pb0, pb1, 0);
            KERNEL_MMA_8ACC(pb0, pb1, pb0, pb1, pb0, pb1, pb0, pb1,
                            ra0, ra0, ra1, ra1, ra2, ra2, ra3, ra3);
          }
        }
      } else {
        for (k = 0; k < k2; k += 2) {
          LOAD_A_1x8(k, m);
          LOAD_BTP_8x2(n, k);
          KERNEL_MMA_8ACC(pb0, pb1, pb0, pb1, pb0, pb1, pb0, pb1,
                          ra0, ra0, ra1, ra1, ra2, ra2, ra3, ra3);
          LOAD_A_1x8(k+1, m);
          KERNEL_MMA_8ACC(pb2, pb3, pb2, pb3, pb2, pb3, pb2, pb3,
                          ra0, ra0, ra1, ra1, ra2, ra2, ra3, ra3);
        }
        for (; k < K; k++) {
          LOAD_A_1x8(k, m);
          LOAD_BTP_8x1(n, k);
          KERNEL_MMA_8ACC(pb0, pb1, pb0, pb1, pb0, pb1, pb0, pb1,
                          ra0, ra0, ra1, ra1, ra2, ra2, ra3, ra3);
        }
      }

#if !defined(B0)
      register vector double rc0;
#endif
      vector double result[4];
      SAVE_4x2_ACC(&acc0, n+0, m+0);
      SAVE_4x2_ACC(&acc2, n+0, m+2);
      SAVE_4x2_ACC(&acc4, n+0, m+4);
      SAVE_4x2_ACC(&acc6, n+0, m+6);
      SAVE_4x2_ACC(&acc1, n+4, m+0);
      SAVE_4x2_ACC(&acc3, n+4, m+2);
      SAVE_4x2_ACC(&acc5, n+4, m+4);
      SAVE_4x2_ACC(&acc7, n+4, m+6);
    }

    for (; m < m4; m += 4) {
      __vector_quad acc0, acc1, acc2, acc3;

      INIT_4ACCS();

      register vector double ra0, ra1;
      register vector double rb0, rb1, rb2, rb3, rb4, rb5, rb6, rb7;
      register vector double t0, t1;

      __vector_pair pb0, pb1, pb2, pb3;

      if (!has_packing) {
        for (k = 0; k < k2; k += 2) {
          LOAD_A_1x4(k, m);
          LOAD_BTP_8x2(n, k);
          KERNEL_MMA_4ACC(pb0, pb1, pb0, pb1, ra0, ra0, ra1, ra1);
          LOAD_A_1x4(k+1, m);
          KERNEL_MMA_4ACC(pb2, pb3, pb2, pb3, ra0, ra0, ra1, ra1);
        }
        for (; k < K; k++) {
          LOAD_A_1x4(k, m);
          LOAD_BTP_8x1(n, k);
          KERNEL_MMA_4ACC(pb0, pb1, pb0, pb1, ra0, ra0, ra1, ra1);
        }
      } else {
        for (k = 0; k < k2; k += 2) {
          LOAD_A_1x4(k, m);
          LOAD_PACKED_B(pb0, pb1, 0);
          KERNEL_MMA_4ACC(pb0, pb1, pb0, pb1, ra0, ra0, ra1, ra1);
          LOAD_A_1x4(k+1, m);
          LOAD_PACKED_B(pb2, pb3, 8);
          KERNEL_MMA_4ACC(pb2, pb3, pb2, pb3, ra0, ra0, ra1, ra1);
        }
        for (; k < K; k++) {
          LOAD_A_1x4(k, m);
          LOAD_PACKED_B(pb0, pb1, 0);
          KERNEL_MMA_4ACC(pb0, pb1, pb0, pb1, ra0, ra0, ra1, ra1);
        }
      }

#if !defined(B0)
      register vector double rc0;
#endif
      vector double result[4];
      SAVE_4x2_ACC(&acc0, n+0, m+0);
      SAVE_4x2_ACC(&acc2, n+0, m+2);
      SAVE_4x2_ACC(&acc1, n+4, m+0);
      SAVE_4x2_ACC(&acc3, n+4, m+2);
    }

    for (; m < m2; m += 2) {
      __vector_quad acc0, acc1, acc2, acc3;

      INIT_4ACCS();

      register vector double ra0;
      register vector double rb0, rb1, rb2, rb3, rb4, rb5, rb6, rb7;
      register vector double t0, t1;

      __vector_pair pb0, pb1, pb2, pb3;

      if (!has_packing) {
        for (k = 0; k < k2; k += 2) {
          LOAD_A_1x2(k, m);
          LOAD_BTP_8x2(n, k);
          KERNEL_MMA_2ACC(pb0, pb1, ra0, ra0);
          LOAD_A_1x2(k+1, m);
          KERNEL_MMA_2ACC(pb2, pb3, ra0, ra0);
        }
        for (; k < K; k++) {
          LOAD_A_1x2(k, m);
          LOAD_BTP_8x1(n, k);
          KERNEL_MMA_2ACC(pb0, pb1, ra0, ra0);
        }
      } else {
        for (k = 0; k < k2; k += 2) {
          LOAD_A_1x2(k, m);
          LOAD_PACKED_B(pb0, pb1, 0);
          KERNEL_MMA_2ACC(pb0, pb1, ra0, ra0);
          LOAD_A_1x2(k+1, m);
          LOAD_PACKED_B(pb2, pb3, 8);
          KERNEL_MMA_2ACC(pb2, pb3, ra0, ra0);
        }
        for (; k < K; k++) {
          LOAD_A_1x2(k, m);
          LOAD_PACKED_B(pb0, pb1, 0);
          KERNEL_MMA_2ACC(pb0, pb1, ra0, ra0);
        }
      }

#if !defined(B0)
      register vector double rc0;
#endif
      vector double result[4];
      SAVE_4x2_ACC(&acc0, n+0, m+0);
      SAVE_4x2_ACC(&acc1, n+4, m+0);
    }

    for (; m < M; m++) {
      __vector_quad acc0, acc1;

      INIT_2ACCS();

      register vector double ra0;
      register vector double rb0, rb1, rb2, rb3, rb4, rb5, rb6, rb7;
      register vector double t0, t1;

      __vector_pair pb0, pb1, pb2, pb3;

      if (!has_packing) {
        for (k = 0; k < k2; k += 2) {
          LOAD_A_1x1(k, m);
          LOAD_BTP_8x2(n, k);
          KERNEL_MMA_2ACC(pb0, pb1, ra0, ra0);
          LOAD_A_1x1(k+1, m);
          KERNEL_MMA_2ACC(pb2, pb3, ra0, ra0);
        }
        for (; k < K; k++) {
          LOAD_A_1x1(k, m);
          LOAD_BTP_8x1(n, k);
          KERNEL_MMA_2ACC(pb0, pb1, ra0, ra0);
        }
      } else {
        for (k = 0; k < k2; k += 2) {
          LOAD_A_1x1(k, m);
          LOAD_PACKED_B(pb0, pb1, 0);
          KERNEL_MMA_2ACC(pb0, pb1, ra0, ra0);
          LOAD_A_1x1(k+1, m);
          LOAD_PACKED_B(pb2, pb3, 8);
          KERNEL_MMA_2ACC(pb2, pb3, ra0, ra0);
        }
        for (; k < K; k++) {
          LOAD_A_1x1(k, m);
          LOAD_PACKED_B(pb0, pb1, 0);
          KERNEL_MMA_2ACC(pb0, pb1, ra0, ra0);
        }
      }

#if !defined(B0)
      register vector double rc0;
#endif
      vector double result[4];
      SAVE_4x1_ACC(&acc0, n+0, m+0);
      SAVE_4x1_ACC(&acc1, n+4, m+0);
    }
  }

  for (; n < n4; n += 4) {
    for (m = 0; m < m8; m += 8) {
      __vector_quad acc0, acc1, acc2, acc3;

      INIT_4ACCS();

      register vector double ra0, ra1, ra2, ra3;
      register vector double rb0, rb1, rb2, rb3;
      register vector double t0, t1;

      __vector_pair pb0, pb1;

      for (k = 0; k < k2; k += 2) {
        LOAD_A_1x8(k, m);
        LOAD_BTP_4x2(n, k);
        KERNEL_MMA_4ACC(pb0, pb0, pb0, pb0, ra0, ra1, ra2, ra3);
        LOAD_A_1x8(k+1, m);
        KERNEL_MMA_4ACC(pb1, pb1, pb1, pb1, ra0, ra1, ra2, ra3);
      }
      for (; k < K; k++) {
        LOAD_A_1x8(k, m);
        LOAD_BTP_4x1(n, k);
        KERNEL_MMA_4ACC(pb0, pb0, pb0, pb0, ra0, ra1, ra2, ra3);
      }

#if !defined(B0)
      register vector double rc0;
#endif
      vector double result[4];
      SAVE_4x2_ACC(&acc0, n+0, m+0);
      SAVE_4x2_ACC(&acc1, n+0, m+2);
      SAVE_4x2_ACC(&acc2, n+0, m+4);
      SAVE_4x2_ACC(&acc3, n+0, m+6);
    }

    for (; m < m4; m += 4) {
      __vector_quad acc0, acc1;

      INIT_2ACCS();

      register vector double ra0, ra1;
      register vector double rb0, rb1, rb2, rb3;
      register vector double t0, t1;

      __vector_pair pb0, pb1;

      for (k = 0; k < k2; k += 2) {
        LOAD_A_1x4(k, m);
        LOAD_BTP_4x2(n, k);
        KERNEL_MMA_2ACC(pb0, pb0, ra0, ra1);
        LOAD_A_1x4(k+1, m);
        KERNEL_MMA_2ACC(pb1, pb1, ra0, ra1);
      }
      for (; k < K; k++) {
        LOAD_A_1x4(k, m);
        LOAD_BTP_4x1(n, k);
        KERNEL_MMA_2ACC(pb0, pb0, ra0, ra1);
      }

#if !defined(B0)
      register vector double rc0;
#endif
      vector double result[4];
      SAVE_4x2_ACC(&acc0, n+0, m+0);
      SAVE_4x2_ACC(&acc1, n+0, m+2);
    }

    for (; m < m2; m += 2) {
      __vector_quad acc0;

      INIT_1ACC();

      register vector double ra0;
      register vector double rb0, rb1, rb2, rb3;
      register vector double t0, t1;

      __vector_pair pb0, pb1;

      for (k = 0; k < k2; k += 2) {
        LOAD_A_1x2(k, m);
        LOAD_BTP_4x2(n, k);
        KERNEL_MMA_1ACC(pb0, ra0);
        LOAD_A_1x2(k+1, m);
        KERNEL_MMA_1ACC(pb1, ra0);
      }
      for (; k < K; k++) {
        LOAD_A_1x2(k, m);
        LOAD_BTP_4x1(n, k);
        KERNEL_MMA_1ACC(pb0, ra0);
      }

#if !defined(B0)
      register vector double rc0;
#endif
      vector double result[4];
      SAVE_4x2_ACC(&acc0, n, m);
    }

    for (; m < M; m++) {
      __vector_quad acc0;

      INIT_1ACC();

      register vector double ra0;
      register vector double rb0, rb1, rb2, rb3;
      register vector double t0, t1;

      __vector_pair pb0, pb1;

      for (k = 0; k < k2; k += 2) {
        LOAD_A_1x1(k, m);
        LOAD_BTP_4x2(n, k);
        KERNEL_MMA_1ACC(pb0, ra0);
        LOAD_A_1x1(k+1, m);
        KERNEL_MMA_1ACC(pb1, ra0);
      }
      for (; k < K; k++) {
        LOAD_A_1x1(k, m);
        LOAD_BTP_4x1(n, k);
        KERNEL_MMA_1ACC(pb0, ra0);
      }

#if !defined(B0)
      register vector double rc0;
#endif
      vector double result[4];
      SAVE_4x1_ACC(&acc0, n, m);
    }
  }

  for (; n < n2; n += 2) {
    for (m = 0; m < m8; m += 8) {
      __vector_quad acc0, acc1, acc2, acc3;

      INIT_4ACCS();

      register vector double ra0, ra1, ra2, ra3;
      register vector double rb0, rb1;
      register vector double t0, t1;

      __vector_pair pb0, pb1;

      for (k = 0; k < k2; k += 2) {
        LOAD_A_1x8(k, m);
        LOAD_BTP_2x2(n, k);
        KERNEL_MMA_4ACC(pb0, pb0, pb0, pb0, ra0, ra1, ra2, ra3);
        LOAD_A_1x8(k+1, m);
        KERNEL_MMA_4ACC(pb1, pb1, pb1, pb1, ra0, ra1, ra2, ra3);
      }
      for (; k < K; k++) {
        LOAD_A_1x8(k, m);
        LOAD_BTP_2x1(n, k);
        KERNEL_MMA_4ACC(pb0, pb0, pb0, pb0, ra0, ra1, ra2, ra3);
      }

#if !defined(B0)
      register vector double rc0;
#endif
      vector double result[4];
      SAVE_2x2_ACC(&acc0, n+0, m+0);
      SAVE_2x2_ACC(&acc1, n+0, m+2);
      SAVE_2x2_ACC(&acc2, n+0, m+4);
      SAVE_2x2_ACC(&acc3, n+0, m+6);
    }

    for (; m < m4; m += 4) {
      __vector_quad acc0, acc1;

      INIT_2ACCS();

      register vector double ra0, ra1;
      register vector double rb0, rb1;
      register vector double t0, t1;

      __vector_pair pb0, pb1;

      for (k = 0; k < k2; k += 2) {
        LOAD_A_1x4(k, m);
        LOAD_BTP_2x2(n, k);
        KERNEL_MMA_2ACC(pb0, pb0, ra0, ra1);
        LOAD_A_1x4(k+1, m);
        KERNEL_MMA_2ACC(pb1, pb1, ra0, ra1);
      }
      for (; k < K; k++) {
        LOAD_A_1x4(k, m);
        LOAD_BTP_2x1(n, k);
        KERNEL_MMA_2ACC(pb0, pb0, ra0, ra1);
      }

#if !defined(B0)
      register vector double rc0;
#endif
      vector double result[4];
      SAVE_2x2_ACC(&acc0, n+0, m+0);
      SAVE_2x2_ACC(&acc1, n+0, m+2);
    }

    for (; m < m2; m += 2) {
      __vector_quad acc0;

      INIT_1ACC();

      register vector double ra0;
      register vector double rb0, rb1;
      register vector double t0, t1;

      __vector_pair pb0, pb1;

      for (k = 0; k < k2; k += 2) {
        LOAD_A_1x2(k, m);
        LOAD_BTP_2x2(n, k);
        KERNEL_MMA_1ACC(pb0, ra0);
        LOAD_A_1x2(k+1, m);
        KERNEL_MMA_1ACC(pb1, ra0);
      }
      for (; k < K; k++) {
        LOAD_A_1x2(k, m);
        LOAD_BTP_2x1(n, k);
        KERNEL_MMA_1ACC(pb0, ra0);
      }

#if !defined(B0)
      register vector double rc0;
#endif
      vector double result[4];
      SAVE_2x2_ACC(&acc0, n+0, m+0);
    }

    for (; m < M; m++) {
      __vector_quad acc0;

      INIT_1ACC();

      register vector double ra0;
      register vector double rb0, rb1;
      register vector double t0, t1;

      __vector_pair pb0, pb1;

      for (k = 0; k < k2; k += 2) {
        LOAD_A_1x1(k, m);
        LOAD_BTP_2x2(n, k);
        KERNEL_MMA_1ACC(pb0, ra0);
        LOAD_A_1x1(k+1, m);
        KERNEL_MMA_1ACC(pb1, ra0);
      }
      for (; k < K; k++) {
        LOAD_A_1x1(k, m);
        LOAD_BTP_2x1(n, k);
        KERNEL_MMA_1ACC(pb0, ra0);
      }

#if !defined(B0)
      register vector double rc0;
#endif
      vector double result[4];
      SAVE_2x1_ACC(&acc0, n+0, m+0);
    }
  }

  for (; n < N; n++) {
    for (m = 0; m < m8; m += 8) {
      vector double result = ((vector double){0.,0.});
      vector double result1 = ((vector double){0.,0.});
      vector double result2 = ((vector double){0.,0.});
      vector double result3 = ((vector double){0.,0.});

      register vector double ra0, ra1, ra2, ra3;
      register vector double rb0;

      for (k = 0; k < K; k++) {
        LOAD_A_1x8(k, m);
        LOAD_B_1x1(n, k);
        KERNEL_VMADD_4VSR(ra0, ra1, ra2, ra3, rb0, rb0, rb0, rb0);
      }

#if !defined(B0)
      register vector double rc0;
#endif
      SAVE_1x4_VSR(result, n, m+0);
      SAVE_1x4_VSR(result1, n, m+2);
      SAVE_1x4_VSR(result2, n, m+4);
      SAVE_1x4_VSR(result3, n, m+6);
    }

    for (; m < m4; m += 4) {
      vector double result = ((vector double){0.,0.});
      vector double result1 = ((vector double){0.,0.});

      register vector double ra0, ra1;
      register vector double rb0;

      for (k = 0; k < K; k++) {
        LOAD_A_1x4(k, m);
        LOAD_B_1x1(n, k);
        KERNEL_VMADD_2VSR(ra0, ra1, rb0, rb0);
      }

#if !defined(B0)
      register vector double rc0;
#endif
      SAVE_1x4_VSR(result, n, m+0);
      SAVE_1x4_VSR(result1, n, m+2);
    }

    for (; m < m2; m += 2) {
      vector double result = ((vector double){0.,0.});

      register vector double ra0;
      register vector double rb0;

      for (k = 0; k < K; k++) {
        LOAD_A_1x2(k, m);
        LOAD_B_1x1(n, k);
        KERNEL_VMADD_1VSR(ra0, rb0);
      }

#if !defined(B0)
      register vector double rc0;
#endif
      SAVE_1x4_VSR(result, n, m+0);
    }

    for (; m < M; m++) {
      FLOAT result = 0.0;

      for (k = 0; k < K; k++) {
        result += A[m+k*lda] * B[n*ldb+k];
      }
      result = result * alpha;

#if !defined(B0)
      C[n*ldc+m] = (C[n*ldc+m] * beta) + result;
#else
      C[n*ldc+m] = result;
#endif
    }
  }

  if (has_packing) free(packB);

  return 0;
}
