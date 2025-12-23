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
#define SAVE_4x4_ACC(ACC, N, M)                         \
  __builtin_mma_disassemble_acc ((void *)result, ACC);  \
  rc0 = vec_xl(0, C+(N+0)*ldc+M);                       \
  rc0 = vec_mul(rc0, vbeta);                            \
  result[0] = vec_madd(result[0], valpha, rc0);         \
  vec_xst(result[0], 0, C+(N+0)*ldc+M);                 \
  rc0 = vec_xl(0, C+(N+1)*ldc+M);                       \
  rc0 = vec_mul(rc0, vbeta);                            \
  result[1] = vec_madd(result[1], valpha, rc0);         \
  vec_xst(result[1], 0, C+(N+1)*ldc+M);                 \
  rc0 = vec_xl(0, C+(N+2)*ldc+M);                       \
  rc0 = vec_mul(rc0, vbeta);                            \
  result[2] = vec_madd(result[2], valpha, rc0);         \
  vec_xst(result[2], 0, C+(N+2)*ldc+M);                 \
  rc0 = vec_xl(0, C+(N+3)*ldc+M);                       \
  rc0 = vec_mul(rc0, vbeta);                            \
  result[3] = vec_madd(result[3], valpha, rc0);         \
  vec_xst(result[3], 0, C+(N+3)*ldc+M);

#define SAVE_4x2_ACC(ACC, N, M)                         \
  __builtin_mma_disassemble_acc ((void *)result, ACC);  \
  rc0 = vec_xl_len(C+(N+0)*ldc+M, 8);                   \
  rc0 = vec_mul(rc0, vbeta);                            \
  result[0] = vec_madd(result[0], valpha, rc0);         \
  vec_xst_len(result[0], C+(N+0)*ldc+M, 8);             \
  rc0 = vec_xl_len(C+(N+1)*ldc+M, 8);                   \
  rc0 = vec_mul(rc0, vbeta);                            \
  result[1] = vec_madd(result[1], valpha, rc0);         \
  vec_xst_len(result[1], C+(N+1)*ldc+M, 8);             \
  rc0 = vec_xl_len(C+(N+2)*ldc+M, 8);                   \
  rc0 = vec_mul(rc0, vbeta);                            \
  result[2] = vec_madd(result[2], valpha, rc0);         \
  vec_xst_len(result[2], C+(N+2)*ldc+M, 8);             \
  rc0 = vec_xl_len(C+(N+3)*ldc+M, 8);                   \
  rc0 = vec_mul(rc0, vbeta);                            \
  result[3] = vec_madd(result[3], valpha, rc0);         \
  vec_xst_len(result[3], C+(N+3)*ldc+M, 8);

#define SAVE_2x4_ACC(ACC, N, M)                         \
  __builtin_mma_disassemble_acc ((void *)result, ACC);  \
  rc0 = vec_xl(0, C+(N+0)*ldc+M);                       \
  rc0 = vec_mul(rc0, vbeta);                            \
  result[0] = vec_madd(result[0], valpha, rc0);         \
  vec_xst(result[0], 0, C+(N+0)*ldc+M);                 \
  rc0 = vec_xl(0, C+(N+1)*ldc+M);                       \
  rc0 = vec_mul(rc0, vbeta);                            \
  result[1] = vec_madd(result[1], valpha, rc0);         \
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

#define SAVE_4x4_ACC(ACC, N, M)                         \
  __builtin_mma_disassemble_acc ((void *)result, ACC);  \
  result[0] = vec_mul(result[0], valpha);               \
  vec_xst(result[0], 0, C+(N+0)*ldc+M);                 \
  result[1] = vec_mul(result[1], valpha);               \
  vec_xst(result[1], 0, C+(N+1)*ldc+M);                 \
  result[2] = vec_mul(result[2], valpha);               \
  vec_xst(result[2], 0, C+(N+2)*ldc+M);                 \
  result[3] = vec_mul(result[3], valpha);               \
  vec_xst(result[3], 0, C+(N+3)*ldc+M);

#define SAVE_4x2_ACC(ACC, N, M)                         \
  __builtin_mma_disassemble_acc ((void *)result, ACC);  \
  result[0] = vec_mul(result[0], valpha);               \
  vec_xst_len(result[0], C+(N+0)*ldc+M, 8);             \
  result[1] = vec_mul(result[1], valpha);               \
  vec_xst_len(result[1], C+(N+1)*ldc+M, 8);             \
  result[2] = vec_mul(result[2], valpha);               \
  vec_xst_len(result[2], C+(N+2)*ldc+M, 8);             \
  result[3] = vec_mul(result[3], valpha);               \
  vec_xst_len(result[3], C+(N+3)*ldc+M, 8);

#define SAVE_2x4_ACC(ACC, N, M)                         \
  __builtin_mma_disassemble_acc ((void *)result, ACC);  \
  result[0] = vec_mul(result[0], valpha);               \
  vec_xst(result[0], 0, C+(N+0)*ldc+M);                 \
  result[1] = vec_mul(result[1], valpha);               \
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

#define INIT_1ACC()               \
  __builtin_mma_xxsetaccz(&acc0);

#define LOAD_A_1x16(K, M)           \
  ra0 = vec_xl(0, A+((K)*lda)+M+0); \
  ra1 = vec_xl(0, A+((K)*lda)+M+4); \
  ra2 = vec_xl(0, A+((K)*lda)+M+8); \
  ra3 = vec_xl(0, A+((K)*lda)+M+12);

#define LOAD_A_1x8(K, M)            \
  ra0 = vec_xl(0, A+((K)*lda)+M+0); \
  ra1 = vec_xl(0, A+((K)*lda)+M+4);

#define LOAD_A_1x4(K, M) ra0 = vec_xl(0, A+((K)*lda)+M);

#define LOAD_A_2x2(K, M)                  \
  ra0 = vec_splats(A[K*lda+M]);           \
  ra0 = vec_insert(A[K*lda+M+1], ra0, 1); \
  ra0 = vec_insert(A[K*lda+M+1], ra0, 3);

#define LOAD_A_1x2(K, M) ra0 = vec_xl_len(A+((K)*lda)+M, 8);

#define LOAD_A_1x1(K, M) ra0 = vec_splats(A[(K)*lda+M]);

#define LOAD_BT_16x4(N, K)           \
  rb0 = vec_xl(0, B+(N+0)*ldb+K);    \
  rb1 = vec_xl(0, B+(N+1)*ldb+K);    \
  t0 = vec_mergeh(rb0, rb1);         \
  t1 = vec_mergel(rb0, rb1);         \
  rb2 = vec_xl(0, B+(N+2)*ldb+K);    \
  rb3 = vec_xl(0, B+(N+3)*ldb+K);    \
  t2 = vec_mergeh(rb2, rb3);         \
  t3 = vec_mergel(rb2, rb3);         \
  rb0 = vec_xxpermdi(t0, t2, 0b00);  \
  rb1 = vec_xxpermdi(t0, t2, 0b11);  \
  rb2 = vec_xxpermdi(t1, t3, 0b00);  \
  rb3 = vec_xxpermdi(t1, t3, 0b11);  \
  rb4 = vec_xl(0, B+(N+4)*ldb+K);    \
  rb5 = vec_xl(0, B+(N+5)*ldb+K);    \
  t0 = vec_mergeh(rb4, rb5);         \
  t1 = vec_mergel(rb4, rb5);         \
  rb6 = vec_xl(0, B+(N+6)*ldb+K);    \
  rb7 = vec_xl(0, B+(N+7)*ldb+K);    \
  t2 = vec_mergeh(rb6, rb7);         \
  t3 = vec_mergel(rb6, rb7);         \
  rb4 = vec_xxpermdi(t0, t2, 0b00);  \
  rb5 = vec_xxpermdi(t0, t2, 0b11);  \
  rb6 = vec_xxpermdi(t1, t3, 0b00);  \
  rb7 = vec_xxpermdi(t1, t3, 0b11);  \
  rb8 = vec_xl(0, B+(N+8)*ldb+K);    \
  rb9 = vec_xl(0, B+(N+9)*ldb+K);    \
  t0 = vec_mergeh(rb8, rb9);         \
  t1 = vec_mergel(rb8, rb9);         \
  rb10 = vec_xl(0, B+(N+10)*ldb+K);  \
  rb11 = vec_xl(0, B+(N+11)*ldb+K);  \
  t2 = vec_mergeh(rb10, rb11);       \
  t3 = vec_mergel(rb10, rb11);       \
  rb8 = vec_xxpermdi(t0, t2, 0b00);  \
  rb9 = vec_xxpermdi(t0, t2, 0b11);  \
  rb10 = vec_xxpermdi(t1, t3, 0b00); \
  rb11 = vec_xxpermdi(t1, t3, 0b11); \
  rb12 = vec_xl(0, B+(N+12)*ldb+K);  \
  rb13 = vec_xl(0, B+(N+13)*ldb+K);  \
  t0 = vec_mergeh(rb12, rb13);       \
  t1 = vec_mergel(rb12, rb13);       \
  rb14 = vec_xl(0, B+(N+14)*ldb+K);  \
  rb15 = vec_xl(0, B+(N+15)*ldb+K);  \
  t2 = vec_mergeh(rb14, rb15);       \
  t3 = vec_mergel(rb14, rb15);       \
  rb12 = vec_xxpermdi(t0, t2, 0b00); \
  rb13 = vec_xxpermdi(t0, t2, 0b11); \
  rb14 = vec_xxpermdi(t1, t3, 0b00); \
  rb15 = vec_xxpermdi(t1, t3, 0b11);

#define LOAD_BT_16x2(N, K)              \
  rb0 = vec_xl_len(B+(N+0)*ldb+K, 8);   \
  rb1 = vec_xl_len(B+(N+1)*ldb+K, 8);   \
  t0 = vec_mergeh(rb0, rb1);            \
  rb2 = vec_xl_len(B+(N+2)*ldb+K, 8);   \
  rb3 = vec_xl_len(B+(N+3)*ldb+K, 8);   \
  t1 = vec_mergeh(rb2, rb3);            \
  rb0 = vec_xxpermdi(t0, t1, 0b00);     \
  rb1 = vec_xxpermdi(t0, t1, 0b11);     \
  rb4 = vec_xl_len(B+(N+4)*ldb+K, 8);   \
  rb5 = vec_xl_len(B+(N+5)*ldb+K, 8);   \
  t0 = vec_mergeh(rb4, rb5);            \
  rb6 = vec_xl_len(B+(N+6)*ldb+K, 8);   \
  rb7 = vec_xl_len(B+(N+7)*ldb+K, 8);   \
  t1 = vec_mergeh(rb6, rb7);            \
  rb2 = vec_xxpermdi(t0, t1, 0b00);     \
  rb3 = vec_xxpermdi(t0, t1, 0b11);     \
  rb8 = vec_xl_len(B+(N+8)*ldb+K, 8);   \
  rb9 = vec_xl_len(B+(N+9)*ldb+K, 8);   \
  t0 = vec_mergeh(rb8, rb9);            \
  rb10 = vec_xl_len(B+(N+10)*ldb+K, 8); \
  rb11 = vec_xl_len(B+(N+11)*ldb+K, 8); \
  t1 = vec_mergeh(rb10, rb11);          \
  rb4 = vec_xxpermdi(t0, t1, 0b00);     \
  rb5 = vec_xxpermdi(t0, t1, 0b11);     \
  rb12 = vec_xl_len(B+(N+12)*ldb+K, 8); \
  rb13 = vec_xl_len(B+(N+13)*ldb+K, 8); \
  t0 = vec_mergeh(rb12, rb13);          \
  rb14 = vec_xl_len(B+(N+14)*ldb+K, 8); \
  rb15 = vec_xl_len(B+(N+15)*ldb+K, 8); \
  t1 = vec_mergeh(rb14, rb15);          \
  rb6 = vec_xxpermdi(t0, t1, 0b00);     \
  rb7 = vec_xxpermdi(t0, t1, 0b11);

#define LOAD_BT_16x1(N, K)                   \
  rb0 = vec_xor(rb0, rb0);                   \
  rb0 = vec_insert(B[(N+0)*ldb+K], rb0, 0);  \
  rb0 = vec_insert(B[(N+1)*ldb+K], rb0, 1);  \
  rb0 = vec_insert(B[(N+2)*ldb+K], rb0, 2);  \
  rb0 = vec_insert(B[(N+3)*ldb+K], rb0, 3);  \
  rb1 = vec_xor(rb1, rb1);                   \
  rb1 = vec_insert(B[(N+4)*ldb+K], rb1, 0);  \
  rb1 = vec_insert(B[(N+5)*ldb+K], rb1, 1);  \
  rb1 = vec_insert(B[(N+6)*ldb+K], rb1, 2);  \
  rb1 = vec_insert(B[(N+7)*ldb+K], rb1, 3);  \
  rb2 = vec_xor(rb2, rb2);                   \
  rb2 = vec_insert(B[(N+8)*ldb+K], rb2, 0);  \
  rb2 = vec_insert(B[(N+9)*ldb+K], rb2, 1);  \
  rb2 = vec_insert(B[(N+10)*ldb+K], rb2, 2); \
  rb2 = vec_insert(B[(N+11)*ldb+K], rb2, 3); \
  rb3 = vec_xor(rb3, rb3);                   \
  rb3 = vec_insert(B[(N+12)*ldb+K], rb3, 0); \
  rb3 = vec_insert(B[(N+13)*ldb+K], rb3, 1); \
  rb3 = vec_insert(B[(N+14)*ldb+K], rb3, 2); \
  rb3 = vec_insert(B[(N+15)*ldb+K], rb3, 3);

#define LOAD_BT_8x4(N, K)           \
  rb0 = vec_xl(0, B+(N+0)*ldb+K);   \
  rb1 = vec_xl(0, B+(N+1)*ldb+K);   \
  t0 = vec_mergeh(rb0, rb1);        \
  t1 = vec_mergel(rb0, rb1);        \
  rb2 = vec_xl(0, B+(N+2)*ldb+K);   \
  rb3 = vec_xl(0, B+(N+3)*ldb+K);   \
  t2 = vec_mergeh(rb2, rb3);        \
  t3 = vec_mergel(rb2, rb3);        \
  rb0 = vec_xxpermdi(t0, t2, 0b00); \
  rb1 = vec_xxpermdi(t0, t2, 0b11); \
  rb2 = vec_xxpermdi(t1, t3, 0b00); \
  rb3 = vec_xxpermdi(t1, t3, 0b11); \
  rb4 = vec_xl(0, B+(N+4)*ldb+K);   \
  rb5 = vec_xl(0, B+(N+5)*ldb+K);   \
  t0 = vec_mergeh(rb4, rb5);        \
  t1 = vec_mergel(rb4, rb5);        \
  rb6 = vec_xl(0, B+(N+6)*ldb+K);   \
  rb7 = vec_xl(0, B+(N+7)*ldb+K);   \
  t2 = vec_mergeh(rb6, rb7);        \
  t3 = vec_mergel(rb6, rb7);        \
  rb4 = vec_xxpermdi(t0, t2, 0b00); \
  rb5 = vec_xxpermdi(t0, t2, 0b11); \
  rb6 = vec_xxpermdi(t1, t3, 0b00); \
  rb7 = vec_xxpermdi(t1, t3, 0b11);

#define LOAD_BT_8x2(N, K)             \
  rb0 = vec_xl_len(B+(N+0)*ldb+K, 8); \
  rb1 = vec_xl_len(B+(N+1)*ldb+K, 8); \
  t0 = vec_mergeh(rb0, rb1);          \
  rb2 = vec_xl_len(B+(N+2)*ldb+K, 8); \
  rb3 = vec_xl_len(B+(N+3)*ldb+K, 8); \
  t1 = vec_mergeh(rb2, rb3);          \
  rb0 = vec_xxpermdi(t0, t1, 0b00);   \
  rb1 = vec_xxpermdi(t0, t1, 0b11);   \
  rb4 = vec_xl_len(B+(N+4)*ldb+K, 8); \
  rb5 = vec_xl_len(B+(N+5)*ldb+K, 8); \
  t0 = vec_mergeh(rb4, rb5);          \
  rb6 = vec_xl_len(B+(N+6)*ldb+K, 8); \
  rb7 = vec_xl_len(B+(N+7)*ldb+K, 8); \
  t1 = vec_mergeh(rb6, rb7);          \
  rb2 = vec_xxpermdi(t0, t1, 0b00);   \
  rb3 = vec_xxpermdi(t0, t1, 0b11);

#define LOAD_BT_8x1(N, K)                   \
  rb0 = vec_xor(rb0, rb0);                  \
  rb0 = vec_insert(B[(N+0)*ldb+K], rb0, 0); \
  rb0 = vec_insert(B[(N+1)*ldb+K], rb0, 1); \
  rb0 = vec_insert(B[(N+2)*ldb+K], rb0, 2); \
  rb0 = vec_insert(B[(N+3)*ldb+K], rb0, 3); \
  rb1 = vec_xor(rb1, rb1);                  \
  rb1 = vec_insert(B[(N+4)*ldb+K], rb1, 0); \
  rb1 = vec_insert(B[(N+5)*ldb+K], rb1, 1); \
  rb1 = vec_insert(B[(N+6)*ldb+K], rb1, 2); \
  rb1 = vec_insert(B[(N+7)*ldb+K], rb1, 3);

#define LOAD_BT_4x4(N, K)           \
  rb0 = vec_xl(0, B+(N+0)*ldb+K);   \
  rb1 = vec_xl(0, B+(N+1)*ldb+K);   \
  t0 = vec_mergeh(rb0, rb1);        \
  t1 = vec_mergel(rb0, rb1);        \
  rb2 = vec_xl(0, B+(N+2)*ldb+K);   \
  rb3 = vec_xl(0, B+(N+3)*ldb+K);   \
  t2 = vec_mergeh(rb2, rb3);        \
  t3 = vec_mergel(rb2, rb3);        \
  rb0 = vec_xxpermdi(t0, t2, 0b00); \
  rb1 = vec_xxpermdi(t0, t2, 0b11); \
  rb2 = vec_xxpermdi(t1, t3, 0b00); \
  rb3 = vec_xxpermdi(t1, t3, 0b11);

#define LOAD_BT_4x2(N, K)             \
  rb0 = vec_xl_len(B+(N+0)*ldb+K, 8); \
  rb1 = vec_xl_len(B+(N+1)*ldb+K, 8); \
  t0 = vec_mergeh(rb0, rb1);          \
  rb2 = vec_xl_len(B+(N+2)*ldb+K, 8); \
  rb3 = vec_xl_len(B+(N+3)*ldb+K, 8); \
  t1 = vec_mergeh(rb2, rb3);          \
  rb0 = vec_xxpermdi(t0, t1, 0b00);   \
  rb1 = vec_xxpermdi(t0, t1, 0b11);

#define LOAD_BT_4x1(N, K)                   \
  rb0 = vec_xor(rb0, rb0);                  \
  rb0 = vec_insert(B[(N+0)*ldb+K], rb0, 0); \
  rb0 = vec_insert(B[(N+1)*ldb+K], rb0, 1); \
  rb0 = vec_insert(B[(N+2)*ldb+K], rb0, 2); \
  rb0 = vec_insert(B[(N+3)*ldb+K], rb0, 3);

#define LOAD_BT_2x4(N, K)                       \
  rb0 = vec_xl(0, B+(N+0)*ldb+K);               \
  rb1 = vec_xl(0, B+(N+1)*ldb+K);               \
  t0 = vec_mergeh(rb0, rb1);                    \
  t1 = vec_mergeo(rb0, rb1);                    \
  t2 = vec_mergel(rb0, rb1);                    \
  rb0 = t0;                                     \
  rb1 = t1;                                     \
  rb2 = t2;                                     \
  rb3 = vec_xor(rb3, rb3);                      \
  rb3 = vec_insert(vec_extract(t2,2), rb3, 0);  \
  rb3 = vec_insert(vec_extract(t2,3), rb3, 1);

#define LOAD_BT_2x2(N, K)             \
  rb0 = vec_xl_len(B+(N+0)*ldb+K, 8); \
  rb1 = vec_xl_len(B+(N+1)*ldb+K, 8); \
  t0 = vec_mergee(rb0, rb1);          \
  t1 = vec_mergeo(rb0, rb1);          \
  rb0 = t0;                           \
  rb1 = t1;

#define LOAD_BT_2x1(N, K)                   \
  rb0 = vec_xor(rb0, rb0);                  \
  rb0 = vec_insert(B[(N+0)*ldb+K], rb0, 0); \
  rb0 = vec_insert(B[(N+1)*ldb+K], rb0, 1);

#define LOAD_B_2x2(N, K)                    \
  rb0 = vec_splats(B[(N+0)*ldb+K]);         \
  rb0 = vec_insert(B[(N+1)*ldb+K], rb0, 2); \
  rb0 = vec_insert(B[(N+1)*ldb+K], rb0, 3);

#define LOAD_B_2x1(N, K)                    \
  rb0 = vec_insert(B[(n+0)*ldb+k], rb0, 0); \
  rb0 = vec_insert(B[(n+1)*ldb+k], rb0, 1);

#define LOAD_B_1x1(N, K) rb0 = vec_splats(B[(N)*ldb+K]);

#define KERNEL_MMA_8ACC(b0, b1, b2, b3, b4, b5, b6, b7,   \
                        a0, a1, a2, a3, a4, a5, a6, a7)   \
  __builtin_mma_xvf32gerpp(&acc0, (vec_t)b0, (vec_t)a0);  \
  __builtin_mma_xvf32gerpp(&acc1, (vec_t)b1, (vec_t)a1);  \
  __builtin_mma_xvf32gerpp(&acc2, (vec_t)b2, (vec_t)a2);  \
  __builtin_mma_xvf32gerpp(&acc3, (vec_t)b3, (vec_t)a3);  \
  __builtin_mma_xvf32gerpp(&acc4, (vec_t)b4, (vec_t)a4);  \
  __builtin_mma_xvf32gerpp(&acc5, (vec_t)b5, (vec_t)a5);  \
  __builtin_mma_xvf32gerpp(&acc6, (vec_t)b6, (vec_t)a6);  \
  __builtin_mma_xvf32gerpp(&acc7, (vec_t)b7, (vec_t)a7);

#define KERNEL_MMA_4ACC(b0, b1, b2, b3, a0, a1, a2, a3)   \
  __builtin_mma_xvf32gerpp(&acc0, (vec_t)b0, (vec_t)a0);  \
  __builtin_mma_xvf32gerpp(&acc1, (vec_t)b1, (vec_t)a1);  \
  __builtin_mma_xvf32gerpp(&acc2, (vec_t)b2, (vec_t)a2);  \
  __builtin_mma_xvf32gerpp(&acc3, (vec_t)b3, (vec_t)a3);

#define KERNEL_MMA_2ACC(b0, b1, a0, a1)                   \
  __builtin_mma_xvf32gerpp(&acc0, (vec_t)b0, (vec_t)a0);  \
  __builtin_mma_xvf32gerpp(&acc1, (vec_t)b1, (vec_t)a1);

#define KERNEL_MMA_1ACC(b0, a0)                           \
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

#define PACK_B(rb0, rb1, rb2, rb3, offset) \
  vec_xst(rb0, 0, packB+(k*16)+0+offset);  \
  vec_xst(rb1, 0, packB+(k*16)+4+offset);  \
  vec_xst(rb2, 0, packB+(k*16)+8+offset);  \
  vec_xst(rb3, 0, packB+(k*16)+12+offset);

#define LOAD_PACKED_B(rb0, rb1, rb2, rb3, offset) \
  rb0 = vec_xl(0, packB+(k*16)+0+offset);         \
  rb1 = vec_xl(0, packB+(k*16)+4+offset);         \
  rb2 = vec_xl(0, packB+(k*16)+8+offset);         \
  rb3 = vec_xl(0, packB+(k*16)+12+offset);

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

  BLASLONG k4 = K & ~3;
  BLASLONG k2 = K & ~1;

#if defined(__GNUC__) && !defined(__clang__)
  int has_packing = (M >= 32 && N >= 32 && K >= 32) ? 1 : 0;
#else
  int has_packing = 0;
#endif

  float *packB;
  if (has_packing) packB = (float *)malloc(K*16*sizeof(float));

  vector float valpha = vec_splats(alpha);
#if !defined(B0)
  vector float vbeta = vec_splats(beta);
#endif

  for (n = 0; n < n16; n += 16) {
    for (m = 0; m < m8; m += 8) {
      __vector_quad acc0, acc1, acc2, acc3, acc4, acc5, acc6, acc7;

      INIT_8ACCS();

      register vector float ra0, ra1;
      register vector float rb0, rb1, rb2, rb3, rb4, rb5, rb6, rb7, rb8, rb9,
          rb10, rb11, rb12, rb13, rb14, rb15;
      register vector float t0, t1, t2, t3;

      if (has_packing) {
        if (m == 0) {
          for (k = 0; k < k4; k += 4) {
            LOAD_A_1x8(k, m);
            LOAD_BT_16x4(n, k);
            KERNEL_MMA_8ACC(rb0, rb4, rb8, rb12, rb0, rb4, rb8, rb12,
                            ra0, ra0, ra0, ra0, ra1, ra1, ra1, ra1);
            PACK_B(rb0, rb4, rb8, rb12, 0);
            LOAD_A_1x8(k+1, m);
            KERNEL_MMA_8ACC(rb1, rb5, rb9, rb13, rb1, rb5, rb9, rb13,
                            ra0, ra0, ra0, ra0, ra1, ra1, ra1, ra1);
            PACK_B(rb1, rb5, rb9, rb13, 16);
            LOAD_A_1x8(k+2, m);
            KERNEL_MMA_8ACC(rb2, rb6, rb10, rb14, rb2, rb6, rb10, rb14,
                            ra0, ra0, ra0, ra0, ra1, ra1, ra1, ra1);
            PACK_B(rb2, rb6, rb10, rb14, 32);
            LOAD_A_1x8(k+3, m);
            KERNEL_MMA_8ACC(rb3, rb7, rb11, rb15, rb3, rb7, rb11, rb15,
                            ra0, ra0, ra0, ra0, ra1, ra1, ra1, ra1);
            PACK_B(rb3, rb7, rb11, rb15, 48);
          }
          for (; k < k2; k += 2) {
            LOAD_A_1x8(k, m);
            LOAD_BT_16x2(n, k);
            KERNEL_MMA_8ACC(rb0, rb2, rb4, rb6, rb0, rb2, rb4, rb6,
                            ra0, ra0, ra0, ra0, ra1, ra1, ra1, ra1);
            PACK_B(rb0, rb2, rb4, rb6, 0);
            LOAD_A_1x8(k+1, m);
            KERNEL_MMA_8ACC(rb1, rb3, rb5, rb7, rb1, rb3, rb5, rb7,
                            ra0, ra0, ra0, ra0, ra1, ra1, ra1, ra1);
            PACK_B(rb1, rb3, rb5, rb7, 16);
          }
          for (; k < K; k++) {
            LOAD_A_1x8(k, m);
            LOAD_BT_16x1(n, k);
            KERNEL_MMA_8ACC(rb0, rb1, rb2, rb3, rb0, rb1, rb2, rb3,
                            ra0, ra0, ra0, ra0, ra1, ra1, ra1, ra1);
            PACK_B(rb0, rb1, rb2, rb3, 0);
          }
        } else {
          for (k = 0; k < k4; k += 4) {
            LOAD_A_1x8(k, m);
            LOAD_PACKED_B(rb0, rb4, rb8, rb12, 0);
            KERNEL_MMA_8ACC(rb0, rb4, rb8, rb12, rb0, rb4, rb8, rb12,
                            ra0, ra0, ra0, ra0, ra1, ra1, ra1, ra1);
            LOAD_A_1x8(k+1, m);
            LOAD_PACKED_B(rb1, rb5, rb9, rb13, 16);
            KERNEL_MMA_8ACC(rb1, rb5, rb9, rb13, rb1, rb5, rb9, rb13,
                            ra0, ra0, ra0, ra0, ra1, ra1, ra1, ra1);
            LOAD_A_1x8(k+2, m);
            LOAD_PACKED_B(rb2, rb6, rb10, rb14, 32);
            KERNEL_MMA_8ACC(rb2, rb6, rb10, rb14, rb2, rb6, rb10, rb14,
                            ra0, ra0, ra0, ra0, ra1, ra1, ra1, ra1);
            LOAD_A_1x8(k+3, m);
            LOAD_PACKED_B(rb3, rb7, rb11, rb15, 48);
            KERNEL_MMA_8ACC(rb3, rb7, rb11, rb15, rb3, rb7, rb11, rb15,
                            ra0, ra0, ra0, ra0, ra1, ra1, ra1, ra1);
          }
          for (; k < k2; k += 2) {
            LOAD_A_1x8(k, m);
            LOAD_PACKED_B(rb0, rb2, rb4, rb6, 0);
            KERNEL_MMA_8ACC(rb0, rb2, rb4, rb6, rb0, rb2, rb4, rb6,
                            ra0, ra0, ra0, ra0, ra1, ra1, ra1, ra1);
            LOAD_A_1x8(k+1, m);
            LOAD_PACKED_B(rb1, rb3, rb5, rb7, 16);
            KERNEL_MMA_8ACC(rb1, rb3, rb5, rb7, rb1, rb3, rb5, rb7,
                            ra0, ra0, ra0, ra0, ra1, ra1, ra1, ra1);
          }
          for (; k < K; k++) {
            LOAD_A_1x8(k, m);
            LOAD_PACKED_B(rb0, rb1, rb2, rb3, 0);
            KERNEL_MMA_8ACC(rb0, rb1, rb2, rb3, rb0, rb1, rb2, rb3,
                            ra0, ra0, ra0, ra0, ra1, ra1, ra1, ra1);
          }
        }
      } else {
        for (k = 0; k < k4; k += 4) {
          LOAD_A_1x8(k, m);
          LOAD_BT_16x4(n, k);
          KERNEL_MMA_8ACC(rb0, rb4, rb8, rb12, rb0, rb4, rb8, rb12,
                          ra0, ra0, ra0, ra0, ra1, ra1, ra1, ra1);
          LOAD_A_1x8(k+1, m);
          KERNEL_MMA_8ACC(rb1, rb5, rb9, rb13, rb1, rb5, rb9, rb13,
                          ra0, ra0, ra0, ra0, ra1, ra1, ra1, ra1);
          LOAD_A_1x8(k+2, m);
          KERNEL_MMA_8ACC(rb2, rb6, rb10, rb14, rb2, rb6, rb10, rb14,
                          ra0, ra0, ra0, ra0, ra1, ra1, ra1, ra1);
          LOAD_A_1x8(k+3, m);
          KERNEL_MMA_8ACC(rb3, rb7, rb11, rb15, rb3, rb7, rb11, rb15,
                          ra0, ra0, ra0, ra0, ra1, ra1, ra1, ra1);
        }
        for (; k < k2; k += 2) {
          LOAD_A_1x8(k, m);
          LOAD_BT_16x2(n, k);
          KERNEL_MMA_8ACC(rb0, rb2, rb4, rb6, rb0, rb2, rb4, rb6,
                          ra0, ra0, ra0, ra0, ra1, ra1, ra1, ra1);
          LOAD_A_1x8(k+1, m);
          KERNEL_MMA_8ACC(rb1, rb3, rb5, rb7, rb1, rb3, rb5, rb7,
                          ra0, ra0, ra0, ra0, ra1, ra1, ra1, ra1);
        }
        for (; k < K; k++) {
          LOAD_A_1x8(k, m);
          LOAD_BT_16x1(n, k);
          KERNEL_MMA_8ACC(rb0, rb1, rb2, rb3, rb0, rb1, rb2, rb3,
                          ra0, ra0, ra0, ra0, ra1, ra1, ra1, ra1);
        }
      }

#if !defined(B0)
      register vector float rc0;
#endif
      vector float result[4];
      SAVE_4x4_ACC(&acc0, n+0, m+0);
      SAVE_4x4_ACC(&acc1, n+4, m+0);
      SAVE_4x4_ACC(&acc2, n+8, m+0);
      SAVE_4x4_ACC(&acc3, n+12, m+0);
      SAVE_4x4_ACC(&acc4, n+0, m+4);
      SAVE_4x4_ACC(&acc5, n+4, m+4);
      SAVE_4x4_ACC(&acc6, n+8, m+4);
      SAVE_4x4_ACC(&acc7, n+12, m+4);
    }

    for (; m < m4; m += 4) {
      __vector_quad acc0, acc1, acc2, acc3;

      INIT_4ACCS();

      register vector float ra0;
      register vector float rb0, rb1, rb2, rb3, rb4, rb5, rb6, rb7, rb8, rb9,
          rb10, rb11, rb12, rb13, rb14, rb15;
      register vector float t0, t1, t2, t3;

      if (!has_packing) {
        for (k = 0; k < k4; k += 4) {
          LOAD_A_1x4(k, m);
          LOAD_BT_16x4(n, k);
          KERNEL_MMA_4ACC(rb0, rb4, rb8, rb12, ra0, ra0, ra0, ra0);
          LOAD_A_1x4(k+1, m);
          KERNEL_MMA_4ACC(rb1, rb5, rb9, rb13, ra0, ra0, ra0, ra0);
          LOAD_A_1x4(k+2, m);
          KERNEL_MMA_4ACC(rb2, rb6, rb10, rb14, ra0, ra0, ra0, ra0);
          LOAD_A_1x4(k+3, m);
          KERNEL_MMA_4ACC(rb3, rb7, rb11, rb15, ra0, ra0, ra0, ra0);
        }
        for (; k < k2; k += 2) {
          LOAD_A_1x4(k, m);
          LOAD_BT_16x2(n, k);
          KERNEL_MMA_4ACC(rb0, rb2, rb4, rb6, ra0, ra0, ra0, ra0);
          LOAD_A_1x4(k+1, m);
          KERNEL_MMA_4ACC(rb1, rb3, rb5, rb7, ra0, ra0, ra0, ra0);
        }
        for (; k < K; k++) {
          LOAD_A_1x4(k, m);
          LOAD_BT_16x1(n, k);
          KERNEL_MMA_4ACC(rb0, rb1, rb2, rb3, ra0, ra0, ra0, ra0);
        }
      } else {
        for (k = 0; k < k4; k += 4) {
          LOAD_A_1x4(k, m);
          LOAD_PACKED_B(rb0, rb4, rb8, rb12, 0);
          KERNEL_MMA_4ACC(rb0, rb4, rb8, rb12, ra0, ra0, ra0, ra0);
          LOAD_A_1x4(k+1, m);
          LOAD_PACKED_B(rb1, rb5, rb9, rb13, 16);
          KERNEL_MMA_4ACC(rb1, rb5, rb9, rb13, ra0, ra0, ra0, ra0);
          LOAD_A_1x4(k+2, m);
          LOAD_PACKED_B(rb2, rb6, rb10, rb14, 32);
          KERNEL_MMA_4ACC(rb2, rb6, rb10, rb14, ra0, ra0, ra0, ra0);
          LOAD_A_1x4(k+3, m);
          LOAD_PACKED_B(rb3, rb7, rb11, rb15, 48);
          KERNEL_MMA_4ACC(rb3, rb7, rb11, rb15, ra0, ra0, ra0, ra0);
        }
        for (; k < k2; k += 2) {
          LOAD_A_1x4(k, m);
          LOAD_PACKED_B(rb0, rb2, rb4, rb6, 0);
          KERNEL_MMA_4ACC(rb0, rb2, rb4, rb6, ra0, ra0, ra0, ra0);
          LOAD_A_1x4(k+1, m);
          LOAD_PACKED_B(rb1, rb3, rb5, rb7, 16);
          KERNEL_MMA_4ACC(rb1, rb3, rb5, rb7, ra0, ra0, ra0, ra0);
        }
        for (; k < K; k++) {
          LOAD_A_1x4(k, m);
          LOAD_PACKED_B(rb0, rb1, rb2, rb3, 0);
          KERNEL_MMA_4ACC(rb0, rb1, rb2, rb3, ra0, ra0, ra0, ra0);
        }
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

    for (; m < m2; m += 2) {
      __vector_quad acc0, acc1, acc2, acc3;

      INIT_4ACCS();

      register vector float ra0;
      register vector float rb0, rb1, rb2, rb3, rb4, rb5, rb6, rb7, rb8, rb9,
          rb10, rb11, rb12, rb13, rb14, rb15;
      register vector float t0, t1, t2, t3;

      if (!has_packing) {
        for (k = 0; k < k4; k += 4) {
          LOAD_A_1x2(k, m);
          LOAD_BT_16x4(n, k);
          KERNEL_MMA_4ACC(rb0, rb4, rb8, rb12, ra0, ra0, ra0, ra0);
          LOAD_A_1x2(k+1, m);
          KERNEL_MMA_4ACC(rb1, rb5, rb9, rb13, ra0, ra0, ra0, ra0);
          LOAD_A_1x2(k+2, m);
          KERNEL_MMA_4ACC(rb2, rb6, rb10, rb14, ra0, ra0, ra0, ra0);
          LOAD_A_1x2(k+3, m);
          KERNEL_MMA_4ACC(rb3, rb7, rb11, rb15, ra0, ra0, ra0, ra0);
        }
        for (; k < k2; k += 2) {
          LOAD_A_1x2(k, m);
          LOAD_BT_16x2(n, k);
          KERNEL_MMA_4ACC(rb0, rb2, rb4, rb6, ra0, ra0, ra0, ra0);
          LOAD_A_1x2(k+1, m);
          KERNEL_MMA_4ACC(rb1, rb3, rb5, rb7, ra0, ra0, ra0, ra0);
        }
        for (; k < K; k++) {
          LOAD_A_1x2(k, m);
          LOAD_BT_16x1(n, k);
          KERNEL_MMA_4ACC(rb0, rb1, rb2, rb3, ra0, ra0, ra0, ra0);
        }
      } else {
        for (k = 0; k < k4; k += 4) {
          LOAD_A_1x2(k, m);
          LOAD_PACKED_B(rb0, rb4, rb8, rb12, 0);
          KERNEL_MMA_4ACC(rb0, rb4, rb8, rb12, ra0, ra0, ra0, ra0);
          LOAD_A_1x2(k+1, m);
          LOAD_PACKED_B(rb1, rb5, rb9, rb13, 16);
          KERNEL_MMA_4ACC(rb1, rb5, rb9, rb13, ra0, ra0, ra0, ra0);
          LOAD_A_1x2(k+2, m);
          LOAD_PACKED_B(rb2, rb6, rb10, rb14, 32);
          KERNEL_MMA_4ACC(rb2, rb6, rb10, rb14, ra0, ra0, ra0, ra0);
          LOAD_A_1x2(k+3, m);
          LOAD_PACKED_B(rb3, rb7, rb11, rb15, 48);
          KERNEL_MMA_4ACC(rb3, rb7, rb11, rb15, ra0, ra0, ra0, ra0);
        }
        for (; k < k2; k += 2) {
          LOAD_A_1x2(k, m);
          LOAD_PACKED_B(rb0, rb2, rb4, rb6, 0);
          KERNEL_MMA_4ACC(rb0, rb2, rb4, rb6, ra0, ra0, ra0, ra0);
          LOAD_A_1x2(k+1, m);
          LOAD_PACKED_B(rb1, rb3, rb5, rb7, 16);
          KERNEL_MMA_4ACC(rb1, rb3, rb5, rb7, ra0, ra0, ra0, ra0);
        }
        for (; k < K; k++) {
          LOAD_A_1x2(k, m);
          LOAD_PACKED_B(rb0, rb1, rb2, rb3, 0);
          KERNEL_MMA_4ACC(rb0, rb1, rb2, rb3, ra0, ra0, ra0, ra0);
        }
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

    for (; m < M; m++) {
      register vector float ra0;
      register vector float rb0, rb1, rb2, rb3, rb4, rb5, rb6, rb7, rb8, rb9,
          rb10, rb11, rb12, rb13, rb14, rb15;
      register vector float t0, t1, t2, t3;

      vector float result = ((vector float){0.,0.,0.,0.});
      vector float result1 = ((vector float){0.,0.,0.,0.});
      vector float result2 = ((vector float){0.,0.,0.,0.});
      vector float result3 = ((vector float){0.,0.,0.,0.});

      if (!has_packing) {
        for (k = 0; k < k4; k += 4) {
          LOAD_A_1x1(k, m);
          LOAD_BT_16x4(n, k);
          KERNEL_VMADD_4VSR(ra0, ra0, ra0, ra0, rb0, rb4, rb8, rb12);
          LOAD_A_1x1(k+1, m);
          KERNEL_VMADD_4VSR(ra0, ra0, ra0, ra0, rb1, rb5, rb9, rb13);
          LOAD_A_1x1(k+2, m);
          KERNEL_VMADD_4VSR(ra0, ra0, ra0, ra0, rb2, rb6, rb10, rb14);
          LOAD_A_1x1(k+3, m);
          KERNEL_VMADD_4VSR(ra0, ra0, ra0, ra0, rb3, rb7, rb11, rb15);
        }
        for (; k < k2; k += 2) {
          LOAD_A_1x1(k, m);
          LOAD_BT_16x2(n, k);
          KERNEL_VMADD_4VSR(ra0, ra0, ra0, ra0, rb0, rb2, rb4, rb6);
          LOAD_A_1x1(k+1, m);
          KERNEL_VMADD_4VSR(ra0, ra0, ra0, ra0, rb1, rb3, rb5, rb7);
        }
        for (; k < K; k++) {
          LOAD_A_1x1(k, m);
          LOAD_BT_16x1(n, k);
          KERNEL_VMADD_4VSR(ra0, ra0, ra0, ra0, rb0, rb1, rb2, rb3);
        }
      } else {
        for (k = 0; k < k4; k += 4) {
          LOAD_A_1x1(k, m);
          LOAD_PACKED_B(rb0, rb4, rb8, rb12, 0);
          KERNEL_VMADD_4VSR(ra0, ra0, ra0, ra0, rb0, rb4, rb8, rb12);
          LOAD_A_1x1(k+1, m);
          LOAD_PACKED_B(rb1, rb5, rb9, rb13, 16);
          KERNEL_VMADD_4VSR(ra0, ra0, ra0, ra0, rb1, rb5, rb9, rb13);
          LOAD_A_1x1(k+2, m);
          LOAD_PACKED_B(rb2, rb6, rb10, rb14, 32);
          KERNEL_VMADD_4VSR(ra0, ra0, ra0, ra0, rb2, rb6, rb10, rb14);
          LOAD_A_1x1(k+3, m);
          LOAD_PACKED_B(rb3, rb7, rb11, rb15, 48);
          KERNEL_VMADD_4VSR(ra0, ra0, ra0, ra0, rb3, rb7, rb11, rb15);
        }
        for (; k < k2; k += 2) {
          LOAD_A_1x1(k, m);
          LOAD_PACKED_B(rb0, rb2, rb4, rb6, 0);
          KERNEL_VMADD_4VSR(ra0, ra0, ra0, ra0, rb0, rb2, rb4, rb6);
          LOAD_A_1x1(k+1, m);
          LOAD_PACKED_B(rb1, rb3, rb5, rb7, 16);
          KERNEL_VMADD_4VSR(ra0, ra0, ra0, ra0, rb1, rb3, rb5, rb7);
        }
        for (; k < K; k++) {
          LOAD_A_1x1(k, m);
          LOAD_PACKED_B(rb0, rb1, rb2, rb3, 0);
          KERNEL_VMADD_4VSR(ra0, ra0, ra0, ra0, rb0, rb1, rb2, rb3);
        }
      }

      SAVE_4x1_VSR(result, n+0, m);
      SAVE_4x1_VSR(result1, n+4, m);
      SAVE_4x1_VSR(result2, n+8, m);
      SAVE_4x1_VSR(result3, n+12, m);
    }
  }

  for (; n < n8; n += 8) {
    for (m = 0; m < m16; m += 16) {
      __vector_quad acc0, acc1, acc2, acc3, acc4, acc5, acc6, acc7;

      INIT_8ACCS();

      register vector float ra0, ra1, ra2, ra3;
      register vector float rb0, rb1, rb2, rb3, rb4, rb5, rb6, rb7;
      register vector float t0, t1, t2, t3;

      for (k = 0; k < k4; k += 4) {
        LOAD_A_1x16(k, m);
        LOAD_BT_8x4(n, k);
        KERNEL_MMA_8ACC(rb0, rb4, rb0, rb4, rb0, rb4, rb0, rb4,
                        ra0, ra0, ra1, ra1, ra2, ra2, ra3, ra3);
        LOAD_A_1x16(k+1, m);
        KERNEL_MMA_8ACC(rb1, rb5, rb1, rb5, rb1, rb5, rb1, rb5,
                        ra0, ra0, ra1, ra1, ra2, ra2, ra3, ra3);
        LOAD_A_1x16(k+2, m);
        KERNEL_MMA_8ACC(rb2, rb6, rb2, rb6, rb2, rb6, rb2, rb6,
                        ra0, ra0, ra1, ra1, ra2, ra2, ra3, ra3);
        LOAD_A_1x16(k+3, m);
        KERNEL_MMA_8ACC(rb3, rb7, rb3, rb7, rb3, rb7, rb3, rb7,
                        ra0, ra0, ra1, ra1, ra2, ra2, ra3, ra3);
      }
      for (; k < k2; k += 2) {
        LOAD_A_1x16(k, m);
        LOAD_BT_8x2(n, k);
        KERNEL_MMA_8ACC(rb0, rb2, rb0, rb2, rb0, rb2, rb0, rb2,
                        ra0, ra0, ra1, ra1, ra2, ra2, ra3, ra3);
        LOAD_A_1x16(k+1, m);
        KERNEL_MMA_8ACC(rb1, rb3, rb1, rb3, rb1, rb3, rb1, rb3,
                        ra0, ra0, ra1, ra1, ra2, ra2, ra3, ra3);
      }
      for (; k < K; k++) {
        LOAD_A_1x16(k, m);
        LOAD_BT_8x1(n, k);
        KERNEL_MMA_8ACC(rb0, rb1, rb0, rb1, rb0, rb1, rb0, rb1,
                        ra0, ra0, ra1, ra1, ra2, ra2, ra3, ra3);
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

    for (; m < m8; m += 8) {
      __vector_quad acc0, acc1, acc2, acc3;

      INIT_4ACCS();

      register vector float ra0, ra1;
      register vector float rb0, rb1, rb2, rb3, rb4, rb5, rb6, rb7;
      register vector float t0, t1, t2, t3;

      for (k = 0; k < k4; k += 4) {
        LOAD_A_1x8(k, m);
        LOAD_BT_8x4(n, k);
        KERNEL_MMA_4ACC(rb0, rb4, rb0, rb4, ra0, ra0, ra1, ra1);
        LOAD_A_1x8(k+1, m);
        KERNEL_MMA_4ACC(rb1, rb5, rb1, rb5, ra0, ra0, ra1, ra1);
        LOAD_A_1x8(k+2, m);
        KERNEL_MMA_4ACC(rb2, rb6, rb2, rb6, ra0, ra0, ra1, ra1);
        LOAD_A_1x8(k+3, m);
        KERNEL_MMA_4ACC(rb3, rb7, rb3, rb7, ra0, ra0, ra1, ra1);
      }
      for (; k < k2; k += 2) {
        LOAD_A_1x8(k, m);
        LOAD_BT_8x2(n, k);
        KERNEL_MMA_4ACC(rb0, rb2, rb0, rb2, ra0, ra0, ra1, ra1);
        LOAD_A_1x8(k+1, m);
        KERNEL_MMA_4ACC(rb1, rb3, rb1, rb3, ra0, ra0, ra1, ra1);
      }
      for (; k < K; k++) {
        LOAD_A_1x8(k, m);
        LOAD_BT_8x1(n, k);
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

    for (; m < m4; m += 4) {
      __vector_quad acc0, acc1;

      INIT_2ACCS();

      register vector float ra0;
      register vector float rb0, rb1, rb2, rb3, rb4, rb5, rb6, rb7;
      register vector float t0, t1, t2, t3;

      for (k = 0; k < k4; k += 4) {
        LOAD_A_1x4(k, m);
        LOAD_BT_8x4(n, k);
        KERNEL_MMA_2ACC(rb0, rb4, ra0, ra0);
        LOAD_A_1x4(k+1, m);
        KERNEL_MMA_2ACC(rb1, rb5, ra0, ra0);
        LOAD_A_1x4(k+2, m);
        KERNEL_MMA_2ACC(rb2, rb6, ra0, ra0);
        LOAD_A_1x4(k+3, m);
        KERNEL_MMA_2ACC(rb3, rb7, ra0, ra0);
      }
      for (; k < k2; k += 2) {
        LOAD_A_1x4(k, m);
        LOAD_BT_8x2(n, k);
        KERNEL_MMA_2ACC(rb0, rb2, ra0, ra0);
        LOAD_A_1x4(k+1, m);
        KERNEL_MMA_2ACC(rb1, rb3, ra0, ra0);
      }
      for (; k < K; k++) {
        LOAD_A_1x4(k, m);
        LOAD_BT_8x1(n, k);
        KERNEL_MMA_2ACC(rb0, rb1, ra0, ra0);
      }

#if !defined(B0)
      register vector float rc0;
#endif
      vector float result[4];
      SAVE_4x4_ACC(&acc0, n+0, m+0);
      SAVE_4x4_ACC(&acc1, n+4, m+0);
    }

    for (; m < m2; m += 2) {
      __vector_quad acc0, acc1;

      INIT_2ACCS();

      register vector float ra0;
      register vector float rb0, rb1, rb2, rb3, rb4, rb5, rb6, rb7;
      register vector float t0, t1, t2, t3;

      for (k = 0; k < k4; k += 4) {
        LOAD_A_1x2(k, m);
        LOAD_BT_8x4(n, k);
        KERNEL_MMA_2ACC(rb0, rb4, ra0, ra0);
        LOAD_A_1x2(k+1, m);
        KERNEL_MMA_2ACC(rb1, rb5, ra0, ra0);
        LOAD_A_1x2(k+2, m);
        KERNEL_MMA_2ACC(rb2, rb6, ra0, ra0);
        LOAD_A_1x2(k+3, m);
        KERNEL_MMA_2ACC(rb3, rb7, ra0, ra0);
      }
      for (; k < k2; k += 2) {
        LOAD_A_1x2(k, m);
        LOAD_BT_8x2(n, k);
        KERNEL_MMA_2ACC(rb0, rb2, ra0, ra0);
        LOAD_A_1x2(k+1, m);
        KERNEL_MMA_2ACC(rb1, rb3, ra0, ra0);
      }
      for (; k < K; k++) {
        LOAD_A_1x2(k, m);
        LOAD_BT_8x1(n, k);
        KERNEL_MMA_2ACC(rb0, rb1, ra0, ra0);
      }

#if !defined(B0)
      register vector float rc0;
#endif
      vector float result[4];
      SAVE_4x2_ACC(&acc0, n+0, m+0);
      SAVE_4x2_ACC(&acc1, n+4, m+0);
    }

    for (; m < M; m++) {
      register vector float ra0;
      register vector float rb0, rb1, rb2, rb3, rb4, rb5, rb6, rb7;
      register vector float t0, t1, t2, t3;

      vector float result = ((vector float){0.,0.,0.,0.});
      vector float result1 = ((vector float){0.,0.,0.,0.});

      for (k = 0; k < k4; k += 4) {
        LOAD_A_1x1(k, m);
        LOAD_BT_8x4(n, k);
        KERNEL_VMADD_2VSR(ra0, ra0, rb0, rb4);
        LOAD_A_1x1(k+1, m);
        KERNEL_VMADD_2VSR(ra0, ra0, rb1, rb5);
        LOAD_A_1x1(k+2, m);
        KERNEL_VMADD_2VSR(ra0, ra0, rb2, rb6);
        LOAD_A_1x1(k+3, m);
        KERNEL_VMADD_2VSR(ra0, ra0, rb3, rb7);
      }
      for (; k < k2; k += 2) {
        LOAD_A_1x1(k, m);
        LOAD_BT_8x2(n, k);
        KERNEL_VMADD_2VSR(ra0, ra0, rb0, rb2);
        LOAD_A_1x1(k+1, m);
        KERNEL_VMADD_2VSR(ra0, ra0, rb1, rb3);
      }
      for (; k < K; k++) {
        LOAD_A_1x1(k, m);
        LOAD_BT_8x1(n, k);
        KERNEL_VMADD_2VSR(ra0, ra0, rb0, rb1);
      }

      SAVE_4x1_VSR(result, n+0, m);
      SAVE_4x1_VSR(result1, n+4, m);
    }
  }

  for (; n < n4; n += 4) {
    for (m = 0; m < m16; m += 16) {
      __vector_quad acc0, acc1, acc2, acc3;

      INIT_4ACCS();

      register vector float ra0, ra1, ra2, ra3;
      register vector float rb0, rb1, rb2, rb3;
      register vector float t0, t1, t2, t3;

      for (k = 0; k < k4; k += 4) {
        LOAD_A_1x16(k, m);
        LOAD_BT_4x4(n, k);
        KERNEL_MMA_4ACC(rb0, rb0, rb0, rb0, ra0, ra1, ra2, ra3);
        LOAD_A_1x16(k+1, m);
        KERNEL_MMA_4ACC(rb1, rb1, rb1, rb1, ra0, ra1, ra2, ra3);
        LOAD_A_1x16(k+2, m);
        KERNEL_MMA_4ACC(rb2, rb2, rb2, rb2, ra0, ra1, ra2, ra3);
        LOAD_A_1x16(k+3, m);
        KERNEL_MMA_4ACC(rb3, rb3, rb3, rb3, ra0, ra1, ra2, ra3);
      }
      for (; k < k2; k += 2) {
        LOAD_A_1x16(k, m);
        LOAD_BT_4x2(n, k);
        KERNEL_MMA_4ACC(rb0, rb0, rb0, rb0, ra0, ra1, ra2, ra3);
        LOAD_A_1x16(k+1, m);
        KERNEL_MMA_4ACC(rb1, rb1, rb1, rb1, ra0, ra1, ra2, ra3);
      }
      for (; k < K; k++) {
        LOAD_A_1x16(k, m);
        LOAD_BT_4x1(n, k);
        KERNEL_MMA_4ACC(rb0, rb0, rb0, rb0, ra0, ra1, ra2, ra3);
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

    for (; m < m8; m += 8) {
      __vector_quad acc0, acc1;

      INIT_2ACCS();

      register vector float ra0, ra1;
      register vector float rb0, rb1, rb2, rb3;
      register vector float t0, t1, t2, t3;

      for (k = 0; k < k4; k += 4) {
        LOAD_A_1x8(k, m);
        LOAD_BT_4x4(n, k);
        KERNEL_MMA_2ACC(rb0, rb0, ra0, ra1);
        LOAD_A_1x8(k+1, m);
        KERNEL_MMA_2ACC(rb1, rb1, ra0, ra1);
        LOAD_A_1x8(k+2, m);
        KERNEL_MMA_2ACC(rb2, rb2, ra0, ra1);
        LOAD_A_1x8(k+3, m);
        KERNEL_MMA_2ACC(rb3, rb3, ra0, ra1);
      }
      for (; k < k2; k += 2) {
        LOAD_A_1x8(k, m);
        LOAD_BT_4x2(n, k);
        KERNEL_MMA_2ACC(rb0, rb0, ra0, ra1);
        LOAD_A_1x8(k+1, m);
        KERNEL_MMA_2ACC(rb1, rb1, ra0, ra1);
      }
      for (; k < K; k++) {
        LOAD_A_1x8(k, m);
        LOAD_BT_4x1(n, k);
        KERNEL_MMA_2ACC(rb0, rb0, ra0, ra1);
      }

#if !defined(B0)
      register vector float rc0;
#endif
      vector float result[4];
      SAVE_4x4_ACC(&acc0, n+0, m+0);
      SAVE_4x4_ACC(&acc1, n+0, m+4);
    }

    for (; m < m4; m += 4) {
      __vector_quad acc0;

      INIT_1ACC();

      register vector float ra0;
      register vector float rb0, rb1, rb2, rb3;
      register vector float t0, t1, t2, t3;

      for (k = 0; k < k4; k += 4) {
        LOAD_A_1x4(k, m);
        LOAD_BT_4x4(n, k);
        KERNEL_MMA_1ACC(rb0, ra0);
        LOAD_A_1x4(k+1, m);
        KERNEL_MMA_1ACC(rb1, ra0);
        LOAD_A_1x4(k+2, m);
        KERNEL_MMA_1ACC(rb2, ra0);
        LOAD_A_1x4(k+3, m);
        KERNEL_MMA_1ACC(rb3, ra0);
      }
      for (; k < k2; k += 2) {
        LOAD_A_1x4(k, m);
        LOAD_BT_4x2(n, k);
        KERNEL_MMA_1ACC(rb0, ra0);
        LOAD_A_1x4(k+1, m);
        KERNEL_MMA_1ACC(rb1, ra0);
      }
      for (; k < K; k++) {
        LOAD_A_1x4(k, m);
        LOAD_BT_4x1(n, k);
        KERNEL_MMA_1ACC(rb0, ra0);
      }

#if !defined(B0)
      register vector float rc0;
#endif
      vector float result[4];
      SAVE_4x4_ACC(&acc0, n, m);
    }

    for (; m < m2; m += 2) {
      __vector_quad acc0;

      INIT_1ACC();

      register vector float ra0;
      register vector float rb0, rb1, rb2, rb3;
      register vector float t0, t1, t2, t3;

      for (k = 0; k < k4; k += 4) {
        LOAD_A_1x2(k, m);
        LOAD_BT_4x4(n, k);
        KERNEL_MMA_1ACC(rb0, ra0);
        LOAD_A_1x2(k+1, m);
        KERNEL_MMA_1ACC(rb1, ra0);
        LOAD_A_1x2(k+2, m);
        KERNEL_MMA_1ACC(rb2, ra0);
        LOAD_A_1x2(k+3, m);
        KERNEL_MMA_1ACC(rb3, ra0);
      }
      for (; k < k2; k += 2) {
        LOAD_A_1x2(k, m);
        LOAD_BT_4x2(n, k);
        KERNEL_MMA_1ACC(rb0, ra0);
        LOAD_A_1x2(k+1, m);
        KERNEL_MMA_1ACC(rb1, ra0);
      }
      for (; k < K; k++) {
        LOAD_A_1x2(k, m);
        LOAD_BT_4x1(n, k);
        KERNEL_MMA_1ACC(rb0, ra0);
      }

#if !defined(B0)
      register vector float rc0;
#endif
      vector float result[4];
      SAVE_4x2_ACC(&acc0, n, m);
    }

    for (; m < M; m++) {
      register vector float ra0;
      register vector float rb0, rb1, rb2, rb3;
      register vector float t0, t1, t2, t3;

      vector float result = ((vector float){0.,0.,0.,0.});

      for (k = 0; k < k4; k += 4) {
        LOAD_A_1x1(k, m);
        LOAD_BT_4x4(n, k);
        KERNEL_VMADD_1VSR(ra0, rb0);
        LOAD_A_1x1(k+1, m);
        KERNEL_VMADD_1VSR(ra0, rb1);
        LOAD_A_1x1(k+2, m);
        KERNEL_VMADD_1VSR(ra0, rb2);
        LOAD_A_1x1(k+3, m);
        KERNEL_VMADD_1VSR(ra0, rb3);
      }
      for (; k < k2; k += 2) {
        LOAD_A_1x1(k, m);
        LOAD_BT_4x2(n, k);
        KERNEL_VMADD_1VSR(ra0, rb0);
        LOAD_A_1x1(k+1, m);
        KERNEL_VMADD_1VSR(ra0, rb1);
      }
      for (; k < K; k++) {
        LOAD_A_1x1(k, m);
        LOAD_BT_4x1(n, k);
        KERNEL_VMADD_1VSR(ra0, rb0);
      }

      SAVE_4x1_VSR(result, n+0, m);
    }
  }

  for (; n < n2; n += 2) {
    for (m = 0; m < m16; m += 16) {
      __vector_quad acc0, acc1, acc2, acc3;

      INIT_4ACCS();

      register vector float ra0, ra1, ra2, ra3;
      register vector float rb0, rb1, rb2, rb3;
      register vector float t0, t1, t2;

      for (k = 0; k < k4; k += 4) {
        LOAD_A_1x16(k, m);
        LOAD_BT_2x4(n, k);
        KERNEL_MMA_4ACC(rb0, rb0, rb0, rb0, ra0, ra1, ra2, ra3);
        LOAD_A_1x16(k+1, m);
        KERNEL_MMA_4ACC(rb1, rb1, rb1, rb1, ra0, ra1, ra2, ra3);
        LOAD_A_1x16(k+2, m);
        KERNEL_MMA_4ACC(rb2, rb2, rb2, rb2, ra0, ra1, ra2, ra3);
        LOAD_A_1x16(k+3, m);
        KERNEL_MMA_4ACC(rb3, rb3, rb3, rb3, ra0, ra1, ra2, ra3);
      }
      for (; k < k2; k += 2) {
        LOAD_A_1x16(k, m);
        LOAD_BT_2x2(n, k);
        KERNEL_MMA_4ACC(rb0, rb0, rb0, rb0, ra0, ra1, ra2, ra3);
        LOAD_A_1x16(k+1, m);
        KERNEL_MMA_4ACC(rb1, rb1, rb1, rb1, ra0, ra1, ra2, ra3);
      }
      for (; k < K; k++) {
        LOAD_A_1x16(k, m);
        LOAD_BT_2x1(n, k);
        KERNEL_MMA_4ACC(rb0, rb0, rb0, rb0, ra0, ra1, ra2, ra3);
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

    for (; m < m8; m += 8) {
      __vector_quad acc0, acc1;

      INIT_2ACCS();

      register vector float ra0, ra1;
      register vector float rb0, rb1, rb2, rb3;
      register vector float t0, t1, t2;

      for (k = 0; k < k4; k += 4) {
        LOAD_A_1x8(k, m);
        LOAD_BT_2x4(n, k);
        KERNEL_MMA_2ACC(rb0, rb0, ra0, ra1);
        LOAD_A_1x8(k+1, m);
        KERNEL_MMA_2ACC(rb1, rb1, ra0, ra1);
        LOAD_A_1x8(k+2, m);
        KERNEL_MMA_2ACC(rb2, rb2, ra0, ra1);
        LOAD_A_1x8(k+3, m);
        KERNEL_MMA_2ACC(rb3, rb3, ra0, ra1);
      }
      for (; k < k2; k += 2) {
        LOAD_A_1x8(k, m);
        LOAD_BT_2x2(n, k);
        KERNEL_MMA_2ACC(rb0, rb0, ra0, ra1);
        LOAD_A_1x8(k+1, m);
        KERNEL_MMA_2ACC(rb1, rb1, ra0, ra1);
      }
      for (; k < K; k++) {
        LOAD_A_1x8(k, m);
        LOAD_BT_2x1(n, k);
        KERNEL_MMA_2ACC(rb0, rb0, ra0, ra1);
      }

#if !defined(B0)
      register vector float rc0;
#endif
      vector float result[4];
      SAVE_2x4_ACC(&acc0, n, m+0);
      SAVE_2x4_ACC(&acc1, n, m+4);
    }

    for (; m < m4; m += 4) {
      __vector_quad acc0;

      INIT_1ACC();

      register vector float ra0;
      register vector float rb0, rb1, rb2, rb3;
      register vector float t0, t1, t2;

      for (k = 0; k < k4; k += 4) {
        LOAD_A_1x4(k, m);
        LOAD_BT_2x4(n, k);
        KERNEL_MMA_1ACC(rb0, ra0);
        LOAD_A_1x4(k+1, m);
        KERNEL_MMA_1ACC(rb1, ra0);
        LOAD_A_1x4(k+2, m);
        KERNEL_MMA_1ACC(rb2, ra0);
        LOAD_A_1x4(k+3, m);
        KERNEL_MMA_1ACC(rb3, ra0);
      }
      for (; k < k2; k += 2) {
        LOAD_A_1x4(k, m);
        LOAD_BT_2x2(n, k);
        KERNEL_MMA_1ACC(rb0, ra0);
        LOAD_A_1x4(k+1, m);
        KERNEL_MMA_1ACC(rb1, ra0);
      }
      for (; k < K; k++) {
        LOAD_A_1x4(k, m);
        LOAD_BT_2x1(n, k);
        KERNEL_MMA_1ACC(rb0, ra0);
      }

#if !defined(B0)
      register vector float rc0;
#endif
      vector float result[4];
      SAVE_2x4_ACC(&acc0, n, m);
    }

    for (; m < m2; m += 2) {
      vector float result = ((vector float){0.,0.,0.,0.});

      register vector float ra0;
      register vector float rb0;

      for (k = 0; k < K; k++) {
        LOAD_A_2x2(k, m);
        LOAD_B_2x2(n, k);
        KERNEL_VMADD_1VSR(ra0, rb0);
      }

#if !defined(B0)
      register vector float rc0;
#endif
      SAVE_2x2_VSR(result, n, m);
    }

    for (; m < M; m++) {
      vector float result = ((vector float){0.,0.,0.,0.});

      register vector float ra0;
      register vector float rb0 = ((vector float){0.,0.,0.,0.});

      for (k = 0; k < K; k++) {
        LOAD_A_1x1(k, m);
        LOAD_B_2x1(n, k);
        KERNEL_VMADD_1VSR(ra0, rb0);
      }

      SAVE_2x1_VSR(result, n, m);
    }
  }

  for (; n < N; n++) {
    for (m = 0; m < m16; m += 16) {
      vector float result = ((vector float){0.,0.,0.,0.});
      vector float result1 = ((vector float){0.,0.,0.,0.});
      vector float result2 = ((vector float){0.,0.,0.,0.});
      vector float result3 = ((vector float){0.,0.,0.,0.});

      register vector float ra0, ra1, ra2, ra3;
      register vector float rb0;

      for (k = 0; k < K; k++) {
        LOAD_A_1x16(k, m);
        LOAD_B_1x1(n, k);
        KERNEL_VMADD_4VSR(ra0, ra1, ra2, ra3, rb0, rb0, rb0, rb0);
      }

#if !defined(B0)
      register vector float rc0;
#endif
      SAVE_1x4_VSR(result, n, m);
      SAVE_1x4_VSR(result1, n, m+4);
      SAVE_1x4_VSR(result2, n, m+8);
      SAVE_1x4_VSR(result3, n, m+12);
    }

    for (; m < m8; m += 8) {
      vector float result = ((vector float){0.,0.,0.,0.});
      vector float result1 = ((vector float){0.,0.,0.,0.});

      register vector float ra0, ra1;
      register vector float rb0;

      for (k = 0; k < K; k++) {
        LOAD_A_1x8(k, m);
        LOAD_B_1x1(n, k);
        KERNEL_VMADD_2VSR(ra0, ra1, rb0, rb0);
      }

#if !defined(B0)
      register vector float rc0;
#endif
      SAVE_1x4_VSR(result, n, m);
      SAVE_1x4_VSR(result1, n, m+4);
    }

    for (; m < m4; m += 4) {
      vector float result = ((vector float){0.,0.,0.,0.});

      register vector float ra0;
      register vector float rb0;

      for (k = 0; k < K; k++) {
        LOAD_A_1x4(k, m);
        LOAD_B_1x1(n, k);
        KERNEL_VMADD_1VSR(ra0, rb0);
      }

#if !defined(B0)
      register vector float rc0;
#endif
      SAVE_1x4_VSR(result, n, m);
    }

    for (; m < m2; m += 2) {
      vector float result = ((vector float){0.,0.,0.,0.});

      register vector float ra0;
      register vector float rb0;

      for (k = 0; k < K; k++) {
        LOAD_A_1x2(k, m);
        LOAD_B_1x1(n, k);
        KERNEL_VMADD_1VSR(ra0, rb0);
      }

#if !defined(B0)
      register vector float rc0;
#endif
      SAVE_1x2_VSR(result, n, m);
    }

    for (; m < M; m++) {
      FLOAT result = 0.0f;

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

  if (has_packing) free (packB);

  return 0;
}
