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

#define INIT_1ACC() __builtin_mma_xxsetaccz(&acc0);

#define LOAD_AT_16x4(M, K)           \
  ra0 = vec_xl(0, A+(M+0)*lda+K);    \
  ra1 = vec_xl(0, A+(M+1)*lda+K);    \
  t0 = vec_mergeh(ra0, ra1);         \
  t1 = vec_mergel(ra0, ra1);         \
  ra2 = vec_xl(0, A+(M+2)*lda+K);    \
  ra3 = vec_xl(0, A+(M+3)*lda+K);    \
  t2 = vec_mergeh(ra2, ra3);         \
  t3 = vec_mergel(ra2, ra3);         \
  ra0 = vec_xxpermdi(t0, t2, 0b00);  \
  ra1 = vec_xxpermdi(t0, t2, 0b11);  \
  ra2 = vec_xxpermdi(t1, t3, 0b00);  \
  ra3 = vec_xxpermdi(t1, t3, 0b11);  \
  ra4 = vec_xl(0, A+(M+4)*lda+K);    \
  ra5 = vec_xl(0, A+(M+5)*lda+K);    \
  t0 = vec_mergeh(ra4, ra5);         \
  t1 = vec_mergel(ra4, ra5);         \
  ra6 = vec_xl(0, A+(M+6)*lda+K);    \
  ra7 = vec_xl(0, A+(M+7)*lda+K);    \
  t2 = vec_mergeh(ra6, ra7);         \
  t3 = vec_mergel(ra6, ra7);         \
  ra4 = vec_xxpermdi(t0, t2, 0b00);  \
  ra5 = vec_xxpermdi(t0, t2, 0b11);  \
  ra6 = vec_xxpermdi(t1, t3, 0b00);  \
  ra7 = vec_xxpermdi(t1, t3, 0b11);  \
  ra8 = vec_xl(0, A+(M+8)*lda+K);    \
  ra9 = vec_xl(0, A+(M+9)*lda+K);    \
  t0 = vec_mergeh(ra8, ra9);         \
  t1 = vec_mergel(ra8, ra9);         \
  ra10 = vec_xl(0, A+(M+10)*lda+K);  \
  ra11 = vec_xl(0, A+(M+11)*lda+K);  \
  t2 = vec_mergeh(ra10, ra11);       \
  t3 = vec_mergel(ra10, ra11);       \
  ra8 = vec_xxpermdi(t0, t2, 0b00);  \
  ra9 = vec_xxpermdi(t0, t2, 0b11);  \
  ra10 = vec_xxpermdi(t1, t3, 0b00); \
  ra11 = vec_xxpermdi(t1, t3, 0b11); \
  ra12 = vec_xl(0, A+(M+12)*lda+K);  \
  ra13 = vec_xl(0, A+(M+13)*lda+K);  \
  t0 = vec_mergeh(ra12, ra13);       \
  t1 = vec_mergel(ra12, ra13);       \
  ra14 = vec_xl(0, A+(M+14)*lda+K);  \
  ra15 = vec_xl(0, A+(M+15)*lda+K);  \
  t2 = vec_mergeh(ra14, ra15);       \
  t3 = vec_mergel(ra14, ra15);       \
  ra12 = vec_xxpermdi(t0, t2, 0b00); \
  ra13 = vec_xxpermdi(t0, t2, 0b11); \
  ra14 = vec_xxpermdi(t1, t3, 0b00); \
  ra15 = vec_xxpermdi(t1, t3, 0b11);

#define LOAD_AT_16x2(M, K)              \
  ra0 = vec_xl_len(A+(M+0)*lda+K, 8);   \
  ra1 = vec_xl_len(A+(M+1)*lda+K, 8);   \
  t0 = vec_mergeh(ra0, ra1);            \
  ra2 = vec_xl_len(A+(M+2)*lda+K, 8);   \
  ra3 = vec_xl_len(A+(M+3)*lda+K, 8);   \
  t1 = vec_mergeh(ra2, ra3);            \
  ra0 = vec_xxpermdi(t0, t1, 0b00);     \
  ra1 = vec_xxpermdi(t0, t1, 0b11);     \
  ra4 = vec_xl_len(A+(M+4)*lda+K, 8);   \
  ra5 = vec_xl_len(A+(M+5)*lda+K, 8);   \
  t0 = vec_mergeh(ra4, ra5);            \
  ra6 = vec_xl_len(A+(M+6)*lda+K, 8);   \
  ra7 = vec_xl_len(A+(M+7)*lda+K, 8);   \
  t1 = vec_mergeh(ra6, ra7);            \
  ra2 = vec_xxpermdi(t0, t1, 0b00);     \
  ra3 = vec_xxpermdi(t0, t1, 0b11);     \
  ra8 = vec_xl_len(A+(M+8)*lda+K, 8);   \
  ra9 = vec_xl_len(A+(M+9)*lda+K, 8);   \
  t0 = vec_mergeh(ra8, ra9);            \
  ra10 = vec_xl_len(A+(M+10)*lda+K, 8); \
  ra11 = vec_xl_len(A+(M+11)*lda+K, 8); \
  t1 = vec_mergeh(ra10, ra11);          \
  ra4 = vec_xxpermdi(t0, t1, 0b00);     \
  ra5 = vec_xxpermdi(t0, t1, 0b11);     \
  ra12 = vec_xl_len(A+(M+12)*lda+K, 8); \
  ra13 = vec_xl_len(A+(M+13)*lda+K, 8); \
  t0 = vec_mergeh(ra12, ra13);          \
  ra14 = vec_xl_len(A+(M+14)*lda+K, 8); \
  ra15 = vec_xl_len(A+(M+15)*lda+K, 8); \
  t1 = vec_mergeh(ra14, ra15);          \
  ra6 = vec_xxpermdi(t0, t1, 0b00);     \
  ra7 = vec_xxpermdi(t0, t1, 0b11);

#define LOAD_AT_16x1(M, K)                   \
  ra0 = vec_xor(ra0, ra0);                   \
  ra0 = vec_insert(A[(M+0)*lda+K], ra0, 0);  \
  ra0 = vec_insert(A[(M+1)*lda+K], ra0, 1);  \
  ra0 = vec_insert(A[(M+2)*lda+K], ra0, 2);  \
  ra0 = vec_insert(A[(M+3)*lda+K], ra0, 3);  \
  ra1 = vec_xor(ra1, ra1);                   \
  ra1 = vec_insert(A[(M+4)*lda+K], ra1, 0);  \
  ra1 = vec_insert(A[(M+5)*lda+K], ra1, 1);  \
  ra1 = vec_insert(A[(M+6)*lda+K], ra1, 2);  \
  ra1 = vec_insert(A[(M+7)*lda+K], ra1, 3);  \
  ra2 = vec_xor(ra2, ra2);                   \
  ra2 = vec_insert(A[(M+8)*lda+K], ra2, 0);  \
  ra2 = vec_insert(A[(M+9)*lda+K], ra2, 1);  \
  ra2 = vec_insert(A[(M+10)*lda+K], ra2, 2); \
  ra2 = vec_insert(A[(M+11)*lda+K], ra2, 3); \
  ra3 = vec_xor(ra3, ra3);                   \
  ra3 = vec_insert(A[(M+12)*lda+K], ra3, 0); \
  ra3 = vec_insert(A[(M+13)*lda+K], ra3, 1); \
  ra3 = vec_insert(A[(M+14)*lda+K], ra3, 2); \
  ra3 = vec_insert(A[(M+15)*lda+K], ra3, 3);

#define LOAD_AT_8x4(M, K)           \
  ra0 = vec_xl(0, A+(M+0)*lda+K);   \
  ra1 = vec_xl(0, A+(M+1)*lda+K);   \
  t0 = vec_mergeh(ra0, ra1);        \
  t1 = vec_mergel(ra0, ra1);        \
  ra2 = vec_xl(0, A+(M+2)*lda+K);   \
  ra3 = vec_xl(0, A+(M+3)*lda+K);   \
  t2 = vec_mergeh(ra2, ra3);        \
  t3 = vec_mergel(ra2, ra3);        \
  ra0 = vec_xxpermdi(t0, t2, 0b00); \
  ra1 = vec_xxpermdi(t0, t2, 0b11); \
  ra2 = vec_xxpermdi(t1, t3, 0b00); \
  ra3 = vec_xxpermdi(t1, t3, 0b11); \
  ra4 = vec_xl(0, A+(M+4)*lda+K);   \
  ra5 = vec_xl(0, A+(M+5)*lda+K);   \
  t0 = vec_mergeh(ra4, ra5);        \
  t1 = vec_mergel(ra4, ra5);        \
  ra6 = vec_xl(0, A+(M+6)*lda+K);   \
  ra7 = vec_xl(0, A+(M+7)*lda+K);   \
  t2 = vec_mergeh(ra6, ra7);        \
  t3 = vec_mergel(ra6, ra7);        \
  ra4 = vec_xxpermdi(t0, t2, 0b00); \
  ra5 = vec_xxpermdi(t0, t2, 0b11); \
  ra6 = vec_xxpermdi(t1, t3, 0b00); \
  ra7 = vec_xxpermdi(t1, t3, 0b11);

#define LOAD_AT_8x2(M, K)             \
  ra0 = vec_xl_len(A+(M+0)*lda+K, 8); \
  ra1 = vec_xl_len(A+(M+1)*lda+K, 8); \
  t0 = vec_mergeh(ra0, ra1);          \
  ra2 = vec_xl_len(A+(M+2)*lda+K, 8); \
  ra3 = vec_xl_len(A+(M+3)*lda+K, 8); \
  t1 = vec_mergeh(ra2, ra3);          \
  ra0 = vec_xxpermdi(t0, t1, 0b00);   \
  ra1 = vec_xxpermdi(t0, t1, 0b11);   \
  ra4 = vec_xl_len(A+(M+4)*lda+K, 8); \
  ra5 = vec_xl_len(A+(M+5)*lda+K, 8); \
  t0 = vec_mergeh(ra4, ra5);          \
  ra6 = vec_xl_len(A+(M+6)*lda+K, 8); \
  ra7 = vec_xl_len(A+(M+7)*lda+K, 8); \
  t1 = vec_mergeh(ra6, ra7);          \
  ra2 = vec_xxpermdi(t0, t1, 0b00);   \
  ra3 = vec_xxpermdi(t0, t1, 0b11);

#define LOAD_AT_8x1(M, K)                   \
  ra0 = vec_xor(ra0, ra0);                  \
  ra0 = vec_insert(A[(M+0)*lda+K], ra0, 0); \
  ra0 = vec_insert(A[(M+1)*lda+K], ra0, 1); \
  ra0 = vec_insert(A[(M+2)*lda+K], ra0, 2); \
  ra0 = vec_insert(A[(M+3)*lda+K], ra0, 3); \
  ra1 = vec_xor(ra1, ra1);                  \
  ra1 = vec_insert(A[(M+4)*lda+K], ra1, 0); \
  ra1 = vec_insert(A[(M+5)*lda+K], ra1, 1); \
  ra1 = vec_insert(A[(M+6)*lda+K], ra1, 2); \
  ra1 = vec_insert(A[(M+7)*lda+K], ra1, 3);

#define LOAD_AT_4x4(M, K)           \
  ra0 = vec_xl(0, A+(M+0)*lda+K);   \
  ra1 = vec_xl(0, A+(M+1)*lda+K);   \
  t0 = vec_mergeh(ra0, ra1);        \
  t1 = vec_mergel(ra0, ra1);        \
  ra2 = vec_xl(0, A+(M+2)*lda+K);   \
  ra3 = vec_xl(0, A+(M+3)*lda+K);   \
  t2 = vec_mergeh(ra2, ra3);        \
  t3 = vec_mergel(ra2, ra3);        \
  ra0 = vec_xxpermdi(t0, t2, 0b00); \
  ra1 = vec_xxpermdi(t0, t2, 0b11); \
  ra2 = vec_xxpermdi(t1, t3, 0b00); \
  ra3 = vec_xxpermdi(t1, t3, 0b11);

#define LOAD_AT_4x2(M, K)             \
  ra0 = vec_xl_len(A+(M+0)*lda+K, 8); \
  ra1 = vec_xl_len(A+(M+1)*lda+K, 8); \
  t0 = vec_mergeh(ra0, ra1);          \
  ra2 = vec_xl_len(A+(M+2)*lda+K, 8); \
  ra3 = vec_xl_len(A+(M+3)*lda+K, 8); \
  t1 = vec_mergeh(ra2, ra3);          \
  ra0 = vec_xxpermdi(t0, t1, 0b00);   \
  ra1 = vec_xxpermdi(t0, t1, 0b11);

#define LOAD_AT_4x1(M, K)                   \
  ra0 = vec_xor(ra0, ra0);                  \
  ra0 = vec_insert(A[(M+0)*lda+K], ra0, 0); \
  ra0 = vec_insert(A[(M+1)*lda+K], ra0, 1); \
  ra0 = vec_insert(A[(M+2)*lda+K], ra0, 2); \
  ra0 = vec_insert(A[(M+3)*lda+K], ra0, 3);

#define LOAD_AT_2x4(M, K)                       \
  ra0 = vec_xl(0, A+(M+0)*lda+K);               \
  ra1 = vec_xl(0, A+(M+1)*lda+K);               \
  t0 = vec_mergeh(ra0, ra1);                    \
  t1 = vec_mergeo(ra0, ra1);                    \
  t2 = vec_mergel(ra0, ra1);                    \
  ra0 = t0;                                     \
  ra1 = t1;                                     \
  ra2 = t2;                                     \
  ra3 = vec_xor(ra3, ra3);                      \
  ra3 = vec_insert(vec_extract(t2,2), ra3, 0);  \
  ra3 = vec_insert(vec_extract(t2,3), ra3, 1);

#define LOAD_AT_2x2(M, K)             \
  ra0 = vec_xl_len(A+(M+0)*lda+K, 8); \
  ra1 = vec_xl_len(A+(M+1)*lda+K, 8); \
  t0 = vec_mergee(ra0, ra1);          \
  t1 = vec_mergeo(ra0, ra1);          \
  ra0 = t0;                           \
  ra1 = t1;

#define LOAD_AT_2x1(M, K)                   \
  ra0 = vec_xor(ra0, ra0);                  \
  ra0 = vec_insert(A[(M+0)*lda+K], ra0, 0); \
  ra0 = vec_insert(A[(M+1)*lda+K], ra0, 1);

#define LOAD_A_2x2(M, K)                    \
  ra0 = vec_splats(A[M*lda+K]);             \
  ra0 = vec_insert(A[(M+1)*lda+K], ra0, 1); \
  ra0 = vec_insert(A[(M+1)*lda+K], ra0, 3);

#define LOAD_A_1x1(M, K) ra0 = vec_splats(A[M*lda+K]);

#define LOAD_B_1x16(K, N)            \
  rb0 = vec_xl(0, B+((K)*ldb)+N+0); \
  rb1 = vec_xl(0, B+((K)*ldb)+N+4); \
  rb2 = vec_xl(0, B+((K)*ldb)+N+8); \
  rb3 = vec_xl(0, B+((K)*ldb)+N+12);

#define LOAD_B_1x8(K, N)            \
  rb0 = vec_xl(0, B+((K)*ldb)+N+0); \
  rb1 = vec_xl(0, B+((K)*ldb)+N+4);

#define LOAD_B_1x4(K, N) rb0 = vec_xl(0, B+((K)*ldb)+N);

#define LOAD_B_1x2(K, N) rb0 = vec_xl_len(B+((K)*ldb)+N, 8);

#define LOAD_B_2x2(K, N)                  \
  rb0 = vec_splats(B[K*ldb+N]);         \
  rb0 = vec_insert(B[K*ldb+N+1], rb0, 2); \
  rb0 = vec_insert(B[K*ldb+N+1], rb0, 3);

#define LOAD_B_1x1(K, N) rb0 = vec_splats(B[(K)*ldb+N]);

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

  BLASLONG k4 = K & ~3;
  BLASLONG k2 = K & ~1;

  vector float valpha = vec_splats(alpha);
#if !defined(B0)
  vector float vbeta = vec_splats(beta);
#endif

#if defined(__GNUC__) && !defined(__clang__)
  int has_packing = (M >= 32 && N >= 32 && K >= 32) ? 1 : 0;
#else
  int has_packing = 0;
#endif

  float *packA;
  if (has_packing) packA = (float *)malloc(K*16*sizeof(float));

  for (m = 0; m < m16; m += 16) {
    for (n = 0; n < n8; n += 8) {
      __vector_quad acc0, acc1, acc2, acc3, acc4, acc5, acc6, acc7;

      INIT_8ACCS();

      register vector float ra0, ra1, ra2, ra3, ra4, ra5, ra6, ra7, ra8, ra9,
          ra10, ra11, ra12, ra13, ra14, ra15;
      register vector float rb0, rb1;
      register vector float t0, t1, t2, t3;

      if (has_packing) {
        if (n == 0) {
          for (k = 0; k < k4; k += 4) {
            LOAD_AT_16x4(m, k);
            LOAD_B_1x8(k, n);
            KERNEL_MMA_8ACC(rb0, rb0, rb0, rb0, rb1, rb1, rb1, rb1,
                            ra0, ra4, ra8, ra12, ra0, ra4, ra8, ra12);
            PACK_A(ra0, ra4, ra8, ra12, 0);
            LOAD_B_1x8(k+1, n);
            KERNEL_MMA_8ACC(rb0, rb0, rb0, rb0, rb1, rb1, rb1, rb1,
                            ra1, ra5, ra9, ra13, ra1, ra5, ra9, ra13);
            PACK_A(ra1, ra5, ra9, ra13, 16);
            LOAD_B_1x8(k+2, n);
            KERNEL_MMA_8ACC(rb0, rb0, rb0, rb0, rb1, rb1, rb1, rb1,
                            ra2, ra6, ra10, ra14, ra2, ra6, ra10, ra14);
            PACK_A(ra2, ra6, ra10, ra14, 32);
            LOAD_B_1x8(k+3, n);
            KERNEL_MMA_8ACC(rb0, rb0, rb0, rb0, rb1, rb1, rb1, rb1,
                            ra3, ra7, ra11, ra15, ra3, ra7, ra11, ra15);
            PACK_A(ra3, ra7, ra11, ra15, 48);
          }
          for (; k < k2; k += 2) {
            LOAD_AT_16x2(m, k);
            LOAD_B_1x8(k, n);
            KERNEL_MMA_8ACC(rb0, rb0, rb0, rb0, rb1, rb1, rb1, rb1,
                            ra0, ra2, ra4, ra6, ra0, ra2, ra4, ra6);
            PACK_A(ra0, ra2, ra4, ra6, 0);
            LOAD_B_1x8(k+1, n);
            KERNEL_MMA_8ACC(rb0, rb0, rb0, rb0, rb1, rb1, rb1, rb1,
                            ra1, ra3, ra5, ra7, ra1, ra3, ra5, ra7);
            PACK_A(ra1, ra3, ra5, ra7, 16);
          }
          for (; k < K; k++) {
            LOAD_AT_16x1(m, k);
            LOAD_B_1x8(k, n);
            KERNEL_MMA_8ACC(rb0, rb0, rb0, rb0, rb1, rb1, rb1, rb1,
                            ra0, ra1, ra2, ra3, ra0, ra1, ra2, ra3);
            PACK_A(ra0, ra1, ra2, ra3, 0);
          }
        } else {
          for (k = 0; k < k4; k += 4) {
            LOAD_PACKED_A(ra0, ra4, ra8, ra12, 0);
            LOAD_B_1x8(k, n);
            KERNEL_MMA_8ACC(rb0, rb0, rb0, rb0, rb1, rb1, rb1, rb1,
                            ra0, ra4, ra8, ra12, ra0, ra4, ra8, ra12);
            LOAD_PACKED_A(ra1, ra5, ra9, ra13, 16);
            LOAD_B_1x8(k+1, n);
            KERNEL_MMA_8ACC(rb0, rb0, rb0, rb0, rb1, rb1, rb1, rb1,
                            ra1, ra5, ra9, ra13, ra1, ra5, ra9, ra13);
            LOAD_PACKED_A(ra2, ra6, ra10, ra14, 32);
            LOAD_B_1x8(k+2, n);
            KERNEL_MMA_8ACC(rb0, rb0, rb0, rb0, rb1, rb1, rb1, rb1,
                            ra2, ra6, ra10, ra14, ra2, ra6, ra10, ra14);
            LOAD_PACKED_A(ra3, ra7, ra11, ra15, 48);
            LOAD_B_1x8(k+3, n);
            KERNEL_MMA_8ACC(rb0, rb0, rb0, rb0, rb1, rb1, rb1, rb1,
                            ra3, ra7, ra11, ra15, ra3, ra7, ra11, ra15);
          }
          for (; k < k2; k += 2) {
            LOAD_PACKED_A(ra0, ra2, ra4, ra6, 0);
            LOAD_B_1x8(k, n);
            KERNEL_MMA_8ACC(rb0, rb0, rb0, rb0, rb1, rb1, rb1, rb1,
                            ra0, ra2, ra4, ra6, ra0, ra2, ra4, ra6);
            LOAD_PACKED_A(ra1, ra3, ra5, ra7, 16);
            LOAD_B_1x8(k+1, n);
            KERNEL_MMA_8ACC(rb0, rb0, rb0, rb0, rb1, rb1, rb1, rb1,
                            ra1, ra3, ra5, ra7, ra1, ra3, ra5, ra7);
          }
          for (; k < K; k++) {
            LOAD_PACKED_A(ra0, ra1, ra2, ra3, 0);
            LOAD_B_1x8(k, n);
            KERNEL_MMA_8ACC(rb0, rb0, rb0, rb0, rb1, rb1, rb1, rb1,
                            ra0, ra1, ra2, ra3, ra0, ra1, ra2, ra3);
          }
        }
      } else {
        for (k = 0; k < k4; k += 4) {
          LOAD_AT_16x4(m, k);
          LOAD_B_1x8(k, n);
          KERNEL_MMA_8ACC(rb0, rb0, rb0, rb0, rb1, rb1, rb1, rb1,
                          ra0, ra4, ra8, ra12, ra0, ra4, ra8, ra12);
          LOAD_B_1x8(k+1, n);
          KERNEL_MMA_8ACC(rb0, rb0, rb0, rb0, rb1, rb1, rb1, rb1,
                          ra1, ra5, ra9, ra13, ra1, ra5, ra9, ra13);
          LOAD_B_1x8(k+2, n);
          KERNEL_MMA_8ACC(rb0, rb0, rb0, rb0, rb1, rb1, rb1, rb1,
                          ra2, ra6, ra10, ra14, ra2, ra6, ra10, ra14);
          LOAD_B_1x8(k+3, n);
          KERNEL_MMA_8ACC(rb0, rb0, rb0, rb0, rb1, rb1, rb1, rb1,
                          ra3, ra7, ra11, ra15, ra3, ra7, ra11, ra15);
        }
        for (; k < k2; k += 2) {
          LOAD_AT_16x2(m, k);
          LOAD_B_1x8(k, n);
          KERNEL_MMA_8ACC(rb0, rb0, rb0, rb0, rb1, rb1, rb1, rb1,
                          ra0, ra2, ra4, ra6, ra0, ra2, ra4, ra6);
          LOAD_B_1x8(k+1, n);
          KERNEL_MMA_8ACC(rb0, rb0, rb0, rb0, rb1, rb1, rb1, rb1,
                          ra1, ra3, ra5, ra7, ra1, ra3, ra5, ra7);
        }
        for (; k < K; k++) {
          LOAD_AT_16x1(m, k);
          LOAD_B_1x8(k, n);
          KERNEL_MMA_8ACC(rb0, rb0, rb0, rb0, rb1, rb1, rb1, rb1,
                          ra0, ra1, ra2, ra3, ra0, ra1, ra2, ra3);
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
      SAVE_4x4_ACC(&acc4, n+4, m+0);
      SAVE_4x4_ACC(&acc5, n+4, m+4);
      SAVE_4x4_ACC(&acc6, n+4, m+8);
      SAVE_4x4_ACC(&acc7, n+4, m+12);
    }

    for (; n < n4; n += 4) {
      __vector_quad acc0, acc1, acc2, acc3;

      INIT_4ACCS();

      register vector float ra0, ra1, ra2, ra3, ra4, ra5, ra6, ra7, ra8, ra9,
          ra10, ra11, ra12, ra13, ra14, ra15;
      register vector float rb0;
      register vector float t0, t1, t2, t3;

      if (!has_packing) {
        for (k = 0; k < k4; k += 4) {
          LOAD_AT_16x4(m, k);
          LOAD_B_1x4(k, n);
          KERNEL_MMA_4ACC(rb0, rb0, rb0, rb0, ra0, ra4, ra8, ra12);
          LOAD_B_1x4(k+1, n);
          KERNEL_MMA_4ACC(rb0, rb0, rb0, rb0, ra1, ra5, ra9, ra13);
          LOAD_B_1x4(k+2, n);
          KERNEL_MMA_4ACC(rb0, rb0, rb0, rb0, ra2, ra6, ra10, ra14);
          LOAD_B_1x4(k+3, n);
          KERNEL_MMA_4ACC(rb0, rb0, rb0, rb0, ra3, ra7, ra11, ra15);
        }
        for (; k < k2; k += 2) {
          LOAD_AT_16x2(m, k);
          LOAD_B_1x4(k, n);
          KERNEL_MMA_4ACC(rb0, rb0, rb0, rb0, ra0, ra2, ra4, ra6);
          LOAD_B_1x4(k+1, n);
          KERNEL_MMA_4ACC(rb0, rb0, rb0, rb0, ra1, ra3, ra5, ra7);
        }
        for (; k < K; k++) {
          LOAD_AT_16x1(m, k);
          LOAD_B_1x4(k, n);
          KERNEL_MMA_4ACC(rb0, rb0, rb0, rb0, ra0, ra1, ra2, ra3);
        }
      } else {
        for (k = 0; k < k4; k += 4) {
          LOAD_PACKED_A(ra0, ra4, ra8, ra12, 0);
          LOAD_B_1x4(k, n);
          KERNEL_MMA_4ACC(rb0, rb0, rb0, rb0, ra0, ra4, ra8, ra12);
          LOAD_PACKED_A(ra1, ra5, ra9, ra13, 16);
          LOAD_B_1x4(k+1, n);
          KERNEL_MMA_4ACC(rb0, rb0, rb0, rb0, ra1, ra5, ra9, ra13);
          LOAD_PACKED_A(ra2, ra6, ra10, ra14, 32);
          LOAD_B_1x4(k+2, n);
          KERNEL_MMA_4ACC(rb0, rb0, rb0, rb0, ra2, ra6, ra10, ra14);
          LOAD_PACKED_A(ra3, ra7, ra11, ra15, 48);
          LOAD_B_1x4(k+3, n);
          KERNEL_MMA_4ACC(rb0, rb0, rb0, rb0, ra3, ra7, ra11, ra15);
        }
        for (; k < k2; k += 2) {
          LOAD_PACKED_A(ra0, ra2, ra4, ra6, 0);
          LOAD_B_1x4(k, n);
          KERNEL_MMA_4ACC(rb0, rb0, rb0, rb0, ra0, ra2, ra4, ra6);
          LOAD_PACKED_A(ra1, ra3, ra5, ra7, 16);
          LOAD_B_1x4(k+1, n);
          KERNEL_MMA_4ACC(rb0, rb0, rb0, rb0, ra1, ra3, ra5, ra7);
        }
        for (; k < K; k++) {
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

      register vector float ra0, ra1, ra2, ra3, ra4, ra5, ra6, ra7, ra8, ra9,
          ra10, ra11, ra12, ra13, ra14, ra15;
      register vector float rb0;
      register vector float t0, t1, t2, t3;

      if (!has_packing) {
        for (k = 0; k < k4; k += 4) {
          LOAD_AT_16x4(m, k);
          LOAD_B_1x2(k, n);
          KERNEL_MMA_4ACC(rb0, rb0, rb0, rb0, ra0, ra4, ra8, ra12);
          LOAD_B_1x2(k+1, n);
          KERNEL_MMA_4ACC(rb0, rb0, rb0, rb0, ra1, ra5, ra9, ra13);
          LOAD_B_1x2(k+2, n);
          KERNEL_MMA_4ACC(rb0, rb0, rb0, rb0, ra2, ra6, ra10, ra14);
          LOAD_B_1x2(k+3, n);
          KERNEL_MMA_4ACC(rb0, rb0, rb0, rb0, ra3, ra7, ra11, ra15);
        }
        for (; k < k2; k += 2) {
          LOAD_AT_16x2(m, k);
          LOAD_B_1x2(k, n);
          KERNEL_MMA_4ACC(rb0, rb0, rb0, rb0, ra0, ra2, ra4, ra6);
          LOAD_B_1x2(k+1, n);
          KERNEL_MMA_4ACC(rb0, rb0, rb0, rb0, ra1, ra3, ra5, ra7);
        }
        for (; k < K; k++) {
          LOAD_AT_16x1(m, k);
          LOAD_B_1x2(k, n);
          KERNEL_MMA_4ACC(rb0, rb0, rb0, rb0, ra0, ra1, ra2, ra3);
        }
      } else {
        for (k = 0; k < k4; k += 4) {
          LOAD_PACKED_A(ra0, ra4, ra8, ra12, 0);
          LOAD_B_1x2(k, n);
          KERNEL_MMA_4ACC(rb0, rb0, rb0, rb0, ra0, ra4, ra8, ra12);
          LOAD_PACKED_A(ra1, ra5, ra9, ra13, 16);
          LOAD_B_1x2(k+1, n);
          KERNEL_MMA_4ACC(rb0, rb0, rb0, rb0, ra1, ra5, ra9, ra13);
          LOAD_PACKED_A(ra2, ra6, ra10, ra14, 32);
          LOAD_B_1x2(k+2, n);
          KERNEL_MMA_4ACC(rb0, rb0, rb0, rb0, ra2, ra6, ra10, ra14);
          LOAD_PACKED_A(ra3, ra7, ra11, ra15, 48);
          LOAD_B_1x2(k+3, n);
          KERNEL_MMA_4ACC(rb0, rb0, rb0, rb0, ra3, ra7, ra11, ra15);
        }
        for (; k < k2; k += 2) {
          LOAD_B_1x2(k, n);
          LOAD_PACKED_A(ra0, ra2, ra4, ra6, 0);
          KERNEL_MMA_4ACC(rb0, rb0, rb0, rb0, ra0, ra2, ra4, ra6);
          LOAD_B_1x2(k+1, n);
          LOAD_PACKED_A(ra1, ra3, ra5, ra7, 16);
          KERNEL_MMA_4ACC(rb0, rb0, rb0, rb0, ra1, ra3, ra5, ra7);
        }
        for (; k < K; k++) {
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
      register vector float ra0, ra1, ra2, ra3, ra4, ra5, ra6, ra7, ra8, ra9,
          ra10, ra11, ra12, ra13, ra14, ra15;
      register vector float rb0;
      register vector float t0, t1, t2, t3;

      vector float result = ((vector float){0.,0.,0.,0.});
      vector float result1 = ((vector float){0.,0.,0.,0.});
      vector float result2 = ((vector float){0.,0.,0.,0.});
      vector float result3 = ((vector float){0.,0.,0.,0.});

      if (!has_packing) {
        for (k = 0; k < k4; k += 4) {
          LOAD_AT_16x4(m, k);
          LOAD_B_1x1(k, n);
          KERNEL_VMADD_4VSR(ra0, ra4, ra8, ra12, rb0, rb0, rb0, rb0);
          LOAD_B_1x1(k+1, n);
          KERNEL_VMADD_4VSR(ra1, ra5, ra9, ra13, rb0, rb0, rb0, rb0);
          LOAD_B_1x1(k+2, n);
          KERNEL_VMADD_4VSR(ra2, ra6, ra10, ra14, rb0, rb0, rb0, rb0);
          LOAD_B_1x1(k+3, n);
          KERNEL_VMADD_4VSR(ra3, ra7, ra11, ra15, rb0, rb0, rb0, rb0);
        }
        for (; k < k2; k += 2) {
          LOAD_AT_16x2(m, k);
          LOAD_B_1x1(k, n);
          KERNEL_VMADD_4VSR(ra0, ra2, ra4, ra6, rb0, rb0, rb0, rb0);
          LOAD_B_1x1(k+1, n);
          KERNEL_VMADD_4VSR(ra1, ra3, ra5, ra7, rb0, rb0, rb0, rb0);
        }
        for (; k < K; k++) {
          LOAD_AT_16x1(m, k);
          LOAD_B_1x1(k, n);
          KERNEL_VMADD_4VSR(ra0, ra1, ra2, ra3, rb0, rb0, rb0, rb0);
        }
      } else {
        for (k = 0; k < k4; k += 4) {
          LOAD_PACKED_A(ra0, ra4, ra8, ra12, 0);
          LOAD_B_1x1(k, n);
          KERNEL_VMADD_4VSR(ra0, ra4, ra8, ra12, rb0, rb0, rb0, rb0);
          LOAD_PACKED_A(ra1, ra5, ra9, ra13, 16);
          LOAD_B_1x1(k+1, n);
          KERNEL_VMADD_4VSR(ra1, ra5, ra9, ra13, rb0, rb0, rb0, rb0);
          LOAD_PACKED_A(ra2, ra6, ra10, ra14, 32);
          LOAD_B_1x1(k+2, n);
          KERNEL_VMADD_4VSR(ra2, ra6, ra10, ra14, rb0, rb0, rb0, rb0);
          LOAD_PACKED_A(ra3, ra7, ra11, ra15, 48);
          LOAD_B_1x1(k+3, n);
          KERNEL_VMADD_4VSR(ra3, ra7, ra11, ra15, rb0, rb0, rb0, rb0);
        }
        for (; k < k2; k += 2) {
          LOAD_PACKED_A(ra0, ra2, ra4, ra6, 0);
          LOAD_B_1x1(k, n);
          KERNEL_VMADD_4VSR(ra0, ra2, ra4, ra6, rb0, rb0, rb0, rb0);
          LOAD_PACKED_A(ra1, ra3, ra5, ra7, 16);
          LOAD_B_1x1(k+1, n);
          KERNEL_VMADD_4VSR(ra1, ra3, ra5, ra7, rb0, rb0, rb0, rb0);
        }
        for (; k < K; k++) {
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

      register vector float ra0, ra1, ra2, ra3, ra4, ra5, ra6, ra7;
      register vector float rb0, rb1, rb2, rb3;
      register vector float t0, t1, t2, t3;

      for (k = 0; k < k4; k += 4) {
        LOAD_AT_8x4(m, k);
        LOAD_B_1x16(k, n);
        KERNEL_MMA_8ACC(rb0, rb1, rb2, rb3, rb0, rb1, rb2, rb3,
                        ra0, ra0, ra0, ra0, ra4, ra4, ra4, ra4);
        LOAD_B_1x16(k+1, n);
        KERNEL_MMA_8ACC(rb0, rb1, rb2, rb3, rb0, rb1, rb2, rb3,
                        ra1, ra1, ra1, ra1, ra5, ra5, ra5, ra5);
        LOAD_B_1x16(k+2, n);
        KERNEL_MMA_8ACC(rb0, rb1, rb2, rb3, rb0, rb1, rb2, rb3,
                        ra2, ra2, ra2, ra2, ra6, ra6, ra6, ra6);
        LOAD_B_1x16(k+3, n);
        KERNEL_MMA_8ACC(rb0, rb1, rb2, rb3, rb0, rb1, rb2, rb3,
                        ra3, ra3, ra3, ra3, ra7, ra7, ra7, ra7);
      }
      for (; k < k2; k += 2) {
        LOAD_AT_8x2(m, k);
        LOAD_B_1x16(k, n);
        KERNEL_MMA_8ACC(rb0, rb1, rb2, rb3, rb0, rb1, rb2, rb3,
                        ra0, ra0, ra0, ra0, ra2, ra2, ra2, ra2);
        LOAD_B_1x16(k+1, n);
        KERNEL_MMA_8ACC(rb0, rb1, rb2, rb3, rb0, rb1, rb2, rb3,
                        ra1, ra1, ra1, ra1, ra3, ra3, ra3, ra3);
      }
      for (; k < K; k++) {
        LOAD_AT_8x1(m, k);
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

      register vector float ra0, ra1, ra2, ra3, ra4, ra5, ra6, ra7;
      register vector float rb0, rb1;
      register vector float t0, t1, t2, t3;

      for (k = 0; k < k4; k += 4) {
        LOAD_AT_8x4(m, k);
        LOAD_B_1x8(k, n);
        KERNEL_MMA_4ACC(rb0, rb1, rb0, rb1, ra0, ra0, ra4, ra4);
        LOAD_B_1x8(k+1, n);
        KERNEL_MMA_4ACC(rb0, rb1, rb0, rb1, ra1, ra1, ra5, ra5);
        LOAD_B_1x8(k+2, n);
        KERNEL_MMA_4ACC(rb0, rb1, rb0, rb1, ra2, ra2, ra6, ra6);
        LOAD_B_1x8(k+3, n);
        KERNEL_MMA_4ACC(rb0, rb1, rb0, rb1, ra3, ra3, ra7, ra7);
      }
      for (; k < k2; k += 2) {
        LOAD_AT_8x2(m, k);
        LOAD_B_1x8(k, n);
        KERNEL_MMA_4ACC(rb0, rb1, rb0, rb1, ra0, ra0, ra2, ra2);
        LOAD_B_1x8(k+1, n);
        KERNEL_MMA_4ACC(rb0, rb1, rb0, rb1, ra1, ra1, ra3, ra3);
      }
      for (; k < K; k++) {
        LOAD_AT_8x1(m, k);
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

      register vector float ra0, ra1, ra2, ra3, ra4, ra5, ra6, ra7;
      register vector float rb0;
      register vector float t0, t1, t2, t3;

      for (k = 0; k < k4; k += 4) {
        LOAD_AT_8x4(m, k);
        LOAD_B_1x4(k, n);
        KERNEL_MMA_2ACC(rb0, rb0, ra0, ra4);
        LOAD_B_1x4(k+1, n);
        KERNEL_MMA_2ACC(rb0, rb0, ra1, ra5);
        LOAD_B_1x4(k+2, n);
        KERNEL_MMA_2ACC(rb0, rb0, ra2, ra6);
        LOAD_B_1x4(k+3, n);
        KERNEL_MMA_2ACC(rb0, rb0, ra3, ra7);
      }
      for (; k < k2; k += 2) {
        LOAD_AT_8x2(m, k);
        LOAD_B_1x4(k, n);
        KERNEL_MMA_2ACC(rb0, rb0, ra0, ra2);
        LOAD_B_1x4(k+1, n);
        KERNEL_MMA_2ACC(rb0, rb0, ra1, ra3);
      }
      for (; k < K; k++) {
        LOAD_AT_8x1(m, k);
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

      register vector float ra0, ra1, ra2, ra3, ra4, ra5, ra6, ra7;
      register vector float rb0;
      register vector float t0, t1, t2, t3;

      for (k = 0; k < k4; k += 4) {
        LOAD_AT_8x4(m, k);
        LOAD_B_1x2(k, n);
        KERNEL_MMA_2ACC(rb0, rb0, ra0, ra4);
        LOAD_B_1x2(k+1, n);
        KERNEL_MMA_2ACC(rb0, rb0, ra1, ra5);
        LOAD_B_1x2(k+2, n);
        KERNEL_MMA_2ACC(rb0, rb0, ra2, ra6);
        LOAD_B_1x2(k+3, n);
        KERNEL_MMA_2ACC(rb0, rb0, ra3, ra7);
      }
      for (; k < k2; k += 2) {
        LOAD_AT_8x2(m, k);
        LOAD_B_1x2(k, n);
        KERNEL_MMA_2ACC(rb0, rb0, ra0, ra2);
        LOAD_B_1x2(k+1, n);
        KERNEL_MMA_2ACC(rb0, rb0, ra1, ra3);
      }
      for (; k < K; k++) {
        LOAD_AT_8x1(m, k);
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
      register vector float ra0, ra1, ra2, ra3, ra4, ra5, ra6, ra7;
      register vector float rb0;
      register vector float t0, t1, t2, t3;

      vector float result = ((vector float){0.,0.,0.,0.});
      vector float result1 = ((vector float){0.,0.,0.,0.});

      for (k = 0; k < k4; k += 4) {
        LOAD_AT_8x4(m, k);
        LOAD_B_1x1(k, n);
        KERNEL_VMADD_2VSR(ra0, ra4, rb0, rb0);
        LOAD_B_1x1(k+1, n);
        KERNEL_VMADD_2VSR(ra1, ra5, rb0, rb0);
        LOAD_B_1x1(k+2, n);
        KERNEL_VMADD_2VSR(ra2, ra6, rb0, rb0);
        LOAD_B_1x1(k+3, n);
        KERNEL_VMADD_2VSR(ra3, ra7, rb0, rb0);
      }
      for (; k < k2; k += 2) {
        LOAD_AT_8x2(m, k);
        LOAD_B_1x1(k, n);
        KERNEL_VMADD_2VSR(ra0, ra2, rb0, rb0);
        LOAD_B_1x1(k+1, n);
        KERNEL_VMADD_2VSR(ra1, ra3, rb0, rb0);
      }
      for (; k < K; k++) {
        LOAD_AT_8x1(m, k);
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

      register vector float ra0, ra1, ra2, ra3;
      register vector float rb0, rb1, rb2, rb3;
      register vector float t0, t1, t2, t3;

      for (k = 0; k < k4; k += 4) {
        LOAD_AT_4x4(m, k);
        LOAD_B_1x16(k, n);
        KERNEL_MMA_4ACC(rb0, rb1, rb2, rb3, ra0, ra0, ra0, ra0);
        LOAD_B_1x16(k+1, n);
        KERNEL_MMA_4ACC(rb0, rb1, rb2, rb3, ra1, ra1, ra1, ra1);
        LOAD_B_1x16(k+2, n);
        KERNEL_MMA_4ACC(rb0, rb1, rb2, rb3, ra2, ra2, ra2, ra2);
        LOAD_B_1x16(k+3, n);
        KERNEL_MMA_4ACC(rb0, rb1, rb2, rb3, ra3, ra3, ra3, ra3);
      }
      for (; k < k2; k += 2) {
        LOAD_AT_4x2(m, k);
        LOAD_B_1x16(k, n);
        KERNEL_MMA_4ACC(rb0, rb1, rb2, rb3, ra0, ra0, ra0, ra0);
        LOAD_B_1x16(k+1, n);
        KERNEL_MMA_4ACC(rb0, rb1, rb2, rb3, ra1, ra1, ra1, ra1);
      }
      for (; k < K; k++) {
        LOAD_AT_4x1(m, k);
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

      register vector float ra0, ra1, ra2, ra3;
      register vector float rb0, rb1;
      register vector float t0, t1, t2, t3;

      for (k = 0; k < k4; k += 4) {
        LOAD_AT_4x4(m, k);
        LOAD_B_1x8(k, n);
        KERNEL_MMA_2ACC(rb0, rb1, ra0, ra0);
        LOAD_B_1x8(k+1, n);
        KERNEL_MMA_2ACC(rb0, rb1, ra1, ra1);
        LOAD_B_1x8(k+2, n);
        KERNEL_MMA_2ACC(rb0, rb1, ra2, ra2);
        LOAD_B_1x8(k+3, n);
        KERNEL_MMA_2ACC(rb0, rb1, ra3, ra3);
      }
      for (; k < k2; k += 2) {
        LOAD_AT_4x2(m, k);
        LOAD_B_1x8(k, n);
        KERNEL_MMA_2ACC(rb0, rb1, ra0, ra0);
        LOAD_B_1x8(k+1, n);
        KERNEL_MMA_2ACC(rb0, rb1, ra1, ra1);
      }
      for (; k < K; k++) {
        LOAD_AT_4x1(m, k);
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

      register vector float ra0, ra1, ra2, ra3;
      register vector float rb0;
      register vector float t0, t1, t2, t3;

      for (k = 0; k < k4; k += 4) {
        LOAD_AT_4x4(m, k);
        LOAD_B_1x4(k, n);
        KERNEL_MMA_1ACC(rb0, ra0);
        LOAD_B_1x4(k+1, n);
        KERNEL_MMA_1ACC(rb0, ra1);
        LOAD_B_1x4(k+2, n);
        KERNEL_MMA_1ACC(rb0, ra2);
        LOAD_B_1x4(k+3, n);
        KERNEL_MMA_1ACC(rb0, ra3);
      }
      for (; k < k2; k += 2) {
        LOAD_AT_4x2(m, k);
        LOAD_B_1x4(k, n);
        KERNEL_MMA_1ACC(rb0, ra0);
        LOAD_B_1x4(k+1, n);
        KERNEL_MMA_1ACC(rb0, ra1);
      }
      for (; k < K; k++) {
        LOAD_AT_4x1(m, k);
        LOAD_B_1x4(k, n);
        KERNEL_MMA_1ACC(rb0, ra0);
      }

#if !defined(B0)
      register vector float rc0;
#endif
      vector float result[4];
      SAVE_4x4_ACC(&acc0, n, m);
    }

    for (; n < n2; n += 2) {
      __vector_quad acc0;

      INIT_1ACC();

      register vector float ra0, ra1, ra2, ra3;
      register vector float rb0;
      register vector float t0, t1, t2, t3;

      for (k = 0; k < k4; k += 4) {
        LOAD_AT_4x4(m, k);
        LOAD_B_1x2(k, n);
        KERNEL_MMA_1ACC(rb0, ra0);
        LOAD_B_1x2(k+1, n);
        KERNEL_MMA_1ACC(rb0, ra1);
        LOAD_B_1x2(k+2, n);
        KERNEL_MMA_1ACC(rb0, ra2);
        LOAD_B_1x2(k+3, n);
        KERNEL_MMA_1ACC(rb0, ra3);
      }
      for (; k < k2; k += 2) {
        LOAD_AT_4x2(m, k);
        LOAD_B_1x2(k, n);
        KERNEL_MMA_1ACC(rb0, ra0);
        LOAD_B_1x2(k+1, n);
        KERNEL_MMA_1ACC(rb0, ra1);
      }
      for (; k < K; k++) {
        LOAD_AT_4x1(m, k);
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
      register vector float ra0, ra1, ra2, ra3;
      register vector float rb0;
      register vector float t0, t1, t2, t3;

      vector float result = ((vector float){0.,0.,0.,0.});

      for (k = 0; k < k4; k += 4) {
        LOAD_AT_4x4(m, k);
        LOAD_B_1x1(k, n);
        KERNEL_VMADD_1VSR(ra0, rb0);
        LOAD_B_1x1(k+1, n);
        KERNEL_VMADD_1VSR(ra1, rb0);
        LOAD_B_1x1(k+2, n);
        KERNEL_VMADD_1VSR(ra2, rb0);
        LOAD_B_1x1(k+3, n);
        KERNEL_VMADD_1VSR(ra3, rb0);
      }
      for (; k < k2; k += 2) {
        LOAD_AT_4x2(m, k);
        LOAD_B_1x1(k, n);
        KERNEL_VMADD_1VSR(ra0, rb0);
        LOAD_B_1x1(k+1, n);
        KERNEL_VMADD_1VSR(ra1, rb0);
      }
      for (; k < K; k++) {
        LOAD_AT_4x1(m, k);
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

      register vector float ra0, ra1, ra2, ra3;
      register vector float rb0, rb1, rb2, rb3;
      register vector float t0, t1, t2;

      for (k = 0; k < k4; k += 4) {
        LOAD_AT_2x4(m, k);
        LOAD_B_1x16(k, n);
        KERNEL_MMA_4ACC(rb0, rb1, rb2, rb3, ra0, ra0, ra0, ra0);
        LOAD_B_1x16(k+1, n);
        KERNEL_MMA_4ACC(rb0, rb1, rb2, rb3, ra1, ra1, ra1, ra1);
        LOAD_B_1x16(k+2, n);
        KERNEL_MMA_4ACC(rb0, rb1, rb2, rb3, ra2, ra2, ra2, ra2);
        LOAD_B_1x16(k+3, n);
        KERNEL_MMA_4ACC(rb0, rb1, rb2, rb3, ra3, ra3, ra3, ra3);
      }
      for (; k < k2; k += 2) {
        LOAD_AT_2x2(m, k);
        LOAD_B_1x16(k, n);
        KERNEL_MMA_4ACC(rb0, rb1, rb2, rb3, ra0, ra0, ra0, ra0);
        LOAD_B_1x16(k+1, n);
        KERNEL_MMA_4ACC(rb0, rb1, rb2, rb3, ra1, ra1, ra1, ra1);
      }
      for (; k < K; k++) {
        LOAD_AT_2x1(m, k);
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

      register vector float ra0, ra1, ra2, ra3;
      register vector float rb0, rb1;
      register vector float t0, t1, t2;

      for (k = 0; k < k4; k += 4) {
        LOAD_AT_2x4(m, k);
        LOAD_B_1x8(k, n);
        KERNEL_MMA_2ACC(rb0, rb1, ra0, ra0);
        LOAD_B_1x8(k+1, n);
        KERNEL_MMA_2ACC(rb0, rb1, ra1, ra1);
        LOAD_B_1x8(k+2, n);
        KERNEL_MMA_2ACC(rb0, rb1, ra2, ra2);
        LOAD_B_1x8(k+3, n);
        KERNEL_MMA_2ACC(rb0, rb1, ra3, ra3);
      }
      for (; k < k2; k += 2) {
        LOAD_AT_2x2(m, k);
        LOAD_B_1x8(k, n);
        KERNEL_MMA_2ACC(rb0, rb1, ra0, ra0);
        LOAD_B_1x8(k+1, n);
        KERNEL_MMA_2ACC(rb0, rb1, ra1, ra1);
      }
      for (; k < K; k++) {
        LOAD_AT_2x1(m, k);
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

      register vector float ra0, ra1, ra2, ra3;
      register vector float rb0;
      register vector float t0, t1, t2;

      for (k = 0; k < k4; k += 4) {
        LOAD_AT_2x4(m, k);
        LOAD_B_1x4(k, n);
        KERNEL_MMA_1ACC(rb0, ra0);
        LOAD_B_1x4(k+1, n);
        KERNEL_MMA_1ACC(rb0, ra1);
        LOAD_B_1x4(k+2, n);
        KERNEL_MMA_1ACC(rb0, ra2);
        LOAD_B_1x4(k+3, n);
        KERNEL_MMA_1ACC(rb0, ra3);
      }
      for (; k < k2; k += 2) {
        LOAD_AT_2x2(m, k);
        LOAD_B_1x4(k, n);
        KERNEL_MMA_1ACC(rb0, ra0);
        LOAD_B_1x4(k+1, n);
        KERNEL_MMA_1ACC(rb0, ra1);
      }
      for (; k < K; k++) {
        LOAD_AT_2x1(m, k);
        LOAD_B_1x4(k, n);
        KERNEL_MMA_1ACC(rb0, ra0);
      }

#if !defined(B0)
      register vector float rc0;
#endif
      vector float result[4];
      SAVE_4x2_ACC(&acc0, n, m);
    }

    for (; n < n2; n += 2) {
      vector float result = ((vector float){0.,0.,0.,0.});

      register vector float ra0;
      register vector float rb0;

      for (k = 0; k < K; k++) {
        LOAD_A_2x2(m, k);
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
        LOAD_A_1x1(m, k);
        ra0 = vec_insert(A[(m+1)*lda+k], ra0, 1);
        ra0 = vec_insert(A[(m+1)*lda+k], ra0, 3);
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
        LOAD_A_1x1(m, k);
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
        LOAD_A_1x1(m, k);
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
        LOAD_A_1x1(m, k);
        LOAD_B_1x4(k, n);
        KERNEL_VMADD_1VSR(ra0, rb0);
      }

      SAVE_4x1_VSR(result, n, m);
    }

    for (; n < n2; n += 2) {
      vector float result = ((vector float){0.,0.,0.,0.});

      register vector float ra0;
      register vector float rb0;

      for (k = 0; k < K; k++) {
        LOAD_A_1x1(m, k);
        LOAD_B_1x2(k, n);
        KERNEL_VMADD_1VSR(ra0, rb0);
      }

      SAVE_2x1_VSR(result, n, m);
    }

    for (; n < N; n++) {
      FLOAT result = 0.0f;

      for (k = 0; k < K; k++) {
        result += A[m*lda+k] * B[k*ldb+n];
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
