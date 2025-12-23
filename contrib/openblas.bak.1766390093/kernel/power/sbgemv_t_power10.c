/***************************************************************************
Copyright (c) 2024, The OpenBLAS Project
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

#ifndef SBGEMV_T_MMA_C
#define SBGEMV_T_MMA_C

#define USE_BFGEMV_T_MMA

#ifdef USE_BFGEMV_T_MMA
#include "sbgemv_common_power10.c"

#ifndef BF16GEMV_T_X
#define BF16GEMV_T_X
#define BF16GEMV_T_8 BF16GEMV_T_MMA_8
#define BF16GEMV_T_4 BF16GEMV_T_MMA_4
#define BF16GEMV_T_2 BF16GEMV_T_MMA_2
#define BF16GEMV_T_1 BF16GEMV_T_MMA_1
#endif

#define USE_BFGEMV_8_T_MMA

static void BF16GEMV_T_MMA_1(BLASLONG n, BLASLONG lda, IFLOAT *ap, IFLOAT *x, FLOAT *y, FLOAT alpha)
{
  IFLOAT *a0;
  vec_bf16 *va0, *v_x;
  __vector_quad temp0;
  vec_f32 temp00[4];
  vec_bf16 inp[4];

  __builtin_mma_xxsetaccz(&temp0);

  a0 = ap;
  va0 = (vec_bf16 *)a0;
  v_x = (vec_bf16 *)x;
  BLASLONG n8 = n / 8;
  BLASLONG i = 0;

  for (; i + 4 <= n8; i += 4) {
    vec_load_pair2(inp, &v_x[i]);

    vec_load_mult4_mma(&temp0, &va0[i + 0], inp);
  }

  if (n8 & 2) {
    vec_load_pair((vec_f32 *)inp, (vec_f32 *)&v_x[i]);

    vec_load_mult2_mma(&temp0, &va0[i + 0], inp);

    i += 2;
  }

  if (n8 & 1) {
    inp[0] = (vec_bf16)vec_load_vec(&v_x[i]);

    vec_load_mult_mma(&temp0, &va0[i], inp[0]);

    i++;
  }

  n &= 7;
  if (n) {
    inp[0] = vec_loadN(&v_x[i], n);

    vec_loadN_mult_mma(&temp0, &va0[i], inp[0], n);
  }

  __builtin_mma_disassemble_acc((void*)temp00, &temp0);

  y[0] += (alpha * (temp00[0][0] + temp00[1][1] + temp00[2][2] + temp00[3][3]));
}

static void BF16GEMV_T_MMA_2(BLASLONG n, BLASLONG lda, IFLOAT *ap, IFLOAT *x, FLOAT *y, FLOAT alpha)
{
  IFLOAT *a0, *a1;
  vec_bf16 *va0, *va1, *v_x;
  __vector_quad temp0[2];
  vec_f32 temp00[4*2];
  vec_bf16 inp[4];

  vec_setzero_2(&temp0[0]);

  a0 = ap;
  a1 = ap + lda;
  va0 = (vec_bf16 *)a0;
  va1 = (vec_bf16 *)a1;
  v_x = (vec_bf16 *)x;
  BLASLONG n8 = n / 8;
  BLASLONG i = 0;

  for (; i + 4 <= n8; i += 4) {
    vec_load_pair2(inp, &v_x[i]);

    vec_load_mult42_mma(&temp0[0], &va0[i + 0], &va1[i + 0], inp);
  }

  if (n8 & 2) {
    vec_load_pair((vec_f32 *)inp, (vec_f32 *)&v_x[i]);

    vec_load_mult22_mma(&temp0[0], &va0[i + 0], &va1[i + 0], inp);

    i += 2;
  }

  if (n8 & 1) {
    inp[0] = (vec_bf16)vec_load_vec(&v_x[i]);

    vec_load_mult12a_mma(&temp0[0], &va0[i], &va1[i], inp[0]);

    i++;
  }

  n &= 7;
  if (n) {
    inp[0] = vec_loadN(&v_x[i], n);

    vec_loadN_mult12a_mma(&temp0[0], &va0[i], &va1[i], inp[0], n);
  }

  vec_reduce_2(temp00, &temp0[0]);

  y[0] += (alpha * (temp00[0][0] + temp00[1][1] + temp00[2][2] + temp00[3][3]));
  y[1] += (alpha * (temp00[4][0] + temp00[5][1] + temp00[6][2] + temp00[7][3]));
}

static void BF16GEMV_T_MMA_4(BLASLONG n, BLASLONG lda, IFLOAT *ap, IFLOAT *x, FLOAT *y, FLOAT alpha)
{
  IFLOAT *a0, *a1, *a2, *a3;
  vec_bf16 *va0, *va1, *va2, *va3, *v_x;
  __vector_quad temp0[4];
  vec_f32 temp00[4*4];
  vec_bf16 inp[4];

  vec_setzero_4(&temp0[0]);

  a0 = ap;
  a1 = ap + lda;
  a2 = a1 + lda;
  a3 = a2 + lda;
  va0 = (vec_bf16 *)a0;
  va1 = (vec_bf16 *)a1;
  va2 = (vec_bf16 *)a2;
  va3 = (vec_bf16 *)a3;
  v_x = (vec_bf16 *)x;
  BLASLONG n8 = n / 8;
  BLASLONG i = 0;

  for (; i + 4 <= n8; i += 4) {
    vec_load_pair2(inp, &v_x[i]);

    vec_load_mult44_mma(&temp0[0], &va0[i + 0], &va1[i + 0], &va2[i + 0], &va3[i + 0], inp);
  }

  if (n8 & 2) {
    vec_load_pair((vec_f32 *)inp, (vec_f32 *)&v_x[i]);

    vec_load_mult24_mma(&temp0[0], &va0[i + 0], &va1[i + 0], &va2[i + 0], &va3[i + 0], inp);

    i += 2;
  }

  if (n8 & 1) {
    inp[0] = (vec_bf16)vec_load_vec(&v_x[i]);

    vec_load_mult14_mma(&temp0[0], &va0[i], &va1[i], &va2[i], &va3[i], inp[0]);

    i++;
  }

  n &= 7;
  if (n) {
    inp[0] = vec_loadN(&v_x[i], n);

    vec_loadN_mult14_mma(&temp0[0], &va0[i], &va1[i], &va2[i], &va3[i], inp[0], n);
  }

  vec_reduce_4(temp00, &temp0[0]);

  vec_f32 t0, t1, t2, t3, t4, t5, t6, t7;
  vec_f32 a = { alpha, alpha, alpha, alpha };
  vec_f32 *v_y = (vec_f32 *) y;

  t0 = vec_mergeh(temp00[ 0], temp00[ 4]);
  t1 = vec_mergeh(temp00[ 8], temp00[12]);
  t2 = vec_mergeo(temp00[ 1], temp00[ 5]);
  t3 = vec_mergeo(temp00[ 9], temp00[13]);
  t4 = vec_mergel(temp00[ 2], temp00[ 6]);
  t5 = vec_mergel(temp00[10], temp00[14]);
  t6 = vec_mergeo(temp00[ 3], temp00[ 7]);
  t7 = vec_mergeo(temp00[11], temp00[15]);
  t0 = vec_xxpermdi(t0, t1, 0);
  t2 = vec_xxpermdi(t2, t3, 0);
  t4 = vec_xxpermdi(t4, t5, 0);
  t6 = vec_xxpermdi(t6, t7, 3);

  t0 += t2 + t4 + t6;

  v_y[0] += (a * t0);
}

#ifdef USE_BFGEMV_8_T_MMA
static void BF16GEMV_T_MMA_8(BLASLONG n, BLASLONG lda, IFLOAT *ap, IFLOAT *x, FLOAT *y, FLOAT alpha)
{
  IFLOAT *a0, *a1, *a2, *a3, *a4, *a5, *a6, *a7;
  vec_bf16 *va0, *va1, *va2, *va3, *va4, *va5, *va6, *va7, *v_x;
  __vector_quad temp0[8];
  vec_f32 temp00[4*8];
  vec_bf16 inp[4];

  vec_setzero_8(&temp0[0]);

  BLASLONG lda4 = lda << 2;
  a0 = ap;
  a1 = ap + lda;
  a2 = a1 + lda;
  a3 = a2 + lda;
  a4 = a0 + lda4;
  a5 = a1 + lda4;
  a6 = a2 + lda4;
  a7 = a3 + lda4;
  va0 = (vec_bf16 *)a0;
  va1 = (vec_bf16 *)a1;
  va2 = (vec_bf16 *)a2;
  va3 = (vec_bf16 *)a3;
  va4 = (vec_bf16 *)a4;
  va5 = (vec_bf16 *)a5;
  va6 = (vec_bf16 *)a6;
  va7 = (vec_bf16 *)a7;
  v_x = (vec_bf16 *)x;
  BLASLONG n8 = n / 8;
  BLASLONG i = 0;

  for (; i + 4 <= n8; i += 4) {
    vec_load_pair2(inp, &v_x[i]);

    vec_load_mult44_mma(&temp0[0], &va0[i + 0], &va1[i + 0], &va2[i + 0], &va3[i + 0], inp);
    vec_load_mult44_mma(&temp0[4], &va4[i + 0], &va5[i + 0], &va6[i + 0], &va7[i + 0], inp);
  }

  if (n8 & 2) {
    vec_load_pair((vec_f32 *)inp, (vec_f32 *)&v_x[i]);

    vec_load_mult24_mma(&temp0[0], &va0[i + 0], &va1[i + 0], &va2[i + 0], &va3[i + 0], inp);
    vec_load_mult24_mma(&temp0[4], &va4[i + 0], &va5[i + 0], &va6[i + 0], &va7[i + 0], inp);

    i += 2;
  }

  if (n8 & 1) {
    inp[0] = (vec_bf16)vec_load_vec(&v_x[i]);

    vec_load_mult14_mma(&temp0[0], &va0[i], &va1[i], &va2[i], &va3[i], inp[0]);
    vec_load_mult14_mma(&temp0[4], &va4[i], &va5[i], &va6[i], &va7[i], inp[0]);

    i++;
  }

  n &= 7;
  if (n) {
    inp[0] = vec_loadN(&v_x[i], n);

    vec_loadN_mult14_mma(&temp0[0], &va0[i], &va1[i], &va2[i], &va3[i], inp[0], n);
    vec_loadN_mult14_mma(&temp0[4], &va4[i], &va5[i], &va6[i], &va7[i], inp[0], n);
  }

  vec_reduce_8(temp00, &temp0[0]);

  vec_f32 t0, t1, t2, t3, t4, t5, t6, t7, t10, t11, t12, t13, t14, t15, t16, t17;
  vec_f32 a = { alpha, alpha, alpha, alpha };
  vec_f32 *v_y = (vec_f32 *) y;

  t0 = vec_mergeh(temp00[ 0], temp00[ 4]);
  t1 = vec_mergeh(temp00[ 8], temp00[12]);
  t2 = vec_mergeo(temp00[ 1], temp00[ 5]);
  t3 = vec_mergeo(temp00[ 9], temp00[13]);
  t4 = vec_mergel(temp00[ 2], temp00[ 6]);
  t5 = vec_mergel(temp00[10], temp00[14]);
  t6 = vec_mergeo(temp00[ 3], temp00[ 7]);
  t7 = vec_mergeo(temp00[11], temp00[15]);
  t0 = vec_xxpermdi(t0, t1, 0);
  t2 = vec_xxpermdi(t2, t3, 0);
  t4 = vec_xxpermdi(t4, t5, 0);
  t6 = vec_xxpermdi(t6, t7, 3);

  t0 += t2 + t4 + t6;

  t10 = vec_mergeh(temp00[16], temp00[20]);
  t11 = vec_mergeh(temp00[24], temp00[28]);
  t12 = vec_mergeo(temp00[17], temp00[21]);
  t13 = vec_mergeo(temp00[25], temp00[29]);
  t14 = vec_mergel(temp00[18], temp00[22]);
  t15 = vec_mergel(temp00[26], temp00[30]);
  t16 = vec_mergeo(temp00[19], temp00[23]);
  t17 = vec_mergeo(temp00[27], temp00[31]);
  t10 = vec_xxpermdi(t10, t11, 0);
  t12 = vec_xxpermdi(t12, t13, 0);
  t14 = vec_xxpermdi(t14, t15, 0);
  t16 = vec_xxpermdi(t16, t17, 3);

  t10 += t12 + t14 + t16;

  vec_f32 inp2[2];
  vec_load_pair(inp2, v_y);
  inp2[0] += (a * t0);
  inp2[1] += (a * t10);
  vec_store_pair(v_y, inp2);
}
#endif

#include "sbgemv_t.c"
#else
#include "sbgemv_t_vsx.c"
#endif
#endif

