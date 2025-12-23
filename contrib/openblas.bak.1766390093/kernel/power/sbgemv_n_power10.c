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

#ifndef SBGEMV_N_MMA_C
#define SBGEMV_N_MMA_C

#define USE_BFGEMV_N_MMA

#ifdef USE_BFGEMV_N_MMA
#include "sbgemv_common_power10.c"

#ifndef BF16GEMV_N_X
#define BF16GEMV_N_X
#define BF16GEMV_N_8 BF16GEMV_N_MMA_8
#define BF16GEMV_N_4 BF16GEMV_N_MMA_4
#define BF16GEMV_N_2 BF16GEMV_N_MMA_2
#define BF16GEMV_N_1 BF16GEMV_N_MMA_1
#endif

#define USE_BFGEMV_8_N_MMA

static void BF16GEMV_N_MMA_1(BLASLONG n, IFLOAT **ap, IFLOAT *xo, FLOAT *y, FLOAT alpha)
{
  IFLOAT *a0;
  __vector_quad temp[2*4];
  vec_f32 temp0[8*4];
  vec_f32 v_alpha = { alpha, alpha, alpha, alpha };

  a0 = ap[0];

  vec_bf16 *va0 = (vec_bf16 *)a0;

  vec_bf16 *x_bf = (vec_bf16 *)(xo);

  vec_f32 *v_y = (vec_f32 *)y;
  BLASLONG n8 = n / 8;
  BLASLONG i = 0;

#ifdef USE_MERGE_MMA
  vec_bf16 v_x0[4];
  v_x0[0] = vec_loadN(x_bf, 1);
  vec_f32 vy0[2*4*2];

  vec_make_mult1(v_x0, false);

  for (; i + 8 <= n8; i += 8) {
    vec_load_mult184_mma(&temp[0], &va0[i +  0], &v_x0[ 0]);
    vec_load_mult184_mma(&temp[2], &va0[i +  4], &v_x0[ 0]);

    vec_load8_pair(vy0, &v_y[(i * 2) + 0]);

    vec_reduce88_mma(&temp[0], temp0 +  0, v_alpha, vy0 +  0);

    vec_store8_pair(&v_y[(i * 2) + 0], vy0);
  }

  if (n8 & 4) {
    vec_load_mult184_mma(&temp[0], &va0[i + 0], &v_x0[ 0]);

    vec_load4_pair(vy0, &v_y[(i * 2) + 0]);

    vec_reduce84_mma(&temp[0], temp0, v_alpha, vy0);

    vec_store4_pair(&v_y[(i * 2) + 0], vy0);

    i += 4;
  }
#else
  vec_bf16 v_x0[1];
  v_x0[0] = vec_loadN(x_bf, 1);
  vec_f32 vy0[2*4];

  for (; i + 4 <= n8; i += 4) {
    vec_load_mult18_mma(&temp[0], &va0[i + 0], v_x0[ 0]);

    vec_load4_pair(vy0, &v_y[(i * 2) + 0]);

    vec_reduce8_mma(&temp[0], temp0, v_alpha, vy0);

    vec_store4_pair(&v_y[(i * 2) + 0], vy0);
  }
#endif

  for (; i < n8; i++) {
    vec_load_mult12_mma(&temp[0], &va0[i], v_x0[ 0]);

    vec_load_pair(vy0, &v_y[(i * 2) + 0]);

    vec_reduce2_mma(&temp[0], temp0, v_alpha, vy0);

    vec_store_pair(&v_y[(i * 2) + 0], vy0);
  }

  n &= 7;
  if (n > 4) {
    vec_loadN_mult12_mma(&temp[0], &va0[i], v_x0[ 0], n);

    n &= 3;
    vec_loadN2_f32(vy0, &v_y[(i * 2) + 0], n);

    vec_reduce2_mma(&temp[0], temp0, v_alpha, vy0);

    vec_storeN2_f32(vy0, &v_y[(i * 2) + 0], n);
  } else if (n) {
    vec_loadN_mult11_mma(&temp[0], &va0[i], v_x0[ 0], n);

    vy0[0] = vec_loadN_f32(&v_y[(i * 2) + 0], n);

    vec_reduce1_mma(&temp[0], temp0, v_alpha, vy0);

    vec_storeN_f32(vy0[0], &v_y[(i * 2) + 0], n);
  }
}

static void BF16GEMV_N_MMA_2(BLASLONG n, IFLOAT **ap, IFLOAT *xo, FLOAT *y, FLOAT alpha)
{
  IFLOAT *a0, *a1;
  __vector_quad temp[2*4];
  vec_f32 temp0[8*4];
  vec_f32 v_alpha = { alpha, alpha, alpha, alpha };

  a0 = ap[0];
  a1 = ap[1];

  vec_bf16 *va0 = (vec_bf16 *)a0;
  vec_bf16 *va1 = (vec_bf16 *)a1;

  vec_bf16 *x_bf = (vec_bf16 *)(xo);

  vec_f32 *v_y = (vec_f32 *)y;
  BLASLONG n8 = n / 8;
  BLASLONG i = 0;

#ifdef USE_MERGE_MMA
  vec_bf16 v_x0[4];
  vec_f32 vy0[2*4*2];
  v_x0[0] = vec_loadN(x_bf, 2);

  vec_make_mult1(v_x0, false);

  for (; i + 8 <= n8; i += 8) {
    vec_load_mult288a_mma(&temp[0], &va0[i +  0], &va1[i +  0], &v_x0[ 0]);

    vec_load8_pair(vy0, &v_y[(i * 2) + 0]);

    vec_reduce88_mma(&temp[0], temp0 +  0, v_alpha, vy0 +  0);

    vec_store8_pair(&v_y[(i * 2) + 0], vy0);
  }

  if (n8 & 4) {
    vec_load_mult284a_mma(&temp[0], &va0[i + 0], &va1[i + 0], &v_x0[ 0]);

    vec_load4_pair(vy0, &v_y[(i * 2) + 0]);

    vec_reduce84_mma(&temp[0], temp0, v_alpha, vy0);

    vec_store4_pair(&v_y[(i * 2) + 0], vy0);

    i += 4;
  }
#else
  vec_bf16 v_x0[1];
  vec_f32 vy0[2*4];
  v_x0[0] = vec_loadN(x_bf, 2);

  for (; i + 4 <= n8; i += 4) {
    vec_load_mult28a_mma(&temp[0], &va0[i + 0], &va1[i + 0], v_x0[ 0]);

    vec_load4_pair(vy0, &v_y[(i * 2) + 0]);

    vec_reduce8_mma(&temp[0], temp0, v_alpha, vy0);

    vec_store4_pair(&v_y[(i * 2) + 0], vy0);
  }
#endif

  for (; i < n8; i++) {
    vec_load_mult22a_mma(&temp[0], &va0[i], &va1[i], v_x0[ 0]);

    vec_load_pair(vy0, &v_y[(i * 2) + 0]);

    vec_reduce2_mma(&temp[0], temp0, v_alpha, vy0);

    vec_store_pair(&v_y[(i * 2) + 0], vy0);
  }

  n &= 7;
  if (n > 4) {
    vec_loadN_mult22a_mma(&temp[0], &va0[i], &va1[i], v_x0[ 0], n);

    n &= 3;
    vec_loadN2_f32(vy0, &v_y[(i * 2) + 0], n);

    vec_reduce2_mma(&temp[0], temp0, v_alpha, vy0);

    vec_storeN2_f32(vy0, &v_y[(i * 2) + 0], n);
  } else if (n) {
    vec_loadN_mult11a_mma(&temp[0], &va0[i], &va1[i], v_x0[ 0], n);

    vy0[0] = vec_loadN_f32(&v_y[(i * 2) + 0], n);

    vec_reduce1_mma(&temp[0], temp0, v_alpha, vy0);

    vec_storeN_f32(vy0[0], &v_y[(i * 2) + 0], n);
  }
}

static void BF16GEMV_N_MMA_4(BLASLONG n, IFLOAT **ap, IFLOAT *xo, FLOAT *y, FLOAT alpha)
{
  IFLOAT *a0, *a1, *a2, *a3;
  __vector_quad temp[2*4];
  vec_f32 temp0[8*4];
  vec_f32 v_alpha = { alpha, alpha, alpha, alpha };

  a0 = ap[0];
  a1 = ap[1];
  a2 = ap[2];
  a3 = ap[3];

  vec_bf16 *va0 = (vec_bf16 *)a0;
  vec_bf16 *va1 = (vec_bf16 *)a1;
  vec_bf16 *va2 = (vec_bf16 *)a2;
  vec_bf16 *va3 = (vec_bf16 *)a3;

  vec_bf16 *x_bf = (vec_bf16 *)(xo);

  vec_f32 *v_y = (vec_f32 *)y;
  BLASLONG n8 = n / 8;
  BLASLONG i = 0;

#ifdef USE_MERGE_MMA
  vec_bf16 v_x0[8];
  vec_f32 vy0[2*4*2];
  v_x0[0] = vec_loadN(x_bf, 4);

  vec_make_mult2(v_x0);

  for (; i + 8 <= n8; i += 8) {
    vec_load_mult288a_mma(&temp[0], &va0[i +  0], &va1[i +  0], &v_x0[ 0]);
    vec_load_mult288b_mma(&temp[0], &va2[i +  0], &va3[i +  0], &v_x0[ 4]);

    vec_load8_pair(vy0, &v_y[(i * 2) + 0]);

    vec_reduce88_mma(&temp[0], temp0 +  0, v_alpha, vy0 +  0);

    vec_store8_pair(&v_y[(i * 2) + 0], vy0);
  }

  if (n8 & 4) {
    vec_load_mult284a_mma(&temp[0], &va0[i + 0], &va1[i + 0], &v_x0[ 0]);
    vec_load_mult284b_mma(&temp[0], &va2[i + 0], &va3[i + 0], &v_x0[ 4]);

    vec_load4_pair(vy0, &v_y[(i * 2) + 0]);

    vec_reduce84_mma(&temp[0], temp0, v_alpha, vy0);

    vec_store4_pair(&v_y[(i * 2) + 0], vy0);

    i += 4;
  }
#else
  vec_bf16 v_x0[5];
  vec_f32 vy0[2*4];
  v_x0[0] = vec_loadN(x_bf, 4);

  v_x0[ 4] = (vec_bf16)vec_splat((vec_f32)v_x0[0], 1);

  for (; i + 4 <= n8; i += 4) {
    vec_load_mult28a_mma(&temp[0], &va0[i + 0], &va1[i + 0], v_x0[ 0]);
    vec_load_mult28b_mma(&temp[0], &va2[i + 0], &va3[i + 0], v_x0[ 4]);

    vec_load4_pair(vy0, &v_y[(i * 2) + 0]);

    vec_reduce8_mma(&temp[0], temp0, v_alpha, vy0);

    vec_store4_pair(&v_y[(i * 2) + 0], vy0);
  }
#endif

  for (; i < n8; i++) {
    vec_load_mult22a_mma(&temp[0], &va0[i], &va1[i], v_x0[ 0]);
    vec_load_mult22b_mma(&temp[0], &va2[i], &va3[i], v_x0[ 4]);

    vec_load_pair(vy0, &v_y[(i * 2) + 0]);

    vec_reduce2_mma(&temp[0], temp0, v_alpha, vy0);

    vec_store_pair(&v_y[(i * 2) + 0], vy0);
  }

  n &= 7;
  if (n > 4) {
    vec_loadN_mult22a_mma(&temp[0], &va0[i], &va1[i], v_x0[ 0], n);
    vec_loadN_mult22b_mma(&temp[0], &va2[i], &va3[i], v_x0[ 4], n);

    n &= 3;
    vec_loadN2_f32(vy0, &v_y[(i * 2) + 0], n);

    vec_reduce2_mma(&temp[0], temp0, v_alpha, vy0);

    vec_storeN2_f32(vy0, &v_y[(i * 2) + 0], n);
  } else if (n) {
    vec_loadN_mult11a_mma(&temp[0], &va0[i], &va1[i], v_x0[ 0], n);
    vec_loadN_mult11b_mma(&temp[0], &va2[i], &va3[i], v_x0[ 4], n);

    vy0[0] = vec_loadN_f32(&v_y[(i * 2) + 0], n);

    vec_reduce1_mma(&temp[0], temp0, v_alpha, vy0);

    vec_storeN_f32(vy0[0], &v_y[(i * 2) + 0], n);
  }
}

#ifdef USE_BFGEMV_8_N_MMA
static void BF16GEMV_N_MMA_8(BLASLONG n, IFLOAT **ap, IFLOAT *xo, FLOAT *y, BLASLONG lda4, FLOAT alpha)
{
  IFLOAT *a0, *a1, *a2, *a3, *b0, *b1, *b2, *b3;
  __vector_quad temp[2*4];
  vec_f32 temp0[8*4];
  vec_f32 v_alpha = { alpha, alpha, alpha, alpha };

  a0 = ap[0];
  a1 = ap[1];
  a2 = ap[2];
  a3 = ap[3];
  b0 = a0 + lda4;
  b1 = a1 + lda4;
  b2 = a2 + lda4;
  b3 = a3 + lda4;

  vec_bf16 *va0 = (vec_bf16 *)a0;
  vec_bf16 *va1 = (vec_bf16 *)a1;
  vec_bf16 *va2 = (vec_bf16 *)a2;
  vec_bf16 *va3 = (vec_bf16 *)a3;
  vec_bf16 *vb0 = (vec_bf16 *)b0;
  vec_bf16 *vb1 = (vec_bf16 *)b1;
  vec_bf16 *vb2 = (vec_bf16 *)b2;
  vec_bf16 *vb3 = (vec_bf16 *)b3;

  vec_bf16 *x_bf = (vec_bf16 *)(xo);

  vec_f32 *v_y = (vec_f32 *)y;
  BLASLONG n8 = n / 8;
  BLASLONG i = 0;

#ifdef USE_MERGE_MMA
  vec_bf16 v_x0[16];
  vec_f32 vy0[2*4*2];
  v_x0[0] = (vec_bf16)vec_load_vec(x_bf);

  vec_make_mult4(v_x0);

  for (; i + 8 <= n8; i += 8) {
    vec_load_mult288a_mma(&temp[0], &va0[i +  0], &va1[i +  0], &v_x0[ 0]);
    vec_load_mult288b_mma(&temp[0], &va2[i +  0], &va3[i +  0], &v_x0[ 4]);
    vec_load_mult288b_mma(&temp[0], &vb0[i +  0], &vb1[i +  0], &v_x0[ 8]);
    vec_load_mult288b_mma(&temp[0], &vb2[i +  0], &vb3[i +  0], &v_x0[12]);

    vec_load8_pair(vy0, &v_y[(i * 2) + 0]);

    vec_reduce88_mma(&temp[0], temp0 +  0, v_alpha, vy0 +  0);

    vec_store8_pair(&v_y[(i * 2) + 0], vy0);
  }

  if (n8 & 4) {
    vec_load_mult284a_mma(&temp[0], &va0[i + 0], &va1[i + 0], &v_x0[ 0]);
    vec_load_mult284b_mma(&temp[0], &va2[i + 0], &va3[i + 0], &v_x0[ 4]);
    vec_load_mult284b_mma(&temp[0], &vb0[i + 0], &vb1[i + 0], &v_x0[ 8]);
    vec_load_mult284b_mma(&temp[0], &vb2[i + 0], &vb3[i + 0], &v_x0[12]);

    vec_load4_pair(vy0, &v_y[(i * 2) + 0]);

    vec_reduce84_mma(&temp[0], temp0, v_alpha, vy0);

    vec_store4_pair(&v_y[(i * 2) + 0], vy0);

    i += 4;
  }
#else
  vec_bf16 v_x0[13];
  vec_f32 vy0[2*4];
  v_x0[0] = (vec_bf16)vec_load_vec(x_bf);

  v_x0[ 4] = (vec_bf16)vec_splat((vec_f32)v_x0[0], 1);
  v_x0[ 8] = (vec_bf16)vec_splat((vec_f32)v_x0[0], 2);
  v_x0[12] = (vec_bf16)vec_splat((vec_f32)v_x0[0], 3);

  for (; i + 4 <= n8; i += 4) {
    vec_load_mult28a_mma(&temp[0], &va0[i + 0], &va1[i + 0], v_x0[ 0]);
    vec_load_mult28b_mma(&temp[0], &va2[i + 0], &va3[i + 0], v_x0[ 4]);
    vec_load_mult28b_mma(&temp[0], &vb0[i + 0], &vb1[i + 0], v_x0[ 8]);
    vec_load_mult28b_mma(&temp[0], &vb2[i + 0], &vb3[i + 0], v_x0[12]);

    vec_load4_pair(vy0, &v_y[(i * 2) + 0]);

    vec_reduce8_mma(&temp[0], temp0, v_alpha, vy0);

    vec_store4_pair(&v_y[(i * 2) + 0], vy0);
  }
#endif

  for (; i < n8; i++) {
    vec_load_mult22a_mma(&temp[0], &va0[i], &va1[i], v_x0[ 0]);
    vec_load_mult22b_mma(&temp[0], &va2[i], &va3[i], v_x0[ 4]);
    vec_load_mult22b_mma(&temp[0], &vb0[i], &vb1[i], v_x0[ 8]);
    vec_load_mult22b_mma(&temp[0], &vb2[i], &vb3[i], v_x0[12]);

    vec_load_pair(vy0, &v_y[(i * 2) + 0]);

    vec_reduce2_mma(&temp[0], temp0, v_alpha, vy0);

    vec_store_pair(&v_y[(i * 2) + 0], vy0);
  }

  n &= 7;
  if (n > 4) {
    vec_loadN_mult22a_mma(&temp[0], &va0[i], &va1[i], v_x0[ 0], n);
    vec_loadN_mult22b_mma(&temp[0], &va2[i], &va3[i], v_x0[ 4], n);
    vec_loadN_mult22b_mma(&temp[0], &vb0[i], &vb1[i], v_x0[ 8], n);
    vec_loadN_mult22b_mma(&temp[0], &vb2[i], &vb3[i], v_x0[12], n);

    n &= 3;
    vec_loadN2_f32(vy0, &v_y[(i * 2) + 0], n);

    vec_reduce2_mma(&temp[0], temp0, v_alpha, vy0);

    vec_storeN2_f32(vy0, &v_y[(i * 2) + 0], n);
  } else if (n) {
    vec_loadN_mult11a_mma(&temp[0], &va0[i], &va1[i], v_x0[ 0], n);
    vec_loadN_mult11b_mma(&temp[0], &va2[i], &va3[i], v_x0[ 4], n);
    vec_loadN_mult11b_mma(&temp[0], &vb0[i], &vb1[i], v_x0[ 8], n);
    vec_loadN_mult11b_mma(&temp[0], &vb2[i], &vb3[i], v_x0[12], n);

    vy0[0] = vec_loadN_f32(&v_y[(i * 2) + 0], n);

    vec_reduce1_mma(&temp[0], temp0, v_alpha, vy0);

    vec_storeN_f32(vy0[0], &v_y[(i * 2) + 0], n);
  }
}
#endif

#include "sbgemv_n.c"
#else
#include "sbgemv_n_vsx.c"
#endif
#endif

