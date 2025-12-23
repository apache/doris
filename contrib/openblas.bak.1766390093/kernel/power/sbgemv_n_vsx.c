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

#ifndef SBGEMV_N_VSX_C
#define SBGEMV_N_VSX_C

#include "sbgemv_common.c"

#ifndef BF16GEMV_N_X
#define BF16GEMV_N_X
#define BF16GEMV_N_8 BF16GEMV_N_VSX_8
#define BF16GEMV_N_4 BF16GEMV_N_VSX_4
#define BF16GEMV_N_2 BF16GEMV_N_VSX_2
#define BF16GEMV_N_1 BF16GEMV_N_VSX_1
#endif

#define USE_BFGEMV_8_N_VSX

static void BF16GEMV_N_VSX_1(BLASLONG n, IFLOAT **ap, IFLOAT *xo, FLOAT *y, FLOAT alpha)
{
  IFLOAT *a0;
  vec_bf16 zero = { 0, 0, 0, 0, 0, 0, 0, 0 };
  vec_f32 v_alpha = { alpha, alpha, alpha, alpha };

  a0 = ap[0];

  vec_bf16 *va0 = (vec_bf16 *)a0;

  vec_bf16 *x_bf = (vec_bf16 *)(xo);
  vec_f32 x_0 = vec_loadNHi(x_bf, 1, zero);
  x_0 *= v_alpha;

  vec_f32 v_x0 = vec_splat(x_0, 0);

  vec_f32 *v_y = (vec_f32 *)y;
  BLASLONG n8 = n / 8;
  BLASLONG i = 0;
  vec_f32 vy0[2];

  for (; i < n8; i++) {
    vec_load_pair(vy0, &v_y[(i * 2) + 0]);

    vec_load_mult2(v_x0, &va0[i], zero, vy0);

    vec_store_pair(&v_y[(i * 2) + 0], vy0);
  }

  n &= 7;
  if (n > 4) {
    BLASLONG n3 = n & 3;
    vec_loadN2_f32(vy0, &v_y[(i * 2) + 0], n3);

    vec_loadN_mult2(v_x0, &va0[i], n, zero, vy0);

    vec_storeN2_f32(vy0, &v_y[(i * 2) + 0], n3);
  } else if (n) {
    vy0[0] = vec_loadN_f32(&v_y[(i * 2) + 0], n);

    vy0[0] += vec_loadNHi_mult(&va0[i], v_x0, n, zero);

    vec_storeN_f32(vy0[0], &v_y[(i * 2) + 0], n);
  }
}

static void BF16GEMV_N_VSX_2(BLASLONG n, IFLOAT **ap, IFLOAT *xo, FLOAT *y, FLOAT alpha)
{
  IFLOAT *a0, *a1;
  vec_bf16 zero = { 0, 0, 0, 0, 0, 0, 0, 0 };
  vec_f32 v_alpha = { alpha, alpha, alpha, alpha };

  a0 = ap[0];
  a1 = ap[1];

  vec_bf16 *va0 = (vec_bf16 *)a0;
  vec_bf16 *va1 = (vec_bf16 *)a1;

  vec_bf16 *x_bf = (vec_bf16 *)(xo);
  vec_f32 x_0 = vec_loadNHi(x_bf, 2, zero);
  x_0 *= v_alpha;

  vec_f32 v_x0 = vec_splat(x_0, 0);
  vec_f32 v_x1 = vec_splat(x_0, 1);

  vec_f32 *v_y = (vec_f32 *)y;
  BLASLONG n8 = n / 8;
  BLASLONG i = 0;
  vec_f32 vy0[2];

  for (; i < n8; i++) {
    vec_load_pair(vy0, &v_y[(i * 2) + 0]);

    vec_load_mult2(v_x0, &va0[i], zero, vy0);
    vec_load_mult2(v_x1, &va1[i], zero, vy0);

    vec_store_pair(&v_y[(i * 2) + 0], vy0);
  }

  n &= 7;
  if (n > 4) {
    BLASLONG n3 = n & 3;
    vec_loadN2_f32(vy0, &v_y[(i * 2) + 0], n3);

    vec_loadN_mult2(v_x0, &va0[i], n, zero, vy0);
    vec_loadN_mult2(v_x1, &va1[i], n, zero, vy0);

    vec_storeN2_f32(vy0, &v_y[(i * 2) + 0], n3);
  } else if (n) {
    vy0[0] = vec_loadN_f32(&v_y[(i * 2) + 0], n);

    vy0[0] += vec_loadNHi_mult(&va0[i], v_x0, n, zero);
    vy0[0] += vec_loadNHi_mult(&va1[i], v_x1, n, zero);

    vec_storeN_f32(vy0[0], &v_y[(i * 2) + 0], n);
  }
}

static void BF16GEMV_N_VSX_4(BLASLONG n, IFLOAT **ap, IFLOAT *xo, FLOAT *y, FLOAT alpha)
{
  IFLOAT *a0, *a1, *a2, *a3;
  vec_bf16 zero = { 0, 0, 0, 0, 0, 0, 0, 0 };
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
  vec_f32 x_0 = vec_loadNHi(x_bf, 4, zero);
  x_0 *= v_alpha;

  vec_f32 v_x0 = vec_splat(x_0, 0);
  vec_f32 v_x1 = vec_splat(x_0, 1);
  vec_f32 v_x2 = vec_splat(x_0, 2);
  vec_f32 v_x3 = vec_splat(x_0, 3);

  vec_f32 *v_y = (vec_f32 *)y;
  BLASLONG n8 = n / 8;
  BLASLONG i = 0;
  vec_f32 vy0[2];

  for (; i < n8; i++) {
    vec_load_pair(vy0, &v_y[(i * 2) + 0]);

    vec_load_mult2(v_x0, &va0[i], zero, vy0);
    vec_load_mult2(v_x1, &va1[i], zero, vy0);
    vec_load_mult2(v_x2, &va2[i], zero, vy0);
    vec_load_mult2(v_x3, &va3[i], zero, vy0);

    vec_store_pair(&v_y[(i * 2) + 0], vy0);
  }

  n &= 7;
  if (n > 4) {
    BLASLONG n3 = n & 3;
    vec_loadN2_f32(vy0, &v_y[(i * 2) + 0], n3);

    vec_loadN_mult2(v_x0, &va0[i], n, zero, vy0);
    vec_loadN_mult2(v_x1, &va1[i], n, zero, vy0);
    vec_loadN_mult2(v_x2, &va2[i], n, zero, vy0);
    vec_loadN_mult2(v_x3, &va3[i], n, zero, vy0);

    vec_storeN2_f32(vy0, &v_y[(i * 2) + 0], n3);
  } else if (n) {
    vy0[0] = vec_loadN_f32(&v_y[(i * 2) + 0], n);

    vy0[0] += vec_loadNHi_mult(&va0[i], v_x0, n, zero);
    vy0[0] += vec_loadNHi_mult(&va1[i], v_x1, n, zero);
    vy0[0] += vec_loadNHi_mult(&va2[i], v_x2, n, zero);
    vy0[0] += vec_loadNHi_mult(&va3[i], v_x3, n, zero);

    vec_storeN_f32(vy0[0], &v_y[(i * 2) + 0], n);
  }
}

#ifdef USE_BFGEMV_8_N_VSX
static void BF16GEMV_N_VSX_8(BLASLONG n, IFLOAT **ap, IFLOAT *xo, FLOAT *y, BLASLONG lda4, FLOAT alpha)
{
  IFLOAT *a0, *a1, *a2, *a3, *b0, *b1, *b2, *b3;
  vec_bf16 zero = { 0, 0, 0, 0, 0, 0, 0, 0 };
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
  vec_bf16 x_in = (vec_bf16)vec_load_vec(x_bf);
  vec_f32 x_0 = BF16_HI(x_in, zero);
  vec_f32 x_1 = BF16_LO(x_in, zero);
  x_0 *= v_alpha;
  x_1 *= v_alpha;

  vec_f32 v_x0 = vec_splat(x_0, 0);
  vec_f32 v_x1 = vec_splat(x_0, 1);
  vec_f32 v_x2 = vec_splat(x_0, 2);
  vec_f32 v_x3 = vec_splat(x_0, 3);
  vec_f32 v_x4 = vec_splat(x_1, 0);
  vec_f32 v_x5 = vec_splat(x_1, 1);
  vec_f32 v_x6 = vec_splat(x_1, 2);
  vec_f32 v_x7 = vec_splat(x_1, 3);

  vec_f32 *v_y = (vec_f32 *)y;
  BLASLONG n8 = n / 8;
  BLASLONG i = 0;
  vec_f32 vy0[2];

  for (; i < n8; i++) {
    vec_load_pair(vy0, &v_y[(i * 2) + 0]);

    vec_load_mult2(v_x0, &va0[i], zero, vy0);
    vec_load_mult2(v_x1, &va1[i], zero, vy0);
    vec_load_mult2(v_x2, &va2[i], zero, vy0);
    vec_load_mult2(v_x3, &va3[i], zero, vy0);
    vec_load_mult2(v_x4, &vb0[i], zero, vy0);
    vec_load_mult2(v_x5, &vb1[i], zero, vy0);
    vec_load_mult2(v_x6, &vb2[i], zero, vy0);
    vec_load_mult2(v_x7, &vb3[i], zero, vy0);

    vec_store_pair(&v_y[(i * 2) + 0], vy0);
  }

  n &= 7;
  if (n > 4) {
    BLASLONG n3 = n & 3;
    vec_loadN2_f32(vy0, &v_y[(i * 2) + 0], n3);

    vec_loadN_mult2(v_x0, &va0[i], n, zero, vy0);
    vec_loadN_mult2(v_x1, &va1[i], n, zero, vy0);
    vec_loadN_mult2(v_x2, &va2[i], n, zero, vy0);
    vec_loadN_mult2(v_x3, &va3[i], n, zero, vy0);
    vec_loadN_mult2(v_x4, &vb0[i], n, zero, vy0);
    vec_loadN_mult2(v_x5, &vb1[i], n, zero, vy0);
    vec_loadN_mult2(v_x6, &vb2[i], n, zero, vy0);
    vec_loadN_mult2(v_x7, &vb3[i], n, zero, vy0);

    vec_storeN2_f32(vy0, &v_y[(i * 2) + 0], n3);
  } else if (n) {
    vy0[0] = vec_loadN_f32(&v_y[(i * 2) + 0], n);

    vy0[0] += vec_loadNHi_mult(&va0[i], v_x0, n, zero);
    vy0[0] += vec_loadNHi_mult(&va1[i], v_x1, n, zero);
    vy0[0] += vec_loadNHi_mult(&va2[i], v_x2, n, zero);
    vy0[0] += vec_loadNHi_mult(&va3[i], v_x3, n, zero);
    vy0[0] += vec_loadNHi_mult(&vb0[i], v_x4, n, zero);
    vy0[0] += vec_loadNHi_mult(&vb1[i], v_x5, n, zero);
    vy0[0] += vec_loadNHi_mult(&vb2[i], v_x6, n, zero);
    vy0[0] += vec_loadNHi_mult(&vb3[i], v_x7, n, zero);

    vec_storeN_f32(vy0[0], &v_y[(i * 2) + 0], n);
  }
}
#endif

#include "sbgemv_n.c"
#endif
