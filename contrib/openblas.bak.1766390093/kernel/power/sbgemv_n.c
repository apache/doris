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

#ifndef SBGEMV_N_COMMON_C
#define SBGEMV_N_COMMON_C

#if (defined(_ARCH_PWR10) && (defined(USE_BFGEMV_8_N_MMA) || (!defined(USE_BFGEMV_N_MMA) && defined(USE_BFGEMV_8_N_VSX)))) || (!defined(_ARCH_PWR10) && defined(USE_BFGEMV_8_N_VSX))
#define USE_N_8
#endif

int CNAME(BLASLONG m, BLASLONG n, FLOAT alpha, IFLOAT *a, BLASLONG lda, IFLOAT *x, BLASLONG inc_x, FLOAT beta, FLOAT *y, BLASLONG inc_y)
{
  IFLOAT *x_ptr, *ap[4];
  IFLOAT xbuffer[8] __attribute__((aligned(16)));
  FLOAT *y_ptr, *ybuffer;
  FLOAT buffer[NBMAX] __attribute__((aligned(16)));

  if ((m < 1) || (n < 1)) return 0;

  ybuffer = buffer;
  y_ptr = y;

  BLASLONG lda4 = lda << 2;
#ifdef USE_N_8
  BLASLONG lda8 = lda << 3;
#endif
  BLASLONG NB = NBMAX;
  BLASLONG m2 = (m & (NBMAX - 1));

  while (NB == NBMAX) {
    m -= NB;
    if (m < 0) {
      if (m2 == 0) break;
      NB = m2;
    }

    if (inc_y != 1) {
      copy_y_beta(NB, y_ptr, ybuffer, inc_y, beta);
    } else {
      ybuffer = y_ptr;
      BF16GEMV_N_beta(NB, ybuffer, ybuffer, beta);
    }

    x_ptr = x;

    ap[0] = a;
    ap[1] = a + lda;
    ap[2] = ap[1] + lda;
    ap[3] = ap[2] + lda;

    if (inc_x == 1) {
#ifdef USE_N_8
      for (BLASLONG j = 0; j + 8 <= n; j += 8) {
        BF16GEMV_N_8(NB, ap, x_ptr, ybuffer, lda4, alpha);
        ap[0] += lda8;
        ap[1] += lda8;
        ap[2] += lda8;
        ap[3] += lda8;
        x_ptr += 8;
      }
      if (n & 4) {
#else
      for (BLASLONG j = 0; j + 4 <= n; j += 4) {
#endif
        BF16GEMV_N_4(NB, ap, x_ptr, ybuffer, alpha);
        ap[0] += lda4;
        ap[1] += lda4;
#ifndef USE_N_8
        ap[2] += lda4;
        ap[3] += lda4;
#endif
        x_ptr += 4;
      }
      if (n & 2) {
        BF16GEMV_N_2(NB, ap, x_ptr, ybuffer, alpha);
        ap[0] += (lda * 2);
        x_ptr += 2;
      }
      if (n & 1) {
        BF16GEMV_N_1(NB, ap, x_ptr, ybuffer, alpha);
      }
    } else {
#ifdef USE_N_8
      for (BLASLONG j = 0; j + 8 <= n; j += 8) {
        copy_x(8, x_ptr, xbuffer, inc_x);
        BF16GEMV_N_8(NB, ap, xbuffer, ybuffer, lda4, alpha);
        ap[0] += lda8;
        ap[1] += lda8;
        ap[2] += lda8;
        ap[3] += lda8;
        x_ptr += 8 * inc_x;
      }
      if (n & 4) {
#else
      for (BLASLONG j = 0; j + 4 <= n; j += 4) {
#endif
        copy_x(4, x_ptr, xbuffer, inc_x);
        BF16GEMV_N_4(NB, ap, xbuffer, ybuffer, alpha);
        ap[0] += lda4;
        ap[1] += lda4;
#ifndef USE_N_8
        ap[2] += lda4;
        ap[3] += lda4;
#endif
        x_ptr += 4 * inc_x;
      }
      if (n & 2) {
        copy_x(2, x_ptr, xbuffer, inc_x);
        BF16GEMV_N_2(NB, ap, xbuffer, ybuffer, alpha);
        ap[0] += (lda * 2);
        x_ptr += 2 * inc_x;
      }
      if (n & 1) {
        copy_x(1, x_ptr, xbuffer, inc_x);
        BF16GEMV_N_1(NB, ap, xbuffer, ybuffer, alpha);
      }
    }

    a += NB;
    if (inc_y != 1) {
      move_y(NB, ybuffer, y_ptr, inc_y);
    }
    y_ptr += (NB * inc_y);
  }

  return 0;
}
#endif
