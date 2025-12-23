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

#ifndef SBGEMV_T_COMMON_C
#define SBGEMV_T_COMMON_C

#if (defined(_ARCH_PWR10) && (defined(USE_BFGEMV_8_T_MMA) || (!defined(USE_BFGEMV_N_MMA) && defined(USE_BFGEMV_8_T_VSX)))) || (!defined(_ARCH_PWR10) && defined(USE_BFGEMV_8_T_VSX))
#define USE_T_8
#endif

int CNAME(BLASLONG m, BLASLONG n, FLOAT alpha, IFLOAT *a, BLASLONG lda, IFLOAT *x, BLASLONG inc_x, FLOAT beta, FLOAT *y, BLASLONG inc_y)
{
  IFLOAT *xbuffer, *a_ptr;
  IFLOAT buffer[NBMAX] __attribute__((aligned(16)));
  FLOAT ybuffer[8] __attribute__((aligned(16)));
  FLOAT *y_ptr;

  if ((m < 1) || (n < 1)) return 0;

  if (inc_y == 1) {
    BF16GEMV_N_beta(n, y, y, beta);
  }

  xbuffer = buffer;

  BLASLONG lda4 = lda << 2;
#ifdef USE_T_8
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

    a_ptr = a;
    a += NB;
    y_ptr = y;

    if (inc_x != 1) {
      copy_x(NB, x, xbuffer, inc_x);
      x += NB * inc_x;
    } else {
      xbuffer = x;
      x += NB;
    }

    if (inc_y == 1) {
#ifdef USE_T_8
      for (BLASLONG j = 0; j + 8 <= n; j += 8) {
        BF16GEMV_T_8(NB, lda, a_ptr, xbuffer, y_ptr, alpha);
        y_ptr += 8;
        a_ptr += lda8;
      }
      if (n & 4) {
#else
      for (BLASLONG j = 0; j + 4 <= n; j += 4) {
#endif
        BF16GEMV_T_4(NB, lda, a_ptr, xbuffer, y_ptr, alpha);
        y_ptr += 4;
        a_ptr += lda4;
      }
      if (n & 2) {
        BF16GEMV_T_2(NB, lda, a_ptr, xbuffer, y_ptr, alpha);
        y_ptr += 2;
        a_ptr += (lda * 2);
      }
      if (n & 1) {
        BF16GEMV_T_1(NB, lda, a_ptr, xbuffer, y_ptr, alpha);
      }
    } else {
#ifdef USE_T_8
      for (BLASLONG j = 0; j + 8 <= n; j += 8) {
        memset(ybuffer, 0, sizeof(FLOAT) * 8);
        BF16GEMV_T_8(NB, lda, a_ptr, xbuffer, ybuffer, alpha);
        copy_y(8, ybuffer, y_ptr, inc_y, beta);
        y_ptr += 8 * inc_y;
        a_ptr += lda8;
      }
      if (n & 4) {
#else
      for (BLASLONG j = 0; j + 4 <= n; j += 4) {
#endif
        memset(ybuffer, 0, sizeof(FLOAT) * 4);
        BF16GEMV_T_4(NB, lda, a_ptr, xbuffer, ybuffer, alpha);
        copy_y(4, ybuffer, y_ptr, inc_y, beta);
        y_ptr += 4 * inc_y;
        a_ptr += lda4;
      }
      if (n & 2) {
        memset(ybuffer, 0, sizeof(FLOAT) * 4);
        BF16GEMV_T_2(NB, lda, a_ptr, xbuffer, ybuffer, alpha);
        copy_y(2, ybuffer, y_ptr, inc_y, beta);
        y_ptr += 2 * inc_y;
        a_ptr += (lda * 2);
      }
      if (n & 1) {
        memset(ybuffer, 0, sizeof(FLOAT) * 4);
        BF16GEMV_T_1(NB, lda, a_ptr, xbuffer, ybuffer, alpha);
        copy_y(1, ybuffer, y_ptr, inc_y, beta);
      }
      beta = (FLOAT)1;
    }
  }

  return 0;
}
#endif

