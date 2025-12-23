/***************************************************************************
Copyright (c) 2025, The OpenBLAS Project
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
      derived from this software without specific prior written
      permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*****************************************************************************/

#include <arm_neon.h>
#include "common.h"

int CNAME(BLASLONG m, BLASLONG n, float alpha, bfloat16 *a, BLASLONG lda, bfloat16 *x, BLASLONG incx, float beta, float *y, BLASLONG incy)
{
  if (m < 1 || n < 1) return(0);
  BLASLONG i;
  BLASLONG ix,iy;
  BLASLONG j;
  bfloat16_t *a_ptr;
  bfloat16_t *x_ptr;
  float *y_ptr;
  float temp;

  iy = 0;
  a_ptr = (bfloat16_t*)(a);
  x_ptr = (bfloat16_t*)(x);

  if (incx == 1) {
    BLASLONG width = n / 4;

    bfloat16_t *a0_ptr = a_ptr + lda * width * 0;
    bfloat16_t *a1_ptr = a_ptr + lda * width * 1;
    bfloat16_t *a2_ptr = a_ptr + lda * width * 2;
    bfloat16_t *a3_ptr = a_ptr + lda * width * 3;

    float *y0_ptr = y + incy * width * 0;
    float *y1_ptr = y + incy * width * 1;
    float *y2_ptr = y + incy * width * 2;
    float *y3_ptr = y + incy * width * 3;

    for (j = 0; j < width; j++) {
      float32x4_t temp0_vec = vdupq_n_f32(0.0f);
      float32x4_t temp1_vec = vdupq_n_f32(0.0f);
      float32x4_t temp2_vec = vdupq_n_f32(0.0f);
      float32x4_t temp3_vec = vdupq_n_f32(0.0f);

      i = 0;
      while (i + 7 < m) {
        bfloat16x8_t x_vec = vld1q_bf16(x_ptr + i);

        bfloat16x8_t a0_vec = vld1q_bf16(a0_ptr + i);
        bfloat16x8_t a1_vec = vld1q_bf16(a1_ptr + i);
        bfloat16x8_t a2_vec = vld1q_bf16(a2_ptr + i);
        bfloat16x8_t a3_vec = vld1q_bf16(a3_ptr + i);

        temp0_vec = vbfdotq_f32(temp0_vec, a0_vec, x_vec);
        temp1_vec = vbfdotq_f32(temp1_vec, a1_vec, x_vec);
        temp2_vec = vbfdotq_f32(temp2_vec, a2_vec, x_vec);
        temp3_vec = vbfdotq_f32(temp3_vec, a3_vec, x_vec);

        i += 8;
      }
      if (i + 3 < m) {
        float32x2_t t0 = vdup_n_f32(0.0f);
        float32x2_t t1 = vdup_n_f32(0.0f);
        float32x2_t t2 = vdup_n_f32(0.0f);
        float32x2_t t3 = vdup_n_f32(0.0f);

        bfloat16x4_t x_vec = vld1_bf16(x_ptr + i);

        bfloat16x4_t a0_vec = vld1_bf16(a0_ptr + i);
        bfloat16x4_t a1_vec = vld1_bf16(a1_ptr + i);
        bfloat16x4_t a2_vec = vld1_bf16(a2_ptr + i);
        bfloat16x4_t a3_vec = vld1_bf16(a3_ptr + i);

        t0 = vbfdot_f32(t0, a0_vec, x_vec);
        t1 = vbfdot_f32(t1, a1_vec, x_vec);
        t2 = vbfdot_f32(t2, a2_vec, x_vec);
        t3 = vbfdot_f32(t3, a3_vec, x_vec);

        float32x2_t temp0_vec_low = vget_low_f32(temp0_vec);
        float32x2_t temp1_vec_low = vget_low_f32(temp1_vec);
        float32x2_t temp2_vec_low = vget_low_f32(temp2_vec);
        float32x2_t temp3_vec_low = vget_low_f32(temp3_vec);

        temp0_vec = vcombine_f32(vadd_f32(t0, temp0_vec_low), vget_high_f32(temp0_vec));
        temp1_vec = vcombine_f32(vadd_f32(t1, temp1_vec_low), vget_high_f32(temp1_vec));
        temp2_vec = vcombine_f32(vadd_f32(t2, temp2_vec_low), vget_high_f32(temp2_vec));
        temp3_vec = vcombine_f32(vadd_f32(t3, temp3_vec_low), vget_high_f32(temp3_vec));

        i += 4;
      }
      if (beta == 0.0f) {
        y0_ptr[iy] = alpha * vaddvq_f32(temp0_vec);
        y1_ptr[iy] = alpha * vaddvq_f32(temp1_vec);
        y2_ptr[iy] = alpha * vaddvq_f32(temp2_vec);
        y3_ptr[iy] = alpha * vaddvq_f32(temp3_vec);
      }
      else {
        y0_ptr[iy] = alpha * vaddvq_f32(temp0_vec) + beta * y0_ptr[iy];
        y1_ptr[iy] = alpha * vaddvq_f32(temp1_vec) + beta * y1_ptr[iy];
        y2_ptr[iy] = alpha * vaddvq_f32(temp2_vec) + beta * y2_ptr[iy];
        y3_ptr[iy] = alpha * vaddvq_f32(temp3_vec) + beta * y3_ptr[iy];
      }

      for (; i < m; ++i) {
        y0_ptr[iy] += alpha * vcvtah_f32_bf16(a0_ptr[i]) * vcvtah_f32_bf16(x_ptr[i]);
        y1_ptr[iy] += alpha * vcvtah_f32_bf16(a1_ptr[i]) * vcvtah_f32_bf16(x_ptr[i]);
        y2_ptr[iy] += alpha * vcvtah_f32_bf16(a2_ptr[i]) * vcvtah_f32_bf16(x_ptr[i]);
        y3_ptr[iy] += alpha * vcvtah_f32_bf16(a3_ptr[i]) * vcvtah_f32_bf16(x_ptr[i]);
      }

      iy += incy;

      a0_ptr += lda;
      a1_ptr += lda;
      a2_ptr += lda;
      a3_ptr += lda;
    }

    a_ptr = a3_ptr;
    y_ptr = y3_ptr;
    for (j = width * 4; j < n; j++) {
      float32x4_t temp0_vec = vdupq_n_f32(0.0f);
      i = 0;
      while (i + 7 < m) {
        bfloat16x8_t x_vec = vld1q_bf16(x_ptr + i);
        bfloat16x8_t a0_vec = vld1q_bf16(a_ptr + i);
        temp0_vec = vbfdotq_f32(temp0_vec, a0_vec, x_vec);

        i += 8;
      }
      if (i + 3 < m) {
        float32x2_t t0 = vdup_n_f32(0.0f);
        bfloat16x4_t x_vec = vld1_bf16(x_ptr + i);
        bfloat16x4_t a0_vec = vld1_bf16(a_ptr + i);

        t0 = vbfdot_f32(t0, a0_vec, x_vec);
        float32x2_t temp0_vec_low = vget_low_f32(temp0_vec);
        temp0_vec = vcombine_f32(vadd_f32(t0, temp0_vec_low), vget_high_f32(temp0_vec));

        i += 4;
      }
      if (beta == 0.0f) {
        y_ptr[iy] = alpha * vaddvq_f32(temp0_vec);
      }
      else {
        y_ptr[iy] = alpha * vaddvq_f32(temp0_vec) + beta * y_ptr[iy];
      }

      for (; i < m; ++i) {
        y_ptr[iy] += alpha * vcvtah_f32_bf16(a_ptr[i]) * vcvtah_f32_bf16(x_ptr[i]);
      }

      iy += incy;

      a_ptr += lda;
    }
    return(0);
  }

  for (j = 0; j < n; j++) {
    temp = 0.0;
    ix = 0;
    for (i = 0; i < m; i++) {
      temp += vcvtah_f32_bf16(a_ptr[i]) * vcvtah_f32_bf16(x_ptr[ix]);
      ix += incx;
    }
    if (beta == 0.0f) {
      y[iy] = alpha * temp;
    }
    else {
      y[iy] = alpha * temp + beta * y[iy];
    }
    iy += incy;
    a_ptr += lda;
  }
  return (0);
}
