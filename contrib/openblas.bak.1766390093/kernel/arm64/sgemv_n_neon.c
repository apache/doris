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

int CNAME(BLASLONG m, BLASLONG n, BLASLONG dummy1, FLOAT alpha, FLOAT *a, BLASLONG lda, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *buffer)
{
  BLASLONG i;
  BLASLONG ix,iy;
  BLASLONG j;
  FLOAT *a_ptr;
  FLOAT temp;

  ix = 0;
  a_ptr = a;

  if (inc_x == 1 && inc_y == 1) {
    FLOAT *a0_ptr = a + lda * 0;
    FLOAT *a1_ptr = a + lda * 1;
    FLOAT *a2_ptr = a + lda * 2;
    FLOAT *a3_ptr = a + lda * 3;
    FLOAT *a4_ptr = a + lda * 4;
    FLOAT *a5_ptr = a + lda * 5;
    FLOAT *a6_ptr = a + lda * 6;
    FLOAT *a7_ptr = a + lda * 7;

    j = 0;
    while (j + 3 < n) {
      float32x4_t x0_vec = vld1q_f32(x + j);
      x0_vec = vmulq_n_f32(x0_vec, alpha);
      i = 0;
      while (i + 7 < m) {
        float32x4_t a00_vec = vld1q_f32(a0_ptr + i);
        float32x4_t a01_vec = vld1q_f32(a0_ptr + i + 4);
        float32x4_t a10_vec = vld1q_f32(a1_ptr + i);
        float32x4_t a11_vec = vld1q_f32(a1_ptr + i + 4);
        float32x4_t a20_vec = vld1q_f32(a2_ptr + i);
        float32x4_t a21_vec = vld1q_f32(a2_ptr + i + 4);
        float32x4_t a30_vec = vld1q_f32(a3_ptr + i);
        float32x4_t a31_vec = vld1q_f32(a3_ptr + i + 4);

        float32x4_t y0_vec = vld1q_f32(y + i);
        float32x4_t y1_vec = vld1q_f32(y + i + 4);
        y0_vec = vmlaq_laneq_f32(y0_vec, a00_vec, x0_vec, 0);
        y0_vec = vmlaq_laneq_f32(y0_vec, a10_vec, x0_vec, 1);
        y0_vec = vmlaq_laneq_f32(y0_vec, a20_vec, x0_vec, 2);
        y0_vec = vmlaq_laneq_f32(y0_vec, a30_vec, x0_vec, 3);
        y1_vec = vmlaq_laneq_f32(y1_vec, a01_vec, x0_vec, 0);
        y1_vec = vmlaq_laneq_f32(y1_vec, a11_vec, x0_vec, 1);
        y1_vec = vmlaq_laneq_f32(y1_vec, a21_vec, x0_vec, 2);
        y1_vec = vmlaq_laneq_f32(y1_vec, a31_vec, x0_vec, 3);

        vst1q_f32(y + i, y0_vec);
        vst1q_f32(y + i + 4, y1_vec);

        i += 8;
      }
      while (i + 3 < m) {
        float32x4_t a0_vec = vld1q_f32(a0_ptr + i);
        float32x4_t a1_vec = vld1q_f32(a1_ptr + i);
        float32x4_t a2_vec = vld1q_f32(a2_ptr + i);
        float32x4_t a3_vec = vld1q_f32(a3_ptr + i);

        float32x4_t y_vec = vld1q_f32(y + i);
        y_vec = vmlaq_laneq_f32(y_vec, a0_vec, x0_vec, 0);
        y_vec = vmlaq_laneq_f32(y_vec, a1_vec, x0_vec, 1);
        y_vec = vmlaq_laneq_f32(y_vec, a2_vec, x0_vec, 2);
        y_vec = vmlaq_laneq_f32(y_vec, a3_vec, x0_vec, 3);

        vst1q_f32(y + i, y_vec);

        i += 4;
      }
      while (i + 1 < m) {
        float32x2_t a0_vec = vld1_f32(a0_ptr + i);
        float32x2_t a1_vec = vld1_f32(a1_ptr + i);
        float32x2_t a2_vec = vld1_f32(a2_ptr + i);
        float32x2_t a3_vec = vld1_f32(a3_ptr + i);

        float32x2_t y_vec = vld1_f32(y + i);
        y_vec = vmla_laneq_f32(y_vec, a0_vec, x0_vec, 0);
        y_vec = vmla_laneq_f32(y_vec, a1_vec, x0_vec, 1);
        y_vec = vmla_laneq_f32(y_vec, a2_vec, x0_vec, 2);
        y_vec = vmla_laneq_f32(y_vec, a3_vec, x0_vec, 3);

        vst1_f32(y + i, y_vec);

        i += 2;
      }
      while (i < m) {
        y[i] += a0_ptr[i] * x0_vec[0];
        y[i] += a1_ptr[i] * x0_vec[1];
        y[i] += a2_ptr[i] * x0_vec[2];
        y[i] += a3_ptr[i] * x0_vec[3];

        i++;
      }

      a0_ptr += lda * 4;
      a1_ptr += lda * 4;
      a2_ptr += lda * 4;
      a3_ptr += lda * 4;

      j += 4;
    }
    while (j + 1 < n) {
      float32x2_t x0_vec = vld1_f32(x + j);
      x0_vec = vmul_n_f32(x0_vec, alpha);
      i = 0;
      while (i + 7 < m) {
        float32x4_t a00_vec = vld1q_f32(a0_ptr + i);
        float32x4_t a01_vec = vld1q_f32(a0_ptr + i + 4);
        float32x4_t a10_vec = vld1q_f32(a1_ptr + i);
        float32x4_t a11_vec = vld1q_f32(a1_ptr + i + 4);

        float32x4_t y0_vec = vld1q_f32(y + i);
        float32x4_t y1_vec = vld1q_f32(y + i + 4);
        y0_vec = vmlaq_lane_f32(y0_vec, a00_vec, x0_vec, 0);
        y0_vec = vmlaq_lane_f32(y0_vec, a10_vec, x0_vec, 1);
        y1_vec = vmlaq_lane_f32(y1_vec, a01_vec, x0_vec, 0);
        y1_vec = vmlaq_lane_f32(y1_vec, a11_vec, x0_vec, 1);

        vst1q_f32(y + i, y0_vec);
        vst1q_f32(y + i + 4, y1_vec);

        i += 8;
      }
      while (i + 3 < m) {
        float32x4_t a0_vec = vld1q_f32(a0_ptr + i);
        float32x4_t a1_vec = vld1q_f32(a1_ptr + i);

        float32x4_t y_vec = vld1q_f32(y + i);
        y_vec = vmlaq_lane_f32(y_vec, a0_vec, x0_vec, 0);
        y_vec = vmlaq_lane_f32(y_vec, a1_vec, x0_vec, 1);

        vst1q_f32(y + i, y_vec);

        i += 4;
      }
      while (i + 1 < m) {
        float32x2_t a0_vec = vld1_f32(a0_ptr + i);
        float32x2_t a1_vec = vld1_f32(a1_ptr + i);

        float32x2_t y_vec = vld1_f32(y + i);
        y_vec = vmla_lane_f32(y_vec, a0_vec, x0_vec, 0);
        y_vec = vmla_lane_f32(y_vec, a1_vec, x0_vec, 1);

        vst1_f32(y + i, y_vec);

        i += 2;
      }
      while (i < m) {
        y[i] += a0_ptr[i] * x0_vec[0];
        y[i] += a1_ptr[i] * x0_vec[1];

        i++;
      }

      a0_ptr += lda * 2;
      a1_ptr += lda * 2;

      j += 2;
    }
    while (j < n) {
      i = 0;
      temp = alpha * x[j];
      while (i < m) {
        y[i] += a0_ptr[i] * temp;
        i++;
      }

      a0_ptr += lda;
      j++;
    }
    return (0);
  }

  for (j = 0; j < n; j++) {
    temp = alpha * x[ix];
    iy = 0;
    for (i = 0; i < m; i++) {
      y[iy] += temp * a_ptr[i];
      iy += inc_y;
    }
    a_ptr += lda;
    ix += inc_x;
  }
  return (0);
}
