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
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*****************************************************************************/

#include "common.h"
#include <arm_neon.h>

static void beta_op(float *x, BLASLONG n, FLOAT beta) {
  if (beta == 0) {
    memset(x, 0, n * sizeof(float));
    return;
  }

  float32x4_t y0, y1, y2, y3;

  for (BLASLONG i = 0; i < n / 16; i++) { 
    y0 = vld1q_f32(x);
    y1 = vld1q_f32(x + 4);
    y2 = vld1q_f32(x + 8);
    y3 = vld1q_f32(x + 12);

    y0 = vmulq_n_f32(y0, beta);
    y1 = vmulq_n_f32(y1, beta);
    y2 = vmulq_n_f32(y2, beta);
    y3 = vmulq_n_f32(y3, beta);

    vst1q_f32(x, y0);
    vst1q_f32(x + 4, y1);
    vst1q_f32(x + 8, y2);
    vst1q_f32(x + 12, y3);

    x += 16;
  }

  if (n & 15) {
    BLASLONG rest_n = n & 15;
    for (BLASLONG i = 0; i < (rest_n) / 4; i++) {
      y0 = vld1q_f32(x);
      y0 = vmulq_n_f32(y0, beta);
      vst1q_f32(x, y0);
      x += 4;
    }

    for (BLASLONG i = 0; i < (rest_n & 3); i ++) {
      x[i] *= beta;
    }
  }
  return;
}

int CNAME(BLASLONG m, BLASLONG n, FLOAT alpha, bfloat16 *a, BLASLONG lda,
          bfloat16 *x, BLASLONG incx, float beta, float *y, BLASLONG incy) {
  BLASLONG i, j;
  bfloat16_t *a_ptr, *x_ptr;
  FLOAT *y_ptr;

  bfloat16x8_t a0, a1, a2, a3, a4, a5, a6, a7;
  bfloat16x8_t t0, t1, t2, t3, t4, t5, t6, t7;

  bfloat16x8_t x_vec;
  bfloat16x4_t x_vecx4;

  float32x4_t y1_vec, y2_vec;
  float32x4_t fp32_low, fp32_high;

  float x0, x1, x2, x3, x4, x5, x6, x7;
  bfloat16_t *a_ptr0, *a_ptr1, *a_ptr2, *a_ptr3, *a_ptr4, *a_ptr5, *a_ptr6,
      *a_ptr7;

  a_ptr = (bfloat16_t *)a;
  x_ptr = (bfloat16_t *)x;

  BLASLONG rest_m = m & 3;

  bfloat16x4_t bf16_zero = vreinterpret_bf16_u16(vdup_n_u16(0)); 
  bfloat16x8_t bf16_zero_q = vreinterpretq_bf16_u16(vdupq_n_u16(0));

  if (incx == 1 && incy == 1) {
    if (beta != 1) {
      beta_op(y, m, beta);
    }

    for (i = 0; i < n / 8; i++) {
      a_ptr0 = a_ptr;
      a_ptr1 = a_ptr0 + lda;
      a_ptr2 = a_ptr1 + lda;
      a_ptr3 = a_ptr2 + lda;
      a_ptr4 = a_ptr3 + lda;
      a_ptr5 = a_ptr4 + lda;
      a_ptr6 = a_ptr5 + lda;
      a_ptr7 = a_ptr6 + lda;

      a_ptr += 8 * lda;

      y_ptr = y;

      x_vec = vld1q_bf16(x_ptr);

      if (alpha != 1) {
        fp32_low = vreinterpretq_f32_u16(
            vzip1q_u16(vreinterpretq_u16_bf16(bf16_zero_q),
                       vreinterpretq_u16_bf16(x_vec)));
        fp32_high = vreinterpretq_f32_u16(
            vzip2q_u16(vreinterpretq_u16_bf16(bf16_zero_q),
                       vreinterpretq_u16_bf16(x_vec)));

        fp32_low = vmulq_n_f32(fp32_low, alpha);
        fp32_high = vmulq_n_f32(fp32_high, alpha);

        x_vec =
            vcombine_bf16(vcvt_bf16_f32(fp32_low), vcvt_bf16_f32(fp32_high));
      }

      for (j = 0; j < m / 8; j++) {
        a0 = vld1q_bf16(a_ptr0);
        a1 = vld1q_bf16(a_ptr1);
        a2 = vld1q_bf16(a_ptr2);
        a3 = vld1q_bf16(a_ptr3);
        a4 = vld1q_bf16(a_ptr4);
        a5 = vld1q_bf16(a_ptr5);
        a6 = vld1q_bf16(a_ptr6);
        a7 = vld1q_bf16(a_ptr7);

        y1_vec = vld1q_f32(y_ptr);
        y2_vec = vld1q_f32(y_ptr + 4);

        t0 = vreinterpretq_bf16_u16(
            vzip1q_u16(vreinterpretq_u16_bf16(a0), vreinterpretq_u16_bf16(a1)));
        t1 = vreinterpretq_bf16_u16(
            vzip1q_u16(vreinterpretq_u16_bf16(a2), vreinterpretq_u16_bf16(a3)));
        t2 = vreinterpretq_bf16_u16(
            vzip1q_u16(vreinterpretq_u16_bf16(a4), vreinterpretq_u16_bf16(a5)));
        t3 = vreinterpretq_bf16_u16(
            vzip1q_u16(vreinterpretq_u16_bf16(a6), vreinterpretq_u16_bf16(a7)));

        t4 = vreinterpretq_bf16_u16(
            vzip2q_u16(vreinterpretq_u16_bf16(a0), vreinterpretq_u16_bf16(a1)));
        t5 = vreinterpretq_bf16_u16(
            vzip2q_u16(vreinterpretq_u16_bf16(a2), vreinterpretq_u16_bf16(a3)));
        t6 = vreinterpretq_bf16_u16(
            vzip2q_u16(vreinterpretq_u16_bf16(a4), vreinterpretq_u16_bf16(a5)));
        t7 = vreinterpretq_bf16_u16(
            vzip2q_u16(vreinterpretq_u16_bf16(a6), vreinterpretq_u16_bf16(a7)));

        y1_vec = vbfmlalbq_laneq_f32(y1_vec, t0, x_vec, 0);
        y1_vec = vbfmlaltq_laneq_f32(y1_vec, t0, x_vec, 1);
        y1_vec = vbfmlalbq_laneq_f32(y1_vec, t1, x_vec, 2);
        y1_vec = vbfmlaltq_laneq_f32(y1_vec, t1, x_vec, 3);
        y1_vec = vbfmlalbq_laneq_f32(y1_vec, t2, x_vec, 4);
        y1_vec = vbfmlaltq_laneq_f32(y1_vec, t2, x_vec, 5);
        y1_vec = vbfmlalbq_laneq_f32(y1_vec, t3, x_vec, 6);
        y1_vec = vbfmlaltq_laneq_f32(y1_vec, t3, x_vec, 7);

        y2_vec = vbfmlalbq_laneq_f32(y2_vec, t4, x_vec, 0);
        y2_vec = vbfmlaltq_laneq_f32(y2_vec, t4, x_vec, 1);
        y2_vec = vbfmlalbq_laneq_f32(y2_vec, t5, x_vec, 2);
        y2_vec = vbfmlaltq_laneq_f32(y2_vec, t5, x_vec, 3);
        y2_vec = vbfmlalbq_laneq_f32(y2_vec, t6, x_vec, 4);
        y2_vec = vbfmlaltq_laneq_f32(y2_vec, t6, x_vec, 5);
        y2_vec = vbfmlalbq_laneq_f32(y2_vec, t7, x_vec, 6);
        y2_vec = vbfmlaltq_laneq_f32(y2_vec, t7, x_vec, 7);

        vst1q_f32(y_ptr, y1_vec);
        vst1q_f32(y_ptr + 4, y2_vec);

        a_ptr0 += 8;
        a_ptr1 += 8;
        a_ptr2 += 8;
        a_ptr3 += 8;
        a_ptr4 += 8;
        a_ptr5 += 8;
        a_ptr6 += 8;
        a_ptr7 += 8;

        y_ptr += 8;
      }

      if (m & 4) {
        bfloat16x4_t a0x4 = vld1_bf16(a_ptr0);
        bfloat16x4_t a1x4 = vld1_bf16(a_ptr1);
        bfloat16x4_t a2x4 = vld1_bf16(a_ptr2);
        bfloat16x4_t a3x4 = vld1_bf16(a_ptr3);
        bfloat16x4_t a4x4 = vld1_bf16(a_ptr4);
        bfloat16x4_t a5x4 = vld1_bf16(a_ptr5);
        bfloat16x4_t a6x4 = vld1_bf16(a_ptr6);
        bfloat16x4_t a7x4 = vld1_bf16(a_ptr7);

        y1_vec = vld1q_f32(y_ptr);

        a0 = vcombine_bf16(a0x4, bf16_zero);
        a1 = vcombine_bf16(a1x4, bf16_zero);
        a2 = vcombine_bf16(a2x4, bf16_zero);
        a3 = vcombine_bf16(a3x4, bf16_zero);
        a4 = vcombine_bf16(a4x4, bf16_zero);
        a5 = vcombine_bf16(a5x4, bf16_zero);
        a6 = vcombine_bf16(a6x4, bf16_zero);
        a7 = vcombine_bf16(a7x4, bf16_zero);

        t0 = vreinterpretq_bf16_u16(
            vzip1q_u16(vreinterpretq_u16_bf16(a0), vreinterpretq_u16_bf16(a1)));
        t1 = vreinterpretq_bf16_u16(
            vzip1q_u16(vreinterpretq_u16_bf16(a2), vreinterpretq_u16_bf16(a3)));
        t2 = vreinterpretq_bf16_u16(
            vzip1q_u16(vreinterpretq_u16_bf16(a4), vreinterpretq_u16_bf16(a5)));
        t3 = vreinterpretq_bf16_u16(
            vzip1q_u16(vreinterpretq_u16_bf16(a6), vreinterpretq_u16_bf16(a7)));

        y1_vec = vbfmlalbq_laneq_f32(y1_vec, t0, x_vec, 0);
        y1_vec = vbfmlaltq_laneq_f32(y1_vec, t0, x_vec, 1);
        y1_vec = vbfmlalbq_laneq_f32(y1_vec, t1, x_vec, 2);
        y1_vec = vbfmlaltq_laneq_f32(y1_vec, t1, x_vec, 3);
        y1_vec = vbfmlalbq_laneq_f32(y1_vec, t2, x_vec, 4);
        y1_vec = vbfmlaltq_laneq_f32(y1_vec, t2, x_vec, 5);
        y1_vec = vbfmlalbq_laneq_f32(y1_vec, t3, x_vec, 6);
        y1_vec = vbfmlaltq_laneq_f32(y1_vec, t3, x_vec, 7);

        vst1q_f32(y_ptr, y1_vec);

        a_ptr0 += 4;
        a_ptr1 += 4;
        a_ptr2 += 4;
        a_ptr3 += 4;
        a_ptr4 += 4;
        a_ptr5 += 4;
        a_ptr6 += 4;
        a_ptr7 += 4;

        y_ptr += 4;
      }

      if (rest_m) {
        x0 = alpha * vcvtah_f32_bf16(x_ptr[0]);
        x1 = alpha * vcvtah_f32_bf16(x_ptr[1]);
        x2 = alpha * vcvtah_f32_bf16(x_ptr[2]);
        x3 = alpha * vcvtah_f32_bf16(x_ptr[3]);
        x4 = alpha * vcvtah_f32_bf16(x_ptr[4]);
        x5 = alpha * vcvtah_f32_bf16(x_ptr[5]);
        x6 = alpha * vcvtah_f32_bf16(x_ptr[6]);
        x7 = alpha * vcvtah_f32_bf16(x_ptr[7]);

        for (BLASLONG j = 0; j < rest_m; j++) {
          y_ptr[j] += x0 * vcvtah_f32_bf16(a_ptr0[j]);
          y_ptr[j] += x1 * vcvtah_f32_bf16(a_ptr1[j]);
          y_ptr[j] += x2 * vcvtah_f32_bf16(a_ptr2[j]);
          y_ptr[j] += x3 * vcvtah_f32_bf16(a_ptr3[j]);
          y_ptr[j] += x4 * vcvtah_f32_bf16(a_ptr4[j]);
          y_ptr[j] += x5 * vcvtah_f32_bf16(a_ptr5[j]);
          y_ptr[j] += x6 * vcvtah_f32_bf16(a_ptr6[j]);
          y_ptr[j] += x7 * vcvtah_f32_bf16(a_ptr7[j]);
        }
      }

      x_ptr += 8;
    }

    if (n & 4) {
      a_ptr0 = a_ptr;
      a_ptr1 = a_ptr0 + lda;
      a_ptr2 = a_ptr1 + lda;
      a_ptr3 = a_ptr2 + lda;

      a_ptr += 4 * lda;

      x_vecx4 = vld1_bf16(x_ptr);
      if (alpha != 1) {
        fp32_low = vcvt_f32_bf16(x_vecx4);
        fp32_low = vmulq_n_f32(fp32_low, alpha);
        x_vecx4 = vcvt_bf16_f32(fp32_low);
      }

      y_ptr = y;
      for (j = 0; j < m / 8; j++) {
        a0 = vld1q_bf16(a_ptr0);
        a1 = vld1q_bf16(a_ptr1);
        a2 = vld1q_bf16(a_ptr2);
        a3 = vld1q_bf16(a_ptr3);

        y1_vec = vld1q_f32(y_ptr);
        y2_vec = vld1q_f32(y_ptr + 4);

        t0 = vreinterpretq_bf16_u16(
            vzip1q_u16(vreinterpretq_u16_bf16(a0), vreinterpretq_u16_bf16(a1)));
        t1 = vreinterpretq_bf16_u16(
            vzip1q_u16(vreinterpretq_u16_bf16(a2), vreinterpretq_u16_bf16(a3)));
        t4 = vreinterpretq_bf16_u16(
            vzip2q_u16(vreinterpretq_u16_bf16(a0), vreinterpretq_u16_bf16(a1)));
        t5 = vreinterpretq_bf16_u16(
            vzip2q_u16(vreinterpretq_u16_bf16(a2), vreinterpretq_u16_bf16(a3)));

        y1_vec = vbfmlalbq_lane_f32(y1_vec, t0, x_vecx4, 0);
        y1_vec = vbfmlaltq_lane_f32(y1_vec, t0, x_vecx4, 1);
        y1_vec = vbfmlalbq_lane_f32(y1_vec, t1, x_vecx4, 2);
        y1_vec = vbfmlaltq_lane_f32(y1_vec, t1, x_vecx4, 3);

        y2_vec = vbfmlalbq_lane_f32(y2_vec, t4, x_vecx4, 0);
        y2_vec = vbfmlaltq_lane_f32(y2_vec, t4, x_vecx4, 1);
        y2_vec = vbfmlalbq_lane_f32(y2_vec, t5, x_vecx4, 2);
        y2_vec = vbfmlaltq_lane_f32(y2_vec, t5, x_vecx4, 3);

        vst1q_f32(y_ptr, y1_vec);
        vst1q_f32(y_ptr + 4, y2_vec);

        a_ptr0 += 8;
        a_ptr1 += 8;
        a_ptr2 += 8;
        a_ptr3 += 8;

        y_ptr += 8;
      }

      if (m & 4) {
        bfloat16x4_t a0x4 = vld1_bf16(a_ptr0);
        bfloat16x4_t a1x4 = vld1_bf16(a_ptr1);
        bfloat16x4_t a2x4 = vld1_bf16(a_ptr2);
        bfloat16x4_t a3x4 = vld1_bf16(a_ptr3);

        y1_vec = vld1q_f32(y_ptr);

        a0 = vcombine_bf16(a0x4, a2x4);
        a1 = vcombine_bf16(a1x4, a3x4);

        t0 = vreinterpretq_bf16_u16(vzip1q_u16(vreinterpretq_u16_bf16(a0), vreinterpretq_u16_bf16(a1)));
        t1 = vreinterpretq_bf16_u16(vzip2q_u16(vreinterpretq_u16_bf16(a0), vreinterpretq_u16_bf16(a1)));

        y1_vec = vbfmlalbq_lane_f32(y1_vec, t0, x_vecx4, 0);
        y1_vec = vbfmlaltq_lane_f32(y1_vec, t0, x_vecx4, 1);
        y1_vec = vbfmlalbq_lane_f32(y1_vec, t1, x_vecx4, 2);
        y1_vec = vbfmlaltq_lane_f32(y1_vec, t1, x_vecx4, 3);

        vst1q_f32(y_ptr, y1_vec);

        a_ptr0 += 4;
        a_ptr1 += 4;
        a_ptr2 += 4;
        a_ptr3 += 4;

        y_ptr += 4;
      }

      if (rest_m) {
        fp32_low = vcvt_f32_bf16(x_vecx4);

        x0 = vgetq_lane_f32(fp32_low, 0);
        x1 = vgetq_lane_f32(fp32_low, 1);
        x2 = vgetq_lane_f32(fp32_low, 2);
        x3 = vgetq_lane_f32(fp32_low, 3);

        for (BLASLONG j = 0; j < rest_m; j++) {
          y_ptr[j] += x0 * vcvtah_f32_bf16(a_ptr0[j]);
          y_ptr[j] += x1 * vcvtah_f32_bf16(a_ptr1[j]);
          y_ptr[j] += x2 * vcvtah_f32_bf16(a_ptr2[j]);
          y_ptr[j] += x3 * vcvtah_f32_bf16(a_ptr3[j]);
        }
      }

      x_ptr += 4;
    }

    if (n & 2) {
      a_ptr0 = a_ptr;
      a_ptr1 = a_ptr0 + lda;

      a_ptr += 2 * lda;

      x_vecx4 = vreinterpret_bf16_u16(vzip1_u16(
        vreinterpret_u16_bf16(vdup_n_bf16(x_ptr[0])),
        vreinterpret_u16_bf16(vdup_n_bf16(x_ptr[1]))
      ));

      if (alpha != 1) {
        fp32_low = vcvt_f32_bf16(x_vecx4);
        fp32_low = vmulq_n_f32(fp32_low, alpha);
        x_vecx4 = vcvt_bf16_f32(fp32_low);
      }

      y_ptr = y;
      for (j = 0; j < m / 8; j++) {
        a0 = vld1q_bf16(a_ptr0);
        a1 = vld1q_bf16(a_ptr1);

        y1_vec = vld1q_f32(y_ptr);
        y2_vec = vld1q_f32(y_ptr + 4);

        t0 = vreinterpretq_bf16_u16(
            vzip1q_u16(vreinterpretq_u16_bf16(a0), vreinterpretq_u16_bf16(a1)));
        t1 = vreinterpretq_bf16_u16(
            vzip2q_u16(vreinterpretq_u16_bf16(a0), vreinterpretq_u16_bf16(a1)));

        y1_vec = vbfmlalbq_lane_f32(y1_vec, t0, x_vecx4, 0);
        y1_vec = vbfmlaltq_lane_f32(y1_vec, t0, x_vecx4, 1);

        y2_vec = vbfmlalbq_lane_f32(y2_vec, t1, x_vecx4, 0);
        y2_vec = vbfmlaltq_lane_f32(y2_vec, t1, x_vecx4, 1);

        vst1q_f32(y_ptr, y1_vec);
        vst1q_f32(y_ptr + 4, y2_vec);

        a_ptr0 += 8;
        a_ptr1 += 8;

        y_ptr += 8;
      }

      if (m & 4) {
        bfloat16x4_t a0x4 = vld1_bf16(a_ptr0);
        bfloat16x4_t a1x4 = vld1_bf16(a_ptr1);

        y1_vec = vld1q_f32(y_ptr);

        a0 = vcombine_bf16(a0x4, bf16_zero);
        a1 = vcombine_bf16(a1x4, bf16_zero);

        t0 = vreinterpretq_bf16_u16(vzip1q_u16(vreinterpretq_u16_bf16(a0), vreinterpretq_u16_bf16(a1)));

        y1_vec = vbfmlalbq_lane_f32(y1_vec, t0, x_vecx4, 0);
        y1_vec = vbfmlaltq_lane_f32(y1_vec, t0, x_vecx4, 1);

        vst1q_f32(y_ptr, y1_vec);

        a_ptr0 += 4;
        a_ptr1 += 4;

        y_ptr += 4;
      }

      if (m & 2) {
        fp32_low = vcvt_f32_bf16(x_vecx4);
        x0 = vgetq_lane_f32(fp32_low, 0);
        x1 = vgetq_lane_f32(fp32_low, 1);


        y_ptr[0] += x0 * vcvtah_f32_bf16(a_ptr0[0]);
        y_ptr[0] += x1 * vcvtah_f32_bf16(a_ptr1[0]);
        y_ptr[1] += x0 * vcvtah_f32_bf16(a_ptr0[1]);
        y_ptr[1] += x1 * vcvtah_f32_bf16(a_ptr1[1]);

        a_ptr0 += 2;
        a_ptr1 += 2;

        y_ptr += 2;
      }

      if (m & 1) {
        fp32_low = vcvt_f32_bf16(x_vecx4);
        x0 = vgetq_lane_f32(fp32_low, 0);
        x1 = vgetq_lane_f32(fp32_low, 1);

        y_ptr[0] += x0 * vcvtah_f32_bf16(a_ptr0[0]);
        y_ptr[0] += x1 * vcvtah_f32_bf16(a_ptr1[0]);
      }

      x_ptr += 2;
    }

    if (n & 1) {
      x0 = vcvtah_f32_bf16(x_ptr[0]) * alpha;
      y_ptr = y;
      a_ptr0 = a_ptr;

      for (j = 0; j < m; j++) {
        y_ptr[j] += x0 * vcvtah_f32_bf16(a_ptr0[j]);
      }
    }

    return (0);
  }

  BLASLONG iy = 0;
  for (i = 0; i < m; i++) {
      y[iy] *= beta;
      iy += incy;
   }

  for (j = 0; j < n; j++) {
    x0 = alpha * vcvtah_f32_bf16(*x_ptr);
    iy = 0;
    for (i = 0; i < m; i++) {
      y[iy] += x0 * vcvtah_f32_bf16(a_ptr[i]);
      iy += incy;
    }

    a_ptr += lda;
    x_ptr += incx;
  }

  return (0);
}
