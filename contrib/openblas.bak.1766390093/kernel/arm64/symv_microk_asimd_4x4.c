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

#include "common.h"
#include <arm_neon.h>

static void symv_kernel_4x4(BLASLONG from, BLASLONG to, FLOAT *a0, FLOAT *a1, FLOAT *a2, FLOAT *a3,
                            FLOAT *x, FLOAT *y, FLOAT *temp1, FLOAT *temp2)
{
#ifdef DOUBLE
  float64x2_t vtmpx0 = vld1q_dup_f64(&temp1[0]);
  float64x2_t vtmpx1 = vld1q_dup_f64(&temp1[1]);
  float64x2_t vtmpx2 = vld1q_dup_f64(&temp1[2]);
  float64x2_t vtmpx3 = vld1q_dup_f64(&temp1[3]);
  float64x2_t vtmpy0 = {0.0, 0.0};
  float64x2_t vtmpy1 = {0.0, 0.0};
  float64x2_t vtmpy2 = {0.0, 0.0};
  float64x2_t vtmpy3 = {0.0, 0.0};
  float64x2_t vxl, vxh, vyl, vyh;
  float64x2_t vap0l, vap0h, vap1l, vap1h, vap2l, vap2h, vap3l, vap3h;
  BLASLONG i;
  for (i = from; i < to; i+=4) {
    vyl = vld1q_f64(&y[i]);
    vyh = vld1q_f64(&y[i+2]);
    vxl = vld1q_f64(&x[i]);
    vxh = vld1q_f64(&x[i+2]);
    vap0l = vld1q_f64(&a0[i]);
    vap0h = vld1q_f64(&a0[i+2]);
    vap1l = vld1q_f64(&a1[i]);
    vap1h = vld1q_f64(&a1[i+2]);
    vap2l = vld1q_f64(&a2[i]);
    vap2h = vld1q_f64(&a2[i+2]);
    vap3l = vld1q_f64(&a3[i]);
    vap3h = vld1q_f64(&a3[i+2]);
    vyl = vfmaq_f64(vyl, vtmpx0, vap0l);
    vyh = vfmaq_f64(vyh, vtmpx0, vap0h);
    vyl = vfmaq_f64(vyl, vtmpx1, vap1l);
    vyh = vfmaq_f64(vyh, vtmpx1, vap1h);
    vyl = vfmaq_f64(vyl, vtmpx2, vap2l);
    vyh = vfmaq_f64(vyh, vtmpx2, vap2h);
    vyl = vfmaq_f64(vyl, vtmpx3, vap3l);
    vyh = vfmaq_f64(vyh, vtmpx3, vap3h);
    vtmpy0 = vfmaq_f64(vtmpy0, vxl, vap0l);
    vtmpy0 = vfmaq_f64(vtmpy0, vxh, vap0h);
    vtmpy1 = vfmaq_f64(vtmpy1, vxl, vap1l);
    vtmpy2 = vfmaq_f64(vtmpy2, vxl, vap2l);
    vtmpy1 = vfmaq_f64(vtmpy1, vxh, vap1h);
    vtmpy2 = vfmaq_f64(vtmpy2, vxh, vap2h);
    vtmpy3 = vfmaq_f64(vtmpy3, vxl, vap3l);
    vtmpy3 = vfmaq_f64(vtmpy3, vxh, vap3h);
    vst1q_f64(&y[i], vyl);
    vst1q_f64(&y[i+2], vyh);
  }
  temp2[0] += vaddvq_f64(vtmpy0);
  temp2[1] += vaddvq_f64(vtmpy1);
  temp2[2] += vaddvq_f64(vtmpy2);
  temp2[3] += vaddvq_f64(vtmpy3);
#else
  float32x4_t vtmpx0 = vld1q_dup_f32(&temp1[0]);
  float32x4_t vtmpx1 = vld1q_dup_f32(&temp1[1]);
  float32x4_t vtmpx2 = vld1q_dup_f32(&temp1[2]);
  float32x4_t vtmpx3 = vld1q_dup_f32(&temp1[3]);
  float32x4_t vtmpy0 = {0.0, 0.0, 0.0, 0.0};
  float32x4_t vtmpy1 = {0.0, 0.0, 0.0, 0.0};
  float32x4_t vtmpy2 = {0.0, 0.0, 0.0, 0.0};
  float32x4_t vtmpy3 = {0.0, 0.0, 0.0, 0.0};
  float32x4_t vx, vy;
  float32x4_t vap0, vap1, vap2, vap3;
  BLASLONG i;
  for (i = from; i < to; i+=4) {
    vy = vld1q_f32(&y[i]);
    vx = vld1q_f32(&x[i]);
    vap0 = vld1q_f32(&a0[i]);
    vap1 = vld1q_f32(&a1[i]);
    vap2 = vld1q_f32(&a2[i]);
    vap3 = vld1q_f32(&a3[i]);
    vy = vfmaq_f32(vy, vtmpx0, vap0);
    vy = vfmaq_f32(vy, vtmpx1, vap1);
    vy = vfmaq_f32(vy, vtmpx2, vap2);
    vy = vfmaq_f32(vy, vtmpx3, vap3);
    vtmpy0 = vfmaq_f32(vtmpy0, vx, vap0);
    vtmpy1 = vfmaq_f32(vtmpy1, vx, vap1);
    vtmpy2 = vfmaq_f32(vtmpy2, vx, vap2);
    vtmpy3 = vfmaq_f32(vtmpy3, vx, vap3);
    vst1q_f32(&y[i], vy);
  }
  temp2[0] += vaddvq_f32(vtmpy0);
  temp2[1] += vaddvq_f32(vtmpy1);
  temp2[2] += vaddvq_f32(vtmpy2);
  temp2[3] += vaddvq_f32(vtmpy3);
#endif
}
