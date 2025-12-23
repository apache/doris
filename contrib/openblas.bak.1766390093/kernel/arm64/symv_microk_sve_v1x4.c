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
#include <arm_sve.h>

#ifdef DOUBLE
#define SV_COUNT svcntd
#define SV_TYPE svfloat64_t
#define SV_TRUE svptrue_b64
#define SV_WHILE svwhilelt_b64_s64
#define SV_DUP svdup_f64
#else
#define SV_COUNT svcntw
#define SV_TYPE svfloat32_t
#define SV_TRUE svptrue_b32
#define SV_WHILE svwhilelt_b32_s64
#define SV_DUP svdup_f32
#endif

static void symv_kernel_v1x4(BLASLONG from, BLASLONG to, FLOAT *a0, FLOAT *a1, FLOAT *a2, FLOAT *a3, 
                             FLOAT *x, FLOAT *y, FLOAT *temp1, FLOAT *temp2)
{
  SV_TYPE vtmpx0 = SV_DUP(temp1[0]);
  SV_TYPE vtmpx1 = SV_DUP(temp1[1]);
  SV_TYPE vtmpx2 = SV_DUP(temp1[2]);
  SV_TYPE vtmpx3 = SV_DUP(temp1[3]);
  SV_TYPE vtmpy0 = SV_DUP(0.0);
  SV_TYPE vtmpy1 = SV_DUP(0.0);
  SV_TYPE vtmpy2 = SV_DUP(0.0);
  SV_TYPE vtmpy3 = SV_DUP(0.0);
  SV_TYPE vx, vy;
  SV_TYPE vap0, vap1, vap2, vap3;
  BLASLONG i;
  uint64_t sve_size = SV_COUNT();
  svbool_t pg;

  for (i = from; i < to; i += sve_size) {
    pg = SV_WHILE(i, to);
    vy = svld1(pg, &y[i]);
    vx = svld1(pg, &x[i]);
    vap0 = svld1(pg, &a0[i]);
    vap1 = svld1(pg, &a1[i]);
    vap2 = svld1(pg, &a2[i]);
    vap3 = svld1(pg, &a3[i]);
    vy = svmla_m(pg, vy, vtmpx0, vap0);
    vy = svmla_m(pg, vy, vtmpx1, vap1);
    vy = svmla_m(pg, vy, vtmpx2, vap2);
    vy = svmla_m(pg, vy, vtmpx3, vap3);
    vtmpy0 = svmla_m(pg, vtmpy0, vx, vap0);
    vtmpy1 = svmla_m(pg, vtmpy1, vx, vap1);
    vtmpy2 = svmla_m(pg, vtmpy2, vx, vap2);
    vtmpy3 = svmla_m(pg, vtmpy3, vx, vap3);
    svst1(pg, &y[i], vy);
  }
  pg = SV_TRUE();
  temp2[0] += svaddv(pg, vtmpy0);
  temp2[1] += svaddv(pg, vtmpy1);
  temp2[2] += svaddv(pg, vtmpy2);
  temp2[3] += svaddv(pg, vtmpy3);
}
