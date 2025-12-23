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

#include <arm_sve.h>
#include "common.h"

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

int CNAME(BLASLONG m, BLASLONG n, BLASLONG dummy1, FLOAT alpha, FLOAT *a, BLASLONG lda, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *buffer)
{
  BLASLONG i;
  BLASLONG ix,iy;
  BLASLONG j;
  FLOAT *a_ptr;
  FLOAT temp;

  iy = 0;
  a_ptr = a;

  if (inc_x == 1) {
    svbool_t pg_true = SV_TRUE();
    uint64_t sve_size = SV_COUNT();
    uint64_t sve_size2 = sve_size * 2;
    BLASLONG m1 = m & -sve_size;
    BLASLONG m2 = m & -sve_size2;

    for (j = 0; j < n; j++) {
      BLASLONG i = 0;

      SV_TYPE temp_vec_v2_0 = SV_DUP(0.0);
      SV_TYPE temp_vec_v2_1 = SV_DUP(0.0);
      for (; i < m2; i += sve_size2) {
        SV_TYPE a_vec0 = svld1(pg_true, a_ptr + i);
        SV_TYPE x_vec0 = svld1(pg_true, x + i);
        SV_TYPE a_vec1 = svld1(pg_true, a_ptr + i + sve_size);
        SV_TYPE x_vec1 = svld1(pg_true, x + i + sve_size);
        temp_vec_v2_0 = svmla_m(pg_true, temp_vec_v2_0, a_vec0, x_vec0);
        temp_vec_v2_1 = svmla_m(pg_true, temp_vec_v2_1, a_vec1, x_vec1);
      }

      SV_TYPE temp_vec_v1 = SV_DUP(0.0);
      for (; i < m1; i += sve_size) {
        SV_TYPE a_vec0 = svld1(pg_true, a_ptr + i);
        SV_TYPE x_vec0 = svld1(pg_true, x + i);
        temp_vec_v1 = svmla_m(pg_true, temp_vec_v1, a_vec0, x_vec0);
      }

      SV_TYPE temp_vec = SV_DUP(0.0);
      for (; i < m; i += sve_size) {
        svbool_t pg = SV_WHILE(i, m);
        SV_TYPE a_vec = svld1(pg, a_ptr + i);
        SV_TYPE x_vec = svld1(pg, x + i);
        temp_vec = svmla_m(pg, temp_vec, a_vec, x_vec);
      }

      y[iy] += alpha * (
        (svaddv(SV_TRUE(), temp_vec_v2_0) + svaddv(SV_TRUE(), temp_vec)) +
        (svaddv(SV_TRUE(), temp_vec_v2_1) + svaddv(SV_TRUE(), temp_vec_v1))
      );

      iy += inc_y;
      a_ptr += lda;
    }
    return(0);
  }

  for (j = 0; j < n; j++) {
    temp = 0.0;
    ix = 0;
    for (i = 0; i < m; i++) {
      temp += a_ptr[i] * x[ix];
      ix += inc_x;
    }
    y[iy] += alpha * temp;
    iy += inc_y;
    a_ptr += lda;
  }
  return (0);
}
