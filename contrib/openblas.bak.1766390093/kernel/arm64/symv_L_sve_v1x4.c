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

#include "symv_microk_sve_v1x4.c"

int CNAME(BLASLONG m, BLASLONG offset, FLOAT alpha, FLOAT *a, BLASLONG lda,
          FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *buffer)
{
  BLASLONG i, j;
  FLOAT temp1, temp2;
  FLOAT tmp1[4];
  FLOAT tmp2[4];
  FLOAT *a0, *a1, *a2, *a3;
  FLOAT x0, x1, x2, x3;
  FLOAT *X = x;
  FLOAT *Y = y;

  if (inc_y != 1) {
    Y = buffer;
    COPY_K(m, y, inc_y, Y, 1);
  }
  if (inc_x != 1) {
    if (inc_y != 1) {
      X = Y + m;
    } else {
      X = buffer;
    }
    COPY_K(m, x, inc_x, X, 1);
  }

  BLASLONG offset1 = (offset / 4) * 4;

  for (j = 0; j < offset1; j+=4) {
    a0 = &a[j*lda];
    a1 = a0 + lda;
    a2 = a1 + lda;
    a3 = a2 + lda;
    x0 = X[j];
    x1 = X[j+1];
    x2 = X[j+2];
    x3 = X[j+3];
    tmp2[0] = a0[j  ]*x0 + a0[j+1]*x1 + a0[j+2]*x2 + a0[j+3]*x3;
    tmp2[1] = a0[j+1]*x0 + a1[j+1]*x1 + a1[j+2]*x2 + a1[j+3]*x3;
    tmp2[2] = a0[j+2]*x0 + a1[j+2]*x1 + a2[j+2]*x2 + a2[j+3]*x3;
    tmp2[3] = a0[j+3]*x0 + a1[j+3]*x1 + a2[j+3]*x2 + a3[j+3]*x3;
    tmp1[0] = alpha * x0;
    tmp1[1] = alpha * x1;
    tmp1[2] = alpha * x2;
    tmp1[3] = alpha * x3;

    symv_kernel_v1x4(j+4, m, a0, a1, a2, a3, X, Y, tmp1, tmp2);

    Y[j]   += alpha * tmp2[0];
    Y[j+1] += alpha * tmp2[1];
    Y[j+2] += alpha * tmp2[2];
    Y[j+3] += alpha * tmp2[3];
  }

  for (j = offset1; j < offset; j++) {
    temp1 = alpha * X[j];
    temp2 = 0.0;
    a0 = &a[j*lda];
    Y[j] += temp1 * a0[j];
    for (i = j+1; i < m; i++) {
      Y[i] += temp1 * a0[i];
      temp2 += a0[i] * X[i];
    }
    Y[j] += alpha * temp2;
  }

  if (inc_y != 1) {
    COPY_K(m, Y, 1, y, inc_y);
  }
  return(0);
}
