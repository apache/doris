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
  BLASLONG i, j, j1, j2, m2;
  FLOAT temp1, temp2;
  FLOAT tmp1[4];
  FLOAT tmp2[4];
  FLOAT *a0, *a1, *a2, *a3;
  FLOAT *X = x;
  FLOAT *Y = y;

  BLASLONG m1 = m - offset;
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

  m2 = m - (offset % 4);
  for (j = m1; j < m2; j += 4) {
    tmp1[0] = alpha * X[j];
    tmp1[1] = alpha * X[j+1];
    tmp1[2] = alpha * X[j+2];
    tmp1[3] = alpha * X[j+3];
    tmp2[0] = 0.0;
    tmp2[1] = 0.0;
    tmp2[2] = 0.0;
    tmp2[3] = 0.0;
    a0 = &a[j*lda];
    a1 = a0 + lda;
    a2 = a1 + lda;
    a3 = a2 + lda;
    symv_kernel_v1x4(0, j, a0, a1, a2, a3, X, Y, tmp1, tmp2);

    j2 = 0;
    for (j1 = j ; j1 < j+4 ; j1++) {
      temp1 = tmp1[j2];
      temp2 = tmp2[j2];
      a0 = &a[j1*lda];
      for (i=j ; i<j1; i++) {
        Y[i] += temp1 * a0[i];
        temp2 += a0[i] * X[i];
      }
      Y[j1] += temp1 * a0[j1] + alpha * temp2;
      j2++;
    }
  }

  for ( ; j < m; j++) {
    temp1 = alpha * X[j];
    temp2 = 0.0;
    a0 = &a[j*lda];
    for (i = 0 ; i < j; i++) {
      Y[i] += temp1 * a0[i];
      temp2 += a0[i] * X[i];
    }
    Y[j] += temp1 * a0[j] + alpha * temp2;
  }

  if (inc_y != 1) {
    COPY_K(m, Y, 1, y, inc_y);
  }
  return(0);
}
