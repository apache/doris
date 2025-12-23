/*******************************************************************************
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
*******************************************************************************/

#include <stdio.h>
#include "common.h"

int CNAME(BLASLONG m, BLASLONG n, FLOAT *a, BLASLONG lda, BLASLONG offset, FLOAT *b){

  BLASLONG i, ii, j, jj, k;

  FLOAT *a1, *a2,  *a3,  *a4,  *a5,  *a6,  *a7,  *a8;
  FLOAT *a9, *a10, *a11, *a12, *a13, *a14, *a15, *a16;

  FLOAT data1, data2;

  lda *= 2;
  jj = offset;

  j = (n >> 4);
  while (j > 0){

    a1  = a +  0 * lda;
    a2  = a +  1 * lda;
    a3  = a +  2 * lda;
    a4  = a +  3 * lda;
    a5  = a +  4 * lda;
    a6  = a +  5 * lda;
    a7  = a +  6 * lda;
    a8  = a +  7 * lda;
    a9  = a +  8 * lda;
    a10 = a +  9 * lda;
    a11 = a + 10 * lda;
    a12 = a + 11 * lda;
    a13 = a + 12 * lda;
    a14 = a + 13 * lda;
    a15 = a + 14 * lda;
    a16 = a + 15 * lda;

    a += 16 * lda;
    ii = 0;

    for (i = 0; i < m; i++) {

      if ((ii >= jj ) && (ii - jj < 16)) {

	data1 = *(a1 + (ii - jj) * lda + 0);
	data2 = *(a1 + (ii - jj) * lda + 1);

	compinv(b +  (ii - jj) * 2, data1, data2);

	for (k = ii - jj + 1; k < 16; k ++) {
	  *(b +  k * 2 + 0) = *(a1 +  k * lda + 0);
	  *(b +  k * 2 + 1) = *(a1 +  k * lda + 1);
	}
      }

      if (ii - jj < 0) {
	*(b +  0) = *(a1  +  0);
	*(b +  1) = *(a1  +  1);
	*(b +  2) = *(a2  +  0);
	*(b +  3) = *(a2  +  1);
	*(b +  4) = *(a3  +  0);
	*(b +  5) = *(a3  +  1);
	*(b +  6) = *(a4  +  0);
	*(b +  7) = *(a4  +  1);
	*(b +  8) = *(a5  +  0);
	*(b +  9) = *(a5  +  1);
	*(b + 10) = *(a6  +  0);
	*(b + 11) = *(a6  +  1);
	*(b + 12) = *(a7  +  0);
	*(b + 13) = *(a7  +  1);
	*(b + 14) = *(a8  +  0);
	*(b + 15) = *(a8  +  1);
  *(b + 16) = *(a9  +  0);
	*(b + 17) = *(a9  +  1);
	*(b + 18) = *(a10  +  0);
	*(b + 19) = *(a10  +  1);
	*(b + 20) = *(a11  +  0);
	*(b + 21) = *(a11  +  1);
	*(b + 22) = *(a12  +  0);
	*(b + 23) = *(a12  +  1);
	*(b + 24) = *(a13  +  0);
	*(b + 25) = *(a13  +  1);
	*(b + 26) = *(a14  +  0);
	*(b + 27) = *(a14  +  1);
	*(b + 28) = *(a15  +  0);
	*(b + 29) = *(a15  +  1);
	*(b + 30) = *(a16  +  0);
	*(b + 31) = *(a16  +  1);
      }

      a1  += 2;
      a2  += 2;
      a3  += 2;
      a4  += 2;
      a5  += 2;
      a6  += 2;
      a7  += 2;
      a8  += 2;
      a9  += 2;
      a10  += 2;
      a11  += 2;
      a12  += 2;
      a13  += 2;
      a14  += 2;
      a15  += 2;
      a16  += 2;
      b  += 32;
      ii ++;
    }

    jj += 16;
    j --;
  }

  if (n & 8) {
    a1  = a +  0 * lda;
    a2  = a +  1 * lda;
    a3  = a +  2 * lda;
    a4  = a +  3 * lda;
    a5  = a +  4 * lda;
    a6  = a +  5 * lda;
    a7  = a +  6 * lda;
    a8  = a +  7 * lda;

    a += 8 * lda;
    ii = 0;

    for (i = 0; i < m; i++) {

      if ((ii >= jj ) && (ii - jj < 8)) {

	data1 = *(a1 + (ii - jj) * lda + 0);
	data2 = *(a1 + (ii - jj) * lda + 1);

	compinv(b +  (ii - jj) * 2, data1, data2);

	for (k = ii - jj + 1; k < 8; k ++) {
	  *(b +  k * 2 + 0) = *(a1 +  k * lda + 0);
	  *(b +  k * 2 + 1) = *(a1 +  k * lda + 1);
	}
    }

      if (ii - jj < 0) {
	*(b +  0) = *(a1  +  0);
	*(b +  1) = *(a1  +  1);
	*(b +  2) = *(a2  +  0);
	*(b +  3) = *(a2  +  1);
	*(b +  4) = *(a3  +  0);
	*(b +  5) = *(a3  +  1);
	*(b +  6) = *(a4  +  0);
	*(b +  7) = *(a4  +  1);
	*(b +  8) = *(a5  +  0);
	*(b +  9) = *(a5  +  1);
	*(b + 10) = *(a6  +  0);
	*(b + 11) = *(a6  +  1);
	*(b + 12) = *(a7  +  0);
	*(b + 13) = *(a7  +  1);
	*(b + 14) = *(a8  +  0);
	*(b + 15) = *(a8  +  1);
      }

      a1  += 2;
      a2  += 2;
      a3  += 2;
      a4  += 2;
      a5  += 2;
      a6  += 2;
      a7  += 2;
      a8  += 2;
      b  += 16;
      ii ++;
    }

    jj += 8;
  }

  if (n & 4) {

    a1  = a +  0 * lda;
    a2  = a +  1 * lda;
    a3  = a +  2 * lda;
    a4  = a +  3 * lda;
    a += 4 * lda;
    ii = 0;

    for (i = 0; i < m; i++) {

      if ((ii >= jj ) && (ii - jj < 4)) {
	data1 = *(a1 + (ii - jj) * lda + 0);
	data2 = *(a1 + (ii - jj) * lda + 1);

	compinv(b +  (ii - jj) * 2, data1, data2);

	for (k = ii - jj + 1; k < 4; k ++) {
	  *(b +  k * 2 + 0) = *(a1 +  k * lda + 0);
	  *(b +  k * 2 + 1) = *(a1 +  k * lda + 1);
	}
      }

      if (ii - jj < 0) {
	*(b +  0) = *(a1  +  0);
	*(b +  1) = *(a1  +  1);
	*(b +  2) = *(a2  +  0);
	*(b +  3) = *(a2  +  1);
	*(b +  4) = *(a3  +  0);
	*(b +  5) = *(a3  +  1);
	*(b +  6) = *(a4  +  0);
	*(b +  7) = *(a4  +  1);
      }

      a1  += 2;
      a2  += 2;
      a3  += 2;
      a4  += 2;
      b  += 8;
      ii ++;
    }

    jj += 4;
  }

  if (n & 2) {

    a1  = a +  0 * lda;
    a2  = a +  1 * lda;
    a += 2 * lda;
    ii = 0;

    for (i = 0; i < m; i++) {

      if ((ii >= jj ) && (ii - jj < 2)) {
	data1 = *(a1 + (ii - jj) * lda + 0);
	data2 = *(a1 + (ii - jj) * lda + 1);

	compinv(b +  (ii - jj) * 2, data1, data2);
	for (k = ii - jj + 1; k < 2; k ++) {
	  *(b +  k * 2 + 0) = *(a1 +  k * lda + 0);
	  *(b +  k * 2 + 1) = *(a1 +  k * lda + 1);
	}
      }

      if (ii - jj < 0) {
	*(b +  0) = *(a1  +  0);
	*(b +  1) = *(a1  +  1);
	*(b +  2) = *(a2  +  0);
	*(b +  3) = *(a2  +  1);
      }

      a1  += 2;
      a2  += 2;
      b  += 4;
      ii ++;
    }

    jj += 2;
  }

  if (n & 1) {

    a1  = a +  0 * lda;
    ii = 0;

    for (i = 0; i < m; i++) {

      if ((ii >= jj ) && (ii - jj < 1)) {
	data1 = *(a1 + (ii - jj) * lda + 0);
	data2 = *(a1 + (ii - jj) * lda + 1);

	compinv(b +  (ii - jj) * 2, data1, data2);
	for (k = ii - jj + 1; k < 1; k ++) {
	  *(b +  k * 2 + 0) = *(a1 +  k * lda + 0);
	  *(b +  k * 2 + 1) = *(a1 +  k * lda + 1);
	}
      }

      if (ii - jj < 0) {
	*(b +  0) = *(a1  +  0);
	*(b +  1) = *(a1  +  1);
      }

      a1  += 2;
      b  += 2;
      ii ++;
    }
  }

  return 0;
}
