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

  FLOAT *a1;
  FLOAT data1, data2;

  lda *= 2;
  jj = offset;

  j = (n >> 4);
  while (j > 0){

    a1 = a;
    a += 32;
    ii = 0;

    for (i = 0; i < m; i++) {

      if ((ii >= jj ) && (ii - jj < 16)) {

	data1 = *(a1 + (ii - jj) * 2 + 0);
	data2 = *(a1 + (ii - jj) * 2 + 1);

	compinv(b +  (ii - jj) * 2, data1, data2);

	for (k = ii - jj + 1; k < 16; k ++) {
	  *(b +  k * 2 + 0) = *(a1 +  k * 2 + 0);
	  *(b +  k * 2 + 1) = *(a1 +  k * 2 + 1);
	}

      }

      if (ii - jj < 0) {
	*(b +  0) = *(a1 +  0);
	*(b +  1) = *(a1 +  1);
	*(b +  2) = *(a1 +  2);
	*(b +  3) = *(a1 +  3);
	*(b +  4) = *(a1 +  4);
	*(b +  5) = *(a1 +  5);
	*(b +  6) = *(a1 +  6);
	*(b +  7) = *(a1 +  7);
	*(b +  8) = *(a1 +  8);
	*(b +  9) = *(a1 +  9);
	*(b + 10) = *(a1 + 10);
	*(b + 11) = *(a1 + 11);
	*(b + 12) = *(a1 + 12);
	*(b + 13) = *(a1 + 13);
	*(b + 14) = *(a1 + 14);
	*(b + 15) = *(a1 + 15);
  *(b + 16) = *(a1 + 16);
	*(b + 17) = *(a1 + 17);
	*(b + 18) = *(a1 + 18);
	*(b + 19) = *(a1 + 19);
	*(b + 20) = *(a1 + 20);
	*(b + 21) = *(a1 + 21);
	*(b + 22) = *(a1 + 22);
	*(b + 23) = *(a1 + 23);
	*(b + 24) = *(a1 + 24);
	*(b + 25) = *(a1 + 25);
	*(b + 26) = *(a1 + 26);
	*(b + 27) = *(a1 + 27);
	*(b + 28) = *(a1 + 28);
	*(b + 29) = *(a1 + 29);
	*(b + 30) = *(a1 + 30);
	*(b + 31) = *(a1 + 31);
      }

      b  += 32;
      a1 += lda;
      ii ++;
    }

    jj += 16;
    j --;
  }

  j = (n & 8);
  if (j > 0) {
    a1 = a;
    a += 16;
    ii = 0;

    for (i = 0; i < m; i++) {

      if ((ii >= jj ) && (ii - jj < 8)) {

	data1 = *(a1 + (ii - jj) * 2 + 0);
	data2 = *(a1 + (ii - jj) * 2 + 1);

	compinv(b +  (ii - jj) * 2, data1, data2);

	for (k = ii - jj + 1; k < 8; k ++) {
	  *(b +  k * 2 + 0) = *(a1 +  k * 2 + 0);
	  *(b +  k * 2 + 1) = *(a1 +  k * 2 + 1);
	}

      }

      if (ii - jj < 0) {
	*(b +  0) = *(a1 +  0);
	*(b +  1) = *(a1 +  1);
	*(b +  2) = *(a1 +  2);
	*(b +  3) = *(a1 +  3);
	*(b +  4) = *(a1 +  4);
	*(b +  5) = *(a1 +  5);
	*(b +  6) = *(a1 +  6);
	*(b +  7) = *(a1 +  7);
	*(b +  8) = *(a1 +  8);
	*(b +  9) = *(a1 +  9);
	*(b + 10) = *(a1 + 10);
	*(b + 11) = *(a1 + 11);
	*(b + 12) = *(a1 + 12);
	*(b + 13) = *(a1 + 13);
	*(b + 14) = *(a1 + 14);
	*(b + 15) = *(a1 + 15);
      }

      b  += 16;
      a1 += lda;
      ii ++;
    }

    jj += 8;
  }

  j = (n & 4);
  if (j > 0) {

    a1 = a;
    a += 8;
    ii = 0;

    for (i = 0; i < m; i++) {

      if ((ii >= jj ) && (ii - jj < 4)) {

	data1 = *(a1 + (ii - jj) * 2 + 0);
	data2 = *(a1 + (ii - jj) * 2 + 1);

	compinv(b +  (ii - jj) * 2, data1, data2);

	for (k = ii - jj + 1; k < 4; k ++) {
	  *(b +  k * 2 + 0) = *(a1 +  k * 2 + 0);
	  *(b +  k * 2 + 1) = *(a1 +  k * 2 + 1);
	}

      }

      if (ii - jj < 0) {
	*(b +  0) = *(a1 +  0);
	*(b +  1) = *(a1 +  1);
	*(b +  2) = *(a1 +  2);
	*(b +  3) = *(a1 +  3);
	*(b +  4) = *(a1 +  4);
	*(b +  5) = *(a1 +  5);
	*(b +  6) = *(a1 +  6);
	*(b +  7) = *(a1 +  7);
      }

      b  += 8;
      a1 += lda;
      ii ++;
    }

    jj += 4;
  }

  j = (n & 2);
  if (j > 0) {

    a1 = a;
    a += 4;
    ii = 0;

    for (i = 0; i < m; i++) {

      if ((ii >= jj ) && (ii - jj < 2)) {

	data1 = *(a1 + (ii - jj) * 2 + 0);
	data2 = *(a1 + (ii - jj) * 2 + 1);

	compinv(b +  (ii - jj) * 2, data1, data2);

	for (k = ii - jj + 1; k < 2; k ++) {
	  *(b +  k * 2 + 0) = *(a1 +  k * 2 + 0);
	  *(b +  k * 2 + 1) = *(a1 +  k * 2 + 1);
	}

      }

      if (ii - jj < 0) {
	*(b +  0) = *(a1 +  0);
	*(b +  1) = *(a1 +  1);
	*(b +  2) = *(a1 +  2);
	*(b +  3) = *(a1 +  3);
      }

      b  += 4;
      a1 += lda;
      ii ++;
    }

    jj += 2;
  }

  j = (n & 1);
  if (j > 0) {

    a1 = a;
    ii = 0;

    for (i = 0; i < m; i++) {

      if ((ii >= jj ) && (ii - jj < 1)) {
	data1 = *(a1 + (ii - jj) * 2 + 0);
	data2 = *(a1 + (ii - jj) * 2 + 1);

	compinv(b +  (ii - jj) * 2, data1, data2);
      }

      if (ii - jj < 0) {
	*(b +  0) = *(a1 +  0);
	*(b +  1) = *(a1 +  1);
      }

      b  += 2;
      a1 += lda;
      ii ++;
    }
  }

  return 0;
}
