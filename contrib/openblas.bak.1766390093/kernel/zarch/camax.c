/***************************************************************************
Copyright (c) 2013-2019, The OpenBLAS Project
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
*****************************************************************************/

#include "common.h"
#include <math.h>

#define CABS1(x,i) (fabsf(x[i]) + fabsf(x[i + 1]))

static FLOAT camax_kernel_32(BLASLONG n, FLOAT *x) {
  FLOAT amax;

  __asm__("vlef   %%v0,0(%[x]),0\n\t"
    "vlef   %%v16,4(%[x]),0\n\t"
    "vlef   %%v0,8(%[x]),1\n\t"
    "vlef   %%v16,12(%[x]),1\n\t"
    "vlef   %%v0,16(%[x]),2\n\t"
    "vlef   %%v16,20(%[x]),2\n\t"
    "vlef   %%v0,24(%[x]),3\n\t"
    "vlef   %%v16,28(%[x]),3\n\t"
    "vflpsb %%v0,%%v0\n\t"
    "vflpsb %%v16,%%v16\n\t"
    "vfasb  %%v0,%%v0,%%v16\n\t"
    "vleib  %%v1,0,0\n\t"
    "vleib  %%v1,1,1\n\t"
    "vleib  %%v1,2,2\n\t"
    "vleib  %%v1,3,3\n\t"
    "vleib  %%v1,8,4\n\t"
    "vleib  %%v1,9,5\n\t"
    "vleib  %%v1,10,6\n\t"
    "vleib  %%v1,11,7\n\t"
    "vleib  %%v1,16,8\n\t"
    "vleib  %%v1,17,9\n\t"
    "vleib  %%v1,18,10\n\t"
    "vleib  %%v1,19,11\n\t"
    "vleib  %%v1,24,12\n\t"
    "vleib  %%v1,25,13\n\t"
    "vleib  %%v1,26,14\n\t"
    "vleib  %%v1,27,15\n\t"
    "srlg  %[n],%[n],5\n\t"
    "xgr %%r1,%%r1\n\t"
    "0:\n\t"
    "pfd 1, 1024(%%r1,%[x])\n\t"
    "vl    %%v16,0(%%r1,%[x])\n\t"
    "vl    %%v2,16(%%r1,%[x])\n\t"
    "vpkg  %%v17,%%v16,%%v2\n\t"
    "vperm %%v16,%%v16,%%v2,%%v1\n\t"
    "vl    %%v18,32(%%r1,%[x])\n\t"
    "vl    %%v2,48(%%r1,%[x])\n\t"
    "vpkg  %%v19,%%v18,%%v2\n\t"
    "vperm %%v18,%%v18,%%v2,%%v1\n\t"
    "vl    %%v20,64(%%r1,%[x])\n\t"
    "vl    %%v2,80(%%r1,%[x])\n\t"
    "vpkg  %%v21,%%v20,%%v2\n\t"
    "vperm %%v20,%%v20,%%v2,%%v1\n\t"
    "vl    %%v22,96(%%r1,%[x])\n\t"
    "vl    %%v2,112(%%r1,%[x])\n\t"
    "vpkg  %%v23,%%v22,%%v2\n\t"
    "vperm %%v22,%%v22,%%v2,%%v1\n\t"
    "vl    %%v24,128(%%r1,%[x])\n\t"
    "vl    %%v2,144(%%r1,%[x])\n\t"
    "vpkg  %%v25,%%v24,%%v2\n\t"
    "vperm %%v24,%%v24,%%v2,%%v1\n\t"
    "vl    %%v26,160(%%r1,%[x])\n\t"
    "vl    %%v2,176(%%r1,%[x])\n\t"
    "vpkg  %%v27,%%v26,%%v2\n\t"
    "vperm %%v26,%%v26,%%v2,%%v1\n\t"
    "vl    %%v28,192(%%r1,%[x])\n\t"
    "vl    %%v2,208(%%r1,%[x])\n\t"
    "vpkg  %%v29,%%v28,%%v2\n\t"
    "vperm %%v28,%%v28,%%v2,%%v1\n\t"
    "vl    %%v30,224(%%r1,%[x])\n\t"
    "vl    %%v2,240(%%r1,%[x])\n\t"
    "vpkg  %%v31,%%v30,%%v2\n\t"
    "vperm %%v30,%%v30,%%v2,%%v1\n\t"
    "vflpsb  %%v16,%%v16\n\t"
    "vflpsb  %%v17,%%v17\n\t"
    "vflpsb  %%v18,%%v18\n\t"
    "vflpsb  %%v19,%%v19\n\t"
    "vflpsb  %%v20,%%v20\n\t"
    "vflpsb  %%v21,%%v21\n\t"
    "vflpsb  %%v22,%%v22\n\t"
    "vflpsb  %%v23,%%v23\n\t"
    "vflpsb  %%v24,%%v24\n\t"
    "vflpsb  %%v25,%%v25\n\t"
    "vflpsb  %%v26,%%v26\n\t"
    "vflpsb  %%v27,%%v27\n\t"
    "vflpsb  %%v28,%%v28\n\t"
    "vflpsb  %%v29,%%v29\n\t"
    "vflpsb  %%v30,%%v30\n\t"
    "vflpsb  %%v31,%%v31\n\t"
    "vfasb %%v16,%%v16,%%v17\n\t"
    "vfasb %%v18,%%v18,%%v19\n\t"
    "vfasb %%v20,%%v20,%%v21\n\t"
    "vfasb %%v22,%%v22,%%v23\n\t"
    "vfasb %%v24,%%v24,%%v25\n\t"
    "vfasb %%v26,%%v26,%%v27\n\t"
    "vfasb %%v28,%%v28,%%v29\n\t"
    "vfasb %%v30,%%v30,%%v31\n\t"
    "vfmaxsb  %%v16,%%v16,%%v24,0\n\t"
    "vfmaxsb  %%v18,%%v18,%%v26,0\n\t"
    "vfmaxsb  %%v20,%%v20,%%v28,0\n\t"
    "vfmaxsb  %%v22,%%v22,%%v30,0\n\t"
    "vfmaxsb  %%v16,%%v16,%%v20,0\n\t"
    "vfmaxsb  %%v18,%%v18,%%v22,0\n\t"
    "vfmaxsb  %%v16,%%v16,%%v18,0\n\t"
    "vfmaxsb  %%v0,%%v0,%%v16,0\n\t"
    "agfi    %%r1, 256\n\t"
    "brctg   %[n], 0b\n\t"
    "veslg   %%v16,%%v0,32\n\t"
    "vfmaxsb %%v0,%%v0,%%v16,0\n\t"
    "vrepf  %%v16,%%v0,2\n\t"
    "wfmaxsb %%v0,%%v0,%%v16,0\n\t"
    "ler    %[amax],%%f0"
    : [amax] "=f"(amax),[n] "+&r"(n)
    : "m"(*(const FLOAT (*)[n * 2]) x),[x] "a"(x)
    : "cc", "r1", "v0", "v1", "v2", "v16", "v17", "v18", "v19", "v20",
       "v21", "v22", "v23", "v24", "v25", "v26", "v27", "v28", "v29", "v30",
       "v31");

  return amax;
}

FLOAT CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x) {
  BLASLONG i = 0;
  BLASLONG ix = 0;
  FLOAT maxf = 0.0;
  BLASLONG inc_x2;

  if (n <= 0 || inc_x <= 0)
    return (maxf);

  if (inc_x == 1) {

    BLASLONG n1 = n & -32;
    if (n1 > 0) {

      maxf = camax_kernel_32(n1, x);
      ix = n1 * 2;
      i = n1;
    } else {
      maxf = CABS1(x, 0);
      ix += 2;
      i++;
    }

    while (i < n) {
      if (CABS1(x, ix) > maxf) {
        maxf = CABS1(x, ix);
      }
      ix += 2;
      i++;
    }
    return (maxf);

  } else {

    maxf = CABS1(x, 0);
    inc_x2 = 2 * inc_x;

    BLASLONG n1 = n & -4;
    while (i < n1) {

      if (CABS1(x, ix) > maxf) {
        maxf = CABS1(x, ix);
      }
      if (CABS1(x, ix + inc_x2) > maxf) {
        maxf = CABS1(x, ix + inc_x2);
      }
      if (CABS1(x, ix + inc_x2 * 2) > maxf) {
        maxf = CABS1(x, ix + inc_x2 * 2);
      }
      if (CABS1(x, ix + inc_x2 * 3) > maxf) {
        maxf = CABS1(x, ix + inc_x2 * 3);
      }

      ix += inc_x2 * 4;

      i += 4;

    }

    while (i < n) {
      if (CABS1(x, ix) > maxf) {
        maxf = CABS1(x, ix);
      }
      ix += inc_x2;
      i++;
    }
    return (maxf);
  }
}
