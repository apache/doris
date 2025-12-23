/***************************************************************************
Copyright (c) 2019, The OpenBLAS Project
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

#define CABS1(x,i) (fabs(x[i]) + fabs(x[i + 1]))

static BLASLONG izamax_kernel_16(BLASLONG n, FLOAT *x, FLOAT *amax) {
  BLASLONG iamax;

  __asm__("vleg   %%v0,0(%[x]),0\n\t"
    "vleg   %%v1,8(%[x]),0\n\t"
    "vleg   %%v0,16(%[x]),1\n\t"
    "vleg   %%v1,24(%[x]),1\n\t"
    "vflpdb %%v0,%%v0\n\t"
    "vflpdb %%v1,%%v1\n\t"
    "vfadb  %%v0,%%v0,%%v1\n\t"
    "vleig  %%v1,0,0\n\t"
    "vleig  %%v1,1,1\n\t"
    "vrepig %%v2,8\n\t"
    "vzero  %%v3\n\t"
    "vleig  %%v24,0,0\n\t"
    "vleig  %%v24,1,1\n\t"
    "vleig  %%v25,2,0\n\t"
    "vleig  %%v25,3,1\n\t"
    "vleig  %%v26,4,0\n\t"
    "vleig  %%v26,5,1\n\t"
    "vleig  %%v27,6,0\n\t"
    "vleig  %%v27,7,1\n\t"
    "srlg  %[n],%[n],4\n\t"
    "xgr %%r1,%%r1\n\t"
    "0:\n\t"
    "pfd 1, 1024(%%r1,%[x])\n\t"
    "vleg  %%v16,0(%%r1,%[x]),0\n\t"
    "vleg  %%v17,8(%%r1,%[x]),0\n\t"
    "vleg  %%v16,16(%%r1,%[x]),1\n\t"
    "vleg  %%v17,24(%%r1,%[x]),1\n\t"
    "vleg  %%v18,32(%%r1,%[x]),0\n\t"
    "vleg  %%v19,40(%%r1,%[x]),0\n\t"
    "vleg  %%v18,48(%%r1,%[x]),1\n\t"
    "vleg  %%v19,56(%%r1,%[x]),1\n\t"
    "vleg  %%v20,64(%%r1,%[x]),0\n\t"
    "vleg  %%v21,72(%%r1,%[x]),0\n\t"
    "vleg  %%v20,80(%%r1,%[x]),1\n\t"
    "vleg  %%v21,88(%%r1,%[x]),1\n\t"
    "vleg  %%v22,96(%%r1,%[x]),0\n\t"
    "vleg  %%v23,104(%%r1,%[x]),0\n\t"
    "vleg  %%v22,112(%%r1,%[x]),1\n\t"
    "vleg  %%v23,120(%%r1,%[x]),1\n\t"
    "vflpdb  %%v16, %%v16\n\t"
    "vflpdb  %%v17, %%v17\n\t"
    "vflpdb  %%v18, %%v18\n\t"
    "vflpdb  %%v19, %%v19\n\t"
    "vflpdb  %%v20, %%v20\n\t"
    "vflpdb  %%v21, %%v21\n\t"
    "vflpdb  %%v22, %%v22\n\t"
    "vflpdb  %%v23, %%v23\n\t"
    "vfadb %%v16,%%v16,%%v17\n\t"
    "vfadb %%v17,%%v18,%%v19\n\t"
    "vfadb %%v18,%%v20,%%v21\n\t"
    "vfadb %%v19,%%v22,%%v23\n\t"
    "vfchedb  %%v4,%%v16,%%v17\n\t"
    "vfchedb  %%v5,%%v18,%%v19\n\t"
    "vsel    %%v16,%%v16,%%v17,%%v4\n\t"
    "vsel    %%v4,%%v24,%%v25,%%v4\n\t"
    "vsel    %%v17,%%v18,%%v19,%%v5\n\t"
    "vsel    %%v5,%%v26,%%v27,%%v5\n\t"
    "vfchedb  %%v18,%%v16,%%v17\n\t"
    "vsel    %%v16,%%v16,%%v17,%%v18\n\t"
    "vsel    %%v4,%%v4,%%v5,%%v18\n\t"
    "vag     %%v4,%%v4,%%v3\n\t"
    "vfchedb  %%v5,%%v0,%%v16\n\t"
    "vsel    %%v0,%%v0,%%v16,%%v5\n\t"
    "vsel    %%v1,%%v1,%%v4,%%v5\n\t"
    "vag     %%v3,%%v3,%%v2\n\t"
    "vleg  %%v16,128(%%r1,%[x]),0\n\t"
    "vleg  %%v17,136(%%r1,%[x]),0\n\t"
    "vleg  %%v16,144(%%r1,%[x]),1\n\t"
    "vleg  %%v17,152(%%r1,%[x]),1\n\t"
    "vleg  %%v18,160(%%r1,%[x]),0\n\t"
    "vleg  %%v19,168(%%r1,%[x]),0\n\t"
    "vleg  %%v18,176(%%r1,%[x]),1\n\t"
    "vleg  %%v19,184(%%r1,%[x]),1\n\t"
    "vleg  %%v20,192(%%r1,%[x]),0\n\t"
    "vleg  %%v21,200(%%r1,%[x]),0\n\t"
    "vleg  %%v20,208(%%r1,%[x]),1\n\t"
    "vleg  %%v21,216(%%r1,%[x]),1\n\t"
    "vleg  %%v22,224(%%r1,%[x]),0\n\t"
    "vleg  %%v23,232(%%r1,%[x]),0\n\t"
    "vleg  %%v22,240(%%r1,%[x]),1\n\t"
    "vleg  %%v23,248(%%r1,%[x]),1\n\t"
    "vflpdb  %%v16, %%v16\n\t"
    "vflpdb  %%v17, %%v17\n\t"
    "vflpdb  %%v18, %%v18\n\t"
    "vflpdb  %%v19, %%v19\n\t"
    "vflpdb  %%v20, %%v20\n\t"
    "vflpdb  %%v21, %%v21\n\t"
    "vflpdb  %%v22, %%v22\n\t"
    "vflpdb  %%v23, %%v23\n\t"
    "vfadb %%v16,%%v16,%%v17\n\t"
    "vfadb %%v17,%%v18,%%v19\n\t"
    "vfadb %%v18,%%v20,%%v21\n\t"
    "vfadb %%v19,%%v22,%%v23\n\t"
    "vfchedb  %%v4,%%v16,%%v17\n\t"
    "vfchedb  %%v5,%%v18,%%v19\n\t"
    "vsel    %%v16,%%v16,%%v17,%%v4\n\t"
    "vsel    %%v4,%%v24,%%v25,%%v4\n\t"
    "vsel    %%v17,%%v18,%%v19,%%v5\n\t"
    "vsel    %%v5,%%v26,%%v27,%%v5\n\t"
    "vfchedb  %%v18,%%v16,%%v17\n\t"
    "vsel    %%v16,%%v16,%%v17,%%v18\n\t"
    "vsel    %%v4,%%v4,%%v5,%%v18\n\t"
    "vag     %%v4,%%v4,%%v3\n\t"
    "vfchedb  %%v5,%%v0,%%v16\n\t"
    "vsel    %%v0,%%v0,%%v16,%%v5\n\t"
    "vsel    %%v1,%%v1,%%v4,%%v5\n\t"
    "vag     %%v3,%%v3,%%v2\n\t"
    "agfi    %%r1, 256\n\t"
    "brctg   %[n], 0b\n\t"
    "vrepg  %%v2,%%v0,1\n\t"
    "vrepg  %%v3,%%v1,1\n\t"
    "wfcdb  %%v2,%%v0\n\t"
    "jne 1f\n\t"
    "vsteg  %%v0,%[amax],0\n\t"
    "vmnlg  %%v0,%%v1,%%v3\n\t"
    "vlgvg  %[iamax],%%v0,0\n\t"
    "j 2f\n\t"
    "1:\n\t"
    "wfchdb %%v4,%%v2,%%v0\n\t"
    "vsel   %%v1,%%v3,%%v1,%%v4\n\t"
    "vsel   %%v0,%%v2,%%v0,%%v4\n\t"
    "std    %%f0,%[amax]\n\t"
    "vlgvg  %[iamax],%%v1,0\n\t"
    "2:\n\t"
    "nop 0"
    : [iamax] "=r"(iamax),[amax] "=Q"(*amax),[n] "+&r"(n)
    : "m"(*(const FLOAT (*)[n * 2]) x),[x] "a"(x)
    : "cc", "r1", "v0", "v1", "v2", "v3", "v4", "v5", "v16", "v17", "v18",
       "v19", "v20", "v21", "v22", "v23", "v24", "v25", "v26", "v27");

  return iamax;
}

BLASLONG CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x) {
  BLASLONG i = 0;
  BLASLONG ix = 0;
  FLOAT maxf = 0;
  BLASLONG max = 0;
  BLASLONG inc_x2;

  if (n <= 0 || inc_x <= 0)
    return (max);

  if (inc_x == 1) {

    BLASLONG n1 = n & -16;
    if (n1 > 0) {

      max = izamax_kernel_16(n1, x, &maxf);
      ix = n1 * 2;
      i = n1;
    } else {
      maxf = CABS1(x, 0);
      ix += 2;
      i++;
    }

    while (i < n) {
      if (CABS1(x, ix) > maxf) {
        max = i;
        maxf = CABS1(x, ix);
      }
      ix += 2;
      i++;
    }
    return (max + 1);

  } else {

    max = 0;
    maxf = CABS1(x, 0);
    inc_x2 = 2 * inc_x;

    BLASLONG n1 = n & -4;
    while (i < n1) {

      if (CABS1(x, ix) > maxf) {
        max = i;
        maxf = CABS1(x, ix);
      }
      if (CABS1(x, ix + inc_x2) > maxf) {
        max = i + 1;
        maxf = CABS1(x, ix + inc_x2);
      }
      if (CABS1(x, ix + 2 * inc_x2) > maxf) {
        max = i + 2;
        maxf = CABS1(x, ix + 2 * inc_x2);
      }
      if (CABS1(x, ix + 3 * inc_x2) > maxf) {
        max = i + 3;
        maxf = CABS1(x, ix + 3 * inc_x2);
      }

      ix += inc_x2 * 4;

      i += 4;

    }

    while (i < n) {
      if (CABS1(x, ix) > maxf) {
        max = i;
        maxf = CABS1(x, ix);
      }
      ix += inc_x2;
      i++;
    }
    return (max + 1);
  }
}
