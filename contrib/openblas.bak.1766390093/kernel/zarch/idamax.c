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

#define ABS fabs

static BLASLONG idamax_kernel_32(BLASLONG n, FLOAT *x, FLOAT *amax) {
  BLASLONG iamax;

  __asm__("vl     %%v0,0(%[x])\n\t"
    "vflpdb %%v0,%%v0\n\t"
    "vleig  %%v1,0,0\n\t"
    "vleig  %%v1,1,1\n\t"
    "vrepig %%v2,16\n\t"
    "vzero  %%v3\n\t"
    "vleig  %%v24,0,0\n\t"
    "vleig  %%v24,1,1\n\t"
    "vleig  %%v25,2,0\n\t"
    "vleig  %%v25,3,1\n\t"
    "vleig  %%v26,4,0\n\t"
    "vleig  %%v26,5,1\n\t"
    "vleig  %%v27,6,0\n\t"
    "vleig  %%v27,7,1\n\t"
    "vleig  %%v28,8,0\n\t"
    "vleig  %%v28,9,1\n\t"
    "vleig  %%v29,10,0\n\t"
    "vleig  %%v29,11,1\n\t"
    "vleig  %%v30,12,0\n\t"
    "vleig  %%v30,13,1\n\t"
    "vleig  %%v31,14,0\n\t"
    "vleig  %%v31,15,1\n\t"
    "srlg  %[n],%[n],5\n\t"
    "xgr %%r1,%%r1\n\t"
    "0:\n\t"
    "pfd 1, 1024(%%r1,%[x])\n\t"
    "vl  %%v16,0(%%r1,%[x])\n\t"
    "vl  %%v17,16(%%r1,%[x])\n\t"
    "vl  %%v18,32(%%r1,%[x])\n\t"
    "vl  %%v19,48(%%r1,%[x])\n\t"
    "vl  %%v20,64(%%r1,%[x])\n\t"
    "vl  %%v21,80(%%r1,%[x])\n\t"
    "vl  %%v22,96(%%r1,%[x])\n\t"
    "vl  %%v23,112(%%r1,%[x])\n\t"
    "vflpdb  %%v16, %%v16\n\t"
    "vflpdb  %%v17, %%v17\n\t"
    "vflpdb  %%v18, %%v18\n\t"
    "vflpdb  %%v19, %%v19\n\t"
    "vflpdb  %%v20, %%v20\n\t"
    "vflpdb  %%v21, %%v21\n\t"
    "vflpdb  %%v22, %%v22\n\t"
    "vflpdb  %%v23, %%v23\n\t"
    "vfchedb  %%v4,%%v16,%%v17\n\t"
    "vfchedb  %%v5,%%v18,%%v19\n\t"
    "vfchedb  %%v6,%%v20,%%v21\n\t"
    "vfchedb  %%v7,%%v22,%%v23\n\t"
    "vsel    %%v16,%%v16,%%v17,%%v4\n\t"
    "vsel    %%v4,%%v24,%%v25,%%v4\n\t"
    "vsel    %%v17,%%v18,%%v19,%%v5\n\t"
    "vsel    %%v5,%%v26,%%v27,%%v5\n\t"
    "vsel    %%v18,%%v20,%%v21,%%v6\n\t"
    "vsel    %%v6,%%v28,%%v29,%%v6\n\t"
    "vsel    %%v19,%%v22,%%v23,%%v7\n\t"
    "vsel    %%v7,%%v30,%%v31,%%v7\n\t"
    "vfchedb  %%v20,%%v16,%%v17\n\t"
    "vfchedb  %%v21,%%v18,%%v19\n\t"
    "vsel    %%v16,%%v16,%%v17,%%v20\n\t"
    "vsel    %%v4,%%v4,%%v5,%%v20\n\t"
    "vsel    %%v17,%%v18,%%v19,%%v21\n\t"
    "vsel    %%v5,%%v6,%%v7,%%v21\n\t"
    "vfchedb  %%v18,%%v16,%%v17\n\t"
    "vsel    %%v16,%%v16,%%v17,%%v18\n\t"
    "vsel    %%v4,%%v4,%%v5,%%v18\n\t"
    "vag     %%v4,%%v4,%%v3\n\t"
    "vfchedb  %%v5,%%v0,%%v16\n\t"
    "vsel    %%v0,%%v0,%%v16,%%v5\n\t"
    "vsel    %%v1,%%v1,%%v4,%%v5\n\t"
    "vag     %%v3,%%v3,%%v2\n\t"
    "vl  %%v16,128(%%r1,%[x])\n\t"
    "vl  %%v17,144(%%r1,%[x])\n\t"
    "vl  %%v18,160(%%r1,%[x])\n\t"
    "vl  %%v19,176(%%r1,%[x])\n\t"
    "vl  %%v20,192(%%r1,%[x])\n\t"
    "vl  %%v21,208(%%r1,%[x])\n\t"
    "vl  %%v22,224(%%r1,%[x])\n\t"
    "vl  %%v23,240(%%r1,%[x])\n\t"
    "vflpdb  %%v16, %%v16\n\t"
    "vflpdb  %%v17, %%v17\n\t"
    "vflpdb  %%v18, %%v18\n\t"
    "vflpdb  %%v19, %%v19\n\t"
    "vflpdb  %%v20, %%v20\n\t"
    "vflpdb  %%v21, %%v21\n\t"
    "vflpdb  %%v22, %%v22\n\t"
    "vflpdb  %%v23, %%v23\n\t"
    "vfchedb  %%v4,%%v16,%%v17\n\t"
    "vfchedb  %%v5,%%v18,%%v19\n\t"
    "vfchedb  %%v6,%%v20,%%v21\n\t"
    "vfchedb  %%v7,%%v22,%%v23\n\t"
    "vsel    %%v16,%%v16,%%v17,%%v4\n\t"
    "vsel    %%v4,%%v24,%%v25,%%v4\n\t"
    "vsel    %%v17,%%v18,%%v19,%%v5\n\t"
    "vsel    %%v5,%%v26,%%v27,%%v5\n\t"
    "vsel    %%v18,%%v20,%%v21,%%v6\n\t"
    "vsel    %%v6,%%v28,%%v29,%%v6\n\t"
    "vsel    %%v19,%%v22,%%v23,%%v7\n\t"
    "vsel    %%v7,%%v30,%%v31,%%v7\n\t"
    "vfchedb  %%v20,%%v16,%%v17\n\t"
    "vfchedb  %%v21,%%v18,%%v19\n\t"
    "vsel    %%v16,%%v16,%%v17,%%v20\n\t"
    "vsel    %%v4,%%v4,%%v5,%%v20\n\t"
    "vsel    %%v17,%%v18,%%v19,%%v21\n\t"
    "vsel    %%v5,%%v6,%%v7,%%v21\n\t"
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
    : "m"(*(const FLOAT (*)[n]) x),[x] "a"(x)
    : "cc", "r1", "v0", "v1", "v2", "v3", "v4", "v5", "v6", "v7", "v16",
       "v17", "v18", "v19", "v20", "v21", "v22", "v23", "v24", "v25", "v26",
       "v27", "v28", "v29", "v30", "v31");

  return iamax;
}

BLASLONG CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x) {
  BLASLONG i = 0;
  BLASLONG j = 0;
  FLOAT maxf = 0.0;
  BLASLONG max = 0;

  if (n <= 0 || inc_x <= 0)
    return (max);

  if (inc_x == 1) {

    BLASLONG n1 = n & -32;
    if (n1 > 0) {

      max = idamax_kernel_32(n1, x, &maxf);

      i = n1;
    } else {
      maxf = ABS(x[0]);
      i++;
    }

    while (i < n) {
      if (ABS(x[i]) > maxf) {
        max = i;
        maxf = ABS(x[i]);
      }
      i++;
    }
    return (max + 1);

  } else {

    max = 0;
    maxf = ABS(x[0]);

    BLASLONG n1 = n & -4;
    while (j < n1) {

      if (ABS(x[i]) > maxf) {
        max = j;
        maxf = ABS(x[i]);
      }
      if (ABS(x[i + inc_x]) > maxf) {
        max = j + 1;
        maxf = ABS(x[i + inc_x]);
      }
      if (ABS(x[i + 2 * inc_x]) > maxf) {
        max = j + 2;
        maxf = ABS(x[i + 2 * inc_x]);
      }
      if (ABS(x[i + 3 * inc_x]) > maxf) {
        max = j + 3;
        maxf = ABS(x[i + 3 * inc_x]);
      }

      i += inc_x * 4;

      j += 4;

    }

    while (j < n) {
      if (ABS(x[i]) > maxf) {
        max = j;
        maxf = ABS(x[i]);
      }
      i += inc_x;
      j++;
    }
    return (max + 1);
  }
}
