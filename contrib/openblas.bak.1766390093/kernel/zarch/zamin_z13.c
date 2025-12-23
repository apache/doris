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

#define CABS1(x,i) (fabs(x[i]) + fabs(x[i + 1]))

static FLOAT zamin_kernel_16(BLASLONG n, FLOAT *x) {
  FLOAT amin;

  __asm__("vleg   %%v0,0(%[x]),0\n\t"
    "vleg   %%v16,8(%[x]),0\n\t"
    "vleg   %%v0,16(%[x]),1\n\t"
    "vleg   %%v16,24(%[x]),1\n\t"
    "vflpdb %%v0,%%v0\n\t"
    "vflpdb %%v16,%%v16\n\t"
    "vfadb  %%v0,%%v0,%%v16\n\t"
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
    "vfchdb  %%v24,%%v17,%%v16\n\t"
    "vfchdb  %%v25,%%v19,%%v18\n\t"
    "vsel    %%v24,%%v16,%%v17,%%v24\n\t"
    "vsel    %%v25,%%v18,%%v19,%%v25\n\t"
    "vfchdb  %%v26,%%v25,%%v24\n\t"
    "vsel    %%v26,%%v24,%%v25,%%v26\n\t"
    "vfchdb  %%v27,%%v0,%%v26\n\t"
    "vsel    %%v0,%%v26,%%v0,%%v27\n\t"
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
    "vfchdb  %%v24,%%v17,%%v16\n\t"
    "vfchdb  %%v25,%%v19,%%v18\n\t"
    "vsel    %%v24,%%v16,%%v17,%%v24\n\t"
    "vsel    %%v25,%%v18,%%v19,%%v25\n\t"
    "vfchdb  %%v26,%%v25,%%v24\n\t"
    "vsel    %%v26,%%v24,%%v25,%%v26\n\t"
    "vfchdb  %%v27,%%v0,%%v26\n\t"
    "vsel    %%v0,%%v26,%%v0,%%v27\n\t"
    "agfi    %%r1, 256\n\t"
    "brctg   %[n], 0b\n\t"
    "vrepg  %%v16,%%v0,1\n\t"
    "wfchdb %%v17,%%v16,%%v0\n\t"
    "vsel   %%v0,%%v0,%%v16,%%v17\n\t"
    "ldr    %[amin],%%f0"
    : [amin] "=f"(amin),[n] "+&r"(n)
    : "m"(*(const FLOAT (*)[n * 2]) x),[x] "a"(x)
    : "cc", "r1", "v0", "v16", "v17", "v18", "v19", "v20", "v21", "v22",
       "v23", "v24", "v25", "v26", "v27");

  return amin;
}

FLOAT CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x) {
  BLASLONG i = 0;
  BLASLONG ix = 0;
  FLOAT minf = 0.0;
  BLASLONG inc_x2;

  if (n <= 0 || inc_x <= 0)
    return (minf);

  if (inc_x == 1) {

    BLASLONG n1 = n & -16;
    if (n1 > 0) {

      minf = zamin_kernel_16(n1, x);
      ix = n1 * 2;
      i = n1;
    } else {
      minf = CABS1(x, 0);
      ix += 2;
      i++;
    }

    while (i < n) {
      if (CABS1(x, ix) < minf) {
        minf = CABS1(x, ix);
      }
      ix += 2;
      i++;
    }
    return (minf);

  } else {

    minf = CABS1(x, 0);
    inc_x2 = 2 * inc_x;

    BLASLONG n1 = n & -4;
    while (i < n1) {

      if (CABS1(x, ix) < minf) {
        minf = CABS1(x, ix);
      }
      if (CABS1(x, ix + inc_x2) < minf) {
        minf = CABS1(x, ix + inc_x2);
      }
      if (CABS1(x, ix + inc_x2 * 2) < minf) {
        minf = CABS1(x, ix + inc_x2 * 2);
      }
      if (CABS1(x, ix + inc_x2 * 3) < minf) {
        minf = CABS1(x, ix + inc_x2 * 3);
      }

      ix += inc_x2 * 4;

      i += 4;

    }

    while (i < n) {
      if (CABS1(x, ix) < minf) {
        minf = CABS1(x, ix);
      }
      ix += inc_x2;
      i++;
    }
    return (minf);
  }
}
