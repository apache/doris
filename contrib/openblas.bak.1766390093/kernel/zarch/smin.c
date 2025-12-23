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

static FLOAT smin_kernel_64(BLASLONG n, FLOAT *x) {
  FLOAT min;

  __asm__("vl     %%v0,0(%[x])\n\t"
    "srlg   %[n],%[n],6\n\t"
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
    "vl  %%v24,128(%%r1,%[x])\n\t"
    "vl  %%v25,144(%%r1,%[x])\n\t"
    "vl  %%v26,160(%%r1,%[x])\n\t"
    "vl  %%v27,176(%%r1,%[x])\n\t"
    "vl  %%v28,192(%%r1,%[x])\n\t"
    "vl  %%v29,208(%%r1,%[x])\n\t"
    "vl  %%v30,224(%%r1,%[x])\n\t"
    "vl  %%v31,240(%%r1,%[x])\n\t"
    "vfminsb  %%v16,%%v16,%%v24,0\n\t"
    "vfminsb  %%v17,%%v17,%%v25,0\n\t"
    "vfminsb  %%v18,%%v18,%%v26,0\n\t"
    "vfminsb  %%v19,%%v19,%%v27,0\n\t"
    "vfminsb  %%v20,%%v20,%%v28,0\n\t"
    "vfminsb  %%v21,%%v21,%%v29,0\n\t"
    "vfminsb  %%v22,%%v22,%%v30,0\n\t"
    "vfminsb  %%v23,%%v23,%%v31,0\n\t"
    "vfminsb  %%v16,%%v16,%%v20,0\n\t"
    "vfminsb  %%v17,%%v17,%%v21,0\n\t"
    "vfminsb  %%v18,%%v18,%%v22,0\n\t"
    "vfminsb  %%v19,%%v19,%%v23,0\n\t"
    "vfminsb  %%v16,%%v16,%%v18,0\n\t"
    "vfminsb  %%v17,%%v17,%%v19,0\n\t"
    "vfminsb  %%v16,%%v16,%%v17,0\n\t"
    "vfminsb  %%v0,%%v0,%%v16,0\n\t"
    "agfi    %%r1, 256\n\t"
    "brctg   %[n], 0b\n\t"
    "veslg   %%v16,%%v0,32\n\t"
    "vfminsb %%v0,%%v0,%%v16,0\n\t"
    "vrepf   %%v16,%%v0,2\n\t"
    "wfminsb %%v0,%%v0,%%v16,0\n\t"
    "ler    %[min],%%f0"
    : [min] "=f"(min),[n] "+&r"(n)
    : "m"(*(const FLOAT (*)[n]) x),[x] "a"(x)
    : "cc", "r1", "v0", "v16", "v17", "v18", "v19", "v20", "v21", "v22",
       "v23", "v24", "v25", "v26", "v27", "v28", "v29", "v30", "v31");

  return min;
}

FLOAT CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x) {
  BLASLONG i = 0;
  BLASLONG j = 0;
  FLOAT minf = 0.0;

  if (n <= 0 || inc_x <= 0)
    return (minf);

  if (inc_x == 1) {

    BLASLONG n1 = n & -64;
    if (n1 > 0) {

      minf = smin_kernel_64(n1, x);

      i = n1;
    } else {
      minf = x[0];
      i++;
    }

    while (i < n) {
      if (x[i] < minf) {
        minf = x[i];
      }
      i++;
    }
    return (minf);

  } else {

    minf = x[0];

    BLASLONG n1 = n & -4;
    while (j < n1) {

      if (x[i] < minf) {
        minf = x[i];
      }
      if (x[i + inc_x] < minf) {
        minf = x[i + inc_x];
      }
      if (x[i + 2 * inc_x] < minf) {
        minf = x[i + 2 * inc_x];
      }
      if (x[i + 3 * inc_x] < minf) {
        minf = x[i + 3 * inc_x];
      }

      i += inc_x * 4;

      j += 4;

    }

    while (j < n) {
      if (x[i] < minf) {
        minf = x[i];
      }
      i += inc_x;
      j++;
    }
    return (minf);
  }
}
