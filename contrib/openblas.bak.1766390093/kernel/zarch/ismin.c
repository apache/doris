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

static BLASLONG ismin_kernel_64(BLASLONG n, FLOAT *x, FLOAT *min) {
  BLASLONG imin;

  __asm__("vl     %%v0,0(%[x])\n\t"
    "vleig  %%v1,0,0\n\t"
    "vleig  %%v1,2,1\n\t"
    "vleig  %%v2,1,0\n\t"
    "vleig  %%v2,3,1\n\t"
    "vrepig %%v3,32\n\t"
    "vzero  %%v4\n\t"
    "vleif  %%v24,0,0\n\t"
    "vleif  %%v24,1,1\n\t"
    "vleif  %%v24,2,2\n\t"
    "vleif  %%v24,3,3\n\t"
    "vleif  %%v25,4,0\n\t"
    "vleif  %%v25,5,1\n\t"
    "vleif  %%v25,6,2\n\t"
    "vleif  %%v25,7,3\n\t"
    "vleif  %%v26,8,0\n\t"
    "vleif  %%v26,9,1\n\t"
    "vleif  %%v26,10,2\n\t"
    "vleif  %%v26,11,3\n\t"
    "vleif  %%v27,12,0\n\t"
    "vleif  %%v27,13,1\n\t"
    "vleif  %%v27,14,2\n\t"
    "vleif  %%v27,15,3\n\t"
    "vleif  %%v28,16,0\n\t"
    "vleif  %%v28,17,1\n\t"
    "vleif  %%v28,18,2\n\t"
    "vleif  %%v28,19,3\n\t"
    "vleif  %%v29,20,0\n\t"
    "vleif  %%v29,21,1\n\t"
    "vleif  %%v29,22,2\n\t"
    "vleif  %%v29,23,3\n\t"
    "vleif  %%v30,24,0\n\t"
    "vleif  %%v30,25,1\n\t"
    "vleif  %%v30,26,2\n\t"
    "vleif  %%v30,27,3\n\t"
    "vleif  %%v31,28,0\n\t"
    "vleif  %%v31,29,1\n\t"
    "vleif  %%v31,30,2\n\t"
    "vleif  %%v31,31,3\n\t"
    "srlg  %[n],%[n],6\n\t"
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
    "vfchesb  %%v5,%%v17,%%v16\n\t"
    "vfchesb  %%v6,%%v19,%%v18\n\t"
    "vfchesb  %%v7,%%v21,%%v20\n\t"
    "vfchesb  %%v8,%%v23,%%v22\n\t"
    "vsel    %%v16,%%v16,%%v17,%%v5\n\t"
    "vsel    %%v5,%%v24,%%v25,%%v5\n\t"
    "vsel    %%v17,%%v18,%%v19,%%v6\n\t"
    "vsel    %%v6,%%v26,%%v27,%%v6\n\t"
    "vsel    %%v18,%%v20,%%v21,%%v7\n\t"
    "vsel    %%v7,%%v28,%%v29,%%v7\n\t"
    "vsel    %%v19,%%v22,%%v23,%%v8\n\t"
    "vsel    %%v8,%%v30,%%v31,%%v8\n\t"
    "vfchesb  %%v20,%%v17,%%v16\n\t"
    "vfchesb  %%v21,%%v19,%%v18\n\t"
    "vsel    %%v16,%%v16,%%v17,%%v20\n\t"
    "vsel    %%v5,%%v5,%%v6,%%v20\n\t"
    "vsel    %%v17,%%v18,%%v19,%%v21\n\t"
    "vsel    %%v6,%%v7,%%v8,%%v21\n\t"
    "vfchesb  %%v18,%%v17,%%v16\n\t"
    "vsel    %%v16,%%v16,%%v17,%%v18\n\t"
    "vsel    %%v5,%%v5,%%v6,%%v18\n\t"
    "vsegf   %%v6,%%v5\n\t"
    "vesrlg  %%v5,%%v5,32\n\t"
    "vag     %%v5,%%v5,%%v4\n\t"
    "vag     %%v6,%%v6,%%v4\n\t"
    "vfchesb  %%v7,%%v16,%%v0\n\t"
    "vsel    %%v0,%%v0,%%v16,%%v7\n\t"
    "vsegf   %%v8,%%v7\n\t"
    "vesrlg  %%v7,%%v7,32\n\t"
    "vsegf   %%v7,%%v7\n\t"
    "vsel    %%v1,%%v1,%%v5,%%v7\n\t"
    "vsel    %%v2,%%v2,%%v6,%%v8\n\t"
    "vag     %%v4,%%v4,%%v3\n\t"
    "vl  %%v16,128(%%r1,%[x])\n\t"
    "vl  %%v17,144(%%r1,%[x])\n\t"
    "vl  %%v18,160(%%r1,%[x])\n\t"
    "vl  %%v19,176(%%r1,%[x])\n\t"
    "vl  %%v20,192(%%r1,%[x])\n\t"
    "vl  %%v21,208(%%r1,%[x])\n\t"
    "vl  %%v22,224(%%r1,%[x])\n\t"
    "vl  %%v23,240(%%r1,%[x])\n\t"
    "vfchesb  %%v5,%%v17,%%v16\n\t"
    "vfchesb  %%v6,%%v19,%%v18\n\t"
    "vfchesb  %%v7,%%v21,%%v20\n\t"
    "vfchesb  %%v8,%%v23,%%v22\n\t"
    "vsel    %%v16,%%v16,%%v17,%%v5\n\t"
    "vsel    %%v5,%%v24,%%v25,%%v5\n\t"
    "vsel    %%v17,%%v18,%%v19,%%v6\n\t"
    "vsel    %%v6,%%v26,%%v27,%%v6\n\t"
    "vsel    %%v18,%%v20,%%v21,%%v7\n\t"
    "vsel    %%v7,%%v28,%%v29,%%v7\n\t"
    "vsel    %%v19,%%v22,%%v23,%%v8\n\t"
    "vsel    %%v8,%%v30,%%v31,%%v8\n\t"
    "vfchesb  %%v20,%%v17,%%v16\n\t"
    "vfchesb  %%v21,%%v19,%%v18\n\t"
    "vsel    %%v16,%%v16,%%v17,%%v20\n\t"
    "vsel    %%v5,%%v5,%%v6,%%v20\n\t"
    "vsel    %%v17,%%v18,%%v19,%%v21\n\t"
    "vsel    %%v6,%%v7,%%v8,%%v21\n\t"
    "vfchesb  %%v18,%%v17,%%v16\n\t"
    "vsel    %%v16,%%v16,%%v17,%%v18\n\t"
    "vsel    %%v5,%%v5,%%v6,%%v18\n\t"
    "vsegf   %%v6,%%v5\n\t"
    "vesrlg  %%v5,%%v5,32\n\t"
    "vag     %%v5,%%v5,%%v4\n\t"
    "vag     %%v6,%%v6,%%v4\n\t"
    "vfchesb  %%v7,%%v16,%%v0\n\t"
    "vsel    %%v0,%%v0,%%v16,%%v7\n\t"
    "vsegf   %%v8,%%v7\n\t"
    "vesrlg  %%v7,%%v7,32\n\t"
    "vsegf   %%v7,%%v7\n\t"
    "vsel    %%v1,%%v1,%%v5,%%v7\n\t"
    "vsel    %%v2,%%v2,%%v6,%%v8\n\t"
    "vag     %%v4,%%v4,%%v3\n\t"
    "agfi    %%r1, 256\n\t"
    "brctg   %[n], 0b\n\t"
    "veslg   %%v3,%%v0,32\n\t"
    "vfchsb  %%v4,%%v3,%%v0\n\t"
    "vchlg   %%v5,%%v2,%%v1\n\t"
    "vfcesb  %%v6,%%v0,%%v3\n\t"
    "vn      %%v5,%%v5,%%v6\n\t"
    "vo      %%v4,%%v4,%%v5\n\t"
    "vsel    %%v0,%%v0,%%v3,%%v4\n\t"
    "vesrlg  %%v4,%%v4,32\n\t"
    "vsegf   %%v4,%%v4\n\t"
    "vsel    %%v1,%%v1,%%v2,%%v4\n\t"
    "vrepf  %%v2,%%v0,2\n\t"
    "vrepg  %%v3,%%v1,1\n\t"
    "wfcsb  %%v2,%%v0\n\t"
    "jne 1f\n\t"
    "vstef  %%v0,%[min],0\n\t"
    "vmnlg  %%v0,%%v1,%%v3\n\t"
    "vlgvg  %[imin],%%v0,0\n\t"
    "j 2f\n\t"
    "1:\n\t"
    "wfchsb %%v4,%%v0,%%v2\n\t"
    "vesrlg %%v4,%%v4,32\n\t"
    "vsegf  %%v4,%%v4\n\t"
    "vsel   %%v1,%%v3,%%v1,%%v4\n\t"
    "vsel   %%v0,%%v2,%%v0,%%v4\n\t"
    "ste    %%f0,%[min]\n\t"
    "vlgvg  %[imin],%%v1,0\n\t"
    "2:\n\t"
    "nop 0"
    : [imin] "=r"(imin),[min] "=Q"(*min),[n] "+&r"(n)
    : "m"(*(const FLOAT (*)[n]) x),[x] "a"(x)
    : "cc", "r1", "v0", "v1", "v2", "v4", "v5", "v6", "v7", "v8", "v16",
       "v17", "v18", "v19", "v20", "v21", "v22", "v23", "v24", "v25", "v26",
       "v27", "v28", "v29", "v30", "v31");

  return imin;
}

BLASLONG CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x) {
  BLASLONG i = 0;
  BLASLONG j = 0;
  FLOAT minf = 0.0;
  BLASLONG min = 0;

  if (n <= 0 || inc_x <= 0)
    return (min);

  if (inc_x == 1) {

    BLASLONG n1 = n & -64;
    if (n1 > 0) {

      min = ismin_kernel_64(n1, x, &minf);

      i = n1;
    } else {
      minf = x[0];
      i++;
    }

    while (i < n) {
      if (x[i] < minf) {
        min = i;
        minf = x[i];
      }
      i++;
    }
    return (min + 1);

  } else {

    min = 0;
    minf = x[0];

    BLASLONG n1 = n & -4;
    while (j < n1) {

      if (x[i] < minf) {
        min = j;
        minf = x[i];
      }
      if (x[i + inc_x] < minf) {
        min = j + 1;
        minf = x[i + inc_x];
      }
      if (x[i + 2 * inc_x] < minf) {
        min = j + 2;
        minf = x[i + 2 * inc_x];
      }
      if (x[i + 3 * inc_x] < minf) {
        min = j + 3;
        minf = x[i + 3 * inc_x];
      }

      i += inc_x * 4;

      j += 4;

    }

    while (j < n) {
      if (x[i] < minf) {
        min = j;
        minf = x[i];
      }
      i += inc_x;
      j++;
    }
    return (min + 1);
  }
}
