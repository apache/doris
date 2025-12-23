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

static void daxpy_kernel_32(BLASLONG n, FLOAT *x, FLOAT *y, FLOAT *alpha) {
  __asm__("vlrepg %%v0,%[alpha]\n\t"
    "srlg  %[n],%[n],5\n\t"
    "xgr   %%r1,%%r1\n\t"
    "0:\n\t"
    "pfd 1, 1024(%%r1,%[x])\n\t"
    "pfd 2, 1024(%%r1,%[y])\n\t"
    "vl  %%v16,0(%%r1,%[x])\n\t"
    "vl  %%v17,16(%%r1,%[x])\n\t"
    "vl  %%v18,32(%%r1,%[x])\n\t"
    "vl  %%v19,48(%%r1,%[x])\n\t"
    "vl  %%v20,0(%%r1,%[y])\n\t"
    "vl  %%v21,16(%%r1,%[y])\n\t"
    "vl  %%v22,32(%%r1,%[y])\n\t"
    "vl  %%v23,48(%%r1,%[y])\n\t"
    "vl  %%v24,64(%%r1,%[x])\n\t"
    "vl  %%v25,80(%%r1,%[x])\n\t"
    "vl  %%v26,96(%%r1,%[x])\n\t"
    "vl  %%v27,112(%%r1,%[x])\n\t"
    "vl  %%v28,64(%%r1,%[y])\n\t"
    "vl  %%v29,80(%%r1,%[y])\n\t"
    "vl  %%v30,96(%%r1,%[y])\n\t"
    "vl  %%v31,112(%%r1,%[y])\n\t"
    "vfmadb   %%v16,%%v0,%%v16,%%v20\n\t"
    "vfmadb   %%v17,%%v0,%%v17,%%v21\n\t"
    "vfmadb   %%v18,%%v0,%%v18,%%v22\n\t"
    "vfmadb   %%v19,%%v0,%%v19,%%v23\n\t"
    "vfmadb   %%v24,%%v0,%%v24,%%v28\n\t"
    "vfmadb   %%v25,%%v0,%%v25,%%v29\n\t"
    "vfmadb   %%v26,%%v0,%%v26,%%v30\n\t"
    "vfmadb   %%v27,%%v0,%%v27,%%v31\n\t"
    "vst  %%v16,0(%%r1,%[y])\n\t"
    "vst  %%v17,16(%%r1,%[y])\n\t"
    "vst  %%v18,32(%%r1,%[y])\n\t"
    "vst  %%v19,48(%%r1,%[y])\n\t"
    "vst  %%v24,64(%%r1,%[y])\n\t"
    "vst  %%v25,80(%%r1,%[y])\n\t"
    "vst  %%v26,96(%%r1,%[y])\n\t"
    "vst  %%v27,112(%%r1,%[y])\n\t"
    "vl  %%v16,128(%%r1,%[x])\n\t"
    "vl  %%v17,144(%%r1,%[x])\n\t"
    "vl  %%v18,160(%%r1,%[x])\n\t"
    "vl  %%v19,176(%%r1,%[x])\n\t"
    "vl  %%v20,128(%%r1,%[y])\n\t"
    "vl  %%v21,144(%%r1,%[y])\n\t"
    "vl  %%v22,160(%%r1,%[y])\n\t"
    "vl  %%v23,176(%%r1,%[y])\n\t"
    "vl  %%v24,192(%%r1,%[x])\n\t"
    "vl  %%v25,208(%%r1,%[x])\n\t"
    "vl  %%v26,224(%%r1,%[x])\n\t"
    "vl  %%v27,240(%%r1,%[x])\n\t"
    "vl  %%v28,192(%%r1,%[y])\n\t"
    "vl  %%v29,208(%%r1,%[y])\n\t"
    "vl  %%v30,224(%%r1,%[y])\n\t"
    "vl  %%v31,240(%%r1,%[y])\n\t"
    "vfmadb   %%v16,%%v0,%%v16,%%v20\n\t"
    "vfmadb   %%v17,%%v0,%%v17,%%v21\n\t"
    "vfmadb   %%v18,%%v0,%%v18,%%v22\n\t"
    "vfmadb   %%v19,%%v0,%%v19,%%v23\n\t"
    "vfmadb   %%v24,%%v0,%%v24,%%v28\n\t"
    "vfmadb   %%v25,%%v0,%%v25,%%v29\n\t"
    "vfmadb   %%v26,%%v0,%%v26,%%v30\n\t"
    "vfmadb   %%v27,%%v0,%%v27,%%v31\n\t"
    "vst  %%v16,128(%%r1,%[y])\n\t"
    "vst  %%v17,144(%%r1,%[y])\n\t"
    "vst  %%v18,160(%%r1,%[y])\n\t"
    "vst  %%v19,176(%%r1,%[y])\n\t"
    "vst  %%v24,192(%%r1,%[y])\n\t"
    "vst  %%v25,208(%%r1,%[y])\n\t"
    "vst  %%v26,224(%%r1,%[y])\n\t"
    "vst  %%v27,240(%%r1,%[y])\n\t"
    "agfi  %%r1,256\n\t"
    "brctg %[n],0b"
    : "+m"(*(FLOAT (*)[n]) y),[n] "+&r"(n)
    : [y] "a"(y), "m"(*(const FLOAT (*)[n]) x),[x] "a"(x),
       [alpha] "Q"(*alpha)
    : "cc", "r1", "v0", "v16", "v17", "v18", "v19", "v20", "v21", "v22",
       "v23", "v24", "v25", "v26", "v27", "v28", "v29", "v30", "v31");
}

int CNAME(BLASLONG n, BLASLONG dummy0, BLASLONG dummy1, FLOAT da, FLOAT *x,
          BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *dummy,
          BLASLONG dummy2) {
  BLASLONG i = 0;
  BLASLONG ix = 0, iy = 0;

  if (n <= 0)
    return 0;

  if ((inc_x == 1) && (inc_y == 1)) {

    BLASLONG n1 = n & -32;

    if (n1)
      daxpy_kernel_32(n1, x, y, &da);

    i = n1;
    while (i < n) {

      y[i] += da * x[i];
      i++;

    }
    return 0;

  }

  BLASLONG n1 = n & -4;

  while (i < n1) {

    FLOAT m1 = da * x[ix];
    FLOAT m2 = da * x[ix + inc_x];
    FLOAT m3 = da * x[ix + 2 * inc_x];
    FLOAT m4 = da * x[ix + 3 * inc_x];

    y[iy] += m1;
    y[iy + inc_y] += m2;
    y[iy + 2 * inc_y] += m3;
    y[iy + 3 * inc_y] += m4;

    ix += inc_x * 4;
    iy += inc_y * 4;
    i += 4;

  }

  while (i < n) {

    y[iy] += da * x[ix];
    ix += inc_x;
    iy += inc_y;
    i++;

  }
  return 0;

}
