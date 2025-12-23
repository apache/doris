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

static FLOAT ddot_kernel_16(BLASLONG n, FLOAT *x, FLOAT *y) {
  FLOAT dot;

  __asm__("vzero %%v0\n\t"
    "vzero %%v1\n\t"
    "vzero %%v2\n\t"
    "vzero %%v3\n\t"
    "vzero %%v4\n\t"
    "vzero %%v5\n\t"
    "vzero %%v6\n\t"
    "vzero %%v7\n\t"
    "srlg  %[n],%[n],4\n\t"
    "xgr   %%r1,%%r1\n\t"
    "0:\n\t"
    "pfd 1,1024(%%r1,%[x])\n\t"
    "pfd 1,1024(%%r1,%[y])\n\t"
    "vl  %%v16,0(%%r1,%[x])\n\t"
    "vl  %%v17,16(%%r1,%[x])\n\t"
    "vl  %%v18,32(%%r1,%[x])\n\t"
    "vl  %%v19,48(%%r1,%[x])\n\t"
    "vl  %%v20,64(%%r1,%[x])\n\t"
    "vl  %%v21,80(%%r1,%[x])\n\t"
    "vl  %%v22,96(%%r1,%[x])\n\t"
    "vl  %%v23,112(%%r1,%[x])\n\t"
    "vl  %%v24,0(%%r1,%[y])\n\t"
    "vl  %%v25,16(%%r1,%[y])\n\t"
    "vl  %%v26,32(%%r1,%[y])\n\t"
    "vl  %%v27,48(%%r1,%[y])\n\t"
    "vl  %%v28,64(%%r1,%[y])\n\t"
    "vl  %%v29,80(%%r1,%[y])\n\t"
    "vl  %%v30,96(%%r1,%[y])\n\t"
    "vl  %%v31,112(%%r1,%[y])\n\t"
    "vfmadb   %%v0,%%v16,%%v24,%%v0\n\t"
    "vfmadb   %%v1,%%v17,%%v25,%%v1\n\t"
    "vfmadb   %%v2,%%v18,%%v26,%%v2\n\t"
    "vfmadb   %%v3,%%v19,%%v27,%%v3\n\t"
    "vfmadb   %%v4,%%v20,%%v28,%%v4\n\t"
    "vfmadb   %%v5,%%v21,%%v29,%%v5\n\t"
    "vfmadb   %%v6,%%v22,%%v30,%%v6\n\t"
    "vfmadb   %%v7,%%v23,%%v31,%%v7\n\t"
    "agfi   %%r1,128\n\t"
    "brctg  %[n],0b\n\t"
    "vfadb   %%v0,%%v0,%%v1\n\t"
    "vfadb   %%v0,%%v0,%%v2\n\t"
    "vfadb   %%v0,%%v0,%%v3\n\t"
    "vfadb   %%v0,%%v0,%%v4\n\t"
    "vfadb   %%v0,%%v0,%%v5\n\t"
    "vfadb   %%v0,%%v0,%%v6\n\t"
    "vfadb   %%v0,%%v0,%%v7\n\t"
    "vrepg  %%v1,%%v0,1\n\t"
    "adbr   %%f0,%%f1\n\t"
    "ldr    %[dot],%%f0"
    : [dot] "=f"(dot),[n] "+&r"(n)
    : "m"(*(const FLOAT (*)[n]) x),[x] "a"(x),
       "m"(*(const FLOAT (*)[n]) y),[y] "a"(y)
    : "cc", "r1", "v0", "v1", "v2", "v3", "v4", "v5", "v6", "v7", "v16",
       "v17", "v18", "v19", "v20", "v21", "v22", "v23", "v24", "v25", "v26",
       "v27", "v28", "v29", "v30", "v31");

  return dot;
}

FLOAT CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y) {
  BLASLONG i = 0;
  BLASLONG ix = 0, iy = 0;

  FLOAT dot = 0.0;

  if (n <= 0)
    return (dot);

  if ((inc_x == 1) && (inc_y == 1)) {

    BLASLONG n1 = n & -16;

    if (n1)
      dot = ddot_kernel_16(n1, x, y);

    i = n1;
    while (i < n) {

      dot += y[i] * x[i];
      i++;

    }
    return (dot);

  }

  FLOAT temp1 = 0.0;
  FLOAT temp2 = 0.0;

  BLASLONG n1 = n & -4;

  while (i < n1) {

    FLOAT m1 = y[iy] * x[ix];
    FLOAT m2 = y[iy + inc_y] * x[ix + inc_x];

    FLOAT m3 = y[iy + 2 * inc_y] * x[ix + 2 * inc_x];
    FLOAT m4 = y[iy + 3 * inc_y] * x[ix + 3 * inc_x];

    ix += inc_x * 4;
    iy += inc_y * 4;

    temp1 += m1 + m3;
    temp2 += m2 + m4;

    i += 4;

  }

  while (i < n) {

    temp1 += y[iy] * x[ix];
    ix += inc_x;
    iy += inc_y;
    i++;

  }
  dot = temp1 + temp2;
  return (dot);

}
