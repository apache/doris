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

static void caxpy_kernel_16(BLASLONG n, FLOAT *x, FLOAT *y, FLOAT *alpha) {
  __asm__(
#if !defined(CONJ)
    "vlrepf %%v0,0(%[alpha])\n\t"
    "vlef   %%v1,4(%[alpha]),0\n\t"
    "vlef   %%v1,4(%[alpha]),2\n\t"
    "vflcsb %%v1,%%v1\n\t"
    "vlef   %%v1,4(%[alpha]),1\n\t"
    "vlef   %%v1,4(%[alpha]),3\n\t"
#else
    "vlef   %%v0,0(%[alpha]),1\n\t"
    "vlef   %%v0,0(%[alpha]),3\n\t"
    "vflcsb %%v0,%%v0\n\t"
    "vlef   %%v0,0(%[alpha]),0\n\t"
    "vlef   %%v0,0(%[alpha]),2\n\t"
    "vlrepf %%v1,4(%[alpha])\n\t"
#endif
    "srlg %[n],%[n],4\n\t"
    "xgr  %%r1,%%r1\n\t"
    "0:\n\t"
    "pfd 1, 1024(%%r1,%[x])\n\t"
    "pfd 2, 1024(%%r1,%[y])\n\t"
    "vl   %%v8,0(%%r1,%[x])\n\t"
    "vl   %%v9,16(%%r1,%[x])\n\t"
    "vl   %%v10,32(%%r1,%[x])\n\t"
    "vl   %%v11,48(%%r1,%[x])\n\t"
    "vl   %%v12,0(%%r1,%[y])\n\t"
    "vl   %%v13,16(%%r1,%[y])\n\t"
    "vl   %%v14,32(%%r1,%[y])\n\t"
    "vl   %%v15,48(%%r1,%[y])\n\t"
    "vl   %%v16,64(%%r1,%[x])\n\t"
    "vl   %%v17,80(%%r1,%[x])\n\t"
    "vl   %%v18,96(%%r1,%[x])\n\t"
    "vl   %%v19,112(%%r1,%[x])\n\t"
    "vl   %%v20,64(%%r1,%[y])\n\t"
    "vl   %%v21,80(%%r1,%[y])\n\t"
    "vl   %%v22,96(%%r1,%[y])\n\t"
    "vl   %%v23,112(%%r1,%[y])\n\t"
    "verllg   %%v24,%%v8,32\n\t"
    "verllg   %%v25,%%v9,32\n\t"
    "verllg   %%v26,%%v10,32\n\t"
    "verllg   %%v27,%%v11,32\n\t"
    "verllg   %%v28,%%v16,32\n\t"
    "verllg   %%v29,%%v17,32\n\t"
    "verllg   %%v30,%%v18,32\n\t"
    "verllg   %%v31,%%v19,32\n\t"
    "vfmasb %%v8,%%v8,%%v0,%%v12\n\t"
    "vfmasb %%v9,%%v9,%%v0,%%v13\n\t"
    "vfmasb %%v10,%%v10,%%v0,%%v14\n\t"
    "vfmasb %%v11,%%v11,%%v0,%%v15\n\t"
    "vfmasb %%v16,%%v16,%%v0,%%v20\n\t"
    "vfmasb %%v17,%%v17,%%v0,%%v21\n\t"
    "vfmasb %%v18,%%v18,%%v0,%%v22\n\t"
    "vfmasb %%v19,%%v19,%%v0,%%v23\n\t"
    "vfmasb %%v8,%%v24,%%v1,%%v8\n\t"
    "vfmasb %%v9,%%v25,%%v1,%%v9\n\t"
    "vfmasb %%v10,%%v26,%%v1,%%v10\n\t"
    "vfmasb %%v11,%%v27,%%v1,%%v11\n\t"
    "vfmasb %%v16,%%v28,%%v1,%%v16\n\t"
    "vfmasb %%v17,%%v29,%%v1,%%v17\n\t"
    "vfmasb %%v18,%%v30,%%v1,%%v18\n\t"
    "vfmasb %%v19,%%v31,%%v1,%%v19\n\t"
    "vst %%v8,0(%%r1,%[y])\n\t"
    "vst %%v9,16(%%r1,%[y])\n\t"
    "vst %%v10,32(%%r1,%[y])\n\t"
    "vst %%v11,48(%%r1,%[y])\n\t"
    "vst %%v16,64(%%r1,%[y])\n\t"
    "vst %%v17,80(%%r1,%[y])\n\t"
    "vst %%v18,96(%%r1,%[y])\n\t"
    "vst %%v19,112(%%r1,%[y])\n\t"
    "agfi  %%r1,128\n\t"
    "brctg %[n],0b"
    : "+m"(*(FLOAT (*)[n * 2]) y),[n] "+&r"(n)
    : [y] "a"(y), "m"(*(const FLOAT (*)[n * 2]) x),[x] "a"(x),
       "m"(*(const FLOAT (*)[2]) alpha),[alpha] "a"(alpha)
    : "cc", "r1", "v0", "v1", "v8", "v9", "v10", "v11", "v12", "v13",
       "v14", "v15", "v16", "v17", "v18", "v19", "v20", "v21", "v22", "v23",
       "v24", "v25", "v26", "v27", "v28", "v29", "v30", "v31");
}

int CNAME(BLASLONG n, BLASLONG dummy0, BLASLONG dummy1, FLOAT da_r, FLOAT da_i,
          FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *dummy,
          BLASLONG dummy2) {
  BLASLONG i = 0;
  BLASLONG ix = 0, iy = 0;
  FLOAT da[2] __attribute__ ((aligned(16)));

  if (n <= 0)
    return (0);

  if ((inc_x == 1) && (inc_y == 1)) {

    BLASLONG n1 = n & -16;

    if (n1) {
      da[0] = da_r;
      da[1] = da_i;
      caxpy_kernel_16(n1, x, y, da);
      ix = 2 * n1;
    }
    i = n1;
    while (i < n) {
#if !defined(CONJ)
      y[ix] += (da_r * x[ix] - da_i * x[ix + 1]);
      y[ix + 1] += (da_r * x[ix + 1] + da_i * x[ix]);
#else
      y[ix] += (da_r * x[ix] + da_i * x[ix + 1]);
      y[ix + 1] -= (da_r * x[ix + 1] - da_i * x[ix]);
#endif
      i++;
      ix += 2;

    }
    return (0);

  }

  inc_x *= 2;
  inc_y *= 2;

  while (i < n) {

#if !defined(CONJ)
    y[iy] += (da_r * x[ix] - da_i * x[ix + 1]);
    y[iy + 1] += (da_r * x[ix + 1] + da_i * x[ix]);
#else
    y[iy] += (da_r * x[ix] + da_i * x[ix + 1]);
    y[iy + 1] -= (da_r * x[ix + 1] - da_i * x[ix]);
#endif
    ix += inc_x;
    iy += inc_y;
    i++;

  }
  return (0);

}
