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

#define NBMAX 2048

static void dgemv_kernel_4x4(BLASLONG n, FLOAT **ap, FLOAT *x, FLOAT *y,
                             FLOAT *alpha) {
  register FLOAT *ap0 = ap[0];
  register FLOAT *ap1 = ap[1];
  register FLOAT *ap2 = ap[2];
  register FLOAT *ap3 = ap[3];

  __asm__("vlrepg %%v0,0(%[x])\n\t"
    "vlrepg %%v1,8(%[x])\n\t"
    "vlrepg %%v2,16(%[x])\n\t"
    "vlrepg %%v3,24(%[x])\n\t"
    "vlrepg %%v4,%[alpha]\n\t"
    "vfmdb  %%v0,%%v0,%%v4\n\t"
    "vfmdb  %%v1,%%v1,%%v4\n\t"
    "vfmdb  %%v2,%%v2,%%v4\n\t"
    "vfmdb  %%v3,%%v3,%%v4\n\t"
    "xgr   %%r1,%%r1\n\t"
    "lghi    %%r0,-16\n\t"
    "ngr     %%r0,%[n]\n\t"
    "ltgr    %%r0,%%r0\n\t"
    "jz      1f\n\t"
    "srlg  %%r0,%%r0,4\n\t"
    "0:\n\t"
    "pfd 1,1024(%%r1,%[ap0])\n\t"
    "pfd 1,1024(%%r1,%[ap1])\n\t"
    "pfd 1,1024(%%r1,%[ap2])\n\t"
    "pfd 1,1024(%%r1,%[ap3])\n\t"
    "pfd 2,1024(%%r1,%[y])\n\t"
    "vl  %%v16,0(%%r1,%[ap0])\n\t"
    "vl  %%v17,0(%%r1,%[ap1])\n\t"
    "vl  %%v18,0(%%r1,%[ap2])\n\t"
    "vl  %%v19,0(%%r1,%[ap3])\n\t"
    "vl  %%v20,16(%%r1,%[ap0])\n\t"
    "vl  %%v21,16(%%r1,%[ap1])\n\t"
    "vl  %%v22,16(%%r1,%[ap2])\n\t"
    "vl  %%v23,16(%%r1,%[ap3])\n\t"
    "vl  %%v24,32(%%r1,%[ap0])\n\t"
    "vl  %%v25,32(%%r1,%[ap1])\n\t"
    "vl  %%v26,32(%%r1,%[ap2])\n\t"
    "vl  %%v27,32(%%r1,%[ap3])\n\t"
    "vl  %%v28,48(%%r1,%[ap0])\n\t"
    "vl  %%v29,48(%%r1,%[ap1])\n\t"
    "vl  %%v30,48(%%r1,%[ap2])\n\t"
    "vl  %%v31,48(%%r1,%[ap3])\n\t"
    "vl  %%v4,0(%%r1,%[y])\n\t"
    "vl  %%v5,16(%%r1,%[y])\n\t"
    "vl  %%v6,32(%%r1,%[y])\n\t"
    "vl  %%v7,48(%%r1,%[y])\n\t"
    "vfmadb   %%v4,%%v16,%%v0,%%v4\n\t"
    "vfmadb   %%v5,%%v20,%%v0,%%v5\n\t"
    "vfmadb   %%v6,%%v24,%%v0,%%v6\n\t"
    "vfmadb   %%v7,%%v28,%%v0,%%v7\n\t"
    "vfmadb   %%v4,%%v17,%%v1,%%v4\n\t"
    "vfmadb   %%v5,%%v21,%%v1,%%v5\n\t"
    "vfmadb   %%v6,%%v25,%%v1,%%v6\n\t"
    "vfmadb   %%v7,%%v29,%%v1,%%v7\n\t"
    "vfmadb   %%v4,%%v18,%%v2,%%v4\n\t"
    "vfmadb   %%v5,%%v22,%%v2,%%v5\n\t"
    "vfmadb   %%v6,%%v26,%%v2,%%v6\n\t"
    "vfmadb   %%v7,%%v30,%%v2,%%v7\n\t"
    "vfmadb   %%v4,%%v19,%%v3,%%v4\n\t"
    "vfmadb   %%v5,%%v23,%%v3,%%v5\n\t"
    "vfmadb   %%v6,%%v27,%%v3,%%v6\n\t"
    "vfmadb   %%v7,%%v31,%%v3,%%v7\n\t"
    "vst %%v4,0(%%r1,%[y])\n\t"
    "vst %%v5,16(%%r1,%[y])\n\t"
    "vst %%v6,32(%%r1,%[y])\n\t"
    "vst %%v7,48(%%r1,%[y])\n\t"
    "vl  %%v16,64(%%r1,%[ap0])\n\t"
    "vl  %%v17,64(%%r1,%[ap1])\n\t"
    "vl  %%v18,64(%%r1,%[ap2])\n\t"
    "vl  %%v19,64(%%r1,%[ap3])\n\t"
    "vl  %%v20,80(%%r1,%[ap0])\n\t"
    "vl  %%v21,80(%%r1,%[ap1])\n\t"
    "vl  %%v22,80(%%r1,%[ap2])\n\t"
    "vl  %%v23,80(%%r1,%[ap3])\n\t"
    "vl  %%v24,96(%%r1,%[ap0])\n\t"
    "vl  %%v25,96(%%r1,%[ap1])\n\t"
    "vl  %%v26,96(%%r1,%[ap2])\n\t"
    "vl  %%v27,96(%%r1,%[ap3])\n\t"
    "vl  %%v28,112(%%r1,%[ap0])\n\t"
    "vl  %%v29,112(%%r1,%[ap1])\n\t"
    "vl  %%v30,112(%%r1,%[ap2])\n\t"
    "vl  %%v31,112(%%r1,%[ap3])\n\t"
    "vl  %%v4,64(%%r1,%[y])\n\t"
    "vl  %%v5,80(%%r1,%[y])\n\t"
    "vl  %%v6,96(%%r1,%[y])\n\t"
    "vl  %%v7,112(%%r1,%[y])\n\t"
    "vfmadb   %%v4,%%v16,%%v0,%%v4\n\t"
    "vfmadb   %%v5,%%v20,%%v0,%%v5\n\t"
    "vfmadb   %%v6,%%v24,%%v0,%%v6\n\t"
    "vfmadb   %%v7,%%v28,%%v0,%%v7\n\t"
    "vfmadb   %%v4,%%v17,%%v1,%%v4\n\t"
    "vfmadb   %%v5,%%v21,%%v1,%%v5\n\t"
    "vfmadb   %%v6,%%v25,%%v1,%%v6\n\t"
    "vfmadb   %%v7,%%v29,%%v1,%%v7\n\t"
    "vfmadb   %%v4,%%v18,%%v2,%%v4\n\t"
    "vfmadb   %%v5,%%v22,%%v2,%%v5\n\t"
    "vfmadb   %%v6,%%v26,%%v2,%%v6\n\t"
    "vfmadb   %%v7,%%v30,%%v2,%%v7\n\t"
    "vfmadb   %%v4,%%v19,%%v3,%%v4\n\t"
    "vfmadb   %%v5,%%v23,%%v3,%%v5\n\t"
    "vfmadb   %%v6,%%v27,%%v3,%%v6\n\t"
    "vfmadb   %%v7,%%v31,%%v3,%%v7\n\t"
    "vst %%v4,64(%%r1,%[y])\n\t"
    "vst %%v5,80(%%r1,%[y])\n\t"
    "vst %%v6,96(%%r1,%[y])\n\t"
    "vst %%v7,112(%%r1,%[y])\n\t"
    "agfi   %%r1,128\n\t"
    "brctg  %%r0,0b\n\t"
    "1:\n\t"
    "lghi    %%r0,12\n\t"
    "ngr     %%r0,%[n]\n\t"
    "ltgr    %%r0,%%r0\n\t"
    "jz      3f\n\t"
    "srlg  %%r0,%%r0,2\n\t"
    "2:\n\t"
    "vl  %%v16,0(%%r1,%[ap0])\n\t"
    "vl  %%v17,0(%%r1,%[ap1])\n\t"
    "vl  %%v18,0(%%r1,%[ap2])\n\t"
    "vl  %%v19,0(%%r1,%[ap3])\n\t"
    "vl  %%v20,16(%%r1,%[ap0])\n\t"
    "vl  %%v21,16(%%r1,%[ap1])\n\t"
    "vl  %%v22,16(%%r1,%[ap2])\n\t"
    "vl  %%v23,16(%%r1,%[ap3])\n\t"
    "vl  %%v4,0(%%r1,%[y])\n\t"
    "vl  %%v5,16(%%r1,%[y])\n\t"
    "vfmadb   %%v4,%%v16,%%v0,%%v4\n\t"
    "vfmadb   %%v5,%%v20,%%v0,%%v5\n\t"
    "vfmadb   %%v4,%%v17,%%v1,%%v4\n\t"
    "vfmadb   %%v5,%%v21,%%v1,%%v5\n\t"
    "vfmadb   %%v4,%%v18,%%v2,%%v4\n\t"
    "vfmadb   %%v5,%%v22,%%v2,%%v5\n\t"
    "vfmadb   %%v4,%%v19,%%v3,%%v4\n\t"
    "vfmadb   %%v5,%%v23,%%v3,%%v5\n\t"
    "vst %%v4,0(%%r1,%[y])\n\t"
    "vst %%v5,16(%%r1,%[y])\n\t"
    "agfi   %%r1,32\n\t"
    "brctg  %%r0,2b\n\t"
    "3:\n\t"
    "nop 0"
    : "+m"(*(FLOAT (*)[n]) y)
    : [y] "a"(y), "m"(*(const FLOAT (*)[n]) ap0),[ap0] "a"(ap0),
       "m"(*(const FLOAT (*)[n]) ap1),[ap1] "a"(ap1),
       "m"(*(const FLOAT (*)[n]) ap2),[ap2] "a"(ap2),
       "m"(*(const FLOAT (*)[n]) ap3),[ap3] "a"(ap3),
       "m"(*(const FLOAT (*)[4]) x),[x] "a"(x),[alpha] "Q"(*alpha),
       [n] "r"(n)
    : "cc", "r0", "r1", "v0", "v1", "v2", "v3", "v4", "v5", "v6", "v7",
       "v16", "v17", "v18", "v19", "v20", "v21", "v22", "v23", "v24", "v25",
       "v26", "v27", "v28", "v29", "v30", "v31");
}

static void dgemv_kernel_4x2(BLASLONG n, FLOAT **ap, FLOAT *x, FLOAT *y,
                             FLOAT *alpha) {
  register FLOAT *ap0 = ap[0];
  register FLOAT *ap1 = ap[1];

  __asm__("vlrepg %%v0,0(%[x])\n\t"
    "vlrepg %%v1,8(%[x])\n\t"
    "vlrepg %%v2,%[alpha]\n\t"
    "vfmdb  %%v0,%%v0,%%v2\n\t"
    "vfmdb  %%v1,%%v1,%%v2\n\t"
    "xgr   %%r1,%%r1\n\t"
    "lghi    %%r0,-16\n\t"
    "ngr     %%r0,%[n]\n\t"
    "ltgr    %%r0,%%r0\n\t"
    "jz      1f\n\t"
    "srlg  %%r0,%%r0,4\n\t"
    "0:\n\t"
    "pfd 1,1024(%%r1,%[ap0])\n\t"
    "pfd 1,1024(%%r1,%[ap1])\n\t"
    "pfd 2,1024(%%r1,%[y])\n\t"
    "vl  %%v16,0(%%r1,%[ap0])\n\t"
    "vl  %%v17,0(%%r1,%[ap1])\n\t"
    "vl  %%v18,16(%%r1,%[ap0])\n\t"
    "vl  %%v19,16(%%r1,%[ap1])\n\t"
    "vl  %%v20,32(%%r1,%[ap0])\n\t"
    "vl  %%v21,32(%%r1,%[ap1])\n\t"
    "vl  %%v22,48(%%r1,%[ap0])\n\t"
    "vl  %%v23,48(%%r1,%[ap1])\n\t"
    "vl  %%v24,64(%%r1,%[ap0])\n\t"
    "vl  %%v25,64(%%r1,%[ap1])\n\t"
    "vl  %%v26,80(%%r1,%[ap0])\n\t"
    "vl  %%v27,80(%%r1,%[ap1])\n\t"
    "vl  %%v28,96(%%r1,%[ap0])\n\t"
    "vl  %%v29,96(%%r1,%[ap1])\n\t"
    "vl  %%v30,112(%%r1,%[ap0])\n\t"
    "vl  %%v31,112(%%r1,%[ap1])\n\t"
    "vl  %%v2,0(%%r1,%[y])\n\t"
    "vl  %%v3,16(%%r1,%[y])\n\t"
    "vl  %%v4,32(%%r1,%[y])\n\t"
    "vl  %%v5,48(%%r1,%[y])\n\t"
    "vl  %%v6,64(%%r1,%[y])\n\t"
    "vl  %%v7,80(%%r1,%[y])\n\t"
    "vl  %%v8,96(%%r1,%[y])\n\t"
    "vl  %%v9,112(%%r1,%[y])\n\t"
    "vfmadb   %%v2,%%v16,%%v0,%%v2\n\t"
    "vfmadb   %%v3,%%v18,%%v0,%%v3\n\t"
    "vfmadb   %%v4,%%v20,%%v0,%%v4\n\t"
    "vfmadb   %%v5,%%v22,%%v0,%%v5\n\t"
    "vfmadb   %%v6,%%v24,%%v0,%%v6\n\t"
    "vfmadb   %%v7,%%v26,%%v0,%%v7\n\t"
    "vfmadb   %%v8,%%v28,%%v0,%%v8\n\t"
    "vfmadb   %%v9,%%v30,%%v0,%%v9\n\t"
    "vfmadb   %%v2,%%v17,%%v1,%%v2\n\t"
    "vfmadb   %%v3,%%v19,%%v1,%%v3\n\t"
    "vfmadb   %%v4,%%v21,%%v1,%%v4\n\t"
    "vfmadb   %%v5,%%v23,%%v1,%%v5\n\t"
    "vfmadb   %%v6,%%v25,%%v1,%%v6\n\t"
    "vfmadb   %%v7,%%v27,%%v1,%%v7\n\t"
    "vfmadb   %%v8,%%v29,%%v1,%%v8\n\t"
    "vfmadb   %%v9,%%v31,%%v1,%%v9\n\t"
    "vst %%v2,0(%%r1,%[y])\n\t"
    "vst %%v3,16(%%r1,%[y])\n\t"
    "vst %%v4,32(%%r1,%[y])\n\t"
    "vst %%v5,48(%%r1,%[y])\n\t"
    "vst %%v6,64(%%r1,%[y])\n\t"
    "vst %%v7,80(%%r1,%[y])\n\t"
    "vst %%v8,96(%%r1,%[y])\n\t"
    "vst %%v9,112(%%r1,%[y])\n\t"
    "agfi   %%r1,128\n\t"
    "brctg  %%r0,0b\n\t"
    "1:\n\t"
    "lghi    %%r0,12\n\t"
    "ngr     %%r0,%[n]\n\t"
    "ltgr    %%r0,%%r0\n\t"
    "jz      3f\n\t"
    "srlg  %%r0,%%r0,2\n\t"
    "2:\n\t"
    "vl  %%v16,0(%%r1,%[ap0])\n\t"
    "vl  %%v17,0(%%r1,%[ap1])\n\t"
    "vl  %%v18,16(%%r1,%[ap0])\n\t"
    "vl  %%v19,16(%%r1,%[ap1])\n\t"
    "vl  %%v2,0(%%r1,%[y])\n\t"
    "vl  %%v3,16(%%r1,%[y])\n\t"
    "vfmadb   %%v2,%%v16,%%v0,%%v2\n\t"
    "vfmadb   %%v3,%%v18,%%v0,%%v3\n\t"
    "vfmadb   %%v2,%%v17,%%v1,%%v2\n\t"
    "vfmadb   %%v3,%%v19,%%v1,%%v3\n\t"
    "vst %%v2,0(%%r1,%[y])\n\t"
    "vst %%v3,16(%%r1,%[y])\n\t"
    "agfi   %%r1,32\n\t"
    "brctg  %%r0,2b\n\t"
    "3:\n\t"
    "nop 0"
    : "+m"(*(FLOAT (*)[n]) y)
    : [y] "a"(y), "m"(*(const FLOAT (*)[n]) ap0),[ap0] "a"(ap0),
       "m"(*(const FLOAT (*)[n]) ap1),[ap1] "a"(ap1),
       "m"(*(const FLOAT (*)[2]) x),[x] "a"(x),[alpha] "Q"(*alpha),
       [n] "r"(n)
    : "cc", "r0", "r1", "v0", "v1", "v2", "v3", "v4", "v5", "v6", "v7",
       "v8", "v9", "v16", "v17", "v18", "v19", "v20", "v21", "v22", "v23",
       "v24", "v25", "v26", "v27", "v28", "v29", "v30", "v31");
}

static void dgemv_kernel_4x1(BLASLONG n, FLOAT *a0, FLOAT *x, FLOAT *y,
                             FLOAT *alpha) {
  __asm__("vlrepg %%v0,0(%[x])\n\t"
    "vlrepg %%v16,%[alpha]\n\t"
    "vfmdb  %%v0,%%v0,%%v16\n\t"
    "xgr   %%r1,%%r1\n\t"
    "lghi    %%r0,-16\n\t"
    "ngr     %%r0,%[n]\n\t"
    "ltgr    %%r0,%%r0\n\t"
    "jz      1f\n\t"
    "srlg  %%r0,%%r0,4\n\t"
    "0:\n\t"
    "pfd 1,1024(%%r1,%[a0])\n\t"
    "pfd 2,1024(%%r1,%[y])\n\t"
    "vl  %%v16,0(%%r1,%[a0])\n\t"
    "vl  %%v17,16(%%r1,%[a0])\n\t"
    "vl  %%v18,32(%%r1,%[a0])\n\t"
    "vl  %%v19,48(%%r1,%[a0])\n\t"
    "vl  %%v20,64(%%r1,%[a0])\n\t"
    "vl  %%v21,80(%%r1,%[a0])\n\t"
    "vl  %%v22,96(%%r1,%[a0])\n\t"
    "vl  %%v23,112(%%r1,%[a0])\n\t"
    "vl  %%v24,0(%%r1,%[y])\n\t"
    "vl  %%v25,16(%%r1,%[y])\n\t"
    "vl  %%v26,32(%%r1,%[y])\n\t"
    "vl  %%v27,48(%%r1,%[y])\n\t"
    "vl  %%v28,64(%%r1,%[y])\n\t"
    "vl  %%v29,80(%%r1,%[y])\n\t"
    "vl  %%v30,96(%%r1,%[y])\n\t"
    "vl  %%v31,112(%%r1,%[y])\n\t"
    "vfmadb   %%v24,%%v16,%%v0,%%v24\n\t"
    "vfmadb   %%v25,%%v17,%%v0,%%v25\n\t"
    "vfmadb   %%v26,%%v18,%%v0,%%v26\n\t"
    "vfmadb   %%v27,%%v19,%%v0,%%v27\n\t"
    "vfmadb   %%v28,%%v20,%%v0,%%v28\n\t"
    "vfmadb   %%v29,%%v21,%%v0,%%v29\n\t"
    "vfmadb   %%v30,%%v22,%%v0,%%v30\n\t"
    "vfmadb   %%v31,%%v23,%%v0,%%v31\n\t"
    "vst %%v24,0(%%r1,%[y])\n\t"
    "vst %%v25,16(%%r1,%[y])\n\t"
    "vst %%v26,32(%%r1,%[y])\n\t"
    "vst %%v27,48(%%r1,%[y])\n\t"
    "vst %%v28,64(%%r1,%[y])\n\t"
    "vst %%v29,80(%%r1,%[y])\n\t"
    "vst %%v30,96(%%r1,%[y])\n\t"
    "vst %%v31,112(%%r1,%[y])\n\t"
    "agfi   %%r1,128\n\t"
    "brctg  %%r0,0b\n\t"
    "1:\n\t"
    "lghi    %%r0,12\n\t"
    "ngr     %%r0,%[n]\n\t"
    "ltgr    %%r0,%%r0\n\t"
    "jz      3f\n\t"
    "srlg  %%r0,%%r0,2\n\t"
    "2:\n\t"
    "vl  %%v16,0(%%r1,%[a0])\n\t"
    "vl  %%v17,16(%%r1,%[a0])\n\t"
    "vl  %%v18,0(%%r1,%[y])\n\t"
    "vl  %%v19,16(%%r1,%[y])\n\t"
    "vfmadb   %%v18,%%v16,%%v0,%%v18\n\t"
    "vfmadb   %%v19,%%v17,%%v0,%%v19\n\t"
    "vst %%v18,0(%%r1,%[y])\n\t"
    "vst %%v19,16(%%r1,%[y])\n\t"
    "agfi   %%r1,32\n\t"
    "brctg  %%r0,2b\n\t"
    "3:\n\t"
    "nop 0"
    : "+m"(*(FLOAT (*)[n]) y)
    : [y] "a"(y), "m"(*(const FLOAT (*)[n]) a0),[a0] "a"(a0),
       "m"(*(const FLOAT (*)[1]) x),[x] "a"(x),[alpha] "Q"(*alpha),
       [n] "r"(n)
    : "cc", "r0", "r1", "v0", "v16", "v17", "v18", "v19", "v20", "v21",
       "v22", "v23", "v24", "v25", "v26", "v27", "v28", "v29", "v30",
       "v31");
}

static void add_y(BLASLONG n, FLOAT *src, FLOAT *dest, BLASLONG inc_dest) {
  BLASLONG i;
  for (i = 0; i < n; i++) {
    *dest += src[i];
    dest += inc_dest;
  }
}

int CNAME(BLASLONG m, BLASLONG n, BLASLONG dummy1, FLOAT alpha, FLOAT *a,
          BLASLONG lda, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y,
          FLOAT *buffer) {
  BLASLONG i;
  FLOAT *a_ptr;
  FLOAT *x_ptr;
  FLOAT *y_ptr;
  FLOAT *ap[4];
  BLASLONG n1;
  BLASLONG m1;
  BLASLONG m2;
  BLASLONG m3;
  BLASLONG n2;
  BLASLONG lda4 = lda << 2;
  FLOAT xbuffer[8], *ybuffer;

  if (m < 1)
    return (0);
  if (n < 1)
    return (0);

  ybuffer = buffer;

  n1 = n >> 2;
  n2 = n & 3;

  m3 = m & 3;
  m1 = m & -4;
  m2 = (m & (NBMAX - 1)) - m3;

  y_ptr = y;

  BLASLONG NB = NBMAX;

  while (NB == NBMAX) {

    m1 -= NB;
    if (m1 < 0) {
      if (m2 == 0)
        break;
      NB = m2;
    }

    a_ptr = a;
    x_ptr = x;

    ap[0] = a_ptr;
    ap[1] = a_ptr + lda;
    ap[2] = ap[1] + lda;
    ap[3] = ap[2] + lda;

    if (inc_y != 1)
      memset(ybuffer, 0, NB * 8);
    else
      ybuffer = y_ptr;

    if (inc_x == 1) {

      for (i = 0; i < n1; i++) {
        dgemv_kernel_4x4(NB, ap, x_ptr, ybuffer, &alpha);
        ap[0] += lda4;
        ap[1] += lda4;
        ap[2] += lda4;
        ap[3] += lda4;
        a_ptr += lda4;
        x_ptr += 4;
      }

      if (n2 & 2) {
        dgemv_kernel_4x2(NB, ap, x_ptr, ybuffer, &alpha);
        a_ptr += lda * 2;
        x_ptr += 2;
      }

      if (n2 & 1) {
        dgemv_kernel_4x1(NB, a_ptr, x_ptr, ybuffer, &alpha);
        /* a_ptr += lda;
           x_ptr += 1; */

      }

    } else {

      for (i = 0; i < n1; i++) {
        xbuffer[0] = x_ptr[0];
        x_ptr += inc_x;
        xbuffer[1] = x_ptr[0];
        x_ptr += inc_x;
        xbuffer[2] = x_ptr[0];
        x_ptr += inc_x;
        xbuffer[3] = x_ptr[0];
        x_ptr += inc_x;
        dgemv_kernel_4x4(NB, ap, xbuffer, ybuffer, &alpha);
        ap[0] += lda4;
        ap[1] += lda4;
        ap[2] += lda4;
        ap[3] += lda4;
        a_ptr += lda4;
      }

      for (i = 0; i < n2; i++) {
        xbuffer[0] = x_ptr[0];
        x_ptr += inc_x;
        dgemv_kernel_4x1(NB, a_ptr, xbuffer, ybuffer, &alpha);
        a_ptr += lda;

      }

    }

    a += NB;
    if (inc_y != 1) {
      add_y(NB, ybuffer, y_ptr, inc_y);
      y_ptr += NB * inc_y;
    } else
      y_ptr += NB;

  }

  if (m3 == 0)
    return (0);

  if (m3 == 3) {
    a_ptr = a;
    x_ptr = x;
    FLOAT temp0 = 0.0;
    FLOAT temp1 = 0.0;
    FLOAT temp2 = 0.0;
    if (lda == 3 && inc_x == 1) {

      for (i = 0; i < (n & -4); i += 4) {

        temp0 += a_ptr[0] * x_ptr[0] + a_ptr[3] * x_ptr[1];
        temp1 += a_ptr[1] * x_ptr[0] + a_ptr[4] * x_ptr[1];
        temp2 += a_ptr[2] * x_ptr[0] + a_ptr[5] * x_ptr[1];

        temp0 += a_ptr[6] * x_ptr[2] + a_ptr[9] * x_ptr[3];
        temp1 += a_ptr[7] * x_ptr[2] + a_ptr[10] * x_ptr[3];
        temp2 += a_ptr[8] * x_ptr[2] + a_ptr[11] * x_ptr[3];

        a_ptr += 12;
        x_ptr += 4;
      }

      for (; i < n; i++) {
        temp0 += a_ptr[0] * x_ptr[0];
        temp1 += a_ptr[1] * x_ptr[0];
        temp2 += a_ptr[2] * x_ptr[0];
        a_ptr += 3;
        x_ptr++;
      }

    } else {

      for (i = 0; i < n; i++) {
        temp0 += a_ptr[0] * x_ptr[0];
        temp1 += a_ptr[1] * x_ptr[0];
        temp2 += a_ptr[2] * x_ptr[0];
        a_ptr += lda;
        x_ptr += inc_x;

      }

    }
    y_ptr[0] += alpha * temp0;
    y_ptr += inc_y;
    y_ptr[0] += alpha * temp1;
    y_ptr += inc_y;
    y_ptr[0] += alpha * temp2;
    return (0);
  }

  if (m3 == 2) {
    a_ptr = a;
    x_ptr = x;
    FLOAT temp0 = 0.0;
    FLOAT temp1 = 0.0;
    if (lda == 2 && inc_x == 1) {

      for (i = 0; i < (n & -4); i += 4) {
        temp0 += a_ptr[0] * x_ptr[0] + a_ptr[2] * x_ptr[1];
        temp1 += a_ptr[1] * x_ptr[0] + a_ptr[3] * x_ptr[1];
        temp0 += a_ptr[4] * x_ptr[2] + a_ptr[6] * x_ptr[3];
        temp1 += a_ptr[5] * x_ptr[2] + a_ptr[7] * x_ptr[3];
        a_ptr += 8;
        x_ptr += 4;

      }

      for (; i < n; i++) {
        temp0 += a_ptr[0] * x_ptr[0];
        temp1 += a_ptr[1] * x_ptr[0];
        a_ptr += 2;
        x_ptr++;
      }

    } else {

      for (i = 0; i < n; i++) {
        temp0 += a_ptr[0] * x_ptr[0];
        temp1 += a_ptr[1] * x_ptr[0];
        a_ptr += lda;
        x_ptr += inc_x;

      }

    }
    y_ptr[0] += alpha * temp0;
    y_ptr += inc_y;
    y_ptr[0] += alpha * temp1;
    return (0);
  }

  if (m3 == 1) {
    a_ptr = a;
    x_ptr = x;
    FLOAT temp = 0.0;
    if (lda == 1 && inc_x == 1) {

      for (i = 0; i < (n & -4); i += 4) {
        temp +=
          a_ptr[i] * x_ptr[i] + a_ptr[i + 1] * x_ptr[i + 1] + a_ptr[i +
                                                                    2] *
          x_ptr[i + 2] + a_ptr[i + 3] * x_ptr[i + 3];

      }

      for (; i < n; i++) {
        temp += a_ptr[i] * x_ptr[i];
      }

    } else {

      for (i = 0; i < n; i++) {
        temp += a_ptr[0] * x_ptr[0];
        a_ptr += lda;
        x_ptr += inc_x;
      }

    }
    y_ptr[0] += alpha * temp;
    return (0);
  }

  return (0);
}
