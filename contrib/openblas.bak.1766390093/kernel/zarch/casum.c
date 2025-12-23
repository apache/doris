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

#define ABS fabsf

static FLOAT casum_kernel_32(BLASLONG n, FLOAT *x) {
  FLOAT asum;

  __asm__("vzero   %%v24\n\t"
    "vzero   %%v25\n\t"
    "vzero   %%v26\n\t"
    "vzero   %%v27\n\t"
    "vzero   %%v28\n\t"
    "vzero   %%v29\n\t"
    "vzero   %%v30\n\t"
    "vzero   %%v31\n\t"
    "srlg  %[n],%[n],5\n\t"
    "xgr %%r1,%%r1\n\t"
    "0:\n\t"
    "pfd  1, 1024(%%r1,%[x])\n\t"
    "vl  %%v16, 0(%%r1,%[x])\n\t"
    "vl  %%v17, 16(%%r1,%[x])\n\t"
    "vl  %%v18, 32(%%r1,%[x])\n\t"
    "vl  %%v19, 48(%%r1,%[x])\n\t"
    "vl  %%v20, 64(%%r1,%[x])\n\t"
    "vl  %%v21, 80(%%r1,%[x])\n\t"
    "vl  %%v22, 96(%%r1,%[x])\n\t"
    "vl  %%v23, 112(%%r1,%[x])\n\t"
    "vflpsb  %%v16, %%v16\n\t"
    "vflpsb  %%v17, %%v17\n\t"
    "vflpsb  %%v18, %%v18\n\t"
    "vflpsb  %%v19, %%v19\n\t"
    "vflpsb  %%v20, %%v20\n\t"
    "vflpsb  %%v21, %%v21\n\t"
    "vflpsb  %%v22, %%v22\n\t"
    "vflpsb  %%v23, %%v23\n\t"
    "vfasb   %%v24,%%v24,%%v16\n\t"
    "vfasb   %%v25,%%v25,%%v17\n\t"
    "vfasb   %%v26,%%v26,%%v18\n\t"
    "vfasb   %%v27,%%v27,%%v19\n\t"
    "vfasb   %%v28,%%v28,%%v20\n\t"
    "vfasb   %%v29,%%v29,%%v21\n\t"
    "vfasb   %%v30,%%v30,%%v22\n\t"
    "vfasb   %%v31,%%v31,%%v23\n\t"
    "vl  %%v16, 128(%%r1,%[x])\n\t"
    "vl  %%v17, 144(%%r1,%[x])\n\t"
    "vl  %%v18, 160(%%r1,%[x])\n\t"
    "vl  %%v19, 176(%%r1,%[x])\n\t"
    "vl  %%v20, 192(%%r1,%[x])\n\t"
    "vl  %%v21, 208(%%r1,%[x])\n\t"
    "vl  %%v22, 224(%%r1,%[x])\n\t"
    "vl  %%v23, 240(%%r1,%[x])\n\t"
    "vflpsb  %%v16, %%v16\n\t"
    "vflpsb  %%v17, %%v17\n\t"
    "vflpsb  %%v18, %%v18\n\t"
    "vflpsb  %%v19, %%v19\n\t"
    "vflpsb  %%v20, %%v20\n\t"
    "vflpsb  %%v21, %%v21\n\t"
    "vflpsb  %%v22, %%v22\n\t"
    "vflpsb  %%v23, %%v23\n\t"
    "vfasb   %%v24,%%v24,%%v16\n\t"
    "vfasb   %%v25,%%v25,%%v17\n\t"
    "vfasb   %%v26,%%v26,%%v18\n\t"
    "vfasb   %%v27,%%v27,%%v19\n\t"
    "vfasb   %%v28,%%v28,%%v20\n\t"
    "vfasb   %%v29,%%v29,%%v21\n\t"
    "vfasb   %%v30,%%v30,%%v22\n\t"
    "vfasb   %%v31,%%v31,%%v23\n\t"
    "agfi  %%r1,256\n\t"
    "brctg %[n],0b\n\t"
    "vfasb   %%v24,%%v24,%%v25\n\t"
    "vfasb   %%v24,%%v24,%%v26\n\t"
    "vfasb   %%v24,%%v24,%%v27\n\t"
    "vfasb   %%v24,%%v24,%%v28\n\t"
    "vfasb   %%v24,%%v24,%%v29\n\t"
    "vfasb   %%v24,%%v24,%%v30\n\t"
    "vfasb   %%v24,%%v24,%%v31\n\t"
    "veslg   %%v25,%%v24,32\n\t"
    "vfasb   %%v24,%%v24,%%v25\n\t"
    "vrepf   %%v25,%%v24,2\n\t"
    "vfasb   %%v24,%%v24,%%v25\n\t"
    "vstef   %%v24,%[asum],0"
    : [asum] "=Q"(asum),[n] "+&r"(n)
    : "m"(*(const FLOAT (*)[n * 2]) x),[x] "a"(x)
    : "cc", "r1", "v16", "v17", "v18", "v19", "v20", "v21", "v22", "v23",
       "v24", "v25", "v26", "v27", "v28", "v29", "v30", "v31");

  return asum;
}

FLOAT CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x) {
  BLASLONG i = 0;
  BLASLONG ip = 0;
  FLOAT sumf = 0.0;
  BLASLONG n1;
  BLASLONG inc_x2;

  if (n <= 0 || inc_x <= 0)
    return (sumf);

  if (inc_x == 1) {

    n1 = n & -32;
    if (n1 > 0) {

      sumf = casum_kernel_32(n1, x);
      i = n1;
      ip = 2 * n1;
    }

    while (i < n) {
      sumf += ABS(x[ip]) + ABS(x[ip + 1]);
      i++;
      ip += 2;
    }

  } else {
    inc_x2 = 2 * inc_x;

    while (i < n) {
      sumf += ABS(x[ip]) + ABS(x[ip + 1]);
      ip += inc_x2;
      i++;
    }

  }
  return (sumf);
}
