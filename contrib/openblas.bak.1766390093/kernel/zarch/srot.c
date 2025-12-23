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

static void srot_kernel_64(BLASLONG n, FLOAT *x, FLOAT *y, FLOAT *c, FLOAT *s) {
  __asm__("vlrepf %%v0,%[c]\n\t"
    "vlrepf %%v1,%[s]\n\t"
    "srlg   %[n],%[n],6\n\t"
    "xgr    %%r1,%%r1\n\t"
    "0:\n\t"
    "pfd 2, 1024(%%r1,%[x])\n\t"
    "pfd 2, 1024(%%r1,%[y])\n\t"
    "vl  %%v24, 0(%%r1,%[x])\n\t"
    "vl  %%v25, 16(%%r1,%[x])\n\t"
    "vl  %%v26, 32(%%r1,%[x])\n\t"
    "vl  %%v27, 48(%%r1,%[x])\n\t"
    "vl  %%v16, 0(%%r1,%[y])\n\t"
    "vl  %%v17, 16(%%r1,%[y])\n\t"
    "vl  %%v18, 32(%%r1,%[y])\n\t"
    "vl  %%v19, 48(%%r1,%[y])\n\t"
    "vfmsb %%v28,%%v24,%%v0\n\t"
    "vfmsb %%v29,%%v25,%%v0\n\t"
    "vfmsb %%v20,%%v24,%%v1\n\t" /* yn=x*s */
    "vfmsb %%v21,%%v25,%%v1\n\t" /* yn=x*s */
    "vfmsb %%v30,%%v26,%%v0\n\t"
    "vfmsb %%v22,%%v26,%%v1\n\t" /* yn=x*s */
    "vfmsb %%v31,%%v27,%%v0\n\t"
    "vfmsb %%v23,%%v27,%%v1\n\t" /* yn=x*s */
    /* 2nd parts */
    "vfmasb %%v28,%%v16,%%v1,%%v28\n\t"
    "vfmssb %%v20,%%v16,%%v0,%%v20\n\t" /* yn=y*c-yn */
    "vfmasb %%v29,%%v17,%%v1,%%v29\n\t"
    "vfmssb %%v21,%%v17,%%v0,%%v21\n\t" /* yn=y*c-yn */
    "vfmasb %%v30,%%v18,%%v1,%%v30\n\t"
    "vfmssb %%v22,%%v18,%%v0,%%v22\n\t" /* yn=y*c-yn */
    "vfmasb %%v31,%%v19,%%v1,%%v31\n\t"
    "vfmssb %%v23,%%v19,%%v0,%%v23\n\t" /* yn=y*c-yn */
    "vst  %%v28, 0(%%r1,%[x])\n\t"
    "vst  %%v29, 16(%%r1,%[x])\n\t"
    "vst  %%v30, 32(%%r1,%[x])\n\t"
    "vst  %%v31, 48(%%r1,%[x])\n\t"
    "vst  %%v20, 0(%%r1,%[y])\n\t"
    "vst  %%v21, 16(%%r1,%[y])\n\t"
    "vst  %%v22, 32(%%r1,%[y])\n\t"
    "vst  %%v23, 48(%%r1,%[y])\n\t"
    "vl  %%v24, 64(%%r1,%[x])\n\t"
    "vl  %%v25, 80(%%r1,%[x])\n\t"
    "vl  %%v26, 96(%%r1,%[x])\n\t"
    "vl  %%v27, 112(%%r1,%[x])\n\t"
    "vl  %%v16, 64(%%r1,%[y])\n\t"
    "vl  %%v17, 80(%%r1,%[y])\n\t"
    "vl  %%v18, 96(%%r1,%[y])\n\t"
    "vl  %%v19, 112(%%r1,%[y])\n\t"
    "vfmsb %%v28,%%v24,%%v0\n\t"
    "vfmsb %%v29,%%v25,%%v0\n\t"
    "vfmsb %%v20,%%v24,%%v1\n\t" /* yn=x*s */
    "vfmsb %%v21,%%v25,%%v1\n\t" /* yn=x*s */
    "vfmsb %%v30,%%v26,%%v0\n\t"
    "vfmsb %%v22,%%v26,%%v1\n\t" /* yn=x*s */
    "vfmsb %%v31,%%v27,%%v0\n\t"
    "vfmsb %%v23,%%v27,%%v1\n\t" /* yn=x*s */
    /* 2nd parts */
    "vfmasb %%v28,%%v16,%%v1,%%v28\n\t"
    "vfmssb %%v20,%%v16,%%v0,%%v20\n\t" /* yn=y*c-yn */
    "vfmasb %%v29,%%v17,%%v1,%%v29\n\t"
    "vfmssb %%v21,%%v17,%%v0,%%v21\n\t" /* yn=y*c-yn */
    "vfmasb %%v30,%%v18,%%v1,%%v30\n\t"
    "vfmssb %%v22,%%v18,%%v0,%%v22\n\t" /* yn=y*c-yn */
    "vfmasb %%v31,%%v19,%%v1,%%v31\n\t"
    "vfmssb %%v23,%%v19,%%v0,%%v23\n\t" /* yn=y*c-yn */
    "vst  %%v28, 64(%%r1,%[x])\n\t"
    "vst  %%v29, 80(%%r1,%[x])\n\t"
    "vst  %%v30, 96(%%r1,%[x])\n\t"
    "vst  %%v31, 112(%%r1,%[x])\n\t"
    "vst  %%v20, 64(%%r1,%[y])\n\t"
    "vst  %%v21, 80(%%r1,%[y])\n\t"
    "vst  %%v22, 96(%%r1,%[y])\n\t"
    "vst  %%v23, 112(%%r1,%[y])\n\t"
    "vl  %%v24, 128(%%r1,%[x])\n\t"
    "vl  %%v25, 144(%%r1,%[x])\n\t"
    "vl  %%v26, 160(%%r1,%[x])\n\t"
    "vl  %%v27, 176(%%r1,%[x])\n\t"
    "vl  %%v16, 128(%%r1,%[y])\n\t"
    "vl  %%v17, 144(%%r1,%[y])\n\t"
    "vl  %%v18, 160(%%r1,%[y])\n\t"
    "vl  %%v19, 176(%%r1,%[y])\n\t"
    "vfmsb %%v28,%%v24,%%v0\n\t"
    "vfmsb %%v29,%%v25,%%v0\n\t"
    "vfmsb %%v20,%%v24,%%v1\n\t" /* yn=x*s */
    "vfmsb %%v21,%%v25,%%v1\n\t" /* yn=x*s */
    "vfmsb %%v30,%%v26,%%v0\n\t"
    "vfmsb %%v22,%%v26,%%v1\n\t" /* yn=x*s */
    "vfmsb %%v31,%%v27,%%v0\n\t"
    "vfmsb %%v23,%%v27,%%v1\n\t" /* yn=x*s */
    /* 2nd parts */
    "vfmasb %%v28,%%v16,%%v1,%%v28\n\t"
    "vfmssb %%v20,%%v16,%%v0,%%v20\n\t" /* yn=y*c-yn */
    "vfmasb %%v29,%%v17,%%v1,%%v29\n\t"
    "vfmssb %%v21,%%v17,%%v0,%%v21\n\t" /* yn=y*c-yn */
    "vfmasb %%v30,%%v18,%%v1,%%v30\n\t"
    "vfmssb %%v22,%%v18,%%v0,%%v22\n\t" /* yn=y*c-yn */
    "vfmasb %%v31,%%v19,%%v1,%%v31\n\t"
    "vfmssb %%v23,%%v19,%%v0,%%v23\n\t" /* yn=y*c-yn */
    "vst  %%v28, 128(%%r1,%[x])\n\t"
    "vst  %%v29, 144(%%r1,%[x])\n\t"
    "vst  %%v30, 160(%%r1,%[x])\n\t"
    "vst  %%v31, 176(%%r1,%[x])\n\t"
    "vst  %%v20, 128(%%r1,%[y])\n\t"
    "vst  %%v21, 144(%%r1,%[y])\n\t"
    "vst  %%v22, 160(%%r1,%[y])\n\t"
    "vst  %%v23, 176(%%r1,%[y])\n\t"
    "vl  %%v24, 192(%%r1,%[x])\n\t"
    "vl  %%v25, 208(%%r1,%[x])\n\t"
    "vl  %%v26, 224(%%r1,%[x])\n\t"
    "vl  %%v27, 240(%%r1,%[x])\n\t"
    "vl  %%v16, 192(%%r1,%[y])\n\t"
    "vl  %%v17, 208(%%r1,%[y])\n\t"
    "vl  %%v18, 224(%%r1,%[y])\n\t"
    "vl  %%v19, 240(%%r1,%[y])\n\t"
    "vfmsb %%v28,%%v24,%%v0\n\t"
    "vfmsb %%v29,%%v25,%%v0\n\t"
    "vfmsb %%v20,%%v24,%%v1\n\t" /* yn=x*s */
    "vfmsb %%v21,%%v25,%%v1\n\t" /* yn=x*s */
    "vfmsb %%v30,%%v26,%%v0\n\t"
    "vfmsb %%v22,%%v26,%%v1\n\t" /* yn=x*s */
    "vfmsb %%v31,%%v27,%%v0\n\t"
    "vfmsb %%v23,%%v27,%%v1\n\t" /* yn=x*s */
    /* 2nd parts */
    "vfmasb %%v28,%%v16,%%v1,%%v28\n\t"
    "vfmssb %%v20,%%v16,%%v0,%%v20\n\t" /* yn=y*c-yn */
    "vfmasb %%v29,%%v17,%%v1,%%v29\n\t"
    "vfmssb %%v21,%%v17,%%v0,%%v21\n\t" /* yn=y*c-yn */
    "vfmasb %%v30,%%v18,%%v1,%%v30\n\t"
    "vfmssb %%v22,%%v18,%%v0,%%v22\n\t" /* yn=y*c-yn */
    "vfmasb %%v31,%%v19,%%v1,%%v31\n\t"
    "vfmssb %%v23,%%v19,%%v0,%%v23\n\t" /* yn=y*c-yn */
    "vst  %%v28, 192(%%r1,%[x])\n\t"
    "vst  %%v29, 208(%%r1,%[x])\n\t"
    "vst  %%v30, 224(%%r1,%[x])\n\t"
    "vst  %%v31, 240(%%r1,%[x])\n\t"
    "vst  %%v20, 192(%%r1,%[y])\n\t"
    "vst  %%v21, 208(%%r1,%[y])\n\t"
    "vst  %%v22, 224(%%r1,%[y])\n\t"
    "vst  %%v23, 240(%%r1,%[y])\n\t"
    "agfi  %%r1,256\n\t"
    "brctg %[n],0b"
    : "+m"(*(FLOAT (*)[n]) x), "+m"(*(FLOAT (*)[n]) y),
       [n] "+&r"(n)
    : [x] "a"(x),[y] "a"(y),[c] "Q"(*c),[s] "Q"(*s)
    : "cc", "r1", "v0", "v1", "v16", "v17", "v18", "v19", "v20", "v21",
       "v22", "v23", "v24", "v25", "v26", "v27", "v28", "v29", "v30",
       "v31");
}

int CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y,
          FLOAT c, FLOAT s) {
  BLASLONG i = 0;
  BLASLONG ix = 0, iy = 0;

  FLOAT temp;

  if (n <= 0)
    return (0);

  if ((inc_x == 1) && (inc_y == 1)) {

    BLASLONG n1 = n & -64;
    if (n1 > 0) {
      FLOAT cosa, sina;
      cosa = c;
      sina = s;
      srot_kernel_64(n1, x, y, &cosa, &sina);
      i = n1;
    }

    while (i < n) {
      temp = c * x[i] + s * y[i];
      y[i] = c * y[i] - s * x[i];
      x[i] = temp;

      i++;

    }

  } else {

    while (i < n) {
      temp = c * x[ix] + s * y[iy];
      y[iy] = c * y[iy] - s * x[ix];
      x[ix] = temp;

      ix += inc_x;
      iy += inc_y;
      i++;

    }

  }
  return (0);

}
