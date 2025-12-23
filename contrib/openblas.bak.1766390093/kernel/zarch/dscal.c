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

static void dscal_kernel_16(BLASLONG n, FLOAT da, FLOAT *x) {
  __asm__("vlrepg %%v0,%[da]\n\t"
    "srlg  %[n],%[n],4\n\t"
    "xgr   %%r1,%%r1\n\t"
    "0:\n\t"
    "pfd 2, 1024(%%r1,%[x])\n\t"
    "vl    %%v24,0(%%r1,%[x])\n\t"
    "vfmdb %%v24,%%v24,%%v0\n\t"
    "vst   %%v24,0(%%r1,%[x])\n\t"
    "vl    %%v25,16(%%r1,%[x])\n\t"
    "vfmdb %%v25,%%v25,%%v0\n\t"
    "vst   %%v25,16(%%r1,%[x])\n\t"
    "vl    %%v26,32(%%r1,%[x])\n\t"
    "vfmdb %%v26,%%v26,%%v0\n\t"
    "vst   %%v26,32(%%r1,%[x])\n\t"
    "vl    %%v27,48(%%r1,%[x])\n\t"
    "vfmdb %%v27,%%v27,%%v0\n\t"
    "vst   %%v27,48(%%r1,%[x])\n\t"
    "vl    %%v28,64(%%r1,%[x])\n\t"
    "vfmdb %%v28,%%v28,%%v0\n\t"
    "vst   %%v28,64(%%r1,%[x])\n\t"
    "vl    %%v29,80(%%r1,%[x])\n\t"
    "vfmdb %%v29,%%v29,%%v0\n\t"
    "vst   %%v29,80(%%r1,%[x])\n\t"
    "vl    %%v30,96(%%r1,%[x])\n\t"
    "vfmdb %%v30,%%v30,%%v0\n\t"
    "vst   %%v30,96(%%r1,%[x])\n\t"
    "vl    %%v31,112(%%r1,%[x])\n\t"
    "vfmdb %%v31,%%v31,%%v0\n\t"
    "vst   %%v31,112(%%r1,%[x])\n\t"
    "agfi   %%r1,128\n\t"
    "brctg  %[n],0b"
    : "+m"(*(FLOAT (*)[n]) x),[n] "+&r"(n)
    : [x] "a"(x),[da] "Q"(da)
    : "cc", "r1", "v0", "v24", "v25", "v26", "v27", "v28", "v29", "v30",
       "v31");
}

static void dscal_kernel_16_zero(BLASLONG n, FLOAT *x) {
  __asm__("vzero %%v0\n\t"
    "srlg %[n],%[n],4\n\t"
    "xgr   %%r1,%%r1\n\t"
    "0:\n\t"
    "pfd 2, 1024(%%r1,%[x])\n\t"
    "vst  %%v0,0(%%r1,%[x])\n\t"
    "vst  %%v0,16(%%r1,%[x])\n\t"
    "vst  %%v0,32(%%r1,%[x])\n\t"
    "vst  %%v0,48(%%r1,%[x])\n\t"
    "vst  %%v0,64(%%r1,%[x])\n\t"
    "vst  %%v0,80(%%r1,%[x])\n\t"
    "vst  %%v0,96(%%r1,%[x])\n\t"
    "vst  %%v0,112(%%r1,%[x])\n\t"
    "agfi  %%r1,128\n\t"
    "brctg %[n],0b"
    : "=m"(*(FLOAT (*)[n]) x),[n] "+&r"(n)
    : [x] "a"(x)
    : "cc", "r1", "v0");
}

int CNAME(BLASLONG n, BLASLONG dummy0, BLASLONG dummy1, FLOAT da, FLOAT *x,
          BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *dummy,
          BLASLONG dummy2) {
  BLASLONG i = 0, j = 0;
  if (n <= 0 || inc_x <= 0)
    return (0);

  if (inc_x == 1) {

    if (da == 0.0) {
    
      if (dummy2 == 0) {
        BLASLONG n1 = n & -16;
        if (n1 > 0) {
          dscal_kernel_16_zero(n1, x);
          j = n1;
        }

        while (j < n) {
          x[j] = 0.0;
          j++;
        }
      } else {
        while (j < n) {
          if (isfinite(x[j]))
            x[j] = 0.0;
          else
            x[j] = NAN;
          j++;
        }
      }
      
    } else {

      BLASLONG n1 = n & -16;
      if (n1 > 0) {
        dscal_kernel_16(n1, da, x);
        j = n1;
      }
      while (j < n) {

        x[j] = da * x[j];
        j++;
      }
    }

  } else {

    if (da == 0.0) {
     if (dummy2 == 0) {
      BLASLONG n1 = n & -4;
      while (j < n1) {
        x[i] = 0.0;
        x[i + inc_x] = 0.0;
        x[i + 2 * inc_x] = 0.0;
        x[i + 3 * inc_x] = 0.0;

        i += inc_x * 4;
        j += 4;
      }
     }
      while (j < n) {
        if (dummy2==0 || isfinite(x[i]))
          x[i] = 0.0;
        else
          x[i] = NAN;
        i += inc_x;
        j++;
      }

    } else {
      BLASLONG n1 = n & -4;

      while (j < n1) {

        x[i] = da * x[i];
        x[i + inc_x] = da * x[i + inc_x];
        x[i + 2 * inc_x] = da * x[i + 2 * inc_x];
        x[i + 3 * inc_x] = da * x[i + 3 * inc_x];

        i += inc_x * 4;
        j += 4;

      }

      while (j < n) {

        x[i] = da * x[i];
        i += inc_x;
        j++;
      }
    }

  }
  return 0;

}
