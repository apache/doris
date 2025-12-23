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

#define NBMAX 1024

static void zgemv_kernel_4x4(BLASLONG n, FLOAT **ap, FLOAT *x, FLOAT *y) {
  register FLOAT *ap0 = ap[0];
  register FLOAT *ap1 = ap[1];
  register FLOAT *ap2 = ap[2];
  register FLOAT *ap3 = ap[3];

  __asm__("vl     %%v16,0(%[x])\n\t"
    "vl     %%v17,16(%[x])\n\t"
    "vl     %%v18,32(%[x])\n\t"
    "vl     %%v19,48(%[x])\n\t"
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
    "vleg   %%v20,8(%[x]),0\n\t"
    "wflcdb %%v20,%%v20\n\t"
    "vleg   %%v20,0(%[x]),1\n\t"
    "vleg   %%v21,24(%[x]),0\n\t"
    "wflcdb %%v21,%%v21\n\t"
    "vleg   %%v21,16(%[x]),1\n\t"
    "vleg   %%v22,40(%[x]),0\n\t"
    "wflcdb %%v22,%%v22\n\t"
    "vleg   %%v22,32(%[x]),1\n\t"
    "vleg   %%v23,56(%[x]),0\n\t"
    "wflcdb %%v23,%%v23\n\t"
    "vleg   %%v23,48(%[x]),1\n\t"
#else
    "vleg   %%v20,0(%[x]),1\n\t"
    "vflcdb %%v20,%%v20\n\t"
    "vleg   %%v20,8(%[x]),0\n\t"
    "vleg   %%v21,16(%[x]),1\n\t"
    "vflcdb %%v21,%%v21\n\t"
    "vleg   %%v21,24(%[x]),0\n\t"
    "vleg   %%v22,32(%[x]),1\n\t"
    "vflcdb %%v22,%%v22\n\t"
    "vleg   %%v22,40(%[x]),0\n\t"
    "vleg   %%v23,48(%[x]),1\n\t"
    "vflcdb %%v23,%%v23\n\t"
    "vleg   %%v23,56(%[x]),0\n\t"
#endif
    "xgr   %%r1,%%r1\n\t"
    "srlg  %[n],%[n],1\n\t"
    "0:\n\t"
    "pfd 1,1024(%%r1,%[ap0])\n\t"
    "pfd 1,1024(%%r1,%[ap1])\n\t"
    "pfd 1,1024(%%r1,%[ap2])\n\t"
    "pfd 1,1024(%%r1,%[ap3])\n\t"
    "pfd 2,1024(%%r1,%[y])\n\t"
    "vl  %%v0,0(%%r1,%[y])\n\t"
    "vl  %%v1,16(%%r1,%[y])\n\t"
    "vlrepg %%v24,0(%%r1,%[ap0])\n\t"
    "vlrepg %%v25,8(%%r1,%[ap0])\n\t"
    "vlrepg %%v26,0(%%r1,%[ap1])\n\t"
    "vlrepg %%v27,8(%%r1,%[ap1])\n\t"
    "vlrepg %%v28,16(%%r1,%[ap0])\n\t"
    "vlrepg %%v29,24(%%r1,%[ap0])\n\t"
    "vlrepg %%v30,16(%%r1,%[ap1])\n\t"
    "vlrepg %%v31,24(%%r1,%[ap1])\n\t"
    "vfmadb   %%v0,%%v24,%%v16,%%v0\n\t"
    "vfmadb   %%v1,%%v28,%%v16,%%v1\n\t"
    "vfmadb   %%v0,%%v25,%%v20,%%v0\n\t"
    "vfmadb   %%v1,%%v29,%%v20,%%v1\n\t"
    "vfmadb   %%v0,%%v26,%%v17,%%v0\n\t"
    "vfmadb   %%v1,%%v30,%%v17,%%v1\n\t"
    "vfmadb   %%v0,%%v27,%%v21,%%v0\n\t"
    "vfmadb   %%v1,%%v31,%%v21,%%v1\n\t"
    "vlrepg %%v24,0(%%r1,%[ap2])\n\t"
    "vlrepg %%v25,8(%%r1,%[ap2])\n\t"
    "vlrepg %%v26,0(%%r1,%[ap3])\n\t"
    "vlrepg %%v27,8(%%r1,%[ap3])\n\t"
    "vlrepg %%v28,16(%%r1,%[ap2])\n\t"
    "vlrepg %%v29,24(%%r1,%[ap2])\n\t"
    "vlrepg %%v30,16(%%r1,%[ap3])\n\t"
    "vlrepg %%v31,24(%%r1,%[ap3])\n\t"
    "vfmadb   %%v0,%%v24,%%v18,%%v0\n\t"
    "vfmadb   %%v1,%%v28,%%v18,%%v1\n\t"
    "vfmadb   %%v0,%%v25,%%v22,%%v0\n\t"
    "vfmadb   %%v1,%%v29,%%v22,%%v1\n\t"
    "vfmadb   %%v0,%%v26,%%v19,%%v0\n\t"
    "vfmadb   %%v1,%%v30,%%v19,%%v1\n\t"
    "vfmadb   %%v0,%%v27,%%v23,%%v0\n\t"
    "vfmadb   %%v1,%%v31,%%v23,%%v1\n\t"
    "vst %%v0,0(%%r1,%[y])\n\t"
    "vst %%v1,16(%%r1,%[y])\n\t"
    "agfi   %%r1,32\n\t"
    "brctg  %[n],0b"
    : "+m"(*(FLOAT (*)[n * 2]) y),[n] "+&r"(n)
    : [y] "a"(y), "m"(*(const FLOAT (*)[n * 2]) ap0),[ap0] "a"(ap0),
       "m"(*(const FLOAT (*)[n * 2]) ap1),[ap1] "a"(ap1),
       "m"(*(const FLOAT (*)[n * 2]) ap2),[ap2] "a"(ap2),
       "m"(*(const FLOAT (*)[n * 2]) ap3),[ap3] "a"(ap3),
       "m"(*(const FLOAT (*)[8]) x),[x] "a"(x)
    : "cc", "r1", "v0", "v1", "v16", "v17", "v18", "v19", "v20", "v21",
       "v22", "v23", "v24", "v25", "v26", "v27", "v28", "v29", "v30",
       "v31");
}

static void zgemv_kernel_4x2(BLASLONG n, FLOAT **ap, FLOAT *x, FLOAT *y) {
  register FLOAT *ap0 = ap[0];
  register FLOAT *ap1 = ap[1];

  __asm__("vl     %%v16,0(%[x])\n\t"
    "vl     %%v17,16(%[x])\n\t"
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
    "vleg   %%v18,8(%[x]),0\n\t"
    "wflcdb %%v18,%%v18\n\t"
    "vleg   %%v18,0(%[x]),1\n\t"
    "vleg   %%v19,24(%[x]),0\n\t"
    "wflcdb %%v19,%%v19\n\t"
    "vleg   %%v19,16(%[x]),1\n\t"
#else
    "vleg   %%v18,0(%[x]),1\n\t"
    "vflcdb %%v18,%%v18\n\t"
    "vleg   %%v18,8(%[x]),0\n\t"
    "vleg   %%v19,16(%[x]),1\n\t"
    "vflcdb %%v19,%%v19\n\t"
    "vleg   %%v19,24(%[x]),0\n\t"
#endif
    "xgr   %%r1,%%r1\n\t"
    "srlg  %[n],%[n],1\n\t"
    "0:\n\t"
    "pfd 1,1024(%%r1,%[ap0])\n\t"
    "pfd 1,1024(%%r1,%[ap1])\n\t"
    "pfd 2,1024(%%r1,%[y])\n\t"
    "vl  %%v0,0(%%r1,%[y])\n\t"
    "vl  %%v1,16(%%r1,%[y])\n\t"
    "vlrepg %%v20,0(%%r1,%[ap0])\n\t"
    "vlrepg %%v21,8(%%r1,%[ap0])\n\t"
    "vlrepg %%v22,0(%%r1,%[ap1])\n\t"
    "vlrepg %%v23,8(%%r1,%[ap1])\n\t"
    "vlrepg %%v24,16(%%r1,%[ap0])\n\t"
    "vlrepg %%v25,24(%%r1,%[ap0])\n\t"
    "vlrepg %%v26,16(%%r1,%[ap1])\n\t"
    "vlrepg %%v27,24(%%r1,%[ap1])\n\t"
    "vfmadb   %%v0,%%v20,%%v16,%%v0\n\t"
    "vfmadb   %%v1,%%v24,%%v16,%%v1\n\t"
    "vfmadb   %%v0,%%v21,%%v18,%%v0\n\t"
    "vfmadb   %%v1,%%v25,%%v18,%%v1\n\t"
    "vfmadb   %%v0,%%v22,%%v17,%%v0\n\t"
    "vfmadb   %%v1,%%v26,%%v17,%%v1\n\t"
    "vfmadb   %%v0,%%v23,%%v19,%%v0\n\t"
    "vfmadb   %%v1,%%v27,%%v19,%%v1\n\t"
    "vst %%v0,0(%%r1,%[y])\n\t"
    "vst %%v1,16(%%r1,%[y])\n\t"
    "agfi   %%r1,32\n\t"
    "brctg  %[n],0b"
    : "+m"(*(FLOAT (*)[n * 2]) y),[n] "+&r"(n)
    : [y] "a"(y), "m"(*(const FLOAT (*)[n * 2]) ap0),[ap0] "a"(ap0),
         "m"(*(const FLOAT (*)[n * 2]) ap1),[ap1] "a"(ap1),
         "m"(*(const FLOAT (*)[4]) x),[x] "a"(x)
    : "cc", "r1", "v0", "v1", "v16", "v17", "v18", "v19", "v20", "v21",
       "v22", "v23", "v24", "v25", "v26", "v27");
}

static void zgemv_kernel_4x1(BLASLONG n, FLOAT *ap, FLOAT *x, FLOAT *y) {
  __asm__("vl     %%v16,0(%[x])\n\t"
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
    "vleg   %%v17,8(%[x]),0\n\t"
    "wflcdb %%v17,%%v17\n\t"
    "vleg   %%v17,0(%[x]),1\n\t"
#else
    "vleg   %%v17,0(%[x]),1\n\t"
    "vflcdb %%v17,%%v17\n\t"
    "vleg   %%v17,8(%[x]),0\n\t"
#endif
    "xgr   %%r1,%%r1\n\t"
    "srlg  %[n],%[n],1\n\t"
    "0:\n\t"
    "pfd 1,1024(%%r1,%[ap])\n\t"
    "pfd 2,1024(%%r1,%[y])\n\t"
    "vl  %%v0,0(%%r1,%[y])\n\t"
    "vl  %%v1,16(%%r1,%[y])\n\t"
    "vlrepg %%v18,0(%%r1,%[ap])\n\t"
    "vlrepg %%v19,8(%%r1,%[ap])\n\t"
    "vlrepg %%v20,16(%%r1,%[ap])\n\t"
    "vlrepg %%v21,24(%%r1,%[ap])\n\t"
    "vfmadb   %%v0,%%v18,%%v16,%%v0\n\t"
    "vfmadb   %%v1,%%v20,%%v16,%%v1\n\t"
    "vfmadb   %%v0,%%v19,%%v17,%%v0\n\t"
    "vfmadb   %%v1,%%v21,%%v17,%%v1\n\t"
    "vst %%v0,0(%%r1,%[y])\n\t"
    "vst %%v1,16(%%r1,%[y])\n\t"
    "agfi   %%r1,32\n\t"
    "brctg  %[n],0b"
    : "+m"(*(FLOAT (*)[n * 2]) y),[n] "+&r"(n)
    : [y] "a"(y), "m"(*(const FLOAT (*)[n * 2]) ap),[ap] "a"(ap),
       "m"(*(const FLOAT (*)[2]) x),[x] "a"(x)
    : "cc", "r1", "v0", "v1", "v16", "v17", "v18", "v19", "v20", "v21");
}

static void add_y_4(BLASLONG n, FLOAT *src, FLOAT *dest, FLOAT alpha_r,
                    FLOAT alpha_i) {
  __asm__(
#if !defined(XCONJ)
    "vlrepg %%v0,%[alpha_r]\n\t"
    "vleg   %%v1,%[alpha_i],0\n\t"
    "wflcdb %%v1,%%v1\n\t"
    "vleg   %%v1,%[alpha_i],1\n\t"
#else
    "vleg   %%v0,%[alpha_r],1\n\t"
    "vflcdb %%v0,%%v0\n\t"
    "vleg   %%v0,%[alpha_r],0\n\t"
    "vlrepg %%v1,%[alpha_i]\n\t"
#endif
    "xgr   %%r1,%%r1\n\t"
    "srlg  %[n],%[n],2\n\t"
    "0:\n\t"
    "pfd 1,1024(%%r1,%[src])\n\t"
    "pfd 2,1024(%%r1,%[dest])\n\t"
    "vl   %%v16,0(%%r1,%[src])\n\t"
    "vl   %%v17,16(%%r1,%[src])\n\t"
    "vl   %%v18,32(%%r1,%[src])\n\t"
    "vl   %%v19,48(%%r1,%[src])\n\t"
    "vl   %%v20,0(%%r1,%[dest])\n\t"
    "vl   %%v21,16(%%r1,%[dest])\n\t"
    "vl   %%v22,32(%%r1,%[dest])\n\t"
    "vl   %%v23,48(%%r1,%[dest])\n\t"
    "vpdi %%v24,%%v16,%%v16,4\n\t"
    "vpdi %%v25,%%v17,%%v17,4\n\t"
    "vpdi %%v26,%%v18,%%v18,4\n\t"
    "vpdi %%v27,%%v19,%%v19,4\n\t"
    "vfmadb %%v28,%%v16,%%v0,%%v20\n\t"
    "vfmadb %%v29,%%v17,%%v0,%%v21\n\t"
    "vfmadb %%v30,%%v18,%%v0,%%v22\n\t"
    "vfmadb %%v31,%%v19,%%v0,%%v23\n\t"
    "vfmadb %%v28,%%v24,%%v1,%%v28\n\t"
    "vfmadb %%v29,%%v25,%%v1,%%v29\n\t"
    "vfmadb %%v30,%%v26,%%v1,%%v30\n\t"
    "vfmadb %%v31,%%v27,%%v1,%%v31\n\t"
    "vst %%v28,0(%%r1,%[dest])\n\t"
    "vst %%v29,16(%%r1,%[dest])\n\t"
    "vst %%v30,32(%%r1,%[dest])\n\t"
    "vst %%v31,48(%%r1,%[dest])\n\t"
    "agfi   %%r1,64\n\t"
    "brctg  %[n],0b"
    : "+m"(*(FLOAT (*)[n * 2]) dest),[n] "+&r"(n)
    : [dest] "a"(dest), "m"(*(const FLOAT (*)[n * 2]) src),
       [src] "a"(src),[alpha_r] "Q"(alpha_r),[alpha_i] "Q"(alpha_i)
    : "cc", "r1", "v0", "v1", "v16", "v17", "v18", "v19", "v20", "v21",
       "v22", "v23", "v24", "v25", "v26", "v27", "v28", "v29", "v30",
       "v31");
}

static void add_y(BLASLONG n, FLOAT *src, FLOAT *dest, BLASLONG inc_dest,
                  FLOAT alpha_r, FLOAT alpha_i) {
  BLASLONG i;

  if (inc_dest != 2) {

    FLOAT temp_r;
    FLOAT temp_i;
    for (i = 0; i < n; i++) {
#if !defined(XCONJ)
      temp_r = alpha_r * src[0] - alpha_i * src[1];
      temp_i = alpha_r * src[1] + alpha_i * src[0];
#else
      temp_r = alpha_r * src[0] + alpha_i * src[1];
      temp_i = -alpha_r * src[1] + alpha_i * src[0];
#endif

      *dest += temp_r;
      *(dest + 1) += temp_i;

      src += 2;
      dest += inc_dest;
    }
    return;
  }

  add_y_4(n, src, dest, alpha_r, alpha_i);
}

int CNAME(BLASLONG m, BLASLONG n, BLASLONG dummy1, FLOAT alpha_r, FLOAT alpha_i,
          FLOAT *a, BLASLONG lda, FLOAT *x, BLASLONG inc_x, FLOAT *y,
          BLASLONG inc_y, FLOAT *buffer) {
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
  BLASLONG lda4;
  FLOAT xbuffer[8], *ybuffer;

  if (m < 1)
    return (0);
  if (n < 1)
    return (0);

  ybuffer = buffer;

  inc_x *= 2;
  inc_y *= 2;
  lda *= 2;
  lda4 = 4 * lda;

  n1 = n / 4;
  n2 = n % 4;

  m3 = m % 4;
  m1 = m - (m % 4);
  m2 = (m % NBMAX) - (m % 4);

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
    ap[0] = a_ptr;
    ap[1] = a_ptr + lda;
    ap[2] = ap[1] + lda;
    ap[3] = ap[2] + lda;
    x_ptr = x;
    //zero_y(NB,ybuffer);
    memset(ybuffer, 0, NB * 16);

    if (inc_x == 2) {

      for (i = 0; i < n1; i++) {
        zgemv_kernel_4x4(NB, ap, x_ptr, ybuffer);
        ap[0] += lda4;
        ap[1] += lda4;
        ap[2] += lda4;
        ap[3] += lda4;
        a_ptr += lda4;
        x_ptr += 8;
      }

      if (n2 & 2) {
        zgemv_kernel_4x2(NB, ap, x_ptr, ybuffer);
        x_ptr += 4;
        a_ptr += 2 * lda;

      }

      if (n2 & 1) {
        zgemv_kernel_4x1(NB, a_ptr, x_ptr, ybuffer);
        /* x_ptr += 2;  
           a_ptr += lda; */

      }
    } else {

      for (i = 0; i < n1; i++) {

        xbuffer[0] = x_ptr[0];
        xbuffer[1] = x_ptr[1];
        x_ptr += inc_x;
        xbuffer[2] = x_ptr[0];
        xbuffer[3] = x_ptr[1];
        x_ptr += inc_x;
        xbuffer[4] = x_ptr[0];
        xbuffer[5] = x_ptr[1];
        x_ptr += inc_x;
        xbuffer[6] = x_ptr[0];
        xbuffer[7] = x_ptr[1];
        x_ptr += inc_x;

        zgemv_kernel_4x4(NB, ap, xbuffer, ybuffer);
        ap[0] += lda4;
        ap[1] += lda4;
        ap[2] += lda4;
        ap[3] += lda4;
        a_ptr += lda4;
      }

      for (i = 0; i < n2; i++) {
        xbuffer[0] = x_ptr[0];
        xbuffer[1] = x_ptr[1];
        x_ptr += inc_x;
        zgemv_kernel_4x1(NB, a_ptr, xbuffer, ybuffer);
        a_ptr += 1 * lda;

      }

    }

    add_y(NB, ybuffer, y_ptr, inc_y, alpha_r, alpha_i);
    a += 2 * NB;
    y_ptr += NB * inc_y;
  }

  if (m3 == 0)
    return (0);

  if (m3 == 1) {
    a_ptr = a;
    x_ptr = x;
    FLOAT temp_r = 0.0;
    FLOAT temp_i = 0.0;

    if (lda == 2 && inc_x == 2) {

      for (i = 0; i < (n & -2); i += 2) {
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
        temp_r += a_ptr[0] * x_ptr[0] - a_ptr[1] * x_ptr[1];
        temp_i += a_ptr[0] * x_ptr[1] + a_ptr[1] * x_ptr[0];
        temp_r += a_ptr[2] * x_ptr[2] - a_ptr[3] * x_ptr[3];
        temp_i += a_ptr[2] * x_ptr[3] + a_ptr[3] * x_ptr[2];
#else
        temp_r += a_ptr[0] * x_ptr[0] + a_ptr[1] * x_ptr[1];
        temp_i += a_ptr[0] * x_ptr[1] - a_ptr[1] * x_ptr[0];
        temp_r += a_ptr[2] * x_ptr[2] + a_ptr[3] * x_ptr[3];
        temp_i += a_ptr[2] * x_ptr[3] - a_ptr[3] * x_ptr[2];
#endif

        a_ptr += 4;
        x_ptr += 4;
      }

      for (; i < n; i++) {
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
        temp_r += a_ptr[0] * x_ptr[0] - a_ptr[1] * x_ptr[1];
        temp_i += a_ptr[0] * x_ptr[1] + a_ptr[1] * x_ptr[0];
#else
        temp_r += a_ptr[0] * x_ptr[0] + a_ptr[1] * x_ptr[1];
        temp_i += a_ptr[0] * x_ptr[1] - a_ptr[1] * x_ptr[0];
#endif

        a_ptr += 2;
        x_ptr += 2;
      }

    } else {

      for (i = 0; i < n; i++) {
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
        temp_r += a_ptr[0] * x_ptr[0] - a_ptr[1] * x_ptr[1];
        temp_i += a_ptr[0] * x_ptr[1] + a_ptr[1] * x_ptr[0];
#else
        temp_r += a_ptr[0] * x_ptr[0] + a_ptr[1] * x_ptr[1];
        temp_i += a_ptr[0] * x_ptr[1] - a_ptr[1] * x_ptr[0];
#endif

        a_ptr += lda;
        x_ptr += inc_x;
      }

    }
#if !defined(XCONJ)
    y_ptr[0] += alpha_r * temp_r - alpha_i * temp_i;
    y_ptr[1] += alpha_r * temp_i + alpha_i * temp_r;
#else
    y_ptr[0] += alpha_r * temp_r + alpha_i * temp_i;
    y_ptr[1] -= alpha_r * temp_i - alpha_i * temp_r;
#endif
    return (0);
  }

  if (m3 == 2) {
    a_ptr = a;
    x_ptr = x;
    FLOAT temp_r0 = 0.0;
    FLOAT temp_i0 = 0.0;
    FLOAT temp_r1 = 0.0;
    FLOAT temp_i1 = 0.0;

    if (lda == 4 && inc_x == 2) {

      for (i = 0; i < (n & -2); i += 2) {
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )

        temp_r0 += a_ptr[0] * x_ptr[0] - a_ptr[1] * x_ptr[1];
        temp_i0 += a_ptr[0] * x_ptr[1] + a_ptr[1] * x_ptr[0];
        temp_r1 += a_ptr[2] * x_ptr[0] - a_ptr[3] * x_ptr[1];
        temp_i1 += a_ptr[2] * x_ptr[1] + a_ptr[3] * x_ptr[0];

        temp_r0 += a_ptr[4] * x_ptr[2] - a_ptr[5] * x_ptr[3];
        temp_i0 += a_ptr[4] * x_ptr[3] + a_ptr[5] * x_ptr[2];
        temp_r1 += a_ptr[6] * x_ptr[2] - a_ptr[7] * x_ptr[3];
        temp_i1 += a_ptr[6] * x_ptr[3] + a_ptr[7] * x_ptr[2];

#else
        temp_r0 += a_ptr[0] * x_ptr[0] + a_ptr[1] * x_ptr[1];
        temp_i0 += a_ptr[0] * x_ptr[1] - a_ptr[1] * x_ptr[0];
        temp_r1 += a_ptr[2] * x_ptr[0] + a_ptr[3] * x_ptr[1];
        temp_i1 += a_ptr[2] * x_ptr[1] - a_ptr[3] * x_ptr[0];

        temp_r0 += a_ptr[4] * x_ptr[2] + a_ptr[5] * x_ptr[3];
        temp_i0 += a_ptr[4] * x_ptr[3] - a_ptr[5] * x_ptr[2];
        temp_r1 += a_ptr[6] * x_ptr[2] + a_ptr[7] * x_ptr[3];
        temp_i1 += a_ptr[6] * x_ptr[3] - a_ptr[7] * x_ptr[2];

#endif

        a_ptr += 8;
        x_ptr += 4;
      }

      for (; i < n; i++) {
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
        temp_r0 += a_ptr[0] * x_ptr[0] - a_ptr[1] * x_ptr[1];
        temp_i0 += a_ptr[0] * x_ptr[1] + a_ptr[1] * x_ptr[0];
        temp_r1 += a_ptr[2] * x_ptr[0] - a_ptr[3] * x_ptr[1];
        temp_i1 += a_ptr[2] * x_ptr[1] + a_ptr[3] * x_ptr[0];
#else
        temp_r0 += a_ptr[0] * x_ptr[0] + a_ptr[1] * x_ptr[1];
        temp_i0 += a_ptr[0] * x_ptr[1] - a_ptr[1] * x_ptr[0];
        temp_r1 += a_ptr[2] * x_ptr[0] + a_ptr[3] * x_ptr[1];
        temp_i1 += a_ptr[2] * x_ptr[1] - a_ptr[3] * x_ptr[0];
#endif

        a_ptr += 4;
        x_ptr += 2;
      }

    } else {

      for (i = 0; i < n; i++) {
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
        temp_r0 += a_ptr[0] * x_ptr[0] - a_ptr[1] * x_ptr[1];
        temp_i0 += a_ptr[0] * x_ptr[1] + a_ptr[1] * x_ptr[0];
        temp_r1 += a_ptr[2] * x_ptr[0] - a_ptr[3] * x_ptr[1];
        temp_i1 += a_ptr[2] * x_ptr[1] + a_ptr[3] * x_ptr[0];
#else
        temp_r0 += a_ptr[0] * x_ptr[0] + a_ptr[1] * x_ptr[1];
        temp_i0 += a_ptr[0] * x_ptr[1] - a_ptr[1] * x_ptr[0];
        temp_r1 += a_ptr[2] * x_ptr[0] + a_ptr[3] * x_ptr[1];
        temp_i1 += a_ptr[2] * x_ptr[1] - a_ptr[3] * x_ptr[0];
#endif

        a_ptr += lda;
        x_ptr += inc_x;
      }

    }
#if !defined(XCONJ)
    y_ptr[0] += alpha_r * temp_r0 - alpha_i * temp_i0;
    y_ptr[1] += alpha_r * temp_i0 + alpha_i * temp_r0;
    y_ptr += inc_y;
    y_ptr[0] += alpha_r * temp_r1 - alpha_i * temp_i1;
    y_ptr[1] += alpha_r * temp_i1 + alpha_i * temp_r1;
#else
    y_ptr[0] += alpha_r * temp_r0 + alpha_i * temp_i0;
    y_ptr[1] -= alpha_r * temp_i0 - alpha_i * temp_r0;
    y_ptr += inc_y;
    y_ptr[0] += alpha_r * temp_r1 + alpha_i * temp_i1;
    y_ptr[1] -= alpha_r * temp_i1 - alpha_i * temp_r1;
#endif
    return (0);
  }

  if (m3 == 3) {
    a_ptr = a;
    x_ptr = x;
    FLOAT temp_r0 = 0.0;
    FLOAT temp_i0 = 0.0;
    FLOAT temp_r1 = 0.0;
    FLOAT temp_i1 = 0.0;
    FLOAT temp_r2 = 0.0;
    FLOAT temp_i2 = 0.0;

    if (lda == 6 && inc_x == 2) {

      for (i = 0; i < n; i++) {
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
        temp_r0 += a_ptr[0] * x_ptr[0] - a_ptr[1] * x_ptr[1];
        temp_i0 += a_ptr[0] * x_ptr[1] + a_ptr[1] * x_ptr[0];
        temp_r1 += a_ptr[2] * x_ptr[0] - a_ptr[3] * x_ptr[1];
        temp_i1 += a_ptr[2] * x_ptr[1] + a_ptr[3] * x_ptr[0];
        temp_r2 += a_ptr[4] * x_ptr[0] - a_ptr[5] * x_ptr[1];
        temp_i2 += a_ptr[4] * x_ptr[1] + a_ptr[5] * x_ptr[0];
#else
        temp_r0 += a_ptr[0] * x_ptr[0] + a_ptr[1] * x_ptr[1];
        temp_i0 += a_ptr[0] * x_ptr[1] - a_ptr[1] * x_ptr[0];
        temp_r1 += a_ptr[2] * x_ptr[0] + a_ptr[3] * x_ptr[1];
        temp_i1 += a_ptr[2] * x_ptr[1] - a_ptr[3] * x_ptr[0];
        temp_r2 += a_ptr[4] * x_ptr[0] + a_ptr[5] * x_ptr[1];
        temp_i2 += a_ptr[4] * x_ptr[1] - a_ptr[5] * x_ptr[0];
#endif

        a_ptr += 6;
        x_ptr += 2;
      }

    } else {

      for (i = 0; i < n; i++) {
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
        temp_r0 += a_ptr[0] * x_ptr[0] - a_ptr[1] * x_ptr[1];
        temp_i0 += a_ptr[0] * x_ptr[1] + a_ptr[1] * x_ptr[0];
        temp_r1 += a_ptr[2] * x_ptr[0] - a_ptr[3] * x_ptr[1];
        temp_i1 += a_ptr[2] * x_ptr[1] + a_ptr[3] * x_ptr[0];
        temp_r2 += a_ptr[4] * x_ptr[0] - a_ptr[5] * x_ptr[1];
        temp_i2 += a_ptr[4] * x_ptr[1] + a_ptr[5] * x_ptr[0];
#else
        temp_r0 += a_ptr[0] * x_ptr[0] + a_ptr[1] * x_ptr[1];
        temp_i0 += a_ptr[0] * x_ptr[1] - a_ptr[1] * x_ptr[0];
        temp_r1 += a_ptr[2] * x_ptr[0] + a_ptr[3] * x_ptr[1];
        temp_i1 += a_ptr[2] * x_ptr[1] - a_ptr[3] * x_ptr[0];
        temp_r2 += a_ptr[4] * x_ptr[0] + a_ptr[5] * x_ptr[1];
        temp_i2 += a_ptr[4] * x_ptr[1] - a_ptr[5] * x_ptr[0];
#endif

        a_ptr += lda;
        x_ptr += inc_x;
      }

    }
#if !defined(XCONJ)
    y_ptr[0] += alpha_r * temp_r0 - alpha_i * temp_i0;
    y_ptr[1] += alpha_r * temp_i0 + alpha_i * temp_r0;
    y_ptr += inc_y;
    y_ptr[0] += alpha_r * temp_r1 - alpha_i * temp_i1;
    y_ptr[1] += alpha_r * temp_i1 + alpha_i * temp_r1;
    y_ptr += inc_y;
    y_ptr[0] += alpha_r * temp_r2 - alpha_i * temp_i2;
    y_ptr[1] += alpha_r * temp_i2 + alpha_i * temp_r2;
#else
    y_ptr[0] += alpha_r * temp_r0 + alpha_i * temp_i0;
    y_ptr[1] -= alpha_r * temp_i0 - alpha_i * temp_r0;
    y_ptr += inc_y;
    y_ptr[0] += alpha_r * temp_r1 + alpha_i * temp_i1;
    y_ptr[1] -= alpha_r * temp_i1 - alpha_i * temp_r1;
    y_ptr += inc_y;
    y_ptr[0] += alpha_r * temp_r2 + alpha_i * temp_i2;
    y_ptr[1] -= alpha_r * temp_i2 - alpha_i * temp_r2;
#endif
    return (0);
  }

  return (0);
}
