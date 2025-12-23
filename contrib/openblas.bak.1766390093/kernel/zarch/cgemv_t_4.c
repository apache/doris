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

static void cgemv_kernel_4x4(BLASLONG n, FLOAT **ap, FLOAT *x, FLOAT *y,
                             FLOAT *alpha) {
  register FLOAT *ap0 = ap[0];
  register FLOAT *ap1 = ap[1];
  register FLOAT *ap2 = ap[2];
  register FLOAT *ap3 = ap[3];

  __asm__("vzero  %%v16\n\t"
        "vzero  %%v17\n\t"
        "vzero  %%v18\n\t"
        "vzero  %%v19\n\t"
        "vzero  %%v20\n\t"
        "vzero  %%v21\n\t"
        "vzero  %%v22\n\t"
        "vzero  %%v23\n\t"
        "vleib  %%v2,0,0\n\t"
        "vleib  %%v2,1,1\n\t"
        "vleib  %%v2,2,2\n\t"
        "vleib  %%v2,3,3\n\t"
        "vleib  %%v2,0,4\n\t"
        "vleib  %%v2,1,5\n\t"
        "vleib  %%v2,2,6\n\t"
        "vleib  %%v2,3,7\n\t"
        "vleib  %%v2,8,8\n\t"
        "vleib  %%v2,9,9\n\t"
        "vleib  %%v2,10,10\n\t"
        "vleib  %%v2,11,11\n\t"
        "vleib  %%v2,8,12\n\t"
        "vleib  %%v2,9,13\n\t"
        "vleib  %%v2,10,14\n\t"
        "vleib  %%v2,11,15\n\t"
        "vleib  %%v3,4,0\n\t"
        "vleib  %%v3,5,1\n\t"
        "vleib  %%v3,6,2\n\t"
        "vleib  %%v3,7,3\n\t"
        "vleib  %%v3,4,4\n\t"
        "vleib  %%v3,5,5\n\t"
        "vleib  %%v3,6,6\n\t"
        "vleib  %%v3,7,7\n\t"
        "vleib  %%v3,12,8\n\t"
        "vleib  %%v3,13,9\n\t"
        "vleib  %%v3,14,10\n\t"
        "vleib  %%v3,15,11\n\t"
        "vleib  %%v3,12,12\n\t"
        "vleib  %%v3,13,13\n\t"
        "vleib  %%v3,14,14\n\t"
        "vleib  %%v3,15,15\n\t"
        "xgr   %%r1,%%r1\n\t"
        "srlg  %[n],%[n],1\n\t"
        "0:\n\t"
        "pfd 1,1024(%%r1,%[ap0])\n\t"
        "pfd 1,1024(%%r1,%[ap1])\n\t"
        "pfd 1,1024(%%r1,%[ap2])\n\t"
        "pfd 1,1024(%%r1,%[ap3])\n\t"
        "pfd 1,1024(%%r1,%[x])\n\t"
        "vl     %%v0,0(%%r1,%[x])\n\t"
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
        "vlef   %%v1,4(%%r1,%[x]),0\n\t"
        "vlef   %%v1,12(%%r1,%[x]),2\n\t"
        "vflcsb %%v1,%%v1\n\t"
        "vlef   %%v1,0(%%r1,%[x]),1\n\t"
        "vlef   %%v1,8(%%r1,%[x]),3\n\t"
#else
        "vlef   %%v1,0(%%r1,%[x]),1\n\t"
        "vlef   %%v1,8(%%r1,%[x]),3\n\t"
        "vflcsb %%v1,%%v1\n\t"
        "vlef   %%v1,4(%%r1,%[x]),0\n\t"
        "vlef   %%v1,12(%%r1,%[x]),2\n\t"
#endif
        "vl    %%v24,0(%%r1,%[ap0])\n\t"
        "vperm %%v25,%%v24,%%v24,%%v3\n\t"
        "vperm %%v24,%%v24,%%v24,%%v2\n\t"
        "vl    %%v26,0(%%r1,%[ap1])\n\t"
        "vperm %%v27,%%v26,%%v26,%%v3\n\t"
        "vperm %%v26,%%v26,%%v26,%%v2\n\t"
        "vl    %%v28,0(%%r1,%[ap2])\n\t"
        "vperm %%v29,%%v28,%%v28,%%v3\n\t"
        "vperm %%v28,%%v28,%%v28,%%v2\n\t"
        "vl    %%v30,0(%%r1,%[ap3])\n\t"
        "vperm %%v31,%%v30,%%v30,%%v3\n\t"
        "vperm %%v30,%%v30,%%v30,%%v2\n\t"
        "vfmasb   %%v16,%%v24,%%v0,%%v16\n\t"
        "vfmasb   %%v20,%%v25,%%v1,%%v20\n\t"
        "vfmasb   %%v17,%%v26,%%v0,%%v17\n\t"
        "vfmasb   %%v21,%%v27,%%v1,%%v21\n\t"
        "vfmasb   %%v18,%%v28,%%v0,%%v18\n\t"
        "vfmasb   %%v22,%%v29,%%v1,%%v22\n\t"
        "vfmasb   %%v19,%%v30,%%v0,%%v19\n\t"
        "vfmasb   %%v23,%%v31,%%v1,%%v23\n\t"
        "agfi   %%r1,16\n\t"
        "brctg  %[n],0b\n\t"
        "vfasb  %%v16,%%v16,%%v20\n\t"
        "vfasb  %%v17,%%v17,%%v21\n\t"
        "vfasb  %%v18,%%v18,%%v22\n\t"
        "vfasb  %%v19,%%v19,%%v23\n\t"
        "vrepg  %%v20,%%v16,1\n\t"
        "vrepg  %%v21,%%v17,1\n\t"
        "vrepg  %%v22,%%v18,1\n\t"
        "vrepg  %%v23,%%v19,1\n\t"
        "vfasb  %%v16,%%v16,%%v20\n\t"
        "vfasb  %%v17,%%v17,%%v21\n\t"
        "vfasb  %%v18,%%v18,%%v22\n\t"
        "vfasb  %%v19,%%v19,%%v23\n\t"
        "vmrhg  %%v16,%%v16,%%v17\n\t"
        "vmrhg  %%v17,%%v18,%%v19\n\t"
        "verllg %%v18,%%v16,32\n\t"
        "verllg %%v19,%%v17,32\n\t"
#if !defined(XCONJ)
        "vlrepf %%v20,0(%[alpha])\n\t"
        "vlef   %%v21,4(%[alpha]),0\n\t"
        "vlef   %%v21,4(%[alpha]),2\n\t"
        "vflcsb %%v21,%%v21\n\t"
        "vlef   %%v21,4(%[alpha]),1\n\t"
        "vlef   %%v21,4(%[alpha]),3\n\t"
#else
        "vlef   %%v20,0(%[alpha]),1\n\t"
        "vlef   %%v20,0(%[alpha]),3\n\t"
        "vflcsb %%v20,%%v20\n\t"
        "vlef   %%v20,0(%[alpha]),0\n\t"
        "vlef   %%v20,0(%[alpha]),2\n\t"
        "vlrepf %%v21,4(%[alpha])\n\t"
#endif
        "vl  %%v22,0(%[y])\n\t"
        "vl  %%v23,16(%[y])\n\t"
        "vfmasb   %%v22,%%v16,%%v20,%%v22\n\t"
        "vfmasb   %%v22,%%v18,%%v21,%%v22\n\t"
        "vfmasb   %%v23,%%v17,%%v20,%%v23\n\t"
        "vfmasb   %%v23,%%v19,%%v21,%%v23\n\t"
        "vst  %%v22,0(%[y])\n\t"
        "vst  %%v23,16(%[y])"
    : "+m"(*(FLOAT (*)[8]) y),[n] "+&r"(n)
    : [y] "a"(y), "m"(*(const FLOAT (*)[n * 2]) ap0),[ap0] "a"(ap0),
       "m"(*(const FLOAT (*)[n * 2]) ap1),[ap1] "a"(ap1),
       "m"(*(const FLOAT (*)[n * 2]) ap2),[ap2] "a"(ap2),
       "m"(*(const FLOAT (*)[n * 2]) ap3),[ap3] "a"(ap3),
       "m"(*(const FLOAT (*)[n * 2]) x),[x] "a"(x),
       "m"(*(const FLOAT (*)[2]) alpha),[alpha] "a"(alpha)
    : "cc", "r1", "v0", "v1", "v2", "v3", "v16", "v17", "v18", "v19", "v20",
       "v21", "v22", "v23", "v24", "v25", "v26", "v27", "v28", "v29", "v30",
       "v31");
}

static void cgemv_kernel_4x2(BLASLONG n, FLOAT **ap, FLOAT *x, FLOAT *y,
                             FLOAT *alpha) {
  register FLOAT *ap0 = ap[0];
  register FLOAT *ap1 = ap[1];

  __asm__("vzero  %%v16\n\t"
        "vzero  %%v17\n\t"
        "vzero  %%v18\n\t"
        "vzero  %%v19\n\t"
        "vleib  %%v2,0,0\n\t"
        "vleib  %%v2,1,1\n\t"
        "vleib  %%v2,2,2\n\t"
        "vleib  %%v2,3,3\n\t"
        "vleib  %%v2,0,4\n\t"
        "vleib  %%v2,1,5\n\t"
        "vleib  %%v2,2,6\n\t"
        "vleib  %%v2,3,7\n\t"
        "vleib  %%v2,8,8\n\t"
        "vleib  %%v2,9,9\n\t"
        "vleib  %%v2,10,10\n\t"
        "vleib  %%v2,11,11\n\t"
        "vleib  %%v2,8,12\n\t"
        "vleib  %%v2,9,13\n\t"
        "vleib  %%v2,10,14\n\t"
        "vleib  %%v2,11,15\n\t"
        "vleib  %%v3,4,0\n\t"
        "vleib  %%v3,5,1\n\t"
        "vleib  %%v3,6,2\n\t"
        "vleib  %%v3,7,3\n\t"
        "vleib  %%v3,4,4\n\t"
        "vleib  %%v3,5,5\n\t"
        "vleib  %%v3,6,6\n\t"
        "vleib  %%v3,7,7\n\t"
        "vleib  %%v3,12,8\n\t"
        "vleib  %%v3,13,9\n\t"
        "vleib  %%v3,14,10\n\t"
        "vleib  %%v3,15,11\n\t"
        "vleib  %%v3,12,12\n\t"
        "vleib  %%v3,13,13\n\t"
        "vleib  %%v3,14,14\n\t"
        "vleib  %%v3,15,15\n\t"
        "xgr   %%r1,%%r1\n\t"
        "srlg  %[n],%[n],1\n\t"
        "0:\n\t"
        "pfd 1,1024(%%r1,%[ap0])\n\t"
        "pfd 1,1024(%%r1,%[ap1])\n\t"
        "pfd 1,1024(%%r1,%[x])\n\t"
        "vl     %%v0,0(%%r1,%[x])\n\t"
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
        "vlef   %%v1,4(%%r1,%[x]),0\n\t"
        "vlef   %%v1,12(%%r1,%[x]),2\n\t"
        "vflcsb %%v1,%%v1\n\t"
        "vlef   %%v1,0(%%r1,%[x]),1\n\t"
        "vlef   %%v1,8(%%r1,%[x]),3\n\t"
#else
        "vlef   %%v1,0(%%r1,%[x]),1\n\t"
        "vlef   %%v1,8(%%r1,%[x]),3\n\t"
        "vflcsb %%v1,%%v1\n\t"
        "vlef   %%v1,4(%%r1,%[x]),0\n\t"
        "vlef   %%v1,12(%%r1,%[x]),2\n\t"
#endif
        "vl    %%v20,0(%%r1,%[ap0])\n\t"
        "vperm %%v21,%%v20,%%v20,%%v3\n\t"
        "vperm %%v20,%%v20,%%v20,%%v2\n\t"
        "vl    %%v22,0(%%r1,%[ap1])\n\t"
        "vperm %%v23,%%v22,%%v22,%%v3\n\t"
        "vperm %%v22,%%v22,%%v22,%%v2\n\t"
        "vfmasb   %%v16,%%v20,%%v0,%%v16\n\t"
        "vfmasb   %%v18,%%v21,%%v1,%%v18\n\t"
        "vfmasb   %%v17,%%v22,%%v0,%%v17\n\t"
        "vfmasb   %%v19,%%v23,%%v1,%%v19\n\t"
        "agfi   %%r1,16\n\t"
        "brctg  %[n],0b\n\t"
        "vfasb  %%v16,%%v16,%%v18\n\t"
        "vfasb  %%v17,%%v17,%%v19\n\t"
        "vrepg  %%v18,%%v16,1\n\t"
        "vrepg  %%v19,%%v17,1\n\t"
        "vfasb  %%v16,%%v16,%%v18\n\t"
        "vfasb  %%v17,%%v17,%%v19\n\t"
        "vmrhg  %%v16,%%v16,%%v17\n\t"
        "verllg %%v17,%%v16,32\n\t"
#if !defined(XCONJ)
        "vlrepf %%v18,0(%[alpha])\n\t"
        "vlef   %%v19,4(%[alpha]),0\n\t"
        "vlef   %%v19,4(%[alpha]),2\n\t"
        "vflcsb %%v19,%%v19\n\t"
        "vlef   %%v19,4(%[alpha]),1\n\t"
        "vlef   %%v19,4(%[alpha]),3\n\t"
#else
        "vlef   %%v18,0(%[alpha]),1\n\t"
        "vlef   %%v18,0(%[alpha]),3\n\t"
        "vflcsb %%v18,%%v18\n\t"
        "vlef   %%v18,0(%[alpha]),0\n\t"
        "vlef   %%v18,0(%[alpha]),2\n\t"
        "vlrepf %%v19,4(%[alpha])\n\t"
#endif
        "vl  %%v20,0(%[y])\n\t"
        "vfmasb   %%v20,%%v16,%%v18,%%v20\n\t"
        "vfmasb   %%v20,%%v17,%%v19,%%v20\n\t"
        "vst  %%v20,0(%[y])"
    : "+m"(*(FLOAT (*)[4]) y),[n] "+&r"(n)
    : [y] "a"(y), "m"(*(const FLOAT (*)[n * 2]) ap0),[ap0] "a"(ap0),
       "m"(*(const FLOAT (*)[n * 2]) ap1),[ap1] "a"(ap1),
       "m"(*(const FLOAT (*)[n * 2]) x),[x] "a"(x),
       "m"(*(const FLOAT (*)[2]) alpha),[alpha] "a"(alpha)
    : "cc", "r1", "v0", "v1", "v2", "v3", "v16", "v17", "v18", "v19", "v20",
       "v21", "v22", "v23");
}

static void cgemv_kernel_4x1(BLASLONG n, FLOAT *ap, FLOAT *x, FLOAT *y,
                             FLOAT *alpha) {
  __asm__("vzero  %%v16\n\t"
        "vzero  %%v17\n\t"
        "vleib  %%v2,0,0\n\t"
        "vleib  %%v2,1,1\n\t"
        "vleib  %%v2,2,2\n\t"
        "vleib  %%v2,3,3\n\t"
        "vleib  %%v2,0,4\n\t"
        "vleib  %%v2,1,5\n\t"
        "vleib  %%v2,2,6\n\t"
        "vleib  %%v2,3,7\n\t"
        "vleib  %%v2,8,8\n\t"
        "vleib  %%v2,9,9\n\t"
        "vleib  %%v2,10,10\n\t"
        "vleib  %%v2,11,11\n\t"
        "vleib  %%v2,8,12\n\t"
        "vleib  %%v2,9,13\n\t"
        "vleib  %%v2,10,14\n\t"
        "vleib  %%v2,11,15\n\t"
        "vleib  %%v3,4,0\n\t"
        "vleib  %%v3,5,1\n\t"
        "vleib  %%v3,6,2\n\t"
        "vleib  %%v3,7,3\n\t"
        "vleib  %%v3,4,4\n\t"
        "vleib  %%v3,5,5\n\t"
        "vleib  %%v3,6,6\n\t"
        "vleib  %%v3,7,7\n\t"
        "vleib  %%v3,12,8\n\t"
        "vleib  %%v3,13,9\n\t"
        "vleib  %%v3,14,10\n\t"
        "vleib  %%v3,15,11\n\t"
        "vleib  %%v3,12,12\n\t"
        "vleib  %%v3,13,13\n\t"
        "vleib  %%v3,14,14\n\t"
        "vleib  %%v3,15,15\n\t"
        "xgr   %%r1,%%r1\n\t"
        "srlg  %[n],%[n],1\n\t"
        "0:\n\t"
        "pfd 1,1024(%%r1,%[ap])\n\t"
        "pfd 1,1024(%%r1,%[x])\n\t"
        "vl     %%v0,0(%%r1,%[x])\n\t"
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
        "vlef   %%v1,4(%%r1,%[x]),0\n\t"
        "vlef   %%v1,12(%%r1,%[x]),2\n\t"
        "vflcsb %%v1,%%v1\n\t"
        "vlef   %%v1,0(%%r1,%[x]),1\n\t"
        "vlef   %%v1,8(%%r1,%[x]),3\n\t"
#else
        "vlef   %%v1,0(%%r1,%[x]),1\n\t"
        "vlef   %%v1,8(%%r1,%[x]),3\n\t"
        "vflcsb %%v1,%%v1\n\t"
        "vlef   %%v1,4(%%r1,%[x]),0\n\t"
        "vlef   %%v1,12(%%r1,%[x]),2\n\t"
#endif
        "vl    %%v18,0(%%r1,%[ap])\n\t"
        "vperm %%v19,%%v18,%%v18,%%v3\n\t"
        "vperm %%v18,%%v18,%%v18,%%v2\n\t"
        "vfmasb   %%v16,%%v18,%%v0,%%v16\n\t"
        "vfmasb   %%v17,%%v19,%%v1,%%v17\n\t"
        "agfi   %%r1,16\n\t"
        "brctg  %[n],0b\n\t"
        "vfasb  %%v16,%%v16,%%v17\n\t"
        "vrepg  %%v17,%%v16,1\n\t"
        "vfasb  %%v16,%%v16,%%v17\n\t"
        "verllg %%v17,%%v16,32\n\t"
#if !defined(XCONJ)
        "vlrepf %%v18,0(%[alpha])\n\t"
        "vlef   %%v19,4(%[alpha]),0\n\t"
        "vflcsb %%v19,%%v19\n\t"
        "vlef   %%v19,4(%[alpha]),1\n\t"
#else
        "vlef   %%v18,0(%[alpha]),1\n\t"
        "vflcsb %%v18,%%v18\n\t"
        "vlef   %%v18,0(%[alpha]),0\n\t"
        "vlrepf %%v19,4(%[alpha])\n\t"
#endif
        "vleg     %%v0,0(%[y]),0\n\t"
        "vfmasb   %%v0,%%v16,%%v18,%%v0\n\t"
        "vfmasb   %%v0,%%v17,%%v19,%%v0\n\t"
        "vsteg    %%v0,0(%[y]),0"
    : "+m"(*(FLOAT (*)[2]) y),[n] "+&r"(n)
    : [y] "a"(y), "m"(*(const FLOAT (*)[n * 2]) ap),[ap] "a"(ap),
       "m"(*(const FLOAT (*)[n * 2]) x),[x] "a"(x),
       "m"(*(const FLOAT (*)[2]) alpha),[alpha] "a"(alpha)
    : "cc", "r1", "v0", "v1", "v2", "v3", "v16", "v17", "v18", "v19");
}

static void copy_x(BLASLONG n, FLOAT *src, FLOAT *dest, BLASLONG inc_src) {
  BLASLONG i;
  for (i = 0; i < n; i++) {
    *dest = *src;
    *(dest + 1) = *(src + 1);
    dest += 2;
    src += inc_src;
  }
}

int CNAME(BLASLONG m, BLASLONG n, BLASLONG dummy1, FLOAT alpha_r, FLOAT alpha_i,
          FLOAT *a, BLASLONG lda, FLOAT *x, BLASLONG inc_x, FLOAT *y,
          BLASLONG inc_y, FLOAT *buffer) {
  BLASLONG i;
  BLASLONG j;
  FLOAT *a_ptr;
  FLOAT *x_ptr;
  FLOAT *y_ptr;
  FLOAT *ap[8];
  BLASLONG n1;
  BLASLONG m1;
  BLASLONG m2;
  BLASLONG m3;
  BLASLONG n2;
  BLASLONG lda4;
  FLOAT ybuffer[8], *xbuffer;
  FLOAT alpha[2];

  if (m < 1)
    return (0);
  if (n < 1)
    return (0);

  inc_x <<= 1;
  inc_y <<= 1;
  lda <<= 1;
  lda4 = lda << 2;

  xbuffer = buffer;

  n1 = n >> 2;
  n2 = n & 3;

  m3 = m & 3;
  m1 = m - m3;
  m2 = (m & (NBMAX - 1)) - m3;

  alpha[0] = alpha_r;
  alpha[1] = alpha_i;

  BLASLONG NB = NBMAX;

  while (NB == NBMAX) {

    m1 -= NB;
    if (m1 < 0) {
      if (m2 == 0)
        break;
      NB = m2;
    }

    y_ptr = y;
    a_ptr = a;
    x_ptr = x;
    ap[0] = a_ptr;
    ap[1] = a_ptr + lda;
    ap[2] = ap[1] + lda;
    ap[3] = ap[2] + lda;
    if (inc_x != 2)
      copy_x(NB, x_ptr, xbuffer, inc_x);
    else
      xbuffer = x_ptr;

    if (inc_y == 2) {

      for (i = 0; i < n1; i++) {
        cgemv_kernel_4x4(NB, ap, xbuffer, y_ptr, alpha);
        ap[0] += lda4;
        ap[1] += lda4;
        ap[2] += lda4;
        ap[3] += lda4;
        a_ptr += lda4;
        y_ptr += 8;

      }

      if (n2 & 2) {
        cgemv_kernel_4x2(NB, ap, xbuffer, y_ptr, alpha);
        a_ptr += lda * 2;
        y_ptr += 4;

      }

      if (n2 & 1) {
        cgemv_kernel_4x1(NB, a_ptr, xbuffer, y_ptr, alpha);
        /* a_ptr += lda;
           y_ptr += 2; */

      }

    } else {

      for (i = 0; i < n1; i++) {
        memset(ybuffer, 0, sizeof(ybuffer));
        cgemv_kernel_4x4(NB, ap, xbuffer, ybuffer, alpha);
        ap[0] += lda4;
        ap[1] += lda4;
        ap[2] += lda4;
        ap[3] += lda4;
        a_ptr += lda4;

        y_ptr[0] += ybuffer[0];
        y_ptr[1] += ybuffer[1];
        y_ptr += inc_y;
        y_ptr[0] += ybuffer[2];
        y_ptr[1] += ybuffer[3];
        y_ptr += inc_y;
        y_ptr[0] += ybuffer[4];
        y_ptr[1] += ybuffer[5];
        y_ptr += inc_y;
        y_ptr[0] += ybuffer[6];
        y_ptr[1] += ybuffer[7];
        y_ptr += inc_y;

      }

      for (i = 0; i < n2; i++) {
        memset(ybuffer, 0, sizeof(ybuffer));
        cgemv_kernel_4x1(NB, a_ptr, xbuffer, ybuffer, alpha);
        a_ptr += lda;
        y_ptr[0] += ybuffer[0];
        y_ptr[1] += ybuffer[1];
        y_ptr += inc_y;

      }

    }
    a += 2 * NB;
    x += NB * inc_x;
  }

  if (m3 == 0)
    return (0);

  x_ptr = x;
  j = 0;
  a_ptr = a;
  y_ptr = y;

  if (m3 == 3) {

    FLOAT temp_r;
    FLOAT temp_i;
    FLOAT x0 = x_ptr[0];
    FLOAT x1 = x_ptr[1];
    x_ptr += inc_x;
    FLOAT x2 = x_ptr[0];
    FLOAT x3 = x_ptr[1];
    x_ptr += inc_x;
    FLOAT x4 = x_ptr[0];
    FLOAT x5 = x_ptr[1];
    while (j < n) {
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
      temp_r = a_ptr[0] * x0 - a_ptr[1] * x1;
      temp_i = a_ptr[0] * x1 + a_ptr[1] * x0;
      temp_r += a_ptr[2] * x2 - a_ptr[3] * x3;
      temp_i += a_ptr[2] * x3 + a_ptr[3] * x2;
      temp_r += a_ptr[4] * x4 - a_ptr[5] * x5;
      temp_i += a_ptr[4] * x5 + a_ptr[5] * x4;
#else

      temp_r = a_ptr[0] * x0 + a_ptr[1] * x1;
      temp_i = a_ptr[0] * x1 - a_ptr[1] * x0;
      temp_r += a_ptr[2] * x2 + a_ptr[3] * x3;
      temp_i += a_ptr[2] * x3 - a_ptr[3] * x2;
      temp_r += a_ptr[4] * x4 + a_ptr[5] * x5;
      temp_i += a_ptr[4] * x5 - a_ptr[5] * x4;
#endif

#if !defined(XCONJ)
      y_ptr[0] += alpha_r * temp_r - alpha_i * temp_i;
      y_ptr[1] += alpha_r * temp_i + alpha_i * temp_r;
#else
      y_ptr[0] += alpha_r * temp_r + alpha_i * temp_i;
      y_ptr[1] -= alpha_r * temp_i - alpha_i * temp_r;
#endif

      a_ptr += lda;
      y_ptr += inc_y;
      j++;
    }
    return (0);
  }

  if (m3 == 2) {

    FLOAT temp_r;
    FLOAT temp_i;
    FLOAT temp_r1;
    FLOAT temp_i1;
    FLOAT x0 = x_ptr[0];
    FLOAT x1 = x_ptr[1];
    x_ptr += inc_x;
    FLOAT x2 = x_ptr[0];
    FLOAT x3 = x_ptr[1];
    FLOAT ar = alpha[0];
    FLOAT ai = alpha[1];

    while (j < (n & -2)) {
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
      temp_r = a_ptr[0] * x0 - a_ptr[1] * x1;
      temp_i = a_ptr[0] * x1 + a_ptr[1] * x0;
      temp_r += a_ptr[2] * x2 - a_ptr[3] * x3;
      temp_i += a_ptr[2] * x3 + a_ptr[3] * x2;
      a_ptr += lda;
      temp_r1 = a_ptr[0] * x0 - a_ptr[1] * x1;
      temp_i1 = a_ptr[0] * x1 + a_ptr[1] * x0;
      temp_r1 += a_ptr[2] * x2 - a_ptr[3] * x3;
      temp_i1 += a_ptr[2] * x3 + a_ptr[3] * x2;
#else

      temp_r = a_ptr[0] * x0 + a_ptr[1] * x1;
      temp_i = a_ptr[0] * x1 - a_ptr[1] * x0;
      temp_r += a_ptr[2] * x2 + a_ptr[3] * x3;
      temp_i += a_ptr[2] * x3 - a_ptr[3] * x2;
      a_ptr += lda;
      temp_r1 = a_ptr[0] * x0 + a_ptr[1] * x1;
      temp_i1 = a_ptr[0] * x1 - a_ptr[1] * x0;
      temp_r1 += a_ptr[2] * x2 + a_ptr[3] * x3;
      temp_i1 += a_ptr[2] * x3 - a_ptr[3] * x2;
#endif

#if !defined(XCONJ)
      y_ptr[0] += ar * temp_r - ai * temp_i;
      y_ptr[1] += ar * temp_i + ai * temp_r;
      y_ptr += inc_y;
      y_ptr[0] += ar * temp_r1 - ai * temp_i1;
      y_ptr[1] += ar * temp_i1 + ai * temp_r1;
#else
      y_ptr[0] += ar * temp_r + ai * temp_i;
      y_ptr[1] -= ar * temp_i - ai * temp_r;
      y_ptr += inc_y;
      y_ptr[0] += ar * temp_r1 + ai * temp_i1;
      y_ptr[1] -= ar * temp_i1 - ai * temp_r1;
#endif

      a_ptr += lda;
      y_ptr += inc_y;
      j += 2;
    }

    while (j < n) {
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
      temp_r = a_ptr[0] * x0 - a_ptr[1] * x1;
      temp_i = a_ptr[0] * x1 + a_ptr[1] * x0;
      temp_r += a_ptr[2] * x2 - a_ptr[3] * x3;
      temp_i += a_ptr[2] * x3 + a_ptr[3] * x2;
#else

      temp_r = a_ptr[0] * x0 + a_ptr[1] * x1;
      temp_i = a_ptr[0] * x1 - a_ptr[1] * x0;
      temp_r += a_ptr[2] * x2 + a_ptr[3] * x3;
      temp_i += a_ptr[2] * x3 - a_ptr[3] * x2;
#endif

#if !defined(XCONJ)
      y_ptr[0] += ar * temp_r - ai * temp_i;
      y_ptr[1] += ar * temp_i + ai * temp_r;
#else
      y_ptr[0] += ar * temp_r + ai * temp_i;
      y_ptr[1] -= ar * temp_i - ai * temp_r;
#endif

      a_ptr += lda;
      y_ptr += inc_y;
      j++;
    }

    return (0);
  }

  if (m3 == 1) {

    FLOAT temp_r;
    FLOAT temp_i;
    FLOAT temp_r1;
    FLOAT temp_i1;
    FLOAT x0 = x_ptr[0];
    FLOAT x1 = x_ptr[1];
    FLOAT ar = alpha[0];
    FLOAT ai = alpha[1];

    while (j < (n & -2)) {
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
      temp_r = a_ptr[0] * x0 - a_ptr[1] * x1;
      temp_i = a_ptr[0] * x1 + a_ptr[1] * x0;
      a_ptr += lda;
      temp_r1 = a_ptr[0] * x0 - a_ptr[1] * x1;
      temp_i1 = a_ptr[0] * x1 + a_ptr[1] * x0;
#else

      temp_r = a_ptr[0] * x0 + a_ptr[1] * x1;
      temp_i = a_ptr[0] * x1 - a_ptr[1] * x0;
      a_ptr += lda;
      temp_r1 = a_ptr[0] * x0 + a_ptr[1] * x1;
      temp_i1 = a_ptr[0] * x1 - a_ptr[1] * x0;
#endif

#if !defined(XCONJ)
      y_ptr[0] += ar * temp_r - ai * temp_i;
      y_ptr[1] += ar * temp_i + ai * temp_r;
      y_ptr += inc_y;
      y_ptr[0] += ar * temp_r1 - ai * temp_i1;
      y_ptr[1] += ar * temp_i1 + ai * temp_r1;
#else
      y_ptr[0] += ar * temp_r + ai * temp_i;
      y_ptr[1] -= ar * temp_i - ai * temp_r;
      y_ptr += inc_y;
      y_ptr[0] += ar * temp_r1 + ai * temp_i1;
      y_ptr[1] -= ar * temp_i1 - ai * temp_r1;
#endif

      a_ptr += lda;
      y_ptr += inc_y;
      j += 2;
    }

    while (j < n) {
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
      temp_r = a_ptr[0] * x0 - a_ptr[1] * x1;
      temp_i = a_ptr[0] * x1 + a_ptr[1] * x0;
#else

      temp_r = a_ptr[0] * x0 + a_ptr[1] * x1;
      temp_i = a_ptr[0] * x1 - a_ptr[1] * x0;
#endif

#if !defined(XCONJ)
      y_ptr[0] += ar * temp_r - ai * temp_i;
      y_ptr[1] += ar * temp_i + ai * temp_r;
#else
      y_ptr[0] += ar * temp_r + ai * temp_i;
      y_ptr[1] -= ar * temp_i - ai * temp_r;
#endif

      a_ptr += lda;
      y_ptr += inc_y;
      j++;
    }
    return (0);
  }

  return (0);
}
