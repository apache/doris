/*********************************************************************/
/* Copyright 2009, 2010 The University of Texas at Austin.           */
/* All rights reserved.                                              */
/*                                                                   */
/* Redistribution and use in source and binary forms, with or        */
/* without modification, are permitted provided that the following   */
/* conditions are met:                                               */
/*                                                                   */
/*   1. Redistributions of source code must retain the above         */
/*      copyright notice, this list of conditions and the following  */
/*      disclaimer.                                                  */
/*                                                                   */
/*   2. Redistributions in binary form must reproduce the above      */
/*      copyright notice, this list of conditions and the following  */
/*      disclaimer in the documentation and/or other materials       */
/*      provided with the distribution.                              */
/*                                                                   */
/*    THIS  SOFTWARE IS PROVIDED  BY THE  UNIVERSITY OF  TEXAS AT    */
/*    AUSTIN  ``AS IS''  AND ANY  EXPRESS OR  IMPLIED WARRANTIES,    */
/*    INCLUDING, BUT  NOT LIMITED  TO, THE IMPLIED  WARRANTIES OF    */
/*    MERCHANTABILITY  AND FITNESS FOR  A PARTICULAR  PURPOSE ARE    */
/*    DISCLAIMED.  IN  NO EVENT SHALL THE UNIVERSITY  OF TEXAS AT    */
/*    AUSTIN OR CONTRIBUTORS BE  LIABLE FOR ANY DIRECT, INDIRECT,    */
/*    INCIDENTAL,  SPECIAL, EXEMPLARY,  OR  CONSEQUENTIAL DAMAGES    */
/*    (INCLUDING, BUT  NOT LIMITED TO,  PROCUREMENT OF SUBSTITUTE    */
/*    GOODS  OR  SERVICES; LOSS  OF  USE,  DATA,  OR PROFITS;  OR    */
/*    BUSINESS INTERRUPTION) HOWEVER CAUSED  AND ON ANY THEORY OF    */
/*    LIABILITY, WHETHER  IN CONTRACT, STRICT  LIABILITY, OR TORT    */
/*    (INCLUDING NEGLIGENCE OR OTHERWISE)  ARISING IN ANY WAY OUT    */
/*    OF  THE  USE OF  THIS  SOFTWARE,  EVEN  IF ADVISED  OF  THE    */
/*    POSSIBILITY OF SUCH DAMAGE.                                    */
/*                                                                   */
/* The views and conclusions contained in the software and           */
/* documentation are those of the authors and should not be          */
/* interpreted as representing official policies, either expressed   */
/* or implied, of The University of Texas at Austin.                 */
/*********************************************************************/

#include "common.h"
#include "arm_sve.h"

static FLOAT dm1 = -1.;

#ifdef CONJ
#define GEMM_KERNEL   GEMM_KERNEL_L
#else
#define GEMM_KERNEL   GEMM_KERNEL_N
#endif

#if GEMM_DEFAULT_UNROLL_N == 1
#define GEMM_UNROLL_N_SHIFT 0
#endif

#if GEMM_DEFAULT_UNROLL_N == 2
#define GEMM_UNROLL_N_SHIFT 1
#endif

#if GEMM_DEFAULT_UNROLL_N == 4
#define GEMM_UNROLL_N_SHIFT 2
#endif

#if GEMM_DEFAULT_UNROLL_N == 8
#define GEMM_UNROLL_N_SHIFT 3
#endif

#if GEMM_DEFAULT_UNROLL_N == 16
#define GEMM_UNROLL_N_SHIFT 4
#endif

#ifndef COMPLEX

static inline void solve(BLASLONG m, BLASLONG n, FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc) {

  FLOAT aa,  bb;

  int i, j, k;

  a += (m - 1) * m;
  b += (m - 1) * n;

  for (i = m - 1; i >= 0; i--) {

    aa = *(a + i);

    for (j = 0; j < n; j ++) {
      bb = *(c + i + j * ldc);
      bb *= aa;
      *b             = bb;
      *(c + i + j * ldc) = bb;
      b ++;

      for (k = 0; k < i; k ++){
        *(c + k + j * ldc) -= bb * *(a + k);
      }

    }
    a -= m;
    b -= 2 * n;
  }

}

#else

static inline void solve(BLASLONG m, BLASLONG n, FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc) {

  FLOAT aa1, aa2;
  FLOAT bb1, bb2;
  FLOAT cc1, cc2;

  int i, j, k;

  ldc *= 2;
  a += (m - 1) * m * 2;
  b += (m - 1) * n * 2;

  for (i = m - 1; i >= 0; i--) {

    aa1 = *(a + i * 2 + 0);
    aa2 = *(a + i * 2 + 1);

    for (j = 0; j < n; j ++) {
      bb1 = *(c + i * 2 + 0 + j * ldc);
      bb2 = *(c + i * 2 + 1 + j * ldc);

#ifndef CONJ
      cc1 = aa1 * bb1 - aa2 * bb2;
      cc2 = aa1 * bb2 + aa2 * bb1;
#else
      cc1 = aa1 * bb1 + aa2 * bb2;
      cc2 = aa1 * bb2 - aa2 * bb1;
#endif


      *(b + 0) = cc1;
      *(b + 1) = cc2;
      *(c + i * 2 + 0 + j * ldc) = cc1;
      *(c + i * 2 + 1 + j * ldc) = cc2;
      b += 2;

      for (k = 0; k < i; k ++){
#ifndef CONJ
        *(c + k * 2 + 0 + j * ldc) -= cc1 * *(a + k * 2 + 0) - cc2 * *(a + k * 2 + 1);
        *(c + k * 2 + 1 + j * ldc) -= cc1 * *(a + k * 2 + 1) + cc2 * *(a + k * 2 + 0);
#else
        *(c + k * 2 + 0 + j * ldc) -=   cc1 * *(a + k * 2 + 0) + cc2 * *(a + k * 2 + 1);
        *(c + k * 2 + 1 + j * ldc) -= - cc1 * *(a + k * 2 + 1) + cc2 * *(a + k * 2 + 0);
#endif
      }

    }
    a -= m * 2;
    b -= 4 * n;
  }

}

#endif


int CNAME(BLASLONG m, BLASLONG n, BLASLONG k,  FLOAT dummy1,
#ifdef COMPLEX
    FLOAT dummy2,
#endif
    FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG offset){

  BLASLONG i, j;
  FLOAT *aa, *cc;
  BLASLONG  kk;
#ifdef DOUBLE
  int sve_size = svcntd();
#else
  int sve_size = svcntw();
#endif

#if 0
  fprintf(stderr, "TRSM KERNEL LN : m = %3ld  n = %3ld  k = %3ld offset = %3ld\n",
      m, n, k, offset);
#endif

  j = (n >> GEMM_UNROLL_N_SHIFT);

  while (j > 0) {

    kk = m + offset;

    i = m % sve_size;
    if (i) {
      aa = a + (m - i) * k * COMPSIZE;
      cc = c + (m - i)     * COMPSIZE;

      if (k - kk > 0) {
        GEMM_KERNEL(i, GEMM_UNROLL_N, k - kk, dm1,
#ifdef COMPLEX
            ZERO,
#endif
            aa + i             * kk * COMPSIZE,
            b  + GEMM_UNROLL_N * kk * COMPSIZE,
            cc,
            ldc);
      }

      solve(i, GEMM_UNROLL_N,
          aa + (kk - i) * i             * COMPSIZE,
          b  + (kk - i) * GEMM_UNROLL_N * COMPSIZE,
          cc, ldc);

      kk -= i;

    }

    int mod = i;
    i = sve_size;
    if (i <= m) {
      aa = a + (m - mod - sve_size) * k * COMPSIZE;
      cc = c + (m - mod - sve_size)     * COMPSIZE;

      do {
        if (k - kk > 0) {
          GEMM_KERNEL(sve_size, GEMM_UNROLL_N, k - kk, dm1,
#ifdef COMPLEX
              ZERO,
#endif
              aa + sve_size * kk * COMPSIZE,
              b +  GEMM_UNROLL_N * kk * COMPSIZE,
              cc,
              ldc);
        }

        solve(sve_size, GEMM_UNROLL_N,
            aa + (kk - sve_size) * sve_size * COMPSIZE,
            b  + (kk - sve_size) * GEMM_UNROLL_N * COMPSIZE,
            cc, ldc);

        aa -= sve_size * k * COMPSIZE;
        cc -= sve_size     * COMPSIZE;
        kk -= sve_size;

        i += sve_size;
      } while (i <= m);
    }


    b += GEMM_UNROLL_N * k * COMPSIZE;
    c += GEMM_UNROLL_N * ldc * COMPSIZE;
    j --;
  }

  if (n & (GEMM_UNROLL_N - 1)) {

    j = (GEMM_UNROLL_N >> 1);
    while (j > 0) {
      if (n & j) {

        kk = m + offset;

        i = m % sve_size;
        if (i) {
          aa = a + (m - i) * k * COMPSIZE;
          cc = c + (m - i)     * COMPSIZE;

          if (k - kk > 0) {
            GEMM_KERNEL(i, j, k - kk, dm1,
#ifdef COMPLEX
                ZERO,
#endif
                aa + i * kk * COMPSIZE,
                b  + j * kk * COMPSIZE,
                cc, ldc);
          }

          solve(i, j,
              aa + (kk - i) * i * COMPSIZE,
              b  + (kk - i) * j * COMPSIZE,
              cc, ldc);

          kk -= i;

        }

        int mod = i;
        i = sve_size;
        if (i <= m) {
          aa = a + (m - mod - sve_size) * k * COMPSIZE;
          cc = c + (m - mod - sve_size)     * COMPSIZE;

          do {
            if (k - kk > 0) {
              GEMM_KERNEL(sve_size, j, k - kk, dm1,
#ifdef COMPLEX
                  ZERO,
#endif
                  aa + sve_size * kk * COMPSIZE,
                  b +  j             * kk * COMPSIZE,
                  cc,
                  ldc);
            }

            solve(sve_size, j,
                aa + (kk - sve_size) * sve_size * COMPSIZE,
                b  + (kk - sve_size) * j             * COMPSIZE,
                cc, ldc);

            aa -= sve_size * k * COMPSIZE;
            cc -= sve_size     * COMPSIZE;
            kk -= sve_size;

            i += sve_size;
          } while (i <= m);
        }

        b += j * k   * COMPSIZE;
        c += j * ldc * COMPSIZE;
      }
      j >>= 1;
    }
  }

  return 0;
}
