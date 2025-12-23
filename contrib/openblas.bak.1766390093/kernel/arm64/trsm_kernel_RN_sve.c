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
#define GEMM_KERNEL   GEMM_KERNEL_R
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

  FLOAT aa, bb;

  int i, j, k;

  for (i = 0; i < n; i++) {

    bb = *(b + i);

    for (j = 0; j < m; j ++) {
      aa = *(c + j + i * ldc);
      aa *= bb;
      *a  = aa;
      *(c + j + i * ldc) = aa;
      a ++;

      for (k = i + 1; k < n; k ++){
	*(c + j + k * ldc) -= aa * *(b + k);
      }

    }
    b += n;
  }
}

#else

static inline void solve(BLASLONG m, BLASLONG n, FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc) {

  FLOAT aa1, aa2;
  FLOAT bb1, bb2;
  FLOAT cc1, cc2;

  int i, j, k;

  ldc *= 2;

  for (i = 0; i < n; i++) {

    bb1 = *(b + i * 2 + 0);
    bb2 = *(b + i * 2 + 1);

    for (j = 0; j < m; j ++) {
      aa1 = *(c + j * 2 + 0 + i * ldc);
      aa2 = *(c + j * 2 + 1 + i * ldc);

#ifndef CONJ
      cc1 = aa1 * bb1 - aa2 * bb2;
      cc2 = aa1 * bb2 + aa2 * bb1;
#else
      cc1 =  aa1 * bb1 + aa2 * bb2;
      cc2 = -aa1 * bb2 + aa2 * bb1;
#endif

      *(a + 0) = cc1;
      *(a + 1) = cc2;
      *(c + j * 2 + 0 + i * ldc) = cc1;
      *(c + j * 2 + 1 + i * ldc) = cc2;
      a += 2;

      for (k = i + 1; k < n; k ++){
#ifndef CONJ
	*(c + j * 2 + 0 + k * ldc) -= cc1 * *(b + k * 2 + 0) - cc2 * *(b + k * 2 + 1);
	*(c + j * 2 + 1 + k * ldc) -= cc1 * *(b + k * 2 + 1) + cc2 * *(b + k * 2 + 0);
#else
	*(c + j * 2 + 0 + k * ldc) -=   cc1 * *(b + k * 2 + 0) + cc2 * *(b + k * 2 + 1);
	*(c + j * 2 + 1 + k * ldc) -= - cc1 * *(b + k * 2 + 1) + cc2 * *(b + k * 2 + 0);
#endif
      }

    }
    b += n * 2;
  }
}

#endif


int CNAME(BLASLONG m, BLASLONG n, BLASLONG k, FLOAT dummy1,
#ifdef COMPLEX
	   FLOAT dummy2,
#endif
	   FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG offset){

  FLOAT *aa, *cc;
  BLASLONG  kk;
  BLASLONG i, j, jj;
#ifdef DOUBLE
  int sve_size = svcntd();
#else
  int sve_size = svcntw();
#endif

#if 0
  fprintf(stderr, "TRSM RN KERNEL m = %3ld  n = %3ld  k = %3ld offset = %3ld\n",
	  m, n, k, offset);
#endif

  jj = 0;
  j = (n >> GEMM_UNROLL_N_SHIFT);
  kk = -offset;

  while (j > 0) {

    aa = a;
    cc = c;

    i = sve_size;

    if (i <= m) {
      do {
	if (kk > 0) {
	  GEMM_KERNEL(sve_size, GEMM_UNROLL_N, kk, dm1,
#ifdef COMPLEX
		      ZERO,
#endif
		      aa, b, cc, ldc);
	}

	solve(sve_size, GEMM_UNROLL_N,
	      aa + kk * sve_size * COMPSIZE,
	      b  + kk * GEMM_UNROLL_N * COMPSIZE,
	      cc, ldc);

	aa += sve_size * k * COMPSIZE;
	cc += sve_size     * COMPSIZE;
	i += sve_size;
      } while (i <= m);
    }


    i = m % sve_size;
    if (i) {
      if (kk > 0) {
        GEMM_KERNEL(i, GEMM_UNROLL_N, kk, dm1,
#ifdef COMPLEX
            ZERO,
#endif
            aa, b, cc, ldc);
      }
      solve(i, GEMM_UNROLL_N,
          aa + kk * i             * COMPSIZE,
          b  + kk * GEMM_UNROLL_N * COMPSIZE,
          cc, ldc);

      aa += i * k * COMPSIZE;
      cc += i     * COMPSIZE;

    }

    kk += GEMM_UNROLL_N;
    b += GEMM_UNROLL_N * k   * COMPSIZE;
    c += GEMM_UNROLL_N * ldc * COMPSIZE;
    j --;
    jj += sve_size;
  }

  if (n & (GEMM_UNROLL_N - 1)) {

    j = (GEMM_UNROLL_N >> 1);
    while (j > 0) {
      if (n & j) {

	aa = a;
	cc = c;

  i = sve_size;

	while (i <= m) {
	  if (kk > 0) {
	    GEMM_KERNEL(sve_size, j, kk, dm1,
#ifdef COMPLEX
			ZERO,
#endif
			aa,
			b,
			cc,
			ldc);
	  }

	  solve(sve_size, j,
		aa + kk * sve_size * COMPSIZE,
		b  + kk * j             * COMPSIZE, cc, ldc);

	  aa += sve_size * k * COMPSIZE;
	  cc += sve_size     * COMPSIZE;
	  i += sve_size;
	}

  i = m % sve_size;
  if (i) {
	      if (kk > 0) {
		GEMM_KERNEL(i, j, kk, dm1,
#ifdef COMPLEX
			    ZERO,
#endif
			    aa,
			    b,
			    cc,
			    ldc);
	      }

	      solve(i, j,
		    aa + kk * i * COMPSIZE,
		    b  + kk * j * COMPSIZE, cc, ldc);

	      aa += i * k * COMPSIZE;
	      cc += i     * COMPSIZE;

  }

	b += j * k   * COMPSIZE;
	c += j * ldc * COMPSIZE;
	kk += j;
      }
      j >>= 1;
    }
  }

  return 0;
}
