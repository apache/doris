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

#include <stdio.h>
#include "common.h"

#ifndef UNIT
#define INV(a) (ONE / (a))
#else
#define INV(a) (ONE)
#endif

int CNAME(BLASLONG m, BLASLONG n, FLOAT *a, BLASLONG lda, BLASLONG offset, FLOAT *b){

  BLASLONG i, ii, j, jj;

  FLOAT data01, data02, data03, data04, data05, data06;
  FLOAT data09, data10, data11, data12, data13, data14;
  FLOAT data17, data18, data19, data20, data21, data22;
  FLOAT data25, data26, data27, data28, data29, data30;
  FLOAT data33, data34, data35, data36, data37, data38;
  FLOAT data41, data42, data43, data44, data45, data46;

  FLOAT *a1, *a2, *a3, *a4, *a5, *a6, *a7, *a8;

  jj = offset;

  BLASLONG mmod6, nmod6, k;
  mmod6 = m - (m/6)*6 ;
  nmod6 = n - (n/6)*6 ;

  // j = (n >> 3);
  j = (n / 6);
  while (j > 0){

    a1 = a + 0 * lda;
    a2 = a + 1 * lda;
    a3 = a + 2 * lda;
    a4 = a + 3 * lda;
    a5 = a + 4 * lda;
    a6 = a + 5 * lda;

    ii = 0;
    // i = (m >> 3);
    i = (m / 6);
    while (i > 0) {

      if (ii == jj) {

#ifndef UNIT
	data01 = *(a1 + 0);
#endif
	data02 = *(a1 + 1);
	data03 = *(a1 + 2);
	data04 = *(a1 + 3);
	data05 = *(a1 + 4);
	data06 = *(a1 + 5);

#ifndef UNIT
	data10 = *(a2 + 1);
#endif
	data11 = *(a2 + 2);
	data12 = *(a2 + 3);
	data13 = *(a2 + 4);
	data14 = *(a2 + 5);

#ifndef UNIT
	data19 = *(a3 + 2);
#endif
	data20 = *(a3 + 3);
	data21 = *(a3 + 4);
	data22 = *(a3 + 5);

#ifndef UNIT
	data28 = *(a4 + 3);
#endif
	data29 = *(a4 + 4);
	data30 = *(a4 + 5);

#ifndef UNIT
	data37 = *(a5 + 4);
#endif
	data38 = *(a5 + 5);

#ifndef UNIT
	data46 = *(a6 + 5);
#endif

	*(b +  0) = INV(data01);
	*(b +  1) = data02;
	*(b +  2) = data03;
	*(b +  3) = data04;
	*(b +  4) = data05;
	*(b +  5) = data06;

	*(b +  7) = INV(data10);
	*(b + 8) = data11;
	*(b + 9) = data12;
	*(b + 10) = data13;
	*(b + 11) = data14;

	*(b + 14) = INV(data19);
	*(b + 15) = data20;
	*(b + 16) = data21;
	*(b + 17) = data22;

	*(b + 21) = INV(data28);
	*(b + 22) = data29;
	*(b + 23) = data30;

	*(b + 28) = INV(data37);
	*(b + 29) = data38;

	*(b + 35) = INV(data46);
      }

      if (ii < jj) {
	data01 = *(a1 + 0);
	data02 = *(a1 + 1);
	data03 = *(a1 + 2);
	data04 = *(a1 + 3);
	data05 = *(a1 + 4);
	data06 = *(a1 + 5);

	data09 = *(a2 + 0);
	data10 = *(a2 + 1);
	data11 = *(a2 + 2);
	data12 = *(a2 + 3);
	data13 = *(a2 + 4);
	data14 = *(a2 + 5);

	data17 = *(a3 + 0);
	data18 = *(a3 + 1);
	data19 = *(a3 + 2);
	data20 = *(a3 + 3);
	data21 = *(a3 + 4);
	data22 = *(a3 + 5);

	data25 = *(a4 + 0);
	data26 = *(a4 + 1);
	data27 = *(a4 + 2);
	data28 = *(a4 + 3);
	data29 = *(a4 + 4);
	data30 = *(a4 + 5);

	data33 = *(a5 + 0);
	data34 = *(a5 + 1);
	data35 = *(a5 + 2);
	data36 = *(a5 + 3);
	data37 = *(a5 + 4);
	data38 = *(a5 + 5);

	data41 = *(a6 + 0);
	data42 = *(a6 + 1);
	data43 = *(a6 + 2);
	data44 = *(a6 + 3);
	data45 = *(a6 + 4);
	data46 = *(a6 + 5);

	*(b +  0) = data01;
	*(b +  1) = data02;
	*(b +  2) = data03;
	*(b +  3) = data04;
	*(b +  4) = data05;
	*(b +  5) = data06;
	*(b +  6) = data09;
	*(b +  7) = data10;
	*(b + 8) = data11;
	*(b + 9) = data12;
	*(b + 10) = data13;
	*(b + 11) = data14;

	*(b + 12) = data17;
	*(b + 13) = data18;
	*(b + 14) = data19;
	*(b + 15) = data20;
	*(b + 16) = data21;
	*(b + 17) = data22;
	*(b + 18) = data25;
	*(b + 19) = data26;
	*(b + 20) = data27;
	*(b + 21) = data28;
	*(b + 22) = data29;
	*(b + 23) = data30;

	*(b + 24) = data33;
	*(b + 25) = data34;
	*(b + 26) = data35;
	*(b + 27) = data36;
	*(b + 28) = data37;
	*(b + 29) = data38;
	*(b + 30) = data41;
	*(b + 31) = data42;
	*(b + 32) = data43;
	*(b + 33) = data44;
	*(b + 34) = data45;
	*(b + 35) = data46;
      }

      a1 += 6 * lda;
      a2 += 6 * lda;
      a3 += 6 * lda;
      a4 += 6 * lda;
      a5 += 6 * lda;
      a6 += 6 * lda;
      a7 += 6 * lda;
      a8 += 6 * lda;
      b += 36;

      i  --;
      ii += 6;
    }

    if (mmod6 & 4) {
      if (ii == jj) {

#ifndef UNIT
	data01 = *(a1 + 0);
#endif
	data02 = *(a1 + 1);
	data03 = *(a1 + 2);
	data04 = *(a1 + 3);
	data05 = *(a1 + 4);
	data06 = *(a1 + 5);

#ifndef UNIT
	data10 = *(a2 + 1);
#endif
	data11 = *(a2 + 2);
	data12 = *(a2 + 3);
	data13 = *(a2 + 4);
	data14 = *(a2 + 5);

#ifndef UNIT
	data19 = *(a3 + 2);
#endif
	data20 = *(a3 + 3);
	data21 = *(a3 + 4);
	data22 = *(a3 + 5);

#ifndef UNIT
	data28 = *(a4 + 3);
#endif
	data29 = *(a4 + 4);
	data30 = *(a4 + 5);

	*(b +  0) = INV(data01);
	*(b +  1) = data02;
	*(b +  2) = data03;
	*(b +  3) = data04;
	*(b +  4) = data05;
	*(b +  5) = data06;

	*(b +  7) = INV(data10);
	*(b + 8) = data11;
	*(b + 9) = data12;
	*(b + 10) = data13;
	*(b + 11) = data14;

	*(b + 14) = INV(data19);
	*(b + 15) = data20;
	*(b + 16) = data21;
	*(b + 17) = data22;

	*(b + 21) = INV(data28);
	*(b + 22) = data29;
	*(b + 23) = data30;
      }

      if (ii < jj) {
	data01 = *(a1 + 0);
	data02 = *(a1 + 1);
	data03 = *(a1 + 2);
	data04 = *(a1 + 3);
	data05 = *(a1 + 4);
	data06 = *(a1 + 5);

	data09 = *(a2 + 0);
	data10 = *(a2 + 1);
	data11 = *(a2 + 2);
	data12 = *(a2 + 3);
	data13 = *(a2 + 4);
	data14 = *(a2 + 5);

	data17 = *(a3 + 0);
	data18 = *(a3 + 1);
	data19 = *(a3 + 2);
	data20 = *(a3 + 3);
	data21 = *(a3 + 4);
	data22 = *(a3 + 5);

	data25 = *(a4 + 0);
	data26 = *(a4 + 1);
	data27 = *(a4 + 2);
	data28 = *(a4 + 3);
	data29 = *(a4 + 4);
	data30 = *(a4 + 5);

	*(b +  0) = data01;
	*(b +  1) = data02;
	*(b +  2) = data03;
	*(b +  3) = data04;
	*(b +  4) = data05;
	*(b +  5) = data06;
	*(b +  6) = data09;
	*(b +  7) = data10;
	*(b + 8) = data11;
	*(b + 9) = data12;
	*(b + 10) = data13;
	*(b + 11) = data14;

	*(b + 12) = data17;
	*(b + 13) = data18;
	*(b + 14) = data19;
	*(b + 15) = data20;
	*(b + 16) = data21;
	*(b + 17) = data22;
	*(b + 18) = data25;
	*(b + 19) = data26;
	*(b + 20) = data27;
	*(b + 21) = data28;
	*(b + 22) = data29;
	*(b + 23) = data30;
      }

      a1 += 4 * lda;
      a2 += 4 * lda;
      /* a3 += 4 * lda;
      a4 += 4 * lda; */
      b += 24;

      ii += 4;
    }

    if (mmod6 & 2) {
      if (ii == jj) {

#ifndef UNIT
	data01 = *(a1 + 0);
#endif
	data02 = *(a1 + 1);
	data03 = *(a1 + 2);
	data04 = *(a1 + 3);
	data05 = *(a1 + 4);
	data06 = *(a1 + 5);

#ifndef UNIT
	data10 = *(a2 + 1);
#endif
	data11 = *(a2 + 2);
	data12 = *(a2 + 3);
	data13 = *(a2 + 4);
	data14 = *(a2 + 5);

	*(b +  0) = INV(data01);
	*(b +  1) = data02;
	*(b +  2) = data03;
	*(b +  3) = data04;
	*(b +  4) = data05;
	*(b +  5) = data06;

	*(b +  7) = INV(data10);
	*(b + 8) = data11;
	*(b + 9) = data12;
	*(b + 10) = data13;
	*(b + 11) = data14;
      }

      if (ii < jj) {
	data01 = *(a1 + 0);
	data02 = *(a1 + 1);
	data03 = *(a1 + 2);
	data04 = *(a1 + 3);
	data05 = *(a1 + 4);
	data06 = *(a1 + 5);

	data09 = *(a2 + 0);
	data10 = *(a2 + 1);
	data11 = *(a2 + 2);
	data12 = *(a2 + 3);
	data13 = *(a2 + 4);
	data14 = *(a2 + 5);

	*(b +  0) = data01;
	*(b +  1) = data02;
	*(b +  2) = data03;
	*(b +  3) = data04;
	*(b +  4) = data05;
	*(b +  5) = data06;
	*(b +  6) = data09;
	*(b +  7) = data10;
	*(b + 8) = data11;
	*(b + 9) = data12;
	*(b + 10) = data13;
	*(b + 11) = data14;
      }

      a1 += 2 * lda;
      // a2 += 2 * lda;
      b += 12;

      ii += 2;
    }

    if (mmod6 & 1) {
      if (ii == jj) {

#ifndef UNIT
	data01 = *(a1 + 0);
#endif
	data02 = *(a1 + 1);
	data03 = *(a1 + 2);
	data04 = *(a1 + 3);
	data05 = *(a1 + 4);
	data06 = *(a1 + 5);

	*(b +  0) = INV(data01);
	*(b +  1) = data02;
	*(b +  2) = data03;
	*(b +  3) = data04;
	*(b +  4) = data05;
	*(b +  5) = data06;
      }

      if (ii < jj) {
	data01 = *(a1 + 0);
	data02 = *(a1 + 1);
	data03 = *(a1 + 2);
	data04 = *(a1 + 3);
	data05 = *(a1 + 4);
	data06 = *(a1 + 5);

	*(b +  0) = data01;
	*(b +  1) = data02;
	*(b +  2) = data03;
	*(b +  3) = data04;
	*(b +  4) = data05;
	*(b +  5) = data06;
      }
      b += 6;
    }
    a += 6;
    jj += 6;
    j  --;
  }

  if (nmod6 & 4) {

    a1 = a;
    a += 4;
    ii = 0;

    for (i = 0; i < m; i++) {

      if ((ii >= jj ) && (ii - jj < 4)) {
	*(b +  ii - jj) = INV(*(a1 + ii - jj));

	for (k = ii - jj + 1; k < 4; k ++) {
	  *(b +  k) = *(a1 +  k);
	}

      }

      if (ii - jj < 0) {
	*(b +  0) = *(a1 +  0);
	*(b +  1) = *(a1 +  1);
	*(b +  2) = *(a1 +  2);
	*(b +  3) = *(a1 +  3);
      }

      b  += 4;
      a1 += lda;
      ii ++;
    }

    jj += 4;
  }

  if (nmod6 & 2) {

    a1 = a + 0 * lda;
    a2 = a + 1 * lda;

    ii = 0;
    i = (m >> 1);
    while (i > 0) {

      if (ii == jj) {

#ifndef UNIT
	data01 = *(a1 + 0);
#endif
	data02 = *(a1 + 1);

#ifndef UNIT
	data10 = *(a2 + 1);
#endif

	*(b +  0) = INV(data01);
	*(b +  1) = data02;
	*(b +  3) = INV(data10);
      }

      if (ii < jj) {
	data01 = *(a1 + 0);
	data02 = *(a1 + 1);
	data09 = *(a2 + 0);
	data10 = *(a2 + 1);

	*(b +  0) = data01;
	*(b +  1) = data02;
	*(b +  2) = data09;
	*(b +  3) = data10;
      }

      a1 += 2 * lda;
      a2 += 2 * lda;
      b += 4;

      i  --;
      ii += 2;
    }

    if (m & 1) {
      if (ii == jj) {

#ifndef UNIT
	data01 = *(a1 + 0);
#endif
	// data02 = *(a1 + 1);

	*(b +  0) = INV(data01);
	// *(b +  1) = data02;
      }

      if (ii < jj) {
	data01 = *(a1 + 0);
	data02 = *(a1 + 1);

	*(b +  0) = data01;
	*(b +  1) = data02;
      }
      b += 2;
    }
    a += 2;
    jj += 2;
  }

  if (nmod6 & 1) {

    a1 = a + 0 * lda;

    ii = 0;
    i = m;
    while (i > 0) {

      if (ii == jj) {
#ifndef UNIT
	data01 = *(a1 + 0);
#endif
	*(b +  0) = INV(data01);
      }

      if (ii < jj) {
	data01 = *(a1 + 0);
	*(b +  0) = data01;
      }

      a1 += lda;
      b += 1;

      i  --;
      ii += 1;
    }

  }

  return 0;
}
