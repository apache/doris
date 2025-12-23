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
#include <altivec.h>
#include "common.h"

typedef IFLOAT vec_bf16 __attribute__ ((vector_size (16)));
int CNAME(BLASLONG m, BLASLONG n, IFLOAT *a, BLASLONG lda, IFLOAT *b){
  BLASLONG i, j;

  IFLOAT *aoffset;
  IFLOAT *aoffset1, *aoffset2, *aoffset3, *aoffset4;
  IFLOAT *aoffset5, *aoffset6, *aoffset7, *aoffset8;

  IFLOAT *boffset;
  vec_bf16 vtemp01, vtemp02, vtemp03, vtemp04;
  vec_bf16 vtemp05, vtemp06, vtemp07, vtemp08;
  vec_bf16 vtemp09, vtemp10, vtemp11, vtemp12;
  vector char mask =
  { 0, 1, 2, 3, 16, 17, 18, 19, 4, 5, 6, 7, 20, 21, 22, 23 };
  vector char mask1 =
  { 8, 9, 10, 11, 24, 25, 26, 27, 12, 13, 14, 15, 28, 29, 30, 31 };
  IFLOAT ctemp01, ctemp02, ctemp03, ctemp04;
  IFLOAT ctemp05, ctemp06, ctemp07, ctemp08;
  IFLOAT ctemp09, ctemp10, ctemp11, ctemp12;
  IFLOAT ctemp13, ctemp14, ctemp15, ctemp16;
  IFLOAT ctemp17;
  IFLOAT ctemp25;
  IFLOAT ctemp33;
  IFLOAT ctemp41;
  IFLOAT ctemp49;
  IFLOAT ctemp57;


  aoffset = a;
  boffset = b;

  j = (n >> 3);
  if (j > 0){
    do{
      aoffset1  = aoffset;
      aoffset2  = aoffset1 + lda;
      aoffset3  = aoffset2 + lda;
      aoffset4  = aoffset3 + lda;
      aoffset5  = aoffset4 + lda;
      aoffset6  = aoffset5 + lda;
      aoffset7  = aoffset6 + lda;
      aoffset8  = aoffset7 + lda;
      aoffset += 8 * lda;

      i = (m >> 3);
      if (i > 0){
	do{
	  vtemp01 = *(vec_bf16 *)(aoffset1);
	  vtemp02 = *(vec_bf16 *)(aoffset2);
	  vtemp03 = *(vec_bf16 *)(aoffset3);
	  vtemp04 = *(vec_bf16 *)(aoffset4);
	  vtemp05 = *(vec_bf16 *)(aoffset5);
	  vtemp06 = *(vec_bf16 *)(aoffset6);
	  vtemp07 = *(vec_bf16 *)(aoffset7);
	  vtemp08 = *(vec_bf16 *)(aoffset8);

	  vtemp09 = vec_perm(vtemp01, vtemp02, mask);
	  vtemp10 = vec_perm(vtemp03, vtemp04, mask);
	  vtemp11 = vec_perm(vtemp05, vtemp06, mask);
	  vtemp12 = vec_perm(vtemp07, vtemp08, mask);

	  *(vec_bf16 *)(boffset + 0) = vec_xxpermdi(vtemp09, vtemp10, 0);
	  *(vec_bf16 *)(boffset + 8) = vec_xxpermdi(vtemp11, vtemp12, 0);
	  *(vec_bf16 *)(boffset + 16) = vec_xxpermdi(vtemp09, vtemp10, 3);
	  *(vec_bf16 *)(boffset + 24) = vec_xxpermdi(vtemp11, vtemp12, 3);

	  vtemp09 = vec_perm(vtemp01, vtemp02, mask1);
	  vtemp10 = vec_perm(vtemp03, vtemp04, mask1);
	  vtemp11 = vec_perm(vtemp05, vtemp06, mask1);
	  vtemp12 = vec_perm(vtemp07, vtemp08, mask1);

	  *(vec_bf16 *)(boffset + 32) = vec_xxpermdi(vtemp09, vtemp10, 0);
	  *(vec_bf16 *)(boffset + 40) = vec_xxpermdi(vtemp11, vtemp12, 0);
	  *(vec_bf16 *)(boffset + 48) = vec_xxpermdi(vtemp09, vtemp10, 3);
	  *(vec_bf16 *)(boffset + 56) = vec_xxpermdi(vtemp11, vtemp12, 3);

	  aoffset1 +=  8;
	  aoffset2 +=  8;
	  aoffset3 +=  8;
	  aoffset4 +=  8;
	  aoffset5 +=  8;
	  aoffset6 +=  8;
	  aoffset7 +=  8;
	  aoffset8 +=  8;
	  boffset  += 64;
	  i --;
	}while(i > 0);
      }

      i = (m & 7);
      if (i >= 2){
	do{
	  ctemp01 = *(aoffset1 +  0);
	  ctemp09 = *(aoffset1 +  1);
	  ctemp17 = *(aoffset2 +  0);
	  ctemp25 = *(aoffset2 +  1);
	  ctemp33 = *(aoffset3 +  0);
	  ctemp41 = *(aoffset3 +  1);
	  ctemp49 = *(aoffset4 +  0);
	  ctemp57 = *(aoffset4 +  1);

	  *(boffset +  0) = ctemp01;
	  *(boffset +  1) = ctemp09;
	  *(boffset +  2) = ctemp17;
	  *(boffset +  3) = ctemp25;
	  *(boffset +  4) = ctemp33;
	  *(boffset +  5) = ctemp41;
	  *(boffset +  6) = ctemp49;
	  *(boffset +  7) = ctemp57;
	  aoffset1 += 2;
	  aoffset2 += 2;
	  aoffset3 += 2;
	  aoffset4 += 2;

	  ctemp01 = *(aoffset5 +  0);
	  ctemp09 = *(aoffset5 +  1);
	  ctemp17 = *(aoffset6 +  0);
	  ctemp25 = *(aoffset6 +  1);
	  ctemp33 = *(aoffset7 + 0);
	  ctemp41 = *(aoffset7 + 1);
	  ctemp49 = *(aoffset8 + 0);
	  ctemp57 = *(aoffset8 + 1);
	  *(boffset +  8) = ctemp01;
	  *(boffset +  9) = ctemp09;
	  *(boffset +  10) = ctemp17;
	  *(boffset +  11) = ctemp25;
	  *(boffset +  12) = ctemp33;
	  *(boffset +  13) = ctemp41;
	  *(boffset +  14) = ctemp49;
	  *(boffset +  15) = ctemp57;

	  aoffset5 += 2;
	  aoffset6 += 2;
	  aoffset7 += 2;
	  aoffset8 += 2;

	  boffset += 16;
	  i -= 2;
	}while(i > 1);
      }
      if (m & 1){
          ctemp01 = *(aoffset1 +  0);
          ctemp09 = *(aoffset2 +  0);
          ctemp17 = *(aoffset3 +  0);
          ctemp25 = *(aoffset4 +  0);
          ctemp33 = *(aoffset5 +  0);
          ctemp41 = *(aoffset6 +  0);
          ctemp49 = *(aoffset7 +  0);
          ctemp57 = *(aoffset8 +  0);

          *(boffset +  0) = ctemp01;
          *(boffset +  1) = ctemp09;
          *(boffset +  2) = ctemp17;
          *(boffset +  3) = ctemp25;
          *(boffset +  4) = ctemp33;
          *(boffset +  5) = ctemp41;
          *(boffset +  6) = ctemp49;
          *(boffset +  7) = ctemp57;

          aoffset1 ++;
          aoffset2 ++;
          aoffset3 ++;
          aoffset4 ++;
          aoffset5 ++;
          aoffset6 ++;
          aoffset7 ++;
          aoffset8 ++;

          boffset += 8;
      }

      j--;
    }while(j > 0);
  } /* end of if(j > 0) */

  if (n & 4){
    aoffset1  = aoffset;
    aoffset2  = aoffset1 + lda;
    aoffset3  = aoffset2 + lda;
    aoffset4  = aoffset3 + lda;
    aoffset += 4 * lda;

    i = (m >> 2);
    if (i > 0){
      do{
	ctemp01 = *(aoffset1 +  0);
	ctemp02 = *(aoffset1 +  1);
	ctemp03 = *(aoffset1 +  2);
	ctemp04 = *(aoffset1 +  3);

	ctemp05 = *(aoffset2 +  0);
	ctemp06 = *(aoffset2 +  1);
	ctemp07 = *(aoffset2 +  2);
	ctemp08 = *(aoffset2 +  3);

	ctemp09 = *(aoffset3 +  0);
	ctemp10 = *(aoffset3 +  1);
	ctemp11 = *(aoffset3 +  2);
	ctemp12 = *(aoffset3 +  3);

	ctemp13 = *(aoffset4 +  0);
	ctemp14 = *(aoffset4 +  1);
	ctemp15 = *(aoffset4 +  2);
	ctemp16 = *(aoffset4 +  3);

	*(boffset +  0) = ctemp01;
	*(boffset +  1) = ctemp02;
	*(boffset +  2) = ctemp05;
	*(boffset +  3) = ctemp06;

	*(boffset +  4) = ctemp09;
	*(boffset +  5) = ctemp10;
	*(boffset +  6) = ctemp13;
	*(boffset +  7) = ctemp14;

	*(boffset +  8) = ctemp03;
	*(boffset +  9) = ctemp04;
	*(boffset + 10) = ctemp07;
	*(boffset + 11) = ctemp08;

	*(boffset + 12) = ctemp11;
	*(boffset + 13) = ctemp12;
	*(boffset + 14) = ctemp15;
	*(boffset + 15) = ctemp16;

	aoffset1 +=  4;
	aoffset2 +=  4;
	aoffset3 +=  4;
	aoffset4 +=  4;
	boffset  +=  16;
	i --;
      }while(i > 0);
    }

    i = (m & 3);
    if (i >= 2){
	do{
	  ctemp01 = *(aoffset1 +  0);
	  ctemp09 = *(aoffset1 +  1);
	  ctemp17 = *(aoffset2 +  0);
	  ctemp25 = *(aoffset2 +  1);
	  ctemp33 = *(aoffset3 +  0);
	  ctemp41 = *(aoffset3 +  1);
	  ctemp49 = *(aoffset4 +  0);
	  ctemp57 = *(aoffset4 +  1);

	  *(boffset +  0) = ctemp01;
	  *(boffset +  1) = ctemp09;
	  *(boffset +  2) = ctemp17;
	  *(boffset +  3) = ctemp25;
	  *(boffset +  4) = ctemp33;
	  *(boffset +  5) = ctemp41;
	  *(boffset +  6) = ctemp49;
	  *(boffset +  7) = ctemp57;
	  aoffset1 += 2;
	  aoffset2 += 2;
	  aoffset3 += 2;
	  aoffset4 += 2;

	  boffset += 8;
	  i -= 2;
	}while(i > 1);
      }
      if (m & 1){
	ctemp01 = *(aoffset1 +  0);
	ctemp02 = *(aoffset2 +  0);
	ctemp03 = *(aoffset3 +  0);
	ctemp04 = *(aoffset4 +  0);

	*(boffset +  0) = ctemp01;
	*(boffset +  1) = ctemp02;
	*(boffset +  2) = ctemp03;
	*(boffset +  3) = ctemp04;

	aoffset1 ++;
	aoffset2 ++;
	aoffset3 ++;
	aoffset4 ++;

	boffset += 4;
      }
    }

  if (n & 2){
    aoffset1  = aoffset;
    aoffset2  = aoffset1 + lda;
    aoffset += 2 * lda;

    i = (m >> 1);
    if (i > 0){
      do{
	ctemp01 = *(aoffset1 +  0);
	ctemp02 = *(aoffset1 +  1);
	ctemp03 = *(aoffset2 +  0);
	ctemp04 = *(aoffset2 +  1);

	*(boffset +  0) = ctemp01;
	*(boffset +  1) = ctemp03;
	*(boffset +  2) = ctemp02;
	*(boffset +  3) = ctemp04;

	aoffset1 +=  2;
	aoffset2 +=  2;
	boffset  +=  4;
	i --;
      }while(i > 0);
    }

    if (m & 1){
      ctemp01 = *(aoffset1 +  0);
      ctemp02 = *(aoffset2 +  0);

      *(boffset +  0) = ctemp01;
      *(boffset +  1) = ctemp02;

      aoffset1 ++;
      aoffset2 ++;
      boffset += 2;
    }
  } /* end of if(j > 0) */

  if (n & 1){
    aoffset1  = aoffset;

    i = m;
    if (i > 0){
      do{
	ctemp01 = *(aoffset1 +  0);

	*(boffset +  0) = ctemp01;

	aoffset1 ++;
	boffset  ++;
	i --;
      }while(i > 0);
    }

  } /* end of if(j > 0) */

  return 0;
}
