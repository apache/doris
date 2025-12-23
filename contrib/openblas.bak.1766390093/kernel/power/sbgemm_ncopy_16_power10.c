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

typedef uint32_t vec_bf16x2 __attribute__ ((vector_size (16)));

int CNAME(BLASLONG m, BLASLONG n, IFLOAT *a, BLASLONG lda, IFLOAT *b){
  BLASLONG i, j;

  IFLOAT *aoffset;
  IFLOAT *aoffset1, *aoffset2, *aoffset3, *aoffset4;
  IFLOAT *aoffset5, *aoffset6, *aoffset7, *aoffset8;
  IFLOAT *aoffset9, *aoffset10, *aoffset11, *aoffset12;
  IFLOAT *aoffset13, *aoffset14, *aoffset15, *aoffset16;

  IFLOAT *boffset;
  IFLOAT ctemp01, ctemp02, ctemp03, ctemp04;
  IFLOAT ctemp05, ctemp06, ctemp07, ctemp08;
  IFLOAT ctemp09, ctemp10, ctemp11, ctemp12;
  IFLOAT ctemp13, ctemp14, ctemp15, ctemp16;
  IFLOAT ctemp17, ctemp18, ctemp19, ctemp20;
  IFLOAT ctemp21, ctemp22, ctemp23, ctemp24;
  IFLOAT ctemp25, ctemp26, ctemp27, ctemp28;
  IFLOAT ctemp29, ctemp30, ctemp31, ctemp32;

  aoffset = a;
  boffset = b;

  j = (n >> 4);
  if (j > 0){
    do{
      aoffset1  = aoffset;
      aoffset2  = aoffset1  + lda;
      aoffset3  = aoffset2  + lda;
      aoffset4  = aoffset3  + lda;
      aoffset5  = aoffset4  + lda;
      aoffset6  = aoffset5  + lda;
      aoffset7  = aoffset6  + lda;
      aoffset8  = aoffset7  + lda;
      aoffset9  = aoffset8  + lda;
      aoffset10 = aoffset9  + lda;
      aoffset11 = aoffset10 + lda;
      aoffset12 = aoffset11 + lda;
      aoffset13 = aoffset12 + lda;
      aoffset14 = aoffset13 + lda;
      aoffset15 = aoffset14 + lda;
      aoffset16 = aoffset15 + lda;
      aoffset += 16 * lda;

      i = (m >> 3);
      if (i > 0) {
        do {
          vec_bf16x2 vtemp01 = *(vec_bf16x2 *)(aoffset1);
          vec_bf16x2 vtemp02 = *(vec_bf16x2 *)(aoffset2);
          vec_bf16x2 vtemp03 = *(vec_bf16x2 *)(aoffset3);
          vec_bf16x2 vtemp04 = *(vec_bf16x2 *)(aoffset4);
          vec_bf16x2 vtemp05 = *(vec_bf16x2 *)(aoffset5);
          vec_bf16x2 vtemp06 = *(vec_bf16x2 *)(aoffset6);
          vec_bf16x2 vtemp07 = *(vec_bf16x2 *)(aoffset7);
          vec_bf16x2 vtemp08 = *(vec_bf16x2 *)(aoffset8);
          vec_bf16x2 vtemp09 = *(vec_bf16x2 *)(aoffset9);
          vec_bf16x2 vtemp10 = *(vec_bf16x2 *)(aoffset10);
          vec_bf16x2 vtemp11 = *(vec_bf16x2 *)(aoffset11);
          vec_bf16x2 vtemp12 = *(vec_bf16x2 *)(aoffset12);
          vec_bf16x2 vtemp13 = *(vec_bf16x2 *)(aoffset13);
          vec_bf16x2 vtemp14 = *(vec_bf16x2 *)(aoffset14);
          vec_bf16x2 vtemp15 = *(vec_bf16x2 *)(aoffset15);
          vec_bf16x2 vtemp16 = *(vec_bf16x2 *)(aoffset16);

          vec_bf16x2 vtemp17 = vec_mergeh(vtemp01, vtemp03);
          vec_bf16x2 vtemp18 = vec_mergel(vtemp01, vtemp03);
          vec_bf16x2 vtemp19 = vec_mergeh(vtemp02, vtemp04);
          vec_bf16x2 vtemp20 = vec_mergel(vtemp02, vtemp04);
          vec_bf16x2 vtemp21 = vec_mergeh(vtemp05, vtemp07);
          vec_bf16x2 vtemp22 = vec_mergel(vtemp05, vtemp07);
          vec_bf16x2 vtemp23 = vec_mergeh(vtemp06, vtemp08);
          vec_bf16x2 vtemp24 = vec_mergel(vtemp06, vtemp08);
          vec_bf16x2 vtemp25 = vec_mergeh(vtemp09, vtemp11);
          vec_bf16x2 vtemp26 = vec_mergel(vtemp09, vtemp11);
          vec_bf16x2 vtemp27 = vec_mergeh(vtemp10, vtemp12);
          vec_bf16x2 vtemp28 = vec_mergel(vtemp10, vtemp12);
          vec_bf16x2 vtemp29 = vec_mergeh(vtemp13, vtemp15);
          vec_bf16x2 vtemp30 = vec_mergel(vtemp13, vtemp15);
          vec_bf16x2 vtemp31 = vec_mergeh(vtemp14, vtemp16);
          vec_bf16x2 vtemp32 = vec_mergel(vtemp14, vtemp16);

          *(vec_bf16x2 *)(boffset +   0) = vec_mergeh(vtemp17, vtemp19);
          *(vec_bf16x2 *)(boffset +   8) = vec_mergeh(vtemp21, vtemp23);
          *(vec_bf16x2 *)(boffset +  16) = vec_mergeh(vtemp25, vtemp27);
          *(vec_bf16x2 *)(boffset +  24) = vec_mergeh(vtemp29, vtemp31);
          *(vec_bf16x2 *)(boffset +  32) = vec_mergel(vtemp17, vtemp19);
          *(vec_bf16x2 *)(boffset +  40) = vec_mergel(vtemp21, vtemp23);
          *(vec_bf16x2 *)(boffset +  48) = vec_mergel(vtemp25, vtemp27);
          *(vec_bf16x2 *)(boffset +  56) = vec_mergel(vtemp29, vtemp31);
          *(vec_bf16x2 *)(boffset +  64) = vec_mergeh(vtemp18, vtemp20);
          *(vec_bf16x2 *)(boffset +  72) = vec_mergeh(vtemp22, vtemp24);
          *(vec_bf16x2 *)(boffset +  80) = vec_mergeh(vtemp26, vtemp28);
          *(vec_bf16x2 *)(boffset +  88) = vec_mergeh(vtemp30, vtemp32);
          *(vec_bf16x2 *)(boffset +  96) = vec_mergel(vtemp18, vtemp20);
          *(vec_bf16x2 *)(boffset + 104) = vec_mergel(vtemp22, vtemp24);
          *(vec_bf16x2 *)(boffset + 112) = vec_mergel(vtemp26, vtemp28);
          *(vec_bf16x2 *)(boffset + 120) = vec_mergel(vtemp30, vtemp32);

          aoffset1  += 8;
          aoffset2  += 8;
          aoffset3  += 8;
          aoffset4  += 8;
          aoffset5  += 8;
          aoffset6  += 8;
          aoffset7  += 8;
          aoffset8  += 8;
          aoffset9  += 8;
          aoffset10 += 8;
          aoffset11 += 8;
          aoffset12 += 8;
          aoffset13 += 8;
          aoffset14 += 8;
          aoffset15 += 8;
          aoffset16 += 8;

          boffset   += 128;

          i--;
        } while (i > 0);
      }

      i = (m & 7) >> 1;
      if (i > 0){
	do{
	  ctemp01 = *(aoffset1 +  0);
	  ctemp02 = *(aoffset1 +  1);
	  ctemp03 = *(aoffset2 +  0);
	  ctemp04 = *(aoffset2 +  1);

	  ctemp05 = *(aoffset3 +  0);
	  ctemp06 = *(aoffset3 +  1);
	  ctemp07 = *(aoffset4 +  0);
	  ctemp08 = *(aoffset4 +  1);

	  ctemp09 = *(aoffset5 +  0);
	  ctemp10 = *(aoffset5 +  1);
	  ctemp11 = *(aoffset6 +  0);
	  ctemp12 = *(aoffset6 +  1);

	  ctemp13 = *(aoffset7 +  0);
	  ctemp14 = *(aoffset7 +  1);
	  ctemp15 = *(aoffset8 +  0);
	  ctemp16 = *(aoffset8 +  1);

	  ctemp17 = *(aoffset9 +  0);
	  ctemp18 = *(aoffset9 +  1);
	  ctemp19 = *(aoffset10 +  0);
	  ctemp20 = *(aoffset10 +  1);

	  ctemp21 = *(aoffset11 +  0);
	  ctemp22 = *(aoffset11 +  1);
	  ctemp23 = *(aoffset12 +  0);
	  ctemp24 = *(aoffset12 +  1);

	  ctemp25 = *(aoffset13 +  0);
	  ctemp26 = *(aoffset13 +  1);
	  ctemp27 = *(aoffset14 +  0);
	  ctemp28 = *(aoffset14 +  1);

	  ctemp29 = *(aoffset15 +  0);
	  ctemp30 = *(aoffset15 +  1);
	  ctemp31 = *(aoffset16 +  0);
	  ctemp32 = *(aoffset16 +  1);

	  *(boffset +  0) = ctemp01;
	  *(boffset +  1) = ctemp02;
	  *(boffset +  2) = ctemp03;
	  *(boffset +  3) = ctemp04;
	  *(boffset +  4) = ctemp05;
	  *(boffset +  5) = ctemp06;
	  *(boffset +  6) = ctemp07;
	  *(boffset +  7) = ctemp08;

	  *(boffset +  8) = ctemp09;
	  *(boffset +  9) = ctemp10;
	  *(boffset + 10) = ctemp11;
	  *(boffset + 11) = ctemp12;
	  *(boffset + 12) = ctemp13;
	  *(boffset + 13) = ctemp14;
	  *(boffset + 14) = ctemp15;
	  *(boffset + 15) = ctemp16;

	  *(boffset + 16) = ctemp17;
	  *(boffset + 17) = ctemp18;
	  *(boffset + 18) = ctemp19;
	  *(boffset + 19) = ctemp20;
	  *(boffset + 20) = ctemp21;
	  *(boffset + 21) = ctemp22;
	  *(boffset + 22) = ctemp23;
	  *(boffset + 23) = ctemp24;

	  *(boffset + 24) = ctemp25;
	  *(boffset + 25) = ctemp26;
	  *(boffset + 26) = ctemp27;
	  *(boffset + 27) = ctemp28;
	  *(boffset + 28) = ctemp29;
	  *(boffset + 29) = ctemp30;
	  *(boffset + 30) = ctemp31;
	  *(boffset + 31) = ctemp32;

	  aoffset1 +=  2;
	  aoffset2 +=  2;
	  aoffset3 +=  2;
	  aoffset4 +=  2;
	  aoffset5 +=  2;
	  aoffset6 +=  2;
	  aoffset7 +=  2;
	  aoffset8 +=  2;

	  aoffset9  +=  2;
	  aoffset10 +=  2;
	  aoffset11 +=  2;
	  aoffset12 +=  2;
	  aoffset13 +=  2;
	  aoffset14 +=  2;
	  aoffset15 +=  2;
	  aoffset16 +=  2;
	  boffset   += 32;

	  i --;
	}while(i > 0);
      }

      if (m & 1){
	ctemp01 = *(aoffset1 +  0);
	ctemp03 = *(aoffset2 +  0);
	ctemp05 = *(aoffset3 +  0);
	ctemp07 = *(aoffset4 +  0);
	ctemp09 = *(aoffset5 +  0);
	ctemp11 = *(aoffset6 +  0);
	ctemp13 = *(aoffset7 +  0);
	ctemp15 = *(aoffset8 +  0);

	ctemp17 = *(aoffset9 +  0);
	ctemp19 = *(aoffset10 +  0);
	ctemp21 = *(aoffset11 +  0);
	ctemp23 = *(aoffset12 +  0);
	ctemp25 = *(aoffset13 +  0);
	ctemp27 = *(aoffset14 +  0);
	ctemp29 = *(aoffset15 +  0);
	ctemp31 = *(aoffset16 +  0);

	*(boffset +  0) = ctemp01;
	*(boffset +  1) = ctemp03;
	*(boffset +  2) = ctemp05;
	*(boffset +  3) = ctemp07;
	*(boffset +  4) = ctemp09;
	*(boffset +  5) = ctemp11;
	*(boffset +  6) = ctemp13;
	*(boffset +  7) = ctemp15;

	*(boffset +  8) = ctemp17;
	*(boffset +  9) = ctemp19;
	*(boffset + 10) = ctemp21;
	*(boffset + 11) = ctemp23;
	*(boffset + 12) = ctemp25;
	*(boffset + 13) = ctemp27;
	*(boffset + 14) = ctemp29;
	*(boffset + 15) = ctemp31;

	boffset   += 16;
      }
      j--;
    }while(j > 0);
  } /* end of if(j > 0) */

  if (n & 8){
    aoffset1  = aoffset;
    aoffset2  = aoffset1  + lda;
    aoffset3  = aoffset2  + lda;
    aoffset4  = aoffset3  + lda;
    aoffset5  = aoffset4  + lda;
    aoffset6  = aoffset5  + lda;
    aoffset7  = aoffset6  + lda;
    aoffset8  = aoffset7  + lda;
    aoffset += 8 * lda;

    i = (m >> 1);
    if (i > 0){
      do{
	ctemp01 = *(aoffset1 +  0);
	ctemp02 = *(aoffset1 +  1);
	ctemp03 = *(aoffset2 +  0);
	ctemp04 = *(aoffset2 +  1);

	ctemp05 = *(aoffset3 +  0);
	ctemp06 = *(aoffset3 +  1);
	ctemp07 = *(aoffset4 +  0);
	ctemp08 = *(aoffset4 +  1);

	ctemp09 = *(aoffset5 +  0);
	ctemp10 = *(aoffset5 +  1);
	ctemp11 = *(aoffset6 +  0);
	ctemp12 = *(aoffset6 +  1);

	ctemp13 = *(aoffset7 +  0);
	ctemp14 = *(aoffset7 +  1);
	ctemp15 = *(aoffset8 +  0);
	ctemp16 = *(aoffset8 +  1);

	*(boffset +  0) = ctemp01;
	*(boffset +  1) = ctemp02;
	*(boffset +  2) = ctemp03;
	*(boffset +  3) = ctemp04;
	*(boffset +  4) = ctemp05;
	*(boffset +  5) = ctemp06;
	*(boffset +  6) = ctemp07;
	*(boffset +  7) = ctemp08;

	*(boffset +  8) = ctemp09;
	*(boffset +  9) = ctemp10;
	*(boffset + 10) = ctemp11;
	*(boffset + 11) = ctemp12;
	*(boffset + 12) = ctemp13;
	*(boffset + 13) = ctemp14;
	*(boffset + 14) = ctemp15;
	*(boffset + 15) = ctemp16;

	aoffset1 +=  2;
	aoffset2 +=  2;
	aoffset3 +=  2;
	aoffset4 +=  2;
	aoffset5 +=  2;
	aoffset6 +=  2;
	aoffset7 +=  2;
	aoffset8 +=  2;

	boffset   += 16;

	i --;
      }while(i > 0);
    }

    if (m & 1){
      ctemp01 = *(aoffset1 +  0);
      ctemp03 = *(aoffset2 +  0);
      ctemp05 = *(aoffset3 +  0);
      ctemp07 = *(aoffset4 +  0);
      ctemp09 = *(aoffset5 +  0);
      ctemp11 = *(aoffset6 +  0);
      ctemp13 = *(aoffset7 +  0);
      ctemp15 = *(aoffset8 +  0);

      *(boffset +  0) = ctemp01;
      *(boffset +  1) = ctemp03;
      *(boffset +  2) = ctemp05;
      *(boffset +  3) = ctemp07;
      *(boffset +  4) = ctemp09;
      *(boffset +  5) = ctemp11;
      *(boffset +  6) = ctemp13;
      *(boffset +  7) = ctemp15;

      boffset   += 8;
    }
  }

  if (n & 4){
    aoffset1  = aoffset;
    aoffset2  = aoffset1  + lda;
    aoffset3  = aoffset2  + lda;
    aoffset4  = aoffset3  + lda;
    aoffset += 4 * lda;

    i = (m >> 1);
    if (i > 0){
      do{
	ctemp01 = *(aoffset1 +  0);
	ctemp02 = *(aoffset1 +  1);
	ctemp03 = *(aoffset2 +  0);
	ctemp04 = *(aoffset2 +  1);

	ctemp05 = *(aoffset3 +  0);
	ctemp06 = *(aoffset3 +  1);
	ctemp07 = *(aoffset4 +  0);
	ctemp08 = *(aoffset4 +  1);

	*(boffset +  0) = ctemp01;
	*(boffset +  1) = ctemp02;
	*(boffset +  2) = ctemp03;
	*(boffset +  3) = ctemp04;
	*(boffset +  4) = ctemp05;
	*(boffset +  5) = ctemp06;
	*(boffset +  6) = ctemp07;
	*(boffset +  7) = ctemp08;

	aoffset1 +=  2;
	aoffset2 +=  2;
	aoffset3 +=  2;
	aoffset4 +=  2;
	boffset   += 8;

	i --;
      }while(i > 0);
    }

    if (m & 1){
      ctemp01 = *(aoffset1 +  0);
      ctemp03 = *(aoffset2 +  0);
      ctemp05 = *(aoffset3 +  0);
      ctemp07 = *(aoffset4 +  0);

      *(boffset +  0) = ctemp01;
      *(boffset +  1) = ctemp03;
      *(boffset +  2) = ctemp05;
      *(boffset +  3) = ctemp07;
      boffset   += 4;
    }
  }

  if (n & 2){
    aoffset1  = aoffset;
    aoffset2  = aoffset1  + lda;
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
	boffset   += 4;

	i --;
      }while(i > 0);
    }

    if (m & 1){
      ctemp01 = *(aoffset1 +  0);
      ctemp03 = *(aoffset2 +  0);

      *(boffset +  0) = ctemp01;
      *(boffset +  1) = ctemp03;
      boffset   += 2;
    }
  }

  if (n & 1){
    aoffset1  = aoffset;

    i = (m >> 1);
    if (i > 0){
      do{
	ctemp01 = *(aoffset1 +  0);
	ctemp02 = *(aoffset1 +  1);

	*(boffset +  0) = ctemp01;
	*(boffset +  1) = ctemp02;

	aoffset1 +=  2;
	boffset   += 2;

	i --;
      }while(i > 0);
    }

    if (m & 1){
      ctemp01 = *(aoffset1 +  0);

      *(boffset +  0) = ctemp01;
      // boffset   += 1;
    }
  }

  return 0;
}
