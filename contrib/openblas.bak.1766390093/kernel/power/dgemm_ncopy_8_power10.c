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
#include <altivec.h>
#define PREFETCHA(x, y) asm volatile ("dcbt %0, %1" : : "r" (x), "b" (y) : "memory");

int CNAME(BLASLONG m, BLASLONG n, IFLOAT *a, BLASLONG lda, IFLOAT *b){
  BLASLONG i, j;

  IFLOAT *aoffset;
  IFLOAT *aoffset1, *aoffset2, *aoffset3, *aoffset4;
  IFLOAT *aoffset5, *aoffset6, *aoffset7, *aoffset8;

  IFLOAT *boffset;
  IFLOAT ctemp01, ctemp02, ctemp03, ctemp04;
  IFLOAT ctemp09, ctemp17, ctemp33;
  IFLOAT ctemp25, ctemp41;
  IFLOAT ctemp49, ctemp57;

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
	PREFETCHA (aoffset1, 384);
	PREFETCHA (aoffset2, 384);
	PREFETCHA (aoffset3, 384);
	PREFETCHA (aoffset4, 384);
	PREFETCHA (aoffset5, 384);
	PREFETCHA (aoffset6, 384);
	PREFETCHA (aoffset7, 384);
	PREFETCHA (aoffset8, 384);
	__vector double va0 = *(__vector double*)(aoffset1 +  0);
	__vector double va1 = *(__vector double*)(aoffset1 +  2);
	__vector double va2 = *(__vector double*)(aoffset1 +  4);
	__vector double va3 = *(__vector double*)(aoffset1 +  6);

	__vector double va4 = *(__vector double*)(aoffset2 +  0);
	__vector double va5 = *(__vector double*)(aoffset2 +  2);
	__vector double va6 = *(__vector double*)(aoffset2 +  4);
	__vector double va7 = *(__vector double*)(aoffset2 +  6);

	__vector double va8 = *(__vector double*)(aoffset3 +  0);
	__vector double va9 = *(__vector double*)(aoffset3 +  2);
	__vector double va10 = *(__vector double*)(aoffset3 + 4);
	__vector double va11 = *(__vector double*)(aoffset3 + 6);

	__vector double va12 = *(__vector double*)(aoffset4 +  0);
	__vector double va13 = *(__vector double*)(aoffset4 +  2);
	__vector double va14 = *(__vector double*)(aoffset4 +  4);
	__vector double va15 = *(__vector double*)(aoffset4 +  6);

	__vector double va16 = *(__vector double*)(aoffset5 +  0);
	__vector double va17 = *(__vector double*)(aoffset5 +  2);
	__vector double va18 = *(__vector double*)(aoffset5 +  4);
	__vector double va19 = *(__vector double*)(aoffset5 +  6);

	__vector double va20 = *(__vector double*)(aoffset6 +  0);
	__vector double va21 = *(__vector double*)(aoffset6 +  2);
	__vector double va22 = *(__vector double*)(aoffset6 +  4);
	__vector double va23 = *(__vector double*)(aoffset6 +  6);

	__vector double va24 = *(__vector double*)(aoffset7 +  0);
	__vector double va25 = *(__vector double*)(aoffset7 +  2);
	__vector double va26 = *(__vector double*)(aoffset7 + 4);
	__vector double va27 = *(__vector double*)(aoffset7 + 6);

	__vector double va28 = *(__vector double*)(aoffset8 +  0);
	__vector double va29 = *(__vector double*)(aoffset8 +  2);
	__vector double va30 = *(__vector double*)(aoffset8 +  4);
	__vector double va31 = *(__vector double*)(aoffset8 +  6);

	*(__vector double*)(boffset +  0) = vec_xxpermdi(va0, va4, 0);
	*(__vector double*)(boffset +  2) = vec_xxpermdi(va8, va12, 0);
	*(__vector double*)(boffset +  4) = vec_xxpermdi(va16, va20, 0);
	*(__vector double*)(boffset +  6) = vec_xxpermdi(va24, va28, 0);
	*(__vector double*)(boffset +  8) = vec_xxpermdi(va0, va4, 3);
	*(__vector double*)(boffset +  10) = vec_xxpermdi(va8, va12, 3);
	*(__vector double*)(boffset +  12) = vec_xxpermdi(va16, va20, 3);
	*(__vector double*)(boffset +  14) = vec_xxpermdi(va24, va28, 3);

	*(__vector double*)(boffset +  16) = vec_xxpermdi(va1, va5, 0);
	*(__vector double*)(boffset +  18) = vec_xxpermdi(va9, va13, 0);
	*(__vector double*)(boffset +  20) = vec_xxpermdi(va17, va21, 0);
	*(__vector double*)(boffset +  22) = vec_xxpermdi(va25, va29, 0);
	*(__vector double*)(boffset +  24) = vec_xxpermdi(va1, va5, 3);
	*(__vector double*)(boffset +  26) = vec_xxpermdi(va9, va13, 3);
	*(__vector double*)(boffset +  28) = vec_xxpermdi(va17, va21, 3);
	*(__vector double*)(boffset +  30) = vec_xxpermdi(va25, va29, 3);

	*(__vector double*)(boffset +  32) = vec_xxpermdi(va2, va6, 0);
	*(__vector double*)(boffset +  34) = vec_xxpermdi(va10, va14, 0);
	*(__vector double*)(boffset +  36) = vec_xxpermdi(va18, va22, 0);
	*(__vector double*)(boffset +  38) = vec_xxpermdi(va26, va30, 0);
	*(__vector double*)(boffset +  40) = vec_xxpermdi(va2, va6, 3);
	*(__vector double*)(boffset +  42) = vec_xxpermdi(va10, va14, 3);
	*(__vector double*)(boffset +  44) = vec_xxpermdi(va18, va22, 3);
	*(__vector double*)(boffset +  46) = vec_xxpermdi(va26, va30, 3);

	*(__vector double*)(boffset +  48) = vec_xxpermdi(va3, va7, 0);
	*(__vector double*)(boffset +  50) = vec_xxpermdi(va11, va15, 0);
	*(__vector double*)(boffset +  52) = vec_xxpermdi(va19, va23, 0);
	*(__vector double*)(boffset +  54) = vec_xxpermdi(va27, va31, 0);
	*(__vector double*)(boffset +  56) = vec_xxpermdi(va3, va7, 3);
	*(__vector double*)(boffset +  58) = vec_xxpermdi(va11, va15, 3);
	*(__vector double*)(boffset +  60) = vec_xxpermdi(va19, va23, 3);
	*(__vector double*)(boffset +  62) = vec_xxpermdi(va27, va31, 3);
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
      if (i > 0){
	do{
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
	  i --;
	}while(i > 0);
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
	PREFETCHA (aoffset1, 384);
	PREFETCHA (aoffset2, 384);
	PREFETCHA (aoffset3, 384);
	PREFETCHA (aoffset4, 384);
	__vector double va0 = *(__vector double*)(aoffset1 +  0);
	__vector double va1 = *(__vector double*)(aoffset1 +  2);
	__vector double va2 = *(__vector double*)(aoffset2 +  0);
	__vector double va3 = *(__vector double*)(aoffset2 +  2);
	__vector double va4 = *(__vector double*)(aoffset3 +  0);
	__vector double va5 = *(__vector double*)(aoffset3 +  2);
	__vector double va6 = *(__vector double*)(aoffset4 +  0);
	__vector double va7 = *(__vector double*)(aoffset4 +  2);
	*(__vector double*)(boffset +  0) = vec_xxpermdi(va0, va2, 0);
	*(__vector double*)(boffset +  2) = vec_xxpermdi(va4, va6, 0);
	*(__vector double*)(boffset +  4) = vec_xxpermdi(va0, va2, 3);
	*(__vector double*)(boffset +  6) = vec_xxpermdi(va4, va6, 3);
	*(__vector double*)(boffset +  8) = vec_xxpermdi(va1, va3, 0);
	*(__vector double*)(boffset +  10) = vec_xxpermdi(va5, va7, 0);
	*(__vector double*)(boffset +  12) = vec_xxpermdi(va1, va3, 3);
	*(__vector double*)(boffset +  14) = vec_xxpermdi(va5, va7, 3);

	aoffset1 +=  4;
	aoffset2 +=  4;
	aoffset3 +=  4;
	aoffset4 +=  4;
	boffset  +=  16;
	i --;
      }while(i > 0);
    }

    i = (m & 3);
    if (i > 0){
      do{
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
	i --;
      }while(i > 0);
    }
  } /* end of if(j > 0) */

  if (n & 2){
    aoffset1  = aoffset;
    aoffset2  = aoffset1 + lda;
    aoffset += 2 * lda;

    i = (m >> 1);
    if (i > 0){
      do{
	__vector double va0 = *(__vector double*)(aoffset1 +  0);
	__vector double va1 = *(__vector double*)(aoffset2 +  0);
	*(__vector double*)(boffset +  0) = vec_xxpermdi(va0, va1, 0);
	*(__vector double*)(boffset +  2) = vec_xxpermdi(va0, va1, 3);

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
