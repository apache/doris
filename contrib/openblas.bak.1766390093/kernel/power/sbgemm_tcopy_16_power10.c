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
  IFLOAT *aoffset1, *aoffset2;
  IFLOAT *boffset;

  vec_bf16 vtemp01, vtemp02, vtemp03, vtemp04;
  IFLOAT ctemp01, ctemp02, ctemp03, ctemp04;
  IFLOAT ctemp05, ctemp06, ctemp07, ctemp08;

  aoffset   = a;
  boffset   = b;

#if 0
  fprintf(stderr, "m = %d n = %d\n", m, n);
#endif

  j = (n >> 4);
  if (j > 0){
    do{
      aoffset1  = aoffset;
      aoffset2  = aoffset + lda;
      aoffset += 16;

      i = (m >> 1);
      if (i > 0){
	do{
	  vtemp01 = *(vec_bf16 *)(aoffset1);
	  vtemp02 = *(vec_bf16 *)(aoffset1+8);
	  vtemp03 = *(vec_bf16 *)(aoffset2);
	  vtemp04 = *(vec_bf16 *)(aoffset2+8);
	  *(vec_bf16 *)(boffset + 0) = vec_mergeh(vtemp01, vtemp03);
	  *(vec_bf16 *)(boffset + 8) = vec_mergel(vtemp01, vtemp03);
	  *(vec_bf16 *)(boffset + 16) = vec_mergeh(vtemp02, vtemp04);
	  *(vec_bf16 *)(boffset + 24) = vec_mergel(vtemp02, vtemp04);
	  aoffset1 +=  2 * lda;
	  aoffset2 +=  2 * lda;
	  boffset   += 32;

	  i --;
	}while(i > 0);
      }

      if (m & 1){
	vtemp01 = *(vec_bf16 *)(aoffset1);
	vtemp02 = *(vec_bf16 *)(aoffset1+8);
	*(vec_bf16 *)(boffset + 0) = vtemp01;
	*(vec_bf16 *)(boffset + 8) = vtemp02;
	boffset   += 16;
      }

      j--;
    }while(j > 0);
  } /* end of if(j > 0) */

  if (n & 8){
    aoffset1  = aoffset;
    aoffset2  = aoffset + lda;
    aoffset += 8;

    i = (m >> 1);
    if (i > 0){
      do{
	vtemp01 = *(vec_bf16 *)(aoffset1);
	vtemp03 = *(vec_bf16 *)(aoffset2);
	*(vec_bf16 *)(boffset + 0) = vec_mergeh(vtemp01, vtemp03);
	*(vec_bf16 *)(boffset + 8) = vec_mergel(vtemp01, vtemp03);

	aoffset1 +=  2 * lda;
	aoffset2 +=  2 * lda;
	boffset   += 16;

	i --;
      }while(i > 0);
    }

    if (m & 1){
      vtemp01 = *(vec_bf16 *)(aoffset1);
      *(vec_bf16 *)(boffset + 0) = vtemp01;
      boffset   += 8;
    }
  }

  if (n & 4){
    aoffset1  = aoffset;
    aoffset2  = aoffset + lda;
    aoffset += 4;

    i = (m >> 1);
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

	*(boffset +  0) = ctemp01;
	*(boffset +  1) = ctemp05;
	*(boffset +  2) = ctemp02;
	*(boffset +  3) = ctemp06;
	*(boffset +  4) = ctemp03;
	*(boffset +  5) = ctemp07;
	*(boffset +  6) = ctemp04;
	*(boffset +  7) = ctemp08;

	aoffset1 +=  2 * lda;
	aoffset2 +=  2 * lda;
	boffset   += 8;

	i --;
      }while(i > 0);
    }

    if (m & 1){
      ctemp01 = *(aoffset1 +  0);
      ctemp02 = *(aoffset1 +  1);
      ctemp03 = *(aoffset1 +  2);
      ctemp04 = *(aoffset1 +  3);

      *(boffset +  0) = ctemp01;
      *(boffset +  1) = ctemp02;
      *(boffset +  2) = ctemp03;
      *(boffset +  3) = ctemp04;

      boffset   += 4;
    }
  }

  if (n & 2){
    aoffset1  = aoffset;
    aoffset2  = aoffset + lda;
    aoffset += 2;

    i = (m >> 1);
    if (i > 0){
      do{
	ctemp01 = *(aoffset1 +  0);
	ctemp02 = *(aoffset1 +  1);
	ctemp03 = *(aoffset2 +  0);
	ctemp04 = *(aoffset2 +  1);

	*(boffset +  0) = ctemp01;
	*(boffset +  1) = ctemp02;
	*(boffset +  2) = ctemp03;
	*(boffset +  3) = ctemp04;

	aoffset1 +=  2 * lda;
	aoffset2 +=  2 * lda;
	boffset   += 4;

	i --;
      }while(i > 0);
    }

    if (m & 1){
      ctemp01 = *(aoffset1 +  0);
      ctemp02 = *(aoffset1 +  1);

      *(boffset +  0) = ctemp01;
      *(boffset +  1) = ctemp02;
      boffset   += 2;
    }
  }

  if (n & 1){
    aoffset1  = aoffset;
    aoffset2  = aoffset + lda;

    i = (m >> 1);
    if (i > 0){
      do{
	ctemp01 = *(aoffset1 +  0);
	ctemp02 = *(aoffset2 +  0);

	*(boffset +  0) = ctemp01;
	*(boffset +  1) = ctemp02;

	aoffset1 +=  2 * lda;
	aoffset2 +=  2 * lda;
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
