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

int CNAME(BLASLONG m, BLASLONG n, FLOAT *a, BLASLONG lda, FLOAT *b){
  BLASLONG i, j;
  BLASLONG nmod6;

  FLOAT *aoffset;
  FLOAT *aoffset1, *aoffset2, *aoffset3, *aoffset4;
  FLOAT *aoffset5, *aoffset6 ;

  FLOAT *boffset;
  FLOAT ctemp01, ctemp02, ctemp03, ctemp04;
  FLOAT ctemp05, ctemp06, ctemp07, ctemp08;
  FLOAT ctemp09, ctemp10, ctemp11, ctemp12;
  FLOAT ctemp13, ctemp14, ctemp15, ctemp16;
  FLOAT ctemp17, ctemp18, ctemp19, ctemp20;
  FLOAT ctemp21, ctemp22, ctemp23, ctemp24;

  nmod6  = n - (n / 6)* 6 ;
  aoffset = a;
  boffset = b;

  // prefex A: 1 block, block size: 4*8 bytes, offset: 16*8 bytes, base: aoffset1,2,,6;
  BLASULONG index = 0x100080; //( (1<<20)|(16<<3)&0xffff) ) ;
  // prefex B: 1 block, block size: 24*8 bytes, offset: 96*8 bytes, base: boffset;
  BLASULONG index_b = 0xb00300; //(11<<20) | ((96*8)&0xffff) ;

  j = (n / 6);
  if (j > 0){
    do{
      aoffset1  = aoffset;
      aoffset2  = aoffset1  + lda;
      aoffset3  = aoffset2  + lda;
      aoffset4  = aoffset3  + lda;
      aoffset5  = aoffset4  + lda;
      aoffset6  = aoffset5  + lda;
      aoffset += 6 * lda;

      i = (m >> 2);
      if (i > 0){
	do{
	  ctemp01  = *(aoffset1 + 0);
	  ctemp02  = *(aoffset1 + 1);
	  ctemp03  = *(aoffset1 + 2);
	  ctemp04  = *(aoffset1 + 3);

	  ctemp05  = *(aoffset2 + 0);
	  ctemp06  = *(aoffset2 + 1);
	  ctemp07  = *(aoffset2 + 2);
	  ctemp08  = *(aoffset2 + 3);

	  ctemp09 = *(aoffset3 + 0);
	  ctemp10 = *(aoffset3 + 1);
	  ctemp11 = *(aoffset3 + 2);
	  ctemp12 = *(aoffset3 + 3);

	  ctemp13 = *(aoffset4 + 0);
	  ctemp14 = *(aoffset4 + 1);
	  ctemp15 = *(aoffset4 + 2);
	  ctemp16 = *(aoffset4 + 3);

	  ctemp17 = *(aoffset5 + 0);
	  ctemp18 = *(aoffset5 + 1);
	  ctemp19 = *(aoffset5 + 2);
	  ctemp20 = *(aoffset5 + 3);

	  ctemp21 = *(aoffset6 + 0);
	  ctemp22 = *(aoffset6 + 1);
	  ctemp23 = *(aoffset6 + 2);
	  ctemp24 = *(aoffset6 + 3);

	  *(boffset +  0) = ctemp01;
	  *(boffset +  1) = ctemp05;
	  *(boffset +  2) = ctemp09;
	  *(boffset +  3) = ctemp13;
	  *(boffset +  4) = ctemp17;
	  *(boffset +  5) = ctemp21;

	  *(boffset +  6) = ctemp02;
	  *(boffset +  7) = ctemp06;
	  *(boffset +  8) = ctemp10;
	  *(boffset +  9) = ctemp14;
	  *(boffset + 10) = ctemp18;
	  *(boffset + 11) = ctemp22;

	  *(boffset + 12) = ctemp03;
	  *(boffset + 13) = ctemp07;
	  *(boffset + 14) = ctemp11;
	  *(boffset + 15) = ctemp15;
	  *(boffset + 16) = ctemp19;
	  *(boffset + 17) = ctemp23;

	  *(boffset + 18) = ctemp04;
	  *(boffset + 19) = ctemp08;
	  *(boffset + 20) = ctemp12;
	  *(boffset + 21) = ctemp16;
	  *(boffset + 22) = ctemp20;
	  *(boffset + 23) = ctemp24;

	  aoffset1 += 4;
	  aoffset2 += 4;
	  aoffset3 += 4;
	  aoffset4 += 4;
	  aoffset5 += 4;
	  aoffset6 += 4;

	  boffset += 24;
	  i --;
	}while(i > 0);
      }

      i = (m & 3);
      if (i > 0){
	    do{
	      ctemp01 = *(aoffset1 +  0);
	      ctemp03 = *(aoffset2 +  0);
	      ctemp05 = *(aoffset3 +  0);
	      ctemp07 = *(aoffset4 +  0);
	      ctemp09 = *(aoffset5 +  0);
	      ctemp11 = *(aoffset6 +  0);

	      *(boffset +  0) = ctemp01;
	      *(boffset +  1) = ctemp03;
	      *(boffset +  2) = ctemp05;
	      *(boffset +  3) = ctemp07;
	      *(boffset +  4) = ctemp09;
	      *(boffset +  5) = ctemp11;

	      aoffset1 ++;
	      aoffset2 ++;
	      aoffset3 ++;
	      aoffset4 ++;
	      aoffset5 ++;
	      aoffset6 ++;
	      boffset   += 6;
	      i --;
	    }while(i > 0);
      }

      j--;
    }while(j > 0);
  } /* end of if(j > 0) */

  if (nmod6 & 4){
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
	*(boffset +  1) = ctemp03;
	*(boffset +  2) = ctemp05;
	*(boffset +  3) = ctemp07;
	*(boffset +  4) = ctemp02;
	*(boffset +  5) = ctemp04;
	*(boffset +  6) = ctemp06;
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

  if (nmod6 & 2){
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

  if (nmod6 & 1){
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
    }
  }

  return 0;
}
