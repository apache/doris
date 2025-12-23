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
  IFLOAT ctemp17,  ctemp19 ;
  IFLOAT ctemp21,  ctemp23 ;
  IFLOAT ctemp25,  ctemp27 ;
  IFLOAT ctemp29,  ctemp31 ;

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
      i = (m >> 2);
      if (i > 0){
	vector float c1, c2, c3, c4, c5, c6, c7, c8;
	vector float c9, c10, c11, c12, c13, c14, c15, c16;
	vector float t1, t2, t3, t4, t5, t6, t7, t8;
	vector float t9, t10, t11, t12;
	do{
	   c1 = vec_xl(0, aoffset1);
	   c2 = vec_xl(0, aoffset2);
	   c3 = vec_xl(0, aoffset3);
	   c4 = vec_xl(0, aoffset4);
	   c5 = vec_xl(0, aoffset5);
	   c6 = vec_xl(0, aoffset6);
	   c7 = vec_xl(0, aoffset7);
	   c8 = vec_xl(0, aoffset8);
	   c9 = vec_xl(0, aoffset9);
	   c10 = vec_xl(0, aoffset10);
	   c11 = vec_xl(0, aoffset11);
	   c12 = vec_xl(0, aoffset12);
	   c13 = vec_xl(0, aoffset13);
	   c14 = vec_xl(0, aoffset14);
	   c15 = vec_xl(0, aoffset15);
	   c16 = vec_xl(0, aoffset16);

           t1  = vec_mergeh(c1, c2);
           t2  = vec_mergeh(c3, c4);
           t3  = vec_mergeh(c5, c6);
           t4  = vec_mergeh(c7, c8);
           t9  = vec_mergeh(c9, c10);
           t10  = vec_mergeh(c11, c12);
           t11  = vec_mergeh(c13, c14);
           t12  = vec_mergeh(c15, c16);

	   t5 = vec_xxpermdi(t1, t2, 0b00);
           t6 = vec_xxpermdi(t3, t4, 0b00);
	   t7 = vec_xxpermdi(t9, t10, 0b00);
	   t8 = vec_xxpermdi(t11, t12, 0b00);

	   vec_xst(t5, 0, boffset);
	   vec_xst(t6, 0, boffset+4);
	   vec_xst(t7, 0, boffset+8);
	   vec_xst(t8, 0, boffset+12);
	   t5 = vec_xxpermdi(t1, t2, 0b11);
	   t6 = vec_xxpermdi(t3, t4, 0b11);
	   t7 = vec_xxpermdi(t9, t10, 0b11);
	   t8 = vec_xxpermdi(t11, t12, 0b11);
	   vec_xst(t5, 0, boffset+16);
	   vec_xst(t6, 0, boffset+20);
	   vec_xst(t7, 0, boffset+24);
	   vec_xst(t8, 0, boffset+28);

           t1  = vec_mergel(c1, c2);
           t2  = vec_mergel(c3, c4);
           t3  = vec_mergel(c5, c6);
           t4  = vec_mergel(c7, c8);
           t9  = vec_mergel(c9, c10);
           t10  = vec_mergel(c11, c12);
           t11  = vec_mergel(c13, c14);
           t12  = vec_mergel(c15, c16);
  	   t5 = vec_xxpermdi(t1, t2, 0b00);
	   t6 = vec_xxpermdi(t3, t4, 0b00);
	   t7 = vec_xxpermdi(t9, t10, 0b00);
	   t8 = vec_xxpermdi(t11, t12, 0b00);
	   vec_xst(t5, 0, boffset+32);
	   vec_xst(t6, 0, boffset+36);
	   vec_xst(t7, 0, boffset+40);
	   vec_xst(t8, 0, boffset+44);

	   t5 = vec_xxpermdi(t1, t2, 0b11);
	   t6 = vec_xxpermdi(t3, t4, 0b11);
	   t7 = vec_xxpermdi(t9, t10, 0b11);
	   t8 = vec_xxpermdi(t11, t12, 0b11);
	   vec_xst(t5, 0, boffset+48);
	   vec_xst(t6, 0, boffset+52);
	   vec_xst(t7, 0, boffset+56);
	   vec_xst(t8, 0, boffset+60);

	  aoffset1 +=  4;
	  aoffset2 +=  4;
	  aoffset3 +=  4;
	  aoffset4 +=  4;
	  aoffset5 +=  4;
	  aoffset6 +=  4;
	  aoffset7 +=  4;
	  aoffset8 +=  4;

	  aoffset9  +=  4;
	  aoffset10 +=  4;
	  aoffset11 +=  4;
	  aoffset12 +=  4;
	  aoffset13 +=  4;
	  aoffset14 +=  4;
	  aoffset15 +=  4;
	  aoffset16 +=  4;
	  boffset   += 64;

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
	  aoffset1+=1;
	  aoffset2+=1;
	  aoffset3+=1;
	  aoffset4+=1;
	  aoffset5+=1;
	  aoffset6+=1;
	  aoffset7+=1;
	  aoffset8+=1;
	  aoffset9+=1;
	  aoffset10+=1;
	  aoffset11+=1;
	  aoffset12+=1;
	  aoffset13+=1;
	  aoffset14+=1;
	  aoffset15+=1;
	  aoffset16+=1;
	boffset  += 16;
        i --;
        }while(i > 0);
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

    i = (m >> 2);
    if (i > 0){
      vector float c1, c2, c3, c4, c5, c6, c7, c8;
      vector float t1, t2, t3, t4, t5, t6, t7, t8;
      do{
        c1 = vec_xl(0, aoffset1);
        c2 = vec_xl(0, aoffset2);
        c3 = vec_xl(0, aoffset3);
        c4 = vec_xl(0, aoffset4);
        c5 = vec_xl(0, aoffset5);
        c6 = vec_xl(0, aoffset6);
        c7 = vec_xl(0, aoffset7);
        c8 = vec_xl(0, aoffset8);

        t1  = vec_mergeh(c1, c2);
        t2  = vec_mergeh(c3, c4);
        t3  = vec_mergeh(c5, c6);
        t4  = vec_mergeh(c7, c8);

        t5 = vec_xxpermdi(t1, t2, 0b00);
        t6 = vec_xxpermdi(t3, t4, 0b00);
        t7 = vec_xxpermdi(t1, t2, 0b11);
        t8 = vec_xxpermdi(t3, t4, 0b11);

        vec_xst(t5, 0, boffset);
        vec_xst(t6, 0, boffset+4);
        vec_xst(t7, 0, boffset+8);
        vec_xst(t8, 0, boffset+12);

        t1  = vec_mergel(c1, c2);
        t2  = vec_mergel(c3, c4);
        t3  = vec_mergel(c5, c6);
        t4  = vec_mergel(c7, c8);

        t5 = vec_xxpermdi(t1, t2, 0b00);
        t6 = vec_xxpermdi(t3, t4, 0b00);
        t7 = vec_xxpermdi(t1, t2, 0b11);
        t8 = vec_xxpermdi(t3, t4, 0b11);

        vec_xst(t5, 0, boffset+16);
        vec_xst(t6, 0, boffset+20);
        vec_xst(t7, 0, boffset+24);
        vec_xst(t8, 0, boffset+28);

        aoffset1 +=  4;
        aoffset2 +=  4;
        aoffset3 +=  4;
        aoffset4 +=  4;
        aoffset5 +=  4;
        aoffset6 +=  4;
        aoffset7 +=  4;
        aoffset8 +=  4;

        boffset   += 32;
        i--;
      }while(i > 0);
    }

    i = (m & 3);
    if (i > 0) {
      do {
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

        aoffset1+=1;
        aoffset2+=1;
        aoffset3+=1;
        aoffset4+=1;
        aoffset5+=1;
        aoffset6+=1;
        aoffset7+=1;
        aoffset8+=1;

        boffset   += 8;
        i--;
      } while (i > 0);
    }
  }

  if (n & 4){
    aoffset1  = aoffset;
    aoffset2  = aoffset1  + lda;
    aoffset3  = aoffset2  + lda;
    aoffset4  = aoffset3  + lda;
    aoffset += 4 * lda;

    i = (m >> 2);
    if (i > 0){
      vector float c1, c2, c3, c4;
      vector float t1, t2, t3, t4;
      do{
        c1 = vec_xl(0, aoffset1);
        c2 = vec_xl(0, aoffset2);
        c3 = vec_xl(0, aoffset3);
        c4 = vec_xl(0, aoffset4);

        t1  = vec_mergeh(c1, c2);
        t2  = vec_mergeh(c3, c4);

        t3 = vec_xxpermdi(t1, t2, 0b00);
        t4 = vec_xxpermdi(t1, t2, 0b11);

        vec_xst(t3, 0, boffset);
        vec_xst(t4, 0, boffset+4);

        t1  = vec_mergel(c1, c2);
        t2  = vec_mergel(c3, c4);

        t3 = vec_xxpermdi(t1, t2, 0b00);
        t4 = vec_xxpermdi(t1, t2, 0b11);

        vec_xst(t3, 0, boffset+8);
        vec_xst(t4, 0, boffset+12);

        aoffset1 +=  4;
        aoffset2 +=  4;
        aoffset3 +=  4;
        aoffset4 +=  4;

        boffset   += 16;
        i--;
      }while(i > 0);
    }

    i = (m & 3);
    if (i > 0) {
      do {
        ctemp01 = *(aoffset1 +  0);
        ctemp03 = *(aoffset2 +  0);
        ctemp05 = *(aoffset3 +  0);
        ctemp07 = *(aoffset4 +  0);

        *(boffset +  0) = ctemp01;
        *(boffset +  1) = ctemp03;
        *(boffset +  2) = ctemp05;
        *(boffset +  3) = ctemp07;

        aoffset1+=1;
        aoffset2+=1;
        aoffset3+=1;
        aoffset4+=1;

        boffset   += 4;
        i--;
      } while (i > 0);
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
