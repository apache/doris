/*******************************************************************************
Copyright (c) 2024, The OpenBLAS Project
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
*******************************************************************************/

#include <stdio.h>
#include "common.h"

int CNAME(BLASLONG m, BLASLONG n, FLOAT *a, BLASLONG lda, FLOAT *b){

  BLASLONG i, j;

  FLOAT *aoffset;
  FLOAT *aoffset1, *aoffset2;

  FLOAT *boffset;

  FLOAT ctemp01, ctemp02, ctemp03, ctemp04;
  FLOAT ctemp05, ctemp06, ctemp07, ctemp08;
  FLOAT ctemp09, ctemp10, ctemp11, ctemp12;
  FLOAT ctemp13, ctemp14, ctemp15, ctemp16;
  FLOAT ctemp17, ctemp18, ctemp19, ctemp20;
  FLOAT ctemp21, ctemp22, ctemp23, ctemp24;
  FLOAT ctemp25, ctemp26, ctemp27, ctemp28;
  FLOAT ctemp29, ctemp30, ctemp31, ctemp32;

  FLOAT ctemp33, ctemp34, ctemp35, ctemp36;
  FLOAT ctemp37, ctemp38, ctemp39, ctemp40;
  FLOAT ctemp41, ctemp42, ctemp43, ctemp44;
  FLOAT ctemp45, ctemp46, ctemp47, ctemp48;
  FLOAT ctemp49, ctemp50, ctemp51, ctemp52;
  FLOAT ctemp53, ctemp54, ctemp55, ctemp56;
  FLOAT ctemp57, ctemp58, ctemp59, ctemp60;
  FLOAT ctemp61, ctemp62, ctemp63, ctemp64;

  aoffset   = a;
  boffset   = b;
  lda *= 2;

#if 0
  fprintf(stderr, "M = %d N = %d\n", m, n);
#endif

  j = (n >> 4);
  if (j > 0){
    do{
      aoffset1  = aoffset;
      aoffset2  = aoffset + lda;
      aoffset += 32;

      i = (m >> 1);
      if (i > 0){
	do{
	  ctemp01 = *(aoffset1 +  0);
	  ctemp02 = *(aoffset1 +  1);
	  ctemp03 = *(aoffset1 +  2);
	  ctemp04 = *(aoffset1 +  3);
	  ctemp05 = *(aoffset1 +  4);
	  ctemp06 = *(aoffset1 +  5);
	  ctemp07 = *(aoffset1 +  6);
	  ctemp08 = *(aoffset1 +  7);
	  ctemp09 = *(aoffset1 +  8);
	  ctemp10 = *(aoffset1 +  9);
	  ctemp11 = *(aoffset1 + 10);
	  ctemp12 = *(aoffset1 + 11);
	  ctemp13 = *(aoffset1 + 12);
	  ctemp14 = *(aoffset1 + 13);
	  ctemp15 = *(aoffset1 + 14);
	  ctemp16 = *(aoffset1 + 15);
	  ctemp17 = *(aoffset1 + 16);
	  ctemp18 = *(aoffset1 + 17);
	  ctemp19 = *(aoffset1 + 18);
	  ctemp20 = *(aoffset1 + 19);
	  ctemp21 = *(aoffset1 + 20);
	  ctemp22 = *(aoffset1 + 21);
	  ctemp23 = *(aoffset1 + 22);
	  ctemp24 = *(aoffset1 + 23);
	  ctemp25 = *(aoffset1 + 24);
	  ctemp26 = *(aoffset1 + 25);
	  ctemp27 = *(aoffset1 + 26);
	  ctemp28 = *(aoffset1 + 27);
	  ctemp29 = *(aoffset1 + 28);
	  ctemp30 = *(aoffset1 + 29);
	  ctemp31 = *(aoffset1 + 30);
	  ctemp32 = *(aoffset1 + 31);

	  ctemp33 = *(aoffset2 +  0);
	  ctemp34 = *(aoffset2 +  1);
	  ctemp35 = *(aoffset2 +  2);
	  ctemp36 = *(aoffset2 +  3);
	  ctemp37 = *(aoffset2 +  4);
	  ctemp38 = *(aoffset2 +  5);
	  ctemp39 = *(aoffset2 +  6);
	  ctemp40 = *(aoffset2 +  7);
	  ctemp41 = *(aoffset2 +  8);
	  ctemp42 = *(aoffset2 +  9);
	  ctemp43 = *(aoffset2 + 10);
	  ctemp44 = *(aoffset2 + 11);
	  ctemp45 = *(aoffset2 + 12);
	  ctemp46 = *(aoffset2 + 13);
	  ctemp47 = *(aoffset2 + 14);
	  ctemp48 = *(aoffset2 + 15);
	  ctemp49 = *(aoffset2 + 16);
	  ctemp50 = *(aoffset2 + 17);
	  ctemp51 = *(aoffset2 + 18);
	  ctemp52 = *(aoffset2 + 19);
	  ctemp53 = *(aoffset2 + 20);
	  ctemp54 = *(aoffset2 + 21);
	  ctemp55 = *(aoffset2 + 22);
	  ctemp56 = *(aoffset2 + 23);
	  ctemp57 = *(aoffset2 + 24);
	  ctemp58 = *(aoffset2 + 25);
	  ctemp59 = *(aoffset2 + 26);
	  ctemp60 = *(aoffset2 + 27);
	  ctemp61 = *(aoffset2 + 28);
	  ctemp62 = *(aoffset2 + 29);
	  ctemp63 = *(aoffset2 + 30);
	  ctemp64 = *(aoffset2 + 31);

	  *(boffset +  0) = -ctemp01;
	  *(boffset +  1) = -ctemp02;
	  *(boffset +  2) = -ctemp03;
	  *(boffset +  3) = -ctemp04;
	  *(boffset +  4) = -ctemp05;
	  *(boffset +  5) = -ctemp06;
	  *(boffset +  6) = -ctemp07;
	  *(boffset +  7) = -ctemp08;

	  *(boffset +  8) = -ctemp09;
	  *(boffset +  9) = -ctemp10;
	  *(boffset + 10) = -ctemp11;
	  *(boffset + 11) = -ctemp12;
	  *(boffset + 12) = -ctemp13;
	  *(boffset + 13) = -ctemp14;
	  *(boffset + 14) = -ctemp15;
	  *(boffset + 15) = -ctemp16;

	  *(boffset + 16) = -ctemp17;
	  *(boffset + 17) = -ctemp18;
	  *(boffset + 18) = -ctemp19;
	  *(boffset + 19) = -ctemp20;
	  *(boffset + 20) = -ctemp21;
	  *(boffset + 21) = -ctemp22;
	  *(boffset + 22) = -ctemp23;
	  *(boffset + 23) = -ctemp24;

	  *(boffset + 24) = -ctemp25;
	  *(boffset + 25) = -ctemp26;
	  *(boffset + 26) = -ctemp27;
	  *(boffset + 27) = -ctemp28;
	  *(boffset + 28) = -ctemp29;
	  *(boffset + 29) = -ctemp30;
	  *(boffset + 30) = -ctemp31;
	  *(boffset + 31) = -ctemp32;

	  *(boffset + 32) = -ctemp33;
	  *(boffset + 33) = -ctemp34;
	  *(boffset + 34) = -ctemp35;
	  *(boffset + 35) = -ctemp36;
	  *(boffset + 36) = -ctemp37;
	  *(boffset + 37) = -ctemp38;
	  *(boffset + 38) = -ctemp39;
	  *(boffset + 39) = -ctemp40;

	  *(boffset + 40) = -ctemp41;
	  *(boffset + 41) = -ctemp42;
	  *(boffset + 42) = -ctemp43;
	  *(boffset + 43) = -ctemp44;
	  *(boffset + 44) = -ctemp45;
	  *(boffset + 45) = -ctemp46;
	  *(boffset + 46) = -ctemp47;
	  *(boffset + 47) = -ctemp48;

	  *(boffset + 48) = -ctemp49;
	  *(boffset + 49) = -ctemp50;
	  *(boffset + 50) = -ctemp51;
	  *(boffset + 51) = -ctemp52;
	  *(boffset + 52) = -ctemp53;
	  *(boffset + 53) = -ctemp54;
	  *(boffset + 54) = -ctemp55;
	  *(boffset + 55) = -ctemp56;

	  *(boffset + 56) = -ctemp57;
	  *(boffset + 57) = -ctemp58;
	  *(boffset + 58) = -ctemp59;
	  *(boffset + 59) = -ctemp60;
	  *(boffset + 60) = -ctemp61;
	  *(boffset + 61) = -ctemp62;
	  *(boffset + 62) = -ctemp63;
	  *(boffset + 63) = -ctemp64;

	  aoffset1 +=  2 * lda;
	  aoffset2 +=  2 * lda;
	  boffset   += 64;

	  i --;
	}while(i > 0);
      }

      if (m & 1){
	ctemp01 = *(aoffset1 +  0);
	ctemp02 = *(aoffset1 +  1);
	ctemp03 = *(aoffset1 +  2);
	ctemp04 = *(aoffset1 +  3);
	ctemp05 = *(aoffset1 +  4);
	ctemp06 = *(aoffset1 +  5);
	ctemp07 = *(aoffset1 +  6);
	ctemp08 = *(aoffset1 +  7);
	ctemp09 = *(aoffset1 +  8);
	ctemp10 = *(aoffset1 +  9);
	ctemp11 = *(aoffset1 + 10);
	ctemp12 = *(aoffset1 + 11);
	ctemp13 = *(aoffset1 + 12);
	ctemp14 = *(aoffset1 + 13);
	ctemp15 = *(aoffset1 + 14);
	ctemp16 = *(aoffset1 + 15);
	ctemp17 = *(aoffset1 + 16);
	ctemp18 = *(aoffset1 + 17);
	ctemp19 = *(aoffset1 + 18);
	ctemp20 = *(aoffset1 + 19);
	ctemp21 = *(aoffset1 + 20);
	ctemp22 = *(aoffset1 + 21);
	ctemp23 = *(aoffset1 + 22);
	ctemp24 = *(aoffset1 + 23);
	ctemp25 = *(aoffset1 + 24);
	ctemp26 = *(aoffset1 + 25);
	ctemp27 = *(aoffset1 + 26);
	ctemp28 = *(aoffset1 + 27);
	ctemp29 = *(aoffset1 + 28);
	ctemp30 = *(aoffset1 + 29);
	ctemp31 = *(aoffset1 + 30);
	ctemp32 = *(aoffset1 + 31);

	*(boffset +  0) = -ctemp01;
	*(boffset +  1) = -ctemp02;
	*(boffset +  2) = -ctemp03;
	*(boffset +  3) = -ctemp04;
	*(boffset +  4) = -ctemp05;
	*(boffset +  5) = -ctemp06;
	*(boffset +  6) = -ctemp07;
	*(boffset +  7) = -ctemp08;

	*(boffset +  8) = -ctemp09;
	*(boffset +  9) = -ctemp10;
	*(boffset + 10) = -ctemp11;
	*(boffset + 11) = -ctemp12;
	*(boffset + 12) = -ctemp13;
	*(boffset + 13) = -ctemp14;
	*(boffset + 14) = -ctemp15;
	*(boffset + 15) = -ctemp16;

	*(boffset + 16) = -ctemp17;
	*(boffset + 17) = -ctemp18;
	*(boffset + 18) = -ctemp19;
	*(boffset + 19) = -ctemp20;
	*(boffset + 20) = -ctemp21;
	*(boffset + 21) = -ctemp22;
	*(boffset + 22) = -ctemp23;
	*(boffset + 23) = -ctemp24;

	*(boffset + 24) = -ctemp25;
	*(boffset + 25) = -ctemp26;
	*(boffset + 26) = -ctemp27;
	*(boffset + 27) = -ctemp28;
	*(boffset + 28) = -ctemp29;
	*(boffset + 29) = -ctemp30;
	*(boffset + 30) = -ctemp31;
	*(boffset + 31) = -ctemp32;

	boffset   += 32;
      }

      j--;
    }while(j > 0);
  } /* end of if(j > 0) */

  if (n & 8){
    aoffset1  = aoffset;
    aoffset2  = aoffset + lda;
    aoffset += 16;

    i = (m >> 1);
    if (i > 0){
      do{
	  ctemp01 = *(aoffset1 +  0);
	  ctemp02 = *(aoffset1 +  1);
	  ctemp03 = *(aoffset1 +  2);
	  ctemp04 = *(aoffset1 +  3);
	  ctemp05 = *(aoffset1 +  4);
	  ctemp06 = *(aoffset1 +  5);
	  ctemp07 = *(aoffset1 +  6);
	  ctemp08 = *(aoffset1 +  7);
	  ctemp09 = *(aoffset1 +  8);
	  ctemp10 = *(aoffset1 +  9);
	  ctemp11 = *(aoffset1 + 10);
	  ctemp12 = *(aoffset1 + 11);
	  ctemp13 = *(aoffset1 + 12);
	  ctemp14 = *(aoffset1 + 13);
	  ctemp15 = *(aoffset1 + 14);
	  ctemp16 = *(aoffset1 + 15);

	  ctemp17 = *(aoffset2 +  0);
	  ctemp18 = *(aoffset2 +  1);
	  ctemp19 = *(aoffset2 +  2);
	  ctemp20 = *(aoffset2 +  3);
	  ctemp21 = *(aoffset2 +  4);
	  ctemp22 = *(aoffset2 +  5);
	  ctemp23 = *(aoffset2 +  6);
	  ctemp24 = *(aoffset2 +  7);
	  ctemp25 = *(aoffset2 +  8);
	  ctemp26 = *(aoffset2 +  9);
	  ctemp27 = *(aoffset2 + 10);
	  ctemp28 = *(aoffset2 + 11);
	  ctemp29 = *(aoffset2 + 12);
	  ctemp30 = *(aoffset2 + 13);
	  ctemp31 = *(aoffset2 + 14);
	  ctemp32 = *(aoffset2 + 15);

	  *(boffset +  0) = -ctemp01;
	  *(boffset +  1) = -ctemp02;
	  *(boffset +  2) = -ctemp03;
	  *(boffset +  3) = -ctemp04;
	  *(boffset +  4) = -ctemp05;
	  *(boffset +  5) = -ctemp06;
	  *(boffset +  6) = -ctemp07;
	  *(boffset +  7) = -ctemp08;

	  *(boffset +  8) = -ctemp09;
	  *(boffset +  9) = -ctemp10;
	  *(boffset + 10) = -ctemp11;
	  *(boffset + 11) = -ctemp12;
	  *(boffset + 12) = -ctemp13;
	  *(boffset + 13) = -ctemp14;
	  *(boffset + 14) = -ctemp15;
	  *(boffset + 15) = -ctemp16;

	  *(boffset + 16) = -ctemp17;
	  *(boffset + 17) = -ctemp18;
	  *(boffset + 18) = -ctemp19;
	  *(boffset + 19) = -ctemp20;
	  *(boffset + 20) = -ctemp21;
	  *(boffset + 21) = -ctemp22;
	  *(boffset + 22) = -ctemp23;
	  *(boffset + 23) = -ctemp24;

	  *(boffset + 24) = -ctemp25;
	  *(boffset + 25) = -ctemp26;
	  *(boffset + 26) = -ctemp27;
	  *(boffset + 27) = -ctemp28;
	  *(boffset + 28) = -ctemp29;
	  *(boffset + 29) = -ctemp30;
	  *(boffset + 30) = -ctemp31;
	  *(boffset + 31) = -ctemp32;

	aoffset1 +=  2 * lda;
	aoffset2 +=  2 * lda;
	boffset   += 32;

	i --;
      }while(i > 0);
    }

    if (m & 1){
    ctemp01 = *(aoffset1 +  0);
	ctemp02 = *(aoffset1 +  1);
	ctemp03 = *(aoffset1 +  2);
	ctemp04 = *(aoffset1 +  3);
	ctemp05 = *(aoffset1 +  4);
	ctemp06 = *(aoffset1 +  5);
	ctemp07 = *(aoffset1 +  6);
	ctemp08 = *(aoffset1 +  7);
	ctemp09 = *(aoffset1 +  8);
	ctemp10 = *(aoffset1 +  9);
	ctemp11 = *(aoffset1 + 10);
	ctemp12 = *(aoffset1 + 11);
	ctemp13 = *(aoffset1 + 12);
	ctemp14 = *(aoffset1 + 13);
	ctemp15 = *(aoffset1 + 14);
	ctemp16 = *(aoffset1 + 15);

    *(boffset +  0) = -ctemp01;
	*(boffset +  1) = -ctemp02;
	*(boffset +  2) = -ctemp03;
	*(boffset +  3) = -ctemp04;
	*(boffset +  4) = -ctemp05;
	*(boffset +  5) = -ctemp06;
	*(boffset +  6) = -ctemp07;
	*(boffset +  7) = -ctemp08;

	*(boffset +  8) = -ctemp09;
	*(boffset +  9) = -ctemp10;
	*(boffset + 10) = -ctemp11;
	*(boffset + 11) = -ctemp12;
	*(boffset + 12) = -ctemp13;
	*(boffset + 13) = -ctemp14;
	*(boffset + 14) = -ctemp15;
	*(boffset + 15) = -ctemp16;

      boffset   += 16;
    }
  }

  if (n & 4){
    aoffset1  = aoffset;
    aoffset2  = aoffset + lda;
    aoffset += 8;

    i = (m >> 1);
    if (i > 0){
      do{
	ctemp01 = *(aoffset1 +  0);
	ctemp02 = *(aoffset1 +  1);
	ctemp03 = *(aoffset1 +  2);
	ctemp04 = *(aoffset1 +  3);
	ctemp05 = *(aoffset1 +  4);
	ctemp06 = *(aoffset1 +  5);
	ctemp07 = *(aoffset1 +  6);
	ctemp08 = *(aoffset1 +  7);

	ctemp09 = *(aoffset2 +  0);
	ctemp10 = *(aoffset2 +  1);
	ctemp11 = *(aoffset2 +  2);
	ctemp12 = *(aoffset2 +  3);
	ctemp13 = *(aoffset2 +  4);
	ctemp14 = *(aoffset2 +  5);
	ctemp15 = *(aoffset2 +  6);
	ctemp16 = *(aoffset2 +  7);

	*(boffset +  0) = -ctemp01;
	*(boffset +  1) = -ctemp02;
	*(boffset +  2) = -ctemp03;
	*(boffset +  3) = -ctemp04;
	*(boffset +  4) = -ctemp05;
	*(boffset +  5) = -ctemp06;
	*(boffset +  6) = -ctemp07;
	*(boffset +  7) = -ctemp08;

	*(boffset +  8) = -ctemp09;
	*(boffset +  9) = -ctemp10;
	*(boffset + 10) = -ctemp11;
	*(boffset + 11) = -ctemp12;
	*(boffset + 12) = -ctemp13;
	*(boffset + 13) = -ctemp14;
	*(boffset + 14) = -ctemp15;
	*(boffset + 15) = -ctemp16;

	aoffset1 +=  2 * lda;
	aoffset2 +=  2 * lda;
	boffset   += 16;

	i --;
      }while(i > 0);
    }

    if (m & 1){
      ctemp01 = *(aoffset1 +  0);
      ctemp02 = *(aoffset1 +  1);
      ctemp03 = *(aoffset1 +  2);
      ctemp04 = *(aoffset1 +  3);
      ctemp05 = *(aoffset1 +  4);
      ctemp06 = *(aoffset1 +  5);
      ctemp07 = *(aoffset1 +  6);
      ctemp08 = *(aoffset1 +  7);

      *(boffset +  0) = -ctemp01;
      *(boffset +  1) = -ctemp02;
      *(boffset +  2) = -ctemp03;
      *(boffset +  3) = -ctemp04;
      *(boffset +  4) = -ctemp05;
      *(boffset +  5) = -ctemp06;
      *(boffset +  6) = -ctemp07;
      *(boffset +  7) = -ctemp08;

      boffset   += 8;
    }
  }

  if (n & 2){
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

	*(boffset +  0) = -ctemp01;
	*(boffset +  1) = -ctemp02;
	*(boffset +  2) = -ctemp03;
	*(boffset +  3) = -ctemp04;
	*(boffset +  4) = -ctemp05;
	*(boffset +  5) = -ctemp06;
	*(boffset +  6) = -ctemp07;
	*(boffset +  7) = -ctemp08;

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

      *(boffset +  0) = -ctemp01;
      *(boffset +  1) = -ctemp02;
      *(boffset +  2) = -ctemp03;
      *(boffset +  3) = -ctemp04;

      boffset   += 4;
    }
  }

  if (n & 1){
    aoffset1  = aoffset;
    aoffset2  = aoffset + lda;
    // aoffset += 2;

    i = (m >> 1);
    if (i > 0){
      do{
	ctemp01 = *(aoffset1 +  0);
	ctemp02 = *(aoffset1 +  1);
	ctemp03 = *(aoffset2 +  0);
	ctemp04 = *(aoffset2 +  1);

	*(boffset +  0) = -ctemp01;
	*(boffset +  1) = -ctemp02;
	*(boffset +  2) = -ctemp03;
	*(boffset +  3) = -ctemp04;

	aoffset1 +=  2 * lda;
	aoffset2 +=  2 * lda;
	boffset   += 4;

	i --;
      }while(i > 0);
    }

    if (m & 1){
      ctemp01 = *(aoffset1 +  0);
      ctemp02 = *(aoffset1 +  1);

      *(boffset +  0) = -ctemp01;
      *(boffset +  1) = -ctemp02;
      // boffset   += 2;
    }
  }

  return 0;
}
