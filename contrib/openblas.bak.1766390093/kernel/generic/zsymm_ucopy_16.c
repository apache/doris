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

int CNAME(BLASLONG m, BLASLONG n, FLOAT *a, BLASLONG lda, BLASLONG posX, BLASLONG posY, FLOAT *b){

  BLASLONG i, js, offset;

  FLOAT data01, data02, data03, data04, data05, data06, data07, data08;
  FLOAT data09, data10, data11, data12, data13, data14, data15, data16;
  FLOAT data17, data18, data19, data20, data21, data22, data23, data24;
  FLOAT data25, data26, data27, data28, data29, data30, data31, data32;

  FLOAT *ao1, *ao2,  *ao3,  *ao4,  *ao5,  *ao6,  *ao7,  *ao8;
  FLOAT *ao9, *ao10, *ao11, *ao12, *ao13, *ao14, *ao15, *ao16;

  lda *= 2;

  js = (n >> 4);
  while (js > 0){

    offset = posX - posY;

    if (offset >  0) ao1 = a + posY * 2 + (posX + 0) * lda; else ao1 = a + (posX + 0) * 2 + posY * lda;
    if (offset > -1) ao2 = a + posY * 2 + (posX + 1) * lda; else ao2 = a + (posX + 1) * 2 + posY * lda;
    if (offset > -2) ao3 = a + posY * 2 + (posX + 2) * lda; else ao3 = a + (posX + 2) * 2 + posY * lda;
    if (offset > -3) ao4 = a + posY * 2 + (posX + 3) * lda; else ao4 = a + (posX + 3) * 2 + posY * lda;
    if (offset > -4) ao5 = a + posY * 2 + (posX + 4) * lda; else ao5 = a + (posX + 4) * 2 + posY * lda;
    if (offset > -5) ao6 = a + posY * 2 + (posX + 5) * lda; else ao6 = a + (posX + 5) * 2 + posY * lda;
    if (offset > -6) ao7 = a + posY * 2 + (posX + 6) * lda; else ao7 = a + (posX + 6) * 2 + posY * lda;
    if (offset > -7) ao8 = a + posY * 2 + (posX + 7) * lda; else ao8 = a + (posX + 7) * 2 + posY * lda;
    if (offset > -8) ao9 = a + posY * 2 + (posX + 8) * lda; else ao9 = a + (posX + 8) * 2 + posY * lda;
    if (offset > -9) ao10 = a + posY * 2 + (posX + 9) * lda; else ao10 = a + (posX + 9) * 2 + posY * lda;
    if (offset > -10) ao11 = a + posY * 2 + (posX + 10) * lda; else ao11 = a + (posX + 10) * 2 + posY * lda;
    if (offset > -11) ao12 = a + posY * 2 + (posX + 11) * lda; else ao12 = a + (posX + 11) * 2 + posY * lda;
    if (offset > -12) ao13 = a + posY * 2 + (posX + 12) * lda; else ao13 = a + (posX + 12) * 2 + posY * lda;
    if (offset > -13) ao14 = a + posY * 2 + (posX + 13) * lda; else ao14 = a + (posX + 13) * 2 + posY * lda;
    if (offset > -14) ao15 = a + posY * 2 + (posX + 14) * lda; else ao15 = a + (posX + 14) * 2 + posY * lda;
    if (offset > -15) ao16 = a + posY * 2 + (posX + 15) * lda; else ao16 = a + (posX + 15) * 2 + posY * lda;

    i     = m;

    while (i > 0) {
      data01 = *(ao1 + 0);
      data02 = *(ao1 + 1);
      data03 = *(ao2 + 0);
      data04 = *(ao2 + 1);
      data05 = *(ao3 + 0);
      data06 = *(ao3 + 1);
      data07 = *(ao4 + 0);
      data08 = *(ao4 + 1);
      data09 = *(ao5 + 0);
      data10 = *(ao5 + 1);
      data11 = *(ao6 + 0);
      data12 = *(ao6 + 1);
      data13 = *(ao7 + 0);
      data14 = *(ao7 + 1);
      data15 = *(ao8 + 0);
      data16 = *(ao8 + 1);
      data17 = *(ao9 + 0);
      data18 = *(ao9 + 1);
      data19 = *(ao10 + 0);
      data20 = *(ao10 + 1);
      data21 = *(ao11 + 0);
      data22 = *(ao11 + 1);
      data23 = *(ao12 + 0);
      data24 = *(ao12 + 1);
      data25 = *(ao13 + 0);
      data26 = *(ao13 + 1);
      data27 = *(ao14 + 0);
      data28 = *(ao14 + 1);
      data29 = *(ao15 + 0);
      data30 = *(ao15 + 1);
      data31 = *(ao16 + 0);
      data32 = *(ao16 + 1);

      if (offset >   0) ao1 += 2; else ao1 += lda;
      if (offset >  -1) ao2 += 2; else ao2 += lda;
      if (offset >  -2) ao3 += 2; else ao3 += lda;
      if (offset >  -3) ao4 += 2; else ao4 += lda;
      if (offset >  -4) ao5 += 2; else ao5 += lda;
      if (offset >  -5) ao6 += 2; else ao6 += lda;
      if (offset >  -6) ao7 += 2; else ao7 += lda;
      if (offset >  -7) ao8 += 2; else ao8 += lda;
      if (offset >  -8) ao9 += 2; else ao9 += lda;
      if (offset >  -9) ao10 += 2; else ao10 += lda;
      if (offset >  -10) ao11 += 2; else ao11 += lda;
      if (offset >  -11) ao12 += 2; else ao12 += lda;
      if (offset >  -12) ao13 += 2; else ao13 += lda;
      if (offset >  -13) ao14 += 2; else ao14 += lda;
      if (offset >  -14) ao15 += 2; else ao15 += lda;
      if (offset >  -15) ao16 += 2; else ao16 += lda;

      b[ 0] = data01;
      b[ 1] = data02;
      b[ 2] = data03;
      b[ 3] = data04;
      b[ 4] = data05;
      b[ 5] = data06;
      b[ 6] = data07;
      b[ 7] = data08;
      b[ 8] = data09;
      b[ 9] = data10;
      b[10] = data11;
      b[11] = data12;
      b[12] = data13;
      b[13] = data14;
      b[14] = data15;
      b[15] = data16;
      b[16] = data17;
      b[17] = data18;
      b[18] = data19;
      b[19] = data20;
      b[20] = data21;
      b[21] = data22;
      b[22] = data23;
      b[23] = data24;
      b[24] = data25;
      b[25] = data26;
      b[26] = data27;
      b[27] = data28;
      b[28] = data29;
      b[29] = data30;
      b[30] = data31;
      b[31] = data32;

      b += 32;

      offset --;
      i --;
    }

    posX += 16;
    js --;
  }

  if (n & 8) {
    offset = posX - posY;

    if (offset >  0) ao1 = a + posY * 2 + (posX + 0) * lda; else ao1 = a + (posX + 0) * 2 + posY * lda;
    if (offset > -1) ao2 = a + posY * 2 + (posX + 1) * lda; else ao2 = a + (posX + 1) * 2 + posY * lda;
    if (offset > -2) ao3 = a + posY * 2 + (posX + 2) * lda; else ao3 = a + (posX + 2) * 2 + posY * lda;
    if (offset > -3) ao4 = a + posY * 2 + (posX + 3) * lda; else ao4 = a + (posX + 3) * 2 + posY * lda;
    if (offset > -4) ao5 = a + posY * 2 + (posX + 4) * lda; else ao5 = a + (posX + 4) * 2 + posY * lda;
    if (offset > -5) ao6 = a + posY * 2 + (posX + 5) * lda; else ao6 = a + (posX + 5) * 2 + posY * lda;
    if (offset > -6) ao7 = a + posY * 2 + (posX + 6) * lda; else ao7 = a + (posX + 6) * 2 + posY * lda;
    if (offset > -7) ao8 = a + posY * 2 + (posX + 7) * lda; else ao8 = a + (posX + 7) * 2 + posY * lda;

    i     = m;

    while (i > 0) {
      data01 = *(ao1 + 0);
      data02 = *(ao1 + 1);
      data03 = *(ao2 + 0);
      data04 = *(ao2 + 1);
      data05 = *(ao3 + 0);
      data06 = *(ao3 + 1);
      data07 = *(ao4 + 0);
      data08 = *(ao4 + 1);
      data09 = *(ao5 + 0);
      data10 = *(ao5 + 1);
      data11 = *(ao6 + 0);
      data12 = *(ao6 + 1);
      data13 = *(ao7 + 0);
      data14 = *(ao7 + 1);
      data15 = *(ao8 + 0);
      data16 = *(ao8 + 1);

      if (offset >   0) ao1 += 2; else ao1 += lda;
      if (offset >  -1) ao2 += 2; else ao2 += lda;
      if (offset >  -2) ao3 += 2; else ao3 += lda;
      if (offset >  -3) ao4 += 2; else ao4 += lda;
      if (offset >  -4) ao5 += 2; else ao5 += lda;
      if (offset >  -5) ao6 += 2; else ao6 += lda;
      if (offset >  -6) ao7 += 2; else ao7 += lda;
      if (offset >  -7) ao8 += 2; else ao8 += lda;

      b[ 0] = data01;
      b[ 1] = data02;
      b[ 2] = data03;
      b[ 3] = data04;
      b[ 4] = data05;
      b[ 5] = data06;
      b[ 6] = data07;
      b[ 7] = data08;
      b[ 8] = data09;
      b[ 9] = data10;
      b[10] = data11;
      b[11] = data12;
      b[12] = data13;
      b[13] = data14;
      b[14] = data15;
      b[15] = data16;

      b += 16;

      offset --;
      i --;
    }

    posX += 8;
  }

  if (n & 4) {

    offset = posX - posY;

    if (offset >  0) ao1 = a + posY * 2 + (posX + 0) * lda; else ao1 = a + (posX + 0) * 2 + posY * lda;
    if (offset > -1) ao2 = a + posY * 2 + (posX + 1) * lda; else ao2 = a + (posX + 1) * 2 + posY * lda;
    if (offset > -2) ao3 = a + posY * 2 + (posX + 2) * lda; else ao3 = a + (posX + 2) * 2 + posY * lda;
    if (offset > -3) ao4 = a + posY * 2 + (posX + 3) * lda; else ao4 = a + (posX + 3) * 2 + posY * lda;

    i     = m;

    while (i > 0) {
      data01 = *(ao1 + 0);
      data02 = *(ao1 + 1);
      data03 = *(ao2 + 0);
      data04 = *(ao2 + 1);
      data05 = *(ao3 + 0);
      data06 = *(ao3 + 1);
      data07 = *(ao4 + 0);
      data08 = *(ao4 + 1);

      if (offset >   0) ao1 += 2; else ao1 += lda;
      if (offset >  -1) ao2 += 2; else ao2 += lda;
      if (offset >  -2) ao3 += 2; else ao3 += lda;
      if (offset >  -3) ao4 += 2; else ao4 += lda;

      b[ 0] = data01;
      b[ 1] = data02;
      b[ 2] = data03;
      b[ 3] = data04;
      b[ 4] = data05;
      b[ 5] = data06;
      b[ 6] = data07;
      b[ 7] = data08;

      b += 8;

      offset --;
      i --;
    }

    posX += 4;
  }

  if (n & 2) {

    offset = posX - posY;

    if (offset >  0) ao1 = a + posY * 2 + (posX + 0) * lda; else ao1 = a + (posX + 0) * 2 + posY * lda;
    if (offset > -1) ao2 = a + posY * 2 + (posX + 1) * lda; else ao2 = a + (posX + 1) * 2 + posY * lda;

    i     = m;

    while (i > 0) {
      data01 = *(ao1 + 0);
      data02 = *(ao1 + 1);
      data03 = *(ao2 + 0);
      data04 = *(ao2 + 1);

      if (offset >   0) ao1 += 2; else ao1 += lda;
      if (offset >  -1) ao2 += 2; else ao2 += lda;

      b[ 0] = data01;
      b[ 1] = data02;
      b[ 2] = data03;
      b[ 3] = data04;

      b += 4;

      offset --;
      i --;
    }

    posX += 2;
  }

  if (n & 1) {

    offset = posX - posY;

    if (offset >  0) ao1 = a + posY * 2 + (posX + 0) * lda; else ao1 = a + (posX + 0) * 2 + posY * lda;

    i     = m;

    while (i > 0) {
      data01 = *(ao1 + 0);
      data02 = *(ao1 + 1);

      if (offset >   0) ao1 += 2; else ao1 += lda;

      b[ 0] = data01;
      b[ 1] = data02;

      b += 2;

      offset --;
      i --;
    }

  }

  return 0;
}
