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

  BLASLONG i, js;
  BLASLONG X, ii;

  FLOAT *a01, *a02, *a03, *a04, *a05, *a06, *a07, *a08;
  FLOAT *a09, *a10, *a11, *a12, *a13, *a14, *a15, *a16;

  lda += lda;

  js = (n >> 4);

  if (js > 0){
    do {
      X = posX;

      if (posX <= posY) {
	a01 = a + posX * 2 + (posY + 0) * lda;
	a02 = a + posX * 2 + (posY + 1) * lda;
	a03 = a + posX * 2 + (posY + 2) * lda;
	a04 = a + posX * 2 + (posY + 3) * lda;
	a05 = a + posX * 2 + (posY + 4) * lda;
	a06 = a + posX * 2 + (posY + 5) * lda;
	a07 = a + posX * 2 + (posY + 6) * lda;
	a08 = a + posX * 2 + (posY + 7) * lda;
	a09 = a + posX * 2 + (posY + 8) * lda;
	a10 = a + posX * 2 + (posY + 9) * lda;
	a11 = a + posX * 2 + (posY + 10) * lda;
	a12 = a + posX * 2 + (posY + 11) * lda;
	a13 = a + posX * 2 + (posY + 12) * lda;
	a14 = a + posX * 2 + (posY + 13) * lda;
	a15 = a + posX * 2 + (posY + 14) * lda;
	a16 = a + posX * 2 + (posY + 15) * lda;
      } else {
	a01 = a + posY * 2 + (posX + 0) * lda;
	a02 = a + posY * 2 + (posX + 1) * lda;
	a03 = a + posY * 2 + (posX + 2) * lda;
	a04 = a + posY * 2 + (posX + 3) * lda;
	a05 = a + posY * 2 + (posX + 4) * lda;
	a06 = a + posY * 2 + (posX + 5) * lda;
	a07 = a + posY * 2 + (posX + 6) * lda;
	a08 = a + posY * 2 + (posX + 7) * lda;
	a09 = a + posY * 2 + (posX + 8) * lda;
	a10 = a + posY * 2 + (posX + 9) * lda;
	a11 = a + posY * 2 + (posX + 10) * lda;
	a12 = a + posY * 2 + (posX + 11) * lda;
	a13 = a + posY * 2 + (posX + 12) * lda;
	a14 = a + posY * 2 + (posX + 13) * lda;
	a15 = a + posY * 2 + (posX + 14) * lda;
	a16 = a + posY * 2 + (posX + 15) * lda;
      }

      i = (m >> 4);
      if (i > 0) {
	do {
	  if (X < posY) {
	    for (ii = 0; ii < 16; ii++){

	      b[  0] = *(a01 +  0);
	      b[  1] = *(a01 +  1);
	      b[  2] = *(a02 +  0);
	      b[  3] = *(a02 +  1);
	      b[  4] = *(a03 +  0);
	      b[  5] = *(a03 +  1);
	      b[  6] = *(a04 +  0);
	      b[  7] = *(a04 +  1);

	      b[  8] = *(a05 +  0);
	      b[  9] = *(a05 +  1);
	      b[ 10] = *(a06 +  0);
	      b[ 11] = *(a06 +  1);
	      b[ 12] = *(a07 +  0);
	      b[ 13] = *(a07 +  1);
	      b[ 14] = *(a08 +  0);
	      b[ 15] = *(a08 +  1);

		  b[ 16] = *(a09 +  0);
	      b[ 17] = *(a09 +  1);
	      b[ 18] = *(a10 +  0);
	      b[ 19] = *(a10 +  1);
	      b[ 20] = *(a11 +  0);
	      b[ 21] = *(a11 +  1);
	      b[ 22] = *(a12 +  0);
	      b[ 23] = *(a12 +  1);

	      b[ 24] = *(a13 +  0);
	      b[ 25] = *(a13 +  1);
	      b[ 26] = *(a14 +  0);
	      b[ 27] = *(a14 +  1);
	      b[ 28] = *(a15 +  0);
	      b[ 29] = *(a15 +  1);
	      b[ 30] = *(a16 +  0);
	      b[ 31] = *(a16 +  1);

	      a01 += 2;
	      a02 += 2;
	      a03 += 2;
	      a04 += 2;
	      a05 += 2;
	      a06 += 2;
	      a07 += 2;
	      a08 += 2;
		  a09 += 2;
	      a10 += 2;
	      a11 += 2;
	      a12 += 2;
	      a13 += 2;
	      a14 += 2;
	      a15 += 2;
	      a16 += 2;
	      b += 32;
	    }
	  } else
	    if (X > posY) {
	      a01 += 16 * lda;
	      a02 += 16 * lda;
	      a03 += 16 * lda;
	      a04 += 16 * lda;
	      a05 += 16 * lda;
	      a06 += 16 * lda;
	      a07 += 16 * lda;
	      a08 += 16 * lda;
		  a09 += 16 * lda;
	      a10 += 16 * lda;
	      a11 += 16 * lda;
	      a12 += 16 * lda;
	      a13 += 16 * lda;
	      a14 += 16 * lda;
	      a15 += 16 * lda;
	      a16 += 16 * lda;

	      b += 512;

	    } else {
#ifdef UNIT
	      b[  0] = ONE;
	      b[  1] = ZERO;
#else
	      b[  0] = *(a01 +  0);
	      b[  1] = *(a01 +  1);
#endif
	      b[  2] = *(a02 +  0);
	      b[  3] = *(a02 +  1);
	      b[  4] = *(a03 +  0);
	      b[  5] = *(a03 +  1);
	      b[  6] = *(a04 +  0);
	      b[  7] = *(a04 +  1);
	      b[  8] = *(a05 +  0);
	      b[  9] = *(a05 +  1);
	      b[ 10] = *(a06 +  0);
	      b[ 11] = *(a06 +  1);
	      b[ 12] = *(a07 +  0);
	      b[ 13] = *(a07 +  1);
	      b[ 14] = *(a08 +  0);
	      b[ 15] = *(a08 +  1);
		  b[ 16] = *(a09 +  0);
	      b[ 17] = *(a09 +  1);
	      b[ 18] = *(a10 +  0);
	      b[ 19] = *(a10 +  1);
	      b[ 20] = *(a11 +  0);
	      b[ 21] = *(a11 +  1);
	      b[ 22] = *(a12 +  0);
	      b[ 23] = *(a12 +  1);
	      b[ 24] = *(a13 +  0);
	      b[ 25] = *(a13 +  1);
	      b[ 26] = *(a14 +  0);
	      b[ 27] = *(a14 +  1);
	      b[ 28] = *(a15 +  0);
	      b[ 29] = *(a15 +  1);
		  b[ 30] = *(a16 +  0);
	      b[ 31] = *(a16 +  1);

	      b[ 32] = ZERO;
	      b[ 33] = ZERO;
#ifdef UNIT
	      b[ 34] = ONE;
	      b[ 35] = ZERO;
#else
	      b[ 34] = *(a02 +  2);
	      b[ 35] = *(a02 +  3);
#endif
	      b[ 36] = *(a03 +  2);
	      b[ 37] = *(a03 +  3);
	      b[ 38] = *(a04 +  2);
	      b[ 39] = *(a04 +  3);
	      b[ 40] = *(a05 +  2);
	      b[ 41] = *(a05 +  3);
	      b[ 42] = *(a06 +  2);
	      b[ 43] = *(a06 +  3);
	      b[ 44] = *(a07 +  2);
	      b[ 45] = *(a07 +  3);
	      b[ 46] = *(a08 +  2);
	      b[ 47] = *(a08 +  3);
	      b[ 48] = *(a09 +  2);
	      b[ 49] = *(a09 +  3);
	      b[ 50] = *(a10 +  2);
	      b[ 51] = *(a10 +  3);
	      b[ 52] = *(a11 +  2);
	      b[ 53] = *(a11 +  3);
	      b[ 54] = *(a12 +  2);
	      b[ 55] = *(a12 +  3);
	      b[ 56] = *(a13 +  2);
	      b[ 57] = *(a13 +  3);
	      b[ 58] = *(a14 +  2);
	      b[ 59] = *(a14 +  3);
	      b[ 60] = *(a15 +  2);
	      b[ 61] = *(a15 +  3);
	      b[ 62] = *(a16 +  2);
	      b[ 63] = *(a16 +  3);

	      b[ 64] = ZERO;
	      b[ 65] = ZERO;
	      b[ 66] = ZERO;
	      b[ 67] = ZERO;
#ifdef UNIT
	      b[ 68] = ONE;
	      b[ 69] = ZERO;
#else
	      b[ 68] = *(a03 +  4);
	      b[ 69] = *(a03 +  5);
#endif
	      b[ 70] = *(a04 +  4);
	      b[ 71] = *(a04 +  5);
	      b[ 72] = *(a05 +  4);
	      b[ 73] = *(a05 +  5);
	      b[ 74] = *(a06 +  4);
	      b[ 75] = *(a06 +  5);
	      b[ 76] = *(a07 +  4);
	      b[ 77] = *(a07 +  5);
	      b[ 78] = *(a08 +  4);
	      b[ 79] = *(a08 +  5);
	      b[ 80] = *(a09 +  4);
	      b[ 81] = *(a09 +  5);
	      b[ 82] = *(a10 +  4);
		  b[ 83] = *(a10 +  5);
	      b[ 84] = *(a11 +  4);
	      b[ 85] = *(a11 +  5);
	      b[ 86] = *(a12 +  4);
	      b[ 87] = *(a12 +  5);
	      b[ 88] = *(a13 +  4);
	      b[ 89] = *(a13 +  5);
	      b[ 90] = *(a14 +  4);
	      b[ 91] = *(a14 +  5);
	      b[ 92] = *(a15 +  4);
	      b[ 93] = *(a15 +  5);
	      b[ 94] = *(a16 +  4);
	      b[ 95] = *(a16 +  5);

	      b[ 96] = ZERO;
	      b[ 97] = ZERO;
	      b[ 98] = ZERO;
	      b[ 99] = ZERO;
	      b[100] = ZERO;
	      b[101] = ZERO;
#ifdef UNIT
	      b[102] = ONE;
	      b[103] = ZERO;
#else
	      b[102] = *(a04 +  6);
	      b[103] = *(a04 +  7);
#endif
	      b[104] = *(a05 +  6);
	      b[105] = *(a05 +  7);
	      b[106] = *(a06 +  6);
	      b[107] = *(a06 +  7);
	      b[108] = *(a07 +  6);
	      b[109] = *(a07 +  7);
	      b[110] = *(a08 +  6);
	      b[111] = *(a08 +  7);
	      b[112] = *(a09 +  6);
	      b[113] = *(a09 +  7);
	      b[114] = *(a10 +  6);
	      b[115] = *(a10 +  7);
		  b[116] = *(a11 +  6);
	      b[117] = *(a11 +  7);
	      b[118] = *(a12 +  6);
	      b[119] = *(a12 +  7);
	      b[120] = *(a13 +  6);
	      b[121] = *(a13 +  7);
	      b[122] = *(a14 +  6);
	      b[123] = *(a14 +  7);
	      b[124] = *(a15 +  6);
	      b[125] = *(a15 +  7);
	      b[126] = *(a16 +  6);
	      b[127] = *(a16 +  7);

	      b[128] = ZERO;
	      b[129] = ZERO;
	      b[130] = ZERO;
	      b[131] = ZERO;
	      b[132] = ZERO;
	      b[133] = ZERO;
	      b[134] = ZERO;
	      b[135] = ZERO;
#ifdef UNIT
	      b[136] = ONE;
	      b[137] = ZERO;
#else
	      b[136] = *(a05 +  8);
	      b[137] = *(a05 +  9);
#endif
	      b[138] = *(a06 +  8);
	      b[139] = *(a06 +  9);
	      b[140] = *(a07 +  8);
	      b[141] = *(a07 +  9);
	      b[142] = *(a08 +  8);
	      b[143] = *(a08 +  9);
	      b[144] = *(a09 +  8);
	      b[145] = *(a09 +  9);
	      b[146] = *(a10 +  8);
	      b[147] = *(a10 +  9);
	      b[148] = *(a11 +  8);
		  b[149] = *(a11 +  9);
	      b[150] = *(a12 +  8);
	      b[151] = *(a12 +  9);
	      b[152] = *(a13 +  8);
	      b[153] = *(a13 +  9);
	      b[154] = *(a14 +  8);
	      b[155] = *(a14 +  9);
	      b[156] = *(a15 +  8);
	      b[157] = *(a15 +  9);
	      b[158] = *(a16 +  8);
	      b[159] = *(a16 +  9);

	      b[160] = ZERO;
	      b[161] = ZERO;
	      b[162] = ZERO;
	      b[163] = ZERO;
	      b[164] = ZERO;
	      b[165] = ZERO;
	      b[166] = ZERO;
	      b[167] = ZERO;
	      b[168] = ZERO;
	      b[169] = ZERO;
#ifdef UNIT
	      b[170] = ONE;
	      b[171] = ZERO;
#else
	      b[170] = *(a06 + 10);
	      b[171] = *(a06 + 11);
#endif
	      b[172] = *(a07 + 10);
	      b[173] = *(a07 + 11);
	      b[174] = *(a08 + 10);
	      b[175] = *(a08 + 11);
	      b[176] = *(a09 + 10);
	      b[177] = *(a09 + 11);
	      b[178] = *(a10 + 10);
	      b[179] = *(a10 + 11);
	      b[180] = *(a11 + 10);
	      b[181] = *(a11 + 11);
		  b[182] = *(a12 + 10);
	      b[183] = *(a12 + 11);
	      b[184] = *(a13 + 10);
	      b[185] = *(a13 + 11);
	      b[186] = *(a14 + 10);
	      b[187] = *(a14 + 11);
	      b[188] = *(a15 + 10);
	      b[189] = *(a15 + 11);
	      b[190] = *(a16 + 10);
	      b[191] = *(a16 + 11);

	      b[192] = ZERO;
	      b[193] = ZERO;
	      b[194] = ZERO;
	      b[195] = ZERO;
	      b[196] = ZERO;
	      b[197] = ZERO;
	      b[198] = ZERO;
	      b[199] = ZERO;
	      b[200] = ZERO;
	      b[201] = ZERO;
	      b[202] = ZERO;
	      b[203] = ZERO;
#ifdef UNIT
	      b[204] = ONE;
	      b[205] = ZERO;
#else
	      b[204] = *(a07 + 12);
	      b[205] = *(a07 + 13);
#endif
	      b[206] = *(a08 + 12);
	      b[207] = *(a08 + 13);
	      b[208] = *(a09 + 12);
	      b[209] = *(a09 + 13);
	      b[210] = *(a10 + 12);
	      b[211] = *(a10 + 13);
	      b[212] = *(a11 + 12);
	      b[213] = *(a11 + 13);
	      b[214] = *(a12 + 12);
		  b[215] = *(a12 + 13);
	      b[216] = *(a13 + 12);
	      b[217] = *(a13 + 13);
	      b[218] = *(a14 + 12);
	      b[219] = *(a14 + 13);
	      b[220] = *(a15 + 12);
	      b[221] = *(a15 + 13);
	      b[222] = *(a16 + 12);
	      b[223] = *(a16 + 13);

	      b[224] = ZERO;
	      b[225] = ZERO;
	      b[226] = ZERO;
	      b[227] = ZERO;
	      b[228] = ZERO;
	      b[229] = ZERO;
	      b[230] = ZERO;
	      b[231] = ZERO;
	      b[232] = ZERO;
	      b[233] = ZERO;
	      b[234] = ZERO;
	      b[235] = ZERO;
	      b[236] = ZERO;
	      b[237] = ZERO;
#ifdef UNIT
	      b[238] = ONE;
	      b[239] = ZERO;
#else
	      b[238] = *(a08 + 14);
	      b[239] = *(a08 + 15);
#endif
		  b[240] = *(a09 + 14);
	      b[241] = *(a09 + 15);
	      b[242] = *(a10 + 14);
	      b[243] = *(a10 + 15);
	      b[244] = *(a11 + 14);
	      b[245] = *(a11 + 15);
	      b[246] = *(a12 + 14);
	      b[247] = *(a12 + 15);
		  b[248] = *(a13 + 14);
	      b[249] = *(a13 + 15);
	      b[250] = *(a14 + 14);
	      b[251] = *(a14 + 15);
	      b[252] = *(a15 + 14);
	      b[253] = *(a15 + 15);
	      b[254] = *(a16 + 14);
	      b[255] = *(a16 + 15);

	      b[256] = ZERO;
	      b[257] = ZERO;
	      b[258] = ZERO;
	      b[259] = ZERO;
	      b[260] = ZERO;
	      b[261] = ZERO;
	      b[262] = ZERO;
	      b[263] = ZERO;
	      b[264] = ZERO;
	      b[265] = ZERO;
	      b[266] = ZERO;
	      b[267] = ZERO;
	      b[268] = ZERO;
	      b[269] = ZERO;
	      b[270] = ZERO;
	      b[271] = ZERO;
#ifdef UNIT
	      b[272] = ONE;
		  b[273] = ZERO;
#else
	      b[272] = *(a09 + 16);
		  b[273] = *(a09 + 17);
#endif
		  b[274] = *(a10 + 16);
	      b[275] = *(a10 + 17);
	      b[276] = *(a11 + 16);
	      b[277] = *(a11 + 17);
	      b[278] = *(a12 + 16);
	      b[279] = *(a12 + 17);
	      b[280] = *(a13 + 16);
		  b[281] = *(a13 + 17);
	      b[282] = *(a14 + 16);
	      b[283] = *(a14 + 17);
	      b[284] = *(a15 + 16);
	      b[285] = *(a15 + 17);
	      b[286] = *(a16 + 16);
	      b[287] = *(a16 + 17);

		  b[288] = ZERO;
		  b[289] = ZERO;
	      b[290] = ZERO;
	      b[291] = ZERO;
	      b[292] = ZERO;
	      b[293] = ZERO;
	      b[294] = ZERO;
	      b[295] = ZERO;
	      b[296] = ZERO;
	      b[297] = ZERO;
	      b[298] = ZERO;
	      b[299] = ZERO;
	      b[300] = ZERO;
	      b[301] = ZERO;
	      b[302] = ZERO;
	      b[303] = ZERO;
	      b[304] = ZERO;
	      b[305] = ZERO;
#ifdef UNIT
	      b[306] = ONE;
		  b[307] = ZERO;
#else
	      b[306] = *(a10 + 18);
		  b[307] = *(a10 + 19);
#endif
		  b[308] = *(a11 + 18);
	      b[309] = *(a11 + 19);
	      b[310] = *(a12 + 18);
	      b[311] = *(a12 + 19);
	      b[312] = *(a13 + 18);
	      b[313] = *(a13 + 19);
		  b[314] = *(a14 + 18);
	      b[315] = *(a14 + 19);
	      b[316] = *(a15 + 18);
	      b[317] = *(a15 + 19);
	      b[318] = *(a16 + 18);
	      b[319] = *(a16 + 19);

		  b[320] = ZERO;
		  b[321] = ZERO;
	      b[322] = ZERO;
	      b[323] = ZERO;
	      b[324] = ZERO;
	      b[325] = ZERO;
	      b[326] = ZERO;
	      b[327] = ZERO;
	      b[328] = ZERO;
	      b[329] = ZERO;
	      b[330] = ZERO;
	      b[331] = ZERO;
	      b[332] = ZERO;
	      b[333] = ZERO;
	      b[334] = ZERO;
	      b[335] = ZERO;
	      b[336] = ZERO;
	      b[337] = ZERO;
	      b[338] = ZERO;
	      b[339] = ZERO;
#ifdef UNIT
	      b[340] = ONE;
	      b[341] = ZERO;
#else
	      b[340] = *(a11 + 20);
	      b[341] = *(a11 + 21);
#endif
		  b[342] = *(a12 + 20);
	      b[343] = *(a12 + 21);
	      b[344] = *(a13 + 20);
	      b[345] = *(a13 + 21);
	      b[346] = *(a14 + 20);
		  b[347] = *(a14 + 21);
	      b[348] = *(a15 + 20);
	      b[349] = *(a15 + 21);
	      b[350] = *(a16 + 20);
	      b[351] = *(a16 + 21);

		  b[352] = ZERO;
		  b[353] = ZERO;
	      b[354] = ZERO;
	      b[355] = ZERO;
	      b[356] = ZERO;
	      b[357] = ZERO;
	      b[358] = ZERO;
	      b[359] = ZERO;
	      b[360] = ZERO;
	      b[361] = ZERO;
	      b[362] = ZERO;
	      b[363] = ZERO;
	      b[364] = ZERO;
	      b[365] = ZERO;
	      b[366] = ZERO;
	      b[367] = ZERO;
	      b[368] = ZERO;
	      b[369] = ZERO;
	      b[370] = ZERO;
	      b[371] = ZERO;
	      b[372] = ZERO;
	      b[373] = ZERO;
#ifdef UNIT
	      b[374] = ONE;
	      b[375] = ZERO;
#else
	      b[374] = *(a12 + 22);
	      b[375] = *(a12 + 23);
#endif
		  b[376] = *(a13 + 22);
	      b[377] = *(a13 + 23);
	      b[378] = *(a14 + 22);
	      b[379] = *(a14 + 23);
		  b[380] = *(a15 + 22);
	      b[381] = *(a15 + 23);
	      b[382] = *(a16 + 22);
	      b[383] = *(a16 + 23);

		  b[384] = ZERO;
		  b[385] = ZERO;
	      b[386] = ZERO;
	      b[387] = ZERO;
	      b[388] = ZERO;
	      b[389] = ZERO;
	      b[390] = ZERO;
	      b[391] = ZERO;
	      b[392] = ZERO;
	      b[393] = ZERO;
	      b[394] = ZERO;
	      b[395] = ZERO;
	      b[396] = ZERO;
	      b[397] = ZERO;
	      b[398] = ZERO;
	      b[399] = ZERO;
	      b[400] = ZERO;
	      b[401] = ZERO;
	      b[402] = ZERO;
	      b[403] = ZERO;
	      b[404] = ZERO;
	      b[405] = ZERO;
	      b[406] = ZERO;
	      b[407] = ZERO;
#ifdef UNIT
	      b[408] = ONE;
	      b[409] = ZERO;
#else
	      b[408] = *(a13 + 24);
	      b[409] = *(a13 + 25);
#endif
		  b[410] = *(a14 + 24);
	      b[411] = *(a14 + 25);
	      b[412] = *(a15 + 24);
		  b[413] = *(a15 + 25);
	      b[414] = *(a16 + 24);
	      b[415] = *(a16 + 25);

		  b[416] = ZERO;
		  b[417] = ZERO;
	      b[418] = ZERO;
	      b[419] = ZERO;
	      b[420] = ZERO;
	      b[421] = ZERO;
	      b[422] = ZERO;
	      b[423] = ZERO;
	      b[424] = ZERO;
	      b[425] = ZERO;
	      b[426] = ZERO;
	      b[427] = ZERO;
	      b[428] = ZERO;
	      b[429] = ZERO;
	      b[430] = ZERO;
	      b[431] = ZERO;
	      b[432] = ZERO;
	      b[433] = ZERO;
	      b[434] = ZERO;
	      b[435] = ZERO;
	      b[436] = ZERO;
	      b[437] = ZERO;
	      b[438] = ZERO;
	      b[439] = ZERO;
	      b[440] = ZERO;
	      b[441] = ZERO;
#ifdef UNIT
	      b[442] = ONE;
	      b[443] = ZERO;
#else
	      b[442] = *(a14 + 26);
	      b[443] = *(a14 + 27);
#endif
		  b[444] = *(a15 + 26);
	      b[445] = *(a15 + 27);
		  b[446] = *(a16 + 26);
	      b[447] = *(a16 + 27);

		  b[448] = ZERO;
		  b[449] = ZERO;
	      b[450] = ZERO;
	      b[451] = ZERO;
	      b[452] = ZERO;
	      b[453] = ZERO;
	      b[454] = ZERO;
	      b[455] = ZERO;
	      b[456] = ZERO;
	      b[457] = ZERO;
	      b[458] = ZERO;
	      b[459] = ZERO;
	      b[460] = ZERO;
	      b[461] = ZERO;
	      b[462] = ZERO;
	      b[463] = ZERO;
	      b[464] = ZERO;
	      b[465] = ZERO;
	      b[466] = ZERO;
	      b[467] = ZERO;
	      b[468] = ZERO;
	      b[469] = ZERO;
	      b[470] = ZERO;
	      b[471] = ZERO;
	      b[472] = ZERO;
	      b[473] = ZERO;
	      b[474] = ZERO;
	      b[475] = ZERO;
#ifdef UNIT
	      b[476] = ONE;
	      b[477] = ZERO;
#else
	      b[476] = *(a15 + 28);
	      b[477] = *(a15 + 29);
#endif
		  b[478] = *(a16 + 28);
		  b[479] = *(a16 + 29);

		  b[480] = ZERO;
		  b[481] = ZERO;
	      b[482] = ZERO;
	      b[483] = ZERO;
	      b[484] = ZERO;
	      b[485] = ZERO;
	      b[486] = ZERO;
	      b[487] = ZERO;
	      b[488] = ZERO;
	      b[489] = ZERO;
	      b[490] = ZERO;
	      b[491] = ZERO;
	      b[492] = ZERO;
	      b[493] = ZERO;
	      b[494] = ZERO;
	      b[495] = ZERO;
	      b[496] = ZERO;
	      b[497] = ZERO;
	      b[498] = ZERO;
	      b[499] = ZERO;
	      b[500] = ZERO;
	      b[501] = ZERO;
	      b[502] = ZERO;
	      b[503] = ZERO;
	      b[504] = ZERO;
	      b[505] = ZERO;
	      b[506] = ZERO;
	      b[507] = ZERO;
	      b[508] = ZERO;
	      b[509] = ZERO;
#ifdef UNIT
	      b[510] = ONE;
	      b[511] = ZERO;
#else
	      b[510] = *(a16 + 30);
	      b[511] = *(a16 + 31);
#endif

	      a01 += 16 * lda;
	      a02 += 16 * lda;
	      a03 += 16 * lda;
	      a04 += 16 * lda;
	      a05 += 16 * lda;
	      a06 += 16 * lda;
	      a07 += 16 * lda;
	      a08 += 16 * lda;
	      a09 += 16 * lda;
	      a10 += 16 * lda;
	      a11 += 16 * lda;
	      a12 += 16 * lda;
	      a13 += 16 * lda;
	      a14 += 16 * lda;
	      a15 += 16 * lda;
	      a16 += 16 * lda;
	      b += 512;
	    }

	  X += 16;
	  i --;
	} while (i > 0);
      }

      i = (m & 15);
      if (i) {

	if (X < posY) {

	  for (ii = 0; ii < i; ii++){
	    b[  0] = *(a01 +  0);
	    b[  1] = *(a01 +  1);
		b[  2] = *(a02 +  0);
		b[  3] = *(a02 +  1);
		b[  4] = *(a03 +  0);
		b[  5] = *(a03 +  1);
		b[  6] = *(a04 +  0);
		b[  7] = *(a04 +  1);
		b[  8] = *(a05 +  0);
		b[  9] = *(a05 +  1);
		b[ 10] = *(a06 +  0);
		b[ 11] = *(a06 +  1);
		b[ 12] = *(a07 +  0);
		b[ 13] = *(a07 +  1);
		b[ 14] = *(a08 +  0);
		b[ 15] = *(a08 +  1);

		b[ 16] = *(a09 +  0);
		b[ 17] = *(a09 +  1);
		b[ 18] = *(a10 +  0);
		b[ 19] = *(a10 +  1);
		b[ 20] = *(a11 +  0);
		b[ 21] = *(a11 +  1);
		b[ 22] = *(a12 +  0);
		b[ 23] = *(a12 +  1);
		b[ 24] = *(a13 +  0);
		b[ 25] = *(a13 +  1);
		b[ 26] = *(a14 +  0);
		b[ 27] = *(a14 +  1);
		b[ 28] = *(a15 +  0);
		b[ 29] = *(a15 +  1);
		b[ 30] = *(a16 +  0);
		b[ 31] = *(a16 +  1);

	    a01 += 2;
	    a02 += 2;
	    a03 += 2;
	    a04 += 2;
	    a05 += 2;
	    a06 += 2;
	    a07 += 2;
	    a08 += 2;
		a09 += 2;
	    a10 += 2;
	    a11 += 2;
	    a12 += 2;
	    a13 += 2;
	    a14 += 2;
	    a15 += 2;
	    a16 += 2;
	    b += 32;
	  }
	} else
	  if (X > posY) {
	    /* a01 += i * lda;
	      a02 += i * lda;
	      a03 += i * lda;
	      a04 += i * lda;
	      a05 += i * lda;
	      a06 += i * lda;
	      a07 += i * lda;
	      a08 += i * lda;
	      a09 += i * lda;
	      a10 += i * lda;
	      a11 += i * lda;
	      a12 += i * lda;
	      a13 += i * lda;
	      a14 += i * lda;
	      a15 += i * lda;
	      a16 += i * lda; */
	    b += 32 * i;
	  } else {
#ifdef UNIT
	      b[  0] = ONE;
	      b[  1] = ZERO;
#else
	      b[  0] = *(a01 +  0);
	      b[  1] = *(a01 +  1);
#endif
	      b[  2] = *(a02 +  0);
	      b[  3] = *(a02 +  1);
	      b[  4] = *(a03 +  0);
	      b[  5] = *(a03 +  1);
	      b[  6] = *(a04 +  0);
	      b[  7] = *(a04 +  1);
	      b[  8] = *(a05 +  0);
	      b[  9] = *(a05 +  1);
	      b[ 10] = *(a06 +  0);
	      b[ 11] = *(a06 +  1);
	      b[ 12] = *(a07 +  0);
	      b[ 13] = *(a07 +  1);
	      b[ 14] = *(a08 +  0);
	      b[ 15] = *(a08 +  1);
		  b[ 16] = *(a09 +  0);
	      b[ 17] = *(a09 +  1);
	      b[ 18] = *(a10 +  0);
	      b[ 19] = *(a10 +  1);
	      b[ 20] = *(a11 +  0);
	      b[ 21] = *(a11 +  1);
	      b[ 22] = *(a12 +  0);
	      b[ 23] = *(a12 +  1);
	      b[ 24] = *(a13 +  0);
	      b[ 25] = *(a13 +  1);
	      b[ 26] = *(a14 +  0);
	      b[ 27] = *(a14 +  1);
	      b[ 28] = *(a15 +  0);
	      b[ 29] = *(a15 +  1);
		  b[ 30] = *(a16 +  0);
	      b[ 31] = *(a16 +  1);
	      b += 32;

	      if (i >= 2) {
		b[ 0] = ZERO;
		b[ 1] = ZERO;
#ifdef UNIT
		b[ 2] = ONE;
		b[ 3] = ZERO;
#else
		b[ 2] = *(a02 +  2);
		b[ 3] = *(a02 +  3);
#endif
		b[  4] = *(a03 +  2);
		b[  5] = *(a03 +  3);
		b[  6] = *(a04 +  2);
		b[  7] = *(a04 +  3);
		b[  8] = *(a05 +  2);
		b[  9] = *(a05 +  3);
		b[ 10] = *(a06 +  2);
		b[ 11] = *(a06 +  3);
		b[ 12] = *(a07 +  2);
		b[ 13] = *(a07 +  3);
		b[ 14] = *(a08 +  2);
		b[ 15] = *(a08 +  3);
		b[ 16] = *(a09 +  2);
		b[ 17] = *(a09 +  3);
		b[ 18] = *(a10 +  2);
		b[ 19] = *(a10 +  3);
		b[ 20] = *(a11 +  2);
		b[ 21] = *(a11 +  3);
		b[ 22] = *(a12 +  2);
		b[ 23] = *(a12 +  3);
		b[ 24] = *(a13 +  2);
		b[ 25] = *(a13 +  3);
		b[ 26] = *(a14 +  2);
		b[ 27] = *(a14 +  3);
		b[ 28] = *(a15 +  2);
		b[ 29] = *(a15 +  3);
		b[ 30] = *(a16 +  2);
		b[ 31] = *(a16 +  3);
		b += 32;
	      }

	      if (i >= 3) {
		b[ 0] = ZERO;
		b[ 1] = ZERO;
		b[ 2] = ZERO;
		b[ 3] = ZERO;
#ifdef UNIT
		b[ 4] = ONE;
		b[ 5] = ZERO;
#else
		b[ 4] = *(a03 +  4);
		b[ 5] = *(a03 +  5);
#endif
		b[  6] = *(a04 +  4);
		b[  7] = *(a04 +  5);
		b[  8] = *(a05 +  4);
		b[  9] = *(a05 +  5);
		b[ 10] = *(a06 +  4);
		b[ 11] = *(a06 +  5);
		b[ 12] = *(a07 +  4);
		b[ 13] = *(a07 +  5);
		b[ 14] = *(a08 +  4);
		b[ 15] = *(a08 +  5);
		b[ 16] = *(a09 +  4);
		b[ 17] = *(a09 +  5);
		b[ 18] = *(a10 +  4);
		b[ 19] = *(a10 +  5);
		b[ 20] = *(a11 +  4);
		b[ 21] = *(a11 +  5);
		b[ 22] = *(a12 +  4);
		b[ 23] = *(a12 +  5);
		b[ 24] = *(a13 +  4);
		b[ 25] = *(a13 +  5);
		b[ 26] = *(a14 +  4);
		b[ 27] = *(a14 +  5);
		b[ 28] = *(a15 +  4);
		b[ 29] = *(a15 +  5);
		b[ 30] = *(a16 +  4);
		b[ 31] = *(a16 +  5);
		b += 32;
	      }

	      if (i >= 4) {
		b[ 0] = ZERO;
		b[ 1] = ZERO;
		b[ 2] = ZERO;
		b[ 3] = ZERO;
		b[ 4] = ZERO;
		b[ 5] = ZERO;
#ifdef UNIT
		b[ 6] = ONE;
		b[ 7] = ZERO;
#else
		b[ 6] = *(a04 +  6);
		b[ 7] = *(a04 +  7);
#endif
		b[  8] = *(a05 +  6);
		b[  9] = *(a05 +  7);
		b[ 10] = *(a06 +  6);
		b[ 11] = *(a06 +  7);
		b[ 12] = *(a07 +  6);
		b[ 13] = *(a07 +  7);
		b[ 14] = *(a08 +  6);
		b[ 15] = *(a08 +  7);
		b[ 16] = *(a09 +  6);
		b[ 17] = *(a09 +  7);
		b[ 18] = *(a10 +  6);
		b[ 19] = *(a10 +  7);
		b[ 20] = *(a11 +  6);
		b[ 21] = *(a11 +  7);
		b[ 22] = *(a12 +  6);
		b[ 23] = *(a12 +  7);
		b[ 24] = *(a13 +  6);
		b[ 25] = *(a13 +  7);
		b[ 26] = *(a14 +  6);
		b[ 27] = *(a14 +  7);
		b[ 28] = *(a15 +  6);
		b[ 29] = *(a15 +  7);
		b[ 30] = *(a16 +  6);
		b[ 31] = *(a16 +  7);
		b += 32;
	      }

	      if (i >= 5) {
		b[ 0] = ZERO;
		b[ 1] = ZERO;
		b[ 2] = ZERO;
		b[ 3] = ZERO;
		b[ 4] = ZERO;
		b[ 5] = ZERO;
		b[ 6] = ZERO;
		b[ 7] = ZERO;
#ifdef UNIT
		b[ 8] = ONE;
		b[ 9] = ZERO;
#else
		b[ 8] = *(a05 +  8);
		b[ 9] = *(a05 +  9);
#endif
		b[ 10] = *(a06 +  8);
		b[ 11] = *(a06 +  9);
		b[ 12] = *(a07 +  8);
		b[ 13] = *(a07 +  9);
		b[ 14] = *(a08 +  8);
		b[ 15] = *(a08 +  9);
		b[ 16] = *(a09 +  8);
		b[ 17] = *(a09 +  9);
		b[ 18] = *(a10 +  8);
		b[ 19] = *(a10 +  9);
		b[ 20] = *(a11 +  8);
		b[ 21] = *(a11 +  9);
		b[ 22] = *(a12 +  8);
		b[ 23] = *(a12 +  9);
		b[ 24] = *(a13 +  8);
		b[ 25] = *(a13 +  9);
		b[ 26] = *(a14 +  8);
		b[ 27] = *(a14 +  9);
		b[ 28] = *(a15 +  8);
		b[ 29] = *(a15 +  9);
		b[ 30] = *(a16 +  8);
		b[ 31] = *(a16 +  9);
		b += 32;
	      }

	      if (i >= 6) {
		b[ 0] = ZERO;
		b[ 1] = ZERO;
		b[ 2] = ZERO;
		b[ 3] = ZERO;
		b[ 4] = ZERO;
		b[ 5] = ZERO;
		b[ 6] = ZERO;
		b[ 7] = ZERO;
		b[ 8] = ZERO;
		b[ 9] = ZERO;
#ifdef UNIT
		b[10] = ONE;
		b[11] = ZERO;
#else
		b[10] = *(a06 + 10);
		b[11] = *(a06 + 11);
#endif
		b[ 12] = *(a07 + 10);
		b[ 13] = *(a07 + 11);
		b[ 14] = *(a08 + 10);
		b[ 15] = *(a08 + 11);
		b[ 16] = *(a09 + 10);
		b[ 17] = *(a09 + 11);
		b[ 18] = *(a10 + 10);
		b[ 19] = *(a10 + 11);
		b[ 20] = *(a11 + 10);
		b[ 21] = *(a11 + 11);
		b[ 22] = *(a12 + 10);
		b[ 23] = *(a12 + 11);
		b[ 24] = *(a13 + 10);
		b[ 25] = *(a13 + 11);
		b[ 26] = *(a14 + 10);
		b[ 27] = *(a14 + 11);
		b[ 28] = *(a15 + 10);
		b[ 29] = *(a15 + 11);
		b[ 30] = *(a16 + 10);
		b[ 31] = *(a16 + 11);
		b += 32;
	      }

	      if (i >= 7) {
		b[ 0] = ZERO;
		b[ 1] = ZERO;
		b[ 2] = ZERO;
		b[ 3] = ZERO;
		b[ 4] = ZERO;
		b[ 5] = ZERO;
		b[ 6] = ZERO;
		b[ 7] = ZERO;
		b[ 8] = ZERO;
		b[ 9] = ZERO;
		b[10] = ZERO;
		b[11] = ZERO;
#ifdef UNIT
		b[12] = ONE;
		b[13] = ZERO;
#else
		b[12] = *(a07 + 12);
		b[13] = *(a07 + 13);
#endif
		b[ 14] = *(a08 + 12);
		b[ 15] = *(a08 + 13);
		b[ 16] = *(a09 + 12);
		b[ 17] = *(a09 + 13);
		b[ 18] = *(a10 + 12);
		b[ 19] = *(a10 + 13);
		b[ 20] = *(a11 + 12);
		b[ 21] = *(a11 + 13);
		b[ 22] = *(a12 + 12);
		b[ 23] = *(a12 + 13);
		b[ 24] = *(a13 + 12);
		b[ 25] = *(a13 + 13);
		b[ 26] = *(a14 + 12);
		b[ 27] = *(a14 + 13);
		b[ 28] = *(a15 + 12);
		b[ 29] = *(a15 + 13);
		b[ 30] = *(a16 + 12);
		b[ 31] = *(a16 + 13);
		b += 32;
	    }

		if (i >= 8) {
	    b[ 0] = ZERO;
		b[ 1] = ZERO;
		b[ 2] = ZERO;
		b[ 3] = ZERO;
		b[ 4] = ZERO;
		b[ 5] = ZERO;
		b[ 6] = ZERO;
		b[ 7] = ZERO;
		b[ 8] = ZERO;
		b[ 9] = ZERO;
		b[10] = ZERO;
		b[11] = ZERO;
	    b[12] = ZERO;
	    b[13] = ZERO;
#ifdef UNIT
	      b[ 14] = ONE;
	      b[ 15] = ZERO;
#else
	      b[ 14] = *(a08 +  14);
	      b[ 15] = *(a08 +  15);
#endif
		b[ 16] = *(a09 + 14);
		b[ 17] = *(a09 + 15);
		b[ 18] = *(a10 + 14);
		b[ 19] = *(a10 + 15);
		b[ 20] = *(a11 + 14);
		b[ 21] = *(a11 + 15);
		b[ 22] = *(a12 + 14);
		b[ 23] = *(a12 + 15);
		b[ 24] = *(a13 + 14);
		b[ 25] = *(a13 + 15);
		b[ 26] = *(a14 + 14);
		b[ 27] = *(a14 + 15);
		b[ 28] = *(a15 + 14);
		b[ 29] = *(a15 + 15);
		b[ 30] = *(a16 + 14);
		b[ 31] = *(a16 + 15);
	      b += 32;
	    }

		if (i >= 9) {
	    b[ 0] = ZERO;
		b[ 1] = ZERO;
		b[ 2] = ZERO;
		b[ 3] = ZERO;
		b[ 4] = ZERO;
		b[ 5] = ZERO;
		b[ 6] = ZERO;
		b[ 7] = ZERO;
		b[ 8] = ZERO;
		b[ 9] = ZERO;
		b[10] = ZERO;
		b[11] = ZERO;
	    b[12] = ZERO;
	    b[13] = ZERO;
	    b[ 14] = ZERO;
	    b[ 15] = ZERO;
#ifdef UNIT
	      b[ 16] = ONE;
	      b[ 17] = ZERO;
#else
	      b[ 16] = *(a09 + 16);
	      b[ 17] = *(a09 + 17);
#endif
	    b[ 18] = *(a10 + 16);
		b[ 19] = *(a10 + 17);
		b[ 20] = *(a11 + 16);
		b[ 21] = *(a11 + 17);
		b[ 22] = *(a12 + 16);
		b[ 23] = *(a12 + 17);
		b[ 24] = *(a13 + 16);
		b[ 25] = *(a13 + 17);
		b[ 26] = *(a14 + 16);
		b[ 27] = *(a14 + 17);
		b[ 28] = *(a15 + 16);
		b[ 29] = *(a15 + 17);
		b[ 30] = *(a16 + 16);
		b[ 31] = *(a16 + 17);
	      b += 32;
	    }

		if (i >= 10) {
	    b[ 0] = ZERO;
		b[ 1] = ZERO;
		b[ 2] = ZERO;
		b[ 3] = ZERO;
		b[ 4] = ZERO;
		b[ 5] = ZERO;
		b[ 6] = ZERO;
		b[ 7] = ZERO;
		b[ 8] = ZERO;
		b[ 9] = ZERO;
		b[10] = ZERO;
		b[11] = ZERO;
	    b[12] = ZERO;
	    b[13] = ZERO;
	    b[ 14] = ZERO;
	    b[ 15] = ZERO;
	    b[ 16] = ZERO;
	    b[ 17] = ZERO;
#ifdef UNIT
	      b[ 18] = ONE;
	      b[ 19] = ZERO;
#else
	      b[ 18] = *(a10 + 18);
	      b[ 19] = *(a10 + 19);
#endif
	    b[ 20] = *(a11 + 18);
		b[ 21] = *(a11 + 19);
		b[ 22] = *(a12 + 18);
		b[ 23] = *(a12 + 19);
		b[ 24] = *(a13 + 18);
		b[ 25] = *(a13 + 19);
		b[ 26] = *(a14 + 18);
		b[ 27] = *(a14 + 19);
		b[ 28] = *(a15 + 18);
		b[ 29] = *(a15 + 19);
		b[ 30] = *(a16 + 18);
		b[ 31] = *(a16 + 19);
	      b += 32;
	    }

		if (i >= 11) {
	    b[ 0] = ZERO;
		b[ 1] = ZERO;
		b[ 2] = ZERO;
		b[ 3] = ZERO;
		b[ 4] = ZERO;
		b[ 5] = ZERO;
		b[ 6] = ZERO;
		b[ 7] = ZERO;
		b[ 8] = ZERO;
		b[ 9] = ZERO;
		b[10] = ZERO;
		b[11] = ZERO;
	    b[12] = ZERO;
	    b[13] = ZERO;
	    b[ 14] = ZERO;
	    b[ 15] = ZERO;
	    b[ 16] = ZERO;
	    b[ 17] = ZERO;
	    b[ 18] = ZERO;
	    b[ 19] = ZERO;
#ifdef UNIT
	      b[ 20] = ONE;
	      b[ 21] = ZERO;
#else
	      b[ 20] = *(a11 + 20);
	      b[ 21] = *(a11 + 21);
#endif
	    b[ 22] = *(a12 + 20);
		b[ 23] = *(a12 + 21);
		b[ 24] = *(a13 + 20);
		b[ 25] = *(a13 + 21);
		b[ 26] = *(a14 + 20);
		b[ 27] = *(a14 + 21);
		b[ 28] = *(a15 + 20);
		b[ 29] = *(a15 + 21);
		b[ 30] = *(a16 + 20);
		b[ 31] = *(a16 + 21);
	      b += 32;
	    }

		if (i >= 12) {
	    b[ 0] = ZERO;
		b[ 1] = ZERO;
		b[ 2] = ZERO;
		b[ 3] = ZERO;
		b[ 4] = ZERO;
		b[ 5] = ZERO;
		b[ 6] = ZERO;
		b[ 7] = ZERO;
		b[ 8] = ZERO;
		b[ 9] = ZERO;
		b[10] = ZERO;
		b[11] = ZERO;
	    b[12] = ZERO;
	    b[13] = ZERO;
	    b[ 14] = ZERO;
	    b[ 15] = ZERO;
	    b[ 16] = ZERO;
	    b[ 17] = ZERO;
	    b[ 18] = ZERO;
	    b[ 19] = ZERO;
	    b[ 20] = ZERO;
	    b[ 21] = ZERO;
#ifdef UNIT
	      b[ 22] = ONE;
	      b[ 23] = ZERO;
#else
	      b[ 22] = *(a12 + 22);
	      b[ 23] = *(a12 + 23);
#endif
	    b[ 24] = *(a13 + 22);
		b[ 25] = *(a13 + 23);
		b[ 26] = *(a14 + 22);
		b[ 27] = *(a14 + 23);
		b[ 28] = *(a15 + 22);
		b[ 29] = *(a15 + 23);
		b[ 30] = *(a16 + 22);
		b[ 31] = *(a16 + 23);
	      b += 32;
	    }

		if (i >= 13) {
	    b[ 0] = ZERO;
		b[ 1] = ZERO;
		b[ 2] = ZERO;
		b[ 3] = ZERO;
		b[ 4] = ZERO;
		b[ 5] = ZERO;
		b[ 6] = ZERO;
		b[ 7] = ZERO;
		b[ 8] = ZERO;
		b[ 9] = ZERO;
		b[10] = ZERO;
		b[11] = ZERO;
	    b[12] = ZERO;
	    b[13] = ZERO;
	    b[ 14] = ZERO;
	    b[ 15] = ZERO;
	    b[ 16] = ZERO;
	    b[ 17] = ZERO;
	    b[ 18] = ZERO;
	    b[ 19] = ZERO;
	    b[ 20] = ZERO;
	    b[ 21] = ZERO;
	    b[ 22] = ZERO;
	    b[ 23] = ZERO;
#ifdef UNIT
	      b[ 24] = ONE;
	      b[ 25] = ZERO;
#else
	      b[ 24] = *(a13 + 24);
	      b[ 25] = *(a13 + 25);
#endif
	    b[ 26] = *(a14 + 24);
		b[ 27] = *(a14 + 25);
		b[ 28] = *(a15 + 24);
		b[ 29] = *(a15 + 25);
		b[ 30] = *(a16 + 24);
		b[ 31] = *(a16 + 25);
	      b += 32;
	    }

		if (i >= 14) {
	    b[ 0] = ZERO;
		b[ 1] = ZERO;
		b[ 2] = ZERO;
		b[ 3] = ZERO;
		b[ 4] = ZERO;
		b[ 5] = ZERO;
		b[ 6] = ZERO;
		b[ 7] = ZERO;
		b[ 8] = ZERO;
		b[ 9] = ZERO;
		b[10] = ZERO;
		b[11] = ZERO;
	    b[12] = ZERO;
	    b[13] = ZERO;
	    b[ 14] = ZERO;
	    b[ 15] = ZERO;
	    b[ 16] = ZERO;
	    b[ 17] = ZERO;
	    b[ 18] = ZERO;
	    b[ 19] = ZERO;
	    b[ 20] = ZERO;
	    b[ 21] = ZERO;
	    b[ 22] = ZERO;
	    b[ 23] = ZERO;
	    b[ 24] = ZERO;
	    b[ 25] = ZERO;
#ifdef UNIT
	      b[ 26] = ONE;
	      b[ 27] = ZERO;
#else
	      b[ 26] = *(a14 + 26);
	      b[ 27] = *(a14 + 27);
#endif
	    b[ 28] = *(a15 + 26);
		b[ 29] = *(a15 + 27);
		b[ 30] = *(a16 + 26);
		b[ 31] = *(a16 + 27);
	      b += 32;
	    }

		if (i >= 15) {
	    b[ 0] = ZERO;
		b[ 1] = ZERO;
		b[ 2] = ZERO;
		b[ 3] = ZERO;
		b[ 4] = ZERO;
		b[ 5] = ZERO;
		b[ 6] = ZERO;
		b[ 7] = ZERO;
		b[ 8] = ZERO;
		b[ 9] = ZERO;
		b[10] = ZERO;
		b[11] = ZERO;
	    b[12] = ZERO;
	    b[13] = ZERO;
	    b[ 14] = ZERO;
	    b[ 15] = ZERO;
	    b[ 16] = ZERO;
	    b[ 17] = ZERO;
	    b[ 18] = ZERO;
	    b[ 19] = ZERO;
	    b[ 20] = ZERO;
	    b[ 21] = ZERO;
	    b[ 22] = ZERO;
	    b[ 23] = ZERO;
	    b[ 24] = ZERO;
	    b[ 25] = ZERO;
	    b[ 26] = ZERO;
	    b[ 27] = ZERO;
#ifdef UNIT
	      b[ 28] = ONE;
	      b[ 29] = ZERO;
#else
	      b[ 28] = *(a15 + 28);
	      b[ 29] = *(a15 + 29);
#endif
	    b[ 30] = *(a16 + 28);
		b[ 31] = *(a16 + 29);
	      b += 32;
	    }
	  }
      }

      posY += 16;
      js --;
    } while (js > 0);
  } /* End of main loop */


  if (n & 8){
      X = posX;

      if (posX <= posY) {
	a01 = a + posX * 2 + (posY + 0) * lda;
	a02 = a + posX * 2 + (posY + 1) * lda;
	a03 = a + posX * 2 + (posY + 2) * lda;
	a04 = a + posX * 2 + (posY + 3) * lda;
	a05 = a + posX * 2 + (posY + 4) * lda;
	a06 = a + posX * 2 + (posY + 5) * lda;
	a07 = a + posX * 2 + (posY + 6) * lda;
	a08 = a + posX * 2 + (posY + 7) * lda;
      } else {
	a01 = a + posY * 2 + (posX + 0) * lda;
	a02 = a + posY * 2 + (posX + 1) * lda;
	a03 = a + posY * 2 + (posX + 2) * lda;
	a04 = a + posY * 2 + (posX + 3) * lda;
	a05 = a + posY * 2 + (posX + 4) * lda;
	a06 = a + posY * 2 + (posX + 5) * lda;
	a07 = a + posY * 2 + (posX + 6) * lda;
	a08 = a + posY * 2 + (posX + 7) * lda;
      }

      i = (m >> 3);
      if (i > 0) {
	do {
	  if (X < posY) {
	    for (ii = 0; ii < 8; ii++){

	    b[  0] = *(a01 +  0);
		b[  1] = *(a01 +  1);
		b[  2] = *(a02 +  0);
		b[  3] = *(a02 +  1);
		b[  4] = *(a03 +  0);
		b[  5] = *(a03 +  1);
		b[  6] = *(a04 +  0);
		b[  7] = *(a04 +  1);

		b[  8] = *(a05 +  0);
		b[  9] = *(a05 +  1);
		b[ 10] = *(a06 +  0);
		b[ 11] = *(a06 +  1);
		b[ 12] = *(a07 +  0);
		b[ 13] = *(a07 +  1);
		b[ 14] = *(a08 +  0);
		b[ 15] = *(a08 +  1);

		a01 += 2;
		a02 += 2;
		a03 += 2;
		a04 += 2;
		a05 += 2;
		a06 += 2;
		a07 += 2;
		a08 += 2;
		b += 16;
	    }
	  } else
	    if (X > posY) {
	      a01 += 8 * lda;
	      a02 += 8 * lda;
	      a03 += 8 * lda;
	      a04 += 8 * lda;
	      a05 += 8 * lda;
	      a06 += 8 * lda;
	      a07 += 8 * lda;
	      a08 += 8 * lda;

	      b += 128;
	    } else {
#ifdef UNIT
	      b[  0] = ONE;
	      b[  1] = ZERO;
#else
	      b[  0] = *(a01 +  0);
	      b[  1] = *(a01 +  1);
#endif
	      b[  2] = *(a02 +  0);
	      b[  3] = *(a02 +  1);
	      b[  4] = *(a03 +  0);
	      b[  5] = *(a03 +  1);
	      b[  6] = *(a04 +  0);
	      b[  7] = *(a04 +  1);

	      b[  8] = *(a05 +  0);
	      b[  9] = *(a05 +  1);
	      b[ 10] = *(a06 +  0);
	      b[ 11] = *(a06 +  1);
	      b[ 12] = *(a07 +  0);
	      b[ 13] = *(a07 +  1);
	      b[ 14] = *(a08 +  0);
	      b[ 15] = *(a08 +  1);

	      b[ 16] = ZERO;
	      b[ 17] = ZERO;
#ifdef UNIT
	      b[ 18] = ONE;
	      b[ 19] = ZERO;
#else
	      b[ 18] = *(a02 +  2);
	      b[ 19] = *(a02 +  3);
#endif
	      b[ 20] = *(a03 +  2);
	      b[ 21] = *(a03 +  3);
	      b[ 22] = *(a04 +  2);
	      b[ 23] = *(a04 +  3);
	      b[ 24] = *(a05 +  2);
	      b[ 25] = *(a05 +  3);
	      b[ 26] = *(a06 +  2);
	      b[ 27] = *(a06 +  3);
	      b[ 28] = *(a07 +  2);
	      b[ 29] = *(a07 +  3);
	      b[ 30] = *(a08 +  2);
	      b[ 31] = *(a08 +  3);

	      b[ 32] = ZERO;
	      b[ 33] = ZERO;
	      b[ 34] = ZERO;
	      b[ 35] = ZERO;
#ifdef UNIT
	      b[ 36] = ONE;
	      b[ 37] = ZERO;
#else
	      b[ 36] = *(a03 +  4);
	      b[ 37] = *(a03 +  5);
#endif
	      b[ 38] = *(a04 +  4);
	      b[ 39] = *(a04 +  5);
	      b[ 40] = *(a05 +  4);
	      b[ 41] = *(a05 +  5);
	      b[ 42] = *(a06 +  4);
	      b[ 43] = *(a06 +  5);
	      b[ 44] = *(a07 +  4);
	      b[ 45] = *(a07 +  5);
	      b[ 46] = *(a08 +  4);
	      b[ 47] = *(a08 +  5);

	      b[ 48] = ZERO;
	      b[ 49] = ZERO;
	      b[ 50] = ZERO;
	      b[ 51] = ZERO;
	      b[ 52] = ZERO;
	      b[ 53] = ZERO;
#ifdef UNIT
	      b[ 54] = ONE;
	      b[ 55] = ZERO;
#else
	      b[ 54] = *(a04 +  6);
	      b[ 55] = *(a04 +  7);
#endif
	      b[ 56] = *(a05 +  6);
	      b[ 57] = *(a05 +  7);
	      b[ 58] = *(a06 +  6);
	      b[ 59] = *(a06 +  7);
	      b[ 60] = *(a07 +  6);
	      b[ 61] = *(a07 +  7);
	      b[ 62] = *(a08 +  6);
	      b[ 63] = *(a08 +  7);

	      b[ 64] = ZERO;
	      b[ 65] = ZERO;
	      b[ 66] = ZERO;
	      b[ 67] = ZERO;
	      b[ 68] = ZERO;
	      b[ 69] = ZERO;
	      b[ 70] = ZERO;
	      b[ 71] = ZERO;
#ifdef UNIT
	      b[ 72] = ONE;
	      b[ 73] = ZERO;
#else
	      b[ 72] = *(a05 +  8);
	      b[ 73] = *(a05 +  9);
#endif
	      b[ 74] = *(a06 +  8);
	      b[ 75] = *(a06 +  9);
	      b[ 76] = *(a07 +  8);
	      b[ 77] = *(a07 +  9);
	      b[ 78] = *(a08 +  8);
	      b[ 79] = *(a08 +  9);

	      b[ 80] = ZERO;
	      b[ 81] = ZERO;
	      b[ 82] = ZERO;
	      b[ 83] = ZERO;
	      b[ 84] = ZERO;
	      b[ 85] = ZERO;
	      b[ 86] = ZERO;
	      b[ 87] = ZERO;
	      b[ 88] = ZERO;
	      b[ 89] = ZERO;
#ifdef UNIT
	      b[ 90] = ONE;
	      b[ 91] = ZERO;
#else
	      b[ 90] = *(a06 + 10);
	      b[ 91] = *(a06 + 11);
#endif
	      b[ 92] = *(a07 + 10);
	      b[ 93] = *(a07 + 11);
	      b[ 94] = *(a08 + 10);
	      b[ 95] = *(a08 + 11);

	      b[ 96] = ZERO;
	      b[ 97] = ZERO;
	      b[ 98] = ZERO;
	      b[ 99] = ZERO;
	      b[100] = ZERO;
	      b[101] = ZERO;
	      b[102] = ZERO;
	      b[103] = ZERO;
	      b[104] = ZERO;
	      b[105] = ZERO;
	      b[106] = ZERO;
	      b[107] = ZERO;
#ifdef UNIT
	      b[108] = ONE;
	      b[109] = ZERO;
#else
	      b[108] = *(a07 + 12);
	      b[109] = *(a07 + 13);
#endif
	      b[110] = *(a08 + 12);
	      b[111] = *(a08 + 13);

	      b[112] = ZERO;
	      b[113] = ZERO;
	      b[114] = ZERO;
	      b[115] = ZERO;
	      b[116] = ZERO;
	      b[117] = ZERO;
	      b[118] = ZERO;
	      b[119] = ZERO;
	      b[120] = ZERO;
	      b[121] = ZERO;
	      b[122] = ZERO;
	      b[123] = ZERO;
	      b[124] = ZERO;
	      b[125] = ZERO;
#ifdef UNIT
	      b[126] = ONE;
	      b[127] = ZERO;
#else
	      b[126] = *(a08 + 14);
	      b[127] = *(a08 + 15);
#endif

	      a01 += 8 * lda;
	      a02 += 8 * lda;
	      a03 += 8 * lda;
	      a04 += 8 * lda;
	      a05 += 8 * lda;
	      a06 += 8 * lda;
	      a07 += 8 * lda;
	      a08 += 8 * lda;
	      b += 128;
	    }

	  X += 8;
	  i --;
	} while (i > 0);
      }

      i = (m & 7);
      if (i) {

	if (X < posY) {
	  for (ii = 0; ii < i; ii++){

	    b[  0] = *(a01 +  0);
	    b[  1] = *(a01 +  1);
	    b[  2] = *(a02 +  0);
	    b[  3] = *(a02 +  1);
	    b[  4] = *(a03 +  0);
	    b[  5] = *(a03 +  1);
	    b[  6] = *(a04 +  0);
	    b[  7] = *(a04 +  1);

	    b[  8] = *(a05 +  0);
	    b[  9] = *(a05 +  1);
	    b[ 10] = *(a06 +  0);
	    b[ 11] = *(a06 +  1);
	    b[ 12] = *(a07 +  0);
	    b[ 13] = *(a07 +  1);
	    b[ 14] = *(a08 +  0);
	    b[ 15] = *(a08 +  1);

	    a01 += 2;
	    a02 += 2;
	    a03 += 2;
	    a04 += 2;
	    a05 += 2;
	    a06 += 2;
	    a07 += 2;
	    a08 += 2;
	    b += 16;
	    }
	} else
	  if (X > posY) {
	      /* a01 += i * lda;
	      a02 += i * lda;
	      a03 += i * lda;
	      a04 += i * lda;
	      a05 += i * lda;
	      a06 += i * lda;
	      a07 += i * lda;
	      a08 += i * lda; */
	      b += 16 * i;
	  } else {
#ifdef UNIT
	    b[ 0] = ONE;
	    b[ 1] = ZERO;
#else
	    b[ 0] = *(a01 +  0);
	    b[ 1] = *(a01 +  1);
#endif
	    b[ 2] = *(a02 +  0);
	    b[ 3] = *(a02 +  1);
	    b[ 4] = *(a03 +  0);
	    b[ 5] = *(a03 +  1);
	    b[ 6] = *(a04 +  0);
	    b[ 7] = *(a04 +  1);
	    b[ 8] = *(a05 +  0);
	    b[ 9] = *(a05 +  1);
	    b[10] = *(a06 +  0);
	    b[11] = *(a06 +  1);
	    b[12] = *(a07 +  0);
	    b[13] = *(a07 +  1);
	    b[14] = *(a08 +  0);
	    b[15] = *(a08 +  1);
	    b += 16;

	    if(i >= 2) {
	      b[ 0] = ZERO;
	      b[ 1] = ZERO;
#ifdef UNIT
	      b[ 2] = ONE;
	      b[ 3] = ZERO;
#else
	      b[ 2] = *(a02 +  2);
	      b[ 3] = *(a02 +  3);
#endif
	      b[ 4] = *(a03 +  2);
	      b[ 5] = *(a03 +  3);
	      b[ 6] = *(a04 +  2);
	      b[ 7] = *(a04 +  3);
	      b[ 8] = *(a05 +  2);
	      b[ 9] = *(a05 +  3);
	      b[10] = *(a06 +  2);
	      b[11] = *(a06 +  3);
	      b[12] = *(a07 +  2);
	      b[13] = *(a07 +  3);
	      b[14] = *(a08 +  2);
	      b[15] = *(a08 +  3);
	      b += 16;
	    }

	    if (i >= 3) {
	      b[ 0] = ZERO;
	      b[ 1] = ZERO;
	      b[ 2] = ZERO;
	      b[ 3] = ZERO;
#ifdef UNIT
	      b[ 4] = ONE;
	      b[ 5] = ZERO;
#else
	      b[ 4] = *(a03 +  4);
	      b[ 5] = *(a03 +  5);
#endif
	      b[ 6] = *(a04 +  4);
	      b[ 7] = *(a04 +  5);
	      b[ 8] = *(a05 +  4);
	      b[ 9] = *(a05 +  5);
	      b[10] = *(a06 +  4);
	      b[11] = *(a06 +  5);
	      b[12] = *(a07 +  4);
	      b[13] = *(a07 +  5);
	      b[14] = *(a08 +  4);
	      b[15] = *(a08 +  5);
	      b += 16;
	    }

	    if (i >= 4) {
	      b[ 0] = ZERO;
	      b[ 1] = ZERO;
	      b[ 2] = ZERO;
	      b[ 3] = ZERO;
	      b[ 4] = ZERO;
	      b[ 5] = ZERO;
#ifdef UNIT
	      b[ 6] = ONE;
	      b[ 7] = ZERO;
#else
	      b[ 6] = *(a04 +  6);
	      b[ 7] = *(a04 +  7);
#endif
	      b[ 8] = *(a05 +  6);
	      b[ 9] = *(a05 +  7);
	      b[10] = *(a06 +  6);
	      b[11] = *(a06 +  7);
	      b[12] = *(a07 +  6);
	      b[13] = *(a07 +  7);
	      b[14] = *(a08 +  6);
	      b[15] = *(a08 +  7);
	      b += 16;
	    }

	    if (i >= 5) {
	      b[ 0] = ZERO;
	      b[ 1] = ZERO;
	      b[ 2] = ZERO;
	      b[ 3] = ZERO;
	      b[ 4] = ZERO;
	      b[ 5] = ZERO;
	      b[ 6] = ZERO;
	      b[ 7] = ZERO;
#ifdef UNIT
	      b[ 8] = ONE;
	      b[ 9] = ZERO;
#else
	      b[ 8] = *(a05 +  8);
	      b[ 9] = *(a05 +  9);
#endif
	      b[10] = *(a06 +  8);
	      b[11] = *(a06 +  9);
	      b[12] = *(a07 +  8);
	      b[13] = *(a07 +  9);
	      b[14] = *(a08 +  8);
	      b[15] = *(a08 +  9);
	      b += 16;
	    }

	    if (i >= 6) {
	      b[ 0] = ZERO;
	      b[ 1] = ZERO;
	      b[ 2] = ZERO;
	      b[ 3] = ZERO;
	      b[ 4] = ZERO;
	      b[ 5] = ZERO;
	      b[ 6] = ZERO;
	      b[ 7] = ZERO;
	      b[ 8] = ZERO;
	      b[ 9] = ZERO;
#ifdef UNIT
	      b[10] = ONE;
	      b[11] = ZERO;
#else
	      b[10] = *(a06 + 10);
	      b[11] = *(a06 + 11);
#endif
	      b[12] = *(a07 + 10);
	      b[13] = *(a07 + 11);
	      b[14] = *(a08 + 10);
	      b[15] = *(a08 + 11);
	      b += 16;
	    }

	    if (i >= 7) {
	      b[ 0] = ZERO;
	      b[ 1] = ZERO;
	      b[ 2] = ZERO;
	      b[ 3] = ZERO;
	      b[ 4] = ZERO;
	      b[ 5] = ZERO;
	      b[ 6] = ZERO;
	      b[ 7] = ZERO;
	      b[ 8] = ZERO;
	      b[ 9] = ZERO;
	      b[10] = ZERO;
	      b[11] = ZERO;
#ifdef UNIT
	      b[12] = ONE;
	      b[13] = ZERO;
#else
	      b[12] = *(a07 + 12);
	      b[13] = *(a07 + 13);
#endif
	      b[14] = *(a08 + 12);
	      b[15] = *(a08 + 13);
	      b += 16;
	    }
	  }
      }

      posY += 8;
  }


  if (n & 4){
      X = posX;

      if (posX <= posY) {
	a01 = a + posX * 2 + (posY + 0) * lda;
	a02 = a + posX * 2 + (posY + 1) * lda;
	a03 = a + posX * 2 + (posY + 2) * lda;
	a04 = a + posX * 2 + (posY + 3) * lda;
      } else {
	a01 = a + posY * 2 + (posX + 0) * lda;
	a02 = a + posY * 2 + (posX + 1) * lda;
	a03 = a + posY * 2 + (posX + 2) * lda;
	a04 = a + posY * 2 + (posX + 3) * lda;
      }

      i = (m >> 2);
      if (i > 0) {
	do {
	  if (X < posY) {
	      for (ii = 0; ii < 4; ii++){

		b[  0] = *(a01 +  0);
		b[  1] = *(a01 +  1);
		b[  2] = *(a02 +  0);
		b[  3] = *(a02 +  1);
		b[  4] = *(a03 +  0);
		b[  5] = *(a03 +  1);
		b[  6] = *(a04 +  0);
		b[  7] = *(a04 +  1);

		a01 += 2;
		a02 += 2;
		a03 += 2;
		a04 += 2;
		b += 8;
	      }
	  } else
	    if (X > posY) {
	      a01 += 4 * lda;
	      a02 += 4 * lda;
	      a03 += 4 * lda;
	      a04 += 4 * lda;
	      b += 32;
	    } else {
#ifdef UNIT
	      b[  0] = ONE;
	      b[  1] = ZERO;
#else
	      b[  0] = *(a01 +  0);
	      b[  1] = *(a01 +  1);
#endif
	      b[  2] = *(a02 +  0);
	      b[  3] = *(a02 +  1);
	      b[  4] = *(a03 +  0);
	      b[  5] = *(a03 +  1);
	      b[  6] = *(a04 +  0);
	      b[  7] = *(a04 +  1);

	      b[  8] = ZERO;
	      b[  9] = ZERO;
#ifdef UNIT
	      b[ 10] = ONE;
	      b[ 11] = ZERO;
#else
	      b[ 10] = *(a02 +  2);
	      b[ 11] = *(a02 +  3);
#endif
	      b[ 12] = *(a03 +  2);
	      b[ 13] = *(a03 +  3);
	      b[ 14] = *(a04 +  2);
	      b[ 15] = *(a04 +  3);

	      b[ 16] = ZERO;
	      b[ 17] = ZERO;
	      b[ 18] = ZERO;
	      b[ 19] = ZERO;
#ifdef UNIT
	      b[ 20] = ONE;
	      b[ 21] = ZERO;
#else
	      b[ 20] = *(a03 +  4);
	      b[ 21] = *(a03 +  5);
#endif
	      b[ 22] = *(a04 +  4);
	      b[ 23] = *(a04 +  5);

	      b[ 24] = ZERO;
	      b[ 25] = ZERO;
	      b[ 26] = ZERO;
	      b[ 27] = ZERO;
	      b[ 28] = ZERO;
	      b[ 29] = ZERO;
#ifdef UNIT
	      b[ 30] = ONE;
	      b[ 31] = ZERO;
#else
	      b[ 30] = *(a04 +  6);
	      b[ 31] = *(a04 +  7);
#endif

	      a01 += 4 * lda;
	      a02 += 4 * lda;
	      a03 += 4 * lda;
	      a04 += 4 * lda;

	      b += 32;
	    }

	  X += 4;
	  i --;
	} while (i > 0);
      }

      i = (m & 3);
      if (i) {

	if (X < posY) {

	  for (ii = 0; ii < i; ii++){
	    b[  0] = *(a01 +  0);
	    b[  1] = *(a01 +  1);
	    b[  2] = *(a02 +  0);
	    b[  3] = *(a02 +  1);
	    b[  4] = *(a03 +  0);
	    b[  5] = *(a03 +  1);
	    b[  6] = *(a04 +  0);
	    b[  7] = *(a04 +  1);

	    a01 += 2;
	    a02 += 2;
	    a03 += 2;
	    a04 += 2;
	    b += 8;
	  }
	} else
	  if (X > posY) {
	    /* a01 += i * lda;
	    a02 += i * lda;
	    a03 += i * lda;
	    a04 += i * lda; */
	    b += 8 * i;
	  } else {
#ifdef UNIT
	    b[ 0] = ONE;
	    b[ 1] = ZERO;
#else
	    b[ 0] = *(a01 +  0);
	    b[ 1] = *(a01 +  1);
#endif
	    b[ 2] = *(a02 +  0);
	    b[ 3] = *(a02 +  1);
	    b[ 4] = *(a03 +  0);
	    b[ 5] = *(a03 +  1);
	    b[ 6] = *(a04 +  0);
	    b[ 7] = *(a04 +  1);
	    b += 8;

	    if(i >= 2) {
	      b[ 0] = ZERO;
	      b[ 1] = ZERO;
#ifdef UNIT
	      b[ 2] = ONE;
	      b[ 3] = ZERO;
#else
	      b[ 2] = *(a02 +  2);
	      b[ 3] = *(a02 +  3);
#endif
	      b[ 4] = *(a03 +  2);
	      b[ 5] = *(a03 +  3);
	      b[ 6] = *(a04 +  2);
	      b[ 7] = *(a04 +  3);
	      b += 8;
	    }

	    if (i >= 3) {
	      b[ 0] = ZERO;
	      b[ 1] = ZERO;
	      b[ 2] = ZERO;
	      b[ 3] = ZERO;
#ifdef UNIT
	      b[ 4] = ONE;
	      b[ 5] = ZERO;
#else
	      b[ 4] = *(a03 +  4);
	      b[ 5] = *(a03 +  5);
#endif
	      b[ 6] = *(a04 +  4);
	      b[ 7] = *(a04 +  5);
	      b += 8;
	    }
	  }
      }

      posY += 4;
  }

  if (n & 2){
      X = posX;

      if (posX <= posY) {
	a01 = a + posX * 2 + (posY + 0) * lda;
	a02 = a + posX * 2 + (posY + 1) * lda;
      } else {
	a01 = a + posY * 2 + (posX + 0) * lda;
	a02 = a + posY * 2 + (posX + 1) * lda;
      }

      i = (m >> 1);
      if (i > 0) {
	do {
	  if (X < posY) {
	    b[  0] = *(a01 +  0);
	    b[  1] = *(a01 +  1);
	    b[  2] = *(a02 +  0);
	    b[  3] = *(a02 +  1);
	    b[  4] = *(a01 +  2);
	    b[  5] = *(a01 +  3);
	    b[  6] = *(a02 +  2);
	    b[  7] = *(a02 +  3);

	    a01 += 4;
	    a02 += 4;
	    b += 8;
	  } else
	    if (X > posY) {
	      a01 += 2 * lda;
	      a02 += 2 * lda;
	      b += 8;
	    } else {
#ifdef UNIT
	      b[  0] = ONE;
	      b[  1] = ZERO;
#else
	      b[  0] = *(a01 +  0);
	      b[  1] = *(a01 +  1);
#endif
	      b[  2] = *(a02 +  0);
	      b[  3] = *(a02 +  1);

	      b[  4] = ZERO;
	      b[  5] = ZERO;
#ifdef UNIT
	      b[  6] = ONE;
	      b[  7] = ZERO;
#else
	      b[  6] = *(a02 +  2);
	      b[  7] = *(a02 +  3);
#endif

	      a01 += 2 * lda;
	      a02 += 2 * lda;
	      b += 8;
	    }

	  X += 2;
	  i --;
	} while (i > 0);
      }

      if (m & 1) {

	if (X < posY) {
	  b[  0] = *(a01 +  0);
	  b[  1] = *(a01 +  1);
	  b[  2] = *(a02 +  0);
	  b[  3] = *(a02 +  1);
	  /* a01 += 2;
	  a02 += 2; */
	  b += 4;
	} else
	  if (X > posY) {
	    /* a01 += 2 * lda;
	    a02 += 2 * lda; */
	    b += 4;
	  } else {
#ifdef UNIT
	    b[  0] = ONE;
	    b[  1] = ZERO;
	    b[  2] = *(a02 +  0);
	    b[  3] = *(a02 +  1);
#else
	    b[  0] = *(a01 +  0);
	    b[  1] = *(a01 +  1);
	    b[  2] = *(a02 +  0);
	    b[  3] = *(a02 +  1);
#endif
	    b += 2;
	  }
      }
      posY += 2;
  }

  if (n & 1){
      X = posX;

      if (posX <= posY) {
	a01 = a + posX * 2 + (posY + 0) * lda;
      } else {
	a01 = a + posY * 2 + (posX + 0) * lda;
      }

      i = m;
      if (m > 0) {
	do {
	  if (X < posY) {
	    b[  0] = *(a01 +  0);
	    b[  1] = *(a01 +  1);
	    a01 += 2;
	    b += 2;
	  } else
	    if (X > posY) {
	      a01 += lda;
	      b += 2;
	    } else {
#ifdef UNIT
	      b[ 0] = ONE;
	      b[ 1] = ZERO;
#else
	      b[  0] = *(a01 +  0);
	      b[  1] = *(a01 +  1);
#endif
	      a01 += lda;
	      b += 2;
	    }

	  X += 1;
	  i --;
	} while (i > 0);
      }
  }

  return 0;
}
