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
      } else {
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
      }

      i = (m >> 4);
      if (i > 0) {
	do {
	  if (X > posY) {
		a01 += 32;
		a02 += 32;
		a03 += 32;
		a04 += 32;
		a05 += 32;
		a06 += 32;
		a07 += 32;
		a08 += 32;
		a09 += 32;
		a10 += 32;
		a11 += 32;
		a12 += 32;
		a13 += 32;
		a14 += 32;
		a15 += 32;
		a16 += 32;
		b += 512;
	  } else
	    if (X < posY) {
	      for (ii = 0; ii < 16; ii++){

	      b[  0] = *(a01 +  0);
	      b[  1] = *(a01 +  1);
	      b[  2] = *(a01 +  2);
	      b[  3] = *(a01 +  3);
	      b[  4] = *(a01 +  4);
	      b[  5] = *(a01 +  5);
	      b[  6] = *(a01 +  6);
	      b[  7] = *(a01 +  7);

	      b[  8] = *(a01 +  8);
	      b[  9] = *(a01 +  9);
	      b[ 10] = *(a01 + 10);
	      b[ 11] = *(a01 + 11);
	      b[ 12] = *(a01 + 12);
	      b[ 13] = *(a01 + 13);
	      b[ 14] = *(a01 + 14);
	      b[ 15] = *(a01 + 15);

		  b[ 16] = *(a01 + 16);
	      b[ 17] = *(a01 + 17);
	      b[ 18] = *(a01 + 18);
	      b[ 19] = *(a01 + 19);
	      b[ 20] = *(a01 + 20);
	      b[ 21] = *(a01 + 21);
	      b[ 22] = *(a01 + 22);
	      b[ 23] = *(a01 + 23);

	      b[ 24] = *(a01 + 24);
	      b[ 25] = *(a01 + 25);
	      b[ 26] = *(a01 + 26);
	      b[ 27] = *(a01 + 27);
	      b[ 28] = *(a01 + 28);
	      b[ 29] = *(a01 + 29);
	      b[ 30] = *(a01 + 30);
	      b[ 31] = *(a01 + 31);

	      a01 += lda;
	      b += 32;
	    }
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

	    } else {
#ifdef UNIT
	      b[  0] = ONE;
	      b[  1] = ZERO;
#else
	      b[  0] = *(a01 +  0);
	      b[  1] = *(a01 +  1);
#endif
	      b[  2] = *(a01 +  2);
	      b[  3] = *(a01 +  3);
	      b[  4] = *(a01 +  4);
	      b[  5] = *(a01 +  5);
	      b[  6] = *(a01 +  6);
	      b[  7] = *(a01 +  7);
	      b[  8] = *(a01 +  8);
	      b[  9] = *(a01 +  9);
	      b[ 10] = *(a01 + 10);
	      b[ 11] = *(a01 + 11);
	      b[ 12] = *(a01 + 12);
	      b[ 13] = *(a01 + 13);
	      b[ 14] = *(a01 + 14);
	      b[ 15] = *(a01 + 15);
		  b[ 16] = *(a01 + 16);
	      b[ 17] = *(a01 + 17);
	      b[ 18] = *(a01 + 18);
	      b[ 19] = *(a01 + 19);
	      b[ 20] = *(a01 + 20);
	      b[ 21] = *(a01 + 21);
	      b[ 22] = *(a01 + 22);
	      b[ 23] = *(a01 + 23);
	      b[ 24] = *(a01 + 24);
	      b[ 25] = *(a01 + 25);
	      b[ 26] = *(a01 + 26);
	      b[ 27] = *(a01 + 27);
	      b[ 28] = *(a01 + 28);
	      b[ 29] = *(a01 + 29);
		  b[ 30] = *(a01 + 30);
	      b[ 31] = *(a01 + 31);

	      b[ 32] = ZERO;
	      b[ 33] = ZERO;
#ifdef UNIT
	      b[ 34] = ONE;
	      b[ 35] = ZERO;
#else
	      b[ 34] = *(a02 +  2);
	      b[ 35] = *(a02 +  3);
#endif
	      b[ 36] = *(a02 +  4);
	      b[ 37] = *(a02 +  5);
	      b[ 38] = *(a02 +  6);
	      b[ 39] = *(a02 +  7);
	      b[ 40] = *(a02 +  8);
	      b[ 41] = *(a02 +  9);
	      b[ 42] = *(a02 + 10);
	      b[ 43] = *(a02 + 11);
	      b[ 44] = *(a02 + 12);
	      b[ 45] = *(a02 + 13);
	      b[ 46] = *(a02 + 14);
	      b[ 47] = *(a02 + 15);
	      b[ 48] = *(a02 + 16);
	      b[ 49] = *(a02 + 17);
	      b[ 50] = *(a02 + 18);
	      b[ 51] = *(a02 + 19);
	      b[ 52] = *(a02 + 20);
	      b[ 53] = *(a02 + 21);
	      b[ 54] = *(a02 + 22);
	      b[ 55] = *(a02 + 23);
	      b[ 56] = *(a02 + 24);
	      b[ 57] = *(a02 + 25);
	      b[ 58] = *(a02 + 26);
	      b[ 59] = *(a02 + 27);
	      b[ 60] = *(a02 + 28);
	      b[ 61] = *(a02 + 29);
	      b[ 62] = *(a02 + 30);
	      b[ 63] = *(a02 + 31);

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
	      b[ 70] = *(a03 +  6);
	      b[ 71] = *(a03 +  7);
	      b[ 72] = *(a03 +  8);
	      b[ 73] = *(a03 +  9);
	      b[ 74] = *(a03 + 10);
	      b[ 75] = *(a03 + 11);
	      b[ 76] = *(a03 + 12);
	      b[ 77] = *(a03 + 13);
	      b[ 78] = *(a03 + 14);
	      b[ 79] = *(a03 + 15);
	      b[ 80] = *(a03 + 16);
	      b[ 81] = *(a03 + 17);
	      b[ 82] = *(a03 + 18);
		  b[ 83] = *(a03 + 19);
	      b[ 84] = *(a03 + 20);
	      b[ 85] = *(a03 + 21);
	      b[ 86] = *(a03 + 22);
	      b[ 87] = *(a03 + 23);
	      b[ 88] = *(a03 + 24);
	      b[ 89] = *(a03 + 25);
	      b[ 90] = *(a03 + 26);
	      b[ 91] = *(a03 + 27);
	      b[ 92] = *(a03 + 28);
	      b[ 93] = *(a03 + 29);
	      b[ 94] = *(a03 + 30);
	      b[ 95] = *(a03 + 31);

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
	      b[104] = *(a04 +  8);
	      b[105] = *(a04 +  9);
	      b[106] = *(a04 + 10);
	      b[107] = *(a04 + 11);
	      b[108] = *(a04 + 12);
	      b[109] = *(a04 + 13);
	      b[110] = *(a04 + 14);
	      b[111] = *(a04 + 15);
	      b[112] = *(a04 + 16);
	      b[113] = *(a04 + 17);
	      b[114] = *(a04 + 18);
	      b[115] = *(a04 + 19);
		  b[116] = *(a04 + 20);
	      b[117] = *(a04 + 21);
	      b[118] = *(a04 + 22);
	      b[119] = *(a04 + 23);
	      b[120] = *(a04 + 24);
	      b[121] = *(a04 + 25);
	      b[122] = *(a04 + 26);
	      b[123] = *(a04 + 27);
	      b[124] = *(a04 + 28);
	      b[125] = *(a04 + 29);
	      b[126] = *(a04 + 30);
	      b[127] = *(a04 + 31);

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
	      b[138] = *(a05 + 10);
	      b[139] = *(a05 + 11);
	      b[140] = *(a05 + 12);
	      b[141] = *(a05 + 13);
	      b[142] = *(a05 + 14);
	      b[143] = *(a05 + 15);
	      b[144] = *(a05 + 16);
	      b[145] = *(a05 + 17);
	      b[146] = *(a05 + 18);
	      b[147] = *(a05 + 19);
	      b[148] = *(a05 + 20);
		  b[149] = *(a05 + 21);
	      b[150] = *(a05 + 22);
	      b[151] = *(a05 + 23);
	      b[152] = *(a05 + 24);
	      b[153] = *(a05 + 25);
	      b[154] = *(a05 + 26);
	      b[155] = *(a05 + 27);
	      b[156] = *(a05 + 28);
	      b[157] = *(a05 + 29);
	      b[158] = *(a05 + 30);
	      b[159] = *(a05 + 31);

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
	      b[172] = *(a06 + 12);
	      b[173] = *(a06 + 13);
	      b[174] = *(a06 + 14);
	      b[175] = *(a06 + 15);
	      b[176] = *(a06 + 16);
	      b[177] = *(a06 + 17);
	      b[178] = *(a06 + 18);
	      b[179] = *(a06 + 19);
	      b[180] = *(a06 + 20);
	      b[181] = *(a06 + 21);
		  b[182] = *(a06 + 22);
	      b[183] = *(a06 + 23);
	      b[184] = *(a06 + 24);
	      b[185] = *(a06 + 25);
	      b[186] = *(a06 + 26);
	      b[187] = *(a06 + 27);
	      b[188] = *(a06 + 28);
	      b[189] = *(a06 + 29);
	      b[190] = *(a06 + 30);
	      b[191] = *(a06 + 31);

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
	      b[206] = *(a07 + 14);
	      b[207] = *(a07 + 15);
	      b[208] = *(a07 + 16);
	      b[209] = *(a07 + 17);
	      b[210] = *(a07 + 18);
	      b[211] = *(a07 + 19);
	      b[212] = *(a07 + 20);
	      b[213] = *(a07 + 21);
	      b[214] = *(a07 + 22);
		  b[215] = *(a07 + 23);
	      b[216] = *(a07 + 24);
	      b[217] = *(a07 + 25);
	      b[218] = *(a07 + 26);
	      b[219] = *(a07 + 27);
	      b[220] = *(a07 + 28);
	      b[221] = *(a07 + 29);
	      b[222] = *(a07 + 30);
	      b[223] = *(a07 + 31);

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
		  b[240] = *(a08 + 16);
	      b[241] = *(a08 + 17);
	      b[242] = *(a08 + 18);
	      b[243] = *(a08 + 19);
	      b[244] = *(a08 + 20);
	      b[245] = *(a08 + 21);
	      b[246] = *(a08 + 22);
	      b[247] = *(a08 + 23);
		  b[248] = *(a08 + 24);
	      b[249] = *(a08 + 25);
	      b[250] = *(a08 + 26);
	      b[251] = *(a08 + 27);
	      b[252] = *(a08 + 28);
	      b[253] = *(a08 + 29);
	      b[254] = *(a08 + 30);
	      b[255] = *(a08 + 31);

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
		  b[274] = *(a09 + 18);
	      b[275] = *(a09 + 19);
	      b[276] = *(a09 + 20);
	      b[277] = *(a09 + 21);
	      b[278] = *(a09 + 22);
	      b[279] = *(a09 + 23);
	      b[280] = *(a09 + 24);
		  b[281] = *(a09 + 25);
	      b[282] = *(a09 + 26);
	      b[283] = *(a09 + 27);
	      b[284] = *(a09 + 28);
	      b[285] = *(a09 + 29);
	      b[286] = *(a09 + 30);
	      b[287] = *(a09 + 31);

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
		  b[308] = *(a10 + 20);
	      b[309] = *(a10 + 21);
	      b[310] = *(a10 + 22);
	      b[311] = *(a10 + 23);
	      b[312] = *(a10 + 24);
	      b[313] = *(a10 + 25);
		  b[314] = *(a10 + 26);
	      b[315] = *(a10 + 27);
	      b[316] = *(a10 + 28);
	      b[317] = *(a10 + 29);
	      b[318] = *(a10 + 30);
	      b[319] = *(a10 + 31);

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
		  b[342] = *(a11 + 22);
	      b[343] = *(a11 + 23);
	      b[344] = *(a11 + 24);
	      b[345] = *(a11 + 25);
	      b[346] = *(a11 + 26);
		  b[347] = *(a11 + 27);
	      b[348] = *(a11 + 28);
	      b[349] = *(a11 + 29);
	      b[350] = *(a11 + 30);
	      b[351] = *(a11 + 31);

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
		  b[376] = *(a12 + 24);
	      b[377] = *(a12 + 25);
	      b[378] = *(a12 + 26);
	      b[379] = *(a12 + 27);
		  b[380] = *(a12 + 28);
	      b[381] = *(a12 + 29);
	      b[382] = *(a12 + 30);
	      b[383] = *(a12 + 31);

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
		  b[410] = *(a13 + 26);
	      b[411] = *(a13 + 27);
	      b[412] = *(a13 + 28);
		  b[413] = *(a13 + 29);
	      b[414] = *(a13 + 30);
	      b[415] = *(a13 + 31);

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
		  b[444] = *(a14 + 28);
	      b[445] = *(a14 + 29);
		  b[446] = *(a14 + 30);
	      b[447] = *(a14 + 31);

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
		  b[478] = *(a15 + 30);
		  b[479] = *(a15 + 31);

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

	      a01 += 32;
	      a02 += 32;
	      a03 += 32;
	      a04 += 32;
	      a05 += 32;
	      a06 += 32;
	      a07 += 32;
	      a08 += 32;
	      a09 += 32;
	      a10 += 32;
	      a11 += 32;
	      a12 += 32;
	      a13 += 32;
	      a14 += 32;
	      a15 += 32;
	      a16 += 32;
	      b += 512;
	    }

	  X += 16;
	  i --;
	} while (i > 0);
      }

      i = (m & 15);
      if (i) {

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
	} else
	  if (X < posY) {
	    for (ii = 0; ii < i; ii++){
	    b[  0] = *(a01 +  0);
	    b[  1] = *(a01 +  1);
		b[  2] = *(a01 +  2);
		b[  3] = *(a01 +  3);
		b[  4] = *(a01 +  4);
		b[  5] = *(a01 +  5);
		b[  6] = *(a01 +  6);
		b[  7] = *(a01 +  7);
		b[  8] = *(a01 +  8);
		b[  9] = *(a01 +  9);
		b[ 10] = *(a01 + 10);
		b[ 11] = *(a01 + 11);
		b[ 12] = *(a01 + 12);
		b[ 13] = *(a01 + 13);
		b[ 14] = *(a01 + 14);
		b[ 15] = *(a01 + 15);

		b[ 16] = *(a01 + 16);
		b[ 17] = *(a01 + 17);
		b[ 18] = *(a01 + 18);
		b[ 19] = *(a01 + 19);
		b[ 20] = *(a01 + 20);
		b[ 21] = *(a01 + 21);
		b[ 22] = *(a01 + 22);
		b[ 23] = *(a01 + 23);
		b[ 24] = *(a01 + 24);
		b[ 25] = *(a01 + 25);
		b[ 26] = *(a01 + 26);
		b[ 27] = *(a01 + 27);
		b[ 28] = *(a01 + 28);
		b[ 29] = *(a01 + 29);
		b[ 30] = *(a01 + 30);
		b[ 31] = *(a01 + 31);

	    a01 += lda;
	    a02 += lda;
	    a03 += lda;
	    a04 += lda;
	    a05 += lda;
	    a06 += lda;
	    a07 += lda;
	    a08 += lda;
		a09 += lda;
	    a10 += lda;
	    a11 += lda;
	    a12 += lda;
	    a13 += lda;
	    a14 += lda;
	    a15 += lda;
	    a16 += lda;
	    b += 32;
	  }
	  } else {
#ifdef UNIT
	      b[  0] = ONE;
	      b[  1] = ZERO;
#else
	      b[  0] = *(a01 +  0);
	      b[  1] = *(a01 +  1);
#endif
	      b[  2] = *(a01 +  2);
	      b[  3] = *(a01 +  3);
	      b[  4] = *(a01 +  4);
	      b[  5] = *(a01 +  5);
	      b[  6] = *(a01 +  6);
	      b[  7] = *(a01 +  7);
	      b[  8] = *(a01 +  8);
	      b[  9] = *(a01 +  9);
	      b[ 10] = *(a01 + 10);
	      b[ 11] = *(a01 + 11);
	      b[ 12] = *(a01 + 12);
	      b[ 13] = *(a01 + 13);
	      b[ 14] = *(a01 + 14);
	      b[ 15] = *(a01 + 15);
		  b[ 16] = *(a01 + 16);
	      b[ 17] = *(a01 + 17);
	      b[ 18] = *(a01 + 18);
	      b[ 19] = *(a01 + 19);
	      b[ 20] = *(a01 + 20);
	      b[ 21] = *(a01 + 21);
	      b[ 22] = *(a01 + 22);
	      b[ 23] = *(a01 + 23);
	      b[ 24] = *(a01 + 24);
	      b[ 25] = *(a01 + 25);
	      b[ 26] = *(a01 + 26);
	      b[ 27] = *(a01 + 27);
	      b[ 28] = *(a01 + 28);
	      b[ 29] = *(a01 + 29);
		  b[ 30] = *(a01 + 30);
	      b[ 31] = *(a01 + 31);
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
		b[  4] = *(a02 +  4);
		b[  5] = *(a02 +  5);
		b[  6] = *(a02 +  6);
		b[  7] = *(a02 +  7);
		b[  8] = *(a02 +  8);
		b[  9] = *(a02 +  9);
		b[ 10] = *(a02 + 10);
		b[ 11] = *(a02 + 11);
		b[ 12] = *(a02 + 12);
		b[ 13] = *(a02 + 13);
		b[ 14] = *(a02 + 14);
		b[ 15] = *(a02 + 15);
		b[ 16] = *(a02 + 16);
		b[ 17] = *(a02 + 17);
		b[ 18] = *(a02 + 18);
		b[ 19] = *(a02 + 19);
		b[ 20] = *(a02 + 20);
		b[ 21] = *(a02 + 21);
		b[ 22] = *(a02 + 22);
		b[ 23] = *(a02 + 23);
		b[ 24] = *(a02 + 24);
		b[ 25] = *(a02 + 25);
		b[ 26] = *(a02 + 26);
		b[ 27] = *(a02 + 27);
		b[ 28] = *(a02 + 28);
		b[ 29] = *(a02 + 29);
		b[ 30] = *(a02 + 30);
		b[ 31] = *(a02 + 31);
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
		b[  6] = *(a03 +  6);
		b[  7] = *(a03 +  7);
		b[  8] = *(a03 +  8);
		b[  9] = *(a03 +  9);
		b[ 10] = *(a03 + 10);
		b[ 11] = *(a03 + 11);
		b[ 12] = *(a03 + 12);
		b[ 13] = *(a03 + 13);
		b[ 14] = *(a03 + 14);
		b[ 15] = *(a03 + 15);
		b[ 16] = *(a03 + 16);
		b[ 17] = *(a03 + 17);
		b[ 18] = *(a03 + 18);
		b[ 19] = *(a03 + 19);
		b[ 20] = *(a03 + 20);
		b[ 21] = *(a03 + 21);
		b[ 22] = *(a03 + 22);
		b[ 23] = *(a03 + 23);
		b[ 24] = *(a03 + 24);
		b[ 25] = *(a03 + 25);
		b[ 26] = *(a03 + 26);
		b[ 27] = *(a03 + 27);
		b[ 28] = *(a03 + 28);
		b[ 29] = *(a03 + 29);
		b[ 30] = *(a03 + 30);
		b[ 31] = *(a03 + 31);
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
		b[  8] = *(a04 +  8);
		b[  9] = *(a04 +  9);
		b[ 10] = *(a04 + 10);
		b[ 11] = *(a04 + 11);
		b[ 12] = *(a04 + 12);
		b[ 13] = *(a04 + 13);
		b[ 14] = *(a04 + 14);
		b[ 15] = *(a04 + 15);
		b[ 16] = *(a04 + 16);
		b[ 17] = *(a04 + 17);
		b[ 18] = *(a04 + 18);
		b[ 19] = *(a04 + 19);
		b[ 20] = *(a04 + 20);
		b[ 21] = *(a04 + 21);
		b[ 22] = *(a04 + 22);
		b[ 23] = *(a04 + 23);
		b[ 24] = *(a04 + 24);
		b[ 25] = *(a04 + 25);
		b[ 26] = *(a04 + 26);
		b[ 27] = *(a04 + 27);
		b[ 28] = *(a04 + 28);
		b[ 29] = *(a04 + 29);
		b[ 30] = *(a04 + 30);
		b[ 31] = *(a04 + 31);
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
		b[ 10] = *(a05 + 10);
		b[ 11] = *(a05 + 11);
		b[ 12] = *(a05 + 12);
		b[ 13] = *(a05 + 13);
		b[ 14] = *(a05 + 14);
		b[ 15] = *(a05 + 15);
		b[ 16] = *(a05 + 16);
		b[ 17] = *(a05 + 17);
		b[ 18] = *(a05 + 18);
		b[ 19] = *(a05 + 19);
		b[ 20] = *(a05 + 20);
		b[ 21] = *(a05 + 21);
		b[ 22] = *(a05 + 22);
		b[ 23] = *(a05 + 23);
		b[ 24] = *(a05 + 24);
		b[ 25] = *(a05 + 25);
		b[ 26] = *(a05 + 26);
		b[ 27] = *(a05 + 27);
		b[ 28] = *(a05 + 28);
		b[ 29] = *(a05 + 29);
		b[ 30] = *(a05 + 30);
		b[ 31] = *(a05 + 31);
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
		b[ 12] = *(a06 + 12);
		b[ 13] = *(a06 + 13);
		b[ 14] = *(a06 + 14);
		b[ 15] = *(a06 + 15);
		b[ 16] = *(a06 + 16);
		b[ 17] = *(a06 + 17);
		b[ 18] = *(a06 + 18);
		b[ 19] = *(a06 + 19);
		b[ 20] = *(a06 + 20);
		b[ 21] = *(a06 + 21);
		b[ 22] = *(a06 + 22);
		b[ 23] = *(a06 + 23);
		b[ 24] = *(a06 + 24);
		b[ 25] = *(a06 + 25);
		b[ 26] = *(a06 + 26);
		b[ 27] = *(a06 + 27);
		b[ 28] = *(a06 + 28);
		b[ 29] = *(a06 + 29);
		b[ 30] = *(a06 + 30);
		b[ 31] = *(a06 + 31);
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
		b[ 14] = *(a07 + 14);
		b[ 15] = *(a07 + 15);
		b[ 16] = *(a07 + 16);
		b[ 17] = *(a07 + 17);
		b[ 18] = *(a07 + 18);
		b[ 19] = *(a07 + 19);
		b[ 20] = *(a07 + 20);
		b[ 21] = *(a07 + 21);
		b[ 22] = *(a07 + 22);
		b[ 23] = *(a07 + 23);
		b[ 24] = *(a07 + 24);
		b[ 25] = *(a07 + 25);
		b[ 26] = *(a07 + 26);
		b[ 27] = *(a07 + 27);
		b[ 28] = *(a07 + 28);
		b[ 29] = *(a07 + 29);
		b[ 30] = *(a07 + 30);
		b[ 31] = *(a07 + 31);
		b += 32;
	    }

		if (i >= 8) {
	      b[  0] = ZERO;
	      b[  1] = ZERO;
	      b[  2] = ZERO;
	      b[  3] = ZERO;
	      b[  4] = ZERO;
	      b[  5] = ZERO;
	      b[  6] = ZERO;
	      b[  7] = ZERO;
	      b[  8] = ZERO;
	      b[  9] = ZERO;
	      b[ 10] = ZERO;
	      b[ 11] = ZERO;
	      b[ 12] = ZERO;
	      b[ 13] = ZERO;
#ifdef UNIT
	      b[ 14] = ONE;
	      b[ 15] = ZERO;
#else
	      b[ 14] = *(a08 +  14);
	      b[ 15] = *(a08 +  15);
#endif
		b[ 16] = *(a08 + 16);
		b[ 17] = *(a08 + 17);
		b[ 18] = *(a08 + 18);
		b[ 19] = *(a08 + 19);
		b[ 20] = *(a08 + 20);
		b[ 21] = *(a08 + 21);
		b[ 22] = *(a08 + 22);
		b[ 23] = *(a08 + 23);
		b[ 24] = *(a08 + 24);
		b[ 25] = *(a08 + 25);
		b[ 26] = *(a08 + 26);
		b[ 27] = *(a08 + 27);
		b[ 28] = *(a08 + 28);
		b[ 29] = *(a08 + 29);
		b[ 30] = *(a08 + 30);
		b[ 31] = *(a08 + 31);
	      b += 32;
	    }

		if (i >= 9) {
	      b[  0] = ZERO;
	      b[  1] = ZERO;
	      b[  2] = ZERO;
	      b[  3] = ZERO;
	      b[  4] = ZERO;
	      b[  5] = ZERO;
	      b[  6] = ZERO;
	      b[  7] = ZERO;
	      b[  8] = ZERO;
	      b[  9] = ZERO;
	      b[ 10] = ZERO;
	      b[ 11] = ZERO;
	      b[ 12] = ZERO;
	      b[ 13] = ZERO;
	      b[ 14] = ZERO;
	      b[ 15] = ZERO;
#ifdef UNIT
	      b[ 16] = ONE;
	      b[ 17] = ZERO;
#else
	      b[ 16] = *(a09 + 16);
	      b[ 17] = *(a09 + 17);
#endif
	    b[ 18] = *(a09 + 18);
		b[ 19] = *(a09 + 19);
		b[ 20] = *(a09 + 20);
		b[ 21] = *(a09 + 21);
		b[ 22] = *(a09 + 22);
		b[ 23] = *(a09 + 23);
		b[ 24] = *(a09 + 24);
		b[ 25] = *(a09 + 25);
		b[ 26] = *(a09 + 26);
		b[ 27] = *(a09 + 27);
		b[ 28] = *(a09 + 28);
		b[ 29] = *(a09 + 29);
		b[ 30] = *(a09 + 30);
		b[ 31] = *(a09 + 31);
	      b += 32;
	    }

		if (i >= 10) {
	      b[  0] = ZERO;
	      b[  1] = ZERO;
	      b[  2] = ZERO;
	      b[  3] = ZERO;
	      b[  4] = ZERO;
	      b[  5] = ZERO;
	      b[  6] = ZERO;
	      b[  7] = ZERO;
	      b[  8] = ZERO;
	      b[  9] = ZERO;
	      b[ 10] = ZERO;
	      b[ 11] = ZERO;
	      b[ 12] = ZERO;
	      b[ 13] = ZERO;
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
	    b[ 20] = *(a10 + 20);
		b[ 21] = *(a10 + 21);
		b[ 22] = *(a10 + 22);
		b[ 23] = *(a10 + 23);
		b[ 24] = *(a10 + 24);
		b[ 25] = *(a10 + 25);
		b[ 26] = *(a10 + 26);
		b[ 27] = *(a10 + 27);
		b[ 28] = *(a10 + 28);
		b[ 29] = *(a10 + 29);
		b[ 30] = *(a10 + 30);
		b[ 31] = *(a10 + 31);
	      b += 32;
	    }

		if (i >= 11) {
	      b[  0] = ZERO;
	      b[  1] = ZERO;
	      b[  2] = ZERO;
	      b[  3] = ZERO;
	      b[  4] = ZERO;
	      b[  5] = ZERO;
	      b[  6] = ZERO;
	      b[  7] = ZERO;
	      b[  8] = ZERO;
	      b[  9] = ZERO;
	      b[ 10] = ZERO;
	      b[ 11] = ZERO;
	      b[ 12] = ZERO;
	      b[ 13] = ZERO;
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
	    b[ 22] = *(a11 + 22);
		b[ 23] = *(a11 + 23);
		b[ 24] = *(a11 + 24);
		b[ 25] = *(a11 + 25);
		b[ 26] = *(a11 + 26);
		b[ 27] = *(a11 + 27);
		b[ 28] = *(a11 + 28);
		b[ 29] = *(a11 + 29);
		b[ 30] = *(a11 + 30);
		b[ 31] = *(a11 + 31);
	      b += 32;
	    }

		if (i >= 12) {
	      b[  0] = ZERO;
	      b[  1] = ZERO;
	      b[  2] = ZERO;
	      b[  3] = ZERO;
	      b[  4] = ZERO;
	      b[  5] = ZERO;
	      b[  6] = ZERO;
	      b[  7] = ZERO;
	      b[  8] = ZERO;
	      b[  9] = ZERO;
	      b[ 10] = ZERO;
	      b[ 11] = ZERO;
	      b[ 12] = ZERO;
	      b[ 13] = ZERO;
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
	    b[ 24] = *(a12 + 24);
		b[ 25] = *(a12 + 25);
		b[ 26] = *(a12 + 26);
		b[ 27] = *(a12 + 27);
		b[ 28] = *(a12 + 28);
		b[ 29] = *(a12 + 29);
		b[ 30] = *(a12 + 30);
		b[ 31] = *(a12 + 31);
	      b += 32;
	    }

		if (i >= 13) {
	      b[  0] = ZERO;
	      b[  1] = ZERO;
	      b[  2] = ZERO;
	      b[  3] = ZERO;
	      b[  4] = ZERO;
	      b[  5] = ZERO;
	      b[  6] = ZERO;
	      b[  7] = ZERO;
	      b[  8] = ZERO;
	      b[  9] = ZERO;
	      b[ 10] = ZERO;
	      b[ 11] = ZERO;
	      b[ 12] = ZERO;
	      b[ 13] = ZERO;
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
	    b[ 26] = *(a13 + 26);
		b[ 27] = *(a13 + 27);
		b[ 28] = *(a13 + 28);
		b[ 29] = *(a12 + 29);
		b[ 30] = *(a13 + 30);
		b[ 31] = *(a13 + 31);
	      b += 32;
	    }

		if (i >= 14) {
	      b[  0] = ZERO;
	      b[  1] = ZERO;
	      b[  2] = ZERO;
	      b[  3] = ZERO;
	      b[  4] = ZERO;
	      b[  5] = ZERO;
	      b[  6] = ZERO;
	      b[  7] = ZERO;
	      b[  8] = ZERO;
	      b[  9] = ZERO;
	      b[ 10] = ZERO;
	      b[ 11] = ZERO;
	      b[ 12] = ZERO;
	      b[ 13] = ZERO;
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
	    b[ 28] = *(a14 + 28);
		b[ 29] = *(a14 + 29);
		b[ 30] = *(a14 + 30);
		b[ 31] = *(a14 + 31);
	      b += 32;
	    }

		if (i >= 15) {
	      b[  0] = ZERO;
	      b[  1] = ZERO;
	      b[  2] = ZERO;
	      b[  3] = ZERO;
	      b[  4] = ZERO;
	      b[  5] = ZERO;
	      b[  6] = ZERO;
	      b[  7] = ZERO;
	      b[  8] = ZERO;
	      b[  9] = ZERO;
	      b[ 10] = ZERO;
	      b[ 11] = ZERO;
	      b[ 12] = ZERO;
	      b[ 13] = ZERO;
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
	      b[ 30] = *(a15 + 30);
		  b[ 31] = *(a15 + 31);
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
	a01 = a + posY * 2 + (posX +  0) * lda;
	a02 = a + posY * 2 + (posX +  1) * lda;
	a03 = a + posY * 2 + (posX +  2) * lda;
	a04 = a + posY * 2 + (posX +  3) * lda;
	a05 = a + posY * 2 + (posX +  4) * lda;
	a06 = a + posY * 2 + (posX +  5) * lda;
	a07 = a + posY * 2 + (posX +  6) * lda;
	a08 = a + posY * 2 + (posX +  7) * lda;
      } else {
	a01 = a + posX * 2 + (posY +  0) * lda;
	a02 = a + posX * 2 + (posY +  1) * lda;
	a03 = a + posX * 2 + (posY +  2) * lda;
	a04 = a + posX * 2 + (posY +  3) * lda;
	a05 = a + posX * 2 + (posY +  4) * lda;
	a06 = a + posX * 2 + (posY +  5) * lda;
	a07 = a + posX * 2 + (posY +  6) * lda;
	a08 = a + posX * 2 + (posY +  7) * lda;
      }

      i = (m >> 3);
      if (i > 0) {
	do {
	  if (X > posY) {
	    a01 += 16;
	    a02 += 16;
	    a03 += 16;
	    a04 += 16;
	    a05 += 16;
	    a06 += 16;
	    a07 += 16;
	    a08 += 16;
	    b += 128;
	  } else
	    if (X < posY) {
	      for (ii = 0; ii < 8; ii++){

		b[  0] = *(a01 +  0);
		b[  1] = *(a01 +  1);
		b[  2] = *(a01 +  2);
		b[  3] = *(a01 +  3);
		b[  4] = *(a01 +  4);
		b[  5] = *(a01 +  5);
		b[  6] = *(a01 +  6);
		b[  7] = *(a01 +  7);

		b[  8] = *(a01 +  8);
		b[  9] = *(a01 +  9);
		b[ 10] = *(a01 + 10);
		b[ 11] = *(a01 + 11);
		b[ 12] = *(a01 + 12);
		b[ 13] = *(a01 + 13);
		b[ 14] = *(a01 + 14);
		b[ 15] = *(a01 + 15);

		a01 += lda;
		b += 16;
	      }
		  a02 += 8 * lda;
	      a03 += 8 * lda;
	      a04 += 8 * lda;
	      a05 += 8 * lda;
	      a06 += 8 * lda;
	      a07 += 8 * lda;
	      a08 += 8 * lda;
	    } else {
#ifdef UNIT
	      b[  0] = ONE;
	      b[  1] = ZERO;
#else
	      b[  0] = *(a01 +  0);
	      b[  1] = *(a01 +  1);
#endif
	      b[  2] = *(a01 +  2);
	      b[  3] = *(a01 +  3);
	      b[  4] = *(a01 +  4);
	      b[  5] = *(a01 +  5);
	      b[  6] = *(a01 +  6);
	      b[  7] = *(a01 +  7);
	      b[  8] = *(a01 +  8);
	      b[  9] = *(a01 +  9);
	      b[ 10] = *(a01 + 10);
	      b[ 11] = *(a01 + 11);
	      b[ 12] = *(a01 + 12);
	      b[ 13] = *(a01 + 13);
	      b[ 14] = *(a01 + 14);
	      b[ 15] = *(a01 + 15);

	      b[ 16] = ZERO;
	      b[ 17] = ZERO;
#ifdef UNIT
	      b[ 18] = ONE;
	      b[ 19] = ZERO;
#else
	      b[ 18] = *(a02 +  2);
	      b[ 19] = *(a02 +  3);
#endif
	      b[ 20] = *(a02 +  4);
	      b[ 21] = *(a02 +  5);
	      b[ 22] = *(a02 +  6);
	      b[ 23] = *(a02 +  7);
	      b[ 24] = *(a02 +  8);
	      b[ 25] = *(a02 +  9);
	      b[ 26] = *(a02 + 10);
	      b[ 27] = *(a02 + 11);
	      b[ 28] = *(a02 + 12);
	      b[ 29] = *(a02 + 13);
	      b[ 30] = *(a02 + 14);
	      b[ 31] = *(a02 + 15);

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
	      b[ 38] = *(a03 +  6);
	      b[ 39] = *(a03 +  7);
	      b[ 40] = *(a03 +  8);
	      b[ 41] = *(a03 +  9);
	      b[ 42] = *(a03 + 10);
	      b[ 43] = *(a03 + 11);
	      b[ 44] = *(a03 + 12);
	      b[ 45] = *(a03 + 13);
	      b[ 46] = *(a03 + 14);
	      b[ 47] = *(a03 + 15);

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
	      b[ 56] = *(a04 +  8);
	      b[ 57] = *(a04 +  9);
	      b[ 58] = *(a04 + 10);
	      b[ 59] = *(a04 + 11);
	      b[ 60] = *(a04 + 12);
	      b[ 61] = *(a04 + 13);
	      b[ 62] = *(a04 + 14);
	      b[ 63] = *(a04 + 15);

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
	      b[ 74] = *(a05 + 10);
	      b[ 75] = *(a05 + 11);
	      b[ 76] = *(a05 + 12);
	      b[ 77] = *(a05 + 13);
	      b[ 78] = *(a05 + 14);
	      b[ 79] = *(a05 + 15);

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
	      b[ 92] = *(a06 + 12);
	      b[ 93] = *(a06 + 13);
	      b[ 94] = *(a06 + 14);
	      b[ 95] = *(a06 + 15);

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
	      b[110] = *(a07 + 14);
	      b[111] = *(a07 + 15);

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

	      a01 += 16;
	      a02 += 16;
	      a03 += 16;
	      a04 += 16;
	      a05 += 16;
	      a06 += 16;
	      a07 += 16;
	      a08 += 16;
	      b += 128;
	    }

	  X += 8;
	  i --;
	} while (i > 0);
      }

      i = (m & 7);
      if (i) {

	if (X > posY) {
	  /* a01 += 2 * i;
	  a02 += 2 * i;
	  a03 += 2 * i;
	  a04 += 2 * i;
	  a05 += 2 * i;
	  a06 += 2 * i;
	  a07 += 2 * i;
	  a08 += 2 * i; */
	  b += 16 * i;
	} else
	  if (X < posY) {
	      for (ii = 0; ii < i; ii++){
	      b[  0] = *(a01 +  0);
	      b[  1] = *(a01 +  1);
	      b[  2] = *(a01 +  2);
	      b[  3] = *(a01 +  3);
	      b[  4] = *(a01 +  4);
	      b[  5] = *(a01 +  5);
	      b[  6] = *(a01 +  6);
	      b[  7] = *(a01 +  7);

	      b[  8] = *(a01 +  8);
	      b[  9] = *(a01 +  9);
	      b[ 10] = *(a01 + 10);
	      b[ 11] = *(a01 + 11);
	      b[ 12] = *(a01 + 12);
	      b[ 13] = *(a01 + 13);
	      b[ 14] = *(a01 + 14);
	      b[ 15] = *(a01 + 15);

	      a01 += lda;
	      a02 += lda;
	      a03 += lda;
	      a04 += lda;
	      a05 += lda;
	      a06 += lda;
	      a07 += lda;
	      a08 += lda;
	      b += 16;
	    }
	  } else {
#ifdef UNIT
	    b[  0] = ONE;
	    b[  1] = ZERO;
#else
	    b[  0] = *(a01 +  0);
	    b[  1] = *(a01 +  1);
#endif
	    b[  2] = *(a01 +  2);
	    b[  3] = *(a01 +  3);
	    b[  4] = *(a01 +  4);
	    b[  5] = *(a01 +  5);
	    b[  6] = *(a01 +  6);
	    b[  7] = *(a01 +  7);

	    b[  8] = *(a01 +  8);
	    b[  9] = *(a01 +  9);
	    b[ 10] = *(a01 + 10);
	    b[ 11] = *(a01 + 11);
	    b[ 12] = *(a01 + 12);
	    b[ 13] = *(a01 + 13);
	    b[ 14] = *(a01 + 14);
	    b[ 15] = *(a01 + 15);
	    b += 16;

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
	      b[ 4] = *(a02 +  4);
	      b[ 5] = *(a02 +  5);
	      b[ 6] = *(a02 +  6);
	      b[ 7] = *(a02 +  7);

	      b[ 8] = *(a02 +  8);
	      b[ 9] = *(a02 +  9);
	      b[10] = *(a02 + 10);
	      b[11] = *(a02 + 11);
	      b[12] = *(a02 + 12);
	      b[13] = *(a02 + 13);
	      b[14] = *(a02 + 14);
	      b[15] = *(a02 + 15);
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
	      b[ 6] = *(a03 +  6);
	      b[ 7] = *(a03 +  7);

	      b[ 8] = *(a03 +  8);
	      b[ 9] = *(a03 +  9);
	      b[10] = *(a03 + 10);
	      b[11] = *(a03 + 11);
	      b[12] = *(a03 + 12);
	      b[13] = *(a03 + 13);
	      b[14] = *(a03 + 14);
	      b[15] = *(a03 + 15);
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

	      b[ 8] = *(a04 +  8);
	      b[ 9] = *(a04 +  9);
	      b[10] = *(a04 + 10);
	      b[11] = *(a04 + 11);
	      b[12] = *(a04 + 12);
	      b[13] = *(a04 + 13);
	      b[14] = *(a04 + 14);
	      b[15] = *(a04 + 15);
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
	      b[10] = *(a05 + 10);
	      b[11] = *(a05 + 11);
	      b[12] = *(a05 + 12);
	      b[13] = *(a05 + 13);
	      b[14] = *(a05 + 14);
	      b[15] = *(a05 + 15);
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
	      b[12] = *(a06 + 12);
	      b[13] = *(a06 + 13);
	      b[14] = *(a06 + 14);
	      b[15] = *(a06 + 15);
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
	      b[14] = *(a07 + 14);
	      b[15] = *(a07 + 15);
	      b += 16;
	    }
	  }
      }

      posY += 8;
  }


  if (n & 4){
    X = posX;

    if (posX <= posY) {
      a01 = a + posY * 2 + (posX +  0) * lda;
      a02 = a + posY * 2 + (posX +  1) * lda;
      a03 = a + posY * 2 + (posX +  2) * lda;
      a04 = a + posY * 2 + (posX +  3) * lda;
    } else {
      a01 = a + posX * 2 + (posY +  0) * lda;
      a02 = a + posX * 2 + (posY +  1) * lda;
      a03 = a + posX * 2 + (posY +  2) * lda;
      a04 = a + posX * 2 + (posY +  3) * lda;
    }

    i = (m >> 2);
    if (i > 0) {
      do {
	if (X > posY) {
	  a01 += 8;
	  a02 += 8;
	  a03 += 8;
	  a04 += 8;
	  b += 32;
	} else
	  if (X < posY) {
	    for (ii = 0; ii < 4; ii++){
	      b[  0] = *(a01 +  0);
	      b[  1] = *(a01 +  1);
	      b[  2] = *(a01 +  2);
	      b[  3] = *(a01 +  3);
	      b[  4] = *(a01 +  4);
	      b[  5] = *(a01 +  5);
	      b[  6] = *(a01 +  6);
	      b[  7] = *(a01 +  7);

	      a01 += lda;
	      b += 8;
	      }

	    a02 += 4 * lda;
	    a03 += 4 * lda;
	    a04 += 4 * lda;
	    } else {
#ifdef UNIT
	      b[  0] = ONE;
	      b[  1] = ZERO;
#else
	      b[  0] = *(a01 +  0);
	      b[  1] = *(a01 +  1);
#endif
	      b[  2] = *(a01 +  2);
	      b[  3] = *(a01 +  3);
	      b[  4] = *(a01 +  4);
	      b[  5] = *(a01 +  5);
	      b[  6] = *(a01 +  6);
	      b[  7] = *(a01 +  7);

	      b[  8] = ZERO;
	      b[  9] = ZERO;
#ifdef UNIT
	      b[ 10] = ONE;
	      b[ 11] = ZERO;
#else
	      b[ 10] = *(a02 +  2);
	      b[ 11] = *(a02 +  3);
#endif
	      b[ 12] = *(a02 +  4);
	      b[ 13] = *(a02 +  5);
	      b[ 14] = *(a02 +  6);
	      b[ 15] = *(a02 +  7);

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
	      b[ 22] = *(a03 +  6);
	      b[ 23] = *(a03 +  7);

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

	      a01 += 8;
	      a02 += 8;
	      a03 += 8;
	      a04 += 8;
	      b += 32;
	    }

	X += 4;
	i --;
      } while (i > 0);
    }

    i = (m & 3);
    if (i > 0) {
      if (X > posY) {
	/* a01 += 2 * i;
	a02 += 2 * i;
	a03 += 2 * i;
	a04 += 2 * i; */
	b += 8 * i;
      } else
	if (X < posY) {
	  for (ii = 0; ii < i; ii++){
	    b[  0] = *(a01 +  0);
	    b[  1] = *(a01 +  1);
	    b[  2] = *(a01 +  2);
	    b[  3] = *(a01 +  3);
	    b[  4] = *(a01 +  4);
	    b[  5] = *(a01 +  5);
	    b[  6] = *(a01 +  6);
	    b[  7] = *(a01 +  7);

	    a01 += lda;
	    a02 += lda;
	    a03 += lda;
	    a04 += lda;
	    b += 8;
	  }
	} else {
#ifdef UNIT
	  b[  0] = ONE;
	  b[  1] = ZERO;
#else
	  b[  0] = *(a01 +  0);
	  b[  1] = *(a01 +  1);
#endif
	  b[  2] = *(a01 +  2);
	  b[  3] = *(a01 +  3);
	  b[  4] = *(a01 +  4);
	  b[  5] = *(a01 +  5);
	  b[  6] = *(a01 +  6);
	  b[  7] = *(a01 +  7);
	  b += 8;

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
	    b[ 4] = *(a02 +  4);
	    b[ 5] = *(a02 +  5);
	    b[ 6] = *(a02 +  6);
	    b[ 7] = *(a02 +  7);
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
	    b[ 6] = *(a03 +  6);
	    b[ 7] = *(a03 +  7);
	    b += 8;
	  }
	}
    }
    posY += 4;
  }

  if (n & 2){
    X = posX;

    if (posX <= posY) {
      a01 = a + posY * 2 + (posX +  0) * lda;
      a02 = a + posY * 2 + (posX +  1) * lda;
    } else {
      a01 = a + posX * 2 + (posY +  0) * lda;
      a02 = a + posX * 2 + (posY +  1) * lda;
    }

    i = (m >> 1);
    if (i > 0) {
      do {
	if (X > posY) {
	  a01 += 4;
	  a02 += 4;
	  b += 8;
	} else
	  if (X < posY) {
	    b[0] = *(a01 +  0);
	    b[1] = *(a01 +  1);
	    b[2] = *(a01 +  2);
	    b[3] = *(a01 +  3);
	    b[4] = *(a02 +  0);
	    b[5] = *(a02 +  1);
	    b[6] = *(a02 +  2);
	    b[7] = *(a02 +  3);
	    a01 += 2 * lda;
	    a02 += 2 * lda;
	    b += 8;
	  } else {
#ifdef UNIT
	    b[0] = ONE;
	    b[1] = ZERO;
#else
	    b[0] = *(a01 +  0);
	    b[1] = *(a01 +  1);
#endif
	    b[2] = *(a01 +  2);
	    b[3] = *(a01 +  3);

	    b[4] = ZERO;
	    b[5] = ZERO;
#ifdef UNIT
	    b[6] = ONE;
	    b[7] = ZERO;
#else
	    b[6] = *(a02 +  2);
	    b[7] = *(a02 +  3);
#endif
	    a01 += 4;
	    a02 += 4;
	    b += 8;
	  }

	X += 2;
	i --;
      } while (i > 0);
    }

    i = (m & 1);
    if (i > 0) {
      if (X > posY) {
	/* a01 += 2;
	a02 += 2; */
	b += 4;
      } else
	if (X < posY) {
	  b[  0] = *(a01 +  0);
	  b[  1] = *(a01 +  1);
	  b[  2] = *(a01 +  2);
	  b[  3] = *(a01 +  3);

	  /* a01 += lda;
	  a02 += lda; */
	  b += 4;
	} else {
#ifdef UNIT
	  b[  0] = ONE;
	  b[  1] = ZERO;
#else
	  b[  0] = *(a01 +  0);
	  b[  1] = *(a01 +  1);
#endif
	  b[  2] = *(a01 +  2);
	  b[  3] = *(a01 +  3);
	  b += 4;
	}
    }
    posY += 2;
  }

  if (n & 1){
    X = posX;

    if (posX <= posY) {
      a01 = a + posY * 2 + (posX +  0) * lda;
    } else {
      a01 = a + posX * 2 + (posY +  0) * lda;
    }

    i = m;
    if (i > 0) {
      do {

	if (X > posY) {
	  a01 += 2;
	  b += 2;
	} else
	  if (X < posY) {
	    b[0] = *(a01 + 0);
	    b[1] = *(a01 + 1);
	    a01 += lda;
	    b += 2;
	  } else {
#ifdef UNIT
	    b[0] = ONE;
	    b[1] = ZERO;
#else
	    b[0] = *(a01 + 0);
	    b[1] = *(a01 + 1);
#endif
	    a01 += 2;
	    b += 2;
	  }

	X += 1;
	i --;
      } while (i > 0);
    }
    // posY += 1;
  }

  return 0;
}
