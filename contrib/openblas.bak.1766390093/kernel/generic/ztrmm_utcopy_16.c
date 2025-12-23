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
	    if (X > posY) {
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
	      b[ 28] = ZERO;
	      b[ 29] = ZERO;
		  b[ 30] = ZERO;
	      b[ 31] = ZERO;

	      b[ 32] = *(a02 +  0);
	      b[ 33] = *(a02 +  1);
#ifdef UNIT
	      b[ 34] = ONE;
	      b[ 35] = ZERO;
#else
	      b[ 34] = *(a02 +  2);
	      b[ 35] = *(a02 +  3);
#endif
	      b[ 36] = ZERO;
	      b[ 37] = ZERO;
	      b[ 38] = ZERO;
	      b[ 39] = ZERO;
	      b[ 40] = ZERO;
	      b[ 41] = ZERO;
	      b[ 42] = ZERO;
	      b[ 43] = ZERO;
	      b[ 44] = ZERO;
	      b[ 45] = ZERO;
	      b[ 46] = ZERO;
	      b[ 47] = ZERO;
	      b[ 48] = ZERO;
	      b[ 49] = ZERO;
	      b[ 50] = ZERO;
	      b[ 51] = ZERO;
	      b[ 52] = ZERO;
	      b[ 53] = ZERO;
	      b[ 54] = ZERO;
	      b[ 55] = ZERO;
	      b[ 56] = ZERO;
	      b[ 57] = ZERO;
	      b[ 58] = ZERO;
	      b[ 59] = ZERO;
	      b[ 60] = ZERO;
	      b[ 61] = ZERO;
	      b[ 62] = ZERO;
	      b[ 63] = ZERO;

	      b[ 64] = *(a03 +  0);
	      b[ 65] = *(a03 +  1);
	      b[ 66] = *(a03 +  2);
	      b[ 67] = *(a03 +  3);
#ifdef UNIT
	      b[ 68] = ONE;
	      b[ 69] = ZERO;
#else
	      b[ 68] = *(a03 +  4);
	      b[ 69] = *(a03 +  5);
#endif
	      b[ 70] = ZERO;
	      b[ 71] = ZERO;
	      b[ 72] = ZERO;
	      b[ 73] = ZERO;
	      b[ 74] = ZERO;
	      b[ 75] = ZERO;
	      b[ 76] = ZERO;
	      b[ 77] = ZERO;
	      b[ 78] = ZERO;
	      b[ 79] = ZERO;
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
	      b[ 90] = ZERO;
	      b[ 91] = ZERO;
	      b[ 92] = ZERO;
	      b[ 93] = ZERO;
	      b[ 94] = ZERO;
	      b[ 95] = ZERO;

	      b[ 96] = *(a04 +  0);
	      b[ 97] = *(a04 +  1);
	      b[ 98] = *(a04 +  2);
	      b[ 99] = *(a04 +  3);
	      b[100] = *(a04 +  4);
	      b[101] = *(a04 +  5);
#ifdef UNIT
	      b[102] = ONE;
	      b[103] = ZERO;
#else
	      b[102] = *(a04 +  6);
	      b[103] = *(a04 +  7);
#endif
	      b[104] = ZERO;
	      b[105] = ZERO;
	      b[106] = ZERO;
	      b[107] = ZERO;
	      b[108] = ZERO;
	      b[109] = ZERO;
	      b[110] = ZERO;
	      b[111] = ZERO;
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
	      b[126] = ZERO;
	      b[127] = ZERO;

	      b[128] = *(a05 +  0);
	      b[129] = *(a05 +  1);
	      b[130] = *(a05 +  2);
	      b[131] = *(a05 +  3);
	      b[132] = *(a05 +  4);
	      b[133] = *(a05 +  5);
	      b[134] = *(a05 +  6);
	      b[135] = *(a05 +  7);
#ifdef UNIT
	      b[136] = ONE;
	      b[137] = ZERO;
#else
	      b[136] = *(a05 +  8);
	      b[137] = *(a05 +  9);
#endif
	      b[138] = ZERO;
	      b[139] = ZERO;
	      b[140] = ZERO;
	      b[141] = ZERO;
	      b[142] = ZERO;
	      b[143] = ZERO;
	      b[144] = ZERO;
	      b[145] = ZERO;
	      b[146] = ZERO;
	      b[147] = ZERO;
	      b[148] = ZERO;
		  b[149] = ZERO;
	      b[150] = ZERO;
	      b[151] = ZERO;
	      b[152] = ZERO;
	      b[153] = ZERO;
	      b[154] = ZERO;
	      b[155] = ZERO;
	      b[156] = ZERO;
	      b[157] = ZERO;
	      b[158] = ZERO;
	      b[159] = ZERO;

	      b[160] = *(a06 +  0);
	      b[161] = *(a06 +  1);
	      b[162] = *(a06 +  2);
	      b[163] = *(a06 +  3);
	      b[164] = *(a06 +  4);
	      b[165] = *(a06 +  5);
	      b[166] = *(a06 +  6);
	      b[167] = *(a06 +  7);
	      b[168] = *(a06 +  8);
	      b[169] = *(a06 +  9);
#ifdef UNIT
	      b[170] = ONE;
	      b[171] = ZERO;
#else
	      b[170] = *(a06 + 10);
	      b[171] = *(a06 + 11);
#endif
	      b[172] = ZERO;
	      b[173] = ZERO;
	      b[174] = ZERO;
	      b[175] = ZERO;
	      b[176] = ZERO;
	      b[177] = ZERO;
	      b[178] = ZERO;
	      b[179] = ZERO;
	      b[180] = ZERO;
	      b[181] = ZERO;
		  b[182] = ZERO;
	      b[183] = ZERO;
	      b[184] = ZERO;
	      b[185] = ZERO;
	      b[186] = ZERO;
	      b[187] = ZERO;
	      b[188] = ZERO;
	      b[189] = ZERO;
	      b[190] = ZERO;
	      b[191] = ZERO;

	      b[192] = *(a07 + 0);
	      b[193] = *(a07 + 1);
	      b[194] = *(a07 + 2);
	      b[195] = *(a07 + 3);
	      b[196] = *(a07 + 4);
	      b[197] = *(a07 + 5);
	      b[198] = *(a07 + 6);
	      b[199] = *(a07 + 7);
	      b[200] = *(a07 + 8);
	      b[201] = *(a07 + 9);
	      b[202] = *(a07 + 10);
	      b[203] = *(a07 + 11);
#ifdef UNIT
	      b[204] = ONE;
	      b[205] = ZERO;
#else
	      b[204] = *(a07 + 12);
	      b[205] = *(a07 + 13);
#endif
	      b[206] = ZERO;
	      b[207] = ZERO;
	      b[208] = ZERO;
	      b[209] = ZERO;
	      b[210] = ZERO;
	      b[211] = ZERO;
	      b[212] = ZERO;
	      b[213] = ZERO;
	      b[214] = ZERO;
		  b[215] = ZERO;
	      b[216] = ZERO;
	      b[217] = ZERO;
	      b[218] = ZERO;
	      b[219] = ZERO;
	      b[220] = ZERO;
	      b[221] = ZERO;
	      b[222] = ZERO;
	      b[223] = ZERO;

	      b[224] = *(a08 + 0);
	      b[225] = *(a08 + 1);
	      b[226] = *(a08 + 2);
	      b[227] = *(a08 + 3);
	      b[228] = *(a08 + 4);
	      b[229] = *(a08 + 5);
	      b[230] = *(a08 + 6);
	      b[231] = *(a08 + 7);
	      b[232] = *(a08 + 8);
	      b[233] = *(a08 + 9);
	      b[234] = *(a08 + 10);
	      b[235] = *(a08 + 11);
	      b[236] = *(a08 + 12);
	      b[237] = *(a08 + 13);
#ifdef UNIT
	      b[238] = ONE;
	      b[239] = ZERO;
#else
	      b[238] = *(a08 + 14);
	      b[239] = *(a08 + 15);
#endif
		  b[240] = ZERO;
	      b[241] = ZERO;
	      b[242] = ZERO;
	      b[243] = ZERO;
	      b[244] = ZERO;
	      b[245] = ZERO;
	      b[246] = ZERO;
	      b[247] = ZERO;
		  b[248] = ZERO;
	      b[249] = ZERO;
	      b[250] = ZERO;
	      b[251] = ZERO;
	      b[252] = ZERO;
	      b[253] = ZERO;
	      b[254] = ZERO;
	      b[255] = ZERO;

	      b[256] = *(a09 + 0);
	      b[257] = *(a09 + 1);
	      b[258] = *(a09 + 2);
	      b[259] = *(a09 + 3);
	      b[260] = *(a09 + 4);
	      b[261] = *(a09 + 5);
	      b[262] = *(a09 + 6);
	      b[263] = *(a09 + 7);
	      b[264] = *(a09 + 8);
	      b[265] = *(a09 + 9);
	      b[266] = *(a09 + 10);
	      b[267] = *(a09 + 11);
	      b[268] = *(a09 + 12);
	      b[269] = *(a09 + 13);
	      b[270] = *(a09 + 14);
	      b[271] = *(a09 + 15);
#ifdef UNIT
	      b[272] = ONE;
		  b[273] = ZERO;
#else
	      b[272] = *(a09 + 16);
		  b[273] = *(a09 + 17);
#endif
		  b[274] = ZERO;
	      b[275] = ZERO;
	      b[276] = ZERO;
	      b[277] = ZERO;
	      b[278] = ZERO;
	      b[279] = ZERO;
	      b[280] = ZERO;
		  b[281] = ZERO;
	      b[282] = ZERO;
	      b[283] = ZERO;
	      b[284] = ZERO;
	      b[285] = ZERO;
	      b[286] = ZERO;
	      b[287] = ZERO;

		  b[288] = *(a10 + 0);
		  b[289] = *(a10 + 1);
	      b[290] = *(a10 + 2);
	      b[291] = *(a10 + 3);
	      b[292] = *(a10 + 4);
	      b[293] = *(a10 + 5);
	      b[294] = *(a10 + 6);
	      b[295] = *(a10 + 7);
	      b[296] = *(a10 + 8);
	      b[297] = *(a10 + 9);
	      b[298] = *(a10 + 10);
	      b[299] = *(a10 + 11);
	      b[300] = *(a10 + 12);
	      b[301] = *(a10 + 13);
	      b[302] = *(a10 + 14);
	      b[303] = *(a10 + 15);
	      b[304] = *(a10 + 16);
	      b[305] = *(a10 + 17);
#ifdef UNIT
	      b[306] = ONE;
		  b[307] = ZERO;
#else
	      b[306] = *(a10 + 18);
		  b[307] = *(a10 + 19);
#endif
		  b[308] = ZERO;
	      b[309] = ZERO;
	      b[310] = ZERO;
	      b[311] = ZERO;
	      b[312] = ZERO;
	      b[313] = ZERO;
		  b[314] = ZERO;
	      b[315] = ZERO;
	      b[316] = ZERO;
	      b[317] = ZERO;
	      b[318] = ZERO;
	      b[319] = ZERO;

		  b[320] = *(a11 + 0);
		  b[321] = *(a11 + 1);
	      b[322] = *(a11 + 2);
	      b[323] = *(a11 + 3);
	      b[324] = *(a11 + 4);
	      b[325] = *(a11 + 5);
	      b[326] = *(a11 + 6);
	      b[327] = *(a11 + 7);
	      b[328] = *(a11 + 8);
	      b[329] = *(a11 + 9);
	      b[330] = *(a11 + 10);
	      b[331] = *(a11 + 11);
	      b[332] = *(a11 + 12);
	      b[333] = *(a11 + 13);
	      b[334] = *(a11 + 14);
	      b[335] = *(a11 + 15);
	      b[336] = *(a11 + 16);
	      b[337] = *(a11 + 17);
	      b[338] = *(a11 + 18);
	      b[339] = *(a11 + 19);
#ifdef UNIT
	      b[340] = ONE;
	      b[341] = ZERO;
#else
	      b[340] = *(a11 + 20);
	      b[341] = *(a11 + 21);
#endif
		  b[342] = ZERO;
	      b[343] = ZERO;
	      b[344] = ZERO;
	      b[345] = ZERO;
	      b[346] = ZERO;
		  b[347] = ZERO;
	      b[348] = ZERO;
	      b[349] = ZERO;
	      b[350] = ZERO;
	      b[351] = ZERO;

		  b[352] = *(a12 + 0);
		  b[353] = *(a12 + 1);
	      b[354] = *(a12 + 2);
	      b[355] = *(a12 + 3);
	      b[356] = *(a12 + 4);
	      b[357] = *(a12 + 5);
	      b[358] = *(a12 + 6);
	      b[359] = *(a12 + 7);
	      b[360] = *(a12 + 8);
	      b[361] = *(a12 + 9);
	      b[362] = *(a12 + 10);
	      b[363] = *(a12 + 11);
	      b[364] = *(a12 + 12);
	      b[365] = *(a12 + 13);
	      b[366] = *(a12 + 14);
	      b[367] = *(a12 + 15);
	      b[368] = *(a12 + 16);
	      b[369] = *(a12 + 17);
	      b[370] = *(a12 + 18);
	      b[371] = *(a12 + 19);
	      b[372] = *(a12 + 20);
	      b[373] = *(a12 + 21);
#ifdef UNIT
	      b[374] = ONE;
	      b[375] = ZERO;
#else
	      b[374] = *(a12 + 22);
	      b[375] = *(a12 + 23);
#endif
		  b[376] = ZERO;
	      b[377] = ZERO;
	      b[378] = ZERO;
	      b[379] = ZERO;
		  b[380] = ZERO;
	      b[381] = ZERO;
	      b[382] = ZERO;
	      b[383] = ZERO;

		  b[384] = *(a13 + 0);
		  b[385] = *(a13 + 1);
	      b[386] = *(a13 + 2);
	      b[387] = *(a13 + 3);
	      b[388] = *(a13 + 4);
	      b[389] = *(a13 + 5);
	      b[390] = *(a13 + 6);
	      b[391] = *(a13 + 7);
	      b[392] = *(a13 + 8);
	      b[393] = *(a13 + 9);
	      b[394] = *(a13 + 10);
	      b[395] = *(a13 + 11);
	      b[396] = *(a13 + 12);
	      b[397] = *(a13 + 13);
	      b[398] = *(a13 + 14);
	      b[399] = *(a13 + 15);
	      b[400] = *(a13 + 16);
	      b[401] = *(a13 + 17);
	      b[402] = *(a13 + 18);
	      b[403] = *(a13 + 19);
	      b[404] = *(a13 + 20);
	      b[405] = *(a13 + 21);
	      b[406] = *(a13 + 22);
	      b[407] = *(a13 + 23);
#ifdef UNIT
	      b[408] = ONE;
	      b[409] = ZERO;
#else
	      b[408] = *(a13 + 24);
	      b[409] = *(a13 + 25);
#endif
		  b[410] = ZERO;
	      b[411] = ZERO;
	      b[412] = ZERO;
		  b[413] = ZERO;
	      b[414] = ZERO;
	      b[415] = ZERO;

		  b[416] = *(a14 + 0);
		  b[417] = *(a14 + 1);
	      b[418] = *(a14 + 2);
	      b[419] = *(a14 + 3);
	      b[420] = *(a14 + 4);
	      b[421] = *(a14 + 5);
	      b[422] = *(a14 + 6);
	      b[423] = *(a14 + 7);
	      b[424] = *(a14 + 8);
	      b[425] = *(a14 + 9);
	      b[426] = *(a14 + 10);
	      b[427] = *(a14 + 11);
	      b[428] = *(a14 + 12);
	      b[429] = *(a14 + 13);
	      b[430] = *(a14 + 14);
	      b[431] = *(a14 + 15);
	      b[432] = *(a14 + 16);
	      b[433] = *(a14 + 17);
	      b[434] = *(a14 + 18);
	      b[435] = *(a14 + 19);
	      b[436] = *(a14 + 20);
	      b[437] = *(a14 + 21);
	      b[438] = *(a14 + 22);
	      b[439] = *(a14 + 23);
	      b[440] = *(a14 + 24);
	      b[441] = *(a14 + 25);
#ifdef UNIT
	      b[442] = ONE;
	      b[443] = ZERO;
#else
	      b[442] = *(a14 + 26);
	      b[443] = *(a14 + 27);
#endif
		  b[444] = ZERO;
	      b[445] = ZERO;
		  b[446] = ZERO;
	      b[447] = ZERO;

		  b[448] = *(a15 + 0);
		  b[449] = *(a15 + 1);
	      b[450] = *(a15 + 2);
	      b[451] = *(a15 + 3);
	      b[452] = *(a15 + 4);
	      b[453] = *(a15 + 5);
	      b[454] = *(a15 + 6);
	      b[455] = *(a15 + 7);
	      b[456] = *(a15 + 8);
	      b[457] = *(a15 + 9);
	      b[458] = *(a15 + 10);
	      b[459] = *(a15 + 11);
	      b[460] = *(a15 + 12);
	      b[461] = *(a15 + 13);
	      b[462] = *(a15 + 14);
	      b[463] = *(a15 + 15);
	      b[464] = *(a15 + 16);
	      b[465] = *(a15 + 17);
	      b[466] = *(a15 + 18);
	      b[467] = *(a15 + 19);
	      b[468] = *(a15 + 20);
	      b[469] = *(a15 + 21);
	      b[470] = *(a15 + 22);
	      b[471] = *(a15 + 23);
	      b[472] = *(a15 + 24);
	      b[473] = *(a15 + 25);
	      b[474] = *(a15 + 26);
	      b[475] = *(a15 + 27);
#ifdef UNIT
	      b[476] = ONE;
	      b[477] = ZERO;
#else
	      b[476] = *(a15 + 28);
	      b[477] = *(a15 + 29);
#endif
		  b[478] = ZERO;
		  b[479] = ZERO;

		  b[480] = *(a16 + 0);
		  b[481] = *(a16 + 1);
	      b[482] = *(a16 + 2);
	      b[483] = *(a16 + 3);
	      b[484] = *(a16 + 4);
	      b[485] = *(a16 + 5);
	      b[486] = *(a16 + 6);
	      b[487] = *(a16 + 7);
	      b[488] = *(a16 + 8);
	      b[489] = *(a16 + 9);
	      b[490] = *(a16 + 10);
	      b[491] = *(a16 + 11);
	      b[492] = *(a16 + 12);
	      b[493] = *(a16 + 13);
	      b[494] = *(a16 + 14);
	      b[495] = *(a16 + 15);
	      b[496] = *(a16 + 16);
	      b[497] = *(a16 + 17);
	      b[498] = *(a16 + 18);
	      b[499] = *(a16 + 19);
	      b[500] = *(a16 + 20);
	      b[501] = *(a16 + 21);
	      b[502] = *(a16 + 22);
	      b[503] = *(a16 + 23);
	      b[504] = *(a16 + 24);
	      b[505] = *(a16 + 25);
	      b[506] = *(a16 + 26);
	      b[507] = *(a16 + 27);
	      b[508] = *(a16 + 28);
	      b[509] = *(a16 + 29);
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
	//   a01 += 2 * i;
	//   a02 += 2 * i;
	//   a03 += 2 * i;
	//   a04 += 2 * i;
	//   a05 += 2 * i;
	//   a06 += 2 * i;
	//   a07 += 2 * i;
	//   a08 += 2 * i;
	//   a09 += 2 * i;
	//   a10 += 2 * i;
	//   a11 += 2 * i;
	//   a12 += 2 * i;
	//   a13 += 2 * i;
	//   a14 += 2 * i;
	//   a15 += 2 * i;
	//   a16 += 2 * i;
	  b += 32 * i;

	} else
	  if (X > posY) {
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
	      b[ 28] = ZERO;
	      b[ 29] = ZERO;
		  b[ 30] = ZERO;
	      b[ 31] = ZERO;
	      b += 32;

	      if (i >= 2) {
		b[ 0] = *(a02 +  0);
		b[ 1] = *(a02 +  1);
#ifdef UNIT
		b[ 2] = ONE;
		b[ 3] = ZERO;
#else
		b[ 2] = *(a02 +  2);
		b[ 3] = *(a02 +  3);
#endif
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
		b[ 28] = ZERO;
		b[ 29] = ZERO;
		b[ 30] = ZERO;
		b[ 31] = ZERO;
		b += 32;
	      }

	      if (i >= 3) {
		b[ 0] = *(a03 +  0);
		b[ 1] = *(a03 +  1);
		b[ 2] = *(a03 +  2);
		b[ 3] = *(a03 +  3);
#ifdef UNIT
		b[ 4] = ONE;
		b[ 5] = ZERO;
#else
		b[ 4] = *(a03 +  4);
		b[ 5] = *(a03 +  5);
#endif
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
		b[ 28] = ZERO;
		b[ 29] = ZERO;
		b[ 30] = ZERO;
		b[ 31] = ZERO;
		b += 32;
	      }

	      if (i >= 4) {
		b[ 0] = *(a04 +  0);
		b[ 1] = *(a04 +  1);
		b[ 2] = *(a04 +  2);
		b[ 3] = *(a04 +  3);
		b[ 4] = *(a04 +  4);
		b[ 5] = *(a04 +  5);
#ifdef UNIT
		b[ 6] = ONE;
		b[ 7] = ZERO;
#else
		b[ 6] = *(a04 +  6);
		b[ 7] = *(a04 +  7);
#endif
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
		b[ 28] = ZERO;
		b[ 29] = ZERO;
		b[ 30] = ZERO;
		b[ 31] = ZERO;
		b += 32;
	      }

	      if (i >= 5) {
		b[ 0] = *(a05 +  0);
		b[ 1] = *(a05 +  1);
		b[ 2] = *(a05 +  2);
		b[ 3] = *(a05 +  3);
		b[ 4] = *(a05 +  4);
		b[ 5] = *(a05 +  5);
		b[ 6] = *(a05 +  6);
		b[ 7] = *(a05 +  7);
#ifdef UNIT
		b[ 8] = ONE;
		b[ 9] = ZERO;
#else
		b[ 8] = *(a05 +  8);
		b[ 9] = *(a05 +  9);
#endif
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
		b[ 28] = ZERO;
		b[ 29] = ZERO;
		b[ 30] = ZERO;
		b[ 31] = ZERO;
		b += 32;
	      }

	      if (i >= 6) {
		b[ 0] = *(a06 + 0);
		b[ 1] = *(a06 + 1);
		b[ 2] = *(a06 + 2);
		b[ 3] = *(a06 + 3);
		b[ 4] = *(a06 + 4);
		b[ 5] = *(a06 + 5);
		b[ 6] = *(a06 + 6);
		b[ 7] = *(a06 + 7);
		b[ 8] = *(a06 + 8);
		b[ 9] = *(a06 + 9);
#ifdef UNIT
		b[10] = ONE;
		b[11] = ZERO;
#else
		b[10] = *(a06 + 10);
		b[11] = *(a06 + 11);
#endif
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
		b[ 28] = ZERO;
		b[ 29] = ZERO;
		b[ 30] = ZERO;
		b[ 31] = ZERO;
		b += 32;
	      }

	      if (i >= 7) {
		b[ 0] = *(a07 + 0);
		b[ 1] = *(a07 + 1);
		b[ 2] = *(a07 + 2);
		b[ 3] = *(a07 + 3);
		b[ 4] = *(a07 + 4);
		b[ 5] = *(a07 + 5);
		b[ 6] = *(a07 + 6);
		b[ 7] = *(a07 + 7);
		b[ 8] = *(a07 + 8);
		b[ 9] = *(a07 + 9);
		b[10] = *(a07 + 10);
		b[11] = *(a07 + 11);
#ifdef UNIT
		b[12] = ONE;
		b[13] = ZERO;
#else
		b[12] = *(a07 + 12);
		b[13] = *(a07 + 13);
#endif
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
		b[ 28] = ZERO;
		b[ 29] = ZERO;
		b[ 30] = ZERO;
		b[ 31] = ZERO;
		b += 32;
	    }

		if (i >= 8) {
	      b[  0] = *(a08 + 0);
	      b[  1] = *(a08 + 1);
	      b[  2] = *(a08 + 2);
	      b[  3] = *(a08 + 3);
	      b[  4] = *(a08 + 4);
	      b[  5] = *(a08 + 5);
	      b[  6] = *(a08 + 6);
	      b[  7] = *(a08 + 7);
	      b[  8] = *(a08 + 8);
	      b[  9] = *(a08 + 9);
	      b[ 10] = *(a08 + 10);
	      b[ 11] = *(a08 + 11);
	      b[ 12] = *(a08 + 12);
	      b[ 13] = *(a08 + 13);
#ifdef UNIT
	      b[ 14] = ONE;
	      b[ 15] = ZERO;
#else
	      b[ 14] = *(a08 +  14);
	      b[ 15] = *(a08 +  15);
#endif
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
		b[ 28] = ZERO;
		b[ 29] = ZERO;
		b[ 30] = ZERO;
		b[ 31] = ZERO;
	      b += 32;
	    }

		if (i >= 9) {
	      b[  0] = *(a09 + 0);
	      b[  1] = *(a09 + 1);
	      b[  2] = *(a09 + 2);
	      b[  3] = *(a09 + 3);
	      b[  4] = *(a09 + 4);
	      b[  5] = *(a09 + 5);
	      b[  6] = *(a09 + 6);
	      b[  7] = *(a09 + 7);
	      b[  8] = *(a09 + 8);
	      b[  9] = *(a09 + 9);
	      b[ 10] = *(a09 + 10);
	      b[ 11] = *(a09 + 11);
	      b[ 12] = *(a09 + 12);
	      b[ 13] = *(a09 + 13);
	      b[ 14] = *(a09 + 14);
	      b[ 15] = *(a09 + 15);
#ifdef UNIT
	      b[ 16] = ONE;
	      b[ 17] = ZERO;
#else
	      b[ 16] = *(a09 + 16);
	      b[ 17] = *(a09 + 17);
#endif
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
		b[ 28] = ZERO;
		b[ 29] = ZERO;
		b[ 30] = ZERO;
		b[ 31] = ZERO;
	      b += 32;
	    }

		if (i >= 10) {
	      b[  0] = *(a10 + 0);
	      b[  1] = *(a10 + 1);
	      b[  2] = *(a10 + 2);
	      b[  3] = *(a10 + 3);
	      b[  4] = *(a10 + 4);
	      b[  5] = *(a10 + 5);
	      b[  6] = *(a10 + 6);
	      b[  7] = *(a10 + 7);
	      b[  8] = *(a10 + 8);
	      b[  9] = *(a10 + 9);
	      b[ 10] = *(a10 + 10);
	      b[ 11] = *(a10 + 11);
	      b[ 12] = *(a10 + 12);
	      b[ 13] = *(a10 + 13);
	      b[ 14] = *(a10 + 14);
	      b[ 15] = *(a10 + 15);
	      b[ 16] = *(a10 + 16);
	      b[ 17] = *(a10 + 17);
#ifdef UNIT
	      b[ 18] = ONE;
	      b[ 19] = ZERO;
#else
	      b[ 18] = *(a10 + 18);
	      b[ 19] = *(a10 + 19);
#endif
	    b[ 20] = ZERO;
		b[ 21] = ZERO;
		b[ 22] = ZERO;
		b[ 23] = ZERO;
		b[ 24] = ZERO;
		b[ 25] = ZERO;
		b[ 26] = ZERO;
		b[ 27] = ZERO;
		b[ 28] = ZERO;
		b[ 29] = ZERO;
		b[ 30] = ZERO;
		b[ 31] = ZERO;
	      b += 32;
	    }

		if (i >= 11) {
	      b[  0] = *(a11 + 0);
	      b[  1] = *(a11 + 1);
	      b[  2] = *(a11 + 2);
	      b[  3] = *(a11 + 3);
	      b[  4] = *(a11 + 4);
	      b[  5] = *(a11 + 5);
	      b[  6] = *(a11 + 6);
	      b[  7] = *(a11 + 7);
	      b[  8] = *(a11 + 8);
	      b[  9] = *(a11 + 9);
	      b[ 10] = *(a11 + 10);
	      b[ 11] = *(a11 + 11);
	      b[ 12] = *(a11 + 12);
	      b[ 13] = *(a11 + 13);
	      b[ 14] = *(a11 + 14);
	      b[ 15] = *(a11 + 15);
	      b[ 16] = *(a11 + 16);
	      b[ 17] = *(a11 + 17);
	      b[ 18] = *(a11 + 18);
	      b[ 19] = *(a11 + 19);
#ifdef UNIT
	      b[ 20] = ONE;
	      b[ 21] = ZERO;
#else
	      b[ 20] = *(a11 + 20);
	      b[ 21] = *(a11 + 21);
#endif
	    b[ 22] = ZERO;
		b[ 23] = ZERO;
		b[ 24] = ZERO;
		b[ 25] = ZERO;
		b[ 26] = ZERO;
		b[ 27] = ZERO;
		b[ 28] = ZERO;
		b[ 29] = ZERO;
		b[ 30] = ZERO;
		b[ 31] = ZERO;
	      b += 32;
	    }

		if (i >= 12) {
	      b[  0] = *(a12 + 0);
	      b[  1] = *(a12 + 1);
	      b[  2] = *(a12 + 2);
	      b[  3] = *(a12 + 3);
	      b[  4] = *(a12 + 4);
	      b[  5] = *(a12 + 5);
	      b[  6] = *(a12 + 6);
	      b[  7] = *(a12 + 7);
	      b[  8] = *(a12 + 8);
	      b[  9] = *(a12 + 9);
	      b[ 10] = *(a12 + 10);
	      b[ 11] = *(a12 + 11);
	      b[ 12] = *(a12 + 12);
	      b[ 13] = *(a12 + 13);
	      b[ 14] = *(a12 + 14);
	      b[ 15] = *(a12 + 15);
	      b[ 16] = *(a12 + 16);
	      b[ 17] = *(a12 + 17);
	      b[ 18] = *(a12 + 18);
	      b[ 19] = *(a12 + 19);
	      b[ 20] = *(a12 + 20);
	      b[ 21] = *(a12 + 21);
#ifdef UNIT
	      b[ 22] = ONE;
	      b[ 23] = ZERO;
#else
	      b[ 22] = *(a12 + 22);
	      b[ 23] = *(a12 + 23);
#endif
	    b[ 24] = ZERO;
		b[ 25] = ZERO;
		b[ 26] = ZERO;
		b[ 27] = ZERO;
		b[ 28] = ZERO;
		b[ 29] = ZERO;
		b[ 30] = ZERO;
		b[ 31] = ZERO;
	      b += 32;
	    }

		if (i >= 13) {
	      b[  0] = *(a13 + 0);
	      b[  1] = *(a13 + 1);
	      b[  2] = *(a13 + 2);
	      b[  3] = *(a13 + 3);
	      b[  4] = *(a13 + 4);
	      b[  5] = *(a13 + 5);
	      b[  6] = *(a13 + 6);
	      b[  7] = *(a13 + 7);
	      b[  8] = *(a13 + 8);
	      b[  9] = *(a13 + 9);
	      b[ 10] = *(a13 + 10);
	      b[ 11] = *(a13 + 11);
	      b[ 12] = *(a13 + 12);
	      b[ 13] = *(a13 + 13);
	      b[ 14] = *(a13 + 14);
	      b[ 15] = *(a13 + 15);
	      b[ 16] = *(a13 + 16);
	      b[ 17] = *(a13 + 17);
	      b[ 18] = *(a13 + 18);
	      b[ 19] = *(a13 + 19);
	      b[ 20] = *(a13 + 20);
	      b[ 21] = *(a13 + 21);
	      b[ 22] = *(a13 + 22);
	      b[ 23] = *(a13 + 23);
#ifdef UNIT
	      b[ 24] = ONE;
	      b[ 25] = ZERO;
#else
	      b[ 24] = *(a13 + 24);
	      b[ 25] = *(a13 + 25);
#endif
	    b[ 26] = ZERO;
		b[ 27] = ZERO;
		b[ 28] = ZERO;
		b[ 29] = ZERO;
		b[ 30] = ZERO;
		b[ 31] = ZERO;
	      b += 32;
	    }

		if (i >= 14) {
	      b[  0] = *(a14 + 0);
	      b[  1] = *(a14 + 1);
	      b[  2] = *(a14 + 2);
	      b[  3] = *(a14 + 3);
	      b[  4] = *(a14 + 4);
	      b[  5] = *(a14 + 5);
	      b[  6] = *(a14 + 6);
	      b[  7] = *(a14 + 7);
	      b[  8] = *(a14 + 8);
	      b[  9] = *(a14 + 9);
	      b[ 10] = *(a14 + 10);
	      b[ 11] = *(a14 + 11);
	      b[ 12] = *(a14 + 12);
	      b[ 13] = *(a14 + 13);
	      b[ 14] = *(a14 + 14);
	      b[ 15] = *(a14 + 15);
	      b[ 16] = *(a14 + 16);
	      b[ 17] = *(a14 + 17);
	      b[ 18] = *(a14 + 18);
	      b[ 19] = *(a14 + 19);
	      b[ 20] = *(a14 + 20);
	      b[ 21] = *(a14 + 21);
	      b[ 22] = *(a14 + 22);
	      b[ 23] = *(a14 + 23);
	      b[ 24] = *(a14 + 24);
	      b[ 25] = *(a14 + 25);
#ifdef UNIT
	      b[ 26] = ONE;
	      b[ 27] = ZERO;
#else
	      b[ 26] = *(a14 + 26);
	      b[ 27] = *(a14 + 27);
#endif
	    b[ 28] = ZERO;
		b[ 29] = ZERO;
		b[ 30] = ZERO;
		b[ 31] = ZERO;
	      b += 32;
	    }

		if (i >= 15) {
	      b[  0] = *(a15 + 0);
	      b[  1] = *(a15 + 1);
	      b[  2] = *(a15 + 2);
	      b[  3] = *(a15 + 3);
	      b[  4] = *(a15 + 4);
	      b[  5] = *(a15 + 5);
	      b[  6] = *(a15 + 6);
	      b[  7] = *(a15 + 7);
	      b[  8] = *(a15 + 8);
	      b[  9] = *(a15 + 9);
	      b[ 10] = *(a15 + 10);
	      b[ 11] = *(a15 + 11);
	      b[ 12] = *(a15 + 12);
	      b[ 13] = *(a15 + 13);
	      b[ 14] = *(a15 + 14);
	      b[ 15] = *(a15 + 15);
	      b[ 16] = *(a15 + 16);
	      b[ 17] = *(a15 + 17);
	      b[ 18] = *(a15 + 18);
	      b[ 19] = *(a15 + 19);
	      b[ 20] = *(a15 + 20);
	      b[ 21] = *(a15 + 21);
	      b[ 22] = *(a15 + 22);
	      b[ 23] = *(a15 + 23);
	      b[ 24] = *(a15 + 24);
	      b[ 25] = *(a15 + 25);
	      b[ 26] = *(a15 + 26);
	      b[ 27] = *(a15 + 27);
#ifdef UNIT
	      b[ 28] = ONE;
	      b[ 29] = ZERO;
#else
	      b[ 28] = *(a15 + 28);
	      b[ 29] = *(a15 + 29);
#endif
	      b[ 30] = ZERO;
	      b[ 31] = ZERO;
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
	a01 = a + posX * 2 + (posY +  0) * lda;
	a02 = a + posX * 2 + (posY +  1) * lda;
	a03 = a + posX * 2 + (posY +  2) * lda;
	a04 = a + posX * 2 + (posY +  3) * lda;
	a05 = a + posX * 2 + (posY +  4) * lda;
	a06 = a + posX * 2 + (posY +  5) * lda;
	a07 = a + posX * 2 + (posY +  6) * lda;
	a08 = a + posX * 2 + (posY +  7) * lda;
      } else {
	a01 = a + posY * 2 + (posX +  0) * lda;
	a02 = a + posY * 2 + (posX +  1) * lda;
	a03 = a + posY * 2 + (posX +  2) * lda;
	a04 = a + posY * 2 + (posX +  3) * lda;
	a05 = a + posY * 2 + (posX +  4) * lda;
	a06 = a + posY * 2 + (posX +  5) * lda;
	a07 = a + posY * 2 + (posX +  6) * lda;
	a08 = a + posY * 2 + (posX +  7) * lda;
      }

      i = (m >> 3);
      if (i > 0) {
	do {
	  if (X < posY) {
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
	    if (X > posY) {
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

	      b[ 16] = *(a02 +  0);
	      b[ 17] = *(a02 +  1);
#ifdef UNIT
	      b[ 18] = ONE;
	      b[ 19] = ZERO;
#else
	      b[ 18] = *(a02 +  2);
	      b[ 19] = *(a02 +  3);
#endif
	      b[ 20] = ZERO;
	      b[ 21] = ZERO;
	      b[ 22] = ZERO;
	      b[ 23] = ZERO;
	      b[ 24] = ZERO;
	      b[ 25] = ZERO;
	      b[ 26] = ZERO;
	      b[ 27] = ZERO;
	      b[ 28] = ZERO;
	      b[ 29] = ZERO;
	      b[ 30] = ZERO;
	      b[ 31] = ZERO;

	      b[ 32] = *(a03 +  0);
	      b[ 33] = *(a03 +  1);
	      b[ 34] = *(a03 +  2);
	      b[ 35] = *(a03 +  3);
#ifdef UNIT
	      b[ 36] = ONE;
	      b[ 37] = ZERO;
#else
	      b[ 36] = *(a03 +  4);
	      b[ 37] = *(a03 +  5);
#endif
	      b[ 38] = ZERO;
	      b[ 39] = ZERO;
	      b[ 40] = ZERO;
	      b[ 41] = ZERO;
	      b[ 42] = ZERO;
	      b[ 43] = ZERO;
	      b[ 44] = ZERO;
	      b[ 45] = ZERO;
	      b[ 46] = ZERO;
	      b[ 47] = ZERO;

	      b[ 48] = *(a04 +  0);
	      b[ 49] = *(a04 +  1);
	      b[ 50] = *(a04 +  2);
	      b[ 51] = *(a04 +  3);
	      b[ 52] = *(a04 +  4);
	      b[ 53] = *(a04 +  5);
#ifdef UNIT
	      b[ 54] = ONE;
	      b[ 55] = ZERO;
#else
	      b[ 54] = *(a04 +  6);
	      b[ 55] = *(a04 +  7);
#endif
	      b[ 56] = ZERO;
	      b[ 57] = ZERO;
	      b[ 58] = ZERO;
	      b[ 59] = ZERO;
	      b[ 60] = ZERO;
	      b[ 61] = ZERO;
	      b[ 62] = ZERO;
	      b[ 63] = ZERO;

	      b[ 64] = *(a05 +  0);
	      b[ 65] = *(a05 +  1);
	      b[ 66] = *(a05 +  2);
	      b[ 67] = *(a05 +  3);
	      b[ 68] = *(a05 +  4);
	      b[ 69] = *(a05 +  5);
	      b[ 70] = *(a05 +  6);
	      b[ 71] = *(a05 +  7);
#ifdef UNIT
	      b[ 72] = ONE;
	      b[ 73] = ZERO;
#else
	      b[ 72] = *(a05 +  8);
	      b[ 73] = *(a05 +  9);
#endif
	      b[ 74] = ZERO;
	      b[ 75] = ZERO;
	      b[ 76] = ZERO;
	      b[ 77] = ZERO;
	      b[ 78] = ZERO;
	      b[ 79] = ZERO;

	      b[ 80] = *(a06 +  0);
	      b[ 81] = *(a06 +  1);
	      b[ 82] = *(a06 +  2);
	      b[ 83] = *(a06 +  3);
	      b[ 84] = *(a06 +  4);
	      b[ 85] = *(a06 +  5);
	      b[ 86] = *(a06 +  6);
	      b[ 87] = *(a06 +  7);
	      b[ 88] = *(a06 +  8);
	      b[ 89] = *(a06 +  9);
#ifdef UNIT
	      b[ 90] = ONE;
	      b[ 91] = ZERO;
#else
	      b[ 90] = *(a06 + 10);
	      b[ 91] = *(a06 + 11);
#endif
	      b[ 92] = ZERO;
	      b[ 93] = ZERO;
	      b[ 94] = ZERO;
	      b[ 95] = ZERO;

	      b[ 96] = *(a07 +  0);
	      b[ 97] = *(a07 +  1);
	      b[ 98] = *(a07 +  2);
	      b[ 99] = *(a07 +  3);
	      b[100] = *(a07 +  4);
	      b[101] = *(a07 +  5);
	      b[102] = *(a07 +  6);
	      b[103] = *(a07 +  7);
	      b[104] = *(a07 +  8);
	      b[105] = *(a07 +  9);
	      b[106] = *(a07 + 10);
	      b[107] = *(a07 + 11);
#ifdef UNIT
	      b[108] = ONE;
	      b[109] = ZERO;
#else
	      b[108] = *(a07 + 12);
	      b[109] = *(a07 + 13);
#endif
	      b[110] = ZERO;
	      b[111] = ZERO;

	      b[112] = *(a08 +  0);
	      b[113] = *(a08 +  1);
	      b[114] = *(a08 +  2);
	      b[115] = *(a08 +  3);
	      b[116] = *(a08 +  4);
	      b[117] = *(a08 +  5);
	      b[118] = *(a08 +  6);
	      b[119] = *(a08 +  7);
	      b[120] = *(a08 +  8);
	      b[121] = *(a08 +  9);
	      b[122] = *(a08 + 10);
	      b[123] = *(a08 + 11);
	      b[124] = *(a08 + 12);
	      b[125] = *(a08 + 13);
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
	  if (X > posY) {
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
	    b[ 0] = ONE;
	    b[ 1] = ZERO;
#else
	    b[ 0] = *(a01 +  0);
	    b[ 1] = *(a01 +  1);
#endif
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
	    b[14] = ZERO;
	    b[15] = ZERO;
	    b += 16;

	    if(i >= 2) {
	      b[ 0] = *(a02 +  0);
	      b[ 1] = *(a02 +  1);
#ifdef UNIT
	      b[ 2] = ONE;
	      b[ 3] = ZERO;
#else
	      b[ 2] = *(a02 +  2);
	      b[ 3] = *(a02 +  3);
#endif
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
	      b[14] = ZERO;
	      b[15] = ZERO;
	      b += 16;
	    }

	    if (i >= 3) {
	      b[ 0] = *(a03 +  0);
	      b[ 1] = *(a03 +  1);
	      b[ 2] = *(a03 +  2);
	      b[ 3] = *(a03 +  3);
#ifdef UNIT
	      b[ 4] = ONE;
	      b[ 5] = ZERO;
#else
	      b[ 4] = *(a03 +  4);
	      b[ 5] = *(a03 +  5);
#endif
	      b[ 6] = ZERO;
	      b[ 7] = ZERO;
	      b[ 8] = ZERO;
	      b[ 9] = ZERO;
	      b[10] = ZERO;
	      b[11] = ZERO;
	      b[12] = ZERO;
	      b[13] = ZERO;
	      b[14] = ZERO;
	      b[15] = ZERO;
	      b += 16;
	    }

	    if (i >= 4) {
	      b[ 0] = *(a04 +  0);
	      b[ 1] = *(a04 +  1);
	      b[ 2] = *(a04 +  2);
	      b[ 3] = *(a04 +  3);
	      b[ 4] = *(a04 +  4);
	      b[ 5] = *(a04 +  5);
#ifdef UNIT
	      b[ 6] = ONE;
	      b[ 7] = ZERO;
#else
	      b[ 6] = *(a04 +  6);
	      b[ 7] = *(a04 +  7);
#endif
	      b[ 8] = ZERO;
	      b[ 9] = ZERO;
	      b[10] = ZERO;
	      b[11] = ZERO;
	      b[12] = ZERO;
	      b[13] = ZERO;
	      b[14] = ZERO;
	      b[15] = ZERO;
	      b += 16;
	    }

	    if (i >= 5) {
	      b[ 0] = *(a05 +  0);
	      b[ 1] = *(a05 +  1);
	      b[ 2] = *(a05 +  2);
	      b[ 3] = *(a05 +  3);
	      b[ 4] = *(a05 +  4);
	      b[ 5] = *(a05 +  5);
	      b[ 6] = *(a05 +  6);
	      b[ 7] = *(a05 +  7);
#ifdef UNIT
	      b[ 8] = ONE;
	      b[ 9] = ZERO;
#else
	      b[ 8] = *(a05 +  8);
	      b[ 9] = *(a05 +  9);
#endif
	      b[10] = ZERO;
	      b[11] = ZERO;
	      b[12] = ZERO;
	      b[13] = ZERO;
	      b[14] = ZERO;
	      b[15] = ZERO;
	      b += 16;
	    }

	    if (i >= 6) {
	      b[ 0] = *(a06 +  0);
	      b[ 1] = *(a06 +  1);
	      b[ 2] = *(a06 +  2);
	      b[ 3] = *(a06 +  3);
	      b[ 4] = *(a06 +  4);
	      b[ 5] = *(a06 +  5);
	      b[ 6] = *(a06 +  6);
	      b[ 7] = *(a06 +  7);
	      b[ 8] = *(a06 +  8);
	      b[ 9] = *(a06 +  9);
#ifdef UNIT
	      b[10] = ONE;
	      b[11] = ZERO;
#else
	      b[10] = *(a06 + 10);
	      b[11] = *(a06 + 11);
#endif
	      b[12] = ZERO;
	      b[13] = ZERO;
	      b[14] = ZERO;
	      b[15] = ZERO;
	      b += 16;
	    }

	    if (i >= 7) {
	      b[ 0] = *(a07 +  0);
	      b[ 1] = *(a07 +  1);
	      b[ 2] = *(a07 +  2);
	      b[ 3] = *(a07 +  3);
	      b[ 4] = *(a07 +  4);
	      b[ 5] = *(a07 +  5);
	      b[ 6] = *(a07 +  6);
	      b[ 7] = *(a07 +  7);
	      b[ 8] = *(a07 +  8);
	      b[ 9] = *(a07 +  9);
	      b[10] = *(a07 + 10);
	      b[11] = *(a07 + 11);
#ifdef UNIT
	      b[12] = ONE;
	      b[13] = ZERO;
#else
	      b[12] = *(a07 + 12);
	      b[13] = *(a07 + 13);
#endif
	      b[14] = ZERO;
	      b[15] = ZERO;
	      b += 16;
	    }
	  }
      }

      posY += 8;
  }


  if (n & 4){
    X = posX;

    if (posX <= posY) {
      a01 = a + posX * 2 + (posY +  0) * lda;
      a02 = a + posX * 2 + (posY +  1) * lda;
      a03 = a + posX * 2 + (posY +  2) * lda;
      a04 = a + posX * 2 + (posY +  3) * lda;
    } else {
      a01 = a + posY * 2 + (posX +  0) * lda;
      a02 = a + posY * 2 + (posX +  1) * lda;
      a03 = a + posY * 2 + (posX +  2) * lda;
      a04 = a + posY * 2 + (posX +  3) * lda;
    }

    i = (m >> 2);
    if (i > 0) {
      do {
	if (X < posY) {
	  a01 += 8;
	  a02 += 8;
	  a03 += 8;
	  a04 += 8;
	  b += 32;
	} else
	  if (X > posY) {

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
	    b[  2] = ZERO;
	    b[  3] = ZERO;
	    b[  4] = ZERO;
	    b[  5] = ZERO;
	    b[  6] = ZERO;
	    b[  7] = ZERO;

	    b[  8] = *(a02 +  0);
	    b[  9] = *(a02 +  1);
#ifdef UNIT
	    b[ 10] = ONE;
	    b[ 11] = ZERO;
#else
	    b[ 10] = *(a02 +  2);
	    b[ 11] = *(a02 +  3);
#endif
	    b[ 12] = ZERO;
	    b[ 13] = ZERO;
	    b[ 14] = ZERO;
	    b[ 15] = ZERO;

	    b[ 16] = *(a03 +  0);
	    b[ 17] = *(a03 +  1);
	    b[ 18] = *(a03 +  2);
	    b[ 19] = *(a03 +  3);
#ifdef UNIT
	    b[ 20] = ONE;
	    b[ 21] = ZERO;
#else
	    b[ 20] = *(a03 +  4);
	    b[ 21] = *(a03 +  5);
#endif
	    b[ 22] = ZERO;
	    b[ 23] = ZERO;

	    b[ 24] = *(a04 +  0);
	    b[ 25] = *(a04 +  1);
	    b[ 26] = *(a04 +  2);
	    b[ 27] = *(a04 +  3);
	    b[ 28] = *(a04 +  4);
	    b[ 29] = *(a04 +  5);
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
	/* a01 += 2 * i;
	a02 += 2 * i;
	a03 += 2 * i;
	a04 += 2 * i; */
	b += 8 * i;
      } else
	if (X > posY) {

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
	  b[ 0] = ONE;
	  b[ 1] = ZERO;
#else
	  b[ 0] = *(a01 +  0);
	  b[ 1] = *(a01 +  1);
#endif
	  b[ 2] = ZERO;
	  b[ 3] = ZERO;
	  b[ 4] = ZERO;
	  b[ 5] = ZERO;
	  b[ 6] = ZERO;
	  b[ 7] = ZERO;
	  b += 8;

	  if(i >= 2) {
	    b[ 0] = *(a02 +  0);
	    b[ 1] = *(a02 +  1);
#ifdef UNIT
	    b[ 2] = ONE;
	    b[ 3] = ZERO;
#else
	    b[ 2] = *(a02 +  2);
	    b[ 3] = *(a02 +  3);
#endif
	    b[ 4] = ZERO;
	    b[ 5] = ZERO;
	    b[ 6] = ZERO;
	    b[ 7] = ZERO;
	    b += 8;
	  }

	  if (i >= 3) {
	    b[ 0] = *(a03 +  0);
	    b[ 1] = *(a03 +  1);
	    b[ 2] = *(a03 +  2);
	    b[ 3] = *(a03 +  3);
#ifdef UNIT
	    b[ 4] = ONE;
	    b[ 5] = ZERO;
#else
	    b[ 4] = *(a03 +  4);
	    b[ 5] = *(a03 +  5);
#endif
	    b[ 6] = ZERO;
	    b[ 7] = ZERO;
	    b += 8;
	  }
	}
    }

    posY += 4;
  }


  if (n & 2){
    X = posX;

    if (posX <= posY) {
      a01 = a + posX * 2 + (posY +  0) * lda;
      a02 = a + posX * 2 + (posY +  1) * lda;
    } else {
      a01 = a + posY * 2 + (posX +  0) * lda;
      a02 = a + posY * 2 + (posX +  1) * lda;
    }

    i = (m >> 1);
    if (i > 0) {
      do {
	if (X < posY) {
	    a01 += 4;
	    a02 += 4;
	    b += 8;
	} else
	  if (X > posY) {
	    b[  0] = *(a01 +  0);
	    b[  1] = *(a01 +  1);
	    b[  2] = *(a01 +  2);
	    b[  3] = *(a01 +  3);
	    b[  4] = *(a02 +  0);
	    b[  5] = *(a02 +  1);
	    b[  6] = *(a02 +  2);
	    b[  7] = *(a02 +  3);

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
	    b[  2] = ZERO;
	    b[  3] = ZERO;

	    b[  4] = *(a02 +  0);
	    b[  5] = *(a02 +  1);
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

    i = (m & 1);
    if (i) {

      if (X < posY) {
	b += 4;
      } else
	if (X > posY) {
	  b[  0] = *(a01 +  0);
	  b[  1] = *(a01 +  1);
	  b[  2] = *(a01 +  2);
	  b[  3] = *(a01 +  3);
	  b += 4;
	  }
#if 1
	}
#else
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
	  b += 4;
	}
#endif
    posY += 2;
  }

  if (n & 1){
    X = posX;

    if (posX <= posY) {
      a01 = a + posX * 2 + (posY +  0) * lda;
    } else {
      a01 = a + posY * 2 + (posX +  0) * lda;
    }

    i = m;
    if (m > 0) {
      do {
	if (X < posY) {
	  a01 += 2;
	} else {
#ifdef UNIT
	  if (X > posY) {
#endif
	    b[  0] = *(a01 +  0);
	    b[  1] = *(a01 +  1);
#ifdef UNIT
	  } else {
	    b[  0] = ONE;
	    b[  1] = ZERO;
	  }
#endif
	  a01 += lda;
	}
	b += 2;
	X ++;
	i --;
      } while (i > 0);
    }
  }

  return 0;
}
