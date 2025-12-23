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
	    if (X < posY) {
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

	      b[ 32] = *(a01 +  2);
	      b[ 33] = *(a01 +  3);
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

	      b[ 64] = *(a01 +  4);
	      b[ 65] = *(a01 +  5);
	      b[ 66] = *(a02 +  4);
	      b[ 67] = *(a02 +  5);
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

	      b[ 96] = *(a01 +  6);
	      b[ 97] = *(a01 +  7);
	      b[ 98] = *(a02 +  6);
	      b[ 99] = *(a02 +  7);
	      b[100] = *(a03 +  6);
	      b[101] = *(a03 +  7);
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

	      b[128] = *(a01 +  8);
	      b[129] = *(a01 +  9);
	      b[130] = *(a02 +  8);
	      b[131] = *(a02 +  9);
	      b[132] = *(a03 +  8);
	      b[133] = *(a03 +  9);
	      b[134] = *(a04 +  8);
	      b[135] = *(a04 +  9);
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

	      b[160] = *(a01 + 10);
	      b[161] = *(a01 + 11);
	      b[162] = *(a02 + 10);
	      b[163] = *(a02 + 11);
	      b[164] = *(a03 + 10);
	      b[165] = *(a03 + 11);
	      b[166] = *(a04 + 10);
	      b[167] = *(a04 + 11);
	      b[168] = *(a05 + 10);
	      b[169] = *(a05 + 11);
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

	      b[192] = *(a01 + 12);
	      b[193] = *(a01 + 13);
	      b[194] = *(a02 + 12);
	      b[195] = *(a02 + 13);
	      b[196] = *(a03 + 12);
	      b[197] = *(a03 + 13);
	      b[198] = *(a04 + 12);
	      b[199] = *(a04 + 13);
	      b[200] = *(a05 + 12);
	      b[201] = *(a05 + 13);
	      b[202] = *(a06 + 12);
	      b[203] = *(a06 + 13);
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

	      b[224] = *(a01 + 14);
	      b[225] = *(a01 + 15);
	      b[226] = *(a02 + 14);
	      b[227] = *(a02 + 15);
	      b[228] = *(a03 + 14);
	      b[229] = *(a03 + 15);
	      b[230] = *(a04 + 14);
	      b[231] = *(a04 + 15);
	      b[232] = *(a05 + 14);
	      b[233] = *(a05 + 15);
	      b[234] = *(a06 + 14);
	      b[235] = *(a06 + 15);
	      b[236] = *(a07 + 14);
	      b[237] = *(a07 + 15);
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

	      b[256] = *(a01 + 16);
	      b[257] = *(a01 + 17);
	      b[258] = *(a02 + 16);
	      b[259] = *(a02 + 17);
	      b[260] = *(a03 + 16);
	      b[261] = *(a03 + 17);
	      b[262] = *(a04 + 16);
	      b[263] = *(a04 + 17);
	      b[264] = *(a05 + 16);
	      b[265] = *(a05 + 17);
	      b[266] = *(a06 + 16);
	      b[267] = *(a06 + 17);
	      b[268] = *(a07 + 16);
	      b[269] = *(a07 + 17);
	      b[270] = *(a08 + 16);
	      b[271] = *(a08 + 17);
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

		  b[288] = *(a01 + 18);
		  b[289] = *(a01 + 19);
	      b[290] = *(a02 + 18);
	      b[291] = *(a02 + 19);
	      b[292] = *(a03 + 18);
	      b[293] = *(a03 + 19);
	      b[294] = *(a04 + 18);
	      b[295] = *(a04 + 19);
	      b[296] = *(a05 + 18);
	      b[297] = *(a05 + 19);
	      b[298] = *(a06 + 18);
	      b[299] = *(a06 + 19);
	      b[300] = *(a07 + 18);
	      b[301] = *(a07 + 19);
	      b[302] = *(a08 + 18);
	      b[303] = *(a08 + 19);
	      b[304] = *(a09 + 18);
	      b[305] = *(a09 + 19);
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

		  b[320] = *(a01 + 20);
		  b[321] = *(a01 + 21);
	      b[322] = *(a02 + 20);
	      b[323] = *(a02 + 21);
	      b[324] = *(a03 + 20);
	      b[325] = *(a03 + 21);
	      b[326] = *(a04 + 20);
	      b[327] = *(a04 + 21);
	      b[328] = *(a05 + 20);
	      b[329] = *(a05 + 21);
	      b[330] = *(a06 + 20);
	      b[331] = *(a06 + 21);
	      b[332] = *(a07 + 20);
	      b[333] = *(a07 + 21);
	      b[334] = *(a08 + 20);
	      b[335] = *(a08 + 21);
	      b[336] = *(a09 + 20);
	      b[337] = *(a09 + 21);
	      b[338] = *(a10 + 20);
	      b[339] = *(a10 + 21);
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

		  b[352] = *(a01 + 22);
		  b[353] = *(a01 + 23);
	      b[354] = *(a02 + 22);
	      b[355] = *(a02 + 23);
	      b[356] = *(a03 + 22);
	      b[357] = *(a03 + 23);
	      b[358] = *(a04 + 22);
	      b[359] = *(a04 + 23);
	      b[360] = *(a05 + 22);
	      b[361] = *(a05 + 23);
	      b[362] = *(a06 + 22);
	      b[363] = *(a06 + 23);
	      b[364] = *(a07 + 22);
	      b[365] = *(a07 + 23);
	      b[366] = *(a08 + 22);
	      b[367] = *(a08 + 23);
	      b[368] = *(a09 + 22);
	      b[369] = *(a09 + 23);
	      b[370] = *(a10 + 22);
	      b[371] = *(a10 + 23);
	      b[372] = *(a11 + 22);
	      b[373] = *(a11 + 23);
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

		  b[384] = *(a01 + 24);
		  b[385] = *(a01 + 25);
	      b[386] = *(a02 + 24);
	      b[387] = *(a02 + 25);
	      b[388] = *(a03 + 24);
	      b[389] = *(a03 + 25);
	      b[390] = *(a04 + 24);
	      b[391] = *(a04 + 25);
	      b[392] = *(a05 + 24);
	      b[393] = *(a05 + 25);
	      b[394] = *(a06 + 24);
	      b[395] = *(a06 + 25);
	      b[396] = *(a07 + 24);
	      b[397] = *(a07 + 25);
	      b[398] = *(a08 + 24);
	      b[399] = *(a08 + 25);
	      b[400] = *(a09 + 24);
	      b[401] = *(a09 + 25);
	      b[402] = *(a10 + 24);
	      b[403] = *(a10 + 25);
	      b[404] = *(a11 + 24);
	      b[405] = *(a11 + 25);
	      b[406] = *(a12 + 24);
	      b[407] = *(a12 + 25);
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

		  b[416] = *(a01 + 26);
		  b[417] = *(a01 + 27);
	      b[418] = *(a02 + 26);
	      b[419] = *(a02 + 27);
	      b[420] = *(a03 + 26);
	      b[421] = *(a03 + 27);
	      b[422] = *(a04 + 26);
	      b[423] = *(a04 + 27);
	      b[424] = *(a05 + 26);
	      b[425] = *(a05 + 27);
	      b[426] = *(a06 + 26);
	      b[427] = *(a06 + 27);
	      b[428] = *(a07 + 26);
	      b[429] = *(a07 + 27);
	      b[430] = *(a08 + 26);
	      b[431] = *(a08 + 27);
	      b[432] = *(a09 + 26);
	      b[433] = *(a09 + 27);
	      b[434] = *(a10 + 26);
	      b[435] = *(a10 + 27);
	      b[436] = *(a11 + 26);
	      b[437] = *(a11 + 27);
	      b[438] = *(a12 + 26);
	      b[439] = *(a12 + 27);
	      b[440] = *(a13 + 26);
	      b[441] = *(a13 + 27);
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

		  b[448] = *(a01 + 28);
		  b[449] = *(a01 + 29);
	      b[450] = *(a02 + 28);
	      b[451] = *(a02 + 29);
	      b[452] = *(a03 + 28);
	      b[453] = *(a03 + 29);
	      b[454] = *(a04 + 28);
	      b[455] = *(a04 + 29);
	      b[456] = *(a05 + 28);
	      b[457] = *(a05 + 29);
	      b[458] = *(a06 + 28);
	      b[459] = *(a06 + 29);
	      b[460] = *(a07 + 28);
	      b[461] = *(a07 + 29);
	      b[462] = *(a08 + 28);
	      b[463] = *(a08 + 29);
	      b[464] = *(a09 + 28);
	      b[465] = *(a09 + 29);
	      b[466] = *(a10 + 28);
	      b[467] = *(a10 + 29);
	      b[468] = *(a11 + 28);
	      b[469] = *(a11 + 29);
	      b[470] = *(a12 + 28);
	      b[471] = *(a12 + 29);
	      b[472] = *(a13 + 28);
	      b[473] = *(a13 + 29);
	      b[474] = *(a14 + 28);
	      b[475] = *(a14 + 29);
#ifdef UNIT
	      b[476] = ONE;
	      b[477] = ZERO;
#else
	      b[476] = *(a15 + 28);
	      b[477] = *(a15 + 29);
#endif
		  b[478] = ZERO;
		  b[479] = ZERO;

		  b[480] = *(a01 + 30);
		  b[481] = *(a01 + 31);
	      b[482] = *(a02 + 30);
	      b[483] = *(a02 + 31);
	      b[484] = *(a03 + 30);
	      b[485] = *(a03 + 31);
	      b[486] = *(a04 + 30);
	      b[487] = *(a04 + 31);
	      b[488] = *(a05 + 30);
	      b[489] = *(a05 + 31);
	      b[490] = *(a06 + 30);
	      b[491] = *(a06 + 31);
	      b[492] = *(a07 + 30);
	      b[493] = *(a07 + 31);
	      b[494] = *(a08 + 30);
	      b[495] = *(a08 + 31);
	      b[496] = *(a09 + 30);
	      b[497] = *(a09 + 31);
	      b[498] = *(a10 + 30);
	      b[499] = *(a10 + 31);
	      b[500] = *(a11 + 30);
	      b[501] = *(a11 + 31);
	      b[502] = *(a12 + 30);
	      b[503] = *(a12 + 31);
	      b[504] = *(a13 + 30);
	      b[505] = *(a13 + 31);
	      b[506] = *(a14 + 30);
	      b[507] = *(a14 + 31);
	      b[508] = *(a15 + 30);
	      b[509] = *(a15 + 31);
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
	  if (X < posY) {
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
		b[ 0] = *(a01 +  2);
		b[ 1] = *(a01 +  3);
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
		b[ 0] = *(a01 +  4);
		b[ 1] = *(a01 +  5);
		b[ 2] = *(a02 +  4);
		b[ 3] = *(a02 +  5);
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
		b[ 0] = *(a01 +  6);
		b[ 1] = *(a01 +  7);
		b[ 2] = *(a02 +  6);
		b[ 3] = *(a02 +  7);
		b[ 4] = *(a03 +  6);
		b[ 5] = *(a03 +  7);
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
		b[ 0] = *(a01 +  8);
		b[ 1] = *(a01 +  9);
		b[ 2] = *(a02 +  8);
		b[ 3] = *(a02 +  9);
		b[ 4] = *(a03 +  8);
		b[ 5] = *(a03 +  9);
		b[ 6] = *(a04 +  8);
		b[ 7] = *(a04 +  9);
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
		b[ 0] = *(a01 + 10);
		b[ 1] = *(a01 + 11);
		b[ 2] = *(a02 + 10);
		b[ 3] = *(a02 + 11);
		b[ 4] = *(a03 + 10);
		b[ 5] = *(a03 + 11);
		b[ 6] = *(a04 + 10);
		b[ 7] = *(a04 + 11);
		b[ 8] = *(a05 + 10);
		b[ 9] = *(a05 + 11);
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
		b[ 0] = *(a01 + 12);
		b[ 1] = *(a01 + 13);
		b[ 2] = *(a02 + 12);
		b[ 3] = *(a02 + 13);
		b[ 4] = *(a03 + 12);
		b[ 5] = *(a03 + 13);
		b[ 6] = *(a04 + 12);
		b[ 7] = *(a04 + 13);
		b[ 8] = *(a05 + 12);
		b[ 9] = *(a05 + 13);
		b[10] = *(a06 + 12);
		b[11] = *(a06 + 13);
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
	      b[  0] = *(a01 + 14);
	      b[  1] = *(a01 + 15);
	      b[  2] = *(a02 + 14);
	      b[  3] = *(a02 + 15);
	      b[  4] = *(a03 + 14);
	      b[  5] = *(a03 + 15);
	      b[  6] = *(a04 + 14);
	      b[  7] = *(a04 + 15);
	      b[  8] = *(a05 + 14);
	      b[  9] = *(a05 + 15);
	      b[ 10] = *(a06 + 14);
	      b[ 11] = *(a06 + 15);
	      b[ 12] = *(a07 + 14);
	      b[ 13] = *(a07 + 15);
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
	      b[  0] = *(a01 + 16);
	      b[  1] = *(a01 + 17);
	      b[  2] = *(a02 + 16);
	      b[  3] = *(a02 + 17);
	      b[  4] = *(a03 + 16);
	      b[  5] = *(a03 + 17);
	      b[  6] = *(a04 + 16);
	      b[  7] = *(a04 + 17);
	      b[  8] = *(a05 + 16);
	      b[  9] = *(a05 + 17);
	      b[ 10] = *(a06 + 16);
	      b[ 11] = *(a06 + 17);
	      b[ 12] = *(a07 + 16);
	      b[ 13] = *(a07 + 17);
	      b[ 14] = *(a08 + 16);
	      b[ 15] = *(a08 + 17);
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
	      b[  0] = *(a01 + 18);
	      b[  1] = *(a01 + 19);
	      b[  2] = *(a02 + 18);
	      b[  3] = *(a02 + 19);
	      b[  4] = *(a03 + 18);
	      b[  5] = *(a03 + 19);
	      b[  6] = *(a04 + 18);
	      b[  7] = *(a04 + 19);
	      b[  8] = *(a05 + 18);
	      b[  9] = *(a05 + 19);
	      b[ 10] = *(a06 + 18);
	      b[ 11] = *(a06 + 19);
	      b[ 12] = *(a07 + 18);
	      b[ 13] = *(a07 + 19);
	      b[ 14] = *(a08 + 18);
	      b[ 15] = *(a08 + 19);
	      b[ 16] = *(a09 + 18);
	      b[ 17] = *(a09 + 19);
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
	      b[  0] = *(a01 + 20);
	      b[  1] = *(a01 + 21);
	      b[  2] = *(a02 + 20);
	      b[  3] = *(a02 + 21);
	      b[  4] = *(a03 + 20);
	      b[  5] = *(a03 + 21);
	      b[  6] = *(a04 + 20);
	      b[  7] = *(a04 + 21);
	      b[  8] = *(a05 + 20);
	      b[  9] = *(a05 + 21);
	      b[ 10] = *(a06 + 20);
	      b[ 11] = *(a06 + 21);
	      b[ 12] = *(a07 + 20);
	      b[ 13] = *(a07 + 21);
	      b[ 14] = *(a08 + 20);
	      b[ 15] = *(a08 + 21);
	      b[ 16] = *(a09 + 20);
	      b[ 17] = *(a09 + 21);
	      b[ 18] = *(a10 + 20);
	      b[ 19] = *(a10 + 21);
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
	      b[  0] = *(a01 + 22);
	      b[  1] = *(a01 + 23);
	      b[  2] = *(a02 + 22);
	      b[  3] = *(a02 + 23);
	      b[  4] = *(a03 + 22);
	      b[  5] = *(a03 + 23);
	      b[  6] = *(a04 + 22);
	      b[  7] = *(a04 + 23);
	      b[  8] = *(a05 + 22);
	      b[  9] = *(a05 + 23);
	      b[ 10] = *(a06 + 22);
	      b[ 11] = *(a06 + 23);
	      b[ 12] = *(a07 + 22);
	      b[ 13] = *(a07 + 23);
	      b[ 14] = *(a08 + 22);
	      b[ 15] = *(a08 + 23);
	      b[ 16] = *(a09 + 22);
	      b[ 17] = *(a09 + 23);
	      b[ 18] = *(a10 + 22);
	      b[ 19] = *(a10 + 23);
	      b[ 20] = *(a11 + 22);
	      b[ 21] = *(a11 + 23);
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
	      b[  0] = *(a01 + 24);
	      b[  1] = *(a01 + 25);
	      b[  2] = *(a02 + 24);
	      b[  3] = *(a02 + 25);
	      b[  4] = *(a03 + 24);
	      b[  5] = *(a03 + 25);
	      b[  6] = *(a04 + 24);
	      b[  7] = *(a04 + 25);
	      b[  8] = *(a05 + 24);
	      b[  9] = *(a05 + 25);
	      b[ 10] = *(a06 + 24);
	      b[ 11] = *(a06 + 25);
	      b[ 12] = *(a07 + 24);
	      b[ 13] = *(a07 + 25);
	      b[ 14] = *(a08 + 24);
	      b[ 15] = *(a08 + 25);
	      b[ 16] = *(a09 + 24);
	      b[ 17] = *(a09 + 25);
	      b[ 18] = *(a10 + 24);
	      b[ 19] = *(a10 + 25);
	      b[ 20] = *(a11 + 24);
	      b[ 21] = *(a11 + 25);
	      b[ 22] = *(a12 + 24);
	      b[ 23] = *(a12 + 25);
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
	      b[  0] = *(a01 + 26);
	      b[  1] = *(a01 + 27);
	      b[  2] = *(a02 + 26);
	      b[  3] = *(a02 + 27);
	      b[  4] = *(a03 + 26);
	      b[  5] = *(a03 + 27);
	      b[  6] = *(a04 + 26);
	      b[  7] = *(a04 + 27);
	      b[  8] = *(a05 + 26);
	      b[  9] = *(a05 + 27);
	      b[ 10] = *(a06 + 26);
	      b[ 11] = *(a06 + 27);
	      b[ 12] = *(a07 + 26);
	      b[ 13] = *(a07 + 27);
	      b[ 14] = *(a08 + 26);
	      b[ 15] = *(a08 + 27);
	      b[ 16] = *(a09 + 26);
	      b[ 17] = *(a09 + 27);
	      b[ 18] = *(a10 + 26);
	      b[ 19] = *(a10 + 27);
	      b[ 20] = *(a11 + 26);
	      b[ 21] = *(a11 + 27);
	      b[ 22] = *(a12 + 26);
	      b[ 23] = *(a12 + 27);
	      b[ 24] = *(a13 + 26);
	      b[ 25] = *(a13 + 27);
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
	      b[  0] = *(a01 + 28);
	      b[  1] = *(a01 + 29);
	      b[  2] = *(a02 + 28);
	      b[  3] = *(a02 + 29);
	      b[  4] = *(a03 + 28);
	      b[  5] = *(a03 + 29);
	      b[  6] = *(a04 + 28);
	      b[  7] = *(a04 + 29);
	      b[  8] = *(a05 + 28);
	      b[  9] = *(a05 + 29);
	      b[ 10] = *(a06 + 28);
	      b[ 11] = *(a06 + 29);
	      b[ 12] = *(a07 + 28);
	      b[ 13] = *(a07 + 29);
	      b[ 14] = *(a08 + 28);
	      b[ 15] = *(a08 + 29);
	      b[ 16] = *(a09 + 28);
	      b[ 17] = *(a09 + 29);
	      b[ 18] = *(a10 + 28);
	      b[ 19] = *(a10 + 29);
	      b[ 20] = *(a11 + 28);
	      b[ 21] = *(a11 + 29);
	      b[ 22] = *(a12 + 28);
	      b[ 23] = *(a12 + 29);
	      b[ 24] = *(a13 + 28);
	      b[ 25] = *(a13 + 29);
	      b[ 26] = *(a14 + 28);
	      b[ 27] = *(a14 + 29);
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
	a01 = a + posY * 2 + (posX + 0) * lda;
	a02 = a + posY * 2 + (posX + 1) * lda;
	a03 = a + posY * 2 + (posX + 2) * lda;
	a04 = a + posY * 2 + (posX + 3) * lda;
	a05 = a + posY * 2 + (posX + 4) * lda;
	a06 = a + posY * 2 + (posX + 5) * lda;
	a07 = a + posY * 2 + (posX + 6) * lda;
	a08 = a + posY * 2 + (posX + 7) * lda;
      } else {
	a01 = a + posX * 2 + (posY + 0) * lda;
	a02 = a + posX * 2 + (posY + 1) * lda;
	a03 = a + posX * 2 + (posY + 2) * lda;
	a04 = a + posX * 2 + (posY + 3) * lda;
	a05 = a + posX * 2 + (posY + 4) * lda;
	a06 = a + posX * 2 + (posY + 5) * lda;
	a07 = a + posX * 2 + (posY + 6) * lda;
	a08 = a + posX * 2 + (posY + 7) * lda;
      }

      i = (m >> 3);
      if (i > 0) {
	do {
	  if (X > posY) {
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
	    if (X < posY) {
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

	      b[ 16] = *(a01 +  2);
	      b[ 17] = *(a01 +  3);
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

	      b[ 32] = *(a01 +  4);
	      b[ 33] = *(a01 +  5);
	      b[ 34] = *(a02 +  4);
	      b[ 35] = *(a02 +  5);
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

	      b[ 48] = *(a01 +  6);
	      b[ 49] = *(a01 +  7);
	      b[ 50] = *(a02 +  6);
	      b[ 51] = *(a02 +  7);
	      b[ 52] = *(a03 +  6);
	      b[ 53] = *(a03 +  7);
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

	      b[ 64] = *(a01 +  8);
	      b[ 65] = *(a01 +  9);
	      b[ 66] = *(a02 +  8);
	      b[ 67] = *(a02 +  9);
	      b[ 68] = *(a03 +  8);
	      b[ 69] = *(a03 +  9);
	      b[ 70] = *(a04 +  8);
	      b[ 71] = *(a04 +  9);
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

	      b[ 80] = *(a01 + 10);
	      b[ 81] = *(a01 + 11);
	      b[ 82] = *(a02 + 10);
	      b[ 83] = *(a02 + 11);
	      b[ 84] = *(a03 + 10);
	      b[ 85] = *(a03 + 11);
	      b[ 86] = *(a04 + 10);
	      b[ 87] = *(a04 + 11);
	      b[ 88] = *(a05 + 10);
	      b[ 89] = *(a05 + 11);
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

	      b[ 96] = *(a01 + 12);
	      b[ 97] = *(a01 + 13);
	      b[ 98] = *(a02 + 12);
	      b[ 99] = *(a02 + 13);
	      b[100] = *(a03 + 12);
	      b[101] = *(a03 + 13);
	      b[102] = *(a04 + 12);
	      b[103] = *(a04 + 13);
	      b[104] = *(a05 + 12);
	      b[105] = *(a05 + 13);
	      b[106] = *(a06 + 12);
	      b[107] = *(a06 + 13);
#ifdef UNIT
	      b[108] = ONE;
	      b[109] = ZERO;
#else
	      b[108] = *(a07 + 12);
	      b[109] = *(a07 + 13);
#endif
	      b[110] = ZERO;
	      b[111] = ZERO;

	      b[112] = *(a01 + 14);
	      b[113] = *(a01 + 15);
	      b[114] = *(a02 + 14);
	      b[115] = *(a02 + 15);
	      b[116] = *(a03 + 14);
	      b[117] = *(a03 + 15);
	      b[118] = *(a04 + 14);
	      b[119] = *(a04 + 15);
	      b[120] = *(a05 + 14);
	      b[121] = *(a05 + 15);
	      b[122] = *(a06 + 14);
	      b[123] = *(a06 + 15);
	      b[124] = *(a07 + 14);
	      b[125] = *(a07 + 15);
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
	  if (X < posY) {
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
	      b += 16;

	    if (i >= 2) {
	    b[ 0] = *(a01 +  2);
		b[ 1] = *(a01 +  3);
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
		b[ 0] = *(a01 +  4);
		b[ 1] = *(a01 +  5);
		b[ 2] = *(a02 +  4);
		b[ 3] = *(a02 +  5);
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
		b[ 0] = *(a01 +  6);
		b[ 1] = *(a01 +  7);
		b[ 2] = *(a02 +  6);
		b[ 3] = *(a02 +  7);
		b[ 4] = *(a03 +  6);
		b[ 5] = *(a03 +  7);
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
		b[ 0] = *(a01 +  8);
		b[ 1] = *(a01 +  9);
		b[ 2] = *(a02 +  8);
		b[ 3] = *(a02 +  9);
		b[ 4] = *(a03 +  8);
		b[ 5] = *(a03 +  9);
		b[ 6] = *(a04 +  8);
		b[ 7] = *(a04 +  9);
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
		b[ 0] = *(a01 + 10);
		b[ 1] = *(a01 + 11);
		b[ 2] = *(a02 + 10);
		b[ 3] = *(a02 + 11);
		b[ 4] = *(a03 + 10);
		b[ 5] = *(a03 + 11);
		b[ 6] = *(a04 + 10);
		b[ 7] = *(a04 + 11);
		b[ 8] = *(a05 + 10);
		b[ 9] = *(a05 + 11);
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
		b[ 0] = *(a01 + 12);
		b[ 1] = *(a01 + 13);
		b[ 2] = *(a02 + 12);
		b[ 3] = *(a02 + 13);
		b[ 4] = *(a03 + 12);
		b[ 5] = *(a03 + 13);
		b[ 6] = *(a04 + 12);
		b[ 7] = *(a04 + 13);
		b[ 8] = *(a05 + 12);
		b[ 9] = *(a05 + 13);
		b[10] = *(a06 + 12);
		b[11] = *(a06 + 13);
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
	a01 = a + posY * 2 + (posX + 0) * lda;
	a02 = a + posY * 2 + (posX + 1) * lda;
	a03 = a + posY * 2 + (posX + 2) * lda;
	a04 = a + posY * 2 + (posX + 3) * lda;
      } else {
	a01 = a + posX * 2 + (posY + 0) * lda;
	a02 = a + posX * 2 + (posY + 1) * lda;
	a03 = a + posX * 2 + (posY + 2) * lda;
	a04 = a + posX * 2 + (posY + 3) * lda;
      }

      i = (m >> 2);
      if (i > 0) {
	do {
	  if (X > posY) {
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
	    if (X < posY) {
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
	      b[  2] = ZERO;
	      b[  3] = ZERO;
	      b[  4] = ZERO;
	      b[  5] = ZERO;
	      b[  6] = ZERO;
	      b[  7] = ZERO;

	      b[  8] = *(a01 +  2);
	      b[  9] = *(a01 +  3);
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

	      b[ 16] = *(a01 +  4);
	      b[ 17] = *(a01 +  5);
	      b[ 18] = *(a02 +  4);
	      b[ 19] = *(a02 +  5);
#ifdef UNIT
	      b[ 20] = ONE;
	      b[ 21] = ZERO;
#else
	      b[ 20] = *(a03 +  4);
	      b[ 21] = *(a03 +  5);
#endif
	      b[ 22] = ZERO;
	      b[ 23] = ZERO;

	      b[ 24] = *(a01 +  6);
	      b[ 25] = *(a01 +  7);
	      b[ 26] = *(a02 +  6);
	      b[ 27] = *(a02 +  7);
	      b[ 28] = *(a03 +  6);
	      b[ 29] = *(a03 +  7);
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
      if (i) {

	if (X > posY) {

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
	  if (X < posY) {
	    /* a01 += i * lda;
	    a02 += i * lda;
	    a03 += i * lda;
	    a04 += i * lda; */
	    b += 8 * i;
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
	    b += 8;

	    if (i >= 2) {
	      b[ 0] = *(a01 +  2);
	      b[ 1] = *(a01 +  3);
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
	      b[ 0] = *(a01 +  4);
	      b[ 1] = *(a01 +  5);
	      b[ 2] = *(a02 +  4);
	      b[ 3] = *(a02 +  5);
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
	a01 = a + posY * 2 + (posX + 0) * lda;
	a02 = a + posY * 2 + (posX + 1) * lda;
      } else {
	a01 = a + posX * 2 + (posY + 0) * lda;
	a02 = a + posX * 2 + (posY + 1) * lda;
      }

      i = (m >> 1);
      if (i > 0) {
	do {
	  if (X > posY) {
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
	    if (X < posY) {
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

	      b[  4] = *(a01 +  2);
	      b[  5] = *(a01 +  3);
#ifdef UNIT
	      b[  6] = ONE;
	      b[  7] = ZERO;
#else
	      b[  6] = *(a02 +  2);
	      b[  7] = *(a02 +  3);
#endif
	      a01 += 4;
	      a02 += 4;
	      b += 8;
	    }

	  X += 2;
	  i --;
	} while (i > 0);
      }

      if (m & 1) {

	if (X > posY) {
	  b[  0] = *(a01 +  0);
	  b[  1] = *(a01 +  1);
	  b[  2] = *(a02 +  0);
	  b[  3] = *(a02 +  1);
	  /* a01 += 2;
	  a02 += 2; */
	  b += 4;
	} else
	  if (X < posY) {
	    /* a01 += 2 * lda;
	    a02 += 2 * lda; */
	    b += 4;
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
	    b += 4;
	  }
      }
      posY += 2;
  }

  if (n & 1){
      X = posX;

      if (posX <= posY) {
	a01 = a + posY * 2 + (posX + 0) * lda;
      } else {
	a01 = a + posX * 2 + (posY + 0) * lda;
      }

      i = m;
      if (m > 0) {
	do {
	  if (X > posY) {
	    b[  0] = *(a01 +  0);
	    b[  1] = *(a01 +  1);
	    a01 += 2;
	    b += 2;
	  } else
	    if (X < posY) {
	      a01 += lda;
	      b += 2;
	    } else {
#ifdef UNIT
	      b[  0] = ONE;
	      b[  1] = ZERO;
#else
	      b[  0] = *(a01 +  0);
	      b[  1] = *(a01 +  1);
#endif
	      a01 += 2;
	      b += 2;
	    }

	  X += 1;
	  i --;
	} while (i > 0);
      }
  }

  return 0;
}
