/*******************************************************************************
Copyright (c) 2023, The OpenBLAS Project
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
#define ASSEMBLER

#include "common.h"
#include "loongarch64_asm.S"

/*********************************************************************
* 2023/07/26 guxiwei
*        UTEST                  : OK
*        CTEST                  : OK
*        TEST                   : OK
*
*
*********************************************************************/

/* int CNAME(BLASLONG m, BLASLONG n, BLASLONG k, FLOAT dummy1, FLOAT *a, FLOAT *b,
 *          FLOAT *c, BLASLONG ldc, BLASLONG offset)
 */
#define M      $r4   // param 1: bm
#define N      $r5   // param 2: bn
#define K      $r6   // param 3: bk
#define A      $r7   // param 5: ba
#define B      $r8   // param 6: bb
#define C      $r9   // param 7: bc
#define LDC    $r10  // param 8: ldc
#define OFFSET $r11  // param 9: offset

/* Cycle control parameters */
#define I      $r13
#define J      $r14
#define L      $r15
#define TL     $r16
/* Matrix address */
#define A0     $r17
#define B0     $r18
#define C0     $r19
#define C1     $r20
#define C2     $r23
#define C3     $r24
#define T0     $r25
#define T1     $r26
#define T2     $r27
#define KK     $r28
#define AA     $r29
#define CC     $r30
#undef  ZERO
#define ZERO   $r0

#define U0     $xr0
#define U1     $xr1
#define U2     $xr2
#define U3     $xr3
#define U4     $xr4
#define U5     $xr5
#define U6     $xr6
#define U7     $xr7
#define U8     $xr8
#define U9     $xr9
#define U10    $xr10
#define U11    $xr11
#define U12    $xr12
#define U13    $xr13
#define U14    $xr14
#define U15    $xr15
#define D0     $xr16
#define D1     $xr17
#define D2     $xr18
#define D3     $xr19
#define D4     $xr20
#define D5     $xr21
#define D6     $xr22
#define D7     $xr23
#define D8     $xr24
#define D9     $xr25
#define D10    $xr26
#define D11    $xr27
#define D12    $xr28
#define D13    $xr29
#define D14    $xr30
#define D15    $xr31

/* Prefetch interval */
#define A_PRE  0x400
#define B_PRE  0x100

#include "dtrsm_kernel_macro.S"

// By integrating the dgemm and dsolve processes, the following advantages can be obtained:
// 1. Avoid the overhead of function calls (by not invoking dgemm_kernel)
// 2. Reduce the storage and retrieval of C data
// 3. Vectorization of dsolve
// GEMM_UNROLL_M x DGEMM_UNROLL_N is 16x4, which is a fairly large size.
// To achieve finer-grained optimization, 15 scenarios have been addressed:
// 16x4, 16x2, 16x1, 8x4, 8x2, 8x1, 4x4, 4x2, 4x1, 2x4, 2x2, 2x1, 1x4, 1x2, 1x1.

.macro dsolve_16 N
// if N = 4 the data layout of C is as follows:
// U0  U1  U2  U3
// U4  U5  U6  U7
// U8  U9  U10 U11
// U12 U13 U14 U15
// if N = 2 the dat layout of C is as follows:
// U0 U1 U2 U3
// U4 U5 U6 U7
// if N = 1 the dat layout of C is as follows:
// U0 U1 U2 U3
// The matrix A has dimensions of 16x16, and
// it will be divided into 4 segments for processing.

#define G12 U3
#define G13 U7
#define G14 U11
#define G15 U15
    GTRANSPOSE4x4_D U3, U7, U11, U15, G12, G13, G14, G15, D0, D1
    // A
    // G12 G13 G14 G15
    // -----------------
    // 204             | D9
    // 220 221         | D8 D7
    // 236 237 238     | D6 D5 D4
    // 252 253 254 255 | D3 D2 D1 D0
    PTR_ADDI T0, A0, 252 * 8
    GLDREPL xv, d, D3, T0, 0, D2, T0, 1 * 8, D1, T0, 2 * 8, D0, T0, 3 * 8
    PTR_ADDI T0, A0, 236 * 8
    GLDREPL xv, d, D6, T0, 0, D5, T0, 1 * 8, D4, T0, 2 * 8
    PTR_ADDI T0, A0, 220 * 8
    GLDREPL xv, d, D8, T0, 0, D7, T0, 1 * 8
    PTR_ADDI T0, A0, 204 * 8
    GLDREPL xv, d, D9, T0, 0

    xvfmul.d    G15,    G15,    D0
    GNMSUB xvf, d, G14, G15, D1, G14
    xvfmul.d    G14,    G14,    D4
    GNMSUB xvf, d, G13, G15, D2, G13, G13, G14, D5, G13
    xvfmul.d    G13,    G13,    D7
    GNMSUB xvf, d, G12, G15, D3, G12, G12, G14, D6, G12, G12, G13, D8, G12
    xvfmul.d    G12,    G12,    D9
    // Store B
.if \N == 4
    // x x x x ... x x x x
    // x x x x ... x x x x
    // x x x x ... x x x x
    // b48 b49 b50 b51 ... b60 b61 b62 b63
    GST xv, , G12, B0, 48 * 8, G13, B0, 52 * 8, G14, B0, 56 * 8, G15, B0, 60 * 8
.elseif \N == 2
    // x x x x ... x x x x
    // x x x x ... x x x x
    // x x x x ... x x x x
    // b24 b25 b26 b27 b28 b29 b30 b31
    GST v, , $vr3, B0, 24 * 8, $vr7, B0, 26 * 8, $vr11, B0, 28 * 8, $vr15, B0, 30 * 8
.elseif \N == 1
    // x x x x
    // x x x x
    // x x x x
    // b12 b13 b14 b15
    GST f, d, $f3, B0, 12 * 8, $f7, B0, 13 * 8, $f11, B0, 14 * 8, $f15, B0, 15 * 8
.endif
    // Transpose G15 G14 G13 G12
    GTRANSPOSE4x4_D G12, G13, G14, G15, D0, D1, D2, D3, D4, D5
    // Store C
.if \N == 4
    // x x x x ... c12 c13 c14 c15
    // x x x x ... c28 c29 c30 c31
    // x x x x ... c44 c45 c46 c47
    // x x x x ... c60 c61 c62 c63
    GST xv, , D0, C0, 12 * 8, D1, C1, 12 * 8, D2, C2, 12 * 8, D3, C3, 12 * 8
.elseif \N == 2
    // x x x x ... c12 c13 c14 c15
    // x x x x ... c28 c29 c30 c31
    GST xv, , D0, C0, 12 * 8, D1, C1, 12 * 8
.elseif \N == 1
    // Store C
    // x x x x ... c12 c13 c14 c15
    GST xv, , D0, C0, 12 * 8
.endif

#define G8  U2
#define G9  U6
#define G10 U10
#define G11 U14
    GTRANSPOSE4x4_D U2, U6, U10, U14, G8, G9, G10, G11, D0, D1
    // A
    // G8  G9  G10  G11
    // -----------------
    // 136             | D9
    // 152 153         | D8  D7
    // 168 169 170     | D6  D5  D4
    // 184 185 186 187 | D3  D2  D1  D0
    // 200 201 202 203 | D15 D14 D13 D12
    // 216 217 218 219 | D11 D10 D9  D8
    // 232 233 234 235 | D7  D6  D5  D4
    // 248 249 250 251 | D3  D2  D1  D0
    PTR_ADDI  T0, A0, 248 * 8
    GLDREPL xv, d, D3, T0, 0, D2, T0, 1 * 8, D1, T0, 2 * 8, D0, T0, 3 * 8
    PTR_ADDI  T0, A0, 232 * 8
    GLDREPL xv, d, D7, T0, 0, D6, T0, 1 * 8, D5, T0, 2 * 8, D4, T0, 3 * 8
    PTR_ADDI  T0, A0, 216 * 8
    GLDREPL xv, d, D11, T0, 0, D10, T0, 1 * 8, D9, T0, 2 * 8, D8, T0, 3 * 8
    PTR_ADDI  T0, A0, 200 * 8
    GLDREPL xv, d, D15, T0, 0, D14, T0, 1 * 8, D13, T0, 2 * 8, D12, T0, 3 * 8
    GNMSUB xvf, d, G11, G15, D0,  G11, G10, G15, D1,  G10, G9, G15, D2,  G9, G8, G15, D3,  G8, \
                   G11, G14, D4,  G11, G10, G14, D5,  G10, G9, G14, D6,  G9, G8, G14, D7,  G8, \
                   G11, G13, D8,  G11, G10, G13, D9,  G10, G9, G13, D10, G9, G8, G13, D11, G8, \
                   G11, G12, D12, G11, G10, G12, D13, G10, G9, G12, D14, G9, G8, G12, D15, G8
    PTR_ADDI T0, A0, 184 * 8
    GLDREPL xv, d, D3, T0, 0, D2, T0, 1 * 8, D1, T0, 2 * 8, D0, T0, 3 * 8
    PTR_ADDI T0, A0, 168 * 8
    GLDREPL xv, d, D6, T0, 0, D5, T0, 1 * 8, D4, T0, 2 * 8
    PTR_ADDI T0, A0, 152 * 8
    GLDREPL xv, d, D8, T0, 0, D7, T0, 1 * 8
    PTR_ADDI T0, A0, 136 * 8
    GLDREPL xv, d, D9, T0, 0

    xvfmul.d    G11,    G11,    D0
    GNMSUB xvf, d, G10, G11, D1, G10, G9, G11, D2, G9, G8, G11, D3, G8
    xvfmul.d    G10,    G10,    D4
    GNMSUB xvf, d, G9, G10, D5, G9, G8, G10, D6, G8
    xvfmul.d    G9,     G9,     D7
    GNMSUB xvf, d, G8, G9, D8, G8
    xvfmul.d    G8,     G8,     D9
    // Store B
.if \N == 4
    // x x x x ... x x x x
    // x x x x ... x x x x
    // b32 b33 b34 b34 ... b44 b45 b46 b47
    // b48 b49 b50 b51 ... b60 b61 b62 b63
    GST xv, , G8, B0, 32 * 8, G9, B0, 36 * 8, G10, B0, 40 * 8, G11, B0, 44 * 8
.elseif \N == 2
    // x x x x ... x x x x
    // x x x x ... x x x x
    // b16 b17 b18 b19 b20 b21 b22 b23
    // b24 b25 b26 b27 b28 b29 b30 b31
    GST v, , $vr2, B0, 16 * 8, $vr6, B0, 18 * 8, $vr10, B0, 20 * 8, $vr14, B0, 22 * 8
.elseif \N == 1
    // x x x x
    // x x x x
    // b8  b9  b10 b11
    // b12 b13 b14 b15
    GST f, d, $f2, B0, 8 * 8, $f6, B0, 9 * 8, $f10, B0, 10 * 8, $f14, B0, 11 * 8
.endif
    // Transpose G11 G10 G9 G8
    GTRANSPOSE4x4_D G8, G9, G10, G11, D0, D1, D2, D3, D4, D5
    // Store C
.if \N == 4
    // x x x x ... c8  c9  c10 c11 c12 c13 c14 c15
    // x x x x ... c24 c25 c26 c27 c28 c29 c30 c31
    // x x x x ... c40 c41 c42 c43 c44 c45 c46 c47
    // x x x x ... c56 c57 c58 c59 c60 c61 c62 c63
    GST xv, , D0, C0, 8 * 8, D1, C1, 8 * 8, D2, C2, 8 * 8, D3, C3, 8 * 8
.elseif \N == 2
    // x x x x ... c8  c9  c10 c11 c12 c13 c14 c15
    // x x x x ... c24 c25 c26 c27 c28 c29 c30 c31
    GST xv, , D0, C0, 8 * 8, D1, C1, 8 * 8
.elseif \N == 1
    // x x x x ... c8  c9  c10 c11 c12 c13 c14 c15
    GST xv, , D0, C0, 8 * 8
.endif

#define G4 U1
#define G5 U5
#define G6 U9
#define G7 U13
    GTRANSPOSE4x4_D U1, U5, U9, U13, G4, G5, G6, G7, D0, D1
    // A
    // G4   G5  G6   G7
    // ------------------
    // 68               | D9
    // 84	85          | D8  D7
    // 100	101	102     | D6  D5  D4
    // 116	117	118	119 | D3  D2  D1  D0
    // 132	133	134	135 | D15 D14 D13 D12
    // 148	149	150	151 | D11 D10 D9  D8
    // 164	165	166	167 | D7  D6  D5  D4
    // 180	181	182	183 | D3  D2  D1  D0
    // 196	197	198	199 | D15 D14 D13 D12
    // 212	213	214	215 | D11 D10 D9  D8
    // 228	229	230	231 | D7  D6  D5  D4
    // 244	245	246	247 | D3  D2  D1  D0
    PTR_ADDI  T0, A0, 244 * 8
    GLDREPL xv, d, D3, T0, 0, D2, T0, 1 * 8, D1, T0, 2 * 8, D0, T0, 3 * 8
    PTR_ADDI  T0, A0, 228 * 8
    GLDREPL xv, d, D7, T0, 0, D6, T0, 1 * 8, D5, T0, 2 * 8, D4, T0, 3 * 8
    PTR_ADDI  T0, A0, 212 * 8
    GLDREPL xv, d, D11, T0, 0, D10, T0, 1 * 8, D9, T0, 2 * 8, D8, T0, 3 * 8
    PTR_ADDI  T0, A0, 196 * 8
    GLDREPL xv, d, D15, T0, 0, D14, T0, 1 * 8, D13, T0, 2 * 8, D12, T0, 3 * 8
    GNMSUB xvf, d, G7, G15, D0,  G7, G6, G15, D1,  G6, G5, G15, D2,  G5, G4, G15, D3,  G4, \
                   G7, G14, D4,  G7, G6, G14, D5,  G6, G5, G14, D6,  G5, G4, G14, D7,  G4, \
                   G7, G13, D8,  G7, G6, G13, D9,  G6, G5, G13, D10, G5, G4, G13, D11, G4, \
                   G7, G12, D12, G7, G6, G12, D13, G6, G5, G12, D14, G5, G4, G12, D15, G4
    PTR_ADDI  T0, A0, 180 * 8
    GLDREPL xv, d, D3, T0, 0, D2, T0, 1 * 8, D1, T0, 2 * 8, D0, T0, 3 * 8
    PTR_ADDI  T0, A0, 164 * 8
    GLDREPL xv, d, D7, T0, 0, D6, T0, 1 * 8, D5, T0, 2 * 8, D4, T0, 3 * 8
    PTR_ADDI  T0, A0, 148 * 8
    GLDREPL xv, d, D11, T0, 0, D10, T0, 1 * 8, D9, T0, 2 * 8, D8, T0, 3 * 8
    PTR_ADDI  T0, A0, 132 * 8
    GLDREPL xv, d, D15, T0, 0, D14, T0, 1 * 8, D13, T0, 2 * 8, D12, T0, 3 * 8
    GNMSUB xvf, d, G7, G11, D0,  G7, G6, G11, D1,  G6, G5, G11, D2,  G5, G4, G11, D3,  G4, \
                   G7, G10, D4,  G7, G6, G10, D5,  G6, G5, G10, D6,  G5, G4, G10, D7,  G4, \
                   G7, G9,  D8,  G7, G6, G9,  D9,  G6, G5, G9,  D10, G5, G4, G9,  D11, G4, \
                   G7, G8,  D12, G7, G6, G8,  D13, G6, G5, G8,  D14, G5, G4, G8,  D15, G4
    PTR_ADDI T0, A0, 116 * 8
    GLDREPL xv, d, D3, T0, 0, D2, T0, 1 * 8, D1, T0, 2 * 8, D0, T0, 3 * 8
    PTR_ADDI T0, A0, 100 * 8
    GLDREPL xv, d, D6, T0, 0, D5, T0, 1 * 8, D4, T0, 2 * 8
    PTR_ADDI T0, A0, 84 * 8
    GLDREPL xv, d, D8, T0, 0, D7, T0, 1 * 8
    PTR_ADDI T0, A0, 68 * 8
    GLDREPL xv, d, D9, T0, 0
    xvfmul.d    G7,     G7,     D0
    GNMSUB xvf, d, G6, G7, D1, G6, G5, G7, D2, G5, G4, G7, D3, G4
    xvfmul.d    G6,     G6,     D4
    GNMSUB xvf, d, G5, G6, D5, G5, G4, G6, D6, G4
    xvfmul.d    G5,     G5,     D7
    GNMSUB xvf, d, G4, G5, D8, G4
    xvfmul.d    G4,     G4,     D9
    // Store B
.if \N == 4
    // x x x x ... x x x x
    // b16 b17 b18 b19 ... b28 b29 b30 b31
    // b32 b33 b34 b34 ... b44 b45 b46 b47
    // b48 b49 b50 b51 ... b60 b61 b62 b63
    GST xv, , G4, B0, 16 * 8, G5, B0, 20 * 8, G6, B0, 24 * 8, G7, B0, 28 * 8
.elseif \N == 2
    // x x x x ... x x x x
    // b8  b9  b10 b11 b12 b13 b14 b15
    // b16 b17 b18 b19 b20 b21 b22 b23
    // b24 b25 b26 b27 b28 b29 b30 b31
    GST v, , $vr1, B0, 8 * 8, $vr5, B0, 10 * 8, $vr9, B0, 12 * 8, $vr13, B0, 14 * 8
.elseif \N == 1
    // x x x x
    // b4  b5  b6  b7
    // b8  b9  b10 b11
    // b12 b13 b14 b15
    GST f, d, $f1, B0, 4 * 8, $f5, B0, 5 * 8, $f9, B0, 6 * 8, $f13, B0, 7 * 8
.endif
    // Transpose G7 G6 G5 G4
    GTRANSPOSE4x4_D G4, G5, G6, G7, D0, D1, D2, D3, D4, D5
    // Store C
.if \N == 4
    // x x x x c4  c5  c6  c7  c8  c9  c10 c11 c12 c13 c14 c15
    // x x x x c20 c21 c22 c23 c24 c25 c26 c27 c28 c29 c30 c31
    // x x x x c36 c37 c38 c39 c40 c41 c42 c43 c44 c45 c46 c47
    // x x x x c52 c53 c54 c55 c56 c57 c58 c59 c60 c61 c62 c63
    GST xv, , D0, C0, 4 * 8, D1, C1, 4 * 8, D2, C2, 4 * 8, D3, C3, 4 * 8
.elseif \N == 2
    // x x x x c4  c5  c6  c7  c8  c9  c10 c11 c12 c13 c14 c15
    // x x x x c20 c21 c22 c23 c24 c25 c26 c27 c28 c29 c30 c31
    GST xv, , D0, C0, 4 * 8, D1, C1, 4 * 8
.elseif \N == 1
    // x x x x c4  c5  c6  c7  c8  c9  c10 c11 c12 c13 c14 c15
    GST xv, , D0, C0, 4 * 8
.endif

#define G0 U0
#define G1 U4
#define G2 U8
#define G3 U12
    GTRANSPOSE4x4_D U0, U4, U8, U12, G0, G1, G2, G3, D0, D1
    // A
    // G0   G1  G2   G3
    // ------------------
    // 0                 | D9
    // 16	17           | D8  D7
    // 32	33	34       | D6  D5  D4
    // 48	49	50	51   | D3  D2  D1  D0
    // 64	65	66	67   | D15 D14 D13 D12
    // 80	81	82	83   | D11 D10 D9  D8
    // 96	97	98	99   | D7  D6  D5  D4
    // 112	113	114	115  | D3  D2  D1  D0
    // 128	129	130	131  | D15 D14 D13 D12
    // 144	145	146	147  | D11 D10 D9  D8
    // 160	161	162	163  | D7  D6  D5  D4
    // 176	177	178	179  | D3  D2  D1  D0
    // 192	193	194	195  | D15 D14 D13 D12
    // 208	209	210	211  | D11 D10 D9  D8
    // 224	225	226	227  | D7  D6  D5  D4
    // 240	241	242	243  | D3  D2  D1  D0
    PTR_ADDI  T0, A0, 240 * 8
    GLDREPL xv, d, D3, T0, 0, D2, T0, 1 * 8, D1, T0, 2 * 8, D0, T0, 3 * 8
    PTR_ADDI  T0, A0, 224 * 8
    GLDREPL xv, d, D7, T0, 0, D6, T0, 1 * 8, D5, T0, 2 * 8, D4, T0, 3 * 8
    PTR_ADDI  T0, A0, 208 * 8
    GLDREPL xv, d, D11, T0, 0, D10, T0, 1 * 8, D9, T0, 2 * 8, D8, T0, 3 * 8
    PTR_ADDI  T0, A0, 192 * 8
    GLDREPL xv, d, D15, T0, 0, D14, T0, 1 * 8, D13, T0, 2 * 8, D12, T0, 3 * 8
    GNMSUB xvf, d, G3, G15, D0,  G3, G2, G15, D1,  G2, G1, G15, D2,  G1, G0, G15, D3,  G0, \
                   G3, G14, D4,  G3, G2, G14, D5,  G2, G1, G14, D6,  G1, G0, G14, D7,  G0, \
                   G3, G13, D8,  G3, G2, G13, D9,  G2, G1, G13, D10, G1, G0, G13, D11, G0, \
                   G3, G12, D12, G3, G2, G12, D13, G2, G1, G12, D14, G1, G0, G12, D15, G0
    PTR_ADDI  T0, A0, 176 * 8
    GLDREPL xv, d, D3, T0, 0, D2, T0, 1 * 8, D1, T0, 2 * 8, D0, T0, 3 * 8
    PTR_ADDI  T0, A0, 160 * 8
    GLDREPL xv, d, D7, T0, 0, D6, T0, 1 * 8, D5, T0, 2 * 8, D4, T0, 3 * 8
    PTR_ADDI  T0, A0, 144 * 8
    GLDREPL xv, d, D11, T0, 0, D10, T0, 1 * 8, D9, T0, 2 * 8, D8, T0, 3 * 8
    PTR_ADDI  T0, A0, 128 * 8
    GLDREPL xv, d, D15, T0, 0, D14, T0, 1 * 8, D13, T0, 2 * 8, D12, T0, 3 * 8
    GNMSUB xvf, d, G3, G11, D0,  G3, G2, G11, D1,  G2, G1, G11, D2,  G1, G0, G11, D3,  G0, \
                   G3, G10, D4,  G3, G2, G10, D5,  G2, G1, G10, D6,  G1, G0, G10, D7,  G0, \
                   G3, G9,  D8,  G3, G2, G9,  D9,  G2, G1, G9,  D10, G1, G0,  G9, D11, G0, \
                   G3, G8,  D12, G3, G2, G8,  D13, G2, G1, G8,  D14, G1, G0,  G8, D15, G0
    PTR_ADDI  T0, A0, 112 * 8
    GLDREPL xv, d, D3, T0, 0, D2, T0, 1 * 8, D1, T0, 2 * 8, D0, T0, 3 * 8
    PTR_ADDI  T0, A0, 96 * 8
    GLDREPL xv, d, D7, T0, 0, D6, T0, 1 * 8, D5, T0, 2 * 8, D4, T0, 3 * 8
    PTR_ADDI  T0, A0, 80 * 8
    GLDREPL xv, d, D11, T0, 0, D10, T0, 1 * 8, D9, T0, 2 * 8, D8, T0, 3 * 8
    PTR_ADDI  T0, A0, 64 * 8
    GLDREPL xv, d, D15, T0, 0, D14, T0, 1 * 8, D13, T0, 2 * 8, D12, T0, 3 * 8
    GNMSUB xvf, d, G3, G7, D0,  G3, G2, G7, D1,  G2, G1, G7, D2,  G1, G0, G7, D3,  G0, \
                   G3, G6, D4,  G3, G2, G6, D5,  G2, G1, G6, D6,  G1, G0, G6, D7,  G0, \
                   G3, G5, D8,  G3, G2, G5, D9,  G2, G1, G5, D10, G1, G0, G5, D11, G0, \
                   G3, G4, D12, G3, G2, G4, D13, G2, G1, G4, D14, G1, G0, G4, D15, G0
    PTR_ADDI T0, A0, 48 * 8
    GLDREPL xv, d, D3, T0, 0, D2, T0, 1 * 8, D1, T0, 2 * 8, D0, T0, 3 * 8
    PTR_ADDI T0, A0, 32 * 8
    GLDREPL xv, d, D6, T0, 0, D5, T0, 1 * 8, D4, T0, 2 * 8
    PTR_ADDI T0, A0, 16 * 8
    GLDREPL xv, d, D8, T0, 0, D7, T0, 1 * 8
    PTR_ADDI T0, A0, 0 * 8
    GLDREPL xv, d, D9, T0, 0

    xvfmul.d    G3,     G3,     D0
    GNMSUB xvf, d, G2, G3, D1, G2, G1, G3, D2, G1, G0, G3, D3, G0
    xvfmul.d    G2,     G2,     D4
    GNMSUB xvf, d, G1, G2, D5, G1, G0, G2, D6, G0
    xvfmul.d    G1,     G1,     D7
    GNMSUB  xvf, d, G0, G1, D8, G0
    xvfmul.d    G0,     G0,     D9
    // Store B
.if \N == 4
    // b0  b1  b2  b3  ... b12 b13 b14 b15
    // b16 b17 b18 b19 ... b28 b29 b30 b31
    // b32 b33 b34 b34 ... b44 b45 b46 b47
    // b48 b49 b50 b51 ... b60 b61 b62 b63
    GST xv, , G0, B0, 0, G1, B0, 4 * 8, G2, B0, 8 * 8, G3, B0, 12 * 8
.elseif \N == 2
    // b0  b1  b2  b3  b4  b5  b6  b7
    // b8  b9  b10 b11 b12 b13 b14 b15
    // b16 b17 b18 b19 b20 b21 b22 b23
    // b24 b25 b26 b27 b28 b29 b30 b31
    GST v, , $vr0, B0, 0, $vr4, B0, 2 * 8, $vr8, B0, 4 * 8, $vr12, B0, 6 * 8
.elseif \N == 1
    // b0  b1  b2  b3
    // b4  b5  b6  b7
    // b8  b9  b10 b11
    // b12 b13 b14 b15
    GST f, d, $f0, B0, 0, $f4, B0, 1 * 8, $f8, B0, 2 * 8, $f12, B0, 3 * 8
.endif
    // Transpose C3 C2 C1 C0
    GTRANSPOSE4x4_D G0, G1, G2, G3, D0, D1, D2, D3, D4, D5
    // Store C
.if \N == 4
    // c0  c1  c2  c3  ... c12 c13 c14 c15
    // c16 c17 c18 c19 ... c28 c29 c30 c31
    // c32 c33 c34 c34 ... c44 c45 c46 c47
    // c48 c49 c50 c51 ... c60 c61 c62 c63
    GST xv, , D0, C0, 0, D1, C1, 0, D2, C2, 0, D3, C3, 0
.elseif \N == 2
    // c0  c1  c2  c3  ... c12 c13 c14 c15
    // c16 c17 c18 c19 ... c28 c29 c30 c31
    GST xv, , D0, C0, 0, D1, C1, 0
.elseif \N == 1
    // c0  c1  c2  c3  ... c12 c13 c14 c15
    GST xv, , D0, C0, 0
.endif

#undef G0
#undef G1
#undef G2
#undef G3
#undef G4
#undef G5
#undef G6
#undef G7
#undef G8
#undef G9
#undef G10
#undef G11
#undef G12
#undef G13
#undef G14
#undef G15
.endm

.macro dsolve_8 N
// if N = 4 the data layout of C is as follows:
// U0  U1
// U2  U3
// U4  U5
// U6  U7
// if N = 2 the dat layout of C is as follows:
// U0  U1
// U2  U3
// if N = 1 the dat layout of C is as follows:
// U0  U1
// The matrix A has dimensions of 8x8, and
// it will be divided into 2 segments for processing.

#define G4 U1
#define G5 U3
#define G6 U5
#define G7 U7
    // Transpose U7 U5 U3 U1
    GTRANSPOSE4x4_D U1, U3, U5, U7, G4, G5, G6, G7, D0, D1
    // A
    // G4   G5  G6  G7
    // ---------------
    // 36              | D9
    // 44	45         | D8 D7
    // 52	53	54     | D6 D5 D4
    // 60	61	62	63 | D3 D2 D1 D0
    PTR_ADDI      T0,     A0,     60 * 8
    GLDREPL xv, d, D3, T0, 0, D2, T0, 1 * 8, D1, T0, 2 * 8, D0, T0, 3 * 8
    PTR_ADDI      T0,     A0,     52 * 8
    GLDREPL xv, d, D6, T0, 0, D5, T0, 1 * 8, D4, T0, 2 * 8
    PTR_ADDI      T0,     A0,     44 * 8
    GLDREPL xv, d, D8, T0, 0, D7, T0, 1 * 8
    PTR_ADDI      T0,     A0,     36 * 8
    GLDREPL xv, d, D9, T0, 0

    xvfmul.d    G7,     G7,     D0
    GNMSUB xvf, d, G6, G7, D1, G6, G5, G7, D2, G5, G4, G7, D3, G4
    xvfmul.d    G6,     G6,     D4
    GNMSUB xvf, d, G5, G6, D5, G5, G4, G6, D6, G4
    xvfmul.d    G5,     G5,     D7
    GNMSUB xvf, d, G4, G5, D8, G4
    xvfmul.d    G4,     G4,     D9
    // Store B
.if \N == 4
    GST xv, , G4, B0, 16 * 8, G5, B0, 20 * 8, G6, B0, 24 * 8, G7, B0, 28 * 8
.elseif \N == 2
    GST v, , $vr1, B0, 8 * 8, $vr3, B0, 10 * 8, $vr5, B0, 12 * 8, $vr7, B0, 14 * 8
.elseif \N == 1
    GST f, d, $f1, B0, 4 * 8, $f3, B0, 5 * 8, $f5, B0, 6 * 8, $f7, B0, 7 * 8
.endif
    // Transpose
    GTRANSPOSE4x4_D G4, G5, G6, G7, D4, D5, D6, D7, D8, D9
    // Store C
.if \N == 4
    GST xv, , D4, C0, 4 * 8, D5, C1, 4 * 8, D6, C2, 4 * 8, D7, C3, 4 * 8
.elseif \N == 2
    GST xv, , D4, C0, 4 * 8, D5, C1, 4 * 8
.elseif \N == 1
    GST xv, , D4, C0, 4 * 8
.endif

#define G0 U0
#define G1 U2
#define G2 U4
#define G3 U6
    // Transpose U6 U4 U2 U0
    GTRANSPOSE4x4_D U0, U2, U4, U6, G0, G1, G2, G3, D0, D1
    // A
    // G0  G1   G2  G3
    //-----------------
    // 0               | D9
    // 8	9          | D8  D7
    // 16	17	18     | D6  D5  D4
    // 24	25	26	27 | D3  D2  D1  D0
    // 32	33	34	35 | D15 D14 D13 D12
    // 40	41	42	43 | D11 D10 D9  D8
    // 48	49	50	51 | D7  D6  D5  D4
    // 56	57	58	59 | D3  D2  D1  D0
    PTR_ADDI  T0, A0, 56 * 8
    GLDREPL xv, d, D3, T0, 0, D2, T0, 1 * 8, D1, T0, 2 * 8, D0, T0, 3 * 8
    PTR_ADDI  T0, A0, 48 * 8
    GLDREPL xv, d, D7, T0, 0, D6, T0, 1 * 8, D5, T0, 2 * 8, D4, T0, 3 * 8
    PTR_ADDI  T0, A0, 40 * 8
    GLDREPL xv, d, D11, T0, 0, D10, T0, 1 * 8, D9, T0, 2 * 8, D8, T0, 3 * 8
    PTR_ADDI  T0, A0, 32 * 8
    GLDREPL xv, d, D15, T0, 0, D14, T0, 1 * 8, D13, T0, 2 * 8, D12, T0, 3 * 8
    GNMSUB xvf, d, G3, G7, D0,  G3, G2, G7, D1,  G2, G1, G7, D2,  G1, G0, G7, D3,  G0, \
                   G3, G6, D4,  G3, G2, G6, D5,  G2, G1, G6, D6,  G1, G0, G6, D7,  G0, \
                   G3, G5, D8,  G3, G2, G5, D9,  G2, G1, G5, D10, G1, G0, G5, D11, G0, \
                   G3, G4, D12, G3, G2, G4, D13, G2, G1, G4, D14, G1, G0, G4, D15, G0
    PTR_ADDI T0, A0, 24 * 8
    GLDREPL xv, d, D3, T0, 0, D2, T0, 1 * 8, D1, T0, 2 * 8, D0, T0, 3 * 8
    PTR_ADDI T0, A0, 16 * 8
    GLDREPL xv, d, D6, T0, 0, D5, T0, 1 * 8, D4, T0, 2 * 8
    PTR_ADDI T0, A0, 8 * 8
    GLDREPL xv, d, D8, T0, 0, D7, T0, 1 * 8
    PTR_ADDI T0, A0, 0 * 8
    GLDREPL xv, d, D9, T0, 0

    xvfmul.d    G3,     G3,     D0
    GNMSUB xvf, d, G2, G3, D1, G2, G1, G3, D2, G1, G0, G3, D3, G0
    xvfmul.d    G2,     G2,     D4
    GNMSUB xvf, d, G1, G2, D5, G1, G0, G2, D6, G0
    xvfmul.d    G1,     G1,     D7
    GNMSUB xvf, d, G0, G1, D8, G0
    xvfmul.d    G0,     G0,     D9
    // Store B
.if \N == 4
    GST xv, , G0, B0, 0, G1, B0, 4 * 8, G2, B0, 8 * 8, G3, B0, 12 * 8
.elseif \N == 2
    GST v, , $vr0, B0, 0, $vr2, B0, 2 * 8, $vr4, B0, 4 * 8, $vr6, B0, 6 * 8
.elseif \N == 1
    GST f, d, $f0, B0, 0, $f2, B0, 1 * 8, $f4, B0, 2 * 8, $f6, B0, 3 * 8
.endif
    // Transpose
    GTRANSPOSE4x4_D G0, G1, G2, G3, D0, D1, D2, D3, D4, D5
    // Store C
.if \N == 4
    GST xv, , D0, C0, 0, D1, C1, 0, D2, C2, 0, D3, C3, 0
.elseif \N == 2
    GST xv, , D0, C0, 0, D1, C1, 0
.elseif \N == 1
    GST xv, , D0, C0, 0
.endif

#undef G0
#undef G1
#undef G2
#undef G3
#undef G4
#undef G5
#undef G6
#undef G7
.endm

.macro dsolve_4 N
// if N = 4 the data layout of C is as follows:
// U0
// U1
// U2
// U3
// if N = 2 the dat layout of C is as follows:
// U0
// U1
// if N = 1 the dat layout of C is as follows:
// U0
// The matrix A has dimensions of 4x4, and
// it will be divided into 1 segments for processing.

#define G0 U0
#define G1 U1
#define G2 U2
#define G3 U3
    // Transpose U3 U2 U1 U0
    GTRANSPOSE4x4_D U0, U1, U2, U3, G0, G1, G2, G3, D0, D1
    // A
    // G0 G1 G2 G3
    //-------------
    // 0           | D9
    // 4  5        | D8 D7
    // 8  9  10    | D6 D5 D4
    // 12 13 14 15 | D3 D2 D1 D0
    GLDREPL xv, d, D3, A0, 12 * 8, D2, A0, 13 * 8, D1, A0, 14 * 8, D0, A0, 15 * 8, \
                   D6, A0, 8 * 8,  D5, A0, 9 * 8,  D4, A0, 10 * 8, \
                   D8, A0, 4 * 8,  D7, A0, 5 * 8, \
                   D9, A0, 0 * 8
    xvfmul.d    G3,     G3,     D0
    GNMSUB xvf, d, G2, G3, D1, G2, G1, G3, D2, G1, G0, G3, D3, G0
    xvfmul.d    G2,     G2,     D4
    GNMSUB xvf, d, G1, G2, D5, G1, G0, G2, D6, G0
    xvfmul.d    G1,     G1,     D7
    GNMSUB xvf, d, G0, G1, D8, G0
    xvfmul.d    G0,     G0,     D9
    // Store B
.if \N == 4
    GST xv, , G0, B0, 0, G1, B0, 4 * 8, G2, B0, 8 * 8, G3, B0, 12 * 8
.elseif \N == 2
    GST v, , $vr0, B0, 0, $vr1, B0, 2 * 8, $vr2, B0, 4 * 8, $vr3, B0, 6 * 8
.elseif \N == 1
    GST f, d, $f0, B0, 0, $f1, B0, 1 * 8, $f2, B0, 2 * 8, $f3, B0, 3 * 8
.endif
    // Transpose
    GTRANSPOSE4x4_D G0, G1, G2, G3, D0, D1, D2, D3, D4, D5
    // Store C
.if \N == 4
    GST xv, , D0, C0, 0, D1, C1, 0, D2, C2, 0, D3, C3, 0
.elseif \N == 2
    GST xv, , D0, C0, 0, D1, C1, 0
.elseif \N == 1
    GST xv, , D0, C0, 0
.endif

#undef G0
#undef G1
#undef G2
#undef G3
.endm

.macro dsolve_2 N
#define G0  U2
#define G1  U3
    // Transpose
    GSBUTTERFLY xv, d, G0, G1, U1, U0
    // A
    // G0 G1
    // ------
    // 0    | D2
    // 2  3 | D1 D0
    GLDREPL xv, d, D2, A0, 0, D1, A0, 2 * 8, D0, A0, 3 * 8
    xvfmul.d    G1,     G1,     D0
    GNMSUB xvf, d, G0, G1, D1, G0
    xvfmul.d    G0,     G0,     D2
    // Store B
.if \N == 4
    GST xv, , G0, B0, 0, G1, B0, 4 * 8
.elseif \N == 2
    GST v, , $vr2, B0, 0, $vr3, B0, 2 * 8
.elseif \N == 1
    GST f, d, $f2, B0, 0, $f3, B0, 8
.endif
    // Transpose
    GSBUTTERFLY xv, d, D0, D1, G1, G0
    // Store C
.if \N == 4
    vst       $vr16,    C0,      0x00
    vst       $vr17,    C1,      0x00
    xvstelm.d D0,  C2,  0x00,    0x02
    xvstelm.d D1,  C3,  0x00,    0x02
    xvstelm.d D0,  C2,  0x08,    0x03
    xvstelm.d D1,  C3,  0x08,    0x03
.elseif \N == 2
    GST v, , $vr16, C0, 0, $vr17, C1, 0
.elseif \N == 1
    GST v, , $vr16, C0, 0
.endif

#undef G0
#undef G1
.endm

.macro dgemm_dsolve_16x4
    or    T1,   A0,     A0
    or    T2,   B0,     B0
    bge   ZERO, L,	.L_dsolve_16x4_load
    dgemm_16x4
    b	.L_dsolve_16x4
.L_dsolve_16x4_load:
    // Load C
    GLD xv, , U0,  C0, 0x00, U1,  C0, 0x20, U2,  C0, 0x40, U3,  C0, 0x60
    GLD xv, , U4,  C1, 0x00, U5,  C1, 0x20, U6,  C1, 0x40, U7,  C1, 0x60
    GLD xv, , U8,  C2, 0x00, U9,  C2, 0x20, U10, C2, 0x40, U11, C2, 0x60
    GLD xv, , U12, C3, 0x00, U13, C3, 0x20, U14, C3, 0x40, U15, C3, 0x60
/********************** solver ******************/
.L_dsolve_16x4:
    PTR_ADDI    A0,    T1,    -(16 * 8 * 8)
    PTR_ADDI    A0,    A0,    -(16 * 8 * 8)
    PTR_ADDI    B0,    T2,    -(16 * 4 * 8)
    dsolve_16 4
.endm

.macro dgemm_dsolve_1x4
    or    T1,   A0,     A0
    or    T2,   B0,     B0
    bge   ZERO, L,    .L_dsolve_1x4_load
    dgemm_1x4
    b   .L_dsolve_1x4
.L_dsolve_1x4_load:
    // Load C
    fld.d       $f0,    C0,     0x00
    fld.d       $f1,    C1,     0x00
    fld.d       $f2,    C2,     0x00
    fld.d       $f3,    C3,     0x00
    xvinsve0.d  U0,     U1,     0x01
    xvinsve0.d  U0,     U2,     0x02
    xvinsve0.d  U0,     U3,     0x03
.L_dsolve_1x4:
    or      A0,     T1,     T1
    or      B0,     T2,     T2
    GLDREPL xv, d, D0, A0, -1 * 8
    GMUL xvf, d, U0, U0, D0
    // Store C
    xvstelm.d   U0,     C0,     0x00,       0x00
    xvstelm.d   U0,     C1,     0x00,       0x01
    xvstelm.d   U0,     C2,     0x00,       0x02
    xvstelm.d   U0,     C3,     0x00,       0x03
    // Store B
    xvst    U0,     B0,     -32
.endm

.macro dgemm_dsolve_2x4
    or    T1,   A0,     A0
    or    T2,   B0,     B0
    bge   ZERO, L,    .L_dsolve_2x4_load
    dgemm_2x4
    b   .L_dsolve_2x4
.L_dsolve_2x4_load:
    /* Load C0  */
    xvld      U0,  C0,  0x00
    /* Load C1  */
    xvld      U1,  C1,  0x00
    /* Load C2  */
    xvld      U2,  C2,  0x00
    /* Load C3  */
    xvld      U3,  C3,  0x00

    xvpermi.q   U0, U2, 0x02
    xvpermi.q   U1, U3, 0x02
/********************** solver ******************/
.L_dsolve_2x4:
    PTR_ADDI      A0,     T1,     -(2 * 2 * 8)
    PTR_ADDI      B0,     T2,     -(2 * 4 * 8)
    dsolve_2 4
.endm

.macro dgemm_dsolve_4x4
    or    T1,   A0,     A0
    or    T2,   B0,     B0
    bge   ZERO, L,    .L_dsolve_4x4_load
    dgemm_4x4
    b .L_dsolve_4x4
.L_dsolve_4x4_load:
    /* Load C0  */
    xvld      U0,  C0,  0x00
    /* Load C1  */
    xvld      U1,  C1,  0x00
    /* Load C2  */
    xvld      U2,  C2,  0x00
    /* Load C3  */
    xvld      U3,  C3,  0x00
/************** solver *****************/
.L_dsolve_4x4:
    PTR_ADDI      A0,     T1,     -(4 * 4 * 8)
    PTR_ADDI      B0,     T2,     -(4 * 4 * 8)

    dsolve_4 4
.endm

.macro dgemm_dsolve_8x4
    or    T1,   A0,     A0
    or    T2,   B0,     B0
    bge   ZERO, L,	.L_dsolve_8x4_load
    dgemm_8x4
    b .L_dsolve_8x4
.L_dsolve_8x4_load:
    /* Load C0  */
    xvld      U0,  C0,  0x00
    xvld      U1,  C0,  0x20

    /* Load C1  */
    xvld      U2,  C1,  0x00
    xvld      U3,  C1,  0x20

    /* Load C2  */
    xvld      U4,  C2,  0x00
    xvld      U5,  C2,  0x20

    /* Load C3  */
    xvld      U6,  C3,  0x00
    xvld      U7,  C3,  0x20
/********* solver *********/
.L_dsolve_8x4:
    PTR_ADDI      A0,     T1,     -(8 * 8 * 8)
    PTR_ADDI      B0,     T2,     -(8 * 4 * 8)

    dsolve_8 4
.endm

.macro dgemm_dsolve_4x2
    or    T1,   A0,     A0
    or    T2,   B0,     B0
    bge   ZERO, L,	.L_dsolve_4x2_load
    dgemm_4x2
    b .L_dsolve_4x2
.L_dsolve_4x2_load:
    /* Load C0  */
    xvld      U0,  C0,  0x00
    /* Load C1  */
    xvld      U1,  C1,  0x00
.L_dsolve_4x2:
    PTR_ADDI      A0,     T1,     -(4 * 4 * 8)
    PTR_ADDI      B0,     T2,     -(4 * 2 * 8)

    dsolve_4 2
.endm

.macro dgemm_dsolve_2x2
    or    T1,   A0,     A0
    or    T2,   B0,     B0
    bge   ZERO, L,	.L_dsolve_2x2_load
    dgemm_2x2
    b .L_dsolve_2x2
.L_dsolve_2x2_load:
    /* Load C0  */
    xvld      U0,  C0,  0x00
    /* Load C1  */
    xvld      U1,  C1,  0x00
.L_dsolve_2x2:
    PTR_ADDI     A0,  T1,     -(2 * 2 * 8)
    PTR_ADDI     B0,  T2,     -(2 * 2 * 8)

    dsolve_2 2
.endm

.macro dgemm_dsolve_8x2
    or    T1,   A0,     A0
    or    T2,   B0,     B0
    bge   ZERO, L,	.L_dsolve_8x2_load
    dgemm_8x2
    b .L_dsolve_8x2
.L_dsolve_8x2_load:
    /* Load C0  */
    xvld      U0,  C0,  0x00
    xvld      U1,  C0,  0x20
    /* Load C1  */
    xvld      U2,  C1,  0x00
    xvld      U3,  C1,  0x20
.L_dsolve_8x2:
    PTR_ADDI     A0,  T1,     -(8 * 8 * 8)
    PTR_ADDI     B0,  T2,     -(8 * 2 * 8)

    dsolve_8 2
.endm

.macro dgemm_dsolve_16x2
    or    T1,   A0,     A0
    or    T2,   B0,     B0
    bge   ZERO, L,	.L_dsolve_16x2_load
    dgemm_16x2
    b .L_dsolve_16x2
.L_dsolve_16x2_load:
    /* Load C0  */
    xvld      U0,  C0,  0x00
    xvld      U1,  C0,  0x20
    xvld      U2,  C0,  0x40
    xvld      U3,  C0,  0x60
    /* Load C1  */
    xvld      U4,  C1,  0x00
    xvld      U5,  C1,  0x20
    xvld      U6,  C1,  0x40
    xvld      U7,  C1,  0x60
.L_dsolve_16x2:
    PTR_ADDI    A0,    T1,    -(16 * 8 * 8)
    PTR_ADDI    A0,    A0,    -(16 * 8 * 8)
    PTR_ADDI    B0,    T2,    -(16 * 2 * 8)

    dsolve_16 2
.endm

.macro dgemm_dsolve_2x1
    or    T1,   A0,     A0
    or    T2,   B0,     B0
    bge   ZERO, L,	.L_dsolve_2x1_load
    dgemm_2x1
    b .L_dsolve_2x1
.L_dsolve_2x1_load:
    /* Load C0  */
    xvld      U0,  C0,  0x00
.L_dsolve_2x1:
    PTR_ADDI     A0,  T1,     -(2 * 2 * 8)
    PTR_ADDI     B0,  T2,     -(2 * 1 * 8)

    dsolve_2 1
.endm

.macro dgemm_dsolve_4x1
    or    T1,   A0,     A0
    or    T2,   B0,     B0
    bge   ZERO, L,	.L_dsolve_4x1_load
    dgemm_4x1
    b .L_dsolve_4x1
.L_dsolve_4x1_load:
    /* Load C0  */
    xvld      U0,  C0,  0x00
.L_dsolve_4x1:
    PTR_ADDI      A0,     T1,     -(4 * 4 * 8)
    PTR_ADDI      B0,     T2,     -(4 * 1 * 8)

    dsolve_4 1
.endm

.macro dgemm_dsolve_8x1
    or    T1,   A0,     A0
    or    T2,   B0,     B0
    bge   ZERO, L,	.L_dsolve_8x1_load
    dgemm_8x1
    b .L_dsolve_8x1
.L_dsolve_8x1_load:
    /* Load C0  */
    xvld      U0,  C0,  0x00
    xvld      U1,  C0,  0x20
.L_dsolve_8x1:
    PTR_ADDI     A0,  T1,     -(8 * 8 * 8)
    PTR_ADDI     B0,  T2,     -(8 * 1 * 8)

    dsolve_8 1
.endm

.macro dgemm_dsolve_16x1
    or    T1,   A0,     A0
    or    T2,   B0,     B0
    bge   ZERO, L,	.L_dsolve_16x1_load
    dgemm_16x1
    b .L_dsolve_16x1
.L_dsolve_16x1_load:
    /* Load C0  */
    xvld      U0,  C0,  0x00
    xvld      U1,  C0,  0x20
    xvld      U2,  C0,  0x40
    xvld      U3,  C0,  0x60
.L_dsolve_16x1:
    PTR_ADDI    A0,    T1,    -(16 * 8 * 8)
    PTR_ADDI    A0,    A0,    -(16 * 8 * 8)
    PTR_ADDI    B0,    T2,    -(16 * 1 * 8)

    dsolve_16 1
.endm

    PROLOGUE
    push_if_used 9, 8
    PTR_SLLI   LDC,   LDC,   3
    /* if (!(N >> 2)) goto L_N3 */
    PTR_SRAI   J,     N,     2     /* J = bn >> 2 */
    andi     N,     N,     0x03
    beq      ZERO,  J,     .L_N3
.align 5
.L_J1:
    PTR_ADDI   J,     J,     -1
    PTR_ADD    KK,    M,     OFFSET

    andi      I,    M,      15
    beq       ZERO, I,      .L_M16
    andi      I,    M,      1
    beqz      I,    .L_M2
.L_M1:
    PTR_ADDI    T0,   M,      -1
    PTR_SLLI    T0,   T0,     3
    PTR_MUL     AA,   T0,     K
    PTR_ADD     AA,   AA,     A
    PTR_ALSL    A0,   KK,     AA,     3 /* a + (m - 1) * k + kk */
    PTR_ADD     CC,   T0,     C         /* c + (m - 1) */

    PTR_SLLI   T0,    KK,     5
    PTR_ADD    B0,    B,      T0 /* b + 4 * kk */
    PTR_SUB    L,     K,      KK
    GADD , d, C0, CC, ZERO, C1, C0, LDC, C2, C1, LDC, C3, C2, LDC
    dgemm_dsolve_1x4
    PTR_ADDI   KK,    KK,     -1
.L_M2:
    andi    I,      M,      2
    beqz    I,      .L_M4
    PTR_SRLI  T0,     M,      1
    PTR_SLLI  T0,     T0,     1
    PTR_ADDI  T0,     T0,     -2
    PTR_SLLI  T0,     T0,     3 /* ((m & -2) - 2) */
    PTR_ADD   CC,     T0,     C /* c + ((m & -2) - 2)*/
    PTR_SLLI  T1,     KK,     4
    PTR_MUL   AA,     T0,     K
    PTR_ADD   AA,     AA,     A
    PTR_ADD   A0,     AA,     T1 /* a + ((m & -2) - 2) * k + 2 * kk */
    PTR_SLLI  T0,     KK,     5
    PTR_ADD   B0,     B,      T0 /* b + 4 * kk */
    PTR_SUB   L,      K,      KK
    GADD , d, C0, CC, ZERO, C1, C0, LDC, C2, C1, LDC, C3, C2, LDC
    dgemm_dsolve_2x4
    PTR_ADDI  KK,     KK,     -2
.L_M4:
    andi    I,      M,      4
    beqz    I,      .L_M8
    PTR_SRLI  T0,     M,      2
    PTR_SLLI  T0,     T0,     2
    PTR_ADDI  T0,     T0,     -4
    PTR_SLLI  T0,     T0,     3 /* ((m & -4) - 4) */
    PTR_ADD   CC,     T0,     C /* c + ((m & -4) - 4)*/
    PTR_SLLI  T1,     KK,     5
    PTR_MUL   AA,     T0,     K
    PTR_ADD   AA,     AA,     A
    PTR_ADD   A0,     AA,     T1 /* a + ((m & -4) - 4) * k + 4 * kk */
    PTR_SLLI  T0,     KK,     5
    PTR_ADD   B0,     B,      T0 /* b + 4 * kk */
    PTR_SUB   L,      K,      KK
    GADD , d, C0, CC, ZERO, C1, C0, LDC, C2, C1, LDC, C3, C2, LDC
    dgemm_dsolve_4x4
    PTR_ADDI  KK,     KK,     -4
.L_M8:
    andi    I,      M,      8
    beqz    I,      .L_M16
    PTR_SRLI  T0,     M,      3
    PTR_SLLI  T0,     T0,     3
    PTR_ADDI  T0,     T0,     -8
    PTR_SLLI  T0,     T0,     3 /* ((m & -8) - 8) */
    PTR_ADD   CC,     T0,     C /* c + ((m & -8) - 8)*/
    PTR_SLLI  T1,     KK,     6
    PTR_MUL   AA,     T0,     K
    PTR_ADD   AA,     AA,     A
    PTR_ADD   A0,     AA,     T1 /* a + ((m & -8) - 8) * k + 8 * kk */
    PTR_SLLI  T0,     KK,     5
    PTR_ADD   B0,     B,      T0 /* b + 4 * kk */
    PTR_SUB   L,      K,      KK
    GADD , d, C0, CC, ZERO, C1, C0, LDC, C2, C1, LDC, C3, C2, LDC
    dgemm_dsolve_8x4
    PTR_ADDI  KK,     KK,     -8
.L_M16:
    PTR_SRAI   I,     M,     4     /* I = bm >> 4 */
    beq      ZERO,  I,     .L_M0

    PTR_SRLI   T0,    M,      4
    PTR_SLLI   T0,    T0,     4
    PTR_ADDI   T0,    T0,     -16 /* ((M & -16)) - 16) */
    PTR_SLLI   T0,    T0,     3
    PTR_MUL    AA,    T0,     K
    PTR_ADD    AA,    A,      AA
    PTR_ADD    CC,    C,      T0
.align 5
.L_I1:
    PTR_SLLI   T0,    KK,     5
    PTR_ADD    B0,    B,      T0
    PTR_SUB    L,     K,      KK
    GADD , d, C0, CC, ZERO, C1, C0, LDC, C2, C1, LDC, C3, C2, LDC
    PTR_SLLI   T0,    KK,     7
    PTR_ADD    A0,    AA,     T0
    dgemm_dsolve_16x4
    PTR_ADDI   I,     I,      -1
    PTR_ADDI   KK,    KK,     -16
    PTR_ADDI   CC,    CC,     -(16 * 8)
    PTR_SLLI   T0,    K,      7
    PTR_SUB    AA,    AA,     T0
    blt      ZERO,  I,      .L_I1
.L_M0:
    PTR_SLLI   T0,    K,      3
    PTR_ALSL   B,     T0,     B,      2 // b += 4 * k;
    PTR_ALSL   C,     LDC,    C,      2 // c += 4 * ldc
    blt      ZERO,  J,      .L_J1
.L_N3:
    andi    J,      N,      2
    beq     ZERO,   J,      .L_N1

    PTR_ADD    KK,    M,     OFFSET
    andi      I,    M,      15
    beq       ZERO, I,      .L_N3_M16
    andi      I,    M,      1
    beqz      I,    .L_N3_M2
.L_N3_M1:
    PTR_ADDI    KK,   KK,     -1

    PTR_ADDI    T0,   M,      -1
    PTR_SLLI    T0,   T0,     3
    PTR_MUL     AA,   T0,     K
    PTR_ADD     AA,   AA,     A
    PTR_ALSL    A0,   KK,     AA,     3 /* a + (m - 1) * k + kk */
    PTR_ADD     CC,   T0,     C         /* c + (m - 1) */

    PTR_SLLI   T0,    KK,     4
    PTR_ADD    B0,    B,      T0 /* b + 2 * kk */
    GADD , d, C0, CC, ZERO, C1, C0, LDC
    // dgemm_dsolve_1x2
    GLD f, d, $f0, A0, 0, $f1, C0, 0, $f2, C1, 0
    GMUL f, d, $f1, $f1, $f0, $f2, $f2, $f0
    GST f, d, $f1, C0, 0, $f2, C1, 0, $f1, B0, 0, $f2, B0, 8
.L_N3_M2:
    andi    I,      M,      2
    beqz    I,      .L_N3_M4
    PTR_SRLI  T0,     M,      1
    PTR_SLLI  T0,     T0,     1
    PTR_ADDI  T0,     T0,     -2
    PTR_SLLI  T0,     T0,     3 /* ((m & -2) - 2) */
    PTR_ADD   CC,     T0,     C /* c + ((m & -2) - 2)*/
    PTR_SLLI  T1,     KK,     4
    PTR_MUL   AA,     T0,     K
    PTR_ADD   AA,     AA,     A
    PTR_ADD   A0,     AA,     T1 /* a + ((m & -2) - 2) * k + 2 * kk */
    PTR_SLLI  T0,     KK,     4
    PTR_ADD   B0,     B,      T0 /* b + 2 * kk */
    PTR_SUB   L,      K,      KK
    GADD , d, C0, CC, ZERO, C1, C0, LDC
    dgemm_dsolve_2x2
    PTR_ADDI  KK,     KK,     -2
.L_N3_M4:
    andi    I,      M,      4
    beqz    I,      .L_N3_M8
    PTR_SRLI  T0,     M,      2
    PTR_SLLI  T0,     T0,     2
    PTR_ADDI  T0,     T0,     -4
    PTR_SLLI  T0,     T0,     3 /* ((m & -4) - 4) */
    PTR_ADD   CC,     T0,     C /* c + ((m & -4) - 4)*/
    PTR_SLLI  T1,     KK,     5
    PTR_MUL   AA,     T0,     K
    PTR_ADD   AA,     AA,     A
    PTR_ADD   A0,     AA,     T1 /* a + ((m & -4) - 4) * k + 4 * kk */
    PTR_SLLI  T0,     KK,     4
    PTR_ADD   B0,     B,      T0 /* b + 2 * kk */
    PTR_SUB   L,      K,      KK
    GADD , d, C0, CC, ZERO, C1, C0, LDC
    dgemm_dsolve_4x2
    PTR_ADDI  KK,     KK,     -4
.L_N3_M8:
    andi    I,      M,      8
    beqz    I,      .L_N3_M16
    PTR_SRLI  T0,     M,      3
    PTR_SLLI  T0,     T0,     3
    PTR_ADDI  T0,     T0,     -8
    PTR_SLLI  T0,     T0,     3 /* ((m & -8) - 8) */
    PTR_ADD   CC,     T0,     C /* c + ((m & -8) - 8)*/
    PTR_SLLI  T1,     KK,     6
    PTR_MUL   AA,     T0,     K
    PTR_ADD   AA,     AA,     A
    PTR_ADD   A0,     AA,     T1 /* a + ((m & -8) - 8) * k + 8 * kk */
    PTR_SLLI  T0,     KK,     4
    PTR_ADD   B0,     B,      T0 /* b + 2 * kk */
    PTR_SUB   L,      K,      KK
    GADD , d, C0, CC, ZERO, C1, C0, LDC
    dgemm_dsolve_8x2
    PTR_ADDI  KK,     KK,     -8
.L_N3_M16:
    PTR_SRAI   I,     M,     4     /* I = bm >> 4 */
    beq      ZERO,  I,     .L_N3_M0

    PTR_SRLI   T0,    M,      4
    PTR_SLLI   T0,    T0,     4
    PTR_ADDI   T0,    T0,     -16 /* ((M & -16)) - 16) */
    PTR_SLLI   T0,    T0,     3
    PTR_MUL    AA,    T0,     K
    PTR_ADD    AA,    A,      AA
    PTR_ADD    CC,    C,      T0
.align 5
.L_N3_I1:
    PTR_SLLI   T0,    KK,     4
    PTR_ADD    B0,    B,      T0
    PTR_SUB    L,     K,      KK
    GADD , d, C0, CC, ZERO, C1, C0, LDC
    PTR_SLLI   T0,    KK,     7
    PTR_ADD    A0,    AA,     T0
    dgemm_dsolve_16x2
    PTR_ADDI   I,     I,      -1
    PTR_ADDI   KK,    KK,     -16
    PTR_ADDI   CC,    CC,     -(16 * 8)
    PTR_SLLI   T0,    K,      7
    PTR_SUB    AA,    AA,     T0
    blt      ZERO,  I,      .L_N3_I1
.L_N3_M0:
    PTR_SLLI   T0,    K,      3
    PTR_ALSL   B,     T0,     B,      1 // b += 2 * k;
    PTR_ALSL   C,     LDC,    C,      1 // c += 2 * ldc
.L_N1:
    andi    J,      N,      1
    beq     ZERO,   J,      .L_N0

    PTR_ADD    KK,    M,     OFFSET
    andi      I,    M,      15
    beq       ZERO, I,      .L_N1_M16
    andi      I,    M,      1
    beqz      I,    .L_N1_M2
.L_N1_M1:
    PTR_ADDI    KK,   KK,     -1

    PTR_ADDI    T0,   M,      -1
    PTR_SLLI    T0,   T0,     3
    PTR_MUL     AA,   T0,     K
    PTR_ADD     AA,   AA,     A
    PTR_ALSL    A0,   KK,     AA,     3 /* a + (m - 1) * k + kk */
    PTR_ADD     CC,   T0,     C         /* c + (m - 1) */

    PTR_SLLI   T0,    KK,     3
    PTR_ADD    B0,    B,      T0 /* b + kk */
    GADD , d, C0, CC, ZERO
    // dgemm_dsolve_1x1
    GLD f, d, $f0, A0, 0, $f1, C0, 0
    GMUL f, d, $f1, $f1, $f0
    GST f, d, $f1, C0, 0, $f1, B0, 0
.L_N1_M2:
    andi    I,      M,      2
    beqz    I,      .L_N1_M4
    PTR_SRLI  T0,     M,      1
    PTR_SLLI  T0,     T0,     1
    PTR_ADDI  T0,     T0,     -2
    PTR_SLLI  T0,     T0,     3 /* ((m & -2) - 2) */
    PTR_ADD   CC,     T0,     C /* c + ((m & -2) - 2)*/
    PTR_SLLI  T1,     KK,     4
    PTR_MUL   AA,     T0,     K
    PTR_ADD   AA,     AA,     A
    PTR_ADD   A0,     AA,     T1 /* a + ((m & -2) - 2) * k + 2 * kk */
    PTR_SLLI  T0,     KK,     3
    PTR_ADD   B0,     B,      T0 /* b + kk */
    PTR_SUB   L,      K,      KK
    GADD , d, C0, CC, ZERO
    dgemm_dsolve_2x1
    PTR_ADDI  KK,     KK,     -2
.L_N1_M4:
    andi    I,      M,      4
    beqz    I,      .L_N1_M8
    PTR_SRLI  T0,     M,      2
    PTR_SLLI  T0,     T0,     2
    PTR_ADDI  T0,     T0,     -4
    PTR_SLLI  T0,     T0,     3 /* ((m & -4) - 4) */
    PTR_ADD   CC,     T0,     C /* c + ((m & -4) - 4)*/
    PTR_SLLI  T1,     KK,     5
    PTR_MUL   AA,     T0,     K
    PTR_ADD   AA,     AA,     A
    PTR_ADD   A0,     AA,     T1 /* a + ((m & -4) - 4) * k + 4 * kk */
    PTR_SLLI  T0,     KK,     3
    PTR_ADD   B0,     B,      T0 /* b + kk */
    PTR_SUB   L,      K,      KK
    GADD , d, C0, CC, ZERO
    dgemm_dsolve_4x1
    PTR_ADDI  KK,     KK,     -4
.L_N1_M8:
    andi    I,      M,      8
    beqz    I,      .L_N1_M16
    PTR_SRLI  T0,     M,      3
    PTR_SLLI  T0,     T0,     3
    PTR_ADDI  T0,     T0,     -8
    PTR_SLLI  T0,     T0,     3 /* ((m & -8) - 8) */
    PTR_ADD   CC,     T0,     C /* c + ((m & -8) - 8)*/
    PTR_SLLI  T1,     KK,     6
    PTR_MUL   AA,     T0,     K
    PTR_ADD   AA,     AA,     A
    PTR_ADD   A0,     AA,     T1 /* a + ((m & -8) - 8) * k + 8 * kk */
    PTR_SLLI  T0,     KK,     3
    PTR_ADD   B0,     B,      T0 /* b +  kk */
    PTR_SUB   L,      K,      KK
    GADD , d, C0, CC, ZERO
    dgemm_dsolve_8x1
    PTR_ADDI  KK,     KK,     -8
.L_N1_M16:
    PTR_SRAI   I,     M,     4     /* I = bm >> 4 */
    beq      ZERO,  I,     .L_N1_M0

    PTR_SRLI   T0,    M,      4
    PTR_SLLI   T0,    T0,     4
    PTR_ADDI   T0,    T0,     -16 /* ((M & -16)) - 16) */
    PTR_SLLI   T0,    T0,     3
    PTR_MUL    AA,    T0,     K
    PTR_ADD    AA,    A,      AA
    PTR_ADD    CC,    C,      T0
.align 5
.L_N1_I1:
    PTR_SLLI   T0,    KK,     3
    PTR_ADD    B0,    B,      T0
    PTR_SUB    L,     K,      KK
    GADD , d, C0, CC, ZERO
    PTR_SLLI   T0,    KK,     7
    PTR_ADD    A0,    AA,     T0
    dgemm_dsolve_16x1
    PTR_ADDI   I,     I,      -1
    PTR_ADDI   KK,    KK,     -16
    PTR_ADDI   CC,    CC,     -(16 * 8)
    PTR_SLLI   T0,    K,      7
    PTR_SUB    AA,    AA,     T0
    blt      ZERO,  I,      .L_N1_I1
.L_N1_M0:
.L_N0:
    pop_if_used 9, 8
    jirl    $r0, $r1, 0x0
    EPILOGUE
