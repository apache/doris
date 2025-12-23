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
* 2023/08/26 guxiwei
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
#define BB     B0
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
#define G0     D0
#define G1     D1
#define G2     D2
#define G3     D3
#define G4     D4
#define G5     D5
#define G6     D6
#define G7     D7
#define G8     D8
#define G9     D9
#define G10    D10
#define G11    D11
#define G12    D12
#define G13    D13
#define G14    D14
#define G15    D15

/* Prefetch interval */
#define A_PRE  0x400
#define B_PRE  0x100

#include "dtrsm_kernel_macro.S"

.macro ldrepl_macro stride:req, index:req, more:vararg
// Load Ux (x = 0...15)
    GLDREPL xv, d, $xr\index, A0, \index * 8 - \stride * 8
.ifnb \more
    ldrepl_macro \stride, \more
.endif
.endm
.macro nmsub_macro reg:req, start0:req, start1:req, more:vararg
// Gx -= reg * Ux
    xvfnmsub.d  $xr\start0, \reg, $xr\start1, $xr\start0
.ifnb \more
    nmsub_macro \reg, \more
.endif
.endm
.macro B_st_macro N:req, stride:req, start:req, more:vararg
// Store Gx(x = 16...31)
.if \N == 4
    xvst    $xr\start, B0, \start * 0x20 - \stride * 0x20
.elseif \N == 2
    vst     $vr\start, B0, \start * 0x10 - \stride * 0x10
.elseif \N == 1
    fst.d   $f\start, B0, \start * 0x08 - \stride * 0x08
.endif
.ifnb \more
    B_st_macro \N, \stride, \more
.endif
.endm

.macro dsolve_16 N
// The data layout of C (4x16) is as follows (store 4 data in each register):
// U0  U1  U2  U3
// U4  U5  U6  U7
// U8  U9  U10 U11
// U12 U13 U14 U15
// The first step is to transpose the result of C
    GTRANSPOSE4x4_D U3, U7, U11, U15, G12, G13, G14, G15, D0, D1
    GTRANSPOSE4x4_D U2, U6, U10, U14, G8, G9, G10, G11, D0, D1
    GTRANSPOSE4x4_D U1, U5, U9, U13, G4, G5, G6, G7, U3, U7
    GTRANSPOSE4x4_D U0, U4, U8, U12, G0, G1, G2, G3, U3, U7
// Now we have the following memory layout of C:
//     0     1    2   3    ...    15
// 0 |    |    |    |    |     |     |
// 1 | G0 | G1 | G2 | G3 | ... | G15 |
// 2 |    |    |    |    |     |     |
// 3 |    |    |    |    |     |     |
// Next we are going to process matrix A with a size of 16x16,
// using only the upper triangular portion. The memory layout of
// matrix A is as follows, quite large.
//0	1	2	3	4	5	6	7	8	9	10	11	12	13	14	15
//	17	18	19	20	21	22	23	24	25	26	27	28	29	30	31
//		34	35	36	37	38	39	40	41	42	43	44	45	46	47
//			51	52	53	54	55	56	57	58	59	60	61	62	63
//				68	69	70	71	72	73	74	75	76	77	78	79
//					85	86	87	88	89	90	91	92	93	94	95
//						102	103	104	105	106	107	108	109	110	111
//							119	120	121	122	123	124	125	126	127
//								136	137	138	139	140	141	142	143
//									153	154	155	156	157	158	159
//										170	171	172	173	174	175
//											187	188	189	190	191
//												204	205	206	207
//													221	222	223
//														238	239
//															255
// Sequentially extract data from A in row order
// Load 0
    ldrepl_macro 0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
    GMUL xvf, d, G0, G0, U0
    nmsub_macro G0, 17, 1, 18, 2, 19, 3, 20, 4, 21, 5, 22, 6, 23, 7, 24, 8, \
                    25, 9, 26, 10, 27, 11, 28, 12, 29, 13, 30, 14, 31, 15
    PTR_ADDI    A0,      A0,      17 * 8
// Load 1
    ldrepl_macro 1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
    GMUL xvf, d, G1, G1, U1
    nmsub_macro G1, 18, 2, 19, 3, 20, 4, 21, 5, 22, 6, 23, 7, 24, 8, \
                    25, 9, 26, 10, 27, 11, 28, 12, 29, 13, 30, 14, 31, 15
    PTR_ADDI    A0,      A0,      17 * 8
// Load 2
    ldrepl_macro 2, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
    GMUL xvf, d, G2, G2, U2
    nmsub_macro G2, 19, 3, 20, 4, 21, 5, 22, 6, 23, 7, 24, 8, 25, 9, 26, \
                    10, 27, 11, 28, 12, 29, 13, 30, 14, 31, 15
    PTR_ADDI    A0,      A0,      17 * 8
// Load 3
    ldrepl_macro 3, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
    GMUL xvf, d, G3, G3, U3
    nmsub_macro G3, 20, 4, 21, 5, 22, 6, 23, 7, 24, 8, 25, 9, 26, 10, \
                    27, 11, 28, 12, 29, 13, 30, 14, 31, 15
    PTR_ADDI    A0,      A0,      17 * 8
// Load 4
    ldrepl_macro 4, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
    GMUL xvf, d, G4, G4, U4
    nmsub_macro G4, 21, 5, 22, 6, 23, 7, 24, 8, 25, 9, 26, 10, 27, 11, \
                    28, 12, 29, 13, 30, 14, 31, 15
    PTR_ADDI    A0,      A0,      17 * 8
// Load 5
    ldrepl_macro 5, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
    GMUL xvf, d, G5, G5, U5
    nmsub_macro G5, 22, 6, 23, 7, 24, 8, 25, 9, 26, 10, 27, 11, 28, 12, \
                    29, 13, 30, 14, 31, 15
    PTR_ADDI    A0,      A0,      17 * 8
// Load 6
    ldrepl_macro 6, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
    GMUL xvf, d, G6, G6, U6
    nmsub_macro G6, 23, 7, 24, 8, 25, 9, 26, 10, 27, 11, 28, 12, 29, 13, \
                    30, 14, 31, 15
    PTR_ADDI    A0,      A0,      17 * 8
// Load 7
    ldrepl_macro 7, 7, 8, 9, 10, 11, 12, 13, 14, 15
    GMUL xvf, d, G7, G7, U7
    nmsub_macro G7, 24, 8, 25, 9, 26, 10, 27, 11, 28, 12, 29, 13, 30, 14, 31, 15
    PTR_ADDI    A0,      A0,      17 * 8
// Load 8
    ldrepl_macro 8, 8, 9, 10, 11, 12, 13, 14, 15
    GMUL xvf, d, G8, G8, U8
    nmsub_macro G8, 25, 9, 26, 10, 27, 11, 28, 12, 29, 13, 30, 14, 31, 15
    PTR_ADDI    A0,      A0,      17 * 8
// Load 9
    ldrepl_macro 9, 9, 10, 11, 12, 13, 14, 15
    GMUL xvf, d, G9, G9, U9
    nmsub_macro G9, 26, 10, 27, 11, 28, 12, 29, 13, 30, 14, 31, 15
    PTR_ADDI    A0,      A0,      17 * 8
// Load 10
    ldrepl_macro 10, 10, 11, 12, 13, 14, 15
    GMUL xvf, d, G10, G10, U10
    nmsub_macro G10, 27, 11, 28, 12, 29, 13, 30, 14, 31, 15
    PTR_ADDI    A0,      A0,      17 * 8
// Load 11
    ldrepl_macro 11, 11, 12, 13, 14, 15
    GMUL xvf, d, G11, G11, U11
    nmsub_macro G11, 28, 12, 29, 13, 30, 14, 31, 15
    PTR_ADDI    A0,      A0,      17 * 8
// Load 12
    ldrepl_macro 12, 12, 13, 14, 15
    GMUL xvf, d, G12, G12, U12
    nmsub_macro G12, 29, 13, 30, 14, 31, 15
    PTR_ADDI    A0,      A0,      17 * 8
// Load 13
    ldrepl_macro 13, 13, 14, 15
    GMUL xvf, d, G13, G13, U13
    nmsub_macro G13, 30, 14, 31, 15
    PTR_ADDI    A0,      A0,      17 * 8
// Load 14
    ldrepl_macro 14, 14, 15
    GMUL xvf, d, G14, G14, U14
    nmsub_macro G14, 31, 15
    PTR_ADDI    A0,      A0,      17 * 8
// Load 15
    ldrepl_macro 15, 15
    GMUL xvf, d, G15, G15, U15
// Finally, We can store the result.
// For B, stored sequentially, and  C, first transpose and then store
    B_st_macro \N, 16, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31
    GTRANSPOSE4x4_D G0, G1, G2, G3, G0, G1, G2, G3, U0, U1
    GTRANSPOSE4x4_D G4, G5, G6, G7, G4, G5, G6, G7, U0, U1
    GTRANSPOSE4x4_D G8, G9, G10, G11, G8, G9, G10, G11, U0, U1
    GTRANSPOSE4x4_D G12, G13, G14, G15, G12, G13, G14, G15, U0, U1
.if \N == 4
    GST xv, , G0, C0, 0x00, G4, C0, 0x20, G8,  C0, 0x40, G12, C0, 0x60, \
              G1, C1, 0x00, G5, C1, 0x20, G9,  C1, 0x40, G13, C1, 0x60, \
              G2, C2, 0x00, G6, C2, 0x20, G10, C2, 0x40, G14, C2, 0x60, \
              G3, C3, 0x00, G7, C3, 0x20, G11, C3, 0x40, G15, C3, 0x60
.elseif \N == 2
    GST xv, , G0, C0, 0x00, G4, C0, 0x20, G8,  C0, 0x40, G12, C0, 0x60, \
              G1, C1, 0x00, G5, C1, 0x20, G9,  C1, 0x40, G13, C1, 0x60
.elseif \N == 1
    GST xv, , G0, C0, 0x00, G4, C0, 0x20, G8,  C0, 0x40, G12, C0, 0x60
.endif
.endm

.macro dgemm_dsolve_16x4
    bge     ZERO,       KK,     .L_dsolve_16x4_load
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
    dsolve_16 4
.endm

.macro dsolve_8 N
// The data layout of C (4x8) is as follows (store 4 data in each register):
// U0  U1
// U2  U3
// U4  U5
// U6  U7
// The first step is to transpose the result of C
    GTRANSPOSE4x4_D U1, U3, U5, U7, G4, G5, G6, G7, G8, G9
    GTRANSPOSE4x4_D U0, U2, U4, U6, G0, G1, G2, G3, G8, G9
// Now we have the following memory layout of C:
//     0     1    2   3    ...   7
// 0 |    |    |    |    |     |    |
// 1 | G0 | G1 | G2 | G3 | ... | G7 |
// 2 |    |    |    |    |     |    |
// 3 |    |    |    |    |     |    |
// Next we are going to process matrix A with a size of 8x8,
// using only the upper triangular portion. The memory layout of
// matrix A is as follows:
//0	1	2	3	4	5	6	7
//	9	10	11	12	13	14	15
//		18	19	20	21	22	23
//			27	28	29	30	31
//				36	37	38	39
//					45	46	47
//						54	55
//							63
// Sequentially extract data from A in row order
// Load 0
    ldrepl_macro 0, 0, 1, 2, 3, 4, 5, 6, 7
    GMUL xvf, d, G0, G0, U0
    nmsub_macro G0, 17, 1, 18, 2, 19, 3, 20, 4, 21, 5, 22, 6, 23, 7
    PTR_ADDI    A0,      A0,      9 * 8
// Load 1
    ldrepl_macro 1, 1, 2, 3, 4, 5, 6, 7
    GMUL xvf, d, G1, G1, U1
    nmsub_macro G1, 18, 2, 19, 3, 20, 4, 21, 5, 22, 6, 23, 7
    PTR_ADDI    A0,      A0,      9 * 8
// Load 2
    ldrepl_macro 2, 2, 3, 4, 5, 6, 7
    GMUL xvf, d, G2, G2, U2
    nmsub_macro G2, 19, 3, 20, 4, 21, 5, 22, 6, 23, 7
    PTR_ADDI    A0,      A0,      9 * 8
// Load 3
    ldrepl_macro 3, 3, 4, 5, 6, 7
    GMUL xvf, d, G3, G3, U3
    nmsub_macro G3, 20, 4, 21, 5, 22, 6, 23, 7
    PTR_ADDI    A0,      A0,      9 * 8
// Load 4
    ldrepl_macro 4, 4, 5, 6, 7
    GMUL xvf, d, G4, G4, U4
    nmsub_macro G4, 21, 5, 22, 6, 23, 7
    PTR_ADDI    A0,      A0,      9 * 8
// Load 5
    ldrepl_macro 5, 5, 6, 7
    GMUL xvf, d, G5, G5, U5
    nmsub_macro G5, 22, 6, 23, 7
    PTR_ADDI    A0,      A0,      9 * 8
// Load 6
    ldrepl_macro 6, 6, 7
    GMUL xvf, d, G6, G6, U6
    nmsub_macro G6, 23, 7
    PTR_ADDI    A0,      A0,      9 * 8
// Load 7
    ldrepl_macro 7, 7
    GMUL xvf, d, G7, G7, U7
// Finally, We can store the result.
// For B, stored sequentially, and  C, first transpose and then store
    B_st_macro \N, 16, 16, 17, 18, 19, 20, 21, 22, 23
    GTRANSPOSE4x4_D G0, G1, G2, G3, G0, G1, G2, G3, U0, U1
    GTRANSPOSE4x4_D G4, G5, G6, G7, G4, G5, G6, G7, U0, U1
.if \N == 4
    GST xv, , G0, C0, 0x00, G4, C0, 0x20, \
              G1, C1, 0x00, G5, C1, 0x20, \
              G2, C2, 0x00, G6, C2, 0x20, \
              G3, C3, 0x00, G7, C3, 0x20
.elseif \N == 2
    GST xv, , G0, C0, 0x00, G4, C0, 0x20, \
              G1, C1, 0x00, G5, C1, 0x20
.elseif \N == 1
    GST xv, , G0, C0, 0x00, G4, C0, 0x20
.endif
.endm

.macro dgemm_dsolve_8x4
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
    dsolve_8 4
.endm

.macro dsolve_4 N
// The data layout of C (4x4) is as follows (store 4 data in each register):
// U0
// U1
// U2
// U3
// The first step is to transpose the result of C
    GTRANSPOSE4x4_D U0, U1, U2, U3, G0, G1, G2, G3, G4, G5
// Now we have the following memory layout of C:
//     0     1    2   3
// 0 |    |    |    |    |
// 1 | G0 | G1 | G2 | G3 |
// 2 |    |    |    |    |
// 3 |    |    |    |    |
// Next we are going to process matrix A with a size of 4x4,
// using only the upper triangular portion. The memory layout of
// matrix A is as follows:
//0	1	2	3
//	5	6	7
//		10	11
//			15
// Sequentially extract data from A in row order
// Load 0
    ldrepl_macro 0, 0, 1, 2, 3
    GMUL xvf, d, G0, G0, U0
    nmsub_macro G0, 17, 1, 18, 2, 19, 3
    PTR_ADDI    A0,      A0,      5 * 8
// Load 1
    ldrepl_macro 1, 1, 2, 3
    GMUL xvf, d, G1, G1, U1
    nmsub_macro G1, 18, 2, 19, 3
    PTR_ADDI    A0,      A0,      5 * 8
// Load 2
    ldrepl_macro 2, 2, 3
    GMUL xvf, d, G2, G2, U2
    nmsub_macro G2, 19, 3
    PTR_ADDI    A0,      A0,      5 * 8
// Load 3
    ldrepl_macro 3, 3
    GMUL xvf, d, G3, G3, U3
// Finally, We can store the result.
// For B, stored sequentially, and  C, first transpose and then store
    B_st_macro \N, 16, 16, 17, 18, 19
    GTRANSPOSE4x4_D G0, G1, G2, G3, G0, G1, G2, G3, U0, U1
.if \N == 4
    GST xv, , G0, C0, 0x00, G1, C1, 0x00, G2, C2, 0x00, G3, C3, 0x00
.elseif \N == 2
    GST xv, , G0, C0, 0x00, G1, C1, 0x00
.elseif \N == 1
    GST xv, , G0, C0, 0x00
.endif
.endm

.macro dgemm_dsolve_4x4
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
    dsolve_4 4
.endm

.macro dsolve_2 N
// Transpose
    GSBUTTERFLY xv, d, G0, G1, U1, U0
// Now we have the following memory layout of C:
//     0     1
// 0 |    |    |
// 1 | G0 | G1 |
// 2 |    |    |
// 3 |    |    |
// Next we are going to process matrix A with a size of 2x2,
// using only the upper triangular portion. The memory layout of
// matrix A is as follows:
//0	1
//	3
// Sequentially extract data from A in row order
// Load 0
    ldrepl_macro 0, 0, 1
    GMUL xvf, d, G0, G0, U0
    nmsub_macro G0, 17, 1
    PTR_ADDI    A0,      A0,      3 * 8
// Load 1
    ldrepl_macro 1, 1
    GMUL xvf, d, G1, G1, U1
// Finally, We can store the result.
// For B, stored sequentially, and  C, first transpose and then store
    B_st_macro \N, 16, 16, 17
    GSBUTTERFLY xv, d, U0, U1, G1, G0
.if \N == 4
    vst       $vr0,     C0,      0x00
    vst       $vr1,     C1,      0x00
    xvstelm.d U0,  C2,  0x00,    0x02
    xvstelm.d U1,  C3,  0x00,    0x02
    xvstelm.d U0,  C2,  0x08,    0x03
    xvstelm.d U1,  C3,  0x08,    0x03
.elseif \N == 2
    vst       $vr0,     C0,      0x00
    vst       $vr1,     C1,      0x00
.elseif \N == 1
    vst       $vr0,     C0,      0x00
.endif
.endm

.macro dgemm_dsolve_2x4
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
    dsolve_2 4
.endm

.macro dgemm_dsolve_1x4
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
    GLDREPL xv, d, D0, A0, 0x00
    GMUL xvf, d, U0, U0, D0
    // Store C
    xvstelm.d   U0,     C0,     0x00,       0x00
    xvstelm.d   U0,     C1,     0x00,       0x01
    xvstelm.d   U0,     C2,     0x00,       0x02
    xvstelm.d   U0,     C3,     0x00,       0x03
    // Store B
    xvst    U0,     B0,     0x00
.endm

.macro dgemm_dsolve_16x2
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
    dsolve_16 2
.endm

.macro dgemm_dsolve_8x2
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
    dsolve_8 2
.endm

.macro dgemm_dsolve_4x2
    bge   ZERO, L,	.L_dsolve_4x2_load
    dgemm_4x2
    b .L_dsolve_4x2
.L_dsolve_4x2_load:
    /* Load C0  */
    xvld      U0,  C0,  0x00
    /* Load C1  */
    xvld      U1,  C1,  0x00
.L_dsolve_4x2:
    dsolve_4 2
.endm

.macro dgemm_dsolve_1x2
    bge   ZERO, L,    .L_dsolve_1x2_load
    dgemm_1x2
    b   .L_dsolve_1x2
.L_dsolve_1x2_load:
    // Load C
    fld.d       $f0,    C0,     0x00
    fld.d       $f1,    C1,     0x00
    xvinsve0.d  U0,     U1,     0x01
.L_dsolve_1x2:
    GLDREPL xv, d, D0, A0, 0x00
    GMUL xvf, d, U0, U0, D0
    // Store C
    xvstelm.d   U0,     C0,     0x00,       0x00
    xvstelm.d   U0,     C1,     0x00,       0x01
    // Store B
    vst    $vr0,     B0,     0x00
.endm

.macro dgemm_dsolve_2x2
    bge   ZERO, L,	.L_dsolve_2x2_load
    dgemm_2x2
    b .L_dsolve_2x2
.L_dsolve_2x2_load:
    /* Load C0  */
    xvld      U0,  C0,  0x00
    /* Load C1  */
    xvld      U1,  C1,  0x00
.L_dsolve_2x2:
    dsolve_2 2
.endm

.macro dgemm_dsolve_16x1
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
    dsolve_16 1
.endm

.macro dgemm_dsolve_8x1
    bge   ZERO, L,	.L_dsolve_8x1_load
    dgemm_8x1
    b .L_dsolve_8x1
.L_dsolve_8x1_load:
    /* Load C0  */
    xvld      U0,  C0,  0x00
    xvld      U1,  C0,  0x20
.L_dsolve_8x1:
    dsolve_8 1
.endm

.macro dgemm_dsolve_4x1
    bge   ZERO, L,	.L_dsolve_4x1_load
    dgemm_4x1
    b .L_dsolve_4x1
.L_dsolve_4x1_load:
    /* Load C0  */
    xvld      U0,  C0,  0x00
.L_dsolve_4x1:
    dsolve_4 1
.endm

.macro dgemm_dsolve_2x1
    bge   ZERO, L,	.L_dsolve_2x1_load
    dgemm_2x1
    b .L_dsolve_2x1
.L_dsolve_2x1_load:
    /* Load C0  */
    xvld      U0,  C0,  0x00
.L_dsolve_2x1:
    dsolve_2 1
.endm

.macro dgemm_dsolve_1x1
    bge   ZERO, L,    .L_dsolve_1x1_load
    dgemm_1x1
    b .L_dsolve_1x1
.L_dsolve_1x1_load:
    // Load C
    fld.d       $f0,    C0,     0x00
.L_dsolve_1x1:
    GLDREPL xv, d, D0, A0, 0x00
    GMUL xvf, d, U0, U0, D0
    // Store C
    xvstelm.d   U0,     C0,     0x00,       0x00
    // Store B
    xvstelm.d   U0,     B0,     0x00,       0x00
.endm

    PROLOGUE
    push_if_used 9, 8
    PTR_SLLI   LDC,   LDC,   3
    /* if (!(N >> 2)) goto L_N3 */
    PTR_SRAI   J,     N,     2     /* J = bn >> 2 */
    andi       N,     N,     0x03
    beq        ZERO,  J,     .L_N3
.align 5
.L_J1:
    PTR_ADDI    J,      J,     -1
    move        KK,     OFFSET
    move        AA,     A
    move        CC,     C
    PTR_SRAI    I,      M,      4 // M >> 4
    beqz        I,      .L_M15
.align 4
.L_I1:
    GADD , d, C0, CC, ZERO, C1, C0, LDC, C2, C1, LDC, C3, C2, LDC
    move        A0,     AA
    move        B0,     B
    move        L,      KK
    dgemm_dsolve_16x4
    PTR_ADDI    I,      I,      -1
    PTR_SLLI    T0,     K,      7
    PTR_ADDI    CC,     CC,     0x80 // cc += 16
    PTR_ADDI    KK,     KK,     0x10 // kk += 16
    PTR_ADD     AA,     AA,     T0 // aa += 16 * k
    bnez        I,      .L_I1
.L_M15:
    andi        I,      M,      8
    beqz        I,      .L_M7
.L_M8:
    GADD , d, C0, CC, ZERO, C1, C0, LDC, C2, C1, LDC, C3, C2, LDC
    move        A0,     AA
    move        B0,     B
    move        L,      KK
    dgemm_dsolve_8x4
    PTR_SLLI    T0,     K,      6
    PTR_ADDI    CC,     CC,     0x40 // cc += 8
    PTR_ADDI    KK,     KK,     0x08 // kk += 8
    PTR_ADD     AA,     AA,     T0 // aa += 8 * k
.L_M7:
    andi        I,      M,      4
    beqz        I,      .L_M3
.L_M4:
    GADD , d, C0, CC, ZERO, C1, C0, LDC, C2, C1, LDC, C3, C2, LDC
    move        A0,     AA
    move        B0,     B
    move        L,      KK
    dgemm_dsolve_4x4
    PTR_SLLI    T0,     K,      5
    PTR_ADDI    CC,     CC,     0x20 // cc += 4
    PTR_ADDI    KK,     KK,     0x04 // kk += 4
    PTR_ADD     AA,     AA,     T0 // aa += 4 * k
.L_M3:
    andi        I,      M,      2
    beqz        I,      .L_M1
.L_M2:
    GADD , d, C0, CC, ZERO, C1, C0, LDC, C2, C1, LDC, C3, C2, LDC
    move        A0,     AA
    move        B0,     B
    move        L,      KK
    dgemm_dsolve_2x4
    PTR_SLLI    T0,     K,      4
    PTR_ADDI    CC,     CC,     0x10 // cc += 2
    PTR_ADDI    KK,     KK,     0x02 // kk += 2
    PTR_ADD     AA,     AA,     T0 // aa += 2 * k
.L_M1:
    andi        I,      M,      1
    beqz        I,      .L_M0
    GADD , d, C0, CC, ZERO, C1, C0, LDC, C2, C1, LDC, C3, C2, LDC
    move        A0,     AA
    move        B0,     B
    move        L,      KK
    dgemm_dsolve_1x4
    PTR_SLLI    T0,     K,      3
    PTR_ADDI    CC,     CC,     0x08 // cc += 1
    PTR_ADDI    KK,     KK,     0x01 // kk += 1
    PTR_ADD     AA,     AA,     T0 // aa += 1 * k
.L_M0:
    PTR_SLLI    T0,     K,      5
    PTR_SLLI    T1,     LDC,    2
    PTR_ADD     B,      B,      T0 // b += 4 * k
    PTR_ADD     C,      C,      T1 // c += 4 * ldc
    bnez        J,      .L_J1
.L_N3:
    andi    J,      N,      2
    beq     ZERO,   J,      .L_N1
.L_N2:
    move        KK,     OFFSET
    move        AA,     A
    move        CC,     C
    PTR_SRAI    I,      M,      4 // M >> 4
    beqz        I,      .L_N2_M15
.align 4
.L_N2_I1:
    GADD , d, C0, CC, ZERO, C1, C0, LDC
    move        A0,     AA
    move        B0,     B
    move        L,      KK
    dgemm_dsolve_16x2
    PTR_ADDI    I,      I,      -1
    PTR_SLLI    T0,     K,      7
    PTR_ADDI    CC,     CC,     0x80 // cc += 16
    PTR_ADDI    KK,     KK,     0x10 // kk += 16
    PTR_ADD     AA,     AA,     T0 // aa += 16 * k
    bnez        I,      .L_N2_I1
.L_N2_M15:
    andi        I,      M,      8
    beqz        I,      .L_N2_M7
.L_N2_M8:
    GADD , d, C0, CC, ZERO, C1, C0, LDC
    move        A0,     AA
    move        B0,     B
    move        L,      KK
    dgemm_dsolve_8x2
    PTR_SLLI    T0,     K,      6
    PTR_ADDI    CC,     CC,     0x40 // cc += 8
    PTR_ADDI    KK,     KK,     0x08 // kk += 8
    PTR_ADD     AA,     AA,     T0 // aa += 8 * k
.L_N2_M7:
    andi        I,      M,      4
    beqz        I,      .L_N2_M3
.L_N2_M4:
    GADD , d, C0, CC, ZERO, C1, C0, LDC
    move        A0,     AA
    move        B0,     B
    move        L,      KK
    dgemm_dsolve_4x2
    PTR_SLLI    T0,     K,      5
    PTR_ADDI    CC,     CC,     0x20 // cc += 4
    PTR_ADDI    KK,     KK,     0x04 // kk += 4
    PTR_ADD     AA,     AA,     T0 // aa += 4 * k
.L_N2_M3:
    andi        I,      M,      2
    beqz        I,      .L_N2_M1
.L_N2_M2:
    GADD , d, C0, CC, ZERO, C1, C0, LDC
    move        A0,     AA
    move        B0,     B
    move        L,      KK
    dgemm_dsolve_2x2
    PTR_SLLI    T0,     K,      4
    PTR_ADDI    CC,     CC,     0x10 // cc += 2
    PTR_ADDI    KK,     KK,     0x02 // kk += 2
    PTR_ADD     AA,     AA,     T0 // aa += 2 * k
.L_N2_M1:
    andi        I,      M,      1
    beqz        I,      .L_N2_M0
    GADD , d, C0, CC, ZERO, C1, C0, LDC
    move        A0,     AA
    move        B0,     B
    move        L,      KK
    dgemm_dsolve_1x2
    PTR_SLLI    T0,     K,      3
    PTR_ADDI    CC,     CC,     0x08 // cc += 1
    PTR_ADDI    KK,     KK,     0x01 // kk += 1
    PTR_ADD     AA,     AA,     T0 // aa += 1 * k
.L_N2_M0:
    PTR_SLLI    T0,     K,      4
    PTR_SLLI    T1,     LDC,    1
    PTR_ADD     B,      B,      T0 // b += 2 * k
    PTR_ADD     C,      C,      T1 // c += 2 * ldc
.L_N1:
    andi    J,      N,      1
    beq     ZERO,   J,      .L_N0

    move        KK,     OFFSET
    move        AA,     A
    move        CC,     C
    PTR_SRAI    I,      M,      4 // M >> 4
    beqz        I,      .L_N1_M15
.align 4
.L_N1_I1:
    GADD , d, C0, CC, ZERO
    move        A0,     AA
    move        B0,     B
    move        L,      KK
    dgemm_dsolve_16x1
    PTR_ADDI    I,      I,      -1
    PTR_SLLI    T0,     K,      7
    PTR_ADDI    CC,     CC,     0x80 // cc += 16
    PTR_ADDI    KK,     KK,     0x10 // kk += 16
    PTR_ADD     AA,     AA,     T0 // aa += 16 * k
    bnez        I,      .L_N1_I1
.L_N1_M15:
    andi        I,      M,      8
    beqz        I,      .L_N1_M7
.L_N1_M8:
    GADD , d, C0, CC, ZERO
    move        A0,     AA
    move        B0,     B
    move        L,      KK
    dgemm_dsolve_8x1
    PTR_SLLI    T0,     K,      6
    PTR_ADDI    CC,     CC,     0x40 // cc += 8
    PTR_ADDI    KK,     KK,     0x08 // kk += 8
    PTR_ADD     AA,     AA,     T0 // aa += 8 * k
.L_N1_M7:
    andi        I,      M,      4
    beqz        I,      .L_N1_M3
.L_N1_M4:
    GADD , d, C0, CC, ZERO
    move        A0,     AA
    move        B0,     B
    move        L,      KK
    dgemm_dsolve_4x1
    PTR_SLLI    T0,     K,      5
    PTR_ADDI    CC,     CC,     0x20 // cc += 4
    PTR_ADDI    KK,     KK,     0x04 // kk += 4
    PTR_ADD     AA,     AA,     T0 // aa += 4 * k
.L_N1_M3:
    andi        I,      M,      2
    beqz        I,      .L_N1_M1
.L_N1_M2:
    GADD , d, C0, CC, ZERO
    move        A0,     AA
    move        B0,     B
    move        L,      KK
    dgemm_dsolve_2x1
    PTR_SLLI    T0,     K,      4
    PTR_ADDI    CC,     CC,     0x10 // cc += 2
    PTR_ADDI    KK,     KK,     0x02 // kk += 2
    PTR_ADD     AA,     AA,     T0 // aa += 2 * k
.L_N1_M1:
    andi        I,      M,      1
    beqz        I,      .L_N1_M0
    GADD , d, C0, CC, ZERO
    move        A0,     AA
    move        B0,     B
    move        L,      KK
    dgemm_dsolve_1x1
    PTR_SLLI    T0,     K,      3
    PTR_ADDI    CC,     CC,     0x08 // cc += 1
    PTR_ADDI    KK,     KK,     0x01 // kk += 1
    PTR_ADD     AA,     AA,     T0 // aa += 1 * k
.L_N1_M0:
.L_N0:
    pop_if_used 9, 8
    jirl    $r0, $r1, 0x0
    EPILOGUE
