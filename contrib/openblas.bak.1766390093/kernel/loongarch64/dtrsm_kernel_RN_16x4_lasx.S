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
* 2023/09/26 guxiwei
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
    GLDREPL xv, d, $xr\index, B0, \index * 8 - \stride * 8
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
.macro A_st_macro N:req, stride:req, start:req, more:vararg
// Store Gx(x = 16...31)
.if \N == 4
    xvst    $xr\start, A0, \start * 0x20 - \stride * 0x20
.elseif \N == 2
    vst     $vr\start, A0, \start * 0x10 - \stride * 0x10
.elseif \N == 1
    fst.d   $f\start, A0, \start * 0x08 - \stride * 0x08
.endif
.ifnb \more
    A_st_macro \N, \stride, \more
.endif
.endm

.macro dsolve_16x4
// We are going to process matrix B with a size of 4x4,
// using only the upper triangular portion. The memory layout of
// matrix B is as follows:
//0	1	2	3
//	5	6	7
//		10	11
//			15
// Sequentially extract data from B in row order
    ldrepl_macro 16, 16, 17, 18, 19
    GMUL xvf, d, U0, D0, U0, U1, D0, U1, U2, D0, U2, U3, D0, U3
    ldrepl_macro 15, 20, 21, 22

    nmsub_macro D1, 4, 0, 5, 1, 6, 2, 7, 3
    ldrepl_macro 13, 23, 24
    GMUL xvf, d, U4, D4, U4, U5, D4, U5, U6, D4, U6, U7, D4, U7
    ldrepl_macro 10, 25
    nmsub_macro D2, 8, 0, 9, 1, 10, 2, 11, 3
    nmsub_macro D5, 8, 4, 9, 5, 10, 6, 11, 7
    GMUL xvf, d, U8, D7, U8, U9, D7, U9, U10, D7, U10, U11, D7, U11
    nmsub_macro D3, 12, 0, 13, 1, 14, 2, 15, 3
    nmsub_macro D6, 12, 4, 13, 5, 14, 6, 15, 7
    nmsub_macro D8, 12, 8, 13, 9, 14, 10, 15, 11
    GMUL xvf, d, U12, D9, U12, U13, D9, U13, U14, D9, U14, U15, D9, U15
// Store A
    A_st_macro 4, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
// Store C
    GST xv, , U0,  C0, 0x00, U1,  C0, 0x20, U2,  C0, 0x40, U3,  C0, 0x60, \
              U4,  C1, 0x00, U5,  C1, 0x20, U6,  C1, 0x40, U7,  C1, 0x60, \
              U8,  C2, 0x00, U9,  C2, 0x20, U10, C2, 0x40, U11, C2, 0x60, \
              U12, C3, 0x00, U13, C3, 0x20, U14, C3, 0x40, U15, C3, 0x60
.endm

.macro dsolve_16x2
// We are going to process matrix B with a size of 2x2,
// using only the upper triangular portion. The memory layout of
// matrix B is as follows:
//0	1
//	3
// Sequentially extract data from B in row order
    ldrepl_macro 16, 16, 17
    GMUL xvf, d, U0, D0, U0, U1, D0, U1, U2, D0, U2, U3, D0, U3
    ldrepl_macro 15, 18
    nmsub_macro D1, 4, 0, 5, 1, 6, 2, 7, 3
    GMUL xvf, d, U4, D2, U4, U5, D2, U5, U6, D2, U6, U7, D2, U7
// Store A
    A_st_macro 4, 0, 0, 1, 2, 3, 4, 5, 6, 7
// Store C
    GST xv, , U0,  C0, 0x00, U1,  C0, 0x20, U2,  C0, 0x40, U3,  C0, 0x60, \
              U4,  C1, 0x00, U5,  C1, 0x20, U6,  C1, 0x40, U7,  C1, 0x60
.endm

.macro dsolve_8x4
// We are going to process matrix B with a size of 4x4,
// using only the upper triangular portion. The memory layout of
// matrix B is as follows:
//0	1	2	3
//	5	6	7
//		10	11
//			15
// Sequentially extract data from B in row order
    ldrepl_macro 16, 16, 17, 18, 19
    GMUL xvf, d, U0, D0, U0, U1, D0, U1
    ldrepl_macro 15, 20, 21, 22
    nmsub_macro D1, 2, 0, 3, 1
    ldrepl_macro 13, 23, 24
    GMUL xvf, d, U2, D4, U2, U3, D4, U3
    ldrepl_macro 10, 25
    nmsub_macro D2, 4, 0, 5, 1
    nmsub_macro D5, 4, 2, 5, 3
    GMUL xvf, d, U4, D7, U4, U5, D7, U5
    nmsub_macro D3, 6, 0, 7, 1
    nmsub_macro D6, 6, 2, 7, 3
    nmsub_macro D8, 6, 4, 7, 5
    GMUL xvf, d, U6, D9, U6, U7, D9, U7
// Store A
    A_st_macro 4, 0, 0, 1, 2, 3, 4, 5, 6, 7
// Store C
    GST xv, , U0, C0, 0x00, U1, C0, 0x20, \
              U2, C1, 0x00, U3, C1, 0x20, \
              U4, C2, 0x00, U5, C2, 0x20, \
              U6, C3, 0x00, U7, C3, 0x20
.endm

.macro dsolve_8x2
// We are going to process matrix B with a size of 2x2,
// using only the upper triangular portion. The memory layout of
// matrix B is as follows:
//0	1
//	3
// Sequentially extract data from B in row order
    ldrepl_macro 16, 16, 17
    GMUL xvf, d, U0, D0, U0, U1, D0, U1
    ldrepl_macro 15, 18
    nmsub_macro D1, 2, 0, 3, 1
    GMUL xvf, d, U2, D2, U2, U3, D2, U3
// Store A
    A_st_macro 4, 0, 0, 1, 2, 3
// Store C
    GST xv, , U0,  C0, 0x00, U1,  C0, 0x20, \
              U2,  C1, 0x00, U3,  C1, 0x20
.endm

.macro dsolve_4x4
// We are going to process matrix B with a size of 4x4,
// using only the upper triangular portion. The memory layout of
// matrix B is as follows:
//0	1	2	3
//	5	6	7
//		10	11
//			15
// Sequentially extract data from B in row order
    ldrepl_macro 16, 16, 17, 18, 19
    GMUL xvf, d, U0, D0, U0
    ldrepl_macro 15, 20, 21, 22
    nmsub_macro D1, 1, 0
    ldrepl_macro 13, 23, 24
    GMUL xvf, d, U1, D4, U1
    ldrepl_macro 10, 25
    nmsub_macro D2, 2, 0
    nmsub_macro D5, 2, 1
    GMUL xvf, d, U2, D7, U2
    nmsub_macro D3, 3, 0
    nmsub_macro D6, 3, 1
    nmsub_macro D8, 3, 2
    GMUL xvf, d, U3, D9, U3
// Store A
    A_st_macro 4, 0, 0, 1, 2, 3
// Store C
    GST xv, , U0, C0, 0x00, U1, C1, 0x00, U2, C2, 0x00, U3, C3, 0x00
.endm

.macro dsolve_4x2
// We are going to process matrix B with a size of 2x2,
// using only the upper triangular portion. The memory layout of
// matrix B is as follows:
//0	1
//	3
// Sequentially extract data from B in row order
    ldrepl_macro 16, 16, 17
    GMUL xvf, d, U0, D0, U0
    ldrepl_macro 15, 18
    nmsub_macro D1, 1, 0
    GMUL xvf, d, U1, D2, U1
// Store A
    A_st_macro 4, 0, 0, 1
// Store C
    GST xv, , U0, C0, 0x00, U1, C1, 0x00
.endm

.macro dsolve_2x4
// We are going to process matrix B with a size of 4x4,
// using only the upper triangular portion. The memory layout of
// matrix B is as follows:
//0	1	2	3
//	5	6	7
//		10	11
//			15
// Sequentially extract data from B in row order
    ldrepl_macro 16, 16, 17, 18, 19
    GMUL xvf, d, U0, D0, U0
    ldrepl_macro 15, 20, 21, 22
    nmsub_macro D1, 1, 0
    ldrepl_macro 13, 23, 24
    GMUL xvf, d, U1, D4, U1

    ldrepl_macro 10, 25
    nmsub_macro D2, 2, 0
    nmsub_macro D5, 2, 1
    GMUL xvf, d, U2, D7, U2
    nmsub_macro D3, 3, 0
    nmsub_macro D6, 3, 1
    nmsub_macro D8, 3, 2
    GMUL xvf, d, U3, D9, U3
// Store A
    A_st_macro 2, 0, 0, 1, 2, 3
// Store C
    GST v, , $vr0, C0, 0x00, $vr1, C1, 0x00, $vr2, C2, 0x00, $vr3, C3, 0x00,
.endm

.macro dsolve_2x2
// We are going to process matrix B with a size of 2x2,
// using only the upper triangular portion. The memory layout of
// matrix B is as follows:
//0	1
//	3
// Sequentially extract data from B in row order
    ldrepl_macro 16, 16, 17
    GMUL xvf, d, U0, D0, U0
    ldrepl_macro 15, 18
    nmsub_macro D1, 1, 0
    GMUL xvf, d, U1, D2, U1
// Store A
    A_st_macro 2, 0, 0, 1
// Store C
    GST v, , $vr0, C0, 0x00, $vr1, C1, 0x00
.endm

.macro dsolve_1x4
// We are going to process matrix B with a size of 4x4,
// using only the upper triangular portion. The memory layout of
// matrix B is as follows:
//0	1	2	3
//	5	6	7
//		10	11
//			15
// Sequentially extract data from B in row order
    ldrepl_macro 16, 16, 17, 18, 19
    GMUL xvf, d, U0, D0, U0
    ldrepl_macro 15, 20, 21, 22
    nmsub_macro D1, 1, 0
    ldrepl_macro 13, 23, 24
    GMUL xvf, d, U1, D4, U1

    ldrepl_macro 10, 25
    nmsub_macro D2, 2, 0
    nmsub_macro D5, 2, 1
    GMUL xvf, d, U2, D7, U2
    nmsub_macro D3, 3, 0
    nmsub_macro D6, 3, 1
    nmsub_macro D8, 3, 2
    GMUL xvf, d, U3, D9, U3
// Store A
    A_st_macro 1, 0, 0, 1, 2, 3
// Store C
    GST f, d, $f0, C0, 0x00, $f1, C1, 0x00, $f2, C2, 0x00, $f3, C3, 0x00,
.endm

.macro dsolve_1x2
// We are going to process matrix B with a size of 2x2,
// using only the upper triangular portion. The memory layout of
// matrix B is as follows:
//0	1
//	3
// Sequentially extract data from B in row order
    ldrepl_macro 16, 16, 17
    GMUL xvf, d, U0, D0, U0
    ldrepl_macro 15, 18
    nmsub_macro D1, 1, 0
    GMUL xvf, d, U1, D2, U1
// Store A
    A_st_macro 1, 0, 0, 1
// Store C
    GST f, d, $f0, C0, 0x00, $f1, C1, 0x00
.endm

.macro dgemm_dsolve_16x4
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
    dsolve_16x4
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
    dsolve_8x4
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
    dsolve_4x4
.endm

.macro dgemm_dsolve_2x4
    bge   ZERO, L,    .L_dsolve_2x4_load
    dgemm_2x4
    xvpermi.q   U2,     U0,     0x01
    xvpermi.q   U3,     U1,     0x01
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
/********************** solver ******************/
.L_dsolve_2x4:
    dsolve_2x4
.endm

.macro dgemm_dsolve_1x4
    bge   ZERO, L,    .L_dsolve_1x4_load
    dgemm_1x4
    xvpackod.d  U1,     U0,     U0
    xvpermi.q   U2,     U0,     0x01
    xvpermi.q   U3,     U1,     0x01
    b   .L_dsolve_1x4
.L_dsolve_1x4_load:
    // Load C
    fld.d       $f0,    C0,     0x00
    fld.d       $f1,    C1,     0x00
    fld.d       $f2,    C2,     0x00
    fld.d       $f3,    C3,     0x00
.L_dsolve_1x4:
    dsolve_1x4
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
    dsolve_16x2
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
    dsolve_8x2
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
    dsolve_4x2
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
    dsolve_2x2
.endm

.macro dgemm_dsolve_1x2
    bge   ZERO, L,    .L_dsolve_1x2_load
    dgemm_1x2
    xvpackod.d  U1,     U0,     U0
    b   .L_dsolve_1x2
.L_dsolve_1x2_load:
    // Load C
    fld.d       $f0,    C0,     0x00
    fld.d       $f1,    C1,     0x00
.L_dsolve_1x2:
    dsolve_1x2
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
    ldrepl_macro 16, 16
    GMUL xvf, d, U0, D0, U0, U1, D0, U1, U2, D0, U2, U3, D0, U3
    // Store A
    A_st_macro 4, 0, 0, 1, 2, 3
    // Strore C
    GST xv, , U0, C0, 0x00, U1, C0, 0x20, U2, C0, 0x40, U3, C0, 0x60
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
    ldrepl_macro 16, 16
    GMUL xvf, d, U0, D0, U0, U1, D0, U1
    // Store A
    A_st_macro 4, 0, 0, 1
    // Strore C
    GST xv, , U0, C0, 0x00, U1, C0, 0x20
.endm

.macro dgemm_dsolve_4x1
    bge   ZERO, L,	.L_dsolve_4x1_load
    dgemm_4x1
    b .L_dsolve_4x1
.L_dsolve_4x1_load:
    /* Load C0  */
    xvld      U0,  C0,  0x00
.L_dsolve_4x1:
    ldrepl_macro 16, 16
    GMUL xvf, d, U0, D0, U0
    // Store A
    A_st_macro 4, 0, 0
    // Strore C
    GST xv, , U0, C0, 0x00
.endm

.macro dgemm_dsolve_2x1
    bge   ZERO, L,	.L_dsolve_2x1_load
    dgemm_2x1
    b .L_dsolve_2x1
.L_dsolve_2x1_load:
    /* Load C0  */
    xvld      U0,  C0,  0x00
.L_dsolve_2x1:
    ldrepl_macro 16, 16
    GMUL xvf, d, U0, D0, U0
    // Store A
    A_st_macro 2, 0, 0
    // Strore C
    GST v, , $vr0, C0, 0x00
.endm

.macro dgemm_dsolve_1x1
    bge   ZERO, L,    .L_dsolve_1x1_load
    dgemm_1x1
    b .L_dsolve_1x1
.L_dsolve_1x1_load:
    // Load C
    fld.d       $f0,    C0,     0x00
.L_dsolve_1x1:
    ldrepl_macro 16, 16
    GMUL xvf, d, U0, D0, U0
    // Store A
    A_st_macro 1, 0, 0
    // Strore C
    GST f, d, $f0, C0, 0x00
.endm

    PROLOGUE
    push_if_used 9, 8
    PTR_SLLI   LDC,   LDC,   3
    PTR_SUB    KK,    ZERO,  OFFSET
    /* if (!(N >> 2)) goto L_N3 */
    PTR_SRAI   J,     N,     2     /* J = bn >> 2 */
    andi       N,     N,     0x03
    beq        ZERO,  J,     .L_N3
.align 5
.L_J1:
    PTR_ADDI    J,      J,     -1
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
    PTR_ADD     AA,     AA,     T0 // aa += 1 * k
.L_M0:
    PTR_SLLI    T0,     K,      5
    PTR_SLLI    T1,     LDC,    2
    PTR_ADD     B,      B,      T0 // b += 4 * k
    PTR_ADD     C,      C,      T1 // c += 4 * ldc
    PTR_ADDI    KK,     KK,     4 // kk += 4
    bnez        J,      .L_J1
.L_N3:
    andi    J,      N,      2
    beq     ZERO,   J,      .L_N1
.L_N2:
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
    PTR_ADD     AA,     AA,     T0 // aa += 1 * k
.L_N2_M0:
    PTR_SLLI    T0,     K,      4
    PTR_SLLI    T1,     LDC,    1
    PTR_ADD     B,      B,      T0 // b += 2 * k
    PTR_ADD     C,      C,      T1 // c += 2 * ldc
    PTR_ADDI    KK,     KK,     2 // kk += 2
.L_N1:
    andi    J,      N,      1
    beq     ZERO,   J,      .L_N0
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
    PTR_ADD     AA,     AA,     T0 // aa += 1 * k
.L_N1_M0:
.L_N0:
    pop_if_used 9, 8
    jirl    $r0, $r1, 0x0
    EPILOGUE
