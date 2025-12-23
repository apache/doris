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
* 2023/08/23 guxiwei
*        UTEST                  : OK
*        CTEST                  : OK
*        TEST                   : OK
*********************************************************************/

/* Function parameters */
#define M      $r4    // param 1: m
#define N      $r5    // param 2: n
#define SRC    $r6    // param 3: src
#define LDA    $r7    // param 4: lda
#define DST    $r8    // param 5: dst

#define I      $r9
#define J      $r10
#define S1     $r12
#define S2     $r13
#define S3     $r14
#define S4     $r15
#define S5     $r16
#define S6     $r17
#define S7     $r18
#define S8     $r19
#define TD     $r20
#define TS     $r11
#define TL     $r7
#define T0     $r6
#undef  ZERO
#define ZERO   $r0

#define F0     $f0
#define F1     $f1
#define F2     $f2
#define F3     $f3
#define F4     $f4
#define F5     $f5
#define F6     $f6
#define F7     $f7
/* LASX vectors */
#define U0     $xr0
#define U1     $xr1
#define U2     $xr2
#define U3     $xr3
#define U4     $xr4
#define U5     $xr5
#define U6     $xr6
#define U7     $xr7
#define D0     $xr8
#define D1     $xr9
#define D2     $xr10
#define D3     $xr11
#define D4     $xr12
#define D5     $xr13
#define D6     $xr14
#define D7     $xr15
#define D8     $xr16
#define D10    $xr17
#define D12    $xr18
#define D14    $xr19

// Loops outline
//.L_N8: <----------------
//|   .L_M8:              |
//|   .L_M7:              | Main Loop
//|   .L_M1:              |
//|   .L_M0:--------------
//.L_N7:
//.L_N4:
//|   .L_N4_M4:
//|   .L_N4_M3:
//|   .L_N4_M1:
//.L_N3:
//.L_N2:
//|   .L_N2_M2:
//|   .L_N2_M1:
//.L_N1:
//|   .L_N1_M1:
//.L_N0

    PROLOGUE
    push_if_used 0, 0

    move       TD,   DST
    move       TS,   SRC
    PTR_SLLI   TL,   LDA,  0x02
    PTR_SLLI   T0,   TL,   0x01
    PTR_SRAI   J,    N,    0x03
    beq        J,    ZERO, .L_N7
.align 5
.L_N8:
    move         S1,   TS
    PTR_ADD      S2,   TS,   TL
    PTR_SRAI     I,    M,    0x03
    PTR_ADD      S3,   S2,   TL
    PTR_ADDI     J,    J,    -1
    PTR_ADD      S4,   S2,   T0
    PTR_ADD      S5,   S3,   T0
    PTR_ADD      S6,   S4,   T0
    PTR_ADD      S7,   S5,   T0
    PTR_ADD      S8,   S6,   T0
    PTR_ADD      TS,   S7,   T0
    beq          I,    ZERO, .L_M7
.align 5
.L_M8:
    xvld       U0,   S1,   0x00
    xvld       U1,   S2,   0x00
    xvld       U2,   S3,   0x00
    xvld       U3,   S4,   0x00
    xvld       U4,   S5,   0x00
    xvld       U5,   S6,   0x00
    xvld       U6,   S7,   0x00
    xvld       U7,   S8,   0x00

    GTRANSPOSE8x8_W D0, D2, D4, D6, D8, D10, D12, D14, \
                    U0, U1, U2, U3, U4, U5, U6, U7, \
                    D1, D3, D5, D7 // As tmp
    GST xv, , D0, TD, 0x00, D2,  TD, 0x20, D4,  TD, 0x40, D6,  TD, 0x60, \
              D8, TD, 0x80, D10, TD, 0xA0, D12, TD, 0xC0, D14, TD, 0xE0
    PTR_ADDI  TD, TD, 0x100
    PTR_ADDI     S1,   S1,   0x20
    PTR_ADDI     S2,   S2,   0x20
    PTR_ADDI     S3,   S3,   0x20
    PTR_ADDI     S4,   S4,   0x20
    PTR_ADDI     S5,   S5,   0x20
    PTR_ADDI     S6,   S6,   0x20
    PTR_ADDI     S7,   S7,   0x20
    PTR_ADDI     S8,   S8,   0x20
    PTR_ADDI     I,    I,    -1
    blt          ZERO, I,    .L_M8
.L_M7:
    andi      I,     M,    0x07
    beq       I,     ZERO, .L_M0
.align 5
.L_M1:
    fld.s     F0,    S1,  0x00
    fld.s     F1,    S2,  0x00
    fld.s     F2,    S3,  0x00
    fld.s     F3,    S4,  0x00
    fld.s     F4,    S5,  0x00
    fld.s     F5,    S6,  0x00
    fld.s     F6,    S7,  0x00
    fld.s     F7,    S8,  0x00

    fst.s       F0,    TD,  0x00
    PTR_ADDI    S1,    S1,  0x04
    fst.s       F1,    TD,  0x04
    PTR_ADDI    S2,    S2,  0x04
    fst.s       F2,    TD,  0x08
    PTR_ADDI    S3,    S3,  0x04
    fst.s       F3,    TD,  0x0C
    PTR_ADDI    S4,    S4,  0x04
    fst.s       F4,    TD,  0x10
    PTR_ADDI    S5,    S5,  0x04
    fst.s       F5,    TD,  0x14
    PTR_ADDI    S6,    S6,  0x04
    fst.s       F6,    TD,  0x18
    PTR_ADDI    S7,    S7,  0x04
    fst.s       F7,    TD,  0x1C
    PTR_ADDI    S8,    S8,  0x04

    PTR_ADDI    TD,    TD,  0x20
    PTR_ADDI    I,     I,   -1
    blt         ZERO,  I,   .L_M1
.L_M0:
    blt       ZERO,  J,   .L_N8
.L_N7:
    andi      J,     N,   0x07
    beq       ZERO,  J,   .L_N0

    andi      J,     N,   0x04
    beq       ZERO,  J,   .L_N3
.L_N4:
    move         S1,   TS
    PTR_ADD      S2,   TS,   TL
    PTR_SRAI     I,    M,    0x02
    PTR_ADD      S3,   S2,   TL
    PTR_ADD      S4,   S2,   T0
    PTR_ADD      TS,   S3,   T0
    beq          I,    ZERO, .L_N4_M3
.align 5
.L_N4_M4:
    GLD v, , $vr0, S1, 0, $vr1, S2, 0, $vr2, S3, 0, $vr3, S4, 0
    GSBUTTERFLY v, w, $vr4, $vr5, $vr2, $vr0
    GSBUTTERFLY v, w, $vr6, $vr7, $vr3, $vr1
    GSBUTTERFLY v, w, $vr0, $vr1, $vr6, $vr4
    GSBUTTERFLY v, w, $vr2, $vr3, $vr7, $vr5
    GST v, , $vr0, TD, 0x00, $vr1, TD, 0x10, $vr2, TD, 0x20, $vr3, TD, 0x30
    PTR_ADDI     S1,   S1,   0x10
    PTR_ADDI     S2,   S2,   0x10
    PTR_ADDI     S3,   S3,   0x10
    PTR_ADDI     S4,   S4,   0x10
    PTR_ADDI     TD,   TD,   0x40
    PTR_ADDI     I,    I,    -1
    blt          ZERO, I,    .L_N4_M4
.L_N4_M3:
    andi      I,     M,    0x03
    beq       I,     ZERO, .L_N3
.align 5
.L_N4_M1:
    fld.s     F0,    S1,  0x00
    fld.s     F1,    S2,  0x00
    fld.s     F2,    S3,  0x00
    fld.s     F3,    S4,  0x00

    fst.s       F0,    TD,  0x00
    PTR_ADDI    S1,    S1,  0x04
    fst.s       F1,    TD,  0x04
    PTR_ADDI    S2,    S2,  0x04
    fst.s       F2,    TD,  0x08
    PTR_ADDI    S3,    S3,  0x04
    fst.s       F3,    TD,  0x0C
    PTR_ADDI    S4,    S4,  0x04

    PTR_ADDI    TD,    TD,  0x10
    PTR_ADDI    I,     I,   -1
    blt         ZERO,  I,   .L_N4_M1
.L_N3:
    andi      J,     N,   0x03
    beq       ZERO,  J,   .L_N0

    andi      J,     N,   0x02
    beq       ZERO,  J,   .L_N1
.L_N2:
    move         S1,   TS
    PTR_ADD      S2,   TS,   TL
    PTR_SRAI     I,    M,    0x01
    PTR_ADD      TS,   S2,   TL
    beq          I,    ZERO, .L_N2_M1
.align 5
.L_N2_M2:
    GLD f, d, F0, S1, 0x00, F1, S2, 0x00
    vilvl.w $vr0, $vr1, $vr0
    GST v, , $vr0, TD, 0x00
    PTR_ADDI     S1,   S1,   0x08
    PTR_ADDI     S2,   S2,   0x08
    PTR_ADDI     TD,   TD,   0x10

    PTR_ADDI     I,    I,    -1
    blt          ZERO, I,    .L_N2_M2
.L_N2_M1:
    andi      I,     M,    0x01
    beq       I,     ZERO, .L_N1

    fld.s     F0,    S1,  0x00
    fld.s     F1,    S2,  0x00

    fst.s       F0,    TD,  0x00
    PTR_ADDI    S1,    S1,  0x04
    fst.s       F1,    TD,  0x04
    PTR_ADDI    S2,    S2,  0x04
    PTR_ADDI    TD,    TD,  0x08
.align 5
.L_N1:
    move      S1,    TS
    beq       ZERO,  M,   .L_N0
.L_N1_M1:
    fld.s       F0,    S1,  0x00
    PTR_ADDI    S1,    S1,  0x04
    fst.s       F0,    TD,  0x00
    PTR_ADDI    TD,    TD,  0x04
    PTR_ADDI    M,     M,   -1
    blt         ZERO,  M,   .L_N1_M1
.L_N0:
    pop_if_used 0, 0
    jirl    $r0, $r1, 0x0
    EPILOGUE
