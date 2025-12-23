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
#define ASSEMBLER

#include "common.h"

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
#define TD     $r20
#define TS     $r11
#define TL     $r19
#define T0     $r23
#define ZERO   $r0

#define F0     $f0
#define F1     $f1
#define F2     $f2
#define F3     $f3
#define F4     $f4
#define F5     $f5
#define F6     $f6
#define F7     $f7

/* LSX vectors */
#define U0     $vr0
#define U1     $vr1
#define U2     $vr2
#define U3     $vr3
#define U4     $vr4
#define U5     $vr5
#define U6     $vr6
#define U7     $vr7
#define D0     $vr8
#define D1     $vr9
#define D2     $vr10
#define D3     $vr11
#define D4     $vr12
#define D5     $vr13
#define D6     $vr14
#define D7     $vr15
#define D8     $vr16

    PROLOGUE

    addi.d     $sp,  $sp,  -8
    SDARG      $r23, $sp,  0

    move       TD,   DST   //boffset
    move       TS,   SRC   //aoffset

    slli.d     TL,   LDA,  0x02
    slli.d     TL,   TL,   0x01

    srai.d     J,    N,    0x02
    beq        J,    ZERO,  .L_N0

.L_J1: /* J-- */
    move       S1,   TS
    add.d      S2,   S1,   TL
    add.d      S3,   S2,   TL
    add.d      S4,   S3,   TL

    slli.d     T0,   TL,   0x02
    add.d      TS,   TS,   T0

    srai.d     I,    M,    0x02
    beq        I,    ZERO,  .L_I3

.L_I1: /* I-- */
    vld       U0,   S1,   0x00
    vld       U1,   S1,   0x10

    vld       U2,   S2,   0x00
    vld       U3,   S2,   0x10

    vld       U4,   S3,   0x00
    vld       U5,   S3,   0x10

    vld       U6,   S4,   0x00
    vld       U7,   S4,   0x10

    vand.v    D0,   U2,   U2
    vand.v    D1,   U3,   U3
    vand.v    D2,   U2,   U2
    vand.v    D3,   U3,   U3
    vand.v    D4,   U6,   U6
    vand.v    D5,   U7,   U7
    vand.v    D6,   U6,   U6
    vand.v    D7,   U7,   U7

    vpermi.w  D0,   U0,   0x44
    vpermi.w  D4,   U4,   0x44
    vpermi.w  D2,   U0,   0xee
    vpermi.w  D6,   U4,   0xee
    vpermi.w  D1,   U1,   0x44
    vpermi.w  D5,   U5,   0x44
    vpermi.w  D3,   U1,   0xee
    vpermi.w  D7,   U5,   0xee

    vst       D0,   TD,   0x00
    vst       D4,   TD,   0x10
    vst       D2,   TD,   0x20
    vst       D6,   TD,   0x30
    vst       D1,   TD,   0x40
    vst       D5,   TD,   0x50
    vst       D3,   TD,   0x60
    vst       D7,   TD,   0x70

    addi.d     S1,   S1,   0x20   // a_offset
    addi.d     S2,   S2,   0x20
    addi.d     S3,   S3,   0x20
    addi.d     S4,   S4,   0x20
    addi.d     TD,   TD,   0x80  // b_offset

    addi.d     I,    I,    -1
    blt        ZERO, I,    .L_I1

.L_I3:  /* if(m&2) */
    andi       I,    M,    0x02
    beq        I,    ZERO, .L_II20

    vld       U0,   S1,   0x00
    vld       U1,   S2,   0x00
    vld       U2,   S3,   0x00
    vld       U3,   S4,   0x00

    vand.v    D0,   U1,   U1
    vand.v    D1,   U1,   U1
    vand.v    D2,   U3,   U3
    vand.v    D3,   U3,   U3

    vpermi.w  D0,   U0,   0x44
    vpermi.w  D2,   U2,   0x44
    vpermi.w  D1,   U0,   0xee
    vpermi.w  D3,   U2,   0xee

    vst       D0,   TD,   0x00
    vst       D2,   TD,   0x10
    vst       D1,   TD,   0x20
    vst       D3,   TD,   0x30

    addi.d     S1,   S1,   0x10
    addi.d     S2,   S2,   0x10
    addi.d     S3,   S3,   0x10
    addi.d     S4,   S4,   0x10
    addi.d     TD,   TD,   0x40

.L_II20:  /* if(m&1) */
    andi       I,    M,    0x01
    beq        I,    ZERO, .L_J0

    fld.s      F0,   S1,   0x00
    fld.s      F1,   S1,   0x04

    fld.s      F2,   S2,   0x00
    fld.s      F3,   S2,   0x04

    fld.s      F4,   S3,   0x00
    fld.s      F5,   S3,   0x04

    fld.s      F6,   S4,   0x00
    fld.s      F7,   S4,   0x04

    fst.s      F0,   TD,   0x00
    fst.s      F1,   TD,   0x04
    fst.s      F2,   TD,   0x08
    fst.s      F3,   TD,   0x0c
    fst.s      F4,   TD,   0x10
    fst.s      F5,   TD,   0x14
    fst.s      F6,   TD,   0x18
    fst.s      F7,   TD,   0x1c

    addi.d     TD,   TD,   0x20

.L_J0:
    addi.d    J,     J,    -1
    blt       ZERO,  J,    .L_J1

.L_N0:  /* if(n&2) */
    andi       I,     N,   0x02
    beq        ZERO,  I,   .L_N20

    move       S1,    TS
    add.d      S2,    S1,   TL

    slli.d     T0,    TL,   0x01
    add.d      TS,    TS,   T0

    srai.d     I,     M,    0x02
    beq        ZERO,  I,    .L_N10

.L_N11: /* if(i>0) */
    vld       U0,    S1,   0x00
    vld       U1,    S1,   0x10
    vld       U2,    S2,   0x00
    vld       U3,    S2,   0x10

    vand.v    D0,    U2,   U2
    vand.v    D1,    U3,   U3
    vand.v    D2,    U2,   U2
    vand.v    D3,    U3,   U3

    vpermi.w  D0,    U0,   0x44
    vpermi.w  D2,    U0,   0xee
    vpermi.w  D1,    U1,   0x44
    vpermi.w  D3,    U1,   0xee

    vst       D0,    TD,   0x00
    vst       D2,    TD,   0x10
    vst       D1,    TD,   0x20
    vst       D3,    TD,   0x30

    addi.d     S1,    S1,   0x20   // a_offset
    addi.d     S2,    S2,   0x20
    addi.d     TD,    TD,   0x40   // b_offset

    addi.d     I,     I,   -1
    blt        ZERO,  I,   .L_N11

.L_N10:  /* if(m&2) */
    andi       I,     M,    0x02
    beq        I,     ZERO, .L_N130

    vld       U0,    S1,   0x00
    vld       U1,    S2,   0x00
    vand.v    D0,    U1,   U1

    vpermi.w  D0,    U0,   0x44
    vpermi.w  U1,    U0,   0xee

    vst       D0,    TD,   0x00
    vst       U1,    TD,   0x10

    addi.d     S1,    S1,   0x10   // a_offset
    addi.d     S2,    S2,   0x10
    addi.d     TD,    TD,   0x20   // b_offset

.L_N130:  /* if(m&1) */
    andi       I,     M,    0x01
    beq        I,     ZERO, .L_N20

    fld.s      F0,     S1,   0x00
    fld.s      F1,     S1,   0x04

    fld.s      F2,     S2,   0x00
    fld.s      F3,     S2,   0x04

    fst.s      F0,     TD,   0x00
    fst.s      F1,     TD,   0x04
    fst.s      F2,     TD,   0x08
    fst.s      F3,     TD,   0x0c

    addi.d     TD,    TD,   0x10

.L_N20:   /* if(n&1) */
    andi       I,     N,    0x01
    beq        I,     ZERO, .L_N00

    move       S1,   TS
    srai.d     I,    M,    0x02

    beq        I,    ZERO, .L_N30

.L_N21:  /* if(i>0) */
    vld       U0,   S1,   0x00
    vld       U1,   S1,   0x10

    vst       U0,   TD,   0x00
    vst       U1,   TD,   0x10

    addi.d     S1,   S1,   0x20   // aoffset1
    addi.d     TD,   TD,   0x20   // b_offset

    addi.d     I,     I,   -1
    blt        ZERO,  I,   .L_N21

.L_N30:  /* if(m&2) */
    andi       I,     M,    0x02
    beq        I,     ZERO, .L_N330

    vld       U0,   S1,   0x00

    vst       U0,   TD,   0x00

    addi.d     S1,   S1,   0x10   // aoffset1
    addi.d     TD,   TD,   0x10   // b_offset

.L_N330:  /* if(m&1) */
    andi       I,     M,    0x01
    beq        I,     ZERO, .L_N00

    fld.s      F0,   S1,   0x00
    fld.s      F1,   S1,   0x04

    fst.s      F0,   TD,   0x00
    fst.s      F1,   TD,   0x04

.L_N00:
    LDARG     $r23,  $sp, 0
    addi.d    $sp,   $sp, 8
    jirl      $r0,   $r1, 0x00

    EPILOGUE