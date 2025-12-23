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
#define TD     $r16
#define TS     $r17
#define TL     $r18
#define T0     $r19
#define S8     $r20
#define S9     $r23
#define S10    $r11
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
#define U8     $vr8
#define U9     $vr9
#define U10    $vr10
#define U11    $vr11
#define U12    $vr12
#define U13    $vr13
#define U14    $vr14
#define U15    $vr15


    PROLOGUE

    addi.d     $sp,  $sp,  -8
    SDARG      $r23, $sp,  0

    move       TS,     SRC          //aoffset
    move       TD,     DST          //boffset
    slli.d     TL,     LDA,   0x02  //lda
    slli.d     TL,     TL,    0x01  //lda

    ori        T0,     ZERO,  0x03
    andn       T0,     N,     T0
    mul.w      T0,     M,     T0
    slli.d     T0,     T0,    0x01
    slli.d     T0,     T0,    0x02
    add.d      S9,     DST,   T0  //boffset2

    ori        T0,     ZERO,  0x01
    andn       T0,     N,     T0
    mul.w      T0,     M,     T0
    slli.d     T0,     T0,    0x01
    slli.d     T0,     T0,    0x02
    add.d      S10,    DST,   T0  //boffset3

    srai.d     J,      M,     0x02  //j

    beq        J,      ZERO,  .L_M1

.L_J1:  /* if(j>0) j--*/
    move       S1,     TS     //aoffset1
    add.d      S2,     S1,    TL
    add.d      S3,     S2,    TL
    add.d      S4,     S3,    TL

    slli.d     T0,     TL,    0x02
    add.d      TS,     TS,    T0

    move       S8,     TD     //boffset1
    addi.d     TD,     TD,    0x80

    srai.d     I,      N,     0x02

    beq        ZERO,   I,     .L_JN1

.L_JI1:  /* if(i>0) i--*/
    vld       U0,   S1,   0x00
    vld       U1,   S1,   0x10

    vld       U2,   S2,   0x00
    vld       U3,   S2,   0x10

    vld       U4,   S3,   0x00
    vld       U5,   S3,   0x10

    vld       U6,   S4,   0x00
    vld       U7,   S4,   0x10

    vst       U0,   S8,   0x00
    vst       U1,   S8,   0x10
    vst       U2,   S8,   0x20
    vst       U3,   S8,   0x30
    vst       U4,   S8,   0x40
    vst       U5,   S8,   0x50
    vst       U6,   S8,   0x60
    vst       U7,   S8,   0x70

    addi.d     S1,   S1,   0x20
    addi.d     S2,   S2,   0x20
    addi.d     S3,   S3,   0x20
    addi.d     S4,   S4,   0x20
    slli.d     T0,   M,    0x05
    add.d      S8,   S8,   T0

    addi.d     I,    I,    -1
    blt        ZERO, I,    .L_JI1

.L_JN1:  /* if(n&2) */
    andi       I,    N,    0x02
    beq        ZERO, I,    .L_JN2

    vld       U0,   S1,   0x00
    vld       U1,   S2,   0x00
    vld       U2,   S3,   0x00
    vld       U3,   S4,   0x00

    vst       U0,   S9,   0x00
    vst       U1,   S9,   0x10
    vst       U2,   S9,   0x20
    vst       U3,   S9,   0x30

    addi.d     S1,   S1,   0x10
    addi.d     S2,   S2,   0x10
    addi.d     S3,   S3,   0x10
    addi.d     S4,   S4,   0x10
    addi.d     S9,   S9,   0x40

.L_JN2:  /* if(n&1) */
    andi       I,    N,    0x01
    beq        ZERO, I,    .L_J0

    fld.s      F0,   S1,   0x00
    fld.s      F1,   S1,   0x04

    fld.s      F2,   S2,   0x00
    fld.s      F3,   S2,   0x04

    fld.s      F4,   S3,   0x00
    fld.s      F5,   S3,   0x04

    fld.s      F6,   S4,   0x00
    fld.s      F7,   S4,   0x04

    fst.s      F0,   S10,   0x00
    fst.s      F1,   S10,   0x04
    fst.s      F2,   S10,   0x08
    fst.s      F3,   S10,   0x0c
    fst.s      F4,   S10,   0x10
    fst.s      F5,   S10,   0x14
    fst.s      F6,   S10,   0x18
    fst.s      F7,   S10,   0x1c

    addi.d     S10,  S10,   0x20

.L_J0:
    addi.d     J,      J,     -1
    blt        ZERO, J,   .L_J1

.L_M1:  /* if(m&2) */
    andi       I,      M,    0x02
    beq        ZERO,   I,    .L_M2

    move       S1,     TS     //aoffset1
    add.d      S2,     S1,    TL

    slli.d     T0,     TL,    0x01
    add.d      TS,     TS,    T0

    move       S8,     TD     //boffset1
    addi.d     TD,     TD,    0x40

    srai.d     I,      N,     0x02
    beq        ZERO,   I,     .L_M1N1

.L_M1I1:  /* if(i>0) */
    vld       U0,     S1,    0x00
    vld       U1,     S1,    0x10

    vld       U2,     S2,    0x00
    vld       U3,     S2,    0x10

    vst       U0,     S8,    0x00
    vst       U1,     S8,    0x10
    vst       U2,     S8,    0x20
    vst       U3,     S8,    0x30

    addi.d     S1,     S1,    0x20
    addi.d     S2,     S2,    0x20
    slli.d     T0,     M,     0x05
    add.d      S8,     S8,    T0

    addi.d     I,      I,     -1
    blt        ZERO,   I,     .L_M1I1

.L_M1N1:  /* if(n&2) */
    andi       I,      N,    0x02
    beq        ZERO,   I,    .L_M1N2

    vld       U0,     S1,    0x00
    vld       U1,     S2,    0x00

    vst       U0,     S9,    0x00
    vst       U1,     S9,    0x10

    addi.d     S1,     S1,    0x10
    addi.d     S2,     S2,    0x10
    addi.d     S9,     S9,    0x20

.L_M1N2:  /* if(n&1) */
    andi       I,      N,    0x01
    beq        ZERO,   I,    .L_M2

    fld.s      F0,      S1,    0x00
    fld.s      F1,      S1,    0x04

    fld.s      F2,      S2,    0x00
    fld.s      F3,      S2,    0x04

    fst.s      F0,      S10,   0x00
    fst.s      F1,      S10,   0x04
    fst.s      F2,      S10,   0x08
    fst.s      F3,      S10,   0x0c

    addi.d     S10,    S10,   0x10

.L_M2:  /* if(m&1) */
    andi       I,      M,    0x01
    beq        ZERO,   I,    .L_M0

    move       S1,     TS     //aoffset1
    move       S8,     TD     //boffset1

    srai.d     I,      N,     0x02
    beq        ZERO,   I,     .L_M2N1

.L_M2I1:  /* if(i>0) */
    vld       U0,     S1,    0x00
    vld       U1,     S1,    0x10

    vst       U0,     S8,    0x00
    vst       U1,     S8,    0x10

    addi.d     S1,     S1,    0x20
    slli.d     T0,     M,     0x05
    add.d      S8,     S8,    T0

    addi.d     I,      I,     -1
    blt        ZERO,   I,     .L_M2I1

.L_M2N1:  /* if(n&2) */
    andi       I,      N,    0x02
    beq        ZERO,   I,    .L_M2N2

    vld       U0,     S1,    0x00

    vst       U0,     S9,    0x00

    addi.d     S1,     S1,    0x10

.L_M2N2:  /* if(n&1) */
    andi       I,      N,    0x01
    beq        ZERO,   I,    .L_M0

    fld.s      F0,     S1,    0x00
    fld.s      F1,     S1,    0x04

    fst.s      F0,     S10,    0x00
    fst.s      F1,     S10,    0x04

.L_M0:
    LDARG     $r23,  $sp, 0
    addi.d    $sp,   $sp, 8
    jirl      $r0,   $r1, 0x00

    EPILOGUE