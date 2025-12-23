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
#define S8     $r19
#define TD     $r20
#define TS     $r11
#define TL     $r7
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

    move       TD,   DST  //boffset
    move       TS,   SRC  //aoffset

    slli.d     TL,   LDA,  0x02  //lda
    slli.d     TL,   TL,   0x01

    slli.d     T0,   TL,   0x03
    srai.d     J,    N,    0x03  //j

    beq        J,    ZERO, .L_N1

.L_J1:  /* if(j>0) j--*/
    move       S1,   TS
    add.d      S2,   TS,   TL
    move       I,    M
    add.d      S3,   S2,   TL
    add.d      S4,   S3,   TL
    add.d      S5,   S4,   TL
    add.d      S6,   S5,   TL
    add.d      S7,   S6,   TL
    add.d      S8,   S7,   TL
    add.d      TS,   TS,   T0

    beq        I,    ZERO, .L_J11

.L_I1:  /* if(i>0) i--*/
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

    fld.s      F0,   S5,   0x00
    fld.s      F1,   S5,   0x04
    fld.s      F2,   S6,   0x00
    fld.s      F3,   S6,   0x04
    fld.s      F4,   S7,   0x00
    fld.s      F5,   S7,   0x04
    fld.s      F6,   S8,   0x00
    fld.s      F7,   S8,   0x04

    fst.s      F0,   TD,   0x20
    fst.s      F1,   TD,   0x24
    fst.s      F2,   TD,   0x28
    fst.s      F3,   TD,   0x2c
    fst.s      F4,   TD,   0x30
    fst.s      F5,   TD,   0x34
    fst.s      F6,   TD,   0x38
    fst.s      F7,   TD,   0x3c

    addi.d     S1,   S1,   0x08
    addi.d     S2,   S2,   0x08
    addi.d     S3,   S3,   0x08
    addi.d     S4,   S4,   0x08
    addi.d     S5,   S5,   0x08
    addi.d     S6,   S6,   0x08
    addi.d     S7,   S7,   0x08
    addi.d     S8,   S8,   0x08
    addi.d     TD,   TD,   0x40

    addi.d     I,    I,    -1
    blt        ZERO, I,    .L_I1

.L_J11: /* j--*/
    addi.d     J,    J,    -1
    blt        ZERO, J,    .L_J1

.L_N1:  /* if(n&4)*/
    andi       I,     N,    0x04
    beq        I,     ZERO, .L_N2

    move       S1,   TS
    add.d      S2,   TS,   TL
    move       I,    M
    add.d      S3,   S2,   TL
    add.d      S4,   S3,   TL
    add.d      TS,   S4,   TL

    beq        I,     ZERO, .L_N2

.L_N11:  /* if(i>0)*/
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

    addi.d     S1,   S1,   0x08
    addi.d     S2,   S2,   0x08
    addi.d     S3,   S3,   0x08
    addi.d     S4,   S4,   0x08
    addi.d     TD,   TD,   0x20

    addi.d     I,    I,    -1
    blt        ZERO, I,    .L_N11

.L_N2:  /* if(n&2)*/
    andi       I,     N,    0x02
    beq        I,     ZERO, .L_N3

    move       S1,   TS
    add.d      S2,   TS,   TL
    move       I,    M
    add.d      TS,   S2,   TL

    beq        I,    ZERO, .L_N3

.L_N21:  /* if(i>0)*/
    fld.s      F0,   S1,   0x00
    fld.s      F1,   S1,   0x04
    fld.s      F2,   S2,   0x00
    fld.s      F3,   S2,   0x04

    fst.s      F0,   TD,   0x00
    fst.s      F1,   TD,   0x04
    fst.s      F2,   TD,   0x08
    fst.s      F3,   TD,   0x0c

    addi.d     S1,   S1,   0x08
    addi.d     S2,   S2,   0x08
    addi.d     TD,   TD,   0x10

    addi.d     I,    I,    -1
    blt        ZERO, I,    .L_N21

.L_N3:  /* if(n&2)*/
    andi       I,    N,    0x01
    beq        I,    ZERO, .L_N0

    move       S1,   TS
    move       I,    M

    beq        I,    ZERO, .L_N0

.L_N31:  /* if(i>0)*/
    fld.s      F0,   S1,   0x00
    fld.s      F1,   S1,   0x04

    fst.s      F0,   TD,   0x00
    fst.s      F1,   TD,   0x04

    addi.d     S1,   S1,   0x08
    addi.d     TD,   TD,   0x08

    addi.d     I,    I,    -1
    blt        ZERO, I,    .L_N31

.L_N0:
    LDARG     $r23,  $sp, 0
    addi.d    $sp,   $sp, 8
    jirl      $r0,   $r1, 0x00

    EPILOGUE