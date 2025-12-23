/*******************************************************************************
Copyright (c) 2021, The OpenBLAS Project
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
/* LASX vectors */
#define U0     $xr0
#define U1     $xr1
#define U2     $xr2
#define U3     $xr3
#define U4     $xr4
#define U5     $xr5
#define U6     $xr6
#define U7     $xr7
#define D0     $xr14
#define D1     $xr8
#define D2     $xr9
#define D3     $xr10
#define D4     $xr11
#define D5     $xr12
#define D6     $xr13
#define D7     $xr15

    PROLOGUE

    addi.d     $sp,  $sp,  -8
    SDARG      $r23, $sp,  0

    move       TD,   DST
    move       TS,   SRC
    slli.d     TL,   LDA,  0x03
    slli.d     T0,   TL,   0x01
    srai.d     J,    N,    0x02
    beq        J,    ZERO, .L_N2

.L_J1: /* J-- */
    move       S1,   TS
    add.d      S2,   TS,   TL
    srai.d     I,    M,    0x02
    add.d      S3,   S2,   TL
    add.d      S4,   S2,   T0
    add.d      TS,   S3,   T0
    addi.d     J,    J,    -1
    beq        I,    ZERO, .L_I3

.L_I1: /* I-- */
    xvld       U0,   S1,   0x00
    xvld       U1,   S2,   0x00
    xvld       U2,   S3,   0x00
    xvld       U3,   S4,   0x00

    xvpackev.d D0,   U1,   U0
    xvpackod.d D1,   U1,   U0
    xvpackev.d D2,   U3,   U2
    xvpackod.d D3,   U3,   U2

    xvand.v    U0,   D0,   D0
    xvpermi.q  D0,   D2,   0x02  // 0
    xvand.v    U1,   D1,   D1
    xvpermi.q  D1,   D3,   0x02  // 1
    xvpermi.q  D2,   U0,   0x31  // 2
    xvpermi.q  D3,   U1,   0x31  // 3

    xvst       D0,   TD,   0x00
    xvst       D1,   TD,   0x20
    xvst       D2,   TD,   0x40
    xvst       D3,   TD,   0x60

    addi.d     S1,   S1,   0x20
    addi.d     S2,   S2,   0x20
    addi.d     S3,   S3,   0x20
    addi.d     S4,   S4,   0x20
    addi.d     TD,   TD,   0x80

    addi.d     I,    I,    -1
    blt        ZERO, I,    .L_I1

.L_I3:
    andi      I,     M,    0x03
    beq       I,     ZERO, .L_I0

.L_II1:
    fld.d     F0,    S1,  0x00
    fld.d     F1,    S2,  0x00
    fld.d     F2,    S3,  0x00
    fld.d     F3,    S4,  0x00

    fst.d     F0,    TD,  0x00
    addi.d    S1,    S1,  0x08
    fst.d     F1,    TD,  0x08
    addi.d    S2,    S2,  0x08
    fst.d     F2,    TD,  0x10
    addi.d    S3,    S3,  0x08
    fst.d     F3,    TD,  0x18
    addi.d    S4,    S4,  0x08

    addi.d    TD,    TD,  0x20
    addi.d    I,     I,   -1
    blt       ZERO,  I,   .L_II1

.L_I0:
    blt       ZERO,  J,   .L_J1

.L_N2:
    andi      J,     N,   0x02
    beq       ZERO,  J,   .L_N1

    move       S1,   TS
    add.d      S2,   TS,   TL
    srai.d     I,    M,    0x02
    add.d      TS,   S2,   TL
    beq        I,    ZERO, .L_2I3

.L_2I1: /* I-- */
    xvld       U0,   S1,   0x00
    xvld       U1,   S2,   0x00

    xvpackev.d D0,   U1,   U0
    xvpackod.d D1,   U1,   U0

    xvand.v    U0,   D0,   D0
    xvpermi.q  D0,   D1,   0x02  // 0
    xvpermi.q  D1,   U0,   0x31  // 1

    xvst       D0,   TD,   0x00
    xvst       D1,   TD,   0x20
    addi.d     S1,   S1,   0x20
    addi.d     S2,   S2,   0x20
    addi.d     TD,   TD,   0x40
    addi.d     I,    I,    -1
    blt        ZERO, I,    .L_2I1

.L_2I3:
    andi       I,    M,    0x03
    beq        ZERO, I,    .L_N1

.L_2II1: /* I-- */
    fld.d      F0,   S1,   0x00
    fld.d      F1,   S2,   0x00
    fst.d      F0,   TD,   0x00
    addi.d     I,    I,    -1
    fst.d      F1,   TD,   0x08
    addi.d     S1,   S1,   0x08
    addi.d     S2,   S2,   0x08
    addi.d     TD,   TD,   0x10
    blt        ZERO, I,    .L_2II1

.L_N1:
    andi       J,    N,    0x01
    beq        ZERO, J,    .L_N0

    move       S1,   TS
    srai.d     I,    M,    0x02
    beq        ZERO, I,    .L_1I3

.L_1I1:
    xvld       U0,   S1,   0x00
    addi.d     S1,   S1,   0x20
    xvst       U0,   TD,   0x00
    addi.d     I,    I,    -1
    addi.d     TD,   TD,   0x20
    blt        ZERO, I,    .L_1I1

.L_1I3:
    andi       I,    M,    0x03
    beq        ZERO, I,    .L_N0

.L_1II1:
    fld.d      F0,   S1,   0x00
    addi.d     S1,   S1,   0x08
    fst.d      F0,   TD,   0x00
    addi.d     I,    I,    -1
    addi.d     TD,   TD,   0x08
    blt        ZERO, I,    .L_1II1

.L_N0:
    LDARG     $r23,  $sp, 0
    addi.d    $sp,   $sp, 8
    jirl      $r0,   $r1, 0x00

    EPILOGUE
