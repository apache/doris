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
#define S0     $r11
#define S1     $r12
#define S2     $r13
#define S3     $r14
#define S4     $r15
#define P0     $r16
#define P1     $r17
#define P2     $r18
#define P3     $r19
#define T0     $r20
#define T1     $r23
#define TL     $r7
#define ZERO   $r0

#define F0     $f0
#define F1     $f1
#define F2     $f2
#define F3     $f3
/* LASX vectors */
#define U0     $xr0
#define U1     $xr1
#define U2     $xr2
#define U3     $xr3

    PROLOGUE

    addi.d     $sp,    $sp,   -8
    SDARG      $r23,   $sp,   0

    move       S0,     SRC
    move       P0,     DST

    srai.d     T0,     N,     0x02
    slli.d     T0,     T0,    0x02
    srai.d     T1,     N,     0x01
    slli.d     T1,     T1,    0x01
    mul.d      T0,     M,     T0
    mul.d      T1,     M,     T1
    slli.d     T0,     T0,    0x03
    slli.d     T1,     T1,    0x03
    add.d      P2,     DST,   T0
    add.d      P3,     DST,   T1

    slli.d     TL,     LDA,   0x03
    srai.d     J,      M,     0x02
    slli.d     T0,     TL,    0x01
    slli.d     T1,     M,     0x05
    beq        ZERO,   J,     .L_M3

.L_J1: /* J-- */
    move       S1,     S0
    add.d      S2,     S0,    TL
    add.d      S3,     S1,    T0
    add.d      S4,     S2,    T0
    add.d      S0,     S3,    T0

    move       P1,     P0
    addi.d     P0,     P0,    0x80

    srai.d     I,      N,     0x02
    addi.d     J,      J,     -1
    beq        ZERO,   I,     .L_N3

.L_I1: /* I-- */
    xvld       U0,     S1,    0x00
    xvld       U1,     S2,    0x00
    xvld       U2,     S3,    0x00
    xvld       U3,     S4,    0x00

    xvst       U0,     P1,    0x00
    xvst       U1,     P1,    0x20
    xvst       U2,     P1,    0x40
    xvst       U3,     P1,    0x60

    addi.d     S1,     S1,    0x20
    addi.d     S2,     S2,    0x20
    addi.d     S3,     S3,    0x20
    addi.d     S4,     S4,    0x20
    add.d      P1,     P1,    T1

    addi.d     I,      I,    -1
    blt        ZERO,   I,    .L_I1

.L_N3:
    andi       I,      N,    0x02
    beq        ZERO,   I,    .L_N1

    xvld       U0,     S1,    0x00
    xvld       U1,     S2,    0x00
    xvld       U2,     S3,    0x00
    xvld       U3,     S4,    0x00

    xvpermi.q  U0,     U1,    0x02
    xvpermi.q  U2,     U3,    0x02

    xvst       U0,     P2,    0x00
    xvst       U2,     P2,    0x20

    addi.d     S1,     S1,    0x10
    addi.d     S2,     S2,    0x10
    addi.d     S3,     S3,    0x10
    addi.d     S4,     S4,    0x10
    addi.d     P2,     P2,    0x40

.L_N1:
    andi       I,      N,     0x01
    beq        ZERO,   I,     .L_N0

    fld.d      F0,     S1,    0x00
    fld.d      F1,     S2,    0x00
    fld.d      F2,     S3,    0x00
    fld.d      F3,     S4,    0x00

    fst.d      F0,     P3,    0x00
    fst.d      F1,     P3,    0x08
    fst.d      F2,     P3,    0x10
    fst.d      F3,     P3,    0x18

    addi.d     S1,     S1,    0x08
    addi.d     S2,     S2,    0x08
    addi.d     S3,     S3,    0x08
    addi.d     S4,     S4,    0x08
    addi.d     P3,     P3,    0x20

.L_N0:
    blt        ZERO,   J,     .L_J1

.L_M3:
    andi       J,      M,      0x02
    beq        ZERO,   J,      .L_M1

    move       S1,     S0
    add.d      S2,     S0,     TL
    add.d      S0,     S0,     T0

    move       P1,     P0
    addi.d     P0,     P0,     0x40

    srai.d     I,      N,      0x02
    beq        ZERO,   I,      .L_2N3

.L_2I1:   /* I-- */
    xvld       U0,     S1,     0x00
    xvld       U1,     S2,     0x00

    xvst       U0,     P1,     0x00
    xvst       U1,     P1,     0x20

    addi.d     S1,     S1,     0x20
    addi.d     S2,     S2,     0x20
    addi.d     I,      I,      -1
    add.d      P1,     P1,     T1

    blt        ZERO,   I,     .L_2I1

.L_2N3:
    andi       I,      N,     0x02
    beq        ZERO,   I,     .L_2N1

    xvld       U0,     S1,     0x00
    xvld       U1,     S2,     0x00

    xvpermi.q  U0,     U1,     0x02

    xvst       U0,     P2,     0x00

    addi.d     S1,     S1,     0x10
    addi.d     S2,     S2,     0x10
    addi.d     P2,     P2,     0x20

.L_2N1:
    addi.d     I,      N,      0x01
    beq        ZERO,   I,      .L_M1

    fld.d      F0,     S1,     0x00
    fld.d      F1,     S2,     0x00

    fst.d      F0,     P3,     0x00
    fst.d      F1,     P3,     0x08

    addi.d     S1,     S1,     0x08
    addi.d     S2,     S2,     0x08
    addi.d     P3,     P3,     0x10

.L_M1:
    andi       J,      M,      0x01
    beq        ZERO,   J,      .L_M0

    move       S1,     S0
    move       P1,     P0

    srai.d     I,      N,      0x02
    beq        ZERO,   I,      .L_1N3

.L_1I1:
    xvld       U0,    S1,      0x00

    xvst       U0,    P1,      0x00

    addi.d     S1,    S1,      0x20
    addi.d     I,     I,       -1
    add.d      P1,    P1,      T1

    blt        ZERO,  I,       .L_1I1

.L_1N3:
    andi       I,     N,       0x02
    beq        I,     ZERO,    .L_1N1

    fld.d      F0,    S1,      0x00
    fld.d      F1,    S1,      0x08

    fst.d      F0,    P2,      0x00
    fst.d      F1,    P2,      0x08

    addi.d     S1,    S1,      0x10
    addi.d     P2,    P2,      0x10

.L_1N1:
    andi       I,     N,       0x01
    beq        I,     ZERO,    .L_M0

    fld.d      F0,    S1,      0x00

    fst.d      F0,    P3,      0x00

.L_M0:
    LDARG      $r23,   $sp,   0
    addi.d     $sp,    $sp,   8
    jirl       $r0,    $r1,   0x00

    EPILOGUE
