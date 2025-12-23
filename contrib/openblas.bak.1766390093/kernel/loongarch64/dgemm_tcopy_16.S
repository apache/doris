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
#define S5     $r16
#define S6     $r17
#define S7     $r18
#define S8     $r19
#define P0     $r20
#define P1     $r23
#define P2     $r24
#define P3     $r25
#define P4     $r26
#define P5     $r27
#define T0     $r28
#define T1     $r29
#define TL     $r7
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

    PROLOGUE

    addi.d     $sp,    $sp,   -56
    SDARG      $r23,   $sp,   0
    SDARG      $r24,   $sp,   8
    SDARG      $r25,   $sp,   16
    SDARG      $r26,   $sp,   24
    SDARG      $r27,   $sp,   32
    SDARG      $r28,   $sp,   40
    SDARG      $r29,   $sp,   48

    move       S0,     SRC
    move       P0,     DST

    srai.d     T0,     N,     0x04
    srai.d     T1,     N,     0x03
    slli.d     T0,     T0,    0x04
    slli.d     T1,     T1,    0x03
    mul.d      P2,     M,     T0
    mul.d      P3,     M,     T1
    slli.d     P2,     P2,    0x03
    slli.d     P3,     P3,    0x03
    add.d      P2,     DST,   P2
    add.d      P3,     DST,   P3

    srai.d     T0,     N,     0x02
    srai.d     T1,     N,     0x01
    slli.d     T0,     T0,    0x02
    slli.d     T1,     T1,    0x01
    mul.d      P4,     M,     T0
    mul.d      P5,     M,     T1
    slli.d     P4,     P4,    0x03
    slli.d     P5,     P5,    0x03
    add.d      P4,     DST,   P4
    add.d      P5,     DST,   P5

    slli.d     TL,     LDA,   0x03
    srai.d     J,      M,     0x03
    slli.d     T0,     TL,    0x01
    slli.d     T1,     M,     0x07
    beq        ZERO,   J,     .L_M7

.L_J1: /* J-- */
    move       S1,     S0
    add.d      S2,     S0,    TL
    add.d      S3,     S1,    T0
    add.d      S4,     S2,    T0
    add.d      S5,     S3,    T0
    add.d      S6,     S4,    T0
    add.d      S7,     S5,    T0
    add.d      S8,     S6,    T0
    add.d      S0,     S7,    T0

    move       P1,     P0
    addi.d     P0,     P0,    0x400

    srai.d     I,      N,     0x04
    addi.d     J,      J,     -1
    beq        ZERO,   I,     .L_N15

.L_I1: /* I-- */
    xvld       U0,     S1,    0x00
    xvld       U1,     S1,    0x20
    xvld       U2,     S1,    0x40
    xvld       U3,     S1,    0x60
    xvld       U4,     S2,    0x00
    xvld       U5,     S2,    0x20
    xvld       U6,     S2,    0x40
    xvld       U7,     S2,    0x60

    xvst       U0,     P1,    0x00
    xvst       U1,     P1,    0x20
    xvst       U2,     P1,    0x40
    xvst       U3,     P1,    0x60
    xvst       U4,     P1,    0x80
    xvst       U5,     P1,    0xA0
    xvst       U6,     P1,    0xC0
    xvst       U7,     P1,    0xE0

    xvld       U0,     S3,    0x00
    xvld       U1,     S3,    0x20
    xvld       U2,     S3,    0x40
    xvld       U3,     S3,    0x60
    xvld       U4,     S4,    0x00
    xvld       U5,     S4,    0x20
    xvld       U6,     S4,    0x40
    xvld       U7,     S4,    0x60

    xvst       U0,     P1,    0x100
    xvst       U1,     P1,    0x120
    xvst       U2,     P1,    0x140
    xvst       U3,     P1,    0x160
    xvst       U4,     P1,    0x180
    xvst       U5,     P1,    0x1A0
    xvst       U6,     P1,    0x1C0
    xvst       U7,     P1,    0x1E0

    xvld       U0,     S5,    0x00
    xvld       U1,     S5,    0x20
    xvld       U2,     S5,    0x40
    xvld       U3,     S5,    0x60
    xvld       U4,     S6,    0x00
    xvld       U5,     S6,    0x20
    xvld       U6,     S6,    0x40
    xvld       U7,     S6,    0x60

    xvst       U0,     P1,    0x200
    xvst       U1,     P1,    0x220
    xvst       U2,     P1,    0x240
    xvst       U3,     P1,    0x260
    xvst       U4,     P1,    0x280
    xvst       U5,     P1,    0x2A0
    xvst       U6,     P1,    0x2C0
    xvst       U7,     P1,    0x2E0

    xvld       U0,     S7,    0x00
    xvld       U1,     S7,    0x20
    xvld       U2,     S7,    0x40
    xvld       U3,     S7,    0x60
    xvld       U4,     S8,    0x00
    xvld       U5,     S8,    0x20
    xvld       U6,     S8,    0x40
    xvld       U7,     S8,    0x60

    xvst       U0,     P1,    0x300
    xvst       U1,     P1,    0x320
    xvst       U2,     P1,    0x340
    xvst       U3,     P1,    0x360
    xvst       U4,     P1,    0x380
    xvst       U5,     P1,    0x3A0
    xvst       U6,     P1,    0x3C0
    xvst       U7,     P1,    0x3E0

    addi.d     S1,     S1,    0x80
    addi.d     S2,     S2,    0x80
    addi.d     S3,     S3,    0x80
    addi.d     S4,     S4,    0x80
    addi.d     S5,     S5,    0x80
    addi.d     S6,     S6,    0x80
    addi.d     S7,     S7,    0x80
    addi.d     S8,     S8,    0x80
    addi.d     I,      I,     -1
    add.d      P1,     P1,    T1
    blt        ZERO,   I,     .L_I1

.L_N15:
    andi       I,      N,     0x08
    beq        ZERO,   I,     .L_N7

    xvld       U0,     S1,    0x00
    xvld       U1,     S1,    0x20
    xvld       U2,     S2,    0x00
    xvld       U3,     S2,    0x20
    xvld       U4,     S3,    0x00
    xvld       U5,     S3,    0x20
    xvld       U6,     S4,    0x00
    xvld       U7,     S4,    0x20

    xvst       U0,     P2,    0x00
    xvst       U1,     P2,    0x20
    xvst       U2,     P2,    0x40
    xvst       U3,     P2,    0x60
    xvst       U4,     P2,    0x80
    xvst       U5,     P2,    0xA0
    xvst       U6,     P2,    0xC0
    xvst       U7,     P2,    0xE0

    xvld       U0,     S5,    0x00
    xvld       U1,     S5,    0x20
    xvld       U2,     S6,    0x00
    xvld       U3,     S6,    0x20
    xvld       U4,     S7,    0x00
    xvld       U5,     S7,    0x20
    xvld       U6,     S8,    0x00
    xvld       U7,     S8,    0x20

    xvst       U0,     P2,    0x100
    xvst       U1,     P2,    0x120
    xvst       U2,     P2,    0x140
    xvst       U3,     P2,    0x160
    xvst       U4,     P2,    0x180
    xvst       U5,     P2,    0x1A0
    xvst       U6,     P2,    0x1C0
    xvst       U7,     P2,    0x1E0

    addi.d     S1,     S1,    0x40
    addi.d     S2,     S2,    0x40
    addi.d     S3,     S3,    0x40
    addi.d     S4,     S4,    0x40
    addi.d     S5,     S5,    0x40
    addi.d     S6,     S6,    0x40
    addi.d     S7,     S7,    0x40
    addi.d     S8,     S8,    0x40
    addi.d     P2,     P2,    0x200

.L_N7:
    andi       I,      N,     0x04
    beq        ZERO,   I,     .L_N3

    xvld       U0,     S1,    0x00
    xvld       U1,     S2,    0x00
    xvld       U2,     S3,    0x00
    xvld       U3,     S4,    0x00
    xvld       U4,     S5,    0x00
    xvld       U5,     S6,    0x00
    xvld       U6,     S7,    0x00
    xvld       U7,     S8,    0x00

    xvst       U0,     P3,    0x00
    xvst       U1,     P3,    0x20
    xvst       U2,     P3,    0x40
    xvst       U3,     P3,    0x60
    xvst       U4,     P3,    0x80
    xvst       U5,     P3,    0xA0
    xvst       U6,     P3,    0xC0
    xvst       U7,     P3,    0xE0

    addi.d     S1,     S1,    0x20
    addi.d     S2,     S2,    0x20
    addi.d     S3,     S3,    0x20
    addi.d     S4,     S4,    0x20
    addi.d     S5,     S5,    0x20
    addi.d     S6,     S6,    0x20
    addi.d     S7,     S7,    0x20
    addi.d     S8,     S8,    0x20
    addi.d     P3,     P3,    0x100

.L_N3:
    andi       I,      N,     0x02
    beq        ZERO,   I,     .L_N1

    xvld       U0,     S1,    0x00
    xvld       U1,     S2,    0x00
    xvld       U2,     S3,    0x00
    xvld       U3,     S4,    0x00
    xvld       U4,     S5,    0x00
    xvld       U5,     S6,    0x00
    xvld       U6,     S7,    0x00
    xvld       U7,     S8,    0x00

    xvpermi.q  U0,     U1,    0x02
    xvpermi.q  U2,     U3,    0x02
    xvpermi.q  U4,     U5,    0x02
    xvpermi.q  U6,     U7,    0x02

    xvst       U0,     P4,    0x00
    xvst       U2,     P4,    0x20
    xvst       U4,     P4,    0x40
    xvst       U6,     P4,    0x60

    addi.d     S1,     S1,    0x10
    addi.d     S2,     S2,    0x10
    addi.d     S3,     S3,    0x10
    addi.d     S4,     S4,    0x10
    addi.d     S5,     S5,    0x10
    addi.d     S6,     S6,    0x10
    addi.d     S7,     S7,    0x10
    addi.d     S8,     S8,    0x10
    addi.d     P4,     P4,    0x80

.L_N1:
    andi       I,      N,     0x01
    beq        ZERO,   I,     .L_N0

    fld.d      F0,     S1,    0x00
    fld.d      F1,     S2,    0x00
    fld.d      F2,     S3,    0x00
    fld.d      F3,     S4,    0x00
    fld.d      F4,     S5,    0x00
    fld.d      F5,     S6,    0x00
    fld.d      F6,     S7,    0x00
    fld.d      F7,     S8,    0x00

    fst.d      F0,     P5,    0x00
    fst.d      F1,     P5,    0x08
    fst.d      F2,     P5,    0x10
    fst.d      F3,     P5,    0x18
    fst.d      F4,     P5,    0x20
    fst.d      F5,     P5,    0x28
    fst.d      F6,     P5,    0x30
    fst.d      F7,     P5,    0x38

    addi.d     S1,     S1,    0x08
    addi.d     S2,     S2,    0x08
    addi.d     S3,     S3,    0x08
    addi.d     S4,     S4,    0x08
    addi.d     S5,     S5,    0x08
    addi.d     S6,     S6,    0x08
    addi.d     S7,     S7,    0x08
    addi.d     S8,     S8,    0x08
    addi.d     P5,     P5,    0x40

.L_N0:
    blt        ZERO,   J,     .L_J1

.L_M7:
    andi       J,      M,     0x04
    beq        ZERO,   J,     .L_M3

    move       S1,     S0
    add.d      S2,     S0,    TL
    add.d      S3,     S1,    T0
    add.d      S4,     S2,    T0
    add.d      S0,     S3,    T0

    move       P1,     P0
    addi.d     P0,     P0,    0x200

    srai.d     I,      N,     0x04
    beq        ZERO,   I,     .L_4N15

.L_4I1: /* I-- */
    xvld       U0,     S1,    0x00
    xvld       U1,     S1,    0x20
    xvld       U2,     S1,    0x40
    xvld       U3,     S1,    0x60
    xvld       U4,     S2,    0x00
    xvld       U5,     S2,    0x20
    xvld       U6,     S2,    0x40
    xvld       U7,     S2,    0x60

    xvst       U0,     P1,    0x00
    xvst       U1,     P1,    0x20
    xvst       U2,     P1,    0x40
    xvst       U3,     P1,    0x60
    xvst       U4,     P1,    0x80
    xvst       U5,     P1,    0xA0
    xvst       U6,     P1,    0xC0
    xvst       U7,     P1,    0xE0

    xvld       U0,     S3,    0x00
    xvld       U1,     S3,    0x20
    xvld       U2,     S3,    0x40
    xvld       U3,     S3,    0x60
    xvld       U4,     S4,    0x00
    xvld       U5,     S4,    0x20
    xvld       U6,     S4,    0x40
    xvld       U7,     S4,    0x60

    xvst       U0,     P1,    0x100
    xvst       U1,     P1,    0x120
    xvst       U2,     P1,    0x140
    xvst       U3,     P1,    0x160
    xvst       U4,     P1,    0x180
    xvst       U5,     P1,    0x1A0
    xvst       U6,     P1,    0x1C0
    xvst       U7,     P1,    0x1E0

    addi.d     S1,     S1,    0x80
    addi.d     S2,     S2,    0x80
    addi.d     S3,     S3,    0x80
    addi.d     S4,     S4,    0x80
    addi.d     I,      I,     -1
    add.d      P1,     P1,    T1
    blt        ZERO,   I,     .L_4I1

.L_4N15:
    andi       I,      N,     0x08
    beq        ZERO,   I,     .L_4N7

    xvld       U0,     S1,    0x00
    xvld       U1,     S1,    0x20
    xvld       U2,     S2,    0x00
    xvld       U3,     S2,    0x20
    xvld       U4,     S3,    0x00
    xvld       U5,     S3,    0x20
    xvld       U6,     S4,    0x00
    xvld       U7,     S4,    0x20

    xvst       U0,     P2,    0x00
    xvst       U1,     P2,    0x20
    xvst       U2,     P2,    0x40
    xvst       U3,     P2,    0x60
    xvst       U4,     P2,    0x80
    xvst       U5,     P2,    0xA0
    xvst       U6,     P2,    0xC0
    xvst       U7,     P2,    0xE0

    addi.d     S1,     S1,    0x40
    addi.d     S2,     S2,    0x40
    addi.d     S3,     S3,    0x40
    addi.d     S4,     S4,    0x40
    addi.d     P2,     P2,    0x100

.L_4N7:
    andi       I,      N,     0x04
    beq        ZERO,   I,     .L_4N3

    xvld       U0,     S1,    0x00
    xvld       U1,     S2,    0x00
    xvld       U2,     S3,    0x00
    xvld       U3,     S4,    0x00

    xvst       U0,     P3,    0x00
    xvst       U1,     P3,    0x20
    xvst       U2,     P3,    0x40
    xvst       U3,     P3,    0x60

    addi.d     S1,     S1,    0x20
    addi.d     S2,     S2,    0x20
    addi.d     S3,     S3,    0x20
    addi.d     S4,     S4,    0x20
    addi.d     P3,     P3,    0x80

.L_4N3:
    andi       I,      N,     0x02
    beq        ZERO,   I,     .L_4N1

    xvld       U0,     S1,    0x00
    xvld       U1,     S2,    0x00
    xvld       U2,     S3,    0x00
    xvld       U3,     S4,    0x00

    xvpermi.q  U0,     U1,    0x02
    xvpermi.q  U2,     U3,    0x02

    xvst       U0,     P4,    0x00
    xvst       U2,     P4,    0x20

    addi.d     S1,     S1,    0x10
    addi.d     S2,     S2,    0x10
    addi.d     S3,     S3,    0x10
    addi.d     S4,     S4,    0x10
    addi.d     P4,     P4,    0x40

.L_4N1:
    andi        I,      N,     0x01
    beq         ZERO,   I,     .L_M3

    fld.d      F0,     S1,    0x00
    fld.d      F1,     S2,    0x00
    fld.d      F2,     S3,    0x00
    fld.d      F3,     S4,    0x00

    fst.d      F0,     P5,    0x00
    fst.d      F1,     P5,    0x08
    fst.d      F2,     P5,    0x10
    fst.d      F3,     P5,    0x18

    addi.d     S1,     S1,    0x08
    addi.d     S2,     S2,    0x08
    addi.d     S3,     S3,    0x08
    addi.d     S4,     S4,    0x08
    addi.d     P5,     P5,    0x20

.L_M3:
    andi       J,      M,     0x02
    beq        ZERO,   J,     .L_M1

    move       S1,     S0
    add.d      S2,     S0,    TL
    add.d      S0,     S0,    T0

    move       P1,     P0
    addi.d     P0,     P0,    0x100

    srai.d     I,      N,     0x04
    beq        ZERO,   I,     .L_2N15

.L_2I1: /* I-- */
    xvld       U0,     S1,    0x00
    xvld       U1,     S1,    0x20
    xvld       U2,     S1,    0x40
    xvld       U3,     S1,    0x60
    xvld       U4,     S2,    0x00
    xvld       U5,     S2,    0x20
    xvld       U6,     S2,    0x40
    xvld       U7,     S2,    0x60

    xvst       U0,     P1,    0x00
    xvst       U1,     P1,    0x20
    xvst       U2,     P1,    0x40
    xvst       U3,     P1,    0x60
    xvst       U4,     P1,    0x80
    xvst       U5,     P1,    0xA0
    xvst       U6,     P1,    0xC0
    xvst       U7,     P1,    0xE0

    addi.d     S1,     S1,    0x80
    addi.d     S2,     S2,    0x80
    addi.d     I,      I,     -1
    add.d      P1,     P1,    T1
    blt        ZERO,   I,     .L_2I1

.L_2N15:
    andi       I,      N,     0x08
    beq        ZERO,   I,     .L_2N7

    xvld       U0,     S1,    0x00
    xvld       U1,     S1,    0x20
    xvld       U2,     S2,    0x00
    xvld       U3,     S2,    0x20

    xvst       U0,     P2,    0x00
    xvst       U1,     P2,    0x20
    xvst       U2,     P2,    0x40
    xvst       U3,     P2,    0x60

    addi.d     S1,     S1,    0x40
    addi.d     S2,     S2,    0x40
    addi.d     P2,     P2,    0x80

.L_2N7:
    andi       I,      N,     0x04
    beq        ZERO,   I,     .L_2N3

    xvld       U0,     S1,    0x00
    xvld       U1,     S2,    0x00

    xvst       U0,     P3,    0x00
    xvst       U1,     P3,    0x20

    addi.d     S1,     S1,    0x20
    addi.d     S2,     S2,    0x20
    addi.d     P3,     P3,    0x40

.L_2N3:
    andi       I,      N,     0x02
    beq        ZERO,   I,     .L_2N1

    xvld       U0,     S1,    0x00
    xvld       U1,     S2,    0x00

    xvpermi.q  U0,     U1,    0x02

    xvst       U0,     P4,    0x00

    addi.d     S1,     S1,    0x10
    addi.d     S2,     S2,    0x10
    addi.d     P4,     P4,    0x20

.L_2N1:
    andi       I,      N,     0x01
    beq        ZERO,   I,     .L_M1

    fld.d      F0,     S1,    0x00
    fld.d      F1,     S2,    0x00

    fst.d      F0,     P5,    0x00
    fst.d      F1,     P5,    0x08

    addi.d     S1,     S1,    0x08
    addi.d     S2,     S2,    0x08
    addi.d     P5,     P5,    0x10

.L_M1:
    andi       J,      M,     0x01
    beq        ZERO,   J,     .L_M0

    move       S1,     S0
    add.d      S2,     S0,    TL

    move       P1,     P0
    addi.d     P0,     P0,    0x80

    srai.d     I,      N,     0x04
    beq        ZERO,   I,     .L_1N15

.L_1I1: /* I-- */
    xvld       U0,     S1,    0x00
    xvld       U1,     S1,    0x20
    xvld       U2,     S1,    0x40
    xvld       U3,     S1,    0x60

    xvst       U0,     P1,    0x00
    xvst       U1,     P1,    0x20
    xvst       U2,     P1,    0x40
    xvst       U3,     P1,    0x60

    addi.d     S1,     S1,    0x80
    addi.d     I,      I,     -1
    add.d      P1,     P1,    T1
    blt        ZERO,   I,     .L_1I1

.L_1N15:
    andi       I,      N,     0x08
    beq        ZERO,   I,     .L_1N7

    xvld       U0,     S1,    0x00
    xvld       U1,     S1,    0x20

    xvst       U0,     P2,    0x00
    xvst       U1,     P2,    0x20

    addi.d     S1,     S1,    0x40
    addi.d     P2,     P2,    0x40

.L_1N7:
    andi       I,      N,     0x04
    beq        ZERO,   I,     .L_1N3

    xvld       U0,     S1,    0x00

    xvst       U0,     P3,    0x00

    addi.d     S1,     S1,    0x20
    addi.d     P3,     P3,    0x20

.L_1N3:
    andi       I,      N,     0x02
    beq        ZERO,   I,     .L_1N1

    fld.d      F0,     S1,    0x00
    fld.d      F1,     S1,    0x08

    fst.d      F0,     P4,    0x00
    fst.d      F1,     P4,    0x08

    addi.d     S1,     S1,    0x10
    addi.d     P4,     P4,    0x10

.L_1N1:
    andi       I,      N,     0x01
    beq        ZERO,   I,     .L_M0

    fld.d      F0,     S1,    0x00

    fst.d      F0,     P5,    0x00

    addi.d     S1,     S1,    0x08
    addi.d     P5,     P5,    0x08

.L_M0:
    LDARG      $r23,   $sp,   0
    LDARG      $r24,   $sp,   8
    LDARG      $r25,   $sp,   16
    LDARG      $r26,   $sp,   24
    LDARG      $r27,   $sp,   32
    LDARG      $r28,   $sp,   40
    LDARG      $r29,   $sp,   48
    addi.d     $sp,    $sp,   56
    jirl       $r0,    $r1,   0x00

    EPILOGUE
