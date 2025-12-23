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
#define U0     $vr0
#define U1     $vr1
#define U2     $vr2
#define U3     $vr3
#define U4     $vr4
#define U5     $vr5
#define U6     $vr6
#define U7     $vr7

    PROLOGUE
    push_if_used 7, 0

    move       S0,     SRC
    move       P0,     DST

    srai.d     T0,     N,     0x03
    srai.d     T1,     N,     0x02
    slli.d     T0,     T0,    0x03
    slli.d     T1,     T1,    0x02
    mul.d      P2,     M,     T0
    mul.d      P3,     M,     T1
    slli.d     P2,     P2,    0x03
    slli.d     P3,     P3,    0x03
    add.d      P2,     DST,   P2
    add.d      P3,     DST,   P3

    srai.d     T0,     N,     0x01
    slli.d     T0,     T0,    0x01
    mul.d      P4,     M,     T0
    slli.d     P4,     P4,    0x03
    add.d      P4,     DST,   P4

    slli.d     TL,     LDA,   0x03
    srai.d     J,      M,     0x03
    slli.d     T0,     TL,    0x01
    slli.d     T1,     M,     0x06
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
    addi.d     P0,     P0,    0x200

    srai.d     I,      N,     0x03
    addi.d     J,      J,     -1
    beq        ZERO,   I,     .L_N7

.L_I1: /* I-- */
    vld       U0,     S1,    0x00
    vld       U1,     S1,    0x10
    vld       U2,     S1,    0x20
    vld       U3,     S1,    0x30
    vld       U4,     S2,    0x00
    vld       U5,     S2,    0x10
    vld       U6,     S2,    0x20
    vld       U7,     S2,    0x30

    vst       U0,     P1,    0x00
    vst       U1,     P1,    0x10
    vst       U2,     P1,    0x20
    vst       U3,     P1,    0x30
    vst       U4,     P1,    0x40
    vst       U5,     P1,    0x50
    vst       U6,     P1,    0x60
    vst       U7,     P1,    0x70

    vld       U0,     S3,    0x00
    vld       U1,     S3,    0x10
    vld       U2,     S3,    0x20
    vld       U3,     S3,    0x30
    vld       U4,     S4,    0x00
    vld       U5,     S4,    0x10
    vld       U6,     S4,    0x20
    vld       U7,     S4,    0x30

    vst       U0,     P1,    0x80
    vst       U1,     P1,    0x90
    vst       U2,     P1,    0xa0
    vst       U3,     P1,    0xb0
    vst       U4,     P1,    0xc0
    vst       U5,     P1,    0xd0
    vst       U6,     P1,    0xe0
    vst       U7,     P1,    0xf0

    vld       U0,     S5,    0x00
    vld       U1,     S5,    0x10
    vld       U2,     S5,    0x20
    vld       U3,     S5,    0x30
    vld       U4,     S6,    0x00
    vld       U5,     S6,    0x10
    vld       U6,     S6,    0x20
    vld       U7,     S6,    0x30

    vst       U0,     P1,    0x100
    vst       U1,     P1,    0x110
    vst       U2,     P1,    0x120
    vst       U3,     P1,    0x130
    vst       U4,     P1,    0x140
    vst       U5,     P1,    0x150
    vst       U6,     P1,    0x160
    vst       U7,     P1,    0x170

    vld       U0,     S7,    0x00
    vld       U1,     S7,    0x10
    vld       U2,     S7,    0x20
    vld       U3,     S7,    0x30
    vld       U4,     S8,    0x00
    vld       U5,     S8,    0x10
    vld       U6,     S8,    0x20
    vld       U7,     S8,    0x30

    vst       U0,     P1,    0x180
    vst       U1,     P1,    0x190
    vst       U2,     P1,    0x1a0
    vst       U3,     P1,    0x1b0
    vst       U4,     P1,    0x1c0
    vst       U5,     P1,    0x1d0
    vst       U6,     P1,    0x1e0
    vst       U7,     P1,    0x1f0

    addi.d     S1,     S1,    0x40
    addi.d     S2,     S2,    0x40
    addi.d     S3,     S3,    0x40
    addi.d     S4,     S4,    0x40
    addi.d     S5,     S5,    0x40
    addi.d     S6,     S6,    0x40
    addi.d     S7,     S7,    0x40
    addi.d     S8,     S8,    0x40
    addi.d     I,      I,     -1
    add.d      P1,     P1,    T1
    blt        ZERO,   I,     .L_I1
.L_N7:
    andi       I,      N,     0x04
    beq        ZERO,   I,     .L_N3

    vld       U0,     S1,    0x00
    vld       U1,     S1,    0x10
    vld       U2,     S2,    0x00
    vld       U3,     S2,    0x10
    vld       U4,     S3,    0x00
    vld       U5,     S3,    0x10
    vld       U6,     S4,    0x00
    vld       U7,     S4,    0x10

    vst       U0,     P2,    0x00
    vst       U1,     P2,    0x10
    vst       U2,     P2,    0x20
    vst       U3,     P2,    0x30
    vst       U4,     P2,    0x40
    vst       U5,     P2,    0x50
    vst       U6,     P2,    0x60
    vst       U7,     P2,    0x70

    vld       U0,     S5,    0x00
    vld       U1,     S5,    0x10
    vld       U2,     S6,    0x00
    vld       U3,     S6,    0x10
    vld       U4,     S7,    0x00
    vld       U5,     S7,    0x10
    vld       U6,     S8,    0x00
    vld       U7,     S8,    0x10

    vst       U0,     P2,    0x80
    vst       U1,     P2,    0x90
    vst       U2,     P2,    0xa0
    vst       U3,     P2,    0xb0
    vst       U4,     P2,    0xc0
    vst       U5,     P2,    0xd0
    vst       U6,     P2,    0xe0
    vst       U7,     P2,    0xf0

    addi.d     S1,     S1,    0x20
    addi.d     S2,     S2,    0x20
    addi.d     S3,     S3,    0x20
    addi.d     S4,     S4,    0x20
    addi.d     S5,     S5,    0x20
    addi.d     S6,     S6,    0x20
    addi.d     S7,     S7,    0x20
    addi.d     S8,     S8,    0x20
    addi.d     P2,     P2,    0x100

.L_N3:
    andi       I,      N,     0x02
    beq        ZERO,   I,     .L_N1

    vld       U0,     S1,    0x00
    vld       U1,     S2,    0x00
    vld       U2,     S3,    0x00
    vld       U3,     S4,    0x00
    vld       U4,     S5,    0x00
    vld       U5,     S6,    0x00
    vld       U6,     S7,    0x00
    vld       U7,     S8,    0x00

    vst       U0,     P3,    0x00
    vst       U1,     P3,    0x10
    vst       U2,     P3,    0x20
    vst       U3,     P3,    0x30
    vst       U4,     P3,    0x40
    vst       U5,     P3,    0x50
    vst       U6,     P3,    0x60
    vst       U7,     P3,    0x70

    addi.d     S1,     S1,    0x10
    addi.d     S2,     S2,    0x10
    addi.d     S3,     S3,    0x10
    addi.d     S4,     S4,    0x10
    addi.d     S5,     S5,    0x10
    addi.d     S6,     S6,    0x10
    addi.d     S7,     S7,    0x10
    addi.d     S8,     S8,    0x10
    addi.d     P3,     P3,    0x80

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

    fst.d      F0,     P4,    0x00
    fst.d      F1,     P4,    0x08
    fst.d      F2,     P4,    0x10
    fst.d      F3,     P4,    0x18
    fst.d      F4,     P4,    0x20
    fst.d      F5,     P4,    0x28

    fst.d      F6,     P4,    0x30
    fst.d      F7,     P4,    0x38

    addi.d     S1,     S1,    0x08
    addi.d     S2,     S2,    0x08
    addi.d     S3,     S3,    0x08
    addi.d     S4,     S4,    0x08
    addi.d     S5,     S5,    0x08
    addi.d     S6,     S6,    0x08
    addi.d     S7,     S7,    0x08
    addi.d     S8,     S8,    0x08
    addi.d     P4,     P4,    0x40

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
    addi.d     P0,     P0,    0x100

    srai.d     I,      N,     0x03
    beq        ZERO,   I,     .L_4N7
.L_4I1: /* I-- */
    vld       U0,     S1,    0x00
    vld       U1,     S1,    0x10
    vld       U2,     S1,    0x20
    vld       U3,     S1,    0x30
    vld       U4,     S2,    0x00
    vld       U5,     S2,    0x10
    vld       U6,     S2,    0x20
    vld       U7,     S2,    0x30

    vst       U0,     P1,    0x00
    vst       U1,     P1,    0x10
    vst       U2,     P1,    0x20
    vst       U3,     P1,    0x30
    vst       U4,     P1,    0x40
    vst       U5,     P1,    0x50
    vst       U6,     P1,    0x60
    vst       U7,     P1,    0x70

    vld       U0,     S3,    0x00
    vld       U1,     S3,    0x10
    vld       U2,     S3,    0x20
    vld       U3,     S3,    0x30
    vld       U4,     S4,    0x00
    vld       U5,     S4,    0x10
    vld       U6,     S4,    0x20
    vld       U7,     S4,    0x30

    vst       U0,     P1,    0x80
    vst       U1,     P1,    0x90
    vst       U2,     P1,    0xa0
    vst       U3,     P1,    0xb0
    vst       U4,     P1,    0xc0
    vst       U5,     P1,    0xd0
    vst       U6,     P1,    0xe0
    vst       U7,     P1,    0xf0

    addi.d     S1,     S1,    0x40
    addi.d     S2,     S2,    0x40
    addi.d     S3,     S3,    0x40
    addi.d     S4,     S4,    0x40
    addi.d     I,      I,     -1
    add.d      P1,     P1,    T1
    blt        ZERO,   I,     .L_4I1
.L_4N7:
    andi       I,      N,     0x04
    beq        ZERO,   I,     .L_4N3

    vld       U0,     S1,    0x00
    vld       U1,     S1,    0x10
    vld       U2,     S2,    0x00
    vld       U3,     S2,    0x10
    vld       U4,     S3,    0x00
    vld       U5,     S3,    0x10
    vld       U6,     S4,    0x00
    vld       U7,     S4,    0x10

    vst       U0,     P2,    0x00
    vst       U1,     P2,    0x10
    vst       U2,     P2,    0x20
    vst       U3,     P2,    0x30
    vst       U4,     P2,    0x40
    vst       U5,     P2,    0x50
    vst       U6,     P2,    0x60
    vst       U7,     P2,    0x70

    addi.d     S1,     S1,    0x20
    addi.d     S2,     S2,    0x20
    addi.d     S3,     S3,    0x20
    addi.d     S4,     S4,    0x20
    addi.d     P2,     P2,    0x80

.L_4N3:
    andi       I,      N,     0x02
    beq        ZERO,   I,     .L_4N1

    vld       U0,     S1,    0x00
    vld       U1,     S2,    0x00
    vld       U2,     S3,    0x00
    vld       U3,     S4,    0x00

    vst       U0,     P3,    0x00
    vst       U1,     P3,    0x10
    vst       U2,     P3,    0x20
    vst       U3,     P3,    0x30

    addi.d     S1,     S1,    0x10
    addi.d     S2,     S2,    0x10
    addi.d     S3,     S3,    0x10
    addi.d     S4,     S4,    0x10
    addi.d     P3,     P3,    0x40

.L_4N1:
    andi        I,      N,     0x01
    beq         ZERO,   I,     .L_M3

    fld.d      F0,     S1,    0x00
    fld.d      F1,     S2,    0x00
    fld.d      F2,     S3,    0x00
    fld.d      F3,     S4,    0x00

    fst.d      F0,     P4,    0x00
    fst.d      F1,     P4,    0x08
    fst.d      F2,     P4,    0x10
    fst.d      F3,     P4,    0x18

    addi.d     S1,     S1,    0x08
    addi.d     S2,     S2,    0x08
    addi.d     S3,     S3,    0x08
    addi.d     S4,     S4,    0x08
    addi.d     P4,     P4,    0x20
.L_M3:
    andi       J,      M,     0x02
    beq        ZERO,   J,     .L_M1

    move       S1,     S0
    add.d      S2,     S0,    TL
    add.d      S0,     S0,    T0

    move       P1,     P0
    addi.d     P0,     P0,    0x80

    srai.d     I,      N,     0x03
    beq        ZERO,   I,     .L_2N7
.L_2I1: /* I-- */
    vld       U0,     S1,    0x00
    vld       U1,     S1,    0x10
    vld       U2,     S1,    0x20
    vld       U3,     S1,    0x30
    vld       U4,     S2,    0x00
    vld       U5,     S2,    0x10
    vld       U6,     S2,    0x20
    vld       U7,     S2,    0x30

    vst       U0,     P1,    0x00
    vst       U1,     P1,    0x10
    vst       U2,     P1,    0x20
    vst       U3,     P1,    0x30
    vst       U4,     P1,    0x40
    vst       U5,     P1,    0x50
    vst       U6,     P1,    0x60
    vst       U7,     P1,    0x70

    addi.d     S1,     S1,    0x40
    addi.d     S2,     S2,    0x40
    addi.d     I,      I,     -1
    add.d      P1,     P1,    T1
    blt        ZERO,   I,     .L_2I1
.L_2N7:
    andi       I,      N,     0x04
    beq        ZERO,   I,     .L_2N3

    vld       U0,     S1,    0x00
    vld       U1,     S1,    0x10
    vld       U2,     S2,    0x00
    vld       U3,     S2,    0x10

    vst       U0,     P2,    0x00
    vst       U1,     P2,    0x10
    vst       U2,     P2,    0x20
    vst       U3,     P2,    0x30

    addi.d     S1,     S1,    0x20
    addi.d     S2,     S2,    0x20
    addi.d     P2,     P2,    0x40

.L_2N3:
    andi       I,      N,     0x02
    beq        ZERO,   I,     .L_2N1

    vld       U0,     S1,    0x00
    vld       U1,     S2,    0x00

    vst       U0,     P3,    0x00
    vst       U1,     P3,    0x10

    addi.d     S1,     S1,    0x10
    addi.d     S2,     S2,    0x10
    addi.d     P3,     P3,    0x20

.L_2N1:
    andi       I,      N,     0x01
    beq        ZERO,   I,     .L_M1

    fld.d      F0,     S1,    0x00
    fld.d      F1,     S2,    0x00

    fst.d      F0,     P4,    0x00
    fst.d      F1,     P4,    0x08

    addi.d     S1,     S1,    0x08
    addi.d     S2,     S2,    0x08
    addi.d     P4,     P4,    0x10
.L_M1:
    andi       J,      M,     0x01
    beq        ZERO,   J,     .L_M0

    move       S1,     S0
    add.d      S2,     S0,    TL

    move       P1,     P0
    addi.d     P0,     P0,    0x40

    srai.d     I,      N,     0x03
    beq        ZERO,   I,     .L_1N7
.L_1I1: /* I-- */
    vld       U0,     S1,    0x00
    vld       U1,     S1,    0x10
    vld       U2,     S1,    0x20
    vld       U3,     S1,    0x30

    vst       U0,     P1,    0x00
    vst       U1,     P1,    0x10
    vst       U2,     P1,    0x20
    vst       U3,     P1,    0x30

    addi.d     S1,     S1,    0x40
    addi.d     I,      I,     -1
    add.d      P1,     P1,    T1
    blt        ZERO,   I,     .L_1I1

.L_1N7:
    andi       I,      N,     0x04
    beq        ZERO,   I,     .L_1N3

    vld       U0,     S1,    0x00
    vld       U1,     S1,    0x10

    vst       U0,     P2,    0x00
    vst       U1,     P2,    0x10

    addi.d     S1,     S1,    0x20
    addi.d     P2,     P2,    0x20

.L_1N3:
    andi       I,      N,     0x02
    beq        ZERO,   I,     .L_1N1

    vld       U0,     S1,    0x00
    vst       U0,     P3,    0x00

    addi.d     S1,     S1,    0x10
    addi.d     P3,     P3,    0x10

.L_1N1:
    andi       I,      N,     0x01
    beq        ZERO,   I,     .L_M0

    fld.d      F0,     S1,    0x00

    fst.d      F0,     P4,    0x00

    addi.d     S1,     S1,    0x08
    addi.d     P4,     P4,    0x08
.L_M0:
    pop_if_used 7, 0
    jirl       $r0,    $r1,   0x00
    EPILOGUE
