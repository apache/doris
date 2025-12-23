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
#define T0     $r27
#define T1     $r28
#define T2     $r29
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
/* LSX vectors */
#define V0     $vr0
#define V1     $vr1
#define V2     $vr2
#define V3     $vr3
/* LASX vectors */
#define U0     $xr4
#define U1     $xr5
#define U2     $xr6
#define U3     $xr7
#define U4     $xr8
#define U5     $xr9
#define U6     $xr10
#define U7     $xr11

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

    addi.d     I,      ZERO,  0x06
    div.d      T0,     N,     I  // 1
    mul.d      T1,     I,     T0 // 6
    sub.d      N,      N,     T1 // 1

    srai.d     T2,     N,     0x02
    slli.d     T2,     T2,    0x02
    add.d      T2,     T1,    T2

    mul.d      P2,     M,     T1
    mul.d      P3,     M,     T2
    slli.d     P2,     P2,    0x03
    slli.d     P3,     P3,    0x03

    srai.d     T2,     N,     0x01
    srai.d     J,      M,     0x03
    slli.d     T2,     T2,    0x01
    add.d      T2,     T1,    T2

    add.d      P2,     DST,   P2
    mul.d      P4,     M,     T2
    add.d      P3,     DST,   P3
    slli.d     P4,     P4,    0x03
    slli.d     TL,     LDA,   0x03
    add.d      P4,     DST,   P4

    slli.d     T2,     TL,    0x01
    slli.d     T1,     M,     0x03
    mul.d      T1,     T1,    I
    beq        ZERO,   J,     .L_M7

.L_J1: /* J-- */
    move       S1,     S0
    add.d      S2,     S0,    TL
    add.d      S3,     S1,    T2
    add.d      S4,     S2,    T2
    add.d      S5,     S3,    T2
    add.d      S6,     S4,    T2
    add.d      S7,     S5,    T2
    add.d      S8,     S6,    T2
    add.d      S0,     S7,    T2

    move       P1,     P0
    addi.d     P0,     P0,    0x180

    move       I,      T0
    addi.d     J,      J,     -1
    beq        ZERO,   I,     .L_N7

.L_I1: /* I-- */
    xvld       U0,     S1,    0x00
    vld        V0,     S1,    0x20
    xvld       U1,     S2,    0x00
    vld        V1,     S2,    0x20
    xvld       U2,     S3,    0x00
    vld        V2,     S3,    0x20
    xvld       U3,     S4,    0x00
    vld        V3,     S4,    0x20

    xvst       U0,     P1,    0x00
    vst        V0,     P1,    0x20

    xvst       U1,     P1,    0x30
    vst        V1,     P1,    0x50

    xvst       U2,     P1,    0x60
    vst        V2,     P1,    0x80

    xvst       U3,     P1,    0x90
    vst        V3,     P1,    0xB0

    xvld       U0,     S5,    0x00
    vld        V0,     S5,    0x20
    xvld       U1,     S6,    0x00
    vld        V1,     S6,    0x20
    xvld       U2,     S7,    0x00
    vld        V2,     S7,    0x20
    xvld       U3,     S8,    0x00
    vld        V3,     S8,    0x20

    xvst       U0,     P1,    0xC0
    vst        V0,     P1,    0xE0

    xvst       U1,     P1,    0xF0
    vst        V1,     P1,    0x110

    xvst       U2,     P1,    0x120
    vst        V2,     P1,    0x140

    xvst       U3,     P1,    0x150
    vst        V3,     P1,    0x170

    addi.d     S1,     S1,    0x30
    addi.d     S2,     S2,    0x30
    addi.d     S3,     S3,    0x30
    addi.d     S4,     S4,    0x30
    addi.d     S5,     S5,    0x30
    addi.d     S6,     S6,    0x30
    addi.d     S7,     S7,    0x30
    addi.d     S8,     S8,    0x30
    addi.d     I,      I,     -1

    add.d      P1,     P1,    T1
    blt        ZERO,   I,     .L_I1

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

    xvst       U0,     P2,    0x00
    xvst       U1,     P2,    0x20
    xvst       U2,     P2,    0x40
    xvst       U3,     P2,    0x60
    xvst       U4,     P2,    0x80
    xvst       U5,     P2,    0xA0
    xvst       U6,     P2,    0xC0
    xvst       U7,     P2,    0xE0

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

    xvst       U0,     P3,    0x00
    xvst       U2,     P3,    0x20
    xvst       U4,     P3,    0x40
    xvst       U6,     P3,    0x60

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
    add.d      S3,     S1,    T2
    add.d      S4,     S2,    T2
    add.d      S0,     S3,    T2

    move       P1,     P0
    addi.d     P0,     P0,    0xC0

    move       I,      T0
    beq        ZERO,   I,     .L_4N7

.L_4I1: /* I-- */
    xvld       U0,     S1,    0x00
    vld        V0,     S1,    0x20
    xvld       U1,     S2,    0x00
    vld        V1,     S2,    0x20
    xvld       U2,     S3,    0x00
    vld        V2,     S3,    0x20
    xvld       U3,     S4,    0x00
    vld        V3,     S4,    0x20

    xvst       U0,     P1,    0x00
    vst        V0,     P1,    0x20

    xvst       U1,     P1,    0x30
    vst        V1,     P1,    0x50

    xvst       U2,     P1,    0x60
    vst        V2,     P1,    0x80

    xvst       U3,     P1,    0x90
    vst        V3,     P1,    0xB0

    addi.d     S1,     S1,    0x30
    addi.d     S2,     S2,    0x30
    addi.d     S3,     S3,    0x30
    addi.d     S4,     S4,    0x30

    addi.d     I,      I,     -1
    add.d      P1,     P1,    T1
    blt        ZERO,   I,     .L_4I1

.L_4N7:
    andi       I,      N,     0x04
    beq        ZERO,   I,     .L_4N3

    xvld       U0,     S1,    0x00
    xvld       U1,     S2,    0x00
    xvld       U2,     S3,    0x00
    xvld       U3,     S4,    0x00

    xvst       U0,     P2,    0x00
    xvst       U1,     P2,    0x20
    xvst       U2,     P2,    0x40
    xvst       U3,     P2,    0x60

    addi.d     S1,     S1,    0x20
    addi.d     S2,     S2,    0x20
    addi.d     S3,     S3,    0x20
    addi.d     S4,     S4,    0x20
    addi.d     P2,     P2,    0x80

.L_4N3:
    andi       I,      N,     0x02
    beq        ZERO,   I,     .L_4N1

    xvld       U0,     S1,    0x00
    xvld       U1,     S2,    0x00
    xvld       U2,     S3,    0x00
    xvld       U3,     S4,    0x00

    xvpermi.q  U0,     U1,    0x02
    xvpermi.q  U2,     U3,    0x02

    xvst       U0,     P3,    0x00
    xvst       U2,     P3,    0x20

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
    add.d      S0,     S0,    T2

    move       P1,     P0
    addi.d     P0,     P0,    0x60

    move       I,      T0
    beq        ZERO,   I,     .L_2N7

.L_2I1: /* I-- */
    xvld       U0,     S1,    0x00
    vld        V0,     S1,    0x20
    xvld       U1,     S2,    0x00
    vld        V1,     S2,    0x20

    xvst       U0,     P1,    0x00
    vst        V0,     P1,    0x20

    xvst       U1,     P1,    0x30
    vst        V1,     P1,    0x50

    addi.d     S1,     S1,    0x30
    addi.d     S2,     S2,    0x30
    addi.d     I,      I,     -1
    add.d      P1,     P1,    T1
    blt        ZERO,   I,     .L_2I1

.L_2N7:
    andi       I,      N,     0x04
    beq        ZERO,   I,     .L_2N3

    xvld       U0,     S1,    0x00
    xvld       U1,     S2,    0x00

    xvst       U0,     P2,    0x00
    xvst       U1,     P2,    0x20

    addi.d     S1,     S1,    0x20
    addi.d     S2,     S2,    0x20
    addi.d     P2,     P2,    0x40

.L_2N3:
    andi       I,      N,     0x02
    beq        ZERO,   I,     .L_2N1

    xvld       U0,     S1,    0x00
    xvld       U1,     S2,    0x00

    xvpermi.q  U0,     U1,    0x02

    xvst       U0,     P3,    0x00

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
    addi.d     P0,     P0,    0x30

    move       I,      T0
    beq        ZERO,   I,     .L_1N7

.L_1I1: /* I-- */
    xvld       U0,     S1,    0x00
    vld        V0,     S1,    0x20

    xvst       U0,     P1,    0x00
    vst        V0,     P1,    0x20

    addi.d     S1,     S1,    0x30
    addi.d     I,      I,     -1
    add.d      P1,     P1,    T1
    blt        ZERO,   I,     .L_1I1

.L_1N7:
    andi       I,      N,     0x04
    beq        ZERO,   I,     .L_1N3

    xvld       U0,     S1,    0x00

    xvst       U0,     P2,    0x00

    addi.d     S1,     S1,    0x20
    addi.d     P2,     P2,    0x20

.L_1N3:
    andi       I,      N,     0x02
    beq        ZERO,   I,     .L_1N1

    fld.d      F0,     S1,    0x00
    fld.d      F1,     S1,    0x08

    fst.d      F0,     P3,    0x00
    fst.d      F1,     P3,    0x08

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
