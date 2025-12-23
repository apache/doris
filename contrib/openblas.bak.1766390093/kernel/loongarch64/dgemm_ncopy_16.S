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
#define S9     $r20
#define S10    $r23
#define S11    $r24
#define S12    $r25
#define S13    $r26
#define S14    $r27
#define S15    $r28
#define S16    $r29
#define TD     $r30
#define TS     $r31
#define TL     $r7
#define T0     $r6
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

    PROLOGUE

    addi.d     $sp,  $sp,  -0x90
    SDARG      $r23, $sp,  0x00
    SDARG      $r24, $sp,  0x08
    SDARG      $r25, $sp,  0x10
    SDARG      $r26, $sp,  0x18
    SDARG      $r27, $sp,  0x20
    SDARG      $r28, $sp,  0x28
    SDARG      $r29, $sp,  0x30
    SDARG      $r30, $sp,  0x38
    SDARG      $r31, $sp,  0x40
    ST         $f23, $sp,  0x48
    ST         $f24, $sp,  0x50
    ST         $f25, $sp,  0x58
    ST         $f26, $sp,  0x60
    ST         $f27, $sp,  0x68
    ST         $f28, $sp,  0x70
    ST         $f29, $sp,  0x78
    ST         $f30, $sp,  0x80
    ST         $f31, $sp,  0x88

    move       TD,   DST
    move       TS,   SRC
    slli.d     TL,   LDA,  0x03
    slli.d     T0,   TL,   0x01
    srai.d     J,    N,    0x04
    beq        J,    ZERO, .L_N8

.L_J1: /* J-- */
    move       S1,   TS
    add.d      S2,   TS,   TL
    srai.d     I,    M,    0x03
    add.d      S3,   S2,   TL
    addi.d     J,    J,    -1
    add.d      S4,   S3,   TL
    add.d      S5,   S3,   T0
    add.d      S6,   S4,   T0
    add.d      S7,   S5,   T0
    add.d      S8,   S6,   T0
    add.d      S9,   S7,   T0
    add.d      S10,  S8,   T0
    add.d      S11,  S9,   T0
    add.d      S12,  S10,  T0
    add.d      S13,  S11,  T0
    add.d      S14,  S12,  T0
    add.d      S15,  S13,  T0
    add.d      S16,  S14,  T0
    add.d      TS,   S15,  T0
    beq        I,    ZERO, .L_I7

.L_I1: /* I-- */
    xvld       U0,   S1,   0x00
    xvld       U1,   S2,   0x00
    xvld       U2,   S3,   0x00
    xvld       U3,   S4,   0x00
    xvld       U4,   S5,   0x00
    xvld       U5,   S6,   0x00
    xvld       U6,   S7,   0x00
    xvld       U7,   S8,   0x00
    xvld       U8,   S9,   0x00
    xvld       U9,   S10,  0x00
    xvld       U10,  S11,  0x00
    xvld       U11,  S12,  0x00
    xvld       U12,  S13,  0x00
    xvld       U13,  S14,  0x00
    xvld       U14,  S15,  0x00
    xvld       U15,  S16,  0x00

    xvpackev.d D0,   U1,   U0
    xvpackod.d D1,   U1,   U0
    xvpackev.d D2,   U3,   U2
    xvpackod.d D3,   U3,   U2
    xvpackev.d D4,   U5,   U4
    xvpackod.d D5,   U5,   U4
    xvpackev.d D6,   U7,   U6
    xvpackod.d D7,   U7,   U6

    xvpackev.d D8,   U9,   U8
    xvpackod.d D9,   U9,   U8
    xvpackev.d D10,  U11,  U10
    xvpackod.d D11,  U11,  U10
    xvpackev.d D12,  U13,  U12
    xvpackod.d D13,  U13,  U12
    xvpackev.d D14,  U15,  U14
    xvpackod.d D15,  U15,  U14

    xvand.v    U0,   D0,   D0
    xvpermi.q  D0,   D2,   0x02  // 0
    xvand.v    U4,   D4,   D4
    xvpermi.q  D4,   D6,   0x02  // 1
    xvand.v    U1,   D1,   D1
    xvpermi.q  D1,   D3,   0x02  // 4
    xvand.v    U5,   D5,   D5
    xvpermi.q  D5,   D7,   0x02  // 5
    xvpermi.q  D2,   U0,   0x31  // 8
    xvpermi.q  D6,   U4,   0x31  // 9
    xvpermi.q  D3,   U1,   0x31  // 12
    xvpermi.q  D7,   U5,   0x31  // 13

    xvand.v    U8,   D8,   D8
    xvpermi.q  D8,   D10,  0x02  // 2
    xvand.v    U12,  D12,  D12
    xvpermi.q  D12,  D14,  0x02  // 3
    xvand.v    U9,   D9,   D9
    xvpermi.q  D9,   D11,  0x02  // 6
    xvand.v    U13,  D13,  D13
    xvpermi.q  D13,  D15,  0x02  // 7
    xvpermi.q  D10,  U8,   0x31  // 10
    xvpermi.q  D14,  U12,  0x31  // 11
    xvpermi.q  D11,  U9,   0x31  // 14
    xvpermi.q  D15,  U13,  0x31  // 15

    xvst       D0,   TD,   0x00  // 0
    xvst       D4,   TD,   0x20  // 1
    xvst       D8,   TD,   0x40  // 2
    xvst       D12,  TD,   0x60  // 3
    xvst       D1,   TD,   0x80  // 4
    xvst       D5,   TD,   0xA0  // 5
    xvst       D9,   TD,   0xC0  // 6
    xvst       D13,  TD,   0xE0  // 7
    addi.d     TD,   TD,   0x100
    xvst       D2,   TD,   0x00  // 8
    xvst       D6,   TD,   0x20  // 9
    xvst       D10,  TD,   0x40  // 10
    xvst       D14,  TD,   0x60  // 11
    xvst       D3,   TD,   0x80  // 12
    xvst       D7,   TD,   0xA0  // 13
    xvst       D11,  TD,   0xC0  // 14
    xvst       D15,  TD,   0xE0  // 15
    addi.d     TD,   TD,   0x100

    xvld       U0,   S1,   0x20
    xvld       U1,   S2,   0x20
    xvld       U2,   S3,   0x20
    xvld       U3,   S4,   0x20
    xvld       U4,   S5,   0x20
    xvld       U5,   S6,   0x20
    xvld       U6,   S7,   0x20
    xvld       U7,   S8,   0x20
    xvld       U8,   S9,   0x20
    xvld       U9,   S10,  0x20
    xvld       U10,  S11,  0x20
    xvld       U11,  S12,  0x20
    xvld       U12,  S13,  0x20
    xvld       U13,  S14,  0x20
    xvld       U14,  S15,  0x20
    xvld       U15,  S16,  0x20

    xvpackev.d D0,   U1,   U0
    xvpackod.d D1,   U1,   U0
    xvpackev.d D2,   U3,   U2
    xvpackod.d D3,   U3,   U2
    xvpackev.d D4,   U5,   U4
    xvpackod.d D5,   U5,   U4
    xvpackev.d D6,   U7,   U6
    xvpackod.d D7,   U7,   U6

    xvpackev.d D8,   U9,   U8
    xvpackod.d D9,   U9,   U8
    xvpackev.d D10,  U11,  U10
    xvpackod.d D11,  U11,  U10
    xvpackev.d D12,  U13,  U12
    xvpackod.d D13,  U13,  U12
    xvpackev.d D14,  U15,  U14
    xvpackod.d D15,  U15,  U14

    xvand.v    U0,   D0,   D0
    xvpermi.q  D0,   D2,   0x02  // 0
    xvand.v    U4,   D4,   D4
    xvpermi.q  D4,   D6,   0x02  // 1
    xvand.v    U1,   D1,   D1
    xvpermi.q  D1,   D3,   0x02  // 4
    xvand.v    U5,   D5,   D5
    xvpermi.q  D5,   D7,   0x02  // 5
    xvpermi.q  D2,   U0,   0x31  // 8
    xvpermi.q  D6,   U4,   0x31  // 9
    xvpermi.q  D3,   U1,   0x31  // 12
    xvpermi.q  D7,   U5,   0x31  // 13

    xvand.v    U8,   D8,   D8
    xvpermi.q  D8,   D10,  0x02  // 2
    xvand.v    U12,  D12,  D12
    xvpermi.q  D12,  D14,  0x02  // 3
    xvand.v    U9,   D9,   D9
    xvpermi.q  D9,   D11,  0x02  // 6
    xvand.v    U13,  D13,  D13
    xvpermi.q  D13,  D15,  0x02  // 7
    xvpermi.q  D10,  U8,   0x31  // 10
    xvpermi.q  D14,  U12,  0x31  // 11
    xvpermi.q  D11,  U9,   0x31  // 14
    xvpermi.q  D15,  U13,  0x31  // 15

    xvst       D0,   TD,   0x00  // 0
    xvst       D4,   TD,   0x20  // 1
    xvst       D8,   TD,   0x40  // 2
    xvst       D12,  TD,   0x60  // 3
    xvst       D1,   TD,   0x80  // 4
    xvst       D5,   TD,   0xA0  // 5
    xvst       D9,   TD,   0xC0  // 6
    xvst       D13,  TD,   0xE0  // 7
    addi.d     TD,   TD,   0x100
    xvst       D2,   TD,   0x00  // 8
    xvst       D6,   TD,   0x20  // 9
    xvst       D10,  TD,   0x40  // 10
    xvst       D14,  TD,   0x60  // 11
    xvst       D3,   TD,   0x80  // 12
    xvst       D7,   TD,   0xA0  // 13
    xvst       D11,  TD,   0xC0  // 14
    xvst       D15,  TD,   0xE0  // 15
    addi.d     TD,   TD,   0x100


    addi.d     S1,   S1,   0x40
    addi.d     S2,   S2,   0x40
    addi.d     S3,   S3,   0x40
    addi.d     S4,   S4,   0x40
    addi.d     S5,   S5,   0x40
    addi.d     S6,   S6,   0x40
    addi.d     S7,   S7,   0x40
    addi.d     S8,   S8,   0x40
    addi.d     S9,   S9,   0x40
    addi.d     S10,  S10,  0x40
    addi.d     S11,  S11,  0x40
    addi.d     S12,  S12,  0x40
    addi.d     S13,  S13,  0x40
    addi.d     S14,  S14,  0x40
    addi.d     S15,  S15,  0x40
    addi.d     S16,  S16,  0x40

    addi.d     I,    I,    -1
    blt        ZERO, I,    .L_I1

.L_I7:
    andi      I,     M,    0x07
    beq       I,     ZERO, .L_I0

.L_II1: /* I-- */
    fld.d     F0,    S1,  0x00
    fld.d     F1,    S2,  0x00
    fld.d     F2,    S3,  0x00
    fld.d     F3,    S4,  0x00
    fld.d     F4,    S5,  0x00
    fld.d     F5,    S6,  0x00
    fld.d     F6,    S7,  0x00
    fld.d     F7,    S8,  0x00

    fst.d     F0,    TD,  0x00
    addi.d    S1,    S1,  0x08
    fst.d     F1,    TD,  0x08
    addi.d    S2,    S2,  0x08
    fst.d     F2,    TD,  0x10
    addi.d    S3,    S3,  0x08
    fst.d     F3,    TD,  0x18
    addi.d    S4,    S4,  0x08
    fst.d     F4,    TD,  0x20
    addi.d    S5,    S5,  0x08
    fst.d     F5,    TD,  0x28
    addi.d    S6,    S6,  0x08
    fst.d     F6,    TD,  0x30
    addi.d    S7,    S7,  0x08
    fst.d     F7,    TD,  0x38
    addi.d    S8,    S8,  0x08
    addi.d    TD,    TD,  0x40

    fld.d     F0,    S9,  0x00
    fld.d     F1,    S10, 0x00
    fld.d     F2,    S11, 0x00
    fld.d     F3,    S12, 0x00
    fld.d     F4,    S13, 0x00
    fld.d     F5,    S14, 0x00
    fld.d     F6,    S15, 0x00
    fld.d     F7,    S16, 0x00

    fst.d     F0,    TD,  0x00
    addi.d    S9,    S9,  0x08
    fst.d     F1,    TD,  0x08
    addi.d    S10,   S10, 0x08
    fst.d     F2,    TD,  0x10
    addi.d    S11,   S11, 0x08
    fst.d     F3,    TD,  0x18
    addi.d    S12,   S12, 0x08
    fst.d     F4,    TD,  0x20
    addi.d    S13,   S13, 0x08
    fst.d     F5,    TD,  0x28
    addi.d    S14,   S14, 0x08
    fst.d     F6,    TD,  0x30
    addi.d    S15,   S15, 0x08
    fst.d     F7,    TD,  0x38
    addi.d    S16,   S16, 0x08
    addi.d    TD,    TD,  0x40

    addi.d    I,     I,   -1
    blt       ZERO,  I,   .L_II1

.L_I0:
    blt       ZERO,  J,   .L_J1

.L_N8:
    andi      J,     N,   0x08
    beq       ZERO,  J,   .L_N4

    move       S1,   TS
    add.d      S2,   TS,   TL
    srai.d     I,    M,    0x03
    add.d      S3,   S2,   TL
    add.d      S4,   S2,   T0
    add.d      S5,   S3,   T0
    add.d      S6,   S4,   T0
    add.d      S7,   S5,   T0
    add.d      S8,   S6,   T0
    add.d      TS,   S7,   T0
    beq        I,    ZERO, .L_8I3

.L_8I1:  /* I-- */
    xvld       U0,   S1,   0x00
    xvld       U1,   S2,   0x00
    xvld       U2,   S3,   0x00
    xvld       U3,   S4,   0x00
    xvld       U4,   S5,   0x00
    xvld       U5,   S6,   0x00
    xvld       U6,   S7,   0x00
    xvld       U7,   S8,   0x00

    xvpackev.d D0,   U1,   U0
    xvpackod.d D1,   U1,   U0
    xvpackev.d D2,   U3,   U2
    xvpackod.d D3,   U3,   U2
    xvpackev.d D4,   U5,   U4
    xvpackod.d D5,   U5,   U4
    xvpackev.d D6,   U7,   U6
    xvpackod.d D7,   U7,   U6

    xvand.v    U0,   D0,   D0
    xvpermi.q  D0,   D2,   0x02  // 0
    xvand.v    U4,   D4,   D4
    xvpermi.q  D4,   D6,   0x02  // 1
    xvand.v    U1,   D1,   D1
    xvpermi.q  D1,   D3,   0x02  // 2
    xvand.v    U5,   D5,   D5
    xvpermi.q  D5,   D7,   0x02  // 3
    xvpermi.q  D2,   U0,   0x31  // 4
    xvpermi.q  D6,   U4,   0x31  // 5
    xvpermi.q  D3,   U1,   0x31  // 6
    xvpermi.q  D7,   U5,   0x31  // 7

    xvst       D0,   TD,   0x00
    xvst       D4,   TD,   0x20
    xvst       D1,   TD,   0x40
    xvst       D5,   TD,   0x60
    xvst       D2,   TD,   0x80
    xvst       D6,   TD,   0xA0
    xvst       D3,   TD,   0xC0
    xvst       D7,   TD,   0xE0
    addi.d     TD,   TD,   0x100

    xvld       U0,   S1,   0x20
    xvld       U1,   S2,   0x20
    xvld       U2,   S3,   0x20
    xvld       U3,   S4,   0x20
    xvld       U4,   S5,   0x20
    xvld       U5,   S6,   0x20
    xvld       U6,   S7,   0x20
    xvld       U7,   S8,   0x20

    xvpackev.d D0,   U1,   U0
    xvpackod.d D1,   U1,   U0
    xvpackev.d D2,   U3,   U2
    xvpackod.d D3,   U3,   U2
    xvpackev.d D4,   U5,   U4
    xvpackod.d D5,   U5,   U4
    xvpackev.d D6,   U7,   U6
    xvpackod.d D7,   U7,   U6

    xvand.v    U0,   D0,   D0
    xvpermi.q  D0,   D2,   0x02  // 0
    xvand.v    U4,   D4,   D4
    xvpermi.q  D4,   D6,   0x02  // 1
    xvand.v    U1,   D1,   D1
    xvpermi.q  D1,   D3,   0x02  // 2
    xvand.v    U5,   D5,   D5
    xvpermi.q  D5,   D7,   0x02  // 3
    xvpermi.q  D2,   U0,   0x31  // 4
    xvpermi.q  D6,   U4,   0x31  // 5
    xvpermi.q  D3,   U1,   0x31  // 6
    xvpermi.q  D7,   U5,   0x31  // 7

    xvst       D0,   TD,   0x00
    xvst       D4,   TD,   0x20
    xvst       D1,   TD,   0x40
    xvst       D5,   TD,   0x60
    xvst       D2,   TD,   0x80
    xvst       D6,   TD,   0xA0
    xvst       D3,   TD,   0xC0
    xvst       D7,   TD,   0xE0
    addi.d     TD,   TD,   0x100

    addi.d     S1,   S1,   0x40
    addi.d     S2,   S2,   0x40
    addi.d     S3,   S3,   0x40
    addi.d     S4,   S4,   0x40
    addi.d     S5,   S5,   0x40
    addi.d     S6,   S6,   0x40
    addi.d     S7,   S7,   0x40
    addi.d     S8,   S8,   0x40

    addi.d     I,    I,    -1
    blt        ZERO, I,    .L_8I1

.L_8I3:
    andi      I,     M,    0x07
    beq       I,     ZERO, .L_N4

.L_8I11:
    fld.d     F0,    S1,  0x00
    fld.d     F1,    S2,  0x00
    fld.d     F2,    S3,  0x00
    fld.d     F3,    S4,  0x00
    fld.d     F4,    S5,  0x00
    fld.d     F5,    S6,  0x00
    fld.d     F6,    S7,  0x00
    fld.d     F7,    S8,  0x00

    fst.d     F0,    TD,  0x00
    addi.d    S1,    S1,  0x08
    fst.d     F1,    TD,  0x08
    addi.d    S2,    S2,  0x08
    fst.d     F2,    TD,  0x10
    addi.d    S3,    S3,  0x08
    fst.d     F3,    TD,  0x18
    addi.d    S4,    S4,  0x08
    fst.d     F4,    TD,  0x20
    addi.d    S5,    S5,  0x08
    fst.d     F5,    TD,  0x28
    addi.d    S6,    S6,  0x08
    fst.d     F6,    TD,  0x30
    addi.d    S7,    S7,  0x08
    fst.d     F7,    TD,  0x38
    addi.d    S8,    S8,  0x08

    addi.d    TD,    TD,  0x40
    addi.d    I,     I,   -1
    blt       ZERO,  I,   .L_8I11

.L_N4:
    andi      J,     N,   0x04
    beq       ZERO,  J,   .L_N2

    move       S1,   TS
    add.d      S2,   TS,   TL
    srai.d     I,    M,    0x02
    add.d      S3,   S2,   TL
    add.d      S4,   S2,   T0
    add.d      TS,   S3,   T0
    beq        I,    ZERO, .L_I3

.L_4I1: /* I-- */
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
    blt        ZERO, I,    .L_4I1

.L_I3:
    andi      I,     M,    0x03
    beq       I,     ZERO, .L_N2

.L_4II1:
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
    blt       ZERO,  I,   .L_4II1

.L_N2:
    andi      J,     N,   0x02
    beq       ZERO,  J,   .L_N1

    move       S1,   TS
    add.d      S2,   TS,   TL
    srai.d     I,    M,    0x01
    add.d      TS,   S2,   TL
    beq        I,    ZERO, .L_NI1

.L_2I1: /* I-- */
    xvld       U0,   S1,   0x00
    xvld       U1,   S2,   0x00

    xvpackev.d D0,   U1,   U0
    xvpackod.d D1,   U1,   U0

    xvpermi.q  D0,   D1,   0x02  // 0

    xvst       D0,   TD,   0x00

    addi.d     S1,   S1,   0x10
    addi.d     S2,   S2,   0x10
    addi.d     TD,   TD,   0x20

    addi.d     I,    I,    -1
    blt        ZERO, I,    .L_2I1

.L_NI1:
    andi      I,     M,    0x01
    beq       I,     ZERO, .L_N1


    fld.d     F0,    S1,  0x00
    fld.d     F1,    S2,  0x00

    fst.d     F0,    TD,  0x00
    addi.d    S1,    S1,  0x08
    fst.d     F1,    TD,  0x08
    addi.d    S2,    S2,  0x08
    addi.d    TD,    TD,  0x10

.L_N1:
    andi      J,     N,   0x01
    beq       ZERO,  J,   .L_N0
    move      S1,    TS
    beq       ZERO,  M,   .L_N0

.L_M1:
    fld.d     F0,    S1,  0x00
    addi.d    S1,    S1,  0x08
    fst.d     F0,    TD,  0x00
    addi.d    TD,    TD,  0x08
    addi.d    M,     M,   -1
    blt       ZERO,  M,   .L_M1

.L_N0:
    LDARG      $r23, $sp,  0x00
    LDARG      $r24, $sp,  0x08
    LDARG      $r25, $sp,  0x10
    LDARG      $r26, $sp,  0x18
    LDARG      $r27, $sp,  0x20
    LDARG      $r28, $sp,  0x28
    LDARG      $r29, $sp,  0x30
    LDARG      $r30, $sp,  0x38
    LDARG      $r31, $sp,  0x40
    LD         $f23, $sp,  0x48
    LD         $f24, $sp,  0x50
    LD         $f25, $sp,  0x58
    LD         $f26, $sp,  0x60
    LD         $f27, $sp,  0x68
    LD         $f28, $sp,  0x70
    LD         $f29, $sp,  0x78
    LD         $f30, $sp,  0x80
    LD         $f31, $sp,  0x88
    addi.d     $sp,  $sp,  0x90
    jirl       $r0,  $r1,  0x00

    EPILOGUE
