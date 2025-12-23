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


    PROLOGUE

    addi.d     $sp,  $sp,  -8
    SDARG      $r23, $sp,  0

    move       TS,     SRC          //aoffset
    move       TD,     DST          //boffset
    slli.d     TL,     LDA,   0x03  //lda
    slli.d     TL,     TL,    0x01

    srai.d     J,      N,     0x03  //j

    beq        J,      ZERO,  .L_N1

.L_J1:  /* if(j>0) j--*/
    move       S1,     TS     //aoffset1
    slli.d     T0,     TL,    0x01  //2*lda
    add.d      S2,     TS,    TL
    addi.d     TS,     TS,    0x80

    srai.d     I,      M,     0x01
    beq        ZERO,   I,     .L_J1M1

.L_J1I1:  /* if(i>0) i--*/
    xvld       U0,     S1,    0x00
    xvld       U1,     S1,    0x20
    xvld       U2,     S1,    0x40
    xvld       U3,     S1,    0x60
    xvld       U4,     S2,    0x00
    xvld       U5,     S2,    0x20
    xvld       U6,     S2,    0x40
    xvld       U7,     S2,    0x60

    xvst       U0,     TD,    0x00
    xvst       U1,     TD,    0x20
    xvst       U2,     TD,    0x40
    xvst       U3,     TD,    0x60
    xvst       U4,     TD,    0x80
    xvst       U5,     TD,    0xa0
    xvst       U6,     TD,    0xc0
    xvst       U7,     TD,    0xe0

    add.d      S1,     S1,    T0
    add.d      S2,     S2,    T0
    addi.d     TD,     TD,    0x100

    addi.d     I,      I,     -1
    blt        ZERO,   I,     .L_J1I1

.L_J1M1:  /* if(m&1) */
    andi       I,      M,     0x01
    beq        ZERO,   I,     .L_J0

    xvld       U0,     S1,    0x00
    xvld       U1,     S1,    0x20
    xvld       U2,     S1,    0x40
    xvld       U3,     S1,    0x60

    xvst       U0,     TD,    0x00
    xvst       U1,     TD,    0x20
    xvst       U2,     TD,    0x40
    xvst       U3,     TD,    0x60

    addi.d     TD,     TD,    0x80

.L_J0:
    addi.d     J,      J,     -1
    blt        ZERO,   J,     .L_J1

.L_N1:  /* if(n&4) */
    andi       I,      N,     0x04
    beq        ZERO,   I,     .L_N2

    move       S1,     TS     //aoffset1
    slli.d     T0,     TL,    0x01  //2*lda
    add.d      S2,     TS,    TL
    addi.d     TS,     TS,    0x40

    srai.d     I,      M,     0x01
    beq        ZERO,   I,     .L_N1M1

.L_N1I1:   /* if(i>0) i-- */
    xvld       U0,     S1,    0x00
    xvld       U1,     S1,    0x20
    xvld       U2,     S2,    0x00
    xvld       U3,     S2,    0x20

    xvst       U0,     TD,    0x00
    xvst       U1,     TD,    0x20
    xvst       U2,     TD,    0x40
    xvst       U3,     TD,    0x60

    add.d      S1,     S1,    T0
    add.d      S2,     S2,    T0
    addi.d     TD,     TD,    0x80

    addi.d     I,      I,     -1
    blt        ZERO,   I,     .L_N1I1

.L_N1M1:  /* if(m&1) */
    andi       I,      M,     0x01
    beq        ZERO,   I,     .L_N2

    xvld       U0,     S1,    0x00
    xvld       U1,     S1,    0x20

    xvst       U0,     TD,    0x00
    xvst       U1,     TD,    0x20

    addi.d     TD,     TD,    0x40

.L_N2:  /* if(n&2) */
    andi       I,      N,     0x02
    beq        ZERO,   I,     .L_N3

    move       S1,     TS     //aoffset1
    slli.d     T0,     TL,    0x01  //2*lda
    add.d      S2,     TS,    TL
    addi.d     TS,     TS,    0x20

    srai.d     I,      M,     0x01
    beq        ZERO,   I,     .L_N2M1

.L_N2I1:  /* if(i>0) i-- */
    xvld       U0,     S1,    0x00
    xvld       U1,     S2,    0x00

    xvst       U0,     TD,    0x00
    xvst       U1,     TD,    0x20

    add.d      S1,     S1,    T0
    add.d      S2,     S2,    T0

    addi.d     TD,     TD,    0x40

    addi.d     I,      I,     -1
    blt        ZERO,   I,     .L_N2I1

.L_N2M1:   /* if(m&1) */
    andi       I,      M,     0x01
    beq        ZERO,   I,     .L_N3

    xvld       U0,     S1,    0x00

    xvst       U0,     TD,    0x00

    addi.d     TD,     TD,    0x20

.L_N3:   /* if(n&1) */
    andi       I,      N,     0x01
    beq        ZERO,   I,     .L_N0

    move       S1,     TS     //aoffset1
    slli.d     T0,     TL,    0x01  //2*lda
    add.d      S2,     TS,    TL

    srai.d     I,      M,     0x01
    beq        ZERO,   I,     .L_N3M1

.L_N3I1:  /* if(i>0) i-- */
    vld       $vr0,     S1,    0x00
    vld       $vr1,     S2,    0x00

    vst       $vr0,     TD,    0x00
    vst       $vr1,     TD,    0x10

    add.d      S1,     S1,    T0
    add.d      S2,     S2,    T0
    addi.d     TD,     TD,    0x20

    addi.d     I,      I,     -1
    blt        ZERO,   I,     .L_N3I1

.L_N3M1:  /* if(m&1) */
    andi       I,      M,     0x01
    beq        ZERO,   I,     .L_N0

    vld       $vr0,     S1,    0x00

    vst       $vr0,     TD,    0x00

.L_N0:
    LDARG      $r23,   $sp,   0
    addi.d     $sp,    $sp,   8
    jirl       $r0,    $r1,   0x00

    EPILOGUE