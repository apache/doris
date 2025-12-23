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
#define D0     $xr8
#define D1     $xr9
#define D2     $xr10
#define D3     $xr11
#define D4     $xr12
#define D5     $xr13
#define D6     $xr14
#define D7     $xr15
#define D8     $xr16

    PROLOGUE

    addi.d     $sp,  $sp,  -8
    SDARG      $r23, $sp,  0

    move       TD,   DST   //boffset
    move       TS,   SRC   //aoffset

    slli.d     TL,   LDA,  0x02  //lda
    slli.d     TL,   TL,   0x01
    slli.d     T0,   TL,   0x01

    srai.d     I,    N,    0x01
    beq        I,    ZERO,  .L_N0

.L_J1: /* if (i > 0) I-- */
    move       S1,   TS         //a_offset1
    add.d      S2,   TS,   TL   //a_offset2
    srai.d     J,    M,    0x02
    add.d      TS,   TS,   T0

    beq        J,    ZERO,  .L_I3

.L_I1: /* if (j > 0) J-- */
    xvld       U0,   S1,   0x00
    xvld       U1,   S1,   0x00
    xvld       U2,   S2,   0x00

    xvpermi.q  U0,   U2,   0x02
    xvpermi.q  U2,   U1,   0x31

    xvpermi.d  U0,   U0,   0xd8
    xvpermi.d  U2,   U2,   0xd8

    xvst       U0,   TD,   0x00
    xvst       U2,   TD,   0x20

    addi.d     S1,   S1,   0x20   // a_offset1
    addi.d     S2,   S2,   0x20
    addi.d     TD,   TD,   0x40  // b_offset

    addi.d     J,    J,    -1
    blt        ZERO, J,    .L_I1

.L_I3:
    andi       J,    M,    0x03
    beq        J,    ZERO, .L_II20

.L_II1:  /* j = (m & 3)  if (j > 0) */
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

    addi.d     J,    J,    -1
    blt        ZERO, J,    .L_II1

.L_II20:
    addi.d     I,    I,    -1
    blt        ZERO, I,    .L_J1

.L_N0:  /* if(n&1)*/
    andi       I,     N,   0x01
    beq        ZERO,  I,   .L_N00

.L_N1:
    srai.d     J,     M,   0x02
    beq        ZERO,  J,   .L_N10

.L_N11: /* j = (m >> 2) if (j > 0) */
    xvld       U0,    TS,   0x00

    xvst       U0,    TD,   0x00

    addi.d     TS,    TS,   0x20   // a_offset
    addi.d     TD,    TD,   0x20   // b_offset

    addi.d     J,     J,   -1
    blt        ZERO,  J,   .L_N11

.L_N10:
    andi       J,     M,    0x03
    beq        J,     ZERO, .L_N00

.L_N12:  /* j = (m & 3)  if (j > 0) */
    fld.s      F0,    TS,   0x00
    fld.s      F1,    TS,   0x04

    fst.s      F0,    TD,   0x00
    fst.s      F1,    TD,   0x04

    addi.d     TS,    TS,   0x08   // a_offset
    addi.d     TD,    TD,   0x08   // b_offset

    addi.d     J,     J,   -1
    blt        ZERO,  J,   .L_N12

.L_N00:
    LDARG     $r23,  $sp, 0
    addi.d    $sp,   $sp, 8
    jirl      $r0,   $r1, 0x00

    EPILOGUE