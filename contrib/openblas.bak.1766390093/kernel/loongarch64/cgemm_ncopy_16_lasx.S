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
#define S9     $r23
#define S10    $r24
#define S11    $r25
#define S12    $r26
#define S13    $r27
#define S14    $r28
#define S15    $r29
#define S16    $r30
#define TD     $r20
#define TS     $r11
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
#define F8     $f8
#define F9     $f9
#define F10    $f10
#define F11    $f11
#define F12    $f12
#define F13    $f13
#define F14    $f14
#define F15    $f15
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

    addi.d     $sp,  $sp,  -64
    SDARG      $r23, $sp,  0
    SDARG      $r24, $sp,  8
    SDARG      $r25, $sp,  16
    SDARG      $r26, $sp,  24
    SDARG      $r27, $sp,  32
    SDARG      $r28, $sp,  40
    SDARG      $r29, $sp,  48
    SDARG      $r30, $sp,  56

    move       TD,   DST  //boffset
    move       TS,   SRC  //aoffset
    slli.d     TL,   LDA,  0x03  //lda
    srai.d     J,    N,    0x04  //j
    beq        J,    ZERO, .L_N8

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
    add.d      S9,   S8,   TL
    add.d      S10,  S9,   TL
    add.d      S11,  S10,  TL
    add.d      S12,  S11,  TL
    add.d      S13,  S12,  TL
    add.d      S14,  S13,  TL
    add.d      S15,  S14,  TL
    add.d      S16,  S15,  TL
    add.d      TS,   S16,  TL
    beq        I,    ZERO, .L_J11

.L_I1:  /* if(i>0) i--*/
    fld.d      F0,   S1,   0x00
    fld.d      F1,   S2,   0x00
    fld.d      F2,   S3,   0x00
    fld.d      F3,   S4,   0x00
    fld.d      F4,   S5,   0x00
    fld.d      F5,   S6,   0x00
    fld.d      F6,   S7,   0x00
    fld.d      F7,   S8,   0x00

    fst.d      F0,   TD,   0x00
    fst.d      F1,   TD,   0x08
    fst.d      F2,   TD,   0x10
    fst.d      F3,   TD,   0x18
    fst.d      F4,   TD,   0x20
    fst.d      F5,   TD,   0x28
    fst.d      F6,   TD,   0x30
    fst.d      F7,   TD,   0x38

    fld.d      F0,   S9,   0x00
    fld.d      F1,   S10,  0x00
    fld.d      F2,   S11,  0x00
    fld.d      F3,   S12,  0x00
    fld.d      F4,   S13,  0x00
    fld.d      F5,   S14,  0x00
    fld.d      F6,   S15,  0x00
    fld.d      F7,   S16,  0x00

    fst.d      F0,   TD,   0x40
    fst.d      F1,   TD,   0x48
    fst.d      F2,   TD,   0x50
    fst.d      F3,   TD,   0x58
    fst.d      F4,   TD,   0x60
    fst.d      F5,   TD,   0x68
    fst.d      F6,   TD,   0x70
    fst.d      F7,   TD,   0x78

    addi.d     S1,   S1,   0x08
    addi.d     S2,   S2,   0x08
    addi.d     S3,   S3,   0x08
    addi.d     S4,   S4,   0x08
    addi.d     S5,   S5,   0x08
    addi.d     S6,   S6,   0x08
    addi.d     S7,   S7,   0x08
    addi.d     S8,   S8,   0x08
    addi.d     S9,   S9,   0x08
    addi.d     S10,  S10,  0x08
    addi.d     S11,  S11,  0x08
    addi.d     S12,  S12,  0x08
    addi.d     S13,  S13,  0x08
    addi.d     S14,  S14,  0x08
    addi.d     S15,  S15,  0x08
    addi.d     S16,  S16,  0x08
    addi.d     TD,   TD,   0x80
    addi.d     I,    I,    -1
    blt        ZERO, I,    .L_I1

.L_J11: /* j--*/
    addi.d     J,    J,    -1
    blt        ZERO, J,    .L_J1

.L_N8:  /* if(n&8)*/
    andi       I,     N,    0x08
    beq        I,     ZERO, .L_N4

    move       S1,   TS
    add.d      S2,   TS,   TL
    move       I,    M
    add.d      S3,   S2,   TL
    add.d      S4,   S3,   TL
    add.d      S5,   S4,   TL
    add.d      S6,   S5,   TL
    add.d      S7,   S6,   TL
    add.d      S8,   S7,   TL
    add.d      TS,   S8,   TL
    beq        I,     ZERO, .L_N4

.L_N81:  /* if(i>0) i--*/
    fld.d      F0,   S1,   0x00
    fld.d      F1,   S2,   0x00
    fld.d      F2,   S3,   0x00
    fld.d      F3,   S4,   0x00
    fld.d      F4,   S5,   0x00
    fld.d      F5,   S6,   0x00
    fld.d      F6,   S7,   0x00
    fld.d      F7,   S8,   0x00

    fst.d      F0,   TD,   0x00
    fst.d      F1,   TD,   0x08
    fst.d      F2,   TD,   0x10
    fst.d      F3,   TD,   0x18
    fst.d      F4,   TD,   0x20
    fst.d      F5,   TD,   0x28
    fst.d      F6,   TD,   0x30
    fst.d      F7,   TD,   0x38

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
    blt        ZERO, I,    .L_N81

.L_N4:  /* if(n&4)*/
    andi       I,     N,    0x04
    beq        I,     ZERO, .L_N2

    move       S1,   TS
    add.d      S2,   TS,   TL
    move       I,    M
    add.d      S3,   S2,   TL
    add.d      S4,   S3,   TL
    add.d      TS,   S4,   TL
    beq        I,     ZERO, .L_N2

.L_N41:  /* if(i>0)*/
    fld.d      F0,   S1,   0x00
    fld.d      F1,   S2,   0x00
    fld.d      F2,   S3,   0x00
    fld.d      F3,   S4,   0x00

    fst.d      F0,   TD,   0x00
    fst.d      F1,   TD,   0x08
    fst.d      F2,   TD,   0x10
    fst.d      F3,   TD,   0x18

    addi.d     S1,   S1,   0x08
    addi.d     S2,   S2,   0x08
    addi.d     S3,   S3,   0x08
    addi.d     S4,   S4,   0x08
    addi.d     TD,   TD,   0x20
    addi.d     I,    I,    -1
    blt        ZERO, I,    .L_N41

.L_N2:  /* if(n&2)*/
    andi       I,     N,    0x02
    beq        I,     ZERO, .L_N1

    move       S1,   TS
    add.d      S2,   TS,   TL
    move       I,    M
    add.d      TS,   S2,   TL
    beq        I,    ZERO, .L_N1

.L_N21:  /* if(i>0)*/
    fld.d      F0,   S1,   0x00
    fld.d      F1,   S2,   0x00

    fst.d      F0,   TD,   0x00
    fst.d      F1,   TD,   0x08

    addi.d     S1,   S1,   0x08
    addi.d     S2,   S2,   0x08
    addi.d     TD,   TD,   0x10
    addi.d     I,    I,    -1
    blt        ZERO, I,    .L_N21

.L_N1:  /* if(n&2)*/
    andi       I,    N,    0x01
    beq        I,    ZERO, .L_N0

    move       S1,   TS
    move       I,    M
    beq        I,    ZERO, .L_N0

.L_N11:  /* if(i>0)*/
    fld.d      F0,   S1,   0x00
    fst.d      F0,   TD,   0x00

    addi.d     S1,   S1,   0x08
    addi.d     TD,   TD,   0x08
    addi.d     I,    I,    -1
    blt        ZERO, I,    .L_N11

.L_N0:
    LDARG     $r23,  $sp, 0
    LDARG     $r24,  $sp, 8
    LDARG     $r25,  $sp, 16
    LDARG     $r26,  $sp, 24
    LDARG     $r27,  $sp, 32
    LDARG     $r28,  $sp, 40
    LDARG     $r29,  $sp, 48
    LDARG     $r30,  $sp, 56
    addi.d    $sp,   $sp, 64
    jirl      $r0,   $r1, 0x00

    EPILOGUE