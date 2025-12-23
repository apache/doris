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

/* Param */
#define M      $r4
#define N      $r5
#define A      $r7
#define LDA    $r8
#define X      $r9
#define INCX   $r10
#define Y      $r11
#define INCY   $r6
#define BUFFER $r16
#define ALPHA  $f0

#define YORIG  $r18
#define T0     $r19
#define T1     $r20
#define AO3    $r12
#define AO4    $r13
#define I      $r14
#define J      $r15
#define AO1    $r23
#define AO2    $r24
#define IX     $r25
#define IY     $r26
#define II     $r27
#define T2     $r28
#define T3     $r29
#define T4     $r30

/* LSX vectors */
#define U0     $vr11
#define U1     $vr12
#define U2     $vr2
#define U3     $vr3
#define U4     $vr4
#define U5     $vr5
#define U6     $vr6
#define U7     $vr7
#define U8     $vr8
#define U9     $vr9
#define VALPHA $vr10

#define a1     $f3
#define a2     $f4
#define a3     $f5
#define a4     $f6
#define a5     $f7
#define a6     $f8
#define a7     $f9
#define a8     $f10


    PROLOGUE

    LDARG  INCY,    $sp,  0
    LDARG  BUFFER,  $sp,  8

    addi.d  $sp, $sp, -80

    SDARG  $r23,  $sp,    0
    SDARG  $r24,  $sp,    8
    SDARG  $r25,  $sp,    16
    SDARG  $r26,  $sp,    32
    SDARG  $r27,  $sp,    40
    SDARG  $r28,  $sp,    48
    SDARG  $r29,  $sp,    56
    SDARG  $r30,  $sp,    64
    ST     ALPHA, $sp,    72

    vldrepl.w  VALPHA, $sp, 72

    slli.d     LDA,     LDA,  BASE_SHIFT
    slli.d     INCX,    INCX, BASE_SHIFT
    slli.d     INCY,    INCY, BASE_SHIFT

    bge    $r0,    M,    .L999
    bge    $r0,    N,    .L999

    move   J,      $r0
    move   IY,     $r0

    move   AO1,    A  //a_ptr1

    srai.d     T0,     N,     2  //n/4
    beq        J,      T0,    .L04

.L01: /* j<n/4 */
    vxor.v    U0,     U0,   U0
    add.d    AO2,    AO1,  LDA
    add.d    AO3,    AO2,  LDA
    add.d    AO4,    AO3,  LDA

    move      IX,     $r0
    move      I,      $r0
    move      II,     $r0

    beq       $r0,    M,    .L03

.L02:  /* i<m */
    vldx        U1,    X,    IX
    fldx.s      $f2,   AO1,   II
    fldx.s      $f3,   AO2,   II
    fldx.s      $f4,   AO3,   II
    fldx.s      $f5,   AO4,   II

    vpermi.w    U1,    U1,    0x00
    vextrins.w  U2,    U3,    0x10
    vextrins.w  U2,    U4,    0x20
    vextrins.w  U2,    U5,    0x30

    vfmadd.s    U0,    U2,    U1,    U0  //temp1,2,3,4

    add.d       IX,    IX,    INCX

    addi.d     II,     II,    4
    addi.d     I,      I,     1
    blt        I,      M,    .L02

.L03:
    move        T1,    IY
    add.d       T2,    T1,    INCY
    add.d       T3,    T2,    INCY
    add.d       T4,    T3,    INCY

    fldx.s      a1,    Y,    T1
    fldx.s      a2,    Y,    T2
    fldx.s      a3,    Y,    T3
    fldx.s      a4,    Y,    T4

    vextrins.w  U3,    U4,    0x10
    vextrins.w  U3,    U5,    0x20
    vextrins.w  U3,    U6,    0x30

    vfmadd.s    U3,    VALPHA,    U0,    U3

    vextrins.w  U4,    U3,    0x01
    vextrins.w  U5,    U3,    0x02
    vextrins.w  U6,    U3,    0x03

    fstx.s     a1,     Y,    T1
    fstx.s     a2,     Y,    T2
    fstx.s     a3,     Y,    T3
    fstx.s     a4,     Y,    T4

    slli.d     T1,     LDA,     2
    add.d      AO1,    AO1,     T1
    add.d      IY,     T4,      INCY

    addi.d     J,      J,     1
    blt        J,      T0,    .L01

.L04: /* if(n&2) */
    andi       T0,     N,     2
    beq        $r0,    T0,    .L07

    MTC       a1,     $r0
    MTC       a2,     $r0

    add.d    AO2,    AO1,  LDA

    move      IX,     $r0
    move      I,      $r0
    move      II,     $r0

    beq       $r0,    M,    .L06

.L05:  /* i<m */
    fldx.s      a3,    X,     IX
    fldx.s      a4,    AO1,   II
    fldx.s      a5,    AO2,   II

    fmadd.s     a1,    a4,    a3,    a1  //temp1
    fmadd.s     a2,    a5,    a3,    a2  //temp2

    add.d       IX,    IX,    INCX

    addi.d     II,     II,    4
    addi.d     I,      I,     1
    blt        I,      M,    .L05

.L06:
    move        T1,    IY
    add.d       T2,    T1,    INCY

    fldx.s      a3,    Y,    T1
    fldx.s      a4,    Y,    T2

    fmadd.s    a3,    ALPHA,    a1,    a3
    fmadd.s    a4,    ALPHA,    a2,    a4

    fstx.s     a3,     Y,    T1
    fstx.s     a4,     Y,    T2

    slli.d     T0,     LDA,     1
    add.d      AO1,    AO1,     T0
    add.d      IY,     T2,      INCY

.L07: /* if(n&1) */
    andi       T0,     N,     1
    beq        $r0,    T0,    .L999

    MTC       a1,     $r0

    move      IX,     $r0
    move      I,      $r0
    move      II,      $r0

    beq       $r0,    M,    .L09

.L08:  /* i<m */
    fldx.s      a3,    X,     IX
    fldx.s      a4,    AO1,   II

    fmadd.s     a1,    a4,    a3,    a1  //temp1

    add.d       IX,    IX,    INCX

    addi.d     II,     II,    4
    addi.d     I,      I,     1
    blt        I,      M,    .L08

.L09:
    fldx.s      a3,    Y,    IY

    fmadd.s    a3,    ALPHA,    a1,    a3

    fstx.s     a3,     Y,    IY

    add.d      AO1,    AO1,     LDA
    add.d      IY,     IY,      INCY

.L999:
    LDARG  $r23,  $sp,    0
    LDARG  $r24,  $sp,    8
    LDARG  $r25,  $sp,    16
    LDARG  $r26,  $sp,    32
    LDARG  $r27,  $sp,    40
    LDARG  $r28,  $sp,    48
    LDARG  $r29,  $sp,    56
    LDARG  $r30,  $sp,    64
    LD     ALPHA, $sp,    72
    addi.d  $sp, $sp, 80
    jirl    $r0, $r1, 0x0

    EPILOGUE
