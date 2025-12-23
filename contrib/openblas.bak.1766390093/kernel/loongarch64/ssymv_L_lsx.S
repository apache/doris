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
#include "loongarch64_asm.S"

/* Param */
#define M      $r4
#define N      $r5
#define A      $r6
#define LDA    $r7
#define X      $r8
#define INCX   $r9
#define Y      $r10
#define INCY   $r11
#define BUFFER $r16
#define ALPHA  $f0

#define JY     $r18
#define JX     $r31
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
#define T5     $r17
#define T6     $r16

/* LSX vectors */
#define U0     $vr31
#define U1     $vr1
#define U2     $vr2
#define U3     $vr3
#define U4     $vr4
#define U5     $vr5
#define U6     $vr6
#define U7     $vr7
#define U8     $vr8
#define U9     $vr9
#define U10    $vr10
#define U11    $vr11
#define U12    $vr12
#define U13    $vr13
#define U14    $vr14
#define U15    $vr15
#define U16    $vr16
#define VALPHA $vr17

#define a2     $f2
#define a3     $f3
#define a4     $f4
#define a5     $f5
#define a6     $f6
#define a7     $f7
#define a8     $f8
#define a9     $f9


.macro LOAD_Y_8
    beqz T5, .L01_Y_0
    add.d       T2,    IY,    INCY
    fldx.s      $f4,   Y,     T2
    add.d       T2,    T2,    INCY
    fldx.s      $f5,   Y,     T2
    add.d       T2,    T2,    INCY
    fldx.s      $f6,   Y,     T2
    add.d       T2,    T2,    INCY
    fldx.s      $f7,   Y,     T2

    add.d       T2,    T2,    INCY
    fldx.s      $f8,   Y,     T2
    add.d       T2,    T2,    INCY
    fldx.s      $f9,   Y,     T2
    add.d       T2,    T2,    INCY
    fldx.s      $f10,  Y,     T2
    add.d       T2,    T2,    INCY
    fldx.s      $f11,  Y,     T2

    vextrins.w  U4,    U5,    0x10
    vextrins.w  U4,    U6,    0x20
    vextrins.w  U4,    U7,    0x30
    vextrins.w  U8,    U9,    0x10
    vextrins.w  U8,    U10,   0x20
    vextrins.w  U8,    U11,   0x30
    b .L01_Y_1
.L01_Y_0:
    add.d T3, IY, INCY
    vldx U4, Y,  T3
    alsl.d    T4, INCY, T3, 2
    vldx  U8, Y,  T4
.L01_Y_1:
.endm

.macro STORE_Y_8
    beqz T5, .L01_Y_2
    vextrins.w  U5,    U4,    0x01
    vextrins.w  U6,    U4,    0x02
    vextrins.w  U7,    U4,    0x03
    vextrins.w  U9,    U8,    0x01
    vextrins.w  U10,   U8,    0x02
    vextrins.w  U11,   U8,    0x03

    add.d       T2,    IY,    INCY
    fstx.s      $f4,   Y,     T2
    add.d       T2,    T2,    INCY
    fstx.s      $f5,   Y,     T2
    add.d       T2,    T2,    INCY
    fstx.s      $f6,   Y,     T2
    add.d       T2,    T2,    INCY
    fstx.s      $f7,   Y,     T2

    add.d       T2,    T2,    INCY
    fstx.s      $f8,   Y,     T2
    add.d       T2,    T2,    INCY
    fstx.s      $f9,   Y,     T2
    add.d       T2,    T2,    INCY
    fstx.s      $f10,  Y,     T2
    add.d       T2,    T2,    INCY
    fstx.s      $f11,  Y,     T2
    b .L01_Y_3
.L01_Y_2:
    vstx  U4, Y, T3
    vstx  U8, Y, T4
.L01_Y_3:
.endm

.macro LOAD_X_8
    beqz T6, .L01_X_0
    add.d       T2,    IX,    INCX
    fldx.s      $f4,   X,     T2
    add.d       T2,    T2,    INCX
    fldx.s      $f5,   X,     T2
    add.d       T2,    T2,    INCX
    fldx.s      $f6,   X,     T2
    add.d       T2,    T2,    INCX
    fldx.s      $f7,   X,     T2

    add.d       T2,    T2,    INCX
    fldx.s      $f8,   X,     T2
    add.d       T2,    T2,    INCX
    fldx.s      $f9,   X,     T2
    add.d       T2,    T2,    INCX
    fldx.s      $f10,  X,     T2
    add.d       T2,    T2,    INCX
    fldx.s      $f11,  X,     T2

    vextrins.w  $vr4,    $vr5,    0x10
    vextrins.w  $vr4,    $vr6,    0x20
    vextrins.w  $vr4,    $vr7,    0x30
    vextrins.w  $vr8,    $vr9,    0x10
    vextrins.w  $vr8,    $vr10,   0x20
    vextrins.w  $vr8,    $vr11,   0x30
    b .L01_X_1
.L01_X_0:
    add.d T3, IX, INCX
    vldx  U4, X,  T3
    alsl.d    T4, INCX, T3, 2
    vldx  U8, X,  T4
.L01_X_1:
.endm

    PROLOGUE

    addi.d  $sp, $sp, -88

    SDARG  $r23,  $sp,    0
    SDARG  $r24,  $sp,    8
    SDARG  $r25,  $sp,    16
    SDARG  $r26,  $sp,    32
    SDARG  $r27,  $sp,    40
    SDARG  $r28,  $sp,    48
    SDARG  $r29,  $sp,    56
    SDARG  $r30,  $sp,    64
    SDARG  $r31,  $sp,    72
    ST     ALPHA, $sp,    80

    vldrepl.w  VALPHA, $sp, 80

    addi.d     T5,      INCY, -1
    addi.d     T6,      INCX, -1
    slli.d     LDA,     LDA,  BASE_SHIFT
    slli.d     INCX,    INCX, BASE_SHIFT
    slli.d     INCY,    INCY, BASE_SHIFT

    bge    $r0,    M,    .L999
    bge    $r0,    N,    .L999

    move   J,      $r0
    move   JY,     $r0
    move   JX,     $r0
    move   AO1,    A

    beq        J,      N,    .L999

.L01:
    vxor.v      U2,   U2,  U2
    fldx.s      a6,   X,   JX
    fmul.s      a3,   ALPHA, a6  //temp1
    vpermi.w    U3,   U3,   0x00

    mul.w       T0,   J,   LDA
    slli.d      T1,   J,   BASE_SHIFT
    add.w       T0,   T0,  T1
    fldx.s      a6,   AO1, T0
    fldx.s      a4,   Y,   JY
    fmadd.s     a4,   a3,  a6,  a4
    fstx.s      a4,   Y,   JY

    move      IY,     JY
    move      IX,     JX
    addi.d    II,     J,    1
    move      I,      II
    slli.d    II,     II,   BASE_SHIFT

    sub.d     T0,     M,    J
    addi.d    T0,     T0,   -1
    srai.d    T0,     T0,   3
    add.d     T0,     T0,   J
    addi.d    T0,     T0,   1
    beq       I,      T0,    .L03
    bge       I,      T0,    .L03

    mul.w       T1,     J,    LDA
    add.d       T1,     T1,   II

.L02:  /* /8 */
    vldx        U1,    AO1,   T1
    addi.d      T1,     T1,   16
    vldx        U14,   AO1,   T1
    addi.d      T1,     T1,   16

    LOAD_Y_8

    vfmadd.s    U4,    U3,    U1,    U4
    vfmadd.s    U8,    U3,    U14,   U8

    STORE_Y_8

    alsl.d      IY,    INCY,  IY,   3

    LOAD_X_8

    vfmadd.s    U2,    U1,    U4,    U2
    vfmadd.s    U2,    U14,   U8,    U2

    alsl.d      IX,    INCX,  IX,  3

    addi.d     II,     II,    32
    addi.d     I,      I,     1
    blt        I,      T0,    .L02

    // Acc U2
    GACC vf, s, U4, U2
    vpermi.w    U2, U4, 0

.L03:  /* &4 */
    sub.d     T0,     M,    J
    addi.d    T0,     T0,   -1
    andi      T0,     T0,   4
    beq       $r0,    T0,    .L04

    mul.w     T1,     J,    LDA
    add.d     T1,     T1,   II

    vldx       U1,    AO1,    T1

    add.d       T1,    IY,    INCY
    add.d       T2,    T1,    INCY
    add.d       T3,    T2,    INCY
    add.d       T4,    T3,    INCY

    fldx.s      $f4,   Y,     T1
    fldx.s      $f5,   Y,     T2
    fldx.s      $f6,   Y,     T3
    fldx.s      $f7,   Y,     T4

    vextrins.w  U4,    U5,    0x10
    vextrins.w  U4,    U6,    0x20
    vextrins.w  U4,    U7,    0x30

    vfmadd.s    U4,    U3,    U1,    U4

    vextrins.w  U5,    U4,    0x01
    vextrins.w  U6,    U4,    0x02
    vextrins.w  U7,    U4,    0x03

    fstx.s      $f4,   Y,     T1
    fstx.s      $f5,   Y,     T2
    fstx.s      $f6,   Y,     T3
    fstx.s      $f7,   Y,     T4

    slli.d      T1,    INCY,  2
    add.d       IY,    IY,    T1

    add.d       T1,    IX,    INCX
    add.d       T2,    T1,    INCX
    add.d       T3,    T2,    INCX
    add.d       T4,    T3,    INCX

    fldx.s      $f4,   X,     T1
    fldx.s      $f5,   X,     T2
    fldx.s      $f6,   X,     T3
    fldx.s      $f7,   X,     T4

    vextrins.w  U4,    U5,    0x10
    vextrins.w  U4,    U6,    0x20
    vextrins.w  U4,    U7,    0x30

    vand.v      $vr12, $vr2, $vr2

    vfmadd.s    U2,    U1,    U4,    U2
    vfsub.s     $vr2,    $vr2,    $vr12

    vextrins.w   U4,    U2,    0x01
    vextrins.w   U5,    U2,    0x02
    vextrins.w   U6,    U2,    0x03

    fadd.s     $f2,    $f2,   $f4
    fadd.s     $f2,    $f2,   $f5
    fadd.s     $f2,    $f2,   $f6
    fadd.s     $f2,    $f2,   $f12

    vpermi.w    U2,    U2,    0x00

    slli.d     T2,    INCX,  2
    add.d      IX,    IX,    T2

    addi.d     II,    II,    16

.L04: /* &2 */
    sub.d     T0,     M,    J
    addi.d    T0,     T0,   -1
    andi      T0,     T0,   2
    beq       $r0,    T0,    .L05

    mul.w     T1,     J,    LDA
    add.d     T1,     T1,   II
    addi.d    T2,     T1,   4

    fldx.s    $f4,    AO1,  T1
    fldx.s    $f5,    AO1,  T2

    add.d       T1,    IY,    INCY
    add.d       T2,    T1,    INCY

    fldx.s      $f6,   Y,     T1
    fldx.s      $f7,   Y,     T2

    fmadd.s     $f6,   $f3,   $f4,   $f6
    fmadd.s     $f7,   $f3,   $f5,   $f7

    fstx.s      $f6,   Y,     T1
    fstx.s      $f7,   Y,     T2

    slli.d      T1,    INCY,  1
    add.d       IY,    IY,    T1

    add.d       T1,    IX,    INCX
    add.d       T2,    T1,    INCX

    fldx.s      $f6,   X,     T1
    fldx.s      $f7,   X,     T2

    fmadd.s     $f2,   $f4,   $f6,   $f2
    fmadd.s     $f2,   $f5,   $f7,   $f2

    slli.d     T2,    INCX,  1
    add.d      IX,    IX,    T2

    addi.d     II,    II,    8

.L05:  /* &1 */
    sub.d     T0,     M,    J
    addi.d    T0,     T0,   -1
    andi      T0,     T0,   1
    beq       $r0,    T0,    .L06

    mul.w     T1,     J,    LDA
    add.d     T1,     T1,   II

    fldx.s      $f4,   AO1,   T1
    add.d       IY,    IY,    INCY
    fldx.s      $f6,   Y,     IY
    fmadd.s     $f6,   $f3,   $f4,   $f6
    fstx.s      $f6,   Y,     IY

    add.d       IX,    IX,    INCX
    fldx.s      $f6,   X,     IX
    fmadd.s     $f2,   $f4,   $f6,   $f2

    addi.d     II,    II,    4

.L06:
    fldx.s      $f6,   Y,     JY
    fmadd.s     $f6,   ALPHA, $f2,   $f6
    fstx.s      $f6,   Y,     JY

    add.d       JX,    JX,    INCX
    add.d       JY,    JY,    INCY

    addi.d      J,    J,    1
    blt         J,    N,    .L01

.L999:
    LDARG  $r23,  $sp,    0
    LDARG  $r24,  $sp,    8
    LDARG  $r25,  $sp,    16
    LDARG  $r26,  $sp,    32
    LDARG  $r27,  $sp,    40
    LDARG  $r28,  $sp,    48
    LDARG  $r29,  $sp,    56
    LDARG  $r30,  $sp,    64
    LDARG  $r31,  $sp,    72

    addi.d  $sp, $sp, 88
    jirl    $r0, $r1, 0x0

    EPILOGUE
