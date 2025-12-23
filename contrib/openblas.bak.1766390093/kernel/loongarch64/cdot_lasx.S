/***************************************************************************
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
*****************************************************************************/

#define ASSEMBLER
#include "common.h"

#define N      $r4
#define X      $r5
#define INCX   $r6
#define Y      $r7
#define INCY   $r8
#define I      $r19
#define TEMP   $r10
#define t1     $r11
#define t2     $r12
#define t3     $r13
#define t4     $r14
#define a1     $f12
#define a2     $f13
#define a3     $f14
#define a4     $f15
#define s1     $f16
#define s2     $f17
#define s3     $f18
#define s4     $f19
#define res1   $xr16
#define res2   $xr17
#define res3   $xr18
#define res4   $xr19
#define VX0    $xr12
#define VX1    $xr13
#define VX2    $xr14
#define VX3    $xr15
#define x1     $xr20
#define x2     $xr21
#define x3     $xr22
#define x4     $xr23

    PROLOGUE
    xvxor.v res1, res1, res1
    xvxor.v res2, res2, res2
    xvxor.v res3, res3, res3
    xvxor.v res4, res4, res4
    bge $r0, N, .L999
    li.d  TEMP, 2 * SIZE
    slli.d  INCX, INCX, ZBASE_SHIFT
    slli.d  INCY, INCY, ZBASE_SHIFT
#ifdef DOUBLE
    srai.d I, N, 2
#else
    srai.d I, N, 3
#endif
    bne INCX, TEMP, .L20
    bne INCY, TEMP, .L12 // INCX==1 and INCY!=1
    b .L11  // INCX==1 and INCY==1
.L20:
    bne INCY, TEMP, .L22 // INCX!=1 and INCY!=1
    b .L21 // INCX!=1 and INCY==1

.L11:
    bge $r0, I, .L997
    .align 3

.L111:
    xvld VX0, X, 0 * SIZE
#ifdef DOUBLE
    xvld VX1, X, 4 * SIZE
    xvld VX2, Y, 0 * SIZE
    xvld VX3, Y, 4 * SIZE
    xvpickev.d x1, VX1, VX0
    xvpickod.d x2, VX1, VX0
    xvpickev.d x3, VX3, VX2
    xvpickod.d x4, VX3, VX2
    xvfmadd.d res1, x1, x3, res1
    xvfmadd.d res2, x2, x3, res2
    xvfmadd.d res3, x1, x4, res3
    xvfmadd.d res4, x2, x4, res4
    addi.d X, X, 8 * SIZE
    addi.d Y, Y, 8 * SIZE
#else
    xvld VX1, X, 8 * SIZE
    xvld VX2, Y, 0 * SIZE
    xvld VX3, Y, 8 * SIZE
    xvpickev.w x1, VX1, VX0
    xvpickod.w x2, VX1, VX0
    xvpickev.w x3, VX3, VX2
    xvpickod.w x4, VX3, VX2
    xvfmadd.s res1, x1, x3, res1
    xvfmadd.s res2, x2, x3, res2
    xvfmadd.s res3, x1, x4, res3
    xvfmadd.s res4, x2, x4, res4
    addi.d X, X, 16 * SIZE
    addi.d Y, Y, 16 * SIZE
#endif
    addi.d  I, I, -1
    blt $r0, I, .L111
    b .L996
    .align 3

.L12:
    bge $r0, I, .L997
    .align 3

.L121:
    xvld VX0, X, 0 * SIZE
#ifdef DOUBLE
    ld.d t1, Y, 0 * SIZE
    ld.d t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.d t3, Y, 0 * SIZE
    ld.d t4, Y, 1 * SIZE
    add.d Y, Y, INCY
    xvinsgr2vr.d x3, t1, 0
    xvinsgr2vr.d x4, t2, 0
    xvinsgr2vr.d x3, t3, 2
    xvinsgr2vr.d x4, t4, 2
    xvld VX1, X, 4 * SIZE
    ld.d t1, Y, 0 * SIZE
    ld.d t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.d t3, Y, 0 * SIZE
    ld.d t4, Y, 1 * SIZE
    add.d Y, Y, INCY
    xvinsgr2vr.d x3, t1, 1
    xvinsgr2vr.d x4, t2, 1
    xvinsgr2vr.d x3, t3, 3
    xvinsgr2vr.d x4, t4, 3
    addi.d X, X, 8 * SIZE
    xvpickev.d x1, VX1, VX0
    xvpickod.d x2, VX1, VX0
    xvfmadd.d res1, x1, x3, res1
    xvfmadd.d res2, x2, x3, res2
    xvfmadd.d res3, x1, x4, res3
    xvfmadd.d res4, x2, x4, res4
#else
    ld.w t1, Y, 0 * SIZE
    ld.w t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.w t3, Y, 0 * SIZE
    ld.w t4, Y, 1 * SIZE
    add.d Y, Y, INCY
    xvinsgr2vr.w x3, t1, 0
    xvinsgr2vr.w x4, t2, 0
    xvinsgr2vr.w x3, t3, 1
    xvinsgr2vr.w x4, t4, 1
    ld.w t1, Y, 0 * SIZE
    ld.w t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.w t3, Y, 0 * SIZE
    ld.w t4, Y, 1 * SIZE
    add.d Y, Y, INCY
    xvinsgr2vr.w x3, t1, 4
    xvinsgr2vr.w x4, t2, 4
    xvinsgr2vr.w x3, t3, 5
    xvinsgr2vr.w x4, t4, 5
    xvld VX1, X, 8 * SIZE
    ld.w t1, Y, 0 * SIZE
    ld.w t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.w t3, Y, 0 * SIZE
    ld.w t4, Y, 1 * SIZE
    add.d Y, Y, INCY
    xvinsgr2vr.w x3, t1, 2
    xvinsgr2vr.w x4, t2, 2
    xvinsgr2vr.w x3, t3, 3
    xvinsgr2vr.w x4, t4, 3
    ld.w t1, Y, 0 * SIZE
    ld.w t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.w t3, Y, 0 * SIZE
    ld.w t4, Y, 1 * SIZE
    add.d Y, Y, INCY
    xvinsgr2vr.w x3, t1, 6
    xvinsgr2vr.w x4, t2, 6
    xvinsgr2vr.w x3, t3, 7
    xvinsgr2vr.w x4, t4, 7
    addi.d X, X, 16 * SIZE
    xvpickev.w x1, VX1, VX0
    xvpickod.w x2, VX1, VX0
    xvfmadd.s res1, x1, x3, res1
    xvfmadd.s res2, x2, x3, res2
    xvfmadd.s res3, x1, x4, res3
    xvfmadd.s res4, x2, x4, res4
#endif
    addi.d  I, I, -1
    blt $r0, I, .L121
    b .L996
    .align 3

.L21:
    bge $r0, I, .L997
    .align 3

.L211:
    xvld VX2, Y, 0 * SIZE
#ifdef DOUBLE
    ld.d t1, X, 0 * SIZE
    ld.d t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.d t3, X, 0 * SIZE
    ld.d t4, X, 1 * SIZE
    add.d X, X, INCX
    xvinsgr2vr.d x1, t1, 0
    xvinsgr2vr.d x2, t2, 0
    xvinsgr2vr.d x1, t3, 2
    xvinsgr2vr.d x2, t4, 2
    xvld VX3, Y, 4 * SIZE
    ld.d t1, X, 0 * SIZE
    ld.d t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.d t3, X, 0 * SIZE
    ld.d t4, X, 1 * SIZE
    add.d X, X, INCX
    xvinsgr2vr.d x1, t1, 1
    xvinsgr2vr.d x2, t2, 1
    xvinsgr2vr.d x1, t3, 3
    xvinsgr2vr.d x2, t4, 3
    addi.d Y, Y, 8 * SIZE
    xvpickev.d x3, VX3, VX2
    xvpickod.d x4, VX3, VX2
    xvfmadd.d res1, x1, x3, res1
    xvfmadd.d res2, x2, x3, res2
    xvfmadd.d res3, x1, x4, res3
    xvfmadd.d res4, x2, x4, res4
#else
    ld.w t1, X, 0 * SIZE
    ld.w t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.w t3, X, 0 * SIZE
    ld.w t4, X, 1 * SIZE
    add.d X, X, INCX
    xvinsgr2vr.w x1, t1, 0
    xvinsgr2vr.w x2, t2, 0
    xvinsgr2vr.w x1, t3, 1
    xvinsgr2vr.w x2, t4, 1
    ld.w t1, X, 0 * SIZE
    ld.w t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.w t3, X, 0 * SIZE
    ld.w t4, X, 1 * SIZE
    add.d X, X, INCX
    xvinsgr2vr.w x1, t1, 4
    xvinsgr2vr.w x2, t2, 4
    xvinsgr2vr.w x1, t3, 5
    xvinsgr2vr.w x2, t4, 5
    xvld VX3, Y, 8 * SIZE
    ld.w t1, X, 0 * SIZE
    ld.w t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.w t3, X, 0 * SIZE
    ld.w t4, X, 1 * SIZE
    add.d X, X, INCX
    xvinsgr2vr.w x1, t1, 2
    xvinsgr2vr.w x2, t2, 2
    xvinsgr2vr.w x1, t3, 3
    xvinsgr2vr.w x2, t4, 3
    ld.w t1, X, 0 * SIZE
    ld.w t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.w t3, X, 0 * SIZE
    ld.w t4, X, 1 * SIZE
    add.d X, X, INCX
    xvinsgr2vr.w x1, t1, 6
    xvinsgr2vr.w x2, t2, 6
    xvinsgr2vr.w x1, t3, 7
    xvinsgr2vr.w x2, t4, 7
    addi.d Y, Y, 16 * SIZE
    xvpickev.w x3, VX3, VX2
    xvpickod.w x4, VX3, VX2
    xvfmadd.s res1, x1, x3, res1
    xvfmadd.s res2, x2, x3, res2
    xvfmadd.s res3, x1, x4, res3
    xvfmadd.s res4, x2, x4, res4
#endif
    addi.d  I, I, -1
    blt $r0, I, .L211
    b .L996
    .align 3

.L22:
    bge $r0, I, .L997
    .align 3

.L222:
#ifdef DOUBLE
    ld.d t1, X, 0 * SIZE
    ld.d t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.d t3, X, 0 * SIZE
    ld.d t4, X, 1 * SIZE
    add.d X, X, INCX
    xvinsgr2vr.d x1, t1, 0
    xvinsgr2vr.d x2, t2, 0
    xvinsgr2vr.d x1, t3, 1
    xvinsgr2vr.d x2, t4, 1
    ld.d t1, Y, 0 * SIZE
    ld.d t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.d t3, Y, 0 * SIZE
    ld.d t4, Y, 1 * SIZE
    add.d Y, Y, INCY
    xvinsgr2vr.d x3, t1, 0
    xvinsgr2vr.d x4, t2, 0
    xvinsgr2vr.d x3, t3, 1
    xvinsgr2vr.d x4, t4, 1
    ld.d t1, X, 0 * SIZE
    ld.d t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.d t3, X, 0 * SIZE
    ld.d t4, X, 1 * SIZE
    add.d X, X, INCX
    xvinsgr2vr.d x1, t1, 2
    xvinsgr2vr.d x2, t2, 2
    xvinsgr2vr.d x1, t3, 3
    xvinsgr2vr.d x2, t4, 3
    ld.d t1, Y, 0 * SIZE
    ld.d t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.d t3, Y, 0 * SIZE
    ld.d t4, Y, 1 * SIZE
    add.d Y, Y, INCY
    xvinsgr2vr.d x3, t1, 2
    xvinsgr2vr.d x4, t2, 2
    xvinsgr2vr.d x3, t3, 3
    xvinsgr2vr.d x4, t4, 3
    xvfmadd.d res1, x1, x3, res1
    xvfmadd.d res2, x2, x3, res2
    xvfmadd.d res3, x1, x4, res3
    xvfmadd.d res4, x2, x4, res4
#else
    ld.w t1, X, 0 * SIZE
    ld.w t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.w t3, X, 0 * SIZE
    ld.w t4, X, 1 * SIZE
    add.d X, X, INCX
    xvinsgr2vr.w x1, t1, 0
    xvinsgr2vr.w x2, t2, 0
    xvinsgr2vr.w x1, t3, 1
    xvinsgr2vr.w x2, t4, 1
    ld.w t1, Y, 0 * SIZE
    ld.w t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.w t3, Y, 0 * SIZE
    ld.w t4, Y, 1 * SIZE
    add.d Y, Y, INCY
    xvinsgr2vr.w x3, t1, 0
    xvinsgr2vr.w x4, t2, 0
    xvinsgr2vr.w x3, t3, 1
    xvinsgr2vr.w x4, t4, 1
    ld.w t1, X, 0 * SIZE
    ld.w t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.w t3, X, 0 * SIZE
    ld.w t4, X, 1 * SIZE
    add.d X, X, INCX
    xvinsgr2vr.w x1, t1, 2
    xvinsgr2vr.w x2, t2, 2
    xvinsgr2vr.w x1, t3, 3
    xvinsgr2vr.w x2, t4, 3
    ld.w t1, Y, 0 * SIZE
    ld.w t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.w t3, Y, 0 * SIZE
    ld.w t4, Y, 1 * SIZE
    add.d Y, Y, INCY
    xvinsgr2vr.w x3, t1, 2
    xvinsgr2vr.w x4, t2, 2
    xvinsgr2vr.w x3, t3, 3
    xvinsgr2vr.w x4, t4, 3
    ld.w t1, X, 0 * SIZE
    ld.w t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.w t3, X, 0 * SIZE
    ld.w t4, X, 1 * SIZE
    add.d X, X, INCX
    xvinsgr2vr.w x1, t1, 4
    xvinsgr2vr.w x2, t2, 4
    xvinsgr2vr.w x1, t3, 5
    xvinsgr2vr.w x2, t4, 5
    ld.w t1, Y, 0 * SIZE
    ld.w t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.w t3, Y, 0 * SIZE
    ld.w t4, Y, 1 * SIZE
    add.d Y, Y, INCY
    xvinsgr2vr.w x3, t1, 4
    xvinsgr2vr.w x4, t2, 4
    xvinsgr2vr.w x3, t3, 5
    xvinsgr2vr.w x4, t4, 5
    ld.w t1, X, 0 * SIZE
    ld.w t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.w t3, X, 0 * SIZE
    ld.w t4, X, 1 * SIZE
    add.d X, X, INCX
    xvinsgr2vr.w x1, t1, 6
    xvinsgr2vr.w x2, t2, 6
    xvinsgr2vr.w x1, t3, 7
    xvinsgr2vr.w x2, t4, 7
    ld.w t1, Y, 0 * SIZE
    ld.w t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.w t3, Y, 0 * SIZE
    ld.w t4, Y, 1 * SIZE
    add.d Y, Y, INCY
    xvinsgr2vr.w x3, t1, 6
    xvinsgr2vr.w x4, t2, 6
    xvinsgr2vr.w x3, t3, 7
    xvinsgr2vr.w x4, t4, 7
    xvfmadd.s res1, x1, x3, res1
    xvfmadd.s res2, x2, x3, res2
    xvfmadd.s res3, x1, x4, res3
    xvfmadd.s res4, x2, x4, res4
#endif
    addi.d  I, I, -1
    blt $r0, I, .L222
    .align 3

.L996:
#ifdef DOUBLE
    xvpickve.d VX1, res1, 1
    xvpickve.d VX2, res1, 2
    xvpickve.d VX3, res1, 3
    xvfadd.d res1, VX1, res1
    xvfadd.d res1, VX2, res1
    xvfadd.d res1, VX3, res1
    xvpickve.d VX1, res2, 1
    xvpickve.d VX2, res2, 2
    xvpickve.d VX3, res2, 3
    xvfadd.d res2, VX1, res2
    xvfadd.d res2, VX2, res2
    xvfadd.d res2, VX3, res2
    xvpickve.d VX1, res3, 1
    xvpickve.d VX2, res3, 2
    xvpickve.d VX3, res3, 3
    xvfadd.d res3, VX1, res3
    xvfadd.d res3, VX2, res3
    xvfadd.d res3, VX3, res3
    xvpickve.d VX1, res4, 1
    xvpickve.d VX2, res4, 2
    xvpickve.d VX3, res4, 3
    xvfadd.d res4, VX1, res4
    xvfadd.d res4, VX2, res4
    xvfadd.d res4, VX3, res4
#else
    xvpickve.w VX0, res1, 1
    xvpickve.w VX1, res1, 2
    xvpickve.w VX2, res1, 3
    xvpickve.w VX3, res1, 4
    xvpickve.w x1, res1, 5
    xvpickve.w x2, res1, 6
    xvpickve.w x3, res1, 7
    xvfadd.s res1, VX0, res1
    xvfadd.s res1, VX1, res1
    xvfadd.s res1, VX2, res1
    xvfadd.s res1, VX3, res1
    xvfadd.s res1, x1, res1
    xvfadd.s res1, x2, res1
    xvfadd.s res1, x3, res1
    xvpickve.w VX0, res2, 1
    xvpickve.w VX1, res2, 2
    xvpickve.w VX2, res2, 3
    xvpickve.w VX3, res2, 4
    xvpickve.w x1, res2, 5
    xvpickve.w x2, res2, 6
    xvpickve.w x3, res2, 7
    xvfadd.s res2, VX0, res2
    xvfadd.s res2, VX1, res2
    xvfadd.s res2, VX2, res2
    xvfadd.s res2, VX3, res2
    xvfadd.s res2, x1, res2
    xvfadd.s res2, x2, res2
    xvfadd.s res2, x3, res2
    xvpickve.w VX0, res3, 1
    xvpickve.w VX1, res3, 2
    xvpickve.w VX2, res3, 3
    xvpickve.w VX3, res3, 4
    xvpickve.w x1, res3, 5
    xvpickve.w x2, res3, 6
    xvpickve.w x3, res3, 7
    xvfadd.s res3, VX0, res3
    xvfadd.s res3, VX1, res3
    xvfadd.s res3, VX2, res3
    xvfadd.s res3, VX3, res3
    xvfadd.s res3, x1, res3
    xvfadd.s res3, x2, res3
    xvfadd.s res3, x3, res3
    xvpickve.w VX0, res4, 1
    xvpickve.w VX1, res4, 2
    xvpickve.w VX2, res4, 3
    xvpickve.w VX3, res4, 4
    xvpickve.w x1, res4, 5
    xvpickve.w x2, res4, 6
    xvpickve.w x3, res4, 7
    xvfadd.s res4, VX0, res4
    xvfadd.s res4, VX1, res4
    xvfadd.s res4, VX2, res4
    xvfadd.s res4, VX3, res4
    xvfadd.s res4, x1, res4
    xvfadd.s res4, x2, res4
    xvfadd.s res4, x3, res4
#endif
    .align 3

.L997:
#ifdef DOUBLE
    andi I, N, 3
#else
    andi I, N, 7
#endif
    bge $r0, I, .L999
    .align 3

.L998:
    LD   a1, X, 0 * SIZE
    LD   a2, X, 1 * SIZE
    LD   a3, Y, 0 * SIZE
    LD   a4, Y, 1 * SIZE
    MADD s1, a1, a3, s1
    MADD s2, a2, a3, s2
    MADD s3, a1, a4, s3
    MADD s4, a2, a4, s4
    addi.d I, I, -1
    add.d  X, X, INCX
    add.d  Y, Y, INCY
    blt $r0, I, .L998
    .align 3

.L999:
#ifndef CONJ
    SUB $f0, s1, s4
    ADD $f1, s3, s2
#else
    ADD $f0, s1, s4
    SUB $f1, s3, s2
#endif
    jirl    $r0, $r1, 0x0
    .align 3

    EPILOGUE
