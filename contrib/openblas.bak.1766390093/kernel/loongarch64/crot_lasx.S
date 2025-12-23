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
#define C      $f0
#define S      $f1

#define I      $r12
#define TEMP   $r13
#define t1     $r14
#define t2     $r16
#define t3     $r15
#define t4     $r17
#define XX     $r18
#define YY     $r19
#define a1     $f12
#define a2     $f13
#define a3     $f14
#define a4     $f15
#define s1     $f16
#define s2     $f17
#define s3     $f18
#define s4     $f19
#define VX0    $xr8
#define VX1    $xr20
#define VX2    $xr21
#define VX3    $xr22
#define VT0    $xr10
#define VT1    $xr18
#define VXC    $xr23
#define VXS    $xr9
#define VXZ    $xr11
#define x1     $xr12
#define x2     $xr13
#define x3     $xr14
#define x4     $xr15

    PROLOGUE

    bge $r0, N, .L999
    li.d TEMP, 1
    movgr2fr.d a1, $r0
    FFINT  a1, a1
    slli.d  TEMP, TEMP, ZBASE_SHIFT
    slli.d  INCX, INCX, ZBASE_SHIFT
    slli.d  INCY, INCY, ZBASE_SHIFT
    MTG  t1, C
    MTG  t2, S
    MTG  t3, a1
#ifdef DOUBLE
    xvreplgr2vr.d VXC, t1
    xvreplgr2vr.d VXS, t2
    xvreplgr2vr.d VXZ, t3
    srai.d I, N, 2
#else
    xvreplgr2vr.w VXC, t1
    xvreplgr2vr.w VXS, t2
    xvreplgr2vr.w VXZ, t3
    srai.d I, N, 3
#endif
    beq INCX, $r0, .L996
    beq INCY, $r0, .L996
    bne INCX, TEMP, .L22 // INCX!=1 or INCY!=1
    bne INCY, TEMP, .L22

.L11:
    bge $r0, I, .L997
    CMPEQ $fcc0, C, a1
    bcnez $fcc0, .L110
    CMPEQ $fcc0, S, a1
    bcnez $fcc0, .L112 // C!=0 S==0
    b .L111 // C!=0 S!=0
    .align 3

.L110:
    CMPEQ $fcc0, S, a1
    bcnez $fcc0, .L114 // C==0 S==0
    b .L113 // C==0 S!=0
    .align 3

.L111: // C!=0 S!=0
    xvld VX0, X, 0 * SIZE
    xvld VX2, Y, 0 * SIZE
#ifdef DOUBLE
    xvld VX1, X, 4 * SIZE
    xvld VX3, Y, 4 * SIZE
    xvpickev.d x1, VX1, VX0
    xvpickod.d x2, VX1, VX0
    xvpickev.d x3, VX3, VX2
    xvpickod.d x4, VX3, VX2
    xvfmul.d VX0, x1, VXC
    xvfmadd.d VX0, x3, VXS, VX0
    xvfmul.d VX1, x1, VXS
    xvfmsub.d VX1, x3, VXC, VX1
    xvfmul.d VX2, x2, VXC
    xvfmadd.d VX2, x4, VXS, VX2
    xvfmul.d VX3, x2, VXS
    xvfmsub.d VX3, x4, VXC, VX3
    xvilvl.d x1, VX2 ,VX0
    xvilvh.d x2, VX2, VX0
    xvilvl.d x3, VX3 ,VX1
    xvilvh.d x4, VX3, VX1
    xvst x1, X, 0 * SIZE
    xvst x3, Y, 0 * SIZE
    xvst x2, X, 4 * SIZE
    xvst x4, Y, 4 * SIZE
    addi.d X, X, 8 * SIZE
    addi.d Y, Y, 8 * SIZE
#else
    xvld VX1, X, 8 * SIZE
    xvld VX3, Y, 8 * SIZE
    xvpickev.w x1, VX1, VX0
    xvpickod.w x2, VX1, VX0
    xvpickev.w x3, VX3, VX2
    xvpickod.w x4, VX3, VX2
    xvfmul.s VX0, x1, VXC
    xvfmadd.s VX0, x3, VXS, VX0
    xvfmul.s VX1, x1, VXS
    xvfmsub.s VX1, x3, VXC, VX1
    xvfmul.s VX2, x2, VXC
    xvfmadd.s VX2, x4, VXS, VX2
    xvfmul.s VX3, x2, VXS
    xvfmsub.s VX3, x4, VXC, VX3
    xvilvl.w x1, VX2 ,VX0
    xvilvh.w x2, VX2, VX0
    xvilvl.w x3, VX3 ,VX1
    xvilvh.w x4, VX3, VX1
    xvst x1, X, 0 * SIZE
    xvst x3, Y, 0 * SIZE
    xvst x2, X, 8 * SIZE
    xvst x4, Y, 8 * SIZE
    addi.d X, X, 16 * SIZE
    addi.d Y, Y, 16 * SIZE
#endif
    addi.d  I, I, -1
    blt $r0, I, .L111
    b .L997
    .align 3

.L112: // C!=0 S==0
    xvld VX0, X, 0 * SIZE
    xvld VX2, Y, 0 * SIZE
#ifdef DOUBLE
    xvld VX1, X, 4 * SIZE
    xvld VX3, Y, 4 * SIZE
    xvpickev.d x1, VX1, VX0
    xvpickod.d x2, VX1, VX0
    xvpickev.d x3, VX3, VX2
    xvpickod.d x4, VX3, VX2
    xvfmul.d VX0, x1, VXC
    xvfmul.d VX1, x3, VXC
    xvfmul.d VX2, x2, VXC
    xvfmul.d VX3, x4, VXC
    xvilvl.d x1, VX2 ,VX0
    xvilvh.d x2, VX2, VX0
    xvilvl.d x3, VX3 ,VX1
    xvilvh.d x4, VX3, VX1
    xvst x1, X, 0 * SIZE
    xvst x3, Y, 0 * SIZE
    xvst x2, X, 4 * SIZE
    xvst x4, Y, 4 * SIZE
    addi.d X, X, 8 * SIZE
    addi.d Y, Y, 8 * SIZE
#else
    xvld VX1, X, 8 * SIZE
    xvld VX3, Y, 8 * SIZE
    xvpickev.w x1, VX1, VX0
    xvpickod.w x2, VX1, VX0
    xvpickev.w x3, VX3, VX2
    xvpickod.w x4, VX3, VX2
    xvfmul.s VX0, x1, VXC
    xvfmul.s VX1, x3, VXC
    xvfmul.s VX2, x2, VXC
    xvfmul.s VX3, x4, VXC
    xvilvl.w x1, VX2 ,VX0
    xvilvh.w x2, VX2, VX0
    xvilvl.w x3, VX3 ,VX1
    xvilvh.w x4, VX3, VX1
    xvst x1, X, 0 * SIZE
    xvst x3, Y, 0 * SIZE
    xvst x2, X, 8 * SIZE
    xvst x4, Y, 8 * SIZE
    addi.d X, X, 16 * SIZE
    addi.d Y, Y, 16 * SIZE
#endif
    addi.d  I, I, -1
    blt $r0, I, .L112
    b .L997
    .align 3

.L113: // C==0 S!=0
    xvld VX0, X, 0 * SIZE
    xvld VX2, Y, 0 * SIZE
#ifdef DOUBLE
    xvld VX1, X, 4 * SIZE
    xvld VX3, Y, 4 * SIZE
    xvpickev.d x1, VX1, VX0
    xvpickod.d x2, VX1, VX0
    xvpickev.d x3, VX3, VX2
    xvpickod.d x4, VX3, VX2
    xvfmul.d VX0, x3, VXS
    xvfmul.d VX1, x1, VXS
    xvfsub.d VX1, VXZ, VX1
    xvfmul.d VX2, x4, VXS
    xvfmul.d VX3, x2, VXS
    xvfsub.d VX3, VXZ, VX3
    xvilvl.d x1, VX2 ,VX0
    xvilvh.d x2, VX2, VX0
    xvilvl.d x3, VX3 ,VX1
    xvilvh.d x4, VX3, VX1
    xvst x1, X, 0 * SIZE
    xvst x3, Y, 0 * SIZE
    xvst x2, X, 4 * SIZE
    xvst x4, Y, 4 * SIZE
    addi.d X, X, 8 * SIZE
    addi.d Y, Y, 8 * SIZE
#else
    xvld VX1, X, 8 * SIZE
    xvld VX3, Y, 8 * SIZE
    xvpickev.w x1, VX1, VX0
    xvpickod.w x2, VX1, VX0
    xvpickev.w x3, VX3, VX2
    xvpickod.w x4, VX3, VX2
    xvfmul.s VX0, x3, VXS
    xvfmul.s VX1, x1, VXS
    xvfsub.s VX1, VXZ, VX1
    xvfmul.s VX2, x4, VXS
    xvfmul.s VX3, x2, VXS
    xvfsub.s VX3, VXZ, VX3
    xvilvl.w x1, VX2 ,VX0
    xvilvh.w x2, VX2, VX0
    xvilvl.w x3, VX3 ,VX1
    xvilvh.w x4, VX3, VX1
    xvst x1, X, 0 * SIZE
    xvst x3, Y, 0 * SIZE
    xvst x2, X, 8 * SIZE
    xvst x4, Y, 8 * SIZE
    addi.d X, X, 16 * SIZE
    addi.d Y, Y, 16 * SIZE
#endif
    addi.d  I, I, -1
    blt $r0, I, .L113
    b .L997
    .align 3

.L114: // C==0 S==0
    xvst VXZ, X, 0 * SIZE
    xvst VXZ, Y, 0 * SIZE
#ifdef DOUBLE
    xvst VXZ, X, 4 * SIZE
    xvst VXZ, Y, 4 * SIZE
    addi.d X, X, 8 * SIZE
    addi.d Y, Y, 8 * SIZE
#else
    xvst VXZ, X, 8 * SIZE
    xvst VXZ, Y, 8 * SIZE
    addi.d X, X, 16 * SIZE
    addi.d Y, Y, 16 * SIZE
#endif
    addi.d  I, I, -1
    blt $r0, I, .L114
    b .L997
    .align 3

.L22:
    bge $r0, I, .L997
    move YY, Y
    move XX, X
    CMPEQ $fcc0, C, a1
    bcnez $fcc0, .L220
    CMPEQ $fcc0, S, a1
    bcnez $fcc0, .L222 // C!=0 S==0
    b .L221 // C!=0 S!=0
    .align 3

.L220:
    CMPEQ $fcc0, S, a1
    bcnez $fcc0, .L224 // C==0 S==0
    b .L223 // C==0 S!=0
    .align 3

.L221: // C!=0 S!=0
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
    xvinsgr2vr.d x1, t1, 2
    xvinsgr2vr.d x2, t2, 2
    xvinsgr2vr.d x1, t3, 3
    xvinsgr2vr.d x2, t4, 3
    add.d X, X, INCX
    ld.d t1, Y, 0 * SIZE
    ld.d t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.d t3, Y, 0 * SIZE
    ld.d t4, Y, 1 * SIZE
    xvinsgr2vr.d x3, t1, 2
    xvinsgr2vr.d x4, t2, 2
    xvinsgr2vr.d x3, t3, 3
    xvinsgr2vr.d x4, t4, 3
    add.d Y, Y, INCY

    xvfmul.d VX0, x1, VXC
    xvfmadd.d VX0, x3, VXS, VX0
    xvfmul.d VX1, x1, VXS
    xvfmsub.d VX1, x3, VXC, VX1
    xvfmul.d VX2, x2, VXC
    xvfmadd.d VX2, x4, VXS, VX2
    xvfmul.d VX3, x2, VXS
    xvfmsub.d VX3, x4, VXC, VX3
    xvstelm.d VX0, XX, 0, 0
    xvstelm.d VX2, XX, 1 * SIZE, 0
    add.d XX, XX, INCX
    xvstelm.d VX0, XX, 0, 1
    xvstelm.d VX2, XX, 1 * SIZE, 1
    add.d XX, XX, INCX
    xvstelm.d VX0, XX, 0, 2
    xvstelm.d VX2, XX, 1 * SIZE, 2
    add.d XX, XX, INCX
    xvstelm.d VX0, XX, 0, 3
    xvstelm.d VX2, XX, 1 * SIZE, 3
    add.d XX, XX, INCX
    xvstelm.d VX1, YY, 0, 0
    xvstelm.d VX3, YY, 1 * SIZE, 0
    add.d YY, YY, INCY
    xvstelm.d VX1, YY, 0, 1
    xvstelm.d VX3, YY, 1 * SIZE, 1
    add.d YY, YY, INCY
    xvstelm.d VX1, YY, 0, 2
    xvstelm.d VX3, YY, 1 * SIZE, 2
    add.d YY, YY, INCY
    xvstelm.d VX1, YY, 0, 3
    xvstelm.d VX3, YY, 1 * SIZE, 3
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
    xvinsgr2vr.w x1, t1, 2
    xvinsgr2vr.w x2, t2, 2
    xvinsgr2vr.w x1, t3, 3
    xvinsgr2vr.w x2, t4, 3
    add.d X, X, INCX
    ld.w t1, Y, 0 * SIZE
    ld.w t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.w t3, Y, 0 * SIZE
    ld.w t4, Y, 1 * SIZE
    xvinsgr2vr.w x3, t1, 2
    xvinsgr2vr.w x4, t2, 2
    xvinsgr2vr.w x3, t3, 3
    xvinsgr2vr.w x4, t4, 3
    add.d Y, Y, INCY

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
    xvinsgr2vr.w x1, t1, 6
    xvinsgr2vr.w x2, t2, 6
    xvinsgr2vr.w x1, t3, 7
    xvinsgr2vr.w x2, t4, 7
    add.d X, X, INCX
    ld.w t1, Y, 0 * SIZE
    ld.w t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.w t3, Y, 0 * SIZE
    ld.w t4, Y, 1 * SIZE
    xvinsgr2vr.w x3, t1, 6
    xvinsgr2vr.w x4, t2, 6
    xvinsgr2vr.w x3, t3, 7
    xvinsgr2vr.w x4, t4, 7
    add.d Y, Y, INCY

    xvfmul.s VX0, x1, VXC
    xvfmadd.s VX0, x3, VXS, VX0
    xvfmul.s VX1, x1, VXS
    xvfmsub.s VX1, x3, VXC, VX1
    xvfmul.s VX2, x2, VXC
    xvfmadd.s VX2, x4, VXS, VX2
    xvfmul.s VX3, x2, VXS
    xvfmsub.s VX3, x4, VXC, VX3
    xvstelm.w VX0, XX, 0, 0
    xvstelm.w VX2, XX, 1 * SIZE, 0
    add.d XX, XX, INCX
    xvstelm.w VX0, XX, 0, 1
    xvstelm.w VX2, XX, 1 * SIZE, 1
    add.d XX, XX, INCX
    xvstelm.w VX0, XX, 0, 2
    xvstelm.w VX2, XX, 1 * SIZE, 2
    add.d XX, XX, INCX
    xvstelm.w VX0, XX, 0, 3
    xvstelm.w VX2, XX, 1 * SIZE, 3
    add.d XX, XX, INCX
    xvstelm.w VX1, YY, 0, 0
    xvstelm.w VX3, YY, 1 * SIZE, 0
    add.d YY, YY, INCY
    xvstelm.w VX1, YY, 0, 1
    xvstelm.w VX3, YY, 1 * SIZE, 1
    add.d YY, YY, INCY
    xvstelm.w VX1, YY, 0, 2
    xvstelm.w VX3, YY, 1 * SIZE, 2
    add.d YY, YY, INCY
    xvstelm.w VX1, YY, 0, 3
    xvstelm.w VX3, YY, 1 * SIZE, 3
    add.d YY, YY, INCY
    xvstelm.w VX0, XX, 0, 4
    xvstelm.w VX2, XX, 1 * SIZE, 4
    add.d XX, XX, INCX
    xvstelm.w VX0, XX, 0, 5
    xvstelm.w VX2, XX, 1 * SIZE, 5
    add.d XX, XX, INCX
    xvstelm.w VX0, XX, 0, 6
    xvstelm.w VX2, XX, 1 * SIZE, 6
    add.d XX, XX, INCX
    xvstelm.w VX0, XX, 0, 7
    xvstelm.w VX2, XX, 1 * SIZE, 7
    add.d XX, XX, INCX
    xvstelm.w VX1, YY, 0, 4
    xvstelm.w VX3, YY, 1 * SIZE, 4
    add.d YY, YY, INCY
    xvstelm.w VX1, YY, 0, 5
    xvstelm.w VX3, YY, 1 * SIZE, 5
    add.d YY, YY, INCY
    xvstelm.w VX1, YY, 0, 6
    xvstelm.w VX3, YY, 1 * SIZE, 6
    add.d YY, YY, INCY
    xvstelm.w VX1, YY, 0, 7
    xvstelm.w VX3, YY, 1 * SIZE, 7
#endif
    add.d YY, YY, INCY
    addi.d  I, I, -1
    blt $r0, I, .L221
    b .L997
    .align 3

.L222: // C!=0 S==0
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
    xvinsgr2vr.d x1, t1, 2
    xvinsgr2vr.d x2, t2, 2
    xvinsgr2vr.d x1, t3, 3
    xvinsgr2vr.d x2, t4, 3
    add.d X, X, INCX
    ld.d t1, Y, 0 * SIZE
    ld.d t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.d t3, Y, 0 * SIZE
    ld.d t4, Y, 1 * SIZE
    xvinsgr2vr.d x3, t1, 2
    xvinsgr2vr.d x4, t2, 2
    xvinsgr2vr.d x3, t3, 3
    xvinsgr2vr.d x4, t4, 3
    add.d Y, Y, INCY
    xvfmul.d VX0, x1, VXC
    xvfmul.d VX1, x3, VXC
    xvfmul.d VX2, x2, VXC
    xvfmul.d VX3, x4, VXC
    xvstelm.d VX0, XX, 0, 0
    xvstelm.d VX2, XX, 1 * SIZE, 0
    add.d XX, XX, INCX
    xvstelm.d VX0, XX, 0, 1
    xvstelm.d VX2, XX, 1 * SIZE, 1
    add.d XX, XX, INCX
    xvstelm.d VX0, XX, 0, 2
    xvstelm.d VX2, XX, 1 * SIZE, 2
    add.d XX, XX, INCX
    xvstelm.d VX0, XX, 0, 3
    xvstelm.d VX2, XX, 1 * SIZE, 3
    add.d XX, XX, INCX
    xvstelm.d VX1, YY, 0, 0
    xvstelm.d VX3, YY, 1 * SIZE, 0
    add.d YY, YY, INCY
    xvstelm.d VX1, YY, 0, 1
    xvstelm.d VX3, YY, 1 * SIZE, 1
    add.d YY, YY, INCY
    xvstelm.d VX1, YY, 0, 2
    xvstelm.d VX3, YY, 1 * SIZE, 2
    add.d YY, YY, INCY
    xvstelm.d VX1, YY, 0, 3
    xvstelm.d VX3, YY, 1 * SIZE, 3
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
    xvinsgr2vr.w x1, t1, 2
    xvinsgr2vr.w x2, t2, 2
    xvinsgr2vr.w x1, t3, 3
    xvinsgr2vr.w x2, t4, 3
    add.d X, X, INCX
    ld.w t1, Y, 0 * SIZE
    ld.w t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.w t3, Y, 0 * SIZE
    ld.w t4, Y, 1 * SIZE
    xvinsgr2vr.w x3, t1, 2
    xvinsgr2vr.w x4, t2, 2
    xvinsgr2vr.w x3, t3, 3
    xvinsgr2vr.w x4, t4, 3
    add.d Y, Y, INCY

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
    xvinsgr2vr.w x1, t1, 6
    xvinsgr2vr.w x2, t2, 6
    xvinsgr2vr.w x1, t3, 7
    xvinsgr2vr.w x2, t4, 7
    add.d X, X, INCX
    ld.w t1, Y, 0 * SIZE
    ld.w t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.w t3, Y, 0 * SIZE
    ld.w t4, Y, 1 * SIZE
    xvinsgr2vr.w x3, t1, 6
    xvinsgr2vr.w x4, t2, 6
    xvinsgr2vr.w x3, t3, 7
    xvinsgr2vr.w x4, t4, 7
    add.d Y, Y, INCY
    xvfmul.s VX0, x1, VXC
    xvfmul.s VX1, x3, VXC
    xvfmul.s VX2, x2, VXC
    xvfmul.s VX3, x4, VXC
    xvstelm.w VX0, XX, 0, 0
    xvstelm.w VX2, XX, 1 * SIZE, 0
    add.d XX, XX, INCX
    xvstelm.w VX0, XX, 0, 1
    xvstelm.w VX2, XX, 1 * SIZE, 1
    add.d XX, XX, INCX
    xvstelm.w VX0, XX, 0, 2
    xvstelm.w VX2, XX, 1 * SIZE, 2
    add.d XX, XX, INCX
    xvstelm.w VX0, XX, 0, 3
    xvstelm.w VX2, XX, 1 * SIZE, 3
    add.d XX, XX, INCX
    xvstelm.w VX1, YY, 0, 0
    xvstelm.w VX3, YY, 1 * SIZE, 0
    add.d YY, YY, INCY
    xvstelm.w VX1, YY, 0, 1
    xvstelm.w VX3, YY, 1 * SIZE, 1
    add.d YY, YY, INCY
    xvstelm.w VX1, YY, 0, 2
    xvstelm.w VX3, YY, 1 * SIZE, 2
    add.d YY, YY, INCY
    xvstelm.w VX1, YY, 0, 3
    xvstelm.w VX3, YY, 1 * SIZE, 3
    add.d YY, YY, INCY
    xvstelm.w VX0, XX, 0, 4
    xvstelm.w VX2, XX, 1 * SIZE, 4
    add.d XX, XX, INCX
    xvstelm.w VX0, XX, 0, 5
    xvstelm.w VX2, XX, 1 * SIZE, 5
    add.d XX, XX, INCX
    xvstelm.w VX0, XX, 0, 6
    xvstelm.w VX2, XX, 1 * SIZE, 6
    add.d XX, XX, INCX
    xvstelm.w VX0, XX, 0, 7
    xvstelm.w VX2, XX, 1 * SIZE, 7
    add.d XX, XX, INCX
    xvstelm.w VX1, YY, 0, 4
    xvstelm.w VX3, YY, 1 * SIZE, 4
    add.d YY, YY, INCY
    xvstelm.w VX1, YY, 0, 5
    xvstelm.w VX3, YY, 1 * SIZE, 5
    add.d YY, YY, INCY
    xvstelm.w VX1, YY, 0, 6
    xvstelm.w VX3, YY, 1 * SIZE, 6
    add.d YY, YY, INCY
    xvstelm.w VX1, YY, 0, 7
    xvstelm.w VX3, YY, 1 * SIZE, 7
#endif
    add.d YY, YY, INCY
    addi.d  I, I, -1
    blt $r0, I, .L222
    b .L997
    .align 3

.L223: // C==0 S!=0
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
    xvinsgr2vr.d x1, t1, 2
    xvinsgr2vr.d x2, t2, 2
    xvinsgr2vr.d x1, t3, 3
    xvinsgr2vr.d x2, t4, 3
    add.d X, X, INCX
    ld.d t1, Y, 0 * SIZE
    ld.d t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.d t3, Y, 0 * SIZE
    ld.d t4, Y, 1 * SIZE
    xvinsgr2vr.d x3, t1, 2
    xvinsgr2vr.d x4, t2, 2
    xvinsgr2vr.d x3, t3, 3
    xvinsgr2vr.d x4, t4, 3
    add.d Y, Y, INCY
    xvfmul.d VX0, x3, VXS
    xvfmul.d VX1, x1, VXS
    xvfsub.d VX1, VXZ, VX1
    xvfmul.d VX2, x4, VXS
    xvfmul.d VX3, x2, VXS
    xvfsub.d VX3, VXZ, VX3
    xvstelm.d VX0, XX, 0, 0
    xvstelm.d VX2, XX, 1 * SIZE, 0
    add.d XX, XX, INCX
    xvstelm.d VX0, XX, 0, 1
    xvstelm.d VX2, XX, 1 * SIZE, 1
    add.d XX, XX, INCX
    xvstelm.d VX0, XX, 0, 2
    xvstelm.d VX2, XX, 1 * SIZE, 2
    add.d XX, XX, INCX
    xvstelm.d VX0, XX, 0, 3
    xvstelm.d VX2, XX, 1 * SIZE, 3
    add.d XX, XX, INCX
    xvstelm.d VX1, YY, 0, 0
    xvstelm.d VX3, YY, 1 * SIZE, 0
    add.d YY, YY, INCY
    xvstelm.d VX1, YY, 0, 1
    xvstelm.d VX3, YY, 1 * SIZE, 1
    add.d YY, YY, INCY
    xvstelm.d VX1, YY, 0, 2
    xvstelm.d VX3, YY, 1 * SIZE, 2
    add.d YY, YY, INCY
    xvstelm.d VX1, YY, 0, 3
    xvstelm.d VX3, YY, 1 * SIZE, 3
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
    xvinsgr2vr.w x1, t1, 2
    xvinsgr2vr.w x2, t2, 2
    xvinsgr2vr.w x1, t3, 3
    xvinsgr2vr.w x2, t4, 3
    add.d X, X, INCX
    ld.w t1, Y, 0 * SIZE
    ld.w t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.w t3, Y, 0 * SIZE
    ld.w t4, Y, 1 * SIZE
    xvinsgr2vr.w x3, t1, 2
    xvinsgr2vr.w x4, t2, 2
    xvinsgr2vr.w x3, t3, 3
    xvinsgr2vr.w x4, t4, 3
    add.d Y, Y, INCY

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
    xvinsgr2vr.w x1, t1, 6
    xvinsgr2vr.w x2, t2, 6
    xvinsgr2vr.w x1, t3, 7
    xvinsgr2vr.w x2, t4, 7
    add.d X, X, INCX
    ld.w t1, Y, 0 * SIZE
    ld.w t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.w t3, Y, 0 * SIZE
    ld.w t4, Y, 1 * SIZE
    xvinsgr2vr.w x3, t1, 6
    xvinsgr2vr.w x4, t2, 6
    xvinsgr2vr.w x3, t3, 7
    xvinsgr2vr.w x4, t4, 7
    add.d Y, Y, INCY
    xvfmul.s VX0, x3, VXS
    xvfmul.s VX1, x1, VXS
    xvfsub.s VX1, VXZ, VX1
    xvfmul.s VX2, x4, VXS
    xvfmul.s VX3, x2, VXS
    xvfsub.s VX3, VXZ, VX3
    xvstelm.w VX0, XX, 0, 0
    xvstelm.w VX2, XX, 1 * SIZE, 0
    add.d XX, XX, INCX
    xvstelm.w VX0, XX, 0, 1
    xvstelm.w VX2, XX, 1 * SIZE, 1
    add.d XX, XX, INCX
    xvstelm.w VX0, XX, 0, 2
    xvstelm.w VX2, XX, 1 * SIZE, 2
    add.d XX, XX, INCX
    xvstelm.w VX0, XX, 0, 3
    xvstelm.w VX2, XX, 1 * SIZE, 3
    add.d XX, XX, INCX
    xvstelm.w VX1, YY, 0, 0
    xvstelm.w VX3, YY, 1 * SIZE, 0
    add.d YY, YY, INCY
    xvstelm.w VX1, YY, 0, 1
    xvstelm.w VX3, YY, 1 * SIZE, 1
    add.d YY, YY, INCY
    xvstelm.w VX1, YY, 0, 2
    xvstelm.w VX3, YY, 1 * SIZE, 2
    add.d YY, YY, INCY
    xvstelm.w VX1, YY, 0, 3
    xvstelm.w VX3, YY, 1 * SIZE, 3
    add.d YY, YY, INCY
    xvstelm.w VX0, XX, 0, 4
    xvstelm.w VX2, XX, 1 * SIZE, 4
    add.d XX, XX, INCX
    xvstelm.w VX0, XX, 0, 5
    xvstelm.w VX2, XX, 1 * SIZE, 5
    add.d XX, XX, INCX
    xvstelm.w VX0, XX, 0, 6
    xvstelm.w VX2, XX, 1 * SIZE, 6
    add.d XX, XX, INCX
    xvstelm.w VX0, XX, 0, 7
    xvstelm.w VX2, XX, 1 * SIZE, 7
    add.d XX, XX, INCX
    xvstelm.w VX1, YY, 0, 4
    xvstelm.w VX3, YY, 1 * SIZE, 4
    add.d YY, YY, INCY
    xvstelm.w VX1, YY, 0, 5
    xvstelm.w VX3, YY, 1 * SIZE, 5
    add.d YY, YY, INCY
    xvstelm.w VX1, YY, 0, 6
    xvstelm.w VX3, YY, 1 * SIZE, 6
    add.d YY, YY, INCY
    xvstelm.w VX1, YY, 0, 7
    xvstelm.w VX3, YY, 1 * SIZE, 7
#endif
    add.d YY, YY, INCY
    addi.d  I, I, -1
    blt $r0, I, .L223
    b .L997
    .align 3

.L224: // C==0 S==0
#ifdef DOUBLE
    xvstelm.d VXZ, XX, 0, 0
    xvstelm.d VXZ, XX, 1 * SIZE, 0
    add.d XX, XX, INCX
    xvstelm.d VXZ, XX, 0, 0
    xvstelm.d VXZ, XX, 1 * SIZE, 0
    add.d XX, XX, INCX
    xvstelm.d VXZ, XX, 0, 0
    xvstelm.d VXZ, XX, 1 * SIZE, 0
    add.d XX, XX, INCX
    xvstelm.d VXZ, XX, 0, 0
    xvstelm.d VXZ, XX, 1 * SIZE, 0
    add.d XX, XX, INCX
    xvstelm.d VXZ, YY, 0, 0
    xvstelm.d VXZ, YY, 1 * SIZE, 0
    add.d YY, YY, INCY
    xvstelm.d VXZ, YY, 0, 0
    xvstelm.d VXZ, YY, 1 * SIZE, 0
    add.d YY, YY, INCY
    xvstelm.d VXZ, YY, 0, 0
    xvstelm.d VXZ, YY, 1 * SIZE, 0
    add.d YY, YY, INCY
    xvstelm.d VXZ, YY, 0, 0
    xvstelm.d VXZ, YY, 1 * SIZE, 0
#else
    xvstelm.w VXZ, XX, 0, 0
    xvstelm.w VXZ, XX, 1 * SIZE, 0
    add.d XX, XX, INCX
    xvstelm.w VXZ, XX, 0, 0
    xvstelm.w VXZ, XX, 1 * SIZE, 0
    add.d XX, XX, INCX
    xvstelm.w VXZ, XX, 0, 0
    xvstelm.w VXZ, XX, 1 * SIZE, 0
    add.d XX, XX, INCX
    xvstelm.w VXZ, XX, 0, 0
    xvstelm.w VXZ, XX, 1 * SIZE, 0
    add.d XX, XX, INCX
    xvstelm.w VXZ, YY, 0, 0
    xvstelm.w VXZ, YY, 1 * SIZE, 0
    add.d YY, YY, INCY
    xvstelm.w VXZ, YY, 0, 0
    xvstelm.w VXZ, YY, 1 * SIZE, 0
    add.d YY, YY, INCY
    xvstelm.w VXZ, YY, 0, 0
    xvstelm.w VXZ, YY, 1 * SIZE, 0
    add.d YY, YY, INCY
    xvstelm.w VXZ, YY, 0, 0
    xvstelm.w VXZ, YY, 1 * SIZE, 0
    add.d YY, YY, INCY

    xvstelm.w VXZ, XX, 0, 0
    xvstelm.w VXZ, XX, 1 * SIZE, 0
    add.d XX, XX, INCX
    xvstelm.w VXZ, XX, 0, 0
    xvstelm.w VXZ, XX, 1 * SIZE, 0
    add.d XX, XX, INCX
    xvstelm.w VXZ, XX, 0, 0
    xvstelm.w VXZ, XX, 1 * SIZE, 0
    add.d XX, XX, INCX
    xvstelm.w VXZ, XX, 0, 0
    xvstelm.w VXZ, XX, 1 * SIZE, 0
    add.d XX, XX, INCX
    xvstelm.w VXZ, YY, 0, 0
    xvstelm.w VXZ, YY, 1 * SIZE, 0
    add.d YY, YY, INCY
    xvstelm.w VXZ, YY, 0, 0
    xvstelm.w VXZ, YY, 1 * SIZE, 0
    add.d YY, YY, INCY
    xvstelm.w VXZ, YY, 0, 0
    xvstelm.w VXZ, YY, 1 * SIZE, 0
    add.d YY, YY, INCY
    xvstelm.w VXZ, YY, 0, 0
    xvstelm.w VXZ, YY, 1 * SIZE, 0
#endif
    add.d YY, YY, INCY
    addi.d  I, I, -1
    blt $r0, I, .L224
    move X, XX
    move Y, YY
    b .L997
    .align 3

.L996:
    move I, N
    b .L998
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
    MUL  s1, a1, C
    MADD s1, a3, S, s1
    MUL  s2, a1, S
    MSUB s2, a3, C, s2
    MUL  s3, a2, C
    MADD s3, a4, S, s3
    MUL  s4, a2, S
    MSUB s4, a4, C, s4
    addi.d I, I, -1
    ST   s1, X, 0 * SIZE
    ST   s2, Y, 0 * SIZE
    ST   s3, X, 1 * SIZE
    ST   s4, Y, 1 * SIZE
    add.d  X, X, INCX
    add.d  Y, Y, INCY
    blt $r0, I, .L998
    .align 3

.L999:
    move $r4, $r12
    jirl $r0, $r1, 0x0
    .align 3

    EPILOGUE
