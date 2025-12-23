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
#define XX     $r5
#define YY     $r6
#define ALPHAR $f0
#define ALPHAI $f1
#define X      $r7
#define INCX   $r8
#define Y      $r9
#define INCY   $r10

#define I      $r12
#define TEMP   $r13
#define t1     $r14
#define t2     $r16
#define t3     $r15
#define t4     $r17
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
#define VXAR   $xr23
#define VXAI   $xr19
#define x1     $xr18
#define x2     $xr17
#define x3     $xr16
#define x4     $xr15

    PROLOGUE

    bge $r0, N, .L999
    li.d TEMP, 1
    movgr2fr.d a1, $r0
    FFINT  a1, a1
    CMPEQ  $fcc0, ALPHAR, a1
    CMPEQ  $fcc1, ALPHAI, a1
    bceqz $fcc0, .L10
    bcnez $fcc1, .L999
.L10:
    slli.d  TEMP, TEMP, ZBASE_SHIFT
    slli.d  INCX, INCX, ZBASE_SHIFT
    slli.d  INCY, INCY, ZBASE_SHIFT
    MTG  t1, ALPHAR
    MTG  t2, ALPHAI
#ifdef DOUBLE
    xvreplgr2vr.d VXAR, t1
    xvreplgr2vr.d VXAI, t2
    srai.d I, N, 2
#else
    xvreplgr2vr.w VXAR, t1
    xvreplgr2vr.w VXAI, t2
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
#ifdef DOUBLE
    xvld VX0, X, 0 * SIZE
    xvld VX2, Y, 0 * SIZE
    xvld VX1, X, 4 * SIZE
    xvld VX3, Y, 4 * SIZE
    xvpickev.d x1, VX1, VX0
    xvpickod.d x2, VX1, VX0
    xvpickev.d x3, VX3, VX2
    xvpickod.d x4, VX3, VX2
#else
    xvld VX0, X, 0 * SIZE
    xvld VX2, Y, 0 * SIZE
    xvld VX1, X, 8 * SIZE
    xvld VX3, Y, 8 * SIZE
    xvpickev.w x1, VX1, VX0
    xvpickod.w x2, VX1, VX0
    xvpickev.w x3, VX3, VX2
    xvpickod.w x4, VX3, VX2
#endif
#if !defined(CONJ)
#ifdef DOUBLE
    xvfmul.d VX0, VXAI, x2
    xvfmul.d VX2, VXAI, x1
    xvfmsub.d VX1, VXAR, x1, VX0
    xvfmadd.d VX3, x2, VXAR, VX2
    xvfadd.d x3, x3, VX1
    xvfadd.d x4, x4, VX3
#else
    xvfmul.s VX0, VXAI, x2
    xvfmul.s VX2, VXAI, x1
    xvfmsub.s VX1, VXAR, x1, VX0
    xvfmadd.s VX3, x2, VXAR, VX2
    xvfadd.s x3, x3, VX1
    xvfadd.s x4, x4, VX3
#endif
#else
#ifdef DOUBLE
    xvfmul.d VX0, VXAI, x2
    xvfmul.d VX2, VXAI, x1
    xvfmadd.d VX1, VXAR, x1, VX0
    xvfmsub.d VX3, x2, VXAR, VX2
    xvfadd.d x3, x3, VX1
    xvfsub.d x4, x4, VX3
#else
    xvfmul.s VX0, VXAI, x2
    xvfmul.s VX2, VXAI, x1
    xvfmadd.s VX1, VXAR, x1, VX0
    xvfmsub.s VX3, x2, VXAR, VX2
    xvfadd.s x3, x3, VX1
    xvfsub.s x4, x4, VX3
#endif
#endif
#ifdef DOUBLE
    xvilvl.d VX2, x4 ,x3
    xvilvh.d VX3, x4, x3
    xvst VX2, Y, 0 * SIZE
    xvst VX3, Y, 4 * SIZE
    addi.d X, X, 8 * SIZE
    addi.d Y, Y, 8 * SIZE
#else
    xvilvl.w VX2, x4 ,x3
    xvilvh.w VX3, x4, x3
    xvst VX2, Y, 0 * SIZE
    xvst VX3, Y, 8 * SIZE
    addi.d X, X, 16 * SIZE
    addi.d Y, Y, 16 * SIZE
#endif
    addi.d  I, I, -1
    blt $r0, I, .L111
    b .L997
    .align 3

.L12: // INCX==1 and INCY!=1
    bge $r0, I, .L997
    move YY, Y
    .align 3

.L121:
#ifdef DOUBLE
    xvld VX0, X, 0 * SIZE
    xvld VX1, X, 4 * SIZE
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
    ld.d t1, Y, 0 * SIZE
    ld.d t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.d t3, Y, 0 * SIZE
    ld.d t4, Y, 1 * SIZE
    xvinsgr2vr.d x3, t1, 1
    xvinsgr2vr.d x4, t2, 1
    xvinsgr2vr.d x3, t3, 3
    xvinsgr2vr.d x4, t4, 3
    add.d Y, Y, INCY
    xvpickev.d x1, VX1, VX0
    xvpickod.d x2, VX1, VX0
#else
    xvld VX0, X, 0 * SIZE
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
    xvinsgr2vr.w x3, t1, 6
    xvinsgr2vr.w x4, t2, 6
    xvinsgr2vr.w x3, t3, 7
    xvinsgr2vr.w x4, t4, 7
    add.d Y, Y, INCY
    xvpickev.w x1, VX1, VX0
    xvpickod.w x2, VX1, VX0
#endif
#if !defined(CONJ)
#ifdef DOUBLE
    xvfmul.d VX0, VXAI, x2
    xvfmul.d VX2, VXAI, x1
    xvfmsub.d VX1, VXAR, x1, VX0
    xvfmadd.d VX3, x2, VXAR, VX2
    xvfadd.d x3, x3, VX1
    xvfadd.d x4, x4, VX3
#else
    xvfmul.s VX0, VXAI, x2
    xvfmul.s VX2, VXAI, x1
    xvfmsub.s VX1, VXAR, x1, VX0
    xvfmadd.s VX3, x2, VXAR, VX2
    xvfadd.s x3, x3, VX1
    xvfadd.s x4, x4, VX3
#endif
#else
#ifdef DOUBLE
    xvfmul.d VX0, VXAI, x2
    xvfmul.d VX2, VXAI, x1
    xvfmadd.d VX1, VXAR, x1, VX0
    xvfmsub.d VX3, x2, VXAR, VX2
    xvfadd.d x3, x3, VX1
    xvfsub.d x4, x4, VX3
#else
    xvfmul.s VX0, VXAI, x2
    xvfmul.s VX2, VXAI, x1
    xvfmadd.s VX1, VXAR, x1, VX0
    xvfmsub.s VX3, x2, VXAR, VX2
    xvfadd.s x3, x3, VX1
    xvfsub.s x4, x4, VX3
#endif
#endif
#ifdef DOUBLE
    xvstelm.d x3, YY, 0 * SIZE, 0
    xvstelm.d x4, YY, 1 * SIZE, 0
    add.d YY, YY, INCY
    xvstelm.d x3, YY, 0 * SIZE, 2
    xvstelm.d x4, YY, 1 * SIZE, 2
    add.d YY, YY, INCY
    xvstelm.d x3, YY, 0 * SIZE, 1
    xvstelm.d x4, YY, 1 * SIZE, 1
    add.d YY, YY, INCY
    xvstelm.d x3, YY, 0 * SIZE, 3
    xvstelm.d x4, YY, 1 * SIZE, 3
    add.d YY, YY, INCY
    addi.d X, X, 8 * SIZE
    addi.d  I, I, -1
#else
    addi.d  I, I, -1
    xvstelm.w x3, YY, 0 * SIZE, 0
    xvstelm.w x4, YY, 1 * SIZE, 0
    add.d YY, YY, INCY
    xvstelm.w x3, YY, 0 * SIZE, 1
    xvstelm.w x4, YY, 1 * SIZE, 1
    add.d YY, YY, INCY
    xvstelm.w x3, YY, 0 * SIZE, 4
    xvstelm.w x4, YY, 1 * SIZE, 4
    add.d YY, YY, INCY
    xvstelm.w x3, YY, 0 * SIZE, 5
    xvstelm.w x4, YY, 1 * SIZE, 5
    add.d YY, YY, INCY
    xvstelm.w x3, YY, 0 * SIZE, 2
    xvstelm.w x4, YY, 1 * SIZE, 2
    add.d YY, YY, INCY
    xvstelm.w x3, YY, 0 * SIZE, 3
    xvstelm.w x4, YY, 1 * SIZE, 3
    add.d YY, YY, INCY
    xvstelm.w x3, YY, 0 * SIZE, 6
    xvstelm.w x4, YY, 1 * SIZE, 6
    add.d YY, YY, INCY
    xvstelm.w x3, YY, 0 * SIZE, 7
    xvstelm.w x4, YY, 1 * SIZE, 7
    add.d YY, YY, INCY
    addi.d X, X, 16 * SIZE
#endif
    blt $r0, I, .L121
    b .L997
    .align 3

.L21:// INCX!=1 and INCY==1
    bge $r0, I, .L997
    .align 3

.L211:
#ifdef DOUBLE
    xvld VX2, Y, 0 * SIZE
    xvld VX3, Y, 4 * SIZE
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
    ld.d t1, X, 0 * SIZE
    ld.d t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.d t3, X, 0 * SIZE
    ld.d t4, X, 1 * SIZE
    xvinsgr2vr.d x1, t1, 1
    xvinsgr2vr.d x2, t2, 1
    xvinsgr2vr.d x1, t3, 3
    xvinsgr2vr.d x2, t4, 3
    add.d X, X, INCX
    xvpickev.d x3, VX3, VX2
    xvpickod.d x4, VX3, VX2
#else
    xvld VX2, Y, 0 * SIZE
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
    xvpickev.w x3, VX3, VX2
    xvpickod.w x4, VX3, VX2
#endif
#if !defined(CONJ)
#ifdef DOUBLE
    xvfmul.d VX0, VXAI, x2
    xvfmul.d VX2, VXAI, x1
    xvfmsub.d VX1, VXAR, x1, VX0
    xvfmadd.d VX3, x2, VXAR, VX2
    xvfadd.d x3, x3, VX1
    xvfadd.d x4, x4, VX3
#else
    xvfmul.s VX0, VXAI, x2
    xvfmul.s VX2, VXAI, x1
    xvfmsub.s VX1, VXAR, x1, VX0
    xvfmadd.s VX3, x2, VXAR, VX2
    xvfadd.s x3, x3, VX1
    xvfadd.s x4, x4, VX3
#endif
#else
#ifdef DOUBLE
    xvfmul.d VX0, VXAI, x2
    xvfmul.d VX2, VXAI, x1
    xvfmadd.d VX1, VXAR, x1, VX0
    xvfmsub.d VX3, x2, VXAR, VX2
    xvfadd.d x3, x3, VX1
    xvfsub.d x4, x4, VX3
#else
    xvfmul.s VX0, VXAI, x2
    xvfmul.s VX2, VXAI, x1
    xvfmadd.s VX1, VXAR, x1, VX0
    xvfmsub.s VX3, x2, VXAR, VX2
    xvfadd.s x3, x3, VX1
    xvfsub.s x4, x4, VX3
#endif
#endif
#ifdef DOUBLE
    xvilvl.d VX2, x4 ,x3
    xvilvh.d VX3, x4, x3
    addi.d  I, I, -1
    xvst VX2, Y, 0 * SIZE
    xvst VX3, Y, 4 * SIZE
    addi.d Y, Y, 8 * SIZE
#else
    xvilvl.w VX2, x4 ,x3
    xvilvh.w VX3, x4, x3
    addi.d  I, I, -1
    xvst VX2, Y, 0 * SIZE
    xvst VX3, Y, 8 * SIZE
    addi.d Y, Y, 16 * SIZE
#endif
    blt $r0, I, .L211
    b .L997
    .align 3

.L22:
    bge $r0, I, .L997
    move YY, Y
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
    xvinsgr2vr.d x3, t1, 0
    xvinsgr2vr.d x4, t2, 0
    xvinsgr2vr.d x3, t3, 1
    xvinsgr2vr.d x4, t4, 1
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
    xvinsgr2vr.w x3, t1, 4
    xvinsgr2vr.w x4, t2, 4
    xvinsgr2vr.w x3, t3, 5
    xvinsgr2vr.w x4, t4, 5
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
#endif
#if !defined(CONJ)
#ifdef DOUBLE
    xvfmul.d VX0, VXAI, x2
    xvfmul.d VX2, VXAI, x1
    xvfmsub.d VX1, VXAR, x1, VX0
    xvfmadd.d VX3, x2, VXAR, VX2
    xvfadd.d x3, x3, VX1
    xvfadd.d x4, x4, VX3
#else
    xvfmul.s VX0, VXAI, x2
    xvfmul.s VX2, VXAI, x1
    xvfmsub.s VX1, VXAR, x1, VX0
    xvfmadd.s VX3, x2, VXAR, VX2
    xvfadd.s x3, x3, VX1
    xvfadd.s x4, x4, VX3
#endif
#else
#ifdef DOUBLE
    xvfmul.d VX0, VXAI, x2
    xvfmul.d VX2, VXAI, x1
    xvfmadd.d VX1, VXAR, x1, VX0
    xvfmsub.d VX3, x2, VXAR, VX2
    xvfadd.d x3, x3, VX1
    xvfsub.d x4, x4, VX3
#else
    xvfmul.s VX0, VXAI, x2
    xvfmul.s VX2, VXAI, x1
    xvfmadd.s VX1, VXAR, x1, VX0
    xvfmsub.s VX3, x2, VXAR, VX2
    xvfadd.s x3, x3, VX1
    xvfsub.s x4, x4, VX3
#endif
#endif
    addi.d  I, I, -1
#ifdef DOUBLE
    xvstelm.d x3, YY, 0 * SIZE, 0
    xvstelm.d x4, YY, 1 * SIZE, 0
    add.d YY, YY, INCY
    xvstelm.d x3, YY, 0 * SIZE, 1
    xvstelm.d x4, YY, 1 * SIZE, 1
    add.d YY, YY, INCY
    xvstelm.d x3, YY, 0 * SIZE, 2
    xvstelm.d x4, YY, 1 * SIZE, 2
    add.d YY, YY, INCY
    xvstelm.d x3, YY, 0 * SIZE, 3
    xvstelm.d x4, YY, 1 * SIZE, 3
#else
    xvstelm.w x3, YY, 0 * SIZE, 0
    xvstelm.w x4, YY, 1 * SIZE, 0
    add.d YY, YY, INCY
    xvstelm.w x3, YY, 0 * SIZE, 1
    xvstelm.w x4, YY, 1 * SIZE, 1
    add.d YY, YY, INCY
    xvstelm.w x3, YY, 0 * SIZE, 2
    xvstelm.w x4, YY, 1 * SIZE, 2
    add.d YY, YY, INCY
    xvstelm.w x3, YY, 0 * SIZE, 3
    xvstelm.w x4, YY, 1 * SIZE, 3
    add.d YY, YY, INCY
    xvstelm.w x3, YY, 0 * SIZE, 4
    xvstelm.w x4, YY, 1 * SIZE, 4
    add.d YY, YY, INCY
    xvstelm.w x3, YY, 0 * SIZE, 5
    xvstelm.w x4, YY, 1 * SIZE, 5
    add.d YY, YY, INCY
    xvstelm.w x3, YY, 0 * SIZE, 6
    xvstelm.w x4, YY, 1 * SIZE, 6
    add.d YY, YY, INCY
    xvstelm.w x3, YY, 0 * SIZE, 7
    xvstelm.w x4, YY, 1 * SIZE, 7
#endif
    add.d YY, YY, INCY
    blt $r0, I, .L222
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
    LD  a1, X, 0 * SIZE
    LD  a2, X, 1 * SIZE
    LD  a3, Y, 0 * SIZE
    LD  a4, Y, 1 * SIZE
    addi.d I, I, -1
#if !defined(CONJ)
    MUL  s1, ALPHAI, a2
    MUL  s2, ALPHAI, a1
    MSUB s3, ALPHAR, a1, s1
    MADD s4, a2, ALPHAR, s2
    ADD  s3, s3, a3
    ADD  s4, s4, a4
#else
    MUL  s1, ALPHAI, a2
    MUL  s2, ALPHAI, a1
    MADD s3, ALPHAR, a1, s1
    MSUB s4, a2, ALPHAR, s2
    ADD  s3, s3, a3
    SUB  s4, a4, s4
#endif
    ST  s3, Y, 0 * SIZE
    ST  s4, Y, 1 * SIZE
    add.d X, X, INCX
    add.d Y, Y, INCY
    blt $r0, I, .L998
    .align 3

.L999:
    move $r4, $r12
    jirl $r0, $r1, 0x0
    .align 3

    EPILOGUE
