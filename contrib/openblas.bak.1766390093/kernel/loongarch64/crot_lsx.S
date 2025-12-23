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
#define VX0    $vr8
#define VX1    $vr20
#define VX2    $vr21
#define VX3    $vr22
#define VT0    $vr10
#define VT1    $vr18
#define VXC    $vr23
#define VXS    $vr9
#define VXZ    $vr11
#define x1     $vr12
#define x2     $vr13
#define x3     $vr14
#define x4     $vr15

    PROLOGUE

    bge $r0, N, .L999
    li.d TEMP, 1
    movgr2fr.d a1, $r0
    FFINT a1, a1
    slli.d  TEMP, TEMP, ZBASE_SHIFT
    slli.d  INCX, INCX, ZBASE_SHIFT
    slli.d  INCY, INCY, ZBASE_SHIFT
    move YY, Y
    move XX, X
    MTG  t1, C
    MTG  t2, S
    MTG  t3, a1
#ifdef DOUBLE
    vreplgr2vr.d VXC, t1
    vreplgr2vr.d VXS, t2
    vreplgr2vr.d VXZ, t3
    srai.d I, N, 1
#else
    vreplgr2vr.w VXC, t1
    vreplgr2vr.w VXS, t2
    vreplgr2vr.w VXZ, t3
    srai.d I, N, 2
#endif
    bge $r0, I, .L997
    beq INCX, $r0, .L996
    beq INCY, $r0, .L996
    bne INCX, TEMP, .L221 // INCX!=1 or INCY!=1
    bne INCY, TEMP, .L221

.L111: // C!=0 S!=0
    vld VX0, X, 0 * SIZE
    vld VX2, Y, 0 * SIZE
#ifdef DOUBLE
    vld VX1, X, 2 * SIZE
    vld VX3, Y, 2 * SIZE
    vpickev.d x1, VX1, VX0
    vpickod.d x2, VX1, VX0
    vpickev.d x3, VX3, VX2
    vpickod.d x4, VX3, VX2
    vfmul.d VX0, x1, VXC
    vfmadd.d VX0, x3, VXS, VX0
    vfmul.d VX1, x1, VXS
    vfmsub.d VX1, x3, VXC, VX1
    vfmul.d VX2, x2, VXC
    vfmadd.d VX2, x4, VXS, VX2
    vfmul.d VX3, x2, VXS
    vfmsub.d VX3, x4, VXC, VX3
    vilvl.d x1, VX2 ,VX0
    vilvh.d x2, VX2, VX0
    vilvl.d x3, VX3 ,VX1
    vilvh.d x4, VX3, VX1
    vst x1, X, 0 * SIZE
    vst x3, Y, 0 * SIZE
    vst x2, X, 2 * SIZE
    vst x4, Y, 2 * SIZE
    addi.d X, X, 4 * SIZE
    addi.d Y, Y, 4 * SIZE
#else
    vld VX1, X, 4 * SIZE
    vld VX3, Y, 4 * SIZE
    vpickev.w x1, VX1, VX0
    vpickod.w x2, VX1, VX0
    vpickev.w x3, VX3, VX2
    vpickod.w x4, VX3, VX2
    vfmul.s VX0, x1, VXC
    vfmadd.s VX0, x3, VXS, VX0
    vfmul.s VX1, x1, VXS
    vfmsub.s VX1, x3, VXC, VX1
    vfmul.s VX2, x2, VXC
    vfmadd.s VX2, x4, VXS, VX2
    vfmul.s VX3, x2, VXS
    vfmsub.s VX3, x4, VXC, VX3
    vilvl.w x1, VX2 ,VX0
    vilvh.w x2, VX2, VX0
    vilvl.w x3, VX3 ,VX1
    vilvh.w x4, VX3, VX1
    vst x1, X, 0 * SIZE
    vst x3, Y, 0 * SIZE
    vst x2, X, 4 * SIZE
    vst x4, Y, 4 * SIZE
    addi.d X, X, 8 * SIZE
    addi.d Y, Y, 8 * SIZE
#endif
    addi.d  I, I, -1
    blt $r0, I, .L111
    b .L997
    .align 3

.L221: // C!=0 S!=0
#ifdef DOUBLE
    ld.d t1, X, 0 * SIZE
    ld.d t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.d t3, X, 0 * SIZE
    ld.d t4, X, 1 * SIZE
    add.d X, X, INCX
    vinsgr2vr.d x1, t1, 0
    vinsgr2vr.d x2, t2, 0
    vinsgr2vr.d x1, t3, 1
    vinsgr2vr.d x2, t4, 1
    ld.d t1, Y, 0 * SIZE
    ld.d t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.d t3, Y, 0 * SIZE
    ld.d t4, Y, 1 * SIZE
    vinsgr2vr.d x3, t1, 0
    vinsgr2vr.d x4, t2, 0
    vinsgr2vr.d x3, t3, 1
    vinsgr2vr.d x4, t4, 1
    add.d Y, Y, INCY
    vfmul.d VX0, x1, VXC
    vfmadd.d VX0, x3, VXS, VX0
    vfmul.d VX1, x1, VXS
    vfmsub.d VX1, x3, VXC, VX1
    vfmul.d VX2, x2, VXC
    vfmadd.d VX2, x4, VXS, VX2
    vfmul.d VX3, x2, VXS
    vfmsub.d VX3, x4, VXC, VX3
    vstelm.d VX0, XX, 0, 0
    vstelm.d VX2, XX, 1 * SIZE, 0
    add.d XX, XX, INCX
    vstelm.d VX0, XX, 0, 1
    vstelm.d VX2, XX, 1 * SIZE, 1
    add.d XX, XX, INCX
    vstelm.d VX1, YY, 0, 0
    vstelm.d VX3, YY, 1 * SIZE, 0
    add.d YY, YY, INCY
    vstelm.d VX1, YY, 0, 1
    vstelm.d VX3, YY, 1 * SIZE, 1
    add.d YY, YY, INCY
#else
    ld.w t1, X, 0 * SIZE
    ld.w t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.w t3, X, 0 * SIZE
    ld.w t4, X, 1 * SIZE
    add.d X, X, INCX
    vinsgr2vr.w x1, t1, 0
    vinsgr2vr.w x2, t2, 0
    vinsgr2vr.w x1, t3, 1
    vinsgr2vr.w x2, t4, 1
    ld.w t1, Y, 0 * SIZE
    ld.w t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.w t3, Y, 0 * SIZE
    ld.w t4, Y, 1 * SIZE
    add.d Y, Y, INCY
    vinsgr2vr.w x3, t1, 0
    vinsgr2vr.w x4, t2, 0
    vinsgr2vr.w x3, t3, 1
    vinsgr2vr.w x4, t4, 1
    ld.w t1, X, 0 * SIZE
    ld.w t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.w t3, X, 0 * SIZE
    ld.w t4, X, 1 * SIZE
    vinsgr2vr.w x1, t1, 2
    vinsgr2vr.w x2, t2, 2
    vinsgr2vr.w x1, t3, 3
    vinsgr2vr.w x2, t4, 3
    add.d X, X, INCX
    ld.w t1, Y, 0 * SIZE
    ld.w t2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ld.w t3, Y, 0 * SIZE
    ld.w t4, Y, 1 * SIZE
    vinsgr2vr.w x3, t1, 2
    vinsgr2vr.w x4, t2, 2
    vinsgr2vr.w x3, t3, 3
    vinsgr2vr.w x4, t4, 3
    add.d Y, Y, INCY

    vfmul.s VX0, x1, VXC
    vfmadd.s VX0, x3, VXS, VX0
    vfmul.s VX1, x1, VXS
    vfmsub.s VX1, x3, VXC, VX1
    vfmul.s VX2, x2, VXC
    vfmadd.s VX2, x4, VXS, VX2
    vfmul.s VX3, x2, VXS
    vfmsub.s VX3, x4, VXC, VX3
    vstelm.w VX0, XX, 0, 0
    vstelm.w VX2, XX, 1 * SIZE, 0
    add.d XX, XX, INCX
    vstelm.w VX0, XX, 0, 1
    vstelm.w VX2, XX, 1 * SIZE, 1
    add.d XX, XX, INCX
    vstelm.w VX0, XX, 0, 2
    vstelm.w VX2, XX, 1 * SIZE, 2
    add.d XX, XX, INCX
    vstelm.w VX0, XX, 0, 3
    vstelm.w VX2, XX, 1 * SIZE, 3
    add.d XX, XX, INCX
    vstelm.w VX1, YY, 0, 0
    vstelm.w VX3, YY, 1 * SIZE, 0
    add.d YY, YY, INCY
    vstelm.w VX1, YY, 0, 1
    vstelm.w VX3, YY, 1 * SIZE, 1
    add.d YY, YY, INCY
    vstelm.w VX1, YY, 0, 2
    vstelm.w VX3, YY, 1 * SIZE, 2
    add.d YY, YY, INCY
    vstelm.w VX1, YY, 0, 3
    vstelm.w VX3, YY, 1 * SIZE, 3
    add.d YY, YY, INCY
#endif
    addi.d  I, I, -1
    blt $r0, I, .L221
    b .L997
    .align 3

.L996:
    move I, N
    b .L998
    .align 3

.L997:
#ifdef DOUBLE
    andi I, N, 1
#else
    andi I, N, 3
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
