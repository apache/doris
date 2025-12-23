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
#define VX0    $vr8
#define VX1    $vr20
#define VX2    $vr21
#define VX3    $vr22
#define VT0    $vr10
#define VT1    $vr18
#define VXC    $vr23
#define VXS    $vr9
#define VXZ    $vr19

    PROLOGUE

    bge $r0, N, .L999
    li.d TEMP, 1
    movgr2fr.d a1, $r0
    FFINT a1, a1
    slli.d  TEMP, TEMP, BASE_SHIFT
    slli.d  INCX, INCX, BASE_SHIFT
    slli.d  INCY, INCY, BASE_SHIFT
    move    XX,   X
    move    YY,   Y
#ifdef DOUBLE
    movfr2gr.d t1, C
    vreplgr2vr.d VXC, t1
    movfr2gr.d t2, S
    vreplgr2vr.d VXS, t2
    movfr2gr.d t3, a1
    vreplgr2vr.d VXZ, t3
#else
    movfr2gr.s t1, C
    vreplgr2vr.w VXC, t1
    movfr2gr.s t2, S
    vreplgr2vr.w VXS, t2
    movfr2gr.s t3, a1
    vreplgr2vr.w VXZ, t3
#endif
    srai.d I, N, 3
    bge $r0, I, .L997
    bne INCX, TEMP, .L20
    bne INCY, TEMP, .L121    // INCX==1 and INCY!=1
    b .L111                  // INCX==1 and INCY==1
.L20:
    bne INCY, TEMP, .L221    // INCX!=1 and INCY!=1
    b .L211                  // INCX!=1 and INCY==1

.L111: // C!=0 S!=0
    vld VX0, X, 0 * SIZE
    vld VX2, Y, 0 * SIZE
#ifdef DOUBLE
    vld VX1, X, 2 * SIZE
    vld VX3, Y, 2 * SIZE
#else
    vld VX1, X, 4 * SIZE
    vld VX3, Y, 4 * SIZE
#endif
    VMUL VT0, VX0, VXC
    VFMADD VT0, VX2, VXS, VT0
    VMUL VT1, VX0, VXS
    VMSUB VT1, VX2, VXC, VT1
    vst VT0, X, 0 * SIZE
    vst VT1, Y, 0 * SIZE
    VMUL VT0, VX1, VXC
    VFMADD VT0, VX3, VXS, VT0
    VMUL VT1, VX1, VXS
    VMSUB VT1, VX3, VXC, VT1
#ifdef DOUBLE
    vst VT0, X, 2 * SIZE
    vst VT1, Y, 2 * SIZE
    vld VX0, X, 4 * SIZE
    vld VX2, Y, 4 * SIZE
    vld VX1, X, 6 * SIZE
    vld VX3, Y, 6 * SIZE
    VMUL VT0, VX0, VXC
    VFMADD VT0, VX2, VXS, VT0
    VMUL VT1, VX0, VXS
    VMSUB VT1, VX2, VXC, VT1
#endif
    vst VT0, X, 4 * SIZE
    vst VT1, Y, 4 * SIZE
#ifdef DOUBLE
    VMUL VT0, VX1, VXC
    VFMADD VT0, VX3, VXS, VT0
    VMUL VT1, VX1, VXS
    VMSUB VT1, VX3, VXC, VT1
    vst VT0, X, 6 * SIZE
    vst VT1, Y, 6 * SIZE
#endif
    addi.d X, X, 8 * SIZE
    addi.d Y, Y, 8 * SIZE
    addi.d  I, I, -1
    blt $r0, I, .L111
    b .L997
    .align 3

.L121: // C!=0 S!=0
#ifdef DOUBLE
    vld VX0, X, 0 * SIZE
    ld.d  t1, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.d  t2, Y, 0 * SIZE
    vinsgr2vr.d VX2, t1, 0
    vinsgr2vr.d VX2, t2, 1
#else
    vld VX0, X, 0 * SIZE
    ld.w  t1, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.w  t2, Y, 0 * SIZE
#endif
    add.d Y, Y, INCY
#ifndef DOUBLE
    ld.w t3, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.w t4, Y, 0 * SIZE
    vinsgr2vr.w VX2, t1, 0
    vinsgr2vr.w VX2, t2, 1
    vinsgr2vr.w VX2, t3, 2
    vinsgr2vr.w VX2, t4, 3
    add.d Y, Y, INCY
#endif
    VMUL VT0, VX0, VXC
    VFMADD VT0, VX2, VXS, VT0
    VMUL VT1, VX0, VXS
    VMSUB VT1, VX2, VXC, VT1
    vst VT0, X, 0 * SIZE
#ifdef DOUBLE
    vstelm.d VT1, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.d VT1, YY, 0, 1
    add.d YY, YY, INCY
    vld VX0, X, 2 * SIZE
    ld.d t3, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.d t4, Y, 0 * SIZE
    vinsgr2vr.d VX2, t3, 0
    vinsgr2vr.d VX2, t4, 1
    add.d Y, Y, INCY
    VMUL VT0, VX0, VXC
    VFMADD VT0, VX2, VXS, VT0
    VMUL VT1, VX0, VXS
    VMSUB VT1, VX2, VXC, VT1
    vst VT0, X, 2 * SIZE
    vstelm.d VT1, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.d VT1, YY, 0, 1
    add.d YY, YY, INCY
    vld VX1, X, 4 * SIZE
    ld.d t1, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.d t2, Y, 0 * SIZE
    vinsgr2vr.d VX3, t1, 0
    vinsgr2vr.d VX3, t2, 1
#else
    vstelm.w VT1, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.w VT1, YY, 0, 1
    add.d YY, YY, INCY
    vstelm.w VT1, YY, 0, 2
    add.d YY, YY, INCY
    vstelm.w VT1, YY, 0, 3
    add.d YY, YY, INCY
    vld VX1, X, 4 * SIZE
    ld.w t1, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.w t2, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.w t3, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.w t4, Y, 0 * SIZE
    vinsgr2vr.w VX3, t1, 0
    vinsgr2vr.w VX3, t2, 1
    vinsgr2vr.w VX3, t3, 2
    vinsgr2vr.w VX3, t4, 3
#endif
    add.d Y, Y, INCY
    VMUL VT0, VX1, VXC
    VFMADD VT0, VX3, VXS, VT0
    VMUL VT1, VX1, VXS
    VMSUB VT1, VX3, VXC, VT1
    vst VT0, X, 4 * SIZE
#ifdef DOUBLE
    vstelm.d VT1, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.d VT1, YY, 0, 1
    add.d YY, YY, INCY
    vld VX1, X, 6 * SIZE
    ld.d t3, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.d t4, Y, 0 * SIZE
    vinsgr2vr.d VX3, t3, 0
    vinsgr2vr.d VX3, t4, 1
    add.d Y, Y, INCY
    VMUL VT0, VX1, VXC
    VFMADD VT0, VX3, VXS, VT0
    VMUL VT1, VX1, VXS
    VMSUB VT1, VX3, VXC, VT1
    vst VT0, X, 6 * SIZE
    vstelm.d VT1, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.d VT1, YY, 0, 1
#else
    vstelm.w VT1, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.w VT1, YY, 0, 1
    add.d YY, YY, INCY
    vstelm.w VT1, YY, 0, 2
    add.d YY, YY, INCY
    vstelm.w VT1, YY, 0, 3
#endif
    add.d YY, YY, INCY
    addi.d X, X, 8 * SIZE
    addi.d  I, I, -1
    blt $r0, I, .L121
    b .L997
    .align 3

.L211: // C!=0 S!=0
#ifdef DOUBLE
    vld VX2, Y, 0 * SIZE
    ld.d  t1, X, 0 * SIZE
    add.d X, X, INCX
    ld.d  t2, X, 0 * SIZE
#else
    vld VX2, Y, 0 * SIZE
    ld.w  t1, X, 0 * SIZE
    add.d X, X, INCX
    ld.w  t2, X, 0 * SIZE
    add.d X, X, INCX
    ld.w t3, X, 0 * SIZE
    add.d X, X, INCX
    ld.w t4, X, 0 * SIZE
#endif
#ifdef DOUBLE
    vinsgr2vr.d VX0, t1, 0
    vinsgr2vr.d VX0, t2, 1
#else
    vinsgr2vr.w VX0, t1, 0
    vinsgr2vr.w VX0, t2, 1
    vinsgr2vr.w VX0, t3, 2
    vinsgr2vr.w VX0, t4, 3
#endif
    add.d X, X, INCX
    VMUL VT0, VXC, VX0
    VFMADD VT0, VX2, VXS, VT0
    VMUL VT1, VXS, VX0
    VMSUB VT1, VX2, VXC, VT1
#ifdef DOUBLE
    vstelm.d VT0, XX, 0, 0
    add.d XX, XX, INCX
    vstelm.d VT0, XX, 0, 1
    add.d XX, XX, INCX
    vst VT1, Y, 0 * SIZE
    vld VX2, Y, 2 * SIZE
    ld.d t3, X, 0 * SIZE
    add.d X, X, INCX
    ld.d t4, X, 0 * SIZE
    vinsgr2vr.d VX0, t3, 0
    vinsgr2vr.d VX0, t4, 1
    add.d X, X, INCX
    VMUL VT0, VXC, VX0
    VFMADD VT0, VX2, VXS, VT0
    VMUL VT1, VXS, VX0
    VMSUB VT1, VX2, VXC, VT1
    vstelm.d VT0, XX, 0, 0
    add.d XX, XX, INCX
    vstelm.d VT0, XX, 0, 1
    add.d XX, XX, INCX
    vst VT1, Y, 2 * SIZE
    vld VX3, Y, 4 * SIZE
    ld.d t1, X, 0 * SIZE
    add.d X, X, INCX
    ld.d t2, X, 0 * SIZE
    vinsgr2vr.d VX1, t1, 0
    vinsgr2vr.d VX1, t2, 1
    add.d X, X, INCX
    VMUL VT0, VX1, VXC
    VFMADD VT0, VX3, VXS, VT0
    VMUL VT1, VX1, VXS
    VMSUB VT1, VX3, VXC, VT1
    vstelm.d VT0, XX, 0, 0
    add.d XX, XX, INCX
    vstelm.d VT0, XX, 0, 1
    add.d XX, XX, INCX
    vst VT1, Y, 4 * SIZE
    vld VX3, Y, 6 * SIZE
    ld.d t3, X, 0 * SIZE
    add.d X, X, INCX
    ld.d t4, X, 0 * SIZE
    vinsgr2vr.d VX1, t3, 0
    vinsgr2vr.d VX1, t4, 1
    add.d X, X, INCX
    VMUL VT0, VX1, VXC
    VFMADD VT0, VX3, VXS, VT0
    VMUL VT1, VX1, VXS
    VMSUB VT1, VX3, VXC, VT1
    vstelm.d VT0, XX, 0, 0
    add.d XX, XX, INCX
    vstelm.d VT0, XX, 0, 1
    add.d XX, XX, INCX
    vst VT1, Y, 6 * SIZE
#else
    vstelm.w VT0, XX, 0, 0
    add.d XX, XX, INCX
    vstelm.w VT0, XX, 0, 1
    add.d XX, XX, INCX
    vstelm.w VT0, XX, 0, 2
    add.d XX, XX, INCX
    vstelm.w VT0, XX, 0, 3
    add.d XX, XX, INCX
    vst VT1, Y, 0 * SIZE
    vld VX3, Y, 4 * SIZE
    ld.w t1, X, 0 * SIZE
    add.d X, X, INCX
    ld.w t2, X, 0 * SIZE
    add.d X, X, INCX
    ld.w t3, X, 0 * SIZE
    add.d X, X, INCX
    ld.w t4, X, 0 * SIZE
    vinsgr2vr.w VX1, t1, 0
    vinsgr2vr.w VX1, t2, 1
    vinsgr2vr.w VX1, t3, 2
    vinsgr2vr.w VX1, t4, 3
    add.d X, X, INCX
    VMUL VT0, VX1, VXC
    VFMADD VT0, VX3, VXS, VT0
    VMUL VT1, VX1, VXS
    VMSUB VT1, VX3, VXC, VT1
    vstelm.w VT0, XX, 0, 0
    add.d XX, XX, INCX
    vstelm.w VT0, XX, 0, 1
    add.d XX, XX, INCX
    vstelm.w VT0, XX, 0, 2
    add.d XX, XX, INCX
    vstelm.w VT0, XX, 0, 3
    add.d XX, XX, INCX
    vst VT1, Y, 4 * SIZE
#endif
    addi.d Y, Y, 8 * SIZE
    addi.d  I, I, -1
    blt $r0, I, .L211
    b .L997
    .align 3

.L221: // C!=0 S!=0
#ifdef DOUBLE
    ld.d  t1, X, 0 * SIZE
    add.d X, X, INCX
    ld.d  t2, X, 0 * SIZE
    add.d X, X, INCX
#else
    ld.w  t1, X, 0 * SIZE
    add.d X, X, INCX
    ld.w  t2, X, 0 * SIZE
    add.d X, X, INCX
    ld.w t3, X, 0 * SIZE
    add.d X, X, INCX
    ld.w t4, X, 0 * SIZE
    add.d X, X, INCX
#endif
#ifdef DOUBLE
    vinsgr2vr.d VX0, t1, 0
    vinsgr2vr.d VX0, t2, 1
    ld.d t1, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.d t2, Y, 0 * SIZE
    vinsgr2vr.d VX2, t1, 0
    vinsgr2vr.d VX2, t2, 1
    add.d Y, Y, INCY
    VMUL VT0, VX0, VXC
    VFMADD VT0, VX2, VXS, VT0
    VMUL VT1, VX0, VXS
    VMSUB VT1, VX2, VXC, VT1
    vstelm.d VT0, XX, 0, 0
    add.d XX, XX, INCX
    vstelm.d VT0, XX, 0, 1
    add.d XX, XX, INCX
    vstelm.d VT1, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.d VT1, YY, 0, 1
    add.d YY, YY, INCY
    ld.d t3, X, 0 * SIZE
    add.d X, X, INCX
    ld.d t4, X, 0 * SIZE
    add.d X, X, INCX
    vinsgr2vr.d VX0, t3, 0
    vinsgr2vr.d VX0, t4, 1
    ld.d t3, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.d t4, Y, 0 * SIZE
    vinsgr2vr.d VX2, t3, 0
    vinsgr2vr.d VX2, t4, 1
    add.d Y, Y, INCY
    VMUL  VT0, VX0, VXC
    VFMADD VT0, VX2, VXS, VT0
    VMUL  VT1, VX0, VXS
    VMSUB VT1, VX2, VXC, VT1
    vstelm.d VT0, XX, 0, 0
    add.d XX, XX, INCX
    vstelm.d VT0, XX, 0, 1
    add.d XX, XX, INCX
    vstelm.d VT1, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.d VT1, YY, 0, 1
    add.d YY, YY, INCY
    ld.d t1, X, 0 * SIZE
    add.d X, X, INCX
    ld.d t2, X, 0 * SIZE
    add.d X, X, INCX
    vinsgr2vr.d VX1, t1, 0
    vinsgr2vr.d VX1, t2, 1
    ld.d t1, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.d t2, Y, 0 * SIZE
    vinsgr2vr.d VX3, t1, 0
    vinsgr2vr.d VX3, t2, 1
    add.d Y, Y, INCY
    VMUL VT0, VX1, VXC
    VFMADD VT0, VX3, VXS, VT0
    VMUL VT1, VX1, VXS
    VMSUB VT1, VX3, VXC, VT1
    vstelm.d VT0, XX, 0, 0
    add.d XX, XX, INCX
    vstelm.d VT0, XX, 0, 1
    add.d XX, XX, INCX
    vstelm.d VT1, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.d VT1, YY, 0, 1
    add.d YY, YY, INCY
    ld.d t3, X, 0 * SIZE
    add.d X, X, INCX
    ld.d t4, X, 0 * SIZE
    vinsgr2vr.d VX1, t3, 0
    vinsgr2vr.d VX1, t4, 1
    add.d X, X, INCX
    ld.d t3, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.d t4, Y, 0 * SIZE
    vinsgr2vr.d VX3, t3, 0
    vinsgr2vr.d VX3, t4, 1
    add.d Y, Y, INCY
    VMUL VT0, VX1, VXC
    VFMADD VT0, VX3, VXS, VT0
    VMUL VT1, VX1, VXS
    VMSUB VT1, VX3, VXC, VT1
    vstelm.d VT0, XX, 0, 0
    add.d XX, XX, INCX
    vstelm.d VT0, XX, 0, 1
    add.d XX, XX, INCX
    vstelm.d VT1, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.d VT1, YY, 0, 1
    add.d YY, YY, INCY
#else
    vinsgr2vr.w VX0, t1, 0
    vinsgr2vr.w VX0, t2, 1
    vinsgr2vr.w VX0, t3, 2
    vinsgr2vr.w VX0, t4, 3
    ld.w t1, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.w t2, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.w t3, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.w t4, Y, 0 * SIZE
    vinsgr2vr.w VX2, t1, 0
    vinsgr2vr.w VX2, t2, 1
    vinsgr2vr.w VX2, t3, 2
    vinsgr2vr.w VX2, t4, 3
    add.d Y, Y, INCY
    VMUL VT0, VX0, VXC
    VFMADD VT0, VX2, VXS, VT0
    VMUL VT1, VX0, VXS
    VMSUB VT1, VX2, VXC, VT1
    vstelm.w VT0, XX, 0, 0
    add.d XX, XX, INCX
    vstelm.w VT0, XX, 0, 1
    add.d XX, XX, INCX
    vstelm.w VT0, XX, 0, 2
    add.d XX, XX, INCX
    vstelm.w VT0, XX, 0, 3
    add.d XX, XX, INCX
    vstelm.w VT1, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.w VT1, YY, 0, 1
    add.d YY, YY, INCY
    vstelm.w VT1, YY, 0, 2
    add.d YY, YY, INCY
    vstelm.w VT1, YY, 0, 3
    add.d YY, YY, INCY
    ld.w t1, X, 0 * SIZE
    add.d X, X, INCX
    ld.w t2, X, 0 * SIZE
    add.d X, X, INCX
    ld.w t3, X, 0 * SIZE
    add.d X, X, INCX
    ld.w t4, X, 0 * SIZE
    vinsgr2vr.w VX1, t1, 0
    vinsgr2vr.w VX1, t2, 1
    vinsgr2vr.w VX1, t3, 2
    vinsgr2vr.w VX1, t4, 3
    add.d X, X, INCX
    ld.w t1, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.w t2, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.w t3, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.w t4, Y, 0 * SIZE
    vinsgr2vr.w VX3, t1, 0
    vinsgr2vr.w VX3, t2, 1
    vinsgr2vr.w VX3, t3, 2
    vinsgr2vr.w VX3, t4, 3
    add.d Y, Y, INCY
    VMUL VT0, VX1, VXC
    VFMADD VT0, VX3, VXS, VT0
    VMUL VT1, VX1, VXS
    VMSUB VT1, VX3, VXC, VT1
    vstelm.w VT0, XX, 0, 0
    add.d XX, XX, INCX
    vstelm.w VT0, XX, 0, 1
    add.d XX, XX, INCX
    vstelm.w VT0, XX, 0, 2
    add.d XX, XX, INCX
    vstelm.w VT0, XX, 0, 3
    add.d XX, XX, INCX
    vstelm.w VT1, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.w VT1, YY, 0, 1
    add.d YY, YY, INCY
    vstelm.w VT1, YY, 0, 2
    add.d YY, YY, INCY
    vstelm.w VT1, YY, 0, 3
    add.d YY, YY, INCY
#endif
    addi.d  I, I, -1
    blt $r0, I, .L221
    b .L997
    .align 3

.L997:
    andi I, N, 7
    bge $r0, I, .L999
    .align 3

.L998:
    LD   $f12, X, 0 * SIZE
    LD   $f13, Y, 0 * SIZE
    MUL  $f10, $f12, C
    MADD $f10, $f13, S, $f10
    ST   $f10, X, 0 * SIZE
    addi.d I, I, -1
    MUL  $f20, $f12, S
    MSUB $f20, $f13, C, $f20
    ST   $f20, Y, 0 * SIZE
    add.d  X, X, INCX
    add.d  Y, Y, INCY
    blt $r0, I, .L998
    .align 3

.L999:
    move $r4, $r12
    jirl $r0, $r1, 0x0
    .align 3

    EPILOGUE
