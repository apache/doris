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
#define VX0    $xr8
#define VX1    $xr20
#define VX2    $xr21
#define VX3    $xr22
#define VT0    $xr10
#define VT1    $xr18
#define VXC    $xr23
#define VXS    $xr9
#define VXZ    $xr19

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
    xvreplgr2vr.d VXC, t1
    movfr2gr.d t2, S
    xvreplgr2vr.d VXS, t2
    movfr2gr.d t3, a1
    xvreplgr2vr.d VXZ, t3
#else
    movfr2gr.s t1, C
    xvreplgr2vr.w VXC, t1
    movfr2gr.s t2, S
    xvreplgr2vr.w VXS, t2
    movfr2gr.s t3, a1
    xvreplgr2vr.w VXZ, t3
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
    xvld VX0, X, 0 * SIZE
    xvld VX2, Y, 0 * SIZE
#ifdef DOUBLE
    xvld VX1, X, 4 * SIZE
    xvld VX3, Y, 4 * SIZE
#endif
    XVMUL VT0, VX0, VXC
    XVFMADD VT0, VX2, VXS, VT0
    XVMUL VT1, VX0, VXS
    XVMSUB VT1, VX2, VXC, VT1
    xvst VT0, X, 0 * SIZE
    xvst VT1, Y, 0 * SIZE
#ifdef DOUBLE
    XVMUL VT0, VX1, VXC
    XVFMADD VT0, VX3, VXS, VT0
    XVMUL VT1, VX1, VXS
    XVMSUB VT1, VX3, VXC, VT1
    xvst VT0, X, 4 * SIZE
    xvst VT1, Y, 4 * SIZE
#endif
    addi.d X, X, 8 * SIZE
    addi.d Y, Y, 8 * SIZE
    addi.d  I, I, -1
    blt $r0, I, .L111
    b .L997
    .align 3

.L121: // C!=0 S!=0
#ifdef DOUBLE
    xvld VX0, X, 0 * SIZE
    ld.d  t1, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.d  t2, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.d  t3, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.d  t4, Y, 0 * SIZE
    add.d Y, Y, INCY
    xvinsgr2vr.d VX2, t1, 0
    xvinsgr2vr.d VX2, t2, 1
    xvinsgr2vr.d VX2, t3, 2
    xvinsgr2vr.d VX2, t4, 3
#else
    xvld VX0, X, 0 * SIZE
    ld.w  t1, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.w  t2, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.w  t3, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.w  t4, Y, 0 * SIZE
    add.d Y, Y, INCY
    xvinsgr2vr.w VX2, t1, 0
    xvinsgr2vr.w VX2, t2, 1
    xvinsgr2vr.w VX2, t3, 2
    xvinsgr2vr.w VX2, t4, 3
    ld.w  t1, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.w  t2, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.w  t3, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.w  t4, Y, 0 * SIZE
    add.d Y, Y, INCY
    xvinsgr2vr.w VX2, t1, 4
    xvinsgr2vr.w VX2, t2, 5
    xvinsgr2vr.w VX2, t3, 6
    xvinsgr2vr.w VX2, t4, 7
#endif
    XVMUL VT0, VX0, VXC
    XVFMADD VT0, VX2, VXS, VT0
    XVMUL VT1, VX0, VXS
    XVMSUB VT1, VX2, VXC, VT1
    xvst VT0, X, 0 * SIZE
#ifdef DOUBLE
    xvstelm.d VT1, YY, 0, 0
    add.d YY, YY, INCY
    xvstelm.d VT1, YY, 0, 1
    add.d YY, YY, INCY
    xvstelm.d VT1, YY, 0, 2
    add.d YY, YY, INCY
    xvstelm.d VT1, YY, 0, 3
    add.d YY, YY, INCY

    xvld VX0, X, 4 * SIZE
    ld.d  t1, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.d  t2, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.d  t3, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.d  t4, Y, 0 * SIZE
    add.d Y, Y, INCY
    xvinsgr2vr.d VX2, t1, 0
    xvinsgr2vr.d VX2, t2, 1
    xvinsgr2vr.d VX2, t3, 2
    xvinsgr2vr.d VX2, t4, 3

    XVMUL VT0, VX0, VXC
    XVFMADD VT0, VX2, VXS, VT0
    XVMUL VT1, VX0, VXS
    XVMSUB VT1, VX2, VXC, VT1

    xvst VT0, X, 4 * SIZE
    xvstelm.d VT1, YY, 0, 0
    add.d YY, YY, INCY
    xvstelm.d VT1, YY, 0, 1
    add.d YY, YY, INCY
    xvstelm.d VT1, YY, 0, 2
    add.d YY, YY, INCY
    xvstelm.d VT1, YY, 0, 3
    add.d YY, YY, INCY
#else
    xvstelm.w VT1, YY, 0, 0
    add.d YY, YY, INCY
    xvstelm.w VT1, YY, 0, 1
    add.d YY, YY, INCY
    xvstelm.w VT1, YY, 0, 2
    add.d YY, YY, INCY
    xvstelm.w VT1, YY, 0, 3
    add.d YY, YY, INCY
    xvstelm.w VT1, YY, 0, 4
    add.d YY, YY, INCY
    xvstelm.w VT1, YY, 0, 5
    add.d YY, YY, INCY
    xvstelm.w VT1, YY, 0, 6
    add.d YY, YY, INCY
    xvstelm.w VT1, YY, 0, 7
    add.d YY, YY, INCY
#endif
    addi.d X, X, 8 * SIZE
    addi.d  I, I, -1
    blt $r0, I, .L121
    b .L997
    .align 3

.L211: // C!=0 S!=0
#ifdef DOUBLE
    xvld VX2, Y, 0 * SIZE
    ld.d  t1, X, 0 * SIZE
    add.d X, X, INCX
    ld.d  t2, X, 0 * SIZE
    add.d X, X, INCX
    ld.d  t3, X, 0 * SIZE
    add.d X, X, INCX
    ld.d  t4, X, 0 * SIZE
    add.d X, X, INCX
    xvinsgr2vr.d VX0, t1, 0
    xvinsgr2vr.d VX0, t2, 1
    xvinsgr2vr.d VX0, t3, 2
    xvinsgr2vr.d VX0, t4, 3
#else
    xvld VX2, Y, 0 * SIZE
    ld.w  t1, X, 0 * SIZE
    add.d X, X, INCX
    ld.w  t2, X, 0 * SIZE
    add.d X, X, INCX
    ld.w t3, X, 0 * SIZE
    add.d X, X, INCX
    ld.w t4, X, 0 * SIZE
    add.d X, X, INCX
    xvinsgr2vr.w VX0, t1, 0
    xvinsgr2vr.w VX0, t2, 1
    xvinsgr2vr.w VX0, t3, 2
    xvinsgr2vr.w VX0, t4, 3
    ld.w  t1, X, 0 * SIZE
    add.d X, X, INCX
    ld.w  t2, X, 0 * SIZE
    add.d X, X, INCX
    ld.w t3, X, 0 * SIZE
    add.d X, X, INCX
    ld.w t4, X, 0 * SIZE
    add.d X, X, INCX
    xvinsgr2vr.w VX0, t1, 4
    xvinsgr2vr.w VX0, t2, 5
    xvinsgr2vr.w VX0, t3, 6
    xvinsgr2vr.w VX0, t4, 7
#endif
    XVMUL VT0, VXC, VX0
    XVFMADD VT0, VX2, VXS, VT0
    XVMUL VT1, VXS, VX0
    XVMSUB VT1, VX2, VXC, VT1
#ifdef DOUBLE
    xvstelm.d VT0, XX, 0, 0
    add.d XX, XX, INCX
    xvstelm.d VT0, XX, 0, 1
    add.d XX, XX, INCX
    xvstelm.d VT0, XX, 0, 2
    add.d XX, XX, INCX
    xvstelm.d VT0, XX, 0, 3
    add.d XX, XX, INCX
    xvst VT1, Y, 0 * SIZE

    xvld VX2, Y, 4 * SIZE
    ld.d t1, X, 0 * SIZE
    add.d X, X, INCX
    ld.d t2, X, 0 * SIZE
    add.d X, X, INCX
    ld.d t3, X, 0 * SIZE
    add.d X, X, INCX
    ld.d t4, X, 0 * SIZE
    add.d X, X, INCX
    xvinsgr2vr.d VX0, t1, 0
    xvinsgr2vr.d VX0, t2, 1
    xvinsgr2vr.d VX0, t3, 2
    xvinsgr2vr.d VX0, t4, 3

    XVMUL VT0, VXC, VX0
    XVFMADD VT0, VX2, VXS, VT0
    XVMUL VT1, VXS, VX0
    XVMSUB VT1, VX2, VXC, VT1
    xvstelm.d VT0, XX, 0, 0
    add.d XX, XX, INCX
    xvstelm.d VT0, XX, 0, 1
    add.d XX, XX, INCX
    xvstelm.d VT0, XX, 0, 2
    add.d XX, XX, INCX
    xvstelm.d VT0, XX, 0, 3
    add.d XX, XX, INCX
    xvst VT1, Y, 4 * SIZE
#else
    xvstelm.w VT0, XX, 0, 0
    add.d XX, XX, INCX
    xvstelm.w VT0, XX, 0, 1
    add.d XX, XX, INCX
    xvstelm.w VT0, XX, 0, 2
    add.d XX, XX, INCX
    xvstelm.w VT0, XX, 0, 3
    add.d XX, XX, INCX
    xvstelm.w VT0, XX, 0, 4
    add.d XX, XX, INCX
    xvstelm.w VT0, XX, 0, 5
    add.d XX, XX, INCX
    xvstelm.w VT0, XX, 0, 6
    add.d XX, XX, INCX
    xvstelm.w VT0, XX, 0, 7
    add.d XX, XX, INCX
    xvst VT1, Y, 0 * SIZE
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
    ld.d  t3, X, 0 * SIZE
    add.d X, X, INCX
    ld.d  t4, X, 0 * SIZE
    add.d X, X, INCX
    xvinsgr2vr.d VX0, t1, 0
    xvinsgr2vr.d VX0, t2, 1
    xvinsgr2vr.d VX0, t3, 2
    xvinsgr2vr.d VX0, t4, 3
    ld.d  t1, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.d  t2, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.d  t3, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.d  t4, Y, 0 * SIZE
    add.d Y, Y, INCY
    xvinsgr2vr.d VX2, t1, 0
    xvinsgr2vr.d VX2, t2, 1
    xvinsgr2vr.d VX2, t3, 2
    xvinsgr2vr.d VX2, t4, 3
#else
    ld.w  t1, X, 0 * SIZE
    add.d X, X, INCX
    ld.w  t2, X, 0 * SIZE
    add.d X, X, INCX
    ld.w  t3, X, 0 * SIZE
    add.d X, X, INCX
    ld.w  t4, X, 0 * SIZE
    add.d X, X, INCX
    xvinsgr2vr.w VX0, t1, 0
    xvinsgr2vr.w VX0, t2, 1
    xvinsgr2vr.w VX0, t3, 2
    xvinsgr2vr.w VX0, t4, 3
    ld.w  t1, X, 0 * SIZE
    add.d X, X, INCX
    ld.w  t2, X, 0 * SIZE
    add.d X, X, INCX
    ld.w  t3, X, 0 * SIZE
    add.d X, X, INCX
    ld.w  t4, X, 0 * SIZE
    add.d X, X, INCX
    xvinsgr2vr.w VX0, t1, 4
    xvinsgr2vr.w VX0, t2, 5
    xvinsgr2vr.w VX0, t3, 6
    xvinsgr2vr.w VX0, t4, 7

    ld.w  t1, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.w  t2, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.w  t3, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.w  t4, Y, 0 * SIZE
    add.d Y, Y, INCY
    xvinsgr2vr.w VX2, t1, 0
    xvinsgr2vr.w VX2, t2, 1
    xvinsgr2vr.w VX2, t3, 2
    xvinsgr2vr.w VX2, t4, 3
    ld.w  t1, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.w  t2, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.w  t3, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.w  t4, Y, 0 * SIZE
    add.d Y, Y, INCY
    xvinsgr2vr.w VX2, t1, 4
    xvinsgr2vr.w VX2, t2, 5
    xvinsgr2vr.w VX2, t3, 6
    xvinsgr2vr.w VX2, t4, 7
#endif
    XVMUL VT0, VX0, VXC
    XVFMADD VT0, VX2, VXS, VT0
    XVMUL VT1, VX0, VXS
    XVMSUB VT1, VX2, VXC, VT1
#ifdef DOUBLE
    xvstelm.d VT0, XX, 0, 0
    add.d XX, XX, INCX
    xvstelm.d VT0, XX, 0, 1
    add.d XX, XX, INCX
    xvstelm.d VT0, XX, 0, 2
    add.d XX, XX, INCX
    xvstelm.d VT0, XX, 0, 3
    add.d XX, XX, INCX
    xvstelm.d VT1, YY, 0, 0
    add.d YY, YY, INCY
    xvstelm.d VT1, YY, 0, 1
    add.d YY, YY, INCY
    xvstelm.d VT1, YY, 0, 2
    add.d YY, YY, INCY
    xvstelm.d VT1, YY, 0, 3
    add.d YY, YY, INCY

    ld.d  t1, X, 0 * SIZE
    add.d X, X, INCX
    ld.d  t2, X, 0 * SIZE
    add.d X, X, INCX
    ld.d  t3, X, 0 * SIZE
    add.d X, X, INCX
    ld.d  t4, X, 0 * SIZE
    add.d X, X, INCX
    xvinsgr2vr.d VX0, t1, 0
    xvinsgr2vr.d VX0, t2, 1
    xvinsgr2vr.d VX0, t3, 2
    xvinsgr2vr.d VX0, t4, 3
    ld.d  t1, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.d  t2, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.d  t3, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.d  t4, Y, 0 * SIZE
    add.d Y, Y, INCY
    xvinsgr2vr.d VX2, t1, 0
    xvinsgr2vr.d VX2, t2, 1
    xvinsgr2vr.d VX2, t3, 2
    xvinsgr2vr.d VX2, t4, 3

    XVMUL VT0, VX0, VXC
    XVFMADD VT0, VX2, VXS, VT0
    XVMUL VT1, VX0, VXS
    XVMSUB VT1, VX2, VXC, VT1
    xvstelm.d VT0, XX, 0, 0
    add.d XX, XX, INCX
    xvstelm.d VT0, XX, 0, 1
    add.d XX, XX, INCX
    xvstelm.d VT0, XX, 0, 2
    add.d XX, XX, INCX
    xvstelm.d VT0, XX, 0, 3
    add.d XX, XX, INCX
    xvstelm.d VT1, YY, 0, 0
    add.d YY, YY, INCY
    xvstelm.d VT1, YY, 0, 1
    add.d YY, YY, INCY
    xvstelm.d VT1, YY, 0, 2
    add.d YY, YY, INCY
    xvstelm.d VT1, YY, 0, 3
    add.d YY, YY, INCY
#else
    xvstelm.w VT0, XX, 0, 0
    add.d XX, XX, INCX
    xvstelm.w VT0, XX, 0, 1
    add.d XX, XX, INCX
    xvstelm.w VT0, XX, 0, 2
    add.d XX, XX, INCX
    xvstelm.w VT0, XX, 0, 3
    add.d XX, XX, INCX
    xvstelm.w VT0, XX, 0, 4
    add.d XX, XX, INCX
    xvstelm.w VT0, XX, 0, 5
    add.d XX, XX, INCX
    xvstelm.w VT0, XX, 0, 6
    add.d XX, XX, INCX
    xvstelm.w VT0, XX, 0, 7
    add.d XX, XX, INCX

    xvstelm.w VT1, YY, 0, 0
    add.d YY, YY, INCY
    xvstelm.w VT1, YY, 0, 1
    add.d YY, YY, INCY
    xvstelm.w VT1, YY, 0, 2
    add.d YY, YY, INCY
    xvstelm.w VT1, YY, 0, 3
    add.d YY, YY, INCY
    xvstelm.w VT1, YY, 0, 4
    add.d YY, YY, INCY
    xvstelm.w VT1, YY, 0, 5
    add.d YY, YY, INCY
    xvstelm.w VT1, YY, 0, 6
    add.d YY, YY, INCY
    xvstelm.w VT1, YY, 0, 7
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
