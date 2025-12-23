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
#define X      $r7
#define INCX   $r8
#define Y      $r9
#define INCY   $r10

#define I      $r17
#define TEMP   $r18
#define XX     $r5
#define YY     $r6
#define t1     $r14
#define t2     $r15
#define t3     $r16
#define t4     $r19
#define a1     $f12
#define a2     $f13
#define a3     $f14
#define a4     $f15
#define b1     $f16
#define b2     $f17
#define b3     $f18
#define b4     $f19
#define VX0    $vr12
#define VX1    $vr13
#define VX2    $vr14
#define VX3    $vr15


    PROLOGUE
    bge $r0, N, .L999
    li.d TEMP, 1
    slli.d  TEMP, TEMP, BASE_SHIFT
    slli.d  INCX, INCX, BASE_SHIFT
    slli.d  INCY, INCY, BASE_SHIFT
    srai.d I, N, 3
    bne INCX, TEMP, .L20
    bne INCY, TEMP, .L12 // INCX==1 and INCY!=1
    b .L11  // INCX==1 and INCY==1
.L20:
    bne INCY, TEMP, .L22 // INCX!=1 and INCY!=1
    b .L21 // INCX!=1 and INCY==1

/* INCX==1 and incy==1 */
.L11:
    bge $r0, I, .L112
    .align 3

.L111:
    vld VX0, X, 0
    vld VX1, X, 16
    vld VX2, Y, 0
    vld VX3, Y, 16
    addi.d  I, I, -1
    vst VX2, X, 0
    vst VX3, X, 16
    vst VX0, Y, 0
    vst VX1, Y, 16
#ifdef DOUBLE
    vld VX0, X, 32
    vld VX1, X, 48
    vld VX2, Y, 32
    vld VX3, Y, 48
    vst VX2, X, 32
    vst VX3, X, 48
    vst VX0, Y, 32
    vst VX1, Y, 48
#endif
    addi.d X, X, 8 * SIZE
    addi.d Y, Y, 8 * SIZE
    blt $r0, I, .L111
    .align 3

.L112:
    andi I, N, 7
    bge $r0, I, .L999
    .align 3

.L113:
#ifdef DOUBLE
    fld.d $f12, X, 0
    fld.d $f14, Y, 0
    addi.d I, I, -1
    fst.d $f12, Y, 0
    fst.d $f14, X, 0
#else
    fld.s $f12, X, 0
    fld.s $f14, Y, 0
    addi.d I, I, -1
    fst.s $f12, Y, 0
    fst.s $f14, X, 0
#endif
    addi.d  X, X, SIZE
    addi.d  Y, Y, SIZE
    blt $r0, I, .L113
    b .L999
    .align 3

/* INCX==1 and INCY!=1 */
.L12:
    bge $r0, I, .L122
    .align 3

.L121:
#ifdef DOUBLE
    vld VX0, X, 0
    ld.d t1, Y, 0
    vstelm.d VX0, Y, 0, 0
    add.d Y, Y, INCY
    ld.d t2, Y, 0
    vstelm.d VX0, Y, 0, 1
    vinsgr2vr.d VX2, t1, 0
    vinsgr2vr.d VX2, t2, 1
    add.d Y, Y, INCY
    vst VX2, X, 0
    vld VX1, X, 2 * SIZE
    ld.d t3, Y, 0
    vstelm.d VX1, Y, 0, 0
    add.d Y, Y, INCY
    ld.d t4, Y, 0
    vstelm.d VX1, Y, 0, 1
    vinsgr2vr.d VX3, t3, 0
    vinsgr2vr.d VX3, t4, 1
    add.d Y, Y, INCY
    vst VX3, X, 2 * SIZE
    vld VX0, X, 4 * SIZE
    ld.d t1, Y, 0
    vstelm.d VX0, Y, 0, 0
    add.d Y, Y, INCY
    ld.d t2, Y, 0
    vstelm.d VX0, Y, 0, 1
    vinsgr2vr.d VX2, t1, 0
    vinsgr2vr.d VX2, t2, 1
    add.d Y, Y, INCY
    vst VX2, X, 4 * SIZE
    vld VX1, X, 6 * SIZE
    ld.d t3, Y, 0
    vstelm.d VX1, Y, 0, 0
    add.d Y, Y, INCY
    ld.d t4, Y, 0
    vstelm.d VX1, Y, 0, 1
    vinsgr2vr.d VX3, t3, 0
    vinsgr2vr.d VX3, t4, 1
    add.d Y, Y, INCY
    vst VX3, X, 6 * SIZE
    addi.d X, X, 8 * SIZE
#else
    vld VX0, X, 0
    ld.w t1, Y, 0
    vstelm.w VX0, Y, 0, 0
    add.d Y, Y, INCY
    ld.w t2, Y, 0
    vstelm.w VX0, Y, 0, 1
    add.d Y, Y, INCY
    ld.w t3, Y, 0
    vstelm.w VX0, Y, 0, 2
    add.d Y, Y, INCY
    ld.w t4, Y, 0
    vstelm.w VX0, Y, 0, 3
    vinsgr2vr.w VX2, t1, 0
    vinsgr2vr.w VX2, t2, 1
    vinsgr2vr.w VX2, t3, 2
    vinsgr2vr.w VX2, t4, 3
    add.d Y, Y, INCY
    vst VX2, X, 0

    vld VX1, X, 4 * SIZE
    ld.w t1, Y, 0
    vstelm.w VX1, Y, 0, 0
    add.d Y, Y, INCY
    ld.w t2, Y, 0
    vstelm.w VX1, Y, 0, 1
    add.d Y, Y, INCY
    ld.w t3, Y, 0
    vstelm.w VX1, Y, 0, 2
    add.d Y, Y, INCY
    ld.w t4, Y, 0
    vstelm.w VX1, Y, 0, 3
    vinsgr2vr.w VX3, t1, 0
    vinsgr2vr.w VX3, t2, 1
    vinsgr2vr.w VX3, t3, 2
    vinsgr2vr.w VX3, t4, 3
    add.d Y, Y, INCY
    vst VX3, X, 4 * SIZE
    addi.d X, X, 8 * SIZE
#endif
    addi.d  I, I, -1
    blt $r0, I, .L121
    .align 3

.L122:
    andi I, N, 7
    bge $r0, I, .L999
    .align 3

.L123:
    LD  $f12, X, 0
    LD  $f14, Y, 0
    addi.d I, I, -1
    ST  $f12, Y, 0
    ST  $f14, X, 0
    addi.d X, X, SIZE
    add.d  Y, Y, INCY
    blt  $r0, I, .L123
    b .L999
    .align 3

/* INCX!=1 and INCY==1 */
.L21:
    bge $r0, I, .L212
    .align 3

.L211:
#ifdef DOUBLE
    vld VX2, Y, 0
    ld.d t1, X, 0
    vstelm.d VX2, X, 0, 0
    add.d X, X, INCX
    ld.d t2, X, 0
    vstelm.d VX2, X, 0, 1
    vinsgr2vr.d VX0, t1, 0
    vinsgr2vr.d VX0, t2, 1
    add.d X, X, INCX
    vst VX0, Y, 0
    vld VX3, Y, 2 * SIZE
    ld.d t3, X, 0
    vstelm.d VX3, X, 0, 0
    add.d X, X, INCX
    ld.d t4, X, 0
    vstelm.d VX3, X, 0, 1
    vinsgr2vr.d VX1, t3, 0
    vinsgr2vr.d VX1, t4, 1
    add.d X, X, INCX
    vst VX1, Y, 2 * SIZE
    vld VX2, Y, 4 * SIZE
    ld.d t1, X, 0
    vstelm.d VX2, X, 0, 0
    add.d X, X, INCX
    ld.d t2, X, 0
    vstelm.d VX2, X, 0, 1
    vinsgr2vr.d VX0, t1, 0
    vinsgr2vr.d VX0, t2, 1
    add.d X, X, INCX
    vst VX0, Y, 4 * SIZE
    vld VX3, Y, 6 * SIZE
    ld.d t3, X, 0
    vstelm.d VX3, X, 0, 0
    add.d X, X, INCX
    ld.d t4, X, 0
    vstelm.d VX3, X, 0, 1
    vinsgr2vr.d VX1, t3, 0
    vinsgr2vr.d VX1, t4, 1
    add.d X, X, INCX
    vst  VX1, Y, 6 * SIZE
    addi.d Y, Y, 8 * SIZE
#else
    vld VX2, Y, 0
    ld.w t1, X, 0
    vstelm.w VX2, X, 0, 0
    add.d X, X, INCX
    ld.w t2, X, 0
    vstelm.w VX2, X, 0, 1
    add.d X, X, INCX
    ld.w t3, X, 0
    vstelm.w VX2, X, 0, 2
    add.d X, X, INCX
    ld.w t4, X, 0
    vstelm.w VX2, X, 0, 3
    vinsgr2vr.w VX0, t1, 0
    vinsgr2vr.w VX0, t2, 1
    vinsgr2vr.w VX0, t3, 2
    vinsgr2vr.w VX0, t4, 3
    add.d X, X, INCX
    vst VX0, Y, 0
    vld VX3, Y, 4 * SIZE
    ld.w t1, X, 0
    vstelm.w VX3, X, 0, 0
    add.d X, X, INCX
    ld.w t2, X, 0
    vstelm.w VX3, X, 0, 1
    add.d X, X, INCX
    ld.w t3, X, 0
    vstelm.w VX3, X, 0, 2
    add.d X, X, INCX
    ld.w t4, X, 0
    vstelm.w VX3, X, 0, 3
    vinsgr2vr.w VX1, t1, 0
    vinsgr2vr.w VX1, t2, 1
    vinsgr2vr.w VX1, t3, 2
    vinsgr2vr.w VX1, t4, 3
    add.d X, X, INCX
    vst  VX1, Y, 4 * SIZE
    addi.d Y, Y, 8 * SIZE
#endif
    addi.d  I, I, -1
    blt $r0, I, .L211
    .align 3

.L212:
    andi I, N, 7
    bge $r0, I, .L999
    .align 3

.L213:
    LD  $f12, X, 0 * SIZE
    LD  $f14, Y, 0 * SIZE
    addi.d I, I, -1
    ST  $f12, Y, 0 * SIZE
    ST  $f14, X, 0 * SIZE
    add.d  X, X, INCX
    addi.d Y, Y, SIZE
    blt  $r0, I, .L213
    b .L999
    .align 3

.L22:
    bge $r0, I, .L223
    .align 3
    move XX, X

.L222:
.rept 8
    LD  $f12, X, 0
    LD  $f14, Y, 0
    ST  $f12, Y, 0
    ST  $f14, X, 0
    add.d  X, X, INCX
    add.d  Y, Y, INCY
.endr
    addi.d I, I, -1
    blt $r0, I, .L222
    .align 3

.L223:
    andi I, N, 7
    bge $r0, I, .L999
    .align 3

.L224:
    LD  $f12, X, 0
    LD  $f14, Y, 0
    addi.d I, I, -1
    ST  $f12, Y, 0
    ST  $f14, X, 0
    add.d  X, X, INCX
    add.d  Y, Y, INCY
    blt $r0, I, .L224
    .align 3

.L999:
    move $r4, $r12
    jirl $r0, $r1, 0x0
    .align 3

    EPILOGUE
