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
#define I      $r17
#define TEMP   $r18
#define t1     $r14
#define t2     $r15
#define t3     $r16
#define t4     $r19
#define a1     $f12
#define a2     $f13
#define a3     $f14
#define a4     $f15
#define VX0    $xr12
#define VX1    $xr13
#define VX2    $xr14
#define VX3    $xr15

    PROLOGUE
    bge $r0, N, .L999
    li.d TEMP, 1
    slli.d  TEMP, TEMP, ZBASE_SHIFT
    slli.d  INCX, INCX, ZBASE_SHIFT
    slli.d  INCY, INCY, ZBASE_SHIFT
    srai.d I, N, 3
    bne INCX, TEMP, .L20
    bne INCY, TEMP, .L12 // INCX==1 and INCY!=1
    b .L11  // INCX==1 and INCY==1
.L20:
    bne INCY, TEMP, .L22 // INCX!=1 and INCY!=1
    b .L21 // INCX!=1 and INCY==1

.L11:
    bge $r0, I, .L112
    .align 3

.L111:
    xvld VX0, X, 0 * SIZE
#ifdef DOUBLE
    xvld VX1, X, 4 * SIZE
    xvld VX2, X, 8 * SIZE
    xvld VX3, X, 12 * SIZE
    xvst VX0, Y, 0 * SIZE
    xvst VX1, Y, 4 * SIZE
    xvst VX2, Y, 8 * SIZE
    xvst VX3, Y, 12 * SIZE
#else
    xvld VX1, X, 8 * SIZE
    xvst VX0, Y, 0 * SIZE
    xvst VX1, Y, 8 * SIZE
#endif
    addi.d  I, I, -1
    addi.d X, X, 16 * SIZE
    addi.d Y, Y, 16 * SIZE
    blt $r0, I, .L111
    .align 3

.L112:
    andi I, N, 7
    bge $r0, I, .L999
    .align 3

.L113:
    LD  a1, X, 0 * SIZE
    LD  a2, X, 1 * SIZE
    addi.d I, I, -1
    addi.d  X, X, 2 * SIZE
    ST  a1, Y, 0 * SIZE
    ST  a2, Y, 1 * SIZE
    addi.d  Y, Y, 2 * SIZE
    blt $r0, I, .L113
    b .L999
    .align 3

.L12:
    bge $r0, I, .L122
    .align 3

.L121:
    xvld VX0, X, 0 * SIZE
#ifdef DOUBLE
    xvld VX1, X, 4 * SIZE
    xvld VX2, X, 8 * SIZE
    xvld VX3, X, 12 * SIZE
    xvstelm.d VX0, Y, 0 * SIZE, 0
    xvstelm.d VX0, Y, 1 * SIZE, 1
    add.d Y, Y, INCY
    xvstelm.d VX0, Y, 0 * SIZE, 2
    xvstelm.d VX0, Y, 1 * SIZE, 3
    add.d Y, Y, INCY
    xvstelm.d VX1, Y, 0 * SIZE, 0
    xvstelm.d VX1, Y, 1 * SIZE, 1
    add.d Y, Y, INCY
    xvstelm.d VX1, Y, 0 * SIZE, 2
    xvstelm.d VX1, Y, 1 * SIZE, 3
    add.d Y, Y, INCY
    xvstelm.d VX2, Y, 0 * SIZE, 0
    xvstelm.d VX2, Y, 1 * SIZE, 1
    add.d Y, Y, INCY
    xvstelm.d VX2, Y, 0 * SIZE, 2
    xvstelm.d VX2, Y, 1 * SIZE, 3
    add.d Y, Y, INCY
    xvstelm.d VX3, Y, 0 * SIZE, 0
    xvstelm.d VX3, Y, 1 * SIZE, 1
    add.d Y, Y, INCY
    xvstelm.d VX3, Y, 0 * SIZE, 2
    xvstelm.d VX3, Y, 1 * SIZE, 3
#else
    xvld VX1, X, 8 * SIZE
    xvstelm.w VX0, Y, 0 * SIZE, 0
    xvstelm.w VX0, Y, 1 * SIZE, 1
    add.d Y, Y, INCY
    xvstelm.w VX0, Y, 0 * SIZE, 2
    xvstelm.w VX0, Y, 1 * SIZE, 3
    add.d Y, Y, INCY
    xvstelm.w VX0, Y, 0 * SIZE, 4
    xvstelm.w VX0, Y, 1 * SIZE, 5
    add.d Y, Y, INCY
    xvstelm.w VX0, Y, 0 * SIZE, 6
    xvstelm.w VX0, Y, 1 * SIZE, 7
    add.d Y, Y, INCY
    xvstelm.w VX1, Y, 0 * SIZE, 0
    xvstelm.w VX1, Y, 1 * SIZE, 1
    add.d Y, Y, INCY
    xvstelm.w VX1, Y, 0 * SIZE, 2
    xvstelm.w VX1, Y, 1 * SIZE, 3
    add.d Y, Y, INCY
    xvstelm.w VX1, Y, 0 * SIZE, 4
    xvstelm.w VX1, Y, 1 * SIZE, 5
    add.d Y, Y, INCY
    xvstelm.w VX1, Y, 0 * SIZE, 6
    xvstelm.w VX1, Y, 1 * SIZE, 7
#endif
    add.d Y, Y, INCY
    addi.d X, X, 16 * SIZE
    addi.d  I, I, -1
    blt $r0, I, .L121
    .align 3

.L122:
    andi I, N, 7
    bge $r0, I, .L999
    .align 3

.L123:
    LD  a1, X, 0 * SIZE
    LD  a2, X, 1 * SIZE
    addi.d I, I, -1
    addi.d  X, X, 2 * SIZE
    ST  a1, Y, 0 * SIZE
    ST  a2, Y, 1 * SIZE
    add.d  Y, Y, INCY
    blt $r0, I, .L123
    b .L999
    .align 3

.L21:
    bge $r0, I, .L212
    .align 3

.L211:
#ifdef DOUBLE
    ld.d t1, X, 0 * SIZE
    ld.d t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.d t3, X, 0 * SIZE
    ld.d t4, X, 1 * SIZE
    add.d X, X, INCX
    xvinsgr2vr.d VX0, t1, 0
    xvinsgr2vr.d VX0, t2, 1
    xvinsgr2vr.d VX0, t3, 2
    xvinsgr2vr.d VX0, t4, 3
    ld.d t1, X, 0 * SIZE
    ld.d t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.d t3, X, 0 * SIZE
    ld.d t4, X, 1 * SIZE
    add.d X, X, INCX
    xvinsgr2vr.d VX1, t1, 0
    xvinsgr2vr.d VX1, t2, 1
    xvinsgr2vr.d VX1, t3, 2
    xvinsgr2vr.d VX1, t4, 3
    ld.d t1, X, 0 * SIZE
    ld.d t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.d t3, X, 0 * SIZE
    ld.d t4, X, 1 * SIZE
    add.d X, X, INCX
    xvinsgr2vr.d VX2, t1, 0
    xvinsgr2vr.d VX2, t2, 1
    xvinsgr2vr.d VX2, t3, 2
    xvinsgr2vr.d VX2, t4, 3
    ld.d t1, X, 0 * SIZE
    ld.d t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.d t3, X, 0 * SIZE
    ld.d t4, X, 1 * SIZE
    add.d X, X, INCX
    xvinsgr2vr.d VX3, t1, 0
    xvinsgr2vr.d VX3, t2, 1
    xvinsgr2vr.d VX3, t3, 2
    xvinsgr2vr.d VX3, t4, 3
    xvst VX0, Y, 0 * SIZE
    xvst VX1, Y, 4 * SIZE
    xvst VX2, Y, 8 * SIZE
    xvst VX3, Y, 12 * SIZE
#else
    ld.w t1, X, 0 * SIZE
    ld.w t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.w t3, X, 0 * SIZE
    ld.w t4, X, 1 * SIZE
    add.d X, X, INCX
    xvinsgr2vr.w VX0, t1, 0
    xvinsgr2vr.w VX0, t2, 1
    xvinsgr2vr.w VX0, t3, 2
    xvinsgr2vr.w VX0, t4, 3
    ld.w t1, X, 0 * SIZE
    ld.w t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.w t3, X, 0 * SIZE
    ld.w t4, X, 1 * SIZE
    add.d X, X, INCX
    xvinsgr2vr.w VX0, t1, 4
    xvinsgr2vr.w VX0, t2, 5
    xvinsgr2vr.w VX0, t3, 6
    xvinsgr2vr.w VX0, t4, 7
    ld.w t1, X, 0 * SIZE
    ld.w t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.w t3, X, 0 * SIZE
    ld.w t4, X, 1 * SIZE
    add.d X, X, INCX
    xvinsgr2vr.w VX1, t1, 0
    xvinsgr2vr.w VX1, t2, 1
    xvinsgr2vr.w VX1, t3, 2
    xvinsgr2vr.w VX1, t4, 3
    ld.w t1, X, 0 * SIZE
    ld.w t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.w t3, X, 0 * SIZE
    ld.w t4, X, 1 * SIZE
    add.d X, X, INCX
    xvinsgr2vr.w VX1, t1, 4
    xvinsgr2vr.w VX1, t2, 5
    xvinsgr2vr.w VX1, t3, 6
    xvinsgr2vr.w VX1, t4, 7
    xvst VX0, Y, 0 * SIZE
    xvst VX1, Y, 8 * SIZE
#endif
    addi.d  I, I, -1
    addi.d Y, Y, 16 * SIZE
    blt $r0, I, .L211
    .align 3

.L212:
    andi I, N, 7
    bge $r0, I, .L999
    .align 3

.L213:
    LD  a1, X, 0 * SIZE
    LD  a2, X, 1 * SIZE
    addi.d I, I, -1
    ST  a1, Y, 0 * SIZE
    ST  a2, Y, 1 * SIZE
    add.d  X, X, INCX
    addi.d  Y, Y, 2 * SIZE
    blt $r0, I, .L213
    b .L999
    .align 3

.L22:
    bge $r0, I, .L223
    .align 3

.L222:
    LD  a1, X, 0 * SIZE
    LD  a2, X, 1 * SIZE
    add.d X, X, INCX
    LD  a3, X, 0 * SIZE
    LD  a4, X, 1 * SIZE
    add.d X, X, INCX
    ST  a1, Y, 0 * SIZE
    ST  a2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ST  a3, Y, 0 * SIZE
    ST  a4, Y, 1 * SIZE
    add.d Y, Y, INCY

    LD  a1, X, 0 * SIZE
    LD  a2, X, 1 * SIZE
    add.d X, X, INCX
    LD  a3, X, 0 * SIZE
    LD  a4, X, 1 * SIZE
    add.d X, X, INCX
    ST  a1, Y, 0 * SIZE
    ST  a2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ST  a3, Y, 0 * SIZE
    ST  a4, Y, 1 * SIZE
    add.d Y, Y, INCY

    LD  a1, X, 0 * SIZE
    LD  a2, X, 1 * SIZE
    add.d X, X, INCX
    LD  a3, X, 0 * SIZE
    LD  a4, X, 1 * SIZE
    add.d X, X, INCX
    ST  a1, Y, 0 * SIZE
    ST  a2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ST  a3, Y, 0 * SIZE
    ST  a4, Y, 1 * SIZE
    add.d Y, Y, INCY

    LD  a1, X, 0 * SIZE
    LD  a2, X, 1 * SIZE
    add.d X, X, INCX
    LD  a3, X, 0 * SIZE
    LD  a4, X, 1 * SIZE
    add.d X, X, INCX
    ST  a1, Y, 0 * SIZE
    ST  a2, Y, 1 * SIZE
    add.d Y, Y, INCY
    ST  a3, Y, 0 * SIZE
    ST  a4, Y, 1 * SIZE
    add.d Y, Y, INCY
    addi.d  I, I, -1
    blt $r0, I, .L222
    .align 3

.L223:
    andi I, N, 7
    bge $r0, I, .L999
    .align 3

.L224:
    LD  a1, X, 0 * SIZE
    LD  a2, X, 1 * SIZE
    addi.d I, I, -1
    ST  a1, Y, 0 * SIZE
    ST  a2, Y, 1 * SIZE
    add.d  X, X, INCX
    add.d  Y, Y, INCY
    blt $r0, I, .L224
    .align 3

.L999:
    move $r4, $r12
    jirl $r0, $r1, 0x0
    .align 3

    EPILOGUE
