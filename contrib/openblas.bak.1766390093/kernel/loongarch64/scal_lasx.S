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

#define N $r4
#define ALPHA $f0
#define X  $r7
#define INCX $r8
#define I $r12
#define TEMP $r13
#define t1 $r14
#define t2 $r18
#define t3 $r15
#define t4 $r17
#define XX $r16
#define VX0 $xr12
#define VX1 $xr13
#define VT0 $xr14
#define VT1 $xr15
#define VALPHA $xr19
#define a1 $f8
#define a2 $f23

    PROLOGUE

    bge $r0, N, .L999
    bge $r0, INCX, .L999
    li.d TEMP, 1
    ld.d t1, $sp,  0 // Load dummp2
    movgr2fr.d a1, $r0
    FFINT  a1, a1
    movgr2fr.d a2, TEMP
    FFINT  a2, a2
    slli.d TEMP, TEMP, BASE_SHIFT
    slli.d INCX, INCX, BASE_SHIFT
    slli.d t1,   t1,   BASE_SHIFT
    CMPEQ  $fcc0, ALPHA, a1
    bcnez  $fcc0, .L20   //ALPHA==0
    CMPEQ  $fcc0, ALPHA, a2
    bcnez $fcc0, .L999  //ALPHA==1 return
.L1:
    srai.d I, N, 3
    beq INCX, TEMP, .L30    //ALPHA !=0|1 and INCX==1
    MTG  TEMP, ALPHA
#ifdef DOUBLE
    xvreplgr2vr.d VALPHA, TEMP
#else
    xvreplgr2vr.w VALPHA, TEMP
#endif
    move XX, X
    .align 3

.L10:   //ALPHA !=0|1 and INCX!=1
    bge $r0, I, .L32
    .align 3
.L11:
#ifdef DOUBLE
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
    ld.d t1, X, 0 * SIZE
    add.d X, X, INCX
    ld.d t2, X, 0 * SIZE
    add.d X, X, INCX
    xvfmul.d VT0, VX0, VALPHA
    ld.d t3, X, 0 * SIZE
    add.d X, X, INCX
    ld.d t4, X, 0 * SIZE
    add.d X, X, INCX
    xvinsgr2vr.d VX1, t1, 0
    xvinsgr2vr.d VX1, t2, 1
    xvinsgr2vr.d VX1, t3, 2
    xvinsgr2vr.d VX1, t4, 3
    xvstelm.d VT0, XX, 0, 0
    add.d XX, XX, INCX
    xvstelm.d VT0, XX, 0, 1
    add.d XX, XX, INCX
    xvstelm.d VT0, XX, 0, 2
    add.d XX, XX, INCX
    xvstelm.d VT0, XX, 0, 3
    add.d XX, XX, INCX
    xvfmul.d VT1, VX1, VALPHA
    xvstelm.d VT1, XX, 0, 0
    add.d XX, XX, INCX
    xvstelm.d VT1, XX, 0, 1
    add.d XX, XX, INCX
    xvstelm.d VT1, XX, 0, 2
    add.d XX, XX, INCX
    xvstelm.d VT1, XX, 0, 3
#else
    ld.w t1, X, 0 * SIZE
    add.d X, X, INCX
    ld.w t2, X, 0 * SIZE
    add.d X, X, INCX
    ld.w t3, X, 0 * SIZE
    add.d X, X, INCX
    ld.w t4, X, 0 * SIZE
    add.d X, X, INCX
    xvinsgr2vr.w VX0, t1, 0
    xvinsgr2vr.w VX0, t2, 1
    xvinsgr2vr.w VX0, t3, 2
    xvinsgr2vr.w VX0, t4, 3
    ld.w t1, X, 0 * SIZE
    add.d X, X, INCX
    ld.w t2, X, 0 * SIZE
    add.d X, X, INCX
    ld.w t3, X, 0 * SIZE
    add.d X, X, INCX
    ld.w t4, X, 0 * SIZE
    add.d X, X, INCX
    xvinsgr2vr.w VX0, t1, 4
    xvinsgr2vr.w VX0, t2, 5
    xvinsgr2vr.w VX0, t3, 6
    xvinsgr2vr.w VX0, t4, 7
    xvfmul.s VT0, VX0, VALPHA
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
#endif
    add.d XX, XX, INCX
    addi.d  I, I, -1
    blt $r0, I, .L11
    b .L32
    .align 3

.L20:
    beq  t1, TEMP, .L1 // if dummp2 == 1, do not directly copy 0
    srai.d I, N, 3
    beq INCX, TEMP, .L24
    bge $r0, I, .L22
    .align 3

.L21:
    ST a1, X, 0
    add.d X, X, INCX
    ST a1, X, 0
    add.d X, X, INCX
    ST a1, X, 0
    add.d X, X, INCX
    ST a1, X, 0
    add.d X, X, INCX
    ST a1, X, 0
    add.d X, X, INCX
    ST a1, X, 0
    add.d X, X, INCX
    ST a1, X, 0
    add.d X, X, INCX
    ST a1, X, 0
    add.d X, X, INCX
    addi.d  I, I, -1
    blt $r0, I, .L21
    .align 3

.L22:
    andi I, N, 7
    bge $r0, I, .L999
    .align 3

.L23:
    ST  a1, X, 0 * SIZE
    addi.d I, I, -1
    add.d X, X, INCX
    blt $r0, I, .L23
    jirl $r0, $r1, 0
    .align 3

.L24:
    bge $r0, I, .L26 /*N<8 INCX==1*/
    .align 3
.L25:
    xvxor.v VX0, VX0, VX0
    xvst VX0, X, 0 * SIZE
#ifdef DOUBLE
    xvst VX0, X, 4 * SIZE
#endif
    addi.d I, I, -1
    addi.d X, X, 8 * SIZE
    blt $r0, I, .L25
    .align 3

.L26:
    andi I, N, 7
    bge $r0, I, .L999
    .align 3

.L27:
    ST  a1, X, 0 * SIZE
    addi.d I, I, -1
    addi.d X, X, SIZE
    blt $r0, I, .L27
    jirl $r0, $r1, 0
    .align 3

.L30:
    bge $r0, I, .L32/*N<8 INCX==1*/
    MTG TEMP, ALPHA
#ifdef DOUBLE
    xvreplgr2vr.d VALPHA , TEMP
#else
    xvreplgr2vr.w VALPHA , TEMP
#endif
    .align 3

.L31:
    xvld VX0, X, 0 * SIZE
#ifdef DOUBLE
    xvld VX1, X, 4 * SIZE
    xvfmul.d VT0, VX0, VALPHA
    xvfmul.d VT1, VX1, VALPHA
    xvst VT0, X, 0 * SIZE
    xvst VT1, X, 4 * SIZE
#else
    xvfmul.s VT0, VX0, VALPHA
    xvst VT0, X, 0 * SIZE
#endif
    addi.d I, I, -1
    addi.d X, X, 8 * SIZE
    blt $r0, I, .L31
    .align 3

.L32:
    andi I, N, 7
    bge $r0, I, .L999
    .align 3

.L33:
    LD    a1, X, 0 * SIZE
    addi.d I, I, -1
    MUL   a1, ALPHA, a1
    ST    a1, X, 0 * SIZE
    add.d X, X, INCX
    blt $r0, I, .L33
    jirl $r0, $r1, 0
    .align 3

.L999:
    jirl    $r0, $r1, 0x0

    EPILOGUE
