/*****************************************************************************
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
      derived from this software without specific prior written
      permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
**********************************************************************************/

#define ASSEMBLER

#include "common.h"

#define N      $r4
#define X      $r5
#define INCX   $r6
#define XX     $r19
#define I      $r17
#define TEMP   $r18
#define t1     $r12
#define t2     $r13
#define t3     $r14
#define t4     $r15

/* Don't change following FR unless you know the effects. */
#define VX0    $vr15
#define VX1    $vr16
#define VM0    $vr17
#define VM1    $vr18
#define VM2    $vr13
#define VM3    $vr14
#define res1   $vr19
#define res2   $vr20
#define VALPHA $vr21
#define INF    $f23
#define a1     $f22
#define max    $f17
#define ALPHA  $f12

    PROLOGUE

#ifdef F_INTERFACE
    LDINT   N,     0(N)
    LDINT   INCX,  0(INCX)
#endif

    vxor.v   res1, res1, res1
    vxor.v   res2, res2, res2
    vxor.v VM0, VM0, VM0
    bge $r0, N, .L999
    beq $r0, INCX, .L999
    move  XX, X
    // Init INF
    addi.d  TEMP, $r0,  0x7FF
    slli.d  TEMP, TEMP, 52
    MTC  INF, TEMP
    li.d  TEMP, SIZE
    slli.d INCX, INCX, BASE_SHIFT
    srai.d I, N, 3
    bne INCX, TEMP, .L20

    bge $r0,  I, .L97
    .align 3

.L10:
    vld VX0, X, 0 * SIZE
    vld VX1, X, 2 * SIZE
    vfmaxa.d VM1, VX1, VX0
    vld VX0, X, 4 * SIZE
    vld VX1, X, 6 * SIZE
    vfmaxa.d VM2, VX1, VX0
    vfmaxa.d VM3, VM1, VM2
    vfmaxa.d VM0, VM0, VM3
    addi.d I, I, -1
    addi.d X, X, 8 * SIZE
    blt $r0, I, .L10
    b .L96
    .align 3

.L20: // INCX!=1
    bge $r0, I, .L97
    .align 3

.L21:
    ld.d t1, X, 0 * SIZE
    add.d X, X, INCX
    vinsgr2vr.d VX0, t1, 0
    ld.d t2, X, 0 * SIZE
    add.d X, X, INCX
    vinsgr2vr.d VX0, t2, 1
    ld.d t3, X, 0 * SIZE
    add.d X, X, INCX
    vinsgr2vr.d VX1, t3, 0
    ld.d t4, X, 0 * SIZE
    add.d X, X, INCX
    vinsgr2vr.d VX1, t4, 1
    vfmaxa.d VM1, VX0, VX1
    ld.d t1, X, 0 * SIZE
    add.d X, X, INCX
    vinsgr2vr.d VX0, t1, 0
    ld.d t2, X, 0 * SIZE
    add.d X, X, INCX
    vinsgr2vr.d VX0, t2, 1
    ld.d t3, X, 0 * SIZE
    add.d X, X, INCX
    vinsgr2vr.d VX1, t3, 0
    ld.d t4, X, 0 * SIZE
    add.d X, X, INCX
    vinsgr2vr.d VX1, t4, 1
    vfmaxa.d VM2, VX0, VX1
    vfmaxa.d VM3, VM1, VM2
    vfmaxa.d VM0, VM0, VM3
    addi.d I, I, -1
    blt $r0, I, .L21
    b .L96
    .align 3

.L96:
    vreplvei.d VX0, VM0, 0
    vreplvei.d VX1, VM0, 1
    vfmaxa.d VM0, VX0, VX1
    .align 3

.L97:
    andi I, N, 7
    bge $r0, I, .L99
    .align 3

.L98:
    vld VX1, X, 0
    vfmaxa.d VM0, VM0, VX1
    addi.d I, I, -1
    add.d X, X, INCX
    blt $r0, I, .L98
    .align 3

.L99:
    fabs.d  max, max
    lu12i.w  TEMP, 0x3f800 // 1
    movgr2fr.d  a1, $r0
    movgr2fr.w  ALPHA, TEMP
    CMPEQ  $fcc0, max, a1
    fcvt.d.s  ALPHA, ALPHA
    bcnez  $fcc0, .L999
    fdiv.d  ALPHA, ALPHA, max
    CMPEQ  $fcc0, INF, ALPHA
    bcnez  $fcc0, .L999
    movfr2gr.d TEMP, ALPHA
    vreplgr2vr.d VALPHA, TEMP

.L100:
    li.d TEMP, SIZE
    bne INCX, TEMP, .L120
    srai.d  I, N, 3
    bge $r0, I, .L997
    .align 3

.L110:
    vld VX0, XX, 0 * SIZE
    vld VX1, XX, 2 * SIZE
    vfmul.d VM2, VX0, VALPHA
    vfmul.d VM3, VX1, VALPHA
    vfmadd.d res1, VM2, VM2, res1
    vfmadd.d res2, VM3, VM3, res2
    vld VX0, XX, 4 * SIZE
    vld VX1, XX, 6 * SIZE
    vfmul.d VM2, VX0, VALPHA
    vfmul.d VM3, VX1, VALPHA
    vfmadd.d res1, VM2, VM2, res1
    vfmadd.d res2, VM3, VM3, res2
    addi.d XX, XX, 8 * SIZE
    addi.d I, I, -1
    blt $r0, I, .L110
    b .L996
    .align 3

.L120:
    srai.d I, N, 3
    bge $r0, I, .L997
    .align 3

.L121:
    ld.d t1, XX,  0 * SIZE
    add.d XX, XX, INCX
    ld.d t2, XX, 0 * SIZE
    add.d XX, XX, INCX
    ld.d t3, XX, 0 * SIZE
    add.d XX, XX, INCX
    ld.d t4, XX, 0 * SIZE
    add.d XX, XX, INCX
    vinsgr2vr.d VX0, t1, 0
    vinsgr2vr.d VX0, t2, 1
    vinsgr2vr.d VX1, t3, 0
    vinsgr2vr.d VX1, t4, 1
    vfmul.d VM2, VX0, VALPHA
    ld.d t1, XX, 0 * SIZE
    add.d XX, XX, INCX
    vfmul.d VM3, VX1, VALPHA
    ld.d t2, XX, 0 * SIZE
    add.d XX, XX, INCX
    vfmadd.d res1, VM2, VM2, res1
    vfmadd.d res2, VM3, VM3, res2
    ld.d t3, XX, 0 * SIZE
    add.d XX, XX, INCX
    ld.d t4, XX, 0 * SIZE
    add.d XX, XX, INCX
    vinsgr2vr.d VX0, t1, 0
    vinsgr2vr.d VX0, t2, 1
    vinsgr2vr.d VX1, t3, 0
    vinsgr2vr.d VX1, t4, 1
    vfmul.d VM2, VX0, VALPHA
    vfmul.d VM3, VX1, VALPHA
    vfmadd.d res1, VM2, VM2, res1
    vfmadd.d res2, VM3, VM3, res2
    addi.d  I, I, -1
    blt $r0, I, .L121
    b .L996
    .align 3

.L996:
    vfadd.d res1, res1, res2
    vreplvei.d VX1, res1, 1
    vfadd.d res1, VX1, res1
    .align 3

.L997:
    andi I, N, 7
    bge $r0, I, .L999
    .align 3

.L998:
    fld.d $f15, XX, 0 * SIZE
    addi.d I, I, -1
    fmul.d $f15, $f15, ALPHA
    fmadd.d $f19, $f15, $f15, $f19
    add.d XX, XX , INCX
    blt $r0, I, .L998
    .align 3

.L999:
    fsqrt.d $f19, $f19
    fmul.d $f0, max, $f19
    jirl $r0, $r1, 0x0

    EPILOGUE
