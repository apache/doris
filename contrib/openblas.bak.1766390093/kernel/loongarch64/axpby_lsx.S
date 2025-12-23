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
#define ALPHA  $f0
#define X      $r5
#define INCX   $r6
#define BETA   $f1
#define Y      $r7
#define INCY   $r8

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
#define VX0    $vr8
#define VX1    $vr20
#define VX2    $vr21
#define VX3    $vr22
#define VXA    $vr23
#define VXB    $vr9
#define VXZ    $vr19

    PROLOGUE

    bge $r0, N, .L999
    movgr2fr.d a1, $r0
    ffint.s.l a1, a1
    slli.d  INCX, INCX, BASE_SHIFT
    slli.d  INCY, INCY, BASE_SHIFT
    MTG  t1, ALPHA
    MTG  t2, BETA
    MTG  t3, a1
#ifdef DOUBLE
    vreplgr2vr.d VXA, t1
    vreplgr2vr.d VXB, t2
    vreplgr2vr.d VXZ, t3
#else
    vreplgr2vr.w VXA, t1
    vreplgr2vr.w VXB, t2
    vreplgr2vr.w VXZ, t3
#endif
    // If incx == 0 || incy == 0, do one by one
    and TEMP, INCX, INCY
    or  I,    N,    N
    beqz TEMP, .L998

    li.d TEMP, 1
    slli.d  TEMP, TEMP, BASE_SHIFT
    srai.d I, N, 3
    bne INCX, TEMP, .L20
    bne INCY, TEMP, .L12 // INCX==1 and INCY!=1
    b .L11  // INCX==1 and INCY==1
.L20:
    bne INCY, TEMP, .L22 // INCX!=1 and INCY!=1
    b .L21 // INCX!=1 and INCY==1

.L11:
    bge $r0, I, .L997
    CMPEQ $fcc0, ALPHA, a1
    bcnez $fcc0, .L110
    CMPEQ $fcc0, BETA, a1
    bcnez $fcc0, .L112 // ALPHA!=0 BETA==0
    b .L111 // ALPHA!=0 BETA!=0
    .align 3

.L110:
    CMPEQ $fcc0, BETA, a1
    bcnez $fcc0, .L114 // ALPHA==0 BETA==0
    b .L113 // ALPHA==0 BETA!=0
    .align 3

.L111: // ALPHA!=0 BETA!=0
    vld VX0, X, 0 * SIZE
    vld VX2, Y, 0 * SIZE
#ifdef DOUBLE
    vld VX1, X, 2 * SIZE
    vld VX3, Y, 2 * SIZE
    vfmul.d VX0, VX0, VXA
    vfmul.d VX1, VX1, VXA
    vfmadd.d VX2, VX2, VXB, VX0
    vfmadd.d VX3, VX3, VXB, VX1
    vst VX2, Y, 0 * SIZE
    vst VX3, Y, 2 * SIZE
    vld VX0, X, 4 * SIZE
    vld VX2, Y, 4 * SIZE
    vld VX1, X, 6 * SIZE
    vld VX3, Y, 6 * SIZE
    vfmul.d VX0, VX0, VXA
    vfmul.d VX1, VX1, VXA
    vfmadd.d VX2, VX2, VXB, VX0
    vfmadd.d VX3, VX3, VXB, VX1
    vst VX2, Y, 4 * SIZE
    vst VX3, Y, 6 * SIZE
#else
    vld VX1, X, 4 * SIZE
    vld VX3, Y, 4 * SIZE
    vfmul.s VX0, VX0, VXA
    vfmul.s VX1, VX1, VXA
    vfmadd.s VX2, VX2, VXB, VX0
    vfmadd.s VX3, VX3, VXB, VX1
    vst VX2, Y, 0 * SIZE
    vst VX3, Y, 4 * SIZE
#endif
    addi.d X, X, 8 * SIZE
    addi.d Y, Y, 8 * SIZE
    addi.d  I, I, -1
    blt $r0, I, .L111
    b .L997
    .align 3

.L112: // ALPHA!=0 BETA==0
    vld VX0, X, 0 * SIZE
#ifdef DOUBLE
    vld VX1, X, 2 * SIZE
    vfmul.d VX0, VX0, VXA
    vfmul.d VX1, VX1, VXA
    vst VX0, Y, 0 * SIZE
    vst VX1, Y, 2 * SIZE
    vld VX2, X, 4 * SIZE
    vld VX3, X, 6 * SIZE
    vfmul.d VX2, VX2, VXA
    vfmul.d VX3, VX3, VXA
    vst VX2, Y, 4 * SIZE
    vst VX3, Y, 6 * SIZE
#else
    vld VX1, X, 4 * SIZE
    vfmul.s VX0, VX0, VXA
    vfmul.s VX1, VX1, VXA
    vst VX0, Y, 0 * SIZE
    vst VX1, Y, 4 * SIZE
#endif
    addi.d X, X, 8 * SIZE
    addi.d Y, Y, 8 * SIZE
    addi.d  I, I, -1
    blt $r0, I, .L112
    b .L997
    .align 3

.L113: // ALPHA==0 BETA!=0
#ifdef DOUBLE
    vld VX0, Y, 0 * SIZE
    vld VX1, Y, 2 * SIZE
    vfmul.d VX0, VX0, VXB
    vfmul.d VX1, VX1, VXB
    vst VX0, Y, 0 * SIZE
    vst VX1, Y, 2 * SIZE
    vld VX2, Y, 4 * SIZE
    vld VX3, Y, 6 * SIZE
    vfmul.d VX2, VX2, VXB
    vfmul.d VX3, VX3, VXB
    vst VX2, Y, 4 * SIZE
    vst VX3, Y, 6 * SIZE
#else
    vld VX2, Y, 0 * SIZE
    vld VX3, Y, 4 * SIZE
    vfmul.s VX2, VX2, VXB
    vfmul.s VX3, VX3, VXB
    vst VX2, Y, 0 * SIZE
    vst VX3, Y, 4 * SIZE
#endif
    addi.d Y, Y, 8 * SIZE
    addi.d  I, I, -1
    blt $r0, I, .L113
    b .L997
    .align 3

.L114: // ALPHA==0 BETA==0
    vst VXZ, Y, 0 * SIZE
#ifdef DOUBLE
    vst VXZ, Y, 2 * SIZE
    vst VXZ, Y, 4 * SIZE
    vst VXZ, Y, 6 * SIZE
#else
    vst VXZ, Y, 4 * SIZE
#endif
    addi.d Y, Y, 8 * SIZE
    addi.d  I, I, -1
    blt $r0, I, .L114
    b .L997
    .align 3

.L12: // INCX==1 and INCY!=1
    bge $r0, I, .L997
    move YY, Y
    CMPEQ $fcc0, ALPHA, a1
    bcnez $fcc0, .L120
    CMPEQ $fcc0, BETA, a1
    bcnez $fcc0, .L122 // ALPHA!=0 BETA==0
    b .L121 // ALPHA!=0 BETA!=0
    .align 3

.L120:
    CMPEQ $fcc0, BETA, a1
    bcnez $fcc0, .L124 // ALPHA==0 BETA==0
    b .L123 // ALPHA==0 BETA!=0
    .align 3

.L121: // ALPHA!=0 BETA!=0
    vld VX0, X, 0 * SIZE
#ifdef DOUBLE
    ld.d t1, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.d t2, Y, 0 * SIZE
    add.d Y, Y, INCY
    vinsgr2vr.d VX2, t1, 0
    vinsgr2vr.d VX2, t2, 1
    vfmul.d VX0, VX0, VXA
    vld VX1, X, 2 * SIZE
    vfmadd.d VX2, VX2, VXB, VX0
    ld.d t3, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.d t4, Y, 0 * SIZE
    add.d Y, Y, INCY
    vinsgr2vr.d VX3, t3, 0
    vinsgr2vr.d VX3, t4, 1
    vstelm.d VX2, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.d VX2, YY, 0, 1
    add.d YY, YY, INCY
    vfmul.d VX1, VX1, VXA
    vld VX0, X, 4 * SIZE
    vfmadd.d VX3, VX3, VXB, VX1
    ld.d t1, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.d t2, Y, 0 * SIZE
    add.d Y, Y, INCY
    vinsgr2vr.d VX2, t1, 0
    vinsgr2vr.d VX2, t2, 1
    vstelm.d VX3, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.d VX3, YY, 0, 1
    add.d YY, YY, INCY
    vfmul.d VX0, VX0, VXA
    vld VX1, X, 6 * SIZE
    vfmadd.d VX2, VX2, VXB, VX0
    ld.d t3, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.d t4, Y, 0 * SIZE
    add.d Y, Y, INCY
    vinsgr2vr.d VX3, t3, 0
    vinsgr2vr.d VX3, t4, 1
    vstelm.d VX2, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.d VX2, YY, 0, 1
    add.d YY, YY, INCY
    vfmul.d VX1, VX1, VXA
    vfmadd.d VX3, VX3, VXB, VX1
    addi.d  I, I, -1
    vstelm.d VX3, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.d VX3, YY, 0, 1
#else
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
    vfmul.s VX0, VX0, VXA
    vld VX1, X, 4 * SIZE
    vfmadd.s VX2, VX2, VXB, VX0
    ld.w t1, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.w t2, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.w t3, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.w t4, Y, 0 * SIZE
    add.d Y, Y, INCY
    vinsgr2vr.w VX3, t1, 0
    vinsgr2vr.w VX3, t2, 1
    vinsgr2vr.w VX3, t3, 2
    vinsgr2vr.w VX3, t4, 3
    vstelm.w VX2, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.w VX2, YY, 0, 1
    add.d YY, YY, INCY
    vstelm.w VX2, YY, 0, 2
    add.d YY, YY, INCY
    vstelm.w VX2, YY, 0, 3
    add.d YY, YY, INCY
    vfmul.s VX1, VX1, VXA
    vfmadd.s VX3, VX3, VXB, VX1
    addi.d  I, I, -1
    vstelm.w VX3, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.w VX3, YY, 0, 1
    add.d YY, YY, INCY
    vstelm.w VX3, YY, 0, 2
    add.d YY, YY, INCY
    vstelm.w VX3, YY, 0, 3
#endif
    add.d YY, YY, INCY
    addi.d X, X, 8 * SIZE
    blt $r0, I, .L121
    b .L997
    .align 3

.L122: // ALPHA!=0 BETA==0
    vld VX0, X, 0 * SIZE
#ifdef DOUBLE
    vld VX1, X, 2 * SIZE
    vfmul.d VX0, VX0, VXA
    vfmul.d VX1, VX1, VXA
    vstelm.d VX0, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.d VX0, YY, 0, 1
    add.d YY, YY, INCY
    vstelm.d VX1, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.d VX1, YY, 0, 1
    add.d YY, YY, INCY
    vld VX0, X, 4 * SIZE
    vld VX1, X, 6 * SIZE
    vfmul.d VX0, VX0, VXA
    vfmul.d VX1, VX1, VXA
    vstelm.d VX0, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.d VX0, YY, 0, 1
    add.d YY, YY, INCY
    vstelm.d VX1, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.d VX1, YY, 0, 1
#else
    vld VX1, X, 4 * SIZE
    vfmul.s VX0, VX0, VXA
    vfmul.s VX1, VX1, VXA
    vstelm.w VX0, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.w VX0, YY, 0, 1
    add.d YY, YY, INCY
    vstelm.w VX0, YY, 0, 2
    add.d YY, YY, INCY
    vstelm.w VX0, YY, 0, 3
    add.d YY, YY, INCY
    vstelm.w VX1, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.w VX1, YY, 0, 1
    add.d YY, YY, INCY
    vstelm.w VX1, YY, 0, 2
    add.d YY, YY, INCY
    vstelm.w VX1, YY, 0, 3
#endif
    add.d YY, YY, INCY
    addi.d X, X, 8 * SIZE
    addi.d  I, I, -1
    blt $r0, I, .L122
    b .L997
    .align 3

.L123: // ALPHA==0 BETA!=0
#ifdef DOUBLE
    ld.d t1, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.d t2, Y, 0 * SIZE
    vinsgr2vr.d VX2, t1, 0
    vinsgr2vr.d VX2, t2, 1
    add.d Y, Y, INCY
    vfmul.d VX2, VX2, VXB
    ld.d t3, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.d t4, Y, 0 * SIZE
    add.d Y, Y, INCY
    vinsgr2vr.d VX3, t3, 0
    vinsgr2vr.d VX3, t4, 1
    vstelm.d VX2, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.d VX2, YY, 0, 1
    add.d YY, YY, INCY
    vfmul.d VX3, VX3, VXB
    ld.d t1, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.d t2, Y, 0 * SIZE
    add.d Y, Y, INCY
    vinsgr2vr.d VX2, t1, 0
    vinsgr2vr.d VX2, t2, 1
    vstelm.d VX3, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.d VX3, YY, 0, 1
    add.d YY, YY, INCY
    vfmul.d VX2, VX2, VXB
    ld.d t3, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.d t4, Y, 0 * SIZE
    add.d Y, Y, INCY
    vinsgr2vr.d VX3, t3, 0
    vinsgr2vr.d VX3, t4, 1
    vstelm.d VX2, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.d VX2, YY, 0, 1
    add.d YY, YY, INCY
    vfmul.d VX3, VX3, VXB
    addi.d  I, I, -1
    vstelm.d VX3, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.d VX3, YY, 0, 1
#else
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
    vfmul.s VX2, VX2, VXB
    ld.w t1, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.w t2, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.w t3, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.w t4, Y, 0 * SIZE
    add.d Y, Y, INCY
    vinsgr2vr.w VX3, t1, 0
    vinsgr2vr.w VX3, t2, 1
    vinsgr2vr.w VX3, t3, 2
    vinsgr2vr.w VX3, t4, 3
    vstelm.w VX2, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.w VX2, YY, 0, 1
    add.d YY, YY, INCY
    vstelm.w VX2, YY, 0, 2
    add.d YY, YY, INCY
    vstelm.w VX2, YY, 0, 3
    add.d YY, YY, INCY
    vfmul.s VX3, VX3, VXB
    addi.d  I, I, -1
    vstelm.w VX3, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.w VX3, YY, 0, 1
    add.d YY, YY, INCY
    vstelm.w VX3, YY, 0, 2
    add.d YY, YY, INCY
    vstelm.w VX3, YY, 0, 3
#endif
    add.d YY, YY, INCY
    blt $r0, I, .L123
    b .L997
    .align 3

.L124: // ALPHA==0 BETA==0
#ifdef DOUBLE
    vstelm.d VXZ, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.d VXZ, YY, 0, 1
    add.d YY, YY, INCY
    vstelm.d VXZ, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.d VXZ, YY, 0, 1
    add.d YY, YY, INCY
    vstelm.d VXZ, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.d VXZ, YY, 0, 1
    add.d YY, YY, INCY
    vstelm.d VXZ, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.d VXZ, YY, 0, 1
#else
    vstelm.w VXZ, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.w VXZ, YY, 0, 1
    add.d YY, YY, INCY
    vstelm.w VXZ, YY, 0, 2
    add.d YY, YY, INCY
    vstelm.w VXZ, YY, 0, 3
    add.d YY, YY, INCY
    vstelm.w VXZ, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.w VXZ, YY, 0, 1
    add.d YY, YY, INCY
    vstelm.w VXZ, YY, 0, 2
    add.d YY, YY, INCY
    vstelm.w VXZ, YY, 0, 3
#endif
    add.d YY, YY, INCY
    addi.d  I, I, -1
    blt $r0, I, .L124
    b .L997
    .align 3

.L21:// INCX!=1 and INCY==1
    bge $r0, I, .L997
    CMPEQ $fcc0, ALPHA, a1
    bcnez $fcc0, .L210
    CMPEQ $fcc0, BETA, a1
    bcnez $fcc0, .L212 // ALPHA!=0 BETA==0
    b .L211 // ALPHA!=0 BETA!=0
    .align 3

.L210:
    CMPEQ $fcc0, BETA, a1
    bcnez $fcc0, .L214 // ALPHA==0 BETA==0
    b .L213 // ALPHA==0 BETA!=0
    .align 3

.L211: // ALPHA!=0 BETA!=0
    vld VX2, Y, 0 * SIZE
#ifdef DOUBLE
    ld.d t1, X, 0 * SIZE
    add.d X, X, INCX
    ld.d t2, X, 0 * SIZE
    vinsgr2vr.d VX0, t1, 0
    vinsgr2vr.d VX0, t2, 1
    add.d X, X, INCX
    vfmul.d VX0, VXA, VX0
    vld VX3, Y, 2 * SIZE
    vfmadd.d VX2, VX2, VXB, VX0
    ld.d t3, X, 0 * SIZE
    add.d X, X, INCX
    ld.d t4, X, 0 * SIZE
    add.d X, X, INCX
    vinsgr2vr.d VX1, t3, 0
    vinsgr2vr.d VX1, t4, 1
    vst VX2, Y, 0 * SIZE
    vfmul.d VX1, VXA, VX1
    vld VX2, Y, 4 * SIZE
    vfmadd.d VX3, VX3, VXB, VX1
    ld.d t1, X, 0 * SIZE
    add.d X, X, INCX
    ld.d t2, X, 0 * SIZE
    add.d X, X, INCX
    vinsgr2vr.d VX0, t1, 0
    vinsgr2vr.d VX0, t2, 1
    vst VX3, Y, 2 * SIZE
    vfmul.d VX0, VX0, VXA
    vld VX3, Y, 6 * SIZE
    vfmadd.d VX2, VX2, VXB, VX0
    ld.d t3, X, 0 * SIZE
    add.d X, X, INCX
    ld.d t4, X, 0 * SIZE
    add.d X, X, INCX
    vinsgr2vr.d VX1, t3, 0
    vinsgr2vr.d VX1, t4, 1
    vst VX2, Y, 4 * SIZE
    vfmul.d VX1, VX1, VXA
    vfmadd.d VX3, VX3, VXB, VX1
    addi.d  I, I, -1
    vst VX3, Y, 6 * SIZE
#else
    ld.w t1, X, 0 * SIZE
    add.d X, X, INCX
    ld.w t2, X, 0 * SIZE
    add.d X, X, INCX
    ld.w t3, X, 0 * SIZE
    add.d X, X, INCX
    ld.w t4, X, 0 * SIZE
    vinsgr2vr.w VX0, t1, 0
    vinsgr2vr.w VX0, t2, 1
    vinsgr2vr.w VX0, t3, 2
    vinsgr2vr.w VX0, t4, 3
    add.d X, X, INCX
    vfmul.s VX0, VXA, VX0
    vld VX3, Y, 4 * SIZE
    vfmadd.s VX2, VX2, VXB, VX0
    ld.w t1, X, 0 * SIZE
    add.d X, X, INCX
    ld.w t2, X, 0 * SIZE
    add.d X, X, INCX
    ld.w t3, X, 0 * SIZE
    add.d X, X, INCX
    ld.w t4, X, 0 * SIZE
    add.d X, X, INCX
    vinsgr2vr.w VX1, t1, 0
    vinsgr2vr.w VX1, t2, 1
    vinsgr2vr.w VX1, t3, 2
    vinsgr2vr.w VX1, t4, 3
    vst VX2, Y, 0 * SIZE
    vfmul.s VX1, VX1, VXA
    vfmadd.s VX3, VX3, VXB, VX1
    addi.d  I, I, -1
    vst VX3, Y, 4 * SIZE
#endif
    addi.d Y, Y, 8 * SIZE
    blt $r0, I, .L211
    b .L997
    .align 3

.L212: // ALPHA!=0 BETA==0
#ifdef DOUBLE
    ld.d t1, X, 0 * SIZE
    add.d X, X, INCX
    ld.d t2, X, 0 * SIZE
    vinsgr2vr.d VX0, t1, 0
    vinsgr2vr.d VX0, t2, 1
    add.d X, X, INCX
    vfmul.d VX0, VXA, VX0
    ld.d t3, X, 0 * SIZE
    add.d X, X, INCX
    ld.d t4, X, 0 * SIZE
    add.d X, X, INCX
    vinsgr2vr.d VX1, t3, 0
    vinsgr2vr.d VX1, t4, 1
    vst VX0, Y, 0 * SIZE
    vfmul.d VX1, VXA, VX1
    ld.d t1, X, 0 * SIZE
    add.d X, X, INCX
    ld.d t2, X, 0 * SIZE
    add.d X, X, INCX
    vinsgr2vr.d VX0, t1, 0
    vinsgr2vr.d VX0, t2, 1
    vst VX1, Y, 2 * SIZE
    vfmul.d VX0, VX0, VXA
    ld.d t3, X, 0 * SIZE
    add.d X, X, INCX
    ld.d t4, X, 0 * SIZE
    add.d X, X, INCX
    vinsgr2vr.d VX1, t3, 0
    vinsgr2vr.d VX1, t4, 1
    vst VX0, Y, 4 * SIZE
    vfmul.d VX1, VX1, VXA
    addi.d  I, I, -1
    vst VX1, Y, 6 * SIZE
#else
    ld.w t1, X, 0 * SIZE
    add.d X, X, INCX
    ld.w t2, X, 0 * SIZE
    add.d X, X, INCX
    ld.w t3, X, 0 * SIZE
    add.d X, X, INCX
    ld.w t4, X, 0 * SIZE
    vinsgr2vr.w VX0, t1, 0
    vinsgr2vr.w VX0, t2, 1
    vinsgr2vr.w VX0, t3, 2
    vinsgr2vr.w VX0, t4, 3
    add.d X, X, INCX
    vfmul.s VX0, VXA, VX0
    ld.w t1, X, 0 * SIZE
    add.d X, X, INCX
    ld.w t2, X, 0 * SIZE
    add.d X, X, INCX
    ld.w t3, X, 0 * SIZE
    add.d X, X, INCX
    ld.w t4, X, 0 * SIZE
    add.d X, X, INCX
    vinsgr2vr.w VX1, t1, 0
    vinsgr2vr.w VX1, t2, 1
    vinsgr2vr.w VX1, t3, 2
    vinsgr2vr.w VX1, t4, 3
    vst VX0, Y, 0 * SIZE
    vfmul.s VX1, VX1, VXA
    addi.d  I, I, -1
    vst VX1, Y, 4 * SIZE
#endif
    addi.d Y, Y, 8 * SIZE
    blt $r0, I, .L212
    b .L997
    .align 3

.L213: // ALPHA==0 BETA!=0
    vld VX2, Y, 0 * SIZE
#ifdef DOUBLE
    vld VX3, Y, 2 * SIZE
    vfmul.d VX2, VX2, VXB
    vfmul.d VX3, VX3, VXB
    vst VX2, Y, 0 * SIZE
    vst VX3, Y, 2 * SIZE
    vld VX2, Y, 4 * SIZE
    vld VX3, Y, 6 * SIZE
    vfmul.d VX2, VX2, VXB
    vfmul.d VX3, VX3, VXB
    vst VX2, Y, 4 * SIZE
    vst VX3, Y, 6 * SIZE
#else
    vld VX3, Y, 4 * SIZE
    vfmul.s VX2, VX2, VXB
    vfmul.s VX3, VX3, VXB
    vst VX2, Y, 0 * SIZE
    vst VX3, Y, 4 * SIZE
#endif
    addi.d Y, Y, 8 * SIZE
    addi.d  I, I, -1
    blt $r0, I, .L213
    b .L997
    .align 3

.L214: // ALPHA==0 BETA==0
    vst VXZ, Y, 0 * SIZE
#ifdef DOUBLE
    vst VXZ, Y, 2 * SIZE
    vst VXZ, Y, 4 * SIZE
    vst VXZ, Y, 6 * SIZE
#else
    vst VXZ, Y, 4 * SIZE
#endif
    addi.d Y, Y, 8 * SIZE
    addi.d  I, I, -1
    blt $r0, I, .L214
    b .L997
    .align 3

.L22:
    bge $r0, I, .L997
    move YY, Y
    CMPEQ $fcc0, ALPHA, a1
    bcnez $fcc0, .L220
    CMPEQ $fcc0, BETA, a1
    bcnez $fcc0, .L222 // ALPHA!=0 BETA==0
    b .L221 // ALPHA!=0 BETA!=0
    .align 3

.L220:
    CMPEQ $fcc0, BETA, a1
    bcnez $fcc0, .L224 // ALPHA==0 BETA==0
    b .L223 // ALPHA==0 BETA!=0
    .align 3

.L221: // ALPHA!=0 BETA!=0
#ifdef DOUBLE
    ld.d t1, X, 0 * SIZE
    add.d X, X, INCX
    ld.d t2, X, 0 * SIZE
    add.d X, X, INCX
    vinsgr2vr.d VX0, t1, 0
    vinsgr2vr.d VX0, t2, 1
    ld.d t3, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.d t4, Y, 0 * SIZE
    vinsgr2vr.d VX2, t3, 0
    vinsgr2vr.d VX2, t4, 1
    add.d Y, Y, INCY
    vfmul.d VX0, VX0, VXA
    vfmadd.d VX2, VX2, VXB, VX0
    ld.d t1, X, 0 * SIZE
    add.d X, X, INCX
    ld.d t2, X, 0 * SIZE
    add.d X, X, INCX
    vinsgr2vr.d VX1, t1, 0
    vinsgr2vr.d VX1, t2, 1
    vstelm.d VX2, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.d VX2, YY, 0, 1
    add.d YY, YY, INCY
    ld.d t3, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.d t4, Y, 0 * SIZE
    vinsgr2vr.d VX3, t3, 0
    vinsgr2vr.d VX3, t4, 1
    add.d Y, Y, INCY
    vfmul.d VX1, VX1, VXA
    vfmadd.d VX3, VX3, VXB, VX1
    ld.d t1, X, 0 * SIZE
    add.d X, X, INCX
    ld.d t2, X, 0 * SIZE
    add.d X, X, INCX
    vinsgr2vr.d VX0, t1, 0
    vinsgr2vr.d VX0, t2, 1
    vstelm.d VX3, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.d VX3, YY, 0, 1
    add.d YY, YY, INCY
    ld.d t3, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.d t4, Y, 0 * SIZE
    vinsgr2vr.d VX2, t3, 0
    vinsgr2vr.d VX2, t4, 1
    add.d Y, Y, INCY
    vfmul.d VX0, VX0, VXA
    vfmadd.d VX2, VX2, VXB, VX0
    ld.d t3, X, 0 * SIZE
    add.d X, X, INCX
    ld.d t4, X, 0 * SIZE
    add.d X, X, INCX
    vinsgr2vr.d VX1, t3, 0
    vinsgr2vr.d VX1, t4, 1
    vstelm.d VX2, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.d VX2, YY, 0, 1
    add.d YY, YY, INCY
    ld.d t1, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.d t2, Y, 0 * SIZE
    vinsgr2vr.d VX3, t1, 0
    vinsgr2vr.d VX3, t2, 1
    add.d Y, Y, INCY
    vfmul.d VX1, VX1, VXA
    vfmadd.d VX3, VX3, VXB, VX1
    addi.d  I, I, -1
    vstelm.d VX3, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.d VX3, YY, 0, 1
#else
    ld.w t1, X, 0 * SIZE
    add.d X, X, INCX
    ld.w t2, X, 0 * SIZE
    add.d X, X, INCX
    ld.w t3, X, 0 * SIZE
    add.d X, X, INCX
    ld.w t4, X, 0 * SIZE
    add.d X, X, INCX
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
    vfmul.s VX0, VX0, VXA
    ld.w t1, X, 0 * SIZE
    add.d X, X, INCX
    ld.w t2, X, 0 * SIZE
    add.d X, X, INCX
    ld.w t3, X, 0 * SIZE
    add.d X, X, INCX
    ld.w t4, X, 0 * SIZE
    add.d X, X, INCX
    vfmadd.s VX2, VX2, VXB, VX0
    vinsgr2vr.w VX1, t1, 0
    vinsgr2vr.w VX1, t2, 1
    vinsgr2vr.w VX1, t3, 2
    vinsgr2vr.w VX1, t4, 3
    vstelm.w VX2, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.w VX2, YY, 0, 1
    add.d YY, YY, INCY
    vstelm.w VX2, YY, 0, 2
    add.d YY, YY, INCY
    vstelm.w VX2, YY, 0, 3
    add.d YY, YY, INCY
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
    vfmul.s VX1, VX1, VXA
    addi.d  I, I, -1
    vfmadd.s VX3, VX3, VXB, VX1
    vstelm.w VX3, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.w VX3, YY, 0, 1
    add.d YY, YY, INCY
    vstelm.w VX3, YY, 0, 2
    add.d YY, YY, INCY
    vstelm.w VX3, YY, 0, 3
#endif
    add.d YY, YY, INCY
    blt $r0, I, .L221
    b .L997
    .align 3

.L222: // ALPHA!=0 BETA==0
#ifdef DOUBLE
    ld.d t1, X, 0 * SIZE
    add.d X, X, INCX
    ld.d t2, X, 0 * SIZE
    vinsgr2vr.d VX0, t1, 0
    vinsgr2vr.d VX0, t2, 1
    add.d X, X, INCX
    vfmul.d VX0, VX0, VXA
    ld.d t3, X, 0 * SIZE
    add.d X, X, INCX
    ld.d t4, X, 0 * SIZE
    add.d X, X, INCX
    vinsgr2vr.d VX1, t3, 0
    vinsgr2vr.d VX1, t4, 1
    vstelm.d VX0, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.d VX0, YY, 0, 1
    add.d YY, YY, INCY
    vfmul.d VX1, VX1, VXA
    ld.d t1, X, 0 * SIZE
    add.d X, X, INCX
    ld.d t2, X, 0 * SIZE
    add.d X, X, INCX
    vinsgr2vr.d VX0, t1, 0
    vinsgr2vr.d VX0, t2, 1
    vstelm.d VX1, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.d VX1, YY, 0, 1
    add.d YY, YY, INCY
    vfmul.d VX0, VX0, VXA
    ld.d t3, X, 0 * SIZE
    add.d X, X, INCX
    ld.d t4, X, 0 * SIZE
    add.d X, X, INCX
    vinsgr2vr.d VX1, t3, 0
    vinsgr2vr.d VX1, t4, 1
    vstelm.d VX0, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.d VX0, YY, 0, 1
    add.d YY, YY, INCY
    vfmul.d VX1, VX1, VXA
    addi.d  I, I, -1
    vstelm.d VX1, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.d VX1, YY, 0, 1
#else
    ld.w t1, X, 0 * SIZE
    add.d X, X, INCX
    ld.w t2, X, 0 * SIZE
    add.d X, X, INCX
    ld.w t3, X, 0 * SIZE
    add.d X, X, INCX
    ld.w t4, X, 0 * SIZE
    vinsgr2vr.w VX0, t1, 0
    vinsgr2vr.w VX0, t2, 1
    vinsgr2vr.w VX0, t3, 2
    vinsgr2vr.w VX0, t4, 3
    add.d X, X, INCX
    vfmul.s VX0, VX0, VXA
    ld.w t1, X, 0 * SIZE
    add.d X, X, INCX
    ld.w t2, X, 0 * SIZE
    add.d X, X, INCX
    ld.w t3, X, 0 * SIZE
    add.d X, X, INCX
    ld.w t4, X, 0 * SIZE
    add.d X, X, INCX
    vinsgr2vr.w VX1, t1, 0
    vinsgr2vr.w VX1, t2, 1
    vinsgr2vr.w VX1, t3, 2
    vinsgr2vr.w VX1, t4, 3
    vstelm.w VX0, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.w VX0, YY, 0, 1
    add.d YY, YY, INCY
    vstelm.w VX0, YY, 0, 2
    add.d YY, YY, INCY
    vstelm.w VX0, YY, 0, 3
    add.d YY, YY, INCY
    vfmul.s VX1, VX1, VXA
    addi.d  I, I, -1
    vstelm.w VX1, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.w VX1, YY, 0, 1
    add.d YY, YY, INCY
    vstelm.w VX1, YY, 0, 2
    add.d YY, YY, INCY
    vstelm.w VX1, YY, 0, 3
#endif
    add.d YY, YY, INCY
    blt $r0, I, .L222
    move Y, YY
    b .L997
    .align 3

.L223: // ALPHA==0 BETA!=0
#ifdef DOUBLE
    ld.d t1, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.d t2, Y, 0 * SIZE
    vinsgr2vr.d VX2, t1, 0
    vinsgr2vr.d VX2, t2, 1
    add.d Y, Y, INCY
    vfmul.d VX2, VX2, VXB
    ld.d t3, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.d t4, Y, 0 * SIZE
    add.d Y, Y, INCY
    vinsgr2vr.d VX3, t3, 0
    vinsgr2vr.d VX3, t4, 1
    vstelm.d VX2, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.d VX2, YY, 0, 1
    add.d YY, YY, INCY
    vfmul.d VX3, VX3, VXB
    ld.d t1, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.d t2, Y, 0 * SIZE
    add.d Y, Y, INCY
    vinsgr2vr.d VX2, t1, 0
    vinsgr2vr.d VX2, t2, 1
    vstelm.d VX3, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.d VX3, YY, 0, 1
    add.d YY, YY, INCY
    vfmul.d VX2, VX2, VXB
    ld.d t3, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.d t4, Y, 0 * SIZE
    add.d Y, Y, INCY
    vinsgr2vr.d VX3, t3, 0
    vinsgr2vr.d VX3, t4, 1
    vstelm.d VX2, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.d VX2, YY, 0, 1
    add.d YY, YY, INCY
    vfmul.d VX3, VX3, VXB
    addi.d  I, I, -1
    vstelm.d VX3, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.d VX3, YY, 0, 1
#else
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
    vfmul.s VX2, VX2, VXB
    ld.w t1, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.w t2, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.w t3, Y, 0 * SIZE
    add.d Y, Y, INCY
    ld.w t4, Y, 0 * SIZE
    add.d Y, Y, INCY
    vinsgr2vr.w VX3, t1, 0
    vinsgr2vr.w VX3, t2, 1
    vinsgr2vr.w VX3, t3, 2
    vinsgr2vr.w VX3, t4, 3
    vstelm.w VX2, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.w VX2, YY, 0, 1
    add.d YY, YY, INCY
    vstelm.w VX2, YY, 0, 2
    add.d YY, YY, INCY
    vstelm.w VX2, YY, 0, 3
    add.d YY, YY, INCY
    vfmul.s VX3, VX3, VXB
    addi.d  I, I, -1
    vstelm.w VX3, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.w VX3, YY, 0, 1
    add.d YY, YY, INCY
    vstelm.w VX3, YY, 0, 2
    add.d YY, YY, INCY
    vstelm.w VX3, YY, 0, 3
#endif
    add.d YY, YY, INCY
    blt $r0, I, .L223
    b .L997
    .align 3

.L224: // ALPHA==0 BETA==0
#ifdef DOUBLE
    vstelm.d VXZ, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.d VXZ, YY, 0, 1
    add.d YY, YY, INCY
    vstelm.d VXZ, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.d VXZ, YY, 0, 1
    add.d YY, YY, INCY
    vstelm.d VXZ, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.d VXZ, YY, 0, 1
    add.d YY, YY, INCY
    vstelm.d VXZ, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.d VXZ, YY, 0, 1
#else
    vstelm.w VXZ, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.w VXZ, YY, 0, 1
    add.d YY, YY, INCY
    vstelm.w VXZ, YY, 0, 2
    add.d YY, YY, INCY
    vstelm.w VXZ, YY, 0, 3
    add.d YY, YY, INCY
    vstelm.w VXZ, YY, 0, 0
    add.d YY, YY, INCY
    vstelm.w VXZ, YY, 0, 1
    add.d YY, YY, INCY
    vstelm.w VXZ, YY, 0, 2
    add.d YY, YY, INCY
    vstelm.w VXZ, YY, 0, 3
#endif
    add.d YY, YY, INCY
    addi.d  I, I, -1
    blt $r0, I, .L224
    b .L997
    .align 3

.L997:
    andi I, N, 7
    bge $r0, I, .L999
    .align 3

.L998:
    LD   $f12, X, 0 * SIZE
    LD   $f13, Y, 0 * SIZE
    addi.d I, I, -1
    MUL  $f12, $f12, ALPHA
    MADD $f13, $f13, BETA, $f12
    ST   $f13, Y, 0 * SIZE
    add.d  X, X, INCX
    add.d  Y, Y, INCY
    blt $r0, I, .L998
    .align 3

.L999:
    move $r4, $r12
    jirl $r0, $r1, 0x0
    .align 3

    EPILOGUE
