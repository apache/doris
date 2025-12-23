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

#define N     $r4
#define X     $r5
#define INCX  $r6

#define I     $r12
#define TEMP  $r13

#define VM0 $vr0
#define VM1 $vr1
#define VM2 $vr2
#define VX0 $vr3
#define VX1 $vr4
#define VX2 $vr5
#define VX3 $vr6

#define t1 $r14
#define t2 $r15
#define t3 $r16
#define t4 $r17

    PROLOGUE

#ifdef F_INTERFACE
    LDINT   N,     0(N)
    LDINT   INCX,  0(INCX)
#endif

    bge $r0, N, .L999
    bge $r0, INCX, .L999
    li.d TEMP, 1
    slli.d TEMP, TEMP, BASE_SHIFT
    slli.d INCX, INCX, BASE_SHIFT
#ifdef DOUBLE
    vldrepl.d VM0, X, 0
#else
    vldrepl.w VM0, X, 0
#endif
    bne INCX, TEMP, .L20

    srai.d I, N, 3
    bge $r0, I, .L11
    .align 3

.L10:
#ifdef DOUBLE
    vld VX0, X, 0
    vld VX1, X, 16
    vld VX2, X, 32
    vld VX3, X, 48
    addi.d I, I, -1
    addi.d X, X, 64
    VFMIN    VM1, VX0, VX1
    VFMIN    VM2, VX2, VX3
    VFMIN    VM0, VM0, VM1
    VFMIN    VM0, VM0, VM2
#else
    vld VX0, X, 0
    vld VX1, X, 16
    addi.d I, I, -1
    addi.d X, X, 32
    VFMIN    VM1, VX0, VX1
    VFMIN    VM0, VM0, VM1
#endif
    blt $r0, I, .L10

#ifdef DOUBLE
    vreplvei.d VX0, VM0, 0
    vreplvei.d VX1, VM0, 1
    VFMIN    VM0, VX0, VX1
#else
    vreplvei.w VX0, VM0, 0
    vreplvei.w VX1, VM0, 1
    vreplvei.w VX2, VM0, 2
    vreplvei.w VX3, VM0, 3
    VFMIN    VM1, VX0, VX1
    VFMIN    VM2, VX2, VX3
    VFMIN    VM0, VM1, VM2
#endif
    .align 3

.L11:
    andi I, N, 7
    bge $r0, I, .L13
    .align 3

.L12:
    LD   $f1, X, 0
    addi.d  I, I, -1
    addi.d  X, X, SIZE
    FMIN  $f0, $f0, $f1
    bnez    I, .L12
    .align 3

.L13:
    jirl $r0,  $r1, 0x0
    .align 3

.L20: // INCX!=1
    srai.d I, N, 3
    bge $r0, I, .L23
    .align 3

.L21:
#ifdef DOUBLE
    ld.d t1, X, 0 * SIZE
    add.d X, X, INCX
    ld.d t2, X, 0 * SIZE
    add.d X, X, INCX
    vinsgr2vr.d VX0, t1, 0
    vinsgr2vr.d VX0, t2, 1
    ld.d t3, X, 0 * SIZE
    add.d X, X, INCX
    ld.d t4, X, 0 * SIZE
    add.d X, X, INCX
    vinsgr2vr.d VX1, t3, 0
    vinsgr2vr.d VX1, t4, 1
    vfmaxa.d VM1, VX0, VX1
    ld.d t1, X, 0 * SIZE
    add.d X, X, INCX
    ld.d t2, X, 0 * SIZE
    add.d X, X, INCX
    vinsgr2vr.d VX0, t1, 0
    vinsgr2vr.d VX0, t2, 1
    ld.d t3, X, 0 * SIZE
    add.d X, X, INCX
    ld.d t4, X, 0 * SIZE
    add.d X, X, INCX
    vinsgr2vr.d VX1, t3, 0
    vinsgr2vr.d VX1, t4, 1
    vfmaxa.d VM2, VX0, VX1
    vfmaxa.d VM1, VM1, VM2
    vfmaxa.d VM0, VM0, VM1
#else
    ld.w t1, X, 0
    add.d X, X, INCX
    ld.w t2, X, 0
    add.d X, X, INCX
    ld.w t3, X, 0
    add.d X, X, INCX
    ld.w t4, X, 0
    add.d X, X, INCX
    vinsgr2vr.w VX0, t1, 0
    vinsgr2vr.w VX0, t2, 1
    vinsgr2vr.w VX0, t3, 2
    vinsgr2vr.w VX0, t4, 3
    ld.w t1, X, 0
    add.d X, X, INCX
    ld.w t2, X, 0
    add.d X, X, INCX
    ld.w t3, X, 0
    add.d X, X, INCX
    ld.w t4, X, 0
    add.d X, X, INCX
    vinsgr2vr.w VX1, t1, 0
    vinsgr2vr.w VX1, t2, 1
    vinsgr2vr.w VX1, t3, 2
    vinsgr2vr.w VX1, t4, 3
    vfmaxa.s VM1, VX0, VX1
    vfmaxa.s VM0, VM0, VM1
#endif
    addi.d I, I, -1
    blt $r0, I, .L21
    .align 3

.L22:
#ifdef DOUBLE
    vreplvei.d VX0, VM0, 0
    vreplvei.d VX1, VM0, 1
    VFMIN    VM0, VX0, VX1
#else
    vreplvei.w VX0, VM0, 0
    vreplvei.w VX1, VM0, 1
    vreplvei.w VX2, VM0, 2
    vreplvei.w VX3, VM0, 3
    VFMIN    VM1, VX0, VX1
    VFMIN    VM2, VX2, VX3
    VFMIN    VM0, VM1, VM2
#endif
    .align 3

.L23: //INCX!=1 and N<8
    andi I, N, 7
    bge $r0, I, .L999
    .align 3

.L24:
    LD   $f1, X, 0
    addi.d  I, I, -1
    add.d   X, X, INCX
    FMIN  $f0, $f0, $f1
    bnez    I, .L24
    .align 3

.L999:
    jirl $r0, $r1, 0x0

    EPILOGUE
