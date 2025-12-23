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

#define N    $r4
#define X    $r5
#define INCX $r6
#define I    $r12
#define t1   $f14
#define t2   $f18
#define t3   $f15
#define t4   $f17
#define s1   $f22
#define s2   $f9
#define s3   $f10
#define s4   $f11
#define TEMP $r16
#define a0   $f20
#define a1   $f21
#define x1   $vr9
#define x2   $vr10
#define x3   $vr11
#define x4   $vr12
#define VT0  $vr13
#define VT1  $vr14
#define res0 $vr18
#define neg1 $vr19
#define VX0  $vr20
#define VX1  $vr21
#define VM0  $vr22
#define VM1  $vr23

    PROLOGUE
    MTC s1, $r0
    vxor.v res0, res0, res0
    bge $r0, N, .L999
    bge $r0, INCX, .L999
    LD a0, X, 0 * SIZE
    LD a1, X, 1 * SIZE
    FABS a0, a0
    FABS a1, a1
    ADD s1, a1, a0
#ifdef DOUBLE
    vreplvei.d VM0, VM0, 0
#else
    vreplvei.w VM0, VM0, 0
#endif
    li.d TEMP, 1
    slli.d TEMP, TEMP, ZBASE_SHIFT
    slli.d INCX, INCX, ZBASE_SHIFT
    srai.d I, N, 3
    bne INCX, TEMP, .L20
    bge $r0, I, .L23
    .align 3

.L10:
    vld VX0, X, 0
    vld VX1, X, 16
#ifdef DOUBLE
    vpickev.d x1, VX1, VX0
    vpickod.d x2, VX1, VX0
#else
    vpickev.w x1, VX1, VX0
    vpickod.w x2, VX1, VX0
#endif
    VFSUB x3, res0, x1
    VFSUB x4, res0, x2
    VFMAX x1, x1, x3
    VFMAX x2, x2, x4
    VFADD VM1, x1, x2

    vld VX0, X, 32
    vld VX1, X, 48
#ifdef DOUBLE
    vpickev.d x1, VX1, VX0
    vpickod.d x2, VX1, VX0
#else
    vpickev.w x1, VX1, VX0
    vpickod.w x2, VX1, VX0
#endif
    VFSUB x3, res0, x1
    VFSUB x4, res0, x2
    VFMAX x1, x1, x3
    VFMAX x2, x2, x4
    VFADD x1, x1, x2
    VFMIN VM1, x1, VM1
    VFMIN VM0, VM0, VM1
#ifdef DOUBLE
    vld VX0, X, 64
    vld VX1, X, 80
    vpickev.d x1, VX1, VX0
    vpickod.d x2, VX1, VX0
    VFSUB x3, res0, x1
    VFSUB x4, res0, x2
    VFMAX x1, x1, x3
    VFMAX x2, x2, x4
    VFADD VM1, x1, x2

    vld VX0, X, 96
    vld VX1, X, 112
    vpickev.d x1, VX1, VX0
    vpickod.d x2, VX1, VX0
    VFSUB x3, res0, x1
    VFSUB x4, res0, x2
    VFMAX x1, x1, x3
    VFMAX x2, x2, x4
    VFADD x1, x1, x2
    VFMIN VM1, x1, VM1
    VFMIN VM0, VM0, VM1
#endif
    addi.d I, I, -1
    addi.d X, X, 16 * SIZE
    blt $r0, I, .L10
    .align 3

.L11:
#ifdef DOUBLE
    vreplvei.d x1, VM0, 0
    vreplvei.d x2, VM0, 1
    VFMIN VM0, x1, x2
#else
    vreplvei.w x1, VM0, 0
    vreplvei.w x2, VM0, 1
    vreplvei.w x3, VM0, 2
    vreplvei.w x4, VM0, 3
    VFMIN VM1, x1, x2
    VFMIN VM0, x3, x4
    VFMIN VM0, VM0, VM1
#endif
    b .L23
    .align 3

.L20: // INCX!=1
    bge $r0, I, .L23
    .align 3

.L21:
    LD t1, X, 0 * SIZE
    LD t2, X, 1 * SIZE
    add.d X, X, INCX
    LD t3, X, 0 * SIZE
    LD t4, X, 1 * SIZE
    add.d X, X, INCX
    FABS t1, t1
    FABS t2, t2
    FABS t3, t3
    FABS t4, t4
    ADD t1, t1, t2
    ADD t3, t3, t4
    FMIN s1, t1, t3
    LD t1, X, 0 * SIZE
    LD t2, X, 1 * SIZE
    add.d X, X, INCX
    LD t3, X, 0 * SIZE
    LD t4, X, 1 * SIZE
    add.d X, X, INCX
    FABS t1, t1
    FABS t2, t2
    FABS t3, t3
    FABS t4, t4
    ADD t1, t1, t2
    ADD t3, t3, t4
    FMIN s2, t1, t3
    LD t1, X, 0 * SIZE
    LD t2, X, 1 * SIZE
    add.d X, X, INCX
    LD t3, X, 0 * SIZE
    LD t4, X, 1 * SIZE
    add.d X, X, INCX
    FABS t1, t1
    FABS t2, t2
    FABS t3, t3
    FABS t4, t4
    addi.d I, I, -1
    ADD t1, t1, t2
    ADD t3, t3, t4
    FMIN s3, t1, t3
    LD t1, X, 0 * SIZE
    LD t2, X, 1 * SIZE
    add.d X, X, INCX
    LD t3, X, 0 * SIZE
    LD t4, X, 1 * SIZE
    add.d X, X, INCX
    FABS t1, t1
    FABS t2, t2
    FABS t3, t3
    FABS t4, t4
    ADD t1, t1, t2
    ADD t3, t3, t4
    FMIN s4, t1, t3

    FMIN s1, s1, s2
    FMIN s3, s3, s4
    FMIN a0, a0, s3
    FMIN a0, a0, s1
    blt $r0, I, .L21
    .align 3

.L22:
    MOV s1, a0
    .align 3

.L23: //N<8
    andi I, N, 7
    bge $r0, I, .L999
    .align 3

.L24:
    LD a0, X, 0 * SIZE
    LD a1, X, 1 * SIZE
    addi.d I, I, -1
    FABS a0, a0
    FABS a1, a1
    ADD a0, a0, a1
    add.d  X, X, INCX
    FMIN s1, a0, s1
    blt $r0, I, .L24
    .align 3

.L999:
    MOV $f0, $f22
    jirl $r0, $r1, 0x0
    .align 3

    EPILOGUE
