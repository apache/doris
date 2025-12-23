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
#define TEMP $r16
#define t1   $f14
#define t2   $f18
#define t3   $f15
#define t4   $f17
#define s1   $f22
#define s2   $f9
#define s3   $f10
#define s4   $f11
#define a0   $f20
#define a1   $f21
#define x1   $xr9
#define x2   $xr10
#define x3   $xr11
#define x4   $xr12
#define VT0  $xr13
#define VT1  $xr14
#define res0 $xr18
#define neg1 $xr19
#define VX0  $xr20
#define VX1  $xr21
#define VM0  $xr22
#define VM1  $xr23

    PROLOGUE
    MTC s1, $r0
    xvxor.v res0, res0, res0
    bge $r0, N, .L999
    bge $r0, INCX, .L999
    LD a0, X, 0 * SIZE
    LD a1, X, 1 * SIZE
    FABS a0, a0
    FABS a1, a1
    ADD s1, a1, a0
#ifdef DOUBLE
    xvreplve0.d VM0, VM0
#else
    xvreplve0.w VM0, VM0
#endif
    li.d TEMP, 1
    slli.d TEMP, TEMP, ZBASE_SHIFT
    slli.d INCX, INCX, ZBASE_SHIFT
    srai.d I, N, 3
    bne INCX, TEMP, .L20
    bge $r0, I, .L23
    .align 3

.L10:
    xvld VX0, X, 0
    xvld VX1, X, 32
#ifdef DOUBLE
    xvpickev.d x1, VX1, VX0
    xvpickod.d x2, VX1, VX0
#else
    xvpickev.w x1, VX1, VX0
    xvpickod.w x2, VX1, VX0
#endif
    XVFSUB x3, res0, x1
    XVFSUB x4, res0, x2
    XVFMAX x1, x1, x3
    XVFMAX x2, x2, x4
    XVFADD VM1, x1, x2
    XVFMIN VM0, VM0, VM1
#ifdef DOUBLE
    xvld VX0, X, 64
    xvld VX1, X, 96
    xvpickev.d x1, VX1, VX0
    xvpickod.d x2, VX1, VX0
    XVFSUB x3, res0, x1
    XVFSUB x4, res0, x2
    XVFMAX x1, x1, x3
    XVFMAX x2, x2, x4
    XVFADD VM1, x1, x2
    XVFMIN VM0, VM0, VM1
#endif
    addi.d I, I, -1
    addi.d X, X, 16 * SIZE
    blt $r0, I, .L10
    .align 3

.L11:
#ifdef DOUBLE
    xvpickve.d x1, VM0, 0
    xvpickve.d x2, VM0, 1
    XVFMIN VX0, x1, x2
    xvpickve.d x1, VM0, 2
    xvpickve.d x2, VM0, 3
    XVFMIN VX1, x1, x2
    XVFMIN VM0, VX0, VX1
#else
    xvpickve.w x1, VM0, 0
    xvpickve.w x2, VM0, 1
    xvpickve.w x3, VM0, 2
    xvpickve.w x4, VM0, 3
    XVFMIN VX0, x1, x2
    XVFMIN VX1, x3, x4
    XVFMIN VX0, VX0, VX1
    xvpickve.w x1, VM0, 4
    xvpickve.w x2, VM0, 5
    xvpickve.w x3, VM0, 6
    xvpickve.w x4, VM0, 7
    XVFMIN VM0, x1, x2
    XVFMIN VM1, x3, x4
    XVFMIN VM0, VM0, VM1
    XVFMIN VM0, VM0, VX0
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
