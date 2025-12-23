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
#define I      $r17
#define TEMP   $r18
#define t1     $r15
#define t2     $r12
#define t3     $r13
#define t4     $r14
#define a1     $f12
#define a2     $f13
#define a3     $f14
#define a4     $f15
#define s1     $f16
#define VX0    $xr12
#define VX1    $xr13
#define VX2    $xr14
#define VX3    $xr15
#define res1   $xr16
#define res2   $xr17
#define res3   $xr18
#define res0   $xr19
#define neg1   $xr20
#define VT0    $xr21
#define VT1    $xr22

    PROLOGUE
    xvxor.v res1, res1, res1
    xvxor.v res2, res2, res2
    xvxor.v res0, res0, res0
    bge $r0, N, .L999
    bge $r0, INCX, .L999
#ifdef DOUBLE
    li.d t1, -1
    xvreplgr2vr.d neg1, t1
    xvffint.d.l neg1, neg1
#else
    li.w t1, -1
    xvreplgr2vr.w neg1, t1
    xvffint.s.w neg1, neg1
#endif
    li.d  TEMP, 1
    slli.d  TEMP, TEMP, ZBASE_SHIFT
    slli.d  INCX, INCX, ZBASE_SHIFT
    srai.d I, N, 3
    bne INCX, TEMP, .L20
    bge $r0, I, .L13
    .align 3

.L11:
#ifdef DOUBLE
    xvld VX0, X, 0 * SIZE
    xvld VX1, X, 4 * SIZE
    xvfmul.d VX2, neg1, VX0
    xvfmul.d VX3, neg1, VX1
    xvfcmp.clt.d VT0, VX0, res0
    xvfcmp.clt.d VT1, VX1, res0
    xvbitsel.v VX0, VX0, VX2, VT0
    xvbitsel.v VX1, VX1, VX3, VT1
    xvfadd.d res2, VX0, VX1
    xvfadd.d res1, res1, res2
    xvld VX2, X, 8 * SIZE
    xvld VX3, X, 12 * SIZE
    xvfmul.d VX0, neg1, VX2
    xvfmul.d VX1, neg1, VX3
    xvfcmp.clt.d VT0, VX2, res0
    xvfcmp.clt.d VT1, VX3, res0
    xvbitsel.v VX2, VX2, VX0, VT0
    xvbitsel.v VX3, VX3, VX1, VT1
    xvfadd.d res2, VX2, VX3
    xvfadd.d res1, res1, res2
#else
    xvld VX0, X, 0 * SIZE
    xvld VX1, X, 8 * SIZE
    xvfmul.s VX2, neg1, VX0
    xvfmul.s VX3, neg1, VX1
    xvfcmp.clt.s VT0, VX0, res0
    xvfcmp.clt.s VT1, VX1, res0
    xvbitsel.v VX0, VX0, VX2, VT0
    xvbitsel.v VX1, VX1, VX3, VT1
    xvfadd.s res2, VX0, VX1
    xvfadd.s res1, res2, res1
#endif
    addi.d X, X, 16 * SIZE
    addi.d  I, I, -1
    blt $r0, I, .L11
    .align 3

.L12:
#ifdef DOUBLE
    xvpickve.d VX1, res1, 1
    xvpickve.d VX2, res1, 2
    xvpickve.d VX3, res1, 3
    xvfadd.d res1, VX1, res1
    xvfadd.d res1, VX2, res1
    xvfadd.d res1, VX3, res1
#else
    xvfadd.s res2, res1, res2
    xvpickve.w VX1, res1, 1
    xvpickve.w VX2, res1, 2
    xvpickve.w VX3, res1, 3
    xvfadd.s res1, VX1, res1
    xvfadd.s res1, VX2, res1
    xvfadd.s res1, VX3, res1
    xvpickve.w VX0, res2, 4
    xvpickve.w VX1, res2, 5
    xvpickve.w VX2, res2, 6
    xvpickve.w VX3, res2, 7
    xvfadd.s res1, VX0, res1
    xvfadd.s res1, VX1, res1
    xvfadd.s res1, VX2, res1
    xvfadd.s res1, VX2, res1
#endif
    .align 3

.L13:
    andi I, N, 7
    bge $r0, I, .L999
    .align 3

.L14:
    LD   a1, X, 0 * SIZE
    LD   a2, X, 1 * SIZE
    FABS a1, a1
    FABS a2, a2
    addi.d I, I, -1
    ADD  a1, a1, a2
    ADD  s1, a1, s1
    addi.d  X, X, 2 * SIZE
    blt $r0, I, .L14
    b .L999
    .align 3

.L20:
    bge $r0, I, .L23
    .align 3

.L21:
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
    xvfmul.d VX2, neg1, VX0
    xvfmul.d VX3, neg1, VX1
    xvfcmp.clt.d VT0, VX0, res0
    xvfcmp.clt.d VT1, VX1, res0
    xvbitsel.v VX0, VX0, VX2, VT0
    xvbitsel.v VX1, VX1, VX3, VT1
    xvfadd.d res2, VX0, VX1
    xvfadd.d res1, res1, res2
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
    xvfmul.d VX2, neg1, VX0
    xvfmul.d VX3, neg1, VX1
    xvfcmp.clt.d VT0, VX0, res0
    xvfcmp.clt.d VT1, VX1, res0
    xvbitsel.v VX0, VX0, VX2, VT0
    xvbitsel.v VX1, VX1, VX3, VT1
    xvfadd.d res2, VX0, VX1
    xvfadd.d res1, res1, res2
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
    xvfmul.s VX2, neg1, VX0
    xvfmul.s VX3, neg1, VX1
    xvfcmp.clt.s VT0, VX0, res0
    xvfcmp.clt.s VT1, VX1, res0
    xvbitsel.v VX0, VX0, VX2, VT0
    xvbitsel.v VX1, VX1, VX3, VT1
    xvfadd.s res2, VX0, VX1
    xvfadd.s res1, res2, res1
#endif
    addi.d  I, I, -1
    blt $r0, I, .L21
    .align 3

.L22:
#ifdef DOUBLE
    xvpickve.d VX1, res1, 1
    xvpickve.d VX2, res1, 2
    xvpickve.d VX3, res1, 3
    xvfadd.d res1, VX1, res1
    xvfadd.d res1, VX2, res1
    xvfadd.d res1, VX3, res1
#else
    xvfadd.s res2, res1, res2
    xvpickve.w VX1, res1, 1
    xvpickve.w VX2, res1, 2
    xvpickve.w VX3, res1, 3
    xvfadd.s res1, VX1, res1
    xvfadd.s res1, VX2, res1
    xvfadd.s res1, VX3, res1
    xvpickve.w VX0, res2, 4
    xvpickve.w VX1, res2, 5
    xvpickve.w VX2, res2, 6
    xvpickve.w VX3, res2, 7
    xvfadd.s res1, VX0, res1
    xvfadd.s res1, VX1, res1
    xvfadd.s res1, VX2, res1
    xvfadd.s res1, VX2, res1
#endif
    .align 3

.L23:
    andi I, N, 7
    bge $r0, I, .L999
    .align 3

.L24:
    LD   a1, X, 0 * SIZE
    LD   a2, X, 1 * SIZE
    FABS a1, a1
    FABS a2, a2
    addi.d I, I, -1
    ADD  a1, a1, a2
    ADD  s1, a1, s1
    add.d  X, X, INCX
    blt $r0, I, .L24
    .align 3

.L999:
    MOV  $f0, $f16
    jirl $r0, $r1, 0x0
    .align 3

    EPILOGUE
