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
#define I      $r17
#define TEMP   $r18
#define t1     $r15
#define t2     $r12
#define t3     $r13
#define t4     $r14
#define VX0    $vr12
#define VX1    $vr13
#define VX2    $vr14
#define VX3    $vr15
#define res1   $vr16
#define res2   $vr17
    PROLOGUE
    vxor.v res1, res1, res1
    vxor.v res2, res2, res2
    bge $r0, N, .L999
    bge $r0, INCX, .L999
    li.d  TEMP, SIZE
    slli.d  INCX, INCX, BASE_SHIFT
    srai.d I, N, 3
    bne INCX, TEMP, .L20
    bge $r0, I, .L13
    .align 3

.L11:
    vld VX0, X, 0
    vld VX1, X, 16
    VFADD res2, VX0, VX1
    VFADD res1, res1, res2
#ifdef DOUBLE
    vld VX0, X, 32
    vld VX1, X, 48
    VFADD res2, VX0, VX1
    VFADD res1, res1, res2
#endif
    addi.d  X, X, 8 * SIZE
    addi.d  I, I, -1
    blt $r0, I, .L11
    .align 3

.L12:
#ifdef DOUBLE
    vreplvei.d VX1, res1, 1
    vfadd.d   res1, res1, VX1
#else
    vreplvei.w VX1, res1, 1
    vreplvei.w VX2, res1, 2
    vreplvei.w VX3, res1, 3
    vfadd.s res1, VX1, res1
    vfadd.s res1, VX2, res1
    vfadd.s res1, VX3, res1
#endif
    .align 3

.L13:
    andi I, N, 7
    bge $r0, I, .L999
    .align 3

.L14:
    LD   $f12, X, 0
    ADD  $f16, $f12, $f16
    addi.d  I, I, -1
    addi.d  X, X, SIZE
    blt $r0, I, .L14
    b .L999
    .align 3

.L20:
    bge $r0, I, .L23
    .align 3

.L21:
#ifdef DOUBLE
    ld.d t1, X, 0
    add.d X, X, INCX
    ld.d t2, X, 0
    add.d X, X, INCX
    vinsgr2vr.d VX0, t1, 0
    vinsgr2vr.d VX0, t2, 1
    ld.d t1, X, 0
    add.d X, X, INCX
    ld.d t2, X, 0
    add.d X, X, INCX
    vinsgr2vr.d VX1, t1, 0
    vinsgr2vr.d VX1, t2, 1
    vfadd.d res2, VX0, VX1
    vfadd.d res1, res1, res2
    ld.d t3, X, 0
    add.d X, X, INCX
    ld.d t4, X, 0
    add.d X, X, INCX
    vinsgr2vr.d VX0, t3, 0
    vinsgr2vr.d VX0, t4, 1
    ld.d t3, X, 0
    add.d X, X, INCX
    ld.d t4, X, 0
    add.d X, X, INCX
    vinsgr2vr.d VX1, t3, 0
    vinsgr2vr.d VX1, t4, 1
    vfadd.d res2, VX0, VX1
    vfadd.d res1, res1, res2
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
    vfadd.s res2, VX0, VX1
    vfadd.s res1, res1, res2
#endif
    addi.d  I, I, -1
    blt $r0, I, .L21
    .align 3

.L22:
#ifdef DOUBLE
    vreplvei.d VX1, res1, 1
    vfadd.d res1, VX1, res1
#else
    vreplvei.w VX1, res1, 1
    vreplvei.w VX2, res1, 2
    vreplvei.w VX3, res1, 3
    vfadd.s res1, VX1, res1
    vfadd.s res1, VX2, res1
    vfadd.s res1, VX3, res1
#endif
    .align 3

.L23:
    andi I, N, 7
    bge $r0, I, .L999
    .align 3

.L24:
    LD  $f12, X, 0
    ADD $f16, $f12, $f16
    addi.d I, I, -1
    add.d  X, X, INCX
    blt  $r0, I, .L24
    .align 3

.L999:
    fmov.s $f0, $f16
    jirl $r0, $r1, 0x0
    .align 3

    EPILOGUE
