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
#define ALPHAR $f0
#define ALPHAI $f1
#define X      $r7
#define INCX   $r8
#define DUMMY2 $r9

#define I      $r12
#define TEMP   $r13
#define t1     $r14
#define t2     $r16
#define t3     $r15
#define t4     $r17
#define XX     $r18
#define a1     $f12
#define a2     $f13
#define a3     $f14
#define a4     $f15
#define s1     $f16
#define s2     $f17
#define s3     $f18
#define s4     $f19
#define VX0    $xr8
#define VX1    $xr20
#define VX2    $xr21
#define VX3    $xr22
#define VXAR   $xr23
#define VXAI   $xr19
#define VXZ    $xr12
#define x1     $xr18
#define x2     $xr17
#define x3     $xr16
#define x4     $xr15

    PROLOGUE

    bge $r0, N, .L999
    bge $r0, INCX, .L999
    ld.d DUMMY2, $sp, 0
    li.d TEMP, 1
    movgr2fr.d a1, $r0
    FFINT    a1, a1
    slli.d  TEMP, TEMP, ZBASE_SHIFT
    slli.d  INCX, INCX, ZBASE_SHIFT
    MTG  t1, ALPHAR
#ifdef DOUBLE
    xvreplgr2vr.d VXAR, t1
    movfr2gr.d t2, ALPHAI
    xvreplgr2vr.d VXAI, t2
    xvxor.v VXZ, VXZ, VXZ
    srai.d I, N, 2
#else
    xvreplgr2vr.w VXAR, t1
    movfr2gr.s t2, ALPHAI
    xvreplgr2vr.w VXAI, t2
    xvxor.v VXZ, VXZ, VXZ
    srai.d I, N, 3
#endif
    bne INCX, TEMP, .L22

///////    INCX == 1   ////////
.L11:
    CMPEQ  $fcc0, ALPHAR, a1
    CMPEQ  $fcc1, ALPHAI, a1
    bge $r0, I, .L19
///////   INCX == 1 && N >= 4   ////////
    bnez  DUMMY2, .L17 // if DUMMY2 == 1, called from c/zscal.

    bceqz $fcc0, .L17

    bceqz $fcc1, .L17

.L15:  //alpha_r == 0.0 && alpha_i == 0.0
    xvst VXZ, X, 0 * SIZE
#ifdef DOUBLE
    xvst VXZ, X, 4 * SIZE
    addi.d X, X, 8 * SIZE
#else
    xvst VXZ, X, 8 * SIZE
    addi.d X, X, 16 * SIZE
#endif
    addi.d  I, I, -1
    blt $r0, I, .L15
    b .L19
    .align 3

.L17:
    xvld VX0, X, 0 * SIZE
#ifdef DOUBLE
    xvld VX1, X, 4 * SIZE
    xvpickev.d x1, VX1, VX0
    xvpickod.d x2, VX1, VX0
    xvfmul.d VX0, VXAI, x2
    xvfmsub.d x3, VXAR, x1, VX0
    xvfmul.d VX1, VXAI, x1
    xvfmadd.d x4, VXAR, x2, VX1
    xvilvl.d VX2, x4 ,x3
    xvilvh.d VX3, x4, x3
    xvst VX2, X, 0 * SIZE
    xvst VX3, X, 4 * SIZE
    addi.d X, X, 8 * SIZE
#else
    xvld VX1, X, 8 * SIZE
    xvpickev.w x1, VX1, VX0
    xvpickod.w x2, VX1, VX0
    xvfmul.s VX0, VXAI, x2
    xvfmsub.s x3, VXAR, x1, VX0
    xvfmul.s VX1, VXAI, x1
    xvfmadd.s x4, VXAR, x2, VX1
    xvilvl.w VX2, x4 ,x3
    xvilvh.w VX3, x4, x3
    xvst VX2, X, 0 * SIZE
    xvst VX3, X, 8 * SIZE
    addi.d X, X, 16 * SIZE
#endif
    addi.d  I, I, -1
    blt $r0, I, .L17
    b .L19

    .align 3

///////  INCX == 1 && N < 8   ///////
.L19:
#ifdef DOUBLE
    andi   I,   N,   3
#else
    andi   I,   N,   7
#endif
    beqz   I,   .L999
    bnez  DUMMY2, .L998 // if DUMMY2 == 1, called from c/zscal.

    bceqz $fcc0, .L998

    bceqz $fcc1, .L998

    b .L995            // alpha_r == 0.0 && alpha_i == 0.0
    .align 3

///////    INCX != 1   ////////
.L22:
    CMPEQ  $fcc0, ALPHAR, a1
    CMPEQ  $fcc1, ALPHAI, a1
    move XX, X
    bge $r0, I, .L29
    bnez  DUMMY2, .L25 // if DUMMY2 == 1, called from c/zscal.
    bceqz $fcc0, .L25

    bceqz $fcc1, .L25

.L27:  //alpha_r == 0.0 && alpha_i == 0.0
#ifdef DOUBLE
    xvstelm.d VXZ, X, 0, 0
    xvstelm.d VXZ, X, 1 * SIZE, 0
    add.d X, X, INCX
    xvstelm.d VXZ, X, 0, 0
    xvstelm.d VXZ, X, 1 * SIZE, 0
    add.d X, X, INCX
    xvstelm.d VXZ, X, 0, 0
    xvstelm.d VXZ, X, 1 * SIZE, 0
    add.d X, X, INCX
    xvstelm.d VXZ, X, 0, 0
    xvstelm.d VXZ, X, 1 * SIZE, 0
#else
    xvstelm.w VXZ, X, 0, 0
    xvstelm.w VXZ, X, 1 * SIZE, 0
    add.d X, X, INCX
    xvstelm.w VXZ, X, 0, 0
    xvstelm.w VXZ, X, 1 * SIZE, 0
    add.d X, X, INCX
    xvstelm.w VXZ, X, 0, 0
    xvstelm.w VXZ, X, 1 * SIZE, 0
    add.d X, X, INCX
    xvstelm.w VXZ, X, 0, 0
    xvstelm.w VXZ, X, 1 * SIZE, 0
    add.d X, X, INCX
    xvstelm.w VXZ, X, 0, 0
    xvstelm.w VXZ, X, 1 * SIZE, 0
    add.d X, X, INCX
    xvstelm.w VXZ, X, 0, 0
    xvstelm.w VXZ, X, 1 * SIZE, 0
    add.d X, X, INCX
    xvstelm.w VXZ, X, 0, 0
    xvstelm.w VXZ, X, 1 * SIZE, 0
    add.d X, X, INCX
    xvstelm.w VXZ, X, 0, 0
    xvstelm.w VXZ, X, 1 * SIZE, 0
#endif
    add.d X, X, INCX
    addi.d  I, I, -1
    blt $r0, I, .L27
    b .L29
    .align 3

.L25:
#ifdef DOUBLE
    ld.d t1, X, 0 * SIZE
    ld.d t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.d t3, X, 0 * SIZE
    ld.d t4, X, 1 * SIZE
    add.d X, X, INCX
    xvinsgr2vr.d x1, t1, 0
    xvinsgr2vr.d x2, t2, 0
    xvinsgr2vr.d x1, t3, 1
    xvinsgr2vr.d x2, t4, 1
    ld.d t1, X, 0 * SIZE
    ld.d t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.d t3, X, 0 * SIZE
    ld.d t4, X, 1 * SIZE
    xvinsgr2vr.d x1, t1, 2
    xvinsgr2vr.d x2, t2, 2
    xvinsgr2vr.d x1, t3, 3
    xvinsgr2vr.d x2, t4, 3
    add.d X, X, INCX
    xvfmul.d VX0, VXAI, x2
    xvfmsub.d x3, VXAR, x1, VX0
    xvfmul.d VX1, VXAI, x1
    xvfmadd.d x4, VXAR, x2, VX1
    addi.d  I, I, -1
    xvstelm.d x3, XX, 0 * SIZE, 0
    xvstelm.d x4, XX, 1 * SIZE, 0
    add.d XX, XX, INCX
    xvstelm.d x3, XX, 0 * SIZE, 1
    xvstelm.d x4, XX, 1 * SIZE, 1
    add.d XX, XX, INCX
    xvstelm.d x3, XX, 0 * SIZE, 2
    xvstelm.d x4, XX, 1 * SIZE, 2
    add.d XX, XX, INCX
    xvstelm.d x3, XX, 0 * SIZE, 3
    xvstelm.d x4, XX, 1 * SIZE, 3
#else
    ld.w t1, X, 0 * SIZE
    ld.w t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.w t3, X, 0 * SIZE
    ld.w t4, X, 1 * SIZE
    add.d X, X, INCX
    xvinsgr2vr.w x1, t1, 0
    xvinsgr2vr.w x2, t2, 0
    xvinsgr2vr.w x1, t3, 1
    xvinsgr2vr.w x2, t4, 1
    ld.w t1, X, 0 * SIZE
    ld.w t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.w t3, X, 0 * SIZE
    ld.w t4, X, 1 * SIZE
    xvinsgr2vr.w x1, t1, 2
    xvinsgr2vr.w x2, t2, 2
    xvinsgr2vr.w x1, t3, 3
    xvinsgr2vr.w x2, t4, 3
    add.d X, X, INCX
    ld.w t1, X, 0 * SIZE
    ld.w t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.w t3, X, 0 * SIZE
    ld.w t4, X, 1 * SIZE
    add.d X, X, INCX
    xvinsgr2vr.w x1, t1, 4
    xvinsgr2vr.w x2, t2, 4
    xvinsgr2vr.w x1, t3, 5
    xvinsgr2vr.w x2, t4, 5
    ld.w t1, X, 0 * SIZE
    ld.w t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.w t3, X, 0 * SIZE
    ld.w t4, X, 1 * SIZE
    xvinsgr2vr.w x1, t1, 6
    xvinsgr2vr.w x2, t2, 6
    xvinsgr2vr.w x1, t3, 7
    xvinsgr2vr.w x2, t4, 7
    add.d X, X, INCX
    xvfmul.s VX0, VXAI, x2
    xvfmsub.s x3, VXAR, x1, VX0
    xvfmul.s VX1, VXAI, x1
    xvfmadd.s x4, VXAR, x2, VX1
    addi.d  I, I, -1
    xvstelm.w x3, XX, 0 * SIZE, 0
    xvstelm.w x4, XX, 1 * SIZE, 0
    add.d XX, XX, INCX
    xvstelm.w x3, XX, 0 * SIZE, 1
    xvstelm.w x4, XX, 1 * SIZE, 1
    add.d XX, XX, INCX
    xvstelm.w x3, XX, 0 * SIZE, 2
    xvstelm.w x4, XX, 1 * SIZE, 2
    add.d XX, XX, INCX
    xvstelm.w x3, XX, 0 * SIZE, 3
    xvstelm.w x4, XX, 1 * SIZE, 3
    add.d XX, XX, INCX
    xvstelm.w x3, XX, 0 * SIZE, 4
    xvstelm.w x4, XX, 1 * SIZE, 4
    add.d XX, XX, INCX
    xvstelm.w x3, XX, 0 * SIZE, 5
    xvstelm.w x4, XX, 1 * SIZE, 5
    add.d XX, XX, INCX
    xvstelm.w x3, XX, 0 * SIZE, 6
    xvstelm.w x4, XX, 1 * SIZE, 6
    add.d XX, XX, INCX
    xvstelm.w x3, XX, 0 * SIZE, 7
    xvstelm.w x4, XX, 1 * SIZE, 7
#endif
    add.d XX, XX, INCX
    blt $r0, I, .L25
    b .L29
    .align 3

///////  INCX != 1 && N < 8   ///////
.L29:
#ifdef DOUBLE
    andi   I,   N,   3
#else
    andi   I,   N,   7
#endif
    beqz   I,   .L999
    bnez  DUMMY2, .L998 // if DUMMY2 == 1, called from c/zscal.

    bceqz $fcc0, .L998

    bceqz $fcc1, .L998

.L995: // alpha_r == 0.0 && alpha_i == 0.0
    ST   a1, X, 0 * SIZE
    ST   a1, X, 1 * SIZE
    addi.d I, I, -1
    add.d  X, X, INCX
    blt $r0, I, .L995
    b .L999
.L998:
    LD   a1, X, 0 * SIZE
    LD   a2, X, 1 * SIZE
    addi.d I, I, -1
    MUL  s1, ALPHAI, a2
    MUL  s2, ALPHAI, a1
    MSUB s1, ALPHAR, a1, s1
    MADD s2, ALPHAR, a2, s2
    ST   s1, X, 0 * SIZE
    ST   s2, X, 1 * SIZE
    add.d X, X, INCX
    blt $r0, I, .L998
    b .L999

.L999:
    move $r4, $r12
    jirl $r0, $r1, 0x0
    .align 3
    EPILOGUE
