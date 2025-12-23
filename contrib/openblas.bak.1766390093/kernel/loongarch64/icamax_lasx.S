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
#define X $r5
#define INCX $r6
#define I $r12
#define t1 $r13
#define t2 $r15
#define t3 $r18
#define t4 $r16
#define i0 $r17
#define i1 $r14
#define TEMP $r19
#define a0 $f12
#define a1 $f13
#define s1 $f15
#define x1 $xr9
#define x2 $xr10
#define x3 $xr11
#define x4 $xr12
#define VX0 $xr13
#define VX1 $xr14
#define VM0 $xr15
#define VM1 $xr16
#define VINC4 $xr17
#define VINC8 $xr18
#define VI0 $xr20
#define VI1 $xr21
#define VI2 $xr22
#define VI3 $xr8
#define VI4 $xr19
#define VT0 $xr23

    PROLOGUE
    li.d  i0, 0
    bge $r0, N, .L999
    bge $r0, INCX, .L999
    li.d TEMP, 1
    xvxor.v VM0, VM0, VM0
    slli.d TEMP, TEMP, ZBASE_SHIFT
    slli.d INCX, INCX, ZBASE_SHIFT
    xvxor.v VI3, VI3, VI3 // 0
#ifdef DOUBLE
    li.d I, -1
    xvreplgr2vr.d VI4, I
    xvffint.d.l VI4, VI4 // -1
    bne INCX, TEMP, .L20
    addi.d i0, i0, 1
    srai.d I, N, 2
    bge $r0, I, .L21
    slli.d i0, i0, 1 //2
    xvreplgr2vr.d VINC4, i0
    addi.d i0, i0, -3
    xvinsgr2vr.d VI1, i0, 0 //initialize the index value for vectorization
    addi.d i0, i0, 1
    xvinsgr2vr.d VI1, i0, 1
    addi.d i0, i0, 1
    xvinsgr2vr.d VI1, i0, 2
    addi.d i0, i0, 1
    xvinsgr2vr.d VI1, i0, 3
    addi.d i0, i0, -1
    xvinsgr2vr.d VI0, i0, 0
    addi.d i0, i0, 1
    xvinsgr2vr.d VI0, i0, 1
    addi.d i0, i0, 1
    xvinsgr2vr.d VI0, i0, 2
    addi.d i0, i0, 1
    xvinsgr2vr.d VI0, i0, 3
#else
    li.w I, -1
    xvreplgr2vr.w VI4, I
    xvffint.s.w VI4, VI4 // -1
    bne INCX, TEMP, .L20
    addi.w i0, i0, 1
    srai.d I, N, 2
    bge $r0, I, .L21
    slli.w i0, i0, 2 //4
    xvreplgr2vr.w VINC4, i0
    addi.w i0, i0, -7
    xvinsgr2vr.w VI1, i0, 0 //initialize the index value for vectorization
    addi.w i0, i0, 1
    xvinsgr2vr.w VI1, i0, 1
    addi.w i0, i0, 1
    xvinsgr2vr.w VI1, i0, 2
    addi.w i0, i0, 1
    xvinsgr2vr.w VI1, i0, 3
    addi.w i0, i0, 1
    xvinsgr2vr.w VI1, i0, 4
    addi.w i0, i0, 1
    xvinsgr2vr.w VI1, i0, 5
    addi.w i0, i0, 1
    xvinsgr2vr.w VI1, i0, 6
    addi.w i0, i0, 1
    xvinsgr2vr.w VI1, i0, 7
    addi.w i0, i0, -3
    xvinsgr2vr.w VI0, i0, 0
    addi.w i0, i0, 1
    xvinsgr2vr.w VI0, i0, 1
    addi.w i0, i0, 1
    xvinsgr2vr.w VI0, i0, 2
    addi.w i0, i0, 1
    xvinsgr2vr.w VI0, i0, 3
    addi.w i0, i0, 1
    xvinsgr2vr.w VI0, i0, 4
    addi.w i0, i0, 1
    xvinsgr2vr.w VI0, i0, 5
    addi.w i0, i0, 1
    xvinsgr2vr.w VI0, i0, 6
    addi.w i0, i0, 1
    xvinsgr2vr.w VI0, i0, 7
#endif
    .align 3

.L10:
    xvld VX0, X, 0 * SIZE
#ifdef DOUBLE
    xvadd.d VI1, VI1, VINC4
    xvld VX1, X, 2 * SIZE
    addi.d I, I, -1
    xvpickev.d x1, VX1, VX0
    xvpickod.d x2, VX1, VX0
    xvfmul.d x3, VI4, x1
    xvfmul.d x4, VI4, x2
    xvfcmp.clt.d VT0, x1, VI3
    xvfcmp.clt.d VINC8, x2, VI3
    xvbitsel.v x1, x1, x3, VT0
    xvbitsel.v x2, x2, x4, VINC8
    xvfadd.d x1, x1, x2
    xvfmax.d x3, VM0, x1
    xvfcmp.ceq.d VT0, x3, VM0
    xvbitsel.v VM0, x3, VM0, VT0
    xvbitsel.v VI0, VI1, VI0, VT0
    xvld VX0, X, 4 * SIZE
    xvadd.d VI1, VI1, VINC4
    xvld VX1, X, 6 * SIZE
    xvpickev.d x1, VX1, VX0
    xvpickod.d x2, VX1, VX0
    xvfmul.d x3, VI4, x1
    xvfmul.d x4, VI4, x2
#else
    xvadd.w VI1, VI1, VINC4
    xvld VX1, X, 4 * SIZE
    addi.d I, I, -1
    xvpickev.w x1, VX1, VX0
    xvpickod.w x2, VX1, VX0
    xvfmul.s x3, VI4, x1
    xvfmul.s x4, VI4, x2
#endif
    XVCMPLT  VT0, x1, VI3
    XVCMPLT  VINC8, x2, VI3
    xvbitsel.v x1, x1, x3, VT0
    xvbitsel.v x2, x2, x4, VINC8
    XVFADD  x1, x1, x2
    XVFMAX  x3, VM0, x1
    XVCMPEQ VT0, x3, VM0
    addi.d X, X, 8 * SIZE
    xvbitsel.v VM0, x3, VM0, VT0
    xvbitsel.v VI0, VI1, VI0, VT0
    blt $r0, I, .L10
    .align 3

.L15:
#ifdef DOUBLE
    vreplvei.d $vr21, $vr20, 0
    vreplvei.d $vr22, $vr20, 1
    vreplvei.d $vr9,  $vr15, 0
    vreplvei.d $vr10, $vr15, 1
    fcmp.ceq.d $fcc0, $f10, $f9
    bceqz $fcc0, .L26
    xvfcmp.clt.d VT0, VI1, VI2
    xvbitsel.v VI0, VI2, VI1, VT0
    b .L27
#else
    vreplvei.w $vr21, $vr20, 0
    vreplvei.w $vr22, $vr20, 1
    vreplvei.w $vr8,  $vr20, 2
    vreplvei.w $vr19, $vr20, 3
    vreplvei.w $vr9,  $vr15, 0
    vreplvei.w $vr10, $vr15, 1
    vreplvei.w $vr11, $vr15, 2
    vreplvei.w $vr12, $vr15, 3
    xvfmaxa.s VM1, x1, x2
    xvfcmp.ceq.s VT0, VM1, x1
    xvbitsel.v VINC4, VI2, VI1, VT0
    xvfmaxa.s VM0, x3, x4
    xvfcmp.ceq.s VT0, x3, VM0
    xvbitsel.v VINC8, VI4, VI3, VT0
    xvfmaxa.s VM0, VM0, VM1
    xvfcmp.ceq.s VT0, VM0, VM1
    xvbitsel.v VI0, VINC8, VINC4, VT0
    fcmp.ceq.d $fcc0, $f15, $f9
    bceqz $fcc0, .L26
    xvfcmp.clt.s VT0, VI1, VI0
    xvbitsel.v VI0, VI0, VI1, VT0
    b .L26
#endif
    .align 3

.L20: // INCX!=1
#ifdef DOUBLE
    addi.d i0, i0, 1
    srai.d I, N, 2
    bge $r0, I, .L21
    slli.d i0, i0, 1 //2
    xvreplgr2vr.d VINC4, i0
    addi.d i0, i0, -3
    xvinsgr2vr.d VI1, i0, 0 //initialize the index value for vectorization
    addi.d i0, i0, 1
    xvinsgr2vr.d VI1, i0, 1
    addi.d i0, i0, 1
    xvinsgr2vr.d VI1, i0, 2
    addi.d i0, i0, 1
    xvinsgr2vr.d VI1, i0, 3
    addi.d i0, i0, -1
    xvinsgr2vr.d VI0, i0, 0
    addi.d i0, i0, 1
    xvinsgr2vr.d VI0, i0, 1
    addi.d i0, i0, 1
    xvinsgr2vr.d VI0, i0, 2
    addi.d i0, i0, 1
    xvinsgr2vr.d VI0, i0, 3
#else
    addi.w i0, i0, 1
    srai.d I, N, 2
    bge $r0, I, .L21
    slli.w i0, i0, 2 //4
    xvreplgr2vr.w VINC4, i0
    addi.w i0, i0, -7
    xvinsgr2vr.w VI1, i0, 0 //initialize the index value for vectorization
    addi.w i0, i0, 1
    xvinsgr2vr.w VI1, i0, 1
    addi.w i0, i0, 1
    xvinsgr2vr.w VI1, i0, 2
    addi.w i0, i0, 1
    xvinsgr2vr.w VI1, i0, 3
    addi.w i0, i0, 1
    xvinsgr2vr.w VI1, i0, 4
    addi.w i0, i0, 1
    xvinsgr2vr.w VI1, i0, 5
    addi.w i0, i0, 1
    xvinsgr2vr.w VI1, i0, 6
    addi.w i0, i0, 1
    xvinsgr2vr.w VI1, i0, 7
    addi.w i0, i0, -3
    xvinsgr2vr.w VI0, i0, 0
    addi.w i0, i0, 1
    xvinsgr2vr.w VI0, i0, 1
    addi.w i0, i0, 1
    xvinsgr2vr.w VI0, i0, 2
    addi.w i0, i0, 1
    xvinsgr2vr.w VI0, i0, 3
    addi.w i0, i0, 1
    xvinsgr2vr.w VI0, i0, 4
    addi.w i0, i0, 1
    xvinsgr2vr.w VI0, i0, 5
    addi.w i0, i0, 1
    xvinsgr2vr.w VI0, i0, 6
    addi.w i0, i0, 1
    xvinsgr2vr.w VI0, i0, 7
#endif
    .align 3

.L24:
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
    xvadd.d VI1, VI1, VINC4
    xvfmul.d x3, VI4, x1
    xvfmul.d x4, VI4, x2
    xvfcmp.clt.d VT0, x1, VI3
    xvfcmp.clt.d VINC8, x2, VI3
    xvbitsel.v x1, x1, x3, VT0
    xvbitsel.v x2, x2, x4, VINC8
    xvfadd.d x1, x1, x2
    xvfmax.d x3, VM0, x1
    ld.d t1, X, 0 * SIZE
    xvfcmp.ceq.d VT0, x3, VM0
    ld.d t2, X, 1 * SIZE
    xvbitsel.v VM0, x3, VM0, VT0
    xvbitsel.v VI0, VI1, VI0, VT0
    add.d X, X, INCX
    ld.d t3, X, 0 * SIZE
    ld.d t4, X, 1 * SIZE
    add.d X, X, INCX
    xvinsgr2vr.d x1, t1, 0
    xvinsgr2vr.d x2, t2, 0
    xvinsgr2vr.d x1, t3, 1
    xvinsgr2vr.d x2, t4, 1
    xvadd.d VI1, VI1, VINC4
    addi.d I, I, -1
    xvfmul.d x3, VI4, x1
    xvfmul.d x4, VI4, x2
    xvfcmp.clt.d VT0, x1, VI3
    xvfcmp.clt.d VINC8, x2, VI3
    xvbitsel.v x1, x1, x3, VT0
    xvbitsel.v x2, x2, x4, VINC8
    xvfadd.d x1, x1, x2
    xvfmax.d x3, VM0, x1
    xvfcmp.ceq.d VT0, x3, VM0
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
    xvadd.w VI1, VI1, VINC4
    ld.w t1, X, 0 * SIZE
    ld.w t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.w t3, X, 0 * SIZE
    ld.w t4, X, 1 * SIZE
    add.d X, X, INCX
    xvinsgr2vr.w x1, t1, 2
    xvinsgr2vr.w x2, t2, 2
    xvinsgr2vr.w x1, t3, 3
    xvinsgr2vr.w x2, t4, 3
    addi.d I, I, -1
    xvfmul.s x3, VI4, x1
    xvfmul.s x4, VI4, x2
    xvfcmp.clt.s VT0, x1, VI3
    xvfcmp.clt.s VINC8, x2, VI3
    xvbitsel.v x1, x1, x3, VT0
    xvbitsel.v x2, x2, x4, VINC8
    xvfadd.s x1, x1, x2
    xvfmax.s x3, VM0, x1
    xvfcmp.ceq.s VT0, x3, VM0
#endif
    xvbitsel.v VM0, x3, VM0, VT0
    xvbitsel.v VI0, VI1, VI0, VT0
    blt $r0, I, .L24
    .align 3

.L25:
#ifdef DOUBLE
    vreplvei.d $vr21, $vr20, 0
    vreplvei.d $vr22, $vr20, 1
    vreplvei.d $vr9,  $vr15, 0
    vreplvei.d $vr10, $vr15, 1
    fcmp.ceq.d $fcc0, $f10, $f9
    bceqz $fcc0, .L26
    xvfcmp.clt.d VT0, VI1, VI2
    xvbitsel.v VI0, VI2, VI1, VT0
    b .L27
#else
    vreplvei.w $vr21, $vr20, 0
    vreplvei.w $vr22, $vr20, 1
    vreplvei.w $vr8,  $vr20, 2
    vreplvei.w $vr19, $vr20, 3
    vreplvei.w $vr9, $vr15, 0
    vreplvei.w $vr10, $vr15, 1
    vreplvei.w $vr11, $vr15, 2
    vreplvei.w $vr12, $vr15, 3
    xvfmaxa.s VM1, x1, x2
    xvfcmp.ceq.s VT0, VM1, x1
    xvbitsel.v VINC4, VI2, VI1, VT0
    xvfmaxa.s VM0, x3, x4
    xvfcmp.ceq.s VT0, x3, VM0
    xvbitsel.v VINC8, VI4, VI3, VT0
    xvfmaxa.s VM0, VM0, VM1
    xvfcmp.ceq.s VT0, VM0, VM1
    xvbitsel.v VI0, VINC8, VINC4, VT0
    fcmp.ceq.d $fcc0, $f15, $f9
    bceqz $fcc0, .L26
    xvfcmp.clt.s VT0, VI1, VI0
    xvbitsel.v VI0, VI0, VI1, VT0
#endif
    .align 3

#ifdef DOUBLE
.L26:
    xvfmaxa.d VM0, x1, x2
    xvfcmp.ceq.d VT0, x1, VM0
    xvbitsel.v VI0, VI2, VI1, VT0
    .align 3

.L27:
    movfr2gr.d i0, $f20
    .align 3
#else
.L26:
    fcmp.ceq.d $fcc0, $f15, $f10
    bceqz $fcc0, .L27
    xvfcmp.clt.s VT0, VI2, VI0
    xvbitsel.v VI0, VI0, VI2, VT0
    .align 3

.L27:
    fcmp.ceq.d $fcc0, $f15, $f11
    bceqz $fcc0, .L28
    xvfcmp.clt.s VT0, VI3, VI0
    xvbitsel.v VI0, VI0, VI3, VT0
    .align 3

.L28:
    fcmp.ceq.d $fcc0, $f15, $f12
    bceqz $fcc0, .L29
    xvfcmp.clt.s VT0, VI4, VI0
    xvbitsel.v VI0, VI0, VI4, VT0
    .align 3

.L29:
    movfr2gr.s i0, $f20
    .align 3

#endif
.L21: //N<4
    andi I, N, 3
    bge $r0, I, .L999
    srai.d i1, N, 2
    slli.d i1, i1, 2
    addi.d i1, i1, 1 //current index
    movgr2fr.d $f21, i1
    movgr2fr.d $f20, i0
    .align 3

.L22:
    LD a0, X, 0 * SIZE
    LD a1, X, 1 * SIZE
    addi.d I, I, -1
    FABS a0, a0
    FABS a1, a1
    ADD a0, a0, a1
    FMAX a1, s1, a0
    CMPEQ $fcc0, s1, a1
    add.d  X, X, INCX
    fsel s1, a1, s1, $fcc0
    fsel $f20, $f21, $f20, $fcc0
    addi.d i1, i1, 1
    movgr2fr.d $f21, i1
    blt $r0, I, .L22
    MTG  i0, $f20
    .align 3

.L999:
    move $r4, $r17
    jirl $r0, $r1, 0x0
    .align 3

    EPILOGUE
