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
#define x1 $vr9
#define x2 $vr10
#define x3 $vr11
#define x4 $vr12
#define VX0 $vr13
#define VX1 $vr14
#define VM0 $vr15
#define VM1 $vr16
#define VINC4 $vr17
#define VINC8 $vr18
#define VI0 $vr20
#define VI1 $vr21
#define VI2 $vr22
#define VI3 $vr8
#define VI4 $vr19
#define VT0 $vr23

    PROLOGUE
    li.d  i0, 0
    bge $r0, N, .L999
    bge $r0, INCX, .L999
    li.d TEMP, 1
    slli.d TEMP, TEMP, ZBASE_SHIFT
    slli.d INCX, INCX, ZBASE_SHIFT
    LD   a0, X, 0 * SIZE
    LD   a1, X, 1 * SIZE
    FABS a0, a0
    FABS a1, a1
    ADD  s1, a1, a0  // Initialization value
    vxor.v VI3, VI3, VI3 // 0
#ifdef DOUBLE
    li.d I, -1
    vreplgr2vr.d VI4, I
    vffint.d.l VI4, VI4 // -1
    bne INCX, TEMP, .L20 // incx != 1

    // Init Index
    addi.d i0, i0, 1
    slli.d i0, i0, 1 // 2
    vreplgr2vr.d VINC4, i0
    addi.d i0, i0, -3
    vinsgr2vr.d VI1, i0, 0 //initialize the index value for vectorization
    addi.d i0, i0, 1
    vinsgr2vr.d VI1, i0, 1
    addi.d i0, i0, 1
    vinsgr2vr.d VI0, i0, 0 //1
    addi.d i0, i0, 1
    vinsgr2vr.d VI0, i0, 1 //2

    srai.d I, N, 2
    bge $r0, I, .L21

    // Init VM0
    vld VX0, X, 0 * SIZE
    vld VX1, X, 2 * SIZE
    vpickev.d x1, VX1, VX0
    vpickod.d x2, VX1, VX0
    vfmul.d x3, VI4, x1
    vfmul.d x4, VI4, x2
    vfcmp.clt.d VT0, x1, VI3
    vfcmp.clt.d VINC8, x2, VI3
    vbitsel.v x1, x1, x3, VT0
    vbitsel.v x2, x2, x4, VINC8
    vfadd.d  VM0, x1, x2
#else
    li.w I, -1
    vreplgr2vr.w VI4, I
    vffint.s.w VI4, VI4 // -1
    bne INCX, TEMP, .L20 // incx != 1

    // Init Index
    addi.w i0, i0, 1
    slli.w i0, i0, 2 //4
    vreplgr2vr.w VINC4, i0
    addi.w i0, i0, -7
    vinsgr2vr.w VI1, i0, 0 //initialize the index value for vectorization
    addi.w i0, i0, 1
    vinsgr2vr.w VI1, i0, 1
    addi.w i0, i0, 1
    vinsgr2vr.w VI1, i0, 2
    addi.w i0, i0, 1
    vinsgr2vr.w VI1, i0, 3
    addi.w i0, i0, 1
    vinsgr2vr.w VI0, i0, 0 //1
    addi.w i0, i0, 1
    vinsgr2vr.w VI0, i0, 1 //2
    addi.w i0, i0, 1
    vinsgr2vr.w VI0, i0, 2 //3
    addi.w i0, i0, 1
    vinsgr2vr.w VI0, i0, 3 //4

    srai.d I, N, 2
    bge $r0, I, .L21

    // Init VM0
    vld VX0, X, 0 * SIZE
    vld VX1, X, 4 * SIZE
    vpickev.w x1, VX1, VX0
    vpickod.w x2, VX1, VX0
    vfmul.s x3, VI4, x1
    vfmul.s x4, VI4, x2
    vfcmp.clt.s VT0, x1, VI3
    vfcmp.clt.s VINC8, x2, VI3
    vbitsel.v x1, x1, x3, VT0
    vbitsel.v x2, x2, x4, VINC8
    vfadd.s  VM0, x1, x2
#endif
    .align 3

.L10:
    vld VX0, X, 0 * SIZE
#ifdef DOUBLE
    vadd.d VI1, VI1, VINC4
    vld VX1, X, 2 * SIZE
    addi.d I, I, -1
    vpickev.d x1, VX1, VX0
    vpickod.d x2, VX1, VX0
    vfmul.d x3, VI4, x1
    vfmul.d x4, VI4, x2
    vfcmp.clt.d VT0, x1, VI3
    vfcmp.clt.d VINC8, x2, VI3
    vbitsel.v x1, x1, x3, VT0
    vbitsel.v x2, x2, x4, VINC8
    vfadd.d x1, x1, x2
    vfmin.d x3, VM0, x1
    vfcmp.ceq.d VT0, x3, VM0
    vbitsel.v VM0, x3, VM0, VT0
    vbitsel.v VI0, VI1, VI0, VT0

    vld VX0, X, 4 * SIZE
    vadd.d VI1, VI1, VINC4
    vld VX1, X, 6 * SIZE
    vpickev.d x1, VX1, VX0
    vpickod.d x2, VX1, VX0
#else
    vadd.w VI1, VI1, VINC4
    vld VX1, X, 4 * SIZE
    addi.d I, I, -1
    vpickev.w x1, VX1, VX0
    vpickod.w x2, VX1, VX0
#endif
    VFMUL  x3, VI4, x1
    VFMUL  x4, VI4, x2
    VCMPLT VT0, x1, VI3
    VCMPLT VINC8, x2, VI3
    vbitsel.v x1, x1, x3, VT0
    vbitsel.v x2, x2, x4, VINC8
    VFADD  x1, x1, x2
    VFMIN  x3, VM0, x1
    VCMPEQ VT0, x3, VM0
    addi.d X, X, 8 * SIZE
    vbitsel.v VM0, x3, VM0, VT0
    vbitsel.v VI0, VI1, VI0, VT0
    blt $r0, I, .L10
    .align 3

.L15:
#ifdef DOUBLE
    vreplvei.d VI1, VI0, 0
    vreplvei.d VI2, VI0, 1
    vreplvei.d x1, VM0, 0
    vreplvei.d x2, VM0, 1
    fcmp.ceq.d $fcc0, $f10, $f9
    bceqz $fcc0, .L26
    vfcmp.clt.d VT0, VI1, VI2
    vbitsel.v VI0, VI2, VI1, VT0
    b .L27
#else
    vreplvei.w VI1, VI0, 0
    vreplvei.w VI2, VI0, 1
    vreplvei.w VI3, VI0, 2
    vreplvei.w VI4, VI0, 3
    vreplvei.w x1, VM0, 0
    vreplvei.w x2, VM0, 1
    vreplvei.w x3, VM0, 2
    vreplvei.w x4, VM0, 3
    vfmina.s VM1, x1, x2
    vfcmp.ceq.s VT0, VM1, x1
    vbitsel.v VINC4, VI2, VI1, VT0
    vfmina.s VM0, x3, x4
    vfcmp.ceq.s VT0, x3, VM0
    vbitsel.v VINC8, VI4, VI3, VT0
    vfmina.s VM0, VM0, VM1
    vfcmp.ceq.s VT0, VM0, VM1
    vbitsel.v VI0, VINC8, VINC4, VT0
    fcmp.ceq.d $fcc0, $f15, $f9
    bceqz $fcc0, .L26
    vfcmp.clt.s VT0, VI1, VI0
    vbitsel.v VI0, VI0, VI1, VT0
    b .L26
#endif
    .align 3

.L20: // INCX!=1
#ifdef DOUBLE
    addi.d i0, i0, 1
    // Init index
    slli.d i0, i0, 1   //2
    vreplgr2vr.d VINC4, i0
    addi.d i0, i0, -3
    vinsgr2vr.d VI1, i0, 0 //initialize the index value for vectorization
    addi.d i0, i0, 1
    vinsgr2vr.d VI1, i0, 1
    addi.d i0, i0, 1
    vinsgr2vr.d VI0, i0, 0 //1
    addi.d i0, i0, 1
    vinsgr2vr.d VI0, i0, 1 //2

    srai.d I, N, 2
    bge $r0, I, .L21 // N < 4

    // Init VM0
    ld.d t1, X, 0 * SIZE
    ld.d t2, X, 1 * SIZE
    add.d i1, X, INCX
    ld.d t3, i1, 0 * SIZE
    ld.d t4, i1, 1 * SIZE
    add.d i1, i1, INCX
    vinsgr2vr.d x1, t1, 0
    vinsgr2vr.d x2, t2, 0
    vinsgr2vr.d x1, t3, 1
    vinsgr2vr.d x2, t4, 1
    vfmul.d x3, VI4, x1
    vfmul.d x4, VI4, x2
    vfcmp.clt.d VT0, x1, VI3
    vfcmp.clt.d VINC8, x2, VI3
    vbitsel.v x1, x1, x3, VT0
    vbitsel.v x2, x2, x4, VINC8
    vfadd.d VM0, x1, x2
#else
    addi.w i0, i0, 1

    // Init index
    slli.w i0, i0, 2 //4
    vreplgr2vr.w VINC4, i0
    addi.w i0, i0, -7
    vinsgr2vr.w VI1, i0, 0 //initialize the index value for vectorization
    addi.w i0, i0, 1
    vinsgr2vr.w VI1, i0, 1
    addi.w i0, i0, 1
    vinsgr2vr.w VI1, i0, 2
    addi.w i0, i0, 1
    vinsgr2vr.w VI1, i0, 3
    addi.w i0, i0, 1
    vinsgr2vr.w VI0, i0, 0 //1
    addi.w i0, i0, 1
    vinsgr2vr.w VI0, i0, 1 //2
    addi.w i0, i0, 1
    vinsgr2vr.w VI0, i0, 2 //3
    addi.w i0, i0, 1
    vinsgr2vr.w VI0, i0, 3 //4

    srai.d I, N, 2
    bge $r0, I, .L21 // N < 4

    // Init VM0
    ld.w t1, X, 0 * SIZE
    ld.w t2, X, 1 * SIZE
    add.d i1, X, INCX
    ld.w t3, i1, 0 * SIZE
    ld.w t4, i1, 1 * SIZE
    add.d i1, i1, INCX
    vinsgr2vr.w x1, t1, 0
    vinsgr2vr.w x2, t2, 0
    vinsgr2vr.w x1, t3, 1
    vinsgr2vr.w x2, t4, 1
    ld.w t1, i1, 0 * SIZE
    ld.w t2, i1, 1 * SIZE
    add.d i1, i1, INCX
    ld.w t3, i1, 0 * SIZE
    ld.w t4, i1, 1 * SIZE
    add.d i1, i1, INCX
    vinsgr2vr.w x1, t1, 2
    vinsgr2vr.w x2, t2, 2
    vinsgr2vr.w x1, t3, 3
    vinsgr2vr.w x2, t4, 3
    vfcmp.clt.s VT0, x1, VI3
    vfcmp.clt.s VINC8, x2, VI3
    vbitsel.v x1, x1, x3, VT0
    vbitsel.v x2, x2, x4, VINC8
    vfadd.s VM0, x1, x2
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
    vinsgr2vr.d x1, t1, 0
    vinsgr2vr.d x2, t2, 0
    vinsgr2vr.d x1, t3, 1
    vinsgr2vr.d x2, t4, 1
    vadd.d VI1, VI1, VINC4
    vfmul.d x3, VI4, x1
    vfmul.d x4, VI4, x2
    vfcmp.clt.d VT0, x1, VI3
    vfcmp.clt.d VINC8, x2, VI3
    vbitsel.v x1, x1, x3, VT0
    vbitsel.v x2, x2, x4, VINC8
    vfadd.d x1, x1, x2
    vfmin.d x3, VM0, x1
    ld.d t1, X, 0 * SIZE
    vfcmp.ceq.d VT0, x3, VM0
    ld.d t2, X, 1 * SIZE
    vbitsel.v VM0, x3, VM0, VT0
    vbitsel.v VI0, VI1, VI0, VT0
    add.d X, X, INCX
    ld.d t3, X, 0 * SIZE
    ld.d t4, X, 1 * SIZE
    add.d X, X, INCX
    vinsgr2vr.d x1, t1, 0
    vinsgr2vr.d x2, t2, 0
    vinsgr2vr.d x1, t3, 1
    vinsgr2vr.d x2, t4, 1
    vadd.d VI1, VI1, VINC4
#else
    ld.w t1, X, 0 * SIZE
    ld.w t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.w t3, X, 0 * SIZE
    ld.w t4, X, 1 * SIZE
    add.d X, X, INCX
    vinsgr2vr.w x1, t1, 0
    vinsgr2vr.w x2, t2, 0
    vinsgr2vr.w x1, t3, 1
    vinsgr2vr.w x2, t4, 1
    vadd.w VI1, VI1, VINC4
    ld.w t1, X, 0 * SIZE
    ld.w t2, X, 1 * SIZE
    add.d X, X, INCX
    ld.w t3, X, 0 * SIZE
    ld.w t4, X, 1 * SIZE
    add.d X, X, INCX
    vinsgr2vr.w x1, t1, 2
    vinsgr2vr.w x2, t2, 2
    vinsgr2vr.w x1, t3, 3
    vinsgr2vr.w x2, t4, 3
#endif
    addi.d I, I, -1
    VFMUL  x3, VI4, x1
    VFMUL  x4, VI4, x2
    VCMPLT VT0, x1, VI3
    VCMPLT VINC8, x2, VI3
    vbitsel.v x1, x1, x3, VT0
    vbitsel.v x2, x2, x4, VINC8
    VFADD  x1, x1, x2
    VFMIN  x3, VM0, x1
    VCMPEQ VT0, x3, VM0
    vbitsel.v VM0, x3, VM0, VT0
    vbitsel.v VI0, VI1, VI0, VT0
    blt $r0, I, .L24
    .align 3

.L25:
#ifdef DOUBLE
    vreplvei.d VI1, VI0, 0
    vreplvei.d VI2, VI0, 1
    vreplvei.d x1, VM0, 0
    vreplvei.d x2, VM0, 1
    fcmp.ceq.d $fcc0, $f10, $f9
    bceqz $fcc0, .L26
    vfcmp.clt.d VT0, VI1, VI2
    vbitsel.v VI0, VI2, VI1, VT0
    b .L27
#else
    vreplvei.w VI1, VI0, 0
    vreplvei.w VI2, VI0, 1
    vreplvei.w VI3, VI0, 2
    vreplvei.w VI4, VI0, 3
    vreplvei.w x1, VM0, 0
    vreplvei.w x2, VM0, 1
    vreplvei.w x3, VM0, 2
    vreplvei.w x4, VM0, 3
    vfmina.s VM1, x1, x2
    vfcmp.ceq.s VT0, VM1, x1
    vbitsel.v VINC4, VI2, VI1, VT0
    vfmina.s VM0, x3, x4
    vfcmp.ceq.s VT0, x3, VM0
    vbitsel.v VINC8, VI4, VI3, VT0
    vfmina.s VM0, VM0, VM1
    vfcmp.ceq.s VT0, VM0, VM1
    vbitsel.v VI0, VINC8, VINC4, VT0
    fcmp.ceq.d $fcc0, $f15, $f9
    bceqz $fcc0, .L26
    vfcmp.clt.s VT0, VI1, VI0
    vbitsel.v VI0, VI0, VI1, VT0
#endif
    .align 3

.L26:
#ifdef DOUBLE
    vfmina.d VM0, x1, x2
    vfcmp.ceq.d VT0, x1, VM0
    vbitsel.v VI0, VI2, VI1, VT0
#else
    fcmp.ceq.d $fcc0, $f15, $f10
    bceqz $fcc0, .L27
    vfcmp.clt.s VT0, VI2, VI0
    vbitsel.v VI0, VI0, VI2, VT0
#endif
    .align 3

.L27:
#ifdef DOUBLE
    movfr2gr.d i0, $f20
    .align 3
#else
    fcmp.ceq.d $fcc0, $f15, $f11
    bceqz $fcc0, .L28
    vfcmp.clt.s VT0, VI3, VI0
    vbitsel.v VI0, VI0, VI3, VT0
    .align 3

.L28:
    fcmp.ceq.d $fcc0, $f15, $f12
    bceqz $fcc0, .L29
    vfcmp.clt.s VT0, VI4, VI0
    vbitsel.v VI0, VI0, VI4, VT0
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
    FMIN a1, s1, a0
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
