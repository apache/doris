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
    slli.d TEMP, TEMP, BASE_SHIFT
    slli.d INCX, INCX, BASE_SHIFT
    bne INCX, TEMP, .L20
    xvld VM0, X, 0
#ifdef DOUBLE
    addi.d i0, i0, 1
    srai.d I, N, 3
    bge $r0, I, .L21
    slli.d i0, i0, 2 //4
    xvreplgr2vr.d VINC4, i0
    slli.d i0, i0, 1 //8
    xvreplgr2vr.d VINC8, i0
    addi.d i0, i0, -15
    xvinsgr2vr.d VI1, i0, 0 //initialize the index value for vectorization
    addi.d i0, i0, 1
    xvinsgr2vr.d VI1, i0, 1
    addi.d i0, i0, 1
    xvinsgr2vr.d VI1, i0, 2
    addi.d i0, i0, 1
    xvinsgr2vr.d VI1, i0, 3
    addi.d i0, i0, 5
    xvinsgr2vr.d VI0, i0, 0 //1
    addi.d i0, i0, 1
    xvinsgr2vr.d VI0, i0, 1 //2
    addi.d i0, i0, 1
    xvinsgr2vr.d VI0, i0, 2 //3
    addi.d i0, i0, 1
    xvinsgr2vr.d VI0, i0, 3 //4
#else
    addi.w i0, i0, 1
    srai.d I, N, 3
    bge $r0, I, .L21
    slli.w i0, i0, 3 //8
    xvreplgr2vr.w VINC8, i0
    addi.w i0, i0, -15
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
    addi.w i0, i0, 1
    xvinsgr2vr.w VI0, i0, 0 //1
    addi.w i0, i0, 1
    xvinsgr2vr.w VI0, i0, 1 //2
    addi.w i0, i0, 1
    xvinsgr2vr.w VI0, i0, 2 //3
    addi.w i0, i0, 1
    xvinsgr2vr.w VI0, i0, 3 //4
    addi.w i0, i0, 1
    xvinsgr2vr.w VI0, i0, 4 //5
    addi.w i0, i0, 1
    xvinsgr2vr.w VI0, i0, 5 //6
    addi.w i0, i0, 1
    xvinsgr2vr.w VI0, i0, 6 //7
    addi.w i0, i0, 1
    xvinsgr2vr.w VI0, i0, 7 //8
#endif
    .align 3

.L10:
    xvld VX0, X, 0 * SIZE
#ifdef DOUBLE
    xvadd.d VI1, VI1, VINC8
    xvld VX1, X, 4 * SIZE
    xvadd.d VI2, VI1, VINC4
    XVCMPLT VT0, VX1, VX0
    addi.d I, I, -1
    xvbitsel.v VM1, VX0, VX1, VT0
    xvbitsel.v VI2, VI1, VI2, VT0
    XVCMPLT VT0, VM1, VM0
    addi.d X, X, 8 * SIZE
    xvbitsel.v VM0, VM0, VM1, VT0
    xvbitsel.v VI0, VI0, VI2, VT0
#else
    xvadd.w VI1, VI1, VINC8
    XVCMPLT VT0, VX0, VM0
    addi.d I, I, -1
    xvbitsel.v VM0, VM0, VX0, VT0
    xvbitsel.v VI0, VI0, VI1, VT0
    addi.d X, X, 8 * SIZE
#endif
    blt $r0, I, .L10
    .align 3

.L15:
#ifdef DOUBLE
    xvpickve.d VI1, VI0, 0
    xvpickve.d VI2, VI0, 1
    xvpickve.d VI3, VI0, 2
    xvpickve.d VI4, VI0, 3
    xvpickve.d  x1, VM0, 0
    xvpickve.d  x2, VM0, 1
    xvpickve.d  x3, VM0, 2
    xvpickve.d  x4, VM0, 3
#else
    xvxor.v VX0, VX0, VX0
    xvor.v VX0, VI0, VX0
    xvxor.v VX1, VX1, VX1
    xvor.v VX1, VM0, VX1
    xvpickve.w VI1, VI0, 0
    xvpickve.w VI2, VI0, 1
    xvpickve.w VI3, VI0, 2
    xvpickve.w VI4, VI0, 3
    xvpickve.w  x1, VM0, 0
    xvpickve.w  x2, VM0, 1
    xvpickve.w  x3, VM0, 2
    xvpickve.w  x4, VM0, 3
#endif
    XVCMPLT VT0, x2, x1
    xvbitsel.v VM1, x1, x2, VT0
    xvbitsel.v VINC4, VI1, VI2, VT0
    XVCMPLT VT0, x4, x3
    xvbitsel.v VM0, x3, x4, VT0
    xvbitsel.v VINC8, VI3, VI4, VT0
    XVCMPLT VT0, VM1, VM0
    xvbitsel.v VM0, VM0, VM1, VT0
    xvbitsel.v VI0, VINC8, VINC4, VT0
    fcmp.ceq.d $fcc0, $f15, $f9
    bceqz $fcc0, .L26
    XVCMPLT VT0, VI1, VI0
    xvbitsel.v VI0, VI0, VI1, VT0
    b .L26
    .align 3

.L20: // INCX!=1
    move TEMP, X
#ifdef DOUBLE
    addi.d i0, i0, 1
    ld.d t1, TEMP, 0 * SIZE
    add.d TEMP, TEMP, INCX
    xvinsgr2vr.d VM0, t1, 0
    srai.d I, N, 3
    bge $r0, I, .L21
    ld.d t2, TEMP, 0 * SIZE
    add.d TEMP, TEMP, INCX
    ld.d t3, TEMP, 0 * SIZE
    add.d TEMP, TEMP, INCX
    ld.d t4, TEMP, 0 * SIZE
    add.d TEMP, TEMP, INCX
    xvinsgr2vr.d VM0, t1, 0
    xvinsgr2vr.d VM0, t2, 1
    xvinsgr2vr.d VM0, t3, 2
    xvinsgr2vr.d VM0, t4, 3
    slli.d i0, i0, 2 //4
    xvreplgr2vr.d VINC4, i0
    slli.d i0, i0, 1 //8
    xvreplgr2vr.d VINC8, i0
    addi.d i0, i0, -15
    xvinsgr2vr.d VI1, i0, 0 //initialize the index value for vectorization
    addi.d i0, i0, 1
    xvinsgr2vr.d VI1, i0, 1
    addi.d i0, i0, 1
    xvinsgr2vr.d VI1, i0, 2
    addi.d i0, i0, 1
    xvinsgr2vr.d VI1, i0, 3
    addi.d i0, i0, 5
    xvinsgr2vr.d VI0, i0, 0 //1
    addi.d i0, i0, 1
    xvinsgr2vr.d VI0, i0, 1 //2
    addi.d i0, i0, 1
    xvinsgr2vr.d VI0, i0, 2 //3
    addi.d i0, i0, 1
    xvinsgr2vr.d VI0, i0, 3 //4
#else
    addi.w i0, i0, 1
    ld.w t1, TEMP, 0 * SIZE
    add.d TEMP, TEMP, INCX
    srai.d I, N, 3
    bge $r0, I, .L21
    ld.w t2, TEMP, 0 * SIZE
    add.d TEMP, TEMP, INCX
    ld.w t3, TEMP, 0 * SIZE
    add.d TEMP, TEMP, INCX
    ld.w t4, TEMP, 0 * SIZE
    add.d TEMP, TEMP, INCX
    xvinsgr2vr.w VM0, t1, 0
    xvinsgr2vr.w VM0, t2, 1
    xvinsgr2vr.w VM0, t3, 2
    xvinsgr2vr.w VM0, t4, 3
    ld.w t1, TEMP, 0 * SIZE
    add.d TEMP, TEMP, INCX
    ld.w t2, TEMP, 0 * SIZE
    add.d TEMP, TEMP, INCX
    ld.w t3, TEMP, 0 * SIZE
    add.d TEMP, TEMP, INCX
    ld.w t4, TEMP, 0 * SIZE
    add.d TEMP, TEMP, INCX
    xvinsgr2vr.w VM0, t1, 4
    xvinsgr2vr.w VM0, t2, 5
    xvinsgr2vr.w VM0, t3, 6
    xvinsgr2vr.w VM0, t4, 7
    slli.w i0, i0, 3 //8
    xvreplgr2vr.w VINC8, i0
    addi.w i0, i0, -15
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
    addi.w i0, i0, 1
    xvinsgr2vr.w VI0, i0, 0 //1
    addi.w i0, i0, 1
    xvinsgr2vr.w VI0, i0, 1 //2
    addi.w i0, i0, 1
    xvinsgr2vr.w VI0, i0, 2 //3
    addi.w i0, i0, 1
    xvinsgr2vr.w VI0, i0, 3 //4
    addi.w i0, i0, 1
    xvinsgr2vr.w VI0, i0, 4 //5
    addi.w i0, i0, 1
    xvinsgr2vr.w VI0, i0, 5 //6
    addi.w i0, i0, 1
    xvinsgr2vr.w VI0, i0, 6 //7
    addi.w i0, i0, 1
    xvinsgr2vr.w VI0, i0, 7 //8
#endif
    .align 3

.L24:
#ifdef DOUBLE
    ld.d t1, X, 0 * SIZE
    add.d X, X, INCX
    ld.d t2, X, 0 * SIZE
    add.d X, X, INCX
    ld.d t3, X, 0 * SIZE
    add.d X, X, INCX
    ld.d t4, X, 0 * SIZE
    add.d X, X, INCX
    xvinsgr2vr.d VX0, t1, 0
    xvinsgr2vr.d VX0, t2, 1
    xvinsgr2vr.d VX0, t3, 2
    xvinsgr2vr.d VX0, t4, 3
    xvadd.d VI1, VI1, VINC8
    ld.d t1, X, 0 * SIZE
    add.d X, X, INCX
    ld.d t2, X, 0 * SIZE
    add.d X, X, INCX
    ld.d t3, X, 0 * SIZE
    add.d X, X, INCX
    ld.d t4, X, 0 * SIZE
    add.d X, X, INCX
    xvinsgr2vr.d VX1, t1, 0
    xvinsgr2vr.d VX1, t2, 1
    xvinsgr2vr.d VX1, t3, 2
    xvinsgr2vr.d VX1, t4, 3
    xvadd.d VI1, VI1, VINC8
    xvadd.d VI2, VI1, VINC4
    XVCMPLT VT0, VX1, VX0
    addi.d I, I, -1
    xvbitsel.v VM1, VX0, VX1, VT0
    xvbitsel.v VI2, VI1, VI2, VT0
    XVCMPLT VT0, VM1, VM0
    xvbitsel.v VM0, VM0, VM1, VT0
    xvbitsel.v VI0, VI0, VI2, VT0
#else
    ld.w t1, X, 0 * SIZE
    add.d X, X, INCX
    ld.w t2, X, 0 * SIZE
    add.d X, X, INCX
    ld.w t3, X, 0 * SIZE
    add.d X, X, INCX
    ld.w t4, X, 0 * SIZE
    add.d X, X, INCX
    xvinsgr2vr.w VX0, t1, 0
    xvinsgr2vr.w VX0, t2, 1
    xvinsgr2vr.w VX0, t3, 2
    xvinsgr2vr.w VX0, t4, 3
    ld.w t1, X, 0 * SIZE
    add.d X, X, INCX
    ld.w t2, X, 0 * SIZE
    add.d X, X, INCX
    ld.w t3, X, 0 * SIZE
    add.d X, X, INCX
    ld.w t4, X, 0 * SIZE
    add.d X, X, INCX
    xvinsgr2vr.w VX0, t1, 4
    xvinsgr2vr.w VX0, t2, 5
    xvinsgr2vr.w VX0, t3, 6
    xvinsgr2vr.w VX0, t4, 7
    xvadd.w VI1, VI1, VINC8
    XVCMPLT VT0, VX0, VM0
    addi.d I, I, -1
    xvbitsel.v VM0, VM0, VX0, VT0
    xvbitsel.v VI0, VI0, VI1, VT0
#endif
    blt $r0, I, .L24
    .align 3

.L25:
#ifdef DOUBLE
    xvpickve.d VI1, VI0, 0
    xvpickve.d VI2, VI0, 1
    xvpickve.d VI3, VI0, 2
    xvpickve.d VI4, VI0, 3
    xvpickve.d x1,  VM0, 0
    xvpickve.d x2,  VM0, 1
    xvpickve.d x3,  VM0, 2
    xvpickve.d x4,  VM0, 3
#else
    xvxor.v VX0, VX0, VX0
    xvor.v  VX0, VI0, VX0
    xvxor.v VX1, VX1, VX1
    xvor.v  VX1, VM0, VX1
    xvpickve.w VI1, VI0, 0
    xvpickve.w VI2, VI0, 1
    xvpickve.w VI3, VI0, 2
    xvpickve.w VI4, VI0, 3
    xvpickve.w  x1, VM0, 0
    xvpickve.w  x2, VM0, 1
    xvpickve.w  x3, VM0, 2
    xvpickve.w  x4, VM0, 3
#endif
    XVCMPLT    VT0, x2, x1
    xvbitsel.v VM1, x1, x2, VT0
    xvbitsel.v VINC4, VI1, VI2, VT0
    XVCMPLT    VT0, x4, x3
    xvbitsel.v VM0, x3, x4, VT0
    xvbitsel.v VINC8, VI3, VI4, VT0
    XVCMPLT VT0, VM1, VM0
    xvbitsel.v VM0, VM0, VM1, VT0
    xvbitsel.v VI0, VINC8, VINC4, VT0
    fcmp.ceq.d $fcc0, $f15, $f9
    bceqz $fcc0, .L26
    XVCMPLT VT0, VI1, VI0
    xvbitsel.v VI0, VI0, VI1, VT0
    .align 3

.L26:
    fcmp.ceq.d $fcc0, $f15, $f10
    bceqz $fcc0, .L27
    XVCMPLT VT0, VI2, VI0
    xvbitsel.v VI0, VI0, VI2, VT0
    .align 3

.L27:
    fcmp.ceq.d $fcc0, $f15, $f11
    bceqz $fcc0, .L28
    XVCMPLT VT0, VI3, VI0
    xvbitsel.v VI0, VI0, VI3, VT0
    .align 3

.L28:
    fcmp.ceq.d $fcc0, $f15, $f12
    bceqz $fcc0, .L29
    XVCMPLT VT0, VI4, VI0
    xvbitsel.v VI0, VI0, VI4, VT0
    .align 3

.L29:
#ifdef DOUBLE
    MTG i0, $f20
#else
    fmov.s $f16, $f20
#endif
    .align 3

#ifndef DOUBLE
.L252:
    xvxor.v VI0, VI0, VI0
    xvor.v VI0, VI0, VX0
    fmov.s $f13, $f15
    xvxor.v VM0, VM0, VM0
    xvor.v VM0, VM0, VX1
    xvpickve.w VI1, VI0, 4
    xvpickve.w VI2, VI0, 5
    xvpickve.w VI3, VI0, 6
    xvpickve.w VI4, VI0, 7
    xvpickve.w x1, VM0, 4
    xvpickve.w x2, VM0, 5
    xvpickve.w x3, VM0, 6
    xvpickve.w x4, VM0, 7
    XVCMPLT VT0, x2, x1
    xvbitsel.v x1, x1, x2, VT0
    xvbitsel.v VINC4, VI1, VI2, VT0
    XVCMPLT VT0, x4, x3
    xvbitsel.v VM0, x3, x4, VT0
    xvbitsel.v VINC8, VI3, VI4, VT0
    XVCMPLT VT0, x1, VM0
    xvbitsel.v VM0, VM0, x1, VT0
    xvbitsel.v VI0, VINC8, VINC4, VT0
    li.d TEMP, 1 //处理尾数相等时取最小序号
    movgr2fr.w $f17, TEMP
    ffint.s.w $f17, $f17
    xvfcmp.ceq.s VT0, VM0, x1
    fcmp.ceq.s $fcc0, $f23, $f17
    bceqz $fcc0, .L262
    XVCMPLT VT0, VI1, VI0
    xvbitsel.v VI0, VI0, VI1, VT0
    .align 3

.L262:
    xvfcmp.ceq.s VT0, VM0, x2
    fcmp.ceq.s $fcc0, $f23, $f17
    bceqz $fcc0, .L272
    XVCMPLT VT0, VI2, VI0
    xvbitsel.v VI0, VI0, VI2, VT0
    .align 3

.L272:
    xvfcmp.ceq.s VT0, VM0, x3
    fcmp.ceq.s $fcc0, $f23, $f17
    bceqz $fcc0, .L282
    XVCMPLT VT0, VI3, VI0
    xvbitsel.v VI0, VI0, VI3, VT0
    .align 3

.L282:
    xvfcmp.ceq.s VT0, VM0, x4
    fcmp.ceq.s $fcc0, $f23, $f17
    bceqz $fcc0, .L292
    XVCMPLT VT0, VI4, VI0
    xvbitsel.v VI0, VI0, VI4, VT0
    .align 3

.L292:
    CMPLT $fcc0, $f13, $f15
    fsel $f15, $f15, $f13, $fcc0
    fsel $f20, $f20, $f16, $fcc0
    MTG i0, $f20
#endif

.L21: //N<8
    andi I, N, 7
    bge $r0, I, .L999
    srai.d i1, N, 3
    slli.d i1, i1, 3
    addi.d i1, i1, 1 //current index
    movgr2fr.d $f21, i1
    movgr2fr.d $f20, i0
    .align 3

.L22:
    fld.d $f9, X, 0
    addi.d I, I, -1
    CMPLT $fcc0, $f9, $f15
    add.d  X, X, INCX
    fsel $f15, $f15, $f9, $fcc0
    fsel $f20, $f20, $f21, $fcc0
    addi.d i1, i1, 1
    movgr2fr.d $f21, i1
    blt $r0, I, .L22
    MTG i0, $f20
    .align 3

.L999:
    move $r4, $r17
    jirl $r0, $r1, 0x0
    .align 3

    EPILOGUE
