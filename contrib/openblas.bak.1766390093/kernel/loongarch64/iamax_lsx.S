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
#define x1 $vr9
#define x2 $vr10
#define x3 $vr11
#define x4 $vr12
#define VX0 $vr13
#define VX1 $vr14
#define VM0 $vr15
#define VM1 $vr16
#define VINC2 $vr17
#define VINC4 $vr18
#define VI0 $vr20
#define VI1 $vr21
#define VI2 $vr22
#define VI3 $vr8
#define VI4 $vr19
#define VT0 $vr23
#define VZE $vr3
#define VT1 $vr4
#define VT2 $vr5
#define VC0 $vr6

    PROLOGUE
    li.d  i0, 0
    bge $r0, N, .L999
    bge $r0, INCX, .L999
    li.d TEMP, 1
    vldi  VZE,   0
    slli.d TEMP, TEMP, BASE_SHIFT
    slli.d INCX, INCX, BASE_SHIFT
    bne INCX, TEMP, .L20
    vld VM0, X, 0
#ifdef DOUBLE
    vfsub.d VT1, VZE, VM0
    addi.d i0, i0, 1
    srai.d I, N, 3
    vfmaxa.d VM0, VM0, VT1
    bge $r0, I, .L11
    slli.d i0, i0, 1 //2
    vreplgr2vr.d VINC2, i0
    slli.d i0, i0, 1 //4
    vreplgr2vr.d VINC4, i0
    addi.d i0, i0, -7
    vinsgr2vr.d VI1, i0, 0 //initialize the index value for vectorization
    addi.d i0, i0, 1
    vinsgr2vr.d VI1, i0, 1
    addi.d i0, i0, 3
    vinsgr2vr.d VI0, i0, 0 //initialize the index value for vectorization
    addi.d i0, i0, 1
    vinsgr2vr.d VI0, i0, 1
#else
    vfsub.s VT1, VZE, VM0
    addi.w i0, i0, 1
    srai.d I, N, 3
    vfmaxa.s VM0, VM0, VT1
    bge $r0, I, .L21
    slli.w i0, i0, 2 //4
    vreplgr2vr.w VINC2, i0
    slli.w i0, i0, 1 //8
    vreplgr2vr.w VINC4, i0
    addi.w i0, i0, -15
    vinsgr2vr.w VI1, i0, 0 //initialize the index value for vectorization
    addi.w i0, i0, 1
    vinsgr2vr.w VI1, i0, 1
    addi.w i0, i0, 1
    vinsgr2vr.w VI1, i0, 2
    addi.w i0, i0, 1
    vinsgr2vr.w VI1, i0, 3
    addi.w i0, i0, 5
    vinsgr2vr.w VI0, i0, 0 //1
    addi.w i0, i0, 1
    vinsgr2vr.w VI0, i0, 1 //2
    addi.w i0, i0, 1
    vinsgr2vr.w VI0, i0, 2 //3
    addi.w i0, i0, 1
    vinsgr2vr.w VI0, i0, 3 //4
#endif
    .align 3

.L10:
#ifdef DOUBLE
    vld VX0, X, 0 * SIZE
    vadd.d VI1, VI1, VINC4
    vld VX1, X, 2 * SIZE
    vadd.d VI2, VI1, VINC2
    vfsub.d VT1, VZE, VX0
    vfsub.d VT2, VZE, VX1
    vfmaxa.d VX0, VX0, VT1
    vfmaxa.d VX1, VX1, VT2
    vfcmp.clt.d VT0, VX0, VX1   //abx(x0) < abs(x1)
    vbitsel.v x1, VX0, VX1, VT0   //abs(maxf)
    vbitsel.v x2, VI1, VI2, VT0   //i

    vld VX0, X, 4 * SIZE
    vadd.d VI1, VI2, VINC2
    vld VX1, X, 6 * SIZE
    vadd.d VI2, VI1, VINC2
    vfsub.d  VT1, VZE, VX0
    vfsub.d  VT2, VZE, VX1
    vfmaxa.d VX0, VX0, VT1
    vfmaxa.d VX1, VX1, VT2
    vfcmp.clt.d VT0, VX0, VX1
    vbitsel.v x3, VX0, VX1, VT0  //abs(maxf)
    vbitsel.v x4, VI1, VI2, VT0  //i
    vfcmp.clt.d VC0, x1, x3
    vbitsel.v x1, x1, x3, VC0  //abs(maxf)
    vbitsel.v x2, x2, x4, VC0  //i
    vfcmp.clt.d VT0, VM0, x1
    addi.d I, I, -1
    addi.d X, X, 8 * SIZE
    vbitsel.v VM0, VM0, x1, VT0
    vbitsel.v VI0, VI0, x2, VT0
#else
    vld VX0, X, 0 * SIZE
    vadd.w VI1, VI1, VINC4
    vld VX1, X, 4 * SIZE
    vadd.w VI2, VI1, VINC2
    vfsub.s VT1, VZE, VX0
    vfsub.s VT2, VZE, VX1
    vfmaxa.s VX0, VX0, VT1
    vfmaxa.s VX1, VX1, VT2
    vfcmp.clt.s VT0, VX0, VX1
    vbitsel.v x1, VX0, VX1, VT0  //abs(maxf)
    vbitsel.v x2, VI1, VI2, VT0  //i
    addi.d I, I, -1
    vfcmp.clt.s VT0, VM0, x1
    addi.d X, X, 8 * SIZE
    vbitsel.v VM0, VM0, x1, VT0
    vbitsel.v VI0, VI0, x2, VT0

#endif
    blt $r0, I, .L10
    .align 3

.L15:
#ifdef DOUBLE
    vreplvei.d VI1, VI0, 0
    vreplvei.d VI2, VI0, 1
    vreplvei.d x1, VM0, 0
    vreplvei.d x2, VM0, 1
    fcmp.ceq.d $fcc0, $f9, $f10
    bceqz $fcc0, .L16
    vfcmp.clt.d VT0, VI1, VI2
    vbitsel.v VI0, VI2, VI1, VT0
    b .L17
#else
    vreplvei.w VI1, VI0, 0
    vreplvei.w VI2, VI0, 1
    vreplvei.w VI3, VI0, 2
    vreplvei.w VI4, VI0, 3
    vreplvei.w x1, VM0, 0
    vreplvei.w x2, VM0, 1
    vreplvei.w x3, VM0, 2
    vreplvei.w x4, VM0, 3
    b .L26
#endif
    .align 3

#ifdef DOUBLE
.L16:
    vfcmp.clt.d VT0, x1, x2
    vbitsel.v VI0, VI1, VI2, VT0
    vbitsel.v VM0, x1, x2, VT0
    .align 3

.L17:
    movfr2gr.d i0, $f20
    .align 3

.L11: //INCX==1 and N<8
    andi I, N, 7
    bge $r0, I, .L14
    srai.d i1, N, 3
    slli.d i1, i1, 3
    addi.d i1, i1, 1 //current index
    movgr2fr.d $f21, i1
    movgr2fr.d $f20, i0
    .align 3

.L13:
    fld.d $f9, X, 0
    fsub.d $f10, $f3, $f9
    vfmaxa.d x1, x1, x2
    vfcmp.clt.d VT0, VM0, x1
    vbitsel.v VM0, VM0, x1,  VT0
    vbitsel.v VI0, VI0, VI1, VT0
    addi.d I, I, -1
    addi.d i1, i1, 1
    addi.d  X, X, SIZE
    movgr2fr.d $f21, i1
    blt $r0, I, .L13
    movfr2gr.d i0, $f20
    .align 3

.L14:
    move $r4, $r17
    jirl $r0, $r1, 0x0
    .align 3

.L20: // INCX!=1
    move TEMP, X
    addi.d i0, i0, 1
    ld.d t1, TEMP, 0 * SIZE
    add.d TEMP, TEMP, INCX
    vinsgr2vr.d VM0, t1, 0
    srai.d I, N, 3
    bge $r0, I, .L21
    ld.d t2, TEMP, 0 * SIZE
    add.d TEMP, TEMP, INCX
    vinsgr2vr.d VM0, t2, 1
    slli.d i0, i0, 1 //2
    vfsub.d VT1, VZE, VM0
    vreplgr2vr.d VINC2, i0
    slli.d i0, i0, 1 //4
    vreplgr2vr.d VINC4, i0
    addi.d i0, i0, -7
    vfmaxa.d VM0, VM0, VT1
    vinsgr2vr.d VI1, i0, 0 //initialize the index value for vectorization
    addi.d i0, i0, 1
    vinsgr2vr.d VI1, i0, 1
    addi.d i0, i0, 3
    vinsgr2vr.d VI0, i0, 0 //1
    addi.d i0, i0, 1
    vinsgr2vr.d VI0, i0, 1 //2
    .align 3

.L24:
    ld.d t1, X, 0 * SIZE
    add.d X, X, INCX
    vinsgr2vr.d VX0, t1, 0
    ld.d t2, X, 0 * SIZE
    add.d X, X, INCX
    vinsgr2vr.d VX0, t2, 1
    vadd.d VI1, VI1, VINC4
    ld.d t1, X, 0 * SIZE
    add.d X, X, INCX
    vinsgr2vr.d VX1, t1, 0
    ld.d t2, X, 0 * SIZE
    add.d X, X, INCX
    vinsgr2vr.d VX1, t2, 1
    vadd.d VI2, VI1, VINC2

    vfsub.d VT1, VZE, VX0
    vfsub.d VT2, VZE, VX1
    vfmaxa.d VX0, VX0, VT1
    vfmaxa.d VX1, VX1, VT2
    vfcmp.clt.d VT0, VX0, VX1
    vbitsel.v x1, VX0, VX1, VT0
    vbitsel.v x2, VI1, VI2, VT0
    ld.d t1, X, 0 * SIZE
    add.d X, X, INCX
    vinsgr2vr.d VX0, t1, 0
    ld.d t2, X, 0 * SIZE
    add.d X, X, INCX
    vinsgr2vr.d VX0, t2, 1
    vadd.d VI1, VI2, VINC2
    ld.d t1, X, 0 * SIZE
    add.d X, X, INCX
    vinsgr2vr.d VX1, t1, 0
    ld.d t2, X, 0 * SIZE
    add.d X, X, INCX
    vinsgr2vr.d VX1, t2, 1
    vadd.d VI2, VI1, VINC2
    vfsub.d VT1, VZE, VX0
    vfsub.d VT2, VZE, VX1
    vfmaxa.d VX0, VX0, VT1
    vfmaxa.d VX1, VX1, VT2
    vfcmp.clt.d VT0, VX0, VX1
    vbitsel.v x3, VX0, VX1, VT0
    vbitsel.v x4, VI1, VI2, VT0
    vfcmp.clt.d VC0, x1, x3
    vbitsel.v x1, x1, x3, VC0
    vbitsel.v x2, x2, x4, VC0
    vfcmp.clt.d VT0, VM0, x1
    vbitsel.v VM0, VM0, x1, VT0
    vbitsel.v VI0, VI0, x2, VT0

    addi.d I, I, -1
    blt $r0, I, .L24
    .align 3

.L25:
    vreplvei.d VI1, VI0, 0
    vreplvei.d VI2, VI0, 1
    vreplvei.d x1, VM0, 0
    vreplvei.d x2, VM0, 1
    fcmp.ceq.d $fcc0, $f10, $f9
    bceqz $fcc0, .L26
    vfcmp.clt.d VT0, VI1, VI2
    vbitsel.v VI0, VI2, VI1, VT0
    b .L27
    .align 3

.L26:
    vfcmp.clt.d VT0, x1, x2
    vbitsel.v VI0, VI1, VI2, VT0
    vbitsel.v VM0, x1, x2, VT0
    .align 3

.L27:
    movfr2gr.d i0, $f20
    .align 3

#else
.L20: // INCX!=1
    move TEMP, X
    addi.w i0, i0, 1
    ld.w t1, TEMP, 0 * SIZE
    add.d TEMP, TEMP, INCX
    vinsgr2vr.w VM0, t1, 0
    srai.d I, N, 3
    bge $r0, I, .L21
    ld.w t2, TEMP, 0 * SIZE
    add.d TEMP, TEMP, INCX
    ld.w t3, TEMP, 0 * SIZE
    add.d TEMP, TEMP, INCX
    ld.w t4, TEMP, 0 * SIZE
    add.d TEMP, TEMP, INCX
    vinsgr2vr.w VM0, t2, 1
    vinsgr2vr.w VM0, t3, 2
    vinsgr2vr.w VM0, t4, 3
    slli.w i0, i0, 2 //4
    vreplgr2vr.w VINC2, i0
    slli.w i0, i0, 1 //8
    vreplgr2vr.w VINC4, i0
    addi.w i0, i0, -15
    vinsgr2vr.w VI1, i0, 0 //initialize the index value for vectorization
    addi.w i0, i0, 1
    vinsgr2vr.w VI1, i0, 1
    addi.w i0, i0, 1
    vinsgr2vr.w VI1, i0, 2
    addi.w i0, i0, 1
    vinsgr2vr.w VI1, i0, 3
    addi.w i0, i0, 5
    vinsgr2vr.w VI0, i0, 0 //1
    addi.w i0, i0, 1
    vinsgr2vr.w VI0, i0, 1 //2
    addi.w i0, i0, 1
    vinsgr2vr.w VI0, i0, 2 //3
    addi.w i0, i0, 1
    vinsgr2vr.w VI0, i0, 3 //4
    .align 3

.L24:
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
    vadd.w VI1, VI1, VINC4
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
    vadd.w VI2, VI1, VINC2
    vfsub.s VT1, VZE, VX0
    vfsub.s VT2, VZE, VX1
    vfmaxa.s VX0, VX0, VT1
    vfmaxa.s VX1, VX1, VT2
    vfcmp.clt.s VT0, VX0, VX1
    vbitsel.v x1, VX0, VX1, VT0
    vbitsel.v x2, VI1, VI2, VT0 //i

    addi.d I, I, -1
    vfcmp.clt.s VT0, VM0, x1
    vbitsel.v VM0, VM0, x1, VT0
    vbitsel.v VI0, VI0, x2, VT0
    blt $r0, I, .L24
    .align 3

.L25:
    vreplvei.w VI1, VI0, 0
    vreplvei.w VI2, VI0, 1
    vreplvei.w VI3, VI0, 2
    vreplvei.w VI4, VI0, 3
    vreplvei.w x1, VM0, 0
    vreplvei.w x2, VM0, 1
    vreplvei.w x3, VM0, 2
    vreplvei.w x4, VM0, 3
    .align 3

.L26:
    fcmp.ceq.s $fcc0, $f9, $f10
    bceqz $fcc0, .L31
    vfcmp.clt.s VT0, VI1, VI2
    vbitsel.v VI1, VI2, VI1, VT0
    b .L32
    .align 3
.L31:
    vfcmp.clt.s VT0, x1, x2
    vbitsel.v VI1, VI1, VI2, VT0
    vbitsel.v x1, x1, x2, VT0
    .align 3
.L32:
    fcmp.ceq.s $fcc0, $f11, $f12
    bceqz $fcc0, .L33
    vfcmp.clt.s VT1, VI3, VI4
    vbitsel.v VI3, VI4, VI3, VT1
    b .L34
    .align 3
.L33:
    vfcmp.clt.s VT1, x3, x4
    vbitsel.v x3, x3, x4, VT1
    vbitsel.v VI3, VI3, VI4, VT1
    .align 3
.L34:
    fcmp.ceq.s $fcc0, $f9, $f11
    bceqz $fcc0, .L35
    vfcmp.clt.s VT0, VI1, VI3
    vbitsel.v VI0, VI3, VI1, VT0
    vxor.v VM0, x1, VZE
    b .L29
    .align 3
.L35:
    vfcmp.clt.s VT0, x1, x3
    vbitsel.v VM0, x1, x3, VT0
    vbitsel.v VI0, VI1, VI3, VT0
    .align 3
.L29:
    movfr2gr.s i0, $f20
    .align 3

#endif
.L21: // N<8
    andi I, N, 7
    bge $r0, I, .L999
    srai.d i1, N, 3
    slli.d i1, i1, 3
    addi.d i1, i1, 1 //current index
    movgr2fr.d $f21, i1
    movgr2fr.d $f20, i0
    .align 3

.L22:
    LD $f9, X, 0
#ifdef DOUBLE
    fsub.d $f10, $f3, $f9
    vfmaxa.d x1, x1, x2
    vfcmp.clt.d VT0, VM0, x1
#else
    fsub.s $f10, $f3, $f9
    vfmaxa.s x1, x1, x2
    vfcmp.clt.s VT0, VM0, x1
#endif
    vbitsel.v VM0, VM0, x1, VT0
    vbitsel.v VI0, VI0, VI1, VT0
    addi.d I, I, -1
    addi.d i1, i1, 1
    add.d  X, X, INCX
    movgr2fr.d $f21, i1
    blt $r0, I, .L22
    MTG   i0,  $f20
    .align 3

.L999:
    move $r4, $r17
    jirl $r0, $r1, 0x0
    .align 3

    EPILOGUE
