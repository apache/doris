/*******************************************************************************
Copyright (c) 2024, The OpenBLAS Project
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
*******************************************************************************/
#define ASSEMBLER

#include "common.h"
#include "loongarch64_asm.S"

/* int CNAME(BLASLONG m, BLASLONG n, BLASLONG dummy1, FLOAT alpha_r, FLOAT alpha_i,
 * FLOAT *a, BLASLONG lda, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *buffer)
 */
#define M       $r4
#define N       $r5
#define ALPHA_R $f0
#define ALPHA_I $f1
#define A       $r7
#define LDA     $r8
#define X       $r9
#define INC_X   $r10
#define Y       $r11
#define INC_Y   $r6

#define J       $r12
#define I       $r13
#define K       $r14
#define PY0     $r14
#define X_ORG   $r15
#define PY1     $r16
#define K_LDA   $r17
#define PY2     $r18
#define T0      $r19
#define PA0     $r20
#define PA1     $r23
#define PA2     $r24
#define PA3     $r25
#define PA4     $r26
#define PA5     $r27
#define PA6     $r28
#define PA7     $r29
#define M8      $r30

#define VALPHA  $vr0
#define X0      $vr1
#define X1      $vr2
#define A0      $vr3
#define A1      $vr4
#define A2      $vr5
#define A3      $vr6
#define A4      $vr7
#define A5      $vr8
#define A6      $vr9
#define A7      $vr10
#define A8      $vr11
#define A9      $vr12
#define A10     $vr13
#define A11     $vr14
#define A12     $vr15
#define A13     $vr16
#define A14     $vr17
#define A15     $vr18
#define TP0     $vr19
#define TP1     $vr20
#define TP2     $vr21
#define TP3     $vr22
#define TP4     $vr23
#define TP5     $vr24
#define TP6     $vr25
#define TP7     $vr26
#define TMP0    $vr27
#define TMP1    $vr28
#define TMP2    $vr29
#define Y0      $vr3
#define Y1      $vr4
#define Y2      $vr5
#define Y3      $vr6
#define Y4      $vr7
#define Y5      $vr8
#define Y6      $vr9
#define Y7      $vr10

#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
#define GXCONJ1 0
#define GCONJ1  0
#else
#define GXCONJ1 1
#define GCONJ1  0
#endif

#if !defined(XCONJ)
#define GXCONJ2 0
#define GCONJ2  0
#else
#define GXCONJ2 0
#define GCONJ2  1
#endif

.macro ZERO_Y4
    GXOR v, v, TP0, TP0, TP0, TP1, TP1, TP1, TP2, TP2, TP2, TP3, TP3, TP3
.endm

.macro ZERO_Y1
    GXOR v, v, TP0, TP0, TP0
.endm

.macro CLOAD_X4
    GLD v, , X0, X, 0x00, X1, X, 0x10
.endm

.macro CLOAD_X4_GAP
    fld.d       $f1,    X,    0x00
    fldx.d      $f3,    X,    INC_X
    PTR_ALSL    T0,     INC_X,      X,      1
    fld.d       $f2,    T0,   0x00
    fldx.d      $f4,    T0,   INC_X
    vpackev.d   X0, A0, X0
    vpackev.d   X1, A1, X1
.endm

.macro CGEMV_T_4x4
    GLD_INC v, , 0x10,       \
    A0,  PA0, 0, A1,  PA0, 0, \
    A2,  PA1, 0, A3,  PA1, 0, \
    A4,  PA2, 0, A5,  PA2, 0, \
    A6,  PA3, 0, A7,  PA3, 0
    GCOMPLEXMADD GXCONJ1, GCONJ1, \
    vf, s, TP0, A0,  X0, TP0, TMP0, TMP1, TMP2, TP0, A1,  X1, TP0, TMP0, TMP1, TMP2, \
    TP1, A2,  X0, TP1, TMP0, TMP1, TMP2, TP1, A3,  X1, TP1, TMP0, TMP1, TMP2, \
    TP2, A4,  X0, TP2, TMP0, TMP1, TMP2, TP2, A5,  X1, TP2, TMP0, TMP1, TMP2, \
    TP3, A6,  X0, TP3, TMP0, TMP1, TMP2, TP3, A7,  X1, TP3, TMP0, TMP1, TMP2
.endm

.macro CGEMV_T_LSX XW:req, X4:req
    PTR_SRLI  J,      N,      2
    beqz      J,      .L_\XW\()_N_3
    PTR_SLLI  K_LDA,  LDA,    2
    PTR_SUB   K_LDA,  K_LDA,  M8
.L_\XW\()_N_L4:
    ZERO_Y4
    move      X,      X_ORG
    PTR_SRLI  I,      M,       2
    beqz      I,      .L_\XW\()_M_3
.align 5
.L_\XW\()_M_L4:
    CLOAD_\X4
    CGEMV_T_4x4
    PTR_ADDI    I,  I,  -1
    PTR_ALSL    X,  INC_X,  X,  2
    bnez        I,  .L_\XW\()_M_L4
.L_\XW\()_M_3:
    // Accumulated
    GCOMPLEXACC vf, s, Y0, TP0, Y1, TP1, Y2, TP2, Y3, TP3
    andi        I,      M,      3
    beqz        I,      .L_\XW\()_M_END
.align 5
.L_\XW\()_M_L1:
    fld.d   $f1,    X,      0x00
    fld.d   $f11,   PA0,    0x00
    fld.d   $f12,   PA1,    0x00
    fld.d   $f13,   PA2,    0x00
    fld.d   $f14,   PA3,    0x00
#if __loongarch_grlen == 64
    GADDI , d, PA0, PA0, 0x08, PA1, PA1, 0x08, PA2, PA2, 0x08, PA3, PA3, 0x08
#elif __loongarch_grlen == 32
    GADDI , w, PA0, PA0, 0x08, PA1, PA1, 0x08, PA2, PA2, 0x08, PA3, PA3, 0x08
#else
    GADDI , d, PA0, PA0, 0x08, PA1, PA1, 0x08, PA2, PA2, 0x08, PA3, PA3, 0x08
#endif

    GCOMPLEXMADD GXCONJ1, GCONJ1, \
    vf, s, A0, A8,  X0, A0, TMP0, TMP1, TMP2, A1, A9,  X0, A1, TMP0, TMP1, TMP2, \
    A2, A10, X0, A2, TMP0, TMP1, TMP2, A3, A11, X0, A3, TMP0, TMP1, TMP2

    PTR_ADDI  I,      I,      -1
    PTR_ADD   X,      X,      INC_X
    bnez      I,      .L_\XW\()_M_L1
.L_\XW\()_M_END:
    fld.d   $f11,   Y,  0x00
    fldx.d  $f12,   Y,  INC_Y
    PTR_ALSL    PY0, INC_Y,  Y,  1
    fld.d   $f13,   PY0,    0x00
    fldx.d  $f14,   PY0,    INC_Y

    GCOMPLEXMADD GXCONJ2, GCONJ2, \
    vf, s, A8,  VALPHA, A0, A8,  TMP0, TMP1, TMP2, A9,  VALPHA, A1, A9,  TMP0, TMP1, TMP2,\
    A10, VALPHA, A2, A10, TMP0, TMP1, TMP2, A11, VALPHA, A3, A11, TMP0, TMP1, TMP2

    PTR_ADDI    J,      J,      -1
#if __loongarch_grlen == 64
    GADD , d, PA0, PA0, K_LDA, PA1, PA1, K_LDA, PA2, PA2, K_LDA, PA3, PA3, K_LDA
#elif __loongarch_grlen == 32
    GADD , w, PA0, PA0, K_LDA, PA1, PA1, K_LDA, PA2, PA2, K_LDA, PA3, PA3, K_LDA
#else
    GADD , d, PA0, PA0, K_LDA, PA1, PA1, K_LDA, PA2, PA2, K_LDA, PA3, PA3, K_LDA
#endif
    fst.d   $f11,   Y,      0x00
    fstx.d  $f12,   Y,      INC_Y
    fst.d   $f13,   PY0,    0x00
    fstx.d  $f14,   PY0,    INC_Y
    PTR_ALSL    Y,      INC_Y,  Y,  2
    bnez        J,      .L_\XW\()_N_L4
.L_\XW\()_N_3:
    andi        J,      N,      3
    beqz        J,      .L_END
    PTR_SUB     K_LDA,  LDA,    M8
.L_\XW\()_N_1:
    ZERO_Y1
    move    X,      X_ORG
    move    I,      M
    beqz    I,      .L_END
.align 5
.L_\XW\()_N_1_M_L1:
    fld.d   $f3,    PA0,    0x00
    fld.d   $f1,    X,      0x00
    GCOMPLEXMADD GXCONJ1, GCONJ1, \
    vf, s, TP0, A0, X0, TP0, TMP0, TMP1, TMP2
    PTR_ADDI  I,      I,      -1
    PTR_ADD   X,      X,      INC_X
    PTR_ADDI  PA0,    PA0,    0x08
    bnez      I,      .L_\XW\()_N_1_M_L1
.L_\XW\()_N_1_M_END:
    PTR_ADDI  J,      J,      -1
    fld.d     $f3,    Y,      0x00
    GCOMPLEXMADD GXCONJ2, GCONJ2, \
    vf, s, A0, VALPHA, TP0, A0, TMP0, TMP1, TMP2
    fst.d     $f3,    Y,      0x00
    PTR_ADD   PA0,    PA0,    K_LDA
    PTR_ADD   Y,      Y,      INC_Y
    bnez      J,      .L_\XW\()_N_1

    b .L_END
.endm

    PROLOGUE
    PTR_LD     INC_Y,  $sp,    0
    push_if_used 8, 6
    PTR_ADDI   K,      $r0,     0x01
    PTR_SUB    I,      INC_X,   K
    maskeqz    I,      K,       I  /* if(inc_x == 1) I = 0; else I = 1; */
    GSLLI , d, LDA, LDA, 3, INC_X, INC_X, 3, INC_Y, INC_Y, 3, M8, M, 3
    // Init VALPHA
    vpackev.w      $vr0,   $vr1,   $vr0
    vpackev.d      VALPHA, $vr0,   $vr0
    move     X_ORG,  X
    move     PA0,    A
#if __loongarch_grlen == 64
    GADD , d, PA1, PA0, LDA, PA2, PA1, LDA, PA3, PA2, LDA, PA4, PA3, LDA
#elif __loongarch_grlen == 32
    GADD , w, PA1, PA0, LDA, PA2, PA1, LDA, PA3, PA2, LDA, PA4, PA3, LDA
#else
    GADD , d, PA1, PA0, LDA, PA2, PA1, LDA, PA3, PA2, LDA, PA4, PA3, LDA
#endif
    la.local    T0,     .L_GAP_TABLE
    PTR_ALSL    I,      I,      T0,     1
    ld.h        K,      I,      0
    PTR_ADD     T0,     T0,     K
    jirl        $r0,    T0,     0
.L_GAP_TABLE:
    .hword .L_GAP_0 - .L_GAP_TABLE
    .hword .L_GAP_1 - .L_GAP_TABLE
.L_GAP_0: /* if (incx == 1) */
    CGEMV_T_LSX GAP_0, X4
.L_GAP_1: /* if (incx != 1) */
    CGEMV_T_LSX GAP_1, X4_GAP
.L_END:
    pop_if_used 8, 6
    jirl    $r0, $r1, 0x0
    EPILOGUE
