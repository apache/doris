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
#define Y_ORG   $r15
#define OFFSET  $r16
#define K_LDA   $r17
#define M16     $r18
#define T0      $r19
#define PA0     $r20
#define PA1     $r23
#define PA2     $r24
#define PA3     $r25
#define PA4     $r26
#define PA5     $r27
#define PA6     $r28
#define PA7     $r29

#define VALPHA  $vr1
#define X0      $vr2
#define X1      $vr3
#define X2      $vr4
#define X3      $vr5
#define X4      $vr6
#define X5      $vr7
#define X6      $vr8
#define X7      $vr9
#define Y0      $vr10
#define Y1      $vr11
#define A0      $vr12
#define A1      $vr13
#define A2      $vr14
#define A3      $vr15
#define A4      $vr16
#define A5      $vr17
#define A6      $vr18
#define A7      $vr19
#define A8      $vr20
#define A9      $vr21
#define A10     $vr22
#define A11     $vr23
#define A12     $vr24
#define A13     $vr25
#define A14     $vr26
#define A15     $vr27
#define TMP0    $vr28
#define TMP1    $vr29
#define TMP2    $vr30

#if !defined(CONJ)
#if !defined(XCONJ)
#define GXCONJ 0
#define GCONJ  0
#else
#define GXCONJ 1
#define GCONJ  0
#endif
#else
#if !defined(XCONJ)
#define GXCONJ 0
#define GCONJ  1
#else
#define GXCONJ 1
#define GCONJ  1
#endif
#endif

.macro ZLOAD_X_2
    GLD v, , X0, X, 0x00, X1, X, 0x10
    GCOMPLEXMUL GXCONJ, \
    vf, d, X0, VALPHA, X0, TMP0, TMP1, TMP2, \
    X1, VALPHA, X1, TMP0, TMP1, TMP2
.endm

.macro ZLOAD_X_2_GAP
    vld         X0,     X,      0
    PTR_ADD     T0,     X,      INC_X
    vld         X1,     T0,     0

    GCOMPLEXMUL GXCONJ, \
    vf, d, X0, VALPHA, X0, TMP0, TMP1, TMP2, \
    X1, VALPHA, X1, TMP0, TMP1, TMP2
.endm

.macro ZLOAD_X_1
    GLD v, , X0, X, 0x00
    GCOMPLEXMUL GXCONJ, \
    vf, d, X0, VALPHA, X0, TMP0, TMP1, TMP2
.endm

.macro ZLOAD_Y_2
    GLD v, , Y0, Y, 0, Y1, Y, 0x10
.endm

.macro ZLOAD_Y_2_GAP
    vld  $vr10, Y, 0
    vldx $vr11, Y, INC_Y
.endm

.macro ZLOAD_Y_1
    vld $vr10, Y, 0
.endm

.macro ZGEMV_N_2x2
    GLD_INC v, , 0x10,       \
    A0,  PA0, 0, A1,  PA0, 0, \
    A2,  PA1, 0, A3,  PA1, 0
    GCOMPLEXMADD GXCONJ, GCONJ, \
    vf, d, Y0, X0, A0,  Y0, TMP0, TMP1, TMP2, Y1, X0, A1,  Y1, TMP0, TMP1, TMP2, \
    Y0, X1, A2,  Y0, TMP0, TMP1, TMP2, Y1, X1, A3,  Y1, TMP0, TMP1, TMP2
.endm

.macro ZGEMV_N_1x2
    GLD_INC v, , 0x10, $vr12, PA0, 0, $vr14, PA1, 0
    GCOMPLEXMADD GXCONJ, GCONJ, \
    vf, d, Y0, X0,  A0, Y0, TMP0, TMP1, TMP2, \
    Y0, X1, A2,  Y0, TMP0, TMP1, TMP2
.endm

.macro ZGEMV_N_1x1
    GLD_INC v, , 0x10, $vr12, PA0, 0
    GCOMPLEXMADD GXCONJ, GCONJ, \
    vf, d, Y0, X0,  A0, Y0, TMP0, TMP1, TMP2
.endm

.macro ZSTORE_Y_2
    GST v, , Y0, Y, 0, Y1, Y, 0x10
.endm

.macro ZSTORE_Y_2_GAP
    vst  Y0, Y, 0
    vstx Y1, Y, INC_Y
.endm

.macro ZSTORE_Y_1
    vst $vr10, Y, 0
.endm

.macro ZGEMV_N_LSX XW:req, X_2:req, X_1:req, Y_2:req, Y_1:req
    PTR_SRLI  J,      N,      1
    beqz      J,      .L_\XW\()_N_1
    PTR_SLLI  K_LDA,  LDA,    1
    PTR_SUB   K_LDA,  K_LDA,  M16
.L_\XW\()_N_L2:
    ZLOAD_\X_2
    xor     K,      K,      K
    move    Y,      Y_ORG
    PTR_SRLI  I,      M,       1
    beqz      I,      .L_\XW\()_M_1
.align 5
.L_\XW\()_M_L2:
    ZLOAD_\Y_2
    ZGEMV_N_2x2
    ZSTORE_\Y_2
    PTR_ADDI    I,      I,      -1
    PTR_ALSL    Y,      INC_Y,  Y,  1
    PTR_ADDI    K,      K,      4
    bnez        I,      .L_\XW\()_M_L2
.L_\XW\()_M_1:
    andi        I,      M,      1
    beqz        I,      .L_\XW\()_M_END
.align 5
.L_\XW\()_M_L1:
    ZLOAD_\Y_1
    ZGEMV_N_1x2
    ZSTORE_\Y_1
    PTR_ADDI    I,      I,      -1
    PTR_ADD     Y,      Y,      INC_Y
    PTR_ADDI    K,      K,      1
    bnez        I,      .L_\XW\()_M_L1
.L_\XW\()_M_END:
    PTR_ADDI    J,      J,      -1
#if __loongarch_grlen == 64
    GADD , d, PA0, PA0, K_LDA, PA1, PA1, K_LDA
#elif __loongarch_grlen == 32
    GADD , w, PA0, PA0, K_LDA, PA1, PA1, K_LDA
#else
    GADD , d, PA0, PA0, K_LDA, PA1, PA1, K_LDA
#endif
    PTR_ALSL    X,      INC_X,  X,  1
    bnez        J,      .L_\XW\()_N_L2
.L_\XW\()_N_1:
    andi        J,      N,      1
    beqz        J,      .L_END
.L_\XW\()_N_L1:
    ZLOAD_\X_1
    xor     K,      K,      K
    move    Y,      Y_ORG
    move    I,      M
    beqz    I,      .L_END
.align 5
.L_\XW\()_N_1_M_L1:
    ZLOAD_\Y_1
    ZGEMV_N_1x1
    ZSTORE_\Y_1
    PTR_ADDI  I,      I,      -1
    PTR_ADD   Y,      Y,      INC_Y
    PTR_ADDI  K,      K,      1
    bnez    I,      .L_\XW\()_N_1_M_L1
.L_\XW\()_N_1_M_END:
    PTR_ADDI    J,      J,      -1
    PTR_SUB     K_LDA,  LDA,    M16
    PTR_ADD     PA0,    PA0,    K_LDA
    PTR_ADD     X,      X,      INC_X
    bnez        J,      .L_\XW\()_N_L1

    b .L_END
.endm

    PROLOGUE
    PTR_LD     INC_Y,  $sp,    0
    push_if_used 7, 7
    PTR_ADDI   K,      $r0,     0x01
    PTR_SUB    I,      INC_X,   K
    PTR_SUB    J,      INC_Y,   K
    maskeqz    I,      K,       I  /* if(inc_x == 1) I = 0; else I = 1; */
    maskeqz    J,      K,       J  /* if(inc_y == 1) j = 0; else j = 1; */
    PTR_ALSL   I,      I,       J,      1
    GSLLI , d, LDA, LDA, 4, INC_X, INC_X, 4, INC_Y, INC_Y, 4, M16, M, 4
    // Init VALPHA
    vpackev.d      VALPHA,   $vr1,   $vr0
    move     Y_ORG,  Y
    move     PA0,    A
#if __loongarch_grlen == 64
    GADD , d, PA1, PA0, LDA
#elif __loongarch_grlen == 32
    GADD , w, PA1, PA0, LDA
#else
    GADD , d, PA1, PA0, LDA
#endif
    la.local    T0,     .L_GAP_TABLE
    PTR_ALSL    I,      I,      T0,     1
    ld.h        K,      I,      0 // Obtain the offset address
    PTR_ADD     T0,     T0,     K
    jirl        $r0,    T0,     0
.L_GAP_TABLE:
    .hword  .L_GAP_0_0 - .L_GAP_TABLE
    .hword  .L_GAP_0_1 - .L_GAP_TABLE
    .hword  .L_GAP_1_0 - .L_GAP_TABLE
    .hword  .L_GAP_1_1 - .L_GAP_TABLE
.L_GAP_0_0: /* if (inc_x == 1) && (incy == 1) */
    ZGEMV_N_LSX GAP_0_0, X_2, X_1, Y_2, Y_1
.L_GAP_0_1: /* if (inc_x == 1) && (incy != 1) */
    ZGEMV_N_LSX GAP_0_1, X_2, X_1, Y_2_GAP, Y_1
.L_GAP_1_0: /* if (inc_x != 1) && (incy == 1) */
    ZGEMV_N_LSX GAP_1_0, X_2_GAP, X_1, Y_2, Y_1
.L_GAP_1_1: /* if (inc_x != 1) && (incy != 1) */
    ZGEMV_N_LSX GAP_1_1, X_2_GAP, X_1, Y_2_GAP, Y_1
.L_END:
    pop_if_used 7, 7
    jirl    $r0, $r1, 0x0
    EPILOGUE
