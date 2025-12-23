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

/*********************************************************************
* 2024/02/20 guxiwei
*        UTEST                  : OK
*        CTEST                  : OK
*        TEST                   : OK
*
*
*********************************************************************/

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

#define VALPHA  $xr1
#define X0      $xr2
#define X1      $xr3
#define X2      $xr4
#define X3      $xr5
#define X4      $xr6
#define X5      $xr7
#define X6      $xr8
#define X7      $xr9
#define Y0      $xr10
#define Y1      $xr11
#define A0      $xr12
#define A1      $xr13
#define A2      $xr14
#define A3      $xr15
#define A4      $xr16
#define A5      $xr17
#define A6      $xr18
#define A7      $xr19
#define A8      $xr20
#define A9      $xr21
#define A10     $xr22
#define A11     $xr23
#define A12     $xr24
#define A13     $xr25
#define A14     $xr26
#define A15     $xr27
#define TMP0    $xr28
#define TMP1    $xr29
#define TMP2    $xr30

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

.macro ZLOAD_X_4
    GLD xv, , X0, X, 0x00, X1, X, 0x10, X2, X, 0x20, X3, X, 0x30
    GPERMI xv, q, X0, X0, 0, X1, X1, 0, X2, X2, 0, X3, X3, 0
    GCOMPLEXMUL GXCONJ, \
    xvf, d, X0, VALPHA, X0, TMP0, TMP1, TMP2, \
    X1, VALPHA, X1, TMP0, TMP1, TMP2, \
    X2, VALPHA, X2, TMP0, TMP1, TMP2, \
    X3, VALPHA, X3, TMP0, TMP1, TMP2
.endm

.macro ZLOAD_X_4_GAP
    xvld        X0,     X,      0
    xvpermi.q   X0,     X0,     0

    PTR_ADD     T0,     X,      INC_X
    xvld        X1,     T0,     0
    xvpermi.q   X1,     X1,     0

    PTR_ADD     T0,     T0,     INC_X
    xvld        X2,     T0,     0
    xvpermi.q   X2,     X2,     0

    PTR_ADD     T0,     T0,     INC_X
    xvld        X3,     T0,     0
    xvpermi.q   X3,     X3,     0

    GCOMPLEXMUL GXCONJ, \
    xvf, d, X0, VALPHA, X0, TMP0, TMP1, TMP2, \
    X1, VALPHA, X1, TMP0, TMP1, TMP2, \
    X2, VALPHA, X2, TMP0, TMP1, TMP2, \
    X3, VALPHA, X3, TMP0, TMP1, TMP2
.endm

.macro ZLOAD_Y_4
    GLD xv, , Y0, Y, 0, Y1, Y, 0x20
.endm

.macro ZLOAD_Y_4_GAP
    vld  $vr10, Y, 0
    vldx $vr13, Y, INC_Y
    PTR_ALSL  T0,   INC_Y,  Y,  1
    vld  $vr11, T0, 0
    vldx $vr14, T0, INC_Y
    GPERMI xv, q, Y0, A1, 0x02, Y1, A2, 0x02
.endm

.macro ZGEMV_N_4x4
    GLD_INC xv, , 0x20,       \
    A0,  PA0, 0, A1,  PA0, 0, \
    A2,  PA1, 0, A3,  PA1, 0, \
    A4,  PA2, 0, A5,  PA2, 0, \
    A6,  PA3, 0, A7,  PA3, 0
    GCOMPLEXMADD GXCONJ, GCONJ, \
    xvf, d, Y0, X0, A0,  Y0, TMP0, TMP1, TMP2, Y1, X0, A1,  Y1, TMP0, TMP1, TMP2, \
    Y0, X1, A2,  Y0, TMP0, TMP1, TMP2, Y1, X1, A3,  Y1, TMP0, TMP1, TMP2, \
    Y0, X2, A4,  Y0, TMP0, TMP1, TMP2, Y1, X2, A5,  Y1, TMP0, TMP1, TMP2, \
    Y0, X3, A6,  Y0, TMP0, TMP1, TMP2, Y1, X3, A7,  Y1, TMP0, TMP1, TMP2
.endm

.macro ZSTORE_Y_4
    GST xv, , Y0, Y, 0, Y1, Y, 0x20
.endm

.macro ZSTORE_Y_4_GAP
    xvstelm.d   Y0,     Y,      0,      0
    xvstelm.d   Y0,     Y,      0x08,   1
    PTR_ADD     T0,     Y,      INC_Y
    xvstelm.d   Y0,     T0,     0,      2
    xvstelm.d   Y0,     T0,     0x08,   3
    PTR_ADD     T0,     T0,     INC_Y
    xvstelm.d   Y1,     T0,     0,      0
    xvstelm.d   Y1,     T0,     0x08,   1
    PTR_ADD     T0,     T0,     INC_Y
    xvstelm.d   Y1,     T0,     0,      2
    xvstelm.d   Y1,     T0,     0x08,   3
.endm

.macro ZLOAD_Y_1
    vld $vr10, Y, 0
.endm

.macro ZGEMV_N_1x4
    GLD_INC v, , 0x10, $vr12, PA0, 0, $vr14, PA1, 0, $vr16, PA2, 0, $vr18, PA3, 0
    GCOMPLEXMADD GXCONJ, GCONJ, \
    xvf, d, Y0, X0,  A0, Y0, TMP0, TMP1, TMP2, \
    Y0, X1, A2,  Y0, TMP0, TMP1, TMP2, \
    Y0, X2, A4,  Y0, TMP0, TMP1, TMP2, \
    Y0, X3, A6,  Y0, TMP0, TMP1, TMP2
.endm

.macro ZSTORE_Y_1
    vst $vr10, Y, 0
.endm

.macro ZLOAD_X_1
    GLD xv, , X0, X, 0x00
    GPERMI xv, q, X0, X0, 0
    GCOMPLEXMUL GXCONJ, \
    xvf, d, X0, VALPHA, X0, TMP0, TMP1, TMP2
.endm

.macro ZGEMV_N_1x1
    GLD_INC v, , 0x10, $vr12, PA0, 0
    GCOMPLEXMADD GXCONJ, GCONJ, \
    xvf, d, Y0, X0,  A0, Y0, TMP0, TMP1, TMP2
.endm

.macro ZGEMV_N_LASX XW:req, X_4:req, X_1:req, Y_4:req, Y_1:req
    PTR_SRLI  J,      N,      2
    beqz      J,      .L_\XW\()_N_3
    PTR_SLLI  K_LDA,  LDA,    2
    PTR_SUB   K_LDA,  K_LDA,  M16
.L_\XW\()_N_L4:
    ZLOAD_\X_4
    xor     K,      K,      K
    move    Y,      Y_ORG
    PTR_SRLI  I,      M,       2
    beqz      I,      .L_\XW\()_M_3
.align 5
.L_\XW\()_M_L4:
    ZLOAD_\Y_4
    ZGEMV_N_4x4
    ZSTORE_\Y_4
    PTR_ADDI    I,      I,      -1
    PTR_ALSL    Y,      INC_Y,  Y,  2
    PTR_ADDI    K,      K,      4
    bnez        I,      .L_\XW\()_M_L4
.L_\XW\()_M_3:
    andi        I,      M,      3
    beqz        I,      .L_\XW\()_M_END
.align 5
.L_\XW\()_M_L1:
    ZLOAD_\Y_1
    ZGEMV_N_1x4
    ZSTORE_\Y_1
    PTR_ADDI    I,      I,      -1
    PTR_ADD     Y,      Y,      INC_Y
    PTR_ADDI    K,      K,      1
    bnez        I,      .L_\XW\()_M_L1
.L_\XW\()_M_END:
    PTR_ADDI    J,      J,      -1
#if __loongarch_grlen == 64
    GADD , d, PA0, PA0, K_LDA, PA1, PA1, K_LDA, PA2, PA2, K_LDA, PA3, PA3, K_LDA
#elif __loongarch_grlen == 32
    GADD , w, PA0, PA0, K_LDA, PA1, PA1, K_LDA, PA2, PA2, K_LDA, PA3, PA3, K_LDA
#else
    GADD , d, PA0, PA0, K_LDA, PA1, PA1, K_LDA, PA2, PA2, K_LDA, PA3, PA3, K_LDA
#endif
    PTR_ALSL    X,      INC_X,  X,  2
    bnez        J,      .L_\XW\()_N_L4
.L_\XW\()_N_3:
    andi        J,      N,      3
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
    xvpackev.d      $xr0,   $xr1,   $xr0
    xvreplve0.q     VALPHA, $xr0
    move     Y_ORG,  Y
    move     PA0,    A
#if __loongarch_grlen == 64
    GADD , d, PA1, PA0, LDA, PA2, PA1, LDA, PA3, PA2, LDA
#elif __loongarch_grlen == 32
    GADD , w, PA1, PA0, LDA, PA2, PA1, LDA, PA3, PA2, LDA
#else
    GADD , d, PA1, PA0, LDA, PA2, PA1, LDA, PA3, PA2, LDA
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
    ZGEMV_N_LASX GAP_0_0, X_4, X_1, Y_4, Y_1
.L_GAP_0_1: /* if (inc_x == 1) && (incy != 1) */
    ZGEMV_N_LASX GAP_0_1, X_4, X_1, Y_4_GAP, Y_1
.L_GAP_1_0: /* if (inc_x != 1) && (incy == 1) */
    ZGEMV_N_LASX GAP_1_0, X_4_GAP, X_1, Y_4, Y_1
.L_GAP_1_1: /* if (inc_x != 1) && (incy != 1) */
    ZGEMV_N_LASX GAP_1_1, X_4_GAP, X_1, Y_4_GAP, Y_1
.L_END:
    pop_if_used 7, 7
    jirl    $r0, $r1, 0x0
    EPILOGUE

