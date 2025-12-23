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
#define M8      $r18
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

.macro CLOAD_X_8
    GLDREPL xv, d, X0, X, 0x00, X1, X, 0x08, X2, X, 0x10, X3, X, 0x18, \
                   X4, X, 0x20, X5, X, 0x28, X6, X, 0x30, X7, X, 0x38
    GCOMPLEXMUL GXCONJ, \
    xvf, s, X0, VALPHA, X0, TMP0, TMP1, TMP2, \
    X1, VALPHA, X1, TMP0, TMP1, TMP2, \
    X2, VALPHA, X2, TMP0, TMP1, TMP2, \
    X3, VALPHA, X3, TMP0, TMP1, TMP2, \
    X4, VALPHA, X4, TMP0, TMP1, TMP2, \
    X5, VALPHA, X5, TMP0, TMP1, TMP2, \
    X6, VALPHA, X6, TMP0, TMP1, TMP2, \
    X7, VALPHA, X7, TMP0, TMP1, TMP2
.endm

.macro CLOAD_X_8_GAP
    xvldrepl.d  X0,     X,      0x00
    PTR_ADD     T0,     X,      INC_X
    xvldrepl.d  X1,     T0,     0x00
    PTR_ADD     T0,     T0,     INC_X
    xvldrepl.d  X2,     T0,     0x00
    PTR_ADD     T0,     T0,     INC_X
    xvldrepl.d  X3,     T0,     0x00
    PTR_ADD     T0,     T0,     INC_X
    xvldrepl.d  X4,     T0,     0x00
    PTR_ADD     T0,     T0,     INC_X
    xvldrepl.d  X5,     T0,     0x00
    PTR_ADD     T0,     T0,     INC_X
    xvldrepl.d  X6,     T0,     0x00
    PTR_ADD     T0,     T0,     INC_X
    xvldrepl.d  X7,     T0,     0x00

    GCOMPLEXMUL GXCONJ, \
    xvf, s, X0, VALPHA, X0, TMP0, TMP1, TMP2, \
    X1, VALPHA, X1, TMP0, TMP1, TMP2, \
    X2, VALPHA, X2, TMP0, TMP1, TMP2, \
    X3, VALPHA, X3, TMP0, TMP1, TMP2, \
    X4, VALPHA, X4, TMP0, TMP1, TMP2, \
    X5, VALPHA, X5, TMP0, TMP1, TMP2, \
    X6, VALPHA, X6, TMP0, TMP1, TMP2, \
    X7, VALPHA, X7, TMP0, TMP1, TMP2
.endm

.macro CLOAD_Y_8
    GLD xv, , Y0, Y, 0, Y1, Y, 0x20
.endm

.macro CLOAD_Y_8_GAP
    fld.d   $f10,   Y,  0
    fldx.d  $f13,   Y,  INC_Y
    PTR_ALSL  T0,   INC_Y,  Y,  1
    fld.d   $f14,   T0, 0
    fldx.d  $f15,   T0, INC_Y
    PTR_ALSL  T0,   INC_Y,  Y,  2
    fld.d   $f11,   T0, 0
    fldx.d  $f17,   T0, INC_Y
    PTR_ADD   T0,   T0, INC_Y
    PTR_ADD   T0,   T0, INC_Y
    fld.d   $f18,   T0, 0
    fldx.d  $f19,   T0, INC_Y
    GINSVE0 xv, d, Y0, A1, 1, Y0, A2, 2, Y0, A3, 3, Y1, A5, 1, Y1, A6, 2, Y1, A7, 3
.endm

.macro CSTORE_Y_8_GAP
    xvstelm.d   Y0,     Y,      0,      0
    PTR_ADD     T0,     Y,      INC_Y
    xvstelm.d   Y0,     T0,     0,      1
    PTR_ADD     T0,     T0,     INC_Y
    xvstelm.d   Y0,     T0,     0,      2
    PTR_ADD     T0,     T0,     INC_Y
    xvstelm.d   Y0,     T0,     0,      3

    PTR_ADD     T0,     T0,     INC_Y
    xvstelm.d   Y1,     T0,     0,      0
    PTR_ADD     T0,     T0,     INC_Y
    xvstelm.d   Y1,     T0,     0,      1
    PTR_ADD     T0,     T0,     INC_Y
    xvstelm.d   Y1,     T0,     0,      2
    PTR_ADD     T0,     T0,     INC_Y
    xvstelm.d   Y1,     T0,     0,      3
.endm

.macro CGEMV_N_8x8
    GLD_INC xv, , 0x20,       \
    A0,  PA0, 0, A1,  PA0, 0, \
    A2,  PA1, 0, A3,  PA1, 0, \
    A4,  PA2, 0, A5,  PA2, 0, \
    A6,  PA3, 0, A7,  PA3, 0, \
    A8,  PA4, 0, A9,  PA4, 0, \
    A10, PA5, 0, A11, PA5, 0, \
    A12, PA6, 0, A13, PA6, 0, \
    A14, PA7, 0, A15, PA7, 0

    GCOMPLEXMADD GXCONJ, GCONJ, \
    xvf, s, Y0, X0, A0,  Y0, TMP0, TMP1, TMP2, Y1, X0, A1,  Y1, TMP0, TMP1, TMP2, \
    Y0, X1, A2,  Y0, TMP0, TMP1, TMP2, Y1, X1, A3,  Y1, TMP0, TMP1, TMP2, \
    Y0, X2, A4,  Y0, TMP0, TMP1, TMP2, Y1, X2, A5,  Y1, TMP0, TMP1, TMP2, \
    Y0, X3, A6,  Y0, TMP0, TMP1, TMP2, Y1, X3, A7,  Y1, TMP0, TMP1, TMP2, \
    Y0, X4, A8,  Y0, TMP0, TMP1, TMP2, Y1, X4, A9,  Y1, TMP0, TMP1, TMP2, \
    Y0, X5, A10, Y0, TMP0, TMP1, TMP2, Y1, X5, A11, Y1, TMP0, TMP1, TMP2, \
    Y0, X6, A12, Y0, TMP0, TMP1, TMP2, Y1, X6, A13, Y1, TMP0, TMP1, TMP2, \
    Y0, X7, A14, Y0, TMP0, TMP1, TMP2, Y1, X7, A15, Y1, TMP0, TMP1, TMP2
.endm

.macro CSTORE_Y_8
    GST xv, , Y0, Y, 0, Y1, Y, 0x20
.endm

.macro CLOAD_X_1
    GLDREPL xv, d, X0, X, 0x00
    GCOMPLEXMUL GXCONJ, \
    xvf, s, X0, VALPHA, X0, TMP0, TMP1, TMP2
.endm

.macro CLOAD_Y_1
    fld.d  $f10,   Y,  0
.endm

.macro CGEMV_N_1x8
    GLD_INC f, d, 0x08, $f12, PA0, 0, $f14, PA1, 0, $f16, PA2, 0, $f18, PA3, 0, \
                        $f20, PA4, 0, $f22, PA5, 0, $f24, PA6, 0, $f26, PA7, 0
    GCOMPLEXMADD GXCONJ, GCONJ, \
    xvf, s, Y0, X0,  A0, Y0, TMP0, TMP1, TMP2, \
    Y0, X1, A2,  Y0, TMP0, TMP1, TMP2, \
    Y0, X2, A4,  Y0, TMP0, TMP1, TMP2, \
    Y0, X3, A6,  Y0, TMP0, TMP1, TMP2, \
    Y0, X4, A8,  Y0, TMP0, TMP1, TMP2, \
    Y0, X5, A10, Y0, TMP0, TMP1, TMP2, \
    Y0, X6, A12, Y0, TMP0, TMP1, TMP2, \
    Y0, X7, A14, Y0, TMP0, TMP1, TMP2
.endm

.macro CSTORE_Y_1
    fst.d   $f10,   Y,  0
.endm

.macro CGEMV_N_1x1
    fld.d   $f12,    PA0,    0
    PTR_ADDI PA0,   PA0,    0x08
    GCOMPLEXMADD GXCONJ, GCONJ, \
    xvf, s, Y0, X0, A0, Y0, TMP0, TMP1, TMP2
.endm

.macro CGEMV_N_LASX XW:req, X_8:req, X_1:req, Y_8:req, Y_1:req
    PTR_SRLI  J,      N,      3
    beqz      J,      .L_\XW\()_N_7
    PTR_SLLI  K_LDA,  LDA,    3
    PTR_SUB   K_LDA,  K_LDA,  M8
.L_\XW\()_N_L8:
    CLOAD_\X_8
    xor     K,      K,      K
    move    Y,      Y_ORG
    PTR_SRLI  I,      M,       3
    beqz      I,      .L_\XW\()_M_7
.align 5
.L_\XW\()_M_L8:
    CLOAD_\Y_8
    CGEMV_N_8x8
    CSTORE_\Y_8
    PTR_ADDI    I,      I,      -1
    PTR_ALSL    Y,      INC_Y,  Y,  3
    PTR_ADDI    K,      K,      8
    bnez        I,      .L_\XW\()_M_L8
.L_\XW\()_M_7:
    andi        I,      M,      7
    beqz        I,      .L_\XW\()_M_END
.align 5
.L_\XW\()_M_L1:
    CLOAD_\Y_1
    CGEMV_N_1x8
    CSTORE_\Y_1
    PTR_ADDI    I,      I,      -1
    PTR_ADD     Y,      Y,      INC_Y
    PTR_ADDI    K,      K,      1
    bnez        I,      .L_\XW\()_M_L1
.L_\XW\()_M_END:
    PTR_ADDI    J,      J,      -1
#if __loongarch_grlen == 64
    GADD , d, PA0, PA0, K_LDA, PA1, PA1, K_LDA, PA2, PA2, K_LDA, PA3, PA3, K_LDA, \
              PA4, PA4, K_LDA, PA5, PA5, K_LDA, PA6, PA6, K_LDA, PA7, PA7, K_LDA
#elif __loongarch_grlen == 32
    GADD , w, PA0, PA0, K_LDA, PA1, PA1, K_LDA, PA2, PA2, K_LDA, PA3, PA3, K_LDA, \
              PA4, PA4, K_LDA, PA5, PA5, K_LDA, PA6, PA6, K_LDA, PA7, PA7, K_LDA
#else
    GADD , d, PA0, PA0, K_LDA, PA1, PA1, K_LDA, PA2, PA2, K_LDA, PA3, PA3, K_LDA, \
              PA4, PA4, K_LDA, PA5, PA5, K_LDA, PA6, PA6, K_LDA, PA7, PA7, K_LDA
#endif
    PTR_ALSL    X,      INC_X,  X,  3
    bnez        J,      .L_\XW\()_N_L8
.L_\XW\()_N_7:
    andi        J,      N,      7
    beqz        J,      .L_END
.L_\XW\()_N_L1:
    CLOAD_\X_1
    xor     K,      K,      K
    move    Y,      Y_ORG
    move    I,      M
    beqz    I,      .L_END
.align 5
.L_\XW\()_N_1_M_L1:
    CLOAD_\Y_1
    CGEMV_N_1x1
    CSTORE_\Y_1
    PTR_ADDI  I,      I,      -1
    PTR_ADD   Y,      Y,      INC_Y
    PTR_ADDI  K,      K,      1
    bnez    I,      .L_\XW\()_N_1_M_L1
.L_\XW\()_N_1_M_END:
    PTR_ADDI    J,      J,      -1
    PTR_SUB     K_LDA,  LDA,    M8
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
    GSLLI , d, LDA, LDA, 3, INC_X, INC_X, 3, INC_Y, INC_Y, 3, M8, M, 3
    // Init VALPHA
    xvpackev.w      $xr0,   $xr1,   $xr0
    xvreplve0.d     VALPHA, $xr0
    move     Y_ORG,  Y
    move     PA0,    A
#if __loongarch_grlen == 64
    GADD , d, PA1, PA0, LDA, PA2, PA1, LDA, PA3, PA2, LDA, PA4, PA3, LDA, \
              PA5, PA4, LDA, PA6, PA5, LDA, PA7, PA6, LDA
#elif __loongarch_grlen == 32
    GADD , w, PA1, PA0, LDA, PA2, PA1, LDA, PA3, PA2, LDA, PA4, PA3, LDA, \
              PA5, PA4, LDA, PA6, PA5, LDA, PA7, PA6, LDA
#else
    GADD , d, PA1, PA0, LDA, PA2, PA1, LDA, PA3, PA2, LDA, PA4, PA3, LDA, \
              PA5, PA4, LDA, PA6, PA5, LDA, PA7, PA6, LDA
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
    CGEMV_N_LASX GAP_0_0, X_8, X_1, Y_8, Y_1
.L_GAP_0_1: /* if (inc_x == 1) && (incy != 1) */
    CGEMV_N_LASX GAP_0_1, X_8, X_1, Y_8_GAP, Y_1
.L_GAP_1_0: /* if (inc_x != 1) && (incy == 1) */
    CGEMV_N_LASX GAP_1_0, X_8_GAP, X_1, Y_8, Y_1
.L_GAP_1_1: /* if (inc_x != 1) && (incy != 1) */
    CGEMV_N_LASX GAP_1_1, X_8_GAP, X_1, Y_8_GAP, Y_1
.L_END:
    pop_if_used 7, 7
    jirl    $r0, $r1, 0x0
    EPILOGUE
