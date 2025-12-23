/*******************************************************************************
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
*******************************************************************************/
#define ASSEMBLER

#include "common.h"
#include "loongarch64_asm.S"

/*********************************************************************
* 2023/08/30 guxiwei
*        UTEST                  : OK
*        CTEST                  : OK
*        TEST                   : OK
*
*
*********************************************************************/

/* int CNAME(BLASLONG m, BLASLONG n, BLASLONG dummy1, FLOAT alpha,
 * FLOAT *a, BLASLONG lda, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *buffer)
 */
#define M       $r4
#define N       $r5
#define ALPHA   $f0
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
#define M4      $r18
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
#define A0      $xr11
#define A1      $xr12
#define A2      $xr13
#define A3      $xr14
#define A4      $xr15
#define A5      $xr16
#define A6      $xr17
#define A7      $xr18

#define X0_F    $f2
#define X1_F    $f3
#define X2_F    $f4
#define X3_F    $f5
#define X4_F    $f6
#define X5_F    $f7
#define X6_F    $f8
#define X7_F    $f9
#define Y0_F    $f10
#define A0_F    $f11
#define A1_F    $f12
#define A2_F    $f13
#define A3_F    $f14
#define A4_F    $f15
#define A5_F    $f16
#define A6_F    $f17
#define A7_F    $f18

.macro SLOAD_X_8
    GLDREPL xv, w, X0, X, 0x00, X1, X, 0x04, X2, X, 0x08, X3, X, 0x0C, \
                   X4, X, 0x10, X5, X, 0x14, X6, X, 0x18, X7, X, 0x1C
    GMUL xvf, s, X0, X0, VALPHA, X1, X1, VALPHA, X2, X2, VALPHA, X3, X3, VALPHA, \
                 X4, X4, VALPHA, X5, X5, VALPHA, X6, X6, VALPHA, X7, X7, VALPHA
.endm

.macro SLOAD_X_8_GAP
    xvldrepl.w  X0,     X,      0x00
    PTR_ADD     T0,     X,      INC_X
    xvldrepl.w  X1,     T0,     0x00
    PTR_ADD     T0,     T0,     INC_X
    xvldrepl.w  X2,     T0,     0x00
    PTR_ADD     T0,     T0,     INC_X
    xvldrepl.w  X3,     T0,     0x00
    PTR_ADD     T0,     T0,     INC_X
    xvldrepl.w  X4,     T0,     0x00
    PTR_ADD     T0,     T0,     INC_X
    xvldrepl.w  X5,     T0,     0x00
    PTR_ADD     T0,     T0,     INC_X
    xvldrepl.w  X6,     T0,     0x00
    PTR_ADD     T0,     T0,     INC_X
    xvldrepl.w  X7,     T0,     0x00
    GMUL xvf, s, X0, X0, VALPHA, X1, X1, VALPHA, X2, X2, VALPHA, X3, X3, VALPHA, \
                 X4, X4, VALPHA, X5, X5, VALPHA, X6, X6, VALPHA, X7, X7, VALPHA
.endm

.macro SLOAD_X_4
    GLDREPL xv, w, X0, X, 0x00, X1, X, 0x04, X2, X, 0x08, X3, X, 0x0C
    GMUL xvf, s, X0, X0, VALPHA, X1, X1, VALPHA, X2, X2, VALPHA, X3, X3, VALPHA
.endm

.macro SLOAD_X_4_GAP
    xvldrepl.w  X0,     X,      0x00
    PTR_ADD     T0,     X,      INC_X
    xvldrepl.w  X1,     T0,     0x00
    PTR_ADD     T0,     T0,     INC_X
    xvldrepl.w  X2,     T0,     0x00
    PTR_ADD     T0,     T0,     INC_X
    xvldrepl.w  X3,     T0,     0x00
    GMUL xvf, s, X0, X0, VALPHA, X1, X1, VALPHA, X2, X2, VALPHA, X3, X3, VALPHA
.endm

.macro SLOAD_X_2
    GLDREPL xv, w, X0, X, 0x00, X1, X, 0x04
    GMUL xvf, s, X0, X0, VALPHA, X1, X1, VALPHA
.endm

.macro SLOAD_X_2_GAP
    xvldrepl.w  X0,     X,      0x00
    PTR_ADD     T0,     X,      INC_X
    xvldrepl.w  X1,     T0,     0x00
    GMUL xvf, s, X0, X0, VALPHA, X1, X1, VALPHA
.endm

.macro SLOAD_X_1
    GLDREPL xv, w, X0, X, 0x00
    GMUL xvf, s, X0, X0, VALPHA
.endm

.macro SLOAD_Y_8
    GLD xv, , Y0, Y, 0
.endm

.macro SLOAD_Y_8_GAP
    fld.s   Y0_F,   Y,  0
    fldx.s  A0_F,   Y,  INC_Y
    PTR_ALSL  T0,   INC_Y,  Y,  1
    fld.s   A1_F,   T0, 0
    fldx.s  A2_F,   T0, INC_Y
    PTR_ALSL  T0,   INC_Y,  Y,  2
    fld.s   A3_F,   T0, 0
    fldx.s  A4_F,   T0, INC_Y
    PTR_ADD   T0,   T0, INC_Y
    PTR_ADD   T0,   T0, INC_Y
    fld.s   A5_F,   T0, 0
    fldx.s  A6_F,   T0, INC_Y
    GINSVE0 xv, w, Y0, A0, 1, Y0, A1, 2, Y0, A2, 3, Y0, A3, 4, \
                   Y0, A4, 5, Y0, A5, 6, Y0, A6, 7
.endm

.macro SLOAD_Y_1
    GLD f, s, Y0_F, Y, 0
.endm

.macro SGEMV_N_8x8
    GLD_INC xv, , 0x20,             \
            A0, PA0, 0, A1, PA1, 0, \
            A2, PA2, 0, A3, PA3, 0, \
            A4, PA4, 0, A5, PA5, 0, \
            A6, PA6, 0, A7, PA7, 0
    GMADD xvf, s, Y0, A0, X0, Y0, Y0, A1, X1, Y0, \
                  Y0, A2, X2, Y0, Y0, A3, X3, Y0, \
                  Y0, A4, X4, Y0, Y0, A5, X5, Y0, \
                  Y0, A6, X6, Y0, Y0, A7, X7, Y0
.endm

.macro SGEMV_N_1x8
    GLD_INC f, s, 0x04,                 \
            A0_F, PA0, 0, A1_F, PA1, 0, \
            A2_F, PA2, 0, A3_F, PA3, 0, \
            A4_F, PA4, 0, A5_F, PA5, 0, \
            A6_F, PA6, 0, A7_F, PA7, 0
    GMADD f, s, Y0_F, A0_F, X0_F, Y0_F, Y0_F, A1_F, X1_F, Y0_F, \
                Y0_F, A2_F, X2_F, Y0_F, Y0_F, A3_F, X3_F, Y0_F, \
                Y0_F, A4_F, X4_F, Y0_F, Y0_F, A5_F, X5_F, Y0_F, \
                Y0_F, A6_F, X6_F, Y0_F, Y0_F, A7_F, X7_F, Y0_F
.endm

.macro SGEMV_N_8x4
    GLD_INC xv, , 0x20,             \
            A0, PA0, 0, A1, PA1, 0, \
            A2, PA2, 0, A3, PA3, 0
    GMADD xvf, s, Y0, A0, X0, Y0, Y0, A1, X1, Y0, \
                  Y0, A2, X2, Y0, Y0, A3, X3, Y0
.endm

.macro SGEMV_N_1x4
    GLD_INC f, s, 0x04,                 \
            A0_F, PA0, 0, A1_F, PA1, 0, \
            A2_F, PA2, 0, A3_F, PA3, 0
    GMADD f, s, Y0_F, A0_F, X0_F, Y0_F, Y0_F, A1_F, X1_F, Y0_F, \
                Y0_F, A2_F, X2_F, Y0_F, Y0_F, A3_F, X3_F, Y0_F
.endm

.macro SGEMV_N_8x2
    GLD_INC xv, , 0x20,             \
            A0, PA0, 0, A1, PA1, 0
    GMADD xvf, s, Y0, A0, X0, Y0, Y0, A1, X1, Y0
.endm

.macro SGEMV_N_1x2
    GLD_INC f, s, 0x04,             \
            A0_F, PA0, 0, A1_F, PA1, 0
    GMADD f, s, Y0_F, A0_F, X0_F, Y0_F, Y0_F, A1_F, X1_F, Y0_F
.endm

.macro SGEMV_N_1x1
    GLD_INC f, s, 0x04, A0_F, PA0, 0
    GMADD f, s, Y0_F, A0_F, X0_F, Y0_F
.endm

.macro SSTORE_Y_8
    GST xv, , Y0, Y, 0
.endm

.macro SSTORE_Y_8_GAP
    xvstelm.w   Y0,     Y,      0,      0
    PTR_ADD     T0,     Y,      INC_Y
    xvstelm.w   Y0,     T0,     0,      1
    PTR_ADD     T0,     T0,     INC_Y
    xvstelm.w   Y0,     T0,     0,      2
    PTR_ADD     T0,     T0,     INC_Y
    xvstelm.w   Y0,     T0,     0,      3

    PTR_ADD     T0,     T0,     INC_Y
    xvstelm.w   Y0,     T0,     0,      4
    PTR_ADD     T0,     T0,     INC_Y
    xvstelm.w   Y0,     T0,     0,      5
    PTR_ADD     T0,     T0,     INC_Y
    xvstelm.w   Y0,     T0,     0,      6
    PTR_ADD     T0,     T0,     INC_Y
    xvstelm.w   Y0,     T0,     0,      7
.endm

.macro SSTORE_Y_1
    GST f, s, Y0_F, Y, 0
.endm

.macro SGEMV_N_LASX XW:req, X_8:req, X_4:req, X_2:req, X_1:req, Y_8:req, Y_4:req, Y_1:req
    PTR_SRLI  J,      N,      3
    beqz      J,      .L_\XW\()_N_7
    PTR_SLLI  K_LDA,  LDA,    3
    PTR_SUB   K_LDA,  K_LDA,  M4
.L_\XW\()_N_L8:
    SLOAD_\X_8
    xor     K,      K,      K
    move    Y,      Y_ORG
    PTR_SRLI  I,      M,       3
    beqz      I,      .L_\XW\()_M_7
.align 5
.L_\XW\()_M_L8:
    SLOAD_\Y_8
    SGEMV_N_8x8
    SSTORE_\Y_8
    PTR_ADDI    I,      I,      -1
    PTR_ALSL    Y,      INC_Y,  Y,  3
    PTR_ADDI    K,      K,      8
    bnez        I,      .L_\XW\()_M_L8
.L_\XW\()_M_7:
    andi        I,      M,      7
    beqz        I,      .L_\XW\()_M_END
.align 5
.L_\XW\()_M_L1:
    SLOAD_\Y_1
    SGEMV_N_1x8
    SSTORE_\Y_1
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
    andi        J,      N,      4
    beqz        J,      .L_\XW\()_N_3
    SLOAD_\X_4
    xor         K,      K,      K
    move        Y,      Y_ORG

    PTR_SRLI  I,      M,       3
    beqz      I,      .L_\XW\()_N_4_M_7
.align 5
.L_\XW\()_N_4_M_L8:
    SLOAD_\Y_8
    SGEMV_N_8x4
    SSTORE_\Y_8
    PTR_ADDI  I,      I,      -1
    PTR_ADDI  K,      K,      8
    PTR_ALSL  Y,      INC_Y,  Y,  3
    bnez    I,      .L_\XW\()_N_4_M_L8
.L_\XW\()_N_4_M_7:
    andi        I,      M,      7
    beqz        I,      .L_\XW\()_N_4_M_END
.align 5
.L_\XW\()_N_4_M_L1:
    SLOAD_\Y_1
    SGEMV_N_1x4
    SSTORE_\Y_1
    PTR_ADDI    I,      I,      -1
    PTR_ADD     Y,      Y,      INC_Y
    PTR_ADDI    K,      K,      1
    bnez        I,      .L_\XW\()_N_4_M_L1
.L_\XW\()_N_4_M_END:
    PTR_SLLI    K_LDA,  LDA,    2
    PTR_SUB     K_LDA,  K_LDA,  M4
#if __loongarch_grlen == 64
    GADD , d, PA0, PA0, K_LDA, PA1, PA1, K_LDA, PA2, PA2, K_LDA, PA3, PA3, K_LDA
#elif __loongarch_grlen == 32
    GADD , w PA0, PA0, K_LDA, PA1, PA1, K_LDA, PA2, PA2, K_LDA, PA3, PA3, K_LDA
#else
    GADD , d, PA0, PA0, K_LDA, PA1, PA1, K_LDA, PA2, PA2, K_LDA, PA3, PA3, K_LDA
#endif
    PTR_ALSL    X,      INC_X,  X,  2
.L_\XW\()_N_3:
    andi        J,      N,      2
    beqz        J,      .L_\XW\()_N_1
    SLOAD_\X_2
    xor         K,      K,      K
    move        Y,      Y_ORG
    PTR_SRLI    I,      M,       3
    beqz    I,      .L_\XW\()_N_2_M_7
.align 5
.L_\XW\()_N_2_M_L8:
    SLOAD_\Y_8
    SGEMV_N_8x2
    SSTORE_\Y_8
    PTR_ADDI  I,      I,      -1
    PTR_ADDI  K,      K,      8
    PTR_ALSL  Y,      INC_Y,  Y,  3
    bnez    I,      .L_\XW\()_N_2_M_L8
.L_\XW\()_N_2_M_7:
    andi    I,      M,      7
    beqz    I,      .L_\XW\()_N_2_M_END
.align 5
.L_\XW\()_N_2_M_L1:
    SLOAD_\Y_1
    SGEMV_N_1x2
    SSTORE_\Y_1
    PTR_ADDI  I,      I,      -1
    PTR_ADD   Y,      Y,      INC_Y
    PTR_ADDI  K,      K,      1
    bnez    I,      .L_\XW\()_N_2_M_L1
.L_\XW\()_N_2_M_END:
    PTR_SLLI    K_LDA,  LDA,    1
    PTR_SUB     K_LDA,  K_LDA,  M4
    PTR_ADD     PA0,    PA0,    K_LDA
    PTR_ADD     PA1,    PA1,    K_LDA
    PTR_ALSL    X,      INC_X,  X,  1
.L_\XW\()_N_1:
    andi    J,      N,      1
    beqz    J,      .L_END
    SLOAD_\X_1
    xor     K,      K,      K
    move    Y,      Y_ORG
    move    I,      M
    beqz    I,      .L_END
.align 5
.L_\XW\()_N_1_M_L1:
    SLOAD_\Y_1
    SGEMV_N_1x1
    SSTORE_\Y_1
    PTR_ADDI  I,      I,      -1
    PTR_ADD   Y,      Y,      INC_Y
    PTR_ADDI  K,      K,      1
    bnez    I,      .L_\XW\()_N_1_M_L1
    b .L_END
.endm

    PROLOGUE
    PTR_LD     INC_Y,  $sp,    0
    push_if_used 7, 0
    PTR_ADDI   K,      $r0,     0x01
    PTR_SUB    I,      INC_X,   K
    PTR_SUB    J,      INC_Y,   K
    maskeqz    I,      K,       I  /* if(inc_x == 1) I = 0; else I = 1; */
    maskeqz    J,      K,       J  /* if(inc_y == 1) j = 0; else j = 1; */
    PTR_ALSL   I,      I,       J,      1
    GSLLI , d, LDA, LDA, 2, INC_X, INC_X, 2, INC_Y, INC_Y, 2, M4, M, 2
    xvreplve0.w     VALPHA, $xr0
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
    ld.h        K,      I,      0
    PTR_ADD     T0,     T0,     K
    jirl        $r0,    T0,     0
.L_GAP_TABLE:
    .hword  .L_GAP_0_0 - .L_GAP_TABLE
    .hword  .L_GAP_0_1 - .L_GAP_TABLE
    .hword  .L_GAP_1_0 - .L_GAP_TABLE
    .hword  .L_GAP_1_1 - .L_GAP_TABLE
.L_GAP_0_0: /* if (inc_x == 1) && (incy == 1) */
    SGEMV_N_LASX GAP_0_0, X_8, X_4, X_2, X_1, Y_8, Y_4, Y_1
.L_GAP_0_1: /* if (inc_x == 1) && (incy != 1) */
    SGEMV_N_LASX GAP_0_1, X_8, X_4, X_2, X_1, Y_8_GAP, Y_4_GAP, Y_1
.L_GAP_1_0: /* if (inc_x != 1) && (incy == 1) */
    SGEMV_N_LASX GAP_1_0, X_8_GAP, X_4_GAP, X_2_GAP, X_1, Y_8, Y_4, Y_1
.L_GAP_1_1: /* if (inc_x != 1) && (incy != 1) */
    SGEMV_N_LASX GAP_1_1, X_8_GAP, X_4_GAP, X_2_GAP, X_1, Y_8_GAP, Y_4_GAP, Y_1
.L_END:
    pop_if_used 7, 0
    jirl    $r0, $r1, 0x0
    EPILOGUE
