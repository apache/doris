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
* 2023/07/14 guxiwei
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

.macro DLOAD_X_8
    GLDREPL xv, d, X0, X, 0x00, X1, X, 0x08, X2, X, 0x10, X3, X, 0x18, \
                   X4, X, 0x20, X5, X, 0x28, X6, X, 0x30, X7, X, 0x38
    GMUL xvf, d, X0, X0, VALPHA, X1, X1, VALPHA, X2, X2, VALPHA, X3, X3, VALPHA, \
                 X4, X4, VALPHA, X5, X5, VALPHA, X6, X6, VALPHA, X7, X7, VALPHA
.endm

.macro DLOAD_X_4
    GLDREPL xv, d, X0, X, 0x00, X1, X, 0x08, X2, X, 0x10, X3, X, 0x18
    GMUL xvf, d, X0, X0, VALPHA, X1, X1, VALPHA, X2, X2, VALPHA, X3, X3, VALPHA
.endm

.macro DLOAD_X_2
    GLDREPL xv, d, X0, X, 0x00, X1, X, 0x08
    GMUL xvf, d, X0, X0, VALPHA, X1, X1, VALPHA
.endm

.macro DLOAD_X_1
    GLDREPL xv, d, X0, X, 0x00
    GMUL xvf, d, X0, X0, VALPHA
.endm

.macro DLOAD_Y_8
    GLD xv, , Y0, Y, 0, Y1, Y, 0x20
.endm

.macro DLOAD_Y_4
    GLD xv, , Y0, Y, 0
.endm

.macro DLOAD_Y_1
    fld.d  $f10,   Y,  0
.endm

.macro DSTORE_Y_8
    GST xv, , Y0, Y, 0, Y1, Y, 0x20
.endm

.macro DSTORE_Y_4
    GST xv, , Y0, Y, 0
.endm

.macro DSTORE_Y_1
    fst.d   $f10,   Y,  0
.endm

// Unable to use vector load/store ins
.macro DLOAD_Y_8_GAP
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

.macro DLOAD_Y_4_GAP
    fld.d   $f10,   Y,  0
    fldx.d  $f13,   Y,  INC_Y
    PTR_ALSL  T0,   INC_Y,  Y,  1
    fld.d   $f14,   T0, 0
    fldx.d  $f15,   T0, INC_Y
    GINSVE0 xv, d, Y0, A1, 1, Y0, A2, 2, Y0, A3, 3
.endm

.macro DSTORE_Y_8_GAP
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

.macro DSTORE_Y_4_GAP
    xvstelm.d   Y0,     Y,      0,      0
    PTR_ADD     T0,     Y,      INC_Y
    xvstelm.d   Y0,     T0,     0,      1
    PTR_ADD     T0,     T0,     INC_Y
    xvstelm.d   Y0,     T0,     0,      2
    PTR_ADD     T0,     T0,     INC_Y
    xvstelm.d   Y0,     T0,     0,      3
.endm

.macro DLOAD_X_8_GAP
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
    GMUL xvf, d, X0, X0, VALPHA, X1, X1, VALPHA, X2, X2, VALPHA, X3, X3, VALPHA, \
                 X4, X4, VALPHA, X5, X5, VALPHA, X6, X6, VALPHA, X7, X7, VALPHA
.endm

.macro DLOAD_X_4_GAP
    xvldrepl.d  X0,     X,      0x00
    PTR_ADD     T0,     X,      INC_X
    xvldrepl.d  X1,     T0,     0x00
    PTR_ADD     T0,     T0,     INC_X
    xvldrepl.d  X2,     T0,     0x00
    PTR_ADD     T0,     T0,     INC_X
    xvldrepl.d  X3,     T0,     0x00
    GMUL xvf, d, X0, X0, VALPHA, X1, X1, VALPHA, X2, X2, VALPHA, X3, X3, VALPHA
.endm

.macro DLOAD_X_2_GAP
    xvldrepl.d  X0,     X,      0x00
    PTR_ADD     T0,     X,      INC_X
    xvldrepl.d  X1,     T0,     0x00
    GMUL xvf, d, X0, X0, VALPHA, X1, X1, VALPHA
.endm

.macro DGEMV_N_8x8
    GLD_INC xv, , 0x20,       \
    A0,  PA0, 0, A1,  PA0, 0, \
    A2,  PA1, 0, A3,  PA1, 0, \
    A4,  PA2, 0, A5,  PA2, 0, \
    A6,  PA3, 0, A7,  PA3, 0, \
    A8,  PA4, 0, A9,  PA4, 0, \
    A10, PA5, 0, A11, PA5, 0, \
    A12, PA6, 0, A13, PA6, 0, \
    A14, PA7, 0, A15, PA7, 0

    GMADD xvf, d, Y0, A0,  X0, Y0, Y1, A1,  X0, Y1, \
                  Y0, A2,  X1, Y0, Y1, A3,  X1, Y1, \
                  Y0, A4,  X2, Y0, Y1, A5,  X2, Y1, \
                  Y0, A6,  X3, Y0, Y1, A7,  X3, Y1, \
                  Y0, A8,  X4, Y0, Y1, A9,  X4, Y1, \
                  Y0, A10, X5, Y0, Y1, A11, X5, Y1, \
                  Y0, A12, X6, Y0, Y1, A13, X6, Y1, \
                  Y0, A14, X7, Y0, Y1, A15, X7, Y1
.endm

.macro DGEMV_N_4x8
    GLD_INC xv, , 0x20, A0,  PA0, 0, \
                        A2,  PA1, 0, \
                        A4,  PA2, 0, \
                        A6,  PA3, 0, \
                        A8,  PA4, 0, \
                        A10, PA5, 0, \
                        A12, PA6, 0, \
                        A14, PA7, 0

    GMADD xvf, d, Y0, A0,  X0, Y0, \
                  Y0, A2,  X1, Y0, \
                  Y0, A4,  X2, Y0, \
                  Y0, A6,  X3, Y0, \
                  Y0, A8,  X4, Y0, \
                  Y0, A10, X5, Y0, \
                  Y0, A12, X6, Y0, \
                  Y0, A14, X7, Y0
.endm

.macro DGEMV_N_1x8
    GLD_INC f, d, 0x08, $f12, PA0, 0, $f14, PA1, 0, $f16, PA2, 0, $f18, PA3, 0, \
                        $f20, PA4, 0, $f22, PA5, 0, $f24, PA6, 0, $f26, PA7, 0
    GMADD f, d, $f10, $f12, $f2, $f10, \
                $f10, $f14, $f3, $f10, \
                $f10, $f16, $f4, $f10, \
                $f10, $f18, $f5, $f10, \
                $f10, $f20, $f6, $f10, \
                $f10, $f22, $f7, $f10, \
                $f10, $f24, $f8, $f10, \
                $f10, $f26, $f9, $f10,
.endm

.macro DGEMV_N_8x4
    GLD_INC xv, , 0x20,       \
    A0,  PA0, 0, A1,  PA0, 0, \
    A2,  PA1, 0, A3,  PA1, 0, \
    A4,  PA2, 0, A5,  PA2, 0, \
    A6,  PA3, 0, A7,  PA3, 0

    GMADD xvf, d, Y0, A0,  X0, Y0, Y1, A1,  X0, Y1, \
                  Y0, A2,  X1, Y0, Y1, A3,  X1, Y1, \
                  Y0, A4,  X2, Y0, Y1, A5,  X2, Y1, \
                  Y0, A6,  X3, Y0, Y1, A7,  X3, Y1
.endm

.macro DGEMV_N_4x4
    GLD_INC xv, , 0x20, A0, PA0, 0, A2, PA1, 0, A4, PA2, 0, A6, PA3, 0

    GMADD xvf, d, Y0, A0,  X0, Y0, Y0, A2,  X1, Y0, \
                  Y0, A4,  X2, Y0, Y0, A6,  X3, Y0
.endm

.macro DGEMV_N_1x4
    GLD_INC f, d, 0x08, $f12, PA0, 0, $f14, PA1, 0, $f16, PA2, 0, $f18, PA3, 0
    GMADD f, d, $f10, $f12, $f2, $f10, $f10, $f14, $f3, $f10, \
                $f10, $f16, $f4, $f10, $f10, $f18, $f5, $f10
.endm

.macro DGEMV_N_8x2
    GLD_INC xv, , 0x20,       \
    A0,  PA0, 0, A1,  PA0, 0, \
    A2,  PA1, 0, A3,  PA1, 0
    GMADD xvf, d, Y0, A0,  X0, Y0, Y1, A1,  X0, Y1, \
                  Y0, A2,  X1, Y0, Y1, A3,  X1, Y1
.endm

.macro DGEMV_N_4x2
    GLD_INC xv, , 0x20, A0,  PA0, 0, A2,  PA1, 0
    GMADD xvf, d, Y0, A0,  X0, Y0, \
                  Y0, A2,  X1, Y0
.endm

.macro DGEMV_N_1x2
    GLD_INC f, d, 0x08, $f12, PA0, 0, $f14, PA1, 0
    GMADD f, d, $f10, $f12, $f2, $f10, \
                $f10, $f14, $f3, $f10
.endm

.macro DGEMV_N_1x1
    fld.d   $f12,    PA0,    0
    PTR_ADDI PA0,   PA0,    0x08
    fmadd.d $f10,   $f12,   $f2,    $f10
.endm

.macro DGEMV_N_LASX XW:req, X_8:req, X_4:req, X_2:req, X_1:req, Y_8:req, Y_4:req, Y_1:req
    PTR_SRLI  J,      N,      3
    beqz      J,      .L_\XW\()_N_7
    PTR_SLLI  K_LDA,  LDA,    3
    PTR_SUB   K_LDA,  K_LDA,  M8
.L_\XW\()_N_L8:
    DLOAD_\X_8
    xor     K,      K,      K
    move    Y,      Y_ORG
    PTR_SRLI  I,      M,       3
    beqz      I,      .L_\XW\()_M_7
.align 5
.L_\XW\()_M_L8:
    DLOAD_\Y_8
    DGEMV_N_8x8
    DSTORE_\Y_8
    PTR_ADDI    I,      I,      -1
    PTR_ALSL    Y,      INC_Y,  Y,  3
    PTR_ADDI    K,      K,      8
    bnez        I,      .L_\XW\()_M_L8
.L_\XW\()_M_7:
    andi        I,      M,      4
    beqz        I,      .L_\XW\()_M_3
    DLOAD_\Y_4
    DGEMV_N_4x8
    DSTORE_\Y_4
    PTR_ALSL    Y,      INC_Y,  Y,  2
    PTR_ADDI    K,      K,      4
.L_\XW\()_M_3:
    andi        I,      M,      3
    beqz        I,      .L_\XW\()_M_END
.align 5
.L_\XW\()_M_L1:
    DLOAD_\Y_1
    DGEMV_N_1x8
    DSTORE_\Y_1
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
    DLOAD_\X_4
    xor         K,      K,      K
    move        Y,      Y_ORG

    PTR_SRLI  I,      M,       3
    beqz      I,      .L_\XW\()_N_4_M_7
.align 5
.L_\XW\()_N_4_M_L8:
    DLOAD_\Y_8
    DGEMV_N_8x4
    DSTORE_\Y_8
    PTR_ADDI  I,      I,      -1
    PTR_ADDI  K,      K,      8
    PTR_ALSL  Y,      INC_Y,  Y,  3
    bnez    I,      .L_\XW\()_N_4_M_L8
.L_\XW\()_N_4_M_7:
    andi    I,      M,      4
    beqz    I,      .L_\XW\()_N_4_M_3
    DLOAD_\Y_4
    DGEMV_N_4x4
    DSTORE_\Y_4
    PTR_ALSL  Y,    INC_Y,  Y,  2
    PTR_ADDI  K,    K,      4
.L_\XW\()_N_4_M_3:
    andi        I,      M,      3
    beqz        I,      .L_\XW\()_N_4_M_END
.align 5
.L_\XW\()_N_4_M_L1:
    DLOAD_\Y_1
    DGEMV_N_1x4
    DSTORE_\Y_1
    PTR_ADDI    I,      I,      -1
    PTR_ADD     Y,      Y,      INC_Y
    PTR_ADDI    K,      K,      1
    bnez        I,      .L_\XW\()_N_4_M_L1
.L_\XW\()_N_4_M_END:
    PTR_SLLI    K_LDA,  LDA,    2
    PTR_SUB     K_LDA,  K_LDA,  M8
#if __loongarch_grlen == 64
    GADD , d, PA0, PA0, K_LDA, PA1, PA1, K_LDA, PA2, PA2, K_LDA, PA3, PA3, K_LDA
#elif __loongarch_grlen == 32
    GADD , w, PA0, PA0, K_LDA, PA1, PA1, K_LDA, PA2, PA2, K_LDA, PA3, PA3, K_LDA
#else
    GADD , d, PA0, PA0, K_LDA, PA1, PA1, K_LDA, PA2, PA2, K_LDA, PA3, PA3, K_LDA
#endif
    PTR_ALSL    X,      INC_X,  X,  2
.L_\XW\()_N_3:
    andi        J,      N,      2
    beqz        J,      .L_\XW\()_N_1
    DLOAD_\X_2
    xor         K,      K,      K
    move        Y,      Y_ORG
    PTR_SRLI    I,      M,       3
    beqz    I,      .L_\XW\()_N_2_M_7
.align 5
.L_\XW\()_N_2_M_L8:
    DLOAD_\Y_8
    DGEMV_N_8x2
    DSTORE_\Y_8
    PTR_ADDI  I,      I,      -1
    PTR_ADDI  K,      K,      8
    PTR_ALSL  Y,      INC_Y,  Y,  3
    bnez    I,      .L_\XW\()_N_2_M_L8
.L_\XW\()_N_2_M_7:
    andi    I,      M,      4
    beqz    I,      .L_\XW\()_N_2_M_3
    DLOAD_\Y_4
    DGEMV_N_4x2
    DSTORE_\Y_4
    PTR_ALSL  Y,        INC_Y,      Y,      2
    PTR_ADDI  K,        K,          4
.L_\XW\()_N_2_M_3:
    andi    I,      M,      3
    beqz    I,      .L_\XW\()_N_2_M_END
.align 5
.L_\XW\()_N_2_M_L1:
    DLOAD_\Y_1
    DGEMV_N_1x2
    DSTORE_\Y_1
    PTR_ADDI  I,      I,      -1
    PTR_ADD   Y,      Y,      INC_Y
    PTR_ADDI  K,      K,      1
    bnez    I,      .L_\XW\()_N_2_M_L1
.L_\XW\()_N_2_M_END:
    PTR_SLLI    K_LDA,  LDA,    1
    PTR_SUB     K_LDA,  K_LDA,  M8
    PTR_ADD     PA0,    PA0,    K_LDA
    PTR_ADD     PA1,    PA1,    K_LDA
    PTR_ALSL    X,      INC_X,  X,  1
.L_\XW\()_N_1:
    andi    J,      N,      1
    beqz    J,      .L_END
    DLOAD_\X_1
    xor     K,      K,      K
    move    Y,      Y_ORG
    move    I,      M
    beqz    I,      .L_END
.align 5
.L_\XW\()_N_1_M_L1:
    DLOAD_\Y_1
    DGEMV_N_1x1
    DSTORE_\Y_1
    PTR_ADDI  I,      I,      -1
    PTR_ADD   Y,      Y,      INC_Y
    PTR_ADDI  K,      K,      1
    bnez    I,      .L_\XW\()_N_1_M_L1
    b .L_END
.endm

    PROLOGUE
    PTR_LD     INC_Y,  $sp,    0
    push_if_used 7, 4
    PTR_ADDI   K,      $r0,     0x01
    PTR_SUB    I,      INC_X,   K
    PTR_SUB    J,      INC_Y,   K
    maskeqz    I,      K,       I  /* if(inc_x == 1) I = 0; else I = 1; */
    maskeqz    J,      K,       J  /* if(inc_y == 1) j = 0; else j = 1; */
    PTR_ALSL   I,      I,       J,      1
    GSLLI , d, LDA, LDA, 3, INC_X, INC_X, 3, INC_Y, INC_Y, 3, M8, M, 3
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
    ld.h        K,      I,      0
    PTR_ADD     T0,     T0,     K
    jirl        $r0,    T0,     0
.L_GAP_TABLE:
    .hword  .L_GAP_0_0 - .L_GAP_TABLE
    .hword  .L_GAP_0_1 - .L_GAP_TABLE
    .hword  .L_GAP_1_0 - .L_GAP_TABLE
    .hword  .L_GAP_1_1 - .L_GAP_TABLE
.L_GAP_0_0: /* if (inc_x == 1) && (incy == 1) */
    DGEMV_N_LASX GAP_0_0, X_8, X_4, X_2, X_1, Y_8, Y_4, Y_1
.L_GAP_0_1: /* if (inc_x == 1) && (incy != 1) */
    DGEMV_N_LASX GAP_0_1, X_8, X_4, X_2, X_1, Y_8_GAP, Y_4_GAP, Y_1
.L_GAP_1_0: /* if (inc_x != 1) && (incy == 1) */
    DGEMV_N_LASX GAP_1_0, X_8_GAP, X_4_GAP, X_2_GAP, X_1, Y_8, Y_4, Y_1
.L_GAP_1_1: /* if (inc_x != 1) && (incy != 1) */
    DGEMV_N_LASX GAP_1_1, X_8_GAP, X_4_GAP, X_2_GAP, X_1, Y_8_GAP, Y_4_GAP, Y_1
.L_END:
    pop_if_used 7, 4
    jirl    $r0, $r1, 0x0
    EPILOGUE
