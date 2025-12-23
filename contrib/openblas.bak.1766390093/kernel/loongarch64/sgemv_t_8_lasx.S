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
#define M4      $r30

#define VALPHA  $xr0
#define X0      $xr1
#define A0      $xr2
#define A1      $xr3
#define A2      $xr4
#define A3      $xr5
#define A4      $xr6
#define A5      $xr7
#define A6      $xr8
#define A7      $xr9
#define TP0     $xr10
#define TP1     $xr11
#define TP2     $xr12
#define TP3     $xr13
#define TP4     $xr14
#define TP5     $xr15
#define TP6     $xr16
#define TP7     $xr17
#define Y0      $xr2
#define Y1      $xr3
#define Y2      $xr4
#define Y3      $xr5
#define Y4      $xr6
#define Y5      $xr7
#define Y6      $xr8
#define Y7      $xr9

.macro ZERO_Y8
    GXOR xv, v, TP0, TP0, TP0, TP1, TP1, TP1, TP2, TP2, TP2, TP3, TP3, TP3, \
                TP4, TP4, TP4, TP5, TP5, TP5, TP6, TP6, TP6, TP7, TP7, TP7
.endm

.macro ZERO_Y4
    GXOR xv, v, TP0, TP0, TP0, TP1, TP1, TP1, TP2, TP2, TP2, TP3, TP3, TP3
.endm

.macro ZERO_Y2
    GXOR xv, v, TP0, TP0, TP0, TP1, TP1, TP1
.endm

.macro ZERO_Y1
    GXOR xv, v, TP0, TP0, TP0
.endm

.macro SLOAD_X8
    GLD xv, , X0, X, 0x00
.endm

.macro SLOAD_X8_GAP
    fld.s       $f1,    X,    0x00
    fldx.s      $f2,    X,    INC_X
    PTR_ALSL    T0,     INC_X,      X,      1
    fld.s       $f3,    T0,   0x00
    fldx.s      $f4,    T0,   INC_X
    GINSVE0 xv, w, X0, A0, 1, X0, A1, 2, X0, A2, 3
    PTR_ALSL    T0,     INC_X,      X,      2
    fld.s       $f2,    T0,   0x00
    fldx.s      $f3,    T0,   INC_X
    PTR_ALSL    T0,     INC_X,      T0,     1
    fld.s       $f4,    T0,   0x00
    fldx.s      $f5,    T0,   INC_X
    GINSVE0 xv, w, X0, A0, 4, X0, A1, 5, X0, A2, 6, X0, A3, 7
.endm

.macro SGEMV_T_8x8
    GLD_INC xv, , 0x20,             \
            A0, PA0, 0, A1, PA1, 0, \
            A2, PA2, 0, A3, PA3, 0, \
            A4, PA4, 0, A5, PA5, 0, \
            A6, PA6, 0, A7, PA7, 0
    GMADD xvf, s, TP0, A0, X0, TP0, TP1, A1, X0, TP1, \
                  TP2, A2, X0, TP2, TP3, A3, X0, TP3, \
                  TP4, A4, X0, TP4, TP5, A5, X0, TP5, \
                  TP6, A6, X0, TP6, TP7, A7, X0, TP7
.endm

.macro SGEMV_T_4x8
    GLD_INC xv, , 0x20,             \
            A0, PA0, 0, A1, PA1, 0, \
            A2, PA2, 0, A3, PA3, 0
    GMADD xvf, s, TP0, A0, X0, TP0, TP1, A1, X0, TP1, \
                  TP2, A2, X0, TP2, TP3, A3, X0, TP3
.endm

.macro SGEMV_T_2x8
    GLD_INC xv, , 0x20,             \
            A0, PA0, 0, A1, PA1, 0
    GMADD xvf, s, TP0, A0, X0, TP0, TP1, A1, X0, TP1
.endm

.macro SGEMV_T_LASX XW:req X8:req, X4:req
    PTR_SRLI  J,      N,      3
    beqz      J,      .L_\XW\()_N_7
    PTR_SLLI  K_LDA,  LDA,    3
    PTR_SUB   K_LDA,  K_LDA,  M4
.L_\XW\()_N_L8:
    ZERO_Y8
    move      X,      X_ORG
    PTR_SRLI  I,      M,       3
    beqz      I,      .L_\XW\()_M_7
.align 5
.L_\XW\()_M_L8:
    SLOAD_\X8
    SGEMV_T_8x8
    PTR_ADDI    I,  I,  -1
    PTR_ALSL    X,  INC_X,  X,  3
    bnez        I,  .L_\XW\()_M_L8
.L_\XW\()_M_7:
    // Accumulated
    GACC xvf, s, Y0, TP0, Y1, TP1, Y2, TP2, Y3, TP3, Y4, TP4, \
                 Y5, TP5, Y6, TP6, Y7, TP7
    andi        I,      M,      7
    beqz        I,      .L_\XW\()_M_END
.align 5
.L_\XW\()_M_L1:
    fld.s   $f1,    X,      0x00
    fld.s   $f10,   PA0,    0x00
    fld.s   $f11,   PA1,    0x00
    fld.s   $f12,   PA2,    0x00
    fld.s   $f13,   PA3,    0x00
    fld.s   $f14,   PA4,    0x00
    fld.s   $f15,   PA5,    0x00
    fld.s   $f16,   PA6,    0x00
    fld.s   $f17,   PA7,    0x00
#if __loongarch_grlen == 64
    GADDI , d, PA0, PA0, 0x04, PA1, PA1, 0x04, PA2, PA2, 0x04, PA3, PA3, 0x04, \
               PA4, PA4, 0x04, PA5, PA5, 0x04, PA6, PA6, 0x04, PA7, PA7, 0x04
#elif __loongarch_grlen == 32
    GADDI , w, PA0, PA0, 0x04, PA1, PA1, 0x04, PA2, PA2, 0x04, PA3, PA3, 0x04, \
               PA4, PA4, 0x04, PA5, PA5, 0x04, PA6, PA6, 0x04, PA7, PA7, 0x04
#else
    GADDI , d, PA0, PA0, 0x04, PA1, PA1, 0x04, PA2, PA2, 0x04, PA3, PA3, 0x04, \
               PA4, PA4, 0x04, PA5, PA5, 0x04, PA6, PA6, 0x04, PA7, PA7, 0x04
#endif
    GMADD f, s, $f2, $f10, $f1, $f2, $f3, $f11, $f1, $f3, $f4, $f12, $f1, $f4, $f5, $f13, $f1, $f5, \
                $f6, $f14, $f1, $f6, $f7, $f15, $f1, $f7, $f8, $f16, $f1, $f8, $f9, $f17, $f1, $f9,
    PTR_ADDI  I,      I,      -1
    PTR_ADD   X,      X,      INC_X
    bnez      I,      .L_\XW\()_M_L1
.L_\XW\()_M_END:
    fld.s   $f10,   Y,  0x00
    fldx.s  $f11,   Y,  INC_Y
    PTR_ALSL    PY0, INC_Y,  Y,  1
    fld.s   $f12,   PY0,    0x00
    fldx.s  $f13,   PY0,    INC_Y
    PTR_ALSL    PY1, INC_Y,  Y,  2
    fld.s   $f14,   PY1,    0x00
    fldx.s  $f15,   PY1,    INC_Y
    PTR_ALSL    PY2, INC_Y,  PY1, 1
    fld.s   $f16,   PY2,    0x00
    fldx.s  $f17,   PY2,    INC_Y

    GMADD f, s, $f10, ALPHA, $f2, $f10, $f11, ALPHA, $f3, $f11, $f12, ALPHA, $f4, $f12, $f13, ALPHA, $f5, $f13, \
                $f14, ALPHA, $f6, $f14, $f15, ALPHA, $f7, $f15, $f16, ALPHA, $f8, $f16, $f17, ALPHA, $f9, $f17

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
    fst.s   $f10,   Y,      0x00
    fstx.s  $f11,   Y,      INC_Y
    fst.s   $f12,   PY0,    0x00
    fstx.s  $f13,   PY0,    INC_Y
    fst.s   $f14,   PY1,    0x00
    fstx.s  $f15,   PY1,    INC_Y
    fst.s   $f16,   PY2,    0x00
    fstx.s  $f17,   PY2,    INC_Y

    PTR_ALSL    Y,      INC_Y,  Y,  3
    bnez        J,      .L_\XW\()_N_L8
.L_\XW\()_N_7:
    andi        J,      N,      4
    beqz        J,      .L_\XW\()_N_3
    ZERO_Y4
    move        X,      X_ORG
    PTR_SRLI    I,      M,       3
    beqz        I,      .L_\XW\()_N_4_M_7
.align 5
.L_\XW\()_N_4_M_L8:
    SLOAD_\X8
    SGEMV_T_4x8
    PTR_ADDI  I,      I,      -1
    PTR_ALSL  X,      INC_X,  X,  3
    bnez      I,      .L_\XW\()_N_4_M_L8
.L_\XW\()_N_4_M_7:
    // Accumulated
    GACC xvf, s, Y0, TP0, Y1, TP1, Y2, TP2, Y3, TP3
    andi        I,      M,      7
    beqz        I,      .L_\XW\()_N_4_M_END
.align 5
.L_\XW\()_N_4_M_L1:
    fld.s   $f1,    X,      0x00
    GLD_INC f, s, 0x04, $f10, PA0, 0x00, $f11, PA1, 0x00, $f12, PA2, 0x00, $f13, PA3, 0x00
    GMADD f, s, $f2, $f10, $f1, $f2, $f3, $f11, $f1, $f3, $f4, $f12, $f1, $f4, $f5, $f13, $f1, $f5
    PTR_ADDI  I,      I,      -1
    PTR_ADD   X,      X,      INC_X
    bnez      I,      .L_\XW\()_N_4_M_L1
.L_\XW\()_N_4_M_END:
    fld.s   $f10,   Y,  0x00
    fldx.s  $f11,   Y,  INC_Y
    PTR_ALSL    PY0, INC_Y,  Y,  1
    fld.s   $f12,   PY0,    0x00
    fldx.s  $f13,   PY0,    INC_Y

    GMADD f, s, $f10, ALPHA, $f2, $f10, $f11, ALPHA, $f3, $f11, $f12, ALPHA, $f4, $f12, $f13, ALPHA, $f5, $f13

    PTR_SLLI    K_LDA,  LDA,    2
    PTR_SUB     K_LDA,  K_LDA,  M4

#if __loongarch_grlen == 64
    GADD , d, PA0, PA0, K_LDA, PA1, PA1, K_LDA, PA2, PA2, K_LDA, PA3, PA3, K_LDA
#elif __loongarch_grlen == 32
    GADD , w, PA0, PA0, K_LDA, PA1, PA1, K_LDA, PA2, PA2, K_LDA, PA3, PA3, K_LDA
#else
    GADD , d, PA0, PA0, K_LDA, PA1, PA1, K_LDA, PA2, PA2, K_LDA, PA3, PA3, K_LDA
#endif
    fst.s   $f10,   Y,      0x00
    fstx.s  $f11,   Y,      INC_Y
    fst.s   $f12,   PY0,    0x00
    fstx.s  $f13,   PY0,    INC_Y
    PTR_ALSL    Y,      INC_Y,  Y,  2
.L_\XW\()_N_3:
    andi        J,      N,      2
    beqz        J,      .L_\XW\()_N_1
    ZERO_Y2
    move        X,      X_ORG
    PTR_SRLI    I,      M,       3
    beqz        I,      .L_\XW\()_N_2_M_7
.align 5
.L_\XW\()_N_2_M_L8:
    SLOAD_\X8
    SGEMV_T_2x8
    PTR_ADDI  I,      I,      -1
    PTR_ALSL  X,      INC_X,  X,  3
    bnez      I,      .L_\XW\()_N_2_M_L8
.L_\XW\()_N_2_M_7:
    // Accumulated
    GACC xvf, s, Y0, TP0, Y1, TP1
    andi        I,      M,      7
    beqz        I,      .L_\XW\()_N_2_M_END
.align 5
.L_\XW\()_N_2_M_L1:
    fld.s   $f1,    X,      0x00
    GLD_INC f, s, 0x04, $f10, PA0, 0x00, $f11, PA1, 0x00
    GMADD f, s, $f2, $f10, $f1, $f2, $f3, $f11, $f1, $f3
    PTR_ADDI  I,      I,      -1
    PTR_ADD   X,      X,      INC_X
    bnez      I,      .L_\XW\()_N_2_M_L1
.L_\XW\()_N_2_M_END:
    fld.s   $f10,   Y,  0x00
    fldx.s  $f11,   Y,  INC_Y

    GMADD f, s, $f10, ALPHA, $f2, $f10, $f11, ALPHA, $f3, $f11

    PTR_SLLI    K_LDA,  LDA,    1
    PTR_SUB     K_LDA,  K_LDA,  M4

#if __loongarch_grlen == 64
    GADD , d, PA0, PA0, K_LDA, PA1, PA1, K_LDA
#elif __loongarch_grlen == 32
    GADD , w, PA0, PA0, K_LDA, PA1, PA1, K_LDA
#else
    GADD , d, PA0, PA0, K_LDA, PA1, PA1, K_LDA
#endif
    fst.s   $f10,   Y,      0x00
    fstx.s  $f11,   Y,      INC_Y
    PTR_ALSL        Y,      INC_Y,  Y,  1
.L_\XW\()_N_1:
    andi    J,      N,      1
    beqz    J,      .L_END
    ZERO_Y1
    move    X,      X_ORG
    move    I,      M
    beqz    I,      .L_END
.align 5
.L_\XW\()_N_1_M_L1:
    fld.s   $f2,    PA0,    0x00
    fld.s   $f1,    X,      0x00
    fmadd.s $f10,   $f2,    $f1,    $f10
    PTR_ADDI  I,      I,      -1
    PTR_ADD   X,      X,      INC_X
    PTR_ADDI  PA0,    PA0,    0x04
    bnez      I,      .L_\XW\()_N_1_M_L1

    fld.s     $f2,    Y,      0x00
    fmadd.s   $f2,    ALPHA,  $f10,  $f2
    fst.s     $f2,    Y,      0x00
    b .L_END
.endm

    PROLOGUE
    PTR_LD     INC_Y,  $sp,    0
    push_if_used 8, 0
    PTR_ADDI   K,      $r0,     0x01
    PTR_SUB    I,      INC_X,   K
    maskeqz    I,      K,       I  /* if(inc_x == 1) I = 0; else I = 1; */
    GSLLI , d, LDA, LDA, 2, INC_X, INC_X, 2, INC_Y, INC_Y, 2, M4, M, 2
    xvreplve0.w     VALPHA, $xr0
    move     X_ORG,  X
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
    .hword .L_GAP_0 - .L_GAP_TABLE
    .hword .L_GAP_1 - .L_GAP_TABLE
.L_GAP_0: /* if (incx == 1) */
    SGEMV_T_LASX GAP_0, X8, X4
.L_GAP_1: /* if (incx != 1) */
    SGEMV_T_LASX GAP_1, X8_GAP, X4_GAP
.L_END:
    pop_if_used 8, 0
    jirl    $r0, $r1, 0x0
    EPILOGUE
