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
#define M16     $r30

#define VALPHA  $xr0
#define X0      $xr1
#define X1      $xr2
#define A0      $xr3
#define A1      $xr4
#define A2      $xr5
#define A3      $xr6
#define A4      $xr7
#define A5      $xr8
#define A6      $xr9
#define A7      $xr10
#define A8      $xr11
#define A9      $xr12
#define A10     $xr13
#define A11     $xr14
#define A12     $xr15
#define A13     $xr16
#define A14     $xr17
#define A15     $xr18
#define TP0     $xr19
#define TP1     $xr20
#define TP2     $xr21
#define TP3     $xr22
#define TP4     $xr23
#define TP5     $xr24
#define TP6     $xr25
#define TP7     $xr26
#define TMP0    $xr27
#define TMP1    $xr28
#define TMP2    $xr29
#define Y0      $xr3
#define Y1      $xr4
#define Y2      $xr5
#define Y3      $xr6
#define Y4      $xr7
#define Y5      $xr8
#define Y6      $xr9
#define Y7      $xr10

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
    GXOR xv, v, TP0, TP0, TP0, TP1, TP1, TP1, TP2, TP2, TP2, TP3, TP3, TP3
.endm

.macro ZERO_Y1
    GXOR xv, v, TP0, TP0, TP0
.endm

.macro ZLOAD_X4
    GLD xv, , X0, X, 0x00, X1, X, 0x20
.endm

.macro ZLOAD_X4_GAP
    xvld        X0,     X,      0

    PTR_ADD     T0,     X,      INC_X
    xvld        A0,     T0,     0
    xvpermi.q   X0,     A0,     0x02

    PTR_ADD     T0,     T0,     INC_X
    xvld        X1,     T0,     0

    PTR_ADD     T0,     T0,     INC_X
    xvld        A0,     T0,     0
    xvpermi.q   X1,     A0,     0x02
.endm

.macro ZGEMV_T_4x4
    GLD_INC xv, , 0x20,       \
    A0,  PA0, 0, A1,  PA0, 0, \
    A2,  PA1, 0, A3,  PA1, 0, \
    A4,  PA2, 0, A5,  PA2, 0, \
    A6,  PA3, 0, A7,  PA3, 0
    GCOMPLEXMADD GXCONJ1, GCONJ1, \
    xvf, d, TP0, A0,  X0, TP0, TMP0, TMP1, TMP2, TP0, A1,  X1, TP0, TMP0, TMP1, TMP2, \
    TP1, A2,  X0, TP1, TMP0, TMP1, TMP2, TP1, A3,  X1, TP1, TMP0, TMP1, TMP2, \
    TP2, A4,  X0, TP2, TMP0, TMP1, TMP2, TP2, A5,  X1, TP2, TMP0, TMP1, TMP2, \
    TP3, A6,  X0, TP3, TMP0, TMP1, TMP2, TP3, A7,  X1, TP3, TMP0, TMP1, TMP2
.endm

.macro ZGEMV_T_LASX XW:req, X4:req
    PTR_SRLI  J,      N,      2
    beqz      J,      .L_\XW\()_N_3
    PTR_SLLI  K_LDA,  LDA,    2
    PTR_SUB   K_LDA,  K_LDA,  M16
.L_\XW\()_N_L4:
    ZERO_Y4
    move      X,      X_ORG
    PTR_SRLI  I,      M,       2
    beqz      I,      .L_\XW\()_M_3
.align 5
.L_\XW\()_M_L4:
    ZLOAD_\X4
    ZGEMV_T_4x4
    PTR_ADDI    I,  I,  -1
    PTR_ALSL    X,  INC_X,  X,  2
    bnez        I,  .L_\XW\()_M_L4
.L_\XW\()_M_3:
    // Accumulated
    GCOMPLEXACC xvf, d, Y0, TP0, Y1, TP1, Y2, TP2, Y3, TP3
    andi        I,      M,      3
    beqz        I,      .L_\XW\()_M_END
.align 5
.L_\XW\()_M_L1:
    GLD xv, , X0, X, 0x00, A8, PA0, 0x00, A9, PA1, 0x00, A10, PA2, 0x00, A11, PA3, 0x00
#if __loongarch_grlen == 64
    GADDI , d, PA0, PA0, 0x10, PA1, PA1, 0x10, PA2, PA2, 0x10, PA3, PA3, 0x10
#elif __loongarch_grlen == 32
    GADDI , w, PA0, PA0, 0x10, PA1, PA1, 0x10, PA2, PA2, 0x10, PA3, PA3, 0x10
#else
    GADDI , d, PA0, PA0, 0x10, PA1, PA1, 0x10, PA2, PA2, 0x10, PA3, PA3, 0x10
#endif

    GCOMPLEXMADD GXCONJ1, GCONJ1, \
    xvf, d, A0, A8,  X0, A0, TMP0, TMP1, TMP2, A1, A9,  X0, A1, TMP0, TMP1, TMP2, \
    A2, A10, X0, A2, TMP0, TMP1, TMP2, A3, A11, X0, A3, TMP0, TMP1, TMP2

    PTR_ADDI  I,      I,      -1
    PTR_ADD   X,      X,      INC_X
    bnez      I,      .L_\XW\()_M_L1
.L_\XW\()_M_END:
    xvld  A8, Y, 0x00
    xvldx A9, Y, INC_Y
    PTR_ALSL    PY0, INC_Y,  Y,  1
    xvld  A10, PY0, 0x00
    xvldx A11, PY0, INC_Y

    GCOMPLEXMADD GXCONJ2, GCONJ2, \
    xvf, d, A8,  VALPHA, A0, A8,  TMP0, TMP1, TMP2, A9,  VALPHA, A1, A9,  TMP0, TMP1, TMP2,\
    A10, VALPHA, A2, A10, TMP0, TMP1, TMP2, A11, VALPHA, A3, A11, TMP0, TMP1, TMP2

    PTR_ADDI    J,      J,      -1
#if __loongarch_grlen == 64
    GADD , d, PA0, PA0, K_LDA, PA1, PA1, K_LDA, PA2, PA2, K_LDA, PA3, PA3, K_LDA
#elif __loongarch_grlen == 32
    GADD , w, PA0, PA0, K_LDA, PA1, PA1, K_LDA, PA2, PA2, K_LDA, PA3, PA3, K_LDA
#else
    GADD , d, PA0, PA0, K_LDA, PA1, PA1, K_LDA, PA2, PA2, K_LDA, PA3, PA3, K_LDA
#endif
    vst  $vr11, Y,   0x00
    vstx $vr12, Y,   INC_Y
    vst  $vr13, PY0, 0x00
    vstx $vr14, PY0, INC_Y
    PTR_ALSL    Y,      INC_Y,  Y,  2
    bnez        J,      .L_\XW\()_N_L4
.L_\XW\()_N_3:
    andi        J,      N,      3
    beqz        J,      .L_END
    PTR_SUB     K_LDA,  LDA,    M16
.L_\XW\()_N_1:
    ZERO_Y1
    move    X,      X_ORG
    move    I,      M
    beqz    I,      .L_END
.align 5
.L_\XW\()_N_1_M_L1:
    GLD xv, , A0, PA0, 0x00, X0, X, 0x00
    GCOMPLEXMADD GXCONJ1, GCONJ1, \
    xvf, d, TP0, A0, X0, TP0, TMP0, TMP1, TMP2
    PTR_ADDI  I,      I,      -1
    PTR_ADD   X,      X,      INC_X
    PTR_ADDI  PA0,    PA0,    0x10
    bnez      I,      .L_\XW\()_N_1_M_L1
.L_\XW\()_N_1_M_END:
    PTR_ADDI  J,      J,      -1
    xvld      A0,     Y,      0x00
    GCOMPLEXMADD GXCONJ2, GCONJ2, \
    xvf, d, A0, VALPHA, TP0, A0, TMP0, TMP1, TMP2
    vst       $vr3,   Y,      0x00
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
    GSLLI , d, LDA, LDA, 4, INC_X, INC_X, 4, INC_Y, INC_Y, 4, M16, M, 4
    // Init VALPHA
    xvpackev.d      $xr0,   $xr1,   $xr0
    xvreplve0.q     VALPHA, $xr0
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
    ZGEMV_T_LASX GAP_0, X4
.L_GAP_1: /* if (incx != 1) */
    ZGEMV_T_LASX GAP_1, X4_GAP
.L_END:
    pop_if_used 8, 6
    jirl    $r0, $r1, 0x0
    EPILOGUE
