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

/* Function parameters */
#define M      $r4   // param 1: bm
#define N      $r5   // param 2: bn
#define K      $r6   // param 3: bk
#define ALPHA  $f0   // param 4: alpha
#define A      $r7   // param 5: ba
#define B      $r8   // param 6: bb
#define C      $r9   // param 7: bc
#define LDC    $r10  // param 8: ldc

#ifdef TRMMKERNEL
#define OFFSET $r11  // param 9: offset
#endif
#define OFF    $r12

/* Cycle control parameters */
#define I      $r13
#define J      $r14
#define L      $r15
#define TL     $r16
/* Matrix address */
#define A0     $r17
#define B0     $r18
#define C0     $r19
#define C1     $r20
#define C2     $r23
#define C3     $r24
#define C4     $r25
#define C5     $r26
#define T0     $r27 /* !! DO NOT USE $r21 and $r22 !! */
#define T1     $r28
#define T2     $r29
#define I48    $r30
#define ZERO   $r0

/* LASX vectors */
#define U0     $xr0
#define U1     $xr1
#define U2     $xr2
#define U3     $xr3
#define U4     $xr4
#define U5     $xr5
#define U6     $xr6
#define D0     $xr7
#define D1     $xr8
#define D2     $xr9
#define D3     $xr10
#define D4     $xr11
#define D5     $xr12
#define D6     $xr13
#define D7     $xr14
#define D8     $xr15
#define D9     $xr16
#define D10    $xr17
#define D11    $xr18
#define D12    $xr19
#define D13    $xr20
#define D14    $xr21
#define D15    $xr22
#define D16    $xr23
#define D17    $xr24
#define D18    $xr25
#define D19    $xr26
#define D20    $xr27
#define D21    $xr28
#define D22    $xr29
#define D23    $xr30
#define VALPHA $xr31

/* Prefetch interval */
#define A_PRE  0x200 /* 0x200 / 0x80 = 4 */
#define B_PRE  0x100 /* 0x100 / 0x30 = 4 */

.macro KERNEL_16x6
    /* Load 16 * 64 from A0 */
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20
    xvld     U2,   A0,    0x40
    xvld     U3,   A0,    0x60

    /* Cumulative D0~D23 */
    xvldrepl.d U4,  B0, 0x00
    xvldrepl.d U5,  B0, 0x08
    xvldrepl.d U6,  B0, 0x10

    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1
    xvfmadd.d  D2,  U2, U4, D2
    xvfmadd.d  D3,  U3, U4, D3
    preld      0,   B0, B_PRE

    xvfmadd.d  D4,  U0, U5, D4
    xvfmadd.d  D5,  U1, U5, D5
    xvfmadd.d  D6,  U2, U5, D6
    xvfmadd.d  D7,  U3, U5, D7

    xvfmadd.d  D8,  U0, U6, D8
    xvfmadd.d  D9,  U1, U6, D9
    xvfmadd.d  D10, U2, U6, D10
    xvfmadd.d  D11, U3, U6, D11
    preld      0,   A0, A_PRE

    xvldrepl.d U4,  B0, 0x18
    xvldrepl.d U5,  B0, 0x20
    xvldrepl.d U6,  B0, 0x28

    xvfmadd.d  D12, U0, U4, D12
    xvfmadd.d  D13, U1, U4, D13
    xvfmadd.d  D14, U2, U4, D14
    xvfmadd.d  D15, U3, U4, D15

    xvfmadd.d  D16, U0, U5, D16
    xvfmadd.d  D17, U1, U5, D17
    xvfmadd.d  D18, U2, U5, D18
    xvfmadd.d  D19, U3, U5, D19
    preld      0,   A0, A_PRE + 0x40

    xvfmadd.d  D20, U0, U6, D20
    xvfmadd.d  D21, U1, U6, D21
    xvfmadd.d  D22, U2, U6, D22
    xvfmadd.d  D23, U3, U6, D23

    addi.d     A0,  A0, 0x80
    addi.d     B0,  B0, 0x30
.endm

    PROLOGUE

    addi.d   $sp,   $sp,   -160
    /* Store $r23~$31 */
    SDARG    $r23,  $sp,   0
    SDARG    $r24,  $sp,   8
    SDARG    $r25,  $sp,   16
    SDARG    $r26,  $sp,   24
    SDARG    $r27,  $sp,   32
    SDARG    $r28,  $sp,   40
    SDARG    $r29,  $sp,   48
    SDARG    $r30,  $sp,   56
    SDARG    $r31,  $sp,   64
    fst.d    $f23,  $sp,   72
    fst.d    $f24,  $sp,   80
    fst.d    $f25,  $sp,   96
    fst.d    $f26,  $sp,   104
    fst.d    $f27,  $sp,   112
    fst.d    $f28,  $sp,   120
    fst.d    $f29,  $sp,   128
    fst.d    $f30,  $sp,   136
    fst.d    $f31,  $sp,   144
    fst.d    ALPHA, $sp,   152

#if defined (TRMMKERNEL) && !defined(LEFT)
    sub.d   OFF,   ZERO,  OFFSET
#else
    xor     OFF,   OFF,   OFF
#endif

    addi.d       I48,    ZERO, 48
    /* VALPHA = {ALPHA, ALPHA, ALPHA, ALPHA} */
    xvld         VALPHA, $sp,  152
    xvreplve0.d  VALPHA, VALPHA
    xor      T0,    T0,    T0
    addi.d   T0,    T0,    6
    /* if (!(N / 6)) goto L_N5 */
    div.d    J,     N,     T0     /* J = bn / 6 */
    mul.d    T0,    J,     T0
    sub.d    N,     N,     T0
    beq      ZERO,  J,     .L_N5

.L_J1: /* J-- && This loop include Condition 1 */

/************************* Condition 1 if((N / 6) && (M >> 4)) START !!! *************************
*                                                   dgemm_core_16x6                                */
    move     C0,    C
    move     A0,    A
    slli.d   T0,    LDC,   3
    add.d    C1,    C0,    T0
    addi.d   J,     J,     -1   /* J-- */
    add.d    C2,    C1,    T0
    add.d    C3,    C2,    T0
    add.d    C4,    C3,    T0
    add.d    C5,    C4,    T0

#if defined(TRMMKERNEL) && defined(LEFT)
    move     OFF,   OFFSET
#endif

    /* if (!(M >> 3)) goto L_M8 */
    srai.d   I,     M,     4     /* I = bm >> 4 */
    beq      ZERO,  I,     .L_M8

.L_I1: /* I-- */
#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    move     B0,    B
#else
    slli.d   T0,    OFF,  0x07
    add.d    A0,    A0,   T0
    mul.d    T0,    OFF,  I48
    add.d    B0,    B,    T0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    sub.d    L,     K,    OFF
#elif defined(LEFT)
    /* number of values in A */
    addi.d   L,     OFF,  16
#else
    /* number of values in B */
    addi.d   L,     OFF,  6
#endif
#else   // #if !defined(TRMMKERNEL)
    move     B0,    B
    move     L,     K /* L = bk */
#endif
    /* Calculate the first set of D0~D23,
     * avoidig set 0 operation
     * Load 16 * 64 from A0
     * U0 = {a3,  a2,  a1,  a0}
     * U1 = {a7,  a6,  a5,  a4}
     * U2 = {a11, a10, a9,  a8}
     * U3 = {a15, a14, a13, a12}
     */
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20
    xvld     U2,   A0,    0x40
    xvld     U3,   A0,    0x60

    xvldrepl.d     U4, B0, 0x00
    xvldrepl.d     U5, B0, 0x08
    xvldrepl.d     U6, B0, 0x10

    preld          0,  C0, 0x00
    /* line 1 */
    xvfmul.d  D0,  U0, U4
    xvfmul.d  D1,  U1, U4
    preld     0,   C0, 0x40
    xvfmul.d  D2,  U2, U4
    xvfmul.d  D3,  U3, U4

    preld          0,   C1,    0x00
    /* line 2 */
    xvfmul.d  D4,  U0, U5
    xvfmul.d  D5,  U1, U5
    preld     0,   C1,    0x40
    xvfmul.d  D6,  U2, U5
    xvfmul.d  D7,  U3, U5

    preld          0,   C2,    0x00
    /* line 3 */
    xvfmul.d  D8,  U0, U6
    xvfmul.d  D9,  U1, U6
    preld     0,   C2,    0x40
    xvfmul.d  D10, U2, U6
    xvfmul.d  D11, U3, U6

    xvldrepl.d     U4, B0, 0x18
    xvldrepl.d     U5, B0, 0x20
    xvldrepl.d     U6, B0, 0x28

    preld          0,   C3,    0x00
    /* line 4 */
    xvfmul.d  D12, U0, U4
    xvfmul.d  D13, U1, U4
    preld     0,   C3,    0x40
    xvfmul.d  D14, U2, U4
    xvfmul.d  D15, U3, U4

    preld          0,   C4,    0x00
    /* line 5 */
    xvfmul.d  D16, U0, U5
    xvfmul.d  D17, U1, U5
    preld     0,   C4,    0x40
    xvfmul.d  D18, U2, U5
    xvfmul.d  D19, U3, U5

    preld          0,   C5,    0x00
    /* line 6 */
    xvfmul.d  D20, U0, U6
    xvfmul.d  D21, U1, U6
    preld     0,   C5,    0x40
    xvfmul.d  D22, U2, U6
    xvfmul.d  D23, U3, U6

    /* Add stride for A0 and B0 */
    addi.d    A0,  A0, 0x80
    addi.d    B0,  B0, 0x30
    /* Reduce L */
    addi.d    L,   L,  -1
    srai.d    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_L7 */
    beq       ZERO,TL, .L_L7

    /* Calculate 8 sets of D0~D23 */
.L_TL1: /* TL-- */
    KERNEL_16x6
    KERNEL_16x6
    KERNEL_16x6
    KERNEL_16x6
    KERNEL_16x6
    KERNEL_16x6
    KERNEL_16x6
    KERNEL_16x6
    addi.d    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_TL1

   /* Maybe we need calculate the last
    * 7 sets of D0~D23?
    */
.L_L7:
    /* if (!(L & 7)) goto L_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_L0

.L_L71:
    /* Load 16 * 64 from A0 */
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20
    xvld     U2,   A0,    0x40
    xvld     U3,   A0,    0x60

    /* Cumulative D0~D23 */
    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1
    xvfmadd.d  D2,  U2, U4, D2
    xvfmadd.d  D3,  U3, U4, D3

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4
    xvfmadd.d  D5,  U1, U4, D5
    xvfmadd.d  D6,  U2, U4, D6
    xvfmadd.d  D7,  U3, U4, D7

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8
    xvfmadd.d  D9,  U1, U4, D9
    xvfmadd.d  D10, U2, U4, D10
    xvfmadd.d  D11, U3, U4, D11

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12
    xvfmadd.d  D13, U1, U4, D13
    xvfmadd.d  D14, U2, U4, D14
    xvfmadd.d  D15, U3, U4, D15

    xvldrepl.d U4,  B0, 0x20
    xvfmadd.d  D16, U0, U4, D16
    xvfmadd.d  D17, U1, U4, D17
    xvfmadd.d  D18, U2, U4, D18
    xvfmadd.d  D19, U3, U4, D19

    xvldrepl.d U4,  B0, 0x28
    xvfmadd.d  D20, U0, U4, D20
    xvfmadd.d  D21, U1, U4, D21
    xvfmadd.d  D22, U2, U4, D22
    xvfmadd.d  D23, U3, U4, D23

    /* Add stride for A0, B0 */
    addi.d     A0,  A0, 0x80
    addi.d     B0,  B0, 0x30

    addi.d     TL,  TL, -1
    blt        ZERO,TL, .L_L71

.L_L0:
#if defined(TRMMKERNEL)
    xvfmul.d  D0,   D0,  VALPHA
    xvfmul.d  D1,   D1,  VALPHA
    xvfmul.d  D2,   D2,  VALPHA
    xvfmul.d  D3,   D3,  VALPHA
    xvfmul.d  D4,   D4,  VALPHA
    xvfmul.d  D5,   D5,  VALPHA
    xvfmul.d  D6,   D6,  VALPHA
    xvfmul.d  D7,   D7,  VALPHA
    xvfmul.d  D8,   D8,  VALPHA
    xvfmul.d  D9,   D9,  VALPHA
    xvfmul.d  D10,  D10, VALPHA
    xvfmul.d  D11,  D11, VALPHA
    xvfmul.d  D12,  D12, VALPHA
    xvfmul.d  D13,  D13, VALPHA
    xvfmul.d  D14,  D14, VALPHA
    xvfmul.d  D15,  D15, VALPHA
    xvfmul.d  D16,  D16, VALPHA
    xvfmul.d  D17,  D17, VALPHA
    xvfmul.d  D18,  D18, VALPHA
    xvfmul.d  D19,  D19, VALPHA
    xvfmul.d  D20,  D20, VALPHA
    xvfmul.d  D21,  D21, VALPHA
    xvfmul.d  D22,  D22, VALPHA
    xvfmul.d  D23,  D23, VALPHA
#else
    /* Load C0  */
    xvld      U0,  C0,  0x00
    xvld      U1,  C0,  0x20
    xvld      U2,  C0,  0x40
    xvld      U3,  C0,  0x60
    xvfmadd.d D0,  D0,  VALPHA,  U0 /* D0 = U0 + (D0 * VALPHA) */
    xvfmadd.d D1,  D1,  VALPHA,  U1
    xvfmadd.d D2,  D2,  VALPHA,  U2
    xvfmadd.d D3,  D3,  VALPHA,  U3

    /* Load C1  */
    xvld      U0,  C1,  0x00
    xvld      U1,  C1,  0x20
    xvld      U2,  C1,  0x40
    xvld      U3,  C1,  0x60
    xvfmadd.d D4,  D4,  VALPHA,  U0
    xvfmadd.d D5,  D5,  VALPHA,  U1
    xvfmadd.d D6,  D6,  VALPHA,  U2
    xvfmadd.d D7,  D7,  VALPHA,  U3

    /* Load C2  */
    xvld      U0,  C2,  0x00
    xvld      U1,  C2,  0x20
    xvld      U2,  C2,  0x40
    xvld      U3,  C2,  0x60
    xvfmadd.d D8,  D8,  VALPHA,  U0
    xvfmadd.d D9,  D9,  VALPHA,  U1
    xvfmadd.d D10, D10, VALPHA,  U2
    xvfmadd.d D11, D11, VALPHA,  U3

    /* Load C3  */
    xvld      U0,  C3,  0x00
    xvld      U1,  C3,  0x20
    xvld      U2,  C3,  0x40
    xvld      U3,  C3,  0x60
    xvfmadd.d D12, D12, VALPHA,  U0
    xvfmadd.d D13, D13, VALPHA,  U1
    xvfmadd.d D14, D14, VALPHA,  U2
    xvfmadd.d D15, D15, VALPHA,  U3

    /* Load C4  */
    xvld      U0,  C4,  0x00
    xvld      U1,  C4,  0x20
    xvld      U2,  C4,  0x40
    xvld      U3,  C4,  0x60
    xvfmadd.d D16, D16, VALPHA,  U0
    xvfmadd.d D17, D17, VALPHA,  U1
    xvfmadd.d D18, D18, VALPHA,  U2
    xvfmadd.d D19, D19, VALPHA,  U3

    /* Load C5  */
    xvld      U0,  C5,  0x00
    xvld      U1,  C5,  0x20
    xvld      U2,  C5,  0x40
    xvld      U3,  C5,  0x60
    xvfmadd.d D20, D20, VALPHA,  U0
    xvfmadd.d D21, D21, VALPHA,  U1
    xvfmadd.d D22, D22, VALPHA,  U2
    xvfmadd.d D23, D23, VALPHA,  U3
 #endif

    /* Store C0 */
    xvst      D0,  C0,  0x00
    xvst      D1,  C0,  0x20
    xvst      D2,  C0,  0x40
    xvst      D3,  C0,  0x60
    /* Store C1 */
    xvst      D4,  C1,  0x00
    xvst      D5,  C1,  0x20
    xvst      D6,  C1,  0x40
    xvst      D7,  C1,  0x60
    /* Store C2 */
    xvst      D8,  C2,  0x00
    xvst      D9,  C2,  0x20
    xvst      D10, C2,  0x40
    xvst      D11, C2,  0x60
    /* Store C3 */
    xvst      D12, C3,  0x00
    xvst      D13, C3,  0x20
    xvst      D14, C3,  0x40
    xvst      D15, C3,  0x60
    /* Store C4 */
    xvst      D16, C4,  0x00
    xvst      D17, C4,  0x20
    xvst      D18, C4,  0x40
    xvst      D19, C4,  0x60
    /* Store C5 */
    xvst      D20,  C5,  0x00
    xvst      D21,  C5,  0x20
    xvst      D22,  C5,  0x40
    xvst      D23,  C5,  0x60

    /* Add stride for C */
    addi.d    C0,  C0,  0x80
    addi.d    C1,  C1,  0x80
    addi.d    C2,  C2,  0x80
    addi.d    C3,  C3,  0x80
    addi.d    C4,  C4,  0x80
    addi.d    C5,  C5,  0x80

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d     L,   K,   OFF
#ifdef LEFT
    /* number of values  in A */
    addi.d    L,   L,   -16
#else
    /* number of values in B */
    addi.d    L,   L,   -6
#endif
    slli.d    T0,  L,  0x07
    add.d     A0,  A0, T0
    mul.d     T0,  L,  I48
    add.d     B0,  B0, T0
#endif

#ifdef LEFT
    addi.d    OFF, OFF, 0x10
#endif
#endif   // #if defined(TRMMKERNEL)

    addi.d    I,   I,   -1  /* I-- */
    blt       ZERO,I,   .L_I1

.L_M8:
    /* We have done M & 16, considering M=8/4/2/1 */
    andi      I,   M,   15
    beq       ZERO,I,   .L_M0

    andi      I,   M,   8
    beq       ZERO,I,   .L_M4

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    move     B0,    B
#else
    slli.d   T0,    OFF,  0x06
    add.d    A0,    A0,   T0
    mul.d    T0,    OFF,  I48
    add.d    B0,    B,    T0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    sub.d    L,     K,    OFF
#elif defined(LEFT)
    /* number of values in A */
    addi.d   L,     OFF,  8
#else
    /* number of values in B */
    addi.d   L,     OFF,  6
#endif
#else   // #if !defined(TRMMKERNEL)
    move     B0,    B
    move     L,     K /* L = bk */
#endif  // #if defined(TRMMKERNEL)

    /* Load 8 * 64 from A0 */
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20

    xvldrepl.d     U4, B0, 0x00
    /* line 1 */
    xvfmul.d  D0,  U0, U4
    xvfmul.d  D1,  U1, U4

    xvldrepl.d     U4, B0, 0x08
    /* line 2 */
    xvfmul.d  D4,  U0, U4
    xvfmul.d  D5,  U1, U4

    xvldrepl.d     U4, B0, 0x10
    /* line 3 */
    xvfmul.d  D8,  U0, U4
    xvfmul.d  D9,  U1, U4

    xvldrepl.d     U4, B0, 0x18
    /* line 4 */
    xvfmul.d  D12, U0, U4
    xvfmul.d  D13, U1, U4

    xvldrepl.d     U4, B0, 0x20
    /* line 5 */
    xvfmul.d  D16, U0, U4
    xvfmul.d  D17, U1, U4

    xvldrepl.d     U4, B0, 0x28
    /* line 6 */
    xvfmul.d  D20, U0, U4
    xvfmul.d  D21, U1, U4

    /* Add stride for A0 and B0 */
    addi.d    A0,  A0, 0x40
    addi.d    B0,  B0, 0x30
    /* Reduce L */
    addi.d    L,   L,  -1
    srai.d    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_M8_L7 */
    beq       ZERO,TL, .L_M8_L7

.L_M8_TL1: /* TL-- */
           /***8-1***/
    /* Load 16 * 64 from A0 */
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4
    xvfmadd.d  D5,  U1, U4, D5

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8
    xvfmadd.d  D9,  U1, U4, D9

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12
    xvfmadd.d  D13, U1, U4, D13

    xvldrepl.d U4,  B0, 0x20
    xvfmadd.d  D16, U0, U4, D16
    xvfmadd.d  D17, U1, U4, D17

    xvldrepl.d U4,  B0, 0x28
    xvfmadd.d  D20, U0, U4, D20
    xvfmadd.d  D21, U1, U4, D21

    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x30

           /***8-2***/
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4
    xvfmadd.d  D5,  U1, U4, D5

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8
    xvfmadd.d  D9,  U1, U4, D9

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12
    xvfmadd.d  D13, U1, U4, D13

    xvldrepl.d U4,  B0, 0x20
    xvfmadd.d  D16, U0, U4, D16
    xvfmadd.d  D17, U1, U4, D17

    xvldrepl.d U4,  B0, 0x28
    xvfmadd.d  D20, U0, U4, D20
    xvfmadd.d  D21, U1, U4, D21

    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x30

           /***8-3***/
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4
    xvfmadd.d  D5,  U1, U4, D5

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8
    xvfmadd.d  D9,  U1, U4, D9

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12
    xvfmadd.d  D13, U1, U4, D13

    xvldrepl.d U4,  B0, 0x20
    xvfmadd.d  D16, U0, U4, D16
    xvfmadd.d  D17, U1, U4, D17

    xvldrepl.d U4,  B0, 0x28
    xvfmadd.d  D20, U0, U4, D20
    xvfmadd.d  D21, U1, U4, D21

    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x30

           /***8-4***/
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4
    xvfmadd.d  D5,  U1, U4, D5

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8
    xvfmadd.d  D9,  U1, U4, D9

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12
    xvfmadd.d  D13, U1, U4, D13

    xvldrepl.d U4,  B0, 0x20
    xvfmadd.d  D16, U0, U4, D16
    xvfmadd.d  D17, U1, U4, D17

    xvldrepl.d U4,  B0, 0x28
    xvfmadd.d  D20, U0, U4, D20
    xvfmadd.d  D21, U1, U4, D21

    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x30

           /***8-5***/
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4
    xvfmadd.d  D5,  U1, U4, D5

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8
    xvfmadd.d  D9,  U1, U4, D9

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12
    xvfmadd.d  D13, U1, U4, D13

    xvldrepl.d U4,  B0, 0x20
    xvfmadd.d  D16, U0, U4, D16
    xvfmadd.d  D17, U1, U4, D17

    xvldrepl.d U4,  B0, 0x28
    xvfmadd.d  D20, U0, U4, D20
    xvfmadd.d  D21, U1, U4, D21

    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x30

           /***8-6***/
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4
    xvfmadd.d  D5,  U1, U4, D5

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8
    xvfmadd.d  D9,  U1, U4, D9

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12
    xvfmadd.d  D13, U1, U4, D13

    xvldrepl.d U4,  B0, 0x20
    xvfmadd.d  D16, U0, U4, D16
    xvfmadd.d  D17, U1, U4, D17

    xvldrepl.d U4,  B0, 0x28
    xvfmadd.d  D20, U0, U4, D20
    xvfmadd.d  D21, U1, U4, D21

    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x30

           /***8-7***/
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4
    xvfmadd.d  D5,  U1, U4, D5

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8
    xvfmadd.d  D9,  U1, U4, D9

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12
    xvfmadd.d  D13, U1, U4, D13

    xvldrepl.d U4,  B0, 0x20
    xvfmadd.d  D16, U0, U4, D16
    xvfmadd.d  D17, U1, U4, D17

    xvldrepl.d U4,  B0, 0x28
    xvfmadd.d  D20, U0, U4, D20
    xvfmadd.d  D21, U1, U4, D21

    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x30

           /***8-8***/
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4
    xvfmadd.d  D5,  U1, U4, D5

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8
    xvfmadd.d  D9,  U1, U4, D9

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12
    xvfmadd.d  D13, U1, U4, D13

    xvldrepl.d U4,  B0, 0x20
    xvfmadd.d  D16, U0, U4, D16
    xvfmadd.d  D17, U1, U4, D17

    xvldrepl.d U4,  B0, 0x28
    xvfmadd.d  D20, U0, U4, D20
    xvfmadd.d  D21, U1, U4, D21

    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x30

    addi.d    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_M8_TL1

.L_M8_L7:
    /* if (!(L & 7)) goto L_M8_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_M8_L0

.L_M8_L71:
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4
    xvfmadd.d  D5,  U1, U4, D5

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8
    xvfmadd.d  D9,  U1, U4, D9

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12
    xvfmadd.d  D13, U1, U4, D13

    xvldrepl.d U4,  B0, 0x20
    xvfmadd.d  D16, U0, U4, D16
    xvfmadd.d  D17, U1, U4, D17

    xvldrepl.d U4,  B0, 0x28
    xvfmadd.d  D20, U0, U4, D20
    xvfmadd.d  D21, U1, U4, D21

    /* Add stride for A0, B0 */
    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x30

    addi.d     TL,  TL, -1
    blt        ZERO,TL, .L_M8_L71

.L_M8_L0:
#if defined(TRMMKERNEL)
    xvfmul.d  D0,   D0,  VALPHA
    xvfmul.d  D1,   D1,  VALPHA
    xvfmul.d  D4,   D4,  VALPHA
    xvfmul.d  D5,   D5,  VALPHA
    xvfmul.d  D8,   D8,  VALPHA
    xvfmul.d  D9,   D9,  VALPHA
    xvfmul.d  D12,  D12, VALPHA
    xvfmul.d  D13,  D13, VALPHA
    xvfmul.d  D16,  D16, VALPHA
    xvfmul.d  D17,  D17, VALPHA
    xvfmul.d  D20,  D20, VALPHA
    xvfmul.d  D21,  D21, VALPHA
#else
    /* Load C0  */
    xvld      U0,  C0,  0x00
    xvld      U1,  C0,  0x20
    xvfmadd.d D0,  D0,  VALPHA,  U0 /* D0 = U0 + (D0 * VALPHA) */
    xvfmadd.d D1,  D1,  VALPHA,  U1

    /* Load C1  */
    xvld      U0,  C1,  0x00
    xvld      U1,  C1,  0x20
    xvfmadd.d D4,  D4,  VALPHA,  U0
    xvfmadd.d D5,  D5,  VALPHA,  U1

    /* Load C2  */
    xvld      U0,  C2,  0x00
    xvld      U1,  C2,  0x20
    xvfmadd.d D8,  D8,  VALPHA,  U0
    xvfmadd.d D9,  D9,  VALPHA,  U1

    /* Load C3  */
    xvld      U0,  C3,  0x00
    xvld      U1,  C3,  0x20
    xvfmadd.d D12, D12, VALPHA,  U0
    xvfmadd.d D13, D13, VALPHA,  U1

    /* Load C4  */
    xvld      U0,  C4,  0x00
    xvld      U1,  C4,  0x20
    xvfmadd.d D16, D16, VALPHA,  U0
    xvfmadd.d D17, D17, VALPHA,  U1

    /* Load C5  */
    xvld      U0,  C5,  0x00
    xvld      U1,  C5,  0x20
    xvfmadd.d D20, D20, VALPHA,  U0
    xvfmadd.d D21, D21, VALPHA,  U1
#endif

    /* Store C0 */
    xvst      D0,  C0,  0x00
    xvst      D1,  C0,  0x20
    /* Store C1 */
    xvst      D4,  C1,  0x00
    xvst      D5,  C1,  0x20
    /* Store C2 */
    xvst      D8,  C2,  0x00
    xvst      D9,  C2,  0x20
    /* Store C3 */
    xvst      D12, C3,  0x00
    xvst      D13, C3,  0x20
    /* Store C4 */
    xvst      D16, C4,  0x00
    xvst      D17, C4,  0x20
    /* Store C5 */
    xvst      D20,  C5,  0x00
    xvst      D21,  C5,  0x20

    /* Add stride for C */
    addi.d    C0,  C0,  0x40
    addi.d    C1,  C1,  0x40
    addi.d    C2,  C2,  0x40
    addi.d    C3,  C3,  0x40
    addi.d    C4,  C4,  0x40
    addi.d    C5,  C5,  0x40

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d     L,   K,   OFF
#ifdef LEFT
    /* number of values in A */
    addi.d    L,   L,   -8
#else
    /* number of values in B */
    addi.d    L,   L,   -6
#endif
    slli.d    T0,  L,  0x06
    add.d     A0,  A0, T0
    mul.d     T0,  L,  I48
    add.d     B0,  B0, T0
#endif

#ifdef LEFT
    /* number of values in A */
    addi.d    OFF,   OFF,  0x08
#endif
#endif   // #if defined(TRMMKERNEL)

/********LOOP (if(N / 6 ) && (M & 8)) End************/

.L_M4:
    andi      I,   M,   4
    beq       ZERO,I,   .L_M2

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    move     B0,    B
#else
    slli.d   T0,    OFF,  0x05
    add.d    A0,    A0,   T0
    mul.d    T0,    OFF,  I48
    add.d    B0,    B,    T0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    sub.d    L,     K,    OFF
#elif defined(LEFT)
    /* number of values in A */
    addi.d   L,     OFF,  4
#else
    /* number of values in B */
    addi.d   L,     OFF,  6
#endif
#else   // #if !defined(TRMMKERNEL)
    move     B0,    B
    move     L,     K /* L = bk */
#endif

    /* Load 4 * 64 from A0 */
    xvld     U0,   A0,    0x00

    xvldrepl.d     U4, B0, 0x00
    /* line 1 */
    xvfmul.d  D0,  U0, U4

    xvldrepl.d     U4, B0, 0x08
    /* line 2 */
    xvfmul.d  D4,  U0, U4

    xvldrepl.d     U4, B0, 0x10
    /* line 3 */
    xvfmul.d  D8,  U0, U4

    xvldrepl.d     U4, B0, 0x18
    /* line 4 */
    xvfmul.d  D12, U0, U4

    xvldrepl.d     U4, B0, 0x20
    /* line 5 */
    xvfmul.d  D16, U0, U4

    xvldrepl.d     U4, B0, 0x28
    /* line 6 */
    xvfmul.d  D20, U0, U4

    /* Add stride for A0 and B0 */
    addi.d    A0,  A0, 0x20
    addi.d    B0,  B0, 0x30
    /* Reduce L */
    addi.d    L,   L,  -1
    srai.d    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_M4_L7 */
    beq       ZERO,TL, .L_M4_L7

.L_M4_TL1: /* TL-- */
           /***8-1***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    xvldrepl.d U4,  B0, 0x20
    xvfmadd.d  D16, U0, U4, D16

    xvldrepl.d U4,  B0, 0x28
    xvfmadd.d  D20, U0, U4, D20

    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x30

           /***8-2***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    xvldrepl.d U4,  B0, 0x20
    xvfmadd.d  D16, U0, U4, D16

    xvldrepl.d U4,  B0, 0x28
    xvfmadd.d  D20, U0, U4, D20

    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x30

           /***8-3***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    xvldrepl.d U4,  B0, 0x20
    xvfmadd.d  D16, U0, U4, D16

    xvldrepl.d U4,  B0, 0x28
    xvfmadd.d  D20, U0, U4, D20

    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x30

           /***8-4***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    xvldrepl.d U4,  B0, 0x20
    xvfmadd.d  D16, U0, U4, D16

    xvldrepl.d U4,  B0, 0x28
    xvfmadd.d  D20, U0, U4, D20

    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x30

           /***8-5***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    xvldrepl.d U4,  B0, 0x20
    xvfmadd.d  D16, U0, U4, D16

    xvldrepl.d U4,  B0, 0x28
    xvfmadd.d  D20, U0, U4, D20

    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x30

           /***8-6***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    xvldrepl.d U4,  B0, 0x20
    xvfmadd.d  D16, U0, U4, D16

    xvldrepl.d U4,  B0, 0x28
    xvfmadd.d  D20, U0, U4, D20

    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x30

           /***8-7***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    xvldrepl.d U4,  B0, 0x20
    xvfmadd.d  D16, U0, U4, D16

    xvldrepl.d U4,  B0, 0x28
    xvfmadd.d  D20, U0, U4, D20

    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x30

           /***8-8***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    xvldrepl.d U4,  B0, 0x20
    xvfmadd.d  D16, U0, U4, D16

    xvldrepl.d U4,  B0, 0x28
    xvfmadd.d  D20, U0, U4, D20

    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x30

    addi.d    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_M4_TL1

.L_M4_L7:
    /* if (!(L & 7)) goto L_M4_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_M4_L0

.L_M4_L71:
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    xvldrepl.d U4,  B0, 0x20
    xvfmadd.d  D16, U0, U4, D16

    xvldrepl.d U4,  B0, 0x28
    xvfmadd.d  D20, U0, U4, D20

    /* Add stride for A0, B0 */
    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x30

    addi.d     TL,  TL, -1
    blt        ZERO,TL, .L_M4_L71

.L_M4_L0:
#if defined(TRMMKERNEL)
    xvfmul.d  D0,   D0,  VALPHA
    xvfmul.d  D4,   D4,  VALPHA
    xvfmul.d  D8,   D8,  VALPHA
    xvfmul.d  D12,  D12, VALPHA
    xvfmul.d  D16,  D16, VALPHA
    xvfmul.d  D20,  D20, VALPHA
#else
    /* Load C0  */
    xvld      U0,  C0,  0x00
    xvfmadd.d D0,  D0,  VALPHA,  U0 /* D0 = U0 + (D0 * VALPHA) */

    /* Load C1  */
    xvld      U0,  C1,  0x00
    xvfmadd.d D4,  D4,  VALPHA,  U0

    /* Load C2  */
    xvld      U0,  C2,  0x00
    xvfmadd.d D8,  D8,  VALPHA,  U0

    /* Load C3  */
    xvld      U0,  C3,  0x00
    xvfmadd.d D12, D12, VALPHA,  U0

    /* Load C4  */
    xvld      U0,  C4,  0x00
    xvfmadd.d D16, D16, VALPHA,  U0

    /* Load C5  */
    xvld      U0,  C5,  0x00
    xvfmadd.d D20, D20, VALPHA,  U0
#endif

    /* Store C0 */
    xvst      D0,  C0,  0x00
    /* Store C1 */
    xvst      D4,  C1,  0x00
    /* Store C2 */
    xvst      D8,  C2,  0x00
    /* Store C3 */
    xvst      D12, C3,  0x00
    /* Store C4 */
    xvst      D16, C4,  0x00
    /* Store C5 */
    xvst      D20, C5,  0x00

    /* Add stride for C */
    addi.d    C0,  C0,  0x20
    addi.d    C1,  C1,  0x20
    addi.d    C2,  C2,  0x20
    addi.d    C3,  C3,  0x20
    addi.d    C4,  C4,  0x20
    addi.d    C5,  C5,  0x20

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d     L,   K,   OFF
#ifdef LEFT
    /* number of values in A */
    addi.d    L,   L,   -4
#else
    /* number of values in B */
    addi.d    L,   L,   -6
#endif
    slli.d    T0,  L,  0x05
    add.d     A0,  A0, T0
    mul.d     T0,  L,  I48
    add.d     B0,  B0, T0
#endif

#ifdef LEFT
    /* number of values in A */
    addi.d    OFF,   OFF,  0x04
#endif
#endif   // #if defined(TRMMKERNEL)

/********LOOP (if(N / 6 ) && (M & 4) ) End************/

.L_M2:
    andi      I,   M,   2
    beq       ZERO,I,   .L_M1

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    move     B0,    B
#else
    slli.d   T0,    OFF,  0x04
    add.d    A0,    A0,   T0
    mul.d    T0,    OFF,  I48
    add.d    B0,    B,    T0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    sub.d    L,     K,    OFF
#elif defined(LEFT)
    /* number of values in A */
    addi.d   L,     OFF,  2
#else
    /* number of values in B */
    addi.d   L,     OFF,  6
#endif
#else   // #if !defined(TRMMKERNEL)
    move     B0,    B
    move     L,     K /* L = bk */
#endif

    /* Load 2 * 64 from A0 */
    xvld     U0,   A0,    0x00

    xvldrepl.d     U4, B0, 0x00
    /* line 1 */
    xvfmul.d  D0,  U0, U4

    xvldrepl.d     U4, B0, 0x08
    /* line 2 */
    xvfmul.d  D4,  U0, U4

    xvldrepl.d     U4, B0, 0x10
    /* line 3 */
    xvfmul.d  D8,  U0, U4

    xvldrepl.d     U4, B0, 0x18
    /* line 4 */
    xvfmul.d  D12, U0, U4

    xvldrepl.d     U4, B0, 0x20
    /* line 5 */
    xvfmul.d  D16, U0, U4

    xvldrepl.d     U4, B0, 0x28
    /* line 6 */
    xvfmul.d  D20, U0, U4

    /* Add stride for A0 and B0 */
    addi.d    A0,  A0, 0x10
    addi.d    B0,  B0, 0x30
    /* Reduce L */
    addi.d    L,   L,  -1
    srai.d    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_M2_L7 */
    beq       ZERO,TL, .L_M2_L7

.L_M2_TL1: /* TL-- */
           /***8-1***/
    /* Load 2 * 64 from A0 */
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    xvldrepl.d U4,  B0, 0x20
    xvfmadd.d  D16, U0, U4, D16

    xvldrepl.d U4,  B0, 0x28
    xvfmadd.d  D20, U0, U4, D20

    addi.d     A0,  A0, 0x10
    addi.d     B0,  B0, 0x30

           /***8-2***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    xvldrepl.d U4,  B0, 0x20
    xvfmadd.d  D16, U0, U4, D16

    xvldrepl.d U4,  B0, 0x28
    xvfmadd.d  D20, U0, U4, D20

    addi.d     A0,  A0, 0x10
    addi.d     B0,  B0, 0x30

           /***8-3***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    xvldrepl.d U4,  B0, 0x20
    xvfmadd.d  D16, U0, U4, D16

    xvldrepl.d U4,  B0, 0x28
    xvfmadd.d  D20, U0, U4, D20

    addi.d     A0,  A0, 0x10
    addi.d     B0,  B0, 0x30

           /***8-4***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    xvldrepl.d U4,  B0, 0x20
    xvfmadd.d  D16, U0, U4, D16

    xvldrepl.d U4,  B0, 0x28
    xvfmadd.d  D20, U0, U4, D20

    addi.d     A0,  A0, 0x10
    addi.d     B0,  B0, 0x30

           /***8-5***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    xvldrepl.d U4,  B0, 0x20
    xvfmadd.d  D16, U0, U4, D16

    xvldrepl.d U4,  B0, 0x28
    xvfmadd.d  D20, U0, U4, D20

    addi.d     A0,  A0, 0x10
    addi.d     B0,  B0, 0x30

           /***8-6***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    xvldrepl.d U4,  B0, 0x20
    xvfmadd.d  D16, U0, U4, D16

    xvldrepl.d U4,  B0, 0x28
    xvfmadd.d  D20, U0, U4, D20

    addi.d     A0,  A0, 0x10
    addi.d     B0,  B0, 0x30

           /***8-7***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    xvldrepl.d U4,  B0, 0x20
    xvfmadd.d  D16, U0, U4, D16

    xvldrepl.d U4,  B0, 0x28
    xvfmadd.d  D20, U0, U4, D20

    addi.d     A0,  A0, 0x10
    addi.d     B0,  B0, 0x30

           /***8-8***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    xvldrepl.d U4,  B0, 0x20
    xvfmadd.d  D16, U0, U4, D16

    xvldrepl.d U4,  B0, 0x28
    xvfmadd.d  D20, U0, U4, D20

    addi.d     A0,  A0, 0x10
    addi.d     B0,  B0, 0x30

    addi.d    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_M2_TL1

.L_M2_L7:
    /* if (!(L & 7)) goto L_M2_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_M2_L0

.L_M2_L71:
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    xvldrepl.d U4,  B0, 0x20
    xvfmadd.d  D16, U0, U4, D16

    xvldrepl.d U4,  B0, 0x28
    xvfmadd.d  D20, U0, U4, D20

    /* Add stride for A0, B0 */
    addi.d     A0,  A0, 0x10
    addi.d     B0,  B0, 0x30

    addi.d     TL,  TL, -1
    blt        ZERO,TL, .L_M2_L71

.L_M2_L0:
#if defined(TRMMKERNEL)
    xvfmul.d  D0,   D0,  VALPHA
    xvfmul.d  D4,   D4,  VALPHA
    xvfmul.d  D8,   D8,  VALPHA
    xvfmul.d  D12,  D12, VALPHA
    xvfmul.d  D16,  D16, VALPHA
    xvfmul.d  D20,  D20, VALPHA
#else
    /* Load C0  */
    xvld      U0,  C0,  0x00
    xvfmadd.d D0,  D0,  VALPHA,  U0 /* D0 = U0 + (D0 * VALPHA) */

    /* Load C1  */
    xvld      U0,  C1,  0x00
    xvfmadd.d D4,  D4,  VALPHA,  U0

    /* Load C2  */
    xvld      U0,  C2,  0x00
    xvfmadd.d D8,  D8,  VALPHA,  U0

    /* Load C3  */
    xvld      U0,  C3,  0x00
    xvfmadd.d D12, D12, VALPHA,  U0

    /* Load C4  */
    xvld      U0,  C4,  0x00
    xvfmadd.d D16, D16, VALPHA,  U0

    /* Load C5  */
    xvld      U0,  C5,  0x00
    xvfmadd.d D20, D20, VALPHA,  U0
#endif

    xvstelm.d D0,  C0,  0x00,    0x00
    xvstelm.d D4,  C1,  0x00,    0x00
    xvstelm.d D8,  C2,  0x00,    0x00
    xvstelm.d D12, C3,  0x00,    0x00
    xvstelm.d D16, C4,  0x00,    0x00
    xvstelm.d D20, C5,  0x00,    0x00
    xvstelm.d D0,  C0,  0x08,    0x01
    xvstelm.d D4,  C1,  0x08,    0x01
    xvstelm.d D8,  C2,  0x08,    0x01
    xvstelm.d D12, C3,  0x08,    0x01
    xvstelm.d D16, C4,  0x08,    0x01
    xvstelm.d D20, C5,  0x08,    0x01

    /* Add stride for C */
    addi.d    C0,  C0,  0x10
    addi.d    C1,  C1,  0x10
    addi.d    C2,  C2,  0x10
    addi.d    C3,  C3,  0x10
    addi.d    C4,  C4,  0x10
    addi.d    C5,  C5,  0x10

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d     L,   K,   OFF
#ifdef LEFT
    /* number of values in A */
    addi.d    L,   L,   -2
#else
    /* number of values in B */
    addi.d    L,   L,   -6
#endif
    slli.d    T0,  L,  0x04
    add.d     A0,  A0, T0
    mul.d     T0,  L,  I48
    add.d     B0,  B0, T0
#endif

#ifdef LEFT
    /* number of values in A */
    addi.d    OFF,   OFF,  0x02
#endif
#endif   // #if defined(TRMMKERNEL)

/********LOOP (if(N / 6 ) && (M & 2) ) End************/

.L_M1:
    andi      I,   M,   1
    beq       ZERO,I,   .L_M0

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    move     B0,    B
#else
    slli.d   T0,    OFF,  0x03
    add.d    A0,    A0,   T0
    mul.d    T0,    OFF,  I48
    add.d    B0,    B,    T0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    sub.d    L,     K,    OFF
#elif defined(LEFT)
    /* number of values in A */
    addi.d   L,     OFF,  1
#else
    /* number of values in B */
    addi.d   L,     OFF,  6
#endif
#else   // #if !defined(TRMMKERNEL)
    move     B0,    B
    move     L,     K /* L = bk */
#endif

    /* Load 1 * 64 from A0 */
    xvld     U0,   A0,    0x00

    xvldrepl.d     U4, B0, 0x00
    /* line 1 */
    xvfmul.d  D0,  U0, U4

    xvldrepl.d     U4, B0, 0x08
    /* line 2 */
    xvfmul.d  D4,  U0, U4

    xvldrepl.d     U4, B0, 0x10
    /* line 3 */
    xvfmul.d  D8,  U0, U4

    xvldrepl.d     U4, B0, 0x18
    /* line 4 */
    xvfmul.d  D12, U0, U4

    xvldrepl.d     U4, B0, 0x20
    /* line 5 */
    xvfmul.d  D16, U0, U4

    xvldrepl.d     U4, B0, 0x28
    /* line 6 */
    xvfmul.d  D20, U0, U4

    /* Add stride for A0 and B0 */
    addi.d    A0,  A0, 0x08
    addi.d    B0,  B0, 0x30
    /* Reduce L */
    addi.d    L,   L,  -1
    srai.d    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_M1_L7 */
    beq       ZERO,TL, .L_M1_L7

.L_M1_TL1: /* TL-- */
           /***8-1***/
    /* Load 1 * 64 from A0 */
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    xvldrepl.d U4,  B0, 0x20
    xvfmadd.d  D16, U0, U4, D16

    xvldrepl.d U4,  B0, 0x28
    xvfmadd.d  D20, U0, U4, D20

    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x30

           /***8-2***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    xvldrepl.d U4,  B0, 0x20
    xvfmadd.d  D16, U0, U4, D16

    xvldrepl.d U4,  B0, 0x28
    xvfmadd.d  D20, U0, U4, D20

    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x30

           /***8-3***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    xvldrepl.d U4,  B0, 0x20
    xvfmadd.d  D16, U0, U4, D16

    xvldrepl.d U4,  B0, 0x28
    xvfmadd.d  D20, U0, U4, D20

    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x30

           /***8-4***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    xvldrepl.d U4,  B0, 0x20
    xvfmadd.d  D16, U0, U4, D16

    xvldrepl.d U4,  B0, 0x28
    xvfmadd.d  D20, U0, U4, D20

    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x30

           /***8-5***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    xvldrepl.d U4,  B0, 0x20
    xvfmadd.d  D16, U0, U4, D16

    xvldrepl.d U4,  B0, 0x28
    xvfmadd.d  D20, U0, U4, D20

    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x30

           /***8-6***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    xvldrepl.d U4,  B0, 0x20
    xvfmadd.d  D16, U0, U4, D16

    xvldrepl.d U4,  B0, 0x28
    xvfmadd.d  D20, U0, U4, D20

    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x30

           /***8-7***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    xvldrepl.d U4,  B0, 0x20
    xvfmadd.d  D16, U0, U4, D16

    xvldrepl.d U4,  B0, 0x28
    xvfmadd.d  D20, U0, U4, D20

    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x30

           /***8-8***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    xvldrepl.d U4,  B0, 0x20
    xvfmadd.d  D16, U0, U4, D16

    xvldrepl.d U4,  B0, 0x28
    xvfmadd.d  D20, U0, U4, D20

    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x30

    addi.d    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_M1_TL1

.L_M1_L7:
    /* if (!(L & 7)) goto L_M1_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_M1_L0

.L_M1_L71:
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    xvldrepl.d U4,  B0, 0x20
    xvfmadd.d  D16, U0, U4, D16

    xvldrepl.d U4,  B0, 0x28
    xvfmadd.d  D20, U0, U4, D20

    /* Add stride for A0, B0 */
    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x30

    addi.d     TL,  TL, -1
    blt        ZERO,TL, .L_M1_L71

.L_M1_L0:
#ifdef TRMMKERNEL
    xvfmul.d  D0,   D0,  VALPHA
    xvfmul.d  D4,   D4,  VALPHA
    xvfmul.d  D8,   D8,  VALPHA
    xvfmul.d  D12,  D12, VALPHA
    xvfmul.d  D16,  D16, VALPHA
    xvfmul.d  D20,  D20, VALPHA
#else
    /* Load C0  */
    xvld      U0,  C0,  0x00
    xvfmadd.d D0,  D0,  VALPHA,  U0 /* D0 = U0 + (D0 * VALPHA) */

    /* Load C1  */
    xvld      U0,  C1,  0x00
    xvfmadd.d D4,  D4,  VALPHA,  U0

    /* Load C2  */
    xvld      U0,  C2,  0x00
    xvfmadd.d D8,  D8,  VALPHA,  U0

    /* Load C3  */
    xvld      U0,  C3,  0x00
    xvfmadd.d D12, D12, VALPHA,  U0

    /* Load C4  */
    xvld      U0,  C4,  0x00
    xvfmadd.d D16, D16, VALPHA,  U0

    /* Load C5  */
    xvld      U0,  C5,  0x00
    xvfmadd.d D20, D20, VALPHA,  U0
#endif

    xvstelm.d D0,  C0,  0x00,    0x00
    xvstelm.d D4,  C1,  0x00,    0x00
    xvstelm.d D8,  C2,  0x00,    0x00
    xvstelm.d D12, C3,  0x00,    0x00
    xvstelm.d D16, C4,  0x00,    0x00
    xvstelm.d D20, C5,  0x00,    0x00

    /* Add stride for C */
    addi.d    C0,  C0,  0x08
    addi.d    C1,  C1,  0x08
    addi.d    C2,  C2,  0x08
    addi.d    C3,  C3,  0x08
    addi.d    C4,  C4,  0x08
    addi.d    C5,  C5,  0x08

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d     L,   K,   OFF
#ifdef LEFT
    /* number of values in A */
    addi.d    L,   L,   -1
#else
    /* number of values in B */
    addi.d    L,   L,   -6
#endif
    slli.d    T0,  L,  0x03
    add.d     A0,  A0, T0
    mul.d     T0,  L,  I48
    add.d     B0,  B0, T0
#endif

#ifdef LEFT
    /* number of values in A */
    addi.d    OFF,   OFF,  0x01
#endif
#endif   // #if defined(TRMMKERNEL)

/********LOOP (if(N / 6 ) && (M & 1) ) End************/

.L_M0:
    /* Add stride for B and C
     * B += (K * 6)
     * C += (LDC * 6)
     */
    /* since the array type is double,
     * so we must mul 48
     */
    addi.d    T2,   ZERO,48
    mul.d     T0,   K,   T2
    mul.d     T1,   LDC, T2
    add.d     B,    B,   T0
    add.d     C,    C,   T1

#if defined(TRMMKERNEL) && !defined(LEFT)
    addi.d    OFF,  OFF, 0x06
#endif

    blt      ZERO,  J,   .L_J1

//////////////// go back to L_J1 /////////////////
/////////////////////////////////////////////////
/************************ Condition 1 if((N >> 2) && (M >> 3)) END !!! ************************/

.L_N5:
    andi     J,    N,   4
    beq      ZERO, J,   .L_N3

/************************* Condition 2 if((N & 4) && (M >> 4)) START !!! *************************
*                                                   dgemm_core_16x4                                */

    move     C0,    C
    move     A0,    A
    slli.d   T0,    LDC,   3
    add.d    C1,    C0,    T0
    add.d    C2,    C1,    T0
    add.d    C3,    C2,    T0

#if defined(TRMMKERNEL) && defined(LEFT)
    move     OFF,   OFFSET
#endif

    /* if (!(M >> 3)) goto L_N5_M8 */
    srai.d   I,     M,     4     /* I = bm >> 4 */
    beq      ZERO,  I,     .L_N5_M8

.L_N5_I1:
#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    move     B0,    B
#else
    slli.d   T0,    OFF,  0x07
    add.d    A0,    A0,   T0
    slli.d   T0,    OFF,  0x05
    add.d    B0,    B,    T0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    sub.d    L,     K,    OFF
#elif defined(LEFT)
    /* number of values in A */
    addi.d   L,     OFF,  16
#else
    /* number of values in B */
    addi.d   L,     OFF,  4
#endif
#else   // #if !defined(TRMMKERNEL)
    move     B0,    B
    move     L,     K /* L = bk */
#endif
    /* Load 16 * 64 from A0
     * U0 = {a3,  a2,  a1,  a0}
     * U1 = {a7,  a6,  a5,  a4}
     * U2 = {a11, a10, a9,  a8}
     * U3 = {a15, a14, a13, a12}
     */
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20
    xvld     U2,   A0,    0x40
    xvld     U3,   A0,    0x60

    xvldrepl.d     U4, B0, 0x00
    /* line 1 */
    xvfmul.d  D0,  U0, U4
    xvfmul.d  D1,  U1, U4
    xvfmul.d  D2,  U2, U4
    xvfmul.d  D3,  U3, U4

    xvldrepl.d     U4, B0, 0x08
    /* line 2 */
    xvfmul.d  D4,  U0, U4
    xvfmul.d  D5,  U1, U4
    xvfmul.d  D6,  U2, U4
    xvfmul.d  D7,  U3, U4

    xvldrepl.d     U4, B0, 0x10
    /* line 3 */
    xvfmul.d  D8,  U0, U4
    xvfmul.d  D9,  U1, U4
    xvfmul.d  D10, U2, U4
    xvfmul.d  D11, U3, U4

    xvldrepl.d     U4, B0, 0x18
    /* line 4 */
    xvfmul.d  D12, U0, U4
    xvfmul.d  D13, U1, U4
    xvfmul.d  D14, U2, U4
    xvfmul.d  D15, U3, U4

    /* Add stride for A0 and B0 */
    addi.d    A0,  A0, 0x80
    addi.d    B0,  B0, 0x20
    /* Reduce L */
    addi.d    L,   L,  -1
    srai.d    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_N5_L7 */
    beq       ZERO,TL, .L_N5_L7

.L_N5_TL1: /* TL-- */
           /***8-1***/
    /* Load 16 * 64 from A0 */
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20
    xvld     U2,   A0,    0x40
    xvld     U3,   A0,    0x60

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1
    xvfmadd.d  D2,  U2, U4, D2
    xvfmadd.d  D3,  U3, U4, D3

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4
    xvfmadd.d  D5,  U1, U4, D5
    xvfmadd.d  D6,  U2, U4, D6
    xvfmadd.d  D7,  U3, U4, D7

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8
    xvfmadd.d  D9,  U1, U4, D9
    xvfmadd.d  D10, U2, U4, D10
    xvfmadd.d  D11, U3, U4, D11

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12
    xvfmadd.d  D13, U1, U4, D13
    xvfmadd.d  D14, U2, U4, D14
    xvfmadd.d  D15, U3, U4, D15

    addi.d     A0,  A0, 0x80
    addi.d     B0,  B0, 0x20

           /***8-2***/
    /* Load 16 * 64 from A0 */
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20
    xvld     U2,   A0,    0x40
    xvld     U3,   A0,    0x60

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1
    xvfmadd.d  D2,  U2, U4, D2
    xvfmadd.d  D3,  U3, U4, D3

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4
    xvfmadd.d  D5,  U1, U4, D5
    xvfmadd.d  D6,  U2, U4, D6
    xvfmadd.d  D7,  U3, U4, D7

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8
    xvfmadd.d  D9,  U1, U4, D9
    xvfmadd.d  D10, U2, U4, D10
    xvfmadd.d  D11, U3, U4, D11

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12
    xvfmadd.d  D13, U1, U4, D13
    xvfmadd.d  D14, U2, U4, D14
    xvfmadd.d  D15, U3, U4, D15

    addi.d     A0,  A0, 0x80
    addi.d     B0,  B0, 0x20

           /***8-3***/
    /* Load 16 * 64 from A0 */
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20
    xvld     U2,   A0,    0x40
    xvld     U3,   A0,    0x60

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1
    xvfmadd.d  D2,  U2, U4, D2
    xvfmadd.d  D3,  U3, U4, D3

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4
    xvfmadd.d  D5,  U1, U4, D5
    xvfmadd.d  D6,  U2, U4, D6
    xvfmadd.d  D7,  U3, U4, D7

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8
    xvfmadd.d  D9,  U1, U4, D9
    xvfmadd.d  D10, U2, U4, D10
    xvfmadd.d  D11, U3, U4, D11

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12
    xvfmadd.d  D13, U1, U4, D13
    xvfmadd.d  D14, U2, U4, D14
    xvfmadd.d  D15, U3, U4, D15

    addi.d     A0,  A0, 0x80
    addi.d     B0,  B0, 0x20

           /***8-4***/
    /* Load 16 * 64 from A0 */
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20
    xvld     U2,   A0,    0x40
    xvld     U3,   A0,    0x60

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1
    xvfmadd.d  D2,  U2, U4, D2
    xvfmadd.d  D3,  U3, U4, D3

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4
    xvfmadd.d  D5,  U1, U4, D5
    xvfmadd.d  D6,  U2, U4, D6
    xvfmadd.d  D7,  U3, U4, D7

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8
    xvfmadd.d  D9,  U1, U4, D9
    xvfmadd.d  D10, U2, U4, D10
    xvfmadd.d  D11, U3, U4, D11

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12
    xvfmadd.d  D13, U1, U4, D13
    xvfmadd.d  D14, U2, U4, D14
    xvfmadd.d  D15, U3, U4, D15

    addi.d     A0,  A0, 0x80
    addi.d     B0,  B0, 0x20

           /***8-5***/
    /* Load 16 * 64 from A0 */
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20
    xvld     U2,   A0,    0x40
    xvld     U3,   A0,    0x60

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1
    xvfmadd.d  D2,  U2, U4, D2
    xvfmadd.d  D3,  U3, U4, D3

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4
    xvfmadd.d  D5,  U1, U4, D5
    xvfmadd.d  D6,  U2, U4, D6
    xvfmadd.d  D7,  U3, U4, D7

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8
    xvfmadd.d  D9,  U1, U4, D9
    xvfmadd.d  D10, U2, U4, D10
    xvfmadd.d  D11, U3, U4, D11

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12
    xvfmadd.d  D13, U1, U4, D13
    xvfmadd.d  D14, U2, U4, D14
    xvfmadd.d  D15, U3, U4, D15

    addi.d     A0,  A0, 0x80
    addi.d     B0,  B0, 0x20

           /***8-6***/
    /* Load 16 * 64 from A0 */
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20
    xvld     U2,   A0,    0x40
    xvld     U3,   A0,    0x60

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1
    xvfmadd.d  D2,  U2, U4, D2
    xvfmadd.d  D3,  U3, U4, D3

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4
    xvfmadd.d  D5,  U1, U4, D5
    xvfmadd.d  D6,  U2, U4, D6
    xvfmadd.d  D7,  U3, U4, D7

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8
    xvfmadd.d  D9,  U1, U4, D9
    xvfmadd.d  D10, U2, U4, D10
    xvfmadd.d  D11, U3, U4, D11

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12
    xvfmadd.d  D13, U1, U4, D13
    xvfmadd.d  D14, U2, U4, D14
    xvfmadd.d  D15, U3, U4, D15

    addi.d     A0,  A0, 0x80
    addi.d     B0,  B0, 0x20

           /***8-7***/
    /* Load 16 * 64 from A0 */
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20
    xvld     U2,   A0,    0x40
    xvld     U3,   A0,    0x60

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1
    xvfmadd.d  D2,  U2, U4, D2
    xvfmadd.d  D3,  U3, U4, D3

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4
    xvfmadd.d  D5,  U1, U4, D5
    xvfmadd.d  D6,  U2, U4, D6
    xvfmadd.d  D7,  U3, U4, D7

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8
    xvfmadd.d  D9,  U1, U4, D9
    xvfmadd.d  D10, U2, U4, D10
    xvfmadd.d  D11, U3, U4, D11

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12
    xvfmadd.d  D13, U1, U4, D13
    xvfmadd.d  D14, U2, U4, D14
    xvfmadd.d  D15, U3, U4, D15

    addi.d     A0,  A0, 0x80
    addi.d     B0,  B0, 0x20

           /***8-8***/
    /* Load 16 * 64 from A0 */
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20
    xvld     U2,   A0,    0x40
    xvld     U3,   A0,    0x60

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1
    xvfmadd.d  D2,  U2, U4, D2
    xvfmadd.d  D3,  U3, U4, D3

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4
    xvfmadd.d  D5,  U1, U4, D5
    xvfmadd.d  D6,  U2, U4, D6
    xvfmadd.d  D7,  U3, U4, D7

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8
    xvfmadd.d  D9,  U1, U4, D9
    xvfmadd.d  D10, U2, U4, D10
    xvfmadd.d  D11, U3, U4, D11

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12
    xvfmadd.d  D13, U1, U4, D13
    xvfmadd.d  D14, U2, U4, D14
    xvfmadd.d  D15, U3, U4, D15

    addi.d     A0,  A0, 0x80
    addi.d     B0,  B0, 0x20

    addi.d    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_N5_TL1

.L_N5_L7:
    /* if (!(L & 7)) goto L_N5_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_N5_L0

.L_N5_L71:
    /* Load 16 * 64 from A0 */
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20
    xvld     U2,   A0,    0x40
    xvld     U3,   A0,    0x60

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1
    xvfmadd.d  D2,  U2, U4, D2
    xvfmadd.d  D3,  U3, U4, D3

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4
    xvfmadd.d  D5,  U1, U4, D5
    xvfmadd.d  D6,  U2, U4, D6
    xvfmadd.d  D7,  U3, U4, D7

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8
    xvfmadd.d  D9,  U1, U4, D9
    xvfmadd.d  D10, U2, U4, D10
    xvfmadd.d  D11, U3, U4, D11

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12
    xvfmadd.d  D13, U1, U4, D13
    xvfmadd.d  D14, U2, U4, D14
    xvfmadd.d  D15, U3, U4, D15

    /* Add stride for A0, B0 */
    addi.d     A0,  A0, 0x80
    addi.d     B0,  B0, 0x20

    addi.d     TL,  TL, -1
    blt        ZERO,TL, .L_N5_L71

.L_N5_L0:
#if defined(TRMMKERNEL)
    xvfmul.d  D0,   D0,  VALPHA
    xvfmul.d  D1,   D1,  VALPHA
    xvfmul.d  D2,   D2,  VALPHA
    xvfmul.d  D3,   D3,  VALPHA
    xvfmul.d  D4,   D4,  VALPHA
    xvfmul.d  D5,   D5,  VALPHA
    xvfmul.d  D6,   D6,  VALPHA
    xvfmul.d  D7,   D7,  VALPHA
    xvfmul.d  D8,   D8,  VALPHA
    xvfmul.d  D9,   D9,  VALPHA
    xvfmul.d  D10,  D10, VALPHA
    xvfmul.d  D11,  D11, VALPHA
    xvfmul.d  D12,  D12, VALPHA
    xvfmul.d  D13,  D13, VALPHA
    xvfmul.d  D14,  D14, VALPHA
    xvfmul.d  D15,  D15, VALPHA
#else
    /* Load C0  */
    xvld      U0,  C0,  0x00
    xvld      U1,  C0,  0x20
    xvld      U2,  C0,  0x40
    xvld      U3,  C0,  0x60
    xvfmadd.d D0,  D0,  VALPHA,  U0 /* D0 = U0 + (D0 * VALPHA) */
    xvfmadd.d D1,  D1,  VALPHA,  U1
    xvfmadd.d D2,  D2,  VALPHA,  U2
    xvfmadd.d D3,  D3,  VALPHA,  U3

    /* Load C1  */
    xvld      U0,  C1,  0x00
    xvld      U1,  C1,  0x20
    xvld      U2,  C1,  0x40
    xvld      U3,  C1,  0x60
    xvfmadd.d D4,  D4,  VALPHA,  U0
    xvfmadd.d D5,  D5,  VALPHA,  U1
    xvfmadd.d D6,  D6,  VALPHA,  U2
    xvfmadd.d D7,  D7,  VALPHA,  U3

    /* Load C2  */
    xvld      U0,  C2,  0x00
    xvld      U1,  C2,  0x20
    xvld      U2,  C2,  0x40
    xvld      U3,  C2,  0x60
    xvfmadd.d D8,  D8,  VALPHA,  U0
    xvfmadd.d D9,  D9,  VALPHA,  U1
    xvfmadd.d D10, D10, VALPHA,  U2
    xvfmadd.d D11, D11, VALPHA,  U3

    /* Load C3  */
    xvld      U0,  C3,  0x00
    xvld      U1,  C3,  0x20
    xvld      U2,  C3,  0x40
    xvld      U3,  C3,  0x60
    xvfmadd.d D12, D12, VALPHA,  U0
    xvfmadd.d D13, D13, VALPHA,  U1
    xvfmadd.d D14, D14, VALPHA,  U2
    xvfmadd.d D15, D15, VALPHA,  U3
 #endif

    /* Store C0 */
    xvst      D0,  C0,  0x00
    xvst      D1,  C0,  0x20
    xvst      D2,  C0,  0x40
    xvst      D3,  C0,  0x60
    /* Store C1 */
    xvst      D4,  C1,  0x00
    xvst      D5,  C1,  0x20
    xvst      D6,  C1,  0x40
    xvst      D7,  C1,  0x60
    /* Store C2 */
    xvst      D8,  C2,  0x00
    xvst      D9,  C2,  0x20
    xvst      D10, C2,  0x40
    xvst      D11, C2,  0x60
    /* Store C3 */
    xvst      D12, C3,  0x00
    xvst      D13, C3,  0x20
    xvst      D14, C3,  0x40
    xvst      D15, C3,  0x60

    /* Add stride for C */
    addi.d    C0,  C0,  0x80
    addi.d    C1,  C1,  0x80
    addi.d    C2,  C2,  0x80
    addi.d    C3,  C3,  0x80

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d     L,   K,   OFF
#ifdef LEFT
    /* number of values  in A */
    addi.d    L,   L,   -16
#else
    /* number of values in B */
    addi.d    L,   L,   -4
#endif
    slli.d    T0,  L,  0x07
    add.d     A0,  A0, T0
    slli.d    T0,  L,  0x05
    add.d     B0,  B0, T0
#endif

#ifdef LEFT
    addi.d    OFF, OFF, 0x10
#endif
#endif   // #if defined(TRMMKERNEL)

    addi.d    I,   I,   -1  /* I-- */
    blt       ZERO,I,   .L_N5_I1

.L_N5_M8:
    /* We have done M & 16, considering M=8/4/2/1 */
    andi      I,   M,   15
    beq       ZERO,I,   .L_N5_M0

    andi      I,   M,   8
    beq       ZERO,I,   .L_N5_M4

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    move     B0,    B
#else
    slli.d   T0,    OFF,  0x06
    add.d    A0,    A0,   T0
    slli.d   T0,    OFF,  0x05
    add.d    B0,    B,    T0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    sub.d    L,     K,    OFF
#elif defined(LEFT)
    /* number of values in A */
    addi.d   L,     OFF,  8
#else
    /* number of values in B */
    addi.d   L,     OFF,  4
#endif
#else   // #if !defined(TRMMKERNEL)
    move     B0,    B
    move     L,     K /* L = bk */
#endif  // #if defined(TRMMKERNEL)

    /* Load 8 * 64 from A0 */
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20

    xvldrepl.d     U4, B0, 0x00
    /* line 1 */
    xvfmul.d  D0,  U0, U4
    xvfmul.d  D1,  U1, U4

    xvldrepl.d     U4, B0, 0x08
    /* line 2 */
    xvfmul.d  D4,  U0, U4
    xvfmul.d  D5,  U1, U4

    xvldrepl.d     U4, B0, 0x10
    /* line 3 */
    xvfmul.d  D8,  U0, U4
    xvfmul.d  D9,  U1, U4

    xvldrepl.d     U4, B0, 0x18
    /* line 4 */
    xvfmul.d  D12, U0, U4
    xvfmul.d  D13, U1, U4

    /* Add stride for A0 and B0 */
    addi.d    A0,  A0, 0x40
    addi.d    B0,  B0, 0x20
    /* Reduce L */
    addi.d    L,   L,  -1
    srai.d    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_N5_M8_L7 */
    beq       ZERO,TL, .L_N5_M8_L7

.L_N5_M8_TL1: /* TL-- */
           /***8-1***/
    /* Load 16 * 64 from A0 */
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4
    xvfmadd.d  D5,  U1, U4, D5

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8
    xvfmadd.d  D9,  U1, U4, D9

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12
    xvfmadd.d  D13, U1, U4, D13

    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x20

           /***8-2***/
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4
    xvfmadd.d  D5,  U1, U4, D5

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8
    xvfmadd.d  D9,  U1, U4, D9

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12
    xvfmadd.d  D13, U1, U4, D13

    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x20

           /***8-3***/
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4
    xvfmadd.d  D5,  U1, U4, D5

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8
    xvfmadd.d  D9,  U1, U4, D9

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12
    xvfmadd.d  D13, U1, U4, D13

    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x20

           /***8-4***/
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4
    xvfmadd.d  D5,  U1, U4, D5

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8
    xvfmadd.d  D9,  U1, U4, D9

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12
    xvfmadd.d  D13, U1, U4, D13

    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x20

           /***8-5***/
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20

    /* Cumulative D0~D23 */
    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4
    xvfmadd.d  D5,  U1, U4, D5

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8
    xvfmadd.d  D9,  U1, U4, D9

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12
    xvfmadd.d  D13, U1, U4, D13

    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x20

           /***8-6***/
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4
    xvfmadd.d  D5,  U1, U4, D5

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8
    xvfmadd.d  D9,  U1, U4, D9

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12
    xvfmadd.d  D13, U1, U4, D13

    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x20

           /***8-7***/
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4
    xvfmadd.d  D5,  U1, U4, D5

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8
    xvfmadd.d  D9,  U1, U4, D9

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12
    xvfmadd.d  D13, U1, U4, D13

    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x20

           /***8-8***/
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4
    xvfmadd.d  D5,  U1, U4, D5

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8
    xvfmadd.d  D9,  U1, U4, D9

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12
    xvfmadd.d  D13, U1, U4, D13

    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x20

    addi.d    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_N5_M8_TL1

.L_N5_M8_L7:
    /* if (!(L & 7)) goto L_N5_M8_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_N5_M8_L0

.L_N5_M8_L71:
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4
    xvfmadd.d  D5,  U1, U4, D5

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8
    xvfmadd.d  D9,  U1, U4, D9

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12
    xvfmadd.d  D13, U1, U4, D13

    /* Add stride for A0, B0 */
    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x20

    addi.d     TL,  TL, -1
    blt        ZERO,TL, .L_N5_M8_L71

.L_N5_M8_L0:
#if defined(TRMMKERNEL)
    xvfmul.d  D0,   D0,  VALPHA
    xvfmul.d  D1,   D1,  VALPHA
    xvfmul.d  D4,   D4,  VALPHA
    xvfmul.d  D5,   D5,  VALPHA
    xvfmul.d  D8,   D8,  VALPHA
    xvfmul.d  D9,   D9,  VALPHA
    xvfmul.d  D12,  D12, VALPHA
    xvfmul.d  D13,  D13, VALPHA
#else
    /* Load C0  */
    xvld      U0,  C0,  0x00
    xvld      U1,  C0,  0x20
    xvfmadd.d D0,  D0,  VALPHA,  U0 /* D0 = U0 + (D0 * VALPHA) */
    xvfmadd.d D1,  D1,  VALPHA,  U1

    /* Load C1  */
    xvld      U0,  C1,  0x00
    xvld      U1,  C1,  0x20
    xvfmadd.d D4,  D4,  VALPHA,  U0
    xvfmadd.d D5,  D5,  VALPHA,  U1

    /* Load C2  */
    xvld      U0,  C2,  0x00
    xvld      U1,  C2,  0x20
    xvfmadd.d D8,  D8,  VALPHA,  U0
    xvfmadd.d D9,  D9,  VALPHA,  U1

    /* Load C3  */
    xvld      U0,  C3,  0x00
    xvld      U1,  C3,  0x20
    xvfmadd.d D12, D12, VALPHA,  U0
    xvfmadd.d D13, D13, VALPHA,  U1
#endif

    /* Store C0 */
    xvst      D0,  C0,  0x00
    xvst      D1,  C0,  0x20
    /* Store C1 */
    xvst      D4,  C1,  0x00
    xvst      D5,  C1,  0x20
    /* Store C2 */
    xvst      D8,  C2,  0x00
    xvst      D9,  C2,  0x20
    /* Store C3 */
    xvst      D12, C3,  0x00
    xvst      D13, C3,  0x20

    /* Add stride for C */
    addi.d    C0,  C0,  0x40
    addi.d    C1,  C1,  0x40
    addi.d    C2,  C2,  0x40
    addi.d    C3,  C3,  0x40

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d     L,   K,   OFF
#ifdef LEFT
    /* number of values in A */
    addi.d    L,   L,   -8
#else
    /* number of values in B */
    addi.d    L,   L,   -4
#endif
    slli.d    T0,  L,  0x06
    add.d     A0,  A0, T0
    slli.d    T0,  L,  0x05
    add.d     B0,  B0, T0
#endif

#ifdef LEFT
    /* number of values in A */
    addi.d    OFF,   OFF,  0x08
#endif
#endif   // #if defined(TRMMKERNEL)

/********LOOP (if(N & 4 ) && (M & 8) ) End************/

.L_N5_M4:
    andi      I,   M,   4
    beq       ZERO,I,   .L_N5_M2

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    move     B0,    B
#else
    slli.d   T0,    OFF,  0x05
    add.d    A0,    A0,   T0
    add.d    B0,    B,    T0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    sub.d    L,     K,    OFF
#elif defined(LEFT)
    /* number of values in A */
    addi.d   L,     OFF,  4
#else
    /* number of values in B */
    addi.d   L,     OFF,  4
#endif
#else   // #if !defined(TRMMKERNEL)
    move     B0,    B
    move     L,     K /* L = bk */
#endif

    /* Load 4 * 64 from A0 */
    xvld     U0,   A0,    0x00

    xvldrepl.d     U4, B0, 0x00
    /* line 1 */
    xvfmul.d  D0,  U0, U4

    xvldrepl.d     U4, B0, 0x08
    /* line 2 */
    xvfmul.d  D4,  U0, U4

    xvldrepl.d     U4, B0, 0x10
    /* line 3 */
    xvfmul.d  D8,  U0, U4

    xvldrepl.d     U4, B0, 0x18
    /* line 4 */
    xvfmul.d  D12, U0, U4

    /* Add stride for A0 and B0 */
    addi.d    A0,  A0, 0x20
    addi.d    B0,  B0, 0x20
    /* Reduce L */
    addi.d    L,   L,  -1
    srai.d    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_N5_M4_L7 */
    beq       ZERO,TL, .L_N5_M4_L7

.L_N5_M4_TL1: /* TL-- */
           /***8-1***/
    /* Load 8 * 64 from A0 */
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x20

           /***8-2***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x20

           /***8-3***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x20

           /***8-4***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x20

           /***8-5***/
    xvld     U0,   A0,    0x00

    /* Cumulative D0~D23 */
    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x20

           /***8-6***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x20

           /***8-7***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x20

           /***8-8***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x20

    addi.d    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_N5_M4_TL1

.L_N5_M4_L7:
    /* if (!(L & 7)) goto L_N5_M4_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_N5_M4_L0

.L_N5_M4_L71:
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    /* Add stride for A0, B0 */
    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x20

    addi.d     TL,  TL, -1
    blt        ZERO,TL, .L_N5_M4_L71

.L_N5_M4_L0:
#if defined(TRMMKERNEL)
    xvfmul.d  D0,   D0,  VALPHA
    xvfmul.d  D4,   D4,  VALPHA
    xvfmul.d  D8,   D8,  VALPHA
    xvfmul.d  D12,  D12, VALPHA
#else
    /* Load C0  */
    xvld      U0,  C0,  0x00
    xvfmadd.d D0,  D0,  VALPHA,  U0 /* D0 = U0 + (D0 * VALPHA) */

    /* Load C1  */
    xvld      U0,  C1,  0x00
    xvfmadd.d D4,  D4,  VALPHA,  U0

    /* Load C2  */
    xvld      U0,  C2,  0x00
    xvfmadd.d D8,  D8,  VALPHA,  U0

    /* Load C3  */
    xvld      U0,  C3,  0x00
    xvfmadd.d D12, D12, VALPHA,  U0
#endif

    /* Store C0 */
    xvst      D0,  C0,  0x00
    /* Store C1 */
    xvst      D4,  C1,  0x00
    /* Store C2 */
    xvst      D8,  C2,  0x00
    /* Store C3 */
    xvst      D12, C3,  0x00

    /* Add stride for C */
    addi.d    C0,  C0,  0x20
    addi.d    C1,  C1,  0x20
    addi.d    C2,  C2,  0x20
    addi.d    C3,  C3,  0x20

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d     L,   K,   OFF
#ifdef LEFT
    /* number of values in A */
    addi.d    L,   L,   -4
#else
    /* number of values in B */
    addi.d    L,   L,   -4
#endif
    slli.d    T0,  L,  0x05
    add.d     A0,  A0, T0
    add.d     B0,  B0, T0
#endif

#ifdef LEFT
    /* number of values in A */
    addi.d    OFF,   OFF,  0x04
#endif
#endif   // #if defined(TRMMKERNEL)

/********LOOP (if(N & 4 ) && (M & 4) ) End************/

.L_N5_M2:
    andi      I,   M,   2
    beq       ZERO,I,   .L_N5_M1

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    move     B0,    B
#else
    slli.d   T0,    OFF,  0x04
    add.d    A0,    A0,   T0
    slli.d   T0,    OFF,  0x05
    add.d    B0,    B,    T0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    sub.d    L,     K,    OFF
#elif defined(LEFT)
    /* number of values in A */
    addi.d   L,     OFF,  2
#else
    /* number of values in B */
    addi.d   L,     OFF,  4
#endif
#else   // #if !defined(TRMMKERNEL)
    move     B0,    B
    move     L,     K /* L = bk */
#endif

    /* Load 2 * 64 from A0 */
    xvld     U0,   A0,    0x00

    xvldrepl.d     U4, B0, 0x00
    /* line 1 */
    xvfmul.d  D0,  U0, U4

    xvldrepl.d     U4, B0, 0x08
    /* line 2 */
    xvfmul.d  D4,  U0, U4

    xvldrepl.d     U4, B0, 0x10
    /* line 3 */
    xvfmul.d  D8,  U0, U4

    xvldrepl.d     U4, B0, 0x18
    /* line 4 */
    xvfmul.d  D12, U0, U4

    /* Add stride for A0 and B0 */
    addi.d    A0,  A0, 0x10
    addi.d    B0,  B0, 0x20
    /* Reduce L */
    addi.d    L,   L,  -1
    srai.d    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_N5_M2_L7 */
    beq       ZERO,TL, .L_N5_M2_L7

.L_N5_M2_TL1: /* TL-- */
           /***8-1***/
    /* Load 2 * 64 from A0 */
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    addi.d     A0,  A0, 0x10
    addi.d     B0,  B0, 0x20

           /***8-2***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    addi.d     A0,  A0, 0x10
    addi.d     B0,  B0, 0x20

           /***8-3***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    addi.d     A0,  A0, 0x10
    addi.d     B0,  B0, 0x20

           /***8-4***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    addi.d     A0,  A0, 0x10
    addi.d     B0,  B0, 0x20

           /***8-5***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    addi.d     A0,  A0, 0x10
    addi.d     B0,  B0, 0x20

           /***8-6***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    addi.d     A0,  A0, 0x10
    addi.d     B0,  B0, 0x20

           /***8-7***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    addi.d     A0,  A0, 0x10
    addi.d     B0,  B0, 0x20

           /***8-8***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    addi.d     A0,  A0, 0x10
    addi.d     B0,  B0, 0x20

    addi.d    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_N5_M2_TL1

.L_N5_M2_L7:
    /* if (!(L & 7)) goto L_N5_M2_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_N5_M2_L0

.L_N5_M2_L71:
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    /* Add stride for A0, B0 */
    addi.d     A0,  A0, 0x10
    addi.d     B0,  B0, 0x20

    addi.d     TL,  TL, -1
    blt        ZERO,TL, .L_N5_M2_L71

.L_N5_M2_L0:
#if defined(TRMMKERNEL)
    xvfmul.d  D0,   D0,  VALPHA
    xvfmul.d  D4,   D4,  VALPHA
    xvfmul.d  D8,   D8,  VALPHA
    xvfmul.d  D12,  D12, VALPHA
 #else
    /* Load C0  */
    xvld      U0,  C0,  0x00
    xvfmadd.d D0,  D0,  VALPHA,  U0 /* D0 = U0 + (D0 * VALPHA) */

    /* Load C1  */
    xvld      U0,  C1,  0x00
    xvfmadd.d D4,  D4,  VALPHA,  U0

    /* Load C2  */
    xvld      U0,  C2,  0x00
    xvfmadd.d D8,  D8,  VALPHA,  U0

    /* Load C3  */
    xvld      U0,  C3,  0x00
    xvfmadd.d D12, D12, VALPHA,  U0
 #endif

    xvstelm.d D0,  C0,  0x00,    0x00
    xvstelm.d D4,  C1,  0x00,    0x00
    xvstelm.d D8,  C2,  0x00,    0x00
    xvstelm.d D12, C3,  0x00,    0x00
    xvstelm.d D0,  C0,  0x08,    0x01
    xvstelm.d D4,  C1,  0x08,    0x01
    xvstelm.d D8,  C2,  0x08,    0x01
    xvstelm.d D12, C3,  0x08,    0x01

    /* Add stride for C */
    addi.d    C0,  C0,  0x10
    addi.d    C1,  C1,  0x10
    addi.d    C2,  C2,  0x10
    addi.d    C3,  C3,  0x10

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d     L,   K,   OFF
#ifdef LEFT
    /* number of values in A */
    addi.d    L,   L,   -2
#else
    /* number of values in B */
    addi.d    L,   L,   -4
#endif
    slli.d    T0,  L,  0x04
    add.d     A0,  A0, T0
    slli.d    T0,  L,  0x05
    add.d     B0,  B0, T0
#endif

#ifdef LEFT
    /* number of values in A */
    addi.d    OFF,   OFF,  0x02
#endif
#endif   // #if defined(TRMMKERNEL)

/********LOOP (if(N & 4 ) && (M & 2) ) End************/

.L_N5_M1:
    andi      I,   M,   1
    beq       ZERO,I,   .L_N5_M0

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    move     B0,    B
#else
    slli.d   T0,    OFF,  0x03
    add.d    A0,    A0,   T0
    slli.d   T0,    OFF,  0x05
    add.d    B0,    B,    T0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    sub.d    L,     K,    OFF
#elif defined(LEFT)
    /* number of values in A */
    addi.d   L,     OFF,  1
#else
    /* number of values in B */
    addi.d   L,     OFF,  4
#endif
#else   // #if !defined(TRMMKERNEL)
    move     B0,    B
    move     L,     K /* L = bk */
#endif

    /* Load 1 * 64 from A0 */
    xvld     U0,   A0,    0x00

    xvldrepl.d     U4, B0, 0x00
    /* line 1 */
    xvfmul.d  D0,  U0, U4

    xvldrepl.d     U4, B0, 0x08
    /* line 2 */
    xvfmul.d  D4,  U0, U4

    xvldrepl.d     U4, B0, 0x10
    /* line 3 */
    xvfmul.d  D8,  U0, U4

    xvldrepl.d     U4, B0, 0x18
    /* line 4 */
    xvfmul.d  D12, U0, U4

    /* Add stride for A0 and B0 */
    addi.d    A0,  A0, 0x08
    addi.d    B0,  B0, 0x20
    /* Reduce L */
    addi.d    L,   L,  -1
    srai.d    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_N5_M1_L7 */
    beq       ZERO,TL, .L_N5_M1_L7

.L_N5_M1_TL1: /* TL-- */
           /***8-1***/
    /* Load 1 * 64 from A0 */
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x20

           /***8-2***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x20

           /***8-3***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x20

           /***8-4***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x20

           /***8-5***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x20

           /***8-6***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x20

           /***8-7***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x20

           /***8-8***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x20

    addi.d    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_N5_M1_TL1

.L_N5_M1_L7:
    /* if (!(L & 7)) goto L_N5_M1_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_N5_M1_L0

.L_N5_M1_L71:
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    /* Add stride for A0, B0 */
    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x20

    addi.d     TL,  TL, -1
    blt        ZERO,TL, .L_N5_M1_L71

.L_N5_M1_L0:
#if defined(TRMMKERNEL)
    xvfmul.d  D0,   D0,  VALPHA
    xvfmul.d  D4,   D4,  VALPHA
    xvfmul.d  D8,   D8,  VALPHA
    xvfmul.d  D12,  D12, VALPHA
 #else
    /* Load C0  */
    xvld      U0,  C0,  0x00
    xvfmadd.d D0,  D0,  VALPHA,  U0 /* D0 = U0 + (D0 * VALPHA) */

    /* Load C1  */
    xvld      U0,  C1,  0x00
    xvfmadd.d D4,  D4,  VALPHA,  U0

    /* Load C2  */
    xvld      U0,  C2,  0x00
    xvfmadd.d D8,  D8,  VALPHA,  U0

    /* Load C3  */
    xvld      U0,  C3,  0x00
    xvfmadd.d D12, D12, VALPHA,  U0
#endif

    xvstelm.d D0,  C0,  0x00,    0x00
    xvstelm.d D4,  C1,  0x00,    0x00
    xvstelm.d D8,  C2,  0x00,    0x00
    xvstelm.d D12, C3,  0x00,    0x00

    /* Add stride for C */
    addi.d    C0,  C0,  0x08
    addi.d    C1,  C1,  0x08
    addi.d    C2,  C2,  0x08
    addi.d    C3,  C3,  0x08

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d     L,   K,   OFF
#ifdef LEFT
    /* number of values in A */
    addi.d    L,   L,   -1
#else
    /* number of values in B */
    addi.d    L,   L,   -4
#endif
    slli.d    T0,  L,  0x03
    add.d     A0,  A0, T0
    slli.d    T0,  L,  0x05
    add.d     B0,  B0, T0
#endif

#ifdef LEFT
    /* number of values in A */
    addi.d    OFF,   OFF,  0x01
#endif
#endif   // #if defined(TRMMKERNEL)

/********LOOP (if(N & 4 ) && (M & 1) ) End************/

.L_N5_M0:
    /* Add stride for B and C
     * B += (K * 32)
     * C += (LDC * 32)
     */
    /* since the array type is double,
     * so we must mul 32
     */
    addi.d    T2,   ZERO,32
    mul.d     T0,   K,   T2
    mul.d     T1,   LDC, T2
    add.d     B,    B,   T0
    add.d     C,    C,   T1

#if defined(TRMMKERNEL) && !defined(LEFT)
    addi.d    OFF,  OFF, 0x04
#endif

    /* We must reinit I */
    srai.d   I,     M,   4     /* I = bm >> 4 */

/************************* Condition 2 if((N & 4) && (M >> 4)) End !!! *************************
*                                                   dgemm_core_16x4                                */

.L_N3:
    andi     J,    N,   2
    beq      ZERO, J,   .L_N1

/************************* Condition 3 if((N & 2) && (M >> 4)) START !!! *************************
*                                                   dgemm_core_16x2                                */

    move     C0,    C
    move     A0,    A
    slli.d   T0,    LDC,   3
    add.d    C1,    C0,    T0

#if defined(TRMMKERNEL) && defined(LEFT)
    move     OFF,   OFFSET
#endif

    /* if (!(M >> 4)) goto L_N3_M8 */
    srai.d   I,     M,     4     /* I = bm >> 4 */
    beq      ZERO,  I,     .L_N3_M8

.L_N3_I1:
#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    move     B0,    B
#else
    slli.d   T0,    OFF,  0x07
    add.d    A0,    A0,   T0
    slli.d   T0,    OFF,  0x04
    add.d    B0,    B,    T0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    sub.d    L,     K,    OFF
#elif defined(LEFT)
    /* number of values in A */
    addi.d   L,     OFF,  16
#else
    /* number of values in B */
    addi.d   L,     OFF,  2
#endif
#else   // #if !defined(TRMMKERNEL)
    move     B0,    B
    move     L,     K /* L = bk */
#endif

    /* Load 16 * 64 from A0
     * U0 = {a3,  a2,  a1,  a0}
     * U1 = {a7,  a6,  a5,  a4}
     * U2 = {a11, a10, a9,  a8}
     * U3 = {a15, a14, a13, a12}
     */
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20
    xvld     U2,   A0,    0x40
    xvld     U3,   A0,    0x60

    xvldrepl.d     U4, B0, 0x00
    /* line 1 */
    xvfmul.d  D0,  U0, U4
    xvfmul.d  D1,  U1, U4
    xvfmul.d  D2,  U2, U4
    xvfmul.d  D3,  U3, U4

    xvldrepl.d     U4, B0, 0x08
    /* line 2 */
    xvfmul.d  D4,  U0, U4
    xvfmul.d  D5,  U1, U4
    xvfmul.d  D6,  U2, U4
    xvfmul.d  D7,  U3, U4

    /* Add stride for A0 and B0 */
    addi.d    A0,  A0, 0x80
    addi.d    B0,  B0, 0x10
    /* Reduce L */
    addi.d    L,   L,  -1
    srai.d    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_N3_L7 */
    beq       ZERO,TL, .L_N3_L7

.L_N3_TL1: /* TL-- */
           /***8-1***/
    /* Load 16 * 64 from A0 */
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20
    xvld     U2,   A0,    0x40
    xvld     U3,   A0,    0x60

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1
    xvfmadd.d  D2,  U2, U4, D2
    xvfmadd.d  D3,  U3, U4, D3

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4
    xvfmadd.d  D5,  U1, U4, D5
    xvfmadd.d  D6,  U2, U4, D6
    xvfmadd.d  D7,  U3, U4, D7

    addi.d     A0,  A0, 0x80
    addi.d     B0,  B0, 0x10

           /***8-2***/
    /* Load 16 * 64 from A0 */
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20
    xvld     U2,   A0,    0x40
    xvld     U3,   A0,    0x60

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1
    xvfmadd.d  D2,  U2, U4, D2
    xvfmadd.d  D3,  U3, U4, D3

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4
    xvfmadd.d  D5,  U1, U4, D5
    xvfmadd.d  D6,  U2, U4, D6
    xvfmadd.d  D7,  U3, U4, D7

    addi.d     A0,  A0, 0x80
    addi.d     B0,  B0, 0x10

           /***8-3***/
    /* Load 16 * 64 from A0 */
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20
    xvld     U2,   A0,    0x40
    xvld     U3,   A0,    0x60

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1
    xvfmadd.d  D2,  U2, U4, D2
    xvfmadd.d  D3,  U3, U4, D3

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4
    xvfmadd.d  D5,  U1, U4, D5
    xvfmadd.d  D6,  U2, U4, D6
    xvfmadd.d  D7,  U3, U4, D7

    addi.d     A0,  A0, 0x80
    addi.d     B0,  B0, 0x10

           /***8-4***/
    /* Load 16 * 64 from A0 */
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20
    xvld     U2,   A0,    0x40
    xvld     U3,   A0,    0x60

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1
    xvfmadd.d  D2,  U2, U4, D2
    xvfmadd.d  D3,  U3, U4, D3

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4
    xvfmadd.d  D5,  U1, U4, D5
    xvfmadd.d  D6,  U2, U4, D6
    xvfmadd.d  D7,  U3, U4, D7

    addi.d     A0,  A0, 0x80
    addi.d     B0,  B0, 0x10

           /***8-5***/
    /* Load 16 * 64 from A0 */
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20
    xvld     U2,   A0,    0x40
    xvld     U3,   A0,    0x60

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1
    xvfmadd.d  D2,  U2, U4, D2
    xvfmadd.d  D3,  U3, U4, D3

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4
    xvfmadd.d  D5,  U1, U4, D5
    xvfmadd.d  D6,  U2, U4, D6
    xvfmadd.d  D7,  U3, U4, D7

    addi.d     A0,  A0, 0x80
    addi.d     B0,  B0, 0x10

           /***8-6***/
    /* Load 16 * 64 from A0 */
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20
    xvld     U2,   A0,    0x40
    xvld     U3,   A0,    0x60

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1
    xvfmadd.d  D2,  U2, U4, D2
    xvfmadd.d  D3,  U3, U4, D3

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4
    xvfmadd.d  D5,  U1, U4, D5
    xvfmadd.d  D6,  U2, U4, D6
    xvfmadd.d  D7,  U3, U4, D7

    addi.d     A0,  A0, 0x80
    addi.d     B0,  B0, 0x10

           /***8-7***/
    /* Load 16 * 64 from A0 */
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20
    xvld     U2,   A0,    0x40
    xvld     U3,   A0,    0x60

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1
    xvfmadd.d  D2,  U2, U4, D2
    xvfmadd.d  D3,  U3, U4, D3

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4
    xvfmadd.d  D5,  U1, U4, D5
    xvfmadd.d  D6,  U2, U4, D6
    xvfmadd.d  D7,  U3, U4, D7

    addi.d     A0,  A0, 0x80
    addi.d     B0,  B0, 0x10

           /***8-8***/
    /* Load 16 * 64 from A0 */
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20
    xvld     U2,   A0,    0x40
    xvld     U3,   A0,    0x60

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1
    xvfmadd.d  D2,  U2, U4, D2
    xvfmadd.d  D3,  U3, U4, D3

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4
    xvfmadd.d  D5,  U1, U4, D5
    xvfmadd.d  D6,  U2, U4, D6
    xvfmadd.d  D7,  U3, U4, D7

    addi.d     A0,  A0, 0x80
    addi.d     B0,  B0, 0x10

    addi.d    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_N3_TL1

.L_N3_L7:
    /* if (!(L & 7)) goto L_N3_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_N3_L0

.L_N3_L71:
    /* Load 16 * 64 from A0 */
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20
    xvld     U2,   A0,    0x40
    xvld     U3,   A0,    0x60

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1
    xvfmadd.d  D2,  U2, U4, D2
    xvfmadd.d  D3,  U3, U4, D3

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4
    xvfmadd.d  D5,  U1, U4, D5
    xvfmadd.d  D6,  U2, U4, D6
    xvfmadd.d  D7,  U3, U4, D7

    /* Add stride for A0, B0 */
    addi.d     A0,  A0, 0x80
    addi.d     B0,  B0, 0x10

    addi.d     TL,  TL, -1
    blt        ZERO,TL, .L_N3_L71

.L_N3_L0:
#if defined(TRMMKERNEL)
    xvfmul.d  D0,   D0,  VALPHA
    xvfmul.d  D1,   D1,  VALPHA
    xvfmul.d  D2,   D2,  VALPHA
    xvfmul.d  D3,   D3,  VALPHA
    xvfmul.d  D4,   D4,  VALPHA
    xvfmul.d  D5,   D5,  VALPHA
    xvfmul.d  D6,   D6,  VALPHA
    xvfmul.d  D7,   D7,  VALPHA
#else
    /* Load C0  */
    xvld      U0,  C0,  0x00
    xvld      U1,  C0,  0x20
    xvld      U2,  C0,  0x40
    xvld      U3,  C0,  0x60
    xvfmadd.d D0,  D0,  VALPHA,  U0 /* D0 = U0 + (D0 * VALPHA) */
    xvfmadd.d D1,  D1,  VALPHA,  U1
    xvfmadd.d D2,  D2,  VALPHA,  U2
    xvfmadd.d D3,  D3,  VALPHA,  U3

    /* Load C1  */
    xvld      U0,  C1,  0x00
    xvld      U1,  C1,  0x20
    xvld      U2,  C1,  0x40
    xvld      U3,  C1,  0x60
    xvfmadd.d D4,  D4,  VALPHA,  U0
    xvfmadd.d D5,  D5,  VALPHA,  U1
    xvfmadd.d D6,  D6,  VALPHA,  U2
    xvfmadd.d D7,  D7,  VALPHA,  U3
#endif

    /* Store C0 */
    xvst      D0,  C0,  0x00
    xvst      D1,  C0,  0x20
    xvst      D2,  C0,  0x40
    xvst      D3,  C0,  0x60
    /* Store C1 */
    xvst      D4,  C1,  0x00
    xvst      D5,  C1,  0x20
    xvst      D6,  C1,  0x40
    xvst      D7,  C1,  0x60

    /* Add stride for C */
    addi.d    C0,  C0,  0x80
    addi.d    C1,  C1,  0x80

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d     L,   K,   OFF
#ifdef LEFT
    addi.d    L,   L,   -16
#else
    addi.d    L,   L,   -2
#endif
    slli.d    T0,  L,  0x07
    add.d     A0,  A0, T0
    slli.d    T0,  L,  0x04
    add.d     B0,  B0, T0
#endif

#ifdef LEFT
    addi.d    OFF,   OFF,  0x10
#endif
#endif   // #if defined(TRMMKERNEL)

    addi.d    I,   I,   -1  /* I-- */
    blt       ZERO,I,   .L_N3_I1

.L_N3_M8:
    /* We have done M & 16, considering M=8/4/2/1 */
    andi      I,   M,   15
    beq       ZERO,I,   .L_N3_M0

    andi      I,   M,   8
    beq       ZERO,I,   .L_N3_M4

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    move     B0,    B
#else
    slli.d   T0,    OFF,  0x06
    add.d    A0,    A0,   T0
    slli.d   T0,    OFF,  0x04
    add.d    B0,    B,    T0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    sub.d    L,     K,    OFF
#elif defined(LEFT)
    /* number of values in A */
    addi.d   L,     OFF,  8
#else
    /* number of values in B */
    addi.d   L,     OFF,  2
#endif
#else   // #if !defined(TRMMKERNEL)
    move     B0,    B
    move     L,     K /* L = bk */
#endif

    /* Load 8 * 64 from A0 */
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20

    xvldrepl.d     U4, B0, 0x00
    /* line 1 */
    xvfmul.d  D0,  U0, U4
    xvfmul.d  D1,  U1, U4

    xvldrepl.d     U4, B0, 0x08
    /* line 2 */
    xvfmul.d  D4,  U0, U4
    xvfmul.d  D5,  U1, U4

    /* Add stride for A0 and B0 */
    addi.d    A0,  A0, 0x40
    addi.d    B0,  B0, 0x10
    /* Reduce L */
    addi.d    L,   L,  -1
    srai.d    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_N3_M8_L7 */
    beq       ZERO,TL, .L_N3_M8_L7

.L_N3_M8_TL1: /* TL-- */
           /***8-1***/
    /* Load 16 * 64 from A0 */
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4
    xvfmadd.d  D5,  U1, U4, D5

    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x10

           /***8-2***/
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4
    xvfmadd.d  D5,  U1, U4, D5

    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x10

           /***8-3***/
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4
    xvfmadd.d  D5,  U1, U4, D5

    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x10

           /***8-4***/
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4
    xvfmadd.d  D5,  U1, U4, D5

    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x10

           /***8-5***/
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20

    /* Cumulative D0~D23 */
    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4
    xvfmadd.d  D5,  U1, U4, D5

    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x10

           /***8-6***/
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4
    xvfmadd.d  D5,  U1, U4, D5

    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x10

           /***8-7***/
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4
    xvfmadd.d  D5,  U1, U4, D5

    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x10

           /***8-8***/
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4
    xvfmadd.d  D5,  U1, U4, D5

    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x10

    addi.d    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_N3_M8_TL1

.L_N3_M8_L7:
    /* if (!(L & 7)) goto L_N3_M8_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_N3_M8_L0

.L_N3_M8_L71:
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4
    xvfmadd.d  D5,  U1, U4, D5

    /* Add stride for A0, B0 */
    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x10

    addi.d     TL,  TL, -1
    blt        ZERO,TL, .L_N3_M8_L71

.L_N3_M8_L0:
#if defined(TRMMKERNEL)
    xvfmul.d  D0,   D0,  VALPHA
    xvfmul.d  D1,   D1,  VALPHA
    xvfmul.d  D4,   D4,  VALPHA
    xvfmul.d  D5,   D5,  VALPHA
#else
    /* Load C0  */
    xvld      U0,  C0,  0x00
    xvld      U1,  C0,  0x20
    xvfmadd.d D0,  D0,  VALPHA,  U0 /* D0 = U0 + (D0 * VALPHA) */
    xvfmadd.d D1,  D1,  VALPHA,  U1

    /* Load C1  */
    xvld      U0,  C1,  0x00
    xvld      U1,  C1,  0x20
    xvfmadd.d D4,  D4,  VALPHA,  U0
    xvfmadd.d D5,  D5,  VALPHA,  U1
#endif

    /* Store C0 */
    xvst      D0,  C0,  0x00
    xvst      D1,  C0,  0x20
    /* Store C1 */
    xvst      D4,  C1,  0x00
    xvst      D5,  C1,  0x20

    /* Add stride for C */
    addi.d    C0,  C0,  0x40
    addi.d    C1,  C1,  0x40

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d     L,   K,   OFF
#ifdef LEFT
    addi.d    L,   L,   -8
#else
    addi.d    L,   L,   -2
#endif
    slli.d    T0,  L,  0x06
    add.d     A0,  A0, T0
    slli.d    T0,  L,  0x04
    add.d     B0,  B0, T0
#endif

#ifdef LEFT
    addi.d    OFF,   OFF,  0x08
#endif
#endif   // #if defined(TRMMKERNEL)

/********LOOP (if(N & 2) && (M & 8) ) End************/

.L_N3_M4:
    andi      I,   M,   4
    beq       ZERO,I,   .L_N3_M2

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    move     B0,    B
#else
    slli.d   T0,    OFF,  0x05
    add.d    A0,    A0,   T0
    slli.d   T0,    OFF,  0x04
    add.d    B0,    B,    T0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    sub.d    L,     K,    OFF
#elif defined(LEFT)
    /* number of values in A */
    addi.d   L,     OFF,  4
#else
    /* number of values in B */
    addi.d   L,     OFF,  2
#endif
#else   // #if !defined(TRMMKERNEL)
    move     B0,    B
    move     L,     K /* L = bk */
#endif

    /* Load 4 * 64 from A0 */
    xvld     U0,   A0,    0x00

    xvldrepl.d     U4, B0, 0x00
    /* line 1 */
    xvfmul.d  D0,  U0, U4

    xvldrepl.d     U4, B0, 0x08
    /* line 2 */
    xvfmul.d  D4,  U0, U4

    /* Add stride for A0 and B0 */
    addi.d    A0,  A0, 0x20
    addi.d    B0,  B0, 0x10
    /* Reduce L */
    addi.d    L,   L,  -1
    srai.d    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_N3_M4_L7 */
    beq       ZERO,TL, .L_N3_M4_L7

.L_N3_M4_TL1: /* TL-- */
           /***8-1***/
    /* Load 8 * 64 from A0 */
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x10

           /***8-2***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x10

           /***8-3***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x10

           /***8-4***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x10

           /***8-5***/
    xvld     U0,   A0,    0x00

    /* Cumulative D0~D23 */
    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x10

           /***8-6***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x10

           /***8-7***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x10

           /***8-8***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x10

    addi.d    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_N3_M4_TL1

.L_N3_M4_L7:
    /* if (!(L & 7)) goto L_N3_M4_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_N3_M4_L0

.L_N3_M4_L71:
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    /* Add stride for A0, B0 */
    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x10

    addi.d     TL,  TL, -1
    blt        ZERO,TL, .L_N3_M4_L71

.L_N3_M4_L0:
#if defined(TRMMKERNEL)
    xvfmul.d  D0,   D0,  VALPHA
    xvfmul.d  D4,   D4,  VALPHA
#else
    /* Load C0  */
    xvld      U0,  C0,  0x00
    xvfmadd.d D0,  D0,  VALPHA,  U0 /* D0 = U0 + (D0 * VALPHA) */

    /* Load C1  */
    xvld      U0,  C1,  0x00
    xvfmadd.d D4,  D4,  VALPHA,  U0
#endif

    /* Store C0 */
    xvst      D0,  C0,  0x00
    /* Store C1 */
    xvst      D4,  C1,  0x00

    /* Add stride for C */
    addi.d    C0,  C0,  0x20
    addi.d    C1,  C1,  0x20

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d     L,   K,   OFF
#ifdef LEFT
    addi.d    L,   L,   -4
#else
    addi.d    L,   L,   -2
#endif
    slli.d    T0,  L,  0x05
    add.d     A0,  A0, T0
    slli.d    T0,  L,  0x04
    add.d     B0,  B0, T0
#endif

#ifdef LEFT
    addi.d    OFF,   OFF,  0x04
#endif
#endif   // #if defined(TRMMKERNEL)

/********LOOP (if(N & 2 ) && (M & 4) ) End************/

.L_N3_M2:
    andi      I,   M,   2
    beq       ZERO,I,   .L_N3_M1

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    move     B0,    B
#else
    slli.d   T0,    OFF,  0x04
    add.d    A0,    A0,   T0
    add.d    B0,    B,    T0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    sub.d    L,     K,    OFF
#elif defined(LEFT)
    /* number of values in A */
    addi.d   L,     OFF,  2
#else
    /* number of values in B */
    addi.d   L,     OFF,  2
#endif
#else   // #if !defined(TRMMKERNEL)
    move     B0,    B
    move     L,     K /* L = bk */
#endif

    /* Load 2 * 64 from A0 */
    xvld     U0,   A0,    0x00

    xvldrepl.d     U4, B0, 0x00
    /* line 1 */
    xvfmul.d  D0,  U0, U4

    xvldrepl.d     U4, B0, 0x08
    /* line 2 */
    xvfmul.d  D4,  U0, U4

    /* Add stride for A0 and B0 */
    addi.d    A0,  A0, 0x10
    addi.d    B0,  B0, 0x10
    /* Reduce L */
    addi.d    L,   L,  -1
    srai.d    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_N3_M2_L7 */
    beq       ZERO,TL, .L_N3_M2_L7

.L_N3_M2_TL1: /* TL-- */
           /***8-1***/
    /* Load 2 * 64 from A0 */
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    addi.d     A0,  A0, 0x10
    addi.d     B0,  B0, 0x10

           /***8-2***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    addi.d     A0,  A0, 0x10
    addi.d     B0,  B0, 0x10

           /***8-3***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    addi.d     A0,  A0, 0x10
    addi.d     B0,  B0, 0x10

           /***8-4***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    addi.d     A0,  A0, 0x10
    addi.d     B0,  B0, 0x10

           /***8-5***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    addi.d     A0,  A0, 0x10
    addi.d     B0,  B0, 0x10

           /***8-6***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    addi.d     A0,  A0, 0x10
    addi.d     B0,  B0, 0x10

           /***8-7***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    addi.d     A0,  A0, 0x10
    addi.d     B0,  B0, 0x10

           /***8-8***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    addi.d     A0,  A0, 0x10
    addi.d     B0,  B0, 0x10

    addi.d    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_N3_M2_TL1

.L_N3_M2_L7:
    /* if (!(L & 7)) goto L_N3_M2_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_N3_M2_L0

.L_N3_M2_L71:
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    /* Add stride for A0, B0 */
    addi.d     A0,  A0, 0x10
    addi.d     B0,  B0, 0x10

    addi.d     TL,  TL, -1
    blt        ZERO,TL, .L_N3_M2_L71

.L_N3_M2_L0:
#if defined(TRMMKERNEL)
    xvfmul.d  D0,   D0,  VALPHA
    xvfmul.d  D4,   D4,  VALPHA
#else
    /* Load C0  */
    xvld      U0,  C0,  0x00
    xvfmadd.d D0,  D0,  VALPHA,  U0 /* D0 = U0 + (D0 * VALPHA) */

    /* Load C1  */
    xvld      U0,  C1,  0x00
    xvfmadd.d D4,  D4,  VALPHA,  U0
#endif

    xvstelm.d D0,  C0,  0x00,    0x00
    xvstelm.d D4,  C1,  0x00,    0x00
    xvstelm.d D0,  C0,  0x08,    0x01
    xvstelm.d D4,  C1,  0x08,    0x01

    /* Add stride for C */
    addi.d    C0,  C0,  0x10
    addi.d    C1,  C1,  0x10

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d     L,   K,   OFF
#ifdef LEFT
    addi.d    L,   L,   -2
#else
    addi.d    L,   L,   -2
#endif
    slli.d    T0,  L,  0x04
    add.d     A0,  A0, T0
    add.d     B0,  B0, T0
#endif

#ifdef LEFT
    addi.d    OFF,   OFF,  0x02
#endif
#endif   // #if defined(TRMMKERNEL)

/********LOOP (if(N & 2 ) && (M & 2) ) End************/

.L_N3_M1:
    andi      I,   M,   1
    beq       ZERO,I,   .L_N3_M0

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    move     B0,    B
#else
    slli.d   T0,    OFF,  0x03
    add.d    A0,    A0,   T0
    slli.d   T0,    OFF,  0x04
    add.d    B0,    B,    T0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    sub.d    L,     K,    OFF
#elif defined(LEFT)
    /* number of values in A */
    addi.d   L,     OFF,  1
#else
    /* number of values in B */
    addi.d   L,     OFF,  2
#endif
#else   // #if !defined(TRMMKERNEL)
    move     B0,    B
    move     L,     K /* L = bk */
#endif

    /* Load 1 * 64 from A0 */
    xvld     U0,   A0,    0x00

    xvldrepl.d     U4, B0, 0x00
    /* line 1 */
    xvfmul.d  D0,  U0, U4

    xvldrepl.d     U4, B0, 0x08
    /* line 2 */
    xvfmul.d  D4,  U0, U4

    /* Add stride for A0 and B0 */
    addi.d    A0,  A0, 0x08
    addi.d    B0,  B0, 0x10
    /* Reduce L */
    addi.d    L,   L,  -1
    srai.d    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_N3_M1_L7 */
    beq       ZERO,TL, .L_N3_M1_L7

.L_N3_M1_TL1: /* TL-- */
           /***8-1***/
    /* Load 1 * 64 from A0 */
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x10

           /***8-2***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x10

           /***8-3***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x10

           /***8-4***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x10

           /***8-5***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x10

           /***8-6***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x10

           /***8-7***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x10

           /***8-8***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x10

    addi.d    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_N3_M1_TL1

.L_N3_M1_L7:
    /* if (!(L & 7)) goto L_N3_M1_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_N3_M1_L0

.L_N3_M1_L71:
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    /* Add stride for A0, B0 */
    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x10

    addi.d     TL,  TL, -1
    blt        ZERO,TL, .L_N3_M1_L71

.L_N3_M1_L0:
#if defined(TRMMKERNEL)
    xvfmul.d  D0,   D0,  VALPHA
    xvfmul.d  D4,   D4,  VALPHA
#else
    /* Load C0  */
    xvld      U0,  C0,  0x00
    xvfmadd.d D0,  D0,  VALPHA,  U0 /* D0 = U0 + (D0 * VALPHA) */

    /* Load C1  */
    xvld      U0,  C1,  0x00
    xvfmadd.d D4,  D4,  VALPHA,  U0
#endif

    xvstelm.d D0,  C0,  0x00,    0x00
    xvstelm.d D4,  C1,  0x00,    0x00

    /* Add stride for C */
    addi.d    C0,  C0,  0x08
    addi.d    C1,  C1,  0x08

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d     L,   K,   OFF
#ifdef LEFT
    addi.d    L,   L,   -1
#else
    addi.d    L,   L,   -2
#endif
    slli.d    T0,  L,  0x03
    add.d     A0,  A0, T0
    slli.d    T0,  L,  0x04
    add.d     B0,  B0, T0
#endif

#ifdef LEFT
    addi.d    OFF,   OFF,  0x01
#endif
#endif   // #if defined(TRMMKERNEL)

/********LOOP (if(N & 2 ) && (M & 1) ) End************/

.L_N3_M0:
    /* Add stride for B and C
     * B += (K * 16)
     * C += (LDC * 16)
     */
    /* since the array type is double,
     * so we must mul 32
     */
    addi.d    T2,   ZERO,16
    mul.d     T0,   K,   T2
    mul.d     T1,   LDC, T2
    add.d     B,    B,   T0
    add.d     C,    C,   T1

#if defined(TRMMKERNEL) && !defined(LEFT)
    addi.d    OFF,  OFF, 0x02
#endif

    /* We must reinit I */
    srai.d   I,     M,   4     /* I = bm >> 4 */

/************************* Condition 3 if((N & 2) && (M >> 4)) End !!! *************************
*                                                   dgemm_core_16x2                                */

.L_N1:
    andi     J,    N,   1
    beq      ZERO, J,   .L_N0

/************************* Condition 4 if((N & 1) && (M >> 4)) START !!! *************************
*                                                   dgemm_core_16x1                                */

    move     C0,    C
    move     A0,    A

#if defined(TRMMKERNEL) && defined(LEFT)
    move     OFF,   OFFSET
#endif

    /* if (!(M >> 4)) goto L_N1_M8 */
    srai.d   I,     M,     4     /* I = bm >> 4 */
    beq      ZERO,  I,     .L_N1_M8

.L_N1_I1:
#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    move     B0,    B
#else
    slli.d   T0,    OFF,  0x07
    add.d    A0,    A0,   T0
    slli.d   T0,    OFF,  0x03
    add.d    B0,    B,    T0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    sub.d    L,     K,    OFF
#elif defined(LEFT)
    /* number of values in A */
    addi.d   L,     OFF,  16
#else
    /* number of values in B */
    addi.d   L,     OFF,  1
#endif
#else   // #if !defined(TRMMKERNEL)
    move     B0,    B
    move     L,     K /* L = bk */
#endif

    /* Load 16 * 64 from A0
     * U0 = {a3,  a2,  a1,  a0}
     * U1 = {a7,  a6,  a5,  a4}
     * U2 = {a11, a10, a9,  a8}
     * U3 = {a15, a14, a13, a12}
     */
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20
    xvld     U2,   A0,    0x40
    xvld     U3,   A0,    0x60

    xvldrepl.d     U4, B0, 0x00
    /* line 1 */
    xvfmul.d  D0,  U0, U4
    xvfmul.d  D1,  U1, U4
    xvfmul.d  D2,  U2, U4
    xvfmul.d  D3,  U3, U4

    /* Add stride for A0 and B0 */
    addi.d    A0,  A0, 0x80
    addi.d    B0,  B0, 0x08
    /* Reduce L */
    addi.d    L,   L,  -1
    srai.d    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_N1_L7 */
    beq       ZERO,TL, .L_N1_L7

.L_N1_TL1: /* TL-- */
           /***8-1***/
    /* Load 16 * 64 from A0 */
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20
    xvld     U2,   A0,    0x40
    xvld     U3,   A0,    0x60

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1
    xvfmadd.d  D2,  U2, U4, D2
    xvfmadd.d  D3,  U3, U4, D3

    addi.d     A0,  A0, 0x80
    addi.d     B0,  B0, 0x08

           /***8-2***/
    /* Load 16 * 64 from A0 */
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20
    xvld     U2,   A0,    0x40
    xvld     U3,   A0,    0x60

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1
    xvfmadd.d  D2,  U2, U4, D2
    xvfmadd.d  D3,  U3, U4, D3

    addi.d     A0,  A0, 0x80
    addi.d     B0,  B0, 0x08

           /***8-3***/
    /* Load 16 * 64 from A0 */
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20
    xvld     U2,   A0,    0x40
    xvld     U3,   A0,    0x60

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1
    xvfmadd.d  D2,  U2, U4, D2
    xvfmadd.d  D3,  U3, U4, D3

    addi.d     A0,  A0, 0x80
    addi.d     B0,  B0, 0x08

           /***8-4***/
    /* Load 16 * 64 from A0 */
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20
    xvld     U2,   A0,    0x40
    xvld     U3,   A0,    0x60

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1
    xvfmadd.d  D2,  U2, U4, D2
    xvfmadd.d  D3,  U3, U4, D3

    addi.d     A0,  A0, 0x80
    addi.d     B0,  B0, 0x08

           /***8-5***/
    /* Load 16 * 64 from A0 */
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20
    xvld     U2,   A0,    0x40
    xvld     U3,   A0,    0x60

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1
    xvfmadd.d  D2,  U2, U4, D2
    xvfmadd.d  D3,  U3, U4, D3

    addi.d     A0,  A0, 0x80
    addi.d     B0,  B0, 0x08

           /***8-6***/
    /* Load 16 * 64 from A0 */
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20
    xvld     U2,   A0,    0x40
    xvld     U3,   A0,    0x60

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1
    xvfmadd.d  D2,  U2, U4, D2
    xvfmadd.d  D3,  U3, U4, D3

    addi.d     A0,  A0, 0x80
    addi.d     B0,  B0, 0x08

           /***8-7***/
    /* Load 16 * 64 from A0 */
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20
    xvld     U2,   A0,    0x40
    xvld     U3,   A0,    0x60

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1
    xvfmadd.d  D2,  U2, U4, D2
    xvfmadd.d  D3,  U3, U4, D3

    addi.d     A0,  A0, 0x80
    addi.d     B0,  B0, 0x08

           /***8-8***/
    /* Load 16 * 64 from A0 */
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20
    xvld     U2,   A0,    0x40
    xvld     U3,   A0,    0x60

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1
    xvfmadd.d  D2,  U2, U4, D2
    xvfmadd.d  D3,  U3, U4, D3

    addi.d     A0,  A0, 0x80
    addi.d     B0,  B0, 0x08

    addi.d    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_N1_TL1

.L_N1_L7:
    /* if (!(L & 7)) goto L_N1_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_N1_L0

.L_N1_L71:
    /* Load 16 * 64 from A0 */
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20
    xvld     U2,   A0,    0x40
    xvld     U3,   A0,    0x60

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1
    xvfmadd.d  D2,  U2, U4, D2
    xvfmadd.d  D3,  U3, U4, D3

    /* Add stride for A0, B0 */
    addi.d     A0,  A0, 0x80
    addi.d     B0,  B0, 0x08

    addi.d     TL,  TL, -1
    blt        ZERO,TL, .L_N1_L71

.L_N1_L0:
#if defined(TRMMKERNEL)
    xvfmul.d  D0,   D0,  VALPHA
    xvfmul.d  D1,   D1,  VALPHA
    xvfmul.d  D2,   D2,  VALPHA
    xvfmul.d  D3,   D3,  VALPHA
#else
    /* Load C0  */
    xvld      U0,  C0,  0x00
    xvld      U1,  C0,  0x20
    xvld      U2,  C0,  0x40
    xvld      U3,  C0,  0x60
    xvfmadd.d D0,  D0,  VALPHA,  U0 /* D0 = U0 + (D0 * VALPHA) */
    xvfmadd.d D1,  D1,  VALPHA,  U1
    xvfmadd.d D2,  D2,  VALPHA,  U2
    xvfmadd.d D3,  D3,  VALPHA,  U3
#endif

    /* Store C0 */
    xvst      D0,  C0,  0x00
    xvst      D1,  C0,  0x20
    xvst      D2,  C0,  0x40
    xvst      D3,  C0,  0x60

    /* Add stride for C */
    addi.d    C0,  C0,  0x80

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d     L,   K,   OFF
#ifdef LEFT
    addi.d    L,   L,   -16
#else
    addi.d    L,   L,   -1
#endif
    slli.d    T0,  L,  0x07
    add.d     A0,  A0, T0
    slli.d    T0,  L,  0x03
    add.d     B0,  B0, T0
#endif

#ifdef LEFT
    addi.d    OFF,   OFF,  0x10
#endif
#endif   // #if defined(TRMMKERNEL)

    addi.d    I,   I,   -1  /* I-- */
    blt       ZERO,I,   .L_N1_I1

.L_N1_M8:
    /* We have done M & 16, considering M=8/4/2/1 */
    andi      I,   M,   15
    beq       ZERO,I,   .L_N1_M0

    andi      I,   M,   8
    beq       ZERO,I,   .L_N1_M4

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    move     B0,    B
#else
    slli.d   T0,    OFF,  0x06
    add.d    A0,    A0,   T0
    slli.d   T0,    OFF,  0x03
    add.d    B0,    B,    T0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    sub.d    L,     K,    OFF
#elif defined(LEFT)
    /* number of values in A */
    addi.d   L,     OFF,  8
#else
    /* number of values in B */
    addi.d   L,     OFF,  1
#endif
#else   // #if !defined(TRMMKERNEL)
    move     B0,    B
    move     L,     K /* L = bk */
#endif

    /* Load 8 * 64 from A0 */
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20

    xvldrepl.d     U4, B0, 0x00
    /* line 1 */
    xvfmul.d  D0,  U0, U4
    xvfmul.d  D1,  U1, U4

    /* Add stride for A0 and B0 */
    addi.d    A0,  A0, 0x40
    addi.d    B0,  B0, 0x08
    /* Reduce L */
    addi.d    L,   L,  -1
    srai.d    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_N1_M8_L7 */
    beq       ZERO,TL, .L_N1_M8_L7

.L_N1_M8_TL1: /* TL-- */
           /***8-1***/
    /* Load 16 * 64 from A0 */
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1

    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x08

           /***8-2***/
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1

    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x08

           /***8-3***/
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1

    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x08

           /***8-4***/
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1

    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x08

           /***8-5***/
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1

    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x08

           /***8-6***/
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1

    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x08

           /***8-7***/
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1

    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x08

           /***8-8***/
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1

    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x08

    addi.d    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_N1_M8_TL1

.L_N1_M8_L7:
    /* if (!(L & 7)) goto L_N1_M8_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_N1_M8_L0

.L_N1_M8_L71:
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1

    /* Add stride for A0, B0 */
    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x08

    addi.d     TL,  TL, -1
    blt        ZERO,TL, .L_N1_M8_L71

.L_N1_M8_L0:
#if defined(TRMMKERNEL)
    xvfmul.d  D0,   D0,  VALPHA
    xvfmul.d  D1,   D1,  VALPHA
#else
    /* Load C0  */
    xvld      U0,  C0,  0x00
    xvld      U1,  C0,  0x20
    xvfmadd.d D0,  D0,  VALPHA,  U0 /* D0 = U0 + (D0 * VALPHA) */
    xvfmadd.d D1,  D1,  VALPHA,  U1
#endif

    /* Store C0 */
    xvst      D0,  C0,  0x00
    xvst      D1,  C0,  0x20

    /* Add stride for C */
    addi.d    C0,  C0,  0x40

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d     L,   K,   OFF
#ifdef LEFT
    addi.d    L,   L,   -8
#else
    addi.d    L,   L,   -1
#endif
    slli.d    T0,  L,  0x06
    add.d     A0,  A0, T0
    slli.d    T0,  L,  0x03
    add.d     B0,  B0, T0
#endif

#ifdef LEFT
    addi.d    OFF,   OFF,  0x08
#endif
#endif   // #if defined(TRMMKERNEL)

/********LOOP (if(N & 2) && (M & 8) ) End************/

.L_N1_M4:
    andi      I,   M,   4
    beq       ZERO,I,   .L_N1_M2

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    move     B0,    B
#else
    slli.d   T0,    OFF,  0x05
    add.d    A0,    A0,   T0
    slli.d   T0,    OFF,  0x03
    add.d    B0,    B,    T0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    sub.d    L,     K,    OFF
#elif defined(LEFT)
    /* number of values in A */
    addi.d   L,     OFF,  4
#else
    /* number of values in B */
    addi.d   L,     OFF,  1
#endif
#else   // #if !defined(TRMMKERNEL)
    move     B0,    B
    move     L,     K /* L = bk */
#endif

    /* Load 4 * 64 from A0 */
    xvld     U0,   A0,    0x00

    xvldrepl.d     U4, B0, 0x00
    /* line 1 */
    xvfmul.d  D0,  U0, U4

    /* Add stride for A0 and B0 */
    addi.d    A0,  A0, 0x20
    addi.d    B0,  B0, 0x08
    /* Reduce L */
    addi.d    L,   L,  -1
    srai.d    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_N1_M4_L7 */
    beq       ZERO,TL, .L_N1_M4_L7

.L_N1_M4_TL1: /* TL-- */
           /***8-1***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x08

           /***8-2***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x08

           /***8-3***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x08

           /***8-4***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x08

           /***8-5***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x08

           /***8-6***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x08

           /***8-7***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x08

           /***8-8***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x08

    addi.d    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_N1_M4_TL1

.L_N1_M4_L7:
    /* if (!(L & 7)) goto L_N1_M4_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_N1_M4_L0

.L_N1_M4_L71:
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    /* Add stride for A0, B0 */
    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x08

    addi.d     TL,  TL, -1
    blt        ZERO,TL, .L_N1_M4_L71

.L_N1_M4_L0:
#if defined(TRMMKERNEL)
    xvfmul.d  D0,   D0,  VALPHA
#else
    /* Load C0  */
    xvld      U0,  C0,  0x00
    xvfmadd.d D0,  D0,  VALPHA,  U0 /* D0 = U0 + (D0 * VALPHA) */
 #endif

    /* Store C0 */
    xvst      D0,  C0,  0x00

    /* Add stride for C */
    addi.d    C0,  C0,  0x20

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d     L,   K,   OFF
#ifdef LEFT
    addi.d    L,   L,   -4
#else
    addi.d    L,   L,   -1
#endif
    slli.d    T0,  L,  0x05
    add.d     A0,  A0, T0
    slli.d    T0,  L,  0x03
    add.d     B0,  B0, T0
#endif

#ifdef LEFT
    addi.d    OFF,   OFF,  0x04
#endif
#endif   // #if defined(TRMMKERNEL)

/********LOOP (if(N & 2 ) && (M & 4) ) End************/

.L_N1_M2:
    andi      I,   M,   2
    beq       ZERO,I,   .L_N1_M1

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    move     B0,    B
#else
    slli.d   T0,    OFF,  0x04
    add.d    A0,    A0,   T0
    slli.d   T0,    OFF,  0x03
    add.d    B0,    B,    T0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    sub.d    L,     K,    OFF
#elif defined(LEFT)
    /* number of values in A */
    addi.d   L,     OFF,  2
#else
    /* number of values in B */
    addi.d   L,     OFF,  1
#endif
#else   // #if !defined(TRMMKERNEL)
    move     B0,    B
    move     L,     K /* L = bk */
#endif

    /* Load 2 * 64 from A0 */
    xvld     U0,   A0,    0x00

    xvldrepl.d     U4, B0, 0x00
    /* line 1 */
    xvfmul.d  D0,  U0, U4

    /* Add stride for A0 and B0 */
    addi.d    A0,  A0, 0x10
    addi.d    B0,  B0, 0x08
    /* Reduce L */
    addi.d    L,   L,  -1
    srai.d    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_N1_M2_L7 */
    beq       ZERO,TL, .L_N1_M2_L7

.L_N1_M2_TL1: /* TL-- */
           /***8-1***/
    /* Load 2 * 64 from A0 */
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    addi.d     A0,  A0, 0x10
    addi.d     B0,  B0, 0x08

           /***8-2***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    addi.d     A0,  A0, 0x10
    addi.d     B0,  B0, 0x08

           /***8-3***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    addi.d     A0,  A0, 0x10
    addi.d     B0,  B0, 0x08

           /***8-4***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    addi.d     A0,  A0, 0x10
    addi.d     B0,  B0, 0x08

           /***8-5***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    addi.d     A0,  A0, 0x10
    addi.d     B0,  B0, 0x08

           /***8-6***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    addi.d     A0,  A0, 0x10
    addi.d     B0,  B0, 0x08

           /***8-7***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    addi.d     A0,  A0, 0x10
    addi.d     B0,  B0, 0x08

           /***8-8***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    addi.d     A0,  A0, 0x10
    addi.d     B0,  B0, 0x08

    addi.d    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_N1_M2_TL1

.L_N1_M2_L7:
    /* if (!(L & 7)) goto L_N1_M2_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_N1_M2_L0

.L_N1_M2_L71:
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    /* Add stride for A0, B0 */
    addi.d     A0,  A0, 0x10
    addi.d     B0,  B0, 0x08

    addi.d     TL,  TL, -1
    blt        ZERO,TL, .L_N1_M2_L71

.L_N1_M2_L0:
#if defined(TRMMKERNEL)
    xvfmul.d  D0,   D0,  VALPHA
#else
    /* Load C0  */
    xvld      U0,  C0,  0x00
    xvfmadd.d D0,  D0,  VALPHA,  U0 /* D0 = U0 + (D0 * VALPHA) */
#endif

    xvstelm.d D0,  C0,  0x00,    0x00
    xvstelm.d D0,  C0,  0x08,    0x01

    /* Add stride for C */
    addi.d    C0,  C0,  0x10

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d     L,   K,   OFF
#ifdef LEFT
    addi.d    L,   L,   -2
#else
    addi.d    L,   L,   -1
#endif
    slli.d    T0,  L,  0x04
    add.d     A0,  A0, T0
    slli.d    T0,  L,  0x03
    add.d     B0,  B0, T0
#endif

#ifdef LEFT
    addi.d    OFF,   OFF,  0x02
#endif
#endif   // #if defined(TRMMKERNEL)

/********LOOP (if(N & 2 ) && (M & 2) ) End************/

.L_N1_M1:
    andi      I,   M,   1
    beq       ZERO,I,   .L_N1_M0

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    move     B0,    B
#else
    slli.d   T0,    OFF,  0x03
    add.d    A0,    A0,   T0
    add.d    B0,    B,    T0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    sub.d    L,     K,    OFF
#elif defined(LEFT)
    /* number of values in A */
    addi.d   L,     OFF,  1
#else
    /* number of values in B */
    addi.d   L,     OFF,  1
#endif
#else   // #if !defined(TRMMKERNEL)
    move     B0,    B
    move     L,     K /* L = bk */
#endif

    /* Load 1 * 64 from A0 */
    xvld     U0,   A0,    0x00

    xvldrepl.d     U4, B0, 0x00
    /* line 1 */
    xvfmul.d  D0,  U0, U4

    /* Add stride for A0 and B0 */
    addi.d    A0,  A0, 0x08
    addi.d    B0,  B0, 0x08
    /* Reduce L */
    addi.d    L,   L,  -1
    srai.d    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_N1_M1_L7 */
    beq       ZERO,TL, .L_N1_M1_L7

.L_N1_M1_TL1: /* TL-- */
           /***8-1***/
    /* Load 1 * 64 from A0 */
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x08

           /***8-2***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x08

           /***8-3***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x08

           /***8-4***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x08

           /***8-5***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x08

           /***8-6***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x08

           /***8-7***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x08

           /***8-8***/
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x08

    addi.d    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_N1_M1_TL1

.L_N1_M1_L7:
    /* if (!(L & 7)) goto L_N1_M1_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_N1_M1_L0

.L_N1_M1_L71:
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    /* Add stride for A0, B0 */
    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x08

    addi.d     TL,  TL, -1
    blt        ZERO,TL, .L_N1_M1_L71

.L_N1_M1_L0:
#if defined(TRMMKERNEL)
    xvfmul.d  D0,   D0,  VALPHA
#else
    /* Load C0  */
    xvld      U0,  C0,  0x00
    xvfmadd.d D0,  D0,  VALPHA,  U0 /* D0 = U0 + (D0 * VALPHA) */
#endif

    xvstelm.d D0,  C0,  0x00,    0x00

    /* Add stride for C */
    addi.d    C0,  C0,  0x08

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d     L,   K,   OFF
#ifdef LEFT
    addi.d    L,   L,   -1
#else
    addi.d    L,   L,   -1
#endif
    slli.d    T0,  L,  0x03
    add.d     A0,  A0, T0
    add.d     B0,  B0, T0
#endif

#ifdef LEFT
    addi.d    OFF,   OFF,  0x01
#endif
#endif   // #if defined(TRMMKERNEL)

/********LOOP (if(N & 2 ) && (M & 1) ) End************/

.L_N1_M0:

/************************* Condition 4 if((N & 1) && (M >> 4)) End !!! *************************
*                                                   dgemm_core_16x1                                */

.L_N0:
    /* Restore $r23~$31 */
    LDARG    $r23,  $sp,   0
    LDARG    $r24,  $sp,   8
    LDARG    $r25,  $sp,   16
    LDARG    $r26,  $sp,   24
    LDARG    $r27,  $sp,   32
    LDARG    $r28,  $sp,   40
    LDARG    $r29,  $sp,   48
    LDARG    $r30,  $sp,   56
    LDARG    $r31,  $sp,   64
    fld.d    $f23,  $sp,   72
    fld.d    $f24,  $sp,   80
    fld.d    $f25,  $sp,   96
    fld.d    $f26,  $sp,   104
    fld.d    $f27,  $sp,   112
    fld.d    $f28,  $sp,   120
    fld.d    $f29,  $sp,   128
    fld.d    $f30,  $sp,   136
    fld.d    $f31,  $sp,   144
    addi.d   $sp,   $sp,   160

    /* Back home */
    jirl    $r0, $r1, 0x0

    EPILOGUE
