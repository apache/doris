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


/* Function parameters */
#define M      $r4   // param 1: bm
#define N      $r5   // param 2: bn
#define K      $r6   // param 3: bk
#define ALPHA_R $f0   // param 4: alphar
#define ALPHA_I $f1   // param 5: alphai
#define A      $r7   // param 6: ba
#define B      $r8  // param 7: bb
#define C      $r9  // param 8: bc
#define LDC    $r10  // param 9: ldc

#if defined (TRMMKERNEL)
#define OFFSET $r11  // param 10: offset
#endif
#define OFF    $r26

#define I      $r12
#define J      $r13
#define L      $r14
#define TL     $r15
#define A0     $r16
#define B0     $r17
#define C0     $r18
#define C1     $r19
#define C2     $r20
#define C3     $r23
#define T0     $r24
#define T1     $r25

#define a1     $f2
#define a2     $f3
#define a3     $f4
#define a4     $f5
#define a5     $f6
#define a6     $f7
#define a7     $f8
#define a8     $f9
#define b1     $f10
#define b2     $f11
#define b3     $f12
#define b4     $f13
#define b5     $f14
#define b6     $f15
#define b7     $f16
#define b8     $f17
#define c11    $f18
#define c12    $f19
#define c21    $f20
#define c22    $f21
#define c31    $f22
#define c32    $f23
#define c41    $f24
#define c42    $f25
#define c51    $f26
#define c52    $f27
#define c61    $f28
#define c62    $f29


#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
#define    MADD1       MADD
#define    MADD2       MADD
#define    MADD3       NMSUB
#define    MADD4       MADD
#endif

#if   defined(NR) || defined(NC) || defined(TR) || defined(TC)
#define    MADD1       MADD
#define    MADD2       MADD
#define    MADD3       MADD
#define    MADD4       NMSUB
#endif

#if   defined(RN) || defined(RT) || defined(CN) || defined(CT)
#define    MADD1       MADD
#define    MADD2       NMSUB
#define    MADD3       MADD
#define    MADD4       MADD
#endif

#if   defined(RR) || defined(RC) || defined(CR) || defined(CC)
#define    MADD1       MADD
#define    MADD2       NMSUB
#define    MADD3       NMSUB
#define    MADD4       NMSUB
#endif

    PROLOGUE

    addi.d     $sp,    $sp,   -88
    SDARG      $r23,   $sp,   0
    SDARG      $r24,   $sp,   8
    SDARG      $r25,   $sp,   16
    SDARG      $r26,   $sp,   24
    ST         $f23,   $sp,   32
    ST         $f24,   $sp,   40
    ST         $f25,   $sp,   48
    ST         $f26,   $sp,   56
    ST         $f27,   $sp,   64
    ST         $f28,   $sp,   72
    ST         $f29,   $sp,   80


#if defined (TRMMKERNEL) && !defined(LEFT)
    sub.d      OFF,    $r0,   OFFSET
#else
    xor        OFF,    OFF,   OFF
#endif

    slli.d     LDC,    LDC,   BASE_SHIFT

    move       J,      $r0
    srai.d     T0,     N,     1
    beq        J,      T0,    .L19

.L10:  /* for(j=0; j<bn/2; j+=1) */
    move       C0,     C
    slli.d     TL,     LDC,   1
    add.d      C1,     C0,    TL
    move       A0,     A    //ptrba

#if defined(TRMMKERNEL) && defined(LEFT)
    move       OFF,    OFFSET
#endif

    move       I,      $r0
    srai.d     T0,     M,     1
    beq        I,      T0,    .L150

.L11:  /* for(i=0; i<bm/2; i+=1) */
    move       B0,     B      //ptrbb
    move       TL,     K      /* TL = bk */
#if defined(TRMMKERNEL)

#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    move       B0,     B     //ptrbb
#else
    slli.d     C3,     OFF,   0x05
    add.d      A0,     A0,    C3
    add.d      B0,     B,     C3
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    sub.d      TL,     K,     OFF   //temp
#elif defined(LEFT)
    addi.d     TL,     OFF,   2
#else
    addi.d     TL,     OFF,   2
#endif

#endif  // #if defined(TRMMKERNEL)

    MTC        c11,    $r0
    MOV        c12,    c11
    MOV        c21,    c11
    MOV        c22,    c11
    MOV        c31,    c11
    MOV        c32,    c11
    MOV        c41,    c11
    MOV        c42,    c11

    move       L,      $r0   //cycle param k
    srai.d     C2,     TL,     2
    beq        L,      C2,    .L130
    blt        C2,     L,     .L130

.L12:  /* for(k=0; k<bk/4; k+=1) */
    LD         a1,     A0,    0x00        //load0
    LD         b1,     B0,    0x00        //load1
    MADD1      c11,    a1,    b1,     c11  //res0
    LD         a2,     A0,    0x08        //load2
    MADD2      c12,    a2,    b1,     c12  //res1
    LD         b2,     B0,    0x08        //load3
    MADD3      c11,    a2,    b2,     c11
    MADD4      c12,    a1,    b2,     c12
    LD         a3,     A0,    0x10        //load4
    MADD1      c21,    a3,    b1,     c21  //res2
    LD         a4,     A0,    0x18        //load5
    MADD2      c22,    a4,    b1,     c22  //res3
    MADD3      c21,    a4,    b2,     c21
    MADD4      c22,    a3,    b2,     c22
    LD         b3,     B0,    0x10        //load6
    MADD1      c31,    a1,    b3,     c31  //res4
    MADD2      c32,    a2,    b3,     c32  //res5
    LD         b4,     B0,    0x18        //load7
    MADD3      c31,    a2,    b4,     c31
    MADD4      c32,    a1,    b4,     c32
    MADD1      c41,    a3,    b3,     c41  //res6
    MADD2      c42,    a4,    b3,     c42  //res7
    MADD3      c41,    a4,    b4,     c41
    MADD4      c42,    a3,    b4,     c42
    LD         a5,     A0,    0x20        //load8
    LD         b5,     B0,    0x20        //load9
    MADD1      c11,    a5,    b5,     c11
    LD         a6,     A0,    0x28        //load10
    MADD2      c12,    a6,    b5,     c12
    LD         b6,     B0,    0x28        //load11
    MADD3      c11,    a6,    b6,     c11
    MADD4      c12,    a5,    b6,     c12
    LD         a7,     A0,    0x30        //load12
    MADD1      c21,    a7,    b5,     c21
    LD         a8,     A0,    0x38        //load13
    MADD2      c22,    a8,    b5,     c22
    MADD3      c21,    a8,    b6,     c21
    MADD4      c22,    a7,    b6,     c22
    LD         b7,     B0,    0x30        //load14
    MADD1      c31,    a5,    b7,     c31
    MADD2      c32,    a6,    b7,     c32
    LD         b8,     B0,    0x38        //load15
    MADD3      c31,    a6,    b8,     c31
    MADD4      c32,    a5,    b8,     c32
    MADD1      c41,    a7,    b7,     c41
    MADD2      c42,    a8,    b7,     c42
    MADD3      c41,    a8,    b8,     c41
    MADD4      c42,    a7,    b8,     c42

    LD         a1,     A0,    0x40        //load0
    LD         b1,     B0,    0x40        //load1
    MADD1      c11,    a1,    b1,     c11  //res0
    LD         a2,     A0,    0x48        //load2
    MADD2      c12,    a2,    b1,     c12  //res1
    LD         b2,     B0,    0x48        //load3
    MADD3      c11,    a2,    b2,     c11
    MADD4      c12,    a1,    b2,     c12
    LD         a3,     A0,    0x50        //load4
    MADD1      c21,    a3,    b1,     c21  //res2
    LD         a4,     A0,    0x58        //load5
    MADD2      c22,    a4,    b1,     c22  //res3
    MADD3      c21,    a4,    b2,     c21
    MADD4      c22,    a3,    b2,     c22
    LD         b3,     B0,    0x50        //load6
    MADD1      c31,    a1,    b3,     c31  //res4
    MADD2      c32,    a2,    b3,     c32  //res5
    LD         b4,     B0,    0x58        //load7
    MADD3      c31,    a2,    b4,     c31
    MADD4      c32,    a1,    b4,     c32
    MADD1      c41,    a3,    b3,     c41  //res6
    MADD2      c42,    a4,    b3,     c42  //res7
    MADD3      c41,    a4,    b4,     c41
    MADD4      c42,    a3,    b4,     c42
    LD         a5,     A0,    0x60        //load8
    LD         b5,     B0,    0x60        //load9
    MADD1      c11,    a5,    b5,     c11
    LD         a6,     A0,    0x68        //load10
    MADD2      c12,    a6,    b5,     c12
    LD         b6,     B0,    0x68        //load11
    MADD3      c11,    a6,    b6,     c11
    MADD4      c12,    a5,    b6,     c12
    LD         a7,     A0,    0x70        //load12
    MADD1      c21,    a7,    b5,     c21
    LD         a8,     A0,    0x78        //load13
    MADD2      c22,    a8,    b5,     c22
    MADD3      c21,    a8,    b6,     c21
    MADD4      c22,    a7,    b6,     c22
    LD         b7,     B0,    0x70        //load14
    MADD1      c31,    a5,    b7,     c31
    MADD2      c32,    a6,    b7,     c32
    LD         b8,     B0,    0x78        //load15
    MADD3      c31,    a6,    b8,     c31
    MADD4      c32,    a5,    b8,     c32
    MADD1      c41,    a7,    b7,     c41
    MADD2      c42,    a8,    b7,     c42
    MADD3      c41,    a8,    b8,     c41
    MADD4      c42,    a7,    b8,     c42

    addi.d     A0,     A0,    0x80
    addi.d     B0,     B0,    0x80

    addi.d     L,      L,     1
    blt        L,      C2,    .L12

.L130:
    move       L,      $r0
    andi       C2,     TL,     3
    beq        L,      C2,    .L14

.L13:  /* for(k=0; k<(bk&3); k+=1) */
    LD         a1,     A0,    0x00        //load0
    LD         b1,     B0,    0x00        //load1
    MADD1      c11,    a1,    b1,     c11  //res0
    LD         a2,     A0,    0x08        //load2
    MADD2      c12,    a2,    b1,     c12  //res1
    LD         b2,     B0,    0x08        //load3
    MADD3      c11,    a2,    b2,     c11
    MADD4      c12,    a1,    b2,     c12
    LD         a3,     A0,    0x10        //load4
    MADD1      c21,    a3,    b1,     c21  //res2
    LD         a4,     A0,    0x18        //load5
    MADD2      c22,    a4,    b1,     c22  //res3
    MADD3      c21,    a4,    b2,     c21
    MADD4      c22,    a3,    b2,     c22
    LD         b3,     B0,    0x10        //load6
    MADD1      c31,    a1,    b3,     c31  //res4
    MADD2      c32,    a2,    b3,     c32  //res5
    LD         b4,     B0,    0x18        //load7
    MADD3      c31,    a2,    b4,     c31
    MADD4      c32,    a1,    b4,     c32
    MADD1      c41,    a3,    b3,     c41  //res6
    MADD2      c42,    a4,    b3,     c42  //res7
    MADD3      c41,    a4,    b4,     c41
    MADD4      c42,    a3,    b4,     c42

    addi.d     A0,     A0,    0x20
    addi.d     B0,     B0,    0x20

    addi.d     L,      L,     1
    blt        L,      C2,    .L13

.L14:
#if defined(TRMMKERNEL)
    MUL        a5,     c11,   ALPHA_R
    MUL        a6,     c12,   ALPHA_I
    SUB        a5,     a5,    a6
    ST         a5,     C0,    0x00

    MUL        a5,     c12,   ALPHA_R
    MUL        a6,     c11,   ALPHA_I
    ADD        a6,     a5,    a6
    ST         a6,     C0,    0x08

    MUL        a7,     c21,   ALPHA_R
    MUL        a8,     c22,   ALPHA_I
    SUB        a7,     a7,    a8
    ST         a7,     C0,    0x10

    MUL        a7,     c22,   ALPHA_R
    MUL        a8,     c21,   ALPHA_I
    ADD        a8,     a7,    a8
    ST         a8,     C0,    0x18

    MUL        b5,     c31,   ALPHA_R
    MUL        b6,     c32,   ALPHA_I
    SUB        b5,     b5,    b6
    ST         b5,     C1,    0x00

    MUL        b5,     c32,   ALPHA_R
    MUL        b6,     c31,   ALPHA_I
    ADD        b6,     b5,    b6
    ST         b6,     C1,    0x08

    MUL        b7,     c41,   ALPHA_R
    MUL        b8,     c42,   ALPHA_I
    SUB        b7,     b7,    b8
    ST         b7,     C1,    0x10

    MUL        b7,     c42,   ALPHA_R
    MUL        b8,     c41,   ALPHA_I
    ADD        b8,     b7,    b8
    ST         b8,     C1,    0x18
#else
    LD         a5,     C0,    0x00    //C0[0]
    LD         a6,     C0,    0x08    //C0[1]
    LD         a7,     C0,    0x10    //C0[2]
    LD         a8,     C0,    0x18    //C0[3]
    LD         b5,     C1,    0x00    //C1[0]
    LD         b6,     C1,    0x08    //C1[1]
    LD         b7,     C1,    0x10    //C1[2]
    LD         b8,     C1,    0x18    //C1[3]

    MADD       a5,     c11,   ALPHA_R, a5
    MADD       a6,     c12,   ALPHA_R, a6
    NMSUB      a5,     c12,   ALPHA_I, a5
    MADD       a6,     c11,   ALPHA_I, a6
    ST         a5,     C0,    0x00
    ST         a6,     C0,    0x08

    MADD       a7,     c21,   ALPHA_R, a7
    MADD       a8,     c22,   ALPHA_R, a8
    NMSUB      a7,     c22,   ALPHA_I, a7
    MADD       a8,     c21,   ALPHA_I, a8
    ST         a7,     C0,    0x10
    ST         a8,     C0,    0x18

    MADD       b5,     c31,   ALPHA_R, b5
    MADD       b6,     c32,   ALPHA_R, b6
    NMSUB      b5,     c32,   ALPHA_I, b5
    MADD       b6,     c31,   ALPHA_I, b6
    ST         b5,     C1,    0x00
    ST         b6,     C1,    0x08

    MADD       b7,     c41,   ALPHA_R, b7
    MADD       b8,     c42,   ALPHA_R, b8
    NMSUB      b7,     c42,   ALPHA_I, b7
    MADD       b8,     c41,   ALPHA_I, b8
    ST         b7,     C1,    0x10
    ST         b8,     C1,    0x18
#endif

#if defined(TRMMKERNEL)

#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d      TL,     K,    OFF
#ifdef LEFT
    addi.d     TL,     TL,   -2
#else
    addi.d     TL,     TL,   -2
#endif
    slli.d     C3,     TL,   0x05
    add.d      A0,     A0,   C3
    add.d      B0,     B0,   C3
#endif

#ifdef LEFT
    addi.d     OFF,    OFF,   2
#endif

#endif   // #if defined(TRMMKERNEL)

    addi.d     C0,     C0,    0x20
    addi.d     C1,     C1,    0x20

    addi.d     I,      I,     1
    blt        I,      T0,    .L11

.L150:
    move       I,      $r0
    andi       T0,     M,     1
    beq        I,      T0,    .L18

.L15:  /* for(i=0; i<(bm&1); i+=1) */
    move       B0,     B      //ptrbb
    move       TL,     K      /* TL = bk */
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) ||(!defined(LEFT) && !defined(TRANSA))
    move       B0,     B    //ptrbb
#else
    slli.d     C3,     OFF,  0x04
    add.d      A0,     A0,   C3
    slli.d     C3,     OFF,  0x05
    add.d      B0,     B,    C3
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    sub.d      TL,     K,     OFF
#elif defined(LEFT)
    addi.d     TL,     OFF,   1
#else
    addi.d     TL,     OFF,   2
#endif

#endif  // #if defined(TRMMKERNEL)

    MTC        c11,    $r0
    MTC        c12,    $r0
    MTC        c21,    $r0
    MTC        c22,    $r0

    move       L,      $r0   //cycle param k
    beq        L,      TL,    .L17
    blt        TL,     L,     .L17

.L16:  /* for (k=0; k<bk; k+=1) */
    LD         a1,     A0,    0x00        //load0
    LD         b1,     B0,    0x00        //load1
    MADD1      c11,    a1,    b1,     c11  //res0
    LD         a2,     A0,    0x08        //load2
    MADD2      c12,    a2,    b1,     c12  //res1
    LD         b2,     B0,    0x08        //load3
    MADD3      c11,    a2,    b2,     c11
    MADD4      c12,    a1,    b2,     c12
    LD         b3,     B0,    0x10        //load4
    MADD1      c21,    a1,    b3,     c21  //res2
    MADD2      c22,    a2,    b3,     c22  //res3
    LD         b4,     B0,    0x18        //load5
    MADD3      c21,    a2,    b4,     c21
    MADD4      c22,    a1,    b4,     c22

    addi.d     A0,     A0,    0x10
    addi.d     B0,     B0,    0x20

    addi.d     L,      L,     1
    blt        L,      TL,     .L16

.L17:
#if defined(TRMMKERNEL)
    MUL        a5,     c11,   ALPHA_R
    MUL        a6,     c12,   ALPHA_I
    SUB        a5,     a5,    a6
    ST         a5,     C0,    0x00

    MUL        a5,     c12,   ALPHA_R
    MUL        a6,     c11,   ALPHA_I
    ADD        a6,     a5,    a6
    ST         a6,     C0,    0x08

    MUL        b5,     c21,   ALPHA_R
    MUL        b6,     c22,   ALPHA_I
    SUB        b5,     b5,    b6
    ST         b5,     C1,    0x00

    MUL        b5,     c22,   ALPHA_R
    MUL        b6,     c21,   ALPHA_I
    ADD        b6,     b5,    b6
    ST         b6,     C1,    0x08
#else
    LD         a5,     C0,    0x00    //C0[0]
    LD         a6,     C0,    0x08    //C0[1]
    LD         b5,     C1,    0x00    //C1[0]
    LD         b6,     C1,    0x08    //C1[1]

    MADD       a5,     c11,   ALPHA_R, a5
    MADD       a6,     c12,   ALPHA_R, a6
    NMSUB      a5,     c12,   ALPHA_I, a5
    MADD       a6,     c11,   ALPHA_I, a6
    ST         a5,     C0,    0x00
    ST         a6,     C0,    0x08

    MADD       b5,     c21,   ALPHA_R, b5
    MADD       b6,     c22,   ALPHA_R, b6
    NMSUB      b5,     c22,   ALPHA_I, b5
    MADD       b6,     c21,   ALPHA_I, b6
    ST         b5,     C1,    0x00
    ST         b6,     C1,    0x08
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d      TL,     K,    OFF
#ifdef LEFT
    addi.d     TL,     TL,   -1
#else
    addi.d     TL,     TL,   -2
#endif
    slli.d     C3,     TL,   0x04
    add.d      A0,     A0,   C3
    slli.d     C3,     TL,   0x05
    add.d      B0,     B0,   C3
#endif

#ifdef LEFT
    addi.d     OFF,    OFF,   1
#endif
#endif   // #if defined(TRMMKERNEL)

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10

    addi.d     I,      I,     1
    blt        I,      T0,    .L15

.L18:
#if defined(TRMMKERNEL) && !defined(LEFT)
    addi.d     OFF,    OFF,   2
#endif

    slli.d     L,      K,     0x05
    add.d      B,      B,     L

    slli.d     I,      LDC,   0x02
    add.d      C,      C,     I

    addi.d     J,      J,     1
    srai.d     T0,     N,     1
    blt        J,      T0,    .L10

.L19:
    move       J,      $r0
    andi       T0,     N,     1
    beq        J,      T0,    .L30

.L20: /* for (j=0; j<(bn&1); j+=1) */
#if defined(TRMMKERNEL) && defined(LEFT)
    move       OFF,    OFFSET
#endif

    move       C0,     C
    move       A0,     A    //ptrba

    move       I,      $r0
    srai.d     T0,     M,     1
    beq        I,      T0,    .L24

.L21:  /* for (i=0; i<bm/2; i+=1) */
    move       B0,     B      //ptrbb
    move       TL,     K      /* TL = bk */
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) ||(!defined(LEFT) && !defined(TRANSA))
    move       B0,     B    //ptrbb
#else
    slli.d     C3,     OFF,  0x05
    add.d      A0,     A0,   C3
    slli.d     C3,     OFF,  0x04
    add.d      B0,     B,    C3
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    sub.d      TL,     K,     OFF
#elif defined(LEFT)
    addi.d     TL,     OFF,   2
#else
    addi.d     TL,     OFF,   1
#endif

#endif  // #if defined(TRMMKERNEL)

    MTC        c11,    $r0
    MTC        c12,    $r0
    MTC        c21,    $r0
    MTC        c22,    $r0

    move       L,      $r0   //cycle param k
    beq        L,      TL,    .L23
    blt        TL,     L,     .L23

.L22:  /* for (k=0; k<bk; k+=1) */
    LD         a1,     A0,    0x00        //load0
    LD         b1,     B0,    0x00        //load1
    MADD1      c11,    a1,    b1,     c11  //res0
    LD         a2,     A0,    0x08        //load2
    MADD2      c12,    a2,    b1,     c12  //res1
    LD         b2,     B0,    0x08        //load3
    MADD3      c11,    a2,    b2,     c11
    MADD4      c12,    a1,    b2,     c12
    LD         a3,     A0,    0x10        //load4
    MADD1      c21,    a3,    b1,     c21  //res2
    LD         a4,     A0,    0x18        //load5
    MADD2      c22,    a4,    b1,     c22  //res3
    MADD3      c21,    a4,    b2,     c21
    MADD4      c22,    a3,    b2,     c22

    addi.d     A0,     A0,    0x20
    addi.d     B0,     B0,    0x10

    addi.d     L,      L,     1
    blt        L,      TL,    .L22

.L23:
#if defined(TRMMKERNEL)
    MUL        a5,     c11,   ALPHA_R
    MUL        a6,     c12,   ALPHA_I
    SUB        a5,     a5,    a6
    ST         a5,     C0,    0x00

    MUL        a5,     c12,   ALPHA_R
    MUL        a6,     c11,   ALPHA_I
    ADD        a6,     a5,    a6
    ST         a6,     C0,    0x08

    MUL        a7,     c21,   ALPHA_R
    MUL        a8,     c22,   ALPHA_I
    SUB        a7,     a7,    a8
    ST         a7,     C0,    0x10

    MUL        a7,     c22,   ALPHA_R
    MUL        a8,     c21,   ALPHA_I
    ADD        a8,     a7,    a8
    ST         a8,     C0,    0x18
#else
    LD         a5,     C0,    0x00    //C0[0]
    LD         a6,     C0,    0x08    //C0[1]
    LD         a7,     C0,    0x10    //C1[2]
    LD         a8,     C0,    0x18    //C1[3]

    MADD       a5,     c11,   ALPHA_R, a5
    MADD       a6,     c12,   ALPHA_R, a6
    NMSUB      a5,     c12,   ALPHA_I, a5
    MADD       a6,     c11,   ALPHA_I, a6
    MADD       a7,     c21,   ALPHA_R, a7
    MADD       a8,     c22,   ALPHA_R, a8
    NMSUB      a7,     c22,   ALPHA_I, a7
    MADD       a8,     c21,   ALPHA_I, a8

    ST         a5,     C0,    0x00
    ST         a6,     C0,    0x08
    ST         a7,     C0,    0x10
    ST         a8,     C0,    0x18
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d      TL,     K,    OFF
#ifdef LEFT
    addi.d     TL,     TL,   -2
#else
    addi.d     TL,     TL,   -1
#endif
    slli.d     C3,     TL,   0x05
    add.d      A0,     A0,   C3
    slli.d     C3,     TL,   0x04
    add.d      B0,     B0,   C3
#endif

#ifdef LEFT
    addi.d     OFF,    OFF,   2
#endif
#endif   // #if defined(TRMMKERNEL)

    addi.d     C0,     C0,    0x20

    addi.d     I,      I,     1
    blt        I,      T0,    .L21

.L24:
    move       I,      $r0
    andi       T1,     M,     1    //bm&1
    beq        I,      T1,    .L28

.L25:  /* for (i=0; i<(bm&1); i+=1) */
    move       B0,     B      //ptrbb
    move       TL,     K      /* TL = bk */
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) ||(!defined(LEFT) && !defined(TRANSA))
    move       B0,     B    //ptrbb
#else
    slli.d     C3,     OFF,   0x04
    add.d      A0,     A0,   C3
    add.d      B0,     B,    C3
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    sub.d      TL,     K,     OFF
#elif defined(LEFT)
    addi.d     TL,     OFF,   1
#else
    addi.d     TL,     OFF,   1
#endif

#endif  // #if defined(TRMMKERNEL)

    MTC        c11,    $r0
    MTC        c12,    $r0

    move       L,      $r0   //cycle param k
    beq        L,      TL,    .L27
    blt        TL,     L,     .L27

.L26:  /* for (k=0; k<bk; k+=1) */
    LD         a1,     A0,    0x00        //load0
    LD         b1,     B0,    0x00        //load1
    MADD1      c11,    a1,    b1,     c11  //res0
    LD         a2,     A0,    0x08        //load2
    MADD2      c12,    a2,    b1,     c12  //res1
    LD         b2,     B0,    0x08        //load3
    MADD3      c11,    a2,    b2,     c11
    MADD4      c12,    a1,    b2,     c12

    addi.d     A0,     A0,    0x10
    addi.d     B0,     B0,    0x10

    addi.d     L,      L,     1
    blt        L,      TL,    .L26

.L27:
#if defined(TRMMKERNEL)
    MUL        a5,     c11,   ALPHA_R
    MUL        a6,     c12,   ALPHA_I
    SUB        a5,     a5,    a6
    ST         a5,     C0,    0x00

    MUL        a5,     c12,   ALPHA_R
    MUL        a6,     c11,   ALPHA_I
    ADD        a6,     a5,    a6
    ST         a6,     C0,    0x08
#else
    LD         a5,     C0,    0x00    //C0[0]
    LD         a6,     C0,    0x08    //C0[1]

    MADD       a5,     c11,   ALPHA_R, a5
    MADD       a6,     c12,   ALPHA_R, a6
    NMSUB      a5,     c12,   ALPHA_I, a5
    MADD       a6,     c11,   ALPHA_I, a6

    ST         a5,     C0,    0x00
    ST         a6,     C0,    0x08
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d      TL,     K,    OFF
#ifdef LEFT
    addi.d     TL,     TL,   -1
#else
    addi.d     TL,     TL,   -1
#endif
    slli.d     C3,     TL,   0x04
    add.d      A0,     A0,   C3
    add.d      B0,     B0,   C3
#endif

#ifdef LEFT
    addi.d     OFF,    OFF,   1
#endif
#endif   // #if defined(TRMMKERNEL)

    addi.d     C0,     C0,    0x10

    addi.d     I,      I,     1
    blt        I,      T1,    .L25

.L28:
    slli.d     L,      K,     4
    add.d      B,      B,     L

    slli.d     I,      LDC,   1
    add.d      C,      C,     I

    addi.d     J,      J,     1
    andi       T0,     N,     1
    blt        J,      T0,    .L20

.L30:
    LDARG      $r23,   $sp,   0
    LDARG      $r24,   $sp,   8
    LDARG      $r25,   $sp,   16
    LDARG      $r26,   $sp,   24
    LD         $f23,   $sp,   32
    LD         $f24,   $sp,   40
    LD         $f25,   $sp,   48
    LD         $f26,   $sp,   56
    LD         $f27,   $sp,   64
    LD         $f28,   $sp,   72
    LD         $f29,   $sp,   80

    addi.d     $sp,    $sp,   88
    jirl       $r0,    $r1,   0x0

    EPILOGUE