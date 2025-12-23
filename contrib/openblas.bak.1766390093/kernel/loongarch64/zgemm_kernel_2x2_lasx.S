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

/* LASX vectors */
#define U0     $xr30
#define U1     $xr31
#define U2     $xr2
#define U3     $xr3
#define U4     $xr4
#define U5     $xr5
#define U6     $xr6
#define U7     $xr7
#define U8     $xr8
#define U9     $xr9
#define U10    $xr10
#define U11    $xr11
#define U12    $xr12
#define U13    $xr13
#define U14    $xr14
#define U15    $xr15
#define D0     $xr16
#define D1     $xr17
#define D2     $xr18
#define D3     $xr19
#define D4     $xr20
#define D5     $xr21
#define D6     $xr22
#define D7     $xr23
#define D8     $xr24
#define D9     $xr25
#define D10    $xr26
#define D11    $xr27
#define D12    $xr28
#define D13    $xr29
#define VALPHAR $xr28
#define VALPHAI $xr29


#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
#define    XVMADD1       XVFMADD
#define    XVMADD2       XVFMADD
#define    XVMADD3       XVNMSUB
#define    XVMADD4       XVFMADD

#define    MADD1       MADD
#define    MADD2       MADD
#define    MADD3       NMSUB
#define    MADD4       MADD
#endif

#if   defined(NR) || defined(NC) || defined(TR) || defined(TC)
#define    XVMADD1       XVFMADD
#define    XVMADD2       XVFMADD
#define    XVMADD3       XVFMADD
#define    XVMADD4       XVNMSUB

#define    MADD1       MADD
#define    MADD2       MADD
#define    MADD3       MADD
#define    MADD4       NMSUB
#endif

#if   defined(RN) || defined(RT) || defined(CN) || defined(CT)
#define    XVMADD1       XVFMADD
#define    XVMADD2       XVNMSUB
#define    XVMADD3       XVFMADD
#define    XVMADD4       XVFMADD

#define    MADD1       MADD
#define    MADD2       NMSUB
#define    MADD3       MADD
#define    MADD4       MADD
#endif

#if   defined(RR) || defined(RC) || defined(CR) || defined(CC)
#define    XVMADD1       XVFMADD
#define    XVMADD2       XVNMSUB
#define    XVMADD3       XVNMSUB
#define    XVMADD4       XVNMSUB

#define    MADD1       MADD
#define    MADD2       NMSUB
#define    MADD3       NMSUB
#define    MADD4       NMSUB
#endif

    PROLOGUE

    addi.d     $sp,    $sp,   -128
    SDARG      $r23,   $sp,   0
    SDARG      $r24,   $sp,   8
    SDARG      $r25,   $sp,   16
    SDARG      $r26,   $sp,   24
    SDARG      $r27,   $sp,   32
    ST         $f23,   $sp,   40
    ST         $f24,   $sp,   48
    ST         $f25,   $sp,   56
    ST         $f26,   $sp,   64
    ST         $f27,   $sp,   72
    ST         $f28,   $sp,   80
    ST         $f29,   $sp,   88
    ST         $f30,   $sp,   96
    ST         $f31,   $sp,   104
    ST         ALPHA_R,$sp,   112
    ST         ALPHA_I,$sp,   120

    xvldrepl.d  VALPHAR, $sp, 112
    xvldrepl.d  VALPHAI, $sp, 120

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

    xvxor.v    U0,     U0,   U0
    move       L,      $r0   //cycle param k
    srai.d     C2,     TL,     2
    xvxor.v    U1,     U1,   U1
    beq        L,      C2,    .L130
    blt        C2,     L,     .L130

.L12:  /* for(k=0; k<bk/4; k+=1) */
    xvld       D0,     A0,    0x00  //a0 a1 a2 a3
    xvld       D1,     B0,    0x00  //b0 b1 b2 b3

    xvpermi.d  D4,     D1,    0xa0    //b0 b0 b2 b2
    xvpermi.d  D5,     D1,    0xf5    //b1 b1 b3 b3
    xvpermi.d  D2,     D0,    0x88    //a0 a2 a0 a2
    xvpermi.d  D3,     D0,    0xdd    //a1 a3 a1 a3

    XVMADD1      U0,     D2,    D4,     U0  //res0 2 4 6
    XVMADD2      U1,     D3,    D4,     U1  //res1 3 4 7
    XVMADD3      U0,     D3,    D5,     U0
    XVMADD4      U1,     D2,    D5,     U1

    xvld       D0,     A0,    0x20  //a0 a1 a2 a3
    xvld       D1,     B0,    0x20  //b0 b1 b2 b3

    xvpermi.d  D4,     D1,    0xa0    //b0 b0 b2 b2
    xvpermi.d  D5,     D1,    0xf5    //b1 b1 b3 b3
    xvpermi.d  D2,     D0,    0x88    //a0 a2 a0 a2
    xvpermi.d  D3,     D0,    0xdd    //a1 a3 a1 a3

    XVMADD1      U0,     D2,    D4,     U0  //res0 2 4 6
    XVMADD2      U1,     D3,    D4,     U1  //res1 3 4 7
    XVMADD3      U0,     D3,    D5,     U0
    XVMADD4      U1,     D2,    D5,     U1

    xvld       D0,     A0,    0x40  //a0 a1 a2 a3
    xvld       D1,     B0,    0x40  //b0 b1 b2 b3

    xvpermi.d  D4,     D1,    0xa0    //b0 b0 b2 b2
    xvpermi.d  D5,     D1,    0xf5    //b1 b1 b3 b3
    xvpermi.d  D2,     D0,    0x88    //a0 a2 a0 a2
    xvpermi.d  D3,     D0,    0xdd    //a1 a3 a1 a3

    XVMADD1      U0,     D2,    D4,     U0  //res0 2 4 6
    XVMADD2      U1,     D3,    D4,     U1  //res1 3 4 7
    XVMADD3      U0,     D3,    D5,     U0
    XVMADD4      U1,     D2,    D5,     U1

    xvld       D0,     A0,    0x60  //a0 a1 a2 a3
    xvld       D1,     B0,    0x60  //b0 b1 b2 b3

    xvpermi.d  D4,     D1,    0xa0    //b0 b0 b2 b2
    xvpermi.d  D5,     D1,    0xf5    //b1 b1 b3 b3
    xvpermi.d  D2,     D0,    0x88    //a0 a2 a0 a2
    xvpermi.d  D3,     D0,    0xdd    //a1 a3 a1 a3

    XVMADD1      U0,     D2,    D4,     U0  //res0 2 4 6
    XVMADD2      U1,     D3,    D4,     U1  //res1 3 4 7
    XVMADD3      U0,     D3,    D5,     U0
    XVMADD4      U1,     D2,    D5,     U1

    addi.d     A0,     A0,    0x80
    addi.d     B0,     B0,    0x80

    addi.d     L,      L,     1
    blt        L,      C2,    .L12

.L130:
    move       L,      $r0
    andi       C2,     TL,     3
    beq        L,      C2,    .L14

.L13:  /* for(k=0; k<(bk&3); k+=1) */
    xvld       D0,     A0,    0x00  //a0 a1 a2 a3
    xvld       D1,     B0,    0x00  //b0 b1 b2 b3

    xvpermi.d  D4,     D1,    0xa0    //b0 b0 b2 b2
    xvpermi.d  D5,     D1,    0xf5    //b1 b1 b3 b3
    xvpermi.d  D2,     D0,    0x88    //a0 a2 a0 a2
    xvpermi.d  D3,     D0,    0xdd    //a1 a3 a1 a3

    XVMADD1      U0,     D2,    D4,     U0  //res0 2 4 6
    XVMADD2      U1,     D3,    D4,     U1  //res1 3 5 7
    XVMADD3      U0,     D3,    D5,     U0
    XVMADD4      U1,     D2,    D5,     U1

    addi.d     A0,     A0,    0x20
    addi.d     B0,     B0,    0x20

    addi.d     L,      L,     1
    blt        L,      C2,    .L13

.L14:
#if defined(TRMMKERNEL)
    xvld       U8,     C0,    0x00  //0 1 2 3
    xvld       U9,     C1,    0x00  //4 5 6 7

    xvpackev.d U10,    U9,    U8
    xvpermi.d  U10,    U10,   0xd8  //0 2 4 6
    xvpackod.d U11,    U9,    U8
    xvpermi.d  U11,    U11,   0xd8  //1 3 5 7

    xvfmul.d      U10,    U0,    VALPHAR
    xvfmul.d      U11,    U1,    VALPHAR
    XVNMSUB     U10,    U1,    VALPHAI, U10
    XVFMADD      U11,    U0,    VALPHAI, U11

    xvand.v    U8,     U10,   U10  //0 2 4 6
    xvpermi.q  U8,     U11,   0x02 //0 2 1 3
    xvpermi.d  U8,     U8,    0xd8 //0 1 2 3

    xvand.v    U9,     U11,   U11  //1 3 5 7
    xvpermi.q  U9,     U10,   0x31 //4 6 5 7
    xvpermi.d  U9,     U9,    0xd8 //4 5 6 7

    xvst       U8,     C0,    0x00
    xvst       U9,     C1,    0x00
#else
    xvld       U8,     C0,    0x00  //0 1 2 3
    xvld       U9,     C1,    0x00  //4 5 6 7

    xvpackev.d U10,    U9,    U8
    xvpermi.d  U10,    U10,   0xd8  //0 2 4 6
    xvpackod.d U11,    U9,    U8
    xvpermi.d  U11,    U11,   0xd8  //1 3 5 7

    XVFMADD      U10,    U0,    VALPHAR, U10
    XVFMADD      U11,    U1,    VALPHAR, U11
    XVNMSUB     U10,    U1,    VALPHAI, U10
    XVFMADD      U11,    U0,    VALPHAI, U11

    xvand.v    U8,     U10,   U10  //0 2 4 6
    xvpermi.q  U8,     U11,   0x02 //0 2 1 3
    xvpermi.d  U8,     U8,    0xd8 //0 1 2 3

    xvand.v    U9,     U11,   U11  //1 3 5 7
    xvpermi.q  U9,     U10,   0x31 //4 6 5 7
    xvpermi.d  U9,     U9,    0xd8 //4 5 6 7

    xvst       U8,     C0,    0x00
    xvst       U9,     C1,    0x00
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
    LD         a2,     A0,    0x08        //load2
    LD         b2,     B0,    0x08        //load3
    LD         b3,     B0,    0x10        //load4
    LD         b4,     B0,    0x18        //load5

    MADD1      c11,    a1,    b1,     c11  //res0
    MADD2      c12,    a2,    b1,     c12  //res1
    MADD3      c11,    a2,    b2,     c11
    MADD4      c12,    a1,    b2,     c12

    MADD1      c21,    a1,    b3,     c21  //res2
    MADD2      c22,    a2,    b3,     c22  //res3
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

    MADD       b5,     c21,   ALPHA_R, b5
    MADD       b6,     c22,   ALPHA_R, b6
    NMSUB      b5,     c22,   ALPHA_I, b5
    MADD       b6,     c21,   ALPHA_I, b6

    ST         a5,     C0,    0x00
    ST         a6,     C0,    0x08

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
    LD         a2,     A0,    0x08        //load2
    LD         b2,     B0,    0x08        //load3
    LD         a3,     A0,    0x10        //load4
    LD         a4,     A0,    0x18        //load5

    MADD1      c11,    a1,    b1,     c11  //res0
    MADD2      c12,    a2,    b1,     c12  //res1
    MADD3      c11,    a2,    b2,     c11
    MADD4      c12,    a1,    b2,     c12

    MADD1      c21,    a3,    b1,     c21  //res2
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
    LD         a2,     A0,    0x08        //load2
    LD         b2,     B0,    0x08        //load3

    MADD1      c11,    a1,    b1,     c11  //res0
    MADD2      c12,    a2,    b1,     c12  //res1
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
    LDARG      $r27,   $sp,   32
    LD         $f23,   $sp,   40
    LD         $f24,   $sp,   48
    LD         $f25,   $sp,   56
    LD         $f26,   $sp,   64
    LD         $f27,   $sp,   72
    LD         $f28,   $sp,   80
    LD         $f29,   $sp,   88
    LD         $f30,   $sp,   96
    LD         $f31,   $sp,   104

    addi.d     $sp,    $sp,   128
    jirl       $r0,    $r1,   0x0

    EPILOGUE