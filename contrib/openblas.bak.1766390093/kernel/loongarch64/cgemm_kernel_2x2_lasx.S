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
#define VALPHAR    $xr28
#define VALPHAI    $xr29


#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
#define    XVMADD1       XVFMADD
#define    XVMADD2       XVFMADD
#define    XVMADD3       XVNMSUB
#define    XVMADD4       XVFMADD

#define    VMADD1       VFMADD
#define    VMADD2       VFMADD
#define    VMADD3       VNMSUB
#define    VMADD4       VFMADD

#define    XVFADD1       XVFADD
#define    XVFADD2       XVFADD
#define    XVFADD3       XVFSUB
#define    XVFADD4       XVFADD

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

#define    VMADD1       VFMADD
#define    VMADD2       VFMADD
#define    VMADD3       VFMADD
#define    VMADD4       VNMSUB

#define    XVFADD1       XVFADD
#define    XVFADD2       XVFADD
#define    XVFADD3       XVFADD
#define    XVFADD4       XVFSUB

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

#define    VMADD1       VFMADD
#define    VMADD2       VNMSUB
#define    VMADD3       VFMADD
#define    VMADD4       VFMADD

#define    XVFADD1       XVFADD
#define    XVFADD2       XVFSUB
#define    XVFADD3       XVFADD
#define    XVFADD4       XVFADD

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

#define    VMADD1       VFMADD
#define    VMADD2       VNMSUB
#define    VMADD3       VNMSUB
#define    VMADD4       VNMSUB

#define    XVFADD1       XVFADD
#define    XVFADD2       XVFSUB
#define    XVFADD3       XVFSUB
#define    XVFADD4       XVFSUB

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

    xvldrepl.w  VALPHAR, $sp, 112
    xvldrepl.w  VALPHAI, $sp, 120

#if defined (TRMMKERNEL) && !defined(LEFT)
    sub.d      OFF,    $r0,   OFFSET
#else
    xor        OFF,    OFF,   OFF
#endif

    slli.d     LDC,    LDC,   2

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
    slli.d     C3,     OFF,   0x04
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
    xvxor.v    U1,     U1,   U1

    move       L,      $r0   //cycle param k
    srai.d     C2,     TL,     2
    beq        L,      C2,    .L130
    blt        C2,     L,     .L130

.L12:  /* for(k=0; k<bk/4; k+=1) */
    xvld       D0,     A0,    0x00  //a 0-7
    xvld       D1,     A0,    0x20  //a 8-15
    xvld       D2,     B0,    0x00  //b 0-7
    xvld       D3,     B0,    0x20  //b 8-15

    xvand.v    D4,     D0,    D0
    xvpermi.q  D4,     D1,    0x02  //a 0 1 2 3 8 9 10 11
    xvand.v    D5,     D4,    D4
    xvshuf4i.w D4,     D4,    0x88  //a 0 2 0 2 8 10 8 10
    xvshuf4i.w D5,     D5,    0xdd  //a 1 3 1 3 9 11 9 11

    xvand.v    D6,     D1,    D1
    xvpermi.q  D6,     D0,    0x31  //a 4 5 6 7 12 13 14 15
    xvand.v    D7,     D6,    D6
    xvshuf4i.w D6,     D6,    0x88  //a 4 6 4 6 12 14 12 14
    xvshuf4i.w D7,     D7,    0xdd  //a 5 7 5 7 13 15 13 15

    xvand.v    D8,     D2,    D2
    xvpermi.q  D8,     D3,    0x02  //b 0 1 2 3 8 9 10 11
    xvand.v    D9,     D8,    D8
    xvshuf4i.w D8,     D8,    0xa0  //b 0 0 2 2 8 8 10 10
    xvshuf4i.w D9,     D9,    0xf5  //a 1 1 3 3 9 9 11 11

    xvand.v    D10,     D3,    D3
    xvpermi.q  D10,     D2,    0x31  //b 4 5 6 7 12 13 14 15
    xvand.v    D11,     D10,    D10
    xvshuf4i.w D10,     D10,    0xa0  //b 4 4 6 6 12 12 14 14
    xvshuf4i.w D11,     D11,    0xf5  //a 5 5 7 7 13 13 15 15

    XVMADD1      U0,     D4,    D8,     U0  //res0 2 4 6 0 2 4 6
    XVMADD2      U1,     D5,    D8,     U1  //res1 3 4 7 1 3 4 7

    xvpermi.q    U0,     U0,    0x01
    xvpermi.q    U1,     U1,    0x01
    XVMADD1      U0,     D4,    D8,     U0
    XVMADD2      U1,     D5,    D8,     U1

    XVMADD3      U0,     D5,    D9,     U0
    XVMADD4      U1,     D4,    D9,     U1

    xvpermi.q    U0,     U0,    0x01
    xvpermi.q    U1,     U1,    0x01
    XVMADD3      U0,     D5,    D9,     U0
    XVMADD4      U1,     D4,    D9,     U1

    XVMADD1      U0,     D6,    D10,     U0  //res0 2 4 6 0 2 4 6
    XVMADD2      U1,     D7,    D10,     U1  //res1 3 4 7 1 3 4 7

    xvpermi.q    U0,     U0,    0x01
    xvpermi.q    U1,     U1,    0x01
    XVMADD1      U0,     D6,    D10,     U0
    XVMADD2      U1,     D7,    D10,     U1

    XVMADD3      U0,     D7,    D11,     U0
    XVMADD4      U1,     D6,    D11,     U1

    xvpermi.q    U0,     U0,    0x01
    xvpermi.q    U1,     U1,    0x01
    XVMADD3      U0,     D7,    D11,     U0
    XVMADD4      U1,     D6,    D11,     U1

    addi.d     A0,     A0,    0x40
    addi.d     B0,     B0,    0x40

    addi.d     L,      L,     1
    blt        L,      C2,    .L12

.L130:
    move       L,      $r0
    andi       C2,     TL,     3
    beq        L,      C2,    .L14

.L13:  /* for(k=0; k<(bk&3); k+=1) */
    vld       $vr16,     A0,    0x00  //a0 a1 a2 a3
    vld       $vr17,     B0,    0x00  //b0 b1 b2 b3

    vshuf4i.w $vr20,     $vr17,    0xa0    //b0 b0 b2 b2
    vshuf4i.w $vr21,     $vr17,    0xf5    //b1 b1 b3 b3

    vshuf4i.w  $vr18,     $vr16,    0x88  //a0 a2 a0 a2
    vshuf4i.w  $vr19,     $vr16,    0xdd  //a1 a3 a1 a3

    VMADD1      $vr30,     $vr18,    $vr20,     $vr30  //res0 2 4 6
    VMADD2      $vr31,     $vr19,    $vr20,     $vr31  //res1 3 5 7
    VMADD3      $vr30,     $vr19,    $vr21,     $vr30
    VMADD4      $vr31,     $vr18,    $vr21,     $vr31

    addi.d     A0,     A0,    0x10
    addi.d     B0,     B0,    0x10

    addi.d     L,      L,     1
    blt        L,      C2,    .L13

.L14:
#if defined(TRMMKERNEL)
    vld       $vr8,     C0,    0x00  //0 1 2 3
    vld       $vr9,     C1,    0x00  //4 5 6 7

    vpackev.w $vr10,    $vr9,    $vr8    //0 4 2 6
    vpermi.w  $vr10,    $vr10,   0xd8    //0 2 4 6

    vpackod.w $vr11,    $vr9,    $vr8    //1 5 3 7
    vpermi.w  $vr11,    $vr11,   0xd8    //1 3 5 7

    vfmul.s      $vr10,    $vr30,    $vr28
    vfmul.s      $vr11,    $vr31,    $vr28
    VNMSUB    $vr10,    $vr31,    $vr29, $vr10
    VFMADD     $vr11,    $vr30,    $vr29, $vr11

    vilvl.w   $vr8,     $vr11,   $vr10  //0 1 2 3

    vilvh.w   $vr9,     $vr11,   $vr10  //4 5 6 7

    vst       $vr8,     C0,    0x00
    vst       $vr9,     C1,    0x00
#else
    vld       $vr8,     C0,    0x00  //0 1 2 3
    vld       $vr9,     C1,    0x00  //4 5 6 7

    vpackev.w $vr10,    $vr9,    $vr8    //0 4 2 6
    vpermi.w  $vr10,    $vr10,   0xd8    //0 2 4 6

    vpackod.w $vr11,    $vr9,    $vr8    //1 5 3 7
    vpermi.w  $vr11,    $vr11,   0xd8    //1 3 5 7

    VFMADD      $vr10,    $vr30,    $vr28, $vr10
    VFMADD      $vr11,    $vr31,    $vr28, $vr11
    VNMSUB     $vr10,    $vr31,    $vr29, $vr10
    VFMADD      $vr11,    $vr30,    $vr29, $vr11

    vilvl.w   $vr8,     $vr11,   $vr10  //0 1 2 3

    vilvh.w   $vr9,     $vr11,   $vr10  //4 5 6 7

    vst       $vr8,     C0,    0x00
    vst       $vr9,     C1,    0x00
#endif

#if defined(TRMMKERNEL)

#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d      TL,     K,    OFF
#ifdef LEFT
    addi.d     TL,     TL,   -2
#else
    addi.d     TL,     TL,   -2
#endif
    slli.d     C3,     TL,   0x04
    add.d      A0,     A0,   C3
    add.d      B0,     B0,   C3
#endif

#ifdef LEFT
    addi.d     OFF,    OFF,   2
#endif

#endif   // #if defined(TRMMKERNEL)

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10

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
    slli.d     C3,     OFF,  0x03
    add.d      A0,     A0,   C3
    slli.d     C3,     OFF,  0x04
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
    LD         a2,     A0,    0x04        //load2
    MADD2      c12,    a2,    b1,     c12  //res1
    LD         b2,     B0,    0x04        //load3
    MADD3      c11,    a2,    b2,     c11
    MADD4      c12,    a1,    b2,     c12
    LD         b3,     B0,    0x08        //load4
    MADD1      c21,    a1,    b3,     c21  //res2
    MADD2      c22,    a2,    b3,     c22  //res3
    LD         b4,     B0,    0x0c        //load5
    MADD3      c21,    a2,    b4,     c21
    MADD4      c22,    a1,    b4,     c22

    addi.d     A0,     A0,    0x08
    addi.d     B0,     B0,    0x10

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
    ST         a6,     C0,    0x04

    MUL        b5,     c21,   ALPHA_R
    MUL        b6,     c22,   ALPHA_I
    SUB        b5,     b5,    b6
    ST         b5,     C1,    0x00

    MUL        b5,     c22,   ALPHA_R
    MUL        b6,     c21,   ALPHA_I
    ADD        b6,     b5,    b6
    ST         b6,     C1,    0x04
#else
    LD         a5,     C0,    0x00    //C0[0]
    LD         a6,     C0,    0x04    //C0[1]
    LD         b5,     C1,    0x00    //C1[0]
    LD         b6,     C1,    0x04    //C1[1]

    MADD       a5,     c11,   ALPHA_R, a5
    MADD       a6,     c12,   ALPHA_R, a6
    NMSUB      a5,     c12,   ALPHA_I, a5
    MADD       a6,     c11,   ALPHA_I, a6
    ST         a5,     C0,    0x00
    ST         a6,     C0,    0x04

    MADD       b5,     c21,   ALPHA_R, b5
    MADD       b6,     c22,   ALPHA_R, b6
    NMSUB      b5,     c22,   ALPHA_I, b5
    MADD       b6,     c21,   ALPHA_I, b6
    ST         b5,     C1,    0x00
    ST         b6,     C1,    0x04
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d      TL,     K,    OFF
#ifdef LEFT
    addi.d     TL,     TL,   -1
#else
    addi.d     TL,     TL,   -2
#endif
    slli.d     C3,     TL,   0x03
    add.d      A0,     A0,   C3
    slli.d     C3,     TL,   0x04
    add.d      B0,     B0,   C3
#endif

#ifdef LEFT
    addi.d     OFF,    OFF,   1
#endif
#endif   // #if defined(TRMMKERNEL)

    addi.d     C0,     C0,    0x08
    addi.d     C1,     C1,    0x08

    addi.d     I,      I,     1
    blt        I,      T0,    .L15

.L18:
#if defined(TRMMKERNEL) && !defined(LEFT)
    addi.d     OFF,    OFF,   2
#endif

    slli.d     L,      K,     0x04
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
    slli.d     C3,     OFF,  0x04
    add.d      A0,     A0,   C3
    slli.d     C3,     OFF,  0x03
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
    LD         a2,     A0,    0x04        //load2
    MADD2      c12,    a2,    b1,     c12  //res1
    LD         b2,     B0,    0x04        //load3
    MADD3      c11,    a2,    b2,     c11
    MADD4      c12,    a1,    b2,     c12
    LD         a3,     A0,    0x08        //load4
    MADD1      c21,    a3,    b1,     c21  //res2
    LD         a4,     A0,    0x0c        //load5
    MADD2      c22,    a4,    b1,     c22  //res3
    MADD3      c21,    a4,    b2,     c21
    MADD4      c22,    a3,    b2,     c22

    addi.d     A0,     A0,    0x10
    addi.d     B0,     B0,    0x08

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
    ST         a6,     C0,    0x04

    MUL        a7,     c21,   ALPHA_R
    MUL        a8,     c22,   ALPHA_I
    SUB        a7,     a7,    a8
    ST         a7,     C0,    0x08

    MUL        a7,     c22,   ALPHA_R
    MUL        a8,     c21,   ALPHA_I
    ADD        a8,     a7,    a8
    ST         a8,     C0,    0x0c
#else
    LD         a5,     C0,    0x00    //C0[0]
    LD         a6,     C0,    0x04    //C0[1]
    LD         a7,     C0,    0x08    //C1[2]
    LD         a8,     C0,    0x0c    //C1[3]

    MADD       a5,     c11,   ALPHA_R, a5
    MADD       a6,     c12,   ALPHA_R, a6
    NMSUB      a5,     c12,   ALPHA_I, a5
    MADD       a6,     c11,   ALPHA_I, a6
    MADD       a7,     c21,   ALPHA_R, a7
    MADD       a8,     c22,   ALPHA_R, a8
    NMSUB      a7,     c22,   ALPHA_I, a7
    MADD       a8,     c21,   ALPHA_I, a8

    ST         a5,     C0,    0x00
    ST         a6,     C0,    0x04
    ST         a7,     C0,    0x08
    ST         a8,     C0,    0x0c
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d      TL,     K,    OFF
#ifdef LEFT
    addi.d     TL,     TL,   -2
#else
    addi.d     TL,     TL,   -1
#endif
    slli.d     C3,     TL,   0x04
    add.d      A0,     A0,   C3
    slli.d     C3,     TL,   0x03
    add.d      B0,     B0,   C3
#endif

#ifdef LEFT
    addi.d     OFF,    OFF,   2
#endif
#endif   // #if defined(TRMMKERNEL)

    addi.d     C0,     C0,    0x10

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
    slli.d     C3,     OFF,   0x03
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
    LD         a2,     A0,    0x04        //load2
    MADD2      c12,    a2,    b1,     c12  //res1
    LD         b2,     B0,    0x04        //load3
    MADD3      c11,    a2,    b2,     c11
    MADD4      c12,    a1,    b2,     c12

    addi.d     A0,     A0,    0x08
    addi.d     B0,     B0,    0x08

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
    ST         a6,     C0,    0x04
#else
    LD         a5,     C0,    0x00    //C0[0]
    LD         a6,     C0,    0x04    //C0[1]

    MADD       a5,     c11,   ALPHA_R, a5
    MADD       a6,     c12,   ALPHA_R, a6
    NMSUB      a5,     c12,   ALPHA_I, a5
    MADD       a6,     c11,   ALPHA_I, a6

    ST         a5,     C0,    0x00
    ST         a6,     C0,    0x04
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d      TL,     K,    OFF
#ifdef LEFT
    addi.d     TL,     TL,   -1
#else
    addi.d     TL,     TL,   -1
#endif
    slli.d     C3,     TL,   0x03
    add.d      A0,     A0,   C3
    add.d      B0,     B0,   C3
#endif

#ifdef LEFT
    addi.d     OFF,    OFF,   1
#endif
#endif   // #if defined(TRMMKERNEL)

    addi.d     C0,     C0,    0x08

    addi.d     I,      I,     1
    blt        I,      T1,    .L25

.L28:
    slli.d     L,      K,     3
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