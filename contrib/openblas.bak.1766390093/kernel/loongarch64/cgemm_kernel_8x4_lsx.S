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
#define T2     $r26
#define T3     $r27

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

/* LSX vectors */
#define U0     $vr30
#define U1     $vr31
#define U2     $vr2
#define U3     $vr3
#define U4     $vr4
#define U5     $vr5
#define U6     $vr6
#define U7     $vr7
#define U8     $vr8
#define U9     $vr9
#define U10    $vr10
#define U11    $vr11
#define U12    $vr12
#define U13    $vr13
#define U14    $vr14
#define U15    $vr15
#define D0     $vr16
#define D1     $vr17
#define D2     $vr18
#define D3     $vr19
#define D4     $vr20
#define D5     $vr21
#define D6     $vr22
#define D7     $vr23
#define D8     $vr24
#define D9     $vr25
#define D10    $vr26
#define D11    $vr27
#define D12    $vr28
#define D13    $vr29
#define VALPHAR $vr28
#define VALPHAI $vr29


#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
#define    VMADD1       VFMADD
#define    VMADD2       VFMADD
#define    VMADD3       VNMSUB
#define    VMADD4       VFMADD

#define    MADD1       MADD
#define    MADD2       MADD
#define    MADD3       NMSUB
#define    MADD4       MADD
#endif

#if   defined(NR) || defined(NC) || defined(TR) || defined(TC)
#define    VMADD1       VFMADD
#define    VMADD2       VFMADD
#define    VMADD3       VFMADD
#define    VMADD4       VNMSUB

#define    MADD1       MADD
#define    MADD2       MADD
#define    MADD3       MADD
#define    MADD4       NMSUB
#endif

#if   defined(RN) || defined(RT) || defined(CN) || defined(CT)
#define    VMADD1       VFMADD
#define    VMADD2       VNMSUB
#define    VMADD3       VFMADD
#define    VMADD4       VFMADD

#define    MADD1       MADD
#define    MADD2       NMSUB
#define    MADD3       MADD
#define    MADD4       MADD
#endif

#if   defined(RR) || defined(RC) || defined(CR) || defined(CC)
#define    VMADD1       VFMADD
#define    VMADD2       VNMSUB
#define    VMADD3       VNMSUB
#define    VMADD4       VNMSUB

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

    vldrepl.w  VALPHAR, $sp, 112
    vldrepl.w  VALPHAI, $sp, 120

#if defined (TRMMKERNEL) && !defined(LEFT)
    sub.d      OFF,    $r0,   OFFSET
#else
    xor        OFF,    OFF,   OFF
#endif

    slli.d     LDC,    LDC,   2

    move       J,      $r0
    srai.d     T0,     N,     2  //bn/4
    beq        J,      T0,    .L19

.L10:  /* for(j=0; j<bn/4; j+=1) */
    move       C0,     C
    slli.d     TL,     LDC,   1
    add.d      C1,     C0,    TL
    add.d      C2,     C1,    TL
    add.d      C3,     C2,    TL
    move       A0,     A    //ptrba

#if defined(TRMMKERNEL) && defined(LEFT)
    move       OFF,    OFFSET
#endif

    move       I,      $r0
    srai.d     T0,     M,     3  //bm/8
    beq        I,      T0,    .L150

.L11:  /* for(i=0; i<bm/8; i+=1) */
    move       B0,     B      //ptrbb
    move       TL,     K      /* TL = bk */
#if defined(TRMMKERNEL)

#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    move       B0,     B     //ptrbb
#else
    slli.d     T3,     OFF,   0x06
    add.d      A0,     A0,    T3
    slli.d     T3,     OFF,   0x05
    add.d      B0,     B,     T3
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    sub.d      TL,     K,     OFF   //temp
#elif defined(LEFT)
    addi.d     TL,     OFF,   8
#else
    addi.d     TL,     OFF,   4
#endif

#endif  // #if defined(TRMMKERNEL)

    vxor.v    U0,     U0,   U0
    vxor.v    U1,     U1,   U1
    vxor.v    U2,     U2,   U2
    vxor.v    U3,     U3,   U3
    vxor.v    U4,     U4,   U4
    vxor.v    U5,     U5,   U5
    vxor.v    U6,     U6,   U6
    vxor.v    U7,     U7,   U7
    vxor.v    U8,     U8,   U8
    vxor.v    U9,     U9,   U9
    vxor.v    U10,    U10,  U10
    vxor.v    U11,    U11,  U11
    vxor.v    U12,    U12,  U12
    vxor.v    U13,    U13,  U13
    vxor.v    U14,    U14,  U14
    vxor.v    U15,    U15,  U15

    move       L,      $r0   //cycle param k
    beq        L,      TL,    .L13
    blt        TL,     L,     .L13

.L12:  /* for(k=0; k<temp; k+=1) */
    vld       D0,     A0,    0x00  // a0ri a1ri
    vld       D2,     B0,    0x00  // b0ri b1ri
    vld       D3,     B0,    0x10  // b2ri b3ri

    vshuf4i.w  D4,     D0,    0x00  //a0r
    vshuf4i.w  D5,     D0,    0x55  //a0i

    vpackev.w  D6,     D3,    D2
    vshuf4i.w  D6,     D6,    0xd8  //b0r b1r b2r b3r

    vpackod.w  D7,     D3,    D2
    vshuf4i.w  D7,     D7,    0xd8  //b0i b1i b2i b3i

    VMADD1    U0,     D4,    D6,     U0  //00r 10r 20r 30r
    VMADD2    U1,     D5,    D6,     U1  //00i 10i 20i 30i
    VMADD3    U0,     D5,    D7,     U0
    VMADD4    U1,     D4,    D7,     U1

    vshuf4i.w  D4,     D0,    0xaa  //a1r
    vshuf4i.w  D5,     D0,    0xff  //a1i

    VMADD1    U2,     D4,    D6,     U2  //01r 11r 21r 31r
    VMADD2    U3,     D5,    D6,     U3  //01i 11i 21i 31i
    VMADD3    U2,     D5,    D7,     U2
    VMADD4    U3,     D4,    D7,     U3

    vld       D0,     A0,    0x10  // a2ri a3ri

    vshuf4i.w  D4,     D0,    0x00  //a2r
    vshuf4i.w  D5,     D0,    0x55  //a2i

    VMADD1    U4,     D4,    D6,     U4  //02r 12r 22r 32r
    VMADD2    U5,     D5,    D6,     U5  //02i 12i 22i 32i
    VMADD3    U4,     D5,    D7,     U4
    VMADD4    U5,     D4,    D7,     U5

    vshuf4i.w  D4,     D0,    0xaa  //a3r
    vshuf4i.w  D5,     D0,    0xff  //a3i

    VMADD1    U6,     D4,    D6,     U6  //03r 13r 23r 33r
    VMADD2    U7,     D5,    D6,     U7  //03i 13i 23i 33i
    VMADD3    U6,     D5,    D7,     U6
    VMADD4    U7,     D4,    D7,     U7

    vld       D0,     A0,    0x20  // a4ri a5ri

    vshuf4i.w  D4,     D0,    0x00  //a4r
    vshuf4i.w  D5,     D0,    0x55  //a4i

    VMADD1    U8,     D4,    D6,     U8  //04r 14r 24r 34r
    VMADD2    U9,     D5,    D6,     U9  //04i 14i 24i 34i
    VMADD3    U8,     D5,    D7,     U8
    VMADD4    U9,     D4,    D7,     U9

    vshuf4i.w  D4,     D0,    0xaa  //a5r
    vshuf4i.w  D5,     D0,    0xff  //a5i

    VMADD1    U10,     D4,    D6,     U10  //05r 15r 25r 35r
    VMADD2    U11,     D5,    D6,     U11  //05i 15i 25i 35i
    VMADD3    U10,     D5,    D7,     U10
    VMADD4    U11,     D4,    D7,     U11

    vld       D0,     A0,    0x30  // a6ri a7ri

    vshuf4i.w  D4,     D0,    0x00  //a6r
    vshuf4i.w  D5,     D0,    0x55  //a6i

    VMADD1    U12,     D4,    D6,     U12  //06r 16r 26r 36r
    VMADD2    U13,     D5,    D6,     U13  //06i 16i 26i 36i
    VMADD3    U12,     D5,    D7,     U12
    VMADD4    U13,     D4,    D7,     U13

    vshuf4i.w  D4,     D0,    0xaa  //a5r
    vshuf4i.w  D5,     D0,    0xff  //a5i

    VMADD1    U14,     D4,    D6,     U14  //07r 17r 27r 37r
    VMADD2    U15,     D5,    D6,     U15  //07i 17i 27i 37i
    VMADD3    U14,     D5,    D7,     U14
    VMADD4    U15,     D4,    D7,     U15

    addi.d     A0,     A0,    0x40
    addi.d     B0,     B0,    0x20

    addi.d     L,      L,     1
    blt        L,      TL,    .L12

.L13:
#if defined(TRMMKERNEL)
    //res00 res10 res20 res30
    vld       D0,     C0,    0x00 //c0: 0 1 2 3
    vld       D1,     C1,    0x00 //c1: 0 1 2 3
    vld       D2,     C2,    0x00 //c2: 0 1 2 3
    vld       D3,     C3,    0x00 //c3: 0 1 2 3

    vand.v    D4,     D1,    D1
    vpermi.w  D4,     D0,    0x44 //c0:0 1, c1:0 1
    vshuf4i.w  D6,     D4,    0xd8 //c0[0] c1[0] c0[1] c1[1]
    vshuf4i.w  D7,     D4,    0x8d //c0[1] c1[1] c0[0] c1[0]

    vand.v    D5,     D3,    D3
    vpermi.w  D5,     D2,    0x44 //c2:0 1, c3:0 1
    vshuf4i.w  D8,     D5,    0xd8 //c2[0] c3[0] c2[1] c3[1]
    vshuf4i.w  D9,     D5,    0x8d //c2[1] c3[1] c2[0] c3[0]

    vpermi.w  D8,     D6,    0x44 //c0[0] c1[0] c2[0] c3[0]
    vpermi.w  D9,     D7,    0x44 //c0[1] c1[1] c2[1] c3[1]

    vfmul.s      D8,    U0,    VALPHAR
    vfmul.s      D9,    U1,    VALPHAR
    VNMSUB      D8,    U1,    VALPHAI, D8
    VFMADD      D9,    U0,    VALPHAI, D9

    vand.v    D10,     D9,    D9 //c0[1] c1[1] c2[1] c3[1]
    vand.v    D11,     D9,    D9 //c0[0] c1[0] c2[0] c3[0]

    vpermi.w  D10,     D8,    0x44 //c0[0] c1[0] c0[1] c1[1]
    vshuf4i.w  D10,     D10,   0xd8 //c0[0] c0[1] c1[0] c1[1]

    vpermi.w  D11,     D8,    0xee //c2[0] c3[0] c2[1] c3[1]
    vshuf4i.w  D11,     D11,   0xd8 //c2[0] c2[1] c3[0] c3[1]

    //res01 res11 res21 res31
    vand.v    D4,     D1,    D1
    vpermi.w  D4,     D0,    0xee //c0:2 3, c1:2 3
    vshuf4i.w  D6,     D4,    0xd8 //c0[2] c1[2] c0[3] c1[3]
    vshuf4i.w  D7,     D4,    0x8d //c0[3] c1[3] c0[2] c1[2]

    vand.v    D5,     D3,    D3
    vpermi.w  D5,     D2,    0xee //c2:2 3, c3:2 3
    vshuf4i.w  D8,     D5,    0xd8 //c2[2] c3[2] c2[3] c3[3]
    vshuf4i.w  D9,     D5,    0x8d //c2[3] c3[3] c2[2] c3[2]

    vpermi.w  D8,     D6,    0x44 //c0[2] c1[2] c2[2] c3[2]
    vpermi.w  D9,     D7,    0x44 //c0[3] c1[3] c2[3] c3[3]

    vfmul.s      D8,    U2,    VALPHAR
    vfmul.s      D9,    U3,    VALPHAR
    VNMSUB      D8,    U3,    VALPHAI, D8
    VFMADD      D9,    U2,    VALPHAI, D9

    vand.v    D4,     D9,    D9
    vpermi.w  D4,     D8,    0x44 //c0[2] c1[2] c0[3] c1[3]
    vshuf4i.w D4,     D4,    0xd8 //c0[2] c0[3] c1[2] c1[3]
    vand.v    D2,     D4,    D4

    vand.v    D5,     D9,    D9
    vpermi.w  D5,     D8,    0xee //c2[2] c3[2] c2[3] c3[3]
    vshuf4i.w D5,     D5,    0xd8 //c2[2] c2[3] c3[2] c3[3]
    vand.v    D3,     D5,    D5

    vand.v    D0,     D10,    D10 //c0[0] c0[1] c1[0] c1[1]
    vand.v    D1,     D11,    D11 //c2[0] c2[1] c3[0] c3[1]

    vpermi.w  D4,     D0,     0x44 //c0: 0 1 2 3
    vpermi.w  D2,     D0,     0xee //c1: 0 1 2 3
    vpermi.w  D5,     D1,     0x44 //c2: 0 1 2 3
    vpermi.w  D3,     D1,     0xee //c3: 0 1 2 3

    vst       D4,     C0,    0x00
    vst       D2,     C1,    0x00
    vst       D5,     C2,    0x00
    vst       D3,     C3,    0x00

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10
    addi.d     C2,     C2,    0x10
    addi.d     C3,     C3,    0x10

    //res02 res12 res22 res32
    vld       D0,     C0,    0x00 //c0: 0 1 2 3
    vld       D1,     C1,    0x00 //c1: 0 1 2 3
    vld       D2,     C2,    0x00 //c2: 0 1 2 3
    vld       D3,     C3,    0x00 //c3: 0 1 2 3

    vand.v    D4,     D1,    D1
    vpermi.w  D4,     D0,    0x44 //c0:0 1, c1:0 1
    vshuf4i.w  D6,     D4,    0xd8 //c0[0] c1[0] c0[1] c1[1]
    vshuf4i.w  D7,     D4,    0x8d //c0[1] c1[1] c0[0] c1[0]

    vand.v    D5,     D3,    D3
    vpermi.w  D5,     D2,    0x44 //c2:0 1, c3:0 1
    vshuf4i.w  D8,     D5,    0xd8 //c2[0] c3[0] c2[1] c3[1]
    vshuf4i.w  D9,     D5,    0x8d //c2[1] c3[1] c2[0] c3[0]

    vpermi.w  D8,     D6,    0x44 //c0[0] c1[0] c2[0] c3[0]
    vpermi.w  D9,     D7,    0x44 //c0[1] c1[1] c2[1] c3[1]

    vfmul.s      D8,    U4,    VALPHAR
    vfmul.s      D9,    U5,    VALPHAR
    VNMSUB      D8,    U5,    VALPHAI, D8
    VFMADD      D9,    U4,    VALPHAI, D9

    vand.v    D10,     D9,    D9 //c0[1] c1[1] c2[1] c3[1]
    vand.v    D11,     D9,    D9 //c0[0] c1[0] c2[0] c3[0]

    vpermi.w  D10,     D8,    0x44 //c0[0] c1[0] c0[1] c1[1]
    vshuf4i.w  D10,     D10,   0xd8 //c0[0] c0[1] c1[0] c1[1]

    vpermi.w  D11,     D8,    0xee //c2[0] c3[0] c2[1] c3[1]
    vshuf4i.w  D11,     D11,   0xd8 //c2[0] c2[1] c3[0] c3[1]

    //res03 res13 res23 res33
    vand.v    D4,     D1,    D1
    vpermi.w  D4,     D0,    0xee //c0:2 3, c1:2 3
    vshuf4i.w  D6,     D4,    0xd8 //c0[2] c1[2] c0[3] c1[3]
    vshuf4i.w  D7,     D4,    0x8d //c0[3] c1[3] c0[2] c1[2]

    vand.v    D5,     D3,    D3
    vpermi.w  D5,     D2,    0xee //c2:2 3, c3:2 3
    vshuf4i.w  D8,     D5,    0xd8 //c2[2] c3[2] c2[3] c3[3]
    vshuf4i.w  D9,     D5,    0x8d //c2[3] c3[3] c2[2] c3[2]

    vpermi.w  D8,     D6,    0x44 //c0[2] c1[2] c2[2] c3[2]
    vpermi.w  D9,     D7,    0x44 //c0[3] c1[3] c2[3] c3[3]

    vfmul.s      D8,    U6,    VALPHAR
    vfmul.s      D9,    U7,    VALPHAR
    VNMSUB      D8,    U7,    VALPHAI, D8
    VFMADD      D9,    U6,    VALPHAI, D9

    vand.v    D4,     D9,    D9
    vpermi.w  D4,     D8,    0x44 //c0[2] c1[2] c0[3] c1[3]
    vshuf4i.w D4,     D4,    0xd8 //c0[2] c0[3] c1[2] c1[3]
    vand.v    D2,     D4,    D4

    vand.v    D5,     D9,    D9
    vpermi.w  D5,     D8,    0xee //c2[2] c3[2] c2[3] c3[3]
    vshuf4i.w D5,     D5,    0xd8 //c2[2] c2[3] c3[2] c3[3]
    vand.v    D3,     D5,    D5

    vand.v    D0,     D10,    D10 //c0[0] c0[1] c1[0] c1[1]
    vand.v    D1,     D11,    D11 //c2[0] c2[1] c3[0] c3[1]

    vpermi.w  D4,     D0,     0x44 //c0: 0 1 2 3
    vpermi.w  D2,     D0,     0xee //c1: 0 1 2 3
    vpermi.w  D5,     D1,     0x44 //c2: 0 1 2 3
    vpermi.w  D3,     D1,     0xee //c3: 0 1 2 3

    vst       D4,     C0,    0x00
    vst       D2,     C1,    0x00
    vst       D5,     C2,    0x00
    vst       D3,     C3,    0x00

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10
    addi.d     C2,     C2,    0x10
    addi.d     C3,     C3,    0x10

    //res04 res14 res24 res34
    vld       D0,     C0,    0x00 //c0: 0 1 2 3
    vld       D1,     C1,    0x00 //c1: 0 1 2 3
    vld       D2,     C2,    0x00 //c2: 0 1 2 3
    vld       D3,     C3,    0x00 //c3: 0 1 2 3

    vand.v    D4,     D1,    D1
    vpermi.w  D4,     D0,    0x44 //c0:0 1, c1:0 1
    vshuf4i.w  D6,     D4,    0xd8 //c0[0] c1[0] c0[1] c1[1]
    vshuf4i.w  D7,     D4,    0x8d //c0[1] c1[1] c0[0] c1[0]

    vand.v    D5,     D3,    D3
    vpermi.w  D5,     D2,    0x44 //c2:0 1, c3:0 1
    vshuf4i.w  D8,     D5,    0xd8 //c2[0] c3[0] c2[1] c3[1]
    vshuf4i.w  D9,     D5,    0x8d //c2[1] c3[1] c2[0] c3[0]

    vpermi.w  D8,     D6,    0x44 //c0[0] c1[0] c2[0] c3[0]
    vpermi.w  D9,     D7,    0x44 //c0[1] c1[1] c2[1] c3[1]

    vfmul.s      D8,    U8,    VALPHAR
    vfmul.s      D9,    U9,    VALPHAR
    VNMSUB      D8,    U9,    VALPHAI, D8
    VFMADD      D9,    U8,    VALPHAI, D9

    vand.v    D10,     D9,    D9 //c0[1] c1[1] c2[1] c3[1]
    vand.v    D11,     D9,    D9 //c0[0] c1[0] c2[0] c3[0]

    vpermi.w  D10,     D8,    0x44 //c0[0] c1[0] c0[1] c1[1]
    vshuf4i.w  D10,     D10,   0xd8 //c0[0] c0[1] c1[0] c1[1]

    vpermi.w  D11,     D8,    0xee //c2[0] c3[0] c2[1] c3[1]
    vshuf4i.w  D11,     D11,   0xd8 //c2[0] c2[1] c3[0] c3[1]

    //res05 res15 res25 res35
    vand.v    D4,     D1,    D1
    vpermi.w  D4,     D0,    0xee //c0:2 3, c1:2 3
    vshuf4i.w  D6,     D4,    0xd8 //c0[2] c1[2] c0[3] c1[3]
    vshuf4i.w  D7,     D4,    0x8d //c0[3] c1[3] c0[2] c1[2]

    vand.v    D5,     D3,    D3
    vpermi.w  D5,     D2,    0xee //c2:2 3, c3:2 3
    vshuf4i.w  D8,     D5,    0xd8 //c2[2] c3[2] c2[3] c3[3]
    vshuf4i.w  D9,     D5,    0x8d //c2[3] c3[3] c2[2] c3[2]

    vpermi.w  D8,     D6,    0x44 //c0[2] c1[2] c2[2] c3[2]
    vpermi.w  D9,     D7,    0x44 //c0[3] c1[3] c2[3] c3[3]

    vfmul.s      D8,    U10,    VALPHAR
    vfmul.s      D9,    U11,    VALPHAR
    VNMSUB      D8,    U11,    VALPHAI, D8
    VFMADD      D9,    U10,    VALPHAI, D9

    vand.v    D4,     D9,    D9
    vpermi.w  D4,     D8,    0x44 //c0[2] c1[2] c0[3] c1[3]
    vshuf4i.w D4,     D4,    0xd8 //c0[2] c0[3] c1[2] c1[3]
    vand.v    D2,     D4,    D4

    vand.v    D5,     D9,    D9
    vpermi.w  D5,     D8,    0xee //c2[2] c3[2] c2[3] c3[3]
    vshuf4i.w D5,     D5,    0xd8 //c2[2] c2[3] c3[2] c3[3]
    vand.v    D3,     D5,    D5

    vand.v    D0,     D10,    D10 //c0[0] c0[1] c1[0] c1[1]
    vand.v    D1,     D11,    D11 //c2[0] c2[1] c3[0] c3[1]

    vpermi.w  D4,     D0,     0x44 //c0: 0 1 2 3
    vpermi.w  D2,     D0,     0xee //c1: 0 1 2 3
    vpermi.w  D5,     D1,     0x44 //c2: 0 1 2 3
    vpermi.w  D3,     D1,     0xee //c3: 0 1 2 3

    vst       D4,     C0,    0x00
    vst       D2,     C1,    0x00
    vst       D5,     C2,    0x00
    vst       D3,     C3,    0x00

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10
    addi.d     C2,     C2,    0x10
    addi.d     C3,     C3,    0x10

    //res06 res16 res26 res36
    vld       D0,     C0,    0x00 //c0: 0 1 2 3
    vld       D1,     C1,    0x00 //c1: 0 1 2 3
    vld       D2,     C2,    0x00 //c2: 0 1 2 3
    vld       D3,     C3,    0x00 //c3: 0 1 2 3

    vand.v    D4,     D1,    D1
    vpermi.w  D4,     D0,    0x44 //c0:0 1, c1:0 1
    vshuf4i.w  D6,     D4,    0xd8 //c0[0] c1[0] c0[1] c1[1]
    vshuf4i.w  D7,     D4,    0x8d //c0[1] c1[1] c0[0] c1[0]

    vand.v    D5,     D3,    D3
    vpermi.w  D5,     D2,    0x44 //c2:0 1, c3:0 1
    vshuf4i.w  D8,     D5,    0xd8 //c2[0] c3[0] c2[1] c3[1]
    vshuf4i.w  D9,     D5,    0x8d //c2[1] c3[1] c2[0] c3[0]

    vpermi.w  D8,     D6,    0x44 //c0[0] c1[0] c2[0] c3[0]
    vpermi.w  D9,     D7,    0x44 //c0[1] c1[1] c2[1] c3[1]

    vfmul.s      D8,    U12,    VALPHAR
    vfmul.s      D9,    U13,    VALPHAR
    VNMSUB      D8,    U13,    VALPHAI, D8
    VFMADD      D9,    U12,    VALPHAI, D9

    vand.v    D10,     D9,    D9 //c0[1] c1[1] c2[1] c3[1]
    vand.v    D11,     D9,    D9 //c0[0] c1[0] c2[0] c3[0]

    vpermi.w  D10,     D8,    0x44 //c0[0] c1[0] c0[1] c1[1]
    vshuf4i.w  D10,     D10,   0xd8 //c0[0] c0[1] c1[0] c1[1]

    vpermi.w  D11,     D8,    0xee //c2[0] c3[0] c2[1] c3[1]
    vshuf4i.w  D11,     D11,   0xd8 //c2[0] c2[1] c3[0] c3[1]

    //res07 res17 res27 res37
    vand.v    D4,     D1,    D1
    vpermi.w  D4,     D0,    0xee //c0:2 3, c1:2 3
    vshuf4i.w  D6,     D4,    0xd8 //c0[2] c1[2] c0[3] c1[3]
    vshuf4i.w  D7,     D4,    0x8d //c0[3] c1[3] c0[2] c1[2]

    vand.v    D5,     D3,    D3
    vpermi.w  D5,     D2,    0xee //c2:2 3, c3:2 3
    vshuf4i.w  D8,     D5,    0xd8 //c2[2] c3[2] c2[3] c3[3]
    vshuf4i.w  D9,     D5,    0x8d //c2[3] c3[3] c2[2] c3[2]

    vpermi.w  D8,     D6,    0x44 //c0[2] c1[2] c2[2] c3[2]
    vpermi.w  D9,     D7,    0x44 //c0[3] c1[3] c2[3] c3[3]

    vfmul.s      D8,    U14,    VALPHAR
    vfmul.s      D9,    U15,    VALPHAR
    VNMSUB      D8,    U15,    VALPHAI, D8
    VFMADD      D9,    U14,    VALPHAI, D9

    vand.v    D4,     D9,    D9
    vpermi.w  D4,     D8,    0x44 //c0[2] c1[2] c0[3] c1[3]
    vshuf4i.w D4,     D4,    0xd8 //c0[2] c0[3] c1[2] c1[3]
    vand.v    D2,     D4,    D4

    vand.v    D5,     D9,    D9
    vpermi.w  D5,     D8,    0xee //c2[2] c3[2] c2[3] c3[3]
    vshuf4i.w D5,     D5,    0xd8 //c2[2] c2[3] c3[2] c3[3]
    vand.v    D3,     D5,    D5

    vand.v    D0,     D10,    D10 //c0[0] c0[1] c1[0] c1[1]
    vand.v    D1,     D11,    D11 //c2[0] c2[1] c3[0] c3[1]

    vpermi.w  D4,     D0,     0x44 //c0: 0 1 2 3
    vpermi.w  D2,     D0,     0xee //c1: 0 1 2 3
    vpermi.w  D5,     D1,     0x44 //c2: 0 1 2 3
    vpermi.w  D3,     D1,     0xee //c3: 0 1 2 3

    vst       D4,     C0,    0x00
    vst       D2,     C1,    0x00
    vst       D5,     C2,    0x00
    vst       D3,     C3,    0x00

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10
    addi.d     C2,     C2,    0x10
    addi.d     C3,     C3,    0x10
#else
    //res00 res10 res20 res30
    vld       D0,     C0,    0x00 //c0: 0 1 2 3
    vld       D1,     C1,    0x00 //c1: 0 1 2 3
    vld       D2,     C2,    0x00 //c2: 0 1 2 3
    vld       D3,     C3,    0x00 //c3: 0 1 2 3

    vand.v    D4,     D1,    D1
    vpermi.w  D4,     D0,    0x44 //c0:0 1, c1:0 1
    vshuf4i.w  D6,     D4,    0xd8 //c0[0] c1[0] c0[1] c1[1]
    vshuf4i.w  D7,     D4,    0x8d //c0[1] c1[1] c0[0] c1[0]

    vand.v    D5,     D3,    D3
    vpermi.w  D5,     D2,    0x44 //c2:0 1, c3:0 1
    vshuf4i.w  D8,     D5,    0xd8 //c2[0] c3[0] c2[1] c3[1]
    vshuf4i.w  D9,     D5,    0x8d //c2[1] c3[1] c2[0] c3[0]

    vpermi.w  D8,     D6,    0x44 //c0[0] c1[0] c2[0] c3[0]
    vpermi.w  D9,     D7,    0x44 //c0[1] c1[1] c2[1] c3[1]

    VFMADD      D8,    U0,    VALPHAR, D8
    VFMADD      D9,    U1,    VALPHAR, D9
    VNMSUB      D8,    U1,    VALPHAI, D8
    VFMADD      D9,    U0,    VALPHAI, D9

    vand.v    D10,     D9,    D9 //c0[1] c1[1] c2[1] c3[1]
    vand.v    D11,     D9,    D9 //c0[0] c1[0] c2[0] c3[0]

    vpermi.w  D10,     D8,    0x44 //c0[0] c1[0] c0[1] c1[1]
    vshuf4i.w  D10,     D10,   0xd8 //c0[0] c0[1] c1[0] c1[1]

    vpermi.w  D11,     D8,    0xee //c2[0] c3[0] c2[1] c3[1]
    vshuf4i.w  D11,     D11,   0xd8 //c2[0] c2[1] c3[0] c3[1]

    //res01 res11 res21 res31
    vand.v    D4,     D1,    D1
    vpermi.w  D4,     D0,    0xee //c0:2 3, c1:2 3
    vshuf4i.w  D6,     D4,    0xd8 //c0[2] c1[2] c0[3] c1[3]
    vshuf4i.w  D7,     D4,    0x8d //c0[3] c1[3] c0[2] c1[2]

    vand.v    D5,     D3,    D3
    vpermi.w  D5,     D2,    0xee //c2:2 3, c3:2 3
    vshuf4i.w  D8,     D5,    0xd8 //c2[2] c3[2] c2[3] c3[3]
    vshuf4i.w  D9,     D5,    0x8d //c2[3] c3[3] c2[2] c3[2]

    vpermi.w  D8,     D6,    0x44 //c0[2] c1[2] c2[2] c3[2]
    vpermi.w  D9,     D7,    0x44 //c0[3] c1[3] c2[3] c3[3]

    VFMADD      D8,    U2,    VALPHAR, D8
    VFMADD      D9,    U3,    VALPHAR, D9
    VNMSUB      D8,    U3,    VALPHAI, D8
    VFMADD      D9,    U2,    VALPHAI, D9

    vand.v    D4,     D9,    D9
    vpermi.w  D4,     D8,    0x44 //c0[2] c1[2] c0[3] c1[3]
    vshuf4i.w D4,     D4,    0xd8 //c0[2] c0[3] c1[2] c1[3]
    vand.v    D2,     D4,    D4

    vand.v    D5,     D9,    D9
    vpermi.w  D5,     D8,    0xee //c2[2] c3[2] c2[3] c3[3]
    vshuf4i.w D5,     D5,    0xd8 //c2[2] c2[3] c3[2] c3[3]
    vand.v    D3,     D5,    D5

    vand.v    D0,     D10,    D10 //c0[0] c0[1] c1[0] c1[1]
    vand.v    D1,     D11,    D11 //c2[0] c2[1] c3[0] c3[1]

    vpermi.w  D4,     D0,     0x44 //c0: 0 1 2 3
    vpermi.w  D2,     D0,     0xee //c1: 0 1 2 3
    vpermi.w  D5,     D1,     0x44 //c2: 0 1 2 3
    vpermi.w  D3,     D1,     0xee //c3: 0 1 2 3

    vst       D4,     C0,    0x00
    vst       D2,     C1,    0x00
    vst       D5,     C2,    0x00
    vst       D3,     C3,    0x00

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10
    addi.d     C2,     C2,    0x10
    addi.d     C3,     C3,    0x10

    //res02 res12 res22 res32
    vld       D0,     C0,    0x00 //c0: 0 1 2 3
    vld       D1,     C1,    0x00 //c1: 0 1 2 3
    vld       D2,     C2,    0x00 //c2: 0 1 2 3
    vld       D3,     C3,    0x00 //c3: 0 1 2 3

    vand.v    D4,     D1,    D1
    vpermi.w  D4,     D0,    0x44 //c0:0 1, c1:0 1
    vshuf4i.w  D6,     D4,    0xd8 //c0[0] c1[0] c0[1] c1[1]
    vshuf4i.w  D7,     D4,    0x8d //c0[1] c1[1] c0[0] c1[0]

    vand.v    D5,     D3,    D3
    vpermi.w  D5,     D2,    0x44 //c2:0 1, c3:0 1
    vshuf4i.w  D8,     D5,    0xd8 //c2[0] c3[0] c2[1] c3[1]
    vshuf4i.w  D9,     D5,    0x8d //c2[1] c3[1] c2[0] c3[0]

    vpermi.w  D8,     D6,    0x44 //c0[0] c1[0] c2[0] c3[0]
    vpermi.w  D9,     D7,    0x44 //c0[1] c1[1] c2[1] c3[1]

    VFMADD      D8,    U4,    VALPHAR, D8
    VFMADD      D9,    U5,    VALPHAR, D9
    VNMSUB      D8,    U5,    VALPHAI, D8
    VFMADD      D9,    U4,    VALPHAI, D9

    vand.v    D10,     D9,    D9 //c0[1] c1[1] c2[1] c3[1]
    vand.v    D11,     D9,    D9 //c0[0] c1[0] c2[0] c3[0]

    vpermi.w  D10,     D8,    0x44 //c0[0] c1[0] c0[1] c1[1]
    vshuf4i.w  D10,     D10,   0xd8 //c0[0] c0[1] c1[0] c1[1]

    vpermi.w  D11,     D8,    0xee //c2[0] c3[0] c2[1] c3[1]
    vshuf4i.w  D11,     D11,   0xd8 //c2[0] c2[1] c3[0] c3[1]

    //res03 res13 res23 res33
    vand.v    D4,     D1,    D1
    vpermi.w  D4,     D0,    0xee //c0:2 3, c1:2 3
    vshuf4i.w  D6,     D4,    0xd8 //c0[2] c1[2] c0[3] c1[3]
    vshuf4i.w  D7,     D4,    0x8d //c0[3] c1[3] c0[2] c1[2]

    vand.v    D5,     D3,    D3
    vpermi.w  D5,     D2,    0xee //c2:2 3, c3:2 3
    vshuf4i.w  D8,     D5,    0xd8 //c2[2] c3[2] c2[3] c3[3]
    vshuf4i.w  D9,     D5,    0x8d //c2[3] c3[3] c2[2] c3[2]

    vpermi.w  D8,     D6,    0x44 //c0[2] c1[2] c2[2] c3[2]
    vpermi.w  D9,     D7,    0x44 //c0[3] c1[3] c2[3] c3[3]

    VFMADD      D8,    U6,    VALPHAR, D8
    VFMADD      D9,    U7,    VALPHAR, D9
    VNMSUB      D8,    U7,    VALPHAI, D8
    VFMADD      D9,    U6,    VALPHAI, D9

    vand.v    D4,     D9,    D9
    vpermi.w  D4,     D8,    0x44 //c0[2] c1[2] c0[3] c1[3]
    vshuf4i.w D4,     D4,    0xd8 //c0[2] c0[3] c1[2] c1[3]
    vand.v    D2,     D4,    D4

    vand.v    D5,     D9,    D9
    vpermi.w  D5,     D8,    0xee //c2[2] c3[2] c2[3] c3[3]
    vshuf4i.w D5,     D5,    0xd8 //c2[2] c2[3] c3[2] c3[3]
    vand.v    D3,     D5,    D5

    vand.v    D0,     D10,    D10 //c0[0] c0[1] c1[0] c1[1]
    vand.v    D1,     D11,    D11 //c2[0] c2[1] c3[0] c3[1]

    vpermi.w  D4,     D0,     0x44 //c0: 0 1 2 3
    vpermi.w  D2,     D0,     0xee //c1: 0 1 2 3
    vpermi.w  D5,     D1,     0x44 //c2: 0 1 2 3
    vpermi.w  D3,     D1,     0xee //c3: 0 1 2 3

    vst       D4,     C0,    0x00
    vst       D2,     C1,    0x00
    vst       D5,     C2,    0x00
    vst       D3,     C3,    0x00

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10
    addi.d     C2,     C2,    0x10
    addi.d     C3,     C3,    0x10

    //res04 res14 res24 res34
    vld       D0,     C0,    0x00 //c0: 0 1 2 3
    vld       D1,     C1,    0x00 //c1: 0 1 2 3
    vld       D2,     C2,    0x00 //c2: 0 1 2 3
    vld       D3,     C3,    0x00 //c3: 0 1 2 3

    vand.v    D4,     D1,    D1
    vpermi.w  D4,     D0,    0x44 //c0:0 1, c1:0 1
    vshuf4i.w  D6,     D4,    0xd8 //c0[0] c1[0] c0[1] c1[1]
    vshuf4i.w  D7,     D4,    0x8d //c0[1] c1[1] c0[0] c1[0]

    vand.v    D5,     D3,    D3
    vpermi.w  D5,     D2,    0x44 //c2:0 1, c3:0 1
    vshuf4i.w  D8,     D5,    0xd8 //c2[0] c3[0] c2[1] c3[1]
    vshuf4i.w  D9,     D5,    0x8d //c2[1] c3[1] c2[0] c3[0]

    vpermi.w  D8,     D6,    0x44 //c0[0] c1[0] c2[0] c3[0]
    vpermi.w  D9,     D7,    0x44 //c0[1] c1[1] c2[1] c3[1]

    VFMADD      D8,    U8,    VALPHAR, D8
    VFMADD      D9,    U9,    VALPHAR, D9
    VNMSUB      D8,    U9,    VALPHAI, D8
    VFMADD      D9,    U8,    VALPHAI, D9

    vand.v    D10,     D9,    D9 //c0[1] c1[1] c2[1] c3[1]
    vand.v    D11,     D9,    D9 //c0[0] c1[0] c2[0] c3[0]

    vpermi.w  D10,     D8,    0x44 //c0[0] c1[0] c0[1] c1[1]
    vshuf4i.w  D10,     D10,   0xd8 //c0[0] c0[1] c1[0] c1[1]

    vpermi.w  D11,     D8,    0xee //c2[0] c3[0] c2[1] c3[1]
    vshuf4i.w  D11,     D11,   0xd8 //c2[0] c2[1] c3[0] c3[1]

    //res05 res15 res25 res35
    vand.v    D4,     D1,    D1
    vpermi.w  D4,     D0,    0xee //c0:2 3, c1:2 3
    vshuf4i.w  D6,     D4,    0xd8 //c0[2] c1[2] c0[3] c1[3]
    vshuf4i.w  D7,     D4,    0x8d //c0[3] c1[3] c0[2] c1[2]

    vand.v    D5,     D3,    D3
    vpermi.w  D5,     D2,    0xee //c2:2 3, c3:2 3
    vshuf4i.w  D8,     D5,    0xd8 //c2[2] c3[2] c2[3] c3[3]
    vshuf4i.w  D9,     D5,    0x8d //c2[3] c3[3] c2[2] c3[2]

    vpermi.w  D8,     D6,    0x44 //c0[2] c1[2] c2[2] c3[2]
    vpermi.w  D9,     D7,    0x44 //c0[3] c1[3] c2[3] c3[3]

    VFMADD      D8,    U10,    VALPHAR, D8
    VFMADD      D9,    U11,    VALPHAR, D9
    VNMSUB      D8,    U11,    VALPHAI, D8
    VFMADD      D9,    U10,    VALPHAI, D9

    vand.v    D4,     D9,    D9
    vpermi.w  D4,     D8,    0x44 //c0[2] c1[2] c0[3] c1[3]
    vshuf4i.w D4,     D4,    0xd8 //c0[2] c0[3] c1[2] c1[3]
    vand.v    D2,     D4,    D4

    vand.v    D5,     D9,    D9
    vpermi.w  D5,     D8,    0xee //c2[2] c3[2] c2[3] c3[3]
    vshuf4i.w D5,     D5,    0xd8 //c2[2] c2[3] c3[2] c3[3]
    vand.v    D3,     D5,    D5

    vand.v    D0,     D10,    D10 //c0[0] c0[1] c1[0] c1[1]
    vand.v    D1,     D11,    D11 //c2[0] c2[1] c3[0] c3[1]

    vpermi.w  D4,     D0,     0x44 //c0: 0 1 2 3
    vpermi.w  D2,     D0,     0xee //c1: 0 1 2 3
    vpermi.w  D5,     D1,     0x44 //c2: 0 1 2 3
    vpermi.w  D3,     D1,     0xee //c3: 0 1 2 3

    vst       D4,     C0,    0x00
    vst       D2,     C1,    0x00
    vst       D5,     C2,    0x00
    vst       D3,     C3,    0x00

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10
    addi.d     C2,     C2,    0x10
    addi.d     C3,     C3,    0x10

    //res06 res16 res26 res36
    vld       D0,     C0,    0x00 //c0: 0 1 2 3
    vld       D1,     C1,    0x00 //c1: 0 1 2 3
    vld       D2,     C2,    0x00 //c2: 0 1 2 3
    vld       D3,     C3,    0x00 //c3: 0 1 2 3

    vand.v    D4,     D1,    D1
    vpermi.w  D4,     D0,    0x44 //c0:0 1, c1:0 1
    vshuf4i.w  D6,     D4,    0xd8 //c0[0] c1[0] c0[1] c1[1]
    vshuf4i.w  D7,     D4,    0x8d //c0[1] c1[1] c0[0] c1[0]

    vand.v    D5,     D3,    D3
    vpermi.w  D5,     D2,    0x44 //c2:0 1, c3:0 1
    vshuf4i.w  D8,     D5,    0xd8 //c2[0] c3[0] c2[1] c3[1]
    vshuf4i.w  D9,     D5,    0x8d //c2[1] c3[1] c2[0] c3[0]

    vpermi.w  D8,     D6,    0x44 //c0[0] c1[0] c2[0] c3[0]
    vpermi.w  D9,     D7,    0x44 //c0[1] c1[1] c2[1] c3[1]

    VFMADD      D8,    U12,    VALPHAR, D8
    VFMADD      D9,    U13,    VALPHAR, D9
    VNMSUB      D8,    U13,    VALPHAI, D8
    VFMADD      D9,    U12,    VALPHAI, D9

    vand.v    D10,     D9,    D9 //c0[1] c1[1] c2[1] c3[1]
    vand.v    D11,     D9,    D9 //c0[0] c1[0] c2[0] c3[0]

    vpermi.w  D10,     D8,    0x44 //c0[0] c1[0] c0[1] c1[1]
    vshuf4i.w  D10,     D10,   0xd8 //c0[0] c0[1] c1[0] c1[1]

    vpermi.w  D11,     D8,    0xee //c2[0] c3[0] c2[1] c3[1]
    vshuf4i.w  D11,     D11,   0xd8 //c2[0] c2[1] c3[0] c3[1]

    //res07 res17 res27 res37
    vand.v    D4,     D1,    D1
    vpermi.w  D4,     D0,    0xee //c0:2 3, c1:2 3
    vshuf4i.w  D6,     D4,    0xd8 //c0[2] c1[2] c0[3] c1[3]
    vshuf4i.w  D7,     D4,    0x8d //c0[3] c1[3] c0[2] c1[2]

    vand.v    D5,     D3,    D3
    vpermi.w  D5,     D2,    0xee //c2:2 3, c3:2 3
    vshuf4i.w  D8,     D5,    0xd8 //c2[2] c3[2] c2[3] c3[3]
    vshuf4i.w  D9,     D5,    0x8d //c2[3] c3[3] c2[2] c3[2]

    vpermi.w  D8,     D6,    0x44 //c0[2] c1[2] c2[2] c3[2]
    vpermi.w  D9,     D7,    0x44 //c0[3] c1[3] c2[3] c3[3]

    VFMADD      D8,    U14,    VALPHAR, D8
    VFMADD      D9,    U15,    VALPHAR, D9
    VNMSUB      D8,    U15,    VALPHAI, D8
    VFMADD      D9,    U14,    VALPHAI, D9

    vand.v    D4,     D9,    D9
    vpermi.w  D4,     D8,    0x44 //c0[2] c1[2] c0[3] c1[3]
    vshuf4i.w D4,     D4,    0xd8 //c0[2] c0[3] c1[2] c1[3]
    vand.v    D2,     D4,    D4

    vand.v    D5,     D9,    D9
    vpermi.w  D5,     D8,    0xee //c2[2] c3[2] c2[3] c3[3]
    vshuf4i.w D5,     D5,    0xd8 //c2[2] c2[3] c3[2] c3[3]
    vand.v    D3,     D5,    D5

    vand.v    D0,     D10,    D10 //c0[0] c0[1] c1[0] c1[1]
    vand.v    D1,     D11,    D11 //c2[0] c2[1] c3[0] c3[1]

    vpermi.w  D4,     D0,     0x44 //c0: 0 1 2 3
    vpermi.w  D2,     D0,     0xee //c1: 0 1 2 3
    vpermi.w  D5,     D1,     0x44 //c2: 0 1 2 3
    vpermi.w  D3,     D1,     0xee //c3: 0 1 2 3

    vst       D4,     C0,    0x00
    vst       D2,     C1,    0x00
    vst       D5,     C2,    0x00
    vst       D3,     C3,    0x00

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10
    addi.d     C2,     C2,    0x10
    addi.d     C3,     C3,    0x10
#endif

#if defined(TRMMKERNEL)

#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d      TL,     K,    OFF
#ifdef LEFT
    addi.d     TL,     TL,   -8
#else
    addi.d     TL,     TL,   -4
#endif
    slli.d     T3,     TL,   0x06
    add.d      A0,     A0,   T3
    slli.d     T3,     TL,   0x05
    add.d      B0,     B0,   T3
#endif

#ifdef LEFT
    addi.d     OFF,    OFF,   8
#endif

#endif   // #if defined(TRMMKERNEL)

    addi.d     I,      I,     1
    blt        I,      T0,    .L11

.L150:
    move       I,      $r0
    andi       T0,     M,     4
    beq        I,      T0,    .L18

.L15:  /* if (bm & 4) */
    move       B0,     B      //ptrbb
    move       TL,     K      /* TL = bk */
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) ||(!defined(LEFT) && !defined(TRANSA))
    move       B0,     B    //ptrbb
#else
    slli.d     T3,     OFF,  0x05
    add.d      A0,     A0,   T3
    slli.d     T3,     OFF,  0x05
    add.d      B0,     B,    T3
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    sub.d      TL,     K,     OFF
#elif defined(LEFT)
    addi.d     TL,     OFF,   4
#else
    addi.d     TL,     OFF,   4
#endif

#endif  // #if defined(TRMMKERNEL)

    vxor.v    U0,     U0,   U0
    vxor.v    U1,     U1,   U1
    vxor.v    U2,     U2,   U2
    vxor.v    U3,     U3,   U3
    vxor.v    U4,     U4,   U4
    vxor.v    U5,     U5,   U5
    vxor.v    U6,     U6,   U6
    vxor.v    U7,     U7,   U7

    move       L,      $r0   //cycle param k
    beq        L,      TL,    .L17
    blt        TL,     L,     .L17

.L16:  /* for (k=0; k<temp; k++) */
    vld       D0,     A0,    0x00  // a0ri a1ri
    vld       D2,     B0,    0x00  // b0ri b1ri
    vld       D3,     B0,    0x10  // b2ri b3ri

    vshuf4i.w  D4,     D0,    0x00  //a0r
    vshuf4i.w  D5,     D0,    0x55  //a0i

    vpackev.w  D6,     D3,    D2
    vshuf4i.w  D6,     D6,    0xd8  //b0r b1r b2r b3r

    vpackod.w  D7,     D3,    D2
    vshuf4i.w  D7,     D7,    0xd8  //b0i b1i b2i b3i

    VMADD1    U0,     D4,    D6,     U0  //00r 10r 20r 30r
    VMADD2    U1,     D5,    D6,     U1  //00i 10i 20i 30i
    VMADD3    U0,     D5,    D7,     U0
    VMADD4    U1,     D4,    D7,     U1

    vshuf4i.w  D4,     D0,    0xaa  //a1r
    vshuf4i.w  D5,     D0,    0xff  //a1i

    VMADD1    U2,     D4,    D6,     U2  //01r 11r 21r 31r
    VMADD2    U3,     D5,    D6,     U3  //01i 11i 21i 31i
    VMADD3    U2,     D5,    D7,     U2
    VMADD4    U3,     D4,    D7,     U3

    vld       D0,     A0,    0x10  // a2ri a3ri

    vshuf4i.w  D4,     D0,    0x00  //a2r
    vshuf4i.w  D5,     D0,    0x55  //a2i

    VMADD1    U4,     D4,    D6,     U4  //02r 12r 22r 32r
    VMADD2    U5,     D5,    D6,     U5  //02i 12i 22i 32i
    VMADD3    U4,     D5,    D7,     U4
    VMADD4    U5,     D4,    D7,     U5

    vshuf4i.w  D4,     D0,    0xaa  //a3r
    vshuf4i.w  D5,     D0,    0xff  //a3i

    VMADD1    U6,     D4,    D6,     U6  //03r 13r 23r 33r
    VMADD2    U7,     D5,    D6,     U7  //03i 13i 23i 33i
    VMADD3    U6,     D5,    D7,     U6
    VMADD4    U7,     D4,    D7,     U7

    addi.d     A0,     A0,    0x20
    addi.d     B0,     B0,    0x20

    addi.d     L,      L,     1
    blt        L,      TL,     .L16

.L17:
#if defined(TRMMKERNEL)
    //res00 res10 res20 res30
    vld       D0,     C0,    0x00 //c0: 0 1 2 3
    vld       D1,     C1,    0x00 //c1: 0 1 2 3
    vld       D2,     C2,    0x00 //c2: 0 1 2 3
    vld       D3,     C3,    0x00 //c3: 0 1 2 3

    vand.v    D4,     D1,    D1
    vpermi.w  D4,     D0,    0x44 //c0:0 1, c1:0 1
    vshuf4i.w  D6,     D4,    0xd8 //c0[0] c1[0] c0[1] c1[1]
    vshuf4i.w  D7,     D4,    0x8d //c0[1] c1[1] c0[0] c1[0]

    vand.v    D5,     D3,    D3
    vpermi.w  D5,     D2,    0x44 //c2:0 1, c3:0 1
    vshuf4i.w  D8,     D5,    0xd8 //c2[0] c3[0] c2[1] c3[1]
    vshuf4i.w  D9,     D5,    0x8d //c2[1] c3[1] c2[0] c3[0]

    vpermi.w  D8,     D6,    0x44 //c0[0] c1[0] c2[0] c3[0]
    vpermi.w  D9,     D7,    0x44 //c0[1] c1[1] c2[1] c3[1]

    vfmul.s      D8,    U0,    VALPHAR
    vfmul.s      D9,    U1,    VALPHAR
    VNMSUB      D8,    U1,    VALPHAI, D8
    VFMADD      D9,    U0,    VALPHAI, D9

    vand.v    D10,     D9,    D9 //c0[1] c1[1] c2[1] c3[1]
    vand.v    D11,     D9,    D9 //c0[0] c1[0] c2[0] c3[0]

    vpermi.w  D10,     D8,    0x44 //c0[0] c1[0] c0[1] c1[1]
    vshuf4i.w  D10,     D10,   0xd8 //c0[0] c0[1] c1[0] c1[1]

    vpermi.w  D11,     D8,    0xee //c2[0] c3[0] c2[1] c3[1]
    vshuf4i.w  D11,     D11,   0xd8 //c2[0] c2[1] c3[0] c3[1]

    //res01 res11 res21 res31
    vand.v    D4,     D1,    D1
    vpermi.w  D4,     D0,    0xee //c0:2 3, c1:2 3
    vshuf4i.w  D6,     D4,    0xd8 //c0[2] c1[2] c0[3] c1[3]
    vshuf4i.w  D7,     D4,    0x8d //c0[3] c1[3] c0[2] c1[2]

    vand.v    D5,     D3,    D3
    vpermi.w  D5,     D2,    0xee //c2:2 3, c3:2 3
    vshuf4i.w  D8,     D5,    0xd8 //c2[2] c3[2] c2[3] c3[3]
    vshuf4i.w  D9,     D5,    0x8d //c2[3] c3[3] c2[2] c3[2]

    vpermi.w  D8,     D6,    0x44 //c0[2] c1[2] c2[2] c3[2]
    vpermi.w  D9,     D7,    0x44 //c0[3] c1[3] c2[3] c3[3]

    vfmul.s      D8,    U2,    VALPHAR
    vfmul.s      D9,    U3,    VALPHAR
    VNMSUB      D8,    U3,    VALPHAI, D8
    VFMADD      D9,    U2,    VALPHAI, D9

    vand.v    D4,     D9,    D9
    vpermi.w  D4,     D8,    0x44 //c0[2] c1[2] c0[3] c1[3]
    vshuf4i.w D4,     D4,    0xd8 //c0[2] c0[3] c1[2] c1[3]
    vand.v    D2,     D4,    D4

    vand.v    D5,     D9,    D9
    vpermi.w  D5,     D8,    0xee //c2[2] c3[2] c2[3] c3[3]
    vshuf4i.w D5,     D5,    0xd8 //c2[2] c2[3] c3[2] c3[3]
    vand.v    D3,     D5,    D5

    vand.v    D0,     D10,    D10 //c0[0] c0[1] c1[0] c1[1]
    vand.v    D1,     D11,    D11 //c2[0] c2[1] c3[0] c3[1]

    vpermi.w  D4,     D0,     0x44 //c0: 0 1 2 3
    vpermi.w  D2,     D0,     0xee //c1: 0 1 2 3
    vpermi.w  D5,     D1,     0x44 //c2: 0 1 2 3
    vpermi.w  D3,     D1,     0xee //c3: 0 1 2 3

    vst       D4,     C0,    0x00
    vst       D2,     C1,    0x00
    vst       D5,     C2,    0x00
    vst       D3,     C3,    0x00

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10
    addi.d     C2,     C2,    0x10
    addi.d     C3,     C3,    0x10

    //res02 res12 res22 res32
    vld       D0,     C0,    0x00 //c0: 0 1 2 3
    vld       D1,     C1,    0x00 //c1: 0 1 2 3
    vld       D2,     C2,    0x00 //c2: 0 1 2 3
    vld       D3,     C3,    0x00 //c3: 0 1 2 3

    vand.v    D4,     D1,    D1
    vpermi.w  D4,     D0,    0x44 //c0:0 1, c1:0 1
    vshuf4i.w  D6,     D4,    0xd8 //c0[0] c1[0] c0[1] c1[1]
    vshuf4i.w  D7,     D4,    0x8d //c0[1] c1[1] c0[0] c1[0]

    vand.v    D5,     D3,    D3
    vpermi.w  D5,     D2,    0x44 //c2:0 1, c3:0 1
    vshuf4i.w  D8,     D5,    0xd8 //c2[0] c3[0] c2[1] c3[1]
    vshuf4i.w  D9,     D5,    0x8d //c2[1] c3[1] c2[0] c3[0]

    vpermi.w  D8,     D6,    0x44 //c0[0] c1[0] c2[0] c3[0]
    vpermi.w  D9,     D7,    0x44 //c0[1] c1[1] c2[1] c3[1]

    vfmul.s      D8,    U4,    VALPHAR
    vfmul.s      D9,    U5,    VALPHAR
    VNMSUB      D8,    U5,    VALPHAI, D8
    VFMADD      D9,    U4,    VALPHAI, D9

    vand.v    D10,     D9,    D9 //c0[1] c1[1] c2[1] c3[1]
    vand.v    D11,     D9,    D9 //c0[0] c1[0] c2[0] c3[0]

    vpermi.w  D10,     D8,    0x44 //c0[0] c1[0] c0[1] c1[1]
    vshuf4i.w  D10,     D10,   0xd8 //c0[0] c0[1] c1[0] c1[1]

    vpermi.w  D11,     D8,    0xee //c2[0] c3[0] c2[1] c3[1]
    vshuf4i.w  D11,     D11,   0xd8 //c2[0] c2[1] c3[0] c3[1]

    //res03 res13 res23 res33
    vand.v    D4,     D1,    D1
    vpermi.w  D4,     D0,    0xee //c0:2 3, c1:2 3
    vshuf4i.w  D6,     D4,    0xd8 //c0[2] c1[2] c0[3] c1[3]
    vshuf4i.w  D7,     D4,    0x8d //c0[3] c1[3] c0[2] c1[2]

    vand.v    D5,     D3,    D3
    vpermi.w  D5,     D2,    0xee //c2:2 3, c3:2 3
    vshuf4i.w  D8,     D5,    0xd8 //c2[2] c3[2] c2[3] c3[3]
    vshuf4i.w  D9,     D5,    0x8d //c2[3] c3[3] c2[2] c3[2]

    vpermi.w  D8,     D6,    0x44 //c0[2] c1[2] c2[2] c3[2]
    vpermi.w  D9,     D7,    0x44 //c0[3] c1[3] c2[3] c3[3]

    vfmul.s      D8,    U6,    VALPHAR
    vfmul.s      D9,    U7,    VALPHAR
    VNMSUB      D8,    U7,    VALPHAI, D8
    VFMADD      D9,    U6,    VALPHAI, D9

    vand.v    D4,     D9,    D9
    vpermi.w  D4,     D8,    0x44 //c0[2] c1[2] c0[3] c1[3]
    vshuf4i.w D4,     D4,    0xd8 //c0[2] c0[3] c1[2] c1[3]
    vand.v    D2,     D4,    D4

    vand.v    D5,     D9,    D9
    vpermi.w  D5,     D8,    0xee //c2[2] c3[2] c2[3] c3[3]
    vshuf4i.w D5,     D5,    0xd8 //c2[2] c2[3] c3[2] c3[3]
    vand.v    D3,     D5,    D5

    vand.v    D0,     D10,    D10 //c0[0] c0[1] c1[0] c1[1]
    vand.v    D1,     D11,    D11 //c2[0] c2[1] c3[0] c3[1]

    vpermi.w  D4,     D0,     0x44 //c0: 0 1 2 3
    vpermi.w  D2,     D0,     0xee //c1: 0 1 2 3
    vpermi.w  D5,     D1,     0x44 //c2: 0 1 2 3
    vpermi.w  D3,     D1,     0xee //c3: 0 1 2 3

    vst       D4,     C0,    0x00
    vst       D2,     C1,    0x00
    vst       D5,     C2,    0x00
    vst       D3,     C3,    0x00

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10
    addi.d     C2,     C2,    0x10
    addi.d     C3,     C3,    0x10
#else
    //res00 res10 res20 res30
    vld       D0,     C0,    0x00 //c0: 0 1 2 3
    vld       D1,     C1,    0x00 //c1: 0 1 2 3
    vld       D2,     C2,    0x00 //c2: 0 1 2 3
    vld       D3,     C3,    0x00 //c3: 0 1 2 3

    vand.v    D4,     D1,    D1
    vpermi.w  D4,     D0,    0x44 //c0:0 1, c1:0 1
    vshuf4i.w  D6,     D4,    0xd8 //c0[0] c1[0] c0[1] c1[1]
    vshuf4i.w  D7,     D4,    0x8d //c0[1] c1[1] c0[0] c1[0]

    vand.v    D5,     D3,    D3
    vpermi.w  D5,     D2,    0x44 //c2:0 1, c3:0 1
    vshuf4i.w  D8,     D5,    0xd8 //c2[0] c3[0] c2[1] c3[1]
    vshuf4i.w  D9,     D5,    0x8d //c2[1] c3[1] c2[0] c3[0]

    vpermi.w  D8,     D6,    0x44 //c0[0] c1[0] c2[0] c3[0]
    vpermi.w  D9,     D7,    0x44 //c0[1] c1[1] c2[1] c3[1]

    VFMADD      D8,    U0,    VALPHAR, D8
    VFMADD      D9,    U1,    VALPHAR, D9
    VNMSUB      D8,    U1,    VALPHAI, D8
    VFMADD      D9,    U0,    VALPHAI, D9

    vst       VALPHAR,     C0,    0x00
    vst       VALPHAI,     C1,    0x00

    vand.v    D10,     D9,    D9 //c0[1] c1[1] c2[1] c3[1]
    vand.v    D11,     D9,    D9 //c0[0] c1[0] c2[0] c3[0]

    vpermi.w  D10,     D8,    0x44 //c0[0] c1[0] c0[1] c1[1]
    vshuf4i.w  D10,     D10,   0xd8 //c0[0] c0[1] c1[0] c1[1]

    vpermi.w  D11,     D8,    0xee //c2[0] c3[0] c2[1] c3[1]
    vshuf4i.w  D11,     D11,   0xd8 //c2[0] c2[1] c3[0] c3[1]

    //res01 res11 res21 res31
    vand.v    D4,     D1,    D1
    vpermi.w  D4,     D0,    0xee //c0:2 3, c1:2 3
    vshuf4i.w  D6,     D4,    0xd8 //c0[2] c1[2] c0[3] c1[3]
    vshuf4i.w  D7,     D4,    0x8d //c0[3] c1[3] c0[2] c1[2]

    vand.v    D5,     D3,    D3
    vpermi.w  D5,     D2,    0xee //c2:2 3, c3:2 3
    vshuf4i.w  D8,     D5,    0xd8 //c2[2] c3[2] c2[3] c3[3]
    vshuf4i.w  D9,     D5,    0x8d //c2[3] c3[3] c2[2] c3[2]

    vpermi.w  D8,     D6,    0x44 //c0[2] c1[2] c2[2] c3[2]
    vpermi.w  D9,     D7,    0x44 //c0[3] c1[3] c2[3] c3[3]

    VFMADD      D8,    U2,    VALPHAR, D8
    VFMADD      D9,    U3,    VALPHAR, D9
    VNMSUB      D8,    U3,    VALPHAI, D8
    VFMADD      D9,    U2,    VALPHAI, D9

    vand.v    D4,     D9,    D9
    vpermi.w  D4,     D8,    0x44 //c0[2] c1[2] c0[3] c1[3]
    vshuf4i.w D4,     D4,    0xd8 //c0[2] c0[3] c1[2] c1[3]
    vand.v    D2,     D4,    D4

    vand.v    D5,     D9,    D9
    vpermi.w  D5,     D8,    0xee //c2[2] c3[2] c2[3] c3[3]
    vshuf4i.w D5,     D5,    0xd8 //c2[2] c2[3] c3[2] c3[3]
    vand.v    D3,     D5,    D5

    vand.v    D0,     D10,    D10 //c0[0] c0[1] c1[0] c1[1]
    vand.v    D1,     D11,    D11 //c2[0] c2[1] c3[0] c3[1]

    vpermi.w  D4,     D0,     0x44 //c0: 0 1 2 3
    vpermi.w  D2,     D0,     0xee //c1: 0 1 2 3
    vpermi.w  D5,     D1,     0x44 //c2: 0 1 2 3
    vpermi.w  D3,     D1,     0xee //c3: 0 1 2 3

    // vst       VALPHAR,C0,    0x00
    // LD        $f15,   C0,    0x00
    // LD        $f15,   C0,    0x04
    // LD        $f15,   C0,    0x08
    // LD        $f15,   C0,    0x0c

    // vst       VALPHAI,C0,    0x00
    // LD        $f15,   C0,    0x00
    // LD        $f15,   C0,    0x04
    // LD        $f15,   C0,    0x08
    // LD        $f15,   C0,    0x0c

    vst       D4,     C0,    0x00
    vst       D2,     C1,    0x00
    vst       D5,     C2,    0x00
    vst       D3,     C3,    0x00

    // LD        $f15,   C0,    0x00
    // LD        $f15,   C0,    0x04
    // LD        $f15,   C0,    0x08
    // LD        $f15,   C0,    0x0c

    // LD        $f15,   C1,    0x00
    // LD        $f15,   C1,    0x04
    // LD        $f15,   C1,    0x08
    // LD        $f15,   C1,    0x0c

    // LD        $f15,   C2,    0x00
    // LD        $f15,   C2,    0x04
    // LD        $f15,   C2,    0x08
    // LD        $f15,   C2,    0x0c

    // LD        $f15,   C3,    0x00
    // LD        $f15,   C3,    0x04
    // LD        $f15,   C3,    0x08
    // LD        $f15,   C3,    0x0c

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10
    addi.d     C2,     C2,    0x10
    addi.d     C3,     C3,    0x10

    //res02 res12 res22 res32
    vld       D0,     C0,    0x00 //c0: 0 1 2 3
    vld       D1,     C1,    0x00 //c1: 0 1 2 3
    vld       D2,     C2,    0x00 //c2: 0 1 2 3
    vld       D3,     C3,    0x00 //c3: 0 1 2 3

    vand.v    D4,     D1,    D1
    vpermi.w  D4,     D0,    0x44 //c0:0 1, c1:0 1
    vshuf4i.w  D6,     D4,    0xd8 //c0[0] c1[0] c0[1] c1[1]
    vshuf4i.w  D7,     D4,    0x8d //c0[1] c1[1] c0[0] c1[0]

    vand.v    D5,     D3,    D3
    vpermi.w  D5,     D2,    0x44 //c2:0 1, c3:0 1
    vshuf4i.w  D8,     D5,    0xd8 //c2[0] c3[0] c2[1] c3[1]
    vshuf4i.w  D9,     D5,    0x8d //c2[1] c3[1] c2[0] c3[0]

    vpermi.w  D8,     D6,    0x44 //c0[0] c1[0] c2[0] c3[0]
    vpermi.w  D9,     D7,    0x44 //c0[1] c1[1] c2[1] c3[1]

    VFMADD      D8,    U4,    VALPHAR, D8
    VFMADD      D9,    U5,    VALPHAR, D9
    VNMSUB      D8,    U5,    VALPHAI, D8
    VFMADD      D9,    U4,    VALPHAI, D9

    vand.v    D10,     D9,    D9 //c0[1] c1[1] c2[1] c3[1]
    vand.v    D11,     D9,    D9 //c0[0] c1[0] c2[0] c3[0]

    vpermi.w  D10,     D8,    0x44 //c0[0] c1[0] c0[1] c1[1]
    vshuf4i.w  D10,     D10,   0xd8 //c0[0] c0[1] c1[0] c1[1]

    vpermi.w  D11,     D8,    0xee //c2[0] c3[0] c2[1] c3[1]
    vshuf4i.w  D11,     D11,   0xd8 //c2[0] c2[1] c3[0] c3[1]

    //res03 res13 res23 res33
    vand.v    D4,     D1,    D1
    vpermi.w  D4,     D0,    0xee //c0:2 3, c1:2 3
    vshuf4i.w  D6,     D4,    0xd8 //c0[2] c1[2] c0[3] c1[3]
    vshuf4i.w  D7,     D4,    0x8d //c0[3] c1[3] c0[2] c1[2]

    vand.v    D5,     D3,    D3
    vpermi.w  D5,     D2,    0xee //c2:2 3, c3:2 3
    vshuf4i.w  D8,     D5,    0xd8 //c2[2] c3[2] c2[3] c3[3]
    vshuf4i.w  D9,     D5,    0x8d //c2[3] c3[3] c2[2] c3[2]

    vpermi.w  D8,     D6,    0x44 //c0[2] c1[2] c2[2] c3[2]
    vpermi.w  D9,     D7,    0x44 //c0[3] c1[3] c2[3] c3[3]

    VFMADD      D8,    U6,    VALPHAR, D8
    VFMADD      D9,    U7,    VALPHAR, D9
    VNMSUB      D8,    U7,    VALPHAI, D8
    VFMADD      D9,    U6,    VALPHAI, D9

    vand.v    D4,     D9,    D9
    vpermi.w  D4,     D8,    0x44 //c0[2] c1[2] c0[3] c1[3]
    vshuf4i.w D4,     D4,    0xd8 //c0[2] c0[3] c1[2] c1[3]
    vand.v    D2,     D4,    D4

    vand.v    D5,     D9,    D9
    vpermi.w  D5,     D8,    0xee //c2[2] c3[2] c2[3] c3[3]
    vshuf4i.w D5,     D5,    0xd8 //c2[2] c2[3] c3[2] c3[3]
    vand.v    D3,     D5,    D5

    vand.v    D0,     D10,    D10 //c0[0] c0[1] c1[0] c1[1]
    vand.v    D1,     D11,    D11 //c2[0] c2[1] c3[0] c3[1]

    vpermi.w  D4,     D0,     0x44 //c0: 0 1 2 3
    vpermi.w  D2,     D0,     0xee //c1: 0 1 2 3
    vpermi.w  D5,     D1,     0x44 //c2: 0 1 2 3
    vpermi.w  D3,     D1,     0xee //c3: 0 1 2 3

    vst       D4,     C0,    0x00
    vst       D2,     C1,    0x00
    vst       D5,     C2,    0x00
    vst       D3,     C3,    0x00

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10
    addi.d     C2,     C2,    0x10
    addi.d     C3,     C3,    0x10
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d      TL,     K,    OFF
#ifdef LEFT
    addi.d     TL,     TL,   -4
#else
    addi.d     TL,     TL,   -4
#endif
    slli.d     T3,     TL,   0x05
    add.d      A0,     A0,   T3
    add.d      B0,     B0,   T3
#endif

#ifdef LEFT
    addi.d     OFF,    OFF,   4
#endif
#endif   // #if defined(TRMMKERNEL)

.L18:   /* if (bm & 2) */
    move       I,      $r0
    andi       T0,     M,     2
    beq        I,      T0,    .L183

    move       B0,     B      //ptrbb
    move       TL,     K      /* TL = bk */
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) ||(!defined(LEFT) && !defined(TRANSA))
    move       B0,     B    //ptrbb
#else
    slli.d     T3,     OFF,  0x04
    add.d      A0,     A0,   T3
    slli.d     T3,     OFF,  0x05
    add.d      B0,     B,    T3
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    sub.d      TL,     K,     OFF
#elif defined(LEFT)
    addi.d     TL,     OFF,   2
#else
    addi.d     TL,     OFF,   4
#endif

#endif  // #if defined(TRMMKERNEL)

    vxor.v    U0,     U0,   U0
    vxor.v    U1,     U1,   U1
    vxor.v    U2,     U2,   U2
    vxor.v    U3,     U3,   U3

    move       L,      $r0   //cycle param k
    beq        L,      TL,    .L182
    blt        TL,     L,     .L182

.L181:  /* for (k=0; k<temp; k++) */
    vld       D0,     A0,    0x00  // a0ri a1ri
    vld       D2,     B0,    0x00  // b0ri b1ri
    vld       D3,     B0,    0x10  // b2ri b3ri

    vshuf4i.w  D4,     D0,    0x00  //a0r
    vshuf4i.w  D5,     D0,    0x55  //a0i

    vpackev.w  D6,     D3,    D2
    vshuf4i.w  D6,     D6,    0xd8  //b0r b1r b2r b3r

    vpackod.w  D7,     D3,    D2
    vshuf4i.w  D7,     D7,    0xd8  //b0i b1i b2i b3i

    VMADD1    U0,     D4,    D6,     U0  //00r 10r 20r 30r
    VMADD2    U1,     D5,    D6,     U1  //00i 10i 20i 30i
    VMADD3    U0,     D5,    D7,     U0
    VMADD4    U1,     D4,    D7,     U1

    vshuf4i.w  D4,     D0,    0xaa  //a1r
    vshuf4i.w  D5,     D0,    0xff  //a1i

    VMADD1    U2,     D4,    D6,     U2  //01r 11r 21r 31r
    VMADD2    U3,     D5,    D6,     U3  //01i 11i 21i 31i
    VMADD3    U2,     D5,    D7,     U2
    VMADD4    U3,     D4,    D7,     U3

    addi.d     A0,     A0,    0x10
    addi.d     B0,     B0,    0x20

    addi.d     L,      L,     1
    blt        L,      TL,    .L181

.L182:
#if defined(TRMMKERNEL)
    //res00 res10 res20 res30
    vld       D0,     C0,    0x00 //c0: 0 1 2 3
    vld       D1,     C1,    0x00 //c1: 0 1 2 3
    vld       D2,     C2,    0x00 //c2: 0 1 2 3
    vld       D3,     C3,    0x00 //c3: 0 1 2 3

    vand.v    D4,     D1,    D1
    vpermi.w  D4,     D0,    0x44 //c0:0 1, c1:0 1
    vshuf4i.w  D6,     D4,    0xd8 //c0[0] c1[0] c0[1] c1[1]
    vshuf4i.w  D7,     D4,    0x8d //c0[1] c1[1] c0[0] c1[0]

    vand.v    D5,     D3,    D3
    vpermi.w  D5,     D2,    0x44 //c2:0 1, c3:0 1
    vshuf4i.w  D8,     D5,    0xd8 //c2[0] c3[0] c2[1] c3[1]
    vshuf4i.w  D9,     D5,    0x8d //c2[1] c3[1] c2[0] c3[0]

    vpermi.w  D8,     D6,    0x44 //c0[0] c1[0] c2[0] c3[0]
    vpermi.w  D9,     D7,    0x44 //c0[1] c1[1] c2[1] c3[1]

    vfmul.s      D8,    U0,    VALPHAR
    vfmul.s      D9,    U1,    VALPHAR
    VNMSUB      D8,    U1,    VALPHAI, D8
    VFMADD      D9,    U0,    VALPHAI, D9

    vand.v    D10,     D9,    D9 //c0[1] c1[1] c2[1] c3[1]
    vand.v    D11,     D9,    D9 //c0[0] c1[0] c2[0] c3[0]

    vpermi.w  D10,     D8,    0x44 //c0[0] c1[0] c0[1] c1[1]
    vshuf4i.w  D10,     D10,   0xd8 //c0[0] c0[1] c1[0] c1[1]

    vpermi.w  D11,     D8,    0xee //c2[0] c3[0] c2[1] c3[1]
    vshuf4i.w  D11,     D11,   0xd8 //c2[0] c2[1] c3[0] c3[1]

    //res01 res11 res21 res31
    vand.v    D4,     D1,    D1
    vpermi.w  D4,     D0,    0xee //c0:2 3, c1:2 3
    vshuf4i.w  D6,     D4,    0xd8 //c0[2] c1[2] c0[3] c1[3]
    vshuf4i.w  D7,     D4,    0x8d //c0[3] c1[3] c0[2] c1[2]

    vand.v    D5,     D3,    D3
    vpermi.w  D5,     D2,    0xee //c2:2 3, c3:2 3
    vshuf4i.w  D8,     D5,    0xd8 //c2[2] c3[2] c2[3] c3[3]
    vshuf4i.w  D9,     D5,    0x8d //c2[3] c3[3] c2[2] c3[2]

    vpermi.w  D8,     D6,    0x44 //c0[2] c1[2] c2[2] c3[2]
    vpermi.w  D9,     D7,    0x44 //c0[3] c1[3] c2[3] c3[3]

    vfmul.s      D8,    U2,    VALPHAR
    vfmul.s      D9,    U3,    VALPHAR
    VNMSUB      D8,    U3,    VALPHAI, D8
    VFMADD      D9,    U2,    VALPHAI, D9

    vand.v    D4,     D9,    D9
    vpermi.w  D4,     D8,    0x44 //c0[2] c1[2] c0[3] c1[3]
    vshuf4i.w D4,     D4,    0xd8 //c0[2] c0[3] c1[2] c1[3]
    vand.v    D2,     D4,    D4

    vand.v    D5,     D9,    D9
    vpermi.w  D5,     D8,    0xee //c2[2] c3[2] c2[3] c3[3]
    vshuf4i.w D5,     D5,    0xd8 //c2[2] c2[3] c3[2] c3[3]
    vand.v    D3,     D5,    D5

    vand.v    D0,     D10,    D10 //c0[0] c0[1] c1[0] c1[1]
    vand.v    D1,     D11,    D11 //c2[0] c2[1] c3[0] c3[1]

    vpermi.w  D4,     D0,     0x44 //c0: 0 1 2 3
    vpermi.w  D2,     D0,     0xee //c1: 0 1 2 3
    vpermi.w  D5,     D1,     0x44 //c2: 0 1 2 3
    vpermi.w  D3,     D1,     0xee //c3: 0 1 2 3

    vst       D4,     C0,    0x00
    vst       D2,     C1,    0x00
    vst       D5,     C2,    0x00
    vst       D3,     C3,    0x00

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10
    addi.d     C2,     C2,    0x10
    addi.d     C3,     C3,    0x10
#else
    //res00 res10 res20 res30
    vld       D0,     C0,    0x00 //c0: 0 1 2 3
    vld       D1,     C1,    0x00 //c1: 0 1 2 3
    vld       D2,     C2,    0x00 //c2: 0 1 2 3
    vld       D3,     C3,    0x00 //c3: 0 1 2 3

    vand.v    D4,     D1,    D1
    vpermi.w  D4,     D0,    0x44 //c0:0 1, c1:0 1
    vshuf4i.w  D6,     D4,    0xd8 //c0[0] c1[0] c0[1] c1[1]
    vshuf4i.w  D7,     D4,    0x8d //c0[1] c1[1] c0[0] c1[0]

    vand.v    D5,     D3,    D3
    vpermi.w  D5,     D2,    0x44 //c2:0 1, c3:0 1
    vshuf4i.w  D8,     D5,    0xd8 //c2[0] c3[0] c2[1] c3[1]
    vshuf4i.w  D9,     D5,    0x8d //c2[1] c3[1] c2[0] c3[0]

    vpermi.w  D8,     D6,    0x44 //c0[0] c1[0] c2[0] c3[0]
    vpermi.w  D9,     D7,    0x44 //c0[1] c1[1] c2[1] c3[1]

    VFMADD      D8,    U0,    VALPHAR, D8
    VFMADD      D9,    U1,    VALPHAR, D9
    VNMSUB      D8,    U1,    VALPHAI, D8
    VFMADD      D9,    U0,    VALPHAI, D9

    vand.v    D10,     D9,    D9 //c0[1] c1[1] c2[1] c3[1]
    vand.v    D11,     D9,    D9 //c0[0] c1[0] c2[0] c3[0]

    vpermi.w  D10,     D8,    0x44 //c0[0] c1[0] c0[1] c1[1]
    vshuf4i.w  D10,     D10,   0xd8 //c0[0] c0[1] c1[0] c1[1]

    vpermi.w  D11,     D8,    0xee //c2[0] c3[0] c2[1] c3[1]
    vshuf4i.w  D11,     D11,   0xd8 //c2[0] c2[1] c3[0] c3[1]

    //res01 res11 res21 res31
    vand.v    D4,     D1,    D1
    vpermi.w  D4,     D0,    0xee //c0:2 3, c1:2 3
    vshuf4i.w  D6,     D4,    0xd8 //c0[2] c1[2] c0[3] c1[3]
    vshuf4i.w  D7,     D4,    0x8d //c0[3] c1[3] c0[2] c1[2]

    vand.v    D5,     D3,    D3
    vpermi.w  D5,     D2,    0xee //c2:2 3, c3:2 3
    vshuf4i.w  D8,     D5,    0xd8 //c2[2] c3[2] c2[3] c3[3]
    vshuf4i.w  D9,     D5,    0x8d //c2[3] c3[3] c2[2] c3[2]

    vpermi.w  D8,     D6,    0x44 //c0[2] c1[2] c2[2] c3[2]
    vpermi.w  D9,     D7,    0x44 //c0[3] c1[3] c2[3] c3[3]

    VFMADD      D8,    U2,    VALPHAR, D8
    VFMADD      D9,    U3,    VALPHAR, D9
    VNMSUB      D8,    U3,    VALPHAI, D8
    VFMADD      D9,    U2,    VALPHAI, D9

    vand.v    D4,     D9,    D9
    vpermi.w  D4,     D8,    0x44 //c0[2] c1[2] c0[3] c1[3]
    vshuf4i.w D4,     D4,    0xd8 //c0[2] c0[3] c1[2] c1[3]
    vand.v    D2,     D4,    D4

    vand.v    D5,     D9,    D9
    vpermi.w  D5,     D8,    0xee //c2[2] c3[2] c2[3] c3[3]
    vshuf4i.w D5,     D5,    0xd8 //c2[2] c2[3] c3[2] c3[3]
    vand.v    D3,     D5,    D5

    vand.v    D0,     D10,    D10 //c0[0] c0[1] c1[0] c1[1]
    vand.v    D1,     D11,    D11 //c2[0] c2[1] c3[0] c3[1]

    vpermi.w  D4,     D0,     0x44 //c0: 0 1 2 3
    vpermi.w  D2,     D0,     0xee //c1: 0 1 2 3
    vpermi.w  D5,     D1,     0x44 //c2: 0 1 2 3
    vpermi.w  D3,     D1,     0xee //c3: 0 1 2 3

    vst       D4,     C0,    0x00
    vst       D2,     C1,    0x00
    vst       D5,     C2,    0x00
    vst       D3,     C3,    0x00

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10
    addi.d     C2,     C2,    0x10
    addi.d     C3,     C3,    0x10
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d      TL,     K,    OFF
#ifdef LEFT
    addi.d     TL,     TL,   -2
#else
    addi.d     TL,     TL,   -4
#endif
    slli.d     T3,     TL,   0x04
    add.d      A0,     A0,   T3
    slli.d     T3,     TL,   0x05
    add.d      B0,     B0,   T3
#endif

#ifdef LEFT
    addi.d     OFF,    OFF,   2
#endif
#endif   // #if defined(TRMMKERNEL)

.L183:   /* if (bm & 1) */
    move       I,      $r0
    andi       T0,     M,     1
    beq        I,      T0,    .L186

    move       B0,     B      //ptrbb
    move       TL,     K      /* TL = bk */
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) ||(!defined(LEFT) && !defined(TRANSA))
    move       B0,     B    //ptrbb
#else
    slli.d     T3,     OFF,  0x03
    add.d      A0,     A0,   T3
    slli.d     T3,     OFF,  0x05
    add.d      B0,     B,    T3
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    sub.d      TL,     K,     OFF
#elif defined(LEFT)
    addi.d     TL,     OFF,   1
#else
    addi.d     TL,     OFF,   4
#endif

#endif  // #if defined(TRMMKERNEL)

    MTC        c11,    $r0
    MTC        c12,    $r0
    MTC        c21,    $r0
    MTC        c22,    $r0
    MTC        c31,    $r0
    MTC        c32,    $r0
    MTC        c41,    $r0
    MTC        c42,    $r0

    move       L,      $r0   //cycle param k
    beq        L,      TL,    .L185
    blt        TL,     L,     .L185

.L184:  /* for (k=0; k<temp; k++) */
    LD         a1,     A0,    0x00        //a0r
    LD         a2,     A0,    0x04        //a0i

    LD         b1,     B0,    0x00        //b0r
    LD         b2,     B0,    0x04        //b0i
    LD         b3,     B0,    0x08        //b1r
    LD         b4,     B0,    0x0c        //b1i
    LD         b5,     B0,    0x10        //b2r
    LD         b6,     B0,    0x14        //b2i
    LD         b7,     B0,    0x18        //b3r
    LD         b8,     B0,    0x1c        //b3i

    MADD1      c11,    a1,    b1,     c11  //res00r
    MADD2      c12,    a2,    b1,     c12  //res00i
    MADD3      c11,    a2,    b2,     c11
    MADD4      c12,    a1,    b2,     c12

    MADD1      c21,    a1,    b3,     c21  //res10r
    MADD2      c22,    a2,    b3,     c22  //res10i
    MADD3      c21,    a2,    b4,     c21
    MADD4      c22,    a1,    b4,     c22

    MADD1      c31,    a1,    b5,     c31  //res20r
    MADD2      c32,    a2,    b5,     c32  //res20i
    MADD3      c31,    a2,    b6,     c31
    MADD4      c32,    a1,    b6,     c32

    MADD1      c41,    a1,    b7,     c41  //res30r
    MADD2      c42,    a2,    b7,     c42  //res30i
    MADD3      c41,    a2,    b8,     c41
    MADD4      c42,    a1,    b8,     c42

    addi.d     A0,     A0,    0x08
    addi.d     B0,     B0,    0x20

    addi.d     L,      L,     1
    blt        L,      TL,    .L184

.L185:
#if defined(TRMMKERNEL)
    //res00 res10 res20 res30
    LD         a5,     C0,    0x00    //C0[0]
    LD         a6,     C0,    0x04    //C0[1]

    MUL       a5,     c11,   ALPHA_R
    MUL       a6,     c12,   ALPHA_R
    NMSUB      a5,     c12,   ALPHA_I, a5
    MADD       a6,     c11,   ALPHA_I, a6

    ST         a5,     C0,    0x00
    ST         a6,     C0,    0x04

    LD         a5,     C1,    0x00    //C1[0]
    LD         a6,     C1,    0x04    //C1[1]

    MUL       a5,     c21,   ALPHA_R
    MUL       a6,     c22,   ALPHA_R
    NMSUB      a5,     c22,   ALPHA_I, a5
    MADD       a6,     c21,   ALPHA_I, a6

    ST         a5,     C1,    0x00
    ST         a6,     C1,    0x04

    LD         a5,     C2,    0x00    //C2[0]
    LD         a6,     C2,    0x04    //C2[1]

    MUL       a5,     c31,   ALPHA_R
    MUL       a6,     c32,   ALPHA_R
    NMSUB      a5,     c32,   ALPHA_I, a5
    MADD       a6,     c31,   ALPHA_I, a6

    ST         a5,     C2,    0x00
    ST         a6,     C2,    0x04

    LD         a5,     C3,    0x00    //C3[0]
    LD         a6,     C3,    0x04    //C3[1]

    MUL       a5,     c41,   ALPHA_R
    MUL       a6,     c42,   ALPHA_R
    NMSUB      a5,     c42,   ALPHA_I, a5
    MADD       a6,     c41,   ALPHA_I, a6

    ST         a5,     C3,    0x00
    ST         a6,     C3,    0x04

    addi.d     C0,     C0,    0x08
    addi.d     C1,     C1,    0x08
    addi.d     C2,     C2,    0x08
    addi.d     C3,     C3,    0x08
#else
    //res00 res10 res20 res30
    LD         a5,     C0,    0x00    //C0[0]
    LD         a6,     C0,    0x04    //C0[1]

    MADD       a5,     c11,   ALPHA_R, a5
    MADD       a6,     c12,   ALPHA_R, a6
    NMSUB      a5,     c12,   ALPHA_I, a5
    MADD       a6,     c11,   ALPHA_I, a6

    ST         a5,     C0,    0x00
    ST         a6,     C0,    0x04

    LD         a5,     C1,    0x00    //C1[0]
    LD         a6,     C1,    0x04    //C1[1]

    MADD       a5,     c21,   ALPHA_R, a5
    MADD       a6,     c22,   ALPHA_R, a6
    NMSUB      a5,     c22,   ALPHA_I, a5
    MADD       a6,     c21,   ALPHA_I, a6

    ST         a5,     C1,    0x00
    ST         a6,     C1,    0x04

    LD         a5,     C2,    0x00    //C2[0]
    LD         a6,     C2,    0x04    //C2[1]

    MADD       a5,     c31,   ALPHA_R, a5
    MADD       a6,     c32,   ALPHA_R, a6
    NMSUB      a5,     c32,   ALPHA_I, a5
    MADD       a6,     c31,   ALPHA_I, a6

    ST         a5,     C2,    0x00
    ST         a6,     C2,    0x04

    LD         a5,     C3,    0x00    //C3[0]
    LD         a6,     C3,    0x04    //C3[1]

    MADD       a5,     c41,   ALPHA_R, a5
    MADD       a6,     c42,   ALPHA_R, a6
    NMSUB      a5,     c42,   ALPHA_I, a5
    MADD       a6,     c41,   ALPHA_I, a6

    ST         a5,     C3,    0x00
    ST         a6,     C3,    0x04

    addi.d     C0,     C0,    0x08
    addi.d     C1,     C1,    0x08
    addi.d     C2,     C2,    0x08
    addi.d     C3,     C3,    0x08
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d      TL,     K,    OFF
#ifdef LEFT
    addi.d     TL,     TL,   -1
#else
    addi.d     TL,     TL,   -4
#endif
    slli.d     T3,     TL,   0x03
    add.d      A0,     A0,   T3
    slli.d     C3,     TL,   0x05
    add.d      B0,     B0,   T3
#endif

#ifdef LEFT
    addi.d     OFF,    OFF,   1
#endif
#endif   // #if defined(TRMMKERNEL)


.L186:
#if defined(TRMMKERNEL) && !defined(LEFT)
    addi.d     OFF,    OFF,   4
#endif

    slli.d     L,      K,     0x05
    add.d      B,      B,     L

    slli.d     I,      LDC,   0x03
    add.d      C,      C,     I

    addi.d     J,      J,     1
    srai.d     T0,     N,     2
    blt        J,      T0,    .L10

.L19:
    move       J,      $r0
    andi       T0,     N,     2
    beq        J,      T0,    .L30

.L20: /* for (j=0; j<(bn&2); j+=2) */
#if defined(TRMMKERNEL) && defined(LEFT)
    move       OFF,    OFFSET
#endif

    move       C0,     C
    slli.d     TL,     LDC,   1
    add.d      C1,     C0,    TL
    move       A0,     A    //ptrba

    move       I,      $r0
    srai.d     T0,     M,     3  //bm/8
    beq        I,      T0,    .L24

.L21:  /* for (i=0; i<bm/8; i+=1) */
    move       B0,     B      //ptrbb
    move       TL,     K      /* TL = bk */
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) ||(!defined(LEFT) && !defined(TRANSA))
    move       B0,     B    //ptrbb
#else
    slli.d     T3,     OFF,  0x06
    add.d      A0,     A0,   T3
    slli.d     T3,     OFF,  0x04
    add.d      B0,     B,    T3
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    sub.d      TL,     K,     OFF
#elif defined(LEFT)
    addi.d     TL,     OFF,   8
#else
    addi.d     TL,     OFF,   2
#endif

#endif  // #if defined(TRMMKERNEL)

    vxor.v    U0,     U0,   U0
    vxor.v    U1,     U1,   U1
    vxor.v    U2,     U2,   U2
    vxor.v    U3,     U3,   U3
    vxor.v    U4,     U4,   U4
    vxor.v    U5,     U5,   U5
    vxor.v    U6,     U6,   U6
    vxor.v    U7,     U7,   U7

    move       L,      $r0   //cycle param k
    beq        L,      TL,    .L23
    blt        TL,     L,     .L23

.L22:  /* for (k=0; k<temp; k++) */
    vld       D0,     A0,    0x00  // a0ri a1ri
    vld       D2,     B0,    0x00  // b0ri b1ri

    vshuf4i.w  D4,     D0,    0xa0  //a0rr a1rr
    vshuf4i.w  D5,     D0,    0xf5  //a0ii a1ii

    vshuf4i.w  D6,     D2,    0x88  //b0r b1r b0r b1r
    vshuf4i.w  D7,     D2,    0xdd  //b0i b1i b0i b1i

    VMADD1    U0,     D4,    D6,     U0  //00r 10r 01r 11r
    VMADD2    U1,     D5,    D6,     U1  //00i 10i 01i 11i
    VMADD3    U0,     D5,    D7,     U0
    VMADD4    U1,     D4,    D7,     U1

    vld       D0,     A0,    0x10  // a2ri a3ri

    vshuf4i.w  D4,     D0,    0xa0  //a2rr a3rr
    vshuf4i.w  D5,     D0,    0xf5  //a2ii a3ii

    VMADD1    U2,     D4,    D6,     U2  //02r 12r 03r 13r
    VMADD2    U3,     D5,    D6,     U3  //02i 12i 03i 13i
    VMADD3    U2,     D5,    D7,     U2
    VMADD4    U3,     D4,    D7,     U3

    vld       D0,     A0,    0x20  // a4ri a5ri

    vshuf4i.w  D4,     D0,    0xa0  //a4rr a5rr
    vshuf4i.w  D5,     D0,    0xf5  //a4ii a5ii

    VMADD1    U4,     D4,    D6,     U4  //04r 14r 05r 15r
    VMADD2    U5,     D5,    D6,     U5  //04i 14i 05i 15i
    VMADD3    U4,     D5,    D7,     U4
    VMADD4    U5,     D4,    D7,     U5

    vld       D0,     A0,    0x30  // a6ri a7ri

    vshuf4i.w  D4,     D0,    0xa0  //a6rr a7rr
    vshuf4i.w  D5,     D0,    0xf5  //a6ii a7ii

    VMADD1    U6,     D4,    D6,     U6  //06r 16r 07r 17r
    VMADD2    U7,     D5,    D6,     U7  //06i 16i 07i 17i
    VMADD3    U6,     D5,    D7,     U6
    VMADD4    U7,     D4,    D7,     U7

    addi.d     A0,     A0,    0x40
    addi.d     B0,     B0,    0x10

    addi.d     L,      L,     1
    blt        L,      TL,    .L22

.L23:
#if defined(TRMMKERNEL)
    //res00 res10 res01 res11
    vld       D0,     C0,    0x00 //c0: 0 1 2 3
    vld       D1,     C1,    0x00 //c1: 0 1 2 3

    vpackev.w D2,     D1,    D0  //0 4 2 6
    vpackod.w D3,     D1,    D0  //1 5 3 7

    vfmul.s      D2,    U0,    VALPHAR
    vfmul.s      D3,    U1,    VALPHAR
    VNMSUB      D2,    U1,    VALPHAI, D2
    VFMADD      D3,    U0,    VALPHAI, D3

    vpackev.w D4,     D3,    D2  //0 1 2 3
    vpackod.w D5,     D3,    D2  //4 5 6 7

    vst       D4,     C0,    0x00 //c0: 0 1 2 3
    vst       D5,     C1,    0x00 //c1: 0 1 2 3

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10

    //res02 res12 res03 res13
    vld       D0,     C0,    0x00 //c0: 0 1 2 3
    vld       D1,     C1,    0x00 //c1: 0 1 2 3

    vpackev.w D2,     D1,    D0  //0 4 2 6
    vpackod.w D3,     D1,    D0  //1 5 3 7

    vfmul.s      D2,    U2,    VALPHAR
    vfmul.s      D3,    U3,    VALPHAR
    VNMSUB      D2,    U3,    VALPHAI, D2
    VFMADD      D3,    U2,    VALPHAI, D3

    vpackev.w D4,     D3,    D2  //0 1 2 3
    vpackod.w D5,     D3,    D2  //4 5 6 7

    vst       D4,     C0,    0x00 //c0: 0 1 2 3
    vst       D5,     C1,    0x00 //c1: 0 1 2 3

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10

    //res04 res14 res05 res15
    vld       D0,     C0,    0x00 //c0: 0 1 2 3
    vld       D1,     C1,    0x00 //c1: 0 1 2 3

    vpackev.w D2,     D1,    D0  //0 4 2 6
    vpackod.w D3,     D1,    D0  //1 5 3 7

    vfmul.s      D2,    U4,    VALPHAR
    vfmul.s      D3,    U5,    VALPHAR
    VNMSUB      D2,    U5,    VALPHAI, D2
    VFMADD      D3,    U4,    VALPHAI, D3

    vpackev.w D4,     D3,    D2  //0 1 2 3
    vpackod.w D5,     D3,    D2  //4 5 6 7

    vst       D4,     C0,    0x00 //c0: 0 1 2 3
    vst       D5,     C1,    0x00 //c1: 0 1 2 3

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10

    //res06 res16 res07 res17
    vld       D0,     C0,    0x00 //c0: 0 1 2 3
    vld       D1,     C1,    0x00 //c1: 0 1 2 3

    vpackev.w D2,     D1,    D0  //0 4 2 6
    vpackod.w D3,     D1,    D0  //1 5 3 7

    vfmul.s      D2,    U6,    VALPHAR
    vfmul.s      D3,    U7,    VALPHAR
    VNMSUB      D2,    U7,    VALPHAI, D2
    VFMADD      D3,    U6,    VALPHAI, D3

    vpackev.w D4,     D3,    D2  //0 1 2 3
    vpackod.w D5,     D3,    D2  //4 5 6 7

    vst       D4,     C0,    0x00 //c0: 0 1 2 3
    vst       D5,     C1,    0x00 //c1: 0 1 2 3

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10
#else
    //res00 res10 res01 res11
    vld       D0,     C0,    0x00 //c0: 0 1 2 3
    vld       D1,     C1,    0x00 //c1: 0 1 2 3

    vpackev.w D2,     D1,    D0  //0 4 2 6
    vpackod.w D3,     D1,    D0  //1 5 3 7

    VFMADD      D2,    U0,    VALPHAR, D2
    VFMADD      D3,    U1,    VALPHAR, D3
    VNMSUB      D2,    U1,    VALPHAI, D2
    VFMADD      D3,    U0,    VALPHAI, D3

    vpackev.w D4,     D3,    D2  //0 1 2 3
    vpackod.w D5,     D3,    D2  //4 5 6 7

    vst       D4,     C0,    0x00 //c0: 0 1 2 3
    vst       D5,     C1,    0x00 //c1: 0 1 2 3

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10

    //res02 res12 res03 res13
    vld       D0,     C0,    0x00 //c0: 0 1 2 3
    vld       D1,     C1,    0x00 //c1: 0 1 2 3

    vpackev.w D2,     D1,    D0  //0 4 2 6
    vpackod.w D3,     D1,    D0  //1 5 3 7

    VFMADD      D2,    U2,    VALPHAR, D2
    VFMADD      D3,    U3,    VALPHAR, D3
    VNMSUB      D2,    U3,    VALPHAI, D2
    VFMADD      D3,    U2,    VALPHAI, D3

    vpackev.w D4,     D3,    D2  //0 1 2 3
    vpackod.w D5,     D3,    D2  //4 5 6 7

    vst       D4,     C0,    0x00 //c0: 0 1 2 3
    vst       D5,     C1,    0x00 //c1: 0 1 2 3

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10

    //res04 res14 res05 res15
    vld       D0,     C0,    0x00 //c0: 0 1 2 3
    vld       D1,     C1,    0x00 //c1: 0 1 2 3

    vpackev.w D2,     D1,    D0  //0 4 2 6
    vpackod.w D3,     D1,    D0  //1 5 3 7

    VFMADD      D2,    U4,    VALPHAR, D2
    VFMADD      D3,    U5,    VALPHAR, D3
    VNMSUB      D2,    U5,    VALPHAI, D2
    VFMADD      D3,    U4,    VALPHAI, D3

    vpackev.w D4,     D3,    D2  //0 1 2 3
    vpackod.w D5,     D3,    D2  //4 5 6 7

    vst       D4,     C0,    0x00 //c0: 0 1 2 3
    vst       D5,     C1,    0x00 //c1: 0 1 2 3

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10

    //res06 res16 res07 res17
    vld       D0,     C0,    0x00 //c0: 0 1 2 3
    vld       D1,     C1,    0x00 //c1: 0 1 2 3

    vpackev.w D2,     D1,    D0  //0 4 2 6
    vpackod.w D3,     D1,    D0  //1 5 3 7

    VFMADD      D2,    U6,    VALPHAR, D2
    VFMADD      D3,    U7,    VALPHAR, D3
    VNMSUB      D2,    U7,    VALPHAI, D2
    VFMADD      D3,    U6,    VALPHAI, D3

    vpackev.w D4,     D3,    D2  //0 1 2 3
    vpackod.w D5,     D3,    D2  //4 5 6 7

    vst       D4,     C0,    0x00 //c0: 0 1 2 3
    vst       D5,     C1,    0x00 //c1: 0 1 2 3

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d      TL,     K,    OFF
#ifdef LEFT
    addi.d     TL,     TL,   -8
#else
    addi.d     TL,     TL,   -2
#endif
    slli.d     T3,     TL,   0x06
    add.d      A0,     A0,   T3
    slli.d     T3,     TL,   0x04
    add.d      B0,     B0,   T3
#endif

#ifdef LEFT
    addi.d     OFF,    OFF,   8
#endif
#endif   // #if defined(TRMMKERNEL)

    addi.d     I,      I,     1
    blt        I,      T0,    .L21

.L24:   /* if ( bm & 4 ) */
    move       I,      $r0
    andi       T1,     M,     4    //bm&4
    beq        I,      T1,    .L280

.L25:
    move       B0,     B      //ptrbb
    move       TL,     K      /* TL = bk */
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) ||(!defined(LEFT) && !defined(TRANSA))
    move       B0,     B    //ptrbb
#else
    slli.d     T3,     OFF,   0x05
    add.d      A0,     A0,    T3
    slli.d     T3,     OFF,   0x04
    add.d      B0,     B,     T3
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    sub.d      TL,     K,     OFF
#elif defined(LEFT)
    addi.d     TL,     OFF,   4
#else
    addi.d     TL,     OFF,   2
#endif

#endif  // #if defined(TRMMKERNEL)

    vxor.v    U0,     U0,   U0
    vxor.v    U1,     U1,   U1
    vxor.v    U2,     U2,   U2
    vxor.v    U3,     U3,   U3

    move       L,      $r0   //cycle param k
    beq        L,      TL,    .L27
    blt        TL,     L,     .L27

.L26:  /* for (k=0; k<temp; k++) */
    vld       D0,     A0,    0x00  // a0ri a1ri
    vld       D2,     B0,    0x00  // b0ri b1ri

    vshuf4i.w  D4,     D0,    0xa0  //a0rr a1rr
    vshuf4i.w  D5,     D0,    0xf5  //a0ii a1ii

    vshuf4i.w  D6,     D2,    0x88  //b0r b1r b0r b1r
    vshuf4i.w  D7,     D2,    0xdd  //b0i b1i b0i b1i

    VMADD1    U0,     D4,    D6,     U0  //00r 10r 01r 11r
    VMADD2    U1,     D5,    D6,     U1  //00i 10i 01i 11i
    VMADD3    U0,     D5,    D7,     U0
    VMADD4    U1,     D4,    D7,     U1

    vld       D0,     A0,    0x10  // a2ri a3ri

    vshuf4i.w  D4,     D0,    0xa0  //a2rr a3rr
    vshuf4i.w  D5,     D0,    0xf5  //a2ii a3ii

    VMADD1    U2,     D4,    D6,     U2  //02r 12r 03r 13r
    VMADD2    U3,     D5,    D6,     U3  //02i 12i 03i 13i
    VMADD3    U2,     D5,    D7,     U2
    VMADD4    U3,     D4,    D7,     U3

    addi.d     A0,     A0,    0x20
    addi.d     B0,     B0,    0x10

    addi.d     L,      L,     1
    blt        L,      TL,    .L26

.L27:
#if defined(TRMMKERNEL)
    //res00 res10 res01 res11
    vld       D0,     C0,    0x00 //c0: 0 1 2 3
    vld       D1,     C1,    0x00 //c1: 0 1 2 3

    vpackev.w D2,     D1,    D0  //0 4 2 6
    vpackod.w D3,     D1,    D0  //1 5 3 7

    vfmul.s      D2,    U0,    VALPHAR
    vfmul.s      D3,    U1,    VALPHAR
    VNMSUB      D2,    U1,    VALPHAI, D2
    VFMADD      D3,    U0,    VALPHAI, D3

    vpackev.w D4,     D3,    D2  //0 1 2 3
    vpackod.w D5,     D3,    D2  //4 5 6 7

    vst       D4,     C0,    0x00 //c0: 0 1 2 3
    vst       D5,     C1,    0x00 //c1: 0 1 2 3

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10

    //res02 res12 res03 res13
    vld       D0,     C0,    0x00 //c0: 0 1 2 3
    vld       D1,     C1,    0x00 //c1: 0 1 2 3

    vpackev.w D2,     D1,    D0  //0 4 2 6
    vpackod.w D3,     D1,    D0  //1 5 3 7

    vfmul.s      D2,    U2,    VALPHAR
    vfmul.s      D3,    U3,    VALPHAR
    VNMSUB      D2,    U3,    VALPHAI, D2
    VFMADD      D3,    U2,    VALPHAI, D3

    vpackev.w D4,     D3,    D2  //0 1 2 3
    vpackod.w D5,     D3,    D2  //4 5 6 7

    vst       D4,     C0,    0x00 //c0: 0 1 2 3
    vst       D5,     C1,    0x00 //c1: 0 1 2 3

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10
#else
    //res00 res10 res01 res11
    vld       D0,     C0,    0x00 //c0: 0 1 2 3
    vld       D1,     C1,    0x00 //c1: 0 1 2 3

    vpackev.w D2,     D1,    D0  //0 4 2 6
    vpackod.w D3,     D1,    D0  //1 5 3 7

    VFMADD      D2,    U0,    VALPHAR, D2
    VFMADD      D3,    U1,    VALPHAR, D3
    VNMSUB      D2,    U1,    VALPHAI, D2
    VFMADD      D3,    U0,    VALPHAI, D3

    vpackev.w D4,     D3,    D2  //0 1 2 3
    vpackod.w D5,     D3,    D2  //4 5 6 7

    vst       D4,     C0,    0x00 //c0: 0 1 2 3
    vst       D5,     C1,    0x00 //c1: 0 1 2 3

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10

    //res02 res12 res03 res13
    vld       D0,     C0,    0x00 //c0: 0 1 2 3
    vld       D1,     C1,    0x00 //c1: 0 1 2 3

    vpackev.w D2,     D1,    D0  //0 4 2 6
    vpackod.w D3,     D1,    D0  //1 5 3 7

    VFMADD      D2,    U2,    VALPHAR, D2
    VFMADD      D3,    U3,    VALPHAR, D3
    VNMSUB      D2,    U3,    VALPHAI, D2
    VFMADD      D3,    U2,    VALPHAI, D3

    vpackev.w D4,     D3,    D2  //0 1 2 3
    vpackod.w D5,     D3,    D2  //4 5 6 7

    vst       D4,     C0,    0x00 //c0: 0 1 2 3
    vst       D5,     C1,    0x00 //c1: 0 1 2 3

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d      TL,     K,    OFF
#ifdef LEFT
    addi.d     TL,     TL,   -4
#else
    addi.d     TL,     TL,   -2
#endif
    slli.d     T3,     TL,   0x05
    add.d      A0,     A0,   T3
    slli.d     T3,     TL,   0x04
    add.d      B0,     B0,   T3
#endif

#ifdef LEFT
    addi.d     OFF,    OFF,   4
#endif
#endif   // #if defined(TRMMKERNEL)

.L280:   /* if ( bm & 2 )*/
    move       I,      $r0
    andi       T1,     M,     2    //bm&2
    beq        I,      T1,    .L284

.L281:
    move       B0,     B      //ptrbb
    move       TL,     K      /* TL = bk */
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) ||(!defined(LEFT) && !defined(TRANSA))
    move       B0,     B    //ptrbb
#else
    slli.d     T3,     OFF,   0x04
    add.d      A0,     A0,    T3
    add.d      B0,     B,     T3
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    sub.d      TL,     K,     OFF
#elif defined(LEFT)
    addi.d     TL,     OFF,   2
#else
    addi.d     TL,     OFF,   2
#endif

#endif  // #if defined(TRMMKERNEL)

    vxor.v    U0,     U0,   U0
    vxor.v    U1,     U1,   U1

    move       L,      $r0   //cycle param k
    beq        L,      TL,    .L283
    blt        TL,     L,     .L283

.L282:  /* for (k=0; k<temp; k++) */
    vld       D0,     A0,    0x00  // a0ri a1ri
    vld       D2,     B0,    0x00  // b0ri b1ri

    vshuf4i.w  D4,     D0,    0xa0  //a0rr a1rr
    vshuf4i.w  D5,     D0,    0xf5  //a0ii a1ii

    vshuf4i.w  D6,     D2,    0x88  //b0r b1r b0r b1r
    vshuf4i.w  D7,     D2,    0xdd  //b0i b1i b0i b1i

    VMADD1    U0,     D4,    D6,     U0  //00r 10r 01r 11r
    VMADD2    U1,     D5,    D6,     U1  //00i 10i 01i 11i
    VMADD3    U0,     D5,    D7,     U0
    VMADD4    U1,     D4,    D7,     U1

    addi.d     A0,     A0,    0x10
    addi.d     B0,     B0,    0x10

    addi.d     L,      L,     1
    blt        L,      TL,    .L282

.L283:
#if defined(TRMMKERNEL)
    //res00 res10 res01 res11
    vld       D0,     C0,    0x00 //c0: 0 1 2 3
    vld       D1,     C1,    0x00 //c1: 0 1 2 3

    vpackev.w D2,     D1,    D0  //0 4 2 6
    vpackod.w D3,     D1,    D0  //1 5 3 7

    vfmul.s      D2,    U0,    VALPHAR
    vfmul.s      D3,    U1,    VALPHAR
    VNMSUB      D2,    U1,    VALPHAI, D2
    VFMADD      D3,    U0,    VALPHAI, D3

    vpackev.w D4,     D3,    D2  //0 1 2 3
    vpackod.w D5,     D3,    D2  //4 5 6 7

    vst       D4,     C0,    0x00 //c0: 0 1 2 3
    vst       D5,     C1,    0x00 //c1: 0 1 2 3

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10
#else
    //res00 res10 res01 res11
    vld       D0,     C0,    0x00 //c0: 0 1 2 3
    vld       D1,     C1,    0x00 //c1: 0 1 2 3

    vpackev.w D2,     D1,    D0  //0 4 2 6
    vpackod.w D3,     D1,    D0  //1 5 3 7

    VFMADD      D2,    U0,    VALPHAR, D2
    VFMADD      D3,    U1,    VALPHAR, D3
    VNMSUB      D2,    U1,    VALPHAI, D2
    VFMADD      D3,    U0,    VALPHAI, D3

    vpackev.w D4,     D3,    D2  //0 1 2 3
    vpackod.w D5,     D3,    D2  //4 5 6 7

    vst       D4,     C0,    0x00 //c0: 0 1 2 3
    vst       D5,     C1,    0x00 //c1: 0 1 2 3

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d      TL,     K,    OFF
#ifdef LEFT
    addi.d     TL,     TL,   -2
#else
    addi.d     TL,     TL,   -2
#endif
    slli.d     T3,     TL,   0x04
    add.d      A0,     A0,   T3
    slli.d     T3,     TL,   0x04
    add.d      B0,     B0,   T3
#endif

#ifdef LEFT
    addi.d     OFF,    OFF,   2
#endif
#endif   // #if defined(TRMMKERNEL)

.L284:   /* if ( bm & 1 )*/
    move       I,      $r0
    andi       T1,     M,     1    //bm&1
    beq        I,      T1,    .L288

.L285:
    move       B0,     B      //ptrbb
    move       TL,     K      /* TL = bk */
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) ||(!defined(LEFT) && !defined(TRANSA))
    move       B0,     B    //ptrbb
#else
    slli.d     T3,     OFF,   0x03
    add.d      A0,     A0,    T3
    slli.d     T3,     OFF,   0x04
    add.d      B0,     B,     T3
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
    beq        L,      TL,    .L287
    blt        TL,     L,     .L287

.L286:  /* for (k=0; k<temp; k++) */
    LD         a1,     A0,    0x00        //a0r
    LD         a2,     A0,    0x04        //a0i

    LD         b1,     B0,    0x00        //b0r
    LD         b2,     B0,    0x04        //b0i
    LD         b3,     B0,    0x08        //b1r
    LD         b4,     B0,    0x0c        //b1i

    MADD1      c11,    a1,    b1,     c11  //res00r
    MADD2      c12,    a2,    b1,     c12  //res00i
    MADD3      c11,    a2,    b2,     c11
    MADD4      c12,    a1,    b2,     c12

    MADD1      c21,    a1,    b3,     c21  //res10r
    MADD2      c22,    a2,    b3,     c22  //res10i
    MADD3      c21,    a2,    b4,     c21
    MADD4      c22,    a1,    b4,     c22

    addi.d     A0,     A0,    0x08
    addi.d     B0,     B0,    0x10

    addi.d     L,      L,     1
    blt        L,      TL,    .L286

.L287:
#if defined(TRMMKERNEL)
    //res00 res10
    LD         a5,     C0,    0x00    //C0[0]
    LD         a6,     C0,    0x04    //C0[1]
    LD         a7,     C1,    0x00    //C1[0]
    LD         a8,     C1,    0x04    //C1[1]

    MUL       a5,     c11,   ALPHA_R
    MUL       a6,     c12,   ALPHA_R
    NMSUB      a5,     c12,   ALPHA_I, a5
    MADD       a6,     c11,   ALPHA_I, a6

    MUL       a7,     c21,   ALPHA_R
    MUL       a8,     c22,   ALPHA_R
    NMSUB      a7,     c22,   ALPHA_I, a7
    MADD       a8,     c21,   ALPHA_I, a8

    ST         a5,     C0,    0x00
    ST         a6,     C0,    0x04

    ST         a7,     C1,    0x00
    ST         a8,     C1,    0x04

    addi.d     C0,     C0,    0x08
    addi.d     C1,     C1,    0x08
#else
    //res00 res10
    LD         a5,     C0,    0x00    //C0[0]
    LD         a6,     C0,    0x04    //C0[1]
    LD         a7,     C1,    0x00    //C1[0]
    LD         a8,     C1,    0x04    //C1[1]

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

    ST         a7,     C1,    0x00
    ST         a8,     C1,    0x04

    addi.d     C0,     C0,    0x08
    addi.d     C1,     C1,    0x08
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d      TL,     K,    OFF
#ifdef LEFT
    addi.d     TL,     TL,   -1
#else
    addi.d     TL,     TL,   -2
#endif
    slli.d     T3,     TL,   0x03
    add.d      A0,     A0,   T3
    slli.d     T3,     TL,   0x04
    add.d      B0,     B0,   T3
#endif

#ifdef LEFT
    addi.d     OFF,    OFF,   1
#endif
#endif   // #if defined(TRMMKERNEL)

.L288:
#if defined(TRMMKERNEL) && !defined(LEFT)
    addi.d     OFF,    OFF,   2
#endif
    slli.d     L,      K,     4
    add.d      B,      B,     L

    slli.d     I,      LDC,   2
    add.d      C,      C,     I

    addi.d     J,      J,     2
    andi       T0,     N,     2
    blt        J,      T0,    .L20

.L30:
    move       J,      $r0
    andi       T0,     N,     1
    beq        J,      T0,    .L999

.L300:  /* for (j=0; j<(bn&1); j+=1) */
#if defined(TRMMKERNEL) && defined(LEFT)
    move       OFF,    OFFSET
#endif

    move       C0,     C
    move       A0,     A    //ptrba

    move       I,      $r0
    srai.d     T0,     M,     3  //bm/8
    beq        I,      T0,    .L34

.L31:  /* for (i=0; i<bm/8; i+=1) */
    move       B0,     B      //ptrbb
    move       TL,     K      /* TL = bk */
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) ||(!defined(LEFT) && !defined(TRANSA))
    move       B0,     B    //ptrbb
#else
    slli.d     T3,     OFF,  0x06
    add.d      A0,     A0,   T3
    slli.d     T3,     OFF,  0x03
    add.d      B0,     B,    T3
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    sub.d      TL,     K,     OFF
#elif defined(LEFT)
    addi.d     TL,     OFF,   8
#else
    addi.d     TL,     OFF,   1
#endif

#endif  // #if defined(TRMMKERNEL)

    vxor.v    U0,     U0,   U0
    vxor.v    U1,     U1,   U1
    vxor.v    U2,     U2,   U2
    vxor.v    U3,     U3,   U3

    move       L,      $r0   //cycle param k
    beq        L,      TL,    .L33
    blt        TL,     L,     .L33

.L32:  /* for (k=0; k<temp; k++) */
    vld       D0,     A0,    0x00  // a0ri a1ri
    vld       D1,     A0,    0x10  // a2ri a3ri

    vldrepl.w D2,     B0,    0x00 //b0r
    vldrepl.w D3,     B0,    0x04 //b0i

    vpackev.w D4,     D1,    D0
    vshuf4i.w  D4,     D4,    0xd8  //a0r a1r a2r a3r

    vpackod.w D5,     D1,    D0
    vshuf4i.w  D5,     D5,    0xd8  //a0i a1i a2i a3i

    VMADD1    U0,     D4,    D2,     U0  //00r 01r 02r 03r
    VMADD2    U1,     D5,    D2,     U1  //00i 01i 02i 03i
    VMADD3    U0,     D5,    D3,     U0
    VMADD4    U1,     D4,    D3,     U1

    vld       D0,     A0,    0x20  // a4ri a5ri
    vld       D1,     A0,    0x30  // a6ri a7ri

    vpackev.w D4,     D1,    D0
    vshuf4i.w  D4,     D4,    0xd8  //a4r a5r a6r a7r

    vpackod.w D5,     D1,    D0
    vshuf4i.w  D5,     D5,    0xd8  //a4i a5i a6i a7i

    VMADD1    U2,     D4,    D2,     U2  //04r 05r 06r 07r
    VMADD2    U3,     D5,    D2,     U3  //04i 05i 06i 07i
    VMADD3    U2,     D5,    D3,     U2
    VMADD4    U3,     D4,    D3,     U3

    addi.d     A0,     A0,    0x40
    addi.d     B0,     B0,    0x08

    addi.d     L,      L,     1
    blt        L,      TL,    .L32

.L33:
#if defined(TRMMKERNEL)
    //res00 res01 res02 res03
    vld       D0,     C0,    0x00 //c0: 0 1 2 3
    vld       D1,     C0,    0x10 //c0: 4 5 6 7

    vpackev.w D2,     D1,    D0
    vshuf4i.w  D2,     D2,    0xd8  //0 2 4 6
    vpackod.w D3,     D1,    D0
    vshuf4i.w  D3,     D3,    0xd8  //1 3 5 7

    vfmul.s      D2,    U0,    VALPHAR
    vfmul.s      D3,    U1,    VALPHAR
    VNMSUB      D2,    U1,    VALPHAI, D2
    VFMADD      D3,    U0,    VALPHAI, D3

    vand.v    D4,     D3,   D3  //1 3 5 7
    vpermi.w  D4,     D2,   0x44 //0 2 1 3
    vshuf4i.w  D4,     D4,   0xd8 //0 1 2 3

    vand.v    D5,     D3,   D3  //1 3 5 7
    vpermi.w  D5,     D2,   0xee //4 6 5 7
    vshuf4i.w  D5,     D5,   0xd8 //4 5 6 7

    vst       D4,     C0,    0x00
    vst       D5,     C0,    0x10

    //res04 res05 res06 res07
    vld       D0,     C0,    0x20 //c0: 8 9 10 11
    vld       D1,     C0,    0x30 //c0: 12 13 14 15

    vpackev.w D2,     D1,    D0
    vshuf4i.w  D2,     D2,    0xd8  //8 10 12 14
    vpackod.w D3,     D1,    D0
    vshuf4i.w  D3,     D3,    0xd8  //9 11 13 15

    vfmul.s      D2,    U2,    VALPHAR
    vfmul.s      D3,    U3,    VALPHAR
    VNMSUB      D2,    U3,    VALPHAI, D2
    VFMADD      D3,    U2,    VALPHAI, D3

    vand.v    D4,     D3,   D3  //8 10 12 14
    vpermi.w  D4,     D2,   0x44 //8 10 9 11
    vshuf4i.w  D4,     D4,   0xd8 //8 9 10 11

    vand.v    D5,     D3,   D3  //9 11 13 15
    vpermi.w  D5,     D2,   0xee //12 14 13 15
    vshuf4i.w  D5,     D5,   0xd8 //12 13 14 15

    vst       D4,     C0,    0x20
    vst       D5,     C0,    0x30

    addi.d     C0,     C0,    0x40
#else
    //res00 res01 res02 res03
    vld       D0,     C0,    0x00 //c0: 0 1 2 3
    vld       D1,     C0,    0x10 //c0: 4 5 6 7

    vpackev.w D2,     D1,    D0
    vshuf4i.w  D2,     D2,    0xd8  //0 2 4 6
    vpackod.w D3,     D1,    D0
    vshuf4i.w  D3,     D3,    0xd8  //1 3 5 7

    VFMADD      D2,    U0,    VALPHAR, D2
    VFMADD      D3,    U1,    VALPHAR, D3
    VNMSUB      D2,    U1,    VALPHAI, D2
    VFMADD      D3,    U0,    VALPHAI, D3

    vand.v    D4,     D3,   D3  //1 3 5 7
    vpermi.w  D4,     D2,   0x44 //0 2 1 3
    vshuf4i.w  D4,     D4,   0xd8 //0 1 2 3

    vand.v    D5,     D3,   D3  //1 3 5 7
    vpermi.w  D5,     D2,   0xee //4 6 5 7
    vshuf4i.w  D5,     D5,   0xd8 //4 5 6 7

    vst       D4,     C0,    0x00
    vst       D5,     C0,    0x10

    //res04 res05 res06 res07
    vld       D0,     C0,    0x20 //c0: 8 9 10 11
    vld       D1,     C0,    0x30 //c0: 12 13 14 15

    vpackev.w D2,     D1,    D0
    vshuf4i.w  D2,     D2,    0xd8  //8 10 12 14
    vpackod.w D3,     D1,    D0
    vshuf4i.w  D3,     D3,    0xd8  //9 11 13 15

    VFMADD      D2,    U2,    VALPHAR, D2
    VFMADD      D3,    U3,    VALPHAR, D3
    VNMSUB      D2,    U3,    VALPHAI, D2
    VFMADD      D3,    U2,    VALPHAI, D3

    vand.v    D4,     D3,   D3  //8 10 12 14
    vpermi.w  D4,     D2,   0x44 //8 10 9 11
    vshuf4i.w  D4,     D4,   0xd8 //8 9 10 11

    vand.v    D5,     D3,   D3  //9 11 13 15
    vpermi.w  D5,     D2,   0xee //12 14 13 15
    vshuf4i.w  D5,     D5,   0xd8 //12 13 14 15

    vst       D4,     C0,    0x20
    vst       D5,     C0,    0x30

    addi.d     C0,     C0,    0x40
#endif

#if defined(TRMMKERNEL)

#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d      TL,     K,    OFF
#ifdef LEFT
    addi.d     TL,     TL,   -8
#else
    addi.d     TL,     TL,   -1
#endif
    slli.d     T3,     TL,   0x06
    add.d      A0,     A0,   T3
    slli.d     T3,     TL,   0x03
    add.d      B0,     B0,   T3
#endif

#ifdef LEFT
    addi.d     OFF,    OFF,   8
#endif

#endif   // #if defined(TRMMKERNEL)

    addi.d     I,      I,     1
    blt        I,      T0,    .L31

.L34:   /* if ( bm & 4 ) */
    move       I,      $r0
    andi       T1,     M,     4    //bm&4
    beq        I,      T1,    .L38

.L35:
    move       B0,     B      //ptrbb
    move       TL,     K      /* TL = bk */
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) ||(!defined(LEFT) && !defined(TRANSA))
    move       B0,     B    //ptrbb
#else
    slli.d     T3,     OFF,   0x05
    add.d      A0,     A0,    T3
    slli.d     T3,     OFF,   0x03
    add.d      B0,     B,     T3
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    sub.d      TL,     K,     OFF
#elif defined(LEFT)
    addi.d     TL,     OFF,   4
#else
    addi.d     TL,     OFF,   1
#endif

#endif  // #if defined(TRMMKERNEL)

    vxor.v    U0,     U0,   U0
    vxor.v    U1,     U1,   U1

    move       L,      $r0   //cycle param k
    beq        L,      TL,    .L37
    blt        TL,     L,     .L37

.L36:  /* for (k=0; k<temp; k++) */
    vld       D0,     A0,    0x00  // a0ri a1ri
    vld       D1,     A0,    0x10  // a2ri a3ri

    vldrepl.w D2,     B0,    0x00 //b0r
    vldrepl.w D3,     B0,    0x04 //b0i

    vpackev.w D4,     D1,    D0
    vshuf4i.w  D4,     D4,    0xd8  //a0r a1r a2r a3r

    vpackod.w D5,     D1,    D0
    vshuf4i.w  D5,     D5,    0xd8  //a0i a1i a2i a3i

    VMADD1    U0,     D4,    D2,     U0  //00r 01r 02r 03r
    VMADD2    U1,     D5,    D2,     U1  //00i 01i 02i 03i
    VMADD3    U0,     D5,    D3,     U0
    VMADD4    U1,     D4,    D3,     U1

    addi.d     A0,     A0,    0x20
    addi.d     B0,     B0,    0x08

    addi.d     L,      L,     1
    blt        L,      TL,    .L36

.L37:
#if defined(TRMMKERNEL)
    //res00 res01 res02 res03
    vld       D0,     C0,    0x00 //c0: 0 1 2 3
    vld       D1,     C0,    0x10 //c0: 4 5 6 7

    vpackev.w D2,     D1,    D0
    vshuf4i.w  D2,     D2,    0xd8  //0 2 4 6
    vpackod.w D3,     D1,    D0
    vshuf4i.w  D3,     D3,    0xd8  //1 3 5 7

    vfmul.s      D2,    U0,    VALPHAR
    vfmul.s      D3,    U1,    VALPHAR
    VNMSUB      D2,    U1,    VALPHAI, D2
    VFMADD      D3,    U0,    VALPHAI, D3

    vand.v    D4,     D3,   D3  //1 3 5 7
    vpermi.w  D4,     D2,   0x44 //0 2 1 3
    vshuf4i.w  D4,     D4,   0xd8 //0 1 2 3

    vand.v    D5,     D3,   D3  //1 3 5 7
    vpermi.w  D5,     D2,   0xee //4 6 5 7
    vshuf4i.w  D5,     D5,   0xd8 //4 5 6 7

    vst       D4,     C0,    0x00
    vst       D5,     C0,    0x10

    addi.d     C0,     C0,    0x20
#else
    //res00 res01 res02 res03
    vld       D0,     C0,    0x00 //c0: 0 1 2 3
    vld       D1,     C0,    0x10 //c0: 4 5 6 7

    vpackev.w D2,     D1,    D0
    vshuf4i.w  D2,     D2,    0xd8  //0 2 4 6
    vpackod.w D3,     D1,    D0
    vshuf4i.w  D3,     D3,    0xd8  //1 3 5 7

    VFMADD      D2,    U0,    VALPHAR, D2
    VFMADD      D3,    U1,    VALPHAR, D3
    VNMSUB      D2,    U1,    VALPHAI, D2
    VFMADD      D3,    U0,    VALPHAI, D3

    vand.v    D4,     D3,   D3  //1 3 5 7
    vpermi.w  D4,     D2,   0x44 //0 2 1 3
    vshuf4i.w  D4,     D4,   0xd8 //0 1 2 3

    vand.v    D5,     D3,   D3  //1 3 5 7
    vpermi.w  D5,     D2,   0xee //4 6 5 7
    vshuf4i.w  D5,     D5,   0xd8 //4 5 6 7

    vst       D4,     C0,    0x00
    vst       D5,     C0,    0x10

    addi.d     C0,     C0,    0x20
#endif

#if defined(TRMMKERNEL)

#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d      TL,     K,    OFF
#ifdef LEFT
    addi.d     TL,     TL,   -4
#else
    addi.d     TL,     TL,   -1
#endif
    slli.d     T3,     TL,   0x05
    add.d      A0,     A0,   T3
    slli.d     T3,     TL,   0x03
    add.d      B0,     B0,   T3
#endif

#ifdef LEFT
    addi.d     OFF,    OFF,   4
#endif

#endif   // #if defined(TRMMKERNEL)

.L38:   /* if ( bm & 2 ) */
    move       I,      $r0
    andi       T1,     M,     2    //bm&2
    beq        I,      T1,    .L312

.L39:
    move       B0,     B      //ptrbb
    move       TL,     K      /* TL = bk */
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) ||(!defined(LEFT) && !defined(TRANSA))
    move       B0,     B    //ptrbb
#else
    slli.d     T3,     OFF,   0x04
    add.d      A0,     A0,    T3
    slli.d     T3,     OFF,   0x03
    add.d      B0,     B,     T3
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
    beq        L,      TL,    .L311
    blt        TL,     L,     .L311

.L310:  /* for (k=0; k<temp; k++) */
    LD         a1,     A0,    0x00        //a0r
    LD         a2,     A0,    0x04        //a0i
    LD         a3,     A0,    0x08        //a1r
    LD         a4,     A0,    0x0c        //a1i

    LD         b1,     B0,    0x00        //b0r
    LD         b2,     B0,    0x04        //b0i

    MADD1      c11,    a1,    b1,     c11  //res00r
    MADD2      c12,    a2,    b1,     c12  //res00i
    MADD3      c11,    a2,    b2,     c11
    MADD4      c12,    a1,    b2,     c12

    MADD1      c21,    a3,    b1,     c21  //res10r
    MADD2      c22,    a4,    b1,     c22  //res10i
    MADD3      c21,    a4,    b2,     c21
    MADD4      c22,    a3,    b2,     c22

    addi.d     A0,     A0,    0x10
    addi.d     B0,     B0,    0x08

    addi.d     L,      L,     1
    blt        L,      TL,    .L310

.L311:
#if defined(TRMMKERNEL)
    //res00 res10
    LD         a5,     C0,    0x00    //C0[0]
    LD         a6,     C0,    0x04    //C0[1]
    LD         a7,     C0,    0x08    //C0[2]
    LD         a8,     C0,    0x0c    //C0[3]

    MUL       a5,     c11,   ALPHA_R
    MUL       a6,     c12,   ALPHA_R
    NMSUB      a5,     c12,   ALPHA_I, a5
    MADD       a6,     c11,   ALPHA_I, a6

    MUL       a7,     c21,   ALPHA_R
    MUL       a8,     c22,   ALPHA_R
    NMSUB      a7,     c22,   ALPHA_I, a7
    MADD       a8,     c21,   ALPHA_I, a8

    ST         a5,     C0,    0x00
    ST         a6,     C0,    0x04

    ST         a7,     C0,    0x08
    ST         a8,     C0,    0x0c

    addi.d     C0,     C0,    0x10
#else
    //res00 res10
    LD         a5,     C0,    0x00    //C0[0]
    LD         a6,     C0,    0x04    //C0[1]
    LD         a7,     C0,    0x08    //C0[2]
    LD         a8,     C0,    0x0c    //C0[3]

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

    addi.d     C0,     C0,    0x10
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d      TL,     K,    OFF
#ifdef LEFT
    addi.d     TL,     TL,   -2
#else
    addi.d     TL,     TL,   -1
#endif
    slli.d     T3,     TL,   0x04
    add.d      A0,     A0,   T3
    slli.d     T3,     TL,   0x03
    add.d      B0,     B0,   T3
#endif

#ifdef LEFT
    addi.d     OFF,    OFF,   2
#endif
#endif   // #if defined(TRMMKERNEL)

.L312:   /* if ( bm & 1 )*/
    move       I,      $r0
    andi       T1,     M,     1    //bm&1
    beq        I,      T1,    .L316

.L313:
    move       B0,     B      //ptrbb
    move       TL,     K      /* TL = bk */
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) ||(!defined(LEFT) && !defined(TRANSA))
    move       B0,     B    //ptrbb
#else
    slli.d     T3,     OFF,   0x03
    add.d      A0,     A0,    T3
    slli.d     T3,     OFF,   0x03
    add.d      B0,     B,     T3
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
    beq        L,      TL,    .L315
    blt        TL,     L,     .L315

.L314:  /* for (k=0; k<temp; k++) */
    LD         a1,     A0,    0x00
    LD         a2,     A0,    0x04

    LD         b1,     B0,    0x00
    LD         b2,     B0,    0x04

    MADD1      c11,    a1,    b1,     c11
    MADD2      c12,    a2,    b1,     c12
    MADD3      c11,    a2,    b2,     c11
    MADD4      c12,    a1,    b2,     c12

    addi.d     A0,     A0,    0x08
    addi.d     B0,     B0,    0x08

    addi.d     L,      L,     1
    blt        L,      TL,    .L314

.L315:
#if defined(TRMMKERNEL)
    MUL        a5,     c11,   ALPHA_R
    MUL        a6,     c12,   ALPHA_I
    SUB        a5,     a5,    a6
    ST         a5,     C0,    0x00

    MUL        a5,     c12,   ALPHA_R
    MUL        a6,     c11,   ALPHA_I
    ADD        a6,     a5,    a6
    ST         a6,     C0,    0x04

    addi.d     C0,     C0,    0x08
#else
    LD         a5,     C0,    0x00    //C0[0]
    LD         a6,     C0,    0x04    //C0[1]

    MADD       a5,     c11,   ALPHA_R, a5
    MADD       a6,     c12,   ALPHA_R, a6
    NMSUB      a5,     c12,   ALPHA_I, a5
    MADD       a6,     c11,   ALPHA_I, a6

    ST         a5,     C0,    0x00
    ST         a6,     C0,    0x04

    addi.d     C0,     C0,    0x08
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d      TL,     K,    OFF
#ifdef LEFT
    addi.d     TL,     TL,   -1
#else
    addi.d     TL,     TL,   -1
#endif
    slli.d     T3,     TL,   0x03
    add.d      A0,     A0,   T3
    add.d      B0,     B0,   T3
#endif

#ifdef LEFT
    addi.d     OFF,    OFF,   1
#endif
#endif   // #if defined(TRMMKERNEL)

.L316:
    slli.d     L,      K,     3
    add.d      B,      B,     L

    slli.d     I,      LDC,   1
    add.d      C,      C,     I

    addi.d     J,      J,     1
    andi       T0,     N,     1
    blt        J,      T0,    .L300

.L999:
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