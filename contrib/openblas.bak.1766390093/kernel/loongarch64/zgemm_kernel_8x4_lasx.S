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
#define    XVMADD1       XVFMADD
#define    XVMADD2       XVFMADD
#define    XVMADD3       XVFMADD
#define    XVMADD4       XVNMSUB

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
#define    XVMADD1       XVFMADD
#define    XVMADD2       XVNMSUB
#define    XVMADD3       XVFMADD
#define    XVMADD4       XVFMADD

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
#define    XVMADD1       XVFMADD
#define    XVMADD2       XVNMSUB
#define    XVMADD3       XVNMSUB
#define    XVMADD4       XVNMSUB

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
    fst.d         $f23,   $sp,   40
    fst.d         $f24,   $sp,   48
    fst.d         $f25,   $sp,   56
    fst.d         $f26,   $sp,   64
    fst.d         $f27,   $sp,   72
    fst.d         $f28,   $sp,   80
    fst.d         $f29,   $sp,   88
    fst.d         $f30,   $sp,   96
    fst.d         $f31,   $sp,   104
    fst.d         ALPHA_R,$sp,   112
    fst.d         ALPHA_I,$sp,   120

    xvldrepl.d  VALPHAR, $sp, 112
    xvldrepl.d  VALPHAI, $sp, 120

#if defined (TRMMKERNEL) && !defined(LEFT)
    sub.d      OFF,    $r0,   OFFSET
#else
    xor        OFF,    OFF,   OFF
#endif

    slli.d     LDC,    LDC,   BASE_SHIFT

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
    slli.d     T3,     OFF,   0x07
    add.d      A0,     A0,    T3
    slli.d     T3,     OFF,   0x06
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

    xvxor.v    U0,     U0,   U0
    xvxor.v    U1,     U1,   U1
    xvxor.v    U2,     U2,   U2
    xvxor.v    U3,     U3,   U3
    xvxor.v    U4,     U4,   U4
    xvxor.v    U5,     U5,   U5
    xvxor.v    U6,     U6,   U6
    xvxor.v    U7,     U7,   U7
    xvxor.v    U8,     U8,   U8
    xvxor.v    U9,     U9,   U9
    xvxor.v    U10,    U10,  U10
    xvxor.v    U11,    U11,  U11
    xvxor.v    U12,    U12,  U12
    xvxor.v    U13,    U13,  U13
    xvxor.v    U14,    U14,  U14
    xvxor.v    U15,    U15,  U15

    move       L,      $r0   //cycle param k
    beq        L,      TL,    .L13
    blt        TL,     L,     .L13

.L12:  /* for(k=0; k<temp; k+=1) */
    xvld       D0,     A0,    0x00  // a0ri a1ri
    xvld       D2,     B0,    0x00  // b0ri b1ri
    xvld       D3,     B0,    0x20  // b2ri b3ri

    xvpermi.d  D4,     D0,    0x00  //a0r
    xvpermi.d  D5,     D0,    0x55  //a0i

    xvpackev.d D6,     D3,    D2
    xvpermi.d  D6,     D6,    0xd8  //b0r b1r b2r b3r

    xvpackod.d D7,     D3,    D2
    xvpermi.d  D7,     D7,    0xd8  //b0i b1i b2i b3i

    XVMADD1    U0,     D4,    D6,     U0  //00r 10r 20r 30r
    XVMADD2    U1,     D5,    D6,     U1  //00i 10i 20i 30i
    XVMADD3    U0,     D5,    D7,     U0
    XVMADD4    U1,     D4,    D7,     U1

    xvpermi.d  D4,     D0,    0xaa  //a1r
    xvpermi.d  D5,     D0,    0xff  //a1i

    XVMADD1    U2,     D4,    D6,     U2  //01r 11r 21r 31r
    XVMADD2    U3,     D5,    D6,     U3  //01i 11i 21i 31i
    XVMADD3    U2,     D5,    D7,     U2
    XVMADD4    U3,     D4,    D7,     U3

    xvld       D0,     A0,    0x20  // a2ri a3ri

    xvpermi.d  D4,     D0,    0x00  //a2r
    xvpermi.d  D5,     D0,    0x55  //a2i

    XVMADD1    U4,     D4,    D6,     U4  //02r 12r 22r 32r
    XVMADD2    U5,     D5,    D6,     U5  //02i 12i 22i 32i
    XVMADD3    U4,     D5,    D7,     U4
    XVMADD4    U5,     D4,    D7,     U5

    xvpermi.d  D4,     D0,    0xaa  //a3r
    xvpermi.d  D5,     D0,    0xff  //a3i

    XVMADD1    U6,     D4,    D6,     U6  //03r 13r 23r 33r
    XVMADD2    U7,     D5,    D6,     U7  //03i 13i 23i 33i
    XVMADD3    U6,     D5,    D7,     U6
    XVMADD4    U7,     D4,    D7,     U7

    xvld       D0,     A0,    0x40  // a4ri a5ri

    xvpermi.d  D4,     D0,    0x00  //a4r
    xvpermi.d  D5,     D0,    0x55  //a4i

    XVMADD1    U8,     D4,    D6,     U8  //04r 14r 24r 34r
    XVMADD2    U9,     D5,    D6,     U9  //04i 14i 24i 34i
    XVMADD3    U8,     D5,    D7,     U8
    XVMADD4    U9,     D4,    D7,     U9

    xvpermi.d  D4,     D0,    0xaa  //a5r
    xvpermi.d  D5,     D0,    0xff  //a5i

    XVMADD1    U10,     D4,    D6,     U10  //05r 15r 25r 35r
    XVMADD2    U11,     D5,    D6,     U11  //05i 15i 25i 35i
    XVMADD3    U10,     D5,    D7,     U10
    XVMADD4    U11,     D4,    D7,     U11

    xvld       D0,     A0,    0x60  // a6ri a7ri

    xvpermi.d  D4,     D0,    0x00  //a6r
    xvpermi.d  D5,     D0,    0x55  //a6i

    XVMADD1    U12,     D4,    D6,     U12  //06r 16r 26r 36r
    XVMADD2    U13,     D5,    D6,     U13  //06i 16i 26i 36i
    XVMADD3    U12,     D5,    D7,     U12
    XVMADD4    U13,     D4,    D7,     U13

    xvpermi.d  D4,     D0,    0xaa  //a5r
    xvpermi.d  D5,     D0,    0xff  //a5i

    XVMADD1    U14,     D4,    D6,     U14  //07r 17r 27r 37r
    XVMADD2    U15,     D5,    D6,     U15  //07i 17i 27i 37i
    XVMADD3    U14,     D5,    D7,     U14
    XVMADD4    U15,     D4,    D7,     U15

    addi.d     A0,     A0,    0x80
    addi.d     B0,     B0,    0x40

    addi.d     L,      L,     1
    blt        L,      TL,    .L12

.L13:
#if defined(TRMMKERNEL)
    //res00 res10 res20 res30
    xvld       D0,     C0,    0x00 //c0: 0 1 2 3
    xvld       D1,     C1,    0x00 //c1: 0 1 2 3
    xvld       D2,     C2,    0x00 //c2: 0 1 2 3
    xvld       D3,     C3,    0x00 //c3: 0 1 2 3

    xvand.v    D4,     D0,    D0
    xvpermi.q  D4,     D1,    0x02 //c0:0 1, c1:0 1
    xvpermi.d  D6,     D4,    0xd8 //c0[0] c1[0] c0[1] c1[1]
    xvpermi.d  D7,     D4,    0x8d //c0[1] c1[1] c0[0] c1[0]

    xvand.v    D5,     D2,    D2
    xvpermi.q  D5,     D3,    0x02 //c2:0 1, c3:0 1
    xvpermi.d  D8,     D5,    0xd8 //c2[0] c3[0] c2[1] c3[1]
    xvpermi.d  D9,     D5,    0x8d //c2[1] c3[1] c2[0] c3[0]

    xvpermi.q  D6,     D8,    0x02 //c0[0] c1[0] c2[0] c3[0]
    xvpermi.q  D7,     D9,    0x02 //c0[1] c1[1] c2[1] c3[1]

    xvfmul.d      D6,    U0,    VALPHAR
    xvfmul.d      D7,    U1,    VALPHAR
    XVNMSUB      D6,    U1,    VALPHAI, D6
    XVFMADD      D7,    U0,    VALPHAI, D7

    xvand.v    D10,     D6,    D6
    xvpermi.q  D10,     D7,    0x02 //c0[0] c1[0] c0[1] c1[1]
    xvpermi.d  D10,     D10,   0xd8 //c0[0] c0[1] c1[0] c1[1]

    xvand.v    D11,     D7,    D7
    xvpermi.q  D11,     D6,    0x31 //c2[0] c3[0] c2[1] c3[1]
    xvpermi.d  D11,     D11,   0xd8 //c2[0] c2[1] c3[0] c3[1]

    //res01 res11 res21 res31
    xvand.v    D4,     D1,    D1
    xvpermi.q  D4,     D0,    0x31 //c0:2 3, c1:2 3
    xvpermi.d  D6,     D4,    0xd8 //c0[2] c1[2] c0[3] c1[3]
    xvpermi.d  D7,     D4,    0x8d //c0[3] c1[3] c0[2] c1[2]

    xvand.v    D5,     D3,    D3
    xvpermi.q  D5,     D2,    0x31 //c2:2 3, c3:2 3
    xvpermi.d  D8,     D5,    0xd8 //c2[2] c3[2] c2[3] c3[3]
    xvpermi.d  D9,     D5,    0x8d //c2[3] c3[3] c2[2] c3[2]

    xvpermi.q  D6,     D8,    0x02 //c0[2] c1[2] c2[2] c3[2]
    xvpermi.q  D7,     D9,    0x02 //c0[3] c1[3] c2[3] c3[3]

    xvfmul.d      D6,    U2,    VALPHAR
    xvfmul.d      D7,    U3,    VALPHAR
    XVNMSUB      D6,    U3,    VALPHAI, D6
    XVFMADD      D7,    U2,    VALPHAI, D7

    xvand.v    D4,     D6,    D6
    xvpermi.q  D4,     D7,    0x02 //c0[2] c1[2] c0[3] c1[3]
    xvpermi.d  D4,     D4,    0xd8 //c0[2] c0[3] c1[2] c1[3]

    xvand.v    D5,     D7,    D7
    xvpermi.q  D5,     D6,    0x31 //c2[2] c3[2] c2[3] c3[3]
    xvpermi.d  D5,     D5,    0xd8 //c2[2] c2[3] c3[2] c3[3]

    xvand.v    D0,     D10,    D10
    xvand.v    D1,     D11,    D11

    xvpermi.q  D0,     D4,     0x02 //c0: 0 1 2 3
    xvpermi.q  D4,     D10,    0x31 //c1: 0 1 2 3
    xvpermi.q  D1,     D5,     0x02 //c2: 0 1 2 3
    xvpermi.q  D5,     D11,    0x31 //c3: 0 1 2 3

    xvst       D0,     C0,    0x00
    xvst       D4,     C1,    0x00
    xvst       D1,     C2,    0x00
    xvst       D5,     C3,    0x00

    addi.d     C0,     C0,    0x20
    addi.d     C1,     C1,    0x20
    addi.d     C2,     C2,    0x20
    addi.d     C3,     C3,    0x20

    //res02 res12 res22 res32
    xvld       D0,     C0,    0x00 //c0: 0 1 2 3
    xvld       D1,     C1,    0x00 //c1: 0 1 2 3
    xvld       D2,     C2,    0x00 //c2: 0 1 2 3
    xvld       D3,     C3,    0x00 //c3: 0 1 2 3

    xvand.v    D4,     D0,    D0
    xvpermi.q  D4,     D1,    0x02 //c0:0 1, c1:0 1
    xvpermi.d  D6,     D4,    0xd8 //c0[0] c1[0] c0[1] c1[1]
    xvpermi.d  D7,     D4,    0x8d //c0[1] c1[1] c0[0] c1[0]

    xvand.v    D5,     D2,    D2
    xvpermi.q  D5,     D3,    0x02 //c2:0 1, c3:0 1
    xvpermi.d  D8,     D5,    0xd8 //c2[0] c3[0] c2[1] c3[1]
    xvpermi.d  D9,     D5,    0x8d //c2[1] c3[1] c2[0] c3[0]

    xvpermi.q  D6,     D8,    0x02 //c0[0] c1[0] c2[0] c3[0]
    xvpermi.q  D7,     D9,    0x02 //c0[1] c1[1] c2[1] c3[1]

    xvfmul.d      D6,    U4,    VALPHAR
    xvfmul.d      D7,    U5,    VALPHAR
    XVNMSUB      D6,    U5,    VALPHAI, D6
    XVFMADD      D7,    U4,    VALPHAI, D7

    xvand.v    D10,     D6,    D6
    xvpermi.q  D10,     D7,    0x02 //c0[0] c1[0] c0[1] c1[1]
    xvpermi.d  D10,     D10,   0xd8 //c0[0] c0[1] c1[0] c1[1]

    xvand.v    D11,     D7,    D7
    xvpermi.q  D11,     D6,    0x31 //c2[0] c3[0] c2[1] c3[1]
    xvpermi.d  D11,     D11,   0xd8 //c2[0] c2[1] c3[0] c3[1]

    //res03 res13 res23 res33
    xvand.v    D4,     D1,    D1
    xvpermi.q  D4,     D0,    0x31 //c0:2 3, c1:2 3
    xvpermi.d  D6,     D4,    0xd8 //c0[2] c1[2] c0[3] c1[3]
    xvpermi.d  D7,     D4,    0x8d //c0[3] c1[3] c0[2] c1[2]

    xvand.v    D5,     D3,    D3
    xvpermi.q  D5,     D2,    0x31 //c2:2 3, c3:2 3
    xvpermi.d  D8,     D5,    0xd8 //c2[2] c3[2] c2[3] c3[3]
    xvpermi.d  D9,     D5,    0x8d //c2[3] c3[3] c2[2] c3[2]

    xvpermi.q  D6,     D8,    0x02 //c0[2] c1[2] c2[2] c3[2]
    xvpermi.q  D7,     D9,    0x02 //c0[3] c1[3] c2[3] c3[3]

    xvfmul.d      D6,    U6,    VALPHAR
    xvfmul.d      D7,    U7,    VALPHAR
    XVNMSUB      D6,    U7,    VALPHAI, D6
    XVFMADD      D7,    U6,    VALPHAI, D7

    xvand.v    D4,     D6,    D6
    xvpermi.q  D4,     D7,    0x02 //c0[2] c1[2] c0[3] c1[3]
    xvpermi.d  D4,     D4,    0xd8 //c0[2] c0[3] c1[2] c1[3]

    xvand.v    D5,     D7,    D7
    xvpermi.q  D5,     D6,    0x31 //c2[2] c3[2] c2[3] c3[3]
    xvpermi.d  D5,     D5,    0xd8 //c2[2] c2[3] c3[2] c3[3]

    xvand.v    D0,     D10,    D10
    xvand.v    D1,     D11,    D11

    xvpermi.q  D0,     D4,     0x02 //c0: 0 1 2 3
    xvpermi.q  D4,     D10,    0x31 //c1: 0 1 2 3
    xvpermi.q  D1,     D5,     0x02 //c2: 0 1 2 3
    xvpermi.q  D5,     D11,    0x31 //c3: 0 1 2 3

    xvst       D0,     C0,    0x00
    xvst       D4,     C1,    0x00
    xvst       D1,     C2,    0x00
    xvst       D5,     C3,    0x00

    addi.d     C0,     C0,    0x20
    addi.d     C1,     C1,    0x20
    addi.d     C2,     C2,    0x20
    addi.d     C3,     C3,    0x20

    //res04 res14 res24 res34
    xvld       D0,     C0,    0x00 //c0: 0 1 2 3
    xvld       D1,     C1,    0x00 //c1: 0 1 2 3
    xvld       D2,     C2,    0x00 //c2: 0 1 2 3
    xvld       D3,     C3,    0x00 //c3: 0 1 2 3

    xvand.v    D4,     D0,    D0
    xvpermi.q  D4,     D1,    0x02 //c0:0 1, c1:0 1
    xvpermi.d  D6,     D4,    0xd8 //c0[0] c1[0] c0[1] c1[1]
    xvpermi.d  D7,     D4,    0x8d //c0[1] c1[1] c0[0] c1[0]

    xvand.v    D5,     D2,    D2
    xvpermi.q  D5,     D3,    0x02 //c2:0 1, c3:0 1
    xvpermi.d  D8,     D5,    0xd8 //c2[0] c3[0] c2[1] c3[1]
    xvpermi.d  D9,     D5,    0x8d //c2[1] c3[1] c2[0] c3[0]

    xvpermi.q  D6,     D8,    0x02 //c0[0] c1[0] c2[0] c3[0]
    xvpermi.q  D7,     D9,    0x02 //c0[1] c1[1] c2[1] c3[1]

    xvfmul.d      D6,    U8,    VALPHAR
    xvfmul.d      D7,    U9,    VALPHAR
    XVNMSUB      D6,    U9,    VALPHAI, D6
    XVFMADD      D7,    U8,    VALPHAI, D7

    xvand.v    D10,     D6,    D6
    xvpermi.q  D10,     D7,    0x02 //c0[0] c1[0] c0[1] c1[1]
    xvpermi.d  D10,     D10,   0xd8 //c0[0] c0[1] c1[0] c1[1]

    xvand.v    D11,     D7,    D7
    xvpermi.q  D11,     D6,    0x31 //c2[0] c3[0] c2[1] c3[1]
    xvpermi.d  D11,     D11,   0xd8 //c2[0] c2[1] c3[0] c3[1]

    //res05 res15 res25 res35
    xvand.v    D4,     D1,    D1
    xvpermi.q  D4,     D0,    0x31 //c0:2 3, c1:2 3
    xvpermi.d  D6,     D4,    0xd8 //c0[2] c1[2] c0[3] c1[3]
    xvpermi.d  D7,     D4,    0x8d //c0[3] c1[3] c0[2] c1[2]

    xvand.v    D5,     D3,    D3
    xvpermi.q  D5,     D2,    0x31 //c2:2 3, c3:2 3
    xvpermi.d  D8,     D5,    0xd8 //c2[2] c3[2] c2[3] c3[3]
    xvpermi.d  D9,     D5,    0x8d //c2[3] c3[3] c2[2] c3[2]

    xvpermi.q  D6,     D8,    0x02 //c0[2] c1[2] c2[2] c3[2]
    xvpermi.q  D7,     D9,    0x02 //c0[3] c1[3] c2[3] c3[3]

    xvfmul.d      D6,    U10,    VALPHAR
    xvfmul.d      D7,    U11,    VALPHAR
    XVNMSUB      D6,    U11,    VALPHAI, D6
    XVFMADD      D7,    U10,    VALPHAI, D7

    xvand.v    D4,     D6,    D6
    xvpermi.q  D4,     D7,    0x02 //c0[2] c1[2] c0[3] c1[3]
    xvpermi.d  D4,     D4,    0xd8 //c0[2] c0[3] c1[2] c1[3]

    xvand.v    D5,     D7,    D7
    xvpermi.q  D5,     D6,    0x31 //c2[2] c3[2] c2[3] c3[3]
    xvpermi.d  D5,     D5,    0xd8 //c2[2] c2[3] c3[2] c3[3]

    xvand.v    D0,     D10,    D10
    xvand.v    D1,     D11,    D11

    xvpermi.q  D0,     D4,     0x02 //c0: 0 1 2 3
    xvpermi.q  D4,     D10,    0x31 //c1: 0 1 2 3
    xvpermi.q  D1,     D5,     0x02 //c2: 0 1 2 3
    xvpermi.q  D5,     D11,    0x31 //c3: 0 1 2 3

    xvst       D0,     C0,    0x00
    xvst       D4,     C1,    0x00
    xvst       D1,     C2,    0x00
    xvst       D5,     C3,    0x00

    addi.d     C0,     C0,    0x20
    addi.d     C1,     C1,    0x20
    addi.d     C2,     C2,    0x20
    addi.d     C3,     C3,    0x20

    //res06 res16 res26 res36
    xvld       D0,     C0,    0x00 //c0: 0 1 2 3
    xvld       D1,     C1,    0x00 //c1: 0 1 2 3
    xvld       D2,     C2,    0x00 //c2: 0 1 2 3
    xvld       D3,     C3,    0x00 //c3: 0 1 2 3

    xvand.v    D4,     D0,    D0
    xvpermi.q  D4,     D1,    0x02 //c0:0 1, c1:0 1
    xvpermi.d  D6,     D4,    0xd8 //c0[0] c1[0] c0[1] c1[1]
    xvpermi.d  D7,     D4,    0x8d //c0[1] c1[1] c0[0] c1[0]

    xvand.v    D5,     D2,    D2
    xvpermi.q  D5,     D3,    0x02 //c2:0 1, c3:0 1
    xvpermi.d  D8,     D5,    0xd8 //c2[0] c3[0] c2[1] c3[1]
    xvpermi.d  D9,     D5,    0x8d //c2[1] c3[1] c2[0] c3[0]

    xvpermi.q  D6,     D8,    0x02 //c0[0] c1[0] c2[0] c3[0]
    xvpermi.q  D7,     D9,    0x02 //c0[1] c1[1] c2[1] c3[1]

    xvfmul.d      D6,    U12,    VALPHAR
    xvfmul.d      D7,    U13,    VALPHAR
    XVNMSUB      D6,    U13,    VALPHAI, D6
    XVFMADD      D7,    U12,    VALPHAI, D7

    xvand.v    D10,     D6,    D6
    xvpermi.q  D10,     D7,    0x02 //c0[0] c1[0] c0[1] c1[1]
    xvpermi.d  D10,     D10,   0xd8 //c0[0] c0[1] c1[0] c1[1]

    xvand.v    D11,     D7,    D7
    xvpermi.q  D11,     D6,    0x31 //c2[0] c3[0] c2[1] c3[1]
    xvpermi.d  D11,     D11,   0xd8 //c2[0] c2[1] c3[0] c3[1]

    //res07 res17 res27 res37
    xvand.v    D4,     D1,    D1
    xvpermi.q  D4,     D0,    0x31 //c0:2 3, c1:2 3
    xvpermi.d  D6,     D4,    0xd8 //c0[2] c1[2] c0[3] c1[3]
    xvpermi.d  D7,     D4,    0x8d //c0[3] c1[3] c0[2] c1[2]

    xvand.v    D5,     D3,    D3
    xvpermi.q  D5,     D2,    0x31 //c2:2 3, c3:2 3
    xvpermi.d  D8,     D5,    0xd8 //c2[2] c3[2] c2[3] c3[3]
    xvpermi.d  D9,     D5,    0x8d //c2[3] c3[3] c2[2] c3[2]

    xvpermi.q  D6,     D8,    0x02 //c0[2] c1[2] c2[2] c3[2]
    xvpermi.q  D7,     D9,    0x02 //c0[3] c1[3] c2[3] c3[3]

    xvfmul.d      D6,    U14,    VALPHAR
    xvfmul.d      D7,    U15,    VALPHAR
    XVNMSUB      D6,    U15,    VALPHAI, D6
    XVFMADD      D7,    U14,    VALPHAI, D7

    xvand.v    D4,     D6,    D6
    xvpermi.q  D4,     D7,    0x02 //c0[2] c1[2] c0[3] c1[3]
    xvpermi.d  D4,     D4,    0xd8 //c0[2] c0[3] c1[2] c1[3]

    xvand.v    D5,     D7,    D7
    xvpermi.q  D5,     D6,    0x31 //c2[2] c3[2] c2[3] c3[3]
    xvpermi.d  D5,     D5,    0xd8 //c2[2] c2[3] c3[2] c3[3]

    xvand.v    D0,     D10,    D10
    xvand.v    D1,     D11,    D11

    xvpermi.q  D0,     D4,     0x02 //c0: 0 1 2 3
    xvpermi.q  D4,     D10,    0x31 //c1: 0 1 2 3
    xvpermi.q  D1,     D5,     0x02 //c2: 0 1 2 3
    xvpermi.q  D5,     D11,    0x31 //c3: 0 1 2 3

    xvst       D0,     C0,    0x00
    xvst       D4,     C1,    0x00
    xvst       D1,     C2,    0x00
    xvst       D5,     C3,    0x00

    addi.d     C0,     C0,    0x20
    addi.d     C1,     C1,    0x20
    addi.d     C2,     C2,    0x20
    addi.d     C3,     C3,    0x20
#else
    //res00 res10 res20 res30
    xvld       D0,     C0,    0x00 //c0: 0 1 2 3
    xvld       D1,     C1,    0x00 //c1: 0 1 2 3
    xvld       D2,     C2,    0x00 //c2: 0 1 2 3
    xvld       D3,     C3,    0x00 //c3: 0 1 2 3

    xvand.v    D4,     D0,    D0
    xvpermi.q  D4,     D1,    0x02 //c0:0 1, c1:0 1
    xvpermi.d  D6,     D4,    0xd8 //c0[0] c1[0] c0[1] c1[1]
    xvpermi.d  D7,     D4,    0x8d //c0[1] c1[1] c0[0] c1[0]

    xvand.v    D5,     D2,    D2
    xvpermi.q  D5,     D3,    0x02 //c2:0 1, c3:0 1
    xvpermi.d  D8,     D5,    0xd8 //c2[0] c3[0] c2[1] c3[1]
    xvpermi.d  D9,     D5,    0x8d //c2[1] c3[1] c2[0] c3[0]

    xvpermi.q  D6,     D8,    0x02 //c0[0] c1[0] c2[0] c3[0]
    xvpermi.q  D7,     D9,    0x02 //c0[1] c1[1] c2[1] c3[1]

    XVFMADD      D6,    U0,    VALPHAR, D6
    XVFMADD      D7,    U1,    VALPHAR, D7
    XVNMSUB      D6,    U1,    VALPHAI, D6
    XVFMADD      D7,    U0,    VALPHAI, D7

    xvand.v    D10,     D6,    D6
    xvpermi.q  D10,     D7,    0x02 //c0[0] c1[0] c0[1] c1[1]
    xvpermi.d  D10,     D10,   0xd8 //c0[0] c0[1] c1[0] c1[1]

    xvand.v    D11,     D7,    D7
    xvpermi.q  D11,     D6,    0x31 //c2[0] c3[0] c2[1] c3[1]
    xvpermi.d  D11,     D11,   0xd8 //c2[0] c2[1] c3[0] c3[1]

    //res01 res11 res21 res31
    xvand.v    D4,     D1,    D1
    xvpermi.q  D4,     D0,    0x31 //c0:2 3, c1:2 3
    xvpermi.d  D6,     D4,    0xd8 //c0[2] c1[2] c0[3] c1[3]
    xvpermi.d  D7,     D4,    0x8d //c0[3] c1[3] c0[2] c1[2]

    xvand.v    D5,     D3,    D3
    xvpermi.q  D5,     D2,    0x31 //c2:2 3, c3:2 3
    xvpermi.d  D8,     D5,    0xd8 //c2[2] c3[2] c2[3] c3[3]
    xvpermi.d  D9,     D5,    0x8d //c2[3] c3[3] c2[2] c3[2]

    xvpermi.q  D6,     D8,    0x02 //c0[2] c1[2] c2[2] c3[2]
    xvpermi.q  D7,     D9,    0x02 //c0[3] c1[3] c2[3] c3[3]

    XVFMADD      D6,    U2,    VALPHAR, D6
    XVFMADD      D7,    U3,    VALPHAR, D7
    XVNMSUB      D6,    U3,    VALPHAI, D6
    XVFMADD      D7,    U2,    VALPHAI, D7

    xvand.v    D4,     D6,    D6
    xvpermi.q  D4,     D7,    0x02 //c0[2] c1[2] c0[3] c1[3]
    xvpermi.d  D4,     D4,    0xd8 //c0[2] c0[3] c1[2] c1[3]

    xvand.v    D5,     D7,    D7
    xvpermi.q  D5,     D6,    0x31 //c2[2] c3[2] c2[3] c3[3]
    xvpermi.d  D5,     D5,    0xd8 //c2[2] c2[3] c3[2] c3[3]

    xvand.v    D0,     D10,    D10
    xvand.v    D1,     D11,    D11

    xvpermi.q  D0,     D4,     0x02 //c0: 0 1 2 3
    xvpermi.q  D4,     D10,    0x31 //c1: 0 1 2 3
    xvpermi.q  D1,     D5,     0x02 //c2: 0 1 2 3
    xvpermi.q  D5,     D11,    0x31 //c3: 0 1 2 3

    xvst       D0,     C0,    0x00
    xvst       D4,     C1,    0x00
    xvst       D1,     C2,    0x00
    xvst       D5,     C3,    0x00

    addi.d     C0,     C0,    0x20
    addi.d     C1,     C1,    0x20
    addi.d     C2,     C2,    0x20
    addi.d     C3,     C3,    0x20

    //res02 res12 res22 res32
    xvld       D0,     C0,    0x00 //c0: 0 1 2 3
    xvld       D1,     C1,    0x00 //c1: 0 1 2 3
    xvld       D2,     C2,    0x00 //c2: 0 1 2 3
    xvld       D3,     C3,    0x00 //c3: 0 1 2 3

    xvand.v    D4,     D0,    D0
    xvpermi.q  D4,     D1,    0x02 //c0:0 1, c1:0 1
    xvpermi.d  D6,     D4,    0xd8 //c0[0] c1[0] c0[1] c1[1]
    xvpermi.d  D7,     D4,    0x8d //c0[1] c1[1] c0[0] c1[0]

    xvand.v    D5,     D2,    D2
    xvpermi.q  D5,     D3,    0x02 //c2:0 1, c3:0 1
    xvpermi.d  D8,     D5,    0xd8 //c2[0] c3[0] c2[1] c3[1]
    xvpermi.d  D9,     D5,    0x8d //c2[1] c3[1] c2[0] c3[0]

    xvpermi.q  D6,     D8,    0x02 //c0[0] c1[0] c2[0] c3[0]
    xvpermi.q  D7,     D9,    0x02 //c0[1] c1[1] c2[1] c3[1]

    XVFMADD      D6,    U4,    VALPHAR, D6
    XVFMADD      D7,    U5,    VALPHAR, D7
    XVNMSUB      D6,    U5,    VALPHAI, D6
    XVFMADD      D7,    U4,    VALPHAI, D7

    xvand.v    D10,     D6,    D6
    xvpermi.q  D10,     D7,    0x02 //c0[0] c1[0] c0[1] c1[1]
    xvpermi.d  D10,     D10,   0xd8 //c0[0] c0[1] c1[0] c1[1]

    xvand.v    D11,     D7,    D7
    xvpermi.q  D11,     D6,    0x31 //c2[0] c3[0] c2[1] c3[1]
    xvpermi.d  D11,     D11,   0xd8 //c2[0] c2[1] c3[0] c3[1]

    //res03 res13 res23 res33
    xvand.v    D4,     D1,    D1
    xvpermi.q  D4,     D0,    0x31 //c0:2 3, c1:2 3
    xvpermi.d  D6,     D4,    0xd8 //c0[2] c1[2] c0[3] c1[3]
    xvpermi.d  D7,     D4,    0x8d //c0[3] c1[3] c0[2] c1[2]

    xvand.v    D5,     D3,    D3
    xvpermi.q  D5,     D2,    0x31 //c2:2 3, c3:2 3
    xvpermi.d  D8,     D5,    0xd8 //c2[2] c3[2] c2[3] c3[3]
    xvpermi.d  D9,     D5,    0x8d //c2[3] c3[3] c2[2] c3[2]

    xvpermi.q  D6,     D8,    0x02 //c0[2] c1[2] c2[2] c3[2]
    xvpermi.q  D7,     D9,    0x02 //c0[3] c1[3] c2[3] c3[3]

    XVFMADD      D6,    U6,    VALPHAR, D6
    XVFMADD      D7,    U7,    VALPHAR, D7
    XVNMSUB      D6,    U7,    VALPHAI, D6
    XVFMADD      D7,    U6,    VALPHAI, D7

    xvand.v    D4,     D6,    D6
    xvpermi.q  D4,     D7,    0x02 //c0[2] c1[2] c0[3] c1[3]
    xvpermi.d  D4,     D4,    0xd8 //c0[2] c0[3] c1[2] c1[3]

    xvand.v    D5,     D7,    D7
    xvpermi.q  D5,     D6,    0x31 //c2[2] c3[2] c2[3] c3[3]
    xvpermi.d  D5,     D5,    0xd8 //c2[2] c2[3] c3[2] c3[3]

    xvand.v    D0,     D10,    D10
    xvand.v    D1,     D11,    D11

    xvpermi.q  D0,     D4,     0x02 //c0: 0 1 2 3
    xvpermi.q  D4,     D10,    0x31 //c1: 0 1 2 3
    xvpermi.q  D1,     D5,     0x02 //c2: 0 1 2 3
    xvpermi.q  D5,     D11,    0x31 //c3: 0 1 2 3

    xvst       D0,     C0,    0x00
    xvst       D4,     C1,    0x00
    xvst       D1,     C2,    0x00
    xvst       D5,     C3,    0x00

    addi.d     C0,     C0,    0x20
    addi.d     C1,     C1,    0x20
    addi.d     C2,     C2,    0x20
    addi.d     C3,     C3,    0x20

    //res04 res14 res24 res34
    xvld       D0,     C0,    0x00 //c0: 0 1 2 3
    xvld       D1,     C1,    0x00 //c1: 0 1 2 3
    xvld       D2,     C2,    0x00 //c2: 0 1 2 3
    xvld       D3,     C3,    0x00 //c3: 0 1 2 3

    xvand.v    D4,     D0,    D0
    xvpermi.q  D4,     D1,    0x02 //c0:0 1, c1:0 1
    xvpermi.d  D6,     D4,    0xd8 //c0[0] c1[0] c0[1] c1[1]
    xvpermi.d  D7,     D4,    0x8d //c0[1] c1[1] c0[0] c1[0]

    xvand.v    D5,     D2,    D2
    xvpermi.q  D5,     D3,    0x02 //c2:0 1, c3:0 1
    xvpermi.d  D8,     D5,    0xd8 //c2[0] c3[0] c2[1] c3[1]
    xvpermi.d  D9,     D5,    0x8d //c2[1] c3[1] c2[0] c3[0]

    xvpermi.q  D6,     D8,    0x02 //c0[0] c1[0] c2[0] c3[0]
    xvpermi.q  D7,     D9,    0x02 //c0[1] c1[1] c2[1] c3[1]

    XVFMADD      D6,    U8,    VALPHAR, D6
    XVFMADD      D7,    U9,    VALPHAR, D7
    XVNMSUB      D6,    U9,    VALPHAI, D6
    XVFMADD      D7,    U8,    VALPHAI, D7

    xvand.v    D10,     D6,    D6
    xvpermi.q  D10,     D7,    0x02 //c0[0] c1[0] c0[1] c1[1]
    xvpermi.d  D10,     D10,   0xd8 //c0[0] c0[1] c1[0] c1[1]

    xvand.v    D11,     D7,    D7
    xvpermi.q  D11,     D6,    0x31 //c2[0] c3[0] c2[1] c3[1]
    xvpermi.d  D11,     D11,   0xd8 //c2[0] c2[1] c3[0] c3[1]

    //res05 res15 res25 res35
    xvand.v    D4,     D1,    D1
    xvpermi.q  D4,     D0,    0x31 //c0:2 3, c1:2 3
    xvpermi.d  D6,     D4,    0xd8 //c0[2] c1[2] c0[3] c1[3]
    xvpermi.d  D7,     D4,    0x8d //c0[3] c1[3] c0[2] c1[2]

    xvand.v    D5,     D3,    D3
    xvpermi.q  D5,     D2,    0x31 //c2:2 3, c3:2 3
    xvpermi.d  D8,     D5,    0xd8 //c2[2] c3[2] c2[3] c3[3]
    xvpermi.d  D9,     D5,    0x8d //c2[3] c3[3] c2[2] c3[2]

    xvpermi.q  D6,     D8,    0x02 //c0[2] c1[2] c2[2] c3[2]
    xvpermi.q  D7,     D9,    0x02 //c0[3] c1[3] c2[3] c3[3]

    XVFMADD      D6,    U10,    VALPHAR, D6
    XVFMADD      D7,    U11,    VALPHAR, D7
    XVNMSUB      D6,    U11,    VALPHAI, D6
    XVFMADD      D7,    U10,    VALPHAI, D7

    xvand.v    D4,     D6,    D6
    xvpermi.q  D4,     D7,    0x02 //c0[2] c1[2] c0[3] c1[3]
    xvpermi.d  D4,     D4,    0xd8 //c0[2] c0[3] c1[2] c1[3]

    xvand.v    D5,     D7,    D7
    xvpermi.q  D5,     D6,    0x31 //c2[2] c3[2] c2[3] c3[3]
    xvpermi.d  D5,     D5,    0xd8 //c2[2] c2[3] c3[2] c3[3]

    xvand.v    D0,     D10,    D10
    xvand.v    D1,     D11,    D11

    xvpermi.q  D0,     D4,     0x02 //c0: 0 1 2 3
    xvpermi.q  D4,     D10,    0x31 //c1: 0 1 2 3
    xvpermi.q  D1,     D5,     0x02 //c2: 0 1 2 3
    xvpermi.q  D5,     D11,    0x31 //c3: 0 1 2 3

    xvst       D0,     C0,    0x00
    xvst       D4,     C1,    0x00
    xvst       D1,     C2,    0x00
    xvst       D5,     C3,    0x00

    addi.d     C0,     C0,    0x20
    addi.d     C1,     C1,    0x20
    addi.d     C2,     C2,    0x20
    addi.d     C3,     C3,    0x20

    //res06 res16 res26 res36
    xvld       D0,     C0,    0x00 //c0: 0 1 2 3
    xvld       D1,     C1,    0x00 //c1: 0 1 2 3
    xvld       D2,     C2,    0x00 //c2: 0 1 2 3
    xvld       D3,     C3,    0x00 //c3: 0 1 2 3

    xvand.v    D4,     D0,    D0
    xvpermi.q  D4,     D1,    0x02 //c0:0 1, c1:0 1
    xvpermi.d  D6,     D4,    0xd8 //c0[0] c1[0] c0[1] c1[1]
    xvpermi.d  D7,     D4,    0x8d //c0[1] c1[1] c0[0] c1[0]

    xvand.v    D5,     D2,    D2
    xvpermi.q  D5,     D3,    0x02 //c2:0 1, c3:0 1
    xvpermi.d  D8,     D5,    0xd8 //c2[0] c3[0] c2[1] c3[1]
    xvpermi.d  D9,     D5,    0x8d //c2[1] c3[1] c2[0] c3[0]

    xvpermi.q  D6,     D8,    0x02 //c0[0] c1[0] c2[0] c3[0]
    xvpermi.q  D7,     D9,    0x02 //c0[1] c1[1] c2[1] c3[1]

    XVFMADD      D6,    U12,    VALPHAR, D6
    XVFMADD      D7,    U13,    VALPHAR, D7
    XVNMSUB      D6,    U13,    VALPHAI, D6
    XVFMADD      D7,    U12,    VALPHAI, D7

    xvand.v    D10,     D6,    D6
    xvpermi.q  D10,     D7,    0x02 //c0[0] c1[0] c0[1] c1[1]
    xvpermi.d  D10,     D10,   0xd8 //c0[0] c0[1] c1[0] c1[1]

    xvand.v    D11,     D7,    D7
    xvpermi.q  D11,     D6,    0x31 //c2[0] c3[0] c2[1] c3[1]
    xvpermi.d  D11,     D11,   0xd8 //c2[0] c2[1] c3[0] c3[1]

    //res07 res17 res27 res37
    xvand.v    D4,     D1,    D1
    xvpermi.q  D4,     D0,    0x31 //c0:2 3, c1:2 3
    xvpermi.d  D6,     D4,    0xd8 //c0[2] c1[2] c0[3] c1[3]
    xvpermi.d  D7,     D4,    0x8d //c0[3] c1[3] c0[2] c1[2]

    xvand.v    D5,     D3,    D3
    xvpermi.q  D5,     D2,    0x31 //c2:2 3, c3:2 3
    xvpermi.d  D8,     D5,    0xd8 //c2[2] c3[2] c2[3] c3[3]
    xvpermi.d  D9,     D5,    0x8d //c2[3] c3[3] c2[2] c3[2]

    xvpermi.q  D6,     D8,    0x02 //c0[2] c1[2] c2[2] c3[2]
    xvpermi.q  D7,     D9,    0x02 //c0[3] c1[3] c2[3] c3[3]

    XVFMADD      D6,    U14,    VALPHAR, D6
    XVFMADD      D7,    U15,    VALPHAR, D7
    XVNMSUB      D6,    U15,    VALPHAI, D6
    XVFMADD      D7,    U14,    VALPHAI, D7

    xvand.v    D4,     D6,    D6
    xvpermi.q  D4,     D7,    0x02 //c0[2] c1[2] c0[3] c1[3]
    xvpermi.d  D4,     D4,    0xd8 //c0[2] c0[3] c1[2] c1[3]

    xvand.v    D5,     D7,    D7
    xvpermi.q  D5,     D6,    0x31 //c2[2] c3[2] c2[3] c3[3]
    xvpermi.d  D5,     D5,    0xd8 //c2[2] c2[3] c3[2] c3[3]

    xvand.v    D0,     D10,    D10
    xvand.v    D1,     D11,    D11

    xvpermi.q  D0,     D4,     0x02 //c0: 0 1 2 3
    xvpermi.q  D4,     D10,    0x31 //c1: 0 1 2 3
    xvpermi.q  D1,     D5,     0x02 //c2: 0 1 2 3
    xvpermi.q  D5,     D11,    0x31 //c3: 0 1 2 3

    xvst       D0,     C0,    0x00
    xvst       D4,     C1,    0x00
    xvst       D1,     C2,    0x00
    xvst       D5,     C3,    0x00

    addi.d     C0,     C0,    0x20
    addi.d     C1,     C1,    0x20
    addi.d     C2,     C2,    0x20
    addi.d     C3,     C3,    0x20
#endif

#if defined(TRMMKERNEL)

#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d      TL,     K,    OFF
#ifdef LEFT
    addi.d     TL,     TL,   -8
#else
    addi.d     TL,     TL,   -4
#endif
    slli.d     T3,     TL,   0x07
    add.d      A0,     A0,   T3
    slli.d     T3,     TL,   0x06
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
    slli.d     T3,     OFF,  0x06
    add.d      A0,     A0,   T3
    slli.d     T3,     OFF,  0x06
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

    xvxor.v    U0,     U0,   U0
    xvxor.v    U1,     U1,   U1
    xvxor.v    U2,     U2,   U2
    xvxor.v    U3,     U3,   U3
    xvxor.v    U4,     U4,   U4
    xvxor.v    U5,     U5,   U5
    xvxor.v    U6,     U6,   U6
    xvxor.v    U7,     U7,   U7

    move       L,      $r0   //cycle param k
    beq        L,      TL,    .L17
    blt        TL,     L,     .L17

.L16:  /* for (k=0; k<temp; k++) */
    xvld       D0,     A0,    0x00  // a0ri a1ri
    xvld       D2,     B0,    0x00  // b0ri b1ri
    xvld       D3,     B0,    0x20  // b2ri b3ri

    xvpermi.d  D4,     D0,    0x00  //a0r
    xvpermi.d  D5,     D0,    0x55  //a0i

    xvpackev.d D6,     D3,    D2
    xvpermi.d  D6,     D6,    0xd8  //b0r b1r b2r b3r

    xvpackod.d D7,     D3,    D2
    xvpermi.d  D7,     D7,    0xd8  //b0i b1i b2i b3i

    XVMADD1    U0,     D4,    D6,     U0  //00r 10r 20r 30r
    XVMADD2    U1,     D5,    D6,     U1  //00i 10i 20i 30i
    XVMADD3    U0,     D5,    D7,     U0
    XVMADD4    U1,     D4,    D7,     U1

    xvpermi.d  D4,     D0,    0xaa  //a1r
    xvpermi.d  D5,     D0,    0xff  //a1i

    XVMADD1    U2,     D4,    D6,     U2  //01r 11r 21r 31r
    XVMADD2    U3,     D5,    D6,     U3  //01i 11i 21i 31i
    XVMADD3    U2,     D5,    D7,     U2
    XVMADD4    U3,     D4,    D7,     U3

    xvld       D0,     A0,    0x20  // a2ri a3ri

    xvpermi.d  D4,     D0,    0x00  //a2r
    xvpermi.d  D5,     D0,    0x55  //a2i

    XVMADD1    U4,     D4,    D6,     U4  //02r 12r 22r 32r
    XVMADD2    U5,     D5,    D6,     U5  //02i 12i 22i 32i
    XVMADD3    U4,     D5,    D7,     U4
    XVMADD4    U5,     D4,    D7,     U5

    xvpermi.d  D4,     D0,    0xaa  //a3r
    xvpermi.d  D5,     D0,    0xff  //a3i

    XVMADD1    U6,     D4,    D6,     U6  //03r 13r 23r 33r
    XVMADD2    U7,     D5,    D6,     U7  //03i 13i 23i 33i
    XVMADD3    U6,     D5,    D7,     U6
    XVMADD4    U7,     D4,    D7,     U7

    addi.d     A0,     A0,    0x40
    addi.d     B0,     B0,    0x40

    addi.d     L,      L,     1
    blt        L,      TL,     .L16

.L17:
#if defined(TRMMKERNEL)
    //res00 res10 res20 res30
    xvld       D0,     C0,    0x00 //c0: 0 1 2 3
    xvld       D1,     C1,    0x00 //c1: 0 1 2 3
    xvld       D2,     C2,    0x00 //c2: 0 1 2 3
    xvld       D3,     C3,    0x00 //c3: 0 1 2 3

    xvand.v    D4,     D0,    D0
    xvpermi.q  D4,     D1,    0x02 //c0:0 1, c1:0 1
    xvpermi.d  D6,     D4,    0xd8 //c0[0] c1[0] c0[1] c1[1]
    xvpermi.d  D7,     D4,    0x8d //c0[1] c1[1] c0[0] c1[0]

    xvand.v    D5,     D2,    D2
    xvpermi.q  D5,     D3,    0x02 //c2:0 1, c3:0 1
    xvpermi.d  D8,     D5,    0xd8 //c2[0] c3[0] c2[1] c3[1]
    xvpermi.d  D9,     D5,    0x8d //c2[1] c3[1] c2[0] c3[0]

    xvpermi.q  D6,     D8,    0x02 //c0[0] c1[0] c2[0] c3[0]
    xvpermi.q  D7,     D9,    0x02 //c0[1] c1[1] c2[1] c3[1]

    xvfmul.d      D6,    U0,    VALPHAR
    xvfmul.d      D7,    U1,    VALPHAR
    XVNMSUB      D6,    U1,    VALPHAI, D6
    XVFMADD      D7,    U0,    VALPHAI, D7

    xvand.v    D10,     D6,    D6
    xvpermi.q  D10,     D7,    0x02 //c0[0] c1[0] c0[1] c1[1]
    xvpermi.d  D10,     D10,   0xd8 //c0[0] c0[1] c1[0] c1[1]

    xvand.v    D11,     D7,    D7
    xvpermi.q  D11,     D6,    0x31 //c2[0] c3[0] c2[1] c3[1]
    xvpermi.d  D11,     D11,   0xd8 //c2[0] c2[1] c3[0] c3[1]

    //res01 res11 res21 res31
    xvand.v    D4,     D1,    D1
    xvpermi.q  D4,     D0,    0x31 //c0:2 3, c1:2 3
    xvpermi.d  D6,     D4,    0xd8 //c0[2] c1[2] c0[3] c1[3]
    xvpermi.d  D7,     D4,    0x8d //c0[3] c1[3] c0[2] c1[2]

    xvand.v    D5,     D3,    D3
    xvpermi.q  D5,     D2,    0x31 //c2:2 3, c3:2 3
    xvpermi.d  D8,     D5,    0xd8 //c2[2] c3[2] c2[3] c3[3]
    xvpermi.d  D9,     D5,    0x8d //c2[3] c3[3] c2[2] c3[2]

    xvpermi.q  D6,     D8,    0x02 //c0[2] c1[2] c2[2] c3[2]
    xvpermi.q  D7,     D9,    0x02 //c0[3] c1[3] c2[3] c3[3]

    xvfmul.d      D6,    U2,    VALPHAR
    xvfmul.d      D7,    U3,    VALPHAR
    XVNMSUB      D6,    U3,    VALPHAI, D6
    XVFMADD      D7,    U2,    VALPHAI, D7

    xvand.v    D4,     D6,    D6
    xvpermi.q  D4,     D7,    0x02 //c0[2] c1[2] c0[3] c1[3]
    xvpermi.d  D4,     D4,    0xd8 //c0[2] c0[3] c1[2] c1[3]

    xvand.v    D5,     D7,    D7
    xvpermi.q  D5,     D6,    0x31 //c2[2] c3[2] c2[3] c3[3]
    xvpermi.d  D5,     D5,    0xd8 //c2[2] c2[3] c3[2] c3[3]

    xvand.v    D0,     D10,    D10
    xvand.v    D1,     D11,    D11

    xvpermi.q  D0,     D4,     0x02 //c0: 0 1 2 3
    xvpermi.q  D4,     D10,    0x31 //c1: 0 1 2 3
    xvpermi.q  D1,     D5,     0x02 //c2: 0 1 2 3
    xvpermi.q  D5,     D11,    0x31 //c3: 0 1 2 3

    xvst       D0,     C0,    0x00
    xvst       D4,     C1,    0x00
    xvst       D1,     C2,    0x00
    xvst       D5,     C3,    0x00

    addi.d     C0,     C0,    0x20
    addi.d     C1,     C1,    0x20
    addi.d     C2,     C2,    0x20
    addi.d     C3,     C3,    0x20

    //res02 res12 res22 res32
    xvld       D0,     C0,    0x00 //c0: 0 1 2 3
    xvld       D1,     C1,    0x00 //c1: 0 1 2 3
    xvld       D2,     C2,    0x00 //c2: 0 1 2 3
    xvld       D3,     C3,    0x00 //c3: 0 1 2 3

    xvand.v    D4,     D0,    D0
    xvpermi.q  D4,     D1,    0x02 //c0:0 1, c1:0 1
    xvpermi.d  D6,     D4,    0xd8 //c0[0] c1[0] c0[1] c1[1]
    xvpermi.d  D7,     D4,    0x8d //c0[1] c1[1] c0[0] c1[0]

    xvand.v    D5,     D2,    D2
    xvpermi.q  D5,     D3,    0x02 //c2:0 1, c3:0 1
    xvpermi.d  D8,     D5,    0xd8 //c2[0] c3[0] c2[1] c3[1]
    xvpermi.d  D9,     D5,    0x8d //c2[1] c3[1] c2[0] c3[0]

    xvpermi.q  D6,     D8,    0x02 //c0[0] c1[0] c2[0] c3[0]
    xvpermi.q  D7,     D9,    0x02 //c0[1] c1[1] c2[1] c3[1]

    xvfmul.d      D6,    U4,    VALPHAR
    xvfmul.d      D7,    U5,    VALPHAR
    XVNMSUB      D6,    U5,    VALPHAI, D6
    XVFMADD      D7,    U4,    VALPHAI, D7

    xvand.v    D10,     D6,    D6
    xvpermi.q  D10,     D7,    0x02 //c0[0] c1[0] c0[1] c1[1]
    xvpermi.d  D10,     D10,   0xd8 //c0[0] c0[1] c1[0] c1[1]

    xvand.v    D11,     D7,    D7
    xvpermi.q  D11,     D6,    0x31 //c2[0] c3[0] c2[1] c3[1]
    xvpermi.d  D11,     D11,   0xd8 //c2[0] c2[1] c3[0] c3[1]

    //res03 res13 res23 res33
    xvand.v    D4,     D1,    D1
    xvpermi.q  D4,     D0,    0x31 //c0:2 3, c1:2 3
    xvpermi.d  D6,     D4,    0xd8 //c0[2] c1[2] c0[3] c1[3]
    xvpermi.d  D7,     D4,    0x8d //c0[3] c1[3] c0[2] c1[2]

    xvand.v    D5,     D3,    D3
    xvpermi.q  D5,     D2,    0x31 //c2:2 3, c3:2 3
    xvpermi.d  D8,     D5,    0xd8 //c2[2] c3[2] c2[3] c3[3]
    xvpermi.d  D9,     D5,    0x8d //c2[3] c3[3] c2[2] c3[2]

    xvpermi.q  D6,     D8,    0x02 //c0[2] c1[2] c2[2] c3[2]
    xvpermi.q  D7,     D9,    0x02 //c0[3] c1[3] c2[3] c3[3]

    xvfmul.d      D6,    U6,    VALPHAR
    xvfmul.d      D7,    U7,    VALPHAR
    XVNMSUB      D6,    U7,    VALPHAI, D6
    XVFMADD      D7,    U6,    VALPHAI, D7

    xvand.v    D4,     D6,    D6
    xvpermi.q  D4,     D7,    0x02 //c0[2] c1[2] c0[3] c1[3]
    xvpermi.d  D4,     D4,    0xd8 //c0[2] c0[3] c1[2] c1[3]

    xvand.v    D5,     D7,    D7
    xvpermi.q  D5,     D6,    0x31 //c2[2] c3[2] c2[3] c3[3]
    xvpermi.d  D5,     D5,    0xd8 //c2[2] c2[3] c3[2] c3[3]

    xvand.v    D0,     D10,    D10
    xvand.v    D1,     D11,    D11

    xvpermi.q  D0,     D4,     0x02 //c0: 0 1 2 3
    xvpermi.q  D4,     D10,    0x31 //c1: 0 1 2 3
    xvpermi.q  D1,     D5,     0x02 //c2: 0 1 2 3
    xvpermi.q  D5,     D11,    0x31 //c3: 0 1 2 3

    xvst       D0,     C0,    0x00
    xvst       D4,     C1,    0x00
    xvst       D1,     C2,    0x00
    xvst       D5,     C3,    0x00

    addi.d     C0,     C0,    0x20
    addi.d     C1,     C1,    0x20
    addi.d     C2,     C2,    0x20
    addi.d     C3,     C3,    0x20
#else
    //res00 res10 res20 res30
    xvld       D0,     C0,    0x00 //c0: 0 1 2 3
    xvld       D1,     C1,    0x00 //c1: 0 1 2 3
    xvld       D2,     C2,    0x00 //c2: 0 1 2 3
    xvld       D3,     C3,    0x00 //c3: 0 1 2 3

    xvand.v    D4,     D0,    D0
    xvpermi.q  D4,     D1,    0x02 //c0:0 1, c1:0 1
    xvpermi.d  D6,     D4,    0xd8 //c0[0] c1[0] c0[1] c1[1]
    xvpermi.d  D7,     D4,    0x8d //c0[1] c1[1] c0[0] c1[0]

    xvand.v    D5,     D2,    D2
    xvpermi.q  D5,     D3,    0x02 //c2:0 1, c3:0 1
    xvpermi.d  D8,     D5,    0xd8 //c2[0] c3[0] c2[1] c3[1]
    xvpermi.d  D9,     D5,    0x8d //c2[1] c3[1] c2[0] c3[0]

    xvpermi.q  D6,     D8,    0x02 //c0[0] c1[0] c2[0] c3[0]
    xvpermi.q  D7,     D9,    0x02 //c0[1] c1[1] c2[1] c3[1]

    XVFMADD      D6,    U0,    VALPHAR, D6
    XVFMADD      D7,    U1,    VALPHAR, D7
    XVNMSUB      D6,    U1,    VALPHAI, D6
    XVFMADD      D7,    U0,    VALPHAI, D7

    xvand.v    D10,     D6,    D6
    xvpermi.q  D10,     D7,    0x02 //c0[0] c1[0] c0[1] c1[1]
    xvpermi.d  D10,     D10,   0xd8 //c0[0] c0[1] c1[0] c1[1]

    xvand.v    D11,     D7,    D7
    xvpermi.q  D11,     D6,    0x31 //c2[0] c3[0] c2[1] c3[1]
    xvpermi.d  D11,     D11,   0xd8 //c2[0] c2[1] c3[0] c3[1]

    //res01 res11 res21 res31
    xvand.v    D4,     D1,    D1
    xvpermi.q  D4,     D0,    0x31 //c0:2 3, c1:2 3
    xvpermi.d  D6,     D4,    0xd8 //c0[2] c1[2] c0[3] c1[3]
    xvpermi.d  D7,     D4,    0x8d //c0[3] c1[3] c0[2] c1[2]

    xvand.v    D5,     D3,    D3
    xvpermi.q  D5,     D2,    0x31 //c2:2 3, c3:2 3
    xvpermi.d  D8,     D5,    0xd8 //c2[2] c3[2] c2[3] c3[3]
    xvpermi.d  D9,     D5,    0x8d //c2[3] c3[3] c2[2] c3[2]

    xvpermi.q  D6,     D8,    0x02 //c0[2] c1[2] c2[2] c3[2]
    xvpermi.q  D7,     D9,    0x02 //c0[3] c1[3] c2[3] c3[3]

    XVFMADD      D6,    U2,    VALPHAR, D6
    XVFMADD      D7,    U3,    VALPHAR, D7
    XVNMSUB      D6,    U3,    VALPHAI, D6
    XVFMADD      D7,    U2,    VALPHAI, D7

    xvand.v    D4,     D6,    D6
    xvpermi.q  D4,     D7,    0x02 //c0[2] c1[2] c0[3] c1[3]
    xvpermi.d  D4,     D4,    0xd8 //c0[2] c0[3] c1[2] c1[3]

    xvand.v    D5,     D7,    D7
    xvpermi.q  D5,     D6,    0x31 //c2[2] c3[2] c2[3] c3[3]
    xvpermi.d  D5,     D5,    0xd8 //c2[2] c2[3] c3[2] c3[3]

    xvand.v    D0,     D10,    D10
    xvand.v    D1,     D11,    D11

    xvpermi.q  D0,     D4,     0x02 //c0: 0 1 2 3
    xvpermi.q  D4,     D10,    0x31 //c1: 0 1 2 3
    xvpermi.q  D1,     D5,     0x02 //c2: 0 1 2 3
    xvpermi.q  D5,     D11,    0x31 //c3: 0 1 2 3

    xvst       D0,     C0,    0x00
    xvst       D4,     C1,    0x00
    xvst       D1,     C2,    0x00
    xvst       D5,     C3,    0x00

    addi.d     C0,     C0,    0x20
    addi.d     C1,     C1,    0x20
    addi.d     C2,     C2,    0x20
    addi.d     C3,     C3,    0x20

    //res02 res12 res22 res32
    xvld       D0,     C0,    0x00 //c0: 0 1 2 3
    xvld       D1,     C1,    0x00 //c1: 0 1 2 3
    xvld       D2,     C2,    0x00 //c2: 0 1 2 3
    xvld       D3,     C3,    0x00 //c3: 0 1 2 3

    xvand.v    D4,     D0,    D0
    xvpermi.q  D4,     D1,    0x02 //c0:0 1, c1:0 1
    xvpermi.d  D6,     D4,    0xd8 //c0[0] c1[0] c0[1] c1[1]
    xvpermi.d  D7,     D4,    0x8d //c0[1] c1[1] c0[0] c1[0]

    xvand.v    D5,     D2,    D2
    xvpermi.q  D5,     D3,    0x02 //c2:0 1, c3:0 1
    xvpermi.d  D8,     D5,    0xd8 //c2[0] c3[0] c2[1] c3[1]
    xvpermi.d  D9,     D5,    0x8d //c2[1] c3[1] c2[0] c3[0]

    xvpermi.q  D6,     D8,    0x02 //c0[0] c1[0] c2[0] c3[0]
    xvpermi.q  D7,     D9,    0x02 //c0[1] c1[1] c2[1] c3[1]

    XVFMADD      D6,    U4,    VALPHAR, D6
    XVFMADD      D7,    U5,    VALPHAR, D7
    XVNMSUB      D6,    U5,    VALPHAI, D6
    XVFMADD      D7,    U4,    VALPHAI, D7

    xvand.v    D10,     D6,    D6
    xvpermi.q  D10,     D7,    0x02 //c0[0] c1[0] c0[1] c1[1]
    xvpermi.d  D10,     D10,   0xd8 //c0[0] c0[1] c1[0] c1[1]

    xvand.v    D11,     D7,    D7
    xvpermi.q  D11,     D6,    0x31 //c2[0] c3[0] c2[1] c3[1]
    xvpermi.d  D11,     D11,   0xd8 //c2[0] c2[1] c3[0] c3[1]

    //res03 res13 res23 res33
    xvand.v    D4,     D1,    D1
    xvpermi.q  D4,     D0,    0x31 //c0:2 3, c1:2 3
    xvpermi.d  D6,     D4,    0xd8 //c0[2] c1[2] c0[3] c1[3]
    xvpermi.d  D7,     D4,    0x8d //c0[3] c1[3] c0[2] c1[2]

    xvand.v    D5,     D3,    D3
    xvpermi.q  D5,     D2,    0x31 //c2:2 3, c3:2 3
    xvpermi.d  D8,     D5,    0xd8 //c2[2] c3[2] c2[3] c3[3]
    xvpermi.d  D9,     D5,    0x8d //c2[3] c3[3] c2[2] c3[2]

    xvpermi.q  D6,     D8,    0x02 //c0[2] c1[2] c2[2] c3[2]
    xvpermi.q  D7,     D9,    0x02 //c0[3] c1[3] c2[3] c3[3]

    XVFMADD      D6,    U6,    VALPHAR, D6
    XVFMADD      D7,    U7,    VALPHAR, D7
    XVNMSUB      D6,    U7,    VALPHAI, D6
    XVFMADD      D7,    U6,    VALPHAI, D7

    xvand.v    D4,     D6,    D6
    xvpermi.q  D4,     D7,    0x02 //c0[2] c1[2] c0[3] c1[3]
    xvpermi.d  D4,     D4,    0xd8 //c0[2] c0[3] c1[2] c1[3]

    xvand.v    D5,     D7,    D7
    xvpermi.q  D5,     D6,    0x31 //c2[2] c3[2] c2[3] c3[3]
    xvpermi.d  D5,     D5,    0xd8 //c2[2] c2[3] c3[2] c3[3]

    xvand.v    D0,     D10,    D10
    xvand.v    D1,     D11,    D11

    xvpermi.q  D0,     D4,     0x02 //c0: 0 1 2 3
    xvpermi.q  D4,     D10,    0x31 //c1: 0 1 2 3
    xvpermi.q  D1,     D5,     0x02 //c2: 0 1 2 3
    xvpermi.q  D5,     D11,    0x31 //c3: 0 1 2 3

    xvst       D0,     C0,    0x00
    xvst       D4,     C1,    0x00
    xvst       D1,     C2,    0x00
    xvst       D5,     C3,    0x00

    addi.d     C0,     C0,    0x20
    addi.d     C1,     C1,    0x20
    addi.d     C2,     C2,    0x20
    addi.d     C3,     C3,    0x20
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d      TL,     K,    OFF
#ifdef LEFT
    addi.d     TL,     TL,   -4
#else
    addi.d     TL,     TL,   -4
#endif
    slli.d     T3,     TL,   0x06
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
    slli.d     T3,     OFF,  0x05
    add.d      A0,     A0,   T3
    slli.d     T3,     OFF,  0x06
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

    xvxor.v    U0,     U0,   U0
    xvxor.v    U1,     U1,   U1
    xvxor.v    U2,     U2,   U2
    xvxor.v    U3,     U3,   U3

    move       L,      $r0   //cycle param k
    beq        L,      TL,    .L182
    blt        TL,     L,     .L182

.L181:  /* for (k=0; k<temp; k++) */
    xvld       D0,     A0,    0x00  // a0ri a1ri
    xvld       D2,     B0,    0x00  // b0ri b1ri
    xvld       D3,     B0,    0x20  // b2ri b3ri

    xvpermi.d  D4,     D0,    0x00  //a0r
    xvpermi.d  D5,     D0,    0x55  //a0i

    xvpackev.d D6,     D3,    D2
    xvpermi.d  D6,     D6,    0xd8  //b0r b1r b2r b3r

    xvpackod.d D7,     D3,    D2
    xvpermi.d  D7,     D7,    0xd8  //b0i b1i b2i b3i

    XVMADD1    U0,     D4,    D6,     U0  //00r 10r 20r 30r
    XVMADD2    U1,     D5,    D6,     U1  //00i 10i 20i 30i
    XVMADD3    U0,     D5,    D7,     U0
    XVMADD4    U1,     D4,    D7,     U1

    xvpermi.d  D4,     D0,    0xaa  //a1r
    xvpermi.d  D5,     D0,    0xff  //a1i

    XVMADD1    U2,     D4,    D6,     U2  //01r 11r 21r 31r
    XVMADD2    U3,     D5,    D6,     U3  //01i 11i 21i 31i
    XVMADD3    U2,     D5,    D7,     U2
    XVMADD4    U3,     D4,    D7,     U3

    addi.d     A0,     A0,    0x20
    addi.d     B0,     B0,    0x40

    addi.d     L,      L,     1
    blt        L,      TL,    .L181

.L182:
#if defined(TRMMKERNEL)
    //res00 res10 res20 res30
    xvld       D0,     C0,    0x00 //c0: 0 1 2 3
    xvld       D1,     C1,    0x00 //c1: 0 1 2 3
    xvld       D2,     C2,    0x00 //c2: 0 1 2 3
    xvld       D3,     C3,    0x00 //c3: 0 1 2 3

    xvand.v    D4,     D0,    D0
    xvpermi.q  D4,     D1,    0x02 //c0:0 1, c1:0 1
    xvpermi.d  D6,     D4,    0xd8 //c0[0] c1[0] c0[1] c1[1]
    xvpermi.d  D7,     D4,    0x8d //c0[1] c1[1] c0[0] c1[0]

    xvand.v    D5,     D2,    D2
    xvpermi.q  D5,     D3,    0x02 //c2:0 1, c3:0 1
    xvpermi.d  D8,     D5,    0xd8 //c2[0] c3[0] c2[1] c3[1]
    xvpermi.d  D9,     D5,    0x8d //c2[1] c3[1] c2[0] c3[0]

    xvpermi.q  D6,     D8,    0x02 //c0[0] c1[0] c2[0] c3[0]
    xvpermi.q  D7,     D9,    0x02 //c0[1] c1[1] c2[1] c3[1]

    xvfmul.d      D6,    U0,    VALPHAR
    xvfmul.d      D7,    U1,    VALPHAR
    XVNMSUB      D6,    U1,    VALPHAI, D6
    XVFMADD      D7,    U0,    VALPHAI, D7

    xvand.v    D10,     D6,    D6
    xvpermi.q  D10,     D7,    0x02 //c0[0] c1[0] c0[1] c1[1]
    xvpermi.d  D10,     D10,   0xd8 //c0[0] c0[1] c1[0] c1[1]

    xvand.v    D11,     D7,    D7
    xvpermi.q  D11,     D6,    0x31 //c2[0] c3[0] c2[1] c3[1]
    xvpermi.d  D11,     D11,   0xd8 //c2[0] c2[1] c3[0] c3[1]

    //res01 res11 res21 res31
    xvand.v    D4,     D1,    D1
    xvpermi.q  D4,     D0,    0x31 //c0:2 3, c1:2 3
    xvpermi.d  D6,     D4,    0xd8 //c0[2] c1[2] c0[3] c1[3]
    xvpermi.d  D7,     D4,    0x8d //c0[3] c1[3] c0[2] c1[2]

    xvand.v    D5,     D3,    D3
    xvpermi.q  D5,     D2,    0x31 //c2:2 3, c3:2 3
    xvpermi.d  D8,     D5,    0xd8 //c2[2] c3[2] c2[3] c3[3]
    xvpermi.d  D9,     D5,    0x8d //c2[3] c3[3] c2[2] c3[2]

    xvpermi.q  D6,     D8,    0x02 //c0[2] c1[2] c2[2] c3[2]
    xvpermi.q  D7,     D9,    0x02 //c0[3] c1[3] c2[3] c3[3]

    xvfmul.d      D6,    U2,    VALPHAR
    xvfmul.d      D7,    U3,    VALPHAR
    XVNMSUB      D6,    U3,    VALPHAI, D6
    XVFMADD      D7,    U2,    VALPHAI, D7

    xvand.v    D4,     D6,    D6
    xvpermi.q  D4,     D7,    0x02 //c0[2] c1[2] c0[3] c1[3]
    xvpermi.d  D4,     D4,    0xd8 //c0[2] c0[3] c1[2] c1[3]

    xvand.v    D5,     D7,    D7
    xvpermi.q  D5,     D6,    0x31 //c2[2] c3[2] c2[3] c3[3]
    xvpermi.d  D5,     D5,    0xd8 //c2[2] c2[3] c3[2] c3[3]

    xvand.v    D0,     D10,    D10
    xvand.v    D1,     D11,    D11

    xvpermi.q  D0,     D4,     0x02 //c0: 0 1 2 3
    xvpermi.q  D4,     D10,    0x31 //c1: 0 1 2 3
    xvpermi.q  D1,     D5,     0x02 //c2: 0 1 2 3
    xvpermi.q  D5,     D11,    0x31 //c3: 0 1 2 3

    xvst       D0,     C0,    0x00
    xvst       D4,     C1,    0x00
    xvst       D1,     C2,    0x00
    xvst       D5,     C3,    0x00

    addi.d     C0,     C0,    0x20
    addi.d     C1,     C1,    0x20
    addi.d     C2,     C2,    0x20
    addi.d     C3,     C3,    0x20
#else
    //res00 res10 res20 res30
    xvld       D0,     C0,    0x00 //c0: 0 1 2 3
    xvld       D1,     C1,    0x00 //c1: 0 1 2 3
    xvld       D2,     C2,    0x00 //c2: 0 1 2 3
    xvld       D3,     C3,    0x00 //c3: 0 1 2 3

    xvand.v    D4,     D0,    D0
    xvpermi.q  D4,     D1,    0x02 //c0:0 1, c1:0 1
    xvpermi.d  D6,     D4,    0xd8 //c0[0] c1[0] c0[1] c1[1]
    xvpermi.d  D7,     D4,    0x8d //c0[1] c1[1] c0[0] c1[0]

    xvand.v    D5,     D2,    D2
    xvpermi.q  D5,     D3,    0x02 //c2:0 1, c3:0 1
    xvpermi.d  D8,     D5,    0xd8 //c2[0] c3[0] c2[1] c3[1]
    xvpermi.d  D9,     D5,    0x8d //c2[1] c3[1] c2[0] c3[0]

    xvpermi.q  D6,     D8,    0x02 //c0[0] c1[0] c2[0] c3[0]
    xvpermi.q  D7,     D9,    0x02 //c0[1] c1[1] c2[1] c3[1]

    XVFMADD      D6,    U0,    VALPHAR, D6
    XVFMADD      D7,    U1,    VALPHAR, D7
    XVNMSUB      D6,    U1,    VALPHAI, D6
    XVFMADD      D7,    U0,    VALPHAI, D7

    xvand.v    D10,     D6,    D6
    xvpermi.q  D10,     D7,    0x02 //c0[0] c1[0] c0[1] c1[1]
    xvpermi.d  D10,     D10,   0xd8 //c0[0] c0[1] c1[0] c1[1]

    xvand.v    D11,     D7,    D7
    xvpermi.q  D11,     D6,    0x31 //c2[0] c3[0] c2[1] c3[1]
    xvpermi.d  D11,     D11,   0xd8 //c2[0] c2[1] c3[0] c3[1]

    //res01 res11 res21 res31
    xvand.v    D4,     D1,    D1
    xvpermi.q  D4,     D0,    0x31 //c0:2 3, c1:2 3
    xvpermi.d  D6,     D4,    0xd8 //c0[2] c1[2] c0[3] c1[3]
    xvpermi.d  D7,     D4,    0x8d //c0[3] c1[3] c0[2] c1[2]

    xvand.v    D5,     D3,    D3
    xvpermi.q  D5,     D2,    0x31 //c2:2 3, c3:2 3
    xvpermi.d  D8,     D5,    0xd8 //c2[2] c3[2] c2[3] c3[3]
    xvpermi.d  D9,     D5,    0x8d //c2[3] c3[3] c2[2] c3[2]

    xvpermi.q  D6,     D8,    0x02 //c0[2] c1[2] c2[2] c3[2]
    xvpermi.q  D7,     D9,    0x02 //c0[3] c1[3] c2[3] c3[3]

    XVFMADD      D6,    U2,    VALPHAR, D6
    XVFMADD      D7,    U3,    VALPHAR, D7
    XVNMSUB      D6,    U3,    VALPHAI, D6
    XVFMADD      D7,    U2,    VALPHAI, D7

    xvand.v    D4,     D6,    D6
    xvpermi.q  D4,     D7,    0x02 //c0[2] c1[2] c0[3] c1[3]
    xvpermi.d  D4,     D4,    0xd8 //c0[2] c0[3] c1[2] c1[3]

    xvand.v    D5,     D7,    D7
    xvpermi.q  D5,     D6,    0x31 //c2[2] c3[2] c2[3] c3[3]
    xvpermi.d  D5,     D5,    0xd8 //c2[2] c2[3] c3[2] c3[3]

    xvand.v    D0,     D10,    D10
    xvand.v    D1,     D11,    D11

    xvpermi.q  D0,     D4,     0x02 //c0: 0 1 2 3
    xvpermi.q  D4,     D10,    0x31 //c1: 0 1 2 3
    xvpermi.q  D1,     D5,     0x02 //c2: 0 1 2 3
    xvpermi.q  D5,     D11,    0x31 //c3: 0 1 2 3

    xvst       D0,     C0,    0x00
    xvst       D4,     C1,    0x00
    xvst       D1,     C2,    0x00
    xvst       D5,     C3,    0x00

    addi.d     C0,     C0,    0x20
    addi.d     C1,     C1,    0x20
    addi.d     C2,     C2,    0x20
    addi.d     C3,     C3,    0x20
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d      TL,     K,    OFF
#ifdef LEFT
    addi.d     TL,     TL,   -2
#else
    addi.d     TL,     TL,   -4
#endif
    slli.d     T3,     TL,   0x05
    add.d      A0,     A0,   T3
    slli.d     T3,     TL,   0x06
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
    slli.d     T3,     OFF,  0x04
    add.d      A0,     A0,   T3
    slli.d     T3,     OFF,  0x06
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

    xvxor.v    U0,     U0,   U0
    xvxor.v    U1,     U1,   U1

    move       L,      $r0   //cycle param k
    beq        L,      TL,    .L185
    blt        TL,     L,     .L185

.L184:  /* for (k=0; k<temp; k++) */
    xvld       D0,     A0,    0x00  // a0ri a1ri
    xvld       D2,     B0,    0x00  // b0ri b1ri
    xvld       D3,     B0,    0x20  // b2ri b3ri

    xvpermi.d  D4,     D0,    0x00  //a0r
    xvpermi.d  D5,     D0,    0x55  //a0i

    xvpackev.d D6,     D3,    D2
    xvpermi.d  D6,     D6,    0xd8  //b0r b1r b2r b3r

    xvpackod.d D7,     D3,    D2
    xvpermi.d  D7,     D7,    0xd8  //b0i b1i b2i b3i

    XVMADD1    U0,     D4,    D6,     U0  //00r 10r 20r 30r
    XVMADD2    U1,     D5,    D6,     U1  //00i 10i 20i 30i
    XVMADD3    U0,     D5,    D7,     U0
    XVMADD4    U1,     D4,    D7,     U1

    addi.d     A0,     A0,    0x10
    addi.d     B0,     B0,    0x40

    addi.d     L,      L,     1
    blt        L,      TL,    .L184

.L185:
#if defined(TRMMKERNEL)
    //res00 res10 res20 res30
    xvld       D0,     C0,    0x00 //c0: 0 1 2 3
    xvld       D1,     C1,    0x00 //c1: 0 1 2 3
    xvld       D2,     C2,    0x00 //c2: 0 1 2 3
    xvld       D3,     C3,    0x00 //c3: 0 1 2 3

    xvand.v    D4,     D0,    D0
    xvpermi.q  D4,     D1,    0x02 //c0:0 1, c1:0 1
    xvpermi.d  D6,     D4,    0xd8 //c0[0] c1[0] c0[1] c1[1]
    xvpermi.d  D7,     D4,    0x8d //c0[1] c1[1] c0[0] c1[0]

    xvand.v    D5,     D2,    D2
    xvpermi.q  D5,     D3,    0x02 //c2:0 1, c3:0 1
    xvpermi.d  D8,     D5,    0xd8 //c2[0] c3[0] c2[1] c3[1]
    xvpermi.d  D9,     D5,    0x8d //c2[1] c3[1] c2[0] c3[0]

    xvpermi.q  D6,     D8,    0x02 //c0[0] c1[0] c2[0] c3[0]
    xvpermi.q  D7,     D9,    0x02 //c0[1] c1[1] c2[1] c3[1]

    xvfmul.d      D6,    U0,    VALPHAR
    xvfmul.d      D7,    U1,    VALPHAR
    XVNMSUB      D6,    U1,    VALPHAI, D6
    XVFMADD      D7,    U0,    VALPHAI, D7

    xvand.v    D10,     D6,    D6
    xvpermi.q  D10,     D7,    0x02 //c0[0] c1[0] c0[1] c1[1]
    xvpermi.d  D10,     D10,   0xd8 //c0[0] c0[1] c1[0] c1[1]

    xvand.v    D11,     D7,    D7
    xvpermi.q  D11,     D6,    0x31 //c2[0] c3[0] c2[1] c3[1]
    xvpermi.d  D11,     D11,   0xd8 //c2[0] c2[1] c3[0] c3[1]

    xvpermi.d  D8,     D10,   0x4e //c1[0] c1[1] c0[0] c0[1]
    xvpermi.d  D9,     D11,   0x4e //c3[0] c3[1] c2[0] c2[1]

    vst       $vr26,     C0,    0x00
    vst       $vr24,     C1,    0x00
    vst       $vr27,     C2,    0x00
    vst       $vr25,     C3,    0x00

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10
    addi.d     C2,     C2,    0x10
    addi.d     C3,     C3,    0x10
#else
    //res00 res10 res20 res30
    xvld       D0,     C0,    0x00 //c0: 0 1 2 3
    xvld       D1,     C1,    0x00 //c1: 0 1 2 3
    xvld       D2,     C2,    0x00 //c2: 0 1 2 3
    xvld       D3,     C3,    0x00 //c3: 0 1 2 3

    xvand.v    D4,     D0,    D0
    xvpermi.q  D4,     D1,    0x02 //c0:0 1, c1:0 1
    xvpermi.d  D6,     D4,    0xd8 //c0[0] c1[0] c0[1] c1[1]
    xvpermi.d  D7,     D4,    0x8d //c0[1] c1[1] c0[0] c1[0]

    xvand.v    D5,     D2,    D2
    xvpermi.q  D5,     D3,    0x02 //c2:0 1, c3:0 1
    xvpermi.d  D8,     D5,    0xd8 //c2[0] c3[0] c2[1] c3[1]
    xvpermi.d  D9,     D5,    0x8d //c2[1] c3[1] c2[0] c3[0]

    xvpermi.q  D6,     D8,    0x02 //c0[0] c1[0] c2[0] c3[0]
    xvpermi.q  D7,     D9,    0x02 //c0[1] c1[1] c2[1] c3[1]

    XVFMADD      D6,    U0,    VALPHAR, D6
    XVFMADD      D7,    U1,    VALPHAR, D7
    XVNMSUB      D6,    U1,    VALPHAI, D6
    XVFMADD      D7,    U0,    VALPHAI, D7

    xvand.v    D10,     D6,    D6
    xvpermi.q  D10,     D7,    0x02 //c0[0] c1[0] c0[1] c1[1]
    xvpermi.d  D10,     D10,   0xd8 //c0[0] c0[1] c1[0] c1[1]

    xvand.v    D11,     D7,    D7
    xvpermi.q  D11,     D6,    0x31 //c2[0] c3[0] c2[1] c3[1]
    xvpermi.d  D11,     D11,   0xd8 //c2[0] c2[1] c3[0] c3[1]

    xvpermi.d  D8,     D10,   0x4e //c1[0] c1[1] c0[0] c0[1]
    xvpermi.d  D9,     D11,   0x4e //c3[0] c3[1] c2[0] c2[1]

    vst       $vr26,     C0,    0x00
    vst       $vr24,     C1,    0x00
    vst       $vr27,     C2,    0x00
    vst       $vr25,     C3,    0x00

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10
    addi.d     C2,     C2,    0x10
    addi.d     C3,     C3,    0x10
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d      TL,     K,    OFF
#ifdef LEFT
    addi.d     TL,     TL,   -1
#else
    addi.d     TL,     TL,   -4
#endif
    slli.d     T3,     TL,   0x04
    add.d      A0,     A0,   T3
    slli.d     T3,     TL,   0x06
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

    slli.d     L,      K,     0x06
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
    slli.d     T3,     OFF,  0x07
    add.d      A0,     A0,   T3
    slli.d     T3,     OFF,  0x05
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

    vxor.v    $vr30,     $vr30,   $vr30
    vxor.v    $vr31,     $vr31,   $vr31
    vxor.v    $vr2,      $vr2,    $vr2
    vxor.v    $vr3,      $vr3,    $vr3
    vxor.v    $vr4,      $vr4,    $vr4
    vxor.v    $vr5,      $vr5,    $vr5
    vxor.v    $vr6,      $vr6,    $vr6
    vxor.v    $vr7,      $vr7,    $vr7
    vxor.v    $vr8,      $vr8,    $vr8
    vxor.v    $vr9,      $vr9,    $vr9
    vxor.v    $vr10,      $vr10,    $vr10
    vxor.v    $vr11,      $vr11,    $vr11
    vxor.v    $vr12,      $vr12,    $vr12
    vxor.v    $vr13,      $vr13,    $vr13
    vxor.v    $vr14,      $vr14,    $vr14
    vxor.v    $vr15,      $vr15,    $vr15

    move       L,      $r0   //cycle param k
    beq        L,      TL,    .L23
    blt        TL,     L,     .L23

.L22:  /* for (k=0; k<temp; k++) */
    vld       $vr16,     A0,    0x00  // a0ri

    vld       $vr18,     B0,    0x00  // b0ri
    vld       $vr19,     B0,    0x10  // b1ri

    vshuf4i.d  $vr21,     $vr16,    0x0a  //a0rr
    vshuf4i.d  $vr22,     $vr16,    0x0f  //a0ii

    vand.v    $vr23,     $vr18,    $vr18
    vshuf4i.d $vr23,     $vr19,    0x08 //b0r b1r
    vshuf4i.d $vr18,     $vr19,    0x0d //b0i b1i

    VMADD1    $vr30,     $vr21,    $vr23,     $vr30  //00r 10r
    VMADD2    $vr31,     $vr22,    $vr23,     $vr31  //00i 10i
    VMADD3    $vr30,     $vr22,    $vr18,     $vr30
    VMADD4    $vr31,     $vr21,    $vr18,     $vr31

    vld       $vr16,     A0,    0x10  // a1ri

    vshuf4i.d  $vr21,     $vr16,    0x0a  //a1rr
    vshuf4i.d  $vr22,     $vr16,    0x0f  //a1ii

    VMADD1    $vr2,     $vr21,    $vr23,     $vr2  //01r 11r
    VMADD2    $vr3,     $vr22,    $vr23,     $vr3  //01i 11i
    VMADD3    $vr2,     $vr22,    $vr18,     $vr2
    VMADD4    $vr3,     $vr21,    $vr18,     $vr3

    vld       $vr16,     A0,    0x20  // a2ri

    vshuf4i.d  $vr21,     $vr16,    0x0a  //a2rr
    vshuf4i.d  $vr22,     $vr16,    0x0f  //a2ii

    VMADD1    $vr4,     $vr21,    $vr23,     $vr4  //02r 12r
    VMADD2    $vr5,     $vr22,    $vr23,     $vr5  //02i 12i
    VMADD3    $vr4,     $vr22,    $vr18,     $vr4
    VMADD4    $vr5,     $vr21,    $vr18,     $vr5

    vld       $vr16,     A0,    0x30  // a3ri

    vshuf4i.d  $vr21,     $vr16,    0x0a  //a3rr
    vshuf4i.d  $vr22,     $vr16,    0x0f  //a3ii

    VMADD1    $vr6,     $vr21,    $vr23,     $vr6  //03r 13r
    VMADD2    $vr7,     $vr22,    $vr23,     $vr7  //03i 13i
    VMADD3    $vr6,     $vr22,    $vr18,     $vr6
    VMADD4    $vr7,     $vr21,    $vr18,     $vr7

    vld       $vr16,     A0,    0x40  // a4ri

    vshuf4i.d  $vr21,     $vr16,    0x0a  //a4rr
    vshuf4i.d  $vr22,     $vr16,    0x0f  //a4ii

    VMADD1    $vr8,     $vr21,    $vr23,     $vr8  //04r 14r
    VMADD2    $vr9,     $vr22,    $vr23,     $vr9  //04i 14i
    VMADD3    $vr8,     $vr22,    $vr18,     $vr8
    VMADD4    $vr9,     $vr21,    $vr18,     $vr9

    vld       $vr16,     A0,    0x50  // a5ri

    vshuf4i.d  $vr21,     $vr16,    0x0a  //a5rr
    vshuf4i.d  $vr22,     $vr16,    0x0f  //a5ii

    VMADD1    $vr10,     $vr21,    $vr23,     $vr10  //05r 15r
    VMADD2    $vr11,     $vr22,    $vr23,     $vr11  //05i 15i
    VMADD3    $vr10,     $vr22,    $vr18,     $vr10
    VMADD4    $vr11,     $vr21,    $vr18,     $vr11

    vld       $vr16,     A0,    0x60  // a6ri

    vshuf4i.d  $vr21,     $vr16,    0x0a  //a6rr
    vshuf4i.d  $vr22,     $vr16,    0x0f  //a6ii

    VMADD1    $vr12,     $vr21,    $vr23,     $vr12  //06r 16r
    VMADD2    $vr13,     $vr22,    $vr23,     $vr13  //06i 16i
    VMADD3    $vr12,     $vr22,    $vr18,     $vr12
    VMADD4    $vr13,     $vr21,    $vr18,     $vr13

    vld       $vr16,     A0,    0x70  // a7ri

    vshuf4i.d  $vr21,     $vr16,    0x0a  //a7rr
    vshuf4i.d  $vr22,     $vr16,    0x0f  //a7ii

    VMADD1    $vr14,     $vr21,    $vr23,     $vr14  //07r 17r
    VMADD2    $vr15,     $vr22,    $vr23,     $vr15  //07i 17i
    VMADD3    $vr14,     $vr22,    $vr18,     $vr14
    VMADD4    $vr15,     $vr21,    $vr18,     $vr15

    addi.d     A0,     A0,    0x80
    addi.d     B0,     B0,    0x20

    addi.d     L,      L,     1
    blt        L,      TL,    .L22

.L23:
#if defined(TRMMKERNEL)
    //res00 res10
    vld       $vr16,     C0,    0x00 //c0: 0 1
    vld       $vr17,     C1,    0x00 //c1: 0 1

    vpackev.d $vr18,     $vr17,    $vr16
    vpackod.d $vr19,     $vr17,    $vr16

    vfmul.d      $vr18,    $vr30,    $vr28
    vfmul.d      $vr19,    $vr31,    $vr28
    VNMSUB      $vr18,    $vr31,    $vr29, $vr18
    VFMADD      $vr19,    $vr30,    $vr29, $vr19

    vpackev.d $vr16,     $vr19,    $vr18
    vpackod.d $vr17,     $vr19,    $vr18

    vst       $vr16,     C0,    0x00 //c0: 0 1
    vst       $vr17,     C1,    0x00 //c1: 0 1

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10

    //res01 res11
    vld       $vr16,     C0,    0x00 //c0: 0 1
    vld       $vr17,     C1,    0x00 //c1: 0 1

    vpackev.d $vr18,     $vr17,    $vr16
    vpackod.d $vr19,     $vr17,    $vr16

    vfmul.d      $vr18,    $vr2,    $vr28
    vfmul.d      $vr19,    $vr3,    $vr28
    VNMSUB      $vr18,    $vr3,    $vr29, $vr18
    VFMADD      $vr19,    $vr2,    $vr29, $vr19

    vpackev.d $vr16,     $vr19,    $vr18
    vpackod.d $vr17,     $vr19,    $vr18

    vst       $vr16,     C0,    0x00 //c0: 0 1
    vst       $vr17,     C1,    0x00 //c1: 0 1

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10

    //res02 res12
    vld       $vr16,     C0,    0x00 //c0: 0 1
    vld       $vr17,     C1,    0x00 //c1: 0 1

    vpackev.d $vr18,     $vr17,    $vr16
    vpackod.d $vr19,     $vr17,    $vr16

    vfmul.d      $vr18,    $vr4,    $vr28
    vfmul.d      $vr19,    $vr5,    $vr28
    VNMSUB      $vr18,    $vr5,    $vr29, $vr18
    VFMADD      $vr19,    $vr4,    $vr29, $vr19

    vpackev.d $vr16,     $vr19,    $vr18
    vpackod.d $vr17,     $vr19,    $vr18

    vst       $vr16,     C0,    0x00 //c0: 0 1
    vst       $vr17,     C1,    0x00 //c1: 0 1

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10

    //res03 res13
    vld       $vr16,     C0,    0x00 //c0: 0 1
    vld       $vr17,     C1,    0x00 //c1: 0 1

    vpackev.d $vr18,     $vr17,    $vr16
    vpackod.d $vr19,     $vr17,    $vr16

    vfmul.d      $vr18,    $vr6,    $vr28
    vfmul.d      $vr19,    $vr7,    $vr28
    VNMSUB      $vr18,    $vr7,    $vr29, $vr18
    VFMADD      $vr19,    $vr6,    $vr29, $vr19

    vpackev.d $vr16,     $vr19,    $vr18
    vpackod.d $vr17,     $vr19,    $vr18

    vst       $vr16,     C0,    0x00 //c0: 0 1
    vst       $vr17,     C1,    0x00 //c1: 0 1

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10

    //res04 res14
    vld       $vr16,     C0,    0x00 //c0: 0 1
    vld       $vr17,     C1,    0x00 //c1: 0 1

    vpackev.d $vr18,     $vr17,    $vr16
    vpackod.d $vr19,     $vr17,    $vr16

    vfmul.d      $vr18,    $vr8,    $vr28
    vfmul.d      $vr19,    $vr9,    $vr28
    VNMSUB      $vr18,    $vr9,    $vr29, $vr18
    VFMADD      $vr19,    $vr8,    $vr29, $vr19

    vpackev.d $vr16,     $vr19,    $vr18
    vpackod.d $vr17,     $vr19,    $vr18

    vst       $vr16,     C0,    0x00 //c0: 0 1
    vst       $vr17,     C1,    0x00 //c1: 0 1

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10

    //res05 res15
    vld       $vr16,     C0,    0x00 //c0: 0 1
    vld       $vr17,     C1,    0x00 //c1: 0 1

    vpackev.d $vr18,     $vr17,    $vr16
    vpackod.d $vr19,     $vr17,    $vr16

    vfmul.d      $vr18,    $vr10,    $vr28
    vfmul.d      $vr19,    $vr11,    $vr28
    VNMSUB      $vr18,    $vr11,    $vr29, $vr18
    VFMADD      $vr19,    $vr10,    $vr29, $vr19

    vpackev.d $vr16,     $vr19,    $vr18
    vpackod.d $vr17,     $vr19,    $vr18

    vst       $vr16,     C0,    0x00 //c0: 0 1
    vst       $vr17,     C1,    0x00 //c1: 0 1

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10

    //res06 res16
    vld       $vr16,     C0,    0x00 //c0: 0 1
    vld       $vr17,     C1,    0x00 //c1: 0 1

    vpackev.d $vr18,     $vr17,    $vr16
    vpackod.d $vr19,     $vr17,    $vr16

    vfmul.d      $vr18,    $vr12,    $vr28
    vfmul.d      $vr19,    $vr13,    $vr28
    VNMSUB      $vr18,    $vr13,    $vr29, $vr18
    VFMADD      $vr19,    $vr12,    $vr29, $vr19

    vpackev.d $vr16,     $vr19,    $vr18
    vpackod.d $vr17,     $vr19,    $vr18

    vst       $vr16,     C0,    0x00 //c0: 0 1
    vst       $vr17,     C1,    0x00 //c1: 0 1

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10

    //res07 res17
    vld       $vr16,     C0,    0x00 //c0: 0 1
    vld       $vr17,     C1,    0x00 //c1: 0 1

    vpackev.d $vr18,     $vr17,    $vr16
    vpackod.d $vr19,     $vr17,    $vr16

    vfmul.d      $vr18,    $vr14,    $vr28
    vfmul.d      $vr19,    $vr15,    $vr28
    VNMSUB      $vr18,    $vr15,    $vr29, $vr18
    VFMADD      $vr19,    $vr14,    $vr29, $vr19

    vpackev.d $vr16,     $vr19,    $vr18
    vpackod.d $vr17,     $vr19,    $vr18

    vst       $vr16,     C0,    0x00 //c0: 0 1
    vst       $vr17,     C1,    0x00 //c1: 0 1

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10
#else
    //res00 res10
    vld       $vr16,     C0,    0x00 //c0: 0 1
    vld       $vr17,     C1,    0x00 //c1: 0 1

    vpackev.d $vr18,     $vr17,    $vr16
    vpackod.d $vr19,     $vr17,    $vr16

    VFMADD      $vr18,    $vr30,    $vr28, $vr18
    VFMADD      $vr19,    $vr31,    $vr28, $vr19
    VNMSUB      $vr18,    $vr31,    $vr29, $vr18
    VFMADD      $vr19,    $vr30,    $vr29, $vr19

    vpackev.d $vr16,     $vr19,    $vr18
    vpackod.d $vr17,     $vr19,    $vr18

    vst       $vr16,     C0,    0x00 //c0: 0 1
    vst       $vr17,     C1,    0x00 //c1: 0 1

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10

    //res01 res11
    vld       $vr16,     C0,    0x00 //c0: 0 1
    vld       $vr17,     C1,    0x00 //c1: 0 1

    vpackev.d $vr18,     $vr17,    $vr16
    vpackod.d $vr19,     $vr17,    $vr16

    VFMADD      $vr18,    $vr2,    $vr28, $vr18
    VFMADD      $vr19,    $vr3,    $vr28, $vr19
    VNMSUB      $vr18,    $vr3,    $vr29, $vr18
    VFMADD      $vr19,    $vr2,    $vr29, $vr19

    vpackev.d $vr16,     $vr19,    $vr18
    vpackod.d $vr17,     $vr19,    $vr18

    vst       $vr16,     C0,    0x00 //c0: 0 1
    vst       $vr17,     C1,    0x00 //c1: 0 1

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10

    //res02 res12
    vld       $vr16,     C0,    0x00 //c0: 0 1
    vld       $vr17,     C1,    0x00 //c1: 0 1

    vpackev.d $vr18,     $vr17,    $vr16
    vpackod.d $vr19,     $vr17,    $vr16

    VFMADD      $vr18,    $vr4,    $vr28, $vr18
    VFMADD      $vr19,    $vr5,    $vr28, $vr19
    VNMSUB      $vr18,    $vr5,    $vr29, $vr18
    VFMADD      $vr19,    $vr4,    $vr29, $vr19

    vpackev.d $vr16,     $vr19,    $vr18
    vpackod.d $vr17,     $vr19,    $vr18

    vst       $vr16,     C0,    0x00 //c0: 0 1
    vst       $vr17,     C1,    0x00 //c1: 0 1

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10

    //res03 res13
    vld       $vr16,     C0,    0x00 //c0: 0 1
    vld       $vr17,     C1,    0x00 //c1: 0 1

    vpackev.d $vr18,     $vr17,    $vr16
    vpackod.d $vr19,     $vr17,    $vr16

    VFMADD      $vr18,    $vr6,    $vr28, $vr18
    VFMADD      $vr19,    $vr7,    $vr28, $vr19
    VNMSUB      $vr18,    $vr7,    $vr29, $vr18
    VFMADD      $vr19,    $vr6,    $vr29, $vr19

    vpackev.d $vr16,     $vr19,    $vr18
    vpackod.d $vr17,     $vr19,    $vr18

    vst       $vr16,     C0,    0x00 //c0: 0 1
    vst       $vr17,     C1,    0x00 //c1: 0 1

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10

    //res04 res14
    vld       $vr16,     C0,    0x00 //c0: 0 1
    vld       $vr17,     C1,    0x00 //c1: 0 1

    vpackev.d $vr18,     $vr17,    $vr16
    vpackod.d $vr19,     $vr17,    $vr16

    VFMADD      $vr18,    $vr8,    $vr28, $vr18
    VFMADD      $vr19,    $vr9,    $vr28, $vr19
    VNMSUB      $vr18,    $vr9,    $vr29, $vr18
    VFMADD      $vr19,    $vr8,    $vr29, $vr19

    vpackev.d $vr16,     $vr19,    $vr18
    vpackod.d $vr17,     $vr19,    $vr18

    vst       $vr16,     C0,    0x00 //c0: 0 1
    vst       $vr17,     C1,    0x00 //c1: 0 1

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10

    //res05 res15
    vld       $vr16,     C0,    0x00 //c0: 0 1
    vld       $vr17,     C1,    0x00 //c1: 0 1

    vpackev.d $vr18,     $vr17,    $vr16
    vpackod.d $vr19,     $vr17,    $vr16

    VFMADD      $vr18,    $vr10,    $vr28, $vr18
    VFMADD      $vr19,    $vr11,    $vr28, $vr19
    VNMSUB      $vr18,    $vr11,    $vr29, $vr18
    VFMADD      $vr19,    $vr10,    $vr29, $vr19

    vpackev.d $vr16,     $vr19,    $vr18
    vpackod.d $vr17,     $vr19,    $vr18

    vst       $vr16,     C0,    0x00 //c0: 0 1
    vst       $vr17,     C1,    0x00 //c1: 0 1

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10

    //res06 res16
    vld       $vr16,     C0,    0x00 //c0: 0 1
    vld       $vr17,     C1,    0x00 //c1: 0 1

    vpackev.d $vr18,     $vr17,    $vr16
    vpackod.d $vr19,     $vr17,    $vr16

    VFMADD      $vr18,    $vr12,    $vr28, $vr18
    VFMADD      $vr19,    $vr13,    $vr28, $vr19
    VNMSUB      $vr18,    $vr13,    $vr29, $vr18
    VFMADD      $vr19,    $vr12,    $vr29, $vr19

    vpackev.d $vr16,     $vr19,    $vr18
    vpackod.d $vr17,     $vr19,    $vr18

    vst       $vr16,     C0,    0x00 //c0: 0 1
    vst       $vr17,     C1,    0x00 //c1: 0 1

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10

    //res07 res17
    vld       $vr16,     C0,    0x00 //c0: 0 1
    vld       $vr17,     C1,    0x00 //c1: 0 1

    vpackev.d $vr18,     $vr17,    $vr16
    vpackod.d $vr19,     $vr17,    $vr16

    VFMADD      $vr18,    $vr14,    $vr28, $vr18
    VFMADD      $vr19,    $vr15,    $vr28, $vr19
    VNMSUB      $vr18,    $vr15,    $vr29, $vr18
    VFMADD      $vr19,    $vr14,    $vr29, $vr19

    vpackev.d $vr16,     $vr19,    $vr18
    vpackod.d $vr17,     $vr19,    $vr18

    vst       $vr16,     C0,    0x00 //c0: 0 1
    vst       $vr17,     C1,    0x00 //c1: 0 1

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
    slli.d     T3,     TL,   0x07
    add.d      A0,     A0,   T3
    slli.d     T3,     TL,   0x05
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
    slli.d     T3,     OFF,   0x06
    add.d      A0,     A0,    T3
    slli.d     T3,     OFF,   0x05
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

    vxor.v    $vr30,     $vr30,   $vr30
    vxor.v    $vr31,     $vr31,   $vr31
    vxor.v    $vr2,      $vr2,    $vr2
    vxor.v    $vr3,      $vr3,    $vr3
    vxor.v    $vr4,      $vr4,    $vr4
    vxor.v    $vr5,      $vr5,    $vr5
    vxor.v    $vr6,      $vr6,    $vr6
    vxor.v    $vr7,      $vr7,    $vr7

    move       L,      $r0   //cycle param k
    beq        L,      TL,    .L27
    blt        TL,     L,     .L27

.L26:  /* for (k=0; k<temp; k++) */
    vld       $vr16,     A0,    0x00  // a0ri

    vld       $vr18,     B0,    0x00  // b0ri
    vld       $vr19,     B0,    0x10  // b1ri

    vshuf4i.d  $vr21,     $vr16,    0x0a  //a0rr
    vshuf4i.d  $vr22,     $vr16,    0x0f  //a0ii

    vand.v    $vr23,     $vr18,    $vr18
    vshuf4i.d $vr23,     $vr19,    0x08 //b0r b1r
    vshuf4i.d $vr18,     $vr19,    0x0d //b0i b1i

    VMADD1    $vr30,     $vr21,    $vr23,     $vr30  //00r 10r
    VMADD2    $vr31,     $vr22,    $vr23,     $vr31  //00i 10i
    VMADD3    $vr30,     $vr22,    $vr18,     $vr30
    VMADD4    $vr31,     $vr21,    $vr18,     $vr31

    vld       $vr16,     A0,    0x10  // a1ri

    vshuf4i.d  $vr21,     $vr16,    0x0a  //a1rr
    vshuf4i.d  $vr22,     $vr16,    0x0f  //a1ii

    VMADD1    $vr2,     $vr21,    $vr23,     $vr2  //01r 11r
    VMADD2    $vr3,     $vr22,    $vr23,     $vr3  //01i 11i
    VMADD3    $vr2,     $vr22,    $vr18,     $vr2
    VMADD4    $vr3,     $vr21,    $vr18,     $vr3

    vld       $vr16,     A0,    0x20  // a2ri

    vshuf4i.d  $vr21,     $vr16,    0x0a  //a2rr
    vshuf4i.d  $vr22,     $vr16,    0x0f  //a2ii

    VMADD1    $vr4,     $vr21,    $vr23,     $vr4  //02r 12r
    VMADD2    $vr5,     $vr22,    $vr23,     $vr5  //02i 12i
    VMADD3    $vr4,     $vr22,    $vr18,     $vr4
    VMADD4    $vr5,     $vr21,    $vr18,     $vr5

    vld       $vr16,     A0,    0x30  // a3ri

    vshuf4i.d  $vr21,     $vr16,    0x0a  //a3rr
    vshuf4i.d  $vr22,     $vr16,    0x0f  //a3ii

    VMADD1    $vr6,     $vr21,    $vr23,     $vr6  //03r 13r
    VMADD2    $vr7,     $vr22,    $vr23,     $vr7  //03i 13i
    VMADD3    $vr6,     $vr22,    $vr18,     $vr6
    VMADD4    $vr7,     $vr21,    $vr18,     $vr7

    addi.d     A0,     A0,    0x40
    addi.d     B0,     B0,    0x20

    addi.d     L,      L,     1
    blt        L,      TL,    .L26

.L27:
#if defined(TRMMKERNEL)
    //res00 res10
    vld       $vr16,     C0,    0x00 //c0: 0 1
    vld       $vr17,     C1,    0x00 //c1: 0 1

    vpackev.d $vr18,     $vr17,    $vr16
    vpackod.d $vr19,     $vr17,    $vr16

    vfmul.d      $vr18,    $vr30,    $vr28
    vfmul.d      $vr19,    $vr31,    $vr28
    VNMSUB      $vr18,    $vr31,    $vr29, $vr18
    VFMADD      $vr19,    $vr30,    $vr29, $vr19

    vpackev.d $vr16,     $vr19,    $vr18
    vpackod.d $vr17,     $vr19,    $vr18

    vst       $vr16,     C0,    0x00 //c0: 0 1
    vst       $vr17,     C1,    0x00 //c1: 0 1

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10

    //res01 res11
    vld       $vr16,     C0,    0x00 //c0: 0 1
    vld       $vr17,     C1,    0x00 //c1: 0 1

    vpackev.d $vr18,     $vr17,    $vr16
    vpackod.d $vr19,     $vr17,    $vr16

    vfmul.d      $vr18,    $vr2,    $vr28
    vfmul.d      $vr19,    $vr3,    $vr28
    VNMSUB      $vr18,    $vr3,    $vr29, $vr18
    VFMADD      $vr19,    $vr2,    $vr29, $vr19

    vpackev.d $vr16,     $vr19,    $vr18
    vpackod.d $vr17,     $vr19,    $vr18

    vst       $vr16,     C0,    0x00 //c0: 0 1
    vst       $vr17,     C1,    0x00 //c1: 0 1

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10

    //res02 res12
    vld       $vr16,     C0,    0x00 //c0: 0 1
    vld       $vr17,     C1,    0x00 //c1: 0 1

    vpackev.d $vr18,     $vr17,    $vr16
    vpackod.d $vr19,     $vr17,    $vr16

    vfmul.d      $vr18,    $vr4,    $vr28
    vfmul.d      $vr19,    $vr5,    $vr28
    VNMSUB      $vr18,    $vr5,    $vr29, $vr18
    VFMADD      $vr19,    $vr4,    $vr29, $vr19

    vpackev.d $vr16,     $vr19,    $vr18
    vpackod.d $vr17,     $vr19,    $vr18

    vst       $vr16,     C0,    0x00 //c0: 0 1
    vst       $vr17,     C1,    0x00 //c1: 0 1

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10

    //res03 res13
    vld       $vr16,     C0,    0x00 //c0: 0 1
    vld       $vr17,     C1,    0x00 //c1: 0 1

    vpackev.d $vr18,     $vr17,    $vr16
    vpackod.d $vr19,     $vr17,    $vr16

    vfmul.d      $vr18,    $vr6,    $vr28
    vfmul.d      $vr19,    $vr7,    $vr28
    VNMSUB      $vr18,    $vr7,    $vr29, $vr18
    VFMADD      $vr19,    $vr6,    $vr29, $vr19

    vpackev.d $vr16,     $vr19,    $vr18
    vpackod.d $vr17,     $vr19,    $vr18

    vst       $vr16,     C0,    0x00 //c0: 0 1
    vst       $vr17,     C1,    0x00 //c1: 0 1

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10
#else
    //res00 res10
    vld       $vr16,     C0,    0x00 //c0: 0 1
    vld       $vr17,     C1,    0x00 //c1: 0 1

    vpackev.d $vr18,     $vr17,    $vr16
    vpackod.d $vr19,     $vr17,    $vr16

    VFMADD      $vr18,    $vr30,    $vr28, $vr18
    VFMADD      $vr19,    $vr31,    $vr28, $vr19
    VNMSUB      $vr18,    $vr31,    $vr29, $vr18
    VFMADD      $vr19,    $vr30,    $vr29, $vr19

    vpackev.d $vr16,     $vr19,    $vr18
    vpackod.d $vr17,     $vr19,    $vr18

    vst       $vr16,     C0,    0x00 //c0: 0 1
    vst       $vr17,     C1,    0x00 //c1: 0 1

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10

    //res01 res11
    vld       $vr16,     C0,    0x00 //c0: 0 1
    vld       $vr17,     C1,    0x00 //c1: 0 1

    vpackev.d $vr18,     $vr17,    $vr16
    vpackod.d $vr19,     $vr17,    $vr16

    VFMADD      $vr18,    $vr2,    $vr28, $vr18
    VFMADD      $vr19,    $vr3,    $vr28, $vr19
    VNMSUB      $vr18,    $vr3,    $vr29, $vr18
    VFMADD      $vr19,    $vr2,    $vr29, $vr19

    vpackev.d $vr16,     $vr19,    $vr18
    vpackod.d $vr17,     $vr19,    $vr18

    vst       $vr16,     C0,    0x00 //c0: 0 1
    vst       $vr17,     C1,    0x00 //c1: 0 1

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10

    //res02 res12
    vld       $vr16,     C0,    0x00 //c0: 0 1
    vld       $vr17,     C1,    0x00 //c1: 0 1

    vpackev.d $vr18,     $vr17,    $vr16
    vpackod.d $vr19,     $vr17,    $vr16

    VFMADD      $vr18,    $vr4,    $vr28, $vr18
    VFMADD      $vr19,    $vr5,    $vr28, $vr19
    VNMSUB      $vr18,    $vr5,    $vr29, $vr18
    VFMADD      $vr19,    $vr4,    $vr29, $vr19

    vpackev.d $vr16,     $vr19,    $vr18
    vpackod.d $vr17,     $vr19,    $vr18

    vst       $vr16,     C0,    0x00 //c0: 0 1
    vst       $vr17,     C1,    0x00 //c1: 0 1

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10

    //res03 res13
    vld       $vr16,     C0,    0x00 //c0: 0 1
    vld       $vr17,     C1,    0x00 //c1: 0 1

    vpackev.d $vr18,     $vr17,    $vr16
    vpackod.d $vr19,     $vr17,    $vr16

    VFMADD      $vr18,    $vr6,    $vr28, $vr18
    VFMADD      $vr19,    $vr7,    $vr28, $vr19
    VNMSUB      $vr18,    $vr7,    $vr29, $vr18
    VFMADD      $vr19,    $vr6,    $vr29, $vr19

    vpackev.d $vr16,     $vr19,    $vr18
    vpackod.d $vr17,     $vr19,    $vr18

    vst       $vr16,     C0,    0x00 //c0: 0 1
    vst       $vr17,     C1,    0x00 //c1: 0 1

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
    slli.d     T3,     TL,   0x06
    add.d      A0,     A0,   T3
    slli.d     T3,     TL,   0x05
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
    slli.d     T3,     OFF,   0x05
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

    xvxor.v    U0,     U0,   U0
    xvxor.v    U1,     U1,   U1

    move       L,      $r0   //cycle param k
    beq        L,      TL,    .L283
    blt        TL,     L,     .L283

.L282:  /* for (k=0; k<temp; k++) */
    xvld       D0,     A0,    0x00  // a0ri a1ri
    xvld       D2,     B0,    0x00  // b0ri b1ri

    xvpermi.d  D1,     D0,    0xf5  //a0ii a1ii
    xvpermi.d  D0,     D0,    0xa0  //a0rr a1rr

    xvpermi.d  D3,     D2,    0xdd  //b0i b1i b0i b1i
    xvpermi.d  D2,     D2,    0x88  //b0r b1r b0r b1r

    XVMADD1    U0,     D0,    D2,     U0  //00r 10r 01r 11r
    XVMADD2    U1,     D1,    D2,     U1  //00i 10i 01i 11i
    XVMADD3    U0,     D1,    D3,     U0
    XVMADD4    U1,     D0,    D3,     U1

    addi.d     A0,     A0,    0x20
    addi.d     B0,     B0,    0x20

    addi.d     L,      L,     1
    blt        L,      TL,    .L282

.L283:
#if defined(TRMMKERNEL)
    //res00 res10 res01 res11
    xvld       D0,     C0,    0x00 //c0: 0 1 2 3
    xvld       D1,     C1,    0x00 //c1: 0 1 2 3

    xvpackev.d D2,     D1,    D0  //0 4 2 6
    xvpackod.d D3,     D1,    D0  //1 5 3 7

    xvfmul.d      D2,    U0,    VALPHAR
    xvfmul.d      D3,    U1,    VALPHAR
    XVNMSUB      D2,    U1,    VALPHAI, D2
    XVFMADD      D3,    U0,    VALPHAI, D3

    xvpackev.d D4,     D3,    D2  //0 1 2 3
    xvpackod.d D5,     D3,    D2  //4 5 6 7

    xvst       D4,     C0,    0x00 //c0: 0 1 2 3
    xvst       D5,     C1,    0x00 //c1: 0 1 2 3

    addi.d     C0,     C0,    0x20
    addi.d     C1,     C1,    0x20
#else
    //res00 res10 res01 res11
    xvld       D0,     C0,    0x00 //c0: 0 1 2 3
    xvld       D1,     C1,    0x00 //c1: 0 1 2 3

    xvpackev.d D2,     D1,    D0  //0 4 2 6
    xvpackod.d D3,     D1,    D0  //1 5 3 7

    XVFMADD      D2,    U0,    VALPHAR, D2
    XVFMADD      D3,    U1,    VALPHAR, D3
    XVNMSUB      D2,    U1,    VALPHAI, D2
    XVFMADD      D3,    U0,    VALPHAI, D3

    xvpackev.d D4,     D3,    D2  //0 1 2 3
    xvpackod.d D5,     D3,    D2  //4 5 6 7

    xvst       D4,     C0,    0x00 //c0: 0 1 2 3
    xvst       D5,     C1,    0x00 //c1: 0 1 2 3

    addi.d     C0,     C0,    0x20
    addi.d     C1,     C1,    0x20
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d      TL,     K,    OFF
#ifdef LEFT
    addi.d     TL,     TL,   -2
#else
    addi.d     TL,     TL,   -2
#endif
    slli.d     T3,     TL,   0x05
    add.d      A0,     A0,   T3
    slli.d     T3,     TL,   0x05
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
    slli.d     T3,     OFF,   0x04
    add.d      A0,     A0,    T3
    slli.d     T3,     OFF,   0x05
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

    vxor.v    $vr30,     $vr30,   $vr30
    vxor.v    $vr31,     $vr31,   $vr31

    move       L,      $r0   //cycle param k
    beq        L,      TL,    .L287
    blt        TL,     L,     .L287

.L286:  /* for (k=0; k<temp; k++) */
    vld       $vr16,     A0,    0x00  // a0ri

    vld       $vr18,     B0,    0x00  // b0ri
    vld       $vr19,     B0,    0x10  // b1ri

    vshuf4i.d  $vr21,     $vr16,    0x0a  //a0rr
    vshuf4i.d  $vr22,     $vr16,    0x0f  //a0ii

    vand.v    $vr23,     $vr18,    $vr18
    vshuf4i.d $vr23,     $vr19,    0x08 //b0r b1r
    vshuf4i.d $vr18,     $vr19,    0x0d //b0i b1i

    VMADD1    $vr30,     $vr21,    $vr23,     $vr30  //00r 10r
    VMADD2    $vr31,     $vr22,    $vr23,     $vr31  //00i 10i
    VMADD3    $vr30,     $vr22,    $vr18,     $vr30
    VMADD4    $vr31,     $vr21,    $vr18,     $vr31

    addi.d     A0,     A0,    0x10
    addi.d     B0,     B0,    0x20

    addi.d     L,      L,     1
    blt        L,      TL,    .L286

.L287:
#if defined(TRMMKERNEL)
    //res00 res10
    vld       $vr16,     C0,    0x00 //c0: 0 1
    vld       $vr17,     C1,    0x00 //c1: 0 1

    vand.v    $vr18,     $vr16,    $vr16
    vshuf4i.d $vr18,     $vr17,    0x08 //c0[0] c1[0]
    vshuf4i.d $vr16,     $vr17,    0x0d //c0[1] c1[1]

    vfmul.d      $vr18,    $vr30,    $vr28
    vfmul.d      $vr16,    $vr31,    $vr28
    VNMSUB      $vr18,    $vr31,    $vr29, $vr18
    VFMADD      $vr16,    $vr30,    $vr29, $vr16

    vand.v    $vr19,     $vr18,    $vr18
    vshuf4i.d $vr19,     $vr16,    0x08 //c0[0] c0[1]
    vshuf4i.d $vr18,     $vr16,    0x0d //c1[0] c1[1]

    vst       $vr19,     C0,    0x00 //c0: 0 1
    vst       $vr18,     C1,    0x00 //c1: 0 1

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10
#else
    //res00 res10
    vld       $vr16,     C0,    0x00 //c0: 0 1
    vld       $vr17,     C1,    0x00 //c1: 0 1

    vand.v    $vr18,     $vr16,    $vr16
    vshuf4i.d $vr18,     $vr17,    0x08 //c0[0] c1[0]
    vshuf4i.d $vr16,     $vr17,    0x0d //c0[1] c1[1]

    VFMADD      $vr18,    $vr30,    $vr28, $vr18
    VFMADD      $vr16,    $vr31,    $vr28, $vr16
    VNMSUB      $vr18,    $vr31,    $vr29, $vr18
    VFMADD      $vr16,    $vr30,    $vr29, $vr16

    vand.v    $vr19,     $vr18,    $vr18
    vshuf4i.d $vr19,     $vr16,    0x08 //c0[0] c0[1]
    vshuf4i.d $vr18,     $vr16,    0x0d //c1[0] c1[1]

    vst       $vr19,     C0,    0x00 //c0: 0 1
    vst       $vr18,     C1,    0x00 //c1: 0 1

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d      TL,     K,    OFF
#ifdef LEFT
    addi.d     TL,     TL,   -1
#else
    addi.d     TL,     TL,   -2
#endif
    slli.d     T3,     TL,   0x04
    add.d      A0,     A0,   T3
    slli.d     T3,     TL,   0x05
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
    slli.d     L,      K,     5
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
    slli.d     T3,     OFF,  0x07
    add.d      A0,     A0,   T3
    slli.d     T3,     OFF,  0x04
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

    xvxor.v    U0,     U0,   U0
    xvxor.v    U1,     U1,   U1
    xvxor.v    U2,     U2,   U2
    xvxor.v    U3,     U3,   U3

    move       L,      $r0   //cycle param k
    beq        L,      TL,    .L33
    blt        TL,     L,     .L33

.L32:  /* for (k=0; k<temp; k++) */
    xvld       D0,     A0,    0x00  // a0ri a1ri
    xvld       D1,     A0,    0x20  // a2ri a3ri

    xvldrepl.d D2,     B0,    0x00 //b0r
    xvldrepl.d D3,     B0,    0x08 //b0i

    xvpackev.d D4,     D1,    D0
    xvpermi.d  D4,     D4,    0xd8  //a0r a1r a2r a3r

    xvpackod.d D5,     D1,    D0
    xvpermi.d  D5,     D5,    0xd8  //a0i a1i a2i a3i

    XVMADD1    U0,     D4,    D2,     U0  //00r 01r 02r 03r
    XVMADD2    U1,     D5,    D2,     U1  //00i 01i 02i 03i
    XVMADD3    U0,     D5,    D3,     U0
    XVMADD4    U1,     D4,    D3,     U1

    xvld       D0,     A0,    0x40  // a4ri a5ri
    xvld       D1,     A0,    0x60  // a6ri a7ri

    xvpackev.d D4,     D1,    D0
    xvpermi.d  D4,     D4,    0xd8  //a4r a5r a6r a7r

    xvpackod.d D5,     D1,    D0
    xvpermi.d  D5,     D5,    0xd8  //a4i a5i a6i a7i

    XVMADD1    U2,     D4,    D2,     U2  //04r 05r 06r 07r
    XVMADD2    U3,     D5,    D2,     U3  //04i 05i 06i 07i
    XVMADD3    U2,     D5,    D3,     U2
    XVMADD4    U3,     D4,    D3,     U3

    addi.d     A0,     A0,    0x80
    addi.d     B0,     B0,    0x10

    addi.d     L,      L,     1
    blt        L,      TL,    .L32

.L33:
#if defined(TRMMKERNEL)
    //res00 res01 res02 res03
    xvld       D0,     C0,    0x00 //c0: 0 1 2 3
    xvld       D1,     C0,    0x20 //c0: 4 5 6 7

    xvpackev.d D2,     D1,    D0
    xvpermi.d  D2,     D2,    0xd8  //0 2 4 6
    xvpackod.d D3,     D1,    D0
    xvpermi.d  D3,     D3,    0xd8  //1 3 5 7

    xvfmul.d      D2,    U0,    VALPHAR
    xvfmul.d      D3,    U1,    VALPHAR
    XVNMSUB      D2,    U1,    VALPHAI, D2
    XVFMADD      D3,    U0,    VALPHAI, D3

    xvand.v    D4,     D2,   D2  //0 2 4 6
    xvpermi.q  D4,     D3,   0x02 //0 2 1 3
    xvpermi.d  D4,     D4,   0xd8 //0 1 2 3

    xvand.v    D5,     D3,   D3  //1 3 5 7
    xvpermi.q  D5,     D2,   0x31 //4 6 5 7
    xvpermi.d  D5,     D5,   0xd8 //4 5 6 7

    xvst       D4,     C0,    0x00
    xvst       D5,     C0,    0x20

    //res04 res05 res06 res07
    xvld       D0,     C0,    0x40 //c0: 8 9 10 11
    xvld       D1,     C0,    0x60 //c0: 12 13 14 15

    xvpackev.d D2,     D1,    D0
    xvpermi.d  D2,     D2,    0xd8  //8 10 12 14
    xvpackod.d D3,     D1,    D0
    xvpermi.d  D3,     D3,    0xd8  //9 11 13 15

    xvfmul.d      D2,    U2,    VALPHAR
    xvfmul.d      D3,    U3,    VALPHAR
    XVNMSUB      D2,    U3,    VALPHAI, D2
    XVFMADD      D3,    U2,    VALPHAI, D3

    xvand.v    D4,     D2,   D2  //8 10 12 14
    xvpermi.q  D4,     D3,   0x02 //8 10 9 11
    xvpermi.d  D4,     D4,   0xd8 //8 9 10 11

    xvand.v    D5,     D3,   D3  //9 11 13 15
    xvpermi.q  D5,     D2,   0x31 //12 14 13 15
    xvpermi.d  D5,     D5,   0xd8 //12 13 14 15

    xvst       D4,     C0,    0x40
    xvst       D5,     C0,    0x60

    addi.d     C0,     C0,    0x80
#else
    //res00 res01 res02 res03
    xvld       D0,     C0,    0x00 //c0: 0 1 2 3
    xvld       D1,     C0,    0x20 //c0: 4 5 6 7

    xvpackev.d D2,     D1,    D0
    xvpermi.d  D2,     D2,    0xd8  //0 2 4 6
    xvpackod.d D3,     D1,    D0
    xvpermi.d  D3,     D3,    0xd8  //1 3 5 7

    XVFMADD      D2,    U0,    VALPHAR, D2
    XVFMADD      D3,    U1,    VALPHAR, D3
    XVNMSUB      D2,    U1,    VALPHAI, D2
    XVFMADD      D3,    U0,    VALPHAI, D3

    xvand.v    D4,     D2,   D2  //0 2 4 6
    xvpermi.q  D4,     D3,   0x02 //0 2 1 3
    xvpermi.d  D4,     D4,   0xd8 //0 1 2 3

    xvand.v    D5,     D3,   D3  //1 3 5 7
    xvpermi.q  D5,     D2,   0x31 //4 6 5 7
    xvpermi.d  D5,     D5,   0xd8 //4 5 6 7

    xvst       D4,     C0,    0x00
    xvst       D5,     C0,    0x20

    //res04 res05 res06 res07
    xvld       D0,     C0,    0x40 //c0: 8 9 10 11
    xvld       D1,     C0,    0x60 //c0: 12 13 14 15

    xvpackev.d D2,     D1,    D0
    xvpermi.d  D2,     D2,    0xd8  //8 10 12 14
    xvpackod.d D3,     D1,    D0
    xvpermi.d  D3,     D3,    0xd8  //9 11 13 15

    XVFMADD      D2,    U2,    VALPHAR, D2
    XVFMADD      D3,    U3,    VALPHAR, D3
    XVNMSUB      D2,    U3,    VALPHAI, D2
    XVFMADD      D3,    U2,    VALPHAI, D3

    xvand.v    D4,     D2,   D2  //8 10 12 14
    xvpermi.q  D4,     D3,   0x02 //8 10 9 11
    xvpermi.d  D4,     D4,   0xd8 //8 9 10 11

    xvand.v    D5,     D3,   D3  //9 11 13 15
    xvpermi.q  D5,     D2,   0x31 //12 14 13 15
    xvpermi.d  D5,     D5,   0xd8 //12 13 14 15

    xvst       D4,     C0,    0x40
    xvst       D5,     C0,    0x60

    addi.d     C0,     C0,    0x80
#endif

#if defined(TRMMKERNEL)

#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d      TL,     K,    OFF
#ifdef LEFT
    addi.d     TL,     TL,   -8
#else
    addi.d     TL,     TL,   -1
#endif
    slli.d     T3,     TL,   0x07
    add.d      A0,     A0,   T3
    slli.d     T3,     TL,   0x04
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
    slli.d     T3,     OFF,   0x06
    add.d      A0,     A0,    T3
    slli.d     T3,     OFF,   0x04
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

    xvxor.v    U0,     U0,   U0
    xvxor.v    U1,     U1,   U1

    move       L,      $r0   //cycle param k
    beq        L,      TL,    .L37
    blt        TL,     L,     .L37

.L36:  /* for (k=0; k<temp; k++) */
    xvld       D0,     A0,    0x00  // a0ri a1ri
    xvld       D1,     A0,    0x20  // a2ri a3ri

    xvldrepl.d D2,     B0,    0x00 //b0r
    xvldrepl.d D3,     B0,    0x08 //b0i

    xvpackev.d D4,     D1,    D0
    xvpermi.d  D4,     D4,    0xd8  //a0r a1r a2r a3r

    xvpackod.d D5,     D1,    D0
    xvpermi.d  D5,     D5,    0xd8  //a0i a1i a2i a3i

    XVMADD1    U0,     D4,    D2,     U0  //00r 01r 02r 03r
    XVMADD2    U1,     D5,    D2,     U1  //00i 01i 02i 03i
    XVMADD3    U0,     D5,    D3,     U0
    XVMADD4    U1,     D4,    D3,     U1

    addi.d     A0,     A0,    0x40
    addi.d     B0,     B0,    0x10

    addi.d     L,      L,     1
    blt        L,      TL,    .L36

.L37:
#if defined(TRMMKERNEL)
    //res00 res01 res02 res03
    xvld       D0,     C0,    0x00 //c0: 0 1 2 3
    xvld       D1,     C0,    0x20 //c0: 4 5 6 7

    xvpackev.d D2,     D1,    D0
    xvpermi.d  D2,     D2,    0xd8  //0 2 4 6
    xvpackod.d D3,     D1,    D0
    xvpermi.d  D3,     D3,    0xd8  //1 3 5 7

    xvfmul.d      D2,    U0,    VALPHAR
    xvfmul.d      D3,    U1,    VALPHAR
    XVNMSUB      D2,    U1,    VALPHAI, D2
    XVFMADD      D3,    U0,    VALPHAI, D3

    xvand.v    D4,     D2,   D2  //0 2 4 6
    xvpermi.q  D4,     D3,   0x02 //0 2 1 3
    xvpermi.d  D4,     D4,   0xd8 //0 1 2 3

    xvand.v    D5,     D3,   D3  //1 3 5 7
    xvpermi.q  D5,     D2,   0x31 //4 6 5 7
    xvpermi.d  D5,     D5,   0xd8 //4 5 6 7

    xvst       D4,     C0,    0x00
    xvst       D5,     C0,    0x20

    addi.d     C0,     C0,    0x40
#else
    //res00 res01 res02 res03
    xvld       D0,     C0,    0x00 //c0: 0 1 2 3
    xvld       D1,     C0,    0x20 //c0: 4 5 6 7

    xvpackev.d D2,     D1,    D0
    xvpermi.d  D2,     D2,    0xd8  //0 2 4 6
    xvpackod.d D3,     D1,    D0
    xvpermi.d  D3,     D3,    0xd8  //1 3 5 7

    XVFMADD      D2,    U0,    VALPHAR, D2
    XVFMADD      D3,    U1,    VALPHAR, D3
    XVNMSUB      D2,    U1,    VALPHAI, D2
    XVFMADD      D3,    U0,    VALPHAI, D3

    xvand.v    D4,     D2,   D2  //0 2 4 6
    xvpermi.q  D4,     D3,   0x02 //0 2 1 3
    xvpermi.d  D4,     D4,   0xd8 //0 1 2 3

    xvand.v    D5,     D3,   D3  //1 3 5 7
    xvpermi.q  D5,     D2,   0x31 //4 6 5 7
    xvpermi.d  D5,     D5,   0xd8 //4 5 6 7

    xvst       D4,     C0,    0x00
    xvst       D5,     C0,    0x20

    addi.d     C0,     C0,    0x40
#endif

#if defined(TRMMKERNEL)

#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d      TL,     K,    OFF
#ifdef LEFT
    addi.d     TL,     TL,   -4
#else
    addi.d     TL,     TL,   -1
#endif
    slli.d     T3,     TL,   0x06
    add.d      A0,     A0,   T3
    slli.d     T3,     TL,   0x04
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
    slli.d     T3,     OFF,   0x05
    add.d      A0,     A0,    T3
    slli.d     T3,     OFF,   0x04
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

    vxor.v    $vr30,     $vr30,   $vr30
    vxor.v    $vr31,     $vr31,   $vr31

    move       L,      $r0   //cycle param k
    beq        L,      TL,    .L311
    blt        TL,     L,     .L311

.L310:  /* for (k=0; k<temp; k++) */
    vld       $vr16,     A0,    0x00  // a0ri
    vld       $vr17,     A0,    0x10  // a1ri

    //vldrepl.d $vr18,     B0,    0x00 //b0rr
    //vldrepl.d $vr19,     B0,    0x08 //b0ii
    vld       $vr18,     B0,    0x00
    vld       $vr19,     B0,    0x00
    vshuf4i.d  $vr18,     $vr18,    0x00 //b0rr
    vshuf4i.d  $vr19,     $vr19,    0x05 //b0ii

    vand.v     $vr20,     $vr16,    $vr16
    vshuf4i.d  $vr20,     $vr17,    0x08  //a0r a1r
    vshuf4i.d  $vr16,     $vr17,    0x0d  //a0i a1i

    VMADD1    $vr30,     $vr20,    $vr18,     $vr30  //00r 01r
    VMADD2    $vr31,     $vr16,    $vr18,     $vr31  //00i 01i
    VMADD3    $vr30,     $vr16,    $vr19,     $vr30
    VMADD4    $vr31,     $vr20,    $vr19,     $vr31

    addi.d     A0,     A0,    0x20
    addi.d     B0,     B0,    0x10

    addi.d     L,      L,     1
    blt        L,      TL,    .L310

.L311:
#if defined(TRMMKERNEL)
    //res00 res01
    vld       $vr16,     C0,    0x00 //c0: 0 1
    vld       $vr17,     C0,    0x10 //c0: 2 3

    vand.v    $vr18,     $vr16,    $vr16
    vshuf4i.d $vr18,     $vr17,    0x08 //c0[0] c0[2]
    vshuf4i.d $vr16,     $vr17,    0x0d //c0[1] c0[3]

    vfmul.d      $vr18,    $vr30,    $vr28
    vfmul.d      $vr16,    $vr31,    $vr28
    VNMSUB      $vr18,    $vr31,    $vr29, $vr18
    VFMADD      $vr16,    $vr30,    $vr29, $vr16

    vand.v    $vr19,     $vr18,    $vr18
    vshuf4i.d $vr19,     $vr16,    0x08 //c0[0] c0[1]
    vshuf4i.d $vr18,     $vr16,    0x0d //c0[2] c0[3]

    vst       $vr19,     C0,    0x00 //c0: 0 1
    vst       $vr18,     C0,    0x10 //c1: 2 3

    addi.d     C0,     C0,    0x20
#else
    //res00 res01
    vld       $vr16,     C0,    0x00 //c0: 0 1
    vld       $vr17,     C0,    0x10 //c0: 2 3

    vand.v    $vr18,     $vr16,    $vr16
    vshuf4i.d $vr18,     $vr17,    0x08 //c0[0] c0[2]
    vshuf4i.d $vr16,     $vr17,    0x0d //c0[1] c0[3]

    VFMADD      $vr18,    $vr30,    $vr28, $vr18
    VFMADD      $vr16,    $vr31,    $vr28, $vr16
    VNMSUB      $vr18,    $vr31,    $vr29, $vr18
    VFMADD      $vr16,    $vr30,    $vr29, $vr16

    vand.v    $vr19,     $vr18,    $vr18
    vshuf4i.d $vr19,     $vr16,    0x08 //c0[0] c0[1]
    vshuf4i.d $vr18,     $vr16,    0x0d //c0[2] c0[3]

    vst       $vr19,     C0,    0x00 //c0: 0 1
    vst       $vr18,     C0,    0x10 //c1: 2 3

    addi.d     C0,     C0,    0x20
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d      TL,     K,    OFF
#ifdef LEFT
    addi.d     TL,     TL,   -2
#else
    addi.d     TL,     TL,   -1
#endif
    slli.d     T3,     TL,   0x05
    add.d      A0,     A0,   T3
    slli.d     T3,     TL,   0x04
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
    slli.d     T3,     OFF,   0x04
    add.d      A0,     A0,    T3
    slli.d     T3,     OFF,   0x04
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
    LD         a2,     A0,    0x08

    LD         b1,     B0,    0x00
    LD         b2,     B0,    0x08

    MADD1      c11,    a1,    b1,     c11
    MADD2      c12,    a2,    b1,     c12
    MADD3      c11,    a2,    b2,     c11
    MADD4      c12,    a1,    b2,     c12

    addi.d     A0,     A0,    0x10
    addi.d     B0,     B0,    0x10

    addi.d     L,      L,     1
    blt        L,      TL,    .L314

.L315:
#if defined(TRMMKERNEL)
    LD         a5,     C0,    0x00    //C0[0]
    LD         a6,     C0,    0x08    //C0[1]

    MUL       a5,     c11,   ALPHA_R
    MUL       a6,     c12,   ALPHA_R
    NMSUB      a5,     c12,   ALPHA_I, a5
    MADD       a6,     c11,   ALPHA_I, a6

    ST         a5,     C0,    0x00
    ST         a6,     C0,    0x08

    addi.d     C0,     C0,    0x10
#else
    LD         a5,     C0,    0x00    //C0[0]
    LD         a6,     C0,    0x08    //C0[1]

    MADD       a5,     c11,   ALPHA_R, a5
    MADD       a6,     c12,   ALPHA_R, a6
    NMSUB      a5,     c12,   ALPHA_I, a5
    MADD       a6,     c11,   ALPHA_I, a6

    ST         a5,     C0,    0x00
    ST         a6,     C0,    0x08

    addi.d     C0,     C0,    0x10
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d      TL,     K,    OFF
#ifdef LEFT
    addi.d     TL,     TL,   -1
#else
    addi.d     TL,     TL,   -1
#endif
    slli.d     T3,     TL,   0x04
    add.d      A0,     A0,   T3
    add.d      B0,     B0,   T3
#endif

#ifdef LEFT
    addi.d     OFF,    OFF,   1
#endif
#endif   // #if defined(TRMMKERNEL)

.L316:
    slli.d     L,      K,     4
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
    fld.d         $f23,   $sp,   40
    fld.d         $f24,   $sp,   48
    fld.d         $f25,   $sp,   56
    fld.d         $f26,   $sp,   64
    fld.d         $f27,   $sp,   72
    fld.d         $f28,   $sp,   80
    fld.d         $f29,   $sp,   88
    fld.d         $f30,   $sp,   96
    fld.d         $f31,   $sp,   104

    addi.d     $sp,    $sp,   128
    jirl       $r0,    $r1,   0x0

    EPILOGUE
