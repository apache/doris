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

    xvldrepl.w  VALPHAR, $sp, 112
    xvldrepl.w  VALPHAI, $sp, 120

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
    srai.d     T0,     M,     4  //bm/16
    beq        I,      T0,    .L11

.L101:  /* for(i=0; i<bm/16; i+=1) */
    move       B0,     B      //ptrbb
    move       TL,     K      /* TL = bk */
#if defined(TRMMKERNEL)

#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    move       B0,     B     //ptrbb
#else
    slli.d     T3,     OFF,   0x07
    add.d      A0,     A0,    T3
    slli.d     T3,     OFF,   0x05
    add.d      B0,     B,     T3
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    sub.d      TL,     K,     OFF   //temp
#elif defined(LEFT)
    addi.d     TL,     OFF,   16
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
    xvxor.v    U10,     U10,   U10
    xvxor.v    U11,     U11,   U11
    xvxor.v    U12,     U12,   U12
    xvxor.v    U13,     U13,   U13
    xvxor.v    U14,     U14,   U14
    xvxor.v    U15,     U15,   U15

    move       L,      $r0   //cycle param k
    beq        L,      TL,    .L103
    blt        TL,     L,     .L103

.L102:  /* for(k=0; k<temp; k+=1) */
    xvld       D2,     B0,    0x00  // b0ri b1ri b2ri b3ri

    vldrepl.w  $vr20,  A0,    0x00
    vldrepl.w  $vr16,  A0,    0x08

    vldrepl.w  $vr21,  A0,    0x04
    vldrepl.w  $vr19,  A0,    0x0c

    xvpermi.q  D4,     D0,    0x02  //a0rrrr a1rrrr
    xvpermi.q  D5,     D3,    0x02  //a0iiii a1iiii

    xvpickev.w D6,     D2,    D2    //b0r b1r b0r b1r b2r b3r b2r b3r
    xvpermi.d  D6,     D6,    0xd8  //b0r b1r b2r b3r b0r b1r b2r b3r

    xvpickod.w D7,     D2,    D2    //b0i b1i b0i b1i b2i b3i b2i b3i
    xvpermi.d  D7,     D7,    0xd8  //b0i b1i b2i b3i b0i b1i b2i b3i

    XVMADD1    U0,     D4,    D6,     U0  //00r 10r 20r 30r 01r 11r 21r 31r
    XVMADD2    U1,     D5,    D6,     U1  //00i 10i 20i 30i 01i 11i 21i 31i
    XVMADD3    U0,     D5,    D7,     U0
    XVMADD4    U1,     D4,    D7,     U1

    vldrepl.w  $vr20,  A0,    0x10
    vldrepl.w  $vr16,  A0,    0x18

    vldrepl.w  $vr21,  A0,    0x14
    vldrepl.w  $vr19,  A0,    0x1c

    xvpermi.q  D4,     D0,    0x02
    xvpermi.q  D5,     D3,    0x02

    XVMADD1    U2,     D4,    D6,     U2  //02r 12r 22r 32r 03r 13r 23r 33r
    XVMADD2    U3,     D5,    D6,     U3  //02i 12i 22i 32i 03i 13i 23i 33i
    XVMADD3    U2,     D5,    D7,     U2
    XVMADD4    U3,     D4,    D7,     U3

    vldrepl.w  $vr20,  A0,    0x20
    vldrepl.w  $vr16,  A0,    0x28

    vldrepl.w  $vr21,  A0,    0x24
    vldrepl.w  $vr19,  A0,    0x2c

    xvpermi.q  D4,     D0,    0x02
    xvpermi.q  D5,     D3,    0x02

    XVMADD1    U4,     D4,    D6,     U4  //04r 14r 24r 34r 05r 15r 25r 35r
    XVMADD2    U5,     D5,    D6,     U5  //04i 14i 24i 34i 05i 15i 25i 35i
    XVMADD3    U4,     D5,    D7,     U4
    XVMADD4    U5,     D4,    D7,     U5

    vldrepl.w  $vr20,  A0,    0x30
    vldrepl.w  $vr16,  A0,    0x38

    vldrepl.w  $vr21,  A0,    0x34
    vldrepl.w  $vr19,  A0,    0x3c

    xvpermi.q  D4,     D0,    0x02
    xvpermi.q  D5,     D3,    0x02

    XVMADD1    U6,     D4,    D6,     U6  //06r 16r 26r 36r 07r 17r 27r 37r
    XVMADD2    U7,     D5,    D6,     U7  //06i 16i 26i 36i 07i 17i 27i 37i
    XVMADD3    U6,     D5,    D7,     U6
    XVMADD4    U7,     D4,    D7,     U7

    vldrepl.w  $vr20,  A0,    0x40
    vldrepl.w  $vr16,  A0,    0x48

    vldrepl.w  $vr21,  A0,    0x44
    vldrepl.w  $vr19,  A0,    0x4c

    xvpermi.q  D4,     D0,    0x02
    xvpermi.q  D5,     D3,    0x02

    XVMADD1    U8,     D4,    D6,     U8  //08r 18r 28r 38r 09r 19r 29r 39r
    XVMADD2    U9,     D5,    D6,     U9  //08i 18i 28i 38i 09i 19i 29i 39i
    XVMADD3    U8,     D5,    D7,     U8
    XVMADD4    U9,     D4,    D7,     U9

    vldrepl.w  $vr20,  A0,    0x50
    vldrepl.w  $vr16,  A0,    0x58

    vldrepl.w  $vr21,  A0,    0x54
    vldrepl.w  $vr19,  A0,    0x5c

    xvpermi.q  D4,     D0,    0x02
    xvpermi.q  D5,     D3,    0x02

    XVMADD1    U10,     D4,    D6,     U10  //0ar 1ar 2ar 3ar 0br 1br 2br 3br
    XVMADD2    U11,     D5,    D6,     U11  //0ai 1ai 2ai 3ai 0bi 1bi 2bi 3bi
    XVMADD3    U10,     D5,    D7,     U10
    XVMADD4    U11,     D4,    D7,     U11

    vldrepl.w  $vr20,  A0,    0x60
    vldrepl.w  $vr16,  A0,    0x68

    vldrepl.w  $vr21,  A0,    0x64
    vldrepl.w  $vr19,  A0,    0x6c

    xvpermi.q  D4,     D0,    0x02
    xvpermi.q  D5,     D3,    0x02

    XVMADD1    U12,     D4,    D6,     U12  //0cr 1cr 2cr 3cr 0dr 1dr 2dr 3dr
    XVMADD2    U13,     D5,    D6,     U13  //0ci 1ci 2ci 3ci 0di 1di 2di 3di
    XVMADD3    U12,     D5,    D7,     U12
    XVMADD4    U13,     D4,    D7,     U13

    vldrepl.w  $vr20,  A0,    0x70
    vldrepl.w  $vr16,  A0,    0x78

    vldrepl.w  $vr21,  A0,    0x74
    vldrepl.w  $vr19,  A0,    0x7c

    xvpermi.q  D4,     D0,    0x02
    xvpermi.q  D5,     D3,    0x02

    XVMADD1    U14,     D4,    D6,     U14  //0er 1er 2er 3er 0fr 1fr 2fr 3fr
    XVMADD2    U15,     D5,    D6,     U15  //0ei 1ei 2ei 3ei 0fi 1fi 2fi 3fi
    XVMADD3    U14,     D5,    D7,     U14
    XVMADD4    U15,     D4,    D7,     U15

    addi.d     A0,     A0,    0x80
    addi.d     B0,     B0,    0x20

    addi.d     L,      L,     1
    blt        L,      TL,    .L102

.L103:
#if defined(TRMMKERNEL)
    //res00 res10 res20 res30 res01 res11 res21 res31
    //res02 res12 res22 res32 res03 res13 res23 res33
    xvld       D0,     C0,    0x00 //c0: 0 1 2 3 4 5 6 7
    xvld       D1,     C1,    0x00 //c1: 0 1 2 3 4 5 6 7
    xvld       D2,     C2,    0x00 //c2: 0 1 2 3 4 5 6 7
    xvld       D3,     C3,    0x00 //c3: 0 1 2 3 4 5 6 7

    xvpackev.w D4,     D1,    D0   //c0[0] c1[0] c0[2] c1[2] c0[4] c1[4] c0[6] c1[6]
    xvpackev.w D5,     D3,    D2   //c2[0] c3[0] c2[2] c3[2] c2[4] c3[4] c2[6] c3[6]

    xvand.v    D6,     D4,    D4
    xvpermi.q  D6,     D5,    0x02 //c0[0] c1[0] c0[2] c1[2] c2[0] c3[0] c2[2] 03[2]
    xvpermi.d  D6,     D6,    0xd8 //c0[0] c1[0] c2[0] c3[0] c0[2] c1[2] c2[2] 03[2]

    xvand.v    D7,     D5,    D5
    xvpermi.q  D7,     D4,    0x31 //c0[4] c1[4] c0[6] c1[6] c2[4] c3[4] c2[6] 03[6]
    xvpermi.d  D7,     D7,    0xd8 //c0[4] c1[4] c2[4] c3[4] c0[6] c1[6] c2[6] 03[6]

    xvpackod.w D4,     D1,    D0   //c0[1] c1[1] c0[3] c1[3] c0[5] c1[5] c0[7] c1[7]
    xvpackod.w D5,     D3,    D2   //c2[1] c3[1] c2[3] c3[3] c2[5] c3[5] c2[7] c3[7]

    xvand.v    D8,     D4,    D4
    xvpermi.q  D8,     D5,    0x02 //c0[1] c1[1] c0[3] c1[3] c2[1] c3[1] c2[3] 03[3]
    xvpermi.d  D8,     D8,    0xd8 //c0[1] c1[1] c2[1] c3[1] c0[3] c1[3] c2[3] 03[3]

    xvand.v    D9,     D5,    D5
    xvpermi.q  D9,     D4,    0x31 //c0[5] c1[5] c0[7] c1[7] c2[5] c3[5] c2[7] 03[7]
    xvpermi.d  D9,     D9,    0xd8 //c0[5] c1[5] c2[5] c3[5] c0[7] c1[7] c2[7] 03[7]

    xvfmul.s      D6,    U0,    VALPHAR
    xvfmul.s      D8,    U1,    VALPHAR
    XVNMSUB      D6,    U1,    VALPHAI, D6
    XVFMADD      D8,    U0,    VALPHAI, D8

    xvfmul.s      D7,    U2,    VALPHAR
    xvfmul.s      D9,    U3,    VALPHAR
    XVNMSUB      D7,    U3,    VALPHAI, D7
    XVFMADD      D9,    U2,    VALPHAI, D9

    xvpackev.w D4,     D8,    D6 //c0[0] c0[1] c2[0] c2[1] c0[2] c0[3] c2[2] c2[3]
    xvpermi.d  D4,     D4,    0xd8 //c0[0] c0[1] c0[2] c0[3] c2[0] c2[1] c2[2] c2[3]
    xvpackev.w D5,     D9,    D7 //c0[4] c0[5] c2[4] c2[5] c0[6] c0[7] c2[6] c2[7]
    xvpermi.d  D5,     D5,    0xd8 //c0[4] c0[5] c0[6] c0[7] c2[4] c2[5] c2[6] c2[7]

    xvand.v    D0,     D4,    D4
    xvand.v    D1,     D5,    D5
    xvpermi.q  D0,     D1,    0x02 //c0: 0 1 2 3 4 5 6 7
    xvpermi.q  D1,     D4,    0x31 //c2: 0 1 2 3 4 5 6 7

    xvpackod.w D4,     D8,    D6 //c1[0] c1[1] c3[0] c3[1] c1[2] c1[3] c3[2] c3[3]
    xvpermi.d  D4,     D4,    0xd8 //c1[0] c1[1] c1[2] c1[3] c3[0] c3[1] c3[2] c3[3]
    xvpackod.w D5,     D9,    D7 //c1[4] c1[5] c3[4] c3[5] c1[6] c1[7] c3[6] c3[7]
    xvpermi.d  D5,     D5,    0xd8 //c1[4] c1[5] c1[6] c1[7] c3[4] c3[5] c3[6] c3[7]

    xvand.v    D2,     D4,    D4
    xvand.v    D3,     D5,    D5
    xvpermi.q  D2,     D3,    0x02 //c1: 0 1 2 3 4 5 6 7
    xvpermi.q  D3,     D4,    0x31 //c3: 0 1 2 3 4 5 6 7

    xvst       D0,     C0,    0x00
    xvst       D2,     C1,    0x00
    xvst       D1,     C2,    0x00
    xvst       D3,     C3,    0x00

    //res04 res14 res24 res34 res05 res15 res25 res35
    //res06 res16 res26 res36 res07 res17 res27 res37
    xvld       D0,     C0,    0x20
    xvld       D1,     C1,    0x20
    xvld       D2,     C2,    0x20
    xvld       D3,     C3,    0x20

    xvpackev.w D4,     D1,    D0
    xvpackev.w D5,     D3,    D2

    xvand.v    D6,     D4,    D4
    xvpermi.q  D6,     D5,    0x02
    xvpermi.d  D6,     D6,    0xd8

    xvand.v    D7,     D5,    D5
    xvpermi.q  D7,     D4,    0x31
    xvpermi.d  D7,     D7,    0xd8

    xvpackod.w D4,     D1,    D0
    xvpackod.w D5,     D3,    D2

    xvand.v    D8,     D4,    D4
    xvpermi.q  D8,     D5,    0x02
    xvpermi.d  D8,     D8,    0xd8

    xvand.v    D9,     D5,    D5
    xvpermi.q  D9,     D4,    0x31
    xvpermi.d  D9,     D9,    0xd8

    xvfmul.s      D6,    U4,    VALPHAR
    xvfmul.s      D8,    U5,    VALPHAR
    XVNMSUB      D6,    U5,    VALPHAI, D6
    XVFMADD      D8,    U4,    VALPHAI, D8

    xvfmul.s      D7,    U6,    VALPHAR
    xvfmul.s      D9,    U7,    VALPHAR
    XVNMSUB      D7,    U7,    VALPHAI, D7
    XVFMADD      D9,    U6,    VALPHAI, D9

    xvpackev.w D4,     D8,    D6
    xvpermi.d  D4,     D4,    0xd8
    xvpackev.w D5,     D9,    D7
    xvpermi.d  D5,     D5,    0xd8

    xvand.v    D0,     D4,    D4
    xvand.v    D1,     D5,    D5
    xvpermi.q  D0,     D1,    0x02
    xvpermi.q  D1,     D4,    0x31

    xvpackod.w D4,     D8,    D6
    xvpermi.d  D4,     D4,    0xd8
    xvpackod.w D5,     D9,    D7
    xvpermi.d  D5,     D5,    0xd8

    xvand.v    D2,     D4,    D4
    xvand.v    D3,     D5,    D5
    xvpermi.q  D2,     D3,    0x02
    xvpermi.q  D3,     D4,    0x31

    xvst       D0,     C0,    0x20
    xvst       D2,     C1,    0x20
    xvst       D1,     C2,    0x20
    xvst       D3,     C3,    0x20

    //res08 res18 res28 res38 res09 res19 res29 res39
    //res0a res1a res2a res3a res0b res1b res2b res3b
    xvld       D0,     C0,    0x40
    xvld       D1,     C1,    0x40
    xvld       D2,     C2,    0x40
    xvld       D3,     C3,    0x40

    xvpackev.w D4,     D1,    D0
    xvpackev.w D5,     D3,    D2

    xvand.v    D6,     D4,    D4
    xvpermi.q  D6,     D5,    0x02
    xvpermi.d  D6,     D6,    0xd8

    xvand.v    D7,     D5,    D5
    xvpermi.q  D7,     D4,    0x31
    xvpermi.d  D7,     D7,    0xd8

    xvpackod.w D4,     D1,    D0
    xvpackod.w D5,     D3,    D2

    xvand.v    D8,     D4,    D4
    xvpermi.q  D8,     D5,    0x02
    xvpermi.d  D8,     D8,    0xd8

    xvand.v    D9,     D5,    D5
    xvpermi.q  D9,     D4,    0x31
    xvpermi.d  D9,     D9,    0xd8

    xvfmul.s      D6,    U8,    VALPHAR
    xvfmul.s      D8,    U9,    VALPHAR
    XVNMSUB      D6,    U9,    VALPHAI, D6
    XVFMADD      D8,    U8,    VALPHAI, D8

    xvfmul.s      D7,    U10,    VALPHAR
    xvfmul.s      D9,    U11,    VALPHAR
    XVNMSUB      D7,    U11,    VALPHAI, D7
    XVFMADD      D9,    U10,    VALPHAI, D9

    xvpackev.w D4,     D8,    D6
    xvpermi.d  D4,     D4,    0xd8
    xvpackev.w D5,     D9,    D7
    xvpermi.d  D5,     D5,    0xd8

    xvand.v    D0,     D4,    D4
    xvand.v    D1,     D5,    D5
    xvpermi.q  D0,     D1,    0x02
    xvpermi.q  D1,     D4,    0x31

    xvpackod.w D4,     D8,    D6
    xvpermi.d  D4,     D4,    0xd8
    xvpackod.w D5,     D9,    D7
    xvpermi.d  D5,     D5,    0xd8

    xvand.v    D2,     D4,    D4
    xvand.v    D3,     D5,    D5
    xvpermi.q  D2,     D3,    0x02
    xvpermi.q  D3,     D4,    0x31

    xvst       D0,     C0,    0x40
    xvst       D2,     C1,    0x40
    xvst       D1,     C2,    0x40
    xvst       D3,     C3,    0x40

    //res0c res1c res2c res3c res0d res1d res2d res3d
    //res0e res1e res2e res3e res0f res1f res2f res3f
    xvld       D0,     C0,    0x60
    xvld       D1,     C1,    0x60
    xvld       D2,     C2,    0x60
    xvld       D3,     C3,    0x60

    xvpackev.w D4,     D1,    D0
    xvpackev.w D5,     D3,    D2

    xvand.v    D6,     D4,    D4
    xvpermi.q  D6,     D5,    0x02
    xvpermi.d  D6,     D6,    0xd8

    xvand.v    D7,     D5,    D5
    xvpermi.q  D7,     D4,    0x31
    xvpermi.d  D7,     D7,    0xd8

    xvpackod.w D4,     D1,    D0
    xvpackod.w D5,     D3,    D2

    xvand.v    D8,     D4,    D4
    xvpermi.q  D8,     D5,    0x02
    xvpermi.d  D8,     D8,    0xd8

    xvand.v    D9,     D5,    D5
    xvpermi.q  D9,     D4,    0x31
    xvpermi.d  D9,     D9,    0xd8

    xvfmul.s      D6,    U12,    VALPHAR
    xvfmul.s      D8,    U13,    VALPHAR
    XVNMSUB      D6,    U13,    VALPHAI, D6
    XVFMADD      D8,    U12,    VALPHAI, D8

    xvfmul.s      D7,    U14,    VALPHAR
    xvfmul.s      D9,    U15,    VALPHAR
    XVNMSUB      D7,    U15,    VALPHAI, D7
    XVFMADD      D9,    U14,    VALPHAI, D9

    xvpackev.w D4,     D8,    D6
    xvpermi.d  D4,     D4,    0xd8
    xvpackev.w D5,     D9,    D7
    xvpermi.d  D5,     D5,    0xd8

    xvand.v    D0,     D4,    D4
    xvand.v    D1,     D5,    D5
    xvpermi.q  D0,     D1,    0x02
    xvpermi.q  D1,     D4,    0x31

    xvpackod.w D4,     D8,    D6
    xvpermi.d  D4,     D4,    0xd8
    xvpackod.w D5,     D9,    D7
    xvpermi.d  D5,     D5,    0xd8

    xvand.v    D2,     D4,    D4
    xvand.v    D3,     D5,    D5
    xvpermi.q  D2,     D3,    0x02
    xvpermi.q  D3,     D4,    0x31

    xvst       D0,     C0,    0x60
    xvst       D2,     C1,    0x60
    xvst       D1,     C2,    0x60
    xvst       D3,     C3,    0x60

    addi.d     C0,     C0,    0x80
    addi.d     C1,     C1,    0x80
    addi.d     C2,     C2,    0x80
    addi.d     C3,     C3,    0x80
#else
    //res00 res10 res20 res30 res01 res11 res21 res31
    //res02 res12 res22 res32 res03 res13 res23 res33
    xvld       D0,     C0,    0x00 //c0: 0 1 2 3 4 5 6 7
    xvld       D1,     C1,    0x00 //c1: 0 1 2 3 4 5 6 7
    xvld       D2,     C2,    0x00 //c2: 0 1 2 3 4 5 6 7
    xvld       D3,     C3,    0x00 //c3: 0 1 2 3 4 5 6 7

    xvpackev.w D4,     D1,    D0   //c0[0] c1[0] c0[2] c1[2] c0[4] c1[4] c0[6] c1[6]
    xvpackev.w D5,     D3,    D2   //c2[0] c3[0] c2[2] c3[2] c2[4] c3[4] c2[6] c3[6]

    xvand.v    D6,     D4,    D4
    xvpermi.q  D6,     D5,    0x02 //c0[0] c1[0] c0[2] c1[2] c2[0] c3[0] c2[2] 03[2]
    xvpermi.d  D6,     D6,    0xd8 //c0[0] c1[0] c2[0] c3[0] c0[2] c1[2] c2[2] 03[2]

    xvand.v    D7,     D5,    D5
    xvpermi.q  D7,     D4,    0x31 //c0[4] c1[4] c0[6] c1[6] c2[4] c3[4] c2[6] 03[6]
    xvpermi.d  D7,     D7,    0xd8 //c0[4] c1[4] c2[4] c3[4] c0[6] c1[6] c2[6] 03[6]

    xvpackod.w D4,     D1,    D0   //c0[1] c1[1] c0[3] c1[3] c0[5] c1[5] c0[7] c1[7]
    xvpackod.w D5,     D3,    D2   //c2[1] c3[1] c2[3] c3[3] c2[5] c3[5] c2[7] c3[7]

    xvand.v    D8,     D4,    D4
    xvpermi.q  D8,     D5,    0x02 //c0[1] c1[1] c0[3] c1[3] c2[1] c3[1] c2[3] 03[3]
    xvpermi.d  D8,     D8,    0xd8 //c0[1] c1[1] c2[1] c3[1] c0[3] c1[3] c2[3] 03[3]

    xvand.v    D9,     D5,    D5
    xvpermi.q  D9,     D4,    0x31 //c0[5] c1[5] c0[7] c1[7] c2[5] c3[5] c2[7] 03[7]
    xvpermi.d  D9,     D9,    0xd8 //c0[5] c1[5] c2[5] c3[5] c0[7] c1[7] c2[7] 03[7]

    XVFMADD      D6,    U0,    VALPHAR, D6
    XVFMADD      D8,    U1,    VALPHAR, D8
    XVNMSUB      D6,    U1,    VALPHAI, D6
    XVFMADD      D8,    U0,    VALPHAI, D8

    XVFMADD      D7,    U2,    VALPHAR, D7
    XVFMADD      D9,    U3,    VALPHAR, D9
    XVNMSUB      D7,    U3,    VALPHAI, D7
    XVFMADD      D9,    U2,    VALPHAI, D9

    xvpackev.w D4,     D8,    D6 //c0[0] c0[1] c2[0] c2[1] c0[2] c0[3] c2[2] c2[3]
    xvpermi.d  D4,     D4,    0xd8 //c0[0] c0[1] c0[2] c0[3] c2[0] c2[1] c2[2] c2[3]
    xvpackev.w D5,     D9,    D7 //c0[4] c0[5] c2[4] c2[5] c0[6] c0[7] c2[6] c2[7]
    xvpermi.d  D5,     D5,    0xd8 //c0[4] c0[5] c0[6] c0[7] c2[4] c2[5] c2[6] c2[7]

    xvand.v    D0,     D4,    D4
    xvand.v    D1,     D5,    D5
    xvpermi.q  D0,     D1,    0x02 //c0: 0 1 2 3 4 5 6 7
    xvpermi.q  D1,     D4,    0x31 //c2: 0 1 2 3 4 5 6 7

    xvpackod.w D4,     D8,    D6 //c1[0] c1[1] c3[0] c3[1] c1[2] c1[3] c3[2] c3[3]
    xvpermi.d  D4,     D4,    0xd8 //c1[0] c1[1] c1[2] c1[3] c3[0] c3[1] c3[2] c3[3]
    xvpackod.w D5,     D9,    D7 //c1[4] c1[5] c3[4] c3[5] c1[6] c1[7] c3[6] c3[7]
    xvpermi.d  D5,     D5,    0xd8 //c1[4] c1[5] c1[6] c1[7] c3[4] c3[5] c3[6] c3[7]

    xvand.v    D2,     D4,    D4
    xvand.v    D3,     D5,    D5
    xvpermi.q  D2,     D3,    0x02 //c1: 0 1 2 3 4 5 6 7
    xvpermi.q  D3,     D4,    0x31 //c3: 0 1 2 3 4 5 6 7

    xvst       D0,     C0,    0x00
    xvst       D2,     C1,    0x00
    xvst       D1,     C2,    0x00
    xvst       D3,     C3,    0x00

    //res04 res14 res24 res34 res05 res15 res25 res35
    //res06 res16 res26 res36 res07 res17 res27 res37
    xvld       D0,     C0,    0x20
    xvld       D1,     C1,    0x20
    xvld       D2,     C2,    0x20
    xvld       D3,     C3,    0x20

    xvpackev.w D4,     D1,    D0
    xvpackev.w D5,     D3,    D2

    xvand.v    D6,     D4,    D4
    xvpermi.q  D6,     D5,    0x02
    xvpermi.d  D6,     D6,    0xd8

    xvand.v    D7,     D5,    D5
    xvpermi.q  D7,     D4,    0x31
    xvpermi.d  D7,     D7,    0xd8

    xvpackod.w D4,     D1,    D0
    xvpackod.w D5,     D3,    D2

    xvand.v    D8,     D4,    D4
    xvpermi.q  D8,     D5,    0x02
    xvpermi.d  D8,     D8,    0xd8

    xvand.v    D9,     D5,    D5
    xvpermi.q  D9,     D4,    0x31
    xvpermi.d  D9,     D9,    0xd8

    XVFMADD      D6,    U4,    VALPHAR, D6
    XVFMADD      D8,    U5,    VALPHAR, D8
    XVNMSUB      D6,    U5,    VALPHAI, D6
    XVFMADD      D8,    U4,    VALPHAI, D8

    XVFMADD      D7,    U6,    VALPHAR, D7
    XVFMADD      D9,    U7,    VALPHAR, D9
    XVNMSUB      D7,    U7,    VALPHAI, D7
    XVFMADD      D9,    U6,    VALPHAI, D9

    xvpackev.w D4,     D8,    D6
    xvpermi.d  D4,     D4,    0xd8
    xvpackev.w D5,     D9,    D7
    xvpermi.d  D5,     D5,    0xd8

    xvand.v    D0,     D4,    D4
    xvand.v    D1,     D5,    D5
    xvpermi.q  D0,     D1,    0x02
    xvpermi.q  D1,     D4,    0x31

    xvpackod.w D4,     D8,    D6
    xvpermi.d  D4,     D4,    0xd8
    xvpackod.w D5,     D9,    D7
    xvpermi.d  D5,     D5,    0xd8

    xvand.v    D2,     D4,    D4
    xvand.v    D3,     D5,    D5
    xvpermi.q  D2,     D3,    0x02
    xvpermi.q  D3,     D4,    0x31

    xvst       D0,     C0,    0x20
    xvst       D2,     C1,    0x20
    xvst       D1,     C2,    0x20
    xvst       D3,     C3,    0x20

    //res08 res18 res28 res38 res09 res19 res29 res39
    //res0a res1a res2a res3a res0b res1b res2b res3b
    xvld       D0,     C0,    0x40
    xvld       D1,     C1,    0x40
    xvld       D2,     C2,    0x40
    xvld       D3,     C3,    0x40

    xvpackev.w D4,     D1,    D0
    xvpackev.w D5,     D3,    D2

    xvand.v    D6,     D4,    D4
    xvpermi.q  D6,     D5,    0x02
    xvpermi.d  D6,     D6,    0xd8

    xvand.v    D7,     D5,    D5
    xvpermi.q  D7,     D4,    0x31
    xvpermi.d  D7,     D7,    0xd8

    xvpackod.w D4,     D1,    D0
    xvpackod.w D5,     D3,    D2

    xvand.v    D8,     D4,    D4
    xvpermi.q  D8,     D5,    0x02
    xvpermi.d  D8,     D8,    0xd8

    xvand.v    D9,     D5,    D5
    xvpermi.q  D9,     D4,    0x31
    xvpermi.d  D9,     D9,    0xd8

    XVFMADD      D6,    U8,    VALPHAR, D6
    XVFMADD      D8,    U9,    VALPHAR, D8
    XVNMSUB      D6,    U9,    VALPHAI, D6
    XVFMADD      D8,    U8,    VALPHAI, D8

    XVFMADD      D7,    U10,    VALPHAR, D7
    XVFMADD      D9,    U11,    VALPHAR, D9
    XVNMSUB      D7,    U11,    VALPHAI, D7
    XVFMADD      D9,    U10,    VALPHAI, D9

    xvpackev.w D4,     D8,    D6
    xvpermi.d  D4,     D4,    0xd8
    xvpackev.w D5,     D9,    D7
    xvpermi.d  D5,     D5,    0xd8

    xvand.v    D0,     D4,    D4
    xvand.v    D1,     D5,    D5
    xvpermi.q  D0,     D1,    0x02
    xvpermi.q  D1,     D4,    0x31

    xvpackod.w D4,     D8,    D6
    xvpermi.d  D4,     D4,    0xd8
    xvpackod.w D5,     D9,    D7
    xvpermi.d  D5,     D5,    0xd8

    xvand.v    D2,     D4,    D4
    xvand.v    D3,     D5,    D5
    xvpermi.q  D2,     D3,    0x02
    xvpermi.q  D3,     D4,    0x31

    xvst       D0,     C0,    0x40
    xvst       D2,     C1,    0x40
    xvst       D1,     C2,    0x40
    xvst       D3,     C3,    0x40

    //res0c res1c res2c res3c res0d res1d res2d res3d
    //res0e res1e res2e res3e res0f res1f res2f res3f
    xvld       D0,     C0,    0x60
    xvld       D1,     C1,    0x60
    xvld       D2,     C2,    0x60
    xvld       D3,     C3,    0x60

    xvpackev.w D4,     D1,    D0
    xvpackev.w D5,     D3,    D2

    xvand.v    D6,     D4,    D4
    xvpermi.q  D6,     D5,    0x02
    xvpermi.d  D6,     D6,    0xd8

    xvand.v    D7,     D5,    D5
    xvpermi.q  D7,     D4,    0x31
    xvpermi.d  D7,     D7,    0xd8

    xvpackod.w D4,     D1,    D0
    xvpackod.w D5,     D3,    D2

    xvand.v    D8,     D4,    D4
    xvpermi.q  D8,     D5,    0x02
    xvpermi.d  D8,     D8,    0xd8

    xvand.v    D9,     D5,    D5
    xvpermi.q  D9,     D4,    0x31
    xvpermi.d  D9,     D9,    0xd8

    XVFMADD      D6,    U12,    VALPHAR, D6
    XVFMADD      D8,    U13,    VALPHAR, D8
    XVNMSUB      D6,    U13,    VALPHAI, D6
    XVFMADD      D8,    U12,    VALPHAI, D8

    XVFMADD      D7,    U14,    VALPHAR, D7
    XVFMADD      D9,    U15,    VALPHAR, D9
    XVNMSUB      D7,    U15,    VALPHAI, D7
    XVFMADD      D9,    U14,    VALPHAI, D9

    xvpackev.w D4,     D8,    D6
    xvpermi.d  D4,     D4,    0xd8
    xvpackev.w D5,     D9,    D7
    xvpermi.d  D5,     D5,    0xd8

    xvand.v    D0,     D4,    D4
    xvand.v    D1,     D5,    D5
    xvpermi.q  D0,     D1,    0x02
    xvpermi.q  D1,     D4,    0x31

    xvpackod.w D4,     D8,    D6
    xvpermi.d  D4,     D4,    0xd8
    xvpackod.w D5,     D9,    D7
    xvpermi.d  D5,     D5,    0xd8

    xvand.v    D2,     D4,    D4
    xvand.v    D3,     D5,    D5
    xvpermi.q  D2,     D3,    0x02
    xvpermi.q  D3,     D4,    0x31

    xvst       D0,     C0,    0x60
    xvst       D2,     C1,    0x60
    xvst       D1,     C2,    0x60
    xvst       D3,     C3,    0x60

    addi.d     C0,     C0,    0x80
    addi.d     C1,     C1,    0x80
    addi.d     C2,     C2,    0x80
    addi.d     C3,     C3,    0x80
#endif

#if defined(TRMMKERNEL)

#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d      TL,     K,    OFF
#ifdef LEFT
    addi.d     TL,     TL,   -16
#else
    addi.d     TL,     TL,   -4
#endif
    slli.d     T3,     TL,   0x07
    add.d      A0,     A0,   T3
    slli.d     T3,     TL,   0x05
    add.d      B0,     B0,   T3
#endif

#ifdef LEFT
    addi.d     OFF,    OFF,   16
#endif

#endif   // #if defined(TRMMKERNEL)

    addi.d     I,      I,     1
    blt        I,      T0,    .L101

.L11:  /* if ( bm & 8 ) */
    move       I,      $r0
    andi       T0,     M,     8
    beq        I,      T0,    .L150

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

    xvxor.v    U0,     U0,   U0
    xvxor.v    U1,     U1,   U1
    xvxor.v    U2,     U2,   U2
    xvxor.v    U3,     U3,   U3
    xvxor.v    U4,     U4,   U4
    xvxor.v    U5,     U5,   U5
    xvxor.v    U6,     U6,   U6
    xvxor.v    U7,     U7,   U7

    move       L,      $r0   //cycle param k
    beq        L,      TL,    .L13
    blt        TL,     L,     .L13

.L12:  /* for(k=0; k<temp; k+=1) */
    xvld       D0,     A0,    0x00  // a0ri a1ri a2ri a3ri
    xvld       D2,     B0,    0x00  // b0ri b1ri b2ri b3ri

    xvand.v    D1,     D0,    D0
    xvpermi.q  D1,     D0,    0x02  //a0ri a1ri a0ri a1ri
    xvpermi.d  D1,     D1,    0xd8  //a0ri a0ri a1ri a1ri

    xvand.v    D4,     D1,    D1
    xvand.v    D5,     D1,    D1
    xvpermi.w  D4,     D4,    0x00  //a0rrrr a1rrrr
    xvpermi.w  D5,     D5,    0x55  //a0iiii a1iiii

    xvpackev.w D6,     D2,    D2    //b0rr b1rr b2rr b3rr
    xvpermi.w  D6,     D6,    0x88  //b0r b1r b0r b1r b2r b3r b2r b3r
    xvpermi.d  D6,     D6,    0xd8  //b0r b1r b2r b3r b0r b1r b2r b3r

    xvpackod.w D7,     D2,    D2    //b0ii b1ii b2ii b3ii
    xvpermi.w  D7,     D7,    0x88  //b0i b1i b0i b1i b2i b3i b2i b3i
    xvpermi.d  D7,     D7,    0xd8  //b0r b1r b2r b3r b0r b1r b2r b3r

    XVMADD1    U0,     D4,    D6,     U0  //00r 10r 20r 30r 01r 11r 21r 31r
    XVMADD2    U1,     D5,    D6,     U1  //00i 10i 20i 30i 01i 11i 21i 31i
    XVMADD3    U0,     D5,    D7,     U0
    XVMADD4    U1,     D4,    D7,     U1

    xvand.v    D1,     D0,    D0
    xvpermi.q  D1,     D0,    0x31  //a0ri a1ri a0ri a1ri
    xvpermi.d  D1,     D1,    0xd8  //a0ri a0ri a1ri a1ri

    xvand.v    D4,     D1,    D1
    xvand.v    D5,     D1,    D1
    xvpermi.w  D4,     D4,    0x00  //a0rrrr a1rrrr
    xvpermi.w  D5,     D5,    0x55  //a0iiii a1iiii

    XVMADD1    U2,     D4,    D6,     U2  //02r 12r 22r 32r 03r 13r 23r 33r
    XVMADD2    U3,     D5,    D6,     U3  //02i 12i 22i 32i 03i 13i 23i 33i
    XVMADD3    U2,     D5,    D7,     U2
    XVMADD4    U3,     D4,    D7,     U3

    xvld       D0,     A0,    0x20  // a4ri a5ri a6ri a7ri

    xvand.v    D1,     D0,    D0
    xvpermi.q  D1,     D0,    0x02  //a4ri a5ri a4ri a5ri
    xvpermi.d  D1,     D1,    0xd8  //a4ri a4ri a5ri a5ri

    xvand.v    D4,     D1,    D1
    xvand.v    D5,     D1,    D1
    xvpermi.w  D4,     D4,    0x00  //a4rrrr a5rrrr
    xvpermi.w  D5,     D5,    0x55  //a4iiii a5iiii

    XVMADD1    U4,     D4,    D6,     U4  //04r 14r 24r 34r 05r 15r 25r 35r
    XVMADD2    U5,     D5,    D6,     U5  //04i 14i 24i 34i 05i 15i 25i 35i
    XVMADD3    U4,     D5,    D7,     U4
    XVMADD4    U5,     D4,    D7,     U5

    xvand.v    D1,     D0,    D0
    xvpermi.q  D1,     D0,    0x31  //a6ri a7ri a6ri a7ri
    xvpermi.d  D1,     D1,    0xd8  //a6ri a6ri a7ri a7ri

    xvand.v    D4,     D1,    D1
    xvand.v    D5,     D1,    D1
    xvpermi.w  D4,     D4,    0x00  //a6rrrr a7rrrr
    xvpermi.w  D5,     D5,    0x55  //a6iiii a7iiii

    XVMADD1    U6,     D4,    D6,     U6  //06r 16r 26r 36r 07r 17r 27r 37r
    XVMADD2    U7,     D5,    D6,     U7  //06i 16i 26i 36i 07i 17i 27i 37i
    XVMADD3    U6,     D5,    D7,     U6
    XVMADD4    U7,     D4,    D7,     U7

    addi.d     A0,     A0,    0x40
    addi.d     B0,     B0,    0x20

    addi.d     L,      L,     1
    blt        L,      TL,    .L12

.L13:
#if defined(TRMMKERNEL)
    //res00 res10 res20 res30 res01 res11 res21 res31
    //res02 res12 res22 res32 res03 res13 res23 res33
    xvld       D0,     C0,    0x00 //c0: 0 1 2 3 4 5 6 7
    xvld       D1,     C1,    0x00 //c1: 0 1 2 3 4 5 6 7
    xvld       D2,     C2,    0x00 //c2: 0 1 2 3 4 5 6 7
    xvld       D3,     C3,    0x00 //c3: 0 1 2 3 4 5 6 7

    xvpackev.w D4,     D1,    D0   //c0[0] c1[0] c0[2] c1[2] c0[4] c1[4] c0[6] c1[6]
    xvpackev.w D5,     D3,    D2   //c2[0] c3[0] c2[2] c3[2] c2[4] c3[4] c2[6] c3[6]

    xvand.v    D6,     D4,    D4
    xvpermi.q  D6,     D5,    0x02 //c0[0] c1[0] c0[2] c1[2] c2[0] c3[0] c2[2] 03[2]
    xvpermi.d  D6,     D6,    0xd8 //c0[0] c1[0] c2[0] c3[0] c0[2] c1[2] c2[2] 03[2]

    xvand.v    D7,     D5,    D5
    xvpermi.q  D7,     D4,    0x31 //c0[4] c1[4] c0[6] c1[6] c2[4] c3[4] c2[6] 03[6]
    xvpermi.d  D7,     D7,    0xd8 //c0[4] c1[4] c2[4] c3[4] c0[6] c1[6] c2[6] 03[6]

    xvpackod.w D4,     D1,    D0   //c0[1] c1[1] c0[3] c1[3] c0[5] c1[5] c0[7] c1[7]
    xvpackod.w D5,     D3,    D2   //c2[1] c3[1] c2[3] c3[3] c2[5] c3[5] c2[7] c3[7]

    xvand.v    D8,     D4,    D4
    xvpermi.q  D8,     D5,    0x02 //c0[1] c1[1] c0[3] c1[3] c2[1] c3[1] c2[3] 03[3]
    xvpermi.d  D8,     D8,    0xd8 //c0[1] c1[1] c2[1] c3[1] c0[3] c1[3] c2[3] 03[3]

    xvand.v    D9,     D5,    D5
    xvpermi.q  D9,     D4,    0x31 //c0[5] c1[5] c0[7] c1[7] c2[5] c3[5] c2[7] 03[7]
    xvpermi.d  D9,     D9,    0xd8 //c0[5] c1[5] c2[5] c3[5] c0[7] c1[7] c2[7] 03[7]

    xvfmul.s      D6,    U0,    VALPHAR
    xvfmul.s      D8,    U1,    VALPHAR
    XVNMSUB      D6,    U1,    VALPHAI, D6
    XVFMADD      D8,    U0,    VALPHAI, D8

    xvfmul.s      D7,    U2,    VALPHAR
    xvfmul.s      D9,    U3,    VALPHAR
    XVNMSUB      D7,    U3,    VALPHAI, D7
    XVFMADD      D9,    U2,    VALPHAI, D9

    xvpackev.w D4,     D8,    D6 //c0[0] c0[1] c2[0] c2[1] c0[2] c0[3] c2[2] c2[3]
    xvpermi.d  D4,     D4,    0xd8 //c0[0] c0[1] c0[2] c0[3] c2[0] c2[1] c2[2] c2[3]
    xvpackev.w D5,     D9,    D7 //c0[4] c0[5] c2[4] c2[5] c0[6] c0[7] c2[6] c2[7]
    xvpermi.d  D5,     D5,    0xd8 //c0[4] c0[5] c0[6] c0[7] c2[4] c2[5] c2[6] c2[7]

    xvand.v    D0,     D4,    D4
    xvand.v    D1,     D5,    D5
    xvpermi.q  D0,     D1,    0x02 //c0: 0 1 2 3 4 5 6 7
    xvpermi.q  D1,     D4,    0x31 //c2: 0 1 2 3 4 5 6 7

    xvpackod.w D4,     D8,    D6 //c1[0] c1[1] c3[0] c3[1] c1[2] c1[3] c3[2] c3[3]
    xvpermi.d  D4,     D4,    0xd8 //c1[0] c1[1] c1[2] c1[3] c3[0] c3[1] c3[2] c3[3]
    xvpackod.w D5,     D9,    D7 //c1[4] c1[5] c3[4] c3[5] c1[6] c1[7] c3[6] c3[7]
    xvpermi.d  D5,     D5,    0xd8 //c1[4] c1[5] c1[6] c1[7] c3[4] c3[5] c3[6] c3[7]

    xvand.v    D2,     D4,    D4
    xvand.v    D3,     D5,    D5
    xvpermi.q  D2,     D3,    0x02 //c1: 0 1 2 3 4 5 6 7
    xvpermi.q  D3,     D4,    0x31 //c3: 0 1 2 3 4 5 6 7

    xvst       D0,     C0,    0x00
    xvst       D2,     C1,    0x00
    xvst       D1,     C2,    0x00
    xvst       D3,     C3,    0x00

    //res04 res14 res24 res34 res05 res15 res25 res35
    //res06 res16 res26 res36 res07 res17 res27 res37
    xvld       D0,     C0,    0x20
    xvld       D1,     C1,    0x20
    xvld       D2,     C2,    0x20
    xvld       D3,     C3,    0x20

    xvpackev.w D4,     D1,    D0
    xvpackev.w D5,     D3,    D2

    xvand.v    D6,     D4,    D4
    xvpermi.q  D6,     D5,    0x02
    xvpermi.d  D6,     D6,    0xd8

    xvand.v    D7,     D5,    D5
    xvpermi.q  D7,     D4,    0x31
    xvpermi.d  D7,     D7,    0xd8

    xvpackod.w D4,     D1,    D0
    xvpackod.w D5,     D3,    D2

    xvand.v    D8,     D4,    D4
    xvpermi.q  D8,     D5,    0x02
    xvpermi.d  D8,     D8,    0xd8

    xvand.v    D9,     D5,    D5
    xvpermi.q  D9,     D4,    0x31
    xvpermi.d  D9,     D9,    0xd8

    xvfmul.s      D6,    U4,    VALPHAR
    xvfmul.s      D8,    U5,    VALPHAR
    XVNMSUB      D6,    U5,    VALPHAI, D6
    XVFMADD      D8,    U4,    VALPHAI, D8

    xvfmul.s      D7,    U6,    VALPHAR
    xvfmul.s      D9,    U7,    VALPHAR
    XVNMSUB      D7,    U7,    VALPHAI, D7
    XVFMADD      D9,    U6,    VALPHAI, D9

    xvpackev.w D4,     D8,    D6
    xvpermi.d  D4,     D4,    0xd8
    xvpackev.w D5,     D9,    D7
    xvpermi.d  D5,     D5,    0xd8

    xvand.v    D0,     D4,    D4
    xvand.v    D1,     D5,    D5
    xvpermi.q  D0,     D1,    0x02
    xvpermi.q  D1,     D4,    0x31

    xvpackod.w D4,     D8,    D6
    xvpermi.d  D4,     D4,    0xd8
    xvpackod.w D5,     D9,    D7
    xvpermi.d  D5,     D5,    0xd8

    xvand.v    D2,     D4,    D4
    xvand.v    D3,     D5,    D5
    xvpermi.q  D2,     D3,    0x02
    xvpermi.q  D3,     D4,    0x31

    xvst       D0,     C0,    0x20
    xvst       D2,     C1,    0x20
    xvst       D1,     C2,    0x20
    xvst       D3,     C3,    0x20

    addi.d     C0,     C0,    0x40
    addi.d     C1,     C1,    0x40
    addi.d     C2,     C2,    0x40
    addi.d     C3,     C3,    0x40
#else
    //res00 res10 res20 res30 res01 res11 res21 res31
    //res02 res12 res22 res32 res03 res13 res23 res33
    xvld       D0,     C0,    0x00 //c0: 0 1 2 3 4 5 6 7
    xvld       D1,     C1,    0x00 //c1: 0 1 2 3 4 5 6 7
    xvld       D2,     C2,    0x00 //c2: 0 1 2 3 4 5 6 7
    xvld       D3,     C3,    0x00 //c3: 0 1 2 3 4 5 6 7

    xvpackev.w D4,     D1,    D0   //c0[0] c1[0] c0[2] c1[2] c0[4] c1[4] c0[6] c1[6]
    xvpackev.w D5,     D3,    D2   //c2[0] c3[0] c2[2] c3[2] c2[4] c3[4] c2[6] c3[6]

    xvand.v    D6,     D4,    D4
    xvpermi.q  D6,     D5,    0x02 //c0[0] c1[0] c0[2] c1[2] c2[0] c3[0] c2[2] 03[2]
    xvpermi.d  D6,     D6,    0xd8 //c0[0] c1[0] c2[0] c3[0] c0[2] c1[2] c2[2] 03[2]

    xvand.v    D7,     D5,    D5
    xvpermi.q  D7,     D4,    0x31 //c0[4] c1[4] c0[6] c1[6] c2[4] c3[4] c2[6] 03[6]
    xvpermi.d  D7,     D7,    0xd8 //c0[4] c1[4] c2[4] c3[4] c0[6] c1[6] c2[6] 03[6]

    xvpackod.w D4,     D1,    D0   //c0[1] c1[1] c0[3] c1[3] c0[5] c1[5] c0[7] c1[7]
    xvpackod.w D5,     D3,    D2   //c2[1] c3[1] c2[3] c3[3] c2[5] c3[5] c2[7] c3[7]

    xvand.v    D8,     D4,    D4
    xvpermi.q  D8,     D5,    0x02 //c0[1] c1[1] c0[3] c1[3] c2[1] c3[1] c2[3] 03[3]
    xvpermi.d  D8,     D8,    0xd8 //c0[1] c1[1] c2[1] c3[1] c0[3] c1[3] c2[3] 03[3]

    xvand.v    D9,     D5,    D5
    xvpermi.q  D9,     D4,    0x31 //c0[5] c1[5] c0[7] c1[7] c2[5] c3[5] c2[7] 03[7]
    xvpermi.d  D9,     D9,    0xd8 //c0[5] c1[5] c2[5] c3[5] c0[7] c1[7] c2[7] 03[7]

    XVFMADD      D6,    U0,    VALPHAR, D6
    XVFMADD      D8,    U1,    VALPHAR, D8
    XVNMSUB      D6,    U1,    VALPHAI, D6
    XVFMADD      D8,    U0,    VALPHAI, D8

    XVFMADD      D7,    U2,    VALPHAR, D7
    XVFMADD      D9,    U3,    VALPHAR, D9
    XVNMSUB      D7,    U3,    VALPHAI, D7
    XVFMADD      D9,    U2,    VALPHAI, D9

    xvpackev.w D4,     D8,    D6 //c0[0] c0[1] c2[0] c2[1] c0[2] c0[3] c2[2] c2[3]
    xvpermi.d  D4,     D4,    0xd8 //c0[0] c0[1] c0[2] c0[3] c2[0] c2[1] c2[2] c2[3]
    xvpackev.w D5,     D9,    D7 //c0[4] c0[5] c2[4] c2[5] c0[6] c0[7] c2[6] c2[7]
    xvpermi.d  D5,     D5,    0xd8 //c0[4] c0[5] c0[6] c0[7] c2[4] c2[5] c2[6] c2[7]

    xvand.v    D0,     D4,    D4
    xvand.v    D1,     D5,    D5
    xvpermi.q  D0,     D1,    0x02 //c0: 0 1 2 3 4 5 6 7
    xvpermi.q  D1,     D4,    0x31 //c2: 0 1 2 3 4 5 6 7

    xvpackod.w D4,     D8,    D6 //c1[0] c1[1] c3[0] c3[1] c1[2] c1[3] c3[2] c3[3]
    xvpermi.d  D4,     D4,    0xd8 //c1[0] c1[1] c1[2] c1[3] c3[0] c3[1] c3[2] c3[3]
    xvpackod.w D5,     D9,    D7 //c1[4] c1[5] c3[4] c3[5] c1[6] c1[7] c3[6] c3[7]
    xvpermi.d  D5,     D5,    0xd8 //c1[4] c1[5] c1[6] c1[7] c3[4] c3[5] c3[6] c3[7]

    xvand.v    D2,     D4,    D4
    xvand.v    D3,     D5,    D5
    xvpermi.q  D2,     D3,    0x02 //c1: 0 1 2 3 4 5 6 7
    xvpermi.q  D3,     D4,    0x31 //c3: 0 1 2 3 4 5 6 7

    xvst       D0,     C0,    0x00
    xvst       D2,     C1,    0x00
    xvst       D1,     C2,    0x00
    xvst       D3,     C3,    0x00

    //res04 res14 res24 res34 res05 res15 res25 res35
    //res06 res16 res26 res36 res07 res17 res27 res37
    xvld       D0,     C0,    0x20
    xvld       D1,     C1,    0x20
    xvld       D2,     C2,    0x20
    xvld       D3,     C3,    0x20

    xvpackev.w D4,     D1,    D0
    xvpackev.w D5,     D3,    D2

    xvand.v    D6,     D4,    D4
    xvpermi.q  D6,     D5,    0x02
    xvpermi.d  D6,     D6,    0xd8

    xvand.v    D7,     D5,    D5
    xvpermi.q  D7,     D4,    0x31
    xvpermi.d  D7,     D7,    0xd8

    xvpackod.w D4,     D1,    D0
    xvpackod.w D5,     D3,    D2

    xvand.v    D8,     D4,    D4
    xvpermi.q  D8,     D5,    0x02
    xvpermi.d  D8,     D8,    0xd8

    xvand.v    D9,     D5,    D5
    xvpermi.q  D9,     D4,    0x31
    xvpermi.d  D9,     D9,    0xd8

    XVFMADD      D6,    U4,    VALPHAR, D6
    XVFMADD      D8,    U5,    VALPHAR, D8
    XVNMSUB      D6,    U5,    VALPHAI, D6
    XVFMADD      D8,    U4,    VALPHAI, D8

    XVFMADD      D7,    U6,    VALPHAR, D7
    XVFMADD      D9,    U7,    VALPHAR, D9
    XVNMSUB      D7,    U7,    VALPHAI, D7
    XVFMADD      D9,    U6,    VALPHAI, D9

    xvpackev.w D4,     D8,    D6
    xvpermi.d  D4,     D4,    0xd8
    xvpackev.w D5,     D9,    D7
    xvpermi.d  D5,     D5,    0xd8

    xvand.v    D0,     D4,    D4
    xvand.v    D1,     D5,    D5
    xvpermi.q  D0,     D1,    0x02
    xvpermi.q  D1,     D4,    0x31

    xvpackod.w D4,     D8,    D6
    xvpermi.d  D4,     D4,    0xd8
    xvpackod.w D5,     D9,    D7
    xvpermi.d  D5,     D5,    0xd8

    xvand.v    D2,     D4,    D4
    xvand.v    D3,     D5,    D5
    xvpermi.q  D2,     D3,    0x02
    xvpermi.q  D3,     D4,    0x31

    xvst       D0,     C0,    0x20
    xvst       D2,     C1,    0x20
    xvst       D1,     C2,    0x20
    xvst       D3,     C3,    0x20

    addi.d     C0,     C0,    0x40
    addi.d     C1,     C1,    0x40
    addi.d     C2,     C2,    0x40
    addi.d     C3,     C3,    0x40
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

    xvxor.v    U0,     U0,   U0
    xvxor.v    U1,     U1,   U1
    xvxor.v    U2,     U2,   U2
    xvxor.v    U3,     U3,   U3

    move       L,      $r0   //cycle param k
    beq        L,      TL,    .L17
    blt        TL,     L,     .L17

.L16:  /* for (k=0; k<temp; k++) */
    xvld       D0,     A0,    0x00  // a0ri a1ri a2ri a3ri
    xvld       D2,     B0,    0x00  // b0ri b1ri b2ri b3ri

    xvand.v    D1,     D0,    D0
    xvpermi.q  D1,     D0,    0x02  //a0ri a1ri a0ri a1ri
    xvpermi.d  D1,     D1,    0xd8  //a0ri a0ri a1ri a1ri

    xvand.v    D4,     D1,    D1
    xvand.v    D5,     D1,    D1
    xvpermi.w  D4,     D4,    0x00  //a0rrrr a1rrrr
    xvpermi.w  D5,     D5,    0x55  //a0iiii a1iiii

    xvpackev.w D6,     D2,    D2    //b0rr b1rr b2rr b3rr
    xvpermi.w  D6,     D6,    0x88  //b0r b1r b0r b1r b2r b3r b2r b3r
    xvpermi.d  D6,     D6,    0xd8  //b0r b1r b2r b3r b0r b1r b2r b3r

    xvpackod.w D7,     D2,    D2    //b0ii b1ii b2ii b3ii
    xvpermi.w  D7,     D7,    0x88  //b0i b1i b0i b1i b2i b3i b2i b3i
    xvpermi.d  D7,     D7,    0xd8  //b0i b1i b2i b3i b0i b1i b2i b3i

    XVMADD1    U0,     D4,    D6,     U0  //00r 10r 20r 30r 01r 11r 21r 31r
    XVMADD2    U1,     D5,    D6,     U1  //00i 10i 20i 30i 01i 11i 21i 31i
    XVMADD3    U0,     D5,    D7,     U0
    XVMADD4    U1,     D4,    D7,     U1

    xvand.v    D1,     D0,    D0
    xvpermi.q  D1,     D0,    0x31  //a2ri a3ri a2ri a3ri
    xvpermi.d  D1,     D1,    0xd8  //a2ri a2ri a3ri a3ri

    xvand.v    D4,     D1,    D1
    xvand.v    D5,     D1,    D1
    xvpermi.w  D4,     D4,    0x00  //a2rrrr a3rrrr
    xvpermi.w  D5,     D5,    0x55  //a2iiii a3iiii

    XVMADD1    U2,     D4,    D6,     U2  //02r 12r 22r 32r 03r 13r 23r 33r
    XVMADD2    U3,     D5,    D6,     U3  //02i 12i 22i 32i 03i 13i 23i 33i
    XVMADD3    U2,     D5,    D7,     U2
    XVMADD4    U3,     D4,    D7,     U3

    addi.d     A0,     A0,    0x20
    addi.d     B0,     B0,    0x20

    addi.d     L,      L,     1
    blt        L,      TL,     .L16

.L17:
#if defined(TRMMKERNEL)
    //res00 res10 res20 res30 res01 res11 res21 res31
    //res02 res12 res22 res32 res03 res13 res23 res33
    xvld       D0,     C0,    0x00 //c0: 0 1 2 3 4 5 6 7
    xvld       D1,     C1,    0x00 //c1: 0 1 2 3 4 5 6 7
    xvld       D2,     C2,    0x00 //c2: 0 1 2 3 4 5 6 7
    xvld       D3,     C3,    0x00 //c3: 0 1 2 3 4 5 6 7

    xvpackev.w D4,     D1,    D0   //c0[0] c1[0] c0[2] c1[2] c0[4] c1[4] c0[6] c1[6]
    xvpackev.w D5,     D3,    D2   //c2[0] c3[0] c2[2] c3[2] c2[4] c3[4] c2[6] c3[6]

    xvand.v    D6,     D4,    D4
    xvpermi.q  D6,     D5,    0x02 //c0[0] c1[0] c0[2] c1[2] c2[0] c3[0] c2[2] 03[2]
    xvpermi.d  D6,     D6,    0xd8 //c0[0] c1[0] c2[0] c3[0] c0[2] c1[2] c2[2] 03[2]

    xvand.v    D7,     D5,    D5
    xvpermi.q  D7,     D4,    0x31 //c0[4] c1[4] c0[6] c1[6] c2[4] c3[4] c2[6] 03[6]
    xvpermi.d  D7,     D7,    0xd8 //c0[4] c1[4] c2[4] c3[4] c0[6] c1[6] c2[6] 03[6]

    xvpackod.w D4,     D1,    D0   //c0[1] c1[1] c0[3] c1[3] c0[5] c1[5] c0[7] c1[7]
    xvpackod.w D5,     D3,    D2   //c2[1] c3[1] c2[3] c3[3] c2[5] c3[5] c2[7] c3[7]

    xvand.v    D8,     D4,    D4
    xvpermi.q  D8,     D5,    0x02 //c0[1] c1[1] c0[3] c1[3] c2[1] c3[1] c2[3] 03[3]
    xvpermi.d  D8,     D8,    0xd8 //c0[1] c1[1] c2[1] c3[1] c0[3] c1[3] c2[3] 03[3]

    xvand.v    D9,     D5,    D5
    xvpermi.q  D9,     D4,    0x31 //c0[5] c1[5] c0[7] c1[7] c2[5] c3[5] c2[7] 03[7]
    xvpermi.d  D9,     D9,    0xd8 //c0[5] c1[5] c2[5] c3[5] c0[7] c1[7] c2[7] 03[7]

    xvfmul.s      D6,    U0,    VALPHAR
    xvfmul.s      D8,    U1,    VALPHAR
    XVNMSUB      D6,    U1,    VALPHAI, D6
    XVFMADD      D8,    U0,    VALPHAI, D8

    xvfmul.s      D7,    U2,    VALPHAR
    xvfmul.s      D9,    U3,    VALPHAR
    XVNMSUB      D7,    U3,    VALPHAI, D7
    XVFMADD      D9,    U2,    VALPHAI, D9

    xvpackev.w D4,     D8,    D6 //c0[0] c0[1] c2[0] c2[1] c0[2] c0[3] c2[2] c2[3]
    xvpermi.d  D4,     D4,    0xd8 //c0[0] c0[1] c0[2] c0[3] c2[0] c2[1] c2[2] c2[3]
    xvpackev.w D5,     D9,    D7 //c0[4] c0[5] c2[4] c2[5] c0[6] c0[7] c2[6] c2[7]
    xvpermi.d  D5,     D5,    0xd8 //c0[4] c0[5] c0[6] c0[7] c2[4] c2[5] c2[6] c2[7]

    xvand.v    D0,     D4,    D4
    xvand.v    D1,     D5,    D5
    xvpermi.q  D0,     D1,    0x02 //c0: 0 1 2 3 4 5 6 7
    xvpermi.q  D1,     D4,    0x31 //c2: 0 1 2 3 4 5 6 7

    xvpackod.w D4,     D8,    D6 //c1[0] c1[1] c3[0] c3[1] c1[2] c1[3] c3[2] c3[3]
    xvpermi.d  D4,     D4,    0xd8 //c1[0] c1[1] c1[2] c1[3] c3[0] c3[1] c3[2] c3[3]
    xvpackod.w D5,     D9,    D7 //c1[4] c1[5] c3[4] c3[5] c1[6] c1[7] c3[6] c3[7]
    xvpermi.d  D5,     D5,    0xd8 //c1[4] c1[5] c1[6] c1[7] c3[4] c3[5] c3[6] c3[7]

    xvand.v    D2,     D4,    D4
    xvand.v    D3,     D5,    D5
    xvpermi.q  D2,     D3,    0x02 //c1: 0 1 2 3 4 5 6 7
    xvpermi.q  D3,     D4,    0x31 //c3: 0 1 2 3 4 5 6 7

    xvst       D0,     C0,    0x00
    xvst       D2,     C1,    0x00
    xvst       D1,     C2,    0x00
    xvst       D3,     C3,    0x00

    addi.d     C0,     C0,    0x20
    addi.d     C1,     C1,    0x20
    addi.d     C2,     C2,    0x20
    addi.d     C3,     C3,    0x20
#else
    //res00 res10 res20 res30 res01 res11 res21 res31
    //res02 res12 res22 res32 res03 res13 res23 res33
    xvld       D0,     C0,    0x00 //c0: 0 1 2 3 4 5 6 7
    xvld       D1,     C1,    0x00 //c1: 0 1 2 3 4 5 6 7
    xvld       D2,     C2,    0x00 //c2: 0 1 2 3 4 5 6 7
    xvld       D3,     C3,    0x00 //c3: 0 1 2 3 4 5 6 7

    xvpackev.w D4,     D1,    D0   //c0[0] c1[0] c0[2] c1[2] c0[4] c1[4] c0[6] c1[6]
    xvpackev.w D5,     D3,    D2   //c2[0] c3[0] c2[2] c3[2] c2[4] c3[4] c2[6] c3[6]

    xvand.v    D6,     D4,    D4
    xvpermi.q  D6,     D5,    0x02 //c0[0] c1[0] c0[2] c1[2] c2[0] c3[0] c2[2] 03[2]
    xvpermi.d  D6,     D6,    0xd8 //c0[0] c1[0] c2[0] c3[0] c0[2] c1[2] c2[2] 03[2]

    xvand.v    D7,     D5,    D5
    xvpermi.q  D7,     D4,    0x31 //c0[4] c1[4] c0[6] c1[6] c2[4] c3[4] c2[6] 03[6]
    xvpermi.d  D7,     D7,    0xd8 //c0[4] c1[4] c2[4] c3[4] c0[6] c1[6] c2[6] 03[6]

    xvpackod.w D4,     D1,    D0   //c0[1] c1[1] c0[3] c1[3] c0[5] c1[5] c0[7] c1[7]
    xvpackod.w D5,     D3,    D2   //c2[1] c3[1] c2[3] c3[3] c2[5] c3[5] c2[7] c3[7]

    xvand.v    D8,     D4,    D4
    xvpermi.q  D8,     D5,    0x02 //c0[1] c1[1] c0[3] c1[3] c2[1] c3[1] c2[3] 03[3]
    xvpermi.d  D8,     D8,    0xd8 //c0[1] c1[1] c2[1] c3[1] c0[3] c1[3] c2[3] 03[3]

    xvand.v    D9,     D5,    D5
    xvpermi.q  D9,     D4,    0x31 //c0[5] c1[5] c0[7] c1[7] c2[5] c3[5] c2[7] 03[7]
    xvpermi.d  D9,     D9,    0xd8 //c0[5] c1[5] c2[5] c3[5] c0[7] c1[7] c2[7] 03[7]

    XVFMADD      D6,    U0,    VALPHAR, D6
    XVFMADD      D8,    U1,    VALPHAR, D8
    XVNMSUB      D6,    U1,    VALPHAI, D6
    XVFMADD      D8,    U0,    VALPHAI, D8

    XVFMADD      D7,    U2,    VALPHAR, D7
    XVFMADD      D9,    U3,    VALPHAR, D9
    XVNMSUB      D7,    U3,    VALPHAI, D7
    XVFMADD      D9,    U2,    VALPHAI, D9

    xvpackev.w D4,     D8,    D6 //c0[0] c0[1] c2[0] c2[1] c0[2] c0[3] c2[2] c2[3]
    xvpermi.d  D4,     D4,    0xd8 //c0[0] c0[1] c0[2] c0[3] c2[0] c2[1] c2[2] c2[3]
    xvpackev.w D5,     D9,    D7 //c0[4] c0[5] c2[4] c2[5] c0[6] c0[7] c2[6] c2[7]
    xvpermi.d  D5,     D5,    0xd8 //c0[4] c0[5] c0[6] c0[7] c2[4] c2[5] c2[6] c2[7]

    xvand.v    D0,     D4,    D4
    xvand.v    D1,     D5,    D5
    xvpermi.q  D0,     D1,    0x02 //c0: 0 1 2 3 4 5 6 7
    xvpermi.q  D1,     D4,    0x31 //c2: 0 1 2 3 4 5 6 7

    xvpackod.w D4,     D8,    D6 //c1[0] c1[1] c3[0] c3[1] c1[2] c1[3] c3[2] c3[3]
    xvpermi.d  D4,     D4,    0xd8 //c1[0] c1[1] c1[2] c1[3] c3[0] c3[1] c3[2] c3[3]
    xvpackod.w D5,     D9,    D7 //c1[4] c1[5] c3[4] c3[5] c1[6] c1[7] c3[6] c3[7]
    xvpermi.d  D5,     D5,    0xd8 //c1[4] c1[5] c1[6] c1[7] c3[4] c3[5] c3[6] c3[7]

    xvand.v    D2,     D4,    D4
    xvand.v    D3,     D5,    D5
    xvpermi.q  D2,     D3,    0x02 //c1: 0 1 2 3 4 5 6 7
    xvpermi.q  D3,     D4,    0x31 //c3: 0 1 2 3 4 5 6 7

    xvst       D0,     C0,    0x00
    xvst       D2,     C1,    0x00
    xvst       D1,     C2,    0x00
    xvst       D3,     C3,    0x00

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

    vxor.v    $vr2,     $vr2,   $vr2
    vxor.v    $vr3,     $vr3,   $vr3
    vxor.v    $vr4,     $vr4,   $vr4
    vxor.v    $vr5,     $vr5,   $vr5

    move       L,      $r0   //cycle param k
    beq        L,      TL,    .L182
    blt        TL,     L,     .L182

.L181:  /* for (k=0; k<temp; k++) */
    vld       $vr16,     A0,    0x00  // a0ri a1ri
    vld       $vr18,     B0,    0x00  // b0ri b1ri
    vld       $vr19,     B0,    0x10  // b2ri b3ri

    vshuf4i.w  $vr20,     $vr16,    0x00  //a0r
    vshuf4i.w  $vr21,     $vr16,    0x55  //a0i

    vpackev.w  $vr22,     $vr19,    $vr18
    vshuf4i.w  $vr22,     $vr22,    0xd8  //b0r b1r b2r b3r

    vpackod.w  $vr23,     $vr19,    $vr18
    vshuf4i.w  $vr23,     $vr23,    0xd8  //b0i b1i b2i b3i

    VMADD1    $vr2,     $vr20,    $vr22,     $vr2  //00r 10r 20r 30r
    VMADD2    $vr3,     $vr21,    $vr22,     $vr3  //00i 10i 20i 30i
    VMADD3    $vr2,     $vr21,    $vr23,     $vr2
    VMADD4    $vr3,     $vr20,    $vr23,     $vr3

    vshuf4i.w  $vr20,     $vr16,    0xaa  //a1r
    vshuf4i.w  $vr21,     $vr16,    0xff  //a1i

    VMADD1    $vr4,     $vr20,    $vr22,     $vr4  //01r 11r 21r 31r
    VMADD2    $vr5,     $vr21,    $vr22,     $vr5  //01i 11i 21i 31i
    VMADD3    $vr4,     $vr21,    $vr23,     $vr4
    VMADD4    $vr5,     $vr20,    $vr23,     $vr5

    addi.d     A0,     A0,    0x10
    addi.d     B0,     B0,    0x20

    addi.d     L,      L,     1
    blt        L,      TL,    .L181

.L182:
#if defined(TRMMKERNEL)
    //res00 res10 res20 res30
    vld       $vr16,     C0,    0x00 //c0: 0 1 2 3
    vld       $vr17,     C1,    0x00 //c1: 0 1 2 3
    vld       $vr18,     C2,    0x00 //c2: 0 1 2 3
    vld       $vr19,     C3,    0x00 //c3: 0 1 2 3

    vand.v    $vr20,     $vr17,    $vr17
    vpermi.w  $vr20,     $vr16,    0x44 //c0:0 1, c1:0 1
    vshuf4i.w  $vr22,     $vr20,    0xd8 //c0[0] c1[0] c0[1] c1[1]
    vshuf4i.w  $vr23,     $vr20,    0x8d //c0[1] c1[1] c0[0] c1[0]

    vand.v    $vr21,     $vr19,    $vr19
    vpermi.w  $vr21,     $vr18,    0x44 //c2:0 1, c3:0 1
    vshuf4i.w  $vr24,     $vr21,    0xd8 //c2[0] c3[0] c2[1] c3[1]
    vshuf4i.w  $vr25,     $vr21,    0x8d //c2[1] c3[1] c2[0] c3[0]

    vpermi.w  $vr24,     $vr22,    0x44 //c0[0] c1[0] c2[0] c3[0]
    vpermi.w  $vr25,     $vr23,    0x44 //c0[1] c1[1] c2[1] c3[1]

    vfmul.s      $vr24,    $vr2,    $vr28
    vfmul.s      $vr25,    $vr3,    $vr28
    VNMSUB      $vr24,    $vr3,    $vr29, $vr24
    VFMADD      $vr25,    $vr2,    $vr29, $vr25

    vand.v    $vr26,     $vr25,    $vr25 //c0[1] c1[1] c2[1] c3[1]
    vand.v    $vr27,     $vr25,    $vr25 //c0[0] c1[0] c2[0] c3[0]

    vpermi.w  $vr26,     $vr24,    0x44 //c0[0] c1[0] c0[1] c1[1]
    vshuf4i.w  $vr26,     $vr26,   0xd8 //c0[0] c0[1] c1[0] c1[1]

    vpermi.w  $vr27,     $vr24,    0xee //c2[0] c3[0] c2[1] c3[1]
    vshuf4i.w  $vr27,     $vr27,   0xd8 //c2[0] c2[1] c3[0] c3[1]

    //res01 res11 res21 res31
    vand.v    $vr20,     $vr17,    $vr17
    vpermi.w  $vr20,     $vr16,    0xee //c0:2 3, c1:2 3
    vshuf4i.w  $vr22,     $vr20,    0xd8 //c0[2] c1[2] c0[3] c1[3]
    vshuf4i.w  $vr23,     $vr20,    0x8d //c0[3] c1[3] c0[2] c1[2]

    vand.v    $vr21,     $vr19,    $vr19
    vpermi.w  $vr21,     $vr18,    0xee //c2:2 3, c3:2 3
    vshuf4i.w  $vr24,     $vr21,    0xd8 //c2[2] c3[2] c2[3] c3[3]
    vshuf4i.w  $vr25,     $vr21,    0x8d //c2[3] c3[3] c2[2] c3[2]

    vpermi.w  $vr24,     $vr22,    0x44 //c0[2] c1[2] c2[2] c3[2]
    vpermi.w  $vr25,     $vr23,    0x44 //c0[3] c1[3] c2[3] c3[3]

    vfmul.s      $vr24,    $vr4,    $vr28
    vfmul.s      $vr25,    $vr5,    $vr28
    VNMSUB      $vr24,    $vr5,    $vr29, $vr24
    VFMADD      $vr25,    $vr4,    $vr29, $vr25

    vand.v    $vr20,     $vr25,    $vr25
    vpermi.w  $vr20,     $vr24,    0x44 //c0[2] c1[2] c0[3] c1[3]
    vshuf4i.w $vr20,     $vr20,    0xd8 //c0[2] c0[3] c1[2] c1[3]
    vand.v    $vr18,     $vr20,    $vr20

    vand.v    $vr21,     $vr25,    $vr25
    vpermi.w  $vr21,     $vr24,    0xee //c2[2] c3[2] c2[3] c3[3]
    vshuf4i.w $vr21,     $vr21,    0xd8 //c2[2] c2[3] c3[2] c3[3]
    vand.v    $vr19,     $vr21,    $vr21

    vand.v    $vr16,     $vr26,    $vr26 //c0[0] c0[1] c1[0] c1[1]
    vand.v    $vr17,     $vr27,    $vr27 //c2[0] c2[1] c3[0] c3[1]

    vpermi.w  $vr20,     $vr16,     0x44 //c0: 0 1 2 3
    vpermi.w  $vr18,     $vr16,     0xee //c1: 0 1 2 3
    vpermi.w  $vr21,     $vr17,     0x44 //c2: 0 1 2 3
    vpermi.w  $vr19,     $vr17,     0xee //c3: 0 1 2 3

    vst       $vr20,     C0,    0x00
    vst       $vr18,     C1,    0x00
    vst       $vr21,     C2,    0x00
    vst       $vr19,     C3,    0x00

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10
    addi.d     C2,     C2,    0x10
    addi.d     C3,     C3,    0x10
#else
    //res00 res10 res20 res30
    vld       $vr16,     C0,    0x00 //c0: 0 1 2 3
    vld       $vr17,     C1,    0x00 //c1: 0 1 2 3
    vld       $vr18,     C2,    0x00 //c2: 0 1 2 3
    vld       $vr19,     C3,    0x00 //c3: 0 1 2 3

    vand.v    $vr20,     $vr17,    $vr17
    vpermi.w  $vr20,     $vr16,    0x44 //c0:0 1, c1:0 1
    vshuf4i.w  $vr22,     $vr20,    0xd8 //c0[0] c1[0] c0[1] c1[1]
    vshuf4i.w  $vr23,     $vr20,    0x8d //c0[1] c1[1] c0[0] c1[0]

    vand.v    $vr21,     $vr19,    $vr19
    vpermi.w  $vr21,     $vr18,    0x44 //c2:0 1, c3:0 1
    vshuf4i.w  $vr24,     $vr21,    0xd8 //c2[0] c3[0] c2[1] c3[1]
    vshuf4i.w  $vr25,     $vr21,    0x8d //c2[1] c3[1] c2[0] c3[0]

    vpermi.w  $vr24,     $vr22,    0x44 //c0[0] c1[0] c2[0] c3[0]
    vpermi.w  $vr25,     $vr23,    0x44 //c0[1] c1[1] c2[1] c3[1]

    VFMADD      $vr24,    $vr2,    $vr28, $vr24
    VFMADD      $vr25,    $vr3,    $vr28, $vr25
    VNMSUB      $vr24,    $vr3,    $vr29, $vr24
    VFMADD      $vr25,    $vr2,    $vr29, $vr25

    vand.v    $vr26,     $vr25,    $vr25 //c0[1] c1[1] c2[1] c3[1]
    vand.v    $vr27,     $vr25,    $vr25 //c0[0] c1[0] c2[0] c3[0]

    vpermi.w  $vr26,     $vr24,    0x44 //c0[0] c1[0] c0[1] c1[1]
    vshuf4i.w  $vr26,     $vr26,   0xd8 //c0[0] c0[1] c1[0] c1[1]

    vpermi.w  $vr27,     $vr24,    0xee //c2[0] c3[0] c2[1] c3[1]
    vshuf4i.w  $vr27,     $vr27,   0xd8 //c2[0] c2[1] c3[0] c3[1]

    //res01 res11 res21 res31
    vand.v    $vr20,     $vr17,    $vr17
    vpermi.w  $vr20,     $vr16,    0xee //c0:2 3, c1:2 3
    vshuf4i.w  $vr22,     $vr20,    0xd8 //c0[2] c1[2] c0[3] c1[3]
    vshuf4i.w  $vr23,     $vr20,    0x8d //c0[3] c1[3] c0[2] c1[2]

    vand.v    $vr21,     $vr19,    $vr19
    vpermi.w  $vr21,     $vr18,    0xee //c2:2 3, c3:2 3
    vshuf4i.w  $vr24,     $vr21,    0xd8 //c2[2] c3[2] c2[3] c3[3]
    vshuf4i.w  $vr25,     $vr21,    0x8d //c2[3] c3[3] c2[2] c3[2]

    vpermi.w  $vr24,     $vr22,    0x44 //c0[2] c1[2] c2[2] c3[2]
    vpermi.w  $vr25,     $vr23,    0x44 //c0[3] c1[3] c2[3] c3[3]

    VFMADD      $vr24,    $vr4,    $vr28, $vr24
    VFMADD      $vr25,    $vr5,    $vr28, $vr25
    VNMSUB      $vr24,    $vr5,    $vr29, $vr24
    VFMADD      $vr25,    $vr4,    $vr29, $vr25

    vand.v    $vr20,     $vr25,    $vr25
    vpermi.w  $vr20,     $vr24,    0x44 //c0[2] c1[2] c0[3] c1[3]
    vshuf4i.w $vr20,     $vr20,    0xd8 //c0[2] c0[3] c1[2] c1[3]
    vand.v    $vr18,     $vr20,    $vr20

    vand.v    $vr21,     $vr25,    $vr25
    vpermi.w  $vr21,     $vr24,    0xee //c2[2] c3[2] c2[3] c3[3]
    vshuf4i.w $vr21,     $vr21,    0xd8 //c2[2] c2[3] c3[2] c3[3]
    vand.v    $vr19,     $vr21,    $vr21

    vand.v    $vr16,     $vr26,    $vr26 //c0[0] c0[1] c1[0] c1[1]
    vand.v    $vr17,     $vr27,    $vr27 //c2[0] c2[1] c3[0] c3[1]

    vpermi.w  $vr20,     $vr16,     0x44 //c0: 0 1 2 3
    vpermi.w  $vr18,     $vr16,     0xee //c1: 0 1 2 3
    vpermi.w  $vr21,     $vr17,     0x44 //c2: 0 1 2 3
    vpermi.w  $vr19,     $vr17,     0xee //c3: 0 1 2 3

    vst       $vr20,     C0,    0x00
    vst       $vr18,     C1,    0x00
    vst       $vr21,     C2,    0x00
    vst       $vr19,     C3,    0x00

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
    slli.d     T3,     TL,   0x05
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
    srai.d     T0,     M,     4  //bm/16
    beq        I,      T0,    .L21

.L201:  /* for (i=0; i<bm/16; i+=1) */
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
    addi.d     TL,     OFF,   16
#else
    addi.d     TL,     OFF,   2
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
    beq        L,      TL,    .L203
    blt        TL,     L,     .L203

.L202:  /* for (k=0; k<temp; k++) */
    xvld       D0,     A0,    0x00  //a0ri a1ri a2ri a3ri
    vld       $vr18,     B0,    0x00  //b0ri b1ri

    xvpackev.w D4,     D0,    D0  //a0rr a1rr a2rr a3rr
    xvpackod.w D5,     D0,    D0  //a0ii a1ii a2ii a3ii

    vpackev.w $vr22,     $vr18,    $vr18 //b0rr b1rr
    vpackod.w $vr23,     $vr18,    $vr18 //b0ii b1ii

    vpermi.w  $vr22,     $vr22,    0x88 //b0r b1r b0r b1r
    vpermi.w  $vr23,     $vr23,    0x88 //b0i b1i b0i b1i

    xvpermi.d  D6,     D6,    0x00 //b0r b1r b0r b1r b0r b1r b0r b1r
    xvpermi.d  D7,     D7,    0x00 //b0i b1i b0i b1i b0i b1i b0i b1i

    XVMADD1    U0,     D4,    D6,     U0  //00r 10r 01r 11r 02r 12r 03r 13r
    XVMADD2    U1,     D5,    D6,     U1  //00i 10i 01i 11i 02i 12i 03i 13i
    XVMADD3    U0,     D5,    D7,     U0
    XVMADD4    U1,     D4,    D7,     U1

    xvld       D0,     A0,    0x20

    xvpackev.w D4,     D0,    D0
    xvpackod.w D5,     D0,    D0

    XVMADD1    U2,     D4,    D6,     U2  //04r 14r 05r 15r 06r 16r 07r 17r
    XVMADD2    U3,     D5,    D6,     U3  //04i 14i 05i 15i 06i 16i 07i 17i
    XVMADD3    U2,     D5,    D7,     U2
    XVMADD4    U3,     D4,    D7,     U3

    xvld       D0,     A0,    0x40

    xvpackev.w D4,     D0,    D0
    xvpackod.w D5,     D0,    D0

    XVMADD1    U4,     D4,    D6,     U4  //08r 18r 09r 19r 0ar 1ar 0br 1br
    XVMADD2    U5,     D5,    D6,     U5  //08i 18i 09i 19i 0ai 1ai 0bi 1bi
    XVMADD3    U4,     D5,    D7,     U4
    XVMADD4    U5,     D4,    D7,     U5

    xvld       D0,     A0,    0x60

    xvpackev.w D4,     D0,    D0
    xvpackod.w D5,     D0,    D0

    XVMADD1    U6,     D4,    D6,     U6  //0cr 1cr 0dr 1dr 0er 1er 0fr 1fr
    XVMADD2    U7,     D5,    D6,     U7  //0ci 1ci 0di 1di 0ei 1ei 0fi 1fi
    XVMADD3    U6,     D5,    D7,     U6
    XVMADD4    U7,     D4,    D7,     U7

    addi.d     A0,     A0,    0x80
    addi.d     B0,     B0,    0x10

    addi.d     L,      L,     1
    blt        L,      TL,    .L202

.L203:
#if defined(TRMMKERNEL)
    //res00 res10 res01 res11 res02 res12 res03 res13
    xvld       D0,     C0,    0x00 //c0: 0 1 2 3 4 5 6 7
    xvld       D1,     C1,    0x00 //c1: 0 1 2 3 4 5 6 7

    xvpackev.w D4,     D1,    D0   //c0[0] c1[0] c0[2] c1[2] c0[4] c1[4] c0[6] c1[6]
    xvpackod.w D5,     D1,    D0   //c0[1] c1[1] c0[3] c1[3] c0[5] c1[5] c0[7] c1[7]

    xvfmul.s      D4,    U0,    VALPHAR
    xvfmul.s      D5,    U1,    VALPHAR
    XVNMSUB      D4,    U1,    VALPHAI, D4
    XVFMADD      D5,    U0,    VALPHAI, D5

    xvpackev.w D0,     D5,    D4   //c0: 0 1 2 3 4 5 6 7
    xvpackod.w D1,     D5,    D4   //c1: 0 1 2 3 4 5 6 7

    xvst       D0,     C0,    0x00
    xvst       D1,     C1,    0x00

    addi.d     C0,     C0,    0x20
    addi.d     C1,     C1,    0x20

    //res04 res14 res05 res15 res06 res16 res07 res17
    xvld       D0,     C0,    0x00
    xvld       D1,     C1,    0x00

    xvpackev.w D4,     D1,    D0
    xvpackod.w D5,     D1,    D0

    xvfmul.s      D4,    U2,    VALPHAR
    xvfmul.s      D5,    U3,    VALPHAR
    XVNMSUB      D4,    U3,    VALPHAI, D4
    XVFMADD      D5,    U2,    VALPHAI, D5

    xvpackev.w D0,     D5,    D4
    xvpackod.w D1,     D5,    D4

    xvst       D0,     C0,    0x00
    xvst       D1,     C1,    0x00

    addi.d     C0,     C0,    0x20
    addi.d     C1,     C1,    0x20

    //res08 res18 res09 res19 res0a res1a res0b res1b
    xvld       D0,     C0,    0x00
    xvld       D1,     C1,    0x00

    xvpackev.w D4,     D1,    D0
    xvpackod.w D5,     D1,    D0

    xvfmul.s      D4,    U4,    VALPHAR
    xvfmul.s      D5,    U5,    VALPHAR
    XVNMSUB      D4,    U5,    VALPHAI, D4
    XVFMADD      D5,    U4,    VALPHAI, D5

    xvpackev.w D0,     D5,    D4
    xvpackod.w D1,     D5,    D4

    xvst       D0,     C0,    0x00
    xvst       D1,     C1,    0x00

    addi.d     C0,     C0,    0x20
    addi.d     C1,     C1,    0x20

    //res0c res1c res0d res1d res0e res1e res0f res1f
    xvld       D0,     C0,    0x00
    xvld       D1,     C1,    0x00

    xvpackev.w D4,     D1,    D0
    xvpackod.w D5,     D1,    D0

    xvfmul.s      D4,    U6,    VALPHAR
    xvfmul.s      D5,    U7,    VALPHAR
    XVNMSUB      D4,    U7,    VALPHAI, D4
    XVFMADD      D5,    U6,    VALPHAI, D5

    xvpackev.w D0,     D5,    D4
    xvpackod.w D1,     D5,    D4

    xvst       D0,     C0,    0x00
    xvst       D1,     C1,    0x00

    addi.d     C0,     C0,    0x20
    addi.d     C1,     C1,    0x20
#else
    //res00 res10 res01 res11 res02 res12 res03 res13
    xvld       D0,     C0,    0x00 //c0: 0 1 2 3 4 5 6 7
    xvld       D1,     C1,    0x00 //c1: 0 1 2 3 4 5 6 7

    xvpackev.w D4,     D1,    D0   //c0[0] c1[0] c0[2] c1[2] c0[4] c1[4] c0[6] c1[6]
    xvpackod.w D5,     D1,    D0   //c0[1] c1[1] c0[3] c1[3] c0[5] c1[5] c0[7] c1[7]

    XVFMADD      D4,    U0,    VALPHAR, D4
    XVFMADD      D5,    U1,    VALPHAR, D5
    XVNMSUB      D4,    U1,    VALPHAI, D4
    XVFMADD      D5,    U0,    VALPHAI, D5

    xvpackev.w D0,     D5,    D4   //c0: 0 1 2 3 4 5 6 7
    xvpackod.w D1,     D5,    D4   //c1: 0 1 2 3 4 5 6 7

    xvst       D0,     C0,    0x00
    xvst       D1,     C1,    0x00

    addi.d     C0,     C0,    0x20
    addi.d     C1,     C1,    0x20

    //res04 res14 res05 res15 res06 res16 res07 res17
    xvld       D0,     C0,    0x00
    xvld       D1,     C1,    0x00

    xvpackev.w D4,     D1,    D0
    xvpackod.w D5,     D1,    D0

    XVFMADD      D4,    U2,    VALPHAR, D4
    XVFMADD      D5,    U3,    VALPHAR, D5
    XVNMSUB      D4,    U3,    VALPHAI, D4
    XVFMADD      D5,    U2,    VALPHAI, D5

    xvpackev.w D0,     D5,    D4
    xvpackod.w D1,     D5,    D4

    xvst       D0,     C0,    0x00
    xvst       D1,     C1,    0x00

    addi.d     C0,     C0,    0x20
    addi.d     C1,     C1,    0x20

    //res08 res18 res09 res19 res0a res1a res0b res1b
    xvld       D0,     C0,    0x00
    xvld       D1,     C1,    0x00

    xvpackev.w D4,     D1,    D0
    xvpackod.w D5,     D1,    D0

    XVFMADD      D4,    U4,    VALPHAR, D4
    XVFMADD      D5,    U5,    VALPHAR, D5
    XVNMSUB      D4,    U5,    VALPHAI, D4
    XVFMADD      D5,    U4,    VALPHAI, D5

    xvpackev.w D0,     D5,    D4
    xvpackod.w D1,     D5,    D4

    xvst       D0,     C0,    0x00
    xvst       D1,     C1,    0x00

    addi.d     C0,     C0,    0x20
    addi.d     C1,     C1,    0x20

    //res0c res1c res0d res1d res0e res1e res0f res1f
    xvld       D0,     C0,    0x00
    xvld       D1,     C1,    0x00

    xvpackev.w D4,     D1,    D0
    xvpackod.w D5,     D1,    D0

    XVFMADD      D4,    U6,    VALPHAR, D4
    XVFMADD      D5,    U7,    VALPHAR, D5
    XVNMSUB      D4,    U7,    VALPHAI, D4
    XVFMADD      D5,    U6,    VALPHAI, D5

    xvpackev.w D0,     D5,    D4
    xvpackod.w D1,     D5,    D4

    xvst       D0,     C0,    0x00
    xvst       D1,     C1,    0x00

    addi.d     C0,     C0,    0x20
    addi.d     C1,     C1,    0x20
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d      TL,     K,    OFF
#ifdef LEFT
    addi.d     TL,     TL,   -16
#else
    addi.d     TL,     TL,   -2
#endif
    slli.d     T3,     TL,   0x07
    add.d      A0,     A0,   T3
    slli.d     T3,     TL,   0x04
    add.d      B0,     B0,   T3
#endif

#ifdef LEFT
    addi.d     OFF,    OFF,   16
#endif
#endif   // #if defined(TRMMKERNEL)

    addi.d     I,      I,     1
    blt        I,      T0,    .L201

.L21:  /* if (bm & 8) */
    move       I,      $r0
    andi       T1,     M,     8    //bm&8
    beq        I,      T1,    .L24

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

    xvxor.v    U0,     U0,   U0
    xvxor.v    U1,     U1,   U1
    xvxor.v    U2,     U2,   U2
    xvxor.v    U3,     U3,   U3

    move       L,      $r0   //cycle param k
    beq        L,      TL,    .L23
    blt        TL,     L,     .L23

.L22:  /* for (k=0; k<temp; k++) */
    xvld       D0,     A0,    0x00  //a0ri a1ri a2ri a3ri
    vld       $vr18,     B0,    0x00  //b0ri b1ri

    xvpackev.w D4,     D0,    D0  //a0rr a1rr a2rr a3rr
    xvpackod.w D5,     D0,    D0  //a0ii a1ii a2ii a3ii

    vpackev.w $vr22,     $vr18,    $vr18 //b0rr b1rr
    vpackod.w $vr23,     $vr18,    $vr18 //b0ii b1ii

    vpermi.w  $vr22,     $vr22,    0x88 //b0r b1r b0r b1r
    vpermi.w  $vr23,     $vr23,    0x88 //b0i b1i b0i b1i

    xvpermi.d  D6,     D6,    0x00 //b0r b1r b0r b1r b0r b1r b0r b1r
    xvpermi.d  D7,     D7,    0x00 //b0i b1i b0i b1i b0i b1i b0i b1i

    XVMADD1    U0,     D4,    D6,     U0  //00r 10r 01r 11r 02r 12r 03r 13r
    XVMADD2    U1,     D5,    D6,     U1  //00i 10i 01i 11i 02i 12i 03i 13i
    XVMADD3    U0,     D5,    D7,     U0
    XVMADD4    U1,     D4,    D7,     U1

    xvld       D0,     A0,    0x20

    xvpackev.w D4,     D0,    D0
    xvpackod.w D5,     D0,    D0

    XVMADD1    U2,     D4,    D6,     U2  //04r 14r 05r 15r 06r 16r 07r 17r
    XVMADD2    U3,     D5,    D6,     U3  //04i 14i 05i 15i 06i 16i 07i 17i
    XVMADD3    U2,     D5,    D7,     U2
    XVMADD4    U3,     D4,    D7,     U3

    addi.d     A0,     A0,    0x40
    addi.d     B0,     B0,    0x10

    addi.d     L,      L,     1
    blt        L,      TL,    .L22

.L23:
#if defined(TRMMKERNEL)
    //res00 res10 res01 res11 res02 res12 res03 res13
    xvld       D0,     C0,    0x00 //c0: 0 1 2 3 4 5 6 7
    xvld       D1,     C1,    0x00 //c1: 0 1 2 3 4 5 6 7

    xvpackev.w D4,     D1,    D0   //c0[0] c1[0] c0[2] c1[2] c0[4] c1[4] c0[6] c1[6]
    xvpackod.w D5,     D1,    D0   //c0[1] c1[1] c0[3] c1[3] c0[5] c1[5] c0[7] c1[7]

    xvfmul.s      D4,    U0,    VALPHAR
    xvfmul.s      D5,    U1,    VALPHAR
    XVNMSUB      D4,    U1,    VALPHAI, D4
    XVFMADD      D5,    U0,    VALPHAI, D5

    xvpackev.w D0,     D5,    D4   //c0: 0 1 2 3 4 5 6 7
    xvpackod.w D1,     D5,    D4   //c1: 0 1 2 3 4 5 6 7

    xvst       D0,     C0,    0x00
    xvst       D1,     C1,    0x00

    addi.d     C0,     C0,    0x20
    addi.d     C1,     C1,    0x20

    //res04 res14 res05 res15 res06 res16 res07 res17
    xvld       D0,     C0,    0x00
    xvld       D1,     C1,    0x00

    xvpackev.w D4,     D1,    D0
    xvpackod.w D5,     D1,    D0

    xvfmul.s      D4,    U2,    VALPHAR
    xvfmul.s      D5,    U3,    VALPHAR
    XVNMSUB      D4,    U3,    VALPHAI, D4
    XVFMADD      D5,    U2,    VALPHAI, D5

    xvpackev.w D0,     D5,    D4
    xvpackod.w D1,     D5,    D4

    xvst       D0,     C0,    0x00
    xvst       D1,     C1,    0x00

    addi.d     C0,     C0,    0x20
    addi.d     C1,     C1,    0x20
#else
    //res00 res10 res01 res11 res02 res12 res03 res13
    xvld       D0,     C0,    0x00 //c0: 0 1 2 3 4 5 6 7
    xvld       D1,     C1,    0x00 //c1: 0 1 2 3 4 5 6 7

    xvpackev.w D4,     D1,    D0   //c0[0] c1[0] c0[2] c1[2] c0[4] c1[4] c0[6] c1[6]
    xvpackod.w D5,     D1,    D0   //c0[1] c1[1] c0[3] c1[3] c0[5] c1[5] c0[7] c1[7]

    XVFMADD      D4,    U0,    VALPHAR, D4
    XVFMADD      D5,    U1,    VALPHAR, D5
    XVNMSUB      D4,    U1,    VALPHAI, D4
    XVFMADD      D5,    U0,    VALPHAI, D5

    xvpackev.w D0,     D5,    D4   //c0: 0 1 2 3 4 5 6 7
    xvpackod.w D1,     D5,    D4   //c1: 0 1 2 3 4 5 6 7

    xvst       D0,     C0,    0x00
    xvst       D1,     C1,    0x00

    addi.d     C0,     C0,    0x20
    addi.d     C1,     C1,    0x20

    //res04 res14 res05 res15 res06 res16 res07 res17
    xvld       D0,     C0,    0x00
    xvld       D1,     C1,    0x00

    xvpackev.w D4,     D1,    D0
    xvpackod.w D5,     D1,    D0

    XVFMADD      D4,    U2,    VALPHAR, D4
    XVFMADD      D5,    U3,    VALPHAR, D5
    XVNMSUB      D4,    U3,    VALPHAI, D4
    XVFMADD      D5,    U2,    VALPHAI, D5

    xvpackev.w D0,     D5,    D4
    xvpackod.w D1,     D5,    D4

    xvst       D0,     C0,    0x00
    xvst       D1,     C1,    0x00

    addi.d     C0,     C0,    0x20
    addi.d     C1,     C1,    0x20
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

    xvxor.v    U0,     U0,   U0
    xvxor.v    U1,     U1,   U1

    move       L,      $r0   //cycle param k
    beq        L,      TL,    .L27
    blt        TL,     L,     .L27

.L26:  /* for (k=0; k<temp; k++) */
    xvld       D0,     A0,    0x00  //a0ri a1ri a2ri a3ri
    vld       $vr18,     B0,    0x00  //b0ri b1ri

    xvpackev.w D4,     D0,    D0  //a0rr a1rr a2rr a3rr
    xvpackod.w D5,     D0,    D0  //a0ii a1ii a2ii a3ii

    vpackev.w $vr22,     $vr18,    $vr18 //b0rr b1rr
    vpackod.w $vr23,     $vr18,    $vr18 //b0ii b1ii

    vpermi.w  $vr22,     $vr22,    0x88 //b0r b1r b0r b1r
    vpermi.w  $vr23,     $vr23,    0x88 //b0i b1i b0i b1i

    xvpermi.d  D6,     D6,    0x00 //b0r b1r b0r b1r b0r b1r b0r b1r
    xvpermi.d  D7,     D7,    0x00 //b0i b1i b0i b1i b0i b1i b0i b1i

    XVMADD1    U0,     D4,    D6,     U0  //00r 10r 01r 11r 02r 12r 03r 13r
    XVMADD2    U1,     D5,    D6,     U1  //00i 10i 01i 11i 02i 12i 03i 13i
    XVMADD3    U0,     D5,    D7,     U0
    XVMADD4    U1,     D4,    D7,     U1

    addi.d     A0,     A0,    0x20
    addi.d     B0,     B0,    0x10

    addi.d     L,      L,     1
    blt        L,      TL,    .L26

.L27:
#if defined(TRMMKERNEL)
    //res00 res10 res01 res11 res02 res12 res03 res13
    xvld       D0,     C0,    0x00 //c0: 0 1 2 3 4 5 6 7
    xvld       D1,     C1,    0x00 //c1: 0 1 2 3 4 5 6 7

    xvpackev.w D4,     D1,    D0   //c0[0] c1[0] c0[2] c1[2] c0[4] c1[4] c0[6] c1[6]
    xvpackod.w D5,     D1,    D0   //c0[1] c1[1] c0[3] c1[3] c0[5] c1[5] c0[7] c1[7]

    xvfmul.s      D4,    U0,    VALPHAR
    xvfmul.s      D5,    U1,    VALPHAR
    XVNMSUB      D4,    U1,    VALPHAI, D4
    XVFMADD      D5,    U0,    VALPHAI, D5

    xvpackev.w D0,     D5,    D4   //c0: 0 1 2 3 4 5 6 7
    xvpackod.w D1,     D5,    D4   //c1: 0 1 2 3 4 5 6 7

    xvst       D0,     C0,    0x00
    xvst       D1,     C1,    0x00

    addi.d     C0,     C0,    0x20
    addi.d     C1,     C1,    0x20
#else
    //res00 res10 res01 res11 res02 res12 res03 res13
    xvld       D0,     C0,    0x00 //c0: 0 1 2 3 4 5 6 7
    xvld       D1,     C1,    0x00 //c1: 0 1 2 3 4 5 6 7

    xvpackev.w D4,     D1,    D0   //c0[0] c1[0] c0[2] c1[2] c0[4] c1[4] c0[6] c1[6]
    xvpackod.w D5,     D1,    D0   //c0[1] c1[1] c0[3] c1[3] c0[5] c1[5] c0[7] c1[7]

    XVFMADD      D4,    U0,    VALPHAR, D4
    XVFMADD      D5,    U1,    VALPHAR, D5
    XVNMSUB      D4,    U1,    VALPHAI, D4
    XVFMADD      D5,    U0,    VALPHAI, D5

    xvpackev.w D0,     D5,    D4   //c0: 0 1 2 3 4 5 6 7
    xvpackod.w D1,     D5,    D4   //c1: 0 1 2 3 4 5 6 7

    xvst       D0,     C0,    0x00
    xvst       D1,     C1,    0x00

    addi.d     C0,     C0,    0x20
    addi.d     C1,     C1,    0x20
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

    vxor.v    $vr2,     $vr2,   $vr2
    vxor.v    $vr3,     $vr3,   $vr3

    move       L,      $r0   //cycle param k
    beq        L,      TL,    .L283
    blt        TL,     L,     .L283

.L282:  /* for (k=0; k<temp; k++) */
    vld       $vr16,     A0,    0x00  // a0ri a1ri
    vld       $vr18,     B0,    0x00  // b0ri b1ri

    vpackev.w $vr20,     $vr16,    $vr16  //a0rr a1rr
    vpackod.w $vr21,     $vr16,    $vr16  //a0ii a1ii

    vpackev.w $vr22,     $vr18,    $vr18 //b0rr b1rr
    vpackod.w $vr23,     $vr18,    $vr18 //b0ii b1ii

    vpermi.w  $vr22,     $vr22,    0x88 //b0r b1r b0r b1r
    vpermi.w  $vr23,     $vr23,    0x88 //b0i b1i b0i b1i

    VMADD1    $vr2,     $vr20,    $vr22,     $vr2  //00r 10r 01r 11r
    VMADD2    $vr3,     $vr21,    $vr22,     $vr3  //00i 10i 01i 11i
    VMADD3    $vr2,     $vr21,    $vr23,     $vr2
    VMADD4    $vr3,     $vr20,    $vr23,     $vr3

    addi.d     A0,     A0,    0x10
    addi.d     B0,     B0,    0x10

    addi.d     L,      L,     1
    blt        L,      TL,    .L282

.L283:
#if defined(TRMMKERNEL)
    //res00 res10 res01 res11
    vld       $vr16,     C0,    0x00 //c0: 0 1 2 3
    vld       $vr17,     C1,    0x00 //c1: 0 1 2 3

    vpackev.w $vr18,     $vr17,    $vr16  //c0[0] c1[0] c0[2] c1[2]
    vpackod.w $vr19,     $vr17,    $vr16  //c0[1] c1[1] c0[3] c1[3]

    vfmul.s      $vr18,    $vr2,    $vr28
    vfmul.s      $vr19,    $vr3,    $vr28
    VNMSUB      $vr18,    $vr3,    $vr29, $vr18
    VFMADD      $vr19,    $vr2,    $vr29, $vr19

    vpackev.w $vr20,     $vr19,    $vr18
    vpackod.w $vr21,     $vr19,    $vr18

    vst       $vr20,     C0,    0x00 //c0: 0 1 2 3
    vst       $vr21,     C1,    0x00 //c1: 0 1 2 3

    addi.d     C0,     C0,    0x10
    addi.d     C1,     C1,    0x10
#else
    //res00 res10 res01 res11
    vld       $vr16,     C0,    0x00 //c0: 0 1 2 3
    vld       $vr17,     C1,    0x00 //c1: 0 1 2 3

    vpackev.w $vr18,     $vr17,    $vr16  //c0[0] c1[0] c0[2] c1[2]
    vpackod.w $vr19,     $vr17,    $vr16  //c0[1] c1[1] c0[3] c1[3]

    VFMADD      $vr18,    $vr2,    $vr28, $vr18
    VFMADD      $vr19,    $vr3,    $vr28, $vr19
    VNMSUB      $vr18,    $vr3,    $vr29, $vr18
    VFMADD      $vr19,    $vr2,    $vr29, $vr19

    vpackev.w $vr20,     $vr19,    $vr18
    vpackod.w $vr21,     $vr19,    $vr18

    vst       $vr20,     C0,    0x00 //c0: 0 1 2 3
    vst       $vr21,     C1,    0x00 //c1: 0 1 2 3

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

    addi.d     C0,     C0,    0x08
    addi.d     C1,     C1,    0x08
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
    srai.d     T0,     M,     4  //bm/16
    beq        I,      T0,    .L31

.L301:  /* for (i=0; i<bm/16; i+=1) */
    move       B0,     B      //ptrbb
    move       TL,     K      /* TL = bk */
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) ||(!defined(LEFT) && !defined(TRANSA))
    move       B0,     B    //ptrbb
#else
    slli.d     T3,     OFF,  0x07
    add.d      A0,     A0,   T3
    slli.d     T3,     OFF,  0x03
    add.d      B0,     B,    T3
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
    sub.d      TL,     K,     OFF
#elif defined(LEFT)
    addi.d     TL,     OFF,   16
#else
    addi.d     TL,     OFF,   1
#endif

#endif  // #if defined(TRMMKERNEL)

    xvxor.v    U0,     U0,   U0
    xvxor.v    U1,     U1,   U1
    xvxor.v    U2,     U2,   U2
    xvxor.v    U3,     U3,   U3

    move       L,      $r0   //cycle param k
    beq        L,      TL,    .L303
    blt        TL,     L,     .L303

.L302:  /* for (k=0; k<temp; k++) */
    xvld       D0,     A0,    0x00  //a0ri a1ri a2ri a3ri
    xvld       D1,     A0,    0x20  //a4ri a5ri a6ri a7ri

    xvldrepl.w D2,     B0,    0x00 //b0r
    xvldrepl.w D3,     B0,    0x04 //b0i

    xvpackev.w D4,     D1,    D0    //a0r a4r a1r a5r a2r a6r a3r a7r
    xvpermi.w  D4,     D4,    0xd8  //a0r a1r a4r a5r a2r a3r a6r a7r
    xvpermi.d  D4,     D4,    0xd8  //a0r a1r a2r a3r a4r a5r a6r a7r

    xvpackod.w D5,     D1,    D0    //a0i a4i a1i a5i a2i a6i a3i a7i
    xvpermi.w  D5,     D5,    0xd8  //a0i a1i a4i a5i a2i a3i a6i a7i
    xvpermi.d  D5,     D5,    0xd8  //a0i a1i a2i a3i a4i a5i a6i a7i

    XVMADD1    U0,     D4,    D2,     U0  //00r 01r 02r 03r 04r 05r 06r 07r
    XVMADD2    U1,     D5,    D2,     U1  //00i 01i 02i 03i 04i 05i 06i 07i
    XVMADD3    U0,     D5,    D3,     U0
    XVMADD4    U1,     D4,    D3,     U1

    xvld       D0,     A0,    0x40  //a8ri a9ri a10ri a11ri
    xvld       D1,     A0,    0x60  //a12ri a13ri a14ri a15ri

    xvpackev.w D4,     D1,    D0
    xvpermi.w  D4,     D4,    0xd8
    xvpermi.d  D4,     D4,    0xd8  //a8r a9r a10r a11r a12r a13r a14r a15r

    xvpackod.w D5,     D1,    D0
    xvpermi.w  D5,     D5,    0xd8
    xvpermi.d  D5,     D5,    0xd8  //a8i a9i a10i a11i a12i a13i a14i a15i

    XVMADD1    U2,     D4,    D2,     U2  //08r 09r 0ar 0br 0cr 0dr 0er 0fr
    XVMADD2    U3,     D5,    D2,     U3  //08i 09i 0ai 0bi 0ci 0di 0ei 0fi
    XVMADD3    U2,     D5,    D3,     U2
    XVMADD4    U3,     D4,    D3,     U3

    addi.d     A0,     A0,    0x80
    addi.d     B0,     B0,    0x08

    addi.d     L,      L,     1
    blt        L,      TL,    .L302

.L303:
#if defined(TRMMKERNEL)
    //res00 res01 res02 res03 res04 res05 res06 res07
    xvld       D0,     C0,    0x00 //c0:0 1 2 3 4 5 6 7
    xvld       D1,     C0,    0x20 //c0:8 9 10 11 12 13 14 15

    xvpackev.w D2,     D1,    D0  //0 8 2 10 4 12 6 14
    xvpermi.w  D2,     D2,    0xd8  //0 2 8 10 4 6 12 14
    xvpermi.d  D2,     D2,    0xd8  //0 2 4 6 8 10 12 14

    xvpackod.w D3,     D1,    D0
    xvpermi.w  D3,     D3,    0xd8
    xvpermi.d  D3,     D3,    0xd8 //1 3 5 7 9 11 13 15

    xvfmul.s      D2,    U0,    VALPHAR
    xvfmul.s      D3,    U1,    VALPHAR
    XVNMSUB      D2,    U1,    VALPHAI, D2
    XVFMADD      D3,    U0,    VALPHAI, D3

    xvand.v    D4,     D2,   D2  //0 2 4 6 8 10 12 14
    xvpermi.q  D4,     D3,   0x02 //0 2 4 6 1 3 5 7
    xvpermi.d  D4,     D4,   0xd8 //0 2 1 3 4 6 5 7
    xvpermi.w  D4,     D4,   0xd8 //0 1 2 3 4 5 6 7

    xvand.v    D5,     D3,   D3
    xvpermi.q  D5,     D2,   0x31
    xvpermi.d  D5,     D5,   0xd8
    xvpermi.w  D5,     D5,   0xd8

    xvst       D4,     C0,    0x00
    xvst       D5,     C0,    0x20

    addi.d     C0,     C0,    0x40

    //res08 res09 res0a res0b res0c res0d res0e res0f
    xvld       D0,     C0,    0x00 //c0:0 1 2 3 4 5 6 7
    xvld       D1,     C0,    0x20 //c0:8 9 10 11 12 13 14 15

    xvpackev.w D2,     D1,    D0  //0 8 2 10 4 12 6 14
    xvpermi.w  D2,     D2,    0xd8  //0 2 8 10 4 6 12 14
    xvpermi.d  D2,     D2,    0xd8  //0 2 4 6 8 10 12 14

    xvpackod.w D3,     D1,    D0
    xvpermi.w  D3,     D3,    0xd8
    xvpermi.d  D3,     D3,    0xd8 //1 3 5 7 9 11 13 15

    xvfmul.s      D2,    U2,    VALPHAR
    xvfmul.s      D3,    U3,    VALPHAR
    XVNMSUB      D2,    U3,    VALPHAI, D2
    XVFMADD      D3,    U2,    VALPHAI, D3

    xvand.v    D4,     D2,   D2  //0 2 4 6 8 10 12 14
    xvpermi.q  D4,     D3,   0x02 //0 2 4 6 1 3 5 7
    xvpermi.d  D4,     D4,   0xd8 //0 2 1 3 4 6 5 7
    xvpermi.w  D4,     D4,   0xd8 //0 1 2 3 4 5 6 7

    xvand.v    D5,     D3,   D3
    xvpermi.q  D5,     D2,   0x31
    xvpermi.d  D5,     D5,   0xd8
    xvpermi.w  D5,     D5,   0xd8

    xvst       D4,     C0,    0x00
    xvst       D5,     C0,    0x20

    addi.d     C0,     C0,    0x40
#else
    //res00 res01 res02 res03 res04 res05 res06 res07
    xvld       D0,     C0,    0x00 //c0:0 1 2 3 4 5 6 7
    xvld       D1,     C0,    0x20 //c0:8 9 10 11 12 13 14 15

    xvpackev.w D2,     D1,    D0  //0 8 2 10 4 12 6 14
    xvpermi.w  D2,     D2,    0xd8  //0 2 8 10 4 6 12 14
    xvpermi.d  D2,     D2,    0xd8  //0 2 4 6 8 10 12 14

    xvpackod.w D3,     D1,    D0
    xvpermi.w  D3,     D3,    0xd8
    xvpermi.d  D3,     D3,    0xd8 //1 3 5 7 9 11 13 15

    XVFMADD      D2,    U0,    VALPHAR, D2
    XVFMADD      D3,    U1,    VALPHAR, D3
    XVNMSUB      D2,    U1,    VALPHAI, D2
    XVFMADD      D3,    U0,    VALPHAI, D3

    xvand.v    D4,     D2,   D2  //0 2 4 6 8 10 12 14
    xvpermi.q  D4,     D3,   0x02 //0 2 4 6 1 3 5 7
    xvpermi.d  D4,     D4,   0xd8 //0 2 1 3 4 6 5 7
    xvpermi.w  D4,     D4,   0xd8 //0 1 2 3 4 5 6 7

    xvand.v    D5,     D3,   D3
    xvpermi.q  D5,     D2,   0x31
    xvpermi.d  D5,     D5,   0xd8
    xvpermi.w  D5,     D5,   0xd8

    xvst       D4,     C0,    0x00
    xvst       D5,     C0,    0x20

    addi.d     C0,     C0,    0x40

    //res08 res09 res0a res0b res0c res0d res0e res0f
    xvld       D0,     C0,    0x00 //c0:0 1 2 3 4 5 6 7
    xvld       D1,     C0,    0x20 //c0:8 9 10 11 12 13 14 15

    xvpackev.w D2,     D1,    D0  //0 8 2 10 4 12 6 14
    xvpermi.w  D2,     D2,    0xd8  //0 2 8 10 4 6 12 14
    xvpermi.d  D2,     D2,    0xd8  //0 2 4 6 8 10 12 14

    xvpackod.w D3,     D1,    D0
    xvpermi.w  D3,     D3,    0xd8
    xvpermi.d  D3,     D3,    0xd8 //1 3 5 7 9 11 13 15

    XVFMADD      D2,    U2,    VALPHAR, D2
    XVFMADD      D3,    U3,    VALPHAR, D3
    XVNMSUB      D2,    U3,    VALPHAI, D2
    XVFMADD      D3,    U2,    VALPHAI, D3

    xvand.v    D4,     D2,   D2  //0 2 4 6 8 10 12 14
    xvpermi.q  D4,     D3,   0x02 //0 2 4 6 1 3 5 7
    xvpermi.d  D4,     D4,   0xd8 //0 2 1 3 4 6 5 7
    xvpermi.w  D4,     D4,   0xd8 //0 1 2 3 4 5 6 7

    xvand.v    D5,     D3,   D3
    xvpermi.q  D5,     D2,   0x31
    xvpermi.d  D5,     D5,   0xd8
    xvpermi.w  D5,     D5,   0xd8

    xvst       D4,     C0,    0x00
    xvst       D5,     C0,    0x20

    addi.d     C0,     C0,    0x40
#endif

#if defined(TRMMKERNEL)

#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d      TL,     K,    OFF
#ifdef LEFT
    addi.d     TL,     TL,   -16
#else
    addi.d     TL,     TL,   -1
#endif
    slli.d     T3,     TL,   0x07
    add.d      A0,     A0,   T3
    slli.d     T3,     TL,   0x03
    add.d      B0,     B0,   T3
#endif

#ifdef LEFT
    addi.d     OFF,    OFF,   16
#endif

#endif   // #if defined(TRMMKERNEL)

    addi.d     I,      I,     1
    blt        I,      T0,    .L301

.L31:  /* if ( bm & 8 )*/
    move       I,      $r0
    andi       T1,     M,     8    //bm&8
    beq        I,      T1,    .L34

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

    xvxor.v    U0,     U0,   U0
    xvxor.v    U1,     U1,   U1

    move       L,      $r0   //cycle param k
    beq        L,      TL,    .L33
    blt        TL,     L,     .L33

.L32:  /* for (k=0; k<temp; k++) */
    xvld       D0,     A0,    0x00  //a0ri a1ri a2ri a3ri
    xvld       D1,     A0,    0x20  //a4ri a5ri a6ri a7ri

    xvldrepl.w D2,     B0,    0x00 //b0r
    xvldrepl.w D3,     B0,    0x04 //b0i

    xvpackev.w D4,     D1,    D0    //a0r a4r a1r a5r a2r a6r a3r a7r
    xvpermi.w  D4,     D4,    0xd8  //a0r a1r a4r a5r a2r a3r a6r a7r
    xvpermi.d  D4,     D4,    0xd8  //a0r a1r a2r a3r a4r a5r a6r a7r

    xvpackod.w D5,     D1,    D0    //a0i a4i a1i a5i a2i a6i a3i a7i
    xvpermi.w  D5,     D5,    0xd8  //a0i a1i a4i a5i a2i a3i a6i a7i
    xvpermi.d  D5,     D5,    0xd8  //a0i a1i a2i a3i a4i a5i a6i a7i

    XVMADD1    U0,     D4,    D2,     U0  //00r 01r 02r 03r 04r 05r 06r 07r
    XVMADD2    U1,     D5,    D2,     U1  //00i 01i 02i 03i 04i 05i 06i 07i
    XVMADD3    U0,     D5,    D3,     U0
    XVMADD4    U1,     D4,    D3,     U1

    addi.d     A0,     A0,    0x40
    addi.d     B0,     B0,    0x08

    addi.d     L,      L,     1
    blt        L,      TL,    .L32

.L33:
#if defined(TRMMKERNEL)
    //res00 res01 res02 res03 res04 res05 res06 res07
    xvld       D0,     C0,    0x00 //c0:0 1 2 3 4 5 6 7
    xvld       D1,     C0,    0x20 //c0:8 9 10 11 12 13 14 15

    xvpackev.w D2,     D1,    D0  //0 8 2 10 4 12 6 14
    xvpermi.w  D2,     D2,    0xd8  //0 2 8 10 4 6 12 14
    xvpermi.d  D2,     D2,    0xd8  //0 2 4 6 8 10 12 14

    xvpackod.w D3,     D1,    D0
    xvpermi.w  D3,     D3,    0xd8
    xvpermi.d  D3,     D3,    0xd8 //1 3 5 7 9 11 13 15

    xvfmul.s      D2,    U0,    VALPHAR
    xvfmul.s      D3,    U1,    VALPHAR
    XVNMSUB      D2,    U1,    VALPHAI, D2
    XVFMADD      D3,    U0,    VALPHAI, D3

    xvand.v    D4,     D2,   D2  //0 2 4 6 8 10 12 14
    xvpermi.q  D4,     D3,   0x02 //0 2 4 6 1 3 5 7
    xvpermi.d  D4,     D4,   0xd8 //0 2 1 3 4 6 5 7
    xvpermi.w  D4,     D4,   0xd8 //0 1 2 3 4 5 6 7

    xvand.v    D5,     D3,   D3
    xvpermi.q  D5,     D2,   0x31
    xvpermi.d  D5,     D5,   0xd8
    xvpermi.w  D5,     D5,   0xd8

    xvst       D4,     C0,    0x00
    xvst       D5,     C0,    0x20

    addi.d     C0,     C0,    0x40
#else
    //res00 res01 res02 res03 res04 res05 res06 res07
    xvld       D0,     C0,    0x00 //c0:0 1 2 3 4 5 6 7
    xvld       D1,     C0,    0x20 //c0:8 9 10 11 12 13 14 15

    xvpackev.w D2,     D1,    D0  //0 8 2 10 4 12 6 14
    xvpermi.w  D2,     D2,    0xd8  //0 2 8 10 4 6 12 14
    xvpermi.d  D2,     D2,    0xd8  //0 2 4 6 8 10 12 14

    xvpackod.w D3,     D1,    D0
    xvpermi.w  D3,     D3,    0xd8
    xvpermi.d  D3,     D3,    0xd8 //1 3 5 7 9 11 13 15

    XVFMADD      D2,    U0,    VALPHAR, D2
    XVFMADD      D3,    U1,    VALPHAR, D3
    XVNMSUB      D2,    U1,    VALPHAI, D2
    XVFMADD      D3,    U0,    VALPHAI, D3

    xvand.v    D4,     D2,   D2  //0 2 4 6 8 10 12 14
    xvpermi.q  D4,     D3,   0x02 //0 2 4 6 1 3 5 7
    xvpermi.d  D4,     D4,   0xd8 //0 2 1 3 4 6 5 7
    xvpermi.w  D4,     D4,   0xd8 //0 1 2 3 4 5 6 7

    xvand.v    D5,     D3,   D3
    xvpermi.q  D5,     D2,   0x31
    xvpermi.d  D5,     D5,   0xd8
    xvpermi.w  D5,     D5,   0xd8

    xvst       D4,     C0,    0x00
    xvst       D5,     C0,    0x20

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

    vxor.v    $vr2,     $vr2,   $vr2
    vxor.v    $vr3,     $vr3,   $vr3

    move       L,      $r0   //cycle param k
    beq        L,      TL,    .L37
    blt        TL,     L,     .L37

.L36:  /* for (k=0; k<temp; k++) */
    vld       $vr16,     A0,    0x00  // a0ri a1ri
    vld       $vr17,     A0,    0x10  // a2ri a3ri

    vldrepl.w $vr18,     B0,    0x00 //b0r
    vldrepl.w $vr19,     B0,    0x04 //b0i

    vpackev.w $vr20,     $vr17,    $vr16
    vshuf4i.w  $vr20,     $vr20,    0xd8  //a0r a1r a2r a3r

    vpackod.w $vr21,     $vr17,    $vr16
    vshuf4i.w  $vr21,     $vr21,    0xd8  //a0i a1i a2i a3i

    VMADD1    $vr2,     $vr20,    $vr18,     $vr2  //00r 01r 02r 03r
    VMADD2    $vr3,     $vr21,    $vr18,     $vr3  //00i 01i 02i 03i
    VMADD3    $vr2,     $vr21,    $vr19,     $vr2
    VMADD4    $vr3,     $vr20,    $vr19,     $vr3

    addi.d     A0,     A0,    0x20
    addi.d     B0,     B0,    0x08

    addi.d     L,      L,     1
    blt        L,      TL,    .L36

.L37:
#if defined(TRMMKERNEL)
    //res00 res01 res02 res03
    vld       $vr16,     C0,    0x00 //c0: 0 1 2 3
    vld       $vr17,     C0,    0x10 //c0: 4 5 6 7

    vpackev.w $vr18,     $vr17,    $vr16
    vshuf4i.w  $vr18,     $vr18,    0xd8  //0 2 4 6
    vpackod.w $vr19,     $vr17,    $vr16
    vshuf4i.w  $vr19,     $vr19,    0xd8  //1 3 5 7

    vfmul.s      $vr18,    $vr2,    $vr28
    vfmul.s      $vr19,    $vr3,    $vr28
    VNMSUB      $vr18,    $vr3,    $vr29, $vr18
    VFMADD      $vr19,    $vr2,    $vr29, $vr19

    vand.v    $vr20,     $vr19,   $vr19  //1 3 5 7
    vpermi.w  $vr20,     $vr18,   0x44 //0 2 1 3
    vshuf4i.w  $vr20,     $vr20,   0xd8 //0 1 2 3

    vand.v    $vr21,     $vr19,   $vr19  //1 3 5 7
    vpermi.w  $vr21,     $vr18,   0xee //4 6 5 7
    vshuf4i.w  $vr21,     $vr21,   0xd8 //4 5 6 7

    vst       $vr20,     C0,    0x00
    vst       $vr21,     C0,    0x10

    addi.d     C0,     C0,    0x20
#else
    //res00 res01 res02 res03
    vld       $vr16,     C0,    0x00 //c0: 0 1 2 3
    vld       $vr17,     C0,    0x10 //c0: 4 5 6 7

    vpackev.w $vr18,     $vr17,    $vr16
    vshuf4i.w  $vr18,     $vr18,    0xd8  //0 2 4 6
    vpackod.w $vr19,     $vr17,    $vr16
    vshuf4i.w  $vr19,     $vr19,    0xd8  //1 3 5 7

    VFMADD      $vr18,    $vr2,    $vr28, $vr18
    VFMADD      $vr19,    $vr3,    $vr28, $vr19
    VNMSUB      $vr18,    $vr3,    $vr29, $vr18
    VFMADD      $vr19,    $vr2,    $vr29, $vr19

    vand.v    $vr20,     $vr19,   $vr19  //1 3 5 7
    vpermi.w  $vr20,     $vr18,   0x44 //0 2 1 3
    vshuf4i.w  $vr20,     $vr20,   0xd8 //0 1 2 3

    vand.v    $vr21,     $vr19,   $vr19  //1 3 5 7
    vpermi.w  $vr21,     $vr18,   0xee //4 6 5 7
    vshuf4i.w  $vr21,     $vr21,   0xd8 //4 5 6 7

    vst       $vr20,     C0,    0x00
    vst       $vr21,     C0,    0x10

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

    MADD1      c21,    a3,    b1,     c21  //res01r
    MADD2      c22,    a4,    b1,     c22  //res01i
    MADD3      c21,    a4,    b2,     c21
    MADD4      c22,    a3,    b2,     c22

    addi.d     A0,     A0,    0x10
    addi.d     B0,     B0,    0x08

    addi.d     L,      L,     1
    blt        L,      TL,    .L310

.L311:
#if defined(TRMMKERNEL)
    //res00 res01
    LD         a5,     C0,    0x00    //C0[0]
    LD         a6,     C0,    0x04    //C0[1]

    MUL       a5,     c11,   ALPHA_R
    MUL       a6,     c12,   ALPHA_R
    NMSUB      a5,     c12,   ALPHA_I, a5
    MADD       a6,     c11,   ALPHA_I, a6

    ST         a5,     C0,    0x00
    ST         a6,     C0,    0x04

    LD         a5,     C0,    0x08    //C0[2]
    LD         a6,     C0,    0x0c    //C0[3]

    MUL       a5,     c21,   ALPHA_R
    MUL       a6,     c22,   ALPHA_R
    NMSUB      a5,     c22,   ALPHA_I, a5
    MADD       a6,     c21,   ALPHA_I, a6

    ST         a5,     C0,    0x08
    ST         a6,     C0,    0x0c

    addi.d     C0,     C0,    0x10
#else
    //res00 res01
    LD         a5,     C0,    0x00    //C0[0]
    LD         a6,     C0,    0x04    //C0[1]

    MADD       a5,     c11,   ALPHA_R, a5
    MADD       a6,     c12,   ALPHA_R, a6
    NMSUB      a5,     c12,   ALPHA_I, a5
    MADD       a6,     c11,   ALPHA_I, a6

    ST         a5,     C0,    0x00
    ST         a6,     C0,    0x04

    LD         a5,     C0,    0x08    //C0[2]
    LD         a6,     C0,    0x0c    //C0[3]

    MADD       a5,     c21,   ALPHA_R, a5
    MADD       a6,     c22,   ALPHA_R, a6
    NMSUB      a5,     c22,   ALPHA_I, a5
    MADD       a6,     c21,   ALPHA_I, a6

    ST         a5,     C0,    0x08
    ST         a6,     C0,    0x0c

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
    LD         a5,     C0,    0x00    //C0[0]
    LD         a6,     C0,    0x04    //C0[1]

    MUL       a5,     c11,   ALPHA_R
    MUL       a6,     c12,   ALPHA_R
    NMSUB      a5,     c12,   ALPHA_I, a5
    MADD       a6,     c11,   ALPHA_I, a6

    ST         a5,     C0,    0x00
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
