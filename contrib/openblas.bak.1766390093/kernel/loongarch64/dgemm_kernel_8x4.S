/*******************************************************************************
Copyright (c) 2021, The OpenBLAS Project
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
#define T0     $r25 /* !! DO NOT USE $r21 and $r22 !! */
#define T1     $r26
#define T2     $r27
#define ZERO   $r0

/* LSX vectors */
#define U0     $vr0
#define U1     $vr1
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
#define D14    $vr30
#define D15    $vr31
#define VALPHA $vr15

/* Prefetch interval */
#define A_PRE  0x200
#define B_PRE  0x100

.macro KERNEL2x8x4
    vld     U0,   A0,    0x00
    vfmadd.d  D0,  U8, U12, D0
    vfmadd.d  D1,  U9, U12, D1

    vld     U1,   A0,    0x10
    vfmadd.d  D2,  U10, U12, D2
    vfmadd.d  D3,  U11, U12, D3

    vld     U2,   A0,    0x20
    vfmadd.d  D4,  U8, U13, D4
    vfmadd.d  D5,  U9, U13, D5

    vld     U3,   A0,    0x30
    vfmadd.d  D6,  U10, U13, D6
    vfmadd.d  D7,  U11, U13, D7

    vldrepl.d U4,  B0, 0x00
    vfmadd.d  D8,  U8, U14, D8
    vfmadd.d  D9,  U9, U14, D9

    preld     0,   B0, B_PRE
    vldrepl.d U5,  B0, 0x08
    vfmadd.d  D10, U10, U14, D10
    vfmadd.d  D11, U11, U14, D11

    preld     0,   A0, A_PRE
    vldrepl.d U6,  B0, 0x10
    vfmadd.d  D12, U8, U15, D12
    vfmadd.d  D13, U9, U15, D13

    preld     0,   A0, A_PRE + 0x40
    vldrepl.d U7,  B0, 0x18
    vfmadd.d  D14, U10, U15, D14
    vfmadd.d  D15, U11, U15, D15

    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x20

    vld     U8,   A0,    0x00
    vfmadd.d  D0,  U0, U4, D0
    vfmadd.d  D1,  U1, U4, D1

    vld     U9,   A0,    0x10
    vfmadd.d  D2,  U2, U4, D2
    vfmadd.d  D3,  U3, U4, D3

    vld     U10,  A0,    0x20
    vfmadd.d  D4,  U0, U5, D4
    vfmadd.d  D5,  U1, U5, D5

    vld     U11,  A0,    0x30
    vfmadd.d  D6,  U2, U5, D6
    vfmadd.d  D7,  U3, U5, D7

    vldrepl.d U12,  B0, 0x00
    vfmadd.d  D8,  U0, U6, D8
    vfmadd.d  D9,  U1, U6, D9

    preld     0,   B0, B_PRE
    vldrepl.d U13,  B0, 0x08
    vfmadd.d  D10, U2, U6, D10
    vfmadd.d  D11, U3, U6, D11

    preld     0,   A0, A_PRE
    vldrepl.d U14,  B0, 0x10
    vfmadd.d  D12, U0, U7, D12
    vfmadd.d  D13, U1, U7, D13

    preld     0,   A0, A_PRE + 0x40
    vldrepl.d U15,  B0, 0x18
    vfmadd.d  D14, U2, U7, D14
    vfmadd.d  D15, U3, U7, D15

    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x20
.endm

.macro KERNEL2x8x4_END
    vld     U0,   A0,    0x00
    vfmadd.d  D0,  U8, U12, D0
    vfmadd.d  D1,  U9, U12, D1

    vld     U1,   A0,    0x10
    vfmadd.d  D2,  U10, U12, D2
    vfmadd.d  D3,  U11, U12, D3

    vld     U2,   A0,    0x20
    vfmadd.d  D4,  U8, U13, D4
    vfmadd.d  D5,  U9, U13, D5

    vld     U3,   A0,    0x30
    vfmadd.d  D6,  U10, U13, D6
    vfmadd.d  D7,  U11, U13, D7

    vldrepl.d U4,  B0, 0x00
    vfmadd.d  D8,  U8, U14, D8
    vfmadd.d  D9,  U9, U14, D9

    preld     0,   B0, B_PRE
    vldrepl.d U5,  B0, 0x08
    vfmadd.d  D10, U10, U14, D10
    vfmadd.d  D11, U11, U14, D11

    preld     0,   A0, A_PRE
    vldrepl.d U6,  B0, 0x10
    vfmadd.d  D12, U8, U15, D12
    vfmadd.d  D13, U9, U15, D13

    preld     0,   A0, A_PRE + 0x40
    vldrepl.d U7,  B0, 0x18
    vfmadd.d  D14, U10, U15, D14
    vfmadd.d  D15, U11, U15, D15

    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x20

    vfmadd.d  D0,  U0, U4, D0
    vfmadd.d  D1,  U1, U4, D1

    vfmadd.d  D2,  U2, U4, D2
    vfmadd.d  D3,  U3, U4, D3

    vfmadd.d  D4,  U0, U5, D4
    vfmadd.d  D5,  U1, U5, D5

    vfmadd.d  D6,  U2, U5, D6
    vfmadd.d  D7,  U3, U5, D7

    vfmadd.d  D8,  U0, U6, D8
    vfmadd.d  D9,  U1, U6, D9

    preld     0,   B0, B_PRE
    vfmadd.d  D10, U2, U6, D10
    vfmadd.d  D11, U3, U6, D11

    preld     0,   A0, A_PRE
    vfmadd.d  D12, U0, U7, D12
    vfmadd.d  D13, U1, U7, D13

    preld     0,   A0, A_PRE + 0x40
    vfmadd.d  D14, U2, U7, D14
    vfmadd.d  D15, U3, U7, D15
.endm

.macro KERNEL8x8x4
.rept 4
    KERNEL2x8x4
.endr
.endm

.macro KERNEL8x8x4_END
.rept 3
    KERNEL2x8x4
.endr
    KERNEL2x8x4_END
.endm

.macro KERNEL2x4x4
    vld     U0,   A0,    0x00
    vld     U1,   A0,    0x10

    vldrepl.d U4,  B0, 0x00
    vfmadd.d  D0,  U8, U12, D0
    vfmadd.d  D1,  U9, U12, D1

    vldrepl.d U5,  B0, 0x08
    vfmadd.d  D4,  U8, U13, D4
    vfmadd.d  D5,  U9, U13, D5

    vldrepl.d U6,  B0, 0x10
    vfmadd.d  D8,  U8, U14, D8
    vfmadd.d  D9,  U9, U14, D9

    vldrepl.d U7,  B0, 0x18
    vfmadd.d  D12, U8, U15, D12
    vfmadd.d  D13, U9, U15, D13

    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x20

    vld     U8,   A0,    0x00
    vld     U9,   A0,    0x10

    vldrepl.d U12, B0, 0x00
    vfmadd.d  D0,  U0, U4, D0
    vfmadd.d  D1,  U1, U4, D1

    vldrepl.d U13, B0, 0x08
    vfmadd.d  D4,  U0, U5, D4
    vfmadd.d  D5,  U1, U5, D5

    vldrepl.d U14, B0, 0x10
    vfmadd.d  D8,  U0, U6, D8
    vfmadd.d  D9,  U1, U6, D9

    vldrepl.d U15,  B0, 0x18
    vfmadd.d  D12, U0, U7, D12
    vfmadd.d  D13, U1, U7, D13

    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x20
.endm

.macro KERNEL2x4x4_END
    vld     U0,   A0,    0x00
    vld     U1,   A0,    0x10

    vldrepl.d U4,  B0, 0x00
    vfmadd.d  D0,  U8, U12, D0
    vfmadd.d  D1,  U9, U12, D1

    vldrepl.d U5,  B0, 0x08
    vfmadd.d  D4,  U8, U13, D4
    vfmadd.d  D5,  U9, U13, D5

    vldrepl.d U6,  B0, 0x10
    vfmadd.d  D8,  U8, U14, D8
    vfmadd.d  D9,  U9, U14, D9

    vldrepl.d U7,  B0, 0x18
    vfmadd.d  D12, U8, U15, D12
    vfmadd.d  D13, U9, U15, D13

    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x20

    vfmadd.d  D0,  U0, U4, D0
    vfmadd.d  D1,  U1, U4, D1
    vfmadd.d  D4,  U0, U5, D4
    vfmadd.d  D5,  U1, U5, D5
    vfmadd.d  D8,  U0, U6, D8
    vfmadd.d  D9,  U1, U6, D9
    vfmadd.d  D12, U0, U7, D12
    vfmadd.d  D13, U1, U7, D13
.endm

.macro KERNEL8x4x4
.rept 4
    KERNEL2x4x4
.endr
.endm

.macro KERNEL8x4x4_END
.rept 3
    KERNEL2x4x4
.endr
    KERNEL2x4x4_END
.endm

.macro KERNEL2x2x4
    vldrepl.d     U0,   A0,    0x00
    vldrepl.d     U1,   A0,    0x08

    vfmadd.d   D0,     U8,     U12,    D0
    vfmadd.d   D1,     U8,     U13,    D1
    vfmadd.d   D2,     U9,     U12,    D2
    vfmadd.d   D3,     U9,     U13,    D3

    vld    U4,  B0, 0x00
    vld    U5,  B0, 0x10
    addi.d  A0,  A0, 0x10
    addi.d  B0,  B0, 0x20

    vldrepl.d     U8,   A0,    0x00
    vldrepl.d     U9,   A0,    0x08

    vfmadd.d   D0,     U0,     U4,    D0
    vfmadd.d   D1,     U0,     U5,    D1
    vfmadd.d   D2,     U1,     U4,    D2
    vfmadd.d   D3,     U1,     U5,    D3

    vld       U12, B0, 0x00
    vld       U13, B0, 0x10
    addi.d    A0,  A0, 0x10
    addi.d    B0,  B0, 0x20
.endm

.macro KERNEL2x2x4_END
    vldrepl.d     U0,   A0,    0x00
    vldrepl.d     U1,   A0,    0x08

    vfmadd.d   D0,     U8,     U12,    D0
    vfmadd.d   D1,     U8,     U13,    D1
    vfmadd.d   D2,     U9,     U12,    D2
    vfmadd.d   D3,     U9,     U13,    D3

    vld     U4,  B0, 0x00
    vld     U5,  B0, 0x10
    addi.d  A0,  A0, 0x10
    addi.d  B0,  B0, 0x20

    vfmadd.d   D0,     U0,     U4,    D0
    vfmadd.d   D1,     U0,     U5,    D1
    vfmadd.d   D2,     U1,     U4,    D2
    vfmadd.d   D3,     U1,     U5,    D3
.endm

.macro KERNEL8x2x4
.rept 4
    KERNEL2x2x4
.endr
.endm

.macro KERNEL8x2x4_END
.rept 3
    KERNEL2x2x4
.endr
    KERNEL2x2x4_END
.endm

.macro KERNEL2x1x4
    vldrepl.d  U0,     A0,     0x00
    vfmadd.d   D0,     U8,     U12,    D0
    vfmadd.d   D1,     U8,     U13,    D1
    vld        U4,     B0,     0x00
    vld        U5,     B0,     0x10

    vldrepl.d  U8,     A0,     0x08
    vfmadd.d   D0,     U0,     U4,     D0
    vfmadd.d   D1,     U0,     U5,     D1
    vld        U12,    B0,     0x20
    vld        U13,    B0,     0x30

    addi.d     A0,  A0, 0x10
    addi.d     B0,  B0, 0x40
.endm

.macro KERNEL2x1x4_END
    vldrepl.d  U0,     A0,     0x00
    vfmadd.d   D0,     U8,     U12,    D0
    vfmadd.d   D1,     U8,     U13,    D1
    vld        U4,     B0,     0x00
    vld        U5,     B0,     0x10

    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x20

    vfmadd.d   D0,     U0,     U4,     D0
    vfmadd.d   D1,     U0,     U5,     D1
.endm

.macro KERNEL8x1x4
.rept 4
    KERNEL2x1x4
.endr
.endm

.macro KERNEL8x1x4_END
.rept 3
    KERNEL2x1x4
.endr
    KERNEL2x1x4_END
.endm

.macro KERNEL2x8x2
    vld     U0,   A0,    0x00
    vfmadd.d  D0,  U8, U12, D0
    vfmadd.d  D1,  U9, U12, D1

    vld     U1,   A0,    0x10
    vfmadd.d  D2,  U10, U12, D2
    vfmadd.d  D3,  U11, U12, D3

    vld     U2,   A0,    0x20
    vfmadd.d  D4,  U8, U13, D4
    vfmadd.d  D5,  U9, U13, D5

    vld     U3,   A0,    0x30
    vfmadd.d  D6,  U10, U13, D6
    vfmadd.d  D7,  U11, U13, D7

    vldrepl.d U4,  B0, 0x00
    vldrepl.d U5,  B0, 0x08

    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x10

    vld     U8,   A0,    0x00
    vfmadd.d  D0,  U0, U4, D0
    vfmadd.d  D1,  U1, U4, D1

    vld     U9,   A0,    0x10
    vfmadd.d  D2,  U2, U4, D2
    vfmadd.d  D3,  U3, U4, D3

    vld     U10,   A0,   0x20
    vfmadd.d  D4,  U0, U5, D4
    vfmadd.d  D5,  U1, U5, D5

    vld     U11,   A0,   0x30
    vfmadd.d  D6,  U2, U5, D6
    vfmadd.d  D7,  U3, U5, D7

    vldrepl.d U12,  B0, 0x00
    vldrepl.d U13,  B0, 0x08

    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x10
.endm

.macro KERNEL2x8x2_END
    vld     U0,   A0,    0x00
    vfmadd.d  D0,  U8, U12, D0
    vfmadd.d  D1,  U9, U12, D1

    vld     U1,   A0,    0x10
    vfmadd.d  D2,  U10, U12, D2
    vfmadd.d  D3,  U11, U12, D3

    vld     U2,   A0,    0x20
    vfmadd.d  D4,  U8, U13, D4
    vfmadd.d  D5,  U9, U13, D5

    vld     U3,   A0,    0x30
    vfmadd.d  D6,  U10, U13, D6
    vfmadd.d  D7,  U11, U13, D7

    vldrepl.d U4,  B0, 0x00
    vldrepl.d U5,  B0, 0x08

    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x10

    vfmadd.d  D0,  U0, U4, D0
    vfmadd.d  D1,  U1, U4, D1

    vfmadd.d  D2,  U2, U4, D2
    vfmadd.d  D3,  U3, U4, D3

    vfmadd.d  D4,  U0, U5, D4
    vfmadd.d  D5,  U1, U5, D5

    vfmadd.d  D6,  U2, U5, D6
    vfmadd.d  D7,  U3, U5, D7
.endm

.macro KERNEL8x8x2
.rept 4
    KERNEL2x8x2
.endr
.endm

.macro KERNEL8x8x2_END
.rept 3
    KERNEL2x8x2
.endr
    KERNEL2x8x2_END
.endm

.macro KERNEL2x4x2
    vld     U0,   A0,    0x00
    vld     U1,   A0,    0x10
    vfmadd.d  D0,  U8, U12, D0
    vfmadd.d  D1,  U9, U12, D1
    vfmadd.d  D4,  U8, U13, D4
    vfmadd.d  D5,  U9, U13, D5

    vldrepl.d U4,  B0, 0x00
    vldrepl.d U5,  B0, 0x08

    vld     U8,   A0,    0x20
    vld     U9,   A0,    0x30
    vfmadd.d  D0,  U0, U4, D0
    vfmadd.d  D1,  U1, U4, D1
    vfmadd.d  D4,  U0, U5, D4
    vfmadd.d  D5,  U1, U5, D5

    vldrepl.d U12,  B0, 0x10
    vldrepl.d U13,  B0, 0x18

    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x20
.endm

.macro KERNEL2x4x2_END
    vld     U0,   A0,    0x00
    vld     U1,   A0,    0x10
    vfmadd.d  D0,  U8, U12, D0
    vfmadd.d  D1,  U9, U12, D1
    vfmadd.d  D4,  U8, U13, D4
    vfmadd.d  D5,  U9, U13, D5

    vldrepl.d U4,  B0, 0x00
    vldrepl.d U5,  B0, 0x08

    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x10

    vfmadd.d  D0,  U0, U4, D0
    vfmadd.d  D1,  U1, U4, D1
    vfmadd.d  D4,  U0, U5, D4
    vfmadd.d  D5,  U1, U5, D5
.endm

.macro KERNEL8x4x2
.rept 4
    KERNEL2x4x2
.endr
.endm

.macro KERNEL8x4x2_END
.rept 3
    KERNEL2x4x2
.endr
    KERNEL2x4x2_END
.endm

.macro KERNEL2x2x2
    vld     U0,   A0,    0x00
    vfmadd.d  D0,  U8, U12, D0
    vfmadd.d  D4,  U8, U13, D4

    vldrepl.d U4,  B0, 0x00
    vldrepl.d U5,  B0, 0x08

    vld     U8,   A0,    0x10
    vldrepl.d U12,  B0, 0x10
    vldrepl.d U13,  B0, 0x18

    vfmadd.d  D0,  U0, U4, D0
    vfmadd.d  D4,  U0, U5, D4

    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x20
.endm

.macro KERNEL2x2x2_END
    vld     U0,   A0,    0x00
    vfmadd.d  D0,  U8, U12, D0
    vfmadd.d  D4,  U8, U13, D4

    vldrepl.d U4,  B0, 0x00
    vldrepl.d U5,  B0, 0x08

    addi.d     A0,  A0, 0x10
    addi.d     B0,  B0, 0x10

    vfmadd.d  D0,  U0, U4, D0
    vfmadd.d  D4,  U0, U5, D4
.endm

.macro KERNEL8x2x2
.rept 4
    KERNEL2x2x2
.endr
.endm

.macro KERNEL8x2x2_END
.rept 3
    KERNEL2x2x2
.endr
    KERNEL2x2x2_END
.endm

.macro KERNEL2x1x2
    vldrepl.d U0,  A0, 0x00
    vfmadd.d  D0,  U8, U12, D0

    vld       U4,  B0, 0x00
    vldrepl.d U8,  A0, 0x08
    vld       U12, B0, 0x10
    vfmadd.d  D0,  U0, U4, D0

    addi.d    A0,  A0, 0x10
    addi.d    B0,  B0, 0x20
.endm

.macro KERNEL2x1x2_END
    vldrepl.d U0,  A0, 0x00
    vfmadd.d  D0,  U8, U12, D0

    vld       U4,  B0, 0x00
    addi.d    A0,  A0, 0x08
    addi.d    B0,  B0, 0x10

    vfmadd.d  D0,  U0, U4, D0
.endm

.macro KERNEL8x1x2
.rept 4
    KERNEL2x1x2
.endr
.endm

.macro KERNEL8x1x2_END
.rept 3
    KERNEL2x1x2
.endr
    KERNEL2x1x2_END
.endm

.macro KERNEL2x8x1
    vld     U0,   A0,    0x00
    vfmadd.d  D0,  U8, U12, D0
    vfmadd.d  D1,  U9, U12, D1

    vld     U1,   A0,    0x10
    vfmadd.d  D2,  U10, U12, D2
    vfmadd.d  D3,  U11, U12, D3

    vldrepl.d U4,  B0, 0x00
    vld     U2,   A0,    0x20
    vld     U3,   A0,    0x30

    vld     U8,   A0,    0x40

    vfmadd.d  D0,  U0, U4, D0
    vfmadd.d  D1,  U1, U4, D1

    vld     U9,   A0,    0x50
    vfmadd.d  D2,  U2, U4, D2
    vfmadd.d  D3,  U3, U4, D3

    vld     U10,   A0,    0x60
    vld     U11,   A0,    0x70

    vldrepl.d U12,  B0, 0x08

    addi.d     A0,  A0, 0x80
    addi.d     B0,  B0, 0x10
.endm

.macro KERNEL2x8x1_END
    vld     U0,   A0,    0x00
    vfmadd.d  D0,  U8, U12, D0
    vfmadd.d  D1,  U9, U12, D1

    vld     U1,   A0,    0x10
    vfmadd.d  D2,  U10, U12, D2
    vfmadd.d  D3,  U11, U12, D3

    vld     U2,   A0,    0x20
    vld     U3,   A0,    0x30

    vldrepl.d U4,  B0, 0x00

    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x08

    vfmadd.d  D0,  U0, U4, D0
    vfmadd.d  D1,  U1, U4, D1

    vfmadd.d  D2,  U2, U4, D2
    vfmadd.d  D3,  U3, U4, D3
.endm

.macro KERNEL8x8x1
.rept 4
    KERNEL2x8x1
.endr
.endm

.macro KERNEL8x8x1_END
.rept 3
    KERNEL2x8x1
.endr
    KERNEL2x8x1_END
.endm

.macro KERNEL2x4x1
    vld     U0,   A0,    0x00
    vld     U1,   A0,    0x10
    vfmadd.d  D0,  U8, U12, D0
    vfmadd.d  D1,  U9, U12, D1
    vldrepl.d U4,  B0, 0x00

    vld     U8,   A0,    0x20
    vld     U9,   A0,    0x30
    vfmadd.d  D0,  U0, U4, D0
    vfmadd.d  D1,  U1, U4, D1
    vldrepl.d U12,  B0, 0x08

    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x10
.endm

.macro KERNEL2x4x1_END
    vld     U0,   A0,    0x00
    vld     U1,   A0,    0x10
    vfmadd.d  D0,  U8, U12, D0
    vfmadd.d  D1,  U9, U12, D1
    vldrepl.d U4,  B0, 0x00

    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x08

    vfmadd.d  D0,  U0, U4, D0
    vfmadd.d  D1,  U1, U4, D1
.endm

.macro KERNEL8x4x1
.rept 4
    KERNEL2x4x1
.endr
.endm

.macro KERNEL8x4x1_END
.rept 3
    KERNEL2x4x1
.endr
    KERNEL2x4x1_END
.endm

.macro KERNEL2x2x1
    vld     U0,   A0,    0x00
    vfmadd.d  D0,  U8, U12, D0
    vldrepl.d U4,  B0, 0x00

    addi.d     A0,  A0, 0x10
    addi.d     B0,  B0, 0x08

    vld     U8,   A0,    0x00
    vfmadd.d  D0,  U0, U4, D0
    vldrepl.d U12,  B0, 0x00

    addi.d     A0,  A0, 0x10
    addi.d     B0,  B0, 0x08
.endm

.macro KERNEL2x2x1_END
    vld     U0,   A0,    0x00
    vfmadd.d  D0,  U8, U12, D0
    vldrepl.d U4,  B0, 0x00

    addi.d     A0,  A0, 0x10
    addi.d     B0,  B0, 0x08

    vfmadd.d  D0,  U0, U4, D0
.endm

.macro KERNEL8x2x1
.rept 4
    KERNEL2x2x1
.endr
.endm

.macro KERNEL8x2x1_END
.rept 3
    KERNEL2x2x1
.endr
    KERNEL2x2x1_END
.endm

.macro KERNEL2x1x1
    vldrepl.d  U0,  A0,    0x00
    vfmadd.d   D0,  U8, U12, D0
    vldrepl.d  U4,  B0, 0x00

    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x08

    vldrepl.d  U8,  A0,    0x00
    vfmadd.d   D0,  U0, U4, D0
    vldrepl.d  U12, B0, 0x00

    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x08
.endm

.macro KERNEL2x1x1_END
    vldrepl.d  U0,  A0,    0x00
    vfmadd.d   D0,  U8, U12, D0
    vldrepl.d  U4,  B0, 0x00

    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x08

    vfmadd.d  D0,  U0, U4, D0
.endm

.macro KERNEL8x1x1
.rept 4
    KERNEL2x1x1
.endr
.endm

.macro KERNEL8x1x1_END
.rept 3
    KERNEL2x1x1
.endr
    KERNEL2x1x1_END
.endm


    PROLOGUE

    addi.d   $sp,   $sp,   -112
    /* Store regs */
    SDARG    $r23,  $sp,   0
    SDARG    $r24,  $sp,   8
    SDARG    $r25,  $sp,   16
    SDARG    $r26,  $sp,   24
    SDARG    $r27,  $sp,   32
    ST       $f24,  $sp,   40
    ST       $f25,  $sp,   48
    ST       $f26,  $sp,   56
    ST       $f27,  $sp,   64
    ST       $f28,  $sp,   72
    ST       $f29,  $sp,   80
    ST       $f30,  $sp,   88
    ST       $f31,  $sp,   96
    ST       ALPHA, $sp,   104

#if defined (TRMMKERNEL) && !defined(LEFT)
    sub.d   OFF,   ZERO,  OFFSET
#else
    xor     OFF,   OFF,   OFF
#endif

    /* if (!(N >> 2)) goto L_N3 */
    srai.d   J,     N,     2     /* J = bn >> 2 */
    andi     N,     N,     0x03
    vldrepl.d  VALPHA, $sp, 104 /* When N < 4, VALPHA will not changed */
    beq      ZERO,  J,     .L_N3

.L_J1: /* J-- && This loop include Condition 1 */

/************************* Condition 1 if((N >> 2) && (M >> 3)) START !!! *************************
*                                                   dgemm_core_16x4                                */
    move     C0,    C
    move     A0,    A
    slli.d   T0,    LDC,   3
    add.d    C1,    C0,    T0
    addi.d   J,     J,     -1   /* J-- */
    add.d    C2,    C1,    T0
    add.d    C3,    C2,    T0

#if defined(TRMMKERNEL) && defined(LEFT)
    move     OFF,   OFFSET
#endif

    /* if (!(M >> 3)) goto L_M8 */
    srai.d   I,     M,     3     /* I = bm >> 3 */
    beq      ZERO,  I,     .L_M8

.L_I1: /* I-- */
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
#endif
    /* Calculate the first set of D0~D15,
     * avoidig set 0 operation
     * Load 8 * 64 from A0
     * U0 = {a1,  a0}
     * U1 = {a3,  a2}
     * U2 = {a5,  a4}
     * U3 = {a7,  a6}
     */
    vld     U0,   A0,    0x00
    vld     U1,   A0,    0x10
    vld     U2,   A0,    0x20
    vld     U3,   A0,    0x30

    vldrepl.d     U4, B0, 0x00
    preld         0,  C0, 0x00
    /* line 1 */
    vfmul.d  D0,  U0, U4
    vfmul.d  D1,  U1, U4
    preld    0,   C0, 0x20
    vfmul.d  D2,  U2, U4
    vfmul.d  D3,  U3, U4

    vldrepl.d     U5, B0, 0x08
    preld         0,  C1, 0x00
    /* line 2 */
    vfmul.d  D4,  U0, U5
    vfmul.d  D5,  U1, U5
    preld    0,   C1, 0x20
    vfmul.d  D6,  U2, U5
    vfmul.d  D7,  U3, U5

    vldrepl.d     U6, B0, 0x10
    preld         0,  C2, 0x00
    /* line 3 */
    vfmul.d  D8,  U0, U6
    vfmul.d  D9,  U1, U6
    preld    0,   C2, 0x20
    vfmul.d  D10, U2, U6
    vfmul.d  D11, U3, U6

    vldrepl.d     U7, B0, 0x18
    preld         0,  C3, 0x00
    /* line 4 */
    vfmul.d  D12, U0, U7
    vfmul.d  D13, U1, U7
    preld    0,   C3, 0x20
    vfmul.d  D14, U2, U7
    vfmul.d  D15, U3, U7

    /* Add stride for A0 and B0 */
    addi.d    A0,  A0, 0x40
    addi.d    B0,  B0, 0x20
    /* Reduce L */
    addi.d    L,   L,  -1
    srai.d    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_L7 */
    beq       ZERO,TL, .L_L7

    vld     U8,   A0,    0x00
    vld     U9,   A0,    0x10
    vld     U10,  A0,    0x20
    vld     U11,  A0,    0x30

    addi.d    TL,  TL,  -1

    vldrepl.d  U12,   B0,  0x00
    vldrepl.d  U13,   B0,  0x08
    vldrepl.d  U14,   B0,  0x10
    vldrepl.d  U15,   B0,  0x18
    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x20

    beq    ZERO,    TL,  .L_TL1_END
.L_TL1: /* TL-- */
    KERNEL8x8x4
    addi.d    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_TL1

.L_TL1_END:
    KERNEL8x8x4_END

   /* Maybe we need calculate the last
    * 7 sets of D0~D15?
    */
.L_L7:
    /* if (!(L & 7)) goto L_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_L0

.L_L71:
    /* Load 16 * 64 from A0 */
    vld     U0,   A0,    0x00
    vld     U1,   A0,    0x10
    vld     U2,   A0,    0x20
    vld     U3,   A0,    0x30

    /* Cumulative D0~D15 */
    vldrepl.d U4,  B0, 0x00
    vfmadd.d  D0,  U0, U4, D0
    vfmadd.d  D1,  U1, U4, D1
    vfmadd.d  D2,  U2, U4, D2
    vfmadd.d  D3,  U3, U4, D3

    vldrepl.d U5,  B0, 0x08
    vfmadd.d  D4,  U0, U5, D4
    vfmadd.d  D5,  U1, U5, D5
    vfmadd.d  D6,  U2, U5, D6
    vfmadd.d  D7,  U3, U5, D7

    vldrepl.d U6,  B0, 0x10
    vfmadd.d  D8,  U0, U6, D8
    vfmadd.d  D9,  U1, U6, D9
    vfmadd.d  D10, U2, U6, D10
    vfmadd.d  D11, U3, U6, D11

    vldrepl.d U7,  B0, 0x18
    vfmadd.d  D12, U0, U7, D12
    vfmadd.d  D13, U1, U7, D13
    vfmadd.d  D14, U2, U7, D14
    vfmadd.d  D15, U3, U7, D15

    /* Add stride for A0, B0 */
    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x20

    addi.d     TL,  TL, -1
    blt        ZERO,TL, .L_L71

.L_L0:
    vldrepl.d  VALPHA, $sp, 104
#if defined(TRMMKERNEL)
    vfmul.d  D0,   D0,  VALPHA
    vfmul.d  D1,   D1,  VALPHA
    vfmul.d  D2,   D2,  VALPHA
    vfmul.d  D3,   D3,  VALPHA
    vfmul.d  D4,   D4,  VALPHA
    vfmul.d  D5,   D5,  VALPHA
    vfmul.d  D6,   D6,  VALPHA
    vfmul.d  D7,   D7,  VALPHA
    vfmul.d  D8,   D8,  VALPHA
    vfmul.d  D9,   D9,  VALPHA
    vfmul.d  D10,  D10, VALPHA
    vfmul.d  D11,  D11, VALPHA
    vfmul.d  D12,  D12, VALPHA
    vfmul.d  D13,  D13, VALPHA
    vfmul.d  D14,  D14, VALPHA
    vfmul.d  D15,  D15, VALPHA
#else
    /* Load C0  */
    vld      U0,  C0,  0x00
    vld      U1,  C0,  0x10
    vld      U2,  C0,  0x20
    vld      U3,  C0,  0x30
    vfmadd.d D0,  D0,  VALPHA,  U0 /* D0 = U0 + (D0 * VALPHA) */
    vfmadd.d D1,  D1,  VALPHA,  U1
    vfmadd.d D2,  D2,  VALPHA,  U2
    vfmadd.d D3,  D3,  VALPHA,  U3

    /* Load C1  */
    vld      U4,  C1,  0x00
    vld      U5,  C1,  0x10
    vld      U6,  C1,  0x20
    vld      U7,  C1,  0x30
    vfmadd.d D4,  D4,  VALPHA,  U4
    vfmadd.d D5,  D5,  VALPHA,  U5
    vfmadd.d D6,  D6,  VALPHA,  U6
    vfmadd.d D7,  D7,  VALPHA,  U7

    /* Load C2  */
    vld      U8,  C2,  0x00
    vld      U9,  C2,  0x10
    vld      U10, C2,  0x20
    vld      U11, C2,  0x30
    vfmadd.d D8,  D8,  VALPHA,  U8
    vfmadd.d D9,  D9,  VALPHA,  U9
    vfmadd.d D10, D10, VALPHA,  U10
    vfmadd.d D11, D11, VALPHA,  U11

    /* Load C3  */
    vld      U0,  C3,  0x00
    vld      U1,  C3,  0x10
    vld      U2,  C3,  0x20
    vld      U3,  C3,  0x30
    vfmadd.d D12, D12, VALPHA,  U0
    vfmadd.d D13, D13, VALPHA,  U1
    vfmadd.d D14, D14, VALPHA,  U2
    vfmadd.d D15, D15, VALPHA,  U3
#endif // #if defined(TRMMKERNEL)

    /* Store C0 */
    vst      D0,  C0,  0x00
    vst      D1,  C0,  0x10
    vst      D2,  C0,  0x20
    vst      D3,  C0,  0x30
    /* Store C1 */
    vst      D4,  C1,  0x00
    vst      D5,  C1,  0x10
    vst      D6,  C1,  0x20
    vst      D7,  C1,  0x30
    /* Store C2 */
    vst      D8,  C2,  0x00
    vst      D9,  C2,  0x10
    vst      D10, C2,  0x20
    vst      D11, C2,  0x30
    /* Store C3 */
    vst      D12, C3,  0x00
    vst      D13, C3,  0x10
    vst      D14, C3,  0x20
    vst      D15, C3,  0x30

    /* Add stride for C */
    addi.d    C0,  C0,  0x40
    addi.d    C1,  C1,  0x40
    addi.d    C2,  C2,  0x40
    addi.d    C3,  C3,  0x40

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    sub.d     L,   K,   OFF
#ifdef LEFT
    /* number of values  in A */
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
    addi.d    OFF, OFF, 0x08
#endif
#endif   // #if defined(TRMMKERNEL)

    addi.d    I,   I,   -1  /* I-- */
    blt       ZERO,I,   .L_I1

.L_M8:
    /* We have done M & 16, considering M=8/4/2/1 */
    andi      I,   M,   7
    beq       ZERO,I,   .L_M0

    andi      I,   M,   4
    beq       ZERO,I,   .L_M2

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
    vld     U0,   A0,    0x00
    vld     U1,   A0,    0x10

    vldrepl.d     U4, B0, 0x00
    /* line 1 */
    vfmul.d  D0,  U0, U4
    vfmul.d  D1,  U1, U4

    vldrepl.d     U5, B0, 0x08
    /* line 2 */
    vfmul.d  D4,  U0, U5
    vfmul.d  D5,  U1, U5

    vldrepl.d     U6, B0, 0x10
    /* line 3 */
    vfmul.d  D8,  U0, U6
    vfmul.d  D9,  U1, U6

    vldrepl.d     U7, B0, 0x18
    /* line 4 */
    vfmul.d  D12, U0, U7
    vfmul.d  D13, U1, U7

    /* Add stride for A0 and B0 */
    addi.d    A0,  A0, 0x20
    addi.d    B0,  B0, 0x20
    /* Reduce L */
    addi.d    L,   L,  -1
    srai.d    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_M4_L7 */
    beq       ZERO,TL, .L_M4_L7

    vld     U8,   A0,    0x00
    vld     U9,   A0,    0x10

    addi.d    TL,  TL,  -1

    vldrepl.d  U12,   B0,  0x00
    vldrepl.d  U13,   B0,  0x08
    vldrepl.d  U14,   B0,  0x10
    vldrepl.d  U15,   B0,  0x18
    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x20

    beq    ZERO,    TL,  .L_M4_TL1_END

.L_M4_TL1: /* TL-- */
    KERNEL8x4x4

    addi.d     TL,  TL, -1
    blt        ZERO,TL, .L_M4_TL1

.L_M4_TL1_END:
    KERNEL8x4x4_END

.L_M4_L7:
    /* if (!(L & 7)) goto L_M4_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_M4_L0

.L_M4_L71:
    vld     U0,   A0,    0x00
    vld     U1,   A0,    0x10

    vldrepl.d U4,  B0, 0x00
    vfmadd.d  D0,  U0, U4, D0
    vfmadd.d  D1,  U1, U4, D1

    vldrepl.d U5,  B0, 0x08
    vfmadd.d  D4,  U0, U5, D4
    vfmadd.d  D5,  U1, U5, D5

    vldrepl.d U6,  B0, 0x10
    vfmadd.d  D8,  U0, U6, D8
    vfmadd.d  D9,  U1, U6, D9

    vldrepl.d U7,  B0, 0x18
    vfmadd.d  D12, U0, U7, D12
    vfmadd.d  D13, U1, U7, D13

    /* Add stride for A0, B0 */
    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x20

    addi.d     TL,  TL, -1
    blt        ZERO,TL, .L_M4_L71

.L_M4_L0:
    vldrepl.d  VALPHA, $sp, 104
#if defined(TRMMKERNEL)
    vfmul.d  D0,   D0,  VALPHA
    vfmul.d  D1,   D1,  VALPHA
    vfmul.d  D4,   D4,  VALPHA
    vfmul.d  D5,   D5,  VALPHA
    vfmul.d  D8,   D8,  VALPHA
    vfmul.d  D9,   D9,  VALPHA
    vfmul.d  D12,  D12, VALPHA
    vfmul.d  D13,  D13, VALPHA
#else
    /* Load C0  */
    vld      U0,  C0,  0x00
    vld      U1,  C0,  0x10
    vfmadd.d D0,  D0,  VALPHA,  U0 /* D0 = U0 + (D0 * VALPHA) */
    vfmadd.d D1,  D1,  VALPHA,  U1

    /* Load C1  */
    vld      U2,  C1,  0x00
    vld      U3,  C1,  0x10
    vfmadd.d D4,  D4,  VALPHA,  U2
    vfmadd.d D5,  D5,  VALPHA,  U3

    /* Load C2  */
    vld      U4,  C2,  0x00
    vld      U5,  C2,  0x10
    vfmadd.d D8,  D8,  VALPHA,  U4
    vfmadd.d D9,  D9,  VALPHA,  U5

    /* Load C3  */
    vld      U6,  C3,  0x00
    vld      U7,  C3,  0x10
    vfmadd.d D12, D12, VALPHA,  U6
    vfmadd.d D13, D13, VALPHA,  U7
#endif   // #if defined(TRMMKERNEL)

    /* Store C0 */
    vst      D0,  C0,  0x00
    vst      D1,  C0,  0x10
    /* Store C1 */
    vst      D4,  C1,  0x00
    vst      D5,  C1,  0x10
    /* Store C2 */
    vst      D8,  C2,  0x00
    vst      D9,  C2,  0x10
    /* Store C3 */
    vst      D12, C3,  0x00
    vst      D13, C3,  0x10

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

/********LOOP (if(N >> 2 ) && (M & 4) ) End************/

.L_M2:
    andi      I,   M,   2
    beq       ZERO,I,   .L_M1

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
    vldrepl.d     U0,   A0,    0x00
    vldrepl.d     U1,   A0,    0x08

    vld    U4,     B0,     0x00
    vld    U5,     B0,     0x10

    vfmul.d    D0,     U0,     U4
    vfmul.d    D1,     U0,     U5
    vfmul.d    D2,     U1,     U4
    vfmul.d    D3,     U1,     U5

    /* Add stride for A0 and B0 */
    addi.d    A0,  A0, 0x10
    addi.d    B0,  B0, 0x20
    /* Reduce L */
    addi.d    L,   L,  -1
    srai.d    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_M2_L7 */
    beq       ZERO,TL, .L_M2_L7

    vldrepl.d     U8,   A0,    0x00
    vldrepl.d     U9,   A0,    0x08

    addi.d    TL,  TL,  -1

    vld       U12, B0, 0x00
    vld       U13, B0, 0x10
    addi.d    A0,  A0, 0x10
    addi.d    B0,  B0, 0x20

    beq    ZERO,    TL,  .L_M2_TL1_END
.L_M2_TL1: /* TL-- */
    KERNEL8x2x4

    addi.d    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_M2_TL1
.L_M2_TL1_END:
    KERNEL8x2x4_END

.L_M2_L7:
    /* if (!(L & 7)) goto L_M2_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_M2_L0

.L_M2_L71:
    vldrepl.d     U0,   A0,    0x00
    vldrepl.d     U1,   A0,    0x08

    vld    U4,  B0, 0x00
    vld    U5,  B0, 0x10

    vfmadd.d   D0,     U0,     U4,    D0
    vfmadd.d   D1,     U0,     U5,    D1
    vfmadd.d   D2,     U1,     U4,    D2
    vfmadd.d   D3,     U1,     U5,    D3
    /* Add stride for A0, B0 */
    addi.d     A0,  A0, 0x10
    addi.d     B0,  B0, 0x20

    addi.d     TL,  TL, -1
    blt        ZERO,TL, .L_M2_L71

.L_M2_L0:
    vldrepl.d  VALPHA, $sp, 104
#if defined(TRMMKERNEL)
    vfmul.d  D0,   D0,  VALPHA
    vfmul.d  D1,   D1,  VALPHA
    vfmul.d  D2,   D2,  VALPHA
    vfmul.d  D3,   D3,  VALPHA

    vstelm.d  D0,  C0,  0x00,    0x00
    vstelm.d  D0,  C1,  0x00,    0x01
    vstelm.d  D1,  C2,  0x00,    0x00
    vstelm.d  D1,  C3,  0x00,    0x01
    vstelm.d  D2,  C0,  0x08,    0x00
    vstelm.d  D2,  C1,  0x08,    0x01
    vstelm.d  D3,  C2,  0x08,    0x00
    vstelm.d  D3,  C3,  0x08,    0x01
#else
    /* Load C0  */
    vld      U0,  C0,  0x00
    /* Load C1  */
    vld      U1,  C1,  0x00
    /* Load C2  */
    vld      U2,  C2,  0x00
    /* Load C3  */
    vld      U3,  C3,  0x00

    vilvl.d  D4,  D2,  D0   //C0
    vilvh.d  D5,  D2,  D0   //C1
    vilvl.d  D6,  D3,  D1   //C2
    vilvh.d  D7,  D3,  D1   //C3

    vfmadd.d D0,  D4,  VALPHA,  U0
    vfmadd.d D2,  D5,  VALPHA,  U1
    vfmadd.d D1,  D6,  VALPHA,  U2
    vfmadd.d D3,  D7,  VALPHA,  U3

    vst      D0,  C0,  0x00
    vst      D2,  C1,  0x00
    vst      D1,  C2,  0x00
    vst      D3,  C3,  0x00
#endif   // #if defined(TRMMKERNEL)

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

/********LOOP (if(N >> 2 ) && (M & 2) ) End************/

.L_M1:
    andi      I,   M,   1
    beq       ZERO,I,   .L_M0

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

    vldrepl.d  U0,     A0,     0x00
    vld        U4,     B0,     0x00
    vld        U5,     B0,     0x10
    vfmul.d    D0,     U0,     U4
    vfmul.d    D1,     U0,     U5

    /* Add stride for A0 and B0 */
    addi.d    A0,  A0, 0x08
    addi.d    B0,  B0, 0x20
    /* Reduce L */
    addi.d    L,   L,  -1
    srai.d    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_M1_L7 */
    beq       ZERO,TL, .L_M1_L7

    vldrepl.d  U8,     A0,     0x00

    addi.d     TL,  TL,  -1
    vld        U12, B0,  0x00
    vld        U13, B0,  0x10
    addi.d     A0,  A0,  0x08
    addi.d     B0,  B0,  0x20

    beq    ZERO,    TL,  .L_M1_TL1_END

.L_M1_TL1: /* TL-- */
    KERNEL8x1x4

    addi.d    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_M1_TL1
.L_M1_TL1_END:
    KERNEL8x1x4_END

.L_M1_L7:
    /* if (!(L & 7)) goto L_M1_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_M1_L0

.L_M1_L71:
    vldrepl.d  U0,     A0,     0x00
    vld        U4,     B0,     0x00
    vld        U5,     B0,     0x10
    vfmadd.d   D0,     U0,     U4,    D0
    vfmadd.d   D1,     U0,     U5,    D1

    /* Add stride for A0, B0 */
    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x20

    addi.d     TL,  TL, -1
    blt        ZERO,TL, .L_M1_L71

.L_M1_L0:
    vldrepl.d  VALPHA, $sp, 104
#if defined(TRMMKERNEL)
    vfmul.d  D0,   D0,  VALPHA
    vfmul.d  D1,   D1,  VALPHA

    vstelm.d D0,  C0,  0x00,    0x00
    vstelm.d D0,  C1,  0x00,    0x01
    vstelm.d D1,  C2,  0x00,    0x00
    vstelm.d D1,  C3,  0x00,    0x01
#else
    /* Load C0  */
    vldrepl.d     U0,  C0,  0x00
    vldrepl.d     U1,  C1,  0x00
    vilvl.d       D4,  U1,  U0
    vfmadd.d D6,  D0,  VALPHA,  D4

    vldrepl.d     U2,  C2,  0x00
    vldrepl.d     U3,  C3,  0x00
    vilvl.d       D5,  U3,  U2
    vfmadd.d D7,  D1,  VALPHA,  D5

    vstelm.d D6,  C0,  0x00,    0x00
    vstelm.d D6,  C1,  0x00,    0x01
    vstelm.d D7,  C2,  0x00,    0x00
    vstelm.d D7,  C3,  0x00,    0x01
#endif   // #if defined(TRMMKERNEL)

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

/********LOOP (if(N >> 2 ) && (M & 1) ) End************/

.L_M0:
    /* Add stride for B and C
     * B += (K * 32)
     * C += (LDC * 32)
     */
    /* since the array type is double,
     * so we must mul 32
     */
    slli.d    T0,   K,   5
    slli.d    T1,   LDC, 5
    add.d     B,    B,   T0
    add.d     C,    C,   T1

#if defined(TRMMKERNEL) && !defined(LEFT)
    addi.d    OFF,  OFF, 0x04
#endif

    blt      ZERO,  J,   .L_J1

//////////////// go back to L_J1 /////////////////
/////////////////////////////////////////////////
/************************ Condition 1 if((N >> 2) && (M >> 3)) END !!! ************************/

    vldrepl.d  VALPHA, $sp, 104

.L_N3:
    andi     J,    N,   2
    beq      ZERO, J,   .L_N1

/************************* Condition 2 if((N & 2) && (M >> 3)) START !!! *************************
*                                                   dgemm_core_16x2                                */

    move     C0,    C
    move     A0,    A
    slli.d   T0,    LDC,   3
    add.d    C1,    C0,    T0

#if defined(TRMMKERNEL) && defined(LEFT)
    move     OFF,   OFFSET
#endif

    /* if (!(M >> 3)) goto L_N3_M8 */
    srai.d   I,     M,     3     /* I = bm >> 3 */
    beq      ZERO,  I,     .L_N3_M8

.L_N3_I1:
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

    /* Load 8 * 64 from A0
     * U0 = {a1,  a0}
     * U1 = {a3,  a2}
     * U2 = {a5,  a4}
     * U3 = {a7,  a6}
     */
    vld     U0,   A0,    0x00
    vld     U1,   A0,    0x10
    vld     U2,   A0,    0x20
    vld     U3,   A0,    0x30

    vldrepl.d     U4, B0, 0x00
    /* line 1 */
    vfmul.d  D0,  U0, U4
    vfmul.d  D1,  U1, U4
    vfmul.d  D2,  U2, U4
    vfmul.d  D3,  U3, U4

    vldrepl.d     U5, B0, 0x08
    /* line 2 */
    vfmul.d  D4,  U0, U5
    vfmul.d  D5,  U1, U5
    vfmul.d  D6,  U2, U5
    vfmul.d  D7,  U3, U5

    /* Add stride for A0 and B0 */
    addi.d    A0,  A0, 0x40
    addi.d    B0,  B0, 0x10
    /* Reduce L */
    addi.d    L,   L,  -1
    srai.d    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_N3_L7 */
    beq       ZERO,TL, .L_N3_L7

    vld     U8,   A0,    0x00
    vld     U9,   A0,    0x10
    vld     U10,  A0,    0x20
    vld     U11,  A0,    0x30

    addi.d    TL,  TL,  -1

    vldrepl.d  U12,   B0,  0x00
    vldrepl.d  U13,   B0,  0x08
    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x10

    beq    ZERO,    TL,  .L_N3_TL1_END

.L_N3_TL1: /* TL-- */
    KERNEL8x8x2

    addi.d    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_N3_TL1
.L_N3_TL1_END:
    KERNEL8x8x2_END

.L_N3_L7:
    /* if (!(L & 7)) goto L_N3_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_N3_L0

.L_N3_L71:
    /* Load 16 * 64 from A0 */
    vld     U0,   A0,    0x00
    vld     U1,   A0,    0x10
    vld     U2,   A0,    0x20
    vld     U3,   A0,    0x30

    vldrepl.d U4,  B0, 0x00
    vfmadd.d  D0,  U0, U4, D0
    vfmadd.d  D1,  U1, U4, D1
    vfmadd.d  D2,  U2, U4, D2
    vfmadd.d  D3,  U3, U4, D3

    vldrepl.d U5,  B0, 0x08
    vfmadd.d  D4,  U0, U5, D4
    vfmadd.d  D5,  U1, U5, D5
    vfmadd.d  D6,  U2, U5, D6
    vfmadd.d  D7,  U3, U5, D7
    /* Add stride for A0, B0 */
    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x10

    addi.d     TL,  TL, -1
    blt        ZERO,TL, .L_N3_L71

.L_N3_L0:
#if defined(TRMMKERNEL)
    vfmul.d  D0,   D0,  VALPHA
    vfmul.d  D1,   D1,  VALPHA
    vfmul.d  D2,   D2,  VALPHA
    vfmul.d  D3,   D3,  VALPHA
    vfmul.d  D4,   D4,  VALPHA
    vfmul.d  D5,   D5,  VALPHA
    vfmul.d  D6,   D6,  VALPHA
    vfmul.d  D7,   D7,  VALPHA
#else
    /* Load C0  */
    vld      U0,  C0,  0x00
    vld      U1,  C0,  0x10
    vld      U2,  C0,  0x20
    vld      U3,  C0,  0x30
    vfmadd.d D0,  D0,  VALPHA,  U0 /* D0 = U0 + (D0 * VALPHA) */
    vfmadd.d D1,  D1,  VALPHA,  U1
    vfmadd.d D2,  D2,  VALPHA,  U2
    vfmadd.d D3,  D3,  VALPHA,  U3

    /* Load C1  */
    vld      U4,  C1,  0x00
    vld      U5,  C1,  0x10
    vld      U6,  C1,  0x20
    vld      U7,  C1,  0x30
    vfmadd.d D4,  D4,  VALPHA,  U4
    vfmadd.d D5,  D5,  VALPHA,  U5
    vfmadd.d D6,  D6,  VALPHA,  U6
    vfmadd.d D7,  D7,  VALPHA,  U7
#endif // #if defined(TRMMKERNEL)

    /* Store C0 */
    vst      D0,  C0,  0x00
    vst      D1,  C0,  0x10
    vst      D2,  C0,  0x20
    vst      D3,  C0,  0x30
    /* Store C1 */
    vst      D4,  C1,  0x00
    vst      D5,  C1,  0x10
    vst      D6,  C1,  0x20
    vst      D7,  C1,  0x30

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
    addi.d    OFF,   OFF,  0x8
#endif
#endif   // #if defined(TRMMKERNEL)

    addi.d    I,   I,   -1  /* I-- */
    blt       ZERO,I,   .L_N3_I1

.L_N3_M8:
    /* We have done M & 8, considering M=4/2/1 */
    andi      I,   M,   7
    beq       ZERO,I,   .L_N3_M0

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
    vld     U0,   A0,    0x00
    vld     U1,   A0,    0x10

    vldrepl.d     U4, B0, 0x00
    /* line 1 */
    vfmul.d  D0,  U0, U4
    vfmul.d  D1,  U1, U4

    vldrepl.d     U5, B0, 0x08
    /* line 2 */
    vfmul.d  D4,  U0, U5
    vfmul.d  D5,  U1, U5

    /* Add stride for A0 and B0 */
    addi.d    A0,  A0, 0x20
    addi.d    B0,  B0, 0x10
    /* Reduce L */
    addi.d    L,   L,  -1
    srai.d    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_N3_M4_L7 */
    beq       ZERO,TL, .L_N3_M4_L7

    vld     U8,   A0,    0x00
    vld     U9,   A0,    0x10

    addi.d    TL,  TL,  -1

    vldrepl.d  U12,   B0,  0x00
    vldrepl.d  U13,   B0,  0x08
    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x10

    beq    ZERO,    TL,  .L_N3_M4_TL1_END

.L_N3_M4_TL1: /* TL-- */
    KERNEL8x4x2

    addi.d    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_N3_M4_TL1
.L_N3_M4_TL1_END:
    KERNEL8x4x2_END

.L_N3_M4_L7:
    /* if (!(L & 7)) goto L_N3_M4_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_N3_M4_L0

.L_N3_M4_L71:
    vld     U0,   A0,    0x00
    vld     U1,   A0,    0x10

    vldrepl.d U4,  B0, 0x00
    vfmadd.d  D0,  U0, U4, D0
    vfmadd.d  D1,  U1, U4, D1

    vldrepl.d U5,  B0, 0x08
    vfmadd.d  D4,  U0, U5, D4
    vfmadd.d  D5,  U1, U5, D5

    /* Add stride for A0, B0 */
    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x10

    addi.d     TL,  TL, -1
    blt        ZERO,TL, .L_N3_M4_L71

.L_N3_M4_L0:
#if defined(TRMMKERNEL)
    vfmul.d  D0,   D0,  VALPHA
    vfmul.d  D1,   D1,  VALPHA
    vfmul.d  D4,   D4,  VALPHA
    vfmul.d  D5,   D5,  VALPHA
#else
    /* Load C0  */
    vld      U0,  C0,  0x00
    vld      U1,  C0,  0x10
    vfmadd.d D0,  D0,  VALPHA,  U0 /* D0 = U0 + (D0 * VALPHA) */
    vfmadd.d D1,  D1,  VALPHA,  U1

    /* Load C1  */
    vld      U2,  C1,  0x00
    vld      U3,  C1,  0x10
    vfmadd.d D4,  D4,  VALPHA,  U2
    vfmadd.d D5,  D5,  VALPHA,  U3
#endif // #if defined(TRMMKERNEL)

    /* Store C0 */
    vst      D0,  C0,  0x00
    vst      D1,  C0,  0x10
    /* Store C1 */
    vst      D4,  C1,  0x00
    vst      D5,  C1,  0x10

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
    vld     U0,   A0,    0x00

    vldrepl.d     U4, B0, 0x00
    /* line 1 */
    vfmul.d  D0,  U0, U4

    vldrepl.d     U4, B0, 0x08
    /* line 2 */
    vfmul.d  D4,  U0, U4

    /* Add stride for A0 and B0 */
    addi.d    A0,  A0, 0x10
    addi.d    B0,  B0, 0x10
    /* Reduce L */
    addi.d    L,   L,  -1
    srai.d    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_N3_M2_L7 */
    beq       ZERO,TL, .L_N3_M2_L7

    vld     U8,   A0,    0x00

    addi.d    TL,  TL,  -1

    vldrepl.d  U12,   B0,  0x00
    vldrepl.d  U13,   B0,  0x08
    addi.d     A0,  A0, 0x10
    addi.d     B0,  B0, 0x10

    beq    ZERO,    TL,  .L_N3_M2_TL1_END

.L_N3_M2_TL1: /* TL-- */
    KERNEL8x2x2

    addi.d    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_N3_M2_TL1
.L_N3_M2_TL1_END:
    KERNEL8x2x2_END

.L_N3_M2_L7:
    /* if (!(L & 7)) goto L_N3_M2_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_N3_M2_L0

.L_N3_M2_L71:
    vld     U0,   A0,    0x00

    vldrepl.d U4,  B0, 0x00
    vldrepl.d U5,  B0, 0x08
    vfmadd.d  D0,  U0, U4, D0

    vfmadd.d  D4,  U0, U5, D4

    /* Add stride for A0, B0 */
    addi.d     A0,  A0, 0x10
    addi.d     B0,  B0, 0x10

    addi.d     TL,  TL, -1
    blt        ZERO,TL, .L_N3_M2_L71

.L_N3_M2_L0:
#if defined(TRMMKERNEL)
    vfmul.d  D0,   D0,  VALPHA
    vfmul.d  D4,   D4,  VALPHA
#else
    /* Load C0  */
    vld      U0,  C0,  0x00
    vfmadd.d D0,  D0,  VALPHA,  U0 /* D0 = U0 + (D0 * VALPHA) */

    /* Load C1  */
    vld      U1,  C1,  0x00
    vfmadd.d D4,  D4,  VALPHA,  U1
#endif // #if defined(TRMMKERNEL)

    vst       D0,  C0,  0x00
    vst       D4,  C1,  0x00

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
    vldrepl.d   U0,   A0,  0x00

    vld         U4,   B0,  0x00
    /* line 1 */
    vfmul.d  D0,  U0, U4

    /* Add stride for A0 and B0 */
    addi.d    A0,  A0, 0x08
    addi.d    B0,  B0, 0x10
    /* Reduce L */
    addi.d    L,   L,  -1
    srai.d    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_N3_M1_L7 */
    beq       ZERO,TL, .L_N3_M1_L7

    vldrepl.d  U8,   A0,    0x00

    addi.d    TL,  TL,  -1

    vld        U12,  B0,    0x00
    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x10

    beq    ZERO,    TL,  .L_N3_M1_TL1_END

.L_N3_M1_TL1: /* TL-- */
    KERNEL8x1x2

    addi.d    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_N3_M1_TL1
.L_N3_M1_TL1_END:
    KERNEL8x1x2_END

.L_N3_M1_L7:
    /* if (!(L & 7)) goto L_N3_M1_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_N3_M1_L0

.L_N3_M1_L71:
    vldrepl.d  U0,  A0,    0x00

    vld        U4,  B0,    0x00
    vfmadd.d   D0,  U0, U4, D0

    /* Add stride for A0, B0 */
    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x10

    addi.d     TL,  TL, -1
    blt        ZERO,TL, .L_N3_M1_L71

.L_N3_M1_L0:
#if defined(TRMMKERNEL)
    vfmul.d  D0,   D0,  VALPHA
#else
    /* Load C0  */
    vld      U0,  C0,  0x00
    vld      U1,  C1,  0x00
    vilvl.d  U2,  U1,  U0
    vfmadd.d D0,  D0,  VALPHA,  U2
#endif // #if defined(TRMMKERNEL)

    vstelm.d D0,  C0,  0x00,  0x00
    vstelm.d D0,  C1,  0x00,  0x01

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
     * so we must mul 16
     */
    slli.d    T0,   K,   4
    slli.d    T1,   LDC, 4
    add.d     B,    B,   T0
    add.d     C,    C,   T1

#if defined(TRMMKERNEL) && !defined(LEFT)
    addi.d    OFF,  OFF, 0x02
#endif

    /* We must reinit I */
    srai.d   I,     M,   4     /* I = bm >> 4 */

/************************* Condition 2 if((N & 2) && (M >> 3)) End !!! *************************
*                                                   dgemm_core_16x2                                */

.L_N1:
    andi     J,    N,   1
    beq      ZERO, J,   .L_N0

/************************* Condition 3 if((N & 1) && (M >> 3)) START !!! *************************
*                                                   dgemm_core_16x1                                */

    move     C0,    C
    move     A0,    A

#if defined(TRMMKERNEL) && defined(LEFT)
    move     OFF,   OFFSET
#endif

    /* if (!(M >> 3)) goto L_N1_M8 */
    srai.d   I,     M,     3     /* I = bm >> 3 */
    beq      ZERO,  I,     .L_N1_M8

.L_N1_I1:
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

    /* Load 8 * 64 from A0
     * U0 = {a3,  a2}
     * U1 = {a1,  a0}
     * U2 = {a5,  a4}
     * U3 = {a7,  a6}
     */

    vld     U0,   A0,    0x00
    vld     U1,   A0,    0x10
    vld     U2,   A0,    0x20
    vld     U3,   A0,    0x30

    vldrepl.d   U4, B0, 0x00
    /* line 1 */
    vfmul.d  D0,  U0, U4
    vfmul.d  D1,  U1, U4
    vfmul.d  D2,  U2, U4
    vfmul.d  D3,  U3, U4

    /* Add stride for A0 and B0 */
    addi.d    A0,  A0, 0x40
    addi.d    B0,  B0, 0x08
    /* Reduce L */
    addi.d    L,   L,  -1
    srai.d    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_N1_L7 */
    beq       ZERO,TL, .L_N1_L7

    vld     U8,   A0,    0x00
    vld     U9,   A0,    0x10
    vld     U10,  A0,    0x20
    vld     U11,  A0,    0x30

    addi.d    TL,  TL,  -1

    vldrepl.d  U12,   B0,  0x00
    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x08

    beq    ZERO,    TL,  .L_N1_TL1_END
.L_N1_TL1: /* TL-- */
    KERNEL8x8x1

    addi.d    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_N1_TL1
.L_N1_TL1_END:
    KERNEL8x8x1_END

.L_N1_L7:
    /* if (!(L & 7)) goto L_N1_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_N1_L0

.L_N1_L71:
    /* Load 16 * 64 from A0 */
    vld     U0,   A0,    0x00
    vld     U1,   A0,    0x10
    vld     U2,   A0,    0x20
    vld     U3,   A0,    0x30

    vldrepl.d U4,  B0, 0x00
    vfmadd.d  D0,  U0, U4, D0
    vfmadd.d  D1,  U1, U4, D1
    vfmadd.d  D2,  U2, U4, D2
    vfmadd.d  D3,  U3, U4, D3

    /* Add stride for A0, B0 */
    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x08

    addi.d     TL,  TL, -1
    blt        ZERO,TL, .L_N1_L71

.L_N1_L0:
#if defined(TRMMKERNEL)
    vfmul.d  D0,   D0,  VALPHA
    vfmul.d  D1,   D1,  VALPHA
    vfmul.d  D2,   D2,  VALPHA
    vfmul.d  D3,   D3,  VALPHA
#else
    /* Load C0  */
    vld      U0,  C0,  0x00
    vld      U1,  C0,  0x10
    vld      U2,  C0,  0x20
    vld      U3,  C0,  0x30
    vfmadd.d D0,  D0,  VALPHA,  U0 /* D0 = U0 + (D0 * VALPHA) */
    vfmadd.d D1,  D1,  VALPHA,  U1
    vfmadd.d D2,  D2,  VALPHA,  U2
    vfmadd.d D3,  D3,  VALPHA,  U3
#endif // #if defined(TRMMKERNEL)

    /* Store C0 */
    vst      D0,  C0,  0x00
    vst      D1,  C0,  0x10
    vst      D2,  C0,  0x20
    vst      D3,  C0,  0x30

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
    addi.d    OFF,   OFF,  0x8
#endif
#endif   // #if defined(TRMMKERNEL)

    addi.d    I,   I,   -1  /* I-- */
    blt       ZERO,I,   .L_N1_I1

.L_N1_M8:
    /* We have done M & 16, considering M=8/4/2/1 */
    andi      I,   M,   7
    beq       ZERO,I,   .L_N1_M0

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
    vld     U0,   A0,    0x00
    vld     U1,   A0,    0x10

    vldrepl.d     U4, B0, 0x00
    /* line 1 */
    vfmul.d  D0,  U0, U4
    vfmul.d  D1,  U1, U4

    /* Add stride for A0 and B0 */
    addi.d    A0,  A0, 0x20
    addi.d    B0,  B0, 0x08
    /* Reduce L */
    addi.d    L,   L,  -1
    srai.d    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_N1_M4_L7 */
    beq       ZERO,TL, .L_N1_M4_L7

    vld     U8,   A0,    0x00
    vld     U9,   A0,    0x10

    addi.d    TL,  TL,  -1

    vldrepl.d  U12,   B0,  0x00
    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x08

    beq    ZERO,    TL,  .L_N1_M4_TL1_END

.L_N1_M4_TL1: /* TL-- */
    KERNEL8x4x1

    addi.d    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_N1_M4_TL1
.L_N1_M4_TL1_END:
    KERNEL8x4x1_END

.L_N1_M4_L7:
    /* if (!(L & 7)) goto L_N1_M4_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_N1_M4_L0

.L_N1_M4_L71:
    vld     U0,   A0,    0x00
    vld     U1,   A0,    0x10

    vldrepl.d U4,  B0, 0x00
    vfmadd.d  D0,  U0, U4, D0
    vfmadd.d  D1,  U1, U4, D1

    /* Add stride for A0, B0 */
    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x08

    addi.d     TL,  TL, -1
    blt        ZERO,TL, .L_N1_M4_L71

.L_N1_M4_L0:
#if defined(TRMMKERNEL)
    vfmul.d  D0,   D0,  VALPHA
    vfmul.d  D1,   D1,  VALPHA
#else
    /* Load C0  */
    vld      U0,  C0,  0x00
    vld      U1,  C0,  0x10
    vfmadd.d D0,  D0,  VALPHA,  U0 /* D0 = U0 + (D0 * VALPHA) */
    vfmadd.d D1,  D1,  VALPHA,  U1
#endif // #if defined(TRMMKERNEL)

    /* Store C0 */
    vst      D0,  C0,  0x00
    vst      D1,  C0,  0x10

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

/********LOOP (if(N & 1) && (M & 4) ) End************/

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
    vld     U0,   A0,    0x00

    vldrepl.d     U4, B0, 0x00
    /* line 1 */
    vfmul.d  D0,  U0, U4

    /* Add stride for A0 and B0 */
    addi.d    A0,  A0, 0x10
    addi.d    B0,  B0, 0x08
    /* Reduce L */
    addi.d    L,   L,  -1
    srai.d    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_N1_M2_L7 */
    beq       ZERO,TL, .L_N1_M2_L7

    vld     U8,   A0,    0x00

    addi.d    TL,  TL,  -1

    vldrepl.d  U12,   B0,  0x00
    addi.d     A0,  A0, 0x10
    addi.d     B0,  B0, 0x08

    beq    ZERO,    TL,  .L_N1_M2_TL1_END

.L_N1_M2_TL1: /* TL-- */
    KERNEL8x2x1

    addi.d    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_N1_M2_TL1
.L_N1_M2_TL1_END:
    KERNEL8x2x1_END

.L_N1_M2_L7:
    /* if (!(L & 7)) goto L_N1_M2_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_N1_M2_L0

.L_N1_M2_L71:
    vld     U0,   A0,    0x00

    vldrepl.d U4,  B0, 0x00
    vfmadd.d  D0,  U0, U4, D0

    /* Add stride for A0, B0 */
    addi.d     A0,  A0, 0x10
    addi.d     B0,  B0, 0x08

    addi.d     TL,  TL, -1
    blt        ZERO,TL, .L_N1_M2_L71

.L_N1_M2_L0:
#if defined(TRMMKERNEL)
    vfmul.d  D0,   D0,  VALPHA
#else
    /* Load C0  */
    vld      U0,  C0,  0x00
    vfmadd.d D0,  D0,  VALPHA,  U0 /* D0 = U0 + (D0 * VALPHA) */
#endif // #if defined(TRMMKERNEL)

    vstelm.d D0,  C0,  0x00,    0x00
    vstelm.d D0,  C0,  0x08,    0x01

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

/********LOOP (if(N & 1 ) && (M & 2) ) End************/

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
    vldrepl.d  U0,   A0,    0x00

    vldrepl.d  U4, B0, 0x00
    /* line 1 */
    vfmul.d  D0,  U0, U4

    /* Add stride for A0 and B0 */
    addi.d    A0,  A0, 0x08
    addi.d    B0,  B0, 0x08
    /* Reduce L */
    addi.d    L,   L,  -1
    srai.d    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_N1_M1_L7 */
    beq       ZERO,TL, .L_N1_M1_L7

    vldrepl.d  U8,   A0,    0x00

    addi.d    TL,  TL,  -1

    vldrepl.d  U12,   B0,  0x00
    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x08

    beq    ZERO,    TL,  .L_N1_M1_TL1_END

.L_N1_M1_TL1: /* TL-- */
    KERNEL8x1x1

    addi.d    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_N1_M1_TL1
.L_N1_M1_TL1_END:
    KERNEL8x1x1_END

.L_N1_M1_L7:
    /* if (!(L & 7)) goto L_N1_M1_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_N1_M1_L0

.L_N1_M1_L71:
    vldrepl.d  U0,   A0,    0x00

    vldrepl.d  U4,  B0, 0x00
    vfmadd.d   D0,  U0, U4, D0

    /* Add stride for A0, B0 */
    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x08

    addi.d     TL,  TL, -1
    blt        ZERO,TL, .L_N1_M1_L71

.L_N1_M1_L0:
#if defined(TRMMKERNEL)
    vfmul.d  D0,   D0,  VALPHA
#else
    /* Load C0  */
    vldrepl.d  U0,  C0,  0x00
    vfmadd.d  D0,  D0,  VALPHA,  U0 /* D0 = U0 + (D0 * VALPHA) */
#endif // #if defined(TRMMKERNEL)

    vstelm.d D0,  C0,  0x00,  0x00

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

/********LOOP (if(N & 1 ) && (M & 1) ) End************/

.L_N1_M0:

/************************* Condition 3 if((N & 1) && (M >> 3)) End !!! *************************
*                                                   dgemm_core_16x1                                */

.L_N0:
    /* Restore regs */
    LDARG    $r23,  $sp,   0
    LDARG    $r24,  $sp,   8
    LDARG    $r25,  $sp,   16
    LDARG    $r26,  $sp,   24
    LDARG    $r27,  $sp,   32
    LD       $f24,  $sp,   40
    LD       $f25,  $sp,   48
    LD       $f26,  $sp,   56
    LD       $f27,  $sp,   64
    LD       $f28,  $sp,   72
    LD       $f29,  $sp,   80
    LD       $f30,  $sp,   88
    LD       $f31,  $sp,   96
    addi.d   $sp,   $sp,   112

    jirl    $r0, $r1, 0x0

    EPILOGUE
