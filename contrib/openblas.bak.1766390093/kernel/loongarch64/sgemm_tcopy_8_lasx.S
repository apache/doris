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
* 2023/08/23 guxiwei
*        UTEST                  : OK
*        CTEST                  : OK
*        TEST                   : OK
*********************************************************************/

/* Function parameters */
#define M      $r4    // param 1: m
#define N      $r5    // param 2: n
#define SRC    $r6    // param 3: src
#define LDA    $r7    // param 4: lda
#define DST    $r8    // param 5: dst

#define I      $r9
#define J      $r10
#define S0     $r11
#define S1     $r12
#define S2     $r13
#define S3     $r14
#define S4     $r15
#define S5     $r16
#define S6     $r17
#define S7     $r18
#define S8     $r19
#define P0     $r20
#define P1     $r23
#define P2     $r24
#define P3     $r25
#define P4     $r26
#define T0     $r27
#define T1     $r28
#define TL     $r7
#undef  ZERO
#define ZERO   $r0

/* LASX vectors */
#define U0     $xr0
#define U1     $xr1
#define U2     $xr2
#define U3     $xr3
#define U4     $xr4
#define U5     $xr5
#define U6     $xr6
#define U7     $xr7

// Loops outline
//.L_M8 <-------------------
//|   .L_N8:                |
//|   .L_N7:                | Main Loop
//|   .L_N4:                |
//|   .L_N3:                |
//|   .L_N2:                |
//|   .L_N1:                |
//|   .L_N0: ---------------
//.L_M7
//.L_M4
//|   .L_M4_N8:
//|   .L_M4_N7:
//|   .L_M4_N4:
//|   .L_M4_N3:
//|   .L_M4_N2:
//|   .L_M4_N1:
//.L_M3
//.L_M2
//|   .L_M2_N8:
//|   .L_M2_N7:
//|   .L_M2_N4:
//|   .L_M2_N3:
//|   .L_M2_N2:
//|   .L_M2_N1:
//.L_M1
//|   .L_M1_N8:
//|   .L_M1_N7:
//|   .L_M1_N4:
//|   .L_M1_N3:
//|   .L_M1_N2:
//|   .L_M1_N1:
//.L_M0

    PROLOGUE
    push_if_used 6, 0

    move       S0,     SRC
    move       P0,     DST

    PTR_SRAI     T0,     N,     0x04
    PTR_SRAI     T1,     N,     0x03
    PTR_SLLI     T0,     T0,    0x04
    PTR_SLLI     T1,     T1,    0x03

    PTR_MUL      P2,     M,     T1
    PTR_SLLI     P2,     P2,    0x02
    PTR_ADD      P2,     DST,   P2
    PTR_SRAI     T0,     N,     0x02
    PTR_SRAI     T1,     N,     0x01
    PTR_SLLI     T0,     T0,    0x02
    PTR_SLLI     T1,     T1,    0x01
    PTR_MUL      P3,     M,     T0
    PTR_MUL      P4,     M,     T1
    PTR_SLLI     P3,     P3,    0x02
    PTR_SLLI     P4,     P4,    0x02
    PTR_ADD      P3,     DST,   P3
    PTR_ADD      P4,     DST,   P4

    PTR_SLLI     TL,     LDA,   0x02
    PTR_SRAI     J,      M,     0x03
    PTR_SLLI     T0,     TL,    0x01
    PTR_SLLI     T1,     M,     0x05
    beq          ZERO,   J,     .L_M7
.align 5
.L_M8:
    move         S1,     S0
    PTR_ADD      S2,     S0,    TL
    PTR_ADD      S3,     S1,    T0
    PTR_ADD      S4,     S2,    T0
    PTR_ADD      S5,     S3,    T0
    PTR_ADD      S6,     S4,    T0
    PTR_ADD      S7,     S5,    T0
    PTR_ADD      S8,     S6,    T0
    PTR_ADD      S0,     S7,    T0

    move         P1,     P0
    PTR_ADDI     P0,     P0,    0x100

    PTR_SRAI     I,      N,     0x03
    PTR_ADDI     J,      J,     -1
    beq          ZERO,   I,     .L_N7
.L_N8:
    xvld       U0,     S1,    0x00
    xvld       U1,     S2,    0x00
    xvld       U2,     S3,    0x00
    xvld       U3,     S4,    0x00
    xvld       U4,     S5,    0x00
    xvld       U5,     S6,    0x00
    xvld       U6,     S7,    0x00
    xvld       U7,     S8,    0x00

    GST xv, , U0, P1, 0x00, U1, P1, 0x20, U2, P1, 0x40, U3, P1, 0x60, \
              U4, P1, 0x80, U5, P1, 0xA0, U6, P1, 0xC0, U7, P1, 0xE0

    PTR_ADDI     S1,     S1,    0x20
    PTR_ADDI     S2,     S2,    0x20
    PTR_ADDI     S3,     S3,    0x20
    PTR_ADDI     S4,     S4,    0x20
    PTR_ADDI     S5,     S5,    0x20
    PTR_ADDI     S6,     S6,    0x20
    PTR_ADDI     S7,     S7,    0x20
    PTR_ADDI     S8,     S8,    0x20

    PTR_ADDI     I,      I,     -1
    PTR_ADD      P1,     P1,    T1
    blt          ZERO,   I,     .L_N8
.L_N7:
    andi       I,      N,     0x04
    beq        ZERO,   I,     .L_N3
.L_N4:
    GLD v, , $vr0, S1, 0x00, $vr1, S2, 0x00, $vr2, S3, 0x00, $vr3, S4, 0x00, \
             $vr4, S5, 0x00, $vr5, S6, 0x00, $vr6, S7, 0x00, $vr7, S8, 0x00
    GST v, , $vr0, P2, 0x00, $vr1, P2, 0x10, $vr2, P2, 0x20, $vr3, P2, 0x30, \
             $vr4, P2, 0x40, $vr5, P2, 0x50, $vr6, P2, 0x60, $vr7, P2, 0x70
    PTR_ADDI     S1,     S1,    0x10
    PTR_ADDI     S2,     S2,    0x10
    PTR_ADDI     S3,     S3,    0x10
    PTR_ADDI     S4,     S4,    0x10
    PTR_ADDI     S5,     S5,    0x10
    PTR_ADDI     S6,     S6,    0x10
    PTR_ADDI     S7,     S7,    0x10
    PTR_ADDI     S8,     S8,    0x10
    PTR_ADDI     P2,     P2,    0x80
.L_N3:
    andi       I,      N,     0x02
    beq        ZERO,   I,     .L_N1
.L_N2:
    GLD f, d, $f0, S1, 0x00, $f1, S2, 0x00, $f2, S3, 0x00, $f3, S4, 0x00, \
              $f4, S5, 0x00, $f5, S6, 0x00, $f6, S7, 0x00, $f7, S8, 0x00
    GST f, d, $f0, P3, 0x00, $f1, P3, 0x08, $f2, P3, 0x10, $f3, P3, 0x18, \
              $f4, P3, 0x20, $f5, P3, 0x28, $f6, P3, 0x30, $f7, P3, 0x38
    PTR_ADDI     S1,     S1,    0x08
    PTR_ADDI     S2,     S2,    0x08
    PTR_ADDI     S3,     S3,    0x08
    PTR_ADDI     S4,     S4,    0x08
    PTR_ADDI     S5,     S5,    0x08
    PTR_ADDI     S6,     S6,    0x08
    PTR_ADDI     S7,     S7,    0x08
    PTR_ADDI     S8,     S8,    0x08
    PTR_ADDI     P3,     P3,    0x40
.L_N1:
    andi       I,      N,     0x01
    beq        ZERO,   I,     .L_N0

    GLD f, s, $f0, S1, 0x00, $f1, S2, 0x00, $f2, S3, 0x00, $f3, S4, 0x00, \
              $f4, S5, 0x00, $f5, S6, 0x00, $f6, S7, 0x00, $f7, S8, 0x00
    GST f, s, $f0, P4, 0x00, $f1, P4, 0x04, $f2, P4, 0x08, $f3, P4, 0x0C, \
              $f4, P4, 0x10, $f5, P4, 0x14, $f6, P4, 0x18, $f7, P4, 0x1C
    PTR_ADDI     S1,     S1,    0x04
    PTR_ADDI     S2,     S2,    0x04
    PTR_ADDI     S3,     S3,    0x04
    PTR_ADDI     S4,     S4,    0x04
    PTR_ADDI     S5,     S5,    0x04
    PTR_ADDI     S6,     S6,    0x04
    PTR_ADDI     S7,     S7,    0x04
    PTR_ADDI     S8,     S8,    0x04
    PTR_ADDI     P4,     P4,    0x20
.L_N0:
    blt        ZERO,   J,     .L_M8

.L_M7:
    andi       J,      M,     0x04
    beq        ZERO,   J,     .L_M3
.L_M4:
    move         S1,     S0
    PTR_ADD      S2,     S0,    TL
    PTR_ADD      S3,     S1,    T0
    PTR_ADD      S4,     S2,    T0
    PTR_ADD      S0,     S3,    T0

    move         P1,     P0
    PTR_ADDI     P0,     P0,    0x80

    PTR_SRAI     I,      N,     0x03
    beq          ZERO,   I,     .L_M4_N7
.align 5
.L_M4_N8:
    xvld       U0,     S1,    0x00
    xvld       U1,     S2,    0x00
    xvld       U2,     S3,    0x00
    xvld       U3,     S4,    0x00

    GST xv, , U0, P1, 0x00, U1, P1, 0x20, U2, P1, 0x40, U3, P1, 0x60

    PTR_ADDI     S1,     S1,    0x20
    PTR_ADDI     S2,     S2,    0x20
    PTR_ADDI     S3,     S3,    0x20
    PTR_ADDI     S4,     S4,    0x20

    PTR_ADDI     I,      I,     -1
    PTR_ADD      P1,     P1,    T1
    blt          ZERO,   I,     .L_M4_N8
.L_M4_N7:
    andi       I,      N,     0x04
    beq        ZERO,   I,     .L_M4_N3
.L_M4_N4:
    GLD v, , $vr0, S1, 0x00, $vr1, S2, 0x00, $vr2, S3, 0x00, $vr3, S4, 0x00
    GST v, , $vr0, P2, 0x00, $vr1, P2, 0x10, $vr2, P2, 0x20, $vr3, P2, 0x30
    PTR_ADDI     S1,     S1,    0x10
    PTR_ADDI     S2,     S2,    0x10
    PTR_ADDI     S3,     S3,    0x10
    PTR_ADDI     S4,     S4,    0x10
    PTR_ADDI     P2,     P2,    0x40
.L_M4_N3:
    andi       I,      N,     0x02
    beq        ZERO,   I,     .L_M4_N1
.L_M4_N2:
    GLD f, d, $f0, S1, 0x00, $f1, S2, 0x00, $f2, S3, 0x00, $f3, S4, 0x00
    GST f, d, $f0, P3, 0x00, $f1, P3, 0x08, $f2, P3, 0x10, $f3, P3, 0x18
    PTR_ADDI     S1,     S1,    0x08
    PTR_ADDI     S2,     S2,    0x08
    PTR_ADDI     S3,     S3,    0x08
    PTR_ADDI     S4,     S4,    0x08
    PTR_ADDI     P3,     P3,    0x20
.L_M4_N1:
    andi        I,      N,     0x01
    beq         ZERO,   I,     .L_M3

    GLD f, s, $f0, S1, 0x00, $f1, S2, 0x00, $f2, S3, 0x00, $f3, S4, 0x00
    GST f, s, $f0, P4, 0x00, $f1, P4, 0x04, $f2, P4, 0x08, $f3, P4, 0x0C
    PTR_ADDI     S1,     S1,    0x04
    PTR_ADDI     S2,     S2,    0x04
    PTR_ADDI     S3,     S3,    0x04
    PTR_ADDI     S4,     S4,    0x04
    PTR_ADDI     P4,     P4,    0x10
.L_M3:
    andi       J,      M,     0x02
    beq        ZERO,   J,     .L_M1
.L_M2:
    move         S1,     S0
    PTR_ADD      S2,     S0,    TL
    PTR_ADD      S0,     S0,    T0

    move         P1,     P0
    PTR_ADDI     P0,     P0,    0x40

    PTR_SRAI     I,      N,     0x03
    beq          ZERO,   I,     .L_M2_N7
.align 5
.L_M2_N8:
    xvld       U0,     S1,    0x00
    xvld       U1,     S2,    0x00

    GST xv, , U0, P1, 0x00, U1, P1, 0x20

    PTR_ADDI     S1,     S1,    0x20
    PTR_ADDI     S2,     S2,    0x20
    PTR_ADDI     I,      I,     -1
    PTR_ADD      P1,     P1,    T1
    blt          ZERO,   I,     .L_M2_N8
.L_M2_N7:
    andi       I,      N,     0x04
    beq        ZERO,   I,     .L_M2_N3
.L_M2_N4:
    GLD v, , $vr0, S1, 0x00, $vr1, S2, 0x00
    GST v, , $vr0, P2, 0x00, $vr1, P2, 0x10
    PTR_ADDI     S1,     S1,    0x10
    PTR_ADDI     S2,     S2,    0x10
    PTR_ADDI     P2,     P2,    0x20
.L_M2_N3:
    andi       I,      N,     0x02
    beq        ZERO,   I,     .L_M2_N1
.L_M2_N2:
    GLD f, d, $f0, S1, 0x00, $f1, S2, 0x00
    GST f, d, $f0, P3, 0x00, $f1, P3, 0x08
    PTR_ADDI     S1,     S1,    0x08
    PTR_ADDI     S2,     S2,    0x08
    PTR_ADDI     P3,     P3,    0x10
.L_M2_N1:
    andi       I,      N,     0x01
    beq        ZERO,   I,     .L_M1

    GLD f, s, $f0, S1, 0x00, $f1, S2, 0x00
    GST f, s, $f0, P4, 0x00, $f1, P4, 0x04
    PTR_ADDI     S1,     S1,    0x04
    PTR_ADDI     S2,     S2,    0x04
    PTR_ADDI     P4,     P4,    0x08
.L_M1:
    andi       J,      M,     0x01
    beq        ZERO,   J,     .L_M0

    move         S1,     S0
    PTR_ADD      S2,     S0,    TL

    move         P1,     P0
    PTR_ADDI     P0,     P0,    0x20

    PTR_SRAI     I,      N,     0x03
    beq          ZERO,   I,     .L_M1_N7
.align 5
.L_M1_N8:
    xvld       U0,     S1,    0x00

    GST xv, , U0, P1, 0x00

    PTR_ADDI     S1,     S1,    0x20

    PTR_ADDI     I,      I,     -1
    PTR_ADD      P1,     P1,    T1
    blt          ZERO,   I,     .L_M1_N8
.L_M1_N7:
    andi       I,      N,     0x04
    beq        ZERO,   I,     .L_M1_N3
.L_M1_N4:
    GLD v, , $vr0, S1, 0x00
    GST v, , $vr0, P2, 0x00
    PTR_ADDI     S1,     S1,    0x10
    PTR_ADDI     P2,     P2,    0x10
.L_M1_N3:
    andi       I,      N,     0x02
    beq        ZERO,   I,     .L_M1_N1
.L_M1_N2:
    GLD f, d, $f0, S1, 0x00
    GST f, d, $f0, P3, 0x00
    PTR_ADDI     S1,     S1,    0x08
    PTR_ADDI     P3,     P3,    0x08
.L_M1_N1:
    andi       I,      N,     0x01
    beq        ZERO,   I,     .L_M0

    GLD f, s, $f0, S1, 0x00
    GST f, s, $f0, P4, 0x00
    PTR_ADDI     S1,     S1,    0x04
    PTR_ADDI     P4,     P4,    0x04
.L_M0:
    pop_if_used 6, 0
    jirl       $r0,    $r1,   0x00
    EPILOGUE
