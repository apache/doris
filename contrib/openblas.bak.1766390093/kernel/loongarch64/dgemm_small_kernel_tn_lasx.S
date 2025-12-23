/***************************************************************************
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
*****************************************************************************/

#define ASSEMBLER

#include "common.h"
#include "loongarch64_asm.S"

#define M       $a0
#define N       $a1
#define K       $a2
#define A       $a3
#define LDA     $a4
#define ALPHA   $f0
#define B       $a5
#define LDB     $a6
#define C       $a7
#define LDC     $t0
#ifdef B0
#define BETA    $f1
#endif
#undef ZERO
#define ZERO    $r0

#define M4      $t1
#define M2      $t1
#define M1      $t1
#define N4      $t2
#define N2      $t2
#define N1      $t2
#define K8      $t3
#define A0      $t4
#define X0      $t5
#define B1      $t6
#define B2      $t7
#define B3      $t8
#define C0      $s0
#define C1      $s1
#define C2      $s2
#define C3      $s3
#define K1      $s4
#define A1      $s5
#define A2      $s6
#define A3      $s7

#define VALPHA  $xr0
#ifndef B0
#define VBETA   $xr1
#endif
#define D0     $xr2
#define D1     $xr3
#define D2     $xr4
#define D3     $xr5
#define T0     $xr6
#define T1     $xr7
#define T2     $xr8
#define T3     $xr9
#define Y0     $xr10
#define Y1     $xr11
#define Y2     $xr12
#define Y3     $xr13
#define G0     $xr14
#define G1     $xr15
#define G2     $xr16
#define G3     $xr17
#define S0     $xr18
#define S1     $xr19
#define S2     $xr20
#define S3     $xr21
#define Z0     $xr22
#define Z1     $xr23
#define Z2     $xr24
#define Z3     $xr25
#define V0     $vr2
#define V1     $vr3
#define V2     $vr4
#define V3     $vr5
#define F0     $f2
#define F1     $f3
#define F2     $f4
#define F3     $f5

    PROLOGUE
    PTR_LD          LDC,    $sp,    0
    push_if_used    8, 2
    xvreplve0.d     VALPHA, VALPHA
#ifndef B0
    xvreplve0.d     VBETA,  VBETA
#endif
    PTR_SLLI    LDA,    LDA,    3
    PTR_SLLI    LDB,    LDB,    3
    PTR_SLLI    LDC,    LDC,    3
    PTR_SLLI    K8,     K,      3
    PTR_SRAI    M4,     M,      2 // M >> 2
    beqz        M4,    .L_M3
.L_M4:
    PTR_SRAI    N4,     N,      2 // N >> 2
    move        A0,     A // Restore A0
    PTR_ADD     A1,     A0,     LDA
    PTR_ADD     A2,     A1,     LDA
    PTR_ADD     A3,     A2,     LDA
    move        X0,     B // Restore X0
    PTR_ADD     B1,     X0,     LDB
    PTR_ADD     B2,     B1,     LDB
    PTR_ADD     B3,     B2,     LDB
    move        C0,     C // Restore C0
    PTR_ADD     C1,     C0,     LDC
    PTR_ADD     C2,     C1,     LDC
    PTR_ADD     C3,     C2,     LDC
    beqz        N4,     .L_M4_N3
.L_M4_N4:
    GXOR xv, v, D0, D0, D0, D1, D1, D1, D2, D2, D2, D3, D3, D3
    move        K1,     K // Restore K1
    PTR_ADDI    N4,     N4,     -1
    bge         ZERO,   K,      .L_M4_N4_END
    PTR_SRAI    K1,     K1,     3
    beq         ZERO,   K1,     .L_M4_N4_K7
.L_M4_N4_K8:
    PTR_ADDI    K1,     K1,    -1
    GLD xv, , T0, A0, 0x00, T1, A1, 0x00, T2, A2, 0x00, T3, A3, 0x00
    GTRANSPOSE4x4_D T0, T1, T2, T3, S0, S1, S2, S3, Z0, Z1

    GLDREPL xv, d, Z0, X0, 0x00, Z1, B1, 0x00, Z2, B2, 0x00, Z3, B3, 0x00
    GLDREPL xv, d, T0, X0, 0x08, T1, B1, 0x08, T2, B2, 0x08, T3, B3, 0x08
    GLDREPL xv, d, Y0, X0, 0x10, Y1, B1, 0x10, Y2, B2, 0x10, Y3, B3, 0x10
    GLDREPL xv, d, G0, X0, 0x18, G1, B1, 0x18, G2, B2, 0x18, G3, B3, 0x18
    GMADD xvf, d, D0, S0, Z0, D0, \
                  D1, S0, Z1, D1, \
                  D2, S0, Z2, D2, \
                  D3, S0, Z3, D3
    GMADD xvf, d, D0, S1, T0, D0, \
                  D1, S1, T1, D1, \
                  D2, S1, T2, D2, \
                  D3, S1, T3, D3
    GMADD xvf, d, D0, S2, Y0, D0, \
                  D1, S2, Y1, D1, \
                  D2, S2, Y2, D2, \
                  D3, S2, Y3, D3
    GMADD xvf, d, D0, S3, G0, D0, \
                  D1, S3, G1, D1, \
                  D2, S3, G2, D2, \
                  D3, S3, G3, D3

    GLD xv, , T0, A0, 0x20, T1, A1, 0x20, T2, A2, 0x20, T3, A3, 0x20
    GTRANSPOSE4x4_D T0, T1, T2, T3, S0, S1, S2, S3, Z0, Z1

    GLDREPL xv, d, Z0, X0, 0x20, Z1, B1, 0x20, Z2, B2, 0x20, Z3, B3, 0x20
    GLDREPL xv, d, T0, X0, 0x28, T1, B1, 0x28, T2, B2, 0x28, T3, B3, 0x28
    GLDREPL xv, d, Y0, X0, 0x30, Y1, B1, 0x30, Y2, B2, 0x30, Y3, B3, 0x30
    GLDREPL xv, d, G0, X0, 0x38, G1, B1, 0x38, G2, B2, 0x38, G3, B3, 0x38
    GMADD xvf, d, D0, S0, Z0, D0, \
                  D1, S0, Z1, D1, \
                  D2, S0, Z2, D2, \
                  D3, S0, Z3, D3
    GMADD xvf, d, D0, S1, T0, D0, \
                  D1, S1, T1, D1, \
                  D2, S1, T2, D2, \
                  D3, S1, T3, D3
    GMADD xvf, d, D0, S2, Y0, D0, \
                  D1, S2, Y1, D1, \
                  D2, S2, Y2, D2, \
                  D3, S2, Y3, D3
    GMADD xvf, d, D0, S3, G0, D0, \
                  D1, S3, G1, D1, \
                  D2, S3, G2, D2, \
                  D3, S3, G3, D3

    PTR_ADDI    X0,     X0,     0x40
    PTR_ADDI    B1,     B1,     0x40
    PTR_ADDI    B2,     B2,     0x40
    PTR_ADDI    B3,     B3,     0x40
    PTR_ADDI    A0,     A0,     0x40
    PTR_ADDI    A1,     A1,     0x40
    PTR_ADDI    A2,     A2,     0x40
    PTR_ADDI    A3,     A3,     0x40
    bnez        K1,     .L_M4_N4_K8
 .L_M4_N4_K7:
    andi        K1,     K,      4
    beqz        K1,     .L_M4_N4_3
 .L_M4_N4_K4:
    GLD xv, , T0, A0, 0x00, T1, A1, 0x00, T2, A2, 0x00, T3, A3, 0x00
    GTRANSPOSE4x4_D T0, T1, T2, T3, S0, S1, S2, S3, Z0, Z1

    GLDREPL xv, d, Z0, X0, 0x00, Z1, B1, 0x00, Z2, B2, 0x00, Z3, B3, 0x00
    GLDREPL xv, d, T0, X0, 0x08, T1, B1, 0x08, T2, B2, 0x08, T3, B3, 0x08
    GLDREPL xv, d, Y0, X0, 0x10, Y1, B1, 0x10, Y2, B2, 0x10, Y3, B3, 0x10
    GLDREPL xv, d, G0, X0, 0x18, G1, B1, 0x18, G2, B2, 0x18, G3, B3, 0x18
    GMADD xvf, d, D0, S0, Z0, D0, \
                  D1, S0, Z1, D1, \
                  D2, S0, Z2, D2, \
                  D3, S0, Z3, D3
    GMADD xvf, d, D0, S1, T0, D0, \
                  D1, S1, T1, D1, \
                  D2, S1, T2, D2, \
                  D3, S1, T3, D3
    GMADD xvf, d, D0, S2, Y0, D0, \
                  D1, S2, Y1, D1, \
                  D2, S2, Y2, D2, \
                  D3, S2, Y3, D3
    GMADD xvf, d, D0, S3, G0, D0, \
                  D1, S3, G1, D1, \
                  D2, S3, G2, D2, \
                  D3, S3, G3, D3
    PTR_ADDI    X0,     X0,     0x20
    PTR_ADDI    B1,     B1,     0x20
    PTR_ADDI    B2,     B2,     0x20
    PTR_ADDI    B3,     B3,     0x20
    PTR_ADDI    A0,     A0,     0x20
    PTR_ADDI    A1,     A1,     0x20
    PTR_ADDI    A2,     A2,     0x20
    PTR_ADDI    A3,     A3,     0x20
 .L_M4_N4_3:
    andi        K1,     K,      3
    beqz        K1,     .L_M4_N4_END
 .L_M4_N4_K1:
    GLD xv, , S0, A0, 0x00, S1, A1, 0x00, S2, A2, 0x00, S3, A3, 0x00
    GINSVE0 xv, d, S0, S1, 1, S0, S2, 2, S0, S3, 3
    GLDREPL xv, d, Z0, X0, 0x00, Z1, B1, 0x00, Z2, B2, 0x00, Z3, B3, 0x00
    GMADD xvf, d, D0, S0, Z0, D0, \
                  D1, S0, Z1, D1, \
                  D2, S0, Z2, D2, \
                  D3, S0, Z3, D3
    PTR_ADDI    K1,     K1,     -1
    PTR_ADDI    X0,     X0,     0x08
    PTR_ADDI    B1,     B1,     0x08
    PTR_ADDI    B2,     B2,     0x08
    PTR_ADDI    B3,     B3,     0x08
    PTR_ADDI    A0,     A0,     0x08
    PTR_ADDI    A1,     A1,     0x08
    PTR_ADDI    A2,     A2,     0x08
    PTR_ADDI    A3,     A3,     0x08
    bnez        K1,     .L_M4_N4_K1
 .L_M4_N4_END:
    GMUL xvf, d, D0,  D0,  VALPHA, D1,  D1,  VALPHA, D2,  D2,  VALPHA, D3,  D3,  VALPHA
#ifndef B0
    GLD xv, , S0, C0, 0x00, S1, C1, 0x00, S2, C2, 0x00, S3, C3, 0x00
    GMADD xvf, d, D0, S0, VBETA, D0, \
                  D1, S1, VBETA, D1, \
                  D2, S2, VBETA, D2, \
                  D3, S3, VBETA, D3
#endif
    GST xv, , D3, C3, 0x00, \
              D2, C2, 0x00, \
              D1, C1, 0x00, \
              D0, C0, 0x00
    // Update C0, C1, C2, C3
    PTR_ALSL    C0,     LDC,    C0,     2
    PTR_ALSL    C1,     LDC,    C1,     2
    PTR_ALSL    C2,     LDC,    C2,     2
    PTR_ALSL    C3,     LDC,    C3,     2
    // Update X0, B1, B2, B3
    PTR_SUB     X0,     X0,    K8
    PTR_SUB     B1,     B1,    K8
    PTR_SUB     B2,     B2,    K8
    PTR_SUB     B3,     B3,    K8

    PTR_ALSL    X0,     LDB,    X0,     2
    PTR_ALSL    B1,     LDB,    B1,     2
    PTR_ALSL    B2,     LDB,    B2,     2
    PTR_ALSL    B3,     LDB,    B3,     2
    // Restore A0, A1, A2, A3
    move        A0,     A
    PTR_ADD     A1,     A0,     LDA
    PTR_ADD     A2,     A1,     LDA
    PTR_ADD     A3,     A2,     LDA
    bnez        N4,     .L_M4_N4
.L_M4_N3:
    andi        N2,     N,      0x02
    beqz        N2,     .L_M4_N1
.L_M4_N2:
    GXOR xv, v, D0, D0, D0, D1, D1, D1
    move        K1,     K // Restore K1
    bge         ZERO,   K,      .L_M4_N2_END
.L_M4_N2_K1:
    PTR_ADDI    K1,     K1,    -1
    GLD xv, , S0, A0, 0x00, S1, A1, 0x00, S2, A2, 0x00, S3, A3, 0x00
    GINSVE0 xv, d, S0, S1, 1, S0, S2, 2, S0, S3, 3
    GLDREPL xv, d, Z0, X0, 0x00, Z1, B1, 0x00
    GMADD xvf, d, D0, S0, Z0, D0, \
                  D1, S0, Z1, D1
    PTR_ADDI    X0,     X0,     0x08
    PTR_ADDI    B1,     B1,     0x08
    PTR_ADDI    A0,     A0,     0x08
    PTR_ADDI    A1,     A1,     0x08
    PTR_ADDI    A2,     A2,     0x08
    PTR_ADDI    A3,     A3,     0x08
    bnez        K1,     .L_M4_N2_K1
.L_M4_N2_END:
    GMUL xvf, d, D0,  D0,  VALPHA, D1,  D1,  VALPHA
#ifndef B0
    GLD xv, , S0, C0, 0x00, S1, C1, 0x00
    GMADD xvf, d, D0, S0, VBETA, D0, D1, S1, VBETA, D1
#endif
    GST xv, , D1, C1, 0x00, \
              D0, C0, 0x00
    // Update C0, C1
    PTR_ALSL    C0,     LDC,    C0,     1
    PTR_ALSL    C1,     LDC,    C1,     1
    // Update X0, B1
    PTR_SUB     X0,     X0,    K8
    PTR_SUB     B1,     B1,    K8
    PTR_ALSL    X0,     LDB,    X0,     1
    PTR_ALSL    B1,     LDB,    B1,     1
    // Restore A0
    move        A0,     A
    PTR_ADD     A1,     A0,     LDA
    PTR_ADD     A2,     A1,     LDA
    PTR_ADD     A3,     A2,     LDA
.L_M4_N1:
    andi        N1,     N,      0x01
    beqz        N1,     .L_M4_END
    GXOR xv, v, D0, D0, D0
    move        K1,     K // Restore K1
    bge         ZERO,   K,      .L_M4_N1_END
.L_M4_N1_K1:
    PTR_ADDI    K1,     K1,    -1
    GLD xv, , S0, A0, 0x00, S1, A1, 0x00, S2, A2, 0x00, S3, A3, 0x00
    GINSVE0 xv, d, S0, S1, 1, S0, S2, 2, S0, S3, 3
    GLDREPL xv, d, Z0, X0, 0x00
    GMADD xvf, d, D0,  S0, Z0, D0
    PTR_ADDI    X0,     X0,     0x08
    PTR_ADDI    A0,     A0,     0x08
    PTR_ADDI    A1,     A1,     0x08
    PTR_ADDI    A2,     A2,     0x08
    PTR_ADDI    A3,     A3,     0x08
    bnez        K1,     .L_M4_N1_K1
.L_M4_N1_END:
    GMUL xvf, d, D0,  D0,  VALPHA
#ifndef B0
    GLD xv, , S0, C0, 0x00
    GMADD xvf, d, D0, S0, VBETA, D0
#endif
    GST xv, , D0,  C0, 0x00
    // Update C0
    PTR_ALSL    C0,     LDC,    C0,     2
    // Update X0
    PTR_SUB     X0,     X0,    K8
    PTR_ALSL    X0,     LDB,   X0,     2
    // Restore A0
    move        A0,     A
    PTR_ADD     A1,     A0,     LDA
    PTR_ADD     A2,     A1,     LDA
    PTR_ADD     A3,     A2,     LDA
.L_M4_END:
    PTR_ADDI    M4,     M4,   -1
    PTR_ALSL    A,      LDA,   A,     2 // A += LDA << 2;
    PTR_ADDI    C,      C,     0x20
    bnez        M4,    .L_M4
.L_M3:
    andi        M2,     M,     0x02
    beqz        M2,     .L_M1
.L_M2:
    PTR_SRAI    N4,     N,      2 // N >> 2
    move        A0,     A // Restore A0
    PTR_ADD     A1,     A0,     LDA
    move        X0,     B // Restore X0
    PTR_ADD     B1,     X0,     LDB
    PTR_ADD     B2,     B1,     LDB
    PTR_ADD     B3,     B2,     LDB
    move        C0,     C // Restore C0
    PTR_ADD     C1,     C0,     LDC
    PTR_ADD     C2,     C1,     LDC
    PTR_ADD     C3,     C2,     LDC
    beqz        N4,     .L_M2_N3
.L_M2_N4:
    GXOR xv, v, D0, D0, D0, D1, D1, D1, D2, D2, D2, D3, D3, D3
    move        K1,     K // Restore K1
    PTR_ADDI    N4,     N4,     -1
    bge         ZERO,   K,      .L_M2_N4_END
.L_M2_N4_K1:
    PTR_ADDI    K1,     K1,    -1
    GLD xv, , S0, A0, 0x00, S1, A1, 0x00
    GINSVE0 xv, d, S0, S1, 1
    GLDREPL xv, d, Z0, X0, 0x00, Z1, B1, 0x00, Z2, B2, 0x00, Z3, B3, 0x00
    GMADD xvf, d, D0, S0, Z0, D0, \
                  D1, S0, Z1, D1, \
                  D2, S0, Z2, D2, \
                  D3, S0, Z3, D3
    PTR_ADDI    X0,     X0,     0x08
    PTR_ADDI    B1,     B1,     0x08
    PTR_ADDI    B2,     B2,     0x08
    PTR_ADDI    B3,     B3,     0x08
    PTR_ADDI    A0,     A0,     0x08
    PTR_ADDI    A1,     A1,     0x08
    bnez        K1,     .L_M2_N4_K1
 .L_M2_N4_END:
    GMUL xvf, d, D0,  D0,  VALPHA, D1,  D1,  VALPHA, D2,  D2,  VALPHA, D3,  D3,  VALPHA
#ifndef B0
    GLD xv, , S0, C0, 0x00, S1, C1, 0x00, S2, C2, 0x00, S3, C3, 0x00
    GMADD xvf, d, D0, S0, VBETA, D0, D1, S1, VBETA, D1, D2, S2, VBETA, D2, D3, S3, VBETA, D3
#endif
    GST v, , V3, C3, 0x00, \
             V2, C2, 0x00, \
             V1, C1, 0x00, \
             V0, C0, 0x00
    // Update C0, C1, C2, C3
    PTR_ALSL    C0,     LDC,    C0,     2
    PTR_ALSL    C1,     LDC,    C1,     2
    PTR_ALSL    C2,     LDC,    C2,     2
    PTR_ALSL    C3,     LDC,    C3,     2
    // Update X0, B1, B2, B3
    PTR_SUB     X0,     X0,    K8
    PTR_SUB     B1,     B1,    K8
    PTR_SUB     B2,     B2,    K8
    PTR_SUB     B3,     B3,    K8

    PTR_ALSL    X0,     LDB,    X0,     2
    PTR_ALSL    B1,     LDB,    B1,     2
    PTR_ALSL    B2,     LDB,    B2,     2
    PTR_ALSL    B3,     LDB,    B3,     2
    // Restore A0, A1
    move        A0,     A
    PTR_ADD     A1,     A0,     LDA
    bnez        N4,     .L_M2_N4
.L_M2_N3:
    andi        N2,     N,      0x02
    beqz        N2,     .L_M2_N1
.L_M2_N2:
    GXOR xv, v, D0, D0, D0, D1, D1, D1
    move        K1,     K // Restore K1
    bge         ZERO,   K,      .L_M2_N2_END
.L_M2_N2_K1:
    PTR_ADDI    K1,     K1,    -1
    GLD xv, , S0, A0, 0x00, S1, A1, 0x00
    GINSVE0 xv, d, S0, S1, 1
    GLDREPL xv, d, Z0, X0, 0x00, Z1, B1, 0x00
    GMADD xvf, d, D0, S0, Z0, D0, \
                  D1, S0, Z1, D1
    PTR_ADDI    X0,     X0,     0x08
    PTR_ADDI    B1,     B1,     0x08
    PTR_ADDI    A0,     A0,     0x08
    PTR_ADDI    A1,     A1,     0x08
    bnez        K1,     .L_M2_N2_K1
.L_M2_N2_END:
    GMUL xvf, d, D0,  D0,  VALPHA, D1,  D1,  VALPHA
#ifndef B0
    GLD xv, , S0, C0, 0x00, S1, C1, 0x00
    GMADD xvf, d, D0, S0, VBETA, D0, D1, S1, VBETA, D1
#endif
    GST v, , V1, C1, 0x00, \
             V0, C0, 0x00
    // Update C0, C1
    PTR_ALSL    C0,     LDC,    C0,     1
    PTR_ALSL    C1,     LDC,    C1,     1
    // Update X0, B1
    PTR_SUB     X0,     X0,    K8
    PTR_SUB     B1,     B1,    K8
    PTR_ALSL    X0,     LDB,    X0,     1
    PTR_ALSL    B1,     LDB,    B1,     1
    // Restore A0, A1
    move        A0,     A
    PTR_ADD     A1,     A0,     LDA
.L_M2_N1:
    andi        N1,     N,      0x01
    beqz        N1,     .L_M2_END
    GXOR xv, v, D0, D0, D0
    move        K1,     K // Restore K1
    bge         ZERO,   K,      .L_M2_N1_END
.L_M2_N1_K1:
    PTR_ADDI    K1,     K1,    -1
    GLD xv, , S0, A0, 0x00, S1, A1, 0x00
    GINSVE0 xv, d, S0, S1, 1
    GLDREPL xv, d, Z0, X0, 0x00
    GMADD xvf, d, D0,  S0, Z0, D0
    PTR_ADDI    X0,     X0,     0x08
    PTR_ADDI    A0,     A0,     0x08
    PTR_ADDI    A1,     A1,     0x08
    bnez        K1,     .L_M2_N1_K1
.L_M2_N1_END:
    GMUL xvf, d, D0,  D0,  VALPHA
#ifndef B0
    GLD xv, , S0, C0, 0x00
    GMADD xvf, d, D0, S0, VBETA, D0
#endif
    GST v, , V0, C0, 0x00
    // Update C0
    PTR_ALSL    C0,     LDC,    C0,     2
    // Update X0
    PTR_SUB     X0,     X0,    K8
    PTR_ALSL    X0,     LDB,   X0,     2
    // Restore A0, A1
    move        A0,     A
    PTR_ADD     A1,     A0,     LDA
.L_M2_END:
    PTR_ALSL    A,     LDA,   A,     1 // A += LDA << 1;
    PTR_ADDI    C,     C,     0x10
.L_M1:
    andi        M1,     M,     0x01
    beqz        M1,     .L_M0

    PTR_SRAI    N4,     N,      2 // N >> 2
    move        A0,     A // Restore A0
    move        X0,     B // Restore X0
    PTR_ADD     B1,     X0,     LDB
    PTR_ADD     B2,     B1,     LDB
    PTR_ADD     B3,     B2,     LDB
    move        C0,     C // Restore C0
    PTR_ADD     C1,     C0,     LDC
    PTR_ADD     C2,     C1,     LDC
    PTR_ADD     C3,     C2,     LDC
    beqz        N4,     .L_M1_N3
.L_M1_N4:
    GXOR xv, v, D0, D0, D0, D1, D1, D1, D2, D2, D2, D3, D3, D3
    move        K1,     K // Restore K1
    PTR_ADDI    N4,     N4,     -1
    bge         ZERO,   K,      .L_M1_N4_END
.L_M1_N4_K1:
    PTR_ADDI    K1,     K1,    -1
    GLD xv, , S0, A0, 0x00
    GLDREPL xv, d, Z0, X0, 0x00, Z1, B1, 0x00, Z2, B2, 0x00, Z3, B3, 0x00
    GMADD xvf, d, D0, S0, Z0, D0, \
                  D1, S0, Z1, D1, \
                  D2, S0, Z2, D2, \
                  D3, S0, Z3, D3
    PTR_ADDI    X0,     X0,     0x08
    PTR_ADDI    B1,     B1,     0x08
    PTR_ADDI    B2,     B2,     0x08
    PTR_ADDI    B3,     B3,     0x08
    PTR_ADDI    A0,     A0,     0x08
    bnez        K1,     .L_M1_N4_K1
 .L_M1_N4_END:
    GMUL xvf, d, D0,  D0,  VALPHA, D1,  D1,  VALPHA, D2,  D2,  VALPHA, D3,  D3,  VALPHA
#ifndef B0
    GLD xv, , S0, C0, 0x00, S1, C1, 0x00, S2, C2, 0x00, S3, C3, 0x00
    GMADD xvf, d, D0, S0, VBETA, D0, D1, S1, VBETA, D1, D2, S2, VBETA, D2, D3, S3, VBETA, D3
#endif
    GST f, d, F3, C3, 0x00, \
              F2, C2, 0x00, \
              F1, C1, 0x00, \
              F0, C0, 0x00
    // Update C0, C1, C2, C3
    PTR_ALSL    C0,     LDC,    C0,     2
    PTR_ALSL    C1,     LDC,    C1,     2
    PTR_ALSL    C2,     LDC,    C2,     2
    PTR_ALSL    C3,     LDC,    C3,     2
    // Update X0, B1, B2, B3
    PTR_SUB     X0,     X0,    K8
    PTR_SUB     B1,     B1,    K8
    PTR_SUB     B2,     B2,    K8
    PTR_SUB     B3,     B3,    K8

    PTR_ALSL    X0,     LDB,    X0,     2
    PTR_ALSL    B1,     LDB,    B1,     2
    PTR_ALSL    B2,     LDB,    B2,     2
    PTR_ALSL    B3,     LDB,    B3,     2
    // Restore A0, A1
    move        A0,     A
    bnez        N4,     .L_M1_N4
.L_M1_N3:
    andi        N2,     N,      0x02
    beqz        N2,     .L_M1_N1
.L_M1_N2:
    GXOR xv, v, D0, D0, D0, D1, D1, D1
    move        K1,     K // Restore K1
    bge         ZERO,   K,      .L_M1_N2_END
.L_M1_N2_K1:
    PTR_ADDI    K1,     K1,    -1
    GLD xv, , S0, A0, 0x00
    GLDREPL xv, d, Z0, X0, 0x00, Z1, B1, 0x00
    GMADD xvf, d, D0, S0, Z0, D0, \
                  D1, S0, Z1, D1
    PTR_ADDI    X0,     X0,     0x08
    PTR_ADDI    B1,     B1,     0x08
    PTR_ADDI    A0,     A0,     0x08
    bnez        K1,     .L_M1_N2_K1
.L_M1_N2_END:
    GMUL xvf, d, D0,  D0,  VALPHA, D1,  D1,  VALPHA
#ifndef B0
    GLD xv, , S0, C0, 0x00, S1, C1, 0x00
    GMADD xvf, d, D0, S0, VBETA, D0, D1, S1, VBETA, D1
#endif
    GST f, d, F1, C1, 0x00, \
              F0, C0, 0x00
    // Update C0, C1
    PTR_ALSL    C0,     LDC,    C0,     1
    PTR_ALSL    C1,     LDC,    C1,     1
    // Update X0, B1
    PTR_SUB     X0,     X0,    K8
    PTR_SUB     B1,     B1,    K8
    PTR_ALSL    X0,     LDB,    X0,     1
    PTR_ALSL    B1,     LDB,    B1,     1
    // Restore A0
    move        A0,     A
.L_M1_N1:
    andi        N1,     N,      0x01
    beqz        N1,     .L_M0
    GXOR xv, v, D0, D0, D0
    move        K1,     K // Restore K1
    bge         ZERO,   K,      .L_M1_N1_END
.L_M1_N1_K1:
    PTR_ADDI    K1,     K1,    -1
    GLD xv, , S0, A0, 0x00
    GLDREPL xv, d, Z0, X0, 0x00
    GMADD xvf, d, D0,  S0, Z0, D0
    PTR_ADDI    X0,     X0,     0x08
    PTR_ADDI    A0,     A0,     0x08
    bnez        K1,     .L_M1_N1_K1
.L_M1_N1_END:
    GMUL xvf, d, D0,  D0,  VALPHA
#ifndef B0
    GLD xv, , S0, C0, 0x00
    GMADD xvf, d, D0, S0, VBETA, D0
#endif
    GST f, d, F0, C0, 0x00
    // Update C0
    PTR_ALSL    C0,     LDC,    C0,     2
    // Update X0
    PTR_SUB     X0,     X0,    K8
    PTR_ALSL    X0,     LDB,   X0,     2
    // Restore A0
    move        A0,     A
.L_M0:
    pop_if_used  8, 2
    jirl    $r0, $r1, 0x0
    EPILOGUE
