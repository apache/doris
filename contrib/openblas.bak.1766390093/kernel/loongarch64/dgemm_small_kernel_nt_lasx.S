/***************************************************************************
Copyright (c) 2024 The OpenBLAS Project
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

#define M16     $t1
#define M8      $t1
#define M4      $t1
#define M2      $t1
#define M1      $t1
#define N4      $t2
#define N2      $t2
#define N1      $t2
#define K_LDB   $t3
#define A0      $t4
#define X0      $t5
#define C0      $t6
#define C1      $t7
#define C2      $t8
#define C3      $s0
#define K1      $s1

#define VALPHA  $xr0
#ifndef B0
#define VBETA   $xr1
#endif
#define D0     $xr2
#define D1     $xr3
#define D2     $xr4
#define D3     $xr5
#define D4     $xr6
#define D5     $xr7
#define D6     $xr8
#define D7     $xr9
#define D8     $xr10
#define D9     $xr11
#define D10    $xr12
#define D11    $xr13
#define D12    $xr14
#define D13    $xr15
#define D14    $xr16
#define D15    $xr17
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

.macro DGEMM_SMALL_KERNEL_NT_TAIL M
    PTR_SRAI    N4,     N,      2 // N >> 2
    move        A0,     A // Restore A0
    move        X0,     B // Restore X0
    move        C0,     C // Restore C0
    PTR_ADD     C1,     C0,     LDC
    PTR_ADD     C2,     C1,     LDC
    PTR_ADD     C3,     C2,     LDC
    beqz        N4,     .L_M\M\()_N3
.L_M\M\()_N4:
    GXOR xv, v, D0, D0, D0, D1, D1, D1, D2, D2, D2, D3, D3, D3
    move        K1,     K // Restore K1
    PTR_ADDI    N4,     N4,     -1
    bge         ZERO,   K,      .L_M\M\()_N4_END
.L_M\M\()_N4_K1:
    PTR_ADDI    K1,     K1,    -1
    GLD xv, , S0, A0, 0x00
    GLDREPL xv, d, Z0, X0, 0x00, Z1, X0, 0x08, Z2, X0, 0x10, Z3, X0, 0x18
    GMADD xvf, d, D0, S0, Z0, D0, D1, S0, Z1, D1, D2, S0, Z2, D2, D3, S0, Z3, D3
    PTR_ADD     X0,     X0,     LDB
    PTR_ADD     A0,     A0,     LDA
    bnez        K1,     .L_M\M\()_N4_K1
.L_M\M\()_N4_END:
    GMUL xvf, d, D0, D0, VALPHA, D1,  D1, VALPHA, D2, D2, VALPHA, D3, D3, VALPHA
#ifndef B0
    GLD xv, , S0, C0, 0x00
    GMADD xvf, d, D0, S0, VBETA, D0
    GLD xv, , S0, C1, 0x00
    GMADD xvf, d, D1, S0, VBETA, D1
    GLD xv, , S0, C2, 0x00
    GMADD xvf, d, D2, S0, VBETA, D2
    GLD xv, , S0, C3, 0x00
    GMADD xvf, d, D3, S0, VBETA, D3
#endif
.if \M == 4
    GST xv, , D0, C0, 0x00, D1, C1, 0x00, D2, C2, 0x00, D3, C3, 0x00
.elseif \M == 2
    GST v, , V0, C0, 0x00, V1, C1, 0x00, V2, C2, 0x00, V3, C3, 0x00
.elseif \M == 1
    GST f, d, F0, C0, 0x00, F1, C1, 0x00, F2, C2, 0x00, F3, C3, 0x00
.endif
    // Update C0, C1, C2, C3
    PTR_ALSL    C0,     LDC,    C0,     2
    PTR_ALSL    C1,     LDC,    C1,     2
    PTR_ALSL    C2,     LDC,    C2,     2
    PTR_ALSL    C3,     LDC,    C3,     2
    // Update X0
    PTR_SUB     X0,     X0,     K_LDB
    PTR_ADDI    X0,     X0,     0x20
    // Restore A0
    move        A0,     A
    bnez        N4,     .L_M\M\()_N4
.L_M\M\()_N3:
    andi        N2,     N,      0x02
    beqz        N2,     .L_M\M\()_N1
.L_M\M\()_N2:
    GXOR xv, v, D0, D0, D0, D1, D1, D1
    move        K1,     K // Restore K1
    bge         ZERO,   K,      .L_M\M\()_N2_END
.L_M\M\()_N2_K1:
    PTR_ADDI    K1,     K1,    -1
    GLD xv, , S0, A0, 0x00
    GLDREPL xv, d, Z0, X0, 0x00, Z1, X0, 0x08
    GMADD xvf, d, D0, S0, Z0, D0, D1, S0, Z1, D1
    PTR_ADD     X0,     X0,     LDB
    PTR_ADD     A0,     A0,     LDA
    bnez        K1,     .L_M\M\()_N2_K1
.L_M\M\()_N2_END:
    GMUL xvf, d, D0,  D0, VALPHA, D1, D1, VALPHA
#ifndef B0
    GLD xv, , S0, C0, 0x00
    GMADD xvf, d, D0, S0, VBETA, D0
    GLD xv, , S0, C1, 0x00
    GMADD xvf, d, D1, S0, VBETA, D1
#endif
.if \M == 4
    GST xv, , D0, C0, 0x00, D1, C1, 0x00
.elseif \M == 2
    GST v, , V0, C0, 0x00, V1, C1, 0x00
.elseif \M == 1
    GST f, d, F0, C0, 0x00, F1, C1, 0x00
.endif
    // Update C0, C1
    PTR_ALSL    C0,     LDC,    C0,     1
    PTR_ALSL    C1,     LDC,    C1,     1
    // Update X0
    PTR_SUB     X0,     X0,     K_LDB
    PTR_ADDI    X0,     X0,     0x10
    // Restore A0
    move        A0,     A
.L_M\M\()_N1:
    andi        N1,     N,      0x01
    beqz        N1,     .L_M\M\()_END
    GXOR xv, v, D0, D0, D0
    move        K1,     K // Restore K1
    bge         ZERO,   K,      .L_M\M\()_N1_END
.L_M\M\()_N1_K1:
    PTR_ADDI    K1,     K1,    -1
    GLD xv, , S0, A0, 0x00
    GLDREPL xv, d, Z0, X0, 0x00
    GMADD xvf, d, D0, S0, Z0, D0
    PTR_ADD     X0,     X0,     LDB
    PTR_ADD     A0,     A0,     LDA
    bnez        K1,     .L_M\M\()_N1_K1
.L_M\M\()_N1_END:
    GMUL xvf, d, D0, D0, VALPHA
#ifndef B0
    GLD xv, , S0, C0, 0x00
    GMADD xvf, d, D0, S0, VBETA, D0
#endif
.if \M == 4
    GST xv, , D0, C0, 0x00
.elseif \M == 2
    GST v, , V0, C0, 0x00
.elseif \M == 1
    GST f, d, F0, C0, 0x00
.endif
.L_M\M\()_END:
.if \M == 4
    PTR_ADDI    A,      A,     0x20
    PTR_ADDI    C,      C,     0x20
.elseif \M == 2
    PTR_ADDI    A,      A,     0x10
    PTR_ADDI    C,      C,     0x10
.elseif \M == 1
.endif
.endm

    PROLOGUE
    PTR_LD          LDC,    $sp,    0
    push_if_used    2, 2
    xvreplve0.d     VALPHA, VALPHA
#ifndef B0
    xvreplve0.d     VBETA,  VBETA
#endif
    PTR_SLLI    LDA,    LDA,    3
    PTR_SLLI    LDB,    LDB,    3
    PTR_SLLI    LDC,    LDC,    3
    PTR_MUL     K_LDB,  K,      LDB
    PTR_SRAI    M16,    M,      4 // M >> 4
    beqz        M16,    .L_M15
.L_M16:
    PTR_SRAI    N4,     N,      2 // N >> 2
    move        A0,     A // Restore A0
    move        X0,     B // Restore X0
    move        C0,     C // Restore C0
    PTR_ADD     C1,     C0,     LDC
    PTR_ADD     C2,     C1,     LDC
    PTR_ADD     C3,     C2,     LDC
    beqz        N4,     .L_M16_N3
.L_M16_N4:
    GXOR xv, v, D0, D0, D0, D1, D1, D1, D2, D2, D2, D3, D3, D3,       \
                D4, D4, D4, D5, D5, D5, D6, D6, D6, D7, D7, D7,       \
                D8, D8, D8, D9, D9, D9, D10, D10, D10, D11, D11, D11, \
                D12, D12, D12, D13, D13, D13, D14, D14, D14, D15, D15, D15
    move        K1,     K // Restore K1
    PTR_ADDI    N4,     N4,     -1
    bge         ZERO,   K,      .L_M16_N4_END
.L_M16_N4_K1:
    PTR_ADDI    K1,     K1,    -1
    GLD xv, , S0, A0, 0x00, S1, A0, 0x20, S2, A0, 0x40, S3, A0, 0x60
    GLDREPL xv, d, Z0, X0, 0x00, Z1, X0, 0x08, Z2, X0, 0x10, Z3, X0, 0x18
    GMADD xvf, d, D0,  S0, Z0, D0,  D1,  S1, Z0, D1,  D2,  S2, Z0, D2,  D3,  S3, Z0, D3,  \
                  D4,  S0, Z1, D4,  D5,  S1, Z1, D5,  D6,  S2, Z1, D6,  D7,  S3, Z1, D7,  \
                  D8,  S0, Z2, D8,  D9,  S1, Z2, D9,  D10, S2, Z2, D10, D11, S3, Z2, D11, \
                  D12, S0, Z3, D12, D13, S1, Z3, D13, D14, S2, Z3, D14, D15, S3, Z3, D15
    PTR_ADD     X0,     X0,     LDB
    PTR_ADD     A0,     A0,     LDA
    bnez        K1,     .L_M16_N4_K1
 .L_M16_N4_END:
    GMUL xvf, d, D0,  D0,  VALPHA, D1,  D1,  VALPHA, D2,  D2,  VALPHA, D3,  D3,  VALPHA, \
                 D4,  D4,  VALPHA, D5,  D5,  VALPHA, D6,  D6,  VALPHA, D7,  D7,  VALPHA, \
                 D8,  D8,  VALPHA, D9,  D9,  VALPHA, D10, D10, VALPHA, D11, D11, VALPHA, \
                 D12, D12, VALPHA, D13, D13, VALPHA, D14, D14, VALPHA, D15, D15, VALPHA
#ifndef B0
    GLD xv, , S0, C0, 0x00, S1, C0, 0x20, S2, C0, 0x40, S3, C0, 0x60
    GMADD xvf, d, D0, S0, VBETA, D0, D1, S1, VBETA, D1, D2, S2, VBETA, D2, D3, S3, VBETA, D3
    GLD xv, , S0, C1, 0x00, S1, C1, 0x20, S2, C1, 0x40, S3, C1, 0x60
    GMADD xvf, d, D4, S0, VBETA, D4, D5, S1, VBETA, D5, D6, S2, VBETA, D6, D7, S3, VBETA, D7
    GLD xv, , S0, C2, 0x00, S1, C2, 0x20, S2, C2, 0x40, S3, C2, 0x60
    GMADD xvf, d, D8, S0, VBETA, D8, D9, S1, VBETA, D9, D10, S2, VBETA, D10, D11, S3, VBETA, D11
    GLD xv, , S0, C3, 0x00, S1, C3, 0x20, S2, C3, 0x40, S3, C3, 0x60
    GMADD xvf, d, D12, S0, VBETA, D12, D13, S1, VBETA, D13, D14, S2, VBETA, D14, D15, S3, VBETA, D15
#endif
    GST xv, , D12, C3, 0x00, D13, C3, 0x20, D14, C3, 0x40, D15, C3, 0x60, \
              D8,  C2, 0x00, D9,  C2, 0x20, D10, C2, 0x40, D11, C2, 0x60, \
              D4,  C1, 0x00, D5,  C1, 0x20, D6,  C1, 0x40, D7,  C1, 0x60, \
              D0,  C0, 0x00, D1,  C0, 0x20, D2,  C0, 0x40, D3,  C0, 0x60
    // Update C0, C1, C2, C3
    PTR_ALSL    C0,     LDC,    C0,     2
    PTR_ALSL    C1,     LDC,    C1,     2
    PTR_ALSL    C2,     LDC,    C2,     2
    PTR_ALSL    C3,     LDC,    C3,     2
    // Update X0
    PTR_SUB     X0,     X0,     K_LDB
    PTR_ADDI    X0,     X0,     0x20
    // Restore A0
    move        A0,     A
    bnez        N4,     .L_M16_N4
.L_M16_N3:
    andi        N2,     N,      0x02
    beqz        N2,     .L_M16_N1
.L_M16_N2:
    GXOR xv, v, D0, D0, D0, D1, D1, D1, D2, D2, D2, D3, D3, D3,       \
                D4, D4, D4, D5, D5, D5, D6, D6, D6, D7, D7, D7
    move        K1,     K // Restore K1
    bge         ZERO,   K,      .L_M16_N2_END
.L_M16_N2_K1:
    PTR_ADDI    K1,     K1,    -1
    GLD xv, , S0, A0, 0x00, S1, A0, 0x20, S2, A0, 0x40, S3, A0, 0x60
    GLDREPL xv, d, Z0, X0, 0x00, Z1, X0, 0x08
    GMADD xvf, d, D0,  S0, Z0, D0,  D1,  S1, Z0, D1,  D2,  S2, Z0, D2,  D3,  S3, Z0, D3, \
                  D4,  S0, Z1, D4,  D5,  S1, Z1, D5,  D6,  S2, Z1, D6,  D7,  S3, Z1, D7
    PTR_ADD     X0,     X0,     LDB
    PTR_ADD     A0,     A0,     LDA
    bnez        K1,     .L_M16_N2_K1
.L_M16_N2_END:
    GMUL xvf, d, D0,  D0,  VALPHA, D1,  D1,  VALPHA, D2,  D2,  VALPHA, D3,  D3,  VALPHA, \
                 D4,  D4,  VALPHA, D5,  D5,  VALPHA, D6,  D6,  VALPHA, D7,  D7,  VALPHA
#ifndef B0
    GLD xv, , S0, C0, 0x00, S1, C0, 0x20, S2, C0, 0x40, S3, C0, 0x60
    GMADD xvf, d, D0, S0, VBETA, D0, D1, S1, VBETA, D1, D2, S2, VBETA, D2, D3, S3, VBETA, D3
    GLD xv, , S0, C1, 0x00, S1, C1, 0x20, S2, C1, 0x40, S3, C1, 0x60
    GMADD xvf, d, D4, S0, VBETA, D4, D5, S1, VBETA, D5, D6, S2, VBETA, D6, D7, S3, VBETA, D7
#endif
    GST xv, , D4,  C1, 0x00, D5,  C1, 0x20, D6,  C1, 0x40, D7,  C1, 0x60, \
              D0,  C0, 0x00, D1,  C0, 0x20, D2,  C0, 0x40, D3,  C0, 0x60
    // Update C0, C1
    PTR_ALSL    C0,     LDC,    C0,     1
    PTR_ALSL    C1,     LDC,    C1,     1
    // Update X0
    PTR_SUB     X0,     X0,    K_LDB
    PTR_ADDI    X0,     X0,    0x10
    // Restore A0
    move        A0,     A
.L_M16_N1:
    andi        N1,     N,      0x01
    beqz        N1,     .L_M16_END
    GXOR xv, v, D0, D0, D0, D1, D1, D1, D2, D2, D2, D3, D3, D3
    move        K1,     K // Restore K1
    bge         ZERO,   K,      .L_M16_N1_END
.L_M16_N1_K1:
    PTR_ADDI    K1,     K1,    -1
    GLD xv, , S0, A0, 0x00, S1, A0, 0x20, S2, A0, 0x40, S3, A0, 0x60
    GLDREPL xv, d, Z0, X0, 0x00
    GMADD xvf, d, D0,  S0, Z0, D0,  D1,  S1, Z0, D1,  D2,  S2, Z0, D2,  D3,  S3, Z0, D3
    PTR_ADD     X0,     X0,     LDB
    PTR_ADD     A0,     A0,     LDA
    bnez        K1,     .L_M16_N1_K1
.L_M16_N1_END:
    GMUL xvf, d, D0,  D0,  VALPHA, D1,  D1,  VALPHA, D2,  D2,  VALPHA, D3,  D3,  VALPHA
#ifndef B0
    GLD xv, , S0, C0, 0x00, S1, C0, 0x20, S2, C0, 0x40, S3, C0, 0x60
    GMADD xvf, d, D0, S0, VBETA, D0, D1, S1, VBETA, D1, D2, S2, VBETA, D2, D3, S3, VBETA, D3
#endif
    GST xv, , D0,  C0, 0x00, D1,  C0, 0x20, D2,  C0, 0x40, D3,  C0, 0x60
    // Update C0
    PTR_ALSL    C0,     LDC,    C0,     2
    // Update X0
    PTR_SUB     X0,     X0,     K_LDB
    PTR_ADDI    X0,     X0,     0x08
    // Restore A0
    move        A0,     A
.L_M16_END:
    PTR_ADDI    M16,    M16,   -1
    PTR_ADDI    A,      A,     0x80
    PTR_ADDI    C,      C,     0x80
    bnez        M16,    .L_M16
.L_M15:
    andi        M8,     M,      0x08
    beqz        M8,     .L_M7
.L_M8:
    PTR_SRAI    N4,     N,      2 // N >> 2
    move        A0,     A // Restore A0
    move        X0,     B // Restore X0
    move        C0,     C // Restore C0
    PTR_ADD     C1,     C0,     LDC
    PTR_ADD     C2,     C1,     LDC
    PTR_ADD     C3,     C2,     LDC
    beqz        N4,     .L_M8_N3
.L_M8_N4:
    GXOR xv, v, D0, D0, D0, D1, D1, D1, D2, D2, D2, D3, D3, D3, \
                D4, D4, D4, D5, D5, D5, D6, D6, D6, D7, D7, D7
    move        K1,     K // Restore K1
    PTR_ADDI    N4,     N4,     -1
    bge         ZERO,   K,      .L_M8_N4_END
.L_M8_N4_K1:
    PTR_ADDI    K1,     K1,    -1
    GLD xv, , S0, A0, 0x00, S1, A0, 0x20
    GLDREPL xv, d, Z0, X0, 0x00, Z1, X0, 0x08, Z2, X0, 0x10, Z3, X0, 0x18
    GMADD xvf, d, D0, S0, Z0, D0, D1, S1, Z0, D1, \
                  D2, S0, Z1, D2, D3, S1, Z1, D3, \
                  D4, S0, Z2, D4, D5, S1, Z2, D5, \
                  D6, S0, Z3, D6, D7, S1, Z3, D7,
    PTR_ADD     X0,     X0,     LDB
    PTR_ADD     A0,     A0,     LDA
    bnez        K1,     .L_M8_N4_K1
.L_M8_N4_END:
    GMUL xvf, d, D0,  D0,  VALPHA, D1,  D1,  VALPHA, D2,  D2,  VALPHA, D3,  D3,  VALPHA, \
                 D4,  D4,  VALPHA, D5,  D5,  VALPHA, D6,  D6,  VALPHA, D7,  D7,  VALPHA
#ifndef B0
    GLD xv, , S0, C0, 0x00, S1, C0, 0x20
    GMADD xvf, d, D0, S0, VBETA, D0, D1, S1, VBETA, D1
    GLD xv, , S0, C1, 0x00, S1, C1, 0x20
    GMADD xvf, d, D2, S0, VBETA, D2, D3, S1, VBETA, D3
    GLD xv, , S0, C2, 0x00, S1, C2, 0x20
    GMADD xvf, d, D4, S0, VBETA, D4, D5, S1, VBETA, D5
    GLD xv, , S0, C3, 0x00, S1, C3, 0x20
    GMADD xvf, d, D6, S0, VBETA, D6, D7, S1, VBETA, D7
#endif
    GST xv, , D4,  C2, 0x00, D5,  C2, 0x20, D6,  C3, 0x00, D7,  C3, 0x20, \
              D0,  C0, 0x00, D1,  C0, 0x20, D2,  C1, 0x00, D3,  C1, 0x20
    // Update C0, C1, C2, C3
    PTR_ALSL    C0,     LDC,    C0,     2
    PTR_ALSL    C1,     LDC,    C1,     2
    PTR_ALSL    C2,     LDC,    C2,     2
    PTR_ALSL    C3,     LDC,    C3,     2
    // Update X0
    PTR_SUB     X0,     X0,     K_LDB
    PTR_ADDI    X0,     X0,     0x20
    // Restore A0
    move        A0,     A
    bnez        N4,     .L_M8_N4
.L_M8_N3:
    andi        N2,     N,      0x02
    beqz        N2,     .L_M8_N1
.L_M8_N2:
    GXOR xv, v, D0, D0, D0, D1, D1, D1, D2, D2, D2, D3, D3, D3
    move        K1,     K // Restore K1
    bge         ZERO,   K,      .L_M8_N2_END
.L_M8_N2_K1:
    PTR_ADDI    K1,     K1,    -1
    GLD xv, , S0, A0, 0x00, S1, A0, 0x20
    GLDREPL xv, d, Z0, X0, 0x00, Z1, X0, 0x08
    GMADD xvf, d, D0, S0, Z0, D0, D1, S1, Z0, D1, \
                  D2, S0, Z1, D2, D3, S1, Z1, D3
    PTR_ADD     X0,     X0,     LDB
    PTR_ADD     A0,     A0,     LDA
    bnez        K1,     .L_M8_N2_K1
.L_M8_N2_END:
    GMUL xvf, d, D0,  D0,  VALPHA, D1,  D1,  VALPHA, D2,  D2,  VALPHA, D3,  D3,  VALPHA
#ifndef B0
    GLD xv, , S0, C0, 0x00, S1, C0, 0x20
    GMADD xvf, d, D0, S0, VBETA, D0, D1, S1, VBETA, D1
    GLD xv, , S0, C1, 0x00, S1, C1, 0x20
    GMADD xvf, d, D2, S0, VBETA, D2, D3, S1, VBETA, D3
#endif
    GST xv, , D0,  C0, 0x00, D1,  C0, 0x20, D2,  C1, 0x00, D3,  C1, 0x20
    // Update C0, C1
    PTR_ALSL    C0,     LDC,    C0,     1
    PTR_ALSL    C1,     LDC,    C1,     1
    // Update X0
    PTR_SUB     X0,     X0,     K_LDB
    PTR_ADDI    X0,     X0,     0x10
    // Restore A0
    move        A0,     A
.L_M8_N1:
    andi        N1,     N,      0x01
    beqz        N1,     .L_M8_END
    GXOR xv, v, D0, D0, D0, D1, D1, D1
    move        K1,     K // Restore K1
    bge         ZERO,   K,      .L_M8_N1_END
.L_M8_N1_K1:
    PTR_ADDI    K1,     K1,    -1
    GLD xv, , S0, A0, 0x00, S1, A0, 0x20
    GLDREPL xv, d, Z0, X0, 0x00
    GMADD xvf, d, D0, S0, Z0, D0, D1, S1, Z0, D1
    PTR_ADD     X0,     X0,     LDB
    PTR_ADD     A0,     A0,     LDA
    bnez        K1,     .L_M8_N1_K1
.L_M8_N1_END:
    GMUL xvf, d, D0,  D0,  VALPHA, D1,  D1,  VALPHA
#ifndef B0
    GLD xv, , S0, C0, 0x00, S1, C0, 0x20
    GMADD xvf, d, D0, S0, VBETA, D0, D1, S1, VBETA, D1
#endif
    GST xv, , D0, C0, 0x00, D1, C0, 0x20
.L_M8_END:
    PTR_ADDI    A,      A,     0x40
    PTR_ADDI    C,      C,     0x40
.L_M7:
    andi        M4,     M,     0x04
    beqz        M4,     .L_M3
.L_M4:
    DGEMM_SMALL_KERNEL_NT_TAIL 4
.L_M3:
    andi        M2,     M,     0x02
    beqz        M2,     .L_M1
.L_M2:
    DGEMM_SMALL_KERNEL_NT_TAIL 2
.L_M1:
    andi        M1,     M,     0x01
    beqz        M1,     .L_M0
    DGEMM_SMALL_KERNEL_NT_TAIL 1
.L_M0:
    pop_if_used  2, 2
    jirl    $r0, $r1, 0x0
    EPILOGUE
