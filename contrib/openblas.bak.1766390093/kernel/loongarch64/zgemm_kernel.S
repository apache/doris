/***************************************************************************
Copyright (c) 2020, The OpenBLAS Project
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

#define M      $r4
#define N      $r5
#define K      $r6
#define A      $r7
#define B      $r8
#define C      $r9
#define LDC    $r10

#define AO     $r12
#define BO     $r13
#define I      $r17
#define J      $r18
#define L      $r25
#define CO1    $r14
#define CO2    $r15
#define CO3    $r23
#define CO4    $r24

#if defined(TRMMKERNEL)
#define OFFSET $r11
#define KK     $r26
#define TEMP   $r27
#endif

#define a1     $f22
#define a2     $f8
#define a3     $f28
#define a4     $f29
#define b1     $f23
#define b2     $f9
#define b3     $f10
#define b4     $f11
#define b5     $f12
#define b6     $f13
#define b7     $f14
#define b8     $f15
#define a5     b8
#define c11    $f16
#define c12    $f17
#define c21    $f3
#define c22    $f4
#define c31    $f2
#define c32    $f5
#define c41    $f6
#define c42    $f7
#define c51    $f18
#define c52    $f19
#define c61    $f20
#define c62    $f21
#define c71    $f24
#define c72    $f25
#define c81    $f26
#define c82    $f27
#define ALPHA_R    $f0
#define ALPHA_I    $f1

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
#define    MADD1       MADD
#define    MADD2       MADD
#define    MADD3       MADD
#define    MADD4       NMSUB
#endif

#if   defined(NR) || defined(NC) || defined(TR) || defined(TC)
#define    MADD1       MADD
#define    MADD2       MADD
#define    MADD3       NMSUB
#define    MADD4       MADD
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

   addi.d  $sp, $sp,   -128
   SDARG  $r23,  $sp,  0
   SDARG  $r24,  $sp,  8
   SDARG  $r25,  $sp,  64
   fst.d  $f24,  $sp,  16
   fst.d  $f25,  $sp,  24
   fst.d  $f26,  $sp,  32
   fst.d  $f27,  $sp,  40
   fst.d  $f28,  $sp,  48
   fst.d  $f29,  $sp,  56
#if defined(TRMMKERNEL)
   SDARG  $r26,  $sp,   72
   SDARG  $r27,  $sp,   80
#endif
#ifndef __64BIT__
   fst.d  $f18,  $sp,  88
   fst.d  $f19,  $sp,  96
   fst.d  $f20,  $sp, 104
   fst.d  $f21,  $sp, 112
#endif
   slli.d     LDC,    LDC, ZBASE_SHIFT
#if defined(TRMMKERNEL) && !defined(LEFT)
   sub.d   KK, $r0, OFFSET
#endif
   srai.d  J,  N, 2
nop
   bge $r0,    J, .L20
.L10:
   move    CO1, C
   MTC  c11, $r0
   add.d   CO2, C,      LDC
   move    AO, A
   add.d   CO3, CO2,    LDC
   addi.d  J, J, -1
   add.d   CO4, CO3,    LDC
   MOV c21, c11
   MOV c31, c11
#if defined(TRMMKERNEL) &&  defined(LEFT)
   move    KK, OFFSET
#endif
   MOV c41, c11
   MOV c51, c11
   move    I,  M
   add.d   C,   CO4,    LDC
   MOV    c61, c11
   bge $r0,    I, .L19
.L11:
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
   move    BO,  B
#else
   slli.d  L,    KK,  ZBASE_SHIFT
   slli.d  TEMP, KK, 2 + ZBASE_SHIFT
   add.d   AO, AO, L
   add.d   BO, B,  TEMP
#endif
   LD a1,  AO,   0 * SIZE
   MOV c71, c11
   LD b1,  BO,   0 * SIZE
   MOV c81, c11
   LD a3,  AO,   4 * SIZE
   MOV c12, c11
   LD b2,  BO,   1 * SIZE
   MOV c22, c11
   MOV c32, c11
   LD b3,  BO,   2 * SIZE
   MOV c42, c11
   LD b4,  BO,   3 * SIZE
   MOV c52, c11
   LD b5,  BO,   4 * SIZE
   MOV c62, c11
   LD b6,  BO,   8 * SIZE
   MOV c72, c11
   LD b7,  BO,  12 * SIZE
   MOV c82, c11
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
   sub.d   TEMP, K, KK
#elif defined(LEFT)
   addi.d  TEMP, KK, 1
#else
   addi.d  TEMP, KK, 4
#endif
   srai.d  L,  TEMP, 2
   bge $r0,    L, .L15
#else
   LD a1,  AO,   0 * SIZE
   MOV c71, c11
   LD b1,  B,   0 * SIZE
   MOV c81, c11
   LD a3,  AO,   4 * SIZE
   MOV c12, c11
   LD b2,  B,   1 * SIZE
   MOV c22, c11
   srai.d  L,  K, 2
   MOV c32, c11
   LD b3,  B,   2 * SIZE
   MOV c42, c11
   LD b4,  B,   3 * SIZE
   MOV c52, c11
   LD b5,  B,   4 * SIZE
   MOV c62, c11
   LD b6,  B,   8 * SIZE
   MOV c72, c11
   LD b7,  B,  12 * SIZE
   MOV c82, c11
move   BO,  B
   bge $r0,    L, .L15
#endif
   MADD1  c11, b1, a1, c11
   LD a2,  AO,   1 * SIZE
   MADD3  c21, b2, a1, c21
   addi.d  L, L, -1
   MADD1  c31, b3, a1, c31
   MADD3  c41, b4, a1, c41
   bge $r0,    L, .L13
   .align  3
.L12:
   MADD2  c12, b1, a2, c12
   LD b1,  BO,  16 * SIZE
   MADD4  c22, b2, a2, c22
   LD b2,  BO,   5 * SIZE
   MADD2  c32, b3, a2, c32
   LD b3,  BO,   6 * SIZE
   MADD4  c42, b4, a2, c42
   LD b4,  BO,   7 * SIZE
   MADD1  c51, b5, a1, c51
   MADD3  c61, b2, a1, c61
   LD a4,  AO,   2 * SIZE
   MADD1  c71, b3, a1, c71
   MADD3  c81, b4, a1, c81
   LD a1,  AO,   8 * SIZE
   MADD2  c52, b5, a2, c52
   LD b5,  BO,  20 * SIZE
   MADD4  c62, b2, a2, c62
   LD b2,  BO,   9 * SIZE
   MADD2  c72, b3, a2, c72
   LD b3,  BO,  10 * SIZE
   MADD4  c82, b4, a2, c82
   LD b4,  BO,  11 * SIZE
   MADD1  c11, b6, a4, c11
   LD a2,  AO,   3 * SIZE
   MADD3  c21, b2, a4, c21
   MADD1  c31, b3, a4, c31
   MADD3  c41, b4, a4, c41
   MADD2  c12, b6, a2, c12
   LD b6,  BO,  24 * SIZE
   MADD4  c22, b2, a2, c22
   LD b2,  BO,  13 * SIZE
   MADD2  c32, b3, a2, c32
   LD b3,  BO,  14 * SIZE
   MADD4  c42, b4, a2, c42
   LD b4,  BO,  15 * SIZE
   MADD1  c51, b7, a4, c51
   MADD3  c61, b2, a4, c61
   MADD1  c71, b3, a4, c71
   MADD3  c81, b4, a4, c81
   MADD2  c52, b7, a2, c52
   LD b7,  BO,  28 * SIZE
   MADD4  c62, b2, a2, c62
   LD b2,  BO,  17 * SIZE
   MADD2  c72, b3, a2, c72
   LD b3,  BO,  18 * SIZE
   MADD4  c82, b4, a2, c82
   LD b4,  BO,  19 * SIZE
   MADD1  c11, b1, a3, c11
   LD a2,  AO,   5 * SIZE
   MADD3  c21, b2, a3, c21
   MADD1  c31, b3, a3, c31
   MADD3  c41, b4, a3, c41
   MADD2  c12, b1, a2, c12
   LD b1,  BO,  32 * SIZE
   MADD4  c22, b2, a2, c22
   LD b2,  BO,  21 * SIZE
   MADD2  c32, b3, a2, c32
   LD b3,  BO,  22 * SIZE
   MADD4  c42, b4, a2, c42
   LD b4,  BO,  23 * SIZE
   MADD1  c51, b5, a3, c51
   MADD3  c61, b2, a3, c61
   LD a4,  AO,   6 * SIZE
   MADD1  c71, b3, a3, c71
   MADD3  c81, b4, a3, c81
   LD a3,  AO,  12 * SIZE
   MADD2  c52, b5, a2, c52
   LD b5,  BO,  36 * SIZE
   MADD4  c62, b2, a2, c62
   LD b2,  BO,  25 * SIZE
   MADD2  c72, b3, a2, c72
   LD b3,  BO,  26 * SIZE
   MADD4  c82, b4, a2, c82
   LD b4,  BO,  27 * SIZE
   MADD1  c11, b6, a4, c11
   LD a2,  AO,   7 * SIZE
   MADD3  c21, b2, a4, c21
   MADD1  c31, b3, a4, c31
   MADD3  c41, b4, a4, c41
   addi.d  L, L, -1
   MADD2  c12, b6, a2, c12
   LD b6,  BO,  40 * SIZE
   MADD4  c22, b2, a2, c22
   LD b2,  BO,  29 * SIZE
   MADD2  c32, b3, a2, c32
   LD b3,  BO,  30 * SIZE
   MADD4  c42, b4, a2, c42
   LD b4,  BO,  31 * SIZE
   MADD1  c51, b7, a4, c51
   addi.d  BO, BO, 32 * SIZE
   MADD3  c61, b2, a4, c61
   addi.d  AO, AO,  8 * SIZE
   MADD1  c71, b3, a4, c71
   MADD3  c81, b4, a4, c81
   MADD2  c52, b7, a2, c52
   LD b7,  BO,  12 * SIZE
   MADD4  c62, b2, a2, c62
   LD b2,  BO,   1 * SIZE
   MADD2  c72, b3, a2, c72
   LD b3,  BO,   2 * SIZE
   MADD4  c82, b4, a2, c82
   LD b4,  BO,   3 * SIZE
   MADD1  c11, b1, a1, c11
   LD a2,  AO,   1 * SIZE
   MADD3  c21, b2, a1, c21
   MADD1  c31, b3, a1, c31
   MADD3  c41, b4, a1, c41
   blt $r0,    L, .L12
   .align 3

.L13:
   MADD2  c12, b1, a2, c12
   LD b1,  BO,  16 * SIZE
   MADD4  c22, b2, a2, c22
   LD b2,  BO,   5 * SIZE
   MADD2  c32, b3, a2, c32
   LD b3,  BO,   6 * SIZE
   MADD4  c42, b4, a2, c42
   LD b4,  BO,   7 * SIZE
   MADD1  c51, b5, a1, c51
   MADD3  c61, b2, a1, c61
   LD a4,  AO,   2 * SIZE
   MADD1  c71, b3, a1, c71
   MADD3  c81, b4, a1, c81
   LD a1,  AO,   8 * SIZE
   MADD2  c52, b5, a2, c52
   LD b5,  BO,  20 * SIZE
   MADD4  c62, b2, a2, c62
   LD b2,  BO,   9 * SIZE
   MADD2  c72, b3, a2, c72
   LD b3,  BO,  10 * SIZE
   MADD4  c82, b4, a2, c82
   LD b4,  BO,  11 * SIZE
   MADD1  c11, b6, a4, c11
   LD a2,  AO,   3 * SIZE
   MADD3  c21, b2, a4, c21
   MADD1  c31, b3, a4, c31
   MADD3  c41, b4, a4, c41
   MADD2  c12, b6, a2, c12
   LD b6,  BO,  24 * SIZE
   MADD4  c22, b2, a2, c22
   LD b2,  BO,  13 * SIZE
   MADD2  c32, b3, a2, c32
   LD b3,  BO,  14 * SIZE
   MADD4  c42, b4, a2, c42
   LD b4,  BO,  15 * SIZE
   MADD1  c51, b7, a4, c51
   MADD3  c61, b2, a4, c61
   MADD1  c71, b3, a4, c71
   MADD3  c81, b4, a4, c81
   MADD2  c52, b7, a2, c52
   LD b7,  BO,  28 * SIZE
   MADD4  c62, b2, a2, c62
   LD b2,  BO,  17 * SIZE
   MADD2  c72, b3, a2, c72
   LD b3,  BO,  18 * SIZE
   MADD4  c82, b4, a2, c82
   LD b4,  BO,  19 * SIZE
   MADD1  c11, b1, a3, c11
   LD a2,  AO,   5 * SIZE
   MADD3  c21, b2, a3, c21
   MADD1  c31, b3, a3, c31
   MADD3  c41, b4, a3, c41
   MADD2  c12, b1, a2, c12
   LD b1,  BO,  32 * SIZE
   MADD4  c22, b2, a2, c22
   LD b2,  BO,  21 * SIZE
   MADD2  c32, b3, a2, c32
   LD b3,  BO,  22 * SIZE
   MADD4  c42, b4, a2, c42
   LD b4,  BO,  23 * SIZE
   MADD1  c51, b5, a3, c51
   MADD3  c61, b2, a3, c61
   LD a4,  AO,   6 * SIZE
   MADD1  c71, b3, a3, c71
   MADD3  c81, b4, a3, c81
   LD a3,  AO,  12 * SIZE
   MADD2  c52, b5, a2, c52
   LD b5,  BO,  36 * SIZE
   MADD4  c62, b2, a2, c62
   LD b2,  BO,  25 * SIZE
   MADD2  c72, b3, a2, c72
   LD b3,  BO,  26 * SIZE
   MADD4  c82, b4, a2, c82
   LD b4,  BO,  27 * SIZE
   MADD1  c11, b6, a4, c11
   LD a2,  AO,   7 * SIZE
   MADD3  c21, b2, a4, c21
   MADD1  c31, b3, a4, c31
   MADD3  c41, b4, a4, c41
   MADD2  c12, b6, a2, c12
   LD b6,  BO,  40 * SIZE
   MADD4  c22, b2, a2, c22
   LD b2,  BO,  29 * SIZE
   MADD2  c32, b3, a2, c32
   LD b3,  BO,  30 * SIZE
   MADD4  c42, b4, a2, c42
   LD b4,  BO,  31 * SIZE
   MADD1  c51, b7, a4, c51
   addi.d  BO, BO, 32 * SIZE
   MADD3  c61, b2, a4, c61
   addi.d  AO, AO,  8 * SIZE
   MADD1  c71, b3, a4, c71
   MADD3  c81, b4, a4, c81
   MADD2  c52, b7, a2, c52
   LD b7,  BO,  12 * SIZE
   MADD4  c62, b2, a2, c62
   LD b2,  BO,   1 * SIZE
   MADD2  c72, b3, a2, c72
   LD b3,  BO,   2 * SIZE
   MADD4  c82, b4, a2, c82
   LD b4,  BO,   3 * SIZE
   .align 3

.L15:
#ifndef TRMMKERNEL
   andi    L,  K, 3
#else
   andi    L,  TEMP, 3
#endif
   bge $r0,    L, .L18
   .align  3
.L16:
   MADD1  c11, b1, a1, c11
   LD a2,  AO,   1 * SIZE
   MADD3  c21, b2, a1, c21
   MADD1  c31, b3, a1, c31
   MADD3  c41, b4, a1, c41
   MADD2  c12, b1, a2, c12
   LD b1,  BO,   8 * SIZE
   MADD4  c22, b2, a2, c22
   LD b2,  BO,   5 * SIZE
   MADD2  c32, b3, a2, c32
   LD b3,  BO,   6 * SIZE
   MADD4  c42, b4, a2, c42
   LD b4,  BO,   7 * SIZE
   MADD1  c51, b5, a1, c51
   addi.d  L, L, -1
   MADD3  c61, b2, a1, c61
   addi.d  AO, AO,  2 * SIZE
   MADD1  c71, b3, a1, c71
   addi.d  BO, BO,  8 * SIZE
   MADD3  c81, b4, a1, c81
   LD a1,  AO,   0 * SIZE
   MADD2  c52, b5, a2, c52
   LD b5,  BO,   4 * SIZE
   MADD4  c62, b2, a2, c62
   LD b2,  BO,   1 * SIZE
   MADD2  c72, b3, a2, c72
   LD b3,  BO,   2 * SIZE
   MADD4  c82, b4, a2, c82
   LD b4,  BO,   3 * SIZE
   blt $r0,    L, .L16
.L18:
#ifndef TRMMKERNEL
   LD b1,  CO1,   0 * SIZE
   ADD c11, c11, c22
   LD b2,  CO1,   1 * SIZE
   ADD c12, c12, c21
   LD b3,  CO2,   0 * SIZE
   ADD c31, c31, c42
   LD b4,  CO2,   1 * SIZE
   ADD c32, c32, c41
   LD b5,  CO3,   0 * SIZE
   ADD c51, c51, c62
   LD b6,  CO3,   1 * SIZE
   ADD c52, c52, c61
   LD b7,  CO4,   0 * SIZE
   ADD c71, c71, c82
   LD b8,  CO4,   1 * SIZE
   ADD c72, c72, c81
   MADD  b1, c11, ALPHA_R, b1
   addi.d  CO1,CO1, 2 * SIZE
   MADD  b2, c12, ALPHA_R, b2
   addi.d  CO2,CO2, 2 * SIZE
   MADD  b3, c31, ALPHA_R, b3
   addi.d  CO3,CO3, 2 * SIZE
   MADD  b4, c32, ALPHA_R, b4
   addi.d  CO4,CO4, 2 * SIZE
   MADD  b5, c51, ALPHA_R, b5
   addi.d  I, I, -1
   MADD  b6, c52, ALPHA_R, b6
   MADD  b7, c71, ALPHA_R, b7
   MADD  b8, c72, ALPHA_R, b8
   NMSUB  b1, c12, ALPHA_I, b1
   MADD  b2, c11, ALPHA_I, b2
   MTC  c11, $r0
   NMSUB  b3, c32, ALPHA_I, b3
   MADD  b4, c31, ALPHA_I, b4
   ST b1,  CO1,  -2 * SIZE
   NMSUB  b5, c52, ALPHA_I, b5
   ST b2,  CO1,  -1 * SIZE
   MADD  b6, c51, ALPHA_I, b6
   ST b3,  CO2,  -2 * SIZE
   NMSUB  b7, c72, ALPHA_I, b7
   ST b4,  CO2,  -1 * SIZE
   MADD  b8, c71, ALPHA_I, b8
   ST b5,  CO3,  -2 * SIZE
   MOV c21, c11
   ST b6,  CO3,  -1 * SIZE
   MOV c31, c11
   ST b7,  CO4,  -2 * SIZE
   MOV c41, c11
   ST b8,  CO4,  -1 * SIZE
   MOV c51, c11
#else
   ADD c11, c11, c22
   addi.d  CO1,CO1, 2 * SIZE
   ADD c12, c12, c21
   addi.d  CO2,CO2, 2 * SIZE
   ADD c31, c31, c42
   addi.d  CO3,CO3, 2 * SIZE
   ADD c32, c32, c41
   addi.d  CO4,CO4, 2 * SIZE
   ADD c51, c51, c62
   addi.d  I, I, -1
   ADD c52, c52, c61
   ADD c71, c71, c82
   ADD c72, c72, c81
   MUL b1, ALPHA_R, c11
   MUL b2, ALPHA_R, c12
   MUL b3, ALPHA_R, c31
   MUL b4, ALPHA_R, c32
   MUL b5, ALPHA_R, c51
   MUL b6, ALPHA_R, c52
   MUL b7, ALPHA_R, c71
   MUL b8, ALPHA_R, c72
   NMSUB  b1, c12, ALPHA_I, b1
   MADD  b2, c11, ALPHA_I, b2
   MTC  c11, $r0
   NMSUB  b3, c32, ALPHA_I, b3
   MADD  b4, c31, ALPHA_I, b4
   ST b1,  CO1,  -2 * SIZE
   NMSUB  b5, c52, ALPHA_I, b5
   ST b2,  CO1,  -1 * SIZE
   MADD  b6, c51, ALPHA_I, b6
   ST b3,  CO2,  -2 * SIZE
   NMSUB  b7, c72, ALPHA_I, b7
   ST b4,  CO2,  -1 * SIZE
   MADD  b8, c71, ALPHA_I, b8
   ST b5,  CO3,  -2 * SIZE
   MOV c21, c11
   ST b6,  CO3,  -1 * SIZE
   MOV c31, c11
   ST b7,  CO4,  -2 * SIZE
   MOV c41, c11
   ST b8,  CO4,  -1 * SIZE
   MOV c51, c11
#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
   sub.d   TEMP, K, KK
#ifdef LEFT
   addi.d  TEMP, TEMP, -1
#else
   addi.d  TEMP, TEMP, -4
#endif
   slli.d  L,    TEMP, ZBASE_SHIFT
   slli.d  TEMP, TEMP, 2 + ZBASE_SHIFT
   add.d   AO, AO, L
   add.d   BO, BO, TEMP
#endif
#ifdef LEFT
   addi.d  KK, KK, 1
#endif
#endif
MOV    c61, c11
   blt $r0,    I, .L11
   .align 3

.L19:
#if defined(TRMMKERNEL) && !defined(LEFT)
   addi.d  KK, KK, 4
#endif
move   B, BO
   blt $r0,    J, .L10
   .align 3

.L20:
   andi    J,  N, 2
   MTC  c11, $r0
move   CO1, C
   bge $r0,    J, .L30
   add.d   CO2, C,      LDC
   add.d   C,   CO2,    LDC
#if defined(TRMMKERNEL) &&  defined(LEFT)
   move    KK, OFFSET
#endif
   move    I,  M
move   AO, A
   bge $r0,    I, .L29
   .align 3

.L21:
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
   move    BO,  B
#else
   slli.d  L,    KK,  ZBASE_SHIFT
   slli.d  TEMP, KK, 1 + ZBASE_SHIFT
   add.d   AO, AO, L
   add.d   BO, B,  TEMP
#endif
   LD a1,  AO,   0 * SIZE
   MOV c21, c11
   LD b1,  BO,   0 * SIZE
   MOV c31, c11
   LD a3,  AO,   4 * SIZE
   MOV c41, c11
   LD b2,  BO,   1 * SIZE
   LD b3,  BO,   2 * SIZE
   MOV c12, c11
   LD b4,  BO,   3 * SIZE
   MOV c22, c11
   LD b5,  BO,   4 * SIZE
   MOV c32, c11
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
   sub.d   TEMP, K, KK
#elif defined(LEFT)
   addi.d  TEMP, KK, 1
#else
   addi.d  TEMP, KK, 2
#endif
   srai.d  L,  TEMP, 2
MOV    c42, c11
   bge $r0,    L, .L25
#else
   LD a1,  AO,   0 * SIZE
   MOV c21, c11
   LD b1,  B,   0 * SIZE
   MOV c31, c11
   LD a3,  AO,   4 * SIZE
   MOV c41, c11
   LD b2,  B,   1 * SIZE
   srai.d  L,  K, 2
   LD b3,  B,   2 * SIZE
   MOV c12, c11
   LD b4,  B,   3 * SIZE
   MOV c22, c11
   LD b5,  B,   4 * SIZE
   MOV c32, c11
   MOV c42, c11
move   BO,  B
   bge $r0,    L, .L25
#endif
   .align  3
.L22:
   MADD1  c11, b1, a1, c11
   LD a2,  AO,   1 * SIZE
   MADD3  c21, b2, a1, c21
   addi.d  L, L, -1
   MADD1  c31, b3, a1, c31
   MADD3  c41, b4, a1, c41
   LD a1,  AO,   2 * SIZE
   MADD2  c12, b1, a2, c12
   LD b1,  BO,   8 * SIZE
   MADD4  c22, b2, a2, c22
   LD b2,  BO,   5 * SIZE
   MADD2  c32, b3, a2, c32
   LD b3,  BO,   6 * SIZE
   MADD4  c42, b4, a2, c42
   LD b4,  BO,   7 * SIZE
   MADD1  c11, b5, a1, c11
   LD a2,  AO,   3 * SIZE
   MADD3  c21, b2, a1, c21
   MADD1  c31, b3, a1, c31
   MADD3  c41, b4, a1, c41
   LD a1,  AO,   8 * SIZE
   MADD2  c12, b5, a2, c12
   LD b5,  BO,  12 * SIZE
   MADD4  c22, b2, a2, c22
   LD b2,  BO,   9 * SIZE
   MADD2  c32, b3, a2, c32
   LD b3,  BO,  10 * SIZE
   MADD4  c42, b4, a2, c42
   LD b4,  BO,  11 * SIZE
   MADD1  c11, b1, a3, c11
   LD a2,  AO,   5 * SIZE
   MADD3  c21, b2, a3, c21
   MADD1  c31, b3, a3, c31
   MADD3  c41, b4, a3, c41
   LD a3,  AO,   6 * SIZE
   MADD2  c12, b1, a2, c12
   LD b1,  BO,  16 * SIZE
   MADD4  c22, b2, a2, c22
   LD b2,  BO,  13 * SIZE
   MADD2  c32, b3, a2, c32
   LD b3,  BO,  14 * SIZE
   MADD4  c42, b4, a2, c42
   LD b4,  BO,  15 * SIZE
   MADD1  c11, b5, a3, c11
   LD a2,  AO,   7 * SIZE
   MADD3  c21, b2, a3, c21
   addi.d  AO, AO,  8 * SIZE
   MADD1  c31, b3, a3, c31
   MADD3  c41, b4, a3, c41
   LD a3,  AO,   4 * SIZE
   MADD2  c12, b5, a2, c12
   LD b5,  BO,  20 * SIZE
   MADD4  c22, b2, a2, c22
   LD b2,  BO,  17 * SIZE
   MADD2  c32, b3, a2, c32
   LD b3,  BO,  18 * SIZE
   MADD4  c42, b4, a2, c42
   LD b4,  BO,  19 * SIZE
addi.d BO, BO, 16 * SIZE
   blt $r0,    L, .L22
   .align 3

.L25:
#ifndef TRMMKERNEL
   andi    L,  K, 3
#else
   andi    L,  TEMP, 3
#endif
   bge $r0,    L, .L28
   .align  3
.L26:
   MADD1  c11, b1, a1, c11
   LD a2,  AO,   1 * SIZE
   MADD3  c21, b2, a1, c21
   addi.d  L, L, -1
   MADD1  c31, b3, a1, c31
   addi.d  BO, BO,  4 * SIZE
   MADD3  c41, b4, a1, c41
   LD a1,  AO,   2 * SIZE
   MADD2  c12, b1, a2, c12
   LD b1,  BO,   0 * SIZE
   MADD4  c22, b2, a2, c22
   LD b2,  BO,   1 * SIZE
   MADD2  c32, b3, a2, c32
   LD b3,  BO,   2 * SIZE
   MADD4  c42, b4, a2, c42
   LD b4,  BO,   3 * SIZE
addi.d AO, AO,  2 * SIZE
   blt $r0,    L, .L26
.L28:
#ifndef TRMMKERNEL
   LD b1,  CO1,   0 * SIZE
   ADD c11, c11, c22
   LD b2,  CO1,   1 * SIZE
   ADD c12, c12, c21
   LD b3,  CO2,   0 * SIZE
   ADD c31, c31, c42
   LD b4,  CO2,   1 * SIZE
   ADD c32, c32, c41
   MADD  b1, c11, ALPHA_R, b1
   addi.d  CO1,CO1, 2 * SIZE
   MADD  b2, c12, ALPHA_R, b2
   addi.d  CO2,CO2, 2 * SIZE
   MADD  b3, c31, ALPHA_R, b3
   addi.d  I, I, -1
   MADD  b4, c32, ALPHA_R, b4
   NMSUB  b1, c12, ALPHA_I, b1
   MADD  b2, c11, ALPHA_I, b2
   MTC  c11, $r0
   NMSUB  b3, c32, ALPHA_I, b3
   MADD  b4, c31, ALPHA_I, b4
   ST b1,  CO1,  -2 * SIZE
   ST b2,  CO1,  -1 * SIZE
   ST b3,  CO2,  -2 * SIZE
#else
   ADD c11, c11, c22
   ADD c12, c12, c21
   ADD c31, c31, c42
   ADD c32, c32, c41
   MUL b1, ALPHA_R, c11
   addi.d  CO1,CO1, 2 * SIZE
   MUL b2, ALPHA_R, c12
   addi.d  CO2,CO2, 2 * SIZE
   MUL b3, ALPHA_R, c31
   addi.d  I, I, -1
   MUL b4, ALPHA_R, c32
   NMSUB  b1, c12, ALPHA_I, b1
   MADD  b2, c11, ALPHA_I, b2
   MTC  c11, $r0
   NMSUB  b3, c32, ALPHA_I, b3
   MADD  b4, c31, ALPHA_I, b4
   ST b1,  CO1,  -2 * SIZE
   ST b2,  CO1,  -1 * SIZE
   ST b3,  CO2,  -2 * SIZE
#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
   sub.d   TEMP, K, KK
#ifdef LEFT
   addi.d  TEMP, TEMP, -1
#else
   addi.d  TEMP, TEMP, -2
#endif
   slli.d  L,    TEMP, ZBASE_SHIFT
   slli.d  TEMP, TEMP, 1 + ZBASE_SHIFT
   add.d   AO, AO, L
   add.d   BO, BO, TEMP
#endif
#ifdef LEFT
   addi.d  KK, KK, 1
#endif
#endif
   ST b4,  CO2,  -1 * SIZE
   blt $r0,    I, .L21
   .align 3

.L29:
#if defined(TRMMKERNEL) && !defined(LEFT)
   addi.d  KK, KK, 2
#endif
   move    B, BO
   .align 3

.L30:
   andi    J,  N, 1
   MTC  c11, $r0
move   CO1, C
   bge $r0,    J, .L999
#if defined(TRMMKERNEL) &&  defined(LEFT)
   move    KK, OFFSET
#endif
   move    I,  M
   add.d   C,   CO1,    LDC
move   AO, A
   bge $r0,    I, .L39
   .align 3

.L31:
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
   move    BO,  B
#else
   slli.d  TEMP, KK,  ZBASE_SHIFT
   add.d   AO, AO, TEMP
   add.d   BO, B,  TEMP
#endif
   LD a1,  AO,   0 * SIZE
   MOV c21, c11
   LD b1,  BO,   0 * SIZE
   MOV c31, c11
   LD a2,  AO,   1 * SIZE
   MOV c41, c11
   LD b2,  BO,   1 * SIZE
   MOV c12, c11
   MOV c22, c11
   LD a3,  AO,   4 * SIZE
   MOV c32, c11
   LD b3,  BO,   4 * SIZE
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
   sub.d   TEMP, K, KK
#elif defined(LEFT)
   addi.d  TEMP, KK, 1
#else
   addi.d  TEMP, KK, 1
#endif
   srai.d  L,  TEMP, 2
MOV    c42, c11
   bge $r0,    L, .L35
#else
   LD a1,  AO,   0 * SIZE
   MOV c21, c11
   LD b1,  B,   0 * SIZE
   MOV c31, c11
   LD a2,  AO,   1 * SIZE
   MOV c41, c11
   LD b2,  B,   1 * SIZE
   MOV c12, c11
   srai.d  L,  K, 2
   MOV c22, c11
   LD a3,  AO,   4 * SIZE
   MOV c32, c11
   LD b3,  B,   4 * SIZE
   MOV c42, c11
move   BO,  B
   bge $r0,    L, .L35
#endif
   .align  3
.L32:
   MADD1  c11, b1, a1, c11
   LD b4,  BO,   3 * SIZE
   MADD3  c21, b2, a1, c21
   LD a1,  AO,   2 * SIZE
   MADD2  c12, b1, a2, c12
   LD b1,  BO,   2 * SIZE
   MADD4  c22, b2, a2, c22
   LD a2,  AO,   3 * SIZE
   MADD1  c11, b1, a1, c11
   LD b2,  BO,   5 * SIZE
   MADD3  c21, b4, a1, c21
   LD a1,  AO,   8 * SIZE
   MADD2  c12, b1, a2, c12
   LD b1,  BO,   8 * SIZE
   MADD4  c22, b4, a2, c22
   LD a2,  AO,   5 * SIZE
   MADD1  c11, b3, a3, c11
   LD b4,  BO,   7 * SIZE
   MADD3  c21, b2, a3, c21
   LD a3,  AO,   6 * SIZE
   MADD2  c12, b3, a2, c12
   LD b3,  BO,   6 * SIZE
   MADD4  c22, b2, a2, c22
   LD a2,  AO,   7 * SIZE
   MADD1  c11, b3, a3, c11
   LD b2,  BO,   9 * SIZE
   MADD3  c21, b4, a3, c21
   LD a3,  AO,  12 * SIZE
   MADD2  c12, b3, a2, c12
   LD b3,  BO,  12 * SIZE
   MADD4  c22, b4, a2, c22
   LD a2,  AO,   9 * SIZE
   addi.d  AO, AO,  8 * SIZE
   addi.d  L, L, -1
addi.d BO, BO,  8 * SIZE
   blt $r0,    L, .L32
   .align 3

.L35:
#ifndef TRMMKERNEL
   andi    L,  K, 3
#else
   andi    L,  TEMP, 3
#endif
   bge $r0,    L, .L38
   .align  3
.L36:
   MADD1  c11, b1, a1, c11
   addi.d  L, L, -1
   MADD3  c21, b2, a1, c21
   LD a1,  AO,   2 * SIZE
   MADD2  c12, b1, a2, c12
   LD b1,  BO,   2 * SIZE
   MADD4  c22, b2, a2, c22
   LD a2,  AO,   3 * SIZE
   LD b2,  BO,   3 * SIZE
   addi.d  BO, BO,  2 * SIZE
addi.d AO, AO,  2 * SIZE
   blt $r0,    L, .L36
.L38:
#ifndef TRMMKERNEL
   LD b1,  CO1,   0 * SIZE
   ADD c11, c11, c22
   LD b2,  CO1,   1 * SIZE
   ADD c12, c12, c21
   MADD  b1, c11, ALPHA_R, b1
   addi.d  CO1,CO1, 2 * SIZE
   MADD  b2, c12, ALPHA_R, b2
   addi.d  I, I, -1
   NMSUB  b1, c12, ALPHA_I, b1
   MADD  b2, c11, ALPHA_I, b2
   MTC  c11, $r0
   ST b1,  CO1,  -2 * SIZE
   ST b2,  CO1,  -1 * SIZE
   blt $r0,    I, .L31
#else
   ADD c11, c11, c22
   ADD c12, c12, c21
   MUL b1, ALPHA_R, c11
   addi.d  CO1,CO1, 2 * SIZE
   MUL b2, ALPHA_R, c12
   addi.d  I, I, -1
   NMSUB  b1, c12, ALPHA_I, b1
   MADD  b2, c11, ALPHA_I, b2
   MTC  c11, $r0
#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
   sub.d   TEMP, K, KK
#ifdef LEFT
   addi.d  TEMP, TEMP, -1
#else
   addi.d  TEMP, TEMP, -1
#endif
   slli.d  TEMP, TEMP, ZBASE_SHIFT
   add.d   AO, AO, TEMP
   add.d   BO, BO, TEMP
#endif
#ifdef LEFT
   addi.d  KK, KK, 1
#endif
   ST b1,  CO1,  -2 * SIZE
   ST b2,  CO1,  -1 * SIZE
   blt $r0,    I, .L31
#endif
   .align 3

.L39:
#if defined(TRMMKERNEL) && !defined(LEFT)
   addi.d  KK, KK, 1
#endif
   move    B, BO
   .align 3

.L999:
   LDARG  $r23,  $sp,  0
   LDARG  $r24,  $sp,  8
   LDARG  $r25,  $sp,  64
   fld.d  $f24,  $sp,  16
   fld.d  $f25,  $sp,  24
   fld.d  $f26,  $sp,  32
   fld.d  $f27,  $sp,  40
   fld.d  $f28,  $sp,  48
   fld.d  $f29,  $sp,  56
#if defined(TRMMKERNEL)
   LDARG  $r26,  $sp,   72
   LDARG  $r27,  $sp,   80
#endif
#ifndef __64BIT__
   fld.d  $f18,  $sp,  88
   fld.d  $f19,  $sp,  96
   fld.d  $f20,  $sp, 104
   fld.d  $f21,  $sp, 112
#endif
   addi.d $sp, $sp, 128
   move $r4, $r17
   fmov.d $f0, $f22
   fmov.d $f1, $f23
   jirl    $r0, $r1, 0x0

   EPILOGUE
