/***************************************************************************
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
#define L      $r30
#define PREFETCHSIZE (4 * 10)
#define CO1    $r14
#define CO2    $r15
#define CO3    $r23
#define CO4    $r24
#define CO5    $r25
#define CO6    $r26
#define CO7    $r27
#define CO8    $r28
#define BB     $r29

#if defined(TRMMKERNEL)
#define OFFSET $r11
#define KK     $r20
#define TEMP   $r16
#endif

#define a1     $f22
#define a2     $f8
#define a3     $f27
#define a4     $f28
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
#define c22    $f1
#define c31    $f2
#define c32    $f4
#define c41    $f5
#define c42    $f6
#define c51    $f7
#define c52    $f18
#define c61    $f19
#define c62    $f20
#define c71    $f21
#define c72    $f24
#define c81    $f25
#define c82    $f26
#define ALPHA  $f0

   PROLOGUE

   addi.d  $sp, $sp, -160
   SDARG  $r23,  $sp,    0
   SDARG  $r24,  $sp,    8
   SDARG  $r25,  $sp,   16
   SDARG  $r26,  $sp,   24
   SDARG  $r27,  $sp,   32
   SDARG  $r28,  $sp,   40
   SDARG  $r29,  $sp,   48
   SDARG  $r30,  $sp,   96
   fst.d  $f24,  $sp,  56
   fst.d  $f25,  $sp,  64
   fst.d  $f26,  $sp,  72
   fst.d  $f27,  $sp,  80
   fst.d  $f28,  $sp,  88
#if defined(TRMMKERNEL)
   SDARG  $r20,  $sp,  104
   SDARG  $r16,  $sp,  112
#endif
#ifndef __64BIT__
   fst.d  $f18,  $sp, 120
   fst.d  $f19,  $sp, 128
   fst.d  $f20,  $sp, 136
   fst.d  $f21,  $sp, 144
#endif
   slli.d     LDC,    LDC, BASE_SHIFT
#if defined(TRMMKERNEL) && !defined(LEFT)
   sub.d   KK, $r0, OFFSET
#endif
   srai.d  J,  N, 3
nop
   bge $r0,    J, .L30
.L10:
   move    CO1, C
   MTC  c11, $r0
   add.d   CO2, C,      LDC
   move    AO, A
   add.d   CO3, CO2,    LDC
   addi.d  J, J, -1
   add.d   CO4, CO3,    LDC
   MOV c21, c11
   add.d   CO5, CO4,    LDC
   MOV c31, c11
   add.d   CO6, CO5,    LDC
   MOV c41, c11
   add.d   CO7, CO6,    LDC
   MOV c51, c11
   add.d   CO8, CO7,    LDC
   srai.d  I,  M, 1
   add.d   C,   CO8,    LDC
   slli.d  BB, K, 2 + BASE_SHIFT
   add.d   BB, B, BB
#if defined(TRMMKERNEL) &&  defined(LEFT)
   move    KK, OFFSET
#endif
MOV    c61, c11
   bge $r0,    I, .L20
.L11:
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
   move    BO,  B
#else
   slli.d  L,    KK, 1 + BASE_SHIFT
   slli.d  TEMP, KK, 3 + BASE_SHIFT
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
   addi.d  TEMP, KK, 2
#else
   addi.d  TEMP, KK, 8
#endif
   srai.d  L,  TEMP, 2
   bge $r0,    L, .L15
#else
   LD a1,  AO,   0 * SIZE
   MOV c71, c11
   LD b1,  B,   0 * SIZE
   MOV c81, c11
   preld  1,  CO1,  3 * SIZE
   preld  1,  CO2,  3 * SIZE
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
   MADD  c11, b1, a1, c11
   LD a2,  AO,   1 * SIZE
   MADD  c21, b2, a1, c21
   addi.d  L, L, -1
   MADD  c31, b3, a1, c31
   MADD  c41, b4, a1, c41
   bge $r0,    L, .L13
   preld  1,  CO3,  2 * SIZE
   .align  3
.L12:
   MADD  c12, b1, a2, c12
   LD b1,  BO,  16 * SIZE
   MADD  c22, b2, a2, c22
   LD b2,  BO,   5 * SIZE
   MADD  c32, b3, a2, c32
   LD b3,  BO,   6 * SIZE
   MADD  c42, b4, a2, c42
   LD b4,  BO,   7 * SIZE
   MADD  c51, b5, a1, c51
   LD a4,  AO,   2 * SIZE
   MADD  c61, b2, a1, c61
   MADD  c71, b3, a1, c71
   MADD  c81, b4, a1, c81
   LD a1,  AO,   8 * SIZE
   MADD  c52, b5, a2, c52
   LD b5,  BO,  20 * SIZE
   MADD  c62, b2, a2, c62
   LD b2,  BO,   9 * SIZE
   MADD  c72, b3, a2, c72
   LD b3,  BO,  10 * SIZE
   MADD  c82, b4, a2, c82
   LD b4,  BO,  11 * SIZE
   MADD  c11, b6, a4, c11
   LD a2,  AO,   3 * SIZE
   MADD  c21, b2, a4, c21
   MADD  c31, b3, a4, c31
   MADD  c41, b4, a4, c41
   MADD  c12, b6, a2, c12
   LD b6,  BO,  24 * SIZE
   MADD  c22, b2, a2, c22
   LD b2,  BO,  13 * SIZE
   MADD  c32, b3, a2, c32
   LD b3,  BO,  14 * SIZE
   MADD  c42, b4, a2, c42
   LD b4,  BO,  15 * SIZE
   MADD  c51, b7, a4, c51
   MADD  c61, b2, a4, c61
   MADD  c71, b3, a4, c71
   MADD  c81, b4, a4, c81
   MADD  c52, b7, a2, c52
   LD b7,  BO,  28 * SIZE
   MADD  c62, b2, a2, c62
   LD b2,  BO,  17 * SIZE
   MADD  c72, b3, a2, c72
   LD b3,  BO,  18 * SIZE
   MADD  c82, b4, a2, c82
   LD b4,  BO,  19 * SIZE
   MADD  c11, b1, a3, c11
   LD a2,  AO,   5 * SIZE
   MADD  c21, b2, a3, c21
   MADD  c31, b3, a3, c31
   MADD  c41, b4, a3, c41
   MADD  c12, b1, a2, c12
   LD b1,  BO,  32 * SIZE
   MADD  c22, b2, a2, c22
   LD b2,  BO,  21 * SIZE
   MADD  c32, b3, a2, c32
   LD b3,  BO,  22 * SIZE
   MADD  c42, b4, a2, c42
   LD b4,  BO,  23 * SIZE
   MADD  c51, b5, a3, c51
   LD a4,  AO,   6 * SIZE
   MADD  c61, b2, a3, c61
   MADD  c71, b3, a3, c71
   MADD  c81, b4, a3, c81
   LD a3,  AO,  12 * SIZE
   MADD  c52, b5, a2, c52
   LD b5,  BO,  36 * SIZE
   MADD  c62, b2, a2, c62
   LD b2,  BO,  25 * SIZE
   MADD  c72, b3, a2, c72
   LD b3,  BO,  26 * SIZE
   MADD  c82, b4, a2, c82
   LD b4,  BO,  27 * SIZE
   MADD  c11, b6, a4, c11
   LD a2,  AO,   7 * SIZE
   MADD  c21, b2, a4, c21
   MADD  c31, b3, a4, c31
   MADD  c41, b4, a4, c41
   addi.d  L, L, -1
   MADD  c12, b6, a2, c12
   LD b6,  BO,  40 * SIZE
   MADD  c22, b2, a2, c22
   LD b2,  BO,  29 * SIZE
   MADD  c32, b3, a2, c32
   LD b3,  BO,  30 * SIZE
   MADD  c42, b4, a2, c42
   LD b4,  BO,  31 * SIZE
   MADD  c51, b7, a4, c51
   addi.d  BO, BO, 32 * SIZE
   MADD  c61, b2, a4, c61
   addi.d  AO, AO,  8 * SIZE
   MADD  c71, b3, a4, c71
   MADD  c81, b4, a4, c81
   MADD  c52, b7, a2, c52
   LD b7,  BO,  12 * SIZE
   MADD  c62, b2, a2, c62
   LD b2,  BO,   1 * SIZE
   MADD  c72, b3, a2, c72
   LD b3,  BO,   2 * SIZE
   MADD  c82, b4, a2, c82
   LD b4,  BO,   3 * SIZE
   MADD  c11, b1, a1, c11
   LD a2,  AO,   1 * SIZE
   MADD  c21, b2, a1, c21
   MADD  c31, b3, a1, c31
   MADD  c41, b4, a1, c41
   blt $r0,    L, .L12
   .align 3

.L13:
   MADD  c12, b1, a2, c12
   LD b1,  BO,  16 * SIZE
   MADD  c22, b2, a2, c22
   LD b2,  BO,   5 * SIZE
   MADD  c32, b3, a2, c32
   LD b3,  BO,   6 * SIZE
   MADD  c42, b4, a2, c42
   LD b4,  BO,   7 * SIZE
   MADD  c51, b5, a1, c51
   MADD  c61, b2, a1, c61
   LD a4,  AO,   2 * SIZE
   MADD  c71, b3, a1, c71
   MADD  c81, b4, a1, c81
   LD a1,  AO,   8 * SIZE
   MADD  c52, b5, a2, c52
   LD b5,  BO,  20 * SIZE
   MADD  c62, b2, a2, c62
   LD b2,  BO,   9 * SIZE
   MADD  c72, b3, a2, c72
   LD b3,  BO,  10 * SIZE
   MADD  c82, b4, a2, c82
   LD b4,  BO,  11 * SIZE
   MADD  c11, b6, a4, c11
   LD a2,  AO,   3 * SIZE
   MADD  c21, b2, a4, c21
   MADD  c31, b3, a4, c31
   preld  1,  CO4,  3 * SIZE
   MADD  c41, b4, a4, c41
   MADD  c12, b6, a2, c12
   LD b6,  BO,  24 * SIZE
   MADD  c22, b2, a2, c22
   LD b2,  BO,  13 * SIZE
   MADD  c32, b3, a2, c32
   LD b3,  BO,  14 * SIZE
   MADD  c42, b4, a2, c42
   LD b4,  BO,  15 * SIZE
   MADD  c51, b7, a4, c51
   preld  1,  CO5,  3 * SIZE
   MADD  c61, b2, a4, c61
   MADD  c71, b3, a4, c71
   preld  1,  CO6,  3 * SIZE
   MADD  c81, b4, a4, c81
   MADD  c52, b7, a2, c52
   LD b7,  BO,  28 * SIZE
   MADD  c62, b2, a2, c62
   LD b2,  BO,  17 * SIZE
   MADD  c72, b3, a2, c72
   LD b3,  BO,  18 * SIZE
   MADD  c82, b4, a2, c82
   LD b4,  BO,  19 * SIZE
   MADD  c11, b1, a3, c11
   LD a2,  AO,   5 * SIZE
   MADD  c21, b2, a3, c21
   MADD  c31, b3, a3, c31
   preld  1,  CO7,  3 * SIZE
   MADD  c41, b4, a3, c41
   MADD  c12, b1, a2, c12
   LD b1,  BO,  32 * SIZE
   MADD  c22, b2, a2, c22
   LD b2,  BO,  21 * SIZE
   MADD  c32, b3, a2, c32
   LD b3,  BO,  22 * SIZE
   MADD  c42, b4, a2, c42
   LD b4,  BO,  23 * SIZE
   MADD  c51, b5, a3, c51
   MADD  c61, b2, a3, c61
   LD a4,  AO,   6 * SIZE
   MADD  c71, b3, a3, c71
   MADD  c81, b4, a3, c81
   MADD  c52, b5, a2, c52
   LD b5,  BO,  36 * SIZE
   MADD  c62, b2, a2, c62
   LD b2,  BO,  25 * SIZE
   MADD  c72, b3, a2, c72
   LD b3,  BO,  26 * SIZE
   MADD  c82, b4, a2, c82
   LD b4,  BO,  27 * SIZE
   MADD  c11, b6, a4, c11
   LD a2,  AO,   7 * SIZE
   MADD  c21, b2, a4, c21
   MADD  c31, b3, a4, c31
   MADD  c41, b4, a4, c41
   MADD  c12, b6, a2, c12
   LD b6,  BO,  40 * SIZE
   MADD  c22, b2, a2, c22
   LD b2,  BO,  29 * SIZE
   MADD  c32, b3, a2, c32
   LD b3,  BO,  30 * SIZE
   MADD  c42, b4, a2, c42
   LD b4,  BO,  31 * SIZE
   MADD  c51, b7, a4, c51
   addi.d  BO, BO, 32 * SIZE
   MADD  c61, b2, a4, c61
   addi.d  AO, AO,  8 * SIZE
   MADD  c71, b3, a4, c71
   MADD  c81, b4, a4, c81
   MADD  c52, b7, a2, c52
   LD b7,  BO,  12 * SIZE
   MADD  c62, b2, a2, c62
   LD b2,  BO,   1 * SIZE
   MADD  c72, b3, a2, c72
   LD b3,  BO,   2 * SIZE
   MADD  c82, b4, a2, c82
   LD b4,  BO,   3 * SIZE
   .align 3

.L15:
#ifndef TRMMKERNEL
   andi    L,  K, 3
#else
   andi    L,  TEMP, 3
#endif
   preld  1,  CO8,  3 * SIZE
   bge $r0,    L, .L18
   .align  3
.L16:
   MADD  c11, b1, a1, c11
   LD a2,  AO,   1 * SIZE
   MADD  c21, b2, a1, c21
   MADD  c31, b3, a1, c31
   MADD  c41, b4, a1, c41
   MADD  c12, b1, a2, c12
   LD b1,  BO,   8 * SIZE
   MADD  c22, b2, a2, c22
   LD b2,  BO,   5 * SIZE
   MADD  c32, b3, a2, c32
   LD b3,  BO,   6 * SIZE
   MADD  c42, b4, a2, c42
   LD b4,  BO,   7 * SIZE
   MADD  c51, b5, a1, c51
   addi.d  L, L, -1
   MADD  c61, b2, a1, c61
   addi.d  AO, AO,  2 * SIZE
   MADD  c71, b3, a1, c71
   addi.d  BO, BO,  8 * SIZE
   MADD  c81, b4, a1, c81
   LD a1,  AO,   0 * SIZE
   MADD  c52, b5, a2, c52
   LD b5,  BO,   4 * SIZE
   MADD  c62, b2, a2, c62
   LD b2,  BO,   1 * SIZE
   MADD  c72, b3, a2, c72
   LD b3,  BO,   2 * SIZE
   MADD  c82, b4, a2, c82
   LD b4,  BO,   3 * SIZE
   blt $r0,    L, .L16
.L18:
#ifndef TRMMKERNEL
   LD $f22,  CO1,  0 * SIZE
   addi.d  CO3,CO3, 2 * SIZE
   LD $f8,  CO1,  1 * SIZE
   addi.d  CO1,CO1, 2 * SIZE
   LD $f23,  CO2,  0 * SIZE
   addi.d  CO4,CO4, 2 * SIZE
   LD $f9,  CO2,  1 * SIZE
   addi.d  CO2,CO2, 2 * SIZE
   LD $f10,  CO3,  -2 * SIZE
   addi.d  CO5,CO5, 2 * SIZE
   LD $f11,  CO3,  -1 * SIZE
   addi.d  CO6,CO6, 2 * SIZE
   LD $f12,  CO4,  -2 * SIZE
   addi.d  CO7,CO7, 2 * SIZE
   LD $f13,  CO4,  -1 * SIZE
   addi.d  I, I, -1
   MADD  c11, c11, ALPHA, $f22
   LD $f22,  CO5, -2 * SIZE
   MADD  c12, c12, ALPHA, $f8
   LD $f8,  CO5, -1 * SIZE
   MADD  c21, c21, ALPHA, $f23
   LD $f23,  CO6, -2 * SIZE
   MADD  c22, c22, ALPHA, $f9
   LD $f9,  CO6, -1 * SIZE
   MADD  c31, c31, ALPHA, $f10
   LD $f10,  CO7, -2 * SIZE
   MADD  c32, c32, ALPHA, $f11
   LD $f11,  CO7, -1 * SIZE
   MADD  c41, c41, ALPHA, $f12
   LD $f12,  CO8,  0 * SIZE
   MADD  c42, c42, ALPHA, $f13
   LD $f13,  CO8,  1 * SIZE
   preld  0,  BB,  0 * SIZE
   preld  0,  BB,  8 * SIZE
   ST c11,  CO1,  -2 * SIZE
   MTC  c11, $r0
   ST c12,  CO1,  -1 * SIZE
   addi.d  CO8,CO8, 2 * SIZE
   ST c21,  CO2,  -2 * SIZE
   MOV c21, c11
   ST c22,  CO2,  -1 * SIZE
   addi.d  BB, BB, 16 * SIZE
   MADD  c51, c51, ALPHA, $f22
   ST c31,  CO3,  -2 * SIZE
   MADD  c52, c52, ALPHA, $f8
   ST c32,  CO3,  -1 * SIZE
   MADD  c61, c61, ALPHA, $f23
   ST c41,  CO4,  -2 * SIZE
   MADD  c62, c62, ALPHA, $f9
   ST c42,  CO4,  -1 * SIZE
   MADD  c71, c71, ALPHA, $f10
   ST c51,  CO5,  -2 * SIZE
   MADD  c72, c72, ALPHA, $f11
   ST c52,  CO5,  -1 * SIZE
   MADD  c81, c81, ALPHA, $f12
   ST c61,  CO6,  -2 * SIZE
   MADD  c82, c82, ALPHA, $f13
   ST c62,  CO6,  -1 * SIZE
   ST c71,  CO7,  -2 * SIZE
   MOV c31, c11
   ST c72,  CO7,  -1 * SIZE
   MOV c41, c11
   ST c81,  CO8,  -2 * SIZE
   MOV c51, c11
   ST c82,  CO8,  -1 * SIZE
MOV    c61, c11
   blt $r0,    I, .L11
#else
   addi.d  CO4,CO4, 2 * SIZE
   addi.d  CO5,CO5, 2 * SIZE
   addi.d  CO6,CO6, 2 * SIZE
   addi.d  CO7,CO7, 2 * SIZE
   preld  0,  BB,  0 * SIZE
   preld  0,  BB,  8 * SIZE
   MUL c11, ALPHA, c11
   addi.d  CO1,CO1, 2 * SIZE
   MUL c12, ALPHA, c12
   MTC  a1, $r0
   MUL c21, ALPHA, c21
   addi.d  CO2,CO2, 2 * SIZE
   MUL c22, ALPHA, c22
   addi.d  CO3,CO3, 2 * SIZE
   ST c11,  CO1,  -2 * SIZE
   MUL c31, ALPHA, c31
   ST c12,  CO1,  -1 * SIZE
   MUL c32, ALPHA, c32
   ST c21,  CO2,  -2 * SIZE
   MUL c41, ALPHA, c41
   ST c22,  CO2,  -1 * SIZE
   MUL c42, ALPHA, c42
   ST c31,  CO3,  -2 * SIZE
   MUL c51, ALPHA, c51
   ST c32,  CO3,  -1 * SIZE
   MUL c52, ALPHA, c52
   ST c41,  CO4,  -2 * SIZE
   MUL c61, ALPHA, c61
   ST c42,  CO4,  -1 * SIZE
   MUL c62, ALPHA, c62
   ST c51,  CO5,  -2 * SIZE
   MUL c71, ALPHA, c71
   ST c52,  CO5,  -1 * SIZE
   MUL c72, ALPHA, c72
   ST c61,  CO6,  -2 * SIZE
   MUL c81, ALPHA, c81
   ST c62,  CO6,  -1 * SIZE
   MUL c82, ALPHA, c82
   ST c71,  CO7,  -2 * SIZE
   MOV c11, a1
   ST c72,  CO7,  -1 * SIZE
   MOV c21, a1
   addi.d  CO8,CO8, 2 * SIZE
   addi.d  BB, BB, 16 * SIZE
   ST c81,  CO8,  -2 * SIZE
   MOV c31, a1
   ST c82,  CO8,  -1 * SIZE
   MOV c41, a1
   addi.d  I, I, -1
   MOV c51, a1
#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
   sub.d   TEMP, K, KK
#ifdef LEFT
   addi.d  TEMP, TEMP, -2
#else
   addi.d  TEMP, TEMP, -8
#endif
   slli.d  L,    TEMP, 1 + BASE_SHIFT
   slli.d  TEMP, TEMP, 3 + BASE_SHIFT
   add.d   AO, AO, L
   add.d   BO, BO, TEMP
#endif
#ifdef LEFT
   addi.d  KK, KK, 2
#endif
MOV    c61, a1
   blt $r0,    I, .L11
#endif
   .align 3

.L20:
   andi    I,  M, 1
   MOV c61, c11
MOV    c71, c11
   bge $r0,    I, .L29
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
   move    BO,  B
#else
   slli.d  L,    KK, 0 + BASE_SHIFT
   slli.d  TEMP, KK, 3 + BASE_SHIFT
   add.d   AO, AO, L
   add.d   BO, B,  TEMP
#endif
   LD a1,  AO,   0 * SIZE
   LD a2,  AO,   1 * SIZE
   LD a3,  AO,   2 * SIZE
   LD a4,  AO,   3 * SIZE
   LD b1,  BO,   0 * SIZE
   LD b2,  BO,   1 * SIZE
   LD b3,  BO,   2 * SIZE
   LD b4,  BO,   3 * SIZE
   LD b5,  BO,   4 * SIZE
   LD b6,  BO,   8 * SIZE
   LD b7,  BO,  12 * SIZE
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
   sub.d   TEMP, K, KK
#elif defined(LEFT)
   addi.d  TEMP, KK, 1
#else
   addi.d  TEMP, KK, 8
#endif
   srai.d  L,  TEMP, 2
MOV    c81, c11
   bge $r0,    L, .L25
#else
   LD a1,  AO,   0 * SIZE
   LD a2,  AO,   1 * SIZE
   LD a3,  AO,   2 * SIZE
   LD a4,  AO,   3 * SIZE
   LD b1,  B,   0 * SIZE
   LD b2,  B,   1 * SIZE
   LD b3,  B,   2 * SIZE
   LD b4,  B,   3 * SIZE
   LD b5,  B,   4 * SIZE
   LD b6,  B,   8 * SIZE
   LD b7,  B,  12 * SIZE
   srai.d  L,  K, 2
   MOV c81, c11
move   BO,  B
   bge $r0,    L, .L25
#endif
   .align  3
.L22:
   MADD  c11, b1, a1, c11
   LD b1,  BO,  16 * SIZE
   MADD  c21, b2, a1, c21
   LD b2,  BO,   5 * SIZE
   MADD  c31, b3, a1, c31
   LD b3,  BO,   6 * SIZE
   MADD  c41, b4, a1, c41
   LD b4,  BO,   7 * SIZE
   MADD  c51, b5, a1, c51
   LD b5,  BO,  20 * SIZE
   MADD  c61, b2, a1, c61
   LD b2,  BO,   9 * SIZE
   MADD  c71, b3, a1, c71
   LD b3,  BO,  10 * SIZE
   MADD  c81, b4, a1, c81
   LD b4,  BO,  11 * SIZE
   LD a1,  AO,   4 * SIZE
   addi.d  L, L, -1
   MADD  c11, b6, a2, c11
   LD b6,  BO,  24 * SIZE
   MADD  c21, b2, a2, c21
   LD b2,  BO,  13 * SIZE
   MADD  c31, b3, a2, c31
   LD b3,  BO,  14 * SIZE
   MADD  c41, b4, a2, c41
   LD b4,  BO,  15 * SIZE
   MADD  c51, b7, a2, c51
   LD b7,  BO,  28 * SIZE
   MADD  c61, b2, a2, c61
   LD b2,  BO,  17 * SIZE
   MADD  c71, b3, a2, c71
   LD b3,  BO,  18 * SIZE
   MADD  c81, b4, a2, c81
   LD b4,  BO,  19 * SIZE
   LD a2,  AO,   5 * SIZE
   addi.d  AO, AO,  4 * SIZE
   MADD  c11, b1, a3, c11
   LD b1,  BO,  32 * SIZE
   MADD  c21, b2, a3, c21
   LD b2,  BO,  21 * SIZE
   MADD  c31, b3, a3, c31
   LD b3,  BO,  22 * SIZE
   MADD  c41, b4, a3, c41
   LD b4,  BO,  23 * SIZE
   MADD  c51, b5, a3, c51
   LD b5,  BO,  36 * SIZE
   MADD  c61, b2, a3, c61
   LD b2,  BO,  25 * SIZE
   MADD  c71, b3, a3, c71
   LD b3,  BO,  26 * SIZE
   MADD  c81, b4, a3, c81
   LD b4,  BO,  27 * SIZE
   LD a3,  AO,   2 * SIZE
   addi.d  BO, BO, 32 * SIZE
   MADD  c11, b6, a4, c11
   LD b6,  BO,   8 * SIZE
   MADD  c21, b2, a4, c21
   LD b2,  BO,  -3 * SIZE
   MADD  c31, b3, a4, c31
   LD b3,  BO,  -2 * SIZE
   MADD  c41, b4, a4, c41
   LD b4,  BO,  -1 * SIZE
   MADD  c51, b7, a4, c51
   LD b7,  BO,  12 * SIZE
   MADD  c61, b2, a4, c61
   LD b2,  BO,   1 * SIZE
   MADD  c71, b3, a4, c71
   LD b3,  BO,   2 * SIZE
   MADD  c81, b4, a4, c81
   LD b4,  BO,   3 * SIZE
   LD a4,  AO,   3 * SIZE
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
   MADD  c11, b1, a1, c11
   LD b1,  BO,   8 * SIZE
   MADD  c21, b2, a1, c21
   LD b2,  BO,   5 * SIZE
   MADD  c31, b3, a1, c31
   LD b3,  BO,   6 * SIZE
   MADD  c41, b4, a1, c41
   LD b4,  BO,   7 * SIZE
   addi.d  L, L, -1
   MOV a2, a2
   addi.d  AO, AO,  1 * SIZE
   addi.d  BO, BO,  8 * SIZE
   MADD  c51, b5, a1, c51
   LD b5,  BO,   4 * SIZE
   MADD  c61, b2, a1, c61
   LD b2,  BO,   1 * SIZE
   MADD  c71, b3, a1, c71
   LD b3,  BO,   2 * SIZE
   MADD  c81, b4, a1, c81
   LD a1,  AO,   0 * SIZE
   LD b4,  BO,   3 * SIZE
   blt $r0,    L, .L26
.L28:
#ifndef TRMMKERNEL
   LD $f22,  CO1,  0 * SIZE
   LD $f8,  CO2,  0 * SIZE
   LD $f23,  CO3,  0 * SIZE
   LD $f9,  CO4,  0 * SIZE
   MADD  c11, c11, ALPHA, $f22
   LD $f10,  CO5,  0 * SIZE
   MADD  c21, c21, ALPHA, $f8
   LD $f11,  CO6,  0 * SIZE
   MADD  c31, c31, ALPHA, $f23
   LD $f12,  CO7,  0 * SIZE
   MADD  c41, c41, ALPHA, $f9
   LD $f13,  CO8,  0 * SIZE
   MADD  c51, c51, ALPHA, $f10
   ST c11,  CO1,   0 * SIZE
   MADD  c61, c61, ALPHA, $f11
   ST c21,  CO2,   0 * SIZE
   MADD  c71, c71, ALPHA, $f12
   ST c31,  CO3,   0 * SIZE
   MADD  c81, c81, ALPHA, $f13
   ST c41,  CO4,   0 * SIZE
   ST c51,  CO5,   0 * SIZE
   ST c61,  CO6,   0 * SIZE
   ST c71,  CO7,   0 * SIZE
   ST c81,  CO8,   0 * SIZE
#else
   MUL c11, ALPHA, c11
   MUL c21, ALPHA, c21
   MUL c31, ALPHA, c31
   MUL c41, ALPHA, c41
   ST c11,  CO1,   0 * SIZE
   MUL c51, ALPHA, c51
   ST c21,  CO2,   0 * SIZE
   MUL c61, ALPHA, c61
   ST c31,  CO3,   0 * SIZE
   MUL c71, ALPHA, c71
   ST c41,  CO4,   0 * SIZE
   MUL c81, ALPHA, c81
   ST c51,  CO5,   0 * SIZE
   ST c61,  CO6,   0 * SIZE
   ST c71,  CO7,   0 * SIZE
   ST c81,  CO8,   0 * SIZE
#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
   sub.d   TEMP, K, KK
#ifdef LEFT
   addi.d  TEMP, TEMP, -1
#else
   addi.d  TEMP, TEMP, -8
#endif
   slli.d  L,    TEMP, 0 + BASE_SHIFT
   slli.d  TEMP, TEMP, 3 + BASE_SHIFT
   add.d   AO, AO, L
   add.d   BO, BO, TEMP
#endif
#ifdef LEFT
   addi.d  KK, KK, 1
#endif
#endif
   .align 3

.L29:
#if defined(TRMMKERNEL) && !defined(LEFT)
   addi.d  KK, KK, 8
#endif
move   B, BO
   blt $r0,    J, .L10
   .align 3

.L30:
   andi    J,  N, 4
move   AO, A
   bge $r0,    J, .L50
   move    CO1, C
   MTC  c11, $r0
   add.d   CO2, C,      LDC
   add.d   CO3, CO2,    LDC
   add.d   CO4, CO3,    LDC
   MOV c21, c11
   add.d   C,   CO4,    LDC
   MOV c31, c11
#if defined(TRMMKERNEL) &&  defined(LEFT)
   move    KK, OFFSET
#endif
   srai.d  I,  M, 1
MOV    c41, c11
   bge $r0,    I, .L40
.L31:
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
   move    BO,  B
#else
   slli.d  L,    KK, 1 + BASE_SHIFT
   slli.d  TEMP, KK, 2 + BASE_SHIFT
   add.d   AO, AO, L
   add.d   BO, B,  TEMP
#endif
   LD a1,  AO,   0 * SIZE
   LD a3,  AO,   4 * SIZE
   LD b1,  BO,   0 * SIZE
   MOV c12, c11
   LD b2,  BO,   1 * SIZE
   MOV c22, c11
   LD b3,  BO,   2 * SIZE
   MOV c32, c11
   LD b4,  BO,   3 * SIZE
   MOV c42, c11
   LD b5,  BO,   4 * SIZE
   LD b6,  BO,   8 * SIZE
   LD b7,  BO,  12 * SIZE
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
   sub.d   TEMP, K, KK
#elif defined(LEFT)
   addi.d  TEMP, KK, 2
#else
   addi.d  TEMP, KK, 4
#endif
   srai.d  L,  TEMP, 2
   bge $r0,    L, .L35
#else
   LD a1,  AO,   0 * SIZE
   LD a3,  AO,   4 * SIZE
   LD b1,  B,   0 * SIZE
   MOV c12, c11
   LD b2,  B,   1 * SIZE
   MOV c22, c11
   LD b3,  B,   2 * SIZE
   MOV c32, c11
   LD b4,  B,   3 * SIZE
   MOV c42, c11
   LD b5,  B,   4 * SIZE
   srai.d  L,  K, 2
   LD b6,  B,   8 * SIZE
   LD b7,  B,  12 * SIZE
move   BO,  B
   bge $r0,    L, .L35
#endif
   .align  3
.L32:
   MADD  c11, b1, a1, c11
   LD a2,  AO,   1 * SIZE
   MADD  c21, b2, a1, c21
   addi.d  L, L, -1
   MADD  c31, b3, a1, c31
   MADD  c41, b4, a1, c41
   LD a1,  AO,   2 * SIZE
   MADD  c12, b1, a2, c12
   LD b1,  BO,  16 * SIZE
   MADD  c22, b2, a2, c22
   LD b2,  BO,   5 * SIZE
   MADD  c32, b3, a2, c32
   LD b3,  BO,   6 * SIZE
   MADD  c42, b4, a2, c42
   LD b4,  BO,   7 * SIZE
   MADD  c11, b5, a1, c11
   LD a2,  AO,   3 * SIZE
   MADD  c21, b2, a1, c21
   MADD  c31, b3, a1, c31
   MADD  c41, b4, a1, c41
   LD a1,  AO,   8 * SIZE
   MADD  c12, b5, a2, c12
   LD b5,  BO,  20 * SIZE
   MADD  c22, b2, a2, c22
   LD b2,  BO,   9 * SIZE
   MADD  c32, b3, a2, c32
   LD b3,  BO,  10 * SIZE
   MADD  c42, b4, a2, c42
   LD b4,  BO,  11 * SIZE
   MADD  c11, b6, a3, c11
   LD a2,  AO,   5 * SIZE
   MADD  c21, b2, a3, c21
   MADD  c31, b3, a3, c31
   MADD  c41, b4, a3, c41
   LD a3,  AO,   6 * SIZE
   MADD  c12, b6, a2, c12
   LD b6,  BO,  24 * SIZE
   MADD  c22, b2, a2, c22
   LD b2,  BO,  13 * SIZE
   MADD  c32, b3, a2, c32
   LD b3,  BO,  14 * SIZE
   MADD  c42, b4, a2, c42
   LD b4,  BO,  15 * SIZE
   MADD  c11, b7, a3, c11
   LD a2,  AO,   7 * SIZE
   MADD  c21, b2, a3, c21
   addi.d  AO, AO,  8 * SIZE
   MADD  c31, b3, a3, c31
   addi.d  BO, BO, 16 * SIZE
   MADD  c41, b4, a3, c41
   LD a3,  AO,   4 * SIZE
   MADD  c12, b7, a2, c12
   LD b7,  BO,  12 * SIZE
   MADD  c22, b2, a2, c22
   LD b2,  BO,   1 * SIZE
   MADD  c32, b3, a2, c32
   LD b3,  BO,   2 * SIZE
   MADD  c42, b4, a2, c42
   LD b4,  BO,   3 * SIZE
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
   MADD  c11, b1, a1, c11
   LD a2,  AO,   1 * SIZE
   MADD  c21, b2, a1, c21
   addi.d  L, L, -1
   MADD  c31, b3, a1, c31
   addi.d  AO, AO,  2 * SIZE
   MADD  c41, b4, a1, c41
   LD a1,  AO,   0 * SIZE
   MADD  c12, b1, a2, c12
   LD b1,  BO,   4 * SIZE
   MADD  c22, b2, a2, c22
   LD b2,  BO,   5 * SIZE
   MADD  c32, b3, a2, c32
   LD b3,  BO,   6 * SIZE
   MADD  c42, b4, a2, c42
   LD b4,  BO,   7 * SIZE
addi.d BO, BO,  4 * SIZE
   blt $r0,    L, .L36
.L38:
#ifndef TRMMKERNEL
   LD $f22,  CO1,  0 * SIZE
   addi.d  CO3,CO3, 2 * SIZE
   LD $f8,  CO1,  1 * SIZE
   addi.d  CO1,CO1, 2 * SIZE
   LD $f23,  CO2,  0 * SIZE
   addi.d  CO4,CO4, 2 * SIZE
   LD $f9,  CO2,  1 * SIZE
   addi.d  CO2,CO2, 2 * SIZE
   LD $f10,  CO3,  -2 * SIZE
   MADD  c11, c11, ALPHA, $f22
   LD $f11,  CO3,  -1 * SIZE
   MADD  c12, c12, ALPHA, $f8
   LD $f12,  CO4,  -2 * SIZE
   MADD  c21, c21, ALPHA, $f23
   LD $f13,  CO4,  -1 * SIZE
   MADD  c22, c22, ALPHA, $f9
   MADD  c31, c31, ALPHA, $f10
   ST c11,  CO1,  -2 * SIZE
   MADD  c32, c32, ALPHA, $f11
   ST c12,  CO1,  -1 * SIZE
   MADD  c41, c41, ALPHA, $f12
   ST c21,  CO2,  -2 * SIZE
   MADD  c42, c42, ALPHA, $f13
   ST c22,  CO2,  -1 * SIZE
   ST c31,  CO3,  -2 * SIZE
   MTC  c11, $r0
   ST c32,  CO3,  -1 * SIZE
   addi.d  I, I, -1
   ST c41,  CO4,  -2 * SIZE
   MOV c21, c11
   ST c42,  CO4,  -1 * SIZE
   MOV c31, c11
#else
   MUL c11, ALPHA, c11
   addi.d  CO3,CO3, 2 * SIZE
   MUL c12, ALPHA, c12
   addi.d  CO1,CO1, 2 * SIZE
   MUL c21, ALPHA, c21
   addi.d  CO4,CO4, 2 * SIZE
   MUL c22, ALPHA, c22
   addi.d  CO2,CO2, 2 * SIZE
   ST c11,  CO1,  -2 * SIZE
   MUL c31, ALPHA, c31
   ST c12,  CO1,  -1 * SIZE
   MUL c32, ALPHA, c32
   ST c21,  CO2,  -2 * SIZE
   MUL c41, ALPHA, c41
   ST c22,  CO2,  -1 * SIZE
   MUL c42, ALPHA, c42
   ST c31,  CO3,  -2 * SIZE
   MTC  c11, $r0
   ST c32,  CO3,  -1 * SIZE
   addi.d  I, I, -1
   ST c41,  CO4,  -2 * SIZE
   MOV c21, c11
   ST c42,  CO4,  -1 * SIZE
   MOV c31, c11
#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
   sub.d   TEMP, K, KK
#ifdef LEFT
   addi.d  TEMP, TEMP, -2
#else
   addi.d  TEMP, TEMP, -4
#endif
   slli.d  L,    TEMP, 1 + BASE_SHIFT
   slli.d  TEMP, TEMP, 2 + BASE_SHIFT
   add.d   AO, AO, L
   add.d   BO, BO, TEMP
#endif
#ifdef LEFT
   addi.d  KK, KK, 2
#endif
#endif
MOV    c41, c11
   blt $r0,    I, .L31
   .align 3

.L40:
   andi    I,  M, 1
MOV    c61, c11
   bge $r0,    I, .L49
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
   move    BO,  B
#else
   slli.d  L,    KK, 0 + BASE_SHIFT
   slli.d  TEMP, KK, 2 + BASE_SHIFT
   add.d   AO, AO, L
   add.d   BO, B,  TEMP
#endif
   LD a1,  AO,   0 * SIZE
   MOV c71, c11
   LD a2,  AO,   1 * SIZE
   MOV c81, c11
   LD b1,  BO,   0 * SIZE
   LD b2,  BO,   1 * SIZE
   LD b3,  BO,   2 * SIZE
   LD b4,  BO,   3 * SIZE
   LD b5,  BO,   4 * SIZE
   LD b6,  BO,   8 * SIZE
   LD b7,  BO,  12 * SIZE
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
   sub.d   TEMP, K, KK
#elif defined(LEFT)
   addi.d  TEMP, KK, 1
#else
   addi.d  TEMP, KK, 4
#endif
   srai.d  L,  TEMP, 2
   bge $r0,    L, .L45
#else
   LD a1,  AO,   0 * SIZE
   MOV c71, c11
   LD a2,  AO,   1 * SIZE
   MOV c81, c11
   LD b1,  B,   0 * SIZE
   LD b2,  B,   1 * SIZE
   LD b3,  B,   2 * SIZE
   LD b4,  B,   3 * SIZE
   LD b5,  B,   4 * SIZE
   LD b6,  B,   8 * SIZE
   LD b7,  B,  12 * SIZE
   srai.d  L,  K, 2
move   BO,  B
   bge $r0,    L, .L45
#endif
   .align  3
.L42:
   MADD  c11, b1, a1, c11
   LD b1,  BO,  16 * SIZE
   MADD  c21, b2, a1, c21
   LD b2,  BO,   5 * SIZE
   MADD  c31, b3, a1, c31
   LD b3,  BO,   6 * SIZE
   MADD  c41, b4, a1, c41
   LD b4,  BO,   7 * SIZE
   LD a1,  AO,   4 * SIZE
   addi.d  L, L, -1
   MADD  c11, b5, a2, c11
   LD b5,  BO,  20 * SIZE
   MADD  c21, b2, a2, c21
   LD b2,  BO,   9 * SIZE
   MADD  c31, b3, a2, c31
   LD b3,  BO,  10 * SIZE
   MADD  c41, b4, a2, c41
   LD b4,  BO,  11 * SIZE
   LD a2,  AO,   2 * SIZE
   addi.d  AO, AO,  4 * SIZE
   MADD  c11, b6, a2, c11
   LD b6,  BO,  24 * SIZE
   MADD  c21, b2, a2, c21
   LD b2,  BO,  13 * SIZE
   MADD  c31, b3, a2, c31
   LD b3,  BO,  14 * SIZE
   MADD  c41, b4, a2, c41
   LD b4,  BO,  15 * SIZE
   LD a2,  AO,  -1 * SIZE
   addi.d  BO, BO, 16 * SIZE
   MADD  c11, b7, a2, c11
   LD b7,  BO,  12 * SIZE
   MADD  c21, b2, a2, c21
   LD b2,  BO,   1 * SIZE
   MADD  c31, b3, a2, c31
   LD b3,  BO,   2 * SIZE
   MADD  c41, b4, a2, c41
   LD b4,  BO,   3 * SIZE
   LD a2,  AO,   1 * SIZE
   blt $r0,    L, .L42
   .align 3

.L45:
#ifndef TRMMKERNEL
   andi    L,  K, 3
#else
   andi    L,  TEMP, 3
#endif
   bge $r0,    L, .L48
   .align  3
.L46:
   MADD  c11, b1, a1, c11
   LD b1,  BO,   4 * SIZE
   MADD  c21, b2, a1, c21
   LD b2,  BO,   5 * SIZE
   MADD  c31, b3, a1, c31
   LD b3,  BO,   6 * SIZE
   MADD  c41, b4, a1, c41
   LD a1,  AO,   1 * SIZE
   LD b4,  BO,   7 * SIZE
   addi.d  L, L, -1
   addi.d  AO, AO,  1 * SIZE
   MOV a2, a2
addi.d BO, BO,  4 * SIZE
   blt $r0,    L, .L46
.L48:
#ifndef TRMMKERNEL
   LD $f22,  CO1,  0 * SIZE
   LD $f8,  CO2,  0 * SIZE
   LD $f23,  CO3,  0 * SIZE
   LD $f9,  CO4,  0 * SIZE
   MADD  c11, c11, ALPHA, $f22
   MADD  c21, c21, ALPHA, $f8
   MADD  c31, c31, ALPHA, $f23
   MADD  c41, c41, ALPHA, $f9
   ST c11,  CO1,   0 * SIZE
   ST c21,  CO2,   0 * SIZE
   ST c31,  CO3,   0 * SIZE
   ST c41,  CO4,   0 * SIZE
#else
   MUL c11, ALPHA, c11
   MUL c21, ALPHA, c21
   MUL c31, ALPHA, c31
   MUL c41, ALPHA, c41
   ST c11,  CO1,   0 * SIZE
   ST c21,  CO2,   0 * SIZE
   ST c31,  CO3,   0 * SIZE
   ST c41,  CO4,   0 * SIZE
#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
   sub.d   TEMP, K, KK
#ifdef LEFT
   addi.d  TEMP, TEMP, -1
#else
   addi.d  TEMP, TEMP, -4
#endif
   slli.d  L,    TEMP, 0 + BASE_SHIFT
   slli.d  TEMP, TEMP, 2 + BASE_SHIFT
   add.d   AO, AO, L
   add.d   BO, BO, TEMP
#endif
#ifdef LEFT
   addi.d  KK, KK, 1
#endif
#endif
   .align 3

.L49:
#if defined(TRMMKERNEL) && !defined(LEFT)
   addi.d  KK, KK, 4
#endif
   move    B, BO
   .align 3

.L50:
   andi    J,  N, 2
move   AO, A
   bge $r0,    J, .L70
   move    CO1, C
   add.d   CO2, C,      LDC
#if defined(TRMMKERNEL) &&  defined(LEFT)
   move    KK, OFFSET
#endif
   srai.d  I,  M, 1
add.d  C,   CO2,    LDC
   bge $r0,    I, .L60
.L51:
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
   move    BO,  B
#else
   slli.d  L,    KK, 1 + BASE_SHIFT
   slli.d  TEMP, KK, 1 + BASE_SHIFT
   add.d   AO, AO, L
   add.d   BO, B,  TEMP
#endif
   LD a1,  AO,   0 * SIZE
   MTC  c11, $r0
   LD a2,  AO,   1 * SIZE
   MOV c21, c11
   LD a5,  AO,   4 * SIZE
   LD b1,  BO,   0 * SIZE
   MOV c12, c11
   LD b2,  BO,   1 * SIZE
   MOV c22, c11
   LD b3,  BO,   2 * SIZE
   LD b5,  BO,   4 * SIZE
   LD b6,  BO,   8 * SIZE
   LD b7,  BO,  12 * SIZE
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
   sub.d   TEMP, K, KK
#elif defined(LEFT)
   addi.d  TEMP, KK, 2
#else
   addi.d  TEMP, KK, 2
#endif
   srai.d  L,  TEMP, 2
   bge $r0,    L, .L55
#else
   LD a1,  AO,   0 * SIZE
   MTC  c11, $r0
   LD a2,  AO,   1 * SIZE
   MOV c21, c11
   LD a5,  AO,   4 * SIZE
   LD b1,  B,   0 * SIZE
   MOV c12, c11
   LD b2,  B,   1 * SIZE
   MOV c22, c11
   LD b3,  B,   2 * SIZE
   LD b5,  B,   4 * SIZE
   srai.d  L,  K, 2
   LD b6,  B,   8 * SIZE
   LD b7,  B,  12 * SIZE
move   BO,  B
   bge $r0,    L, .L55
#endif
   .align  3
.L52:
   MADD  c11, b1, a1, c11
   LD a3,  AO,   2 * SIZE
   MADD  c21, b2, a1, c21
   LD b4,  BO,   3 * SIZE
   MADD  c12, b1, a2, c12
   LD a4,  AO,   3 * SIZE
   MADD  c22, b2, a2, c22
   LD b1,  BO,   8 * SIZE
   MADD  c11, b3, a3, c11
   LD a1,  AO,   8 * SIZE
   MADD  c21, b4, a3, c21
   LD b2,  BO,   5 * SIZE
   MADD  c12, b3, a4, c12
   LD a2,  AO,   5 * SIZE
   MADD  c22, b4, a4, c22
   LD b3,  BO,   6 * SIZE
   MADD  c11, b5, a5, c11
   LD a3,  AO,   6 * SIZE
   MADD  c21, b2, a5, c21
   LD b4,  BO,   7 * SIZE
   MADD  c12, b5, a2, c12
   LD a4,  AO,   7 * SIZE
   MADD  c22, b2, a2, c22
   LD b5,  BO,  12 * SIZE
   MADD  c11, b3, a3, c11
   LD a5,  AO,  12 * SIZE
   MADD  c21, b4, a3, c21
   LD b2,  BO,   9 * SIZE
   MADD  c12, b3, a4, c12
   LD a2,  AO,   9 * SIZE
   MADD  c22, b4, a4, c22
   LD b3,  BO,  10 * SIZE
   addi.d  AO, AO,  8 * SIZE
   addi.d  L, L, -1
addi.d BO, BO,  8 * SIZE
   blt $r0,    L, .L52
   .align 3

.L55:
#ifndef TRMMKERNEL
   andi    L,  K, 3
#else
   andi    L,  TEMP, 3
#endif
   bge $r0,    L, .L58
   .align  3
.L56:
   MADD  c11, b1, a1, c11
   LD a2,  AO,   1 * SIZE
   MADD  c21, b2, a1, c21
   LD a1,  AO,   2 * SIZE
   MADD  c12, b1, a2, c12
   LD b1,  BO,   2 * SIZE
   MADD  c22, b2, a2, c22
   LD b2,  BO,   3 * SIZE
   addi.d  L, L, -1
   addi.d  AO, AO,  2 * SIZE
addi.d BO, BO,  2 * SIZE
   blt $r0,    L, .L56
.L58:
#ifndef TRMMKERNEL
   LD $f22,  CO1,  0 * SIZE
   addi.d  I, I, -1
   LD $f8,  CO1,  1 * SIZE
   addi.d  CO1,CO1, 2 * SIZE
   LD $f23,  CO2,  0 * SIZE
   LD $f9,  CO2,  1 * SIZE
   addi.d  CO2,CO2, 2 * SIZE
   MADD  c11, c11, ALPHA, $f22
   MADD  c12, c12, ALPHA, $f8
   MADD  c21, c21, ALPHA, $f23
   MADD  c22, c22, ALPHA, $f9
   ST c11,  CO1,  -2 * SIZE
   ST c12,  CO1,  -1 * SIZE
   ST c21,  CO2,  -2 * SIZE
   ST c22,  CO2,  -1 * SIZE
   blt $r0,    I, .L51
#else
   addi.d  I, I, -1
   addi.d  CO1,CO1, 2 * SIZE
   addi.d  CO2,CO2, 2 * SIZE
   MUL c11, ALPHA, c11
   MUL c12, ALPHA, c12
   MUL c21, ALPHA, c21
   MUL c22, ALPHA, c22
   ST c11,  CO1,  -2 * SIZE
   ST c12,  CO1,  -1 * SIZE
   ST c21,  CO2,  -2 * SIZE
   ST c22,  CO2,  -1 * SIZE
#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
   sub.d   TEMP, K, KK
#ifdef LEFT
   addi.d  TEMP, TEMP, -2
#else
   addi.d  TEMP, TEMP, -2
#endif
   slli.d  L,    TEMP, 1 + BASE_SHIFT
   slli.d  TEMP, TEMP, 1 + BASE_SHIFT
   add.d   AO, AO, L
   add.d   BO, BO, TEMP
#endif
#ifdef LEFT
   addi.d  KK, KK, 2
#endif
   blt $r0,    I, .L51
#endif
   .align 3

.L60:
   andi    I,  M, 1
   bge $r0,    I, .L69
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
   move    BO,  B
#else
   slli.d  L,    KK, 0 + BASE_SHIFT
   slli.d  TEMP, KK, 1 + BASE_SHIFT
   add.d   AO, AO, L
   add.d   BO, B,  TEMP
#endif
   LD a1,  AO,   0 * SIZE
   MTC  c11, $r0
   LD a2,  AO,   1 * SIZE
   MOV c21, c11
   LD a3,  AO,   2 * SIZE
   MOV c31, c11
   LD a4,  AO,   3 * SIZE
   MOV c41, c11
   LD b1,  BO,   0 * SIZE
   LD b2,  BO,   1 * SIZE
   LD b3,  BO,   2 * SIZE
   LD b4,  BO,   3 * SIZE
   LD b5,  BO,   4 * SIZE
   LD b6,  BO,   8 * SIZE
   LD b7,  BO,  12 * SIZE
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
   sub.d   TEMP, K, KK
#elif defined(LEFT)
   addi.d  TEMP, KK, 1
#else
   addi.d  TEMP, KK, 2
#endif
   srai.d  L,  TEMP, 2
   bge $r0,    L, .L65
#else
   srai.d  L,  K, 2
   LD a1,  AO,   0 * SIZE
   MTC  c11, $r0
   LD a2,  AO,   1 * SIZE
   MOV c21, c11
   LD a3,  AO,   2 * SIZE
   MOV c31, c11
   LD a4,  AO,   3 * SIZE
   MOV c41, c11
   LD b1,  B,   0 * SIZE
   LD b2,  B,   1 * SIZE
   LD b3,  B,   2 * SIZE
   LD b4,  B,   3 * SIZE
   LD b5,  B,   4 * SIZE
   LD b6,  B,   8 * SIZE
   LD b7,  B,  12 * SIZE
move   BO,  B
   bge $r0,    L, .L65
#endif
   .align  3
.L62:
   MADD  c11, b1, a1, c11
   LD b1,  BO,   4 * SIZE
   MADD  c21, b2, a1, c21
   LD b2,  BO,   5 * SIZE
   MADD  c31, b3, a2, c31
   LD b3,  BO,   6 * SIZE
   MADD  c41, b4, a2, c41
   LD b4,  BO,   7 * SIZE
   LD a1,  AO,   4 * SIZE
   LD a2,  AO,   5 * SIZE
   MADD  c11, b1, a3, c11
   LD b1,  BO,   8 * SIZE
   MADD  c21, b2, a3, c21
   LD b2,  BO,   9 * SIZE
   MADD  c31, b3, a4, c31
   LD b3,  BO,  10 * SIZE
   MADD  c41, b4, a4, c41
   LD b4,  BO,  11 * SIZE
   LD a3,  AO,   6 * SIZE
   LD a4,  AO,   7 * SIZE
   addi.d  L, L, -1
   addi.d  AO, AO,  4 * SIZE
addi.d BO, BO,  8 * SIZE
   blt $r0,    L, .L62
   .align 3

.L65:
#ifndef TRMMKERNEL
   andi    L,  K, 3
#else
   andi    L,  TEMP, 3
#endif
   bge $r0,    L, .L68
   .align  3
.L66:
   MADD  c11, b1, a1, c11
   LD b1,  BO,   2 * SIZE
   MADD  c21, b2, a1, c21
   LD b2,  BO,   3 * SIZE
   LD a1,  AO,   1 * SIZE
   addi.d  L, L, -1
   addi.d  AO, AO,  1 * SIZE
addi.d BO, BO,  2 * SIZE
   blt $r0,    L, .L66
.L68:
#ifndef TRMMKERNEL
   LD $f22,  CO1,  0 * SIZE
   LD $f8,  CO2,  0 * SIZE
   ADD c11, c11, c31
   ADD c21, c21, c41
   MADD  c11, c11, ALPHA, $f22
   MADD  c21, c21, ALPHA, $f8
   ST c11,  CO1,   0 * SIZE
   ST c21,  CO2,   0 * SIZE
#else
   ADD c11, c11, c31
   ADD c21, c21, c41
   MUL c11, ALPHA, c11
   MUL c21, ALPHA, c21
   ST c11,  CO1,   0 * SIZE
   ST c21,  CO2,   0 * SIZE
#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
   sub.d   TEMP, K, KK
#ifdef LEFT
   addi.d  TEMP, TEMP, -1
#else
   addi.d  TEMP, TEMP, -2
#endif
   slli.d  L,    TEMP, 0 + BASE_SHIFT
   slli.d  TEMP, TEMP, 1 + BASE_SHIFT
   add.d   AO, AO, L
   add.d   BO, BO, TEMP
#endif
#ifdef LEFT
   addi.d  KK, KK, 1
#endif
#endif
   .align 3

.L69:
#if defined(TRMMKERNEL) && !defined(LEFT)
   addi.d  KK, KK, 2
#endif
   move    B, BO
   .align 3

.L70:
   andi    J,  N, 1
move   AO, A
   bge $r0,    J, .L999
   move    CO1, C
#if defined(TRMMKERNEL) &&  defined(LEFT)
   move    KK, OFFSET
#endif
   srai.d  I,  M, 1
add.d  C,   CO1,    LDC
   bge $r0,    I, .L80
.L71:
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
   move    BO,  B
#else
   slli.d  L,    KK, 1 + BASE_SHIFT
   slli.d  TEMP, KK, 0 + BASE_SHIFT
   add.d   AO, AO, L
   add.d   BO, B,  TEMP
#endif
   LD a1,  AO,   0 * SIZE
   MTC  c11, $r0
   LD a2,  AO,   1 * SIZE
   MOV c21, c11
   LD a5,  AO,   4 * SIZE
   LD b1,  BO,   0 * SIZE
   MOV c12, c11
   LD b2,  BO,   1 * SIZE
   MOV c22, c11
   LD b3,  BO,   2 * SIZE
   LD b5,  BO,   4 * SIZE
   LD b6,  BO,   8 * SIZE
   LD b7,  BO,  12 * SIZE
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
   sub.d   TEMP, K, KK
#elif defined(LEFT)
   addi.d  TEMP, KK, 2
#else
   addi.d  TEMP, KK, 1
#endif
   srai.d  L,  TEMP, 2
   bge $r0,    L, .L75
#else
   LD a1,  AO,   0 * SIZE
   MTC  c11, $r0
   LD a2,  AO,   1 * SIZE
   MOV c21, c11
   LD a5,  AO,   4 * SIZE
   LD b1,  B,   0 * SIZE
   MOV c12, c11
   LD b2,  B,   1 * SIZE
   MOV c22, c11
   LD b3,  B,   2 * SIZE
   LD b5,  B,   4 * SIZE
   srai.d  L,  K, 2
   LD b6,  B,   8 * SIZE
   LD b7,  B,  12 * SIZE
move   BO,  B
   bge $r0,    L, .L75
#endif
   .align  3
.L72:
   LD a1,  AO,   0 * SIZE
   LD a2,  AO,   1 * SIZE
   LD b1,  BO,   0 * SIZE
   MADD  c11, b1, a1, c11
   MADD  c12, b1, a2, c12
   LD a1,  AO,   2 * SIZE
   LD a2,  AO,   3 * SIZE
   LD b1,  BO,   1 * SIZE
   MADD  c11, b1, a1, c11
   MADD  c12, b1, a2, c12
   LD a1,  AO,   4 * SIZE
   LD a2,  AO,   5 * SIZE
   LD b1,  BO,   2 * SIZE
   MADD  c11, b1, a1, c11
   MADD  c12, b1, a2, c12
   LD a1,  AO,   6 * SIZE
   LD a2,  AO,   7 * SIZE
   LD b1,  BO,   3 * SIZE
   MADD  c11, b1, a1, c11
   MADD  c12, b1, a2, c12
   addi.d  L, L, -1
   addi.d  AO, AO,  8 * SIZE
addi.d BO, BO,  4 * SIZE
   blt $r0,    L, .L72
   .align 3

.L75:
#ifndef TRMMKERNEL
   andi    L,  K, 3
#else
   andi    L,  TEMP, 3
#endif
   bge $r0,    L, .L78
   .align  3
.L76:
   LD a1,  AO,   0 * SIZE
   LD a2,  AO,   1 * SIZE
   LD b1,  BO,   0 * SIZE
   MADD  c11, b1, a1, c11
   MADD  c12, b1, a2, c12
   addi.d  L, L, -1
   addi.d  AO, AO,  2 * SIZE
addi.d BO, BO,  1 * SIZE
   blt $r0,    L, .L76
.L78:
#ifndef TRMMKERNEL
   LD $f22,  CO1,  0 * SIZE
   addi.d  I, I, -1
   LD $f8,  CO1,  1 * SIZE
   addi.d  CO1,CO1, 2 * SIZE
   ADD c11, c11, c21
   ADD c12, c12, c22
   MADD  c11, c11, ALPHA, $f22
   MADD  c12, c12, ALPHA, $f8
   ST c11,  CO1,  -2 * SIZE
   ST c12,  CO1,  -1 * SIZE
   blt $r0,    I, .L71
#else
   ADD c11, c11, c21
   addi.d  I, I, -1
   ADD c12, c12, c22
   addi.d  CO1,CO1, 2 * SIZE
   MUL c11, ALPHA, c11
   MUL c12, ALPHA, c12
   ST c11,  CO1,  -2 * SIZE
   ST c12,  CO1,  -1 * SIZE
#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
   sub.d   TEMP, K, KK
#ifdef LEFT
   addi.d  TEMP, TEMP, -2
#else
   addi.d  TEMP, TEMP, -1
#endif
   slli.d  L,    TEMP, 1 + BASE_SHIFT
   slli.d  TEMP, TEMP, 0 + BASE_SHIFT
   add.d   AO, AO, L
   add.d   BO, BO, TEMP
#endif
#ifdef LEFT
   addi.d  KK, KK, 2
#endif
   blt $r0,    I, .L71
#endif
   .align 3

.L80:
   andi    I,  M, 1
   bge $r0,    I, .L89
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
   move    BO,  B
#else
   slli.d  L,    KK, 0 + BASE_SHIFT
   slli.d  TEMP, KK, 0 + BASE_SHIFT
   add.d   AO, AO, L
   add.d   BO, B,  TEMP
#endif
   LD a1,  AO,   0 * SIZE
   MTC  c11, $r0
   LD a2,  AO,   1 * SIZE
   MOV c21, c11
   LD a3,  AO,   2 * SIZE
   LD a4,  AO,   3 * SIZE
   LD b1,  BO,   0 * SIZE
   LD b2,  BO,   1 * SIZE
   LD b3,  BO,   2 * SIZE
   LD b4,  BO,   3 * SIZE
   LD b5,  BO,   4 * SIZE
   LD b6,  BO,   8 * SIZE
   LD b7,  BO,  12 * SIZE
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
   sub.d   TEMP, K, KK
#elif defined(LEFT)
   addi.d  TEMP, KK, 1
#else
   addi.d  TEMP, KK, 1
#endif
   srai.d  L,  TEMP, 2
   bge $r0,    L, .L85
#else
   LD a1,  AO,   0 * SIZE
   MTC  c11, $r0
   LD a2,  AO,   1 * SIZE
   MOV c21, c11
   LD a3,  AO,   2 * SIZE
   LD a4,  AO,   3 * SIZE
   LD b1,  B,   0 * SIZE
   LD b2,  B,   1 * SIZE
   LD b3,  B,   2 * SIZE
   LD b4,  B,   3 * SIZE
   LD b5,  B,   4 * SIZE
   LD b6,  B,   8 * SIZE
   LD b7,  B,  12 * SIZE
   srai.d  L,  K, 2
move   BO,  B
   bge $r0,    L, .L85
#endif
   .align  3
.L82:
   LD a1,  AO,   0 * SIZE
   LD b1,  BO,   0 * SIZE
   MADD  c11, b1, a1, c11
   LD a1,  AO,   1 * SIZE
   LD b1,  BO,   1 * SIZE
   MADD  c21, b1, a1, c21
   LD a1,  AO,   2 * SIZE
   LD b1,  BO,   2 * SIZE
   MADD  c11, b1, a1, c11
   LD a1,  AO,   3 * SIZE
   LD b1,  BO,   3 * SIZE
   MADD  c21, b1, a1, c21
   addi.d  L, L, -1
   addi.d  AO, AO,  4 * SIZE
addi.d BO, BO,  4 * SIZE
   blt $r0,    L, .L82
   .align 3

.L85:
#ifndef TRMMKERNEL
   andi    L,  K, 3
#else
   andi    L,  TEMP, 3
#endif
   bge $r0,    L, .L88
   .align  3
.L86:
   LD a1,  AO,   0 * SIZE
   LD b1,  BO,   0 * SIZE
   MADD  c11, b1, a1, c11
   addi.d  L, L, -1
   addi.d  AO, AO,  1 * SIZE
addi.d BO, BO,  1 * SIZE
   blt $r0,    L, .L86
.L88:
#ifndef TRMMKERNEL
   LD $f22,  CO1,  0 * SIZE
   ADD c11, c11, c21
   MADD  c11, c11, ALPHA, $f22
   ST c11,  CO1,   0 * SIZE
#else
   ADD c11, c11, c21
   MUL c11, ALPHA, c11
   ST c11,  CO1,   0 * SIZE
#endif
   .align 3

.L89:
#if defined(TRMMKERNEL) && !defined(LEFT)
   addi.d  KK, KK, 1
#endif
   move    B, BO
   .align 3

.L999:
   LDARG  $r23,  $sp,    0
   LDARG  $r24,  $sp,    8
   LDARG  $r25,  $sp,   16
   LDARG  $r26,  $sp,   24
   LDARG  $r27,  $sp,   32
   LDARG  $r28,  $sp,   40
   LDARG  $r29,  $sp,   48
   LDARG  $r30,  $sp,   96
   fld.d  $f24,  $sp,  56
   fld.d  $f25,  $sp,  64
   fld.d  $f26,  $sp,  72
   fld.d  $f27,  $sp,  80
   fld.d  $f28,  $sp,  88
#if defined(TRMMKERNEL)
   LDARG  $r20,  $sp,  104
   LDARG  $r16,  $sp,  112
#endif
#ifndef __64BIT__
   fld.d  $f18,  $sp, 120
   fld.d  $f19,  $sp, 128
   fld.d  $f20,  $sp, 136
   fld.d  $f21,  $sp, 144
#endif
   addi.d  $sp, $sp, 160
   move $r4, $r17
   fmov.d $f0, $f22
   jirl    $r0, $r1, 0x0

   EPILOGUE
