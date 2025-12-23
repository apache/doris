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
#define OFFSET $r11
#define AO     $r12
#define BO     $r13
#define I      $r17
#define J      $r18
#define L      $r29
#define CO1    $r14
#define CO2    $r15
#define CO3    $r23
#define CO4    $r24
#define CO5    $r25
#define CO6    $r26
#define CO7    $r27
#define CO8    $r28
#define KK     $r30
#define TEMP   $r20
#define AORIG  $r16
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

   addi.d  $sp, $sp, -144
   SDARG  $r23,  $sp,    0
   SDARG  $r24,  $sp,    8
   SDARG  $r25,  $sp,   16
   SDARG  $r26,  $sp,   24
   SDARG  $r27,  $sp,   32
   SDARG  $r28,  $sp,   40
   fst.d  $f24,  $sp,  48
   fst.d  $f25,  $sp,  56
   fst.d  $f26,  $sp,  64
   fst.d  $f27,  $sp,  72
   fst.d  $f28,  $sp,  80
   SDARG  $r29,  $sp,   88
   SDARG  $r30,  $sp,   96
   SDARG  $r20,  $sp,  104
   SDARG  $r16,  $sp,  112
#ifndef __64BIT__
   fst.d  $f18,  $sp, 112
   fst.d  $f19,  $sp, 120
   fst.d  $f20,  $sp, 128
   fst.d  $f21,  $sp, 136
#endif
   slli.d     LDC,    LDC, BASE_SHIFT
#ifdef LN
        mul.w   TEMP, M, K
   slli.d  TEMP, TEMP, BASE_SHIFT
   add.d   A, A, TEMP
   slli.d  TEMP, M, BASE_SHIFT
   add.d   C, C, TEMP
#endif
#ifdef RN
        sub.d   KK, $r0, OFFSET
#endif
#ifdef RT
        mul.w   TEMP, N, K
   slli.d  TEMP, TEMP, BASE_SHIFT
   add.d   B, B, TEMP
        mul.w   TEMP, N,    LDC
   add.d   C, C, TEMP
   sub.d   KK, N, OFFSET
#endif
   srai.d  J,  N, 3
nop
   bge $r0,    J, .L30
.L10:
#ifdef RT
   slli.d  TEMP, K, 3 + BASE_SHIFT
   sub.d   B, B, TEMP
   slli.d  TEMP,    LDC, 3
   sub.d   C, C, TEMP
#endif
   move    CO1, C
MTC  c11, $r0
   add.d   CO2, C,      LDC
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
#ifdef LN
   add.d   KK, M, OFFSET
#endif
#ifdef LT
   move    KK, OFFSET
#endif
#if defined(LN) || defined(RT)
   move    AORIG, A
#else
   move    AO, A
#endif
#ifndef RT
   add.d   C,  CO8,    LDC
#endif
MOV    c61, c11
   bge $r0,    I, .L20
.L11:
#if defined(LT) || defined(RN)
   LD a1,  AO,   0 * SIZE
   MOV c71, c11
   LD b1,  B,   0 * SIZE
   MOV c81, c11
   LD a3,  AO,   4 * SIZE
   MOV c12, c11
   LD b2,  B,   1 * SIZE
   MOV c22, c11
   srai.d  L,  KK, 2
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
#else
#ifdef LN
   slli.d  TEMP,   K,  1 + BASE_SHIFT
   sub.d   AORIG, AORIG, TEMP
#endif
   slli.d  L,    KK, 1 + BASE_SHIFT
   slli.d  TEMP, KK, 3 + BASE_SHIFT
   add.d   AO, AORIG, L
   add.d   BO, B,     TEMP
   sub.d   TEMP, K, KK
   LD a1,  AO,   0 * SIZE
   MOV c71, c11
   LD b1,  BO,   0 * SIZE
   MOV c81, c11
   LD a3,  AO,   4 * SIZE
   MOV c12, c11
   LD b2,  BO,   1 * SIZE
   MOV c22, c11
   srai.d  L,  TEMP, 2
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
   bge $r0,    L, .L15
#endif
   MADD  c11, b1, a1, c11
   LD a2,  AO,   1 * SIZE
   MADD  c21, b2, a1, c21
   addi.d  L, L, -1
   MADD  c31, b3, a1, c31
   MADD  c41, b4, a1, c41
   bge $r0,    L, .L13
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
   MADD  c61, b2, a3, c61
   LD a4,  AO,   6 * SIZE
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
   MADD  c61, b2, a3, c61
   LD a4,  AO,   6 * SIZE
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
#if defined(LT) || defined(RN)
   andi    L, KK,  3
#else
   andi    L, TEMP, 3
#endif
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
#if defined(LN) || defined(RT)
#ifdef LN
   addi.d  TEMP, KK, -2
#else
   addi.d  TEMP, KK, -8
#endif
   slli.d  L,    TEMP, 1 + BASE_SHIFT
   slli.d  TEMP, TEMP, 3 + BASE_SHIFT
   add.d   AO, AORIG, L
   add.d   BO, B,     TEMP
#endif
#if defined(LN) || defined(LT)
   LD b1,  BO,   0 * SIZE
   LD b2,  BO,   1 * SIZE
   LD b3,  BO,   2 * SIZE
   LD b4,  BO,   3 * SIZE
   SUB c11, b1, c11
   LD b5,  BO,   4 * SIZE
   SUB c21, b2, c21
   LD b6,  BO,   5 * SIZE
   SUB c31, b3, c31
   LD b7,  BO,   6 * SIZE
   SUB c41, b4, c41
   LD b8,  BO,   7 * SIZE
   SUB c51, b5, c51
   LD b1,  BO,   8 * SIZE
   SUB c61, b6, c61
   LD b2,  BO,   9 * SIZE
   SUB c71, b7, c71
   LD b3,  BO,  10 * SIZE
   SUB c81, b8, c81
   LD b4,  BO,  11 * SIZE
   SUB c12, b1, c12
   LD b5,  BO,  12 * SIZE
   SUB c22, b2, c22
   LD b6,  BO,  13 * SIZE
   SUB c32, b3, c32
   LD b7,  BO,  14 * SIZE
   SUB c42, b4, c42
   LD b8,  BO,  15 * SIZE
   SUB c52, b5, c52
#ifdef LN
   LD b1,  AO,   3 * SIZE
#else
   LD b1,  AO,   0 * SIZE
#endif
   SUB c62, b6, c62
   SUB c72, b7, c72
   SUB c82, b8, c82
#else
   LD b1,  AO,   0 * SIZE
   LD b2,  AO,   1 * SIZE
   LD b3,  AO,   2 * SIZE
   LD b4,  AO,   3 * SIZE
   SUB c11, b1, c11
   LD b5,  AO,   4 * SIZE
   SUB c12, b2, c12
   LD b6,  AO,   5 * SIZE
   SUB c21, b3, c21
   LD b7,  AO,   6 * SIZE
   SUB c22, b4, c22
   LD b8,  AO,   7 * SIZE
   SUB c31, b5, c31
   LD b1,  AO,   8 * SIZE
   SUB c32, b6, c32
   LD b2,  AO,   9 * SIZE
   SUB c41, b7, c41
   LD b3,  AO,  10 * SIZE
   SUB c42, b8, c42
   LD b4,  AO,  11 * SIZE
   LD b5,  AO,  12 * SIZE
   SUB c51, b1, c51
   LD b6,  AO,  13 * SIZE
   SUB c52, b2, c52
   LD b7,  AO,  14 * SIZE
   SUB c61, b3, c61
   LD b8,  AO,  15 * SIZE
   SUB c62, b4, c62
   SUB c71, b5, c71
   SUB c72, b6, c72
   SUB c81, b7, c81
   SUB c82, b8, c82
#endif
#ifdef LN
   MUL c12, b1, c12
   LD b2,  AO,   2 * SIZE
   MUL c22, b1, c22
   MUL c32, b1, c32
   MUL c42, b1, c42
   MUL c52, b1, c52
   MUL c62, b1, c62
   MUL c72, b1, c72
   MUL c82, b1, c82
   NMSUB  c11, c12, b2, c11
   LD b3,  AO,   0 * SIZE
   NMSUB  c21, c22, b2, c21
   NMSUB  c31, c32, b2, c31
   NMSUB  c41, c42, b2, c41
   NMSUB  c51, c52, b2, c51
   NMSUB  c61, c62, b2, c61
   NMSUB  c71, c72, b2, c71
   NMSUB  c81, c82, b2, c81
   MUL c11, b3, c11
   addi.d  CO1, CO1, -2 * SIZE
   MUL c21, b3, c21
   addi.d  CO2, CO2, -2 * SIZE
   MUL c31, b3, c31
   addi.d  CO3, CO3, -2 * SIZE
   MUL c41, b3, c41
   addi.d  CO4, CO4, -2 * SIZE
   MUL c51, b3, c51
   addi.d  CO5, CO5, -2 * SIZE
   MUL c61, b3, c61
   addi.d  CO6, CO6, -2 * SIZE
   MUL c71, b3, c71
   addi.d  CO7, CO7, -2 * SIZE
   MUL c81, b3, c81
   addi.d  CO8, CO8, -2 * SIZE
#endif
#ifdef LT
   MUL c11, b1, c11
   LD b2,  AO,   1 * SIZE
   MUL c21, b1, c21
   MUL c31, b1, c31
   MUL c41, b1, c41
   MUL c51, b1, c51
   MUL c61, b1, c61
   MUL c71, b1, c71
   MUL c81, b1, c81
   NMSUB  c12, c11, b2, c12
   LD b3,  AO,   3 * SIZE
   NMSUB  c22, c21, b2, c22
   NMSUB  c32, c31, b2, c32
   NMSUB  c42, c41, b2, c42
   NMSUB  c52, c51, b2, c52
   NMSUB  c62, c61, b2, c62
   NMSUB  c72, c71, b2, c72
   NMSUB  c82, c81, b2, c82
   MUL c12, b3, c12
   MUL c22, b3, c22
   MUL c32, b3, c32
   MUL c42, b3, c42
   MUL c52, b3, c52
   MUL c62, b3, c62
   MUL c72, b3, c72
   MUL c82, b3, c82
#endif
#ifdef RN
   LD b1,  BO,   0 * SIZE
   LD b2,  BO,   1 * SIZE
   LD b3,  BO,   2 * SIZE
   LD b4,  BO,   3 * SIZE
   MUL c11, b1, c11
   MUL c12, b1, c12
   LD b5,  BO,   4 * SIZE
   NMSUB  c21, c11, b2, c21
   NMSUB  c22, c12, b2, c22
   LD b6,  BO,   5 * SIZE
   NMSUB  c31, c11, b3, c31
   NMSUB  c32, c12, b3, c32
   LD b7,  BO,   6 * SIZE
   NMSUB  c41, c11, b4, c41
   NMSUB  c42, c12, b4, c42
   LD b8,  BO,   7 * SIZE
   NMSUB  c51, c11, b5, c51
   NMSUB  c52, c12, b5, c52
   LD b2,  BO,   9 * SIZE
   NMSUB  c61, c11, b6, c61
   NMSUB  c62, c12, b6, c62
   LD b3,  BO,  10 * SIZE
   NMSUB  c71, c11, b7, c71
   NMSUB  c72, c12, b7, c72
   LD b4,  BO,  11 * SIZE
   NMSUB  c81, c11, b8, c81
   NMSUB  c82, c12, b8, c82
   LD b5,  BO,  12 * SIZE
   MUL c21, b2, c21
   MUL c22, b2, c22
   LD b6,  BO,  13 * SIZE
   NMSUB  c31, c21, b3, c31
   NMSUB  c32, c22, b3, c32
   LD b7,  BO,  14 * SIZE
   NMSUB  c41, c21, b4, c41
   NMSUB  c42, c22, b4, c42
   LD b8,  BO,  15 * SIZE
   NMSUB  c51, c21, b5, c51
   NMSUB  c52, c22, b5, c52
   LD b3,  BO,  18 * SIZE
   NMSUB  c61, c21, b6, c61
   NMSUB  c62, c22, b6, c62
   LD b4,  BO,  19 * SIZE
   NMSUB  c71, c21, b7, c71
   NMSUB  c72, c22, b7, c72
   LD b5,  BO,  20 * SIZE
   NMSUB  c81, c21, b8, c81
   NMSUB  c82, c22, b8, c82
   LD b6,  BO,  21 * SIZE
   MUL c31, b3, c31
   MUL c32, b3, c32
   LD b7,  BO,  22 * SIZE
   NMSUB  c41, c31, b4, c41
   NMSUB  c42, c32, b4, c42
   LD b8,  BO,  23 * SIZE
   NMSUB  c51, c31, b5, c51
   NMSUB  c52, c32, b5, c52
   LD b4,  BO,  27 * SIZE
   NMSUB  c61, c31, b6, c61
   NMSUB  c62, c32, b6, c62
   LD b5,  BO,  28 * SIZE
   NMSUB  c71, c31, b7, c71
   NMSUB  c72, c32, b7, c72
   LD b6,  BO,  29 * SIZE
   NMSUB  c81, c31, b8, c81
   NMSUB  c82, c32, b8, c82
   LD b7,  BO,  30 * SIZE
   MUL c41, b4, c41
   MUL c42, b4, c42
   LD b8,  BO,  31 * SIZE
   NMSUB  c51, c41, b5, c51
   NMSUB  c52, c42, b5, c52
   LD b5,  BO,  36 * SIZE
   NMSUB  c61, c41, b6, c61
   NMSUB  c62, c42, b6, c62
   LD b6,  BO,  37 * SIZE
   NMSUB  c71, c41, b7, c71
   NMSUB  c72, c42, b7, c72
   LD b7,  BO,  38 * SIZE
   NMSUB  c81, c41, b8, c81
   NMSUB  c82, c42, b8, c82
   LD b8,  BO,  39 * SIZE
   MUL c51, b5, c51
   MUL c52, b5, c52
   NMSUB  c61, c51, b6, c61
   NMSUB  c62, c52, b6, c62
   LD b6,  BO,  45 * SIZE
   NMSUB  c71, c51, b7, c71
   NMSUB  c72, c52, b7, c72
   LD b7,  BO,  46 * SIZE
   NMSUB  c81, c51, b8, c81
   NMSUB  c82, c52, b8, c82
   LD b8,  BO,  47 * SIZE
   MUL c61, b6, c61
   MUL c62, b6, c62
   NMSUB  c71, c61, b7, c71
   NMSUB  c72, c62, b7, c72
   LD b7,  BO,  54 * SIZE
   NMSUB  c81, c61, b8, c81
   NMSUB  c82, c62, b8, c82
   LD b8,  BO,  55 * SIZE
   MUL c71, b7, c71
   MUL c72, b7, c72
   NMSUB  c81, c71, b8, c81
   NMSUB  c82, c72, b8, c82
   LD b8,  BO,  63 * SIZE
   MUL c81, b8, c81
   MUL c82, b8, c82
#endif
#ifdef RT
   LD b1,  BO,  63 * SIZE
   LD b2,  BO,  62 * SIZE
   LD b3,  BO,  61 * SIZE
   LD b4,  BO,  60 * SIZE
   MUL c81, b1, c81
   MUL c82, b1, c82
   LD b5,  BO,  59 * SIZE
   NMSUB  c71, c81, b2, c71
   NMSUB  c72, c82, b2, c72
   LD b6,  BO,  58 * SIZE
   NMSUB  c61, c81, b3, c61
   NMSUB  c62, c82, b3, c62
   LD b7,  BO,  57 * SIZE
   NMSUB  c51, c81, b4, c51
   NMSUB  c52, c82, b4, c52
   LD b8,  BO,  56 * SIZE
   NMSUB  c41, c81, b5, c41
   NMSUB  c42, c82, b5, c42
   LD b2,  BO,  54 * SIZE
   NMSUB  c31, c81, b6, c31
   NMSUB  c32, c82, b6, c32
   LD b3,  BO,  53 * SIZE
   NMSUB  c21, c81, b7, c21
   NMSUB  c22, c82, b7, c22
   LD b4,  BO,  52 * SIZE
   NMSUB  c11, c81, b8, c11
   NMSUB  c12, c82, b8, c12
   LD b5,  BO,  51 * SIZE
   MUL c71, b2, c71
   MUL c72, b2, c72
   LD b6,  BO,  50 * SIZE
   NMSUB  c61, c71, b3, c61
   NMSUB  c62, c72, b3, c62
   LD b7,  BO,  49 * SIZE
   NMSUB  c51, c71, b4, c51
   NMSUB  c52, c72, b4, c52
   LD b8,  BO,  48 * SIZE
   NMSUB  c41, c71, b5, c41
   NMSUB  c42, c72, b5, c42
   LD b3,  BO,  45 * SIZE
   NMSUB  c31, c71, b6, c31
   NMSUB  c32, c72, b6, c32
   LD b4,  BO,  44 * SIZE
   NMSUB  c21, c71, b7, c21
   NMSUB  c22, c72, b7, c22
   LD b5,  BO,  43 * SIZE
   NMSUB  c11, c71, b8, c11
   NMSUB  c12, c72, b8, c12
   LD b6,  BO,  42 * SIZE
   MUL c61, b3, c61
   MUL c62, b3, c62
   LD b7,  BO,  41 * SIZE
   NMSUB  c51, c61, b4, c51
   NMSUB  c52, c62, b4, c52
   LD b8,  BO,  40 * SIZE
   NMSUB  c41, c61, b5, c41
   NMSUB  c42, c62, b5, c42
   LD b4,  BO,  36 * SIZE
   NMSUB  c31, c61, b6, c31
   NMSUB  c32, c62, b6, c32
   LD b5,  BO,  35 * SIZE
   NMSUB  c21, c61, b7, c21
   NMSUB  c22, c62, b7, c22
   LD b6,  BO,  34 * SIZE
   NMSUB  c11, c61, b8, c11
   NMSUB  c12, c62, b8, c12
   LD b7,  BO,  33 * SIZE
   MUL c51, b4, c51
   MUL c52, b4, c52
   LD b8,  BO,  32 * SIZE
   NMSUB  c41, c51, b5, c41
   NMSUB  c42, c52, b5, c42
   LD b5,  BO,  27 * SIZE
   NMSUB  c31, c51, b6, c31
   NMSUB  c32, c52, b6, c32
   LD b6,  BO,  26 * SIZE
   NMSUB  c21, c51, b7, c21
   NMSUB  c22, c52, b7, c22
   LD b7,  BO,  25 * SIZE
   NMSUB  c11, c51, b8, c11
   NMSUB  c12, c52, b8, c12
   LD b8,  BO,  24 * SIZE
   MUL c41, b5, c41
   MUL c42, b5, c42
   NMSUB  c31, c41, b6, c31
   NMSUB  c32, c42, b6, c32
   LD b6,  BO,  18 * SIZE
   NMSUB  c21, c41, b7, c21
   NMSUB  c22, c42, b7, c22
   LD b7,  BO,  17 * SIZE
   NMSUB  c11, c41, b8, c11
   NMSUB  c12, c42, b8, c12
   LD b8,  BO,  16 * SIZE
   MUL c31, b6, c31
   MUL c32, b6, c32
   NMSUB  c21, c31, b7, c21
   NMSUB  c22, c32, b7, c22
   LD b7,  BO,   9 * SIZE
   NMSUB  c11, c31, b8, c11
   NMSUB  c12, c32, b8, c12
   LD b8,  BO,   8 * SIZE
   MUL c21, b7, c21
   MUL c22, b7, c22
   NMSUB  c11, c21, b8, c11
   NMSUB  c12, c22, b8, c12
   LD b8,  BO,   0 * SIZE
   MUL c11, b8, c11
   MUL c12, b8, c12
#endif
#if defined(LN) || defined(LT)
   ST c11,  BO,   0 * SIZE
   ST c21,  BO,   1 * SIZE
   ST c31,  BO,   2 * SIZE
   ST c41,  BO,   3 * SIZE
   ST c51,  BO,   4 * SIZE
   ST c61,  BO,   5 * SIZE
   ST c71,  BO,   6 * SIZE
   ST c81,  BO,   7 * SIZE
   ST c12,  BO,   8 * SIZE
   ST c22,  BO,   9 * SIZE
   ST c32,  BO,  10 * SIZE
   ST c42,  BO,  11 * SIZE
   ST c52,  BO,  12 * SIZE
   ST c62,  BO,  13 * SIZE
   ST c72,  BO,  14 * SIZE
   ST c82,  BO,  15 * SIZE
#else
   ST c11,  AO,   0 * SIZE
   ST c12,  AO,   1 * SIZE
   ST c21,  AO,   2 * SIZE
   ST c22,  AO,   3 * SIZE
   ST c31,  AO,   4 * SIZE
   ST c32,  AO,   5 * SIZE
   ST c41,  AO,   6 * SIZE
   ST c42,  AO,   7 * SIZE
   ST c51,  AO,   8 * SIZE
   ST c52,  AO,   9 * SIZE
   ST c61,  AO,  10 * SIZE
   ST c62,  AO,  11 * SIZE
   ST c71,  AO,  12 * SIZE
   ST c72,  AO,  13 * SIZE
   ST c81,  AO,  14 * SIZE
   ST c82,  AO,  15 * SIZE
#endif
   ST c11,  CO1,   0 * SIZE
   ST c12,  CO1,   1 * SIZE
   ST c21,  CO2,   0 * SIZE
   ST c22,  CO2,   1 * SIZE
   ST c31,  CO3,   0 * SIZE
   ST c32,  CO3,   1 * SIZE
   ST c41,  CO4,   0 * SIZE
   ST c42,  CO4,   1 * SIZE
   ST c51,  CO5,   0 * SIZE
   ST c52,  CO5,   1 * SIZE
   ST c61,  CO6,   0 * SIZE
   ST c62,  CO6,   1 * SIZE
   ST c71,  CO7,   0 * SIZE
   ST c72,  CO7,   1 * SIZE
   ST c81,  CO8,   0 * SIZE
   ST c82,  CO8,   1 * SIZE
MTC  a1, $r0
#ifndef LN
   addi.d  CO1, CO1, 2 * SIZE
   addi.d  CO2, CO2, 2 * SIZE
   addi.d  CO3, CO3, 2 * SIZE
   addi.d  CO4, CO4, 2 * SIZE
   addi.d  CO5, CO5, 2 * SIZE
   addi.d  CO6, CO6, 2 * SIZE
   addi.d  CO7, CO7, 2 * SIZE
   addi.d  CO8, CO8, 2 * SIZE
#endif
   MOV c11, a1
   MOV c21, a1
#ifdef RT
   slli.d  TEMP, K, 1 + BASE_SHIFT
   add.d   AORIG, AORIG, TEMP
#endif
   MOV c31, a1
   MOV c41, a1
#if defined(LT) || defined(RN)
   sub.d   TEMP, K, KK
   slli.d  L,    TEMP, 1 + BASE_SHIFT
   slli.d  TEMP, TEMP, 3 + BASE_SHIFT
   add.d   AO, AO, L
   add.d   BO, BO, TEMP
#endif
#ifdef LT
   addi.d  KK, KK, 2
#endif
#ifdef LN
   addi.d  KK, KK, -2
#endif
   addi.d  I, I, -1
   MOV c51, a1
MOV    c61, a1
   blt $r0,    I, .L11
   .align 3

.L20:
   andi    I,  M, 1
   MOV c61, c11
MOV    c71, c11
   bge $r0,    I, .L29
#if defined(LT) || defined(RN)
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
   srai.d  L,  KK, 2
   MOV c81, c11
move   BO,  B
   bge $r0,    L, .L25
#else
#ifdef LN
   slli.d  TEMP,   K,  0 + BASE_SHIFT
   sub.d   AORIG, AORIG, TEMP
#endif
   slli.d  L,    KK, 0 + BASE_SHIFT
   slli.d  TEMP, KK, 3 + BASE_SHIFT
   add.d   AO, AORIG, L
   add.d   BO, B,     TEMP
   sub.d   TEMP, K, KK
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
   srai.d  L,  TEMP, 2
   MOV c81, c11
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
#if defined(LT) || defined(RN)
   andi    L, KK,  3
#else
   andi    L, TEMP, 3
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
#if defined(LN) || defined(RT)
#ifdef LN
   addi.d  TEMP, KK, -1
#else
   addi.d  TEMP, KK, -8
#endif
   slli.d  L,    TEMP, 0 + BASE_SHIFT
   slli.d  TEMP, TEMP, 3 + BASE_SHIFT
   add.d   AO, AORIG, L
   add.d   BO, B,     TEMP
#endif
#if defined(LN) || defined(LT)
   LD b1,  BO,   0 * SIZE
   LD b2,  BO,   1 * SIZE
   LD b3,  BO,   2 * SIZE
   LD b4,  BO,   3 * SIZE
   LD b5,  BO,   4 * SIZE
   LD b6,  BO,   5 * SIZE
   LD b7,  BO,   6 * SIZE
   LD b8,  BO,   7 * SIZE
   SUB c11, b1, c11
   SUB c21, b2, c21
   SUB c31, b3, c31
   SUB c41, b4, c41
   SUB c51, b5, c51
   SUB c61, b6, c61
   SUB c71, b7, c71
   SUB c81, b8, c81
#else
   LD b1,  AO,   0 * SIZE
   LD b2,  AO,   1 * SIZE
   LD b3,  AO,   2 * SIZE
   LD b4,  AO,   3 * SIZE
   LD b5,  AO,   4 * SIZE
   LD b6,  AO,   5 * SIZE
   LD b7,  AO,   6 * SIZE
   LD b8,  AO,   7 * SIZE
   SUB c11, b1, c11
   SUB c21, b2, c21
   SUB c31, b3, c31
   SUB c41, b4, c41
   SUB c51, b5, c51
   SUB c61, b6, c61
   SUB c71, b7, c71
   SUB c81, b8, c81
#endif
#if defined(LN) || defined(LT)
   LD b1,  AO,   0 * SIZE
   MUL c11, b1, c11
   MUL c21, b1, c21
   MUL c31, b1, c31
   MUL c41, b1, c41
   MUL c51, b1, c51
   MUL c61, b1, c61
   MUL c71, b1, c71
   MUL c81, b1, c81
#endif
#ifdef RN
   LD b1,  BO,   0 * SIZE
   LD b2,  BO,   1 * SIZE
   LD b3,  BO,   2 * SIZE
   LD b4,  BO,   3 * SIZE
   LD b5,  BO,   4 * SIZE
   LD b6,  BO,   5 * SIZE
   LD b7,  BO,   6 * SIZE
   LD b8,  BO,   7 * SIZE
   MUL c11, b1, c11
   NMSUB  c21, c11, b2, c21
   NMSUB  c31, c11, b3, c31
   NMSUB  c41, c11, b4, c41
   NMSUB  c51, c11, b5, c51
   NMSUB  c61, c11, b6, c61
   NMSUB  c71, c11, b7, c71
   NMSUB  c81, c11, b8, c81
   LD b2,  BO,   9 * SIZE
   LD b3,  BO,  10 * SIZE
   LD b4,  BO,  11 * SIZE
   LD b5,  BO,  12 * SIZE
   LD b6,  BO,  13 * SIZE
   LD b7,  BO,  14 * SIZE
   LD b8,  BO,  15 * SIZE
   MUL c21, b2, c21
   NMSUB  c31, c21, b3, c31
   NMSUB  c41, c21, b4, c41
   NMSUB  c51, c21, b5, c51
   NMSUB  c61, c21, b6, c61
   NMSUB  c71, c21, b7, c71
   NMSUB  c81, c21, b8, c81
   LD b3,  BO,  18 * SIZE
   LD b4,  BO,  19 * SIZE
   LD b5,  BO,  20 * SIZE
   LD b6,  BO,  21 * SIZE
   LD b7,  BO,  22 * SIZE
   LD b8,  BO,  23 * SIZE
   MUL c31, b3, c31
   NMSUB  c41, c31, b4, c41
   NMSUB  c51, c31, b5, c51
   NMSUB  c61, c31, b6, c61
   NMSUB  c71, c31, b7, c71
   NMSUB  c81, c31, b8, c81
   LD b4,  BO,  27 * SIZE
   LD b5,  BO,  28 * SIZE
   LD b6,  BO,  29 * SIZE
   LD b7,  BO,  30 * SIZE
   LD b8,  BO,  31 * SIZE
   MUL c41, b4, c41
   NMSUB  c51, c41, b5, c51
   NMSUB  c61, c41, b6, c61
   NMSUB  c71, c41, b7, c71
   NMSUB  c81, c41, b8, c81
   LD b5,  BO,  36 * SIZE
   LD b6,  BO,  37 * SIZE
   LD b7,  BO,  38 * SIZE
   LD b8,  BO,  39 * SIZE
   MUL c51, b5, c51
   NMSUB  c61, c51, b6, c61
   NMSUB  c71, c51, b7, c71
   NMSUB  c81, c51, b8, c81
   LD b6,  BO,  45 * SIZE
   LD b7,  BO,  46 * SIZE
   LD b8,  BO,  47 * SIZE
   MUL c61, b6, c61
   NMSUB  c71, c61, b7, c71
   NMSUB  c81, c61, b8, c81
   LD b7,  BO,  54 * SIZE
   LD b8,  BO,  55 * SIZE
   MUL c71, b7, c71
   NMSUB  c81, c71, b8, c81
   LD b8,  BO,  63 * SIZE
   MUL c81, b8, c81
#endif
#ifdef RT
   LD b1,  BO,  63 * SIZE
   LD b2,  BO,  62 * SIZE
   LD b3,  BO,  61 * SIZE
   LD b4,  BO,  60 * SIZE
   LD b5,  BO,  59 * SIZE
   LD b6,  BO,  58 * SIZE
   LD b7,  BO,  57 * SIZE
   LD b8,  BO,  56 * SIZE
   MUL c81, b1, c81
   NMSUB  c71, c81, b2, c71
   NMSUB  c61, c81, b3, c61
   NMSUB  c51, c81, b4, c51
   NMSUB  c41, c81, b5, c41
   NMSUB  c31, c81, b6, c31
   NMSUB  c21, c81, b7, c21
   NMSUB  c11, c81, b8, c11
   LD b2,  BO,  54 * SIZE
   LD b3,  BO,  53 * SIZE
   LD b4,  BO,  52 * SIZE
   LD b5,  BO,  51 * SIZE
   LD b6,  BO,  50 * SIZE
   LD b7,  BO,  49 * SIZE
   LD b8,  BO,  48 * SIZE
   MUL c71, b2, c71
   NMSUB  c61, c71, b3, c61
   NMSUB  c51, c71, b4, c51
   NMSUB  c41, c71, b5, c41
   NMSUB  c31, c71, b6, c31
   NMSUB  c21, c71, b7, c21
   NMSUB  c11, c71, b8, c11
   LD b3,  BO,  45 * SIZE
   LD b4,  BO,  44 * SIZE
   LD b5,  BO,  43 * SIZE
   LD b6,  BO,  42 * SIZE
   LD b7,  BO,  41 * SIZE
   LD b8,  BO,  40 * SIZE
   MUL c61, b3, c61
   NMSUB  c51, c61, b4, c51
   NMSUB  c41, c61, b5, c41
   NMSUB  c31, c61, b6, c31
   NMSUB  c21, c61, b7, c21
   NMSUB  c11, c61, b8, c11
   LD b4,  BO,  36 * SIZE
   LD b5,  BO,  35 * SIZE
   LD b6,  BO,  34 * SIZE
   LD b7,  BO,  33 * SIZE
   LD b8,  BO,  32 * SIZE
   MUL c51, b4, c51
   NMSUB  c41, c51, b5, c41
   NMSUB  c31, c51, b6, c31
   NMSUB  c21, c51, b7, c21
   NMSUB  c11, c51, b8, c11
   LD b5,  BO,  27 * SIZE
   LD b6,  BO,  26 * SIZE
   LD b7,  BO,  25 * SIZE
   LD b8,  BO,  24 * SIZE
   MUL c41, b5, c41
   NMSUB  c31, c41, b6, c31
   NMSUB  c21, c41, b7, c21
   NMSUB  c11, c41, b8, c11
   LD b6,  BO,  18 * SIZE
   LD b7,  BO,  17 * SIZE
   LD b8,  BO,  16 * SIZE
   MUL c31, b6, c31
   NMSUB  c21, c31, b7, c21
   NMSUB  c11, c31, b8, c11
   LD b7,  BO,   9 * SIZE
   LD b8,  BO,   8 * SIZE
   MUL c21, b7, c21
   NMSUB  c11, c21, b8, c11
   LD b8,  BO,   0 * SIZE
   MUL c11, b8, c11
#endif
#ifdef LN
   addi.d  CO1, CO1, -1 * SIZE
   addi.d  CO2, CO2, -1 * SIZE
   addi.d  CO3, CO3, -1 * SIZE
   addi.d  CO4, CO4, -1 * SIZE
   addi.d  CO5, CO5, -1 * SIZE
   addi.d  CO6, CO6, -1 * SIZE
   addi.d  CO7, CO7, -1 * SIZE
   addi.d  CO8, CO8, -1 * SIZE
#endif
#if defined(LN) || defined(LT)
   ST c11,  BO,   0 * SIZE
   ST c21,  BO,   1 * SIZE
   ST c31,  BO,   2 * SIZE
   ST c41,  BO,   3 * SIZE
   ST c51,  BO,   4 * SIZE
   ST c61,  BO,   5 * SIZE
   ST c71,  BO,   6 * SIZE
   ST c81,  BO,   7 * SIZE
#else
   ST c11,  AO,   0 * SIZE
   ST c21,  AO,   1 * SIZE
   ST c31,  AO,   2 * SIZE
   ST c41,  AO,   3 * SIZE
   ST c51,  AO,   4 * SIZE
   ST c61,  AO,   5 * SIZE
   ST c71,  AO,   6 * SIZE
   ST c81,  AO,   7 * SIZE
#endif
   ST c11,  CO1,   0 * SIZE
   ST c21,  CO2,   0 * SIZE
   ST c31,  CO3,   0 * SIZE
   ST c41,  CO4,   0 * SIZE
   ST c51,  CO5,   0 * SIZE
   ST c61,  CO6,   0 * SIZE
   ST c71,  CO7,   0 * SIZE
   ST c81,  CO8,   0 * SIZE
#ifndef LN
   addi.d  CO1, CO1, 1 * SIZE
   addi.d  CO2, CO2, 1 * SIZE
   addi.d  CO3, CO3, 1 * SIZE
   addi.d  CO4, CO4, 1 * SIZE
   addi.d  CO5, CO5, 1 * SIZE
   addi.d  CO6, CO6, 1 * SIZE
   addi.d  CO7, CO7, 1 * SIZE
   addi.d  CO8, CO8, 1 * SIZE
#endif
#ifdef RT
   slli.d  TEMP, K, BASE_SHIFT
   add.d   AORIG, AORIG, TEMP
#endif
#if defined(LT) || defined(RN)
   sub.d   TEMP, K, KK
   slli.d  L,    TEMP, 0 + BASE_SHIFT
   slli.d  TEMP, TEMP, 3 + BASE_SHIFT
   add.d   AO, AO, L
   add.d   BO, BO, TEMP
#endif
#ifdef LT
   addi.d  KK, KK, 1
#endif
#ifdef LN
   addi.d  KK, KK, -1
#endif
   .align 3

.L29:
#ifdef LN
   slli.d  TEMP, K, 3 + BASE_SHIFT
   add.d   B, B, TEMP
#endif
#if defined(LT) || defined(RN)
   move    B,  BO
#endif
#ifdef RN
   addi.d  KK, KK,  8
#endif
#ifdef RT
   addi.d  KK, KK, -8
#endif
   blt $r0,    J, .L10
   .align 3

.L30:
   andi    J,  N, 4
move   AO, A
   bge $r0,    J, .L50
#ifdef RT
   slli.d  TEMP, K, 2 + BASE_SHIFT
   sub.d   B, B, TEMP
   slli.d  TEMP,    LDC, 2
   sub.d   C, C, TEMP
#endif
   move    CO1, C
MTC  c11, $r0
   add.d   CO2, C,      LDC
   add.d   CO3, CO2,    LDC
   add.d   CO4, CO3,    LDC
   MOV c21, c11
   srai.d  I,  M, 1
   MOV c31, c11
#ifdef LN
   add.d   KK, M, OFFSET
#endif
#ifdef LT
   move    KK, OFFSET
#endif
#if defined(LN) || defined(RT)
   move    AORIG, A
#else
   move    AO, A
#endif
#ifndef RT
   add.d   C,  CO4,    LDC
#endif
MOV    c41, c11
   bge $r0,    I, .L40
.L31:
#if defined(LT) || defined(RN)
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
   srai.d  L,  KK, 2
   LD b6,  B,   8 * SIZE
   LD b7,  B,  12 * SIZE
move   BO,  B
   bge $r0,    L, .L35
#else
#ifdef LN
   slli.d  TEMP,   K,  1 + BASE_SHIFT
   sub.d   AORIG, AORIG, TEMP
#endif
   slli.d  L,    KK, 1 + BASE_SHIFT
   slli.d  TEMP, KK, 2 + BASE_SHIFT
   add.d   AO, AORIG, L
   add.d   BO, B,     TEMP
   sub.d   TEMP, K, KK
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
   srai.d  L,  TEMP, 2
   LD b6,  BO,   8 * SIZE
   LD b7,  BO,  12 * SIZE
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
#if defined(LT) || defined(RN)
   andi    L, KK,  3
#else
   andi    L, TEMP, 3
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
#if defined(LN) || defined(RT)
#ifdef LN
   addi.d  TEMP, KK, -2
#else
   addi.d  TEMP, KK, -4
#endif
   slli.d  L,    TEMP, 1 + BASE_SHIFT
   slli.d  TEMP, TEMP, 2 + BASE_SHIFT
   add.d   AO, AORIG, L
   add.d   BO, B,     TEMP
#endif
#if defined(LN) || defined(LT)
   LD b1,  BO,   0 * SIZE
   LD b2,  BO,   1 * SIZE
   LD b3,  BO,   2 * SIZE
   LD b4,  BO,   3 * SIZE
   LD b5,  BO,   4 * SIZE
   LD b6,  BO,   5 * SIZE
   LD b7,  BO,   6 * SIZE
   LD b8,  BO,   7 * SIZE
   SUB c11, b1, c11
   SUB c21, b2, c21
   SUB c31, b3, c31
   SUB c41, b4, c41
   SUB c12, b5, c12
   SUB c22, b6, c22
   SUB c32, b7, c32
   SUB c42, b8, c42
#else
   LD b1,  AO,   0 * SIZE
   LD b2,  AO,   1 * SIZE
   LD b3,  AO,   2 * SIZE
   LD b4,  AO,   3 * SIZE
   LD b5,  AO,   4 * SIZE
   LD b6,  AO,   5 * SIZE
   LD b7,  AO,   6 * SIZE
   LD b8,  AO,   7 * SIZE
   SUB c11, b1, c11
   SUB c12, b2, c12
   SUB c21, b3, c21
   SUB c22, b4, c22
   SUB c31, b5, c31
   SUB c32, b6, c32
   SUB c41, b7, c41
   SUB c42, b8, c42
#endif
#ifdef LN
   LD b1,  AO,   3 * SIZE
   LD b2,  AO,   2 * SIZE
   LD b3,  AO,   0 * SIZE
   MUL c12, b1, c12
   MUL c22, b1, c22
   MUL c32, b1, c32
   MUL c42, b1, c42
   NMSUB  c11, c12, b2, c11
   NMSUB  c21, c22, b2, c21
   NMSUB  c31, c32, b2, c31
   NMSUB  c41, c42, b2, c41
   MUL c11, b3, c11
   MUL c21, b3, c21
   MUL c31, b3, c31
   MUL c41, b3, c41
#endif
#ifdef LT
   LD b1,  AO,   0 * SIZE
   LD b2,  AO,   1 * SIZE
   LD b3,  AO,   3 * SIZE
   MUL c11, b1, c11
   MUL c21, b1, c21
   MUL c31, b1, c31
   MUL c41, b1, c41
   NMSUB  c12, c11, b2, c12
   NMSUB  c22, c21, b2, c22
   NMSUB  c32, c31, b2, c32
   NMSUB  c42, c41, b2, c42
   MUL c12, b3, c12
   MUL c22, b3, c22
   MUL c32, b3, c32
   MUL c42, b3, c42
#endif
#ifdef RN
   LD b1,  BO,   0 * SIZE
   LD b2,  BO,   1 * SIZE
   LD b3,  BO,   2 * SIZE
   LD b4,  BO,   3 * SIZE
   MUL c11, b1, c11
   MUL c12, b1, c12
   NMSUB  c21, c11, b2, c21
   NMSUB  c22, c12, b2, c22
   NMSUB  c31, c11, b3, c31
   NMSUB  c32, c12, b3, c32
   NMSUB  c41, c11, b4, c41
   NMSUB  c42, c12, b4, c42
   LD b2,  BO,   5 * SIZE
   LD b3,  BO,   6 * SIZE
   LD b4,  BO,   7 * SIZE
   MUL c21, b2, c21
   MUL c22, b2, c22
   NMSUB  c31, c21, b3, c31
   NMSUB  c32, c22, b3, c32
   NMSUB  c41, c21, b4, c41
   NMSUB  c42, c22, b4, c42
   LD b3,  BO,  10 * SIZE
   LD b4,  BO,  11 * SIZE
   MUL c31, b3, c31
   MUL c32, b3, c32
   NMSUB  c41, c31, b4, c41
   NMSUB  c42, c32, b4, c42
   LD b4,  BO,  15 * SIZE
   MUL c41, b4, c41
   MUL c42, b4, c42
#endif
#ifdef RT
   LD b5,  BO,  15 * SIZE
   LD b6,  BO,  14 * SIZE
   LD b7,  BO,  13 * SIZE
   LD b8,  BO,  12 * SIZE
   MUL c41, b5, c41
   MUL c42, b5, c42
   NMSUB  c31, c41, b6, c31
   NMSUB  c32, c42, b6, c32
   NMSUB  c21, c41, b7, c21
   NMSUB  c22, c42, b7, c22
   NMSUB  c11, c41, b8, c11
   NMSUB  c12, c42, b8, c12
   LD b6,  BO,  10 * SIZE
   LD b7,  BO,   9 * SIZE
   LD b8,  BO,   8 * SIZE
   MUL c31, b6, c31
   MUL c32, b6, c32
   NMSUB  c21, c31, b7, c21
   NMSUB  c22, c32, b7, c22
   NMSUB  c11, c31, b8, c11
   NMSUB  c12, c32, b8, c12
   LD b7,  BO,   5 * SIZE
   LD b8,  BO,   4 * SIZE
   MUL c21, b7, c21
   MUL c22, b7, c22
   NMSUB  c11, c21, b8, c11
   NMSUB  c12, c22, b8, c12
   LD b8,  BO,   0 * SIZE
   MUL c11, b8, c11
   MUL c12, b8, c12
#endif
#ifdef LN
   addi.d  CO1, CO1, -2 * SIZE
   addi.d  CO2, CO2, -2 * SIZE
   addi.d  CO3, CO3, -2 * SIZE
   addi.d  CO4, CO4, -2 * SIZE
#endif
#if defined(LN) || defined(LT)
   ST c11,  BO,   0 * SIZE
   ST c21,  BO,   1 * SIZE
   ST c31,  BO,   2 * SIZE
   ST c41,  BO,   3 * SIZE
   ST c12,  BO,   4 * SIZE
   ST c22,  BO,   5 * SIZE
   ST c32,  BO,   6 * SIZE
   ST c42,  BO,   7 * SIZE
#else
   ST c11,  AO,   0 * SIZE
   ST c12,  AO,   1 * SIZE
   ST c21,  AO,   2 * SIZE
   ST c22,  AO,   3 * SIZE
   ST c31,  AO,   4 * SIZE
   ST c32,  AO,   5 * SIZE
   ST c41,  AO,   6 * SIZE
   ST c42,  AO,   7 * SIZE
#endif
   ST c11,  CO1,   0 * SIZE
   ST c12,  CO1,   1 * SIZE
   ST c21,  CO2,   0 * SIZE
   ST c22,  CO2,   1 * SIZE
   ST c31,  CO3,   0 * SIZE
   ST c32,  CO3,   1 * SIZE
   ST c41,  CO4,   0 * SIZE
   ST c42,  CO4,   1 * SIZE
#ifndef LN
   addi.d  CO1, CO1, 2 * SIZE
   addi.d  CO2, CO2, 2 * SIZE
   addi.d  CO3, CO3, 2 * SIZE
   addi.d  CO4, CO4, 2 * SIZE
#endif
#ifdef RT
   slli.d  TEMP, K, 1 + BASE_SHIFT
   add.d   AORIG, AORIG, TEMP
#endif
#if defined(LT) || defined(RN)
   sub.d   TEMP, K, KK
   slli.d  L,    TEMP, 1 + BASE_SHIFT
   slli.d  TEMP, TEMP, 2 + BASE_SHIFT
   add.d   AO, AO, L
   add.d   BO, BO, TEMP
#endif
#ifdef LT
   addi.d  KK, KK, 2
#endif
#ifdef LN
   addi.d  KK, KK, -2
#endif
MTC  a1, $r0
   MOV c11, a1
   MOV c21, a1
   MOV c31, a1
   addi.d  I, I, -1
MOV    c41, c11
   blt $r0,    I, .L31
   .align 3

.L40:
   andi    I,  M, 1
MOV    c61, c11
   bge $r0,    I, .L49
#if defined(LT) || defined(RN)
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
   srai.d  L,  KK, 2
move   BO,  B
   bge $r0,    L, .L45
#else
#ifdef LN
   slli.d  TEMP,   K,  BASE_SHIFT
   sub.d   AORIG, AORIG, TEMP
#endif
   slli.d  L,    KK, 0 + BASE_SHIFT
   slli.d  TEMP, KK, 2 + BASE_SHIFT
   add.d   AO, AORIG, L
   add.d   BO, B,     TEMP
   sub.d   TEMP, K, KK
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
   srai.d  L,  TEMP, 2
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
#if defined(LT) || defined(RN)
   andi    L, KK,  3
#else
   andi    L, TEMP, 3
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
#if defined(LN) || defined(RT)
#ifdef LN
   addi.d  TEMP, KK, -1
#else
   addi.d  TEMP, KK, -4
#endif
   slli.d  L,    TEMP, 0 + BASE_SHIFT
   slli.d  TEMP, TEMP, 2 + BASE_SHIFT
   add.d   AO, AORIG, L
   add.d   BO, B,     TEMP
#endif
#if defined(LN) || defined(LT)
   LD b1,  BO,   0 * SIZE
   LD b2,  BO,   1 * SIZE
   LD b3,  BO,   2 * SIZE
   LD b4,  BO,   3 * SIZE
   SUB c11, b1, c11
   SUB c21, b2, c21
   SUB c31, b3, c31
   SUB c41, b4, c41
#else
   LD b1,  AO,   0 * SIZE
   LD b2,  AO,   1 * SIZE
   LD b3,  AO,   2 * SIZE
   LD b4,  AO,   3 * SIZE
   SUB c11, b1, c11
   SUB c21, b2, c21
   SUB c31, b3, c31
   SUB c41, b4, c41
#endif
#if defined(LN) || defined(LT)
   LD b1,  AO,   0 * SIZE
   MUL c11, b1, c11
   MUL c21, b1, c21
   MUL c31, b1, c31
   MUL c41, b1, c41
#endif
#ifdef RN
   LD b1,  BO,   0 * SIZE
   LD b2,  BO,   1 * SIZE
   LD b3,  BO,   2 * SIZE
   LD b4,  BO,   3 * SIZE
   MUL c11, b1, c11
   NMSUB  c21, c11, b2, c21
   NMSUB  c31, c11, b3, c31
   NMSUB  c41, c11, b4, c41
   LD b2,  BO,   5 * SIZE
   LD b3,  BO,   6 * SIZE
   LD b4,  BO,   7 * SIZE
   MUL c21, b2, c21
   NMSUB  c31, c21, b3, c31
   NMSUB  c41, c21, b4, c41
   LD b3,  BO,  10 * SIZE
   LD b4,  BO,  11 * SIZE
   MUL c31, b3, c31
   NMSUB  c41, c31, b4, c41
   LD b4,  BO,  15 * SIZE
   MUL c41, b4, c41
#endif
#ifdef RT
   LD b5,  BO,  15 * SIZE
   LD b6,  BO,  14 * SIZE
   LD b7,  BO,  13 * SIZE
   LD b8,  BO,  12 * SIZE
   MUL c41, b5, c41
   NMSUB  c31, c41, b6, c31
   NMSUB  c21, c41, b7, c21
   NMSUB  c11, c41, b8, c11
   LD b6,  BO,  10 * SIZE
   LD b7,  BO,   9 * SIZE
   LD b8,  BO,   8 * SIZE
   MUL c31, b6, c31
   NMSUB  c21, c31, b7, c21
   NMSUB  c11, c31, b8, c11
   LD b7,  BO,   5 * SIZE
   LD b8,  BO,   4 * SIZE
   MUL c21, b7, c21
   NMSUB  c11, c21, b8, c11
   LD b8,  BO,   0 * SIZE
   MUL c11, b8, c11
#endif
#ifdef LN
   addi.d  CO1, CO1, -1 * SIZE
   addi.d  CO2, CO2, -1 * SIZE
   addi.d  CO3, CO3, -1 * SIZE
   addi.d  CO4, CO4, -1 * SIZE
#endif
#if defined(LN) || defined(LT)
   ST c11,  BO,   0 * SIZE
   ST c21,  BO,   1 * SIZE
   ST c31,  BO,   2 * SIZE
   ST c41,  BO,   3 * SIZE
#else
   ST c11,  AO,   0 * SIZE
   ST c21,  AO,   1 * SIZE
   ST c31,  AO,   2 * SIZE
   ST c41,  AO,   3 * SIZE
#endif
   ST c11,  CO1,   0 * SIZE
   ST c21,  CO2,   0 * SIZE
   ST c31,  CO3,   0 * SIZE
   ST c41,  CO4,   0 * SIZE
#ifndef LN
   addi.d  CO1, CO1, 1 * SIZE
   addi.d  CO2, CO2, 1 * SIZE
   addi.d  CO3, CO3, 1 * SIZE
   addi.d  CO4, CO4, 1 * SIZE
#endif
#ifdef RT
   slli.d  TEMP, K, BASE_SHIFT
   add.d   AORIG, AORIG, TEMP
#endif
#if defined(LT) || defined(RN)
   sub.d   TEMP, K, KK
   slli.d  L,    TEMP, 0 + BASE_SHIFT
   slli.d  TEMP, TEMP, 2 + BASE_SHIFT
   add.d   AO, AO, L
   add.d   BO, BO, TEMP
#endif
#ifdef LT
   addi.d  KK, KK, 1
#endif
#ifdef LN
   addi.d  KK, KK, -1
#endif
   .align 3

.L49:
#ifdef LN
   slli.d  TEMP, K, 2 + BASE_SHIFT
   add.d   B, B, TEMP
#endif
#if defined(LT) || defined(RN)
   move    B,  BO
#endif
#ifdef RN
   addi.d  KK, KK,  4
#endif
#ifdef RT
   addi.d  KK, KK, -4
#endif
   .align 3

.L50:
   andi    J,  N, 2
#ifdef RT
   slli.d  TEMP, K, 1 + BASE_SHIFT
#else
   move    AO, A
#endif
   bge $r0,    J, .L70
#ifdef RT
   sub.d   B, B, TEMP
   slli.d  TEMP,    LDC, 1
   sub.d   C, C, TEMP
#endif
   move    AO, A
   move    CO1, C
   add.d   CO2, C,      LDC
#ifdef LN
   add.d   KK, M, OFFSET
#endif
#ifdef LT
   move    KK, OFFSET
#endif
#if defined(LN) || defined(RT)
   move    AORIG, A
#else
   move    AO, A
#endif
#ifndef RT
   add.d   C,  CO2,    LDC
#endif
   srai.d  I,  M, 1
   bge $r0,    I, .L60
.L51:
#if defined(LT) || defined(RN)
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
   srai.d  L,  KK, 2
   LD b6,  B,   8 * SIZE
   LD b7,  B,  12 * SIZE
move   BO,  B
   bge $r0,    L, .L55
#else
#ifdef LN
   slli.d  TEMP,   K,  1 + BASE_SHIFT
   sub.d   AORIG, AORIG, TEMP
#endif
   slli.d  L,    KK, 1 + BASE_SHIFT
   slli.d  TEMP, KK, 1 + BASE_SHIFT
   add.d   AO, AORIG, L
   add.d   BO, B,     TEMP
   sub.d   TEMP, K, KK
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
   srai.d  L,  TEMP, 2
   LD b6,  BO,   8 * SIZE
   LD b7,  BO,  12 * SIZE
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
#if defined(LT) || defined(RN)
   andi    L, KK,  3
#else
   andi    L, TEMP, 3
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
#if defined(LN) || defined(RT)
#ifdef LN
   addi.d  TEMP, KK, -2
#else
   addi.d  TEMP, KK, -2
#endif
   slli.d  L,    TEMP, 1 + BASE_SHIFT
   slli.d  TEMP, TEMP, 1 + BASE_SHIFT
   add.d   AO, AORIG, L
   add.d   BO, B,     TEMP
#endif
#if defined(LN) || defined(LT)
   LD b1,  BO,   0 * SIZE
   LD b2,  BO,   1 * SIZE
   LD b3,  BO,   2 * SIZE
   LD b4,  BO,   3 * SIZE
   SUB c11, b1, c11
   SUB c21, b2, c21
   SUB c12, b3, c12
   SUB c22, b4, c22
#else
   LD b1,  AO,   0 * SIZE
   LD b2,  AO,   1 * SIZE
   LD b3,  AO,   2 * SIZE
   LD b4,  AO,   3 * SIZE
   SUB c11, b1, c11
   SUB c12, b2, c12
   SUB c21, b3, c21
   SUB c22, b4, c22
#endif
#ifdef LN
   LD b1,  AO,   3 * SIZE
   LD b2,  AO,   2 * SIZE
   LD b3,  AO,   0 * SIZE
   MUL c12, b1, c12
   MUL c22, b1, c22
   NMSUB  c11, c12, b2, c11
   NMSUB  c21, c22, b2, c21
   MUL c11, b3, c11
   MUL c21, b3, c21
#endif
#ifdef LT
   LD b1,  AO,   0 * SIZE
   LD b2,  AO,   1 * SIZE
   LD b3,  AO,   3 * SIZE
   MUL c11, b1, c11
   MUL c21, b1, c21
   NMSUB  c12, c11, b2, c12
   NMSUB  c22, c21, b2, c22
   MUL c12, b3, c12
   MUL c22, b3, c22
#endif
#ifdef RN
   LD b1,  BO,   0 * SIZE
   LD b2,  BO,   1 * SIZE
   LD b3,  BO,   3 * SIZE
   MUL c11, b1, c11
   MUL c12, b1, c12
   NMSUB  c21, c11, b2, c21
   NMSUB  c22, c12, b2, c22
   MUL c21, b3, c21
   MUL c22, b3, c22
#endif
#ifdef RT
   LD b1,  BO,   3 * SIZE
   LD b2,  BO,   2 * SIZE
   LD b3,  BO,   0 * SIZE
   MUL c21, b1, c21
   MUL c22, b1, c22
   NMSUB  c11, c21, b2, c11
   NMSUB  c12, c22, b2, c12
   MUL c11, b3, c11
   MUL c12, b3, c12
#endif
#ifdef LN
   addi.d  CO1, CO1, -2 * SIZE
   addi.d  CO2, CO2, -2 * SIZE
#endif
#if defined(LN) || defined(LT)
   ST c11,  BO,   0 * SIZE
   ST c21,  BO,   1 * SIZE
   ST c12,  BO,   2 * SIZE
   ST c22,  BO,   3 * SIZE
#else
   ST c11,  AO,   0 * SIZE
   ST c12,  AO,   1 * SIZE
   ST c21,  AO,   2 * SIZE
   ST c22,  AO,   3 * SIZE
#endif
   ST c11,  CO1,   0 * SIZE
   ST c12,  CO1,   1 * SIZE
   ST c21,  CO2,   0 * SIZE
   ST c22,  CO2,   1 * SIZE
#ifndef LN
   addi.d  CO1, CO1, 2 * SIZE
   addi.d  CO2, CO2, 2 * SIZE
#endif
#ifdef RT
   slli.d  TEMP, K, 1 + BASE_SHIFT
   add.d   AORIG, AORIG, TEMP
#endif
#if defined(LT) || defined(RN)
   sub.d   TEMP, K, KK
   slli.d  TEMP, TEMP, 1 + BASE_SHIFT
   add.d   AO, AO, TEMP
   add.d   BO, BO, TEMP
#endif
#ifdef LT
   addi.d  KK, KK, 2
#endif
#ifdef LN
   addi.d  KK, KK, -2
#endif
MTC  a1, $r0
   MOV c11, a1
   MOV c21, a1
   MOV c31, a1
   addi.d  I, I, -1
MOV    c41, c11
   blt $r0,    I, .L51
   .align 3

.L60:
   andi    I,  M, 1
   bge $r0,    I, .L69
#if defined(LT) || defined(RN)
   srai.d  L,  KK, 2
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
#else
#ifdef LN
   slli.d  TEMP,   K,  BASE_SHIFT
   sub.d   AORIG, AORIG, TEMP
#endif
   slli.d  L,    KK, 0 + BASE_SHIFT
   slli.d  TEMP, KK, 1 + BASE_SHIFT
   add.d   AO, AORIG, L
   add.d   BO, B,     TEMP
   sub.d   TEMP, K, KK
   srai.d  L,  TEMP, 2
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
#if defined(LT) || defined(RN)
   andi    L, KK,  3
#else
   andi    L, TEMP, 3
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
   ADD c11, c11, c31
   ADD c21, c21, c41
#if defined(LN) || defined(RT)
#ifdef LN
   addi.d  TEMP, KK, -1
#else
   addi.d  TEMP, KK, -2
#endif
   slli.d  L,    TEMP, 0 + BASE_SHIFT
   slli.d  TEMP, TEMP, 1 + BASE_SHIFT
   add.d   AO, AORIG, L
   add.d   BO, B,     TEMP
#endif
#if defined(LN) || defined(LT)
   LD b1,  BO,   0 * SIZE
   LD b2,  BO,   1 * SIZE
   SUB c11, b1, c11
   SUB c21, b2, c21
#else
   LD b1,  AO,   0 * SIZE
   LD b2,  AO,   1 * SIZE
   SUB c11, b1, c11
   SUB c21, b2, c21
#endif
#if defined(LN) || defined(LT)
   LD b3,  AO,   0 * SIZE
   MUL c11, b3, c11
   MUL c21, b3, c21
#endif
#ifdef RN
   LD b1,  BO,   0 * SIZE
   LD b2,  BO,   1 * SIZE
   LD b3,  BO,   3 * SIZE
   MUL c11, b1, c11
   NMSUB  c21, c11, b2, c21
   MUL c21, b3, c21
#endif
#ifdef RT
   LD b1,  BO,   3 * SIZE
   LD b2,  BO,   2 * SIZE
   LD b3,  BO,   0 * SIZE
   MUL c21, b1, c21
   NMSUB  c11, c21, b2, c11
   MUL c11, b3, c11
#endif
#ifdef LN
   addi.d  CO1, CO1, -1 * SIZE
   addi.d  CO2, CO2, -1 * SIZE
#endif
#if defined(LN) || defined(LT)
   ST c11,  BO,   0 * SIZE
   ST c21,  BO,   1 * SIZE
#else
   ST c11,  AO,   0 * SIZE
   ST c21,  AO,   1 * SIZE
#endif
   ST c11,  CO1,   0 * SIZE
   ST c21,  CO2,   0 * SIZE
#ifndef LN
   addi.d  CO1, CO1, 1 * SIZE
   addi.d  CO2, CO2, 1 * SIZE
#endif
#ifdef RT
   slli.d  TEMP, K, 0 + BASE_SHIFT
   add.d   AORIG, AORIG, TEMP
#endif
#if defined(LT) || defined(RN)
   sub.d   TEMP, K, KK
   slli.d  L,    TEMP, 0 + BASE_SHIFT
   slli.d  TEMP, TEMP, 1 + BASE_SHIFT
   add.d   AO, AO, L
   add.d   BO, BO, TEMP
#endif
#ifdef LT
   addi.d  KK, KK, 1
#endif
#ifdef LN
   addi.d  KK, KK, -1
#endif
   .align 3

.L69:
#ifdef LN
   slli.d  TEMP, K, 1 + BASE_SHIFT
   add.d   B, B, TEMP
#endif
#if defined(LT) || defined(RN)
   move    B,  BO
#endif
#ifdef RN
   addi.d  KK, KK,  2
#endif
#ifdef RT
   addi.d  KK, KK, -2
#endif
   .align 3

.L70:
   andi    J,  N, 1
   bge $r0,    J, .L999
#ifdef RT
   slli.d  TEMP, K, BASE_SHIFT
   sub.d   B, B, TEMP
   sub.d   C, C,    LDC
#endif
   move    AO, A
   move    CO1, C
#ifdef LN
   add.d   KK, M, OFFSET
#endif
#ifdef LT
   move    KK, OFFSET
#endif
#if defined(LN) || defined(RT)
   move    AORIG, A
#else
   move    AO, A
#endif
#ifndef RT
   add.d   C,  CO1,    LDC
#endif
   srai.d  I,  M, 1
   bge $r0,    I, .L80
.L71:
#if defined(LT) || defined(RN)
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
   srai.d  L,  KK, 2
   LD b6,  B,   8 * SIZE
   LD b7,  B,  12 * SIZE
move   BO,  B
   bge $r0,    L, .L75
#else
#ifdef LN
   slli.d  TEMP,   K,  1 + BASE_SHIFT
   sub.d   AORIG, AORIG, TEMP
#endif
   slli.d  L,    KK, 1 + BASE_SHIFT
   slli.d  TEMP, KK, 0 + BASE_SHIFT
   add.d   AO, AORIG, L
   add.d   BO, B,     TEMP
   sub.d   TEMP, K, KK
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
   srai.d  L,  TEMP, 2
   LD b6,  BO,   8 * SIZE
   LD b7,  BO,  12 * SIZE
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
#if defined(LT) || defined(RN)
   andi    L, KK,  3
#else
   andi    L, TEMP, 3
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
   ADD c11, c11, c21
   ADD c12, c12, c22
#if defined(LN) || defined(RT)
#ifdef LN
   addi.d  TEMP, KK, -2
#else
   addi.d  TEMP, KK, -1
#endif
   slli.d  L,    TEMP, 1 + BASE_SHIFT
   slli.d  TEMP, TEMP, 0 + BASE_SHIFT
   add.d   AO, AORIG, L
   add.d   BO, B,     TEMP
#endif
#if defined(LN) || defined(LT)
   LD b1,  BO,   0 * SIZE
   LD b2,  BO,   1 * SIZE
   SUB c11, b1, c11
   SUB c12, b2, c12
#else
   LD b1,  AO,   0 * SIZE
   LD b2,  AO,   1 * SIZE
   SUB c11, b1, c11
   SUB c12, b2, c12
#endif
#ifdef LN
   LD b1,  AO,   3 * SIZE
   LD b2,  AO,   2 * SIZE
   LD b3,  AO,   0 * SIZE
   MUL c12, b1, c12
   NMSUB  c11, c12, b2, c11
   MUL c11, b3, c11
#endif
#ifdef LT
   LD b1,  AO,   0 * SIZE
   LD b2,  AO,   1 * SIZE
   LD b3,  AO,   3 * SIZE
   MUL c11, b1, c11
   NMSUB  c12, c11, b2, c12
   MUL c12, b3, c12
#endif
#if defined(RN) || defined(RT)
   LD b1,  BO,   0 * SIZE
   MUL c11, b1, c11
   MUL c12, b1, c12
#endif
#ifdef LN
   addi.d  CO1, CO1, -2 * SIZE
#endif
#if defined(LN) || defined(LT)
   ST c11,  BO,   0 * SIZE
   ST c12,  BO,   1 * SIZE
#else
   ST c11,  AO,   0 * SIZE
   ST c12,  AO,   1 * SIZE
#endif
   ST c11,  CO1,   0 * SIZE
   ST c12,  CO1,   1 * SIZE
#ifndef LN
   addi.d  CO1, CO1, 2 * SIZE
#endif
#ifdef RT
   slli.d  TEMP, K, 1 + BASE_SHIFT
   add.d   AORIG, AORIG, TEMP
#endif
#if defined(LT) || defined(RN)
   sub.d   TEMP, K, KK
   slli.d  L,    TEMP, 1 + BASE_SHIFT
   slli.d  TEMP, TEMP, 0 + BASE_SHIFT
   add.d   AO, AO, L
   add.d   BO, BO, TEMP
#endif
#ifdef LT
   addi.d  KK, KK, 2
#endif
#ifdef LN
   addi.d  KK, KK, -2
#endif
   addi.d  I, I, -1
   blt $r0,    I, .L71
   .align 3

.L80:
   andi    I,  M, 1
   bge $r0,    I, .L89
#if defined(LT) || defined(RN)
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
   srai.d  L,  KK, 2
move   BO,  B
   bge $r0,    L, .L85
#else
#ifdef LN
   slli.d  TEMP,   K,  BASE_SHIFT
   sub.d   AORIG, AORIG, TEMP
#endif
   slli.d  TEMP, KK, BASE_SHIFT
   add.d   AO, AORIG, TEMP
   add.d   BO, B,     TEMP
   sub.d   TEMP, K, KK
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
   srai.d  L,  TEMP, 2
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
#if defined(LT) || defined(RN)
   andi    L, KK,  3
#else
   andi    L, TEMP, 3
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
   ADD c11, c11, c21
#if defined(LN) || defined(RT)
#ifdef LN
   addi.d  TEMP, KK, -1
#else
   addi.d  TEMP, KK, -1
#endif
   slli.d  TEMP, TEMP, 0 + BASE_SHIFT
   add.d   AO, AORIG, TEMP
   add.d   BO, B,     TEMP
#endif
#if defined(LN) || defined(LT)
   LD b1,  BO,   0 * SIZE
   SUB c11, b1, c11
#else
   LD b1,  AO,   0 * SIZE
   SUB c11, b1, c11
#endif
#if defined(LN) || defined(LT)
   LD b1,  AO,   0 * SIZE
   MUL c11, b1, c11
#endif
#if defined(RN) || defined(RT)
   LD b1,  BO,   0 * SIZE
   MUL c11, b1, c11
#endif
#ifdef LN
   addi.d  CO1, CO1, -1 * SIZE
#endif
#if defined(LN) || defined(LT)
   ST c11,  BO,   0 * SIZE
#else
   ST c11,  AO,   0 * SIZE
#endif
   ST c11,  CO1,   0 * SIZE
#ifndef LN
   addi.d  CO1, CO1, 1 * SIZE
#endif
#ifdef RT
   slli.d  TEMP, K, BASE_SHIFT
   add.d   AORIG, AORIG, TEMP
#endif
#if defined(LT) || defined(RN)
   sub.d   TEMP, K, KK
   slli.d  TEMP, TEMP, 0 + BASE_SHIFT
   add.d   AO, AO, TEMP
   add.d   BO, BO, TEMP
#endif
#ifdef LT
   addi.d  KK, KK, 1
#endif
#ifdef LN
   addi.d  KK, KK, -1
#endif
   .align 3

.L89:
#ifdef LN
   slli.d  TEMP, K, BASE_SHIFT
   add.d   B, B, TEMP
#endif
#if defined(LT) || defined(RN)
   move    B,  BO
#endif
#ifdef RN
   addi.d  KK, KK,  1
#endif
#ifdef RT
   addi.d  KK, KK, -1
#endif
   .align 3

.L999:
   LDARG  $r23,  $sp,    0
   LDARG  $r24,  $sp,    8
   LDARG  $r25,  $sp,   16
   LDARG  $r26,  $sp,   24
   LDARG  $r27,  $sp,   32
   LDARG  $r28,  $sp,   40
   fld.d  $f24,  $sp,  48
   fld.d  $f25,  $sp,  56
   fld.d  $f26,  $sp,  64
   fld.d  $f27,  $sp,  72
   fld.d  $f28,  $sp,  80
   LDARG  $r29,  $sp,   88
   LDARG  $r30,  $sp,   96
   LDARG  $r20,  $sp,  104
   LDARG  $r16,  $sp,  112
#ifndef __64BIT__
   fld.d  $f18,  $sp, 112
   fld.d  $f19,  $sp, 120
   fld.d  $f20,  $sp, 128
   fld.d  $f21,  $sp, 136
#endif
   addi.d  $sp, $sp, 144
   move $r4, $r17
   fmov.d $f0, $f22
   jirl    $r0, $r1, 0x0

   EPILOGUE
