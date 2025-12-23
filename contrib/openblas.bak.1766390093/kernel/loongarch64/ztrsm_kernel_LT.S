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
#define L      $r25
#define CO1    $r14
#define CO2    $r15
#define CO3    $r23
#define CO4    $r24
#define KK     $r26
#define TEMP   $r27
#define AORIG  $r28
#define a1     $f22
#define a2     $f8
#define a3     $f26
#define a4     $f27
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
#define c21    $f0
#define c22    $f1
#define c31    $f2
#define c32    $f3
#define c41    $f4
#define c42    $f5
#define c51    $f6
#define c52    $f7
#define c61    $f18
#define c62    $f19
#define c71    $f20
#define c72    $f21
#define c81    $f24
#define c82    $f25

#ifndef CONJ
#define    MADD1       MADD
#define    MADD2       MADD
#define    MADD3       MADD
#define    MADD4       NMSUB
#define    MADD5       MSUB
#define    MADD6       MADD
#define    MADD7       NMSUB
#define    MADD8       MADD
#else
#if defined(LN) || defined(LT)
#define    MADD1       MADD
#define    MADD2       NMSUB
#define    MADD3       MADD
#define    MADD4       MADD
#else
#define    MADD1       MADD
#define    MADD2       MADD
#define    MADD3       NMSUB
#define    MADD4       MADD
#endif
#define    MADD5       MADD
#define    MADD6       MSUB
#define    MADD7       MADD
#define    MADD8       NMSUB
#endif

   PROLOGUE

   addi.d  $sp, $sp, -128
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
#ifndef __64BIT__
   fst.d  $f18,  $sp,  88
   fst.d  $f19,  $sp,  96
   fst.d  $f20,  $sp, 104
   fst.d  $f21,  $sp, 112
#endif
   slli.d     LDC,    LDC, ZBASE_SHIFT
#ifdef LN
        mul.w   TEMP, M, K
   slli.d  TEMP, TEMP, ZBASE_SHIFT
   add.d   A, A, TEMP
   slli.d  TEMP, M, ZBASE_SHIFT
   add.d   C, C, TEMP
#endif
#ifdef RN
        sub.d   KK, $r0, OFFSET
#endif
#ifdef RT
        mul.w   TEMP, N, K
   slli.d  TEMP, TEMP, ZBASE_SHIFT
   add.d   B, B, TEMP
        mul.w   TEMP, N,    LDC
   add.d   C, C, TEMP
   sub.d   KK, N, OFFSET
#endif
   srai.d  J,  N, 2
nop
   bge $r0,    J, .L20
.L10:
#ifdef RT
   slli.d  TEMP, K, 2 + ZBASE_SHIFT
   sub.d   B, B, TEMP
   slli.d  TEMP,    LDC, 2
   sub.d   C, C, TEMP
#endif
   move    CO1, C
MTC  c11, $r0
   add.d   CO2, C,      LDC
   add.d   CO3, CO2,    LDC
   addi.d  J, J, -1
   add.d   CO4, CO3,    LDC
   MOV c21, c11
   MOV c31, c11
   MOV c41, c11
   MOV c51, c11
   move    I,  M
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
MOV    c61, c11
   bge $r0,    I, .L19
   .align 3

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
   slli.d  TEMP,   K,  ZBASE_SHIFT
   sub.d   AORIG, AORIG, TEMP
#endif
   slli.d  L,    KK, ZBASE_SHIFT
   slli.d  TEMP, KK, 2 + ZBASE_SHIFT
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
#if defined(LT) || defined(RN)
   andi    L, KK,  3
#else
   andi    L, TEMP, 3
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
   ADD c11, c11, c22
   ADD c12, c12, c21
   ADD c31, c31, c42
   ADD c32, c32, c41
   ADD c51, c51, c62
   ADD c52, c52, c61
   ADD c71, c71, c82
   ADD c72, c72, c81
#if defined(LN) || defined(RT)
#ifdef LN
   addi.d  TEMP, KK, -1
#else
   addi.d  TEMP, KK, -4
#endif
   slli.d  L,    TEMP, ZBASE_SHIFT
   slli.d  TEMP, TEMP, 2 + ZBASE_SHIFT
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
   SUB c12, b2, c12
   SUB c31, b3, c31
   SUB c32, b4, c32
   SUB c51, b5, c51
   SUB c52, b6, c52
   SUB c71, b7, c71
   SUB c72, b8, c72
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
   SUB c31, b3, c31
   SUB c32, b4, c32
   SUB c51, b5, c51
   SUB c52, b6, c52
   SUB c71, b7, c71
   SUB c72, b8, c72
#endif
#if defined(LN) || defined(LT)
   LD b1,  AO,   0 * SIZE
   LD b2,  AO,   1 * SIZE
   MUL a1, b2, c12
   MUL a2, b2, c11
   MUL a3, b2, c32
   MUL a4, b2, c31
   MADD5  c11, c11, b1, a1
   MADD6  c12, c12, b1, a2
   MADD5  c31, c31, b1, a3
   MADD6  c32, c32, b1, a4
   MUL a1, b2, c52
   MUL a2, b2, c51
   MUL a3, b2, c72
   MUL a4, b2, c71
   MADD5  c51, c51, b1, a1
   MADD6  c52, c52, b1, a2
   MADD5  c71, c71, b1, a3
   MADD6  c72, c72, b1, a4
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
   MUL a1, b2, c12
   MUL a2, b2, c11
   MADD5  c11, c11, b1, a1
   MADD6  c12, c12, b1, a2
   NMSUB  c31, c11, b3, c31
   MADD7  c32, c11, b4, c32
   NMSUB  c51, c11, b5, c51
   MADD7  c52, c11, b6, c52
   NMSUB  c71, c11, b7, c71
   MADD7  c72, c11, b8, c72
   MADD8  c31, c12, b4, c31
   NMSUB  c32, c12, b3, c32
   MADD8  c51, c12, b6, c51
   NMSUB  c52, c12, b5, c52
   MADD8  c71, c12, b8, c71
   NMSUB  c72, c12, b7, c72
   LD b3,  BO,  10 * SIZE
   LD b4,  BO,  11 * SIZE
   LD b5,  BO,  12 * SIZE
   LD b6,  BO,  13 * SIZE
   LD b7,  BO,  14 * SIZE
   LD b8,  BO,  15 * SIZE
   MUL a1, b4, c32
   MUL a2, b4, c31
   MADD5  c31, c31, b3, a1
   MADD6  c32, c32, b3, a2
   NMSUB  c51, c31, b5, c51
   MADD7  c52, c31, b6, c52
   NMSUB  c71, c31, b7, c71
   MADD7  c72, c31, b8, c72
   MADD8  c51, c32, b6, c51
   NMSUB  c52, c32, b5, c52
   MADD8  c71, c32, b8, c71
   NMSUB  c72, c32, b7, c72
   LD b5,  BO,  20 * SIZE
   LD b6,  BO,  21 * SIZE
   LD b7,  BO,  22 * SIZE
   LD b8,  BO,  23 * SIZE
   MUL a1, b6, c52
   MUL a2, b6, c51
   MADD5  c51, c51, b5, a1
   MADD6  c52, c52, b5, a2
   NMSUB  c71, c51, b7, c71
   MADD7  c72, c51, b8, c72
   MADD8  c71, c52, b8, c71
   NMSUB  c72, c52, b7, c72
   LD b7,  BO,  30 * SIZE
   LD b8,  BO,  31 * SIZE
   MUL a1, b8, c72
   MUL a2, b8, c71
   MADD5  c71, c71, b7, a1
   MADD6  c72, c72, b7, a2
#endif
#ifdef RT
   LD b1,  BO,  30 * SIZE
   LD b2,  BO,  31 * SIZE
   LD b3,  BO,  28 * SIZE
   LD b4,  BO,  29 * SIZE
   LD b5,  BO,  26 * SIZE
   LD b6,  BO,  27 * SIZE
   LD b7,  BO,  24 * SIZE
   LD b8,  BO,  25 * SIZE
   MUL a1, b2, c72
   MUL a2, b2, c71
   MADD5  c71, c71, b1, a1
   MADD6  c72, c72, b1, a2
   NMSUB  c51, c71, b3, c51
   MADD7  c52, c71, b4, c52
   NMSUB  c31, c71, b5, c31
   MADD7  c32, c71, b6, c32
   NMSUB  c11, c71, b7, c11
   MADD7  c12, c71, b8, c12
   MADD8  c51, c72, b4, c51
   NMSUB  c52, c72, b3, c52
   MADD8  c31, c72, b6, c31
   NMSUB  c32, c72, b5, c32
   MADD8  c11, c72, b8, c11
   NMSUB  c12, c72, b7, c12
   LD b3,  BO,  20 * SIZE
   LD b4,  BO,  21 * SIZE
   LD b5,  BO,  18 * SIZE
   LD b6,  BO,  19 * SIZE
   LD b7,  BO,  16 * SIZE
   LD b8,  BO,  17 * SIZE
   MUL a1, b4, c52
   MUL a2, b4, c51
   MADD5  c51, c51, b3, a1
   MADD6  c52, c52, b3, a2
   NMSUB  c31, c51, b5, c31
   MADD7  c32, c51, b6, c32
   NMSUB  c11, c51, b7, c11
   MADD7  c12, c51, b8, c12
   MADD8  c31, c52, b6, c31
   NMSUB  c32, c52, b5, c32
   MADD8  c11, c52, b8, c11
   NMSUB  c12, c52, b7, c12
   LD b5,  BO,  10 * SIZE
   LD b6,  BO,  11 * SIZE
   LD b7,  BO,   8 * SIZE
   LD b8,  BO,   9 * SIZE
   MUL a1, b6, c32
   MUL a2, b6, c31
   MADD5  c31, c31, b5, a1
   MADD6  c32, c32, b5, a2
   NMSUB  c11, c31, b7, c11
   MADD7  c12, c31, b8, c12
   MADD8  c11, c32, b8, c11
   NMSUB  c12, c32, b7, c12
   LD b7,  BO,   0 * SIZE
   LD b8,  BO,   1 * SIZE
   MUL a1, b8, c12
   MUL a2, b8, c11
   MADD5  c11, c11, b7, a1
   MADD6  c12, c12, b7, a2
#endif
#if defined(LN) || defined(LT)
   ST c11,  BO,   0 * SIZE
   ST c12,  BO,   1 * SIZE
   ST c31,  BO,   2 * SIZE
   ST c32,  BO,   3 * SIZE
   ST c51,  BO,   4 * SIZE
   ST c52,  BO,   5 * SIZE
   ST c71,  BO,   6 * SIZE
   ST c72,  BO,   7 * SIZE
#else
   ST c11,  AO,   0 * SIZE
   ST c12,  AO,   1 * SIZE
   ST c31,  AO,   2 * SIZE
   ST c32,  AO,   3 * SIZE
   ST c51,  AO,   4 * SIZE
   ST c52,  AO,   5 * SIZE
   ST c71,  AO,   6 * SIZE
   ST c72,  AO,   7 * SIZE
#endif
#ifdef LN
   addi.d  CO1,CO1, -2 * SIZE
   addi.d  CO2,CO2, -2 * SIZE
   addi.d  CO3,CO3, -2 * SIZE
   addi.d  CO4,CO4, -2 * SIZE
#endif
   ST c11,  CO1,   0 * SIZE
   ST c12,  CO1,   1 * SIZE
   ST c31,  CO2,   0 * SIZE
   ST c32,  CO2,   1 * SIZE
   ST c51,  CO3,   0 * SIZE
   ST c52,  CO3,   1 * SIZE
   ST c71,  CO4,   0 * SIZE
   ST c72,  CO4,   1 * SIZE
#ifndef LN
   addi.d  CO1,CO1, 2 * SIZE
   addi.d  CO2,CO2, 2 * SIZE
   addi.d  CO3,CO3, 2 * SIZE
   addi.d  CO4,CO4, 2 * SIZE
#endif
#ifdef RT
   slli.d  TEMP, K, ZBASE_SHIFT
   add.d   AORIG, AORIG, TEMP
#endif
#if defined(LT) || defined(RN)
   sub.d   TEMP, K, KK
   slli.d  L,    TEMP, ZBASE_SHIFT
   slli.d  TEMP, TEMP, 2 + ZBASE_SHIFT
   add.d   AO, AO, L
   add.d   BO, BO, TEMP
#endif
#ifdef LT
   addi.d  KK, KK, 1
#endif
#ifdef LN
   addi.d  KK, KK, -1
#endif
MTC  c11, $r0
   addi.d  I, I, -1
   MOV c21, c11
   MOV c31, c11
   MOV c41, c11
   MOV c51, c11
MOV    c61, c11
   blt $r0,    I, .L11
   .align 3

.L19:
#ifdef LN
   slli.d  TEMP, K, 2 + ZBASE_SHIFT
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
   blt $r0,    J, .L10
   .align 3

.L20:
   andi    J,  N, 2
   bge $r0,    J, .L30
#ifdef RT
   slli.d  TEMP, K, 1 + ZBASE_SHIFT
   sub.d   B, B, TEMP
   slli.d  TEMP,    LDC, 1
   sub.d   C, C, TEMP
#endif
MTC  c11, $r0
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
   move    I,  M
   bge $r0,    I, .L29
   .align 3

.L21:
#if defined(LT) || defined(RN)
   LD a1,  AO,   0 * SIZE
   MOV c21, c11
   LD b1,  B,   0 * SIZE
   MOV c31, c11
   LD a3,  AO,   4 * SIZE
   MOV c41, c11
   LD b2,  B,   1 * SIZE
   srai.d  L,  KK, 2
   LD b3,  B,   2 * SIZE
   MOV c12, c11
   LD b4,  B,   3 * SIZE
   MOV c22, c11
   LD b5,  B,   4 * SIZE
   MOV c32, c11
   MOV c42, c11
move   BO,  B
   bge $r0,    L, .L25
#else
#ifdef LN
   slli.d  TEMP,   K,  ZBASE_SHIFT
   sub.d   AORIG, AORIG, TEMP
#endif
   slli.d  L,    KK, ZBASE_SHIFT
   slli.d  TEMP, KK, 1 + ZBASE_SHIFT
   add.d   AO, AORIG, L
   add.d   BO, B,     TEMP
   sub.d   TEMP, K, KK
   LD a1,  AO,   0 * SIZE
   MOV c21, c11
   LD b1,  BO,   0 * SIZE
   MOV c31, c11
   LD a3,  AO,   4 * SIZE
   MOV c41, c11
   LD b2,  BO,   1 * SIZE
   srai.d  L,  TEMP, 2
   LD b3,  BO,   2 * SIZE
   MOV c12, c11
   LD b4,  BO,   3 * SIZE
   MOV c22, c11
   LD b5,  BO,   4 * SIZE
   MOV c32, c11
MOV    c42, c11
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
#if defined(LT) || defined(RN)
   andi    L, KK,  3
#else
   andi    L, TEMP, 3
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
   ADD c11, c11, c22
   ADD c12, c12, c21
   ADD c31, c31, c42
   ADD c32, c32, c41
#if defined(LN) || defined(RT)
#ifdef LN
   addi.d  TEMP, KK, -1
#else
   addi.d  TEMP, KK, -2
#endif
   slli.d  L,    TEMP, ZBASE_SHIFT
   slli.d  TEMP, TEMP, 1 + ZBASE_SHIFT
   add.d   AO, AORIG, L
   add.d   BO, B,     TEMP
#endif
#if defined(LN) || defined(LT)
   LD b1,  BO,   0 * SIZE
   LD b2,  BO,   1 * SIZE
   LD b3,  BO,   2 * SIZE
   LD b4,  BO,   3 * SIZE
   SUB c11, b1, c11
   SUB c12, b2, c12
   SUB c31, b3, c31
   SUB c32, b4, c32
#else
   LD b1,  AO,   0 * SIZE
   LD b2,  AO,   1 * SIZE
   LD b3,  AO,   2 * SIZE
   LD b4,  AO,   3 * SIZE
   SUB c11, b1, c11
   SUB c12, b2, c12
   SUB c31, b3, c31
   SUB c32, b4, c32
#endif
#if defined(LN) || defined(LT)
   LD b1,  AO,   0 * SIZE
   LD b2,  AO,   1 * SIZE
   MUL a1, b2, c12
   MUL a2, b2, c11
   MUL a3, b2, c32
   MUL a4, b2, c31
   MADD5  c11, c11, b1, a1
   MADD6  c12, c12, b1, a2
   MADD5  c31, c31, b1, a3
   MADD6  c32, c32, b1, a4
#endif
#ifdef RN
   LD b1,  BO,   0 * SIZE
   LD b2,  BO,   1 * SIZE
   LD b3,  BO,   2 * SIZE
   LD b4,  BO,   3 * SIZE
   MUL a1, b2, c12
   MUL a2, b2, c11
   MADD5  c11, c11, b1, a1
   MADD6  c12, c12, b1, a2
   NMSUB  c31, c11, b3, c31
   MADD7  c32, c11, b4, c32
   MADD8  c31, c12, b4, c31
   NMSUB  c32, c12, b3, c32
   LD b3,  BO,   6 * SIZE
   LD b4,  BO,   7 * SIZE
   MUL a1, b4, c32
   MUL a2, b4, c31
   MADD5  c31, c31, b3, a1
   MADD6  c32, c32, b3, a2
#endif
#ifdef RT
   LD b5,  BO,   6 * SIZE
   LD b6,  BO,   7 * SIZE
   LD b7,  BO,   4 * SIZE
   LD b8,  BO,   5 * SIZE
   MUL a1, b6, c32
   MUL a2, b6, c31
   MADD5  c31, c31, b5, a1
   MADD6  c32, c32, b5, a2
   NMSUB  c11, c31, b7, c11
   MADD7  c12, c31, b8, c12
   MADD8  c11, c32, b8, c11
   NMSUB  c12, c32, b7, c12
   LD b7,  BO,   0 * SIZE
   LD b8,  BO,   1 * SIZE
   MUL a1, b8, c12
   MUL a2, b8, c11
   MADD5  c11, c11, b7, a1
   MADD6  c12, c12, b7, a2
#endif
#if defined(LN) || defined(LT)
   ST c11,  BO,   0 * SIZE
   ST c12,  BO,   1 * SIZE
   ST c31,  BO,   2 * SIZE
   ST c32,  BO,   3 * SIZE
#else
   ST c11,  AO,   0 * SIZE
   ST c12,  AO,   1 * SIZE
   ST c31,  AO,   2 * SIZE
   ST c32,  AO,   3 * SIZE
#endif
#ifdef LN
   addi.d  CO1,CO1, -2 * SIZE
   addi.d  CO2,CO2, -2 * SIZE
#endif
   ST c11,  CO1,   0 * SIZE
   ST c12,  CO1,   1 * SIZE
   ST c31,  CO2,   0 * SIZE
   ST c32,  CO2,   1 * SIZE
#ifndef LN
   addi.d  CO1,CO1, 2 * SIZE
   addi.d  CO2,CO2, 2 * SIZE
#endif
MTC  c11, $r0
#ifdef RT
   slli.d  TEMP, K, ZBASE_SHIFT
   add.d   AORIG, AORIG, TEMP
#endif
#if defined(LT) || defined(RN)
   sub.d   TEMP, K, KK
   slli.d  L,    TEMP, ZBASE_SHIFT
   slli.d  TEMP, TEMP, 1 + ZBASE_SHIFT
   add.d   AO, AO, L
   add.d   BO, BO, TEMP
#endif
#ifdef LT
   addi.d  KK, KK, 1
#endif
#ifdef LN
   addi.d  KK, KK, -1
#endif
   addi.d  I, I, -1
   blt $r0,    I, .L21
   .align 3

.L29:
#ifdef LN
   slli.d  TEMP, K, 1 + ZBASE_SHIFT
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

.L30:
   andi    J,  N, 1
   bge $r0,    J, .L999
#ifdef RT
   slli.d  TEMP, K, ZBASE_SHIFT
   sub.d   B, B, TEMP
   sub.d   C, C,    LDC
#endif
MTC  c11, $r0
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
   move    I,  M
   bge $r0,    I, .L39
   .align 3

.L31:
#if defined(LT) || defined(RN)
   LD a1,  AO,   0 * SIZE
   MOV c21, c11
   LD b1,  B,   0 * SIZE
   MOV c31, c11
   LD a2,  AO,   1 * SIZE
   MOV c41, c11
   LD b2,  B,   1 * SIZE
   MOV c12, c11
   srai.d  L,  KK, 2
   MOV c22, c11
   LD a3,  AO,   4 * SIZE
   MOV c32, c11
   LD b3,  B,   4 * SIZE
   MOV c42, c11
move   BO,  B
   bge $r0,    L, .L35
#else
#ifdef LN
   slli.d  TEMP,   K,  ZBASE_SHIFT
   sub.d   AORIG, AORIG, TEMP
#endif
   slli.d  TEMP, KK, ZBASE_SHIFT
   add.d   AO, AORIG, TEMP
   add.d   BO, B,     TEMP
   sub.d   TEMP, K, KK
   LD a1,  AO,   0 * SIZE
   MOV c21, c11
   LD b1,  BO,   0 * SIZE
   MOV c31, c11
   LD a2,  AO,   1 * SIZE
   MOV c41, c11
   LD b2,  BO,   1 * SIZE
   MOV c12, c11
   srai.d  L, TEMP, 2
   MOV c22, c11
   LD a3,  AO,   4 * SIZE
   MOV c32, c11
   LD b3,  BO,   4 * SIZE
MOV    c42, c11
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
#if defined(LT) || defined(RN)
   andi    L, KK,  3
#else
   andi    L, TEMP, 3
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
   ADD c11, c11, c22
   ADD c12, c12, c21
#if defined(LN) || defined(RT)
   addi.d  TEMP, KK, -1
   slli.d  TEMP, TEMP, ZBASE_SHIFT
   add.d   AO, AORIG, TEMP
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
#if defined(LN) || defined(LT)
   LD b1,  AO,   0 * SIZE
   LD b2,  AO,   1 * SIZE
   MUL a1, b2, c12
   MUL a2, b2, c11
   MADD5  c11, c11, b1, a1
   MADD6  c12, c12, b1, a2
#endif
#if defined(RN) || defined(RT)
   LD b1,  BO,   0 * SIZE
   LD b2,  BO,   1 * SIZE
   MUL a1, b2, c12
   MUL a2, b2, c11
   MADD5  c11, c11, b1, a1
   MADD6  c12, c12, b1, a2
#endif
#if defined(LN) || defined(LT)
   ST c11,  BO,   0 * SIZE
   ST c12,  BO,   1 * SIZE
#else
   ST c11,  AO,   0 * SIZE
   ST c12,  AO,   1 * SIZE
#endif
#ifdef LN
   addi.d  CO1,CO1, -2 * SIZE
#endif
   ST c11,  CO1,   0 * SIZE
   ST c12,  CO1,   1 * SIZE
#ifndef LN
   addi.d  CO1,CO1, 2 * SIZE
#endif
MTC  c11, $r0
#ifdef RT
   slli.d  TEMP, K, ZBASE_SHIFT
   add.d   AORIG, AORIG, TEMP
#endif
#if defined(LT) || defined(RN)
   sub.d   TEMP, K, KK
   slli.d  TEMP, TEMP, ZBASE_SHIFT
   add.d   AO, AO, TEMP
   add.d   BO, BO, TEMP
#endif
#ifdef LT
   addi.d  KK, KK, 1
#endif
#ifdef LN
   addi.d  KK, KK, -1
#endif
   addi.d  I, I, -1
   blt $r0,    I, .L31
   .align 3

.L39:
#ifdef LN
   slli.d  TEMP, K, ZBASE_SHIFT
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
#ifndef __64BIT__
   fld.d  $f18,  $sp,  88
   fld.d  $f19,  $sp,  96
   fld.d  $f20,  $sp, 104
   fld.d  $f21,  $sp, 112
#endif
   addi.d  $sp, $sp, 128
   move $r4, $r17
   fmov.d $f0, $f22
   jirl    $r0, $r1, 0x0

   EPILOGUE
