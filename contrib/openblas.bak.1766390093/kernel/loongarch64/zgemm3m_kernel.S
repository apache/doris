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
#define L      $r11
#define CO1    $r14
#define CO2    $r15
#define CO3    $r23
#define CO4    $r24
#define CO5    $r25
#define CO6    $r26
#define CO7    $r27
#define CO8    $r28

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
   fst.d  $f28,  $sp,  80
   fst.d  $f29,  $sp,  88
   slli.d     LDC,    LDC, ZBASE_SHIFT
   srai.d  J,  N, 3
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
MOV    c61, c11
   bge $r0,    I, .L20
.L11:
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
   andi    L,  K, 3
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
   LD $f22,  CO1,  0 * SIZE
   LD $f8,  CO1,  1 * SIZE
   LD $f23,  CO1,  2 * SIZE
   LD $f9,  CO1,  3 * SIZE
   LD $f10,  CO2,  0 * SIZE
   MADD  $f22, c11, ALPHA_R, $f22
   LD $f11,  CO2,  1 * SIZE
   MADD  $f8, c11, ALPHA_I, $f8
   LD $f12,  CO2,  2 * SIZE
   MADD  $f23, c12, ALPHA_R, $f23
   LD $f13,  CO2,  3 * SIZE
   MADD  $f9, c12, ALPHA_I, $f9
   MADD  $f10, c21, ALPHA_R, $f10
   ST $f22,  CO1,   0 * SIZE
   MADD  $f11, c21, ALPHA_I, $f11
   ST $f8,  CO1,   1 * SIZE
   MADD  $f12, c22, ALPHA_R, $f12
   ST $f23,  CO1,   2 * SIZE
   MADD  $f13, c22, ALPHA_I, $f13
   ST $f9,  CO1,   3 * SIZE
   LD $f22,  CO3,  0 * SIZE
   LD $f8,  CO3,  1 * SIZE
   LD $f23,  CO3,  2 * SIZE
   LD $f9,  CO3,  3 * SIZE
   ST $f10,  CO2,   0 * SIZE
   ST $f11,  CO2,   1 * SIZE
   ST $f12,  CO2,   2 * SIZE
   ST $f13,  CO2,   3 * SIZE
   LD $f10,  CO4,  0 * SIZE
   LD $f11,  CO4,  1 * SIZE
   LD $f12,  CO4,  2 * SIZE
   LD $f13,  CO4,  3 * SIZE
   MADD  $f22, c31, ALPHA_R, $f22
   MADD  $f8, c31, ALPHA_I, $f8
   MADD  $f23, c32, ALPHA_R, $f23
   MADD  $f9, c32, ALPHA_I, $f9
   MADD  $f10, c41, ALPHA_R, $f10
   ST $f22,  CO3,   0 * SIZE
   MADD  $f11, c41, ALPHA_I, $f11
   ST $f8,  CO3,   1 * SIZE
   MADD  $f12, c42, ALPHA_R, $f12
   ST $f23,  CO3,   2 * SIZE
   MADD  $f13, c42, ALPHA_I, $f13
   ST $f9,  CO3,   3 * SIZE
   LD $f22,  CO5,  0 * SIZE
   LD $f8,  CO5,  1 * SIZE
   LD $f23,  CO5,  2 * SIZE
   LD $f9,  CO5,  3 * SIZE
   ST $f10,  CO4,   0 * SIZE
   ST $f11,  CO4,   1 * SIZE
   ST $f12,  CO4,   2 * SIZE
   ST $f13,  CO4,   3 * SIZE
   LD $f10,  CO6,  0 * SIZE
   LD $f11,  CO6,  1 * SIZE
   LD $f12,  CO6,  2 * SIZE
   LD $f13,  CO6,  3 * SIZE
   MADD  $f22, c51, ALPHA_R, $f22
   addi.d  CO1,CO1, 4 * SIZE
   MADD  $f8, c51, ALPHA_I, $f8
   addi.d  CO2,CO2, 4 * SIZE
   MADD  $f23, c52, ALPHA_R, $f23
   addi.d  CO3,CO3, 4 * SIZE
   MADD  $f9, c52, ALPHA_I, $f9
   addi.d  CO4,CO4, 4 * SIZE
   MADD  $f10, c61, ALPHA_R, $f10
   ST $f22,  CO5,   0 * SIZE
   MADD  $f11, c61, ALPHA_I, $f11
   ST $f8,  CO5,   1 * SIZE
   MADD  $f12, c62, ALPHA_R, $f12
   ST $f23,  CO5,   2 * SIZE
   MADD  $f13, c62, ALPHA_I, $f13
   ST $f9,  CO5,   3 * SIZE
   LD $f22,  CO7,  0 * SIZE
   LD $f8,  CO7,  1 * SIZE
   LD $f23,  CO7,  2 * SIZE
   LD $f9,  CO7,  3 * SIZE
   ST $f10,  CO6,   0 * SIZE
   ST $f11,  CO6,   1 * SIZE
   ST $f12,  CO6,   2 * SIZE
   ST $f13,  CO6,   3 * SIZE
   LD $f10,  CO8,  0 * SIZE
   addi.d  I, I, -1
   LD $f11,  CO8,  1 * SIZE
MTC  c11, $r0
   LD $f12,  CO8,  2 * SIZE
   LD $f13,  CO8,  3 * SIZE
   MADD  $f22, c71, ALPHA_R, $f22
   addi.d  CO5,CO5, 4 * SIZE
   MADD  $f8, c71, ALPHA_I, $f8
   addi.d  CO6,CO6, 4 * SIZE
   MADD  $f23, c72, ALPHA_R, $f23
   addi.d  CO7,CO7, 4 * SIZE
   MADD  $f9, c72, ALPHA_I, $f9
   addi.d  CO8,CO8, 4 * SIZE
   MADD  $f10, c81, ALPHA_R, $f10
   ST $f22,  CO7,  -4 * SIZE
   MADD  $f11, c81, ALPHA_I, $f11
   ST $f8,  CO7,  -3 * SIZE
   MADD  $f12, c82, ALPHA_R, $f12
   ST $f23,  CO7,  -2 * SIZE
   MADD  $f13, c82, ALPHA_I, $f13
   ST $f9,  CO7,  -1 * SIZE
   ST $f10,  CO8,  -4 * SIZE
   MOV c21, c11
   ST $f11,  CO8,  -3 * SIZE
   MOV c31, c11
   ST $f12,  CO8,  -2 * SIZE
   MOV c41, c11
   ST $f13,  CO8,  -1 * SIZE
   MOV c51, c11
MOV    c61, c11
   blt $r0,    I, .L11
   .align 3

.L20:
   andi    I,  M, 1
   MOV c61, c11
MOV    c71, c11
   bge $r0,    I, .L29
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
   andi    L,  K, 3
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
   LD $f22,  CO1,  0 * SIZE
   LD $f8,  CO1,  1 * SIZE
   LD $f23,  CO2,  0 * SIZE
   LD $f9,  CO2,  1 * SIZE
   LD $f10,  CO3,  0 * SIZE
   MADD  $f22, c11, ALPHA_R, $f22
   LD $f11,  CO3,  1 * SIZE
   MADD  $f8, c11, ALPHA_I, $f8
   LD $f12,  CO4,  0 * SIZE
   MADD  $f23, c21, ALPHA_R, $f23
   LD $f13,  CO4,  1 * SIZE
   MADD  $f9, c21, ALPHA_I, $f9
   MADD  $f10, c31, ALPHA_R, $f10
   ST $f22,  CO1,   0 * SIZE
   MADD  $f11, c31, ALPHA_I, $f11
   ST $f8,  CO1,   1 * SIZE
   MADD  $f12, c41, ALPHA_R, $f12
   ST $f23,  CO2,   0 * SIZE
   MADD  $f13, c41, ALPHA_I, $f13
   ST $f9,  CO2,   1 * SIZE
   LD $f22,  CO5,  0 * SIZE
   LD $f8,  CO5,  1 * SIZE
   LD $f23,  CO6,  0 * SIZE
   LD $f9,  CO6,  1 * SIZE
   ST $f10,  CO3,   0 * SIZE
   ST $f11,  CO3,   1 * SIZE
   ST $f12,  CO4,   0 * SIZE
   ST $f13,  CO4,   1 * SIZE
   LD $f10,  CO7,  0 * SIZE
   MADD  $f22, c51, ALPHA_R, $f22
   LD $f11,  CO7,  1 * SIZE
   MADD  $f8, c51, ALPHA_I, $f8
   LD $f12,  CO8,  0 * SIZE
   MADD  $f23, c61, ALPHA_R, $f23
   LD $f13,  CO8,  1 * SIZE
   MADD  $f9, c61, ALPHA_I, $f9
   MADD  $f10, c71, ALPHA_R, $f10
   ST $f22,  CO5,   0 * SIZE
   MADD  $f11, c71, ALPHA_I, $f11
   ST $f8,  CO5,   1 * SIZE
   MADD  $f12, c81, ALPHA_R, $f12
   ST $f23,  CO6,   0 * SIZE
   MADD  $f13, c81, ALPHA_I, $f13
   ST $f9,  CO6,   1 * SIZE
   ST $f10,  CO7,   0 * SIZE
   ST $f11,  CO7,   1 * SIZE
   ST $f12,  CO8,   0 * SIZE
   ST $f13,  CO8,   1 * SIZE
   .align 3

.L29:
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
   srai.d  I,  M, 1
MOV    c41, c11
   bge $r0,    I, .L40
.L31:
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
   andi    L,  K, 3
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
   LD $f22,  CO1,  0 * SIZE
   LD $f8,  CO1,  1 * SIZE
   LD $f23,  CO1,  2 * SIZE
   LD $f9,  CO1,  3 * SIZE
   LD $f10,  CO2,  0 * SIZE
   LD $f11,  CO2,  1 * SIZE
   LD $f12,  CO2,  2 * SIZE
   LD $f13,  CO2,  3 * SIZE
   MADD  $f22, c11, ALPHA_R, $f22
   MADD  $f8, c11, ALPHA_I, $f8
   MADD  $f23, c12, ALPHA_R, $f23
   MADD  $f9, c12, ALPHA_I, $f9
   MADD  $f10, c21, ALPHA_R, $f10
   ST $f22,  CO1,   0 * SIZE
   MADD  $f11, c21, ALPHA_I, $f11
   ST $f8,  CO1,   1 * SIZE
   MADD  $f12, c22, ALPHA_R, $f12
   ST $f23,  CO1,   2 * SIZE
   MADD  $f13, c22, ALPHA_I, $f13
   ST $f9,  CO1,   3 * SIZE
   LD $f22,  CO3,  0 * SIZE
   LD $f8,  CO3,  1 * SIZE
   LD $f23,  CO3,  2 * SIZE
   LD $f9,  CO3,  3 * SIZE
   ST $f10,  CO2,   0 * SIZE
   MADD  $f22, c31, ALPHA_R, $f22
   ST $f11,  CO2,   1 * SIZE
   MADD  $f8, c31, ALPHA_I, $f8
   ST $f12,  CO2,   2 * SIZE
   MADD  $f23, c32, ALPHA_R, $f23
   ST $f13,  CO2,   3 * SIZE
   MADD  $f9, c32, ALPHA_I, $f9
   LD $f10,  CO4,  0 * SIZE
   LD $f11,  CO4,  1 * SIZE
   LD $f12,  CO4,  2 * SIZE
   LD $f13,  CO4,  3 * SIZE
   MADD  $f10, c41, ALPHA_R, $f10
   addi.d  CO1,CO1, 4 * SIZE
   MADD  $f11, c41, ALPHA_I, $f11
   addi.d  CO2,CO2, 4 * SIZE
   MADD  $f12, c42, ALPHA_R, $f12
   addi.d  CO3,CO3, 4 * SIZE
   MADD  $f13, c42, ALPHA_I, $f13
   addi.d  CO4,CO4, 4 * SIZE
   ST $f22,  CO3,  -4 * SIZE
   addi.d  I, I, -1
   ST $f8,  CO3,  -3 * SIZE
   ST $f23,  CO3,  -2 * SIZE
   ST $f9,  CO3,  -1 * SIZE
   ST $f10,  CO4,  -4 * SIZE
MTC  c11, $r0
   ST $f11,  CO4,  -3 * SIZE
   MOV c21, c11
   ST $f12,  CO4,  -2 * SIZE
   MOV c31, c11
   ST $f13,  CO4,  -1 * SIZE
MOV    c41, c11
   blt $r0,    I, .L31
   .align 3

.L40:
   andi    I,  M, 1
MOV    c61, c11
   bge $r0,    I, .L49
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
   andi    L,  K, 3
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
   LD $f22,  CO1,  0 * SIZE
   LD $f8,  CO1,  1 * SIZE
   LD $f23,  CO2,  0 * SIZE
   LD $f9,  CO2,  1 * SIZE
   LD $f10,  CO3,  0 * SIZE
   MADD  $f22, c11, ALPHA_R, $f22
   LD $f11,  CO3,  1 * SIZE
   MADD  $f8, c11, ALPHA_I, $f8
   LD $f12,  CO4,  0 * SIZE
   MADD  $f23, c21, ALPHA_R, $f23
   LD $f13,  CO4,  1 * SIZE
   MADD  $f9, c21, ALPHA_I, $f9
   MADD  $f10, c31, ALPHA_R, $f10
   ST $f22,  CO1,   0 * SIZE
   MADD  $f11, c31, ALPHA_I, $f11
   ST $f8,  CO1,   1 * SIZE
   MADD  $f12, c41, ALPHA_R, $f12
   ST $f23,  CO2,   0 * SIZE
   MADD  $f13, c41, ALPHA_I, $f13
   ST $f9,  CO2,   1 * SIZE
   ST $f10,  CO3,   0 * SIZE
   ST $f11,  CO3,   1 * SIZE
   ST $f12,  CO4,   0 * SIZE
   ST $f13,  CO4,   1 * SIZE
   .align 3

.L49:
   move    B, BO
   .align 3

.L50:
   andi    J,  N, 2
move   AO, A
   bge $r0,    J, .L70
   move    CO1, C
   add.d   CO2, C,      LDC
   srai.d  I,  M, 1
add.d  C,   CO2,    LDC
   bge $r0,    I, .L60
.L51:
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
   andi    L,  K, 3
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
   LD $f22,  CO1,  0 * SIZE
   LD $f8,  CO1,  1 * SIZE
   LD $f23,  CO1,  2 * SIZE
   LD $f9,  CO1,  3 * SIZE
   LD $f10,  CO2,  0 * SIZE
   LD $f11,  CO2,  1 * SIZE
   LD $f12,  CO2,  2 * SIZE
   LD $f13,  CO2,  3 * SIZE
   MADD  $f22, c11, ALPHA_R, $f22
   addi.d  I, I, -1
   MADD  $f8, c11, ALPHA_I, $f8
   addi.d  CO1,CO1, 4 * SIZE
   MADD  $f23, c12, ALPHA_R, $f23
   addi.d  CO2,CO2, 4 * SIZE
   MADD  $f9, c12, ALPHA_I, $f9
   MADD  $f10, c21, ALPHA_R, $f10
   MADD  $f11, c21, ALPHA_I, $f11
   MADD  $f12, c22, ALPHA_R, $f12
   MADD  $f13, c22, ALPHA_I, $f13
   ST $f22,  CO1,  -4 * SIZE
   ST $f8,  CO1,  -3 * SIZE
   ST $f23,  CO1,  -2 * SIZE
   ST $f9,  CO1,  -1 * SIZE
   ST $f10,  CO2,  -4 * SIZE
   ST $f11,  CO2,  -3 * SIZE
   ST $f12,  CO2,  -2 * SIZE
   ST $f13,  CO2,  -1 * SIZE
   blt $r0,    I, .L51
   .align 3

.L60:
   andi    I,  M, 1
   bge $r0,    I, .L69
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
   andi    L,  K, 3
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
   LD $f22,  CO1,  0 * SIZE
   LD $f8,  CO1,  1 * SIZE
   LD $f23,  CO2,  0 * SIZE
   LD $f9,  CO2,  1 * SIZE
   ADD c11, c11, c31
   ADD c21, c21, c41
   MADD  $f22, c11, ALPHA_R, $f22
   MADD  $f8, c11, ALPHA_I, $f8
   MADD  $f23, c21, ALPHA_R, $f23
   MADD  $f9, c21, ALPHA_I, $f9
   ST $f22,  CO1,   0 * SIZE
   ST $f8,  CO1,   1 * SIZE
   ST $f23,  CO2,   0 * SIZE
   ST $f9,  CO2,   1 * SIZE
   .align 3

.L69:
   move    B, BO
   .align 3

.L70:
   andi    J,  N, 1
move   AO, A
   bge $r0,    J, .L999
   move    CO1, C
   srai.d  I,  M, 1
add.d  C,   CO1,    LDC
   bge $r0,    I, .L80
.L71:
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
   andi    L,  K, 3
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
   LD $f22,  CO1,  0 * SIZE
   LD $f8,  CO1,  1 * SIZE
   LD $f23,  CO1,  2 * SIZE
   LD $f9,  CO1,  3 * SIZE
   ADD c11, c11, c21
   addi.d  I, I, -1
   ADD c12, c12, c22
   addi.d  CO1,CO1, 4 * SIZE
   MADD  $f22, c11, ALPHA_R, $f22
   MADD  $f8, c11, ALPHA_I, $f8
   MADD  $f23, c12, ALPHA_R, $f23
   MADD  $f9, c12, ALPHA_I, $f9
   ST $f22,  CO1,  -4 * SIZE
   ST $f8,  CO1,  -3 * SIZE
   ST $f23,  CO1,  -2 * SIZE
   ST $f9,  CO1,  -1 * SIZE
   blt $r0,    I, .L71
   .align 3

.L80:
   andi    I,  M, 1
   bge $r0,    I, .L89
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
   andi    L,  K, 3
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
   LD $f22,  CO1,  0 * SIZE
   LD $f8,  CO1,  1 * SIZE
   ADD c11, c11, c21
   MADD  $f22, c11, ALPHA_R, $f22
   MADD  $f8, c11, ALPHA_I, $f8
   ST $f22,  CO1,   0 * SIZE
   ST $f8,  CO1,   1 * SIZE
   .align 3

.L89:
   move    B, BO
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
   fld.d  $f29,  $sp,  88
   addi.d  $sp, $sp, 128
   move $r4, $r17
   fmov.d $f0, $f22
   jirl    $r0, $r1, 0x0

   EPILOGUE
