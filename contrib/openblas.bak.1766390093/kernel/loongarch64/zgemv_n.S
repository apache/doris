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

#define M          $r4
#define N          $r5
#define A          $r7
#define LDA        $r8
#define X          $r9
#define INCX       $r10
#define Y          $r11
#define INCY       $r6
#define BUFFER     $r17

#define YORIG      $r18
#define XX         $r12
#define YY         $r13
#define I          $r14
#define J          $r15
#define AO1        $r23
#define AO2        $r24

#define ALPHA_R    $f0
#define ALPHA_I    $f1
#define a1         $f22
#define a2         $f8
#define a3         $f23
#define a4         $f9
#define a5         $f10
#define a6         $f11
#define a7         $f12
#define a8         $f13
#define x1         $f14
#define x2         $f15
#define x3         $f16
#define x4         $f17
#define y1         $f3
#define y2         $f4
#define y3         $f2
#define y4         $f5
#define t1         $f6
#define t2         $f7
#define t3         $f18
#define t4         $f19
#define t5         $f20
#define t6         $f21
#define t7         $f24
#define t8         $f25

#if !defined(CONJ) && !defined(XCONJ)
#define    MADD1        MADD
#define    MADD2        MADD
#define    MADD3        NMSUB
#define    MADD4        MADD
#endif
#if  defined(CONJ) && !defined(XCONJ)
#define    MADD1        MADD
#define    MADD2        MADD
#define    MADD3        MADD
#define    MADD4        NMSUB
#endif
#if  !defined(CONJ) && defined(XCONJ)
#define    MADD1        MADD
#define    MADD2        NMSUB
#define    MADD3        MADD
#define    MADD4        MADD
#endif
#if  defined(CONJ) && defined(XCONJ)
#define    MADD1        MADD
#define    MADD2        NMSUB
#define    MADD3        NMSUB
#define    MADD4        NMSUB
#endif

   PROLOGUE

   LDARG  INCY,    $sp,  0
   LDARG  BUFFER,  $sp,  8
#ifndef __64BIT__
   addi.d  $sp, $sp, -64
#else
   addi.d  $sp, $sp, -32
#endif
   SDARG  $r23,  $sp,    0
   SDARG  $r24,  $sp,    8
   fst.d  $f24,  $sp,  16
   fst.d  $f25,  $sp,  24
#ifndef __64BIT__
   fst.d  $f18,  $sp,  32
   fst.d  $f19,  $sp,  40
   fst.d  $f20,  $sp,  48
   fst.d  $f21,  $sp,  56
#endif
   slli.d     LDA,     LDA,  ZBASE_SHIFT
   slli.d INCX, INCX, ZBASE_SHIFT
   bge $r0,    M, .L999
   slli.d INCY, INCY, ZBASE_SHIFT
   bge $r0,    N, .L999
   li.d  I, 2 * SIZE
   move   YORIG, Y
   beq INCY, I, .L10
   srai.d  I,  M, 2
   move    YORIG, BUFFER
   move    XX, Y
   move   YY, BUFFER
   bge $r0,    I, .L05
   .align 3

.L02:
   LD a1,  XX,  0 * SIZE
   LD a2,  XX,  1 * SIZE
   add.d   XX, XX, INCY
   LD a3,  XX,  0 * SIZE
   LD a4,  XX,  1 * SIZE
   add.d   XX, XX, INCY
   LD a5,  XX,  0 * SIZE
   LD a6,  XX,  1 * SIZE
   add.d   XX, XX, INCY
   LD a7,  XX,  0 * SIZE
   LD a8,  XX,  1 * SIZE
   add.d   XX, XX, INCY
   addi.d  I, I, -1
   addi.d  YY, YY, 8 * SIZE
   ST a1,  YY,  -8 * SIZE
   ST a2,  YY,  -7 * SIZE
   ST a3,  YY,  -6 * SIZE
   ST a4,  YY,  -5 * SIZE
   ST a5,  YY,  -4 * SIZE
   ST a6,  YY,  -3 * SIZE
   ST a7,  YY,  -2 * SIZE
   ST a8,  YY,  -1 * SIZE
   blt $r0,    I, .L02
   .align 3

.L05:
   andi    I,  M, 3
   bge $r0,    I, .L10
   .align 3

.L06:
   LD a1,  XX,  0 * SIZE
   LD a2,  XX,  1 * SIZE
   add.d   XX, XX, INCY
   addi.d  I, I, -1
   ST a1,  YY,  0 * SIZE
   ST a2,  YY,  1 * SIZE
   addi.d YY, YY, 2 * SIZE
   blt $r0,    I, .L06
   .align 3

.L10:
   srai.d  J,  N, 1
   bge $r0,    J, .L20
   .align 3

.L11:
   LD x1,  X,  0 * SIZE
   LD x2,  X,  1 * SIZE
   add.d   X, X, INCX
   LD x3,  X,  0 * SIZE
   LD x4,  X,  1 * SIZE
   add.d   X, X, INCX
   MUL a1, ALPHA_R, x1
   move    AO1, A
   MUL a2, ALPHA_I, x1
   add.d   AO2, A,      LDA
   MUL a3, ALPHA_R, x3
   add.d   A,   AO2,    LDA
   MUL a4, ALPHA_I, x3
#ifndef XCONJ
   NMSUB  x1, x2, ALPHA_I, a1
   MADD  x2, x2, ALPHA_R, a2
   NMSUB  x3, x4, ALPHA_I, a3
   MADD  x4, x4, ALPHA_R, a4
#else
   MADD  x1, x2, ALPHA_I, a1
   MSUB  x2, x2, ALPHA_R, a2
   MADD  x3, x4, ALPHA_I, a3
   MSUB  x4, x4, ALPHA_R, a4
#endif
   srai.d  I,  M, 2
   move   YY, YORIG
   bge $r0,    I, .L15
   LD y1,  YY,  0 * SIZE
   LD a1,  AO1,  0 * SIZE
   LD y2,  YY,  1 * SIZE
   LD a3,  AO1,  2 * SIZE
   LD y3,  YY,  2 * SIZE
   LD a2,  AO1,  1 * SIZE
   LD y4,  YY,  3 * SIZE
   LD a4,  AO1,  3 * SIZE
   LD a5,  AO2,  0 * SIZE
   LD a6,  AO2,  1 * SIZE
   LD a7,  AO2,  2 * SIZE
   LD a8,  AO2,  3 * SIZE
   MADD1  t1, a1, x1, y1
   LD y1,  YY,   4 * SIZE
   MADD2  t2, a1, x2, y2
   LD a1,  AO1,   4 * SIZE
   MADD1  t3, a3, x1, y3
   LD y2,  YY,   5 * SIZE
   MADD2  t4, a3, x2, y4
   LD a3,  AO1,   6 * SIZE
   MADD3  t1, a2, x2, t1
   LD y3,  YY,   6 * SIZE
   MADD4  t2, a2, x1, t2
   LD a2,  AO1,   5 * SIZE
   MADD3  t3, a4, x2, t3
   LD y4,  YY,   7 * SIZE
   MADD4  t4, a4, x1, t4
   LD a4,  AO1,   7 * SIZE
   MADD1  t1, a5, x3, t1
   MADD2  t2, a5, x4, t2
   LD a5,  AO2,   4 * SIZE
   MADD1  t3, a7, x3, t3
   MADD2  t4, a7, x4, t4
   LD a7,  AO2,   6 * SIZE
   MADD3  t1, a6, x4, t1
   MADD4  t2, a6, x3, t2
   LD a6,  AO2,   5 * SIZE
   MADD3  t3, a8, x4, t3
   addi.d  I, I, -1
   MADD4  t4, a8, x3, t4
   LD a8,  AO2,   7 * SIZE
   bge $r0,    I, .L13
   .align  3
.L12:
   MADD1  t5, a1, x1, y1
   LD y1,  YY,   8 * SIZE
   MADD2  t6, a1, x2, y2
   LD a1,  AO1,   8 * SIZE
   MADD1  t7, a3, x1, y3
   LD y2,  YY,   9 * SIZE
   MADD2  t8, a3, x2, y4
   LD a3,  AO1,  10 * SIZE
   MADD3  t5, a2, x2, t5
   LD y3,  YY,  10 * SIZE
   MADD4  t6, a2, x1, t6
   LD a2,  AO1,   9 * SIZE
   MADD3  t7, a4, x2, t7
   LD y4,  YY,  11 * SIZE
   MADD4  t8, a4, x1, t8
   LD a4,  AO1,  11 * SIZE
   MADD1  t5, a5, x3, t5
   ST t1,  YY,   0 * SIZE
   MADD2  t6, a5, x4, t6
   LD a5,  AO2,   8 * SIZE
   MADD1  t7, a7, x3, t7
   ST t2,  YY,   1 * SIZE
   MADD2  t8, a7, x4, t8
   LD a7,  AO2,  10 * SIZE
   MADD3  t5, a6, x4, t5
   ST t3,  YY,   2 * SIZE
   MADD4  t6, a6, x3, t6
   LD a6,  AO2,   9 * SIZE
   MADD3  t7, a8, x4, t7
   ST t4,  YY,   3 * SIZE
   MADD4  t8, a8, x3, t8
   LD a8,  AO2,  11 * SIZE
   MADD1  t1, a1, x1, y1
   LD y1,  YY,  12 * SIZE
   MADD2  t2, a1, x2, y2
   LD a1,  AO1,  12 * SIZE
   MADD1  t3, a3, x1, y3
   LD y2,  YY,  13 * SIZE
   MADD2  t4, a3, x2, y4
   LD a3,  AO1,  14 * SIZE
   MADD3  t1, a2, x2, t1
   LD y3,  YY,  14 * SIZE
   MADD4  t2, a2, x1, t2
   LD a2,  AO1,  13 * SIZE
   MADD3  t3, a4, x2, t3
   LD y4,  YY,  15 * SIZE
   MADD4  t4, a4, x1, t4
   LD a4,  AO1,  15 * SIZE
   MADD1  t1, a5, x3, t1
   ST t5,  YY,   4 * SIZE
   MADD2  t2, a5, x4, t2
   LD a5,  AO2,  12 * SIZE
   MADD1  t3, a7, x3, t3
   ST t6,  YY,   5 * SIZE
   MADD2  t4, a7, x4, t4
   LD a7,  AO2,  14 * SIZE
   MADD3  t1, a6, x4, t1
   ST t7,  YY,   6 * SIZE
   MADD4  t2, a6, x3, t2
   LD a6,  AO2,  13 * SIZE
   MADD3  t3, a8, x4, t3
   ST t8,  YY,   7 * SIZE
   MADD4  t4, a8, x3, t4
   LD a8,  AO2,  15 * SIZE
   addi.d  I, I, -1
   addi.d  YY,  YY,   8 * SIZE
   addi.d  AO1, AO1,  8 * SIZE
   addi.d AO2, AO2,  8 * SIZE
   blt $r0,    I, .L12
   .align 3

.L13:
   ST t1,  YY,   0 * SIZE
   MADD1  t1, a1, x1, y1
   ST t2,  YY,   1 * SIZE
   MADD2  t2, a1, x2, y2
   ST t3,  YY,   2 * SIZE
   MADD1  t3, a3, x1, y3
   ST t4,  YY,   3 * SIZE
   MADD2  t4, a3, x2, y4
   MADD3  t1, a2, x2, t1
   MADD4  t2, a2, x1, t2
   MADD3  t3, a4, x2, t3
   MADD4  t4, a4, x1, t4
   MADD1  t1, a5, x3, t1
   MADD2  t2, a5, x4, t2
   MADD1  t3, a7, x3, t3
   MADD2  t4, a7, x4, t4
   MADD3  t1, a6, x4, t1
   addi.d  AO1, AO1,  8 * SIZE
   MADD4  t2, a6, x3, t2
   addi.d  AO2, AO2,  8 * SIZE
   MADD3  t3, a8, x4, t3
   addi.d  YY,  YY,   8 * SIZE
   MADD4  t4, a8, x3, t4
   ST t1,  YY,  -4 * SIZE
   ST t2,  YY,  -3 * SIZE
   ST t3,  YY,  -2 * SIZE
   ST t4,  YY,  -1 * SIZE
   .align 3

.L15:
   andi    I,  M, 2
   bge $r0,    I, .L16
   LD a1,  AO1,  0 * SIZE
   LD y1,  YY,  0 * SIZE
   LD a2,  AO1,  1 * SIZE
   LD y2,  YY,  1 * SIZE
   LD a3,  AO1,  2 * SIZE
   LD y3,  YY,  2 * SIZE
   LD a4,  AO1,  3 * SIZE
   LD y4,  YY,  3 * SIZE
   MADD1  t1, a1, x1, y1
   LD a5,  AO2,  0 * SIZE
   MADD2  t2, a1, x2, y2
   LD a6,  AO2,  1 * SIZE
   MADD1  t3, a3, x1, y3
   LD a7,  AO2,  2 * SIZE
   MADD2  t4, a3, x2, y4
   LD a8,  AO2,  3 * SIZE
   MADD3  t1, a2, x2, t1
   MADD4  t2, a2, x1, t2
   MADD3  t3, a4, x2, t3
   MADD4  t4, a4, x1, t4
   MADD1  t1, a5, x3, t1
   MADD2  t2, a5, x4, t2
   MADD1  t3, a7, x3, t3
   MADD2  t4, a7, x4, t4
   MADD3  t1, a6, x4, t1
   addi.d  YY,  YY,   4 * SIZE
   MADD4  t2, a6, x3, t2
   addi.d  AO1, AO1,  4 * SIZE
   MADD3  t3, a8, x4, t3
   addi.d  AO2, AO2,  4 * SIZE
   MADD4  t4, a8, x3, t4
   ST t1,  YY,  -4 * SIZE
   ST t2,  YY,  -3 * SIZE
   ST t3,  YY,  -2 * SIZE
   ST t4,  YY,  -1 * SIZE
   .align 3

.L16:
   andi    I,  M, 1
   bge $r0,    I, .L19
   LD y1,  YY,  0 * SIZE
   LD y2,  YY,  1 * SIZE
   LD a1,  AO1,  0 * SIZE
   LD a2,  AO1,  1 * SIZE
   MADD1  t1, a1, x1, y1
   LD a5,  AO2,  0 * SIZE
   MADD2  t2, a1, x2, y2
   LD a6,  AO2,  1 * SIZE
   MADD3  t1, a2, x2, t1
   MADD4  t2, a2, x1, t2
   MADD1  t1, a5, x3, t1
   MADD2  t2, a5, x4, t2
   MADD3  t1, a6, x4, t1
   MADD4  t2, a6, x3, t2
   ST t1,  YY,   0 * SIZE
   ST t2,  YY,   1 * SIZE
   .align 3

.L19:
   addi.d  J, J, -1
   blt $r0,    J, .L11
   .align 3

.L20:
   andi    J,  N, 1
   bge $r0,    J, .L900
   LD x1,  X,  0 * SIZE
   LD x2,  X,  1 * SIZE
   add.d   X, X, INCX
   MUL a1, ALPHA_R, x1
   move    AO1, A
   MUL a2, ALPHA_I, x1
#ifndef XCONJ
   NMSUB  x1, x2, ALPHA_I, a1
   MADD  x2, x2, ALPHA_R, a2
#else
   MADD  x1, x2, ALPHA_I, a1
   MSUB  x2, x2, ALPHA_R, a2
#endif
   srai.d  I,  M, 2
   move   YY, YORIG
   bge $r0,    I, .L25
   LD y1,  YY,  0 * SIZE
   LD a1,  AO1,  0 * SIZE
   LD y2,  YY,  1 * SIZE
   LD a3,  AO1,  2 * SIZE
   LD y3,  YY,  2 * SIZE
   LD a2,  AO1,  1 * SIZE
   LD y4,  YY,  3 * SIZE
   LD a4,  AO1,  3 * SIZE
   MADD1  t1, a1, x1, y1
   LD y1,  YY,   4 * SIZE
   MADD2  t2, a1, x2, y2
   LD a1,  AO1,   4 * SIZE
   MADD1  t3, a3, x1, y3
   LD y2,  YY,   5 * SIZE
   MADD2  t4, a3, x2, y4
   LD a3,  AO1,   6 * SIZE
   MADD3  t1, a2, x2, t1
   LD y3,  YY,   6 * SIZE
   MADD4  t2, a2, x1, t2
   LD a2,  AO1,   5 * SIZE
   MADD3  t3, a4, x2, t3
   LD y4,  YY,   7 * SIZE
   MADD4  t4, a4, x1, t4
   addi.d  I, I, -1
   LD a4,  AO1,   7 * SIZE
   bge $r0,    I, .L23
   .align  3
.L22:
   MADD1  t5, a1, x1, y1
   LD y1,  YY,   8 * SIZE
   MADD2  t6, a1, x2, y2
   LD a1,  AO1,   8 * SIZE
   MADD1  t7, a3, x1, y3
   LD y2,  YY,   9 * SIZE
   MADD2  t8, a3, x2, y4
   LD a3,  AO1,  10 * SIZE
   MADD3  t5, a2, x2, t5
   LD y3,  YY,  10 * SIZE
   MADD4  t6, a2, x1, t6
   LD a2,  AO1,   9 * SIZE
   MADD3  t7, a4, x2, t7
   LD y4,  YY,  11 * SIZE
   MADD4  t8, a4, x1, t8
   LD a4,  AO1,  11 * SIZE
   ST t1,  YY,   0 * SIZE
   ST t2,  YY,   1 * SIZE
   ST t3,  YY,   2 * SIZE
   ST t4,  YY,   3 * SIZE
   MADD1  t1, a1, x1, y1
   LD y1,  YY,  12 * SIZE
   MADD2  t2, a1, x2, y2
   LD a1,  AO1,  12 * SIZE
   MADD1  t3, a3, x1, y3
   LD y2,  YY,  13 * SIZE
   MADD2  t4, a3, x2, y4
   LD a3,  AO1,  14 * SIZE
   MADD3  t1, a2, x2, t1
   LD y3,  YY,  14 * SIZE
   MADD4  t2, a2, x1, t2
   LD a2,  AO1,  13 * SIZE
   MADD3  t3, a4, x2, t3
   LD y4,  YY,  15 * SIZE
   MADD4  t4, a4, x1, t4
   LD a4,  AO1,  15 * SIZE
   ST t5,  YY,   4 * SIZE
   ST t6,  YY,   5 * SIZE
   ST t7,  YY,   6 * SIZE
   ST t8,  YY,   7 * SIZE
   addi.d  I, I, -1
   addi.d  YY,  YY,   8 * SIZE
   addi.d AO1, AO1,  8 * SIZE
   blt $r0,    I, .L22
   .align 3

.L23:
   ST t1,  YY,   0 * SIZE
   MADD1  t1, a1, x1, y1
   ST t2,  YY,   1 * SIZE
   MADD2  t2, a1, x2, y2
   ST t3,  YY,   2 * SIZE
   MADD1  t3, a3, x1, y3
   ST t4,  YY,   3 * SIZE
   MADD2  t4, a3, x2, y4
   MADD3  t1, a2, x2, t1
   addi.d  AO1, AO1,  8 * SIZE
   MADD4  t2, a2, x1, t2
   addi.d  YY,  YY,   8 * SIZE
   MADD3  t3, a4, x2, t3
   MADD4  t4, a4, x1, t4
   ST t1,  YY,  -4 * SIZE
   ST t2,  YY,  -3 * SIZE
   ST t3,  YY,  -2 * SIZE
   ST t4,  YY,  -1 * SIZE
   .align 3

.L25:
   andi    I,  M, 2
   bge $r0,    I, .L26
   LD a1,  AO1,  0 * SIZE
   LD y1,  YY,  0 * SIZE
   LD a2,  AO1,  1 * SIZE
   LD y2,  YY,  1 * SIZE
   LD a3,  AO1,  2 * SIZE
   LD y3,  YY,  2 * SIZE
   LD a4,  AO1,  3 * SIZE
   LD y4,  YY,  3 * SIZE
   MADD1  t1, a1, x1, y1
   MADD2  t2, a1, x2, y2
   MADD1  t3, a3, x1, y3
   MADD2  t4, a3, x2, y4
   MADD3  t1, a2, x2, t1
   addi.d  YY,  YY,   4 * SIZE
   MADD4  t2, a2, x1, t2
   addi.d  AO1, AO1,  4 * SIZE
   MADD3  t3, a4, x2, t3
   MADD4  t4, a4, x1, t4
   ST t1,  YY,  -4 * SIZE
   ST t2,  YY,  -3 * SIZE
   ST t3,  YY,  -2 * SIZE
   ST t4,  YY,  -1 * SIZE
   .align 3

.L26:
   andi    I,  M, 1
   bge $r0,    I, .L900
   LD y1,  YY,  0 * SIZE
   LD y2,  YY,  1 * SIZE
   LD a1,  AO1,  0 * SIZE
   LD a2,  AO1,  1 * SIZE
   MADD1  t1, a1, x1, y1
   MADD2  t2, a1, x2, y2
   MADD3  t1, a2, x2, t1
   MADD4  t2, a2, x1, t2
   ST t1,  YY,   0 * SIZE
   ST t2,  YY,   1 * SIZE
   .align 3

.L900:
   li.d  YORIG, 2 * SIZE
   srai.d I,  M, 2
   beq INCY, YORIG, .L999
   move   XX, BUFFER
   bge $r0,    I, .L905
   .align 3

.L902:
   LD a1,  XX,  0 * SIZE
   LD a2,  XX,  1 * SIZE
   LD a3,  XX,  2 * SIZE
   LD a4,  XX,  3 * SIZE
   LD a5,  XX,  4 * SIZE
   LD a6,  XX,  5 * SIZE
   LD a7,  XX,  6 * SIZE
   LD a8,  XX,  7 * SIZE
   addi.d  I, I, -1
   ST a1,  Y,  0 * SIZE
   ST a2,  Y,  1 * SIZE
   add.d   Y, Y, INCY
   ST a3,  Y,  0 * SIZE
   ST a4,  Y,  1 * SIZE
   add.d   Y, Y, INCY
   ST a5,  Y,  0 * SIZE
   ST a6,  Y,  1 * SIZE
   add.d   Y, Y, INCY
   ST a7,  Y,  0 * SIZE
   ST a8,  Y,  1 * SIZE
   add.d   Y, Y, INCY
   addi.d XX, XX, 8 * SIZE
   blt $r0,    I, .L902
   .align 3

.L905:
   andi    I,  M, 3
   bge $r0,    I, .L999
   .align 3

.L906:
   LD a1,  XX,  0 * SIZE
   LD a2,  XX,  1 * SIZE
   addi.d  XX, XX, 2 * SIZE
   addi.d  I, I, -1
   ST a1,  Y,  0 * SIZE
   ST a2,  Y,  1 * SIZE
   add.d  Y, Y, INCY
   blt $r0,    I, .L906
   .align 3

.L999:
   LDARG  $r23,  $sp,    0
   LDARG  $r24,  $sp,    8
   fld.d  $f24,  $sp,  16
   fld.d  $f25,  $sp,  24
#ifndef __64BIT__
   fld.d  $f18,  $sp,  32
   fld.d  $f19,  $sp,  40
   fld.d  $f20,  $sp,  48
   fld.d  $f21,  $sp,  56
#endif
#ifdef __64BIT__
   addi.d  $sp, $sp, 32
#else
   addi.d  $sp, $sp, 64
#endif
   move $r4, $r17
   fmov.d $f0, $f22
   jirl    $r0, $r1, 0x0

   EPILOGUE
