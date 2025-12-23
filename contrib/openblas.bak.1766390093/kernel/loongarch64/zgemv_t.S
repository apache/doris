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

#define XORIG      $r18
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
#define y1         $f14
#define y2         $f15
#define y3         $f16
#define y4         $f17
#define x1         $f3
#define x2         $f4
#define x3         $f2
#define x4         $f5
#define x5         $f6
#define x6         $f7
#define x7         $f18
#define x8         $f19

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
#ifdef __64BIT__
   addi.d  $sp, $sp, -16
#else
   addi.d  $sp, $sp, -32
#endif
   MTC  y1, $r0
   SDARG  $r23,  $sp,    0
   SDARG  $r24,  $sp,    8
   slli.d     LDA,     LDA,  ZBASE_SHIFT
#ifndef __64BIT__
   fst.d  $f18,  $sp,  16
   fst.d  $f19,  $sp,  24
#endif
   slli.d INCX, INCX, ZBASE_SHIFT
   bge $r0,    M, .L999
   slli.d INCY, INCY, ZBASE_SHIFT
   bge $r0,    N, .L999
   li.d  I, 2 * SIZE
   move   XORIG, X
   beq INCX, I, .L10
   srai.d  I,  M, 2
   move    XORIG, BUFFER
   move   YY, BUFFER
   bge $r0,    I, .L05
   .align 3

.L02:
   LD a1,  X,  0 * SIZE
   LD a2,  X,  1 * SIZE
   add.d   X, X, INCX
   LD a3,  X,  0 * SIZE
   LD a4,  X,  1 * SIZE
   add.d   X, X, INCX
   LD a5,  X,  0 * SIZE
   LD a6,  X,  1 * SIZE
   add.d   X, X, INCX
   LD a7,  X,  0 * SIZE
   LD a8,  X,  1 * SIZE
   add.d   X, X, INCX
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
   LD a1,  X,  0 * SIZE
   LD a2,  X,  1 * SIZE
   add.d   X, X, INCX
   ST a1,  YY,  0 * SIZE
   ST a2,  YY,  1 * SIZE
   addi.d  I, I, -1
   addi.d YY, YY, 2 * SIZE
   blt $r0,    I, .L06
   .align 3

.L10:
   srai.d  J,  N, 1
   move   YY, Y
   bge $r0,    J, .L20
   .align 3

.L11:
   move    AO1, A
   MOV y2, y1
   add.d   AO2, A,      LDA
   MOV y3, y1
   add.d   A,   AO2,    LDA
   MOV y4, y1
   srai.d  I,  M, 2
   move   XX, XORIG
   bge $r0,    I, .L15
   LD x1,  XX,  0 * SIZE
   LD x2,  XX,  1 * SIZE
   LD x4,  XX,  3 * SIZE
   LD a1,  AO1,  0 * SIZE
   LD a3,  AO2,  0 * SIZE
   LD a2,  AO1,  1 * SIZE
   LD a4,  AO2,  1 * SIZE
   LD a5,  AO1,  2 * SIZE
   LD a7,  AO2,  2 * SIZE
   LD a6,  AO1,  3 * SIZE
   LD a8,  AO2,  3 * SIZE
   addi.d  I, I, -1
   bge $r0,    I, .L13
   .align  3
.L12:
   MADD1  y1, a1, x1, y1
   LD x3,  XX,  2 * SIZE
   MADD2  y2, a1, x2, y2
   LD a1,  AO1,  4 * SIZE
   MADD1  y3, a3, x1, y3
   MADD2  y4, a3, x2, y4
   LD a3,  AO2,  4 * SIZE
   MADD3  y1, a2, x2, y1
   MADD4  y2, a2, x1, y2
   LD a2,  AO1,  5 * SIZE
   MADD3  y3, a4, x2, y3
   LD x2,  XX,  5 * SIZE
   MADD4  y4, a4, x1, y4
   LD a4,  AO2,  5 * SIZE
   MADD1  y1, a5, x3, y1
   LD x1,  XX,  4 * SIZE
   MADD2  y2, a5, x4, y2
   LD a5,  AO1,  6 * SIZE
   MADD1  y3, a7, x3, y3
   MADD2  y4, a7, x4, y4
   LD a7,  AO2,  6 * SIZE
   MADD3  y1, a6, x4, y1
   addi.d  I, I, -1
   MADD4  y2, a6, x3, y2
   LD a6,  AO1,  7 * SIZE
   MADD3  y3, a8, x4, y3
   LD x4,  XX,  7 * SIZE
   MADD4  y4, a8, x3, y4
   LD a8,  AO2,  7 * SIZE
   MADD1  y1, a1, x1, y1
   LD x3,  XX,  6 * SIZE
   MADD2  y2, a1, x2, y2
   LD a1,  AO1,   8 * SIZE
   MADD1  y3, a3, x1, y3
   MADD2  y4, a3, x2, y4
   LD a3,  AO2,   8 * SIZE
   MADD3  y1, a2, x2, y1
   MADD4  y2, a2, x1, y2
   LD a2,  AO1,   9 * SIZE
   MADD3  y3, a4, x2, y3
   LD x2,  XX,   9 * SIZE
   MADD4  y4, a4, x1, y4
   LD a4,  AO2,   9 * SIZE
   MADD1  y1, a5, x3, y1
   LD x1,  XX,   8 * SIZE
   MADD2  y2, a5, x4, y2
   LD a5,  AO1,  10 * SIZE
   MADD1  y3, a7, x3, y3
   addi.d  XX,  XX,   8 * SIZE
   MADD2  y4, a7, x4, y4
   LD a7,  AO2,  10 * SIZE
   MADD3  y1, a6, x4, y1
   addi.d  AO2, AO2,  8 * SIZE
   MADD4  y2, a6, x3, y2
   LD a6,  AO1,  11 * SIZE
   MADD3  y3, a8, x4, y3
   LD x4,  XX,   3 * SIZE
   MADD4  y4, a8, x3, y4
   LD a8,  AO2,   3 * SIZE
   addi.d AO1, AO1,  8 * SIZE
   blt $r0,    I, .L12
   .align 3

.L13:
   MADD1  y1, a1, x1, y1
   LD x3,  XX,   2 * SIZE
   MADD2  y2, a1, x2, y2
   LD a1,  AO1,  4 * SIZE
   MADD1  y3, a3, x1, y3
   MADD2  y4, a3, x2, y4
   LD a3,  AO2,  4 * SIZE
   MADD3  y1, a2, x2, y1
   MADD4  y2, a2, x1, y2
   LD a2,  AO1,  5 * SIZE
   MADD3  y3, a4, x2, y3
   LD x2,  XX,  5 * SIZE
   MADD4  y4, a4, x1, y4
   LD a4,  AO2,  5 * SIZE
   MADD1  y1, a5, x3, y1
   LD x1,  XX,  4 * SIZE
   MADD2  y2, a5, x4, y2
   LD a5,  AO1,  6 * SIZE
   MADD1  y3, a7, x3, y3
   MADD2  y4, a7, x4, y4
   LD a7,  AO2,  6 * SIZE
   MADD3  y1, a6, x4, y1
   MADD4  y2, a6, x3, y2
   LD a6,  AO1,  7 * SIZE
   MADD3  y3, a8, x4, y3
   LD x4,  XX,  7 * SIZE
   MADD4  y4, a8, x3, y4
   LD a8,  AO2,  7 * SIZE
   MADD1  y1, a1, x1, y1
   LD x3,  XX,  6 * SIZE
   MADD2  y2, a1, x2, y2
   MADD1  y3, a3, x1, y3
   MADD2  y4, a3, x2, y4
   MADD3  y1, a2, x2, y1
   MADD4  y2, a2, x1, y2
   MADD3  y3, a4, x2, y3
   MADD4  y4, a4, x1, y4
   MADD1  y1, a5, x3, y1
   MADD2  y2, a5, x4, y2
   MADD1  y3, a7, x3, y3
   MADD2  y4, a7, x4, y4
   MADD3  y1, a6, x4, y1
   addi.d  XX,  XX,   8 * SIZE
   MADD4  y2, a6, x3, y2
   addi.d  AO1, AO1,  8 * SIZE
   MADD3  y3, a8, x4, y3
   addi.d  AO2, AO2,  8 * SIZE
   MADD4  y4, a8, x3, y4
   .align 3

.L15:
   andi    I,  M, 2
   bge $r0,    I, .L17
   LD x1,  XX,  0 * SIZE
   LD x2,  XX,  1 * SIZE
   LD x3,  XX,  2 * SIZE
   LD x4,  XX,  3 * SIZE
   LD a1,  AO1,  0 * SIZE
   LD a3,  AO2,  0 * SIZE
   LD a2,  AO1,  1 * SIZE
   LD a4,  AO2,  1 * SIZE
   LD a5,  AO1,  2 * SIZE
   LD a7,  AO2,  2 * SIZE
   LD a6,  AO1,  3 * SIZE
   LD a8,  AO2,  3 * SIZE
   MADD1  y1, a1, x1, y1
   MADD2  y2, a1, x2, y2
   MADD1  y3, a3, x1, y3
   MADD2  y4, a3, x2, y4
   MADD3  y1, a2, x2, y1
   MADD4  y2, a2, x1, y2
   MADD3  y3, a4, x2, y3
   MADD4  y4, a4, x1, y4
   MADD1  y1, a5, x3, y1
   MADD2  y2, a5, x4, y2
   MADD1  y3, a7, x3, y3
   MADD2  y4, a7, x4, y4
   MADD3  y1, a6, x4, y1
   addi.d  XX,  XX,   4 * SIZE
   MADD4  y2, a6, x3, y2
   addi.d  AO1, AO1,  4 * SIZE
   MADD3  y3, a8, x4, y3
   addi.d  AO2, AO2,  4 * SIZE
   MADD4  y4, a8, x3, y4
   .align 3

.L17:
   andi    I,  M, 1
.align 3

   bge $r0,    I, .L19
.L18:
   LD x1,  XX,  0 * SIZE
   LD x2,  XX,  1 * SIZE
   LD a1,  AO1,  0 * SIZE
   LD a3,  AO2,  0 * SIZE
   MADD1  y1, a1, x1, y1
   LD a2,  AO1,  1 * SIZE
   MADD2  y2, a1, x2, y2
   LD a4,  AO2,  1 * SIZE
   MADD1  y3, a3, x1, y3
   MADD2  y4, a3, x2, y4
   MADD3  y1, a2, x2, y1
   MADD4  y2, a2, x1, y2
   MADD3  y3, a4, x2, y3
   MADD4  y4, a4, x1, y4
   .align 3

.L19:
   LD a1,  Y,  0 * SIZE
   LD a2,  Y,  1 * SIZE
   add.d   Y, Y, INCY
   LD a3,  Y,  0 * SIZE
   LD a4,  Y,  1 * SIZE
   add.d   Y, Y, INCY
   MADD  a1, y1, ALPHA_R, a1
   MADD  a2, y1, ALPHA_I, a2
   MADD  a3, y3, ALPHA_R, a3
   MADD  a4, y3, ALPHA_I, a4
   NMSUB  a1, y2, ALPHA_I, a1
   MADD  a2, y2, ALPHA_R, a2
   NMSUB  a3, y4, ALPHA_I, a3
   MTC  y1, $r0
   MADD  a4, y4, ALPHA_R, a4
   addi.d  J, J, -1
   ST a1,  YY,   0 * SIZE
   ST a2,  YY,   1 * SIZE
   add.d   YY, YY, INCY
   ST a3,  YY,   0 * SIZE
   ST a4,  YY,   1 * SIZE
   add.d  YY, YY, INCY
   blt $r0,    J, .L11
   .align 3

.L20:
   andi    J,  N, 1
   MOV y2, y1
   srai.d I,  M, 2
   bge $r0,    J, .L999
   MOV y3, y1
   move    AO1, A
   MOV y4, y1
   move   XX, XORIG
   bge $r0,    I, .L25
   LD a1,  AO1,  0 * SIZE
   LD x1,  XX,  0 * SIZE
   LD a2,  AO1,  1 * SIZE
   LD x2,  XX,  1 * SIZE
   LD a5,  AO1,  2 * SIZE
   LD x4,  XX,  3 * SIZE
   addi.d  I, I, -1
   LD a6,  AO1,  3 * SIZE
   bge $r0,    I, .L23
   .align  3
.L22:
   MADD1  y1, a1, x1, y1
   LD x3,  XX,  2 * SIZE
   MADD2  y2, a1, x2, y2
   LD a1,  AO1,  4 * SIZE
   MADD3  y3, a2, x2, y3
   LD x2,  XX,  5 * SIZE
   MADD4  y4, a2, x1, y4
   LD a2,  AO1,  5 * SIZE
   MADD1  y1, a5, x3, y1
   LD x1,  XX,  4 * SIZE
   MADD2  y2, a5, x4, y2
   LD a5,  AO1,  6 * SIZE
   MADD3  y3, a6, x4, y3
   LD x4,  XX,  7 * SIZE
   MADD4  y4, a6, x3, y4
   LD a6,  AO1,  7 * SIZE
   MADD1  y1, a1, x1, y1
   LD x3,  XX,  6 * SIZE
   MADD2  y2, a1, x2, y2
   LD a1,  AO1,   8 * SIZE
   MADD3  y3, a2, x2, y3
   LD x2,  XX,   9 * SIZE
   MADD4  y4, a2, x1, y4
   LD a2,  AO1,   9 * SIZE
   MADD1  y1, a5, x3, y1
   LD x1,  XX,   8 * SIZE
   MADD2  y2, a5, x4, y2
   LD a5,  AO1,  10 * SIZE
   MADD3  y3, a6, x4, y3
   LD x4,  XX,  11 * SIZE
   MADD4  y4, a6, x3, y4
   LD a6,  AO1,  11 * SIZE
   addi.d  I, I, -1
   addi.d  XX,  XX,   8 * SIZE
   addi.d AO1, AO1,  8 * SIZE
   blt $r0,    I, .L22
   .align 3

.L23:
   MADD1  y1, a1, x1, y1
   LD x3,  XX,   2 * SIZE
   MADD2  y2, a1, x2, y2
   LD a1,  AO1,  4 * SIZE
   MADD3  y3, a2, x2, y3
   LD x2,  XX,  5 * SIZE
   MADD4  y4, a2, x1, y4
   LD a2,  AO1,  5 * SIZE
   MADD1  y1, a5, x3, y1
   LD x1,  XX,  4 * SIZE
   MADD2  y2, a5, x4, y2
   LD a5,  AO1,  6 * SIZE
   MADD3  y3, a6, x4, y3
   LD x4,  XX,  7 * SIZE
   MADD4  y4, a6, x3, y4
   LD a6,  AO1,  7 * SIZE
   MADD1  y1, a1, x1, y1
   LD x3,  XX,  6 * SIZE
   MADD2  y2, a1, x2, y2
   MADD3  y3, a2, x2, y3
   MADD4  y4, a2, x1, y4
   MADD1  y1, a5, x3, y1
   MADD2  y2, a5, x4, y2
   MADD3  y3, a6, x4, y3
   addi.d  XX,  XX,   8 * SIZE
   MADD4  y4, a6, x3, y4
   addi.d  AO1, AO1,  8 * SIZE
   .align 3

.L25:
   andi    I,  M, 2
   bge $r0,    I, .L27
   LD a1,  AO1,  0 * SIZE
   LD x1,  XX,  0 * SIZE
   LD a2,  AO1,  1 * SIZE
   LD x2,  XX,  1 * SIZE
   LD a5,  AO1,  2 * SIZE
   MADD1  y1, a1, x1, y1
   LD x3,  XX,  2 * SIZE
   MADD2  y2, a1, x2, y2
   LD a6,  AO1,  3 * SIZE
   MADD3  y3, a2, x2, y3
   LD x4,  XX,  3 * SIZE
   MADD4  y4, a2, x1, y4
   MADD1  y1, a5, x3, y1
   MADD2  y2, a5, x4, y2
   MADD3  y3, a6, x4, y3
   addi.d  XX,  XX,   4 * SIZE
   MADD4  y4, a6, x3, y4
   addi.d  AO1, AO1,  4 * SIZE
   .align 3

.L27:
   andi    I,  M, 1
.align 3

   bge $r0,    I, .L29
.L28:
   LD a1,  AO1,  0 * SIZE
   LD x1,  XX,  0 * SIZE
   LD a2,  AO1,  1 * SIZE
   LD x2,  XX,  1 * SIZE
   MADD1  y1, a1, x1, y1
   MADD2  y2, a1, x2, y2
   MADD3  y3, a2, x2, y3
   MADD4  y4, a2, x1, y4
   .align 3

.L29:
   LD a1,  Y,  0 * SIZE
   LD a2,  Y,  1 * SIZE
   ADD y1, y1, y3
   ADD y2, y2, y4
   MADD  a1, y1, ALPHA_R, a1
   MADD  a2, y1, ALPHA_I, a2
   NMSUB  a1, y2, ALPHA_I, a1
   MADD  a2, y2, ALPHA_R, a2
   ST a1,  YY,   0 * SIZE
   ST a2,  YY,   1 * SIZE
   .align 3

.L999:
   LDARG  $r23,  $sp,    0
   LDARG  $r24,  $sp,    8
#ifndef __64BIT__
   fld.d  $f18,  $sp,  16
   fld.d  $f19,  $sp,  24
#endif
#ifdef __64BIT__
   addi.d  $sp, $sp, 16
#else
   addi.d  $sp, $sp, 32
#endif
   move $r4, $r17
   fmov.d $f0, $f22
   jirl    $r0, $r1, 0x0

   EPILOGUE
