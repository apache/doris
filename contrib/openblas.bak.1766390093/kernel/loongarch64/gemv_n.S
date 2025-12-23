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

/* Unused param dummy1 */
#define M      $r4
#define N      $r5
#define A      $r7
#define LDA    $r8
#define X      $r9
#define INCX   $r10
#define Y      $r11
#define INCY   $r6
#define BUFFER $r16
#define YORIG  $r18
#define XX     $r12
#define YY     $r13
#define I      $r14
#define J      $r15
#define AO1    $r23
#define AO2    $r24
#define ALPHA  $f0
#define a1     $f22
#define a2     $f8
#define a3     $f23
#define a4     $f9
#define a5     $f10
#define a6     $f11
#define a7     $f12
#define a8     $f13
#define x1     $f14
#define x2     $f15
#define y1     $f16
#define y2     $f17
#define y3     $f3
#define y4     $f1
#define y5     $f2
#define y6     $f4
#define y7     $f5
#define y8     $f6
#define t1     $f7
#define t2     $f18
#define t3     $f19
#define t4     $f20

   PROLOGUE

   LDARG  INCY,    $sp,  0
   LDARG  BUFFER,  $sp,  8
#ifdef __64BIT__
   addi.d  $sp, $sp, -16
#else
   addi.d  $sp, $sp, -48
#endif
   SDARG  $r23,  $sp,    0
   SDARG  $r24,  $sp,    8
   slli.d     LDA,     LDA,  BASE_SHIFT
#ifndef __64BIT__
   fst.d  $f18,  $sp,  16
   fst.d  $f19,  $sp,  24
   fst.d  $f20,  $sp,  32
#endif
   slli.d INCX, INCX, BASE_SHIFT
   bge $r0,    M, .L999
   slli.d INCY, INCY, BASE_SHIFT
   bge $r0,    N, .L999
   li.d  I, SIZE
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
   add.d   XX, XX, INCY
   LD a2,  XX,  0 * SIZE
   add.d   XX, XX, INCY
   LD a3,  XX,  0 * SIZE
   add.d   XX, XX, INCY
   LD a4,  XX,  0 * SIZE
   add.d   XX, XX, INCY
   ST a1,  YY,  0 * SIZE
   ST a2,  YY,  1 * SIZE
   ST a3,  YY,  2 * SIZE
   ST a4,  YY,  3 * SIZE
   addi.d  I, I, -1
   addi.d YY, YY, 4 * SIZE
   blt $r0,    I, .L02
   .align 3

.L05:
   andi    I,  M, 3
   bge $r0,    I, .L10
   .align 3

.L06:
   LD a1,  XX,  0 * SIZE
   add.d   XX, XX, INCY
   ST a1,  YY,  0 * SIZE
   addi.d  I, I, -1
   addi.d YY, YY, 1 * SIZE
   blt $r0,    I, .L06
   .align 3

.L10:
   srai.d  J,  N, 1
   bge $r0,    J, .L20
   .align 3

.L11:
   LD x1,  X,  0 * SIZE
   add.d   X, X, INCX
   LD x2,  X,  0 * SIZE
   add.d   X, X, INCX
   move    AO1, A
   add.d   AO2, A,      LDA
   add.d   A,   AO2,    LDA
   move    YY, YORIG
   MUL x1, ALPHA, x1
   srai.d  I,  M, 3
   MUL    x2, ALPHA, x2
   bge $r0,    I, .L15
   LD a1,  AO1,  0 * SIZE
   LD y1,  YY,  0 * SIZE
   LD a2,  AO1,  1 * SIZE
   LD y2,  YY,  1 * SIZE
   LD a3,  AO1,  2 * SIZE
   LD y3,  YY,  2 * SIZE
   LD a4,  AO1,  3 * SIZE
   LD y4,  YY,  3 * SIZE
   LD a5,  AO2,  0 * SIZE
   LD y5,  YY,  4 * SIZE
   LD a6,  AO2,  1 * SIZE
   LD y6,  YY,  5 * SIZE
   LD a7,  AO2,  2 * SIZE
   LD y7,  YY,  6 * SIZE
   LD a8,  AO2,  3 * SIZE
   addi.d  I, I, -1
   LD y8,  YY,  7 * SIZE
   bge $r0,    I, .L13
   .align  3
.L12:
   MADD  t1, a1, x1, y1
   LD a1,  AO1,   4 * SIZE
   MADD  t2, a2, x1, y2
   LD a2,  AO1,   5 * SIZE
   LD y1,  YY,   8 * SIZE
   LD y2,  YY,   9 * SIZE
   MADD  t3, a3, x1, y3
   LD a3,  AO1,   6 * SIZE
   MADD  t4, a4, x1, y4
   LD a4,  AO1,   7 * SIZE
   LD y3,  YY,  10 * SIZE
   LD y4,  YY,  11 * SIZE
   MADD  t1, a5, x2, t1
   LD a5,  AO2,   4 * SIZE
   MADD  t2, a6, x2, t2
   LD a6,  AO2,   5 * SIZE
   MADD  t3, a7, x2, t3
   LD a7,  AO2,   6 * SIZE
   MADD  t4, a8, x2, t4
   LD a8,  AO2,   7 * SIZE
   ST t1,  YY,   0 * SIZE
   ST t2,  YY,   1 * SIZE
   ST t3,  YY,   2 * SIZE
   ST t4,  YY,   3 * SIZE
   MADD  t1, a1, x1, y5
   LD a1,  AO1,   8 * SIZE
   MADD  t2, a2, x1, y6
   LD a2,  AO1,   9 * SIZE
   LD y5,  YY,  12 * SIZE
   LD y6,  YY,  13 * SIZE
   MADD  t3, a3, x1, y7
   LD a3,  AO1,  10 * SIZE
   MADD  t4, a4, x1, y8
   LD a4,  AO1,  11 * SIZE
   LD y7,  YY,  14 * SIZE
   LD y8,  YY,  15 * SIZE
   MADD  t1, a5, x2, t1
   LD a5,  AO2,   8 * SIZE
   MADD  t2, a6, x2, t2
   LD a6,  AO2,   9 * SIZE
   MADD  t3, a7, x2, t3
   LD a7,  AO2,  10 * SIZE
   MADD  t4, a8, x2, t4
   LD a8,  AO2,  11 * SIZE
   ST t1,  YY,  4 * SIZE
   ST t2,  YY,  5 * SIZE
   ST t3,  YY,  6 * SIZE
   ST t4,  YY,  7 * SIZE
   addi.d  I, I, -1
   addi.d  YY,  YY,   8 * SIZE
   addi.d  AO1, AO1,  8 * SIZE
   addi.d AO2, AO2,  8 * SIZE
   blt $r0,    I, .L12
   .align 3

.L13:
   MADD  t1, a1, x1, y1
   LD a1,  AO1,   4 * SIZE
   MADD  t2, a2, x1, y2
   LD a2,  AO1,   5 * SIZE
   MADD  t3, a3, x1, y3
   LD a3,  AO1,   6 * SIZE
   MADD  t4, a4, x1, y4
   LD a4,  AO1,   7 * SIZE
   MADD  t1, a5, x2, t1
   LD a5,  AO2,   4 * SIZE
   MADD  t2, a6, x2, t2
   LD a6,  AO2,   5 * SIZE
   MADD  t3, a7, x2, t3
   LD a7,  AO2,   6 * SIZE
   MADD  t4, a8, x2, t4
   LD a8,  AO2,   7 * SIZE
   ST t1,  YY,   0 * SIZE
   MADD  t1, a1, x1, y5
   ST t2,  YY,   1 * SIZE
   MADD  t2, a2, x1, y6
   ST t3,  YY,   2 * SIZE
   MADD  t3, a3, x1, y7
   ST t4,  YY,   3 * SIZE
   MADD  t4, a4, x1, y8
   MADD  t1, a5, x2, t1
   addi.d  AO1, AO1,  8 * SIZE
   MADD  t2, a6, x2, t2
   addi.d  AO2, AO2,  8 * SIZE
   MADD  t3, a7, x2, t3
   addi.d  YY,  YY,   8 * SIZE
   MADD  t4, a8, x2, t4
   ST t1,  YY,  -4 * SIZE
   ST t2,  YY,  -3 * SIZE
   ST t3,  YY,  -2 * SIZE
   ST t4,  YY,  -1 * SIZE
   .align 3

.L15:
   andi    I,  M, 4
   bge $r0,    I, .L16
   LD a1,  AO1,  0 * SIZE
   LD y1,  YY,  0 * SIZE
   LD a2,  AO1,  1 * SIZE
   LD y2,  YY,  1 * SIZE
   LD a3,  AO1,  2 * SIZE
   LD y3,  YY,  2 * SIZE
   LD a4,  AO1,  3 * SIZE
   LD y4,  YY,  3 * SIZE
   LD a5,  AO2,  0 * SIZE
   MADD  y1, a1, x1, y1
   LD a6,  AO2,  1 * SIZE
   MADD  y2, a2, x1, y2
   LD a7,  AO2,  2 * SIZE
   MADD  y3, a3, x1, y3
   LD a8,  AO2,  3 * SIZE
   MADD  y4, a4, x1, y4
   MADD  y1, a5, x2, y1
   addi.d  YY,  YY,   4 * SIZE
   MADD  y2, a6, x2, y2
   addi.d  AO1, AO1,  4 * SIZE
   MADD  y3, a7, x2, y3
   addi.d  AO2, AO2,  4 * SIZE
   MADD  y4, a8, x2, y4
   ST y1,  YY,  -4 * SIZE
   ST y2,  YY,  -3 * SIZE
   ST y3,  YY,  -2 * SIZE
   ST y4,  YY,  -1 * SIZE
   .align 3

.L16:
   andi    I,  M, 2
   bge $r0,    I, .L17
   LD a1,  AO1,  0 * SIZE
   LD y1,  YY,  0 * SIZE
   LD a2,  AO1,  1 * SIZE
   LD y2,  YY,  1 * SIZE
   LD a5,  AO2,  0 * SIZE
   LD a6,  AO2,  1 * SIZE
   MADD  y1, a1, x1, y1
   MADD  y2, a2, x1, y2
   addi.d  YY,  YY,   2 * SIZE
   MADD  y1, a5, x2, y1
   addi.d  AO1, AO1,  2 * SIZE
   MADD  y2, a6, x2, y2
   addi.d  AO2, AO2,  2 * SIZE
   ST y1,  YY,  -2 * SIZE
   ST y2,  YY,  -1 * SIZE
   .align 3

.L17:
   andi    I,  M, 1
   bge $r0,    I, .L19
   LD y1,  YY,  0 * SIZE
   LD a1,  AO1,  0 * SIZE
   LD a5,  AO2,  0 * SIZE
   MADD  y1, a1, x1, y1
   MADD  y1, a5, x2, y1
   ST y1,  YY,  0 * SIZE
   .align 3

.L19:
   addi.d  J, J, -1
   blt $r0,    J, .L11
   .align 3

.L20:
   andi    J,  N, 1
   bge $r0,    J, .L900
   .align 3

.L21:
   LD x1,  X,  0 * SIZE
   add.d   X, X, INCX
   move    YY, YORIG
   move    AO1, A
   srai.d  I,  M, 3
   MUL    x1, ALPHA, x1
   bge $r0,    I, .L25
   LD a1,  AO1,  0 * SIZE
   LD y1,  YY,  0 * SIZE
   LD a2,  AO1,  1 * SIZE
   LD y2,  YY,  1 * SIZE
   LD a3,  AO1,  2 * SIZE
   LD y3,  YY,  2 * SIZE
   LD a4,  AO1,  3 * SIZE
   LD y4,  YY,  3 * SIZE
   LD y5,  YY,  4 * SIZE
   LD y6,  YY,  5 * SIZE
   LD y7,  YY,  6 * SIZE
   addi.d  I, I, -1
   LD y8,  YY,  7 * SIZE
   bge $r0,    I, .L23
   .align  3
.L22:
   MADD  t1, a1, x1, y1
   LD a1,  AO1,   4 * SIZE
   MADD  t2, a2, x1, y2
   LD a2,  AO1,   5 * SIZE
   LD y1,  YY,   8 * SIZE
   LD y2,  YY,   9 * SIZE
   MADD  t3, a3, x1, y3
   LD a3,  AO1,   6 * SIZE
   MADD  t4, a4, x1, y4
   LD a4,  AO1,   7 * SIZE
   LD y3,  YY,  10 * SIZE
   LD y4,  YY,  11 * SIZE
   ST t1,  YY,   0 * SIZE
   ST t2,  YY,   1 * SIZE
   ST t3,  YY,   2 * SIZE
   ST t4,  YY,   3 * SIZE
   MADD  t1, a1, x1, y5
   LD a1,  AO1,   8 * SIZE
   MADD  t2, a2, x1, y6
   LD a2,  AO1,   9 * SIZE
   LD y5,  YY,  12 * SIZE
   LD y6,  YY,  13 * SIZE
   MADD  t3, a3, x1, y7
   LD a3,  AO1,  10 * SIZE
   MADD  t4, a4, x1, y8
   LD a4,  AO1,  11 * SIZE
   LD y7,  YY,  14 * SIZE
   LD y8,  YY,  15 * SIZE
   ST t1,  YY,  4 * SIZE
   ST t2,  YY,  5 * SIZE
   ST t3,  YY,  6 * SIZE
   ST t4,  YY,  7 * SIZE
   addi.d  I, I, -1
   addi.d  YY,  YY,   8 * SIZE
   addi.d AO1, AO1,  8 * SIZE
   blt $r0,    I, .L22
   .align 3

.L23:
   MADD  t1, a1, x1, y1
   LD a1,  AO1,   4 * SIZE
   MADD  t2, a2, x1, y2
   LD a2,  AO1,   5 * SIZE
   MADD  t3, a3, x1, y3
   LD a3,  AO1,   6 * SIZE
   MADD  t4, a4, x1, y4
   LD a4,  AO1,   7 * SIZE
   ST t1,  YY,   0 * SIZE
   MADD  t1, a1, x1, y5
   ST t2,  YY,   1 * SIZE
   MADD  t2, a2, x1, y6
   ST t3,  YY,   2 * SIZE
   MADD  t3, a3, x1, y7
   ST t4,  YY,   3 * SIZE
   MADD  t4, a4, x1, y8
   ST t1,  YY,   4 * SIZE
   ST t2,  YY,   5 * SIZE
   ST t3,  YY,   6 * SIZE
   ST t4,  YY,   7 * SIZE
   addi.d  AO1, AO1,  8 * SIZE
   addi.d  YY,  YY,   8 * SIZE
   .align 3

.L25:
   andi    I,  M, 4
   bge $r0,    I, .L26
   LD a1,  AO1,  0 * SIZE
   LD y1,  YY,  0 * SIZE
   LD a2,  AO1,  1 * SIZE
   LD y2,  YY,  1 * SIZE
   LD a3,  AO1,  2 * SIZE
   LD y3,  YY,  2 * SIZE
   LD a4,  AO1,  3 * SIZE
   LD y4,  YY,  3 * SIZE
   MADD  y1, a1, x1, y1
   MADD  y2, a2, x1, y2
   MADD  y3, a3, x1, y3
   addi.d  YY,  YY,   4 * SIZE
   MADD  y4, a4, x1, y4
   addi.d  AO1, AO1,  4 * SIZE
   ST y1,  YY,  -4 * SIZE
   ST y2,  YY,  -3 * SIZE
   ST y3,  YY,  -2 * SIZE
   ST y4,  YY,  -1 * SIZE
   .align 3

.L26:
   andi    I,  M, 2
   bge $r0,    I, .L27
   LD a1,  AO1,  0 * SIZE
   LD y1,  YY,  0 * SIZE
   LD a2,  AO1,  1 * SIZE
   LD y2,  YY,  1 * SIZE
   MADD  y1, a1, x1, y1
   addi.d  YY,  YY,   2 * SIZE
   MADD  y2, a2, x1, y2
   addi.d  AO1, AO1,  2 * SIZE
   ST y1,  YY,  -2 * SIZE
   ST y2,  YY,  -1 * SIZE
   .align 3

.L27:
   andi    I,  M, 1
   bge $r0,    I, .L900
   LD y1,  YY,  0 * SIZE
   LD a1,  AO1,  0 * SIZE
   MADD  y1, a1, x1, y1
   ST y1,  YY,  0 * SIZE
   .align 3

.L900:
   li.d  YORIG, SIZE
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
   ST a1,  Y,  0 * SIZE
   add.d   Y, Y, INCY
   ST a2,  Y,  0 * SIZE
   add.d   Y, Y, INCY
   ST a3,  Y,  0 * SIZE
   add.d   Y, Y, INCY
   ST a4,  Y,  0 * SIZE
   add.d   Y, Y, INCY
   addi.d  I, I, -1
   addi.d XX, XX, 4 * SIZE
   blt $r0,    I, .L902
   .align 3

.L905:
   andi    I,  M, 3
   bge $r0,    I, .L999
   .align 3

.L906:
   LD a1,  XX,  0 * SIZE
   addi.d  XX, XX, 1 * SIZE
   ST a1,  Y,  0 * SIZE
   addi.d  I, I, -1
   add.d  Y, Y, INCY
   blt $r0,    I, .L906
   .align 3

.L999:
   LDARG  $r23,  $sp,    0
   LDARG  $r24,  $sp,    8
#ifndef __64BIT__
   fld.d  $f18,  $sp,  16
   fld.d  $f19,  $sp,  24
   fld.d  $f20,  $sp,  32
#endif
#ifdef __64BIT__
   addi.d  $sp, $sp, 16
#else
   addi.d  $sp, $sp, 48
#endif
   move $r4, $r17
   fmov.d $f0, $f22
   jirl    $r0, $r1, 0x0

   EPILOGUE
