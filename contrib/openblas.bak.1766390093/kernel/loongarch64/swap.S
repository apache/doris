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

#define N      $r4
#define X      $r7
#define INCX   $r8
#define Y      $r9
#define INCY   $r10

#define I      $r17
#define TEMP   $r18
#define XX     $r5
#define YY     $r6
#define a1     $f22
#define a2     $f8
#define a3     $f23
#define a4     $f9
#define a5     $f10
#define a6     $f11
#define a7     $f12
#define a8     $f13
#define b1     $f14
#define b2     $f15
#define b3     $f16
#define b4     $f17
#define b5     $f0
#define b6     $f1
#define b7     $f2
#define b8     $f3

   PROLOGUE

   li.d  TEMP, SIZE
   slli.d INCX, INCX, BASE_SHIFT
   bge $r0,    N, .L999
   slli.d INCY, INCY, BASE_SHIFT
   bne INCX, TEMP, .L20
   srai.d I, N, 3
   bne INCY, TEMP, .L20
   addi.d I, I, -1
   blt I,  $r0, .L15
   LD a1,  X,   0 * SIZE
   LD b1,  Y,   0 * SIZE
   LD a2,  X,   1 * SIZE
   LD b2,  Y,   1 * SIZE
   LD a3,  X,   2 * SIZE
   LD b3,  Y,   2 * SIZE
   LD a4,  X,   3 * SIZE
   LD b4,  Y,   3 * SIZE
   LD a5,  X,   4 * SIZE
   LD b5,  Y,   4 * SIZE
   LD a6,  X,   5 * SIZE
   LD b6,  Y,   5 * SIZE
   LD a7,  X,   6 * SIZE
   LD b7,  Y,   6 * SIZE
   LD a8,  X,   7 * SIZE
   LD b8,  Y,   7 * SIZE
   bge $r0,    I, .L13
   .align 3

.L12:
   ST a1,  Y,   0 * SIZE
   LD a1,  X,   8 * SIZE
   ST b1,  X,   0 * SIZE
   LD b1,  Y,   8 * SIZE
   ST a2,  Y,   1 * SIZE
   LD a2,  X,   9 * SIZE
   ST b2,  X,   1 * SIZE
   LD b2,  Y,   9 * SIZE
   ST a3,  Y,   2 * SIZE
   LD a3,  X,  10 * SIZE
   ST b3,  X,   2 * SIZE
   LD b3,  Y,  10 * SIZE
   ST a4,  Y,   3 * SIZE
   LD a4,  X,  11 * SIZE
   ST b4,  X,   3 * SIZE
   LD b4,  Y,  11 * SIZE
   ST a5,  Y,   4 * SIZE
   LD a5,  X,  12 * SIZE
   ST b5,  X,   4 * SIZE
   LD b5,  Y,  12 * SIZE
   ST a6,  Y,   5 * SIZE
   LD a6,  X,  13 * SIZE
   ST b6,  X,   5 * SIZE
   LD b6,  Y,  13 * SIZE
   ST a7,  Y,   6 * SIZE
   LD a7,  X,  14 * SIZE
   ST b7,  X,   6 * SIZE
   LD b7,  Y,  14 * SIZE
   ST a8,  Y,   7 * SIZE
   LD a8,  X,  15 * SIZE
   ST b8,  X,   7 * SIZE
   LD b8,  Y,  15 * SIZE
   addi.d  I, I, -1
   addi.d  X, X, 8 * SIZE
   addi.d Y, Y, 8 * SIZE
   blt $r0,    I, .L12
   .align 3

.L13:
   ST a1,  Y,   0 * SIZE
   ST b1,  X,   0 * SIZE
   ST a2,  Y,   1 * SIZE
   ST b2,  X,   1 * SIZE
   ST a3,  Y,   2 * SIZE
   ST b3,  X,   2 * SIZE
   ST a4,  Y,   3 * SIZE
   ST b4,  X,   3 * SIZE
   ST a5,  Y,   4 * SIZE
   ST b5,  X,   4 * SIZE
   ST a6,  Y,   5 * SIZE
   ST b6,  X,   5 * SIZE
   ST a7,  Y,   6 * SIZE
   ST b7,  X,   6 * SIZE
   ST a8,  Y,   7 * SIZE
   ST b8,  X,   7 * SIZE
   addi.d  X, X, 8 * SIZE
   addi.d  Y, Y, 8 * SIZE
   .align 3

.L15:
   andi    I,  N, 7
   bge $r0,    I, .L999
   .align  3
.L16:
   LD a1,  X,   0 * SIZE
   LD b1,  Y,   0 * SIZE
   addi.d  X, X, SIZE
   addi.d  I, I, -1
   addi.d  Y, Y, SIZE
   ST b1,  X,  -1 * SIZE
   ST a1,  Y,  -1 * SIZE
   blt $r0,    I, .L16
   b   .L999
   .align 3

.L20:
   srai.d  I, N, 3
   move    XX, X
   move    YY, Y
   addi.d I, I, -1
   blt I,  $r0, .L25
   LD a1,  X,   0 * SIZE
   add.d   X, X, INCX
   LD b1,  Y,   0 * SIZE
   add.d   Y, Y, INCY
   LD a2,  X,   0 * SIZE
   add.d   X, X, INCX
   LD b2,  Y,   0 * SIZE
   add.d   Y, Y, INCY
   LD a3,  X,   0 * SIZE
   add.d   X, X, INCX
   LD b3,  Y,   0 * SIZE
   add.d   Y, Y, INCY
   LD a4,  X,   0 * SIZE
   add.d   X, X, INCX
   LD b4,  Y,   0 * SIZE
   add.d   Y, Y, INCY
   LD a5,  X,   0 * SIZE
   add.d   X, X, INCX
   LD b5,  Y,   0 * SIZE
   add.d   Y, Y, INCY
   LD a6,  X,   0 * SIZE
   add.d   X, X, INCX
   LD b6,  Y,   0 * SIZE
   add.d   Y, Y, INCY
   LD a7,  X,   0 * SIZE
   add.d   X, X, INCX
   LD b7,  Y,   0 * SIZE
   add.d   Y, Y, INCY
   LD a8,  X,   0 * SIZE
   add.d   X, X, INCX
   LD b8,  Y,   0 * SIZE
   add.d   Y, Y, INCY
   bge $r0,    I, .L23
   .align 3

.L22:
   ST a1,  YY,   0 * SIZE
   add.d   YY, YY, INCY
   LD a1,  X,   0 * SIZE
   add.d   X, X, INCX
   ST b1,  XX,   0 * SIZE
   add.d   XX, XX, INCX
   LD b1,  Y,   0 * SIZE
   add.d   Y, Y, INCY
   ST a2,  YY,   0 * SIZE
   add.d   YY, YY, INCY
   LD a2,  X,   0 * SIZE
   add.d   X, X, INCX
   ST b2,  XX,   0 * SIZE
   add.d   XX, XX, INCX
   LD b2,  Y,   0 * SIZE
   add.d   Y, Y, INCY
   ST a3,  YY,   0 * SIZE
   add.d   YY, YY, INCY
   LD a3,  X,   0 * SIZE
   add.d   X, X, INCX
   ST b3,  XX,   0 * SIZE
   add.d   XX, XX, INCX
   LD b3,  Y,   0 * SIZE
   add.d   Y, Y, INCY
   ST a4,  YY,   0 * SIZE
   add.d   YY, YY, INCY
   LD a4,  X,   0 * SIZE
   add.d   X, X, INCX
   ST b4,  XX,   0 * SIZE
   add.d   XX, XX, INCX
   LD b4,  Y,   0 * SIZE
   add.d   Y, Y, INCY
   ST a5,  YY,   0 * SIZE
   add.d   YY, YY, INCY
   LD a5,  X,   0 * SIZE
   add.d   X, X, INCX
   ST b5,  XX,   0 * SIZE
   add.d   XX, XX, INCX
   LD b5,  Y,   0 * SIZE
   add.d   Y, Y, INCY
   ST a6,  YY,   0 * SIZE
   add.d   YY, YY, INCY
   LD a6,  X,   0 * SIZE
   add.d   X, X, INCX
   ST b6,  XX,   0 * SIZE
   add.d   XX, XX, INCX
   LD b6,  Y,   0 * SIZE
   add.d   Y, Y, INCY
   ST a7,  YY,   0 * SIZE
   add.d   YY, YY, INCY
   LD a7,  X,   0 * SIZE
   add.d   X, X, INCX
   ST b7,  XX,   0 * SIZE
   add.d   XX, XX, INCX
   LD b7,  Y,   0 * SIZE
   add.d   Y, Y, INCY
   ST a8,  YY,   0 * SIZE
   add.d   YY, YY, INCY
   LD a8,  X,   0 * SIZE
   add.d   X, X, INCX
   ST b8,  XX,   0 * SIZE
   add.d   XX, XX, INCX
   LD b8,  Y,   0 * SIZE
   addi.d  I, I, -1
   add.d  Y, Y, INCY
   blt $r0,    I, .L22
   .align 3

.L23:
   ST a1,  YY,   0 * SIZE
   add.d   YY, YY, INCY
   ST b1,  XX,   0 * SIZE
   add.d   XX, XX, INCX
   ST a2,  YY,   0 * SIZE
   add.d   YY, YY, INCY
   ST b2,  XX,   0 * SIZE
   add.d   XX, XX, INCX
   ST a3,  YY,   0 * SIZE
   add.d   YY, YY, INCY
   ST b3,  XX,   0 * SIZE
   add.d   XX, XX, INCX
   ST a4,  YY,   0 * SIZE
   add.d   YY, YY, INCY
   ST b4,  XX,   0 * SIZE
   add.d   XX, XX, INCX
   ST a5,  YY,   0 * SIZE
   add.d   YY, YY, INCY
   ST b5,  XX,   0 * SIZE
   add.d   XX, XX, INCX
   ST a6,  YY,   0 * SIZE
   add.d   YY, YY, INCY
   ST b6,  XX,   0 * SIZE
   add.d   XX, XX, INCX
   ST a7,  YY,   0 * SIZE
   add.d   YY, YY, INCY
   ST b7,  XX,   0 * SIZE
   add.d   XX, XX, INCX
   ST a8,  YY,   0 * SIZE
   add.d   YY, YY, INCY
   ST b8,  XX,   0 * SIZE
   add.d   XX, XX, INCX
   .align 3

.L25:
   andi    I,  N, 7
   bge $r0,    I, .L999
   .align  3
.L26:
   LD a1,  X,   0 * SIZE
   LD b1,  Y,   0 * SIZE
   addi.d  I, I, -1
   ST a1,  Y,   0 * SIZE
   ST b1,  X,   0 * SIZE
   add.d   X, X, INCX
   add.d  Y, Y, INCY
   blt $r0,    I, .L26
   .align 3

.L999:
   move $r4, $r17
   fmov.d $f0, $f22
   jirl    $r0, $r1, 0x0

   EPILOGUE
