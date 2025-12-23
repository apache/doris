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
#define X      $r5
#define INCX   $r6
#define Y      $r7
#define INCY   $r8
#define I      $r17
#define TEMP   $r18
#define a1     $f22
#define a2     $f8
#define a3     $f23
#define a4     $f9
#define a5     $f10
#define a6     $f11
#define a7     $f12
#define a8     $f13

   PROLOGUE

#ifdef F_INTERFACE
   LDINT   N,     0(N)
   LDINT   INCX,  0(INCX)
   LDINT   INCY,  0(INCY)
#endif

   li.d  TEMP, 2 * SIZE
   NOP
   slli.d INCX, INCX, ZBASE_SHIFT
   bge $r0,    N, .L999
   slli.d INCY, INCY, ZBASE_SHIFT
   bne INCX, TEMP, .L20
   srai.d I, N, 2
   bne INCY, TEMP, .L20
   addi.d I, I, -1
   blt     I, $r0,     .L15
   LD a1,  X,   0 * SIZE
   LD a2,  X,   1 * SIZE
   LD a3,  X,   2 * SIZE
   LD a4,  X,   3 * SIZE
   LD a5,  X,   4 * SIZE
   LD a6,  X,   5 * SIZE
   LD a7,  X,   6 * SIZE
   LD a8,  X,   7 * SIZE
   bge $r0,    I, .L13
   .align 3

.L12:
   ST a1,  Y,   0 * SIZE
   LD a1,  X,   8 * SIZE
   ST a2,  Y,   1 * SIZE
   LD a2,  X,   9 * SIZE
   ST a3,  Y,   2 * SIZE
   LD a3,  X,  10 * SIZE
   ST a4,  Y,   3 * SIZE
   LD a4,  X,  11 * SIZE
   ST a5,  Y,   4 * SIZE
   LD a5,  X,  12 * SIZE
   ST a6,  Y,   5 * SIZE
   LD a6,  X,  13 * SIZE
   ST a7,  Y,   6 * SIZE
   LD a7,  X,  14 * SIZE
   ST a8,  Y,   7 * SIZE
   LD a8,  X,  15 * SIZE
   addi.d  I, I, -1
   addi.d  X, X, 8 * SIZE
   addi.d Y, Y, 8 * SIZE
   blt $r0,    I, .L12
   .align 3

.L13:
   ST a1,  Y,   0 * SIZE
   ST a2,  Y,   1 * SIZE
   ST a3,  Y,   2 * SIZE
   ST a4,  Y,   3 * SIZE
   ST a5,  Y,   4 * SIZE
   ST a6,  Y,   5 * SIZE
   ST a7,  Y,   6 * SIZE
   ST a8,  Y,   7 * SIZE
   addi.d  X, X, 8 * SIZE
   addi.d  Y, Y, 8 * SIZE
   .align 3

.L15:
   andi    I,  N, 3
   bge $r0,    I, .L999
   .align  3

.L16:
   LD a1,  X,   0 * SIZE
   LD a2,  X,   1 * SIZE
   addi.d  X, X, 2 * SIZE
   addi.d  Y, Y, 2 * SIZE
   ST a1,  Y,  -2 * SIZE
   addi.d  I, I, -1
   ST a2,  Y,  -1 * SIZE
   blt $r0,    I, .L16
   move $r4, $r17
   fmov.d $f0, $f22
   jirl    $r0, $r1, 0x0
   NOP
   .align 3

.L20:
   srai.d  I, N, 2
   addi.d I, I, -1
   blt I,  $r0, .L25
   LD a1,  X,   0 * SIZE
   LD a2,  X,   1 * SIZE
   add.d   X, X, INCX
   LD a3,  X,   0 * SIZE
   LD a4,  X,   1 * SIZE
   add.d   X, X, INCX
   LD a5,  X,   0 * SIZE
   LD a6,  X,   1 * SIZE
   add.d   X, X, INCX
   LD a7,  X,   0 * SIZE
   LD a8,  X,   1 * SIZE
   add.d  X, X, INCX
   bge $r0,    I, .L23
   .align 3

.L22:
   ST a1,  Y,   0 * SIZE
   LD a1,  X,   0 * SIZE
   ST a2,  Y,   1 * SIZE
   add.d   Y, Y, INCY
   LD a2,  X,   1 * SIZE
   add.d   X, X, INCX
   ST a3,  Y,   0 * SIZE
   LD a3,  X,   0 * SIZE
   ST a4,  Y,   1 * SIZE
   add.d   Y, Y, INCY
   LD a4,  X,   1 * SIZE
   add.d   X, X, INCX
   ST a5,  Y,   0 * SIZE
   LD a5,  X,   0 * SIZE
   ST a6,  Y,   1 * SIZE
   add.d   Y, Y, INCY
   LD a6,  X,   1 * SIZE
   add.d   X, X, INCX
   ST a7,  Y,   0 * SIZE
   LD a7,  X,   0 * SIZE
   ST a8,  Y,   1 * SIZE
   add.d   Y, Y, INCY
   LD a8,  X,   1 * SIZE
   addi.d  I, I, -1
   add.d  X, X, INCX
   blt $r0,    I, .L22
   .align 3

.L23:
   ST a1,  Y,   0 * SIZE
   ST a2,  Y,   1 * SIZE
   add.d   Y, Y, INCY
   ST a3,  Y,   0 * SIZE
   ST a4,  Y,   1 * SIZE
   add.d   Y, Y, INCY
   ST a5,  Y,   0 * SIZE
   ST a6,  Y,   1 * SIZE
   add.d   Y, Y, INCY
   ST a7,  Y,   0 * SIZE
   ST a8,  Y,   1 * SIZE
   add.d   Y, Y, INCY
   .align 3

.L25:
   andi    I,  N, 3
   bge $r0,    I, .L999
   .align  3

.L26:
   LD a1,  X,   0 * SIZE
   LD a2,  X,   1 * SIZE
   add.d   X, X, INCX
   addi.d  I, I, -1
   ST a1,  Y,   0 * SIZE
   ST a2,  Y,   1 * SIZE
   add.d  Y, Y, INCY
   blt $r0,    I, .L26
   .align 3

.L999:
   move $r4, $r17
   fmov.d $f0, $f22
   jirl    $r0, $r1, 0x0

   EPILOGUE
