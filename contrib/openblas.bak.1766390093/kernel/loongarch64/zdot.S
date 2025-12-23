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

#define N      $r4
#define X      $r5
#define INCX   $r6
#define Y      $r7
#define INCY   $r8
#define I      $r17
#define TEMP   $r18
#define a1     $f10
#define a2     $f11
#define a3     $f12
#define a4     $f13
#define b1     $f14
#define b2     $f15
#define b3     $f16
#define b4     $f17
#define s1     $f22
#define s2     $f8
#define s3     $f23
#define s4     $f9

   PROLOGUE

#ifdef F_INTERFACE
   LDINT   N,     0(N)
   LDINT   INCX,  0(INCX)
   LDINT   INCY,  0(INCY)
#endif

   MTC  s1, $r0
   MOV s2, s1
   MOV s3, s2
   MOV s4, s3
   slli.d  INCX, INCX, ZBASE_SHIFT
   li.d  TEMP, 2 * SIZE
   slli.d INCY, INCY, ZBASE_SHIFT
   bge $r0,    N, .L999
   srai.d I, N, 2
   bne INCX, TEMP, .L20
   bne INCY, TEMP, .L20
   bge $r0,    I, .L15
   LD a1,  X,   0 * SIZE
   LD a2,  X,   1 * SIZE
   LD b1,  Y,   0 * SIZE
   addi.d  I, I, -1
   LD b2,  Y,   1 * SIZE
   bge $r0,    I, .L14
   .align 3

.L13:
   MADD  s1, b1, a1, s1
   LD a3,  X,   2 * SIZE
   MADD  s2, b1, a2, s2
   LD a4,  X,   3 * SIZE
   MADD  s3, b2, a1, s3
   LD b3,  Y,   2 * SIZE
   MADD  s4, b2, a2, s4
   LD b4,  Y,   3 * SIZE
   MADD  s1, b3, a3, s1
   LD a1,  X,   4 * SIZE
   MADD  s2, b3, a4, s2
   LD a2,  X,   5 * SIZE
   MADD  s3, b4, a3, s3
   LD b1,  Y,   4 * SIZE
   MADD  s4, b4, a4, s4
   LD b2,  Y,   5 * SIZE
   MADD  s1, b1, a1, s1
   LD a3,  X,   6 * SIZE
   MADD  s2, b1, a2, s2
   LD a4,  X,   7 * SIZE
   MADD  s3, b2, a1, s3
   LD b3,  Y,   6 * SIZE
   MADD  s4, b2, a2, s4
   LD b4,  Y,   7 * SIZE
   MADD  s1, b3, a3, s1
   LD a1,  X,   8 * SIZE
   MADD  s2, b3, a4, s2
   LD a2,  X,   9 * SIZE
   MADD  s3, b4, a3, s3
   LD b1,  Y,   8 * SIZE
   MADD  s4, b4, a4, s4
   LD b2,  Y,   9 * SIZE
   addi.d  I, I, -1
   addi.d  X, X, 8 * SIZE
   addi.d Y, Y, 8 * SIZE
   blt $r0,    I, .L13
   .align 3

.L14:
   MADD  s1, b1, a1, s1
   LD a3,  X,   2 * SIZE
   MADD  s2, b1, a2, s2
   LD a4,  X,   3 * SIZE
   MADD  s3, b2, a1, s3
   LD b3,  Y,   2 * SIZE
   MADD  s4, b2, a2, s4
   LD b4,  Y,   3 * SIZE
   MADD  s1, b3, a3, s1
   LD a1,  X,   4 * SIZE
   MADD  s2, b3, a4, s2
   LD a2,  X,   5 * SIZE
   MADD  s3, b4, a3, s3
   LD b1,  Y,   4 * SIZE
   MADD  s4, b4, a4, s4
   LD b2,  Y,   5 * SIZE
   MADD  s1, b1, a1, s1
   LD a3,  X,   6 * SIZE
   MADD  s2, b1, a2, s2
   LD a4,  X,   7 * SIZE
   MADD  s3, b2, a1, s3
   LD b3,  Y,   6 * SIZE
   MADD  s4, b2, a2, s4
   LD b4,  Y,   7 * SIZE
   MADD  s1, b3, a3, s1
   addi.d  X, X, 8 * SIZE
   MADD  s2, b3, a4, s2
   addi.d  Y, Y, 8 * SIZE
   MADD  s3, b4, a3, s3
   MADD  s4, b4, a4, s4
   .align 3

.L15:
   andi    I,  N, 3
   bge $r0,    I, .L999
   LD a1,  X,   0 * SIZE
   LD a2,  X,   1 * SIZE
   LD b1,  Y,   0 * SIZE
   addi.d  I, I, -1
   LD b2,  Y,   1 * SIZE
   bge $r0,    I, .L17
   .align 3

.L16:
   MADD  s1, b1, a1, s1
   addi.d  I, I, -1
   MADD  s2, b1, a2, s2
   LD b1,  Y,   2 * SIZE
   MADD  s3, b2, a1, s3
   LD a1,  X,   2 * SIZE
   MADD  s4, b2, a2, s4
   LD a2,  X,   3 * SIZE
   LD b2,  Y,   3 * SIZE
   addi.d  X, X, 2 * SIZE
   addi.d Y, Y, 2 * SIZE
   blt $r0,    I, .L16
   .align 3

.L17:
   MADD  s1, b1, a1, s1
   MADD  s2, b1, a2, s2
   MADD  s3, b2, a1, s3
   MADD  s4, b2, a2, s4
   b   .L999
   .align 3

.L20:
#ifdef F_INTERFACE
   bgez    INCX, .L21
   addi.d  TEMP, N, -1
   mult    TEMP, INCX
   mflo    TEMP
   dsub    X, X, TEMP
   .align 3

.L21:
   bgez    INCY, .L22
   addi.d  TEMP, N, -1
   mult    TEMP, INCY
   mflo    TEMP
   dsub    Y, Y, TEMP
   .align 3

.L22:
#endif
   bge $r0,    I, .L25
   LD a1,  X,   0 * SIZE
   LD a2,  X,   1 * SIZE
   LD b1,  Y,   0 * SIZE
   LD b2,  Y,   1 * SIZE
   add.d   X, X, INCX
   addi.d  I, I, -1
   add.d  Y, Y, INCY
   bge $r0,    I, .L24
   .align 3

.L23:
   MADD  s1, b1, a1, s1
   LD a3,  X,   0 * SIZE
   MADD  s2, b1, a2, s2
   LD a4,  X,   1 * SIZE
   MADD  s3, b2, a1, s3
   LD b3,  Y,   0 * SIZE
   MADD  s4, b2, a2, s4
   LD b4,  Y,   1 * SIZE
   add.d   X, X, INCX
   add.d   Y, Y, INCY
   MADD  s1, b3, a3, s1
   LD a1,  X,   0 * SIZE
   MADD  s2, b3, a4, s2
   LD a2,  X,   1 * SIZE
   MADD  s3, b4, a3, s3
   LD b1,  Y,   0 * SIZE
   MADD  s4, b4, a4, s4
   LD b2,  Y,   1 * SIZE
   add.d   X, X, INCX
   add.d   Y, Y, INCY
   MADD  s1, b1, a1, s1
   LD a3,  X,   0 * SIZE
   MADD  s2, b1, a2, s2
   LD a4,  X,   1 * SIZE
   MADD  s3, b2, a1, s3
   LD b3,  Y,   0 * SIZE
   MADD  s4, b2, a2, s4
   LD b4,  Y,   1 * SIZE
   add.d   X, X, INCX
   add.d   Y, Y, INCY
   MADD  s1, b3, a3, s1
   LD a1,  X,   0 * SIZE
   MADD  s2, b3, a4, s2
   LD a2,  X,   1 * SIZE
   MADD  s3, b4, a3, s3
   LD b1,  Y,   0 * SIZE
   MADD  s4, b4, a4, s4
   LD b2,  Y,   1 * SIZE
   add.d   X, X, INCX
   addi.d  I, I, -1
   add.d  Y, Y, INCY
   blt $r0,    I, .L23
   .align 3

.L24:
   MADD  s1, b1, a1, s1
   LD a3,  X,   0 * SIZE
   MADD  s2, b1, a2, s2
   LD a4,  X,   1 * SIZE
   MADD  s3, b2, a1, s3
   LD b3,  Y,   0 * SIZE
   MADD  s4, b2, a2, s4
   LD b4,  Y,   1 * SIZE
   add.d   X, X, INCX
   add.d   Y, Y, INCY
   MADD  s1, b3, a3, s1
   LD a1,  X,   0 * SIZE
   MADD  s2, b3, a4, s2
   LD a2,  X,   1 * SIZE
   MADD  s3, b4, a3, s3
   LD b1,  Y,   0 * SIZE
   MADD  s4, b4, a4, s4
   LD b2,  Y,   1 * SIZE
   add.d   X, X, INCX
   add.d   Y, Y, INCY
   MADD  s1, b1, a1, s1
   LD a3,  X,   0 * SIZE
   MADD  s2, b1, a2, s2
   LD a4,  X,   1 * SIZE
   MADD  s3, b2, a1, s3
   LD b3,  Y,   0 * SIZE
   MADD  s4, b2, a2, s4
   LD b4,  Y,   1 * SIZE
   MADD  s1, b3, a3, s1
   add.d   X, X, INCX
   MADD  s2, b3, a4, s2
   add.d   Y, Y, INCY
   MADD  s3, b4, a3, s3
   MADD  s4, b4, a4, s4
   .align 3

.L25:
   andi    I,  N, 3
   bge $r0,    I, .L999
   .align  3
.L26:
   LD a1,  X,   0 * SIZE
   LD a2,  X,   1 * SIZE
   LD b1,  Y,   0 * SIZE
   LD b2,  Y,   1 * SIZE
   MADD  s1, b1, a1, s1
   MADD  s2, b1, a2, s2
   MADD  s3, b2, a1, s3
   MADD  s4, b2, a2, s4
   add.d   X, X, INCX
   add.d   Y, Y, INCY
   addi.d  I, I, -1
   blt $r0,    I, .L26
   .align 3

.L999:
#ifndef CONJ
   SUB $f0, s1, s4
#else
   ADD $f0, s1, s4
#endif
#ifndef CONJ
   ADD $f1, s3, s2
#else
   SUB $f1, s3, s2
#endif
   jirl    $r0, $r1, 0x0

   EPILOGUE
