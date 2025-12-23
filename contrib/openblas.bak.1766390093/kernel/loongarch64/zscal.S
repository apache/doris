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

#define N          $r4
#define X          $r7
#define INCX       $r8
#define I          $r17
#define TEMP       $r18
#define XX         $r5
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
#define t1         $f14
#define t2         $f15
#define t3         $f16
#define t4         $f17

   PROLOGUE

   li.d  TEMP, 2 * SIZE
   ld.d XX, $sp, 0 // Load dummy2
   slli.d XX, XX, ZBASE_SHIFT
   MTC  a1, $r0
   slli.d INCX, INCX, ZBASE_SHIFT
   bge $r0,    N, .L999
   CMPEQ   $fcc0, ALPHA_R, a1
   CMPEQ   $fcc1, ALPHA_I, a1
   bceqz   $fcc0, .L50
   bceqz   $fcc1, .L50
   beq     XX,    TEMP,  .L50  // if dummp2 == 1, do not directly copy 0
   srai.d I, N, 2
   bne INCX, TEMP, .L20
   bge $r0,    I, .L15
   .align 3

.L12:
   ST a1,  X,   0 * SIZE
   ST a1,  X,   1 * SIZE
   ST a1,  X,   2 * SIZE
   ST a1,  X,   3 * SIZE
   ST a1,  X,   4 * SIZE
   ST a1,  X,   5 * SIZE
   ST a1,  X,   6 * SIZE
   ST a1,  X,   7 * SIZE
   addi.w  I, I, -1
   addi.d X, X, 8 * SIZE
   blt $r0,    I, .L12
   .align 3

.L15:
   andi    I,  N, 3
   bge $r0,    I, .L999
   .align  3
.L16:
   ST a1,  X,   0 * SIZE
   ST a1,  X,   1 * SIZE
   addi.d  I, I, -1
   addi.d X, X, 2 * SIZE
   blt $r0,    I, .L16
   move $r4, $r17
   fmov.d $f0, $f22
   jirl    $r0, $r1, 0x0
   .align 3

.L20:
   srai.d  I, N, 2
   bge $r0,    I, .L25
   .align 3

.L22:
   ST a1,  X,   0 * SIZE
   ST a1,  X,   1 * SIZE
   add.d   X, X, INCX
   ST a1,  X,   0 * SIZE
   ST a1,  X,   1 * SIZE
   add.d   X, X, INCX
   ST a1,  X,   0 * SIZE
   ST a1,  X,   1 * SIZE
   add.d   X, X, INCX
   ST a1,  X,   0 * SIZE
   ST a1,  X,   1 * SIZE
   addi.d  I, I, -1
   add.d  X, X, INCX
   blt $r0,    I, .L22
   .align 3

.L25:
   andi    I,  N, 3
   bge $r0,    I, .L999
   .align  3
.L26:
   ST a1,  X,   0 * SIZE
   addi.d  I, I, -1
   ST a1,  X,   1 * SIZE
   add.d  X, X, INCX
   blt $r0,    I, .L26
   move $r4, $r17
   fmov.d $f0, $f22
   jirl    $r0, $r1, 0x0
   .align 3

.L50:
   srai.d I, N, 2
   bne INCX, TEMP, .L60
   addi.d I, I, -1
   blt I,  $r0, .L55
   LD a1,  X,   0 * SIZE
   LD a2,  X,   1 * SIZE
   LD a3,  X,   2 * SIZE
   LD a4,  X,   3 * SIZE
   LD a5,  X,   4 * SIZE
   LD a6,  X,   5 * SIZE
   MUL t1, ALPHA_R, a1
   LD a7,  X,   6 * SIZE
   MUL t2, ALPHA_I, a1
   LD a8,  X,   7 * SIZE
   MUL t3, ALPHA_R, a3
   MUL    t4, ALPHA_I, a3
   bge $r0,    I, .L53
   .align 3

.L52:
   NMSUB  t1, a2, ALPHA_I, t1
   LD a1,  X,   8 * SIZE
   MADD  t2, a2, ALPHA_R, t2
   LD a2,  X,   9 * SIZE
   NMSUB  t3, a4, ALPHA_I, t3
   LD a3,  X,  10 * SIZE
   MADD  t4, a4, ALPHA_R, t4
   LD a4,  X,  11 * SIZE
   ST t1,  X,   0 * SIZE
   MUL t1, ALPHA_R, a5
   ST t2,  X,   1 * SIZE
   MUL t2, ALPHA_I, a5
   ST t3,  X,   2 * SIZE
   MUL t3, ALPHA_R, a7
   ST t4,  X,   3 * SIZE
   MUL t4, ALPHA_I, a7
   NMSUB  t1, a6, ALPHA_I, t1
   LD a5,  X,  12 * SIZE
   MADD  t2, a6, ALPHA_R, t2
   LD a6,  X,  13 * SIZE
   NMSUB  t3, a8, ALPHA_I, t3
   LD a7,  X,  14 * SIZE
   MADD  t4, a8, ALPHA_R, t4
   LD a8,  X,  15 * SIZE
   ST t1,  X,   4 * SIZE
   MUL t1, ALPHA_R, a1
   ST t2,  X,   5 * SIZE
   MUL t2, ALPHA_I, a1
   ST t3,  X,   6 * SIZE
   MUL t3, ALPHA_R, a3
   ST t4,  X,   7 * SIZE
   MUL t4, ALPHA_I, a3
   addi.d  I, I, -1
   addi.d X, X, 8 * SIZE
   blt $r0,    I, .L52
   .align 3

.L53:
   NMSUB  t1, a2, ALPHA_I, t1
   MADD  t2, a2, ALPHA_R, t2
   NMSUB  t3, a4, ALPHA_I, t3
   MADD  t4, a4, ALPHA_R, t4
   ST t1,  X,   0 * SIZE
   MUL t1, ALPHA_R, a5
   ST t2,  X,   1 * SIZE
   MUL t2, ALPHA_I, a5
   ST t3,  X,   2 * SIZE
   MUL t3, ALPHA_R, a7
   ST t4,  X,   3 * SIZE
   MUL t4, ALPHA_I, a7
   NMSUB  t1, a6, ALPHA_I, t1
   MADD  t2, a6, ALPHA_R, t2
   NMSUB  t3, a8, ALPHA_I, t3
   MADD  t4, a8, ALPHA_R, t4
   ST t1,  X,   4 * SIZE
   ST t2,  X,   5 * SIZE
   ST t3,  X,   6 * SIZE
   ST t4,  X,   7 * SIZE
   addi.d  X, X, 8 * SIZE
   .align 3

.L55:
   andi    I,  N, 3
   bge $r0,    I, .L999
   .align  3
.L56:
   LD a1,  X,   0 * SIZE
   LD a2,  X,   1 * SIZE
   MUL t1, ALPHA_R, a1
   MUL t2, ALPHA_I, a1
   NMSUB  t1, a2, ALPHA_I, t1
   MADD  t2, a2, ALPHA_R, t2
   addi.d  X, X, 2 * SIZE
   addi.d  I, I, -1
   ST t1,  X,  -2 * SIZE
   ST t2,  X,  -1 * SIZE
   blt $r0,    I, .L56
   move $r4, $r17
   fmov.d $f0, $f22
   jirl    $r0, $r1, 0x0
   .align 3

.L60:
   srai.d  I, N, 2
   move    XX, X
   addi.d I, I, -1
   blt I,  $r0, .L65
   LD a1,  X,   0 * SIZE
   LD a2,  X,   1 * SIZE
   add.d   X, X, INCX
   LD a3,  X,   0 * SIZE
   LD a4,  X,   1 * SIZE
   add.d   X, X, INCX
   LD a5,  X,   0 * SIZE
   LD a6,  X,   1 * SIZE
   add.d   X, X, INCX
   MUL t1, ALPHA_R, a1
   LD a7,  X,   0 * SIZE
   MUL t2, ALPHA_I, a1
   LD a8,  X,   1 * SIZE
   MUL t3, ALPHA_R, a3
   add.d   X, X, INCX
   MUL    t4, ALPHA_I, a3
   bge $r0,    I, .L63
   .align 3

.L62:
   NMSUB  t1, a2, ALPHA_I, t1
   LD a1,  X,   0 * SIZE
   MADD  t2, a2, ALPHA_R, t2
   LD a2,  X,   1 * SIZE
   add.d   X, X, INCX
   NMSUB  t3, a4, ALPHA_I, t3
   LD a3,  X,   0 * SIZE
   MADD  t4, a4, ALPHA_R, t4
   LD a4,  X,   1 * SIZE
   add.d   X, X, INCX
   ST t1,  XX,   0 * SIZE
   MUL t1, ALPHA_R, a5
   ST t2,  XX,   1 * SIZE
   MUL t2, ALPHA_I, a5
   add.d   XX, XX, INCX
   ST t3,  XX,   0 * SIZE
   MUL t3, ALPHA_R, a7
   ST t4,  XX,   1 * SIZE
   MUL t4, ALPHA_I, a7
   add.d   XX, XX, INCX
   NMSUB  t1, a6, ALPHA_I, t1
   LD a5,  X,   0 * SIZE
   MADD  t2, a6, ALPHA_R, t2
   LD a6,  X,   1 * SIZE
   add.d   X, X, INCX
   NMSUB  t3, a8, ALPHA_I, t3
   LD a7,  X,   0 * SIZE
   MADD  t4, a8, ALPHA_R, t4
   LD a8,  X,   1 * SIZE
   add.d   X, X, INCX
   ST t1,  XX,   0 * SIZE
   MUL t1, ALPHA_R, a1
   ST t2,  XX,   1 * SIZE
   MUL t2, ALPHA_I, a1
   add.d   XX, XX, INCX
   ST t3,  XX,   0 * SIZE
   MUL t3, ALPHA_R, a3
   ST t4,  XX,   1 * SIZE
   MUL t4, ALPHA_I, a3
   addi.d  I, I, -1
   add.d  XX, XX, INCX
   blt $r0,    I, .L62
   .align 3

.L63:
   NMSUB  t1, a2, ALPHA_I, t1
   MADD  t2, a2, ALPHA_R, t2
   NMSUB  t3, a4, ALPHA_I, t3
   MADD  t4, a4, ALPHA_R, t4
   ST t1,  XX,   0 * SIZE
   MUL t1, ALPHA_R, a5
   ST t2,  XX,   1 * SIZE
   MUL t2, ALPHA_I, a5
   add.d   XX, XX, INCX
   ST t3,  XX,   0 * SIZE
   MUL t3, ALPHA_R, a7
   ST t4,  XX,   1 * SIZE
   MUL t4, ALPHA_I, a7
   add.d   XX, XX, INCX
   NMSUB  t1, a6, ALPHA_I, t1
   MADD  t2, a6, ALPHA_R, t2
   NMSUB  t3, a8, ALPHA_I, t3
   MADD  t4, a8, ALPHA_R, t4
   ST t1,  XX,   0 * SIZE
   ST t2,  XX,   1 * SIZE
   add.d   XX, XX, INCX
   ST t3,  XX,   0 * SIZE
   ST t4,  XX,   1 * SIZE
   add.d   XX, XX, INCX
   .align 3

.L65:
   andi    I,  N, 3
   bge $r0,    I, .L999
   .align  3
.L66:
   LD a1,  X,   0 * SIZE
   LD a2,  X,   1 * SIZE
   MUL t1, ALPHA_R, a1
   MUL t2, ALPHA_I, a1
   NMSUB  t1, a2, ALPHA_I, t1
   MADD  t2, a2, ALPHA_R, t2
   addi.d  I, I, -1
   ST t1,  X,   0 * SIZE
   ST t2,  X,   1 * SIZE
   add.d  X, X, INCX
   blt $r0,    I, .L66
   .align 3

.L999:
   move $r4, $r17
   fmov.d $f0, $f22
   jirl    $r0, $r1, 0x0

   EPILOGUE
