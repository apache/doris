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
#define I      $r17
#define TEMP   $r18
#define a1 $f23
#define a2 $f9
#define a3 $f10
#define a4 $f11
#define a5 $f12
#define a6 $f13
#define a7 $f14
#define a8 $f15
#define t1 $f16
#define t2 $f17
#define t3 $f0
#define t4 $f1
#define s1 $f22
#define s2 $f8
   PROLOGUE
#ifdef F_INTERFACE
   LDINT   N,     0(N)
   LDINT   INCX,  0(INCX)
#endif
   MTC  s1, $r0
   MTC  s2, $r0
   slli.d  INCX, INCX, BASE_SHIFT
   li.d TEMP, SIZE
   bge $r0,    N, .L999
   srai.d  I, N, 3
   bne INCX, TEMP, .L20
   bge $r0,    I, .L15
   LD a1,  X,   0 * SIZE
   LD a2,  X,   1 * SIZE
   LD a3,  X,   2 * SIZE
   LD a4,  X,   3 * SIZE
   LD a5,  X,   4 * SIZE
   FABS    t1, a1
   LD a6,  X,   5 * SIZE
   FABS    t2, a2
   LD a7,  X,   6 * SIZE
   FABS    t3, a3
   FABS    t4, a4
   addi.d  I, I, -1
   LD a8,  X,   7 * SIZE
   bge $r0,    I, .L13
   .align 3
.L12:
   ADD s1, s1, t1
   LD a1,  X,   8 * SIZE
   FABS    t1, a5
   addi.d  I, I, -1
   ADD s2, s2, t2
   LD a2,  X,   9 * SIZE
   FABS    t2, a6
   NOP
   ADD s1, s1, t3
   LD a3,  X,  10 * SIZE
   FABS    t3, a7
   NOP
   ADD s2, s2, t4
   LD a4,  X,  11 * SIZE
   FABS    t4, a8
   addi.d  X, X, 8 * SIZE
   ADD s1, s1, t1
   LD a5,  X,   4 * SIZE
   FABS    t1, a1
   NOP
   ADD s2, s2, t2
   LD a6,  X,   5 * SIZE
   FABS    t2, a2
   NOP
   ADD s1, s1, t3
   LD a7,  X,   6 * SIZE
   FABS    t3, a3
   NOP
   ADD s2, s2, t4
   LD a8,  X,   7 * SIZE
   FABS   t4, a4
   blt $r0,    I, .L12
   .align 3
.L13:
   ADD s1, s1, t1
   addi.d  X, X, 8 * SIZE
   FABS    t1, a5
   NOP
   ADD s2, s2, t2
   FABS    t2, a6
   ADD s1, s1, t3
   FABS    t3, a7
   ADD s2, s2, t4
   FABS    t4, a8
   ADD s1, s1, t1
   ADD s2, s2, t2
   ADD s1, s1, t3
   ADD s2, s2, t4
   .align 3
.L15:
   andi    I,  N, 7
   bge $r0,    I, .L999
   .align  3
.L16:
   LD a1,  X,   0 * SIZE
   addi.d  I, I, -1
   FABS    t1, a1
   ADD s1, s1, t1
   addi.d X, X, SIZE
   blt $r0,    I, .L16
   b   .L999
   .align 3
.L20:
   bge $r0,    I, .L25
   LD a1,  X,   0 * SIZE
   add.d   X, X, INCX
   LD a2,  X,   0 * SIZE
   add.d   X, X, INCX
   LD a3,  X,   0 * SIZE
   add.d   X, X, INCX
   LD a4,  X,   0 * SIZE
   add.d   X, X, INCX
   LD a5,  X,   0 * SIZE
   add.d   X, X, INCX
   LD a6,  X,   0 * SIZE
   add.d   X, X, INCX
   FABS    t1, a1
   LD a7,  X,   0 * SIZE
   FABS    t2, a2
   add.d   X, X, INCX
   FABS    t3, a3
   LD a8,  X,   0 * SIZE
   FABS    t4, a4
   addi.d  I, I, -1
   add.d  X, X, INCX
   bge $r0,    I, .L24
   .align 3
.L23:
   ADD s1, s1, t1
   LD a1,  X,   0 * SIZE
   FABS    t1, a5
   add.d   X, X, INCX
   ADD s2, s2, t2
   LD a2,  X,   0 * SIZE
   FABS    t2, a6
   add.d   X, X, INCX
   ADD s1, s1, t3
   LD a3,  X,   0 * SIZE
   FABS    t3, a7
   add.d   X, X, INCX
   ADD s2, s2, t4
   LD a4,  X,   0 * SIZE
   FABS    t4, a8
   add.d   X, X, INCX
   ADD s1, s1, t1
   LD a5,  X,   0 * SIZE
   FABS    t1, a1
   add.d   X, X, INCX
   ADD s2, s2, t2
   LD a6,  X,   0 * SIZE
   FABS    t2, a2
   add.d   X, X, INCX
   ADD s1, s1, t3
   LD a7,  X,   0 * SIZE
   FABS    t3, a3
   add.d   X, X, INCX
   ADD s2, s2, t4
   LD a8,  X,   0 * SIZE
   FABS    t4, a4
   addi.d  I, I, -1
   add.d  X, X, INCX
   blt $r0,    I, .L23
   .align 3
.L24:
   ADD s1, s1, t1
   FABS    t1, a5
   ADD s2, s2, t2
   FABS    t2, a6
   ADD s1, s1, t3
   FABS    t3, a7
   ADD s2, s2, t4
   FABS    t4, a8
   ADD s1, s1, t1
   ADD s2, s2, t2
   ADD s1, s1, t3
   ADD s2, s2, t4
   .align 3
.L25:
   andi    I,  N, 7
   bge $r0,    I, .L999
   .align  3
.L26:
   LD a1,  X,   0 * SIZE
   addi.d  I, I, -1
   FABS    t1, a1
   add.d   X, X, INCX
   ADD    s1, s1, t1
   blt $r0,    I, .L26
   .align 3
.L999:
   ADD s1, s1, s2
   move $r4, $r17
   fmov.d $f0, $f22
   jirl    $r0, $r1, 0x0
   EPILOGUE
