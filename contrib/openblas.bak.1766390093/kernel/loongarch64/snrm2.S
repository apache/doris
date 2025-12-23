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
#define a1     $f12
#define a2     $f13
#define a3     $f14
#define a4     $f15
#define a5     $f16
#define a6     $f17
#define a7     $f0
#define a8     $f1
#define s1     $f22
#define s2     $f8
#define t1     $f23
#define t2     $f9
#define t3     $f10
#define t4     $f11

   PROLOGUE

#ifdef F_INTERFACE
   LDINT   N,     0(N)
   LDINT   INCX,  0(INCX)
#endif

   movgr2fr.d  s1, $r0
   li.d  TEMP, SIZE
   fmov.d s2, s1
   bge $r0,    N, .L999
   slli.d INCX, INCX, BASE_SHIFT
   beq $r0,    INCX, .L999
   srai.d I, N, 3
   bne INCX, TEMP, .L20
   bge $r0,    I, .L15
   LD a1,  X,   0 * SIZE
   LD a2,  X,   1 * SIZE
   LD a3,  X,   2 * SIZE
   LD a4,  X,   3 * SIZE
   LD a5,  X,   4 * SIZE
   addi.d  I, I, -1
   fcvt.d.s    t1, a1
   LD a6,  X,   5 * SIZE
   fcvt.d.s    t2, a2
   LD a7,  X,   6 * SIZE
   fcvt.d.s    t3, a3
   LD a8,  X,   7 * SIZE
   fcvt.d.s   t4, a4
   bge $r0,    I, .L13
   .align 3

.L12:
   fmadd.d  s1, t1, t1, s1
   LD a1,  X,   8 * SIZE
   fcvt.d.s    t1, a5
   NOP
   fmadd.d  s2, t2, t2, s2
   LD a2,  X,   9 * SIZE
   fcvt.d.s    t2, a6
   NOP
   fmadd.d  s1, t3, t3, s1
   LD a3,  X,  10 * SIZE
   fcvt.d.s    t3, a7
   NOP
   fmadd.d  s2, t4, t4, s2
   LD a4,  X,  11 * SIZE
   fcvt.d.s    t4, a8
   NOP
   fmadd.d  s1, t1, t1, s1
   LD a5,  X,  12 * SIZE
   fcvt.d.s    t1, a1
   NOP
   fmadd.d  s2, t2, t2, s2
   LD a6,  X,  13 * SIZE
   fcvt.d.s    t2, a2
   addi.d  I, I, -1
   fmadd.d  s1, t3, t3, s1
   LD a7,  X,  14 * SIZE
   fcvt.d.s    t3, a3
   addi.d  X, X, 8 * SIZE
   fmadd.d  s2, t4, t4, s2
   LD a8,  X,   7 * SIZE
   fcvt.d.s   t4, a4
   blt $r0,    I, .L12
   .align 3

.L13:
   fmadd.d  s1, t1, t1, s1
   fcvt.d.s    t1, a5
   fmadd.d  s2, t2, t2, s2
   fcvt.d.s    t2, a6
   fmadd.d  s1, t3, t3, s1
   fcvt.d.s    t3, a7
   fmadd.d  s2, t4, t4, s2
   fcvt.d.s    t4, a8
   fmadd.d  s1, t1, t1, s1
   fmadd.d  s2, t2, t2, s2
   fmadd.d  s1, t3, t3, s1
   fmadd.d  s2, t4, t4, s2
   addi.d  X, X, 8 * SIZE
   .align 3

.L15:
   andi    I,  N, 7
   bge $r0,    I, .L999
   .align  3

.L16:
   LD a1,  X,   0 * SIZE
   addi.d  I, I, -1
   fcvt.d.s    t1, a1
   fmadd.d  s1, t1, t1, s1
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
   LD a7,  X,   0 * SIZE
   add.d   X, X, INCX
   LD a8,  X,   0 * SIZE
   addi.d  I, I, -1
   fcvt.d.s    t1, a1
   fcvt.d.s    t2, a2
   fcvt.d.s    t3, a3
   fcvt.d.s    t4, a4
   add.d  X, X, INCX
   bge $r0,    I, .L24
   .align 3

.L23:
   fmadd.d  s1, t1, t1, s1
   LD a1,  X,   0 * SIZE
   fcvt.d.s    t1, a5
   add.d   X, X, INCX
   fmadd.d  s2, t2, t2, s2
   LD a2,  X,   0 * SIZE
   fcvt.d.s    t2, a6
   add.d   X, X, INCX
   fmadd.d  s1, t3, t3, s1
   LD a3,  X,   0 * SIZE
   fcvt.d.s    t3, a7
   add.d   X, X, INCX
   fmadd.d  s2, t4, t4, s2
   LD a4,  X,   0 * SIZE
   fcvt.d.s    t4, a8
   add.d   X, X, INCX
   fmadd.d  s1, t1, t1, s1
   LD a5,  X,   0 * SIZE
   fcvt.d.s    t1, a1
   add.d   X, X, INCX
   fmadd.d  s2, t2, t2, s2
   LD a6,  X,   0 * SIZE
   fcvt.d.s    t2, a2
   add.d   X, X, INCX
   fmadd.d  s1, t3, t3, s1
   LD a7,  X,   0 * SIZE
   fcvt.d.s    t3, a3
   add.d   X, X, INCX
   fmadd.d  s2, t4, t4, s2
   LD a8,  X,   0 * SIZE
   fcvt.d.s    t4, a4
   addi.d  I, I, -1
   add.d  X, X, INCX
   blt $r0,    I, .L23
   .align 3

.L24:
   fmadd.d  s1, t1, t1, s1
   fcvt.d.s    t1, a5
   fmadd.d  s2, t2, t2, s2
   fcvt.d.s    t2, a6
   fmadd.d  s1, t3, t3, s1
   fcvt.d.s    t3, a7
   fmadd.d  s2, t4, t4, s2
   fcvt.d.s    t4, a8
   fmadd.d  s1, t1, t1, s1
   fmadd.d  s2, t2, t2, s2
   fmadd.d  s1, t3, t3, s1
   fmadd.d  s2, t4, t4, s2
   .align 3

.L25:
   andi    I,  N, 7
   bge $r0,    I, .L999
   .align  3

.L26:
   LD a1,  X,   0 * SIZE
   addi.d  I, I, -1
   fcvt.d.s    t1, a1
   add.d   X, X, INCX
   fmadd.d  s1, t1, t1, s1
   blt $r0,    I, .L26
   .align 3

.L999:
   fadd.d  s1, s1, s2
   fsqrt.d s1, s1
   move $r4, $r17
   fcvt.s.d    $f0, s1
   jirl    $r0, $r1, 0x0

   EPILOGUE
