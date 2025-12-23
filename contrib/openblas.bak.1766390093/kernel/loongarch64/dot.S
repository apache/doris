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
#define a1     $f23
#define a2     $f9
#define a3     $f10
#define a4     $f11
#define b1     $f12
#define b2     $f13
#define b3     $f14
#define b4     $f15
#define s1     $f22
#define s2     $f8

   PROLOGUE

#ifdef F_INTERFACE
   LDINT   N,     0(N)
   LDINT   INCX,  0(INCX)
   LDINT   INCY,  0(INCY)
#endif

   MTC  s1, $r0
   MTC  s2, $r0
   slli.d  INCX, INCX, BASE_SHIFT
   li.d  TEMP, SIZE
   slli.d INCY, INCY, BASE_SHIFT
   bge $r0,    N, .L999
   srai.d I, N, 3
   bne INCX, TEMP, .L20
   bne INCY, TEMP, .L20
   bge $r0,    I, .L15
   LD a1,  X,   0 * SIZE
   LD b1,  Y,   0 * SIZE
   LD a2,  X,   1 * SIZE
   LD b2,  Y,   1 * SIZE
   LD a3,  X,   2 * SIZE
   LD b3,  Y,   2 * SIZE
   LD a4,  X,   3 * SIZE
   addi.d  I, I, -1
   LD b4,  Y,   3 * SIZE
   bge $r0,    I, .L13
   .align 3

.L12:
#ifdef DSDOT
   fcvt.d.s  a1, a1
   fcvt.d.s  b1, b1
   fmadd.d  s1, b1, a1, s1
#else
   MADD  s1, b1, a1, s1
#endif
   LD a1,  X,   4 * SIZE
   LD b1,  Y,   4 * SIZE
#ifdef DSDOT
   fcvt.d.s  a2, a2
   fcvt.d.s  b2, b2
   fmadd.d  s2, b2, a2, s2
#else
   MADD  s2, b2, a2, s2
#endif
   LD a2,  X,   5 * SIZE
   LD b2,  Y,   5 * SIZE
#ifdef DSDOT
   fcvt.d.s  a3, a3
   fcvt.d.s  b3, b3
   fmadd.d  s1, b3, a3, s1
#else
   MADD  s1, b3, a3, s1
#endif
   LD a3,  X,   6 * SIZE
   LD b3,  Y,   6 * SIZE
#ifdef DSDOT
   fcvt.d.s  a4, a4
   fcvt.d.s  b4, b4
   fmadd.d  s2, b4, a4, s2
#else
   MADD  s2, b4, a4, s2
#endif
   LD a4,  X,   7 * SIZE
   LD b4,  Y,   7 * SIZE
#ifdef DSDOT
   fcvt.d.s  a1, a1
   fcvt.d.s  b1, b1
   fmadd.d  s1, b1, a1, s1
#else
   MADD  s1, b1, a1, s1
#endif
   LD a1,  X,   8 * SIZE
   LD b1,  Y,   8 * SIZE
#ifdef DSDOT
   fcvt.d.s  a2, a2
   fcvt.d.s  b2, b2
   fmadd.d  s2, b2, a2, s2
#else
   MADD  s2, b2, a2, s2
#endif
   LD a2,  X,   9 * SIZE
   LD b2,  Y,   9 * SIZE
#ifdef DSDOT
   fcvt.d.s  a3, a3
   fcvt.d.s  b3, b3
   fmadd.d  s1, b3, a3, s1
#else
   MADD  s1, b3, a3, s1
#endif
   LD a3,  X,  10 * SIZE
   LD b3,  Y,  10 * SIZE
#ifdef DSDOT
   fcvt.d.s  a4, a4
   fcvt.d.s  b4, b4
   fmadd.d  s2, b4, a4, s2
#else
   MADD  s2, b4, a4, s2
#endif
   LD a4,  X,  11 * SIZE
   LD b4,  Y,  11 * SIZE
   addi.d  I, I, -1
   addi.d  X, X, 8 * SIZE
addi.d Y, Y, 8 * SIZE
   blt $r0,    I, .L12
   .align 3
.L13:
#ifdef DSDOT
   fcvt.d.s  a1, a1
   fcvt.d.s  b1, b1
   fmadd.d  s1, b1, a1, s1
#else
   MADD  s1, b1, a1, s1
#endif
   LD a1,  X,   4 * SIZE
   LD b1,  Y,   4 * SIZE
#ifdef DSDOT
   fcvt.d.s a2, a2
   fcvt.d.s b2, b2
   fmadd.d  s2, b2, a2, s2
#else
   MADD  s2, b2, a2, s2
#endif
   LD a2,  X,   5 * SIZE
   LD b2,  Y,   5 * SIZE
#ifdef DSDOT
   fcvt.d.s a3, a3
   fcvt.d.s b3, b3
   fmadd.d  s1, b3, a3, s1
#else
   MADD  s1, b3, a3, s1
#endif
   LD a3,  X,   6 * SIZE
   LD b3,  Y,   6 * SIZE
#ifdef DSDOT
   fcvt.d.s a4, a4
   fcvt.d.s b4, b4
   fmadd.d  s2, b4, a4, s2
#else
   MADD  s2, b4, a4, s2
#endif
   LD a4,  X,   7 * SIZE
   LD b4,  Y,   7 * SIZE
#ifdef DSDOT
   fcvt.d.s  a1, a1
   fcvt.d.s  b1, b1
   fmadd.d  s1, b1, a1, s1
#else
   MADD  s1, b1, a1, s1
#endif
   addi.d  X, X, 8 * SIZE
#ifdef DSDOT
   fcvt.d.s  a2, a2
   fcvt.d.s  b2, b2
   fmadd.d  s2, b2, a2, s2
#else
   MADD  s2, b2, a2, s2
#endif
   addi.d  Y, Y, 8 * SIZE
#ifdef DSDOT
   fcvt.d.s  a3, a3
   fcvt.d.s  b3, b3
   fmadd.d  s1, b3, a3, s1
#else
   MADD  s1, b3, a3, s1
#endif
#ifdef DSDOT
   fcvt.d.s  a4, a4
   fcvt.d.s  b4, b4
   fmadd.d  s2, b4, a4, s2
#else
   MADD  s2, b4, a4, s2
#endif
   .align 3
.L15:
   andi    I,  N, 7
   bge $r0,    I, .L999
   .align  3
.L16:
   LD a1,  X,   0 * SIZE
   LD b1,  Y,   0 * SIZE
#ifdef DSDOT
   fcvt.d.s  a1, a1
   fcvt.d.s  b1, b1
   fmadd.d  s1, b1, a1, s1
#else
   MADD  s1, b1, a1, s1
#endif
   addi.d  I, I, -1
   addi.d  X, X, SIZE
   addi.d  Y, Y, SIZE
   blt $r0,    I, .L16
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
   .align 3

.L23:
   LD a1,  X,   0 * SIZE
   add.d   X, X, INCX
   LD b1,  Y,   0 * SIZE
   add.d   Y, Y, INCY
#ifdef DSDOT
   fcvt.d.s  a1, a1
   fcvt.d.s  b1, b1
   fmadd.d  s1, b1, a1, s1
#else
   MADD  s1, b1, a1, s1
#endif
   LD a1,  X,   0 * SIZE
   add.d   X, X, INCX
   LD b1,  Y,   0 * SIZE
   add.d   Y, Y, INCY
#ifdef DSDOT
   fcvt.d.s  a1, a1
   fcvt.d.s  b1, b1
   fmadd.d  s2, b1, a1, s2
#else
   MADD  s2, b1, a1, s2
#endif
   LD a1,  X,   0 * SIZE
   add.d   X, X, INCX
   LD b1,  Y,   0 * SIZE
   add.d   Y, Y, INCY
#ifdef DSDOT
   fcvt.d.s  a1, a1
   fcvt.d.s  b1, b1
   fmadd.d  s1, b1, a1, s1
#else
   MADD  s1, b1, a1, s1
#endif
   LD a1,  X,   0 * SIZE
   add.d   X, X, INCX
   LD b1,  Y,   0 * SIZE
   add.d   Y, Y, INCY
#ifdef DSDOT
   fcvt.d.s  a1, a1
   fcvt.d.s  b1, b1
   fmadd.d  s2, b1, a1, s2
#else
   MADD  s2, b1, a1, s2
#endif
   LD a1,  X,   0 * SIZE
   add.d   X, X, INCX
   LD b1,  Y,   0 * SIZE
   add.d   Y, Y, INCY
#ifdef DSDOT
   fcvt.d.s  a1, a1
   fcvt.d.s  b1, b1
   fmadd.d  s1, b1, a1, s1
#else
   MADD  s1, b1, a1, s1
#endif
   LD a1,  X,   0 * SIZE
   add.d   X, X, INCX
   LD b1,  Y,   0 * SIZE
   add.d   Y, Y, INCY
#ifdef DSDOT
   fcvt.d.s  a1, a1
   fcvt.d.s  b1, b1
   fmadd.d  s2, b1, a1, s2
#else
   MADD  s2, b1, a1, s2
#endif
   LD a1,  X,   0 * SIZE
   add.d   X, X, INCX
   LD b1,  Y,   0 * SIZE
   add.d   Y, Y, INCY
#ifdef DSDOT
   fcvt.d.s  a1, a1
   fcvt.d.s  b1, b1
   fmadd.d  s1, b1, a1, s1
#else
   MADD  s1, b1, a1, s1
#endif
   LD a1,  X,   0 * SIZE
   add.d   X, X, INCX
   LD b1,  Y,   0 * SIZE
   add.d   Y, Y, INCY
   addi.d  I, I, -1
#ifdef DSDOT
   fcvt.d.s  a1, a1
   fcvt.d.s  b1, b1
   fmadd.d  s2, b1, a1, s2
#else
   MADD  s2, b1, a1, s2
#endif
   blt $r0,    I, .L23
   .align 3

.L25:
   andi    I,  N, 7
   bge $r0,    I, .L999
   .align  3

.L26:
   LD a1,  X,   0 * SIZE
   add.d   X, X, INCX
   LD b1,  Y,   0 * SIZE
   add.d   Y, Y, INCY
   addi.d  I, I, -1
#ifdef DSDOT
   fcvt.d.s  a1, a1
   fcvt.d.s  b1, b1
   fmadd.d  s1, b1, a1, s1
#else
   MADD  s1, b1, a1, s1
#endif
   blt $r0,    I, .L26
   .align 3

.L999:
#ifdef DSDOT
   fadd.d $f0, s1, s2
#else
   ADD    $f0, s1, s2
#endif
   move $r4, $r17
   jirl    $r0, $r1, 0x0

   EPILOGUE
