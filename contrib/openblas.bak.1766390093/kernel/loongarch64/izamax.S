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
#define I      $r18
#define TEMP   $r7
#define a1     $f10
#define a2     $f11
#define a3     $f12
#define a4     $f13
#define a5     $f14
#define a6     $f15
#define a7     $f16
#define a8     $f17
#define t1     $f0
#define t2     $f1
#define t3     $f2
#define t4     $f3
#define t5     $f4
#define t6     $f5
#define t7     $f6
#define t8     $f7
#define s1     $f22
#define s2     $f8
#define s3     $f23
#define s4     $f9
#define x1     $r17
#define x2     $r8
#define x3     $r9
#define x4     $r10

   PROLOGUE

#ifdef F_INTERFACE
      LDINT   N,     0(N)
      LDINT   INCX,  0(INCX)
#endif

   li.d x1, 0
   bge $r0,    N, .L999
   slli.d INCX, INCX, ZBASE_SHIFT
   bge $r0,    INCX, .L999
   LD a1,  X,   0 * SIZE
   LD a2,  X,   1 * SIZE
   FABS    t1, a1
   FABS    t2, a2
   ADD s1, t1, t2
   ADD s2, t1, t2
   ADD s3, t1, t2
   ADD s4, t1, t2
   addi.d  N, N, -1
   li.d x1, 1
   bge $r0,    N, .L999
   add.d   X, X, INCX
   li.d  x2, 1
   srai.d  I, N, 2
   li.d  x3, 1
   li.d  TEMP, 2
   li.d x4, 1
   bge $r0,    I, .L15
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
   addi.d  I, I, -1
   add.d  X, X, INCX
   bge $r0,    I, .L13
   .align 3

.L12:
   FABS    t1, a1
   LD a1,  X,   0 * SIZE
   FABS    t2, a2
   LD a2,  X,   1 * SIZE
   FABS    t3, a3
   add.d   X, X, INCX
   FABS    t4, a4
   FABS    t5, a5
   LD a3,  X,   0 * SIZE
   FABS    t6, a6
   LD a4,  X,   1 * SIZE
   FABS    t7, a7
   add.d   X, X, INCX
   FABS    t8, a8
   ADD t1, t1, t2
   LD a5,  X,   0 * SIZE
   ADD t3, t3, t4
   LD a6,  X,   1 * SIZE
   ADD t5, t5, t6
   add.d   X, X, INCX
   ADD t7, t7, t8
   CMPLT   $fcc0, s1, t1
   LD a7,  X,   0 * SIZE
   CMPLT   $fcc1, s2, t3
   LD a8,  X,   1 * SIZE
   CMPLT   $fcc2, s3, t5
   add.d   X, X, INCX
   CMPLT   $fcc3, s4, t7
   addi.d  I, I, -1
   CMOVT  s1,  s1,  t1,  $fcc0
   MOVT(x1, TEMP, $fcc0)
   CMOVT  s2,  s2,  t3,  $fcc1
   MOVT(x2, TEMP, $fcc1)
   CMOVT  s3,  s3,  t5,  $fcc2
   MOVT(x3, TEMP, $fcc2)
   CMOVT  s4,  s4,  t7,  $fcc3
   MOVT(x4, TEMP, $fcc3)
   addi.d TEMP, TEMP, 4
   blt $r0,    I, .L12
   .align 3

.L13:
   FABS    t1, a1
   FABS    t2, a2
   FABS    t3, a3
   FABS    t4, a4
   FABS    t5, a5
   FABS    t6, a6
   FABS    t7, a7
   FABS    t8, a8
   ADD t1, t1, t2
   ADD t3, t3, t4
   ADD t5, t5, t6
   ADD t7, t7, t8
   CMPLT   $fcc0, s1, t1
   CMPLT   $fcc1, s2, t3
   CMPLT   $fcc2, s3, t5
   CMPLT   $fcc3, s4, t7
   CMOVT  s1,  s1,  t1,  $fcc0
   MOVT(x1, TEMP, $fcc0)
   CMOVT  s2,  s2,  t3,  $fcc1
   MOVT(x2, TEMP, $fcc1)
   CMOVT  s3,  s3,  t5,  $fcc2
   MOVT(x3, TEMP, $fcc2)
   CMOVT  s4,  s4,  t7,  $fcc3
   MOVT(x4, TEMP, $fcc3)
   addi.d  TEMP, TEMP, 4
   addi.d  x2, x2, 1
   addi.d  x3, x3, 2
   addi.d  x4, x4, 3
   .align 3

.L15:
   andi    I,  N, 3
   bge $r0,    I, .L998
   .align  3

.L16:
   LD a1,  X,   0 * SIZE
   LD a2,  X,   1 * SIZE
   add.d   X, X, INCX
   FABS    t1, a1
   FABS    t2, a2
   ADD t1, t1, t2
   addi.d  I, I, -1
   CMPLT   $fcc0, s1, t1
   CMOVT  s1,  s1,  t1,  $fcc0
   MOVT(x1, TEMP, $fcc0)
   addi.d TEMP, TEMP, 1
   blt $r0,    I, .L16
   .align 3

.L998:
   CMPLT   $fcc0, s1, s2
   CMPLT   $fcc1, s3, s4
   CMOVT  s1,  s1,  s2,  $fcc0
   MOVT(x1, x2, $fcc0)
   CMOVT  s3,  s3,  s4,  $fcc1
   MOVT(x3, x4, $fcc1)
   CMPLT   $fcc0, s1, s3
   CMOVT  s1,  s1,  s3,  $fcc0
   MOVT(x1, x3, $fcc0)
   .align 3

.L999:
   move $r4, $r17
   fmov.d $f0, $f22
   jirl    $r0, $r1, 0x0

   EPILOGUE
