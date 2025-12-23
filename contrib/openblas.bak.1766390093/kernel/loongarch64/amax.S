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

#define a1 $f10
#define a2 $f11
#define a3 $f12
#define a4 $f13
#define a5 $f14
#define a6 $f15
#define a7 $f16
#define a8 $f17

#define t1 $f0
#define t2 $f1
#define t3 $f2
#define t4 $f3

#define s1 $f22
#define s2 $f8
#define s3 $f23
#define s4 $f9

   PROLOGUE

#ifdef F_INTERFACE
   LDINT   N,     0(N)
   LDINT   INCX,  0(INCX)
#endif

   MTC  s1, $r0
   bge $r0,    N, .L999

   slli.d INCX, INCX, BASE_SHIFT
   bge $r0,    INCX, .L999

   LD a1,  X,   0 * SIZE
   addi.d  N, N, -1

   add.d   X, X, INCX
   FABS    s1, a1

   FABS   s2, a1
   bge $r0,    N, .L999

   FABS    s3, a1
   srai.d  I, N, 3

   FABS   s4, a1
   bge $r0,    I, .L15

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

   add.d  X, X, INCX
   bge $r0,    I, .L13
   .align 3

.L12:
   FABS    t1, a1
   LD a1,  X,   0 * SIZE
   FABS    t2, a2
   add.d   X, X, INCX

   FABS    t3, a3
   LD a2,  X,   0 * SIZE
   FABS    t4, a4
   add.d   X, X, INCX

   CMPLT   $fcc0, s1, t1
   LD a3,  X,   0 * SIZE
   CMPLT   $fcc1, s2, t2
   add.d   X, X, INCX

   CMPLT   $fcc2, s3, t3
   LD a4,  X,   0 * SIZE
   CMPLT   $fcc3, s4, t4
   add.d   X, X, INCX

   CMOVT  s1,  s1,  t1,  $fcc0
   CMOVT  s2,  s2,  t2,  $fcc1
   CMOVT  s3,  s3,  t3,  $fcc2
   CMOVT  s4,  s4,  t4,  $fcc3

   FABS    t1, a5
   LD a5,  X,   0 * SIZE
   FABS    t2, a6
   add.d   X, X, INCX

   FABS    t3, a7
   LD a6,  X,   0 * SIZE
   FABS    t4, a8
   add.d   X, X, INCX

   CMPLT   $fcc0, s1, t1
   LD a7,  X,   0 * SIZE
   CMPLT   $fcc1, s2, t2
   add.d   X, X, INCX

   CMPLT   $fcc2, s3, t3
   LD a8,  X,   0 * SIZE
   CMPLT   $fcc3, s4, t4
   add.d   X, X, INCX

   CMOVT  s1,  s1,  t1,  $fcc0
   addi.d  I, I, -1

   CMOVT  s2,  s2,  t2,  $fcc1
   CMOVT  s3,  s3,  t3,  $fcc2

   CMOVT  s4,  s4,  t4,  $fcc3
   blt $r0,    I, .L12
   .align 3

.L13:
   FABS    t1, a1
   FABS    t2, a2
   FABS    t3, a3
   FABS    t4, a4

   CMPLT   $fcc0, s1, t1
   CMPLT   $fcc1, s2, t2
   CMPLT   $fcc2, s3, t3
   CMPLT   $fcc3, s4, t4

   CMOVT  s1,  s1,  t1,  $fcc0
   CMOVT  s2,  s2,  t2,  $fcc1
   CMOVT  s3,  s3,  t3,  $fcc2
   CMOVT  s4,  s4,  t4,  $fcc3

   FABS    t1, a5
   FABS    t2, a6
   FABS    t3, a7
   FABS    t4, a8

   CMPLT   $fcc0, s1, t1
   CMPLT   $fcc1, s2, t2
   CMPLT   $fcc2, s3, t3
   CMPLT   $fcc3, s4, t4

   CMOVT  s1,  s1,  t1,  $fcc0
   CMOVT  s2,  s2,  t2,  $fcc1
   CMOVT  s3,  s3,  t3,  $fcc2
   CMOVT  s4,  s4,  t4,  $fcc3
   .align 3

.L15:
   andi    I,  N, 7

   bge $r0,    I, .L998
   .align  3

.L16:
   LD a1,  X,   0 * SIZE
   addi.d  I, I, -1

   FABS    t1, a1

   CMPLT   $fcc0, s1, t1

   CMOVT  s1,  s1,  t1,  $fcc0

   add.d  X, X, INCX
   blt $r0,    I, .L16
   .align 3

.L998:
   CMPLT   $fcc0, s1, s2
   CMPLT   $fcc1, s3, s4

   CMOVT  s1,  s1,  s2,  $fcc0
   CMOVT  s3,  s3,  s4,  $fcc1

   CMPLT   $fcc0, s1, s3
   CMOVT  s1,  s1,  s3,  $fcc0
   .align 3

.L999:
  move $r4, $r17
  fmov.d $f0, $f22
  jirl    $r0, $r1, 0x0

  EPILOGUE
