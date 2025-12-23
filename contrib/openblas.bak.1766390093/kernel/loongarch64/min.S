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
#define a1     $f10
#define a2     $f11
#define a3     $f12
#define a4     $f13
#define a5     $f14
#define a6     $f15
#define a7     $f16
#define a8     $f17
#define s1     $f22
#define s2     $f8
#define s3     $f23
#define s4     $f9

   PROLOGUE

#ifdef F_INTERFACE
   LDINT   N,     0(N)
   LDINT   INCX,  0(INCX)
#endif

   MTC  s1, $r0
   bge $r0,    N, .L999
   slli.d INCX, INCX, BASE_SHIFT
   bge $r0,    INCX, .L999
   LD s1,  X,   0 * SIZE
   addi.d  N, N, -1
   add.d   X, X, INCX
   MOV    s2, s1
   bge $r0,    N, .L999
   MOV s3, s1
   srai.d  I, N, 3
   MOV    s4, s1
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
   addi.d  I, I, -1
   add.d  X, X, INCX
   bge $r0,    I, .L13
   .align 3

.L12:
   CMPLT   $fcc0, a1, s1
   LD a7,  X,   0 * SIZE
   CMPLT   $fcc1, a2, s2
   add.d   X, X, INCX
   CMPLT   $fcc2, a3, s3
   LD a8,  X,   0 * SIZE
   CMPLT   $fcc3, a4, s4
   add.d   X, X, INCX
   CMOVT  s1,  s1,  a1,  $fcc0
   LD a1,  X,   0 * SIZE
   CMOVT  s2,  s2,  a2,  $fcc1
   add.d   X, X, INCX
   CMOVT  s3,  s3,  a3,  $fcc2
   LD a2,  X,   0 * SIZE
   CMOVT  s4,  s4,  a4,  $fcc3
   add.d   X, X, INCX
   CMPLT   $fcc0, a5, s1
   LD a3,  X,   0 * SIZE
   CMPLT   $fcc1, a6, s2
   add.d   X, X, INCX
   CMPLT   $fcc2, a7, s3
   LD a4,  X,   0 * SIZE
   CMPLT   $fcc3, a8, s4
   add.d   X, X, INCX
   CMOVT  s1,  s1,  a5,  $fcc0
   LD a5,  X,   0 * SIZE
   CMOVT  s2,  s2,  a6,  $fcc1
   add.d   X, X, INCX
   CMOVT  s3,  s3,  a7,  $fcc2
   LD a6,  X,   0 * SIZE
   CMOVT  s4,  s4,  a8,  $fcc3
   addi.d  I, I, -1
   add.d  X, X, INCX
   blt $r0,    I, .L12
   .align 3

.L13:
   CMPLT   $fcc0, a1, s1
   LD a7,  X,   0 * SIZE
   CMPLT   $fcc1, a2, s2
   add.d   X, X, INCX
   CMPLT   $fcc2, a3, s3
   LD a8,  X,   0 * SIZE
   CMPLT   $fcc3, a4, s4
   add.d   X, X, INCX
   CMOVT  s1,  s1,  a1,  $fcc0
   CMOVT  s2,  s2,  a2,  $fcc1
   CMOVT  s3,  s3,  a3,  $fcc2
   CMOVT  s4,  s4,  a4,  $fcc3
   CMPLT   $fcc0, a5, s1
   CMPLT   $fcc1, a6, s2
   CMPLT   $fcc2, a7, s3
   CMPLT   $fcc3, a8, s4
   CMOVT  s1,  s1,  a5,  $fcc0
   CMOVT  s2,  s2,  a6,  $fcc1
   CMOVT  s3,  s3,  a7,  $fcc2
   CMOVT  s4,  s4,  a8,  $fcc3
   .align 3

.L15:
   andi    I,  N, 7
   bge $r0,    I, .L998
   .align  3

.L16:
   LD a1,  X,   0 * SIZE
   addi.d  I, I, -1
   CMPLT   $fcc0, a1, s1
   CMOVT  s1,  s1,  a1,  $fcc0
   add.d  X, X, INCX
   blt $r0,    I, .L16
   .align 3

.L998:
   CMPLT   $fcc0, s2, s1
   CMPLT   $fcc1, s4, s3
   CMOVT  s1,  s1,  s2,  $fcc0
   CMOVT  s3,  s3,  s4,  $fcc1
   CMPLT   $fcc0, s3, s1
   CMOVT  s1,  s1,  s3,  $fcc0
   .align 3

.L999:
   move $r4, $r17
   fmov.d $f0, $f22
   jirl    $r0, $r1, 0x0

   EPILOGUE
