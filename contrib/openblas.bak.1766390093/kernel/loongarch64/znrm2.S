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
#define XX     $r7
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
#define t1     $f0
#define t2     $f1
#define t3     $f2
#define t4     $f3
#define s1     $f22
#define s2     $f8
#define s3     $f23
#define s4     $f9
#define ALPHA  $f4
#define max    $f5

   PROLOGUE

#ifdef F_INTERFACE
   LDINT   N,     0(N)
   LDINT   INCX,  0(INCX)
#endif

   MTC  s1, $r0
   bge $r0,    N, .L999
   slli.d INCX, INCX, ZBASE_SHIFT
   beq $r0,    INCX, .L999
   move    XX, X
   MOV s2, s1
   srai.d  I, N, 2
   MOV s3, s1
   MOV    s4, s1
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
   NOP
   FABS    t3, a3
   LD a2,  X,   1 * SIZE
   FABS    t4, a4
   add.d   X, X, INCX
   CMPLT   $fcc0, s1, t1
   LD a3,  X,   0 * SIZE
   CMPLT   $fcc1, s2, t2
   NOP
   CMPLT   $fcc2, s3, t3
   LD a4,  X,   1 * SIZE
   CMPLT   $fcc3, s4, t4
   add.d   X, X, INCX
   CMOVT  s1,  s1,  t1,  $fcc0
   CMOVT  s2,  s2,  t2,  $fcc1
   CMOVT  s3,  s3,  t3,  $fcc2
   CMOVT  s4,  s4,  t4,  $fcc3
   FABS    t1, a5
   LD a5,  X,   0 * SIZE
   FABS    t2, a6
   NOP
   FABS    t3, a7
   LD a6,  X,   1 * SIZE
   FABS    t4, a8
   add.d   X, X, INCX
   CMPLT   $fcc0, s1, t1
   LD a7,  X,   0 * SIZE
   CMPLT   $fcc1, s2, t2
   NOP
   CMPLT   $fcc2, s3, t3
   LD a8,  X,   1 * SIZE
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
   andi    I,  N, 3
   bge $r0,    I, .L100
   .align  3

.L16:
   LD a1,  X,   0 * SIZE
   LD a2,  X,   1 * SIZE
   addi.d  I, I, -1
   FABS    t1, a1
   FABS    t2, a2
   CMPLT   $fcc0, s1, t1
   CMPLT   $fcc1, s2, t2
   CMOVT  s1,  s1,  t1,  $fcc0
   CMOVT  s2,  s2,  t2,  $fcc1
   add.d  X, X, INCX
   blt $r0,    I, .L16
   .align 3

.L100:
   CMPLT   $fcc0, s1, s2
   CMPLT   $fcc1, s3, s4
   CMOVT  s1,  s1,  s2,  $fcc0
   CMOVT  s3,  s3,  s4,  $fcc1
   CMPLT   $fcc0, s1, s3
   CMOVT  s1,  s1,  s3,  $fcc0
   lu12i.w TEMP, 0x3f800
   movgr2fr.d  a1,   $r0
   movgr2fr.w  ALPHA, TEMP
   CMPEQ   $fcc0, s1, a1
   fcvt.d.s   ALPHA, ALPHA
   bcnez   $fcc0, .L999
   fdiv.d  ALPHA, ALPHA, s1
   MOV max, s1
   MOV s1, a1
   MOV s2, a1
   MOV s3, a1
   MOV s4, a1
   srai.d  I, N, 2
   bge $r0,    I, .L105
   LD a1,  XX,   0 * SIZE
   LD a2,  XX,   1 * SIZE
   add.d   XX, XX, INCX
   LD a3,  XX,   0 * SIZE
   LD a4,  XX,   1 * SIZE
   add.d   XX, XX, INCX
   LD a5,  XX,   0 * SIZE
   LD a6,  XX,   1 * SIZE
   add.d   XX, XX, INCX
   LD a7,  XX,   0 * SIZE
   LD a8,  XX,   1 * SIZE
   addi.d  I, I, -1
   add.d  XX, XX, INCX
   bge $r0,    I, .L104
   .align 3

.L103:
   MUL t1, ALPHA, a1
   LD a1,  XX,   0 * SIZE
   MUL t2, ALPHA, a2
   addi.d  I, I, -1
   MUL t3, ALPHA, a3
   LD a2,  XX,   1 * SIZE
   MUL t4, ALPHA, a4
   add.d   XX, XX, INCX
   MADD  s1, t1, t1, s1
   LD a3,  XX,   0 * SIZE
   MADD  s2, t2, t2, s2
   NOP
   MADD  s3, t3, t3, s3
   LD a4,  XX,   1 * SIZE
   MADD  s4, t4, t4, s4
   add.d   XX, XX, INCX
   MUL t1, ALPHA, a5
   LD a5,  XX,   0 * SIZE
   MUL t2, ALPHA, a6
   NOP
   MUL t3, ALPHA, a7
   LD a6,  XX,   1 * SIZE
   MUL t4, ALPHA, a8
   add.d   XX, XX, INCX
   MADD  s1, t1, t1, s1
   LD a7,  XX,   0 * SIZE
   MADD  s2, t2, t2, s2
   LD a8,  XX,   1 * SIZE
   MADD  s3, t3, t3, s3
   add.d   XX, XX, INCX
   MADD  s4, t4, t4, s4
   blt $r0,    I, .L103
   .align 3

.L104:
   MUL t1, ALPHA, a1
   MUL t2, ALPHA, a2
   MUL t3, ALPHA, a3
   MUL t4, ALPHA, a4
   MADD  s1, t1, t1, s1
   MADD  s2, t2, t2, s2
   MADD  s3, t3, t3, s3
   MADD  s4, t4, t4, s4
   MUL t1, ALPHA, a5
   MUL t2, ALPHA, a6
   MUL t3, ALPHA, a7
   MUL t4, ALPHA, a8
   MADD  s1, t1, t1, s1
   MADD  s2, t2, t2, s2
   MADD  s3, t3, t3, s3
   MADD  s4, t4, t4, s4
   .align 3

.L105:
   andi    I,  N, 3
   bge $r0,    I, .L998
   .align  3

.L106:
   LD a1,  XX,   0 * SIZE
   LD a2,  XX,   1 * SIZE
   addi.d  I, I, -1
   MUL t1, ALPHA, a1
   MUL t2, ALPHA, a2
   MADD  s1, t1, t1, s1
   add.d   XX, XX, INCX
   MADD  s2, t2, t2, s2
   blt $r0,    I, .L106
   .align 3

.L998:
   ADD s1, s1, s2
   ADD s3, s3, s4
   ADD s1, s1, s3
   fsqrt.d s1, s1
   move $r4, $r17
   MUL $f0, max, s1
   jirl    $r0, $r1, 0x0
   .align 3

.L999:
   move $r4, $r17
   fmov.d $f0, $f22
   jirl    $r0, $r1, 0x0

   EPILOGUE
