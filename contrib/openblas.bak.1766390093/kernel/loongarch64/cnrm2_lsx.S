/***************************************************************************
Copyright (c) 2023, The OpenBLAS Project
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
#define t1     $r12
#define t2     $r13
#define t3     $r14
#define t4     $r15
#define a1     $f15
#define a2     $f16
#define res    $f19
#define VX0    $vr15
#define VX1    $vr16
#define VX2    $vr17
#define VX3    $vr18
#define VX4    $vr21
#define res1   $vr19
#define res2   $vr20
#define RCP    $f2
#define VALPHA $vr3

   PROLOGUE

#ifdef F_INTERFACE
   LDINT   N,     0(N)
   LDINT   INCX,  0(INCX)
#endif

   bge $r0, N, .L999
   beq $r0, INCX, .L999
   addi.d  $sp,     $sp,     -32
   st.d    $ra,     $sp,     0
   st.d    N,       $sp,     8
   st.d    X,       $sp,     16
   st.d    INCX,    $sp,     24
#ifdef DYNAMIC_ARCH
   bl camax_k_LA264
#else
   bl camax_k
#endif
   ld.d    $ra,     $sp,     0
   ld.d    N,       $sp,     8
   ld.d    X,       $sp,     16
   ld.d    INCX,    $sp,     24
   addi.d  $sp,     $sp,     32

   frecip.s RCP, $f0
   vreplvei.w   VALPHA, $vr2, 0
   vxor.v res1, res1, res1
   vxor.v res2, res2, res2
   fcmp.ceq.s  $fcc0, $f0,  $f19
   bcnez  $fcc0, .L999
   li.d  TEMP, 1
   slli.d TEMP, TEMP, ZBASE_SHIFT
   slli.d INCX, INCX, ZBASE_SHIFT
   srai.d I, N, 2
   bne INCX, TEMP, .L20
   bge $r0,  I, .L997
   .align 3

.L10:
   vld VX0, X, 0 * SIZE
   addi.d I, I, -1
   vld VX0, X, 0 * SIZE
   vld VX1, X, 4 * SIZE
   vfmul.s  VX0,  VX0,  VALPHA
   vfmul.s  VX1,  VX1,  VALPHA

   vfmadd.s res1, VX0, VX0, res1
   vfmadd.s res2, VX1, VX1, res2

   addi.d X, X, 8 * SIZE
   blt $r0, I, .L10
   b .L996
   .align 3

.L20:
   bge $r0, I, .L997
   .align 3

.L21:
   ld.w t1, X, 0 * SIZE
   ld.w t2, X, 1 * SIZE
   add.d X, X, INCX
   ld.w t3, X, 0 * SIZE
   ld.w t4, X, 1 * SIZE
   vinsgr2vr.w VX0, t1, 0
   vinsgr2vr.w VX0, t2, 1
   vinsgr2vr.w VX0, t3, 2
   vinsgr2vr.w VX0, t4, 3
   add.d X, X, INCX
   vfmul.s  VX0,  VX0,  VALPHA
   vfmadd.s res1, VX0, VX0, res1

   ld.w t1, X, 0 * SIZE
   ld.w t2, X, 1 * SIZE
   add.d X, X, INCX
   ld.w t3, X, 0 * SIZE
   ld.w t4, X, 1 * SIZE
   vinsgr2vr.w VX0, t1, 0
   vinsgr2vr.w VX0, t2, 1
   vinsgr2vr.w VX0, t3, 2
   vinsgr2vr.w VX0, t4, 3
   add.d X, X, INCX
   vfmul.s  VX0,  VX0,  VALPHA
   vfmadd.s res2, VX0, VX0, res2

   addi.d  I, I, -1
   blt $r0, I, .L21
   b .L996
   .align 3

.L996:
   vfadd.s res1, res1, res2
   vreplvei.w VX1, res1, 1
   vreplvei.w VX2, res1, 2
   vreplvei.w VX3, res1, 3
   vfadd.s res1, VX1, res1
   vfadd.s res1, VX2, res1
   vfadd.s res1, VX3, res1
   .align 3

.L997:
   andi I, N, 3
   bge $r0, I, .L999
   .align 3

.L998:
    fld.s a1, X, 0 * SIZE
    fld.s a2, X, 1 * SIZE
    addi.d I, I, -1
    fmul.s a1, a1, RCP
    fmul.s a2, a2, RCP
    fmadd.s res, a1, a1, res
    fmadd.s res, a2, a2, res
    add.d X, X, INCX
    blt $r0, I, .L998
    .align 3

.L999:
    fsqrt.s res, res
    fmul.s $f0, res, $f0
    move $r4, $r17
    jirl $r0, $r1, 0x0
    .align 3

    EPILOGUE
