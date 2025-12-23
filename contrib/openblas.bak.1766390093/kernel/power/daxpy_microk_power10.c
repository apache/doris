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

#define HAVE_KERNEL_8 1

static void daxpy_kernel_8 (long n, double *x, double *y, double alpha)
{
  __vector double t0;

  __asm__
    (
       XXSPLTD_S(%x4,%x6,0)

       "dcbt		0, %2		\n\t"
       "dcbt		0, %3		\n\t"

       "lxvp		32, 0(%2)	\n\t"
       "lxvp		34, 32(%2)	\n\t"
       "lxvp		40, 64(%2)	\n\t"
       "lxvp		42, 96(%2)	\n\t"

       "lxvp		36, 0(%3)	\n\t"
       "lxvp		38, 32(%3)	\n\t"
       "lxvp		44, 64(%3)	\n\t"
       "lxvp		46, 96(%3)	\n\t"

       "addi		%2, %2, 128	\n\t"

       "addic.		%1, %1, -16	\n\t"
       "ble		two%=		\n\t"

       ".align 5			\n"
     "one%=:				\n\t"

       "xvmaddadp	36, 32, %x4	\n\t"
       "xvmaddadp	37, 33, %x4	\n\t"

       "lxvp		32, 0(%2)	\n\t"
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
       "stxv		36, 0(%3)	\n\t"
       "stxv		37, 16(%3)	\n\t"
#else
       "stxv		37, 0(%3)	\n\t"
       "stxv		36, 16(%3)	\n\t"
#endif

       "xvmaddadp	38, 34, %x4	\n\t"
       "xvmaddadp	39, 35, %x4	\n\t"

       "lxvp		34, 32(%2)	\n\t"
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
       "stxv		38, 32(%3)	\n\t"
       "stxv		39, 48(%3)	\n\t"
#else
       "stxv		39, 32(%3)	\n\t"
       "stxv		38, 48(%3)	\n\t"
#endif

       "lxvp		36, 128(%3)	\n\t"
       "lxvp		38, 160(%3)	\n\t"

       "xvmaddadp	44, 40, %x4	\n\t"
       "xvmaddadp	45, 41, %x4	\n\t"

       "lxvp		40, 64(%2)	\n\t"
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
       "stxv		44, 64(%3)	\n\t"
       "stxv		45, 80(%3)	\n\t"
#else
       "stxv		45, 64(%3)	\n\t"
       "stxv		44, 80(%3)	\n\t"
#endif

       "xvmaddadp	46, 42, %x4	\n\t"
       "xvmaddadp	47, 43, %x4	\n\t"

       "lxvp		42, 96(%2)	\n\t"
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
       "stxv		46, 96(%3)	\n\t"
       "stxv		47, 112(%3)	\n\t"
#else
       "stxv		47, 96(%3)	\n\t"
       "stxv		46, 112(%3)	\n\t"
#endif

       "addi		%2, %2, 128	\n\t"
       "addi		%3, %3, 128	\n\t"

       "lxvp		44, 64(%3)	\n\t"
       "lxvp		46, 96(%3)	\n\t"

       "addic.		%1, %1, -16	\n\t"
       "bgt		one%=		\n"

     "two%=:				\n\t"

       "xvmaddadp	36, 32, %x4	\n\t"
       "xvmaddadp	37, 33, %x4	\n\t"
       "xvmaddadp	38, 34, %x4	\n\t"
       "xvmaddadp	39, 35, %x4	\n\t"

       "xvmaddadp	44, 40, %x4	\n\t"
       "xvmaddadp	45, 41, %x4	\n\t"
       "xvmaddadp	46, 42, %x4	\n\t"
       "xvmaddadp	47, 43, %x4	\n\t"

#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
       "stxv		36, 0(%3)	\n\t"
       "stxv		37, 16(%3)	\n\t"
       "stxv		38, 32(%3)	\n\t"
       "stxv		39, 48(%3)	\n\t"
       "stxv		44, 64(%3)	\n\t"
       "stxv		45, 80(%3)	\n\t"
       "stxv		46, 96(%3)	\n\t"
       "stxv		47, 112(%3)	\n\t"
#else
       "stxv		37, 0(%3)	\n\t"
       "stxv		36, 16(%3)	\n\t"
       "stxv		39, 32(%3)	\n\t"
       "stxv		38, 48(%3)	\n\t"
       "stxv		45, 64(%3)	\n\t"
       "stxv		44, 80(%3)	\n\t"
       "stxv		47, 96(%3)	\n\t"
       "stxv		46, 112(%3)	\n\t"
#endif

     "#n=%1 x=%5=%2 y=%0=%3 alpha=%6 t0=%x4\n"
     :
       "+m" (*y),
       "+r" (n),	// 1
       "+b" (x),	// 2
       "+b" (y),	// 3
       "=wa" (t0)	// 4
     :
       "m" (*x),
       "d" (alpha)	// 6 
     :
       "cr0",
       "vs32","vs33","vs34","vs35","vs36","vs37", "vs38", "vs39",
       "vs40","vs41","vs42","vs43","vs44","vs45","vs46","vs47"
     );

}


