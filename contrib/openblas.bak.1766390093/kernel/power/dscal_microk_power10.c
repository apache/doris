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

#define HAVE_KERNEL_8 1

static void dscal_kernel_8 (long n, double *x, double alpha)
{
  __asm__
    (
       "dcbt		0, %2		\n\t"

       XXSPLTD_S(48,%x3,0)

       "lxvp		32, 0(%2)	\n\t"
       "lxvp		34, 32(%2)	\n\t"
       "lxvp		36, 64(%2)	\n\t"
       "lxvp		38, 96(%2)	\n\t"

       "addic.		%1, %1, -16	\n\t"
       "ble		two%=		\n\t"

       ".align	5		\n"
     "one%=:				\n\t"

       "xvmuldp		40, 32, 48	\n\t"
       "xvmuldp		41, 33, 48	\n\t"
       "xvmuldp		42, 34, 48	\n\t"
       "xvmuldp		43, 35, 48	\n\t"
       "lxvp		32, 128(%2)	\n\t"
       "lxvp		34, 160(%2)	\n\t"
       "xvmuldp		44, 36, 48	\n\t"
       "xvmuldp		45, 37, 48	\n\t"
       "xvmuldp		46, 38, 48	\n\t"
       "xvmuldp		47, 39, 48	\n\t"
       "lxvp		36, 192(%2)	\n\t"
       "lxvp		38, 224(%2)	\n\t"

#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
       "stxv		40, 0(%2)   \n\t"
       "stxv		41, 16(%2)  \n\t"
       "stxv		42, 32(%2)  \n\t"
       "stxv		43, 48(%2)  \n\t"
       "stxv		44, 64(%2)   \n\t"
       "stxv		45, 80(%2)  \n\t"
       "stxv		46, 96(%2)  \n\t"
       "stxv		47, 112(%2)  \n\t"
#else
       "stxv		41, 0(%2)   \n\t"
       "stxv		40, 16(%2)  \n\t"
       "stxv		43, 32(%2)  \n\t"
       "stxv		42, 48(%2)  \n\t"
       "stxv		45, 64(%2)   \n\t"
       "stxv		44, 80(%2)  \n\t"
       "stxv		47, 96(%2)  \n\t"
       "stxv		46, 112(%2)  \n\t"
#endif

       "addi		%2, %2, 128	\n\t"

       "addic.		%1, %1, -16	\n\t"
       "bgt		one%=		\n"

     "two%=:				\n\t"

       "xvmuldp		40, 32, 48	\n\t"
       "xvmuldp		41, 33, 48	\n\t"
       "xvmuldp		42, 34, 48	\n\t"
       "xvmuldp		43, 35, 48	\n\t"

       "xvmuldp		44, 36, 48	\n\t"
       "xvmuldp		45, 37, 48	\n\t"
       "xvmuldp		46, 38, 48	\n\t"
       "xvmuldp		47, 39, 48	\n\t"

#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
       "stxv		40, 0(%2)   \n\t"
       "stxv		41, 16(%2)  \n\t"
       "stxv		42, 32(%2)  \n\t"
       "stxv		43, 48(%2)  \n\t"
       "stxv		44, 64(%2)   \n\t"
       "stxv		45, 80(%2)  \n\t"
       "stxv		46, 96(%2)  \n\t"
       "stxv		47, 112(%2)  \n\t"
#else
       "stxv		41, 0(%2)   \n\t"
       "stxv		40, 16(%2)  \n\t"
       "stxv		43, 32(%2)  \n\t"
       "stxv		42, 48(%2)  \n\t"
       "stxv		45, 64(%2)   \n\t"
       "stxv		44, 80(%2)  \n\t"
       "stxv		47, 96(%2)  \n\t"
       "stxv		46, 112(%2)  \n\t"
#endif

     "#n=%1 alpha=%3 x=%0=%2"
     :
       "+m" (*x),
       "+r" (n),	// 1
       "+b" (x)		// 2
     :
       "d" (alpha)	// 3
     :
       "cr0",
       "vs32","vs33","vs34","vs35","vs36","vs37","vs38","vs39",
       "vs40","vs41","vs42","vs43","vs44","vs45","vs46","vs47","vs48"
     );
}


static void dscal_kernel_8_zero (long n, double *x)
{

  __asm__
    (
       "xxlxor		32, 32, 32	\n\t"
       "xxlxor		33, 33, 33	\n\t"

       ".align	5		\n"
     "one%=:				\n\t"

       "stxv		32, 0(%2)	\n\t"
       "stxv		32, 16(%2)	\n\t"
       "stxv		32, 32(%2)	\n\t"
       "stxv		32, 48(%2)	\n\t"
       "stxv		32, 64(%2)	\n\t"
       "stxv		32, 80(%2)	\n\t"
       "stxv		32, 96(%2)	\n\t"
       "stxv		32, 112(%2)	\n\t"

       "addi		%2, %2, 128	\n\t"

       "addic.		%1, %1, -16	\n\t"
       "bgt		one%=		\n"

     "#n=%1 x=%0=%2 "
     :
       "=m" (*x),
       "+r" (n),	// 1
       "+b" (x)	// 2
     :
     :
       "cr0","vs32","vs33"
     );
}
