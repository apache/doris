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
#define HAVE_KERNEL_32 1

#if defined(DOUBLE)
static void dswap_kernel_32 (long n, double *x, double *y)
#else
static void sswap_kernel_32 (long n, float *x, float *y)
#endif
{
  __asm__
    (
       ".align	5		\n"
     "one%=:				\n\t"

       "lxvp		32, 0(%4)	\n\t"
       "lxvp		34, 32(%4)	\n\t"
       "lxvp		36, 64(%4)	\n\t"
       "lxvp		38, 96(%4)	\n\t"

       "lxvp		40, 128(%4)	\n\t"
       "lxvp		42, 160(%4)	\n\t"
       "lxvp		44, 192(%4)	\n\t"
       "lxvp		46, 224(%4)	\n\t"

       "lxvp		48, 0(%3)	\n\t"
       "lxvp		50, 32(%3)	\n\t"
       "lxvp		52, 64(%3)	\n\t"
       "lxvp		54, 96(%3)	\n\t"

       "lxvp		56, 128(%3)	\n\t"
       "lxvp		58, 160(%3)	\n\t"
       "lxvp		60, 192(%3)	\n\t"
       "lxvp		62, 224(%3)	\n\t"

#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
       "stxv		32, 0(%3)	\n\t"
       "stxv		33, 16(%3)	\n\t"
       "stxv		34, 32(%3)	\n\t"
       "stxv		35, 48(%3)	\n\t"
       "stxv		36, 64(%3)	\n\t"
       "stxv		37, 80(%3)	\n\t"
       "stxv		38, 96(%3)	\n\t"
       "stxv		39, 112(%3)	\n\t"

       "stxv		40, 128(%3)	\n\t"
       "stxv		41, 144(%3)	\n\t"
       "stxv		42, 160(%3)	\n\t"
       "stxv		43, 176(%3)	\n\t"
       "stxv		44, 192(%3)	\n\t"
       "stxv		45, 208(%3)	\n\t"
       "stxv		46, 224(%3)	\n\t"
       "stxv		47, 240(%3)	\n\t"

       "stxv		48, 0(%4)	\n\t"
       "stxv		49, 16(%4)	\n\t"
       "stxv		50, 32(%4)	\n\t"
       "stxv		51, 48(%4)	\n\t"
       "stxv		52, 64(%4)	\n\t"
       "stxv		53, 80(%4)	\n\t"
       "stxv		54, 96(%4)	\n\t"
       "stxv		55, 112(%4)	\n\t"

       "stxv		56, 128(%4)	\n\t"
       "stxv		57, 144(%4)	\n\t"
       "stxv		58, 160(%4)	\n\t"
       "stxv		59, 176(%4)	\n\t"
       "stxv		60, 192(%4)	\n\t"
       "stxv		61, 208(%4)	\n\t"
       "stxv		62, 224(%4)	\n\t"
       "stxv		63, 240(%4)	\n\t"
#else
       "stxv		33, 0(%3)	\n\t"
       "stxv		32, 16(%3)	\n\t"
       "stxv		35, 32(%3)	\n\t"
       "stxv		34, 48(%3)	\n\t"
       "stxv		37, 64(%3)	\n\t"
       "stxv		36, 80(%3)	\n\t"
       "stxv		39, 96(%3)	\n\t"
       "stxv		38, 112(%3)	\n\t"

       "stxv		41, 128(%3)	\n\t"
       "stxv		40, 144(%3)	\n\t"
       "stxv		43, 160(%3)	\n\t"
       "stxv		42, 176(%3)	\n\t"
       "stxv		45, 192(%3)	\n\t"
       "stxv		44, 208(%3)	\n\t"
       "stxv		47, 224(%3)	\n\t"
       "stxv		46, 240(%3)	\n\t"

       "stxv		49, 0(%4)	\n\t"
       "stxv		48, 16(%4)	\n\t"
       "stxv		51, 32(%4)	\n\t"
       "stxv		50, 48(%4)	\n\t"
       "stxv		53, 64(%4)	\n\t"
       "stxv		52, 80(%4)	\n\t"
       "stxv		55, 96(%4)	\n\t"
       "stxv		54, 112(%4)	\n\t"

       "stxv		57, 128(%4)	\n\t"
       "stxv		56, 144(%4)	\n\t"
       "stxv		59, 160(%4)	\n\t"
       "stxv		58, 176(%4)	\n\t"
       "stxv		61, 192(%4)	\n\t"
       "stxv		60, 208(%4)	\n\t"
       "stxv		63, 224(%4)	\n\t"
       "stxv		62, 240(%4)	\n\t"
#endif

       "addi		%4, %4, 256	\n\t"
       "addi		%3, %3, 256	\n\t"

#if defined(DOUBLE)
       "addic.		%2, %2, -32	\n\t"
#else
       "addic.		%2, %2, -64	\n\t"
#endif
       "bgt		one%=		\n"

     "#n=%2 x=%0=%3 y=%1=%4"
     :
       "+m" (*x),
       "+m" (*y),
       "+r" (n),	// 2
       "+b" (x),	// 3
       "+b" (y)		// 4
     :
     :
       "cr0",
       "vs32","vs33","vs34","vs35","vs36","vs37","vs38","vs39",
       "vs40","vs41","vs42","vs43","vs44","vs45","vs46","vs47",
       "vs48","vs49","vs50","vs51","vs52","vs53","vs54","vs55",
       "vs56","vs57","vs58","vs59","vs60","vs61","vs62","vs63"
     );
}
