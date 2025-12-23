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

#define HAVE_KERNEL_16 1

static float sdot_kernel_16 (long n, float *x, float *y)
{
  float dot;

  __asm__
    (
       "dcbt		0, %2		\n\t"
       "dcbt		0, %3		\n\t"

       "xxlxor		32, 32,	32	\n\t"
       "xxlxor		33, 33,	33	\n\t"
       "xxlxor		34, 34,	34	\n\t"
       "xxlxor		35, 35,	35	\n\t"
       "xxlxor		36, 36,	36	\n\t"
       "xxlxor		37, 37,	37	\n\t"
       "xxlxor		38, 38,	38	\n\t"
       "xxlxor		39, 39,	39	\n\t"

       "lxvp		40, 0(%2)	\n\t"
       "lxvp		42, 32(%2)	\n\t"
       "lxvp		44, 64(%2)	\n\t"
       "lxvp		46, 96(%2)	\n\t"
       "lxvp		48, 0(%3)	\n\t"
       "lxvp		50, 32(%3)	\n\t"
       "lxvp		52, 64(%3)	\n\t"
       "lxvp		54, 96(%3)	\n\t"

       "addi		%2, %2, 128	\n\t"
       "addi		%3, %3, 128	\n\t"

       "addic.		%1, %1, -32	\n\t"
       "ble		two%=		\n\t"

       ".align	5		\n"
     "one%=:				\n\t"

       "xvmaddasp	32, 40, 48	\n\t"
       "xvmaddasp	33, 41, 49	\n\t"
       "lxvp		40, 0(%2)	\n\t"
       "lxvp		48, 0(%3)	\n\t"
       "xvmaddasp	34, 42, 50	\n\t"
       "xvmaddasp	35, 43, 51	\n\t"
       "lxvp		42, 32(%2)	\n\t"
       "lxvp		50, 32(%3)	\n\t"
       "xvmaddasp	36, 44, 52	\n\t"
       "xvmaddasp	37, 45, 53	\n\t"
       "lxvp		44, 64(%2)	\n\t"
       "lxvp		52, 64(%3)	\n\t"
       "xvmaddasp	38, 46, 54 	\n\t"
       "xvmaddasp	39, 47, 55 	\n\t"
       "lxvp		46, 96(%2)	\n\t"
       "lxvp		54, 96(%3)	\n\t"

       "addi		%2, %2, 128	\n\t"
       "addi		%3, %3, 128	\n\t"

       "addic.		%1, %1, -32	\n\t"
       "bgt		one%=		\n"

     "two%=:				\n\t"

       "xvmaddasp	32, 40, 48	\n\t"
       "xvmaddasp	33, 41, 49	\n\t"
       "xvmaddasp	34, 42, 50	\n\t"
       "xvmaddasp	35, 43, 51	\n\t"
       "xvmaddasp	36, 44, 52	\n\t"
       "xvmaddasp	37, 45, 53	\n\t"
       "xvmaddasp	38, 46, 54	\n\t"
       "xvmaddasp	39, 47, 55	\n\t"

       "xvaddsp		32, 32, 33	\n\t"
       "xvaddsp		34, 34, 35	\n\t"
       "xvaddsp		36, 36, 37	\n\t"
       "xvaddsp		38, 38, 39	\n\t"

       "xvaddsp		32, 32, 34	\n\t"
       "xvaddsp		36, 36, 38	\n\t"

       "xvaddsp		32, 32, 36	\n\t"

       "xxsldwi		33, 32, 32, 2	\n\t"
       "xvaddsp		32, 32, 33	\n\t"

       "xxsldwi		33, 32, 32, 1	\n\t"
       "xvaddsp		32, 32, 33	\n\t"

       "xscvspdp	%x0, 32		\n"

     "#dot=%0 n=%1 x=%4=%2 y=%5=%3\n"
     :
       "=f" (dot),	// 0
       "+r" (n),	// 1
       "+b" (x),	// 2
       "+b" (y)		// 3
     :
       "m" (*x),
       "m" (*y)
     :
       "cr0",
       "vs32","vs33","vs34","vs35","vs36","vs37","vs38","vs39",
       "vs40","vs41","vs42","vs43","vs44","vs45","vs46","vs47",
       "vs48","vs49","vs50","vs51","vs52","vs53","vs54","vs55"
     );

  return dot;
}
