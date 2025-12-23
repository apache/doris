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

static void zscal_kernel_8 (long n, float *x, float alpha_r, float alpha_i)
{
  __vector float t0 = {-alpha_i, alpha_i, -alpha_i, alpha_i};
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
  __vector unsigned char mask = {4,5,6,7,0,1,2,3,12,13,14,15,8,9,10,11};
#else
  __vector unsigned char mask = { 11,10,9,8,15,14,13,12,3,2,1,0,7,6,5,4};
#endif
  __asm__
    (
       "dcbt		0, %2		\n\t"
       "xscvdpspn	32, %x3    \n\t"
       "xxspltw		32, 32, 0	\n\t"

       "lxvp		40, 0(%2)	\n\t"
       "lxvp		42, 32(%2)	\n\t"
       "lxvp		44, 64(%2)	\n\t"
       "lxvp		46, 96(%2)	\n\t"

       "addic.		%1, %1, -16	\n\t"
       "ble		two%=		\n\t"

       ".align	5		\n"
     "one%=:				\n\t"

       "xvmulsp		48, 40, 32	\n\t"	// x0_r * alpha_r, x0_i * alpha_r
       "xvmulsp		49, 41, 32	\n\t"
       "xvmulsp		50, 42, 32	\n\t"
       "xvmulsp		51, 43, 32	\n\t"
       "xvmulsp		52, 44, 32	\n\t"
       "xvmulsp		53, 45, 32	\n\t"
       "xvmulsp		54, 46, 32	\n\t"
       "xvmulsp		55, 47, 32	\n\t"

       "xxperm 34, 40, %x5 \n\t"
       "xxperm 35, 41, %x5 \n\t"
       "xxperm 36, 42, %x5 \n\t"
       "xxperm 37, 43, %x5 \n\t"
       "xxperm 38, 44, %x5 \n\t"
       "xxperm 39, 45, %x5 \n\t"
       "xxperm 56, 46, %x5 \n\t"
       "xxperm 57, 47, %x5 \n\t"

       "xvmulsp		34, 34, %x4	\n\t"	// x0_i * -alpha_i, x0_r * alpha_i
       "xvmulsp		35, 35, %x4	\n\t"

       "lxvp		40, 128(%2)	\n\t"

       "xvmulsp		36, 36, %x4	\n\t"
       "xvmulsp		37, 37, %x4	\n\t"

       "lxvp		42, 160(%2)	\n\t"

       "xvmulsp		38, 38, %x4	\n\t"
       "xvmulsp		39, 39, %x4	\n\t"

       "lxvp		44, 192(%2)	\n\t"

       "xvmulsp		56, 56, %x4	\n\t"
       "xvmulsp		57, 57, %x4	\n\t"

       "lxvp		46, 224(%2)	\n\t"

       "xvaddsp		48, 48, 34	\n\t"
       "xvaddsp		49, 49, 35	\n\t"
       "xvaddsp		50, 50, 36	\n\t"
       "xvaddsp		51, 51, 37	\n\t"

#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
       "stxv		48, 0(%2)   \n\t"
       "stxv		49, 16(%2)  \n\t"
#else
       "stxv		49, 0(%2)	\n\t"
       "stxv		48, 16(%2)	\n\t"
#endif
       "xvaddsp		52, 52, 38	\n\t"
       "xvaddsp		53, 53, 39	\n\t"

#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
       "stxv		50, 32(%2)  \n\t"
       "stxv		51, 48(%2)  \n\t"
#else
       "stxv		51, 32(%2)	\n\t"
       "stxv		50, 48(%2)	\n\t"
#endif

       "xvaddsp		54, 54, 56	\n\t"
       "xvaddsp		55, 55, 57	\n\t"

#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
       "stxv		52, 64(%2)	\n\t"
       "stxv		53, 80(%2)	\n\t"
       "stxv		54, 96(%2)	\n\t"
       "stxv		55, 112(%2)	\n\t"
#else
       "stxv		53, 64(%2)	\n\t"
       "stxv		52, 80(%2)	\n\t"
       "stxv		55, 96(%2)	\n\t"
       "stxv		54, 112(%2)	\n\t"
#endif

       "addi		%2, %2, 128	\n\t"

       "addic.		%1, %1, -16	\n\t"
       "bgt		one%=		\n"

     "two%=:				\n\t"

       "xvmulsp		48, 40, 32	\n\t"	// x0_r * alpha_r, x0_i * alpha_r
       "xvmulsp		49, 41, 32	\n\t"
       "xvmulsp		50, 42, 32	\n\t"
       "xvmulsp		51, 43, 32	\n\t"
       "xvmulsp		52, 44, 32	\n\t"
       "xvmulsp		53, 45, 32	\n\t"
       "xvmulsp		54, 46, 32	\n\t"
       "xvmulsp		55, 47, 32	\n\t"

       "xxperm 34, 40, %x5 \n\t"
       "xxperm 35, 41, %x5 \n\t"
       "xxperm 36, 42, %x5 \n\t"
       "xxperm 37, 43, %x5 \n\t"
       "xxperm 38, 44, %x5 \n\t"
       "xxperm 39, 45, %x5 \n\t"
       "xxperm 56, 46, %x5 \n\t"
       "xxperm 57, 47, %x5 \n\t"


       "xvmulsp		34, 34, %x4	\n\t"	// x0_i * -alpha_i, x0_r * alpha_i
       "xvmulsp		35, 35, %x4	\n\t"
       "xvmulsp		36, 36, %x4	\n\t"
       "xvmulsp		37, 37, %x4	\n\t"
       "xvmulsp		38, 38, %x4	\n\t"
       "xvmulsp		39, 39, %x4	\n\t"
       "xvmulsp		56, 56, %x4	\n\t"
       "xvmulsp		57, 57, %x4	\n\t"

       "xvaddsp		48, 48, 34	\n\t"
       "xvaddsp		49, 49, 35	\n\t"
       "xvaddsp		50, 50, 36	\n\t"
       "xvaddsp		51, 51, 37	\n\t"

#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
       "stxv		48, 0(%2)   \n\t"
       "stxv		49, 16(%2)  \n\t"
#else
       "stxv		49, 0(%2)	\n\t"
       "stxv		48, 16(%2)	\n\t"
#endif

       "xvaddsp		52, 52, 38	\n\t"
       "xvaddsp		53, 53, 39	\n\t"

#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
       "stxv		50, 32(%2)  \n\t"
       "stxv		51, 48(%2)  \n\t"
#else
       "stxv		51, 32(%2)	\n\t"
       "stxv		50, 48(%2)	\n\t"
#endif

       "xvaddsp		54, 54, 56	\n\t"
       "xvaddsp		55, 55, 57	\n\t"

#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
       "stxv		52, 64(%2)	\n\t"
       "stxv		53, 80(%2)	\n\t"
       "stxv		54, 96(%2)	\n\t"
       "stxv		55, 112(%2)	\n\t"
#else
       "stxv		53, 64(%2)	\n\t"
       "stxv		52, 80(%2)	\n\t"
       "stxv		55, 96(%2)	\n\t"
       "stxv		54, 112(%2)	\n\t"
#endif

     "#n=%1 x=%0=%2 alpha=(%3,%4)\n"
     :
       "+m" (*x),
       "+r" (n),	// 1
       "+b" (x)		// 2
     :
       "f" (alpha_r),	// 3 
       "wa" (t0),	// 4 
       "wa" (mask)	// 5
     :
       "cr0",
       "vs32","vs33","vs34","vs35","vs36","vs37","vs38","vs39",
       "vs40","vs41","vs42","vs43","vs44","vs45","vs46","vs47",
       "vs48","vs49","vs50","vs51","vs52","vs53","vs54","vs55",
       "vs56","vs57"
     );
}
