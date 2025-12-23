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
static void caxpy_kernel_8 (long n, float *x, float *y,
			    float alpha_r, float alpha_i)
{
#if !defined(CONJ)
  static const float mvec[4] = { -1.0, 1.0, -1.0, 1.0 };
#else
  static const float mvec[4] = { 1.0, -1.0, 1.0, -1.0 };
#endif
  const float *mvecp = mvec;
  /* We have to load reverse mask for big endian.  */
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
  __vector unsigned char mask={ 4,5,6,7,0,1,2,3,12,13,14,15,8,9,10,11}; 
#else
  __vector unsigned char mask = { 11,10,9,8,15,14,13,12,3,2,1,0,7,6,5,4};
#endif

  long ytmp;

  __asm__
    (
       "xscvdpspn 32, %7    \n\t"
       "xscvdpspn 33, %8    \n\t"
       "xxspltw 32, 32, 0   \n\t"
       "xxspltw 33, 33, 0   \n\t"
       "lxvd2x          36, 0, %9       \n\t"   // mvec

#if !defined(CONJ)
       "xvmulsp		33, 33, 36	\n\t"	// alpha_i * mvec
#else
       "xvmulsp		32, 32, 36	\n\t"	// alpha_r * mvec
#endif
       "mr		%4, %3		\n\t"
       "dcbt		0, %2		\n\t"
       "dcbt		0, %3		\n\t"

       "lxvp		40, 0(%2)	\n\t"	// x0
       "lxvp		42, 32(%2)	\n\t"	// x2
       "lxvp		48, 0(%3)	\n\t"	// y0
       "lxvp		50, 32(%3)	\n\t"	// y2

       "xxperm 52, 40, %x10 \n\t"       // exchange real and imag part
       "xxperm 53, 41, %x10 \n\t"       // exchange real and imag part
       "xxperm 54, 42, %x10 \n\t"       // exchange real and imag part
       "xxperm 55, 43, %x10 \n\t"       // exchange real and imag part

       "lxvp		44, 64(%2)	\n\t"	// x4
       "lxvp		46, 96(%2)	\n\t"	// x6
       "lxvp		34, 64(%3)	\n\t"	// y4
       "lxvp		38, 96(%3)	\n\t"	// y6

       "xxperm 56, 44, %x10 \n\t"       // exchange real and imag part
       "xxperm 57, 45, %x10 \n\t"       // exchange real and imag part
       "xxperm 58, 46, %x10 \n\t"       // exchange real and imag part
       "xxperm 59, 47, %x10 \n\t"       // exchange real and imag part

       "addi		%2, %2, 128	\n\t"
       "addi		%3, %3, 128	\n\t"

       "addic.		%1, %1, -16	\n\t"
       "ble		two%=		\n\t"

       ".align	5		\n"
       "one%=:				\n\t"

       "xvmaddasp	48, 40, 32	\n\t"	// alpha_r * x0_r , alpha_r * x0_i
       "xvmaddasp	49, 41, 32	\n\t"
       "lxvp		40, 0(%2)	\n\t"	// x0
       "xvmaddasp	50, 42, 32	\n\t"
       "xvmaddasp	51, 43, 32	\n\t"
       "lxvp		42, 32(%2)	\n\t"	// x2

       "xvmaddasp	34, 44, 32	\n\t"
       "xvmaddasp	35, 45, 32	\n\t"
       "lxvp		44, 64(%2)	\n\t"	// x4
       "xvmaddasp	38, 46, 32	\n\t"
       "xvmaddasp	39, 47, 32	\n\t"
       "lxvp		46, 96(%2)	\n\t"	// x6

       "xvmaddasp	48, 52, 33	\n\t"	// alpha_i * x0_i , alpha_i * x0_r
       "addi		%2, %2, 128	\n\t"
       "xvmaddasp	49, 53, 33	\n\t"
       "xvmaddasp	50, 54, 33	\n\t"
       "xvmaddasp	51, 55, 33	\n\t"

       "xvmaddasp	34, 56, 33	\n\t"
       "xvmaddasp	35, 57, 33	\n\t"
       "xvmaddasp	38, 58, 33	\n\t"
       "xvmaddasp	39, 59, 33	\n\t"

#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
       "stxv        48, 0(%4)   \n\t"
       "stxv        49, 16(%4)  \n\t"
       "stxv        50, 32(%4)  \n\t"
       "stxv        51, 48(%4)  \n\t"
       "stxv        34, 64(%4)  \n\t"
       "stxv        35, 80(%4)  \n\t"
       "stxv        38, 96(%4)  \n\t"
       "stxv        39, 112(%4) \n\t"
#else 
       "stxv		49, 0(%4)	\n\t"
       "stxv		48, 16(%4)	\n\t"
       "stxv		51, 32(%4)	\n\t"
       "stxv		50, 48(%4)	\n\t"
       "stxv		35, 64(%4)	\n\t"
       "stxv		34, 80(%4)	\n\t"
       "stxv		39, 96(%4)	\n\t"
       "stxv		38, 112(%4)	\n\t"
#endif

       "addi		%4, %4, 128	\n\t"
       "xxperm 52, 40, %x10 \n\t"       // exchange real and imag part
       "xxperm 53, 41, %x10 \n\t"       // exchange real and imag part

       "lxvp		48, 0(%3)	\n\t"	// y0
       "xxperm 54, 42, %x10 \n\t"       // exchange real and imag part
       "xxperm 55, 43, %x10 \n\t"       // exchange real and imag part
       "lxvp		50, 32(%3)	\n\t"	// y2

       "xxperm 56, 44, %x10 \n\t"       // exchange real and imag part
       "xxperm 57, 45, %x10 \n\t"       // exchange real and imag part
       "lxvp		34, 64(%3)	\n\t"	// y4
       "xxperm 58, 46, %x10 \n\t"       // exchange real and imag part
       "xxperm 59, 47, %x10 \n\t"       // exchange real and imag part
       "lxvp		38, 96(%3)	\n\t"	// y6

       "addi		%3, %3, 128	\n\t"

       "addic.		%1, %1, -16	\n\t"
       "bgt		one%=		\n"

       "two%=:				\n\t"
       "xvmaddasp	48, 40, 32	\n\t"	// alpha_r * x0_r , alpha_r * x0_i
       "xvmaddasp	49, 41, 32	\n\t"
       "xvmaddasp	50, 42, 32	\n\t"
       "xvmaddasp	51, 43, 32	\n\t"

       "xvmaddasp	34, 44, 32	\n\t"
       "xvmaddasp	35, 45, 32	\n\t"
       "xvmaddasp	38, 46, 32	\n\t"
       "xvmaddasp	39, 47, 32	\n\t"

       "xvmaddasp	48, 52, 33	\n\t"	// alpha_i * x0_i , alpha_i * x0_r
       "xvmaddasp	49, 53, 33	\n\t"
       "xvmaddasp	50, 54, 33	\n\t"
       "xvmaddasp	51, 55, 33	\n\t"

       "xvmaddasp	34, 56, 33	\n\t"
       "xvmaddasp	35, 57, 33	\n\t"
       "xvmaddasp	38, 58, 33	\n\t"
       "xvmaddasp	39, 59, 33	\n\t"

#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
       "stxv        48, 0(%4)   \n\t"
       "stxv        49, 16(%4)  \n\t"
       "stxv        50, 32(%4)  \n\t"
       "stxv        51, 48(%4)  \n\t"
       "stxv        34, 64(%4)  \n\t"
       "stxv        35, 80(%4)  \n\t"
       "stxv        38, 96(%4)  \n\t"
       "stxv        39, 112(%4) \n\t"
#else
       "stxv		49, 0(%4)	\n\t"
       "stxv		48, 16(%4)	\n\t"
       "stxv		51, 32(%4)	\n\t"
       "stxv		50, 48(%4)	\n\t"
       "stxv		35, 64(%4)	\n\t"
       "stxv		34, 80(%4)	\n\t"
       "stxv		39, 96(%4)	\n\t"
       "stxv		38, 112(%4)	\n\t"
#endif

     "#n=%1 x=%5=%2 y=%0=%3 alpha=(%7,%8) mvecp=%6=%9 ytmp=%4\n"
     :
       "+m" (*y),
       "+r" (n),	// 1
       "+b" (x),	// 2
       "+b" (y),	// 3
       "=b" (ytmp)	// 4 
     :
       "m" (*x),
       "m" (*mvecp),
       "d" (alpha_r),	// 7
       "d" (alpha_i),	// 8
       "4" (mvecp),	// 9
       "wa" (mask)
     :
       "cr0",
       "vs32","vs33","vs34","vs35","vs36","vs37","vs38","vs39",
       "vs40","vs41","vs42","vs43","vs44","vs45","vs46","vs47",
       "vs48","vs49","vs50","vs51","vs52","vs53","vs54","vs55",
       "vs56","vs57","vs58","vs59"
     );
}
