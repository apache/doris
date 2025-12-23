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

static void zscal_kernel_8 (long n, double *x, double alpha_r, double alpha_i)
{
  __vector double t0;
  __vector double t1;
  __vector double t2;
  __vector double t3;
  __vector double t4;
  __vector double t5;

  __asm__
    (
       "dcbt		0, %2		\n\t"

       "xsnegdp		33, %x10	\n\t"	// -alpha_i
       XXSPLTD_S(32,%x9,0)	// alpha_r , alpha_r
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
       XXMRGHD_S(33,33, %x10) // -alpha_i , alpha_i
#else
       XXMRGHD_S(33,%x10, 33)	// -alpha_i , alpha_i
#endif

       "lxvp		40, 0(%2)	\n\t"
       "lxvp		42, 32(%2)	\n\t"
       "lxvp		44, 64(%2)	\n\t"
       "lxvp		46, 96(%2)	\n\t"

       "addic.		%1, %1, -8	\n\t"
       "ble		two%=		\n\t"

       ".align	5		\n"
     "one%=:				\n\t"

       "xvmuldp		48, 40, 32	\n\t"	// x0_r * alpha_r, x0_i * alpha_r
       "xvmuldp		49, 41, 32	\n\t"
       "xvmuldp		50, 42, 32	\n\t"
       "xvmuldp		51, 43, 32	\n\t"
       "xvmuldp		34, 44, 32	\n\t"
       "xvmuldp		35, 45, 32	\n\t"
       "xvmuldp		36, 46, 32	\n\t"
       "xvmuldp		37, 47, 32	\n\t"

       XXSWAPD_S(38,40)
       XXSWAPD_S(39,41)
       XXSWAPD_S(%x3,42)
       XXSWAPD_S(%x4,43)
       XXSWAPD_S(%x5,44)
       XXSWAPD_S(%x6,45)
       XXSWAPD_S(%x7,46)
       XXSWAPD_S(%x8,47)

       "xvmuldp		38, 38, 33	\n\t"	// x0_i * -alpha_i, x0_r * alpha_i
       "xvmuldp		39, 39, 33	\n\t"


       "xvmuldp		%x3, %x3, 33	\n\t"
       "xvmuldp		%x4, %x4, 33	\n\t"


       "lxvp		40, 128(%2)	\n\t"
       "lxvp		42, 160(%2)	\n\t"
       "xvmuldp		%x5, %x5, 33	\n\t"
       "xvmuldp		%x6, %x6, 33	\n\t"


       "xvmuldp		%x7, %x7, 33	\n\t"
       "xvmuldp		%x8, %x8, 33	\n\t"
       "lxvp		44, 192(%2)	\n\t"
       "lxvp		46, 224(%2)	\n\t"


       "xvadddp		48, 48, 38	\n\t"
       "xvadddp		49, 49, 39	\n\t"
       "xvadddp		50, 50, %x3	\n\t"
       "xvadddp		51, 51, %x4	\n\t"
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
       "stxv        48, 0(%2)   \n\t"
       "stxv        49, 16(%2)  \n\t"
       "stxv        50, 32(%2)  \n\t"
       "stxv        51, 48(%2)  \n\t"
#else
       "stxv		49, 0(%2)	\n\t"
       "stxv		48, 16(%2)	\n\t"
       "stxv		51, 32(%2)	\n\t"
       "stxv		50, 48(%2)	\n\t"
#endif


       "xvadddp		34, 34, %x5	\n\t"
       "xvadddp		35, 35, %x6	\n\t"


       "xvadddp		36, 36, %x7	\n\t"
       "xvadddp		37, 37, %x8	\n\t"
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
       "stxv        34, 64(%2)  \n\t"
       "stxv        35, 80(%2)  \n\t"
       "stxv        36, 96(%2)  \n\t"
       "stxv        37, 112(%2) \n\t"
#else
       "stxv		35, 64(%2)	\n\t"
       "stxv		34, 80(%2)	\n\t"
       "stxv		37, 96(%2)	\n\t"
       "stxv		36, 112(%2)	\n\t"
#endif
       "addi		%2, %2, 128	\n\t"

       "addic.		%1, %1, -8	\n\t"
       "bgt		one%=		\n"

     "two%=:				\n\t"

       "xvmuldp		48, 40, 32	\n\t"	// x0_r * alpha_r, x0_i * alpha_r
       "xvmuldp		49, 41, 32	\n\t"
       "xvmuldp		50, 42, 32	\n\t"
       "xvmuldp		51, 43, 32	\n\t"
       "xvmuldp		34, 44, 32	\n\t"
       "xvmuldp		35, 45, 32	\n\t"
       "xvmuldp		36, 46, 32	\n\t"
       "xvmuldp		37, 47, 32	\n\t"

       XXSWAPD_S(38,40)
       XXSWAPD_S(39,41)
       XXSWAPD_S(%x3,42)
       XXSWAPD_S(%x4,43)
       XXSWAPD_S(%x5,44)
       XXSWAPD_S(%x6,45)
       XXSWAPD_S(%x7,46)
       XXSWAPD_S(%x8,47)


       "xvmuldp		38, 38, 33	\n\t"	// x0_i * -alpha_i, x0_r * alpha_i
       "xvmuldp		39, 39, 33	\n\t"
       "xvmuldp		%x3, %x3, 33	\n\t"
       "xvmuldp		%x4, %x4, 33	\n\t"
       "xvmuldp		%x5, %x5, 33	\n\t"
       "xvmuldp		%x6, %x6, 33	\n\t"
       "xvmuldp		%x7, %x7, 33	\n\t"
       "xvmuldp		%x8, %x8, 33	\n\t"

       "xvadddp		48, 48, 38	\n\t"
       "xvadddp		49, 49, 39	\n\t"

       "xvadddp		50, 50, %x3	\n\t"
       "xvadddp		51, 51, %x4	\n\t"
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
       "stxv        48, 0(%2)   \n\t"
       "stxv        49, 16(%2)  \n\t"
       "stxv        50, 32(%2)  \n\t"
       "stxv        51, 48(%2)  \n\t"
#else
       "stxv		49, 0(%2)	\n\t"
       "stxv		48, 16(%2)	\n\t"
       "stxv		51, 32(%2)	\n\t"
       "stxv		50, 48(%2)	\n\t"
#endif
       "xvadddp		34, 34, %x5	\n\t"
       "xvadddp		35, 35, %x6	\n\t"


       "xvadddp		36, 36, %x7	\n\t"
       "xvadddp		37, 37, %x8	\n\t"
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
       "stxv        34, 64(%2)  \n\t"
       "stxv        35, 80(%2)  \n\t"
       "stxv        36, 96(%2)  \n\t"
       "stxv        37, 112(%2) \n\t"
#else
       "stxv		35, 64(%2)	\n\t"
       "stxv		34, 80(%2)	\n\t"
       "stxv		37, 96(%2)	\n\t"
       "stxv		36, 112(%2)	\n\t"
#endif
     "#n=%1 x=%0=%2 alpha=(%9,%10) \n"
     :
       "+m" (*x),
       "+r" (n),	// 1
       "+b" (x),	// 2
       "=wa" (t0),	// 3
       "=wa" (t1),	// 4
       "=wa" (t2),	// 5
       "=wa" (t3),	// 6
       "=wa" (t4),	// 7
       "=wa" (t5)	// 8
     :
       "d" (alpha_r),	// 9 
       "d" (alpha_i)	// 10
     :
       "cr0",
       "vs32","vs33","vs34","vs35","vs36","vs37","vs38","vs39",
       "vs40","vs41","vs42","vs43","vs44","vs45","vs46","vs47",
       "vs48","vs49","vs50","vs51"
     );
}
