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

#define HAVE_KERNEL_4 1
static void zaxpy_kernel_4 (long n, double *x, double *y,
			    double alpha_r, double alpha_i)
{
#if !defined(CONJ)
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
  static const double mvec[2] = { -1.0, 1.0 };
#else
  static const double mvec[2] = { 1.0, -1.0 };
#endif
#else
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
  static const double mvec[2] = { 1.0, -1.0 };
#else
  static const double mvec[2] = { -1.0, 1.0 };
#endif
#endif
  const double *mvecp = mvec;

  __vector double t0;
  __vector double t1;
  __vector double t2;
  __vector double t3;
  __vector double t4;
  __vector double t5;
  __vector double t6;
  __vector double t7;
  long ytmp;

  __asm__
    (
       XXSPLTD_S(32,%x15,0)	// alpha_r
       XXSPLTD_S(33,%x16,0)	// alpha_i
       "lxvd2x		36, 0, %17	\n\t"	// mvec

#if !defined(CONJ)
       "xvmuldp		33, 33, 36	\n\t"	// alpha_i * mvec
#else
       "xvmuldp		32, 32, 36	\n\t"	// alpha_r * mvec
#endif

       "mr		%12, %3		\n\t"
       "dcbt		0, %2		\n\t"
       "dcbt		0, %3		\n\t"


       "lxvp		40, 0(%2)	\n\t"	// x0
       "lxvp		42, 32(%2)	\n\t"	// x2
       "lxvp		48, 0(%3)	\n\t"	// y0
       "lxvp		50, 32(%3)	\n\t"	// y2

       XXSWAPD_S(%x4,40)	// exchange real and imag part
       XXSWAPD_S(%x5,41)	// exchange real and imag part
       XXSWAPD_S(%x6,42)	// exchange real and imag part
       XXSWAPD_S(%x7,43)	// exchange real and imag part

       "lxvp		44, 64(%2)	\n\t"	// x4
       "lxvp		46, 96(%2)	\n\t"	// x6
       "lxvp		34, 64(%3)	\n\t"	// y4
       "lxvp		38, 96(%3)	\n\t"	// y6

       XXSWAPD_S(%x8,44)	// exchange real and imag part
       XXSWAPD_S(%x9,45)	// exchange real and imag part
       XXSWAPD_S(%x10,46)	// exchange real and imag part
       XXSWAPD_S(%x11,47)	// exchange real and imag part

       "addi		%2, %2, 128	\n\t"
       "addi		%3, %3, 128	\n\t"

       "addic.		%1, %1, -8	\n\t"
       "ble		two%=		\n\t"

       ".align	5		\n"
       "one%=:				\n\t"

       "xvmaddadp	48, 40, 32	\n\t"	// alpha_r * x0_r , alpha_r * x0_i
       "xvmaddadp	49, 41, 32	\n\t"
       "lxvp		40, 0(%2)	\n\t"	// x0
       "xvmaddadp	50, 42, 32	\n\t"
       "xvmaddadp	51, 43, 32	\n\t"
       "lxvp		42, 32(%2)	\n\t"	// x2

       "xvmaddadp	34, 44, 32	\n\t"
       "xvmaddadp	35, 45, 32	\n\t"
       "lxvp		44, 64(%2)	\n\t"	// x4
       "xvmaddadp	38, 46, 32	\n\t"
       "xvmaddadp	39, 47, 32	\n\t"
       "lxvp		46, 96(%2)	\n\t"	// x6

       "xvmaddadp	48, %x4, 33	\n\t"	// alpha_i * x0_i , alpha_i * x0_r
       "addi		%2, %2, 128	\n\t"
       "xvmaddadp	49, %x5, 33	\n\t"
       "xvmaddadp	50, %x6, 33	\n\t"
       "xvmaddadp	51, %x7, 33	\n\t"

       "xvmaddadp	34, %x8, 33	\n\t"
       "xvmaddadp	35, %x9, 33	\n\t"
       "xvmaddadp	38, %x10, 33	\n\t"
       "xvmaddadp	39, %x11, 33	\n\t"

#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
       "stxv		48, 0(%12)	\n\t"
       "stxv		49, 16(%12)	\n\t"
       "stxv		50, 32(%12)	\n\t"
       "stxv		51, 48(%12)	\n\t"
       "stxv		34, 64(%12)	\n\t"
       "stxv		35, 80(%12)	\n\t"
       "stxv		38, 96(%12)	\n\t"
       "stxv		39, 112(%12)	\n\t"
#else
       "stxv		49, 0(%12)	\n\t"
       "stxv		48, 16(%12)	\n\t"
       "stxv		51, 32(%12)	\n\t"
       "stxv		50, 48(%12)	\n\t"
       "stxv		35, 64(%12)	\n\t"
       "stxv		34, 80(%12)	\n\t"
       "stxv		39, 96(%12)	\n\t"
       "stxv		38, 112(%12)	\n\t"
#endif

       "addi		%12, %12, 128	\n\t"

       XXSWAPD_S(%x4,40)	// exchange real and imag part
       XXSWAPD_S(%x5,41)	// exchange real and imag part
       "lxvp		48, 0(%3)	\n\t"	// y0
       XXSWAPD_S(%x6,42)	// exchange real and imag part
       XXSWAPD_S(%x7,43)	// exchange real and imag part
       "lxvp		50, 32(%3)	\n\t"	// y2

       XXSWAPD_S(%x8,44)	// exchange real and imag part
       XXSWAPD_S(%x9,45)	// exchange real and imag part
       "lxvp		34, 64(%3)	\n\t"	// y4
       XXSWAPD_S(%x10,46)	// exchange real and imag part
       XXSWAPD_S(%x11,47)	// exchange real and imag part
       "lxvp		38, 96(%3)	\n\t"	// y6

       "addi		%3, %3, 128	\n\t"

       "addic.		%1, %1, -8	\n\t"
       "bgt		one%=		\n"

       "two%=:				\n\t"
       "xvmaddadp	48, 40, 32	\n\t"	// alpha_r * x0_r , alpha_r * x0_i
       "xvmaddadp	49, 41, 32	\n\t"
       "xvmaddadp	50, 42, 32	\n\t"
       "xvmaddadp	51, 43, 32	\n\t"

       "xvmaddadp	34, 44, 32	\n\t"
       "xvmaddadp	35, 45, 32	\n\t"
       "xvmaddadp	38, 46, 32	\n\t"
       "xvmaddadp	39, 47, 32	\n\t"

       "xvmaddadp	48, %x4, 33	\n\t"	// alpha_i * x0_i , alpha_i * x0_r
       "xvmaddadp	49, %x5, 33	\n\t"
       "xvmaddadp	50, %x6, 33	\n\t"
       "xvmaddadp	51, %x7, 33	\n\t"

       "xvmaddadp	34, %x8, 33	\n\t"
       "xvmaddadp	35, %x9, 33	\n\t"
       "xvmaddadp	38, %x10, 33	\n\t"
       "xvmaddadp	39, %x11, 33	\n\t"

#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
       "stxv		48, 0(%12)	\n\t"
       "stxv		49, 16(%12)	\n\t"
       "stxv		50, 32(%12)	\n\t"
       "stxv		51, 48(%12)	\n\t"
       "stxv		34, 64(%12)	\n\t"
       "stxv		35, 80(%12)	\n\t"
       "stxv		38, 96(%12)	\n\t"
       "stxv		39, 112(%12)	\n\t"
#else
       "stxv		49, 0(%12)	\n\t"
       "stxv		48, 16(%12)	\n\t"
       "stxv		51, 32(%12)	\n\t"
       "stxv		50, 48(%12)	\n\t"
       "stxv		35, 64(%12)	\n\t"
       "stxv		34, 80(%12)	\n\t"
       "stxv		39, 96(%12)	\n\t"
       "stxv		38, 112(%12)	\n\t"
#endif

     "#n=%1 x=%13=%2 y=%0=%3 alpha=(%15,%16) mvecp=%14=%17 ytmp=%12\n"
     "#t0=%x4 t1=%x5 t2=%x6 t3=%x7 t4=%x8 t5=%x9 t6=%x10 t7=%x11"
     :
       "+m" (*y),
       "+r" (n),	// 1
       "+b" (x),	// 2
       "+b" (y),	// 3
       "=wa" (t0),	// 4
       "=wa" (t1),	// 5
       "=wa" (t2),	// 6
       "=wa" (t3),	// 7
       "=wa" (t4),	// 8
       "=wa" (t5),	// 9
       "=wa" (t6),	// 10
       "=wa" (t7),	// 11
       "=b" (ytmp)	// 12
     :
       "m" (*x),
       "m" (*mvecp),
       "d" (alpha_r),	// 15
       "d" (alpha_i),	// 16
       "12" (mvecp)	// 17
     :
       "cr0",
       "vs32","vs33","vs34","vs35","vs36","vs37","vs38","vs39",
       "vs40","vs41","vs42","vs43","vs44","vs45","vs46","vs47",
       "vs48","vs49","vs50","vs51"
     );
}
