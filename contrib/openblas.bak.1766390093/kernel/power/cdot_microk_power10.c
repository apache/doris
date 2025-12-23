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

static void cdot_kernel_8 (long n, float *x, float *y, float *dot)
{
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
  __vector unsigned char mask = {4,5,6,7, 0,1,2,3, 12,13,14,15, 8,9,10,11};
#else
  __vector unsigned char mask = { 11,10,9,8,15,14,13,12,3,2,1,0,7,6,5,4};
#endif
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

       "lxvp            40, 0(%2)       \n\t"
       "lxvp            42, 32(%2)      \n\t"
       "lxvp            44, 64(%2)      \n\t"
       "lxvp            46, 96(%2)      \n\t"
       "lxvp            48, 0(%3)       \n\t"
       "lxvp            50, 32(%3)      \n\t"
       "lxvp            52, 64(%3)      \n\t"
       "lxvp            54, 96(%3)      \n\t"

       "xxperm          56, 48, %x7     \n\t"
       "xxperm          57, 49, %x7     \n\t"
       "xxperm          58, 50, %x7     \n\t"
       "xxperm          59, 51, %x7     \n\t"

       "xxperm          60, 52, %x7     \n\t"
       "xxperm          61, 53, %x7     \n\t"
       "xxperm          62, 54, %x7     \n\t"
       "xxperm          63, 55, %x7     \n\t"

       "addi		%2, %2, 128	\n\t"
       "addi		%3, %3, 128	\n\t"

       "addic.		%1, %1, -16	\n\t"
       "ble		two%=		\n\t"

       ".align	5		\n"
     "one%=:				\n\t"

       "xvmaddasp	32, 40, 48	\n\t"	// x0_r * y0_r , x0_i * y0_i
       "xvmaddasp	34, 41, 49	\n\t"	// x1_r * y1_r , x1_i * y1_i
       "lxvp            48, 0(%3)       \n\t"

       "xvmaddasp	36, 42, 50	\n\t"	// x2_r * y2_r , x2_i * y2_i
       "xvmaddasp	38, 43, 51	\n\t"	// x3_r * y3_r , x3_i * y3_i
       "lxvp            50, 32(%3)      \n\t"

       "xvmaddasp	33, 40, 56 	\n\t"	// x0_r * y0_i , x0_i * y0_r
       "xvmaddasp	35, 41, 57 	\n\t"	// x1_r * y1_i , x1_i * y1_r
       "lxvp            40, 0(%2)       \n\t"

       "xvmaddasp	37, 42, 58	\n\t"	// x2_r * y2_i , x2_i * y2_r
       "xvmaddasp	39, 43, 59	\n\t"	// x3_r * y3_i , x3_i * y3_r
       "lxvp            42, 32(%2)      \n\t"

       "xxperm          56, 48, %x7     \n\t"
       "xxperm          57, 49, %x7     \n\t"
       "xxperm          58, 50, %x7     \n\t"
       "xxperm          59, 51, %x7     \n\t"

       "xvmaddasp	32, 44, 52	\n\t"	// x0_r * y0_r , x0_i * y0_i
       "xvmaddasp	34, 45, 53	\n\t"	// x1_r * y1_r , x1_i * y1_i
       "lxvp            52, 64(%3)      \n\t"

       "xvmaddasp	36, 46, 54 	\n\t"	// x2_r * y2_r , x2_i * y2_i
       "xvmaddasp	38, 47, 55	\n\t"	// x3_r * y3_r , x3_i * y3_i
       "lxvp            54, 96(%3)      \n\t"

       "xvmaddasp	33, 44, 60	\n\t"	// x0_r * y0_i , x0_i * y0_r
       "xvmaddasp	35, 45, 61	\n\t"	// x1_r * y1_i , x1_i * y1_r
       "lxvp            44, 64(%2)      \n\t"
       "xvmaddasp	37, 46, 62	\n\t"	// x2_r * y2_i , x2_i * y2_r
       "xvmaddasp	39, 47, 63	\n\t"	// x3_r * y3_i , x3_i * y3_r
       "lxvp            46, 96(%2)      \n\t"

       "xxperm          60, 52, %x7     \n\t"
       "xxperm          61, 53, %x7     \n\t"
       "xxperm          62, 54, %x7     \n\t"
       "xxperm          63, 55, %x7     \n\t"

       "addi		%2, %2, 128 	\n\t"
       "addi		%3, %3, 128	\n\t"

       "addic.		%1, %1, -16	\n\t"
       "bgt		one%=		\n"

     "two%=:				\n\t"

       "xvmaddasp	32, 40, 48	\n\t"	// x0_r * y0_r , x0_i * y0_i
       "xvmaddasp	34, 41, 49	\n\t"	// x1_r * y1_r , x1_i * y1_i
       "xvmaddasp	36, 42, 50	\n\t"	// x2_r * y2_r , x2_i * y2_i
       "xvmaddasp	38, 43, 51	\n\t"	// x3_r * y3_r , x3_i * y3_i

       "xvmaddasp	33, 40, 56	\n\t"	// x0_r * y0_i , x0_i * y0_r
       "xvmaddasp	35, 41, 57	\n\t"	// x1_r * y1_i , x1_i * y1_r
       "xvmaddasp	37, 42, 58	\n\t"	// x2_r * y2_i , x2_i * y2_r
       "xvmaddasp	39, 43, 59	\n\t"	// x3_r * y3_i , x3_i * y3_r

       "xvmaddasp	32, 44, 52	\n\t"	// x0_r * y0_r , x0_i * y0_i
       "xvmaddasp	34, 45, 53	\n\t"	// x1_r * y1_r , x1_i * y1_i
       "xvmaddasp	36, 46, 54	\n\t"	// x2_r * y2_r , x2_i * y2_i
       "xvmaddasp	38, 47, 55	\n\t"	// x3_r * y3_r , x3_i * y3_i

       "xvmaddasp	33, 44, 60	\n\t"	// x0_r * y0_i , x0_i * y0_r
       "xvmaddasp	35, 45, 61	\n\t"	// x1_r * y1_i , x1_i * y1_r
       "xvmaddasp	37, 46, 62	\n\t"	// x2_r * y2_i , x2_i * y2_r
       "xvmaddasp	39, 47, 63	\n\t"	// x3_r * y3_i , x3_i * y3_r

       "xvaddsp		32, 32, 34	\n\t"
       "xvaddsp		36, 36, 38	\n\t"

       "xvaddsp		33, 33, 35	\n\t"
       "xvaddsp		37, 37, 39	\n\t"

       "xvaddsp		35, 32, 36	\n\t"
       "xvaddsp		34, 33, 37	\n\t"
       "xxswapd		32, 35		\n\t"
       "xxswapd		33, 34		\n\t"
       "xvaddsp		35, 35, 32	\n\t"
       "xvaddsp		34, 34, 33	\n\t"
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
       "xxpermdi 	34, 35, 34, 0 	\n\t"
#else
       "xxpermdi	34, 34, 35, 2	\n\t"
#endif
       "stxv		34, 0(%6)       \n\t"

     "#n=%1 x=%4=%2 y=%5=%3 dot=%0=%6"
     :
       "=m" (*dot),
       "+r" (n),	// 1
       "+b" (x),	// 2
       "+b" (y)		// 3
     :
       "m" (*x),
       "m" (*y),
       "b" (dot),	// 6
       "wa" (mask)
     :
       "cr0",
       "vs32","vs33","vs34","vs35","vs36","vs37","vs38","vs39",
       "vs40","vs41","vs42","vs43","vs44","vs45","vs46","vs47",
       "vs48","vs49","vs50","vs51","vs52","vs53","vs54","vs55",
       "vs56","vs57","vs58","vs59","vs60","vs61","vs62","vs63"
     );
}
