/***************************************************************************
Copyright (c) 2013-2016, The OpenBLAS Project
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

#define HAVE_KERNEL_4x4 1

static void dgemv_kernel_4x4 (long n, double *ap, long lda, double *x, double *y, double alpha)
{
  double *a0;
  double *a1;
  double *a2;
  double *a3;

  __asm__
    (
       "lxvp	40, 0(%10)	\n\t"	// x0, x1
       XXSPLTD_S(32,%x9,0)	// alpha, alpha

       "sldi		%6, %13, 3	\n\t"	// lda * sizeof (double)
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
       "xvmuldp     34, 40, 32  \n\t"   // x0 * alpha, x1 * alpha
       "xvmuldp     35, 41, 32  \n\t"	// x2 * alpha, x3 * alpha
#else
       "xvmuldp		34, 41, 32	\n\t"	// x0 * alpha, x1 * alpha
       "xvmuldp		35, 40, 32	\n\t"	// x2 * alpha, x3 * alpha
#endif

       "add		%4, %3, %6	\n\t"	// a0 = ap, a1 = a0 + lda
       "add		%6, %6, %6	\n\t"	// 2 * lda
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
       XXSPLTD_S(32,34,0)   // x0 * alpha, x0 * alpha
       XXSPLTD_S(33,34,1)   // x1 * alpha, x1 * alpha
       XXSPLTD_S(34,35,0)   // x2 * alpha, x2 * alpha
       XXSPLTD_S(35,35,1)   // x3 * alpha, x3 * alpha
#else
       XXSPLTD_S(32,34,1)	// x0 * alpha, x0 * alpha
       XXSPLTD_S(33,34,0)	// x1 * alpha, x1 * alpha
       XXSPLTD_S(34,35,1)	// x2 * alpha, x2 * alpha
       XXSPLTD_S(35,35,0)	// x3 * alpha, x3 * alpha
#endif
       "add		%5, %3, %6	\n\t"	// a2 = a0 + 2 * lda
       "add		%6, %4, %6	\n\t"	// a3 = a1 + 2 * lda

       "dcbt		0, %3		\n\t"
       "dcbt		0, %4		\n\t"
       "dcbt		0, %5		\n\t"
       "dcbt		0, %6		\n\t"

       "lxvp		40, 0(%3)	\n\t"	// a0[0], a0[1]

       "lxvp		42, 0(%4)	\n\t"	// a1[0], a1[1]

       "lxvp		44, 0(%5)	\n\t"	// a2[0], a2[1]

       "lxvp		46, 0(%6)	\n\t"	// a3[0], a3[1]

       "dcbt		0, %2		\n\t"

       "addi		%3, %3, 32	\n\t"
       "addi		%4, %4, 32	\n\t"
       "addi		%5, %5, 32	\n\t"
       "addi		%6, %6, 32	\n\t"

       "addic.		%1, %1, -4	\n\t"
       "ble		two%=		\n\t"

       ".align	5		\n"
     "one%=:				\n\t"

       "lxvp		36, 0(%2)	\n\t"	// y0, y1

       "xvmaddadp 	36, 40, 32	\n\t"
       "xvmaddadp 	37, 41, 32	\n\t"

       "lxvp		40, 0(%3)	\n\t"	// a0[0], a0[1]

       "xvmaddadp 	36, 42, 33	\n\t"
       "addi		%3, %3, 32	\n\t"
       "xvmaddadp 	37, 43, 33	\n\t"

       "lxvp		42, 0(%4)	\n\t"	// a1[0], a1[1]

       "xvmaddadp 	36, 44, 34	\n\t"
       "addi		%4, %4, 32	\n\t"
       "xvmaddadp 	37, 45, 34	\n\t"

       "lxvp		44, 0(%5)	\n\t"	// a2[0], a2[1]

       "xvmaddadp 	36, 46, 35	\n\t"
       "addi		%5, %5, 32	\n\t"
       "xvmaddadp 	37, 47, 35	\n\t"

       "stxvp		36, 0(%2)	\n\t"	// y0, y1

       "lxvp		46, 0(%6)	\n\t"	// a3[0], a3[1]

       "addi		%6, %6, 32	\n\t"
       "addi		%2, %2, 32	\n\t"

       "addic.		%1, %1, -4	\n\t"
       "ble		two%=		\n\t"


       "lxvp		36, 0(%2)	\n\t"	// y0, y1

       "xvmaddadp 	36, 40, 32	\n\t"
       "xvmaddadp 	37, 41, 32	\n\t"

       "lxvp		40, 0(%3)	\n\t"	// a0[0], a0[1]

       "xvmaddadp 	36, 42, 33	\n\t"
       "addi		%3, %3, 32	\n\t"
       "xvmaddadp 	37, 43, 33	\n\t"

       "lxvp		42, 0(%4)	\n\t"	// a1[0], a1[1]

       "xvmaddadp 	36, 44, 34	\n\t"
       "addi		%4, %4, 32	\n\t"
       "xvmaddadp 	37, 45, 34	\n\t"

       "lxvp		44, 0(%5)	\n\t"	// a2[0], a2[1]

       "xvmaddadp 	36, 46, 35	\n\t"
       "addi		%5, %5, 32	\n\t"
       "xvmaddadp 	37, 47, 35	\n\t"

       "stxvp		36, 0(%2)	\n\t"	// y0, y1

       "lxvp	46, 0(%6)	\n\t"	// a3[0], a3[1]

       "addi		%6, %6, 32	\n\t"
       "addi		%2, %2, 32	\n\t"

       "addic.		%1, %1, -4	\n\t"
       "ble		two%=		\n\t"


       "lxvp		36, 0(%2)	\n\t"	// y0, y1

       "xvmaddadp 	36, 40, 32	\n\t"
       "xvmaddadp 	37, 41, 32	\n\t"

       "lxvp		40, 0(%3)	\n\t"	// a0[0], a0[1]

       "xvmaddadp 	36, 42, 33	\n\t"
       "addi		%3, %3, 32	\n\t"
       "xvmaddadp 	37, 43, 33	\n\t"

       "lxvp		42, 0(%4)	\n\t"	// a1[0], a1[1]

       "xvmaddadp 	36, 44, 34	\n\t"
       "addi		%4, %4, 32	\n\t"
       "xvmaddadp 	37, 45, 34	\n\t"

       "lxvp		44, 0(%5)	\n\t"	// a2[0], a2[1]

       "xvmaddadp 	36, 46, 35	\n\t"
       "addi		%5, %5, 32	\n\t"
       "xvmaddadp 	37, 47, 35	\n\t"

       "stxvp		36, 0(%2)	\n\t"	// y0, y1

       "lxvp		46, 0(%6)	\n\t"	// a3[0], a3[1]

       "addi		%6, %6, 32	\n\t"
       "addi		%2, %2, 32	\n\t"

       "addic.		%1, %1, -4	\n\t"
       "ble		two%=		\n\t"


       "lxvp		36, 0(%2)	\n\t"	// y0, y1

       "xvmaddadp 	36, 40, 32	\n\t"
       "xvmaddadp 	37, 41, 32	\n\t"

       "lxvp		40, 0(%3)	\n\t"	// a0[0], a0[1]

       "xvmaddadp 	36, 42, 33	\n\t"
       "addi		%3, %3, 32	\n\t"
       "xvmaddadp 	37, 43, 33	\n\t"

       "lxvp		42, 0(%4)	\n\t"	// a1[0], a1[1]

       "xvmaddadp 	36, 44, 34	\n\t"
       "addi		%4, %4, 32	\n\t"
       "xvmaddadp 	37, 45, 34	\n\t"

       "lxvp		44, 0(%5)	\n\t"	// a2[0], a2[1]

       "xvmaddadp 	36, 46, 35	\n\t"
       "addi		%5, %5, 32	\n\t"
       "xvmaddadp 	37, 47, 35	\n\t"

       "stxvp		36, 0(%2)	\n\t"	// y0, y1

       "lxvp		46, 0(%6)	\n\t"	// a3[0], a3[1]

       "addi		%6, %6, 32	\n\t"
       "addi		%2, %2, 32	\n\t"

       "addic.		%1, %1, -4	\n\t"
       "bgt		one%=		\n"

     "two%=:				\n\t"

       "lxvp		36, 0(%2)	\n\t"	// y0, y1

       "xvmaddadp 	36, 40, 32	\n\t"
       "xvmaddadp 	37, 41, 32	\n\t"

       "xvmaddadp 	36, 42, 33	\n\t"
       "xvmaddadp 	37, 43, 33	\n\t"

       "xvmaddadp 	36, 44, 34	\n\t"
       "xvmaddadp 	37, 45, 34	\n\t"

       "xvmaddadp 	36, 46, 35	\n\t"
       "xvmaddadp 	37, 47, 35	\n\t"

       "stxvp		36, 0(%2)	\n\t"	// y0, y1

     "#n=%1 ap=%8=%12 lda=%13 x=%7=%10 y=%0=%2 alpha=%9 o16=%11\n"
     "#a0=%3 a1=%4 a2=%5 a3=%6"
     :
       "+m" (*y),
       "+r" (n),	// 1
       "+b" (y),	// 2
       "=b" (a0),	// 3
       "=b" (a1),	// 4
       "=&b" (a2),	// 5
       "=&b" (a3)	// 6
     :
       "m" (*x),
       "m" (*ap),
       "d" (alpha),	// 9
       "r" (x),		// 10
       "b" (16),	// 11
       "3" (ap),	// 12
       "4" (lda)	// 13
     :
       "cr0",
       "vs32","vs33","vs34","vs35","vs36","vs37",
       "vs40","vs41","vs42","vs43","vs44","vs45","vs46","vs47"
     );
}
static void dgemv_kernel_4x8 (long n, double *ap, long lda, double *x, double *y, double alpha)
{

  double *a0;
  double *a1;
  double *a2;
  double *a3;
  double *a4;
  double *a5;
  double *a6;
  double *a7;
  long tmp;
  __asm__
    (
       "lxvp		34, 0( %15)	\n\t"	// x0, x1
       "lxvp		38, 32( %15)	\n\t"	// x4, x5

       XXSPLTD_S(58,%x14,0)	// alpha, alpha
       "sldi		%10, %17, 3	\n\t"	// lda * sizeof (double)
       "xvmuldp         34, 34, 58      \n\t"   // x0 * alpha, x1 * alpha
       "xvmuldp         35, 35, 58      \n\t"   // x2 * alpha, x3 * alpha
       "xvmuldp         38, 38, 58      \n\t"   // x4 * alpha, x5 * alpha
       "xvmuldp         39, 39, 58      \n\t"   // x6 * alpha, x7 * alpha

       "li		%11, 32   \n\t"

       "add		%4, %3, %10	\n\t"	// a0 = ap, a1 = a0 + lda
       "add		%10, %10, %10	\n\t"	// 2 * lda
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
       XXSPLTD_S(32,34,0)       // x0 * alpha, x0 * alpha
       XXSPLTD_S(33,34,1)       // x1 * alpha, x1 * alpha
       XXSPLTD_S(34,35,0)       // x2 * alpha, x2 * alpha
       XXSPLTD_S(35,35,1)       // x3 * alpha, x3 * alpha
       XXSPLTD_S(48,39,0)       // x6 * alpha, x6 * alpha
       XXSPLTD_S(49,39,1)       // x7 * alpha, x7 * alpha
       XXSPLTD_S(39,38,1)       // x5 * alpha, x5 * alpha
       XXSPLTD_S(38,38,0)       // x4 * alpha, x4 * alpha
#else
       XXSPLTD_S(32,34,1)       // x0 * alpha, x0 * alpha
       XXSPLTD_S(33,34,0)       // x1 * alpha, x1 * alpha
       XXSPLTD_S(34,35,1)       // x2 * alpha, x2 * alpha
       XXSPLTD_S(35,35,0)       // x3 * alpha, x3 * alpha
       XXSPLTD_S(48,39,1)       // x6 * alpha, x6 * alpha
       XXSPLTD_S(49,39,0)       // x7 * alpha, x7 * alpha
       XXSPLTD_S(39,38,0)       // x5 * alpha, x5 * alpha
       XXSPLTD_S(38,38,1)       // x4 * alpha, x4 * alpha
#endif

       "add		%5, %3, %10	\n\t"	// a2 = a0 + 2 * lda
       "add		%6, %4, %10	\n\t"	// a3 = a1 + 2 * lda
       "add		%7, %5, %10	\n\t"	// a4 = a2 + 2 * lda
       "add		%8, %6, %10	\n\t"	// a5 = a3 + 2 * lda
       "add		%9, %7, %10	\n\t"	// a6 = a4 + 2 * lda
       "add		%10, %8, %10	\n\t"	// a7 = a5 + 2 * lda

       "lxvp		40, 0( %3)	\n\t"	// a0[0], a0[1]
       "lxvp		42, 0( %4)	\n\t"	// a1[0], a1[1]
       "lxvp		44, 0( %5)	\n\t"	// a2[0], a2[1]
       "lxvp		46, 0( %6)	\n\t"	// a3[0], a3[1]
       "lxvp		50, 0( %7)	\n\t"	// a4[0]
       "lxvp		52, 0( %8)	\n\t"	// a5[0]
       "lxvp		54, 0( %9)	\n\t"	// a6[0]
       "lxvp		56, 0( %10)	\n\t"	// a7[0]


       "addic.		%1, %1, -4	\n\t"
       "ble		two%=		\n\t"

       ".align	5		\n"
     "one%=:				\n\t"

       "lxvp		36, 0( %2)	\n\t"	// y0, y1
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
       "xvmaddadp       36, 40, 32      \n\t"
       "xvmaddadp       37, 41, 32      \n\t"
#else
       "xvmaddadp       36, 40, 34      \n\t"
       "xvmaddadp       37, 41, 34      \n\t"
#endif
       "lxvpx		40, %3, %11	\n\t"	// a0[0], a0[1]
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
       "xvmaddadp       36, 42, 33      \n\t"
       "xvmaddadp       37, 43, 33      \n\t"
#else
       "xvmaddadp       36, 42, 35      \n\t"
       "xvmaddadp       37, 43, 35      \n\t"
#endif
       "lxvpx		42, %4, %11	\n\t"	// a1[0], a1[1]
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
       "xvmaddadp       36, 44, 34      \n\t"
       "xvmaddadp       37, 45, 34      \n\t"
#else
       "xvmaddadp       36, 44, 32      \n\t"
       "xvmaddadp       37, 45, 32      \n\t"
#endif
       "lxvpx		44, %5, %11	\n\t"	// a2[0], a2[1]
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
       "xvmaddadp       36, 46, 35      \n\t"
       "xvmaddadp       37, 47, 35      \n\t"
#else
       "xvmaddadp       36, 46, 33      \n\t"
       "xvmaddadp       37, 47, 33      \n\t"
#endif
       "lxvpx		46, %6, %11	\n\t"	// a3[0], a3[1]
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
       "xvmaddadp       36, 50, 38      \n\t"
       "xvmaddadp       37, 51, 38      \n\t"
#else
       "xvmaddadp       36, 50, 48      \n\t"
       "xvmaddadp       37, 51, 48      \n\t"
#endif
       "lxvpx		50, %7, %11	\n\t"	// a4[0]
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
       "xvmaddadp       36, 52, 39      \n\t"
       "xvmaddadp       37, 53, 39      \n\t"
#else
       "xvmaddadp       36, 52, 49      \n\t"
       "xvmaddadp       37, 53, 49      \n\t"
#endif
       "lxvpx		52, %8, %11	\n\t"	// a5[0]
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
       "xvmaddadp       36, 54, 48      \n\t"
       "xvmaddadp       37, 55, 48      \n\t"
#else
       "xvmaddadp       36, 54, 38      \n\t"
       "xvmaddadp       37, 55, 38      \n\t"
#endif
       "lxvpx		54, %9, %11	\n\t"	// a6[0]
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
       "xvmaddadp       36, 56, 49      \n\t"
       "xvmaddadp       37, 57, 49      \n\t"
#else
       "xvmaddadp       36, 56, 39      \n\t"
       "xvmaddadp       37, 57, 39      \n\t"
#endif
       "lxvpx		56, %10, %11	\n\t"	// a7[0]
       "addi		%11, %11, 32    \n\t"

       "stxvp		36, 0( %2)	\n\t"	// y0, y1
       "addi		%2, %2, 32	\n\t"

       "addic.		%1, %1, -4	\n\t"
       "bgt		one%=		\n"

     "two%=:				\n\t"

       "lxvp		36, 0( %2)	\n\t"	// y0, y1
#if  (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
       "xvmaddadp       36, 40, 32      \n\t"
       "xvmaddadp       37, 41, 32      \n\t"
       "xvmaddadp       36, 42, 33      \n\t"
       "xvmaddadp       37, 43, 33      \n\t"
       "xvmaddadp       36, 44, 34      \n\t"
       "xvmaddadp       37, 45, 34      \n\t"
       "xvmaddadp       36, 46, 35      \n\t"
       "xvmaddadp       37, 47, 35      \n\t"
       "xvmaddadp       36, 50, 38      \n\t"
       "xvmaddadp       37, 51, 38      \n\t"
       "xvmaddadp       36, 52, 39      \n\t"
       "xvmaddadp       37, 53, 39      \n\t"
       "xvmaddadp       36, 54, 48      \n\t"
       "xvmaddadp       37, 55, 48      \n\t"
       "xvmaddadp       36, 56, 49      \n\t"
       "xvmaddadp       37, 57, 49      \n\t"
#else
       "xvmaddadp       36, 40, 34      \n\t"
       "xvmaddadp       37, 41, 34      \n\t"
       "xvmaddadp       36, 42, 35      \n\t"
       "xvmaddadp       37, 43, 35      \n\t"
       "xvmaddadp       36, 44, 32      \n\t"
       "xvmaddadp       37, 45, 32      \n\t"
       "xvmaddadp       36, 46, 33      \n\t"
       "xvmaddadp       37, 47, 33      \n\t"
       "xvmaddadp       36, 50, 48      \n\t"
       "xvmaddadp       37, 51, 48      \n\t"
       "xvmaddadp       36, 52, 49      \n\t"
       "xvmaddadp       37, 53, 49      \n\t"
       "xvmaddadp       36, 54, 38      \n\t"
       "xvmaddadp       37, 55, 38      \n\t"
       "xvmaddadp       36, 56, 39      \n\t"
       "xvmaddadp       37, 57, 39      \n\t"
#endif
       "stxvp		36, 0( %2)	\n\t"	// y0, y1

     :
       "+m" (*y),
       "+r" (n),	// 1
       "+b" (y),	// 2
       "=b" (a0),	// 3
       "=b" (a1),	// 4
       "=&b" (a2),	// 5
       "=&b" (a3),	// 6
       "=&b" (a4),	// 7
       "=&b" (a5),	// 8
       "=&b" (a6),	// 9
       "=&b" (a7),	// 10
       "=b" (tmp)
     :
       "m" (*x),
       "m" (*ap),
       "d" (alpha),	// 14
       "r" (x),		// 15
       "3" (ap),	// 16
       "4" (lda)	// 17
     :
       "cr0",
       "vs32","vs33","vs34","vs35","vs36","vs37",
       "vs40","vs41","vs42","vs43","vs44","vs45","vs46","vs47", "vs48",
       "vs49","vs50","vs51","vs52","vs53","vs54","vs55","vs56", "vs57", "vs58"
     );
}
