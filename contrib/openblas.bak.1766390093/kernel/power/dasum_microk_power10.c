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

#define HAVE_KERNEL_16 1

static double dasum_kernel_16 (long n, double *x)
{
  double sum;
  __vector double t0;
  __vector double t1;
  __vector double t2;
  __vector double t3;
  __vector double t4;
  __vector double t5;
  __vector double t6;
  __vector double t7;
  __vector double a0;
  __vector double a1;
  __vector double a2;
  __vector double a3;
  __vector double a4;
  __vector double a5;
  __vector double a6;
  __vector double a7;


  __asm__
    (
       "dcbt		0, %2		\n\t"

       "xxlxor		32, 32,	32	\n\t"
       "xxlxor		33, 33,	33	\n\t"
       "xxlxor		34, 34,	34	\n\t"
       "xxlxor		35, 35,	35	\n\t"
       "xxlxor		36, 36,	36	\n\t"
       "xxlxor		37, 37,	37	\n\t"
       "xxlxor		38, 38,	38	\n\t"
       "xxlxor		39, 39,	39	\n\t"

       "xxlxor		%x11, %x11, %x11	\n\t"
       "xxlxor		%x12, %x12, %x12	\n\t"
       "xxlxor		%x13, %x13, %x13	\n\t"
       "xxlxor		%x14, %x14, %x14 	\n\t"
       "xxlxor		%x15, %x15, %x15 	\n\t"
       "xxlxor		%x16, %x16, %x16	\n\t"
       "xxlxor		%x17, %x17, %x17	\n\t"
       "xxlxor		%x18, %x18, %x18	\n\t"

       "lxvp            40, 0(%2)       \n\t"
       "lxvp            42, 32(%2)      \n\t"
       "lxvp            44, 64(%2)      \n\t"
       "lxvp            46, 96(%2)      \n\t"
       "lxvp            52, 128(%2)	\n\t"
       "lxvp            54, 160(%2)	\n\t"
       "lxvp            56, 192(%2)	\n\t"
       "lxvp            58, 224(%2)	\n\t"

       "addi		%2, %2, 256	\n\t"

       "addic.		%1, %1, -32	\n\t"
       "ble		two%=		\n\t"

       ".align	5		\n"
     "one%=:				\n\t"

       "xvabsdp		48, 40		\n\t"
       "xvabsdp		49, 41		\n\t"
       "xvabsdp		50, 42		\n\t"
       "xvabsdp		51, 43		\n\t"

       "xvabsdp		%x3, 44		\n\t"
       "xvabsdp		%x4, 45		\n\t"
       "xvabsdp		%x5, 46		\n\t"
       "xvabsdp		%x6, 47		\n\t"

       "xvadddp		32, 32, 48	\n\t"
       "xvadddp		33, 33, 49	\n\t"
       "xvadddp		34, 34, 50	\n\t"
       "xvadddp		35, 35, 51	\n\t"
       "lxvp            40, 0(%2)       \n\t"
       "lxvp            42, 32(%2)      \n\t"
       "lxvp            44, 64(%2)      \n\t"
       "lxvp            46, 96(%2)      \n\t"

       "xvadddp		36, 36, %x3	\n\t"
       "xvadddp		37, 37, %x4	\n\t"
       "xvadddp		38, 38, %x5	\n\t"
       "xvadddp		39, 39, %x6	\n\t"

       "xvabsdp		60, 52 		\n\t"
       "xvabsdp		61, 53 		\n\t"
       "xvabsdp		62, 54 		\n\t"
       "xvabsdp		63, 55 		\n\t"

       "xvabsdp		%x7, 56		\n\t"
       "xvabsdp		%x8, 57		\n\t"
       "xvabsdp		%x9, 58		\n\t"
       "xvabsdp		%x10, 59	\n\t"

       "xvadddp		%x11, %x11, 60	\n\t"
       "xvadddp		%x12, %x12, 61	\n\t"
       "xvadddp		%x13, %x13, 62	\n\t"
       "xvadddp		%x14, %x14, 63	\n\t"

       "lxvp		52, 128(%2)	\n\t"
       "lxvp		54, 160(%2)	\n\t"
       "lxvp		56, 192(%2)	\n\t"
       "lxvp		58, 224(%2)	\n\t"
       "xvadddp		%x15, %x15, %x7	\n\t"
       "xvadddp		%x16, %x16, %x8	\n\t"
       "xvadddp		%x17, %x17, %x9	\n\t"
       "xvadddp		%x18, %x18, %x10	\n\t"
       "addi		%2, %2, 256	\n\t"
       "addic.		%1, %1, -32	\n\t"

       "bgt		one%=		\n"

     "two%=:				\n\t"

       "xvabsdp		48, 40		\n\t"
       "xvabsdp		49, 41		\n\t"
       "xvabsdp		50, 42		\n\t"
       "xvabsdp		51, 43		\n\t"
       "xvabsdp		%x3, 44		\n\t"
       "xvabsdp		%x4, 45		\n\t"
       "xvabsdp		%x5, 46		\n\t"
       "xvabsdp		%x6, 47		\n\t"

       "xvadddp		32, 32, 48	\n\t"
       "xvadddp		33, 33, 49	\n\t"
       "xvadddp		34, 34, 50	\n\t"
       "xvadddp		35, 35, 51	\n\t"
       "xvadddp		36, 36, %x3	\n\t"
       "xvadddp		37, 37, %x4	\n\t"
       "xvadddp		38, 38, %x5	\n\t"
       "xvadddp		39, 39, %x6	\n\t"

       "xvabsdp		60, 52 		\n\t"
       "xvabsdp		61, 53 		\n\t"
       "xvabsdp		62, 54 		\n\t"
       "xvabsdp		63, 55 		\n\t"

       "xvabsdp		%x7, 56		\n\t"
       "xvabsdp		%x8, 57		\n\t"
       "xvabsdp		%x9, 58 	\n\t"
       "xvabsdp		%x10, 59	\n\t"
       "xvadddp		%x11, %x11, 60	\n\t"
       "xvadddp		%x12, %x12, 61	\n\t"
       "xvadddp		%x13, %x13, 62	\n\t"
       "xvadddp		%x14, %x14, 63	\n\t"

       "xvadddp		%x15, %x15, %x7	\n\t"
       "xvadddp		%x16, %x16, %x8	\n\t"
       "xvadddp		%x17, %x17, %x9	\n\t"
       "xvadddp		%x18, %x18, %x10	\n\t"

       "xvadddp		32, 32, 33	\n\t"
       "xvadddp		34, 34, 35	\n\t"
       "xvadddp		36, 36, 37	\n\t"
       "xvadddp		38, 38, 39	\n\t"

       "xvadddp		32, 32, 34	\n\t"
       "xvadddp		36, 36, 38	\n\t"

       "xvadddp		%x11, %x11, %x12 	\n\t"
       "xvadddp		%x13, %x13, %x14	\n\t"
       "xvadddp		%x15, %x15, %x16	\n\t"
       "xvadddp		%x17, %x17, %x18	\n\t"

       "xvadddp		%x11, %x11, %x13	\n\t"
       "xvadddp		%x15, %x15, %x17	\n\t"

       "xvadddp		%x11, %x11, %x15	\n\t"

       "xvadddp		32, 32, 36	\n\t"
       "xvadddp		32, 32, %x11	\n\t"

       XXSWAPD_S(33,32)
       "xsadddp		%x0, 32, 33	\n"

     "#n=%1 x=%3=%2 sum=%0\n"
     "#t0=%x3 t1=%x4 t2=%x5 t3=%x6"
     :
       "=d" (sum),	// 0
       "+r" (n),	// 1
       "+b" (x),	// 2
       "=wa" (t0),	// 3
       "=wa" (t1),	// 4
       "=wa" (t2),	// 5
       "=wa" (t3),	// 6
       "=wa" (t4),	// 7
       "=wa" (t5),	// 8
       "=wa" (t6),	// 9
       "=wa" (t7),	// 10
       "=wa" (a0),	// 11
       "=wa" (a1),	// 12
       "=wa" (a2),	// 13
       "=wa" (a3),	// 14
       "=wa" (a4),	// 15
       "=wa" (a5),	// 16
       "=wa" (a6),	// 17
       "=wa" (a7)	// 18
     :
       "m" (*x)
     :
       "cr0",
       "vs32","vs33","vs34","vs35","vs36","vs37","vs38","vs39",
       "vs40","vs41","vs42","vs43","vs44","vs45","vs46","vs47",
       "vs48","vs49","vs50","vs51","vs52","vs53","vs54","vs55",
       "vs56","vs57","vs58","vs59","vs60","vs61","vs62","vs63"
     );

  return sum;
}


