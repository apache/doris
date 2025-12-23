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

static void drot_kernel_16 (long n, double *x, double *y, double c, double s)
{
  __asm__
    (
       XXSPLTD_S(36,%x5,0)	// load c to both dwords
       XXSPLTD_S(37,%x6,0)	// load s to both dwords
       "lxvp            32, 0(%3)       \n\t"   // load x
       "lxvp            34, 32(%3)      \n\t"
       "lxvp            48, 0(%4)       \n\t"   // load y
       "lxvp            50, 32(%4)      \n\t"

       "addic.		%2, %2, -8	\n\t"
       "ble		two%=		\n\t"

       ".align	5		\n"
     "one%=:				\n\t"

       "xvmuldp		40, 32, 36	\n\t"	// c * x
       "xvmuldp		41, 33, 36	\n\t"
       "xvmuldp		42, 34, 36	\n\t"
       "xvmuldp		43, 35, 36	\n\t"

       "xvmuldp		44, 32, 37	\n\t"	// s * x
       "xvmuldp		45, 33, 37	\n\t"
       "xvmuldp		46, 34, 37	\n\t"
       "xvmuldp		47, 35, 37	\n\t"

       "lxvp            32, 64(%3)       \n\t"   // load x
       "lxvp            34, 96(%3)      \n\t"
       "xvmuldp		52, 48, 36	\n\t"	// c * y
       "xvmuldp		53, 49, 36	\n\t"
       "xvmuldp		54, 50, 36	\n\t"
       "xvmuldp		55, 51, 36	\n\t"

       "xvmuldp		38, 48, 37	\n\t"	// s * y
       "xvmuldp		39, 49, 37	\n\t"
       "xvmuldp		56, 50, 37	\n\t"
       "xvmuldp		57, 51, 37	\n\t"

       "lxvp            48, 64(%4)       \n\t"   // load y
       "lxvp            50, 96(%4)      \n\t"

       "xvadddp		40, 40, 38	\n\t"	// c * x + s * y
       "xvadddp		41, 41, 39	\n\t"	// c * x + s * y
       "xvadddp		42, 42, 56	\n\t"	// c * x + s * y
       "xvadddp		43, 43, 57	\n\t"	// c * x + s * y

       "stxvp           40, 0(%3)       \n\t"   // store x
       "stxvp           42, 32(%3)      \n\t"

       "xvsubdp         52, 52, 44      \n\t"   // c * y - s * x
       "xvsubdp         53, 53, 45      \n\t"   // c * y - s * x
       "xvsubdp         54, 54, 46      \n\t"   // c * y - s * x
       "xvsubdp         55, 55, 47      \n\t"   // c * y - s * x

       "stxvp           52, 0(%4)       \n\t"   // store y
       "stxvp           54, 32(%4)      \n\t"

       "addi		%3, %3, 64	\n\t"
       "addi		%4, %4, 64	\n\t"

       "addic.		%2, %2, -8	\n\t"
       "bgt		one%=		\n"

     "two%=:				\n\t"

       "xvmuldp		40, 32, 36	\n\t"	// c * x
       "xvmuldp		41, 33, 36	\n\t"
       "xvmuldp		42, 34, 36	\n\t"
       "xvmuldp		43, 35, 36	\n\t"

       "xvmuldp         52, 48, 36      \n\t"   // c * y
       "xvmuldp         53, 49, 36      \n\t"
       "xvmuldp         54, 50, 36      \n\t"
       "xvmuldp         55, 51, 36      \n\t"

       "xvmuldp         44, 32, 37      \n\t"   // s * x
       "xvmuldp         45, 33, 37      \n\t"
       "xvmuldp         46, 34, 37      \n\t"
       "xvmuldp         47, 35, 37      \n\t"

       "xvmuldp         38, 48, 37     \n\t"   // s * y
       "xvmuldp         39, 49, 37     \n\t"
       "xvmuldp         56, 50, 37     \n\t"
       "xvmuldp         57, 51, 37     \n\t"

       "xvadddp         40, 40, 38     \n\t"   // c * x + s * y
       "xvadddp         41, 41, 39     \n\t"   // c * x + s * y
       "xvadddp         42, 42, 56     \n\t"   // c * x + s * y
       "xvadddp         43, 43, 57     \n\t"   // c * x + s * y

       "stxvp           40, 0(%3)       \n\t"   // store x
       "stxvp           42, 32(%3)      \n\t"
       "xvsubdp         52, 52, 44      \n\t"   // c * y - s * x
       "xvsubdp         53, 53, 45      \n\t"   // c * y - s * x
       "xvsubdp         54, 54, 46      \n\t"   // c * y - s * x
       "xvsubdp         55, 55, 47      \n\t"   // c * y - s * x

       "stxvp           52, 0(%4)       \n\t"   // store y
       "stxvp           54, 32(%4)      \n\t"

     "#n=%2 x=%0=%3 y=%1=%4 c=%5 s=%6\n"
     :
       "+m" (*x),
       "+m" (*y),
       "+r" (n),	// 2
       "+b" (x),	// 3
       "+b" (y) 	// 4
     :
       "d" (c),		// 5 
       "d" (s)		// 6 
     :
       "cr0",
       "vs32","vs33","vs34","vs35","vs36","vs37","vs38","vs39",
       "vs40","vs41","vs42","vs43","vs44","vs45","vs46","vs47",
       "vs48","vs49","vs50","vs51","vs52","vs53","vs54","vs55",
       "vs56","vs57"
     );
}
