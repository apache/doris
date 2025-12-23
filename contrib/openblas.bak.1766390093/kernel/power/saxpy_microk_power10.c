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

static void saxpy_kernel_64(long n, float *x, float *y, float alpha)
{
  __vector float t0 = {alpha, alpha,alpha, alpha};

  __asm__
    (

       "dcbt		0, %2		\n\t"
       "dcbt		0, %3		\n\t"

       "lxvp		32, 0(%2)	\n\t"
       "lxvp		34, 32(%2)	\n\t"
       "lxvp		40, 64(%2)	\n\t"
       "lxvp		42, 96(%2)	\n\t"
       "lxvp		48, 128(%2)	\n\t"
       "lxvp		50, 160(%2)	\n\t"
       "lxvp		52, 192(%2)	\n\t"
       "lxvp		54, 224(%2)	\n\t"

       "lxvp		36, 0(%3)	\n\t"
       "lxvp		38, 32(%3)	\n\t"
       "lxvp		44, 64(%3)	\n\t"
       "lxvp		46, 96(%3)	\n\t"
       "lxvp		56, 128(%3)	\n\t"
       "lxvp		58, 160(%3)	\n\t"
       "lxvp		60, 192(%3)	\n\t"
       "lxvp		62, 224(%3)	\n\t"

       "addi		%2, %2, 256	\n\t"

       "addic.		%1, %1, -64	\n\t"
       "ble		two%=		\n\t"

       ".align 5			\n"
     "one%=:				\n\t"

       "xvmaddasp	36, 32, %x4	\n\t"
       "xvmaddasp	37, 33, %x4	\n\t"

       "lxvp		32, 0(%2)	\n\t"
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
       "stxv		36, 0(%3)	\n\t"
       "stxv		37, 16(%3)	\n\t"
#else
       "stxv		37, 0(%3)	\n\t"
       "stxv		36, 16(%3)	\n\t"
#endif

       "xvmaddasp	38, 34, %x4	\n\t"
       "xvmaddasp	39, 35, %x4	\n\t"

       "lxvp		34, 32(%2)	\n\t"
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
       "stxv		38, 32(%3)	\n\t"
       "stxv		39, 48(%3)	\n\t"
#else
       "stxv		39, 32(%3)	\n\t"
       "stxv		38, 48(%3)	\n\t"
#endif

       "lxvp		36, 256(%3)	\n\t"
       "lxvp		38, 288(%3)	\n\t"

       "xvmaddasp	44, 40, %x4	\n\t"
       "xvmaddasp	45, 41, %x4	\n\t"

       "lxvp		40, 64(%2)	\n\t"
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
       "stxv		44, 64(%3)	\n\t"
       "stxv		45, 80(%3)	\n\t"
#else
       "stxv		45, 64(%3)	\n\t"
       "stxv		44, 80(%3)	\n\t"
#endif

       "xvmaddasp	46, 42, %x4	\n\t"
       "xvmaddasp	47, 43, %x4	\n\t"

       "lxvp		42, 96(%2)	\n\t"
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
       "stxv		46, 96(%3)	\n\t"
       "stxv		47, 112(%3)	\n\t"
#else
       "stxv		47, 96(%3)	\n\t"
       "stxv		46, 112(%3)	\n\t"
#endif

       "lxvp		44, 320(%3)	\n\t"
       "lxvp		46, 352(%3)	\n\t"

       "xvmaddasp	56, 48, %x4	\n\t"
       "xvmaddasp	57, 49, %x4	\n\t"

       "lxvp		48, 128(%2)	\n\t"
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
       "stxv		56, 128(%3)	\n\t"
       "stxv		57, 144(%3)	\n\t"
#else
       "stxv		57, 128(%3)	\n\t"
       "stxv		56, 144(%3)	\n\t"
#endif

       "xvmaddasp	58, 50, %x4	\n\t"
       "xvmaddasp	59, 51, %x4	\n\t"

       "lxvp		50, 160(%2)	\n\t"
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
       "stxv		58, 160(%3)	\n\t"
       "stxv		59, 176(%3)	\n\t"
#else
       "stxv		59, 160(%3)	\n\t"
       "stxv		58, 176(%3)	\n\t"
#endif

       "lxvp		56, 384(%3)	\n\t"
       "lxvp		58, 416(%3)	\n\t"

       "xvmaddasp	60, 52, %x4	\n\t"
       "xvmaddasp	61, 53, %x4	\n\t"

       "lxvp		52, 192(%2)	\n\t"
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
       "stxv		60, 192(%3)	\n\t"
       "stxv		61, 208(%3)	\n\t"
#else
       "stxv		61, 192(%3)	\n\t"
       "stxv		60, 208(%3)	\n\t"
#endif

       "xvmaddasp	62, 54, %x4	\n\t"
       "xvmaddasp	63, 55, %x4	\n\t"

       "lxvp		54, 224(%2)	\n\t"
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
       "stxv		62, 224(%3)	\n\t"
       "stxv		63, 240(%3)	\n\t"
#else
       "stxv		63, 224(%3)	\n\t"
       "stxv		62, 240(%3)	\n\t"
#endif

       "lxvp		60, 448(%3)	\n\t"
       "lxvp		62, 480(%3)	\n\t"

       "addi		%2, %2, 256	\n\t"
       "addi		%3, %3, 256	\n\t"

       "addic.		%1, %1, -64	\n\t"
       "bgt		one%=		\n"

     "two%=:				\n\t"

       "xvmaddasp	36, 32, %x4	\n\t"
       "xvmaddasp	37, 33, %x4	\n\t"
       "xvmaddasp	38, 34, %x4	\n\t"
       "xvmaddasp	39, 35, %x4	\n\t"

       "xvmaddasp	44, 40, %x4	\n\t"
       "xvmaddasp	45, 41, %x4	\n\t"
       "xvmaddasp	46, 42, %x4	\n\t"
       "xvmaddasp	47, 43, %x4	\n\t"

       "xvmaddasp	56, 48, %x4	\n\t"
       "xvmaddasp	57, 49, %x4	\n\t"
       "xvmaddasp	58, 50, %x4	\n\t"
       "xvmaddasp	59, 51, %x4	\n\t"

       "xvmaddasp	60, 52, %x4	\n\t"
       "xvmaddasp	61, 53, %x4	\n\t"
       "xvmaddasp	62, 54, %x4	\n\t"
       "xvmaddasp	63, 55, %x4	\n\t"
#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
       "stxv		36, 0(%3)	\n\t"
       "stxv		37, 16(%3)	\n\t"
       "stxv		38, 32(%3)	\n\t"
       "stxv		39, 48(%3)	\n\t"
       "stxv		44, 64(%3)	\n\t"
       "stxv		45, 80(%3)	\n\t"
       "stxv		46, 96(%3)	\n\t"
       "stxv		47, 112(%3)	\n\t"

       "stxv		56, 128(%3)	\n\t"
       "stxv		57, 144(%3)	\n\t"
       "stxv		58, 160(%3)	\n\t"
       "stxv		59, 176(%3)	\n\t"
       "stxv		60, 192(%3)	\n\t"
       "stxv		61, 208(%3)	\n\t"
       "stxv		62, 224(%3)	\n\t"
       "stxv		63, 240(%3)	\n\t"
#else
       "stxv		37, 0(%3)	\n\t"
       "stxv		36, 16(%3)	\n\t"
       "stxv		39, 32(%3)	\n\t"
       "stxv		38, 48(%3)	\n\t"
       "stxv		45, 64(%3)	\n\t"
       "stxv		44, 80(%3)	\n\t"
       "stxv		47, 96(%3)	\n\t"
       "stxv		46, 112(%3)	\n\t"

       "stxv		57, 128(%3)	\n\t"
       "stxv		56, 144(%3)	\n\t"
       "stxv		59, 160(%3)	\n\t"
       "stxv		58, 176(%3)	\n\t"
       "stxv		61, 192(%3)	\n\t"
       "stxv		60, 208(%3)	\n\t"
       "stxv		63, 224(%3)	\n\t"
       "stxv		62, 240(%3)	\n\t"
#endif

     "#n=%1 x=%5=%2 y=%0=%3 t0=%x4\n"
     :
       "+m" (*y),
       "+r" (n),	// 1
       "+b" (x),	// 2
       "+b" (y)		// 3
     :
       "wa" (t0),	// 4
       "m" (*x)
     :
       "cr0",
       "vs32","vs33","vs34","vs35","vs36","vs37", "vs38", "vs39",
       "vs40","vs41","vs42","vs43","vs44","vs45","vs46","vs47",
       "vs48","vs49","vs50","vs51","vs52","vs53","vs54","vs55",
       "vs56","vs57","vs58","vs59","vs60","vs61","vs62","vs63"
     );

}


