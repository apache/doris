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

/**************************************************************************************
* 2016/04/21 Werner Saar (wernsaar@googlemail.com)
* 	 BLASTEST 		: OK
* 	 CTEST			: OK
* 	 TEST			: OK
*	 LAPACK-TEST		: OK
**************************************************************************************/


/**********************************************************************************************
* Macros for N=4 and M=16
**********************************************************************************************/

#if defined(_AIX)
define(`COPY_4x16', `
#else
.macro COPY_4x16
#endif

    lxvpx       vs32,   o0,     A0
    lxvpx       vs34,   o32,    A0

    lxvpx       vs36,   o0,     A1
    lxvpx       vs38,   o32,    A1

    lxvpx       vs40,   o0,     A2
    lxvpx       vs42,   o32,    A2

    lxvpx       vs44,   o0,     A3
    lxvpx       vs46,   o32,    A3

	mr		T1,	BO

#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
    stxvx       vs32,   o0,     T1
    stxvx       vs33,   o16,    T1
    stxvx       vs34,   o32,    T1
    stxvx       vs35,   o48,    T1
#else
    stxvx       vs33,   o0,     T1
    stxvx       vs32,   o16,    T1
    stxvx       vs35,   o32,    T1
    stxvx       vs34,   o48,    T1
#endif

	addi		T1,	T1,	64

#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
    stxvx       vs36,   o0,     T1
    stxvx       vs37,   o16,    T1
    stxvx       vs38,   o32,    T1
    stxvx       vs39,   o48,    T1
#else
    stxvx       vs37,   o0,     T1
    stxvx       vs36,   o16,    T1
    stxvx       vs39,   o32,    T1
    stxvx       vs38,   o48,    T1
#endif

	addi		T1,	T1,	64

#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
    stxvx       vs40,   o0,     T1
    stxvx       vs41,   o16,    T1
    stxvx       vs42,   o32,    T1
    stxvx       vs43,   o48,    T1
#else
    stxvx       vs41,   o0,     T1
    stxvx       vs40,   o16,    T1
    stxvx       vs43,   o32,    T1
    stxvx       vs42,   o48,    T1
#endif

	addi		T1,	T1,	64

#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
    stxvx       vs44,   o0,     T1
    stxvx       vs45,   o16,    T1
    stxvx       vs46,   o32,    T1
    stxvx       vs47,   o48,    T1
#else
    stxvx       vs45,   o0,     T1
    stxvx       vs44,   o16,    T1
    stxvx       vs47,   o32,    T1
    stxvx       vs46,   o48,    T1
#endif

#if defined(_AIX)
')
#else
.endm
#endif

/**********************************************************************************************
* Macros for N=4 and M=8
**********************************************************************************************/

#if defined(_AIX)
define(`COPY_4x8', `
#else
.macro COPY_4x8
#endif

    lxvpx       vs32,   o0,     A0

    lxvpx       vs34,   o0,     A1

    lxvpx       vs36,   o0,     A2

    lxvpx       vs38,   o0,     A3

    mr      T1, BO

#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
    stxvx       vs32,   o0,     T1
    stxvx       vs33,   o16,    T1

    stxvx       vs34,   o32,    T1
    stxvx       vs35,   o48,    T1
#else
    stxvx       vs33,   o0,     T1
    stxvx       vs32,   o16,    T1

    stxvx       vs35,   o32,    T1
    stxvx       vs34,   o48,    T1
#endif

    addi        T1, T1, 64

#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
    stxvx       vs36,   o0,     T1
    stxvx       vs37,   o16,    T1

    stxvx       vs38,   o32,    T1
    stxvx       vs39,   o48,    T1
#else
    stxvx       vs37,   o0,     T1
    stxvx       vs36,   o16,    T1

    stxvx       vs39,   o32,    T1
    stxvx       vs38,   o48,    T1
#endif

#if defined(_AIX)
')
#else
.endm
#endif

/**********************************************************************************************
* Macros for N=2 and M=16
**********************************************************************************************/

#if defined(_AIX)
define(`COPY_2x16', `
#else
.macro COPY_2x16
#endif

    lxvpx       vs32,   o0,     A0
    lxvpx       vs34,   o32,    A0

    lxvpx       vs36,   o0,     A1
    lxvpx       vs38,   o32,    A1

	mr		T1,	BO

#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
    stxvx       vs32,   o0,     T1
    stxvx       vs33,   o16,    T1
    stxvx       vs34,   o32,    T1
    stxvx       vs35,   o48,    T1
#else
    stxvx       vs33,   o0,     T1
    stxvx       vs32,   o16,    T1
    stxvx       vs35,   o32,    T1
    stxvx       vs34,   o48,    T1
#endif

	addi		T1,	T1,	64

#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
    stxvx       vs36,   o0, T1
    stxvx       vs37,   o16,    T1
    stxvx       vs38,   o32,    T1
    stxvx       vs39,   o48,    T1
#else
    stxvx       vs37,   o0, T1
    stxvx       vs36,   o16,    T1
    stxvx       vs39,   o32,    T1
    stxvx       vs38,   o48,    T1
#endif

#if defined(_AIX)
')
#else
.endm
#endif

/**********************************************************************************************
* Macros for N=2 and M=8
**********************************************************************************************/

#if defined(_AIX)
define(`COPY_2x8', `
#else
.macro COPY_2x8
#endif

    lxvpx       vs32,   o0,     A0

    lxvpx       vs34,   o0,     A1

    mr      T1, BO

#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
    stxvx       vs32,   o0,     T1
    stxvx       vs33,   o16,    T1

    stxvx       vs34,   o32,    T1
    stxvx       vs35,   o48,    T1
#else
    stxvx       vs33,   o0,     T1
    stxvx       vs32,   o16,    T1

    stxvx       vs35,   o32,    T1
    stxvx       vs34,   o48,    T1
#endif

#if defined(_AIX)
')
#else
.endm
#endif

/**********************************************************************************************
* Macros for N=1 and M=16
**********************************************************************************************/

#if defined(_AIX)
define(`COPY_1x16', `
#else
.macro COPY_1x16
#endif

    lxvpx       vs32,   o0,     A0
    lxvpx       vs34,   o32,    A0

	mr		T1,	BO

#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
    stxvx       vs32,   o0, T1
    stxvx       vs33,   o16,    T1
    stxvx       vs34,   o32,    T1
    stxvx       vs35,   o48,    T1
#else
    stxvx       vs33,   o0, T1
    stxvx       vs32,   o16,    T1
    stxvx       vs35,   o32,    T1
    stxvx       vs34,   o48,    T1
#endif

#if defined(_AIX)
')
#else
.endm
#endif

/**********************************************************************************************
* Macros for N=1 and M=8
**********************************************************************************************/

#if defined(_AIX)
define(`COPY_1x8', `
#else
.macro COPY_1x8
#endif

    lxvpx       vs32,   o0,     A0

    mr      T1, BO

#if (__BYTE_ORDER__ == __ORDER_BIG_ENDIAN__)
    stxvx       vs32,   o0, T1
    stxvx       vs33,   o16,    T1
#else
    stxvx       vs33,   o0, T1
    stxvx       vs32,   o16,    T1
#endif

#if defined(_AIX)
')
#else
.endm
#endif

