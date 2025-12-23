/***************************************************************************
Copyright (c) 2014, The OpenBLAS Project
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

/* need a new enough GCC for avx512 support */
#ifdef __NVCOMPILER
#define NVCOMPVERS ( __NVCOMPILER_MAJOR__ * 100 + __NVCOMPILER_MINOR__ )
#endif
#if (( defined(__GNUC__)  && __GNUC__   >= 6 && defined(__AVX512CD__)) || (defined(__clang__) && __clang_major__ >= 9)) || (defined(__NVCOMPILER) && NVCOMPVERS >= 2203 )

#define HAVE_SGEMV_T_SKYLAKE_KERNEL 1
#include "common.h"
#include <immintrin.h>
#include "sgemv_t_microk_skylakex_template.c"

//sgemv_t:
// ----- m -----
// |<-----------
// |<-----------
// n
// |<-----------
// |<-----------

static int sgemv_kernel_t(BLASLONG m, BLASLONG n, float alpha, float *a, float *x, float *y)
{    
    switch(m) {
        case 1:  sgemv_kernel_t_1(n, alpha, a, x, y); break;
        case 2:  sgemv_kernel_t_2(n, alpha, a, x, y); break;
        case 3:  sgemv_kernel_t_3(n, alpha, a, x, y); break;
        case 4:  sgemv_kernel_t_4(n, alpha, a, x, y); break;
        case 5:  sgemv_kernel_t_5(n, alpha, a, x, y); break;
        case 6:  sgemv_kernel_t_6(n, alpha, a, x, y); break;
        case 7:  sgemv_kernel_t_7(n, alpha, a, x, y); break;
        case 8:  sgemv_kernel_t_8(n, alpha, a, x, y); break;
        default: break;
    }
    return 0;
}

#endif
