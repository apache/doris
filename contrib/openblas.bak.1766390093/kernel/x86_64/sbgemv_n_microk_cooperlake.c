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
#if (( defined(__GNUC__)  && __GNUC__   >= 10 && defined(__AVX512BF16__)) || (defined(__clang__) && __clang_major__ >= 9)) || (defined(__NVCOMPILER) && NVCOMPVERS >= 2203 )

#define HAVE_SBGEMV_N_ACCL_KERNEL 1
#include "common.h"
#include <immintrin.h>

// Define micro kernels for ALPHA not ONE && BETA effective && BETA not ONE scenarios
#undef  ZERO_BETA
#undef  ONE_BETA
#undef  ONE_ALPHA
#include "sbgemv_n_microk_cooperlake_template.c"

// Define micro kernels for ALPHA not ONE && BETA as ONE scenarios
#undef  ZERO_BETA
#define ONE_BETA  1
#undef  ONE_ALPHA
#include "sbgemv_n_microk_cooperlake_template.c"

// Define micro kernels for ALPHA not ONE && BETA in-effective (BETA == 0) scenarios
#define ZERO_BETA 1
#undef  ONE_ALPHA
#include "sbgemv_n_microk_cooperlake_template.c"

// Define micro kernels for ALPHA as ONE && BETA in-effective (BETA == 0) scenarios
#define ZERO_BETA 1
#define ONE_ALPHA 1
#include "sbgemv_n_microk_cooperlake_template.c"

static int sbgemv_kernel_n(BLASLONG m, BLASLONG n, float alpha, bfloat16 *a, BLASLONG lda, bfloat16 *x, float beta, float *y)
{
    if (beta == ZERO) {          // BETA == 0.0, no need to accumulate the original Y data
        if (alpha == ONE) {           // ALPHA == 1.0, no need to multipy ALPHA
            sbgemv_kernel_32xN_lda_direct(m, n, alpha, a, lda, x, y);
        } else {                      // ALPHA != 1.0, need to multipy ALPHA
            sbgemv_kernel_32xN_lda_direct_alpha(m, n, alpha, a, lda, x, y);
        }
    } else {                     // BETA != 0.0, need to accumulate the original Y data no matter what ALPHA is
        if (beta == ONE) {
            sbgemv_kernel_32xN_lda_direct_alpha_one(m, n, alpha, a, lda, x, beta, y);
        } else {
            sbgemv_kernel_32xN_lda_direct_alpha_beta(m, n, alpha, a, lda, x, beta, y);
        }
    }

    return 0;
}

#endif
