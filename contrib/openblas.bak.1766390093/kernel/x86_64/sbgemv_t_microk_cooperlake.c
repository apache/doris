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

#define HAVE_SBGEMV_T_ACCL_KERNEL 1

// Define micro kernels for ALPHA not ONE && BETA effective && BETA not ONE scenarios
#undef  ZERO_BETA
#undef  ONE_BETA
#undef  ONE_ALPHA
#include "sbgemv_t_microk_cooperlake_template.c"

// Define micro kernels for ALPHA not ONE && BETA as ONE scenarios
#undef  ZERO_BETA
#define ONE_BETA  1
#undef  ONE_ALPHA
#include "sbgemv_t_microk_cooperlake_template.c"

// Define micro kernels for ALPHA not ONE && BETA in-effective (BETA == 0) scenarios
#define ZERO_BETA 1
#undef  ONE_ALPHA
#include "sbgemv_t_microk_cooperlake_template.c"

// Define micro kernels for ALPHA as ONE && BETA in-effective (BETA == 0) scenarios
#define ZERO_BETA 1
#define ONE_ALPHA 1
#include "sbgemv_t_microk_cooperlake_template.c"

static int sbgemv_kernel_t(BLASLONG m, BLASLONG n, float alpha, bfloat16 *a, BLASLONG lda, bfloat16 *x, float beta, float *y)
{
    if (beta == ZERO) {          // BETA == 0.0, no need to accumulate the original Y data
        if (alpha == ONE) {           // ALPHA == 1.0, no need to multipy ALPHA
            if (n > 127) {
                sbgemv_kernel_1x128_lda_direct(m, n, alpha, a, lda, x, y);
            } else if (n > 32) {
                sbgemv_kernel_8x32_lda_direct(m, n, alpha, a, lda, x, y);
            } else {
                if (n > 16) {
                    sbgemv_kernel_8x16p_lda(m, n, alpha, a, lda, x, y);
                } else {
                    if (lda == n) {
                        switch(n) {
                            case 1:  sbgemv_kernel_32x1 (m, alpha, a, x, y); break;
                            case 2:  sbgemv_kernel_32x2 (m, alpha, a, x, y); break;
                            case 3:  sbgemv_kernel_32x3 (m, alpha, a, x, y); break;
                            case 4:  sbgemv_kernel_16x4 (m, alpha, a, x, y); break;
                            case 5:  sbgemv_kernel_30x5 (m, alpha, a, x, y); break;
                            case 6:  sbgemv_kernel_16x6 (m, alpha, a, x, y); break;
                            case 7:  sbgemv_kernel_16x7 (m, alpha, a, x, y); break;
                            case 8:  sbgemv_kernel_16x8 (m, alpha, a, x, y); break;
                            case 9:  sbgemv_kernel_14x9 (m, alpha, a, x, y); break;
                            case 10: sbgemv_kernel_12x10(m, alpha, a, x, y); break;
                            case 11: sbgemv_kernel_15x11(m, alpha, a, x, y); break;
                            case 12: sbgemv_kernel_15x12(m, alpha, a, x, y); break;
                            case 13: sbgemv_kernel_16x13(m, alpha, a, x, y); break;
                            case 14: sbgemv_kernel_16x14(m, alpha, a, x, y); break;
                            case 15: sbgemv_kernel_16x15(m, alpha, a, x, y); break;
                            case 16: sbgemv_kernel_16x16(m, alpha, a, x, y); break;
                            default: break;
                        }
                    } else {
                        sbgemv_kernel_8x16m_lda(m, n, alpha, a, lda, x, y);
                    }
                }
            }
        } else {                      // ALPHA != 1.0, need to multipy ALPHA
            if (n > 127) {
                sbgemv_kernel_1x128_lda_direct_alpha(m, n, alpha, a, lda, x, y);
            } else if (n > 32) {
                sbgemv_kernel_8x32_lda_direct_alpha(m, n, alpha, a, lda, x, y);
            } else {
                if (n > 16) {
                    sbgemv_kernel_8x16p_lda_alpha(m, n, alpha, a, lda, x, y);
                } else {
                    if (lda == n) {
                        switch(n) {
                            case 1:  sbgemv_kernel_32x1_alpha (m, alpha, a, x, y); break;
                            case 2:  sbgemv_kernel_32x2_alpha (m, alpha, a, x, y); break;
                            case 3:  sbgemv_kernel_32x3_alpha (m, alpha, a, x, y); break;
                            case 4:  sbgemv_kernel_16x4_alpha (m, alpha, a, x, y); break;
                            case 5:  sbgemv_kernel_30x5_alpha (m, alpha, a, x, y); break;
                            case 6:  sbgemv_kernel_16x6_alpha (m, alpha, a, x, y); break;
                            case 7:  sbgemv_kernel_16x7_alpha (m, alpha, a, x, y); break;
                            case 8:  sbgemv_kernel_16x8_alpha (m, alpha, a, x, y); break;
                            case 9:  sbgemv_kernel_14x9_alpha (m, alpha, a, x, y); break;
                            case 10: sbgemv_kernel_12x10_alpha(m, alpha, a, x, y); break;
                            case 11: sbgemv_kernel_15x11_alpha(m, alpha, a, x, y); break;
                            case 12: sbgemv_kernel_15x12_alpha(m, alpha, a, x, y); break;
                            case 13: sbgemv_kernel_16x13_alpha(m, alpha, a, x, y); break;
                            case 14: sbgemv_kernel_16x14_alpha(m, alpha, a, x, y); break;
                            case 15: sbgemv_kernel_16x15_alpha(m, alpha, a, x, y); break;
                            case 16: sbgemv_kernel_16x16_alpha(m, alpha, a, x, y); break;
                            default: break;
                        }
                    } else {
                        sbgemv_kernel_8x16m_lda_alpha(m, n, alpha, a, lda, x, y);
                    }
                }
            }
        }
    } else {                     // BETA != 0.0, need to accumulate the original Y data no matter what ALPHA is
        if (beta == ONE) {
            if (n > 127) {
                sbgemv_kernel_1x128_lda_direct_alpha_one(m, n, alpha, a, lda, x, beta, y);
            } else if (n > 32) {
                sbgemv_kernel_8x32_lda_direct_alpha_one(m, n, alpha, a, lda, x, beta, y);
            } else {
                if (n > 16) {
                    sbgemv_kernel_8x16p_lda_alpha_one(m, n, alpha, a, lda, x, beta, y);
                } else {
                    if (lda == n) {
                        switch(n) {
                            case 1:  sbgemv_kernel_32x1_alpha_one (m, alpha, a, x, beta, y); break;
                            case 2:  sbgemv_kernel_32x2_alpha_one (m, alpha, a, x, beta, y); break;
                            case 3:  sbgemv_kernel_32x3_alpha_one (m, alpha, a, x, beta, y); break;
                            case 4:  sbgemv_kernel_16x4_alpha_one (m, alpha, a, x, beta, y); break;
                            case 5:  sbgemv_kernel_30x5_alpha_one (m, alpha, a, x, beta, y); break;
                            case 6:  sbgemv_kernel_16x6_alpha_one (m, alpha, a, x, beta, y); break;
                            case 7:  sbgemv_kernel_16x7_alpha_one (m, alpha, a, x, beta, y); break;
                            case 8:  sbgemv_kernel_16x8_alpha_one (m, alpha, a, x, beta, y); break;
                            case 9:  sbgemv_kernel_14x9_alpha_one (m, alpha, a, x, beta, y); break;
                            case 10: sbgemv_kernel_12x10_alpha_one(m, alpha, a, x, beta, y); break;
                            case 11: sbgemv_kernel_15x11_alpha_one(m, alpha, a, x, beta, y); break;
                            case 12: sbgemv_kernel_15x12_alpha_one(m, alpha, a, x, beta, y); break;
                            case 13: sbgemv_kernel_16x13_alpha_one(m, alpha, a, x, beta, y); break;
                            case 14: sbgemv_kernel_16x14_alpha_one(m, alpha, a, x, beta, y); break;
                            case 15: sbgemv_kernel_16x15_alpha_one(m, alpha, a, x, beta, y); break;
                            case 16: sbgemv_kernel_16x16_alpha_one(m, alpha, a, x, beta, y); break;
                            default: break;
                        }
                    } else {
                        sbgemv_kernel_8x16m_lda_alpha_one(m, n, alpha, a, lda, x, beta, y);
                    }
                }
            }
        } else {
            if (n > 127) {
                sbgemv_kernel_1x128_lda_direct_alpha_beta(m, n, alpha, a, lda, x, beta, y);
            } else if (n > 32) {
                sbgemv_kernel_8x32_lda_direct_alpha_beta(m, n, alpha, a, lda, x, beta, y);
            } else {
                if (n > 16) {
                    sbgemv_kernel_8x16p_lda_alpha_beta(m, n, alpha, a, lda, x, beta, y);
                } else {
                    if (lda == n) {
                        switch(n) {
                            case 1:  sbgemv_kernel_32x1_alpha_beta (m, alpha, a, x, beta, y); break;
                            case 2:  sbgemv_kernel_32x2_alpha_beta (m, alpha, a, x, beta, y); break;
                            case 3:  sbgemv_kernel_32x3_alpha_beta (m, alpha, a, x, beta, y); break;
                            case 4:  sbgemv_kernel_16x4_alpha_beta (m, alpha, a, x, beta, y); break;
                            case 5:  sbgemv_kernel_30x5_alpha_beta (m, alpha, a, x, beta, y); break;
                            case 6:  sbgemv_kernel_16x6_alpha_beta (m, alpha, a, x, beta, y); break;
                            case 7:  sbgemv_kernel_16x7_alpha_beta (m, alpha, a, x, beta, y); break;
                            case 8:  sbgemv_kernel_16x8_alpha_beta (m, alpha, a, x, beta, y); break;
                            case 9:  sbgemv_kernel_14x9_alpha_beta (m, alpha, a, x, beta, y); break;
                            case 10: sbgemv_kernel_12x10_alpha_beta(m, alpha, a, x, beta, y); break;
                            case 11: sbgemv_kernel_15x11_alpha_beta(m, alpha, a, x, beta, y); break;
                            case 12: sbgemv_kernel_15x12_alpha_beta(m, alpha, a, x, beta, y); break;
                            case 13: sbgemv_kernel_16x13_alpha_beta(m, alpha, a, x, beta, y); break;
                            case 14: sbgemv_kernel_16x14_alpha_beta(m, alpha, a, x, beta, y); break;
                            case 15: sbgemv_kernel_16x15_alpha_beta(m, alpha, a, x, beta, y); break;
                            case 16: sbgemv_kernel_16x16_alpha_beta(m, alpha, a, x, beta, y); break;
                            default: break;
                        }
                    } else {
                        sbgemv_kernel_8x16m_lda_alpha_beta(m, n, alpha, a, lda, x, beta, y);
                    }
                }
            }
        }
    }

    return 0;
}

#endif
