#include "bf16_common_macros.h"
#include <immintrin.h>

#define BF16_BLOCK_STEP_N 8
#define BF16_BLOCK_THRES_K 1024
#define BF16_BLOCK_THRES_M 32
#define BF16_BLOCK_THRES_N 1024

#define A(i,j) A[(i)*lda+(j)]
#define B(i,j) B[(i)*ldb+(j)]
#define C(i,j) C[(i)*ldc+(j)]

#define ONE  1.e0f
#define ZERO  0.e0f

#define SHUFFLE_MAGIC_NO (const int) 0x39

#undef STORE16_COMPLETE_RESULT
#undef STORE16_MASK_COMPLETE_RESULT
#undef SBGEMM_BLOCK_KERNEL_NN_32x8xK
#undef SBGEMM_BLOCK_KERNEL_NN_16x8xK
#undef SBGEMM_BLOCK_KERNEL_NN_32xNx32
#undef SBGEMM_BLOCK_KERNEL_NN_16xNx32
#undef SBGEMM_BLOCK_KERNEL_NT_32x8xK
#undef SBGEMM_BLOCK_KERNEL_NT_16x8xK
#undef SBGEMM_BLOCK_KERNEL_NT_32xNxK
#undef SBGEMM_BLOCK_KERNEL_NT_16xNxK
#undef SBGEMM_BLOCK_KERNEL_TN_32x8xK
#undef SBGEMM_BLOCK_KERNEL_TN_16x8xK
#undef SBGEMM_BLOCK_KERNEL_TN_32xNx32
#undef SBGEMM_BLOCK_KERNEL_TN_16xNx32
#undef SBGEMM_BLOCK_KERNEL_TT_32x8xK
#undef SBGEMM_BLOCK_KERNEL_TT_16x8xK
#undef SBGEMM_BLOCK_KERNEL_TT_32xNxK
#undef SBGEMM_BLOCK_KERNEL_TT_16xNxK
#undef SBGEMM_BLOCKING_KERNEL_NN
#undef SBGEMM_BLOCKING_KERNEL_NT
#undef SBGEMM_BLOCKING_KERNEL_TN
#undef SBGEMM_BLOCKING_KERNEL_TT

#ifndef ONE_ALPHA      // ALPHA is not ONE
    #define STORE16_COMPLETE_RESULT          STORE16_COMPLETE_RESULT_ALPHA_ONE
    #define STORE16_MASK_COMPLETE_RESULT     STORE16_MASK_COMPLETE_RESULT_ALPHA_ONE

    #define SBGEMM_BLOCK_KERNEL_NN_32x8xK    sbgemm_block_kernel_nn_32x8xK_alpha
    #define SBGEMM_BLOCK_KERNEL_NN_16x8xK    sbgemm_block_kernel_nn_16x8xK_alpha
    #define SBGEMM_BLOCK_KERNEL_NN_32xNx32   sbgemm_block_kernel_nn_32xNx32_alpha
    #define SBGEMM_BLOCK_KERNEL_NN_16xNx32   sbgemm_block_kernel_nn_16xNx32_alpha

    #define SBGEMM_BLOCK_KERNEL_NT_32x8xK    SBGEMM_BLOCK_KERNEL_NN_32x8xK
    #define SBGEMM_BLOCK_KERNEL_NT_16x8xK    SBGEMM_BLOCK_KERNEL_NN_16x8xK
    #define SBGEMM_BLOCK_KERNEL_NT_32xNxK    sbgemm_block_kernel_nt_32xNxK_alpha
    #define SBGEMM_BLOCK_KERNEL_NT_16xNxK    sbgemm_block_kernel_nt_16xNxK_alpha

    #define SBGEMM_BLOCK_KERNEL_TN_32x8xK    sbgemm_block_kernel_tn_32x8xK_alpha
    #define SBGEMM_BLOCK_KERNEL_TN_16x8xK    sbgemm_block_kernel_tn_16x8xK_alpha
    #define SBGEMM_BLOCK_KERNEL_TN_32xNx32   sbgemm_block_kernel_tn_32xNx32_alpha
    #define SBGEMM_BLOCK_KERNEL_TN_16xNx32   sbgemm_block_kernel_tn_16xNx32_alpha

    #define SBGEMM_BLOCK_KERNEL_TT_32x8xK    SBGEMM_BLOCK_KERNEL_TN_32x8xK
    #define SBGEMM_BLOCK_KERNEL_TT_16x8xK    SBGEMM_BLOCK_KERNEL_TN_16x8xK
    #define SBGEMM_BLOCK_KERNEL_TT_32xNxK    sbgemm_block_kernel_tt_32xNxK_alpha
    #define SBGEMM_BLOCK_KERNEL_TT_16xNxK    sbgemm_block_kernel_tt_16xNxK_alpha

    #define SBGEMM_BLOCKING_KERNEL_NN        sbgemm_blocking_kernel_nn_alpha
    #define SBGEMM_BLOCKING_KERNEL_NT        sbgemm_blocking_kernel_nt_alpha
    #define SBGEMM_BLOCKING_KERNEL_TN        sbgemm_blocking_kernel_tn_alpha
    #define SBGEMM_BLOCKING_KERNEL_TT        sbgemm_blocking_kernel_tt_alpha
#else                  // ALPHA is ONE
    #define STORE16_COMPLETE_RESULT          STORE16_COMPLETE_RESULT_ONE_ONE
    #define STORE16_MASK_COMPLETE_RESULT     STORE16_MASK_COMPLETE_RESULT_ONE_ONE

    #define SBGEMM_BLOCK_KERNEL_NN_32x8xK    sbgemm_block_kernel_nn_32x8xK_one
    #define SBGEMM_BLOCK_KERNEL_NN_16x8xK    sbgemm_block_kernel_nn_16x8xK_one
    #define SBGEMM_BLOCK_KERNEL_NN_32xNx32   sbgemm_block_kernel_nn_32xNx32_one
    #define SBGEMM_BLOCK_KERNEL_NN_16xNx32   sbgemm_block_kernel_nn_16xNx32_one

    #define SBGEMM_BLOCK_KERNEL_NT_32x8xK    SBGEMM_BLOCK_KERNEL_NN_32x8xK
    #define SBGEMM_BLOCK_KERNEL_NT_16x8xK    SBGEMM_BLOCK_KERNEL_NN_16x8xK
    #define SBGEMM_BLOCK_KERNEL_NT_32xNxK    sbgemm_block_kernel_nt_32xNxK_one
    #define SBGEMM_BLOCK_KERNEL_NT_16xNxK    sbgemm_block_kernel_nt_16xNxK_one

    #define SBGEMM_BLOCK_KERNEL_TN_32x8xK    sbgemm_block_kernel_tn_32x8xK_one
    #define SBGEMM_BLOCK_KERNEL_TN_16x8xK    sbgemm_block_kernel_tn_16x8xK_one
    #define SBGEMM_BLOCK_KERNEL_TN_32xNx32   sbgemm_block_kernel_tn_32xNx32_one
    #define SBGEMM_BLOCK_KERNEL_TN_16xNx32   sbgemm_block_kernel_tn_16xNx32_one

    #define SBGEMM_BLOCK_KERNEL_TT_32x8xK    SBGEMM_BLOCK_KERNEL_TN_32x8xK
    #define SBGEMM_BLOCK_KERNEL_TT_16x8xK    SBGEMM_BLOCK_KERNEL_TN_16x8xK
    #define SBGEMM_BLOCK_KERNEL_TT_32xNxK    sbgemm_block_kernel_tt_32xNxK_one
    #define SBGEMM_BLOCK_KERNEL_TT_16xNxK    sbgemm_block_kernel_tt_16xNxK_one

    #define SBGEMM_BLOCKING_KERNEL_NN        sbgemm_blocking_kernel_nn_one
    #define SBGEMM_BLOCKING_KERNEL_NT        sbgemm_blocking_kernel_nt_one
    #define SBGEMM_BLOCKING_KERNEL_TN        sbgemm_blocking_kernel_tn_one
    #define SBGEMM_BLOCKING_KERNEL_TT        sbgemm_blocking_kernel_tt_one
#endif

extern bfloat16 * block_A;
extern bfloat16 * block_B;

/* --------------------------------------------- NN kernels ------------------------------------------ */
// SBGEMM Kernel for 16<M<=32, N=8, K can be any number, but the processing will take 32 as a base
#ifndef ONE_ALPHA      // ALPHA is not ONE
void sbgemm_block_kernel_nn_32x8xK_alpha(BLASLONG m, BLASLONG k, float alpha, bfloat16 *A, bfloat16 *B, float *C, int ldc)
#else                  // ALPHA is ONE
void sbgemm_block_kernel_nn_32x8xK_one(BLASLONG m, BLASLONG k, float alpha, bfloat16 *A, bfloat16 *B, float *C, int ldc)
#endif
{
    bfloat16 * A_addr = A;
    bfloat16 * B_addr = B;
    float    * C_addr = C;

#ifndef ONE_ALPHA
    __m512  ALPHAVECTOR = _mm512_set1_ps(alpha);
#endif

    __m512i arrayA_512_0, arrayA_512_1;
    __m512i arrayB_512_0, arrayB_512_1, arrayB_512_2, arrayB_512_3, arrayB_512_4, arrayB_512_5, arrayB_512_6, arrayB_512_7;
    __m512  result_512_0, result_512_1, result_512_2, result_512_3, result_512_4, result_512_5, result_512_6, result_512_7,
            result_512_8, result_512_9, result_512_10, result_512_11, result_512_12, result_512_13, result_512_14, result_512_15;
    __m512  result_512_tmp_0, result_512_tmp_1, result_512_tmp_2, result_512_tmp_3;

    __m512i M512_EPI32_8      = _mm512_set1_epi32(8);
    __m512i shuffle_idx_base0 = _mm512_set_epi32(23, 22, 21, 20, 7, 6, 5, 4, 19, 18, 17, 16, 3, 2, 1, 0);
    __m512i shuffle_idx_base1 = _mm512_add_epi32(shuffle_idx_base0, M512_EPI32_8);

    result_512_0  = _mm512_setzero_ps();
    result_512_1  = _mm512_setzero_ps();
    result_512_2  = _mm512_setzero_ps();
    result_512_3  = _mm512_setzero_ps();
    result_512_4  = _mm512_setzero_ps();
    result_512_5  = _mm512_setzero_ps();
    result_512_6  = _mm512_setzero_ps();
    result_512_7  = _mm512_setzero_ps();
    result_512_8  = _mm512_setzero_ps();
    result_512_9  = _mm512_setzero_ps();
    result_512_10 = _mm512_setzero_ps();
    result_512_11 = _mm512_setzero_ps();
    result_512_12 = _mm512_setzero_ps();
    result_512_13 = _mm512_setzero_ps();
    result_512_14 = _mm512_setzero_ps();
    result_512_15 = _mm512_setzero_ps();

    for (BLASLONG idx_k = 0; idx_k < k; idx_k += 2) {
        // Each two rows are a group for 32-pair bf16 elements
        arrayA_512_0 = _mm512_loadu_si512(A_addr);
        arrayA_512_1 = _mm512_loadu_si512(A_addr + 32);

        _MM512_BROADCASTD_EPI32(B_addr + 0,  arrayB_512_0);
        _MM512_BROADCASTD_EPI32(B_addr + 2,  arrayB_512_1);
        _MM512_BROADCASTD_EPI32(B_addr + 4,  arrayB_512_2);
        _MM512_BROADCASTD_EPI32(B_addr + 6,  arrayB_512_3);
        _MM512_BROADCASTD_EPI32(B_addr + 8,  arrayB_512_4);
        _MM512_BROADCASTD_EPI32(B_addr + 10, arrayB_512_5);
        _MM512_BROADCASTD_EPI32(B_addr + 12, arrayB_512_6);
        _MM512_BROADCASTD_EPI32(B_addr + 14, arrayB_512_7);

        result_512_0  = _mm512_dpbf16_ps(result_512_0,  (__m512bh) arrayA_512_0, (__m512bh) arrayB_512_0);
        result_512_1  = _mm512_dpbf16_ps(result_512_1,  (__m512bh) arrayA_512_0, (__m512bh) arrayB_512_1);
        result_512_2  = _mm512_dpbf16_ps(result_512_2,  (__m512bh) arrayA_512_0, (__m512bh) arrayB_512_2);
        result_512_3  = _mm512_dpbf16_ps(result_512_3,  (__m512bh) arrayA_512_0, (__m512bh) arrayB_512_3);
        result_512_4  = _mm512_dpbf16_ps(result_512_4,  (__m512bh) arrayA_512_0, (__m512bh) arrayB_512_4);
        result_512_5  = _mm512_dpbf16_ps(result_512_5,  (__m512bh) arrayA_512_0, (__m512bh) arrayB_512_5);
        result_512_6  = _mm512_dpbf16_ps(result_512_6,  (__m512bh) arrayA_512_0, (__m512bh) arrayB_512_6);
        result_512_7  = _mm512_dpbf16_ps(result_512_7,  (__m512bh) arrayA_512_0, (__m512bh) arrayB_512_7);

        result_512_8  = _mm512_dpbf16_ps(result_512_8,  (__m512bh) arrayA_512_1, (__m512bh) arrayB_512_0);
        result_512_9  = _mm512_dpbf16_ps(result_512_9,  (__m512bh) arrayA_512_1, (__m512bh) arrayB_512_1);
        result_512_10 = _mm512_dpbf16_ps(result_512_10, (__m512bh) arrayA_512_1, (__m512bh) arrayB_512_2);
        result_512_11 = _mm512_dpbf16_ps(result_512_11, (__m512bh) arrayA_512_1, (__m512bh) arrayB_512_3);
        result_512_12 = _mm512_dpbf16_ps(result_512_12, (__m512bh) arrayA_512_1, (__m512bh) arrayB_512_4);
        result_512_13 = _mm512_dpbf16_ps(result_512_13, (__m512bh) arrayA_512_1, (__m512bh) arrayB_512_5);
        result_512_14 = _mm512_dpbf16_ps(result_512_14, (__m512bh) arrayA_512_1, (__m512bh) arrayB_512_6);
        result_512_15 = _mm512_dpbf16_ps(result_512_15, (__m512bh) arrayA_512_1, (__m512bh) arrayB_512_7);

        // Load B with unroll 8
        B_addr += 16;
        // Load A with unroll 64
        A_addr += 64;
    }

    if (m != 32) {
        unsigned short tail_mask_value = (((unsigned short)0xffff) >> (32-m));
        __mmask16 tail_mask = *((__mmask16*) &tail_mask_value);
        result_512_tmp_0 = _mm512_permutex2var_ps(result_512_0, shuffle_idx_base0, result_512_8);
        result_512_tmp_1 = _mm512_permutex2var_ps(result_512_0, shuffle_idx_base1, result_512_8);
        result_512_tmp_2 = _mm512_permutex2var_ps(result_512_1, shuffle_idx_base0, result_512_9);
        result_512_tmp_3 = _mm512_permutex2var_ps(result_512_1, shuffle_idx_base1, result_512_9);
        STORE16_COMPLETE_RESULT(result_512_tmp_0, (C_addr))
        STORE16_MASK_COMPLETE_RESULT(result_512_tmp_1, (C_addr + 16), tail_mask)
        STORE16_COMPLETE_RESULT(result_512_tmp_2, (C_addr + ldc*1))
        STORE16_MASK_COMPLETE_RESULT(result_512_tmp_3, (C_addr + ldc*1 + 16), tail_mask)
        result_512_tmp_0 = _mm512_permutex2var_ps(result_512_2, shuffle_idx_base0, result_512_10);
        result_512_tmp_1 = _mm512_permutex2var_ps(result_512_2, shuffle_idx_base1, result_512_10);
        result_512_tmp_2 = _mm512_permutex2var_ps(result_512_3, shuffle_idx_base0, result_512_11);
        result_512_tmp_3 = _mm512_permutex2var_ps(result_512_3, shuffle_idx_base1, result_512_11);
        STORE16_COMPLETE_RESULT(result_512_tmp_0, (C_addr + ldc*2))
        STORE16_MASK_COMPLETE_RESULT(result_512_tmp_1, (C_addr + ldc*2 + 16), tail_mask)
        STORE16_COMPLETE_RESULT(result_512_tmp_2, (C_addr + ldc*3))
        STORE16_MASK_COMPLETE_RESULT(result_512_tmp_3, (C_addr + ldc*3 + 16), tail_mask)
        result_512_tmp_0 = _mm512_permutex2var_ps(result_512_4, shuffle_idx_base0, result_512_12);
        result_512_tmp_1 = _mm512_permutex2var_ps(result_512_4, shuffle_idx_base1, result_512_12);
        result_512_tmp_2 = _mm512_permutex2var_ps(result_512_5, shuffle_idx_base0, result_512_13);
        result_512_tmp_3 = _mm512_permutex2var_ps(result_512_5, shuffle_idx_base1, result_512_13);
        STORE16_COMPLETE_RESULT(result_512_tmp_0, (C_addr + ldc*4))
        STORE16_MASK_COMPLETE_RESULT(result_512_tmp_1, (C_addr + ldc*4 + 16), tail_mask)
        STORE16_COMPLETE_RESULT(result_512_tmp_2, (C_addr + ldc*5))
        STORE16_MASK_COMPLETE_RESULT(result_512_tmp_3, (C_addr + ldc*5 + 16), tail_mask)
        result_512_tmp_0 = _mm512_permutex2var_ps(result_512_6, shuffle_idx_base0, result_512_14);
        result_512_tmp_1 = _mm512_permutex2var_ps(result_512_6, shuffle_idx_base1, result_512_14);
        result_512_tmp_2 = _mm512_permutex2var_ps(result_512_7, shuffle_idx_base0, result_512_15);
        result_512_tmp_3 = _mm512_permutex2var_ps(result_512_7, shuffle_idx_base1, result_512_15);
        STORE16_COMPLETE_RESULT(result_512_tmp_0, (C_addr + ldc*6))
        STORE16_MASK_COMPLETE_RESULT(result_512_tmp_1, (C_addr + ldc*6 + 16), tail_mask)
        STORE16_COMPLETE_RESULT(result_512_tmp_2, (C_addr + ldc*7))
        STORE16_MASK_COMPLETE_RESULT(result_512_tmp_3, (C_addr + ldc*7 + 16), tail_mask)
    } else {
        result_512_tmp_0 = _mm512_permutex2var_ps(result_512_0, shuffle_idx_base0, result_512_8);
        result_512_tmp_1 = _mm512_permutex2var_ps(result_512_0, shuffle_idx_base1, result_512_8);
        result_512_tmp_2 = _mm512_permutex2var_ps(result_512_1, shuffle_idx_base0, result_512_9);
        result_512_tmp_3 = _mm512_permutex2var_ps(result_512_1, shuffle_idx_base1, result_512_9);
        STORE16_COMPLETE_RESULT(result_512_tmp_0, (C_addr))
        STORE16_COMPLETE_RESULT(result_512_tmp_1, (C_addr + 16))
        STORE16_COMPLETE_RESULT(result_512_tmp_2, (C_addr + ldc*1))
        STORE16_COMPLETE_RESULT(result_512_tmp_3, (C_addr + ldc*1 + 16))
        result_512_tmp_0 = _mm512_permutex2var_ps(result_512_2, shuffle_idx_base0, result_512_10);
        result_512_tmp_1 = _mm512_permutex2var_ps(result_512_2, shuffle_idx_base1, result_512_10);
        result_512_tmp_2 = _mm512_permutex2var_ps(result_512_3, shuffle_idx_base0, result_512_11);
        result_512_tmp_3 = _mm512_permutex2var_ps(result_512_3, shuffle_idx_base1, result_512_11);
        STORE16_COMPLETE_RESULT(result_512_tmp_0, (C_addr + ldc*2))
        STORE16_COMPLETE_RESULT(result_512_tmp_1, (C_addr + ldc*2 + 16))
        STORE16_COMPLETE_RESULT(result_512_tmp_2, (C_addr + ldc*3))
        STORE16_COMPLETE_RESULT(result_512_tmp_3, (C_addr + ldc*3 + 16))
        result_512_tmp_0 = _mm512_permutex2var_ps(result_512_4, shuffle_idx_base0, result_512_12);
        result_512_tmp_1 = _mm512_permutex2var_ps(result_512_4, shuffle_idx_base1, result_512_12);
        result_512_tmp_2 = _mm512_permutex2var_ps(result_512_5, shuffle_idx_base0, result_512_13);
        result_512_tmp_3 = _mm512_permutex2var_ps(result_512_5, shuffle_idx_base1, result_512_13);
        STORE16_COMPLETE_RESULT(result_512_tmp_0, (C_addr + ldc*4))
        STORE16_COMPLETE_RESULT(result_512_tmp_1, (C_addr + ldc*4 + 16))
        STORE16_COMPLETE_RESULT(result_512_tmp_2, (C_addr + ldc*5))
        STORE16_COMPLETE_RESULT(result_512_tmp_3, (C_addr + ldc*5 + 16))
        result_512_tmp_0 = _mm512_permutex2var_ps(result_512_6, shuffle_idx_base0, result_512_14);
        result_512_tmp_1 = _mm512_permutex2var_ps(result_512_6, shuffle_idx_base1, result_512_14);
        result_512_tmp_2 = _mm512_permutex2var_ps(result_512_7, shuffle_idx_base0, result_512_15);
        result_512_tmp_3 = _mm512_permutex2var_ps(result_512_7, shuffle_idx_base1, result_512_15);
        STORE16_COMPLETE_RESULT(result_512_tmp_0, (C_addr + ldc*6))
        STORE16_COMPLETE_RESULT(result_512_tmp_1, (C_addr + ldc*6 + 16))
        STORE16_COMPLETE_RESULT(result_512_tmp_2, (C_addr + ldc*7))
        STORE16_COMPLETE_RESULT(result_512_tmp_3, (C_addr + ldc*7 + 16))
    }
}

// SBGEMM Kernel for M<=16, N=8, K can be any number
#ifndef ONE_ALPHA      // ALPHA is not ONE
void sbgemm_block_kernel_nn_16x8xK_alpha(BLASLONG m, BLASLONG k, float alpha, bfloat16 *A, bfloat16 *B, float *C, int ldc)
#else                  // ALPHA is ONE
void sbgemm_block_kernel_nn_16x8xK_one(BLASLONG m, BLASLONG k, float alpha, bfloat16 *A, bfloat16 *B, float *C, int ldc)
#endif
{
    bfloat16 * A_addr = A;
    bfloat16 * B_addr = B;
    float    * C_addr = C;

#ifndef ONE_ALPHA
    __m512  ALPHAVECTOR = _mm512_set1_ps(alpha);
#endif

    __m512i arrayA_512_0;
    __m512i arrayB_512_0, arrayB_512_1, arrayB_512_2, arrayB_512_3, arrayB_512_4, arrayB_512_5, arrayB_512_6, arrayB_512_7;
    __m512  result_512_0, result_512_1, result_512_2, result_512_3, result_512_4, result_512_5, result_512_6, result_512_7;

    result_512_0  = _mm512_setzero_ps();
    result_512_1  = _mm512_setzero_ps();
    result_512_2  = _mm512_setzero_ps();
    result_512_3  = _mm512_setzero_ps();
    result_512_4  = _mm512_setzero_ps();
    result_512_5  = _mm512_setzero_ps();
    result_512_6  = _mm512_setzero_ps();
    result_512_7  = _mm512_setzero_ps();

    for (BLASLONG idx_k = 0; idx_k < k; idx_k += 2) {
        // Each two rows are a group for 32-pair bf16 elements
        // Load two rows into a 512 register
        arrayA_512_0 = _mm512_loadu_si512(A_addr);

        _MM512_BROADCASTD_EPI32(B_addr + 0,  arrayB_512_0);
        _MM512_BROADCASTD_EPI32(B_addr + 2,  arrayB_512_1);
        _MM512_BROADCASTD_EPI32(B_addr + 4,  arrayB_512_2);
        _MM512_BROADCASTD_EPI32(B_addr + 6,  arrayB_512_3);
        _MM512_BROADCASTD_EPI32(B_addr + 8,  arrayB_512_4);
        _MM512_BROADCASTD_EPI32(B_addr + 10, arrayB_512_5);
        _MM512_BROADCASTD_EPI32(B_addr + 12, arrayB_512_6);
        _MM512_BROADCASTD_EPI32(B_addr + 14, arrayB_512_7);

        result_512_0  = _mm512_dpbf16_ps(result_512_0,  (__m512bh) arrayA_512_0, (__m512bh) arrayB_512_0);
        result_512_1  = _mm512_dpbf16_ps(result_512_1,  (__m512bh) arrayA_512_0, (__m512bh) arrayB_512_1);
        result_512_2  = _mm512_dpbf16_ps(result_512_2,  (__m512bh) arrayA_512_0, (__m512bh) arrayB_512_2);
        result_512_3  = _mm512_dpbf16_ps(result_512_3,  (__m512bh) arrayA_512_0, (__m512bh) arrayB_512_3);
        result_512_4  = _mm512_dpbf16_ps(result_512_4,  (__m512bh) arrayA_512_0, (__m512bh) arrayB_512_4);
        result_512_5  = _mm512_dpbf16_ps(result_512_5,  (__m512bh) arrayA_512_0, (__m512bh) arrayB_512_5);
        result_512_6  = _mm512_dpbf16_ps(result_512_6,  (__m512bh) arrayA_512_0, (__m512bh) arrayB_512_6);
        result_512_7  = _mm512_dpbf16_ps(result_512_7,  (__m512bh) arrayA_512_0, (__m512bh) arrayB_512_7);

        // Load B with unroll 8
        B_addr += 16;
        // Load A with unroll 16
        A_addr += 32;
    }

    if (m != 16) {
        unsigned short tail_mask = (((unsigned short)0xffff) >> (16-m));

        result_512_0 = _mm512_shuffle_f32x4(result_512_0, result_512_0, 0xd8);
        result_512_1 = _mm512_shuffle_f32x4(result_512_1, result_512_1, 0xd8);
        result_512_2 = _mm512_shuffle_f32x4(result_512_2, result_512_2, 0xd8);
        result_512_3 = _mm512_shuffle_f32x4(result_512_3, result_512_3, 0xd8);
        STORE16_MASK_COMPLETE_RESULT(result_512_0, (C_addr), tail_mask)
        STORE16_MASK_COMPLETE_RESULT(result_512_1, (C_addr + ldc*1), tail_mask)
        STORE16_MASK_COMPLETE_RESULT(result_512_2, (C_addr + ldc*2), tail_mask)
        STORE16_MASK_COMPLETE_RESULT(result_512_3, (C_addr + ldc*3), tail_mask)
        result_512_4 = _mm512_shuffle_f32x4(result_512_4, result_512_4, 0xd8);
        result_512_5 = _mm512_shuffle_f32x4(result_512_5, result_512_5, 0xd8);
        result_512_6 = _mm512_shuffle_f32x4(result_512_6, result_512_6, 0xd8);
        result_512_7 = _mm512_shuffle_f32x4(result_512_7, result_512_7, 0xd8);
        STORE16_MASK_COMPLETE_RESULT(result_512_4, (C_addr + ldc*4), tail_mask)
        STORE16_MASK_COMPLETE_RESULT(result_512_5, (C_addr + ldc*5), tail_mask)
        STORE16_MASK_COMPLETE_RESULT(result_512_6, (C_addr + ldc*6), tail_mask)
        STORE16_MASK_COMPLETE_RESULT(result_512_7, (C_addr + ldc*7), tail_mask)
    } else {
        result_512_0 = _mm512_shuffle_f32x4(result_512_0, result_512_0, 0xd8);
        result_512_1 = _mm512_shuffle_f32x4(result_512_1, result_512_1, 0xd8);
        result_512_2 = _mm512_shuffle_f32x4(result_512_2, result_512_2, 0xd8);
        result_512_3 = _mm512_shuffle_f32x4(result_512_3, result_512_3, 0xd8);
        STORE16_COMPLETE_RESULT(result_512_0, (C_addr))
        STORE16_COMPLETE_RESULT(result_512_1, (C_addr + ldc*1))
        STORE16_COMPLETE_RESULT(result_512_2, (C_addr + ldc*2))
        STORE16_COMPLETE_RESULT(result_512_3, (C_addr + ldc*3))
        result_512_4 = _mm512_shuffle_f32x4(result_512_4, result_512_4, 0xd8);
        result_512_5 = _mm512_shuffle_f32x4(result_512_5, result_512_5, 0xd8);
        result_512_6 = _mm512_shuffle_f32x4(result_512_6, result_512_6, 0xd8);
        result_512_7 = _mm512_shuffle_f32x4(result_512_7, result_512_7, 0xd8);
        STORE16_COMPLETE_RESULT(result_512_4, (C_addr + ldc*4))
        STORE16_COMPLETE_RESULT(result_512_5, (C_addr + ldc*5))
        STORE16_COMPLETE_RESULT(result_512_6, (C_addr + ldc*6))
        STORE16_COMPLETE_RESULT(result_512_7, (C_addr + ldc*7))
    }
}

// SBGEMM Kernel for 16<M<=32, N<8, K can be any number, but the processing will take 32 as a base
#ifndef ONE_ALPHA      // ALPHA is not ONE
void sbgemm_block_kernel_nn_32xNx32_alpha(BLASLONG m, BLASLONG n, BLASLONG k, float alpha, bfloat16 *A, bfloat16 *B, float *C, int ldc)
#else                  // ALPHA is ONE
void sbgemm_block_kernel_nn_32xNx32_one(BLASLONG m, BLASLONG n, BLASLONG k, float alpha, bfloat16 *A, bfloat16 *B, float *C, int ldc)
#endif
{
    bfloat16 * A_addr = A;
    bfloat16 * B_addr = B;
    float    * C_addr = C;

    BLASLONG tag_k_32x = k & (~31);

#ifndef ONE_ALPHA
    __m512  ALPHAVECTOR = _mm512_set1_ps(alpha);
#endif

    __m512i arrayA_512[2];
    __m512i arrayB_512[8];
    __m512  result_512[16];
    __m512  result_512_tmp_0, result_512_tmp_1;

    __m512i M512_EPI32_8      = _mm512_set1_epi32(8);
    __m512i shuffle_idx_base0 = _mm512_set_epi32(23, 22, 21, 20, 7, 6, 5, 4, 19, 18, 17, 16, 3, 2, 1, 0);
    __m512i shuffle_idx_base1 = _mm512_add_epi32(shuffle_idx_base0, M512_EPI32_8);

    for (int i = 0; i < 15; i += 2) {
        result_512[i]    = _mm512_setzero_ps();
        result_512[i+1]  = _mm512_setzero_ps();
    }

    for (BLASLONG idx_k = 0; idx_k < tag_k_32x; idx_k += 32) {
        // Load B with unroll n
        for (int i = 0; i < n; i ++) {
            arrayB_512[i] = _mm512_loadu_si512(B_addr);
            B_addr += 32;
        }

        for (BLASLONG idx = 0; idx < 32;) {
            // Each two rows are a group for 32-pair bf16 elements
            arrayA_512[0] = _mm512_loadu_si512(A_addr);
            arrayA_512[1] = _mm512_loadu_si512(A_addr + 32);
            A_addr += 64;

            for (int i = 0; i < n; i++) {
                result_512[i]   = _mm512_dpbf16_ps(result_512[i]  , (__m512bh) arrayA_512[0], (__m512bh) _mm512_broadcastd_epi32(_mm512_castsi512_si128(arrayB_512[i])));
                result_512[i+8] = _mm512_dpbf16_ps(result_512[i+8], (__m512bh) arrayA_512[1], (__m512bh) _mm512_broadcastd_epi32(_mm512_castsi512_si128(arrayB_512[i])));
                arrayB_512[i]   = _mm512_shuffle_epi32(arrayB_512[i], SHUFFLE_MAGIC_NO);
            }

            idx += 2;
            // Every 4 loops we need to switch to next 128 bits of arrayB registers
            if ((idx & (~7)) == idx) {
                for (int i = 0; i < n; i++) {
                    arrayB_512[i] = _mm512_shuffle_i32x4(arrayB_512[i], arrayB_512[i], SHUFFLE_MAGIC_NO);
                }
            }
        }
    }

    if (tag_k_32x != k) {
        // Load B with unroll n
        for (int i = 0; i < n; i ++) {
            arrayB_512[i] = _mm512_loadu_si512(B_addr);
            B_addr += 32;
        }

        BLASLONG width = k - tag_k_32x;
        for (BLASLONG idx = 0; idx < width;) {
            // Each two rows are a group for 32-pair bf16 elements
            arrayA_512[0] = _mm512_loadu_si512(A_addr);
            arrayA_512[1] = _mm512_loadu_si512(A_addr + 32);
            A_addr += 64;

            for (int i = 0; i < n; i++) {
                result_512[i]   = _mm512_dpbf16_ps(result_512[i]  , (__m512bh) arrayA_512[0], (__m512bh) _mm512_broadcastd_epi32(_mm512_castsi512_si128(arrayB_512[i])));
                result_512[i+8] = _mm512_dpbf16_ps(result_512[i+8], (__m512bh) arrayA_512[1], (__m512bh) _mm512_broadcastd_epi32(_mm512_castsi512_si128(arrayB_512[i])));
                arrayB_512[i]   = _mm512_shuffle_epi32(arrayB_512[i], SHUFFLE_MAGIC_NO);
            }

            idx += 2;
            // Every 4 loops we need to switch to next 128 bits of arrayB registers
            if ((idx & (~7)) == idx) {
                for (int i = 0; i < n; i++) {
                    arrayB_512[i] = _mm512_shuffle_i32x4(arrayB_512[i], arrayB_512[i], SHUFFLE_MAGIC_NO);
                }
            }
        }
    }

    if (m != 32) {
        unsigned short tail_mask = (((unsigned short)0xffff) >> (32-m));
        for (int i = 0; i < n; i++) {
            result_512_tmp_0 = _mm512_permutex2var_ps(result_512[i], shuffle_idx_base0, result_512[i+8]);
            result_512_tmp_1 = _mm512_permutex2var_ps(result_512[i], shuffle_idx_base1, result_512[i+8]);
            STORE16_COMPLETE_RESULT(result_512_tmp_0, (C_addr + ldc*i))
            STORE16_MASK_COMPLETE_RESULT(result_512_tmp_1, (C_addr + ldc*i + 16), tail_mask)
        }
    } else {
        for (int i = 0; i < n; i++) {
            result_512_tmp_0 = _mm512_permutex2var_ps(result_512[i], shuffle_idx_base0, result_512[i+8]);
            result_512_tmp_1 = _mm512_permutex2var_ps(result_512[i], shuffle_idx_base1, result_512[i+8]);
            STORE16_COMPLETE_RESULT(result_512_tmp_0, (C_addr + ldc*i))
            STORE16_COMPLETE_RESULT(result_512_tmp_1, (C_addr + ldc*i + 16))
        }
    }
}

// SBGEMM Kernel for 16<=M, N<8, K can be any number, but the processing will take 32 as a base
#ifndef ONE_ALPHA      // ALPHA is not ONE
void sbgemm_block_kernel_nn_16xNx32_alpha(BLASLONG m, BLASLONG n, BLASLONG k, float alpha, bfloat16 *A, bfloat16 *B, float *C, int ldc)
#else                  // ALPHA is ONE
void sbgemm_block_kernel_nn_16xNx32_one(BLASLONG m, BLASLONG n, BLASLONG k, float alpha, bfloat16 *A, bfloat16 *B, float *C, int ldc)
#endif
{
    bfloat16 * A_addr = A;
    bfloat16 * B_addr = B;
    float    * C_addr = C;

    BLASLONG tag_k_32x = k & (~31);

#ifndef ONE_ALPHA
    __m512  ALPHAVECTOR = _mm512_set1_ps(alpha);
#endif

    __m512i arrayA_512;
    __m512i arrayB_512[8];
    __m512  result_512[8];

    for (int i = 0; i < 8; i += 2) {
        result_512[i]    = _mm512_setzero_ps();
        result_512[i+1]  = _mm512_setzero_ps();
    }

    for (BLASLONG idx_k = 0; idx_k < tag_k_32x; idx_k += 32) {
        // Load B with unroll n
        for (int i = 0; i < n; i++) {
            arrayB_512[i] = _mm512_loadu_si512(B_addr);
            B_addr += 32;
        }

        for (BLASLONG idx = 0; idx < 32;) {
            // Each two rows are a group for 32-pair bf16 elements
            // Load two rows into a 512 register
            arrayA_512 = _mm512_loadu_si512(A_addr);
            A_addr += 32;

            for (int i = 0; i < n; i ++) {
                result_512[i]  = _mm512_dpbf16_ps(result_512[i],  (__m512bh) arrayA_512, (__m512bh) _mm512_broadcastd_epi32(_mm512_castsi512_si128(arrayB_512[i])));
                arrayB_512[i] = _mm512_shuffle_epi32(arrayB_512[i], SHUFFLE_MAGIC_NO);
            }

            idx += 2;
            // Every 4 loops we need to switch to next 128 bits of arrayB registers
            if ((idx & (~7)) == idx) {
                for (int i = 0; i < n; i++) {
                    arrayB_512[i] = _mm512_shuffle_i32x4(arrayB_512[i], arrayB_512[i], SHUFFLE_MAGIC_NO);
                }
            }
        }
    }

    if (tag_k_32x != k) {
        // Load B with unroll n
        for (int i = 0; i < n; i++) {
            arrayB_512[i] = _mm512_loadu_si512(B_addr);
            B_addr += 32;
        }

        BLASLONG width = k - tag_k_32x;
        for (BLASLONG idx = 0; idx < width;) {
            // Each two rows are a group for 32-pair bf16 elements
            // Load two rows into a 512 register
            arrayA_512 = _mm512_loadu_si512(A_addr);
            A_addr += 32;

            for (int i = 0; i < n; i++) {
                result_512[i]  = _mm512_dpbf16_ps(result_512[i],  (__m512bh) arrayA_512, (__m512bh) _mm512_broadcastd_epi32(_mm512_castsi512_si128(arrayB_512[i])));
                arrayB_512[i] = _mm512_shuffle_epi32(arrayB_512[i], SHUFFLE_MAGIC_NO);
            }

            idx += 2;
            // Every 4 loops we need to switch to next 128 bits of arrayB registers
            if ((idx & (~7)) == idx) {
                for (int i = 0; i < n; i++) {
                    arrayB_512[i] = _mm512_shuffle_i32x4(arrayB_512[i], arrayB_512[i], SHUFFLE_MAGIC_NO);
                }
            }
        }
    }

    if (m != 16) {
        unsigned short tail_mask = (((unsigned short)0xffff) >> (16-m));
        for (int i = 0; i < n; i++) {
            result_512[i] = _mm512_shuffle_f32x4(result_512[i], result_512[i], 0xd8);
            STORE16_MASK_COMPLETE_RESULT(result_512[i], (C_addr + ldc*i), tail_mask)
        }
    } else {
        for (int i = 0; i < n; i++) {
            result_512[i] = _mm512_shuffle_f32x4(result_512[i], result_512[i], 0xd8);
            STORE16_COMPLETE_RESULT(result_512[i], (C_addr + ldc*i))
        }
    }
}


#ifndef ONE_ALPHA      // ALPHA is not ONE
void sbgemm_blocking_kernel_nn_alpha(blasint M, blasint N, blasint K, float alpha, bfloat16 *A, blasint lda, bfloat16 *B, blasint ldb, float *C, blasint ldc, bfloat16 * block_A, bfloat16 * block_B)
#else                  // ALPHA is ONE
void sbgemm_blocking_kernel_nn_one(blasint M, blasint N, blasint K, float alpha, bfloat16 *A, blasint lda, bfloat16 *B, blasint ldb, float *C, blasint ldc, bfloat16 * block_A, bfloat16 * block_B)
#endif
{
    BLASLONG m_step, n_step, k_step, k_step_round32;
    BLASLONG tag_m_Nx = M & (~(BF16_BLOCK_THRES_M-1));

    BLASLONG n_from, n_to;
    BLASLONG tag_n_Nx;

    n_from = 0;
    n_to = (BF16_BLOCK_THRES_N > N) ? N : BF16_BLOCK_THRES_N;
    tag_n_Nx = n_to & (~(BF16_BLOCK_STEP_N-1));

    k_step = (K > BF16_BLOCK_THRES_K) ? BF16_BLOCK_THRES_K : K;
    k_step_round32 = k_step & (~31);
    k_step_round32 = (k_step > k_step_round32) ? (k_step_round32 + 32) : k_step_round32;

    if (M >= BF16_BLOCK_THRES_M) {
        while (n_from < N) {
            for (BLASLONG idx_k = 0; idx_k < K;) {
                // Use Kx32 kernel when BF16_BLOCK_THRES_M==32, Kx16 kernel when BF16_BLOCK_THRES_M==16, ...
                COL_MAJOR_INCOPY_KERNEL_Kx32(k_step, 32, &A(idx_k, 0), lda, block_A);
                for (BLASLONG idx_n = n_from; idx_n < tag_n_Nx; idx_n += BF16_BLOCK_STEP_N) {
                    // Use 8x32 kernel when BF16_BLOCK_THRES_N==8, 4x32 kernel when BF16_BLOCK_THRES_N==4, ...
                    COL_MAJOR_ONCOPY_KERNEL_8x32(k_step, &B(idx_n, idx_k), ldb, block_B + (idx_n-n_from)*k_step_round32);
                    SBGEMM_BLOCK_KERNEL_NN_32x8xK(32, k_step, alpha, block_A, block_B + (idx_n-n_from)*k_step_round32, &C(idx_n, 0), ldc);
                }

                if (tag_n_Nx != n_to) {
                    n_step = n_to - tag_n_Nx;
                    COL_MAJOR_ONCOPY_KERNEL_Nx32(n_step, k_step, &B(tag_n_Nx, idx_k), ldb, block_B + (tag_n_Nx-n_from)*k_step_round32);
                    SBGEMM_BLOCK_KERNEL_NN_32xNx32(32, n_step, k_step, alpha, block_A, block_B + (tag_n_Nx-n_from)*k_step_round32, &C(tag_n_Nx, 0), ldc);
                }

                for (BLASLONG idx_m = BF16_BLOCK_THRES_M; idx_m < tag_m_Nx; idx_m += BF16_BLOCK_THRES_M) {
                    COL_MAJOR_INCOPY_KERNEL_Kx32(k_step, 32, &A(idx_k, idx_m), lda, block_A);
                    for (BLASLONG idx_n = n_from; idx_n < tag_n_Nx; idx_n += BF16_BLOCK_STEP_N) {
                        SBGEMM_BLOCK_KERNEL_NN_32x8xK(32, k_step, alpha, block_A, block_B + (idx_n-n_from)*k_step_round32, &C(idx_n, idx_m), ldc);
                    }

                    if (tag_n_Nx != n_to) {
                        n_step = n_to - tag_n_Nx;
                        SBGEMM_BLOCK_KERNEL_NN_32xNx32(32, n_step, k_step, alpha, block_A, block_B + (tag_n_Nx-n_from)*k_step_round32, &C(tag_n_Nx, idx_m), ldc);
                    }
                }

                if (tag_m_Nx != M) {
                    m_step = M - tag_m_Nx;
                    if (m_step > 16) {
                        COL_MAJOR_INCOPY_KERNEL_Kx32(k_step, m_step, &A(idx_k, tag_m_Nx), lda, block_A);
                        for (BLASLONG idx_n = n_from; idx_n < tag_n_Nx; idx_n += BF16_BLOCK_STEP_N) {
                            SBGEMM_BLOCK_KERNEL_NN_32x8xK(m_step, k_step, alpha, block_A, block_B + (idx_n-n_from)*k_step_round32, &C(idx_n, tag_m_Nx), ldc);
                        }

                        if (tag_n_Nx != n_to) {
                            n_step = n_to - tag_n_Nx;
                            SBGEMM_BLOCK_KERNEL_NN_32xNx32(m_step, n_step, k_step, alpha, block_A, block_B + (tag_n_Nx-n_from)*k_step_round32, &C(tag_n_Nx, tag_m_Nx), ldc);
                        }
                    } else {
                        COL_MAJOR_INCOPY_KERNEL_Kx16(k_step, m_step, &A(idx_k, tag_m_Nx), lda, block_A);
                        for (BLASLONG idx_n = n_from; idx_n < tag_n_Nx; idx_n += BF16_BLOCK_STEP_N) {
                            SBGEMM_BLOCK_KERNEL_NN_16x8xK(m_step, k_step, alpha, block_A, block_B + (idx_n-n_from)*k_step_round32, &C(idx_n, tag_m_Nx), ldc);
                        }

                        if (tag_n_Nx != n_to) {
                            n_step = n_to - tag_n_Nx;
                            SBGEMM_BLOCK_KERNEL_NN_16xNx32(m_step, n_step, k_step, alpha, block_A, block_B + (tag_n_Nx-n_from)*k_step_round32, &C(tag_n_Nx, tag_m_Nx), ldc);
                        }
                    }
                }

                idx_k += k_step;
                k_step = K - idx_k;
                k_step = (k_step > BF16_BLOCK_THRES_K) ? BF16_BLOCK_THRES_K : k_step;
                k_step_round32 = k_step & (~31);
                k_step_round32 = (k_step > k_step_round32) ? (k_step_round32 + 32) : k_step_round32;
            }

            n_from = n_to;
            n_to += BF16_BLOCK_THRES_N;
            n_to = (n_to > N) ? N : n_to;
            tag_n_Nx = n_to & (~(BF16_BLOCK_STEP_N-1));           
        }
    } else {
        m_step = M;
        if (m_step > 16) {
            while (n_from < N) {
                for (BLASLONG idx_k = 0; idx_k < K;) {
                    // Use Kx32 kernel when BF16_BLOCK_THRES_M==32, Kx16 kernel when BF16_BLOCK_THRES_M==16, ...
                    COL_MAJOR_INCOPY_KERNEL_Kx32(k_step, m_step, &A(idx_k, 0), lda, block_A);
                    for (BLASLONG idx_n = n_from; idx_n < tag_n_Nx; idx_n += BF16_BLOCK_STEP_N) {
                        // Use 8x32 kernel when BF16_BLOCK_THRES_N==8, 4x32 kernel when BF16_BLOCK_THRES_N==4, ...
                        COL_MAJOR_ONCOPY_KERNEL_8x32(k_step, &B(idx_n, idx_k), ldb, block_B + (idx_n-n_from)*k_step_round32);
                        SBGEMM_BLOCK_KERNEL_NN_32x8xK(m_step, k_step, alpha, block_A, block_B + (idx_n-n_from)*k_step_round32, &C(idx_n, 0), ldc);
                    }

                    if (tag_n_Nx != n_to) {
                        n_step = n_to - tag_n_Nx;
                        COL_MAJOR_ONCOPY_KERNEL_Nx32(n_step, k_step, &B(tag_n_Nx, idx_k), ldb, block_B + (tag_n_Nx-n_from)*k_step_round32);
                        SBGEMM_BLOCK_KERNEL_NN_32xNx32(m_step, n_step, k_step, alpha, block_A, block_B + (tag_n_Nx-n_from)*k_step_round32, &C(tag_n_Nx, 0), ldc);
                    }

                    idx_k += k_step;
                    k_step = K - idx_k;
                    k_step = (k_step > BF16_BLOCK_THRES_K) ? BF16_BLOCK_THRES_K : k_step;
                    k_step_round32 = k_step & (~31);
                    k_step_round32 = (k_step > k_step_round32) ? (k_step_round32 + 32) : k_step_round32;
                }
                n_from = n_to;
                n_to += BF16_BLOCK_THRES_N;
                n_to = (n_to > N) ? N : n_to;
                tag_n_Nx = n_to & (~(BF16_BLOCK_STEP_N-1));
            }
        } else {
            while (n_from < N) {
                for (BLASLONG idx_k = 0; idx_k < K;) {
                    COL_MAJOR_INCOPY_KERNEL_Kx16(k_step, m_step, &A(idx_k, 0), lda, block_A);
                    for (BLASLONG idx_n = n_from; idx_n < tag_n_Nx; idx_n += BF16_BLOCK_STEP_N) {
                        // Use 8x32 kernel when BF16_BLOCK_THRES_N==8, 4x32 kernel when BF16_BLOCK_THRES_N==4, ...
                        COL_MAJOR_ONCOPY_KERNEL_8x32(k_step, &B(idx_n, idx_k), ldb, block_B + (idx_n-n_from)*k_step_round32);
                        SBGEMM_BLOCK_KERNEL_NN_16x8xK(m_step, k_step, alpha, block_A, block_B + (idx_n-n_from)*k_step_round32, &C(idx_n, 0), ldc);
                    }

                    if (tag_n_Nx != n_to) {
                        n_step = n_to - tag_n_Nx;
                        COL_MAJOR_ONCOPY_KERNEL_Nx32(n_step, k_step, &B(tag_n_Nx, idx_k), ldb, block_B + (tag_n_Nx-n_from)*k_step_round32);
                        SBGEMM_BLOCK_KERNEL_NN_16xNx32(m_step, n_step, k_step, alpha, block_A, block_B + (tag_n_Nx-n_from)*k_step_round32, &C(tag_n_Nx, 0), ldc);
                    }

                    idx_k += k_step;
                    k_step = K - idx_k;
                    k_step = (k_step > BF16_BLOCK_THRES_K) ? BF16_BLOCK_THRES_K : k_step;
                    k_step_round32 = k_step & (~31);
                    k_step_round32 = (k_step > k_step_round32) ? (k_step_round32 + 32) : k_step_round32;
                }
                n_from = n_to;
                n_to += BF16_BLOCK_THRES_N;
                n_to = (n_to > N) ? N : n_to;
                tag_n_Nx = n_to & (~(BF16_BLOCK_STEP_N-1));
            }
        }
    }
}
/* ----------------------------------------- End of NN kernels --------------------------------------- */

/* --------------------------------------------- NT kernels ------------------------------------------ */
// SBGEMM Kernel for 16<M<=32, N<8, K can be any number
#ifndef ONE_ALPHA      // ALPHA is not ONE
void sbgemm_block_kernel_nt_32xNxK_alpha(BLASLONG m, BLASLONG n, BLASLONG k, float alpha, bfloat16 *A, bfloat16 *B, float *C, int ldc)
#else                  // ALPHA is ONE
void sbgemm_block_kernel_nt_32xNxK_one(BLASLONG m, BLASLONG n, BLASLONG k, float alpha, bfloat16 *A, bfloat16 *B, float *C, int ldc)
#endif
{
    bfloat16 * A_addr = A;
    bfloat16 * B_addr = B;
    float    * C_addr = C;

#ifndef ONE_ALPHA
    __m512  ALPHAVECTOR = _mm512_set1_ps(alpha);
#endif

    __m512i arrayA_512_0, arrayA_512_1;
    __m512i arrayB_512[8];
    __m512  result_512[16];
    __m512  result_512_tmp_0, result_512_tmp_1;

    __m512i M512_EPI32_8      = _mm512_set1_epi32(8);
    __m512i shuffle_idx_base0 = _mm512_set_epi32(23, 22, 21, 20, 7, 6, 5, 4, 19, 18, 17, 16, 3, 2, 1, 0);
    __m512i shuffle_idx_base1 = _mm512_add_epi32(shuffle_idx_base0, M512_EPI32_8);

    result_512[0]  = _mm512_setzero_ps();
    result_512[1]  = _mm512_setzero_ps();
    result_512[2]  = _mm512_setzero_ps();
    result_512[3]  = _mm512_setzero_ps();
    result_512[4]  = _mm512_setzero_ps();
    result_512[5]  = _mm512_setzero_ps();
    result_512[6]  = _mm512_setzero_ps();
    result_512[7]  = _mm512_setzero_ps();
    result_512[8]  = _mm512_setzero_ps();
    result_512[9]  = _mm512_setzero_ps();
    result_512[10] = _mm512_setzero_ps();
    result_512[11] = _mm512_setzero_ps();
    result_512[12] = _mm512_setzero_ps();
    result_512[13] = _mm512_setzero_ps();
    result_512[14] = _mm512_setzero_ps();
    result_512[15] = _mm512_setzero_ps();

    for (BLASLONG idx_k = 0; idx_k < k; idx_k += 2) {
        // Each two rows are a group for 32-pair bf16 elements
        arrayA_512_0 = _mm512_loadu_si512(A_addr);
        arrayA_512_1 = _mm512_loadu_si512(A_addr + 32);
        A_addr += 64;

        for (int i = 0; i < n; i ++) {
            _MM512_BROADCASTD_EPI32(B_addr + i*2,  arrayB_512[i]);
        }
        B_addr += 16;

        for (int i = 0; i < n; i ++) {
            result_512[i] = _mm512_dpbf16_ps(result_512[i],  (__m512bh) arrayA_512_0, (__m512bh) arrayB_512[i]);
            result_512[i+8] = _mm512_dpbf16_ps(result_512[i+8],  (__m512bh) arrayA_512_1, (__m512bh) arrayB_512[i]);
        }
    }

    if (m != 32) {
        unsigned short tail_mask = (((unsigned short)0xffff) >> (32-m));
        for (int i = 0; i < n; i ++) {
            result_512_tmp_0 = _mm512_permutex2var_ps(result_512[i], shuffle_idx_base0, result_512[i+8]);
            result_512_tmp_1 = _mm512_permutex2var_ps(result_512[i], shuffle_idx_base1, result_512[i+8]);
            STORE16_COMPLETE_RESULT(result_512_tmp_0, (C_addr + ldc*i))
            STORE16_MASK_COMPLETE_RESULT(result_512_tmp_1, (C_addr + ldc*i + 16), tail_mask)
        }
    } else {
        for (int i = 0; i < n; i ++) {
            result_512_tmp_0 = _mm512_permutex2var_ps(result_512[i], shuffle_idx_base0, result_512[i+8]);
            result_512_tmp_1 = _mm512_permutex2var_ps(result_512[i], shuffle_idx_base1, result_512[i+8]);
            STORE16_COMPLETE_RESULT(result_512_tmp_0, (C_addr + ldc*i))
            STORE16_COMPLETE_RESULT(result_512_tmp_1, (C_addr + ldc*i + 16))
        }
    }
}

// SBGEMM Kernel for M<=16, N<8, K can be any number
#ifndef ONE_ALPHA      // ALPHA is not ONE
void sbgemm_block_kernel_nt_16xNxK_alpha(BLASLONG m, BLASLONG n, BLASLONG k, float alpha, bfloat16 *A, bfloat16 *B, float *C, int ldc)
#else                  // ALPHA is ONE
void sbgemm_block_kernel_nt_16xNxK_one(BLASLONG m, BLASLONG n, BLASLONG k, float alpha, bfloat16 *A, bfloat16 *B, float *C, int ldc)
#endif
{
    bfloat16 * A_addr = A;
    bfloat16 * B_addr = B;
    float    * C_addr = C;

#ifndef ONE_ALPHA
    __m512  ALPHAVECTOR = _mm512_set1_ps(alpha);
#endif

    __m512i arrayA_512_0;
    __m512i arrayB_512[8];
    __m512  result_512[8];

    result_512[0]  = _mm512_setzero_ps();
    result_512[1]  = _mm512_setzero_ps();
    result_512[2]  = _mm512_setzero_ps();
    result_512[3]  = _mm512_setzero_ps();
    result_512[4]  = _mm512_setzero_ps();
    result_512[5]  = _mm512_setzero_ps();
    result_512[6]  = _mm512_setzero_ps();
    result_512[7]  = _mm512_setzero_ps();

    for (BLASLONG idx_k = 0; idx_k < k; idx_k += 2) {
        // Each two rows are a group for 16-pair bf16 elements
        // Load two rows into a 512 register
        arrayA_512_0 = _mm512_loadu_si512(A_addr);
        A_addr += 32;

        for (int i = 0; i < n; i ++) {
            _MM512_BROADCASTD_EPI32(B_addr + i*2,  arrayB_512[i]);
        }
        B_addr += 16;

        for (int i = 0; i < n; i ++) {
            result_512[i] = _mm512_dpbf16_ps(result_512[i],  (__m512bh) arrayA_512_0, (__m512bh) arrayB_512[i]);
        }
    }

    if (m != 16) {
        unsigned short tail_mask = (((unsigned short)0xffff) >> (16-m));
        for (int i = 0; i < n; i++) {
            result_512[i] = _mm512_shuffle_f32x4(result_512[i], result_512[i], 0xd8);
            STORE16_MASK_COMPLETE_RESULT(result_512[i], (C_addr + ldc*i), tail_mask)
        }
    } else {
        for (int i = 0; i < n; i++) {
            result_512[i] = _mm512_shuffle_f32x4(result_512[i], result_512[i], 0xd8);
            STORE16_COMPLETE_RESULT(result_512[i], (C_addr + ldc*i))
        }
    }
}

#ifndef ONE_ALPHA      // ALPHA is not ONE
void sbgemm_blocking_kernel_nt_alpha(blasint M, blasint N, blasint K, float alpha, bfloat16 *A, blasint lda, bfloat16 *B, blasint ldb, float *C, blasint ldc, bfloat16 * block_A, bfloat16 * block_B)
#else                  // ALPHA is ONE
void sbgemm_blocking_kernel_nt_one(blasint M, blasint N, blasint K, float alpha, bfloat16 *A, blasint lda, bfloat16 *B, blasint ldb, float *C, blasint ldc, bfloat16 * block_A, bfloat16 * block_B)
#endif
{
    BLASLONG m_step, n_step, k_step, k_step_round32;
    BLASLONG tag_m_Nx = M & (~(BF16_BLOCK_THRES_M-1));

    BLASLONG n_from, n_to;
    BLASLONG tag_n_Nx;

    n_from = 0;
    n_to = (BF16_BLOCK_THRES_N > N) ? N : BF16_BLOCK_THRES_N;
    tag_n_Nx = n_to & (~(BF16_BLOCK_STEP_N-1));

    k_step = (K > BF16_BLOCK_THRES_K) ? BF16_BLOCK_THRES_K : K;
    k_step_round32 = k_step & (~31);
    k_step_round32 = (k_step > k_step_round32) ? (k_step_round32 + 32) : k_step_round32;

    if (M >= BF16_BLOCK_THRES_M) {
        while (n_from < N) {
            for (BLASLONG idx_k = 0; idx_k < K;) {
                // Use Kx32 kernel when BF16_BLOCK_THRES_M==32, Kx16 kernel when BF16_BLOCK_THRES_M==16, ...
                COL_MAJOR_INCOPY_KERNEL_Kx32(k_step, 32, &A(idx_k, 0), lda, block_A);
                for (BLASLONG idx_n = n_from; idx_n < tag_n_Nx; idx_n += BF16_BLOCK_STEP_N) {
                    // Use 8x32 kernel when BF16_BLOCK_THRES_N==8, 4x32 kernel when BF16_BLOCK_THRES_N==4, ...
                    COL_MAJOR_OTCOPY_KERNEL_Kx8(k_step, &B(idx_k, idx_n), ldb, block_B + (idx_n-n_from)*k_step_round32);
                    SBGEMM_BLOCK_KERNEL_NT_32x8xK(32, k_step, alpha, block_A, block_B + (idx_n-n_from)*k_step_round32, &C(idx_n, 0), ldc);
                }

                if (tag_n_Nx != n_to) {
                    n_step = n_to - tag_n_Nx;
                    COL_MAJOR_OTCOPY_KERNEL_Kx8m(k_step, n_step, &B(idx_k, tag_n_Nx), ldb, block_B + (tag_n_Nx-n_from)*k_step_round32);
                    SBGEMM_BLOCK_KERNEL_NT_32xNxK(32, n_step, k_step, alpha, block_A, block_B + (tag_n_Nx-n_from)*k_step_round32, &C(tag_n_Nx, 0), ldc);
                }

                for (BLASLONG idx_m = BF16_BLOCK_THRES_M; idx_m < tag_m_Nx; idx_m += BF16_BLOCK_THRES_M) {
                    COL_MAJOR_INCOPY_KERNEL_Kx32(k_step, 32, &A(idx_k, idx_m), lda, block_A);
                    for (BLASLONG idx_n = n_from; idx_n < tag_n_Nx; idx_n += BF16_BLOCK_STEP_N) {
                        SBGEMM_BLOCK_KERNEL_NT_32x8xK(32, k_step, alpha, block_A, block_B + (idx_n-n_from)*k_step_round32, &C(idx_n, idx_m), ldc);
                    }

                    if (tag_n_Nx != n_to) {
                        n_step = n_to - tag_n_Nx;
                        SBGEMM_BLOCK_KERNEL_NT_32xNxK(32, n_step, k_step, alpha, block_A, block_B + (tag_n_Nx-n_from)*k_step_round32, &C(tag_n_Nx, idx_m), ldc);
                    }
                }

                if (tag_m_Nx != M) {
                    m_step = M - tag_m_Nx;
                    if (m_step > 16) {
                        COL_MAJOR_INCOPY_KERNEL_Kx32(k_step, m_step, &A(idx_k, tag_m_Nx), lda, block_A);
                        for (BLASLONG idx_n = n_from; idx_n < tag_n_Nx; idx_n += BF16_BLOCK_STEP_N) {
                            SBGEMM_BLOCK_KERNEL_NT_32x8xK(m_step, k_step, alpha, block_A, block_B + (idx_n-n_from)*k_step_round32, &C(idx_n, tag_m_Nx), ldc);
                        }

                        if (tag_n_Nx != n_to) {
                            n_step = n_to - tag_n_Nx;
                            SBGEMM_BLOCK_KERNEL_NT_32xNxK(m_step, n_step, k_step, alpha, block_A, block_B + (tag_n_Nx-n_from)*k_step_round32, &C(tag_n_Nx, tag_m_Nx), ldc);
                        }
                    } else {
                        COL_MAJOR_INCOPY_KERNEL_Kx16(k_step, m_step, &A(idx_k, tag_m_Nx), lda, block_A);
                        for (BLASLONG idx_n = n_from; idx_n < tag_n_Nx; idx_n += BF16_BLOCK_STEP_N) {
                            SBGEMM_BLOCK_KERNEL_NT_16x8xK(m_step, k_step, alpha, block_A, block_B + (idx_n-n_from)*k_step_round32, &C(idx_n, tag_m_Nx), ldc);
                        }

                        if (tag_n_Nx != n_to) {
                            n_step = n_to - tag_n_Nx;
                            SBGEMM_BLOCK_KERNEL_NT_16xNxK(m_step, n_step, k_step, alpha, block_A, block_B + (tag_n_Nx-n_from)*k_step_round32, &C(tag_n_Nx, tag_m_Nx), ldc);
                        }
                    }
                }

                idx_k += k_step;
                k_step = K - idx_k;
                k_step = (k_step > BF16_BLOCK_THRES_K) ? BF16_BLOCK_THRES_K : k_step;
                k_step_round32 = k_step & (~31);
                k_step_round32 = (k_step > k_step_round32) ? (k_step_round32 + 32) : k_step_round32;
            }

            n_from = n_to;
            n_to += BF16_BLOCK_THRES_N;
            n_to = (n_to > N) ? N : n_to;
            tag_n_Nx = n_to & (~(BF16_BLOCK_STEP_N-1));
        }
    } else {
        m_step = M;
        if (m_step > 16) {
            while (n_from < N) {
                for (BLASLONG idx_k = 0; idx_k < K;) {
                    // Use Kx32 kernel when BF16_BLOCK_THRES_M==32, Kx16 kernel when BF16_BLOCK_THRES_M==16, ...
                    COL_MAJOR_INCOPY_KERNEL_Kx32(k_step, m_step, &A(idx_k, 0), lda, block_A);
                    for (BLASLONG idx_n = n_from; idx_n < tag_n_Nx; idx_n += BF16_BLOCK_STEP_N) {
                        // Use 8x32 kernel when BF16_BLOCK_THRES_N==8, 4x32 kernel when BF16_BLOCK_THRES_N==4, ...
                        COL_MAJOR_OTCOPY_KERNEL_Kx8(k_step, &B(idx_k, idx_n), ldb, block_B + (idx_n-n_from)*k_step_round32);
                        SBGEMM_BLOCK_KERNEL_NT_32x8xK(m_step, k_step, alpha, block_A, block_B + (idx_n-n_from)*k_step_round32, &C(idx_n, 0), ldc);
                    }

                    if (tag_n_Nx != n_to) {
                        n_step = n_to - tag_n_Nx;
                        COL_MAJOR_OTCOPY_KERNEL_Kx8m(k_step, n_step, &B(idx_k, tag_n_Nx), ldb, block_B + (tag_n_Nx-n_from)*k_step_round32);
                        SBGEMM_BLOCK_KERNEL_NT_32xNxK(m_step, n_step, k_step, alpha, block_A, block_B + (tag_n_Nx-n_from)*k_step_round32, &C(tag_n_Nx, 0), ldc);
                    }

                    idx_k += k_step;
                    k_step = K - idx_k;
                    k_step = (k_step > BF16_BLOCK_THRES_K) ? BF16_BLOCK_THRES_K : k_step;
                    k_step_round32 = k_step & (~31);
                    k_step_round32 = (k_step > k_step_round32) ? (k_step_round32 + 32) : k_step_round32;
                }
                n_from = n_to;
                n_to += BF16_BLOCK_THRES_N;
                n_to = (n_to > N) ? N : n_to;
                tag_n_Nx = n_to & (~(BF16_BLOCK_STEP_N-1));
            }
        } else {
            while (n_from < N) {
                for (BLASLONG idx_k = 0; idx_k < K;) {
                    // Use Kx32 kernel when BF16_BLOCK_THRES_M==32, Kx16 kernel when BF16_BLOCK_THRES_M==16, ...
                    COL_MAJOR_INCOPY_KERNEL_Kx16(k_step, m_step, &A(idx_k, 0), lda, block_A);
                    for (BLASLONG idx_n = n_from; idx_n < tag_n_Nx; idx_n += BF16_BLOCK_STEP_N) {
                        // Use 8x32 kernel when BF16_BLOCK_THRES_N==8, 4x32 kernel when BF16_BLOCK_THRES_N==4, ...
                        COL_MAJOR_OTCOPY_KERNEL_Kx8(k_step, &B(idx_k, idx_n), ldb, block_B + (idx_n-n_from)*k_step_round32);
                        SBGEMM_BLOCK_KERNEL_NT_16x8xK(m_step, k_step, alpha, block_A, block_B + (idx_n-n_from)*k_step_round32, &C(idx_n, 0), ldc);
                    }

                    if (tag_n_Nx != n_to) {
                        n_step = n_to - tag_n_Nx;
                        COL_MAJOR_OTCOPY_KERNEL_Kx8m(k_step, n_step, &B(idx_k, tag_n_Nx), ldb, block_B + (tag_n_Nx-n_from)*k_step_round32);
                        SBGEMM_BLOCK_KERNEL_NT_16xNxK(m_step, n_step, k_step, alpha, block_A, block_B + (tag_n_Nx-n_from)*k_step_round32, &C(tag_n_Nx, 0), ldc);
                    }

                    idx_k += k_step;
                    k_step = K - idx_k;
                    k_step = (k_step > BF16_BLOCK_THRES_K) ? BF16_BLOCK_THRES_K : k_step;
                    k_step_round32 = k_step & (~31);
                    k_step_round32 = (k_step > k_step_round32) ? (k_step_round32 + 32) : k_step_round32;
                }
                n_from = n_to;
                n_to += BF16_BLOCK_THRES_N;
                n_to = (n_to > N) ? N : n_to;
                tag_n_Nx = n_to & (~(BF16_BLOCK_STEP_N-1));
            }
        }
    }
}
/* ----------------------------------------- End of NT kernels --------------------------------------- */

/* --------------------------------------------- TN kernels ------------------------------------------ */
// SBGEMM Kernel for 16<M<=32, N=8, K=Any number
#ifndef ONE_ALPHA      // ALPHA is not ONE
void sbgemm_block_kernel_tn_32x8xK_alpha(BLASLONG m, BLASLONG k, float alpha, bfloat16 *A, bfloat16 *B, float *C, int ldc)
#else                  // ALPHA is ONE
void sbgemm_block_kernel_tn_32x8xK_one(BLASLONG m, BLASLONG k, float alpha, bfloat16 *A, bfloat16 *B, float *C, int ldc)
#endif
{
    bfloat16 * A_addr = A;
    bfloat16 * B_addr = B;
    float    * C_addr = C;

#ifndef ONE_ALPHA
    __m512  ALPHAVECTOR = _mm512_set1_ps(alpha);
#endif

    __m512i arrayA_512_0, arrayA_512_1;
    __m512i arrayB_512_0, arrayB_512_1, arrayB_512_2, arrayB_512_3, arrayB_512_4, arrayB_512_5, arrayB_512_6, arrayB_512_7;
    __m512  result_512_0, result_512_1, result_512_2, result_512_3, result_512_4, result_512_5, result_512_6, result_512_7,
            result_512_8, result_512_9, result_512_10, result_512_11, result_512_12, result_512_13, result_512_14, result_512_15;

    result_512_0  = _mm512_setzero_ps();
    result_512_1  = _mm512_setzero_ps();
    result_512_2  = _mm512_setzero_ps();
    result_512_3  = _mm512_setzero_ps();
    result_512_4  = _mm512_setzero_ps();
    result_512_5  = _mm512_setzero_ps();
    result_512_6  = _mm512_setzero_ps();
    result_512_7  = _mm512_setzero_ps();
    result_512_8  = _mm512_setzero_ps();
    result_512_9  = _mm512_setzero_ps();
    result_512_10 = _mm512_setzero_ps();
    result_512_11 = _mm512_setzero_ps();
    result_512_12 = _mm512_setzero_ps();
    result_512_13 = _mm512_setzero_ps();
    result_512_14 = _mm512_setzero_ps();
    result_512_15 = _mm512_setzero_ps();

    for (BLASLONG idx_k = 0; idx_k < k; idx_k += 2) {
        // Load 32 pair of BF16 elements from A (32 rows)
        arrayA_512_0 = _mm512_loadu_si512(A_addr);
        arrayA_512_1 = _mm512_loadu_si512(A_addr + 32);

        // Load 8 rows of B
        _MM512_BROADCASTD_EPI32(B_addr + 0,  arrayB_512_0);
        _MM512_BROADCASTD_EPI32(B_addr + 2,  arrayB_512_1);
        _MM512_BROADCASTD_EPI32(B_addr + 4,  arrayB_512_2);
        _MM512_BROADCASTD_EPI32(B_addr + 6,  arrayB_512_3);
        _MM512_BROADCASTD_EPI32(B_addr + 8,  arrayB_512_4);
        _MM512_BROADCASTD_EPI32(B_addr + 10, arrayB_512_5);
        _MM512_BROADCASTD_EPI32(B_addr + 12, arrayB_512_6);
        _MM512_BROADCASTD_EPI32(B_addr + 14, arrayB_512_7);

        result_512_0  = _mm512_dpbf16_ps(result_512_0, (__m512bh) arrayA_512_0, (__m512bh) arrayB_512_0);
        result_512_1  = _mm512_dpbf16_ps(result_512_1, (__m512bh) arrayA_512_0, (__m512bh) arrayB_512_1);
        result_512_2  = _mm512_dpbf16_ps(result_512_2, (__m512bh) arrayA_512_0, (__m512bh) arrayB_512_2);
        result_512_3  = _mm512_dpbf16_ps(result_512_3, (__m512bh) arrayA_512_0, (__m512bh) arrayB_512_3);
        result_512_4  = _mm512_dpbf16_ps(result_512_4, (__m512bh) arrayA_512_0, (__m512bh) arrayB_512_4);
        result_512_5  = _mm512_dpbf16_ps(result_512_5, (__m512bh) arrayA_512_0, (__m512bh) arrayB_512_5);
        result_512_6  = _mm512_dpbf16_ps(result_512_6, (__m512bh) arrayA_512_0, (__m512bh) arrayB_512_6);
        result_512_7  = _mm512_dpbf16_ps(result_512_7, (__m512bh) arrayA_512_0, (__m512bh) arrayB_512_7);

        result_512_8  = _mm512_dpbf16_ps(result_512_8,  (__m512bh) arrayA_512_1, (__m512bh) arrayB_512_0);
        result_512_9  = _mm512_dpbf16_ps(result_512_9,  (__m512bh) arrayA_512_1, (__m512bh) arrayB_512_1);
        result_512_10 = _mm512_dpbf16_ps(result_512_10, (__m512bh) arrayA_512_1, (__m512bh) arrayB_512_2);
        result_512_11 = _mm512_dpbf16_ps(result_512_11, (__m512bh) arrayA_512_1, (__m512bh) arrayB_512_3);
        result_512_12 = _mm512_dpbf16_ps(result_512_12, (__m512bh) arrayA_512_1, (__m512bh) arrayB_512_4);
        result_512_13 = _mm512_dpbf16_ps(result_512_13, (__m512bh) arrayA_512_1, (__m512bh) arrayB_512_5);
        result_512_14 = _mm512_dpbf16_ps(result_512_14, (__m512bh) arrayA_512_1, (__m512bh) arrayB_512_6);
        result_512_15 = _mm512_dpbf16_ps(result_512_15, (__m512bh) arrayA_512_1, (__m512bh) arrayB_512_7);

        // Load B with unroll 8
        B_addr += 16;
        // Load A with unroll 64
        A_addr += 64;
    }

    if (m != 32) {
        unsigned short tail_mask_value = (((unsigned short)0xffff) >> (32-m));
        __mmask16 tail_mask = *((__mmask16*) &tail_mask_value);
        STORE16_COMPLETE_RESULT(result_512_0,  (C_addr))
        STORE16_MASK_COMPLETE_RESULT(result_512_8,  (C_addr + 16), tail_mask)
        STORE16_COMPLETE_RESULT(result_512_1,  (C_addr + ldc))
        STORE16_MASK_COMPLETE_RESULT(result_512_9,  (C_addr + ldc + 16), tail_mask)
        STORE16_COMPLETE_RESULT(result_512_2,  (C_addr + ldc*2))
        STORE16_MASK_COMPLETE_RESULT(result_512_10, (C_addr + ldc*2 + 16), tail_mask)
        STORE16_COMPLETE_RESULT(result_512_3,  (C_addr + ldc*3))
        STORE16_MASK_COMPLETE_RESULT(result_512_11, (C_addr + ldc*3 + 16), tail_mask)
        STORE16_COMPLETE_RESULT(result_512_4,  (C_addr + ldc*4))
        STORE16_MASK_COMPLETE_RESULT(result_512_12, (C_addr + ldc*4 + 16), tail_mask)
        STORE16_COMPLETE_RESULT(result_512_5,  (C_addr + ldc*5))
        STORE16_MASK_COMPLETE_RESULT(result_512_13, (C_addr + ldc*5 + 16), tail_mask)
        STORE16_COMPLETE_RESULT(result_512_6,  (C_addr + ldc*6))
        STORE16_MASK_COMPLETE_RESULT(result_512_14, (C_addr + ldc*6 + 16), tail_mask)
        STORE16_COMPLETE_RESULT(result_512_7,  (C_addr + ldc*7))
        STORE16_MASK_COMPLETE_RESULT(result_512_15, (C_addr + ldc*7 + 16), tail_mask)
    } else {
        STORE16_COMPLETE_RESULT(result_512_0,  (C_addr))
        STORE16_COMPLETE_RESULT(result_512_8,  (C_addr + 16))
        STORE16_COMPLETE_RESULT(result_512_1,  (C_addr + ldc))
        STORE16_COMPLETE_RESULT(result_512_9,  (C_addr + ldc + 16))
        STORE16_COMPLETE_RESULT(result_512_2,  (C_addr + ldc*2))
        STORE16_COMPLETE_RESULT(result_512_10, (C_addr + ldc*2 + 16))
        STORE16_COMPLETE_RESULT(result_512_3,  (C_addr + ldc*3))
        STORE16_COMPLETE_RESULT(result_512_11, (C_addr + ldc*3 + 16))
        STORE16_COMPLETE_RESULT(result_512_4,  (C_addr + ldc*4))
        STORE16_COMPLETE_RESULT(result_512_12, (C_addr + ldc*4 + 16))
        STORE16_COMPLETE_RESULT(result_512_5,  (C_addr + ldc*5))
        STORE16_COMPLETE_RESULT(result_512_13, (C_addr + ldc*5 + 16))
        STORE16_COMPLETE_RESULT(result_512_6,  (C_addr + ldc*6))
        STORE16_COMPLETE_RESULT(result_512_14, (C_addr + ldc*6 + 16))
        STORE16_COMPLETE_RESULT(result_512_7,  (C_addr + ldc*7))
        STORE16_COMPLETE_RESULT(result_512_15, (C_addr + ldc*7 + 16))
    }
}

// SBGEMM Kernel for M=16, N=8, K=Any number
#ifndef ONE_ALPHA      // ALPHA is not ONE
void sbgemm_block_kernel_tn_16x8xK_alpha(BLASLONG m, BLASLONG k, float alpha, bfloat16 *A, bfloat16 *B, float *C, int ldc)
#else                  // ALPHA is ONE
void sbgemm_block_kernel_tn_16x8xK_one(BLASLONG m, BLASLONG k, float alpha, bfloat16 *A, bfloat16 *B, float *C, int ldc)
#endif
{
    bfloat16 * A_addr = A;
    bfloat16 * B_addr = B;
    float    * C_addr = C;

#ifndef ONE_ALPHA
    __m512  ALPHAVECTOR = _mm512_set1_ps(alpha);
#endif

    __m512i arrayA_512_0;
    __m512i arrayB_512_0, arrayB_512_1, arrayB_512_2, arrayB_512_3, arrayB_512_4, arrayB_512_5, arrayB_512_6, arrayB_512_7;
    __m512  result_512_0, result_512_1, result_512_2, result_512_3, result_512_4, result_512_5, result_512_6, result_512_7;

    result_512_0  = _mm512_setzero_ps();
    result_512_1  = _mm512_setzero_ps();
    result_512_2  = _mm512_setzero_ps();
    result_512_3  = _mm512_setzero_ps();
    result_512_4  = _mm512_setzero_ps();
    result_512_5  = _mm512_setzero_ps();
    result_512_6  = _mm512_setzero_ps();
    result_512_7  = _mm512_setzero_ps();

    for (BLASLONG idx_k = 0; idx_k < k; idx_k += 2) {
        // Load 16 pair of BF16 elements from A (16 rows)
        arrayA_512_0 = _mm512_loadu_si512(A_addr + 0);

        // Load 8 rows of B
        _MM512_BROADCASTD_EPI32(B_addr + 0,  arrayB_512_0);
        _MM512_BROADCASTD_EPI32(B_addr + 2,  arrayB_512_1);
        _MM512_BROADCASTD_EPI32(B_addr + 4,  arrayB_512_2);
        _MM512_BROADCASTD_EPI32(B_addr + 6,  arrayB_512_3);
        _MM512_BROADCASTD_EPI32(B_addr + 8,  arrayB_512_4);
        _MM512_BROADCASTD_EPI32(B_addr + 10, arrayB_512_5);
        _MM512_BROADCASTD_EPI32(B_addr + 12, arrayB_512_6);
        _MM512_BROADCASTD_EPI32(B_addr + 14, arrayB_512_7);

        result_512_0  = _mm512_dpbf16_ps(result_512_0, (__m512bh) arrayA_512_0, (__m512bh) arrayB_512_0);
        result_512_1  = _mm512_dpbf16_ps(result_512_1, (__m512bh) arrayA_512_0, (__m512bh) arrayB_512_1);
        result_512_2  = _mm512_dpbf16_ps(result_512_2, (__m512bh) arrayA_512_0, (__m512bh) arrayB_512_2);
        result_512_3  = _mm512_dpbf16_ps(result_512_3, (__m512bh) arrayA_512_0, (__m512bh) arrayB_512_3);
        result_512_4  = _mm512_dpbf16_ps(result_512_4, (__m512bh) arrayA_512_0, (__m512bh) arrayB_512_4);
        result_512_5  = _mm512_dpbf16_ps(result_512_5, (__m512bh) arrayA_512_0, (__m512bh) arrayB_512_5);
        result_512_6  = _mm512_dpbf16_ps(result_512_6, (__m512bh) arrayA_512_0, (__m512bh) arrayB_512_6);
        result_512_7  = _mm512_dpbf16_ps(result_512_7, (__m512bh) arrayA_512_0, (__m512bh) arrayB_512_7);

        // Load B with unroll 8
        B_addr += 16;
        // Load A with unroll 32
        A_addr += 32;
    }

    if (m != 16) {
        unsigned short tail_mask_value = (((unsigned short)0xffff) >> (16-m));
        __mmask16 tail_mask = *((__mmask16*) &tail_mask_value);
        STORE16_MASK_COMPLETE_RESULT(result_512_0,  (C_addr), tail_mask)
        STORE16_MASK_COMPLETE_RESULT(result_512_1,  (C_addr + ldc), tail_mask)
        STORE16_MASK_COMPLETE_RESULT(result_512_2,  (C_addr + ldc*2), tail_mask)
        STORE16_MASK_COMPLETE_RESULT(result_512_3,  (C_addr + ldc*3), tail_mask)
        STORE16_MASK_COMPLETE_RESULT(result_512_4,  (C_addr + ldc*4), tail_mask)
        STORE16_MASK_COMPLETE_RESULT(result_512_5,  (C_addr + ldc*5), tail_mask)
        STORE16_MASK_COMPLETE_RESULT(result_512_6,  (C_addr + ldc*6), tail_mask)
        STORE16_MASK_COMPLETE_RESULT(result_512_7,  (C_addr + ldc*7), tail_mask)
    } else {
        STORE16_COMPLETE_RESULT(result_512_0,  (C_addr))
        STORE16_COMPLETE_RESULT(result_512_1,  (C_addr + ldc))
        STORE16_COMPLETE_RESULT(result_512_2,  (C_addr + ldc*2))
        STORE16_COMPLETE_RESULT(result_512_3,  (C_addr + ldc*3))
        STORE16_COMPLETE_RESULT(result_512_4,  (C_addr + ldc*4))
        STORE16_COMPLETE_RESULT(result_512_5,  (C_addr + ldc*5))
        STORE16_COMPLETE_RESULT(result_512_6,  (C_addr + ldc*6))
        STORE16_COMPLETE_RESULT(result_512_7,  (C_addr + ldc*7))
    }
}

// SBGEMM Kernel for 16<M<=32, N<8, K=Any number but will be processed based on 32
#ifndef ONE_ALPHA      // ALPHA is not ONE
void sbgemm_block_kernel_tn_32xNx32_alpha(BLASLONG m, BLASLONG n, BLASLONG k, float alpha, bfloat16 *A, bfloat16 *B, float *C, int ldc)
#else                  // ALPHA is ONE
void sbgemm_block_kernel_tn_32xNx32_one(BLASLONG m, BLASLONG n, BLASLONG k, float alpha, bfloat16 *A, bfloat16 *B, float *C, int ldc)
#endif
{
    bfloat16 * A_addr = A;
    bfloat16 * B_addr = B;
    float    * C_addr = C;

    BLASLONG tag_k_32x = k & (~31);

#ifndef ONE_ALPHA
    __m512  ALPHAVECTOR = _mm512_set1_ps(alpha);
#endif

    __m512i arrayA_512[2];
    __m512i arrayB_512[8];
    __m512  result_512[16];

    for (int i = 0; i < 15; i++) {
        result_512[i] = _mm512_setzero_ps();
    }

    for (BLASLONG idx_k = 0; idx_k < tag_k_32x; idx_k += 32) {
        // Load B with unroll n
        for (int i = 0; i < n; i ++) {
            arrayB_512[i] = _mm512_loadu_si512(B_addr);
            B_addr += 32;
        }

        for (BLASLONG idx = 0; idx < 32;) {
            // Each two rows are a group for 32-pair bf16 elements
            arrayA_512[0] = _mm512_loadu_si512(A_addr);
            arrayA_512[1] = _mm512_loadu_si512(A_addr + 32);
            A_addr += 64;

            for (int i = 0; i < n; i++) {
                result_512[i]   = _mm512_dpbf16_ps(result_512[i]  , (__m512bh) arrayA_512[0], (__m512bh) _mm512_broadcastd_epi32(_mm512_castsi512_si128(arrayB_512[i])));
                result_512[i+8] = _mm512_dpbf16_ps(result_512[i+8], (__m512bh) arrayA_512[1], (__m512bh) _mm512_broadcastd_epi32(_mm512_castsi512_si128(arrayB_512[i])));
                arrayB_512[i]   = _mm512_shuffle_epi32(arrayB_512[i], SHUFFLE_MAGIC_NO);
            }

            idx += 2;
            // Every 4 loops we need to switch to next 128 bits of arrayB registers
            if ((idx & (~7)) == idx) {
                for (int i = 0; i < n; i++) {
                    arrayB_512[i] = _mm512_shuffle_i32x4(arrayB_512[i], arrayB_512[i], SHUFFLE_MAGIC_NO);
                }
            }
        }
    }

    if (tag_k_32x != k) {
            // Load B with unroll n
        for (int i = 0; i < n; i ++) {
            arrayB_512[i] = _mm512_loadu_si512(B_addr);
            B_addr += 32;
        }

        BLASLONG width = k - tag_k_32x;
        for (BLASLONG idx = 0; idx < width;) {
            // Each two rows are a group for 32-pair bf16 elements
            arrayA_512[0] = _mm512_loadu_si512(A_addr);
            arrayA_512[1] = _mm512_loadu_si512(A_addr + 32);
            A_addr += 64;

            for (int i = 0; i < n; i++) {
                result_512[i]   = _mm512_dpbf16_ps(result_512[i]  , (__m512bh) arrayA_512[0], (__m512bh) _mm512_broadcastd_epi32(_mm512_castsi512_si128(arrayB_512[i])));
                result_512[i+8] = _mm512_dpbf16_ps(result_512[i+8], (__m512bh) arrayA_512[1], (__m512bh) _mm512_broadcastd_epi32(_mm512_castsi512_si128(arrayB_512[i])));
                arrayB_512[i]   = _mm512_shuffle_epi32(arrayB_512[i], SHUFFLE_MAGIC_NO);
            }

            idx += 2;
            // Every 4 loops we need to switch to next 128 bits of arrayB registers
            if ((idx & (~7)) == idx) {
                for (int i = 0; i < n; i++) {
                    arrayB_512[i] = _mm512_shuffle_i32x4(arrayB_512[i], arrayB_512[i], SHUFFLE_MAGIC_NO);
                }
            }
        }
    }

    if (m != 32) {
        unsigned short tail_mask = (((unsigned short)0xffff) >> (32-m));
        for (int i = 0; i < n; i++) {
            STORE16_COMPLETE_RESULT(result_512[i], (C_addr + ldc*i))
            STORE16_MASK_COMPLETE_RESULT(result_512[i+8], (C_addr + ldc*i + 16), tail_mask)
        }
    } else {
        for (int i = 0; i < n; i++) {
            STORE16_COMPLETE_RESULT(result_512[i], (C_addr + ldc*i))
            STORE16_COMPLETE_RESULT(result_512[i+8], (C_addr + ldc*i + 16))
        }
    }
}

// SBGEMM Kernel for M<=16, N<8, K=Any number but will be processed based on 32
#ifndef ONE_ALPHA      // ALPHA is not ONE
void sbgemm_block_kernel_tn_16xNx32_alpha(BLASLONG m, BLASLONG n, BLASLONG k, float alpha, bfloat16 *A, bfloat16 *B, float *C, int ldc)
#else                  // ALPHA is ONE
void sbgemm_block_kernel_tn_16xNx32_one(BLASLONG m, BLASLONG n, BLASLONG k, float alpha, bfloat16 *A, bfloat16 *B, float *C, int ldc)
#endif
{
    bfloat16 * A_addr = A;
    bfloat16 * B_addr = B;
    float    * C_addr = C;

    BLASLONG tag_k_32x = k & (~31);

#ifndef ONE_ALPHA
    __m512  ALPHAVECTOR = _mm512_set1_ps(alpha);
#endif

    __m512i arrayA_512;
    __m512i arrayB_512[8];
    __m512  result_512[8];

    for (int i = 0; i < 8; i++) {
        result_512[i]    = _mm512_setzero_ps();
    }

    for (BLASLONG idx_k = 0; idx_k < tag_k_32x; idx_k += 32) {
        // Load B with unroll n
        for (int i = 0; i < n; i ++) {
            arrayB_512[i] = _mm512_loadu_si512(B_addr);
            B_addr += 32;
        }

        for (BLASLONG idx = 0; idx < 32;) {
            // Each two rows are a group for 32-pair bf16 elements
            arrayA_512 = _mm512_loadu_si512(A_addr);
            A_addr += 32;

            for (int i = 0; i < n; i++) {
                result_512[i]   = _mm512_dpbf16_ps(result_512[i], (__m512bh) arrayA_512, (__m512bh) _mm512_broadcastd_epi32(_mm512_castsi512_si128(arrayB_512[i])));
                arrayB_512[i]   = _mm512_shuffle_epi32(arrayB_512[i], SHUFFLE_MAGIC_NO);
            }

            idx += 2;
            // Every 4 loops we need to switch to next 128 bits of arrayB registers
            if ((idx & (~7)) == idx) {
                for (int i = 0; i < n; i++) {
                    arrayB_512[i] = _mm512_shuffle_i32x4(arrayB_512[i], arrayB_512[i], SHUFFLE_MAGIC_NO);
                }
            }
        }
    }

    if (tag_k_32x != k) {
        // Load B with unroll n
        for (int i = 0; i < n; i ++) {
            arrayB_512[i] = _mm512_loadu_si512(B_addr);
            B_addr += 32;
        }

        BLASLONG width = k - tag_k_32x;
        for (BLASLONG idx = 0; idx < width;) {
            // Each two rows are a group for 32-pair bf16 elements
            arrayA_512 = _mm512_loadu_si512(A_addr);
            A_addr += 32;

            for (int i = 0; i < n; i++) {
                result_512[i]   = _mm512_dpbf16_ps(result_512[i], (__m512bh) arrayA_512, (__m512bh) _mm512_broadcastd_epi32(_mm512_castsi512_si128(arrayB_512[i])));
                arrayB_512[i]   = _mm512_shuffle_epi32(arrayB_512[i], SHUFFLE_MAGIC_NO);
            }

            idx += 2;
            // Every 4 loops we need to switch to next 128 bits of arrayB registers
            if ((idx & (~7)) == idx) {
                for (int i = 0; i < n; i++) {
                    arrayB_512[i] = _mm512_shuffle_i32x4(arrayB_512[i], arrayB_512[i], SHUFFLE_MAGIC_NO);
                }
            }
        }
    }

    if (m != 16) {
        unsigned short tail_mask = (((unsigned short)0xffff) >> (16-m));
        for (int i = 0; i < n; i++) {
            STORE16_MASK_COMPLETE_RESULT(result_512[i], (C_addr + ldc*i), tail_mask)
        }
    } else {
        for (int i = 0; i < n; i++) {
            STORE16_COMPLETE_RESULT(result_512[i], (C_addr + ldc*i))
        }
    }
}

#ifndef ONE_ALPHA      // ALPHA is not ONE
void sbgemm_blocking_kernel_tn_alpha(blasint M, blasint N, blasint K, float alpha, bfloat16 *A, blasint lda, bfloat16 *B, blasint ldb, float *C, blasint ldc, bfloat16 * block_A, bfloat16 * block_B)
#else                  // ALPHA is ONE
void sbgemm_blocking_kernel_tn_one(blasint M, blasint N, blasint K, float alpha, bfloat16 *A, blasint lda, bfloat16 *B, blasint ldb, float *C, blasint ldc, bfloat16 * block_A, bfloat16 * block_B)
#endif
{
    BLASLONG m_step, n_step, k_step, k_step_round32;
    BLASLONG tag_m_Nx = M & (~(BF16_BLOCK_THRES_M-1));

    BLASLONG n_from, n_to;
    BLASLONG tag_n_Nx;

    n_from = 0;
    n_to = (BF16_BLOCK_THRES_N > N) ? N : BF16_BLOCK_THRES_N;
    tag_n_Nx = n_to & (~(BF16_BLOCK_STEP_N-1));

    k_step = (K > BF16_BLOCK_THRES_K) ? BF16_BLOCK_THRES_K : K;
    k_step_round32 = k_step & (~31);
    k_step_round32 = (k_step > k_step_round32) ? (k_step_round32 + 32) : k_step_round32;

    if (M >= BF16_BLOCK_THRES_M) {
        while (n_from < N) {
            for (BLASLONG idx_k = 0; idx_k < K;) {
                // Use Kx32 kernel when BF16_BLOCK_THRES_M==32, Kx16 kernel when BF16_BLOCK_THRES_M==16, ...
                COL_MAJOR_ITCOPY_KERNEL_Kx32(k_step, &A(0, idx_k), lda, block_A);
                for (BLASLONG idx_n = n_from; idx_n < tag_n_Nx; idx_n += BF16_BLOCK_STEP_N) {
                    // Use 8x32 kernel when BF16_BLOCK_THRES_N==8, 4x32 kernel when BF16_BLOCK_THRES_N==4, ...
                    COL_MAJOR_ONCOPY_KERNEL_8x32(k_step, &B(idx_n, idx_k), ldb, block_B + (idx_n-n_from)*k_step_round32);
                    SBGEMM_BLOCK_KERNEL_TN_32x8xK(32, k_step, alpha, block_A, block_B + (idx_n-n_from)*k_step_round32, &C(idx_n, 0), ldc); // TODO how to process m
                }

                if (tag_n_Nx != n_to) {
                    n_step = n_to - tag_n_Nx;
                    COL_MAJOR_ONCOPY_KERNEL_Nx32(n_step, k_step, &B(tag_n_Nx, idx_k), ldb, block_B + (tag_n_Nx-n_from)*k_step_round32);
                    SBGEMM_BLOCK_KERNEL_TN_32xNx32(32, n_step, k_step, alpha, block_A, block_B + (tag_n_Nx-n_from)*k_step_round32, &C(tag_n_Nx, 0), ldc);
                }

                for (BLASLONG idx_m = BF16_BLOCK_THRES_M; idx_m < tag_m_Nx; idx_m += BF16_BLOCK_THRES_M) {
                    COL_MAJOR_ITCOPY_KERNEL_Kx32(k_step, &A(idx_m, idx_k), lda, block_A);
                    for (BLASLONG idx_n = n_from; idx_n < tag_n_Nx; idx_n += BF16_BLOCK_STEP_N) {
                        SBGEMM_BLOCK_KERNEL_TN_32x8xK(32, k_step, alpha, block_A, block_B + (idx_n-n_from)*k_step_round32, &C(idx_n, idx_m), ldc);
                    }

                    if (tag_n_Nx != n_to) {
                        n_step = n_to - tag_n_Nx;
                        SBGEMM_BLOCK_KERNEL_TN_32xNx32(32, n_step, k_step, alpha, block_A, block_B + (tag_n_Nx-n_from)*k_step_round32, &C(tag_n_Nx, idx_m), ldc);
                    }
                }

                if (tag_m_Nx != M) {
                    m_step = M - tag_m_Nx;
                    if (m_step > 16) {
                        COL_MAJOR_ITCOPY_KERNEL_Kx32m(m_step, k_step, &A(tag_m_Nx, idx_k), lda, block_A);
                        for (BLASLONG idx_n = n_from; idx_n < tag_n_Nx; idx_n += BF16_BLOCK_STEP_N) {
                            SBGEMM_BLOCK_KERNEL_TN_32x8xK(m_step, k_step, alpha, block_A, block_B + (idx_n-n_from)*k_step_round32, &C(idx_n, tag_m_Nx), ldc);
                        }

                        if (tag_n_Nx != n_to) {
                            n_step = n_to - tag_n_Nx;
                            SBGEMM_BLOCK_KERNEL_TN_32xNx32(m_step, n_step, k_step, alpha, block_A, block_B + (tag_n_Nx-n_from)*k_step_round32, &C(tag_n_Nx, tag_m_Nx), ldc);
                        }
                    } else {
                        COL_MAJOR_ITCOPY_KERNEL_Kx16m(m_step, k_step, &A(tag_m_Nx, idx_k), lda, block_A);
                        for (BLASLONG idx_n = n_from; idx_n < tag_n_Nx; idx_n += BF16_BLOCK_STEP_N) {
                            SBGEMM_BLOCK_KERNEL_TN_16x8xK(m_step, k_step, alpha, block_A, block_B + (idx_n-n_from)*k_step_round32, &C(idx_n, tag_m_Nx), ldc);
                        }

                        if (tag_n_Nx != n_to) {
                            n_step = n_to - tag_n_Nx;
                            SBGEMM_BLOCK_KERNEL_TN_16xNx32(m_step, n_step, k_step, alpha, block_A, block_B + (tag_n_Nx-n_from)*k_step_round32, &C(tag_n_Nx, tag_m_Nx), ldc);
                        }
                    }
                }

                idx_k += k_step;
                k_step = K - idx_k;
                k_step = (k_step > BF16_BLOCK_THRES_K) ? BF16_BLOCK_THRES_K : k_step;
                k_step_round32 = k_step & (~31);
                k_step_round32 = (k_step > k_step_round32) ? (k_step_round32 + 32) : k_step_round32;
            }

            n_from = n_to;
            n_to += BF16_BLOCK_THRES_N;
            n_to = (n_to > N) ? N : n_to;
            tag_n_Nx = n_to & (~(BF16_BLOCK_STEP_N-1));
        }
    } else {
        m_step = M;
        if (m_step > 16) {
            while (n_from < N) {
                for (BLASLONG idx_k = 0; idx_k < K;) {
                    // Use Kx32 kernel when BF16_BLOCK_THRES_M==32, Kx16 kernel when BF16_BLOCK_THRES_M==16, ...
                    COL_MAJOR_ITCOPY_KERNEL_Kx32m(m_step, k_step, &A(0, idx_k), lda, block_A);
                    for (BLASLONG idx_n = n_from; idx_n < tag_n_Nx; idx_n += BF16_BLOCK_STEP_N) {
                        // Use 8x32 kernel when BF16_BLOCK_THRES_N==8, 4x32 kernel when BF16_BLOCK_THRES_N==4, ...
                        COL_MAJOR_ONCOPY_KERNEL_8x32(k_step, &B(idx_n, idx_k), ldb, block_B + (idx_n-n_from)*k_step_round32);
                        SBGEMM_BLOCK_KERNEL_TN_32x8xK(m_step, k_step, alpha, block_A, block_B + (idx_n-n_from)*k_step_round32, &C(idx_n, 0), ldc);
                    }

                    if (tag_n_Nx != n_to) {
                        n_step = n_to - tag_n_Nx;
                        COL_MAJOR_ONCOPY_KERNEL_Nx32(n_step, k_step, &B(tag_n_Nx, idx_k), ldb, block_B + (tag_n_Nx-n_from)*k_step_round32);
                        SBGEMM_BLOCK_KERNEL_TN_32xNx32(m_step, n_step, k_step, alpha, block_A, block_B + (tag_n_Nx-n_from)*k_step_round32, &C(tag_n_Nx, 0), ldc);
                    }

                    idx_k += k_step;
                    k_step = K - idx_k;
                    k_step = (k_step > BF16_BLOCK_THRES_K) ? BF16_BLOCK_THRES_K : k_step;
                    k_step_round32 = k_step & (~31);
                    k_step_round32 = (k_step > k_step_round32) ? (k_step_round32 + 32) : k_step_round32;
                }
                n_from = n_to;
                n_to += BF16_BLOCK_THRES_N;
                n_to = (n_to > N) ? N : n_to;
                tag_n_Nx = n_to & (~(BF16_BLOCK_STEP_N-1));
            }
        } else {
            while (n_from < N) {
                for (BLASLONG idx_k = 0; idx_k < K;) {
                    // Use Kx32 kernel when BF16_BLOCK_THRES_M==32, Kx16 kernel when BF16_BLOCK_THRES_M==16, ...
                    COL_MAJOR_ITCOPY_KERNEL_Kx16m(m_step, k_step, &A(0, idx_k), lda, block_A);
                    for (BLASLONG idx_n = n_from; idx_n < tag_n_Nx; idx_n += BF16_BLOCK_STEP_N) {
                        // Use 8x32 kernel when BF16_BLOCK_THRES_N==8, 4x32 kernel when BF16_BLOCK_THRES_N==4, ...
                        COL_MAJOR_ONCOPY_KERNEL_8x32(k_step, &B(idx_n, idx_k), ldb, block_B + (idx_n-n_from)*k_step_round32);
                        SBGEMM_BLOCK_KERNEL_TN_16x8xK(m_step, k_step, alpha, block_A, block_B + (idx_n-n_from)*k_step_round32, &C(idx_n, 0), ldc);
                    }

                    if (tag_n_Nx != n_to) {
                        n_step = n_to - tag_n_Nx;
                        COL_MAJOR_ONCOPY_KERNEL_Nx32(n_step, k_step, &B(tag_n_Nx, idx_k), ldb, block_B + (tag_n_Nx-n_from)*k_step_round32);
                        SBGEMM_BLOCK_KERNEL_TN_16xNx32(m_step, n_step, k_step, alpha, block_A, block_B + (tag_n_Nx-n_from)*k_step_round32, &C(tag_n_Nx, 0), ldc);
                    }

                    idx_k += k_step;
                    k_step = K - idx_k;
                    k_step = (k_step > BF16_BLOCK_THRES_K) ? BF16_BLOCK_THRES_K : k_step;
                    k_step_round32 = k_step & (~31);
                    k_step_round32 = (k_step > k_step_round32) ? (k_step_round32 + 32) : k_step_round32;
                }
                n_from = n_to;
                n_to += BF16_BLOCK_THRES_N;
                n_to = (n_to > N) ? N : n_to;
                tag_n_Nx = n_to & (~(BF16_BLOCK_STEP_N-1));
            }
        }
    }
}
/* ----------------------------------------- End of TN kernels --------------------------------------- */

/* --------------------------------------------- TT kernels ------------------------------------------ */
// SBGEMM Kernel for 16<M<=32, N<8, K can be any number
#ifndef ONE_ALPHA      // ALPHA is not ONE
void sbgemm_block_kernel_tt_32xNxK_alpha(BLASLONG m, BLASLONG n, BLASLONG k, float alpha, bfloat16 *A, bfloat16 *B, float *C, int ldc)
#else                  // ALPHA is ONE
void sbgemm_block_kernel_tt_32xNxK_one(BLASLONG m, BLASLONG n, BLASLONG k, float alpha, bfloat16 *A, bfloat16 *B, float *C, int ldc)
#endif
{
    bfloat16 * A_addr = A;
    bfloat16 * B_addr = B;
    float    * C_addr = C;

#ifndef ONE_ALPHA
    __m512  ALPHAVECTOR = _mm512_set1_ps(alpha);
#endif

    __m512i arrayA_512_0, arrayA_512_1;
    __m512i arrayB_512[8];
    __m512  result_512[16];

    result_512[0]  = _mm512_setzero_ps();
    result_512[1]  = _mm512_setzero_ps();
    result_512[2]  = _mm512_setzero_ps();
    result_512[3]  = _mm512_setzero_ps();
    result_512[4]  = _mm512_setzero_ps();
    result_512[5]  = _mm512_setzero_ps();
    result_512[6]  = _mm512_setzero_ps();
    result_512[7]  = _mm512_setzero_ps();
    result_512[8]  = _mm512_setzero_ps();
    result_512[9]  = _mm512_setzero_ps();
    result_512[10] = _mm512_setzero_ps();
    result_512[11] = _mm512_setzero_ps();
    result_512[12] = _mm512_setzero_ps();
    result_512[13] = _mm512_setzero_ps();
    result_512[14] = _mm512_setzero_ps();
    result_512[15] = _mm512_setzero_ps();

    for (BLASLONG idx_k = 0; idx_k < k; idx_k += 2) {
        // Each two rows are a group for 32-pair bf16 elements
        arrayA_512_0 = _mm512_loadu_si512(A_addr);
        arrayA_512_1 = _mm512_loadu_si512(A_addr + 32);
        A_addr += 64;

        for (int i = 0; i < n; i ++) {
            _MM512_BROADCASTD_EPI32(B_addr + i*2,  arrayB_512[i]);
        }
        B_addr += 16;

        for (int i = 0; i < n; i ++) {
            result_512[i] = _mm512_dpbf16_ps(result_512[i],  (__m512bh) arrayA_512_0, (__m512bh) arrayB_512[i]);
            result_512[i+8] = _mm512_dpbf16_ps(result_512[i+8],  (__m512bh) arrayA_512_1, (__m512bh) arrayB_512[i]);
        }
    }

    if (m != 32) {
        unsigned short tail_mask = (((unsigned short)0xffff) >> (32-m));
        for (int i = 0; i < n; i ++) {
            STORE16_COMPLETE_RESULT(result_512[i], (C_addr + ldc*i))
            STORE16_MASK_COMPLETE_RESULT(result_512[i+8], (C_addr + ldc*i + 16), tail_mask)
        }
    } else {
        for (int i = 0; i < n; i ++) {
            STORE16_COMPLETE_RESULT(result_512[i], (C_addr + ldc*i))
            STORE16_COMPLETE_RESULT(result_512[i+8], (C_addr + ldc*i + 16))
        }
    }
}

// SBGEMM Kernel for M<=16, N<8, K can be any number
#ifndef ONE_ALPHA      // ALPHA is not ONE
void sbgemm_block_kernel_tt_16xNxK_alpha(BLASLONG m, BLASLONG n, BLASLONG k, float alpha, bfloat16 *A, bfloat16 *B, float *C, int ldc)
#else                  // ALPHA is ONE
void sbgemm_block_kernel_tt_16xNxK_one(BLASLONG m, BLASLONG n, BLASLONG k, float alpha, bfloat16 *A, bfloat16 *B, float *C, int ldc)
#endif
{
    bfloat16 * A_addr = A;
    bfloat16 * B_addr = B;
    float    * C_addr = C;

#ifndef ONE_ALPHA
    __m512  ALPHAVECTOR = _mm512_set1_ps(alpha);
#endif

    __m512i arrayA_512_0;
    __m512i arrayB_512[8];
    __m512  result_512[8];

    result_512[0]  = _mm512_setzero_ps();
    result_512[1]  = _mm512_setzero_ps();
    result_512[2]  = _mm512_setzero_ps();
    result_512[3]  = _mm512_setzero_ps();
    result_512[4]  = _mm512_setzero_ps();
    result_512[5]  = _mm512_setzero_ps();
    result_512[6]  = _mm512_setzero_ps();
    result_512[7]  = _mm512_setzero_ps();

    for (BLASLONG idx_k = 0; idx_k < k; idx_k += 2) {
        // Each two rows are a group for 16-pair bf16 elements
        // Load two rows into a 512 register
        arrayA_512_0 = _mm512_loadu_si512(A_addr);
        A_addr += 32;

        for (int i = 0; i < n; i ++) {
            _MM512_BROADCASTD_EPI32(B_addr + i*2,  arrayB_512[i]);
        }
        B_addr += 16;

        for (int i = 0; i < n; i ++) {
            result_512[i] = _mm512_dpbf16_ps(result_512[i],  (__m512bh) arrayA_512_0, (__m512bh) arrayB_512[i]);
        }
    }

    if (m != 16) {
        unsigned short tail_mask = (((unsigned short)0xffff) >> (16-m));
        for (int i = 0; i < n; i++) {
            STORE16_MASK_COMPLETE_RESULT(result_512[i], (C_addr + ldc*i), tail_mask)
        }
    } else {
        for (int i = 0; i < n; i++) {
            STORE16_COMPLETE_RESULT(result_512[i], (C_addr + ldc*i))
        }
    }
}

#ifndef ONE_ALPHA      // ALPHA is not ONE
void sbgemm_blocking_kernel_tt_alpha(blasint M, blasint N, blasint K, float alpha, bfloat16 *A, blasint lda, bfloat16 *B, blasint ldb, float *C, blasint ldc, bfloat16 * block_A, bfloat16 * block_B)
#else                  // ALPHA is ONE
void sbgemm_blocking_kernel_tt_one(blasint M, blasint N, blasint K, float alpha, bfloat16 *A, blasint lda, bfloat16 *B, blasint ldb, float *C, blasint ldc, bfloat16 * block_A, bfloat16 * block_B)
#endif
{
    BLASLONG m_step, n_step, k_step, k_step_round32;
    BLASLONG tag_m_Nx = M & (~(BF16_BLOCK_THRES_M-1));

    BLASLONG n_from, n_to;
    BLASLONG tag_n_Nx;

    n_from = 0;
    n_to = (BF16_BLOCK_THRES_N > N) ? N : BF16_BLOCK_THRES_N;
    tag_n_Nx = n_to & (~(BF16_BLOCK_STEP_N-1));

    k_step = (K > BF16_BLOCK_THRES_K) ? BF16_BLOCK_THRES_K : K;
    k_step_round32 = k_step & (~31);
    k_step_round32 = (k_step > k_step_round32) ? (k_step_round32 + 32) : k_step_round32;

    if (M >= BF16_BLOCK_THRES_M) {
        while (n_from < N) {
            for (BLASLONG idx_k = 0; idx_k < K;) {
                // Use Kx32 kernel when BF16_BLOCK_THRES_M==32, Kx16 kernel when BF16_BLOCK_THRES_M==16, ...
                COL_MAJOR_ITCOPY_KERNEL_Kx32(k_step, &A(0, idx_k), lda, block_A);
                for (BLASLONG idx_n = n_from; idx_n < tag_n_Nx; idx_n += BF16_BLOCK_STEP_N) {
                    // Use 8x32 kernel when BF16_BLOCK_THRES_N==8, 4x32 kernel when BF16_BLOCK_THRES_N==4, ...
                    COL_MAJOR_OTCOPY_KERNEL_Kx8(k_step, &B(idx_k, idx_n), ldb, block_B + (idx_n-n_from)*k_step_round32);
                    SBGEMM_BLOCK_KERNEL_TT_32x8xK(32, k_step, alpha, block_A, block_B + (idx_n-n_from)*k_step_round32, &C(idx_n, 0), ldc);
                }

                if (tag_n_Nx != n_to) {
                    n_step = n_to - tag_n_Nx;
                    COL_MAJOR_OTCOPY_KERNEL_Kx8m(k_step, n_step, &B(idx_k, tag_n_Nx), ldb, block_B + (tag_n_Nx-n_from)*k_step_round32);
                    SBGEMM_BLOCK_KERNEL_TT_32xNxK(32, n_step, k_step, alpha, block_A, block_B + (tag_n_Nx-n_from)*k_step_round32, &C(tag_n_Nx, 0), ldc);
                }

                for (BLASLONG idx_m = BF16_BLOCK_THRES_M; idx_m < tag_m_Nx; idx_m += BF16_BLOCK_THRES_M) {
                    COL_MAJOR_ITCOPY_KERNEL_Kx32(k_step, &A(idx_m, idx_k), lda, block_A);
                    for (BLASLONG idx_n = n_from; idx_n < tag_n_Nx; idx_n += BF16_BLOCK_STEP_N) {
                        SBGEMM_BLOCK_KERNEL_TT_32x8xK(32, k_step, alpha, block_A, block_B + (idx_n-n_from)*k_step_round32, &C(idx_n, idx_m), ldc);
                    }

                    if (tag_n_Nx != n_to) {
                        n_step = n_to - tag_n_Nx;
                        SBGEMM_BLOCK_KERNEL_TT_32xNxK(32, n_step, k_step, alpha, block_A, block_B + (tag_n_Nx-n_from)*k_step_round32, &C(tag_n_Nx, idx_m), ldc);
                    }
                }

                if (tag_m_Nx != M) {
                    m_step = M - tag_m_Nx;
                    if (m_step > 16) {
                        COL_MAJOR_ITCOPY_KERNEL_Kx32m(m_step, k_step, &A(tag_m_Nx, idx_k), lda, block_A);
                        for (BLASLONG idx_n = n_from; idx_n < tag_n_Nx; idx_n += BF16_BLOCK_STEP_N) {
                            SBGEMM_BLOCK_KERNEL_TT_32x8xK(m_step, k_step, alpha, block_A, block_B + (idx_n-n_from)*k_step_round32, &C(idx_n, tag_m_Nx), ldc);
                        }

                        if (tag_n_Nx != n_to) {
                            n_step = n_to - tag_n_Nx;
                            SBGEMM_BLOCK_KERNEL_TT_32xNxK(m_step, n_step, k_step, alpha, block_A, block_B + (tag_n_Nx-n_from)*k_step_round32, &C(tag_n_Nx, tag_m_Nx), ldc);
                        }
                    } else {
                        COL_MAJOR_ITCOPY_KERNEL_Kx16m(m_step, k_step, &A(tag_m_Nx, idx_k), lda, block_A);
                        for (BLASLONG idx_n = n_from; idx_n < tag_n_Nx; idx_n += BF16_BLOCK_STEP_N) {
                            SBGEMM_BLOCK_KERNEL_TT_16x8xK(m_step, k_step, alpha, block_A, block_B + (idx_n-n_from)*k_step_round32, &C(idx_n, tag_m_Nx), ldc);
                        }

                        if (tag_n_Nx != n_to) {
                            n_step = n_to - tag_n_Nx;
                            SBGEMM_BLOCK_KERNEL_TT_16xNxK(m_step, n_step, k_step, alpha, block_A, block_B + (tag_n_Nx-n_from)*k_step_round32, &C(tag_n_Nx, tag_m_Nx), ldc);
                        }
                    }
                }

                idx_k += k_step;
                k_step = K - idx_k;
                k_step = (k_step > BF16_BLOCK_THRES_K) ? BF16_BLOCK_THRES_K : k_step;
                k_step_round32 = k_step & (~31);
                k_step_round32 = (k_step > k_step_round32) ? (k_step_round32 + 32) : k_step_round32;
            }

            n_from = n_to;
            n_to += BF16_BLOCK_THRES_N;
            n_to = (n_to > N) ? N : n_to;
            tag_n_Nx = n_to & (~(BF16_BLOCK_STEP_N-1));
        }
    } else {
        m_step = M;
        if (m_step > 16) {
            while (n_from < N) {
                for (BLASLONG idx_k = 0; idx_k < K;) {
                    // Use Kx32 kernel when BF16_BLOCK_THRES_M==32, Kx16 kernel when BF16_BLOCK_THRES_M==16, ...
                    COL_MAJOR_ITCOPY_KERNEL_Kx32m(m_step, k_step, &A(0, idx_k), lda, block_A);
                    for (BLASLONG idx_n = n_from; idx_n < tag_n_Nx; idx_n += BF16_BLOCK_STEP_N) {
                        // Use 8x32 kernel when BF16_BLOCK_THRES_N==8, 4x32 kernel when BF16_BLOCK_THRES_N==4, ...
                        COL_MAJOR_OTCOPY_KERNEL_Kx8(k_step, &B(idx_k, idx_n), ldb, block_B + (idx_n-n_from)*k_step_round32);
                        SBGEMM_BLOCK_KERNEL_TT_32x8xK(m_step, k_step, alpha, block_A, block_B + (idx_n-n_from)*k_step_round32, &C(idx_n, 0), ldc);
                    }

                    if (tag_n_Nx != n_to) {
                        n_step = n_to - tag_n_Nx;
                        COL_MAJOR_OTCOPY_KERNEL_Kx8m(k_step, n_step, &B(idx_k, tag_n_Nx), ldb, block_B + (tag_n_Nx-n_from)*k_step_round32);
                        SBGEMM_BLOCK_KERNEL_TT_32xNxK(m_step, n_step, k_step, alpha, block_A, block_B + (tag_n_Nx-n_from)*k_step_round32, &C(tag_n_Nx, 0), ldc);
                    }

                    idx_k += k_step;
                    k_step = K - idx_k;
                    k_step = (k_step > BF16_BLOCK_THRES_K) ? BF16_BLOCK_THRES_K : k_step;
                    k_step_round32 = k_step & (~31);
                    k_step_round32 = (k_step > k_step_round32) ? (k_step_round32 + 32) : k_step_round32;
                }
                n_from = n_to;
                n_to += BF16_BLOCK_THRES_N;
                n_to = (n_to > N) ? N : n_to;
                tag_n_Nx = n_to & (~(BF16_BLOCK_STEP_N-1));
            }
        } else {
            while (n_from < N) {
                for (BLASLONG idx_k = 0; idx_k < K;) {
                    // Use Kx32 kernel when BF16_BLOCK_THRES_M==32, Kx16 kernel when BF16_BLOCK_THRES_M==16, ...
                    COL_MAJOR_ITCOPY_KERNEL_Kx16m(m_step, k_step, &A(0, idx_k), lda, block_A);
                    for (BLASLONG idx_n = n_from; idx_n < tag_n_Nx; idx_n += BF16_BLOCK_STEP_N) {
                        // Use 8x32 kernel when BF16_BLOCK_THRES_N==8, 4x32 kernel when BF16_BLOCK_THRES_N==4, ...
                        COL_MAJOR_OTCOPY_KERNEL_Kx8(k_step, &B(idx_k, idx_n), ldb, block_B + (idx_n-n_from)*k_step_round32);
                        SBGEMM_BLOCK_KERNEL_TT_16x8xK(m_step, k_step, alpha, block_A, block_B + (idx_n-n_from)*k_step_round32, &C(idx_n, 0), ldc);
                    }

                    if (tag_n_Nx != n_to) {
                        n_step = n_to - tag_n_Nx;
                        COL_MAJOR_OTCOPY_KERNEL_Kx8m(k_step, n_step, &B(idx_k, tag_n_Nx), ldb, block_B + (tag_n_Nx-n_from)*k_step_round32);
                        SBGEMM_BLOCK_KERNEL_TT_16xNxK(m_step, n_step, k_step, alpha, block_A, block_B + (tag_n_Nx-n_from)*k_step_round32, &C(tag_n_Nx, 0), ldc);
                    }

                    idx_k += k_step;
                    k_step = K - idx_k;
                    k_step = (k_step > BF16_BLOCK_THRES_K) ? BF16_BLOCK_THRES_K : k_step;
                    k_step_round32 = k_step & (~31);
                    k_step_round32 = (k_step > k_step_round32) ? (k_step_round32 + 32) : k_step_round32;
                }
                n_from = n_to;
                n_to += BF16_BLOCK_THRES_N;
                n_to = (n_to > N) ? N : n_to;
                tag_n_Nx = n_to & (~(BF16_BLOCK_STEP_N-1));
            }
        }
    }
}
/* ----------------------------------------- End of TT kernels --------------------------------------- */

/*
#ifndef ONE_ALPHA      // ALPHA is not ONE
void sbgemm_internal_kernel_alpha(OPENBLAS_CONST enum CBLAS_ORDER Order, OPENBLAS_CONST enum CBLAS_TRANSPOSE TransA, OPENBLAS_CONST enum CBLAS_TRANSPOSE TransB, OPENBLAS_CONST blasint M, OPENBLAS_CONST blasint N, OPENBLAS_CONST blasint K,
		 OPENBLAS_CONST float alpha, OPENBLAS_CONST bfloat16 *A, OPENBLAS_CONST blasint lda, OPENBLAS_CONST bfloat16 *B, OPENBLAS_CONST blasint ldb, float *C, OPENBLAS_CONST blasint ldc)
#else                  // ALPHA is ONE
void sbgemm_internal_kernel_one(OPENBLAS_CONST enum CBLAS_ORDER Order, OPENBLAS_CONST enum CBLAS_TRANSPOSE TransA, OPENBLAS_CONST enum CBLAS_TRANSPOSE TransB, OPENBLAS_CONST blasint M, OPENBLAS_CONST blasint N, OPENBLAS_CONST blasint K,
		 OPENBLAS_CONST float alpha, OPENBLAS_CONST bfloat16 *A, OPENBLAS_CONST blasint lda, OPENBLAS_CONST bfloat16 *B, OPENBLAS_CONST blasint ldb, float *C, OPENBLAS_CONST blasint ldc)
#endif
{
    if (Order == CblasColMajor) {
        if (TransA == CblasNoTrans) {
            if (TransB == CblasNoTrans) {
                SBGEMM_BLOCKING_KERNEL_NN(M, N, K, alpha, A, lda, B, ldb, C, ldc, block_A, block_B);
            } else if (TransB == CblasTrans) {
                SBGEMM_BLOCKING_KERNEL_NT(M, N, K, alpha, A, lda, B, ldb, C, ldc, block_A, block_B);
            }
        } else {
            if (TransB == CblasNoTrans) {
                SBGEMM_BLOCKING_KERNEL_TN(M, N, K, alpha, A, lda, B, ldb, C, ldc, block_A, block_B);
            } else if (TransB == CblasTrans) {
                SBGEMM_BLOCKING_KERNEL_TT(M, N, K, alpha, A, lda, B, ldb, C, ldc, block_A, block_B);
            }
        }
    } else {
        if (TransA == CblasNoTrans) {
            if (TransB == CblasNoTrans) {
                SBGEMM_BLOCKING_KERNEL_NN(N, M, K, alpha, B, ldb, A, lda, C, ldc, block_A, block_B);
            } else if (TransB == CblasTrans) {
                SBGEMM_BLOCKING_KERNEL_TN(N, M, K, alpha, B, ldb, A, lda, C, ldc, block_A, block_B);
            }
        } else {
            if (TransB == CblasNoTrans) {
                SBGEMM_BLOCKING_KERNEL_NT(N, M, K, alpha, B, ldb, A, lda, C, ldc, block_A, block_B);
            } else if (TransB == CblasTrans) {
                SBGEMM_BLOCKING_KERNEL_TT(N, M, K, alpha, B, ldb, A, lda, C, ldc, block_A, block_B);
            }
        }
    }
}
*/
