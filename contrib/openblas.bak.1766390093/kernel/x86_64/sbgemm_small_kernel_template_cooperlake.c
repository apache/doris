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

#include "common.h"
#include <memory.h>

extern void sbgemm_scal_operation(BLASLONG M, BLASLONG N, float beta, float *C, BLASLONG ldc);
extern void sbgemm_zero_operation(BLASLONG M, BLASLONG N, float *C, BLASLONG ldc);

extern void sbgemm_blocking_kernel_nn_alpha(blasint M, blasint N, blasint K, float alpha, bfloat16 *A, blasint lda, bfloat16 *B, blasint ldb, float *C, blasint ldc, bfloat16 * block_A, bfloat16 * block_B);
extern void sbgemm_blocking_kernel_nn_one(blasint M, blasint N, blasint K, float alpha, bfloat16 *A, blasint lda, bfloat16 *B, blasint ldb, float *C, blasint ldc, bfloat16 * block_A, bfloat16 * block_B);
extern void sbgemm_blocking_kernel_nt_alpha(blasint M, blasint N, blasint K, float alpha, bfloat16 *A, blasint lda, bfloat16 *B, blasint ldb, float *C, blasint ldc, bfloat16 * block_A, bfloat16 * block_B);
extern void sbgemm_blocking_kernel_nt_one(blasint M, blasint N, blasint K, float alpha, bfloat16 *A, blasint lda, bfloat16 *B, blasint ldb, float *C, blasint ldc, bfloat16 * block_A, bfloat16 * block_B);
extern void sbgemm_blocking_kernel_tn_alpha(blasint M, blasint N, blasint K, float alpha, bfloat16 *A, blasint lda, bfloat16 *B, blasint ldb, float *C, blasint ldc, bfloat16 * block_A, bfloat16 * block_B);
extern void sbgemm_blocking_kernel_tn_one(blasint M, blasint N, blasint K, float alpha, bfloat16 *A, blasint lda, bfloat16 *B, blasint ldb, float *C, blasint ldc, bfloat16 * block_A, bfloat16 * block_B);
extern void sbgemm_blocking_kernel_tt_alpha(blasint M, blasint N, blasint K, float alpha, bfloat16 *A, blasint lda, bfloat16 *B, blasint ldb, float *C, blasint ldc, bfloat16 * block_A, bfloat16 * block_B);
extern void sbgemm_blocking_kernel_tt_one(blasint M, blasint N, blasint K, float alpha, bfloat16 *A, blasint lda, bfloat16 *B, blasint ldb, float *C, blasint ldc, bfloat16 * block_A, bfloat16 * block_B);

#if defined(TRANS_NN)
#define SBGEMM_BLOCKING_KERNEL_ONE	sbgemm_blocking_kernel_nn_one
#define SBGEMM_BLOCKING_KERNEL_ALPHA	sbgemm_blocking_kernel_nn_alpha
#elif defined(TRANS_NT)
#define SBGEMM_BLOCKING_KERNEL_ONE	sbgemm_blocking_kernel_nt_one
#define SBGEMM_BLOCKING_KERNEL_ALPHA	sbgemm_blocking_kernel_nt_alpha
#elif defined(TRANS_TN)
#define SBGEMM_BLOCKING_KERNEL_ONE	sbgemm_blocking_kernel_tn_one
#define SBGEMM_BLOCKING_KERNEL_ALPHA	sbgemm_blocking_kernel_tn_alpha
#elif defined(TRANS_TT)
#define SBGEMM_BLOCKING_KERNEL_ONE	sbgemm_blocking_kernel_tt_one
#define SBGEMM_BLOCKING_KERNEL_ALPHA	sbgemm_blocking_kernel_tt_alpha
#endif

#define BF16_BLOCK_THRES_K 1024
// If we want to adjust this to be bigger, need to change COL_MAJOR_INCOPY_KERNEL_Kx32 kernel to be bigger also
#define BF16_BLOCK_THRES_M 32
#define BF16_BLOCK_THRES_N 1024

#define MALLOC_ALIGN64(ptr, size, raw_ptr) \
	raw_ptr = malloc((size) + 63); \
	ptr = (bfloat16 *)(((uintptr_t) raw_ptr + 63) & ~(uintptr_t)63)


#if defined(B0)
int CNAME(BLASLONG M, BLASLONG N, BLASLONG K, IFLOAT * A, BLASLONG lda, FLOAT alpha, IFLOAT * B, BLASLONG ldb, FLOAT * C, BLASLONG ldc)
#else
int CNAME(BLASLONG M, BLASLONG N, BLASLONG K, IFLOAT * A, BLASLONG lda, FLOAT alpha, IFLOAT * B, BLASLONG ldb, FLOAT beta, FLOAT * C, BLASLONG ldc)
#endif
{
	bfloat16 * block_A;
	bfloat16 * block_B;
	void* raw_ptrA;
	void* raw_ptrB;

	MALLOC_ALIGN64(block_A, sizeof(bfloat16) * BF16_BLOCK_THRES_K * BF16_BLOCK_THRES_M, raw_ptrA);
	MALLOC_ALIGN64(block_B, sizeof(bfloat16) * BF16_BLOCK_THRES_N * BF16_BLOCK_THRES_K, raw_ptrB);

#if defined(B0)
	sbgemm_zero_operation(M, N, C, ldc);
#else
	sbgemm_scal_operation(M, N, beta, C, ldc);
#endif

	if (alpha == ONE) {
		SBGEMM_BLOCKING_KERNEL_ONE(M, N, K, alpha, A, lda, B, ldb, C, ldc, block_A, block_B);
	} else {
		SBGEMM_BLOCKING_KERNEL_ALPHA(M, N, K, alpha, A, lda, B, ldb, C, ldc, block_A, block_B);
	}

	free(raw_ptrA);
	free(raw_ptrB);
	return 0;
}
