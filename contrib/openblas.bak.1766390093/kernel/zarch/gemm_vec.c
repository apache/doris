/*
 * Copyright (c) IBM Corporation 2020.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *    1. Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *
 *    2. Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *    3. Neither the name of the OpenBLAS project nor the names of
 *       its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written
 *       permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
 * USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
#include "common.h"
#include "vector-common.h"

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>


#ifdef COMPLEX
#error "Handling for complex numbers is not supported in this kernel"
#endif

#ifdef DOUBLE
#define UNROLL_M DGEMM_DEFAULT_UNROLL_M
#define UNROLL_N DGEMM_DEFAULT_UNROLL_N
#else
#define UNROLL_M SGEMM_DEFAULT_UNROLL_M
#define UNROLL_N SGEMM_DEFAULT_UNROLL_N
#endif

static const size_t unroll_m = UNROLL_M;
static const size_t unroll_n = UNROLL_N;

/* Handling of triangular matrices */
#ifdef TRMMKERNEL
static const bool trmm = true;
static const bool left =
#ifdef LEFT
	true;
#else
	false;
#endif

static const bool backwards =
#if defined(LEFT) != defined(TRANSA)
	true;
#else
	false;
#endif

#else
static const bool trmm = false;
static const bool left = false;
static const bool backwards = false;
#endif /* TRMMKERNEL */

/*
 * Background:
 *
 * The algorithm of GotoBLAS / OpenBLAS breaks down the matrix multiplication
 * problem by splitting all matrices into partitions multiple times, so that the
 * submatrices fit into the L1 or L2 caches. As a result, each multiplication of
 * submatrices can stream data fast from L1 and L2 caches. Inbetween, it copies
 * and rearranges the submatrices to enable contiguous memory accesses to
 * improve locality in both caches and TLBs.
 *
 * At the heart of the algorithm is this kernel, which multiplies, a "Block
 * matrix" A (small dimensions) with a "Panel matrix" B (number of rows is
 * small) and adds the result into a "Panel matrix" C; GotoBLAS calls this
 * operation GEBP. This kernel further partitions GEBP twice, such that (1)
 * submatrices of C and B fit into the L1 caches (GEBP_column_block) and (2) a
 * block of C fits into the registers, while multiplying panels from A and B
 * streamed from the L2 and L1 cache, respectively (GEBP_block).
 *
 *
 * Algorithm GEBP(A, B, C, m, n, k, alpha):
 *
 * The problem is calculating C += alpha * (A * B)
 * C is an m x n matrix, A is an m x k matrix, B is an k x n matrix.
 *
 * - C is in column-major-order, with an offset of ldc to the element in the
 *   next column (same row).
 * - A is in row-major-order yet stores SGEMM_UNROLL_M elements of each column
 *   contiguously while walking along rows.
 * - B is in column-major-order but packs SGEMM_UNROLL_N elements of a row
 *   contiguously.
 * If the numbers of rows and columns are not multiples of SGEMM_UNROLL_M or
 * SGEMM_UNROLL_N, the remaining elements are arranged in blocks with power-of-2
 * dimensions (e.g., 5 remaining columns would be in a block-of-4 and a
 * block-of-1).
 *
 * Note that packing A and B into that form is taken care of by the caller in
 * driver/level3/level3.c (actually done by "copy kernels").
 *
 * Steps:
 * - Partition C and B into blocks of n_r (SGEMM_UNROLL_N) columns, C_j and B_j.
 *   Now, B_j should fit into the L1 cache.
 * - For each partition, calculate C_j += alpha * (A * B_j) by
 *     (1) Calculate C_aux := A * B_j (see below)
 *     (2) unpack C_j = C_j + alpha * C_aux
 *
 *
 * Algorithm for Calculating C_aux:
 *
 * - Further partition C_aux and A into groups of m_r (SGEMM_UNROLL_M) rows,
 *   such that the m_r x n_r-submatrix of C_aux can be held in registers. Each
 *   submatrix of C_aux can be calculated independently, and the registers are
 *   added back into C_j.
 *
 * - For each row-block of C_aux:
 *   (uses a row block of A and full B_j)
 *    - stream over all columns of A, multiply with elements from B and
 *      accumulate in registers. (use different inner-kernels to exploit
 *      vectorization for varying block sizes)
 *    - add alpha * row block of C_aux back into C_j.
 *
 * Note that there are additional mechanics for handling triangular matrices,
 * calculating B := alpha (A * B) where either of the matrices A or B can be
 * triangular. In case of A, the macro "LEFT" is defined. In addition, A can
 * optionally be transposed.
 * The code effectively skips an "offset" number of columns in A and rows of B
 * in each block, to save unnecessary work by exploiting the triangular nature.
 * To handle all cases, the code discerns (1) a "left" mode when A is triangular
 * and (2) "forward" / "backwards" modes where only the first "offset"
 * columns/rows of A/B are used or where the first "offset" columns/rows are
 * skipped, respectively.
 *
 * Reference:
 *
 * The summary above is based on staring at various kernel implementations and:
 * K. Goto and R. A. Van de Geijn, Anatomy of High-Performance Matrix
 * Multiplication, in ACM Transactions of Mathematical Software, Vol.  34, No.
 * 3, May 2008.
 */

/**
 * Calculate for a row-block in C_i of size ROWSxCOLS using vector intrinsics.
 *
 * @param[in] 	A	Pointer current block of input matrix A.
 * @param[in]	k	Number of columns in A.
 * @param[in]	B	Pointer current block of input matrix B.
 * @param[inout] C	Pointer current block of output matrix C.
 * @param[in]	ldc	Offset between elements in adjacent columns in C.
 * @param[in]	alpha	Scalar factor.
 */
#define VECTOR_BLOCK(ROWS, COLS)                                              \
	static inline void GEBP_block_##ROWS##_##COLS(                        \
	    FLOAT const *restrict A, BLASLONG bk, FLOAT const *restrict B,    \
	    FLOAT *restrict C, BLASLONG ldc, FLOAT alpha) {                   \
		_Static_assert(                                               \
		    ROWS % VLEN_FLOATS == 0,                                  \
		    "rows in block must be multiples of vector length");      \
		vector_float Caux[ROWS / VLEN_FLOATS][COLS];                  \
                                                                              \
		for (BLASLONG i = 0; i < ROWS / VLEN_FLOATS; i++) {           \
			vector_float A0 =                                     \
			    vec_load_hinted(A + i * VLEN_FLOATS);             \
			for (BLASLONG j = 0; j < COLS; j++)                   \
				Caux[i][j] = A0 * B[j];                       \
		}                                                             \
                                                                              \
		/*                                                            \
		 * Stream over the row-block of A, which is packed            \
		 * column-by-column, multiply by coefficients in B and add up \
		 * into temporaries Caux (which the compiler will hold in     \
		 * registers). Vectorization: Multiply column vectors from A  \
		 * with scalars from B and add up in column vectors of Caux.  \
		 * That equates to unrolling the loop over rows (in i) and    \
		 * executing each unrolled iteration as a vector element.     \
		 */                                                           \
		for (BLASLONG k = 1; k < bk; k++) {                           \
			for (BLASLONG i = 0; i < ROWS / VLEN_FLOATS; i++) {   \
				vector_float Ak = vec_load_hinted(            \
				    A + i * VLEN_FLOATS + k * ROWS);          \
                                                                              \
				for (BLASLONG j = 0; j < COLS; j++)           \
					Caux[i][j] += Ak * B[j + k * COLS];   \
			}                                                     \
		}                                                             \
                                                                              \
		/*                                                            \
		 * Unpack row-block of C_aux into outer C_i, multiply by      \
		 * alpha and add up.                                          \
		 */                                                           \
		for (BLASLONG j = 0; j < COLS; j++) {                         \
			for (BLASLONG i = 0; i < ROWS / VLEN_FLOATS; i++) {   \
				vector_float *C_ij =                          \
				    (vector_float *)(C + i * VLEN_FLOATS +    \
						     j * ldc);                \
				if (trmm) {                                   \
					*C_ij = alpha * Caux[i][j];           \
				} else {                                      \
					*C_ij += alpha * Caux[i][j];          \
				}                                             \
			}                                                     \
		}                                                             \
	}


#if UNROLL_M == 16
VECTOR_BLOCK(16, 2)
VECTOR_BLOCK(16, 1)
#endif
#if UNROLL_N == 8
VECTOR_BLOCK(8, 8)
VECTOR_BLOCK(4, 8)
#endif
#ifndef DOUBLE
VECTOR_BLOCK(8, 4)
#endif
VECTOR_BLOCK(8, 2)
VECTOR_BLOCK(8, 1)
VECTOR_BLOCK(4, 4)
VECTOR_BLOCK(4, 2)
VECTOR_BLOCK(4, 1)

/**
 * Calculate for a row-block in C_i of size ROWSxCOLS using scalar operations.
 * Simple implementation for smaller block sizes
 *
 * @param[in] 	A	Pointer current block of input matrix A.
 * @param[in]	k	Number of columns in A.
 * @param[in]	B	Pointer current block of input matrix B.
 * @param[inout] C	Pointer current block of output matrix C.
 * @param[in]	ldc	Offset between elements in adjacent columns in C.
 * @param[in]	alpha	Scalar factor.
 */
#define SCALAR_BLOCK(ROWS, COLS)                                          \
    static inline void GEBP_block_##ROWS##_##COLS(                        \
	FLOAT const *restrict A, BLASLONG k, FLOAT const *restrict B,     \
	FLOAT *restrict C, BLASLONG ldc, FLOAT alpha) {                   \
	FLOAT Caux[ROWS][COLS] __attribute__((aligned(16)));              \
                                                                          \
	/*                                                                \
	 * Peel off first iteration (i.e., column of A) for               \
	 * initializing Caux                                              \
	 */                                                               \
	for (BLASLONG i = 0; i < ROWS; i++)                               \
	    for (BLASLONG j = 0; j < COLS; j++) Caux[i][j] = A[i] * B[j]; \
                                                                          \
	for (BLASLONG kk = 1; kk < k; kk++)                               \
	    for (BLASLONG i = 0; i < ROWS; i++)                           \
		for (BLASLONG j = 0; j < COLS; j++)                       \
		    Caux[i][j] += A[i + kk * ROWS] * B[j + kk * COLS];    \
                                                                          \
	for (BLASLONG i = 0; i < ROWS; i++)                               \
	    for (BLASLONG j = 0; j < COLS; j++)                           \
		if (trmm) {                                               \
		    C[i + j * ldc] = alpha * Caux[i][j];                  \
		} else {                                                  \
		    C[i + j * ldc] += alpha * Caux[i][j];                 \
		}                                                         \
    }

#ifdef DOUBLE
VECTOR_BLOCK(2, 4)
VECTOR_BLOCK(2, 2)
VECTOR_BLOCK(2, 1)
#else
SCALAR_BLOCK(2, 4)
SCALAR_BLOCK(2, 2)
SCALAR_BLOCK(2, 1)
#endif

SCALAR_BLOCK(1, 4)
SCALAR_BLOCK(1, 2)
SCALAR_BLOCK(1, 1)


/**
 * Calculate a row-block that fits 4x4 vector registers using a loop
 * unrolled-by-2 with explicit interleaving to better overlap loads and
 * computation.
 * This function fits 16x4 blocks for SGEMM and 8x4 blocks for DGEMM.
 */
#ifdef DOUBLE
static inline void GEBP_block_8_4(
#else // float
static inline void GEBP_block_16_4(
#endif
    FLOAT const *restrict A, BLASLONG bk, FLOAT const *restrict B,
    FLOAT *restrict C, BLASLONG ldc, FLOAT alpha) {
#define VEC_ROWS 4
#define VEC_COLS 4
#define ROWS VEC_ROWS * VLEN_FLOATS
#define COLS (VEC_COLS)

	/*
	 * Hold intermediate results in vector registers.
	 * Since we need to force the compiler's hand in places, we need to use
	 * individual variables in contrast to the generic implementation's
	 * arrays.
	 */
#define INIT_ROW_OF_C(ROW)                                        \
    vector_float A##ROW = vec_load_hinted(A + ROW * VLEN_FLOATS); \
    vector_float C_##ROW##_0 = A##ROW * B[0];                     \
    vector_float C_##ROW##_1 = A##ROW * B[1];                     \
    vector_float C_##ROW##_2 = A##ROW * B[2];                     \
    vector_float C_##ROW##_3 = A##ROW * B[3];

	INIT_ROW_OF_C(0)
	INIT_ROW_OF_C(1)
	INIT_ROW_OF_C(2)
	INIT_ROW_OF_C(3)
#undef INIT_ROW_OF_C

	if (bk > 1) {
		BLASLONG k = 1;
		vector_float Ak[VEC_ROWS], Aknext[VEC_ROWS];
		vector_float Bk[VEC_COLS], Bknext[VEC_COLS];

		/*
		 * Note that in several places, we enforce an instruction
		 * sequence that we identified empirically by utilizing dummy
		 * asm statements.
		 */

		for (BLASLONG j = 0; j < VEC_COLS; j++)
			Bk[j] = vec_splats(B[j + k * COLS]);
		asm("");

		for (BLASLONG i = 0; i < VEC_ROWS; i++)
			Ak[i] = vec_load_hinted(A + i * VLEN_FLOATS + k * ROWS);

		for (; k < (bk - 2); k += 2) {
			/*
			 * Load inputs for (k+1) into registers.
			 * Loading from B first is advantageous.
			 */
			for (BLASLONG j = 0; j < VEC_COLS; j++)
				Bknext[j] = vec_splats(B[j + (k + 1) * COLS]);
			asm("");
			for (BLASLONG i = 0; i < VEC_ROWS; i++)
				Aknext[i] = vec_load_hinted(A + i * VLEN_FLOATS +
						(k + 1) * ROWS);

			/*
			 * To achieve better instruction-level parallelism,
			 * make sure to first load input data for (k+1) before
			 * initiating compute for k. We enforce that ordering
			 * with a pseudo asm statement.
			 * Note that we need to massage this particular "barrier"
			 * depending on the gcc version.
			 */
#if __GNUC__ > 7 || defined(__clang__)
#define BARRIER_READ_BEFORE_COMPUTE(SUFFIX)                                    \
    do {                                                                       \
	asm(""                                                                 \
	    : "+v"(C_0_0), "+v"(C_0_1), "+v"(C_0_2), "+v"(C_0_3), "+v"(C_1_0), \
	      "+v"(C_1_1), "+v"(C_1_2), "+v"(C_1_3)                            \
	    : "v"(B##SUFFIX[0]), "v"(B##SUFFIX[1]), "v"(B##SUFFIX[2]),         \
	      "v"(B##SUFFIX[3]), "v"(A##SUFFIX[0]), "v"(A##SUFFIX[1]),         \
	      "v"(A##SUFFIX[2]), "v"(A##SUFFIX[3]));                           \
	asm(""                                                                 \
	    : "+v"(C_2_0), "+v"(C_2_1), "+v"(C_2_2), "+v"(C_2_3), "+v"(C_3_0), \
	      "+v"(C_3_1), "+v"(C_3_2), "+v"(C_3_3)                            \
	    : "v"(B##SUFFIX[0]), "v"(B##SUFFIX[1]), "v"(B##SUFFIX[2]),         \
	      "v"(B##SUFFIX[3]), "v"(A##SUFFIX[0]), "v"(A##SUFFIX[1]),         \
	      "v"(A##SUFFIX[2]), "v"(A##SUFFIX[3]));                           \
    } while (0)
#else // __GNUC__ <= 7
#define BARRIER_READ_BEFORE_COMPUTE(SUFFIX) \
    do {                                    \
	asm("");                            \
    } while (0)
#endif

			BARRIER_READ_BEFORE_COMPUTE(knext);

			/* Compute for (k) */
			C_0_0 += Ak[0] * Bk[0];
			C_1_0 += Ak[1] * Bk[0];
			C_2_0 += Ak[2] * Bk[0];
			C_3_0 += Ak[3] * Bk[0];

			C_0_1 += Ak[0] * Bk[1];
			C_1_1 += Ak[1] * Bk[1];
			C_2_1 += Ak[2] * Bk[1];
			C_3_1 += Ak[3] * Bk[1];

			C_0_2 += Ak[0] * Bk[2];
			C_1_2 += Ak[1] * Bk[2];
			C_2_2 += Ak[2] * Bk[2];
			C_3_2 += Ak[3] * Bk[2];

			C_0_3 += Ak[0] * Bk[3];
			C_1_3 += Ak[1] * Bk[3];
			C_2_3 += Ak[2] * Bk[3];
			C_3_3 += Ak[3] * Bk[3];

			asm("");

			/*
			 * Load inputs for (k+2) into registers.
			 * First load from B.
			 */
			for (BLASLONG j = 0; j < VEC_COLS; j++)
				Bk[j] = vec_splats(B[j + (k + 2) * COLS]);
			asm("");
			for (BLASLONG i = 0; i < VEC_ROWS; i++)
				Ak[i] = vec_load_hinted(A + i * VLEN_FLOATS + (k + 2) * ROWS);

			/*
			 * As above, make sure to first schedule the loads for (k+2)
			 * before compute for (k+1).
			 */
			BARRIER_READ_BEFORE_COMPUTE(k);

			/* Compute on (k+1) */
			C_0_0 += Aknext[0] * Bknext[0];
			C_1_0 += Aknext[1] * Bknext[0];
			C_2_0 += Aknext[2] * Bknext[0];
			C_3_0 += Aknext[3] * Bknext[0];

			C_0_1 += Aknext[0] * Bknext[1];
			C_1_1 += Aknext[1] * Bknext[1];
			C_2_1 += Aknext[2] * Bknext[1];
			C_3_1 += Aknext[3] * Bknext[1];

			C_0_2 += Aknext[0] * Bknext[2];
			C_1_2 += Aknext[1] * Bknext[2];
			C_2_2 += Aknext[2] * Bknext[2];
			C_3_2 += Aknext[3] * Bknext[2];

			C_0_3 += Aknext[0] * Bknext[3];
			C_1_3 += Aknext[1] * Bknext[3];
			C_2_3 += Aknext[2] * Bknext[3];
			C_3_3 += Aknext[3] * Bknext[3];
		}

		/* Wrapup remaining k's */
		for (; k < bk; k++) {
			vector_float Ak;

#define COMPUTE_WRAPUP_ROW(ROW)                             \
    Ak = vec_load_hinted(A + ROW * VLEN_FLOATS + k * ROWS); \
    C_##ROW##_0 += Ak * B[0 + k * COLS];                    \
    C_##ROW##_1 += Ak * B[1 + k * COLS];                    \
    C_##ROW##_2 += Ak * B[2 + k * COLS];                    \
    C_##ROW##_3 += Ak * B[3 + k * COLS];

			COMPUTE_WRAPUP_ROW(0)
			COMPUTE_WRAPUP_ROW(1)
			COMPUTE_WRAPUP_ROW(2)
			COMPUTE_WRAPUP_ROW(3)
#undef COMPUTE_WRAPUP_ROW
		}
	}

	/*
	 * Unpack row-block of C_aux into outer C_i, multiply by
	 * alpha and add up (or assign for TRMM).
	 */
#define WRITE_BACK_C(ROW, COL)                                   \
    do {                                                         \
	vector_float *Cij =                                      \
	    (vector_float *)(C + ROW * VLEN_FLOATS + COL * ldc); \
	if (trmm) {                                              \
	    *Cij = alpha * C_##ROW##_##COL;                   \
	} else {                                                 \
	    *Cij += alpha * C_##ROW##_##COL;                  \
	}                                                        \
    } while (0)

	WRITE_BACK_C(0, 0); WRITE_BACK_C(0, 1); WRITE_BACK_C(0, 2); WRITE_BACK_C(0, 3);
	WRITE_BACK_C(1, 0); WRITE_BACK_C(1, 1); WRITE_BACK_C(1, 2); WRITE_BACK_C(1, 3);
	WRITE_BACK_C(2, 0); WRITE_BACK_C(2, 1); WRITE_BACK_C(2, 2); WRITE_BACK_C(2, 3);
	WRITE_BACK_C(3, 0); WRITE_BACK_C(3, 1); WRITE_BACK_C(3, 2); WRITE_BACK_C(3, 3);
#undef WRITE_BACK_C

#undef ROWS
#undef VEC_ROWS
#undef COLS
#undef VEC_COLS
#undef BARRIER_READ_BEFORE_COMPUTE
}

/**
 * Handle calculation for row blocks in C_i of any size by dispatching into
 * macro-defined (inline) functions or by deferring to a simple generic
 * implementation. Note that the compiler can remove this awkward-looking
 * dispatching code while inlineing.
 *
 * @param[in]	m	Number of rows in block C_i.
 * @param[in]	n	Number of columns in block C_i.
 * @param[in]	first_row Index of first row of the block C_i (relative to C).
 * @param[in]	A	Pointer to input matrix A (note: all of it).
 * @param[in]	k	Number of columns in A and rows in B.
 * @param[in]	B	Pointer to current column block (panel) of input matrix B.
 * @param[inout] C	Pointer to current column block (panel) of output matrix C.
 * @param[in]	ldc	Offset between elements in adjacent columns in C.
 * @param[in]	alpha	Scalar factor.
 * @param[in]	offset  Number of columns of A and rows of B to skip (for triangular matrices).
 * @param[in]	off	Running offset for handling triangular matrices.
 */
static inline void GEBP_block(BLASLONG m, BLASLONG n,
		       BLASLONG first_row,
		       const FLOAT * restrict A, BLASLONG k,
		       const FLOAT * restrict B,
		       FLOAT *restrict C, BLASLONG ldc,
		       FLOAT alpha,
		       BLASLONG offset, BLASLONG off)
{
	if (trmm && left)
		off = offset + first_row;

	A += first_row * k;
	C += first_row;

	if (trmm) {
		if (backwards) {
			A += off * m;
			B += off * n;
			k -= off;
		} else {
			if (left) {
				k = off + m;
			} else {
				k = off + n;
			}
		}
	}

	/* Dispatch into the implementation for each block size: */

#define BLOCK(bm, bn)                                           \
	if (m == bm && n == bn) {                               \
		GEBP_block_##bm##_##bn(A, k, B, C, ldc, alpha); \
		return;                                         \
	}

#if UNROLL_M == 16
	BLOCK(16, 4); BLOCK(16, 2); BLOCK(16, 1);
#endif
#if UNROLL_N == 8
	BLOCK(8, 8); BLOCK(4, 8);
#endif
	BLOCK(8, 4); BLOCK(8, 2); BLOCK(8, 1);
	BLOCK(4, 4); BLOCK(4, 2); BLOCK(4, 1);

	BLOCK(2, 4); BLOCK(2, 2); BLOCK(2, 1);

	BLOCK(1, 4); BLOCK(1, 2); BLOCK(1, 1);

#undef BLOCK
}

/**
 * Handle a column block (panel) of C and B while calculating C += alpha(A * B).
 *
 * @param[in]	num_cols	Number of columns in the block (in C and B).
 * @param[in]	first_col	First column of the current block (in C and B).
 * @param[in]	A	Pointer to input matrix A.
 * @param[in]	bk	Number of columns in A and rows in B.
 * @param[in]	B	Pointer to input matrix B (note: all of it).
 * @param[in]	bm	Number of rows in C and A.
 * @param[inout] C	Pointer to output matrix C (note: all of it).
 * @param[in]	ldc	Offset between elements in adjacent columns in C.
 * @param[in]	alpha	Scalar factor.
 * @param[in]	offset	Number of columns of A and rows of B to skip (for triangular matrices).
 */
static inline void GEBP_column_block(BLASLONG num_cols, BLASLONG first_col,
			const FLOAT *restrict A, BLASLONG bk,
			const FLOAT *restrict B, BLASLONG bm,
			FLOAT *restrict C, BLASLONG ldc,
			FLOAT alpha,
			BLASLONG const offset) {

	FLOAT *restrict C_i = C + first_col * ldc;
	/*
	 * B is in column-order with n_r packed row elements, which does
	 * not matter -- we always move in full such blocks of
	 * column*pack
	 */
	const FLOAT *restrict B_i = B + first_col * bk;

	BLASLONG off = 0;
	if (trmm) {
		if (left) {
			off = offset;
		} else {
			off = -offset + first_col;
		}
	}

	/*
	 * Calculate C_aux := A * B_j
	 * then unpack C_i += alpha * C_aux.
	 *
	 * For that purpose, further partition C_aux and A into blocks
	 * of m_r (unroll_m) rows, or powers-of-2 if smaller.
	 */
	BLASLONG row = 0;
	for (BLASLONG block_size = unroll_m; block_size > 0; block_size /= 2)
		for (; bm - row >= block_size; row += block_size)
			GEBP_block(block_size, num_cols, row, A, bk, B_i, C_i,
				   ldc, alpha, offset, off);
}

/**
 * Inner kernel for matrix-matrix multiplication. C += alpha (A * B)
 * where C is an m-by-n matrix, A is m-by-k and B is k-by-n. Note that A, B, and
 * C are pointers to submatrices of the actual matrices.
 *
 * For triangular matrix multiplication, calculate B := alpha (A * B) where A
 * or B can be triangular (in case of A, the macro LEFT will be defined).
 *
 * @param[in]	bm	Number of rows in C and A.
 * @param[in]	bn	Number of columns in C and B.
 * @param[in]	bk	Number of columns in A and rows in B.
 * @param[in]	alpha	Scalar factor.
 * @param[in]	ba	Pointer to input matrix A.
 * @param[in]	bb	Pointer to input matrix B.
 * @param[inout] C	Pointer to output matrix C.
 * @param[in]	ldc	Offset between elements in adjacent columns in C.
 * @param[in]	offset	Number of columns of A and rows of B to skip (for triangular matrices).
 * @returns 0 on success.
 */
int CNAME(BLASLONG bm, BLASLONG bn, BLASLONG bk, FLOAT alpha,
	  FLOAT *restrict ba, FLOAT *restrict bb,
	  FLOAT *restrict C, BLASLONG ldc
#ifdef TRMMKERNEL
	  , BLASLONG offset
#endif
	  )
{
	if ( (bm == 0) || (bn == 0) || (bk == 0) || (alpha == ZERO))
		return 0;

	/*
	 * interface code allocates buffers for ba and bb at page
	 * granularity (i.e., using mmap(MAP_ANONYMOUS), so enable the compiler
	 * to make use of the fact in vector load operations.
	 */
	ba = __builtin_assume_aligned(ba, 16);
	bb = __builtin_assume_aligned(bb, 16);

	/*
	 * Use offset and off even when compiled as SGEMMKERNEL to simplify
	 * function signatures and function calls.
	 */
#ifndef TRMMKERNEL
	BLASLONG const offset = 0;
#endif

	/*
	 * Partition B and C into blocks of n_r (unroll_n) columns, called B_i
	 * and C_i. For each partition, calculate C_i += alpha * (A * B_j).
	 *
	 * For remaining columns that do not fill up a block of n_r, iteratively
	 * use smaller block sizes of powers of 2.
	 */
	BLASLONG col = 0;
	for (BLASLONG block_size = unroll_n; block_size > 0; block_size /= 2)
		for (; bn - col >= block_size; col += block_size)
			GEBP_column_block(block_size, col, ba, bk, bb, bm, C, ldc, alpha, offset);

   return 0;
}
