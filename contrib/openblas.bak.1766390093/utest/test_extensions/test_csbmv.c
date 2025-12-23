/*****************************************************************************
Copyright (c) 2023, The OpenBLAS Project
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
      derived from this software without specific prior written
      permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

**********************************************************************************/

#include "utest/openblas_utest.h"
#include "common.h"

#define DATASIZE 100
#define INCREMENT 2

struct DATA_CSBMV {
    float sp_matrix[DATASIZE * (DATASIZE + 1)];
    float sb_matrix[DATASIZE * DATASIZE * 2];
    float b_test[DATASIZE * 2 * INCREMENT];
    float c_test[DATASIZE * 2 * INCREMENT];
    float c_verify[DATASIZE * 2 * INCREMENT];
};

// SINGLE_EPS_ZGEMV = MAX_VAL * NUMBER OF OPERATIONS * FLT_EPSILON
// SINGLE_EPS_ZGEMV = 5.0 * O(100 * 100) * 1.19e-07 = 5*e-03
#define SINGLE_EPS_ZGEMV 5e-03

#ifdef BUILD_COMPLEX
static struct DATA_CSBMV data_csbmv;

/** 
 * Transform full-storage symmetric band matrix A to upper (U) or lower (L)
 * band-packed storage mode.
 * 
 * param uplo specifies whether matrix a is upper or lower band-packed.
 * param n - number of rows and columns of A
 * param k - number of super-diagonals of A
 * output param a - buffer for holding symmetric band-packed matrix
 * param lda - specifies the leading dimension of a
 * param sb_matrix - buffer holding full-storage symmetric band matrix A 
 * param ldm - specifies the leading dimension of A
 */
static void transform_to_band_storage(char uplo, blasint n, blasint k, float* a, blasint lda,
                                     float* sb_matrix, blasint ldm) 
{
    blasint i, j, m;
    if (uplo == 'L') {
        for (j = 0; j < n; j++)
        {
            m = -j;
            for (i = 2 * j; i < MIN(2 * n, 2 * (j + k + 1)); i += 2)
            {
                a[(2*m + i) + j * lda * 2] = sb_matrix[i + j * ldm * 2];
                a[(2*m + (i + 1)) + j * lda * 2] = sb_matrix[(i + 1) + j * ldm * 2];
            }
        }
    }
    else {
        for (j = 0; j < n; j++)
        {   
            m = k - j;
            for (i = MAX(0, 2*(j - k)); i <= j*2; i += 2)
            {
                a[(2*m + i) + j * lda * 2] = sb_matrix[i + j * ldm * 2];
                a[(2*m + (i + 1)) + j * lda * 2] = sb_matrix[(i + 1) + j * ldm * 2];
            }
        }
    }
}

/** 
 * Generate full-storage symmetric band matrix A with k - super-diagonals
 * from input symmetric packed matrix in lower packed mode (L)
 * 
 * output param sb_matrix - buffer for holding full-storage symmetric band matrix.
 * param sp_matrix - buffer holding input symmetric packed matrix
 * param n - number of rows and columns of A
 * param k - number of super-diagonals of A
 */
static void get_symmetric_band_matr(float *sb_matrix, float *sp_matrix, blasint n, blasint k)
{
    blasint m;
    blasint i, j;
    m = 0;
    for (i = 0; i < n; i++)
    {
        for (j = 0; j < n * 2; j += 2)
        {
            // Make matrix band with k super-diagonals
            if (fabs((i+1) - ceil((j+1)/2.0f)) > k) 
            {
                sb_matrix[i * n * 2 + j] = 0.0f;
                sb_matrix[i * n * 2 + j + 1] = 0.0f;
                continue;
            }

            if (j / 2 < i)
            {
                sb_matrix[i * n * 2 + j] = 
                        sb_matrix[j * n + i * 2];
                sb_matrix[i * n * 2 + j + 1] = 
                        sb_matrix[j * n + i * 2 + 1];
            }
            else
            {
                sb_matrix[i * n * 2 + j] = sp_matrix[m++];
                sb_matrix[i * n * 2 + j + 1] = sp_matrix[m++];
            }
        }
    }
}

/** 
 * Check if error function was called with expected function name
 * and param info
 * 
 * param uplo specifies whether matrix a is upper or lower band-packed.
 * param n - number of rows and columns of A
 * param k - number of super-diagonals of A
 * param lda - specifies the leading dimension of a
 * param inc_b - stride of vector b_test
 * param inc_c - stride of vector c_test
 * param expected_info - expected invalid parameter number in csbmv
 * return TRUE if everything is ok, otherwise FALSE 
 */
static int check_badargs(char uplo, blasint n, blasint k, blasint lda, blasint inc_b,
                          blasint inc_c, int expected_info)
{
    float alpha[] = {1.0f, 1.0f};
    float beta[] = {0.0f, 0.0f};

    float a[2];
    srand_generate(a, 2);

    set_xerbla("CSBMV ", expected_info);

    BLASFUNC(csbmv)(&uplo, &n, &k, alpha, a, &lda, data_csbmv.b_test, 
                    &inc_b, beta, data_csbmv.c_test, &inc_c);

    return check_error();
}

/**
 * Comapare results computed by csbmv and cgemv 
 * since csbmv is cgemv for symmetric band matrix
 * 
 * param uplo specifies whether matrix A is upper or lower triangular
 * param n - number of rows and columns of A
 * param k - number of super-diagonals of A
 * param alpha - scaling factor for the matrix-vector product
 * param lda - specifies the leading dimension of a
 * param inc_b - stride of vector b_test
 * param beta - scaling factor for vector c_test
 * param inc_c - stride of vector c_test
 * param lda - specifies the leading dimension of a
 * return norm of differences 
 */
static float check_csbmv(char uplo, blasint n, blasint k, float *alpha, blasint lda, 
    blasint inc_b, float *beta, blasint inc_c, blasint ldm)
{
    blasint i;

    // Trans param for gemv (can use any, since the input matrix is symmetric)
    char trans = 'N';

    // Symmetric band packed matrix for sbmv
    float *a = (float*) malloc(lda * n * 2 * sizeof(float));

    // Fill symmetric packed matrix sp_matrix, vector b_test, vector c_test 
    srand_generate(data_csbmv.sp_matrix, n * (n + 1));
    srand_generate(data_csbmv.b_test, n * inc_b * 2);
    srand_generate(data_csbmv.c_test, n * inc_c * 2);

    // Copy vector c_test for cgemv
    for (i = 0; i < n * inc_c * 2; i++)
        data_csbmv.c_verify[i] = data_csbmv.c_test[i];

    // Generate full-storage symmetric band matrix
    // with k super-diagonals from symmetric packed matrix
    get_symmetric_band_matr(data_csbmv.sb_matrix, data_csbmv.sp_matrix, n, k);

    // Transform symmetric band matrix from conventional
    // full matrix storage  to band storage for csbmv
    transform_to_band_storage(uplo, n, k, a, lda, data_csbmv.sb_matrix, ldm);

    BLASFUNC(cgemv)(&trans, &n, &n, alpha, data_csbmv.sb_matrix, &ldm, data_csbmv.b_test,
                    &inc_b, beta, data_csbmv.c_verify, &inc_c);

    BLASFUNC(csbmv)(&uplo, &n, &k, alpha, a, &lda,
                    data_csbmv.b_test, &inc_b, beta, data_csbmv.c_test, &inc_c);

    // Find the differences between output vector caculated by csbmv and cgemv
    for (i = 0; i < n * inc_c * 2; i++)
        data_csbmv.c_test[i] -= data_csbmv.c_verify[i];
    free(a);
    // Find the norm of differences
    return BLASFUNC(scnrm2)(&n, data_csbmv.c_test, &inc_c);
}

/**
 * Test csbmv by comparing it against cgemv
 * with the following options:
 * 
 * a is upper-band-packed symmetric matrix
 * Number of rows and columns of A is 100
 * Stride of vector b_test is 1
 * Stride of vector c_test is 1
 * Number of super-diagonals k is 0 
 */
CTEST(csbmv, upper_k_0_inc_b_1_inc_c_1_n_100)
{
    blasint n = DATASIZE, inc_b = 1, inc_c = 1;
    blasint k = 0;
    blasint lda = k + 1;
    blasint ldm = n;
    char uplo = 'U';

    float alpha[] = {1.0f, 1.0f};
    float beta[] = {1.0f, 1.0f};

    float norm = check_csbmv(uplo, n, k, alpha, lda, inc_b, beta, inc_c, ldm);
    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS_ZGEMV);
}

/**
 * Test csbmv by comparing it against cgemv
 * with the following options:
 * 
 * a is upper-band-packed symmetric matrix
 * Number of rows and columns of A is 100
 * Stride of vector b_test is 1
 * Stride of vector c_test is 1
 * Number of super-diagonals k is 1
 */
CTEST(csbmv, upper_k_1_inc_b_1_inc_c_1_n_100)
{
    blasint n = DATASIZE, inc_b = 1, inc_c = 1;
    blasint k = 1;
    blasint lda = k + 1;
    blasint ldm = n;
    char uplo = 'U';

    float alpha[] = {1.0f, 1.0f};
    float beta[] = {1.0f, 1.0f};

    float norm = check_csbmv(uplo, n, k, alpha, lda, inc_b, beta, inc_c, ldm);
    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS_ZGEMV);
}

/**
 * Test csbmv by comparing it against cgemv
 * with the following options:
 * 
 * a is upper-band-packed symmetric matrix
 * Number of rows and columns of A is 100
 * Stride of vector b_test is 1
 * Stride of vector c_test is 1
 * Number of super-diagonals k is 2
 */
CTEST(csbmv, upper_k_2_inc_b_1_inc_c_1_n_100)
{
    blasint n = DATASIZE, inc_b = 1, inc_c = 1;
    blasint k = 2;
    blasint lda = k + 1;
    blasint ldm = n;
    char uplo = 'U';

    float alpha[] = {1.0f, 1.0f};
    float beta[] = {1.0f, 1.0f};

    float norm = check_csbmv(uplo, n, k, alpha, lda, inc_b, beta, inc_c, ldm);
    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS_ZGEMV);
}

/**
 * Test csbmv by comparing it against cgemv
 * with the following options:
 * 
 * a is upper-band-packed symmetric matrix
 * Number of rows and columns of A is 100
 * Stride of vector b_test is 2
 * Stride of vector c_test is 1
 * Number of super-diagonals k is 2
 */
CTEST(csbmv, upper_k_2_inc_b_2_inc_c_1_n_100)
{
    blasint n = DATASIZE, inc_b = 2, inc_c = 1;
    blasint k = 2;
    blasint lda = k + 1;
    blasint ldm = n;
    char uplo = 'U';

    float alpha[] = {2.0f, 1.0f};
    float beta[] = {2.0f, 1.0f};

    float norm = check_csbmv(uplo, n, k, alpha, lda, inc_b, beta, inc_c, ldm);
    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS_ZGEMV);
}

/**
 * Test csbmv by comparing it against cgemv
 * with the following options:
 * 
 * a is upper-band-packed symmetric matrix
 * Number of rows and columns of A is 100
 * Stride of vector b_test is 2
 * Stride of vector c_test is 2
 * Number of super-diagonals k is 2
 */
CTEST(csbmv, upper_k_2_inc_b_2_inc_c_2_n_100)
{
    blasint n = DATASIZE, inc_b = 2, inc_c = 2;
    blasint k = 2;
    blasint lda = k + 1;
    blasint ldm = n;
    char uplo = 'U';

    float alpha[] = {2.0f, 1.0f};
    float beta[] = {2.0f, 1.0f};

    float norm = check_csbmv(uplo, n, k, alpha, lda, inc_b, beta, inc_c, ldm);
    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS_ZGEMV);
}

/**
 * Test csbmv by comparing it against cgemv
 * with the following options:
 * 
 * a is lower-band-packed symmetric matrix
 * Number of rows and columns of A is 100
 * Stride of vector b_test is 1
 * Stride of vector c_test is 1
 * Number of super-diagonals k is 0
 */
CTEST(csbmv, lower_k_0_inc_b_1_inc_c_1_n_100)
{
    blasint n = DATASIZE, inc_b = 1, inc_c = 1;
    blasint k = 0;
    blasint lda = k + 1;
    blasint ldm = n;
    char uplo = 'L';

    float alpha[] = {1.0f, 1.0f};
    float beta[] = {1.0f, 1.0f};

    float norm = check_csbmv(uplo, n, k, alpha, lda, inc_b, beta, inc_c, ldm);
    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS_ZGEMV);
}

/**
 * Test csbmv by comparing it against cgemv
 * with the following options:
 * 
 * a is lower-band-packed symmetric matrix
 * Number of rows and columns of A is 100
 * Stride of vector b_test is 1
 * Stride of vector c_test is 1
 * Number of super-diagonals k is 1
 */
CTEST(csbmv, lower_k_1_inc_b_1_inc_c_1_n_100)
{
    blasint n = DATASIZE, inc_b = 1, inc_c = 1;
    blasint k = 1;
    blasint lda = k + 1;
    blasint ldm = n;
    char uplo = 'L';

    float alpha[] = {1.0f, 1.0f};
    float beta[] = {1.0f, 1.0f};

    float norm = check_csbmv(uplo, n, k, alpha, lda, inc_b, beta, inc_c, ldm);
    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS_ZGEMV);
}

/**
 * Test csbmv by comparing it against cgemv
 * with the following options:
 * 
 * a is lower-band-packed symmetric matrix
 * Number of rows and columns of A is 100
 * Stride of vector b_test is 1
 * Stride of vector c_test is 1
 * Number of super-diagonals k is 2
 */
CTEST(csbmv, lower_k_2_inc_b_1_inc_c_1_n_100)
{
    blasint n = DATASIZE, inc_b = 1, inc_c = 1;
    blasint k = 2;
    blasint lda = k + 1;
    blasint ldm = n;
    char uplo = 'L';

    float alpha[] = {1.0f, 1.0f};
    float beta[] = {1.0f, 1.0f};

    float norm = check_csbmv(uplo, n, k, alpha, lda, inc_b, beta, inc_c, ldm);
    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS_ZGEMV);
}

/**
 * Test csbmv by comparing it against cgemv
 * with the following options:
 * 
 * a is lower-band-packed symmetric matrix
 * Number of rows and columns of A is 100
 * Stride of vector b_test is 2
 * Stride of vector c_test is 1
 * Number of super-diagonals k is 2
 */
CTEST(csbmv, lower_k_2_inc_b_2_inc_c_1_n_100)
{
    blasint n = DATASIZE, inc_b = 2, inc_c = 1;
    blasint k = 2;
    blasint lda = k + 1;
    blasint ldm = n;
    char uplo = 'L';

    float alpha[] = {2.0f, 1.0f};
    float beta[] = {2.0f, 1.0f};

    float norm = check_csbmv(uplo, n, k, alpha, lda, inc_b, beta, inc_c, ldm);
    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS_ZGEMV);
}

/**
 * Test csbmv by comparing it against cgemv
 * with the following options:
 * 
 * a is lower-band-packed symmetric matrix
 * Number of rows and columns of A is 100
 * Stride of vector b_test is 2
 * Stride of vector c_test is 2
 * Number of super-diagonals k is 2
 */
CTEST(csbmv, lower_k_2_inc_b_2_inc_c_2_n_100)
{
    blasint n = DATASIZE, inc_b = 2, inc_c = 2;
    blasint k = 2;
    blasint lda = k + 1;
    blasint ldm = n;
    char uplo = 'L';

    float alpha[] = {2.0f, 1.0f};
    float beta[] = {2.0f, 1.0f};

    float norm = check_csbmv(uplo, n, k, alpha, lda, inc_b, beta, inc_c, ldm);
    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS_ZGEMV);
}

/** 
 * Check if output matrix a contains any NaNs
 */
CTEST(csbmv, check_for_NaN)
{
    blasint n = DATASIZE, inc_b = 1, inc_c = 1;
    blasint k = 0;
    blasint lda = k + 1;
    blasint ldm = n;
    char uplo = 'U';

    float alpha[] = {1.0f, 1.0f};
    float beta[] = {1.0f, 1.0f};

    float norm = check_csbmv(uplo, n, k, alpha, lda, inc_b, beta, inc_c, ldm);

    ASSERT_TRUE(norm == norm); /* NaN == NaN is false  */
}

/**
 * Test error function for an invalid param uplo.
 * Uplo specifies whether a is in upper (U) or lower (L) band-packed storage mode.
 */
CTEST(csbmv, xerbla_uplo_invalid)
{
    blasint n = 1, inc_b = 1, inc_c = 1;
    char uplo = 'O';
    blasint k = 0;
    blasint lda = k + 1;
    int expected_info = 1;

    int passed = check_badargs(uplo, n, k, lda, inc_b, inc_c, expected_info);
    ASSERT_EQUAL(TRUE, passed);
}

/** 
 * Test error function for an invalid param N -
 * number of rows and columns of A. Must be at least zero.
 */
CTEST(csbmv, xerbla_n_invalid)
{
    blasint n = INVALID, inc_b = 1, inc_c = 1;
    char uplo = 'U';
    blasint k = 0;
    blasint lda = k + 1;
    int expected_info = 2;

    int passed = check_badargs(uplo, n, k, lda, inc_b, inc_c, expected_info);
    ASSERT_EQUAL(TRUE, passed);
}

/**
 * Check if n - number of rows and columns of A equal zero.
 */
CTEST(csbmv, check_n_zero)
{
    blasint n = 0, inc_b = 1, inc_c = 1;
    blasint k = 0;
    blasint lda = k + 1;
    blasint ldm = 1;
    char uplo = 'U';

    float alpha[] = {1.0f, 1.0f};
    float beta[] = {0.0f, 0.0f};

    float norm = check_csbmv(uplo, n, k, alpha, lda, inc_b, beta, inc_c, ldm);
    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS_ZGEMV);
}

/**
 * Test error function for an invalid param inc_b -
 * stride of vector b_test. Can't be zero. 
 */
CTEST(csbmv, xerbla_inc_b_zero)
{
    blasint n = 1, inc_b = 0, inc_c = 1;
    char uplo = 'U';
    blasint k = 0;
    blasint lda = k + 1;
    int expected_info = 8;

    int passed = check_badargs(uplo, n, k, lda, inc_b, inc_c, expected_info);
    ASSERT_EQUAL(TRUE, passed);
}

/**
 * Test error function for an invalid param inc_c -
 * stride of vector c_test. Can't be zero. 
 */
CTEST(csbmv, xerbla_inc_c_zero)
{
    blasint n = 1, inc_b = 1, inc_c = 0;
    char uplo = 'U';
    blasint k = 0;
    blasint lda = k + 1;
    int expected_info = 11;

    int passed = check_badargs(uplo, n, k, lda, inc_b, inc_c, expected_info);
    ASSERT_EQUAL(TRUE, passed);
}

/**
 * Test error function for an invalid param k -
 * number of super-diagonals of A. Must be at least zero.
 */
CTEST(csbmv, xerbla_k_invalid)
{
    blasint n = 1, inc_b = 1, inc_c = 1;
    char uplo = 'U';
    blasint k = INVALID;
    blasint lda = 1;
    int expected_info = 3;

    int passed = check_badargs(uplo, n, k, lda, inc_b, inc_c, expected_info);
    ASSERT_EQUAL(TRUE, passed);
}

/**
 * Test error function for an invalid param lda -
 * specifies the leading dimension of a. Must be at least (k+1).
 */
CTEST(csbmv, xerbla_lda_invalid)
{
    blasint n = 1, inc_b = 1, inc_c = 1;
    char uplo = 'U';
    blasint k = 0;
    blasint lda = INVALID;
    int expected_info = 6;

    int passed = check_badargs(uplo, n, k, lda, inc_b, inc_c, expected_info);
    ASSERT_EQUAL(TRUE, passed);
}
#endif
