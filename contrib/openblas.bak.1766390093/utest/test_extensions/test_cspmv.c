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

struct DATA_CSPMV {
    float a_verify[DATASIZE * DATASIZE * 2];
    float a_test[DATASIZE * (DATASIZE + 1)];
    float b_test[DATASIZE * 2 * INCREMENT];
    float c_test[DATASIZE * 2 * INCREMENT];
    float c_verify[DATASIZE * 2 * INCREMENT];
};

#ifdef BUILD_COMPLEX
static struct DATA_CSPMV data_cspmv;

/**
 * Compute spmv via gemv since spmv is gemv for symmetric packed matrix
 *
 * param uplo specifies whether matrix A is upper or lower triangular
 * param n - number of rows and columns of A
 * param alpha - scaling factor for the matrix-vector product
 * param a - buffer holding input matrix A
 * param b - Buffer holding input vector b
 * param inc_b - stride of vector b
 * param beta - scaling factor for vector c
 * param c - buffer holding input/output vector c
 * param inc_c - stride of vector c
 * output param data_cspmv.c_verify - matrix computed by gemv
 */
static void cspmv_trusted(char uplo, blasint n, float *alpha, float *a,
                          float *b, blasint inc_b, float *beta, float *c,
                          blasint inc_c)
{
    blasint k;
    blasint i, j;

    // param for gemv (can use any, since the input matrix is symmetric)
    char trans = 'N';

    // Unpack the input symmetric packed matrix
    if (uplo == 'L')
    {
        k = 0;
        for (i = 0; i < n; i++)
        {
            for (j = 0; j < n * 2; j += 2)
            {
                if (j / 2 < i)
                {
                    data_cspmv.a_verify[i * n * 2 + j] =
                        data_cspmv.a_verify[j * n + i * 2];
                    data_cspmv.a_verify[i * n * 2 + j + 1] =
                        data_cspmv.a_verify[j * n + i * 2 + 1];
                }
                else
                {
                    data_cspmv.a_verify[i * n * 2 + j] = a[k++];
                    data_cspmv.a_verify[i * n * 2 + j + 1] = a[k++];
                }
            }
        }
    }
    else
    {
        k = n * (n + 1) - 1;
        for (j = 2 * n - 1; j >= 0; j -= 2)
        {
            for (i = n - 1; i >= 0; i--)
            {
                if (j / 2 < i)
                {
                    data_cspmv.a_verify[i * n * 2 + j] =
                        data_cspmv.a_verify[(j - 1) * n + i * 2 + 1];
                    data_cspmv.a_verify[i * n * 2 + j - 1] =
                        data_cspmv.a_verify[(j - 1) * n + i * 2];
                }
                else
                {
                    data_cspmv.a_verify[i * n * 2 + j] = a[k--];
                    data_cspmv.a_verify[i * n * 2 + j - 1] = a[k--];
                }
            }
        }
    }

    // Run gemv with the unpacked matrix
    BLASFUNC(cgemv)(&trans, &n, &n, alpha, data_cspmv.a_verify, &n, b,
                    &inc_b, beta, data_cspmv.c_verify, &inc_c);
}

/**
 * Comapare results computed by cspmv and cspmv_trusted
 *
 * param uplo specifies whether matrix A is upper or lower triangular
 * param n - number of rows and columns of A
 * param alpha - scaling factor for the matrix-vector product
 * param inc_b - stride of vector b
 * param beta - scaling factor for vector c
 * param inc_c - stride of vector c
 * return norm of differences
 */
static float check_cspmv(char uplo, blasint n, float *alpha, blasint inc_b,
                         float *beta, blasint inc_c)
{
    blasint i;

    // Fill symmetric packed maxtix a, vectors b and c
    srand_generate(data_cspmv.a_test, n * (n + 1));
    srand_generate(data_cspmv.b_test, 2 * n * inc_b);
    srand_generate(data_cspmv.c_test, 2 * n * inc_c);

    // Copy vector c for cspmv_trusted
    for (i = 0; i < n * 2 * inc_c; i++)
        data_cspmv.c_verify[i] = data_cspmv.c_test[i];

    cspmv_trusted(uplo, n, alpha, data_cspmv.a_test, data_cspmv.b_test,
                  inc_b, beta, data_cspmv.c_verify, inc_c);

    BLASFUNC(cspmv)(&uplo, &n, alpha, data_cspmv.a_test, data_cspmv.b_test,
                    &inc_b, beta, data_cspmv.c_test, &inc_c);

    // Find the differences between output vector computed by cspmv and cspmv_trusted
    for (i = 0; i < n * 2 * inc_c; i++)
        data_cspmv.c_test[i] -= data_cspmv.c_verify[i];

    // Find the norm of differences
    return BLASFUNC(scnrm2)(&n, data_cspmv.c_test, &inc_c);
}

/**
 * Check if error function was called with expected function name
 * and param info
 *
 * param uplo specifies whether matrix A is upper or lower triangular
 * param n - number of rows and columns of A
 * param inc_b - stride of vector b
 * param inc_c - stride of vector c
 * param expected_info - expected invalid parameter number in cspmv
 * return TRUE if everything is ok, otherwise FALSE
 */
static int check_badargs(char uplo, blasint n, blasint inc_b,
                         blasint inc_c, int expected_info)
{
    float alpha[] = {1.0, 1.0};
    float beta[] = {0.0, 0.0};

    set_xerbla("CSPMV ", expected_info);

    BLASFUNC(cspmv)(&uplo, &n, alpha, data_cspmv.a_test, data_cspmv.b_test,
                    &inc_b, beta, data_cspmv.c_test, &inc_c);

    return check_error();
}

/**
 * Test cspmv by comparing it against cgemv
 * with the following options:
 *
 * A is upper triangular
 * Number of rows and columns of A is 100
 * Stride of vector b is 1
 * Stride of vector c is 1
 */
CTEST(cspmv, upper_inc_b_1_inc_c_1_N_100)
{
    blasint N = DATASIZE, inc_b = 1, inc_c = 1;
    char uplo = 'U';
    float alpha[] = {1.0f, 1.0f};
    float beta[] = {0.0f, 0.0f};

    float norm = check_cspmv(uplo, N, alpha, inc_b, beta, inc_c);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_TOL);
}

/**
 * Test cspmv by comparing it against cgemv
 * with the following options:
 *
 * A is upper triangular
 * Number of rows and columns of A is 100
 * Stride of vector b is 1
 * Stride of vector c is 2
 */
CTEST(cspmv, upper_inc_b_1_inc_c_2_N_100)
{
    blasint N = DATASIZE, inc_b = 1, inc_c = 2;
    char uplo = 'U';
    float alpha[] = {1.0f, 1.0f};
    float beta[] = {0.0f, 0.0f};

    float norm = check_cspmv(uplo, N, alpha, inc_b, beta, inc_c);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_TOL);
}

/**
 * Test cspmv by comparing it against cgemv
 * with the following options:
 *
 * A is upper triangular
 * Number of rows and columns of A is 100
 * Stride of vector b is 2
 * Stride of vector c is 1
 */
CTEST(cspmv, upper_inc_b_2_inc_c_1_N_100)
{
    blasint N = DATASIZE, inc_b = 2, inc_c = 1;
    char uplo = 'U';
    float alpha[] = {1.0f, 0.0f};
    float beta[] = {1.0f, 0.0f};

    float norm = check_cspmv(uplo, N, alpha, inc_b, beta, inc_c);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_TOL);
}

/**
 * Test cspmv by comparing it against cgemv
 * with the following options:
 *
 * A is upper triangular
 * Number of rows and columns of A is 100
 * Stride of vector b is 2
 * Stride of vector c is 2
 */
CTEST(cspmv, upper_inc_b_2_inc_c_2_N_100)
{
    blasint N = DATASIZE, inc_b = 2, inc_c = 2;
    char uplo = 'U';
    float alpha[] = {2.5, -2.1};
    float beta[] = {0.0f, 1.0f};

    float norm = check_cspmv(uplo, N, alpha, inc_b, beta, inc_c);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_TOL);
}

/**
 * Test cspmv by comparing it against cgemv
 * with the following options:
 *
 * A is lower triangular
 * Number of rows and columns of A is 100
 * Stride of vector b is 1
 * Stride of vector c is 1
 */
CTEST(cspmv, lower_inc_b_1_inc_c_1_N_100)
{
    blasint N = DATASIZE, inc_b = 1, inc_c = 1;
    char uplo = 'L';
    float alpha[] = {1.0f, 1.0f};
    float beta[] = {0.0f, 0.0f};

    float norm = check_cspmv(uplo, N, alpha, inc_b, beta, inc_c);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_TOL);
}

/**
 * Test cspmv by comparing it against cgemv
 * with the following options:
 *
 * A is lower triangular
 * Number of rows and columns of A is 100
 * Stride of vector b is 1
 * Stride of vector c is 2
 */
CTEST(cspmv, lower_inc_b_1_inc_c_2_N_100)
{
    blasint N = DATASIZE, inc_b = 1, inc_c = 2;
    char uplo = 'L';
    float alpha[] = {1.0f, 1.0f};
    float beta[] = {0.0f, 0.0f};

    float norm = check_cspmv(uplo, N, alpha, inc_b, beta, inc_c);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_TOL);
}

/**
 * Test cspmv by comparing it against cgemv
 * with the following options:
 *
 * A is lower triangular
 * Number of rows and columns of A is 100
 * Stride of vector b is 2
 * Stride of vector c is 1
 */
CTEST(cspmv, lower_inc_b_2_inc_c_1_N_100)
{
    blasint N = DATASIZE, inc_b = 2, inc_c = 1;
    char uplo = 'L';
    float alpha[] = {1.0f, 0.0f};
    float beta[] = {1.0f, 0.0f};

    float norm = check_cspmv(uplo, N, alpha, inc_b, beta, inc_c);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_TOL);
}

/**
 * Test cspmv by comparing it against cgemv
 * with the following options:
 *
 * A is lower triangular
 * Number of rows and columns of A is 100
 * Stride of vector b is 2
 * Stride of vector c is 2
 */
CTEST(cspmv, lower_inc_b_2_inc_c_2_N_100)
{
    blasint N = DATASIZE, inc_b = 2, inc_c = 2;
    char uplo = 'L';
    float alpha[] = {2.5, -2.1};
    float beta[] = {0.0f, 1.0f};

    float norm = check_cspmv(uplo, N, alpha, inc_b, beta, inc_c);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_TOL);
}

/**
 * Check if output matrix A contains any NaNs
 */
CTEST(cspmv, check_for_NaN)
{
    blasint N = DATASIZE, inc_b = 1, inc_c = 1;
    char uplo = 'U';
    float alpha[] = {1.0f, 1.0f};
    float beta[] = {0.0f, 0.0f};

    float norm = check_cspmv(uplo, N, alpha, inc_b, beta, inc_c);

    ASSERT_TRUE(norm == norm); /* NaN == NaN is false */
}

/**
 * Test error function for an invalid param uplo.
 * uplo specifies whether A is upper or lower triangular.
 */
CTEST(cspmv, xerbla_uplo_invalid)
{
    blasint N = DATASIZE, inc_b = 1, inc_c = 1;
    char uplo = 'O';
    int expected_info = 1;

    int passed = check_badargs(uplo, N, inc_b, inc_c, expected_info);
    ASSERT_EQUAL(TRUE, passed);
}

/**
 * Test error function for an invalid param N -
 * number of rows and columns of A. Must be at least zero.
 */
CTEST(cspmv, xerbla_N_invalid)
{
    blasint N = INVALID, inc_b = 1, inc_c = 1;
    char uplo = 'U';
    int expected_info = 2;

    int passed = check_badargs(uplo, N, inc_b, inc_c, expected_info);
    ASSERT_EQUAL(TRUE, passed);
}

/**
 * Test error function for an invalid param inc_b -
 * stride of vector b. Can't be zero.
 */
CTEST(cspmv, xerbla_inc_b_zero)
{
    blasint N = DATASIZE, inc_b = 0, inc_c = 1;
    char uplo = 'U';
    int expected_info = 6;

    int passed = check_badargs(uplo, N, inc_b, inc_c, expected_info);
    ASSERT_EQUAL(TRUE, passed);
}

/**
 * Test error function for an invalid param inc_c -
 * stride of vector c. Can't be zero.
 */
CTEST(cspmv, xerbla_inc_c_zero)
{
    blasint N = DATASIZE, inc_b = 1, inc_c = 0;
    char uplo = 'U';
    int expected_info = 9;

    int passed = check_badargs(uplo, N, inc_b, inc_c, expected_info);
    ASSERT_EQUAL(TRUE, passed);
}
#endif
