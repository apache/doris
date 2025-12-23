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

struct DATA_CGEMMT {
    float a_test[DATASIZE * DATASIZE * 2];
    float b_test[DATASIZE * DATASIZE * 2];
    float c_test[DATASIZE * DATASIZE * 2];
    float c_verify[DATASIZE * DATASIZE * 2];
    float c_gemm[DATASIZE * DATASIZE * 2];
};

#ifdef BUILD_COMPLEX
static struct DATA_CGEMMT data_cgemmt;

/**
 * Compute gemmt via gemm since gemmt is gemm but updates only 
 * the upper or lower triangular part of the result matrix
 *
 * param api specifies tested api (C or Fortran)
 * param order specifies row or column major order (for Fortran API column major always)
 * param uplo specifies whether C’s data is stored in its upper or lower triangle
 * param transa specifies op(A), the transposition operation applied to A
 * param transb specifies op(B), the transposition operation applied to B
 * param m - number of rows of op(A), columns of op(B), and columns and rows of C
 * param k - number of columns of op(A) and rows of op(B)
 * param alpha - scaling factor for the matrix-matrix product
 * param lda - leading dimension of A
 * param ldb - leading dimension of B
 * param beta - scaling factor for matrix C
 * param ldc - leading dimension of C
 */
static void cgemmt_trusted(char api, enum CBLAS_ORDER order, char uplo, char transa, 
                           char transb, blasint m, blasint k, float *alpha, blasint lda, 
                           blasint ldb, float *beta, blasint ldc)
{
    blasint i, j;

    if(api == 'F')
        BLASFUNC(cgemm)(&transa, &transb, &m, &m, &k, alpha, data_cgemmt.a_test, &lda,
                        data_cgemmt.b_test, &ldb, beta, data_cgemmt.c_gemm, &ldc);
#ifndef NO_CBLAS
    else
        cblas_cgemm(order, transa, transb, m, m, k, alpha, data_cgemmt.a_test, lda,
                data_cgemmt.b_test, ldb, beta, data_cgemmt.c_gemm, ldc);
#endif

    ldc *= 2;

#ifndef NO_CBLAS
    if (order == CblasRowMajor) {
    if (uplo == 'U' || uplo == CblasUpper)
    {
        for (i = 0; i < m; i++)
            for (j = i * 2; j < m * 2; j+=2){
                data_cgemmt.c_verify[i * ldc + j] =
                    data_cgemmt.c_gemm[i * ldc + j];
                data_cgemmt.c_verify[i * ldc + j + 1] =
                    data_cgemmt.c_gemm[i * ldc + j + 1];
            }
    } else {
        for (i = 0; i < m; i++)
            for (j = 0; j <= i * 2; j+=2){
                data_cgemmt.c_verify[i * ldc + j] =
                    data_cgemmt.c_gemm[i * ldc + j];
                data_cgemmt.c_verify[i * ldc + j + 1] =
                    data_cgemmt.c_gemm[i * ldc + j + 1];
            }
    }
    } else 
#endif
    if (uplo == 'L' || uplo == CblasLower)
    {
        for (i = 0; i < m; i++)
            for (j = i * 2; j < m * 2; j+=2){
                data_cgemmt.c_verify[i * ldc + j] =
                    data_cgemmt.c_gemm[i * ldc + j];
                data_cgemmt.c_verify[i * ldc + j + 1] =
                    data_cgemmt.c_gemm[i * ldc + j + 1];
            }
    } else {
        for (i = 0; i < m; i++)
            for (j = 0; j <= i * 2; j+=2){
                data_cgemmt.c_verify[i * ldc + j] =
                    data_cgemmt.c_gemm[i * ldc + j];
                data_cgemmt.c_verify[i * ldc + j + 1] =
                    data_cgemmt.c_gemm[i * ldc + j + 1];
            }
    }
}

/**
 * Comapare results computed by cgemmt and cgemmt_trusted
 *
 * param api specifies tested api (C or Fortran)
 * param order specifies row or column major order (for Fortran API column major always)
 * param uplo specifies whether C’s data is stored in its upper or lower triangle
 * param transa specifies op(A), the transposition operation applied to A
 * param transb specifies op(B), the transposition operation applied to B
 * param m - number of rows of op(A), columns of op(B), and columns and rows of C
 * param k - number of columns of op(A) and rows of op(B)
 * param alpha - scaling factor for the matrix-matrix product
 * param lda - leading dimension of A
 * param ldb - leading dimension of B
 * param beta - scaling factor for matrix C
 * param ldc - leading dimension of C
 * return norm of differences
 */
static float check_cgemmt(char api, enum CBLAS_ORDER order, char uplo, char transa, 
                          char transb, blasint m, blasint k, float *alpha, blasint lda, 
                          blasint ldb, float *beta, blasint ldc)
{
    blasint i;
    blasint b_cols;
    blasint a_cols;
    blasint inc = 1;
    blasint size_c = m * ldc * 2;

    if(order == CblasColMajor){
        if (transa == 'T' || transa == 'C' || 
            transa == CblasTrans || transa == CblasConjTrans) 
            a_cols = m;
        else a_cols = k;

        if (transb == 'T' || transb == 'C' || 
            transb == CblasTrans || transb == CblasConjTrans) 
            b_cols = k;
        else b_cols = m;
    } else {
        if (transa == 'T' || transa == 'C' || 
            transa == CblasTrans || transa == CblasConjTrans) 
            a_cols = k;
        else a_cols = m;

        if (transb == 'T' || transb == 'C' ||
            transb == CblasTrans || transb == CblasConjTrans) 
            b_cols = m;
        else b_cols = k;
    }

    srand_generate(data_cgemmt.a_test, a_cols * lda * 2);
    srand_generate(data_cgemmt.b_test, b_cols * ldb  * 2);
    srand_generate(data_cgemmt.c_test, m * ldc * 2);

    for (i = 0; i < m * ldc * 2; i++)
        data_cgemmt.c_gemm[i] = data_cgemmt.c_verify[i] = data_cgemmt.c_test[i];

    cgemmt_trusted(api, order, uplo, transa, transb, m, k, alpha, lda, ldb, beta, ldc);

    if (api == 'F')
        BLASFUNC(cgemmt)(&uplo, &transa, &transb, &m, &k, alpha, data_cgemmt.a_test,
                         &lda, data_cgemmt.b_test, &ldb, beta, data_cgemmt.c_test, &ldc);
#ifndef NO_CBLAS
    else
        cblas_cgemmt(order, uplo, transa, transb, m, k, alpha, data_cgemmt.a_test, lda,
                     data_cgemmt.b_test, ldb, beta, data_cgemmt.c_test, ldc);
#endif

    for (i = 0; i < m * ldc * 2; i++)
        data_cgemmt.c_verify[i] -= data_cgemmt.c_test[i];

    return BLASFUNC(snrm2)(&size_c, data_cgemmt.c_verify, &inc) / size_c;
}

/**
 * Check if error function was called with expected function name
 * and param info
 *
 * param uplo specifies whether C’s data is stored in its upper or lower triangle
 * param transa specifies op(A), the transposition operation applied to A
 * param transb specifies op(B), the transposition operation applied to B
 * param m - number of rows of op(A), columns of op(B), and columns and rows of C
 * param k - number of columns of op(A) and rows of op(B)
 * param lda - leading dimension of A
 * param ldb - leading dimension of B
 * param ldc - leading dimension of C
 * param expected_info - expected invalid parameter number in cgemmt
 * return TRUE if everything is ok, otherwise FALSE
 */
static int check_badargs(char api, enum CBLAS_ORDER order, char uplo, char transa, 
                         char transb, blasint m, blasint k, blasint lda, blasint ldb,
                         blasint ldc, int expected_info)
{
    float alpha[] = {1.0f, 1.0f};
    float beta[] = {0.0f, 0.0f};

    set_xerbla("CGEMMT ", expected_info);

    if (api == 'F')
        BLASFUNC(cgemmt)(&uplo, &transa, &transb, &m, &k, alpha, data_cgemmt.a_test,
                         &lda, data_cgemmt.b_test, &ldb, beta, data_cgemmt.c_test, &ldc);
#ifndef NO_CBLAS
    else
        cblas_cgemmt(order, uplo, transa, transb, m, k, alpha, data_cgemmt.a_test, lda,
                     data_cgemmt.b_test, ldb, beta, data_cgemmt.c_test, ldc);
#endif

    return check_error();
}

/**
 * Fortran API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * C’s data is stored in its upper triangle part
 * A not transposed
 * B not transposed
 */
CTEST(cgemmt, upper_M_50_K_50_a_notrans_b_notrans)
{
    blasint M = 50, K = 50;
    blasint lda = 50, ldb = 50, ldc = 50;
    char transa = 'N', transb = 'N';
    char uplo = 'U';
    float alpha[] = {-1.0f, 1.0f};
    float beta[] = {2.0f, 1.0f};

    float norm = check_cgemmt('F', CblasColMajor, uplo, transa, transb,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * Fortran API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * C’s data is stored in its upper triangle part
 * A transposed
 * B not transposed
 */
CTEST(cgemmt, upper_M_50_K_25_a_trans_b_notrans)
{
    blasint M = 50, K = 25;
    blasint lda = 25, ldb = 25, ldc = 50;
    char transa = 'T', transb = 'N';
    char uplo = 'U';
    float alpha[] = {1.0f, 1.0f};
    float beta[] = {2.0f, 1.0f};

    float norm = check_cgemmt('F', CblasColMajor, uplo, transa, transb,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * Fortran API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * C’s data is stored in its upper triangle part
 * A not transposed
 * B transposed
 */
CTEST(cgemmt, upper_M_25_K_50_a_notrans_b_trans)
{
    blasint M = 25, K = 50;
    blasint lda = 25, ldb = 25, ldc = 25;
    char transa = 'N', transb = 'T';
    char uplo = 'U';
    float alpha[] = {1.0f, 1.0f};
    float beta[] = {0.0f, 0.0f};

    float norm = check_cgemmt('F', CblasColMajor, uplo, transa, transb,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * Fortran API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * C’s data is stored in its upper triangle part
 * A transposed
 * B transposed
 */
CTEST(cgemmt, upper_M_50_K_50_a_trans_b_trans)
{
    blasint M = 50, K = 50;
    blasint lda = 50, ldb = 50, ldc = 50;
    char transa = 'T', transb = 'T';
    char uplo = 'U';
    float alpha[] = {-1.0f, 1.0f};
    float beta[] = {2.0f, 1.0f};

    float norm = check_cgemmt('F', CblasColMajor, uplo, transa, transb,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * Fortran API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * C’s data is stored in its upper triangle part
 * A conjugate not transposed
 * B conjugate not transposed
 */
CTEST(cgemmt, upper_M_25_K_50_a_conjnotrans_b_conjnotrans)
{
    blasint M = 25, K = 50;
    blasint lda = 25, ldb = 50, ldc = 25;
    char transa = 'R', transb = 'R';
    char uplo = 'U';
    float alpha[] = {2.0f, 1.0f};
    float beta[] = {-1.0f, 2.0f};

    float norm = check_cgemmt('F', CblasColMajor, uplo, transa, transb,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * Fortran API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * C’s data is stored in its upper triangle part
 * A conjugate transposed
 * B conjugate not transposed
 */
CTEST(cgemmt, upper_M_50_K_50_a_conjtrans_b_conjnotrans)
{
    blasint M = 50, K = 50;
    blasint lda = 50, ldb = 50, ldc = 50;
    char transa = 'C', transb = 'R';
    char uplo = 'U';
    float alpha[] = {1.0f, 1.0f};
    float beta[] = {1.0f, 1.0f};

    float norm = check_cgemmt('F', CblasColMajor, uplo, transa, transb,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * Fortran API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * C’s data is stored in its upper triangle part
 * A conjugate not transposed
 * B conjugate transposed
 */
CTEST(cgemmt, upper_M_50_K_50_a_conjnotrans_b_conjtrans)
{
    blasint M = 50, K = 50;
    blasint lda = 50, ldb = 50, ldc = 50;
    char transa = 'R', transb = 'C';
    char uplo = 'U';
    float alpha[] = {2.0f, 1.0f};
    float beta[] = {-1.0f, 2.0f};

    float norm = check_cgemmt('F', CblasColMajor, uplo, transa, transb,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * Fortran API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * C’s data is stored in its upper triangle part
 * A conjugate transposed
 * B conjugate transposed
 */
CTEST(cgemmt, upper_M_50_K_25_a_conjtrans_b_conjtrans)
{
    blasint M = 50, K = 25;
    blasint lda = 50, ldb = 50, ldc = 50;
    char transa = 'C', transb = 'C';
    char uplo = 'U';
    float alpha[] = {2.0f, 1.0f};
    float beta[] = {-1.0f, 2.0f};

    float norm = check_cgemmt('F', CblasColMajor, uplo, transa, transb,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * Fortran API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * C’s data is stored in its upper triangle part
 * alpha_r = 0.0, alpha_i = 0.0
 */
CTEST(cgemmt, upper_alpha_zero)
{
    blasint M = 50, K = 50;
    blasint lda = 50, ldb = 50, ldc = 50;
    char transa = 'N', transb = 'N';
    char uplo = 'U';
    float alpha[] = {0.0f, 0.0f};
    float beta[] = {2.0f, 1.0f};

    float norm = check_cgemmt('F', CblasColMajor, uplo, transa, transb,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * Fortran API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * C’s data is stored in its upper triangle part
 * beta_r = 1.0, beta_i = 0.0
 */
CTEST(cgemmt, upper_beta_one)
{
    blasint M = 50, K = 50;
    blasint lda = 50, ldb = 50, ldc = 50;
    char transa = 'N', transb = 'N';
    char uplo = 'U';
    float alpha[] = {1.0f, 1.0f};
    float beta[] = {1.0f, 0.0f};

    float norm = check_cgemmt('F', CblasColMajor, uplo, transa, transb,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * Fortran API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * C’s data is stored in its lower triangle part
 * A not transposed
 * B not transposed
 */
CTEST(cgemmt, lower_M_50_K_50_a_notrans_b_notrans)
{
    blasint M = 50, K = 50;
    blasint lda = 50, ldb = 50, ldc = 50;
    char transa = 'N', transb = 'N';
    char uplo = 'L';
    float alpha[] = {-1.0f, 1.0f};
    float beta[] = {2.0f, 1.0f};

    float norm = check_cgemmt('F', CblasColMajor, uplo, transa, transb,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * Fortran API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * C’s data is stored in its lower triangle part
 * A transposed
 * B not transposed
 */
CTEST(cgemmt, lower_M_50_K_25_a_trans_b_notrans)
{
    blasint M = 50, K = 25;
    blasint lda = 25, ldb = 25, ldc = 50;
    char transa = 'T', transb = 'N';
    char uplo = 'L';
    float alpha[] = {1.0f, 1.0f};
    float beta[] = {2.0f, 1.0f};

    float norm = check_cgemmt('F', CblasColMajor, uplo, transa, transb,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * Fortran API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * C’s data is stored in its lower triangle part
 * A not transposed
 * B transposed
 */
CTEST(cgemmt, lower_M_25_K_50_a_notrans_b_trans)
{
    blasint M = 25, K = 50;
    blasint lda = 25, ldb = 25, ldc = 25;
    char transa = 'N', transb = 'T';
    char uplo = 'L';
    float alpha[] = {1.0f, 1.0f};
    float beta[] = {0.0f, 0.0f};

    float norm = check_cgemmt('F', CblasColMajor, uplo, transa, transb,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * Fortran API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * C’s data is stored in its lower triangle part
 * A transposed
 * B transposed
 */
CTEST(cgemmt, lower_M_50_K_50_a_trans_b_trans)
{
    blasint M = 50, K = 50;
    blasint lda = 50, ldb = 50, ldc = 50;
    char transa = 'T', transb = 'T';
    char uplo = 'L';
    float alpha[] = {-1.0f, 1.0f};
    float beta[] = {2.0f, 1.0f};

    float norm = check_cgemmt('F', CblasColMajor, uplo, transa, transb,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * Fortran API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * C’s data is stored in its lower triangle part
 * A conjugate not transposed
 * B conjugate not transposed
 */
CTEST(cgemmt, lower_M_25_K_50_a_conjnotrans_b_conjnotrans)
{
    blasint M = 25, K = 50;
    blasint lda = 25, ldb = 50, ldc = 25;
    char transa = 'R', transb = 'R';
    char uplo = 'L';
    float alpha[] = {2.0f, 1.0f};
    float beta[] = {-1.0f, 2.0f};

    float norm = check_cgemmt('F', CblasColMajor, uplo, transa, transb,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * Fortran API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * C’s data is stored in its lower triangle part
 * A conjugate transposed
 * B conjugate not transposed
 */
CTEST(cgemmt, lower_M_50_K_50_a_conjtrans_b_conjnotrans)
{
    blasint M = 50, K = 50;
    blasint lda = 50, ldb = 50, ldc = 50;
    char transa = 'C', transb = 'R';
    char uplo = 'L';
    float alpha[] = {1.0f, 1.0f};
    float beta[] = {1.0f, 1.0f};

    float norm = check_cgemmt('F', CblasColMajor, uplo, transa, transb,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * Fortran API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * C’s data is stored in its lower triangle part
 * A conjugate not transposed
 * B conjugate transposed
 */
CTEST(cgemmt, lower_M_50_K_50_a_conjnotrans_b_conjtrans)
{
    blasint M = 50, K = 50;
    blasint lda = 50, ldb = 50, ldc = 50;
    char transa = 'R', transb = 'C';
    char uplo = 'L';
    float alpha[] = {2.0f, 1.0f};
    float beta[] = {-1.0f, 2.0f};

    float norm = check_cgemmt('F', CblasColMajor, uplo, transa, transb,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * Fortran API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * C’s data is stored in its lower triangle part
 * A conjugate transposed
 * B conjugate transposed
 */
CTEST(cgemmt, lower_M_50_K_25_a_conjtrans_b_conjtrans)
{
    blasint M = 50, K = 25;
    blasint lda = 50, ldb = 50, ldc = 50;
    char transa = 'C', transb = 'C';
    char uplo = 'L';
    float alpha[] = {2.0f, 1.0f};
    float beta[] = {-1.0f, 2.0f};

    float norm = check_cgemmt('F', CblasColMajor, uplo, transa, transb,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * Fortran API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * C’s data is stored in its lower triangle part
 * alpha_r = 0.0, alpha_i = 0.0
 */
CTEST(cgemmt, lower_alpha_zero)
{
    blasint M = 50, K = 50;
    blasint lda = 50, ldb = 50, ldc = 50;
    char transa = 'N', transb = 'N';
    char uplo = 'L';
    float alpha[] = {0.0f, 0.0f};
    float beta[] = {2.0f, 1.0f};

    float norm = check_cgemmt('F', CblasColMajor, uplo, transa, transb,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * Fortran API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * C’s data is stored in its lower triangle part
 * beta_r = 1.0, beta_i = 0.0
 */
CTEST(cgemmt, lower_beta_one)
{
    blasint M = 50, K = 50;
    blasint lda = 50, ldb = 50, ldc = 50;
    char transa = 'N', transb = 'N';
    char uplo = 'L';
    float alpha[] = {1.0f, 1.0f};
    float beta[] = {1.0f, 0.0f};

    float norm = check_cgemmt('F', CblasColMajor, uplo, transa, transb,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

#ifndef NO_CBLAS
/**
 * C API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * Column Major
 * C’s data is stored in its upper triangle part
 * A not transposed
 * B not transposed
 */
CTEST(cgemmt, c_api_colmajor_upper_M_50_K_50_a_notrans_b_notrans)
{
    blasint M = 50, K = 50;
    blasint lda = 50, ldb = 50, ldc = 50;
    float alpha[] = {-1.0f, 1.0f};
    float beta[] = {2.0f, 1.0f};

    float norm = check_cgemmt('C', CblasColMajor, CblasUpper, CblasNoTrans, CblasNoTrans,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * C API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * Column Major
 * C’s data is stored in its upper triangle part
 * A transposed
 * B not transposed
 */
CTEST(cgemmt, c_api_colmajor_upper_M_50_K_25_a_trans_b_notrans)
{
    blasint M = 50, K = 25;
    blasint lda = 25, ldb = 25, ldc = 50;
    float alpha[] = {1.0f, 1.0f};
    float beta[] = {2.0f, 1.0f};

    float norm = check_cgemmt('C', CblasColMajor, CblasUpper, CblasTrans, CblasNoTrans,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * C API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * Column Major
 * C’s data is stored in its upper triangle part
 * A not transposed
 * B transposed
 */
CTEST(cgemmt, c_api_colmajor_upper_M_25_K_50_a_notrans_b_trans)
{
    blasint M = 25, K = 50;
    blasint lda = 25, ldb = 25, ldc = 25;
    float alpha[] = {1.0f, 1.0f};
    float beta[] = {0.0f, 0.0f};

    float norm = check_cgemmt('C', CblasColMajor, CblasUpper, CblasNoTrans, CblasTrans,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * C API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * Column Major
 * C’s data is stored in its upper triangle part
 * A transposed
 * B transposed
 */
CTEST(cgemmt, c_api_colmajor_upper_M_50_K_50_a_trans_b_trans)
{
    blasint M = 50, K = 50;
    blasint lda = 50, ldb = 50, ldc = 50;
    float alpha[] = {-1.0f, 1.0f};
    float beta[] = {2.0f, 1.0f};

    float norm = check_cgemmt('C', CblasColMajor, CblasUpper, CblasTrans, CblasTrans,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * C API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * Column Major
 * C’s data is stored in its upper triangle part
 * A conjugate not transposed
 * B conjugate not transposed
 */
CTEST(cgemmt, c_api_colmajor_upper_M_25_K_50_a_conjnotrans_b_conjnotrans)
{
    blasint M = 25, K = 50;
    blasint lda = 25, ldb = 50, ldc = 25;
    float alpha[] = {2.0f, 1.0f};
    float beta[] = {-1.0f, 2.0f};

    float norm = check_cgemmt('C', CblasColMajor, CblasUpper, CblasConjNoTrans, CblasConjNoTrans,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * C API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * Column Major
 * C’s data is stored in its upper triangle part
 * A conjugate transposed
 * B conjugate not transposed
 */
CTEST(cgemmt, c_api_colmajor_upper_M_50_K_50_a_conjtrans_b_conjnotrans)
{
    blasint M = 50, K = 50;
    blasint lda = 50, ldb = 50, ldc = 50;
    float alpha[] = {1.0f, 1.0f};
    float beta[] = {1.0f, 1.0f};

    float norm = check_cgemmt('C', CblasColMajor, CblasUpper, CblasConjTrans, CblasConjNoTrans,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * C API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * Column Major
 * C’s data is stored in its upper triangle part
 * A conjugate not transposed
 * B conjugate transposed
 */
CTEST(cgemmt, c_api_colmajor_upper_M_50_K_50_a_conjnotrans_b_conjtrans)
{
    blasint M = 50, K = 50;
    blasint lda = 50, ldb = 50, ldc = 50;
    float alpha[] = {2.0f, 1.0f};
    float beta[] = {-1.0f, 2.0f};

    float norm = check_cgemmt('C', CblasColMajor, CblasUpper, CblasConjNoTrans, CblasConjTrans,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * C API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * Column Major
 * C’s data is stored in its upper triangle part
 * A conjugate transposed
 * B conjugate transposed
 */
CTEST(cgemmt, c_api_colmajor_upper_M_50_K_25_a_conjtrans_b_conjtrans)
{
    blasint M = 50, K = 25;
    blasint lda = 50, ldb = 50, ldc = 50;
    float alpha[] = {2.0f, 1.0f};
    float beta[] = {-1.0f, 2.0f};

    float norm = check_cgemmt('C', CblasColMajor, CblasUpper, CblasConjTrans, CblasConjTrans,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * C API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * Column Major
 * C’s data is stored in its upper triangle part
 * alpha_r = 0.0, alpha_i = 0.0
 */
CTEST(cgemmt, c_api_colmajor_upper_alpha_zero)
{
    blasint M = 50, K = 50;
    blasint lda = 50, ldb = 50, ldc = 50;
    float alpha[] = {0.0f, 0.0f};
    float beta[] = {2.0f, 1.0f};

    float norm = check_cgemmt('C', CblasColMajor, CblasUpper, CblasNoTrans, CblasNoTrans,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * C API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * Column Major
 * C’s data is stored in its upper triangle part
 * beta_r = 1.0, beta_i = 0.0
 */
CTEST(cgemmt, c_api_colmajor_upper_beta_one)
{
    blasint M = 50, K = 50;
    blasint lda = 50, ldb = 50, ldc = 50;
    float alpha[] = {1.0f, 1.0f};
    float beta[] = {1.0f, 0.0f};

    float norm = check_cgemmt('C', CblasColMajor, CblasUpper, CblasNoTrans, CblasNoTrans,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * C API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * Column Major
 * C’s data is stored in its lower triangle part
 * A not transposed
 * B not transposed
 */
CTEST(cgemmt, c_api_colmajor_lower_M_50_K_50_a_notrans_b_notrans)
{
    blasint M = 50, K = 50;
    blasint lda = 50, ldb = 50, ldc = 50;
    float alpha[] = {-1.0f, 1.0f};
    float beta[] = {2.0f, 1.0f};

    float norm = check_cgemmt('C', CblasColMajor, CblasLower, CblasNoTrans, CblasNoTrans,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * C API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * Column Major
 * C’s data is stored in its lower triangle part
 * A transposed
 * B not transposed
 */
CTEST(cgemmt, c_api_colmajor_lower_M_50_K_25_a_trans_b_notrans)
{
    blasint M = 50, K = 25;
    blasint lda = 25, ldb = 25, ldc = 50;
    float alpha[] = {1.0f, 1.0f};
    float beta[] = {2.0f, 1.0f};

    float norm = check_cgemmt('C', CblasColMajor, CblasLower, CblasTrans, CblasNoTrans,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * C API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * Column Major
 * C’s data is stored in its lower triangle part
 * A not transposed
 * B transposed
 */
CTEST(cgemmt, c_api_colmajor_lower_M_25_K_50_a_notrans_b_trans)
{
    blasint M = 25, K = 50;
    blasint lda = 25, ldb = 25, ldc = 25;
    float alpha[] = {1.0f, 1.0f};
    float beta[] = {0.0f, 0.0f};

    float norm = check_cgemmt('C', CblasColMajor, CblasLower, CblasNoTrans, CblasTrans,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * C API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * Column Major
 * C’s data is stored in its lower triangle part
 * A transposed
 * B transposed
 */
CTEST(cgemmt, c_api_colmajor_lower_M_50_K_50_a_trans_b_trans)
{
    blasint M = 50, K = 50;
    blasint lda = 50, ldb = 50, ldc = 50;
    float alpha[] = {-1.0f, 1.0f};
    float beta[] = {2.0f, 1.0f};

    float norm = check_cgemmt('C', CblasColMajor, CblasLower, CblasTrans, CblasTrans,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * C API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * Column Major
 * C’s data is stored in its lower triangle part
 * A conjugate not transposed
 * B conjugate not transposed
 */
CTEST(cgemmt, c_api_colmajor_lower_M_25_K_50_a_conjnotrans_b_conjnotrans)
{
    blasint M = 25, K = 50;
    blasint lda = 25, ldb = 50, ldc = 25;
    float alpha[] = {2.0f, 1.0f};
    float beta[] = {-1.0f, 2.0f};

    float norm = check_cgemmt('C', CblasColMajor, CblasLower, CblasConjNoTrans, CblasConjNoTrans,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * C API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * Column Major
 * C’s data is stored in its lower triangle part
 * A conjugate transposed
 * B conjugate not transposed
 */
CTEST(cgemmt, c_api_colmajor_lower_M_50_K_50_a_conjtrans_b_conjnotrans)
{
    blasint M = 50, K = 50;
    blasint lda = 50, ldb = 50, ldc = 50;
    float alpha[] = {2.0f, 1.0f};
    float beta[] = {-1.0f, 2.0f};

    float norm = check_cgemmt('C', CblasColMajor, CblasLower, CblasConjTrans, CblasConjNoTrans,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * C API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * Column Major
 * C’s data is stored in its lower triangle part
 * A conjugate not transposed
 * B conjugate transposed
 */
CTEST(cgemmt, c_api_colmajor_lower_M_50_K_50_a_conjnotrans_b_conjtrans)
{
    blasint M = 50, K = 50;
    blasint lda = 50, ldb = 50, ldc = 50;
    float alpha[] = {2.0f, 1.0f};
    float beta[] = {-1.0f, 2.0f};

    float norm = check_cgemmt('C', CblasColMajor, CblasLower, CblasConjNoTrans, CblasConjTrans,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * C API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * Column Major
 * C’s data is stored in its lower triangle part
 * A conjugate transposed
 * B conjugate transposed
 */
CTEST(cgemmt, c_api_colmajor_lower_M_50_K_25_a_conjtrans_b_conjtrans)
{
    blasint M = 50, K = 25;
    blasint lda = 50, ldb = 50, ldc = 50;
    float alpha[] = {2.0f, 1.0f};
    float beta[] = {-1.0f, 2.0f};

    float norm = check_cgemmt('C', CblasColMajor, CblasLower, CblasConjTrans, CblasConjTrans,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * C API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * Column Major
 * C’s data is stored in its lower triangle part
 * alpha_r = 0.0, alpha_i = 0.0
 */
CTEST(cgemmt, c_api_colmajor_lower_alpha_zero)
{
    blasint M = 50, K = 50;
    blasint lda = 50, ldb = 50, ldc = 50;
    float alpha[] = {0.0f, 0.0f};
    float beta[] = {2.0f, 1.0f};

    float norm = check_cgemmt('C', CblasColMajor, CblasLower, CblasNoTrans, CblasNoTrans,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * C API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * Column Major
 * C’s data is stored in its lower triangle part
 * beta_r = 1.0, beta_i = 0.0
 */
CTEST(cgemmt, c_api_colmajor_lower_beta_one)
{
    blasint M = 50, K = 50;
    blasint lda = 50, ldb = 50, ldc = 50;
    float alpha[] = {1.0f, 1.0f};
    float beta[] = {1.0f, 0.0f};

    float norm = check_cgemmt('C', CblasColMajor, CblasLower, CblasNoTrans, CblasNoTrans,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * C API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * Row Major
 * C’s data is stored in its upper triangle part
 * A not transposed
 * B not transposed
 */
CTEST(cgemmt, c_api_rowmajor_upper_M_50_K_50_a_notrans_b_notrans)
{
    blasint M = 50, K = 50;
    blasint lda = 50, ldb = 50, ldc = 50;
    float alpha[] = {-1.0f, 1.0f};
    float beta[] = {2.0f, 1.0f};

    float norm = check_cgemmt('C', CblasRowMajor, CblasUpper, CblasNoTrans, CblasNoTrans,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * C API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * Row Major
 * C’s data is stored in its upper triangle part
 * A transposed
 * B not transposed
 */
CTEST(cgemmt, c_api_rowmajor_upper_M_50_K_25_a_trans_b_notrans)
{
    blasint M = 50, K = 25;
    blasint lda = 50, ldb = 50, ldc = 50;
    float alpha[] = {1.0f, 1.0f};
    float beta[] = {2.0f, 1.0f};

    float norm = check_cgemmt('C', CblasRowMajor, CblasUpper, CblasTrans, CblasNoTrans,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * C API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * Row Major
 * C’s data is stored in its upper triangle part
 * A not transposed
 * B transposed
 */
CTEST(cgemmt, c_api_rowmajor_upper_M_25_K_50_a_notrans_b_trans)
{
    blasint M = 25, K = 50;
    blasint lda = 50, ldb = 50, ldc = 25;
    float alpha[] = {1.0f, 1.0f};
    float beta[] = {-1.0f, -1.0f};

    float norm = check_cgemmt('C', CblasRowMajor, CblasUpper, CblasNoTrans, CblasTrans,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * C API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * Row Major
 * C’s data is stored in its upper triangle part
 * A transposed
 * B transposed
 */
CTEST(cgemmt, c_api_rowmajor_upper_M_50_K_50_a_trans_b_trans)
{
    blasint M = 50, K = 50;
    blasint lda = 50, ldb = 50, ldc = 50;
    float alpha[] = {-1.0f, 1.0f};
    float beta[] = {2.0f, 1.0f};

    float norm = check_cgemmt('C', CblasRowMajor, CblasUpper, CblasTrans, CblasTrans,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * C API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * Row Major
 * C’s data is stored in its upper triangle part
 * A conjugate not transposed
 * B conjugate not transposed
 */
CTEST(cgemmt, c_api_rowmajor_upper_M_25_K_50_a_conjnotrans_b_conjnotrans)
{
    blasint M = 25, K = 50;
    blasint lda = 50, ldb = 25, ldc = 25;
    float alpha[] = {2.0f, 1.0f};
    float beta[] = {-1.0f, 2.0f};

    float norm = check_cgemmt('C', CblasRowMajor, CblasUpper, CblasConjNoTrans, CblasConjNoTrans,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * C API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * Row Major
 * C’s data is stored in its upper triangle part
 * A conjugate transposed
 * B conjugate not transposed
 */
CTEST(cgemmt, c_api_rowmajor_upper_M_50_K_50_a_conjtrans_b_conjnotrans)
{
    blasint M = 50, K = 50;
    blasint lda = 50, ldb = 50, ldc = 50;
    float alpha[] = {2.0f, 1.0f};
    float beta[] = {-1.0f, 2.0f};

    float norm = check_cgemmt('C', CblasRowMajor, CblasUpper, CblasConjTrans, CblasConjNoTrans,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * C API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * Row Major
 * C’s data is stored in its upper triangle part
 * A conjugate not transposed
 * B conjugate transposed
 */
CTEST(cgemmt, c_api_rowmajor_upper_M_50_K_50_a_conjnotrans_b_conjtrans)
{
    blasint M = 50, K = 50;
    blasint lda = 50, ldb = 50, ldc = 50;
    float alpha[] = {2.0f, 1.0f};
    float beta[] = {-1.0f, 2.0f};

    float norm = check_cgemmt('C', CblasRowMajor, CblasUpper, CblasConjNoTrans, CblasConjTrans,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * C API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * Row Major
 * C’s data is stored in its upper triangle part
 * A conjugate transposed
 * B conjugate transposed
 */
CTEST(cgemmt, c_api_rowmajor_upper_M_25_K_50_a_conjtrans_b_conjtrans)
{
    blasint M = 25, K = 50;
    blasint lda = 25, ldb = 50, ldc = 25;
    float alpha[] = {2.0f, 1.0f};
    float beta[] = {-1.0f, 2.0f};

    float norm = check_cgemmt('C', CblasRowMajor, CblasUpper, CblasConjTrans, CblasConjTrans,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * C API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * Row Major
 * C’s data is stored in its upper triangle part
 * alpha_r = 0.0, alpha_i = 0.0
 */
CTEST(cgemmt, c_api_rowmajor_upper_alpha_zero)
{
    blasint M = 50, K = 50;
    blasint lda = 50, ldb = 50, ldc = 50;
    float alpha[] = {0.0f, 0.0f};
    float beta[] = {2.0f, 1.0f};

    float norm = check_cgemmt('C', CblasRowMajor, CblasUpper, CblasNoTrans, CblasNoTrans,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * C API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * Row Major
 * C’s data is stored in its upper triangle part
 * beta_r = 1.0, beta_i = 0.0
 */
CTEST(cgemmt, c_api_rowmajor_upper_beta_one)
{
    blasint M = 50, K = 50;
    blasint lda = 50, ldb = 50, ldc = 50;
    float alpha[] = {1.0f, 1.0f};
    float beta[] = {1.0f, 0.0f};

    float norm = check_cgemmt('C', CblasRowMajor, CblasUpper, CblasNoTrans, CblasNoTrans,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * C API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * Row Major
 * C’s data is stored in its lower triangle part
 * A not transposed
 * B not transposed
 */
CTEST(cgemmt, c_api_rowmajor_lower_M_50_K_50_a_notrans_b_notrans)
{
    blasint M = 50, K = 50;
    blasint lda = 50, ldb = 50, ldc = 50;
    float alpha[] = {-1.0f, 1.0f};
    float beta[] = {2.0f, 1.0f};

    float norm = check_cgemmt('C', CblasRowMajor, CblasLower, CblasNoTrans, CblasNoTrans,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * C API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * Row Major
 * C’s data is stored in its lower triangle part
 * A transposed
 * B not transposed
 */
CTEST(cgemmt, c_api_rowmajor_lower_M_50_K_25_a_trans_b_notrans)
{
    blasint M = 50, K = 25;
    blasint lda = 50, ldb = 50, ldc = 50;
    float alpha[] = {1.0f, 1.0f};
    float beta[] = {2.0f, 1.0f};

    float norm = check_cgemmt('C', CblasRowMajor, CblasLower, CblasTrans, CblasNoTrans,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * C API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * Row Major
 * C’s data is stored in its lower triangle part
 * A not transposed
 * B transposed
 */
CTEST(cgemmt, c_api_rowmajor_lower_M_25_K_50_a_notrans_b_trans)
{
    blasint M = 25, K = 50;
    blasint lda = 50, ldb = 50, ldc = 25;
    float alpha[] = {1.0f, 1.0f};
    float beta[] = {0.0f, 0.0f};

    float norm = check_cgemmt('C', CblasRowMajor, CblasLower, CblasNoTrans, CblasTrans,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * C API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * Row Major
 * C’s data is stored in its lower triangle part
 * A transposed
 * B transposed
 */
CTEST(cgemmt, c_api_rowmajor_lower_M_50_K_50_a_trans_b_trans)
{
    blasint M = 50, K = 50;
    blasint lda = 50, ldb = 50, ldc = 50;
    float alpha[] = {-1.0f, 1.0f};
    float beta[] = {2.0f, 1.0f};

    float norm = check_cgemmt('C', CblasRowMajor, CblasLower, CblasTrans, CblasTrans,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * C API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * Row Major
 * C’s data is stored in its lower triangle part
 * A conjugate not transposed
 * B conjugate not transposed
 */
CTEST(cgemmt, c_api_rowmajor_lower_M_25_K_50_a_conjnotrans_b_conjnotrans)
{
    blasint M = 25, K = 50;
    blasint lda = 50, ldb = 25, ldc = 25;
    float alpha[] = {2.0f, 1.0f};
    float beta[] = {-1.0f, 2.0f};

    float norm = check_cgemmt('C', CblasRowMajor, CblasLower, CblasConjNoTrans, CblasConjNoTrans,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * C API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * Row Major
 * C’s data is stored in its lower triangle part
 * A conjugate transposed
 * B conjugate not transposed
 */
CTEST(cgemmt, c_api_rowmajor_lower_M_50_K_50_a_conjtrans_b_conjnotrans)
{
    blasint M = 50, K = 50;
    blasint lda = 50, ldb = 50, ldc = 50;
    float alpha[] = {2.0f, 1.0f};
    float beta[] = {-1.0f, 2.0f};

    float norm = check_cgemmt('C', CblasRowMajor, CblasLower, CblasConjTrans, CblasConjNoTrans,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * C API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * Row Major
 * C’s data is stored in its lower triangle part
 * A conjugate not transposed
 * B conjugate transposed
 */
CTEST(cgemmt, c_api_rowmajor_lower_M_50_K_50_a_conjnotrans_b_conjtrans)
{
    blasint M = 50, K = 50;
    blasint lda = 50, ldb = 50, ldc = 50;
    float alpha[] = {2.0f, 1.0f};
    float beta[] = {-1.0f, 2.0f};

    float norm = check_cgemmt('C', CblasRowMajor, CblasLower, CblasConjNoTrans, CblasConjTrans,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * C API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * Row Major
 * C’s data is stored in its lower triangle part
 * A conjugate transposed
 * B conjugate transposed
 */
CTEST(cgemmt, c_api_rowmajor_lower_M_50_K_25_a_conjtrans_b_conjtrans)
{
    blasint M = 50, K = 25;
    blasint lda = 50, ldb = 25, ldc = 50;
    float alpha[] = {2.0f, 1.0f};
    float beta[] = {-1.0f, 2.0f};

    float norm = check_cgemmt('C', CblasRowMajor, CblasLower, CblasConjTrans, CblasConjTrans,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * C API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * Row Major
 * C’s data is stored in its lower triangle part
 * alpha_r = 0.0, alpha_i = 0.0
 */
CTEST(cgemmt, c_api_rowmajor_lower_alpha_zero)
{
    blasint M = 50, K = 50;
    blasint lda = 50, ldb = 50, ldc = 50;
    float alpha[] = {0.0f, 0.0f};
    float beta[] = {2.0f, 1.0f};

    float norm = check_cgemmt('C', CblasRowMajor, CblasLower, CblasNoTrans, CblasNoTrans,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * C API specific test
 * Test cgemmt by comparing it against sgemm
 * with the following options:
 *
 * Row Major
 * C’s data is stored in its lower triangle part
 * beta_r = 1.0, beta_i = 0.0
 */
CTEST(cgemmt, c_api_rowmajor_lower_beta_one)
{
    blasint M = 50, K = 50;
    blasint lda = 50, ldb = 50, ldc = 50;
    float alpha[] = {1.0f, 1.0f};
    float beta[] = {1.0f, 0.0f};

    float norm = check_cgemmt('C', CblasRowMajor, CblasLower, CblasNoTrans, CblasNoTrans,
                              M, K, alpha, lda, ldb, beta, ldc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}
#endif

/**
 * Fortran API specific test
 * Test error function for an invalid param uplo.
 * Must be upper (U) or lower (L).
 */
CTEST(cgemmt, xerbla_uplo_invalid)
{
    blasint M = 50, K = 50;
    blasint lda = 50, ldb = 50, ldc = 50;
    char transa = 'N', transb = 'N';
    char uplo = 'O';
    int expected_info = 1;

    int passed = check_badargs('F', CblasColMajor, uplo, transa, transb,
                            M, K, lda, ldb, ldc, expected_info);
    ASSERT_EQUAL(TRUE, passed);
}

/**
 * Fortran API specific test
 * Test error function for an invalid param transa.
 * Must be trans (T/C) or no-trans (N/R).
 */
CTEST(cgemmt, xerbla_transa_invalid)
{
    blasint M = 50, K = 50;
    blasint lda = 50, ldb = 50, ldc = 50;
    char transa = 'O', transb = 'N';
    char uplo = 'U';
    int expected_info = 2;

    int passed = check_badargs('F', CblasColMajor, uplo, transa, transb,
                            M, K, lda, ldb, ldc, expected_info);
    ASSERT_EQUAL(TRUE, passed);
}

/**
 * Fortran API specific test
 * Test error function for an invalid param transb.
 * Must be trans (T/C) or no-trans (N/R).
 */
CTEST(cgemmt, xerbla_transb_invalid)
{
    blasint M = 50, K = 50;
    blasint lda = 50, ldb = 50, ldc = 50;
    char transa = 'N', transb = 'O';
    char uplo = 'U';
    int expected_info = 3;

    int passed = check_badargs('F', CblasColMajor, uplo, transa, transb,
                            M, K, lda, ldb, ldc, expected_info);
    ASSERT_EQUAL(TRUE, passed);
}

/**
 * Fortran API specific test
 * Test error function for an invalid param M.
 * Must be positive.
 */
CTEST(cgemmt, xerbla_m_invalid)
{
    blasint M = -1, K = 50;
    blasint lda = 50, ldb = 50, ldc = 50;
    char transa = 'N', transb = 'N';
    char uplo = 'U';
    int expected_info = 4;

    int passed = check_badargs('F', CblasColMajor, uplo, transa, transb,
                            M, K, lda, ldb, ldc, expected_info);
    ASSERT_EQUAL(TRUE, passed);
}

/**
 * Fortran API specific test
 * Test error function for an invalid param K.
 * Must be positive.
 */
CTEST(cgemmt, xerbla_k_invalid)
{
    blasint M = 50, K = -1;
    blasint lda = 50, ldb = 50, ldc = 50;
    char transa = 'N', transb = 'N';
    char uplo = 'U';
    int expected_info = 5;

    int passed = check_badargs('F', CblasColMajor, uplo, transa, transb,
                            M, K, lda, ldb, ldc, expected_info);
    ASSERT_EQUAL(TRUE, passed);
}

/**
 * Fortran API specific test
 * Test error function for an invalid param lda.
 * Must be must be at least K if matrix A transposed.
 */
CTEST(cgemmt, xerbla_lda_invalid)
{
    blasint M = 50, K = 100;
    blasint lda = 50, ldb = 100, ldc = 50;
    char transa = 'T', transb = 'N';
    char uplo = 'U';
    int expected_info = 8;

    int passed = check_badargs('F', CblasColMajor, uplo, transa, transb,
                            M, K, lda, ldb, ldc, expected_info);
    ASSERT_EQUAL(TRUE, passed);
}

/**
 * Fortran API specific test
 * Test error function for an invalid param ldb.
 * Must be must be at least K if matrix B not transposed.
 */
CTEST(cgemmt, xerbla_ldb_invalid)
{
    blasint M = 50, K = 100;
    blasint lda = 50, ldb = 50, ldc = 50;
    char transa = 'N', transb = 'N';
    char uplo = 'U';
    int expected_info = 10;

    int passed = check_badargs('F', CblasColMajor, uplo, transa, transb,
                            M, K, lda, ldb, ldc, expected_info);
    ASSERT_EQUAL(TRUE, passed);
}

/**
 * Fortran API specific test
 * Test error function for an invalid param ldc.
 * Must be must be at least M.
 */
CTEST(cgemmt, xerbla_ldc_invalid)
{
    blasint M = 100, K = 50;
    blasint lda = 50, ldb = 50, ldc = 50;
    char transa = 'T', transb = 'N';
    char uplo = 'U';
    int expected_info = 13;

    int passed = check_badargs('F', CblasColMajor, uplo, transa, transb,
                            M, K, lda, ldb, ldc, expected_info);
    ASSERT_EQUAL(TRUE, passed);
}

#ifndef NO_CBLAS
/**
 * C API specific test.
 * Test error function for an invalid param order.
 * Must be column or row major.
 */
CTEST(cgemmt, xerbla_c_api_major_invalid)
{
    blasint M = 50, K = 50;
    blasint lda = 50, ldb = 50, ldc = 50;
    int expected_info = 0;

    int passed = check_badargs('C', 'O', CblasUpper, CblasNoTrans, CblasNoTrans,
                            M, K, lda, ldb, ldc, expected_info);
    ASSERT_EQUAL(TRUE, passed);
}

/**
 * C API specific test. Column Major
 * Test error function for an invalid param uplo.
 * Must be upper or lower.
 */
CTEST(cgemmt, xerbla_c_api_colmajor_uplo_invalid)
{
    blasint M = 50, K = 50;
    blasint lda = 50, ldb = 50, ldc = 50;
    int expected_info = 1;

    int passed = check_badargs('C', CblasColMajor, 'O', CblasNoTrans, CblasNoTrans,
                            M, K, lda, ldb, ldc, expected_info);
    ASSERT_EQUAL(TRUE, passed);
}

/**
 * C API specific test. Column Major
 * Test error function for an invalid param transa.
 * Must be trans or no-trans.
 */
CTEST(cgemmt, xerbla_c_api_colmajor_transa_invalid)
{
    blasint M = 50, K = 50;
    blasint lda = 50, ldb = 50, ldc = 50;
    int expected_info = 2;

    int passed = check_badargs('C', CblasColMajor, CblasUpper, 'O', CblasNoTrans,
                            M, K, lda, ldb, ldc, expected_info);
    ASSERT_EQUAL(TRUE, passed);
}

/**
 * C API specific test. Column Major
 * Test error function for an invalid param transb.
 * Must be trans or no-trans.
 */
CTEST(cgemmt, xerbla_c_api_colmajor_transb_invalid)
{
    blasint M = 50, K = 50;
    blasint lda = 50, ldb = 50, ldc = 50;
    int expected_info = 3;

    int passed = check_badargs('C', CblasColMajor, CblasUpper, CblasNoTrans, 'O',
                            M, K, lda, ldb, ldc, expected_info);
    ASSERT_EQUAL(TRUE, passed);
}

/**
 * C API specific test. Column Major
 * Test error function for an invalid param M.
 * Must be positive.
 */
CTEST(cgemmt, xerbla_c_api_colmajor_m_invalid)
{
    blasint M = -1, K = 50;
    blasint lda = 50, ldb = 50, ldc = 50;
    int expected_info = 4;

    int passed = check_badargs('C', CblasColMajor, CblasUpper, CblasNoTrans, CblasNoTrans,
                            M, K, lda, ldb, ldc, expected_info);
    ASSERT_EQUAL(TRUE, passed);
}

/**
 * C API specific test. Column Major
 * Test error function for an invalid param K.
 * Must be positive.
 */
CTEST(cgemmt, xerbla_c_api_colmajor_k_invalid)
{
    blasint M = 50, K = -1;
    blasint lda = 50, ldb = 50, ldc = 50;
    int expected_info = 5;

    int passed = check_badargs('C', CblasColMajor, CblasUpper, CblasNoTrans, CblasNoTrans,
                            M, K, lda, ldb, ldc, expected_info);
    ASSERT_EQUAL(TRUE, passed);
}

/**
 * C API specific test. Column Major
 * Test error function for an invalid param lda.
 * Must be must be at least K if matrix A transposed.
 */
CTEST(cgemmt, xerbla_c_api_colmajor_lda_invalid)
{
    blasint M = 50, K = 100;
    blasint lda = 50, ldb = 100, ldc = 50;
    int expected_info = 8;

    int passed = check_badargs('C', CblasColMajor, CblasUpper, CblasTrans, CblasNoTrans,
                            M, K, lda, ldb, ldc, expected_info);
    ASSERT_EQUAL(TRUE, passed);
}

/**
 * C API specific test. Column Major
 * Test error function for an invalid param ldb.
 * Must be must be at least K if matrix B not transposed.
 */
CTEST(cgemmt, xerbla_c_api_colmajor_ldb_invalid)
{
    blasint M = 50, K = 100;
    blasint lda = 50, ldb = 50, ldc = 50;
    int expected_info = 10;

    int passed = check_badargs('C', CblasColMajor, CblasUpper, CblasNoTrans, CblasNoTrans,
                            M, K, lda, ldb, ldc, expected_info);
    ASSERT_EQUAL(TRUE, passed);
}

/**
 * C API specific test. Column Major
 * Test error function for an invalid param ldc.
 * Must be must be at least M.
 */
CTEST(cgemmt, xerbla_c_api_colmajor_ldc_invalid)
{
    blasint M = 100, K = 50;
    blasint lda = 50, ldb = 50, ldc = 50;
    int expected_info = 13;

    int passed = check_badargs('C', CblasColMajor, CblasUpper, CblasTrans, CblasNoTrans,
                            M, K, lda, ldb, ldc, expected_info);
    ASSERT_EQUAL(TRUE, passed);
}

/**
 * C API specific test. Row Major
 * Test error function for an invalid param uplo.
 * Must be upper or lower.
 */
CTEST(cgemmt, xerbla_c_api_rowmajor_uplo_invalid)
{
    blasint M = 50, K = 50;
    blasint lda = 50, ldb = 50, ldc = 50;
    int expected_info = 1;

    int passed = check_badargs('C', CblasRowMajor, 'O', CblasNoTrans, CblasNoTrans,
                            M, K, lda, ldb, ldc, expected_info);
    ASSERT_EQUAL(TRUE, passed);
}

/**
 * C API specific test. Row Major
 * Test error function for an invalid param transa.
 * Must be trans or no-trans.
 */
CTEST(cgemmt, xerbla_c_api_rowmajor_transa_invalid)
{
    blasint M = 50, K = 50;
    blasint lda = 50, ldb = 50, ldc = 50;
    int expected_info = 2;

    int passed = check_badargs('C', CblasRowMajor, CblasUpper, 'O', CblasNoTrans,
                            M, K, lda, ldb, ldc, expected_info);
    ASSERT_EQUAL(TRUE, passed);
}

/**
 * C API specific test. Row Major
 * Test error function for an invalid param transb.
 * Must be trans or no-trans.
 */
CTEST(cgemmt, xerbla_c_api_rowmajor_transb_invalid)
{
    blasint M = 50, K = 50;
    blasint lda = 50, ldb = 50, ldc = 50;
    int expected_info = 3;

    int passed = check_badargs('C', CblasRowMajor, CblasUpper, CblasNoTrans, 'O',
                            M, K, lda, ldb, ldc, expected_info);
    ASSERT_EQUAL(TRUE, passed);
}

/**
 * C API specific test. Row Major
 * Test error function for an invalid param M.
 * Must be positive.
 */
CTEST(cgemmt, xerbla_c_api_rowmajor_m_invalid)
{
    blasint M = -1, K = 50;
    blasint lda = 50, ldb = 50, ldc = 50;
    int expected_info = 4;

    int passed = check_badargs('C', CblasRowMajor, CblasUpper, CblasNoTrans, CblasNoTrans,
                            M, K, lda, ldb, ldc, expected_info);
    ASSERT_EQUAL(TRUE, passed);
}

/**
 * C API specific test. Row Major
 * Test error function for an invalid param K.
 * Must be positive.
 */
CTEST(cgemmt, xerbla_c_api_rowmajor_k_invalid)
{
    blasint M = 50, K = -1;
    blasint lda = 50, ldb = 50, ldc = 50;
    int expected_info = 5;

    int passed = check_badargs('C', CblasRowMajor, CblasUpper, CblasNoTrans, CblasNoTrans,
                            M, K, lda, ldb, ldc, expected_info);
    ASSERT_EQUAL(TRUE, passed);
}

/**
 * C API specific test. Row Major
 * Test error function for an invalid param lda.
 * Must be must be at least K if matrix A transposed.
 */
CTEST(cgemmt, xerbla_c_api_rowmajor_lda_invalid)
{
    blasint M = 50, K = 100;
    blasint lda = 50, ldb = 50, ldc = 50;
    int expected_info = 8;

    int passed = check_badargs('C', CblasRowMajor, CblasUpper, CblasNoTrans, CblasNoTrans,
                            M, K, lda, ldb, ldc, expected_info);
    ASSERT_EQUAL(TRUE, passed);
}

/**
 * C API specific test. Row Major
 * Test error function for an invalid param ldb.
 * Must be must be at least K if matrix B transposed.
 */
CTEST(cgemmt, xerbla_c_api_rowmajor_ldb_invalid)
{
    blasint M = 50, K = 100;
    blasint lda = 50, ldb = 50, ldc = 50;
    int expected_info = 10;

    int passed = check_badargs('C', CblasRowMajor, CblasUpper, CblasTrans, CblasTrans,
                            M, K, lda, ldb, ldc, expected_info);
    ASSERT_EQUAL(TRUE, passed);
}

/**
 * C API specific test. Row Major
 * Test error function for an invalid param ldc.
 * Must be must be at least M.
 */
CTEST(cgemmt, xerbla_c_api_rowmajor_ldc_invalid)
{
    blasint M = 100, K = 50;
    blasint lda = 100, ldb = 100, ldc = 50;
    int expected_info = 13;

    int passed = check_badargs('C', CblasRowMajor, CblasUpper, CblasTrans, CblasNoTrans,
                            M, K, lda, ldb, ldc, expected_info);
    ASSERT_EQUAL(TRUE, passed);
}
#endif
#endif
