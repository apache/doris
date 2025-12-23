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

struct DATA_ZIMATCOPY {
    double a_test[DATASIZE * DATASIZE * 2];
    double a_verify[DATASIZE * DATASIZE * 2];
};

#ifdef BUILD_COMPLEX16
static struct DATA_ZIMATCOPY data_zimatcopy;

/**
 * Comapare results computed by zimatcopy and reference func
 *
 * param api specifies tested api (C or Fortran)
 * param order specifies row or column major order
 * param trans specifies op(A), the transposition operation 
 * applied to the matrix A
 * param rows specifies number of rows of A
 * param cols specifies number of columns of A
 * param alpha specifies scaling factor for matrix A
 * param lda_src - leading dimension of the matrix A
 * param lda_dst - leading dimension of output matrix A
 * return norm of difference between openblas and reference func
 */
static double check_zimatcopy(char api, char order, char trans, blasint rows, blasint cols, double *alpha, 
                             blasint lda_src, blasint lda_dst)
{
    blasint m, n;
    blasint rows_out, cols_out;
    enum CBLAS_ORDER corder;
    enum CBLAS_TRANSPOSE ctrans;
    int conj = -1;

    if (order == 'C') {
        n = rows; m = cols;
    }
    else {
        m = rows; n = cols;
    }

    if(trans == 'T' || trans == 'C') {
        rows_out = n; cols_out = m*2;
        if (trans == 'C')
            conj = 1;
    }
    else {
        rows_out = m; cols_out = n*2;
        if (trans == 'R')
            conj = 1;
    }

    drand_generate(data_zimatcopy.a_test, lda_src*m*2);

    if (trans == 'T' || trans == 'C') {
        ztranspose(m, n, alpha, data_zimatcopy.a_test, lda_src, data_zimatcopy.a_verify, lda_dst, conj);
    } 
    else {
        my_zcopy(m, n, alpha, data_zimatcopy.a_test, lda_src, data_zimatcopy.a_verify, lda_dst, conj);
    }

    if (api == 'F') {
        BLASFUNC(zimatcopy)(&order, &trans, &rows, &cols, alpha, data_zimatcopy.a_test, 
                            &lda_src, &lda_dst);
    }
#ifndef NO_CBLAS
    else {
        if (order == 'C') corder = CblasColMajor;
        if (order == 'R') corder = CblasRowMajor;
        if (trans == 'T') ctrans = CblasTrans;
        if (trans == 'N') ctrans = CblasNoTrans;
        if (trans == 'C') ctrans = CblasConjTrans;
        if (trans == 'R') ctrans = CblasConjNoTrans;
        cblas_zimatcopy(corder, ctrans, rows, cols, alpha, data_zimatcopy.a_test, 
                    lda_src, lda_dst);
    }
#endif

    // Find the differences between output matrix computed by zimatcopy and reference func
    return dmatrix_difference(data_zimatcopy.a_test, data_zimatcopy.a_verify, cols_out, rows_out, lda_dst*2);    
}

/**
 * Check if error function was called with expected function name
 * and param info
 *
 * param order specifies row or column major order
 * param trans specifies op(A), the transposition operation 
 * applied to the matrix A
 * param rows specifies number of rows of A
 * param cols specifies number of columns of A
 * param lda_src - leading dimension of the matrix A
 * param lda_dst - leading dimension of output matrix A
 * param expected_info - expected invalid parameter number
 * return TRUE if everything is ok, otherwise FALSE
 */
static int check_badargs(char order, char trans, blasint rows, blasint cols,
                          blasint lda_src, blasint lda_dst, int expected_info)
{
    double alpha[] = {1.0, 1.0};

    set_xerbla("ZIMATCOPY", expected_info);

    BLASFUNC(zimatcopy)(&order, &trans, &rows, &cols, alpha, data_zimatcopy.a_test, 
                        &lda_src, &lda_dst);

    return check_error();
}

/**
 * Fortran API specific test
 * Test zimatcopy by comparing it against reference
 * with the following options:
 *
 * Column Major
 * Transposition
 * Square matrix
 * alpha_r = 1.0, alpha_i = 2.0
 */
CTEST(zimatcopy, colmajor_trans_col_100_row_100)
{
    blasint m = 100, n = 100;
    blasint lda_src = 100, lda_dst = 100;
    char order = 'C';
    char trans = 'T';
    double alpha[] = {1.0, 2.0};

    double norm = check_zimatcopy('F', order, trans, m, n, alpha, lda_src, lda_dst);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * Fortran API specific test
 * Test zimatcopy by comparing it against reference
 * with the following options:
 *
 * Column Major
 * Copy only
 * Square matrix
 * alpha_r = -3.0, alpha_i = 1.0
 */
CTEST(zimatcopy, colmajor_notrans_col_100_row_100)
{
    blasint m = 100, n = 100;
    blasint lda_src = 100, lda_dst = 100;
    char order = 'C';
    char trans = 'N';
    double alpha[] = {-3.0, 1.0};

    double norm = check_zimatcopy('F', order, trans, m, n, alpha, lda_src, lda_dst);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * Fortran API specific test
 * Test zimatcopy by comparing it against reference
 * with the following options:
 *
 * Column Major
 * Copy and conjugate
 * alpha_r = 1.0, alpha_i = 2.0
 */
CTEST(zimatcopy, colmajor_conj_col_100_row_100)
{
    blasint m = 100, n = 100;
    blasint lda_src = 100, lda_dst = 100;
    char order = 'C';
    char trans = 'R';
    double alpha[] = {1.0, 2.0};

    double norm = check_zimatcopy('F', order, trans, m, n, alpha, lda_src, lda_dst);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * Fortran API specific test
 * Test zimatcopy by comparing it against reference
 * with the following options:
 *
 * Column Major
 * Transposition and conjugate
 * alpha_r = 2.0, alpha_i = 1.0
 */
CTEST(zimatcopy, colmajor_conjtrans_col_100_row_100)
{
    blasint m = 100, n = 100;
    blasint lda_src = 100, lda_dst = 100;
    char order = 'C';
    char trans = 'C';
    double alpha[] = {2.0, 1.0};

    double norm = check_zimatcopy('F', order, trans, m, n, alpha, lda_src, lda_dst);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * Fortran API specific test
 * Test zimatcopy by comparing it against reference
 * with the following options:
 *
 * Column Major
 * Transposition
 * Rectangular matrix
 * alpha_r = 1.0, alpha_i = 2.0
 */
CTEST(zimatcopy, colmajor_trans_col_50_row_100)
{
    blasint m = 100, n = 50;
    blasint lda_src = 100, lda_dst = 50;
    char order = 'C';
    char trans = 'T';
    double alpha[] = {1.0, 2.0};

    double norm = check_zimatcopy('F', order, trans, m, n, alpha, lda_src, lda_dst);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * Fortran API specific test
 * Test zimatcopy by comparing it against reference
 * with the following options:
 *
 * Column Major
 * Copy only
 * Rectangular matrix
 * alpha_r = 1.0, alpha_i = 2.0
 */
CTEST(zimatcopy, colmajor_notrans_col_100_row_50)
{
    blasint m = 50, n = 100;
    blasint lda_src = 50, lda_dst = 50;
    char order = 'C';
    char trans = 'N';
    double alpha[] = {1.0, 2.0};

    double norm = check_zimatcopy('F', order, trans, m, n, alpha, lda_src, lda_dst);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * Fortran API specific test
 * Test zimatcopy by comparing it against reference
 * with the following options:
 *
 * Column Major
 * Transposition and conjugate
 * Rectangular matrix
 * alpha_r = 1.0, alpha_i = 1.0
 */
CTEST(zimatcopy, colmajor_conjtrans_col_50_row_100)
{
    blasint m = 100, n = 50;
    blasint lda_src = 100, lda_dst = 50;
    char order = 'C';
    char trans = 'C';
    double alpha[] = {1.0, 1.0};

    double norm = check_zimatcopy('F', order, trans, m, n, alpha, lda_src, lda_dst);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * Fortran API specific test
 * Test zimatcopy by comparing it against reference
 * with the following options:
 *
 * Column Major
 * Copy and conjugate
 * Rectangular matrix
 * alpha_r = 1.0, alpha_i = 2.0
 */
CTEST(zimatcopy, colmajor_conj_col_100_row_50)
{
    blasint m = 50, n = 100;
    blasint lda_src = 50, lda_dst = 50;
    char order = 'C';
    char trans = 'R';
    double alpha[] = {1.0, 2.0};

    double norm = check_zimatcopy('F', order, trans, m, n, alpha, lda_src, lda_dst);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * Fortran API specific test
 * Test zimatcopy by comparing it against reference
 * with the following options:
 *
 * Row Major
 * Transposition
 * Square matrix
 * alpha_r = 1.0, alpha_i = 2.0
 */
CTEST(zimatcopy, rowmajor_trans_col_100_row_100)
{
    blasint m = 100, n = 100;
    blasint lda_src = 100, lda_dst = 100;
    char order = 'R';
    char trans = 'T';
    double alpha[] = {1.0, 2.0};

    double norm = check_zimatcopy('F', order, trans, m, n, alpha, lda_src, lda_dst);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * Fortran API specific test
 * Test zimatcopy by comparing it against reference
 * with the following options:
 *
 * Row Major
 * Copy only
 * Square matrix
 * alpha_r = 2.0, alpha_i = 3.0
 */
CTEST(zimatcopy, rowmajor_notrans_col_100_row_100)
{
    blasint m = 100, n = 100;
    blasint lda_src = 100, lda_dst = 100;
    char order = 'R';
    char trans = 'N';
    double alpha[] = {2.0, 3.0};

    double norm = check_zimatcopy('F', order, trans, m, n, alpha, lda_src, lda_dst);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * Fortran API specific test
 * Test zimatcopy by comparing it against reference
 * with the following options:
 *
 * Column Major
 * Copy and conjugate
 * alpha_r = 1.0, alpha_i = 2.0
 */
CTEST(zimatcopy, rowmajor_conj_col_100_row_100)
{
    blasint m = 100, n = 100;
    blasint lda_src = 100, lda_dst = 100;
    char order = 'R';
    char trans = 'R';
    double alpha[] = {1.0, 2.0};

    double norm = check_zimatcopy('F', order, trans, m, n, alpha, lda_src, lda_dst);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * Fortran API specific test
 * Test zimatcopy by comparing it against reference
 * with the following options:
 *
 * Column Major
 * Transposition and conjugate
 * alpha_r = 2.0, alpha_i = 1.0
 */
CTEST(zimatcopy, rowmajor_conjtrans_col_100_row_100)
{
    blasint m = 100, n = 100;
    blasint lda_src = 100, lda_dst = 100;
    char order = 'R';
    char trans = 'C';
    double alpha[] = {2.0, 1.0};

    double norm = check_zimatcopy('F', order, trans, m, n, alpha, lda_src, lda_dst);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * Fortran API specific test
 * Test zimatcopy by comparing it against reference
 * with the following options:
 *
 * Row Major
 * Copy only
 * Rectangular matrix
 * alpha_r = 2.0, alpha_i = 1.0
 */
CTEST(zimatcopy, rowmajor_notrans_col_50_row_100)
{
    blasint m = 100, n = 50;
    blasint lda_src = 50, lda_dst = 50;
    char order = 'R';
    char trans = 'N'; 
    double alpha[] = {2.0, 1.0};

    double norm = check_zimatcopy('F', order, trans, m, n, alpha, lda_src, lda_dst);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * Fortran API specific test
 * Test zimatcopy by comparing it against reference
 * with the following options:
 *
 * Row Major
 * Transposition
 * Rectangular matrix
 * alpha_r = 1.0, alpha_i = 2.0
 */
CTEST(zimatcopy, rowmajor_trans_col_50_row_100)
{
    blasint m = 100, n = 50;
    blasint lda_src = 50, lda_dst = 100;
    char order = 'R';
    char trans = 'T';
    double alpha[] = {1.0, 2.0};

    double norm = check_zimatcopy('F', order, trans, m, n, alpha, lda_src, lda_dst);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * Fortran API specific test
 * Test zimatcopy by comparing it against reference
 * with the following options:
 *
 * Row Major
 * Copy and conjugate
 * alpha_r = 1.5, alpha_i = -1.0
 */
CTEST(zimatcopy, rowmajor_conj_col_50_row_100)
{
    blasint m = 100, n = 50;
    blasint lda_src = 50, lda_dst = 50;
    char order = 'R';
    char trans = 'R'; 
    double alpha[] = {1.5, -1.0};

    double norm = check_zimatcopy('F', order, trans, m, n, alpha, lda_src, lda_dst);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * Fortran API specific test
 * Test zimatcopy by comparing it against reference
 * with the following options:
 *
 * Row Major
 * Transposition and conjugate
 * alpha_r = 1.0, alpha_i = 2.0
 */
CTEST(zimatcopy, rowmajor_conjtrans_col_50_row_100)
{
    blasint m = 100, n = 50;
    blasint lda_src = 50, lda_dst = 100;
    char order = 'R';
    char trans = 'C';
    double alpha[] = {1.0, 2.0};

    double norm = check_zimatcopy('F', order, trans, m, n, alpha, lda_src, lda_dst);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

#ifndef NO_CBLAS
/**
 * C API specific test
 * Test zimatcopy by comparing it against reference
 * with the following options:
 *
 * Column Major
 * Transposition
 * Square matrix
 * alpha_r = 3.0, alpha_i = 2.0
 */
CTEST(zimatcopy, c_api_colmajor_trans_col_100_row_100)
{
    blasint m = 100, n = 100;
    blasint lda_src = 100, lda_dst = 100;
    char order = 'C';
    char trans = 'T';
    double alpha[] = {3.0, 2.0};

    double norm = check_zimatcopy('C', order, trans, m, n, alpha, lda_src, lda_dst);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * C API specific test
 * Test zimatcopy by comparing it against reference
 * with the following options:
 *
 * Column Major
 * Copy only
 * Square matrix
 * alpha_r = 3.0, alpha_i = 1.5
 */
CTEST(zimatcopy, c_api_colmajor_notrans_col_100_row_100)
{
    blasint m = 100, n = 100;
    blasint lda_src = 100, lda_dst = 100;
    char order = 'C';
    char trans = 'N';
    double alpha[] = {3.0, 1.5};

    double norm = check_zimatcopy('C', order, trans, m, n, alpha, lda_src, lda_dst);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * C API specific test
 * Test zimatcopy by comparing it against reference
 * with the following options:
 *
 * Row Major
 * Transposition
 * Square matrix
 * alpha_r = 3.0, alpha_i = 1.0
 */
CTEST(zimatcopy, c_api_rowmajor_trans_col_100_row_100)
{
    blasint m = 100, n = 100;
    blasint lda_src = 100, lda_dst = 100;
    char order = 'R';
    char trans = 'T';
    double alpha[] = {3.0, 1.0};

    double norm = check_zimatcopy('C', order, trans, m, n, alpha, lda_src, lda_dst);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * C API specific test
 * Test zimatcopy by comparing it against reference
 * with the following options:
 *
 * Column Major
 * Copy and conjugate
 * alpha_r = 1.0, alpha_i = 2.0
 */
CTEST(zimatcopy, c_api_colmajor_conj_col_100_row_100)
{
    blasint m = 100, n = 100;
    blasint lda_src = 100, lda_dst = 100;
    char order = 'C';
    char trans = 'R';
    double alpha[] = {1.0, 2.0};

    double norm = check_zimatcopy('C', order, trans, m, n, alpha, lda_src, lda_dst);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * C API specific test
 * Test zimatcopy by comparing it against reference
 * with the following options:
 *
 * Column Major
 * Transposition and conjugate
 * alpha_r = 2.0, alpha_i = 1.0
 */
CTEST(zimatcopy, c_api_colmajor_conjtrans_col_100_row_100)
{
    blasint m = 100, n = 100;
    blasint lda_src = 100, lda_dst = 100;
    char order = 'C';
    char trans = 'C';
    double alpha[] = {2.0, 1.0};

    double norm = check_zimatcopy('C', order, trans, m, n, alpha, lda_src, lda_dst);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * C API specific test
 * Test zimatcopy by comparing it against reference
 * with the following options:
 *
 * Row Major
 * Copy only
 * Square matrix
 * alpha_r = 1.0, alpha_i = 1.0
 */
CTEST(zimatcopy, c_api_rowmajor_notrans_col_100_row_100)
{
    blasint m = 100, n = 100;
    blasint lda_src = 100, lda_dst = 100;
    char order = 'R';
    char trans = 'N';
    double alpha[] = {1.0, 1.0};

    double norm = check_zimatcopy('C', order, trans, m, n, alpha, lda_src, lda_dst);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * C API specific test
 * Test zimatcopy by comparing it against reference
 * with the following options:
 *
 * Row Major
 * Copy and conjugate
 * alpha_r = 1.5, alpha_i = -1.0
 */
CTEST(zimatcopy, c_api_rowmajor_conj_col_100_row_100)
{
    blasint m = 100, n = 100;
    blasint lda_src = 100, lda_dst = 100;
    char order = 'R';
    char trans = 'R'; 
    double alpha[] = {1.5, -1.0};

    double norm = check_zimatcopy('C', order, trans, m, n, alpha, lda_src, lda_dst);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * C API specific test
 * Test zimatcopy by comparing it against reference
 * with the following options:
 *
 * Row Major
 * Transposition and conjugate
 * alpha_r = 1.0, alpha_i = 2.0
 */
CTEST(zimatcopy, c_api_rowmajor_conjtrans_col_100_row_100)
{
    blasint m = 100, n = 100;
    blasint lda_src = 100, lda_dst = 100;
    char order = 'R';
    char trans = 'C';
    double alpha[] = {1.0, 2.0};

    double norm = check_zimatcopy('C', order, trans, m, n, alpha, lda_src, lda_dst);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}
#endif

/**
 * Test error function for an invalid param order.
 * Must be column (C) or row major (R).
 */
CTEST(zimatcopy, xerbla_invalid_order)
{
    blasint m = 100, n = 100;
    blasint lda_src = 100, lda_dst = 100;
    char order = 'O';
    char trans = 'T';
    int expected_info = 1;

    int passed = check_badargs(order, trans, m, n, lda_src, lda_dst, expected_info);
    ASSERT_EQUAL(TRUE, passed);
}

/**
 * Test error function for an invalid param trans.
 * Must be trans (T/C) or no-trans (N/R).
 */
CTEST(zimatcopy, xerbla_invalid_trans)
{
    blasint m = 100, n = 100;
    blasint lda_src = 100, lda_dst = 100;
    char order = 'C';
    char trans = 'O';
    int expected_info = 2;

    int passed = check_badargs(order, trans, m, n, lda_src, lda_dst, expected_info);
    ASSERT_EQUAL(TRUE, passed);
}

/**
 * Test error function for an invalid param lda_src.
 * If matrices are stored using row major layout, 
 * lda_src must be at least n.
 */
CTEST(zimatcopy, xerbla_rowmajor_invalid_lda)
{
    blasint m = 50, n = 100;
    blasint lda_src = 50, lda_dst = 100;
    char order = 'R';
    char trans = 'T';
    int expected_info = 7;

    int passed = check_badargs(order, trans, m, n, lda_src, lda_dst, expected_info);
    ASSERT_EQUAL(TRUE, passed);
}

/**
 * Test error function for an invalid param lda_src.
 * If matrices are stored using column major layout,
 * lda_src must be at least m.
 */
CTEST(zimatcopy, xerbla_colmajor_invalid_lda)
{
    blasint m = 100, n = 50;
    blasint lda_src = 50, lda_dst = 100;
    char order = 'C';
    char trans = 'T';
    int expected_info = 7;

    int passed = check_badargs(order, trans, m, n, lda_src, lda_dst, expected_info);
    ASSERT_EQUAL(TRUE, passed);
}

/**
 * Test error function for an invalid param lda_dst.
 * If matrices are stored using row major layout and 
 * there is no transposition, lda_dst must be at least n.
 */
CTEST(zimatcopy, xerbla_rowmajor_notrans_invalid_ldb)
{
    blasint m = 50, n = 100;
    blasint lda_src = 100, lda_dst = 50;
    char order = 'R';
    char trans = 'N';
    int expected_info = 9;

    int passed = check_badargs(order, trans, m, n, lda_src, lda_dst, expected_info);
    ASSERT_EQUAL(TRUE, passed);
}

/**
 * Test error function for an invalid param lda_dst.
 * If matrices are stored using row major layout and 
 * there is transposition, lda_dst must be at least m.
 */
CTEST(zimatcopy, xerbla_rowmajor_trans_invalid_ldb)
{
    blasint m = 100, n = 50;
    blasint lda_src = 100, lda_dst = 50;
    char order = 'R';
    char trans = 'T';
    int expected_info = 9;

    int passed = check_badargs(order, trans, m, n, lda_src, lda_dst, expected_info);
    ASSERT_EQUAL(TRUE, passed);
}

/**
 * Test error function for an invalid param lda_dst.
 * If matrices are stored using column major layout and 
 * there is no transposition, lda_dst must be at least m.
 */
CTEST(zimatcopy, xerbla_colmajor_notrans_invalid_ldb)
{
    blasint m = 100, n = 50;
    blasint lda_src = 100, lda_dst = 50;
    char order = 'C';
    char trans = 'N';
    int expected_info = 9;

    int passed = check_badargs(order, trans, m, n, lda_src, lda_dst, expected_info);
    ASSERT_EQUAL(TRUE, passed);
}

/**
 * Test error function for an invalid param lda_dst.
 * If matrices are stored using column major layout and 
 * there is transposition, lda_dst must be at least n.
 */
CTEST(zimatcopy, xerbla_colmajor_trans_invalid_ldb)
{
    blasint m = 50, n = 100;
    blasint lda_src = 100, lda_dst = 50;
    char order = 'C';
    char trans = 'T';
    int expected_info = 9;

    int passed = check_badargs(order, trans, m, n, lda_src, lda_dst, expected_info);
    ASSERT_EQUAL(TRUE, passed);
}
#endif
