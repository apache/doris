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

struct DATA_ZSPMV_N {
    double a_test[DATASIZE * DATASIZE * 2];
    double b_test[DATASIZE * 2 * INCREMENT];
    double c_test[DATASIZE * 2 * INCREMENT];
    double c_verify[DATASIZE * 2 * INCREMENT];
};

#ifdef BUILD_COMPLEX16

static struct DATA_ZSPMV_N data_zgemv_n;

/**
 * zgemv not transposed reference code
 *
 * param trans specifies whether matris A is conj or/and xconj
 * param m - number of rows of A
 * param n - number of columns of A
 * param alpha - scaling factor for the matrib-vector product
 * param a - buffer holding input matrib A
 * param lda - leading dimension of matrix A
 * param b - Buffer holding input vector b
 * param inc_b - stride of vector b
 * param beta - scaling factor for vector c
 * param c - buffer holding input/output vector c
 * param inc_c - stride of vector c
 */
static void zgemv_n_trusted(char trans, blasint m, blasint n, double *alpha, double *a,
                          blasint lda, double *b, blasint inc_b, double *beta, double *c,
                          blasint inc_c)
{
	blasint i, j;
    blasint i2 = 0;
	blasint ib = 0, ic = 0;

    double temp_r, temp_i;

	double *a_ptr = a;
    blasint lda2 = 2*lda;

	blasint inc_b2 = 2 * inc_b;
    blasint inc_c2 = 2 * inc_c;

    BLASFUNC(zscal)(&m, beta, c, &inc_c);

	for (j = 0; j < n; j++)
	{

        if (trans == 'N' || trans == 'R') {
            temp_r = alpha[0] * b[ib] - alpha[1] * b[ib+1];
            temp_i = alpha[0] * b[ib+1] + alpha[1] * b[ib];
        } else {
            temp_r = alpha[0] * b[ib] + alpha[1] * b[ib+1];
            temp_i = alpha[0] * b[ib+1] - alpha[1] * b[ib];
        }

		ic = 0;
		i2 = 0;

		for (i = 0; i < m; i++)
		{
                if (trans == 'N') {
                    c[ic] += temp_r * a_ptr[i2] - temp_i * a_ptr[i2+1];
                    c[ic+1] += temp_r * a_ptr[i2+1] + temp_i * a_ptr[i2];
                } 
                if (trans == 'O') {
                    c[ic] += temp_r * a_ptr[i2] + temp_i * a_ptr[i2+1];
                    c[ic+1] += temp_r * a_ptr[i2+1] - temp_i * a_ptr[i2];
                }
                if (trans == 'R') {
                    c[ic] += temp_r * a_ptr[i2] + temp_i * a_ptr[i2+1];
                    c[ic+1] -= temp_r * a_ptr[i2+1] - temp_i * a_ptr[i2];
                }
                if (trans == 'S') {
                    c[ic] += temp_r * a_ptr[i2] - temp_i * a_ptr[i2+1];
                    c[ic+1] -= temp_r * a_ptr[i2+1] + temp_i * a_ptr[i2];
                }
			i2 += 2;
			ic += inc_c2;
		}
		a_ptr += lda2;
		ib += inc_b2;
	}

}

/**
 * Comapare results computed by zgemv and zgemv_n_trusted
 *
 * param trans specifies whether matris A is conj or/and xconj
 * param m - number of rows of A
 * param n - number of columns of A
 * param alpha - scaling factor for the matrib-vector product
 * param lda - leading dimension of matrix A
 * param inc_b - stride of vector b
 * param beta - scaling factor for vector c
 * param inc_c - stride of vector c
 * return norm of differences
 */
static double check_zgemv_n(char trans, blasint m, blasint n, double *alpha, blasint lda, 
                            blasint inc_b, double *beta, blasint inc_c)
{
    blasint i;

    drand_generate(data_zgemv_n.a_test, n * lda);
    drand_generate(data_zgemv_n.b_test, 2 * n * inc_b);
    drand_generate(data_zgemv_n.c_test, 2 * m * inc_c);

    for (i = 0; i < m * 2 * inc_c; i++)
        data_zgemv_n.c_verify[i] = data_zgemv_n.c_test[i];

    zgemv_n_trusted(trans, m, n, alpha, data_zgemv_n.a_test, lda, data_zgemv_n.b_test, 
                  inc_b, beta, data_zgemv_n.c_test, inc_c);
    BLASFUNC(zgemv)(&trans, &m, &n, alpha, data_zgemv_n.a_test, &lda, data_zgemv_n.b_test, 
                    &inc_b, beta, data_zgemv_n.c_verify, &inc_c);

    for (i = 0; i < m * 2 * inc_c; i++)
        data_zgemv_n.c_verify[i] -= data_zgemv_n.c_test[i];

    return BLASFUNC(dznrm2)(&n, data_zgemv_n.c_verify, &inc_c);
}

/**
 * Test zgemv by comparing it against reference
 * with the following options:
 *
 * A is xconj
 * Number of rows and columns of A is 100
 * Stride of vector b is 1
 * Stride of vector c is 1
 */
CTEST(zgemv, trans_o_square_matrix)
{
    blasint n = 100, m = 100, lda = 100;
    blasint inc_b = 1, inc_c = 1;
    char trans = 'O';
    double alpha[] = {2.0, -1.0};
    double beta[] = {1.4, 5.0};

    double norm = check_zgemv_n(trans, m, n, alpha, lda, inc_b, beta, inc_c);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_TOL);
}

/**
 * Test zgemv by comparing it against reference
 * with the following options:
 *
 * A is xconj
 * Number of rows of A is 50
 * Number of colums of A is 100
 * Stride of vector b is 1
 * Stride of vector c is 1
 */
CTEST(zgemv, trans_o_rectangular_matrix_rows_less_then_cols)
{
    blasint n = 100, m = 50, lda = 50;
    blasint inc_b = 1, inc_c = 1;
    char trans = 'O';
    double alpha[] = {2.0, -1.0};
    double beta[] = {1.4, 5.0};

    double norm = check_zgemv_n(trans, m, n, alpha, lda, inc_b, beta, inc_c);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_TOL);
}

/**
 * Test zgemv by comparing it against reference
 * with the following options:
 *
 * A is xconj
 * Number of rows of A is 100
 * Number of colums of A is 50
 * Stride of vector b is 1
 * Stride of vector c is 1
 */
CTEST(zgemv, trans_o_rectangular_matrix_cols_less_then_rows)
{
    blasint n = 50, m = 100, lda = 100;
    blasint inc_b = 1, inc_c = 1;
    char trans = 'O';
    double alpha[] = {2.0, -1.0};
    double beta[] = {1.4, 5.0};

    double norm = check_zgemv_n(trans, m, n, alpha, lda, inc_b, beta, inc_c);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_TOL);
}

/**
 * Test zgemv by comparing it against reference
 * with the following options:
 *
 * A is xconj
 * Number of rows and columns of A is 100
 * Stride of vector b is 2
 * Stride of vector c is 2
 */
CTEST(zgemv, trans_o_double_strides)
{
    blasint n = 100, m = 100, lda = 100;
    blasint inc_b = 2, inc_c = 2;
    char trans = 'O';
    double alpha[] = {2.0, -1.0};
    double beta[] = {1.4, 5.0};

    double norm = check_zgemv_n(trans, m, n, alpha, lda, inc_b, beta, inc_c);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_TOL);
}

/**
 * Test zgemv by comparing it against reference
 * with the following options:
 *
 * A is xconj and conj
 * Number of rows and columns of A is 100
 * Stride of vector b is 1
 * Stride of vector c is 1
 */
CTEST(zgemv, trans_s_square_matrix)
{
    blasint n = 100, m = 100, lda = 100;
    blasint inc_b = 1, inc_c = 1;
    char trans = 'S';
    double alpha[] = {1.0, 1.0};
    double beta[] = {1.4, 5.0};

    double norm = check_zgemv_n(trans, m, n, alpha, lda, inc_b, beta, inc_c);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_TOL);
}

/**
 * Test zgemv by comparing it against reference
 * with the following options:
 *
 * A is xconj and conj
 * Number of rows of A is 50
 * Number of colums of A is 100
 * Stride of vector b is 1
 * Stride of vector c is 1
 */
CTEST(zgemv, trans_s_rectangular_matrix_rows_less_then_cols)
{
    blasint n = 100, m = 50, lda = 50;
    blasint inc_b = 1, inc_c = 1;
    char trans = 'S';
    double alpha[] = {2.0, -1.0};
    double beta[] = {1.4, 5.0};

    double norm = check_zgemv_n(trans, m, n, alpha, lda, inc_b, beta, inc_c);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_TOL);
}

/**
 * Test zgemv by comparing it against reference
 * with the following options:
 *
 * A is xconj and conj
 * Number of rows of A is 100
 * Number of colums of A is 50
 * Stride of vector b is 1
 * Stride of vector c is 1
 */
CTEST(zgemv, trans_s_rectangular_matrix_cols_less_then_rows)
{
    blasint n = 50, m = 100, lda = 100;
    blasint inc_b = 1, inc_c = 1;
    char trans = 'S';
    double alpha[] = {2.0, -1.0};
    double beta[] = {1.4, 0.0};

    double norm = check_zgemv_n(trans, m, n, alpha, lda, inc_b, beta, inc_c);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_TOL);
}

/**
 * Test zgemv by comparing it against reference
 * with the following options:
 *
 * A is xconj and conj
 * Number of rows and columns of A is 100
 * Stride of vector b is 2
 * Stride of vector c is 2
 */
CTEST(zgemv, trans_s_double_strides)
{
    blasint n = 100, m = 100, lda = 100;
    blasint inc_b = 2, inc_c = 2;
    char trans = 'S';
    double alpha[] = {2.0, -1.0};
    double beta[] = {1.0, 5.0};

    double norm = check_zgemv_n(trans, m, n, alpha, lda, inc_b, beta, inc_c);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_TOL);
}

#endif
