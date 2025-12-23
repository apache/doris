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
#define INCREMENT 1

struct DATA_ZGBMV {
    double a_test[DATASIZE * DATASIZE * 2];
    double a_band_storage[DATASIZE * DATASIZE * 2];
    double matrix[DATASIZE * DATASIZE * 2];
    double b_test[DATASIZE * 2 * INCREMENT];
    double c_test[DATASIZE * 2 * INCREMENT];
    double c_verify[DATASIZE * 2 * INCREMENT];
};

#ifdef BUILD_COMPLEX16

static struct DATA_ZGBMV data_zgbmv;

/** 
 * Transform full-storage band matrix A to band-packed storage mode.
 * 
 * param m - number of rows of A
 * param n - number of columns of A
 * param kl - number of sub-diagonals of the matrix A
 * param ku - number of super-diagonals of the matrix A
 * output param a - buffer for holding band-packed matrix
 * param lda - specifies the leading dimension of a
 * param matrix - buffer holding full-storage band matrix A 
 * param ldm - specifies the leading full-storage band matrix A
 */
static void transform_to_band_storage(blasint m, blasint n, blasint kl, 
                                      blasint ku, double* a, blasint lda,
                                      double* matrix, blasint ldm)
{
    blasint i, j, k;
    for (j = 0; j < n; j++) 
    {
        k = 2 * (ku - j);
        for (i = MAX(0, 2*(j - ku)); i < MIN(m, j + kl + 1) * 2; i+=2) 
        {
            a[(k + i) + j * lda * 2] = matrix[i + j * ldm * 2];
            a[(k + i) + j * lda * 2 + 1] = matrix[i + j * ldm * 2 + 1];
        }
    }
}

/** 
 * Generate full-storage band matrix A with kl sub-diagonals and ku super-diagonals
 * 
 * param m - number of rows of A
 * param n - number of columns of A
 * param kl - number of sub-diagonals of the matrix A
 * param ku - number of super-diagonals of the matrix A
 * output param band_matrix - buffer for full-storage band matrix.
 * param matrix - buffer holding input general matrix
 * param ldm - specifies the leading of input general matrix
 */
static void get_band_matrix(blasint m, blasint n, blasint kl, blasint ku, 
                            double *band_matrix, double *matrix, blasint ldm)
{
    blasint i, j;
    blasint k = 0;
    for (i = 0; i < n; i++)
    {
        for (j = 0; j < m * 2; j += 2)
        {
            if ((blasint)(j/2) > kl + i || i > ku + (blasint)(j/2)) 
            {
                band_matrix[i * ldm * 2 + j] = 0.0;
                band_matrix[i * ldm * 2 + j + 1] = 0.0;
                continue;
            }

            band_matrix[i * ldm * 2 + j] = matrix[k++];
            band_matrix[i * ldm * 2 + j + 1] = matrix[k++];
        }
    }
}

/**
 * Comapare results computed by zgbmv and zgemv 
 * since gbmv is gemv for band matrix
 * 
 * param trans specifies op(A), the transposition operation applied to A
 * param m - number of rows of A
 * param n - number of columns of A
 * param kl - number of sub-diagonals of the matrix A
 * param ku - number of super-diagonals of the matrix A
 * param alpha - scaling factor for the matrix-vector product
 * param lda - specifies the leading dimension of a
 * param inc_b - stride of vector b
 * param beta - scaling factor for vector c
 * param inc_c - stride of vector c
 * return norm of differences 
 */
static double check_zgbmv(char trans, blasint m, blasint n, blasint kl, blasint ku,
    double *alpha, blasint lda, blasint inc_b, double *beta, blasint inc_c)
{
    blasint i;
    blasint lenb, lenc;
    
    if(trans == 'T' || trans == 'C' || trans == 'D' || trans == 'U'){
        lenb = m;
        lenc = n;
    } else {
        lenb = n;
        lenc = m;
    }

    drand_generate(data_zgbmv.matrix, m * n * 2);
    drand_generate(data_zgbmv.b_test, 2 * (1 + (lenb - 1) * inc_b));
    drand_generate(data_zgbmv.c_test, 2 * (1 + (lenc - 1) * inc_c));

    for (i = 0; i < 2 * (1 + (lenc - 1) * inc_c); i++)
        data_zgbmv.c_verify[i] = data_zgbmv.c_test[i];

    get_band_matrix(m, n, kl, ku, data_zgbmv.a_test, data_zgbmv.matrix, m);

    transform_to_band_storage(m, n, kl, ku, data_zgbmv.a_band_storage, lda, data_zgbmv.a_test, m);

    BLASFUNC(zgemv)(&trans, &m, &n, alpha, data_zgbmv.a_test, &m, data_zgbmv.b_test,
                    &inc_b, beta, data_zgbmv.c_verify, &inc_c);

    BLASFUNC(zgbmv)(&trans, &m, &n, &kl, &ku, alpha, data_zgbmv.a_band_storage, &lda, data_zgbmv.b_test, 
                    &inc_b, beta, data_zgbmv.c_test, &inc_c);

    for (i = 0; i < 2 * (1 + (lenc - 1) * inc_c); i++)
        data_zgbmv.c_verify[i] -= data_zgbmv.c_test[i];

    return BLASFUNC(dznrm2)(&lenc, data_zgbmv.c_verify, &inc_c);
}

/**
 * Test zgbmv by comparing it against zgemv
 * with param trans is D
 */
CTEST(zgbmv, trans_D)
{
    blasint m = 50, n = 25;
    blasint inc_b = 1, inc_c = 1;
    blasint kl = 20, ku = 11;
    blasint lda = 50;
    char trans = 'D';

    double alpha[] = {7.0, 1.0};
    double beta[] = {1.5, -1.5};

    double norm = check_zgbmv(trans, m, n, kl, ku, alpha, lda, inc_b, beta, inc_c);
    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_TOL);
}

/**
 * Test zgbmv by comparing it against zgemv
 * with param trans is O
 */
CTEST(zgbmv, trans_O)
{
    blasint m = 50, n = 25;
    blasint inc_b = 1, inc_c = 1;
    blasint kl = 20, ku = 10;
    blasint lda = 50;
    char trans = 'O';

    double alpha[] = {7.0, 1.0};
    double beta[] = {1.5, -1.5};

    double norm = check_zgbmv(trans, m, n, kl, ku, alpha, lda, inc_b, beta, inc_c);
    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_TOL);
}

/**
 * Test zgbmv by comparing it against zgemv
 * with param trans is S
 */
CTEST(zgbmv, trans_S)
{
    blasint m = 50, n = 25;
    blasint inc_b = 1, inc_c = 1;
    blasint kl = 6, ku = 9;
    blasint lda = 50;
    char trans = 'S';

    double alpha[] = {7.0, 1.0};
    double beta[] = {1.5, -1.5};

    double norm = check_zgbmv(trans, m, n, kl, ku, alpha, lda, inc_b, beta, inc_c);
    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_TOL);
}

/**
 * Test zgbmv by comparing it against zgemv
 * with param trans is U
 */
CTEST(zgbmv, trans_U)
{
    blasint m = 25, n = 50;
    blasint inc_b = 1, inc_c = 1;
    blasint kl = 7, ku = 11;
    blasint lda = kl + ku + 1;
    char trans = 'U';

    double alpha[] = {7.0, 1.0};
    double beta[] = {1.5, -1.5};

    double norm = check_zgbmv(trans, m, n, kl, ku, alpha, lda, inc_b, beta, inc_c);
    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_TOL);
}

/**
 * Test zgbmv by comparing it against zgemv
 * with param trans is C
 */
CTEST(zgbmv, trans_C)
{
    blasint m = 50, n = 25;
    blasint inc_b = 1, inc_c = 1;
    blasint kl = 20, ku = 11;
    blasint lda = 50;
    char trans = 'C';

    double alpha[] = {7.0, 1.0};
    double beta[] = {1.5, -1.5};

    double norm = check_zgbmv(trans, m, n, kl, ku, alpha, lda, inc_b, beta, inc_c);
    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_TOL);
}

/**
 * Test zgbmv by comparing it against zgemv
 * with param trans is R
 */
CTEST(zgbmv, trans_R)
{
    blasint m = 50, n = 100;
    blasint inc_b = 1, inc_c = 1;
    blasint kl = 20, ku = 11;
    blasint lda = 50;
    char trans = 'R';

    double alpha[] = {7.0, 1.0};
    double beta[] = {1.5, -1.5};

    double norm = check_zgbmv(trans, m, n, kl, ku, alpha, lda, inc_b, beta, inc_c);
    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_TOL);
}
#endif
