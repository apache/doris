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

struct DATA_CGBMV {
    float a_test[DATASIZE * DATASIZE * 2];
    float a_band_storage[DATASIZE * DATASIZE * 2];
    float matrix[DATASIZE * DATASIZE * 2];
    float b_test[DATASIZE * 2 * INCREMENT];
    float c_test[DATASIZE * 2 * INCREMENT];
    float c_verify[DATASIZE * 2 * INCREMENT];
};

#ifdef BUILD_COMPLEX
static struct DATA_CGBMV data_cgbmv;

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
                                      blasint ku, float* a, blasint lda,
                                      float* matrix, blasint ldm)
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
                            float *band_matrix, float *matrix, blasint ldm)
{
    blasint i, j;
    blasint k = 0;
    for (i = 0; i < n; i++)
    {
        for (j = 0; j < m * 2; j += 2)
        {
            if ((blasint)(j/2) > kl + i || i > ku + (blasint)(j/2)) 
            {
                band_matrix[i * ldm * 2 + j] = 0.0f;
                band_matrix[i * ldm * 2 + j + 1] = 0.0f;
                continue;
            }

            band_matrix[i * ldm * 2 + j] = matrix[k++];
            band_matrix[i * ldm * 2 + j + 1] = matrix[k++];
        }
    }
}

/**
 * Comapare results computed by cgbmv and cgemv 
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
static float check_cgbmv(char trans, blasint m, blasint n, blasint kl, blasint ku,
    float *alpha, blasint lda, blasint inc_b, float *beta, blasint inc_c)
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
    
    srand_generate(data_cgbmv.matrix, m * n * 2);
    srand_generate(data_cgbmv.b_test, 2 * (1 + (lenb - 1) * inc_b));
    srand_generate(data_cgbmv.c_test, 2 * (1 + (lenc - 1) * inc_c));

    for (i = 0; i < 2 * (1 + (lenc - 1) * inc_c); i++)
        data_cgbmv.c_verify[i] = data_cgbmv.c_test[i];

    get_band_matrix(m, n, kl, ku, data_cgbmv.a_test, data_cgbmv.matrix, m);

    transform_to_band_storage(m, n, kl, ku, data_cgbmv.a_band_storage, lda, data_cgbmv.a_test, m);

    BLASFUNC(cgemv)(&trans, &m, &n, alpha, data_cgbmv.a_test, &m, data_cgbmv.b_test,
                    &inc_b, beta, data_cgbmv.c_verify, &inc_c);

    BLASFUNC(cgbmv)(&trans, &m, &n, &kl, &ku, alpha, data_cgbmv.a_band_storage, &lda, data_cgbmv.b_test, 
                    &inc_b, beta, data_cgbmv.c_test, &inc_c);

    for (i = 0; i < 2 * (1 + (lenc - 1) * inc_c); i++)
        data_cgbmv.c_verify[i] -= data_cgbmv.c_test[i];

    return BLASFUNC(scnrm2)(&lenc, data_cgbmv.c_verify, &inc_c);
}

/**
 * Test cgbmv by comparing it against cgemv
 * with param trans is D
 */
CTEST(cgbmv, trans_D)
{
    blasint m = 50, n = 25;
    blasint inc_b = 1, inc_c = 1;
    blasint kl = 20, ku = 11;
    blasint lda = 50;
    char trans = 'D';

    float alpha[] = {7.0f, 1.0f};
    float beta[] = {1.5f, -1.5f};

    float norm = check_cgbmv(trans, m, n, kl, ku, alpha, lda, inc_b, beta, inc_c);
    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_TOL);
}

/**
 * Test cgbmv by comparing it against cgemv
 * with param trans is O
 */
CTEST(cgbmv, trans_O)
{
    blasint m = 50, n = 25;
    blasint inc_b = 1, inc_c = 1;
    blasint kl = 20, ku = 10;
    blasint lda = 50;
    char trans = 'O';

    float alpha[] = {7.0f, 1.0f};
    float beta[] = {1.5f, -1.5f};

    float norm = check_cgbmv(trans, m, n, kl, ku, alpha, lda, inc_b, beta, inc_c);
    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_TOL);
}

/**
 * Test cgbmv by comparing it against cgemv
 * with param trans is S
 */
CTEST(cgbmv, trans_S)
{
    blasint m = 50, n = 25;
    blasint inc_b = 1, inc_c = 1;
    blasint kl = 6, ku = 9;
    blasint lda = 50;
    char trans = 'S';

    float alpha[] = {7.0f, 1.0f};
    float beta[] = {1.5f, -1.5f};

    float norm = check_cgbmv(trans, m, n, kl, ku, alpha, lda, inc_b, beta, inc_c);
    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_TOL);
}

/**
 * Test cgbmv by comparing it against cgemv
 * with param trans is U
 */
CTEST(cgbmv, trans_U)
{
    blasint m = 25, n = 50;
    blasint inc_b = 1, inc_c = 1;
    blasint kl = 7, ku = 11;
    blasint lda = kl + ku + 1;
    char trans = 'U';

    float alpha[] = {7.0f, 1.0f};
    float beta[] = {1.5f, -1.5f};

    float norm = check_cgbmv(trans, m, n, kl, ku, alpha, lda, inc_b, beta, inc_c);
    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_TOL);
}

/**
 * Test cgbmv by comparing it against cgemv
 * with param trans is C
 */
CTEST(cgbmv, trans_C)
{
    blasint m = 50, n = 25;
    blasint inc_b = 1, inc_c = 1;
    blasint kl = 20, ku = 11;
    blasint lda = 50;
    char trans = 'C';

    float alpha[] = {7.0f, 1.0f};
    float beta[] = {1.5f, -1.5f};

    float norm = check_cgbmv(trans, m, n, kl, ku, alpha, lda, inc_b, beta, inc_c);
    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_TOL);
}

/**
 * Test cgbmv by comparing it against cgemv
 * with param trans is R
 */
CTEST(cgbmv, trans_R)
{
    blasint m = 50, n = 100;
    blasint inc_b = 1, inc_c = 1;
    blasint kl = 20, ku = 11;
    blasint lda = 50;
    char trans = 'R';

    float alpha[] = {7.0f, 1.0f};
    float beta[] = {1.5f, -1.5f};

    float norm = check_cgbmv(trans, m, n, kl, ku, alpha, lda, inc_b, beta, inc_c);
    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_TOL);
}
#endif
