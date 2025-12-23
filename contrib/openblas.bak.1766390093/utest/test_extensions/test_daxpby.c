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

struct DATA_DAXPBY{
    double x_test[DATASIZE * INCREMENT];
    double x_verify[DATASIZE * INCREMENT];
    double y_test[DATASIZE * INCREMENT];
    double y_verify[DATASIZE * INCREMENT];
};

#ifdef BUILD_DOUBLE
static struct DATA_DAXPBY data_daxpby;

/**
 * Fortran API specific function
 * Test daxpby by comparing it with dscal and daxpy.
 * Compare with the following options:
 * 
 * param n - number of elements in vectors x and y
 * param alpha - scalar alpha
 * param incx - increment for the elements of x
 * param beta - scalar beta
 * param incy - increment for the elements of y
 * return norm of difference
 */
static double check_daxpby(blasint n, double alpha, blasint incx, double beta, blasint incy)
{
    blasint i;

    // dscal accept only positive increments
    blasint incx_abs = labs(incx);
    blasint incy_abs = labs(incy);

    // Fill vectors x, y
    drand_generate(data_daxpby.x_test, n * incx_abs);
    drand_generate(data_daxpby.y_test, n * incy_abs);

    // Copy vector x for daxpy
    for (i = 0; i < n * incx_abs; i++)
        data_daxpby.x_verify[i] = data_daxpby.x_test[i];

    // Copy vector y for dscal
    for (i = 0; i < n * incy_abs; i++)
        data_daxpby.y_verify[i] = data_daxpby.y_test[i];

    // Find beta*y
    BLASFUNC(dscal)(&n, &beta, data_daxpby.y_verify, &incy_abs);

    // Find sum of alpha*x and beta*y
    BLASFUNC(daxpy)(&n, &alpha, data_daxpby.x_verify, &incx,
                        data_daxpby.y_verify, &incy);
    
    BLASFUNC(daxpby)(&n, &alpha, data_daxpby.x_test, &incx,
                        &beta, data_daxpby.y_test, &incy);

    // Find the differences between output vector caculated by daxpby and daxpy
    for (i = 0; i < n * incy_abs; i++)
        data_daxpby.y_test[i] -= data_daxpby.y_verify[i];

    // Find the norm of differences
    return BLASFUNC(dnrm2)(&n, data_daxpby.y_test, &incy_abs);
}

#ifndef NO_CBLAS
/**
 * C API specific function
 * Test daxpby by comparing it with dscal and daxpy.
 * Compare with the following options:
 * 
 * param n - number of elements in vectors x and y
 * param alpha - scalar alpha
 * param incx - increment for the elements of x
 * param beta - scalar beta
 * param incy - increment for the elements of y
 * return norm of difference
 */
static double c_api_check_daxpby(blasint n, double alpha, blasint incx, double beta, blasint incy)
{
    blasint i;

    // dscal accept only positive increments
    blasint incx_abs = labs(incx);
    blasint incy_abs = labs(incy);

    // Copy vector x for daxpy
    for (i = 0; i < n * incx_abs; i++)
        data_daxpby.x_verify[i] = data_daxpby.x_test[i];

    // Copy vector y for dscal
    for (i = 0; i < n * incy_abs; i++)
        data_daxpby.y_verify[i] = data_daxpby.y_test[i];

    // Find beta*y
    cblas_dscal(n, beta, data_daxpby.y_verify, incy_abs);

    // Find sum of alpha*x and beta*y
    cblas_daxpy(n, alpha, data_daxpby.x_verify, incx,
                        data_daxpby.y_verify, incy);
    
    cblas_daxpby(n, alpha, data_daxpby.x_test, incx,
                        beta, data_daxpby.y_test, incy);

    // Find the differences between output vector caculated by daxpby and daxpy
    for (i = 0; i < n * incy_abs; i++)
        data_daxpby.y_test[i] -= data_daxpby.y_verify[i];

    // Find the norm of differences
    return cblas_dnrm2(n, data_daxpby.y_test, incy_abs);
}
#endif
/**
 * Fortran API specific test
 * Test daxpby by comparing it with dscal and daxpy.
 * Test with the following options:
 * 
 * Size of vectors x, y is 100
 * Stride of vector x is 1
 * Stride of vector y is 1
 */
CTEST(daxpby, inc_x_1_inc_y_1_N_100)
{
    blasint n = DATASIZE, incx = 1, incy = 1;
    double alpha = 1.0;
    double beta = 1.0;

    double norm = check_daxpby(n, alpha, incx, beta, incy);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * Fortran API specific test
 * Test daxpby by comparing it with dscal and daxpy.
 * Test with the following options:
 * 
 * Size of vectors x, y is 100
 * Stride of vector x is 2
 * Stride of vector y is 1
 */
CTEST(daxpby, inc_x_2_inc_y_1_N_100)
{
    blasint n = DATASIZE, incx = 2, incy = 1;
    double alpha = 2.0;
    double beta = 1.0;

    double norm = check_daxpby(n, alpha, incx, beta, incy);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * Fortran API specific test
 * Test daxpby by comparing it with dscal and daxpy.
 * Test with the following options:
 * 
 * Size of vectors x, y is 100
 * Stride of vector x is 1
 * Stride of vector y is 2
 */
CTEST(daxpby, inc_x_1_inc_y_2_N_100)
{
    blasint n = DATASIZE, incx = 1, incy = 2;
    double alpha = 1.0;
    double beta = 2.0;

    double norm = check_daxpby(n, alpha, incx, beta, incy);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * Fortran API specific test
 * Test daxpby by comparing it with dscal and daxpy.
 * Test with the following options:
 * 
 * Size of vectors x, y is 100
 * Stride of vector x is 2
 * Stride of vector y is 2
 */
CTEST(daxpby, inc_x_2_inc_y_2_N_100)
{
    blasint n = DATASIZE, incx = 2, incy = 2;
    double alpha = 3.0;
    double beta = 4.0;

    double norm = check_daxpby(n, alpha, incx, beta, incy);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * Fortran API specific test
 * Test daxpby by comparing it with dscal and daxpy.
 * Test with the following options:
 * 
 * Size of vectors x, y is 100
 * Stride of vector x is -1
 * Stride of vector y is 2
 */
CTEST(daxpby, inc_x_neg_1_inc_y_2_N_100)
{
    blasint n = DATASIZE, incx = -1, incy = 2;
    double alpha = 5.0;
    double beta = 4.0;

    double norm = check_daxpby(n, alpha, incx, beta, incy);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * Fortran API specific test
 * Test daxpby by comparing it with dscal and daxpy.
 * Test with the following options:
 * 
 * Size of vectors x, y is 100
 * Stride of vector x is 2
 * Stride of vector y is -1
 */
CTEST(daxpby, inc_x_2_inc_y_neg_1_N_100)
{
    blasint n = DATASIZE, incx = 2, incy = -1;
    double alpha = 1.0;
    double beta = 6.0;

    double norm = check_daxpby(n, alpha, incx, beta, incy);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * Fortran API specific test
 * Test daxpby by comparing it with dscal and daxpy.
 * Test with the following options:
 * 
 * Size of vectors x, y is 100
 * Stride of vector x is -2
 * Stride of vector y is -1
 */
CTEST(daxpby, inc_x_neg_2_inc_y_neg_1_N_100)
{
    blasint n = DATASIZE, incx = -2, incy = -1;
    double alpha = 7.0;
    double beta = 3.5;

    double norm = check_daxpby(n, alpha, incx, beta, incy);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * Fortran API specific test
 * Test daxpby by comparing it with dscal and daxpy.
 * Test with the following options:
 * 
 * Size of vectors x, y is 100
 * Stride of vector x is 1
 * Stride of vector y is 1
 * Scalar alpha is zero
 */
CTEST(daxpby, inc_x_1_inc_y_1_N_100_alpha_zero)
{
    blasint n = DATASIZE, incx = 1, incy = 1;
    double alpha = 0.0;
    double beta = 1.0;

    double norm = check_daxpby(n, alpha, incx, beta, incy);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * Fortran API specific test
 * Test daxpby by comparing it with dscal and daxpy.
 * Test with the following options:
 * 
 * Size of vectors x, y is 100
 * Stride of vector x is 1
 * Stride of vector y is 2
 * Scalar alpha is zero
*/
CTEST(daxpby, inc_x_1_inc_y_2_N_100_alpha_zero)
{
    blasint n = DATASIZE, incx = 1, incy = 2;
    double alpha = 0.0;
    double beta = 1.0;

    double norm = check_daxpby(n, alpha, incx, beta, incy);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * Fortran API specific test
 * Test daxpby by comparing it with dscal and daxpy.
 * Test with the following options:
 * 
 * Size of vectors x, y is 100
 * Stride of vector x is 1
 * Stride of vector y is 1
 * Scalar beta is zero
 */
CTEST(daxpby, inc_x_1_inc_y_1_N_100_beta_zero)
{
    blasint n = DATASIZE, incx = 1, incy = 1;
    double alpha = 1.0;
    double beta = 0.0;

    double norm = check_daxpby(n, alpha, incx, beta, incy);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * Fortran API specific test
 * Test daxpby by comparing it with dscal and daxpy.
 * Test with the following options:
 * 
 * Size of vectors x, y is 100
 * Stride of vector x is 2
 * Stride of vector y is 1
 * Scalar beta is zero
*/
CTEST(daxpby, inc_x_2_inc_y_1_N_100_beta_zero)
{
    blasint n = DATASIZE, incx = 2, incy = 1;
    double alpha = 1.0;
    double beta = 0.0;

    double norm = check_daxpby(n, alpha, incx, beta, incy);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * Fortran API specific test
 * Test daxpby by comparing it with dscal and daxpy.
 * Test with the following options:
 * 
 * Size of vectors x, y is 100
 * Stride of vector x is 1
 * Stride of vector y is 2
 * Scalar beta is zero
*/
CTEST(daxpby, inc_x_1_inc_y_2_N_100_beta_zero)
{
    blasint n = DATASIZE, incx = 1, incy = 2;
    double alpha = 1.0;
    double beta = 0.0;

    double norm = check_daxpby(n, alpha, incx, beta, incy);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * Fortran API specific test
 * Test daxpby by comparing it with dscal and daxpy.
 * Test with the following options:
 * 
 * Size of vectors x, y is 100
 * Stride of vector x is 2
 * Stride of vector y is 2
 * Scalar beta is zero
*/
CTEST(daxpby, inc_x_2_inc_y_2_N_100_beta_zero)
{
    blasint n = DATASIZE, incx = 2, incy = 2;
    double alpha = 1.0;
    double beta = 0.0;

    double norm = check_daxpby(n, alpha, incx, beta, incy);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * Fortran API specific test
 * Test daxpby by comparing it with dscal and daxpy.
 * Test with the following options:
 * 
 * Size of vectors x, y is 100
 * Stride of vector x is 1
 * Stride of vector y is 1
 * Scalar alpha is zero
 * Scalar beta is zero
 */
CTEST(daxpby, inc_x_1_inc_y_1_N_100_alpha_beta_zero)
{
    blasint n = DATASIZE, incx = 1, incy = 1;
    double alpha = 0.0;
    double beta = 0.0;

    double norm = check_daxpby(n, alpha, incx, beta, incy);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * Fortran API specific test
 * Test daxpby by comparing it with dscal and daxpy.
 * Test with the following options:
 * 
 * Size of vectors x, y is 100
 * Stride of vector x is 1
 * Stride of vector y is 2
 * Scalar alpha is zero
 * Scalar beta is zero
*/
CTEST(daxpby, inc_x_1_inc_y_2_N_100_alpha_beta_zero)
{
    blasint n = DATASIZE, incx = 1, incy = 2;
    double alpha = 0.0;
    double beta = 0.0;

    double norm = check_daxpby(n, alpha, incx, beta, incy);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * Fortran API specific test
 * Check if n - size of vectors x, y is zero
 */
CTEST(daxpby, check_n_zero)
{
    blasint n = 0, incx = 1, incy = 1;
    double alpha = 1.0;
    double beta = 1.0;

    double norm = check_daxpby(n, alpha, incx, beta, incy);
    
    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

#ifndef NO_CBLAS
/**
 * C API specific test
 * Test daxpby by comparing it with dscal and daxpy.
 * Test with the following options:
 * 
 * Size of vectors x, y is 100
 * Stride of vector x is 1
 * Stride of vector y is 1
 */
CTEST(daxpby, c_api_inc_x_1_inc_y_1_N_100)
{
    blasint n = DATASIZE, incx = 1, incy = 1;
    double alpha = 1.0;
    double beta = 1.0;

    double norm = c_api_check_daxpby(n, alpha, incx, beta, incy);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * C API specific test
 * Test daxpby by comparing it with dscal and daxpy.
 * Test with the following options:
 * 
 * Size of vectors x, y is 100
 * Stride of vector x is 2
 * Stride of vector y is 1
 */
CTEST(daxpby, c_api_inc_x_2_inc_y_1_N_100)
{
    blasint n = DATASIZE, incx = 2, incy = 1;
    double alpha = 2.0;
    double beta = 1.0;

    double norm = c_api_check_daxpby(n, alpha, incx, beta, incy);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * C API specific test
 * Test daxpby by comparing it with dscal and daxpy.
 * Test with the following options:
 * 
 * Size of vectors x, y is 100
 * Stride of vector x is 1
 * Stride of vector y is 2
 */
CTEST(daxpby, c_api_inc_x_1_inc_y_2_N_100)
{
    blasint n = DATASIZE, incx = 1, incy = 2;
    double alpha = 1.0;
    double beta = 2.0;

    double norm = c_api_check_daxpby(n, alpha, incx, beta, incy);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * C API specific test
 * Test daxpby by comparing it with dscal and daxpy.
 * Test with the following options:
 * 
 * Size of vectors x, y is 100
 * Stride of vector x is 2
 * Stride of vector y is 2
 */
CTEST(daxpby, c_api_inc_x_2_inc_y_2_N_100)
{
    blasint n = DATASIZE, incx = 2, incy = 2;
    double alpha = 3.0;
    double beta = 4.0;

    double norm = c_api_check_daxpby(n, alpha, incx, beta, incy);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * C API specific test
 * Test daxpby by comparing it with dscal and daxpy.
 * Test with the following options:
 * 
 * Size of vectors x, y is 100
 * Stride of vector x is -1
 * Stride of vector y is 2
 */
CTEST(daxpby, c_api_inc_x_neg_1_inc_y_2_N_100)
{
    blasint n = DATASIZE, incx = -1, incy = 2;
    double alpha = 5.0;
    double beta = 4.0;

    double norm = c_api_check_daxpby(n, alpha, incx, beta, incy);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * C API specific test
 * Test daxpby by comparing it with dscal and daxpy.
 * Test with the following options:
 * 
 * Size of vectors x, y is 100
 * Stride of vector x is 2
 * Stride of vector y is -1
 */
CTEST(daxpby, c_api_inc_x_2_inc_y_neg_1_N_100)
{
    blasint n = DATASIZE, incx = 2, incy = -1;
    double alpha = 1.0;
    double beta = 6.0;

    double norm = c_api_check_daxpby(n, alpha, incx, beta, incy);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * C API specific test
 * Test daxpby by comparing it with dscal and daxpy.
 * Test with the following options:
 * 
 * Size of vectors x, y is 100
 * Stride of vector x is -2
 * Stride of vector y is -1
 */
CTEST(daxpby, c_api_inc_x_neg_2_inc_y_neg_1_N_100)
{
    blasint n = DATASIZE, incx = -2, incy = -1;
    double alpha = 7.0;
    double beta = 3.5;

    double norm = c_api_check_daxpby(n, alpha, incx, beta, incy);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * C API specific test
 * Test daxpby by comparing it with dscal and daxpy.
 * Test with the following options:
 * 
 * Size of vectors x, y is 100
 * Stride of vector x is 1
 * Stride of vector y is 1
 * Scalar alpha is zero
 */
CTEST(daxpby, c_api_inc_x_1_inc_y_1_N_100_alpha_zero)
{
    blasint n = DATASIZE, incx = 1, incy = 1;
    double alpha = 0.0;
    double beta = 1.0;

    double norm = c_api_check_daxpby(n, alpha, incx, beta, incy);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * C API specific test
 * Test daxpby by comparing it with dscal and daxpy.
 * Test with the following options:
 * 
 * Size of vectors x, y is 100
 * Stride of vector x is 1
 * Stride of vector y is 2
 * Scalar alpha is zero
*/
CTEST(daxpby, c_api_inc_x_1_inc_y_2_N_100_alpha_zero)
{
    blasint n = DATASIZE, incx = 1, incy = 2;
    double alpha = 0.0;
    double beta = 1.0;

    double norm = c_api_check_daxpby(n, alpha, incx, beta, incy);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * C API specific test
 * 
 * Test daxpby by comparing it with dscal and daxpy.
 * Test with the following options:
 * 
 * Size of vectors x, y is 100
 * Stride of vector x is 1
 * Stride of vector y is 1
 * Scalar beta is zero
 */
CTEST(daxpby, c_api_inc_x_1_inc_y_1_N_100_beta_zero)
{
    blasint n = DATASIZE, incx = 1, incy = 1;
    double alpha = 1.0;
    double beta = 0.0;

    double norm = c_api_check_daxpby(n, alpha, incx, beta, incy);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * C API specific test
 * Test daxpby by comparing it with dscal and daxpy.
 * Test with the following options:
 * 
 * Size of vectors x, y is 100
 * Stride of vector x is 2
 * Stride of vector y is 1
 * Scalar beta is zero
*/
CTEST(daxpby, c_api_inc_x_2_inc_y_1_N_100_beta_zero)
{
    blasint n = DATASIZE, incx = 2, incy = 1;
    double alpha = 1.0;
    double beta = 0.0;

    double norm = c_api_check_daxpby(n, alpha, incx, beta, incy);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * C API specific test
 * 
 * Test daxpby by comparing it with dscal and daxpy.
 * Test with the following options:
 * 
 * Size of vectors x, y is 100
 * Stride of vector x is 1
 * Stride of vector y is 2
 * Scalar beta is zero
*/
CTEST(daxpby, c_api_inc_x_1_inc_y_2_N_100_beta_zero)
{
    blasint n = DATASIZE, incx = 1, incy = 2;
    double alpha = 1.0;
    double beta = 0.0;

    double norm = c_api_check_daxpby(n, alpha, incx, beta, incy);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * C API specific test
 * 
 * Test daxpby by comparing it with dscal and daxpy.
 * Test with the following options:
 * 
 * Size of vectors x, y is 100
 * Stride of vector x is 2
 * Stride of vector y is 2
 * Scalar beta is zero
*/
CTEST(daxpby, c_api_inc_x_2_inc_y_2_N_100_beta_zero)
{
    blasint n = DATASIZE, incx = 2, incy = 2;
    double alpha = 1.0;
    double beta = 0.0;

    double norm = c_api_check_daxpby(n, alpha, incx, beta, incy);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * C API specific test
 * 
 * Test daxpby by comparing it with dscal and daxpy.
 * Test with the following options:
 * 
 * Size of vectors x, y is 100
 * Stride of vector x is 1
 * Stride of vector y is 1
 * Scalar alpha is zero
 * Scalar beta is zero
 */
CTEST(daxpby, c_api_inc_x_1_inc_y_1_N_100_alpha_beta_zero)
{
    blasint n = DATASIZE, incx = 1, incy = 1;
    double alpha = 0.0;
    double beta = 0.0;

    double norm = c_api_check_daxpby(n, alpha, incx, beta, incy);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * C API specific test
 * Test daxpby by comparing it with dscal and daxpy.
 * Test with the following options:
 * 
 * Size of vectors x, y is 100
 * Stride of vector x is 1
 * Stride of vector y is 2
 * Scalar alpha is zero
 * Scalar beta is zero
*/
CTEST(daxpby, c_api_inc_x_1_inc_y_2_N_100_alpha_beta_zero)
{
    blasint n = DATASIZE, incx = 1, incy = 2;
    double alpha = 0.0;
    double beta = 0.0;

    double norm = c_api_check_daxpby(n, alpha, incx, beta, incy);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * C API specific test
 * Check if n - size of vectors x, y is zero
 */
CTEST(daxpby, c_api_check_n_zero)
{
    blasint n = 0, incx = 1, incy = 1;
    double alpha = 1.0;
    double beta = 1.0;

    double norm = c_api_check_daxpby(n, alpha, incx, beta, incy);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}
#endif
#endif
