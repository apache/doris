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
#include <cblas.h>
#include "common.h"

#define DATASIZE 100
#define INCREMENT 2

struct DATA_ZSCAL {
    double x_test[DATASIZE * 2 * INCREMENT];
    double x_verify[DATASIZE * 2 * INCREMENT];
};

#ifdef BUILD_COMPLEX16
static struct DATA_ZSCAL data_zscal;


/**
 * zscal reference code
 *
 * param n - number of elements of vector x
 * param alpha - scaling factor for the vector product
 * param x - buffer holding input vector x
 * param inc - stride of vector x
 */
static void zscal_trusted(blasint n, double *alpha, double* x, blasint inc){
    blasint i, ip = 0;
    blasint inc_x2 = 2 * inc;
    double temp;
    for (i = 0; i < n; i++)
	{
        temp = alpha[0] * x[ip] - alpha[1] * x[ip+1];
		x[ip+1] = alpha[0] * x[ip+1] + alpha[1] * x[ip];
        x[ip] = temp;
        ip += inc_x2;
    }
}

/**
 * Comapare results computed by zscal and zscal_trusted
 *
 * param api specifies tested api (C or Fortran)
 * param n - number of elements of vector x
 * param alpha - scaling factor for the vector product
 * param inc - stride of vector x
 * return norm of differences
 */
static double check_zscal(char api, blasint n, double *alpha, blasint inc)
{
    blasint i;

    // Fill vectors x
    drand_generate(data_zscal.x_test, n * inc * 2);

    // Copy vector x for zscal_trusted
    for (i = 0; i < n * 2 * inc; i++)
        data_zscal.x_verify[i] = data_zscal.x_test[i];

    zscal_trusted(n, alpha, data_zscal.x_verify, inc);

    if(api == 'F')
        BLASFUNC(zscal)(&n, alpha, data_zscal.x_test, &inc);
#ifndef NO_CBLAS
    else
        cblas_zscal(n, alpha, data_zscal.x_test, inc);
#endif

    // Find the differences between output vector computed by zscal and zscal_trusted
    for (i = 0; i < n * 2 * inc; i++)
        data_zscal.x_verify[i] -= data_zscal.x_test[i];

    // Find the norm of differences
    return BLASFUNC(dznrm2)(&n, data_zscal.x_verify, &inc);
}

/**
 * Fortran API specific test
 * Test zscal by comparing it against reference
 */
CTEST(zscal, alpha_r_zero_alpha_i_not_zero)
{
    blasint N = DATASIZE;
    blasint inc = 1;
    double alpha[2] = {0.0, 1.0};

    double norm = check_zscal('F', N, alpha, inc);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * Fortran API specific test
 * Test zscal by comparing it against reference
 */
CTEST(zscal, alpha_r_zero_alpha_i_zero_inc_2)
{
    blasint N = DATASIZE;
    blasint inc = 2;
    double alpha[2] = {0.0, 0.0};

    double norm = check_zscal('F', N, alpha, inc);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

#ifndef NO_CBLAS
/**
 * C API specific test
 * Test zscal by comparing it against reference
 */
CTEST(zscal, c_api_alpha_r_zero_alpha_i_not_zero)
{
    blasint N = DATASIZE;
    blasint inc = 1;
    double alpha[2] = {0.0, 1.0};

    double norm = check_zscal('C', N, alpha, inc);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * C API specific test
 * Test zscal by comparing it against reference
 */
CTEST(zscal, c_api_alpha_r_zero_alpha_i_zero_inc_2)
{
    blasint N = DATASIZE;
    blasint inc = 2;
    double alpha[2] = {0.0, 0.0};

    double norm = check_zscal('C', N, alpha, inc);

    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}
#endif
#endif
