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

struct DATA_CSCAL {
    float x_test[DATASIZE * 2 * INCREMENT];
    float x_verify[DATASIZE * 2 * INCREMENT];
};

#ifdef BUILD_COMPLEX
static struct DATA_CSCAL data_cscal;

/**
 * cscal reference code
 *
 * param n - number of elements of vector x
 * param alpha - scaling factor for the vector product
 * param x - buffer holding input vector x
 * param inc - stride of vector x
 */
static void cscal_trusted(blasint n, float *alpha, float* x, blasint inc){
    blasint i, ip = 0;
    blasint inc_x2 = 2 * inc;
    float temp;
    for (i = 0; i < n; i++)
	{
        temp = alpha[0] * x[ip] - alpha[1] * x[ip+1];
		x[ip+1] = alpha[0] * x[ip+1] + alpha[1] * x[ip];
        x[ip] = temp;
        ip += inc_x2;
    }
}

/**
 * Comapare results computed by cscal and cscal_trusted
 *
 * param api specifies tested api (C or Fortran)
 * param n - number of elements of vector x
 * param alpha - scaling factor for the vector product
 * param inc - stride of vector x
 * return norm of differences
 */
static float check_cscal(char api, blasint n, float *alpha, blasint inc)
{
    blasint i;

    // Fill vectors a 
    srand_generate(data_cscal.x_test, n * inc * 2);

    // Copy vector x for cscal_trusted
    for (i = 0; i < n * 2 * inc; i++)
        data_cscal.x_verify[i] = data_cscal.x_test[i];

    cscal_trusted(n, alpha, data_cscal.x_verify, inc);

    if(api == 'F')
        BLASFUNC(cscal)(&n, alpha, data_cscal.x_test, &inc);
#ifndef NO_CBLAS
    else
        cblas_cscal(n, alpha, data_cscal.x_test, inc);
#endif

    // Find the differences between output vector computed by cscal and cscal_trusted
    for (i = 0; i < n * 2 * inc; i++)
        data_cscal.x_verify[i] -= data_cscal.x_test[i];

    // Find the norm of differences
    return BLASFUNC(scnrm2)(&n, data_cscal.x_verify, &inc);
}

/**
 * Fortran API specific test
 * Test cscal by comparing it against reference
 */
CTEST(cscal, alpha_r_zero_alpha_i_not_zero)
{
    blasint N = DATASIZE;
    blasint inc = 1;
    float alpha[2] = {0.0f, 1.0f};

    float norm = check_cscal('F', N, alpha, inc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * Fortran API specific test
 * Test cscal by comparing it against reference
 */
CTEST(cscal, alpha_r_zero_alpha_i_zero_inc_2)
{
    blasint N = DATASIZE;
    blasint inc = 2;
    float alpha[2] = {0.0f, 0.0f};

    float norm = check_cscal('F', N, alpha, inc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

#ifndef NO_CBLAS
/**
 * C API specific test
 * Test cscal by comparing it against reference
 */
CTEST(cscal, c_api_alpha_r_zero_alpha_i_not_zero)
{
    blasint N = DATASIZE;
    blasint inc = 1;
    float alpha[2] = {0.0f, 1.0f};

    float norm = check_cscal('C', N, alpha, inc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * C API specific test
 * Test cscal by comparing it against reference
 */
CTEST(cscal, c_api_alpha_r_zero_alpha_i_zero_inc_2)
{
    blasint N = DATASIZE;
    blasint inc = 2;
    float alpha[2] = {0.0f, 0.0f};

    float norm = check_cscal('C', N, alpha, inc);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}
#endif
#endif
