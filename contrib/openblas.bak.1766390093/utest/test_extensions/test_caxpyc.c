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

struct DATA_CAXPYC {
    float x_test[DATASIZE * INCREMENT * 2];
    float x_verify[DATASIZE * INCREMENT * 2];
    float y_test[DATASIZE * INCREMENT * 2];
    float y_verify[DATASIZE * INCREMENT * 2];
};

#ifdef BUILD_COMPLEX
static struct DATA_CAXPYC data_caxpyc;

/**
 * Test caxpyc by conjugating vector x and comparing with caxpy.
 * Compare with the following options:
 *
 * param n - number of elements in vectors x and y
 * param alpha - scalar alpha
 * param incx - increment for the elements of x
 * param incy - increment for the elements of y
 * return norm of difference
 */
static float check_caxpyc(blasint n, float *alpha, blasint incx, blasint incy)
{
    blasint i;

    srand_generate(data_caxpyc.x_test, n * incx * 2);
    srand_generate(data_caxpyc.y_test, n * incy * 2);

    for (i = 0; i < n * incx * 2; i++)
        data_caxpyc.x_verify[i] = data_caxpyc.x_test[i];

    for (i = 0; i < n * incy * 2; i++)
        data_caxpyc.y_verify[i] = data_caxpyc.y_test[i];

    cconjugate_vector(n, incx, data_caxpyc.x_verify);

    BLASFUNC(caxpy)(&n, alpha, data_caxpyc.x_verify, &incx,
                    data_caxpyc.y_verify, &incy);

    BLASFUNC(caxpyc)(&n, alpha, data_caxpyc.x_test, &incx,
                     data_caxpyc.y_test, &incy);

    for (i = 0; i < n * incy * 2; i++)
        data_caxpyc.y_verify[i] -= data_caxpyc.y_test[i];

    return BLASFUNC(scnrm2)(&n, data_caxpyc.y_verify, &incy);
}

/**
 * Test caxpyc by conjugating vector x and comparing with caxpy.
 * Test with the following options:
 *
 * Size of vectors x, y is 100
 * Stride of vector x is 1
 * Stride of vector y is 1
 */
CTEST(caxpyc, conj_strides_one)
{
    blasint n = DATASIZE, incx = 1, incy = 1;
    float alpha[] = {5.0f, 2.2f};

    float norm = check_caxpyc(n, alpha, incx, incy);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * Test caxpyc by conjugating vector x and comparing with caxpy.
 * Test with the following options:
 *
 * Size of vectors x, y is 100
 * Stride of vector x is 1
 * Stride of vector y is 2
 */
CTEST(caxpyc, conj_incx_one)
{
    blasint n = DATASIZE, incx = 1, incy = 2;
    float alpha[] = {5.0f, 2.2f};

    float norm = check_caxpyc(n, alpha, incx, incy);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * Test caxpyc by conjugating vector x and comparing with caxpy.
 * Test with the following options:
 *
 * Size of vectors x, y is 100
 * Stride of vector x is 2
 * Stride of vector y is 1
 */
CTEST(caxpyc, conj_incy_one)
{
    blasint n = DATASIZE, incx = 2, incy = 1;
    float alpha[] = {5.0f, 2.2f};

    float norm = check_caxpyc(n, alpha, incx, incy);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}

/**
 * Test caxpyc by conjugating vector x and comparing with caxpy.
 * Test with the following options:
 *
 * Size of vectors x, y is 100
 * Stride of vector x is 2
 * Stride of vector y is 2
 */
CTEST(caxpyc, conj_strides_two)
{
    blasint n = DATASIZE, incx = 2, incy = 2;
    float alpha[] = {5.0f, 2.2f};

    float norm = check_caxpyc(n, alpha, incx, incy);

    ASSERT_DBL_NEAR_TOL(0.0f, norm, SINGLE_EPS);
}
#endif
