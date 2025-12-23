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

struct DATA_ZAXPYC {
	double x_test[DATASIZE * INCREMENT * 2];
	double x_verify[DATASIZE * INCREMENT * 2];
	double y_test[DATASIZE * INCREMENT * 2];
	double y_verify[DATASIZE * INCREMENT * 2];
};
#ifdef BUILD_COMPLEX16
static struct DATA_ZAXPYC data_zaxpyc;

/**
 * Test zaxpyc by conjugating vector x and comparing with zaxpy.
 * Compare with the following options:
 *
 * param n - number of elements in vectors x and y
 * param alpha - scalar alpha
 * param incx - increment for the elements of x
 * param incy - increment for the elements of y
 * return norm of difference
 */
static double check_zaxpyc(blasint n, double *alpha, blasint incx, blasint incy)
{
	blasint i;

	drand_generate(data_zaxpyc.x_test, n * incx * 2);
	drand_generate(data_zaxpyc.y_test, n * incy * 2);

	for (i = 0; i < n * incx * 2; i++)
		data_zaxpyc.x_verify[i] = data_zaxpyc.x_test[i];

	for (i = 0; i < n * incy * 2; i++)
		data_zaxpyc.y_verify[i] = data_zaxpyc.y_test[i];

	zconjugate_vector(n, incx, data_zaxpyc.x_verify);

	BLASFUNC(zaxpy)
	(&n, alpha, data_zaxpyc.x_verify, &incx,
	 data_zaxpyc.y_verify, &incy);

	BLASFUNC(zaxpyc)
	(&n, alpha, data_zaxpyc.x_test, &incx,
	 data_zaxpyc.y_test, &incy);

	for (i = 0; i < n * incy * 2; i++)
		data_zaxpyc.y_verify[i] -= data_zaxpyc.y_test[i];

	return BLASFUNC(dznrm2)(&n, data_zaxpyc.y_verify, &incy);
}

/**
 * Test zaxpyc by conjugating vector x and comparing with zaxpy.
 * Test with the following options:
 *
 * Size of vectors x, y is 100
 * Stride of vector x is 1
 * Stride of vector y is 1
 */
CTEST(zaxpyc, conj_strides_one)
{
	blasint n = DATASIZE, incx = 1, incy = 1;
	double alpha[] = {5.0, 2.2};

	double norm = check_zaxpyc(n, alpha, incx, incy);

	ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * Test zaxpyc by conjugating vector x and comparing with zaxpy.
 * Test with the following options:
 *
 * Size of vectors x, y is 100
 * Stride of vector x is 1
 * Stride of vector y is 2
 */
CTEST(zaxpyc, conj_incx_one)
{
	blasint n = DATASIZE, incx = 1, incy = 2;
	double alpha[] = {5.0, 2.2};

	double norm = check_zaxpyc(n, alpha, incx, incy);

	ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * Test zaxpyc by conjugating vector x and comparing with zaxpy.
 * Test with the following options:
 *
 * Size of vectors x, y is 100
 * Stride of vector x is 2
 * Stride of vector y is 1
 */
CTEST(zaxpyc, conj_incy_one)
{
	blasint n = DATASIZE, incx = 2, incy = 1;
	double alpha[] = {5.0, 2.2};

	double norm = check_zaxpyc(n, alpha, incx, incy);

	ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * Test zaxpyc by conjugating vector x and comparing with zaxpy.
 * Test with the following options:
 *
 * Size of vectors x, y is 100
 * Stride of vector x is 2
 * Stride of vector y is 2
 */
CTEST(zaxpyc, conj_strides_two)
{
	blasint n = DATASIZE, incx = 2, incy = 2;
	double alpha[] = {5.0, 2.2};

	double norm = check_zaxpyc(n, alpha, incx, incy);

	ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}
#endif
