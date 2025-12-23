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

struct DATA_ZROT {
    double x_test[DATASIZE * INCREMENT * 2];
    double y_test[DATASIZE * INCREMENT * 2];
    double x_verify[DATASIZE * INCREMENT * 2];
    double y_verify[DATASIZE * INCREMENT * 2];
};

#ifdef BUILD_COMPLEX16
static struct DATA_ZROT data_zrot;

/**
 * Comapare results computed by zdrot and zaxpby 
 * 
 * param n specifies size of vector x
 * param inc_x specifies increment of vector x
 * param inc_y specifies increment of vector y
 * param c specifies cosine
 * param s specifies sine
 * return norm of differences 
 */
static double check_zdrot(blasint n, blasint inc_x, blasint inc_y, double *c, double *s)
{
    blasint i;
    double norm = 0;
    double s_neg[] = {-s[0], s[1]};

    blasint inc_x_abs = labs(inc_x);
    blasint inc_y_abs = labs(inc_y);

    // Fill vectors x, y
    drand_generate(data_zrot.x_test, n * inc_x_abs * 2);
    drand_generate(data_zrot.y_test, n * inc_y_abs * 2);

    if (inc_x == 0 && inc_y == 0) {
        drand_generate(data_zrot.x_test, n * 2);
        drand_generate(data_zrot.y_test, n * 2);
    }

    // Copy vector x for zaxpby
    for (i = 0; i < n * inc_x_abs * 2; i++)
        data_zrot.x_verify[i] = data_zrot.x_test[i];

    // Copy vector y for zaxpby
    for (i = 0; i < n * inc_y_abs * 2; i++)
        data_zrot.y_verify[i] = data_zrot.y_test[i];
    
    // Find cx = c*x + s*y
    BLASFUNC(zaxpby)(&n, s, data_zrot.y_test, &inc_y, c, data_zrot.x_verify, &inc_x);

    // Find cy = -conjg(s)*x + c*y
    BLASFUNC(zaxpby)(&n, s_neg, data_zrot.x_test, &inc_x, c, data_zrot.y_verify, &inc_y);

    BLASFUNC(zdrot)(&n, data_zrot.x_test, &inc_x, data_zrot.y_test, &inc_y, c, s);

    // Find the differences between vector x caculated by zaxpby and zdrot
    for (i = 0; i < n * 2 * inc_x_abs; i++)
        data_zrot.x_test[i] -= data_zrot.x_verify[i];

    // Find the differences between vector y caculated by zaxpby and zdrot
    for (i = 0; i < n * 2 * inc_y_abs; i++)
        data_zrot.y_test[i] -= data_zrot.y_verify[i];

    // Find the norm of differences
    norm += BLASFUNC(dznrm2)(&n, data_zrot.x_test, &inc_x_abs);
    norm += BLASFUNC(dznrm2)(&n, data_zrot.y_test, &inc_y_abs);
    return (norm / 2);
}

#ifndef NO_CBLAS
/**
 * C API specific function
 * Comapare results computed by zdrot and zaxpby 
 * 
 * param n specifies size of vector x
 * param inc_x specifies increment of vector x
 * param inc_y specifies increment of vector y
 * param c specifies cosine
 * param s specifies sine
 * return norm of differences 
 */
static double c_api_check_zdrot(blasint n, blasint inc_x, blasint inc_y, double *c, double *s)
{
    blasint i;
    double norm = 0;
    double s_neg[] = {-s[0], s[1]};

    blasint inc_x_abs = labs(inc_x);
    blasint inc_y_abs = labs(inc_y);

    // Fill vectors x, y
    drand_generate(data_zrot.x_test, n * inc_x_abs * 2);
    drand_generate(data_zrot.y_test, n * inc_y_abs * 2);

    if (inc_x == 0 && inc_y == 0) {
        drand_generate(data_zrot.x_test, n * 2);
        drand_generate(data_zrot.y_test, n * 2);
    }

    // Copy vector x for zaxpby
    for (i = 0; i < n * inc_x_abs * 2; i++)
        data_zrot.x_verify[i] = data_zrot.x_test[i];

    // Copy vector y for zaxpby
    for (i = 0; i < n * inc_y_abs * 2; i++)
        data_zrot.y_verify[i] = data_zrot.y_test[i];
    
    // Find cx = c*x + s*y
    cblas_zaxpby(n, s, data_zrot.y_test, inc_y, c, data_zrot.x_verify, inc_x);

    // Find cy = -conjg(s)*x + c*y
    cblas_zaxpby(n, s_neg, data_zrot.x_test, inc_x, c, data_zrot.y_verify, inc_y);

    cblas_zdrot(n, data_zrot.x_test, inc_x, data_zrot.y_test, inc_y, c[0], s[0]);

    // Find the differences between vector x caculated by zaxpby and zdrot
    for (i = 0; i < n * 2 * inc_x_abs; i++)
        data_zrot.x_test[i] -= data_zrot.x_verify[i];

    // Find the differences between vector y caculated by zaxpby and zdrot
    for (i = 0; i < n * 2 * inc_y_abs; i++)
        data_zrot.y_test[i] -= data_zrot.y_verify[i];

    // Find the norm of differences
    norm += cblas_dznrm2(n, data_zrot.x_test, inc_x_abs);
    norm += cblas_dznrm2(n, data_zrot.y_test, inc_y_abs);
    return (norm / 2);
}
#if 0
/**
 * Fortran API specific test
 * Test zrot by comparing it with zaxpby.
 * Test with the following options:
 * 
 * Size of vectors x, y is 100
 * Stride of vector x is 0
 * Stride of vector y is 0
 * c = 1.0
 * s = 2.0
 */
CTEST(zrot, inc_x_0_inc_y_0)
{
    blasint n = 100;
    
    blasint inc_x = 0;
    blasint inc_y = 0;

    // Imaginary  part for zaxpby
    double c[] = {1.0, 0.0};
    double s[] = {2.0, 0.0};

    double norm = check_zdrot(n, inc_x, inc_y, c, s);
    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}
#endif
/**
 * Fortran API specific test
 * Test zrot by comparing it with zaxpby.
 * Test with the following options:
 * 
 * Size of vectors x, y is 100
 * Stride of vector x is 1
 * Stride of vector y is 1
 * c = 1.0
 * s = 1.0
 */
CTEST(zrot, inc_x_1_inc_y_1)
{
    blasint n = 100;
    
    blasint inc_x = 1;
    blasint inc_y = 1;

    // Imaginary  part for zaxpby
    double c[] = {1.0, 0.0};
    double s[] = {1.0, 0.0};

    double norm = check_zdrot(n, inc_x, inc_y, c, s);
    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * Fortran API specific test
 * Test zrot by comparing it with zaxpby.
 * Test with the following options:
 * 
 * Size of vectors x, y is 100
 * Stride of vector x is -1
 * Stride of vector y is -1
 * c = 1.0
 * s = 1.0
 */
CTEST(zrot, inc_x_neg_1_inc_y_neg_1)
{
    blasint n = 100;
    
    blasint inc_x = -1;
    blasint inc_y = -1;

    // Imaginary  part for zaxpby
    double c[] = {1.0, 0.0};
    double s[] = {1.0, 0.0};

    double norm = check_zdrot(n, inc_x, inc_y, c, s);
    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * Fortran API specific test
 * Test zrot by comparing it with zaxpby.
 * Test with the following options:
 * 
 * Size of vectors x, y is 100
 * Stride of vector x is 2
 * Stride of vector y is 1
 * c = 3.0
 * s = 2.0
 */
CTEST(zrot, inc_x_2_inc_y_1)
{
    blasint n = 100;
    
    blasint inc_x = 2;
    blasint inc_y = 1;

    // Imaginary  part for zaxpby
    double c[] = {3.0, 0.0};
    double s[] = {2.0, 0.0};

    double norm = check_zdrot(n, inc_x, inc_y, c, s);
    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * Fortran API specific test
 * Test zrot by comparing it with zaxpby.
 * Test with the following options:
 * 
 * Size of vectors x, y is 100
 * Stride of vector x is -2
 * Stride of vector y is 1
 * c = 1.0
 * s = 1.0
 */
CTEST(zrot, inc_x_neg_2_inc_y_1)
{
    blasint n = 100;
    
    blasint inc_x = -2;
    blasint inc_y = 1;

    // Imaginary  part for zaxpby
    double c[] = {1.0, 0.0};
    double s[] = {1.0, 0.0};

    double norm = check_zdrot(n, inc_x, inc_y, c, s);
    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * Fortran API specific test
 * Test zrot by comparing it with zaxpby.
 * Test with the following options:
 * 
 * Size of vectors x, y is 100
 * Stride of vector x is 1
 * Stride of vector y is 2
 * c = 1.0
 * s = 1.0
 */
CTEST(zrot, inc_x_1_inc_y_2)
{
    blasint n = 100;
    
    blasint inc_x = 1;
    blasint inc_y = 2;

    // Imaginary  part for zaxpby
    double c[] = {1.0, 0.0};
    double s[] = {1.0, 0.0};

    double norm = check_zdrot(n, inc_x, inc_y, c, s);
    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * Fortran API specific test
 * Test zrot by comparing it with zaxpby.
 * Test with the following options:
 * 
 * Size of vectors x, y is 100
 * Stride of vector x is 1
 * Stride of vector y is -2
 * c = 2.0
 * s = 1.0
 */
CTEST(zrot, inc_x_1_inc_y_neg_2)
{
    blasint n = 100;
    
    blasint inc_x = 1;
    blasint inc_y = -2;

    // Imaginary  part for zaxpby
    double c[] = {2.0, 0.0};
    double s[] = {1.0, 0.0};

    double norm = check_zdrot(n, inc_x, inc_y, c, s);
    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * Fortran API specific test
 * Test zrot by comparing it with zaxpby.
 * Test with the following options:
 * 
 * Size of vectors x, y is 100
 * Stride of vector x is 2
 * Stride of vector y is 2
 * c = 1.0
 * s = 2.0
 */
CTEST(zrot, inc_x_2_inc_y_2)
{
    blasint n = 100;
    
    blasint inc_x = 2;
    blasint inc_y = 2;

    // Imaginary  part for zaxpby
    double c[] = {1.0, 0.0};
    double s[] = {2.0, 0.0};

    double norm = check_zdrot(n, inc_x, inc_y, c, s);
    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * Fortran API specific test
 * Test zrot by comparing it with zaxpby.
 * Test with the following options:
 * 
 * Size of vectors x, y is 100
 * Stride of vector x is 2
 * Stride of vector y is 2
 * c = 1.0
 * s = 1.0
 */
CTEST(zrot, inc_x_neg_2_inc_y_neg_2)
{
    blasint n = 100;
    
    blasint inc_x = -2;
    blasint inc_y = -2;

    // Imaginary  part for zaxpby
    double c[] = {1.0, 0.0};
    double s[] = {1.0, 0.0};

    double norm = check_zdrot(n, inc_x, inc_y, c, s);
    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * Fortran API specific test
 * Test zrot by comparing it with zaxpby.
 * Test with the following options:
 * 
 * Size of vectors x, y is 100
 * Stride of vector x is 2
 * Stride of vector y is 2
 * c = 0.0
 * s = 1.0
 */
CTEST(zrot, inc_x_2_inc_y_2_c_zero)
{
    blasint n = 100;
    
    blasint inc_x = 2;
    blasint inc_y = 2;

    // Imaginary  part for zaxpby
    double c[] = {0.0, 0.0};
    double s[] = {1.0, 0.0};

    double norm = check_zdrot(n, inc_x, inc_y, c, s);
    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * Fortran API specific test
 * Test zrot by comparing it with zaxpby.
 * Test with the following options:
 * 
 * Size of vectors x, y is 100
 * Stride of vector x is 2
 * Stride of vector y is 2
 * c = 1.0
 * s = 0.0
 */
CTEST(zrot, inc_x_2_inc_y_2_s_zero)
{
    blasint n = 100;
    
    blasint inc_x = 2;
    blasint inc_y = 2;

    // Imaginary  part for zaxpby
    double c[] = {1.0, 0.0};
    double s[] = {0.0, 0.0};

    double norm = check_zdrot(n, inc_x, inc_y, c, s);
    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * Fortran API specific test
 * Test zrot by comparing it with zaxpby.
 * Test with the following options:
 * 
 * Size of vectors x, y is 0
 * Stride of vector x is 1
 * Stride of vector y is 1
 * c = 1.0
 * s = 1.0
 */
CTEST(zrot, check_n_zero)
{
    blasint n = 0;
    
    blasint inc_x = 1;
    blasint inc_y = 1;

    // Imaginary  part for zaxpby
    double c[] = {1.0, 0.0};
    double s[] = {1.0, 0.0};

    double norm = check_zdrot(n, inc_x, inc_y, c, s);
    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}
#if 0
/**
 * C API specific test 
 * Test zrot by comparing it with zaxpby.
 * Test with the following options:
 * 
 * Size of vectors x, y is 100
 * Stride of vector x is 0
 * Stride of vector y is 0
 * c = 1.0
 * s = 2.0
 */
CTEST(zrot, c_api_inc_x_0_inc_y_0)
{
    blasint n = 100;
    
    blasint inc_x = 0;
    blasint inc_y = 0;

    // Imaginary  part for zaxpby
    double c[] = {3.0, 0.0};
    double s[] = {2.0, 0.0};

    double norm = c_api_check_zdrot(n, inc_x, inc_y, c, s);
    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}
#endif
/**
 * C API specific test
 * Test zrot by comparing it with zaxpby.
 * Test with the following options:
 * 
 * Size of vectors x, y is 100
 * Stride of vector x is 1
 * Stride of vector y is 1
 * c = 1.0
 * s = 1.0
 */
CTEST(zrot, c_api_inc_x_1_inc_y_1)
{
    blasint n = 100;
    
    blasint inc_x = 1;
    blasint inc_y = 1;

    // Imaginary  part for zaxpby
    double c[] = {1.0, 0.0};
    double s[] = {1.0, 0.0};

    double norm = c_api_check_zdrot(n, inc_x, inc_y, c, s);
    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * C API specific test
 * Test zrot by comparing it with zaxpby.
 * Test with the following options:
 * 
 * Size of vectors x, y is 100
 * Stride of vector x is -1
 * Stride of vector y is -1
 * c = 1.0
 * s = 1.0
 */
CTEST(zrot, c_api_inc_x_neg_1_inc_y_neg_1)
{
    blasint n = 100;
    
    blasint inc_x = -1;
    blasint inc_y = -1;

    // Imaginary  part for zaxpby
    double c[] = {1.0, 0.0};
    double s[] = {1.0, 0.0};

    double norm = c_api_check_zdrot(n, inc_x, inc_y, c, s);
    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * C API specific test
 * Test zrot by comparing it with zaxpby.
 * Test with the following options:
 * 
 * Size of vectors x, y is 100
 * Stride of vector x is 2
 * Stride of vector y is 1
 * c = 3.0
 * s = 2.0
 */
CTEST(zrot, c_api_inc_x_2_inc_y_1)
{
    blasint n = 100;
    
    blasint inc_x = 2;
    blasint inc_y = 1;

    // Imaginary  part for zaxpby
    double c[] = {3.0, 0.0};
    double s[] = {2.0, 0.0};

    double norm = c_api_check_zdrot(n, inc_x, inc_y, c, s);
    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * C API specific test
 * Test zrot by comparing it with zaxpby.
 * Test with the following options:
 * 
 * Size of vectors x, y is 100
 * Stride of vector x is -2
 * Stride of vector y is 1
 * c = 1.0
 * s = 1.0
 */
CTEST(zrot, c_api_inc_x_neg_2_inc_y_1)
{
    blasint n = 100;
    
    blasint inc_x = -2;
    blasint inc_y = 1;

    // Imaginary  part for zaxpby
    double c[] = {1.0, 0.0};
    double s[] = {1.0, 0.0};

    double norm = c_api_check_zdrot(n, inc_x, inc_y, c, s);
    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * C API specific test
 * Test zrot by comparing it with zaxpby.
 * Test with the following options:
 * 
 * Size of vectors x, y is 100
 * Stride of vector x is 1
 * Stride of vector y is 2
 * c = 1.0
 * s = 1.0
 */
CTEST(zrot, c_api_inc_x_1_inc_y_2)
{
    blasint n = 100;
    
    blasint inc_x = 1;
    blasint inc_y = 2;

    // Imaginary  part for zaxpby
    double c[] = {1.0, 0.0};
    double s[] = {1.0, 0.0};

    double norm = c_api_check_zdrot(n, inc_x, inc_y, c, s);
    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * C API specific test
 * Test zrot by comparing it with zaxpby.
 * Test with the following options:
 * 
 * Size of vectors x, y is 100
 * Stride of vector x is 1
 * Stride of vector y is -2
 * c = 2.0
 * s = 1.0
 */
CTEST(zrot, c_api_inc_x_1_inc_y_neg_2)
{
    blasint n = 100;
    
    blasint inc_x = 1;
    blasint inc_y = -2;

    // Imaginary  part for zaxpby
    double c[] = {2.0, 0.0};
    double s[] = {1.0, 0.0};

    double norm = c_api_check_zdrot(n, inc_x, inc_y, c, s);
    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * C API specific test
 * Test zrot by comparing it with zaxpby.
 * Test with the following options:
 * 
 * Size of vectors x, y is 100
 * Stride of vector x is 2
 * Stride of vector y is 2
 * c = 1.0
 * s = 2.0
 */
CTEST(zrot, c_api_inc_x_2_inc_y_2)
{
    blasint n = 100;
    
    blasint inc_x = 2;
    blasint inc_y = 2;

    // Imaginary  part for zaxpby
    double c[] = {1.0, 0.0};
    double s[] = {2.0, 0.0};

    double norm = c_api_check_zdrot(n, inc_x, inc_y, c, s);
    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * C API specific test
 * Test zrot by comparing it with zaxpby.
 * Test with the following options:
 * 
 * Size of vectors x, y is 100
 * Stride of vector x is 2
 * Stride of vector y is 2
 * c = 1.0
 * s = 1.0
 */
CTEST(zrot, c_api_inc_x_neg_2_inc_y_neg_2)
{
    blasint n = 100;
    
    blasint inc_x = -2;
    blasint inc_y = -2;

    // Imaginary  part for zaxpby
    double c[] = {1.0, 0.0};
    double s[] = {1.0, 0.0};

    double norm = c_api_check_zdrot(n, inc_x, inc_y, c, s);
    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * C API specific test
 * Test zrot by comparing it with zaxpby.
 * Test with the following options:
 * 
 * Size of vectors x, y is 100
 * Stride of vector x is 2
 * Stride of vector y is 2
 * c = 0.0
 * s = 1.0
 */
CTEST(zrot, c_api_inc_x_2_inc_y_2_c_zero)
{
    blasint n = 100;
    
    blasint inc_x = 2;
    blasint inc_y = 2;

    // Imaginary  part for zaxpby
    double c[] = {0.0, 0.0};
    double s[] = {1.0, 0.0};

    double norm = c_api_check_zdrot(n, inc_x, inc_y, c, s);
    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * C API specific test
 * Test zrot by comparing it with zaxpby.
 * Test with the following options:
 * 
 * Size of vectors x, y is 100
 * Stride of vector x is 2
 * Stride of vector y is 2
 * c = 1.0
 * s = 0.0
 */
CTEST(zrot, c_api_inc_x_2_inc_y_2_s_zero)
{
    blasint n = 100;
    
    blasint inc_x = 2;
    blasint inc_y = 2;

    // Imaginary  part for zaxpby
    double c[] = {1.0, 0.0};
    double s[] = {0.0, 0.0};

    double norm = c_api_check_zdrot(n, inc_x, inc_y, c, s);
    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}

/**
 * C API specific test
 * Test zrot by comparing it with zaxpby.
 * Test with the following options:
 * 
 * Size of vectors x, y is 0
 * Stride of vector x is 1
 * Stride of vector y is 1
 * c = 1.0
 * s = 1.0
 */
CTEST(zrot, c_api_check_n_zero)
{
    blasint n = 0;
    
    blasint inc_x = 1;
    blasint inc_y = 1;

    // Imaginary  part for zaxpby
    double c[] = {1.0, 0.0};
    double s[] = {1.0, 0.0};

    double norm = c_api_check_zdrot(n, inc_x, inc_y, c, s);
    ASSERT_DBL_NEAR_TOL(0.0, norm, DOUBLE_EPS);
}
#endif
#endif
