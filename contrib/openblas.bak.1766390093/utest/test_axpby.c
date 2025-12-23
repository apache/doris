/*****************************************************************************
Copyright (c) 2011-2024, The OpenBLAS Project
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

#include "openblas_utest.h"

#ifdef BUILD_SINGLE
CTEST(axpby, saxpby_inc_0)
{
    blasint i;
    blasint N = 9, incX = 0, incY = 0;
    float alpha = 1.0, beta = 2.0;
    float x1[] = { 1.0, 3.0, 5.0, 7.0, 1.0, 3.0, 5.0, 7.0, 9.0 };
    float y1[] = { 2.0, 4.0, 6.0, 8.0, 2.0, 4.0, 6.0, 8.0, 10.0 };

    BLASFUNC(saxpby)(&N, &alpha, x1, &incX, &beta, y1, &incY);

    float x2[] = { 1.0, 3.0, 5.0, 7.0, 1.0, 3.0, 5.0, 7.0, 9.0 };
    float y2[] = { 1535.0, 4.0, 6.0, 8.0, 2.0, 4.0, 6.0, 8.0, 10.0 };

    for(i = 0; i < N; i++){
        ASSERT_DBL_NEAR_TOL(x2[i], x1[i], SINGLE_EPS);
        ASSERT_DBL_NEAR_TOL(y2[i], y1[i], SINGLE_EPS);
    }
}

CTEST(axpby, saxpby_inc_1)
{
    blasint i;
    blasint N = 9, incX = 1, incY = 1;
    float alpha = 0.25, beta = 0.75;
    float x1[] = { 1.0, 3.0, 5.0, 7.0, 1.0, 3.0, 5.0, 7.0, 9.0 };
    float y1[] = { 2.0, 4.0, 6.0, 8.0, 2.0, 4.0, 6.0, 8.0, 10.0 };

    BLASFUNC(saxpby)(&N, &alpha, x1, &incX, &beta, y1, &incY);

    float x2[] = { 1.0, 3.0, 5.0, 7.0, 1.0, 3.0, 5.0, 7.0, 9.0 };
    float y2[] = { 1.75, 3.75, 5.75, 7.75, 1.75, 3.75, 5.75, 7.75, 9.75 };

    for(i = 0; i < N; i++){
        ASSERT_DBL_NEAR_TOL(x2[i], x1[i], SINGLE_EPS);
        ASSERT_DBL_NEAR_TOL(y2[i], y1[i], SINGLE_EPS);
    }
}

CTEST(axpby, saxpby_inc_2)
{
    blasint i;
    blasint N = 9, incX = 2, incY = 2;
    float alpha = 0.25, beta = 0.75;
    float x1[] = { 1.0, 3.0, 5.0, 7.0, 1.0, 3.0, 5.0, 7.0, 9.0,
                   1.0, 3.0, 5.0, 7.0, 1.0, 3.0, 5.0, 7.0, 9.0 };
    float y1[] = { 2.0, 4.0, 6.0, 8.0, 2.0, 4.0, 6.0, 8.0, 10.0,
                   2.0, 4.0, 6.0, 8.0, 2.0, 4.0, 6.0, 8.0, 10.0 };

    BLASFUNC(saxpby)(&N, &alpha, x1, &incX, &beta, y1, &incY);

    float x2[] = { 1.0, 3.0, 5.0, 7.0, 1.0, 3.0, 5.0, 7.0, 9.0,
                   1.0, 3.0, 5.0, 7.0, 1.0, 3.0, 5.0, 7.0, 9.0 };
    float y2[] = { 1.75, 4.00, 5.75, 8.00, 1.75, 4.00, 5.75, 8.00,
                   9.75, 2.00, 3.75, 6.00, 7.75, 2.00, 3.75, 6.00,
                   7.75, 10.00 };

    for(i = 0; i < 2 * N; i++){
        ASSERT_DBL_NEAR_TOL(x2[i], x1[i], SINGLE_EPS);
        ASSERT_DBL_NEAR_TOL(y2[i], y1[i], SINGLE_EPS);
    }
}
#endif

#ifdef BUILD_DOUBLE
CTEST(axpby, daxpby_inc_0)
{
    blasint i;
    blasint N = 9, incX = 0, incY = 0;
    double alpha = 1.0, beta  = 2.0;
    double x1[] = { 1.0, 3.0, 5.0, 7.0, 1.0, 3.0, 5.0, 7.0, 9.0 };
    double y1[] = { 2.0, 4.0, 6.0, 8.0, 2.0, 4.0, 6.0, 8.0, 10.0 };

    BLASFUNC(daxpby)(&N, &alpha, x1, &incX, &beta, y1, &incY);

    double x2[] = { 1.0, 3.0, 5.0, 7.0, 1.0, 3.0, 5.0, 7.0, 9.0 };
    double y2[] = { 1535.0, 4.0, 6.0, 8.0, 2.0, 4.0, 6.0, 8.0, 10.0 };

    for(i = 0; i < N; i++){
        ASSERT_DBL_NEAR_TOL(x2[i], x1[i], DOUBLE_EPS);
        ASSERT_DBL_NEAR_TOL(y2[i], y1[i], DOUBLE_EPS);
    }
}

CTEST(axpby, daxpby_inc_1)
{
    blasint i;
    blasint N = 9, incX = 1, incY = 1;
    double alpha = 0.25, beta = 0.75;
    double x1[] = { 1.0, 3.0, 5.0, 7.0, 1.0, 3.0, 5.0, 7.0, 9.0 };
    double y1[] = { 2.0, 4.0, 6.0, 8.0, 2.0, 4.0, 6.0, 8.0, 10.0 };

    BLASFUNC(daxpby)(&N, &alpha, x1, &incX, &beta, y1, &incY);

    double x2[] = { 1.0, 3.0, 5.0, 7.0, 1.0, 3.0, 5.0, 7.0, 9.0 };
    double y2[] = { 1.75, 3.75, 5.75, 7.75, 1.75, 3.75, 5.75, 7.75, 9.75 };

    for(i = 0; i < N; i++){
        ASSERT_DBL_NEAR_TOL(x2[i], x1[i], DOUBLE_EPS);
        ASSERT_DBL_NEAR_TOL(y2[i], y1[i], DOUBLE_EPS);
    }
}

CTEST(axpby, daxpby_inc_2)
{
    blasint i;
    blasint N = 9, incX = 2, incY = 2;
    double alpha = 0.25, beta = 0.75;
    double x1[] = { 1.0, 3.0, 5.0, 7.0, 1.0, 3.0, 5.0, 7.0, 9.0,
                    1.0, 3.0, 5.0, 7.0, 1.0, 3.0, 5.0, 7.0, 9.0 };
    double y1[] = { 2.0, 4.0, 6.0, 8.0, 2.0, 4.0, 6.0, 8.0, 10.0,
                    2.0, 4.0, 6.0, 8.0, 2.0, 4.0, 6.0, 8.0, 10.0 };

    BLASFUNC(daxpby)(&N, &alpha, x1, &incX, &beta, y1, &incY);

    double x2[] = { 1.0, 3.0, 5.0, 7.0, 1.0, 3.0, 5.0, 7.0, 9.0,
                    1.0, 3.0, 5.0, 7.0, 1.0, 3.0, 5.0, 7.0, 9.0 };
    double y2[] = { 1.75, 4.00, 5.75, 8.00, 1.75, 4.00, 5.75, 8.00,
                    9.75, 2.00, 3.75, 6.00, 7.75, 2.00, 3.75, 6.00,
                    7.75, 10.00 };

    for(i = 0; i < 2 * N; i++){
        ASSERT_DBL_NEAR_TOL(x2[i], x1[i], DOUBLE_EPS);
        ASSERT_DBL_NEAR_TOL(y2[i], y1[i], DOUBLE_EPS);
    }
}
#endif

#ifdef BUILD_COMPLEX
CTEST(axpby, caxpby_inc_0)
{
    blasint i;
    blasint N = 9, incX = 0, incY = 0;
    float alpha[] = { 1.0, 2.0 }, beta[] = { 2.0, 1.0 };
    float x1[] = { 1.0, 3.0, 5.0, 7.0, 1.0, 3.0, 5.0, 7.0, 9.0,
                   1.0, 3.0, 5.0, 7.0, 1.0, 3.0, 5.0, 7.0, 9.0 };
    float y1[] = { 2.0, 4.0, 6.0, 8.0, 2.0, 4.0, 6.0, 8.0, 10.0,
                   2.0, 4.0, 6.0, 8.0, 2.0, 4.0, 6.0, 8.0, 10.0 };

    BLASFUNC(caxpby)(&N, alpha, x1, &incX, beta, y1, &incY);

    float x2[] = { 1.0, 3.0, 5.0, 7.0, 1.0, 3.0, 5.0, 7.0, 9.0,
                   1.0, 3.0, 5.0, 7.0, 1.0, 3.0, 5.0, 7.0, 9.0 };
    float y2[] = { 9355.0, -8865.0, 6.0, 8.0, 2.0, 4.0, 6.0, 8.0,
                   10.0, 2.0, 4.0, 6.0, 8.0, 2.0, 4.0, 6.0, 8.0, 10.0 };

    for(i = 0; i < 2 * N; i++){
        ASSERT_DBL_NEAR_TOL(x2[i], x1[i], SINGLE_EPS);
        ASSERT_DBL_NEAR_TOL(y2[i], y1[i], SINGLE_EPS);
    }
}

CTEST(axpby, caxpby_inc_1)
{
    blasint i;
    blasint N = 9, incX = 1, incY = 1;
    float alpha[] = { 0.25, 0.25 }, beta[] = { 0.75, 0.75 };
    float x1[] = { 1.0, 3.0, 5.0, 7.0, 1.0, 3.0, 5.0, 7.0, 9.0,
                   1.0, 3.0, 5.0, 7.0, 1.0, 3.0, 5.0, 7.0, 9.0 };
    float y1[] = { 2.0, 4.0, 6.0, 8.0, 2.0, 4.0, 6.0, 8.0, 10.0,
                   2.0, 4.0, 6.0, 8.0, 2.0, 4.0, 6.0, 8.0, 10.0 };

    BLASFUNC(caxpby)(&N, alpha, x1, &incX, beta, y1, &incY);

    float x2[] = { 1.0, 3.0, 5.0, 7.0, 1.0, 3.0, 5.0, 7.0, 9.0,
                   1.0, 3.0, 5.0, 7.0, 1.0, 3.0, 5.0, 7.0, 9.0 };
    float y2[] = { -2.0, 5.5, -2.0, 13.5, -2.0, 5.5, -2.0, 13.5,
                   8.0, 11.5, -2.0, 9.5, 6.0, 9.5, -2.0, 9.5, -2.0, 17.5 };

    for(i = 0; i < 2 * N; i++){
        ASSERT_DBL_NEAR_TOL(x2[i], x1[i], SINGLE_EPS);
        ASSERT_DBL_NEAR_TOL(y2[i], y1[i], SINGLE_EPS);
    }
}

CTEST(axpby, caxpby_inc_2)
{
    blasint i;
    blasint N = 9, incX = 2, incY = 2;
    float alpha[] = { 0.25, 0.25 }, beta[] = { 0.75, 0.75 };
    float x1[] = { 1.0, 3.0, 5.0, 7.0, 1.0, 3.0, 5.0, 7.0, 9.0,
                   1.0, 3.0, 5.0, 7.0, 1.0, 3.0, 5.0, 7.0, 9.0,
                   1.0, 3.0, 5.0, 7.0, 1.0, 3.0, 5.0, 7.0, 9.0,
                   1.0, 3.0, 5.0, 7.0, 1.0, 3.0, 5.0, 7.0, 9.0 };
    float y1[] = { 2.0, 4.0, 6.0, 8.0, 2.0, 4.0, 6.0, 8.0, 10.0,
                   2.0, 4.0, 6.0, 8.0, 2.0, 4.0, 6.0, 8.0, 10.0,
                   2.0, 4.0, 6.0, 8.0, 2.0, 4.0, 6.0, 8.0, 10.0,
                   2.0, 4.0, 6.0, 8.0, 2.0, 4.0, 6.0, 8.0, 10.0 };

    BLASFUNC(caxpby)(&N, &alpha, x1, &incX, &beta, y1, &incY);

    float x2[] = { 1.0, 3.0, 5.0, 7.0, 1.0, 3.0, 5.0, 7.0, 9.0,
                   1.0, 3.0, 5.0, 7.0, 1.0, 3.0, 5.0, 7.0, 9.0,
                   1.0, 3.0, 5.0, 7.0, 1.0, 3.0, 5.0, 7.0, 9.0,
                   1.0, 3.0, 5.0, 7.0, 1.0, 3.0, 5.0, 7.0, 9.0 };
    float y2[] = { -2.0, 5.5, 6.0, 8.0, -2.0, 5.5, 6.0, 8.0, 8.0,
                   11.5, 4.0, 6.0, 6.0, 9.5, 4.0, 6.0, -2.0, 17.5,
                   2.0, 4.0, -2.0, 13.5, 2.0, 4.0, -2.0, 13.5, 10.0,
                   2.0, -2.0, 9.5, 8.0, 2.0, -2.0, 9.5, 8.0, 10.0 };

    for(i = 0; i < 4 * N; i++){
        ASSERT_DBL_NEAR_TOL(x2[i], x1[i], SINGLE_EPS);
        ASSERT_DBL_NEAR_TOL(y2[i], y1[i], SINGLE_EPS);
    }
}
#endif

#ifdef BUILD_COMPLEX16
CTEST(axpby, zaxpby_inc_0)
{
    blasint i;
    blasint N = 9, incX = 0, incY = 0;
    double alpha[] = { 1.0, 2.0 }, beta[] = { 2.0, 1.0 };
    double x1[] = { 1.0, 3.0, 5.0, 7.0, 1.0, 3.0, 5.0, 7.0, 9.0,
                   1.0, 3.0, 5.0, 7.0, 1.0, 3.0, 5.0, 7.0, 9.0 };
    double y1[] = { 2.0, 4.0, 6.0, 8.0, 2.0, 4.0, 6.0, 8.0, 10.0,
                   2.0, 4.0, 6.0, 8.0, 2.0, 4.0, 6.0, 8.0, 10.0 };

    BLASFUNC(zaxpby)(&N, alpha, x1, &incX, beta, y1, &incY);

    double x2[] = { 1.0, 3.0, 5.0, 7.0, 1.0, 3.0, 5.0, 7.0, 9.0,
                   1.0, 3.0, 5.0, 7.0, 1.0, 3.0, 5.0, 7.0, 9.0 };
    double y2[] = { 9355.0, -8865.0, 6.0, 8.0, 2.0, 4.0, 6.0, 8.0,
                   10.0, 2.0, 4.0, 6.0, 8.0, 2.0, 4.0, 6.0, 8.0, 10.0 };

    for(i = 0; i < 2 * N; i++){
        ASSERT_DBL_NEAR_TOL(x2[i], x1[i], DOUBLE_EPS);
        ASSERT_DBL_NEAR_TOL(y2[i], y1[i], DOUBLE_EPS);
    }
}

CTEST(axpby, zaxpby_inc_1)
{
    blasint i;
    blasint N = 9, incX = 1, incY = 1;
    double alpha[] = { 0.25, 0.25 }, beta[] = { 0.75, 0.75 };
    double x1[] = { 1.0, 3.0, 5.0, 7.0, 1.0, 3.0, 5.0, 7.0, 9.0,
                   1.0, 3.0, 5.0, 7.0, 1.0, 3.0, 5.0, 7.0, 9.0 };
    double y1[] = { 2.0, 4.0, 6.0, 8.0, 2.0, 4.0, 6.0, 8.0, 10.0,
                   2.0, 4.0, 6.0, 8.0, 2.0, 4.0, 6.0, 8.0, 10.0 };

    BLASFUNC(zaxpby)(&N, alpha, x1, &incX, beta, y1, &incY);

    double x2[] = { 1.0, 3.0, 5.0, 7.0, 1.0, 3.0, 5.0, 7.0, 9.0,
                   1.0, 3.0, 5.0, 7.0, 1.0, 3.0, 5.0, 7.0, 9.0 };
    double y2[] = { -2.0, 5.5, -2.0, 13.5, -2.0, 5.5, -2.0, 13.5,
                   8.0, 11.5, -2.0, 9.5, 6.0, 9.5, -2.0, 9.5, -2.0, 17.5 };

    for(i = 0; i < 2 * N; i++){
        ASSERT_DBL_NEAR_TOL(x2[i], x1[i], DOUBLE_EPS);
        ASSERT_DBL_NEAR_TOL(y2[i], y1[i], DOUBLE_EPS);
    }
}

CTEST(axpby, zaxpby_inc_2)
{
    blasint i;
    blasint N = 9, incX = 2, incY = 2;
    double alpha[] = { 0.25, 0.25 }, beta[] = { 0.75, 0.75 };
    double x1[] = { 1.0, 3.0, 5.0, 7.0, 1.0, 3.0, 5.0, 7.0, 9.0,
                   1.0, 3.0, 5.0, 7.0, 1.0, 3.0, 5.0, 7.0, 9.0,
                   1.0, 3.0, 5.0, 7.0, 1.0, 3.0, 5.0, 7.0, 9.0,
                   1.0, 3.0, 5.0, 7.0, 1.0, 3.0, 5.0, 7.0, 9.0 };
    double y1[] = { 2.0, 4.0, 6.0, 8.0, 2.0, 4.0, 6.0, 8.0, 10.0,
                   2.0, 4.0, 6.0, 8.0, 2.0, 4.0, 6.0, 8.0, 10.0,
                   2.0, 4.0, 6.0, 8.0, 2.0, 4.0, 6.0, 8.0, 10.0,
                   2.0, 4.0, 6.0, 8.0, 2.0, 4.0, 6.0, 8.0, 10.0 };

    BLASFUNC(zaxpby)(&N, &alpha, x1, &incX, &beta, y1, &incY);

    double x2[] = { 1.0, 3.0, 5.0, 7.0, 1.0, 3.0, 5.0, 7.0, 9.0,
                   1.0, 3.0, 5.0, 7.0, 1.0, 3.0, 5.0, 7.0, 9.0,
                   1.0, 3.0, 5.0, 7.0, 1.0, 3.0, 5.0, 7.0, 9.0,
                   1.0, 3.0, 5.0, 7.0, 1.0, 3.0, 5.0, 7.0, 9.0 };
    double y2[] = { -2.0, 5.5, 6.0, 8.0, -2.0, 5.5, 6.0, 8.0, 8.0,
                   11.5, 4.0, 6.0, 6.0, 9.5, 4.0, 6.0, -2.0, 17.5,
                   2.0, 4.0, -2.0, 13.5, 2.0, 4.0, -2.0, 13.5, 10.0,
                   2.0, -2.0, 9.5, 8.0, 2.0, -2.0, 9.5, 8.0, 10.0 };

    for(i = 0; i < 4 * N; i++){
        ASSERT_DBL_NEAR_TOL(x2[i], x1[i], DOUBLE_EPS);
        ASSERT_DBL_NEAR_TOL(y2[i], y1[i], DOUBLE_EPS);
    }
}
#endif
