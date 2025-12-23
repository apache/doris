#include "openblas_utest.h"
#include <cblas.h>

#ifndef NAN
#define NAN 0.0/0.0
#endif
#ifndef INFINITY
#define INFINITY 1.0/0.0
#endif

#ifdef BUILD_SINGLE

CTEST(sgemv, 0_nan_inf)
{
    int i;
    blasint N = 17;
    blasint incX = 1;
    blasint incY = 1;
    float alpha = 0.0;
    float beta = 0.0;
    char  trans = 'N';
    float A[17 * 17];
    float X[17];
    float Y[17];

    memset(A, 0, sizeof(A));
    memset(X, 0, sizeof(X));
    for (i = 0; i < (N - 1); i += 2)
    {
        Y[i]     = NAN;
        Y[i + 1] = INFINITY;
    }
    Y[N - 1] = NAN;
    BLASFUNC(sgemv)(&trans, &N, &N, &alpha, A, &N, X, &incX, &beta, Y, &incY);
    for (i = 0; i < N; i ++)
        ASSERT_TRUE(Y[i] == 0.0);
}

CTEST(sgemv, 0_nan_inf_incy_2)
{
    int i;
    blasint N = 17;
    blasint Ny = 33;
    blasint incX = 1;
    blasint incY = 2;
    float alpha = 0.0;
    float beta = 0.0;
    char  trans = 'N';
    float A[17 * 17];
    float X[17];
    float Y[33];
    float *ay = Y;

    memset(A, 0, sizeof(A));
    memset(X, 0, sizeof(X));
    memset(Y, 0, sizeof(Y));
    for (i = 0; i < (N - 1); i += 2)
    {
        ay[0]   = NAN;
        ay     += 2;
        ay[0]   = INFINITY;
        ay     += 2;
    }
    Y[Ny - 1] = NAN;
    BLASFUNC(sgemv)(&trans, &N, &N, &alpha, A, &N, X, &incX, &beta, Y, &incY);
    for (i = 0; i < Ny; i ++)
        ASSERT_TRUE(Y[i] == 0.0);
}

#endif

#ifdef BUILD_DOUBLE
CTEST(dgemv, 0_nan_inf)
{
    int i;
    blasint N = 17;
    blasint incX = 1;
    blasint incY = 1;
    double alpha = 0.0;
    double beta = 0.0;
    char  trans = 'N';
    double A[17 * 17];
    double X[17];
    double Y[17];

    memset(A, 0, sizeof(A));
    memset(X, 0, sizeof(X));
    for (i = 0; i < (N - 1); i += 2)
    {
        Y[i]     = NAN;
        Y[i + 1] = INFINITY;
    }
    Y[N - 1] = NAN;
    BLASFUNC(dgemv)(&trans, &N, &N, &alpha, A, &N, X, &incX, &beta, Y, &incY);
    for (i = 0; i < N; i ++)
        ASSERT_TRUE(Y[i] == 0.0);
}

CTEST(dgemv, 0_nan_inf_incy_2)
{
    int i;
    blasint N = 17;
    blasint Ny = 33;
    blasint incX = 1;
    blasint incY = 2;
    double alpha = 0.0;
    double beta = 0.0;
    char  trans = 'N';
    double A[17 * 17];
    double X[17];
    double Y[33];
    double *ay = Y;

    memset(A, 0, sizeof(A));
    memset(X, 0, sizeof(X));
    memset(Y, 0, sizeof(Y));
    for (i = 0; i < (N - 1); i += 2)
    {
        ay[0]   = NAN;
        ay     += 2;
        ay[0]   = INFINITY;
        ay     += 2;
    }
    Y[Ny - 1] = NAN;
    BLASFUNC(dgemv)(&trans, &N, &N, &alpha, A, &N, X, &incX, &beta, Y, &incY);
    for (i = 0; i < Ny; i ++)
        ASSERT_TRUE(Y[i] == 0.0);
}

#endif

#ifdef BUILD_COMPLEX

CTEST(cgemv, 0_nan_inf)
{
    int i;
    blasint N = 17;
    blasint incX = 1;
    blasint incY = 1;
    float alpha[2] = {0.0, 0.0};
    float beta[2] = {0.0, 0.0};
    char  trans = 'N';
    float A[17 * 17 * 4];
    float X[17 * 2];
    float Y[17 * 2];

    memset(A, 0, sizeof(A));
    memset(X, 0, sizeof(X));
    for (i = 0; i < (2 * N - 2); i += 4)
    {
        Y[i]     = NAN;
        Y[i + 1] = NAN;

        Y[i + 2] = INFINITY;
        Y[i + 3] = INFINITY;
    }
    Y[2 * N - 1] = NAN;
    Y[2 * N - 2] = NAN;
    BLASFUNC(cgemv)(&trans, &N, &N, alpha, A, &N, X, &incX, beta, Y, &incY);
    for (i = 0; i < 2 * N; i ++)
        ASSERT_TRUE(Y[i] == 0.0);
}

CTEST(cgemv, 0_nan_inf_incy_2)
{
    int i;
    blasint N = 17;
    blasint incX = 1;
    blasint incY = 2;
    float alpha[2] = {0.0, 0.0};
    float beta[2] = {0.0, 0.0};
    char  trans = 'N';
    float A[17 * 17 * 4];
    float X[17];
    float Y[17 * 2 * 2];
    float *ay = Y;

    memset(A, 0, sizeof(A));
    memset(X, 0, sizeof(X));
    memset(Y, 0, sizeof(Y));
    for (i = 0; i < (2 * N - 2); i += 4)
    {
        ay[0]   = NAN;
        ay[1]   = NAN;
        ay     += 4;
        ay[0]   = INFINITY;
        ay[1]   = INFINITY;
        ay     += 4;
    }
    Y[4 * N - 4] = NAN;
    Y[4 * N - 3] = NAN;
    BLASFUNC(cgemv)(&trans, &N, &N, alpha, A, &N, X, &incX, beta, Y, &incY);
    for (i = 0; i < 4 * N; i ++)
        ASSERT_TRUE(Y[i] == 0.0);
}

CTEST(cgemv, 0_2_nan_1_inf_1)
{
    int i;
    blasint N = 17;
    blasint incX = 1;
    blasint incY = 1;
    float alpha[2] = {0.0, 0.0};
    float beta[2] = {0.0, 2.0};
    char  trans = 'N';
    float A[17 * 17 * 4];
    float X[17 * 2];
    float Y[17 * 2];

    memset(A, 0, sizeof(A));
    memset(X, 0, sizeof(X));
    for (i = 0; i < (2 * N - 2); i += 4)
    {
        Y[i]     = NAN;
        Y[i + 1] = 1.0;

        Y[i + 2] = INFINITY;
        Y[i + 3] = 1.0;
    }
    Y[2 * N - 2] = NAN;
    Y[2 * N - 1] = 1.0;
    BLASFUNC(cgemv)(&trans, &N, &N, alpha, A, &N, X, &incX, beta, Y, &incY);
    for (i = 0; i < 2 * N; i += 2) {
        if ((i >> 1) % 2){
            ASSERT_TRUE(isnan(Y[i]));
            ASSERT_TRUE(isinf(Y[i + 1]));
        }
        else {
            ASSERT_TRUE(isnan(Y[i]));
            ASSERT_TRUE(isnan(Y[i + 1]));
        }
    }
}

CTEST(cgemv, 0_2_nan_1_inf_1_incy_2)
{
    int i;
    blasint N = 17;
    blasint incX = 1;
    blasint incY = 2;
    float alpha[2] = {0.0, 0.0};
    float beta[2] = {0.0, 2.0};
    char  trans = 'N';
    float A[17 * 17 * 4];
    float X[17];
    float Y[17 * 2 * 2];
    float *ay = Y;

    memset(A, 0, sizeof(A));
    memset(X, 0, sizeof(X));
    memset(Y, 0, sizeof(Y));
    for (i = 0; i < (2 * N - 2); i += 4)
    {
        ay[0]   = NAN;
        ay[1]   = 1.0;
        ay     += 4;
        ay[0]   = INFINITY;
        ay[1]   = 1.0;
        ay     += 4;
    }
    Y[4 * N - 4] = NAN;
    Y[4 * N - 3] = 1.0;
    BLASFUNC(cgemv)(&trans, &N, &N, alpha, A, &N, X, &incX, beta, Y, &incY);
    for (i = 0; i < 4 * N; i += 2) {
        if ((i >> 1) % 2) {
            ASSERT_TRUE(Y[i] == 0.0);
            ASSERT_TRUE(Y[i + 1] == 0.0);
        }
        else {
            if ((i >> 2) % 2) {
                ASSERT_TRUE(isnan(Y[i]));
                ASSERT_TRUE(isinf(Y[i + 1]));
            }
            else {
                ASSERT_TRUE(isnan(Y[i]));
                ASSERT_TRUE(isnan(Y[i + 1]));
            }
        }
    }
}

CTEST(cgemv, 2_0_nan_1_inf_1)
{
    int i;
    blasint N = 17;
    blasint incX = 1;
    blasint incY = 1;
    float alpha[2] = {0.0, 0.0};
    float beta[2] = {2.0, 0.0};
    char  trans = 'N';
    float A[17 * 17 * 4];
    float X[17 * 2];
    float Y[17 * 2];

    memset(A, 0, sizeof(A));
    memset(X, 0, sizeof(X));
    for (i = 0; i < (2 * N - 2); i += 4)
    {
        Y[i]     = NAN;
        Y[i + 1] = 1.0;

        Y[i + 2] = INFINITY;
        Y[i + 3] = 1.0;
    }
    Y[2 * N - 2] = NAN;
    Y[2 * N - 1] = 1.0;
    BLASFUNC(cgemv)(&trans, &N, &N, alpha, A, &N, X, &incX, beta, Y, &incY);
    for (i = 0; i < 2 * N; i += 2) {
        if ((i >> 1) % 2){
            ASSERT_TRUE(isinf(Y[i]));
            ASSERT_TRUE(isnan(Y[i + 1]));
        }
        else {
            ASSERT_TRUE(isnan(Y[i]));
            ASSERT_TRUE(isnan(Y[i + 1]));
        }
    }
}

CTEST(cgemv, 2_0_nan_1_inf_1_incy_2)
{
    int i;
    blasint N = 17;
    blasint incX = 1;
    blasint incY = 2;
    float alpha[2] = {0.0, 0.0};
    float beta[2] = {2.0, 0.0};
    char  trans = 'N';
    float A[17 * 17 * 4];
    float X[17];
    float Y[17 * 2 * 2];
    float *ay = Y;

    memset(A, 0, sizeof(A));
    memset(X, 0, sizeof(X));
    memset(Y, 0, sizeof(Y));
    for (i = 0; i < (2 * N - 2); i += 4)
    {
        ay[0]   = NAN;
        ay[1]   = 1.0;
        ay     += 4;
        ay[0]   = INFINITY;
        ay[1]   = 1.0;
        ay     += 4;
    }
    Y[4 * N - 4] = NAN;
    Y[4 * N - 3] = 1.0;
    BLASFUNC(cgemv)(&trans, &N, &N, alpha, A, &N, X, &incX, beta, Y, &incY);
    for (i = 0; i < 4 * N; i += 2) {
        if ((i >> 1) % 2) {
            ASSERT_TRUE(Y[i] == 0.0);
            ASSERT_TRUE(Y[i + 1] == 0.0);
        }
        else {
            if ((i >> 2) % 2) {
                ASSERT_TRUE(isinf(Y[i]));
                ASSERT_TRUE(isnan(Y[i + 1]));
            }
            else {
                ASSERT_TRUE(isnan(Y[i]));
                ASSERT_TRUE(isnan(Y[i + 1]));
            }
        }
    }
}

#endif

#ifdef BUILD_COMPLEX16

CTEST(zgemv, 0_nan_inf)
{
    int i;
    blasint N = 17;
    blasint incX = 1;
    blasint incY = 1;
    double alpha[2] = {0.0, 0.0};
    double beta[2] = {0.0, 0.0};
    char  trans = 'N';
    double A[17 * 17 * 4];
    double X[17 * 2];
    double Y[17 * 2];

    memset(A, 0, sizeof(A));
    memset(X, 0, sizeof(X));
    for (i = 0; i < (2 * N - 2); i += 4)
    {
        Y[i]     = NAN;
        Y[i + 1] = NAN;

        Y[i + 2] = INFINITY;
        Y[i + 3] = INFINITY;
    }
    Y[2 * N - 1] = NAN;
    Y[2 * N - 2] = NAN;
    BLASFUNC(zgemv)(&trans, &N, &N, alpha, A, &N, X, &incX, beta, Y, &incY);
    for (i = 0; i < 2 * N; i ++)
        ASSERT_TRUE(Y[i] == 0.0);
}

CTEST(zgemv, 0_nan_inf_incy_2)
{
    int i;
    blasint N = 17;
    blasint incX = 1;
    blasint incY = 2;
    double alpha[2] = {0.0, 0.0};
    double beta[2] = {0.0, 0.0};
    char  trans = 'N';
    double A[17 * 17 * 4];
    double X[17];
    double Y[17 * 2 * 2];
    double *ay = Y;

    memset(A, 0, sizeof(A));
    memset(X, 0, sizeof(X));
    memset(Y, 0, sizeof(Y));
    for (i = 0; i < (2 * N - 2); i += 4)
    {
        ay[0]   = NAN;
        ay[1]   = NAN;
        ay     += 4;
        ay[0]   = INFINITY;
        ay[1]   = INFINITY;
        ay     += 4;
    }
    Y[4 * N - 4] = NAN;
    Y[4 * N - 3] = NAN;
    BLASFUNC(zgemv)(&trans, &N, &N, alpha, A, &N, X, &incX, beta, Y, &incY);
    for (i = 0; i < 4 * N; i ++)
        ASSERT_TRUE(Y[i] == 0.0);
}

CTEST(zgemv, 0_2_nan_1_inf_1)
{
    int i;
    blasint N = 17;
    blasint incX = 1;
    blasint incY = 1;
    double alpha[2] = {0.0, 0.0};
    double beta[2] = {0.0, 2.0};
    char  trans = 'N';
    double A[17 * 17 * 4];
    double X[17 * 2];
    double Y[17 * 2];

    memset(A, 0, sizeof(A));
    memset(X, 0, sizeof(X));
    for (i = 0; i < (2 * N - 2); i += 4)
    {
        Y[i]     = NAN;
        Y[i + 1] = 1.0;

        Y[i + 2] = INFINITY;
        Y[i + 3] = 1.0;
    }
    Y[2 * N - 2] = NAN;
    Y[2 * N - 1] = 1.0;
    BLASFUNC(zgemv)(&trans, &N, &N, alpha, A, &N, X, &incX, beta, Y, &incY);
    for (i = 0; i < 2 * N; i += 2) {
        if ((i >> 1) % 2){
            ASSERT_TRUE(isnan(Y[i]));
            ASSERT_TRUE(isinf(Y[i + 1]));
        }
        else {
            ASSERT_TRUE(isnan(Y[i]));
            ASSERT_TRUE(isnan(Y[i + 1]));
        }
    }
}

CTEST(zgemv, 0_2_nan_1_inf_1_incy_2)
{
    int i;
    blasint N = 17;
    blasint incX = 1;
    blasint incY = 2;
    double alpha[2] = {0.0, 0.0};
    double beta[2] = {0.0, 2.0};
    char  trans = 'N';
    double A[17 * 17 * 4];
    double X[17];
    double Y[17 * 2 * 2];
    double *ay = Y;

    memset(A, 0, sizeof(A));
    memset(X, 0, sizeof(X));
    memset(Y, 0, sizeof(Y));
    for (i = 0; i < (2 * N - 2); i += 4)
    {
        ay[0]   = NAN;
        ay[1]   = 1.0;
        ay     += 4;
        ay[0]   = INFINITY;
        ay[1]   = 1.0;
        ay     += 4;
    }
    Y[4 * N - 4] = NAN;
    Y[4 * N - 3] = 1.0;
    BLASFUNC(zgemv)(&trans, &N, &N, alpha, A, &N, X, &incX, beta, Y, &incY);
    for (i = 0; i < 4 * N; i += 2) {
        if ((i >> 1) % 2) {
            ASSERT_TRUE(Y[i] == 0.0);
            ASSERT_TRUE(Y[i + 1] == 0.0);
        }
        else {
            if ((i >> 2) % 2) {
                ASSERT_TRUE(isnan(Y[i]));
                ASSERT_TRUE(isinf(Y[i + 1]));
            }
            else {
                ASSERT_TRUE(isnan(Y[i]));
                ASSERT_TRUE(isnan(Y[i + 1]));
            }
        }
    }
}

CTEST(zgemv, 2_0_nan_1_inf_1)
{
    int i;
    blasint N = 17;
    blasint incX = 1;
    blasint incY = 1;
    double alpha[2] = {0.0, 0.0};
    double beta[2] = {2.0, 0.0};
    char  trans = 'N';
    double A[17 * 17 * 4];
    double X[17 * 2];
    double Y[17 * 2];

    memset(A, 0, sizeof(A));
    memset(X, 0, sizeof(X));
    for (i = 0; i < (2 * N - 2); i += 4)
    {
        Y[i]     = NAN;
        Y[i + 1] = 1.0;

        Y[i + 2] = INFINITY;
        Y[i + 3] = 1.0;
    }
    Y[2 * N - 2] = NAN;
    Y[2 * N - 1] = 1.0;
    BLASFUNC(zgemv)(&trans, &N, &N, alpha, A, &N, X, &incX, beta, Y, &incY);
    for (i = 0; i < 2 * N; i += 2) {
        if ((i >> 1) % 2){
            ASSERT_TRUE(isinf(Y[i]));
            ASSERT_TRUE(isnan(Y[i + 1]));
        }
        else {
            ASSERT_TRUE(isnan(Y[i]));
            ASSERT_TRUE(isnan(Y[i + 1]));
        }
    }
}

CTEST(zgemv, 2_0_nan_1_inf_1_incy_2)
{
    int i;
    blasint N = 17;
    blasint incX = 1;
    blasint incY = 2;
    double alpha[2] = {0.0, 0.0};
    double beta[2] = {2.0, 0.0};
    char  trans = 'N';
    double A[17 * 17 * 4];
    double X[17];
    double Y[17 * 2 * 2];
    double *ay = Y;

    memset(A, 0, sizeof(A));
    memset(X, 0, sizeof(X));
    memset(Y, 0, sizeof(Y));
    for (i = 0; i < (2 * N - 2); i += 4)
    {
        ay[0]   = NAN;
        ay[1]   = 1.0;
        ay     += 4;
        ay[0]   = INFINITY;
        ay[1]   = 1.0;
        ay     += 4;
    }
    Y[4 * N - 4] = NAN;
    Y[4 * N - 3] = 1.0;
    BLASFUNC(zgemv)(&trans, &N, &N, alpha, A, &N, X, &incX, beta, Y, &incY);
    for (i = 0; i < 4 * N; i += 2) {
        if ((i >> 1) % 2) {
            ASSERT_TRUE(Y[i] == 0.0);
            ASSERT_TRUE(Y[i + 1] == 0.0);
        }
        else {
            if ((i >> 2) % 2) {
                ASSERT_TRUE(isinf(Y[i]));
                ASSERT_TRUE(isnan(Y[i + 1]));
            }
            else {
                ASSERT_TRUE(isnan(Y[i]));
                ASSERT_TRUE(isnan(Y[i + 1]));
            }
        }
    }
}

#endif
