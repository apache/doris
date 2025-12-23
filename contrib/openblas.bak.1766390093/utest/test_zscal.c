#include "openblas_utest.h"
#include <cblas.h>
#ifdef BUILD_SINGLE

#ifndef NAN
#define NAN 0.0/0.0
#endif
#ifndef INFINITY
#define INFINITY 1.0/0.0
#endif

CTEST(sscal, 0_nan)
{
    blasint N=9;
    blasint incX=1;
    float i = 0.0;
    float x[] = {NAN, NAN, NAN, NAN, NAN, NAN, NAN, NAN, NAN, NAN};
    BLASFUNC(sscal)(&N, &i, x, &incX);
    ASSERT_TRUE(isnan(x[0]));
    ASSERT_TRUE(isnan(x[8]));
}

CTEST(sscal, 0_nan_inc_2)
{
    blasint N=9;
    blasint incX=2;
    float i = 0.0;
    float x[] = {NAN, NAN, NAN, NAN, NAN, NAN, NAN, NAN, NAN, NAN,
                    NAN, NAN, NAN, NAN, NAN, NAN, NAN, NAN, NAN, NAN};
    BLASFUNC(sscal)(&N, &i, x, &incX);
    ASSERT_TRUE(isnan(x[0]));
    ASSERT_TRUE(isnan(x[8]));
}

CTEST(sscal, nan_0)
{
    blasint N=9;
    blasint incX=1;
    float i = NAN;
    float x[] = {0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
    BLASFUNC(sscal)(&N, &i, x, &incX);
    ASSERT_TRUE(isnan(x[0]));
    ASSERT_TRUE(isnan(x[8]));
}

CTEST(sscal, nan_0_inc_2)
{
    blasint N=9;
    blasint incX=2;
    float i = NAN;
    float x[] = {0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                    0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
    BLASFUNC(sscal)(&N, &i, x, &incX);
    ASSERT_TRUE(isnan(x[0]));
    ASSERT_TRUE(isnan(x[8]));
}

CTEST(sscal, 0_inf)
{
    blasint N=9;
    blasint incX=1;
    float i = 0.0;
    float x[] = {INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY};
    BLASFUNC(sscal)(&N, &i, x, &incX);
    ASSERT_TRUE(isnan(x[0]));
    ASSERT_TRUE(isnan(x[8]));
}

CTEST(sscal, 0_inf_inc_2)
{
    blasint N=9;
    blasint incX=2;
    float i = 0.0;
    float x[] = {INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY,
                    INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY};
    BLASFUNC(sscal)(&N, &i, x, &incX);
    ASSERT_TRUE(isnan(x[0]));
    ASSERT_TRUE(isnan(x[8]));
}

CTEST(sscal, inf_0)
{
    blasint N=9;
    blasint incX=1;
    float i = INFINITY;
    float x[] = {0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
    BLASFUNC(sscal)(&N, &i, x, &incX);
    ASSERT_TRUE(isnan(x[0]));
    ASSERT_TRUE(isnan(x[8]));
}

CTEST(sscal, inf_0_inc_2)
{
    blasint N=9;
    blasint incX=2;
    float i = INFINITY;
    float x[] = {0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                    0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
    BLASFUNC(sscal)(&N, &i, x, &incX);
    ASSERT_TRUE(isnan(x[0]));
    ASSERT_TRUE(isnan(x[8]));
}

CTEST(sscal, nan_inf)
{
    blasint N=9;
    blasint incX=1;
    float i = NAN;
    float x[] = {INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY};
    BLASFUNC(sscal)(&N, &i, x, &incX);
    ASSERT_TRUE(isnan(x[0]));
    ASSERT_TRUE(isnan(x[8]));
}

CTEST(sscal, nan_inf_inc_2)
{
    blasint N=9;
    blasint incX=2;
    float i = NAN;
    float x[] = {INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY,
                    INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY};
    BLASFUNC(sscal)(&N, &i, x, &incX);
    ASSERT_TRUE(isnan(x[0]));
    ASSERT_TRUE(isnan(x[8]));
}

CTEST(sscal, inf_nan)
{
    blasint N=9;
    blasint incX=1;
    float i = INFINITY;
    float x[] = {NAN, NAN, NAN, NAN, NAN, NAN, NAN, NAN, NAN, NAN};
    BLASFUNC(sscal)(&N, &i, x, &incX);
    ASSERT_TRUE(isnan(x[0]));
    ASSERT_TRUE(isnan(x[8]));
}

CTEST(sscal, inf_nan_inc_2)
{
    blasint N=9;
    blasint incX=2;
    float i = INFINITY;
    float x[] = {NAN, NAN, NAN, NAN, NAN, NAN, NAN, NAN, NAN, NAN,
                    NAN, NAN, NAN, NAN, NAN, NAN, NAN, NAN, NAN, NAN};
    BLASFUNC(sscal)(&N, &i, x, &incX);
    ASSERT_TRUE(isnan(x[0]));
    ASSERT_TRUE(isnan(x[8]));
}

#endif

#ifdef BUILD_DOUBLE

#ifndef NAN
#define NAN 0.0/0.0
#endif
#ifndef INFINITY
#define INFINITY 1.0/0.0
#endif

CTEST(dscal, 0_nan)
{
    blasint N=9;
    blasint incX=1;
    double i = 0.0;
    double x[] = {NAN, NAN, NAN, NAN, NAN, NAN, NAN, NAN, NAN, NAN};
    BLASFUNC(dscal)(&N, &i, x, &incX);
    ASSERT_TRUE(isnan(x[0]));
    ASSERT_TRUE(isnan(x[8]));
}

CTEST(dscal, 0_nan_inc_2)
{
    blasint N=9;
    blasint incX=2;
    double i = 0.0;
    double x[] = {NAN, NAN, NAN, NAN, NAN, NAN, NAN, NAN, NAN, NAN,
                    NAN, NAN, NAN, NAN, NAN, NAN, NAN, NAN, NAN, NAN};
    BLASFUNC(dscal)(&N, &i, x, &incX);
    ASSERT_TRUE(isnan(x[0]));
    ASSERT_TRUE(isnan(x[8]));
}

CTEST(dscal, nan_0)
{
    blasint N=9;
    blasint incX=1;
    double i = NAN;
    double x[] = {0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
    BLASFUNC(dscal)(&N, &i, x, &incX);
    ASSERT_TRUE(isnan(x[0]));
    ASSERT_TRUE(isnan(x[8]));
}

CTEST(dscal, nan_0_inc_2)
{
    blasint N=9;
    blasint incX=2;
    double i = NAN;
    double x[] = {0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                    0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
    BLASFUNC(dscal)(&N, &i, x, &incX);
    ASSERT_TRUE(isnan(x[0]));
    ASSERT_TRUE(isnan(x[8]));
}

CTEST(dscal, 0_inf)
{
    blasint N=9;
    blasint incX=1;
    double i = 0.0;
    double x[] = {INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY};
    BLASFUNC(dscal)(&N, &i, x, &incX);
    ASSERT_TRUE(isnan(x[0]));
    ASSERT_TRUE(isnan(x[8]));
}

CTEST(dscal, 0_inf_inc_2)
{
    blasint N=9;
    blasint incX=2;
    double i = 0.0;
    double x[] = {INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY,
                    INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY};
    BLASFUNC(dscal)(&N, &i, x, &incX);
    ASSERT_TRUE(isnan(x[0]));
    ASSERT_TRUE(isnan(x[8]));
}

CTEST(dscal, inf_0)
{
    blasint N=9;
    blasint incX=1;
    double i = INFINITY;
    double x[] = {0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
    BLASFUNC(dscal)(&N, &i, x, &incX);
    ASSERT_TRUE(isnan(x[0]));
    ASSERT_TRUE(isnan(x[8]));
}

CTEST(dscal, inf_0_inc_2)
{
    blasint N=9;
    blasint incX=2;
    double i = INFINITY;
    double x[] = {0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                    0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
    BLASFUNC(dscal)(&N, &i, x, &incX);
    ASSERT_TRUE(isnan(x[0]));
    ASSERT_TRUE(isnan(x[8]));
}

CTEST(dscal, nan_inf)
{
    blasint N=9;
    blasint incX=1;
    double i = NAN;
    double x[] = {INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY};
    BLASFUNC(dscal)(&N, &i, x, &incX);
    ASSERT_TRUE(isnan(x[0]));
    ASSERT_TRUE(isnan(x[8]));
}

CTEST(dscal, nan_inf_inc_2)
{
    blasint N=9;
    blasint incX=2;
    double i = NAN;
    double x[] = {INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY,
                    INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY, INFINITY};
    BLASFUNC(dscal)(&N, &i, x, &incX);
    ASSERT_TRUE(isnan(x[0]));
    ASSERT_TRUE(isnan(x[8]));
}

CTEST(dscal, inf_nan)
{
    blasint N=9;
    blasint incX=1;
    double i = INFINITY;
    double x[] = {NAN, NAN, NAN, NAN, NAN, NAN, NAN, NAN, NAN, NAN};
    BLASFUNC(dscal)(&N, &i, x, &incX);
    ASSERT_TRUE(isnan(x[0]));
    ASSERT_TRUE(isnan(x[8]));
}

CTEST(dscal, inf_nan_inc_2)
{
    blasint N=9;
    blasint incX=2;
    double i = INFINITY;
    double x[] = {NAN, NAN, NAN, NAN, NAN, NAN, NAN, NAN, NAN, NAN,
                    NAN, NAN, NAN, NAN, NAN, NAN, NAN, NAN, NAN, NAN};
    BLASFUNC(dscal)(&N, &i, x, &incX);
    ASSERT_TRUE(isnan(x[0]));
    ASSERT_TRUE(isnan(x[8]));
}

#endif

#ifdef BUILD_COMPLEX

#ifndef NAN
#define NAN 0.0/0.0
#endif
#ifndef INFINITY
#define INFINITY 1.0/0.0
#endif

CTEST(cscal, i_nan)
{
    blasint N=9;
    blasint incX=1;
    float i[] = {0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1 };
    float nan[] = {NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0};
    BLASFUNC(cscal)(&N, i, nan, &incX);
    ASSERT_TRUE(isnan(nan[0]));
    ASSERT_TRUE(isnan(nan[1]));
    ASSERT_TRUE(isnan(nan[16]));
    ASSERT_TRUE(isnan(nan[17]));
}

CTEST(cscal, i_nan_inc_2)
{
    blasint N=9;
    blasint incX=2;
    float i[] = {0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1 };
    float nan[] = {NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0,
                    NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0};
    BLASFUNC(cscal)(&N, i, nan, &incX);
    ASSERT_TRUE(isnan(nan[0]));
    ASSERT_TRUE(isnan(nan[1]));
    ASSERT_TRUE(isnan(nan[16]));
    ASSERT_TRUE(isnan(nan[17]));
}

CTEST(cscal, nan_i)
{
    blasint N=9;
    blasint incX=1;
    float i[] = {0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1 };
    float nan[] = {NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0};
    BLASFUNC(cscal)(&N, nan, i, &incX);
    ASSERT_TRUE(isnan(i[0]));
    ASSERT_TRUE(isnan(i[1]));
    ASSERT_TRUE(isnan(i[16]));
    ASSERT_TRUE(isnan(i[17]));
}

CTEST(cscal, nan_i_inc_2)
{
    blasint N=9;
    blasint incX=2;
    float i[] = {0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1,
                  0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1 };
    float nan[] = {NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0};
    BLASFUNC(cscal)(&N, nan, i, &incX);
    ASSERT_TRUE(isnan(i[0]));
    ASSERT_TRUE(isnan(i[1]));
    ASSERT_TRUE(isnan(i[16]));
    ASSERT_TRUE(isnan(i[17]));
}

CTEST(cscal, i_inf)
{
    blasint N=9;
    blasint incX=1;
    float i[] = {0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1 };
    float inf[] = {INFINITY, 0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0};
    BLASFUNC(cscal)(&N, i, inf, &incX);
    ASSERT_TRUE(isnan(inf[0]));
    ASSERT_TRUE(isinf(inf[1]));
    ASSERT_TRUE(isnan(inf[16]));
    ASSERT_TRUE(isinf(inf[17]));
}

CTEST(cscal, i_inf_inc_2)
{
    blasint N=9;
    blasint incX=2;
    float i[] = {0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1 };
    float inf[] = {INFINITY, 0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0,
                    INFINITY, 0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0};
    BLASFUNC(cscal)(&N, i, inf, &incX);
    ASSERT_TRUE(isnan(inf[0]));
    ASSERT_TRUE(isinf(inf[1]));
    ASSERT_TRUE(isnan(inf[16]));
    ASSERT_TRUE(isinf(inf[17]));
}

CTEST(cscal, inf_i)
{
    blasint N=9;
    blasint incX=1;
    float i[] = {0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1 };
    float inf[] = {INFINITY, 0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0};
    BLASFUNC(cscal)(&N, inf, i, &incX);
    ASSERT_TRUE(isnan(i[0]));
    ASSERT_TRUE(isinf(i[1]));
    ASSERT_TRUE(isnan(i[16]));
    ASSERT_TRUE(isinf(i[17]));
}

CTEST(cscal, inf_i_inc_2)
{
    blasint N=9;
    blasint incX=2;
    float i[] = {0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1,
                  0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1 };
    float inf[] = {INFINITY, 0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0};
    BLASFUNC(cscal)(&N, inf, i, &incX);
    ASSERT_TRUE(isnan(i[0]));
    ASSERT_TRUE(isinf(i[1]));
    ASSERT_TRUE(isnan(i[16]));
    ASSERT_TRUE(isinf(i[17]));
}

CTEST(cscal, i_0inf)
{
    blasint N=9;
    blasint incX=1;
    float i[] = {0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1 };
    float inf[] = {0,INFINITY, 0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY};
    BLASFUNC(cscal)(&N, i, inf, &incX);
    ASSERT_TRUE(isinf(inf[0]));
    ASSERT_TRUE(isnan(inf[1]));
    ASSERT_TRUE(isinf(inf[16]));
    ASSERT_TRUE(isnan(inf[17]));
}

CTEST(cscal, i_0inf_inc_2)
{
    blasint N=9;
    blasint incX=2;
    float i[] = {0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1 };
    float inf[] = {0,INFINITY, 0,INFINITY, 0,INFINITY, 0,INFINITY, 0,INFINITY, 0,INFINITY, 0,INFINITY, 0,INFINITY, 0,INFINITY,
                    0,INFINITY, 0,INFINITY, 0,INFINITY, 0,INFINITY, 0,INFINITY, 0,INFINITY, 0,INFINITY, 0,INFINITY, 0,INFINITY};
    BLASFUNC(cscal)(&N, i, inf, &incX);
    ASSERT_TRUE(isinf(inf[0]));
    ASSERT_TRUE(isnan(inf[1]));
    ASSERT_TRUE(isinf(inf[16]));
    ASSERT_TRUE(isnan(inf[17]));
}

CTEST(cscal, i00_NAN)
{
    blasint N=9;
    blasint incX=1;
    float i[] = {0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0 };
    float nan[] = {NAN, 0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0};
    BLASFUNC(cscal)(&N, i, nan, &incX);
    ASSERT_TRUE(isnan(nan[0]));
    ASSERT_TRUE(isnan(nan[1]));
    ASSERT_TRUE(isnan(nan[16]));
    ASSERT_TRUE(isnan(nan[17]));
}

CTEST(cscal, i00_NAN_incx_2)
{
    blasint N=9;
    blasint incX=2;
    float i[] = {0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0 };
    float nan[] = {0,NAN, 0,NAN, 0,NAN, 0,NAN, 0,NAN, 0,NAN, 0,NAN, 0,NAN, 0,NAN,
                   0,NAN, 0,NAN, 0,NAN, 0,NAN, 0,NAN, 0,NAN, 0,NAN, 0,NAN, 0,NAN};
    BLASFUNC(cscal)(&N, i, nan, &incX);
    ASSERT_TRUE(isnan(nan[0]));
    ASSERT_TRUE(isnan(nan[1]));
    ASSERT_TRUE(isnan(nan[16]));
    ASSERT_TRUE(isnan(nan[17]));
}

#endif

#ifdef BUILD_COMPLEX16

#ifndef NAN
#define NAN 0.0/0.0
#endif
#ifndef INFINITY
#define INFINITY 1.0/0.0
#endif

CTEST(zscal, i_nan)
{
    blasint N=9;
    blasint incX=1;
    double i[] = {0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1 };
    double nan[] = {NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0};
    BLASFUNC(zscal)(&N, i, nan, &incX);
    ASSERT_TRUE(isnan(nan[0]));
    ASSERT_TRUE(isnan(nan[1]));
    ASSERT_TRUE(isnan(nan[16]));
    ASSERT_TRUE(isnan(nan[17]));
}

CTEST(zscal, i_nan_inc_2)
{
    blasint N=9;
    blasint incX=2;
    double i[] = {0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1 };
    double nan[] = {NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0,
                    NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0};
    BLASFUNC(zscal)(&N, i, nan, &incX);
    ASSERT_TRUE(isnan(nan[0]));
    ASSERT_TRUE(isnan(nan[1]));
    ASSERT_TRUE(isnan(nan[16]));
    ASSERT_TRUE(isnan(nan[17]));
}

CTEST(zscal, nan_i)
{
    blasint N=9;
    blasint incX=1;
    double i[] = {0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1 };
    double nan[] = {NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0};
    BLASFUNC(zscal)(&N, nan, i, &incX);
    ASSERT_TRUE(isnan(i[0]));
    ASSERT_TRUE(isnan(i[1]));
    ASSERT_TRUE(isnan(i[16]));
    ASSERT_TRUE(isnan(i[17]));
}

CTEST(zscal, nan_i_inc_2)
{
    blasint N=9;
    blasint incX=2;
    double i[] = {0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1,
                  0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1 };
    double nan[] = {NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0};
    BLASFUNC(zscal)(&N, nan, i, &incX);
    ASSERT_TRUE(isnan(i[0]));
    ASSERT_TRUE(isnan(i[1]));
    ASSERT_TRUE(isnan(i[16]));
    ASSERT_TRUE(isnan(i[17]));
}

CTEST(zscal, i_inf)
{
    blasint N=9;
    blasint incX=1;
    double i[] = {0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1 };
    double inf[] = {INFINITY, 0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0};
    BLASFUNC(zscal)(&N, i, inf, &incX);
    ASSERT_TRUE(isnan(inf[0]));
    ASSERT_TRUE(isinf(inf[1]));
    ASSERT_TRUE(isnan(inf[16]));
    ASSERT_TRUE(isinf(inf[17]));
}

CTEST(zscal, i_inf_inc_2)
{
    blasint N=9;
    blasint incX=2;
    double i[] = {0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1 };
    double inf[] = {INFINITY, 0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0,
                    INFINITY, 0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0};
    BLASFUNC(zscal)(&N, i, inf, &incX);
    ASSERT_TRUE(isnan(inf[0]));
    ASSERT_TRUE(isinf(inf[1]));
    ASSERT_TRUE(isnan(inf[16]));
    ASSERT_TRUE(isinf(inf[17]));
}

CTEST(zscal, inf_i)
{
    blasint N=9;
    blasint incX=1;
    double i[] = {0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1 };
    double inf[] = {INFINITY, 0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0};
    BLASFUNC(zscal)(&N, inf, i, &incX);
    ASSERT_TRUE(isnan(i[0]));
    ASSERT_TRUE(isinf(i[1]));
    ASSERT_TRUE(isnan(i[16]));
    ASSERT_TRUE(isinf(i[17]));
}

CTEST(zscal, inf_i_inc_2)
{
    blasint N=9;
    blasint incX=2;
    double i[] = {0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1,
                  0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1 };
    double inf[] = {INFINITY, 0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0};
    BLASFUNC(zscal)(&N, inf, i, &incX);
    ASSERT_TRUE(isnan(i[0]));
    ASSERT_TRUE(isinf(i[1]));
    ASSERT_TRUE(isnan(i[16]));
    ASSERT_TRUE(isinf(i[17]));
}

CTEST(zscal, i_0inf)
{
    blasint N=9;
    blasint incX=1;
    double i[] = {0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1 };
    double inf[] = {0,INFINITY, 0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY,0, INFINITY};
    BLASFUNC(zscal)(&N, i, inf, &incX);
    ASSERT_TRUE(isinf(inf[0]));
    ASSERT_TRUE(isnan(inf[1]));
    ASSERT_TRUE(isinf(inf[16]));
    ASSERT_TRUE(isnan(inf[17]));
}

CTEST(zscal, i_0inf_inc_2)
{
    blasint N=9;
    blasint incX=2;
    double i[] = {0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1, 0,1 };
    double inf[] = {0,INFINITY, 0,INFINITY, 0,INFINITY, 0,INFINITY, 0,INFINITY, 0,INFINITY, 0,INFINITY, 0,INFINITY, 0,INFINITY,
                    0,INFINITY, 0,INFINITY, 0,INFINITY, 0,INFINITY, 0,INFINITY, 0,INFINITY, 0,INFINITY, 0,INFINITY, 0,INFINITY};
    BLASFUNC(zscal)(&N, i, inf, &incX);
    ASSERT_TRUE(isinf(inf[0]));
    ASSERT_TRUE(isnan(inf[1]));
    ASSERT_TRUE(isinf(inf[16]));
    ASSERT_TRUE(isnan(inf[17]));
}

CTEST(zscal, i00_NAN)
{
    blasint N=9;
    blasint incX=1;
    double i[] = {0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0 };
    double nan[] = {NAN, 0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0, NAN,0};
    BLASFUNC(zscal)(&N, i, nan, &incX);
    ASSERT_TRUE(isnan(nan[0]));
    ASSERT_TRUE(isnan(nan[1]));
    ASSERT_TRUE(isnan(nan[16]));
    ASSERT_TRUE(isnan(nan[17]));
}

CTEST(zscal, i00_NAN_incx_2)
{
    blasint N=9;
    blasint incX=2;
    double i[] = {0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0, 0,0 };
    double nan[] = {0,NAN, 0,NAN, 0,NAN, 0,NAN, 0,NAN, 0,NAN, 0,NAN, 0,NAN, 0,NAN,
                    0,NAN, 0,NAN, 0,NAN, 0,NAN, 0,NAN, 0,NAN, 0,NAN, 0,NAN, 0,NAN};
    BLASFUNC(zscal)(&N, i, nan, &incX);
    ASSERT_TRUE(isnan(nan[0]));
    ASSERT_TRUE(isnan(nan[1]));
    ASSERT_TRUE(isnan(nan[16]));
    ASSERT_TRUE(isnan(nan[17]));
}

#endif
