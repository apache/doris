/*****************************************************************************
Copyright (c) 2011-2016, The OpenBLAS Project
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
CTEST(min, smin_negative){
  blasint N=3, inc=1;
  float te_min=0.0, tr_min=0.0;
  float x[]={-1.1, -2.2, -3.3};

  te_min=BLASFUNC(smin)(&N, x, &inc);
  tr_min=-3.3;

  ASSERT_DBL_NEAR_TOL((double)(tr_min), (double)(te_min), SINGLE_EPS);
}
#endif

#ifdef BUILD_DOUBLE
CTEST(min, dmin_positive){
  blasint N=3, inc=1;
  double te_min=0.0, tr_min=0.0;
  double x[]={1.1, 0.0, 3.3};

  te_min=BLASFUNC(dmin)(&N, x, &inc);
  tr_min=0.0;

  ASSERT_DBL_NEAR_TOL((double)(tr_min), (double)(te_min), DOUBLE_EPS);
}
#endif

#ifdef BUILD_SINGLE
CTEST(min, smin_zero){
  blasint N=3, inc=1;
  float te_min=0.0, tr_min=0.0;
  float x[]={1.1, 2.2, 0.0};

  te_min=BLASFUNC(smin)(&N, x, &inc);
  tr_min=0.0;

  ASSERT_DBL_NEAR_TOL((double)(tr_min), (double)(te_min), SINGLE_EPS);
}

CTEST(max, smax_negative){
  blasint N=3, inc=1;
  float te_max=0.0, tr_max=0.0;
  float x[]={-1.1, -2.2, -3.3};

  te_max=BLASFUNC(smax)(&N, x, &inc);
  tr_max=-1.1;

  ASSERT_DBL_NEAR_TOL((double)(tr_max), (double)(te_max), SINGLE_EPS);
}
#endif

#ifdef BUILD_DOUBLE
CTEST(max, dmax_positive){
  blasint N=3, inc=1;
  double te_max=0.0, tr_max=0.0;
  double x[]={1.1, 0.0, 3.3};

  te_max=BLASFUNC(dmax)(&N, x, &inc);
  tr_max=3.3;

  ASSERT_DBL_NEAR_TOL((double)(tr_max), (double)(te_max), DOUBLE_EPS);
}
#endif
#ifdef BUILD_SINGLE
CTEST(max, smax_zero){
  blasint N=3, inc=1;
  float te_max=0.0, tr_max=0.0;
  float x[]={-1.1, -2.2, 0.0};

  te_max=BLASFUNC(smax)(&N, x, &inc);
  tr_max=0.0;

  ASSERT_DBL_NEAR_TOL((double)(tr_max), (double)(te_max), SINGLE_EPS);
}
#endif

