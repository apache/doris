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

#define ELEMENTS 70
#define INCREMENT 2

#ifdef BUILD_COMPLEX16

/**
 * Test dzamax by comparing it against pre-calculated values
 */
CTEST(dzamax, bad_args_N_0){
   blasint i;
   blasint N = 0, inc = 1;
   double x[ELEMENTS * 2];
   for (i = 0; i < ELEMENTS * inc * 2; i ++) {
      x[i] = 1000 - i;
   }
   double amax = BLASFUNC(dzamax)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(0.0, amax, DOUBLE_EPS);
}

/**
 * Test dzamax by comparing it against pre-calculated values
 */
CTEST(dzamax, step_zero){
   blasint i;
   blasint N = ELEMENTS * 2, inc = 0;
   double x[ELEMENTS * 2];
   for (i = 0; i < N; i ++) {
      x[i] = i - 1000;
   }
   double amax = BLASFUNC(dzamax)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL((fabs(x[0]) + fabs(x[1])), amax, DOUBLE_EPS);
}

/**
 * Test dzamax by comparing it against pre-calculated values
 */
CTEST(dzamax, positive_step_1_N_1){
   blasint N = 1, inc = 1;
   double x[] = {1.0, 2.0};
   double amax = BLASFUNC(dzamax)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(3.0, amax, DOUBLE_EPS);
}

/**
 * Test dzamax by comparing it against pre-calculated values
 */
CTEST(dzamax, negative_step_1_N_1){
   blasint N = 1, inc = 1;
   double x[] = {-1.0, -2.0};
   double amax = BLASFUNC(dzamax)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(3.0, amax, DOUBLE_EPS);
}

/**
 * Test dzamax by comparing it against pre-calculated values
 */
CTEST(dzamax, positive_step_2_N_1){
   blasint N = 1, inc = 2;
   double x[] = {1.0, 2.0, 0.0, 0.0};
   double amax = BLASFUNC(dzamax)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(3.0, amax, DOUBLE_EPS);
}

/**
 * Test dzamax by comparing it against pre-calculated values
 */
CTEST(dzamax, negative_step_2_N_1){
   blasint N = 1, inc = 2;
   double x[] = {-1.0, -2.0, 0.0, 0.0};
   double amax = BLASFUNC(dzamax)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(3.0, amax, DOUBLE_EPS);
}

/**
 * Test dzamax by comparing it against pre-calculated values
 */
CTEST(dzamax, positive_step_1_N_2){
   blasint N = 2, inc = 1;
   double x[] = {1.0, 2.0, 0.0, 0.0};
   double amax = BLASFUNC(dzamax)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(3.0, amax, DOUBLE_EPS);
}

/**
 * Test dzamax by comparing it against pre-calculated values
 */
CTEST(dzamax, negative_step_1_N_2){
   blasint N = 2, inc = 1;
   double x[] = {-1.0, -2.0, 0.0, 0.0};
   double amax = BLASFUNC(dzamax)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(3.0, amax, DOUBLE_EPS);
}

/**
 * Test dzamax by comparing it against pre-calculated values
 */
CTEST(dzamax, positive_step_2_N_2){
   blasint N = 2, inc = 2;
   double x[] = {1.0, 2.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0};
   double amax = BLASFUNC(dzamax)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(3.0, amax, DOUBLE_EPS);
}

/**
 * Test dzamax by comparing it against pre-calculated values
 */
CTEST(dzamax, negative_step_2_N_2){
   blasint N = 2, inc = 2;
   double x[] = {-1.0, -2.0, 0.0, 0.0, -1.0, -1.0, 0.0, 0.0};
   double amax = BLASFUNC(dzamax)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(3.0, amax, DOUBLE_EPS);
}

/**
 * Test dzamax by comparing it against pre-calculated values
 */
CTEST(dzamax, positive_step_1_N_3){
   blasint N = 3, inc = 1;
   double x[] = {1.0, 2.0, 0.0, 0.0, 2.0, 1.0};
   double amax = BLASFUNC(dzamax)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(3.0, amax, DOUBLE_EPS);
}

/**
 * Test dzamax by comparing it against pre-calculated values
 */
CTEST(dzamax, negative_step_1_N_3){
   blasint N = 3, inc = 1;
   double x[] = {-1.0, -2.0, 0.0, 0.0, -3.0, -1.0};
   double amax = BLASFUNC(dzamax)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(4.0, amax, DOUBLE_EPS);
}

/**
 * Test dzamax by comparing it against pre-calculated values
 */
CTEST(dzamax, positive_step_2_N_3){
   blasint N = 3, inc = 2;
   double x[] = {1.0, 2.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 3.0, 1.0, 0.0, 0.0};
   double amax = BLASFUNC(dzamax)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(4.0, amax, DOUBLE_EPS);
}

/**
 * Test dzamax by comparing it against pre-calculated values
 */
CTEST(dzamax, negative_step_2_N_3){
   blasint N = 3, inc = 2;
   double x[] = {-1.0, -2.0, 0.0, 0.0, -1.0, -1.0, 0.0, 0.0, -3.0, -1.0, 0.0, 0.0};
   double amax = BLASFUNC(dzamax)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(4.0, amax, DOUBLE_EPS);
}

/**
 * Test dzamax by comparing it against pre-calculated values
 */
CTEST(dzamax, positive_step_1_N_4){
   blasint N = 4, inc = 1;
   double x[] = {1.0, 2.0, 0.0, 0.0, 2.0, 1.0, -2.0, -2.0};
   double amax = BLASFUNC(dzamax)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(4.0, amax, DOUBLE_EPS);
}

/**
 * Test dzamax by comparing it against pre-calculated values
 */
CTEST(dzamax, negative_step_1_N_4){
   blasint N = 4, inc = 1;
   double x[] = {-1.0, -2.0, 0.0, 0.0, -2.0, -1.0, -2.0, -2.0};
   double amax = BLASFUNC(dzamax)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(4.0, amax, DOUBLE_EPS);
}

/**
 * Test dzamax by comparing it against pre-calculated values
 */
CTEST(dzamax, positive_step_2_N_4){
   blasint N = 4, inc = 2;
   double x[] = {1.0, 2.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 2.0, 1.0, 0.0, 0.0, -2.0, -2.0, 0.0, 0.0};
   double amax = BLASFUNC(dzamax)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(4.0, amax, DOUBLE_EPS);
}

/**
 * Test dzamax by comparing it against pre-calculated values
 */
CTEST(dzamax, negative_step_2_N_4){
   blasint N = 4, inc = 2;
   double x[] = {-1.0, -2.0, 0.0, 0.0, -1.0, -1.0, 0.0, 0.0, -2.0, -1.0, 0.0, 0.0, -2.0, -2.0, 0.0, 0.0};
   double amax = BLASFUNC(dzamax)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(4.0, amax, DOUBLE_EPS);
}

/**
 * Test dzamax by comparing it against pre-calculated values
 */
CTEST(dzamax, positive_step_1_N_70){
   blasint i;
   blasint N = ELEMENTS, inc = 1;
   double x[ELEMENTS * 2];
   for (i = 0; i < N * inc * 2; i ++) {
      x[i] = i;
   }
   x[7 * inc * 2] = 1000.0;
   x[7 * inc * 2 + 1] = 1000.0;
   double amax = BLASFUNC(dzamax)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(2000.0, amax, DOUBLE_EPS);
}

/**
 * Test dzamax by comparing it against pre-calculated values
 */
CTEST(dzamax, negative_step_1_N_70){
   blasint i;
   blasint N = ELEMENTS, inc = 1;
   double x[ELEMENTS * 2];
   for (i = 0; i < N * inc * 2; i ++) {
      x[i] = -i;
   }
   x[7 * inc * 2] = 1000.0;
   x[7 * inc * 2 + 1] = 1000.0;
   double amax = BLASFUNC(dzamax)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(2000.0, amax, DOUBLE_EPS);
}

/**
 * Test dzamax by comparing it against pre-calculated values
 */
CTEST(dzamax, positive_step_2_N_70){
   blasint i;
   blasint N = ELEMENTS, inc = INCREMENT;
   double x[ELEMENTS * INCREMENT * 2];
   for (i = 0; i < N * inc * 2; i ++) {
      x[i] = i;
   }
   x[7 * inc * 2] = 1000.0;
   x[7 * inc * 2 + 1] = 1000.0;
   double amax = BLASFUNC(dzamax)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(2000.0, amax, DOUBLE_EPS);
}

/**
 * Test dzamax by comparing it against pre-calculated values
 */
CTEST(dzamax, negative_step_2_N_70){
   blasint i;
   blasint N = ELEMENTS, inc = INCREMENT;
   double x[ELEMENTS * INCREMENT * 2];
   for (i = 0; i < N * inc * 2; i ++) {
      x[i] = -i;
   }
   x[7 * inc * 2] = 1000.0;
   x[7 * inc * 2 + 1] = 1000.0;
   double amax = BLASFUNC(dzamax)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(2000.0, amax, DOUBLE_EPS);
}
#endif