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

#ifdef BUILD_SINGLE

/**
 * Test samin by comparing it against pre-calculated values
 */
CTEST(samin, bad_args_N_0){
   blasint i;
   blasint N = 0, inc = 1;
   float x[ELEMENTS];
   for (i = 0; i < ELEMENTS * inc; i ++) {
      x[i] = 1000 - i;
   }
   float amin = BLASFUNC(samin)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(0.0f, amin, SINGLE_EPS);
}

/**
 * Test samin by comparing it against pre-calculated values
 */
CTEST(samin, step_zero){
   blasint i;
   blasint N = ELEMENTS, inc = 0;
   float x[ELEMENTS];
   for (i = 0; i < N; i ++) {
      x[i] = i - 1000;
   }
   x[8] = 0.0;
   float amin = BLASFUNC(samin)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(fabsf(x[0]), amin, SINGLE_EPS);
}

/**
 * Test samin by comparing it against pre-calculated values
 */
CTEST(samin, positive_step_1_N_1){
   blasint N = 1, inc = 1;
   float x[] = {1.1f};

   float amin = BLASFUNC(samin)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(1.1f, amin, SINGLE_EPS);
}

/**
 * Test samin by comparing it against pre-calculated values
 */
CTEST(samin, negative_step_1_N_1){
   blasint N = 1, inc = 1;
   float x[] = {-1.1f};

   float amin = BLASFUNC(samin)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(1.1f, amin, SINGLE_EPS);
}

/**
 * Test samin by comparing it against pre-calculated values
 */
CTEST(samin, positive_step_2_N_1){
   blasint N = 1, inc = 2;
   float x[] = {1.1f, 0.0f};

   float amin = BLASFUNC(samin)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(1.1f, amin, SINGLE_EPS);
}

/**
 * Test samin by comparing it against pre-calculated values
 */
CTEST(samin, negative_step_2_N_1){
   blasint N = 1, inc = 2;
   float x[] = {-1.1f, 0.0f};

   float amin = BLASFUNC(samin)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(1.1f, amin, SINGLE_EPS);
}

/**
 * Test samin by comparing it against pre-calculated values
 */
CTEST(samin, positive_step_1_N_2){
   blasint N = 2, inc = 1;
   float x[] = {1.1f, 1.0f};

   float amin = BLASFUNC(samin)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(1.0f, amin, SINGLE_EPS);
}

/**
 * Test samin by comparing it against pre-calculated values
 */
CTEST(samin, negative_step_1_N_2){
   blasint N = 2, inc = 1;
   float x[] = {-1.1f, 1.0f};

   float amin = BLASFUNC(samin)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(1.0f, amin, SINGLE_EPS);
}

/**
 * Test samin by comparing it against pre-calculated values
 */
CTEST(samin, positive_step_2_N_2){
   blasint N = 2, inc = 2;
   float x[] = {1.1f, 0.0f, 1.0f, 0.0f};

   float amin = BLASFUNC(samin)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(1.0f, amin, SINGLE_EPS);
}

/**
 * Test samin by comparing it against pre-calculated values
 */
CTEST(samin, negative_step_2_N_2){
   blasint N = 2, inc = 2;
   float x[] = {-1.1f, 0.0f, 1.0f, 0.0f};

   float amin = BLASFUNC(samin)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(1.0f, amin, SINGLE_EPS);
}

/**
 * Test samin by comparing it against pre-calculated values
 */
CTEST(samin, positive_step_1_N_3){
   blasint N = 3, inc = 1;
   float x[] = {1.1f, 1.0f, 2.2f};

   float amin = BLASFUNC(samin)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(1.0f, amin, SINGLE_EPS);
}

/**
 * Test samin by comparing it against pre-calculated values
 */
CTEST(samin, negative_step_1_N_3){
   blasint N = 3, inc = 1;
   float x[] = {-1.1f, 1.0f, -2.2f};

   float amin = BLASFUNC(samin)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(1.0f, amin, SINGLE_EPS);
}

/**
 * Test samin by comparing it against pre-calculated values
 */
CTEST(samin, positive_step_2_N_3){
   blasint N = 3, inc = 2;
   float x[] = {1.1f, 0.0f, 1.0f, 0.0f, 2.2f, 0.0f};

   float amin = BLASFUNC(samin)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(1.0f, amin, SINGLE_EPS);
}

/**
 * Test samin by comparing it against pre-calculated values
 */
CTEST(samin, negative_step_2_N_3){
   blasint N = 3, inc = 2;
   float x[] = {-1.1f, 0.0f, 1.0f, 0.0f, -2.2f, 0.0f};

   float amin = BLASFUNC(samin)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(1.0f, amin, SINGLE_EPS);
}

/**
 * Test samin by comparing it against pre-calculated values
 */
CTEST(samin, positive_step_1_N_4){
   blasint N = 4, inc = 1;
   float x[] = {1.1f, 1.0f, 2.2f, 3.3f};

   float amin = BLASFUNC(samin)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(1.0f, amin, SINGLE_EPS);
}

/**
 * Test samin by comparing it against pre-calculated values
 */
CTEST(samin, negative_step_1_N_4){
   blasint N = 4, inc = 1;
   float x[] = {-1.1f, 1.0f, -2.2f, -3.3f};

   float amin = BLASFUNC(samin)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(1.0f, amin, SINGLE_EPS);
}

/**
 * Test samin by comparing it against pre-calculated values
 */
CTEST(samin, positive_step_2_N_4){
   blasint N = 4, inc = 2;
   float x[] = {1.1f, 0.0f, 1.0f, 0.0f, 2.2f, 0.0f, 3.3f, 0.0f};

   float amin = BLASFUNC(samin)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(1.0f, amin, SINGLE_EPS);
}

/**
 * Test samin by comparing it against pre-calculated values
 */
CTEST(samin, negative_step_2_N_4){
   blasint N = 4, inc = 2;
   float x[] = {-1.1f, 0.0f, 1.0f, 0.0f, -2.2f, 0.0f, -3.3f, 0.0f};

   float amin = BLASFUNC(samin)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(1.0f, amin, SINGLE_EPS);
}

/**
 * Test samin by comparing it against pre-calculated values
 */
CTEST(samin, positive_step_1_N_5){
   blasint N = 5, inc = 1;
   float x[] = {1.1f, 1.0f, 2.2f, 3.3f, 0.0f};

   float amin = BLASFUNC(samin)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(0.0f, amin, SINGLE_EPS);
}

/**
 * Test samin by comparing it against pre-calculated values
 */
CTEST(samin, negative_step_1_N_5){
   blasint N = 5, inc = 1;
   float x[] = {-1.1f, 1.0f, -2.2f, -3.3f, 0.0f};

   float amin = BLASFUNC(samin)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(0.0f, amin, SINGLE_EPS);
}

/**
 * Test samin by comparing it against pre-calculated values
 */
CTEST(samin, positive_step_2_N_5){
   blasint N = 5, inc = 2;
   float x[] = {1.1f, 0.0f, 1.0f, 0.0f, 2.2f, 0.0f, 3.3f, 0.0f, 0.0f, 0.0f};

   float amin = BLASFUNC(samin)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(0.0f, amin, SINGLE_EPS);
}

/**
 * Test samin by comparing it against pre-calculated values
 */
CTEST(samin, negative_step_2_N_5){
   blasint N = 5, inc = 2;
   float x[] = {-1.1f, 0.0f, 1.0f, 0.0f, -2.2f, 0.0f, -3.3f, 0.0f, 0.0f, 0.0f};

   float amin = BLASFUNC(samin)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(0.0f, amin, SINGLE_EPS);
}

/**
 * Test samin by comparing it against pre-calculated values
 */
CTEST(samin, positive_step_1_N_70){
   blasint i;
   blasint N = ELEMENTS, inc = 1;
   float x[ELEMENTS];
   for (i = 0; i < N * inc; i ++) {
      x[i] = i + 1000;
   }

   x[8 * inc] = 0.0f;
   float amin = BLASFUNC(samin)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(0.0f, amin, SINGLE_EPS);
}

/**
 * Test samin by comparing it against pre-calculated values
 */
CTEST(samin, negative_step_1_N_70){
   blasint i;
   blasint N = ELEMENTS, inc = 1;
   float x[ELEMENTS];
   for (i = 0; i < N  * inc; i ++) {
      x[i] = - i - 1000;
   }

   x[8 * inc] = -1.0f;
   float amin = BLASFUNC(samin)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(1.0f, amin, SINGLE_EPS);
}

/**
 * Test samin by comparing it against pre-calculated values
 */
CTEST(samin, positive_step_2_N_70){
   blasint i;
   blasint N = ELEMENTS, inc = INCREMENT;
   float x[ELEMENTS * INCREMENT];
   for (i = 0; i < N * inc; i ++) {
      x[i] = i + 1000;
   }

   x[8 * inc] = 1.0f;
   float amin = BLASFUNC(samin)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(1.0f, amin, SINGLE_EPS);
}

/**
 * Test samin by comparing it against pre-calculated values
 */
CTEST(samin, negative_step_2_N_70){
   blasint i;
   blasint N = ELEMENTS, inc = INCREMENT;
   float x[ELEMENTS * INCREMENT];
   for (i = 0; i < N * inc; i ++) {
      x[i] = - i - 1000;
   }

   x[8 * inc] = -1.0f;
   float amin = BLASFUNC(samin)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(1.0f, amin, SINGLE_EPS);
}
#endif