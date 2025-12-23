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

#define ELEMENTS 50
#define INCREMENT 2

#ifdef BUILD_COMPLEX

/**
 * Fortran API specific test
 * Test scsum by comparing it against pre-calculated values
 */
CTEST(scsum, bad_args_N_0){
   blasint i;
   blasint N = 0, inc = 1;
   float x[ELEMENTS * 2];
   for (i = 0; i < ELEMENTS * inc * 2; i ++) {
      x[i] = 1000 - i;
   }
   float sum = BLASFUNC(scsum)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(0.0f, sum, SINGLE_EPS);
}

/**
 * Fortran API specific test
 * Test scsum by comparing it against pre-calculated values
 */
CTEST(scsum, step_zero){
   blasint i;
   blasint N = ELEMENTS, inc = 0;
   float x[ELEMENTS];
   x[0] = 0.0f;
   for (i = 0; i < N  * inc * 2; i ++) {
      x[i] = i + 1000;
   }
   x[8] = 0.0f;
   float sum = BLASFUNC(scsum)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(0.0f, sum, SINGLE_EPS);
}

/**
 * Fortran API specific test
 * Test scsum by comparing it against pre-calculated values
 */
CTEST(scsum, step_1_N_1){
   blasint N = 1, inc = 1;
   float x[] = {1.1f, -1.0f};

   float sum = BLASFUNC(scsum)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(0.1f, sum, SINGLE_EPS);
}

/**
 * Fortran API specific test
 * Test scsum by comparing it against pre-calculated values
 */
CTEST(scsum, step_2_N_1){
   blasint N = 1, inc = 2;
   float x[] = {1.1f, 0.0f, 2.3f, -1.0f};

   float sum = BLASFUNC(scsum)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(1.1f, sum, SINGLE_EPS);
}

/**
 * Fortran API specific test
 * Test scsum by comparing it against pre-calculated values
 */
CTEST(scsum, step_1_N_2){
   blasint N = 2, inc = 1;
   float x[] = {1.1f, -1.0f, 2.3f, -1.0f};

   float sum = BLASFUNC(scsum)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(1.4f, sum, SINGLE_EPS);
}

/**
 * Fortran API specific test
 * Test scsum by comparing it against pre-calculated values
 */
CTEST(scsum, step_2_N_2){
   blasint N = 2, inc = 2;
   float x[] = {1.1f, -1.5f, 1.1f, -1.0f, 1.0f, 1.0f, 1.1f, -1.0f};

   float sum = BLASFUNC(scsum)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(1.6f, sum, SINGLE_EPS);
}

/**
 * Fortran API specific test
 * Test scsum by comparing it against pre-calculated values
 */
CTEST(scsum, step_1_N_3){
   blasint N = 3, inc = 1;
   float x[] = {1.1f, 1.0f, 2.2f, 1.1f, -1.0f, 0.0f};

   float sum = BLASFUNC(scsum)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(4.4f, sum, SINGLE_EPS);
}

/**
 * Fortran API specific test
 * Test scsum by comparing it against pre-calculated values
 */
CTEST(scsum, step_2_N_3){
   blasint N = 3, inc = 2;
   float x[] = {1.1f, 0.0f, -1.0f, 0.0f, -1.0f, -3.0f, -1.0f, 0.0f, 2.2f, 3.0f, -1.0f, 0.0f};

   float sum = BLASFUNC(scsum)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(2.3f, sum, SINGLE_EPS);
}

/**
 * Fortran API specific test
 * Test scsum by comparing it against pre-calculated values
 */
CTEST(scsum, step_1_N_4){
   blasint N = 4, inc = 1;
   float x[] = {1.1f, 1.0f, -2.2f, 3.3f, 1.1f, 1.0f, -2.2f, 3.3f};

   float sum = BLASFUNC(scsum)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(6.4f, sum, SINGLE_EPS);
}

/**
 * Fortran API specific test
 * Test scsum by comparing it against pre-calculated values
 */
CTEST(scsum, step_2_N_4){
   blasint N = 4, inc = 2;
   float x[] = {1.1f, 0.0f, 1.1f, 1.0f, 1.0f, 2.0f, 1.1f, 1.0f, 2.2f, 2.7f, 1.1f, 1.0f, -3.3f, -5.9f};

   float sum = BLASFUNC(scsum)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(-0.2f, sum, SINGLE_EPS);
}

/**
 * Fortran API specific test
 * Test scsum by comparing it against pre-calculated values
 */
CTEST(scsum, step_1_N_5){
   blasint N = 5, inc = 1;
   float x[] = {0.0f, 1.0f, 2.2f, 3.3f, 0.0f, 0.0f, 1.0f, 2.2f, 3.3f, 0.0f};

   float sum = BLASFUNC(scsum)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(13.0f, sum, SINGLE_EPS);
}

/**
 * Fortran API specific test
 * Test scsum by comparing it against pre-calculated values
 */
CTEST(scsum, step_2_N_5){
   blasint N = 5, inc = 2;
   float x[] = {0.0f, 3.0f, 1.0f, 2.2f, 1.0f, -2.2f, 1.0f, 2.2f, 2.2f, -1.7f, 1.0f, 2.2f, 3.3f, 14.5f, 1.0f, 2.2f, 0.0f, -9.0f, 1.0f, 2.2f};

   float sum = BLASFUNC(scsum)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(11.1f, sum, SINGLE_EPS);
}

/**
 * Fortran API specific test
 * Test scsum by comparing it against pre-calculated values
 */
CTEST(scsum, step_1_N_50){
   blasint i;
   blasint N = ELEMENTS, inc = 1;
   float x[ELEMENTS * 2];
   for (i = 0; i < N * inc * 2; i ++) {
      x[i] = (i & 1) ? -1.0f : 1.0f;
   }
   float sum = BLASFUNC(scsum)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(0.0f, sum, SINGLE_EPS);
}

/**
 * Fortran API specific test
 * Test scsum by comparing it against pre-calculated values
 */
CTEST(scsum, step_2_N_50){
   blasint i;
   blasint N = ELEMENTS, inc = INCREMENT;
   float x[ELEMENTS * INCREMENT * 2];
   for (i = 0; i < N * inc * 2; i ++) {
      x[i] = (i & 1) ? -1.0f : 1.0f;
   }
   float sum = BLASFUNC(scsum)(&N, x, &inc);
   ASSERT_DBL_NEAR_TOL(0.0f, sum, SINGLE_EPS);
}

#ifndef NO_CBLAS
/**
 * C API specific test
 * Test scsum by comparing it against pre-calculated values
 */
CTEST(scsum, c_api_bad_args_N_0){
   blasint i;
   blasint N = 0, inc = 1;
   float x[ELEMENTS * 2];
   for (i = 0; i < ELEMENTS * inc * 2; i ++) {
      x[i] = 1000 - i;
   }
   float sum = cblas_scsum(N, x, inc);
   ASSERT_DBL_NEAR_TOL(0.0f, sum, SINGLE_EPS);
}

/**
 * C API specific test
 * Test scsum by comparing it against pre-calculated values
 */
CTEST(scsum, c_api_step_zero){
   blasint i;
   blasint N = ELEMENTS, inc = 0;
   float x[ELEMENTS];
   x[0] = 0.0f;
   for (i = 0; i < N  * inc * 2; i ++) {
      x[i] = i + 1000;
   }
   x[8] = 0.0f;
   float sum = cblas_scsum(N, x, inc);
   ASSERT_DBL_NEAR_TOL(0.0f, sum, SINGLE_EPS);
}

/**
 * C API specific test
 * Test scsum by comparing it against pre-calculated values
 */
CTEST(scsum, c_api_step_1_N_1){
   blasint N = 1, inc = 1;
   float x[] = {1.1f, -1.0f};

   float sum = cblas_scsum(N, x, inc);
   ASSERT_DBL_NEAR_TOL(0.1f, sum, SINGLE_EPS);
}

/**
 * C API specific test
 * Test scsum by comparing it against pre-calculated values
 */
CTEST(scsum, c_api_step_2_N_1){
   blasint N = 1, inc = 2;
   float x[] = {1.1f, 0.0f, 2.3f, -1.0f};

   float sum = cblas_scsum(N, x, inc);
   ASSERT_DBL_NEAR_TOL(1.1f, sum, SINGLE_EPS);
}

/**
 * C API specific test
 * Test scsum by comparing it against pre-calculated values
 */
CTEST(scsum, c_api_step_1_N_2){
   blasint N = 2, inc = 1;
   float x[] = {1.1f, -1.0f, 2.3f, -1.0f};

   float sum = cblas_scsum(N, x, inc);
   ASSERT_DBL_NEAR_TOL(1.4f, sum, SINGLE_EPS);
}

/**
 * C API specific test
 * Test scsum by comparing it against pre-calculated values
 */
CTEST(scsum, c_api_step_2_N_2){
   blasint N = 2, inc = 2;
   float x[] = {1.1f, -1.5f, 1.1f, -1.0f, 1.0f, 1.0f, 1.1f, -1.0f};

   float sum = cblas_scsum(N, x, inc);
   ASSERT_DBL_NEAR_TOL(1.6f, sum, SINGLE_EPS);
}

/**
 * C API specific test
 * Test scsum by comparing it against pre-calculated values
 */
CTEST(scsum, c_api_step_1_N_3){
   blasint N = 3, inc = 1;
   float x[] = {1.1f, 1.0f, 2.2f, 1.1f, -1.0f, 0.0f};

   float sum = cblas_scsum(N, x, inc);
   ASSERT_DBL_NEAR_TOL(4.4f, sum, SINGLE_EPS);
}

/**
 * C API specific test
 * Test scsum by comparing it against pre-calculated values
 */
CTEST(scsum, c_api_step_2_N_3){
   blasint N = 3, inc = 2;
   float x[] = {1.1f, 0.0f, -1.0f, 0.0f, -1.0f, -3.0f, -1.0f, 0.0f, 2.2f, 3.0f, -1.0f, 0.0f};

   float sum = cblas_scsum(N, x, inc);
   ASSERT_DBL_NEAR_TOL(2.3f, sum, SINGLE_EPS);
}

/**
 * C API specific test
 * Test scsum by comparing it against pre-calculated values
 */
CTEST(scsum, c_api_step_1_N_4){
   blasint N = 4, inc = 1;
   float x[] = {1.1f, 1.0f, -2.2f, 3.3f, 1.1f, 1.0f, -2.2f, 3.3f};

   float sum = cblas_scsum(N, x, inc);
   ASSERT_DBL_NEAR_TOL(6.4f, sum, SINGLE_EPS);
}

/**
 * C API specific test
 * Test scsum by comparing it against pre-calculated values
 */
CTEST(scsum, c_api_step_2_N_4){
   blasint N = 4, inc = 2;
   float x[] = {1.1f, 0.0f, 1.1f, 1.0f, 1.0f, 2.0f, 1.1f, 1.0f, 2.2f, 2.7f, 1.1f, 1.0f, -3.3f, -5.9f};

   float sum = cblas_scsum(N, x, inc);
   ASSERT_DBL_NEAR_TOL(-0.2f, sum, SINGLE_EPS);
}

/**
 * C API specific test
 * Test scsum by comparing it against pre-calculated values
 */
CTEST(scsum, c_api_step_1_N_5){
   blasint N = 5, inc = 1;
   float x[] = {0.0f, 1.0f, 2.2f, 3.3f, 0.0f, 0.0f, 1.0f, 2.2f, 3.3f, 0.0f};

   float sum = cblas_scsum(N, x, inc);
   ASSERT_DBL_NEAR_TOL(13.0f, sum, SINGLE_EPS);
}

/**
 * C API specific test
 * Test scsum by comparing it against pre-calculated values
 */
CTEST(scsum, c_api_step_2_N_5){
   blasint N = 5, inc = 2;
   float x[] = {0.0f, 3.0f, 1.0f, 2.2f, 1.0f, -2.2f, 1.0f, 2.2f, 2.2f, -1.7f, 1.0f, 2.2f, 3.3f, 14.5f, 1.0f, 2.2f, 0.0f, -9.0f, 1.0f, 2.2f};

   float sum = cblas_scsum(N, x, inc);
   ASSERT_DBL_NEAR_TOL(11.1f, sum, SINGLE_EPS);
}

/**
 * C API specific test
 * Test scsum by comparing it against pre-calculated values
 */
CTEST(scsum, c_api_step_1_N_50){
   blasint i;
   blasint N = ELEMENTS, inc = 1;
   float x[ELEMENTS * 2];
   for (i = 0; i < N * inc * 2; i ++) {
      x[i] = (i & 1) ? -1.0f : 1.0f;
   }
   float sum = cblas_scsum(N, x, inc);
   ASSERT_DBL_NEAR_TOL(0.0f, sum, SINGLE_EPS);
}

/**
 * C API specific test
 * Test scsum by comparing it against pre-calculated values
 */
CTEST(scsum, c_api_step_2_N_50){
   blasint i;
   blasint N = ELEMENTS, inc = INCREMENT;
   float x[ELEMENTS * INCREMENT * 2];
   for (i = 0; i < N * inc * 2; i ++) {
      x[i] = (i & 1) ? -1.0f : 1.0f;
   }
   float sum = cblas_scsum(N, x, inc);
   ASSERT_DBL_NEAR_TOL(0.0f, sum, SINGLE_EPS);
}
#endif
#endif
