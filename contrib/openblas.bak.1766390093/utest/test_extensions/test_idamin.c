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

#ifdef BUILD_DOUBLE

/**
 * Fortran API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, bad_args_N_0){
   blasint i;
   blasint N = 0, inc = 1;
   double x[ELEMENTS];
   for (i = 0; i < ELEMENTS * inc; i ++) {
      x[i] = 1000 - i;
   }
   blasint index = BLASFUNC(idamin)(&N, x, &inc);
   ASSERT_EQUAL(0, index);
}

/**
 * Fortran API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, step_zero){
   blasint i;
   blasint N = ELEMENTS, inc = 0;
   double x[ELEMENTS];
   for (i = 0; i < N; i ++) {
      x[i] = i + 1000;
   }
   x[8] = 0.0;
   blasint index = BLASFUNC(idamin)(&N, x, &inc);
   ASSERT_EQUAL(0, index);
}

/**
 * Fortran API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, positive_step_1_N_1){
   blasint N = 1, inc = 1;
   double x[] = {1.1};

   blasint index = BLASFUNC(idamin)(&N, x, &inc);
   ASSERT_EQUAL(1, index);
}

/**
 * Fortran API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, negative_step_1_N_1){
   blasint N = 1, inc = 1;
   double x[] = {-1.1};

   blasint index = BLASFUNC(idamin)(&N, x, &inc);
   ASSERT_EQUAL(1, index);
}

/**
 * Fortran API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, positive_step_2_N_1){
   blasint N = 1, inc = 2;
   double x[] = {1.1, 0.0};

   blasint index = BLASFUNC(idamin)(&N, x, &inc);
   ASSERT_EQUAL(1, index);
}

/**
 * Fortran API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, negative_step_2_N_1){
   blasint N = 1, inc = 2;
   double x[] = {-1.1, 0.0};

   blasint index = BLASFUNC(idamin)(&N, x, &inc);
   ASSERT_EQUAL(1, index);
}

/**
 * Fortran API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, positive_step_1_N_2){
   blasint N = 2, inc = 1;
   double x[] = {1.1, 1.0};

   blasint index = BLASFUNC(idamin)(&N, x, &inc);
   ASSERT_EQUAL(2, index);
}

/**
 * Fortran API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, negative_step_1_N_2){
   blasint N = 2, inc = 1;
   double x[] = {-1.1, 1.0};

   blasint index = BLASFUNC(idamin)(&N, x, &inc);
   ASSERT_EQUAL(2, index);
}

/**
 * Fortran API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, positive_step_2_N_2){
   blasint N = 2, inc = 2;
   double x[] = {1.1, 0.0, 1.0, 0.0};

   blasint index = BLASFUNC(idamin)(&N, x, &inc);
   ASSERT_EQUAL(2, index);
}

/**
 * Fortran API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, negative_step_2_N_2){
   blasint N = 2, inc = 2;
   double x[] = {-1.1, 0.0, 1.0, 0.0};

   blasint index = BLASFUNC(idamin)(&N, x, &inc);
   ASSERT_EQUAL(2, index);
}

/**
 * Fortran API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, positive_step_1_N_3){
   blasint N = 3, inc = 1;
   double x[] = {1.1, 1.0, 2.2};

   blasint index = BLASFUNC(idamin)(&N, x, &inc);
   ASSERT_EQUAL(2, index);
}

/**
 * Fortran API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, negative_step_1_N_3){
   blasint N = 3, inc = 1;
   double x[] = {-1.1, 1.0, -2.2};

   blasint index = BLASFUNC(idamin)(&N, x, &inc);
   ASSERT_EQUAL(2, index);
}

/**
 * Fortran API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, positive_step_2_N_3){
   blasint N = 3, inc = 2;
   double x[] = {1.1, 0.0, 1.0, 0.0, 2.2, 0.0};

   blasint index = BLASFUNC(idamin)(&N, x, &inc);
   ASSERT_EQUAL(2, index);
}

/**
 * Fortran API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, negative_step_2_N_3){
   blasint N = 3, inc = 2;
   double x[] = {-1.1, 0.0, 1.0, 0.0, -2.2, 0.0};

   blasint index = BLASFUNC(idamin)(&N, x, &inc);
   ASSERT_EQUAL(2, index);
}

/**
 * Fortran API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, positive_step_1_N_4){
   blasint N = 4, inc = 1;
   double x[] = {1.1, 1.0, 2.2, 3.3};

   blasint index = BLASFUNC(idamin)(&N, x, &inc);
   ASSERT_EQUAL(2, index);
}

/**
 * Fortran API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, negative_step_1_N_4){
   blasint N = 4, inc = 1;
   double x[] = {-1.1, 1.0, -2.2, -3.3};

   blasint index = BLASFUNC(idamin)(&N, x, &inc);
   ASSERT_EQUAL(2, index);
}

/**
 * Fortran API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, positive_step_2_N_4){
   blasint N = 4, inc = 2;
   double x[] = {1.1, 0.0, 1.0, 0.0, 2.2, 0.0, 3.3, 0.0};

   blasint index = BLASFUNC(idamin)(&N, x, &inc);
   ASSERT_EQUAL(2, index);
}

/**
 * Fortran API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, negative_step_2_N_4){
   blasint N = 4, inc = 2;
   double x[] = {-1.1, 0.0, 1.0, 0.0, -2.2, 0.0, -3.3, 0.0};

   blasint index = BLASFUNC(idamin)(&N, x, &inc);
   ASSERT_EQUAL(2, index);
}

/**
 * Fortran API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, positive_step_1_N_5){
   blasint N = 5, inc = 1;
   double x[] = {1.1, 1.0, 2.2, 3.3, 0.0};

   blasint index = BLASFUNC(idamin)(&N, x, &inc);
   ASSERT_EQUAL(5, index);
}

/**
 * Fortran API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, negative_step_1_N_5){
   blasint N = 5, inc = 1;
   double x[] = {-1.1, 1.0, -2.2, -3.3, 0.0};

   blasint index = BLASFUNC(idamin)(&N, x, &inc);
   ASSERT_EQUAL(5, index);
}

/**
 * Fortran API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, positive_step_2_N_5){
   blasint N = 5, inc = 2;
   double x[] = {1.1, 0.0, 1.0, 0.0, 2.2, 0.0, 3.3, 0.0, 0.0, 0.0};

   blasint index = BLASFUNC(idamin)(&N, x, &inc);
   ASSERT_EQUAL(5, index);
}

/**
 * Fortran API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, negative_step_2_N_5){
   blasint N = 5, inc = 2;
   double x[] = {-1.1, 0.0, 1.0, 0.0, -2.2, 0.0, -3.3, 0.0, 0.0, 0.0};

   blasint index = BLASFUNC(idamin)(&N, x, &inc);
   ASSERT_EQUAL(5, index);
}

/**
 * Fortran API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, positive_step_1_N_50){
   blasint i;
   blasint N = ELEMENTS, inc = 1;
   double x[ELEMENTS];
   for (i = 0; i < N * inc; i ++) {
      x[i] = i + 1000;
   }

   x[8 * inc] = 0.0;
   blasint index = BLASFUNC(idamin)(&N, x, &inc);
   ASSERT_EQUAL(9, index);
}

/**
 * Fortran API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, negative_step_1_N_50){
   blasint i;
   blasint N = ELEMENTS, inc = 1;
   double x[ELEMENTS];
   for (i = 0; i < N  * inc; i ++) {
      x[i] = - i - 1000;
   }

   x[8 * inc] = -1.0;
   blasint index = BLASFUNC(idamin)(&N, x, &inc);
   ASSERT_EQUAL(9, index);
}

/**
 * Fortran API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, positive_step_2_N_50){
   blasint i;
   blasint N = ELEMENTS, inc = INCREMENT;
   double x[ELEMENTS * INCREMENT];
   for (i = 0; i < N * inc; i ++) {
      x[i] = i + 1000;
   }

   x[8 * inc] = 0.0;
   blasint index = BLASFUNC(idamin)(&N, x, &inc);
   ASSERT_EQUAL(9, index);
}

/**
 * Fortran API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, negative_step_2_N_50){
   blasint i;
   blasint N = ELEMENTS, inc = INCREMENT;
   double x[ELEMENTS * INCREMENT];
   for (i = 0; i < N * inc; i ++) {
      x[i] = - i - 1000;
   }

   x[8 * inc] = -1.0;
   blasint index = BLASFUNC(idamin)(&N, x, &inc);
   ASSERT_EQUAL(9, index);
}

/**
 * Fortran API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, min_idx_in_vec_tail){
   blasint i;
   blasint N = ELEMENTS, inc = INCREMENT;
   double x[ELEMENTS * INCREMENT];
   for (i = 0; i < N * inc; i ++) {
      x[i] = i + 1000;
   }

   x[(N - 1) * inc] = 0.0;
   blasint index = BLASFUNC(idamin)(&N, x, &inc);
   ASSERT_EQUAL(N, index);
}

/**
 * Fortran API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, min_idx_in_vec_tail_inc_1){
   blasint i;
   blasint N = ELEMENTS, inc = 1;
   double *x = (double*)malloc(ELEMENTS * inc * sizeof(double));
   for (i = 0; i < N * inc; i ++) {
      x[i] = i + 1000;
   }

   x[(N - 1) * inc] = 0.0f;
   blasint index = BLASFUNC(idamin)(&N, x, &inc);
   free(x);
   ASSERT_EQUAL(N, index);
}

#ifndef NO_CBLAS
/**
 * C API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, c_api_bad_args_N_0){
   blasint i;
   blasint N = 0, inc = 1;
   double x[ELEMENTS];
   for (i = 0; i < ELEMENTS * inc; i ++) {
      x[i] = 1000 - i;
   }
   blasint index = cblas_idamin(N, x, inc);
   ASSERT_EQUAL(0, index);
}

/**
 * C API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, c_api_step_zero){
   blasint i;
   blasint N = ELEMENTS, inc = 0;
   double x[ELEMENTS];
   for (i = 0; i < N; i ++) {
      x[i] = i + 1000;
   }
   x[8] = 0.0;
   blasint index = cblas_idamin(N, x, inc);
   ASSERT_EQUAL(0, index);
}

/**
 * C API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, c_api_positive_step_1_N_1){
   blasint N = 1, inc = 1;
   double x[] = {1.1};

   blasint index = cblas_idamin(N, x, inc);
   ASSERT_EQUAL(0, index);
}

/**
 * C API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, c_api_negative_step_1_N_1){
   blasint N = 1, inc = 1;
   double x[] = {-1.1};

   blasint index = cblas_idamin(N, x, inc);
   ASSERT_EQUAL(0, index);
}

/**
 * C API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, c_api_positive_step_2_N_1){
   blasint N = 1, inc = 2;
   double x[] = {1.1, 0.0};

   blasint index = cblas_idamin(N, x, inc);
   ASSERT_EQUAL(0, index);
}

/**
 * C API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, c_api_negative_step_2_N_1){
   blasint N = 1, inc = 2;
   double x[] = {-1.1, 0.0};

   blasint index = cblas_idamin(N, x, inc);
   ASSERT_EQUAL(0, index);
}

/**
 * C API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, c_api_positive_step_1_N_2){
   blasint N = 2, inc = 1;
   double x[] = {1.1, 1.0};

   blasint index = cblas_idamin(N, x, inc);
   ASSERT_EQUAL(1, index);
}

/**
 * C API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, c_api_negative_step_1_N_2){
   blasint N = 2, inc = 1;
   double x[] = {-1.1, 1.0};

   blasint index = cblas_idamin(N, x, inc);
   ASSERT_EQUAL(1, index);
}

/**
 * C API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, c_api_positive_step_2_N_2){
   blasint N = 2, inc = 2;
   double x[] = {1.1, 0.0, 1.0, 0.0};

   blasint index = cblas_idamin(N, x, inc);
   ASSERT_EQUAL(1, index);
}

/**
 * C API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, c_api_negative_step_2_N_2){
   blasint N = 2, inc = 2;
   double x[] = {-1.1, 0.0, 1.0, 0.0};

   blasint index = cblas_idamin(N, x, inc);
   ASSERT_EQUAL(1, index);
}

/**
 * C API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, c_api_positive_step_1_N_3){
   blasint N = 3, inc = 1;
   double x[] = {1.1, 1.0, 2.2};

   blasint index = cblas_idamin(N, x, inc);
   ASSERT_EQUAL(1, index);
}

/**
 * C API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, c_api_negative_step_1_N_3){
   blasint N = 3, inc = 1;
   double x[] = {-1.1, 1.0, -2.2};

   blasint index = cblas_idamin(N, x, inc);
   ASSERT_EQUAL(1, index);
}

/**
 * C API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, c_api_positive_step_2_N_3){
   blasint N = 3, inc = 2;
   double x[] = {1.1, 0.0, 1.0, 0.0, 2.2, 0.0};

   blasint index = cblas_idamin(N, x, inc);
   ASSERT_EQUAL(1, index);
}

/**
 * C API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, c_api_negative_step_2_N_3){
   blasint N = 3, inc = 2;
   double x[] = {-1.1, 0.0, 1.0, 0.0, -2.2, 0.0};

   blasint index = cblas_idamin(N, x, inc);
   ASSERT_EQUAL(1, index);
}

/**
 * C API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, c_api_positive_step_1_N_4){
   blasint N = 4, inc = 1;
   double x[] = {1.1, 1.0, 2.2, 3.3};

   blasint index = cblas_idamin(N, x, inc);
   ASSERT_EQUAL(1, index);
}

/**
 * C API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, c_api_negative_step_1_N_4){
   blasint N = 4, inc = 1;
   double x[] = {-1.1, 1.0, -2.2, -3.3};

   blasint index = cblas_idamin(N, x, inc);
   ASSERT_EQUAL(1, index);
}

/**
 * C API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, c_api_positive_step_2_N_4){
   blasint N = 4, inc = 2;
   double x[] = {1.1, 0.0, 1.0, 0.0, 2.2, 0.0, 3.3, 0.0};

   blasint index = cblas_idamin(N, x, inc);
   ASSERT_EQUAL(1, index);
}

/**
 * C API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, c_api_negative_step_2_N_4){
   blasint N = 4, inc = 2;
   double x[] = {-1.1, 0.0, 1.0, 0.0, -2.2, 0.0, -3.3, 0.0};

   blasint index = cblas_idamin(N, x, inc);
   ASSERT_EQUAL(1, index);
}

/**
 * C API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, c_api_positive_step_1_N_5){
   blasint N = 5, inc = 1;
   double x[] = {1.1, 1.0, 2.2, 3.3, 0.0};

   blasint index = cblas_idamin(N, x, inc);
   ASSERT_EQUAL(4, index);
}

/**
 * C API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, c_api_negative_step_1_N_5){
   blasint N = 5, inc = 1;
   double x[] = {-1.1, 1.0, -2.2, -3.3, 0.0};

   blasint index = cblas_idamin(N, x, inc);
   ASSERT_EQUAL(4, index);
}

/**
 * C API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, c_api_positive_step_2_N_5){
   blasint N = 5, inc = 2;
   double x[] = {1.1, 0.0, 1.0, 0.0, 2.2, 0.0, 3.3, 0.0, 0.0, 0.0};

   blasint index = cblas_idamin(N, x, inc);
   ASSERT_EQUAL(4, index);
}

/**
 * C API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, c_api_negative_step_2_N_5){
   blasint N = 5, inc = 2;
   double x[] = {-1.1, 0.0, 1.0, 0.0, -2.2, 0.0, -3.3, 0.0, 0.0, 0.0};

   blasint index = cblas_idamin(N, x, inc);
   ASSERT_EQUAL(4, index);
}

/**
 * C API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, c_api_positive_step_1_N_50){
   blasint i;
   blasint N = ELEMENTS, inc = 1;
   double x[ELEMENTS];
   for (i = 0; i < N * inc; i ++) {
      x[i] = i + 1000;
   }

   x[8 * inc] = 0.0;
   blasint index = cblas_idamin(N, x, inc);
   ASSERT_EQUAL(8, index);
}

/**
 * C API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, c_api_negative_step_1_N_50){
   blasint i;
   blasint N = ELEMENTS, inc = 1;
   double x[ELEMENTS];
   for (i = 0; i < N  * inc; i ++) {
      x[i] = - i - 1000;
   }

   x[8 * inc] = -1.0;
   blasint index = cblas_idamin(N, x, inc);
   ASSERT_EQUAL(8, index);
}

/**
 * C API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, c_api_positive_step_2_N_50){
   blasint i;
   blasint N = ELEMENTS, inc = INCREMENT;
   double x[ELEMENTS * INCREMENT];
   for (i = 0; i < N * inc; i ++) {
      x[i] = i + 1000;
   }

   x[8 * inc] = 0.0;
   blasint index = cblas_idamin(N, x, inc);
   ASSERT_EQUAL(8, index);
}

/**
 * C API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, c_api_negative_step_2_N_50){
   blasint i;
   blasint N = ELEMENTS, inc = INCREMENT;
   double x[ELEMENTS * INCREMENT];
   for (i = 0; i < N * inc; i ++) {
      x[i] = - i - 1000;
   }

   x[8 * inc] = -1.0;
   blasint index = cblas_idamin(N, x, inc);
   ASSERT_EQUAL(8, index);
}

/**
 * C API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, c_api_min_idx_in_vec_tail){
   blasint i;
   blasint N = ELEMENTS, inc = INCREMENT;
   double x[ELEMENTS * INCREMENT];
   for (i = 0; i < N * inc; i ++) {
      x[i] = i + 1000;
   }

   x[(N - 1) * inc] = 0.0;
   blasint index = cblas_idamin(N, x, inc);
   ASSERT_EQUAL(N - 1, index);
}

/**
 * C API specific test
 * Test idamin by comparing it against pre-calculated values
 */
CTEST(idamin, c_api_min_idx_in_vec_tail_inc_1){
   blasint i;
   blasint N = ELEMENTS, inc = 1;
   double *x = (double*) malloc(ELEMENTS * inc * sizeof(double));
   for (i = 0; i < N * inc; i ++) {
      x[i] = i + 1000;
   }

   x[(N - 1) * inc] = 0.0;
   blasint index = cblas_idamin(N, x, inc);
   free(x);
   ASSERT_EQUAL(N - 1, index);
}
#endif
#endif
