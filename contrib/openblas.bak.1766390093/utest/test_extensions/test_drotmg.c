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

#ifdef BUILD_DOUBLE

/**
 * Fortran API specific test
 * Test drotmg by comparing it against pre-calculated values
 */
CTEST(drotmg, y1_zero)
{
	double te_d1, tr_d1;
	double te_d2, tr_d2;
	double te_x1, tr_x1;
	double te_y1, tr_y1;
	double te_param[5];
	double tr_param[5];
	int i = 0;
	te_d1 = tr_d1 = 2.0;
	te_d2 = tr_d2 = 2.0;
	te_x1 = tr_x1 = 8.0;
	te_y1 = tr_y1 = 0.0;

	for(i=0; i<5; i++){
	  te_param[i] = tr_param[i] = 0.0;
	}
	
	//reference values as calculated by netlib blas
	tr_d1 = 2.0;
	tr_d2 = 2.0;
	tr_x1 = 8.0;
	tr_y1 = 0.0;

	tr_param[0] = -2.0;
	tr_param[1] = 0.0;
	tr_param[2] = 0.0;
	tr_param[3] = 0.0;
	tr_param[4] = 0.0;

	//OpenBLAS
	BLASFUNC(drotmg)(&te_d1, &te_d2, &te_x1, &te_y1, te_param);

	ASSERT_DBL_NEAR_TOL(tr_d1, te_d1, DOUBLE_EPS);
	ASSERT_DBL_NEAR_TOL(tr_d2, te_d2, DOUBLE_EPS);
	ASSERT_DBL_NEAR_TOL(tr_x1, te_x1, DOUBLE_EPS);
	ASSERT_DBL_NEAR_TOL(tr_y1, te_y1, DOUBLE_EPS);

	for(i=0; i<5; i++){
		ASSERT_DBL_NEAR_TOL(tr_param[i], te_param[i], DOUBLE_EPS);
	}
}

/**
 * Fortran API specific test
 * Test drotmg by comparing it against pre-calculated values
 */
CTEST(drotmg, d1_negative)
{
	double te_d1, tr_d1;
	double te_d2, tr_d2;
	double te_x1, tr_x1;
	double te_y1, tr_y1;
	double te_param[5];
	double tr_param[5];
	int i = 0;
	te_d1 = tr_d1 = -1.0;
	te_d2 = tr_d2 = 2.0;
	te_x1 = tr_x1 = 8.0;
	te_y1 = tr_y1 = 8.0;

	for(i=0; i<5; i++){
	  te_param[i] = tr_param[i] = 0.0;
	}
	
	//reference values as calculated by netlib blas
	tr_d1 = 0.0;
	tr_d2 = 0.0;
	tr_x1 = 0.0;
	tr_y1 = 8.0;

	tr_param[0] = -1.0;
	tr_param[1] = 0.0;
	tr_param[2] = 0.0;
	tr_param[3] = 0.0;
	tr_param[4] = 0.0;

	//OpenBLAS
	BLASFUNC(drotmg)(&te_d1, &te_d2, &te_x1, &te_y1, te_param);

	ASSERT_DBL_NEAR_TOL(tr_d1, te_d1, DOUBLE_EPS);
	ASSERT_DBL_NEAR_TOL(tr_d2, te_d2, DOUBLE_EPS);
	ASSERT_DBL_NEAR_TOL(tr_x1, te_x1, DOUBLE_EPS);
	ASSERT_DBL_NEAR_TOL(tr_y1, te_y1, DOUBLE_EPS);

	for(i=0; i<5; i++){
		ASSERT_DBL_NEAR_TOL(tr_param[i], te_param[i], DOUBLE_EPS);
	}
}

/**
 * Fortran API specific test
 * Test drotmg by comparing it against pre-calculated values
 */
CTEST(drotmg, d1_positive_d2_positive_x1_zero)
{
	double te_d1, tr_d1;
	double te_d2, tr_d2;
	double te_x1, tr_x1;
	double te_y1, tr_y1;
	double te_param[5];
	double tr_param[5];
	int i = 0;
	te_d1 = tr_d1 = 2.0;
	te_d2 = tr_d2 = 2.0;
	te_x1 = tr_x1 = 0.0;
	te_y1 = tr_y1 = 8.0;

	for(i=0; i<5; i++){
	  te_param[i] = tr_param[i] = 0.0;
	}
	
	//reference values as calculated by netlib blas
	tr_d1 = 2.0;
	tr_d2 = 2.0;
	tr_x1 = 8.0;
	tr_y1 = 8.0;

	tr_param[0] = 1.0;
	tr_param[1] = 0.0;
	tr_param[2] = 0.0;
	tr_param[3] = 0.0;
	tr_param[4] = 0.0;

	//OpenBLAS
	BLASFUNC(drotmg)(&te_d1, &te_d2, &te_x1, &te_y1, te_param);

	ASSERT_DBL_NEAR_TOL(tr_d1, te_d1, DOUBLE_EPS);
	ASSERT_DBL_NEAR_TOL(tr_d2, te_d2, DOUBLE_EPS);
	ASSERT_DBL_NEAR_TOL(tr_x1, te_x1, DOUBLE_EPS);
	ASSERT_DBL_NEAR_TOL(tr_y1, te_y1, DOUBLE_EPS);

	for(i=0; i<5; i++){
		ASSERT_DBL_NEAR_TOL(tr_param[i], te_param[i], DOUBLE_EPS);
	}
}

/**
 * Fortran API specific test
 * Test drotmg by comparing it against pre-calculated values
 */
CTEST(drotmg, scaled_y_greater_than_scaled_x)
{
	double te_d1, tr_d1;
	double te_d2, tr_d2;
	double te_x1, tr_x1;
	double te_y1, tr_y1;
	double te_param[5];
	double tr_param[5];
	int i = 0;
	te_d1 = tr_d1 = 1.0;
	te_d2 = tr_d2 = -2.0;
	te_x1 = tr_x1 = 8.0;
	te_y1 = tr_y1 = 8.0;

	for(i=0; i<5; i++){
	  te_param[i] = tr_param[i] = 0.0;
	}
	
	//reference values as calculated by netlib blas
	tr_d1 = 0.0;
	tr_d2 = 0.0;
	tr_x1 = 0.0;
	tr_y1 = 8.0;

	tr_param[0] = -1.0;
	tr_param[1] = 0.0;
	tr_param[2] = 0.0;
	tr_param[3] = 0.0;
	tr_param[4] = 0.0;

	//OpenBLAS
	BLASFUNC(drotmg)(&te_d1, &te_d2, &te_x1, &te_y1, te_param);

	ASSERT_DBL_NEAR_TOL(tr_d1, te_d1, DOUBLE_EPS);
	ASSERT_DBL_NEAR_TOL(tr_d2, te_d2, DOUBLE_EPS);
	ASSERT_DBL_NEAR_TOL(tr_x1, te_x1, DOUBLE_EPS);
	ASSERT_DBL_NEAR_TOL(tr_y1, te_y1, DOUBLE_EPS);

	for(i=0; i<5; i++){
		ASSERT_DBL_NEAR_TOL(tr_param[i], te_param[i], DOUBLE_EPS);
	}
}

#ifndef NO_CBLAS
/**
 * C API specific test
 * Test drotmg by comparing it against pre-calculated values
 */
CTEST(drotmg, c_api_y1_zero)
{
	double te_d1, tr_d1;
	double te_d2, tr_d2;
	double te_x1, tr_x1;
	double te_y1, tr_y1;
	double te_param[5];
	double tr_param[5];
	int i = 0;
	te_d1 = tr_d1 = 2.0;
	te_d2 = tr_d2 = 2.0;
	te_x1 = tr_x1 = 8.0;
	te_y1 = tr_y1 = 0.0;

	for(i=0; i<5; i++){
	  te_param[i] = tr_param[i] = 0.0;
	}
	
	//reference values as calculated by netlib blas
	tr_d1 = 2.0;
	tr_d2 = 2.0;
	tr_x1 = 8.0;
	tr_y1 = 0.0;

	tr_param[0] = -2.0;
	tr_param[1] = 0.0;
	tr_param[2] = 0.0;
	tr_param[3] = 0.0;
	tr_param[4] = 0.0;

	//OpenBLAS
	cblas_drotmg(&te_d1, &te_d2, &te_x1, te_y1, te_param);

	ASSERT_DBL_NEAR_TOL(tr_d1, te_d1, DOUBLE_EPS);
	ASSERT_DBL_NEAR_TOL(tr_d2, te_d2, DOUBLE_EPS);
	ASSERT_DBL_NEAR_TOL(tr_x1, te_x1, DOUBLE_EPS);
	ASSERT_DBL_NEAR_TOL(tr_y1, te_y1, DOUBLE_EPS);

	for(i=0; i<5; i++){
		ASSERT_DBL_NEAR_TOL(tr_param[i], te_param[i], DOUBLE_EPS);
	}
}

/**
 * C API specific test
 * Test drotmg by comparing it against pre-calculated values
 */
CTEST(drotmg, c_api_d1_negative)
{
	double te_d1, tr_d1;
	double te_d2, tr_d2;
	double te_x1, tr_x1;
	double te_y1, tr_y1;
	double te_param[5];
	double tr_param[5];
	int i = 0;
	te_d1 = tr_d1 = -1.0;
	te_d2 = tr_d2 = 2.0;
	te_x1 = tr_x1 = 8.0;
	te_y1 = tr_y1 = 8.0;

	for(i=0; i<5; i++){
	  te_param[i] = tr_param[i] = 0.0;
	}
	
	//reference values as calculated by netlib blas
	tr_d1 = 0.0;
	tr_d2 = 0.0;
	tr_x1 = 0.0;
	tr_y1 = 8.0;

	tr_param[0] = -1.0;
	tr_param[1] = 0.0;
	tr_param[2] = 0.0;
	tr_param[3] = 0.0;
	tr_param[4] = 0.0;

	//OpenBLAS
	cblas_drotmg(&te_d1, &te_d2, &te_x1, te_y1, te_param);

	ASSERT_DBL_NEAR_TOL(tr_d1, te_d1, DOUBLE_EPS);
	ASSERT_DBL_NEAR_TOL(tr_d2, te_d2, DOUBLE_EPS);
	ASSERT_DBL_NEAR_TOL(tr_x1, te_x1, DOUBLE_EPS);
	ASSERT_DBL_NEAR_TOL(tr_y1, te_y1, DOUBLE_EPS);

	for(i=0; i<5; i++){
		ASSERT_DBL_NEAR_TOL(tr_param[i], te_param[i], DOUBLE_EPS);
	}
}

/**
 * C API specific test
 * Test drotmg by comparing it against pre-calculated values
 */
CTEST(drotmg, c_api_d1_positive_d2_positive_x1_zero)
{
	double te_d1, tr_d1;
	double te_d2, tr_d2;
	double te_x1, tr_x1;
	double te_y1, tr_y1;
	double te_param[5];
	double tr_param[5];
	int i = 0;
	te_d1 = tr_d1 = 2.0;
	te_d2 = tr_d2 = 2.0;
	te_x1 = tr_x1 = 0.0;
	te_y1 = tr_y1 = 8.0;

	for(i=0; i<5; i++){
	  te_param[i] = tr_param[i] = 0.0;
	}
	
	//reference values as calculated by netlib blas
	tr_d1 = 2.0;
	tr_d2 = 2.0;
	tr_x1 = 8.0;
	tr_y1 = 8.0;

	tr_param[0] = 1.0;
	tr_param[1] = 0.0;
	tr_param[2] = 0.0;
	tr_param[3] = 0.0;
	tr_param[4] = 0.0;

	//OpenBLAS
	cblas_drotmg(&te_d1, &te_d2, &te_x1, te_y1, te_param);

	ASSERT_DBL_NEAR_TOL(tr_d1, te_d1, DOUBLE_EPS);
	ASSERT_DBL_NEAR_TOL(tr_d2, te_d2, DOUBLE_EPS);
	ASSERT_DBL_NEAR_TOL(tr_x1, te_x1, DOUBLE_EPS);
	ASSERT_DBL_NEAR_TOL(tr_y1, te_y1, DOUBLE_EPS);

	for(i=0; i<5; i++){
		ASSERT_DBL_NEAR_TOL(tr_param[i], te_param[i], DOUBLE_EPS);
	}
}

/**
 * C API specific test
 * Test drotmg by comparing it against pre-calculated values
 */
CTEST(drotmg, c_api_scaled_y_greater_than_scaled_x)
{
	double te_d1, tr_d1;
	double te_d2, tr_d2;
	double te_x1, tr_x1;
	double te_y1, tr_y1;
	double te_param[5];
	double tr_param[5];
	int i = 0;
	te_d1 = tr_d1 = 1.0;
	te_d2 = tr_d2 = -2.0;
	te_x1 = tr_x1 = 8.0;
	te_y1 = tr_y1 = 8.0;

	for(i=0; i<5; i++){
	  te_param[i] = tr_param[i] = 0.0;
	}
	
	//reference values as calculated by netlib blas
	tr_d1 = 0.0;
	tr_d2 = 0.0;
	tr_x1 = 0.0;
	tr_y1 = 8.0;

	tr_param[0] = -1.0;
	tr_param[1] = 0.0;
	tr_param[2] = 0.0;
	tr_param[3] = 0.0;
	tr_param[4] = 0.0;

	//OpenBLAS
	cblas_drotmg(&te_d1, &te_d2, &te_x1, te_y1, te_param);

	ASSERT_DBL_NEAR_TOL(tr_d1, te_d1, DOUBLE_EPS);
	ASSERT_DBL_NEAR_TOL(tr_d2, te_d2, DOUBLE_EPS);
	ASSERT_DBL_NEAR_TOL(tr_x1, te_x1, DOUBLE_EPS);
	ASSERT_DBL_NEAR_TOL(tr_y1, te_y1, DOUBLE_EPS);

	for(i=0; i<5; i++){
		ASSERT_DBL_NEAR_TOL(tr_param[i], te_param[i], DOUBLE_EPS);
	}
}
#endif
#endif
