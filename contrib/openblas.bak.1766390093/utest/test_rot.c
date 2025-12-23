/*****************************************************************************
Copyright (c) 2011-2014, The OpenBLAS Project
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

#ifdef BUILD_DOUBLE
CTEST(rot,drot_inc_0)
{
	blasint i=0;
	blasint N=4,incX=0,incY=0;
	double c=0.25,s=0.5;
	double x1[]={1.0,3.0,5.0,7.0};
	double y1[]={2.0,4.0,6.0,8.0};
	double x2[]={-0.21484375000000,3.0,5.0,7.0};
	double y2[]={ 0.03906250000000,4.0,6.0,8.0};


	//OpenBLAS
	BLASFUNC(drot)(&N,x1,&incX,y1,&incY,&c,&s);

	for(i=0; i<N; i++){
		ASSERT_DBL_NEAR_TOL(x2[i], x1[i], DOUBLE_EPS);
		ASSERT_DBL_NEAR_TOL(y2[i], y1[i], DOUBLE_EPS);
	}
}
CTEST(rot,drot_inc_1)
{
	blasint i=0;
	blasint N=4,incX=1,incY=1;
	double c=1.0,s=1.0;
	double x1[]={1.0,3.0,5.0,7.0};
	double y1[]={2.0,4.0,6.0,8.0};
	double x2[]={3.0,7.0,11.0,15.0};
	double y2[]={1.0,1.0,1.0,1.0};

	BLASFUNC(drot)(&N,x1,&incX,y1,&incY,&c,&s);

	for(i=0; i<N; i++){
		ASSERT_DBL_NEAR_TOL(x2[i], x1[i], DOUBLE_EPS);
		ASSERT_DBL_NEAR_TOL(y2[i], y1[i], DOUBLE_EPS);
	}
}
CTEST(rot,drotm_inc_1)
{
	blasint i = 0;
	blasint N = 12, incX = 1, incY = 1;
	double param[5] = {1.0, 2.0, 3.0, 4.0, 5.0};
	double x_actual[] = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0};
	double y_actual[] = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0};
	double x_referece[] = {3.0, 6.0, 9.0, 12.0, 15.0, 18.0, 21.0, 24.0, 27.0, 30.0, 33.0, 36.0};
	double y_referece[] = {4.0, 8.0, 12.0, 16.0, 20.0, 24.0, 28.0, 32.0, 36.0, 40.0, 44.0, 48.0};

	//OpenBLAS
	BLASFUNC(drotm)(&N, x_actual, &incX, y_actual, &incY, param);

	for(i = 0; i < N; i++){
		ASSERT_DBL_NEAR_TOL(x_referece[i], x_actual[i], DOUBLE_EPS);
		ASSERT_DBL_NEAR_TOL(y_referece[i], y_actual[i], DOUBLE_EPS);
	}
}
#endif

#ifdef BUILD_COMPLEX16
CTEST(rot,zdrot_inc_0)
{
	blasint i=0;
	blasint N=4,incX=0,incY=0;
	double c=0.25,s=0.5;
	double x1[]={1.0,3.0,5.0,7.0,1.0,3.0,5.0,7.0};
	double y1[]={2.0,4.0,6.0,8.0,2.0,4.0,6.0,8.0};
	double x2[]={-0.21484375000000,-0.45703125000000 ,5.0,7.0,1.0,3.0,5.0,7.0};
	double y2[]={ 0.03906250000000, 0.17187500000000 ,6.0,8.0,2.0,4.0,6.0,8.0};
	

	//OpenBLAS
	BLASFUNC(zdrot)(&N,x1,&incX,y1,&incY,&c,&s);

	for(i=0; i<2*N; i++){
		ASSERT_DBL_NEAR_TOL(x2[i], x1[i], DOUBLE_EPS);
		ASSERT_DBL_NEAR_TOL(y2[i], y1[i], DOUBLE_EPS);
	}
}
#endif

#ifdef BUILD_SINGLE
CTEST(rot,srot_inc_0)
{
	blasint i=0;
	blasint N=4,incX=0,incY=0;
	float c=0.25,s=0.5;
	float x1[]={1.0,3.0,5.0,7.0};
	float y1[]={2.0,4.0,6.0,8.0};
	float x2[]={-0.21484375000000,3.0,5.0,7.0};
	float y2[]={ 0.03906250000000,4.0,6.0,8.0};

	//OpenBLAS
	BLASFUNC(srot)(&N,x1,&incX,y1,&incY,&c,&s);

	for(i=0; i<N; i++){
		ASSERT_DBL_NEAR_TOL(x2[i], x1[i], SINGLE_EPS);
		ASSERT_DBL_NEAR_TOL(y2[i], y1[i], SINGLE_EPS);
	}
}
CTEST(rot,srot_inc_1)
{
	blasint i=0;
	blasint N=4,incX=1,incY=1;
	float c=1.0,s=1.0;
	float x1[]={1.0,3.0,5.0,7.0};
	float y1[]={2.0,4.0,6.0,8.0};
	float x2[]={3.0,7.0,11.0,15.0};
	float y2[]={1.0,1.0,1.0,1.0};

	BLASFUNC(srot)(&N,x1,&incX,y1,&incY,&c,&s);

	for(i=0; i<N; i++){
		ASSERT_DBL_NEAR_TOL(x2[i], x1[i], SINGLE_EPS);
		ASSERT_DBL_NEAR_TOL(y2[i], y1[i], SINGLE_EPS);
	}
}
CTEST(rot,srotm_inc_1)
{
	blasint i = 0;
	blasint N = 12, incX = 1, incY = 1;
	float param[5] = {1.0, 2.0, 3.0, 4.0, 5.0};
	float x_actual[] = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0};
	float y_actual[] = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0};
	float x_referece[] = {3.0, 6.0, 9.0, 12.0, 15.0, 18.0, 21.0, 24.0, 27.0, 30.0, 33.0, 36.0};
	float y_referece[] = {4.0, 8.0, 12.0, 16.0, 20.0, 24.0, 28.0, 32.0, 36.0, 40.0, 44.0, 48.0};

	//OpenBLAS
	BLASFUNC(srotm)(&N, x_actual, &incX, y_actual, &incY, param);

	for(i = 0; i < N; i++){
		ASSERT_DBL_NEAR_TOL(x_referece[i], x_actual[i], SINGLE_EPS);
		ASSERT_DBL_NEAR_TOL(y_referece[i], y_actual[i], SINGLE_EPS);
	}
}
#endif

#ifdef BUILD_COMPLEX
CTEST(rot, csrot_inc_0)
{
	blasint i=0;
	blasint N=4,incX=0,incY=0;
	float c=0.25,s=0.5;
	float x1[]={1.0,3.0,5.0,7.0,1.0,3.0,5.0,7.0};
	float y1[]={2.0,4.0,6.0,8.0,2.0,4.0,6.0,8.0};
	float x2[]={-0.21484375000000,-0.45703125000000 ,5.0,7.0,1.0,3.0,5.0,7.0};
	float y2[]={ 0.03906250000000, 0.17187500000000 ,6.0,8.0,2.0,4.0,6.0,8.0};
	
	//OpenBLAS
	BLASFUNC(csrot)(&N,x1,&incX,y1,&incY,&c,&s);

	for(i=0; i<2*N; i++){
		ASSERT_DBL_NEAR_TOL(x2[i], x1[i], SINGLE_EPS);
		ASSERT_DBL_NEAR_TOL(y2[i], y1[i], SINGLE_EPS);
	}
}
#endif

