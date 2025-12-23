/***************************************************************************
Copyright (c) 2015, The OpenBLAS Project
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
derived from this software without specific prior written permission.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE OPENBLAS PROJECT OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*****************************************************************************/

#include "common.h"

#if (defined(__GNUC__) && __GNUC__ > 11) 
#pragma GCC optimize("no-tree-vectorize")
#endif

#if defined(BULLDOZER) 
#include "zdot_microk_bulldozer-2.c"
#elif defined(STEAMROLLER) || defined(PILEDRIVER) || defined(EXCAVATOR)
#include "zdot_microk_steamroller-2.c"
#elif defined(HASWELL) || defined(ZEN) || defined (SKYLAKEX) || defined (COOPERLAKE) || defined (SAPPHIRERAPIDS)
#include "zdot_microk_haswell-2.c"
#elif defined(SANDYBRIDGE)
#include "zdot_microk_sandy-2.c"
#endif


#ifndef HAVE_KERNEL_8

static void zdot_kernel_8(BLASLONG n, FLOAT *x, FLOAT *y, FLOAT *d) __attribute__ ((noinline));

static void zdot_kernel_8(BLASLONG n, FLOAT *x, FLOAT *y, FLOAT *d)
{
	BLASLONG register i = 0;
	FLOAT dot[4] = { 0.0, 0.0, 0.0, 0.0 };
	BLASLONG j=0;

	while( i < n )
        {

            dot[0] += x[j]   * y[j]   ;
            dot[1] += x[j+1] * y[j+1] ;
            dot[2] += x[j]   * y[j+1] ;
            dot[3] += x[j+1] * y[j]   ;

            dot[0] += x[j+2] * y[j+2] ;
            dot[1] += x[j+3] * y[j+3] ;
            dot[2] += x[j+2] * y[j+3] ;
            dot[3] += x[j+3] * y[j+2] ;

            dot[0] += x[j+4] * y[j+4] ;
            dot[1] += x[j+5] * y[j+5] ;
            dot[2] += x[j+4] * y[j+5] ;
            dot[3] += x[j+5] * y[j+4] ;

            dot[0] += x[j+6] * y[j+6] ;
            dot[1] += x[j+7] * y[j+7] ;
            dot[2] += x[j+6] * y[j+7] ;
            dot[3] += x[j+7] * y[j+6] ;

	    j+=8;
            i+=4;

        }
	d[0] = dot[0];
	d[1] = dot[1];
	d[2] = dot[2];
	d[3] = dot[3];

}

#endif


#if defined(SMP)
extern int blas_level1_thread_with_return_value(int mode, BLASLONG m, BLASLONG n,
        BLASLONG k, void *alpha, void *a, BLASLONG lda, void *b, BLASLONG ldb,
        void *c, BLASLONG ldc, int (*function)(void), int nthreads);
#endif
                
                

static void zdot_compute (BLASLONG n, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y,OPENBLAS_COMPLEX_FLOAT *result)
{
	BLASLONG i;
	BLASLONG ix,iy;
	FLOAT  dot[4] = { 0.0, 0.0, 0.0 , 0.0 } ; 
	
	if ( n <= 0 ) 
	{
		OPENBLAS_COMPLEX_FLOAT res=OPENBLAS_MAKE_COMPLEX_FLOAT(0.0,0.0);
		*result=res;
		return;

	}

	if ( (inc_x == 1) && (inc_y == 1) )
	{

		BLASLONG n1 = n & -8;

		if ( n1 )
			zdot_kernel_8(n1, x, y , dot );

		i = n1;
		BLASLONG j = i * 2;

		while( i < n )
		{

			dot[0] += x[j]   * y[j]   ;
			dot[1] += x[j+1] * y[j+1] ;
			dot[2] += x[j]   * y[j+1] ;
			dot[3] += x[j+1] * y[j]   ;

			j+=2;
			i++ ;

		}


	}
	else
	{
		i=0;
		ix=0;
		iy=0;
		inc_x *= 2;
		inc_y *= 2;
		while(i < n)
		{

			dot[0] += x[ix]   * y[iy]   ;
			dot[1] += x[ix+1] * y[iy+1] ;
			dot[2] += x[ix]   * y[iy+1] ;
			dot[3] += x[ix+1] * y[iy]   ;

			ix  += inc_x ;
			iy  += inc_y ;
			i++ ;

		}
	}

#if !defined(CONJ)
	OPENBLAS_COMPLEX_FLOAT res=OPENBLAS_MAKE_COMPLEX_FLOAT(dot[0]-dot[1],dot[2]+dot[3]);
#else
	OPENBLAS_COMPLEX_FLOAT res=OPENBLAS_MAKE_COMPLEX_FLOAT(dot[0]+dot[1],dot[2]-dot[3]);
#endif
        *result=res;
	return;
}

#if defined(SMP)
static int zdot_thread_function(BLASLONG n, BLASLONG dummy0,
BLASLONG dummy1, FLOAT dummy2r, FLOAT dummy2i, FLOAT *x, BLASLONG inc_x, FLOAT *y,
BLASLONG inc_y, FLOAT *result, BLASLONG dummy3)
{
        zdot_compute(n, x, inc_x, y, inc_y, (void *)result);
        return 0;
}
#endif

OPENBLAS_COMPLEX_FLOAT CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y)
{
#if defined(SMP)
	int nthreads;
	FLOAT dummy_alpha;
#if defined(C_PGI) || defined(C_SUN)	
	FLOAT zdotr=0., zdoti=0.;
#endif	
#endif
	
	OPENBLAS_COMPLEX_FLOAT zdot;
#if defined(C_PGI) || defined(C_SUN)	
        zdot=OPENBLAS_MAKE_COMPLEX_FLOAT(0.0,0.0);
#else
	CREAL(zdot) = 0.0;
	CIMAG(zdot) = 0.0;
#endif
	
#if defined(SMP)
	if (inc_x == 0 || inc_y == 0 || n <= 10000)
		nthreads = 1;
	else
		nthreads = num_cpu_avail(1);

	if (nthreads == 1) {
		zdot_compute(n, x, inc_x, y, inc_y, &zdot);
	} else {
		int mode, i;
		char result[MAX_CPU_NUMBER * sizeof(double) * 2];
		OPENBLAS_COMPLEX_FLOAT *ptr;

#if !defined(DOUBLE)
		mode = BLAS_SINGLE  | BLAS_COMPLEX;
#else
		mode = BLAS_DOUBLE  | BLAS_COMPLEX;
#endif

		blas_level1_thread_with_return_value(mode, n, 0, 0, &dummy_alpha,
				   x, inc_x, y, inc_y, result, 0,
				   (int (*)(void))zdot_thread_function, nthreads);

		ptr = (OPENBLAS_COMPLEX_FLOAT *)result;
		for (i = 0; i < nthreads; i++) {
#if defined(C_PGI) || defined(C_SUN)			
			zdotr += CREAL(*ptr);
			zdoti += CIMAG(*ptr);
#else			
			CREAL(zdot) = CREAL(zdot) + CREAL(*ptr);
			CIMAG(zdot) = CIMAG(zdot) + CIMAG(*ptr);
#endif
			ptr = (void *)(((char *)ptr) + sizeof(double) * 2);
		}
#if defined(C_PGI) || defined(C_SUN)		
	zdot = OPENBLAS_MAKE_COMPLEX_FLOAT(zdotr,zdoti);
#endif
	}
#else
	zdot_compute(n, x, inc_x, y, inc_y, &zdot);
#endif
	
	return zdot;
}

