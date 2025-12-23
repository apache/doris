/***************************************************************************
Copyright (c) 2014, The OpenBLAS Project
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
#include "../simd/intrin.h"
#if defined(DSDOT)
double CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y)
#else
FLOAT CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y)
#endif
{
	BLASLONG i=0;
	BLASLONG ix=0,iy=0;

#if defined(DSDOT)
	double dot = 0.0 ;
#else
	FLOAT  dot = 0.0 ;
#endif

	if ( n < 1 )  return(dot);

	if ( (inc_x == 1) && (inc_y == 1) )
	{
#if V_SIMD && !defined(DSDOT)
        const int vstep = v_nlanes_f32;
        const int unrollx4 = n & (-vstep * 4);
        const int unrollx  = n &  -vstep;
		v_f32 vsum0 = v_zero_f32();
        v_f32 vsum1 = v_zero_f32();
        v_f32 vsum2 = v_zero_f32();
        v_f32 vsum3 = v_zero_f32();
		while(i < unrollx4)
        {
            vsum0 = v_muladd_f32(
                v_loadu_f32(x + i),           v_loadu_f32(y + i),           vsum0
            );
            vsum1 = v_muladd_f32(
                v_loadu_f32(x + i + vstep),   v_loadu_f32(y + i + vstep),   vsum1
            );
            vsum2 = v_muladd_f32(
                v_loadu_f32(x + i + vstep*2), v_loadu_f32(y + i + vstep*2), vsum2
            );
            vsum3 = v_muladd_f32(
                v_loadu_f32(x + i + vstep*3), v_loadu_f32(y + i + vstep*3), vsum3
            );
            i += vstep*4;
        }
        vsum0 = v_add_f32(
            v_add_f32(vsum0, vsum1), v_add_f32(vsum2 , vsum3)
        );
		while(i < unrollx)
        {
            vsum0 = v_muladd_f32(
                v_loadu_f32(x + i), v_loadu_f32(y + i), vsum0
            );
            i += vstep;
        }
        dot = v_sum_f32(vsum0);
#elif defined(DSDOT)
        int n1 = n & -4;
		for (; i < n1; i += 4)
		{
			dot += (double) y[i] * (double) x[i]
			    + (double) y[i+1] * (double) x[i+1]
			    + (double) y[i+2] * (double) x[i+2]
			    + (double) y[i+3] * (double) x[i+3] ;
		}
#else
        int n1 = n & -4;
		for (; i < n1; i += 4)
		{
			dot += y[i] * x[i]
			    + y[i+1] * x[i+1]
			    + y[i+2] * x[i+2]
			    + y[i+3] * x[i+3] ;
		}
#endif
		while(i < n)
		{

#if defined(DSDOT)
			dot += (double) y[i] * (double) x[i] ;
#else
			dot += y[i] * x[i] ;
#endif
			i++ ;

		}
		return(dot);


	}

	while(i < n)
	{

#if defined(DSDOT)
		dot += (double) y[iy] * (double) x[ix] ;
#else
		dot += y[iy] * x[ix] ;
#endif
		ix  += inc_x ;
		iy  += inc_y ;
		i++ ;

	}
	return(dot);

}


