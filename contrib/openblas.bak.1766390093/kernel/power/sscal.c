/***************************************************************************
Copyright (c) 2013-2016, The OpenBLAS Project
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

/**************************************************************************************
* 2016/03/27 Werner Saar (wernsaar@googlemail.com)
* 	 BLASTEST 		: OK
* 	 CTEST			: OK
* 	 TEST			: OK
*	 LAPACK-TEST		: OK
**************************************************************************************/

#include "common.h"

#if defined(__VEC__) || defined(__ALTIVEC__)
#if defined(POWER8) || defined(POWER9)
#include "sscal_microk_power8.c"
#elif defined(POWER10)
#include "sscal_microk_power10.c"
#endif
#endif


#if !defined(HAVE_KERNEL_16)

static void sscal_kernel_16 (BLASLONG n, FLOAT *x, FLOAT alpha)
{

        BLASLONG i;

        for( i=0; i<n; i+=8 )
        {
                x[0] *= alpha;
                x[1] *= alpha;
                x[2] *= alpha;
                x[3] *= alpha;
                x[4] *= alpha;
                x[5] *= alpha;
                x[6] *= alpha;
                x[7] *= alpha;
                x+=8;
        }

}

static void sscal_kernel_16_zero( BLASLONG n, FLOAT *x )
{

        BLASLONG i;
	FLOAT alpha=0.0;

        for( i=0; i<n; i+=8 )
        {
		x[0] = alpha;
		x[1] = alpha;
		x[2] = alpha;
		x[3] = alpha;
		x[4] = alpha;
		x[5] = alpha;
		x[6] = alpha;
		x[7] = alpha;
		x[8] = alpha;
		x[9] = alpha;
		x[10] = alpha;
		x[11] = alpha;
		x[12] = alpha;
		x[13] = alpha;
		x[14] = alpha;
		x[15] = alpha;
#if 0
		if (isfinite(x[0]))
			x[0] = alpha;
		else
			x[0] = NAN;
		if (isfinite(x[1]))
                	x[1] = alpha;
		else
			x[1] = NAN;
		if (isfinite(x[2]))
                	x[2] = alpha;
		else
			x[2] = NAN;
		if (isfinite(x[3]))
                	x[3] = alpha;
		else
			x[3] = NAN;
		if (isfinite(x[4]))
                	x[4] = alpha;
		else
			x[4] = NAN;
		if (isfinite(x[5]))
                	x[5] = alpha;
		else
			x[5] = NAN;
		if (isfinite(x[6]))
                	x[6] = alpha;
		else
			x[6] = NAN;
		if (isfinite(x[7]))
                	x[7] = alpha;
		else
			x[7] = NAN;
                x+=8;
#endif
    	}

}


#endif

int CNAME(BLASLONG n, BLASLONG dummy0, BLASLONG dummy1, FLOAT da, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *dummy, BLASLONG dummy2)
{
	BLASLONG i=0,j=0;

	if ( n <= 0 || inc_x <=0 )
		return(0);


	if ( inc_x == 1 )
	{

		if ( da == 0.0 )
		{		

#if defined(POWER10)
			if ( n >= 32 )
			{
				BLASLONG align = ((32 - ((uintptr_t)x & (uintptr_t)0x1F)) >> 2) & 0x7;
				if (dummy2 == 0)
					for (j = 0; j < align; j++){
						x[j] = 0.0;
					}
				else
				for (j = 0; j < align; j++) {
					if (isfinite(x[j]))
						x[j] = 0.0;
					else
						x[j] = NAN;
				}
			}
			BLASLONG n1 = (n-j) & -32;
			if ( n1 > 0 )
			{
				sscal_kernel_16_zero(n1, &x[j]);
				j+=n1;
			}
#else
			BLASLONG n1 = n & -32;
			if ( n1 > 0 )
			{
				sscal_kernel_16_zero(n1, x);
				j=n1;
			}
#endif
			if (dummy2 == 0)
			while(j < n)
			{
				x[j] = 0.0;
				j++;
			}
			else
			while(j < n)
			{	
				if (isfinite(x[j]))
					x[j]=0.0;
				else
					x[j]=NAN;
				j++;
			}

		}
		else
		{

#if defined(POWER10)
			if ( n >= 32 )
			{
				BLASLONG align = ((32 - ((uintptr_t)x & (uintptr_t)0x1F)) >> 2) & 0x7;
				for (j = 0; j < align; j++) {
					x[j] = da * x[j];
				}
			}
			BLASLONG n1 = (n-j) & -32;
			if ( n1 > 0 )
			{
				sscal_kernel_16(n1, &x[j], da);
				j+=n1;
			}
#else
			BLASLONG n1 = n & -32;
			if ( n1 > 0 )
			{
				sscal_kernel_16(n1, x, da);
				j=n1;
			}
#endif
			while(j < n)
			{

				x[j] = da * x[j] ;
				j++;
			}
		}


	}
	else
	{

		if ( da == 0.0 )
		{		
		if (dummy2 == 0)
			while(j < n)
			{
				x[i]=0.0;
				i += inc_x;
				j++;
			}
		else
			while(j < n)
			{
				if (isfinite(x[i]))
					x[i]=0.0;
				else
					x[i]=NAN;
				i += inc_x ;
				j++;
			}

		}
		else
		{

			while(j < n)
			{

				x[i] = da * x[i] ;
				i += inc_x ;
				j++;
			}
		}

	}
	return 0;

}


