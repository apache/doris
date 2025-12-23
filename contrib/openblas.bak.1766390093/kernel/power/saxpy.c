/***************************************************************************
Copyright (c) 2013-2018, The OpenBLAS Project
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

#define  offset_0 0
#define  offset_1 16
#define  offset_2 32
#define  offset_3 48
#define  offset_4 64
#define  offset_5 80
#define  offset_6 96
#define  offset_7 112
#define  offset_8 128
#define  offset_9 144
#define  offset_10 160
#define  offset_11 176
#define  offset_12 192
#define  offset_13 208
#define  offset_14 224
#define  offset_15 240
 

#if defined(__VEC__) || defined(__ALTIVEC__)

#ifndef HAVE_KERNEL_8
#include <altivec.h> 

static void saxpy_kernel_64(BLASLONG n, FLOAT *x, FLOAT *y, FLOAT alpha)
{
    BLASLONG  i = 0;
    __vector float v_a __attribute((aligned(16))) = {alpha,alpha,alpha,alpha}; 
    __vector float * vptr_y =(__vector float *)y;
    __vector float * vptr_x =(__vector float *)x;
        
    for(; i<n/4; i+=16){


        register __vector float vy_0 = vec_vsx_ld( offset_0 ,vptr_y ) ;
        register __vector float vy_1 = vec_vsx_ld( offset_1 ,vptr_y ) ;
        register __vector float vy_2 = vec_vsx_ld( offset_2 ,vptr_y ) ;
        register __vector float vy_3 = vec_vsx_ld( offset_3 ,vptr_y ) ;
        register __vector float vy_4 = vec_vsx_ld( offset_4 ,vptr_y ) ;
        register __vector float vy_5 = vec_vsx_ld( offset_5 ,vptr_y ) ;
        register __vector float vy_6 = vec_vsx_ld( offset_6 ,vptr_y ) ;
        register __vector float vy_7 = vec_vsx_ld( offset_7 ,vptr_y ) ;
        register __vector float vy_8 = vec_vsx_ld( offset_8 ,vptr_y ) ;
        register __vector float vy_9 = vec_vsx_ld( offset_9 ,vptr_y ) ;
        register __vector float vy_10 = vec_vsx_ld( offset_10 ,vptr_y ) ;
        register __vector float vy_11 = vec_vsx_ld( offset_11 ,vptr_y ) ;
        register __vector float vy_12 = vec_vsx_ld( offset_12 ,vptr_y ) ;
        register __vector float vy_13 = vec_vsx_ld( offset_13 ,vptr_y ) ;
        register __vector float vy_14 = vec_vsx_ld( offset_14 ,vptr_y ) ;
        register __vector float vy_15 = vec_vsx_ld( offset_15 ,vptr_y ) ;

        register __vector float vx_0 = vec_vsx_ld( offset_0 ,vptr_x ) ;
        register __vector float vx_1 = vec_vsx_ld( offset_1 ,vptr_x ) ;
        register __vector float vx_2 = vec_vsx_ld( offset_2 ,vptr_x ) ;
        register __vector float vx_3 = vec_vsx_ld( offset_3 ,vptr_x ) ;
        register __vector float vx_4 = vec_vsx_ld( offset_4 ,vptr_x ) ;
        register __vector float vx_5 = vec_vsx_ld( offset_5 ,vptr_x ) ;
        register __vector float vx_6 = vec_vsx_ld( offset_6 ,vptr_x ) ;
        register __vector float vx_7 = vec_vsx_ld( offset_7 ,vptr_x ) ;
        register __vector float vx_8 = vec_vsx_ld( offset_8 ,vptr_x ) ;
        register __vector float vx_9 = vec_vsx_ld( offset_9 ,vptr_x ) ;
        register __vector float vx_10 = vec_vsx_ld( offset_10 ,vptr_x ) ;
        register __vector float vx_11 = vec_vsx_ld( offset_11 ,vptr_x ) ;
        register __vector float vx_12 = vec_vsx_ld( offset_12 ,vptr_x ) ;
        register __vector float vx_13 = vec_vsx_ld( offset_13 ,vptr_x ) ;
        register __vector float vx_14 = vec_vsx_ld( offset_14 ,vptr_x ) ;
        register __vector float vx_15 = vec_vsx_ld( offset_15 ,vptr_x ) ;
        vy_0 += vx_0*v_a;
        vy_1 += vx_1*v_a;
        vy_2 += vx_2*v_a;
        vy_3 += vx_3*v_a;
        vy_4 += vx_4*v_a;
        vy_5 += vx_5*v_a;
        vy_6 += vx_6*v_a;
        vy_7 += vx_7*v_a;
        vy_8 += vx_8*v_a;
        vy_9 += vx_9*v_a;
        vy_10 += vx_10*v_a;
        vy_11 += vx_11*v_a;
        vy_12 += vx_12*v_a;
        vy_13 += vx_13*v_a;
        vy_14 += vx_14*v_a;
        vy_15 += vx_15*v_a;

    	vec_vsx_st( vy_0, offset_0 ,vptr_y ) ;
        vec_vsx_st( vy_1, offset_1 ,vptr_y ) ;
        vec_vsx_st( vy_2, offset_2 ,vptr_y ) ;
        vec_vsx_st( vy_3, offset_3 ,vptr_y ) ;
        vec_vsx_st( vy_4, offset_4 ,vptr_y ) ;
        vec_vsx_st( vy_5, offset_5 ,vptr_y ) ;
        vec_vsx_st( vy_6, offset_6 ,vptr_y ) ;
	vec_vsx_st( vy_7, offset_7 ,vptr_y ) ; 
    	vec_vsx_st( vy_8, offset_8 ,vptr_y ) ;
        vec_vsx_st( vy_9, offset_9 ,vptr_y ) ;
        vec_vsx_st( vy_10, offset_10 ,vptr_y ) ;
        vec_vsx_st( vy_11, offset_11 ,vptr_y ) ;
        vec_vsx_st( vy_12, offset_12 ,vptr_y ) ;
        vec_vsx_st( vy_13, offset_13 ,vptr_y ) ;
        vec_vsx_st( vy_14, offset_14 ,vptr_y ) ;
	vec_vsx_st( vy_15, offset_15 ,vptr_y ) ; 

        vptr_x+=16;
	vptr_y+=16; 

/*

        v_y[i]    += v_a * v_x[i];
        v_y[i+1]  += v_a * v_x[i+1];
        v_y[i+2]  += v_a * v_x[i+2];
        v_y[i+3]  += v_a * v_x[i+3];
        v_y[i+4]  += v_a * v_x[i+4];
        v_y[i+5]  += v_a * v_x[i+5];
        v_y[i+6]  += v_a * v_x[i+6];
        v_y[i+7]  += v_a * v_x[i+7]; 
        v_y[i+8]  += v_a * v_x[i+8];
        v_y[i+9]  += v_a * v_x[i+9];
        v_y[i+10] += v_a * v_x[i+10];
        v_y[i+11] += v_a * v_x[i+11];
        v_y[i+12] += v_a * v_x[i+12];
        v_y[i+13] += v_a * v_x[i+13];
        v_y[i+14] += v_a * v_x[i+14];
        v_y[i+15] += v_a * v_x[i+15];
*/
    }
}
#endif
#endif

int CNAME(BLASLONG n, BLASLONG dummy0, BLASLONG dummy1, FLOAT da, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *dummy, BLASLONG dummy2)
{
	BLASLONG i=0;
	BLASLONG ix=0,iy=0;

	if ( n <= 0 )  return(0);

	if ( (inc_x == 1) && (inc_y == 1) )
	{

		BLASLONG n1 = n & -64;
#if defined(__VEC__) || defined(__ALTIVEC__)

		if ( n1 )
			saxpy_kernel_64(n1, x, y, da);

		i = n1;
#endif
		while(i < n)
		{

			y[i] += da * x[i] ;
			i++ ;

		}
		return(0);


	}

	BLASLONG n1 = n & -4;

	while(i < n1)
	{

		FLOAT m1      = da * x[ix] ;
		FLOAT m2      = da * x[ix+inc_x] ;
		FLOAT m3      = da * x[ix+2*inc_x] ;
		FLOAT m4      = da * x[ix+3*inc_x] ;

		y[iy]         += m1 ;
		y[iy+inc_y]   += m2 ;
		y[iy+2*inc_y] += m3 ;
		y[iy+3*inc_y] += m4 ;

		ix  += inc_x*4 ;
		iy  += inc_y*4 ;
		i+=4 ;

	}

	while(i < n)
	{

		y[iy] += da * x[ix] ;
		ix  += inc_x ;
		iy  += inc_y ;
		i++ ;

	}
	return(0);

}



