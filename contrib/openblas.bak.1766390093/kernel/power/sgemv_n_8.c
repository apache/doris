/***************************************************************************
Copyright (c) 2019, The OpenBLAS Project
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


/****Note***
UnUsed kernel
This kernel works. But it was not competitive enough to be added in production
It could be used and tested in future or could provide barebone for switching to inline assembly
*/

#include "common.h"

#define NBMAX 4096

static void sgemv_kernel_8x8(BLASLONG n, FLOAT **ap, FLOAT *xo, FLOAT *y, BLASLONG lda4, FLOAT *alpha)
{

    BLASLONG i;
	FLOAT *a0,*a1,*a2,*a3,*b0,*b1,*b2,*b3; 
    FLOAT x0,x1,x2,x3,x4,x5,x6,x7;
	a0 = ap[0];
	a1 = ap[1];
	a2 = ap[2];
	a3 = ap[3]; 
    b0 = a0 + lda4 ;
	b1 = a1 + lda4 ;
	b2 = a2 + lda4 ;
	b3 = a3 + lda4 ;
    x0 = xo[0] * *alpha;
    x1 = xo[1] * *alpha;
    x2 = xo[2] * *alpha;
    x3 = xo[3] * *alpha;
    x4 = xo[4] * *alpha;
    x5 = xo[5] * *alpha;
    x6 = xo[6] * *alpha;
    x7 = xo[7] * *alpha;
    __vector float* va0 = (__vector float*)a0;
    __vector float* va1 = (__vector float*)a1;
    __vector float* va2 = (__vector float*)a2;
    __vector float* va3 = (__vector float*)a3;
    __vector float* vb0 = (__vector float*)b0;
    __vector float* vb1 = (__vector float*)b1;
    __vector float* vb2 = (__vector float*)b2;
    __vector float* vb3 = (__vector float*)b3; 
    
    register __vector float   v_x0 = {x0,x0,x0,x0};
    register __vector float   v_x1 = {x1,x1,x1,x1};
    register __vector float   v_x2 = {x2,x2,x2,x2};
    register __vector float   v_x3 = {x3,x3,x3,x3};
    register __vector float   v_x4 = {x4,x4,x4,x4};
    register __vector float   v_x5 = {x5,x5,x5,x5};
    register __vector float   v_x6 = {x6,x6,x6,x6};
    register __vector float   v_x7 = {x7,x7,x7,x7};
    __vector float* v_y =(__vector float*)y;   
 
    for ( i=0; i< n/4; i+=2)
    {
        register __vector float vy_1=v_y[i];
        register __vector float vy_2=v_y[i+1];
        register __vector float va0_1=va0[i] ; 
        register __vector float va0_2=va0[i+1] ; 
        register __vector float va1_1=va1[i] ; 
        register __vector float va1_2=va1[i+1] ; 
        register __vector float va2_1=va2[i] ; 
        register __vector float va2_2=va2[i+1] ; 
        register __vector float va3_1=va3[i] ; 
        register __vector float va3_2=va3[i+1] ;
        register __vector float vb0_1=vb0[i] ; 
        register __vector float vb0_2=vb0[i+1] ; 
        register __vector float vb1_1=vb1[i] ; 
        register __vector float vb1_2=vb1[i+1] ; 
        register __vector float vb2_1=vb2[i] ; 
        register __vector float vb2_2=vb2[i+1] ; 
        register __vector float vb3_1=vb3[i] ; 
        register __vector float vb3_2=vb3[i+1] ;         
        vy_1   += v_x0 * va0_1  +  v_x1 * va1_1  + v_x2 * va2_1  + v_x3 * va3_1 ;
        vy_1   += v_x4 * vb0_1   +  v_x5 * vb1_1   + v_x6 * vb2_1   + v_x7 * vb3_1 ;
        vy_2   +=  v_x0 * va0_2   +  v_x1 * va1_2   + v_x2 * va2_2   + v_x3 * va3_2 ; 
        vy_2   += v_x4 * vb0_2   +  v_x5 * vb1_2   + v_x6 * vb2_2   + v_x7 * vb3_2 ;
        v_y[i] =vy_1;
        v_y[i+1] =vy_2;   
    }

}
	 
static void sgemv_kernel_8x4(BLASLONG n, FLOAT **ap, FLOAT *xo, FLOAT *y, FLOAT *alpha)
{
    BLASLONG i;
    FLOAT x0,x1,x2,x3;
    x0 = xo[0] * *alpha;
    x1 = xo[1] * *alpha;
    x2 = xo[2] * *alpha;
    x3 = xo[3] * *alpha;
    __vector float   v_x0 = {x0,x0,x0,x0};
    __vector float   v_x1 = {x1,x1,x1,x1};
    __vector float   v_x2 = {x2,x2,x2,x2};
    __vector float   v_x3 = {x3,x3,x3,x3};
    __vector float* v_y =(__vector float*)y;      
    __vector float* va0 = (__vector float*)ap[0];
    __vector float* va1 = (__vector float*)ap[1];
    __vector float* va2 = (__vector float*)ap[2];
    __vector float* va3 = (__vector float*)ap[3]; 
 
    for ( i=0; i< n/4; i+=2 )
    {
        register __vector float vy_1=v_y[i];
        register __vector float vy_2=v_y[i+1];
        register __vector float va0_1=va0[i] ; 
        register __vector float va0_2=va0[i+1] ; 
        register __vector float va1_1=va1[i] ; 
        register __vector float va1_2=va1[i+1] ; 
        register __vector float va2_1=va2[i] ; 
        register __vector float va2_2=va2[i+1] ; 
        register __vector float va3_1=va3[i] ; 
        register __vector float va3_2=va3[i+1] ;      
        vy_1   += v_x0 * va0_1  +  v_x1 * va1_1  + v_x2 * va2_1  + v_x3 * va3_1 ;
        vy_2   +=  v_x0 * va0_2   +  v_x1 * va1_2   + v_x2 * va2_2   + v_x3 * va3_2 ;
        v_y[i] =vy_1;
        v_y[i+1] =vy_2;   
    }
  
} 

static void sgemv_kernel_8x2( BLASLONG n, FLOAT **ap, FLOAT *x, FLOAT *y, FLOAT *alpha)
{

    BLASLONG i;
    FLOAT x0,x1;
    x0 = x[0] * *alpha;
    x1 = x[1] * *alpha; 
    __vector float   v_x0 = {x0,x0,x0,x0};
    __vector float   v_x1 = {x1,x1,x1,x1}; 
    __vector float* v_y =(__vector float*)y;      
    __vector float* va0 = (__vector float*)ap[0];
    __vector float* va1 = (__vector float*)ap[1]; 
 
    for ( i=0; i< n/4; i+=2 )
    { 
        v_y[i]   += v_x0 * va0[i]   +  v_x1 * va1[i] ;
        v_y[i+1]  += v_x0 * va0[i+1]   +  v_x1 * va1[i+1] ;     
    }

} 
 
 
static void sgemv_kernel_8x1(BLASLONG n, FLOAT *ap, FLOAT *x, FLOAT *y, FLOAT *alpha)
{

    BLASLONG i;
    FLOAT x0 ;
    x0 = x[0] * *alpha; 
    __vector float   v_x0 = {x0,x0,x0,x0}; 
    __vector float* v_y =(__vector float*)y;      
    __vector float* va0 = (__vector float*)ap; 
 
    for ( i=0; i< n/4; i+=2 )
    { 
        v_y[i]   += v_x0 * va0[i]   ;
        v_y[i+1] +=   v_x0 * va0[i+1]   ;        
    }

}
 
static void add_y(BLASLONG n, FLOAT *src, FLOAT *dest, BLASLONG inc_dest)
{
    BLASLONG i;
        
    for ( i=0; i<n; i++ ){
            *dest += *src;
            src++;
            dest += inc_dest;
    }
    return;
     

}

int CNAME(BLASLONG m, BLASLONG n, BLASLONG dummy1, FLOAT alpha, FLOAT *a, BLASLONG lda, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *buffer)
{
	BLASLONG i;
	FLOAT *a_ptr;
	FLOAT *x_ptr;
	FLOAT *y_ptr;
	FLOAT *ap[4];
	BLASLONG n1;
	BLASLONG m1;
	BLASLONG m2;
	BLASLONG m3;
	BLASLONG n2;
	BLASLONG lda4 =  lda << 2;
	BLASLONG lda8 =  lda << 3;
	FLOAT xbuffer[8] __attribute__((aligned(16)));
	FLOAT *ybuffer;

        if ( m < 1 ) return(0);
        if ( n < 1 ) return(0);

	ybuffer = buffer;
	
        if ( inc_x == 1 )
	{
		n1 = n >> 3 ;
		n2 = n &  7 ;
	}
	else
	{
		n1 = n >> 2 ;
		n2 = n &  3 ;

	}
	 
        m3 = m & 7  ;
        m1 = m - m3;
        m2 = (m & (NBMAX-1)) - m3 ;


	y_ptr = y;

	BLASLONG NB = NBMAX;

	while ( NB == NBMAX )
	{
		
		m1 -= NB;
		if ( m1 < 0)
		{
			if ( m2 == 0 ) break;	
			NB = m2;
		}
		
		a_ptr = a;
		x_ptr = x;
		
		ap[0] = a_ptr;
		ap[1] = a_ptr + lda;
		ap[2] = ap[1] + lda;
		ap[3] = ap[2] + lda;

		if ( inc_y != 1 )
			memset(ybuffer,0,NB*4);
		else
			ybuffer = y_ptr;

		if ( inc_x == 1 )
		{


			for( i = 0; i < n1 ; i++)
			{
				sgemv_kernel_8x8(NB,ap,x_ptr,ybuffer,lda4,&alpha);
				ap[0] += lda8; 
				ap[1] += lda8; 
				ap[2] += lda8; 
				ap[3] += lda8; 
				a_ptr += lda8;
				x_ptr += 8;	
			}


			if ( n2 & 4 )
			{
				sgemv_kernel_8x4(NB,ap,x_ptr,ybuffer,&alpha);
				ap[0] += lda4; 
				ap[1] += lda4; 
				ap[2] += lda4; 
				ap[3] += lda4; 
				a_ptr += lda4;
				x_ptr += 4;	
			}

			if ( n2 & 2 )
			{
				sgemv_kernel_8x2(NB,ap,x_ptr,ybuffer,&alpha);
				a_ptr += lda*2;
				x_ptr += 2;	
			}


			if ( n2 & 1 )
			{
				sgemv_kernel_8x1(NB,a_ptr,x_ptr,ybuffer,&alpha); 
                a_ptr += lda;
                x_ptr += 1;   
			}


		}
		else
		{

			for( i = 0; i < n1 ; i++)
			{
				xbuffer[0] = x_ptr[0];
				x_ptr += inc_x;	
				xbuffer[1] =  x_ptr[0];
				x_ptr += inc_x;	
				xbuffer[2] =  x_ptr[0];
				x_ptr += inc_x;	
				xbuffer[3] = x_ptr[0];
				x_ptr += inc_x;	
				sgemv_kernel_8x4(NB,ap,xbuffer,ybuffer,&alpha);
				ap[0] += lda4; 
				ap[1] += lda4; 
				ap[2] += lda4; 
				ap[3] += lda4; 
				a_ptr += lda4;
			}

			for( i = 0; i < n2 ; i++)
			{
				xbuffer[0] = x_ptr[0];
				x_ptr += inc_x;	
				sgemv_kernel_8x1(NB,a_ptr,xbuffer,ybuffer,&alpha);
				a_ptr += lda;

			}

		}

		a     += NB;
		if ( inc_y != 1 )
		{
			add_y(NB,ybuffer,y_ptr,inc_y);
			y_ptr += NB * inc_y;
		}
		else
			y_ptr += NB ;

	}

	 
	if ( m3 & 4 )
	{
		a_ptr = a;
		x_ptr = x;
		FLOAT temp0 = 0.0;
		FLOAT temp1 = 0.0;
		FLOAT temp2 = 0.0;
		FLOAT temp3 = 0.0;		
		if ( lda == 4 && inc_x ==1 )
		{

			for( i = 0; i < ( n & -4 ); i+=4 )
			{

				temp0 += a_ptr[0] * x_ptr[0] + a_ptr[4] * x_ptr[1];
				temp1 += a_ptr[1] * x_ptr[0] + a_ptr[5] * x_ptr[1];
				temp2 += a_ptr[2] * x_ptr[0] + a_ptr[6] * x_ptr[1];
				temp3 += a_ptr[3] * x_ptr[0] + a_ptr[7] * x_ptr[1];

				temp0 += a_ptr[8] * x_ptr[2] + a_ptr[12]  * x_ptr[3];
				temp1 += a_ptr[9] * x_ptr[2] + a_ptr[13] * x_ptr[3];
				temp2 += a_ptr[10] * x_ptr[2] + a_ptr[14] * x_ptr[3];
				temp3 += a_ptr[11] * x_ptr[2] + a_ptr[15] * x_ptr[3];

				a_ptr += 16;
				x_ptr += 4;
			}

			for( ; i < n; i++ )
			{
				temp0 += a_ptr[0] * x_ptr[0];
				temp1 += a_ptr[1] * x_ptr[0];
				temp2 += a_ptr[2] * x_ptr[0];
				temp3 += a_ptr[3] * x_ptr[0] ;
				a_ptr +=4;
				x_ptr ++;
			}

		}
		else
		{

			for( i = 0; i < n; i++ )
			{
				temp0 += a_ptr[0] * x_ptr[0];
				temp1 += a_ptr[1] * x_ptr[0];
				temp2 += a_ptr[2] * x_ptr[0];
				temp3 += a_ptr[3] * x_ptr[0];
				a_ptr += lda;
				x_ptr += inc_x;


			}

		}
		y_ptr[0] += alpha * temp0;
		y_ptr += inc_y;
		y_ptr[0] += alpha * temp1;
		y_ptr += inc_y;
		y_ptr[0] += alpha * temp2;
		y_ptr += inc_y;
		y_ptr[0] += alpha * temp3; 
		y_ptr += inc_y;
        a     += 4;
	}


	if ( m3 & 2 )
	{
		a_ptr = a;
		x_ptr = x;
		FLOAT temp0 = 0.0;
		FLOAT temp1 = 0.0;
		if ( lda == 2 && inc_x ==1 )
		{

			for( i = 0; i < (n & -4) ; i+=4 )
			{
				temp0 += a_ptr[0] * x_ptr[0] + a_ptr[2] * x_ptr[1];
				temp1 += a_ptr[1] * x_ptr[0] + a_ptr[3] * x_ptr[1];
				temp0 += a_ptr[4] * x_ptr[2] + a_ptr[6] * x_ptr[3];
				temp1 += a_ptr[5] * x_ptr[2] + a_ptr[7] * x_ptr[3];
				a_ptr += 8;
				x_ptr += 4;

			}


			for( ; i < n; i++ )
			{
				temp0 += a_ptr[0]   * x_ptr[0];
				temp1 += a_ptr[1]   * x_ptr[0];
				a_ptr += 2;
				x_ptr ++;
			}

		}
		else
		{

			for( i = 0; i < n; i++ )
			{
				temp0 += a_ptr[0] * x_ptr[0];
				temp1 += a_ptr[1] * x_ptr[0];
				a_ptr += lda;
				x_ptr += inc_x;


			}

		}
		y_ptr[0] += alpha * temp0;
		y_ptr += inc_y;
		y_ptr[0] += alpha * temp1;
 		y_ptr += inc_y;
        a     += 2;
	}

	if ( m3 & 1 )
	{
		a_ptr = a;
		x_ptr = x;
		FLOAT temp = 0.0;
		if ( lda == 1 && inc_x ==1 )
		{

			for( i = 0; i < (n & -4); i+=4 )
			{
				temp += a_ptr[i] * x_ptr[i] + a_ptr[i+1] * x_ptr[i+1] + a_ptr[i+2] * x_ptr[i+2] + a_ptr[i+3] * x_ptr[i+3];
	
			}

			for( ; i < n; i++ )
			{
				temp += a_ptr[i] * x_ptr[i];
			}

		}
		else
		{

			for( i = 0; i < n; i++ )
			{
				temp += a_ptr[0] * x_ptr[0];
				a_ptr += lda;
				x_ptr += inc_x;
			}

		}
		y_ptr[0] += alpha * temp;
 
 
	}


	return(0);
}


