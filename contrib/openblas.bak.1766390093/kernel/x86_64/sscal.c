/***************************************************************************
Copyright (c) 2013 - 2022, The OpenBLAS Project
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

#if defined(HASWELL) || defined(ZEN)
#include "sscal_microk_haswell-2.c"
#elif  defined (SKYLAKEX) || defined (COOPERLAKE) || defined (SAPPHIRERAPIDS)
#include "sscal_microk_skylakex-2.c"
#endif


#if !defined(HAVE_KERNEL_16)

static void sscal_kernel_16( BLASLONG n, FLOAT *da , FLOAT *x )
{

    BLASLONG i;
    FLOAT alpha = *da;

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


static void sscal_kernel_16_zero( BLASLONG n, FLOAT *alpha , FLOAT *x )
{

    BLASLONG i;
    for( i=0; i<n; i+=8 )
    {
        x[0] = 0.0;
        x[1] = 0.0;
        x[2] = 0.0;
        x[3] = 0.0;
        x[4] = 0.0;
        x[5] = 0.0;
        x[6] = 0.0;
        x[7] = 0.0;
        x+=8;
    }

}

#endif


static void sscal_kernel_inc_8(BLASLONG n, FLOAT *alpha, FLOAT *x, BLASLONG inc_x)  __attribute__ ((noinline));

static void sscal_kernel_inc_8(BLASLONG n, FLOAT *alpha, FLOAT *x, BLASLONG inc_x)
{

    BLASLONG i;
    BLASLONG inc_x2 = 2 * inc_x;
    BLASLONG inc_x3 = inc_x2 + inc_x;
    FLOAT t0,t1,t2,t3;
    FLOAT da = alpha[0];

    for ( i=0; i<n; i+=4 )
    {
        t0 = da * x[0];
        t1 = da * x[inc_x];
        t2 = da * x[inc_x2];
        t3 = da * x[inc_x3];

        x[0]        = t0;
        x[inc_x]    = t1;
        x[inc_x2]   = t2;
        x[inc_x3]   = t3;

        x+=4*inc_x;

    }


}

int CNAME(BLASLONG n, BLASLONG dummy0, BLASLONG dummy1, FLOAT da, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *dummy, BLASLONG dummy2)
{
    BLASLONG i = 0, j = 0;

    // Resolved issue 4728 when the caller is sscal
    if (dummy2 == 1 && da == 0.0)
    {
        if ( inc_x != 1 )
        {
            BLASLONG n1 = n & -8;
            if ( n1 > 0 )
            {
                sscal_kernel_inc_8(n1, &da, x, inc_x);
                i = n1 * inc_x;
                j = n1;
            }
            while(j < n)
            {
                x[i] *= da;
                i += inc_x ;
                j++;
            }
        }
        else
        {
            BLASLONG n1 = n & -16;
            if ( n1 > 0)
                sscal_kernel_16(n1 , &da , x);
            for ( i = n1 ; i < n; i++ )
                x[i] *= da;
        }
    }
    else
    {
        if ( inc_x != 1 )
        {
            if( da == 0.0)
            {
                BLASLONG n1 = n & -2;
                while(j < n1)
                {
                    x[i] = 0.0;
                    x[i+inc_x] = 0.0;
                    i += 2 * inc_x ;
                    j += 2;
                }
                while(j < n)
                {
                    x[i] = 0.0;
                    i += inc_x ;
                    j++;
                }
            }
            else
            {
                BLASLONG n1 = n & -8;
                if ( n1 > 0 )
                {
                    sscal_kernel_inc_8(n1, &da, x, inc_x);
                    i = n1 * inc_x;
                    j = n1;
                }
                while(j < n)
                {
                    x[i] *= da;
                    i += inc_x ;
                    j++;
                }
            }
        }
        else
        {
            if ( da == 0.0 )
            {
                BLASLONG n1 = n & -16;
                if ( n1 > 0)
                   sscal_kernel_16_zero(n1, &da, x);
                for ( i = n1 ; i < n; i++ )
                    x[i] = 0.0;
            }
            else
            {
                BLASLONG n1 = n & -16;
                if ( n1 > 0)
                    sscal_kernel_16(n1 , &da , x);
                for ( i = n1 ; i < n; i++ )
                    x[i] *= da;
            }
        }
    }
    return(0);
}
