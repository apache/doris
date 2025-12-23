/*
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
#ifndef HAVE_ASM_KERNEL
#include <altivec.h> 

#define  offset_0 0
#define  offset_1 16
#define  offset_2 32
#define  offset_3 48
#define  offset_4 64
#define  offset_5 80
#define  offset_6 96
#define  offset_7 112

static const unsigned char __attribute__((aligned(16))) swap_mask_arr[]={ 4,5,6,7,0,1,2,3, 12,13,14,15, 8,9,10,11};

static void caxpy_kernel_16(BLASLONG n, FLOAT *x, FLOAT *y, FLOAT alpha_r, FLOAT alpha_i)
{

#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )

    register __vector float valpha_r = {alpha_r, alpha_r,alpha_r, alpha_r};
    register __vector float valpha_i = {-alpha_i, alpha_i,-alpha_i, alpha_i};

#else
    register __vector float valpha_r = {alpha_r, -alpha_r,alpha_r, -alpha_r};
    register __vector float valpha_i = {alpha_i, alpha_i,alpha_i, alpha_i};
#endif

    __vector unsigned char swap_mask = *((__vector unsigned char*)swap_mask_arr);
    register __vector float *vptr_y = (__vector float *) y;
    register __vector float *vptr_x = (__vector float *) x; 
    BLASLONG i=0;
    for(;i<n/2;i+=8){ 

        register __vector float vy_0 = vec_vsx_ld( offset_0 ,vptr_y ) ;
        register __vector float vy_1 = vec_vsx_ld( offset_1 ,vptr_y ) ;
        register __vector float vy_2 = vec_vsx_ld( offset_2 ,vptr_y ) ;
        register __vector float vy_3 = vec_vsx_ld( offset_3 ,vptr_y ) ;
        register __vector float vy_4 = vec_vsx_ld( offset_4 ,vptr_y ) ;
        register __vector float vy_5 = vec_vsx_ld( offset_5 ,vptr_y ) ;
        register __vector float vy_6 = vec_vsx_ld( offset_6 ,vptr_y ) ;
        register __vector float vy_7 = vec_vsx_ld( offset_7 ,vptr_y ) ;

        register __vector float vx_0 = vec_vsx_ld( offset_0 ,vptr_x ) ;
        register __vector float vx_1 = vec_vsx_ld( offset_1 ,vptr_x ) ;
        register __vector float vx_2 = vec_vsx_ld( offset_2 ,vptr_x ) ;
        register __vector float vx_3 = vec_vsx_ld( offset_3 ,vptr_x ) ;
        register __vector float vx_4 = vec_vsx_ld( offset_4 ,vptr_x ) ;
        register __vector float vx_5 = vec_vsx_ld( offset_5 ,vptr_x ) ;
        register __vector float vx_6 = vec_vsx_ld( offset_6 ,vptr_x ) ;
        register __vector float vx_7 = vec_vsx_ld( offset_7 ,vptr_x ) ;
        vy_0 += vx_0*valpha_r;
        vy_1 += vx_1*valpha_r;
        vy_2 += vx_2*valpha_r;
        vy_3 += vx_3*valpha_r;
        vy_4 += vx_4*valpha_r;
        vy_5 += vx_5*valpha_r;
        vy_6 += vx_6*valpha_r;
        vy_7 += vx_7*valpha_r;
        vx_0 = vec_perm(vx_0, vx_0, swap_mask);
        vx_1 = vec_perm(vx_1, vx_1, swap_mask);
        vx_2 = vec_perm(vx_2, vx_2, swap_mask);
        vx_3 = vec_perm(vx_3, vx_3, swap_mask);
        vx_4 = vec_perm(vx_4, vx_4, swap_mask);
        vx_5 = vec_perm(vx_5, vx_5, swap_mask);
        vx_6 = vec_perm(vx_6, vx_6, swap_mask);
        vx_7 = vec_perm(vx_7, vx_7, swap_mask);
        vy_0 += vx_0*valpha_i;
        vy_1 += vx_1*valpha_i;
        vy_2 += vx_2*valpha_i;
        vy_3 += vx_3*valpha_i;
        vy_4 += vx_4*valpha_i;
        vy_5 += vx_5*valpha_i;
        vy_6 += vx_6*valpha_i;
        vy_7 += vx_7*valpha_i;
        vec_vsx_st( vy_0, offset_0 ,vptr_y ) ;
        vec_vsx_st( vy_1, offset_1 ,vptr_y ) ;
        vec_vsx_st( vy_2, offset_2 ,vptr_y ) ;
        vec_vsx_st( vy_3, offset_3 ,vptr_y ) ;
        vec_vsx_st( vy_4, offset_4 ,vptr_y ) ;
        vec_vsx_st( vy_5, offset_5 ,vptr_y ) ;
        vec_vsx_st( vy_6, offset_6 ,vptr_y ) ;
        vec_vsx_st( vy_7, offset_7 ,vptr_y ) ;   

        vptr_x+=8;
        vptr_y+=8;   
    }
}
#endif
int CNAME(BLASLONG n, BLASLONG dummy0, BLASLONG dummy1, FLOAT da_r, FLOAT da_i, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *dummy, BLASLONG dummy2) {
    BLASLONG i = 0;
    BLASLONG ix = 0, iy = 0;
    if (n <= 0) return (0);
    if ((inc_x == 1) && (inc_y == 1)) {
        BLASLONG n1 = n & -16;
        if (n1) { 
            caxpy_kernel_16(n1, x, y, da_r,da_i);
            ix = 2 * n1;
        }
        i = n1;
        while (i < n) {
#if !defined(CONJ)
            y[ix] += (da_r * x[ix] - da_i * x[ix + 1]);
            y[ix + 1] += (da_r * x[ix + 1] + da_i * x[ix]);
#else
            y[ix] += (da_r * x[ix] + da_i * x[ix + 1]);
            y[ix + 1] -= (da_r * x[ix + 1] - da_i * x[ix]);
#endif
            i++;
            ix += 2;
        }
        return (0);

    }
    inc_x *= 2;
    inc_y *= 2;
    while (i < n) {
#if !defined(CONJ)
        y[iy] += (da_r * x[ix] - da_i * x[ix + 1]);
        y[iy + 1] += (da_r * x[ix + 1] + da_i * x[ix]);
#else
        y[iy] += (da_r * x[ix] + da_i * x[ix + 1]);
        y[iy + 1] -= (da_r * x[ix + 1] - da_i * x[ix]);
#endif
        ix += inc_x;
        iy += inc_y;
        i++;
    }
    return (0);
}

