/*Copyright (c) 2013-201\n8, The OpenBLAS Project
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
#if !defined(__VEC__) || !defined(__ALTIVEC__)
#include "../arm/zdot.c"
#else

#include "common.h"
#if defined(POWER10)
#pragma GCC optimize "O1"
#include "cdot_microk_power10.c"
#else
#ifndef HAVE_KERNEL_8
#include <altivec.h> 

#define  offset_0 0
#define  offset_1 16
#define  offset_2 32
#define  offset_3 48



static const unsigned char __attribute__((aligned(16))) swap_mask_arr[]={ 4,5,6,7,0,1,2,3, 12,13,14,15, 8,9,10,11};
static void cdot_kernel_8(BLASLONG n, FLOAT *x, FLOAT *y, float *dot)
{
    __vector unsigned char swap_mask = *((__vector unsigned char*)swap_mask_arr);
    register __vector float *vptr_y = (__vector float *) y;
    register __vector float *vptr_x = (__vector float *) x;
    register __vector float vd_0  = { 0 };
    register __vector float vd_1  = { 0 };
    register __vector float vd_2  = { 0 };
    register __vector float vd_3  = { 0 };
    register __vector float vdd_0 = { 0 };
    register __vector float vdd_1 = { 0 };
    register __vector float vdd_2 = { 0 };
    register __vector float vdd_3 = { 0 };     
    BLASLONG i=0;
    for(;i<n/2;i+=4){ 

        register __vector float vy_0 = vec_vsx_ld( offset_0 ,vptr_y ) ;
        register __vector float vy_1 = vec_vsx_ld( offset_1 ,vptr_y ) ;
        register __vector float vy_2 = vec_vsx_ld( offset_2 ,vptr_y ) ;
        register __vector float vy_3 = vec_vsx_ld( offset_3 ,vptr_y ) ; 

        register __vector float vx_0 = vec_vsx_ld( offset_0 ,vptr_x ) ;
        register __vector float vx_1 = vec_vsx_ld( offset_1 ,vptr_x ) ;
        register __vector float vyy_0 = vec_perm(vy_0, vy_0, swap_mask);
        register __vector float vyy_1 = vec_perm(vy_1, vy_1, swap_mask);
        register __vector float vx_2 = vec_vsx_ld( offset_2 ,vptr_x ) ;
        register __vector float vx_3 = vec_vsx_ld( offset_3 ,vptr_x ) ;  
        register __vector float vyy_2 = vec_perm(vy_2, vy_2, swap_mask);
        register __vector float vyy_3 = vec_perm(vy_3, vy_3, swap_mask);  

        vd_0 += vx_0 * vy_0;
        vd_1 += vx_1 * vy_1;
        vd_2 += vx_2 * vy_2;
        vd_3 += vx_3 * vy_3;

        vdd_0 += vx_0 * vyy_0;
        vdd_1 += vx_1 * vyy_1;
        vdd_2 += vx_2 * vyy_2;
        vdd_3 += vx_3 * vyy_3;       
       
       vptr_x+=4;
       vptr_y+=4;

    }
    //aggregate
    vd_0 = vd_0 + vd_1 +vd_2 +vd_3;
    vdd_0= vdd_0 + vdd_1 +vdd_2 +vdd_3; 
     //reverse and aggregate 
    vd_1=vec_xxpermdi(vd_0,vd_0,2)  ;
    vdd_1=vec_xxpermdi(vdd_0,vdd_0,2);
    vd_2=vd_0+vd_1;
    vdd_2=vdd_0+vdd_1;

    dot[0]=vd_2[0];
    dot[1]=vd_2[1];
    dot[2]=vdd_2[0];
    dot[3]=vdd_2[1];
 
}
#endif
#endif
 

OPENBLAS_COMPLEX_FLOAT CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y) {
    BLASLONG i = 0;
    BLASLONG ix=0, iy=0;
    OPENBLAS_COMPLEX_FLOAT result;
    FLOAT dot[4] __attribute__((aligned(16))) = {0.0, 0.0, 0.0, 0.0};

    if (n <= 0) {
        CREAL(result) = 0.0;
        CIMAG(result) = 0.0;
        return (result);

    }

    if ((inc_x == 1) && (inc_y == 1)) {

#if defined(POWER10)
        BLASLONG n1 = n & -16;
#else
        BLASLONG n1 = n & -8;
#endif
        BLASLONG j=0; 

        if (n1){
            cdot_kernel_8(n1, x, y, dot);
            i = n1;
            j = n1 <<1;
        }
 

        while (i < n) {

            dot[0] += x[j] * y[j];
            dot[1] += x[j + 1] * y[j + 1];
            dot[2] += x[j] * y[j + 1];
            dot[3] += x[j + 1] * y[j];

            j += 2;
            i++;

        }


    } else {
        i = 0;
        ix = 0;
        iy = 0;
        inc_x <<= 1;
        inc_y <<= 1;
        while (i < n) {

            dot[0] += x[ix] * y[iy];
            dot[1] += x[ix + 1] * y[iy + 1];
            dot[2] += x[ix] * y[iy + 1];
            dot[3] += x[ix + 1] * y[iy];

            ix += inc_x;
            iy += inc_y;
            i++;

        }
    }

#if !defined(CONJ)
    CREAL(result) = dot[0] - dot[1];
    CIMAG(result) = dot[2] + dot[3];
#else
    CREAL(result) = dot[0] + dot[1];
    CIMAG(result) = dot[2] - dot[3];

#endif

    return (result);

}
#endif
