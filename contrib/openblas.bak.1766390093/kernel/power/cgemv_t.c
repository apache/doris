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
#if !defined(__VEC__) || !defined(__ALTIVEC__)
#include "../arm/zgemv_t.c"
#else

#include "common.h"

#define NBMAX 1024 
#include <altivec.h> 
static const unsigned char __attribute__((aligned(16))) swap_mask_arr[]={ 4,5,6,7,0,1,2,3, 12,13,14,15, 8,9,10,11};

static void cgemv_kernel_4x4(BLASLONG n, BLASLONG lda, FLOAT *ap, FLOAT *x, FLOAT *y, FLOAT alpha_r, FLOAT alpha_i) {

    FLOAT *a0, *a1, *a2, *a3;
    a0 = ap;
    a1 = ap + lda;
    a2 = a1 + lda;
    a3 = a2 + lda;
    __vector unsigned char swap_mask = *((__vector unsigned char*)swap_mask_arr);
    //p for positive(real*real,image*image,real*real,image*image) r for image (real*image,image*real,real*image,image*real)
    register __vector float vtemp0_p = {0.0, 0.0,0.0,0.0};
    register __vector float vtemp0_r = {0.0, 0.0,0.0,0.0};
    register __vector float vtemp1_p = {0.0, 0.0,0.0,0.0};
    register __vector float vtemp1_r = {0.0, 0.0,0.0,0.0};
    register __vector float vtemp2_p = {0.0, 0.0,0.0,0.0};
    register __vector float vtemp2_r = {0.0, 0.0,0.0,0.0};
    register __vector float vtemp3_p = {0.0, 0.0,0.0,0.0};
    register __vector float vtemp3_r = {0.0, 0.0,0.0,0.0};
    __vector float* vptr_a0 = (__vector float*) a0;
    __vector float* vptr_a1 = (__vector float*) a1;
    __vector float* vptr_a2 = (__vector float*) a2;
    __vector float* vptr_a3 = (__vector float*) a3;
    __vector float* v_x = (__vector float*) x;

    BLASLONG  i = 0;
    BLASLONG  i2 = 16;  
    for (;i< n * 8; i+=32, i2+=32) { 
        register __vector float vx_0  = vec_vsx_ld( i,v_x) ; 
        register __vector float vx_1  = vec_vsx_ld(i2, v_x); 
                
        register __vector float vxr_0 = vec_perm(vx_0, vx_0, swap_mask);
        register __vector float vxr_1 = vec_perm(vx_1, vx_1, swap_mask);

        register __vector float va0   = vec_vsx_ld(i,vptr_a0);
        register __vector float va1   = vec_vsx_ld(i, vptr_a1);
        register __vector float va2   = vec_vsx_ld(i ,vptr_a2);
        register __vector float va3   = vec_vsx_ld(i ,vptr_a3);
        register __vector float va0_1 = vec_vsx_ld(i2 ,vptr_a0);
        register __vector float va1_1 = vec_vsx_ld(i2 ,vptr_a1);
        register __vector float va2_1 = vec_vsx_ld(i2 ,vptr_a2);
        register __vector float va3_1 = vec_vsx_ld(i2 ,vptr_a3);


        vtemp0_p += vx_0*va0 + vx_1*va0_1 ;
        vtemp0_r += vxr_0*va0 + vxr_1*va0_1; 
        vtemp1_p += vx_0*va1 + vx_1*va1_1;
        vtemp1_r += vxr_0*va1 + vxr_1*va1_1; 
        vtemp2_p += vx_0*va2 + vx_1*va2_1;
        vtemp2_r += vxr_0*va2 + vxr_1*va2_1; 
        vtemp3_p += vx_0*va3 + vx_1*va3_1;
        vtemp3_r += vxr_0*va3 + vxr_1*va3_1; 

    }

#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )

    register FLOAT temp_r0 = vtemp0_p[0] - vtemp0_p[1] + vtemp0_p[2] - vtemp0_p[3];
    register FLOAT temp_i0 = vtemp0_r[0] + vtemp0_r[1] + vtemp0_r[2] + vtemp0_r[3];

    register FLOAT temp_r1 = vtemp1_p[0] - vtemp1_p[1] + vtemp1_p[2] - vtemp1_p[3];
    register FLOAT temp_i1 = vtemp1_r[0] + vtemp1_r[1] + vtemp1_r[2] + vtemp1_r[3];

    register FLOAT temp_r2 = vtemp2_p[0] - vtemp2_p[1] + vtemp2_p[2] - vtemp2_p[3];
    register FLOAT temp_i2 = vtemp2_r[0] + vtemp2_r[1] + vtemp2_r[2] + vtemp2_r[3];

    register FLOAT temp_r3 = vtemp3_p[0] - vtemp3_p[1] + vtemp3_p[2] - vtemp3_p[3];
    register FLOAT temp_i3 = vtemp3_r[0] + vtemp3_r[1] + vtemp3_r[2] + vtemp3_r[3];

#else
    register FLOAT temp_r0 = vtemp0_p[0] + vtemp0_p[1] + vtemp0_p[2] + vtemp0_p[3];
    register FLOAT temp_i0 = vtemp0_r[0] - vtemp0_r[1] + vtemp0_r[2] - vtemp0_r[3];

    register FLOAT temp_r1 = vtemp1_p[0] + vtemp1_p[1] + vtemp1_p[2] + vtemp1_p[3];
    register FLOAT temp_i1 = vtemp1_r[0] - vtemp1_r[1] + vtemp1_r[2] - vtemp1_r[3];

    register FLOAT temp_r2 = vtemp2_p[0] + vtemp2_p[1] + vtemp2_p[2] + vtemp2_p[3];
    register FLOAT temp_i2 = vtemp2_r[0] - vtemp2_r[1] + vtemp2_r[2] - vtemp2_r[3];

    register FLOAT temp_r3 = vtemp3_p[0] + vtemp3_p[1] + vtemp3_p[2] + vtemp3_p[3];
    register FLOAT temp_i3 = vtemp3_r[0] - vtemp3_r[1] + vtemp3_r[2] - vtemp3_r[3];

#endif    

#if !defined(XCONJ)

    y[0] += alpha_r * temp_r0 - alpha_i * temp_i0;
    y[1] += alpha_r * temp_i0 + alpha_i * temp_r0;
    y[2] += alpha_r * temp_r1 - alpha_i * temp_i1;
    y[3] += alpha_r * temp_i1 + alpha_i * temp_r1;
    y[4] += alpha_r * temp_r2 - alpha_i * temp_i2;
    y[5] += alpha_r * temp_i2 + alpha_i * temp_r2;
    y[6] += alpha_r * temp_r3 - alpha_i * temp_i3;
    y[7] += alpha_r * temp_i3 + alpha_i * temp_r3;

#else

    y[0] += alpha_r * temp_r0 + alpha_i * temp_i0;
    y[1] -= alpha_r * temp_i0 - alpha_i * temp_r0;
    y[2] += alpha_r * temp_r1 + alpha_i * temp_i1;
    y[3] -= alpha_r * temp_i1 - alpha_i * temp_r1;
    y[4] += alpha_r * temp_r2 + alpha_i * temp_i2;
    y[5] -= alpha_r * temp_i2 - alpha_i * temp_r2;
    y[6] += alpha_r * temp_r3 + alpha_i * temp_i3;
    y[7] -= alpha_r * temp_i3 - alpha_i * temp_r3;

#endif

}
 

static void cgemv_kernel_4x2(BLASLONG n, BLASLONG lda, FLOAT *ap, FLOAT *x, FLOAT *y, FLOAT alpha_r, FLOAT alpha_i) {

    FLOAT *a0, *a1;
    a0 = ap;
    a1 = ap + lda; 
    __vector unsigned char swap_mask = *((__vector unsigned char*)swap_mask_arr);
    //p for positive(real*real,image*image,real*real,image*image) r for image (real*image,image*real,real*image,image*real)
    register __vector float vtemp0_p = {0.0, 0.0,0.0,0.0};
    register __vector float vtemp0_r = {0.0, 0.0,0.0,0.0};
    register __vector float vtemp1_p = {0.0, 0.0,0.0,0.0};
    register __vector float vtemp1_r = {0.0, 0.0,0.0,0.0}; 


    __vector float* vptr_a0 = (__vector float*) a0;
    __vector float* vptr_a1 = (__vector float*) a1; 
    __vector float* v_x = (__vector float*) x;

    BLASLONG  i = 0;
    BLASLONG  i2 = 16;  
    for (;i< n * 8; i+=32, i2+=32) { 
        register __vector float vx_0  = vec_vsx_ld( i,v_x) ; 
        register __vector float vx_1  = vec_vsx_ld(i2, v_x); 
                
        register __vector float vxr_0 = vec_perm(vx_0, vx_0, swap_mask);
        register __vector float vxr_1 = vec_perm(vx_1, vx_1, swap_mask);

        register __vector float va0   = vec_vsx_ld(i,vptr_a0);
        register __vector float va1   = vec_vsx_ld(i, vptr_a1); 
        register __vector float va0_1 = vec_vsx_ld(i2 ,vptr_a0);
        register __vector float va1_1 = vec_vsx_ld(i2 ,vptr_a1); 


        vtemp0_p += vx_0*va0 + vx_1*va0_1 ;
        vtemp0_r += vxr_0*va0 + vxr_1*va0_1; 
        vtemp1_p += vx_0*va1 + vx_1*va1_1;
        vtemp1_r += vxr_0*va1 + vxr_1*va1_1;  

    }
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )

    register FLOAT temp_r0 = vtemp0_p[0] - vtemp0_p[1] + vtemp0_p[2] - vtemp0_p[3];
    register FLOAT temp_i0 = vtemp0_r[0] + vtemp0_r[1] + vtemp0_r[2] + vtemp0_r[3];

    register FLOAT temp_r1 = vtemp1_p[0] - vtemp1_p[1] + vtemp1_p[2] - vtemp1_p[3];
    register FLOAT temp_i1 = vtemp1_r[0] + vtemp1_r[1] + vtemp1_r[2] + vtemp1_r[3];
 

#else
    register FLOAT temp_r0 = vtemp0_p[0] + vtemp0_p[1] + vtemp0_p[2] + vtemp0_p[3];
    register FLOAT temp_i0 = vtemp0_r[0] - vtemp0_r[1] + vtemp0_r[2] - vtemp0_r[3];

    register FLOAT temp_r1 = vtemp1_p[0] + vtemp1_p[1] + vtemp1_p[2] + vtemp1_p[3];
    register FLOAT temp_i1 = vtemp1_r[0] - vtemp1_r[1] + vtemp1_r[2] - vtemp1_r[3]; 

#endif    

#if !defined(XCONJ)

    y[0] += alpha_r * temp_r0 - alpha_i * temp_i0;
    y[1] += alpha_r * temp_i0 + alpha_i * temp_r0;
    y[2] += alpha_r * temp_r1 - alpha_i * temp_i1;
    y[3] += alpha_r * temp_i1 + alpha_i * temp_r1; 

#else

    y[0] += alpha_r * temp_r0 + alpha_i * temp_i0;
    y[1] -= alpha_r * temp_i0 - alpha_i * temp_r0;
    y[2] += alpha_r * temp_r1 + alpha_i * temp_i1;
    y[3] -= alpha_r * temp_i1 - alpha_i * temp_r1; 

#endif
  
}
 

static void cgemv_kernel_4x1(BLASLONG n, FLOAT *ap, FLOAT *x, FLOAT *y, FLOAT alpha_r, FLOAT alpha_i) {
 
    __vector unsigned char swap_mask = *((__vector unsigned char*)swap_mask_arr);
    //p for positive(real*real,image*image,real*real,image*image) r for image (real*image,image*real,real*image,image*real)
    register __vector float vtemp0_p = {0.0, 0.0,0.0,0.0};
    register __vector float vtemp0_r = {0.0, 0.0,0.0,0.0}; 
    __vector float* vptr_a0 = (__vector float*) ap; 
    __vector float* v_x = (__vector float*) x;
    BLASLONG  i = 0;
    BLASLONG  i2 = 16;  
    for (;i< n * 8; i+=32, i2+=32) { 
        register __vector float vx_0  = vec_vsx_ld( i,v_x) ; 
        register __vector float vx_1  = vec_vsx_ld(i2, v_x); 
                
        register __vector float vxr_0 = vec_perm(vx_0, vx_0, swap_mask);
        register __vector float vxr_1 = vec_perm(vx_1, vx_1, swap_mask);

        register __vector float va0   = vec_vsx_ld(i,vptr_a0); 
        register __vector float va0_1 = vec_vsx_ld(i2 ,vptr_a0);  

        vtemp0_p += vx_0*va0 + vx_1*va0_1 ;
        vtemp0_r += vxr_0*va0 + vxr_1*va0_1;  
    }

#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )

    register FLOAT temp_r0 = vtemp0_p[0] - vtemp0_p[1] + vtemp0_p[2] - vtemp0_p[3];
    register FLOAT temp_i0 = vtemp0_r[0] + vtemp0_r[1] + vtemp0_r[2] + vtemp0_r[3]; 

#else
    register FLOAT temp_r0 = vtemp0_p[0] + vtemp0_p[1] + vtemp0_p[2] + vtemp0_p[3];
    register FLOAT temp_i0 = vtemp0_r[0] - vtemp0_r[1] + vtemp0_r[2] - vtemp0_r[3]; 

#endif    

#if !defined(XCONJ)

    y[0] += alpha_r * temp_r0 - alpha_i * temp_i0;
    y[1] += alpha_r * temp_i0 + alpha_i * temp_r0; 

#else

    y[0] += alpha_r * temp_r0 + alpha_i * temp_i0;
    y[1] -= alpha_r * temp_i0 - alpha_i * temp_r0; 

#endif


}
 
static void copy_x(BLASLONG n, FLOAT *src, FLOAT *dest, BLASLONG inc_src) {
    BLASLONG i;
    for (i = 0; i < n; i++) {
        *dest = *src;
        *(dest + 1) = *(src + 1);
        dest += 2;
        src += inc_src;
    }
}

int CNAME(BLASLONG m, BLASLONG n, BLASLONG dummy1, FLOAT alpha_r, FLOAT alpha_i, FLOAT *a, BLASLONG lda, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *buffer) {
    BLASLONG i=0;
    BLASLONG j=0;
    FLOAT *a_ptr;
    FLOAT *x_ptr;
    FLOAT *y_ptr;

    BLASLONG n1;
    BLASLONG m1;
    BLASLONG m2;
    BLASLONG m3;
    BLASLONG n2;
    FLOAT ybuffer[8] __attribute__((aligned(16)));
    FLOAT *xbuffer;

    if (m < 1) return (0);
    if (n < 1) return (0);

    inc_x <<= 1;
    inc_y <<= 1;
    lda <<= 1;

    xbuffer = buffer;

    n1 = n >> 2;
    n2 = n & 3;

    m3 = m & 3;
    m1 = m - m3;
    m2 = (m & (NBMAX - 1)) - m3;

    BLASLONG NB = NBMAX;

    while (NB == NBMAX) {

        m1 -= NB;
        if (m1 < 0) {
            if (m2 == 0) break;
            NB = m2;
        }

        y_ptr = y;
        a_ptr = a;
        x_ptr = x;

        if (inc_x != 2)
            copy_x(NB, x_ptr, xbuffer, inc_x);
        else
            xbuffer = x_ptr;

        if (inc_y == 2) {

            for (i = 0; i < n1; i++) {
                cgemv_kernel_4x4(NB, lda, a_ptr, xbuffer, y_ptr, alpha_r, alpha_i);
                a_ptr += lda << 2;
                y_ptr += 8;

            }

            if (n2 & 2) {
                cgemv_kernel_4x2(NB, lda, a_ptr, xbuffer, y_ptr, alpha_r, alpha_i);
                a_ptr += lda << 1;
                y_ptr += 4;

            }

            if (n2 & 1) {
                cgemv_kernel_4x1(NB, a_ptr, xbuffer, y_ptr, alpha_r, alpha_i);
                a_ptr += lda;
                y_ptr += 2;

            }

        } else {

            for (i = 0; i < n1; i++) {
                memset(ybuffer, 0, sizeof (ybuffer));
                cgemv_kernel_4x4(NB, lda, a_ptr, xbuffer, ybuffer, alpha_r, alpha_i);

                a_ptr += lda << 2;

                y_ptr[0] += ybuffer[0];
                y_ptr[1] += ybuffer[1];
                y_ptr += inc_y;
                y_ptr[0] += ybuffer[2];
                y_ptr[1] += ybuffer[3];
                y_ptr += inc_y;
                y_ptr[0] += ybuffer[4];
                y_ptr[1] += ybuffer[5];
                y_ptr += inc_y;
                y_ptr[0] += ybuffer[6];
                y_ptr[1] += ybuffer[7];
                y_ptr += inc_y;

            }

            for (i = 0; i < n2; i++) {
                memset(ybuffer, 0, sizeof (ybuffer));
                cgemv_kernel_4x1(NB, a_ptr, xbuffer, ybuffer, alpha_r, alpha_i);
                a_ptr += lda;
                y_ptr[0] += ybuffer[0];
                y_ptr[1] += ybuffer[1];
                y_ptr += inc_y;

            }

        }
        a += 2 * NB;
        x += NB * inc_x;
    }

    if (m3 == 0) return (0);

    x_ptr = x;
    j = 0;
    a_ptr = a;
    y_ptr = y;

    if (m3 == 3) {

        FLOAT temp_r;
        FLOAT temp_i;
        FLOAT x0 = x_ptr[0];
        FLOAT x1 = x_ptr[1];
        x_ptr += inc_x;
        FLOAT x2 = x_ptr[0];
        FLOAT x3 = x_ptr[1];
        x_ptr += inc_x;
        FLOAT x4 = x_ptr[0];
        FLOAT x5 = x_ptr[1];
        while (j < n) {
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
            temp_r = a_ptr[0] * x0 - a_ptr[1] * x1;
            temp_i = a_ptr[0] * x1 + a_ptr[1] * x0;
            temp_r += a_ptr[2] * x2 - a_ptr[3] * x3;
            temp_i += a_ptr[2] * x3 + a_ptr[3] * x2;
            temp_r += a_ptr[4] * x4 - a_ptr[5] * x5;
            temp_i += a_ptr[4] * x5 + a_ptr[5] * x4;
#else

            temp_r = a_ptr[0] * x0 + a_ptr[1] * x1;
            temp_i = a_ptr[0] * x1 - a_ptr[1] * x0;
            temp_r += a_ptr[2] * x2 + a_ptr[3] * x3;
            temp_i += a_ptr[2] * x3 - a_ptr[3] * x2;
            temp_r += a_ptr[4] * x4 + a_ptr[5] * x5;
            temp_i += a_ptr[4] * x5 - a_ptr[5] * x4;
#endif

#if !defined(XCONJ) 
            y_ptr[0] += alpha_r * temp_r - alpha_i * temp_i;
            y_ptr[1] += alpha_r * temp_i + alpha_i * temp_r;
#else
            y_ptr[0] += alpha_r * temp_r + alpha_i * temp_i;
            y_ptr[1] -= alpha_r * temp_i - alpha_i * temp_r;
#endif

            a_ptr += lda;
            y_ptr += inc_y;
            j++;
        }
        return (0);
    }

    if (m3 == 2) {

        FLOAT temp_r;
        FLOAT temp_i;
        FLOAT temp_r1;
        FLOAT temp_i1;
        FLOAT x0 = x_ptr[0];
        FLOAT x1 = x_ptr[1];
        x_ptr += inc_x;
        FLOAT x2 = x_ptr[0];
        FLOAT x3 = x_ptr[1];

        while (j < (n & -2)) {
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
            temp_r = a_ptr[0] * x0 - a_ptr[1] * x1;
            temp_i = a_ptr[0] * x1 + a_ptr[1] * x0;
            temp_r += a_ptr[2] * x2 - a_ptr[3] * x3;
            temp_i += a_ptr[2] * x3 + a_ptr[3] * x2;
            a_ptr += lda;
            temp_r1 = a_ptr[0] * x0 - a_ptr[1] * x1;
            temp_i1 = a_ptr[0] * x1 + a_ptr[1] * x0;
            temp_r1 += a_ptr[2] * x2 - a_ptr[3] * x3;
            temp_i1 += a_ptr[2] * x3 + a_ptr[3] * x2;
#else

            temp_r = a_ptr[0] * x0 + a_ptr[1] * x1;
            temp_i = a_ptr[0] * x1 - a_ptr[1] * x0;
            temp_r += a_ptr[2] * x2 + a_ptr[3] * x3;
            temp_i += a_ptr[2] * x3 - a_ptr[3] * x2;
            a_ptr += lda;
            temp_r1 = a_ptr[0] * x0 + a_ptr[1] * x1;
            temp_i1 = a_ptr[0] * x1 - a_ptr[1] * x0;
            temp_r1 += a_ptr[2] * x2 + a_ptr[3] * x3;
            temp_i1 += a_ptr[2] * x3 - a_ptr[3] * x2;
#endif

#if !defined(XCONJ) 
            y_ptr[0] += alpha_r * temp_r - alpha_i * temp_i;
            y_ptr[1] += alpha_r * temp_i + alpha_i * temp_r;
            y_ptr += inc_y;
            y_ptr[0] += alpha_r * temp_r1 - alpha_i * temp_i1;
            y_ptr[1] += alpha_r * temp_i1 + alpha_i * temp_r1;
#else
            y_ptr[0] += alpha_r * temp_r + alpha_i * temp_i;
            y_ptr[1] -= alpha_r * temp_i - alpha_i * temp_r;
            y_ptr += inc_y;
            y_ptr[0] += alpha_r * temp_r1 + alpha_i * temp_i1;
            y_ptr[1] -= alpha_r * temp_i1 - alpha_i * temp_r1;
#endif

            a_ptr += lda;
            y_ptr += inc_y;
            j += 2;
        }

        while (j < n) {
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
            temp_r = a_ptr[0] * x0 - a_ptr[1] * x1;
            temp_i = a_ptr[0] * x1 + a_ptr[1] * x0;
            temp_r += a_ptr[2] * x2 - a_ptr[3] * x3;
            temp_i += a_ptr[2] * x3 + a_ptr[3] * x2;
#else

            temp_r = a_ptr[0] * x0 + a_ptr[1] * x1;
            temp_i = a_ptr[0] * x1 - a_ptr[1] * x0;
            temp_r += a_ptr[2] * x2 + a_ptr[3] * x3;
            temp_i += a_ptr[2] * x3 - a_ptr[3] * x2;
#endif

#if !defined(XCONJ) 
            y_ptr[0] += alpha_r * temp_r - alpha_i * temp_i;
            y_ptr[1] += alpha_r * temp_i + alpha_i * temp_r;
#else
            y_ptr[0] += alpha_r * temp_r + alpha_i * temp_i;
            y_ptr[1] -= alpha_r * temp_i - alpha_i * temp_r;
#endif

            a_ptr += lda;
            y_ptr += inc_y;
            j++;
        }

        return (0);
    }

    if (m3 == 1) {

        FLOAT temp_r;
        FLOAT temp_i;
        FLOAT temp_r1;
        FLOAT temp_i1;
        FLOAT x0 = x_ptr[0];
        FLOAT x1 = x_ptr[1];

        while (j < (n & -2)) {
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
            temp_r = a_ptr[0] * x0 - a_ptr[1] * x1;
            temp_i = a_ptr[0] * x1 + a_ptr[1] * x0;
            a_ptr += lda;
            temp_r1 = a_ptr[0] * x0 - a_ptr[1] * x1;
            temp_i1 = a_ptr[0] * x1 + a_ptr[1] * x0;
#else

            temp_r = a_ptr[0] * x0 + a_ptr[1] * x1;
            temp_i = a_ptr[0] * x1 - a_ptr[1] * x0;
            a_ptr += lda;
            temp_r1 = a_ptr[0] * x0 + a_ptr[1] * x1;
            temp_i1 = a_ptr[0] * x1 - a_ptr[1] * x0;
#endif

#if !defined(XCONJ) 
            y_ptr[0] += alpha_r * temp_r - alpha_i * temp_i;
            y_ptr[1] += alpha_r * temp_i + alpha_i * temp_r;
            y_ptr += inc_y;
            y_ptr[0] += alpha_r * temp_r1 - alpha_i * temp_i1;
            y_ptr[1] += alpha_r * temp_i1 + alpha_i * temp_r1;
#else
            y_ptr[0] += alpha_r * temp_r + alpha_i * temp_i;
            y_ptr[1] -= alpha_r * temp_i - alpha_i * temp_r;
            y_ptr += inc_y;
            y_ptr[0] += alpha_r * temp_r1 + alpha_i * temp_i1;
            y_ptr[1] -= alpha_r * temp_i1 - alpha_i * temp_r1;
#endif

            a_ptr += lda;
            y_ptr += inc_y;
            j += 2;
        }

        while (j < n) {
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
            temp_r = a_ptr[0] * x0 - a_ptr[1] * x1;
            temp_i = a_ptr[0] * x1 + a_ptr[1] * x0;
#else

            temp_r = a_ptr[0] * x0 + a_ptr[1] * x1;
            temp_i = a_ptr[0] * x1 - a_ptr[1] * x0;
#endif

#if !defined(XCONJ) 
            y_ptr[0] += alpha_r * temp_r - alpha_i * temp_i;
            y_ptr[1] += alpha_r * temp_i + alpha_i * temp_r;
#else
            y_ptr[0] += alpha_r * temp_r + alpha_i * temp_i;
            y_ptr[1] -= alpha_r * temp_i - alpha_i * temp_r;
#endif

            a_ptr += lda;
            y_ptr += inc_y;
            j++;
        }
        return (0);
    }

    return (0);

}
#endif
