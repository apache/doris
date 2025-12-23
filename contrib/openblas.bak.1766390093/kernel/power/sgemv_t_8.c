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
It could be used and tested in future or could be used as base for switching to inline assembly
*/

#include "common.h"
#include <stdio.h>
#define NBMAX 4096

#include <altivec.h> 
 
static void sgemv_kernel_8x8(BLASLONG n, BLASLONG lda, FLOAT *ap, FLOAT *x, FLOAT *y, FLOAT alpha) {
    BLASLONG i;  
    FLOAT *a0, *a1, *a2, *a3, *a4, *a5, *a6, *a7;
    __vector float *va0, *va1, *va2, *va3, *va4, *va5, *va6, *va7, *v_x;
    register __vector float temp0 = {0,0,0,0};
    register __vector float temp1 = {0,0,0,0};
    register __vector float temp2 = {0,0,0,0};
    register __vector float temp3 = {0,0,0,0};
    register __vector float temp4 = {0,0,0,0};
    register __vector float temp5 = {0,0,0,0};
    register __vector float temp6 = {0,0,0,0};
    register __vector float temp7 = {0,0,0,0};

    a0 = ap;
    a1 = ap + lda;
    a2 = a1 + lda;
    a3 = a2 + lda;
    a4 = a3 + lda;
    a5 = a4 + lda;
    a6 = a5 + lda;
    a7 = a6 + lda;
    va0 = (__vector float*) a0;
    va1 = (__vector float*) a1;
    va2 = (__vector float*) a2;
    va3 = (__vector float*) a3;
    va4 = (__vector float*) a4;
    va5 = (__vector float*) a5;
    va6 = (__vector float*) a6;
    va7 = (__vector float*) a7;
    v_x = (__vector float*) x;
 
   
        for (i = 0; i < n/4; i +=2) {
            register __vector float vx1=v_x[i] ; 
            register __vector float vx2=v_x[i+1] ; 
            register __vector float va0_1=va0[i] ; 
            register __vector float va0_2=va0[i+1] ; 
            register __vector float va1_1=va1[i] ; 
            register __vector float va1_2=va1[i+1] ; 
            register __vector float va2_1=va2[i] ; 
            register __vector float va2_2=va2[i+1] ; 
            register __vector float va3_1=va3[i] ; 
            register __vector float va3_2=va3[i+1] ; 
            register __vector float va4_1=va4[i] ; 
            register __vector float va4_2=va4[i+1] ;
            register __vector float va5_1=va5[i] ; 
            register __vector float va5_2=va5[i+1] ; 
            register __vector float va6_1=va6[i] ; 
            register __vector float va6_2=va6[i+1] ; 
            register __vector float va7_1=va7[i] ; 
            register __vector float va7_2=va7[i+1] ;                       
            temp0 += vx1* va0_1 + vx2 * va0_2;
            temp1 += vx1* va1_1 + vx2 * va1_2;
            temp2 += vx1* va2_1 + vx2 * va2_2;
            temp3 += vx1* va3_1 + vx2 * va3_2;
            temp4 += vx1* va4_1 + vx2 * va4_2;
            temp5 += vx1* va5_1 + vx2 * va5_2;
            temp6 += vx1* va6_1 + vx2 * va6_2;
            temp7 += vx1* va7_1 + vx2 * va7_2;  
        }
    
  #if defined(POWER8)
    y[0] += alpha * (temp0[0] + temp0[1]+temp0[2] + temp0[3]);
    y[1] += alpha * (temp1[0] + temp1[1]+temp1[2] + temp1[3]);
    y[2] += alpha * (temp2[0] + temp2[1]+temp2[2] + temp2[3]);
    y[3] += alpha * (temp3[0] + temp3[1]+temp3[2] + temp3[3]);

    y[4] += alpha * (temp4[0] + temp4[1]+temp4[2] + temp4[3]);
    y[5] += alpha * (temp5[0] + temp5[1]+temp5[2] + temp5[3]);
    y[6] += alpha * (temp6[0] + temp6[1]+temp6[2] + temp6[3]);
    y[7] += alpha * (temp7[0] + temp7[1]+temp7[2] + temp7[3]);
 #else
    register __vector float t0, t1, t2, t3;
    register __vector float a = { alpha, alpha, alpha, alpha };
     __vector float *v_y = (__vector float*) y;

    t0 = vec_mergeh(temp0, temp2);
    t1 = vec_mergel(temp0, temp2);
    t2 = vec_mergeh(temp1, temp3);
    t3 = vec_mergel(temp1, temp3);
    temp0 = vec_mergeh(t0, t2);
    temp1 = vec_mergel(t0, t2);
    temp2 = vec_mergeh(t1, t3);
    temp3 = vec_mergel(t1, t3);
    temp0 += temp1 + temp2 + temp3;

    t0 = vec_mergeh(temp4, temp6);
    t1 = vec_mergel(temp4, temp6);
    t2 = vec_mergeh(temp5, temp7);
    t3 = vec_mergel(temp5, temp7);
    temp4 = vec_mergeh(t0, t2);
    temp5 = vec_mergel(t0, t2);
    temp6 = vec_mergeh(t1, t3);
    temp7 = vec_mergel(t1, t3);
    temp4 += temp5 + temp6 + temp7;

    v_y[0] += a * temp0;
    v_y[1] += a * temp4;
#endif
}
 

static void sgemv_kernel_8x4(BLASLONG n, BLASLONG lda, FLOAT *ap, FLOAT *x, FLOAT *y, FLOAT alpha) {
    BLASLONG i = 0;
    FLOAT *a0, *a1, *a2, *a3;
    a0 = ap;
    a1 = ap + lda;
    a2 = a1 + lda;
    a3 = a2 + lda;
    __vector float* va0 = (__vector float*) a0;
    __vector float* va1 = (__vector float*) a1;
    __vector float* va2 = (__vector float*) a2;
    __vector float* va3 = (__vector float*) a3;
    __vector float* v_x = (__vector float*) x;
    register __vector float temp0 = {0,0,0,0};
    register __vector float temp1 = {0,0,0,0};
    register __vector float temp2 = {0,0,0,0};
    register __vector float temp3 = {0,0,0,0}; 

    for (i = 0; i < n / 4; i +=2) {
        temp0 += v_x[i] * va0[i] + v_x[i+1] * va0[i+1];
        temp1 += v_x[i] * va1[i] + v_x[i+1] * va1[i+1];
        temp2 += v_x[i] * va2[i] + v_x[i+1] * va2[i+1];
        temp3 += v_x[i] * va3[i] + v_x[i+1] * va3[i+1]; 
    }

 #if defined(POWER8)
    y[0] += alpha * (temp0[0] + temp0[1]+temp0[2] + temp0[3]);
    y[1] += alpha * (temp1[0] + temp1[1]+temp1[2] + temp1[3]);
    y[2] += alpha * (temp2[0] + temp2[1]+temp2[2] + temp2[3]);
    y[3] += alpha * (temp3[0] + temp3[1]+temp3[2] + temp3[3]);
 #else
    register __vector float t0, t1, t2, t3;
    register __vector float a = { alpha, alpha, alpha, alpha };
     __vector float *v_y = (__vector float*) y;

    t0 = vec_mergeh(temp0, temp2);
    t1 = vec_mergel(temp0, temp2);
    t2 = vec_mergeh(temp1, temp3);
    t3 = vec_mergel(temp1, temp3);
    temp0 = vec_mergeh(t0, t2);
    temp1 = vec_mergel(t0, t2);
    temp2 = vec_mergeh(t1, t3);
    temp3 = vec_mergel(t1, t3);
    temp0 += temp1 + temp2 + temp3;

    v_y[0] += a * temp0;
#endif
}
 

static void sgemv_kernel_8x2(BLASLONG n, BLASLONG lda, FLOAT *ap, FLOAT *x, FLOAT *y, FLOAT alpha, BLASLONG inc_y) {

    BLASLONG i;
    FLOAT *a0, *a1;
    a0 = ap;
    a1 = ap + lda;
    __vector float* va0 = (__vector float*) a0;
    __vector float* va1 = (__vector float*) a1;
    __vector float* v_x = (__vector float*) x;
    __vector float temp0 = {0,0,0,0};
    __vector float temp1 = {0,0,0,0};
    for (i = 0; i < n / 4; i +=2) {
        temp0 += v_x[i] * va0[i] + v_x[i+1] * va0[i+1];
        temp1 += v_x[i] * va1[i] + v_x[i+1] * va1[i+1]; 
    }



    y[0] += alpha * (temp0[0] + temp0[1]+temp0[2] + temp0[3]);
    y[inc_y] += alpha * (temp1[0] + temp1[1]+temp1[2] + temp1[3]); 
}

static void sgemv_kernel_8x1(BLASLONG n, FLOAT *ap, FLOAT *x, FLOAT *y, FLOAT alpha) {

    BLASLONG i;
    FLOAT *a0;
    a0 = ap;
    __vector float* va0 = (__vector float*) a0;
    __vector float* v_x = (__vector float*) x;
    __vector float temp0 = {0,0,0,0};
    for (i = 0; i < n / 4; i +=2) {
        temp0 += v_x[i] * va0[i] + v_x[i+1] * va0[i+1]; 
    }
    y[0] += alpha * (temp0[0] + temp0[1]+temp0[2] + temp0[3]);

}
 

static void copy_x(BLASLONG n, FLOAT *src, FLOAT *dest, BLASLONG inc_src) {
    BLASLONG i;
    for (i = 0; i < n; i++) {
        *dest++ = *src;
        src += inc_src;
    }
}

int CNAME(BLASLONG m, BLASLONG n, BLASLONG dummy1, FLOAT alpha, FLOAT *a, BLASLONG lda, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *buffer) {
    BLASLONG i;
    BLASLONG j;
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

    xbuffer = buffer;

    n1 = n >> 3;
    n2 = n & 7;

    m3 = m & 7;
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

        if (inc_x != 1)
            copy_x(NB, x_ptr, xbuffer, inc_x);
        else
            xbuffer = x_ptr;

        BLASLONG lda8 = lda << 3;

  
        if (inc_y == 1) {

            for (i = 0; i < n1; i++) {
                 
                sgemv_kernel_8x8(NB, lda, a_ptr, xbuffer, y_ptr, alpha);
 
                y_ptr += 8;
                a_ptr += lda8;
        
            }

        } else {
                   
            for (i = 0; i < n1; i++) {
                ybuffer[0] = 0;
                ybuffer[1] = 0;
                ybuffer[2] = 0;
                ybuffer[3] = 0;
                ybuffer[4] = 0;
                ybuffer[5] = 0;
                ybuffer[6] = 0;
                ybuffer[7] = 0;
                sgemv_kernel_8x8(NB, lda, a_ptr, xbuffer, ybuffer, alpha);

 

                *y_ptr += ybuffer[0];
                y_ptr += inc_y;
                *y_ptr += ybuffer[1];
                y_ptr += inc_y;
                *y_ptr += ybuffer[2];
                y_ptr += inc_y;
                *y_ptr += ybuffer[3];
                y_ptr += inc_y;

                *y_ptr += ybuffer[4];
                y_ptr += inc_y;
                *y_ptr += ybuffer[5];
                y_ptr += inc_y;
                *y_ptr += ybuffer[6];
                y_ptr += inc_y;
                *y_ptr += ybuffer[7];
                y_ptr += inc_y;

                a_ptr += lda8;
            }

        }


        if (n2 & 4) {
            ybuffer[0] = 0;
            ybuffer[1] = 0;
            ybuffer[2] = 0;
            ybuffer[3] = 0;
            sgemv_kernel_8x4(NB, lda, a_ptr, xbuffer, ybuffer, alpha);

            a_ptr += lda<<2;

            *y_ptr += ybuffer[0];
            y_ptr += inc_y;
            *y_ptr += ybuffer[1];
            y_ptr += inc_y;
            *y_ptr += ybuffer[2];
            y_ptr += inc_y;
            *y_ptr += ybuffer[3];
            y_ptr += inc_y;
        }

        if (n2 & 2) {
            sgemv_kernel_8x2(NB, lda, a_ptr, xbuffer, y_ptr, alpha, inc_y);
            a_ptr += lda << 1;
            y_ptr += 2 * inc_y;

        }

        if (n2 & 1) {
            sgemv_kernel_8x1(NB, a_ptr, xbuffer, y_ptr, alpha);
            a_ptr += lda;
            y_ptr += inc_y;

        }

        a += NB;
        x += NB * inc_x;


    }

    if (m3 == 0) return (0);

    x_ptr = x;
    a_ptr = a;
    if (m3 & 4) {
        FLOAT xtemp0 = *x_ptr * alpha;
        x_ptr += inc_x;
        FLOAT xtemp1 = *x_ptr * alpha;
        x_ptr += inc_x;
        FLOAT xtemp2 = *x_ptr * alpha;
        x_ptr += inc_x;
        FLOAT xtemp3 = *x_ptr * alpha;
        x_ptr += inc_x;
        FLOAT *aj = a_ptr;
        y_ptr = y;
        if (lda == 4 && inc_y == 1) {

            for (j = 0; j < (n & -4); j += 4) {
                y_ptr[j] += aj[0] * xtemp0 + aj[1] * xtemp1  +  aj[2] * xtemp2 + aj[3] * xtemp3;
                y_ptr[j + 1] += aj[4] * xtemp0 + aj[5] * xtemp1  +  aj[6] * xtemp2 + aj[7] * xtemp3;
                y_ptr[j + 2] += aj[8] * xtemp0 + aj[9] * xtemp1  +  aj[10] * xtemp2 + aj[11] * xtemp3;
                y_ptr[j + 3] += aj[12] * xtemp0 + aj[13] * xtemp1  +  aj[14] * xtemp2 + aj[15] * xtemp3;
                aj += 16;

            }

            for (; j < n; j++) {
                y_ptr[j] += aj[0] * xtemp0 + aj[1] * xtemp1 +  aj[2] * xtemp2 + aj[3] * xtemp3;
                aj += 4;
            }

        } else if (inc_y == 1) {
        
                BLASLONG register lda2 = lda << 1;
                BLASLONG register lda4 = lda << 2;
                BLASLONG register lda3 = lda2 + lda;

                for (j = 0; j < (n & -4); j += 4) {

                    y_ptr[j] += *aj * xtemp0 + *(aj + 1) * xtemp1 + *(aj + 2) * xtemp2 + *(aj + 3) * xtemp3;
                    y_ptr[j + 1] += *(aj + lda) * xtemp0 + *(aj + lda + 1) * xtemp1 + *(aj + lda + 2) * xtemp2 + *(aj + lda +3) * xtemp3;
                    y_ptr[j + 2] += *(aj + lda2) * xtemp0 + *(aj + lda2 + 1) * xtemp1 + *(aj + lda2 + 2) * xtemp2  + *(aj +  lda2 +3) * xtemp3;
                    y_ptr[j + 3] += *(aj + lda3) * xtemp0 + *(aj + lda3 + 1) * xtemp1 + *(aj + lda3 + 2) * xtemp2  + *(aj +  lda3+3) * xtemp3;
                    aj += lda4;
                }

                for (; j < n; j++) {

                    y_ptr[j] += *aj * xtemp0 + *(aj + 1) * xtemp1 + *(aj + 2) * xtemp2+*(aj + 3) * xtemp3;
                    aj += lda;
                }

        } else {

                for (j = 0; j < n; j++) {
                    *y_ptr += *aj * xtemp0 + *(aj + 1) * xtemp1 + *(aj + 2) * xtemp2+ *(aj + 3) * xtemp3;
                    y_ptr += inc_y;
                    aj += lda;
                }

            } 
            if (m3==4) return (0);
            a_ptr += 4; 
    }

    if (m3 & 2 ) {
  
        FLOAT xtemp0 = *x_ptr * alpha;
        x_ptr += inc_x;
        FLOAT xtemp1 = *x_ptr * alpha;
        x_ptr += inc_x;
        FLOAT *aj = a_ptr;
        y_ptr = y;

        if (lda == 2 && inc_y == 1) {

            for (j = 0; j < (n & -4); j += 4) {
                y_ptr[j] += aj[0] * xtemp0 + aj[1] * xtemp1;
                y_ptr[j + 1] += aj[2] * xtemp0 + aj[3] * xtemp1;
                y_ptr[j + 2] += aj[4] * xtemp0 + aj[5] * xtemp1;
                y_ptr[j + 3] += aj[6] * xtemp0 + aj[7] * xtemp1;
                aj += 8;

            }

            for (; j < n; j++) {
                y_ptr[j] += aj[0] * xtemp0 + aj[1] * xtemp1;
                aj += 2;
            }

        } else {
            if (inc_y == 1) {

                BLASLONG register lda2 = lda << 1;
                BLASLONG register lda4 = lda << 2;
                BLASLONG register lda3 = lda2 + lda;

                for (j = 0; j < (n & -4); j += 4) {

                    y_ptr[j] += *aj * xtemp0 + *(aj + 1) * xtemp1;
                    y_ptr[j + 1] += *(aj + lda) * xtemp0 + *(aj + lda + 1) * xtemp1;
                    y_ptr[j + 2] += *(aj + lda2) * xtemp0 + *(aj + lda2 + 1) * xtemp1;
                    y_ptr[j + 3] += *(aj + lda3) * xtemp0 + *(aj + lda3 + 1) * xtemp1;
                    aj += lda4;
                }

                for (; j < n; j++) {

                    y_ptr[j] += *aj * xtemp0 + *(aj + 1) * xtemp1;
                    aj += lda;
                }

            } else {
                for (j = 0; j < n; j++) {
                    *y_ptr += *aj * xtemp0 + *(aj + 1) * xtemp1;
                    y_ptr += inc_y;
                    aj += lda;
                }
            }

        } 
        if (m3==2) return (0);
        a_ptr += 2; 
    }
    if (m3 & 1) {
          
    FLOAT xtemp = *x_ptr * alpha;
            x_ptr += inc_x;
    FLOAT *aj = a_ptr;
    y_ptr = y;
    if (lda == 1 && inc_y == 1) {
        for (j = 0; j < (n & -4); j += 4) {
            y_ptr[j] += aj[j] * xtemp;
            y_ptr[j + 1] += aj[j + 1] * xtemp;
            y_ptr[j + 2] += aj[j + 2] * xtemp;
            y_ptr[j + 3] += aj[j + 3] * xtemp;
        }
        for (; j < n; j++) {
            y_ptr[j] += aj[j] * xtemp;
        }


    } else {
        if (inc_y == 1) {

            BLASLONG register lda2 = lda << 1;
            BLASLONG register lda4 = lda << 2;
            BLASLONG register lda3 = lda2 + lda;
            for (j = 0; j < (n & -4); j += 4) {
                y_ptr[j] += *aj * xtemp;
                y_ptr[j + 1] += *(aj + lda) * xtemp;
                y_ptr[j + 2] += *(aj + lda2) * xtemp;
                y_ptr[j + 3] += *(aj + lda3) * xtemp;
                aj += lda4;
            }

            for (; j < n; j++) {
                y_ptr[j] += *aj * xtemp;
                aj += lda;
            }

        } else {
            for (j = 0; j < n; j++) {
                *y_ptr += *aj * xtemp;
                y_ptr += inc_y;
                aj += lda;
            }

        }
    
    }
                a_ptr += 1; 
    }
    return (0);

}

