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
#include <immintrin.h>
#include "common.h"

//Here the m means n in sgemv_t:
// ----- n -----
// |
// |
// m
// |
// |
static int sgemv_kernel_t_1(BLASLONG m, float alpha, float *a, float *x, float *y)
{
    //printf("enter into t_1 kernel\n");
    //printf("m = %ld\n", m);
    __m512 matrixArray_0, matrixArray_1, matrixArray_2, matrixArray_3, matrixArray_4, matrixArray_5, matrixArray_6, matrixArray_7;
    float alphaX = alpha * (*x);
    __m512  ALPHAXVECTOR = _mm512_set1_ps(alphaX);
    
    BLASLONG tag_m_128x = m & (~127);
    BLASLONG tag_m_64x = m & (~63);
    BLASLONG tag_m_32x = m & (~31);
    BLASLONG tag_m_16x = m & (~15);

    for (BLASLONG idx_m = 0; idx_m < tag_m_128x; idx_m+=128) {
            matrixArray_0 = _mm512_loadu_ps(&a[idx_m + 0]);
            matrixArray_1 = _mm512_loadu_ps(&a[idx_m + 16]);
            matrixArray_2 = _mm512_loadu_ps(&a[idx_m + 32]);
            matrixArray_3 = _mm512_loadu_ps(&a[idx_m + 48]);
            matrixArray_4 = _mm512_loadu_ps(&a[idx_m + 64]);
            matrixArray_5 = _mm512_loadu_ps(&a[idx_m + 80]);
            matrixArray_6 = _mm512_loadu_ps(&a[idx_m + 96]);
            matrixArray_7 = _mm512_loadu_ps(&a[idx_m + 112]);
            
        _mm512_storeu_ps(&y[idx_m + 0], _mm512_fmadd_ps(matrixArray_0, ALPHAXVECTOR, _mm512_loadu_ps(&y[idx_m + 0])));
        _mm512_storeu_ps(&y[idx_m + 16], _mm512_fmadd_ps(matrixArray_1, ALPHAXVECTOR, _mm512_loadu_ps(&y[idx_m + 16])));
        _mm512_storeu_ps(&y[idx_m + 32], _mm512_fmadd_ps(matrixArray_2, ALPHAXVECTOR, _mm512_loadu_ps(&y[idx_m + 32])));
        _mm512_storeu_ps(&y[idx_m + 48], _mm512_fmadd_ps(matrixArray_3, ALPHAXVECTOR, _mm512_loadu_ps(&y[idx_m + 48])));
        _mm512_storeu_ps(&y[idx_m + 64], _mm512_fmadd_ps(matrixArray_4, ALPHAXVECTOR, _mm512_loadu_ps(&y[idx_m + 64])));
        _mm512_storeu_ps(&y[idx_m + 80], _mm512_fmadd_ps(matrixArray_5, ALPHAXVECTOR, _mm512_loadu_ps(&y[idx_m + 80])));
        _mm512_storeu_ps(&y[idx_m + 96], _mm512_fmadd_ps(matrixArray_6, ALPHAXVECTOR, _mm512_loadu_ps(&y[idx_m + 96])));
        _mm512_storeu_ps(&y[idx_m + 112], _mm512_fmadd_ps(matrixArray_7, ALPHAXVECTOR, _mm512_loadu_ps(&y[idx_m + 112])));

    }

    if (tag_m_128x != m) {
        for (BLASLONG idx_m = tag_m_128x; idx_m < tag_m_64x; idx_m+=64) {
            matrixArray_0 = _mm512_loadu_ps(&a[idx_m + 0]);
            matrixArray_1 = _mm512_loadu_ps(&a[idx_m + 16]);
            matrixArray_2 = _mm512_loadu_ps(&a[idx_m + 32]);
            matrixArray_3 = _mm512_loadu_ps(&a[idx_m + 48]);
            
            _mm512_storeu_ps(&y[idx_m + 0], _mm512_fmadd_ps(matrixArray_0, ALPHAXVECTOR, _mm512_loadu_ps(&y[idx_m + 0])));
            _mm512_storeu_ps(&y[idx_m + 16], _mm512_fmadd_ps(matrixArray_1, ALPHAXVECTOR, _mm512_loadu_ps(&y[idx_m + 16])));
            _mm512_storeu_ps(&y[idx_m + 32], _mm512_fmadd_ps(matrixArray_2, ALPHAXVECTOR, _mm512_loadu_ps(&y[idx_m + 32])));
            _mm512_storeu_ps(&y[idx_m + 48], _mm512_fmadd_ps(matrixArray_3, ALPHAXVECTOR, _mm512_loadu_ps(&y[idx_m + 48])));
  
        }

        if (tag_m_64x != m) {
            for (BLASLONG idx_m = tag_m_64x; idx_m < tag_m_32x; idx_m+=32) {
                matrixArray_0 = _mm512_loadu_ps(&a[idx_m + 0]);
                matrixArray_1 = _mm512_loadu_ps(&a[idx_m + 16]);

                _mm512_storeu_ps(&y[idx_m + 0], _mm512_fmadd_ps(matrixArray_0, ALPHAXVECTOR, _mm512_loadu_ps(&y[idx_m + 0])));
                _mm512_storeu_ps(&y[idx_m + 16], _mm512_fmadd_ps(matrixArray_1, ALPHAXVECTOR, _mm512_loadu_ps(&y[idx_m + 16])));
 
            }

            if (tag_m_32x != m) {
                for (BLASLONG idx_m = tag_m_32x; idx_m < tag_m_16x; idx_m+=16) {
                    matrixArray_0 = _mm512_loadu_ps(&a[idx_m + 0]);
            
                    _mm512_storeu_ps(&y[idx_m + 0], _mm512_fmadd_ps(matrixArray_0, ALPHAXVECTOR, _mm512_loadu_ps(&y[idx_m + 0])));
                }
            
                if (tag_m_16x != m) {
                    unsigned short tail_mask_value = (((unsigned int)0xffff) >> (16-(m&15)));
                    __mmask16 tail_mask = *((__mmask16*) &tail_mask_value);
                    matrixArray_0 = _mm512_maskz_loadu_ps(tail_mask, &a[tag_m_16x]);

                    _mm512_mask_storeu_ps(&y[tag_m_16x], tail_mask, _mm512_fmadd_ps(matrixArray_0, ALPHAXVECTOR, _mm512_maskz_loadu_ps(tail_mask, &y[tag_m_16x])));

                }

 
            }
        }
    }

    return 0;
}

static int sgemv_kernel_t_2(BLASLONG m, float alpha, float *a, float *x, float *y)
{
    __m512 m0, m1, m2, m3, col0_1, col0_2, col1_1, col1_2, x1Array, x2Array;
    float x1a = x[0] * alpha;
    float x2a = x[1] * alpha;
    x1Array = _mm512_set1_ps(x1a);
    x2Array = _mm512_set1_ps(x2a);
    BLASLONG tag_m_32x = m & (~31);
    BLASLONG tag_m_16x = m & (~15);
    BLASLONG tag_m_8x = m & (~7);
    __m512i M512_EPI32_1 = _mm512_set1_epi32(1);
    __m512i idx_base_0 = _mm512_set_epi32(30, 28, 26, 24, 22, 20, 18, 16, 14, 12, 10, 8, 6, 4, 2, 0);
    __m512i idx_base_1 = _mm512_add_epi32(idx_base_0, M512_EPI32_1);

    for (BLASLONG idx_m = 0; idx_m < tag_m_32x; idx_m+=32) {
        m0 = _mm512_loadu_ps(&a[idx_m*2]);
        m1 = _mm512_loadu_ps(&a[idx_m*2 + 16]);
        m2 = _mm512_loadu_ps(&a[idx_m*2 + 32]);
        m3 = _mm512_loadu_ps(&a[idx_m*2 + 48]);
        col0_1 = _mm512_permutex2var_ps(m0, idx_base_0, m1);
        col0_2 = _mm512_permutex2var_ps(m0, idx_base_1, m1);
        col1_1 = _mm512_permutex2var_ps(m2, idx_base_0, m3);
        col1_2 = _mm512_permutex2var_ps(m2, idx_base_1, m3);

        _mm512_storeu_ps(&y[idx_m], _mm512_add_ps(_mm512_fmadd_ps(x2Array, col0_2, _mm512_mul_ps(col0_1, x1Array)), _mm512_loadu_ps(&y[idx_m])));
        _mm512_storeu_ps(&y[idx_m + 16], _mm512_add_ps(_mm512_fmadd_ps(x2Array, col1_2, _mm512_mul_ps(col1_1, x1Array)), _mm512_loadu_ps(&y[idx_m + 16])));
    }
    if (tag_m_32x != m) {
        for (BLASLONG idx_m = tag_m_32x; idx_m < tag_m_16x; idx_m+=16) {
            m0 = _mm512_loadu_ps(&a[idx_m*2]);
            m1 = _mm512_loadu_ps(&a[idx_m*2 + 16]);
            col1_1 = _mm512_permutex2var_ps(m0, idx_base_0, m1);
            col1_2 = _mm512_permutex2var_ps(m0, idx_base_1, m1);
            _mm512_storeu_ps(&y[idx_m], _mm512_add_ps(_mm512_fmadd_ps(x2Array, col1_2, _mm512_mul_ps(col1_1, x1Array)), _mm512_loadu_ps(&y[idx_m])));
        }
        if (tag_m_16x != m) {
            __m512  ALPHAVECTOR = _mm512_set1_ps(alpha);
            unsigned char load_mask_value = (((unsigned char)0xff) >> 6);
            __mmask8 load_mask = *((__mmask8*) &load_mask_value);
            x1Array = _mm512_broadcast_f32x2(_mm_maskz_loadu_ps(load_mask, x));
            for (BLASLONG idx_m = tag_m_16x; idx_m < tag_m_8x; idx_m+=8) {
                m0 = _mm512_loadu_ps(&a[idx_m*2]);
                m1 = _mm512_mul_ps(_mm512_mul_ps(m0, x1Array), ALPHAVECTOR);
                m2 = _mm512_permutexvar_ps(_mm512_set_epi32(15, 13, 11, 9, 7, 5, 3, 1, 14, 12, 10, 8, 6, 4, 2, 0), m1);
                __m256 ret = _mm256_add_ps(_mm512_extractf32x8_ps(m2, 1), _mm512_extractf32x8_ps(m2, 0));
                _mm256_storeu_ps(&y[idx_m], _mm256_add_ps(ret, _mm256_loadu_ps(&y[idx_m])));
                 
            }

            if (tag_m_8x != m) {
                unsigned short tail_mask_value = (((unsigned int)0xffff) >> (16-(((m-tag_m_8x)*2)&15)));
                __mmask16 a_mask = *((__mmask16*) &tail_mask_value);
                unsigned char y_mask_value = (((unsigned char)0xff) >> (8-(m-tag_m_8x)));
                __mmask8 y_mask = *((__mmask8*) &y_mask_value);

                m0 = _mm512_maskz_loadu_ps(a_mask, &a[tag_m_8x*2]);
                m1 = _mm512_mul_ps(_mm512_mul_ps(m0, x1Array), ALPHAVECTOR);
                m2 = _mm512_permutexvar_ps(_mm512_set_epi32(15, 13, 11, 9, 7, 5, 3, 1, 14, 12, 10, 8, 6, 4, 2, 0), m1);
                __m256 ret = _mm256_add_ps(_mm512_extractf32x8_ps(m2, 1), _mm512_extractf32x8_ps(m2, 0));
                _mm256_mask_storeu_ps(&y[tag_m_8x], y_mask, _mm256_add_ps(ret, _mm256_maskz_loadu_ps(y_mask, &y[tag_m_8x])));
            }                  
        }        
    }
    return 0;
}

static int sgemv_kernel_t_3(BLASLONG m, float alpha, float *a, float *x, float *y)
{
    __m512 m0, m1, m2, c1, c2, c3, tmp, x1Array, x2Array, x3Array;
    float x1a = x[0] * alpha;
    float x2a = x[1] * alpha;
    float x3a = x[2] * alpha;
    x1Array = _mm512_set1_ps(x1a);
    x2Array = _mm512_set1_ps(x2a);
    x3Array = _mm512_set1_ps(x3a);
    BLASLONG tag_m_16x = m & (~15);
    BLASLONG tag_m_8x = m & (~7);
    BLASLONG tag_m_4x = m & (~3);
    BLASLONG tag_m_2x = m & (~1);

    __m512i M512_EPI32_1 = _mm512_set1_epi32(1);
    __m512i M512_EPI32_s1 = _mm512_set1_epi32(-1);
    __m512i idx_c1_1 = _mm512_set_epi32(0, 0, 0, 0, 0, 30, 27, 24, 21, 18, 15, 12, 9, 6, 3, 0);
    __m512i idx_c2_1 = _mm512_add_epi32(idx_c1_1, M512_EPI32_1);
    __m512i idx_c3_1 = _mm512_add_epi32(idx_c2_1, M512_EPI32_1);

    __m512i idx_c3_2 = _mm512_set_epi32(31, 28, 25, 22, 19, 16, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0);
    __m512i idx_c2_2 = _mm512_add_epi32(idx_c3_2, M512_EPI32_s1);
    __m512i idx_c1_2 = _mm512_add_epi32(idx_c2_2, M512_EPI32_s1);

    __mmask16 step_1 = 0x07ff;
    __mmask16 step_2 = 0xf800;
    __mmask16 c31 = 0x03ff;

    for (BLASLONG idx_m = 0; idx_m < tag_m_16x; idx_m+=16) {
        m0 = _mm512_loadu_ps(&a[idx_m*3]);
        m1 = _mm512_loadu_ps(&a[idx_m*3 + 16]);
        m2 = _mm512_loadu_ps(&a[idx_m*3 + 32]);

        tmp = _mm512_mask_permutex2var_ps(m0, step_1, idx_c1_1, m1);
        c1 = _mm512_mask_permutex2var_ps(tmp, step_2, idx_c1_2, m2);
        tmp = _mm512_mask_permutex2var_ps(m0, step_1, idx_c2_1, m1);
        c2 = _mm512_mask_permutex2var_ps(tmp, step_2, idx_c2_2, m2);
        tmp = _mm512_mask_permutex2var_ps(m0, c31, idx_c3_1, m1);
        c3 = _mm512_permutex2var_ps(tmp, idx_c3_2, m2);

        tmp = _mm512_fmadd_ps(x2Array, c2, _mm512_mul_ps(c1, x1Array));
        _mm512_storeu_ps(&y[idx_m], _mm512_add_ps(_mm512_fmadd_ps(x3Array, c3, tmp), _mm512_loadu_ps(&y[idx_m])));
    }

    if(tag_m_16x != m) {
        __mmask8 a_mask = 0xff;
        __m256i M256_EPI32_1 = _mm256_maskz_set1_epi32(a_mask, 1);
        __m256i M256_EPI32_s1 = _mm256_maskz_set1_epi32(a_mask, -1);
        __m256i idx_c1_1 = _mm256_set_epi32(0, 0, 15, 12, 9, 6, 3, 0);
        __m256i idx_c2_1 = _mm256_add_epi32(idx_c1_1, M256_EPI32_1);
        __m256i idx_c3_1 = _mm256_add_epi32(idx_c2_1, M256_EPI32_1);

        __m256i idx_c3_2 = _mm256_set_epi32(15, 12, 9, 0, 0, 0, 0, 0);
        __m256i idx_c2_2 = _mm256_add_epi32(idx_c3_2, M256_EPI32_s1);
        __m256i idx_c1_2 = _mm256_add_epi32(idx_c2_2, M256_EPI32_s1);

        __mmask8 step_1 = 0x1f;
        __mmask8 step_2 = 0xe0;
        __mmask8 c12 = 0xc0;
        
        __m256 m256_0, m256_1, m256_2, tmp256, c256_1, c256_2, c256_3, x256_1, x256_2, x256_3;
        x256_1 = _mm256_set1_ps(x1a);
        x256_2 = _mm256_set1_ps(x2a);
        x256_3 = _mm256_set1_ps(x3a);

        for (BLASLONG idx_m = tag_m_16x; idx_m < tag_m_8x; idx_m+=8) {
            m256_0 = _mm256_loadu_ps(&a[idx_m*3]);
            m256_1 = _mm256_loadu_ps(&a[idx_m*3 + 8]);
            m256_2 = _mm256_loadu_ps(&a[idx_m*3 + 16]);

            tmp256 = _mm256_permutex2var_ps(m256_0, idx_c1_1, m256_1);
            c256_1 = _mm256_mask_permutex2var_ps(tmp256, c12, idx_c1_2, m256_2);
            tmp256 = _mm256_mask_permutex2var_ps(m256_0, step_1, idx_c2_1, m256_1);
            c256_2 = _mm256_mask_permutex2var_ps(tmp256, step_2, idx_c2_2, m256_2);
            tmp256 = _mm256_mask_permutex2var_ps(m256_0, step_1, idx_c3_1, m256_1);
            c256_3 = _mm256_mask_permutex2var_ps(tmp256, step_2, idx_c3_2, m256_2);

            tmp256 = _mm256_fmadd_ps(x256_2, c256_2, _mm256_mul_ps(c256_1, x256_1));
            _mm256_storeu_ps(&y[idx_m], _mm256_maskz_add_ps(a_mask, _mm256_fmadd_ps(x256_3, c256_3, tmp256), _mm256_loadu_ps(&y[idx_m])));
        }

        if(tag_m_8x != m){
            for (BLASLONG idx_m = tag_m_8x; idx_m < tag_m_4x; idx_m+=4){
                m0 = _mm512_maskz_loadu_ps(0x0fff, &a[tag_m_8x*3]);
                m256_0 = _mm512_extractf32x8_ps(m0, 0);
                m256_1 = _mm512_extractf32x8_ps(m0, 1);
                __m256i idx1 = _mm256_set_epi32(10, 7, 4, 1, 9, 6, 3, 0);
                __m256i M256_EPI32_2 = _mm256_maskz_set1_epi32(0x0f, 2);
                __m256i idx2 = _mm256_add_epi32(idx1, M256_EPI32_2);

                c256_1 = _mm256_mask_permutex2var_ps(m256_0, 0xff, idx1, m256_1);
                c256_2 = _mm256_mask_permutex2var_ps(m256_0, 0x0f, idx2, m256_1);

                __m128 c128_1 = _mm256_extractf32x4_ps(c256_1, 0);
                __m128 c128_2 = _mm256_extractf32x4_ps(c256_1, 1);
                __m128 c128_3 = _mm256_extractf32x4_ps(c256_2, 0);

                __m128 x128_1 = _mm_set1_ps(x1a);
                __m128 x128_2 = _mm_set1_ps(x2a);
                __m128 x128_3 = _mm_set1_ps(x3a);

                __m128 tmp128 = _mm_maskz_fmadd_ps(0x0f, c128_1, x128_1, _mm_maskz_mul_ps(0x0f, c128_2, x128_2));
                _mm_mask_storeu_ps(&y[idx_m], 0x0f, _mm_maskz_add_ps(0x0f, _mm_maskz_fmadd_ps(0x0f, c128_3, x128_3, tmp128), _mm_maskz_loadu_ps(0x0f, &y[idx_m])));
            }

            if(tag_m_4x != m) {
                for (BLASLONG idx_m = tag_m_4x; idx_m < tag_m_2x; idx_m+=2) {
                    m256_0 = _mm256_maskz_loadu_ps(0x3f, &a[idx_m*3]);
                    __m128 a128_1 = _mm256_extractf32x4_ps(m256_0, 0);
                    __m128 a128_2 = _mm256_extractf32x4_ps(m256_0, 1);
                    __m128 x128 = _mm_maskz_loadu_ps(0x07, x);

                    __m128i idx128_1= _mm_set_epi32(0, 2, 1, 0);
                    __m128i M128_EPI32_3 = _mm_maskz_set1_epi32(0x07, 3);
                    __m128i idx128_2 = _mm_add_epi32(idx128_1, M128_EPI32_3);

                    __m128 c128_1 = _mm_maskz_permutex2var_ps(0x07, a128_1, idx128_1, a128_2);
                    __m128 c128_2 = _mm_maskz_permutex2var_ps(0x07, a128_1, idx128_2, a128_2);

                    __m128 tmp128 = _mm_hadd_ps(_mm_maskz_mul_ps(0x07, c128_1, x128), _mm_maskz_mul_ps(0x07, c128_2, x128));
                    float ret[4];
                    _mm_mask_storeu_ps(ret, 0x0f, tmp128);
                    y[idx_m] += alpha *(ret[0] + ret[1]);
                    y[idx_m+1] += alpha * (ret[2] + ret[3]);
                }

                if(tag_m_2x != m) {
                    y[tag_m_2x] += alpha*(a[tag_m_2x*3]*x[0] + a[tag_m_2x*3+1]*x[1] + a[tag_m_2x*3+2]*x[2]);
                }
            }
        }
    }

    return 0;
}

static int sgemv_kernel_t_4(BLASLONG m, float alpha, float *a, float *x, float *y)
{
    BLASLONG tag_m_4x = m & (~3);
    BLASLONG tag_m_2x = m & (~1);
    __m512 m0, m1;
    __m256 m256_0, m256_1, c256_1, c256_2;
    __m128 c1, c2, c3, c4, ret;
    __m128 xarray = _mm_maskz_loadu_ps(0x0f, x);
    __m512 x512 = _mm512_broadcast_f32x4(xarray);
    __m512 alphavector = _mm512_set1_ps(alpha);
    __m512 xa512 = _mm512_mul_ps(x512, alphavector);
    __m256i idx1 = _mm256_set_epi32(13, 9, 5, 1, 12, 8, 4, 0);
    __m256i idx2 = _mm256_set_epi32(15, 11, 7, 3, 14, 10, 6, 2);


    for (BLASLONG idx_m = 0; idx_m < tag_m_4x; idx_m+=4) {
        m0 = _mm512_loadu_ps(&a[idx_m*4]);
        m1 = _mm512_mul_ps(m0, xa512);
        m256_0 = _mm512_extractf32x8_ps(m1, 0);
        m256_1 = _mm512_extractf32x8_ps(m1, 1);
        c256_1 = _mm256_mask_permutex2var_ps(m256_0, 0xff, idx1, m256_1);
        c256_2 = _mm256_mask_permutex2var_ps(m256_0, 0xff, idx2, m256_1);

        c1 = _mm256_extractf32x4_ps(c256_1, 0);
        c2 = _mm256_extractf32x4_ps(c256_1, 1);
        c3 = _mm256_extractf32x4_ps(c256_2, 0);
        c4 = _mm256_extractf32x4_ps(c256_2, 1);

        ret = _mm_maskz_add_ps(0xff, _mm_maskz_add_ps(0xff, _mm_maskz_add_ps(0xff, c1, c2), _mm_maskz_add_ps(0xff, c3, c4)), _mm_maskz_loadu_ps(0xff, &y[idx_m]));
        _mm_mask_storeu_ps(&y[idx_m], 0xff, ret);
    }
    
    if(tag_m_4x != m) {
        float result[4];
        for(BLASLONG idx_m=tag_m_4x; idx_m < tag_m_2x; idx_m+=2) {
            m256_0 = _mm256_maskz_loadu_ps(0xff, &a[idx_m*4]);
            c1 = _mm256_maskz_extractf32x4_ps(0xff, m256_0, 0);
            c2 = _mm256_maskz_extractf32x4_ps(0xff, m256_0, 1);

            c3 = _mm_maskz_mul_ps(0x0f, c1, xarray);
            c4 = _mm_maskz_mul_ps(0x0f, c2, xarray);

            ret = _mm_hadd_ps(c3, c4);            
            _mm_mask_storeu_ps(result, 0x0f, ret);
            y[idx_m] += alpha *(result[0] + result[1]);
            y[idx_m+1] += alpha * (result[2] + result[3]);
        }

        if(tag_m_2x != m ) {
            c1 = _mm_maskz_loadu_ps(0x0f, &a[tag_m_2x * 4]);
            c2 = _mm_maskz_mul_ps(0x0f, c1, xarray);
            _mm_mask_storeu_ps(result, 0x0f, c2);
            y[tag_m_2x] += alpha *(result[0] + result[1] + result[2] + result[3]);
        }
    }

    return 0;
}

static int sgemv_kernel_t_5(BLASLONG m, float alpha, float *a, float *x, float *y)
{
    BLASLONG tag_m_16x = m & (~15);
    BLASLONG tag_m_8x = m & (~7);
    BLASLONG tag_m_4x = m & (~3);
    BLASLONG tag_m_2x = m & (~1);
    __m512 m0, m1, m2, m3, m4, tmp0, tmp1, tmp2, accum, c0, c1, c2, c3, c4;
    __m512 x0_512 = _mm512_set1_ps(x[0]);
    __m512 x1_512 = _mm512_set1_ps(x[1]);
    __m512 x2_512 = _mm512_set1_ps(x[2]);
    __m512 x3_512 = _mm512_set1_ps(x[3]);
    __m512 x4_512 = _mm512_set1_ps(x[4]);
    __m512 alpha_512 = _mm512_set1_ps(alpha);


    __m512i M512_EPI32_1 = _mm512_set1_epi32(1);
    __m512i M512_EPI32_16 = _mm512_set1_epi32(16);
    __m512i M512_EPI32_0 = _mm512_setzero_epi32();

    __m512i idx_c0 = _mm512_set_epi32(27, 22, 17, 28, 23, 18, 13, 8, 3, 30, 25, 20, 15, 10, 5, 0);
    __m512i idx_c1 = _mm512_add_epi32(idx_c0, M512_EPI32_1);
    __m512i idx_c2 = _mm512_add_epi32(idx_c1, M512_EPI32_1);
    idx_c2 = _mm512_mask_blend_epi32(0x0040, idx_c2, M512_EPI32_0);
    __m512i idx_c3 = _mm512_add_epi32(idx_c2, M512_EPI32_1);
    __m512i idx_c4 = _mm512_add_epi32(idx_c3, M512_EPI32_1);
    idx_c4 = _mm512_mask_blend_epi32(0x1000, idx_c4, M512_EPI32_16);

    for (BLASLONG idx_m=0; idx_m < tag_m_16x; idx_m+=16) {      
        m0 = _mm512_loadu_ps(&a[idx_m*5]);
        m1 = _mm512_loadu_ps(&a[idx_m*5 + 16]);
        m2 = _mm512_loadu_ps(&a[idx_m*5 + 32]);
        m3 = _mm512_loadu_ps(&a[idx_m*5 + 48]);
        m4 = _mm512_loadu_ps(&a[idx_m*5 + 64]);

        tmp0 = _mm512_maskz_permutex2var_ps(0x007f, m0, idx_c0, m1);
        tmp1 = _mm512_maskz_permutex2var_ps(0x1f80, m2, idx_c0, m3);
        c0 = _mm512_mask_blend_ps(0x1f80, tmp0, tmp1);
        c0 = _mm512_mask_permutex2var_ps(c0, 0xe000, idx_c0, m4);

        tmp0 = _mm512_maskz_permutex2var_ps(0x007f, m0, idx_c1, m1);
        tmp1 = _mm512_maskz_permutex2var_ps(0x1f80, m2, idx_c1, m3);
        c1 = _mm512_mask_blend_ps(0x1f80, tmp0, tmp1);
        c1 = _mm512_mask_permutex2var_ps(c1, 0xe000, idx_c1, m4);

        tmp0 = _mm512_maskz_permutex2var_ps(0x003f, m0, idx_c2, m1);
        tmp1 = _mm512_maskz_permutex2var_ps(0x1fc0, m2, idx_c2, m3);
        c2 = _mm512_mask_blend_ps(0x1fc0, tmp0, tmp1);
        c2 = _mm512_mask_permutex2var_ps(c2, 0xe000, idx_c2, m4);

        tmp0 = _mm512_maskz_permutex2var_ps(0x003f, m0, idx_c3, m1);
        tmp1 = _mm512_maskz_permutex2var_ps(0x1fc0, m2, idx_c3, m3);
        c3 = _mm512_mask_blend_ps(0x1fc0, tmp0, tmp1);
        c3 = _mm512_mask_permutex2var_ps(c3, 0xe000, idx_c3, m4);
       
        tmp0 = _mm512_maskz_permutex2var_ps(0x003f, m0, idx_c4, m1);
        tmp1 = _mm512_maskz_permutex2var_ps(0x0fc0, m2, idx_c4, m3);
        c4 = _mm512_mask_blend_ps(0x0fc0, tmp0, tmp1);
        c4 = _mm512_mask_permutex2var_ps(c4, 0xf000, idx_c4, m4);
       
        accum = _mm512_fmadd_ps(c1, x1_512, _mm512_mul_ps(c0, x0_512));
        accum = _mm512_fmadd_ps(c2, x2_512, accum);
        accum = _mm512_fmadd_ps(c3, x3_512, accum);
        accum = _mm512_fmadd_ps(c4, x4_512, accum);
        accum = _mm512_fmadd_ps(accum, alpha_512, _mm512_loadu_ps(&y[idx_m]));
        _mm512_storeu_ps(&y[idx_m], accum);

    }
    if(tag_m_16x !=m) {
        __m512i idx_c0c2 = _mm512_set_epi32(0, 0, 27, 22, 17, 12, 7, 2 , 0, 30, 25, 20, 15, 10, 5, 0);
        __m512i idx_c1c3 = _mm512_add_epi32(idx_c0c2, M512_EPI32_1);
        idx_c4 = _mm512_add_epi32(idx_c1c3, M512_EPI32_1);
        __m256i idx_c0m4 = _mm256_set_epi32(11, 6, 0, 0, 0, 0, 0, 0);
        __m256i M256_EPI32_1 = _mm256_set1_epi32(1);
        __m256i idx_c1m4 = _mm256_add_epi32(idx_c0m4, M256_EPI32_1);
        __m256i idx_c2m4 = _mm256_add_epi32(idx_c1m4, M256_EPI32_1);
        __m256i idx_c3m4 = _mm256_add_epi32(idx_c2m4, M256_EPI32_1);
        __m256i idx_c4m4 = _mm256_add_epi32(idx_c3m4, M256_EPI32_1);
        //TODO: below can change to use extract to decrease the latency
        __m256 x0_256 = _mm256_set1_ps(x[0]);
        __m256 x1_256 = _mm256_set1_ps(x[1]);
        __m256 x2_256 = _mm256_set1_ps(x[2]);
        __m256 x3_256 = _mm256_set1_ps(x[3]);
        __m256 x4_256 = _mm256_set1_ps(x[4]);
        __m256 alpha256 = _mm256_set1_ps(alpha);
        __m256 accum_256, m256_4;

        for(BLASLONG idx_m=tag_m_16x; idx_m < tag_m_8x; idx_m+=8) {
            m0 = _mm512_loadu_ps(&a[idx_m*5]);
            m1 = _mm512_loadu_ps(&a[idx_m*5 + 16]);
            m256_4 = _mm256_loadu_ps(&a[idx_m*5 + 32]);
            tmp0 = _mm512_permutex2var_ps(m0, idx_c0c2, m1);
            tmp1 = _mm512_permutex2var_ps(m0, idx_c1c3, m1);
            tmp2 = _mm512_permutex2var_ps(m0, idx_c4, m1);

            __m256 c256_0 = _mm512_extractf32x8_ps(tmp0, 0);
            __m256 c256_2 = _mm512_extractf32x8_ps(tmp0, 1);
            __m256 c256_1 = _mm512_extractf32x8_ps(tmp1, 0);
            __m256 c256_3 = _mm512_extractf32x8_ps(tmp1, 1);
            __m256 c256_4 = _mm512_extractf32x8_ps(tmp2, 1);

            c256_0 = _mm256_mask_permutex2var_ps(c256_0, 0x80, idx_c0m4, m256_4);
            c256_1 = _mm256_mask_permutex2var_ps(c256_1, 0x80, idx_c1m4, m256_4);
            c256_2 = _mm256_mask_permutex2var_ps(c256_2, 0xc0, idx_c2m4, m256_4);
            c256_3 = _mm256_mask_permutex2var_ps(c256_3, 0xc0, idx_c3m4, m256_4);
            c256_4 = _mm256_mask_permutex2var_ps(c256_4, 0xc0, idx_c4m4, m256_4);
            
            accum_256 = _mm256_fmadd_ps(c256_1, x1_256, _mm256_mul_ps(c256_0, x0_256));
            accum_256 = _mm256_fmadd_ps(c256_2, x2_256, accum_256);
            accum_256 = _mm256_fmadd_ps(c256_3, x3_256, accum_256);
            accum_256 = _mm256_fmadd_ps(c256_4, x4_256, accum_256);
            accum_256 = _mm256_fmadd_ps(accum_256, alpha256, _mm256_loadu_ps(&y[idx_m]));
            _mm256_storeu_ps(&y[idx_m], accum_256);
        }
        if(tag_m_8x != m) {
            __m256i idx_c02 = _mm256_set_epi32(17, 12, 7, 2, 15, 10, 5, 0);
            __m256i idx_c13 = _mm256_add_epi32(idx_c02, M256_EPI32_1);
            __m256i idx_4 = _mm256_add_epi32(idx_c13, M256_EPI32_1); 
            __m128 accum_128;
            __m256 m256_0, m256_1, tmp256_0, tmp256_1;
            for (BLASLONG idx_m = tag_m_8x; idx_m < tag_m_4x; idx_m+=4){
                m256_0 = _mm256_loadu_ps(&a[idx_m*5]);
                m256_1 = _mm256_loadu_ps(&a[idx_m*5 + 8]);
                __m128 m128_4 = _mm_maskz_loadu_ps(0x0f, &a[idx_m*5 + 16]);

                tmp256_0 = _mm256_permutex2var_ps(m256_0, idx_c02, m256_1);
                tmp256_1 = _mm256_permutex2var_ps(m256_0, idx_c13, m256_1);
                __m256 tmp256_2 = _mm256_maskz_permutex2var_ps(0xf0, m256_0, idx_4, m256_1);

                __m128 c128_0 = _mm256_extractf32x4_ps(tmp256_0, 0);
                __m128 c128_1 = _mm256_extractf32x4_ps(tmp256_1, 0);
                __m128 c128_2 = _mm256_extractf32x4_ps(tmp256_0, 1);
                __m128 c128_3 = _mm256_extractf32x4_ps(tmp256_1, 1);
                __m128 c128_4 = _mm256_extractf32x4_ps(tmp256_2, 1);

                __m128i idx_c14 = _mm_set_epi32(4, 0, 0, 0);
                __m128i M128_EPI32_1 = _mm_set1_epi32(1);
                __m128i idx_c24 = _mm_add_epi32(idx_c14, M128_EPI32_1);
                __m128i idx_c34 = _mm_add_epi32(idx_c24, M128_EPI32_1);
                __m128i idx_c44 = _mm_add_epi32(idx_c34, M128_EPI32_1);

                c128_1 = _mm_mask_permutex2var_ps(c128_1, 0x08, idx_c14, m128_4);
                c128_2 = _mm_mask_permutex2var_ps(c128_2, 0x08, idx_c24, m128_4);
                c128_3 = _mm_mask_permutex2var_ps(c128_3, 0x08, idx_c34, m128_4);
                c128_4 = _mm_mask_permutex2var_ps(c128_4, 0x08, idx_c44, m128_4);

                __m128 x128_0 = _mm256_extractf32x4_ps(x0_256, 0);
                __m128 x128_1 = _mm256_extractf32x4_ps(x1_256, 0);
                __m128 x128_2 = _mm256_extractf32x4_ps(x2_256, 0);
                __m128 x128_3 = _mm256_extractf32x4_ps(x3_256, 0);
                __m128 x128_4 = _mm256_extractf32x4_ps(x4_256, 0);

                __m128 alpha_128 = _mm256_extractf32x4_ps(alpha256, 0);
                accum_128 = _mm_maskz_fmadd_ps(0x0f, c128_1, x128_1, _mm_maskz_mul_ps(0x0f, c128_0, x128_0));
                accum_128 = _mm_maskz_fmadd_ps(0x0f, c128_2, x128_2, accum_128);
                accum_128 = _mm_maskz_fmadd_ps(0x0f, c128_3, x128_3, accum_128);
                accum_128 = _mm_maskz_fmadd_ps(0x0f, c128_4, x128_4, accum_128);
                accum_128 = _mm_maskz_fmadd_ps(0x0f, accum_128, alpha_128, _mm_maskz_loadu_ps(0x0f, &y[idx_m]));
                _mm_mask_storeu_ps(&y[idx_m], 0x0f, accum_128);

            }

            if(tag_m_4x !=m ){
                x0_256 = _mm256_maskz_loadu_ps(0x1f, x);
                x0_256 = _mm256_mul_ps(x0_256, alpha256);
                float ret8[8];

                for(BLASLONG idx_m = tag_m_4x; idx_m < tag_m_2x; idx_m+=2){
                    m256_0 = _mm256_maskz_loadu_ps(0x1f, &a[idx_m*5]);
                    m256_1 = _mm256_maskz_loadu_ps(0x1f, &a[idx_m*5 + 5]);

                    m256_0 = _mm256_mul_ps(m256_0, x0_256);
                    m256_1 = _mm256_mul_ps(m256_1, x0_256);

                    _mm256_mask_storeu_ps(ret8, 0x1f, m256_0);
                    y[idx_m] += ret8[0] + ret8[1] + ret8[2] + ret8[3] + ret8[4];
                    _mm256_mask_storeu_ps(ret8, 0x1f, m256_1);
                    y[idx_m+1] += ret8[0] + ret8[1] + ret8[2] + ret8[3] + ret8[4];                    

                }

                if(tag_m_2x != m){
                    m256_0 = _mm256_maskz_loadu_ps(0x1f, &a[tag_m_2x*5]);
                    m256_0 = _mm256_mul_ps(m256_0, x0_256);


                    _mm256_mask_storeu_ps(ret8, 0x1f, m256_0);
                    y[tag_m_2x] += ret8[0] + ret8[1] + ret8[2] + ret8[3] + ret8[4];

                }
            }
        }

    }
    return 0;
}

static int sgemv_kernel_t_6(BLASLONG m, float alpha, float *a, float *x, float *y)
{
    BLASLONG tag_m_16x = m & (~15);
    BLASLONG tag_m_8x = m & (~7);
    BLASLONG tag_m_4x = m & (~3);
    BLASLONG tag_m_2x = m & (~1);

    __m512 m0, m1, m2, m3, m4, m5, c0, c1, c2, c3, c4, c5, tmp0, tmp1, tmp2, accum;
    __m512i idx_c0 = _mm512_set_epi32(26, 20, 14, 8, 2, 28, 22, 16, 10, 4, 30, 24, 18, 12, 6, 0);
    __m512i M512_EPI32_1 = _mm512_set1_epi32(1);
    __m512i M512_EPI32_0 = _mm512_setzero_epi32();
    __m512i M512_EPI32_16 = _mm512_set1_epi32(16);
    __m512i idx_c1 = _mm512_add_epi32(idx_c0, M512_EPI32_1);
    __m512i idx_c2 = _mm512_add_epi32(idx_c1, M512_EPI32_1);
    idx_c2 = _mm512_mask_blend_epi32(0x0020, idx_c2, M512_EPI32_0);
    __m512i idx_c3 = _mm512_add_epi32(idx_c2, M512_EPI32_1);
    __m512i idx_c4 = _mm512_add_epi32(idx_c3, M512_EPI32_1);
    idx_c4 = _mm512_mask_blend_epi32(0x0400, idx_c4, M512_EPI32_0);
    __m512i idx_c5 = _mm512_add_epi32(idx_c4, M512_EPI32_1);

    __m512 x0_512 = _mm512_set1_ps(x[0]);
    __m512 x1_512 = _mm512_set1_ps(x[1]);
    __m512 x2_512 = _mm512_set1_ps(x[2]);
    __m512 x3_512 = _mm512_set1_ps(x[3]);
    __m512 x4_512 = _mm512_set1_ps(x[4]);
    __m512 x5_512 = _mm512_set1_ps(x[5]);
    __m512 alpha_512 = _mm512_set1_ps(alpha);

    for (BLASLONG idx_m=0; idx_m < tag_m_16x; idx_m+=16) {      
        m0 = _mm512_loadu_ps(&a[idx_m*6]);
        m1 = _mm512_loadu_ps(&a[idx_m*6 + 16]);
        m2 = _mm512_loadu_ps(&a[idx_m*6 + 32]);
        m3 = _mm512_loadu_ps(&a[idx_m*6 + 48]);
        m4 = _mm512_loadu_ps(&a[idx_m*6 + 64]);
        m5 = _mm512_loadu_ps(&a[idx_m*6 + 80]);

        tmp0 = _mm512_maskz_permutex2var_ps(0x003f, m0, idx_c0, m1);
        tmp1 = _mm512_maskz_permutex2var_ps(0x07c0, m2, idx_c0, m3);
        tmp2 = _mm512_maskz_permutex2var_ps(0xf800, m4, idx_c0, m5);
        c0 = _mm512_mask_blend_ps(0x07c0, tmp0, tmp1);
        c0 = _mm512_mask_blend_ps(0xf800, c0, tmp2);

        tmp0 = _mm512_maskz_permutex2var_ps(0x003f, m0, idx_c1, m1);
        tmp1 = _mm512_maskz_permutex2var_ps(0x07c0, m2, idx_c1, m3);
        tmp2 = _mm512_maskz_permutex2var_ps(0xf800, m4, idx_c1, m5);
        c1 = _mm512_mask_blend_ps(0x07c0, tmp0, tmp1);
        c1 = _mm512_mask_blend_ps(0xf800, c1, tmp2);

        tmp0 = _mm512_maskz_permutex2var_ps(0x001f, m0, idx_c2, m1);
        tmp1 = _mm512_maskz_permutex2var_ps(0x07e0, m2, idx_c2, m3);
        tmp2 = _mm512_maskz_permutex2var_ps(0xf800, m4, idx_c2, m5);
        c2 = _mm512_mask_blend_ps(0x07e0, tmp0, tmp1);
        c2 = _mm512_mask_blend_ps(0xf800, c2, tmp2);

        tmp0 = _mm512_maskz_permutex2var_ps(0x001f, m0, idx_c3, m1);
        tmp1 = _mm512_maskz_permutex2var_ps(0x07e0, m2, idx_c3, m3);
        tmp2 = _mm512_maskz_permutex2var_ps(0xf800, m4, idx_c3, m5);
        c3 = _mm512_mask_blend_ps(0x07e0, tmp0, tmp1);
        c3 = _mm512_mask_blend_ps(0xf800, c3, tmp2);

        tmp0 = _mm512_maskz_permutex2var_ps(0x001f, m0, idx_c4, m1);
        tmp1 = _mm512_maskz_permutex2var_ps(0x03e0, m2, idx_c4, m3);
        tmp2 = _mm512_maskz_permutex2var_ps(0xfc00, m4, idx_c4, m5);
        c4 = _mm512_mask_blend_ps(0x03e0, tmp0, tmp1);
        c4 = _mm512_mask_blend_ps(0xfc00, c4, tmp2);

        tmp0 = _mm512_maskz_permutex2var_ps(0x001f, m0, idx_c5 , m1);
        tmp1 = _mm512_maskz_permutex2var_ps(0x03e0, m2, idx_c5 , m3);
        tmp2 = _mm512_maskz_permutex2var_ps(0xfc00, m4, idx_c5 , m5);
        c5 = _mm512_mask_blend_ps(0x03e0, tmp0, tmp1);
        c5 = _mm512_mask_blend_ps(0xfc00, c5, tmp2);

        accum = _mm512_fmadd_ps(c1, x1_512, _mm512_mul_ps(c0, x0_512));
        accum = _mm512_fmadd_ps(c2, x2_512, accum);
        accum = _mm512_fmadd_ps(c3, x3_512, accum);
        accum = _mm512_fmadd_ps(c4, x4_512, accum);
        accum = _mm512_fmadd_ps(c5, x5_512, accum);
        accum = _mm512_fmadd_ps(accum, alpha_512, _mm512_loadu_ps(&y[idx_m]));
        _mm512_storeu_ps(&y[idx_m], accum);
    }

    if(tag_m_16x != m) {
        __m512i idx_c0c3 = _mm512_set_epi32(29, 23, 17, 27, 21, 15, 9, 3, 26, 20, 30, 24, 18, 12, 6, 0);
        __m512i idx_c1c4 = _mm512_add_epi32(idx_c0c3, M512_EPI32_1);
        __m512i idx_c2c5 = _mm512_add_epi32(idx_c1c4, M512_EPI32_1);
        idx_c2c5 = _mm512_mask_blend_epi32(0x0020, idx_c2c5, M512_EPI32_16);
        __m256 c256_0, c256_1, c256_2, c256_3, c256_4, c256_5;

        __m256 x0_256 = _mm256_set1_ps(x[0]);
        __m256 x1_256 = _mm256_set1_ps(x[1]);
        __m256 x2_256 = _mm256_set1_ps(x[2]);
        __m256 x3_256 = _mm256_set1_ps(x[3]);
        __m256 x4_256 = _mm256_set1_ps(x[4]);
        __m256 x5_256 = _mm256_set1_ps(x[5]);
        __m256 alpha256 = _mm256_set1_ps(alpha);
        __m256 accum_256;

        for(BLASLONG idx_m = tag_m_16x; idx_m <tag_m_8x; idx_m+=8){
            m0 = _mm512_loadu_ps(&a[idx_m*6]);
            m1 = _mm512_loadu_ps(&a[idx_m*6 + 16]);
            m2 = _mm512_loadu_ps(&a[idx_m*6 + 32]);

            tmp0 = _mm512_maskz_permutex2var_ps(0x1f3f, m0, idx_c0c3 , m1);
            tmp1 = _mm512_mask_permutex2var_ps(tmp0, 0xe0c0, idx_c0c3 , m2);
            c256_0 = _mm512_extractf32x8_ps(tmp1, 0);
            c256_3 = _mm512_extractf32x8_ps(tmp1, 1);

            tmp0 = _mm512_maskz_permutex2var_ps(0x1f3f, m0, idx_c1c4 , m1);
            tmp1 = _mm512_mask_permutex2var_ps(tmp0, 0xe0c0, idx_c1c4 , m2);
            c256_1 = _mm512_extractf32x8_ps(tmp1, 0);
            c256_4 = _mm512_extractf32x8_ps(tmp1, 1);

            tmp0 = _mm512_maskz_permutex2var_ps(0x1f1f, m0, idx_c2c5 , m1);
            tmp1 = _mm512_mask_permutex2var_ps(tmp0, 0xe0e0, idx_c2c5 , m2);
            c256_2 = _mm512_extractf32x8_ps(tmp1, 0);
            c256_5 = _mm512_extractf32x8_ps(tmp1, 1);            

            accum_256 = _mm256_fmadd_ps(c256_1, x1_256, _mm256_mul_ps(c256_0, x0_256));
            accum_256 = _mm256_fmadd_ps(c256_2, x2_256, accum_256);
            accum_256 = _mm256_fmadd_ps(c256_3, x3_256, accum_256);
            accum_256 = _mm256_fmadd_ps(c256_4, x4_256, accum_256);
            accum_256 = _mm256_fmadd_ps(c256_5, x5_256, accum_256);
            accum_256 = _mm256_fmadd_ps(accum_256, alpha256, _mm256_loadu_ps(&y[idx_m]));
            _mm256_storeu_ps(&y[idx_m], accum_256);
        }

        if(tag_m_8x != m) {
            __m256 m256_0, m256_1, m256_2;
            __m128 c128_0, c128_1;
            idx_c0 = _mm512_set_epi32(22, 16, 10, 4, 19, 13, 7, 1, 21, 15, 9, 3, 18, 12, 6, 0);
            idx_c1 = _mm512_add_epi32(idx_c0, M512_EPI32_1);
            m2 = _mm512_set_ps(x[4], x[4], x[4], x[4], x[1], x[1], x[1], x[1], x[3], x[3], x[3], x[3], x[0], x[0], x[0], x[0]);
            m3 = _mm512_set_ps(x[5], x[5], x[5], x[5], x[2], x[2], x[2], x[2], 0, 0, 0, 0, 0, 0, 0, 0);
            for(BLASLONG idx_m=tag_m_8x; idx_m < tag_m_4x; idx_m+=4) {
                m0 = _mm512_loadu_ps(&a[idx_m*6]);
                m1 = _mm512_maskz_loadu_ps(0x00ff, &a[idx_m*6+16]);

                tmp0 = _mm512_permutex2var_ps(m0, idx_c0, m1);
                tmp1 = _mm512_maskz_permutex2var_ps(0xff00, m0, idx_c1, m1);

                tmp0 = _mm512_mul_ps(tmp0, m2);
                tmp1 = _mm512_mul_ps(tmp1, m3);
                
                tmp0 = _mm512_add_ps(tmp0, tmp1);

                m256_0 = _mm512_extractf32x8_ps(tmp0, 0);
                m256_1 = _mm512_extractf32x8_ps(tmp0, 1);

                m256_0 = _mm256_add_ps(m256_0, m256_1);
                m256_0 = _mm256_mul_ps(m256_0, alpha256);
                c128_0 = _mm256_extractf32x4_ps(m256_0, 0);
                c128_1 = _mm256_extractf32x4_ps(m256_0, 1);

                c128_0 = _mm_maskz_add_ps(0x0f, c128_0, c128_1);
                c128_0 = _mm_maskz_add_ps(0x0f, c128_0, _mm_maskz_loadu_ps(0x0f, &y[idx_m]));
                _mm_mask_storeu_ps(&y[idx_m], 0x0f, c128_0);
            }

            if(tag_m_4x != m) {
                //m256_2 is x*alpha
                m256_2 = _mm256_maskz_loadu_ps(0x3f, x);
                m256_2 = _mm256_mul_ps(m256_2, alpha256);
                float ret8[8];
                for(BLASLONG idx_m=tag_m_4x; idx_m < tag_m_2x;idx_m+=2) {
                    m256_0 = _mm256_maskz_loadu_ps(0x3f, &a[idx_m*6]);
                    m256_1 = _mm256_maskz_loadu_ps(0x3f, &a[idx_m*6 + 6]);
                    
                    m256_0 = _mm256_mul_ps(m256_0, m256_2);
                    m256_1 = _mm256_mul_ps(m256_1, m256_2);

                    _mm256_storeu_ps(ret8, m256_0);
                    for(int i=0; i<6;i++){
                        y[idx_m]+=ret8[i];
                    }

                    _mm256_storeu_ps(ret8, m256_1);
                    for(int i=0; i<6;i++){
                        y[idx_m+1]+=ret8[i];
                    }
                }
                
                if(tag_m_2x !=m) {
                    m256_0 = _mm256_maskz_loadu_ps(0x3f, &a[tag_m_2x*6]);
                    m256_0 = _mm256_mul_ps(m256_0, m256_2);
                    
                    _mm256_storeu_ps(ret8, m256_0);
                    for(int i=0; i<6;i++){
                        y[tag_m_2x]+=ret8[i];
                    }
                }
            }
        }
    }
    
    return 0;
}

static int sgemv_kernel_t_7(BLASLONG m, float alpha, float *a, float *x, float *y)
{
    BLASLONG tag_m_16x = m & (~15);
    BLASLONG tag_m_8x = m & (~7);
    BLASLONG tag_m_4x = m & (~3);
    BLASLONG tag_m_2x = m & (~1);

    __m512 m0, m1, m2, m3, m4, m5, m6, tmp0, tmp1, tmp2, c0, c1, c2, c3, c4, c5, c6, accum;
    __m512i idx_c0 = _mm512_set_epi32(25, 18, 27, 20, 13, 6, 31, 24, 17, 10, 3, 28, 21, 14, 7, 0);

    __m512i M512_EPI32_1 = _mm512_set1_epi32(1);
    __m512i M512_EPI32_0 = _mm512_setzero_epi32();
    __m512i M512_EPI32_16 = _mm512_set1_epi32(16);

    __m512i idx_c1 = _mm512_add_epi32(idx_c0, M512_EPI32_1);
    idx_c1 = _mm512_mask_blend_epi32(0x0200, idx_c1, M512_EPI32_0);
    __m512i idx_c2 = _mm512_add_epi32(idx_c1, M512_EPI32_1);
    __m512i idx_c3 = _mm512_add_epi32(idx_c2, M512_EPI32_1);
    __m512i idx_c4 = _mm512_add_epi32(idx_c3, M512_EPI32_1);
    idx_c4 = _mm512_mask_blend_epi32(0x0010, idx_c4, M512_EPI32_0);
    __m512i idx_c5 = _mm512_add_epi32(idx_c4, M512_EPI32_1);
    idx_c5 = _mm512_mask_blend_epi32(0x2000, idx_c5, M512_EPI32_16);
    __m512i idx_c6 = _mm512_add_epi32(idx_c5, M512_EPI32_1);

    __m512 x0_512 = _mm512_set1_ps(x[0]);
    __m512 x1_512 = _mm512_set1_ps(x[1]);
    __m512 x2_512 = _mm512_set1_ps(x[2]);
    __m512 x3_512 = _mm512_set1_ps(x[3]);
    __m512 x4_512 = _mm512_set1_ps(x[4]);
    __m512 x5_512 = _mm512_set1_ps(x[5]);
    __m512 x6_512 = _mm512_set1_ps(x[6]);
    __m512 alpha_512 = _mm512_set1_ps(alpha);

    for (BLASLONG idx_m=0; idx_m < tag_m_16x; idx_m+=16) {
        m0 = _mm512_loadu_ps(&a[idx_m*7]);
        m1 = _mm512_loadu_ps(&a[idx_m*7 + 16]);
        m2 = _mm512_loadu_ps(&a[idx_m*7 + 32]);
        m3 = _mm512_loadu_ps(&a[idx_m*7 + 48]);
        m4 = _mm512_loadu_ps(&a[idx_m*7 + 64]);
        m5 = _mm512_loadu_ps(&a[idx_m*7 + 80]);
        m6 = _mm512_loadu_ps(&a[idx_m*7 + 96]);

        tmp0 = _mm512_maskz_permutex2var_ps(0x001f, m0, idx_c0, m1);
        tmp1 = _mm512_maskz_permutex2var_ps(0x03e0, m2, idx_c0, m3);
        tmp2 = _mm512_maskz_permutex2var_ps(0x3c00, m4, idx_c0, m5);
        c0 = _mm512_mask_blend_ps(0x03e0, tmp0, tmp1);
        c0 = _mm512_mask_blend_ps(0x3c00, c0, tmp2);        
        c0 = _mm512_mask_permutex2var_ps(c0, 0xc000, idx_c0, m6);        
        
        tmp0 = _mm512_maskz_permutex2var_ps(0x001f, m0, idx_c1, m1);
        tmp1 = _mm512_maskz_permutex2var_ps(0x01e0, m2, idx_c1, m3);
        tmp2 = _mm512_maskz_permutex2var_ps(0x3e00, m4, idx_c1, m5);
        c1 = _mm512_mask_blend_ps(0x01e0, tmp0, tmp1);
        c1 = _mm512_mask_blend_ps(0x3e00, c1, tmp2);        
        c1 = _mm512_mask_permutex2var_ps(c1, 0xc000, idx_c1, m6); 

        tmp0 = _mm512_maskz_permutex2var_ps(0x001f, m0, idx_c2, m1);
        tmp1 = _mm512_maskz_permutex2var_ps(0x01e0, m2, idx_c2, m3);
        tmp2 = _mm512_maskz_permutex2var_ps(0x3e00, m4, idx_c2, m5);
        c2 = _mm512_mask_blend_ps(0x01e0, tmp0, tmp1);
        c2 = _mm512_mask_blend_ps(0x3e00, c2, tmp2);        
        c2 = _mm512_mask_permutex2var_ps(c2, 0xc000, idx_c2, m6); 

        tmp0 = _mm512_maskz_permutex2var_ps(0x001f, m0, idx_c3, m1);
        tmp1 = _mm512_maskz_permutex2var_ps(0x01e0, m2, idx_c3, m3);
        tmp2 = _mm512_maskz_permutex2var_ps(0x3e00, m4, idx_c3, m5);
        c3 = _mm512_mask_blend_ps(0x01e0, tmp0, tmp1);
        c3 = _mm512_mask_blend_ps(0x3e00, c3, tmp2);        
        c3 = _mm512_mask_permutex2var_ps(c3, 0xc000, idx_c3, m6); 

        tmp0 = _mm512_maskz_permutex2var_ps(0x000f, m0, idx_c4, m1);
        tmp1 = _mm512_maskz_permutex2var_ps(0x01f0, m2, idx_c4, m3);
        tmp2 = _mm512_maskz_permutex2var_ps(0x3e00, m4, idx_c4, m5);
        c4 = _mm512_mask_blend_ps(0x01f0, tmp0, tmp1);
        c4 = _mm512_mask_blend_ps(0x3e00, c4, tmp2);        
        c4 = _mm512_mask_permutex2var_ps(c4, 0xc000, idx_c4, m6); 

        tmp0 = _mm512_maskz_permutex2var_ps(0x000f, m0, idx_c5, m1);
        tmp1 = _mm512_maskz_permutex2var_ps(0x01f0, m2, idx_c5, m3);
        tmp2 = _mm512_maskz_permutex2var_ps(0x1e00, m4, idx_c5, m5);
        c5 = _mm512_mask_blend_ps(0x01f0, tmp0, tmp1);
        c5 = _mm512_mask_blend_ps(0x1e00, c5, tmp2);        
        c5 = _mm512_mask_permutex2var_ps(c5, 0xe000, idx_c5, m6); 

        tmp0 = _mm512_maskz_permutex2var_ps(0x000f, m0, idx_c6, m1);
        tmp1 = _mm512_maskz_permutex2var_ps(0x01f0, m2, idx_c6, m3);
        tmp2 = _mm512_maskz_permutex2var_ps(0x1e00, m4, idx_c6, m5);
        c6 = _mm512_mask_blend_ps(0x01f0, tmp0, tmp1);
        c6 = _mm512_mask_blend_ps(0x1e00, c6, tmp2);        
        c6 = _mm512_mask_permutex2var_ps(c6, 0xe000, idx_c6, m6); 

        accum = _mm512_fmadd_ps(c1, x1_512, _mm512_mul_ps(c0, x0_512));
        accum = _mm512_fmadd_ps(c2, x2_512, accum);
        accum = _mm512_fmadd_ps(c3, x3_512, accum);
        accum = _mm512_fmadd_ps(c4, x4_512, accum);
        accum = _mm512_fmadd_ps(c5, x5_512, accum);
        accum = _mm512_fmadd_ps(c6, x6_512, accum);
        accum = _mm512_fmadd_ps(accum, alpha_512, _mm512_loadu_ps(&y[idx_m]));
        _mm512_storeu_ps(&y[idx_m], accum);

    }

    if(tag_m_16x != m){
        //this is idx of c0c3
        idx_c0 = _mm512_set_epi32(20, 13, 6, 31, 24, 17, 10, 3, 17, 10, 3, 28, 21, 14, 7, 0);
        //this is idx of c1c4
        idx_c1 = _mm512_add_epi32(idx_c0, M512_EPI32_1);
        idx_c1 = _mm512_mask_blend_epi32(0x1000, idx_c1, M512_EPI32_0);
        //this is idx of c2c5
        idx_c2 = _mm512_add_epi32(idx_c1, M512_EPI32_1);
        idx_c6 = _mm512_add_epi32(idx_c2, M512_EPI32_1);
        __m256 c256_0, c256_1, c256_2, c256_3, c256_4, c256_5, c256_6;
        __m256 x0_256 = _mm256_set1_ps(x[0]);
        __m256 x1_256 = _mm256_set1_ps(x[1]);
        __m256 x2_256 = _mm256_set1_ps(x[2]);
        __m256 x3_256 = _mm256_set1_ps(x[3]);
        __m256 x4_256 = _mm256_set1_ps(x[4]);
        __m256 x5_256 = _mm256_set1_ps(x[5]);
        __m256 x6_256 = _mm256_set1_ps(x[6]);
        __m256 alpha256 = _mm256_set1_ps(alpha);
        __m256 accum_256;
        for (BLASLONG idx_m=tag_m_16x; idx_m < tag_m_8x; idx_m+=8) {
            m0 = _mm512_loadu_ps(&a[idx_m*7]);
            m1 = _mm512_loadu_ps(&a[idx_m*7 + 16]);
            m2 = _mm512_loadu_ps(&a[idx_m*7 + 32]);
            m3 = _mm512_maskz_loadu_ps(0x00ff, &a[idx_m*7 + 48]);

            tmp0 = _mm512_maskz_permutex2var_ps(0x1f1f, m0, idx_c0, m1);
            tmp1 = _mm512_maskz_permutex2var_ps(0xe0e0, m2, idx_c0, m3);
            //this is c0c3
            c0 = _mm512_mask_blend_ps(0xe0e0, tmp0, tmp1);
            c256_0 = _mm512_extractf32x8_ps(c0, 0);
            c256_3 = _mm512_extractf32x8_ps(c0, 1);

            tmp0 = _mm512_maskz_permutex2var_ps(0x0f1f, m0, idx_c1, m1);
            tmp1 = _mm512_maskz_permutex2var_ps(0xf0e0, m2, idx_c1, m3);
            //this is c1c4
            c1 = _mm512_mask_blend_ps(0xf0e0, tmp0, tmp1);
            c256_1 = _mm512_extractf32x8_ps(c1, 0);
            c256_4 = _mm512_extractf32x8_ps(c1, 1);

            tmp0 = _mm512_maskz_permutex2var_ps(0x0f1f, m0, idx_c2, m1);
            tmp1 = _mm512_maskz_permutex2var_ps(0xf0e0, m2, idx_c2, m3);
            //this is c2c5
            c2 = _mm512_mask_blend_ps(0xf0e0, tmp0, tmp1);
            c256_2 = _mm512_extractf32x8_ps(c2, 0);
            c256_5 = _mm512_extractf32x8_ps(c2, 1);

            tmp0 = _mm512_maskz_permutex2var_ps(0x0f00, m0, idx_c6, m1);
            tmp1 = _mm512_maskz_permutex2var_ps(0xf000, m2, idx_c6, m3);
            //this is c2c5
            c6 = _mm512_mask_blend_ps(0xf000, tmp0, tmp1);  
            c256_6 = _mm512_extractf32x8_ps(c6, 1);
            accum_256 = _mm256_fmadd_ps(c256_1, x1_256, _mm256_mul_ps(c256_0, x0_256));
            accum_256 = _mm256_fmadd_ps(c256_2, x2_256, accum_256);
            accum_256 = _mm256_fmadd_ps(c256_3, x3_256, accum_256);
            accum_256 = _mm256_fmadd_ps(c256_4, x4_256, accum_256);
            accum_256 = _mm256_fmadd_ps(c256_5, x5_256, accum_256);
            accum_256 = _mm256_fmadd_ps(c256_6, x6_256, accum_256);

            accum_256 = _mm256_fmadd_ps(accum_256, alpha256, _mm256_loadu_ps(&y[idx_m]));
            _mm256_storeu_ps(&y[idx_m], accum_256);
        }

        if(tag_m_8x!=m) {
            idx_c0 = _mm512_set_epi32(27, 20, 13, 6, 25, 18, 11, 4, 23, 16, 9, 2 ,21, 14, 7, 0);
            idx_c1 = _mm512_add_epi32(idx_c0, M512_EPI32_1);

            for (BLASLONG idx_m=tag_m_8x; idx_m < tag_m_4x; idx_m+=4) {
                m0 = _mm512_loadu_ps(&a[idx_m*7]);
                m1 = _mm512_maskz_loadu_ps(0x0fff, &a[idx_m*7 + 16]);
                //this is x
                m2 = _mm512_set_ps(x[6], x[6], x[6], x[6], x[4], x[4], x[4], x[4], x[2], x[2], x[2], x[2], x[0], x[0], x[0], x[0]);
                //this is x
                m4 = _mm512_set_ps(0, 0, 0, 0, x[5], x[5], x[5], x[5], x[3], x[3], x[3], x[3], x[1], x[1], x[1], x[1]);

                tmp0 = _mm512_permutex2var_ps(m0, idx_c0, m1);
                tmp1 = _mm512_maskz_permutex2var_ps(0x0fff, m0, idx_c1, m1);

                tmp0 = _mm512_mul_ps(tmp0, m2);
                tmp1 = _mm512_mul_ps(tmp1, m4);

                tmp0 = _mm512_add_ps(tmp0, tmp1);

                c256_0 = _mm512_extractf32x8_ps(tmp0, 0);
                c256_1 = _mm512_extractf32x8_ps(tmp0, 1);

                c256_0 = _mm256_add_ps(c256_0, c256_1);
                c256_0 = _mm256_mul_ps(c256_0, alpha256);

                __m128 c128_0 = _mm256_extractf32x4_ps(c256_0, 0);
                __m128 c128_1 = _mm256_extractf32x4_ps(c256_0, 1);

                c128_0 = _mm_maskz_add_ps(0x0f, c128_0, c128_1);
                c128_0 = _mm_maskz_add_ps(0x0f, c128_0, _mm_maskz_loadu_ps(0x0f, &y[idx_m]));
                _mm_mask_storeu_ps(&y[idx_m], 0x0f, c128_0);

            }

            if(tag_m_4x != m) {
                //c256_2 is x*alpha
                c256_2 = _mm256_maskz_loadu_ps(0x7f, x);
                c256_2 = _mm256_mul_ps(c256_2, alpha256);
                float ret8[8];
                for(BLASLONG idx_m=tag_m_4x; idx_m < tag_m_2x;idx_m+=2) {
                    c256_0 = _mm256_maskz_loadu_ps(0x7f, &a[idx_m*7]);
                    c256_1 = _mm256_maskz_loadu_ps(0x7f, &a[idx_m*7 + 7]);
                    
                    c256_0 = _mm256_mul_ps(c256_0, c256_2);
                    c256_1 = _mm256_mul_ps(c256_1, c256_2);

                    _mm256_storeu_ps(ret8, c256_0);
                    for(int i=0; i<7;i++){
                        y[idx_m]+=ret8[i];
                    }

                    _mm256_storeu_ps(ret8, c256_1);
                    for(int i=0; i<7;i++){
                        y[idx_m+1]+=ret8[i];
                    }
                }
                
                if(tag_m_2x !=m) {
                    c256_0 = _mm256_maskz_loadu_ps(0x7f, &a[tag_m_2x*7]);
                    c256_0 = _mm256_mul_ps(c256_0, c256_2);
                    
                    _mm256_storeu_ps(ret8, c256_0);
                    for(int i=0; i<7;i++){
                        y[tag_m_2x]+=ret8[i];
                    }
                }
            }
        }
    }

    return 0;
}

static int sgemv_kernel_t_8(BLASLONG m, float alpha, float *a, float *x, float *y)
{
    BLASLONG tag_m_8x = m & (~7);
    BLASLONG tag_m_4x = m & (~3);
    BLASLONG tag_m_2x = m & (~1);

    __m512 m0, m1, m2, m3;
    __m256 r0, r1, r2, r3, r4, r5, r6, r7, tmp0, tmp1, tmp2, tmp3;
    __m128 c128_0, c128_1, c128_2, c128_3;
    __m256 alpha256 = _mm256_set1_ps(alpha);

    __m256 x256 = _mm256_loadu_ps(x);
    x256 = _mm256_mul_ps(x256, alpha256);
    __m512 x512 = _mm512_broadcast_f32x8(x256);

    for(BLASLONG idx_m=0; idx_m<tag_m_8x; idx_m+=8) {
        m0 = _mm512_loadu_ps(&a[idx_m*8]);
        m1 = _mm512_loadu_ps(&a[idx_m*8 + 16]);
        m2 = _mm512_loadu_ps(&a[idx_m*8 + 32]);
        m3 = _mm512_loadu_ps(&a[idx_m*8 + 48]);
        m0 = _mm512_mul_ps(m0, x512);
        m1 = _mm512_mul_ps(m1, x512);
        m2 = _mm512_mul_ps(m2, x512);
        m3 = _mm512_mul_ps(m3, x512);

        r0 = _mm512_extractf32x8_ps(m0, 0);
        r1 = _mm512_extractf32x8_ps(m0, 1);
        r2 = _mm512_extractf32x8_ps(m1, 0);
        r3 = _mm512_extractf32x8_ps(m1, 1);
        r4 = _mm512_extractf32x8_ps(m2, 0);
        r5 = _mm512_extractf32x8_ps(m2, 1);
        r6 = _mm512_extractf32x8_ps(m3, 0);
        r7 = _mm512_extractf32x8_ps(m3, 1);

        tmp0 = _mm256_hadd_ps(r0, r1);
        tmp1 = _mm256_hadd_ps(r2, r3);
        tmp2 = _mm256_hadd_ps(r4, r5);
        tmp3 = _mm256_hadd_ps(r6, r7);
        tmp1 = _mm256_hadd_ps(tmp0, tmp1);
        tmp3 = _mm256_hadd_ps(tmp2, tmp3);
        c128_0 = _mm256_extractf32x4_ps(tmp1, 0);
        c128_1 = _mm256_extractf32x4_ps(tmp1, 1);
        c128_2 = _mm256_extractf32x4_ps(tmp3, 0);
        c128_3 = _mm256_extractf32x4_ps(tmp3, 1);

        c128_0 = _mm_add_ps(c128_0, c128_1);
        c128_2 = _mm_add_ps(c128_2, c128_3);
        _mm_storeu_ps(&y[idx_m], _mm_add_ps(c128_0, _mm_loadu_ps(&y[idx_m])));
        _mm_storeu_ps(&y[idx_m+4], _mm_add_ps(c128_2, _mm_loadu_ps(&y[idx_m+4])));
    }

    if (tag_m_8x !=m ){
        for(BLASLONG idx_m=tag_m_8x; idx_m<tag_m_4x; idx_m+=4) {
            m0 = _mm512_loadu_ps(&a[idx_m*8]);
            m1 = _mm512_loadu_ps(&a[idx_m*8 + 16]);

            m0 = _mm512_mul_ps(m0, x512);
            m1 = _mm512_mul_ps(m1, x512);

            r0 = _mm512_extractf32x8_ps(m0, 0);
            r1 = _mm512_extractf32x8_ps(m0, 1);
            r2 = _mm512_extractf32x8_ps(m1, 0);
            r3 = _mm512_extractf32x8_ps(m1, 1);

            tmp0 = _mm256_hadd_ps(r0, r1);
            tmp1 = _mm256_hadd_ps(r2, r3);

            tmp1 = _mm256_hadd_ps(tmp0, tmp1);
            c128_0 = _mm256_extractf32x4_ps(tmp1, 0);
            c128_1 = _mm256_extractf32x4_ps(tmp1, 1);

            c128_0 = _mm_add_ps(c128_0, c128_1);
            _mm_storeu_ps(&y[idx_m], _mm_add_ps(c128_0, _mm_loadu_ps(&y[idx_m])));

        }

        if(tag_m_4x != m) {
            float ret[4];
            for(BLASLONG idx_m=tag_m_4x; idx_m<tag_m_2x; idx_m+=2) {
                m0 = _mm512_loadu_ps(&a[idx_m*8]);
                m0 = _mm512_mul_ps(m0, x512);
                r0 = _mm512_extractf32x8_ps(m0, 0);
                r1 = _mm512_extractf32x8_ps(m0, 1);
                tmp0 = _mm256_hadd_ps(r0, r1);
                c128_0 = _mm256_extractf32x4_ps(tmp0, 0);
                c128_1 = _mm256_extractf32x4_ps(tmp0, 1);                

                c128_0 = _mm_add_ps(c128_0, c128_1);

                _mm_storeu_ps(ret, c128_0);
                y[idx_m] += (ret[0]+ret[1]);
                y[idx_m+1] += (ret[2]+ret[3]);

            }

            if(tag_m_2x!=m) {
                r0 = _mm256_loadu_ps(&a[tag_m_2x*8]);
                r0 = _mm256_mul_ps(r0, x256);

                c128_0 = _mm256_extractf32x4_ps(r0, 0);
                c128_1 = _mm256_extractf32x4_ps(r0, 1);   

                c128_0 = _mm_add_ps(c128_0, c128_1);
                _mm_storeu_ps(ret, c128_0);
                y[tag_m_2x] += (ret[0] + ret[1] + ret[2] + ret[3]);

            }
        }
    }
    return 0;
}
