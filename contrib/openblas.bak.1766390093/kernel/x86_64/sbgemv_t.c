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

#if defined (COOPERLAKE) || defined (SAPPHIRERAPIDS)
#include "sbgemv_t_microk_cooperlake.c"
#endif

#define ALIGN64_ALLOC(alloc_size, TYPE, ptr_align, ptr)   \
    ptr = (TYPE *) malloc(sizeof(TYPE)*alloc_size + 63); \
    ptr_align = ((int)(((uintptr_t)ptr & (uintptr_t)0x3F))!=0) ? (TYPE *)((char *)ptr + (64 - (int)((uintptr_t)ptr & (uintptr_t)0x3F))) : ptr

#define ALIGN64_FREE(ptr) \
    free(ptr)

#ifndef HAVE_SBGEMV_T_ACCL_KERNEL
static void sbgemv_kernel_t(BLASLONG m, BLASLONG n, float alpha, bfloat16 *a, BLASLONG lda, bfloat16 *x, float beta, float *y)
{
	BLASLONG offset_lda, offset_n;
    float accum = 0.0;

    bfloat16 * a_bf16 = malloc(sizeof(bfloat16)*m*n);
    float *    a_fp32 = malloc(sizeof(float)*m*n);
    float *    x_fp32 = malloc(sizeof(float)*n);

    for (BLASLONG i=0; i<m; i++)  {
        offset_lda = lda * i;
        offset_n = n * i;
        for (BLASLONG j=0; j<n; j++) {
            a_bf16[offset_n + j] = a[offset_lda + j];
        }
    }

    SBF16TOS_K(n, x, 1, x_fp32, 1);
    SBF16TOS_K(m*n, a_bf16, 1, a_fp32, 1);

	for (BLASLONG i=0; i<m; i++) {
		offset_n = n * i;
        accum = 0.0;
		for (BLASLONG j=0; j<n; j++) {
		    accum += a_fp32[offset_n + j] * x_fp32[j];
		}
        if (beta == ZERO) {
		    y[i] = alpha * accum;
        } else {
            y[i] = alpha * accum + beta * y[i];
        }
	}

    free(a_bf16);
    free(a_fp32);
    free(x_fp32);
}
#endif

static void bf16_compress_vector(BLASLONG n, bfloat16 * src, bfloat16 * target, BLASLONG inc)
{
    for(BLASLONG i=0; i<n; i++) {
        target[i] = src[i*inc];
    }
}

static void fp32_compress_vector(BLASLONG n, float * src, float * target, BLASLONG inc)
{
    for(BLASLONG i=0; i<n; i++) {
        target[i] = src[i*inc];
    }
}

static void fp32_expand_vector(BLASLONG n, float * src, float * target, BLASLONG inc)
{
    for(BLASLONG i=0; i<n; i++) {
        target[i*inc] = src[i];
    }
}

int CNAME(BLASLONG m, BLASLONG n, float alpha, bfloat16 *a, BLASLONG lda, bfloat16 *x, BLASLONG incx, float beta, float * y, BLASLONG incy)
{
    if ( m < 1 || n < 1) return(0);

    bfloat16 * xbuffer_align = x;
    float    * ybuffer_align = y;

    bfloat16 * xbuffer = NULL;
    float    * ybuffer = NULL;

    // Switch m and n
    BLASLONG t = m;
    m = n;
    n = t;

    if (incx != 1) {
        ALIGN64_ALLOC(n, bfloat16, xbuffer_align, xbuffer);
        bf16_compress_vector(n, x, xbuffer_align, incx);
    }

    if (incy != 1) {
        ALIGN64_ALLOC(m, float, ybuffer_align, ybuffer);
        if (beta != ZERO) {
            fp32_compress_vector(m, y, ybuffer_align, incy);
        }
    }

    sbgemv_kernel_t(m, n, alpha, a, lda, xbuffer_align, beta, ybuffer_align);

    if (incy != 1) {
        fp32_expand_vector(m, ybuffer_align, y, incy);
        ALIGN64_FREE(ybuffer);
    }

    if (incx != 1) {
        ALIGN64_FREE(xbuffer);
    }

	return(0);
}
