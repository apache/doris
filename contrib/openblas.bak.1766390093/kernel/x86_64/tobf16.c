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

#include <stddef.h>
#include "common.h"

#if defined(DOUBLE)
#define FLOAT_TYPE double
#elif defined(SINGLE)
#define FLOAT_TYPE float
#else
#endif

#if defined(COOPERLAKE) || defined(SAPPHIRERAPIDS)
#if defined(DOUBLE)
#include "dtobf16_microk_cooperlake.c"
#elif defined(SINGLE)
#include "stobf16_microk_cooperlake.c"
#endif
#endif

/* Notes for algorithm:
 * - Round to Nearest Even used generally
 * - QNAN for NAN case
 * - Input denormals are treated as zero
 */
static void tobf16_generic_kernel(BLASLONG n, const FLOAT_TYPE * in, BLASLONG inc_in, bfloat16 * out, BLASLONG inc_out)
{
    BLASLONG register index_in  = 0;
    BLASLONG register index_out = 0;
    BLASLONG register index     = 0;
    float             float_in  = 0.0;
    uint32_t *        uint32_in = (uint32_t *)(&float_in);
    uint16_t *        uint16_in = (uint16_t *)(&float_in);

    while(index<n) {
#if defined(DOUBLE)
        float_in = (float)(*(in+index_in));
#else
        float_in = *(in+index_in);
#endif

        switch((*uint32_in) & 0xff800000u) {
            case (0x00000000u):   /* Type 1: Positive denormal */
                *(out+index_out) = 0x0000u;
                break;
            case (0x80000000u):   /* Type 2: Negative denormal */
                *(out+index_out) = 0x8000u;
                break;
            case (0x7f800000u):   /* Type 3: Positive infinity or NAN */
            case (0xff800000u):   /* Type 4: Negative infinity or NAN */
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
                *(out+index_out) = uint16_in[1];
#else
                *(out+index_out) = uint16_in[0];
#endif
                /* Specific for NAN */
                if (((*uint32_in) & 0x007fffffu) != 0) {
                    /* Force to be QNAN */
                    *(out+index_out) |= 0x0040u;
                }
                break;
            default:              /* Type 5: Normal case */
                (*uint32_in) += ((((*uint32_in) >> 16) & 0x1u) + 0x7fffu);
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
                *(out+index_out) = uint16_in[1];
#else
                *(out+index_out) = uint16_in[0];
#endif
                break;
        }

        index_in  += inc_in;
        index_out += inc_out;
        index++;
    }
}

#ifndef HAVE_TOBF16_ACCL_KERNEL
static void tobf16_accl_kernel(BLASLONG n, const FLOAT_TYPE * in, bfloat16 * out)
{
    tobf16_generic_kernel(n, in, 1, out, 1);
}
#endif

static void tobf16_compute(BLASLONG n, FLOAT_TYPE * in, BLASLONG inc_in, bfloat16 * out, BLASLONG inc_out)
{
    if ((inc_in == 1) && (inc_out == 1)) {
        tobf16_accl_kernel(n, in, out);
    } else {
        tobf16_generic_kernel(n, in, inc_in, out, inc_out);
    }
}

#if defined(SMP)
static int tobf16_thread_func(BLASLONG n, BLASLONG dummy0, BLASLONG dummy1, FLOAT_TYPE dummy2,
                            FLOAT_TYPE *x, BLASLONG inc_x, bfloat16 *y, BLASLONG inc_y,
                            FLOAT_TYPE *dummy3, BLASLONG dummy4)
{
        tobf16_compute(n, x, inc_x, y, inc_y);
        return 0;
}

extern int blas_level1_thread(int mode, BLASLONG m, BLASLONG n, BLASLONG k, void *alpha,
                              void *a, BLASLONG lda, void *b, BLASLONG ldb, void *c, BLASLONG ldc,
                              int (*function)(), int nthreads);
#endif

void CNAME(BLASLONG n, FLOAT_TYPE * in, BLASLONG inc_in, bfloat16 * out, BLASLONG inc_out)
{
    if (n <= 0)  return;

#if defined(SMP)
    int nthreads;
    FLOAT_TYPE dummy_alpha;
    FLOAT_TYPE dummy_c;
#endif

#if defined(SMP)
    if (inc_in == 0 || inc_out == 0 || n <= 100000) {
        nthreads = 1;
    } else {
        nthreads = num_cpu_avail(1);
        if (n/100000 < 100) {
            nthreads = MAX(nthreads,4);
//        } else {
//            nthreads = MAX(nthreads,16);
        }
    }

    if (nthreads == 1) {
        tobf16_compute(n, in, inc_in, out, inc_out);
    } else {
#if defined(DOUBLE)
        int mode = BLAS_REAL | BLAS_DTOBF16;
#elif defined(SINGLE)
        int mode = BLAS_REAL | BLAS_STOBF16;
#endif
        blas_level1_thread(mode, n, 0, 0, &dummy_alpha,
                           in, inc_in, out, inc_out, &dummy_c, 0,
                           (void *)tobf16_thread_func, nthreads);
    }
#else
    tobf16_compute(n, in, inc_in, out, inc_out);
#endif

}
