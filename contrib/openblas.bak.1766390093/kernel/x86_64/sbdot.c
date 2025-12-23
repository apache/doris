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

#if defined(COOPERLAKE) || defined(SAPPHIRERAPIDS)
#include "sbdot_microk_cooperlake.c"
#endif

static float sbdot_compute(BLASLONG n, bfloat16 *x, BLASLONG inc_x, bfloat16 *y, BLASLONG inc_y)
{
    float d = 0.0;

#ifdef HAVE_SBDOT_ACCL_KERNEL
    if ((inc_x == 1) && (inc_y == 1)) {
        return sbdot_accl_kernel(n, x, y);
    }
#endif

    float * x_fp32 = malloc(sizeof(float)*n);
    float * y_fp32 = malloc(sizeof(float)*n);

    SBF16TOS_K(n, x, inc_x, x_fp32, 1);
    SBF16TOS_K(n, y, inc_y, y_fp32, 1);

    d = SDOTU_K(n, x_fp32, 1, y_fp32, 1);

    free(x_fp32);
    free(y_fp32);

    return d;
}

#if defined(SMP)
static int sbdot_thread_func(BLASLONG n, BLASLONG dummy0, BLASLONG dummy1, bfloat16 dummy2,
		            bfloat16 *x, BLASLONG inc_x, bfloat16 *y, BLASLONG inc_y,
			    float *result, BLASLONG dummy3)
{
    *(float *)result = sbdot_compute(n, x, inc_x, y, inc_y);
    return 0;
}

extern int blas_level1_thread_with_return_value(int mode, BLASLONG m, BLASLONG n, BLASLONG k, void *alpha,
                            void *a, BLASLONG lda, void *b, BLASLONG ldb, void *c, BLASLONG ldc,
			    int (*function)(), int nthreads);
#endif

float CNAME(BLASLONG n, bfloat16 *x, BLASLONG inc_x, bfloat16 *y, BLASLONG inc_y)
{
    float dot_result = 0.0;

    if (n <= 0)  return 0.0;

#if defined(SMP)
    int nthreads;
    int thread_thres = 40960;
    bfloat16 dummy_alpha;
#endif

#if defined(SMP)
    if (inc_x == 0 || inc_y == 0 || n <= thread_thres)
        nthreads = 1;
    else
        nthreads = num_cpu_avail(1);

    int best_threads = (int) (n/(float)thread_thres + 0.5);

    if (best_threads < nthreads) {
        nthreads = best_threads;
    }

    if (nthreads <= 1) {
        dot_result = sbdot_compute(n, x, inc_x, y, inc_y);
    } else {
        char thread_result[MAX_CPU_NUMBER * sizeof(double) * 2];
        int mode = BLAS_BFLOAT16 | BLAS_REAL;
        blas_level1_thread_with_return_value(mode, n, 0, 0, &dummy_alpha,
                                             x, inc_x, y, inc_y, thread_result, 0,
                                             (void *)sbdot_thread_func, nthreads);
        float * ptr = (float *)thread_result;
        for (int i = 0; i < nthreads; i++) {
            dot_result += (*ptr);
            ptr = (float *)(((char *)ptr) + sizeof(double) * 2);
        }
    }
#else
    dot_result = sbdot_compute(n, x, inc_x, y, inc_y);
#endif

    return dot_result;
}
