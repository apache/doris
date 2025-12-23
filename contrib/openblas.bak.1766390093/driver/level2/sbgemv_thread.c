/*********************************************************************/
/* Copyright 2009, 2010 The University of Texas at Austin.           */
/* All rights reserved.                                              */
/*                                                                   */
/* Redistribution and use in source and binary forms, with or        */
/* without modification, are permitted provided that the following   */
/* conditions are met:                                               */
/*                                                                   */
/*   1. Redistributions of source code must retain the above         */
/*      copyright notice, this list of conditions and the following  */
/*      disclaimer.                                                  */
/*                                                                   */
/*   2. Redistributions in binary form must reproduce the above      */
/*      copyright notice, this list of conditions and the following  */
/*      disclaimer in the documentation and/or other materials       */
/*      provided with the distribution.                              */
/*                                                                   */
/*    THIS  SOFTWARE IS PROVIDED  BY THE  UNIVERSITY OF  TEXAS AT    */
/*    AUSTIN  ``AS IS''  AND ANY  EXPRESS OR  IMPLIED WARRANTIES,    */
/*    INCLUDING, BUT  NOT LIMITED  TO, THE IMPLIED  WARRANTIES OF    */
/*    MERCHANTABILITY  AND FITNESS FOR  A PARTICULAR  PURPOSE ARE    */
/*    DISCLAIMED.  IN  NO EVENT SHALL THE UNIVERSITY  OF TEXAS AT    */
/*    AUSTIN OR CONTRIBUTORS BE  LIABLE FOR ANY DIRECT, INDIRECT,    */
/*    INCIDENTAL,  SPECIAL, EXEMPLARY,  OR  CONSEQUENTIAL DAMAGES    */
/*    (INCLUDING, BUT  NOT LIMITED TO,  PROCUREMENT OF SUBSTITUTE    */
/*    GOODS  OR  SERVICES; LOSS  OF  USE,  DATA,  OR PROFITS;  OR    */
/*    BUSINESS INTERRUPTION) HOWEVER CAUSED  AND ON ANY THEORY OF    */
/*    LIABILITY, WHETHER  IN CONTRACT, STRICT  LIABILITY, OR TORT    */
/*    (INCLUDING NEGLIGENCE OR OTHERWISE)  ARISING IN ANY WAY OUT    */
/*    OF  THE  USE OF  THIS  SOFTWARE,  EVEN  IF ADVISED  OF  THE    */
/*    POSSIBILITY OF SUCH DAMAGE.                                    */
/*                                                                   */
/* The views and conclusions contained in the software and           */
/* documentation are those of the authors and should not be          */
/* interpreted as representing official policies, either expressed   */
/* or implied, of The University of Texas at Austin.                 */
/*********************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include "common.h"

#ifndef TRANSA
#define SBGEMV	SBGEMV_N
#else
#define SBGEMV	SBGEMV_T
#endif

static int sbgemv_kernel(blas_arg_t *args, BLASLONG *range_m, BLASLONG *range_n, FLOAT *dummy1, FLOAT *dummy2, BLASLONG dummy3){

    bfloat16 *a, *x;
    float    *y;
    BLASLONG lda, incx, incy;
    BLASLONG m_from, m_to, n_from, n_to;

    a = (bfloat16 *)args->a;
    x = (bfloat16 *)args->b;
    y = (float *)args->c;

    lda  = args->lda;
    incx = args->ldb;
    incy = args->ldc;
    
#ifndef TRANSA          // N
    m_from = *(range_m + 0);
    m_to   = *(range_m + 1);
    n_from = 0;
    n_to   = args -> n;
    a += m_from;
    y += m_from * incy;
#else                   // T
    m_from = 0;
    m_to   = args->m;
    n_from = *(range_n + 0);
    n_to   = *(range_n + 1);
    a += n_from * lda;
    y += n_from * incy;
#endif

    SBGEMV(m_to - m_from, n_to - n_from, *((FLOAT *)(args->alpha)), a, lda, x, incx, *((FLOAT *)(args->beta)), y, incy);

    return 0;
}

int CNAME(BLASLONG m, BLASLONG n, float alpha, bfloat16 *a, BLASLONG lda, bfloat16 *x, BLASLONG incx, float beta, float *y, BLASLONG incy, int threads)
{
    blas_arg_t args;
    blas_queue_t queue[MAX_CPU_NUMBER];
    BLASLONG range[MAX_CPU_NUMBER + 1];

#ifndef TRANSA
    BLASLONG width_for_split = m;
#else
    BLASLONG width_for_split = n;
#endif

    BLASLONG BLOCK_WIDTH = width_for_split/threads;

    int mode  =  BLAS_BFLOAT16  | BLAS_REAL;

    args.m     = m;
    args.n     = n;
    args.a     = (void *)a;
    args.b     = (void *)x;
    args.c     = (void *)y;
    args.lda   = lda;
    args.ldb   = incx;
    args.ldc   = incy;
    args.alpha = (void *)&alpha;
    args.beta  = (void *)&beta;

    range[0] = 0;

    int thread_idx;

    for (thread_idx=0; thread_idx<threads; thread_idx++) {
        if (thread_idx != threads-1) {
            range[thread_idx + 1] = range[thread_idx] + BLOCK_WIDTH;
        } else {
            range[thread_idx + 1] = range[thread_idx] + width_for_split;
        }

        queue[thread_idx].mode    = mode;
        queue[thread_idx].routine = sbgemv_kernel;
        queue[thread_idx].args    = &args;
#ifndef TRANSA
        queue[thread_idx].range_m = &range[thread_idx];
        queue[thread_idx].range_n = NULL;
#else
        queue[thread_idx].range_m = NULL;
        queue[thread_idx].range_n = &range[thread_idx];
#endif
        queue[thread_idx].sa      = NULL;
        queue[thread_idx].sb      = NULL;
        queue[thread_idx].next    = &queue[thread_idx + 1];

        width_for_split -= BLOCK_WIDTH;
    }

    if (thread_idx) {
        queue[0].sa = NULL;
        queue[0].sb = NULL;
        queue[thread_idx - 1].next = NULL;

        exec_blas(thread_idx, queue);
    }

    return 0;
}
