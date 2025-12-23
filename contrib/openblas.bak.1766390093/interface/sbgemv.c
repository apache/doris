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
#include "common.h"
#include "l1param.h"
#ifdef FUNCTION_PROFILE
#include "functable.h"
#endif

#define ERROR_NAME "SBGEMV "

#ifdef SMP
static int (*sbgemv_thread[])(BLASLONG, BLASLONG, float, bfloat16 *, BLASLONG, bfloat16 * , BLASLONG, float, float *, BLASLONG, int) = {
    sbgemv_thread_n, sbgemv_thread_t,
};
#endif

#ifndef CBLAS

void NAME(char *TRANS, blasint *M, blasint *N, float *ALPHA, bfloat16 *a, blasint *LDA, bfloat16 *x, blasint *INCX, float *BETA, float *y, blasint *INCY)
{
    char trans = *TRANS;
    blasint m = *M;
    blasint n = *N;
    blasint lda = *LDA;
    blasint incx = *INCX;
    blasint incy = *INCY;
    float alpha = *ALPHA;
    float beta  = *BETA;
#ifdef SMP
    int nthreads;
#endif

    int (*sbgemv[])(BLASLONG, BLASLONG, float, bfloat16 *, BLASLONG, bfloat16 * , BLASLONG, float, float *, BLASLONG) = {
        SBGEMV_N, SBGEMV_T,
    };

    blasint info;
    blasint lenx, leny;
    blasint i;

    PRINT_DEBUG_NAME;

    TOUPPER(trans);

    info = 0;

    i = -1;

    if (trans == 'N') {i = 0;}
    if (trans == 'T') {i = 1;}
    if (trans == 'R') {i = 0;}
    if (trans == 'C') {i = 1;}

    if (incy == 0)       {info = 11;}
    if (incx == 0)       {info = 8;}
    if (lda < MAX(1, m)) {info = 6;}
    if (n < 0)           {info = 3;}
    if (m < 0)           {info = 2;}
    if (i < 0)           {info = 1;}

    trans = i;

    if (info != 0) {
        BLASFUNC(xerbla)(ERROR_NAME, &info, sizeof(ERROR_NAME));
        return;
    }

#else

void CNAME(enum CBLAS_ORDER order, enum CBLAS_TRANSPOSE TransA, blasint m, blasint n, float alpha, bfloat16 *a, blasint lda, bfloat16 *x, blasint incx, float beta, float *y, blasint incy)
{
    blasint lenx,  leny;
    int     trans;
    blasint info,  t;
#ifdef SMP
    int     nthreads;
#endif

    int (*sbgemv[])(BLASLONG, BLASLONG, float, bfloat16 *, BLASLONG,  bfloat16 * , BLASLONG, float, float *, BLASLONG) = {
        SBGEMV_N, SBGEMV_T,
    };

    PRINT_DEBUG_CNAME;

    trans = -1;
    info  =  0;

    if (order == CblasColMajor) {   // Column Major
        if (TransA == CblasNoTrans || TransA == CblasConjNoTrans) {
            trans = 0;
        } else if (TransA == CblasTrans || TransA == CblasConjTrans) {
            trans = 1;
        }
    } else {                        // Row Major
        if (TransA == CblasNoTrans || TransA == CblasConjNoTrans) {
            trans = 1;
        } else if (TransA == CblasTrans || TransA == CblasConjTrans) {
            trans = 0;
        }

        t = n;
        n = m;
        m = t;
    }

    info = -1;

    if (incy == 0)       {info = 11;}
    if (incx == 0)       {info = 8;}
    if (lda < MAX(1, m)) {info = 6;}
    if (n < 0)           {info = 3;}
    if (m < 0)           {info = 2;}
    if (trans < 0)       {info = 1;}

    if (info >= 0) {
        BLASFUNC(xerbla)(ERROR_NAME, &info, sizeof(ERROR_NAME));
        return;
    }

#endif

    if ((m==0) || (n==0)) return;

    if (trans) {
        lenx = m;
        leny = n;
    } else {
        lenx = n;
        leny = m;
    }

    if (alpha == ZERO) {
        if (beta != ONE) SCAL_K(leny, 0, 0, beta, y, blasabs(incy), NULL, 0, NULL, 0);
        return;
    }

    IDEBUG_START;
    FUNCTION_PROFILE_START();

    if (incx < 0) {x -= (lenx - 1) * incx;}
    if (incy < 0) {y -= (leny - 1) * incy;}

#ifdef SMP
    if ( 1L * m * n < 115200L * GEMM_MULTITHREAD_THRESHOLD )
      nthreads = 1;
    else
      nthreads = num_cpu_avail(2);

    if (nthreads == 1) {
#endif
        (sbgemv[(int)trans])(m, n, alpha, a, lda, x, incx, beta, y, incy);
#ifdef SMP
    } else {
        (sbgemv_thread[(int)trans])(m, n, alpha, a, lda, x, incx, beta, y, incy, nthreads);
    }
#endif

    FUNCTION_PROFILE_END(1, m * n + m + n,  2 * m * n);
    IDEBUG_END;

    return;
}
