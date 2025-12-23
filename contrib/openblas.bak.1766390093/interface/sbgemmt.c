/*********************************************************************/
/* Copyright 2024, The OpenBLAS Project.                             */
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
/*********************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include "common.h"

#define SMP_THRESHOLD_MIN 65536.0
#define ERROR_NAME "SBGEMMT "

#ifndef GEMM_MULTITHREAD_THRESHOLD
#define GEMM_MULTITHREAD_THRESHOLD 4
#endif

#ifndef CBLAS

void NAME(char *UPLO, char *TRANSA, char *TRANSB,
	  blasint * M, blasint * K,
	  FLOAT * Alpha,
	  IFLOAT * a, blasint * ldA,
	  IFLOAT * b, blasint * ldB, FLOAT * Beta, FLOAT * c, blasint * ldC)
{

	blasint m, k;
	blasint lda, ldb, ldc;
	int transa, transb, uplo;
	blasint info;

	char transA, transB, Uplo;
	blasint nrowa, nrowb;
	IFLOAT *buffer;
	IFLOAT *aa, *bb;
	FLOAT *cc;
	FLOAT alpha, beta;

	PRINT_DEBUG_NAME;

	m = *M;
	k = *K;

	alpha = *Alpha;
	beta = *Beta;

	lda = *ldA;
	ldb = *ldB;
	ldc = *ldC;

	transA = *TRANSA;
	transB = *TRANSB;
	Uplo = *UPLO;
	TOUPPER(transA);
	TOUPPER(transB);
	TOUPPER(Uplo);

	transa = -1;
	transb = -1;
	uplo = -1;

	if (transA == 'N')
		transa = 0;
	if (transA == 'T')
		transa = 1;

	if (transA == 'R')
		transa = 0;
	if (transA == 'C')
		transa = 1;

	if (transB == 'N')
		transb = 0;
	if (transB == 'T')
		transb = 1;

	if (transB == 'R')
		transb = 0;
	if (transB == 'C')
		transb = 1;

	if (Uplo == 'U')
		uplo = 0;
	if (Uplo == 'L')
		uplo = 1;
	nrowa = m;
	if (transa & 1) nrowa = k;
	nrowb = k;
	if (transb & 1) nrowb = m;

	info = 0;

	if (ldc < MAX(1, m))
		info = 13;
	if (ldb < MAX(1, nrowb))
		info = 10;
	if (lda < MAX(1, nrowa))
		info = 8;
	if (k < 0)
		info = 5;
	if (m < 0)
		info = 4;
	if (transb < 0)
		info = 3;
	if (transa < 0)
		info = 2;
	if (uplo < 0)
		info = 1;

	if (info != 0) {
		BLASFUNC(xerbla) (ERROR_NAME, &info, sizeof(ERROR_NAME));
		return;
	}
#else

void CNAME(enum CBLAS_ORDER order, enum CBLAS_UPLO Uplo,
	   enum CBLAS_TRANSPOSE TransA, enum CBLAS_TRANSPOSE TransB, blasint m,
	   blasint k,
	   FLOAT alpha,
	   IFLOAT * A, blasint LDA,
	   IFLOAT * B, blasint LDB, FLOAT beta, FLOAT * c, blasint ldc)
{
	IFLOAT *aa, *bb;
        FLOAT *cc;

	int transa, transb, uplo;
	blasint info;
	blasint lda, ldb;
	IFLOAT *a, *b;
	XFLOAT *buffer;

	PRINT_DEBUG_CNAME;

	uplo = -1;
	transa = -1;
	transb = -1;
	info = 0;

	if (order == CblasColMajor) {
		if (Uplo == CblasUpper) uplo = 0;
		if (Uplo == CblasLower) uplo = 1;

		if (TransA == CblasNoTrans)
			transa = 0;
		if (TransA == CblasTrans)
			transa = 1;

		if (TransA == CblasConjNoTrans)
			transa = 0;
		if (TransA == CblasConjTrans)
			transa = 1;

		if (TransB == CblasNoTrans)
			transb = 0;
		if (TransB == CblasTrans)
			transb = 1;

		if (TransB == CblasConjNoTrans)
			transb = 0;
		if (TransB == CblasConjTrans)
			transb = 1;

		a = (void *)A;
		b = (void *)B;
		lda = LDA;
		ldb = LDB;

		info = -1;

		blasint nrowa;
		blasint nrowb;
		nrowa = m;
		if (transa & 1) nrowa = k;
		nrowb = k;
		if (transb & 1)  nrowb = m;

		if (ldc < MAX(1, m))
			info = 13;
		if (ldb < MAX(1, nrowb))
			info = 10;
		if (lda < MAX(1, nrowa))
			info = 8;
		if (k < 0)
			info = 5;
		if (m < 0)
			info = 4;
		if (transb < 0)
			info = 3;
		if (transa < 0)
			info = 2;
		if (uplo < 0)
			info = 1;
	}

	if (order == CblasRowMajor) {

		a = (void *)B;
		b = (void *)A;

		lda = LDB;
		ldb = LDA;

		if (Uplo == CblasUpper) uplo = 0;
		if (Uplo == CblasLower) uplo = 1;

		if (TransB == CblasNoTrans)
			transa = 0;
		if (TransB == CblasTrans)
			transa = 1;

		if (TransB == CblasConjNoTrans)
			transa = 0;
		if (TransB == CblasConjTrans)
			transa = 1;

		if (TransA == CblasNoTrans)
			transb = 0;
		if (TransA == CblasTrans)
			transb = 1;

		if (TransA == CblasConjNoTrans)
			transb = 0;
		if (TransA == CblasConjTrans)
			transb = 1;

		info = -1;

		blasint ncola; 
		blasint ncolb;

		ncola = m;
		if (transa & 1) ncola = k;
		ncolb = k;

		if (transb & 1) {
			ncolb = m;
		}

		if (ldc < MAX(1,m))
			info = 13;
		if (ldb < MAX(1, ncolb))
			info = 8;
		if (lda < MAX(1, ncola))
			info = 10;
		if (k < 0)
			info = 5;
		if (m < 0)
			info = 4;
		if (transb < 0)
			info = 2;
		if (transa < 0)
			info = 3;
		if (uplo < 0)
			info = 1;
	}

	if (info >= 0) {
		BLASFUNC(xerbla) (ERROR_NAME, &info, sizeof(ERROR_NAME));
		return;
	}

#endif
	int buffer_size;
	blasint i, j;

#ifdef SMP
	int nthreads;
#endif


#ifdef SMP
	static int (*gemv_thread[]) (BLASLONG, BLASLONG, FLOAT, IFLOAT *,
				     BLASLONG, IFLOAT *, BLASLONG, FLOAT,
				     FLOAT *, BLASLONG, int) = {
		sbgemv_thread_n, sbgemv_thread_t,
	};
#endif
	int (*gemv[]) (BLASLONG, BLASLONG, FLOAT, IFLOAT *, BLASLONG,
		       IFLOAT *, BLASLONG, FLOAT, FLOAT *, BLASLONG) = {
	SBGEMV_N, SBGEMV_T,};


	if (m == 0)
		return;

	IDEBUG_START;

	const blasint incb = ((transb & 1) == 0) ? 1 : ldb;

	if (uplo == 1) {
		for (i = 0; i < m; i++) {
			j = m - i;

			aa = a + i;
			bb = b + i * ldb;
			if (transa & 1) {
				aa = a + lda * i;
			}
			if (transb & 1)
				bb = b + i;
			cc = c + i * ldc + i;

#if 0
			if (beta != ONE)
				SCAL_K(l, 0, 0, beta, cc, 1, NULL, 0, NULL, 0);

			if (alpha == ZERO)
				continue;
#endif

			IDEBUG_START;

			buffer_size = j + k + 128 / sizeof(FLOAT);
#ifdef WINDOWS_ABI
			buffer_size += 160 / sizeof(FLOAT);
#endif
			// for alignment
			buffer_size = (buffer_size + 3) & ~3;
			STACK_ALLOC(buffer_size, IFLOAT, buffer);

#ifdef SMP

			if (1L * j * k < 2304L * GEMM_MULTITHREAD_THRESHOLD)
				nthreads = 1;
			else
				nthreads = num_cpu_avail(2);

			if (nthreads == 1) {
#endif

				if (!(transa & 1))
				(gemv[(int)transa]) (j, k, alpha, aa, lda,
						     bb, incb, beta, cc, 1);
				else
				(gemv[(int)transa]) (k, j, alpha, aa, lda,
						     bb, incb, beta, cc, 1);

#ifdef SMP
			} else {
				if (!(transa & 1))
				(gemv_thread[(int)transa]) (j, k, alpha, aa,
							    lda, bb, incb, beta, cc,
							    1, nthreads);
				else
				(gemv_thread[(int)transa]) (k, j, alpha, aa,
							    lda, bb, incb, beta, cc,
							    1, nthreads);

			}
#endif

			STACK_FREE(buffer);
		}
	} else {

		for (i = 0; i < m; i++) {
			j = i + 1;

			bb = b + i * ldb;
			if (transb & 1) {
				bb = b + i;
			}
			cc = c + i * ldc;

#if 0
			if (beta != ONE)
				SCAL_K(l, 0, 0, beta, cc, 1, NULL, 0, NULL, 0);

			if (alpha == ZERO)
				continue;
#endif
			IDEBUG_START;

			buffer_size = j + k + 128 / sizeof(FLOAT);
#ifdef WINDOWS_ABI
			buffer_size += 160 / sizeof(FLOAT);
#endif
			// for alignment
			buffer_size = (buffer_size + 3) & ~3;
			STACK_ALLOC(buffer_size, IFLOAT, buffer);

#ifdef SMP

			if (1L * j * k < 2304L * GEMM_MULTITHREAD_THRESHOLD)
				nthreads = 1;
			else
				nthreads = num_cpu_avail(2);

			if (nthreads == 1) {
#endif

				if (!(transa & 1))
				(gemv[(int)transa]) (j, k, alpha, a, lda, bb,
						     incb, beta, cc, 1);
				else
				(gemv[(int)transa]) (k, j, alpha, a, lda, bb,
						     incb, beta, cc, 1);

#ifdef SMP
			} else {
				if (!(transa & 1))
				(gemv_thread[(int)transa]) (j, k, alpha, a, lda,
							    bb, incb, beta, cc, 1,
							    nthreads);
				else
				(gemv_thread[(int)transa]) (k, j, alpha, a, lda,
							    bb, incb, beta, cc, 1,
							    nthreads);
			}
#endif

			STACK_FREE(buffer);
		}
	}

	IDEBUG_END;

	return;
}
