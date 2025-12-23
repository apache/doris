/*********************************************************************/
/* Copyright 2022, The OpenBLAS Project.                             */
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

#ifndef COMPLEX
#define SMP_THRESHOLD_MIN 65536.0
#ifdef RNAME
#ifdef XDOUBLE
#define ERROR_NAME "QGEMMTR"
#elif defined(DOUBLE)
#define ERROR_NAME "DGEMMTR"
#elif defined(BFLOAT16)
#define ERROR_NAME "SBGEMMTR"
#else
#define ERROR_NAME "SGEMMTR"
#endif
#else
#ifdef XDOUBLE
#define ERROR_NAME "QGEMMT "
#elif defined(DOUBLE)
#define ERROR_NAME "DGEMMT "
#elif defined(BFLOAT16)
#define ERROR_NAME "SBGEMMT "
#else
#define ERROR_NAME "SGEMMT "
#endif
#endif
#else
#define SMP_THRESHOLD_MIN 8192.0
#ifdef RNAME
#ifdef XDOUBLE
#define ERROR_NAME "XGEMMTR"
#elif defined(DOUBLE)
#define ERROR_NAME "ZGEMMTR"
#else
#define ERROR_NAME "CGEMMTR"
#endif
#else
#ifdef XDOUBLE
#define ERROR_NAME "XGEMMT "
#elif defined(DOUBLE)
#define ERROR_NAME "ZGEMMT "
#else
#define ERROR_NAME "CGEMMT "
#endif
#endif
#endif

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
#if defined(COMPLEX)
	blasint ncolb;
#endif
	IFLOAT *buffer;
	IFLOAT *aa, *bb;
	FLOAT *cc;
#if defined(COMPLEX)
	FLOAT alpha_r, alpha_i, beta_r, beta_i;
#else
	FLOAT alpha, beta;
#endif

	PRINT_DEBUG_NAME;

	m = *M;
	k = *K;

#if defined(COMPLEX)
	FLOAT *alpha = Alpha;
	alpha_r = *(Alpha + 0);
	alpha_i = *(Alpha + 1);

	beta_r = *(Beta + 0);
	beta_i = *(Beta + 1);
#else
	alpha = *Alpha;
	beta = *Beta;
#endif

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
#ifndef COMPLEX
	if (transA == 'R')
		transa = 0;
	if (transA == 'C')
		transa = 1;
#else
	if (transA == 'R')
		transa = 2;
	if (transA == 'C')
		transa = 3;
#endif

	if (transB == 'N')
		transb = 0;
	if (transB == 'T')
		transb = 1;
#ifndef COMPLEX
	if (transB == 'R')
		transb = 0;
	if (transB == 'C')
		transb = 1;
#else
	if (transB == 'R')
		transb = 2;
	if (transB == 'C')
		transb = 3;
#endif

	if (Uplo == 'U')
		uplo = 0;
	if (Uplo == 'L')
		uplo = 1;
	
	nrowa = m;
	if (transa & 1) nrowa = k;
	nrowb = k;
#if defined(COMPLEX)
	ncolb = m;
#endif
	if (transb & 1) {
		nrowb = m;
#if defined(COMPLEX)
		ncolb = k;
#endif
	}

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
#ifndef COMPLEX
	   FLOAT alpha,
	   IFLOAT * A, blasint LDA,
	   IFLOAT * B, blasint LDB, FLOAT beta, FLOAT * c, blasint ldc)
{
#else
	   void *valpha,
	   void *va, blasint LDA,
	   void *vb, blasint LDB, void *vbeta, void *vc, blasint ldc)
{
	FLOAT *alpha = (FLOAT *) valpha;
	FLOAT *beta = (FLOAT *) vbeta;
	FLOAT *A = (FLOAT *) va;
	FLOAT *B = (FLOAT *) vb;
	FLOAT *c = (FLOAT *) vc;
#endif
	FLOAT *aa, *bb, *cc;

	int transa, transb, uplo;
	blasint info;
	blasint lda, ldb;
	FLOAT *a, *b;
#if defined(COMPLEX)
	blasint nrowb, ncolb;
#endif
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
#ifndef COMPLEX
		if (TransA == CblasConjNoTrans)
			transa = 0;
		if (TransA == CblasConjTrans)
			transa = 1;
#else
		if (TransA == CblasConjNoTrans)
			transa = 2;
		if (TransA == CblasConjTrans)
			transa = 3;
#endif
		if (TransB == CblasNoTrans)
			transb = 0;
		if (TransB == CblasTrans)
			transb = 1;
#ifndef COMPLEX
		if (TransB == CblasConjNoTrans)
			transb = 0;
		if (TransB == CblasConjTrans)
			transb = 1;
#else
		if (TransB == CblasConjNoTrans)
			transb = 2;
		if (TransB == CblasConjTrans)
			transb = 3;
#endif

		a = (void *)A;
		b = (void *)B;
		lda = LDA;
		ldb = LDB;

		info = -1;

		blasint nrowa;
#if !defined(COMPLEX)
		blasint nrowb;
#endif
		nrowa = m;
		if (transa & 1) nrowa = k;
		nrowb = k;
#if defined(COMPLEX)
		ncolb = m;
#endif
		if (transb & 1) {
			nrowb = m;
#if defined(COMPLEX)
			ncolb = k;
#endif
		}

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

		if (Uplo == CblasUpper) uplo = 1;
		if (Uplo == CblasLower) uplo = 0;

		if (TransB == CblasNoTrans)
			transa = 0;
		if (TransB == CblasTrans)
			transa = 1;
#ifndef COMPLEX
		if (TransB == CblasConjNoTrans)
			transa = 0;
		if (TransB == CblasConjTrans)
			transa = 1;
#else
		if (TransB == CblasConjNoTrans)
			transa = 2;
		if (TransB == CblasConjTrans)
			transa = 3;
#endif
		if (TransA == CblasNoTrans)
			transb = 0;
		if (TransA == CblasTrans)
			transb = 1;
#ifndef COMPLEX
		if (TransA == CblasConjNoTrans)
			transb = 0;
		if (TransA == CblasConjTrans)
			transb = 1;
#else
		if (TransA == CblasConjNoTrans)
			transb = 2;
		if (TransA == CblasConjTrans)
			transb = 3;
#endif

		info = -1;

		blasint ncola; 
#if !defined(COMPLEX)
		blasint ncolb;
#endif
		ncola = m;
		if (transa & 1) ncola = k;
		ncolb = k;
#if defined(COMPLEX)
		nrowb = m;
#endif

		if (transb & 1) {
#if defined(COMPLEX)
			nrowb = k;
#endif
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
#if defined(COMPLEX)
	FLOAT alpha_r = *(alpha + 0);
	FLOAT alpha_i = *(alpha + 1);

	FLOAT beta_r = *(beta + 0);
	FLOAT beta_i = *(beta + 1);
#endif

#endif
	int buffer_size;
	blasint l;
	blasint i, j;

#ifdef SMP
	int nthreads;
#endif

#if defined(COMPLEX)

#ifdef SMP
	static int (*gemv_thread[]) (BLASLONG, BLASLONG, FLOAT *, FLOAT *,
				     BLASLONG, FLOAT *, BLASLONG, FLOAT *,
				     BLASLONG, FLOAT *, int) = {
#ifdef XDOUBLE
		xgemv_thread_n, xgemv_thread_t, xgemv_thread_r, xgemv_thread_c,
		    xgemv_thread_o, xgemv_thread_u, xgemv_thread_s,
		    xgemv_thread_d,
#elif defined DOUBLE
		zgemv_thread_n, zgemv_thread_t, zgemv_thread_r, zgemv_thread_c,
		    zgemv_thread_o, zgemv_thread_u, zgemv_thread_s,
		    zgemv_thread_d,
#else
		cgemv_thread_n, cgemv_thread_t, cgemv_thread_r, cgemv_thread_c,
		    cgemv_thread_o, cgemv_thread_u, cgemv_thread_s,
		    cgemv_thread_d,
#endif
	};
#endif

	int (*gemv[]) (BLASLONG, BLASLONG, BLASLONG, FLOAT, FLOAT, FLOAT *,
		       BLASLONG, FLOAT *, BLASLONG, FLOAT *, BLASLONG,
		       FLOAT *) = {
	GEMV_N, GEMV_T, GEMV_R, GEMV_C, GEMV_O, GEMV_U, GEMV_S, GEMV_D,};

#else

#ifdef SMP
	static int (*gemv_thread[]) (BLASLONG, BLASLONG, FLOAT, FLOAT *,
				     BLASLONG, FLOAT *, BLASLONG, FLOAT *,
				     BLASLONG, FLOAT *, int) = {
#ifdef XDOUBLE
		qgemv_thread_n, qgemv_thread_t,
#elif defined DOUBLE
		dgemv_thread_n, dgemv_thread_t,
#else
		sgemv_thread_n, sgemv_thread_t,
#endif
	};
#endif
	int (*gemv[]) (BLASLONG, BLASLONG, BLASLONG, FLOAT, FLOAT *, BLASLONG,
		       FLOAT *, BLASLONG, FLOAT *, BLASLONG, FLOAT *) = {
	GEMV_N, GEMV_T,};

#endif

	if (m == 0)
		return;

	IDEBUG_START;

#if defined(COMPLEX)
	if (transb > 1){
#ifndef CBLAS
		IMATCOPY_K_CNC(nrowb, ncolb, (FLOAT)(1.0), (FLOAT)(0.0), b, ldb);
#else
		if (order == CblasColMajor)
			IMATCOPY_K_CNC(nrowb, ncolb, (FLOAT)(1.0), (FLOAT)(0.0), b, ldb);
		if (order == CblasRowMajor)
			IMATCOPY_K_RNC(nrowb, ncolb, (FLOAT)(1.0), (FLOAT)(0.0), b, ldb);
#endif
	}
#endif

	const blasint incb = ((transb & 1) == 0) ? 1 : ldb;

	if (uplo == 1) {
		for (i = 0; i < m; i++) {
			j = m - i;

			l = j;
#if defined(COMPLEX)
			aa = a + i * 2;
			bb = b + i * ldb * 2;
			if (transa & 1) {
				aa = a + lda * i * 2;
			}
			if (transb & 1)
				bb = b + i * 2;
			cc = c + i * 2 * ldc + i * 2;
#else
			aa = a + i;
			bb = b + i * ldb;
			if (transa & 1) {
				aa = a + lda * i;
			}
			if (transb & 1)
				bb = b + i;
			cc = c + i * ldc + i;
#endif

#if defined(COMPLEX)
			if (beta_r != ONE || beta_i != ZERO)
				SCAL_K(l, 0, 0, beta_r, beta_i, cc, 1, NULL, 0,
				       NULL, 0);

			if (alpha_r == ZERO && alpha_i == ZERO)
				continue;
#else
			if (beta != ONE)
				SCAL_K(l, 0, 0, beta, cc, 1, NULL, 0, NULL, 0);

			if (alpha == ZERO)
				continue;
#endif

			IDEBUG_START;

			buffer_size = 2 * (j + k) + 128 / sizeof(FLOAT);
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

#if defined(COMPLEX)
				if (!(transa & 1))
				(gemv[(int)transa]) (j, k, 0, alpha_r, alpha_i,
						     aa, lda, bb, incb, cc, 1,
						     buffer);
				else
				(gemv[(int)transa]) (k, j, 0, alpha_r, alpha_i,
						     aa, lda, bb, incb, cc, 1,
						     buffer);
#else
				if (!(transa & 1))
				(gemv[(int)transa]) (j, k, 0, alpha, aa, lda,
						     bb, incb, cc, 1, buffer);
				else
				(gemv[(int)transa]) (k, j, 0, alpha, aa, lda,
						     bb, incb, cc, 1, buffer);
#endif
#ifdef SMP
			} else {
				if (!(transa & 1))
				(gemv_thread[(int)transa]) (j, k, alpha, aa,
							    lda, bb, incb, cc,
							    1, buffer,
							    nthreads);
				else
				(gemv_thread[(int)transa]) (k, j, alpha, aa,
							    lda, bb, incb, cc,
							    1, buffer,
							    nthreads);

			}
#endif

			STACK_FREE(buffer);
		}
	} else {

		for (i = 0; i < m; i++) {
			j = i + 1;

			l = j;
#if defined COMPLEX
			bb = b + i * ldb * 2;
			if (transb & 1) {
				bb = b + i * 2;
			}
			cc = c + i * 2 * ldc;
#else
			bb = b + i * ldb;
			if (transb & 1) {
				bb = b + i;
			}
			cc = c + i * ldc;
#endif

#if defined(COMPLEX)
			if (beta_r != ONE || beta_i != ZERO)
				SCAL_K(l, 0, 0, beta_r, beta_i, cc, 1, NULL, 0,
				       NULL, 0);

			if (alpha_r == ZERO && alpha_i == ZERO)
				continue;
#else
			if (beta != ONE)
				SCAL_K(l, 0, 0, beta, cc, 1, NULL, 0, NULL, 0);

			if (alpha == ZERO)
				continue;
#endif
			IDEBUG_START;

			buffer_size = 2 * (j + k) + 128 / sizeof(FLOAT);
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

#if defined(COMPLEX)
				if (!(transa & 1))
				(gemv[(int)transa]) (j, k, 0, alpha_r, alpha_i,
						     a, lda, bb, incb, cc, 1,
						     buffer);
				else
				(gemv[(int)transa]) (k, j, 0, alpha_r, alpha_i,
						     a, lda, bb, incb, cc, 1,
						     buffer);
#else
				if (!(transa & 1))
				(gemv[(int)transa]) (j, k, 0, alpha, a, lda, bb,
						     incb, cc, 1, buffer);
				else
				(gemv[(int)transa]) (k, j, 0, alpha, a, lda, bb,
						     incb, cc, 1, buffer);
#endif

#ifdef SMP
			} else {
				if (!(transa & 1))
				(gemv_thread[(int)transa]) (j, k, alpha, a, lda,
							    bb, incb, cc, 1,
							    buffer, nthreads);
				else
				(gemv_thread[(int)transa]) (k, j, alpha, a, lda,
							    bb, incb, cc, 1,
							    buffer, nthreads);
			}
#endif

			STACK_FREE(buffer);
		}
	}

	IDEBUG_END;

/* transform B back if necessary */
#if defined(COMPLEX)
	if (transb > 1){
#ifndef CBLAS
		IMATCOPY_K_CNC(nrowb, ncolb, (FLOAT)(1.0), (FLOAT)(0.0), b, ldb);
#else
		if (order == CblasColMajor)
			IMATCOPY_K_CNC(nrowb, ncolb, (FLOAT)(1.0), (FLOAT)(0.0), b, ldb);
		if (order == CblasRowMajor)
			IMATCOPY_K_RNC(nrowb, ncolb, (FLOAT)(1.0), (FLOAT)(0.0), b, ldb);
#endif
	}
#endif

	return;
}
