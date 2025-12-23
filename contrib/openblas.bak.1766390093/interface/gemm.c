/*********************************************************************/
/* Copyright 2024, 2025 The OpenBLAS Project                         */
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
#include <stdbool.h>
#include "common.h"
#ifdef FUNCTION_PROFILE
#include "functable.h"
#endif

#ifndef COMPLEX
#define SMP_THRESHOLD_MIN 65536.0
#ifdef XDOUBLE
#define ERROR_NAME "QGEMM "
#define GEMV BLASFUNC(qgemv)
#elif defined(DOUBLE)
#define ERROR_NAME "DGEMM "
#define GEMV BLASFUNC(dgemv)
#elif defined(BFLOAT16)
#define ERROR_NAME "SBGEMM "
#define GEMV BLASFUNC(sbgemv)
#else
#define ERROR_NAME "SGEMM "
#define GEMV BLASFUNC(sgemv)
#endif
#else
#define SMP_THRESHOLD_MIN 8192.0
#ifndef GEMM3M
#ifdef XDOUBLE
#define ERROR_NAME "XGEMM "
#elif defined(DOUBLE)
#define ERROR_NAME "ZGEMM "
#else
#define ERROR_NAME "CGEMM "
#endif
#else
#ifdef XDOUBLE
#define ERROR_NAME "XGEMM3M "
#elif defined(DOUBLE)
#define ERROR_NAME "ZGEMM3M "
#else
#define ERROR_NAME "CGEMM3M "
#endif
#endif
#endif

#ifndef GEMM_MULTITHREAD_THRESHOLD
#define GEMM_MULTITHREAD_THRESHOLD 4
#endif

static int (*gemm[])(blas_arg_t *, BLASLONG *, BLASLONG *, IFLOAT *, IFLOAT *, BLASLONG) = {
#if !defined(GEMM3M) || defined(GENERIC)
  GEMM_NN, GEMM_TN, GEMM_RN, GEMM_CN,
  GEMM_NT, GEMM_TT, GEMM_RT, GEMM_CT,
  GEMM_NR, GEMM_TR, GEMM_RR, GEMM_CR,
  GEMM_NC, GEMM_TC, GEMM_RC, GEMM_CC,
#if defined(SMP) && !defined(USE_SIMPLE_THREADED_LEVEL3)
  GEMM_THREAD_NN, GEMM_THREAD_TN, GEMM_THREAD_RN, GEMM_THREAD_CN,
  GEMM_THREAD_NT, GEMM_THREAD_TT, GEMM_THREAD_RT, GEMM_THREAD_CT,
  GEMM_THREAD_NR, GEMM_THREAD_TR, GEMM_THREAD_RR, GEMM_THREAD_CR,
  GEMM_THREAD_NC, GEMM_THREAD_TC, GEMM_THREAD_RC, GEMM_THREAD_CC,
#endif
#else
  GEMM3M_NN, GEMM3M_TN, GEMM3M_RN, GEMM3M_CN,
  GEMM3M_NT, GEMM3M_TT, GEMM3M_RT, GEMM3M_CT,
  GEMM3M_NR, GEMM3M_TR, GEMM3M_RR, GEMM3M_CR,
  GEMM3M_NC, GEMM3M_TC, GEMM3M_RC, GEMM3M_CC,
#if defined(SMP) && !defined(USE_SIMPLE_THREADED_LEVEL3)
  GEMM3M_THREAD_NN, GEMM3M_THREAD_TN, GEMM3M_THREAD_RN, GEMM3M_THREAD_CN,
  GEMM3M_THREAD_NT, GEMM3M_THREAD_TT, GEMM3M_THREAD_RT, GEMM3M_THREAD_CT,
  GEMM3M_THREAD_NR, GEMM3M_THREAD_TR, GEMM3M_THREAD_RR, GEMM3M_THREAD_CR,
  GEMM3M_THREAD_NC, GEMM3M_THREAD_TC, GEMM3M_THREAD_RC, GEMM3M_THREAD_CC,
#endif
#endif
};

#if defined(SMALL_MATRIX_OPT) && !defined(GEMM3M) && !defined(XDOUBLE)
#define USE_SMALL_MATRIX_OPT 1
#else
#define USE_SMALL_MATRIX_OPT 0
#endif

#if USE_SMALL_MATRIX_OPT
#ifndef DYNAMIC_ARCH
#define SMALL_KERNEL_ADDR(table, idx) ((void *)(table[idx]))
#else
#define SMALL_KERNEL_ADDR(table, idx) ((void *)(*(uintptr_t *)((char *)gotoblas + (size_t)(table[idx]))))
#endif


#ifndef COMPLEX
static size_t gemm_small_kernel[] = {
	GEMM_SMALL_KERNEL_NN, GEMM_SMALL_KERNEL_TN, 0, 0,
	GEMM_SMALL_KERNEL_NT, GEMM_SMALL_KERNEL_TT, 0, 0,
};


static size_t gemm_small_kernel_b0[] = {
	GEMM_SMALL_KERNEL_B0_NN, GEMM_SMALL_KERNEL_B0_TN, 0, 0,
	GEMM_SMALL_KERNEL_B0_NT, GEMM_SMALL_KERNEL_B0_TT, 0, 0,
};

#define GEMM_SMALL_KERNEL_B0(idx) (int (*)(BLASLONG, BLASLONG, BLASLONG, IFLOAT *, BLASLONG, FLOAT, IFLOAT *, BLASLONG, FLOAT *, BLASLONG)) SMALL_KERNEL_ADDR(gemm_small_kernel_b0, (idx))
#define GEMM_SMALL_KERNEL(idx) (int (*)(BLASLONG, BLASLONG, BLASLONG, IFLOAT *, BLASLONG, FLOAT, IFLOAT *, BLASLONG, FLOAT, FLOAT *, BLASLONG)) SMALL_KERNEL_ADDR(gemm_small_kernel, (idx))
#else

static size_t zgemm_small_kernel[] = {
	GEMM_SMALL_KERNEL_NN, GEMM_SMALL_KERNEL_TN, GEMM_SMALL_KERNEL_RN, GEMM_SMALL_KERNEL_CN,
	GEMM_SMALL_KERNEL_NT, GEMM_SMALL_KERNEL_TT, GEMM_SMALL_KERNEL_RT, GEMM_SMALL_KERNEL_CT,
	GEMM_SMALL_KERNEL_NR, GEMM_SMALL_KERNEL_TR, GEMM_SMALL_KERNEL_RR, GEMM_SMALL_KERNEL_CR,
	GEMM_SMALL_KERNEL_NC, GEMM_SMALL_KERNEL_TC, GEMM_SMALL_KERNEL_RC, GEMM_SMALL_KERNEL_CC,
};

static size_t zgemm_small_kernel_b0[] = {
	GEMM_SMALL_KERNEL_B0_NN, GEMM_SMALL_KERNEL_B0_TN, GEMM_SMALL_KERNEL_B0_RN, GEMM_SMALL_KERNEL_B0_CN,
	GEMM_SMALL_KERNEL_B0_NT, GEMM_SMALL_KERNEL_B0_TT, GEMM_SMALL_KERNEL_B0_RT, GEMM_SMALL_KERNEL_B0_CT,
	GEMM_SMALL_KERNEL_B0_NR, GEMM_SMALL_KERNEL_B0_TR, GEMM_SMALL_KERNEL_B0_RR, GEMM_SMALL_KERNEL_B0_CR,
	GEMM_SMALL_KERNEL_B0_NC, GEMM_SMALL_KERNEL_B0_TC, GEMM_SMALL_KERNEL_B0_RC, GEMM_SMALL_KERNEL_B0_CC,
};

#define ZGEMM_SMALL_KERNEL(idx) (int (*)(BLASLONG, BLASLONG, BLASLONG, FLOAT *, BLASLONG, FLOAT , FLOAT, FLOAT *, BLASLONG, FLOAT , FLOAT, FLOAT *, BLASLONG)) SMALL_KERNEL_ADDR(zgemm_small_kernel, (idx))
#define ZGEMM_SMALL_KERNEL_B0(idx) (int (*)(BLASLONG, BLASLONG, BLASLONG, FLOAT *, BLASLONG, FLOAT , FLOAT, FLOAT *, BLASLONG, FLOAT *, BLASLONG)) SMALL_KERNEL_ADDR(zgemm_small_kernel_b0, (idx))
#endif
#endif

#if defined(__linux__) && defined(__x86_64__) && defined(BFLOAT16)
#define XFEATURE_XTILEDATA 18
#define ARCH_REQ_XCOMP_PERM 0x1023
static int openblas_amxtile_permission = 0;
static int init_amxtile_permission() {
  long status =
      syscall(SYS_arch_prctl, ARCH_REQ_XCOMP_PERM, XFEATURE_XTILEDATA);
  if (status != 0) {
    fprintf(stderr, "XTILEDATA permission not granted in your device(Linux, "
                    "Intel Sapphier Rapids), skip sbgemm calculation\n");
    return -1;
  }
  openblas_amxtile_permission = 1;
  return 0;
}
#endif

#ifdef SMP
#ifdef DYNAMIC_ARCH
extern char* gotoblas_corename(void);
#endif

#if defined(DYNAMIC_ARCH) || defined(NEOVERSEV1)
static inline int get_gemm_optimal_nthreads_neoversev1(double MNK, int ncpu) {
  return
      MNK < 262144L    ? 1
    : MNK < 1124864L   ? MIN(ncpu, 6)
    : MNK < 7880599L   ? MIN(ncpu, 12)
    : MNK < 17173512L  ? MIN(ncpu, 16)
    : MNK < 33386248L  ? MIN(ncpu, 20)
    : MNK < 57066625L  ? MIN(ncpu, 24)
    : MNK < 91733851L  ? MIN(ncpu, 32)
    : MNK < 265847707L ? MIN(ncpu, 40)
    : MNK < 458314011L ? MIN(ncpu, 48)
    : MNK < 729000000L ? MIN(ncpu, 56)
    : ncpu;
}
#endif

#if defined(DYNAMIC_ARCH) || defined(NEOVERSEV2)
static inline int get_gemm_optimal_nthreads_neoversev2(double MNK, int ncpu) {
  return
      MNK < 125000L     ? 1
    : MNK < 1092727L    ? MIN(ncpu, 6)
    : MNK < 2628072L    ? MIN(ncpu, 8)
    : MNK < 8000000L    ? MIN(ncpu, 12)
    : MNK < 20346417L   ? MIN(ncpu, 16)
    : MNK < 57066625L   ? MIN(ncpu, 24)
    : MNK < 91125000L   ? MIN(ncpu, 28)
    : MNK < 238328000L  ? MIN(ncpu, 40)
    : MNK < 454756609L  ? MIN(ncpu, 48)
    : MNK < 857375000L  ? MIN(ncpu, 56)
    : MNK < 1073741824L ? MIN(ncpu, 64)
    : ncpu;
}
#endif

static inline int get_gemm_optimal_nthreads(double MNK) {
  int ncpu = num_cpu_avail(3);
#if defined(NEOVERSEV1) && !defined(COMPLEX) && !defined(DOUBLE) && !defined(BFLOAT16)
  return get_gemm_optimal_nthreads_neoversev1(MNK, ncpu);
#elif defined(NEOVERSEV2) && !defined(COMPLEX) && !defined(DOUBLE) && !defined(BFLOAT16)
  return get_gemm_optimal_nthreads_neoversev2(MNK, ncpu);
#elif defined(DYNAMIC_ARCH) && !defined(COMPLEX) && !defined(DOUBLE) && !defined(BFLOAT16)
  if (strcmp(gotoblas_corename(), "neoversev1") == 0) {
    return get_gemm_optimal_nthreads_neoversev1(MNK, ncpu);
  }
  if (strcmp(gotoblas_corename(), "neoversev2") == 0) {
    return get_gemm_optimal_nthreads_neoversev2(MNK, ncpu);
  }
#endif
  if ( MNK <= (SMP_THRESHOLD_MIN  * (double) GEMM_MULTITHREAD_THRESHOLD) ) {
    return 1;
  }
  else {
    if (MNK/ncpu < SMP_THRESHOLD_MIN*(double)GEMM_MULTITHREAD_THRESHOLD) {
      return MNK/(SMP_THRESHOLD_MIN*(double)GEMM_MULTITHREAD_THRESHOLD);
    }
    else {
      return ncpu;
    }
  }
}
#endif

#ifndef CBLAS

void NAME(char *TRANSA, char *TRANSB,
	  blasint *M, blasint *N, blasint *K,
	  FLOAT *alpha,
	  IFLOAT *a, blasint *ldA,
	  IFLOAT *b, blasint *ldB,
	  FLOAT *beta,
	  FLOAT *c, blasint *ldC){

  blas_arg_t args;

  int transa, transb, nrowa, nrowb;
  blasint info;

  char transA, transB;
  IFLOAT *buffer;
  IFLOAT *sa, *sb;

#ifdef SMP
  double MNK;
#if defined(USE_SIMPLE_THREADED_LEVEL3) || !defined(NO_AFFINITY)
#ifndef COMPLEX
#ifdef XDOUBLE
  int mode  =  BLAS_XDOUBLE | BLAS_REAL;
#elif defined(DOUBLE)
  int mode  =  BLAS_DOUBLE  | BLAS_REAL;
#else
  int mode  =  BLAS_SINGLE  | BLAS_REAL;
#endif
#else
#ifdef XDOUBLE
  int mode  =  BLAS_XDOUBLE | BLAS_COMPLEX;
#elif defined(DOUBLE)
  int mode  =  BLAS_DOUBLE  | BLAS_COMPLEX;
#else
  int mode  =  BLAS_SINGLE  | BLAS_COMPLEX;
#endif
#endif
#endif
#endif

#if defined(SMP) && !defined(NO_AFFINITY) && !defined(USE_SIMPLE_THREADED_LEVEL3)
  int nodes;
#endif

  PRINT_DEBUG_NAME;

  args.m = *M;
  args.n = *N;
  args.k = *K;

  args.a = (void *)a;
  args.b = (void *)b;
  args.c = (void *)c;

  args.lda = *ldA;
  args.ldb = *ldB;
  args.ldc = *ldC;

  args.alpha = (void *)alpha;
  args.beta  = (void *)beta;

  transA = *TRANSA;
  transB = *TRANSB;

  TOUPPER(transA);
  TOUPPER(transB);

  transa = -1;
  transb = -1;

  if (transA == 'N') transa = 0;
  if (transA == 'T') transa = 1;
#ifndef COMPLEX
  if (transA == 'R') transa = 0;
  if (transA == 'C') transa = 1;
#else
  if (transA == 'R') transa = 2;
  if (transA == 'C') transa = 3;
#endif

  if (transB == 'N') transb = 0;
  if (transB == 'T') transb = 1;
#ifndef COMPLEX
  if (transB == 'R') transb = 0;
  if (transB == 'C') transb = 1;
#else
  if (transB == 'R') transb = 2;
  if (transB == 'C') transb = 3;
#endif

  nrowa = args.m;
  if (transa & 1) nrowa = args.k;
  nrowb = args.k;
  if (transb & 1) nrowb = args.n;

  info = 0;

  if (args.ldc < args.m) info = 13;
  if (args.ldb < nrowb)  info = 10;
  if (args.lda < nrowa)  info =  8;
  if (args.k < 0)        info =  5;
  if (args.n < 0)        info =  4;
  if (args.m < 0)        info =  3;
  if (transb < 0)        info =  2;
  if (transa < 0)        info =  1;

  if (info){
    BLASFUNC(xerbla)(ERROR_NAME, &info, sizeof(ERROR_NAME));
    return;
  }

#else

void CNAME(enum CBLAS_ORDER order, enum CBLAS_TRANSPOSE TransA, enum CBLAS_TRANSPOSE TransB,
	   blasint m, blasint n, blasint k,
#ifndef COMPLEX
	   FLOAT alpha,
	   IFLOAT *a, blasint lda,
	   IFLOAT *b, blasint ldb,
	   FLOAT beta,
	   FLOAT *c, blasint ldc) {
#else
	   void *valpha,
	   void *va, blasint lda,
	   void *vb, blasint ldb,
	   void *vbeta,
	   void *vc, blasint ldc) {
  FLOAT *alpha = (FLOAT*) valpha;
  FLOAT *beta  = (FLOAT*) vbeta;
  FLOAT *a = (FLOAT*) va;
  FLOAT *b = (FLOAT*) vb;
  FLOAT *c = (FLOAT*) vc;
#endif

  blas_arg_t args;
  int transa, transb;
  blasint nrowa, nrowb, info;

  XFLOAT *buffer;
  XFLOAT *sa, *sb;

#ifdef SMP
  double MNK;
#if defined(USE_SIMPLE_THREADED_LEVEL3) || !defined(NO_AFFINITY)
#ifndef COMPLEX
#ifdef XDOUBLE
  int mode  =  BLAS_XDOUBLE | BLAS_REAL;
#elif defined(DOUBLE)
  int mode  =  BLAS_DOUBLE  | BLAS_REAL;
#else
  int mode  =  BLAS_SINGLE  | BLAS_REAL;
#endif
#else
#ifdef XDOUBLE
  int mode  =  BLAS_XDOUBLE | BLAS_COMPLEX;
#elif defined(DOUBLE)
  int mode  =  BLAS_DOUBLE  | BLAS_COMPLEX;
#else
  int mode  =  BLAS_SINGLE  | BLAS_COMPLEX;
#endif
#endif
#endif
#endif

#if defined(SMP) && !defined(NO_AFFINITY) && !defined(USE_SIMPLE_THREADED_LEVEL3)
  int nodes;
#endif

  PRINT_DEBUG_CNAME;

#if !defined(COMPLEX) && !defined(DOUBLE) && !defined(BFLOAT16) 
#if defined(ARCH_x86) && (defined(USE_SGEMM_KERNEL_DIRECT)||defined(DYNAMIC_ARCH))
#if defined(DYNAMIC_ARCH)
  if (support_avx512() )
#endif
  if (beta == 0 && alpha == 1.0 && order == CblasRowMajor && TransA == CblasNoTrans && TransB == CblasNoTrans && SGEMM_DIRECT_PERFORMANT(m,n,k)) {
	SGEMM_DIRECT(m, n, k, a, lda, b, ldb, c, ldc);
	return;
  }
#endif
#if defined(ARCH_ARM64) && (defined(USE_SGEMM_KERNEL_DIRECT)||defined(DYNAMIC_ARCH))
#if defined(DYNAMIC_ARCH)
 if (support_sme1())
#endif
  if (beta == 0 && alpha == 1.0 && order == CblasRowMajor && TransA == CblasNoTrans && TransB == CblasNoTrans) {
	SGEMM_DIRECT(m, n, k, a, lda, b, ldb, c, ldc);
	return;
  }
#endif
#endif

#ifndef COMPLEX
  args.alpha = (void *)&alpha;
  args.beta  = (void *)&beta;
#else
  args.alpha = (void *)alpha;
  args.beta  = (void *)beta;
#endif

  transa = -1;
  transb = -1;
  info   =  0;

  if (order == CblasColMajor) {
    args.m = m;
    args.n = n;
    args.k = k;

    args.a = (void *)a;
    args.b = (void *)b;
    args.c = (void *)c;

    args.lda = lda;
    args.ldb = ldb;
    args.ldc = ldc;

    if (TransA == CblasNoTrans)     transa = 0;
    if (TransA == CblasTrans)       transa = 1;
#ifndef COMPLEX
    if (TransA == CblasConjNoTrans) transa = 0;
    if (TransA == CblasConjTrans)   transa = 1;
#else
    if (TransA == CblasConjNoTrans) transa = 2;
    if (TransA == CblasConjTrans)   transa = 3;
#endif
    if (TransB == CblasNoTrans)     transb = 0;
    if (TransB == CblasTrans)       transb = 1;
#ifndef COMPLEX
    if (TransB == CblasConjNoTrans) transb = 0;
    if (TransB == CblasConjTrans)   transb = 1;
#else
    if (TransB == CblasConjNoTrans) transb = 2;
    if (TransB == CblasConjTrans)   transb = 3;
#endif

    nrowa = args.m;
    if (transa & 1) nrowa = args.k;
    nrowb = args.k;
    if (transb & 1) nrowb = args.n;

    info = -1;

    if (args.ldc < args.m) info = 13;
    if (args.ldb < nrowb)  info = 10;
    if (args.lda < nrowa)  info =  8;
    if (args.k < 0)        info =  5;
    if (args.n < 0)        info =  4;
    if (args.m < 0)        info =  3;
    if (transb < 0)        info =  2;
    if (transa < 0)        info =  1;
  }

  if (order == CblasRowMajor) {
    args.m = n;
    args.n = m;
    args.k = k;

    args.a = (void *)b;
    args.b = (void *)a;
    args.c = (void *)c;

    args.lda = ldb;
    args.ldb = lda;
    args.ldc = ldc;

    if (TransB == CblasNoTrans)     transa = 0;
    if (TransB == CblasTrans)       transa = 1;
#ifndef COMPLEX
    if (TransB == CblasConjNoTrans) transa = 0;
    if (TransB == CblasConjTrans)   transa = 1;
#else
    if (TransB == CblasConjNoTrans) transa = 2;
    if (TransB == CblasConjTrans)   transa = 3;
#endif
    if (TransA == CblasNoTrans)     transb = 0;
    if (TransA == CblasTrans)       transb = 1;
#ifndef COMPLEX
    if (TransA == CblasConjNoTrans) transb = 0;
    if (TransA == CblasConjTrans)   transb = 1;
#else
    if (TransA == CblasConjNoTrans) transb = 2;
    if (TransA == CblasConjTrans)   transb = 3;
#endif

    nrowa = args.m;
    if (transa & 1) nrowa = args.k;
    nrowb = args.k;
    if (transb & 1) nrowb = args.n;

    info = -1;

    if (args.ldc < args.m) info = 13;
    if (args.ldb < nrowb)  info = 10;
    if (args.lda < nrowa)  info =  8;
    if (args.k < 0)        info =  5;
    if (args.n < 0)        info =  4;
    if (args.m < 0)        info =  3;
    if (transb < 0)        info =  2;
    if (transa < 0)        info =  1;

  }

  if (info >= 0) {
    BLASFUNC(xerbla)(ERROR_NAME, &info, sizeof(ERROR_NAME));
    return;
  }

#endif

#if defined(__linux__) && defined(__x86_64__) && defined(BFLOAT16)
#if defined(DYNAMIC_ARCH)
  if (gotoblas->need_amxtile_permission &&
      openblas_amxtile_permission == 0 && init_amxtile_permission() == -1) {
    return;
  }
#endif
#if !defined(DYNAMIC_ARCH) && defined(SAPPHIRERAPIDS)
  if (openblas_amxtile_permission == 0 && init_amxtile_permission() == -1) {
    return;
  }
#endif
#endif  // defined(__linux__) && defined(__x86_64__) && defined(BFLOAT16)

  if ((args.m == 0) || (args.n == 0)) return;

#if 0
  fprintf(stderr, "m = %4d  n = %d  k = %d  lda = %4d  ldb = %4d  ldc = %4d\n",
	 args.m, args.n, args.k, args.lda, args.ldb, args.ldc);
#endif

#if defined(GEMM_GEMV_FORWARD) && !defined(GEMM3M) && !defined(COMPLEX) && (!defined(BFLOAT16) || defined(GEMM_GEMV_FORWARD_BF16))
#if defined(ARCH_ARM64)
  // The gemv kernels in arm64/{gemv_n.S,gemv_n_sve.c,gemv_t.S,gemv_t_sve.c}
  // perform poorly in certain circumstances. We use the following boolean
  // variable along with the gemv argument values to avoid these inefficient
  // gemv cases, see github issue#4951.
  bool have_tuned_gemv = false;
#else
  bool have_tuned_gemv = true;
#endif
  // Check if we can convert GEMM -> GEMV
  if (args.k != 0) {
    if (args.n == 1) {
      blasint inc_x = 1;
      blasint inc_y = 1;
      // These were passed in as blasint, but the struct translates them to blaslong
      blasint m = args.m;
      blasint n = args.k;
      blasint lda = args.lda;
      // Create new transpose parameters
      char NT = 'N';
      if (transa & 1) {
        NT = 'T';
        m = args.k;
        n = args.m;
      }
      if (transb & 1) {
        inc_x = args.ldb;
      }
      bool is_efficient_gemv = have_tuned_gemv || ((NT == 'N') || (NT == 'T' && inc_x == 1));
      if (is_efficient_gemv) {
        GEMV(&NT, &m, &n, args.alpha, args.a, &lda, args.b, &inc_x, args.beta, args.c, &inc_y);
        return;
      }
    }
    if (args.m == 1) {
      blasint inc_x = args.lda;
      blasint inc_y = args.ldc;
      // These were passed in as blasint, but the struct translates them to blaslong
      blasint m = args.k;
      blasint n = args.n;
      blasint ldb = args.ldb;
      // Create new transpose parameters
      char NT = 'T';
      if (transa & 1) {
        inc_x = 1;
      }
      if (transb & 1) {
        NT = 'N';
        m = args.n;
        n = args.k;
      }
      bool is_efficient_gemv = have_tuned_gemv || ((NT == 'N' && inc_y == 1) || (NT == 'T' && inc_x == 1));
      if (is_efficient_gemv) {
        GEMV(&NT, &m, &n, args.alpha, args.b, &ldb, args.a, &inc_x, args.beta, args.c, &inc_y);
        return;
      }
    }
  }
#endif

  IDEBUG_START;

  FUNCTION_PROFILE_START();

#if USE_SMALL_MATRIX_OPT
#if !defined(COMPLEX)
  if(GEMM_SMALL_MATRIX_PERMIT(transa, transb, args.m, args.n, args.k, *(FLOAT *)(args.alpha), *(FLOAT *)(args.beta))){
	  if(*(FLOAT *)(args.beta) == 0.0){
		(GEMM_SMALL_KERNEL_B0((transb << 2) | transa))(args.m, args.n, args.k, args.a, args.lda, *(FLOAT *)(args.alpha), args.b, args.ldb, args.c, args.ldc);
	  }else{
		(GEMM_SMALL_KERNEL((transb << 2) | transa))(args.m, args.n, args.k, args.a, args.lda, *(FLOAT *)(args.alpha), args.b, args.ldb, *(FLOAT *)(args.beta), args.c, args.ldc);
	  }
	  return;
  }
#else
  if(GEMM_SMALL_MATRIX_PERMIT(transa, transb, args.m, args.n, args.k, alpha[0], alpha[1], beta[0], beta[1])){
	  if(beta[0] == 0.0 && beta[1] == 0.0){
		(ZGEMM_SMALL_KERNEL_B0((transb << 2) | transa))(args.m, args.n, args.k, args.a, args.lda, alpha[0], alpha[1], args.b, args.ldb, args.c, args.ldc);
	  }else{
		(ZGEMM_SMALL_KERNEL((transb << 2) | transa))(args.m, args.n, args.k, args.a, args.lda, alpha[0], alpha[1], args.b, args.ldb, beta[0], beta[1], args.c, args.ldc);
	  }
	  return;
  }
#endif
#endif

  buffer = (XFLOAT *)blas_memory_alloc(0);

//For LOONGARCH64, applying an offset to the buffer is essential
//for minimizing cache conflicts and optimizing performance.
#if defined(ARCH_LOONGARCH64) && !defined(NO_AFFINITY)
  sa = (XFLOAT *)((BLASLONG)buffer + (WhereAmI() & 0xf) * GEMM_OFFSET_A);
#else
  sa = (XFLOAT *)((BLASLONG)buffer +GEMM_OFFSET_A);
#endif
  sb = (XFLOAT *)(((BLASLONG)sa + ((GEMM_P * GEMM_Q * COMPSIZE * SIZE + GEMM_ALIGN) & ~GEMM_ALIGN)) + GEMM_OFFSET_B);

#ifdef SMP
#if defined(USE_SIMPLE_THREADED_LEVEL3) || !defined(NO_AFFINITY)
  mode |= (transa << BLAS_TRANSA_SHIFT);
  mode |= (transb << BLAS_TRANSB_SHIFT);
#endif

  MNK = (double) args.m * (double) args.n * (double) args.k;
  args.nthreads = get_gemm_optimal_nthreads(MNK);

  args.common = NULL;

 if (args.nthreads == 1) {
#endif

    (gemm[(transb << 2) | transa])(&args, NULL, NULL, sa, sb, 0);

#ifdef SMP

  } else {

#ifndef USE_SIMPLE_THREADED_LEVEL3

#ifndef NO_AFFINITY
      nodes = get_num_nodes();

      if ((nodes > 1) && get_node_equal()) {

	args.nthreads /= nodes;

	gemm_thread_mn(mode, &args, NULL, NULL, gemm[16 | (transb << 2) | transa], sa, sb, nodes);

      } else {
#endif

	(gemm[16 | (transb << 2) | transa])(&args, NULL, NULL, sa, sb, 0);

#else

	GEMM_THREAD(mode, &args, NULL, NULL, gemm[(transb << 2) | transa], sa, sb, args.nthreads);

#endif

#ifndef USE_SIMPLE_THREADED_LEVEL3
#ifndef NO_AFFINITY
      }
#endif
#endif

#endif

#ifdef SMP
  }
#endif

 blas_memory_free(buffer);

  FUNCTION_PROFILE_END(COMPSIZE * COMPSIZE, args.m * args.k + args.k * args.n + args.m * args.n, 2 * args.m * args.n * args.k);

  IDEBUG_END;

  return;
}
