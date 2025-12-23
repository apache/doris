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

#include "bench.h"

#undef GEMM

#ifndef COMPLEX

#ifdef DOUBLE
#define GEMM   BLASFUNC(dgemm)
#elif defined(HALF)
#define GEMM   BLASFUNC(sbgemm)
#else
#define GEMM   BLASFUNC(sgemm)
#endif

#else

#ifdef DOUBLE
#define GEMM   BLASFUNC(zgemm)
#else
#define GEMM   BLASFUNC(cgemm)
#endif

#endif

int main(int argc, char *argv[]){

  IFLOAT *a, *b;
  FLOAT *c;
  FLOAT alpha[] = {1.0, 0.0};
  FLOAT beta [] = {0.0, 0.0};
  char transa = 'N';
  char transb = 'N';
  blasint m, n, k, i, j, lda, ldb, ldc;
  int loops = 1;
  int has_param_m = 0;
  int has_param_n = 0;
  int has_param_k = 0;
  char *p;

  int from =   1;
  int to   = 200;
  int step =   1;

  double time1, timeg;

  argc--;argv++;

  if (argc > 0) { from = atol(*argv);            argc--; argv++; }
  if (argc > 0) { to   = MAX(atol(*argv), from); argc--; argv++; }
  if (argc > 0) { step = atol(*argv);            argc--; argv++; }

  if ((p = getenv("OPENBLAS_TRANS"))) {
    transa=*p;
    transb=*p;
  }
  if ((p = getenv("OPENBLAS_TRANSA"))) {
    transa=*p;
  }
  if ((p = getenv("OPENBLAS_TRANSB"))) {
    transb=*p;
  }
  TOUPPER(transa);
  TOUPPER(transb);

  fprintf(stderr, "From : %3d  To : %3d Step=%d : Transa=%c : Transb=%c\n", from, to, step, transa, transb);

  p = getenv("OPENBLAS_LOOPS");
  if ( p != NULL ) {
    loops = atoi(p);
  }

  if ((p = getenv("OPENBLAS_PARAM_M"))) {
    m = atoi(p);
    has_param_m=1;
  } else {
    m = to;
  }
  if ((p = getenv("OPENBLAS_PARAM_N"))) {
    n = atoi(p);
    has_param_n=1;
  } else {
    n = to;
  }
  if ((p = getenv("OPENBLAS_PARAM_K"))) {
    k = atoi(p);
    has_param_k=1;
  } else {
    k = to;
  }

  if (( a = (IFLOAT *)malloc(sizeof(IFLOAT) * m * k * COMPSIZE)) == NULL) {
    fprintf(stderr,"Out of Memory!!\n");exit(1);
  }
  if (( b = (IFLOAT *)malloc(sizeof(IFLOAT) * k * n * COMPSIZE)) == NULL) {
    fprintf(stderr,"Out of Memory!!\n");exit(1);
  }
  if (( c = (FLOAT *)malloc(sizeof(FLOAT) * m * n * COMPSIZE)) == NULL) {
    fprintf(stderr,"Out of Memory!!\n");exit(1);
  }

#ifdef __linux
  srandom(getpid());
#endif

  for (i = 0; i < m * k * COMPSIZE; i++) {
    a[i] = ((IFLOAT) rand() / (IFLOAT) RAND_MAX) - 0.5;
  }
  for (i = 0; i < k * n * COMPSIZE; i++) {
    b[i] = ((IFLOAT) rand() / (IFLOAT) RAND_MAX) - 0.5;
  }
  for (i = 0; i < m * n * COMPSIZE; i++) {
    c[i] = ((FLOAT) rand() / (FLOAT) RAND_MAX) - 0.5;
  }

  fprintf(stderr, "          SIZE                   Flops             Time\n");

  for (i = from; i <= to; i += step) {
    
    timeg=0;

    if (!has_param_m) { m = i; }
    if (!has_param_n) { n = i; }
    if (!has_param_k) { k = i; }

    if (transa == 'N') { lda = m; }
    else { lda = k; }
    if (transb == 'N') { ldb = k; }
    else { ldb = n; }
    ldc = m;

    fprintf(stderr, " M=%4d, N=%4d, K=%4d : ", (int)m, (int)n, (int)k);
    begin();

    for (j=0; j<loops; j++) {
      GEMM (&transa, &transb, &m, &n, &k, alpha, a, &lda, b, &ldb, beta, c, &ldc);
    }

    end();
    time1 = getsec();

    timeg = time1/loops;
    fprintf(stderr,
	    " %10.2f MFlops %10.6f sec\n",
	    COMPSIZE * COMPSIZE * 2. * (double)k * (double)m * (double)n / timeg * 1.e-6, time1);
    
  }

  return 0;
}

// void main(int argc, char *argv[]) __attribute__((weak, alias("MAIN__")));
