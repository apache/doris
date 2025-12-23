/***************************************************************************
Copyright (c) 2024, The OpenBLAS Project
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

#undef OMATCOPY

#ifndef COMPLEX
#ifdef DOUBLE
#define OMATCOPY BLASFUNC(domatcopy)
#else
#define OMATCOPY BLASFUNC(somatcopy)
#endif
#else
#ifdef DOUBLE
#define OMATCOPY BLASFUNC(zomatcopy)
#else
#define OMATCOPY BLASFUNC(comatcopy)
#endif
#endif
int main(int argc, char *argv[]){
  FLOAT *a, *b;
  FLOAT alpha[] = {1.0, 0.0};
  char trans = 'N';
  char order = 'C';
  blasint crows, ccols, clda, cldb;
  int loops = 1;
  char *p;

  int from =   1;
  int to   = 200;
  int step =   1;
  int i, j;

  double time1, timeg;

  argc--;argv++;

  if (argc > 0) { from = atol(*argv);            argc--; argv++; }
  if (argc > 0) { to   = MAX(atol(*argv), from); argc--; argv++; }
  if (argc > 0) { step = atol(*argv);            argc--; argv++; }

  if ((p = getenv("OPENBLAS_TRANS"))) {
    trans=*p;
  }
  if ((p = getenv("OPENBLAS_ORDER"))) {
    order=*p;
  }
  TOUPPER(trans);
  TOUPPER(order);
  fprintf(stderr, "From : %3d  To : %3d Step=%d : Trans=%c : Order=%c\n", from, to, step, trans, order);
  p = getenv("OPENBLAS_LOOPS");
  if ( p != NULL ) {
    loops = atoi(p);
  }

  if (( a = (FLOAT *)malloc(sizeof(FLOAT) * to * to * COMPSIZE)) == NULL) {
    fprintf(stderr,"Out of Memory!!\n");exit(1);
  }
  if (( b = (FLOAT *)malloc(sizeof(FLOAT) * to * to * COMPSIZE)) == NULL) {
    fprintf(stderr,"Out of Memory!!\n");exit(1);
  }

#ifdef __linux
  srandom(getpid());
#endif

  for (i = 0; i < to * to * COMPSIZE; i++) {
    a[i] = ((FLOAT) rand() / (FLOAT) RAND_MAX) - 0.5;
  }
  for (i = 0; i < to * to * COMPSIZE; i++) {
    b[i] = ((FLOAT) rand() / (FLOAT) RAND_MAX) - 0.5;
  }

  fprintf(stderr, "          SIZE                   Flops             Time\n");
  for (i = from; i <= to; i += step) {
    cldb = clda = crows = ccols = i;
    fprintf(stderr, " ROWS=%4d, COLS=%4d : ", (int)crows, (int)ccols);
    begin();

    for (j=0; j<loops; j++) {
      OMATCOPY (&order, &trans, &crows, &ccols, alpha, a, &clda, b, &cldb);
    }

    end();
    time1 = getsec();

    timeg = time1/loops;
    fprintf(stderr,
	    " %10.2f MFlops %10.6f sec\n",
	    COMPSIZE * COMPSIZE * (double)ccols * (double)crows / timeg * 1.e-6, time1);
  }

  free(a);
  free(b);

  return 0;
}
