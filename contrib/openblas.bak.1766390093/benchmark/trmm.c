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


#undef TRMM

#ifndef COMPLEX

#ifdef DOUBLE
#define TRMM   BLASFUNC(dtrmm)
#else
#define TRMM   BLASFUNC(strmm)
#endif

#else

#ifdef DOUBLE
#define TRMM   BLASFUNC(ztrmm)
#else
#define TRMM   BLASFUNC(ctrmm)
#endif

#endif

int main(int argc, char *argv[]){

  FLOAT *a, *b;
  FLOAT alpha[] = {1.0, 1.0};
  FLOAT beta [] = {1.0, 1.0};
  char *p;

  char side ='L';
  char uplo ='U';
  char trans='N';
  char diag ='U';

  if ((p = getenv("OPENBLAS_SIDE"))) side=*p; 
  if ((p = getenv("OPENBLAS_UPLO"))) uplo=*p;
  if ((p = getenv("OPENBLAS_TRANS"))) trans=*p;
  if ((p = getenv("OPENBLAS_DIAG"))) diag=*p;

  blasint m, i, j;

  int from =   1;
  int to   = 200;
  int step =   1;

  double time1;

  argc--;argv++;

  if (argc > 0) { from     = atol(*argv);		argc--; argv++;}
  if (argc > 0) { to       = MAX(atol(*argv), from);	argc--; argv++;}
  if (argc > 0) { step     = atol(*argv);		argc--; argv++;}

  fprintf(stderr, "From : %3d  To : %3d Step = %3d Side = %c Uplo = %c Trans = %c Diag = %c\n", from, to, step,side,uplo,trans,diag);

  if (( a = (FLOAT *)malloc(sizeof(FLOAT) * to * to * COMPSIZE)) == NULL){
    fprintf(stderr,"Out of Memory!!\n");exit(1);
  }

  if (( b = (FLOAT *)malloc(sizeof(FLOAT) * to * to * COMPSIZE)) == NULL){
    fprintf(stderr,"Out of Memory!!\n");exit(1);
  }



#ifdef __linux
  srandom(getpid());
#endif

  fprintf(stderr, "   SIZE       Flops\n");

  for(m = from; m <= to; m += step)
  {

    fprintf(stderr, " %6d : ", (int)m);

    for(j = 0; j < m; j++){
      for(i = 0; i < m * COMPSIZE; i++){
	a[(long)i + (long)j * (long)m * COMPSIZE] = ((FLOAT) rand() / (FLOAT) RAND_MAX) - 0.5;
	b[(long)i + (long)j * (long)m * COMPSIZE] = ((FLOAT) rand() / (FLOAT) RAND_MAX) - 0.5;
      }
    }

    begin();

    TRMM (&side, &uplo, &trans, &diag, &m, &m, alpha, a, &m, b, &m);

    end();

    time1 = getsec();

    fprintf(stderr,
	    " %10.2f MFlops  %10.6f sec\n",
	    COMPSIZE * COMPSIZE * 1. * (double)m * (double)m * (double)m / time1 * 1.e-6, time1);

  }

  return 0;
}

// void main(int argc, char *argv[]) __attribute__((weak, alias("MAIN__")));
