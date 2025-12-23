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

#undef GEMV
#undef TRSV

#ifndef COMPLEX

#ifdef DOUBLE
#define TRSV   BLASFUNC(dtrsv)
#else
#define TRSV   BLASFUNC(strsv)
#endif

#else

#ifdef DOUBLE
#define TRSV   BLASFUNC(ztrsv)
#else
#define TRSV   BLASFUNC(ctrsv)
#endif

#endif

int main(int argc, char *argv[]){

  FLOAT *a, *x;
  blasint n = 0, i, j;
  blasint inc_x=1;
  int loops = 1;
  int l;
  char *p;

  int from =   1;
  int to   = 200;
  int step =   1;

  time_t seconds = 0;

  double time1,timeg;
  long long nanos = 0;

  argc--;argv++;

  if (argc > 0) { from     = atol(*argv);		argc--; argv++;}
  if (argc > 0) { to       = MAX(atol(*argv), from);	argc--; argv++;}
  if (argc > 0) { step     = atol(*argv);		argc--; argv++;}

  char uplo ='L';
  char transa = 'N';
  char diag ='U';

  if ((p = getenv("OPENBLAS_LOOPS")))  loops = atoi(p);
  if ((p = getenv("OPENBLAS_INCX")))   inc_x = atoi(p);
  if ((p = getenv("OPENBLAS_TRANSA")))  transa=*p;
  if ((p = getenv("OPENBLAS_DIAG")))  diag=*p;
  if ((p = getenv("OPENBLAS_UPLO")))  uplo=*p;

  fprintf(stderr, "From : %3d  To : %3d Step = %3d Transa = '%c' Inc_x = %d uplo=%c diag=%c loop = %d\n", from, to, step,transa,inc_x,
          uplo,diag,loops);


#ifdef __linux
  srandom(getpid());
#endif

  fprintf(stderr, "   SIZE       Flops\n");
  fprintf(stderr, "============================================\n");

  for(n = from; n <= to; n += step)
  {
      timeg=0;
      if (( a = (FLOAT *)malloc(sizeof(FLOAT) * n * n * COMPSIZE)) == NULL){
          fprintf(stderr,"Out of Memory!!\n");exit(1);
      }

      if (( x = (FLOAT *)malloc(sizeof(FLOAT) * n * abs(inc_x) * COMPSIZE)) == NULL){
          fprintf(stderr,"Out of Memory!!\n");exit(1);
      }

      for(j = 0; j < n; j++){
          for(i = 0; i < n * COMPSIZE; i++){
              a[i + j * n * COMPSIZE] = ((FLOAT) rand() / (FLOAT) RAND_MAX) - 0.5;
          }
      }

      for(i = 0; i < n * COMPSIZE * abs(inc_x); i++){
          x[i] = ((FLOAT) rand() / (FLOAT) RAND_MAX) - 0.5;
      }

      for(l =0;l< loops;l++){

          begin();
          TRSV(&uplo,&transa,&diag,&n,a,&n,x,&inc_x);
          end();
          time1 = getsec();
          timeg += time1;
      }

      timeg /= loops;
      long long muls = n*(n+1)/2.0;
      long long adds = (n - 1.0)*n/2.0;

      fprintf(stderr, "%10d :   %10.2f MFlops %10.6f sec\n", n,(muls+adds) / timeg * 1.e-6, timeg);
      if(a != NULL){
        free(a);
      }

      if( x != NULL){
        free(x);
      }

  }

  return 0;
}

