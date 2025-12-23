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

#undef SCAL

#ifdef COMPLEX
#ifdef DOUBLE
#define SCAL   BLASFUNC(zscal)
#else
#define SCAL   BLASFUNC(cscal)
#endif
#else
#ifdef DOUBLE
#define SCAL   BLASFUNC(dscal)
#else
#define SCAL   BLASFUNC(sscal)
#endif
#endif

int main(int argc, char *argv[]){

  FLOAT *x;
  FLOAT alpha[2] = { 2.0, 2.0 };
  blasint m, i;
  blasint inc_x=1,inc_y=1;
  int loops = 1;
  int l;
  char *p;

  int from =   1;
  int to   = 200;
  int step =   1;

  double time1,timeg;

  argc--;argv++;

  if (argc > 0) { from     = atol(*argv);		argc--; argv++;}
  if (argc > 0) { to       = MAX(atol(*argv), from);	argc--; argv++;}
  if (argc > 0) { step     = atol(*argv);		argc--; argv++;}

  if ((p = getenv("OPENBLAS_LOOPS")))  loops = atoi(p);
  if ((p = getenv("OPENBLAS_INCX")))   inc_x = atoi(p);

  fprintf(stderr, "From : %3d  To : %3d Step = %3d Inc_x = %d Inc_y = %d Loops = %d\n", from, to, step,inc_x,inc_y,loops);

  if (( x = (FLOAT *)malloc(sizeof(FLOAT) * to * abs(inc_x) * COMPSIZE)) == NULL){
    fprintf(stderr,"Out of Memory!!\n");exit(1);
  }

#ifdef __linux
  srandom(getpid());
#endif

  fprintf(stderr, "   SIZE       Flops\n");

  for(m = from; m <= to; m += step)
  {

   timeg=0;

   fprintf(stderr, " %6d : ", (int)m);

   for(i = 0; i < m * COMPSIZE * abs(inc_x); i++){
	x[i] = ((FLOAT) rand() / (FLOAT) RAND_MAX) - 0.5;
   }

   begin();
   for (l=0; l<loops; l++)
   {
    	SCAL (&m, alpha, x, &inc_x);
   }
   end();

   time1 = getsec();

   timeg = time1 / loops;

#ifdef COMPLEX
    fprintf(stderr, " %10.2f MFlops %10.6f sec\n", 6. * (double)m / timeg * 1.e-6, timeg);
#else
    fprintf(stderr, " %10.2f MFlops %10.6f sec\n", 1. * (double)m / timeg * 1.e-6, timeg);
#endif

  }

  return 0;
}

// void main(int argc, char *argv[]) __attribute__((weak, alias("MAIN__")));
