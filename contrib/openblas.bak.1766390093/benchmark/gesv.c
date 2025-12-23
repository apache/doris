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

#include "bench.h"

double fabs(double);

#undef GESV
#undef GETRS

#ifndef COMPLEX
#ifdef XDOUBLE
#define GESV   BLASFUNC(qgesv)
#elif defined(DOUBLE)
#define GESV   BLASFUNC(dgesv)
#else
#define GESV   BLASFUNC(sgesv)
#endif
#else
#ifdef XDOUBLE
#define GESV   BLASFUNC(xgesv)
#elif defined(DOUBLE)
#define GESV   BLASFUNC(zgesv)
#else
#define GESV   BLASFUNC(cgesv)
#endif
#endif

int main(int argc, char *argv[]){

  FLOAT *a, *b;
  blasint *ipiv;

  blasint m, i, j, info;

  int from =   1;
  int to   = 200;
  int step =   1;

  double time1;

  argc--;argv++;

  if (argc > 0) { from     = atol(*argv);		argc--; argv++;}
  if (argc > 0) { to       = MAX(atol(*argv), from);	argc--; argv++;}
  if (argc > 0) { step     = atol(*argv);		argc--; argv++;}

  fprintf(stderr, "From : %3d  To : %3d Step = %3d\n", from, to, step);

  if (( a = (FLOAT *)malloc(sizeof(FLOAT) * to * to * COMPSIZE)) == NULL){
    fprintf(stderr,"Out of Memory!!\n");exit(1);
  }

  if (( b = (FLOAT *)malloc(sizeof(FLOAT) * to * to * COMPSIZE)) == NULL){
    fprintf(stderr,"Out of Memory!!\n");exit(1);
  }

  if (( ipiv = (blasint *)malloc(sizeof(blasint) * to * COMPSIZE)) == NULL){
    fprintf(stderr,"Out of Memory!!\n");exit(1);
  }

#ifdef __linux
  srandom(getpid());
#endif

  fprintf(stderr, "   SIZE       Flops              Time\n");

  for(m = from; m <= to; m += step){

    fprintf(stderr, " %dx%d : ", (int)m, (int)m);

    for(j = 0; j < m; j++){
      for(i = 0; i < m * COMPSIZE; i++){
	a[(long)i + (long)j * (long)m * COMPSIZE] = ((FLOAT) rand() / (FLOAT) RAND_MAX) - 0.5;
      }
    }

    for(j = 0; j < m; j++){
      for(i = 0; i < m * COMPSIZE; i++){
	b[(long)i + (long)j * (long)m * COMPSIZE] = 0.0;
      }
    }


    for (j = 0; j < m; ++j) {
      for (i = 0; i < m * COMPSIZE; ++i) {
	b[i] += a[(long)i + (long)j * (long)m * COMPSIZE];
      }
    }

    begin();

    GESV (&m, &m, a, &m, ipiv, b, &m,  &info);

    end();

    time1 = getsec();

    fprintf(stderr,
	    "%10.2f MFlops %10.6f s\n",
	    COMPSIZE * COMPSIZE * (2. / 3. * (double)m * (double)m * (double)m + 2. * (double)m * (double)m * (double)m ) / (time1) * 1.e-6 , time1);

  }

  return 0;
}

// void main(int argc, char *argv[]) __attribute__((weak, alias("MAIN__")));
