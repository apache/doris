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

#undef GEEV

#ifndef COMPLEX
#ifdef XDOUBLE
#define GEEV   BLASFUNC(qgeev)
#elif defined(DOUBLE)
#define GEEV   BLASFUNC(dgeev)
#else
#define GEEV   BLASFUNC(sgeev)
#endif
#else
#ifdef XDOUBLE
#define GEEV   BLASFUNC(xgeev)
#elif defined(DOUBLE)
#define GEEV   BLASFUNC(zgeev)
#else
#define GEEV   BLASFUNC(cgeev)
#endif
#endif

#ifndef COMPLEX
extern void GEEV( char* jobvl, char* jobvr, blasint* n, FLOAT* a,
                blasint* lda, FLOAT* wr, FLOAT* wi, FLOAT* vl, blasint* ldvl,
                FLOAT* vr, blasint* ldvr, FLOAT* work, blasint* lwork, blasint* info );
#else
extern void GEEV( char* jobvl, char* jobvr, blasint* n, FLOAT* a,
                blasint* lda, FLOAT* wr, FLOAT* vl, blasint* ldvl,
                FLOAT* vr, blasint* ldvr, FLOAT* work, blasint* lwork, FLOAT *rwork, blasint* info );
#endif

int main(int argc, char *argv[]){

  FLOAT *a,*vl,*vr,*wi,*wr,*work,*rwork;
  FLOAT wkopt[4];
  char job='V';
  char jobr='N';
  char *p;

  blasint m, i, j, info,lwork;
  double factor = 26.33;

  int from =   1;
  int to   = 200;
  int step =   1;

  double time1;

  argc--;argv++;

  if (argc > 0) { from     = atol(*argv);		argc--; argv++;}
  if (argc > 0) { to       = MAX(atol(*argv), from);	argc--; argv++;}
  if (argc > 0) { step     = atol(*argv);		argc--; argv++;}

  if ((p = getenv("OPENBLAS_JOB")))  job=*p;

  if ( job == 'N' ) factor = 10.0;

  fprintf(stderr, "From : %3d  To : %3d Step = %3d Job=%c\n", from, to, step,job);

  if (( a = (FLOAT *)malloc(sizeof(FLOAT) * to * to * COMPSIZE)) == NULL){
    fprintf(stderr,"Out of Memory!!\n");exit(1);
  }

  if (( vl = (FLOAT *)malloc(sizeof(FLOAT) * to * to * COMPSIZE)) == NULL){
    fprintf(stderr,"Out of Memory!!\n");exit(1);
  }

  if (( vr = (FLOAT *)malloc(sizeof(FLOAT) * to * to * COMPSIZE)) == NULL){
    fprintf(stderr,"Out of Memory!!\n");exit(1);
  }

  if (( wr = (FLOAT *)malloc(sizeof(FLOAT) * to * COMPSIZE)) == NULL){
    fprintf(stderr,"Out of Memory!!\n");exit(1);
  }

  if (( wi = (FLOAT *)malloc(sizeof(FLOAT) * to * COMPSIZE)) == NULL){
    fprintf(stderr,"Out of Memory!!\n");exit(1);
  }

  if (( rwork = (FLOAT *)malloc(sizeof(FLOAT) * to * COMPSIZE)) == NULL){
    fprintf(stderr,"Out of Memory!!\n");exit(1);
  }

    for(j = 0; j < to; j++){
      for(i = 0; i < to * COMPSIZE; i++){
	a[(long)i + (long)j * (long)to * COMPSIZE] = ((FLOAT) rand() / (FLOAT) RAND_MAX) - 0.5;
      }
    }


    lwork = -1;
    m=to;
#ifndef COMPLEX
    GEEV (&job, &jobr, &m, a, &m, wr, wi, vl, &m, vr, &m, wkopt, &lwork, &info);
#else
    GEEV (&job, &jobr, &m, a, &m, wr, vl, &m, vr, &m, wkopt, &lwork,rwork, &info);
#endif

  lwork = (blasint)wkopt[0];
  if (( work = (FLOAT *)malloc(sizeof(FLOAT) * lwork * COMPSIZE)) == NULL){
    fprintf(stderr,"Out of Memory!!\n");exit(1);
  }


#ifdef __linux
  srandom(getpid());
#endif

  fprintf(stderr, "   SIZE           FLops           Time          Lwork\n");

  for(m = from; m <= to; m += step){

    fprintf(stderr, " %6d : ", (int)m);
    begin();

    lwork = -1;
#ifndef COMPLEX
    GEEV (&job, &jobr, &m, a, &m, wr, wi, vl, &m, vr, &m, wkopt, &lwork, &info);
#else
    GEEV (&job, &jobr, &m, a, &m, wr, vl, &m, vr, &m, wkopt, &lwork,rwork, &info);
#endif

    lwork = (blasint)wkopt[0];
#ifndef COMPLEX
    GEEV (&job, &jobr, &m, a, &m, wr, wi, vl, &m, vr, &m, work, &lwork, &info);
#else
    GEEV (&job, &jobr, &m, a, &m, wr, vl, &m, vr, &m, work, &lwork,rwork, &info);
#endif

    end();

    if (info) {
      fprintf(stderr, "failed to compute eigenvalues .. %d\n", info);
      exit(1);
    }

    time1 = getsec();

    fprintf(stderr,
	    " %10.2f MFlops : %10.2f Sec : %d\n",
	    COMPSIZE * COMPSIZE * factor * (double)m * (double)m * (double)m / time1 * 1.e-6,time1,lwork);


  }

  return 0;
}

// void main(int argc, char *argv[]) __attribute__((weak, alias("MAIN__")));
