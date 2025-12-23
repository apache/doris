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

#undef POTRF

#ifndef COMPLEX
#ifdef XDOUBLE
#define POTRF   BLASFUNC(qpotrf)
#define POTRS   BLASFUNC(qpotrs)
#define POTRI   BLASFUNC(qpotri)
#define SYRK    BLASFUNC(qsyrk)
#elif defined(DOUBLE)
#define POTRF   BLASFUNC(dpotrf)
#define POTRS   BLASFUNC(dpotrs)
#define POTRI   BLASFUNC(dpotri)
#define SYRK    BLASFUNC(dsyrk)
#else
#define POTRF   BLASFUNC(spotrf)
#define POTRS   BLASFUNC(spotrs)
#define POTRI   BLASFUNC(spotri)
#define SYRK    BLASFUNC(ssyrk)
#endif
#else
#ifdef XDOUBLE
#define POTRF   BLASFUNC(xpotrf)
#define POTRS   BLASFUNC(xpotrs)
#define POTRI   BLASFUNC(xpotri)
#define SYRK    BLASFUNC(xherk)
#elif defined(DOUBLE)
#define POTRF   BLASFUNC(zpotrf)
#define POTRS   BLASFUNC(zpotrs)
#define POTRI   BLASFUNC(zpotri)
#define SYRK    BLASFUNC(zherk)
#else
#define POTRF   BLASFUNC(cpotrf)
#define POTRS   BLASFUNC(cpotrs)
#define POTRI   BLASFUNC(cpotri)
#define SYRK    BLASFUNC(cherk)
#endif
#endif

// extern void POTRI(char *uplo, blasint *m, FLOAT *a, blasint *lda, blasint *info);
// extern void POTRS(char *uplo, blasint *m, blasint *n, FLOAT *a, blasint *lda, FLOAT *b, blasint *ldb, blasint *info);



int main(int argc, char *argv[]){

#ifndef COMPLEX
  char *trans[] = {"T", "N"};
#else
  char *trans[] = {"C", "N"};
#endif
  char *uplo[]  = {"U", "L"};
  FLOAT alpha[] = {1.0, 0.0};
  FLOAT beta [] = {0.0, 0.0};

  FLOAT *a, *b;

  char *p;
  char btest = 'F';

  blasint m, i, j, l, info, uplos=0;
  double flops = 0.;

  int from =   1;
  int to   = 200;
  int step =   1;
  int loops =  1;

  double time1, timeg;

  argc--;argv++;

  if (argc > 0) { from     = atol(*argv);		argc--; argv++;}
  if (argc > 0) { to       = MAX(atol(*argv), from);	argc--; argv++;}
  if (argc > 0) { step     = atol(*argv);		argc--; argv++;}

  if ((p = getenv("OPENBLAS_UPLO")))
	if (*p == 'L') uplos=1;

  if ((p = getenv("OPENBLAS_TEST"))) btest=*p;

  if ((p = getenv("OPENBLAS_LOOPS"))) loops=atoi(p);

  fprintf(stderr, "From : %3d  To : %3d Step = %3d Uplo = %c\n", from, to, step,*uplo[uplos]);

  if (( a    = (FLOAT *)malloc(sizeof(FLOAT) * to * to * COMPSIZE)) == NULL){
    fprintf(stderr,"Out of Memory!!\n");exit(1);
  }

  if (( b    = (FLOAT *)malloc(sizeof(FLOAT) * to * to * COMPSIZE)) == NULL){
    fprintf(stderr,"Out of Memory!!\n");exit(1);
  }


  for(m = from; m <= to; m += step){
    timeg=0.;
	  for (l = 0; l < loops; l++) {
#ifndef COMPLEX
      if (uplos & 1) {
	for (j = 0; j < m; j++) {
	  for(i = 0; i < j; i++)     a[(long)i + (long)j * (long)m] = 0.;
	  a[(long)j + (long)j * (long)m] = ((double) rand() / (double) RAND_MAX) + 8.;
	  for(i = j + 1; i < m; i++) a[(long)i + (long)j * (long)m] = ((double) rand() / (double) RAND_MAX) - 0.5;
	}
      } else {
	for (j = 0; j < m; j++) {
	  for(i = 0; i < j; i++)     a[(long)i + (long)j * (long)m] = ((double) rand() / (double) RAND_MAX) - 0.5;
	  a[(long)j + (long)j * (long)m] = ((double) rand() / (double) RAND_MAX) + 8.;
	  for(i = j + 1; i < m; i++) a[(long)i + (long)j * (long)m] = 0.;
	}
      }
#else
      if (uplos & 1) {
	for (j = 0; j < m; j++) {
	  for(i = 0; i < j; i++) {
	    a[((long)i + (long)j * (long)m) * 2 + 0] = 0.;
	    a[((long)i + (long)j * (long)m) * 2 + 1] = 0.;
	  }

	  a[((long)j + (long)j * (long)m) * 2 + 0] = ((double) rand() / (double) RAND_MAX) + 8.;
	  a[((long)j + (long)j * (long)m) * 2 + 1] = 0.;

	  for(i = j + 1; i < m; i++) {
	    a[((long)i + (long)j * (long)m) * 2 + 0] = 0;
	    a[((long)i + (long)j * (long)m) * 2 + 1] = ((double) rand() / (double) RAND_MAX) - 0.5;
	  }
	}
      } else {
	for (j = 0; j < m; j++) {
	  for(i = 0; i < j; i++) {
	    a[((long)i + (long)j * (long)m) * 2 + 0] = 0.;
	    a[((long)i + (long)j * (long)m) * 2 + 1] = ((double) rand() / (double) RAND_MAX) - 0.5;
	  }

	  a[((long)j + (long)j * (long)m) * 2 + 0] = ((double) rand() / (double) RAND_MAX) + 8.;
	  a[((long)j + (long)j * (long)m) * 2 + 1] = 0.;

	  for(i = j + 1; i < m; i++) {
	    a[((long)i + (long)j * (long)m) * 2 + 0] = 0.;
	    a[((long)i + (long)j * (long)m) * 2 + 1] = 0.;
	  }
	}
      }
#endif

      SYRK(uplo[uplos], trans[uplos], &m, &m, alpha, a, &m, beta, b, &m);

      begin();

      POTRF(uplo[uplos], &m, b, &m, &info);

      end();

      if (info != 0) {
	fprintf(stderr, "Potrf info = %d\n", info);
	exit(1);
      }

      if ( btest == 'F')
	      timeg += getsec();

      if ( btest == 'S' )
      {
	
 	for(j = 0; j < to; j++){
      		for(i = 0; i < to * COMPSIZE; i++){
        		a[i + j * to * COMPSIZE] = ((FLOAT) rand() / (FLOAT) RAND_MAX) - 0.5;
      		}
    	}

      	begin();

      	POTRS(uplo[uplos], &m, &m, b, &m, a, &m,  &info);

      	end();

      	if (info != 0) {
		fprintf(stderr, "Potrs info = %d\n", info);
		exit(1);
        }
        timeg += getsec();
      }
	
      if ( btest == 'I' )
      {
	
      	begin();

      	POTRI(uplo[uplos], &m, b, &m, &info);

      	end();

      	if (info != 0) {
		fprintf(stderr, "Potri info = %d\n", info);
		exit(1);
        }
        timeg += getsec();
      }
    } // loops

      time1 = timeg/(double)loops;
	if ( btest == 'F')
		flops = COMPSIZE * COMPSIZE * (1.0/3.0 * (double)m * (double)m *(double)m +1.0/2.0* (double)m *(double)m + 1.0/6.0* (double)m) / time1 * 1.e-6;
	if ( btest == 'S')
		flops = COMPSIZE * COMPSIZE * (2.0 * (double)m * (double)m *(double)m ) / time1 * 1.e-6;
	if ( btest == 'I')
		flops = COMPSIZE * COMPSIZE * (2.0/3.0 * (double)m * (double)m *(double)m +1.0/2.0* (double)m *(double)m + 5.0/6.0* (double)m) / time1 * 1.e-6;
      fprintf(stderr, "%8d : %10.2f MFlops : %10.3f Sec : Test=%c\n",m,flops ,time1,btest);


  }


  return 0;
}

// void main(int argc, char *argv[]) __attribute__((weak, alias("MAIN__")));

