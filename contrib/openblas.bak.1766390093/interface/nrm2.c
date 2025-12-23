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

#include <stdio.h>
#include "common.h"
#ifdef FUNCTION_PROFILE
#include "functable.h"
#endif

#ifndef CBLAS

FLOATRET NAME(blasint *N, FLOAT *x, blasint *INCX){

  BLASLONG n    = *N;
  BLASLONG incx = *INCX;
  FLOATRET ret;

  PRINT_DEBUG_NAME;

  if (n <= 0) return 0.;

#ifndef COMPLEX
  if (n == 1)
#ifdef DOUBLE
    return fabs(x[0]);
#else
    return fabsf(x[0]);
#endif
#endif

  if (incx == 0)
#ifndef COMPLEX
#ifdef DOUBLE
  return (sqrt((double)n)*fabs(x[0]));
#else
  return (sqrt((float)n)*fabsf(x[0]));
#endif
#else
#ifdef DOUBLE
  {
  double fr=fabs(x[0]);
  double fi=fabs(x[1]);
  double fmin=MIN(fr,fi);
  double fmax=MAX(fr,fi);
  if (fmax==0.) return(fmax);
  if (fmax==fmin) return(sqrt((double)n)*sqrt(2.)*fmax);
  return (sqrt((double)n) * fmax * sqrt (1. + (fmin/fmax)*(fmin/fmax)));
  }
#else
  {
  float fr=fabs(x[0]);
  float fi=fabs(x[1]);
  float fmin=MIN(fr,fi);
  float fmax=MAX(fr,fi);
  if (fmax==0.) return(fmax);
  if (fmax==fmin) return(sqrt((float)n)*sqrt(2.)*fmax);
  return (sqrt((float)n) * fmax * sqrt (1. + (fmin/fmax)*(fmin/fmax)));
  }
#endif
#endif

  if (incx < 0) 
#ifdef COMPLEX    
    x -= (n - 1) * incx * 2;
#else
    x -= (n - 1) * incx;
#endif
  IDEBUG_START;

  FUNCTION_PROFILE_START();

  ret = (FLOATRET)NRM2_K(n, x, incx);

  FUNCTION_PROFILE_END(COMPSIZE, n, 2 * n);

  IDEBUG_END;

  return ret;
}

#else

#ifdef COMPLEX
FLOAT CNAME(blasint n, void *vx, blasint incx){
  FLOAT *x = (FLOAT*) vx;
#else
FLOAT CNAME(blasint n, FLOAT *x, blasint incx){
#endif

  FLOAT ret;

  PRINT_DEBUG_CNAME;

  if (n <= 0) return 0.;

#ifndef COMPLEX
  if (n == 1)
#ifdef DOUBLE
    return fabs(x[0]);
#else
    return fabsf(x[0]);
#endif
#endif

  if (incx == 0)
#ifndef COMPLEX
#ifdef DOUBLE
  return (sqrt((double)n)*fabs(x[0]));
#else
  return (sqrt((float)n)*fabsf(x[0]));
#endif
#else
#ifdef DOUBLE
  {
  double fr=fabs(x[0]);
  double fi=fabs(x[1]);
  double fmin=MIN(fr,fi);
  double fmax=MAX(fr,fi);
  if (fmax==0.) return(fmax);
  if (fmax==fmin) return(sqrt((double)n)*sqrt(2.)*fmax);
  return (sqrt((double)n) * fmax * sqrt (1. + (fmin/fmax)*(fmin/fmax)));
  }
#else
  {
  float fr=fabs(x[0]);
  float fi=fabs(x[1]);
  float fmin=MIN(fr,fi);
  float fmax=MAX(fr,fi);
  if (fmax==0.) return(fmax);
  if (fmax==fmin) return(sqrt((float)n)*sqrt(2.)*fmax);
  return (sqrt((float)n) * fmax * sqrt (1. + (fmin/fmax)*(fmin/fmax)));
  }
#endif
#endif

  if (incx < 0) 
#ifdef COMPLEX    
    x -= (n - 1) * incx * 2;
#else
    x -= (n - 1) * incx;
#endif
  
  IDEBUG_START;

  FUNCTION_PROFILE_START();

  ret = NRM2_K(n, x, incx);

  FUNCTION_PROFILE_END(COMPSIZE, n, 2 * n);

  IDEBUG_END;

  return ret;
}

#endif
