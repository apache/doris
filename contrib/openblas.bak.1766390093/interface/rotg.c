#include <math.h>
#include <float.h>
#include "common.h"
#ifdef FUNCTION_PROFILE
#include "functable.h"
#endif


#ifndef CBLAS

void NAME(FLOAT *DA, FLOAT *DB, FLOAT *C, FLOAT *S){

#else

void CNAME(FLOAT *DA, FLOAT *DB, FLOAT *C, FLOAT *S){

#endif

#ifdef DOUBLE
  long double safmin = DBL_MIN;
#else
  long double safmin = FLT_MIN;
#endif

#if defined(__i386__) || defined(__x86_64__) || defined(__ia64__) || defined(_M_X64) || defined(_M_IX86)

  long double da = *DA;
  long double db = *DB;
  long double c;
  long double s;
  long double r, z;
  long double sigma, dascal,dbscal;

  long double ada = fabsl(da);
  long double adb = fabsl(db);
  long double maxab = MAX(ada,adb);
  long double safmax;
  long double scale;


#ifndef CBLAS
  PRINT_DEBUG_NAME;
#else
  PRINT_DEBUG_CNAME;
#endif

  if (adb == ZERO) {
    *C = ONE;
    *S = ZERO;
    *DB = ZERO;
  } else if (ada == ZERO) {
    *C = ZERO;
    *S = ONE;
    *DA = *DB;
    *DB = ONE;
  } else {
  safmax = 1./safmin;
  scale = MIN(MAX(safmin,maxab), safmax);
    if (ada > adb)
	sigma = copysign(1.,da);
    else
	sigma = copysign(1.,db);
    dascal = da / scale;
    dbscal = db / scale;
    r = sigma * (scale * sqrt(dascal * dascal + dbscal * dbscal));
    c = da / r;
    s = db / r;
    z = ONE;
    if (ada > adb) z = s;
    if ((ada <= adb) && (c != ZERO)) z = ONE / c;

    *C = c;
    *S = s;
    *DA = r;
    *DB = z;
  }

#else
  FLOAT da = *DA;
  FLOAT db = *DB;
  FLOAT c  = *C;
  FLOAT s  = *S;
  FLOAT sigma;
  FLOAT r, z;

  FLOAT ada = fabs(da);
  FLOAT adb = fabs(db);
  FLOAT maxab = MAX(ada,adb);
  long double safmax ;
  FLOAT scale ;

  safmax = 1./safmin;
  scale = MIN(MAX(safmin,maxab), safmax);

  if (ada > adb)
	sigma = copysign(1.,da);
    else
	sigma = copysign(1.,db);

#ifndef CBLAS
  PRINT_DEBUG_NAME;
#else
  PRINT_DEBUG_CNAME;
#endif


  if (adb == ZERO) {
    *C = ONE;
    *S = ZERO;
    *DB = ZERO;
  } else if (ada == ZERO) {
    *C = ZERO;
    *S = ONE;
    *DA = *DB;
    *DB = ONE;
  } else {
    FLOAT aa = da / scale;
    FLOAT bb = db / scale;

    r = sigma * scale * sqrt(aa * aa + bb * bb);
    c = da / r;
    s = db / r;
    z = ONE;
    if (ada > adb) z = s;
    if ((ada <= adb) && (c != ZERO)) z = ONE / c;

    *C = c;
    *S = s;
    *DA = r;
    *DB = z;
  }
#endif

  return;
}
