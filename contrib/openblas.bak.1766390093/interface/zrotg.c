#include <math.h>
#include <float.h>
#include "common.h"
#ifdef FUNCTION_PROFILE
#include "functable.h"
#endif


#ifndef CBLAS
void NAME(FLOAT *DA, FLOAT *DB, FLOAT *C, FLOAT *S){

#else
void CNAME(void *VDA, void *VDB, FLOAT *C, void *VS) {
    FLOAT *DA = (FLOAT*) VDA;
    FLOAT *DB = (FLOAT*) VDB;
    FLOAT *S  = (FLOAT*) VS;
#endif /* CBLAS */

#ifdef DOUBLE
  long double safmin = DBL_MIN;
  long double rtmin = sqrt(DBL_MIN/DBL_EPSILON);
#else
  long double safmin = FLT_MIN;
  long double rtmin = sqrt(FLT_MIN/FLT_EPSILON);
#endif


  FLOAT da_r = *(DA+0);
  FLOAT da_i = *(DA+1);
  FLOAT db_r = *(DB+0);
  FLOAT db_i = *(DB+1);
  //long double r;
  FLOAT S1[2];
  FLOAT R[2];
  long double d;

  FLOAT ada =  da_r * da_r + da_i * da_i; 
  FLOAT adb =  db_r * db_r + db_i * db_i; 

  PRINT_DEBUG_NAME;

  IDEBUG_START;

  FUNCTION_PROFILE_START();

  if (db_r == ZERO && db_i == ZERO) {
    *C        = ONE;
    *(S  + 0) = ZERO;
    *(S  + 1) = ZERO;
    return;
  }

  long double safmax = 1./safmin;
#if defined DOUBLE
  long double rtmax = safmax /DBL_EPSILON;
#else
  long double rtmax = safmax /FLT_EPSILON;
#endif
		*(S1 + 0) = *(DB + 0);
		*(S1 + 1) = *(DB + 1) *-1;
	if (da_r == ZERO && da_i == ZERO) {
	    *C = ZERO;
	    if (db_r == ZERO) {
		    (*DA) = fabsl(db_i);
		*S = *S1 /(*DA);
		*(S+1) = *(S1+1) /(*DA);
		return;
	    } else if ( db_i == ZERO) {
		    *DA = fabsl(db_r);
		*S = *S1 /(*DA);
		*(S+1) = *(S1+1) /(*DA);
		return;
	    } else {
	        long double g1 = MAX( fabsl(db_r), fabsl(db_i));
	        rtmax =sqrt(safmax/2.);
	        if (g1 > rtmin && g1 < rtmax) { // unscaled
		    d = sqrt(adb);
		    *S = *S1 /d;
		    *(S+1) = *(S1+1) /d;
		    *DA = d ;
		    *(DA+1) = ZERO;
		    return;
    	        } else { // scaled algorithm
		  long double u = MIN ( safmax, MAX ( safmin, g1));
		    FLOAT gs_r = db_r/u;
		    FLOAT gs_i = db_i/u;
		    d = sqrt ( gs_r*gs_r + gs_i*gs_i);
		    *S = gs_r / d;
		    *(S + 1) = (gs_i * -1) / d;
		    *DA = d * u;
		    *(DA+1) = ZERO;
		    return;
	        }
	    }
	} else {
	       FLOAT f1 = MAX ( fabsl(da_r), fabsl(da_i));
	       FLOAT g1 = MAX ( fabsl(db_r), fabsl(db_i));
	       rtmax = sqrt(safmax / 4.);
	       if ( f1 > rtmin && f1 < rtmax && g1 > rtmin && g1 < rtmax) { //unscaled
		    long double h = ada + adb;
	   	    double adahsq = sqrt(ada * h);
		    if (ada >= h *safmin) {
			*C = sqrt(ada/h);
			*R = *DA / *C;
			*(R+1) = *(DA+1) / *C;
			rtmax *= 2.;
			if ( ada > rtmin && h < rtmax) { // no risk of intermediate overflow
				*S = *S1 * (*DA / adahsq) - *(S1+1)* (*(DA+1)/adahsq);
				*(S+1) = *S1 * (*(DA+1) / adahsq) + *(S1+1) * (*DA/adahsq);
			} else {
				*S = *S1 * (*R/h) - *(S1+1) * (*(R+1)/h);
				*(S+1) = *S1 * (*(R+1)/h) + *(S1+1) * (*(R)/h);
			}
	    	    } else {
		        *C = ada / adahsq;
		        if (*C >= safmin) {
			    *R = *DA / *C;
			    *(R+1) = *(DA+1) / *C;
			} else {
			    *R = *DA * (h / adahsq);
			    *(R+1) = *(DA+1) * (h / adahsq);
			}
		        *S = *S1 * ada / adahsq;
		    	*(S+1) = *(S1+1) * ada / adahsq;
		    }
		    *DA=*R;
		    *(DA+1)=*(R+1);
		    return;
	        } else {	// scaled
		   FLOAT fs_r, fs_i, gs_r, gs_i;
		long double v,w,f2,g2,h;
		long double u = MIN ( safmax, MAX ( safmin, MAX(f1,g1)));
		    gs_r = db_r/u;
		    gs_i = db_i/u;
		    g2 = sqrt ( gs_r*gs_r + gs_i*gs_i);
		    if (f1 /u < rtmin) {
			v = MIN (safmax, MAX (safmin, f1));
			w = v / u;
			fs_r = *DA/ v;
			fs_i = *(DA+1) / v;
		        f2 = sqrt ( fs_r*fs_r + fs_i*fs_i);
		        h = f2 * w * w + g2;		
		    } else { // use same scaling for both
			w = 1.;
			fs_r = *DA/ u;
			fs_i = *(DA+1) / u;
		        f2 = sqrt ( fs_r*fs_r + fs_i*fs_i);
			h = f2 + g2;
		    }
		    if ( f2 >= h * safmin) {
			    *C = sqrt ( f2 / h );
			    *DA = fs_r / *C;
			    *(DA+1) = fs_i / *C;
			    rtmax *= 2;
			    if ( f2 > rtmin && h < rtmax) {
				    *S = gs_r * (fs_r /sqrt(f2*h)) - gs_i * (fs_i / sqrt(f2*h));
				    *(S+1) = gs_r * (fs_i /sqrt(f2*h)) + gs_i * -1. * (fs_r / sqrt(f2*h));
			    } else {
				    *S = gs_r * (*DA/h) - gs_i * (*(DA+1) / h);
				    *(S+1) = gs_r * (*(DA+1) /h) + gs_i * -1. * (*DA / h);
			    }
		    } else { // intermediates might overflow
			d = sqrt ( f2 * h);
			*C = f2 /d;
			if (*C >= safmin) {
				*DA = fs_r / *C;
				*(DA+1) = fs_i / *C;
			} else {
				*DA = fs_r * (h / d);
				*(DA+1) = fs_i / (h / d);
			}
			*S = gs_r * (fs_r /d) - gs_i * (fs_i / d);
			*(S+1) = gs_r * (fs_i /d) + gs_i * -1. * (fs_r / d);
	            }
		    *C *= w;
		    *DA *= u;
		    *(DA+1) *= u;
		    return;
		}
	}
}
	
