#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <complex.h>
#ifdef complex
#undef complex
#endif
#ifdef I
#undef I
#endif

#include "common.h"

typedef blasint integer;

typedef unsigned int uinteger;
typedef char *address;
typedef short int shortint;
typedef float real;
typedef double doublereal;
typedef struct { real r, i; } complex;
typedef struct { doublereal r, i; } doublecomplex;
typedef int logical;
typedef short int shortlogical;
typedef char logical1;
typedef char integer1;

#define TRUE_ (1)
#define FALSE_ (0)

/* Extern is for use with -E */
#ifndef Extern
#define Extern extern
#endif

/* I/O stuff */

typedef int flag;
typedef int ftnlen;
typedef int ftnint;

/*external read, write*/
typedef struct
{	flag cierr;
	ftnint ciunit;
	flag ciend;
	char *cifmt;
	ftnint cirec;
} cilist;

/*internal read, write*/
typedef struct
{	flag icierr;
	char *iciunit;
	flag iciend;
	char *icifmt;
	ftnint icirlen;
	ftnint icirnum;
} icilist;

/*open*/
typedef struct
{	flag oerr;
	ftnint ounit;
	char *ofnm;
	ftnlen ofnmlen;
	char *osta;
	char *oacc;
	char *ofm;
	ftnint orl;
	char *oblnk;
} olist;

/*close*/
typedef struct
{	flag cerr;
	ftnint cunit;
	char *csta;
} cllist;

/*rewind, backspace, endfile*/
typedef struct
{	flag aerr;
	ftnint aunit;
} alist;

/* inquire */
typedef struct
{	flag inerr;
	ftnint inunit;
	char *infile;
	ftnlen infilen;
	ftnint	*inex;	/*parameters in standard's order*/
	ftnint	*inopen;
	ftnint	*innum;
	ftnint	*innamed;
	char	*inname;
	ftnlen	innamlen;
	char	*inacc;
	ftnlen	inacclen;
	char	*inseq;
	ftnlen	inseqlen;
	char 	*indir;
	ftnlen	indirlen;
	char	*infmt;
	ftnlen	infmtlen;
	char	*inform;
	ftnint	informlen;
	char	*inunf;
	ftnlen	inunflen;
	ftnint	*inrecl;
	ftnint	*innrec;
	char	*inblank;
	ftnlen	inblanklen;
} inlist;

#define VOID void

union Multitype {	/* for multiple entry points */
	integer1 g;
	shortint h;
	integer i;
	/* longint j; */
	real r;
	doublereal d;
	complex c;
	doublecomplex z;
	};

typedef union Multitype Multitype;

struct Vardesc {	/* for Namelist */
	char *name;
	char *addr;
	ftnlen *dims;
	int  type;
	};
typedef struct Vardesc Vardesc;

struct Namelist {
	char *name;
	Vardesc **vars;
	int nvars;
	};
typedef struct Namelist Namelist;

#define abs(x) ((x) >= 0 ? (x) : -(x))
#define dabs(x) (fabs(x))
#define f2cmin(a,b) ((a) <= (b) ? (a) : (b))
#define f2cmax(a,b) ((a) >= (b) ? (a) : (b))
#define dmin(a,b) (f2cmin(a,b))
#define dmax(a,b) (f2cmax(a,b))
#define bit_test(a,b)	((a) >> (b) & 1)
#define bit_clear(a,b)	((a) & ~((uinteger)1 << (b)))
#define bit_set(a,b)	((a) |  ((uinteger)1 << (b)))

#define abort_() { sig_die("Fortran abort routine called", 1); }
#define c_abs(z) (cabsf(Cf(z)))
#define c_cos(R,Z) { pCf(R)=ccos(Cf(Z)); }
#ifdef _MSC_VER
#define c_div(c, a, b) {Cf(c)._Val[0] = (Cf(a)._Val[0]/Cf(b)._Val[0]); Cf(c)._Val[1]=(Cf(a)._Val[1]/Cf(b)._Val[1]);}
#define z_div(c, a, b) {Cd(c)._Val[0] = (Cd(a)._Val[0]/Cd(b)._Val[0]); Cd(c)._Val[1]=(Cd(a)._Val[1]/Cd(b)._Val[1]);}
#else
#define c_div(c, a, b) {pCf(c) = Cf(a)/Cf(b);}
#define z_div(c, a, b) {pCd(c) = Cd(a)/Cd(b);}
#endif
#define c_exp(R, Z) {pCf(R) = cexpf(Cf(Z));}
#define c_log(R, Z) {pCf(R) = clogf(Cf(Z));}
#define c_sin(R, Z) {pCf(R) = csinf(Cf(Z));}
//#define c_sqrt(R, Z) {*(R) = csqrtf(Cf(Z));}
#define c_sqrt(R, Z) {pCf(R) = csqrtf(Cf(Z));}
#define d_abs(x) (fabs(*(x)))
#define d_acos(x) (acos(*(x)))
#define d_asin(x) (asin(*(x)))
#define d_atan(x) (atan(*(x)))
#define d_atn2(x, y) (atan2(*(x),*(y)))
#define d_cnjg(R, Z) { pCd(R) = conj(Cd(Z)); }
#define r_cnjg(R, Z) { pCf(R) = conjf(Cf(Z)); }
#define d_cos(x) (cos(*(x)))
#define d_cosh(x) (cosh(*(x)))
#define d_dim(__a, __b) ( *(__a) > *(__b) ? *(__a) - *(__b) : 0.0 )
#define d_exp(x) (exp(*(x)))
#define d_imag(z) (cimag(Cd(z)))
#define r_imag(z) (cimagf(Cf(z)))
#define d_int(__x) (*(__x)>0 ? floor(*(__x)) : -floor(- *(__x)))
#define r_int(__x) (*(__x)>0 ? floor(*(__x)) : -floor(- *(__x)))
#define d_lg10(x) ( 0.43429448190325182765 * log(*(x)) )
#define r_lg10(x) ( 0.43429448190325182765 * log(*(x)) )
#define d_log(x) (log(*(x)))
#define d_mod(x, y) (fmod(*(x), *(y)))
#define u_nint(__x) ((__x)>=0 ? floor((__x) + .5) : -floor(.5 - (__x)))
#define d_nint(x) u_nint(*(x))
#define u_sign(__a,__b) ((__b) >= 0 ? ((__a) >= 0 ? (__a) : -(__a)) : -((__a) >= 0 ? (__a) : -(__a)))
#define d_sign(a,b) u_sign(*(a),*(b))
#define r_sign(a,b) u_sign(*(a),*(b))
#define d_sin(x) (sin(*(x)))
#define d_sinh(x) (sinh(*(x)))
#define d_sqrt(x) (sqrt(*(x)))
#define d_tan(x) (tan(*(x)))
#define d_tanh(x) (tanh(*(x)))
#define i_abs(x) abs(*(x))
#define i_dnnt(x) ((integer)u_nint(*(x)))
#define i_len(s, n) (n)
#define i_nint(x) ((integer)u_nint(*(x)))
#define i_sign(a,b) ((integer)u_sign((integer)*(a),(integer)*(b)))
#define pow_dd(ap, bp) ( pow(*(ap), *(bp)))
#define pow_si(B,E) spow_ui(*(B),*(E))
#define pow_ri(B,E) spow_ui(*(B),*(E))
#define pow_di(B,E) dpow_ui(*(B),*(E))
#define pow_zi(p, a, b) {pCd(p) = zpow_ui(Cd(a), *(b));}
#define pow_ci(p, a, b) {pCf(p) = cpow_ui(Cf(a), *(b));}
#define pow_zz(R,A,B) {pCd(R) = cpow(Cd(A),*(B));}
#define s_cat(lpp, rpp, rnp, np, llp) { 	ftnlen i, nc, ll; char *f__rp, *lp; 	ll = (llp); lp = (lpp); 	for(i=0; i < (int)*(np); ++i) {         	nc = ll; 	        if((rnp)[i] < nc) nc = (rnp)[i]; 	        ll -= nc;         	f__rp = (rpp)[i]; 	        while(--nc >= 0) *lp++ = *(f__rp)++;         } 	while(--ll >= 0) *lp++ = ' '; }
#define s_cmp(a,b,c,d) ((integer)strncmp((a),(b),f2cmin((c),(d))))
#define s_copy(A,B,C,D) { int __i,__m; for (__i=0, __m=f2cmin((C),(D)); __i<__m && (B)[__i] != 0; ++__i) (A)[__i] = (B)[__i]; }
#define sig_die(s, kill) { exit(1); }
#define s_stop(s, n) {exit(0);}
#define z_abs(z) (cabs(Cd(z)))
#define z_exp(R, Z) {pCd(R) = cexp(Cd(Z));}
#define z_sqrt(R, Z) {pCd(R) = csqrt(Cd(Z));}
#define myexit_() break;
#define mycycle_() continue;
#define myceiling_(w) {ceil(w)}
#define myhuge_(w) {HUGE_VAL}
//#define mymaxloc_(w,s,e,n) {if (sizeof(*(w)) == sizeof(double)) dmaxloc_((w),*(s),*(e),n); else dmaxloc_((w),*(s),*(e),n);}
#define mymaxloc_(w,s,e,n) dmaxloc_(w,*(s),*(e),n)

/* procedure parameter types for -A and -C++ */

#define F2C_proc_par_types 1

/* Common Block Declarations */

struct {
    integer icase, n, incx, incy, mode;
    logical pass;
} combla_;

#define combla_1 combla_

/* Table of constant values */

static integer c__1 = 1;
static doublereal c_b34 = 1.;

/* Main program */ int main(void)
{
    /* Initialized data */

    static doublereal sfac = 9.765625e-4;

    /* Local variables */
    extern /* Subroutine */ int check0_(doublereal*), check1_(doublereal*), check2_(doublereal*), check3_(doublereal*);
    static integer ic;
    extern /* Subroutine */ int header_(void);

/*     Test program for the DOUBLE PRECISION Level 1 CBLAS. */
/*     Based upon the original CBLAS test routine together with: */
/*     F06EAF Example Program Text */
/*     .. Parameters .. */
/*     .. Scalars in Common .. */
/*     .. Local Scalars .. */
/*     .. External Subroutines .. */
/*     .. Common blocks .. */
/*     .. Data statements .. */
/*     .. Executable Statements .. */
    printf("Real CBLAS Test Program Results\n");
    for (ic = 1; ic <= 11; ++ic) {
	combla_1.icase = ic;
	header_();

/*        .. Initialize  PASS,  INCX,  INCY, and MODE for a new case. .. */
/*        .. the value 9999 for INCX, INCY or MODE will appear in the .. */
/*        .. detailed  output, if any, for cases  that do not involve .. */
/*        .. these parameters .. */

	combla_1.pass = TRUE_;
	combla_1.incx = 9999;
	combla_1.incy = 9999;
	combla_1.mode = 9999;
	if (combla_1.icase == 3) {
	    check0_(&sfac);
	} else if (combla_1.icase == 7 || combla_1.icase == 8 || 
		combla_1.icase == 9 || combla_1.icase == 10) {
	    check1_(&sfac);
	} else if (combla_1.icase == 1 || combla_1.icase == 2 || 
		combla_1.icase == 5 || combla_1.icase == 6) {
	    check2_(&sfac);
	} else if (combla_1.icase == 4 || combla_1.icase == 11) {
	    check3_(&sfac);
	}
/*        -- Print */
	if (combla_1.pass) {
	    printf("                                    ----- PASS -----\n");
	}
/* L20: */
    }
    exit(0);

} /* MAIN__ */

/* Subroutine */ int header_(void)
{
    /* Initialized data */

    static char l[15][13] = {"CBLAS_DDOT  " , "CBLAS_DAXPY " , "CBLAS_DROTG " ,    
    "CBLAS_DROT  " , "CBLAS_DCOPY " , "CBLAS_DSWAP " ,  "CBLAS_DNRM2 " , "CBLAS_DASUM ",
    "CBLAS_DSCAL " , "CBLAS_IDAMAX" , "CBLAS_DROTM "};

/*     .. Parameters .. */
/*     .. Scalars in Common .. */
/*     .. Local Arrays .. */
/*     .. Common blocks .. */
/*     .. Data statements .. */
/*     .. Executable Statements .. */
    printf("Test of subprogram number %3d         %15s", combla_1.icase, l[combla_1.icase -1]);
    return 0;

} /* header_ */

/* Subroutine */ int check0_(doublereal* sfac)
{
    /* Initialized data */

    static doublereal ds1[8] = { .8,.6,.8,-.6,.8,0.,1.,0. };
    static doublereal datrue[8] = { .5,.5,.5,-.5,-.5,0.,1.,1. };
    static doublereal dbtrue[8] = { 0.,.6,0.,-.6,0.,0.,1.,0. };
    static doublereal da1[8] = { .3,.4,-.3,-.4,-.3,0.,0.,1. };
    static doublereal db1[8] = { .4,.3,.4,.3,-.4,0.,1.,0. };
    static doublereal dc1[8] = { .6,.8,-.6,.8,.6,1.,0.,1. };

    /* Local variables */
    static integer k;
    extern /* Subroutine */ int drotgtest_(doublereal*,doublereal*,doublereal*,doublereal*), stest1_(doublereal*,doublereal*,doublereal*,doublereal*);
    static doublereal sa, sb, sc, ss;

/*     .. Parameters .. */
/*     .. Scalar Arguments .. */
/*     .. Scalars in Common .. */
/*     .. Local Scalars .. */
/*     .. Local Arrays .. */
/*     .. External Subroutines .. */
/*     .. Common blocks .. */
/*     .. Data statements .. */
/*     .. Executable Statements .. */

/*     Compute true values which cannot be prestored */
/*     in decimal notation */

    dbtrue[0] = 1.6666666666666667;
    dbtrue[2] = -1.6666666666666667;
    dbtrue[4] = 1.6666666666666667;

    for (k = 1; k <= 8; ++k) {
/*        .. Set N=K for identification in output if any .. */
	combla_1.n = k;
	if (combla_1.icase == 3) {
/*           .. DROTGTEST .. */
	    if (k > 8) {
		goto L40;
	    }
	    sa = da1[k - 1];
	    sb = db1[k - 1];
	    drotgtest_(&sa, &sb, &sc, &ss);
	    stest1_(&sa, &datrue[k - 1], &datrue[k - 1], sfac);
	    stest1_(&sb, &dbtrue[k - 1], &dbtrue[k - 1], sfac);
	    stest1_(&sc, &dc1[k - 1], &dc1[k - 1], sfac);
	    stest1_(&ss, &ds1[k - 1], &ds1[k - 1], sfac);
	} else {
	    fprintf(stderr, " Shouldn't be here in CHECK0\n");
	    exit(0);
	}
/* L20: */
    }
L40:
    return 0;
} /* check0_ */

/* Subroutine */ int check1_(doublereal* sfac)
{
    /* Initialized data */

    static doublereal sa[10] = { .3,-1.,0.,1.,.3,.3,.3,.3,.3,.3 };
    static doublereal dv[80]	/* was [8][5][2] */ = { .1,2.,2.,2.,2.,2.,2.,
	    2.,.3,3.,3.,3.,3.,3.,3.,3.,.3,-.4,4.,4.,4.,4.,4.,4.,.2,-.6,.3,5.,
	    5.,5.,5.,5.,.1,-.3,.5,-.1,6.,6.,6.,6.,.1,8.,8.,8.,8.,8.,8.,8.,.3,
	    9.,9.,9.,9.,9.,9.,9.,.3,2.,-.4,2.,2.,2.,2.,2.,.2,3.,-.6,5.,.3,2.,
	    2.,2.,.1,4.,-.3,6.,-.5,7.,-.1,3. };
    static doublereal dtrue1[5] = { 0.,.3,.5,.7,.6 };
    static doublereal dtrue3[5] = { 0.,.3,.7,1.1,1. };
    static doublereal dtrue5[80]	/* was [8][5][2] */ = { .1,2.,2.,2.,
	    2.,2.,2.,2.,-.3,3.,3.,3.,3.,3.,3.,3.,0.,0.,4.,4.,4.,4.,4.,4.,.2,
	    -.6,.3,5.,5.,5.,5.,5.,.03,-.09,.15,-.03,6.,6.,6.,6.,.1,8.,8.,8.,
	    8.,8.,8.,8.,.09,9.,9.,9.,9.,9.,9.,9.,.09,2.,-.12,2.,2.,2.,2.,2.,
	    .06,3.,-.18,5.,.09,2.,2.,2.,.03,4.,-.09,6.,-.15,7.,-.03,3. };
    static integer itrue2[5] = { 0,1,2,2,3 };

    /* System generated locals */
    integer i__1;
    doublereal d__1;

    /* Local variables */
    static integer i__;
    extern doublereal dnrm2test_(integer*, doublereal*, integer*);
    static doublereal stemp[1], strue[8];
    extern /* Subroutine */ int stest_(integer*,doublereal*,doublereal*,doublereal*,doublereal*), dscaltest_(integer*,doublereal*,doublereal*,integer*);
    extern doublereal dasumtest_(integer*,doublereal*,integer*);
    extern /* Subroutine */ int itest1_(integer*,integer*), stest1_(doublereal*,doublereal*,doublereal*,doublereal*);
    static doublereal sx[8];
    static integer np1;
    extern integer idamaxtest_(integer*,doublereal*,integer*);
    static integer len;

/*     .. Parameters .. */
/*     .. Scalar Arguments .. */
/*     .. Scalars in Common .. */
/*     .. Local Scalars .. */
/*     .. Local Arrays .. */
/*     .. External Functions .. */
/*     .. External Subroutines .. */
/*     .. Intrinsic Functions .. */
/*     .. Common blocks .. */
/*     .. Data statements .. */
/*     .. Executable Statements .. */
    for (combla_1.incx = 1; combla_1.incx <= 2; ++combla_1.incx) {
	for (np1 = 1; np1 <= 5; ++np1) {
	    combla_1.n = np1 - 1;
	    len = f2cmax(combla_1.n,1) << 1;
/*           .. Set vector arguments .. */
	    i__1 = len;
	    for (i__ = 1; i__ <= i__1; ++i__) {
		sx[i__ - 1] = dv[i__ + (np1 + combla_1.incx * 5 << 3) - 49];
/* L20: */
	    }

	    if (combla_1.icase == 7) {
/*              .. DNRM2TEST .. */
		stemp[0] = dtrue1[np1 - 1];
		d__1 = dnrm2test_(&combla_1.n, sx, &combla_1.incx);
		stest1_(&d__1, stemp, stemp, sfac);
	    } else if (combla_1.icase == 8) {
/*              .. DASUMTEST .. */
		stemp[0] = dtrue3[np1 - 1];
		d__1 = dasumtest_(&combla_1.n, sx, &combla_1.incx);
		stest1_(&d__1, stemp, stemp, sfac);
	    } else if (combla_1.icase == 9) {
/*              .. DSCALTEST .. */
		dscaltest_(&combla_1.n, &sa[(combla_1.incx - 1) * 5 + np1 - 1]
			, sx, &combla_1.incx);
		i__1 = len;
		for (i__ = 1; i__ <= i__1; ++i__) {
		    strue[i__ - 1] = dtrue5[i__ + (np1 + combla_1.incx * 5 << 
			    3) - 49];
/* L40: */
		}
		stest_(&len, sx, strue, strue, sfac);
	    } else if (combla_1.icase == 10) {
/*              .. IDAMAXTEST .. */
		i__1 = idamaxtest_(&combla_1.n, sx, &combla_1.incx);
		itest1_(&i__1, &itrue2[np1 - 1]);
	    } else {
		fprintf(stderr, " Shouldn't be here in CHECK1\n");
		exit(0);
	    }
/* L60: */
	}
/* L80: */
    }
    return 0;
} /* check1_ */

/* Subroutine */ int check2_(doublereal* sfac)
{
    /* Initialized data */

    static doublereal sa = .3;
    static integer incxs[4] = { 1,2,-2,-1 };
    static integer incys[4] = { 1,-2,1,-2 };
    static integer lens[8]	/* was [4][2] */ = { 1,1,2,4,1,1,3,7 };
    static integer ns[4] = { 0,1,2,4 };
    static doublereal dx1[7] = { .6,.1,-.5,.8,.9,-.3,-.4 };
    static doublereal dy1[7] = { .5,-.9,.3,.7,-.6,.2,.8 };
    static doublereal dt7[16]	/* was [4][4] */ = { 0.,.3,.21,.62,0.,.3,-.07,
	    .85,0.,.3,-.79,-.74,0.,.3,.33,1.27 };
    static doublereal dt8[112]	/* was [7][4][4] */ = { .5,0.,0.,0.,0.,0.,0.,
	    .68,0.,0.,0.,0.,0.,0.,.68,-.87,0.,0.,0.,0.,0.,.68,-.87,.15,.94,0.,
	    0.,0.,.5,0.,0.,0.,0.,0.,0.,.68,0.,0.,0.,0.,0.,0.,.35,-.9,.48,0.,
	    0.,0.,0.,.38,-.9,.57,.7,-.75,.2,.98,.5,0.,0.,0.,0.,0.,0.,.68,0.,
	    0.,0.,0.,0.,0.,.35,-.72,0.,0.,0.,0.,0.,.38,-.63,.15,.88,0.,0.,0.,
	    .5,0.,0.,0.,0.,0.,0.,.68,0.,0.,0.,0.,0.,0.,.68,-.9,.33,0.,0.,0.,
	    0.,.68,-.9,.33,.7,-.75,.2,1.04 };
    static doublereal dt10x[112]	/* was [7][4][4] */ = { .6,0.,0.,0.,
	    0.,0.,0.,.5,0.,0.,0.,0.,0.,0.,.5,-.9,0.,0.,0.,0.,0.,.5,-.9,.3,.7,
	    0.,0.,0.,.6,0.,0.,0.,0.,0.,0.,.5,0.,0.,0.,0.,0.,0.,.3,.1,.5,0.,0.,
	    0.,0.,.8,.1,-.6,.8,.3,-.3,.5,.6,0.,0.,0.,0.,0.,0.,.5,0.,0.,0.,0.,
	    0.,0.,-.9,.1,.5,0.,0.,0.,0.,.7,.1,.3,.8,-.9,-.3,.5,.6,0.,0.,0.,0.,
	    0.,0.,.5,0.,0.,0.,0.,0.,0.,.5,.3,0.,0.,0.,0.,0.,.5,.3,-.6,.8,0.,
	    0.,0. };
    static doublereal dt10y[112]	/* was [7][4][4] */ = { .5,0.,0.,0.,
	    0.,0.,0.,.6,0.,0.,0.,0.,0.,0.,.6,.1,0.,0.,0.,0.,0.,.6,.1,-.5,.8,
	    0.,0.,0.,.5,0.,0.,0.,0.,0.,0.,.6,0.,0.,0.,0.,0.,0.,-.5,-.9,.6,0.,
	    0.,0.,0.,-.4,-.9,.9,.7,-.5,.2,.6,.5,0.,0.,0.,0.,0.,0.,.6,0.,0.,0.,
	    0.,0.,0.,-.5,.6,0.,0.,0.,0.,0.,-.4,.9,-.5,.6,0.,0.,0.,.5,0.,0.,0.,
	    0.,0.,0.,.6,0.,0.,0.,0.,0.,0.,.6,-.9,.1,0.,0.,0.,0.,.6,-.9,.1,.7,
	    -.5,.2,.8 };
    static doublereal ssize1[4] = { 0.,.3,1.6,3.2 };
    static doublereal ssize2[28]	/* was [14][2] */ = { 0.,0.,0.,0.,0.,
	    0.,0.,0.,0.,0.,0.,0.,0.,0.,1.17,1.17,1.17,1.17,1.17,1.17,1.17,
	    1.17,1.17,1.17,1.17,1.17,1.17,1.17 };

    /* System generated locals */
    integer i__1;
    doublereal d__1;

    /* Local variables */
    static integer lenx, leny;
    extern doublereal ddottest_(integer*,doublereal*,integer*,doublereal*,integer*);
    static integer i__, j, ksize;
    extern /* Subroutine */ int stest_(integer*,doublereal*,doublereal*,doublereal*,doublereal*), dcopytest_(integer*,doublereal*,integer*,doublereal*,integer*), dswaptest_(integer*,doublereal*,integer*,doublereal*,integer*), 
	    daxpytest_(integer*,doublereal*,doublereal*,integer*,doublereal*,integer*), stest1_(doublereal*,doublereal*,doublereal*,doublereal*);
    static integer ki, kn, mx, my;
    static doublereal sx[7], sy[7], stx[7], sty[7];

/*     .. Parameters .. */
/*     .. Scalar Arguments .. */
/*     .. Scalars in Common .. */
/*     .. Local Scalars .. */
/*     .. Local Arrays .. */
/*     .. External Functions .. */
/*     .. External Subroutines .. */
/*     .. Intrinsic Functions .. */
/*     .. Common blocks .. */
/*     .. Data statements .. */
/*     .. Executable Statements .. */

    for (ki = 1; ki <= 4; ++ki) {
	combla_1.incx = incxs[ki - 1];
	combla_1.incy = incys[ki - 1];
	mx = abs(combla_1.incx);
	my = abs(combla_1.incy);

	for (kn = 1; kn <= 4; ++kn) {
	    combla_1.n = ns[kn - 1];
	    ksize = f2cmin(2,kn);
	    lenx = lens[kn + (mx << 2) - 5];
	    leny = lens[kn + (my << 2) - 5];
/*           .. Initialize all argument arrays .. */
	    for (i__ = 1; i__ <= 7; ++i__) {
		sx[i__ - 1] = dx1[i__ - 1];
		sy[i__ - 1] = dy1[i__ - 1];
/* L20: */
	    }

	    if (combla_1.icase == 1) {
/*              .. DDOTTEST .. */
		d__1 = ddottest_(&combla_1.n, sx, &combla_1.incx, sy, &
			combla_1.incy);
		stest1_(&d__1, &dt7[kn + (ki << 2) - 5], &ssize1[kn - 1], 
			sfac);
	    } else if (combla_1.icase == 2) {
/*              .. DAXPYTEST .. */
		daxpytest_(&combla_1.n, &sa, sx, &combla_1.incx, sy, &
			combla_1.incy);
		i__1 = leny;
		for (j = 1; j <= i__1; ++j) {
		    sty[j - 1] = dt8[j + (kn + (ki << 2)) * 7 - 36];
/* L40: */
		}
		stest_(&leny, sy, sty, &ssize2[ksize * 14 - 14], sfac);
	    } else if (combla_1.icase == 5) {
/*              .. DCOPYTEST .. */
		for (i__ = 1; i__ <= 7; ++i__) {
		    sty[i__ - 1] = dt10y[i__ + (kn + (ki << 2)) * 7 - 36];
/* L60: */
		}
		dcopytest_(&combla_1.n, sx, &combla_1.incx, sy, &
			combla_1.incy);
		stest_(&leny, sy, sty, ssize2, &c_b34);
	    } else if (combla_1.icase == 6) {
/*              .. DSWAPTEST .. */
		dswaptest_(&combla_1.n, sx, &combla_1.incx, sy, &
			combla_1.incy);
		for (i__ = 1; i__ <= 7; ++i__) {
		    stx[i__ - 1] = dt10x[i__ + (kn + (ki << 2)) * 7 - 36];
		    sty[i__ - 1] = dt10y[i__ + (kn + (ki << 2)) * 7 - 36];
/* L80: */
		}
		stest_(&lenx, sx, stx, ssize2, &c_b34);
		stest_(&leny, sy, sty, ssize2, &c_b34);
	    } else {
		fprintf(stderr," Shouldn't be here in CHECK2\n");
		exit(0);
	    }
/* L100: */
	}
/* L120: */
    }
    return 0;
} /* check2_ */

/* Subroutine */ int check3_(doublereal* sfac)
{
    /* Initialized data */

    static integer incxs[7] = { 1,1,2,2,-2,-1,-2 };
    static integer incys[7] = { 1,2,2,-2,1,-2,-2 };
    static integer ns[5] = { 0,1,2,4,5 };
    static doublereal dx[10] = { .6,.1,-.5,.8,.9,-.3,-.4,.7,.5,.2 };
    static doublereal dy[10] = { .5,-.9,.3,.7,-.6,.2,.8,-.5,.1,-.3 };
    static doublereal sc = .8;
    static doublereal ss = .6;
    static integer len = 10;
    static doublereal param[20]	/* was [5][4] */ = { -2.,1.,0.,0.,1.,-1.,.2,
	    .3,.4,.5,0.,1.,.3,.4,1.,1.,.2,-1.,1.,.5 };
    static doublereal ssize2[20]	/* was [10][2] */ = { 0.,0.,0.,0.,0.,
	    0.,0.,0.,0.,0.,1.17,1.17,1.17,1.17,1.17,1.17,1.17,1.17,1.17,1.17 }
	    ;

    /* Local variables */
    extern /* Subroutine */ int drottest_(integer*,doublereal*,integer*,doublereal*,integer*,doublereal*,doublereal*);
    static integer i__, k, ksize;
    extern /* Subroutine */int stest_(integer*,doublereal*,doublereal*,doublereal*,doublereal*), drotmtest_(integer*,doublereal*,integer*,doublereal*,integer*,doublereal*);
    static integer ki, kn;
    static doublereal dparam[5], sx[10], sy[10], stx[10], sty[10];

/*     .. Parameters .. */
/*     .. Scalar Arguments .. */
/*     .. Scalars in Common .. */
/*     .. Local Scalars .. */
/*     .. Local Arrays .. */
/*     .. External Subroutines .. */
/*     .. Intrinsic Functions .. */
/*     .. Common blocks .. */
/*     .. Data statements .. */
/*     .. Executable Statements .. */

    for (ki = 1; ki <= 7; ++ki) {
	combla_1.incx = incxs[ki - 1];
	combla_1.incy = incys[ki - 1];

	for (kn = 1; kn <= 5; ++kn) {
	    combla_1.n = ns[kn - 1];
	    ksize = f2cmin(2,kn);

	    if (combla_1.icase == 4) {
/*              .. DROTTEST .. */
		for (i__ = 1; i__ <= 10; ++i__) {
		    sx[i__ - 1] = dx[i__ - 1];
		    sy[i__ - 1] = dy[i__ - 1];
		    stx[i__ - 1] = dx[i__ - 1];
		    sty[i__ - 1] = dy[i__ - 1];
/* L20: */
		}
		drottest_(&combla_1.n, sx, &combla_1.incx, sy, &combla_1.incy,
			 &sc, &ss);
		drot_(&combla_1.n, stx, &combla_1.incx, sty, &combla_1.incy, &
			sc, &ss);
		stest_(&len, sx, stx, &ssize2[ksize * 10 - 10], sfac);
		stest_(&len, sy, sty, &ssize2[ksize * 10 - 10], sfac);
	    } else if (combla_1.icase == 11) {
/*              .. DROTMTEST .. */
		for (i__ = 1; i__ <= 10; ++i__) {
		    sx[i__ - 1] = dx[i__ - 1];
		    sy[i__ - 1] = dy[i__ - 1];
		    stx[i__ - 1] = dx[i__ - 1];
		    sty[i__ - 1] = dy[i__ - 1];
/* L90: */
		}
		for (i__ = 1; i__ <= 4; ++i__) {
		    for (k = 1; k <= 5; ++k) {
			dparam[k - 1] = param[k + i__ * 5 - 6];
/* L80: */
		    }
		    drotmtest_(&combla_1.n, sx, &combla_1.incx, sy, &
			    combla_1.incy, dparam);
		    drotm_(&combla_1.n, stx, &combla_1.incx, sty, &
			    combla_1.incy, dparam);
		    stest_(&len, sx, stx, &ssize2[ksize * 10 - 10], sfac);
		    stest_(&len, sy, sty, &ssize2[ksize * 10 - 10], sfac);
/* L70: */
		}
	    } else {
		fprintf(stderr," Shouldn't be here in CHECK3\n");
		exit(0);
	    }
/* L40: */
	}
/* L60: */
    }
    return 0;
} /* check3_ */

/* Subroutine */ int stest_(integer* len, doublereal* scomp, doublereal* strue, doublereal* ssize, doublereal* sfac)
{
    /* System generated locals */
    integer i__1;
    doublereal d__1, d__2, d__3, d__4, d__5;

    /* Local variables */
    static integer i__;
    extern doublereal sdiff_(doublereal*,doublereal*);
    static doublereal sd;

/*     ********************************* STEST ************************** */

/*     THIS SUBR COMPARES ARRAYS  SCOMP() AND STRUE() OF LENGTH LEN TO */
/*     SEE IF THE TERM BY TERM DIFFERENCES, MULTIPLIED BY SFAC, ARE */
/*     NEGLIGIBLE. */

/*     C. L. LAWSON, JPL, 1974 DEC 10 */

/*     .. Parameters .. */
/*     .. Scalar Arguments .. */
/*     .. Array Arguments .. */
/*     .. Scalars in Common .. */
/*     .. Local Scalars .. */
/*     .. External Functions .. */
/*     .. Intrinsic Functions .. */
/*     .. Common blocks .. */
/*     .. Executable Statements .. */

    /* Parameter adjustments */
    --ssize;
    --strue;
    --scomp;

    /* Function Body */
    i__1 = *len;
    for (i__ = 1; i__ <= i__1; ++i__) {
	sd = scomp[i__] - strue[i__];
	d__4 = (d__1 = ssize[i__], abs(d__1)) + (d__2 = *sfac * sd, abs(d__2))
		;
	d__5 = (d__3 = ssize[i__], abs(d__3));
	if (sdiff_(&d__4, &d__5) == 0.) {
	    goto L40;
	}

/*                             HERE    SCOMP(I) IS NOT CLOSE TO STRUE(I). */

	if (! combla_1.pass) {
	    goto L20;
	}
/*                             PRINT FAIL MESSAGE AND HEADER. */
	combla_1.pass = FALSE_;
	printf("                                       FAIL\n");
	printf("CASE  N INCX INCY MODE  I                           COMP(I)                             TRUE(I)  DIFFERENCE     SIZE(I)\n");
L20:
	printf("%4d %3d %5d %5d %5d %3d %36.8f %36.8f %12.4f %12.4f\n",combla_1.icase, combla_1.n, combla_1.incx, combla_1.incy, combla_1.mode,
	i__, scomp[i__], strue[i__], sd, ssize[i__]);
L40:
	;
    }
    return 0;

} /* stest_ */

/* Subroutine */ int stest1_(doublereal* scomp1, doublereal* strue1, doublereal* ssize, doublereal* sfac)
{
    static doublereal scomp[1], strue[1];
    extern /* Subroutine */ int stest_(integer*, doublereal*, doublereal*, doublereal*, doublereal*);

/*     ************************* STEST1 ***************************** */

/*     THIS IS AN INTERFACE SUBROUTINE TO ACCOMMODATE THE FORTRAN */
/*     REQUIREMENT THAT WHEN A DUMMY ARGUMENT IS AN ARRAY, THE */
/*     ACTUAL ARGUMENT MUST ALSO BE AN ARRAY OR AN ARRAY ELEMENT. */

/*     C.L. LAWSON, JPL, 1978 DEC 6 */

/*     .. Scalar Arguments .. */
/*     .. Array Arguments .. */
/*     .. Local Arrays .. */
/*     .. External Subroutines .. */
/*     .. Executable Statements .. */

    /* Parameter adjustments */
    --ssize;

    /* Function Body */
    scomp[0] = *scomp1;
    strue[0] = *strue1;
    stest_(&c__1, scomp, strue, &ssize[1], sfac);

    return 0;
} /* stest1_ */

doublereal sdiff_(doublereal* sa, doublereal* sb)
{
    /* System generated locals */
    doublereal ret_val;

/*     ********************************* SDIFF ************************** */
/*     COMPUTES DIFFERENCE OF TWO NUMBERS.  C. L. LAWSON, JPL 1974 FEB 15 */

/*     .. Scalar Arguments .. */
/*     .. Executable Statements .. */
    ret_val = *sa - *sb;
    return ret_val;
} /* sdiff_ */

/* Subroutine */ int itest1_(integer* icomp, integer* itrue)
{
    /* Local variables */
    static integer id;

/*     ********************************* ITEST1 ************************* */

/*     THIS SUBROUTINE COMPARES THE VARIABLES ICOMP AND ITRUE FOR */
/*     EQUALITY. */
/*     C. L. LAWSON, JPL, 1974 DEC 10 */

/*     .. Parameters .. */
/*     .. Scalar Arguments .. */
/*     .. Scalars in Common .. */
/*     .. Local Scalars .. */
/*     .. Common blocks .. */
/*     .. Executable Statements .. */

    if (*icomp == *itrue) {
	goto L40;
    }

/*                            HERE ICOMP IS NOT EQUAL TO ITRUE. */

    if (! combla_1.pass) {
	goto L20;
    }
/*                             PRINT FAIL MESSAGE AND HEADER. */
    combla_1.pass = FALSE_;
    printf("                                       FAILn");
    printf("(CASE  N INCX INCY MODE                                COMP                                TRUE     DIFFERENCE\n");
L20:
    id = *icomp - *itrue;
    printf("%4d %3d %5d %5d %5d %36d %36d %12d\n",combla_1.icase, combla_1.n, combla_1.incx, combla_1.incy, 
    combla_1.mode, *icomp, *itrue, id);
L40:
    return 0;

} /* itest1_ */

#if 0
/* Subroutine */ int drot_(n, dx, incx, dy, incy, c__, s)
integer *n;
doublereal *dx;
integer *incx;
doublereal *dy;
integer *incy;
doublereal *c__, *s;
{
    /* System generated locals */
    integer i__1;

    /* Local variables */
    static integer i__;
    static doublereal dtemp;
    static integer ix, iy;

/*     .. Scalar Arguments .. */
/*     .. */
/*     .. Array Arguments .. */
/*     .. */
/*     applies a plane rotation. */
/*     jack dongarra, linpack, 3/11/78. */
/*     modified 12/3/93, array(1) declarations changed to array(*) */

/*     .. Local Scalars .. */
/*     .. */
    /* Parameter adjustments */
    --dy;
    --dx;

    /* Function Body */
    if (*n <= 0) {
	return 0;
    }
    if (*incx == 1 && *incy == 1) {
	goto L20;
    }
    ix = 1;
    iy = 1;
    if (*incx < 0) {
	ix = (-(*n) + 1) * *incx + 1;
    }
    if (*incy < 0) {
	iy = (-(*n) + 1) * *incy + 1;
    }
    i__1 = *n;
    for (i__ = 1; i__ <= i__1; ++i__) {
	dtemp = *c__ * dx[ix] + *s * dy[iy];
	dy[iy] = *c__ * dy[iy] - *s * dx[ix];
	dx[ix] = dtemp;
	ix += *incx;
	iy += *incy;
/* L10: */
    }
    return 0;
L20:
    i__1 = *n;
    for (i__ = 1; i__ <= i__1; ++i__) {
	dtemp = *c__ * dx[i__] + *s * dy[i__];
	dy[i__] = *c__ * dy[i__] - *s * dx[i__];
	dx[i__] = dtemp;
/* L30: */
    }
    return 0;
} /* drot_ */

/* Subroutine */ int drotm_(n, dx, incx, dy, incy, dparam)
integer *n;
doublereal *dx;
integer *incx;
doublereal *dy;
integer *incy;
doublereal *dparam;
{
    /* Initialized data */

    static doublereal zero = 0.;
    static doublereal two = 2.;

    /* System generated locals */
    integer i__1, i__2;

    /* Local variables */
    static integer i__;
    static doublereal dflag, w, z__;
    static integer kx, ky, nsteps;
    static doublereal dh11, dh12, dh21, dh22;


/*  -- Reference BLAS level1 routine (version 3.8.0) -- */
/*  -- Reference BLAS is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     November 2017 */

/*     .. Scalar Arguments .. */
/*     .. */
/*     .. Array Arguments .. */
/*     .. */

/*  ===================================================================== */

/*     .. Local Scalars .. */
/*     .. */
/*     .. Data statements .. */
    /* Parameter adjustments */
    --dparam;
    --dy;
    --dx;

    /* Function Body */
/*     .. */

    dflag = dparam[1];
    if (*n <= 0 || dflag + two == zero) {
	return 0;
    }
    if (*incx == *incy && *incx > 0) {

	nsteps = *n * *incx;
	if (dflag < zero) {
	    dh11 = dparam[2];
	    dh12 = dparam[4];
	    dh21 = dparam[3];
	    dh22 = dparam[5];
	    i__1 = nsteps;
	    i__2 = *incx;
	    for (i__ = 1; i__2 < 0 ? i__ >= i__1 : i__ <= i__1; i__ += i__2) {
		w = dx[i__];
		z__ = dy[i__];
		dx[i__] = w * dh11 + z__ * dh12;
		dy[i__] = w * dh21 + z__ * dh22;
	    }
	} else if (dflag == zero) {
	    dh12 = dparam[4];
	    dh21 = dparam[3];
	    i__2 = nsteps;
	    i__1 = *incx;
	    for (i__ = 1; i__1 < 0 ? i__ >= i__2 : i__ <= i__2; i__ += i__1) {
		w = dx[i__];
		z__ = dy[i__];
		dx[i__] = w + z__ * dh12;
		dy[i__] = w * dh21 + z__;
	    }
	} else {
	    dh11 = dparam[2];
	    dh22 = dparam[5];
	    i__1 = nsteps;
	    i__2 = *incx;
	    for (i__ = 1; i__2 < 0 ? i__ >= i__1 : i__ <= i__1; i__ += i__2) {
		w = dx[i__];
		z__ = dy[i__];
		dx[i__] = w * dh11 + z__;
		dy[i__] = -w + dh22 * z__;
	    }
	}
    } else {
	kx = 1;
	ky = 1;
	if (*incx < 0) {
	    kx = (1 - *n) * *incx + 1;
	}
	if (*incy < 0) {
	    ky = (1 - *n) * *incy + 1;
	}

	if (dflag < zero) {
	    dh11 = dparam[2];
	    dh12 = dparam[4];
	    dh21 = dparam[3];
	    dh22 = dparam[5];
	    i__2 = *n;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		w = dx[kx];
		z__ = dy[ky];
		dx[kx] = w * dh11 + z__ * dh12;
		dy[ky] = w * dh21 + z__ * dh22;
		kx += *incx;
		ky += *incy;
	    }
	} else if (dflag == zero) {
	    dh12 = dparam[4];
	    dh21 = dparam[3];
	    i__2 = *n;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		w = dx[kx];
		z__ = dy[ky];
		dx[kx] = w + z__ * dh12;
		dy[ky] = w * dh21 + z__;
		kx += *incx;
		ky += *incy;
	    }
	} else {
	    dh11 = dparam[2];
	    dh22 = dparam[5];
	    i__2 = *n;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		w = dx[kx];
		z__ = dy[ky];
		dx[kx] = w * dh11 + z__;
		dy[ky] = -w + dh22 * z__;
		kx += *incx;
		ky += *incy;
	    }
	}
    }
    return 0;
} /* drotm_ */

#endif
