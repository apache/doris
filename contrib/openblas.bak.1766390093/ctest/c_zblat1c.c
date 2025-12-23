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
#ifdef _MSC_VER
static inline _Fcomplex Cf(complex *z) {_Fcomplex zz={z->r , z->i}; return zz;}
static inline _Dcomplex Cd(doublecomplex *z) {_Dcomplex zz={z->r , z->i};return zz;}
static inline _Fcomplex * _pCf(complex *z) {return (_Fcomplex*)z;}
static inline _Dcomplex * _pCd(doublecomplex *z) {return (_Dcomplex*)z;}
#else
static inline _Complex float Cf(complex *z) {return z->r + z->i*_Complex_I;}
static inline _Complex double Cd(doublecomplex *z) {return z->r + z->i*_Complex_I;}
static inline _Complex float * _pCf(complex *z) {return (_Complex float*)z;}
static inline _Complex double * _pCd(doublecomplex *z) {return (_Complex double*)z;}
#endif
#define pCf(z) (*_pCf(z))
#define pCd(z) (*_pCd(z))
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
static integer c__5 = 5;
static doublereal c_b43 = 1.;

/* Main program */ int main(void)
{
    /* Initialized data */

    static doublereal sfac = 9.765625e-4;

    /* Local variables */
    extern /* Subroutine */ int check1_(doublereal*), check2_(doublereal*);
    static integer ic;
    extern /* Subroutine */ int header_(void);

/*     Test program for the COMPLEX*16 Level 1 CBLAS. */
/*     Based upon the original CBLAS test routine together with: */
/*     F06GAF Example Program Text */
/*     .. Parameters .. */
/*     .. Scalars in Common .. */
/*     .. Local Scalars .. */
/*     .. External Subroutines .. */
/*     .. Common blocks .. */
/*     .. Data statements .. */
/*     .. Executable Statements .. */
    printf("Complex CBLAS Test Program Results\n");
    for (ic = 1; ic <= 10; ++ic) {
	combla_1.icase = ic;
	header_();

/*        Initialize PASS, INCX, INCY, and MODE for a new case. */
/*        The value 9999 for INCX, INCY or MODE will appear in the */
/*        detailed  output, if any, for cases that do not involve */
/*        these parameters. */

	combla_1.pass = TRUE_;
	combla_1.incx = 9999;
	combla_1.incy = 9999;
	combla_1.mode = 9999;
	if (combla_1.icase <= 5) {
	    check2_(&sfac);
	} else if (combla_1.icase >= 6) {
	    check1_(&sfac);
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

    static char l[15][13] = { "CBLAS_ZDOTC " , "CBLAS_ZDOTU " , "CBLAS_ZAXPY " ,
    "CBLAS_ZCOPY " , "CBLAS_ZSWAP " , "CBLAS_DZNRM2" , "CBLAS_DZASUM" , 
    "CBLAS_ZSCAL " , "CBLAS_ZDSCAL" , "CBLAS_IZAMAX" };

/*     .. Parameters .. */
/*     .. Scalars in Common .. */
/*     .. Local Arrays .. */
/*     .. Common blocks .. */
/*     .. Data statements .. */
/*     .. Executable Statements .. */
    printf("Test of subprogram number %3d         %15s", combla_1.icase, l[combla_1.icase-1]);
    return 0;

} /* header_ */

/* Subroutine */ int check1_(doublereal* sfac)
{
    /* Initialized data */

    static doublereal strue2[5] = { 0.,.5,.6,.7,.7 };
    static doublereal strue4[5] = { 0.,.7,1.,1.3,1.7 };
    static doublecomplex ctrue5[80]	/* was [8][5][2] */ = { {.1,.1},{1.,
	    2.},{1.,2.},{1.,2.},{1.,2.},{1.,2.},{1.,2.},{1.,2.},{-.16,-.37},{
	    3.,4.},{3.,4.},{3.,4.},{3.,4.},{3.,4.},{3.,4.},{3.,4.},{-.17,-.19}
	    ,{.13,-.39},{5.,6.},{5.,6.},{5.,6.},{5.,6.},{5.,6.},{5.,6.},{.11,
	    -.03},{-.17,.46},{-.17,-.19},{7.,8.},{7.,8.},{7.,8.},{7.,8.},{7.,
	    8.},{.19,-.17},{.32,.09},{.23,-.24},{.18,.01},{2.,3.},{2.,3.},{2.,
	    3.},{2.,3.},{.1,.1},{4.,5.},{4.,5.},{4.,5.},{4.,5.},{4.,5.},{4.,
	    5.},{4.,5.},{-.16,-.37},{6.,7.},{6.,7.},{6.,7.},{6.,7.},{6.,7.},{
	    6.,7.},{6.,7.},{-.17,-.19},{8.,9.},{.13,-.39},{2.,5.},{2.,5.},{2.,
	    5.},{2.,5.},{2.,5.},{.11,-.03},{3.,6.},{-.17,.46},{4.,7.},{-.17,
	    -.19},{7.,2.},{7.,2.},{7.,2.},{.19,-.17},{5.,8.},{.32,.09},{6.,9.}
	    ,{.23,-.24},{8.,3.},{.18,.01},{9.,4.} };
    static doublecomplex ctrue6[80]	/* was [8][5][2] */ = { {.1,.1},{1.,
	    2.},{1.,2.},{1.,2.},{1.,2.},{1.,2.},{1.,2.},{1.,2.},{.09,-.12},{
	    3.,4.},{3.,4.},{3.,4.},{3.,4.},{3.,4.},{3.,4.},{3.,4.},{.03,-.09},
	    {.15,-.03},{5.,6.},{5.,6.},{5.,6.},{5.,6.},{5.,6.},{5.,6.},{.03,
	    .03},{-.18,.03},{.03,-.09},{7.,8.},{7.,8.},{7.,8.},{7.,8.},{7.,8.}
	    ,{.09,.03},{.03,.12},{.12,.03},{.03,.06},{2.,3.},{2.,3.},{2.,3.},{
	    2.,3.},{.1,.1},{4.,5.},{4.,5.},{4.,5.},{4.,5.},{4.,5.},{4.,5.},{
	    4.,5.},{.09,-.12},{6.,7.},{6.,7.},{6.,7.},{6.,7.},{6.,7.},{6.,7.},
	    {6.,7.},{.03,-.09},{8.,9.},{.15,-.03},{2.,5.},{2.,5.},{2.,5.},{2.,
	    5.},{2.,5.},{.03,.03},{3.,6.},{-.18,.03},{4.,7.},{.03,-.09},{7.,
	    2.},{7.,2.},{7.,2.},{.09,.03},{5.,8.},{.03,.12},{6.,9.},{.12,.03},
	    {8.,3.},{.03,.06},{9.,4.} };
    static integer itrue3[5] = { 0,1,2,2,2 };
    static doublereal sa = .3;
    static doublecomplex ca = {.4,-.7};
    static doublecomplex cv[80]	/* was [8][5][2] */ = { {.1,.1},{1.,2.},{1.,
	    2.},{1.,2.},{1.,2.},{1.,2.},{1.,2.},{1.,2.},{.3,-.4},{3.,4.},{3.,
	    4.},{3.,4.},{3.,4.},{3.,4.},{3.,4.},{3.,4.},{.1,-.3},{.5,-.1},{5.,
	    6.},{5.,6.},{5.,6.},{5.,6.},{5.,6.},{5.,6.},{.1,.1},{-.6,.1},{.1,
	    -.3},{7.,8.},{7.,8.},{7.,8.},{7.,8.},{7.,8.},{.3,.1},{.1,.4},{.4,
	    .1},{.1,.2},{2.,3.},{2.,3.},{2.,3.},{2.,3.},{.1,.1},{4.,5.},{4.,
	    5.},{4.,5.},{4.,5.},{4.,5.},{4.,5.},{4.,5.},{.3,-.4},{6.,7.},{6.,
	    7.},{6.,7.},{6.,7.},{6.,7.},{6.,7.},{6.,7.},{.1,-.3},{8.,9.},{.5,
	    -.1},{2.,5.},{2.,5.},{2.,5.},{2.,5.},{2.,5.},{.1,.1},{3.,6.},{-.6,
	    .1},{4.,7.},{.1,-.3},{7.,2.},{7.,2.},{7.,2.},{.3,.1},{5.,8.},{.1,
	    .4},{6.,9.},{.4,.1},{8.,3.},{.1,.2},{9.,4.} };

    /* System generated locals */
    integer i__1, i__2, i__3;
    doublereal d__1;
    doublecomplex z__1;

    /* Local variables */
    static integer i__;
    extern /* Subroutine */ int ctest_(integer*, doublecomplex*, doublecomplex*, doublecomplex*, doublereal*);
    static doublecomplex mwpcs[5], mwpct[5];
    extern /* Subroutine */ int zscaltest_(integer*, doublecomplex*, doublecomplex*, integer*), itest1_(integer*, integer*), stest1_(doublereal*, doublereal*, doublereal*, doublereal*);
    static doublecomplex cx[8];
    extern doublereal dznrm2test_(integer*, doublecomplex*, integer*);
    static integer np1;
    extern /* Subroutine */ int zdscaltest_(integer*, doublereal*, doublecomplex*, integer*);
    extern integer izamaxtest_(integer*, doublecomplex*, integer*);
    extern doublereal dzasumtest_(integer*, doublecomplex*, integer*);
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
		i__2 = i__ - 1;
		i__3 = i__ + (np1 + combla_1.incx * 5 << 3) - 49;
		cx[i__2].r = cv[i__3].r, cx[i__2].i = cv[i__3].i;
/* L20: */
	    }
	    if (combla_1.icase == 6) {
/*              .. DZNRM2TEST .. */
		d__1 = dznrm2test_(&combla_1.n, cx, &combla_1.incx);
		stest1_(&d__1, &strue2[np1 - 1], &strue2[np1 - 1], sfac);
	    } else if (combla_1.icase == 7) {
/*              .. DZASUMTEST .. */
		d__1 = dzasumtest_(&combla_1.n, cx, &combla_1.incx);
		stest1_(&d__1, &strue4[np1 - 1], &strue4[np1 - 1], sfac);
	    } else if (combla_1.icase == 8) {
/*              .. ZSCALTEST .. */
		zscaltest_(&combla_1.n, &ca, cx, &combla_1.incx);
		ctest_(&len, cx, &ctrue5[(np1 + combla_1.incx * 5 << 3) - 48],
			 &ctrue5[(np1 + combla_1.incx * 5 << 3) - 48], sfac);
	    } else if (combla_1.icase == 9) {
/*              .. ZDSCALTEST .. */
		zdscaltest_(&combla_1.n, &sa, cx, &combla_1.incx);
		ctest_(&len, cx, &ctrue6[(np1 + combla_1.incx * 5 << 3) - 48],
			 &ctrue6[(np1 + combla_1.incx * 5 << 3) - 48], sfac);
	    } else if (combla_1.icase == 10) {
/*              .. IZAMAXTEST .. */
		i__1 = izamaxtest_(&combla_1.n, cx, &combla_1.incx);
		itest1_(&i__1, &itrue3[np1 - 1]);
	    } else {
		fprintf(stderr,"Shouldn't be here in CHECK1\n");
		exit(0);
	    }

/* L40: */
	}
/* L60: */
    }

    combla_1.incx = 1;
    if (combla_1.icase == 8) {
/*        ZSCALTEST */
/*        Add a test for alpha equal to zero. */
	ca.r = 0., ca.i = 0.;
	for (i__ = 1; i__ <= 5; ++i__) {
	    i__1 = i__ - 1;
	    mwpct[i__1].r = 0., mwpct[i__1].i = 0.;
	    i__1 = i__ - 1;
	    mwpcs[i__1].r = 1., mwpcs[i__1].i = 1.;
/* L80: */
	}
	zscaltest_(&c__5, &ca, cx, &combla_1.incx);
	ctest_(&c__5, cx, mwpct, mwpcs, sfac);
    } else if (combla_1.icase == 9) {
/*        ZDSCALTEST */
/*        Add a test for alpha equal to zero. */
	sa = 0.;
	for (i__ = 1; i__ <= 5; ++i__) {
	    i__1 = i__ - 1;
	    mwpct[i__1].r = 0., mwpct[i__1].i = 0.;
	    i__1 = i__ - 1;
	    mwpcs[i__1].r = 1., mwpcs[i__1].i = 1.;
/* L100: */
	}
	zdscaltest_(&c__5, &sa, cx, &combla_1.incx);
	ctest_(&c__5, cx, mwpct, mwpcs, sfac);
/*        Add a test for alpha equal to one. */
	sa = 1.;
	for (i__ = 1; i__ <= 5; ++i__) {
	    i__1 = i__ - 1;
	    i__2 = i__ - 1;
	    mwpct[i__1].r = cx[i__2].r, mwpct[i__1].i = cx[i__2].i;
	    i__1 = i__ - 1;
	    i__2 = i__ - 1;
	    mwpcs[i__1].r = cx[i__2].r, mwpcs[i__1].i = cx[i__2].i;
/* L120: */
	}
	zdscaltest_(&c__5, &sa, cx, &combla_1.incx);
	ctest_(&c__5, cx, mwpct, mwpcs, sfac);
/*        Add a test for alpha equal to minus one. */
	sa = -1.;
	for (i__ = 1; i__ <= 5; ++i__) {
	    i__1 = i__ - 1;
	    i__2 = i__ - 1;
	    z__1.r = -cx[i__2].r, z__1.i = -cx[i__2].i;
	    mwpct[i__1].r = z__1.r, mwpct[i__1].i = z__1.i;
	    i__1 = i__ - 1;
	    i__2 = i__ - 1;
	    z__1.r = -cx[i__2].r, z__1.i = -cx[i__2].i;
	    mwpcs[i__1].r = z__1.r, mwpcs[i__1].i = z__1.i;
/* L140: */
	}
	zdscaltest_(&c__5, &sa, cx, &combla_1.incx);
	ctest_(&c__5, cx, mwpct, mwpcs, sfac);
    }
    return 0;
} /* check1_ */

/* Subroutine */ int check2_(doublereal* sfac)
{
    /* Initialized data */

    static doublecomplex ca = {.4,-.7};
    static integer incxs[4] = { 1,2,-2,-1 };
    static integer incys[4] = { 1,-2,1,-2 };
    static integer lens[8]	/* was [4][2] */ = { 1,1,2,4,1,1,3,7 };
    static integer ns[4] = { 0,1,2,4 };
    static doublecomplex cx1[7] = { {.7,-.8},{-.4,-.7},{-.1,-.9},{.2,-.8},{
	    -.9,-.4},{.1,.4},{-.6,.6} };
    static doublecomplex cy1[7] = { {.6,-.6},{-.9,.5},{.7,-.6},{.1,-.5},{-.1,
	    -.2},{-.5,-.3},{.8,-.7} };
    static doublecomplex ct8[112]	/* was [7][4][4] */ = { {.6,-.6},{0.,
	    0.},{0.,0.},{0.,0.},{0.,0.},{0.,0.},{0.,0.},{.32,-1.41},{0.,0.},{
	    0.,0.},{0.,0.},{0.,0.},{0.,0.},{0.,0.},{.32,-1.41},{-1.55,.5},{0.,
	    0.},{0.,0.},{0.,0.},{0.,0.},{0.,0.},{.32,-1.41},{-1.55,.5},{.03,
	    -.89},{-.38,-.96},{0.,0.},{0.,0.},{0.,0.},{.6,-.6},{0.,0.},{0.,0.}
	    ,{0.,0.},{0.,0.},{0.,0.},{0.,0.},{.32,-1.41},{0.,0.},{0.,0.},{0.,
	    0.},{0.,0.},{0.,0.},{0.,0.},{-.07,-.89},{-.9,.5},{.42,-1.41},{0.,
	    0.},{0.,0.},{0.,0.},{0.,0.},{.78,.06},{-.9,.5},{.06,-.13},{.1,-.5}
	    ,{-.77,-.49},{-.5,-.3},{.52,-1.51},{.6,-.6},{0.,0.},{0.,0.},{0.,
	    0.},{0.,0.},{0.,0.},{0.,0.},{.32,-1.41},{0.,0.},{0.,0.},{0.,0.},{
	    0.,0.},{0.,0.},{0.,0.},{-.07,-.89},{-1.18,-.31},{0.,0.},{0.,0.},{
	    0.,0.},{0.,0.},{0.,0.},{.78,.06},{-1.54,.97},{.03,-.89},{-.18,
	    -1.31},{0.,0.},{0.,0.},{0.,0.},{.6,-.6},{0.,0.},{0.,0.},{0.,0.},{
	    0.,0.},{0.,0.},{0.,0.},{.32,-1.41},{0.,0.},{0.,0.},{0.,0.},{0.,0.}
	    ,{0.,0.},{0.,0.},{.32,-1.41},{-.9,.5},{.05,-.6},{0.,0.},{0.,0.},{
	    0.,0.},{0.,0.},{.32,-1.41},{-.9,.5},{.05,-.6},{.1,-.5},{-.77,-.49}
	    ,{-.5,-.3},{.32,-1.16} };
    static doublecomplex ct7[16]	/* was [4][4] */ = { {0.,0.},{-.06,
	    -.9},{.65,-.47},{-.34,-1.22},{0.,0.},{-.06,-.9},{-.59,-1.46},{
	    -1.04,-.04},{0.,0.},{-.06,-.9},{-.83,.59},{.07,-.37},{0.,0.},{
	    -.06,-.9},{-.76,-1.15},{-1.33,-1.82} };
    static doublecomplex ct6[16]	/* was [4][4] */ = { {0.,0.},{.9,.06},
	    {.91,-.77},{1.8,-.1},{0.,0.},{.9,.06},{1.45,.74},{.2,.9},{0.,0.},{
	    .9,.06},{-.55,.23},{.83,-.39},{0.,0.},{.9,.06},{1.04,.79},{1.95,
	    1.22} };
    static doublecomplex ct10x[112]	/* was [7][4][4] */ = { {.7,-.8},{0.,
	    0.},{0.,0.},{0.,0.},{0.,0.},{0.,0.},{0.,0.},{.6,-.6},{0.,0.},{0.,
	    0.},{0.,0.},{0.,0.},{0.,0.},{0.,0.},{.6,-.6},{-.9,.5},{0.,0.},{0.,
	    0.},{0.,0.},{0.,0.},{0.,0.},{.6,-.6},{-.9,.5},{.7,-.6},{.1,-.5},{
	    0.,0.},{0.,0.},{0.,0.},{.7,-.8},{0.,0.},{0.,0.},{0.,0.},{0.,0.},{
	    0.,0.},{0.,0.},{.6,-.6},{0.,0.},{0.,0.},{0.,0.},{0.,0.},{0.,0.},{
	    0.,0.},{.7,-.6},{-.4,-.7},{.6,-.6},{0.,0.},{0.,0.},{0.,0.},{0.,0.}
	    ,{.8,-.7},{-.4,-.7},{-.1,-.2},{.2,-.8},{.7,-.6},{.1,.4},{.6,-.6},{
	    .7,-.8},{0.,0.},{0.,0.},{0.,0.},{0.,0.},{0.,0.},{0.,0.},{.6,-.6},{
	    0.,0.},{0.,0.},{0.,0.},{0.,0.},{0.,0.},{0.,0.},{-.9,.5},{-.4,-.7},
	    {.6,-.6},{0.,0.},{0.,0.},{0.,0.},{0.,0.},{.1,-.5},{-.4,-.7},{.7,
	    -.6},{.2,-.8},{-.9,.5},{.1,.4},{.6,-.6},{.7,-.8},{0.,0.},{0.,0.},{
	    0.,0.},{0.,0.},{0.,0.},{0.,0.},{.6,-.6},{0.,0.},{0.,0.},{0.,0.},{
	    0.,0.},{0.,0.},{0.,0.},{.6,-.6},{.7,-.6},{0.,0.},{0.,0.},{0.,0.},{
	    0.,0.},{0.,0.},{.6,-.6},{.7,-.6},{-.1,-.2},{.8,-.7},{0.,0.},{0.,
	    0.},{0.,0.} };
    static doublecomplex ct10y[112]	/* was [7][4][4] */ = { {.6,-.6},{0.,
	    0.},{0.,0.},{0.,0.},{0.,0.},{0.,0.},{0.,0.},{.7,-.8},{0.,0.},{0.,
	    0.},{0.,0.},{0.,0.},{0.,0.},{0.,0.},{.7,-.8},{-.4,-.7},{0.,0.},{
	    0.,0.},{0.,0.},{0.,0.},{0.,0.},{.7,-.8},{-.4,-.7},{-.1,-.9},{.2,
	    -.8},{0.,0.},{0.,0.},{0.,0.},{.6,-.6},{0.,0.},{0.,0.},{0.,0.},{0.,
	    0.},{0.,0.},{0.,0.},{.7,-.8},{0.,0.},{0.,0.},{0.,0.},{0.,0.},{0.,
	    0.},{0.,0.},{-.1,-.9},{-.9,.5},{.7,-.8},{0.,0.},{0.,0.},{0.,0.},{
	    0.,0.},{-.6,.6},{-.9,.5},{-.9,-.4},{.1,-.5},{-.1,-.9},{-.5,-.3},{
	    .7,-.8},{.6,-.6},{0.,0.},{0.,0.},{0.,0.},{0.,0.},{0.,0.},{0.,0.},{
	    .7,-.8},{0.,0.},{0.,0.},{0.,0.},{0.,0.},{0.,0.},{0.,0.},{-.1,-.9},
	    {.7,-.8},{0.,0.},{0.,0.},{0.,0.},{0.,0.},{0.,0.},{-.6,.6},{-.9,
	    -.4},{-.1,-.9},{.7,-.8},{0.,0.},{0.,0.},{0.,0.},{.6,-.6},{0.,0.},{
	    0.,0.},{0.,0.},{0.,0.},{0.,0.},{0.,0.},{.7,-.8},{0.,0.},{0.,0.},{
	    0.,0.},{0.,0.},{0.,0.},{0.,0.},{.7,-.8},{-.9,.5},{-.4,-.7},{0.,0.}
	    ,{0.,0.},{0.,0.},{0.,0.},{.7,-.8},{-.9,.5},{-.4,-.7},{.1,-.5},{
	    -.1,-.9},{-.5,-.3},{.2,-.8} };
    static doublecomplex csize1[4] = { {0.,0.},{.9,.9},{1.63,1.73},{2.9,2.78} 
	    };
    static doublecomplex csize3[14] = { {0.,0.},{0.,0.},{0.,0.},{0.,0.},{0.,
	    0.},{0.,0.},{0.,0.},{1.17,1.17},{1.17,1.17},{1.17,1.17},{1.17,
	    1.17},{1.17,1.17},{1.17,1.17},{1.17,1.17} };
    static doublecomplex csize2[14]	/* was [7][2] */ = { {0.,0.},{0.,0.},{
	    0.,0.},{0.,0.},{0.,0.},{0.,0.},{0.,0.},{1.54,1.54},{1.54,1.54},{
	    1.54,1.54},{1.54,1.54},{1.54,1.54},{1.54,1.54},{1.54,1.54} };

    /* System generated locals */
    integer i__1, i__2;

    /* Local variables */
    static doublecomplex cdot[1];
    static integer lenx, leny, i__;
    extern /* Subroutine */ int ctest_(integer*, doublecomplex*, doublecomplex*, doublecomplex*, doublereal*);
    static integer ksize;
    static doublecomplex ztemp;
    extern /* Subroutine */ int zdotctest_(integer*, doublecomplex*, integer*, doublecomplex*, integer*, doublecomplex*), zcopytest_(integer*, doublecomplex*, integer*, doublecomplex*, integer*);
    static integer ki;
    extern /* Subroutine */ int zdotutest_(integer*, doublecomplex*, integer*, doublecomplex*, integer*, doublecomplex*), zswaptest_(integer*, doublecomplex*, integer*, doublecomplex*, integer*);
    static integer kn;
    extern /* Subroutine */ int zaxpytest_(integer*, doublecomplex*, doublecomplex*, integer*, doublecomplex*, integer*);
    static doublecomplex cx[7], cy[7];
    static integer mx, my;

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
/*           .. initialize all argument arrays .. */
	    for (i__ = 1; i__ <= 7; ++i__) {
		i__1 = i__ - 1;
		i__2 = i__ - 1;
		cx[i__1].r = cx1[i__2].r, cx[i__1].i = cx1[i__2].i;
		i__1 = i__ - 1;
		i__2 = i__ - 1;
		cy[i__1].r = cy1[i__2].r, cy[i__1].i = cy1[i__2].i;
/* L20: */
	    }
	    if (combla_1.icase == 1) {
/*              .. ZDOTCTEST .. */
		zdotctest_(&combla_1.n, cx, &combla_1.incx, cy, &
			combla_1.incy, &ztemp);
		cdot[0].r = ztemp.r, cdot[0].i = ztemp.i;
		ctest_(&c__1, cdot, &ct6[kn + (ki << 2) - 5], &csize1[kn - 1],
			 sfac);
	    } else if (combla_1.icase == 2) {
/*              .. ZDOTUTEST .. */
		zdotutest_(&combla_1.n, cx, &combla_1.incx, cy, &
			combla_1.incy, &ztemp);
		cdot[0].r = ztemp.r, cdot[0].i = ztemp.i;
		ctest_(&c__1, cdot, &ct7[kn + (ki << 2) - 5], &csize1[kn - 1],
			 sfac);
	    } else if (combla_1.icase == 3) {
/*              .. ZAXPYTEST .. */
		zaxpytest_(&combla_1.n, &ca, cx, &combla_1.incx, cy, &
			combla_1.incy);
		ctest_(&leny, cy, &ct8[(kn + (ki << 2)) * 7 - 35], &csize2[
			ksize * 7 - 7], sfac);
	    } else if (combla_1.icase == 4) {
/*              .. ZCOPYTEST .. */
		zcopytest_(&combla_1.n, cx, &combla_1.incx, cy, &
			combla_1.incy);
		ctest_(&leny, cy, &ct10y[(kn + (ki << 2)) * 7 - 35], csize3, &
			c_b43);
	    } else if (combla_1.icase == 5) {
/*              .. ZSWAPTEST .. */
		zswaptest_(&combla_1.n, cx, &combla_1.incx, cy, &
			combla_1.incy);
		ctest_(&lenx, cx, &ct10x[(kn + (ki << 2)) * 7 - 35], csize3, &
			c_b43);
		ctest_(&leny, cy, &ct10y[(kn + (ki << 2)) * 7 - 35], csize3, &
			c_b43);
	    } else {
		fprintf(stderr,"Shouldn't be here in CHECK2\n");
		exit(0);
	    }

/* L40: */
	}
/* L60: */
    }
    return 0;
} /* check2_ */

/* Subroutine */ int stest_(integer* len, doublereal* scomp, doublereal* strue, doublereal* ssize, doublereal* sfac)
{
    /* System generated locals */
    integer i__1;
    doublereal d__1, d__2, d__3, d__4, d__5;

    /* Builtin functions */
    integer s_wsfe(void), e_wsfe(void), do_fio(void);

    /* Local variables */
    static integer i__;
    extern doublereal sdiff_(doublereal*, doublereal*);
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
	printf("CASE  N INCX INCY MODE  I                             COMP(I)                             TRUE(I)  DIFFERENCE     SIZE(I)\n");
L20:
        printf("%4d %3d %5d %5d %5d %3d %36.8f %36.8f %12.4f %12.4f\n",combla_1.icase, combla_1.n, combla_1.incx, combla_1.incy, 
	combla_1.mode, i__, scomp[i__], strue[i__], sd, ssize[i__]);
L40:
	;
    }
    return 0;

} /* stest_ */

/* Subroutine */ int stest1_(doublereal* scomp1, doublereal* strue1, doublereal* ssize, doublereal* sfac)
{
    static doublereal scomp[1], strue[1];
    extern /* Subroutine */ int stest_(integer*,doublereal*, doublereal*, doublereal*, doublereal*);

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

/* Subroutine */ int ctest_(integer* len, doublecomplex* ccomp, doublecomplex* ctrue, doublecomplex* csize, doublereal* sfac)
{
    /* System generated locals */
    integer i__1, i__2;

    /* Local variables */
    static integer i__;
    static doublereal scomp[20], ssize[20], strue[20];
    extern /* Subroutine */ int stest_(integer*, doublereal*, doublereal*, doublereal*, doublereal*);

/*     **************************** CTEST ***************************** */

/*     C.L. LAWSON, JPL, 1978 DEC 6 */

/*     .. Scalar Arguments .. */
/*     .. Array Arguments .. */
/*     .. Local Scalars .. */
/*     .. Local Arrays .. */
/*     .. External Subroutines .. */
/*     .. Intrinsic Functions .. */
/*     .. Executable Statements .. */
    /* Parameter adjustments */
    --csize;
    --ctrue;
    --ccomp;

    /* Function Body */
    i__1 = *len;
    for (i__ = 1; i__ <= i__1; ++i__) {
	i__2 = i__;
	scomp[(i__ << 1) - 2] = ccomp[i__2].r;
	scomp[(i__ << 1) - 1] = d_imag(&ccomp[i__]);
	i__2 = i__;
	strue[(i__ << 1) - 2] = ctrue[i__2].r;
	strue[(i__ << 1) - 1] = d_imag(&ctrue[i__]);
	i__2 = i__;
	ssize[(i__ << 1) - 2] = csize[i__2].r;
	ssize[(i__ << 1) - 1] = d_imag(&csize[i__]);
/* L20: */
    }

    i__1 = *len << 1;
    stest_(&i__1, scomp, strue, ssize, sfac);
    return 0;
} /* ctest_ */

/* Subroutine */ int itest1_(integer* icomp, integer* itrue)
{
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
    printf("                                       FAIL\n");
    printf("CASE  N INCX INCY MODE                                COMP                                TRUE     DIFFERENCE\n");
L20:
    id = *icomp - *itrue;
    printf("%4d %3d %5d %5d %5d %36d %36d %12d\n",combla_1.icase, combla_1.n, combla_1.incx, combla_1.incy,
    combla_1.mode, *icomp, *itrue, id);
L40:
    return 0;

} /* itest1_ */

