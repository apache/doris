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
static real c_b34 = (float)1.;

/* Main program */ int main (void)
{
    /* Initialized data */

    static real sfac = (float)9.765625e-4;

    /* Local variables */
    extern /* Subroutine */ int check0_(real*), check1_(real*), check2_(real*), check3_(real*);
    static integer ic;
    extern /* Subroutine */ int header_(void);

/*     Test program for the REAL             Level 1 CBLAS. */
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

    static char l[15][13] = {"CBLAS_SDOT  " , "CBLAS_SAXPY " , "CBLAS_SROTG " ,
        "CBLAS_SROT  " , "CBLAS_SCOPY " , "CBLAS_SSWAP " , "CBLAS_SNRM2 " , "CBLAS_SASUM ",
        "CBLAS_SSCAL " , "CBLAS_ISAMAX", "CBLAS_SROTM "};

    /* Fortran I/O blocks */


/*     .. Parameters .. */
/*     .. Scalars in Common .. */
/*     .. Local Arrays .. */
/*     .. Common blocks .. */
/*     .. Data statements .. */
/*     .. Executable Statements .. */
    printf("\nTest of subprogram number %3d         %15s",combla_1.icase,l[combla_1.icase-1]);

    return 0;

} /* header_ */

/* Subroutine */ int check0_(real *sfac)
{
    /* Initialized data */

    static real ds1[8] = { (float).8,(float).6,(float).8,(float)-.6,(float).8,
	    (float)0.,(float)1.,(float)0. };
    static real datrue[8] = { (float).5,(float).5,(float).5,(float)-.5,(float)
	    -.5,(float)0.,(float)1.,(float)1. };
    static real dbtrue[8] = { (float)0.,(float).6,(float)0.,(float)-.6,(float)
	    0.,(float)0.,(float)1.,(float)0. };
    static real da1[8] = { (float).3,(float).4,(float)-.3,(float)-.4,(float)
	    -.3,(float)0.,(float)0.,(float)1. };
    static real db1[8] = { (float).4,(float).3,(float).4,(float).3,(float)-.4,
	    (float)0.,(float)1.,(float)0. };
    static real dc1[8] = { (float).6,(float).8,(float)-.6,(float).8,(float).6,
	    (float)1.,(float)0.,(float)1. };

    /* Local variables */
    static integer k;
    extern /* Subroutine */ int srotgtest_(real*,real*,real*,real*), stest1_(real*,real*,real*,real*);
    static real sa, sb, sc, ss;

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

    dbtrue[0] = (float)1.6666666666666667;
    dbtrue[2] = (float)-1.6666666666666667;
    dbtrue[4] = (float)1.6666666666666667;

    for (k = 1; k <= 8; ++k) {
/*        .. Set N=K for identification in output if any .. */
	combla_1.n = k;
	if (combla_1.icase == 3) {
/*           .. SROTGTEST .. */
	    if (k > 8) {
		goto L40;
	    }
	    sa = da1[k - 1];
	    sb = db1[k - 1];
	    srotgtest_(&sa, &sb, &sc, &ss);
	    stest1_(&sa, &datrue[k - 1], &datrue[k - 1], sfac);
	    stest1_(&sb, &dbtrue[k - 1], &dbtrue[k - 1], sfac);
	    stest1_(&sc, &dc1[k - 1], &dc1[k - 1], sfac);
	    stest1_(&ss, &ds1[k - 1], &ds1[k - 1], sfac);
	} else {
	    fprintf (stderr,"Shouldn't be here in CHECK0\n");
	    exit(0);
	}
/* L20: */
    }
L40:
    return 0;
} /* check0_ */

/* Subroutine */ int check1_(real* sfac)
{
    /* Initialized data */

    static real sa[10] = { (float).3,(float)-1.,(float)0.,(float)1.,(float).3,
	    (float).3,(float).3,(float).3,(float).3,(float).3 };
    static real dv[80]	/* was [8][5][2] */ = { (float).1,(float)2.,(float)2.,
	    (float)2.,(float)2.,(float)2.,(float)2.,(float)2.,(float).3,(
	    float)3.,(float)3.,(float)3.,(float)3.,(float)3.,(float)3.,(float)
	    3.,(float).3,(float)-.4,(float)4.,(float)4.,(float)4.,(float)4.,(
	    float)4.,(float)4.,(float).2,(float)-.6,(float).3,(float)5.,(
	    float)5.,(float)5.,(float)5.,(float)5.,(float).1,(float)-.3,(
	    float).5,(float)-.1,(float)6.,(float)6.,(float)6.,(float)6.,(
	    float).1,(float)8.,(float)8.,(float)8.,(float)8.,(float)8.,(float)
	    8.,(float)8.,(float).3,(float)9.,(float)9.,(float)9.,(float)9.,(
	    float)9.,(float)9.,(float)9.,(float).3,(float)2.,(float)-.4,(
	    float)2.,(float)2.,(float)2.,(float)2.,(float)2.,(float).2,(float)
	    3.,(float)-.6,(float)5.,(float).3,(float)2.,(float)2.,(float)2.,(
	    float).1,(float)4.,(float)-.3,(float)6.,(float)-.5,(float)7.,(
	    float)-.1,(float)3. };
    static real dtrue1[5] = { (float)0.,(float).3,(float).5,(float).7,(float)
	    .6 };
    static real dtrue3[5] = { (float)0.,(float).3,(float).7,(float)1.1,(float)
	    1. };
    static real dtrue5[80]	/* was [8][5][2] */ = { (float).1,(float)2.,(
	    float)2.,(float)2.,(float)2.,(float)2.,(float)2.,(float)2.,(float)
	    -.3,(float)3.,(float)3.,(float)3.,(float)3.,(float)3.,(float)3.,(
	    float)3.,(float)0.,(float)0.,(float)4.,(float)4.,(float)4.,(float)
	    4.,(float)4.,(float)4.,(float).2,(float)-.6,(float).3,(float)5.,(
	    float)5.,(float)5.,(float)5.,(float)5.,(float).03,(float)-.09,(
	    float).15,(float)-.03,(float)6.,(float)6.,(float)6.,(float)6.,(
	    float).1,(float)8.,(float)8.,(float)8.,(float)8.,(float)8.,(float)
	    8.,(float)8.,(float).09,(float)9.,(float)9.,(float)9.,(float)9.,(
	    float)9.,(float)9.,(float)9.,(float).09,(float)2.,(float)-.12,(
	    float)2.,(float)2.,(float)2.,(float)2.,(float)2.,(float).06,(
	    float)3.,(float)-.18,(float)5.,(float).09,(float)2.,(float)2.,(
	    float)2.,(float).03,(float)4.,(float)-.09,(float)6.,(float)-.15,(
	    float)7.,(float)-.03,(float)3. };
    static integer itrue2[5] = { 0,1,2,2,3 };

    /* System generated locals */
    integer i__1;
    real r__1;

    /* Local variables */
    static integer i__;
    extern real snrm2test_(integer*,real*,integer*);
    static real stemp[1], strue[8];
    extern /* Subroutine */ int stest_(integer*, real*,real*,real*,real*), sscaltest_(integer*,real*,real*,integer*);
    extern real sasumtest_(integer*,real*,integer*);
    extern /* Subroutine */ int itest1_(integer*,integer*), stest1_(real*,real*,real*,real*);
    static real sx[8];
    static integer np1;
    extern integer isamaxtest_(integer*,real*,integer*);
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
/*              .. SNRM2TEST .. */
		stemp[0] = dtrue1[np1 - 1];
		r__1 = snrm2test_(&combla_1.n, sx, &combla_1.incx);
		stest1_(&r__1, stemp, stemp, sfac);
	    } else if (combla_1.icase == 8) {
/*              .. SASUMTEST .. */
		stemp[0] = dtrue3[np1 - 1];
		r__1 = sasumtest_(&combla_1.n, sx, &combla_1.incx);
		stest1_(&r__1, stemp, stemp, sfac);
	    } else if (combla_1.icase == 9) {
/*              .. SSCALTEST .. */
		sscaltest_(&combla_1.n, &sa[(combla_1.incx - 1) * 5 + np1 - 1]
			, sx, &combla_1.incx);
		i__1 = len;
		for (i__ = 1; i__ <= i__1; ++i__) {
		    strue[i__ - 1] = dtrue5[i__ + (np1 + combla_1.incx * 5 << 
			    3) - 49];
/* L40: */
		}
		stest_(&len, sx, strue, strue, sfac);
	    } else if (combla_1.icase == 10) {
/*              .. ISAMAXTEST .. */
		i__1 = isamaxtest_(&combla_1.n, sx, &combla_1.incx);
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

/* Subroutine */ int check2_(real* sfac)
{
    /* Initialized data */

    static real sa = (float).3;
    static integer incxs[4] = { 1,2,-2,-1 };
    static integer incys[4] = { 1,-2,1,-2 };
    static integer lens[8]	/* was [4][2] */ = { 1,1,2,4,1,1,3,7 };
    static integer ns[4] = { 0,1,2,4 };
    static real dx1[7] = { (float).6,(float).1,(float)-.5,(float).8,(float).9,
	    (float)-.3,(float)-.4 };
    static real dy1[7] = { (float).5,(float)-.9,(float).3,(float).7,(float)
	    -.6,(float).2,(float).8 };
    static real dt7[16]	/* was [4][4] */ = { (float)0.,(float).3,(float).21,(
	    float).62,(float)0.,(float).3,(float)-.07,(float).85,(float)0.,(
	    float).3,(float)-.79,(float)-.74,(float)0.,(float).3,(float).33,(
	    float)1.27 };
    static real dt8[112]	/* was [7][4][4] */ = { (float).5,(float)0.,(
	    float)0.,(float)0.,(float)0.,(float)0.,(float)0.,(float).68,(
	    float)0.,(float)0.,(float)0.,(float)0.,(float)0.,(float)0.,(float)
	    .68,(float)-.87,(float)0.,(float)0.,(float)0.,(float)0.,(float)0.,
	    (float).68,(float)-.87,(float).15,(float).94,(float)0.,(float)0.,(
	    float)0.,(float).5,(float)0.,(float)0.,(float)0.,(float)0.,(float)
	    0.,(float)0.,(float).68,(float)0.,(float)0.,(float)0.,(float)0.,(
	    float)0.,(float)0.,(float).35,(float)-.9,(float).48,(float)0.,(
	    float)0.,(float)0.,(float)0.,(float).38,(float)-.9,(float).57,(
	    float).7,(float)-.75,(float).2,(float).98,(float).5,(float)0.,(
	    float)0.,(float)0.,(float)0.,(float)0.,(float)0.,(float).68,(
	    float)0.,(float)0.,(float)0.,(float)0.,(float)0.,(float)0.,(float)
	    .35,(float)-.72,(float)0.,(float)0.,(float)0.,(float)0.,(float)0.,
	    (float).38,(float)-.63,(float).15,(float).88,(float)0.,(float)0.,(
	    float)0.,(float).5,(float)0.,(float)0.,(float)0.,(float)0.,(float)
	    0.,(float)0.,(float).68,(float)0.,(float)0.,(float)0.,(float)0.,(
	    float)0.,(float)0.,(float).68,(float)-.9,(float).33,(float)0.,(
	    float)0.,(float)0.,(float)0.,(float).68,(float)-.9,(float).33,(
	    float).7,(float)-.75,(float).2,(float)1.04 };
    static real dt10x[112]	/* was [7][4][4] */ = { (float).6,(float)0.,(
	    float)0.,(float)0.,(float)0.,(float)0.,(float)0.,(float).5,(float)
	    0.,(float)0.,(float)0.,(float)0.,(float)0.,(float)0.,(float).5,(
	    float)-.9,(float)0.,(float)0.,(float)0.,(float)0.,(float)0.,(
	    float).5,(float)-.9,(float).3,(float).7,(float)0.,(float)0.,(
	    float)0.,(float).6,(float)0.,(float)0.,(float)0.,(float)0.,(float)
	    0.,(float)0.,(float).5,(float)0.,(float)0.,(float)0.,(float)0.,(
	    float)0.,(float)0.,(float).3,(float).1,(float).5,(float)0.,(float)
	    0.,(float)0.,(float)0.,(float).8,(float).1,(float)-.6,(float).8,(
	    float).3,(float)-.3,(float).5,(float).6,(float)0.,(float)0.,(
	    float)0.,(float)0.,(float)0.,(float)0.,(float).5,(float)0.,(float)
	    0.,(float)0.,(float)0.,(float)0.,(float)0.,(float)-.9,(float).1,(
	    float).5,(float)0.,(float)0.,(float)0.,(float)0.,(float).7,(float)
	    .1,(float).3,(float).8,(float)-.9,(float)-.3,(float).5,(float).6,(
	    float)0.,(float)0.,(float)0.,(float)0.,(float)0.,(float)0.,(float)
	    .5,(float)0.,(float)0.,(float)0.,(float)0.,(float)0.,(float)0.,(
	    float).5,(float).3,(float)0.,(float)0.,(float)0.,(float)0.,(float)
	    0.,(float).5,(float).3,(float)-.6,(float).8,(float)0.,(float)0.,(
	    float)0. };
    static real dt10y[112]	/* was [7][4][4] */ = { (float).5,(float)0.,(
	    float)0.,(float)0.,(float)0.,(float)0.,(float)0.,(float).6,(float)
	    0.,(float)0.,(float)0.,(float)0.,(float)0.,(float)0.,(float).6,(
	    float).1,(float)0.,(float)0.,(float)0.,(float)0.,(float)0.,(float)
	    .6,(float).1,(float)-.5,(float).8,(float)0.,(float)0.,(float)0.,(
	    float).5,(float)0.,(float)0.,(float)0.,(float)0.,(float)0.,(float)
	    0.,(float).6,(float)0.,(float)0.,(float)0.,(float)0.,(float)0.,(
	    float)0.,(float)-.5,(float)-.9,(float).6,(float)0.,(float)0.,(
	    float)0.,(float)0.,(float)-.4,(float)-.9,(float).9,(float).7,(
	    float)-.5,(float).2,(float).6,(float).5,(float)0.,(float)0.,(
	    float)0.,(float)0.,(float)0.,(float)0.,(float).6,(float)0.,(float)
	    0.,(float)0.,(float)0.,(float)0.,(float)0.,(float)-.5,(float).6,(
	    float)0.,(float)0.,(float)0.,(float)0.,(float)0.,(float)-.4,(
	    float).9,(float)-.5,(float).6,(float)0.,(float)0.,(float)0.,(
	    float).5,(float)0.,(float)0.,(float)0.,(float)0.,(float)0.,(float)
	    0.,(float).6,(float)0.,(float)0.,(float)0.,(float)0.,(float)0.,(
	    float)0.,(float).6,(float)-.9,(float).1,(float)0.,(float)0.,(
	    float)0.,(float)0.,(float).6,(float)-.9,(float).1,(float).7,(
	    float)-.5,(float).2,(float).8 };
    static real ssize1[4] = { (float)0.,(float).3,(float)1.6,(float)3.2 };
    static real ssize2[28]	/* was [14][2] */ = { (float)0.,(float)0.,(
	    float)0.,(float)0.,(float)0.,(float)0.,(float)0.,(float)0.,(float)
	    0.,(float)0.,(float)0.,(float)0.,(float)0.,(float)0.,(float)1.17,(
	    float)1.17,(float)1.17,(float)1.17,(float)1.17,(float)1.17,(float)
	    1.17,(float)1.17,(float)1.17,(float)1.17,(float)1.17,(float)1.17,(
	    float)1.17,(float)1.17 };

    /* System generated locals */
    integer i__1;
    real r__1;

    /* Local variables */
    static integer lenx, leny;
    extern real sdottest_(integer*,real*,integer*,real*,integer*);
    static integer i__, j, ksize;
    extern /* Subroutine */ int stest_(integer*,real*,real*,real*,real*), scopytest_(integer*,real*,integer*,real*,integer*), sswaptest_(integer*,real*,integer*,real*,integer*), 
	    saxpytest_(integer*,real*,real*,integer*,real*,integer*);
    static integer ki;
    extern /* Subroutine */ int stest1_(real*,real*,real*,real*);
    static integer kn, mx, my;
    static real sx[7], sy[7], stx[7], sty[7];

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
/*              .. SDOTTEST .. */
		r__1 = sdottest_(&combla_1.n, sx, &combla_1.incx, sy, &
			combla_1.incy);
		stest1_(&r__1, &dt7[kn + (ki << 2) - 5], &ssize1[kn - 1], 
			sfac);
	    } else if (combla_1.icase == 2) {
/*              .. SAXPYTEST .. */
		saxpytest_(&combla_1.n, &sa, sx, &combla_1.incx, sy, &
			combla_1.incy);
		i__1 = leny;
		for (j = 1; j <= i__1; ++j) {
		    sty[j - 1] = dt8[j + (kn + (ki << 2)) * 7 - 36];
/* L40: */
		}
		stest_(&leny, sy, sty, &ssize2[ksize * 14 - 14], sfac);
	    } else if (combla_1.icase == 5) {
/*              .. SCOPYTEST .. */
		for (i__ = 1; i__ <= 7; ++i__) {
		    sty[i__ - 1] = dt10y[i__ + (kn + (ki << 2)) * 7 - 36];
/* L60: */
		}
		scopytest_(&combla_1.n, sx, &combla_1.incx, sy, &
			combla_1.incy);
		stest_(&leny, sy, sty, ssize2, &c_b34);
	    } else if (combla_1.icase == 6) {
/*              .. SSWAPTEST .. */
		sswaptest_(&combla_1.n, sx, &combla_1.incx, sy, &
			combla_1.incy);
		for (i__ = 1; i__ <= 7; ++i__) {
		    stx[i__ - 1] = dt10x[i__ + (kn + (ki << 2)) * 7 - 36];
		    sty[i__ - 1] = dt10y[i__ + (kn + (ki << 2)) * 7 - 36];
/* L80: */
		}
		stest_(&lenx, sx, stx, ssize2, &c_b34);
		stest_(&leny, sy, sty, ssize2, &c_b34);
	    } else {
		fprintf(stderr,"Shouldn't be here in CHECK2\n");
		exit(0);
	    }
/* L100: */
	}
/* L120: */
    }
    return 0;
} /* check2_ */

/* Subroutine */ int check3_(real* sfac)
{
    /* Initialized data */

    static integer incxs[7] = { 1,1,2,2,-2,-1,-2 };
    static integer incys[7] = { 1,2,2,-2,1,-2,-2 };
    static integer ns[7] = { 0,1,2,4,5,8,9 };
    static real dx[19] = { (float).6,(float).1,(float)-.5,(float).8,(float).9,
	    (float)-.3,(float)-.4,(float).5,(float)-.9,(float).3,(float).7,(
	    float)-.6,(float).2,(float).8,(float)-.46,(float).78,(float)-.46,(
	    float)-.22,(float)1.06 };
    static real dy[19] = { (float).5,(float)-.9,(float).3,(float).7,(float)
	    -.6,(float).2,(float).6,(float).1,(float)-.5,(float).8,(float).9,(
	    float)-.3,(float).96,(float).1,(float)-.76,(float).8,(float).9,(
	    float).66,(float).8 };
    static real sc = (float).8;
    static real ss = (float).6;
    static real param[20]	/* was [5][4] */ = { (float)-2.,(float)1.,(
	    float)0.,(float)0.,(float)1.,(float)-1.,(float).2,(float).3,(
	    float).4,(float).5,(float)0.,(float)1.,(float).3,(float).4,(float)
	    1.,(float)1.,(float).2,(float)-1.,(float)1.,(float).5 };
    static integer len = 19;
    static real ssize2[38]	/* was [19][2] */ = { (float)0.,(float)0.,(
	    float)0.,(float)0.,(float)0.,(float)0.,(float)0.,(float)0.,(float)
	    0.,(float)0.,(float)0.,(float)0.,(float)0.,(float)0.,(float)0.,(
	    float)0.,(float)0.,(float)0.,(float)0.,(float)1.17,(float)1.17,(
	    float)1.17,(float)1.17,(float)1.17,(float)1.17,(float)1.17,(float)
	    1.17,(float)1.17,(float)1.17,(float)1.17,(float)1.17,(float)1.17,(
	    float)1.17,(float)1.17,(float)1.17,(float)1.17,(float)1.17,(float)
	    1.17 };

    /* Local variables */
    extern /* Subroutine */ void srottest_(integer*,real*,integer*,real*,integer*,real*,real*);
    static integer i__, k, ksize;
    extern /* Subroutine */ int stest_(integer*,real*,real*,real*,real*), srotmtest_(integer*,real*,integer*,real*,integer*,real*);
    static integer ki, kn;
    static real sx[19], sy[19], sparam[5], stx[19], sty[19];

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

	for (kn = 1; kn <= 7; ++kn) {
	    combla_1.n = ns[kn - 1];
	    ksize = f2cmin(2,kn);

	    if (combla_1.icase == 4) {
/*              .. SROTTEST .. */
		for (i__ = 1; i__ <= 19; ++i__) {
		    sx[i__ - 1] = dx[i__ - 1];
		    sy[i__ - 1] = dy[i__ - 1];
		    stx[i__ - 1] = dx[i__ - 1];
		    sty[i__ - 1] = dy[i__ - 1];
/* L20: */
		}
		srottest_(&combla_1.n, sx, &combla_1.incx, sy, &combla_1.incy,
			 &sc, &ss);
		srot_(&combla_1.n, stx, &combla_1.incx, sty, &combla_1.incy, &
			sc, &ss);
		stest_(&len, sx, stx, &ssize2[ksize * 19 - 19], sfac);
		stest_(&len, sy, sty, &ssize2[ksize * 19 - 19], sfac);
	    } else if (combla_1.icase == 11) {
/*              .. SROTMTEST .. */
		for (i__ = 1; i__ <= 19; ++i__) {
		    sx[i__ - 1] = dx[i__ - 1];
		    sy[i__ - 1] = dy[i__ - 1];
		    stx[i__ - 1] = dx[i__ - 1];
		    sty[i__ - 1] = dy[i__ - 1];
/* L90: */
		}
		for (i__ = 1; i__ <= 4; ++i__) {
		    for (k = 1; k <= 5; ++k) {
			sparam[k - 1] = param[k + i__ * 5 - 6];
/* L80: */
		    }
		    srotmtest_(&combla_1.n, sx, &combla_1.incx, sy, &
			    combla_1.incy, sparam);
		    srotm_(&combla_1.n, stx, &combla_1.incx, sty, &
			    combla_1.incy, sparam);
		    stest_(&len, sx, stx, &ssize2[ksize * 19 - 19], sfac);
		    stest_(&len, sy, sty, &ssize2[ksize * 19 - 19], sfac);
/* L70: */
		}
	    } else {
		fprintf(stderr,"Shouldn't be here in CHECK3\n");
		exit(0);
	    }
/* L40: */
	}
/* L60: */
    }
    return 0;
} /* check3_ */

/* Subroutine */ int stest_(integer* len, real* scomp, real* strue, real* ssize, real* sfac)
{
    integer i__1;
    real r__1, r__2, r__3, r__4, r__5;

    /* Local variables */
    static integer i__;
    extern doublereal sdiff_(real*,real*);
    static real sd;

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
	r__4 = (r__1 = ssize[i__], dabs(r__1)) + (r__2 = *sfac * sd, dabs(
		r__2));
	r__5 = (r__3 = ssize[i__], dabs(r__3));
	if (sdiff_(&r__4, &r__5) == (float)0.) {
	    goto L40;
	}

/*                             HERE    SCOMP(I) IS NOT CLOSE TO STRUE(I). */

	if (! combla_1.pass) {
	    goto L20;
	}
/*                             PRINT FAIL MESSAGE AND HEADER. */
	combla_1.pass = FALSE_;
	printf("                                       FAIL\n");
	printf("CASE  N INCX INCY MODE  I             COMP(I)                             TRUE(I)  DIFFERENCE     SIZE(I)\n");
L20:
    	printf("%4d %3d %5d %5d %5d %3d %36.8f %36.8f %12.4f %12.4f\n",combla_1.icase, combla_1.n,
    	combla_1.incx, combla_1.incy, combla_1.mode, i__, scomp[i__], strue[i__], sd, ssize[i__]);
L40:
	;
    }
    return 0;

} /* stest_ */

/* Subroutine */ int stest1_(real* scomp1, real* strue1, real* ssize, real* sfac)
{
    static real scomp[1], strue[1];
    extern /* Subroutine */ int stest_(integer*,real*,real*,real*,real*);

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

doublereal sdiff_(real* sa, real* sb)
{
    /* System generated locals */
    real ret_val;

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
    printf("                                       FAIL\n");
    printf("CASE  N INCX INCY MODE                               COMP                                TRUE     DIFFERENCE\n");
L20:
    id = *icomp - *itrue;
    printf("%4d %3d %5d %5d %5d %36d %36d %12d\n",
    combla_1.icase, combla_1.n, combla_1.incx, combla_1.incy, combla_1.mode, *icomp,*itrue,id);
L40:
    return 0;

} /* itest1_ */
#if 0
/* Subroutine */ int srot_(n, sx, incx, sy, incy, c__, s)
integer *n;
real *sx;
integer *incx;
real *sy;
integer *incy;
real *c__, *s;
{
    /* System generated locals */
    integer i__1;

    /* Local variables */
    static integer i__;
    static real stemp;
    static integer ix, iy;


/*   --Reference BLAS level1 routine (version 3.8.0) -- */
/*   --Reference BLAS is a software package provided by Univ. of Tennessee,    -- */
/*   --Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     November 2017 */

/*     .. Scalar Arguments .. */
/*     .. */
/*     .. Array Arguments .. */
/*     .. */
/*     .. Local Scalars .. */
/*     .. */
    /* Parameter adjustments */
    --sy;
    --sx;

    /* Function Body */
    if (*n <= 0) {
	return 0;
    }
    if (*incx == 1 && *incy == 1) {
	i__1 = *n;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    stemp = *c__ * sx[i__] + *s * sy[i__];
	    sy[i__] = *c__ * sy[i__] - *s * sx[i__];
	    sx[i__] = stemp;
	}
    } else {
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
	    stemp = *c__ * sx[ix] + *s * sy[iy];
	    sy[iy] = *c__ * sy[iy] - *s * sx[ix];
	    sx[ix] = stemp;
	    ix += *incx;
	    iy += *incy;
	}
    }
    return 0;
} /* srot_ */

/* Subroutine */ int srotm_(n, sx, incx, sy, incy, sparam)
integer *n;
real *sx;
integer *incx;
real *sy;
integer *incy;
real *sparam;
{
    /* Initialized data */

    static real zero = (float)0.;
    static real two = (float)2.;

    /* System generated locals */
    integer i__1, i__2;

    /* Local variables */
    static integer i__;
    static real w, z__, sflag;
    static integer kx, ky, nsteps;
    static real sh11, sh12, sh21, sh22;


/*   --Reference BLAS level1 routine (version 3.8.0) -- */
/*   --Reference BLAS is a software package provided by Univ. of Tennessee,    -- */
/*   --Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     November 2017 */

/*     .. Scalar Arguments .. */
/*     .. */
/*     .. Array Arguments .. */
/*     .. */

/*   ==================================================================== */

/*     .. Local Scalars .. */
/*     .. */
/*     .. Data statements .. */
    /* Parameter adjustments */
    --sparam;
    --sy;
    --sx;

    /* Function Body */
/*     .. */

    sflag = sparam[1];
    if (*n <= 0 || sflag + two == zero) {
	return 0;
    }
    if (*incx == *incy && *incx > 0) {

	nsteps = *n * *incx;
	if (sflag < zero) {
	    sh11 = sparam[2];
	    sh12 = sparam[4];
	    sh21 = sparam[3];
	    sh22 = sparam[5];
	    i__1 = nsteps;
	    i__2 = *incx;
	    for (i__ = 1; i__2 < 0 ? i__ >= i__1 : i__ <= i__1; i__ += i__2) {
		w = sx[i__];
		z__ = sy[i__];
		sx[i__] = w * sh11 + z__ * sh12;
		sy[i__] = w * sh21 + z__ * sh22;
	    }
	} else if (sflag == zero) {
	    sh12 = sparam[4];
	    sh21 = sparam[3];
	    i__2 = nsteps;
	    i__1 = *incx;
	    for (i__ = 1; i__1 < 0 ? i__ >= i__2 : i__ <= i__2; i__ += i__1) {
		w = sx[i__];
		z__ = sy[i__];
		sx[i__] = w + z__ * sh12;
		sy[i__] = w * sh21 + z__;
	    }
	} else {
	    sh11 = sparam[2];
	    sh22 = sparam[5];
	    i__1 = nsteps;
	    i__2 = *incx;
	    for (i__ = 1; i__2 < 0 ? i__ >= i__1 : i__ <= i__1; i__ += i__2) {
		w = sx[i__];
		z__ = sy[i__];
		sx[i__] = w * sh11 + z__;
		sy[i__] = -w + sh22 * z__;
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

	if (sflag < zero) {
	    sh11 = sparam[2];
	    sh12 = sparam[4];
	    sh21 = sparam[3];
	    sh22 = sparam[5];
	    i__2 = *n;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		w = sx[kx];
		z__ = sy[ky];
		sx[kx] = w * sh11 + z__ * sh12;
		sy[ky] = w * sh21 + z__ * sh22;
		kx += *incx;
		ky += *incy;
	    }
	} else if (sflag == zero) {
	    sh12 = sparam[4];
	    sh21 = sparam[3];
	    i__2 = *n;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		w = sx[kx];
		z__ = sy[ky];
		sx[kx] = w + z__ * sh12;
		sy[ky] = w * sh21 + z__;
		kx += *incx;
		ky += *incy;
	    }
	} else {
	    sh11 = sparam[2];
	    sh22 = sparam[5];
	    i__2 = *n;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		w = sx[kx];
		z__ = sy[ky];
		sx[kx] = w * sh11 + z__;
		sy[ky] = -w + sh22 * z__;
		kx += *incx;
		ky += *incy;
	    }
	}
    }
    return 0;
} /* srotm_ */

#endif
