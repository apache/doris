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
static real c_b43 = (float)1.;

/* Main program */ int main(void)
{
    /* Initialized data */

    static real sfac = (float)9.765625e-4;

    /* Local variables */
    extern /* Subroutine */ int check1_(real*), check2_(real*);
    static integer ic;
    extern /* Subroutine */ int header_(void);

/*     Test program for the COMPLEX    Level 1 CBLAS. */
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

    static char l[15][13] = {"CBLAS_CDOTC " , "CBLAS_CDOTU " , "CBLAS_CAXPY " ,
    "CBLAS_CCOPY " , "CBLAS_CSWAP " , "CBLAS_SCNRM2" , "CBLAS_SCASUM" , "CBLAS_CSCAL " ,
    "CBLAS_CSSCAL" , "CBLAS_ICAMAX" };

    /* Format strings */

    /* Builtin functions */
    integer s_wsfe(void), do_fio(void), e_wsfe(void);

/*     .. Parameters .. */
/*     .. Scalars in Common .. */
/*     .. Local Arrays .. */
/*     .. Common blocks .. */
/*     .. Data statements .. */
/*     .. Executable Statements .. */
    printf("Test of subprogram number %3d         %15s", combla_1.icase, l[combla_1.icase - 1]);
    return 0;

} /* header_ */

/* Subroutine */ int check1_(real* sfac)
{
    /* Initialized data */

    static real strue2[5] = { (float)0.,(float).5,(float).6,(float).7,(float)
	    .7 };
    static real strue4[5] = { (float)0.,(float).7,(float)1.,(float)1.3,(float)
	    1.7 };
    static complex ctrue5[80]	/* was [8][5][2] */ = { {(float).1,(float).1},
	    {(float)1.,(float)2.},{(float)1.,(float)2.},{(float)1.,(float)2.},
	    {(float)1.,(float)2.},{(float)1.,(float)2.},{(float)1.,(float)2.},
	    {(float)1.,(float)2.},{(float)-.16,(float)-.37},{(float)3.,(float)
	    4.},{(float)3.,(float)4.},{(float)3.,(float)4.},{(float)3.,(float)
	    4.},{(float)3.,(float)4.},{(float)3.,(float)4.},{(float)3.,(float)
	    4.},{(float)-.17,(float)-.19},{(float).13,(float)-.39},{(float)5.,
	    (float)6.},{(float)5.,(float)6.},{(float)5.,(float)6.},{(float)5.,
	    (float)6.},{(float)5.,(float)6.},{(float)5.,(float)6.},{(float)
	    .11,(float)-.03},{(float)-.17,(float).46},{(float)-.17,(float)
	    -.19},{(float)7.,(float)8.},{(float)7.,(float)8.},{(float)7.,(
	    float)8.},{(float)7.,(float)8.},{(float)7.,(float)8.},{(float).19,
	    (float)-.17},{(float).32,(float).09},{(float).23,(float)-.24},{(
	    float).18,(float).01},{(float)2.,(float)3.},{(float)2.,(float)3.},
	    {(float)2.,(float)3.},{(float)2.,(float)3.},{(float).1,(float).1},
	    {(float)4.,(float)5.},{(float)4.,(float)5.},{(float)4.,(float)5.},
	    {(float)4.,(float)5.},{(float)4.,(float)5.},{(float)4.,(float)5.},
	    {(float)4.,(float)5.},{(float)-.16,(float)-.37},{(float)6.,(float)
	    7.},{(float)6.,(float)7.},{(float)6.,(float)7.},{(float)6.,(float)
	    7.},{(float)6.,(float)7.},{(float)6.,(float)7.},{(float)6.,(float)
	    7.},{(float)-.17,(float)-.19},{(float)8.,(float)9.},{(float).13,(
	    float)-.39},{(float)2.,(float)5.},{(float)2.,(float)5.},{(float)
	    2.,(float)5.},{(float)2.,(float)5.},{(float)2.,(float)5.},{(float)
	    .11,(float)-.03},{(float)3.,(float)6.},{(float)-.17,(float).46},{(
	    float)4.,(float)7.},{(float)-.17,(float)-.19},{(float)7.,(float)
	    2.},{(float)7.,(float)2.},{(float)7.,(float)2.},{(float).19,(
	    float)-.17},{(float)5.,(float)8.},{(float).32,(float).09},{(float)
	    6.,(float)9.},{(float).23,(float)-.24},{(float)8.,(float)3.},{(
	    float).18,(float).01},{(float)9.,(float)4.} };
    static complex ctrue6[80]	/* was [8][5][2] */ = { {(float).1,(float).1},
	    {(float)1.,(float)2.},{(float)1.,(float)2.},{(float)1.,(float)2.},
	    {(float)1.,(float)2.},{(float)1.,(float)2.},{(float)1.,(float)2.},
	    {(float)1.,(float)2.},{(float).09,(float)-.12},{(float)3.,(float)
	    4.},{(float)3.,(float)4.},{(float)3.,(float)4.},{(float)3.,(float)
	    4.},{(float)3.,(float)4.},{(float)3.,(float)4.},{(float)3.,(float)
	    4.},{(float).03,(float)-.09},{(float).15,(float)-.03},{(float)5.,(
	    float)6.},{(float)5.,(float)6.},{(float)5.,(float)6.},{(float)5.,(
	    float)6.},{(float)5.,(float)6.},{(float)5.,(float)6.},{(float).03,
	    (float).03},{(float)-.18,(float).03},{(float).03,(float)-.09},{(
	    float)7.,(float)8.},{(float)7.,(float)8.},{(float)7.,(float)8.},{(
	    float)7.,(float)8.},{(float)7.,(float)8.},{(float).09,(float).03},
	    {(float).03,(float).12},{(float).12,(float).03},{(float).03,(
	    float).06},{(float)2.,(float)3.},{(float)2.,(float)3.},{(float)2.,
	    (float)3.},{(float)2.,(float)3.},{(float).1,(float).1},{(float)4.,
	    (float)5.},{(float)4.,(float)5.},{(float)4.,(float)5.},{(float)4.,
	    (float)5.},{(float)4.,(float)5.},{(float)4.,(float)5.},{(float)4.,
	    (float)5.},{(float).09,(float)-.12},{(float)6.,(float)7.},{(float)
	    6.,(float)7.},{(float)6.,(float)7.},{(float)6.,(float)7.},{(float)
	    6.,(float)7.},{(float)6.,(float)7.},{(float)6.,(float)7.},{(float)
	    .03,(float)-.09},{(float)8.,(float)9.},{(float).15,(float)-.03},{(
	    float)2.,(float)5.},{(float)2.,(float)5.},{(float)2.,(float)5.},{(
	    float)2.,(float)5.},{(float)2.,(float)5.},{(float).03,(float).03},
	    {(float)3.,(float)6.},{(float)-.18,(float).03},{(float)4.,(float)
	    7.},{(float).03,(float)-.09},{(float)7.,(float)2.},{(float)7.,(
	    float)2.},{(float)7.,(float)2.},{(float).09,(float).03},{(float)
	    5.,(float)8.},{(float).03,(float).12},{(float)6.,(float)9.},{(
	    float).12,(float).03},{(float)8.,(float)3.},{(float).03,(float)
	    .06},{(float)9.,(float)4.} };
    static integer itrue3[5] = { 0,1,2,2,2 };
    static real sa = (float).3;
    static complex ca = {(float).4,(float)-.7};
    static complex cv[80]	/* was [8][5][2] */ = { {(float).1,(float).1},
	    {(float)1.,(float)2.},{(float)1.,(float)2.},{(float)1.,(float)2.},
	    {(float)1.,(float)2.},{(float)1.,(float)2.},{(float)1.,(float)2.},
	    {(float)1.,(float)2.},{(float).3,(float)-.4},{(float)3.,(float)4.}
	    ,{(float)3.,(float)4.},{(float)3.,(float)4.},{(float)3.,(float)4.}
	    ,{(float)3.,(float)4.},{(float)3.,(float)4.},{(float)3.,(float)4.}
	    ,{(float).1,(float)-.3},{(float).5,(float)-.1},{(float)5.,(float)
	    6.},{(float)5.,(float)6.},{(float)5.,(float)6.},{(float)5.,(float)
	    6.},{(float)5.,(float)6.},{(float)5.,(float)6.},{(float).1,(float)
	    .1},{(float)-.6,(float).1},{(float).1,(float)-.3},{(float)7.,(
	    float)8.},{(float)7.,(float)8.},{(float)7.,(float)8.},{(float)7.,(
	    float)8.},{(float)7.,(float)8.},{(float).3,(float).1},{(float).1,(
	    float).4},{(float).4,(float).1},{(float).1,(float).2},{(float)2.,(
	    float)3.},{(float)2.,(float)3.},{(float)2.,(float)3.},{(float)2.,(
	    float)3.},{(float).1,(float).1},{(float)4.,(float)5.},{(float)4.,(
	    float)5.},{(float)4.,(float)5.},{(float)4.,(float)5.},{(float)4.,(
	    float)5.},{(float)4.,(float)5.},{(float)4.,(float)5.},{(float).3,(
	    float)-.4},{(float)6.,(float)7.},{(float)6.,(float)7.},{(float)6.,
	    (float)7.},{(float)6.,(float)7.},{(float)6.,(float)7.},{(float)6.,
	    (float)7.},{(float)6.,(float)7.},{(float).1,(float)-.3},{(float)
	    8.,(float)9.},{(float).5,(float)-.1},{(float)2.,(float)5.},{(
	    float)2.,(float)5.},{(float)2.,(float)5.},{(float)2.,(float)5.},{(
	    float)2.,(float)5.},{(float).1,(float).1},{(float)3.,(float)6.},{(
	    float)-.6,(float).1},{(float)4.,(float)7.},{(float).1,(float)-.3},
	    {(float)7.,(float)2.},{(float)7.,(float)2.},{(float)7.,(float)2.},
	    {(float).3,(float).1},{(float)5.,(float)8.},{(float).1,(float).4},
	    {(float)6.,(float)9.},{(float).4,(float).1},{(float)8.,(float)3.},
	    {(float).1,(float).2},{(float)9.,(float)4.} };

    /* System generated locals */
    integer i__1, i__2, i__3;
    real r__1;
    complex q__1;

    /* Local variables */
    static integer i__;
    extern /* Subroutine */ int ctest_(integer*, complex*, complex*, complex*, real*);
    static complex mwpcs[5], mwpct[5];
    extern /* Subroutine */ int itest1_(integer*, integer*), stest1_(real*,real*,real*,real*);
    extern /* Subroutine */ int cscaltest_(integer*, complex*, complex*, integer*);
    static complex cx[8];
    extern real scnrm2test_(integer*, complex*, integer*);
    static integer np1;
    extern integer icamaxtest_(integer*, complex*, integer*);
    extern /* Subroutine */ int csscaltest_(integer*, real*, complex*, integer*);
    extern real scasumtest_(integer*, complex*, integer*);
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
/*              .. SCNRM2TEST .. */
		r__1 = scnrm2test_(&combla_1.n, cx, &combla_1.incx);
		stest1_(&r__1, &strue2[np1 - 1], &strue2[np1 - 1], sfac);
	    } else if (combla_1.icase == 7) {
/*              .. SCASUMTEST .. */
		r__1 = scasumtest_(&combla_1.n, cx, &combla_1.incx);
		stest1_(&r__1, &strue4[np1 - 1], &strue4[np1 - 1], sfac);
	    } else if (combla_1.icase == 8) {
/*              .. CSCAL .. */
		cscaltest_(&combla_1.n, &ca, cx, &combla_1.incx);
		ctest_(&len, cx, &ctrue5[(np1 + combla_1.incx * 5 << 3) - 48],
			 &ctrue5[(np1 + combla_1.incx * 5 << 3) - 48], sfac);
	    } else if (combla_1.icase == 9) {
/*              .. CSSCALTEST .. */
		csscaltest_(&combla_1.n, &sa, cx, &combla_1.incx);
		ctest_(&len, cx, &ctrue6[(np1 + combla_1.incx * 5 << 3) - 48],
			 &ctrue6[(np1 + combla_1.incx * 5 << 3) - 48], sfac);
	    } else if (combla_1.icase == 10) {
/*              .. ICAMAXTEST .. */
		i__1 = icamaxtest_(&combla_1.n, cx, &combla_1.incx);
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
/*        CSCAL */
/*        Add a test for alpha equal to zero. */
	ca.r = (float)0., ca.i = (float)0.;
	for (i__ = 1; i__ <= 5; ++i__) {
	    i__1 = i__ - 1;
	    mwpct[i__1].r = (float)0., mwpct[i__1].i = (float)0.;
	    i__1 = i__ - 1;
	    mwpcs[i__1].r = (float)1., mwpcs[i__1].i = (float)1.;
/* L80: */
	}
	cscaltest_(&c__5, &ca, cx, &combla_1.incx);
	ctest_(&c__5, cx, mwpct, mwpcs, sfac);
    } else if (combla_1.icase == 9) {
/*        CSSCALTEST */
/*        Add a test for alpha equal to zero. */
	sa = (float)0.;
	for (i__ = 1; i__ <= 5; ++i__) {
	    i__1 = i__ - 1;
	    mwpct[i__1].r = (float)0., mwpct[i__1].i = (float)0.;
	    i__1 = i__ - 1;
	    mwpcs[i__1].r = (float)1., mwpcs[i__1].i = (float)1.;
/* L100: */
	}
	csscaltest_(&c__5, &sa, cx, &combla_1.incx);
	ctest_(&c__5, cx, mwpct, mwpcs, sfac);
/*        Add a test for alpha equal to one. */
	sa = (float)1.;
	for (i__ = 1; i__ <= 5; ++i__) {
	    i__1 = i__ - 1;
	    i__2 = i__ - 1;
	    mwpct[i__1].r = cx[i__2].r, mwpct[i__1].i = cx[i__2].i;
	    i__1 = i__ - 1;
	    i__2 = i__ - 1;
	    mwpcs[i__1].r = cx[i__2].r, mwpcs[i__1].i = cx[i__2].i;
/* L120: */
	}
	csscaltest_(&c__5, &sa, cx, &combla_1.incx);
	ctest_(&c__5, cx, mwpct, mwpcs, sfac);
/*        Add a test for alpha equal to minus one. */
	sa = (float)-1.;
	for (i__ = 1; i__ <= 5; ++i__) {
	    i__1 = i__ - 1;
	    i__2 = i__ - 1;
	    q__1.r = -cx[i__2].r, q__1.i = -cx[i__2].i;
	    mwpct[i__1].r = q__1.r, mwpct[i__1].i = q__1.i;
	    i__1 = i__ - 1;
	    i__2 = i__ - 1;
	    q__1.r = -cx[i__2].r, q__1.i = -cx[i__2].i;
	    mwpcs[i__1].r = q__1.r, mwpcs[i__1].i = q__1.i;
/* L140: */
	}
	csscaltest_(&c__5, &sa, cx, &combla_1.incx);
	ctest_(&c__5, cx, mwpct, mwpcs, sfac);
    }
    return 0;
} /* check1_ */

/* Subroutine */ int check2_(real* sfac)
{
    /* Initialized data */

    static complex ca = {(float).4,(float)-.7};
    static integer incxs[4] = { 1,2,-2,-1 };
    static integer incys[4] = { 1,-2,1,-2 };
    static integer lens[8]	/* was [4][2] */ = { 1,1,2,4,1,1,3,7 };
    static integer ns[4] = { 0,1,2,4 };
    static complex cx1[7] = { {(float).7,(float)-.8},{(float)-.4,(float)-.7},{
	    (float)-.1,(float)-.9},{(float).2,(float)-.8},{(float)-.9,(float)
	    -.4},{(float).1,(float).4},{(float)-.6,(float).6} };
    static complex cy1[7] = { {(float).6,(float)-.6},{(float)-.9,(float).5},{(
	    float).7,(float)-.6},{(float).1,(float)-.5},{(float)-.1,(float)
	    -.2},{(float)-.5,(float)-.3},{(float).8,(float)-.7} };
    static complex ct8[112]	/* was [7][4][4] */ = { {(float).6,(float)-.6}
	    ,{(float)0.,(float)0.},{(float)0.,(float)0.},{(float)0.,(float)0.}
	    ,{(float)0.,(float)0.},{(float)0.,(float)0.},{(float)0.,(float)0.}
	    ,{(float).32,(float)-1.41},{(float)0.,(float)0.},{(float)0.,(
	    float)0.},{(float)0.,(float)0.},{(float)0.,(float)0.},{(float)0.,(
	    float)0.},{(float)0.,(float)0.},{(float).32,(float)-1.41},{(float)
	    -1.55,(float).5},{(float)0.,(float)0.},{(float)0.,(float)0.},{(
	    float)0.,(float)0.},{(float)0.,(float)0.},{(float)0.,(float)0.},{(
	    float).32,(float)-1.41},{(float)-1.55,(float).5},{(float).03,(
	    float)-.89},{(float)-.38,(float)-.96},{(float)0.,(float)0.},{(
	    float)0.,(float)0.},{(float)0.,(float)0.},{(float).6,(float)-.6},{
	    (float)0.,(float)0.},{(float)0.,(float)0.},{(float)0.,(float)0.},{
	    (float)0.,(float)0.},{(float)0.,(float)0.},{(float)0.,(float)0.},{
	    (float).32,(float)-1.41},{(float)0.,(float)0.},{(float)0.,(float)
	    0.},{(float)0.,(float)0.},{(float)0.,(float)0.},{(float)0.,(float)
	    0.},{(float)0.,(float)0.},{(float)-.07,(float)-.89},{(float)-.9,(
	    float).5},{(float).42,(float)-1.41},{(float)0.,(float)0.},{(float)
	    0.,(float)0.},{(float)0.,(float)0.},{(float)0.,(float)0.},{(float)
	    .78,(float).06},{(float)-.9,(float).5},{(float).06,(float)-.13},{(
	    float).1,(float)-.5},{(float)-.77,(float)-.49},{(float)-.5,(float)
	    -.3},{(float).52,(float)-1.51},{(float).6,(float)-.6},{(float)0.,(
	    float)0.},{(float)0.,(float)0.},{(float)0.,(float)0.},{(float)0.,(
	    float)0.},{(float)0.,(float)0.},{(float)0.,(float)0.},{(float).32,
	    (float)-1.41},{(float)0.,(float)0.},{(float)0.,(float)0.},{(float)
	    0.,(float)0.},{(float)0.,(float)0.},{(float)0.,(float)0.},{(float)
	    0.,(float)0.},{(float)-.07,(float)-.89},{(float)-1.18,(float)-.31}
	    ,{(float)0.,(float)0.},{(float)0.,(float)0.},{(float)0.,(float)0.}
	    ,{(float)0.,(float)0.},{(float)0.,(float)0.},{(float).78,(float)
	    .06},{(float)-1.54,(float).97},{(float).03,(float)-.89},{(float)
	    -.18,(float)-1.31},{(float)0.,(float)0.},{(float)0.,(float)0.},{(
	    float)0.,(float)0.},{(float).6,(float)-.6},{(float)0.,(float)0.},{
	    (float)0.,(float)0.},{(float)0.,(float)0.},{(float)0.,(float)0.},{
	    (float)0.,(float)0.},{(float)0.,(float)0.},{(float).32,(float)
	    -1.41},{(float)0.,(float)0.},{(float)0.,(float)0.},{(float)0.,(
	    float)0.},{(float)0.,(float)0.},{(float)0.,(float)0.},{(float)0.,(
	    float)0.},{(float).32,(float)-1.41},{(float)-.9,(float).5},{(
	    float).05,(float)-.6},{(float)0.,(float)0.},{(float)0.,(float)0.},
	    {(float)0.,(float)0.},{(float)0.,(float)0.},{(float).32,(float)
	    -1.41},{(float)-.9,(float).5},{(float).05,(float)-.6},{(float).1,(
	    float)-.5},{(float)-.77,(float)-.49},{(float)-.5,(float)-.3},{(
	    float).32,(float)-1.16} };
    static complex ct7[16]	/* was [4][4] */ = { {(float)0.,(float)0.},{(
	    float)-.06,(float)-.9},{(float).65,(float)-.47},{(float)-.34,(
	    float)-1.22},{(float)0.,(float)0.},{(float)-.06,(float)-.9},{(
	    float)-.59,(float)-1.46},{(float)-1.04,(float)-.04},{(float)0.,(
	    float)0.},{(float)-.06,(float)-.9},{(float)-.83,(float).59},{(
	    float).07,(float)-.37},{(float)0.,(float)0.},{(float)-.06,(float)
	    -.9},{(float)-.76,(float)-1.15},{(float)-1.33,(float)-1.82} };
    static complex ct6[16]	/* was [4][4] */ = { {(float)0.,(float)0.},{(
	    float).9,(float).06},{(float).91,(float)-.77},{(float)1.8,(float)
	    -.1},{(float)0.,(float)0.},{(float).9,(float).06},{(float)1.45,(
	    float).74},{(float).2,(float).9},{(float)0.,(float)0.},{(float).9,
	    (float).06},{(float)-.55,(float).23},{(float).83,(float)-.39},{(
	    float)0.,(float)0.},{(float).9,(float).06},{(float)1.04,(float)
	    .79},{(float)1.95,(float)1.22} };
    static complex ct10x[112]	/* was [7][4][4] */ = { {(float).7,(float)-.8}
	    ,{(float)0.,(float)0.},{(float)0.,(float)0.},{(float)0.,(float)0.}
	    ,{(float)0.,(float)0.},{(float)0.,(float)0.},{(float)0.,(float)0.}
	    ,{(float).6,(float)-.6},{(float)0.,(float)0.},{(float)0.,(float)
	    0.},{(float)0.,(float)0.},{(float)0.,(float)0.},{(float)0.,(float)
	    0.},{(float)0.,(float)0.},{(float).6,(float)-.6},{(float)-.9,(
	    float).5},{(float)0.,(float)0.},{(float)0.,(float)0.},{(float)0.,(
	    float)0.},{(float)0.,(float)0.},{(float)0.,(float)0.},{(float).6,(
	    float)-.6},{(float)-.9,(float).5},{(float).7,(float)-.6},{(float)
	    .1,(float)-.5},{(float)0.,(float)0.},{(float)0.,(float)0.},{(
	    float)0.,(float)0.},{(float).7,(float)-.8},{(float)0.,(float)0.},{
	    (float)0.,(float)0.},{(float)0.,(float)0.},{(float)0.,(float)0.},{
	    (float)0.,(float)0.},{(float)0.,(float)0.},{(float).6,(float)-.6},
	    {(float)0.,(float)0.},{(float)0.,(float)0.},{(float)0.,(float)0.},
	    {(float)0.,(float)0.},{(float)0.,(float)0.},{(float)0.,(float)0.},
	    {(float).7,(float)-.6},{(float)-.4,(float)-.7},{(float).6,(float)
	    -.6},{(float)0.,(float)0.},{(float)0.,(float)0.},{(float)0.,(
	    float)0.},{(float)0.,(float)0.},{(float).8,(float)-.7},{(float)
	    -.4,(float)-.7},{(float)-.1,(float)-.2},{(float).2,(float)-.8},{(
	    float).7,(float)-.6},{(float).1,(float).4},{(float).6,(float)-.6},
	    {(float).7,(float)-.8},{(float)0.,(float)0.},{(float)0.,(float)0.}
	    ,{(float)0.,(float)0.},{(float)0.,(float)0.},{(float)0.,(float)0.}
	    ,{(float)0.,(float)0.},{(float).6,(float)-.6},{(float)0.,(float)
	    0.},{(float)0.,(float)0.},{(float)0.,(float)0.},{(float)0.,(float)
	    0.},{(float)0.,(float)0.},{(float)0.,(float)0.},{(float)-.9,(
	    float).5},{(float)-.4,(float)-.7},{(float).6,(float)-.6},{(float)
	    0.,(float)0.},{(float)0.,(float)0.},{(float)0.,(float)0.},{(float)
	    0.,(float)0.},{(float).1,(float)-.5},{(float)-.4,(float)-.7},{(
	    float).7,(float)-.6},{(float).2,(float)-.8},{(float)-.9,(float).5}
	    ,{(float).1,(float).4},{(float).6,(float)-.6},{(float).7,(float)
	    -.8},{(float)0.,(float)0.},{(float)0.,(float)0.},{(float)0.,(
	    float)0.},{(float)0.,(float)0.},{(float)0.,(float)0.},{(float)0.,(
	    float)0.},{(float).6,(float)-.6},{(float)0.,(float)0.},{(float)0.,
	    (float)0.},{(float)0.,(float)0.},{(float)0.,(float)0.},{(float)0.,
	    (float)0.},{(float)0.,(float)0.},{(float).6,(float)-.6},{(float)
	    .7,(float)-.6},{(float)0.,(float)0.},{(float)0.,(float)0.},{(
	    float)0.,(float)0.},{(float)0.,(float)0.},{(float)0.,(float)0.},{(
	    float).6,(float)-.6},{(float).7,(float)-.6},{(float)-.1,(float)
	    -.2},{(float).8,(float)-.7},{(float)0.,(float)0.},{(float)0.,(
	    float)0.},{(float)0.,(float)0.} };
    static complex ct10y[112]	/* was [7][4][4] */ = { {(float).6,(float)-.6}
	    ,{(float)0.,(float)0.},{(float)0.,(float)0.},{(float)0.,(float)0.}
	    ,{(float)0.,(float)0.},{(float)0.,(float)0.},{(float)0.,(float)0.}
	    ,{(float).7,(float)-.8},{(float)0.,(float)0.},{(float)0.,(float)
	    0.},{(float)0.,(float)0.},{(float)0.,(float)0.},{(float)0.,(float)
	    0.},{(float)0.,(float)0.},{(float).7,(float)-.8},{(float)-.4,(
	    float)-.7},{(float)0.,(float)0.},{(float)0.,(float)0.},{(float)0.,
	    (float)0.},{(float)0.,(float)0.},{(float)0.,(float)0.},{(float).7,
	    (float)-.8},{(float)-.4,(float)-.7},{(float)-.1,(float)-.9},{(
	    float).2,(float)-.8},{(float)0.,(float)0.},{(float)0.,(float)0.},{
	    (float)0.,(float)0.},{(float).6,(float)-.6},{(float)0.,(float)0.},
	    {(float)0.,(float)0.},{(float)0.,(float)0.},{(float)0.,(float)0.},
	    {(float)0.,(float)0.},{(float)0.,(float)0.},{(float).7,(float)-.8}
	    ,{(float)0.,(float)0.},{(float)0.,(float)0.},{(float)0.,(float)0.}
	    ,{(float)0.,(float)0.},{(float)0.,(float)0.},{(float)0.,(float)0.}
	    ,{(float)-.1,(float)-.9},{(float)-.9,(float).5},{(float).7,(float)
	    -.8},{(float)0.,(float)0.},{(float)0.,(float)0.},{(float)0.,(
	    float)0.},{(float)0.,(float)0.},{(float)-.6,(float).6},{(float)
	    -.9,(float).5},{(float)-.9,(float)-.4},{(float).1,(float)-.5},{(
	    float)-.1,(float)-.9},{(float)-.5,(float)-.3},{(float).7,(float)
	    -.8},{(float).6,(float)-.6},{(float)0.,(float)0.},{(float)0.,(
	    float)0.},{(float)0.,(float)0.},{(float)0.,(float)0.},{(float)0.,(
	    float)0.},{(float)0.,(float)0.},{(float).7,(float)-.8},{(float)0.,
	    (float)0.},{(float)0.,(float)0.},{(float)0.,(float)0.},{(float)0.,
	    (float)0.},{(float)0.,(float)0.},{(float)0.,(float)0.},{(float)
	    -.1,(float)-.9},{(float).7,(float)-.8},{(float)0.,(float)0.},{(
	    float)0.,(float)0.},{(float)0.,(float)0.},{(float)0.,(float)0.},{(
	    float)0.,(float)0.},{(float)-.6,(float).6},{(float)-.9,(float)-.4}
	    ,{(float)-.1,(float)-.9},{(float).7,(float)-.8},{(float)0.,(float)
	    0.},{(float)0.,(float)0.},{(float)0.,(float)0.},{(float).6,(float)
	    -.6},{(float)0.,(float)0.},{(float)0.,(float)0.},{(float)0.,(
	    float)0.},{(float)0.,(float)0.},{(float)0.,(float)0.},{(float)0.,(
	    float)0.},{(float).7,(float)-.8},{(float)0.,(float)0.},{(float)0.,
	    (float)0.},{(float)0.,(float)0.},{(float)0.,(float)0.},{(float)0.,
	    (float)0.},{(float)0.,(float)0.},{(float).7,(float)-.8},{(float)
	    -.9,(float).5},{(float)-.4,(float)-.7},{(float)0.,(float)0.},{(
	    float)0.,(float)0.},{(float)0.,(float)0.},{(float)0.,(float)0.},{(
	    float).7,(float)-.8},{(float)-.9,(float).5},{(float)-.4,(float)
	    -.7},{(float).1,(float)-.5},{(float)-.1,(float)-.9},{(float)-.5,(
	    float)-.3},{(float).2,(float)-.8} };
    static complex csize1[4] = { {(float)0.,(float)0.},{(float).9,(float).9},{
	    (float)1.63,(float)1.73},{(float)2.9,(float)2.78} };
    static complex csize3[14] = { {(float)0.,(float)0.},{(float)0.,(float)0.},
	    {(float)0.,(float)0.},{(float)0.,(float)0.},{(float)0.,(float)0.},
	    {(float)0.,(float)0.},{(float)0.,(float)0.},{(float)1.17,(float)
	    1.17},{(float)1.17,(float)1.17},{(float)1.17,(float)1.17},{(float)
	    1.17,(float)1.17},{(float)1.17,(float)1.17},{(float)1.17,(float)
	    1.17},{(float)1.17,(float)1.17} };
    static complex csize2[14]	/* was [7][2] */ = { {(float)0.,(float)0.},{(
	    float)0.,(float)0.},{(float)0.,(float)0.},{(float)0.,(float)0.},{(
	    float)0.,(float)0.},{(float)0.,(float)0.},{(float)0.,(float)0.},{(
	    float)1.54,(float)1.54},{(float)1.54,(float)1.54},{(float)1.54,(
	    float)1.54},{(float)1.54,(float)1.54},{(float)1.54,(float)1.54},{(
	    float)1.54,(float)1.54},{(float)1.54,(float)1.54} };

    /* System generated locals */
    integer i__1, i__2;

    /* Local variables */
    static complex cdot[1];
    static integer lenx, leny, i__;
    static complex ctemp;
    extern /* Subroutine */ int ctest_(integer*, complex*, complex*, complex*, real*);
    static integer ksize;
    extern /* Subroutine */ int cdotctest_(integer*, complex*, integer*, complex*, integer*,complex*), ccopytest_(integer*, complex*, integer*, complex*, integer*), cdotutest_(integer*, complex*, integer*, complex*, integer*, complex*), 
	    cswaptest_(integer*, complex*, integer*, complex*, integer*), caxpytest_(integer*, complex*, complex*, integer*, complex*, integer*);
    static integer ki, kn;
    static complex cx[7], cy[7];
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
/*              .. CDOTCTEST .. */
		cdotctest_(&combla_1.n, cx, &combla_1.incx, cy, &
			combla_1.incy, &ctemp);
		cdot[0].r = ctemp.r, cdot[0].i = ctemp.i;
		ctest_(&c__1, cdot, &ct6[kn + (ki << 2) - 5], &csize1[kn - 1],
			 sfac);
	    } else if (combla_1.icase == 2) {
/*              .. CDOTUTEST .. */
		cdotutest_(&combla_1.n, cx, &combla_1.incx, cy, &
			combla_1.incy, &ctemp);
		cdot[0].r = ctemp.r, cdot[0].i = ctemp.i;
		ctest_(&c__1, cdot, &ct7[kn + (ki << 2) - 5], &csize1[kn - 1],
			 sfac);
	    } else if (combla_1.icase == 3) {
/*              .. CAXPYTEST .. */
		caxpytest_(&combla_1.n, &ca, cx, &combla_1.incx, cy, &
			combla_1.incy);
		ctest_(&leny, cy, &ct8[(kn + (ki << 2)) * 7 - 35], &csize2[
			ksize * 7 - 7], sfac);
	    } else if (combla_1.icase == 4) {
/*              .. CCOPYTEST .. */
		ccopytest_(&combla_1.n, cx, &combla_1.incx, cy, &
			combla_1.incy);
		ctest_(&leny, cy, &ct10y[(kn + (ki << 2)) * 7 - 35], csize3, &
			c_b43);
	    } else if (combla_1.icase == 5) {
/*              .. CSWAPTEST .. */
		cswaptest_(&combla_1.n, cx, &combla_1.incx, cy, &
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

/* Subroutine */ int stest_(integer* len, real* scomp, real* strue, real* ssize,real* sfac)
{
    /* System generated locals */
    integer i__1;
    real r__1, r__2, r__3, r__4, r__5;

    /* Local variables */
    static integer i__;
    extern doublereal sdiff_(real*, real*);
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
	printf("CASE  N INCX INCY MODE  I                             COMP(I)                             TRUE(I)  DIFFERENCE     SIZE(I)\n");
L20:
        printf("%4d %3d %5d %5d %5d %3d %36.8e %36.8e %12.4e %12.4e\n",combla_1.icase, combla_1.n, combla_1.incx, combla_1.incy,
	combla_1.mode, i__, scomp[i__], strue[i__], sd, ssize[i__]);
L40:
	;
    }
    return 0;

} /* stest_ */

/* Subroutine */ int stest1_(real* scomp1, real* strue1, real* ssize, real* sfac)
{
    static real scomp[1], strue[1];
    extern /* Subroutine */ int stest_(integer*, real*, real*, real*, real*);

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

/* Subroutine */ int ctest_(integer* len, complex* ccomp, complex* ctrue, complex* csize, real* sfac)
{
    /* System generated locals */
    integer i__1, i__2;

    /* Builtin functions */
//    double r_imag();

    /* Local variables */
    static integer i__;
    static real scomp[20], ssize[20], strue[20];
    extern /* Subroutine */ int stest_(integer*, real*,real*,real*,real*);

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
	scomp[(i__ << 1) - 1] = r_imag(&ccomp[i__]);
	i__2 = i__;
	strue[(i__ << 1) - 2] = ctrue[i__2].r;
	strue[(i__ << 1) - 1] = r_imag(&ctrue[i__]);
	i__2 = i__;
	ssize[(i__ << 1) - 2] = csize[i__2].r;
	ssize[(i__ << 1) - 1] = r_imag(&csize[i__]);
/* L20: */
    }

    i__1 = *len << 1;
    stest_(&i__1, scomp, strue, ssize, sfac);
    return 0;
} /* ctest_ */

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
    printf(" CASE  N INCX INCY MODE                                COMP                                TRUE     DIFFERENCE\n");
L20:
    id = *icomp - *itrue;
    printf("%4d %3d %5d %5d %5d %36d %36d %12d\n",combla_1.icase, combla_1.n, combla_1.incx, combla_1.incy, 
    combla_1.mode, *icomp, *itrue, id);
L40:
    return 0;

} /* itest1_ */

