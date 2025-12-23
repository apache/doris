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

#if defined(_WIN64)
typedef long long BLASLONG;
typedef unsigned long long BLASULONG;
#else
typedef long BLASLONG;
typedef unsigned long BLASULONG;
#endif

#ifdef LAPACK_ILP64
typedef BLASLONG blasint;
#if defined(_WIN64)
#define blasabs(x) llabs(x)
#else
#define blasabs(x) labs(x)
#endif
#else
typedef int blasint;
#define blasabs(x) abs(x)
#endif

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
typedef blasint logical;

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
#define z_div(c, a, b) {Cd(c)._Val[0] = (Cd(a)._Val[0]/Cd(b)._Val[0]); Cd(c)._Val[1]=(Cd(a)._Val[1]/df(b)._Val[1]);}
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
static char junk[] = "\n@(#)LIBF77 VERSION 19990503\n";
#define z_abs(z) (cabs(Cd(z)))
#define z_exp(R, Z) {pCd(R) = cexp(Cd(Z));}
#define z_sqrt(R, Z) {pCd(R) = csqrt(Cd(Z));}
#define myexit_() break;
#define mycycle() continue;
#define myceiling(w) {ceil(w)}
#define myhuge(w) {HUGE_VAL}
//#define mymaxloc_(w,s,e,n) {if (sizeof(*(w)) == sizeof(double)) dmaxloc_((w),*(s),*(e),n); else dmaxloc_((w),*(s),*(e),n);}
#define mymaxloc(w,s,e,n) {dmaxloc_(w,*(s),*(e),n)}

/* procedure parameter types for -A and -C++ */


#ifdef __cplusplus
typedef logical (*L_fp)(...);
#else
typedef logical (*L_fp)();
#endif

static float spow_ui(float x, integer n) {
	float pow=1.0; unsigned long int u;
	if(n != 0) {
		if(n < 0) n = -n, x = 1/x;
		for(u = n; ; ) {
			if(u & 01) pow *= x;
			if(u >>= 1) x *= x;
			else break;
		}
	}
	return pow;
}
static double dpow_ui(double x, integer n) {
	double pow=1.0; unsigned long int u;
	if(n != 0) {
		if(n < 0) n = -n, x = 1/x;
		for(u = n; ; ) {
			if(u & 01) pow *= x;
			if(u >>= 1) x *= x;
			else break;
		}
	}
	return pow;
}
#ifdef _MSC_VER
static _Fcomplex cpow_ui(complex x, integer n) {
	complex pow={1.0,0.0}; unsigned long int u;
		if(n != 0) {
		if(n < 0) n = -n, x.r = 1/x.r, x.i=1/x.i;
		for(u = n; ; ) {
			if(u & 01) pow.r *= x.r, pow.i *= x.i;
			if(u >>= 1) x.r *= x.r, x.i *= x.i;
			else break;
		}
	}
	_Fcomplex p={pow.r, pow.i};
	return p;
}
#else
static _Complex float cpow_ui(_Complex float x, integer n) {
	_Complex float pow=1.0; unsigned long int u;
	if(n != 0) {
		if(n < 0) n = -n, x = 1/x;
		for(u = n; ; ) {
			if(u & 01) pow *= x;
			if(u >>= 1) x *= x;
			else break;
		}
	}
	return pow;
}
#endif
#ifdef _MSC_VER
static _Dcomplex zpow_ui(_Dcomplex x, integer n) {
	_Dcomplex pow={1.0,0.0}; unsigned long int u;
	if(n != 0) {
		if(n < 0) n = -n, x._Val[0] = 1/x._Val[0], x._Val[1] =1/x._Val[1];
		for(u = n; ; ) {
			if(u & 01) pow._Val[0] *= x._Val[0], pow._Val[1] *= x._Val[1];
			if(u >>= 1) x._Val[0] *= x._Val[0], x._Val[1] *= x._Val[1];
			else break;
		}
	}
	_Dcomplex p = {pow._Val[0], pow._Val[1]};
	return p;
}
#else
static _Complex double zpow_ui(_Complex double x, integer n) {
	_Complex double pow=1.0; unsigned long int u;
	if(n != 0) {
		if(n < 0) n = -n, x = 1/x;
		for(u = n; ; ) {
			if(u & 01) pow *= x;
			if(u >>= 1) x *= x;
			else break;
		}
	}
	return pow;
}
#endif
static integer pow_ii(integer x, integer n) {
	integer pow; unsigned long int u;
	if (n <= 0) {
		if (n == 0 || x == 1) pow = 1;
		else if (x != -1) pow = x == 0 ? 1/x : 0;
		else n = -n;
	}
	if ((n > 0) || !(n == 0 || x == 1 || x != -1)) {
		u = n;
		for(pow = 1; ; ) {
			if(u & 01) pow *= x;
			if(u >>= 1) x *= x;
			else break;
		}
	}
	return pow;
}
static integer dmaxloc_(double *w, integer s, integer e, integer *n)
{
	double m; integer i, mi;
	for(m=w[s-1], mi=s, i=s+1; i<=e; i++)
		if (w[i-1]>m) mi=i ,m=w[i-1];
	return mi-s+1;
}
static integer smaxloc_(float *w, integer s, integer e, integer *n)
{
	float m; integer i, mi;
	for(m=w[s-1], mi=s, i=s+1; i<=e; i++)
		if (w[i-1]>m) mi=i ,m=w[i-1];
	return mi-s+1;
}
static inline void cdotc_(complex *z, integer *n_, complex *x, integer *incx_, complex *y, integer *incy_) {
	integer n = *n_, incx = *incx_, incy = *incy_, i;
#ifdef _MSC_VER
	_Fcomplex zdotc = {0.0, 0.0};
	if (incx == 1 && incy == 1) {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc._Val[0] += conjf(Cf(&x[i]))._Val[0] * Cf(&y[i])._Val[0];
			zdotc._Val[1] += conjf(Cf(&x[i]))._Val[1] * Cf(&y[i])._Val[1];
		}
	} else {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc._Val[0] += conjf(Cf(&x[i*incx]))._Val[0] * Cf(&y[i*incy])._Val[0];
			zdotc._Val[1] += conjf(Cf(&x[i*incx]))._Val[1] * Cf(&y[i*incy])._Val[1];
		}
	}
	pCf(z) = zdotc;
}
#else
	_Complex float zdotc = 0.0;
	if (incx == 1 && incy == 1) {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc += conjf(Cf(&x[i])) * Cf(&y[i]);
		}
	} else {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc += conjf(Cf(&x[i*incx])) * Cf(&y[i*incy]);
		}
	}
	pCf(z) = zdotc;
}
#endif
static inline void zdotc_(doublecomplex *z, integer *n_, doublecomplex *x, integer *incx_, doublecomplex *y, integer *incy_) {
	integer n = *n_, incx = *incx_, incy = *incy_, i;
#ifdef _MSC_VER
	_Dcomplex zdotc = {0.0, 0.0};
	if (incx == 1 && incy == 1) {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc._Val[0] += conj(Cd(&x[i]))._Val[0] * Cd(&y[i])._Val[0];
			zdotc._Val[1] += conj(Cd(&x[i]))._Val[1] * Cd(&y[i])._Val[1];
		}
	} else {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc._Val[0] += conj(Cd(&x[i*incx]))._Val[0] * Cd(&y[i*incy])._Val[0];
			zdotc._Val[1] += conj(Cd(&x[i*incx]))._Val[1] * Cd(&y[i*incy])._Val[1];
		}
	}
	pCd(z) = zdotc;
}
#else
	_Complex double zdotc = 0.0;
	if (incx == 1 && incy == 1) {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc += conj(Cd(&x[i])) * Cd(&y[i]);
		}
	} else {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc += conj(Cd(&x[i*incx])) * Cd(&y[i*incy]);
		}
	}
	pCd(z) = zdotc;
}
#endif	
static inline void cdotu_(complex *z, integer *n_, complex *x, integer *incx_, complex *y, integer *incy_) {
	integer n = *n_, incx = *incx_, incy = *incy_, i;
#ifdef _MSC_VER
	_Fcomplex zdotc = {0.0, 0.0};
	if (incx == 1 && incy == 1) {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc._Val[0] += Cf(&x[i])._Val[0] * Cf(&y[i])._Val[0];
			zdotc._Val[1] += Cf(&x[i])._Val[1] * Cf(&y[i])._Val[1];
		}
	} else {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc._Val[0] += Cf(&x[i*incx])._Val[0] * Cf(&y[i*incy])._Val[0];
			zdotc._Val[1] += Cf(&x[i*incx])._Val[1] * Cf(&y[i*incy])._Val[1];
		}
	}
	pCf(z) = zdotc;
}
#else
	_Complex float zdotc = 0.0;
	if (incx == 1 && incy == 1) {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc += Cf(&x[i]) * Cf(&y[i]);
		}
	} else {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc += Cf(&x[i*incx]) * Cf(&y[i*incy]);
		}
	}
	pCf(z) = zdotc;
}
#endif
static inline void zdotu_(doublecomplex *z, integer *n_, doublecomplex *x, integer *incx_, doublecomplex *y, integer *incy_) {
	integer n = *n_, incx = *incx_, incy = *incy_, i;
#ifdef _MSC_VER
	_Dcomplex zdotc = {0.0, 0.0};
	if (incx == 1 && incy == 1) {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc._Val[0] += Cd(&x[i])._Val[0] * Cd(&y[i])._Val[0];
			zdotc._Val[1] += Cd(&x[i])._Val[1] * Cd(&y[i])._Val[1];
		}
	} else {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc._Val[0] += Cd(&x[i*incx])._Val[0] * Cd(&y[i*incy])._Val[0];
			zdotc._Val[1] += Cd(&x[i*incx])._Val[1] * Cd(&y[i*incy])._Val[1];
		}
	}
	pCd(z) = zdotc;
}
#else
	_Complex double zdotc = 0.0;
	if (incx == 1 && incy == 1) {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc += Cd(&x[i]) * Cd(&y[i]);
		}
	} else {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc += Cd(&x[i*incx]) * Cd(&y[i*incy]);
		}
	}
	pCd(z) = zdotc;
}
#endif
/*  -- translated by f2c (version 20000121).
   You must link the resulting object file with the libraries:
	-lf2c -lm   (in that order)
*/




/* Table of constant values */

static integer c_n1 = -1;
static integer c__0 = 0;
static integer c__1 = 1;

/* > \brief \b DGERFSX */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download DGERFSX + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dgerfsx
.f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dgerfsx
.f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dgerfsx
.f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE DGERFSX( TRANS, EQUED, N, NRHS, A, LDA, AF, LDAF, IPIV, */
/*                           R, C, B, LDB, X, LDX, RCOND, BERR, N_ERR_BNDS, */
/*                           ERR_BNDS_NORM, ERR_BNDS_COMP, NPARAMS, PARAMS, */
/*                           WORK, IWORK, INFO ) */

/*       CHARACTER          TRANS, EQUED */
/*       INTEGER            INFO, LDA, LDAF, LDB, LDX, N, NRHS, NPARAMS, */
/*      $                   N_ERR_BNDS */
/*       DOUBLE PRECISION   RCOND */
/*       INTEGER            IPIV( * ), IWORK( * ) */
/*       DOUBLE PRECISION   A( LDA, * ), AF( LDAF, * ), B( LDB, * ), */
/*      $                   X( LDX , * ), WORK( * ) */
/*       DOUBLE PRECISION   R( * ), C( * ), PARAMS( * ), BERR( * ), */
/*      $                   ERR_BNDS_NORM( NRHS, * ), */
/*      $                   ERR_BNDS_COMP( NRHS, * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* >    DGERFSX improves the computed solution to a system of linear */
/* >    equations and provides error bounds and backward error estimates */
/* >    for the solution.  In addition to normwise error bound, the code */
/* >    provides maximum componentwise error bound if possible.  See */
/* >    comments for ERR_BNDS_NORM and ERR_BNDS_COMP for details of the */
/* >    error bounds. */
/* > */
/* >    The original system of linear equations may have been equilibrated */
/* >    before calling this routine, as described by arguments EQUED, R */
/* >    and C below. In this case, the solution and error bounds returned */
/* >    are for the original unequilibrated system. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \verbatim */
/* >     Some optional parameters are bundled in the PARAMS array.  These */
/* >     settings determine how refinement is performed, but often the */
/* >     defaults are acceptable.  If the defaults are acceptable, users */
/* >     can pass NPARAMS = 0 which prevents the source code from accessing */
/* >     the PARAMS argument. */
/* > \endverbatim */
/* > */
/* > \param[in] TRANS */
/* > \verbatim */
/* >          TRANS is CHARACTER*1 */
/* >     Specifies the form of the system of equations: */
/* >       = 'N':  A * X = B     (No transpose) */
/* >       = 'T':  A**T * X = B  (Transpose) */
/* >       = 'C':  A**H * X = B  (Conjugate transpose = Transpose) */
/* > \endverbatim */
/* > */
/* > \param[in] EQUED */
/* > \verbatim */
/* >          EQUED is CHARACTER*1 */
/* >     Specifies the form of equilibration that was done to A */
/* >     before calling this routine. This is needed to compute */
/* >     the solution and error bounds correctly. */
/* >       = 'N':  No equilibration */
/* >       = 'R':  Row equilibration, i.e., A has been premultiplied by */
/* >               diag(R). */
/* >       = 'C':  Column equilibration, i.e., A has been postmultiplied */
/* >               by diag(C). */
/* >       = 'B':  Both row and column equilibration, i.e., A has been */
/* >               replaced by diag(R) * A * diag(C). */
/* >               The right hand side B has been changed accordingly. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >     The order of the matrix A.  N >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] NRHS */
/* > \verbatim */
/* >          NRHS is INTEGER */
/* >     The number of right hand sides, i.e., the number of columns */
/* >     of the matrices B and X.  NRHS >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] A */
/* > \verbatim */
/* >          A is DOUBLE PRECISION array, dimension (LDA,N) */
/* >     The original N-by-N matrix A. */
/* > \endverbatim */
/* > */
/* > \param[in] LDA */
/* > \verbatim */
/* >          LDA is INTEGER */
/* >     The leading dimension of the array A.  LDA >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[in] AF */
/* > \verbatim */
/* >          AF is DOUBLE PRECISION array, dimension (LDAF,N) */
/* >     The factors L and U from the factorization A = P*L*U */
/* >     as computed by DGETRF. */
/* > \endverbatim */
/* > */
/* > \param[in] LDAF */
/* > \verbatim */
/* >          LDAF is INTEGER */
/* >     The leading dimension of the array AF.  LDAF >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[in] IPIV */
/* > \verbatim */
/* >          IPIV is INTEGER array, dimension (N) */
/* >     The pivot indices from DGETRF; for 1<=i<=N, row i of the */
/* >     matrix was interchanged with row IPIV(i). */
/* > \endverbatim */
/* > */
/* > \param[in] R */
/* > \verbatim */
/* >          R is DOUBLE PRECISION array, dimension (N) */
/* >     The row scale factors for A.  If EQUED = 'R' or 'B', A is */
/* >     multiplied on the left by diag(R); if EQUED = 'N' or 'C', R */
/* >     is not accessed. */
/* >     If R is accessed, each element of R should be a power of the radix */
/* >     to ensure a reliable solution and error estimates. Scaling by */
/* >     powers of the radix does not cause rounding errors unless the */
/* >     result underflows or overflows. Rounding errors during scaling */
/* >     lead to refining with a matrix that is not equivalent to the */
/* >     input matrix, producing error estimates that may not be */
/* >     reliable. */
/* > \endverbatim */
/* > */
/* > \param[in] C */
/* > \verbatim */
/* >          C is DOUBLE PRECISION array, dimension (N) */
/* >     The column scale factors for A.  If EQUED = 'C' or 'B', A is */
/* >     multiplied on the right by diag(C); if EQUED = 'N' or 'R', C */
/* >     is not accessed. */
/* >     If C is accessed, each element of C should be a power of the radix */
/* >     to ensure a reliable solution and error estimates. Scaling by */
/* >     powers of the radix does not cause rounding errors unless the */
/* >     result underflows or overflows. Rounding errors during scaling */
/* >     lead to refining with a matrix that is not equivalent to the */
/* >     input matrix, producing error estimates that may not be */
/* >     reliable. */
/* > \endverbatim */
/* > */
/* > \param[in] B */
/* > \verbatim */
/* >          B is DOUBLE PRECISION array, dimension (LDB,NRHS) */
/* >     The right hand side matrix B. */
/* > \endverbatim */
/* > */
/* > \param[in] LDB */
/* > \verbatim */
/* >          LDB is INTEGER */
/* >     The leading dimension of the array B.  LDB >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[in,out] X */
/* > \verbatim */
/* >          X is DOUBLE PRECISION array, dimension (LDX,NRHS) */
/* >     On entry, the solution matrix X, as computed by DGETRS. */
/* >     On exit, the improved solution matrix X. */
/* > \endverbatim */
/* > */
/* > \param[in] LDX */
/* > \verbatim */
/* >          LDX is INTEGER */
/* >     The leading dimension of the array X.  LDX >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[out] RCOND */
/* > \verbatim */
/* >          RCOND is DOUBLE PRECISION */
/* >     Reciprocal scaled condition number.  This is an estimate of the */
/* >     reciprocal Skeel condition number of the matrix A after */
/* >     equilibration (if done).  If this is less than the machine */
/* >     precision (in particular, if it is zero), the matrix is singular */
/* >     to working precision.  Note that the error may still be small even */
/* >     if this number is very small and the matrix appears ill- */
/* >     conditioned. */
/* > \endverbatim */
/* > */
/* > \param[out] BERR */
/* > \verbatim */
/* >          BERR is DOUBLE PRECISION array, dimension (NRHS) */
/* >     Componentwise relative backward error.  This is the */
/* >     componentwise relative backward error of each solution vector X(j) */
/* >     (i.e., the smallest relative change in any element of A or B that */
/* >     makes X(j) an exact solution). */
/* > \endverbatim */
/* > */
/* > \param[in] N_ERR_BNDS */
/* > \verbatim */
/* >          N_ERR_BNDS is INTEGER */
/* >     Number of error bounds to return for each right hand side */
/* >     and each type (normwise or componentwise).  See ERR_BNDS_NORM and */
/* >     ERR_BNDS_COMP below. */
/* > \endverbatim */
/* > */
/* > \param[out] ERR_BNDS_NORM */
/* > \verbatim */
/* >          ERR_BNDS_NORM is DOUBLE PRECISION array, dimension (NRHS, N_ERR_BNDS) */
/* >     For each right-hand side, this array contains information about */
/* >     various error bounds and condition numbers corresponding to the */
/* >     normwise relative error, which is defined as follows: */
/* > */
/* >     Normwise relative error in the ith solution vector: */
/* >             max_j (abs(XTRUE(j,i) - X(j,i))) */
/* >            ------------------------------ */
/* >                  max_j abs(X(j,i)) */
/* > */
/* >     The array is indexed by the type of error information as described */
/* >     below. There currently are up to three pieces of information */
/* >     returned. */
/* > */
/* >     The first index in ERR_BNDS_NORM(i,:) corresponds to the ith */
/* >     right-hand side. */
/* > */
/* >     The second index in ERR_BNDS_NORM(:,err) contains the following */
/* >     three fields: */
/* >     err = 1 "Trust/don't trust" boolean. Trust the answer if the */
/* >              reciprocal condition number is less than the threshold */
/* >              sqrt(n) * dlamch('Epsilon'). */
/* > */
/* >     err = 2 "Guaranteed" error bound: The estimated forward error, */
/* >              almost certainly within a factor of 10 of the true error */
/* >              so long as the next entry is greater than the threshold */
/* >              sqrt(n) * dlamch('Epsilon'). This error bound should only */
/* >              be trusted if the previous boolean is true. */
/* > */
/* >     err = 3  Reciprocal condition number: Estimated normwise */
/* >              reciprocal condition number.  Compared with the threshold */
/* >              sqrt(n) * dlamch('Epsilon') to determine if the error */
/* >              estimate is "guaranteed". These reciprocal condition */
/* >              numbers are 1 / (norm(Z^{-1},inf) * norm(Z,inf)) for some */
/* >              appropriately scaled matrix Z. */
/* >              Let Z = S*A, where S scales each row by a power of the */
/* >              radix so all absolute row sums of Z are approximately 1. */
/* > */
/* >     See Lapack Working Note 165 for further details and extra */
/* >     cautions. */
/* > \endverbatim */
/* > */
/* > \param[out] ERR_BNDS_COMP */
/* > \verbatim */
/* >          ERR_BNDS_COMP is DOUBLE PRECISION array, dimension (NRHS, N_ERR_BNDS) */
/* >     For each right-hand side, this array contains information about */
/* >     various error bounds and condition numbers corresponding to the */
/* >     componentwise relative error, which is defined as follows: */
/* > */
/* >     Componentwise relative error in the ith solution vector: */
/* >                    abs(XTRUE(j,i) - X(j,i)) */
/* >             max_j ---------------------- */
/* >                         abs(X(j,i)) */
/* > */
/* >     The array is indexed by the right-hand side i (on which the */
/* >     componentwise relative error depends), and the type of error */
/* >     information as described below. There currently are up to three */
/* >     pieces of information returned for each right-hand side. If */
/* >     componentwise accuracy is not requested (PARAMS(3) = 0.0), then */
/* >     ERR_BNDS_COMP is not accessed.  If N_ERR_BNDS < 3, then at most */
/* >     the first (:,N_ERR_BNDS) entries are returned. */
/* > */
/* >     The first index in ERR_BNDS_COMP(i,:) corresponds to the ith */
/* >     right-hand side. */
/* > */
/* >     The second index in ERR_BNDS_COMP(:,err) contains the following */
/* >     three fields: */
/* >     err = 1 "Trust/don't trust" boolean. Trust the answer if the */
/* >              reciprocal condition number is less than the threshold */
/* >              sqrt(n) * dlamch('Epsilon'). */
/* > */
/* >     err = 2 "Guaranteed" error bound: The estimated forward error, */
/* >              almost certainly within a factor of 10 of the true error */
/* >              so long as the next entry is greater than the threshold */
/* >              sqrt(n) * dlamch('Epsilon'). This error bound should only */
/* >              be trusted if the previous boolean is true. */
/* > */
/* >     err = 3  Reciprocal condition number: Estimated componentwise */
/* >              reciprocal condition number.  Compared with the threshold */
/* >              sqrt(n) * dlamch('Epsilon') to determine if the error */
/* >              estimate is "guaranteed". These reciprocal condition */
/* >              numbers are 1 / (norm(Z^{-1},inf) * norm(Z,inf)) for some */
/* >              appropriately scaled matrix Z. */
/* >              Let Z = S*(A*diag(x)), where x is the solution for the */
/* >              current right-hand side and S scales each row of */
/* >              A*diag(x) by a power of the radix so all absolute row */
/* >              sums of Z are approximately 1. */
/* > */
/* >     See Lapack Working Note 165 for further details and extra */
/* >     cautions. */
/* > \endverbatim */
/* > */
/* > \param[in] NPARAMS */
/* > \verbatim */
/* >          NPARAMS is INTEGER */
/* >     Specifies the number of parameters set in PARAMS.  If <= 0, the */
/* >     PARAMS array is never referenced and default values are used. */
/* > \endverbatim */
/* > */
/* > \param[in,out] PARAMS */
/* > \verbatim */
/* >          PARAMS is DOUBLE PRECISION array, dimension (NPARAMS) */
/* >     Specifies algorithm parameters.  If an entry is < 0.0, then */
/* >     that entry will be filled with default value used for that */
/* >     parameter.  Only positions up to NPARAMS are accessed; defaults */
/* >     are used for higher-numbered parameters. */
/* > */
/* >       PARAMS(LA_LINRX_ITREF_I = 1) : Whether to perform iterative */
/* >            refinement or not. */
/* >         Default: 1.0D+0 */
/* >            = 0.0:  No refinement is performed, and no error bounds are */
/* >                    computed. */
/* >            = 1.0:  Use the double-precision refinement algorithm, */
/* >                    possibly with doubled-single computations if the */
/* >                    compilation environment does not support DOUBLE */
/* >                    PRECISION. */
/* >              (other values are reserved for future use) */
/* > */
/* >       PARAMS(LA_LINRX_ITHRESH_I = 2) : Maximum number of residual */
/* >            computations allowed for refinement. */
/* >         Default: 10 */
/* >         Aggressive: Set to 100 to permit convergence using approximate */
/* >                     factorizations or factorizations other than LU. If */
/* >                     the factorization uses a technique other than */
/* >                     Gaussian elimination, the guarantees in */
/* >                     err_bnds_norm and err_bnds_comp may no longer be */
/* >                     trustworthy. */
/* > */
/* >       PARAMS(LA_LINRX_CWISE_I = 3) : Flag determining if the code */
/* >            will attempt to find a solution with small componentwise */
/* >            relative error in the double-precision algorithm.  Positive */
/* >            is true, 0.0 is false. */
/* >         Default: 1.0 (attempt componentwise convergence) */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is DOUBLE PRECISION array, dimension (4*N) */
/* > \endverbatim */
/* > */
/* > \param[out] IWORK */
/* > \verbatim */
/* >          IWORK is INTEGER array, dimension (N) */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >       = 0:  Successful exit. The solution to every right-hand side is */
/* >         guaranteed. */
/* >       < 0:  If INFO = -i, the i-th argument had an illegal value */
/* >       > 0 and <= N:  U(INFO,INFO) is exactly zero.  The factorization */
/* >         has been completed, but the factor U is exactly singular, so */
/* >         the solution and error bounds could not be computed. RCOND = 0 */
/* >         is returned. */
/* >       = N+J: The solution corresponding to the Jth right-hand side is */
/* >         not guaranteed. The solutions corresponding to other right- */
/* >         hand sides K with K > J may not be guaranteed as well, but */
/* >         only the first such right-hand side is reported. If a small */
/* >         componentwise error is not requested (PARAMS(3) = 0.0) then */
/* >         the Jth right-hand side is the first with a normwise error */
/* >         bound that is not guaranteed (the smallest J such */
/* >         that ERR_BNDS_NORM(J,1) = 0.0). By default (PARAMS(3) = 1.0) */
/* >         the Jth right-hand side is the first with either a normwise or */
/* >         componentwise error bound that is not guaranteed (the smallest */
/* >         J such that either ERR_BNDS_NORM(J,1) = 0.0 or */
/* >         ERR_BNDS_COMP(J,1) = 0.0). See the definition of */
/* >         ERR_BNDS_NORM(:,1) and ERR_BNDS_COMP(:,1). To get information */
/* >         about all of the right-hand sides check ERR_BNDS_NORM or */
/* >         ERR_BNDS_COMP. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date December 2016 */

/* > \ingroup doubleGEcomputational */

/*  ===================================================================== */
/* Subroutine */ void dgerfsx_(char *trans, char *equed, integer *n, integer *
	nrhs, doublereal *a, integer *lda, doublereal *af, integer *ldaf, 
	integer *ipiv, doublereal *r__, doublereal *c__, doublereal *b, 
	integer *ldb, doublereal *x, integer *ldx, doublereal *rcond, 
	doublereal *berr, integer *n_err_bnds__, doublereal *err_bnds_norm__, 
	doublereal *err_bnds_comp__, integer *nparams, doublereal *params, 
	doublereal *work, integer *iwork, integer *info)
{
    /* System generated locals */
    integer a_dim1, a_offset, af_dim1, af_offset, b_dim1, b_offset, x_dim1, 
	    x_offset, err_bnds_norm_dim1, err_bnds_norm_offset, 
	    err_bnds_comp_dim1, err_bnds_comp_offset, i__1;
    doublereal d__1, d__2;

    /* Local variables */
    doublereal illrcond_thresh__, unstable_thresh__, err_lbnd__;
    char norm[1];
    integer ref_type__;
    extern integer ilatrans_(char *);
    logical ignore_cwise__;
    integer j;
    extern logical lsame_(char *, char *);
    doublereal anorm, rcond_tmp__;
    integer prec_type__;
    extern doublereal dlamch_(char *), dlange_(char *, integer *, 
	    integer *, doublereal *, integer *, doublereal *);
    extern /* Subroutine */ void dgecon_(char *, integer *, doublereal *, 
	    integer *, doublereal *, doublereal *, doublereal *, integer *, 
	    integer *);
    extern int xerbla_(char *, integer *, ftnlen);
    logical colequ, notran, rowequ;
    integer trans_type__;
    extern integer ilaprec_(char *);
    extern doublereal dla_gercond_(char *, integer *, doublereal *, integer *
	    , doublereal *, integer *, integer *, integer *, doublereal *, 
	    integer *, doublereal *, integer *);
    integer ithresh, n_norms__;
    doublereal rthresh, cwise_wrong__;
    extern /* Subroutine */ void dla_gerfsx_extended_(integer *, integer *, 
	    integer *, integer *, doublereal *, integer *, doublereal *, 
	    integer *, integer *, logical *, doublereal *, doublereal *, 
	    integer *, doublereal *, integer *, doublereal *, integer *, 
	    doublereal *, doublereal *, doublereal *, doublereal *, 
	    doublereal *, doublereal *, doublereal *, integer *, doublereal *,
	     doublereal *, logical *, integer *);


/*  -- LAPACK computational routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     December 2016 */


/*  ================================================================== */


/*     Check the input parameters. */

    /* Parameter adjustments */
    err_bnds_comp_dim1 = *nrhs;
    err_bnds_comp_offset = 1 + err_bnds_comp_dim1 * 1;
    err_bnds_comp__ -= err_bnds_comp_offset;
    err_bnds_norm_dim1 = *nrhs;
    err_bnds_norm_offset = 1 + err_bnds_norm_dim1 * 1;
    err_bnds_norm__ -= err_bnds_norm_offset;
    a_dim1 = *lda;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;
    af_dim1 = *ldaf;
    af_offset = 1 + af_dim1 * 1;
    af -= af_offset;
    --ipiv;
    --r__;
    --c__;
    b_dim1 = *ldb;
    b_offset = 1 + b_dim1 * 1;
    b -= b_offset;
    x_dim1 = *ldx;
    x_offset = 1 + x_dim1 * 1;
    x -= x_offset;
    --berr;
    --params;
    --work;
    --iwork;

    /* Function Body */
    *info = 0;
    trans_type__ = ilatrans_(trans);
    ref_type__ = 1;
    if (*nparams >= 1) {
	if (params[1] < 0.) {
	    params[1] = 1.;
	} else {
	    ref_type__ = (integer) params[1];
	}
    }

/*     Set default parameters. */

    illrcond_thresh__ = (doublereal) (*n) * dlamch_("Epsilon");
    ithresh = 10;
    rthresh = .5;
    unstable_thresh__ = .25;
    ignore_cwise__ = FALSE_;

    if (*nparams >= 2) {
	if (params[2] < 0.) {
	    params[2] = (doublereal) ithresh;
	} else {
	    ithresh = (integer) params[2];
	}
    }
    if (*nparams >= 3) {
	if (params[3] < 0.) {
	    if (ignore_cwise__) {
		params[3] = 0.;
	    } else {
		params[3] = 1.;
	    }
	} else {
	    ignore_cwise__ = params[3] == 0.;
	}
    }
    if (ref_type__ == 0 || *n_err_bnds__ == 0) {
	n_norms__ = 0;
    } else if (ignore_cwise__) {
	n_norms__ = 1;
    } else {
	n_norms__ = 2;
    }

    notran = lsame_(trans, "N");
    rowequ = lsame_(equed, "R") || lsame_(equed, "B");
    colequ = lsame_(equed, "C") || lsame_(equed, "B");

/*     Test input parameters. */

    if (trans_type__ == -1) {
	*info = -1;
    } else if (! rowequ && ! colequ && ! lsame_(equed, "N")) {
	*info = -2;
    } else if (*n < 0) {
	*info = -3;
    } else if (*nrhs < 0) {
	*info = -4;
    } else if (*lda < f2cmax(1,*n)) {
	*info = -6;
    } else if (*ldaf < f2cmax(1,*n)) {
	*info = -8;
    } else if (*ldb < f2cmax(1,*n)) {
	*info = -13;
    } else if (*ldx < f2cmax(1,*n)) {
	*info = -15;
    }
    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("DGERFSX", &i__1, (ftnlen)7);
	return;
    }

/*     Quick return if possible. */

    if (*n == 0 || *nrhs == 0) {
	*rcond = 1.;
	i__1 = *nrhs;
	for (j = 1; j <= i__1; ++j) {
	    berr[j] = 0.;
	    if (*n_err_bnds__ >= 1) {
		err_bnds_norm__[j + err_bnds_norm_dim1] = 1.;
		err_bnds_comp__[j + err_bnds_comp_dim1] = 1.;
	    }
	    if (*n_err_bnds__ >= 2) {
		err_bnds_norm__[j + (err_bnds_norm_dim1 << 1)] = 0.;
		err_bnds_comp__[j + (err_bnds_comp_dim1 << 1)] = 0.;
	    }
	    if (*n_err_bnds__ >= 3) {
		err_bnds_norm__[j + err_bnds_norm_dim1 * 3] = 1.;
		err_bnds_comp__[j + err_bnds_comp_dim1 * 3] = 1.;
	    }
	}
	return;
    }

/*     Default to failure. */

    *rcond = 0.;
    i__1 = *nrhs;
    for (j = 1; j <= i__1; ++j) {
	berr[j] = 1.;
	if (*n_err_bnds__ >= 1) {
	    err_bnds_norm__[j + err_bnds_norm_dim1] = 1.;
	    err_bnds_comp__[j + err_bnds_comp_dim1] = 1.;
	}
	if (*n_err_bnds__ >= 2) {
	    err_bnds_norm__[j + (err_bnds_norm_dim1 << 1)] = 1.;
	    err_bnds_comp__[j + (err_bnds_comp_dim1 << 1)] = 1.;
	}
	if (*n_err_bnds__ >= 3) {
	    err_bnds_norm__[j + err_bnds_norm_dim1 * 3] = 0.;
	    err_bnds_comp__[j + err_bnds_comp_dim1 * 3] = 0.;
	}
    }

/*     Compute the norm of A and the reciprocal of the condition */
/*     number of A. */

    if (notran) {
	*(unsigned char *)norm = 'I';
    } else {
	*(unsigned char *)norm = '1';
    }
    anorm = dlange_(norm, n, n, &a[a_offset], lda, &work[1]);
    dgecon_(norm, n, &af[af_offset], ldaf, &anorm, rcond, &work[1], &iwork[1],
	     info);

/*     Perform refinement on each right-hand side */

    if (ref_type__ != 0) {
	prec_type__ = ilaprec_("E");
	if (notran) {
	    dla_gerfsx_extended_(&prec_type__, &trans_type__, n, nrhs, &a[
		    a_offset], lda, &af[af_offset], ldaf, &ipiv[1], &colequ, &
		    c__[1], &b[b_offset], ldb, &x[x_offset], ldx, &berr[1], &
		    n_norms__, &err_bnds_norm__[err_bnds_norm_offset], &
		    err_bnds_comp__[err_bnds_comp_offset], &work[*n + 1], &
		    work[1], &work[(*n << 1) + 1], &work[1], rcond, &ithresh, 
		    &rthresh, &unstable_thresh__, &ignore_cwise__, info);
	} else {
	    dla_gerfsx_extended_(&prec_type__, &trans_type__, n, nrhs, &a[
		    a_offset], lda, &af[af_offset], ldaf, &ipiv[1], &rowequ, &
		    r__[1], &b[b_offset], ldb, &x[x_offset], ldx, &berr[1], &
		    n_norms__, &err_bnds_norm__[err_bnds_norm_offset], &
		    err_bnds_comp__[err_bnds_comp_offset], &work[*n + 1], &
		    work[1], &work[(*n << 1) + 1], &work[1], rcond, &ithresh, 
		    &rthresh, &unstable_thresh__, &ignore_cwise__, info);
	}
    }
/* Computing MAX */
    d__1 = 10., d__2 = sqrt((doublereal) (*n));
    err_lbnd__ = f2cmax(d__1,d__2) * dlamch_("Epsilon");
    if (*n_err_bnds__ >= 1 && n_norms__ >= 1) {

/*     Compute scaled normwise condition number cond(A*C). */

	if (colequ && notran) {
	    rcond_tmp__ = dla_gercond_(trans, n, &a[a_offset], lda, &af[
		    af_offset], ldaf, &ipiv[1], &c_n1, &c__[1], info, &work[1]
		    , &iwork[1]);
	} else if (rowequ && ! notran) {
	    rcond_tmp__ = dla_gercond_(trans, n, &a[a_offset], lda, &af[
		    af_offset], ldaf, &ipiv[1], &c_n1, &r__[1], info, &work[1]
		    , &iwork[1]);
	} else {
	    rcond_tmp__ = dla_gercond_(trans, n, &a[a_offset], lda, &af[
		    af_offset], ldaf, &ipiv[1], &c__0, &r__[1], info, &work[1]
		    , &iwork[1]);
	}
	i__1 = *nrhs;
	for (j = 1; j <= i__1; ++j) {

/*     Cap the error at 1.0. */

	    if (*n_err_bnds__ >= 2 && err_bnds_norm__[j + (err_bnds_norm_dim1 
		    << 1)] > 1.) {
		err_bnds_norm__[j + (err_bnds_norm_dim1 << 1)] = 1.;
	    }

/*     Threshold the error (see LAWN). */

	    if (rcond_tmp__ < illrcond_thresh__) {
		err_bnds_norm__[j + (err_bnds_norm_dim1 << 1)] = 1.;
		err_bnds_norm__[j + err_bnds_norm_dim1] = 0.;
		if (*info <= *n) {
		    *info = *n + j;
		}
	    } else if (err_bnds_norm__[j + (err_bnds_norm_dim1 << 1)] < 
		    err_lbnd__) {
		err_bnds_norm__[j + (err_bnds_norm_dim1 << 1)] = err_lbnd__;
		err_bnds_norm__[j + err_bnds_norm_dim1] = 1.;
	    }

/*     Save the condition number. */

	    if (*n_err_bnds__ >= 3) {
		err_bnds_norm__[j + err_bnds_norm_dim1 * 3] = rcond_tmp__;
	    }
	}
    }
    if (*n_err_bnds__ >= 1 && n_norms__ >= 2) {

/*     Compute componentwise condition number cond(A*diag(Y(:,J))) for */
/*     each right-hand side using the current solution as an estimate of */
/*     the true solution.  If the componentwise error estimate is too */
/*     large, then the solution is a lousy estimate of truth and the */
/*     estimated RCOND may be too optimistic.  To avoid misleading users, */
/*     the inverse condition number is set to 0.0 when the estimated */
/*     cwise error is at least CWISE_WRONG. */

	cwise_wrong__ = sqrt(dlamch_("Epsilon"));
	i__1 = *nrhs;
	for (j = 1; j <= i__1; ++j) {
	    if (err_bnds_comp__[j + (err_bnds_comp_dim1 << 1)] < 
		    cwise_wrong__) {
		rcond_tmp__ = dla_gercond_(trans, n, &a[a_offset], lda, &af[
			af_offset], ldaf, &ipiv[1], &c__1, &x[j * x_dim1 + 1],
			 info, &work[1], &iwork[1]);
	    } else {
		rcond_tmp__ = 0.;
	    }

/*     Cap the error at 1.0. */

	    if (*n_err_bnds__ >= 2 && err_bnds_comp__[j + (err_bnds_comp_dim1 
		    << 1)] > 1.) {
		err_bnds_comp__[j + (err_bnds_comp_dim1 << 1)] = 1.;
	    }

/*     Threshold the error (see LAWN). */

	    if (rcond_tmp__ < illrcond_thresh__) {
		err_bnds_comp__[j + (err_bnds_comp_dim1 << 1)] = 1.;
		err_bnds_comp__[j + err_bnds_comp_dim1] = 0.;
		if (params[3] == 1. && *info < *n + j) {
		    *info = *n + j;
		}
	    } else if (err_bnds_comp__[j + (err_bnds_comp_dim1 << 1)] < 
		    err_lbnd__) {
		err_bnds_comp__[j + (err_bnds_comp_dim1 << 1)] = err_lbnd__;
		err_bnds_comp__[j + err_bnds_comp_dim1] = 1.;
	    }

/*     Save the condition number. */

	    if (*n_err_bnds__ >= 3) {
		err_bnds_comp__[j + err_bnds_comp_dim1 * 3] = rcond_tmp__;
	    }
	}
    }

    return;

/*     End of DGERFSX */

} /* dgerfsx_ */

