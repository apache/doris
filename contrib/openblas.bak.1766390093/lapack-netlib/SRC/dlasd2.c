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

static integer c__1 = 1;
static doublereal c_b30 = 0.;

/* > \brief \b DLASD2 merges the two sets of singular values together into a single sorted set. Used by sbdsdc
. */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download DLASD2 + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dlasd2.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dlasd2.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dlasd2.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE DLASD2( NL, NR, SQRE, K, D, Z, ALPHA, BETA, U, LDU, VT, */
/*                          LDVT, DSIGMA, U2, LDU2, VT2, LDVT2, IDXP, IDX, */
/*                          IDXC, IDXQ, COLTYP, INFO ) */

/*       INTEGER            INFO, K, LDU, LDU2, LDVT, LDVT2, NL, NR, SQRE */
/*       DOUBLE PRECISION   ALPHA, BETA */
/*       INTEGER            COLTYP( * ), IDX( * ), IDXC( * ), IDXP( * ), */
/*      $                   IDXQ( * ) */
/*       DOUBLE PRECISION   D( * ), DSIGMA( * ), U( LDU, * ), */
/*      $                   U2( LDU2, * ), VT( LDVT, * ), VT2( LDVT2, * ), */
/*      $                   Z( * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > DLASD2 merges the two sets of singular values together into a single */
/* > sorted set.  Then it tries to deflate the size of the problem. */
/* > There are two ways in which deflation can occur:  when two or more */
/* > singular values are close together or if there is a tiny entry in the */
/* > Z vector.  For each such occurrence the order of the related secular */
/* > equation problem is reduced by one. */
/* > */
/* > DLASD2 is called from DLASD1. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] NL */
/* > \verbatim */
/* >          NL is INTEGER */
/* >         The row dimension of the upper block.  NL >= 1. */
/* > \endverbatim */
/* > */
/* > \param[in] NR */
/* > \verbatim */
/* >          NR is INTEGER */
/* >         The row dimension of the lower block.  NR >= 1. */
/* > \endverbatim */
/* > */
/* > \param[in] SQRE */
/* > \verbatim */
/* >          SQRE is INTEGER */
/* >         = 0: the lower block is an NR-by-NR square matrix. */
/* >         = 1: the lower block is an NR-by-(NR+1) rectangular matrix. */
/* > */
/* >         The bidiagonal matrix has N = NL + NR + 1 rows and */
/* >         M = N + SQRE >= N columns. */
/* > \endverbatim */
/* > */
/* > \param[out] K */
/* > \verbatim */
/* >          K is INTEGER */
/* >         Contains the dimension of the non-deflated matrix, */
/* >         This is the order of the related secular equation. 1 <= K <=N. */
/* > \endverbatim */
/* > */
/* > \param[in,out] D */
/* > \verbatim */
/* >          D is DOUBLE PRECISION array, dimension(N) */
/* >         On entry D contains the singular values of the two submatrices */
/* >         to be combined.  On exit D contains the trailing (N-K) updated */
/* >         singular values (those which were deflated) sorted into */
/* >         increasing order. */
/* > \endverbatim */
/* > */
/* > \param[out] Z */
/* > \verbatim */
/* >          Z is DOUBLE PRECISION array, dimension(N) */
/* >         On exit Z contains the updating row vector in the secular */
/* >         equation. */
/* > \endverbatim */
/* > */
/* > \param[in] ALPHA */
/* > \verbatim */
/* >          ALPHA is DOUBLE PRECISION */
/* >         Contains the diagonal element associated with the added row. */
/* > \endverbatim */
/* > */
/* > \param[in] BETA */
/* > \verbatim */
/* >          BETA is DOUBLE PRECISION */
/* >         Contains the off-diagonal element associated with the added */
/* >         row. */
/* > \endverbatim */
/* > */
/* > \param[in,out] U */
/* > \verbatim */
/* >          U is DOUBLE PRECISION array, dimension(LDU,N) */
/* >         On entry U contains the left singular vectors of two */
/* >         submatrices in the two square blocks with corners at (1,1), */
/* >         (NL, NL), and (NL+2, NL+2), (N,N). */
/* >         On exit U contains the trailing (N-K) updated left singular */
/* >         vectors (those which were deflated) in its last N-K columns. */
/* > \endverbatim */
/* > */
/* > \param[in] LDU */
/* > \verbatim */
/* >          LDU is INTEGER */
/* >         The leading dimension of the array U.  LDU >= N. */
/* > \endverbatim */
/* > */
/* > \param[in,out] VT */
/* > \verbatim */
/* >          VT is DOUBLE PRECISION array, dimension(LDVT,M) */
/* >         On entry VT**T contains the right singular vectors of two */
/* >         submatrices in the two square blocks with corners at (1,1), */
/* >         (NL+1, NL+1), and (NL+2, NL+2), (M,M). */
/* >         On exit VT**T contains the trailing (N-K) updated right singular */
/* >         vectors (those which were deflated) in its last N-K columns. */
/* >         In case SQRE =1, the last row of VT spans the right null */
/* >         space. */
/* > \endverbatim */
/* > */
/* > \param[in] LDVT */
/* > \verbatim */
/* >          LDVT is INTEGER */
/* >         The leading dimension of the array VT.  LDVT >= M. */
/* > \endverbatim */
/* > */
/* > \param[out] DSIGMA */
/* > \verbatim */
/* >          DSIGMA is DOUBLE PRECISION array, dimension (N) */
/* >         Contains a copy of the diagonal elements (K-1 singular values */
/* >         and one zero) in the secular equation. */
/* > \endverbatim */
/* > */
/* > \param[out] U2 */
/* > \verbatim */
/* >          U2 is DOUBLE PRECISION array, dimension(LDU2,N) */
/* >         Contains a copy of the first K-1 left singular vectors which */
/* >         will be used by DLASD3 in a matrix multiply (DGEMM) to solve */
/* >         for the new left singular vectors. U2 is arranged into four */
/* >         blocks. The first block contains a column with 1 at NL+1 and */
/* >         zero everywhere else; the second block contains non-zero */
/* >         entries only at and above NL; the third contains non-zero */
/* >         entries only below NL+1; and the fourth is dense. */
/* > \endverbatim */
/* > */
/* > \param[in] LDU2 */
/* > \verbatim */
/* >          LDU2 is INTEGER */
/* >         The leading dimension of the array U2.  LDU2 >= N. */
/* > \endverbatim */
/* > */
/* > \param[out] VT2 */
/* > \verbatim */
/* >          VT2 is DOUBLE PRECISION array, dimension(LDVT2,N) */
/* >         VT2**T contains a copy of the first K right singular vectors */
/* >         which will be used by DLASD3 in a matrix multiply (DGEMM) to */
/* >         solve for the new right singular vectors. VT2 is arranged into */
/* >         three blocks. The first block contains a row that corresponds */
/* >         to the special 0 diagonal element in SIGMA; the second block */
/* >         contains non-zeros only at and before NL +1; the third block */
/* >         contains non-zeros only at and after  NL +2. */
/* > \endverbatim */
/* > */
/* > \param[in] LDVT2 */
/* > \verbatim */
/* >          LDVT2 is INTEGER */
/* >         The leading dimension of the array VT2.  LDVT2 >= M. */
/* > \endverbatim */
/* > */
/* > \param[out] IDXP */
/* > \verbatim */
/* >          IDXP is INTEGER array, dimension(N) */
/* >         This will contain the permutation used to place deflated */
/* >         values of D at the end of the array. On output IDXP(2:K) */
/* >         points to the nondeflated D-values and IDXP(K+1:N) */
/* >         points to the deflated singular values. */
/* > \endverbatim */
/* > */
/* > \param[out] IDX */
/* > \verbatim */
/* >          IDX is INTEGER array, dimension(N) */
/* >         This will contain the permutation used to sort the contents of */
/* >         D into ascending order. */
/* > \endverbatim */
/* > */
/* > \param[out] IDXC */
/* > \verbatim */
/* >          IDXC is INTEGER array, dimension(N) */
/* >         This will contain the permutation used to arrange the columns */
/* >         of the deflated U matrix into three groups:  the first group */
/* >         contains non-zero entries only at and above NL, the second */
/* >         contains non-zero entries only below NL+2, and the third is */
/* >         dense. */
/* > \endverbatim */
/* > */
/* > \param[in,out] IDXQ */
/* > \verbatim */
/* >          IDXQ is INTEGER array, dimension(N) */
/* >         This contains the permutation which separately sorts the two */
/* >         sub-problems in D into ascending order.  Note that entries in */
/* >         the first hlaf of this permutation must first be moved one */
/* >         position backward; and entries in the second half */
/* >         must first have NL+1 added to their values. */
/* > \endverbatim */
/* > */
/* > \param[out] COLTYP */
/* > \verbatim */
/* >          COLTYP is INTEGER array, dimension(N) */
/* >         As workspace, this will contain a label which will indicate */
/* >         which of the following types a column in the U2 matrix or a */
/* >         row in the VT2 matrix is: */
/* >         1 : non-zero in the upper half only */
/* >         2 : non-zero in the lower half only */
/* >         3 : dense */
/* >         4 : deflated */
/* > */
/* >         On exit, it is an array of dimension 4, with COLTYP(I) being */
/* >         the dimension of the I-th type columns. */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >          = 0:  successful exit. */
/* >          < 0:  if INFO = -i, the i-th argument had an illegal value. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date June 2017 */

/* > \ingroup OTHERauxiliary */

/* > \par Contributors: */
/*  ================== */
/* > */
/* >     Ming Gu and Huan Ren, Computer Science Division, University of */
/* >     California at Berkeley, USA */
/* > */
/*  ===================================================================== */
/* Subroutine */ void dlasd2_(integer *nl, integer *nr, integer *sqre, integer 
	*k, doublereal *d__, doublereal *z__, doublereal *alpha, doublereal *
	beta, doublereal *u, integer *ldu, doublereal *vt, integer *ldvt, 
	doublereal *dsigma, doublereal *u2, integer *ldu2, doublereal *vt2, 
	integer *ldvt2, integer *idxp, integer *idx, integer *idxc, integer *
	idxq, integer *coltyp, integer *info)
{
    /* System generated locals */
    integer u_dim1, u_offset, u2_dim1, u2_offset, vt_dim1, vt_offset, 
	    vt2_dim1, vt2_offset, i__1;
    doublereal d__1, d__2;

    /* Local variables */
    integer idxi, idxj;
    extern /* Subroutine */ void drot_(integer *, doublereal *, integer *, 
	    doublereal *, integer *, doublereal *, doublereal *);
    integer ctot[4];
    doublereal c__;
    integer i__, j, m, n;
    doublereal s;
    integer idxjp;
    extern /* Subroutine */ void dcopy_(integer *, doublereal *, integer *, 
	    doublereal *, integer *);
    integer jprev, k2;
    doublereal z1;
    extern doublereal dlapy2_(doublereal *, doublereal *);
    integer ct;
    extern doublereal dlamch_(char *);
    integer jp;
    extern /* Subroutine */ void dlamrg_(integer *, integer *, doublereal *, 
	    integer *, integer *, integer *), dlacpy_(char *, integer *, 
	    integer *, doublereal *, integer *, doublereal *, integer *), dlaset_(char *, integer *, integer *, doublereal *, 
	    doublereal *, doublereal *, integer *);
    extern int xerbla_(char *, integer *, ftnlen);
    doublereal hlftol, eps, tau, tol;
    integer psm[4], nlp1, nlp2;


/*  -- LAPACK auxiliary routine (version 3.7.1) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     June 2017 */


/*  ===================================================================== */


/*     Test the input parameters. */

    /* Parameter adjustments */
    --d__;
    --z__;
    u_dim1 = *ldu;
    u_offset = 1 + u_dim1 * 1;
    u -= u_offset;
    vt_dim1 = *ldvt;
    vt_offset = 1 + vt_dim1 * 1;
    vt -= vt_offset;
    --dsigma;
    u2_dim1 = *ldu2;
    u2_offset = 1 + u2_dim1 * 1;
    u2 -= u2_offset;
    vt2_dim1 = *ldvt2;
    vt2_offset = 1 + vt2_dim1 * 1;
    vt2 -= vt2_offset;
    --idxp;
    --idx;
    --idxc;
    --idxq;
    --coltyp;

    /* Function Body */
    *info = 0;

    if (*nl < 1) {
	*info = -1;
    } else if (*nr < 1) {
	*info = -2;
    } else if (*sqre != 1 && *sqre != 0) {
	*info = -3;
    }

    n = *nl + *nr + 1;
    m = n + *sqre;

    if (*ldu < n) {
	*info = -10;
    } else if (*ldvt < m) {
	*info = -12;
    } else if (*ldu2 < n) {
	*info = -15;
    } else if (*ldvt2 < m) {
	*info = -17;
    }
    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("DLASD2", &i__1, (ftnlen)6);
	return;
    }

    nlp1 = *nl + 1;
    nlp2 = *nl + 2;

/*     Generate the first part of the vector Z; and move the singular */
/*     values in the first part of D one position backward. */

    z1 = *alpha * vt[nlp1 + nlp1 * vt_dim1];
    z__[1] = z1;
    for (i__ = *nl; i__ >= 1; --i__) {
	z__[i__ + 1] = *alpha * vt[i__ + nlp1 * vt_dim1];
	d__[i__ + 1] = d__[i__];
	idxq[i__ + 1] = idxq[i__] + 1;
/* L10: */
    }

/*     Generate the second part of the vector Z. */

    i__1 = m;
    for (i__ = nlp2; i__ <= i__1; ++i__) {
	z__[i__] = *beta * vt[i__ + nlp2 * vt_dim1];
/* L20: */
    }

/*     Initialize some reference arrays. */

    i__1 = nlp1;
    for (i__ = 2; i__ <= i__1; ++i__) {
	coltyp[i__] = 1;
/* L30: */
    }
    i__1 = n;
    for (i__ = nlp2; i__ <= i__1; ++i__) {
	coltyp[i__] = 2;
/* L40: */
    }

/*     Sort the singular values into increasing order */

    i__1 = n;
    for (i__ = nlp2; i__ <= i__1; ++i__) {
	idxq[i__] += nlp1;
/* L50: */
    }

/*     DSIGMA, IDXC, IDXC, and the first column of U2 */
/*     are used as storage space. */

    i__1 = n;
    for (i__ = 2; i__ <= i__1; ++i__) {
	dsigma[i__] = d__[idxq[i__]];
	u2[i__ + u2_dim1] = z__[idxq[i__]];
	idxc[i__] = coltyp[idxq[i__]];
/* L60: */
    }

    dlamrg_(nl, nr, &dsigma[2], &c__1, &c__1, &idx[2]);

    i__1 = n;
    for (i__ = 2; i__ <= i__1; ++i__) {
	idxi = idx[i__] + 1;
	d__[i__] = dsigma[idxi];
	z__[i__] = u2[idxi + u2_dim1];
	coltyp[i__] = idxc[idxi];
/* L70: */
    }

/*     Calculate the allowable deflation tolerance */

    eps = dlamch_("Epsilon");
/* Computing MAX */
    d__1 = abs(*alpha), d__2 = abs(*beta);
    tol = f2cmax(d__1,d__2);
/* Computing MAX */
    d__2 = (d__1 = d__[n], abs(d__1));
    tol = eps * 8. * f2cmax(d__2,tol);

/*     There are 2 kinds of deflation -- first a value in the z-vector */
/*     is small, second two (or more) singular values are very close */
/*     together (their difference is small). */

/*     If the value in the z-vector is small, we simply permute the */
/*     array so that the corresponding singular value is moved to the */
/*     end. */

/*     If two values in the D-vector are close, we perform a two-sided */
/*     rotation designed to make one of the corresponding z-vector */
/*     entries zero, and then permute the array so that the deflated */
/*     singular value is moved to the end. */

/*     If there are multiple singular values then the problem deflates. */
/*     Here the number of equal singular values are found.  As each equal */
/*     singular value is found, an elementary reflector is computed to */
/*     rotate the corresponding singular subspace so that the */
/*     corresponding components of Z are zero in this new basis. */

    *k = 1;
    k2 = n + 1;
    i__1 = n;
    for (j = 2; j <= i__1; ++j) {
	if ((d__1 = z__[j], abs(d__1)) <= tol) {

/*           Deflate due to small z component. */

	    --k2;
	    idxp[k2] = j;
	    coltyp[j] = 4;
	    if (j == n) {
		goto L120;
	    }
	} else {
	    jprev = j;
	    goto L90;
	}
/* L80: */
    }
L90:
    j = jprev;
L100:
    ++j;
    if (j > n) {
	goto L110;
    }
    if ((d__1 = z__[j], abs(d__1)) <= tol) {

/*        Deflate due to small z component. */

	--k2;
	idxp[k2] = j;
	coltyp[j] = 4;
    } else {

/*        Check if singular values are close enough to allow deflation. */

	if ((d__1 = d__[j] - d__[jprev], abs(d__1)) <= tol) {

/*           Deflation is possible. */

	    s = z__[jprev];
	    c__ = z__[j];

/*           Find sqrt(a**2+b**2) without overflow or */
/*           destructive underflow. */

	    tau = dlapy2_(&c__, &s);
	    c__ /= tau;
	    s = -s / tau;
	    z__[j] = tau;
	    z__[jprev] = 0.;

/*           Apply back the Givens rotation to the left and right */
/*           singular vector matrices. */

	    idxjp = idxq[idx[jprev] + 1];
	    idxj = idxq[idx[j] + 1];
	    if (idxjp <= nlp1) {
		--idxjp;
	    }
	    if (idxj <= nlp1) {
		--idxj;
	    }
	    drot_(&n, &u[idxjp * u_dim1 + 1], &c__1, &u[idxj * u_dim1 + 1], &
		    c__1, &c__, &s);
	    drot_(&m, &vt[idxjp + vt_dim1], ldvt, &vt[idxj + vt_dim1], ldvt, &
		    c__, &s);
	    if (coltyp[j] != coltyp[jprev]) {
		coltyp[j] = 3;
	    }
	    coltyp[jprev] = 4;
	    --k2;
	    idxp[k2] = jprev;
	    jprev = j;
	} else {
	    ++(*k);
	    u2[*k + u2_dim1] = z__[jprev];
	    dsigma[*k] = d__[jprev];
	    idxp[*k] = jprev;
	    jprev = j;
	}
    }
    goto L100;
L110:

/*     Record the last singular value. */

    ++(*k);
    u2[*k + u2_dim1] = z__[jprev];
    dsigma[*k] = d__[jprev];
    idxp[*k] = jprev;

L120:

/*     Count up the total number of the various types of columns, then */
/*     form a permutation which positions the four column types into */
/*     four groups of uniform structure (although one or more of these */
/*     groups may be empty). */

    for (j = 1; j <= 4; ++j) {
	ctot[j - 1] = 0;
/* L130: */
    }
    i__1 = n;
    for (j = 2; j <= i__1; ++j) {
	ct = coltyp[j];
	++ctot[ct - 1];
/* L140: */
    }

/*     PSM(*) = Position in SubMatrix (of types 1 through 4) */

    psm[0] = 2;
    psm[1] = ctot[0] + 2;
    psm[2] = psm[1] + ctot[1];
    psm[3] = psm[2] + ctot[2];

/*     Fill out the IDXC array so that the permutation which it induces */
/*     will place all type-1 columns first, all type-2 columns next, */
/*     then all type-3's, and finally all type-4's, starting from the */
/*     second column. This applies similarly to the rows of VT. */

    i__1 = n;
    for (j = 2; j <= i__1; ++j) {
	jp = idxp[j];
	ct = coltyp[jp];
	idxc[psm[ct - 1]] = j;
	++psm[ct - 1];
/* L150: */
    }

/*     Sort the singular values and corresponding singular vectors into */
/*     DSIGMA, U2, and VT2 respectively.  The singular values/vectors */
/*     which were not deflated go into the first K slots of DSIGMA, U2, */
/*     and VT2 respectively, while those which were deflated go into the */
/*     last N - K slots, except that the first column/row will be treated */
/*     separately. */

    i__1 = n;
    for (j = 2; j <= i__1; ++j) {
	jp = idxp[j];
	dsigma[j] = d__[jp];
	idxj = idxq[idx[idxp[idxc[j]]] + 1];
	if (idxj <= nlp1) {
	    --idxj;
	}
	dcopy_(&n, &u[idxj * u_dim1 + 1], &c__1, &u2[j * u2_dim1 + 1], &c__1);
	dcopy_(&m, &vt[idxj + vt_dim1], ldvt, &vt2[j + vt2_dim1], ldvt2);
/* L160: */
    }

/*     Determine DSIGMA(1), DSIGMA(2) and Z(1) */

    dsigma[1] = 0.;
    hlftol = tol / 2.;
    if (abs(dsigma[2]) <= hlftol) {
	dsigma[2] = hlftol;
    }
    if (m > n) {
	z__[1] = dlapy2_(&z1, &z__[m]);
	if (z__[1] <= tol) {
	    c__ = 1.;
	    s = 0.;
	    z__[1] = tol;
	} else {
	    c__ = z1 / z__[1];
	    s = z__[m] / z__[1];
	}
    } else {
	if (abs(z1) <= tol) {
	    z__[1] = tol;
	} else {
	    z__[1] = z1;
	}
    }

/*     Move the rest of the updating row to Z. */

    i__1 = *k - 1;
    dcopy_(&i__1, &u2[u2_dim1 + 2], &c__1, &z__[2], &c__1);

/*     Determine the first column of U2, the first row of VT2 and the */
/*     last row of VT. */

    dlaset_("A", &n, &c__1, &c_b30, &c_b30, &u2[u2_offset], ldu2);
    u2[nlp1 + u2_dim1] = 1.;
    if (m > n) {
	i__1 = nlp1;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    vt[m + i__ * vt_dim1] = -s * vt[nlp1 + i__ * vt_dim1];
	    vt2[i__ * vt2_dim1 + 1] = c__ * vt[nlp1 + i__ * vt_dim1];
/* L170: */
	}
	i__1 = m;
	for (i__ = nlp2; i__ <= i__1; ++i__) {
	    vt2[i__ * vt2_dim1 + 1] = s * vt[m + i__ * vt_dim1];
	    vt[m + i__ * vt_dim1] = c__ * vt[m + i__ * vt_dim1];
/* L180: */
	}
    } else {
	dcopy_(&m, &vt[nlp1 + vt_dim1], ldvt, &vt2[vt2_dim1 + 1], ldvt2);
    }
    if (m > n) {
	dcopy_(&m, &vt[m + vt_dim1], ldvt, &vt2[m + vt2_dim1], ldvt2);
    }

/*     The deflated singular values and their corresponding vectors go */
/*     into the back of D, U, and V respectively. */

    if (n > *k) {
	i__1 = n - *k;
	dcopy_(&i__1, &dsigma[*k + 1], &c__1, &d__[*k + 1], &c__1);
	i__1 = n - *k;
	dlacpy_("A", &n, &i__1, &u2[(*k + 1) * u2_dim1 + 1], ldu2, &u[(*k + 1)
		 * u_dim1 + 1], ldu);
	i__1 = n - *k;
	dlacpy_("A", &i__1, &m, &vt2[*k + 1 + vt2_dim1], ldvt2, &vt[*k + 1 + 
		vt_dim1], ldvt);
    }

/*     Copy CTOT into COLTYP for referencing in DLASD3. */

    for (j = 1; j <= 4; ++j) {
	coltyp[j] = ctot[j - 1];
/* L190: */
    }

    return;

/*     End of DLASD2 */

} /* dlasd2_ */

