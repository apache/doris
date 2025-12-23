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

/* > \brief \b SLARRF finds a new relatively robust representation such that at least one of the eigenvalues i
s relatively isolated. */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download SLARRF + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/slarrf.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/slarrf.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/slarrf.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE SLARRF( N, D, L, LD, CLSTRT, CLEND, */
/*                          W, WGAP, WERR, */
/*                          SPDIAM, CLGAPL, CLGAPR, PIVMIN, SIGMA, */
/*                          DPLUS, LPLUS, WORK, INFO ) */

/*       INTEGER            CLSTRT, CLEND, INFO, N */
/*       REAL               CLGAPL, CLGAPR, PIVMIN, SIGMA, SPDIAM */
/*       REAL               D( * ), DPLUS( * ), L( * ), LD( * ), */
/*      $          LPLUS( * ), W( * ), WGAP( * ), WERR( * ), WORK( * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > Given the initial representation L D L^T and its cluster of close */
/* > eigenvalues (in a relative measure), W( CLSTRT ), W( CLSTRT+1 ), ... */
/* > W( CLEND ), SLARRF finds a new relatively robust representation */
/* > L D L^T - SIGMA I = L(+) D(+) L(+)^T such that at least one of the */
/* > eigenvalues of L(+) D(+) L(+)^T is relatively isolated. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          The order of the matrix (subblock, if the matrix split). */
/* > \endverbatim */
/* > */
/* > \param[in] D */
/* > \verbatim */
/* >          D is REAL array, dimension (N) */
/* >          The N diagonal elements of the diagonal matrix D. */
/* > \endverbatim */
/* > */
/* > \param[in] L */
/* > \verbatim */
/* >          L is REAL array, dimension (N-1) */
/* >          The (N-1) subdiagonal elements of the unit bidiagonal */
/* >          matrix L. */
/* > \endverbatim */
/* > */
/* > \param[in] LD */
/* > \verbatim */
/* >          LD is REAL array, dimension (N-1) */
/* >          The (N-1) elements L(i)*D(i). */
/* > \endverbatim */
/* > */
/* > \param[in] CLSTRT */
/* > \verbatim */
/* >          CLSTRT is INTEGER */
/* >          The index of the first eigenvalue in the cluster. */
/* > \endverbatim */
/* > */
/* > \param[in] CLEND */
/* > \verbatim */
/* >          CLEND is INTEGER */
/* >          The index of the last eigenvalue in the cluster. */
/* > \endverbatim */
/* > */
/* > \param[in] W */
/* > \verbatim */
/* >          W is REAL array, dimension */
/* >          dimension is >=  (CLEND-CLSTRT+1) */
/* >          The eigenvalue APPROXIMATIONS of L D L^T in ascending order. */
/* >          W( CLSTRT ) through W( CLEND ) form the cluster of relatively */
/* >          close eigenalues. */
/* > \endverbatim */
/* > */
/* > \param[in,out] WGAP */
/* > \verbatim */
/* >          WGAP is REAL array, dimension */
/* >          dimension is >=  (CLEND-CLSTRT+1) */
/* >          The separation from the right neighbor eigenvalue in W. */
/* > \endverbatim */
/* > */
/* > \param[in] WERR */
/* > \verbatim */
/* >          WERR is REAL array, dimension */
/* >          dimension is >=  (CLEND-CLSTRT+1) */
/* >          WERR contain the semiwidth of the uncertainty */
/* >          interval of the corresponding eigenvalue APPROXIMATION in W */
/* > \endverbatim */
/* > */
/* > \param[in] SPDIAM */
/* > \verbatim */
/* >          SPDIAM is REAL */
/* >          estimate of the spectral diameter obtained from the */
/* >          Gerschgorin intervals */
/* > \endverbatim */
/* > */
/* > \param[in] CLGAPL */
/* > \verbatim */
/* >          CLGAPL is REAL */
/* > \endverbatim */
/* > */
/* > \param[in] CLGAPR */
/* > \verbatim */
/* >          CLGAPR is REAL */
/* >          absolute gap on each end of the cluster. */
/* >          Set by the calling routine to protect against shifts too close */
/* >          to eigenvalues outside the cluster. */
/* > \endverbatim */
/* > */
/* > \param[in] PIVMIN */
/* > \verbatim */
/* >          PIVMIN is REAL */
/* >          The minimum pivot allowed in the Sturm sequence. */
/* > \endverbatim */
/* > */
/* > \param[out] SIGMA */
/* > \verbatim */
/* >          SIGMA is REAL */
/* >          The shift used to form L(+) D(+) L(+)^T. */
/* > \endverbatim */
/* > */
/* > \param[out] DPLUS */
/* > \verbatim */
/* >          DPLUS is REAL array, dimension (N) */
/* >          The N diagonal elements of the diagonal matrix D(+). */
/* > \endverbatim */
/* > */
/* > \param[out] LPLUS */
/* > \verbatim */
/* >          LPLUS is REAL array, dimension (N-1) */
/* >          The first (N-1) elements of LPLUS contain the subdiagonal */
/* >          elements of the unit bidiagonal matrix L(+). */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is REAL array, dimension (2*N) */
/* >          Workspace. */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >          Signals processing OK (=0) or failure (=1) */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date June 2016 */

/* > \ingroup OTHERauxiliary */

/* > \par Contributors: */
/*  ================== */
/* > */
/* > Beresford Parlett, University of California, Berkeley, USA \n */
/* > Jim Demmel, University of California, Berkeley, USA \n */
/* > Inderjit Dhillon, University of Texas, Austin, USA \n */
/* > Osni Marques, LBNL/NERSC, USA \n */
/* > Christof Voemel, University of California, Berkeley, USA */

/*  ===================================================================== */
/* Subroutine */ void slarrf_(integer *n, real *d__, real *l, real *ld, 
	integer *clstrt, integer *clend, real *w, real *wgap, real *werr, 
	real *spdiam, real *clgapl, real *clgapr, real *pivmin, real *sigma, 
	real *dplus, real *lplus, real *work, integer *info)
{
    /* System generated locals */
    integer i__1;
    real r__1, r__2, r__3;

    /* Local variables */
    real growthbound, fail, fact, oldp;
    integer indx;
    real prod;
    integer ktry;
    real fail2;
    integer i__;
    real s, avgap, ldmax, rdmax;
    integer shift;
    extern /* Subroutine */ void scopy_(integer *, real *, integer *, real *, 
	    integer *);
    real bestshift, smlgrowth;
    logical dorrr1;
    real ldelta;
    extern real slamch_(char *);
    logical nofail;
    real mingap, lsigma, rdelta;
    logical forcer;
    real rsigma, clwdth;
    extern logical sisnan_(real *);
    logical sawnan1, sawnan2;
    real eps, tmp;
    logical tryrrr1;
    real max1, max2, rrr1, rrr2, znm2;


/*  -- LAPACK auxiliary routine (version 3.7.1) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     June 2016 */


/*  ===================================================================== */


    /* Parameter adjustments */
    --work;
    --lplus;
    --dplus;
    --werr;
    --wgap;
    --w;
    --ld;
    --l;
    --d__;

    /* Function Body */
    *info = 0;

/*     Quick return if possible */

    if (*n <= 0) {
	return;
    }

    fact = 2.f;
    eps = slamch_("Precision");
    shift = 0;
    forcer = FALSE_;
/*     Note that we cannot guarantee that for any of the shifts tried, */
/*     the factorization has a small or even moderate element growth. */
/*     There could be Ritz values at both ends of the cluster and despite */
/*     backing off, there are examples where all factorizations tried */
/*     (in IEEE mode, allowing zero pivots & infinities) have INFINITE */
/*     element growth. */
/*     For this reason, we should use PIVMIN in this subroutine so that at */
/*     least the L D L^T factorization exists. It can be checked afterwards */
/*     whether the element growth caused bad residuals/orthogonality. */
/*     Decide whether the code should accept the best among all */
/*     representations despite large element growth or signal INFO=1 */
/*     Setting NOFAIL to .FALSE. for quick fix for bug 113 */
    nofail = FALSE_;

/*     Compute the average gap length of the cluster */
    clwdth = (r__1 = w[*clend] - w[*clstrt], abs(r__1)) + werr[*clend] + werr[
	    *clstrt];
    avgap = clwdth / (real) (*clend - *clstrt);
    mingap = f2cmin(*clgapl,*clgapr);
/*     Initial values for shifts to both ends of cluster */
/* Computing MIN */
    r__1 = w[*clstrt], r__2 = w[*clend];
    lsigma = f2cmin(r__1,r__2) - werr[*clstrt];
/* Computing MAX */
    r__1 = w[*clstrt], r__2 = w[*clend];
    rsigma = f2cmax(r__1,r__2) + werr[*clend];
/*     Use a small fudge to make sure that we really shift to the outside */
    lsigma -= abs(lsigma) * 2.f * eps;
    rsigma += abs(rsigma) * 2.f * eps;
/*     Compute upper bounds for how much to back off the initial shifts */
    ldmax = mingap * .25f + *pivmin * 2.f;
    rdmax = mingap * .25f + *pivmin * 2.f;
/* Computing MAX */
    r__1 = avgap, r__2 = wgap[*clstrt];
    ldelta = f2cmax(r__1,r__2) / fact;
/* Computing MAX */
    r__1 = avgap, r__2 = wgap[*clend - 1];
    rdelta = f2cmax(r__1,r__2) / fact;

/*     Initialize the record of the best representation found */

    s = slamch_("S");
    smlgrowth = 1.f / s;
    fail = (real) (*n - 1) * mingap / (*spdiam * eps);
    fail2 = (real) (*n - 1) * mingap / (*spdiam * sqrt(eps));
    bestshift = lsigma;

/*     while (KTRY <= KTRYMAX) */
    ktry = 0;
    growthbound = *spdiam * 8.f;
L5:
    sawnan1 = FALSE_;
    sawnan2 = FALSE_;
/*     Ensure that we do not back off too much of the initial shifts */
    ldelta = f2cmin(ldmax,ldelta);
    rdelta = f2cmin(rdmax,rdelta);
/*     Compute the element growth when shifting to both ends of the cluster */
/*     accept the shift if there is no element growth at one of the two ends */
/*     Left end */
    s = -lsigma;
    dplus[1] = d__[1] + s;
    if (abs(dplus[1]) < *pivmin) {
	dplus[1] = -(*pivmin);
/*        Need to set SAWNAN1 because refined RRR test should not be used */
/*        in this case */
	sawnan1 = TRUE_;
    }
    max1 = abs(dplus[1]);
    i__1 = *n - 1;
    for (i__ = 1; i__ <= i__1; ++i__) {
	lplus[i__] = ld[i__] / dplus[i__];
	s = s * lplus[i__] * l[i__] - lsigma;
	dplus[i__ + 1] = d__[i__ + 1] + s;
	if ((r__1 = dplus[i__ + 1], abs(r__1)) < *pivmin) {
	    dplus[i__ + 1] = -(*pivmin);
/*           Need to set SAWNAN1 because refined RRR test should not be used */
/*           in this case */
	    sawnan1 = TRUE_;
	}
/* Computing MAX */
	r__2 = max1, r__3 = (r__1 = dplus[i__ + 1], abs(r__1));
	max1 = f2cmax(r__2,r__3);
/* L6: */
    }
    sawnan1 = sawnan1 || sisnan_(&max1);
    if (forcer || max1 <= growthbound && ! sawnan1) {
	*sigma = lsigma;
	shift = 1;
	goto L100;
    }
/*     Right end */
    s = -rsigma;
    work[1] = d__[1] + s;
    if (abs(work[1]) < *pivmin) {
	work[1] = -(*pivmin);
/*        Need to set SAWNAN2 because refined RRR test should not be used */
/*        in this case */
	sawnan2 = TRUE_;
    }
    max2 = abs(work[1]);
    i__1 = *n - 1;
    for (i__ = 1; i__ <= i__1; ++i__) {
	work[*n + i__] = ld[i__] / work[i__];
	s = s * work[*n + i__] * l[i__] - rsigma;
	work[i__ + 1] = d__[i__ + 1] + s;
	if ((r__1 = work[i__ + 1], abs(r__1)) < *pivmin) {
	    work[i__ + 1] = -(*pivmin);
/*           Need to set SAWNAN2 because refined RRR test should not be used */
/*           in this case */
	    sawnan2 = TRUE_;
	}
/* Computing MAX */
	r__2 = max2, r__3 = (r__1 = work[i__ + 1], abs(r__1));
	max2 = f2cmax(r__2,r__3);
/* L7: */
    }
    sawnan2 = sawnan2 || sisnan_(&max2);
    if (forcer || max2 <= growthbound && ! sawnan2) {
	*sigma = rsigma;
	shift = 2;
	goto L100;
    }
/*     If we are at this point, both shifts led to too much element growth */
/*     Record the better of the two shifts (provided it didn't lead to NaN) */
    if (sawnan1 && sawnan2) {
/*        both MAX1 and MAX2 are NaN */
	goto L50;
    } else {
	if (! sawnan1) {
	    indx = 1;
	    if (max1 <= smlgrowth) {
		smlgrowth = max1;
		bestshift = lsigma;
	    }
	}
	if (! sawnan2) {
	    if (sawnan1 || max2 <= max1) {
		indx = 2;
	    }
	    if (max2 <= smlgrowth) {
		smlgrowth = max2;
		bestshift = rsigma;
	    }
	}
    }
/*     If we are here, both the left and the right shift led to */
/*     element growth. If the element growth is moderate, then */
/*     we may still accept the representation, if it passes a */
/*     refined test for RRR. This test supposes that no NaN occurred. */
/*     Moreover, we use the refined RRR test only for isolated clusters. */
    if (clwdth < mingap / 128.f && f2cmin(max1,max2) < fail2 && ! sawnan1 && ! 
	    sawnan2) {
	dorrr1 = TRUE_;
    } else {
	dorrr1 = FALSE_;
    }
    tryrrr1 = TRUE_;
    if (tryrrr1 && dorrr1) {
	if (indx == 1) {
	    tmp = (r__1 = dplus[*n], abs(r__1));
	    znm2 = 1.f;
	    prod = 1.f;
	    oldp = 1.f;
	    for (i__ = *n - 1; i__ >= 1; --i__) {
		if (prod <= eps) {
		    prod = dplus[i__ + 1] * work[*n + i__ + 1] / (dplus[i__] *
			     work[*n + i__]) * oldp;
		} else {
		    prod *= (r__1 = work[*n + i__], abs(r__1));
		}
		oldp = prod;
/* Computing 2nd power */
		r__1 = prod;
		znm2 += r__1 * r__1;
/* Computing MAX */
		r__2 = tmp, r__3 = (r__1 = dplus[i__] * prod, abs(r__1));
		tmp = f2cmax(r__2,r__3);
/* L15: */
	    }
	    rrr1 = tmp / (*spdiam * sqrt(znm2));
	    if (rrr1 <= 8.f) {
		*sigma = lsigma;
		shift = 1;
		goto L100;
	    }
	} else if (indx == 2) {
	    tmp = (r__1 = work[*n], abs(r__1));
	    znm2 = 1.f;
	    prod = 1.f;
	    oldp = 1.f;
	    for (i__ = *n - 1; i__ >= 1; --i__) {
		if (prod <= eps) {
		    prod = work[i__ + 1] * lplus[i__ + 1] / (work[i__] * 
			    lplus[i__]) * oldp;
		} else {
		    prod *= (r__1 = lplus[i__], abs(r__1));
		}
		oldp = prod;
/* Computing 2nd power */
		r__1 = prod;
		znm2 += r__1 * r__1;
/* Computing MAX */
		r__2 = tmp, r__3 = (r__1 = work[i__] * prod, abs(r__1));
		tmp = f2cmax(r__2,r__3);
/* L16: */
	    }
	    rrr2 = tmp / (*spdiam * sqrt(znm2));
	    if (rrr2 <= 8.f) {
		*sigma = rsigma;
		shift = 2;
		goto L100;
	    }
	}
    }
L50:
    if (ktry < 1) {
/*        If we are here, both shifts failed also the RRR test. */
/*        Back off to the outside */
/* Computing MAX */
	r__1 = lsigma - ldelta, r__2 = lsigma - ldmax;
	lsigma = f2cmax(r__1,r__2);
/* Computing MIN */
	r__1 = rsigma + rdelta, r__2 = rsigma + rdmax;
	rsigma = f2cmin(r__1,r__2);
	ldelta *= 2.f;
	rdelta *= 2.f;
	++ktry;
	goto L5;
    } else {
/*        None of the representations investigated satisfied our */
/*        criteria. Take the best one we found. */
	if (smlgrowth < fail || nofail) {
	    lsigma = bestshift;
	    rsigma = bestshift;
	    forcer = TRUE_;
	    goto L5;
	} else {
	    *info = 1;
	    return;
	}
    }
L100:
    if (shift == 1) {
    } else if (shift == 2) {
/*        store new L and D back into DPLUS, LPLUS */
	scopy_(n, &work[1], &c__1, &dplus[1], &c__1);
	i__1 = *n - 1;
	scopy_(&i__1, &work[*n + 1], &c__1, &lplus[1], &c__1);
    }
    return;

/*     End of SLARRF */

} /* slarrf_ */

