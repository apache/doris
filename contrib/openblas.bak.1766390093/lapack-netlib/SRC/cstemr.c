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
static real c_b18 = .003f;

/* > \brief \b CSTEMR */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download CSTEMR + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/cstemr.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/cstemr.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/cstemr.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE CSTEMR( JOBZ, RANGE, N, D, E, VL, VU, IL, IU, */
/*                          M, W, Z, LDZ, NZC, ISUPPZ, TRYRAC, WORK, LWORK, */
/*                          IWORK, LIWORK, INFO ) */

/*       CHARACTER          JOBZ, RANGE */
/*       LOGICAL            TRYRAC */
/*       INTEGER            IL, INFO, IU, LDZ, NZC, LIWORK, LWORK, M, N */
/*       REAL             VL, VU */
/*       INTEGER            ISUPPZ( * ), IWORK( * ) */
/*       REAL               D( * ), E( * ), W( * ), WORK( * ) */
/*       COMPLEX            Z( LDZ, * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > CSTEMR computes selected eigenvalues and, optionally, eigenvectors */
/* > of a real symmetric tridiagonal matrix T. Any such unreduced matrix has */
/* > a well defined set of pairwise different real eigenvalues, the corresponding */
/* > real eigenvectors are pairwise orthogonal. */
/* > */
/* > The spectrum may be computed either completely or partially by specifying */
/* > either an interval (VL,VU] or a range of indices IL:IU for the desired */
/* > eigenvalues. */
/* > */
/* > Depending on the number of desired eigenvalues, these are computed either */
/* > by bisection or the dqds algorithm. Numerically orthogonal eigenvectors are */
/* > computed by the use of various suitable L D L^T factorizations near clusters */
/* > of close eigenvalues (referred to as RRRs, Relatively Robust */
/* > Representations). An informal sketch of the algorithm follows. */
/* > */
/* > For each unreduced block (submatrix) of T, */
/* >    (a) Compute T - sigma I  = L D L^T, so that L and D */
/* >        define all the wanted eigenvalues to high relative accuracy. */
/* >        This means that small relative changes in the entries of D and L */
/* >        cause only small relative changes in the eigenvalues and */
/* >        eigenvectors. The standard (unfactored) representation of the */
/* >        tridiagonal matrix T does not have this property in general. */
/* >    (b) Compute the eigenvalues to suitable accuracy. */
/* >        If the eigenvectors are desired, the algorithm attains full */
/* >        accuracy of the computed eigenvalues only right before */
/* >        the corresponding vectors have to be computed, see steps c) and d). */
/* >    (c) For each cluster of close eigenvalues, select a new */
/* >        shift close to the cluster, find a new factorization, and refine */
/* >        the shifted eigenvalues to suitable accuracy. */
/* >    (d) For each eigenvalue with a large enough relative separation compute */
/* >        the corresponding eigenvector by forming a rank revealing twisted */
/* >        factorization. Go back to (c) for any clusters that remain. */
/* > */
/* > For more details, see: */
/* > - Inderjit S. Dhillon and Beresford N. Parlett: "Multiple representations */
/* >   to compute orthogonal eigenvectors of symmetric tridiagonal matrices," */
/* >   Linear Algebra and its Applications, 387(1), pp. 1-28, August 2004. */
/* > - Inderjit Dhillon and Beresford Parlett: "Orthogonal Eigenvectors and */
/* >   Relative Gaps," SIAM Journal on Matrix Analysis and Applications, Vol. 25, */
/* >   2004.  Also LAPACK Working Note 154. */
/* > - Inderjit Dhillon: "A new O(n^2) algorithm for the symmetric */
/* >   tridiagonal eigenvalue/eigenvector problem", */
/* >   Computer Science Division Technical Report No. UCB/CSD-97-971, */
/* >   UC Berkeley, May 1997. */
/* > */
/* > Further Details */
/* > 1.CSTEMR works only on machines which follow IEEE-754 */
/* > floating-point standard in their handling of infinities and NaNs. */
/* > This permits the use of efficient inner loops avoiding a check for */
/* > zero divisors. */
/* > */
/* > 2. LAPACK routines can be used to reduce a complex Hermitean matrix to */
/* > real symmetric tridiagonal form. */
/* > */
/* > (Any complex Hermitean tridiagonal matrix has real values on its diagonal */
/* > and potentially complex numbers on its off-diagonals. By applying a */
/* > similarity transform with an appropriate diagonal matrix */
/* > diag(1,e^{i \phy_1}, ... , e^{i \phy_{n-1}}), the complex Hermitean */
/* > matrix can be transformed into a real symmetric matrix and complex */
/* > arithmetic can be entirely avoided.) */
/* > */
/* > While the eigenvectors of the real symmetric tridiagonal matrix are real, */
/* > the eigenvectors of original complex Hermitean matrix have complex entries */
/* > in general. */
/* > Since LAPACK drivers overwrite the matrix data with the eigenvectors, */
/* > CSTEMR accepts complex workspace to facilitate interoperability */
/* > with CUNMTR or CUPMTR. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] JOBZ */
/* > \verbatim */
/* >          JOBZ is CHARACTER*1 */
/* >          = 'N':  Compute eigenvalues only; */
/* >          = 'V':  Compute eigenvalues and eigenvectors. */
/* > \endverbatim */
/* > */
/* > \param[in] RANGE */
/* > \verbatim */
/* >          RANGE is CHARACTER*1 */
/* >          = 'A': all eigenvalues will be found. */
/* >          = 'V': all eigenvalues in the half-open interval (VL,VU] */
/* >                 will be found. */
/* >          = 'I': the IL-th through IU-th eigenvalues will be found. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          The order of the matrix.  N >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in,out] D */
/* > \verbatim */
/* >          D is REAL array, dimension (N) */
/* >          On entry, the N diagonal elements of the tridiagonal matrix */
/* >          T. On exit, D is overwritten. */
/* > \endverbatim */
/* > */
/* > \param[in,out] E */
/* > \verbatim */
/* >          E is REAL array, dimension (N) */
/* >          On entry, the (N-1) subdiagonal elements of the tridiagonal */
/* >          matrix T in elements 1 to N-1 of E. E(N) need not be set on */
/* >          input, but is used internally as workspace. */
/* >          On exit, E is overwritten. */
/* > \endverbatim */
/* > */
/* > \param[in] VL */
/* > \verbatim */
/* >          VL is REAL */
/* > */
/* >          If RANGE='V', the lower bound of the interval to */
/* >          be searched for eigenvalues. VL < VU. */
/* >          Not referenced if RANGE = 'A' or 'I'. */
/* > \endverbatim */
/* > */
/* > \param[in] VU */
/* > \verbatim */
/* >          VU is REAL */
/* > */
/* >          If RANGE='V', the upper bound of the interval to */
/* >          be searched for eigenvalues. VL < VU. */
/* >          Not referenced if RANGE = 'A' or 'I'. */
/* > \endverbatim */
/* > */
/* > \param[in] IL */
/* > \verbatim */
/* >          IL is INTEGER */
/* > */
/* >          If RANGE='I', the index of the */
/* >          smallest eigenvalue to be returned. */
/* >          1 <= IL <= IU <= N, if N > 0. */
/* >          Not referenced if RANGE = 'A' or 'V'. */
/* > \endverbatim */
/* > */
/* > \param[in] IU */
/* > \verbatim */
/* >          IU is INTEGER */
/* > */
/* >          If RANGE='I', the index of the */
/* >          largest eigenvalue to be returned. */
/* >          1 <= IL <= IU <= N, if N > 0. */
/* >          Not referenced if RANGE = 'A' or 'V'. */
/* > \endverbatim */
/* > */
/* > \param[out] M */
/* > \verbatim */
/* >          M is INTEGER */
/* >          The total number of eigenvalues found.  0 <= M <= N. */
/* >          If RANGE = 'A', M = N, and if RANGE = 'I', M = IU-IL+1. */
/* > \endverbatim */
/* > */
/* > \param[out] W */
/* > \verbatim */
/* >          W is REAL array, dimension (N) */
/* >          The first M elements contain the selected eigenvalues in */
/* >          ascending order. */
/* > \endverbatim */
/* > */
/* > \param[out] Z */
/* > \verbatim */
/* >          Z is COMPLEX array, dimension (LDZ, f2cmax(1,M) ) */
/* >          If JOBZ = 'V', and if INFO = 0, then the first M columns of Z */
/* >          contain the orthonormal eigenvectors of the matrix T */
/* >          corresponding to the selected eigenvalues, with the i-th */
/* >          column of Z holding the eigenvector associated with W(i). */
/* >          If JOBZ = 'N', then Z is not referenced. */
/* >          Note: the user must ensure that at least f2cmax(1,M) columns are */
/* >          supplied in the array Z; if RANGE = 'V', the exact value of M */
/* >          is not known in advance and can be computed with a workspace */
/* >          query by setting NZC = -1, see below. */
/* > \endverbatim */
/* > */
/* > \param[in] LDZ */
/* > \verbatim */
/* >          LDZ is INTEGER */
/* >          The leading dimension of the array Z.  LDZ >= 1, and if */
/* >          JOBZ = 'V', then LDZ >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[in] NZC */
/* > \verbatim */
/* >          NZC is INTEGER */
/* >          The number of eigenvectors to be held in the array Z. */
/* >          If RANGE = 'A', then NZC >= f2cmax(1,N). */
/* >          If RANGE = 'V', then NZC >= the number of eigenvalues in (VL,VU]. */
/* >          If RANGE = 'I', then NZC >= IU-IL+1. */
/* >          If NZC = -1, then a workspace query is assumed; the */
/* >          routine calculates the number of columns of the array Z that */
/* >          are needed to hold the eigenvectors. */
/* >          This value is returned as the first entry of the Z array, and */
/* >          no error message related to NZC is issued by XERBLA. */
/* > \endverbatim */
/* > */
/* > \param[out] ISUPPZ */
/* > \verbatim */
/* >          ISUPPZ is INTEGER array, dimension ( 2*f2cmax(1,M) ) */
/* >          The support of the eigenvectors in Z, i.e., the indices */
/* >          indicating the nonzero elements in Z. The i-th computed eigenvector */
/* >          is nonzero only in elements ISUPPZ( 2*i-1 ) through */
/* >          ISUPPZ( 2*i ). This is relevant in the case when the matrix */
/* >          is split. ISUPPZ is only accessed when JOBZ is 'V' and N > 0. */
/* > \endverbatim */
/* > */
/* > \param[in,out] TRYRAC */
/* > \verbatim */
/* >          TRYRAC is LOGICAL */
/* >          If TRYRAC = .TRUE., indicates that the code should check whether */
/* >          the tridiagonal matrix defines its eigenvalues to high relative */
/* >          accuracy.  If so, the code uses relative-accuracy preserving */
/* >          algorithms that might be (a bit) slower depending on the matrix. */
/* >          If the matrix does not define its eigenvalues to high relative */
/* >          accuracy, the code can uses possibly faster algorithms. */
/* >          If TRYRAC = .FALSE., the code is not required to guarantee */
/* >          relatively accurate eigenvalues and can use the fastest possible */
/* >          techniques. */
/* >          On exit, a .TRUE. TRYRAC will be set to .FALSE. if the matrix */
/* >          does not define its eigenvalues to high relative accuracy. */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is REAL array, dimension (LWORK) */
/* >          On exit, if INFO = 0, WORK(1) returns the optimal */
/* >          (and minimal) LWORK. */
/* > \endverbatim */
/* > */
/* > \param[in] LWORK */
/* > \verbatim */
/* >          LWORK is INTEGER */
/* >          The dimension of the array WORK. LWORK >= f2cmax(1,18*N) */
/* >          if JOBZ = 'V', and LWORK >= f2cmax(1,12*N) if JOBZ = 'N'. */
/* >          If LWORK = -1, then a workspace query is assumed; the routine */
/* >          only calculates the optimal size of the WORK array, returns */
/* >          this value as the first entry of the WORK array, and no error */
/* >          message related to LWORK is issued by XERBLA. */
/* > \endverbatim */
/* > */
/* > \param[out] IWORK */
/* > \verbatim */
/* >          IWORK is INTEGER array, dimension (LIWORK) */
/* >          On exit, if INFO = 0, IWORK(1) returns the optimal LIWORK. */
/* > \endverbatim */
/* > */
/* > \param[in] LIWORK */
/* > \verbatim */
/* >          LIWORK is INTEGER */
/* >          The dimension of the array IWORK.  LIWORK >= f2cmax(1,10*N) */
/* >          if the eigenvectors are desired, and LIWORK >= f2cmax(1,8*N) */
/* >          if only the eigenvalues are to be computed. */
/* >          If LIWORK = -1, then a workspace query is assumed; the */
/* >          routine only calculates the optimal size of the IWORK array, */
/* >          returns this value as the first entry of the IWORK array, and */
/* >          no error message related to LIWORK is issued by XERBLA. */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >          On exit, INFO */
/* >          = 0:  successful exit */
/* >          < 0:  if INFO = -i, the i-th argument had an illegal value */
/* >          > 0:  if INFO = 1X, internal error in SLARRE, */
/* >                if INFO = 2X, internal error in CLARRV. */
/* >                Here, the digit X = ABS( IINFO ) < 10, where IINFO is */
/* >                the nonzero error code returned by SLARRE or */
/* >                CLARRV, respectively. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date June 2016 */

/* > \ingroup complexOTHERcomputational */

/* > \par Contributors: */
/*  ================== */
/* > */
/* > Beresford Parlett, University of California, Berkeley, USA \n */
/* > Jim Demmel, University of California, Berkeley, USA \n */
/* > Inderjit Dhillon, University of Texas, Austin, USA \n */
/* > Osni Marques, LBNL/NERSC, USA \n */
/* > Christof Voemel, University of California, Berkeley, USA */

/*  ===================================================================== */
/* Subroutine */ void cstemr_(char *jobz, char *range, integer *n, real *d__, 
	real *e, real *vl, real *vu, integer *il, integer *iu, integer *m, 
	real *w, complex *z__, integer *ldz, integer *nzc, integer *isuppz, 
	logical *tryrac, real *work, integer *lwork, integer *iwork, integer *
	liwork, integer *info)
{
    /* System generated locals */
    integer z_dim1, z_offset, i__1, i__2;
    real r__1, r__2;

    /* Local variables */
    integer indd, iend, jblk, wend;
    real rmin, rmax;
    integer itmp;
    real tnrm;
    integer inde2;
    extern /* Subroutine */ void slae2_(real *, real *, real *, real *, real *)
	    ;
    integer itmp2;
    real rtol1, rtol2;
    integer i__, j;
    real scale;
    integer indgp;
    extern logical lsame_(char *, char *);
    integer iinfo;
    extern /* Subroutine */ void sscal_(integer *, real *, real *, integer *);
    integer iindw, ilast;
    extern /* Subroutine */ void cswap_(integer *, complex *, integer *, 
	    complex *, integer *);
    integer lwmin;
    extern /* Subroutine */ void scopy_(integer *, real *, integer *, real *, 
	    integer *);
    logical wantz;
    real r1, r2;
    extern /* Subroutine */ void slaev2_(real *, real *, real *, real *, real *
	    , real *, real *);
    integer jj;
    real cs;
    integer in;
    logical alleig, indeig;
    integer ibegin, iindbl;
    real sn, wl;
    logical valeig;
    extern real slamch_(char *);
    integer wbegin;
    real safmin, wu;
    extern /* Subroutine */ int xerbla_(char *, integer *, ftnlen);
    real bignum;
    integer inderr, iindwk, indgrs, offset;
    extern /* Subroutine */ void slarrc_(char *, integer *, real *, real *, 
	    real *, real *, real *, integer *, integer *, integer *, integer *
	    ), clarrv_(integer *, real *, real *, real *, real *, 
	    real *, integer *, integer *, integer *, integer *, real *, real *
	    , real *, real *, real *, real *, integer *, integer *, real *, 
	    complex *, integer *, integer *, real *, integer *, integer *), 
	    slarre_(char *, integer *, real *, real *, integer *, integer *, 
	    real *, real *, real *, real *, real *, real *, integer *, 
	    integer *, integer *, real *, real *, real *, integer *, integer *
	    , real *, real *, real *, integer *, integer *);
    integer iinspl, indwrk, ifirst, liwmin, nzcmin;
    real pivmin, thresh;
    extern real slanst_(char *, integer *, real *, real *);
    extern /* Subroutine */ void slarrj_(integer *, real *, real *, integer *, 
	    integer *, real *, integer *, real *, real *, real *, integer *, 
	    real *, real *, integer *);
    integer nsplit;
    extern /* Subroutine */ void slarrr_(integer *, real *, real *, integer *);
    real smlnum;
    extern /* Subroutine */ void slasrt_(char *, integer *, real *, integer *);
    logical lquery, zquery;
    integer iil, iiu;
    real eps, tmp;


/*  -- LAPACK computational routine (version 3.7.1) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     June 2016 */


/*  ===================================================================== */


/*     Test the input parameters. */

    /* Parameter adjustments */
    --d__;
    --e;
    --w;
    z_dim1 = *ldz;
    z_offset = 1 + z_dim1 * 1;
    z__ -= z_offset;
    --isuppz;
    --work;
    --iwork;

    /* Function Body */
    wantz = lsame_(jobz, "V");
    alleig = lsame_(range, "A");
    valeig = lsame_(range, "V");
    indeig = lsame_(range, "I");

    lquery = *lwork == -1 || *liwork == -1;
    zquery = *nzc == -1;
/*     SSTEMR needs WORK of size 6*N, IWORK of size 3*N. */
/*     In addition, SLARRE needs WORK of size 6*N, IWORK of size 5*N. */
/*     Furthermore, CLARRV needs WORK of size 12*N, IWORK of size 7*N. */
    if (wantz) {
	lwmin = *n * 18;
	liwmin = *n * 10;
    } else {
/*        need less workspace if only the eigenvalues are wanted */
	lwmin = *n * 12;
	liwmin = *n << 3;
    }
    wl = 0.f;
    wu = 0.f;
    iil = 0;
    iiu = 0;
    nsplit = 0;
    if (valeig) {
/*        We do not reference VL, VU in the cases RANGE = 'I','A' */
/*        The interval (WL, WU] contains all the wanted eigenvalues. */
/*        It is either given by the user or computed in SLARRE. */
	wl = *vl;
	wu = *vu;
    } else if (indeig) {
/*        We do not reference IL, IU in the cases RANGE = 'V','A' */
	iil = *il;
	iiu = *iu;
    }

    *info = 0;
    if (! (wantz || lsame_(jobz, "N"))) {
	*info = -1;
    } else if (! (alleig || valeig || indeig)) {
	*info = -2;
    } else if (*n < 0) {
	*info = -3;
    } else if (valeig && *n > 0 && wu <= wl) {
	*info = -7;
    } else if (indeig && (iil < 1 || iil > *n)) {
	*info = -8;
    } else if (indeig && (iiu < iil || iiu > *n)) {
	*info = -9;
    } else if (*ldz < 1 || wantz && *ldz < *n) {
	*info = -13;
    } else if (*lwork < lwmin && ! lquery) {
	*info = -17;
    } else if (*liwork < liwmin && ! lquery) {
	*info = -19;
    }

/*     Get machine constants. */

    safmin = slamch_("Safe minimum");
    eps = slamch_("Precision");
    smlnum = safmin / eps;
    bignum = 1.f / smlnum;
    rmin = sqrt(smlnum);
/* Computing MIN */
    r__1 = sqrt(bignum), r__2 = 1.f / sqrt(sqrt(safmin));
    rmax = f2cmin(r__1,r__2);

    if (*info == 0) {
	work[1] = (real) lwmin;
	iwork[1] = liwmin;

	if (wantz && alleig) {
	    nzcmin = *n;
	} else if (wantz && valeig) {
	    slarrc_("T", n, vl, vu, &d__[1], &e[1], &safmin, &nzcmin, &itmp, &
		    itmp2, info);
	} else if (wantz && indeig) {
	    nzcmin = iiu - iil + 1;
	} else {
/*           WANTZ .EQ. FALSE. */
	    nzcmin = 0;
	}
	if (zquery && *info == 0) {
	    i__1 = z_dim1 + 1;
	    z__[i__1].r = (real) nzcmin, z__[i__1].i = 0.f;
	} else if (*nzc < nzcmin && ! zquery) {
	    *info = -14;
	}
    }
    if (*info != 0) {

	i__1 = -(*info);
	xerbla_("CSTEMR", &i__1, (ftnlen)6);

	return;
    } else if (lquery || zquery) {
	return;
    }

/*     Handle N = 0, 1, and 2 cases immediately */

    *m = 0;
    if (*n == 0) {
	return;
    }

    if (*n == 1) {
	if (alleig || indeig) {
	    *m = 1;
	    w[1] = d__[1];
	} else {
	    if (wl < d__[1] && wu >= d__[1]) {
		*m = 1;
		w[1] = d__[1];
	    }
	}
	if (wantz && ! zquery) {
	    i__1 = z_dim1 + 1;
	    z__[i__1].r = 1.f, z__[i__1].i = 0.f;
	    isuppz[1] = 1;
	    isuppz[2] = 1;
	}
	return;
    }

    if (*n == 2) {
	if (! wantz) {
	    slae2_(&d__[1], &e[1], &d__[2], &r1, &r2);
	} else if (wantz && ! zquery) {
	    slaev2_(&d__[1], &e[1], &d__[2], &r1, &r2, &cs, &sn);
	}
	if (alleig || valeig && r2 > wl && r2 <= wu || indeig && iil == 1) {
	    ++(*m);
	    w[*m] = r2;
	    if (wantz && ! zquery) {
		i__1 = *m * z_dim1 + 1;
		r__1 = -sn;
		z__[i__1].r = r__1, z__[i__1].i = 0.f;
		i__1 = *m * z_dim1 + 2;
		z__[i__1].r = cs, z__[i__1].i = 0.f;
/*              Note: At most one of SN and CS can be zero. */
		if (sn != 0.f) {
		    if (cs != 0.f) {
			isuppz[(*m << 1) - 1] = 1;
			isuppz[*m * 2] = 2;
		    } else {
			isuppz[(*m << 1) - 1] = 1;
			isuppz[*m * 2] = 1;
		    }
		} else {
		    isuppz[(*m << 1) - 1] = 2;
		    isuppz[*m * 2] = 2;
		}
	    }
	}
	if (alleig || valeig && r1 > wl && r1 <= wu || indeig && iiu == 2) {
	    ++(*m);
	    w[*m] = r1;
	    if (wantz && ! zquery) {
		i__1 = *m * z_dim1 + 1;
		z__[i__1].r = cs, z__[i__1].i = 0.f;
		i__1 = *m * z_dim1 + 2;
		z__[i__1].r = sn, z__[i__1].i = 0.f;
/*              Note: At most one of SN and CS can be zero. */
		if (sn != 0.f) {
		    if (cs != 0.f) {
			isuppz[(*m << 1) - 1] = 1;
			isuppz[*m * 2] = 2;
		    } else {
			isuppz[(*m << 1) - 1] = 1;
			isuppz[*m * 2] = 1;
		    }
		} else {
		    isuppz[(*m << 1) - 1] = 2;
		    isuppz[*m * 2] = 2;
		}
	    }
	}
    } else {
/*        Continue with general N */
	indgrs = 1;
	inderr = (*n << 1) + 1;
	indgp = *n * 3 + 1;
	indd = (*n << 2) + 1;
	inde2 = *n * 5 + 1;
	indwrk = *n * 6 + 1;

	iinspl = 1;
	iindbl = *n + 1;
	iindw = (*n << 1) + 1;
	iindwk = *n * 3 + 1;

/*        Scale matrix to allowable range, if necessary. */
/*        The allowable range is related to the PIVMIN parameter; see the */
/*        comments in SLARRD.  The preference for scaling small values */
/*        up is heuristic; we expect users' matrices not to be close to the */
/*        RMAX threshold. */

	scale = 1.f;
	tnrm = slanst_("M", n, &d__[1], &e[1]);
	if (tnrm > 0.f && tnrm < rmin) {
	    scale = rmin / tnrm;
	} else if (tnrm > rmax) {
	    scale = rmax / tnrm;
	}
	if (scale != 1.f) {
	    sscal_(n, &scale, &d__[1], &c__1);
	    i__1 = *n - 1;
	    sscal_(&i__1, &scale, &e[1], &c__1);
	    tnrm *= scale;
	    if (valeig) {
/*              If eigenvalues in interval have to be found, */
/*              scale (WL, WU] accordingly */
		wl *= scale;
		wu *= scale;
	    }
	}

/*        Compute the desired eigenvalues of the tridiagonal after splitting */
/*        into smaller subblocks if the corresponding off-diagonal elements */
/*        are small */
/*        THRESH is the splitting parameter for SLARRE */
/*        A negative THRESH forces the old splitting criterion based on the */
/*        size of the off-diagonal. A positive THRESH switches to splitting */
/*        which preserves relative accuracy. */

	if (*tryrac) {
/*           Test whether the matrix warrants the more expensive relative approach. */
	    slarrr_(n, &d__[1], &e[1], &iinfo);
	} else {
/*           The user does not care about relative accurately eigenvalues */
	    iinfo = -1;
	}
/*        Set the splitting criterion */
	if (iinfo == 0) {
	    thresh = eps;
	} else {
	    thresh = -eps;
/*           relative accuracy is desired but T does not guarantee it */
	    *tryrac = FALSE_;
	}

	if (*tryrac) {
/*           Copy original diagonal, needed to guarantee relative accuracy */
	    scopy_(n, &d__[1], &c__1, &work[indd], &c__1);
	}
/*        Store the squares of the offdiagonal values of T */
	i__1 = *n - 1;
	for (j = 1; j <= i__1; ++j) {
/* Computing 2nd power */
	    r__1 = e[j];
	    work[inde2 + j - 1] = r__1 * r__1;
/* L5: */
	}
/*        Set the tolerance parameters for bisection */
	if (! wantz) {
/*           SLARRE computes the eigenvalues to full precision. */
	    rtol1 = eps * 4.f;
	    rtol2 = eps * 4.f;
	} else {
/*           SLARRE computes the eigenvalues to less than full precision. */
/*           CLARRV will refine the eigenvalue approximations, and we only */
/*           need less accurate initial bisection in SLARRE. */
/*           Note: these settings do only affect the subset case and SLARRE */
/* Computing MAX */
	    r__1 = sqrt(eps) * .05f, r__2 = eps * 4.f;
	    rtol1 = f2cmax(r__1,r__2);
/* Computing MAX */
	    r__1 = sqrt(eps) * .005f, r__2 = eps * 4.f;
	    rtol2 = f2cmax(r__1,r__2);
	}
	slarre_(range, n, &wl, &wu, &iil, &iiu, &d__[1], &e[1], &work[inde2], 
		&rtol1, &rtol2, &thresh, &nsplit, &iwork[iinspl], m, &w[1], &
		work[inderr], &work[indgp], &iwork[iindbl], &iwork[iindw], &
		work[indgrs], &pivmin, &work[indwrk], &iwork[iindwk], &iinfo);
	if (iinfo != 0) {
	    *info = abs(iinfo) + 10;
	    return;
	}
/*        Note that if RANGE .NE. 'V', SLARRE computes bounds on the desired */
/*        part of the spectrum. All desired eigenvalues are contained in */
/*        (WL,WU] */
	if (wantz) {

/*           Compute the desired eigenvectors corresponding to the computed */
/*           eigenvalues */

	    clarrv_(n, &wl, &wu, &d__[1], &e[1], &pivmin, &iwork[iinspl], m, &
		    c__1, m, &c_b18, &rtol1, &rtol2, &w[1], &work[inderr], &
		    work[indgp], &iwork[iindbl], &iwork[iindw], &work[indgrs],
		     &z__[z_offset], ldz, &isuppz[1], &work[indwrk], &iwork[
		    iindwk], &iinfo);
	    if (iinfo != 0) {
		*info = abs(iinfo) + 20;
		return;
	    }
	} else {
/*           SLARRE computes eigenvalues of the (shifted) root representation */
/*           CLARRV returns the eigenvalues of the unshifted matrix. */
/*           However, if the eigenvectors are not desired by the user, we need */
/*           to apply the corresponding shifts from SLARRE to obtain the */
/*           eigenvalues of the original matrix. */
	    i__1 = *m;
	    for (j = 1; j <= i__1; ++j) {
		itmp = iwork[iindbl + j - 1];
		w[j] += e[iwork[iinspl + itmp - 1]];
/* L20: */
	    }
	}

	if (*tryrac) {
/*           Refine computed eigenvalues so that they are relatively accurate */
/*           with respect to the original matrix T. */
	    ibegin = 1;
	    wbegin = 1;
	    i__1 = iwork[iindbl + *m - 1];
	    for (jblk = 1; jblk <= i__1; ++jblk) {
		iend = iwork[iinspl + jblk - 1];
		in = iend - ibegin + 1;
		wend = wbegin - 1;
/*              check if any eigenvalues have to be refined in this block */
L36:
		if (wend < *m) {
		    if (iwork[iindbl + wend] == jblk) {
			++wend;
			goto L36;
		    }
		}
		if (wend < wbegin) {
		    ibegin = iend + 1;
		    goto L39;
		}
		offset = iwork[iindw + wbegin - 1] - 1;
		ifirst = iwork[iindw + wbegin - 1];
		ilast = iwork[iindw + wend - 1];
		rtol2 = eps * 4.f;
		slarrj_(&in, &work[indd + ibegin - 1], &work[inde2 + ibegin - 
			1], &ifirst, &ilast, &rtol2, &offset, &w[wbegin], &
			work[inderr + wbegin - 1], &work[indwrk], &iwork[
			iindwk], &pivmin, &tnrm, &iinfo);
		ibegin = iend + 1;
		wbegin = wend + 1;
L39:
		;
	    }
	}

/*        If matrix was scaled, then rescale eigenvalues appropriately. */

	if (scale != 1.f) {
	    r__1 = 1.f / scale;
	    sscal_(m, &r__1, &w[1], &c__1);
	}
    }

/*     If eigenvalues are not in increasing order, then sort them, */
/*     possibly along with eigenvectors. */

    if (nsplit > 1 || *n == 2) {
	if (! wantz) {
	    slasrt_("I", m, &w[1], &iinfo);
	    if (iinfo != 0) {
		*info = 3;
		return;
	    }
	} else {
	    i__1 = *m - 1;
	    for (j = 1; j <= i__1; ++j) {
		i__ = 0;
		tmp = w[j];
		i__2 = *m;
		for (jj = j + 1; jj <= i__2; ++jj) {
		    if (w[jj] < tmp) {
			i__ = jj;
			tmp = w[jj];
		    }
/* L50: */
		}
		if (i__ != 0) {
		    w[i__] = w[j];
		    w[j] = tmp;
		    if (wantz) {
			cswap_(n, &z__[i__ * z_dim1 + 1], &c__1, &z__[j * 
				z_dim1 + 1], &c__1);
			itmp = isuppz[(i__ << 1) - 1];
			isuppz[(i__ << 1) - 1] = isuppz[(j << 1) - 1];
			isuppz[(j << 1) - 1] = itmp;
			itmp = isuppz[i__ * 2];
			isuppz[i__ * 2] = isuppz[j * 2];
			isuppz[j * 2] = itmp;
		    }
		}
/* L60: */
	    }
	}
    }


    work[1] = (real) lwmin;
    iwork[1] = liwmin;
    return;

/*     End of CSTEMR */

} /* cstemr_ */

