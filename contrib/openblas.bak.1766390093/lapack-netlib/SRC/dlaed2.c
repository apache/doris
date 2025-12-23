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

static doublereal c_b3 = -1.;
static integer c__1 = 1;

/* > \brief \b DLAED2 used by sstedc. Merges eigenvalues and deflates secular equation. Used when the original
 matrix is tridiagonal. */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download DLAED2 + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dlaed2.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dlaed2.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dlaed2.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE DLAED2( K, N, N1, D, Q, LDQ, INDXQ, RHO, Z, DLAMDA, W, */
/*                          Q2, INDX, INDXC, INDXP, COLTYP, INFO ) */

/*       INTEGER            INFO, K, LDQ, N, N1 */
/*       DOUBLE PRECISION   RHO */
/*       INTEGER            COLTYP( * ), INDX( * ), INDXC( * ), INDXP( * ), */
/*      $                   INDXQ( * ) */
/*       DOUBLE PRECISION   D( * ), DLAMDA( * ), Q( LDQ, * ), Q2( * ), */
/*      $                   W( * ), Z( * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > DLAED2 merges the two sets of eigenvalues together into a single */
/* > sorted set.  Then it tries to deflate the size of the problem. */
/* > There are two ways in which deflation can occur:  when two or more */
/* > eigenvalues are close together or if there is a tiny entry in the */
/* > Z vector.  For each such occurrence the order of the related secular */
/* > equation problem is reduced by one. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[out] K */
/* > \verbatim */
/* >          K is INTEGER */
/* >         The number of non-deflated eigenvalues, and the order of the */
/* >         related secular equation. 0 <= K <=N. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >         The dimension of the symmetric tridiagonal matrix.  N >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] N1 */
/* > \verbatim */
/* >          N1 is INTEGER */
/* >         The location of the last eigenvalue in the leading sub-matrix. */
/* >         f2cmin(1,N) <= N1 <= N/2. */
/* > \endverbatim */
/* > */
/* > \param[in,out] D */
/* > \verbatim */
/* >          D is DOUBLE PRECISION array, dimension (N) */
/* >         On entry, D contains the eigenvalues of the two submatrices to */
/* >         be combined. */
/* >         On exit, D contains the trailing (N-K) updated eigenvalues */
/* >         (those which were deflated) sorted into increasing order. */
/* > \endverbatim */
/* > */
/* > \param[in,out] Q */
/* > \verbatim */
/* >          Q is DOUBLE PRECISION array, dimension (LDQ, N) */
/* >         On entry, Q contains the eigenvectors of two submatrices in */
/* >         the two square blocks with corners at (1,1), (N1,N1) */
/* >         and (N1+1, N1+1), (N,N). */
/* >         On exit, Q contains the trailing (N-K) updated eigenvectors */
/* >         (those which were deflated) in its last N-K columns. */
/* > \endverbatim */
/* > */
/* > \param[in] LDQ */
/* > \verbatim */
/* >          LDQ is INTEGER */
/* >         The leading dimension of the array Q.  LDQ >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[in,out] INDXQ */
/* > \verbatim */
/* >          INDXQ is INTEGER array, dimension (N) */
/* >         The permutation which separately sorts the two sub-problems */
/* >         in D into ascending order.  Note that elements in the second */
/* >         half of this permutation must first have N1 added to their */
/* >         values. Destroyed on exit. */
/* > \endverbatim */
/* > */
/* > \param[in,out] RHO */
/* > \verbatim */
/* >          RHO is DOUBLE PRECISION */
/* >         On entry, the off-diagonal element associated with the rank-1 */
/* >         cut which originally split the two submatrices which are now */
/* >         being recombined. */
/* >         On exit, RHO has been modified to the value required by */
/* >         DLAED3. */
/* > \endverbatim */
/* > */
/* > \param[in] Z */
/* > \verbatim */
/* >          Z is DOUBLE PRECISION array, dimension (N) */
/* >         On entry, Z contains the updating vector (the last */
/* >         row of the first sub-eigenvector matrix and the first row of */
/* >         the second sub-eigenvector matrix). */
/* >         On exit, the contents of Z have been destroyed by the updating */
/* >         process. */
/* > \endverbatim */
/* > */
/* > \param[out] DLAMDA */
/* > \verbatim */
/* >          DLAMDA is DOUBLE PRECISION array, dimension (N) */
/* >         A copy of the first K eigenvalues which will be used by */
/* >         DLAED3 to form the secular equation. */
/* > \endverbatim */
/* > */
/* > \param[out] W */
/* > \verbatim */
/* >          W is DOUBLE PRECISION array, dimension (N) */
/* >         The first k values of the final deflation-altered z-vector */
/* >         which will be passed to DLAED3. */
/* > \endverbatim */
/* > */
/* > \param[out] Q2 */
/* > \verbatim */
/* >          Q2 is DOUBLE PRECISION array, dimension (N1**2+(N-N1)**2) */
/* >         A copy of the first K eigenvectors which will be used by */
/* >         DLAED3 in a matrix multiply (DGEMM) to solve for the new */
/* >         eigenvectors. */
/* > \endverbatim */
/* > */
/* > \param[out] INDX */
/* > \verbatim */
/* >          INDX is INTEGER array, dimension (N) */
/* >         The permutation used to sort the contents of DLAMDA into */
/* >         ascending order. */
/* > \endverbatim */
/* > */
/* > \param[out] INDXC */
/* > \verbatim */
/* >          INDXC is INTEGER array, dimension (N) */
/* >         The permutation used to arrange the columns of the deflated */
/* >         Q matrix into three groups:  the first group contains non-zero */
/* >         elements only at and above N1, the second contains */
/* >         non-zero elements only below N1, and the third is dense. */
/* > \endverbatim */
/* > */
/* > \param[out] INDXP */
/* > \verbatim */
/* >          INDXP is INTEGER array, dimension (N) */
/* >         The permutation used to place deflated values of D at the end */
/* >         of the array.  INDXP(1:K) points to the nondeflated D-values */
/* >         and INDXP(K+1:N) points to the deflated eigenvalues. */
/* > \endverbatim */
/* > */
/* > \param[out] COLTYP */
/* > \verbatim */
/* >          COLTYP is INTEGER array, dimension (N) */
/* >         During execution, a label which will indicate which of the */
/* >         following types a column in the Q2 matrix is: */
/* >         1 : non-zero in the upper half only; */
/* >         2 : dense; */
/* >         3 : non-zero in the lower half only; */
/* >         4 : deflated. */
/* >         On exit, COLTYP(i) is the number of columns of type i, */
/* >         for i=1 to 4 only. */
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

/* > \date December 2016 */

/* > \ingroup auxOTHERcomputational */

/* > \par Contributors: */
/*  ================== */
/* > */
/* > Jeff Rutter, Computer Science Division, University of California */
/* > at Berkeley, USA \n */
/* >  Modified by Francoise Tisseur, University of Tennessee */
/* > */
/*  ===================================================================== */
/* Subroutine */ void dlaed2_(integer *k, integer *n, integer *n1, doublereal *
	d__, doublereal *q, integer *ldq, integer *indxq, doublereal *rho, 
	doublereal *z__, doublereal *dlamda, doublereal *w, doublereal *q2, 
	integer *indx, integer *indxc, integer *indxp, integer *coltyp, 
	integer *info)
{
    /* System generated locals */
    integer q_dim1, q_offset, i__1, i__2;
    doublereal d__1, d__2, d__3, d__4;

    /* Local variables */
    integer imax, jmax;
    extern /* Subroutine */ void drot_(integer *, doublereal *, integer *, 
	    doublereal *, integer *, doublereal *, doublereal *);
    integer ctot[4];
    doublereal c__;
    integer i__, j;
    doublereal s, t;
    extern /* Subroutine */ void dscal_(integer *, doublereal *, doublereal *, 
	    integer *), dcopy_(integer *, doublereal *, integer *, doublereal 
	    *, integer *);
    integer k2, n2;
    extern doublereal dlapy2_(doublereal *, doublereal *);
    integer ct, nj;
    extern doublereal dlamch_(char *);
    integer pj, js;
    extern integer idamax_(integer *, doublereal *, integer *);
    extern /* Subroutine */ void dlamrg_(integer *, integer *, doublereal *, 
	    integer *, integer *, integer *), dlacpy_(char *, integer *, 
	    integer *, doublereal *, integer *, doublereal *, integer *);
    extern int xerbla_(char *, integer *, ftnlen);
    integer iq1, iq2, n1p1;
    doublereal eps, tau, tol;
    integer psm[4];


/*  -- LAPACK computational routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     December 2016 */


/*  ===================================================================== */


/*     Test the input parameters. */

    /* Parameter adjustments */
    --d__;
    q_dim1 = *ldq;
    q_offset = 1 + q_dim1 * 1;
    q -= q_offset;
    --indxq;
    --z__;
    --dlamda;
    --w;
    --q2;
    --indx;
    --indxc;
    --indxp;
    --coltyp;

    /* Function Body */
    *info = 0;

    if (*n < 0) {
	*info = -2;
    } else if (*ldq < f2cmax(1,*n)) {
	*info = -6;
    } else /* if(complicated condition) */ {
/* Computing MIN */
	i__1 = 1, i__2 = *n / 2;
	if (f2cmin(i__1,i__2) > *n1 || *n / 2 < *n1) {
	    *info = -3;
	}
    }
    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("DLAED2", &i__1, (ftnlen)6);
	return;
    }

/*     Quick return if possible */

    if (*n == 0) {
	return;
    }

    n2 = *n - *n1;
    n1p1 = *n1 + 1;

    if (*rho < 0.) {
	dscal_(&n2, &c_b3, &z__[n1p1], &c__1);
    }

/*     Normalize z so that norm(z) = 1.  Since z is the concatenation of */
/*     two normalized vectors, norm2(z) = sqrt(2). */

    t = 1. / sqrt(2.);
    dscal_(n, &t, &z__[1], &c__1);

/*     RHO = ABS( norm(z)**2 * RHO ) */

    *rho = (d__1 = *rho * 2., abs(d__1));

/*     Sort the eigenvalues into increasing order */

    i__1 = *n;
    for (i__ = n1p1; i__ <= i__1; ++i__) {
	indxq[i__] += *n1;
/* L10: */
    }

/*     re-integrate the deflated parts from the last pass */

    i__1 = *n;
    for (i__ = 1; i__ <= i__1; ++i__) {
	dlamda[i__] = d__[indxq[i__]];
/* L20: */
    }
    dlamrg_(n1, &n2, &dlamda[1], &c__1, &c__1, &indxc[1]);
    i__1 = *n;
    for (i__ = 1; i__ <= i__1; ++i__) {
	indx[i__] = indxq[indxc[i__]];
/* L30: */
    }

/*     Calculate the allowable deflation tolerance */

    imax = idamax_(n, &z__[1], &c__1);
    jmax = idamax_(n, &d__[1], &c__1);
    eps = dlamch_("Epsilon");
/* Computing MAX */
    d__3 = (d__1 = d__[jmax], abs(d__1)), d__4 = (d__2 = z__[imax], abs(d__2))
	    ;
    tol = eps * 8. * f2cmax(d__3,d__4);

/*     If the rank-1 modifier is small enough, no more needs to be done */
/*     except to reorganize Q so that its columns correspond with the */
/*     elements in D. */

    if (*rho * (d__1 = z__[imax], abs(d__1)) <= tol) {
	*k = 0;
	iq2 = 1;
	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    i__ = indx[j];
	    dcopy_(n, &q[i__ * q_dim1 + 1], &c__1, &q2[iq2], &c__1);
	    dlamda[j] = d__[i__];
	    iq2 += *n;
/* L40: */
	}
	dlacpy_("A", n, n, &q2[1], n, &q[q_offset], ldq);
	dcopy_(n, &dlamda[1], &c__1, &d__[1], &c__1);
	goto L190;
    }

/*     If there are multiple eigenvalues then the problem deflates.  Here */
/*     the number of equal eigenvalues are found.  As each equal */
/*     eigenvalue is found, an elementary reflector is computed to rotate */
/*     the corresponding eigensubspace so that the corresponding */
/*     components of Z are zero in this new basis. */

    i__1 = *n1;
    for (i__ = 1; i__ <= i__1; ++i__) {
	coltyp[i__] = 1;
/* L50: */
    }
    i__1 = *n;
    for (i__ = n1p1; i__ <= i__1; ++i__) {
	coltyp[i__] = 3;
/* L60: */
    }


    *k = 0;
    k2 = *n + 1;
    i__1 = *n;
    for (j = 1; j <= i__1; ++j) {
	nj = indx[j];
	if (*rho * (d__1 = z__[nj], abs(d__1)) <= tol) {

/*           Deflate due to small z component. */

	    --k2;
	    coltyp[nj] = 4;
	    indxp[k2] = nj;
	    if (j == *n) {
		goto L100;
	    }
	} else {
	    pj = nj;
	    goto L80;
	}
/* L70: */
    }
L80:
    ++j;
    nj = indx[j];
    if (j > *n) {
	goto L100;
    }
    if (*rho * (d__1 = z__[nj], abs(d__1)) <= tol) {

/*        Deflate due to small z component. */

	--k2;
	coltyp[nj] = 4;
	indxp[k2] = nj;
    } else {

/*        Check if eigenvalues are close enough to allow deflation. */

	s = z__[pj];
	c__ = z__[nj];

/*        Find sqrt(a**2+b**2) without overflow or */
/*        destructive underflow. */

	tau = dlapy2_(&c__, &s);
	t = d__[nj] - d__[pj];
	c__ /= tau;
	s = -s / tau;
	if ((d__1 = t * c__ * s, abs(d__1)) <= tol) {

/*           Deflation is possible. */

	    z__[nj] = tau;
	    z__[pj] = 0.;
	    if (coltyp[nj] != coltyp[pj]) {
		coltyp[nj] = 2;
	    }
	    coltyp[pj] = 4;
	    drot_(n, &q[pj * q_dim1 + 1], &c__1, &q[nj * q_dim1 + 1], &c__1, &
		    c__, &s);
/* Computing 2nd power */
	    d__1 = c__;
/* Computing 2nd power */
	    d__2 = s;
	    t = d__[pj] * (d__1 * d__1) + d__[nj] * (d__2 * d__2);
/* Computing 2nd power */
	    d__1 = s;
/* Computing 2nd power */
	    d__2 = c__;
	    d__[nj] = d__[pj] * (d__1 * d__1) + d__[nj] * (d__2 * d__2);
	    d__[pj] = t;
	    --k2;
	    i__ = 1;
L90:
	    if (k2 + i__ <= *n) {
		if (d__[pj] < d__[indxp[k2 + i__]]) {
		    indxp[k2 + i__ - 1] = indxp[k2 + i__];
		    indxp[k2 + i__] = pj;
		    ++i__;
		    goto L90;
		} else {
		    indxp[k2 + i__ - 1] = pj;
		}
	    } else {
		indxp[k2 + i__ - 1] = pj;
	    }
	    pj = nj;
	} else {
	    ++(*k);
	    dlamda[*k] = d__[pj];
	    w[*k] = z__[pj];
	    indxp[*k] = pj;
	    pj = nj;
	}
    }
    goto L80;
L100:

/*     Record the last eigenvalue. */

    ++(*k);
    dlamda[*k] = d__[pj];
    w[*k] = z__[pj];
    indxp[*k] = pj;

/*     Count up the total number of the various types of columns, then */
/*     form a permutation which positions the four column types into */
/*     four uniform groups (although one or more of these groups may be */
/*     empty). */

    for (j = 1; j <= 4; ++j) {
	ctot[j - 1] = 0;
/* L110: */
    }
    i__1 = *n;
    for (j = 1; j <= i__1; ++j) {
	ct = coltyp[j];
	++ctot[ct - 1];
/* L120: */
    }

/*     PSM(*) = Position in SubMatrix (of types 1 through 4) */

    psm[0] = 1;
    psm[1] = ctot[0] + 1;
    psm[2] = psm[1] + ctot[1];
    psm[3] = psm[2] + ctot[2];
    *k = *n - ctot[3];

/*     Fill out the INDXC array so that the permutation which it induces */
/*     will place all type-1 columns first, all type-2 columns next, */
/*     then all type-3's, and finally all type-4's. */

    i__1 = *n;
    for (j = 1; j <= i__1; ++j) {
	js = indxp[j];
	ct = coltyp[js];
	indx[psm[ct - 1]] = js;
	indxc[psm[ct - 1]] = j;
	++psm[ct - 1];
/* L130: */
    }

/*     Sort the eigenvalues and corresponding eigenvectors into DLAMDA */
/*     and Q2 respectively.  The eigenvalues/vectors which were not */
/*     deflated go into the first K slots of DLAMDA and Q2 respectively, */
/*     while those which were deflated go into the last N - K slots. */

    i__ = 1;
    iq1 = 1;
    iq2 = (ctot[0] + ctot[1]) * *n1 + 1;
    i__1 = ctot[0];
    for (j = 1; j <= i__1; ++j) {
	js = indx[i__];
	dcopy_(n1, &q[js * q_dim1 + 1], &c__1, &q2[iq1], &c__1);
	z__[i__] = d__[js];
	++i__;
	iq1 += *n1;
/* L140: */
    }

    i__1 = ctot[1];
    for (j = 1; j <= i__1; ++j) {
	js = indx[i__];
	dcopy_(n1, &q[js * q_dim1 + 1], &c__1, &q2[iq1], &c__1);
	dcopy_(&n2, &q[*n1 + 1 + js * q_dim1], &c__1, &q2[iq2], &c__1);
	z__[i__] = d__[js];
	++i__;
	iq1 += *n1;
	iq2 += n2;
/* L150: */
    }

    i__1 = ctot[2];
    for (j = 1; j <= i__1; ++j) {
	js = indx[i__];
	dcopy_(&n2, &q[*n1 + 1 + js * q_dim1], &c__1, &q2[iq2], &c__1);
	z__[i__] = d__[js];
	++i__;
	iq2 += n2;
/* L160: */
    }

    iq1 = iq2;
    i__1 = ctot[3];
    for (j = 1; j <= i__1; ++j) {
	js = indx[i__];
	dcopy_(n, &q[js * q_dim1 + 1], &c__1, &q2[iq2], &c__1);
	iq2 += *n;
	z__[i__] = d__[js];
	++i__;
/* L170: */
    }

/*     The deflated eigenvalues and their corresponding vectors go back */
/*     into the last N - K slots of D and Q respectively. */

    if (*k < *n) {
	dlacpy_("A", n, &ctot[3], &q2[iq1], n, &q[(*k + 1) * q_dim1 + 1], ldq);
	i__1 = *n - *k;
	dcopy_(&i__1, &z__[*k + 1], &c__1, &d__[*k + 1], &c__1);
    }

/*     Copy CTOT into COLTYP for referencing in DLAED3. */

    for (j = 1; j <= 4; ++j) {
	coltyp[j] = ctot[j - 1];
/* L180: */
    }

L190:
    return;

/*     End of DLAED2 */

} /* dlaed2_ */

