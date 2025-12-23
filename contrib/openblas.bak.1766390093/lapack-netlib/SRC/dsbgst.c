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

static doublereal c_b8 = 0.;
static doublereal c_b9 = 1.;
static integer c__1 = 1;
static doublereal c_b20 = -1.;

/* > \brief \b DSBGST */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download DSBGST + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dsbgst.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dsbgst.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dsbgst.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE DSBGST( VECT, UPLO, N, KA, KB, AB, LDAB, BB, LDBB, X, */
/*                          LDX, WORK, INFO ) */

/*       CHARACTER          UPLO, VECT */
/*       INTEGER            INFO, KA, KB, LDAB, LDBB, LDX, N */
/*       DOUBLE PRECISION   AB( LDAB, * ), BB( LDBB, * ), WORK( * ), */
/*      $                   X( LDX, * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > DSBGST reduces a real symmetric-definite banded generalized */
/* > eigenproblem  A*x = lambda*B*x  to standard form  C*y = lambda*y, */
/* > such that C has the same bandwidth as A. */
/* > */
/* > B must have been previously factorized as S**T*S by DPBSTF, using a */
/* > split Cholesky factorization. A is overwritten by C = X**T*A*X, where */
/* > X = S**(-1)*Q and Q is an orthogonal matrix chosen to preserve the */
/* > bandwidth of A. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] VECT */
/* > \verbatim */
/* >          VECT is CHARACTER*1 */
/* >          = 'N':  do not form the transformation matrix X; */
/* >          = 'V':  form X. */
/* > \endverbatim */
/* > */
/* > \param[in] UPLO */
/* > \verbatim */
/* >          UPLO is CHARACTER*1 */
/* >          = 'U':  Upper triangle of A is stored; */
/* >          = 'L':  Lower triangle of A is stored. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          The order of the matrices A and B.  N >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] KA */
/* > \verbatim */
/* >          KA is INTEGER */
/* >          The number of superdiagonals of the matrix A if UPLO = 'U', */
/* >          or the number of subdiagonals if UPLO = 'L'.  KA >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] KB */
/* > \verbatim */
/* >          KB is INTEGER */
/* >          The number of superdiagonals of the matrix B if UPLO = 'U', */
/* >          or the number of subdiagonals if UPLO = 'L'.  KA >= KB >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in,out] AB */
/* > \verbatim */
/* >          AB is DOUBLE PRECISION array, dimension (LDAB,N) */
/* >          On entry, the upper or lower triangle of the symmetric band */
/* >          matrix A, stored in the first ka+1 rows of the array.  The */
/* >          j-th column of A is stored in the j-th column of the array AB */
/* >          as follows: */
/* >          if UPLO = 'U', AB(ka+1+i-j,j) = A(i,j) for f2cmax(1,j-ka)<=i<=j; */
/* >          if UPLO = 'L', AB(1+i-j,j)    = A(i,j) for j<=i<=f2cmin(n,j+ka). */
/* > */
/* >          On exit, the transformed matrix X**T*A*X, stored in the same */
/* >          format as A. */
/* > \endverbatim */
/* > */
/* > \param[in] LDAB */
/* > \verbatim */
/* >          LDAB is INTEGER */
/* >          The leading dimension of the array AB.  LDAB >= KA+1. */
/* > \endverbatim */
/* > */
/* > \param[in] BB */
/* > \verbatim */
/* >          BB is DOUBLE PRECISION array, dimension (LDBB,N) */
/* >          The banded factor S from the split Cholesky factorization of */
/* >          B, as returned by DPBSTF, stored in the first KB+1 rows of */
/* >          the array. */
/* > \endverbatim */
/* > */
/* > \param[in] LDBB */
/* > \verbatim */
/* >          LDBB is INTEGER */
/* >          The leading dimension of the array BB.  LDBB >= KB+1. */
/* > \endverbatim */
/* > */
/* > \param[out] X */
/* > \verbatim */
/* >          X is DOUBLE PRECISION array, dimension (LDX,N) */
/* >          If VECT = 'V', the n-by-n matrix X. */
/* >          If VECT = 'N', the array X is not referenced. */
/* > \endverbatim */
/* > */
/* > \param[in] LDX */
/* > \verbatim */
/* >          LDX is INTEGER */
/* >          The leading dimension of the array X. */
/* >          LDX >= f2cmax(1,N) if VECT = 'V'; LDX >= 1 otherwise. */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is DOUBLE PRECISION array, dimension (2*N) */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >          = 0:  successful exit */
/* >          < 0:  if INFO = -i, the i-th argument had an illegal value. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date December 2016 */

/* > \ingroup doubleOTHERcomputational */

/*  ===================================================================== */
/* Subroutine */ void dsbgst_(char *vect, char *uplo, integer *n, integer *ka, 
	integer *kb, doublereal *ab, integer *ldab, doublereal *bb, integer *
	ldbb, doublereal *x, integer *ldx, doublereal *work, integer *info)
{
    /* System generated locals */
    integer ab_dim1, ab_offset, bb_dim1, bb_offset, x_dim1, x_offset, i__1, 
	    i__2, i__3, i__4;
    doublereal d__1;

    /* Local variables */
    integer inca;
    extern /* Subroutine */ void dger_(integer *, integer *, doublereal *, 
	    doublereal *, integer *, doublereal *, integer *, doublereal *, 
	    integer *), drot_(integer *, doublereal *, integer *, doublereal *
	    , integer *, doublereal *, doublereal *);
    integer i__, j, k, l, m;
    doublereal t;
    extern /* Subroutine */ void dscal_(integer *, doublereal *, doublereal *, 
	    integer *);
    extern logical lsame_(char *, char *);
    integer i0, i1;
    logical upper;
    integer i2, j1, j2;
    logical wantx;
    extern /* Subroutine */ void dlar2v_(integer *, doublereal *, doublereal *,
	     doublereal *, integer *, doublereal *, doublereal *, integer *);
    doublereal ra;
    integer nr, nx;
    extern /* Subroutine */ void dlaset_(char *, integer *, integer *, 
	    doublereal *, doublereal *, doublereal *, integer *), 
	    dlartg_(doublereal *, doublereal *, doublereal *, doublereal *, 
	    doublereal *);
    extern int xerbla_(char *, integer *, ftnlen);
    extern void dlargv_(
	    integer *, doublereal *, integer *, doublereal *, integer *, 
	    doublereal *, integer *);
    logical update;
    extern /* Subroutine */ void dlartv_(integer *, doublereal *, integer *, 
	    doublereal *, integer *, doublereal *, doublereal *, integer *);
    integer ka1, kb1;
    doublereal ra1;
    integer j1t, j2t;
    doublereal bii;
    integer kbt, nrt;


/*  -- LAPACK computational routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     December 2016 */


/*  ===================================================================== */


/*     Test the input parameters */

    /* Parameter adjustments */
    ab_dim1 = *ldab;
    ab_offset = 1 + ab_dim1 * 1;
    ab -= ab_offset;
    bb_dim1 = *ldbb;
    bb_offset = 1 + bb_dim1 * 1;
    bb -= bb_offset;
    x_dim1 = *ldx;
    x_offset = 1 + x_dim1 * 1;
    x -= x_offset;
    --work;

    /* Function Body */
    wantx = lsame_(vect, "V");
    upper = lsame_(uplo, "U");
    ka1 = *ka + 1;
    kb1 = *kb + 1;
    *info = 0;
    if (! wantx && ! lsame_(vect, "N")) {
	*info = -1;
    } else if (! upper && ! lsame_(uplo, "L")) {
	*info = -2;
    } else if (*n < 0) {
	*info = -3;
    } else if (*ka < 0) {
	*info = -4;
    } else if (*kb < 0 || *kb > *ka) {
	*info = -5;
    } else if (*ldab < *ka + 1) {
	*info = -7;
    } else if (*ldbb < *kb + 1) {
	*info = -9;
    } else if (*ldx < 1 || wantx && *ldx < f2cmax(1,*n)) {
	*info = -11;
    }
    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("DSBGST", &i__1, (ftnlen)6);
	return;
    }

/*     Quick return if possible */

    if (*n == 0) {
	return;
    }

    inca = *ldab * ka1;

/*     Initialize X to the unit matrix, if needed */

    if (wantx) {
	dlaset_("Full", n, n, &c_b8, &c_b9, &x[x_offset], ldx);
    }

/*     Set M to the splitting point m. It must be the same value as is */
/*     used in DPBSTF. The chosen value allows the arrays WORK and RWORK */
/*     to be of dimension (N). */

    m = (*n + *kb) / 2;

/*     The routine works in two phases, corresponding to the two halves */
/*     of the split Cholesky factorization of B as S**T*S where */

/*     S = ( U    ) */
/*         ( M  L ) */

/*     with U upper triangular of order m, and L lower triangular of */
/*     order n-m. S has the same bandwidth as B. */

/*     S is treated as a product of elementary matrices: */

/*     S = S(m)*S(m-1)*...*S(2)*S(1)*S(m+1)*S(m+2)*...*S(n-1)*S(n) */

/*     where S(i) is determined by the i-th row of S. */

/*     In phase 1, the index i takes the values n, n-1, ... , m+1; */
/*     in phase 2, it takes the values 1, 2, ... , m. */

/*     For each value of i, the current matrix A is updated by forming */
/*     inv(S(i))**T*A*inv(S(i)). This creates a triangular bulge outside */
/*     the band of A. The bulge is then pushed down toward the bottom of */
/*     A in phase 1, and up toward the top of A in phase 2, by applying */
/*     plane rotations. */

/*     There are kb*(kb+1)/2 elements in the bulge, but at most 2*kb-1 */
/*     of them are linearly independent, so annihilating a bulge requires */
/*     only 2*kb-1 plane rotations. The rotations are divided into a 1st */
/*     set of kb-1 rotations, and a 2nd set of kb rotations. */

/*     Wherever possible, rotations are generated and applied in vector */
/*     operations of length NR between the indices J1 and J2 (sometimes */
/*     replaced by modified values NRT, J1T or J2T). */

/*     The cosines and sines of the rotations are stored in the array */
/*     WORK. The cosines of the 1st set of rotations are stored in */
/*     elements n+2:n+m-kb-1 and the sines of the 1st set in elements */
/*     2:m-kb-1; the cosines of the 2nd set are stored in elements */
/*     n+m-kb+1:2*n and the sines of the second set in elements m-kb+1:n. */

/*     The bulges are not formed explicitly; nonzero elements outside the */
/*     band are created only when they are required for generating new */
/*     rotations; they are stored in the array WORK, in positions where */
/*     they are later overwritten by the sines of the rotations which */
/*     annihilate them. */

/*     **************************** Phase 1 ***************************** */

/*     The logical structure of this phase is: */

/*     UPDATE = .TRUE. */
/*     DO I = N, M + 1, -1 */
/*        use S(i) to update A and create a new bulge */
/*        apply rotations to push all bulges KA positions downward */
/*     END DO */
/*     UPDATE = .FALSE. */
/*     DO I = M + KA + 1, N - 1 */
/*        apply rotations to push all bulges KA positions downward */
/*     END DO */

/*     To avoid duplicating code, the two loops are merged. */

    update = TRUE_;
    i__ = *n + 1;
L10:
    if (update) {
	--i__;
/* Computing MIN */
	i__1 = *kb, i__2 = i__ - 1;
	kbt = f2cmin(i__1,i__2);
	i0 = i__ - 1;
/* Computing MIN */
	i__1 = *n, i__2 = i__ + *ka;
	i1 = f2cmin(i__1,i__2);
	i2 = i__ - kbt + ka1;
	if (i__ < m + 1) {
	    update = FALSE_;
	    ++i__;
	    i0 = m;
	    if (*ka == 0) {
		goto L480;
	    }
	    goto L10;
	}
    } else {
	i__ += *ka;
	if (i__ > *n - 1) {
	    goto L480;
	}
    }

    if (upper) {

/*        Transform A, working with the upper triangle */

	if (update) {

/*           Form  inv(S(i))**T * A * inv(S(i)) */

	    bii = bb[kb1 + i__ * bb_dim1];
	    i__1 = i1;
	    for (j = i__; j <= i__1; ++j) {
		ab[i__ - j + ka1 + j * ab_dim1] /= bii;
/* L20: */
	    }
/* Computing MAX */
	    i__1 = 1, i__2 = i__ - *ka;
	    i__3 = i__;
	    for (j = f2cmax(i__1,i__2); j <= i__3; ++j) {
		ab[j - i__ + ka1 + i__ * ab_dim1] /= bii;
/* L30: */
	    }
	    i__3 = i__ - 1;
	    for (k = i__ - kbt; k <= i__3; ++k) {
		i__1 = k;
		for (j = i__ - kbt; j <= i__1; ++j) {
		    ab[j - k + ka1 + k * ab_dim1] = ab[j - k + ka1 + k * 
			    ab_dim1] - bb[j - i__ + kb1 + i__ * bb_dim1] * ab[
			    k - i__ + ka1 + i__ * ab_dim1] - bb[k - i__ + kb1 
			    + i__ * bb_dim1] * ab[j - i__ + ka1 + i__ * 
			    ab_dim1] + ab[ka1 + i__ * ab_dim1] * bb[j - i__ + 
			    kb1 + i__ * bb_dim1] * bb[k - i__ + kb1 + i__ * 
			    bb_dim1];
/* L40: */
		}
/* Computing MAX */
		i__1 = 1, i__2 = i__ - *ka;
		i__4 = i__ - kbt - 1;
		for (j = f2cmax(i__1,i__2); j <= i__4; ++j) {
		    ab[j - k + ka1 + k * ab_dim1] -= bb[k - i__ + kb1 + i__ * 
			    bb_dim1] * ab[j - i__ + ka1 + i__ * ab_dim1];
/* L50: */
		}
/* L60: */
	    }
	    i__3 = i1;
	    for (j = i__; j <= i__3; ++j) {
/* Computing MAX */
		i__4 = j - *ka, i__1 = i__ - kbt;
		i__2 = i__ - 1;
		for (k = f2cmax(i__4,i__1); k <= i__2; ++k) {
		    ab[k - j + ka1 + j * ab_dim1] -= bb[k - i__ + kb1 + i__ * 
			    bb_dim1] * ab[i__ - j + ka1 + j * ab_dim1];
/* L70: */
		}
/* L80: */
	    }

	    if (wantx) {

/*              post-multiply X by inv(S(i)) */

		i__3 = *n - m;
		d__1 = 1. / bii;
		dscal_(&i__3, &d__1, &x[m + 1 + i__ * x_dim1], &c__1);
		if (kbt > 0) {
		    i__3 = *n - m;
		    dger_(&i__3, &kbt, &c_b20, &x[m + 1 + i__ * x_dim1], &
			    c__1, &bb[kb1 - kbt + i__ * bb_dim1], &c__1, &x[m 
			    + 1 + (i__ - kbt) * x_dim1], ldx);
		}
	    }

/*           store a(i,i1) in RA1 for use in next loop over K */

	    ra1 = ab[i__ - i1 + ka1 + i1 * ab_dim1];
	}

/*        Generate and apply vectors of rotations to chase all the */
/*        existing bulges KA positions down toward the bottom of the */
/*        band */

	i__3 = *kb - 1;
	for (k = 1; k <= i__3; ++k) {
	    if (update) {

/*              Determine the rotations which would annihilate the bulge */
/*              which has in theory just been created */

		if (i__ - k + *ka < *n && i__ - k > 1) {

/*                 generate rotation to annihilate a(i,i-k+ka+1) */

		    dlartg_(&ab[k + 1 + (i__ - k + *ka) * ab_dim1], &ra1, &
			    work[*n + i__ - k + *ka - m], &work[i__ - k + *ka 
			    - m], &ra);

/*                 create nonzero element a(i-k,i-k+ka+1) outside the */
/*                 band and store it in WORK(i-k) */

		    t = -bb[kb1 - k + i__ * bb_dim1] * ra1;
		    work[i__ - k] = work[*n + i__ - k + *ka - m] * t - work[
			    i__ - k + *ka - m] * ab[(i__ - k + *ka) * ab_dim1 
			    + 1];
		    ab[(i__ - k + *ka) * ab_dim1 + 1] = work[i__ - k + *ka - 
			    m] * t + work[*n + i__ - k + *ka - m] * ab[(i__ - 
			    k + *ka) * ab_dim1 + 1];
		    ra1 = ra;
		}
	    }
/* Computing MAX */
	    i__2 = 1, i__4 = k - i0 + 2;
	    j2 = i__ - k - 1 + f2cmax(i__2,i__4) * ka1;
	    nr = (*n - j2 + *ka) / ka1;
	    j1 = j2 + (nr - 1) * ka1;
	    if (update) {
/* Computing MAX */
		i__2 = j2, i__4 = i__ + (*ka << 1) - k + 1;
		j2t = f2cmax(i__2,i__4);
	    } else {
		j2t = j2;
	    }
	    nrt = (*n - j2t + *ka) / ka1;
	    i__2 = j1;
	    i__4 = ka1;
	    for (j = j2t; i__4 < 0 ? j >= i__2 : j <= i__2; j += i__4) {

/*              create nonzero element a(j-ka,j+1) outside the band */
/*              and store it in WORK(j-m) */

		work[j - m] *= ab[(j + 1) * ab_dim1 + 1];
		ab[(j + 1) * ab_dim1 + 1] = work[*n + j - m] * ab[(j + 1) * 
			ab_dim1 + 1];
/* L90: */
	    }

/*           generate rotations in 1st set to annihilate elements which */
/*           have been created outside the band */

	    if (nrt > 0) {
		dlargv_(&nrt, &ab[j2t * ab_dim1 + 1], &inca, &work[j2t - m], &
			ka1, &work[*n + j2t - m], &ka1);
	    }
	    if (nr > 0) {

/*              apply rotations in 1st set from the right */

		i__4 = *ka - 1;
		for (l = 1; l <= i__4; ++l) {
		    dlartv_(&nr, &ab[ka1 - l + j2 * ab_dim1], &inca, &ab[*ka 
			    - l + (j2 + 1) * ab_dim1], &inca, &work[*n + j2 - 
			    m], &work[j2 - m], &ka1);
/* L100: */
		}

/*              apply rotations in 1st set from both sides to diagonal */
/*              blocks */

		dlar2v_(&nr, &ab[ka1 + j2 * ab_dim1], &ab[ka1 + (j2 + 1) * 
			ab_dim1], &ab[*ka + (j2 + 1) * ab_dim1], &inca, &work[
			*n + j2 - m], &work[j2 - m], &ka1);

	    }

/*           start applying rotations in 1st set from the left */

	    i__4 = *kb - k + 1;
	    for (l = *ka - 1; l >= i__4; --l) {
		nrt = (*n - j2 + l) / ka1;
		if (nrt > 0) {
		    dlartv_(&nrt, &ab[l + (j2 + ka1 - l) * ab_dim1], &inca, &
			    ab[l + 1 + (j2 + ka1 - l) * ab_dim1], &inca, &
			    work[*n + j2 - m], &work[j2 - m], &ka1);
		}
/* L110: */
	    }

	    if (wantx) {

/*              post-multiply X by product of rotations in 1st set */

		i__4 = j1;
		i__2 = ka1;
		for (j = j2; i__2 < 0 ? j >= i__4 : j <= i__4; j += i__2) {
		    i__1 = *n - m;
		    drot_(&i__1, &x[m + 1 + j * x_dim1], &c__1, &x[m + 1 + (j 
			    + 1) * x_dim1], &c__1, &work[*n + j - m], &work[j 
			    - m]);
/* L120: */
		}
	    }
/* L130: */
	}

	if (update) {
	    if (i2 <= *n && kbt > 0) {

/*              create nonzero element a(i-kbt,i-kbt+ka+1) outside the */
/*              band and store it in WORK(i-kbt) */

		work[i__ - kbt] = -bb[kb1 - kbt + i__ * bb_dim1] * ra1;
	    }
	}

	for (k = *kb; k >= 1; --k) {
	    if (update) {
/* Computing MAX */
		i__3 = 2, i__2 = k - i0 + 1;
		j2 = i__ - k - 1 + f2cmax(i__3,i__2) * ka1;
	    } else {
/* Computing MAX */
		i__3 = 1, i__2 = k - i0 + 1;
		j2 = i__ - k - 1 + f2cmax(i__3,i__2) * ka1;
	    }

/*           finish applying rotations in 2nd set from the left */

	    for (l = *kb - k; l >= 1; --l) {
		nrt = (*n - j2 + *ka + l) / ka1;
		if (nrt > 0) {
		    dlartv_(&nrt, &ab[l + (j2 - l + 1) * ab_dim1], &inca, &ab[
			    l + 1 + (j2 - l + 1) * ab_dim1], &inca, &work[*n 
			    + j2 - *ka], &work[j2 - *ka], &ka1);
		}
/* L140: */
	    }
	    nr = (*n - j2 + *ka) / ka1;
	    j1 = j2 + (nr - 1) * ka1;
	    i__3 = j2;
	    i__2 = -ka1;
	    for (j = j1; i__2 < 0 ? j >= i__3 : j <= i__3; j += i__2) {
		work[j] = work[j - *ka];
		work[*n + j] = work[*n + j - *ka];
/* L150: */
	    }
	    i__2 = j1;
	    i__3 = ka1;
	    for (j = j2; i__3 < 0 ? j >= i__2 : j <= i__2; j += i__3) {

/*              create nonzero element a(j-ka,j+1) outside the band */
/*              and store it in WORK(j) */

		work[j] *= ab[(j + 1) * ab_dim1 + 1];
		ab[(j + 1) * ab_dim1 + 1] = work[*n + j] * ab[(j + 1) * 
			ab_dim1 + 1];
/* L160: */
	    }
	    if (update) {
		if (i__ - k < *n - *ka && k <= kbt) {
		    work[i__ - k + *ka] = work[i__ - k];
		}
	    }
/* L170: */
	}

	for (k = *kb; k >= 1; --k) {
/* Computing MAX */
	    i__3 = 1, i__2 = k - i0 + 1;
	    j2 = i__ - k - 1 + f2cmax(i__3,i__2) * ka1;
	    nr = (*n - j2 + *ka) / ka1;
	    j1 = j2 + (nr - 1) * ka1;
	    if (nr > 0) {

/*              generate rotations in 2nd set to annihilate elements */
/*              which have been created outside the band */

		dlargv_(&nr, &ab[j2 * ab_dim1 + 1], &inca, &work[j2], &ka1, &
			work[*n + j2], &ka1);

/*              apply rotations in 2nd set from the right */

		i__3 = *ka - 1;
		for (l = 1; l <= i__3; ++l) {
		    dlartv_(&nr, &ab[ka1 - l + j2 * ab_dim1], &inca, &ab[*ka 
			    - l + (j2 + 1) * ab_dim1], &inca, &work[*n + j2], 
			    &work[j2], &ka1);
/* L180: */
		}

/*              apply rotations in 2nd set from both sides to diagonal */
/*              blocks */

		dlar2v_(&nr, &ab[ka1 + j2 * ab_dim1], &ab[ka1 + (j2 + 1) * 
			ab_dim1], &ab[*ka + (j2 + 1) * ab_dim1], &inca, &work[
			*n + j2], &work[j2], &ka1);

	    }

/*           start applying rotations in 2nd set from the left */

	    i__3 = *kb - k + 1;
	    for (l = *ka - 1; l >= i__3; --l) {
		nrt = (*n - j2 + l) / ka1;
		if (nrt > 0) {
		    dlartv_(&nrt, &ab[l + (j2 + ka1 - l) * ab_dim1], &inca, &
			    ab[l + 1 + (j2 + ka1 - l) * ab_dim1], &inca, &
			    work[*n + j2], &work[j2], &ka1);
		}
/* L190: */
	    }

	    if (wantx) {

/*              post-multiply X by product of rotations in 2nd set */

		i__3 = j1;
		i__2 = ka1;
		for (j = j2; i__2 < 0 ? j >= i__3 : j <= i__3; j += i__2) {
		    i__4 = *n - m;
		    drot_(&i__4, &x[m + 1 + j * x_dim1], &c__1, &x[m + 1 + (j 
			    + 1) * x_dim1], &c__1, &work[*n + j], &work[j]);
/* L200: */
		}
	    }
/* L210: */
	}

	i__2 = *kb - 1;
	for (k = 1; k <= i__2; ++k) {
/* Computing MAX */
	    i__3 = 1, i__4 = k - i0 + 2;
	    j2 = i__ - k - 1 + f2cmax(i__3,i__4) * ka1;

/*           finish applying rotations in 1st set from the left */

	    for (l = *kb - k; l >= 1; --l) {
		nrt = (*n - j2 + l) / ka1;
		if (nrt > 0) {
		    dlartv_(&nrt, &ab[l + (j2 + ka1 - l) * ab_dim1], &inca, &
			    ab[l + 1 + (j2 + ka1 - l) * ab_dim1], &inca, &
			    work[*n + j2 - m], &work[j2 - m], &ka1);
		}
/* L220: */
	    }
/* L230: */
	}

	if (*kb > 1) {
	    i__2 = i__ - *kb + (*ka << 1) + 1;
	    for (j = *n - 1; j >= i__2; --j) {
		work[*n + j - m] = work[*n + j - *ka - m];
		work[j - m] = work[j - *ka - m];
/* L240: */
	    }
	}

    } else {

/*        Transform A, working with the lower triangle */

	if (update) {

/*           Form  inv(S(i))**T * A * inv(S(i)) */

	    bii = bb[i__ * bb_dim1 + 1];
	    i__2 = i1;
	    for (j = i__; j <= i__2; ++j) {
		ab[j - i__ + 1 + i__ * ab_dim1] /= bii;
/* L250: */
	    }
/* Computing MAX */
	    i__2 = 1, i__3 = i__ - *ka;
	    i__4 = i__;
	    for (j = f2cmax(i__2,i__3); j <= i__4; ++j) {
		ab[i__ - j + 1 + j * ab_dim1] /= bii;
/* L260: */
	    }
	    i__4 = i__ - 1;
	    for (k = i__ - kbt; k <= i__4; ++k) {
		i__2 = k;
		for (j = i__ - kbt; j <= i__2; ++j) {
		    ab[k - j + 1 + j * ab_dim1] = ab[k - j + 1 + j * ab_dim1] 
			    - bb[i__ - j + 1 + j * bb_dim1] * ab[i__ - k + 1 
			    + k * ab_dim1] - bb[i__ - k + 1 + k * bb_dim1] * 
			    ab[i__ - j + 1 + j * ab_dim1] + ab[i__ * ab_dim1 
			    + 1] * bb[i__ - j + 1 + j * bb_dim1] * bb[i__ - k 
			    + 1 + k * bb_dim1];
/* L270: */
		}
/* Computing MAX */
		i__2 = 1, i__3 = i__ - *ka;
		i__1 = i__ - kbt - 1;
		for (j = f2cmax(i__2,i__3); j <= i__1; ++j) {
		    ab[k - j + 1 + j * ab_dim1] -= bb[i__ - k + 1 + k * 
			    bb_dim1] * ab[i__ - j + 1 + j * ab_dim1];
/* L280: */
		}
/* L290: */
	    }
	    i__4 = i1;
	    for (j = i__; j <= i__4; ++j) {
/* Computing MAX */
		i__1 = j - *ka, i__2 = i__ - kbt;
		i__3 = i__ - 1;
		for (k = f2cmax(i__1,i__2); k <= i__3; ++k) {
		    ab[j - k + 1 + k * ab_dim1] -= bb[i__ - k + 1 + k * 
			    bb_dim1] * ab[j - i__ + 1 + i__ * ab_dim1];
/* L300: */
		}
/* L310: */
	    }

	    if (wantx) {

/*              post-multiply X by inv(S(i)) */

		i__4 = *n - m;
		d__1 = 1. / bii;
		dscal_(&i__4, &d__1, &x[m + 1 + i__ * x_dim1], &c__1);
		if (kbt > 0) {
		    i__4 = *n - m;
		    i__3 = *ldbb - 1;
		    dger_(&i__4, &kbt, &c_b20, &x[m + 1 + i__ * x_dim1], &
			    c__1, &bb[kbt + 1 + (i__ - kbt) * bb_dim1], &i__3,
			     &x[m + 1 + (i__ - kbt) * x_dim1], ldx);
		}
	    }

/*           store a(i1,i) in RA1 for use in next loop over K */

	    ra1 = ab[i1 - i__ + 1 + i__ * ab_dim1];
	}

/*        Generate and apply vectors of rotations to chase all the */
/*        existing bulges KA positions down toward the bottom of the */
/*        band */

	i__4 = *kb - 1;
	for (k = 1; k <= i__4; ++k) {
	    if (update) {

/*              Determine the rotations which would annihilate the bulge */
/*              which has in theory just been created */

		if (i__ - k + *ka < *n && i__ - k > 1) {

/*                 generate rotation to annihilate a(i-k+ka+1,i) */

		    dlartg_(&ab[ka1 - k + i__ * ab_dim1], &ra1, &work[*n + 
			    i__ - k + *ka - m], &work[i__ - k + *ka - m], &ra)
			    ;

/*                 create nonzero element a(i-k+ka+1,i-k) outside the */
/*                 band and store it in WORK(i-k) */

		    t = -bb[k + 1 + (i__ - k) * bb_dim1] * ra1;
		    work[i__ - k] = work[*n + i__ - k + *ka - m] * t - work[
			    i__ - k + *ka - m] * ab[ka1 + (i__ - k) * ab_dim1]
			    ;
		    ab[ka1 + (i__ - k) * ab_dim1] = work[i__ - k + *ka - m] * 
			    t + work[*n + i__ - k + *ka - m] * ab[ka1 + (i__ 
			    - k) * ab_dim1];
		    ra1 = ra;
		}
	    }
/* Computing MAX */
	    i__3 = 1, i__1 = k - i0 + 2;
	    j2 = i__ - k - 1 + f2cmax(i__3,i__1) * ka1;
	    nr = (*n - j2 + *ka) / ka1;
	    j1 = j2 + (nr - 1) * ka1;
	    if (update) {
/* Computing MAX */
		i__3 = j2, i__1 = i__ + (*ka << 1) - k + 1;
		j2t = f2cmax(i__3,i__1);
	    } else {
		j2t = j2;
	    }
	    nrt = (*n - j2t + *ka) / ka1;
	    i__3 = j1;
	    i__1 = ka1;
	    for (j = j2t; i__1 < 0 ? j >= i__3 : j <= i__3; j += i__1) {

/*              create nonzero element a(j+1,j-ka) outside the band */
/*              and store it in WORK(j-m) */

		work[j - m] *= ab[ka1 + (j - *ka + 1) * ab_dim1];
		ab[ka1 + (j - *ka + 1) * ab_dim1] = work[*n + j - m] * ab[ka1 
			+ (j - *ka + 1) * ab_dim1];
/* L320: */
	    }

/*           generate rotations in 1st set to annihilate elements which */
/*           have been created outside the band */

	    if (nrt > 0) {
		dlargv_(&nrt, &ab[ka1 + (j2t - *ka) * ab_dim1], &inca, &work[
			j2t - m], &ka1, &work[*n + j2t - m], &ka1);
	    }
	    if (nr > 0) {

/*              apply rotations in 1st set from the left */

		i__1 = *ka - 1;
		for (l = 1; l <= i__1; ++l) {
		    dlartv_(&nr, &ab[l + 1 + (j2 - l) * ab_dim1], &inca, &ab[
			    l + 2 + (j2 - l) * ab_dim1], &inca, &work[*n + j2 
			    - m], &work[j2 - m], &ka1);
/* L330: */
		}

/*              apply rotations in 1st set from both sides to diagonal */
/*              blocks */

		dlar2v_(&nr, &ab[j2 * ab_dim1 + 1], &ab[(j2 + 1) * ab_dim1 + 
			1], &ab[j2 * ab_dim1 + 2], &inca, &work[*n + j2 - m], 
			&work[j2 - m], &ka1);

	    }

/*           start applying rotations in 1st set from the right */

	    i__1 = *kb - k + 1;
	    for (l = *ka - 1; l >= i__1; --l) {
		nrt = (*n - j2 + l) / ka1;
		if (nrt > 0) {
		    dlartv_(&nrt, &ab[ka1 - l + 1 + j2 * ab_dim1], &inca, &ab[
			    ka1 - l + (j2 + 1) * ab_dim1], &inca, &work[*n + 
			    j2 - m], &work[j2 - m], &ka1);
		}
/* L340: */
	    }

	    if (wantx) {

/*              post-multiply X by product of rotations in 1st set */

		i__1 = j1;
		i__3 = ka1;
		for (j = j2; i__3 < 0 ? j >= i__1 : j <= i__1; j += i__3) {
		    i__2 = *n - m;
		    drot_(&i__2, &x[m + 1 + j * x_dim1], &c__1, &x[m + 1 + (j 
			    + 1) * x_dim1], &c__1, &work[*n + j - m], &work[j 
			    - m]);
/* L350: */
		}
	    }
/* L360: */
	}

	if (update) {
	    if (i2 <= *n && kbt > 0) {

/*              create nonzero element a(i-kbt+ka+1,i-kbt) outside the */
/*              band and store it in WORK(i-kbt) */

		work[i__ - kbt] = -bb[kbt + 1 + (i__ - kbt) * bb_dim1] * ra1;
	    }
	}

	for (k = *kb; k >= 1; --k) {
	    if (update) {
/* Computing MAX */
		i__4 = 2, i__3 = k - i0 + 1;
		j2 = i__ - k - 1 + f2cmax(i__4,i__3) * ka1;
	    } else {
/* Computing MAX */
		i__4 = 1, i__3 = k - i0 + 1;
		j2 = i__ - k - 1 + f2cmax(i__4,i__3) * ka1;
	    }

/*           finish applying rotations in 2nd set from the right */

	    for (l = *kb - k; l >= 1; --l) {
		nrt = (*n - j2 + *ka + l) / ka1;
		if (nrt > 0) {
		    dlartv_(&nrt, &ab[ka1 - l + 1 + (j2 - *ka) * ab_dim1], &
			    inca, &ab[ka1 - l + (j2 - *ka + 1) * ab_dim1], &
			    inca, &work[*n + j2 - *ka], &work[j2 - *ka], &ka1)
			    ;
		}
/* L370: */
	    }
	    nr = (*n - j2 + *ka) / ka1;
	    j1 = j2 + (nr - 1) * ka1;
	    i__4 = j2;
	    i__3 = -ka1;
	    for (j = j1; i__3 < 0 ? j >= i__4 : j <= i__4; j += i__3) {
		work[j] = work[j - *ka];
		work[*n + j] = work[*n + j - *ka];
/* L380: */
	    }
	    i__3 = j1;
	    i__4 = ka1;
	    for (j = j2; i__4 < 0 ? j >= i__3 : j <= i__3; j += i__4) {

/*              create nonzero element a(j+1,j-ka) outside the band */
/*              and store it in WORK(j) */

		work[j] *= ab[ka1 + (j - *ka + 1) * ab_dim1];
		ab[ka1 + (j - *ka + 1) * ab_dim1] = work[*n + j] * ab[ka1 + (
			j - *ka + 1) * ab_dim1];
/* L390: */
	    }
	    if (update) {
		if (i__ - k < *n - *ka && k <= kbt) {
		    work[i__ - k + *ka] = work[i__ - k];
		}
	    }
/* L400: */
	}

	for (k = *kb; k >= 1; --k) {
/* Computing MAX */
	    i__4 = 1, i__3 = k - i0 + 1;
	    j2 = i__ - k - 1 + f2cmax(i__4,i__3) * ka1;
	    nr = (*n - j2 + *ka) / ka1;
	    j1 = j2 + (nr - 1) * ka1;
	    if (nr > 0) {

/*              generate rotations in 2nd set to annihilate elements */
/*              which have been created outside the band */

		dlargv_(&nr, &ab[ka1 + (j2 - *ka) * ab_dim1], &inca, &work[j2]
			, &ka1, &work[*n + j2], &ka1);

/*              apply rotations in 2nd set from the left */

		i__4 = *ka - 1;
		for (l = 1; l <= i__4; ++l) {
		    dlartv_(&nr, &ab[l + 1 + (j2 - l) * ab_dim1], &inca, &ab[
			    l + 2 + (j2 - l) * ab_dim1], &inca, &work[*n + j2]
			    , &work[j2], &ka1);
/* L410: */
		}

/*              apply rotations in 2nd set from both sides to diagonal */
/*              blocks */

		dlar2v_(&nr, &ab[j2 * ab_dim1 + 1], &ab[(j2 + 1) * ab_dim1 + 
			1], &ab[j2 * ab_dim1 + 2], &inca, &work[*n + j2], &
			work[j2], &ka1);

	    }

/*           start applying rotations in 2nd set from the right */

	    i__4 = *kb - k + 1;
	    for (l = *ka - 1; l >= i__4; --l) {
		nrt = (*n - j2 + l) / ka1;
		if (nrt > 0) {
		    dlartv_(&nrt, &ab[ka1 - l + 1 + j2 * ab_dim1], &inca, &ab[
			    ka1 - l + (j2 + 1) * ab_dim1], &inca, &work[*n + 
			    j2], &work[j2], &ka1);
		}
/* L420: */
	    }

	    if (wantx) {

/*              post-multiply X by product of rotations in 2nd set */

		i__4 = j1;
		i__3 = ka1;
		for (j = j2; i__3 < 0 ? j >= i__4 : j <= i__4; j += i__3) {
		    i__1 = *n - m;
		    drot_(&i__1, &x[m + 1 + j * x_dim1], &c__1, &x[m + 1 + (j 
			    + 1) * x_dim1], &c__1, &work[*n + j], &work[j]);
/* L430: */
		}
	    }
/* L440: */
	}

	i__3 = *kb - 1;
	for (k = 1; k <= i__3; ++k) {
/* Computing MAX */
	    i__4 = 1, i__1 = k - i0 + 2;
	    j2 = i__ - k - 1 + f2cmax(i__4,i__1) * ka1;

/*           finish applying rotations in 1st set from the right */

	    for (l = *kb - k; l >= 1; --l) {
		nrt = (*n - j2 + l) / ka1;
		if (nrt > 0) {
		    dlartv_(&nrt, &ab[ka1 - l + 1 + j2 * ab_dim1], &inca, &ab[
			    ka1 - l + (j2 + 1) * ab_dim1], &inca, &work[*n + 
			    j2 - m], &work[j2 - m], &ka1);
		}
/* L450: */
	    }
/* L460: */
	}

	if (*kb > 1) {
	    i__3 = i__ - *kb + (*ka << 1) + 1;
	    for (j = *n - 1; j >= i__3; --j) {
		work[*n + j - m] = work[*n + j - *ka - m];
		work[j - m] = work[j - *ka - m];
/* L470: */
	    }
	}

    }

    goto L10;

L480:

/*     **************************** Phase 2 ***************************** */

/*     The logical structure of this phase is: */

/*     UPDATE = .TRUE. */
/*     DO I = 1, M */
/*        use S(i) to update A and create a new bulge */
/*        apply rotations to push all bulges KA positions upward */
/*     END DO */
/*     UPDATE = .FALSE. */
/*     DO I = M - KA - 1, 2, -1 */
/*        apply rotations to push all bulges KA positions upward */
/*     END DO */

/*     To avoid duplicating code, the two loops are merged. */

    update = TRUE_;
    i__ = 0;
L490:
    if (update) {
	++i__;
/* Computing MIN */
	i__3 = *kb, i__4 = m - i__;
	kbt = f2cmin(i__3,i__4);
	i0 = i__ + 1;
/* Computing MAX */
	i__3 = 1, i__4 = i__ - *ka;
	i1 = f2cmax(i__3,i__4);
	i2 = i__ + kbt - ka1;
	if (i__ > m) {
	    update = FALSE_;
	    --i__;
	    i0 = m + 1;
	    if (*ka == 0) {
		return;
	    }
	    goto L490;
	}
    } else {
	i__ -= *ka;
	if (i__ < 2) {
	    return;
	}
    }

    if (i__ < m - kbt) {
	nx = m;
    } else {
	nx = *n;
    }

    if (upper) {

/*        Transform A, working with the upper triangle */

	if (update) {

/*           Form  inv(S(i))**T * A * inv(S(i)) */

	    bii = bb[kb1 + i__ * bb_dim1];
	    i__3 = i__;
	    for (j = i1; j <= i__3; ++j) {
		ab[j - i__ + ka1 + i__ * ab_dim1] /= bii;
/* L500: */
	    }
/* Computing MIN */
	    i__4 = *n, i__1 = i__ + *ka;
	    i__3 = f2cmin(i__4,i__1);
	    for (j = i__; j <= i__3; ++j) {
		ab[i__ - j + ka1 + j * ab_dim1] /= bii;
/* L510: */
	    }
	    i__3 = i__ + kbt;
	    for (k = i__ + 1; k <= i__3; ++k) {
		i__4 = i__ + kbt;
		for (j = k; j <= i__4; ++j) {
		    ab[k - j + ka1 + j * ab_dim1] = ab[k - j + ka1 + j * 
			    ab_dim1] - bb[i__ - j + kb1 + j * bb_dim1] * ab[
			    i__ - k + ka1 + k * ab_dim1] - bb[i__ - k + kb1 + 
			    k * bb_dim1] * ab[i__ - j + ka1 + j * ab_dim1] + 
			    ab[ka1 + i__ * ab_dim1] * bb[i__ - j + kb1 + j * 
			    bb_dim1] * bb[i__ - k + kb1 + k * bb_dim1];
/* L520: */
		}
/* Computing MIN */
		i__1 = *n, i__2 = i__ + *ka;
		i__4 = f2cmin(i__1,i__2);
		for (j = i__ + kbt + 1; j <= i__4; ++j) {
		    ab[k - j + ka1 + j * ab_dim1] -= bb[i__ - k + kb1 + k * 
			    bb_dim1] * ab[i__ - j + ka1 + j * ab_dim1];
/* L530: */
		}
/* L540: */
	    }
	    i__3 = i__;
	    for (j = i1; j <= i__3; ++j) {
/* Computing MIN */
		i__1 = j + *ka, i__2 = i__ + kbt;
		i__4 = f2cmin(i__1,i__2);
		for (k = i__ + 1; k <= i__4; ++k) {
		    ab[j - k + ka1 + k * ab_dim1] -= bb[i__ - k + kb1 + k * 
			    bb_dim1] * ab[j - i__ + ka1 + i__ * ab_dim1];
/* L550: */
		}
/* L560: */
	    }

	    if (wantx) {

/*              post-multiply X by inv(S(i)) */

		d__1 = 1. / bii;
		dscal_(&nx, &d__1, &x[i__ * x_dim1 + 1], &c__1);
		if (kbt > 0) {
		    i__3 = *ldbb - 1;
		    dger_(&nx, &kbt, &c_b20, &x[i__ * x_dim1 + 1], &c__1, &bb[
			    *kb + (i__ + 1) * bb_dim1], &i__3, &x[(i__ + 1) * 
			    x_dim1 + 1], ldx);
		}
	    }

/*           store a(i1,i) in RA1 for use in next loop over K */

	    ra1 = ab[i1 - i__ + ka1 + i__ * ab_dim1];
	}

/*        Generate and apply vectors of rotations to chase all the */
/*        existing bulges KA positions up toward the top of the band */

	i__3 = *kb - 1;
	for (k = 1; k <= i__3; ++k) {
	    if (update) {

/*              Determine the rotations which would annihilate the bulge */
/*              which has in theory just been created */

		if (i__ + k - ka1 > 0 && i__ + k < m) {

/*                 generate rotation to annihilate a(i+k-ka-1,i) */

		    dlartg_(&ab[k + 1 + i__ * ab_dim1], &ra1, &work[*n + i__ 
			    + k - *ka], &work[i__ + k - *ka], &ra);

/*                 create nonzero element a(i+k-ka-1,i+k) outside the */
/*                 band and store it in WORK(m-kb+i+k) */

		    t = -bb[kb1 - k + (i__ + k) * bb_dim1] * ra1;
		    work[m - *kb + i__ + k] = work[*n + i__ + k - *ka] * t - 
			    work[i__ + k - *ka] * ab[(i__ + k) * ab_dim1 + 1];
		    ab[(i__ + k) * ab_dim1 + 1] = work[i__ + k - *ka] * t + 
			    work[*n + i__ + k - *ka] * ab[(i__ + k) * ab_dim1 
			    + 1];
		    ra1 = ra;
		}
	    }
/* Computing MAX */
	    i__4 = 1, i__1 = k + i0 - m + 1;
	    j2 = i__ + k + 1 - f2cmax(i__4,i__1) * ka1;
	    nr = (j2 + *ka - 1) / ka1;
	    j1 = j2 - (nr - 1) * ka1;
	    if (update) {
/* Computing MIN */
		i__4 = j2, i__1 = i__ - (*ka << 1) + k - 1;
		j2t = f2cmin(i__4,i__1);
	    } else {
		j2t = j2;
	    }
	    nrt = (j2t + *ka - 1) / ka1;
	    i__4 = j2t;
	    i__1 = ka1;
	    for (j = j1; i__1 < 0 ? j >= i__4 : j <= i__4; j += i__1) {

/*              create nonzero element a(j-1,j+ka) outside the band */
/*              and store it in WORK(j) */

		work[j] *= ab[(j + *ka - 1) * ab_dim1 + 1];
		ab[(j + *ka - 1) * ab_dim1 + 1] = work[*n + j] * ab[(j + *ka 
			- 1) * ab_dim1 + 1];
/* L570: */
	    }

/*           generate rotations in 1st set to annihilate elements which */
/*           have been created outside the band */

	    if (nrt > 0) {
		dlargv_(&nrt, &ab[(j1 + *ka) * ab_dim1 + 1], &inca, &work[j1],
			 &ka1, &work[*n + j1], &ka1);
	    }
	    if (nr > 0) {

/*              apply rotations in 1st set from the left */

		i__1 = *ka - 1;
		for (l = 1; l <= i__1; ++l) {
		    dlartv_(&nr, &ab[ka1 - l + (j1 + l) * ab_dim1], &inca, &
			    ab[*ka - l + (j1 + l) * ab_dim1], &inca, &work[*n 
			    + j1], &work[j1], &ka1);
/* L580: */
		}

/*              apply rotations in 1st set from both sides to diagonal */
/*              blocks */

		dlar2v_(&nr, &ab[ka1 + j1 * ab_dim1], &ab[ka1 + (j1 - 1) * 
			ab_dim1], &ab[*ka + j1 * ab_dim1], &inca, &work[*n + 
			j1], &work[j1], &ka1);

	    }

/*           start applying rotations in 1st set from the right */

	    i__1 = *kb - k + 1;
	    for (l = *ka - 1; l >= i__1; --l) {
		nrt = (j2 + l - 1) / ka1;
		j1t = j2 - (nrt - 1) * ka1;
		if (nrt > 0) {
		    dlartv_(&nrt, &ab[l + j1t * ab_dim1], &inca, &ab[l + 1 + (
			    j1t - 1) * ab_dim1], &inca, &work[*n + j1t], &
			    work[j1t], &ka1);
		}
/* L590: */
	    }

	    if (wantx) {

/*              post-multiply X by product of rotations in 1st set */

		i__1 = j2;
		i__4 = ka1;
		for (j = j1; i__4 < 0 ? j >= i__1 : j <= i__1; j += i__4) {
		    drot_(&nx, &x[j * x_dim1 + 1], &c__1, &x[(j - 1) * x_dim1 
			    + 1], &c__1, &work[*n + j], &work[j]);
/* L600: */
		}
	    }
/* L610: */
	}

	if (update) {
	    if (i2 > 0 && kbt > 0) {

/*              create nonzero element a(i+kbt-ka-1,i+kbt) outside the */
/*              band and store it in WORK(m-kb+i+kbt) */

		work[m - *kb + i__ + kbt] = -bb[kb1 - kbt + (i__ + kbt) * 
			bb_dim1] * ra1;
	    }
	}

	for (k = *kb; k >= 1; --k) {
	    if (update) {
/* Computing MAX */
		i__3 = 2, i__4 = k + i0 - m;
		j2 = i__ + k + 1 - f2cmax(i__3,i__4) * ka1;
	    } else {
/* Computing MAX */
		i__3 = 1, i__4 = k + i0 - m;
		j2 = i__ + k + 1 - f2cmax(i__3,i__4) * ka1;
	    }

/*           finish applying rotations in 2nd set from the right */

	    for (l = *kb - k; l >= 1; --l) {
		nrt = (j2 + *ka + l - 1) / ka1;
		j1t = j2 - (nrt - 1) * ka1;
		if (nrt > 0) {
		    dlartv_(&nrt, &ab[l + (j1t + *ka) * ab_dim1], &inca, &ab[
			    l + 1 + (j1t + *ka - 1) * ab_dim1], &inca, &work[*
			    n + m - *kb + j1t + *ka], &work[m - *kb + j1t + *
			    ka], &ka1);
		}
/* L620: */
	    }
	    nr = (j2 + *ka - 1) / ka1;
	    j1 = j2 - (nr - 1) * ka1;
	    i__3 = j2;
	    i__4 = ka1;
	    for (j = j1; i__4 < 0 ? j >= i__3 : j <= i__3; j += i__4) {
		work[m - *kb + j] = work[m - *kb + j + *ka];
		work[*n + m - *kb + j] = work[*n + m - *kb + j + *ka];
/* L630: */
	    }
	    i__4 = j2;
	    i__3 = ka1;
	    for (j = j1; i__3 < 0 ? j >= i__4 : j <= i__4; j += i__3) {

/*              create nonzero element a(j-1,j+ka) outside the band */
/*              and store it in WORK(m-kb+j) */

		work[m - *kb + j] *= ab[(j + *ka - 1) * ab_dim1 + 1];
		ab[(j + *ka - 1) * ab_dim1 + 1] = work[*n + m - *kb + j] * ab[
			(j + *ka - 1) * ab_dim1 + 1];
/* L640: */
	    }
	    if (update) {
		if (i__ + k > ka1 && k <= kbt) {
		    work[m - *kb + i__ + k - *ka] = work[m - *kb + i__ + k];
		}
	    }
/* L650: */
	}

	for (k = *kb; k >= 1; --k) {
/* Computing MAX */
	    i__3 = 1, i__4 = k + i0 - m;
	    j2 = i__ + k + 1 - f2cmax(i__3,i__4) * ka1;
	    nr = (j2 + *ka - 1) / ka1;
	    j1 = j2 - (nr - 1) * ka1;
	    if (nr > 0) {

/*              generate rotations in 2nd set to annihilate elements */
/*              which have been created outside the band */

		dlargv_(&nr, &ab[(j1 + *ka) * ab_dim1 + 1], &inca, &work[m - *
			kb + j1], &ka1, &work[*n + m - *kb + j1], &ka1);

/*              apply rotations in 2nd set from the left */

		i__3 = *ka - 1;
		for (l = 1; l <= i__3; ++l) {
		    dlartv_(&nr, &ab[ka1 - l + (j1 + l) * ab_dim1], &inca, &
			    ab[*ka - l + (j1 + l) * ab_dim1], &inca, &work[*n 
			    + m - *kb + j1], &work[m - *kb + j1], &ka1);
/* L660: */
		}

/*              apply rotations in 2nd set from both sides to diagonal */
/*              blocks */

		dlar2v_(&nr, &ab[ka1 + j1 * ab_dim1], &ab[ka1 + (j1 - 1) * 
			ab_dim1], &ab[*ka + j1 * ab_dim1], &inca, &work[*n + 
			m - *kb + j1], &work[m - *kb + j1], &ka1);

	    }

/*           start applying rotations in 2nd set from the right */

	    i__3 = *kb - k + 1;
	    for (l = *ka - 1; l >= i__3; --l) {
		nrt = (j2 + l - 1) / ka1;
		j1t = j2 - (nrt - 1) * ka1;
		if (nrt > 0) {
		    dlartv_(&nrt, &ab[l + j1t * ab_dim1], &inca, &ab[l + 1 + (
			    j1t - 1) * ab_dim1], &inca, &work[*n + m - *kb + 
			    j1t], &work[m - *kb + j1t], &ka1);
		}
/* L670: */
	    }

	    if (wantx) {

/*              post-multiply X by product of rotations in 2nd set */

		i__3 = j2;
		i__4 = ka1;
		for (j = j1; i__4 < 0 ? j >= i__3 : j <= i__3; j += i__4) {
		    drot_(&nx, &x[j * x_dim1 + 1], &c__1, &x[(j - 1) * x_dim1 
			    + 1], &c__1, &work[*n + m - *kb + j], &work[m - *
			    kb + j]);
/* L680: */
		}
	    }
/* L690: */
	}

	i__4 = *kb - 1;
	for (k = 1; k <= i__4; ++k) {
/* Computing MAX */
	    i__3 = 1, i__1 = k + i0 - m + 1;
	    j2 = i__ + k + 1 - f2cmax(i__3,i__1) * ka1;

/*           finish applying rotations in 1st set from the right */

	    for (l = *kb - k; l >= 1; --l) {
		nrt = (j2 + l - 1) / ka1;
		j1t = j2 - (nrt - 1) * ka1;
		if (nrt > 0) {
		    dlartv_(&nrt, &ab[l + j1t * ab_dim1], &inca, &ab[l + 1 + (
			    j1t - 1) * ab_dim1], &inca, &work[*n + j1t], &
			    work[j1t], &ka1);
		}
/* L700: */
	    }
/* L710: */
	}

	if (*kb > 1) {
/* Computing MIN */
	    i__3 = i__ + *kb;
	    i__4 = f2cmin(i__3,m) - (*ka << 1) - 1;
	    for (j = 2; j <= i__4; ++j) {
		work[*n + j] = work[*n + j + *ka];
		work[j] = work[j + *ka];
/* L720: */
	    }
	}

    } else {

/*        Transform A, working with the lower triangle */

	if (update) {

/*           Form  inv(S(i))**T * A * inv(S(i)) */

	    bii = bb[i__ * bb_dim1 + 1];
	    i__4 = i__;
	    for (j = i1; j <= i__4; ++j) {
		ab[i__ - j + 1 + j * ab_dim1] /= bii;
/* L730: */
	    }
/* Computing MIN */
	    i__3 = *n, i__1 = i__ + *ka;
	    i__4 = f2cmin(i__3,i__1);
	    for (j = i__; j <= i__4; ++j) {
		ab[j - i__ + 1 + i__ * ab_dim1] /= bii;
/* L740: */
	    }
	    i__4 = i__ + kbt;
	    for (k = i__ + 1; k <= i__4; ++k) {
		i__3 = i__ + kbt;
		for (j = k; j <= i__3; ++j) {
		    ab[j - k + 1 + k * ab_dim1] = ab[j - k + 1 + k * ab_dim1] 
			    - bb[j - i__ + 1 + i__ * bb_dim1] * ab[k - i__ + 
			    1 + i__ * ab_dim1] - bb[k - i__ + 1 + i__ * 
			    bb_dim1] * ab[j - i__ + 1 + i__ * ab_dim1] + ab[
			    i__ * ab_dim1 + 1] * bb[j - i__ + 1 + i__ * 
			    bb_dim1] * bb[k - i__ + 1 + i__ * bb_dim1];
/* L750: */
		}
/* Computing MIN */
		i__1 = *n, i__2 = i__ + *ka;
		i__3 = f2cmin(i__1,i__2);
		for (j = i__ + kbt + 1; j <= i__3; ++j) {
		    ab[j - k + 1 + k * ab_dim1] -= bb[k - i__ + 1 + i__ * 
			    bb_dim1] * ab[j - i__ + 1 + i__ * ab_dim1];
/* L760: */
		}
/* L770: */
	    }
	    i__4 = i__;
	    for (j = i1; j <= i__4; ++j) {
/* Computing MIN */
		i__1 = j + *ka, i__2 = i__ + kbt;
		i__3 = f2cmin(i__1,i__2);
		for (k = i__ + 1; k <= i__3; ++k) {
		    ab[k - j + 1 + j * ab_dim1] -= bb[k - i__ + 1 + i__ * 
			    bb_dim1] * ab[i__ - j + 1 + j * ab_dim1];
/* L780: */
		}
/* L790: */
	    }

	    if (wantx) {

/*              post-multiply X by inv(S(i)) */

		d__1 = 1. / bii;
		dscal_(&nx, &d__1, &x[i__ * x_dim1 + 1], &c__1);
		if (kbt > 0) {
		    dger_(&nx, &kbt, &c_b20, &x[i__ * x_dim1 + 1], &c__1, &bb[
			    i__ * bb_dim1 + 2], &c__1, &x[(i__ + 1) * x_dim1 
			    + 1], ldx);
		}
	    }

/*           store a(i,i1) in RA1 for use in next loop over K */

	    ra1 = ab[i__ - i1 + 1 + i1 * ab_dim1];
	}

/*        Generate and apply vectors of rotations to chase all the */
/*        existing bulges KA positions up toward the top of the band */

	i__4 = *kb - 1;
	for (k = 1; k <= i__4; ++k) {
	    if (update) {

/*              Determine the rotations which would annihilate the bulge */
/*              which has in theory just been created */

		if (i__ + k - ka1 > 0 && i__ + k < m) {

/*                 generate rotation to annihilate a(i,i+k-ka-1) */

		    dlartg_(&ab[ka1 - k + (i__ + k - *ka) * ab_dim1], &ra1, &
			    work[*n + i__ + k - *ka], &work[i__ + k - *ka], &
			    ra);

/*                 create nonzero element a(i+k,i+k-ka-1) outside the */
/*                 band and store it in WORK(m-kb+i+k) */

		    t = -bb[k + 1 + i__ * bb_dim1] * ra1;
		    work[m - *kb + i__ + k] = work[*n + i__ + k - *ka] * t - 
			    work[i__ + k - *ka] * ab[ka1 + (i__ + k - *ka) * 
			    ab_dim1];
		    ab[ka1 + (i__ + k - *ka) * ab_dim1] = work[i__ + k - *ka] 
			    * t + work[*n + i__ + k - *ka] * ab[ka1 + (i__ + 
			    k - *ka) * ab_dim1];
		    ra1 = ra;
		}
	    }
/* Computing MAX */
	    i__3 = 1, i__1 = k + i0 - m + 1;
	    j2 = i__ + k + 1 - f2cmax(i__3,i__1) * ka1;
	    nr = (j2 + *ka - 1) / ka1;
	    j1 = j2 - (nr - 1) * ka1;
	    if (update) {
/* Computing MIN */
		i__3 = j2, i__1 = i__ - (*ka << 1) + k - 1;
		j2t = f2cmin(i__3,i__1);
	    } else {
		j2t = j2;
	    }
	    nrt = (j2t + *ka - 1) / ka1;
	    i__3 = j2t;
	    i__1 = ka1;
	    for (j = j1; i__1 < 0 ? j >= i__3 : j <= i__3; j += i__1) {

/*              create nonzero element a(j+ka,j-1) outside the band */
/*              and store it in WORK(j) */

		work[j] *= ab[ka1 + (j - 1) * ab_dim1];
		ab[ka1 + (j - 1) * ab_dim1] = work[*n + j] * ab[ka1 + (j - 1) 
			* ab_dim1];
/* L800: */
	    }

/*           generate rotations in 1st set to annihilate elements which */
/*           have been created outside the band */

	    if (nrt > 0) {
		dlargv_(&nrt, &ab[ka1 + j1 * ab_dim1], &inca, &work[j1], &ka1,
			 &work[*n + j1], &ka1);
	    }
	    if (nr > 0) {

/*              apply rotations in 1st set from the right */

		i__1 = *ka - 1;
		for (l = 1; l <= i__1; ++l) {
		    dlartv_(&nr, &ab[l + 1 + j1 * ab_dim1], &inca, &ab[l + 2 
			    + (j1 - 1) * ab_dim1], &inca, &work[*n + j1], &
			    work[j1], &ka1);
/* L810: */
		}

/*              apply rotations in 1st set from both sides to diagonal */
/*              blocks */

		dlar2v_(&nr, &ab[j1 * ab_dim1 + 1], &ab[(j1 - 1) * ab_dim1 + 
			1], &ab[(j1 - 1) * ab_dim1 + 2], &inca, &work[*n + j1]
			, &work[j1], &ka1);

	    }

/*           start applying rotations in 1st set from the left */

	    i__1 = *kb - k + 1;
	    for (l = *ka - 1; l >= i__1; --l) {
		nrt = (j2 + l - 1) / ka1;
		j1t = j2 - (nrt - 1) * ka1;
		if (nrt > 0) {
		    dlartv_(&nrt, &ab[ka1 - l + 1 + (j1t - ka1 + l) * ab_dim1]
			    , &inca, &ab[ka1 - l + (j1t - ka1 + l) * ab_dim1],
			     &inca, &work[*n + j1t], &work[j1t], &ka1);
		}
/* L820: */
	    }

	    if (wantx) {

/*              post-multiply X by product of rotations in 1st set */

		i__1 = j2;
		i__3 = ka1;
		for (j = j1; i__3 < 0 ? j >= i__1 : j <= i__1; j += i__3) {
		    drot_(&nx, &x[j * x_dim1 + 1], &c__1, &x[(j - 1) * x_dim1 
			    + 1], &c__1, &work[*n + j], &work[j]);
/* L830: */
		}
	    }
/* L840: */
	}

	if (update) {
	    if (i2 > 0 && kbt > 0) {

/*              create nonzero element a(i+kbt,i+kbt-ka-1) outside the */
/*              band and store it in WORK(m-kb+i+kbt) */

		work[m - *kb + i__ + kbt] = -bb[kbt + 1 + i__ * bb_dim1] * 
			ra1;
	    }
	}

	for (k = *kb; k >= 1; --k) {
	    if (update) {
/* Computing MAX */
		i__4 = 2, i__3 = k + i0 - m;
		j2 = i__ + k + 1 - f2cmax(i__4,i__3) * ka1;
	    } else {
/* Computing MAX */
		i__4 = 1, i__3 = k + i0 - m;
		j2 = i__ + k + 1 - f2cmax(i__4,i__3) * ka1;
	    }

/*           finish applying rotations in 2nd set from the left */

	    for (l = *kb - k; l >= 1; --l) {
		nrt = (j2 + *ka + l - 1) / ka1;
		j1t = j2 - (nrt - 1) * ka1;
		if (nrt > 0) {
		    dlartv_(&nrt, &ab[ka1 - l + 1 + (j1t + l - 1) * ab_dim1], 
			    &inca, &ab[ka1 - l + (j1t + l - 1) * ab_dim1], &
			    inca, &work[*n + m - *kb + j1t + *ka], &work[m - *
			    kb + j1t + *ka], &ka1);
		}
/* L850: */
	    }
	    nr = (j2 + *ka - 1) / ka1;
	    j1 = j2 - (nr - 1) * ka1;
	    i__4 = j2;
	    i__3 = ka1;
	    for (j = j1; i__3 < 0 ? j >= i__4 : j <= i__4; j += i__3) {
		work[m - *kb + j] = work[m - *kb + j + *ka];
		work[*n + m - *kb + j] = work[*n + m - *kb + j + *ka];
/* L860: */
	    }
	    i__3 = j2;
	    i__4 = ka1;
	    for (j = j1; i__4 < 0 ? j >= i__3 : j <= i__3; j += i__4) {

/*              create nonzero element a(j+ka,j-1) outside the band */
/*              and store it in WORK(m-kb+j) */

		work[m - *kb + j] *= ab[ka1 + (j - 1) * ab_dim1];
		ab[ka1 + (j - 1) * ab_dim1] = work[*n + m - *kb + j] * ab[ka1 
			+ (j - 1) * ab_dim1];
/* L870: */
	    }
	    if (update) {
		if (i__ + k > ka1 && k <= kbt) {
		    work[m - *kb + i__ + k - *ka] = work[m - *kb + i__ + k];
		}
	    }
/* L880: */
	}

	for (k = *kb; k >= 1; --k) {
/* Computing MAX */
	    i__4 = 1, i__3 = k + i0 - m;
	    j2 = i__ + k + 1 - f2cmax(i__4,i__3) * ka1;
	    nr = (j2 + *ka - 1) / ka1;
	    j1 = j2 - (nr - 1) * ka1;
	    if (nr > 0) {

/*              generate rotations in 2nd set to annihilate elements */
/*              which have been created outside the band */

		dlargv_(&nr, &ab[ka1 + j1 * ab_dim1], &inca, &work[m - *kb + 
			j1], &ka1, &work[*n + m - *kb + j1], &ka1);

/*              apply rotations in 2nd set from the right */

		i__4 = *ka - 1;
		for (l = 1; l <= i__4; ++l) {
		    dlartv_(&nr, &ab[l + 1 + j1 * ab_dim1], &inca, &ab[l + 2 
			    + (j1 - 1) * ab_dim1], &inca, &work[*n + m - *kb 
			    + j1], &work[m - *kb + j1], &ka1);
/* L890: */
		}

/*              apply rotations in 2nd set from both sides to diagonal */
/*              blocks */

		dlar2v_(&nr, &ab[j1 * ab_dim1 + 1], &ab[(j1 - 1) * ab_dim1 + 
			1], &ab[(j1 - 1) * ab_dim1 + 2], &inca, &work[*n + m 
			- *kb + j1], &work[m - *kb + j1], &ka1);

	    }

/*           start applying rotations in 2nd set from the left */

	    i__4 = *kb - k + 1;
	    for (l = *ka - 1; l >= i__4; --l) {
		nrt = (j2 + l - 1) / ka1;
		j1t = j2 - (nrt - 1) * ka1;
		if (nrt > 0) {
		    dlartv_(&nrt, &ab[ka1 - l + 1 + (j1t - ka1 + l) * ab_dim1]
			    , &inca, &ab[ka1 - l + (j1t - ka1 + l) * ab_dim1],
			     &inca, &work[*n + m - *kb + j1t], &work[m - *kb 
			    + j1t], &ka1);
		}
/* L900: */
	    }

	    if (wantx) {

/*              post-multiply X by product of rotations in 2nd set */

		i__4 = j2;
		i__3 = ka1;
		for (j = j1; i__3 < 0 ? j >= i__4 : j <= i__4; j += i__3) {
		    drot_(&nx, &x[j * x_dim1 + 1], &c__1, &x[(j - 1) * x_dim1 
			    + 1], &c__1, &work[*n + m - *kb + j], &work[m - *
			    kb + j]);
/* L910: */
		}
	    }
/* L920: */
	}

	i__3 = *kb - 1;
	for (k = 1; k <= i__3; ++k) {
/* Computing MAX */
	    i__4 = 1, i__1 = k + i0 - m + 1;
	    j2 = i__ + k + 1 - f2cmax(i__4,i__1) * ka1;

/*           finish applying rotations in 1st set from the left */

	    for (l = *kb - k; l >= 1; --l) {
		nrt = (j2 + l - 1) / ka1;
		j1t = j2 - (nrt - 1) * ka1;
		if (nrt > 0) {
		    dlartv_(&nrt, &ab[ka1 - l + 1 + (j1t - ka1 + l) * ab_dim1]
			    , &inca, &ab[ka1 - l + (j1t - ka1 + l) * ab_dim1],
			     &inca, &work[*n + j1t], &work[j1t], &ka1);
		}
/* L930: */
	    }
/* L940: */
	}

	if (*kb > 1) {
/* Computing MIN */
	    i__4 = i__ + *kb;
	    i__3 = f2cmin(i__4,m) - (*ka << 1) - 1;
	    for (j = 2; j <= i__3; ++j) {
		work[*n + j] = work[*n + j + *ka];
		work[j] = work[j + *ka];
/* L950: */
	    }
	}

    }

    goto L490;

/*     End of DSBGST */

} /* dsbgst_ */

