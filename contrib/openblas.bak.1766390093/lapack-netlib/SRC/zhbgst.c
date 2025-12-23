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

static doublecomplex c_b1 = {0.,0.};
static doublecomplex c_b2 = {1.,0.};
static integer c__1 = 1;

/* > \brief \b ZHBGST */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download ZHBGST + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zhbgst.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zhbgst.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zhbgst.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE ZHBGST( VECT, UPLO, N, KA, KB, AB, LDAB, BB, LDBB, X, */
/*                          LDX, WORK, RWORK, INFO ) */

/*       CHARACTER          UPLO, VECT */
/*       INTEGER            INFO, KA, KB, LDAB, LDBB, LDX, N */
/*       DOUBLE PRECISION   RWORK( * ) */
/*       COMPLEX*16         AB( LDAB, * ), BB( LDBB, * ), WORK( * ), */
/*      $                   X( LDX, * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > ZHBGST reduces a complex Hermitian-definite banded generalized */
/* > eigenproblem  A*x = lambda*B*x  to standard form  C*y = lambda*y, */
/* > such that C has the same bandwidth as A. */
/* > */
/* > B must have been previously factorized as S**H*S by ZPBSTF, using a */
/* > split Cholesky factorization. A is overwritten by C = X**H*A*X, where */
/* > X = S**(-1)*Q and Q is a unitary matrix chosen to preserve the */
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
/* >          AB is COMPLEX*16 array, dimension (LDAB,N) */
/* >          On entry, the upper or lower triangle of the Hermitian band */
/* >          matrix A, stored in the first ka+1 rows of the array.  The */
/* >          j-th column of A is stored in the j-th column of the array AB */
/* >          as follows: */
/* >          if UPLO = 'U', AB(ka+1+i-j,j) = A(i,j) for f2cmax(1,j-ka)<=i<=j; */
/* >          if UPLO = 'L', AB(1+i-j,j)    = A(i,j) for j<=i<=f2cmin(n,j+ka). */
/* > */
/* >          On exit, the transformed matrix X**H*A*X, stored in the same */
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
/* >          BB is COMPLEX*16 array, dimension (LDBB,N) */
/* >          The banded factor S from the split Cholesky factorization of */
/* >          B, as returned by ZPBSTF, stored in the first kb+1 rows of */
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
/* >          X is COMPLEX*16 array, dimension (LDX,N) */
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
/* >          WORK is COMPLEX*16 array, dimension (N) */
/* > \endverbatim */
/* > */
/* > \param[out] RWORK */
/* > \verbatim */
/* >          RWORK is DOUBLE PRECISION array, dimension (N) */
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

/* > \ingroup complex16OTHERcomputational */

/*  ===================================================================== */
/* Subroutine */ void zhbgst_(char *vect, char *uplo, integer *n, integer *ka, 
	integer *kb, doublecomplex *ab, integer *ldab, doublecomplex *bb, 
	integer *ldbb, doublecomplex *x, integer *ldx, doublecomplex *work, 
	doublereal *rwork, integer *info)
{
    /* System generated locals */
    integer ab_dim1, ab_offset, bb_dim1, bb_offset, x_dim1, x_offset, i__1, 
	    i__2, i__3, i__4, i__5, i__6, i__7, i__8;
    doublereal d__1;
    doublecomplex z__1, z__2, z__3, z__4, z__5, z__6, z__7, z__8, z__9, z__10;

    /* Local variables */
    integer inca;
    extern /* Subroutine */ void zrot_(integer *, doublecomplex *, integer *, 
	    doublecomplex *, integer *, doublereal *, doublecomplex *);
    integer i__, j, k, l, m;
    doublecomplex t;
    extern logical lsame_(char *, char *);
    extern /* Subroutine */ void zgerc_(integer *, integer *, doublecomplex *, 
	    doublecomplex *, integer *, doublecomplex *, integer *, 
	    doublecomplex *, integer *);
    integer i0, i1;
    logical upper;
    integer i2, j1, j2;
    extern /* Subroutine */ void zgeru_(integer *, integer *, doublecomplex *, 
	    doublecomplex *, integer *, doublecomplex *, integer *, 
	    doublecomplex *, integer *);
    logical wantx;
    extern /* Subroutine */ void zlar2v_(integer *, doublecomplex *, 
	    doublecomplex *, doublecomplex *, integer *, doublereal *, 
	    doublecomplex *, integer *);
    doublecomplex ra;
    integer nr, nx;
    extern /* Subroutine */ int xerbla_(char *, integer *, ftnlen);
    extern void zdscal_(
	    integer *, doublereal *, doublecomplex *, integer *);
    logical update;
    extern /* Subroutine */ void zlacgv_(integer *, doublecomplex *, integer *)
	    ;
    integer ka1, kb1;
    extern /* Subroutine */ void zlaset_(char *, integer *, integer *, 
	    doublecomplex *, doublecomplex *, doublecomplex *, integer *), zlartg_(doublecomplex *, doublecomplex *, doublereal *, 
	    doublecomplex *, doublecomplex *);
    doublecomplex ra1;
    extern /* Subroutine */ void zlargv_(integer *, doublecomplex *, integer *,
	     doublecomplex *, integer *, doublereal *, integer *);
    integer j1t, j2t;
    extern /* Subroutine */ void zlartv_(integer *, doublecomplex *, integer *,
	     doublecomplex *, integer *, doublereal *, doublecomplex *, 
	    integer *);
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
    --rwork;

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
	xerbla_("ZHBGST", &i__1, (ftnlen)6);
	return;
    }

/*     Quick return if possible */

    if (*n == 0) {
	return;
    }

    inca = *ldab * ka1;

/*     Initialize X to the unit matrix, if needed */

    if (wantx) {
	zlaset_("Full", n, n, &c_b1, &c_b2, &x[x_offset], ldx);
    }

/*     Set M to the splitting point m. It must be the same value as is */
/*     used in ZPBSTF. The chosen value allows the arrays WORK and RWORK */
/*     to be of dimension (N). */

    m = (*n + *kb) / 2;

/*     The routine works in two phases, corresponding to the two halves */
/*     of the split Cholesky factorization of B as S**H*S where */

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
/*     inv(S(i))**H*A*inv(S(i)). This creates a triangular bulge outside */
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

/*     The real cosines and complex sines of the rotations are stored in */
/*     the arrays RWORK and WORK, those of the 1st set in elements */
/*     2:m-kb-1, and those of the 2nd set in elements m-kb+1:n. */

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

/*           Form  inv(S(i))**H * A * inv(S(i)) */

	    i__1 = kb1 + i__ * bb_dim1;
	    bii = bb[i__1].r;
	    i__1 = ka1 + i__ * ab_dim1;
	    i__2 = ka1 + i__ * ab_dim1;
	    d__1 = ab[i__2].r / bii / bii;
	    ab[i__1].r = d__1, ab[i__1].i = 0.;
	    i__1 = i1;
	    for (j = i__ + 1; j <= i__1; ++j) {
		i__2 = i__ - j + ka1 + j * ab_dim1;
		i__3 = i__ - j + ka1 + j * ab_dim1;
		z__1.r = ab[i__3].r / bii, z__1.i = ab[i__3].i / bii;
		ab[i__2].r = z__1.r, ab[i__2].i = z__1.i;
/* L20: */
	    }
/* Computing MAX */
	    i__1 = 1, i__2 = i__ - *ka;
	    i__3 = i__ - 1;
	    for (j = f2cmax(i__1,i__2); j <= i__3; ++j) {
		i__1 = j - i__ + ka1 + i__ * ab_dim1;
		i__2 = j - i__ + ka1 + i__ * ab_dim1;
		z__1.r = ab[i__2].r / bii, z__1.i = ab[i__2].i / bii;
		ab[i__1].r = z__1.r, ab[i__1].i = z__1.i;
/* L30: */
	    }
	    i__3 = i__ - 1;
	    for (k = i__ - kbt; k <= i__3; ++k) {
		i__1 = k;
		for (j = i__ - kbt; j <= i__1; ++j) {
		    i__2 = j - k + ka1 + k * ab_dim1;
		    i__4 = j - k + ka1 + k * ab_dim1;
		    i__5 = j - i__ + kb1 + i__ * bb_dim1;
		    d_cnjg(&z__5, &ab[k - i__ + ka1 + i__ * ab_dim1]);
		    z__4.r = bb[i__5].r * z__5.r - bb[i__5].i * z__5.i, 
			    z__4.i = bb[i__5].r * z__5.i + bb[i__5].i * 
			    z__5.r;
		    z__3.r = ab[i__4].r - z__4.r, z__3.i = ab[i__4].i - 
			    z__4.i;
		    d_cnjg(&z__7, &bb[k - i__ + kb1 + i__ * bb_dim1]);
		    i__6 = j - i__ + ka1 + i__ * ab_dim1;
		    z__6.r = z__7.r * ab[i__6].r - z__7.i * ab[i__6].i, 
			    z__6.i = z__7.r * ab[i__6].i + z__7.i * ab[i__6]
			    .r;
		    z__2.r = z__3.r - z__6.r, z__2.i = z__3.i - z__6.i;
		    i__7 = ka1 + i__ * ab_dim1;
		    d__1 = ab[i__7].r;
		    i__8 = j - i__ + kb1 + i__ * bb_dim1;
		    z__9.r = d__1 * bb[i__8].r, z__9.i = d__1 * bb[i__8].i;
		    d_cnjg(&z__10, &bb[k - i__ + kb1 + i__ * bb_dim1]);
		    z__8.r = z__9.r * z__10.r - z__9.i * z__10.i, z__8.i = 
			    z__9.r * z__10.i + z__9.i * z__10.r;
		    z__1.r = z__2.r + z__8.r, z__1.i = z__2.i + z__8.i;
		    ab[i__2].r = z__1.r, ab[i__2].i = z__1.i;
/* L40: */
		}
/* Computing MAX */
		i__1 = 1, i__2 = i__ - *ka;
		i__4 = i__ - kbt - 1;
		for (j = f2cmax(i__1,i__2); j <= i__4; ++j) {
		    i__1 = j - k + ka1 + k * ab_dim1;
		    i__2 = j - k + ka1 + k * ab_dim1;
		    d_cnjg(&z__3, &bb[k - i__ + kb1 + i__ * bb_dim1]);
		    i__5 = j - i__ + ka1 + i__ * ab_dim1;
		    z__2.r = z__3.r * ab[i__5].r - z__3.i * ab[i__5].i, 
			    z__2.i = z__3.r * ab[i__5].i + z__3.i * ab[i__5]
			    .r;
		    z__1.r = ab[i__2].r - z__2.r, z__1.i = ab[i__2].i - 
			    z__2.i;
		    ab[i__1].r = z__1.r, ab[i__1].i = z__1.i;
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
		    i__4 = k - j + ka1 + j * ab_dim1;
		    i__1 = k - j + ka1 + j * ab_dim1;
		    i__5 = k - i__ + kb1 + i__ * bb_dim1;
		    i__6 = i__ - j + ka1 + j * ab_dim1;
		    z__2.r = bb[i__5].r * ab[i__6].r - bb[i__5].i * ab[i__6]
			    .i, z__2.i = bb[i__5].r * ab[i__6].i + bb[i__5].i 
			    * ab[i__6].r;
		    z__1.r = ab[i__1].r - z__2.r, z__1.i = ab[i__1].i - 
			    z__2.i;
		    ab[i__4].r = z__1.r, ab[i__4].i = z__1.i;
/* L70: */
		}
/* L80: */
	    }

	    if (wantx) {

/*              post-multiply X by inv(S(i)) */

		i__3 = *n - m;
		d__1 = 1. / bii;
		zdscal_(&i__3, &d__1, &x[m + 1 + i__ * x_dim1], &c__1);
		if (kbt > 0) {
		    i__3 = *n - m;
		    z__1.r = -1., z__1.i = 0.;
		    zgerc_(&i__3, &kbt, &z__1, &x[m + 1 + i__ * x_dim1], &
			    c__1, &bb[kb1 - kbt + i__ * bb_dim1], &c__1, &x[m 
			    + 1 + (i__ - kbt) * x_dim1], ldx);
		}
	    }

/*           store a(i,i1) in RA1 for use in next loop over K */

	    i__3 = i__ - i1 + ka1 + i1 * ab_dim1;
	    ra1.r = ab[i__3].r, ra1.i = ab[i__3].i;
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

		    zlartg_(&ab[k + 1 + (i__ - k + *ka) * ab_dim1], &ra1, &
			    rwork[i__ - k + *ka - m], &work[i__ - k + *ka - m]
			    , &ra);

/*                 create nonzero element a(i-k,i-k+ka+1) outside the */
/*                 band and store it in WORK(i-k) */

		    i__2 = kb1 - k + i__ * bb_dim1;
		    z__2.r = -bb[i__2].r, z__2.i = -bb[i__2].i;
		    z__1.r = z__2.r * ra1.r - z__2.i * ra1.i, z__1.i = z__2.r 
			    * ra1.i + z__2.i * ra1.r;
		    t.r = z__1.r, t.i = z__1.i;
		    i__2 = i__ - k;
		    i__4 = i__ - k + *ka - m;
		    z__2.r = rwork[i__4] * t.r, z__2.i = rwork[i__4] * t.i;
		    d_cnjg(&z__4, &work[i__ - k + *ka - m]);
		    i__1 = (i__ - k + *ka) * ab_dim1 + 1;
		    z__3.r = z__4.r * ab[i__1].r - z__4.i * ab[i__1].i, 
			    z__3.i = z__4.r * ab[i__1].i + z__4.i * ab[i__1]
			    .r;
		    z__1.r = z__2.r - z__3.r, z__1.i = z__2.i - z__3.i;
		    work[i__2].r = z__1.r, work[i__2].i = z__1.i;
		    i__2 = (i__ - k + *ka) * ab_dim1 + 1;
		    i__4 = i__ - k + *ka - m;
		    z__2.r = work[i__4].r * t.r - work[i__4].i * t.i, z__2.i =
			     work[i__4].r * t.i + work[i__4].i * t.r;
		    i__1 = i__ - k + *ka - m;
		    i__5 = (i__ - k + *ka) * ab_dim1 + 1;
		    z__3.r = rwork[i__1] * ab[i__5].r, z__3.i = rwork[i__1] * 
			    ab[i__5].i;
		    z__1.r = z__2.r + z__3.r, z__1.i = z__2.i + z__3.i;
		    ab[i__2].r = z__1.r, ab[i__2].i = z__1.i;
		    ra1.r = ra.r, ra1.i = ra.i;
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

		i__1 = j - m;
		i__5 = j - m;
		i__6 = (j + 1) * ab_dim1 + 1;
		z__1.r = work[i__5].r * ab[i__6].r - work[i__5].i * ab[i__6]
			.i, z__1.i = work[i__5].r * ab[i__6].i + work[i__5].i 
			* ab[i__6].r;
		work[i__1].r = z__1.r, work[i__1].i = z__1.i;
		i__1 = (j + 1) * ab_dim1 + 1;
		i__5 = j - m;
		i__6 = (j + 1) * ab_dim1 + 1;
		z__1.r = rwork[i__5] * ab[i__6].r, z__1.i = rwork[i__5] * ab[
			i__6].i;
		ab[i__1].r = z__1.r, ab[i__1].i = z__1.i;
/* L90: */
	    }

/*           generate rotations in 1st set to annihilate elements which */
/*           have been created outside the band */

	    if (nrt > 0) {
		zlargv_(&nrt, &ab[j2t * ab_dim1 + 1], &inca, &work[j2t - m], &
			ka1, &rwork[j2t - m], &ka1);
	    }
	    if (nr > 0) {

/*              apply rotations in 1st set from the right */

		i__4 = *ka - 1;
		for (l = 1; l <= i__4; ++l) {
		    zlartv_(&nr, &ab[ka1 - l + j2 * ab_dim1], &inca, &ab[*ka 
			    - l + (j2 + 1) * ab_dim1], &inca, &rwork[j2 - m], 
			    &work[j2 - m], &ka1);
/* L100: */
		}

/*              apply rotations in 1st set from both sides to diagonal */
/*              blocks */

		zlar2v_(&nr, &ab[ka1 + j2 * ab_dim1], &ab[ka1 + (j2 + 1) * 
			ab_dim1], &ab[*ka + (j2 + 1) * ab_dim1], &inca, &
			rwork[j2 - m], &work[j2 - m], &ka1);

		zlacgv_(&nr, &work[j2 - m], &ka1);
	    }

/*           start applying rotations in 1st set from the left */

	    i__4 = *kb - k + 1;
	    for (l = *ka - 1; l >= i__4; --l) {
		nrt = (*n - j2 + l) / ka1;
		if (nrt > 0) {
		    zlartv_(&nrt, &ab[l + (j2 + ka1 - l) * ab_dim1], &inca, &
			    ab[l + 1 + (j2 + ka1 - l) * ab_dim1], &inca, &
			    rwork[j2 - m], &work[j2 - m], &ka1);
		}
/* L110: */
	    }

	    if (wantx) {

/*              post-multiply X by product of rotations in 1st set */

		i__4 = j1;
		i__2 = ka1;
		for (j = j2; i__2 < 0 ? j >= i__4 : j <= i__4; j += i__2) {
		    i__1 = *n - m;
		    d_cnjg(&z__1, &work[j - m]);
		    zrot_(&i__1, &x[m + 1 + j * x_dim1], &c__1, &x[m + 1 + (j 
			    + 1) * x_dim1], &c__1, &rwork[j - m], &z__1);
/* L120: */
		}
	    }
/* L130: */
	}

	if (update) {
	    if (i2 <= *n && kbt > 0) {

/*              create nonzero element a(i-kbt,i-kbt+ka+1) outside the */
/*              band and store it in WORK(i-kbt) */

		i__3 = i__ - kbt;
		i__2 = kb1 - kbt + i__ * bb_dim1;
		z__2.r = -bb[i__2].r, z__2.i = -bb[i__2].i;
		z__1.r = z__2.r * ra1.r - z__2.i * ra1.i, z__1.i = z__2.r * 
			ra1.i + z__2.i * ra1.r;
		work[i__3].r = z__1.r, work[i__3].i = z__1.i;
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
		    zlartv_(&nrt, &ab[l + (j2 - l + 1) * ab_dim1], &inca, &ab[
			    l + 1 + (j2 - l + 1) * ab_dim1], &inca, &rwork[j2 
			    - *ka], &work[j2 - *ka], &ka1);
		}
/* L140: */
	    }
	    nr = (*n - j2 + *ka) / ka1;
	    j1 = j2 + (nr - 1) * ka1;
	    i__3 = j2;
	    i__2 = -ka1;
	    for (j = j1; i__2 < 0 ? j >= i__3 : j <= i__3; j += i__2) {
		i__4 = j;
		i__1 = j - *ka;
		work[i__4].r = work[i__1].r, work[i__4].i = work[i__1].i;
		rwork[j] = rwork[j - *ka];
/* L150: */
	    }
	    i__2 = j1;
	    i__3 = ka1;
	    for (j = j2; i__3 < 0 ? j >= i__2 : j <= i__2; j += i__3) {

/*              create nonzero element a(j-ka,j+1) outside the band */
/*              and store it in WORK(j) */

		i__4 = j;
		i__1 = j;
		i__5 = (j + 1) * ab_dim1 + 1;
		z__1.r = work[i__1].r * ab[i__5].r - work[i__1].i * ab[i__5]
			.i, z__1.i = work[i__1].r * ab[i__5].i + work[i__1].i 
			* ab[i__5].r;
		work[i__4].r = z__1.r, work[i__4].i = z__1.i;
		i__4 = (j + 1) * ab_dim1 + 1;
		i__1 = j;
		i__5 = (j + 1) * ab_dim1 + 1;
		z__1.r = rwork[i__1] * ab[i__5].r, z__1.i = rwork[i__1] * ab[
			i__5].i;
		ab[i__4].r = z__1.r, ab[i__4].i = z__1.i;
/* L160: */
	    }
	    if (update) {
		if (i__ - k < *n - *ka && k <= kbt) {
		    i__3 = i__ - k + *ka;
		    i__2 = i__ - k;
		    work[i__3].r = work[i__2].r, work[i__3].i = work[i__2].i;
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

		zlargv_(&nr, &ab[j2 * ab_dim1 + 1], &inca, &work[j2], &ka1, &
			rwork[j2], &ka1);

/*              apply rotations in 2nd set from the right */

		i__3 = *ka - 1;
		for (l = 1; l <= i__3; ++l) {
		    zlartv_(&nr, &ab[ka1 - l + j2 * ab_dim1], &inca, &ab[*ka 
			    - l + (j2 + 1) * ab_dim1], &inca, &rwork[j2], &
			    work[j2], &ka1);
/* L180: */
		}

/*              apply rotations in 2nd set from both sides to diagonal */
/*              blocks */

		zlar2v_(&nr, &ab[ka1 + j2 * ab_dim1], &ab[ka1 + (j2 + 1) * 
			ab_dim1], &ab[*ka + (j2 + 1) * ab_dim1], &inca, &
			rwork[j2], &work[j2], &ka1);

		zlacgv_(&nr, &work[j2], &ka1);
	    }

/*           start applying rotations in 2nd set from the left */

	    i__3 = *kb - k + 1;
	    for (l = *ka - 1; l >= i__3; --l) {
		nrt = (*n - j2 + l) / ka1;
		if (nrt > 0) {
		    zlartv_(&nrt, &ab[l + (j2 + ka1 - l) * ab_dim1], &inca, &
			    ab[l + 1 + (j2 + ka1 - l) * ab_dim1], &inca, &
			    rwork[j2], &work[j2], &ka1);
		}
/* L190: */
	    }

	    if (wantx) {

/*              post-multiply X by product of rotations in 2nd set */

		i__3 = j1;
		i__2 = ka1;
		for (j = j2; i__2 < 0 ? j >= i__3 : j <= i__3; j += i__2) {
		    i__4 = *n - m;
		    d_cnjg(&z__1, &work[j]);
		    zrot_(&i__4, &x[m + 1 + j * x_dim1], &c__1, &x[m + 1 + (j 
			    + 1) * x_dim1], &c__1, &rwork[j], &z__1);
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
		    zlartv_(&nrt, &ab[l + (j2 + ka1 - l) * ab_dim1], &inca, &
			    ab[l + 1 + (j2 + ka1 - l) * ab_dim1], &inca, &
			    rwork[j2 - m], &work[j2 - m], &ka1);
		}
/* L220: */
	    }
/* L230: */
	}

	if (*kb > 1) {
	    i__2 = j2 + *ka;
	    for (j = *n - 1; j >= i__2; --j) {
		rwork[j - m] = rwork[j - *ka - m];
		i__3 = j - m;
		i__4 = j - *ka - m;
		work[i__3].r = work[i__4].r, work[i__3].i = work[i__4].i;
/* L240: */
	    }
	}

    } else {

/*        Transform A, working with the lower triangle */

	if (update) {

/*           Form  inv(S(i))**H * A * inv(S(i)) */

	    i__2 = i__ * bb_dim1 + 1;
	    bii = bb[i__2].r;
	    i__2 = i__ * ab_dim1 + 1;
	    i__3 = i__ * ab_dim1 + 1;
	    d__1 = ab[i__3].r / bii / bii;
	    ab[i__2].r = d__1, ab[i__2].i = 0.;
	    i__2 = i1;
	    for (j = i__ + 1; j <= i__2; ++j) {
		i__3 = j - i__ + 1 + i__ * ab_dim1;
		i__4 = j - i__ + 1 + i__ * ab_dim1;
		z__1.r = ab[i__4].r / bii, z__1.i = ab[i__4].i / bii;
		ab[i__3].r = z__1.r, ab[i__3].i = z__1.i;
/* L250: */
	    }
/* Computing MAX */
	    i__2 = 1, i__3 = i__ - *ka;
	    i__4 = i__ - 1;
	    for (j = f2cmax(i__2,i__3); j <= i__4; ++j) {
		i__2 = i__ - j + 1 + j * ab_dim1;
		i__3 = i__ - j + 1 + j * ab_dim1;
		z__1.r = ab[i__3].r / bii, z__1.i = ab[i__3].i / bii;
		ab[i__2].r = z__1.r, ab[i__2].i = z__1.i;
/* L260: */
	    }
	    i__4 = i__ - 1;
	    for (k = i__ - kbt; k <= i__4; ++k) {
		i__2 = k;
		for (j = i__ - kbt; j <= i__2; ++j) {
		    i__3 = k - j + 1 + j * ab_dim1;
		    i__1 = k - j + 1 + j * ab_dim1;
		    i__5 = i__ - j + 1 + j * bb_dim1;
		    d_cnjg(&z__5, &ab[i__ - k + 1 + k * ab_dim1]);
		    z__4.r = bb[i__5].r * z__5.r - bb[i__5].i * z__5.i, 
			    z__4.i = bb[i__5].r * z__5.i + bb[i__5].i * 
			    z__5.r;
		    z__3.r = ab[i__1].r - z__4.r, z__3.i = ab[i__1].i - 
			    z__4.i;
		    d_cnjg(&z__7, &bb[i__ - k + 1 + k * bb_dim1]);
		    i__6 = i__ - j + 1 + j * ab_dim1;
		    z__6.r = z__7.r * ab[i__6].r - z__7.i * ab[i__6].i, 
			    z__6.i = z__7.r * ab[i__6].i + z__7.i * ab[i__6]
			    .r;
		    z__2.r = z__3.r - z__6.r, z__2.i = z__3.i - z__6.i;
		    i__7 = i__ * ab_dim1 + 1;
		    d__1 = ab[i__7].r;
		    i__8 = i__ - j + 1 + j * bb_dim1;
		    z__9.r = d__1 * bb[i__8].r, z__9.i = d__1 * bb[i__8].i;
		    d_cnjg(&z__10, &bb[i__ - k + 1 + k * bb_dim1]);
		    z__8.r = z__9.r * z__10.r - z__9.i * z__10.i, z__8.i = 
			    z__9.r * z__10.i + z__9.i * z__10.r;
		    z__1.r = z__2.r + z__8.r, z__1.i = z__2.i + z__8.i;
		    ab[i__3].r = z__1.r, ab[i__3].i = z__1.i;
/* L270: */
		}
/* Computing MAX */
		i__2 = 1, i__3 = i__ - *ka;
		i__1 = i__ - kbt - 1;
		for (j = f2cmax(i__2,i__3); j <= i__1; ++j) {
		    i__2 = k - j + 1 + j * ab_dim1;
		    i__3 = k - j + 1 + j * ab_dim1;
		    d_cnjg(&z__3, &bb[i__ - k + 1 + k * bb_dim1]);
		    i__5 = i__ - j + 1 + j * ab_dim1;
		    z__2.r = z__3.r * ab[i__5].r - z__3.i * ab[i__5].i, 
			    z__2.i = z__3.r * ab[i__5].i + z__3.i * ab[i__5]
			    .r;
		    z__1.r = ab[i__3].r - z__2.r, z__1.i = ab[i__3].i - 
			    z__2.i;
		    ab[i__2].r = z__1.r, ab[i__2].i = z__1.i;
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
		    i__1 = j - k + 1 + k * ab_dim1;
		    i__2 = j - k + 1 + k * ab_dim1;
		    i__5 = i__ - k + 1 + k * bb_dim1;
		    i__6 = j - i__ + 1 + i__ * ab_dim1;
		    z__2.r = bb[i__5].r * ab[i__6].r - bb[i__5].i * ab[i__6]
			    .i, z__2.i = bb[i__5].r * ab[i__6].i + bb[i__5].i 
			    * ab[i__6].r;
		    z__1.r = ab[i__2].r - z__2.r, z__1.i = ab[i__2].i - 
			    z__2.i;
		    ab[i__1].r = z__1.r, ab[i__1].i = z__1.i;
/* L300: */
		}
/* L310: */
	    }

	    if (wantx) {

/*              post-multiply X by inv(S(i)) */

		i__4 = *n - m;
		d__1 = 1. / bii;
		zdscal_(&i__4, &d__1, &x[m + 1 + i__ * x_dim1], &c__1);
		if (kbt > 0) {
		    i__4 = *n - m;
		    z__1.r = -1., z__1.i = 0.;
		    i__3 = *ldbb - 1;
		    zgeru_(&i__4, &kbt, &z__1, &x[m + 1 + i__ * x_dim1], &
			    c__1, &bb[kbt + 1 + (i__ - kbt) * bb_dim1], &i__3,
			     &x[m + 1 + (i__ - kbt) * x_dim1], ldx);
		}
	    }

/*           store a(i1,i) in RA1 for use in next loop over K */

	    i__4 = i1 - i__ + 1 + i__ * ab_dim1;
	    ra1.r = ab[i__4].r, ra1.i = ab[i__4].i;
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

		    zlartg_(&ab[ka1 - k + i__ * ab_dim1], &ra1, &rwork[i__ - 
			    k + *ka - m], &work[i__ - k + *ka - m], &ra);

/*                 create nonzero element a(i-k+ka+1,i-k) outside the */
/*                 band and store it in WORK(i-k) */

		    i__3 = k + 1 + (i__ - k) * bb_dim1;
		    z__2.r = -bb[i__3].r, z__2.i = -bb[i__3].i;
		    z__1.r = z__2.r * ra1.r - z__2.i * ra1.i, z__1.i = z__2.r 
			    * ra1.i + z__2.i * ra1.r;
		    t.r = z__1.r, t.i = z__1.i;
		    i__3 = i__ - k;
		    i__1 = i__ - k + *ka - m;
		    z__2.r = rwork[i__1] * t.r, z__2.i = rwork[i__1] * t.i;
		    d_cnjg(&z__4, &work[i__ - k + *ka - m]);
		    i__2 = ka1 + (i__ - k) * ab_dim1;
		    z__3.r = z__4.r * ab[i__2].r - z__4.i * ab[i__2].i, 
			    z__3.i = z__4.r * ab[i__2].i + z__4.i * ab[i__2]
			    .r;
		    z__1.r = z__2.r - z__3.r, z__1.i = z__2.i - z__3.i;
		    work[i__3].r = z__1.r, work[i__3].i = z__1.i;
		    i__3 = ka1 + (i__ - k) * ab_dim1;
		    i__1 = i__ - k + *ka - m;
		    z__2.r = work[i__1].r * t.r - work[i__1].i * t.i, z__2.i =
			     work[i__1].r * t.i + work[i__1].i * t.r;
		    i__2 = i__ - k + *ka - m;
		    i__5 = ka1 + (i__ - k) * ab_dim1;
		    z__3.r = rwork[i__2] * ab[i__5].r, z__3.i = rwork[i__2] * 
			    ab[i__5].i;
		    z__1.r = z__2.r + z__3.r, z__1.i = z__2.i + z__3.i;
		    ab[i__3].r = z__1.r, ab[i__3].i = z__1.i;
		    ra1.r = ra.r, ra1.i = ra.i;
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

		i__2 = j - m;
		i__5 = j - m;
		i__6 = ka1 + (j - *ka + 1) * ab_dim1;
		z__1.r = work[i__5].r * ab[i__6].r - work[i__5].i * ab[i__6]
			.i, z__1.i = work[i__5].r * ab[i__6].i + work[i__5].i 
			* ab[i__6].r;
		work[i__2].r = z__1.r, work[i__2].i = z__1.i;
		i__2 = ka1 + (j - *ka + 1) * ab_dim1;
		i__5 = j - m;
		i__6 = ka1 + (j - *ka + 1) * ab_dim1;
		z__1.r = rwork[i__5] * ab[i__6].r, z__1.i = rwork[i__5] * ab[
			i__6].i;
		ab[i__2].r = z__1.r, ab[i__2].i = z__1.i;
/* L320: */
	    }

/*           generate rotations in 1st set to annihilate elements which */
/*           have been created outside the band */

	    if (nrt > 0) {
		zlargv_(&nrt, &ab[ka1 + (j2t - *ka) * ab_dim1], &inca, &work[
			j2t - m], &ka1, &rwork[j2t - m], &ka1);
	    }
	    if (nr > 0) {

/*              apply rotations in 1st set from the left */

		i__1 = *ka - 1;
		for (l = 1; l <= i__1; ++l) {
		    zlartv_(&nr, &ab[l + 1 + (j2 - l) * ab_dim1], &inca, &ab[
			    l + 2 + (j2 - l) * ab_dim1], &inca, &rwork[j2 - m]
			    , &work[j2 - m], &ka1);
/* L330: */
		}

/*              apply rotations in 1st set from both sides to diagonal */
/*              blocks */

		zlar2v_(&nr, &ab[j2 * ab_dim1 + 1], &ab[(j2 + 1) * ab_dim1 + 
			1], &ab[j2 * ab_dim1 + 2], &inca, &rwork[j2 - m], &
			work[j2 - m], &ka1);

		zlacgv_(&nr, &work[j2 - m], &ka1);
	    }

/*           start applying rotations in 1st set from the right */

	    i__1 = *kb - k + 1;
	    for (l = *ka - 1; l >= i__1; --l) {
		nrt = (*n - j2 + l) / ka1;
		if (nrt > 0) {
		    zlartv_(&nrt, &ab[ka1 - l + 1 + j2 * ab_dim1], &inca, &ab[
			    ka1 - l + (j2 + 1) * ab_dim1], &inca, &rwork[j2 - 
			    m], &work[j2 - m], &ka1);
		}
/* L340: */
	    }

	    if (wantx) {

/*              post-multiply X by product of rotations in 1st set */

		i__1 = j1;
		i__3 = ka1;
		for (j = j2; i__3 < 0 ? j >= i__1 : j <= i__1; j += i__3) {
		    i__2 = *n - m;
		    zrot_(&i__2, &x[m + 1 + j * x_dim1], &c__1, &x[m + 1 + (j 
			    + 1) * x_dim1], &c__1, &rwork[j - m], &work[j - m]
			    );
/* L350: */
		}
	    }
/* L360: */
	}

	if (update) {
	    if (i2 <= *n && kbt > 0) {

/*              create nonzero element a(i-kbt+ka+1,i-kbt) outside the */
/*              band and store it in WORK(i-kbt) */

		i__4 = i__ - kbt;
		i__3 = kbt + 1 + (i__ - kbt) * bb_dim1;
		z__2.r = -bb[i__3].r, z__2.i = -bb[i__3].i;
		z__1.r = z__2.r * ra1.r - z__2.i * ra1.i, z__1.i = z__2.r * 
			ra1.i + z__2.i * ra1.r;
		work[i__4].r = z__1.r, work[i__4].i = z__1.i;
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
		    zlartv_(&nrt, &ab[ka1 - l + 1 + (j2 - *ka) * ab_dim1], &
			    inca, &ab[ka1 - l + (j2 - *ka + 1) * ab_dim1], &
			    inca, &rwork[j2 - *ka], &work[j2 - *ka], &ka1);
		}
/* L370: */
	    }
	    nr = (*n - j2 + *ka) / ka1;
	    j1 = j2 + (nr - 1) * ka1;
	    i__4 = j2;
	    i__3 = -ka1;
	    for (j = j1; i__3 < 0 ? j >= i__4 : j <= i__4; j += i__3) {
		i__1 = j;
		i__2 = j - *ka;
		work[i__1].r = work[i__2].r, work[i__1].i = work[i__2].i;
		rwork[j] = rwork[j - *ka];
/* L380: */
	    }
	    i__3 = j1;
	    i__4 = ka1;
	    for (j = j2; i__4 < 0 ? j >= i__3 : j <= i__3; j += i__4) {

/*              create nonzero element a(j+1,j-ka) outside the band */
/*              and store it in WORK(j) */

		i__1 = j;
		i__2 = j;
		i__5 = ka1 + (j - *ka + 1) * ab_dim1;
		z__1.r = work[i__2].r * ab[i__5].r - work[i__2].i * ab[i__5]
			.i, z__1.i = work[i__2].r * ab[i__5].i + work[i__2].i 
			* ab[i__5].r;
		work[i__1].r = z__1.r, work[i__1].i = z__1.i;
		i__1 = ka1 + (j - *ka + 1) * ab_dim1;
		i__2 = j;
		i__5 = ka1 + (j - *ka + 1) * ab_dim1;
		z__1.r = rwork[i__2] * ab[i__5].r, z__1.i = rwork[i__2] * ab[
			i__5].i;
		ab[i__1].r = z__1.r, ab[i__1].i = z__1.i;
/* L390: */
	    }
	    if (update) {
		if (i__ - k < *n - *ka && k <= kbt) {
		    i__4 = i__ - k + *ka;
		    i__3 = i__ - k;
		    work[i__4].r = work[i__3].r, work[i__4].i = work[i__3].i;
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

		zlargv_(&nr, &ab[ka1 + (j2 - *ka) * ab_dim1], &inca, &work[j2]
			, &ka1, &rwork[j2], &ka1);

/*              apply rotations in 2nd set from the left */

		i__4 = *ka - 1;
		for (l = 1; l <= i__4; ++l) {
		    zlartv_(&nr, &ab[l + 1 + (j2 - l) * ab_dim1], &inca, &ab[
			    l + 2 + (j2 - l) * ab_dim1], &inca, &rwork[j2], &
			    work[j2], &ka1);
/* L410: */
		}

/*              apply rotations in 2nd set from both sides to diagonal */
/*              blocks */

		zlar2v_(&nr, &ab[j2 * ab_dim1 + 1], &ab[(j2 + 1) * ab_dim1 + 
			1], &ab[j2 * ab_dim1 + 2], &inca, &rwork[j2], &work[
			j2], &ka1);

		zlacgv_(&nr, &work[j2], &ka1);
	    }

/*           start applying rotations in 2nd set from the right */

	    i__4 = *kb - k + 1;
	    for (l = *ka - 1; l >= i__4; --l) {
		nrt = (*n - j2 + l) / ka1;
		if (nrt > 0) {
		    zlartv_(&nrt, &ab[ka1 - l + 1 + j2 * ab_dim1], &inca, &ab[
			    ka1 - l + (j2 + 1) * ab_dim1], &inca, &rwork[j2], 
			    &work[j2], &ka1);
		}
/* L420: */
	    }

	    if (wantx) {

/*              post-multiply X by product of rotations in 2nd set */

		i__4 = j1;
		i__3 = ka1;
		for (j = j2; i__3 < 0 ? j >= i__4 : j <= i__4; j += i__3) {
		    i__1 = *n - m;
		    zrot_(&i__1, &x[m + 1 + j * x_dim1], &c__1, &x[m + 1 + (j 
			    + 1) * x_dim1], &c__1, &rwork[j], &work[j]);
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
		    zlartv_(&nrt, &ab[ka1 - l + 1 + j2 * ab_dim1], &inca, &ab[
			    ka1 - l + (j2 + 1) * ab_dim1], &inca, &rwork[j2 - 
			    m], &work[j2 - m], &ka1);
		}
/* L450: */
	    }
/* L460: */
	}

	if (*kb > 1) {
	    i__3 = j2 + *ka;
	    for (j = *n - 1; j >= i__3; --j) {
		rwork[j - m] = rwork[j - *ka - m];
		i__4 = j - m;
		i__1 = j - *ka - m;
		work[i__4].r = work[i__1].r, work[i__4].i = work[i__1].i;
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

/*           Form  inv(S(i))**H * A * inv(S(i)) */

	    i__3 = kb1 + i__ * bb_dim1;
	    bii = bb[i__3].r;
	    i__3 = ka1 + i__ * ab_dim1;
	    i__4 = ka1 + i__ * ab_dim1;
	    d__1 = ab[i__4].r / bii / bii;
	    ab[i__3].r = d__1, ab[i__3].i = 0.;
	    i__3 = i__ - 1;
	    for (j = i1; j <= i__3; ++j) {
		i__4 = j - i__ + ka1 + i__ * ab_dim1;
		i__1 = j - i__ + ka1 + i__ * ab_dim1;
		z__1.r = ab[i__1].r / bii, z__1.i = ab[i__1].i / bii;
		ab[i__4].r = z__1.r, ab[i__4].i = z__1.i;
/* L500: */
	    }
/* Computing MIN */
	    i__4 = *n, i__1 = i__ + *ka;
	    i__3 = f2cmin(i__4,i__1);
	    for (j = i__ + 1; j <= i__3; ++j) {
		i__4 = i__ - j + ka1 + j * ab_dim1;
		i__1 = i__ - j + ka1 + j * ab_dim1;
		z__1.r = ab[i__1].r / bii, z__1.i = ab[i__1].i / bii;
		ab[i__4].r = z__1.r, ab[i__4].i = z__1.i;
/* L510: */
	    }
	    i__3 = i__ + kbt;
	    for (k = i__ + 1; k <= i__3; ++k) {
		i__4 = i__ + kbt;
		for (j = k; j <= i__4; ++j) {
		    i__1 = k - j + ka1 + j * ab_dim1;
		    i__2 = k - j + ka1 + j * ab_dim1;
		    i__5 = i__ - j + kb1 + j * bb_dim1;
		    d_cnjg(&z__5, &ab[i__ - k + ka1 + k * ab_dim1]);
		    z__4.r = bb[i__5].r * z__5.r - bb[i__5].i * z__5.i, 
			    z__4.i = bb[i__5].r * z__5.i + bb[i__5].i * 
			    z__5.r;
		    z__3.r = ab[i__2].r - z__4.r, z__3.i = ab[i__2].i - 
			    z__4.i;
		    d_cnjg(&z__7, &bb[i__ - k + kb1 + k * bb_dim1]);
		    i__6 = i__ - j + ka1 + j * ab_dim1;
		    z__6.r = z__7.r * ab[i__6].r - z__7.i * ab[i__6].i, 
			    z__6.i = z__7.r * ab[i__6].i + z__7.i * ab[i__6]
			    .r;
		    z__2.r = z__3.r - z__6.r, z__2.i = z__3.i - z__6.i;
		    i__7 = ka1 + i__ * ab_dim1;
		    d__1 = ab[i__7].r;
		    i__8 = i__ - j + kb1 + j * bb_dim1;
		    z__9.r = d__1 * bb[i__8].r, z__9.i = d__1 * bb[i__8].i;
		    d_cnjg(&z__10, &bb[i__ - k + kb1 + k * bb_dim1]);
		    z__8.r = z__9.r * z__10.r - z__9.i * z__10.i, z__8.i = 
			    z__9.r * z__10.i + z__9.i * z__10.r;
		    z__1.r = z__2.r + z__8.r, z__1.i = z__2.i + z__8.i;
		    ab[i__1].r = z__1.r, ab[i__1].i = z__1.i;
/* L520: */
		}
/* Computing MIN */
		i__1 = *n, i__2 = i__ + *ka;
		i__4 = f2cmin(i__1,i__2);
		for (j = i__ + kbt + 1; j <= i__4; ++j) {
		    i__1 = k - j + ka1 + j * ab_dim1;
		    i__2 = k - j + ka1 + j * ab_dim1;
		    d_cnjg(&z__3, &bb[i__ - k + kb1 + k * bb_dim1]);
		    i__5 = i__ - j + ka1 + j * ab_dim1;
		    z__2.r = z__3.r * ab[i__5].r - z__3.i * ab[i__5].i, 
			    z__2.i = z__3.r * ab[i__5].i + z__3.i * ab[i__5]
			    .r;
		    z__1.r = ab[i__2].r - z__2.r, z__1.i = ab[i__2].i - 
			    z__2.i;
		    ab[i__1].r = z__1.r, ab[i__1].i = z__1.i;
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
		    i__1 = j - k + ka1 + k * ab_dim1;
		    i__2 = j - k + ka1 + k * ab_dim1;
		    i__5 = i__ - k + kb1 + k * bb_dim1;
		    i__6 = j - i__ + ka1 + i__ * ab_dim1;
		    z__2.r = bb[i__5].r * ab[i__6].r - bb[i__5].i * ab[i__6]
			    .i, z__2.i = bb[i__5].r * ab[i__6].i + bb[i__5].i 
			    * ab[i__6].r;
		    z__1.r = ab[i__2].r - z__2.r, z__1.i = ab[i__2].i - 
			    z__2.i;
		    ab[i__1].r = z__1.r, ab[i__1].i = z__1.i;
/* L550: */
		}
/* L560: */
	    }

	    if (wantx) {

/*              post-multiply X by inv(S(i)) */

		d__1 = 1. / bii;
		zdscal_(&nx, &d__1, &x[i__ * x_dim1 + 1], &c__1);
		if (kbt > 0) {
		    z__1.r = -1., z__1.i = 0.;
		    i__3 = *ldbb - 1;
		    zgeru_(&nx, &kbt, &z__1, &x[i__ * x_dim1 + 1], &c__1, &bb[
			    *kb + (i__ + 1) * bb_dim1], &i__3, &x[(i__ + 1) * 
			    x_dim1 + 1], ldx);
		}
	    }

/*           store a(i1,i) in RA1 for use in next loop over K */

	    i__3 = i1 - i__ + ka1 + i__ * ab_dim1;
	    ra1.r = ab[i__3].r, ra1.i = ab[i__3].i;
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

		    zlartg_(&ab[k + 1 + i__ * ab_dim1], &ra1, &rwork[i__ + k 
			    - *ka], &work[i__ + k - *ka], &ra);

/*                 create nonzero element a(i+k-ka-1,i+k) outside the */
/*                 band and store it in WORK(m-kb+i+k) */

		    i__4 = kb1 - k + (i__ + k) * bb_dim1;
		    z__2.r = -bb[i__4].r, z__2.i = -bb[i__4].i;
		    z__1.r = z__2.r * ra1.r - z__2.i * ra1.i, z__1.i = z__2.r 
			    * ra1.i + z__2.i * ra1.r;
		    t.r = z__1.r, t.i = z__1.i;
		    i__4 = m - *kb + i__ + k;
		    i__1 = i__ + k - *ka;
		    z__2.r = rwork[i__1] * t.r, z__2.i = rwork[i__1] * t.i;
		    d_cnjg(&z__4, &work[i__ + k - *ka]);
		    i__2 = (i__ + k) * ab_dim1 + 1;
		    z__3.r = z__4.r * ab[i__2].r - z__4.i * ab[i__2].i, 
			    z__3.i = z__4.r * ab[i__2].i + z__4.i * ab[i__2]
			    .r;
		    z__1.r = z__2.r - z__3.r, z__1.i = z__2.i - z__3.i;
		    work[i__4].r = z__1.r, work[i__4].i = z__1.i;
		    i__4 = (i__ + k) * ab_dim1 + 1;
		    i__1 = i__ + k - *ka;
		    z__2.r = work[i__1].r * t.r - work[i__1].i * t.i, z__2.i =
			     work[i__1].r * t.i + work[i__1].i * t.r;
		    i__2 = i__ + k - *ka;
		    i__5 = (i__ + k) * ab_dim1 + 1;
		    z__3.r = rwork[i__2] * ab[i__5].r, z__3.i = rwork[i__2] * 
			    ab[i__5].i;
		    z__1.r = z__2.r + z__3.r, z__1.i = z__2.i + z__3.i;
		    ab[i__4].r = z__1.r, ab[i__4].i = z__1.i;
		    ra1.r = ra.r, ra1.i = ra.i;
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

		i__2 = j;
		i__5 = j;
		i__6 = (j + *ka - 1) * ab_dim1 + 1;
		z__1.r = work[i__5].r * ab[i__6].r - work[i__5].i * ab[i__6]
			.i, z__1.i = work[i__5].r * ab[i__6].i + work[i__5].i 
			* ab[i__6].r;
		work[i__2].r = z__1.r, work[i__2].i = z__1.i;
		i__2 = (j + *ka - 1) * ab_dim1 + 1;
		i__5 = j;
		i__6 = (j + *ka - 1) * ab_dim1 + 1;
		z__1.r = rwork[i__5] * ab[i__6].r, z__1.i = rwork[i__5] * ab[
			i__6].i;
		ab[i__2].r = z__1.r, ab[i__2].i = z__1.i;
/* L570: */
	    }

/*           generate rotations in 1st set to annihilate elements which */
/*           have been created outside the band */

	    if (nrt > 0) {
		zlargv_(&nrt, &ab[(j1 + *ka) * ab_dim1 + 1], &inca, &work[j1],
			 &ka1, &rwork[j1], &ka1);
	    }
	    if (nr > 0) {

/*              apply rotations in 1st set from the left */

		i__1 = *ka - 1;
		for (l = 1; l <= i__1; ++l) {
		    zlartv_(&nr, &ab[ka1 - l + (j1 + l) * ab_dim1], &inca, &
			    ab[*ka - l + (j1 + l) * ab_dim1], &inca, &rwork[
			    j1], &work[j1], &ka1);
/* L580: */
		}

/*              apply rotations in 1st set from both sides to diagonal */
/*              blocks */

		zlar2v_(&nr, &ab[ka1 + j1 * ab_dim1], &ab[ka1 + (j1 - 1) * 
			ab_dim1], &ab[*ka + j1 * ab_dim1], &inca, &rwork[j1], 
			&work[j1], &ka1);

		zlacgv_(&nr, &work[j1], &ka1);
	    }

/*           start applying rotations in 1st set from the right */

	    i__1 = *kb - k + 1;
	    for (l = *ka - 1; l >= i__1; --l) {
		nrt = (j2 + l - 1) / ka1;
		j1t = j2 - (nrt - 1) * ka1;
		if (nrt > 0) {
		    zlartv_(&nrt, &ab[l + j1t * ab_dim1], &inca, &ab[l + 1 + (
			    j1t - 1) * ab_dim1], &inca, &rwork[j1t], &work[
			    j1t], &ka1);
		}
/* L590: */
	    }

	    if (wantx) {

/*              post-multiply X by product of rotations in 1st set */

		i__1 = j2;
		i__4 = ka1;
		for (j = j1; i__4 < 0 ? j >= i__1 : j <= i__1; j += i__4) {
		    zrot_(&nx, &x[j * x_dim1 + 1], &c__1, &x[(j - 1) * x_dim1 
			    + 1], &c__1, &rwork[j], &work[j]);
/* L600: */
		}
	    }
/* L610: */
	}

	if (update) {
	    if (i2 > 0 && kbt > 0) {

/*              create nonzero element a(i+kbt-ka-1,i+kbt) outside the */
/*              band and store it in WORK(m-kb+i+kbt) */

		i__3 = m - *kb + i__ + kbt;
		i__4 = kb1 - kbt + (i__ + kbt) * bb_dim1;
		z__2.r = -bb[i__4].r, z__2.i = -bb[i__4].i;
		z__1.r = z__2.r * ra1.r - z__2.i * ra1.i, z__1.i = z__2.r * 
			ra1.i + z__2.i * ra1.r;
		work[i__3].r = z__1.r, work[i__3].i = z__1.i;
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
		    zlartv_(&nrt, &ab[l + (j1t + *ka) * ab_dim1], &inca, &ab[
			    l + 1 + (j1t + *ka - 1) * ab_dim1], &inca, &rwork[
			    m - *kb + j1t + *ka], &work[m - *kb + j1t + *ka], 
			    &ka1);
		}
/* L620: */
	    }
	    nr = (j2 + *ka - 1) / ka1;
	    j1 = j2 - (nr - 1) * ka1;
	    i__3 = j2;
	    i__4 = ka1;
	    for (j = j1; i__4 < 0 ? j >= i__3 : j <= i__3; j += i__4) {
		i__1 = m - *kb + j;
		i__2 = m - *kb + j + *ka;
		work[i__1].r = work[i__2].r, work[i__1].i = work[i__2].i;
		rwork[m - *kb + j] = rwork[m - *kb + j + *ka];
/* L630: */
	    }
	    i__4 = j2;
	    i__3 = ka1;
	    for (j = j1; i__3 < 0 ? j >= i__4 : j <= i__4; j += i__3) {

/*              create nonzero element a(j-1,j+ka) outside the band */
/*              and store it in WORK(m-kb+j) */

		i__1 = m - *kb + j;
		i__2 = m - *kb + j;
		i__5 = (j + *ka - 1) * ab_dim1 + 1;
		z__1.r = work[i__2].r * ab[i__5].r - work[i__2].i * ab[i__5]
			.i, z__1.i = work[i__2].r * ab[i__5].i + work[i__2].i 
			* ab[i__5].r;
		work[i__1].r = z__1.r, work[i__1].i = z__1.i;
		i__1 = (j + *ka - 1) * ab_dim1 + 1;
		i__2 = m - *kb + j;
		i__5 = (j + *ka - 1) * ab_dim1 + 1;
		z__1.r = rwork[i__2] * ab[i__5].r, z__1.i = rwork[i__2] * ab[
			i__5].i;
		ab[i__1].r = z__1.r, ab[i__1].i = z__1.i;
/* L640: */
	    }
	    if (update) {
		if (i__ + k > ka1 && k <= kbt) {
		    i__3 = m - *kb + i__ + k - *ka;
		    i__4 = m - *kb + i__ + k;
		    work[i__3].r = work[i__4].r, work[i__3].i = work[i__4].i;
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

		zlargv_(&nr, &ab[(j1 + *ka) * ab_dim1 + 1], &inca, &work[m - *
			kb + j1], &ka1, &rwork[m - *kb + j1], &ka1);

/*              apply rotations in 2nd set from the left */

		i__3 = *ka - 1;
		for (l = 1; l <= i__3; ++l) {
		    zlartv_(&nr, &ab[ka1 - l + (j1 + l) * ab_dim1], &inca, &
			    ab[*ka - l + (j1 + l) * ab_dim1], &inca, &rwork[m 
			    - *kb + j1], &work[m - *kb + j1], &ka1);
/* L660: */
		}

/*              apply rotations in 2nd set from both sides to diagonal */
/*              blocks */

		zlar2v_(&nr, &ab[ka1 + j1 * ab_dim1], &ab[ka1 + (j1 - 1) * 
			ab_dim1], &ab[*ka + j1 * ab_dim1], &inca, &rwork[m - *
			kb + j1], &work[m - *kb + j1], &ka1);

		zlacgv_(&nr, &work[m - *kb + j1], &ka1);
	    }

/*           start applying rotations in 2nd set from the right */

	    i__3 = *kb - k + 1;
	    for (l = *ka - 1; l >= i__3; --l) {
		nrt = (j2 + l - 1) / ka1;
		j1t = j2 - (nrt - 1) * ka1;
		if (nrt > 0) {
		    zlartv_(&nrt, &ab[l + j1t * ab_dim1], &inca, &ab[l + 1 + (
			    j1t - 1) * ab_dim1], &inca, &rwork[m - *kb + j1t],
			     &work[m - *kb + j1t], &ka1);
		}
/* L670: */
	    }

	    if (wantx) {

/*              post-multiply X by product of rotations in 2nd set */

		i__3 = j2;
		i__4 = ka1;
		for (j = j1; i__4 < 0 ? j >= i__3 : j <= i__3; j += i__4) {
		    zrot_(&nx, &x[j * x_dim1 + 1], &c__1, &x[(j - 1) * x_dim1 
			    + 1], &c__1, &rwork[m - *kb + j], &work[m - *kb + 
			    j]);
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
		    zlartv_(&nrt, &ab[l + j1t * ab_dim1], &inca, &ab[l + 1 + (
			    j1t - 1) * ab_dim1], &inca, &rwork[j1t], &work[
			    j1t], &ka1);
		}
/* L700: */
	    }
/* L710: */
	}

	if (*kb > 1) {
	    i__4 = i2 - *ka;
	    for (j = 2; j <= i__4; ++j) {
		rwork[j] = rwork[j + *ka];
		i__3 = j;
		i__1 = j + *ka;
		work[i__3].r = work[i__1].r, work[i__3].i = work[i__1].i;
/* L720: */
	    }
	}

    } else {

/*        Transform A, working with the lower triangle */

	if (update) {

/*           Form  inv(S(i))**H * A * inv(S(i)) */

	    i__4 = i__ * bb_dim1 + 1;
	    bii = bb[i__4].r;
	    i__4 = i__ * ab_dim1 + 1;
	    i__3 = i__ * ab_dim1 + 1;
	    d__1 = ab[i__3].r / bii / bii;
	    ab[i__4].r = d__1, ab[i__4].i = 0.;
	    i__4 = i__ - 1;
	    for (j = i1; j <= i__4; ++j) {
		i__3 = i__ - j + 1 + j * ab_dim1;
		i__1 = i__ - j + 1 + j * ab_dim1;
		z__1.r = ab[i__1].r / bii, z__1.i = ab[i__1].i / bii;
		ab[i__3].r = z__1.r, ab[i__3].i = z__1.i;
/* L730: */
	    }
/* Computing MIN */
	    i__3 = *n, i__1 = i__ + *ka;
	    i__4 = f2cmin(i__3,i__1);
	    for (j = i__ + 1; j <= i__4; ++j) {
		i__3 = j - i__ + 1 + i__ * ab_dim1;
		i__1 = j - i__ + 1 + i__ * ab_dim1;
		z__1.r = ab[i__1].r / bii, z__1.i = ab[i__1].i / bii;
		ab[i__3].r = z__1.r, ab[i__3].i = z__1.i;
/* L740: */
	    }
	    i__4 = i__ + kbt;
	    for (k = i__ + 1; k <= i__4; ++k) {
		i__3 = i__ + kbt;
		for (j = k; j <= i__3; ++j) {
		    i__1 = j - k + 1 + k * ab_dim1;
		    i__2 = j - k + 1 + k * ab_dim1;
		    i__5 = j - i__ + 1 + i__ * bb_dim1;
		    d_cnjg(&z__5, &ab[k - i__ + 1 + i__ * ab_dim1]);
		    z__4.r = bb[i__5].r * z__5.r - bb[i__5].i * z__5.i, 
			    z__4.i = bb[i__5].r * z__5.i + bb[i__5].i * 
			    z__5.r;
		    z__3.r = ab[i__2].r - z__4.r, z__3.i = ab[i__2].i - 
			    z__4.i;
		    d_cnjg(&z__7, &bb[k - i__ + 1 + i__ * bb_dim1]);
		    i__6 = j - i__ + 1 + i__ * ab_dim1;
		    z__6.r = z__7.r * ab[i__6].r - z__7.i * ab[i__6].i, 
			    z__6.i = z__7.r * ab[i__6].i + z__7.i * ab[i__6]
			    .r;
		    z__2.r = z__3.r - z__6.r, z__2.i = z__3.i - z__6.i;
		    i__7 = i__ * ab_dim1 + 1;
		    d__1 = ab[i__7].r;
		    i__8 = j - i__ + 1 + i__ * bb_dim1;
		    z__9.r = d__1 * bb[i__8].r, z__9.i = d__1 * bb[i__8].i;
		    d_cnjg(&z__10, &bb[k - i__ + 1 + i__ * bb_dim1]);
		    z__8.r = z__9.r * z__10.r - z__9.i * z__10.i, z__8.i = 
			    z__9.r * z__10.i + z__9.i * z__10.r;
		    z__1.r = z__2.r + z__8.r, z__1.i = z__2.i + z__8.i;
		    ab[i__1].r = z__1.r, ab[i__1].i = z__1.i;
/* L750: */
		}
/* Computing MIN */
		i__1 = *n, i__2 = i__ + *ka;
		i__3 = f2cmin(i__1,i__2);
		for (j = i__ + kbt + 1; j <= i__3; ++j) {
		    i__1 = j - k + 1 + k * ab_dim1;
		    i__2 = j - k + 1 + k * ab_dim1;
		    d_cnjg(&z__3, &bb[k - i__ + 1 + i__ * bb_dim1]);
		    i__5 = j - i__ + 1 + i__ * ab_dim1;
		    z__2.r = z__3.r * ab[i__5].r - z__3.i * ab[i__5].i, 
			    z__2.i = z__3.r * ab[i__5].i + z__3.i * ab[i__5]
			    .r;
		    z__1.r = ab[i__2].r - z__2.r, z__1.i = ab[i__2].i - 
			    z__2.i;
		    ab[i__1].r = z__1.r, ab[i__1].i = z__1.i;
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
		    i__1 = k - j + 1 + j * ab_dim1;
		    i__2 = k - j + 1 + j * ab_dim1;
		    i__5 = k - i__ + 1 + i__ * bb_dim1;
		    i__6 = i__ - j + 1 + j * ab_dim1;
		    z__2.r = bb[i__5].r * ab[i__6].r - bb[i__5].i * ab[i__6]
			    .i, z__2.i = bb[i__5].r * ab[i__6].i + bb[i__5].i 
			    * ab[i__6].r;
		    z__1.r = ab[i__2].r - z__2.r, z__1.i = ab[i__2].i - 
			    z__2.i;
		    ab[i__1].r = z__1.r, ab[i__1].i = z__1.i;
/* L780: */
		}
/* L790: */
	    }

	    if (wantx) {

/*              post-multiply X by inv(S(i)) */

		d__1 = 1. / bii;
		zdscal_(&nx, &d__1, &x[i__ * x_dim1 + 1], &c__1);
		if (kbt > 0) {
		    z__1.r = -1., z__1.i = 0.;
		    zgerc_(&nx, &kbt, &z__1, &x[i__ * x_dim1 + 1], &c__1, &bb[
			    i__ * bb_dim1 + 2], &c__1, &x[(i__ + 1) * x_dim1 
			    + 1], ldx);
		}
	    }

/*           store a(i,i1) in RA1 for use in next loop over K */

	    i__4 = i__ - i1 + 1 + i1 * ab_dim1;
	    ra1.r = ab[i__4].r, ra1.i = ab[i__4].i;
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

		    zlartg_(&ab[ka1 - k + (i__ + k - *ka) * ab_dim1], &ra1, &
			    rwork[i__ + k - *ka], &work[i__ + k - *ka], &ra);

/*                 create nonzero element a(i+k,i+k-ka-1) outside the */
/*                 band and store it in WORK(m-kb+i+k) */

		    i__3 = k + 1 + i__ * bb_dim1;
		    z__2.r = -bb[i__3].r, z__2.i = -bb[i__3].i;
		    z__1.r = z__2.r * ra1.r - z__2.i * ra1.i, z__1.i = z__2.r 
			    * ra1.i + z__2.i * ra1.r;
		    t.r = z__1.r, t.i = z__1.i;
		    i__3 = m - *kb + i__ + k;
		    i__1 = i__ + k - *ka;
		    z__2.r = rwork[i__1] * t.r, z__2.i = rwork[i__1] * t.i;
		    d_cnjg(&z__4, &work[i__ + k - *ka]);
		    i__2 = ka1 + (i__ + k - *ka) * ab_dim1;
		    z__3.r = z__4.r * ab[i__2].r - z__4.i * ab[i__2].i, 
			    z__3.i = z__4.r * ab[i__2].i + z__4.i * ab[i__2]
			    .r;
		    z__1.r = z__2.r - z__3.r, z__1.i = z__2.i - z__3.i;
		    work[i__3].r = z__1.r, work[i__3].i = z__1.i;
		    i__3 = ka1 + (i__ + k - *ka) * ab_dim1;
		    i__1 = i__ + k - *ka;
		    z__2.r = work[i__1].r * t.r - work[i__1].i * t.i, z__2.i =
			     work[i__1].r * t.i + work[i__1].i * t.r;
		    i__2 = i__ + k - *ka;
		    i__5 = ka1 + (i__ + k - *ka) * ab_dim1;
		    z__3.r = rwork[i__2] * ab[i__5].r, z__3.i = rwork[i__2] * 
			    ab[i__5].i;
		    z__1.r = z__2.r + z__3.r, z__1.i = z__2.i + z__3.i;
		    ab[i__3].r = z__1.r, ab[i__3].i = z__1.i;
		    ra1.r = ra.r, ra1.i = ra.i;
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

		i__2 = j;
		i__5 = j;
		i__6 = ka1 + (j - 1) * ab_dim1;
		z__1.r = work[i__5].r * ab[i__6].r - work[i__5].i * ab[i__6]
			.i, z__1.i = work[i__5].r * ab[i__6].i + work[i__5].i 
			* ab[i__6].r;
		work[i__2].r = z__1.r, work[i__2].i = z__1.i;
		i__2 = ka1 + (j - 1) * ab_dim1;
		i__5 = j;
		i__6 = ka1 + (j - 1) * ab_dim1;
		z__1.r = rwork[i__5] * ab[i__6].r, z__1.i = rwork[i__5] * ab[
			i__6].i;
		ab[i__2].r = z__1.r, ab[i__2].i = z__1.i;
/* L800: */
	    }

/*           generate rotations in 1st set to annihilate elements which */
/*           have been created outside the band */

	    if (nrt > 0) {
		zlargv_(&nrt, &ab[ka1 + j1 * ab_dim1], &inca, &work[j1], &ka1,
			 &rwork[j1], &ka1);
	    }
	    if (nr > 0) {

/*              apply rotations in 1st set from the right */

		i__1 = *ka - 1;
		for (l = 1; l <= i__1; ++l) {
		    zlartv_(&nr, &ab[l + 1 + j1 * ab_dim1], &inca, &ab[l + 2 
			    + (j1 - 1) * ab_dim1], &inca, &rwork[j1], &work[
			    j1], &ka1);
/* L810: */
		}

/*              apply rotations in 1st set from both sides to diagonal */
/*              blocks */

		zlar2v_(&nr, &ab[j1 * ab_dim1 + 1], &ab[(j1 - 1) * ab_dim1 + 
			1], &ab[(j1 - 1) * ab_dim1 + 2], &inca, &rwork[j1], &
			work[j1], &ka1);

		zlacgv_(&nr, &work[j1], &ka1);
	    }

/*           start applying rotations in 1st set from the left */

	    i__1 = *kb - k + 1;
	    for (l = *ka - 1; l >= i__1; --l) {
		nrt = (j2 + l - 1) / ka1;
		j1t = j2 - (nrt - 1) * ka1;
		if (nrt > 0) {
		    zlartv_(&nrt, &ab[ka1 - l + 1 + (j1t - ka1 + l) * ab_dim1]
			    , &inca, &ab[ka1 - l + (j1t - ka1 + l) * ab_dim1],
			     &inca, &rwork[j1t], &work[j1t], &ka1);
		}
/* L820: */
	    }

	    if (wantx) {

/*              post-multiply X by product of rotations in 1st set */

		i__1 = j2;
		i__3 = ka1;
		for (j = j1; i__3 < 0 ? j >= i__1 : j <= i__1; j += i__3) {
		    d_cnjg(&z__1, &work[j]);
		    zrot_(&nx, &x[j * x_dim1 + 1], &c__1, &x[(j - 1) * x_dim1 
			    + 1], &c__1, &rwork[j], &z__1);
/* L830: */
		}
	    }
/* L840: */
	}

	if (update) {
	    if (i2 > 0 && kbt > 0) {

/*              create nonzero element a(i+kbt,i+kbt-ka-1) outside the */
/*              band and store it in WORK(m-kb+i+kbt) */

		i__4 = m - *kb + i__ + kbt;
		i__3 = kbt + 1 + i__ * bb_dim1;
		z__2.r = -bb[i__3].r, z__2.i = -bb[i__3].i;
		z__1.r = z__2.r * ra1.r - z__2.i * ra1.i, z__1.i = z__2.r * 
			ra1.i + z__2.i * ra1.r;
		work[i__4].r = z__1.r, work[i__4].i = z__1.i;
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
		    zlartv_(&nrt, &ab[ka1 - l + 1 + (j1t + l - 1) * ab_dim1], 
			    &inca, &ab[ka1 - l + (j1t + l - 1) * ab_dim1], &
			    inca, &rwork[m - *kb + j1t + *ka], &work[m - *kb 
			    + j1t + *ka], &ka1);
		}
/* L850: */
	    }
	    nr = (j2 + *ka - 1) / ka1;
	    j1 = j2 - (nr - 1) * ka1;
	    i__4 = j2;
	    i__3 = ka1;
	    for (j = j1; i__3 < 0 ? j >= i__4 : j <= i__4; j += i__3) {
		i__1 = m - *kb + j;
		i__2 = m - *kb + j + *ka;
		work[i__1].r = work[i__2].r, work[i__1].i = work[i__2].i;
		rwork[m - *kb + j] = rwork[m - *kb + j + *ka];
/* L860: */
	    }
	    i__3 = j2;
	    i__4 = ka1;
	    for (j = j1; i__4 < 0 ? j >= i__3 : j <= i__3; j += i__4) {

/*              create nonzero element a(j+ka,j-1) outside the band */
/*              and store it in WORK(m-kb+j) */

		i__1 = m - *kb + j;
		i__2 = m - *kb + j;
		i__5 = ka1 + (j - 1) * ab_dim1;
		z__1.r = work[i__2].r * ab[i__5].r - work[i__2].i * ab[i__5]
			.i, z__1.i = work[i__2].r * ab[i__5].i + work[i__2].i 
			* ab[i__5].r;
		work[i__1].r = z__1.r, work[i__1].i = z__1.i;
		i__1 = ka1 + (j - 1) * ab_dim1;
		i__2 = m - *kb + j;
		i__5 = ka1 + (j - 1) * ab_dim1;
		z__1.r = rwork[i__2] * ab[i__5].r, z__1.i = rwork[i__2] * ab[
			i__5].i;
		ab[i__1].r = z__1.r, ab[i__1].i = z__1.i;
/* L870: */
	    }
	    if (update) {
		if (i__ + k > ka1 && k <= kbt) {
		    i__4 = m - *kb + i__ + k - *ka;
		    i__3 = m - *kb + i__ + k;
		    work[i__4].r = work[i__3].r, work[i__4].i = work[i__3].i;
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

		zlargv_(&nr, &ab[ka1 + j1 * ab_dim1], &inca, &work[m - *kb + 
			j1], &ka1, &rwork[m - *kb + j1], &ka1);

/*              apply rotations in 2nd set from the right */

		i__4 = *ka - 1;
		for (l = 1; l <= i__4; ++l) {
		    zlartv_(&nr, &ab[l + 1 + j1 * ab_dim1], &inca, &ab[l + 2 
			    + (j1 - 1) * ab_dim1], &inca, &rwork[m - *kb + j1]
			    , &work[m - *kb + j1], &ka1);
/* L890: */
		}

/*              apply rotations in 2nd set from both sides to diagonal */
/*              blocks */

		zlar2v_(&nr, &ab[j1 * ab_dim1 + 1], &ab[(j1 - 1) * ab_dim1 + 
			1], &ab[(j1 - 1) * ab_dim1 + 2], &inca, &rwork[m - *
			kb + j1], &work[m - *kb + j1], &ka1);

		zlacgv_(&nr, &work[m - *kb + j1], &ka1);
	    }

/*           start applying rotations in 2nd set from the left */

	    i__4 = *kb - k + 1;
	    for (l = *ka - 1; l >= i__4; --l) {
		nrt = (j2 + l - 1) / ka1;
		j1t = j2 - (nrt - 1) * ka1;
		if (nrt > 0) {
		    zlartv_(&nrt, &ab[ka1 - l + 1 + (j1t - ka1 + l) * ab_dim1]
			    , &inca, &ab[ka1 - l + (j1t - ka1 + l) * ab_dim1],
			     &inca, &rwork[m - *kb + j1t], &work[m - *kb + 
			    j1t], &ka1);
		}
/* L900: */
	    }

	    if (wantx) {

/*              post-multiply X by product of rotations in 2nd set */

		i__4 = j2;
		i__3 = ka1;
		for (j = j1; i__3 < 0 ? j >= i__4 : j <= i__4; j += i__3) {
		    d_cnjg(&z__1, &work[m - *kb + j]);
		    zrot_(&nx, &x[j * x_dim1 + 1], &c__1, &x[(j - 1) * x_dim1 
			    + 1], &c__1, &rwork[m - *kb + j], &z__1);
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
		    zlartv_(&nrt, &ab[ka1 - l + 1 + (j1t - ka1 + l) * ab_dim1]
			    , &inca, &ab[ka1 - l + (j1t - ka1 + l) * ab_dim1],
			     &inca, &rwork[j1t], &work[j1t], &ka1);
		}
/* L930: */
	    }
/* L940: */
	}

	if (*kb > 1) {
	    i__3 = i2 - *ka;
	    for (j = 2; j <= i__3; ++j) {
		rwork[j] = rwork[j + *ka];
		i__4 = j;
		i__1 = j + *ka;
		work[i__4].r = work[i__1].r, work[i__4].i = work[i__1].i;
/* L950: */
	    }
	}

    }

    goto L490;

/*     End of ZHBGST */

} /* zhbgst_ */

