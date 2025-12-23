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
static real c_b36 = .5f;

/* > \brief \b SLATRS solves a triangular system of equations with the scale factor set to prevent overflow. 
*/

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download SLATRS + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/slatrs.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/slatrs.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/slatrs.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE SLATRS( UPLO, TRANS, DIAG, NORMIN, N, A, LDA, X, SCALE, */
/*                          CNORM, INFO ) */

/*       CHARACTER          DIAG, NORMIN, TRANS, UPLO */
/*       INTEGER            INFO, LDA, N */
/*       REAL               SCALE */
/*       REAL               A( LDA, * ), CNORM( * ), X( * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > SLATRS solves one of the triangular systems */
/* > */
/* >    A *x = s*b  or  A**T*x = s*b */
/* > */
/* > with scaling to prevent overflow.  Here A is an upper or lower */
/* > triangular matrix, A**T denotes the transpose of A, x and b are */
/* > n-element vectors, and s is a scaling factor, usually less than */
/* > or equal to 1, chosen so that the components of x will be less than */
/* > the overflow threshold.  If the unscaled problem will not cause */
/* > overflow, the Level 2 BLAS routine STRSV is called.  If the matrix A */
/* > is singular (A(j,j) = 0 for some j), then s is set to 0 and a */
/* > non-trivial solution to A*x = 0 is returned. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] UPLO */
/* > \verbatim */
/* >          UPLO is CHARACTER*1 */
/* >          Specifies whether the matrix A is upper or lower triangular. */
/* >          = 'U':  Upper triangular */
/* >          = 'L':  Lower triangular */
/* > \endverbatim */
/* > */
/* > \param[in] TRANS */
/* > \verbatim */
/* >          TRANS is CHARACTER*1 */
/* >          Specifies the operation applied to A. */
/* >          = 'N':  Solve A * x = s*b  (No transpose) */
/* >          = 'T':  Solve A**T* x = s*b  (Transpose) */
/* >          = 'C':  Solve A**T* x = s*b  (Conjugate transpose = Transpose) */
/* > \endverbatim */
/* > */
/* > \param[in] DIAG */
/* > \verbatim */
/* >          DIAG is CHARACTER*1 */
/* >          Specifies whether or not the matrix A is unit triangular. */
/* >          = 'N':  Non-unit triangular */
/* >          = 'U':  Unit triangular */
/* > \endverbatim */
/* > */
/* > \param[in] NORMIN */
/* > \verbatim */
/* >          NORMIN is CHARACTER*1 */
/* >          Specifies whether CNORM has been set or not. */
/* >          = 'Y':  CNORM contains the column norms on entry */
/* >          = 'N':  CNORM is not set on entry.  On exit, the norms will */
/* >                  be computed and stored in CNORM. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          The order of the matrix A.  N >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] A */
/* > \verbatim */
/* >          A is REAL array, dimension (LDA,N) */
/* >          The triangular matrix A.  If UPLO = 'U', the leading n by n */
/* >          upper triangular part of the array A contains the upper */
/* >          triangular matrix, and the strictly lower triangular part of */
/* >          A is not referenced.  If UPLO = 'L', the leading n by n lower */
/* >          triangular part of the array A contains the lower triangular */
/* >          matrix, and the strictly upper triangular part of A is not */
/* >          referenced.  If DIAG = 'U', the diagonal elements of A are */
/* >          also not referenced and are assumed to be 1. */
/* > \endverbatim */
/* > */
/* > \param[in] LDA */
/* > \verbatim */
/* >          LDA is INTEGER */
/* >          The leading dimension of the array A.  LDA >= f2cmax (1,N). */
/* > \endverbatim */
/* > */
/* > \param[in,out] X */
/* > \verbatim */
/* >          X is REAL array, dimension (N) */
/* >          On entry, the right hand side b of the triangular system. */
/* >          On exit, X is overwritten by the solution vector x. */
/* > \endverbatim */
/* > */
/* > \param[out] SCALE */
/* > \verbatim */
/* >          SCALE is REAL */
/* >          The scaling factor s for the triangular system */
/* >             A * x = s*b  or  A**T* x = s*b. */
/* >          If SCALE = 0, the matrix A is singular or badly scaled, and */
/* >          the vector x is an exact or approximate solution to A*x = 0. */
/* > \endverbatim */
/* > */
/* > \param[in,out] CNORM */
/* > \verbatim */
/* >          CNORM is REAL array, dimension (N) */
/* > */
/* >          If NORMIN = 'Y', CNORM is an input argument and CNORM(j) */
/* >          contains the norm of the off-diagonal part of the j-th column */
/* >          of A.  If TRANS = 'N', CNORM(j) must be greater than or equal */
/* >          to the infinity-norm, and if TRANS = 'T' or 'C', CNORM(j) */
/* >          must be greater than or equal to the 1-norm. */
/* > */
/* >          If NORMIN = 'N', CNORM is an output argument and CNORM(j) */
/* >          returns the 1-norm of the offdiagonal part of the j-th column */
/* >          of A. */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >          = 0:  successful exit */
/* >          < 0:  if INFO = -k, the k-th argument had an illegal value */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date December 2016 */

/* > \ingroup realOTHERauxiliary */

/* > \par Further Details: */
/*  ===================== */
/* > */
/* > \verbatim */
/* > */
/* >  A rough bound on x is computed; if that is less than overflow, STRSV */
/* >  is called, otherwise, specific code is used which checks for possible */
/* >  overflow or divide-by-zero at every operation. */
/* > */
/* >  A columnwise scheme is used for solving A*x = b.  The basic algorithm */
/* >  if A is lower triangular is */
/* > */
/* >       x[1:n] := b[1:n] */
/* >       for j = 1, ..., n */
/* >            x(j) := x(j) / A(j,j) */
/* >            x[j+1:n] := x[j+1:n] - x(j) * A[j+1:n,j] */
/* >       end */
/* > */
/* >  Define bounds on the components of x after j iterations of the loop: */
/* >     M(j) = bound on x[1:j] */
/* >     G(j) = bound on x[j+1:n] */
/* >  Initially, let M(0) = 0 and G(0) = f2cmax{x(i), i=1,...,n}. */
/* > */
/* >  Then for iteration j+1 we have */
/* >     M(j+1) <= G(j) / | A(j+1,j+1) | */
/* >     G(j+1) <= G(j) + M(j+1) * | A[j+2:n,j+1] | */
/* >            <= G(j) ( 1 + CNORM(j+1) / | A(j+1,j+1) | ) */
/* > */
/* >  where CNORM(j+1) is greater than or equal to the infinity-norm of */
/* >  column j+1 of A, not counting the diagonal.  Hence */
/* > */
/* >     G(j) <= G(0) product ( 1 + CNORM(i) / | A(i,i) | ) */
/* >                  1<=i<=j */
/* >  and */
/* > */
/* >     |x(j)| <= ( G(0) / |A(j,j)| ) product ( 1 + CNORM(i) / |A(i,i)| ) */
/* >                                   1<=i< j */
/* > */
/* >  Since |x(j)| <= M(j), we use the Level 2 BLAS routine STRSV if the */
/* >  reciprocal of the largest M(j), j=1,..,n, is larger than */
/* >  f2cmax(underflow, 1/overflow). */
/* > */
/* >  The bound on x(j) is also used to determine when a step in the */
/* >  columnwise method can be performed without fear of overflow.  If */
/* >  the computed bound is greater than a large constant, x is scaled to */
/* >  prevent overflow, but if the bound overflows, x is set to 0, x(j) to */
/* >  1, and scale to 0, and a non-trivial solution to A*x = 0 is found. */
/* > */
/* >  Similarly, a row-wise scheme is used to solve A**T*x = b.  The basic */
/* >  algorithm for A upper triangular is */
/* > */
/* >       for j = 1, ..., n */
/* >            x(j) := ( b(j) - A[1:j-1,j]**T * x[1:j-1] ) / A(j,j) */
/* >       end */
/* > */
/* >  We simultaneously compute two bounds */
/* >       G(j) = bound on ( b(i) - A[1:i-1,i]**T * x[1:i-1] ), 1<=i<=j */
/* >       M(j) = bound on x(i), 1<=i<=j */
/* > */
/* >  The initial values are G(0) = 0, M(0) = f2cmax{b(i), i=1,..,n}, and we */
/* >  add the constraint G(j) >= G(j-1) and M(j) >= M(j-1) for j >= 1. */
/* >  Then the bound on x(j) is */
/* > */
/* >       M(j) <= M(j-1) * ( 1 + CNORM(j) ) / | A(j,j) | */
/* > */
/* >            <= M(0) * product ( ( 1 + CNORM(i) ) / |A(i,i)| ) */
/* >                      1<=i<=j */
/* > */
/* >  and we can safely call STRSV if 1/M(n) and 1/G(n) are both greater */
/* >  than f2cmax(underflow, 1/overflow). */
/* > \endverbatim */
/* > */
/*  ===================================================================== */
/* Subroutine */ void slatrs_(char *uplo, char *trans, char *diag, char *
	normin, integer *n, real *a, integer *lda, real *x, real *scale, real 
	*cnorm, integer *info)
{
    /* System generated locals */
    integer a_dim1, a_offset, i__1, i__2, i__3;
    real r__1, r__2, r__3;

    /* Local variables */
    integer jinc;
    real xbnd;
    integer imax;
    real tmax, tjjs;
    extern real sdot_(integer *, real *, integer *, real *, integer *);
    real xmax, grow, sumj;
    integer i__, j;
    extern logical lsame_(char *, char *);
    extern /* Subroutine */ void sscal_(integer *, real *, real *, integer *);
    real tscal, uscal;
    integer jlast;
    extern real sasum_(integer *, real *, integer *);
    logical upper;
    extern /* Subroutine */ void saxpy_(integer *, real *, real *, integer *, 
	    real *, integer *), strsv_(char *, char *, char *, integer *, 
	    real *, integer *, real *, integer *);
    real xj;
    extern real slamch_(char *);
    extern /* Subroutine */ int xerbla_(char *, integer *, ftnlen);
    real bignum;
    extern integer isamax_(integer *, real *, integer *);
    logical notran;
    integer jfirst;
    real smlnum;
    logical nounit;
    real rec, tjj;


/*  -- LAPACK auxiliary routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     December 2016 */


/*  ===================================================================== */


    /* Parameter adjustments */
    a_dim1 = *lda;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;
    --x;
    --cnorm;

    /* Function Body */
    *info = 0;
    upper = lsame_(uplo, "U");
    notran = lsame_(trans, "N");
    nounit = lsame_(diag, "N");

/*     Test the input parameters. */

    if (! upper && ! lsame_(uplo, "L")) {
	*info = -1;
    } else if (! notran && ! lsame_(trans, "T") && ! 
	    lsame_(trans, "C")) {
	*info = -2;
    } else if (! nounit && ! lsame_(diag, "U")) {
	*info = -3;
    } else if (! lsame_(normin, "Y") && ! lsame_(normin,
	     "N")) {
	*info = -4;
    } else if (*n < 0) {
	*info = -5;
    } else if (*lda < f2cmax(1,*n)) {
	*info = -7;
    }
    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("SLATRS", &i__1, (ftnlen)6);
	return;
    }

/*     Quick return if possible */

    if (*n == 0) {
	return;
    }

/*     Determine machine dependent parameters to control overflow. */

    smlnum = slamch_("Safe minimum") / slamch_("Precision");
    bignum = 1.f / smlnum;
    *scale = 1.f;

    if (lsame_(normin, "N")) {

/*        Compute the 1-norm of each column, not including the diagonal. */

	if (upper) {

/*           A is upper triangular. */

	    i__1 = *n;
	    for (j = 1; j <= i__1; ++j) {
		i__2 = j - 1;
		cnorm[j] = sasum_(&i__2, &a[j * a_dim1 + 1], &c__1);
/* L10: */
	    }
	} else {

/*           A is lower triangular. */

	    i__1 = *n - 1;
	    for (j = 1; j <= i__1; ++j) {
		i__2 = *n - j;
		cnorm[j] = sasum_(&i__2, &a[j + 1 + j * a_dim1], &c__1);
/* L20: */
	    }
	    cnorm[*n] = 0.f;
	}
    }

/*     Scale the column norms by TSCAL if the maximum element in CNORM is */
/*     greater than BIGNUM. */

    imax = isamax_(n, &cnorm[1], &c__1);
    tmax = cnorm[imax];
    if (tmax <= bignum) {
	tscal = 1.f;
    } else {
	tscal = 1.f / (smlnum * tmax);
	sscal_(n, &tscal, &cnorm[1], &c__1);
    }

/*     Compute a bound on the computed solution vector to see if the */
/*     Level 2 BLAS routine STRSV can be used. */

    j = isamax_(n, &x[1], &c__1);
    xmax = (r__1 = x[j], abs(r__1));
    xbnd = xmax;
    if (notran) {

/*        Compute the growth in A * x = b. */

	if (upper) {
	    jfirst = *n;
	    jlast = 1;
	    jinc = -1;
	} else {
	    jfirst = 1;
	    jlast = *n;
	    jinc = 1;
	}

	if (tscal != 1.f) {
	    grow = 0.f;
	    goto L50;
	}

	if (nounit) {

/*           A is non-unit triangular. */

/*           Compute GROW = 1/G(j) and XBND = 1/M(j). */
/*           Initially, G(0) = f2cmax{x(i), i=1,...,n}. */

	    grow = 1.f / f2cmax(xbnd,smlnum);
	    xbnd = grow;
	    i__1 = jlast;
	    i__2 = jinc;
	    for (j = jfirst; i__2 < 0 ? j >= i__1 : j <= i__1; j += i__2) {

/*              Exit the loop if the growth factor is too small. */

		if (grow <= smlnum) {
		    goto L50;
		}

/*              M(j) = G(j-1) / abs(A(j,j)) */

		tjj = (r__1 = a[j + j * a_dim1], abs(r__1));
/* Computing MIN */
		r__1 = xbnd, r__2 = f2cmin(1.f,tjj) * grow;
		xbnd = f2cmin(r__1,r__2);
		if (tjj + cnorm[j] >= smlnum) {

/*                 G(j) = G(j-1)*( 1 + CNORM(j) / abs(A(j,j)) ) */

		    grow *= tjj / (tjj + cnorm[j]);
		} else {

/*                 G(j) could overflow, set GROW to 0. */

		    grow = 0.f;
		}
/* L30: */
	    }
	    grow = xbnd;
	} else {

/*           A is unit triangular. */

/*           Compute GROW = 1/G(j), where G(0) = f2cmax{x(i), i=1,...,n}. */

/* Computing MIN */
	    r__1 = 1.f, r__2 = 1.f / f2cmax(xbnd,smlnum);
	    grow = f2cmin(r__1,r__2);
	    i__2 = jlast;
	    i__1 = jinc;
	    for (j = jfirst; i__1 < 0 ? j >= i__2 : j <= i__2; j += i__1) {

/*              Exit the loop if the growth factor is too small. */

		if (grow <= smlnum) {
		    goto L50;
		}

/*              G(j) = G(j-1)*( 1 + CNORM(j) ) */

		grow *= 1.f / (cnorm[j] + 1.f);
/* L40: */
	    }
	}
L50:

	;
    } else {

/*        Compute the growth in A**T * x = b. */

	if (upper) {
	    jfirst = 1;
	    jlast = *n;
	    jinc = 1;
	} else {
	    jfirst = *n;
	    jlast = 1;
	    jinc = -1;
	}

	if (tscal != 1.f) {
	    grow = 0.f;
	    goto L80;
	}

	if (nounit) {

/*           A is non-unit triangular. */

/*           Compute GROW = 1/G(j) and XBND = 1/M(j). */
/*           Initially, M(0) = f2cmax{x(i), i=1,...,n}. */

	    grow = 1.f / f2cmax(xbnd,smlnum);
	    xbnd = grow;
	    i__1 = jlast;
	    i__2 = jinc;
	    for (j = jfirst; i__2 < 0 ? j >= i__1 : j <= i__1; j += i__2) {

/*              Exit the loop if the growth factor is too small. */

		if (grow <= smlnum) {
		    goto L80;
		}

/*              G(j) = f2cmax( G(j-1), M(j-1)*( 1 + CNORM(j) ) ) */

		xj = cnorm[j] + 1.f;
/* Computing MIN */
		r__1 = grow, r__2 = xbnd / xj;
		grow = f2cmin(r__1,r__2);

/*              M(j) = M(j-1)*( 1 + CNORM(j) ) / abs(A(j,j)) */

		tjj = (r__1 = a[j + j * a_dim1], abs(r__1));
		if (xj > tjj) {
		    xbnd *= tjj / xj;
		}
/* L60: */
	    }
	    grow = f2cmin(grow,xbnd);
	} else {

/*           A is unit triangular. */

/*           Compute GROW = 1/G(j), where G(0) = f2cmax{x(i), i=1,...,n}. */

/* Computing MIN */
	    r__1 = 1.f, r__2 = 1.f / f2cmax(xbnd,smlnum);
	    grow = f2cmin(r__1,r__2);
	    i__2 = jlast;
	    i__1 = jinc;
	    for (j = jfirst; i__1 < 0 ? j >= i__2 : j <= i__2; j += i__1) {

/*              Exit the loop if the growth factor is too small. */

		if (grow <= smlnum) {
		    goto L80;
		}

/*              G(j) = ( 1 + CNORM(j) )*G(j-1) */

		xj = cnorm[j] + 1.f;
		grow /= xj;
/* L70: */
	    }
	}
L80:
	;
    }

    if (grow * tscal > smlnum) {

/*        Use the Level 2 BLAS solve if the reciprocal of the bound on */
/*        elements of X is not too small. */

	strsv_(uplo, trans, diag, n, &a[a_offset], lda, &x[1], &c__1);
    } else {

/*        Use a Level 1 BLAS solve, scaling intermediate results. */

	if (xmax > bignum) {

/*           Scale X so that its components are less than or equal to */
/*           BIGNUM in absolute value. */

	    *scale = bignum / xmax;
	    sscal_(n, scale, &x[1], &c__1);
	    xmax = bignum;
	}

	if (notran) {

/*           Solve A * x = b */

	    i__1 = jlast;
	    i__2 = jinc;
	    for (j = jfirst; i__2 < 0 ? j >= i__1 : j <= i__1; j += i__2) {

/*              Compute x(j) = b(j) / A(j,j), scaling x if necessary. */

		xj = (r__1 = x[j], abs(r__1));
		if (nounit) {
		    tjjs = a[j + j * a_dim1] * tscal;
		} else {
		    tjjs = tscal;
		    if (tscal == 1.f) {
			goto L95;
		    }
		}
		tjj = abs(tjjs);
		if (tjj > smlnum) {

/*                    abs(A(j,j)) > SMLNUM: */

		    if (tjj < 1.f) {
			if (xj > tjj * bignum) {

/*                          Scale x by 1/b(j). */

			    rec = 1.f / xj;
			    sscal_(n, &rec, &x[1], &c__1);
			    *scale *= rec;
			    xmax *= rec;
			}
		    }
		    x[j] /= tjjs;
		    xj = (r__1 = x[j], abs(r__1));
		} else if (tjj > 0.f) {

/*                    0 < abs(A(j,j)) <= SMLNUM: */

		    if (xj > tjj * bignum) {

/*                       Scale x by (1/abs(x(j)))*abs(A(j,j))*BIGNUM */
/*                       to avoid overflow when dividing by A(j,j). */

			rec = tjj * bignum / xj;
			if (cnorm[j] > 1.f) {

/*                          Scale by 1/CNORM(j) to avoid overflow when */
/*                          multiplying x(j) times column j. */

			    rec /= cnorm[j];
			}
			sscal_(n, &rec, &x[1], &c__1);
			*scale *= rec;
			xmax *= rec;
		    }
		    x[j] /= tjjs;
		    xj = (r__1 = x[j], abs(r__1));
		} else {

/*                    A(j,j) = 0:  Set x(1:n) = 0, x(j) = 1, and */
/*                    scale = 0, and compute a solution to A*x = 0. */

		    i__3 = *n;
		    for (i__ = 1; i__ <= i__3; ++i__) {
			x[i__] = 0.f;
/* L90: */
		    }
		    x[j] = 1.f;
		    xj = 1.f;
		    *scale = 0.f;
		    xmax = 0.f;
		}
L95:

/*              Scale x if necessary to avoid overflow when adding a */
/*              multiple of column j of A. */

		if (xj > 1.f) {
		    rec = 1.f / xj;
		    if (cnorm[j] > (bignum - xmax) * rec) {

/*                    Scale x by 1/(2*abs(x(j))). */

			rec *= .5f;
			sscal_(n, &rec, &x[1], &c__1);
			*scale *= rec;
		    }
		} else if (xj * cnorm[j] > bignum - xmax) {

/*                 Scale x by 1/2. */

		    sscal_(n, &c_b36, &x[1], &c__1);
		    *scale *= .5f;
		}

		if (upper) {
		    if (j > 1) {

/*                    Compute the update */
/*                       x(1:j-1) := x(1:j-1) - x(j) * A(1:j-1,j) */

			i__3 = j - 1;
			r__1 = -x[j] * tscal;
			saxpy_(&i__3, &r__1, &a[j * a_dim1 + 1], &c__1, &x[1],
				 &c__1);
			i__3 = j - 1;
			i__ = isamax_(&i__3, &x[1], &c__1);
			xmax = (r__1 = x[i__], abs(r__1));
		    }
		} else {
		    if (j < *n) {

/*                    Compute the update */
/*                       x(j+1:n) := x(j+1:n) - x(j) * A(j+1:n,j) */

			i__3 = *n - j;
			r__1 = -x[j] * tscal;
			saxpy_(&i__3, &r__1, &a[j + 1 + j * a_dim1], &c__1, &
				x[j + 1], &c__1);
			i__3 = *n - j;
			i__ = j + isamax_(&i__3, &x[j + 1], &c__1);
			xmax = (r__1 = x[i__], abs(r__1));
		    }
		}
/* L100: */
	    }

	} else {

/*           Solve A**T * x = b */

	    i__2 = jlast;
	    i__1 = jinc;
	    for (j = jfirst; i__1 < 0 ? j >= i__2 : j <= i__2; j += i__1) {

/*              Compute x(j) = b(j) - sum A(k,j)*x(k). */
/*                                    k<>j */

		xj = (r__1 = x[j], abs(r__1));
		uscal = tscal;
		rec = 1.f / f2cmax(xmax,1.f);
		if (cnorm[j] > (bignum - xj) * rec) {

/*                 If x(j) could overflow, scale x by 1/(2*XMAX). */

		    rec *= .5f;
		    if (nounit) {
			tjjs = a[j + j * a_dim1] * tscal;
		    } else {
			tjjs = tscal;
		    }
		    tjj = abs(tjjs);
		    if (tjj > 1.f) {

/*                       Divide by A(j,j) when scaling x if A(j,j) > 1. */

/* Computing MIN */
			r__1 = 1.f, r__2 = rec * tjj;
			rec = f2cmin(r__1,r__2);
			uscal /= tjjs;
		    }
		    if (rec < 1.f) {
			sscal_(n, &rec, &x[1], &c__1);
			*scale *= rec;
			xmax *= rec;
		    }
		}

		sumj = 0.f;
		if (uscal == 1.f) {

/*                 If the scaling needed for A in the dot product is 1, */
/*                 call SDOT to perform the dot product. */

		    if (upper) {
			i__3 = j - 1;
			sumj = sdot_(&i__3, &a[j * a_dim1 + 1], &c__1, &x[1], 
				&c__1);
		    } else if (j < *n) {
			i__3 = *n - j;
			sumj = sdot_(&i__3, &a[j + 1 + j * a_dim1], &c__1, &x[
				j + 1], &c__1);
		    }
		} else {

/*                 Otherwise, use in-line code for the dot product. */

		    if (upper) {
			i__3 = j - 1;
			for (i__ = 1; i__ <= i__3; ++i__) {
			    sumj += a[i__ + j * a_dim1] * uscal * x[i__];
/* L110: */
			}
		    } else if (j < *n) {
			i__3 = *n;
			for (i__ = j + 1; i__ <= i__3; ++i__) {
			    sumj += a[i__ + j * a_dim1] * uscal * x[i__];
/* L120: */
			}
		    }
		}

		if (uscal == tscal) {

/*                 Compute x(j) := ( x(j) - sumj ) / A(j,j) if 1/A(j,j) */
/*                 was not used to scale the dotproduct. */

		    x[j] -= sumj;
		    xj = (r__1 = x[j], abs(r__1));
		    if (nounit) {
			tjjs = a[j + j * a_dim1] * tscal;
		    } else {
			tjjs = tscal;
			if (tscal == 1.f) {
			    goto L135;
			}
		    }

/*                    Compute x(j) = x(j) / A(j,j), scaling if necessary. */

		    tjj = abs(tjjs);
		    if (tjj > smlnum) {

/*                       abs(A(j,j)) > SMLNUM: */

			if (tjj < 1.f) {
			    if (xj > tjj * bignum) {

/*                             Scale X by 1/abs(x(j)). */

				rec = 1.f / xj;
				sscal_(n, &rec, &x[1], &c__1);
				*scale *= rec;
				xmax *= rec;
			    }
			}
			x[j] /= tjjs;
		    } else if (tjj > 0.f) {

/*                       0 < abs(A(j,j)) <= SMLNUM: */

			if (xj > tjj * bignum) {

/*                          Scale x by (1/abs(x(j)))*abs(A(j,j))*BIGNUM. */

			    rec = tjj * bignum / xj;
			    sscal_(n, &rec, &x[1], &c__1);
			    *scale *= rec;
			    xmax *= rec;
			}
			x[j] /= tjjs;
		    } else {

/*                       A(j,j) = 0:  Set x(1:n) = 0, x(j) = 1, and */
/*                       scale = 0, and compute a solution to A**T*x = 0. */

			i__3 = *n;
			for (i__ = 1; i__ <= i__3; ++i__) {
			    x[i__] = 0.f;
/* L130: */
			}
			x[j] = 1.f;
			*scale = 0.f;
			xmax = 0.f;
		    }
L135:
		    ;
		} else {

/*                 Compute x(j) := x(j) / A(j,j)  - sumj if the dot */
/*                 product has already been divided by 1/A(j,j). */

		    x[j] = x[j] / tjjs - sumj;
		}
/* Computing MAX */
		r__2 = xmax, r__3 = (r__1 = x[j], abs(r__1));
		xmax = f2cmax(r__2,r__3);
/* L140: */
	    }
	}
	*scale /= tscal;
    }

/*     Scale the column norms by 1/TSCAL for return. */

    if (tscal != 1.f) {
	r__1 = 1.f / tscal;
	sscal_(n, &r__1, &cnorm[1], &c__1);
    }

    return;

/*     End of SLATRS */

} /* slatrs_ */

