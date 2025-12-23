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
static char junk[] = "\n@(#)LIBF77 VERSION 19990503\n";
#define z_abs(z) (cabs(Cd(z)))
#define z_exp(R, Z) {pCd(R) = cexp(Cd(Z));}
#define z_sqrt(R, Z) {pCd(R) = csqrt(Cd(Z));}
#define myexit_() break;
#define mycycle_() continue;
#define myceiling_(w) {ceil(w)}
#define myhuge_(w) {HUGE_VAL}
//#define mymaxloc_(w,s,e,n) {if (sizeof(*(w)) == sizeof(double)) dmaxloc_((w),*(s),*(e),n); else dmaxloc_((w),*(s),*(e),n);}
#define mymaxloc_(w,s,e,n) dmaxloc_(w,*(s),*(e),n)
#define myexp_(w) my_expfunc(w)

static int my_expfunc(float *x) {int e; (void)frexpf(*x,&e); return e;}

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
static integer c_n1 = -1;
static real c_b35 = -1.f;
static real c_b36 = 1.f;

/* > \brief \b SLATRS3 solves a triangular system of equations with the scale factors set to prevent overflow.
 */

/*  Definition: */
/*  =========== */

/*      SUBROUTINE SLATRS3( UPLO, TRANS, DIAG, NORMIN, N, NRHS, A, LDA, */
/*                          X, LDX, SCALE, CNORM, WORK, LWORK, INFO ) */

/*       CHARACTER          DIAG, NORMIN, TRANS, UPLO */
/*       INTEGER            INFO, LDA, LWORK, LDX, N, NRHS */
/*       REAL               A( LDA, * ), CNORM( * ), SCALE( * ), */
/*                          WORK( * ), X( LDX, * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > SLATRS3 solves one of the triangular systems */
/* > */
/* >    A * X = B * diag(scale)  or  A**T * X = B * diag(scale) */
/* > */
/* > with scaling to prevent overflow.  Here A is an upper or lower */
/* > triangular matrix, A**T denotes the transpose of A. X and B are */
/* > n by nrhs matrices and scale is an nrhs element vector of scaling */
/* > factors. A scaling factor scale(j) is usually less than or equal */
/* > to 1, chosen such that X(:,j) is less than the overflow threshold. */
/* > If the matrix A is singular (A(j,j) = 0 for some j), then */
/* > a non-trivial solution to A*X = 0 is returned. If the system is */
/* > so badly scaled that the solution cannot be represented as */
/* > (1/scale(k))*X(:,k), then x(:,k) = 0 and scale(k) is returned. */
/* > */
/* > This is a BLAS-3 version of LATRS for solving several right */
/* > hand sides simultaneously. */
/* > */
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
/* > \param[in] NRHS */
/* > \verbatim */
/* >          NRHS is INTEGER */
/* >          The number of columns of X.  NRHS >= 0. */
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
/* >          X is REAL array, dimension (LDX,NRHS) */
/* >          On entry, the right hand side B of the triangular system. */
/* >          On exit, X is overwritten by the solution matrix X. */
/* > \endverbatim */
/* > */
/* > \param[in] LDX */
/* > \verbatim */
/* >          LDX is INTEGER */
/* >          The leading dimension of the array X.  LDX >= f2cmax (1,N). */
/* > \endverbatim */
/* > */
/* > \param[out] SCALE */
/* > \verbatim */
/* >          SCALE is REAL array, dimension (NRHS) */
/* >          The scaling factor s(k) is for the triangular system */
/* >          A * x(:,k) = s(k)*b(:,k)  or  A**T* x(:,k) = s(k)*b(:,k). */
/* >          If SCALE = 0, the matrix A is singular or badly scaled. */
/* >          If A(j,j) = 0 is encountered, a non-trivial vector x(:,k) */
/* >          that is an exact or approximate solution to A*x(:,k) = 0 */
/* >          is returned. If the system so badly scaled that solution */
/* >          cannot be presented as x(:,k) * 1/s(k), then x(:,k) = 0 */
/* >          is returned. */
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
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is REAL array, dimension (LWORK). */
/* >          On exit, if INFO = 0, WORK(1) returns the optimal size of */
/* >          WORK. */
/* > \endverbatim */
/* > */
/* > \param[in] LWORK */
/* >          LWORK is INTEGER */
/* >          LWORK >= MAX(1, 2*NBA * MAX(NBA, MIN(NRHS, 32)), where */
/* >          NBA = (N + NB - 1)/NB and NB is the optimal block size. */
/* > */
/* >          If LWORK = -1, then a workspace query is assumed; the routine */
/* >          only calculates the optimal dimensions of the WORK array, returns */
/* >          this value as the first entry of the WORK array, and no error */
/* >          message related to LWORK is issued by XERBLA. */
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

/* > \ingroup doubleOTHERauxiliary */
/* > \par Further Details: */
/*  ===================== */
/*  \verbatim */
/*  The algorithm follows the structure of a block triangular solve. */
/*  The diagonal block is solved with a call to the robust the triangular */
/*  solver LATRS for every right-hand side RHS = 1, ..., NRHS */
/*     op(A( J, J )) * x( J, RHS ) = SCALOC * b( J, RHS ), */
/*  where op( A ) = A or op( A ) = A**T. */
/*  The linear block updates operate on block columns of X, */
/*     B( I, K ) - op(A( I, J )) * X( J, K ) */
/*  and use GEMM. To avoid overflow in the linear block update, the worst case */
/*  growth is estimated. For every RHS, a scale factor s <= 1.0 is computed */
/*  such that */
/*     || s * B( I, RHS )||_oo */
/*   + || op(A( I, J )) ||_oo * || s *  X( J, RHS ) ||_oo <= Overflow threshold */

/*  Once all columns of a block column have been rescaled (BLAS-1), the linear */
/*  update is executed with GEMM without overflow. */

/*  To limit rescaling, local scale factors track the scaling of column segments. */
/*  There is one local scale factor s( I, RHS ) per block row I = 1, ..., NBA */
/*  per right-hand side column RHS = 1, ..., NRHS. The global scale factor */
/*  SCALE( RHS ) is chosen as the smallest local scale factor s( I, RHS ) */
/*  I = 1, ..., NBA. */
/*  A triangular solve op(A( J, J )) * x( J, RHS ) = SCALOC * b( J, RHS ) */
/*  updates the local scale factor s( J, RHS ) := s( J, RHS ) * SCALOC. The */
/*  linear update of potentially inconsistently scaled vector segments */
/*     s( I, RHS ) * b( I, RHS ) - op(A( I, J )) * ( s( J, RHS )* x( J, RHS ) ) */
/*  computes a consistent scaling SCAMIN = MIN( s(I, RHS ), s(J, RHS) ) and, */
/*  if necessary, rescales the blocks prior to calling GEMM. */

/*  \endverbatim */
/*  ===================================================================== */
/*  References: */
/*  C. C. Kjelgaard Mikkelsen, A. B. Schwarz and L. Karlsson (2019). */
/*  Parallel robust solution of triangular linear systems. Concurrency */
/*  and Computation: Practice and Experience, 31(19), e5064. */

/*  Contributor: */
/*   Angelika Schwarz, Umea University, Sweden. */

/*  ===================================================================== */
/* Subroutine */ void slatrs3_(char *uplo, char *trans, char *diag, char *
	normin, integer *n, integer *nrhs, real *a, integer *lda, real *x, 
	integer *ldx, real *scale, real *cnorm, real *work, integer *lwork, 
	integer *info)
{
    /* System generated locals */
    integer a_dim1, a_offset, x_dim1, x_offset, i__1, i__2, i__3, i__4, i__5, 
	    i__6, i__7, i__8;
    real r__1, r__2;

    /* Local variables */
    integer iinc, jinc;
    real scal, anrm, bnrm;
    integer awrk;
    real tmax, xnrm[32];
    integer i__, j, k;
    real w[64];
    extern logical lsame_(char *, char *);
    real rscal;
    extern /* Subroutine */ void sscal_(integer *, real *, real *, integer *), 
	    sgemm_(char *, char *, integer *, integer *, integer *, real *, 
	    real *, integer *, real *, integer *, real *, real *, integer *);
    integer lanrm, ilast, jlast, i1;
    logical upper;
    integer i2, j1, j2, k1, k2, nb, ii, kk, lscale;
    real scaloc;
    extern real slamch_(char *), slange_(char *, integer *, integer *,
	     real *, integer *, real *);
    real scamin;
    extern /* Subroutine */ int xerbla_(char *, integer *, ftnlen );
    extern integer ilaenv_(integer *, char *, char *, integer *, integer *, 
	    integer *, integer *, ftnlen, ftnlen);
    real bignum;
    extern real slarmm_(real *, real *, real *);
    integer ifirst;
    logical notran;
    integer jfirst;
    extern /* Subroutine */ void slatrs_(char *, char *, char *, char *, 
	    integer *, real *, integer *, real *, real *, real *, integer *);
    real smlnum;
    logical nounit, lquery;
    integer nba, lds, nbx, rhs;



/*  ===================================================================== */


    /* Parameter adjustments */
    a_dim1 = *lda;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;
    x_dim1 = *ldx;
    x_offset = 1 + x_dim1 * 1;
    x -= x_offset;
    --scale;
    --cnorm;
    --work;

    /* Function Body */
    *info = 0;
    upper = lsame_(uplo, "U");
    notran = lsame_(trans, "N");
    nounit = lsame_(diag, "N");
    lquery = *lwork == -1;

/*     Partition A and X into blocks. */

/* Computing MAX */
    i__1 = 8, i__2 = ilaenv_(&c__1, "SLATRS", "", n, n, &c_n1, &c_n1, (ftnlen)
	    6, (ftnlen)0);
    nb = f2cmax(i__1,i__2);
    nb = f2cmin(64,nb);
/* Computing MAX */
    i__1 = 1, i__2 = (*n + nb - 1) / nb;
    nba = f2cmax(i__1,i__2);
/* Computing MAX */
    i__1 = 1, i__2 = (*nrhs + 31) / 32;
    nbx = f2cmax(i__1,i__2);

/*     Compute the workspace */

/*     The workspace comprises two parts. */
/*     The first part stores the local scale factors. Each simultaneously */
/*     computed right-hand side requires one local scale factor per block */
/*     row. WORK( I + KK * LDS ) is the scale factor of the vector */
/*     segment associated with the I-th block row and the KK-th vector */
/*     in the block column. */
/* Computing MAX */
    i__1 = nba, i__2 = f2cmin(*nrhs,32);
    lscale = nba * f2cmax(i__1,i__2);
    lds = nba;
/*     The second part stores upper bounds of the triangular A. There are */
/*     a total of NBA x NBA blocks, of which only the upper triangular */
/*     part or the lower triangular part is referenced. The upper bound of */
/*     the block A( I, J ) is stored as WORK( AWRK + I + J * NBA ). */
    lanrm = nba * nba;
    awrk = lscale;
    work[1] = (real) (lscale + lanrm);

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
    } else if (*nrhs < 0) {
	*info = -6;
    } else if (*lda < f2cmax(1,*n)) {
	*info = -8;
    } else if (*ldx < f2cmax(1,*n)) {
	*info = -10;
    } else if (! lquery && (real) (*lwork) < work[1]) {
	*info = -14;
    }
    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("SLATRS3", &i__1, 7);
	return;
    } else if (lquery) {
	return;
    }

/*     Initialize scaling factors */

    i__1 = *nrhs;
    for (kk = 1; kk <= i__1; ++kk) {
	scale[kk] = 1.f;
    }

/*     Quick return if possible */

    if (f2cmin(*n,*nrhs) == 0) {
	return;
    }

/*     Determine machine dependent constant to control overflow. */

    bignum = slamch_("Overflow");
    smlnum = slamch_("Safe Minimum");

/*     Use unblocked code for small problems */

    if (*nrhs < 2) {
	slatrs_(uplo, trans, diag, normin, n, &a[a_offset], lda, &x[x_dim1 + 
		1], &scale[1], &cnorm[1], info);
	i__1 = *nrhs;
	for (k = 2; k <= i__1; ++k) {
	    slatrs_(uplo, trans, diag, "Y", n, &a[a_offset], lda, &x[k * 
		    x_dim1 + 1], &scale[k], &cnorm[1], info);
	}
	return;
    }

/*     Compute norms of blocks of A excluding diagonal blocks and find */
/*     the block with the largest norm TMAX. */

    tmax = 0.f;
    i__1 = nba;
    for (j = 1; j <= i__1; ++j) {
	j1 = (j - 1) * nb + 1;
/* Computing MIN */
	i__2 = j * nb;
	j2 = f2cmin(i__2,*n) + 1;
	if (upper) {
	    ifirst = 1;
	    ilast = j - 1;
	} else {
	    ifirst = j + 1;
	    ilast = nba;
	}
	i__2 = ilast;
	for (i__ = ifirst; i__ <= i__2; ++i__) {
	    i1 = (i__ - 1) * nb + 1;
/* Computing MIN */
	    i__3 = i__ * nb;
	    i2 = f2cmin(i__3,*n) + 1;

/*           Compute upper bound of A( I1:I2-1, J1:J2-1 ). */

	    if (notran) {
		i__3 = i2 - i1;
		i__4 = j2 - j1;
		anrm = slange_("I", &i__3, &i__4, &a[i1 + j1 * a_dim1], lda, 
			w);
		work[awrk + i__ + (j - 1) * nba] = anrm;
	    } else {
		i__3 = i2 - i1;
		i__4 = j2 - j1;
		anrm = slange_("1", &i__3, &i__4, &a[i1 + j1 * a_dim1], lda, 
			w);
		work[awrk + j + (i__ - 1) * nba] = anrm;
	    }
	    tmax = f2cmax(tmax,anrm);
	}
    }

    if (! (tmax <= slamch_("Overflow"))) {

/*        Some matrix entries have huge absolute value. At least one upper */
/*        bound norm( A(I1:I2-1, J1:J2-1), 'I') is not a valid floating-point */
/*        number, either due to overflow in LANGE or due to Inf in A. */
/*        Fall back to LATRS. Set normin = 'N' for every right-hand side to */
/*        force computation of TSCAL in LATRS to avoid the likely overflow */
/*        in the computation of the column norms CNORM. */

	i__1 = *nrhs;
	for (k = 1; k <= i__1; ++k) {
	    slatrs_(uplo, trans, diag, "N", n, &a[a_offset], lda, &x[k * 
		    x_dim1 + 1], &scale[k], &cnorm[1], info);
	}
	return;
    }

/*     Every right-hand side requires workspace to store NBA local scale */
/*     factors. To save workspace, X is computed successively in block columns */
/*     of width NBRHS, requiring a total of NBA x NBRHS space. If sufficient */
/*     workspace is available, larger values of NBRHS or NBRHS = NRHS are viable. */
    i__1 = nbx;
    for (k = 1; k <= i__1; ++k) {
/*        Loop over block columns (index = K) of X and, for column-wise scalings, */
/*        over individual columns (index = KK). */
/*        K1: column index of the first column in X( J, K ) */
/*        K2: column index of the first column in X( J, K+1 ) */
/*        so the K2 - K1 is the column count of the block X( J, K ) */
	k1 = (k - 1 << 5) + 1;
/* Computing MIN */
	i__2 = k << 5;
	k2 = f2cmin(i__2,*nrhs) + 1;

/*        Initialize local scaling factors of current block column X( J, K ) */

	i__2 = k2 - k1;
	for (kk = 1; kk <= i__2; ++kk) {
	    i__3 = nba;
	    for (i__ = 1; i__ <= i__3; ++i__) {
		work[i__ + kk * lds] = 1.f;
	    }
	}

	if (notran) {

/*           Solve A * X(:, K1:K2-1) = B * diag(scale(K1:K2-1)) */

	    if (upper) {
		jfirst = nba;
		jlast = 1;
		jinc = -1;
	    } else {
		jfirst = 1;
		jlast = nba;
		jinc = 1;
	    }
	} else {

/*           Solve A**T * X(:, K1:K2-1) = B * diag(scale(K1:K2-1)) */

	    if (upper) {
		jfirst = 1;
		jlast = nba;
		jinc = 1;
	    } else {
		jfirst = nba;
		jlast = 1;
		jinc = -1;
	    }
	}

	i__2 = jlast;
	i__3 = jinc;
	for (j = jfirst; i__3 < 0 ? j >= i__2 : j <= i__2; j += i__3) {
/*           J1: row index of the first row in A( J, J ) */
/*           J2: row index of the first row in A( J+1, J+1 ) */
/*           so that J2 - J1 is the row count of the block A( J, J ) */
	    j1 = (j - 1) * nb + 1;
/* Computing MIN */
	    i__4 = j * nb;
	    j2 = f2cmin(i__4,*n) + 1;

/*           Solve op(A( J, J )) * X( J, RHS ) = SCALOC * B( J, RHS ) */
/*           for all right-hand sides in the current block column, */
/*           one RHS at a time. */

	    i__4 = k2 - k1;
	    for (kk = 1; kk <= i__4; ++kk) {
		rhs = k1 + kk - 1;
		if (kk == 1) {
		    i__5 = j2 - j1;
		    slatrs_(uplo, trans, diag, "N", &i__5, &a[j1 + j1 * 
			    a_dim1], lda, &x[j1 + rhs * x_dim1], &scaloc, &
			    cnorm[1], info);
		} else {
		    i__5 = j2 - j1;
		    slatrs_(uplo, trans, diag, "Y", &i__5, &a[j1 + j1 * 
			    a_dim1], lda, &x[j1 + rhs * x_dim1], &scaloc, &
			    cnorm[1], info);
		}
/*              Find largest absolute value entry in the vector segment */
/*              X( J1:J2-1, RHS ) as an upper bound for the worst case */
/*              growth in the linear updates. */
		i__5 = j2 - j1;
		xnrm[kk - 1] = slange_("I", &i__5, &c__1, &x[j1 + rhs * 
			x_dim1], ldx, w);

		if (scaloc == 0.f) {
/*                 LATRS found that A is singular through A(j,j) = 0. */
/*                 Reset the computation x(1:n) = 0, x(j) = 1, SCALE = 0 */
/*                 and compute A*x = 0 (or A**T*x = 0). Note that */
/*                 X(J1:J2-1, KK) is set by LATRS. */
		    scale[rhs] = 0.f;
		    i__5 = j1 - 1;
		    for (ii = 1; ii <= i__5; ++ii) {
			x[ii + kk * x_dim1] = 0.f;
		    }
		    i__5 = *n;
		    for (ii = j2; ii <= i__5; ++ii) {
			x[ii + kk * x_dim1] = 0.f;
		    }
/*                 Discard the local scale factors. */
		    i__5 = nba;
		    for (ii = 1; ii <= i__5; ++ii) {
			work[ii + kk * lds] = 1.f;
		    }
		    scaloc = 1.f;
		} else if (scaloc * work[j + kk * lds] == 0.f) {
/*                 LATRS computed a valid scale factor, but combined with */
/*                 the current scaling the solution does not have a */
/*                 scale factor > 0. */

/*                 Set WORK( J+KK*LDS ) to smallest valid scale */
/*                 factor and increase SCALOC accordingly. */
		    scal = work[j + kk * lds] / smlnum;
		    scaloc *= scal;
		    work[j + kk * lds] = smlnum;
/*                 If LATRS overestimated the growth, x may be */
/*                 rescaled to preserve a valid combined scale */
/*                 factor WORK( J, KK ) > 0. */
		    rscal = 1.f / scaloc;
		    if (xnrm[kk - 1] * rscal <= bignum) {
			xnrm[kk - 1] *= rscal;
			i__5 = j2 - j1;
			sscal_(&i__5, &rscal, &x[j1 + rhs * x_dim1], &c__1);
			scaloc = 1.f;
		    } else {
/*                    The system op(A) * x = b is badly scaled and its */
/*                    solution cannot be represented as (1/scale) * x. */
/*                    Set x to zero. This approach deviates from LATRS */
/*                    where a completely meaningless non-zero vector */
/*                    is returned that is not a solution to op(A) * x = b. */
			scale[rhs] = 0.f;
			i__5 = *n;
			for (ii = 1; ii <= i__5; ++ii) {
			    x[ii + kk * x_dim1] = 0.f;
			}
/*                    Discard the local scale factors. */
			i__5 = nba;
			for (ii = 1; ii <= i__5; ++ii) {
			    work[ii + kk * lds] = 1.f;
			}
			scaloc = 1.f;
		    }
		}
		scaloc *= work[j + kk * lds];
		work[j + kk * lds] = scaloc;
	    }

/*           Linear block updates */

	    if (notran) {
		if (upper) {
		    ifirst = j - 1;
		    ilast = 1;
		    iinc = -1;
		} else {
		    ifirst = j + 1;
		    ilast = nba;
		    iinc = 1;
		}
	    } else {
		if (upper) {
		    ifirst = j + 1;
		    ilast = nba;
		    iinc = 1;
		} else {
		    ifirst = j - 1;
		    ilast = 1;
		    iinc = -1;
		}
	    }

	    i__4 = ilast;
	    i__5 = iinc;
	    for (i__ = ifirst; i__5 < 0 ? i__ >= i__4 : i__ <= i__4; i__ += 
		    i__5) {
/*              I1: row index of the first column in X( I, K ) */
/*              I2: row index of the first column in X( I+1, K ) */
/*              so the I2 - I1 is the row count of the block X( I, K ) */
		i1 = (i__ - 1) * nb + 1;
/* Computing MIN */
		i__6 = i__ * nb;
		i2 = f2cmin(i__6,*n) + 1;

/*              Prepare the linear update to be executed with GEMM. */
/*              For each column, compute a consistent scaling, a */
/*              scaling factor to survive the linear update, and */
/*              rescale the column segments, if necesssary. Then */
/*              the linear update is safely executed. */

		i__6 = k2 - k1;
		for (kk = 1; kk <= i__6; ++kk) {
		    rhs = k1 + kk - 1;
/*                 Compute consistent scaling */
/* Computing MIN */
		    r__1 = work[i__ + kk * lds], r__2 = work[j + kk * lds];
		    scamin = f2cmin(r__1,r__2);

/*                 Compute scaling factor to survive the linear update */
/*                 simulating consistent scaling. */

		    i__7 = i2 - i1;
		    bnrm = slange_("I", &i__7, &c__1, &x[i1 + rhs * x_dim1], 
			    ldx, w);
		    bnrm *= scamin / work[i__ + kk * lds];
		    xnrm[kk - 1] *= scamin / work[j + kk * lds];
		    anrm = work[awrk + i__ + (j - 1) * nba];
		    scaloc = slarmm_(&anrm, &xnrm[kk - 1], &bnrm);

/*                 Simultaneously apply the robust update factor and the */
/*                 consistency scaling factor to B( I, KK ) and B( J, KK ). */

		    scal = scamin / work[i__ + kk * lds] * scaloc;
		    if (scal != 1.f) {
			i__7 = i2 - i1;
			sscal_(&i__7, &scal, &x[i1 + rhs * x_dim1], &c__1);
			work[i__ + kk * lds] = scamin * scaloc;
		    }

		    scal = scamin / work[j + kk * lds] * scaloc;
		    if (scal != 1.f) {
			i__7 = j2 - j1;
			sscal_(&i__7, &scal, &x[j1 + rhs * x_dim1], &c__1);
			work[j + kk * lds] = scamin * scaloc;
		    }
		}

		if (notran) {

/*                 B( I, K ) := B( I, K ) - A( I, J ) * X( J, K ) */

		    i__6 = i2 - i1;
		    i__7 = k2 - k1;
		    i__8 = j2 - j1;
		    sgemm_("N", "N", &i__6, &i__7, &i__8, &c_b35, &a[i1 + j1 *
			     a_dim1], lda, &x[j1 + k1 * x_dim1], ldx, &c_b36, 
			    &x[i1 + k1 * x_dim1], ldx);
		} else {

/*                 B( I, K ) := B( I, K ) - A( I, J )**T * X( J, K ) */

		    i__6 = i2 - i1;
		    i__7 = k2 - k1;
		    i__8 = j2 - j1;
		    sgemm_("T", "N", &i__6, &i__7, &i__8, &c_b35, &a[j1 + i1 *
			     a_dim1], lda, &x[j1 + k1 * x_dim1], ldx, &c_b36, 
			    &x[i1 + k1 * x_dim1], ldx);
		}
	    }
	}

/*        Reduce local scaling factors */

	i__3 = k2 - k1;
	for (kk = 1; kk <= i__3; ++kk) {
	    rhs = k1 + kk - 1;
	    i__2 = nba;
	    for (i__ = 1; i__ <= i__2; ++i__) {
/* Computing MIN */
		r__1 = scale[rhs], r__2 = work[i__ + kk * lds];
		scale[rhs] = f2cmin(r__1,r__2);
	    }
	}

/*        Realize consistent scaling */

	i__3 = k2 - k1;
	for (kk = 1; kk <= i__3; ++kk) {
	    rhs = k1 + kk - 1;
	    if (scale[rhs] != 1.f && scale[rhs] != 0.f) {
		i__2 = nba;
		for (i__ = 1; i__ <= i__2; ++i__) {
		    i1 = (i__ - 1) * nb + 1;
/* Computing MIN */
		    i__5 = i__ * nb;
		    i2 = f2cmin(i__5,*n) + 1;
		    scal = scale[rhs] / work[i__ + kk * lds];
		    if (scal != 1.f) {
			i__5 = i2 - i1;
			sscal_(&i__5, &scal, &x[i1 + rhs * x_dim1], &c__1);
		    }
		}
	    }
	}
    }
    return;

/*     End of SLATRS3 */

} /* slatrs3_ */

